//! HostRuntime dispatch 闭环的真实 stdio 集成测试（Unix-only）。
//!
//! 每个用例都启动临时 shell sidecar，通过真实 stdin/stdout 收发 JSON-RPC；测试
//! 依赖 Unix shell/FIFO，Windows 不执行；Windows capability/unavailable 门禁见
//! `pr0_windows_hardening.rs`。
//! 只观察非敏感 wire 和 Kit 产品事件，避免 mock 掩盖进程、握手与反向 RPC 接线。

#![cfg(unix)]

use std::collections::BTreeSet;
use std::ffi::CString;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;
use efflab_agent_host::{
    ApprovedMcpSpec, HostApp, HostAppMentions, HostRuntime, HostRuntimeConfig, KitBlock,
    KitCommand, KitEventSink, KitProductEvent, KitReply, LlmChannelConfig, LlmChannelKind,
    LlmSecretSlot, MentionId, Origin, ResolvedMention, ScopeId, SealedSecret, SecretGuard,
};
use serde_json::{Value, json};

/// 单次 sidecar 启动、wire 或事件观察的上限，避免失败实现让测试永久挂起。
///
/// 完整套件会并行启动多个 L3b 与真实子进程；8 秒只覆盖这种资源调度，不放宽下方
/// Send/Resume 的 350ms 协议回执断言。
const TEST_TIMEOUT: Duration = Duration::from_secs(8);
/// `session/prompt` result 人为延迟，用来锁定 dispatch 不得等待该 result。
const DELAYED_PROMPT_RESULT: Duration = Duration::from_millis(700);
/// 故意阻塞同步 sink 的时长；回执必须先于任何事件运输返回。
const SLOW_SINK_DELAY: Duration = Duration::from_millis(250);
/// 断言 actor 已写出通知或决定热恢复后，Kit 回执不得等待慢 sink。
const IMMEDIATE_REPLY_TIMEOUT: Duration = Duration::from_millis(150);
/// 控制命令在通知洪水下仍必须在此固定预算内取得 actor 调度机会。
const CONTROL_REPLY_TIMEOUT: Duration = Duration::from_secs(2);
/// 测试专用 catalog deadline；生产构造器仍冻结为 20 秒合同。
const TEST_MCP_CATALOG_TIMEOUT: Duration = Duration::from_millis(15);
/// fake sidecar 返回的固定已知会话 ID；测试不得用请求值反向建立会话 oracle。
const CANONICAL_SESSION_ID: &str = "sidecar-session";

/// mention 端口的测试行为；每种分支都只在本集成测试中构造。
enum MentionMode {
    /// 产品没有声明 mention 能力。
    Unsupported,
    /// 返回与请求一一对应的安全中文展示文本。
    Resolve,
    /// 模拟未知或跨 scope 标识被产品端口拒绝。
    Reject,
    /// 返回指定展示文本，供 Host 长度与最终安全门回归测试使用。
    Text(String),
}

/// 已配置或未配置 Channel 的最小产品端口；测试凭据只停留在内存中。
struct FakeApp {
    config: Arc<Mutex<LlmChannelConfig>>,
    expected_tools: BTreeSet<String>,
    mention_mode: MentionMode,
}

impl FakeApp {
    /// 构造带指定 mention 端口行为的公开 HTTPS BYOK 配置。
    fn byok_with_mention_mode(
        expected_tools: impl IntoIterator<Item = String>,
        mention_mode: MentionMode,
    ) -> Self {
        Self {
            config: Arc::new(Mutex::new(LlmChannelConfig::Byok {
                base_url: "https://8.8.8.8/v1".to_string(),
                model_id: "fake-byok-model".to_string(),
                api_key: SealedSecret::new(b"test-key".to_vec()),
            })),
            expected_tools: expected_tools.into_iter().collect(),
            mention_mode,
        }
    }

    /// 构造未配置 Channel，验证所有对话命令在 spawn 前 fail-closed。
    fn unconfigured() -> Self {
        Self {
            config: Arc::new(Mutex::new(LlmChannelConfig::Unconfigured)),
            expected_tools: BTreeSet::new(),
            mention_mode: MentionMode::Unsupported,
        }
    }
}

impl HostApp for FakeApp {
    fn app_id(&self) -> &str {
        "dispatch-loop-test"
    }

    fn persist_llm_channel(&self, cfg: &LlmChannelConfig) -> Result<()> {
        *self.config.lock().expect("测试配置锁必须可用") = cfg.clone();
        Ok(())
    }

    fn load_llm_channel(&self) -> Result<LlmChannelConfig> {
        Ok(self.config.lock().expect("测试配置锁必须可用").clone())
    }

    fn seal_secret(&self, plain: &[u8]) -> Result<SealedSecret> {
        Ok(SealedSecret::new(plain.to_vec()))
    }

    fn unseal_secret(&self, sealed: &SealedSecret) -> Result<SecretGuard> {
        Ok(SecretGuard::new(sealed.as_bytes().to_vec()))
    }

    /// 测试适配器显式声明 BYOK 槽，避免依赖遗留通用槽语义。
    fn seal_llm_secret(&self, slot: LlmSecretSlot, plain: &[u8]) -> Result<SealedSecret> {
        match slot {
            LlmSecretSlot::Byok => self.seal_secret(plain),
            LlmSecretSlot::Relay => Err(anyhow::anyhow!("测试不启用 Relay 槽")),
        }
    }

    /// 测试适配器显式声明 BYOK 槽，避免依赖遗留通用槽语义。
    fn unseal_llm_secret(&self, slot: LlmSecretSlot, sealed: &SealedSecret) -> Result<SecretGuard> {
        match slot {
            LlmSecretSlot::Byok => self.unseal_secret(sealed),
            LlmSecretSlot::Relay => Err(anyhow::anyhow!("测试不启用 Relay 槽")),
        }
    }

    fn mcp_for_scope(&self, _scope: &ScopeId) -> Result<ApprovedMcpSpec> {
        Ok(ApprovedMcpSpec::with_expected_tools(
            self.expected_tools.iter().cloned(),
        ))
    }

    /// 只有显式启用的测试端口才向运行时声明 mentions 能力。
    fn mentions(&self) -> Option<&dyn HostAppMentions> {
        if matches!(&self.mention_mode, MentionMode::Unsupported) {
            None
        } else {
            Some(self)
        }
    }
}

impl HostAppMentions for FakeApp {
    /// 按测试模式解析当前 scope 的标识，模拟产品端口的安全与失败关闭语义。
    fn resolve_mentions(&self, scope: &ScopeId, ids: &[MentionId]) -> Result<Vec<ResolvedMention>> {
        if scope.0.as_str() != "scope-a" {
            return Err(anyhow::anyhow!("测试端口拒绝跨 scope mention"));
        }

        match &self.mention_mode {
            MentionMode::Resolve => Ok(ids
                .iter()
                .cloned()
                .map(|id| ResolvedMention {
                    text: format!("曲目：{}；艺人：测试艺人", id.id),
                    id,
                })
                .collect()),
            MentionMode::Reject => Err(anyhow::anyhow!("测试端口拒绝未知 mention")),
            MentionMode::Text(text) => Ok(ids
                .iter()
                .cloned()
                .map(|id| ResolvedMention {
                    id,
                    text: text.clone(),
                })
                .collect()),
            MentionMode::Unsupported => Err(anyhow::anyhow!("未声明的 mention 端口不能解析")),
        }
    }
}

/// 内存事件运输端口；可注入延迟以验证 actor 不会把产品回执绑在同步投影上。
struct MemorySink {
    events: Arc<Mutex<Vec<KitProductEvent>>>,
    delay: Duration,
    fail_next_terminal: Arc<AtomicBool>,
    fail_terminal_attempts: Arc<AtomicUsize>,
    fail_after_commit_terminal: Arc<AtomicBool>,
    terminal_attempts: Arc<AtomicUsize>,
    terminal_identities: Arc<Mutex<Vec<(String, u64)>>>,
}

impl Default for MemorySink {
    fn default() -> Self {
        Self::with_delay(Duration::from_millis(0))
    }
}

impl MemorySink {
    /// 构造带固定投影延迟的测试 sink；延迟不改变事件记录顺序。
    fn with_delay(delay: Duration) -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
            delay,
            fail_next_terminal: Arc::new(AtomicBool::new(false)),
            fail_terminal_attempts: Arc::new(AtomicUsize::new(0)),
            fail_after_commit_terminal: Arc::new(AtomicBool::new(false)),
            terminal_attempts: Arc::new(AtomicUsize::new(0)),
            terminal_identities: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl KitEventSink for MemorySink {
    fn emit(&self, event: KitProductEvent) -> Result<()> {
        if !self.delay.is_zero() {
            thread::sleep(self.delay);
        }
        let is_terminal = matches!(
            &event.block,
            KitBlock::Status { code, .. }
                if matches!(code.as_str(), "turn_completed" | "cancelled" | "error")
        );
        if is_terminal {
            self.terminal_attempts.fetch_add(1, Ordering::AcqRel);
            self.terminal_identities
                .lock()
                .expect("终态身份锁必须可用")
                .push((event.event_id.clone(), event.sequence));
        }
        if is_terminal && self.fail_next_terminal.swap(false, Ordering::AcqRel) {
            return Err(anyhow::anyhow!("测试故意让回合终态运输失败"));
        }
        if is_terminal {
            loop {
                let remaining = self.fail_terminal_attempts.load(Ordering::Acquire);
                if remaining == 0 {
                    break;
                }
                if self
                    .fail_terminal_attempts
                    .compare_exchange(
                        remaining,
                        remaining - 1,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    return Err(anyhow::anyhow!("测试故意让回合终态运输失败"));
                }
            }
            if self
                .fail_after_commit_terminal
                .swap(false, Ordering::AcqRel)
            {
                self.events
                    .lock()
                    .expect("事件锁必须可用")
                    .push(event.clone());
                return Err(anyhow::anyhow!("测试故意在提交后返回终态运输错误"));
            }
        }
        self.events.lock().expect("事件锁必须可用").push(event);
        Ok(())
    }
}

/// 一套真正由 HostRuntime 启动的 fake sidecar 与可观察产物。
struct Harness {
    _temporary: tempfile::TempDir,
    runtime: Arc<HostRuntime>,
    started: PathBuf,
    exited: PathBuf,
    captured: PathBuf,
    prompt_gate: PathBuf,
    prompt_waiting: PathBuf,
    cancel_seen: PathBuf,
    catalog_gate: PathBuf,
    catalog_waiting: PathBuf,
    catalog_response_sent: PathBuf,
    events: Arc<Mutex<Vec<KitProductEvent>>>,
    sidecar: PathBuf,
    control: PathBuf,
    control_gate: PathBuf,
    load_waiting: PathBuf,
    load_completed: PathBuf,
    late_replay: PathBuf,
    fail_next_terminal: Arc<AtomicBool>,
    fail_terminal_attempts: Arc<AtomicUsize>,
    fail_after_commit_terminal: Arc<AtomicBool>,
    terminal_attempts: Arc<AtomicUsize>,
    terminal_identities: Arc<Mutex<Vec<(String, u64)>>>,
}

impl Harness {
    /// 构造已配置 Channel 的运行时；mode 决定 fake sidecar 的 ACP 行为。
    fn configured(
        mode: &str,
        expected_tools: impl IntoIterator<Item = String>,
        idle_after: Duration,
    ) -> Self {
        Self::configured_with_options(
            mode,
            expected_tools,
            MentionMode::Unsupported,
            idle_after,
            Duration::from_millis(0),
            None,
            None,
        )
    }

    /// 构造声明 mention 端口的运行时，用于验证 Host 的解析与最终文本门禁。
    fn configured_with_mentions(
        mode: &str,
        expected_tools: impl IntoIterator<Item = String>,
        mention_mode: MentionMode,
        idle_after: Duration,
    ) -> Self {
        Self::configured_with_options(
            mode,
            expected_tools,
            mention_mode,
            idle_after,
            Duration::from_millis(0),
            None,
            None,
        )
    }

    /// 构造使用短 catalog deadline 的运行时；只供超时回归测试避免真实等待生产 20 秒。
    fn configured_with_catalog_timeout(
        mode: &str,
        expected_tools: impl IntoIterator<Item = String>,
        idle_after: Duration,
        catalog_timeout: Duration,
    ) -> Self {
        Self::configured_with_options(
            mode,
            expected_tools,
            MentionMode::Unsupported,
            idle_after,
            Duration::from_millis(0),
            Some(catalog_timeout),
            None,
        )
    }

    /// 构造使用短 load deadline 的运行时；只供超时回归测试避免真实等待生产 60 秒。
    fn configured_with_load_timeout(
        mode: &str,
        expected_tools: impl IntoIterator<Item = String>,
        idle_after: Duration,
        load_timeout: Duration,
    ) -> Self {
        Self::configured_with_options(
            mode,
            expected_tools,
            MentionMode::Unsupported,
            idle_after,
            Duration::from_millis(0),
            None,
            Some(load_timeout),
        )
    }

    /// 构造同时覆盖 load deadline 与同步 sink 延迟的运行时，验证 response 排队后的截止语义。
    fn configured_with_load_timeout_and_sink_delay(
        mode: &str,
        expected_tools: impl IntoIterator<Item = String>,
        idle_after: Duration,
        load_timeout: Duration,
        sink_delay: Duration,
    ) -> Self {
        Self::configured_with_options(
            mode,
            expected_tools,
            MentionMode::Unsupported,
            idle_after,
            sink_delay,
            None,
            Some(load_timeout),
        )
    }

    /// 构造带可控同步 sink 的运行时，用于锁定产品回执与事件投影的顺序。
    fn configured_with_sink_delay(
        mode: &str,
        expected_tools: impl IntoIterator<Item = String>,
        idle_after: Duration,
        sink_delay: Duration,
    ) -> Self {
        Self::configured_with_options(
            mode,
            expected_tools,
            MentionMode::Unsupported,
            idle_after,
            sink_delay,
            None,
            None,
        )
    }

    /// 集中构造 fake sidecar；只有显式传入时才使用测试专用 catalog deadline。
    fn configured_with_options(
        mode: &str,
        expected_tools: impl IntoIterator<Item = String>,
        mention_mode: MentionMode,
        idle_after: Duration,
        sink_delay: Duration,
        catalog_timeout: Option<Duration>,
        load_timeout: Option<Duration>,
    ) -> Self {
        let temporary = tempfile::tempdir().expect("必须能创建 dispatch loop 临时目录");
        let root = temporary.path();
        let sidecar = root.join("fake-sidecar.sh");
        let started = root.join("sidecar-started");
        let exited = root.join("sidecar-exited");
        let captured = root.join("sidecar-wire.jsonl");
        let prompt_gate = root.join("prompt-control.fifo");
        let prompt_waiting = root.join("prompt-waiting");
        let cancel_seen = root.join("cancel-seen");
        let catalog_gate = root.join("catalog-control.fifo");
        let catalog_waiting = root.join("catalog-waiting");
        let catalog_response_sent = root.join("catalog-response-sent");
        let sidecar_log = root.join("sidecar.log");
        let control = root.join("load-control");
        let control_gate = root.join("load-control.fifo");
        let load_waiting = root.join("load-waiting");
        let load_completed = root.join("load-completed");
        create_fifo(&prompt_gate);
        create_fifo(&catalog_gate);
        create_fifo(&control_gate);
        let late_replay = root.join("late-replay");
        let canonical_root = fs::canonicalize(root).expect("测试临时根必须能 canonicalize");
        let expected_scope_root = canonical_root.join("app-data/dispatch-loop-test");
        write_fake_sidecar(
            &sidecar,
            &started,
            &exited,
            &captured,
            &prompt_gate,
            &prompt_waiting,
            &cancel_seen,
            &catalog_gate,
            &catalog_waiting,
            &catalog_response_sent,
            &control,
            &control_gate,
            &load_waiting,
            &load_completed,
            &late_replay,
            &expected_scope_root,
            mode,
        );

        let sink = MemorySink::with_delay(sink_delay);
        let events = Arc::clone(&sink.events);
        let fail_next_terminal = Arc::clone(&sink.fail_next_terminal);
        let fail_terminal_attempts = Arc::clone(&sink.fail_terminal_attempts);
        let fail_after_commit_terminal = Arc::clone(&sink.fail_after_commit_terminal);
        let terminal_attempts = Arc::clone(&sink.terminal_attempts);
        let terminal_identities = Arc::clone(&sink.terminal_identities);
        let config = HostRuntimeConfig {
            home_root: root.join("app-data"),
            sidecar_bin: sidecar.clone(),
            sidecar_log_path: sidecar_log.clone(),
            mcp_exec_root: root.join("mcp"),
            idle_after,
            l3b: Default::default(),
        };
        let runtime = match (catalog_timeout, load_timeout) {
            (Some(catalog_timeout), None) => HostRuntime::new_for_test_with_mcp_catalog_timeout(
                FakeApp::byok_with_mention_mode(expected_tools, mention_mode),
                sink,
                config,
                catalog_timeout,
            ),
            (None, Some(load_timeout)) => HostRuntime::new_for_test_with_load_timeout(
                FakeApp::byok_with_mention_mode(expected_tools, mention_mode),
                sink,
                config,
                load_timeout,
            ),
            (None, None) => HostRuntime::new(
                FakeApp::byok_with_mention_mode(expected_tools, mention_mode),
                sink,
                config,
            ),
            (Some(_), Some(_)) => panic!("测试构造器不支持同时覆盖两类 deadline"),
        };
        Self {
            _temporary: temporary,
            runtime: Arc::new(runtime),
            started,
            exited,
            captured,
            prompt_gate,
            prompt_waiting,
            cancel_seen,
            catalog_gate,
            catalog_waiting,
            catalog_response_sent,
            events,
            sidecar,
            control,
            control_gate,
            load_waiting,
            load_completed,
            late_replay,
            fail_next_terminal,
            fail_terminal_attempts,
            fail_after_commit_terminal,
            terminal_attempts,
            terminal_identities,
        }
    }

    /// 构造没有 Channel 的运行时；fake executable 不应被执行。
    fn unconfigured() -> (Arc<HostRuntime>, tempfile::TempDir, PathBuf) {
        let temporary = tempfile::tempdir().expect("必须能创建未配置测试目录");
        let home_root = temporary.path().join("app-data");
        let runtime = Arc::new(HostRuntime::new(
            FakeApp::unconfigured(),
            MemorySink::default(),
            HostRuntimeConfig {
                home_root: home_root.clone(),
                sidecar_bin: temporary.path().join("must-not-spawn"),
                sidecar_log_path: temporary.path().join("sidecar.log"),
                mcp_exec_root: temporary.path().join("mcp"),
                idle_after: Duration::from_secs(60),
                l3b: Default::default(),
            },
        ));
        (runtime, temporary, home_root)
    }

    /// 创建 sidecar 的新会话，并取得真实 ACP 返回的 session id。
    fn new_session(&self, scope_id: &str) -> String {
        match self
            .runtime
            .dispatch(KitCommand::NewSession {
                scope_id: scope_id.to_string(),
                client_request_id: None,
            })
            .expect("NewSession 必须等待 sidecar session/new result")
        {
            KitReply::NewSession { session_id } => session_id,
            other => panic!("预期 NewSession reply，实际为 {other:?}"),
        }
    }

    /// 返回当前已写出的完整 JSONL 前缀快照；最终 wire 断言必须先等待对应完成 marker。
    ///
    /// 该快照只描述读取瞬间已经写完换行的内容，不代表 sidecar 已停止追加。
    fn wire(&self) -> Vec<Value> {
        wait_for_file(&self.captured);
        read_complete_json_lines(&self.captured)
    }

    /// 返回某个逻辑 ACP method 已写出的次数。
    fn method_count(&self, method: &str) -> usize {
        self.wire()
            .iter()
            .filter(|wire| wire.get("method").and_then(Value::as_str) == Some(method))
            .count()
    }

    /// 等待 sidecar 出现指定方法，避免断言与 actor 读循环抢跑。
    fn wait_for_method(&self, method: &str) {
        let deadline = Instant::now() + TEST_TIMEOUT;
        loop {
            if try_read_complete_json_lines(&self.captured).is_some_and(|wire| {
                wire.iter()
                    .any(|item| item.get("method").and_then(Value::as_str) == Some(method))
            }) {
                return;
            }
            assert!(Instant::now() < deadline, "等待指定 sidecar method 超时");
            thread::yield_now();
        }
    }

    /// 等待 fake sidecar 已进入受控 prompt 阻塞点，避免释放信号早于 reader。
    fn wait_for_prompt(&self, expected: usize) {
        wait_until(|| marker_line_count(&self.prompt_waiting) >= expected);
    }

    /// 释放 fake sidecar 的 prompt result 屏障；prompt 仍保持由 Host 控制的 in-flight。
    fn release_prompt(&self) {
        release_fifo(&self.prompt_gate);
    }

    /// 等待 fake sidecar 收到 catalog 请求并停在响应前的受控屏障。
    fn wait_for_catalog_request(&self) {
        wait_for_file(&self.catalog_waiting);
    }

    /// 释放 fake sidecar 的 catalog response 屏障；只在 timeout 断言后调用。
    fn release_catalog(&self) {
        release_fifo(&self.catalog_gate);
    }

    /// 读取 fake sidecar 是否已经发送迟到 catalog response。
    fn catalog_response_was_sent(&self) -> bool {
        self.catalog_response_sent.exists()
    }

    /// 返回测试当前已经收到的事件快照。
    fn events(&self) -> Vec<KitProductEvent> {
        self.events.lock().expect("事件锁必须可用").clone()
    }

    /// 返回 fake sidecar 预期的 app-data scope 根，供 session cwd 精确断言。
    fn expected_scope_root(&self) -> PathBuf {
        fs::canonicalize(self._temporary.path())
            .expect("测试临时根必须能 canonicalize")
            .join("app-data/dispatch-loop-test")
    }

    /// 让下一条回合终态事件在 sink 中失败一次。
    fn fail_next_terminal(&self) {
        self.fail_next_terminal.store(true, Ordering::Release);
    }

    /// 让指定数量的回合终态运输尝试失败。
    fn fail_terminal_attempts(&self, attempts: usize) {
        self.fail_terminal_attempts
            .store(attempts, Ordering::Release);
    }

    /// 让下一次回合终态先提交到 sink 再返回错误。
    fn fail_next_terminal_after_commit(&self) {
        self.fail_after_commit_terminal
            .store(true, Ordering::Release);
    }

    /// 等待终态 sink 观察到指定次数的真实运输尝试。
    fn wait_for_terminal_attempts(&self, expected: usize) {
        wait_until(|| self.terminal_attempts.load(Ordering::Acquire) >= expected);
    }

    /// 发起一次 resume 并断言它只等待 Host 的立即受理回执。
    fn resume(&self, session_id: &str) -> KitReply {
        self.runtime
            .dispatch(KitCommand::ResumeSession {
                scope_id: "scope-a".to_string(),
                session_id: session_id.to_string(),
            })
            .expect("Resume 必须返回立即受理回执")
    }

    /// 发起一次 resume 并返回 Host 的结构化错误。
    fn resume_error(&self, session_id: &str) -> efflab_agent_host::KitError {
        self.runtime
            .dispatch(KitCommand::ResumeSession {
                scope_id: "scope-a".to_string(),
                session_id: session_id.to_string(),
            })
            .expect_err("Resume 应在 busy 或失败状态返回结构化错误")
    }

    /// 返回当前已写出的指定 ACP method，调用方先等待 method 后再读取完整 JSON 行。
    fn captured_requests(&self, method: &str) -> Vec<Value> {
        self.wire()
            .into_iter()
            .filter(|wire| wire.get("method").and_then(Value::as_str) == Some(method))
            .collect()
    }

    /// 统计产品事件中的固定 Status code，不把 replay 控制事件存入测试状态。
    fn count_events_with_code(&self, code: &str) -> usize {
        self.events()
            .iter()
            .filter(|event| {
                matches!(&event.block, KitBlock::Status { code: actual, .. } if actual == code)
            })
            .count()
    }

    /// 在 dispatch 前后读取不触发等待或业务动作的生命周期快照。
    fn lifecycle_snapshot(&self) -> LifecycleSnapshot {
        let wire = try_read_complete_json_lines(&self.captured).unwrap_or_default();
        let count_method = |method: &str| {
            wire.iter()
                .filter(|item| item.get("method").and_then(Value::as_str) == Some(method))
                .count()
        };
        let events = self.events();
        LifecycleSnapshot {
            started: marker_line_count(&self.started),
            exited: marker_line_count(&self.exited),
            session_load: count_method("session/load"),
            session_cancel: count_method("session/cancel"),
            session_prompt: count_method("session/prompt"),
            lifecycle_events: events
                .iter()
                .filter(|event| {
                    matches!(&event.block, KitBlock::Status { .. } | KitBlock::Error(_))
                })
                .count(),
            terminal_events: events
                .iter()
                .filter(|event| {
                    matches!(
                        &event.block,
                        KitBlock::Status { code, .. }
                            if matches!(code.as_str(), "turn_completed" | "cancelled" | "error")
                    )
                })
                .count(),
            total_events: events.len(),
        }
    }
}

/// 只记录生命周期计数，不把异步 payload 带入 submission_id 无副作用断言。
#[derive(Debug, Clone, PartialEq, Eq)]
struct LifecycleSnapshot {
    started: usize,
    exited: usize,
    session_load: usize,
    session_cancel: usize,
    session_prompt: usize,
    lifecycle_events: usize,
    terminal_events: usize,
    total_events: usize,
}

/// 读取非敏感 marker 的行数；文件不存在表示该生命周期步骤尚未发生。
fn marker_line_count(path: &Path) -> usize {
    fs::read_to_string(path)
        .map(|contents| contents.lines().count())
        .unwrap_or(0)
}

/// Task 7 禁止恢复的旧 Host 诊断 code；测试只观察 wire-visible 产品事件。
fn is_diagnostic(event: &KitProductEvent) -> bool {
    matches!(
        &event.block,
        KitBlock::Status { code, .. } if code == "skipped_update" || code == "replay_skipped"
    )
}

/// 把任意临时路径变成 POSIX shell 单引号字面量；临时目录通常不含引号，仍保持安全。
fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.display().to_string().replace('\'', "'\\''"))
}

/// 对真实 capture 快照执行 ACP session 请求的闭集字段断言。
fn assert_session_wire_contract(
    wire: &[Value],
    expected_session_id: &str,
    expected_session_cwd: &Path,
    expected_list_cursor: Option<&str>,
) {
    let expected_session_cwd = expected_session_cwd.display().to_string();
    for request in wire {
        let method = request
            .get("method")
            .and_then(Value::as_str)
            .expect("Host 出站 ACP 消息必须带 method");
        let params = request
            .get("params")
            .expect("Host 出站 ACP 消息必须带 params");
        match method {
            "initialize" => {
                assert_exact_object_fields(
                    params,
                    &["clientCapabilities", "clientInfo", "protocolVersion"],
                    method,
                );
            }
            "session/new" => {
                assert_exact_object_fields(params, &["_meta", "cwd", "mcpServers"], method);
                assert_eq!(params["mcpServers"], json!([]));
                assert_eq!(params["_meta"], json!({ "modelId": "byok" }));
                assert_eq!(
                    params["cwd"],
                    Value::String(expected_session_cwd.clone()),
                    "session/new cwd 必须精确等于当前 scope cwd"
                );
            }
            "session/list" => {
                match expected_list_cursor {
                    Some(expected_cursor) => {
                        assert_exact_object_fields(params, &["cursor", "cwd"], method);
                        assert_eq!(
                            params.get("cursor").and_then(Value::as_str),
                            Some(expected_cursor),
                            "session/list cursor 必须等于调用方显式传入的值"
                        );
                    }
                    None => {
                        assert_exact_object_fields(params, &["cwd"], method);
                        assert!(
                            params.get("cursor").is_none(),
                            "ListSessions cursor=None 时 session/list params 不得包含 cursor"
                        );
                    }
                }
                assert_eq!(
                    params["cwd"],
                    Value::String(expected_session_cwd.clone()),
                    "session/list cwd 必须精确等于当前 scope cwd"
                );
            }
            "session/load" => {
                assert_exact_object_fields(
                    params,
                    &["_meta", "cwd", "mcpServers", "sessionId"],
                    method,
                );
                assert_eq!(
                    params["sessionId"], expected_session_id,
                    "session/load 必须精确使用 expected canonical session ID"
                );
                assert_eq!(params["mcpServers"], json!([]));
                assert_eq!(params["_meta"], json!({ "modelId": "byok" }));
                assert_eq!(
                    params["cwd"],
                    Value::String(expected_session_cwd.clone()),
                    "session/load cwd 必须精确等于当前 scope cwd"
                );
            }
            "session/prompt" => {
                assert_exact_object_fields(params, &["_meta", "prompt", "sessionId"], method);
                assert_eq!(
                    params["sessionId"], expected_session_id,
                    "session/prompt 必须精确使用当前 canonical session ID"
                );
                let prompt = params["prompt"]
                    .as_array()
                    .expect("session/prompt prompt 必须是 ContentBlock 数组");
                assert!(!prompt.is_empty(), "session/prompt prompt 不得为空");
                for (index, block) in prompt.iter().enumerate() {
                    let context = format!("{method}.prompt[{index}]");
                    assert_exact_object_fields(block, &["text", "type"], &context);
                    assert_eq!(
                        block["type"], "text",
                        "{context} type 必须是 text ContentBlock"
                    );
                    assert!(
                        block["text"].as_str().is_some_and(|text| !text.is_empty()),
                        "{context} text 必须是非空字符串"
                    );
                }
                let meta = &params["_meta"];
                assert_exact_object_fields(meta, &["promptId"], "session/prompt._meta");
                let meta = meta.as_object().expect("session/prompt _meta 必须是对象");
                assert!(
                    meta["promptId"]
                        .as_str()
                        .is_some_and(|prompt_id| !prompt_id.is_empty()),
                    "session/prompt _meta.promptId 必须是非空字符串"
                );
            }
            "session/cancel" => {
                assert_exact_object_fields(params, &["sessionId"], method);
                assert_eq!(
                    params["sessionId"], expected_session_id,
                    "session/cancel 必须精确使用当前 canonical session ID"
                );
            }
            "_x.ai/mcp/list" => {
                assert_exact_object_fields(params, &["sessionId"], method);
                assert_eq!(
                    params["sessionId"], expected_session_id,
                    "mcp/list 必须精确使用当前 canonical session ID"
                );
            }
            _ => panic!("未知 Host 出站 ACP method: {method}"),
        }
    }
}

/// 严格比较 JSON object 的 key 集合，未知字段和旧参数都不能漏过测试。
fn assert_exact_object_fields(value: &Value, expected: &[&str], context: &str) {
    let actual = value
        .as_object()
        .expect("ACP params 必须是 JSON object")
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let expected = expected.iter().copied().collect::<BTreeSet<_>>();
    assert_eq!(actual, expected, "{context} params 字段集合必须闭集");
}

#[test]
fn shell_quote_escapes_literal_apostrophe_without_changing_path() {
    assert_eq!(
        shell_quote(Path::new("task24/host's launch")),
        "'task24/host'\\''s launch'"
    );
}

/// 写出受控 shell sidecar；它不解析任何产品输入，只按 ACP method 返回固定测试响应。
fn write_fake_sidecar(
    sidecar: &Path,
    started: &Path,
    exited: &Path,
    captured: &Path,
    prompt_gate: &Path,
    prompt_waiting: &Path,
    cancel_seen: &Path,
    catalog_gate: &Path,
    catalog_waiting: &Path,
    catalog_response_sent: &Path,
    control: &Path,
    control_gate: &Path,
    load_waiting: &Path,
    load_completed: &Path,
    late_replay: &Path,
    expected_scope_root: &Path,
    mode: &str,
) {
    let script = r#"#!/bin/sh
mode='__MODE__'
started=__STARTED__
exited=__EXITED__
captured=__CAPTURED__
prompt_gate=__PROMPT_GATE__
prompt_waiting=__PROMPT_WAITING__
cancel_seen=__CANCEL_SEEN__
catalog_gate=__CATALOG_GATE__
catalog_waiting=__CATALOG_WAITING__
catalog_response_sent=__CATALOG_RESPONSE_SENT__
control=__CONTROL__
control_gate=__CONTROL_GATE__
load_waiting=__LOAD_WAITING__
load_completed=__LOAD_COMPLETED__
late_replay=__LATE_REPLAY__
expected_scope_root=__EXPECTED_SCOPE_ROOT__
wait_for_control() {
  IFS= read -r _ < "$control_gate"
}
wait_for_prompt_release() {
  IFS= read -r _ < "$prompt_gate"
}
wait_for_catalog_release() {
  IFS= read -r _ < "$catalog_gate"
}
home=""
runtime_config=""
session_cwd=""
runtime_config_count=0
home_count=0
session_cwd_count=0
stdio_count=0
arg_position=0
while [ "$#" -gt 0 ]; do
  case "$arg_position:$1" in
    0:--runtime-config)
      runtime_config_count=$((runtime_config_count + 1))
      [ "$runtime_config_count" -eq 1 ] || exit 11
      [ "$#" -ge 2 ] || exit 12
      runtime_config="$2"
      shift 2
      arg_position=2
      ;;
    2:--home)
      home_count=$((home_count + 1))
      [ "$home_count" -eq 1 ] || exit 13
      [ "$#" -ge 2 ] || exit 14
      home="$2"
      shift 2
      arg_position=4
      ;;
    4:--session-cwd)
      session_cwd_count=$((session_cwd_count + 1))
      [ "$session_cwd_count" -eq 1 ] || exit 15
      [ "$#" -ge 2 ] || exit 16
      session_cwd="$2"
      shift 2
      arg_position=6
      ;;
    6:--stdio)
      stdio_count=$((stdio_count + 1))
      [ "$stdio_count" -eq 1 ] || exit 17
      shift
      arg_position=7
      ;;
    *:--grok-home|*:--mcp-config|*:--mcp-exec-root)
      exit 18
      ;;
    *)
      exit 19
      ;;
  esac
done
[ "$arg_position" -eq 7 ] || exit 20
[ "$runtime_config_count" -eq 1 ] || exit 21
[ "$home_count" -eq 1 ] || exit 22
[ "$session_cwd_count" -eq 1 ] || exit 23
[ "$stdio_count" -eq 1 ] || exit 24
# spawn 前必须已有由 Host 写入的 v1 配置与本代 binding；绝不落盘 token 本体。
case "$home" in
  "$expected_scope_root/scope-a/home") expected_scope_cwd="$expected_scope_root/scope-a/workspace" ;;
  "$expected_scope_root/scope-b/home") expected_scope_cwd="$expected_scope_root/scope-b/workspace" ;;
  *) exit 25 ;;
esac
test "$session_cwd" = "$expected_scope_cwd" || exit 26
test "$runtime_config" = "$home/runtime-config.v1.toml" || exit 27
test -n "$EFFLAB_L3B_BIND" || exit 41
test -n "$home" || exit 42
test -n "$runtime_config" || exit 43
test -f "$runtime_config" || exit 44
/usr/bin/grep -q '^schema_version = 1$' "$runtime_config" || exit 46
/usr/bin/grep -q '^backend = "chat_completions"$' "$runtime_config" || exit 47
/usr/bin/grep -q '^token_env = "EFFLAB_L3B_BIND"$' "$runtime_config" || exit 48
/usr/bin/grep -q '^session_cwd = ' "$runtime_config" || exit 49
/usr/bin/printf '%s\n' 'started' >> "$started"
pending_prompt_id=""
pending_permission_session=""
new_session_count=0
load_count=0
# 默认 session/new 和 mcp/list 共用固定已知 ID，不从收到的 sessionId 建立 oracle。
canonical_session='sidecar-session'
new_session_id="$canonical_session"
permission_method='session/request_permission'
case "$mode" in
  *_wrapper) permission_method='_x.ai/session/request_permission' ;;
esac
while IFS= read -r line; do
  /usr/bin/printf '%s\n' "$line" >> "$captured"
  id=$(/usr/bin/printf '%s\n' "$line" | /usr/bin/sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  session=$(/usr/bin/printf '%s\n' "$line" | /usr/bin/sed -n 's/.*"sessionId":"\([^"]*\)".*/\1/p')
  cwd=$(/usr/bin/printf '%s\n' "$line" | /usr/bin/sed -n 's/.*"cwd":"\([^"]*\)".*/\1/p')
  if [ -n "$pending_prompt_id" ] && /usr/bin/printf '%s' "$line" | /usr/bin/grep -q '"id":900'; then
    /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$pending_prompt_id"
    pending_prompt_id=""
    continue
  fi
  case "$line" in
    *'"method":"initialize"'*)
      if [ "$mode" = "unknown_without_session" ]; then
        # 无法归属的未来通知不得杀死 actor；它没有 sessionId，不能形成产品事件。
        /usr/bin/printf '%s\n' '{"jsonrpc":"2.0","method":"_x.ai/session/update","params":{"update":{"sessionUpdate":"future_update"}}}'
      fi
      if [ "$mode" = "initialize_missing_meta" ]; then
        /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":1}}\n' "$id"
      elif [ "$mode" = "initialize_wrong_protocol" ]; then
        # ACP 协议版本不匹配时，Host 不得继续消费 deferred session 命令。
        /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":999,"_meta":{"efflabRuntime":"minimal-v1","efflabSchemaVersion":1,"efflabSessionStoreVersion":1}}}\n' "$id"
      elif [ "$mode" = "initialize_wrong_meta" ]; then
        /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":1,"_meta":{"efflabRuntime":"minimal-v1","efflabSchemaVersion":"1","efflabSessionStoreVersion":1}}}\n' "$id"
      elif [ "$mode" = "initialize_extra_meta" ]; then
        /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":1,"_meta":{"efflabRuntime":"minimal-v1","efflabSchemaVersion":1,"efflabSessionStoreVersion":1,"unexpected":true}}}\n' "$id"
      elif [ "$mode" = "initialize_invalid_capabilities" ]; then
        # 这些字段分别覆盖未知 capability、fs/terminal 与 auth.logout 违例。
        /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":1,"agentCapabilities":{"loadSession":true,"promptCapabilities":{"image":false,"audio":false,"embeddedContext":false},"mcpCapabilities":{"http":false,"sse":false},"sessionCapabilities":{"list":{}},"auth":{"logout":{}},"fs":{},"terminal":false,"unknownCapability":true},"authMethods":[],"_meta":{"efflabRuntime":"minimal-v1","efflabSchemaVersion":1,"efflabSessionStoreVersion":1}}}\n' "$id"
      elif [ "$mode" = "initialize_invalid_auth_methods" ]; then
        # 非空 authMethods 不能让 Host 误以为 sidecar 没有认证入口。
        /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":1,"agentCapabilities":{"loadSession":true,"promptCapabilities":{"image":false,"audio":false,"embeddedContext":false},"mcpCapabilities":{"http":false,"sse":false},"sessionCapabilities":{"list":{}},"auth":{}},"authMethods":[{"id":"unexpected","name":"Unexpected"}],"_meta":{"efflabRuntime":"minimal-v1","efflabSchemaVersion":1,"efflabSessionStoreVersion":1}}}\n' "$id"
      else
        # Host 握手需要同时确认 sidecar 的真实能力、认证闭集与 runtime metadata。
        /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":1,"agentCapabilities":{"loadSession":true,"promptCapabilities":{"image":false,"audio":false,"embeddedContext":false},"mcpCapabilities":{"http":false,"sse":false},"sessionCapabilities":{"list":{}},"auth":{}},"authMethods":[],"_meta":{"efflabRuntime":"minimal-v1","efflabSchemaVersion":1,"efflabSessionStoreVersion":1}}}\n' "$id"
      fi
      ;;
    *'"method":"session/new"'*)
      new_session_count=$((new_session_count + 1))
      test "$cwd" = "$expected_scope_cwd" || exit 51
      /usr/bin/printf '%s' "$line" | /usr/bin/grep -q '"mcpServers":\[\]' || exit 52
      /usr/bin/printf '%s' "$line" | /usr/bin/grep -q '"_meta":{"modelId":"byok"}' || exit 58
      if [ "$mode" = "active_non_current_live" ]; then
        # 该模式显式建立两个 active session，验证 current_session 只是最近指针。
        case "$new_session_count" in
          1) new_session_id='active-session-a' ;;
          2) new_session_id='active-session-b' ;;
          *) exit 50 ;;
        esac
      else
        new_session_id="$canonical_session"
      fi
      /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{"sessionId":"%s"}}\n' "$id" "$new_session_id"
      if [ "$mode" = "empty_catalog" ]; then
        # 放行后才消费后续 wire；mcp/list 若错误出现则由下方分支立即失败。
        wait_for_control
      fi
      ;;
    *'"method":"session/list"'*)
      test "$cwd" = "$expected_scope_cwd" || exit 53
      case "$line" in
        *'"limit":'*|*'"_meta":'*) exit 59 ;;
      esac
      /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{"sessions":[{"sessionId":"sidecar-session","title":"来自 sidecar 的标题","updatedAt":"2026-08-14T00:00:00Z"},{"sessionId":"untitled-session","updatedAt":"2026-08-13T00:00:00Z"}],"nextCursor":"next-page"}}\n' "$id"
      ;;
    *'"method":"session/close"'*)
      case "$line" in
        *'"_meta":'*) exit 59 ;;
      esac
      /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id"
      ;;
    *'"method":"session/load"'*)
      load_count=$((load_count + 1))
      test "$cwd" = "$expected_scope_cwd" || exit 54
      /usr/bin/printf '%s' "$line" | /usr/bin/grep -q '"mcpServers":\[\]' || exit 55
      /usr/bin/printf '%s' "$line" | /usr/bin/grep -q '"_meta":{"modelId":"byok"}' || exit 60
      test "$session" = "$canonical_session" || exit 56
      if [ "$mode" = "load_fail" ]; then
        /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"error":{"code":-32004,"message":"not found"}}\n' "$id"
      elif [ "$mode" = "load_error" ]; then
        /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"error":{"code":-32001,"message":"temporary load failure"}}\n' "$id"
      elif [ "$mode" = "load_eof" ]; then
        /usr/bin/printf '%s\n' waiting > "$load_waiting"
        wait_for_control
        /usr/bin/printf '%s\n' exited >> "$exited"
        exit 0
      else
        if [ "$mode" = "load_late" ]; then
          /usr/bin/printf '%s\n' late > "$late_replay"
          wait_for_control
        fi
        if [ "$mode" = "load_queued_after_deadline" ]; then
          # 用当前 canonical session 的合法 live 事件阻塞慢 sink，令目标 replay 排队跨过 deadline。
          /usr/bin/printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"sidecar-session","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"阻塞事件"}},"_meta":{"promptId":"blocking-turn","eventId":"blocking-event"}}}'
        fi
        if [ "$mode" = "load_gate" ] && [ "$load_count" -eq 1 ] && [ ! -f "$control" ]; then
          # 当前 canonical session 的合法 live 事件占住慢 sink，再把旧 replay 排入 reader 队列。
          /usr/bin/printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"sidecar-session","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"阻塞事件"}},"_meta":{"promptId":"blocking-turn","eventId":"blocking-event"}}}'
          /usr/bin/printf '%s\n' waiting > "$load_waiting"
          wait_for_control
          /usr/bin/printf '%s\n' late > "$late_replay"
          /usr/bin/printf '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"%s","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"旧代迟到"}},"_meta":{"isReplay":true,"promptId":"old-turn","eventId":"old-generation-event"}}}\n' "$session"
        elif [ "$mode" = "load_gate" ] && [ -f "$control" ]; then
          # 第二代只返回空 replay，避免测试用慢 sink 再次跨过短 deadline。
          /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id"
          continue
        elif [ "$mode" = "load_cancel" ] && [ "$load_count" -eq 1 ] && [ ! -f "$control" ]; then
          /usr/bin/printf '%s\n' waiting > "$load_waiting"
          wait_for_control
          # 等待 Host 的 cancel notification 后才发送旧 load 结果，锁定命令先行。
          IFS= read -r cancel_line
          /usr/bin/printf '%s\n' late > "$late_replay"
          /usr/bin/printf '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"%s","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"取消旧 load"}},"_meta":{"isReplay":true,"promptId":"cancelled-old-turn","eventId":"cancelled-old-event"}}}\n' "$session"
        elif [ "$mode" = "load_hold_single" ] && [ "$load_count" -eq 1 ]; then
          /usr/bin/printf '%s\n' waiting > "$load_waiting"
          wait_for_control
        fi
        /usr/bin/printf '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"%s","update":{"sessionUpdate":"plan","entries":[]},"_meta":{"isReplay":true}}}\n' "$session"
        if [ "$mode" = "load_wrong_owner" ]; then
          /usr/bin/printf '%s\n' '{"jsonrpc":"2.0","id":999,"result":{}}'
        fi
        if [ "$mode" = "load_wrong_session" ]; then
          /usr/bin/printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"wrong-session","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"错配会话"}},"_meta":{"isReplay":true,"promptId":"wrong-turn","eventId":"wrong-session-event"}}}'
        fi
        /usr/bin/printf '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"%s","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"历史回答"}},"_meta":{"isReplay":true,"promptId":"historic-turn","eventId":"history-event"}}}\n' "$session"
        if [ "$mode" = "load_live_diagnostic" ]; then
          # 该 update 故意伪装成 live 未知通知，验证 Host 不把诊断送入产品或 transcript。
          /usr/bin/printf '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"%s","update":{"sessionUpdate":"plan","entries":[]}}}\n' "$session"
          /usr/bin/printf '%s\n' waiting > "$load_waiting"
          wait_for_control
        fi
        if [ "$mode" != "load_never" ]; then
          /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id"
          # response 已经写出，sidecar 明确标记本次 load 消费完成。
          /usr/bin/printf '%s\n' consumed >> "$load_completed"
        fi
      fi
      ;;
    *'"method":"_x.ai/mcp/list"'*)
      # mcp/list 只接受 fake 已知会话，不能把收到的 sessionId 写回 oracle。
      test "$session" = "$canonical_session" || exit 57
      case "$line" in
        *'"params":{"sessionId":"sidecar-session"}}') ;;
        *) exit 61 ;;
      esac
      case "$mode" in
        empty_catalog)
          # 空批准集绝不能启动 catalog；一旦收到即让 fake sidecar fail-fast。
          exit 62
          ;;
        mcp_extra)
          /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{"result":{"servers":[{"name":"unexpected","session":{"status":"ready","tools":[{"name":"writeback","enabled":true}]}}]}}}\n' "$id"
          ;;
        mcp_missing)
          /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{"result":{"servers":[{"name":"purelab","session":{"status":"unavailable","tools":[]}}]}}}\n' "$id"
          ;;
        mcp_error)
          /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"error":{"code":-32001,"message":"catalog failed"}}\n' "$id"
          ;;
        mcp_queued_after_deadline)
          # 先投递会触发同步 sink 的通知，再投递成功 catalog；处理通知期间跨过 catalog deadline。
          /usr/bin/printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"sidecar-session","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"catalog 阻塞事件"}},"_meta":{"promptId":"catalog-blocking","eventId":"catalog-blocking-event"}}}'
          /usr/bin/printf '%s\n' '{"jsonrpc":"2.0","id":'$id',"result":{"result":{"servers":[{"name":"purelab","session":{"status":"ready","tools":[{"name":"search_tracks","enabled":true}]}}]}}}'
          ;;
        mcp_late)
          # 先记录收到请求，再由测试显式放行响应，建立超时与迟到响应的因果屏障。
          /usr/bin/printf '%s\n' waiting > "$catalog_waiting"
          wait_for_catalog_release
          /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{"result":{"servers":[]}}}\n' "$id"
          /usr/bin/printf '%s\n' sent > "$catalog_response_sent"
          ;;
        mcp_never)
          # 永久不回复 catalog，但继续消费后续 prompt，模拟健康 stdio 上的 MCP 无响应。
          ;;
        noop_only)
          /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{"result":{"servers":[{"name":"builtin","session":{"status":"ready","tools":[{"name":"GrokBuild:efflab_noop","enabled":true}]}}]}}}\n' "$id"
          ;;
        *)
          /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{"result":{"servers":[]}}}\n' "$id"
          ;;
      esac
      ;;
    *'"method":"session/cancel"'*)
      /usr/bin/printf '%s\n' seen >> "$cancel_seen"
      if { [ "$mode" = "permission_after_cancel" ] || [ "$mode" = "permission_after_cancel_wrapper" ]; } && [ -n "$pending_prompt_id" ]; then
        /usr/bin/printf '{"jsonrpc":"2.0","id":900,"method":"%s","params":{"sessionId":"%s","toolCall":{"toolCallId":"tool-1","title":"GrokBuild:efflab_noop"},"options":[{"optionId":"allow-once","name":"Allow once","kind":"allow_once"},{"optionId":"reject-once","name":"Reject once","kind":"reject_once"},{"optionId":"enable-always-approve","name":"Always","kind":"allow_once"}]}}\n' "$permission_method" "$pending_permission_session"
      fi
      ;;
    *'"method":"session/prompt"'*)
      if [ "$mode" = "active_non_current_live" ]; then
        # 第一个 active session 已不是 current_session，但仍必须接收其 live update。
        /usr/bin/printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"active-session-a","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"active-non-current"}},"_meta":{"promptId":"active-live-turn","eventId":"active-non-current-event"}}}'
        # 未激活 session 没有 Host transcript；它不能污染 active session 的 hot resume。
        /usr/bin/printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"unactivated-live-session","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"unactivated-live"}},"_meta":{"promptId":"active-live-turn","eventId":"unactivated-live-event"}}}'
        /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id"
      elif [ "$mode" = "permission_after_cancel" ] || [ "$mode" = "permission_after_cancel_wrapper" ]; then
        pending_prompt_id="$id"
        pending_permission_session="$session"
      elif [ "$mode" = "permission" ] || [ "$mode" = "permission_wrapper" ] || [ "$mode" = "permission_unknown" ] || [ "$mode" = "permission_unknown_wrapper" ]; then
        pending_prompt_id="$id"
        title='GrokBuild:efflab_noop'
        if [ "$mode" = "permission_unknown" ] || [ "$mode" = "permission_unknown_wrapper" ]; then title='unexpected_tool'; fi
        /usr/bin/printf '{"jsonrpc":"2.0","id":900,"method":"%s","params":{"sessionId":"%s","toolCall":{"toolCallId":"tool-1","title":"%s"},"options":[{"optionId":"allow-once","name":"Allow once","kind":"allow_once"},{"optionId":"reject-once","name":"Reject once","kind":"reject_once"},{"optionId":"enable-always-approve","name":"Always","kind":"allow_once"}]}}\n' "$permission_method" "$session" "$title"
      elif [ "$mode" = "prompt_eof" ]; then
        prompt_id=$(/usr/bin/printf '%s\n' "$line" | /usr/bin/sed -n 's/.*"promptId":"\([^"]*\)".*/\1/p')
        /usr/bin/printf '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"%s","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"EOF 前的回答"}},"_meta":{"promptId":"%s","eventId":"eof-live-event"}}}\n' "$session" "$prompt_id"
        /usr/bin/printf '%s\n' exited >> "$exited"
        exit 0
      elif [ "$mode" = "notification_flood" ]; then
        prompt_id=$(/usr/bin/printf '%s\n' "$line" | /usr/bin/sed -n 's/.*"promptId":"\([^"]*\)".*/\1/p')
        index=0
        while [ "$index" -lt 40 ]; do
          /usr/bin/printf '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"%s","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"洪水事件"}},"_meta":{"promptId":"%s","eventId":"flood-%s"}}}\n' "$session" "$prompt_id" "$index"
          index=$((index + 1))
        done
      else
        /usr/bin/printf '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"%s","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"实时回答"}},"_meta":{"promptId":"%s","eventId":"live-event"}}}\n' "$session" "$(/usr/bin/printf '%s\n' "$line" | /usr/bin/sed -n 's/.*"promptId":"\([^"]*\)".*/\1/p')"
        if [ "$mode" = "hold_prompt" ]; then
          # 后台等待释放以保持 prompt in-flight，主循环继续消费 cancel 等控制消息。
          /usr/bin/printf '%s\n' waiting >> "$prompt_waiting"
          (
            wait_for_prompt_release
            /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id"
          ) &
        else
          /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id"
        fi
      fi
      ;;
  esac
done
/usr/bin/printf '%s\n' 'exited' >> "$exited"
"#;
    let script = script
        .replace("__MODE__", mode)
        .replace("__STARTED__", &shell_quote(started))
        .replace("__EXITED__", &shell_quote(exited))
        .replace("__CAPTURED__", &shell_quote(captured))
        .replace("__PROMPT_GATE__", &shell_quote(prompt_gate))
        .replace("__PROMPT_WAITING__", &shell_quote(prompt_waiting))
        .replace("__CANCEL_SEEN__", &shell_quote(cancel_seen))
        .replace("__CATALOG_GATE__", &shell_quote(catalog_gate))
        .replace("__CATALOG_WAITING__", &shell_quote(catalog_waiting))
        .replace(
            "__CATALOG_RESPONSE_SENT__",
            &shell_quote(catalog_response_sent),
        )
        .replace("__CONTROL__", &shell_quote(control))
        .replace("__CONTROL_GATE__", &shell_quote(control_gate))
        .replace("__LOAD_WAITING__", &shell_quote(load_waiting))
        .replace("__LOAD_COMPLETED__", &shell_quote(load_completed))
        .replace("__LATE_REPLAY__", &shell_quote(late_replay))
        .replace("__EXPECTED_SCOPE_ROOT__", &shell_quote(expected_scope_root));
    fs::write(sidecar, script).expect("必须能写入 fake sidecar");
    fs::set_permissions(sidecar, fs::Permissions::from_mode(0o700))
        .expect("fake sidecar 必须可执行");
}

/// 等待 child 留下非敏感启动或 wire 观察文件，避免测试依赖线程调度时序。
fn wait_for_file(path: &Path) {
    wait_until(|| path.exists());
}

/// 等待并返回当前完整 JSONL 前缀快照；最终 wire 断言必须先等待完成 marker。
fn read_complete_json_lines(path: &Path) -> Vec<Value> {
    let deadline = Instant::now() + TEST_TIMEOUT;
    loop {
        if let Some(wire) = try_read_complete_json_lines(path) {
            return wire;
        }
        assert!(
            Instant::now() < deadline,
            "等待完整 JSONL wire 超时: {}",
            path.display()
        );
        thread::yield_now();
    }
}

/// 读取瞬时完整前缀；半行或正在写入的 JSONL 文件暂不暴露给断言，不作为完成信号。
fn try_read_complete_json_lines(path: &Path) -> Option<Vec<Value>> {
    let source = fs::read_to_string(path).ok()?;
    if source.is_empty() || !source.ends_with('\n') {
        return None;
    }
    source
        .lines()
        .map(|line| serde_json::from_str(line).ok())
        .collect()
}

#[test]
fn jsonl_reader_rejects_partial_line_until_newline_is_present() {
    let temporary = tempfile::tempdir().expect("必须能创建 JSONL reader 测试目录");
    let path = temporary.path().join("wire.jsonl");
    fs::write(&path, "{\"jsonrpc\":\"2.0\"").expect("必须能写入半行 JSONL");
    assert!(try_read_complete_json_lines(&path).is_none());

    fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("必须能重新打开 JSONL")
        .write_all(b"}\n")
        .expect("必须能补齐 JSONL 行");
    assert_eq!(
        try_read_complete_json_lines(&path)
            .expect("完整换行 JSONL 才能被读取")
            .len(),
        1
    );
}

/// 等待指定数量的 sidecar generation 启动，避免用固定 sleep 猜测重启完成。
fn wait_for_started_count(harness: &Harness, expected: usize) {
    wait_until(|| {
        fs::read_to_string(&harness.started)
            .map(|started| started.lines().count() >= expected)
            .unwrap_or(false)
    });
}

/// 释放 fake sidecar 的 load barrier；标记文件保留跨 generation 状态，FIFO 负责同步放行。
fn release_load(harness: &Harness) {
    fs::write(&harness.control, "release").expect("必须能记录 fake load barrier 已释放");
    release_fifo(&harness.control_gate);
}

/// 向阻塞在读端的 FIFO 写入释放信号，不依赖固定 wall-clock sleep。
fn release_fifo(path: &Path) {
    let mut writer = OpenOptions::new()
        .write(true)
        .open(path)
        .expect("测试 FIFO 读端必须已经就绪");
    writer.write_all(b"release\n").expect("必须能释放测试 FIFO");
}

/// 创建只供当前 Unix 测试进程使用的 FIFO。
fn create_fifo(path: &Path) {
    let c_path = CString::new(path.as_os_str().as_bytes()).expect("临时 FIFO 路径不能含 NUL");
    // SAFETY: c_path 指向当前进程创建的临时路径，mkfifo 不借用 Rust 内存之外的数据。
    let result = unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) };
    assert_eq!(result, 0, "必须能创建测试 FIFO: {}", path.display());
}

/// 等待 fake sidecar 已进入 load barrier，确保后续命令属于同一 LoadFlight。
fn wait_for_load_waiting(harness: &Harness) {
    let deadline = Instant::now() + TEST_TIMEOUT;
    while !harness.load_waiting.exists() {
        if Instant::now() >= deadline {
            panic!("等待 load barrier 超时");
        }
        thread::yield_now();
    }
}

/// 等待 fake sidecar 写出 load 消费完成 marker，再读取最终 wire 快照。
fn wait_for_load_completed(harness: &Harness, expected: usize) {
    wait_until(|| marker_line_count(&harness.load_completed) >= expected);
}

/// 等待任意 session-level Error，再由调用方断言稳定错误分类。
fn wait_for_any_session_error(harness: &Harness) -> KitProductEvent {
    let deadline = Instant::now() + TEST_TIMEOUT;
    loop {
        let events = harness.events();
        if let Some(event) = events
            .iter()
            .find(|event| matches!(&event.block, KitBlock::Error(_)))
            .cloned()
        {
            return event;
        }
        if Instant::now() >= deadline {
            panic!("等待 dispatch loop 异步结果超时");
        }
        thread::yield_now();
    }
}

/// 判断事件是否包含可恢复 transcript 的回合内容。
fn is_recoverable_content(event: &KitProductEvent) -> bool {
    matches!(
        &event.block,
        KitBlock::User { .. }
            | KitBlock::Assistant { .. }
            | KitBlock::Thinking { .. }
            | KitBlock::Tool { .. }
    )
}

/// 在固定上限内轮询异步 actor 的可观察结果。
fn wait_until(condition: impl Fn() -> bool) {
    let deadline = Instant::now() + TEST_TIMEOUT;
    while !condition() {
        assert!(Instant::now() < deadline, "等待 dispatch loop 异步结果超时");
        thread::yield_now();
    }
}

/// 等待某种 session/process Status 出现在产品 sink。
fn wait_for_status(harness: &Harness, code: &str) -> KitProductEvent {
    wait_until(|| {
        harness.events().iter().any(
            |event| matches!(&event.block, KitBlock::Status { code: actual, .. } if actual == code),
        )
    });
    harness
        .events()
        .into_iter()
        .find(
            |event| matches!(&event.block, KitBlock::Status { code: actual, .. } if actual == code),
        )
        .expect("等待条件已证明状态事件存在")
}

/// 构造冻结的 Send 命令，便于覆盖 prompt、幂等和取消时序。
fn send(
    session_id: &str,
    submission_id: &str,
    text: &str,
    mentions: Option<Vec<MentionId>>,
) -> KitCommand {
    KitCommand::Send {
        scope_id: "scope-a".to_string(),
        session_id: session_id.to_string(),
        submission_id: submission_id.to_string(),
        text: text.to_string(),
        mentions,
    }
}

/// 断言同步 Send 回执的关键字段，避免遗漏 accepted / duplicate / prompt id 契约。
fn assert_send(reply: KitReply, duplicate: bool, session_id: &str, submission_id: &str) {
    assert_eq!(
        reply,
        KitReply::Send {
            accepted: true,
            duplicate,
            session_id: session_id.to_string(),
            turn_id: submission_id.to_string(),
            submission_id: submission_id.to_string(),
        }
    );
}

/// 轮询前一 prompt 的 in-flight 状态，避免依赖 fake sidecar 的固定 wall-clock sleep。
fn send_after_inflight_finishes(
    harness: &Harness,
    session_id: &str,
    submission_id: &str,
    text: &str,
) -> KitReply {
    let deadline = Instant::now() + TEST_TIMEOUT;
    loop {
        match harness
            .runtime
            .dispatch(send(session_id, submission_id, text, None))
        {
            Ok(reply) => return reply,
            Err(error) if error.code == "turn_in_progress" => {
                assert!(
                    Instant::now() < deadline,
                    "等待前一 prompt 完成时超出测试时限"
                );
                thread::yield_now();
            }
            Err(error) => panic!("等待前一 prompt 完成时收到意外错误: {}", error.code),
        }
    }
}

/// TC-LAUNCH / TC-HP：真实 child 只会在 Host 完成 L3b、token 和 TOML 前置后启动。
#[test]
fn launch_handshake_new_session_skips_empty_mcp_catalog_and_keeps_stdio_wired() {
    let harness = Harness::configured("empty_catalog", [], Duration::from_secs(60));
    let session_id = harness.new_session("scope-a");

    assert_eq!(session_id, "sidecar-session");
    wait_for_file(&harness.started);
    let home = harness
        ._temporary
        .path()
        .join("app-data/dispatch-loop-test/scope-a/home");
    let config = fs::read_to_string(home.join("runtime-config.v1.toml"))
        .expect("Host 必须在 fake sidecar spawn 前写入 runtime-config.v1.toml");
    assert!(config.contains("schema_version = 1"));
    assert!(config.contains("backend = \"chat_completions\""));
    assert!(config.contains("token_env = \"EFFLAB_L3B_BIND\""));

    let expected_session_cwd = fs::canonicalize(harness._temporary.path())
        .expect("测试临时根必须能 canonicalize")
        .join("app-data/dispatch-loop-test/scope-a/workspace");

    // fake sidecar 在 session/new 后以 FIFO 放行；若错误发送 mcp/list，则立即失败退出。
    release_fifo(&harness.control_gate);
    assert_send(
        harness
            .runtime
            .dispatch(send(&session_id, "launch-turn", "你好", None))
            .expect("空 MCP catalog 不得阻断 prompt"),
        false,
        &session_id,
        "launch-turn",
    );
    // 回合终态是后续正向完成 marker；JSONL 读取只提供当前完整前缀快照。
    wait_for_status(&harness, "turn_completed");

    let wire = harness.wire();
    assert_session_wire_contract(&wire, &session_id, &expected_session_cwd, None);
    let initialize = wire
        .iter()
        .find(|item| item["method"] == "initialize")
        .expect("必须先发送 initialize");
    assert_eq!(
        initialize["params"]["clientCapabilities"],
        json!({
            "fs": { "readTextFile": false, "writeTextFile": false },
            "terminal": false,
        })
    );
    assert_eq!(
        initialize["params"]["clientInfo"]["name"],
        json!("efflab-agent-host")
    );
    assert!(
        initialize["params"].get("_meta").is_none(),
        "initialize 不得携带任何 _meta"
    );

    let new_session = wire
        .iter()
        .find(|item| item["method"] == "session/new")
        .expect("initialize 成功后必须发送 session/new");
    assert_eq!(new_session["params"]["mcpServers"], json!([]));
    assert_eq!(new_session["params"]["_meta"], json!({ "modelId": "byok" }));
    assert_eq!(
        new_session["params"]["cwd"].as_str(),
        expected_session_cwd.to_str(),
        "session/new cwd 必须精确等于当前 scope 的隔离 workspace"
    );
}

/// initialize 缺少 Efflab runtime metadata 时，deferred command 必须收到失败回执且不得继续发会话请求。
#[test]
fn initialize_missing_runtime_metadata_rejects_deferred_command() {
    let harness = Harness::configured("initialize_missing_meta", [], Duration::from_secs(60));
    let error = harness
        .runtime
        .dispatch(KitCommand::NewSession {
            scope_id: "scope-a".to_string(),
            client_request_id: None,
        })
        .expect_err("缺少 initialize metadata 必须 fail-closed");
    assert_eq!(error.code, "sidecar_unavailable");

    let wire = harness.wire();
    assert!(
        wire.iter()
            .all(|request| !matches!(request.get("method"), Some(Value::String(method)) if matches!(method.as_str(), "session/new" | "session/list" | "session/load" | "session/prompt"))),
        "握手失败后不得发送任何会话或 prompt 请求"
    );
}

/// initialize 返回不兼容的 ACP 协议版本时，deferred session 命令必须被拒绝。
#[test]
fn initialize_wrong_protocol_version_rejects_deferred_command() {
    let harness = Harness::configured("initialize_wrong_protocol", [], Duration::from_secs(60));
    let error = harness
        .runtime
        .dispatch(KitCommand::NewSession {
            scope_id: "scope-a".to_string(),
            client_request_id: None,
        })
        .expect_err("错误 initialize protocolVersion 必须 fail-closed");
    assert_eq!(error.code, "sidecar_unavailable");

    let wire = harness.wire();
    assert!(
        wire.iter().all(|request| !matches!(
            request.get("method"),
            Some(Value::String(method))
                if matches!(
                    method.as_str(),
                    "session/new" | "session/list" | "session/load" | "session/prompt"
                )
        )),
        "协议版本握手失败后不得发送任何会话或 prompt 请求"
    );
    wait_for_file(&harness.exited);
}

/// initialize metadata 的值或 JSON 类型错误时，deferred command 必须被拒绝且 scope 进入 cleanup。
#[test]
fn initialize_wrong_runtime_metadata_rejects_deferred_command() {
    let harness = Harness::configured("initialize_wrong_meta", [], Duration::from_secs(60));
    let error = harness
        .runtime
        .dispatch(KitCommand::ListSessions {
            scope_id: "scope-a".to_string(),
            cursor: None,
        })
        .expect_err("错误 initialize metadata 必须 fail-closed");
    assert_eq!(error.code, "sidecar_unavailable");

    let wire = harness.wire();
    assert!(
        wire.iter()
            .all(|request| !matches!(request.get("method"), Some(Value::String(method)) if matches!(method.as_str(), "session/new" | "session/list" | "session/load" | "session/prompt"))),
        "错误握手后不得发送任何会话或 prompt 请求"
    );
    wait_for_file(&harness.exited);
}

/// initialize metadata 出现未声明字段时，闭集握手必须拒绝 deferred command。
#[test]
fn initialize_extra_runtime_metadata_rejects_deferred_command() {
    let harness = Harness::configured("initialize_extra_meta", [], Duration::from_secs(60));
    let error = harness
        .runtime
        .dispatch(KitCommand::NewSession {
            scope_id: "scope-a".to_string(),
            client_request_id: None,
        })
        .expect_err("额外 initialize metadata 必须按形状错误 fail-closed");
    assert_eq!(error.code, "sidecar_unavailable");

    let wire = harness.wire();
    assert!(
        wire.iter()
            .all(|request| !matches!(request.get("method"), Some(Value::String(method)) if matches!(method.as_str(), "session/new" | "session/list" | "session/load" | "session/prompt"))),
        "握手形状错误后不得发送任何会话或 prompt 请求"
    );
    wait_for_file(&harness.exited);
}

/// initialize metadata 完整匹配最小 Host 合同时，deferred command 才能继续执行 session/new。
#[test]
fn initialize_correct_runtime_metadata_allows_deferred_command() {
    let harness = Harness::configured("initialize_correct_meta", [], Duration::from_secs(60));
    assert_eq!(harness.new_session("scope-a"), "sidecar-session");
    assert_eq!(harness.method_count("session/new"), 1);
}

/// 非法能力或认证声明必须拒绝握手，并清空所有尚未执行的会话命令。
#[test]
fn initialize_illegal_capabilities_or_auth_rejects_deferred_commands() {
    for mode in [
        "initialize_invalid_capabilities",
        "initialize_invalid_auth_methods",
    ] {
        let harness = Harness::configured(mode, [], Duration::from_secs(60));
        let error = harness
            .runtime
            .dispatch(KitCommand::NewSession {
                scope_id: "scope-a".to_string(),
                client_request_id: None,
            })
            .expect_err("非法 initialize capability/auth 声明必须 fail-closed");
        assert_eq!(error.code, "sidecar_unavailable", "模式 {mode} 必须不可用");

        let wire = harness.wire();
        assert!(
            wire.iter().all(|request| !matches!(
                request.get("method"),
                Some(Value::String(method))
                    if matches!(
                        method.as_str(),
                        "session/new" | "session/list" | "session/load" | "session/prompt"
                    )
            )),
            "非法握手模式 {mode} 不得继续发送会话或 prompt 请求"
        );
        wait_for_file(&harness.exited);
    }
}

/// 无 sessionId 的未知 sidecar 通知只能被 actor 安全跳过，后续 initialize/new session 仍必须完成。
#[test]
fn actor_continues_after_unattributed_unknown_notification() {
    let harness = Harness::configured("unknown_without_session", [], Duration::from_secs(60));

    assert_eq!(harness.new_session("scope-a"), "sidecar-session");
    harness.wait_for_method("session/new");
    assert!(
        harness.events().is_empty(),
        "无法归属的未知通知不得伪造产品事件或中断初始化"
    );
}

/// TC-SEND / TC-TURN：prompt 写入后必须立即回执，同时同 session 只允许一个 in-flight turn。
#[test]
fn send_returns_before_prompt_result_projects_events_and_rejects_parallel_turn() {
    let harness = Harness::configured("hold_prompt", [], Duration::from_secs(60));
    let session_id = harness.new_session("scope-a");

    let start = Instant::now();
    assert_send(
        harness
            .runtime
            .dispatch(send(&session_id, "turn-one", "第一轮", None))
            .expect("prompt 写入后必须立即得到 Send 回执"),
        false,
        &session_id,
        "turn-one",
    );
    assert!(
        start.elapsed() < DELAYED_PROMPT_RESULT / 2,
        "Send 不得等待 session/prompt result"
    );
    harness.wait_for_method("session/prompt");
    harness.wait_for_prompt(1);

    let error = harness
        .runtime
        .dispatch(send(&session_id, "turn-two", "并发轮", None))
        .expect_err("同一 session 的第二轮 prompt 必须被拒绝");
    assert_eq!(error.code, "turn_in_progress");

    harness.release_prompt();
    wait_for_status(&harness, "turn_completed");
    assert!(harness.events().iter().any(|event| {
        matches!(
            &event.block,
            KitBlock::Assistant { markdown, streaming } if markdown == "实时回答" && *streaming
        ) && event.turn_id.as_deref() == Some("turn-one")
    }));
}

/// active session 即使不是最近 new/load 的 current_session，也必须接收 live update 并支持 hot resume。
#[test]
fn live_update_for_active_non_current_session_is_projected_and_hot_resumable() {
    let harness = Harness::configured("active_non_current_live", [], Duration::from_secs(60));
    let active_session = harness.new_session("scope-a");
    let current_session = harness.new_session("scope-a");
    assert_eq!(active_session, "active-session-a");
    assert_eq!(current_session, "active-session-b");

    assert_send(
        harness
            .runtime
            .dispatch(send(
                &active_session,
                "active-live-turn",
                "校验 active 会话",
                None,
            ))
            .expect("active session 的 prompt 必须被接受"),
        false,
        &active_session,
        "active-live-turn",
    );
    harness.wait_for_method("session/prompt");
    wait_for_status(&harness, "turn_completed");
    wait_until(|| {
        harness.events().iter().any(|event| {
            event.session_id == active_session && event.event_id == "active-non-current-event"
        })
    });

    // 未激活 session 的 live update 可按自身归属投影，但不得进入 active session 的 transcript。
    let events = harness.events();
    assert!(
        events.iter().any(|event| {
            event.session_id == "unactivated-live-session"
                && event.event_id == "unactivated-live-event"
                && event.origin == Origin::Live
        }),
        "未激活 session 的 live update 应保持自身会话归属，实际事件: {events:?}"
    );

    assert!(
        events.iter().any(|event| {
            event.session_id == active_session
                && event.event_id == "active-non-current-event"
                && matches!(
                    &event.block,
                    KitBlock::Assistant { markdown, .. } if markdown == "active-non-current"
                )
        }),
        "非 current 但 active session 的 live update 必须进入产品事件，实际事件: {events:?}"
    );

    // active session 的 transcript 必须可被 hot resume 重放，且不应触发冷 session/load。
    assert_eq!(
        harness.resume(&active_session),
        KitReply::ResumeSession {
            accepted: true,
            session_id: active_session.clone(),
        }
    );
    wait_until(|| harness.count_events_with_code("replay_complete") >= 1);
    let events = harness.events();
    assert!(
        events.iter().any(|event| {
            event.origin == Origin::Replay
                && event.session_id == active_session
                && event.event_id == "active-non-current-event"
        }),
        "active session 的 live transcript 必须可 hot resume 重放，实际事件: {events:?}"
    );
    assert!(
        events.iter().all(|event| {
            !(event.origin == Origin::Replay && event.event_id == "unactivated-live-event")
        }),
        "未激活 session 的 live update 不得进入任何 hot-resume replay transcript，实际事件: {events:?}"
    );
    assert_eq!(harness.method_count("session/load"), 0);
}

/// TC-NOKEY：未配置时绝不能启动 L3b 或 sidecar；设置页读取仍必须成功。
#[test]
fn unconfigured_channel_rejects_all_conversation_commands_without_spawning() {
    let (runtime, _temporary, home_root) = Harness::unconfigured();
    let commands = [
        KitCommand::GetCapability,
        send("session-a", "submission-a", "hello", None),
        send(
            "session-a",
            "submission-with-mentions",
            "hello",
            Some(vec![MentionId {
                kind: "track".to_string(),
                id: "track-1".to_string(),
            }]),
        ),
        KitCommand::NewSession {
            scope_id: "scope-a".to_string(),
            client_request_id: None,
        },
        KitCommand::ListSessions {
            scope_id: "scope-a".to_string(),
            cursor: None,
        },
        KitCommand::ResumeSession {
            scope_id: "scope-a".to_string(),
            session_id: "session-a".to_string(),
        },
        KitCommand::DeleteSession {
            scope_id: "scope-a".to_string(),
            session_id: "session-a".to_string(),
        },
        KitCommand::Cancel {
            scope_id: "scope-a".to_string(),
            session_id: "session-a".to_string(),
        },
    ];

    for command in commands {
        let error = runtime
            .dispatch(command)
            .expect_err("未配置 Channel 的对话命令必须失败");
        assert_eq!(error.code, "llm_channel_unconfigured");
    }
    assert_eq!(
        runtime
            .dispatch(KitCommand::GetLlmChannelView)
            .expect("未配置时设置页仍必须能读取 view"),
        KitReply::LlmChannelView {
            channel: Default::default(),
        }
    );
    assert!(
        !home_root.exists(),
        "未配置时不得创建 sidecar home 或监听路径"
    );
}

/// Host Kit 入口必须在 SubmissionMap、actor 和 sidecar 之前复用 promptId 的共享校验。
#[test]
fn invalid_submission_ids_have_no_lifecycle_side_effects_before_valid_send() {
    let invalid_ids = vec![
        String::new(),
        "control\nid".to_string(),
        "a".repeat(1025),
        "é".repeat(513),
    ];

    // 已建立 actor：非法 ID 不能轮换 child、写 ACP 或新增产品生命周期事件。
    let established = Harness::configured("default", [], Duration::from_secs(60));
    let established_session = established.new_session("scope-a");
    let established_before = established.lifecycle_snapshot();
    for submission_id in &invalid_ids {
        let error = established
            .runtime
            .dispatch(send(
                &established_session,
                submission_id,
                "非法标识不得写入",
                None,
            ))
            .expect_err("非法 submission_id 必须在进入 SubmissionMap 前拒绝");
        assert_eq!(error.code, "invalid_request");
    }
    assert_eq!(
        established.lifecycle_snapshot(),
        established_before,
        "已建立 actor 的非法 submission_id 不得改变 child、ACP 或生命周期快照"
    );
    assert_send(
        established
            .runtime
            .dispatch(send(
                &established_session,
                "valid-submission-established",
                "合法标识仍可发送",
                None,
            ))
            .expect("拒绝非法标识后，已建立 actor 的合法 Send 仍应可用"),
        false,
        &established_session,
        "valid-submission-established",
    );
    established.wait_for_method("session/prompt");

    // 未建立 actor：非法 ID 不能启动首个 child；随后合法 Send 仍可冷恢复并发送。
    let unestablished = Harness::configured("default", [], Duration::from_secs(60));
    let unestablished_before = unestablished.lifecycle_snapshot();
    for submission_id in &invalid_ids {
        let error = unestablished
            .runtime
            .dispatch(send(
                CANONICAL_SESSION_ID,
                submission_id,
                "非法标识不得启动 actor",
                None,
            ))
            .expect_err("未建立 actor 时非法 submission_id 也必须提前拒绝");
        assert_eq!(error.code, "invalid_request");
    }
    assert_eq!(
        unestablished.lifecycle_snapshot(),
        unestablished_before,
        "未建立 actor 的非法 submission_id 不得启动 child、写 ACP 或产生生命周期事件"
    );
    assert_send(
        unestablished
            .runtime
            .dispatch(send(
                CANONICAL_SESSION_ID,
                "valid-submission-unestablished",
                "非法标识后仍可冷恢复发送",
                None,
            ))
            .expect("拒绝非法标识后，未建立 actor 的合法 Send 仍应可用"),
        false,
        CANONICAL_SESSION_ID,
        "valid-submission-unestablished",
    );
    unestablished.wait_for_method("session/prompt");
}

/// Send 的 mention 必须由产品端口展开为中文文本，不能把不透明 id 直接交给 sidecar。
#[test]
fn send_mentions_resolve_to_chinese_text_before_prompt_is_written() {
    let harness = Harness::configured_with_mentions(
        "default",
        [],
        MentionMode::Resolve,
        Duration::from_secs(60),
    );
    let session_id = harness.new_session("scope-a");
    let mentions = vec![
        MentionId {
            kind: "track".to_string(),
            id: "白日梦".to_string(),
        },
        MentionId {
            kind: "track".to_string(),
            id: "夜航".to_string(),
        },
    ];

    assert_send(
        harness
            .runtime
            .dispatch(send(
                &session_id,
                "mention-expanded",
                "请分析这些曲目",
                Some(mentions),
            ))
            .expect("可解析 mention 必须写入 prompt"),
        false,
        &session_id,
        "mention-expanded",
    );
    harness.wait_for_method("session/prompt");

    let prompt = harness
        .wire()
        .into_iter()
        .find(|item| item["method"] == "session/prompt")
        .expect("必须写出 session/prompt");
    let prompt_text = prompt["params"]["prompt"][0]["text"]
        .as_str()
        .expect("prompt 文本必须是字符串");
    assert!(prompt_text.contains("请分析这些曲目"));
    assert!(prompt_text.contains("曲目：白日梦；艺人：测试艺人"));
    assert!(prompt_text.contains("曲目：夜航；艺人：测试艺人"));
}

/// 原始文本虽在能力上限内，mention 展开后超过上限时也不能写入 sidecar。
#[test]
fn mention_expansion_over_max_prompt_chars_is_invalid_request() {
    let harness = Harness::configured_with_mentions(
        "default",
        [],
        MentionMode::Text("曲".to_string()),
        Duration::from_secs(60),
    );
    let text = "a".repeat(32_000);
    let error = harness
        .runtime
        .dispatch(send(
            CANONICAL_SESSION_ID,
            "overlong-mention-expansion",
            &text,
            Some(vec![MentionId {
                kind: "track".to_string(),
                id: "track-1".to_string(),
            }]),
        ))
        .expect_err("完整 prompt 超过能力上限必须失败关闭");

    assert_eq!(error.code, "invalid_request");
    assert!(
        !harness.started.exists(),
        "完整 prompt 超限时不得启动 sidecar"
    );
}

/// 合法中文标题中的斜杠不是绝对路径，Host 不得因 ASCII-only 边界误拒。
#[test]
fn mention_expansion_allows_chinese_title_with_slash() {
    let expansion = "曲目：天地/人；艺人：甲";
    let harness = Harness::configured_with_mentions(
        "default",
        [],
        MentionMode::Text(expansion.to_string()),
        Duration::from_secs(60),
    );
    let session_id = harness.new_session("scope-a");

    assert_send(
        harness
            .runtime
            .dispatch(send(
                &session_id,
                "chinese-slash-title",
                "请分析曲目",
                Some(vec![MentionId {
                    kind: "track".to_string(),
                    id: "track-1".to_string(),
                }]),
            ))
            .expect("合法中文斜杠标题必须能写入 prompt"),
        false,
        &session_id,
        "chinese-slash-title",
    );
    harness.wait_for_method("session/prompt");
    let prompt = harness
        .wire()
        .into_iter()
        .find(|item| item["method"] == "session/prompt")
        .expect("必须写出 session/prompt");
    assert!(
        prompt["params"]["prompt"][0]["text"]
            .as_str()
            .is_some_and(|text| text.contains(expansion)),
        "完整 prompt 必须保留合法中文标题"
    );
}

/// 尾随 `@` 不会形成 grok-shell 文件引用，应与 Task 3 的文本门保持一致。
#[test]
fn mention_expansion_allows_trailing_at() {
    let expansion = "曲目：尾随符号@";
    let harness = Harness::configured_with_mentions(
        "default",
        [],
        MentionMode::Text(expansion.to_string()),
        Duration::from_secs(60),
    );
    let session_id = harness.new_session("scope-a");

    assert_send(
        harness
            .runtime
            .dispatch(send(
                &session_id,
                "trailing-at-title",
                "请分析曲目",
                Some(vec![MentionId {
                    kind: "track".to_string(),
                    id: "track-1".to_string(),
                }]),
            ))
            .expect("尾随 @ 展示文本必须能写入 prompt"),
        false,
        &session_id,
        "trailing-at-title",
    );
    harness.wait_for_method("session/prompt");
    let prompt = harness
        .wire()
        .into_iter()
        .find(|item| item["method"] == "session/prompt")
        .expect("必须写出 session/prompt");
    assert!(
        prompt["params"]["prompt"][0]["text"]
            .as_str()
            .is_some_and(|text| text.contains(expansion)),
        "完整 prompt 必须保留合法尾随 @"
    );
}

/// 未声明 mention 端口时，即使 Channel 已配置也必须在创建 actor 前拒绝请求。
#[test]
fn send_mentions_without_port_is_invalid_request() {
    let harness = Harness::configured("default", [], Duration::from_secs(60));
    let error = harness
        .runtime
        .dispatch(send(
            CANONICAL_SESSION_ID,
            "unsupported-mentions",
            "请处理曲目",
            Some(vec![MentionId {
                kind: "track".to_string(),
                id: "track-1".to_string(),
            }]),
        ))
        .expect_err("未声明 mention 端口必须失败关闭");

    assert_eq!(error.code, "invalid_request");
    assert!(
        !harness.started.exists(),
        "无 mention 端口时不得启动 sidecar"
    );
}

/// 产品端口拒绝未知或跨 scope 标识时，Host 只能返回不泄漏底层信息的 invalid_request。
#[test]
fn mention_resolution_failure_is_invalid_request() {
    let harness = Harness::configured_with_mentions(
        "default",
        [],
        MentionMode::Reject,
        Duration::from_secs(60),
    );
    let error = harness
        .runtime
        .dispatch(send(
            CANONICAL_SESSION_ID,
            "rejected-mentions",
            "请处理曲目",
            Some(vec![MentionId {
                kind: "track".to_string(),
                id: "other-scope-track".to_string(),
            }]),
        ))
        .expect_err("未知或跨 scope mention 必须失败关闭");

    assert_eq!(error.code, "invalid_request");
    assert!(!harness.started.exists(), "解析失败时不得启动 sidecar");
}

/// Host 对产品展开文本保留最终门禁，不能让路径或 grok-shell 文件引用绕过原始文本校验。
#[test]
fn unsafe_mention_expansions_are_invalid_requests() {
    for (name, unsafe_text) in [
        ("at_file", "曲目：@secret.txt"),
        ("file_uri", "曲目：file:///private/secret.wav"),
        ("absolute_path", "曲目：/private/secret.wav"),
    ] {
        let harness = Harness::configured_with_mentions(
            "default",
            [],
            MentionMode::Text(unsafe_text.to_string()),
            Duration::from_secs(60),
        );
        let error = harness
            .runtime
            .dispatch(send(
                CANONICAL_SESSION_ID,
                &format!("unsafe-mention-{name}"),
                "请处理曲目",
                Some(vec![MentionId {
                    kind: "track".to_string(),
                    id: "track-1".to_string(),
                }]),
            ))
            .expect_err("不安全展开文本必须被 Host 拒绝");

        assert_eq!(error.code, "invalid_request", "用例 {name} 应失败关闭");
        assert!(
            !harness.started.exists(),
            "用例 {name} 在文本门禁前不得启动 sidecar"
        );
    }
}

/// TC-IDEMP / TC-CANCEL：稳定 submission 指纹、无 id cancel notification 与取消竞态。
#[test]
fn idempotency_mentions_and_cancel_keep_prompt_wire_and_inflight_state_correct() {
    let harness = Harness::configured_with_mentions(
        "hold_prompt",
        [],
        MentionMode::Resolve,
        Duration::from_secs(60),
    );
    let session_id = harness.new_session("scope-a");
    let mentions = vec![
        MentionId {
            kind: "track".to_string(),
            id: "1".to_string(),
        },
        MentionId {
            kind: "album".to_string(),
            id: "2".to_string(),
        },
    ];

    assert_send(
        harness
            .runtime
            .dispatch(send(
                &session_id,
                "same-submission",
                "相同文本",
                Some(mentions.clone()),
            ))
            .expect("首次 submission 必须被接受"),
        false,
        &session_id,
        "same-submission",
    );
    // Send 的回执只保证 Host stdin 已写入；等待 fake 消费该行，避免把 shell 调度竞态
    // 误判为重复 submission 又写了一次 prompt。
    harness.wait_for_method("session/prompt");
    harness.wait_for_prompt(1);
    assert_send(
        harness
            .runtime
            .dispatch(send(
                &session_id,
                "same-submission",
                "相同文本",
                Some(vec![mentions[1].clone(), mentions[0].clone()]),
            ))
            .expect("排序不同的 mentions 必须命中同一幂等提交"),
        true,
        &session_id,
        "same-submission",
    );
    assert_eq!(
        harness.method_count("session/prompt"),
        1,
        "幂等命中不得二次写 prompt"
    );

    let conflict = harness
        .runtime
        .dispatch(send(
            &session_id,
            "same-submission",
            "相同文本",
            Some(vec![MentionId {
                kind: "track".to_string(),
                id: "different".to_string(),
            }]),
        ))
        .expect_err("mentions 集合变化必须 fail-closed");
    assert_eq!(conflict.code, "fingerprint_conflict");

    assert_eq!(
        harness
            .runtime
            .dispatch(KitCommand::Cancel {
                scope_id: "scope-a".to_string(),
                session_id: session_id.clone(),
            })
            .expect("已发 prompt 的 cancel 必须立即回执"),
        KitReply::Cancel { accepted: true }
    );
    // prompt result 到达前 in-flight 保持；先发第二条命令，不能让 fake shell 已输出的
    // result 与文件观察之间的调度窗口掩盖该竞态。
    let busy = harness
        .runtime
        .dispatch(send(&session_id, "while-cancelled", "仍在等待结果", None))
        .expect_err("cancel 后仍须等待 prompt result 清除 in-flight");
    assert_eq!(busy.code, "turn_in_progress");
    harness.wait_for_method("session/cancel");
    let cancel_wire = harness
        .wire()
        .into_iter()
        .find(|item| item["method"] == "session/cancel")
        .expect("必须写出 session/cancel notification");
    assert!(
        cancel_wire.get("id").is_none(),
        "cancel 绝不能分配 JSON-RPC id"
    );
    wait_for_status(&harness, "cancelled");
    harness.release_prompt();
    assert_send(
        send_after_inflight_finishes(&harness, &session_id, "after-result", "结果后新轮"),
        false,
        &session_id,
        "after-result",
    );

    // 第二轮 prompt 也由 FIFO 明确放行，等待真实 response 清除 in-flight。
    harness.wait_for_prompt(2);
    harness.release_prompt();
    wait_for_status(&harness, "turn_completed");
    // 无 in-flight 的 cancel 会被下一次 Send 消费：不得向 sidecar 写 prompt，仍要发 cancelled。
    harness
        .runtime
        .dispatch(KitCommand::Cancel {
            scope_id: "scope-a".to_string(),
            session_id: session_id.clone(),
        })
        .expect("预先 cancel 必须被接受");
    let prompt_count = harness.method_count("session/prompt");
    assert_send(
        harness
            .runtime
            .dispatch(send(&session_id, "pre-cancel", "不应写入", None))
            .expect("被预先 cancel 的提交仍必须得到稳定回执"),
        false,
        &session_id,
        "pre-cancel",
    );
    wait_until(|| {
        harness.events().iter().any(|event| {
            matches!(&event.block, KitBlock::Status { code, .. } if code == "cancelled")
                && event.turn_id.as_deref() == Some("pre-cancel")
                && event.submission_id.as_deref() == Some("pre-cancel")
        })
    });
    assert_eq!(
        harness.method_count("session/prompt"),
        prompt_count,
        "预先 cancel 后不得写 prompt"
    );
    wait_until(|| {
        harness
            .events()
            .iter()
            .filter(|event| {
                matches!(&event.block, KitBlock::Status { code, .. } if code == "cancelled")
                    && event.turn_id.as_deref() == Some("pre-cancel")
                    && event.submission_id.as_deref() == Some("pre-cancel")
            })
            .count()
            == 1
    });
    assert_eq!(
        harness
            .events()
            .iter()
            .filter(|event| {
                matches!(&event.block, KitBlock::Status { code, .. } if code == "cancelled")
                    && event.turn_id.as_deref() == Some("pre-cancel")
                    && event.submission_id.as_deref() == Some("pre-cancel")
            })
            .count(),
        1,
        "重复 pre-cancel marker 只能结算一个 cancelled terminal"
    );
}

/// TC-LIST：只依赖标准 session/list 的 sidecar 返回值，绝不扫描本地 session 文件。
#[test]
fn list_waits_for_acp_result_uses_scope_cwd_and_exposes_only_four_summary_fields() {
    let harness = Harness::configured("basic", [], Duration::from_secs(60));
    let session_id = harness.new_session("scope-a");
    let reply = harness
        .runtime
        .dispatch(KitCommand::ListSessions {
            scope_id: "scope-a".to_string(),
            cursor: Some("cursor-1".to_string()),
        })
        .expect("ListSessions 必须等待 sidecar result");

    match reply {
        KitReply::ListSessions {
            sessions,
            next_cursor,
        } => {
            assert_eq!(next_cursor.as_deref(), Some("next-page"));
            assert_eq!(sessions.len(), 2);
            assert_eq!(sessions[0].session_id, session_id);
            assert_eq!(sessions[0].title, "来自 sidecar 的标题");
            assert!(
                sessions[0].is_active,
                "Host 已 attach 的 session 必须标为 active"
            );
            assert_eq!(
                sessions[1].title, "",
                "缺 title 时 Host 不得猜测用户首条文本"
            );
            let json = serde_json::to_value(KitReply::ListSessions {
                sessions,
                next_cursor,
            })
            .expect("List reply 必须可序列化");
            assert_eq!(
                json["sessions"][0]
                    .as_object()
                    .expect("Kit session 摘要必须是对象")
                    .len(),
                4,
                "Kit 摘要只能暴露四个冻结字段"
            );
            assert!(
                json["sessions"][0].get("cwd").is_none(),
                "Kit 摘要不得泄漏 cwd"
            );
        }
        other => panic!("预期 ListSessions reply，实际为 {other:?}"),
    }

    let expected_session_cwd = fs::canonicalize(harness._temporary.path())
        .expect("测试临时根必须能 canonicalize")
        .join("app-data/dispatch-loop-test/scope-a/workspace");
    // ListSessions reply 是 session/list 的完成屏障，之后再读取最终 wire 快照。
    let wire = harness.wire();
    assert_session_wire_contract(&wire, &session_id, &expected_session_cwd, Some("cursor-1"));
    let list_wire = wire
        .into_iter()
        .find(|item| item["method"] == "session/list")
        .expect("必须调用标准 session/list");
    assert_eq!(list_wire["params"]["cursor"], "cursor-1");
    assert!(list_wire["params"].get("limit").is_none());
    assert_eq!(
        list_wire["params"]["cwd"].as_str(),
        expected_session_cwd.to_str(),
        "session/list cwd 必须精确等于当前 scope 的隔离 workspace"
    );
}

/// `ListSessions { cursor: None }` 必须在真实 ACP wire 中省略 cursor。
#[test]
fn list_without_cursor_omits_cursor_from_acp_params() {
    let harness = Harness::configured("basic", [], Duration::from_secs(60));
    let session_id = harness.new_session("scope-a");
    let reply = harness
        .runtime
        .dispatch(KitCommand::ListSessions {
            scope_id: "scope-a".to_string(),
            cursor: None,
        })
        .expect("无 cursor 的 ListSessions 必须等待 sidecar result");
    assert!(matches!(reply, KitReply::ListSessions { .. }));

    let expected_session_cwd = fs::canonicalize(harness._temporary.path())
        .expect("测试临时根必须能 canonicalize")
        .join("app-data/dispatch-loop-test/scope-a/workspace");
    let wire = harness.wire();
    assert_session_wire_contract(&wire, &session_id, &expected_session_cwd, None);
    let list_wire = wire
        .into_iter()
        .find(|item| item["method"] == "session/list")
        .expect("必须调用标准 session/list");
    assert!(
        list_wire["params"].get("cursor").is_none(),
        "ListSessions cursor=None 时 session/list params 不得包含 cursor"
    );
}

/// delete_session 等待 sidecar session/close，并在成功后从 actor 内存摘掉该 session。
#[test]
fn delete_session_waits_for_acp_close_and_drops_active_session() {
    let harness = Harness::configured("basic", [], Duration::from_secs(60));
    let session_id = harness.new_session("scope-a");
    let reply = harness
        .runtime
        .dispatch(KitCommand::DeleteSession {
            scope_id: "scope-a".to_string(),
            session_id: session_id.clone(),
        })
        .expect("DeleteSession 必须等待 sidecar result");
    assert_eq!(reply, KitReply::DeleteSession { session_id: session_id.clone() });

    let close_wire = harness
        .captured_requests("session/close")
        .into_iter()
        .next()
        .expect("必须调用标准 session/close");
    assert_eq!(close_wire["params"]["sessionId"], session_id);
    assert!(
        close_wire["params"].get("_meta").is_none(),
        "session/close 不得携带 _meta"
    );
}

/// Task 7：同一 scope/session 的并发 cold resume 只允许一个 ACP load。
#[test]
fn concurrent_cold_resume_writes_one_session_load() {
    let harness = Harness::configured("load_hold_single", [], Duration::from_secs(2));
    let start = Arc::new(Barrier::new(4));
    let joins = (0..3)
        .map(|_| {
            let runtime = Arc::clone(&harness.runtime);
            let start = Arc::clone(&start);
            thread::spawn(move || {
                start.wait();
                runtime.dispatch(KitCommand::ResumeSession {
                    scope_id: "scope-a".to_string(),
                    session_id: CANONICAL_SESSION_ID.to_string(),
                })
            })
        })
        .collect::<Vec<_>>();
    start.wait();

    wait_for_load_waiting(&harness);
    let replies = joins
        .into_iter()
        .map(|join| {
            join.join()
                .expect("并发 cold resume 线程必须正常 join")
                .expect("并发 cold resume 必须得到结构化 reply")
        })
        .collect::<Vec<_>>();

    for reply in replies {
        assert_eq!(
            reply,
            KitReply::ResumeSession {
                accepted: true,
                session_id: CANONICAL_SESSION_ID.to_string(),
            }
        );
    }
    release_load(&harness);
    wait_for_load_completed(&harness, 1);
    wait_for_status(&harness, "replay_complete");
    // gate 释放且 fake sidecar 已完成消费后，才读取最终 wire 快照。
    let expected_session_cwd = harness.expected_scope_root().join("scope-a/workspace");
    let wire = harness.wire();
    assert_session_wire_contract(&wire, CANONICAL_SESSION_ID, &expected_session_cwd, None);
    assert_eq!(
        wire.iter()
            .filter(|item| item.get("method").and_then(Value::as_str) == Some("session/load"))
            .count(),
        1,
        "同一个 cold load flight 只能写一条 session/load"
    );
}

/// Task 7：异 session 在 cold load 期间 busy，旧诊断不能进入产品或热恢复 transcript。
#[test]
fn different_session_during_load_is_busy_and_diagnostics_never_buffer() {
    let harness = Harness::configured("load_live_diagnostic", [], Duration::from_secs(2));

    assert_eq!(
        harness.resume(CANONICAL_SESSION_ID),
        KitReply::ResumeSession {
            accepted: true,
            session_id: CANONICAL_SESSION_ID.to_string(),
        }
    );
    harness.wait_for_method("session/load");
    wait_for_load_waiting(&harness);
    assert_eq!(harness.resume_error("session-2").code, "session_busy");
    release_load(&harness);

    wait_for_status(&harness, "replay_complete");
    assert_eq!(
        harness.count_events_with_code("replay_complete"),
        1,
        "冷 load 完成前不应生成额外 replay fence"
    );
    assert_eq!(
        harness
            .events()
            .iter()
            .filter(|event| is_diagnostic(event))
            .count(),
        0,
        "未知 live update 不得进入产品事件或 transcript"
    );

    harness.resume(CANONICAL_SESSION_ID);
    wait_until(|| harness.count_events_with_code("replay_complete") == 2);
    assert_eq!(
        harness.count_events_with_code("replay_complete"),
        2,
        "热恢复只能追加本次新生成的 replay fence"
    );
    assert_eq!(
        harness
            .events()
            .iter()
            .filter(|event| is_diagnostic(event))
            .count(),
        0,
        "热恢复不得重放旧 skipped_update/replay_skipped"
    );
}

/// 冷 replay 只进入当前事件流，不得污染后续 hot resume 的 recoverable transcript。
#[test]
fn cold_replay_is_not_replayed_again_by_hot_resume() {
    let harness = Harness::configured("basic", [], Duration::from_secs(2));

    harness.resume(CANONICAL_SESSION_ID);
    wait_for_status(&harness, "replay_complete");
    assert_eq!(
        harness
            .events()
            .iter()
            .filter(|event| {
                matches!(&event.block, KitBlock::Assistant { markdown, .. } if markdown == "历史回答")
            })
            .count(),
        1,
        "cold replay 应只产生一条当前事件"
    );

    harness.resume(CANONICAL_SESSION_ID);
    wait_until(|| harness.count_events_with_code("replay_complete") == 2);
    assert_eq!(
        harness
            .events()
            .iter()
            .filter(|event| {
                matches!(&event.block, KitBlock::Assistant { markdown, .. } if markdown == "历史回答")
            })
            .count(),
        1,
        "cold replay 不得写入 transcript 并被 hot resume 再次展示"
    );
}

/// Task 7：prompt transport EOF 必须终结 active turn，并且迟到生命周期不得重复终态。
#[test]
fn prompt_transport_eof_finishes_active_turn_once() {
    let harness = Harness::configured("prompt_eof", [], Duration::from_secs(2));
    let session_id = harness.new_session("scope-a");

    assert_send(
        harness
            .runtime
            .dispatch(send(&session_id, "eof-turn", "EOF 前发送", None))
            .expect("prompt EOF 前 Send 必须已写入"),
        false,
        &session_id,
        "eof-turn",
    );
    harness.wait_for_method("session/prompt");
    wait_until(|| {
        harness.events().iter().any(|event| {
            event.session_id == session_id
                && event.submission_id.as_deref() == Some("eof-turn")
                && matches!(&event.block, KitBlock::Assistant { markdown, .. } if markdown == "EOF 前的回答")
        })
    });

    wait_until(|| {
        harness
            .events()
            .iter()
            .filter(|event| {
                event.session_id == session_id
                    && event.submission_id.as_deref() == Some("eof-turn")
                    && matches!(&event.block, KitBlock::Status { code, .. } if matches!(code.as_str(), "cancelled" | "error" | "turn_completed"))
            })
            .count()
            == 1
    });
    let terminal = harness
        .events()
        .into_iter()
        .find(|event| {
            event.session_id == session_id
                && event.submission_id.as_deref() == Some("eof-turn")
                && matches!(&event.block, KitBlock::Status { code, .. } if matches!(code.as_str(), "cancelled" | "error" | "turn_completed"))
        })
        .expect("transport EOF 必须留下 turn terminal");
    assert!(matches!(
        terminal.block,
        KitBlock::Status { code, .. } if code == "error"
    ));
    assert_eq!(
        harness
            .events()
            .iter()
            .filter(|event| {
                event.session_id == session_id
                    && event.submission_id.as_deref() == Some("eof-turn")
                    && matches!(&event.block, KitBlock::Status { code, .. } if matches!(code.as_str(), "cancelled" | "error" | "turn_completed"))
            })
            .count(),
        1,
        "transport EOF 只能发一次 turn terminal"
    );
}

/// Task 7：显式 actor shutdown 必须终结 active turn，且不能追加第二个终态。
#[test]
fn explicit_actor_shutdown_finishes_active_turn_once() {
    let harness = Harness::configured("hold_prompt", [], Duration::from_secs(2));
    let session_id = harness.new_session("scope-a");
    assert_send(
        harness
            .runtime
            .dispatch(send(&session_id, "shutdown-turn", "shutdown 前发送", None))
            .expect("shutdown 前 prompt 必须已写入"),
        false,
        &session_id,
        "shutdown-turn",
    );
    harness.wait_for_method("session/prompt");
    harness.wait_for_prompt(1);

    harness
        .runtime
        .dispatch(KitCommand::SetLlmChannel {
            kind: None,
            base_url: None,
            model_id: None,
            relay_base_url: None,
            app_key: None,
            api_key: Some("rotated-shutdown-key".to_string()),
            access_token: None,
            client_request_id: Some("shutdown-turn-test".to_string()),
        })
        .expect("显式 Channel restart 必须等待 actor shutdown");

    wait_until(|| {
        harness
            .events()
            .iter()
            .filter(|event| {
                event.session_id == session_id
                    && event.submission_id.as_deref() == Some("shutdown-turn")
                    && matches!(&event.block, KitBlock::Status { code, .. } if matches!(code.as_str(), "cancelled" | "error" | "turn_completed"))
            })
            .count()
            == 1
    });
    let terminal = harness
        .events()
        .into_iter()
        .find(|event| {
            event.session_id == session_id
                && event.submission_id.as_deref() == Some("shutdown-turn")
                && matches!(&event.block, KitBlock::Status { code, .. } if matches!(code.as_str(), "cancelled" | "error" | "turn_completed"))
        })
        .expect("显式 shutdown 必须留下 turn terminal");
    assert!(matches!(
        terminal.block,
        KitBlock::Status { code, .. } if code == "cancelled"
    ));
}

/// terminal sink 首次失败后只允许一次稳定身份的重试，shutdown 会确定性排空 pending outbox。
#[test]
fn terminal_sink_failure_is_retried_exactly_once() {
    let harness = Harness::configured("basic", [], Duration::from_secs(60));
    let session_id = harness.new_session("scope-a");
    harness.fail_next_terminal();

    assert_send(
        harness
            .runtime
            .dispatch(send(&session_id, "sink-failure-turn", "终态运输失败", None))
            .expect("终态 sink 失败不应影响 Send 的立即回执"),
        false,
        &session_id,
        "sink-failure-turn",
    );
    harness.wait_for_method("session/prompt");
    // 先确认第一次真实 emit 已失败，再让 runtime Drop 触发确定性的 outbox drain。
    harness.wait_for_terminal_attempts(1);

    let terminal_attempts = Arc::clone(&harness.terminal_attempts);
    let terminal_identities = Arc::clone(&harness.terminal_identities);
    let events = Arc::clone(&harness.events);
    let runtime = match Arc::try_unwrap(harness.runtime) {
        Ok(runtime) => runtime,
        Err(_) => panic!("终态重试测试不应残留 runtime 外部引用"),
    };
    drop(runtime);
    // Drop 完成 shutdown/outbox drain 后，再等待第二次真实尝试进入最终快照。
    wait_until(|| terminal_attempts.load(Ordering::Acquire) >= 2);

    let identities = terminal_identities
        .lock()
        .expect("终态身份锁必须可用")
        .clone();
    assert_eq!(
        identities.len(),
        2,
        "shutdown 后终态必须恰好包含首次失败和一次重试"
    );
    assert_eq!(
        identities[0], identities[1],
        "终态重试必须复用相同 event_id 与 sequence"
    );
    assert_eq!(
        events
            .lock()
            .expect("事件锁必须可用")
            .iter()
            .filter(|event| {
                event.session_id == session_id
                    && event.submission_id.as_deref() == Some("sink-failure-turn")
                    && matches!(
                        &event.block,
                        KitBlock::Status { code, .. }
                            if matches!(code.as_str(), "turn_completed" | "cancelled" | "error")
                    )
            })
            .count(),
        1,
        "outbox drain 后产品最终只能观察到一个终态"
    );
}

/// commit-then-error 只能按稳定 event_id/sequence 重试，外部 sink 需按 event_id 去重。
#[test]
fn terminal_commit_then_error_retries_with_same_event_identity() {
    let harness = Harness::configured("basic", [], Duration::from_secs(60));
    let session_id = harness.new_session("scope-a");
    harness.fail_next_terminal_after_commit();

    assert_send(
        harness
            .runtime
            .dispatch(send(
                &session_id,
                "commit-then-error-turn",
                "提交后返回错误",
                None,
            ))
            .expect("commit-then-error 不应影响 Send 的立即回执"),
        false,
        &session_id,
        "commit-then-error-turn",
    );
    harness.wait_for_method("session/prompt");

    wait_until(|| {
        harness
            .events()
            .iter()
            .filter(|event| {
                event.session_id == session_id
                    && event.submission_id.as_deref() == Some("commit-then-error-turn")
                    && matches!(
                        &event.block,
                        KitBlock::Status { code, .. }
                            if matches!(code.as_str(), "turn_completed" | "cancelled" | "error")
                    )
            })
            .count()
            >= 2
    });
    let terminal_events = harness
        .events()
        .into_iter()
        .filter(|event| {
            event.session_id == session_id
                && event.submission_id.as_deref() == Some("commit-then-error-turn")
                && matches!(
                    &event.block,
                    KitBlock::Status { code, .. }
                        if matches!(code.as_str(), "turn_completed" | "cancelled" | "error")
                )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        terminal_events.len(),
        2,
        "commit-then-error 只需一次重试即可收敛"
    );
    assert_eq!(
        terminal_events[0].event_id, terminal_events[1].event_id,
        "重试必须复用稳定 event_id，供下游去重"
    );
    assert_eq!(
        terminal_events[0].sequence, terminal_events[1].sequence,
        "重试不得重新分配 sequence"
    );
}

/// dead actor 在 cleanup 已完成后仍必须重试尚未运输成功的 terminal event。
#[test]
fn dead_actor_shutdown_retries_pending_terminal_event() {
    let harness = Harness::configured_with_sink_delay(
        "prompt_eof",
        [],
        Duration::from_secs(2),
        Duration::from_millis(80),
    );
    let session_id = harness.new_session("scope-a");
    harness.fail_next_terminal();

    assert_send(
        harness
            .runtime
            .dispatch(send(
                &session_id,
                "dead-sink-failure",
                "dead actor 终态失败",
                None,
            ))
            .expect("dead actor 测试的 prompt 必须先成功写入"),
        false,
        &session_id,
        "dead-sink-failure",
    );
    harness.wait_for_method("session/prompt");
    wait_for_file(&harness.exited);
    let failure_deadline = Instant::now() + Duration::from_secs(2);
    while harness.fail_next_terminal.load(Ordering::Acquire) {
        assert!(
            Instant::now() < failure_deadline,
            "dead actor 测试未观察到 terminal sink 失败尝试，当前事件数={}",
            harness.events().len()
        );
        thread::yield_now();
    }

    harness
        .runtime
        .dispatch(KitCommand::SetLlmChannel {
            kind: None,
            base_url: None,
            model_id: None,
            relay_base_url: None,
            app_key: None,
            api_key: Some("dead-sink-retry-key".to_string()),
            access_token: None,
            client_request_id: Some("dead-sink-retry".to_string()),
        })
        .expect("dead actor shutdown 必须先重试 pending terminal event");

    wait_until(|| {
        harness.events().iter().any(|event| {
            event.session_id == session_id
                && event.submission_id.as_deref() == Some("dead-sink-failure")
                && matches!(
                    &event.block,
                    KitBlock::Status { code, .. }
                        if matches!(code.as_str(), "turn_completed" | "cancelled" | "error")
                )
        })
    });
    assert_eq!(
        harness
            .events()
            .iter()
            .filter(|event| {
                event.session_id == session_id
                    && event.submission_id.as_deref() == Some("dead-sink-failure")
                    && matches!(
                        &event.block,
                        KitBlock::Status { code, .. }
                            if matches!(code.as_str(), "turn_completed" | "cancelled" | "error")
                    )
            })
            .count(),
        1,
        "dead actor cleanup 不得丢失或重复运输 terminal event"
    );
}

/// dead/shutdown 连续 sink 失败时，terminal 必须由 Host 级 outbox 保留到后续重试。
#[test]
fn terminal_outbox_survives_multiple_sink_failures_at_dead_shutdown() {
    let harness = Harness::configured("prompt_eof", [], Duration::from_secs(60));
    let session_id = harness.new_session("scope-a");
    harness.fail_terminal_attempts(1_000);

    assert_send(
        harness
            .runtime
            .dispatch(send(
                &session_id,
                "orphan-terminal-turn",
                "多次失败后仍需保留终态",
                None,
            ))
            .expect("terminal outbox 测试的 prompt 必须先成功写入"),
        false,
        &session_id,
        "orphan-terminal-turn",
    );
    harness.wait_for_method("session/prompt");
    wait_for_file(&harness.exited);
    wait_until(|| harness.fail_terminal_attempts.load(Ordering::Acquire) < 1_000);

    let first_restart = harness.runtime.dispatch(KitCommand::SetLlmChannel {
        kind: None,
        base_url: None,
        model_id: None,
        relay_base_url: None,
        app_key: None,
        api_key: Some("orphan-terminal-first-restart".to_string()),
        access_token: None,
        client_request_id: Some("orphan-terminal-first-restart".to_string()),
    });
    assert!(
        first_restart.is_err(),
        "sink 仍连续失败时 cleanup 必须报告 terminal 未完成"
    );
    assert!(
        first_restart
            .as_ref()
            .expect_err("第一次重启应因 terminal pending 失败")
            .retryable,
        "terminal pending 的 cleanup 失败必须可重试"
    );
    assert!(
        harness.events().iter().all(|event| {
            !(event.session_id == session_id
                && event.submission_id.as_deref() == Some("orphan-terminal-turn")
                && matches!(
                    &event.block,
                    KitBlock::Status { code, .. }
                        if matches!(code.as_str(), "turn_completed" | "cancelled" | "error")
                ))
        }),
        "连续 sink 失败时第一次 shutdown 不能伪造已送达终态"
    );

    harness.fail_terminal_attempts(0);
    harness
        .runtime
        .dispatch(KitCommand::SetLlmChannel {
            kind: None,
            base_url: None,
            model_id: None,
            relay_base_url: None,
            app_key: None,
            api_key: Some("orphan-terminal-first-restart".to_string()),
            access_token: None,
            client_request_id: Some("orphan-terminal-first-restart".to_string()),
        })
        .expect("清除 sink 故障后，后续 cleanup 必须重试 Host outbox");

    wait_until(|| {
        harness.events().iter().any(|event| {
            event.session_id == session_id
                && event.submission_id.as_deref() == Some("orphan-terminal-turn")
                && matches!(
                    &event.block,
                    KitBlock::Status { code, .. }
                        if matches!(code.as_str(), "turn_completed" | "cancelled" | "error")
                )
        })
    });
    assert_eq!(
        harness
            .events()
            .iter()
            .filter(|event| {
                event.session_id == session_id
                    && event.submission_id.as_deref() == Some("orphan-terminal-turn")
                    && matches!(
                        &event.block,
                        KitBlock::Status { code, .. }
                            if matches!(code.as_str(), "turn_completed" | "cancelled" | "error")
                    )
            })
            .count(),
        1,
        "Host outbox 重试成功后只能保留一个稳定 terminal 观察结果"
    );
}

/// Task 7：同一 active turn 的重复 Cancel 只能产生一个 cancelled terminal。
#[test]
fn repeated_cancel_is_idempotent_for_active_turn() {
    let harness = Harness::configured("hold_prompt", [], Duration::from_secs(2));
    let session_id = harness.new_session("scope-a");
    assert_send(
        harness
            .runtime
            .dispatch(send(&session_id, "repeat-cancel", "重复取消", None))
            .expect("Cancel 前 prompt 必须已写入"),
        false,
        &session_id,
        "repeat-cancel",
    );
    harness.wait_for_method("session/prompt");
    harness.wait_for_prompt(1);

    for _ in 0..2 {
        assert_eq!(
            harness
                .runtime
                .dispatch(KitCommand::Cancel {
                    scope_id: "scope-a".to_string(),
                    session_id: session_id.clone(),
                })
                .expect("重复 Cancel 必须保持幂等 accepted"),
            KitReply::Cancel { accepted: true }
        );
    }

    wait_until(|| {
        harness.events().iter().any(|event| {
            event.session_id == session_id
                && event.submission_id.as_deref() == Some("repeat-cancel")
                && matches!(&event.block, KitBlock::Status { code, .. } if code == "cancelled")
        })
    });
    harness.release_prompt();
    assert_eq!(
        harness
            .events()
            .iter()
            .filter(|event| {
                event.session_id == session_id
                    && event.submission_id.as_deref() == Some("repeat-cancel")
                    && matches!(&event.block, KitBlock::Status { code, .. } if matches!(code.as_str(), "cancelled" | "error" | "turn_completed"))
            })
            .count(),
        1,
        "重复 Cancel 只能完成一次 turn terminal"
    );
}

/// Task 7：通知洪水期间 Cancel 与显式 shutdown 都必须获得确定性的 actor 调度机会。
#[test]
fn bounded_inbound_drain_keeps_cancel_and_shutdown_reachable() {
    let cancel = Harness::configured_with_sink_delay(
        "notification_flood",
        [],
        Duration::from_secs(2),
        Duration::from_millis(80),
    );
    let cancel_session = cancel.new_session("scope-a");
    assert_send(
        cancel
            .runtime
            .dispatch(send(&cancel_session, "flood-cancel", "洪水中取消", None))
            .expect("通知洪水前 prompt 必须已写入"),
        false,
        &cancel_session,
        "flood-cancel",
    );
    wait_until(|| {
        cancel.events().iter().any(|event| {
            matches!(&event.block, KitBlock::Assistant { markdown, .. } if markdown == "洪水事件")
        })
    });
    let cancel_started = Instant::now();
    assert_eq!(
        cancel
            .runtime
            .dispatch(KitCommand::Cancel {
                scope_id: "scope-a".to_string(),
                session_id: cancel_session.clone(),
            })
            .expect("通知洪水期间 Cancel 仍必须得到同步回执"),
        KitReply::Cancel { accepted: true }
    );
    assert!(
        cancel_started.elapsed() < CONTROL_REPLY_TIMEOUT,
        "通知洪水不得让 Cancel 等待无界 drain"
    );
    wait_until(|| {
        fs::read_to_string(&cancel.cancel_seen)
            .map(|seen| seen.lines().count() >= 1)
            .unwrap_or(false)
    });
    assert_eq!(
        fs::read_to_string(&cancel.cancel_seen)
            .expect("fake sidecar 必须留下 Cancel 到达 marker")
            .lines()
            .count(),
        1,
        "通知洪水期间 Cancel 必须恰好到达 fake sidecar 一次"
    );

    let cancel_runtime = Arc::clone(&cancel.runtime);
    let (cancel_shutdown_reply, cancel_shutdown_result) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let result = cancel_runtime.dispatch(KitCommand::SetLlmChannel {
            kind: None,
            base_url: None,
            model_id: None,
            relay_base_url: None,
            app_key: None,
            api_key: Some("flood-cancel-shutdown-key".to_string()),
            access_token: None,
            client_request_id: Some("flood-cancel-shutdown-test".to_string()),
        });
        cancel_shutdown_reply
            .send(result)
            .expect("Cancel teardown 测试线程必须能交付结果");
    });
    let cancel_shutdown_result = cancel_shutdown_result
        .recv_timeout(CONTROL_REPLY_TIMEOUT)
        .expect("Cancel teardown 不得等待无界 drain")
        .expect("Cancel teardown 必须完成 Channel restart");
    assert!(matches!(
        cancel_shutdown_result,
        KitReply::LlmChannelView { .. }
    ));
    wait_for_file(&cancel.exited);
    wait_for_started_count(&cancel, 2);
    assert_eq!(
        fs::read_to_string(&cancel.cancel_seen)
            .expect("Cancel teardown 后必须能读取 cancel marker")
            .lines()
            .count(),
        1,
        "Cancel teardown 后不得重复发送 session/cancel"
    );

    let shutdown = Harness::configured_with_sink_delay(
        "notification_flood",
        [],
        Duration::from_secs(2),
        Duration::from_millis(80),
    );
    let shutdown_session = shutdown.new_session("scope-a");
    assert_send(
        shutdown
            .runtime
            .dispatch(send(
                &shutdown_session,
                "flood-shutdown",
                "洪水中 shutdown",
                None,
            ))
            .expect("显式 shutdown 前 prompt 必须已写入"),
        false,
        &shutdown_session,
        "flood-shutdown",
    );
    wait_until(|| {
        shutdown.events().iter().any(|event| {
            matches!(&event.block, KitBlock::Assistant { markdown, .. } if markdown == "洪水事件")
        })
    });
    let shutdown_runtime = Arc::clone(&shutdown.runtime);
    let (shutdown_reply, shutdown_result) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let result = shutdown_runtime.dispatch(KitCommand::SetLlmChannel {
            kind: None,
            base_url: None,
            model_id: None,
            relay_base_url: None,
            app_key: None,
            api_key: Some("flood-shutdown-key".to_string()),
            access_token: None,
            client_request_id: Some("flood-shutdown-test".to_string()),
        });
        shutdown_reply
            .send(result)
            .expect("shutdown 测试线程必须能交付结果");
    });
    let shutdown_result = shutdown_result
        .recv_timeout(CONTROL_REPLY_TIMEOUT)
        .expect("通知洪水不得让显式 shutdown 等待无界 drain")
        .expect("显式 shutdown 必须完成 Channel restart");
    assert!(matches!(shutdown_result, KitReply::LlmChannelView { .. }));
    wait_for_file(&shutdown.exited);
    wait_until(|| {
        fs::read_to_string(&shutdown.cancel_seen)
            .map(|seen| seen.lines().count() >= 1)
            .unwrap_or(false)
    });
    assert_eq!(
        fs::read_to_string(&shutdown.cancel_seen)
            .expect("显式 shutdown 后必须能读取 cancel marker")
            .lines()
            .count(),
        1,
        "显式 shutdown 必须恰好发送一次 session/cancel"
    );
    wait_for_started_count(&shutdown, 2);
    assert_eq!(
        fs::read_to_string(&shutdown.exited)
            .expect("显式 shutdown 后必须能读取 child 退出 marker")
            .lines()
            .count(),
        1,
        "显式 shutdown 必须观察到旧 child 恰好退出一次"
    );
}

/// Task 7：冷恢复后每次 hot resume 都生成唯一的新 replay_complete，不重放旧 fence。
#[test]
fn two_hot_resumes_after_cold_load_emit_fresh_replay_complete_once_each() {
    let harness = Harness::configured("basic", [], Duration::from_secs(2));

    harness.resume(CANONICAL_SESSION_ID);
    wait_for_status(&harness, "replay_complete");
    harness.resume(CANONICAL_SESSION_ID);
    harness.resume(CANONICAL_SESSION_ID);
    wait_until(|| harness.count_events_with_code("replay_complete") == 3);

    let fences = harness
        .events()
        .into_iter()
        .filter(|event| {
            matches!(&event.block, KitBlock::Status { code, .. } if code == "replay_complete")
        })
        .collect::<Vec<_>>();
    assert_eq!(fences.len(), 3);
    let fence_ids = fences
        .iter()
        .map(|event| event.event_id.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        fence_ids.len(),
        3,
        "每次 resume 必须生成新的 fence event_id"
    );
    assert_eq!(
        fences.iter().filter(|event| is_diagnostic(event)).count(),
        0
    );
}

/// Task 7：load 超时撤销 ACP owner 后，迟到 response 不得复活 replay。
#[test]
fn timeout_then_late_load_response_is_dropped() {
    let harness = Harness::configured_with_load_timeout(
        "load_late",
        [],
        Duration::from_secs(2),
        Duration::from_millis(15),
    );

    assert_eq!(
        harness.resume(CANONICAL_SESSION_ID),
        KitReply::ResumeSession {
            accepted: true,
            session_id: CANONICAL_SESSION_ID.to_string(),
        }
    );
    harness.wait_for_method("session/load");
    // fake sidecar 先写出迟到 marker，再尝试发送 load response，证明该 response 确实到达发送边界。
    wait_for_file(&harness.late_replay);
    let failure = wait_for_any_session_error(&harness);
    assert_eq!(
        failure.session_id, CANONICAL_SESSION_ID,
        "超时错误必须归属当前 cold load session"
    );
    assert!(
        matches!(&failure.block, KitBlock::Error(error) if error.code == "sidecar_unavailable" && error.retryable)
    );
    // 先让 Host 观察到 deadline 错误，再放行 fake sidecar 的迟到 response。
    release_load(&harness);
    wait_for_file(&harness.exited);

    assert_eq!(
        harness.count_events_with_code("replay_complete"),
        0,
        "超时后迟到 load response 不得生成 replay_complete"
    );
    assert_eq!(
        harness.captured_requests("session/load").len(),
        1,
        "超时不会重发同一 owner request"
    );
}

/// Task 7：response 已进入入站队列但处理被阻塞时，仍必须遵守 load deadline。
#[test]
fn queued_load_response_processed_after_deadline_is_dropped() {
    let harness = Harness::configured_with_load_timeout_and_sink_delay(
        "load_queued_after_deadline",
        [],
        Duration::from_secs(2),
        Duration::from_millis(15),
        Duration::from_millis(80),
    );

    assert_eq!(
        harness.resume(CANONICAL_SESSION_ID),
        KitReply::ResumeSession {
            accepted: true,
            session_id: CANONICAL_SESSION_ID.to_string(),
        }
    );
    harness.wait_for_method("session/load");
    // 先等待 timeout 终止 flight，再断言目标 session 没有任何部分 replay 内容。
    let failure = wait_for_any_session_error(&harness);
    assert!(
        matches!(&failure.block, KitBlock::Error(error) if error.code == "sidecar_unavailable" && error.retryable)
    );
    assert_eq!(
        harness.count_events_with_code("replay_complete"),
        0,
        "处理时已超过 deadline 的 load response 不得生成 replay_complete"
    );
    assert_eq!(
        harness
            .events()
            .iter()
            .filter(|event| {
                event.session_id == CANONICAL_SESSION_ID
                    && event.origin == Origin::Replay
                    && is_recoverable_content(event)
            })
            .count(),
        0,
        "deadline 后排队的 replay 不得发送任何部分 transcript 内容"
    );
}

/// Task 7：load timeout 后必须退休旧 transport，旧代 replay 不能进入新 generation。
#[test]
fn timeout_retires_transport_before_same_session_retry_and_drops_old_replay() {
    let harness = Harness::configured_with_load_timeout_and_sink_delay(
        "load_gate",
        [],
        Duration::from_secs(2),
        // 第一代在 250ms 同步 sink 中跨过 100ms deadline；第二代即时 response 仍留有调度余量。
        Duration::from_millis(100),
        SLOW_SINK_DELAY,
    );

    assert_eq!(
        harness.resume(CANONICAL_SESSION_ID),
        KitReply::ResumeSession {
            accepted: true,
            session_id: CANONICAL_SESSION_ID.to_string(),
        }
    );
    wait_for_load_waiting(&harness);
    // 释放 barrier 后，旧 replay 会在慢 sink 阻塞期间进入 reader 队列；timeout 后必须整代丢弃。
    release_load(&harness);
    wait_for_file(&harness.late_replay);
    let failure = wait_for_any_session_error(&harness);
    assert!(
        matches!(&failure.block, KitBlock::Error(error) if error.code == "sidecar_unavailable" && error.retryable)
    );
    assert_eq!(
        harness.count_events_with_code("replay_complete"),
        0,
        "timeout 不得生成 replay fence"
    );

    assert_eq!(
        harness.resume(CANONICAL_SESSION_ID),
        KitReply::ResumeSession {
            accepted: true,
            session_id: CANONICAL_SESSION_ID.to_string(),
        }
    );
    wait_for_started_count(&harness, 2);
    wait_until(|| harness.count_events_with_code("replay_complete") == 1);

    let events = harness.events();
    assert!(
        !events.iter().any(|event| {
            matches!(&event.block, KitBlock::Assistant { markdown, .. } if markdown == "旧代迟到")
        }),
        "第一代迟到 replay 不得进入新 generation 的产品事件"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(&event.block, KitBlock::Error(error) if error.code == "sidecar_unavailable"))
            .count(),
        1,
        "同一 timeout 只能发一次 session-level failure"
    );
}

/// Task 7：accepted cold resume 遇到 transport EOF 必须收到一次脱敏 session-level error。
#[test]
fn accepted_cold_resume_transport_eof_emits_one_session_error() {
    let harness = Harness::configured("load_eof", [], Duration::from_secs(2));

    assert_eq!(
        harness.resume(CANONICAL_SESSION_ID),
        KitReply::ResumeSession {
            accepted: true,
            session_id: CANONICAL_SESSION_ID.to_string(),
        }
    );
    wait_for_load_waiting(&harness);
    release_load(&harness);

    let failure = wait_for_any_session_error(&harness);
    assert_eq!(failure.session_id, CANONICAL_SESSION_ID);
    assert!(
        matches!(&failure.block, KitBlock::Error(error) if error.code == "sidecar_unavailable" && error.retryable)
    );
    assert!(failure.turn_id.is_none());
    assert!(failure.submission_id.is_none());
    wait_for_file(&harness.exited);
    assert_eq!(
        harness
            .events()
            .iter()
            .filter(|event| matches!(&event.block, KitBlock::Error(error) if error.code == "sidecar_unavailable"))
            .count(),
        1,
        "transport death 只能完成一次 session-level failure"
    );
    assert_eq!(
        harness.count_events_with_code("replay_complete"),
        0,
        "transport death 不得生成 replay fence"
    );
}

/// Task 7：明确 ACP NotFound 才使用 session_not_found，普通 load error 必须可重试。
#[test]
fn load_error_classification_preserves_not_found_and_retryable_failure_codes() {
    let unavailable = Harness::configured("load_error", [], Duration::from_secs(2));
    assert_eq!(
        unavailable.resume(CANONICAL_SESSION_ID),
        KitReply::ResumeSession {
            accepted: true,
            session_id: CANONICAL_SESSION_ID.to_string(),
        }
    );
    let unavailable_error = wait_for_any_session_error(&unavailable);
    assert!(
        matches!(&unavailable_error.block, KitBlock::Error(error) if error.code == "sidecar_unavailable" && error.retryable)
    );
    assert_eq!(
        unavailable.count_events_with_code("replay_complete"),
        0,
        "普通 load error 不得生成 replay fence"
    );
    wait_for_file(&unavailable.exited);

    let not_found = Harness::configured("load_fail", [], Duration::from_secs(2));
    assert_eq!(
        not_found.resume(CANONICAL_SESSION_ID),
        KitReply::ResumeSession {
            accepted: true,
            session_id: CANONICAL_SESSION_ID.to_string(),
        }
    );
    let not_found_error = wait_for_any_session_error(&not_found);
    assert!(
        matches!(&not_found_error.block, KitBlock::Error(error) if error.code == "session_not_found" && !error.retryable)
    );
    assert_eq!(
        not_found.count_events_with_code("replay_complete"),
        0,
        "NotFound 也不得生成 replay fence"
    );
}

/// Task 7：错配 owner/session 的入站消息只能丢弃，不能完成当前 cold load。
#[test]
fn mismatched_load_owner_and_session_messages_are_dropped() {
    let owner = Harness::configured("load_wrong_owner", [], Duration::from_secs(2));
    assert_eq!(
        owner.resume(CANONICAL_SESSION_ID),
        KitReply::ResumeSession {
            accepted: true,
            session_id: CANONICAL_SESSION_ID.to_string(),
        }
    );
    wait_for_status(&owner, "replay_complete");
    assert_eq!(
        owner.count_events_with_code("replay_complete"),
        1,
        "未知 owner response 不得提前完成或重复完成 load"
    );

    let session = Harness::configured("load_wrong_session", [], Duration::from_secs(2));
    assert_eq!(
        session.resume(CANONICAL_SESSION_ID),
        KitReply::ResumeSession {
            accepted: true,
            session_id: CANONICAL_SESSION_ID.to_string(),
        }
    );
    wait_for_status(&session, "replay_complete");
    assert!(
        session
            .events()
            .iter()
            .all(|event| event.session_id != "wrong-session"),
        "其它 session 的 replay notification 不得进入产品"
    );
}

/// Task 7：cold load 中取消必须终结当前 PendingSend，旧成功不能污染下一次 Send。
#[test]
fn cancel_during_cold_load_clears_pending_send_and_allows_independent_retry() {
    let harness = Harness::configured("load_cancel", [], Duration::from_secs(2));
    let (send_result, send_done) = mpsc::sync_channel(1);
    let runtime = Arc::clone(&harness.runtime);
    thread::spawn(move || {
        let result = runtime.dispatch(send(
            CANONICAL_SESSION_ID,
            "cancelled-load-send",
            "取消中的发送",
            None,
        ));
        send_result
            .send(result)
            .expect("测试发送线程必须能交付结果");
    });
    wait_for_load_waiting(&harness);
    release_load(&harness);

    assert_eq!(
        harness
            .runtime
            .dispatch(KitCommand::Cancel {
                scope_id: "scope-a".to_string(),
                session_id: CANONICAL_SESSION_ID.to_string(),
            })
            .expect("cold load 中的 Cancel 必须得到同步受理"),
        KitReply::Cancel { accepted: true }
    );
    let send_error = send_done
        .recv_timeout(TEST_TIMEOUT)
        .expect("PendingSend 必须被取消结算")
        .expect_err("取消的 cold PendingSend 不得返回成功 Send");
    assert_eq!(send_error.code, "cancelled");
    assert_eq!(
        harness.method_count("session/prompt"),
        0,
        "取消的旧 load 不得写 prompt"
    );

    wait_for_file(&harness.late_replay);
    wait_for_file(&harness.exited);
    assert_eq!(
        harness.count_events_with_code("replay_complete"),
        0,
        "取消后的旧 load success 不得生成 replay fence"
    );

    assert_send(
        harness
            .runtime
            .dispatch(send(
                CANONICAL_SESSION_ID,
                "after-cancel",
                "取消后新发送",
                None,
            ))
            .expect("下一次独立 Send 必须使用新 generation 恢复"),
        false,
        CANONICAL_SESSION_ID,
        "after-cancel",
    );
    wait_for_started_count(&harness, 2);
    harness.wait_for_method("session/prompt");
    wait_until(|| {
        harness.events().iter().any(|event| {
            event.submission_id.as_deref() == Some("after-cancel")
                && matches!(&event.block, KitBlock::Status { code, .. } if code == "turn_completed")
        })
    });
    assert_eq!(
        harness.method_count("session/load"),
        2,
        "取消后的下一次 Send 必须新建独立 load flight"
    );
    assert_eq!(
        harness.method_count("session/prompt"),
        1,
        "旧 load success 不得额外写 prompt"
    );
}

/// TC-RESUME / TC-HOT：冷恢复有 replay fence，热恢复不重写 load 且不打断 prompt。
#[test]
fn cold_and_hot_resume_obey_load_timing_replay_fence_and_session_busy_rules() {
    let cold = Harness::configured("load_hold", [], Duration::from_secs(60));
    let (resume_reply, resume_result) = mpsc::sync_channel(1);
    let cold_runtime = Arc::clone(&cold.runtime);
    thread::spawn(move || {
        let _ = resume_reply.send(cold_runtime.dispatch(KitCommand::ResumeSession {
            scope_id: "scope-a".to_string(),
            session_id: CANONICAL_SESSION_ID.to_string(),
        }));
    });
    // 先观测 Host 已写 session/load，再限制回执不得等 fake 的 700ms load result；
    // 这样不把首次 L3b/sidecar 启动成本误算成协议等待时间。
    cold.wait_for_method("session/load");
    assert_eq!(
        resume_result
            .recv_timeout(DELAYED_PROMPT_RESULT / 2)
            .expect("冷 Resume 在写 load 后必须立即回执")
            .expect("冷 Resume 不能因 load 未完成而失败"),
        KitReply::ResumeSession {
            accepted: true,
            session_id: CANONICAL_SESSION_ID.to_string(),
        }
    );
    let replay_complete = wait_for_status(&cold, "replay_complete");
    assert_eq!(replay_complete.turn_id, None);
    assert_eq!(replay_complete.submission_id, None);
    assert_eq!(replay_complete.event_id, replay_complete.block_id);
    assert!(
        replay_complete
            .event_id
            .starts_with(&format!("{CANONICAL_SESSION_ID}:host:replay_complete:"))
    );
    assert_eq!(
        cold.events()
            .iter()
            .filter(|event| is_diagnostic(event))
            .count(),
        0,
        "冷 replay 不得发送旧 skipped_update/replay_skipped 诊断"
    );

    let hot = Harness::configured("hold_prompt", [], Duration::from_secs(60));
    let session_id = hot.new_session("scope-a");
    // 固定 canonical ID 下使用独立未激活 ID，验证 prompting 时跨 session 仍拒绝恢复。
    let other_active_session = "other-active-session".to_string();
    assert_send(
        hot.runtime
            .dispatch(send(&session_id, "hot-turn", "正在生成", None))
            .expect("第一轮 prompt 必须可写入"),
        false,
        &session_id,
        "hot-turn",
    );
    hot.wait_for_method("session/prompt");
    hot.wait_for_prompt(1);
    wait_until(|| {
        hot.events().iter().any(|event| {
            matches!(
                &event.block,
                KitBlock::Assistant { markdown, streaming }
                    if markdown == "实时回答" && *streaming
            ) && event.origin == Origin::Live
        })
    });
    hot.wait_for_prompt(1);
    let load_count = hot.method_count("session/load");
    assert_eq!(
        hot.runtime
            .dispatch(KitCommand::ResumeSession {
                scope_id: "scope-a".to_string(),
                session_id: session_id.clone(),
            })
            .expect("热 Resume 必须立即完成"),
        KitReply::ResumeSession {
            accepted: true,
            session_id: session_id.clone(),
        }
    );
    assert_eq!(
        hot.method_count("session/load"),
        load_count,
        "热 Resume 禁止写 session/load"
    );
    wait_for_status(&hot, "replay_complete");
    assert!(
        hot.events().iter().any(|event| {
            matches!(
                &event.block,
                KitBlock::Assistant { markdown, streaming }
                    if markdown == "实时回答" && !*streaming
            ) && event.origin == Origin::Replay
        }),
        "热恢复必须重放内存中的助手快照并冻结 streaming"
    );
    assert!(
        hot.events().iter().any(|event| {
            matches!(
                &event.block,
                KitBlock::Assistant { markdown, streaming }
                    if markdown == "实时回答" && *streaming
            ) && event.origin == Origin::Live
        }),
        "Prompting 热恢复后必须补回 live streaming 快照"
    );

    let busy = hot
        .runtime
        .dispatch(KitCommand::ResumeSession {
            scope_id: "scope-a".to_string(),
            session_id: other_active_session,
        })
        .expect_err("Prompting 时恢复另一个已 active session 必须被拒绝");
    assert_eq!(busy.code, "session_busy");
    hot.release_prompt();
    wait_for_status(&hot, "turn_completed");
}

/// TC-HOT / TC-CANCEL：完成 ACP 写入或热恢复决策后，回执绝不能等待同步 sink 的事件投影。
#[test]
fn hot_resume_and_inflight_cancel_reply_before_slow_sink_projection() {
    let hot = Harness::configured_with_sink_delay(
        "hold_prompt",
        [],
        Duration::from_secs(60),
        SLOW_SINK_DELAY,
    );
    let hot_session = hot.new_session("scope-a");
    assert_send(
        hot.runtime
            .dispatch(send(&hot_session, "slow-hot", "慢 sink 热恢复", None))
            .expect("热恢复前的 prompt 必须写入"),
        false,
        &hot_session,
        "slow-hot",
    );
    wait_until(|| {
        hot.events().iter().any(|event| {
            matches!(
                &event.block,
                KitBlock::Assistant { markdown, streaming }
                    if markdown == "实时回答" && *streaming
            )
        })
    });
    hot.wait_for_prompt(1);

    let (hot_reply, hot_result) = mpsc::sync_channel(1);
    let hot_runtime = Arc::clone(&hot.runtime);
    let hot_resume_session = hot_session.clone();
    thread::spawn(move || {
        let _ = hot_reply.send(hot_runtime.dispatch(KitCommand::ResumeSession {
            scope_id: "scope-a".to_string(),
            session_id: hot_resume_session,
        }));
    });
    assert_eq!(
        hot_result
            .recv_timeout(IMMEDIATE_REPLY_TIMEOUT)
            .expect("热 Resume 不得等待 replay sink")
            .expect("热 Resume 必须被接受"),
        KitReply::ResumeSession {
            accepted: true,
            session_id: hot_session,
        }
    );

    let cancel = Harness::configured_with_sink_delay(
        "hold_prompt",
        [],
        Duration::from_secs(60),
        SLOW_SINK_DELAY,
    );
    let cancel_session = cancel.new_session("scope-a");
    assert_send(
        cancel
            .runtime
            .dispatch(send(&cancel_session, "slow-cancel", "慢 sink 取消", None))
            .expect("Cancel 前的 prompt 必须写入"),
        false,
        &cancel_session,
        "slow-cancel",
    );
    wait_until(|| {
        cancel.events().iter().any(|event| {
            matches!(
                &event.block,
                KitBlock::Assistant { markdown, streaming }
                    if markdown == "实时回答" && *streaming
            )
        })
    });
    cancel.wait_for_prompt(1);

    let (cancel_reply, cancel_result) = mpsc::sync_channel(1);
    let cancel_runtime = Arc::clone(&cancel.runtime);
    let cancel_request_session = cancel_session.clone();
    thread::spawn(move || {
        let _ = cancel_reply.send(cancel_runtime.dispatch(KitCommand::Cancel {
            scope_id: "scope-a".to_string(),
            session_id: cancel_request_session,
        }));
    });
    cancel.wait_for_method("session/cancel");
    assert_eq!(
        cancel_result
            .recv_timeout(IMMEDIATE_REPLY_TIMEOUT)
            .expect("in-flight Cancel 不得等待 cancelled 状态 sink")
            .expect("in-flight Cancel 必须被接受"),
        KitReply::Cancel { accepted: true }
    );
    cancel.release_prompt();
    wait_for_status(&cancel, "cancelled");
}

/// TC-PERM：只能按本次 options 精确选择连字符 allow-once；未知工具走 reject-once。
#[test]
fn reverse_permission_selects_only_current_allow_once_or_reject_once() {
    let approved = Harness::configured("permission", [], Duration::from_secs(60));
    let session_id = approved.new_session("scope-a");
    approved
        .runtime
        .dispatch(send(&session_id, "permission-turn", "需要权限", None))
        .expect("发送触发权限请求的 prompt 必须成功");
    wait_until(|| {
        approved.captured.exists()
            && fs::read_to_string(&approved.captured)
                .map(|wire| wire.contains("\"id\":900") && wire.contains("allow-once"))
                .unwrap_or(false)
    });
    let allowed_reply = approved
        .wire()
        .into_iter()
        .find(|item| item["id"] == 900 && item.get("result").is_some())
        .expect("Host 必须回复 permission reverse request");
    assert_eq!(allowed_reply["result"]["outcome"]["outcome"], "selected");
    assert_eq!(allowed_reply["result"]["outcome"]["optionId"], "allow-once");
    assert!(!allowed_reply.to_string().contains("allow_once"));
    assert_ne!(
        allowed_reply["result"]["outcome"]["optionId"],
        "enable-always-approve"
    );

    let unknown = Harness::configured("permission_unknown", [], Duration::from_secs(60));
    let unknown_session = unknown.new_session("scope-a");
    unknown
        .runtime
        .dispatch(send(
            &unknown_session,
            "unknown-permission",
            "未知工具",
            None,
        ))
        .expect("发送未知权限工具的 prompt 必须成功");
    wait_until(|| {
        unknown.captured.exists()
            && fs::read_to_string(&unknown.captured)
                .map(|wire| wire.contains("\"id\":900") && wire.contains("reject-once"))
                .unwrap_or(false)
    });
    let rejected_reply = unknown
        .wire()
        .into_iter()
        .find(|item| item["id"] == 900 && item.get("result").is_some())
        .expect("未知工具也必须回复 reverse request");
    assert_eq!(
        rejected_reply["result"]["outcome"]["optionId"],
        "reject-once"
    );

    // cancel 已写出后到达的 permission 必须只回复 cancelled，不能选择任何 option。
    let cancelled = Harness::configured("permission_after_cancel", [], Duration::from_secs(60));
    let cancelled_session = cancelled.new_session("scope-a");
    cancelled
        .runtime
        .dispatch(send(
            &cancelled_session,
            "cancelled-permission",
            "取消后请求权限",
            None,
        ))
        .expect("发送待 permission 的 prompt 必须成功");
    assert_eq!(
        cancelled
            .runtime
            .dispatch(KitCommand::Cancel {
                scope_id: "scope-a".to_string(),
                session_id: cancelled_session,
            })
            .expect("Cancel 必须在 permission 前成功写出"),
        KitReply::Cancel { accepted: true }
    );
    wait_until(|| {
        cancelled.captured.exists()
            && fs::read_to_string(&cancelled.captured)
                .map(|wire| {
                    wire.contains("\"id\":900") && wire.contains("\"outcome\":\"cancelled\"")
                })
                .unwrap_or(false)
    });
    let cancelled_reply = cancelled
        .wire()
        .into_iter()
        .find(|item| item["id"] == 900 && item.get("result").is_some())
        .expect("取消后的 permission reverse request 必须得到回复");
    assert_eq!(cancelled_reply["result"]["outcome"]["outcome"], "cancelled");
    assert!(
        cancelled_reply["result"]["outcome"]
            .get("optionId")
            .is_none(),
        "已取消时不得选择 permission option"
    );
}

/// TC-PERM wrapper：`_x.ai/session/request_permission` 解码后必须与 direct wire 共用严格选择逻辑。
#[test]
fn wrapped_reverse_permission_selects_approved_unknown_and_cancelled_outcomes() {
    let approved = Harness::configured("permission_wrapper", [], Duration::from_secs(60));
    let session_id = approved.new_session("scope-a");
    approved
        .runtime
        .dispatch(send(&session_id, "wrapped-approved", "包装批准", None))
        .expect("包装 permission 的 prompt 必须成功");
    wait_until(|| {
        approved.captured.exists()
            && fs::read_to_string(&approved.captured)
                .map(|wire| wire.contains("\"id\":900") && wire.contains("allow-once"))
                .unwrap_or(false)
    });
    let approved_reply = approved
        .wire()
        .into_iter()
        .find(|item| item["id"] == 900 && item.get("result").is_some())
        .expect("Host 必须回复包装的 approved permission request");
    assert_eq!(
        approved_reply["result"]["outcome"],
        json!({ "outcome": "selected", "optionId": "allow-once" })
    );

    let unknown = Harness::configured("permission_unknown_wrapper", [], Duration::from_secs(60));
    let unknown_session = unknown.new_session("scope-a");
    unknown
        .runtime
        .dispatch(send(
            &unknown_session,
            "wrapped-unknown",
            "包装未知工具",
            None,
        ))
        .expect("包装未知 permission 的 prompt 必须成功");
    wait_until(|| {
        unknown.captured.exists()
            && fs::read_to_string(&unknown.captured)
                .map(|wire| wire.contains("\"id\":900") && wire.contains("reject-once"))
                .unwrap_or(false)
    });
    let unknown_reply = unknown
        .wire()
        .into_iter()
        .find(|item| item["id"] == 900 && item.get("result").is_some())
        .expect("Host 必须回复包装的 unknown permission request");
    assert_eq!(
        unknown_reply["result"]["outcome"],
        json!({ "outcome": "selected", "optionId": "reject-once" })
    );

    let cancelled = Harness::configured(
        "permission_after_cancel_wrapper",
        [],
        Duration::from_secs(60),
    );
    let cancelled_session = cancelled.new_session("scope-a");
    cancelled
        .runtime
        .dispatch(send(
            &cancelled_session,
            "wrapped-cancelled",
            "包装取消后权限",
            None,
        ))
        .expect("包装 cancel permission 的 prompt 必须成功");
    assert_eq!(
        cancelled
            .runtime
            .dispatch(KitCommand::Cancel {
                scope_id: "scope-a".to_string(),
                session_id: cancelled_session,
            })
            .expect("Cancel 必须先于包装 permission 写出"),
        KitReply::Cancel { accepted: true }
    );
    wait_until(|| {
        cancelled.captured.exists()
            && fs::read_to_string(&cancelled.captured)
                .map(|wire| {
                    wire.contains("\"id\":900") && wire.contains("\"outcome\":\"cancelled\"")
                })
                .unwrap_or(false)
    });
    let cancelled_reply = cancelled
        .wire()
        .into_iter()
        .find(|item| item["id"] == 900 && item.get("result").is_some())
        .expect("Host 必须回复包装的 cancelled permission request");
    assert_eq!(
        cancelled_reply["result"]["outcome"],
        json!({ "outcome": "cancelled" })
    );
}

/// TC-HP（MCP 分支）：空集失败不挡聊天；非空期望缺失发 mcp_failed；额外工具 kill。
#[test]
fn mcp_catalog_gates_extra_tools_but_keeps_missing_or_empty_catalog_nonblocking() {
    let empty = Harness::configured("mcp_error", [], Duration::from_secs(60));
    let empty_session = empty.new_session("scope-a");
    assert_send(
        empty
            .runtime
            .dispatch(send(&empty_session, "empty-catalog", "继续对话", None))
            .expect("批准集为空时 mcp/list 失败不得挡对话"),
        false,
        &empty_session,
        "empty-catalog",
    );

    let missing = Harness::configured(
        "mcp_missing",
        ["purelab__search_tracks".to_string()],
        Duration::from_secs(60),
    );
    let missing_session = missing.new_session("scope-a");
    let failure = wait_for_status(&missing, "mcp_failed");
    assert_eq!(failure.turn_id, None);
    assert_eq!(failure.submission_id, None);
    assert_eq!(failure.event_id, failure.block_id);
    assert_send(
        missing
            .runtime
            .dispatch(send(&missing_session, "missing-tool", "仍可聊天", None))
            .expect("缺少只读 MCP 时不得挡对话"),
        false,
        &missing_session,
        "missing-tool",
    );
    let mcp_failure_count = missing.count_events_with_code("mcp_failed");
    assert_eq!(mcp_failure_count, 1);
    missing
        .runtime
        .dispatch(KitCommand::ResumeSession {
            scope_id: "scope-a".to_string(),
            session_id: missing_session.clone(),
        })
        .expect("热恢复必须允许当前 mcp_failed session 继续工作");
    wait_until(|| missing.count_events_with_code("mcp_failed") == mcp_failure_count + 1);
    assert_eq!(
        missing
            .events()
            .iter()
            .filter(|event| {
                event.session_id == missing_session
                    && matches!(&event.block, KitBlock::Status { code, .. } if code == "mcp_failed")
            })
            .count(),
        2,
        "hot resume 必须从当前状态重建一次 mcp_failed"
    );

    let extra = Harness::configured(
        "mcp_extra",
        ["purelab__search_tracks".to_string()],
        Duration::from_secs(60),
    );
    let _ = extra.new_session("scope-a");
    wait_for_file(&extra.exited);
    let starts_before_rejected_command = fs::read_to_string(&extra.started)
        .expect("必须能读取 MCP 安全违例的启动记录")
        .lines()
        .count();
    let error = extra
        .runtime
        .dispatch(KitCommand::Cancel {
            scope_id: "scope-a".to_string(),
            session_id: "sidecar-session".to_string(),
        })
        .expect_err("出现未批准工具后不得自动复活 scope");
    assert_eq!(error.code, "sidecar_unavailable");
    assert_eq!(
        fs::read_to_string(&extra.started)
            .expect("必须能读取 MCP 安全违例后的启动记录")
            .lines()
            .count(),
        starts_before_rejected_command,
        "MCP 安全违例后的普通命令不得创建新 generation"
    );
    let error = extra
        .runtime
        .dispatch(send("sidecar-session", "after-extra", "不得写入", None))
        .expect_err("出现未批准工具后 sidecar 必须被 kill");
    assert_eq!(error.code, "sidecar_unavailable");
    assert_eq!(
        extra.method_count("session/prompt"),
        0,
        "kill 后不得再写 prompt"
    );
}

/// catalog 是可选能力；超时前确认 fake 未回复，超时后 Send 与 Cancel 仍可取得回执。
#[test]
fn pending_mcp_catalog_does_not_delay_send_or_cancel_reply() {
    let harness = Harness::configured_with_catalog_timeout(
        "mcp_late",
        ["purelab__search_tracks".to_string()],
        Duration::from_secs(60),
        TEST_MCP_CATALOG_TIMEOUT,
    );
    let session_id = harness.new_session("scope-a");
    harness.wait_for_catalog_request();
    assert!(
        !harness.catalog_response_was_sent(),
        "catalog response 屏障释放前 fake 不得发送 response"
    );

    // mcp_failed 是短 catalog deadline 到期的业务屏障，不用墙钟推断顺序。
    let failure = wait_for_status(&harness, "mcp_failed");
    assert_eq!(failure.session_id, session_id);
    assert_eq!(failure.turn_id, None);
    assert_eq!(failure.submission_id, None);

    let (send_result, send_done) = mpsc::sync_channel(1);
    let runtime = Arc::clone(&harness.runtime);
    let send_session_id = session_id.clone();
    thread::spawn(move || {
        let result = runtime.dispatch(send(
            &send_session_id,
            "catalog-pending-send",
            "catalog 未完成也要发送",
            None,
        ));
        send_result
            .send(result)
            .expect("测试 Send 线程必须能交付结果");
    });
    let send_result = send_done
        .recv_timeout(TEST_TIMEOUT)
        .expect("Send watchdog 防止 catalog timeout 回归死锁")
        .expect("catalog timeout 后 Send 应仍成功写入");
    assert_send(send_result, false, &session_id, "catalog-pending-send");

    let (cancel_result, cancel_done) = mpsc::sync_channel(1);
    let runtime = Arc::clone(&harness.runtime);
    let cancel_session_id = session_id.clone();
    thread::spawn(move || {
        let result = runtime.dispatch(KitCommand::Cancel {
            scope_id: "scope-a".to_string(),
            session_id: cancel_session_id,
        });
        cancel_result
            .send(result)
            .expect("测试 Cancel 线程必须能交付结果");
    });
    assert_eq!(
        cancel_done
            .recv_timeout(TEST_TIMEOUT)
            .expect("Cancel watchdog 防止 catalog timeout 回归死锁")
            .expect("catalog timeout 后 Cancel 应仍成功写入"),
        KitReply::Cancel { accepted: true }
    );
    assert!(
        !harness.catalog_response_was_sent(),
        "Send/Cancel 回执断言期间 fake 仍不得越过 catalog response 屏障"
    );

    // 业务顺序已断言完毕，再显式放行迟到 response，并等待 sidecar marker。
    harness.release_catalog();
    wait_for_file(&harness.catalog_response_sent);
    harness.wait_for_method("session/prompt");
    wait_for_file(&harness.cancel_seen);
    assert_eq!(
        harness.count_events_with_code("mcp_failed"),
        1,
        "迟到 catalog response 不得清除或重复 timeout 状态"
    );
}

/// catalog response 已入队但前一条同步 sink 事件跨过 deadline 时，迟到 response 仍必须按超时处理。
#[test]
fn queued_mcp_catalog_response_crossing_deadline_is_rejected() {
    let harness = Harness::configured_with_options(
        "mcp_queued_after_deadline",
        ["purelab__search_tracks".to_string()],
        MentionMode::Unsupported,
        Duration::from_secs(60),
        SLOW_SINK_DELAY,
        Some(TEST_MCP_CATALOG_TIMEOUT),
        None,
    );
    let session_id = harness.new_session("scope-a");
    let failure = wait_for_status(&harness, "mcp_failed");
    assert_eq!(failure.session_id, session_id);
    assert_eq!(failure.turn_id, None);
    assert_eq!(failure.submission_id, None);
    assert_eq!(
        harness.count_events_with_code("mcp_failed"),
        1,
        "跨过 deadline 的 catalog response 不得被当作成功结果"
    );
}

/// TC-HP deadline：catalog 超时后按空 catalog 降级，迟到 response 不得释放第二次 prompt。
#[test]
fn mcp_catalog_timeout_degrades_before_late_response_and_preserves_submission_idempotency() {
    let harness = Harness::configured_with_catalog_timeout(
        "mcp_late",
        ["purelab__search_tracks".to_string()],
        Duration::from_secs(60),
        TEST_MCP_CATALOG_TIMEOUT,
    );
    let session_id = harness.new_session("scope-a");
    harness.wait_for_catalog_request();
    assert!(
        !harness.catalog_response_was_sent(),
        "catalog response 屏障释放前 fake 不得发送 response"
    );

    // mcp_failed 是短 catalog deadline 到期的业务屏障，不用墙钟推断顺序。
    let failure = wait_for_status(&harness, "mcp_failed");
    assert_eq!(failure.session_id, session_id);
    assert_eq!(failure.turn_id, None);
    assert_eq!(failure.submission_id, None);

    assert_send(
        harness
            .runtime
            .dispatch(send(
                &session_id,
                "late-catalog",
                "catalog 超时后继续",
                None,
            ))
            .expect("catalog timeout 必须按空 catalog 降级并写 prompt"),
        false,
        &session_id,
        "late-catalog",
    );
    assert!(
        !harness.catalog_response_was_sent(),
        "首次 Send 回执期间 fake 仍不得越过 catalog response 屏障"
    );

    // 先完成 timeout 与首次 Send 断言，再释放迟到 response。
    harness.release_catalog();
    wait_for_file(&harness.catalog_response_sent);
    harness.wait_for_method("session/prompt");
    assert_eq!(
        harness.method_count("session/prompt"),
        1,
        "catalog timeout 只允许原 submission 写入一次 prompt"
    );
    assert_send(
        harness
            .runtime
            .dispatch(send(
                &session_id,
                "late-catalog",
                "catalog 超时后继续",
                None,
            ))
            .expect("迟到 catalog response 后原 submission 重试必须稳定幂等"),
        true,
        &session_id,
        "late-catalog",
    );
    assert_eq!(
        harness.method_count("session/prompt"),
        1,
        "迟到 catalog response 或 retry 均不得制造幽灵 prompt"
    );
    assert_eq!(
        harness.count_events_with_code("mcp_failed"),
        1,
        "迟到 catalog response 不得清除或重复 timeout 状态"
    );
}

/// 永久无 catalog response 时，多次 deadline 也必须释放 ACP 64 项出站账本。
#[test]
fn mcp_catalog_timeouts_revoke_outbound_ledger_for_permanently_unresponsive_sidecar() {
    let harness = Harness::configured_with_catalog_timeout(
        "mcp_never",
        ["purelab__search_tracks".to_string()],
        Duration::from_secs(60),
        TEST_MCP_CATALOG_TIMEOUT,
    );
    // 65 次明确越过 ACP 出站账本的 64 项上限；每次均由短 deadline 降级后继续聊天。
    for index in 0..65 {
        let session_id = harness.new_session("scope-a");
        let submission_id = format!("catalog-timeout-{index}");
        assert_send(
            harness
                .runtime
                .dispatch(send(
                    &session_id,
                    &submission_id,
                    "永久无响应 catalog 后继续",
                    None,
                ))
                .expect("每次 catalog timeout 都必须释放账本并写入 prompt"),
            false,
            &session_id,
            &submission_id,
        );
        wait_until(|| {
            harness.events().iter().any(|event| {
                event.session_id == session_id
                    && event.submission_id.as_deref() == Some(submission_id.as_str())
                    && matches!(&event.block, KitBlock::Status { code, .. } if code == "turn_completed")
            })
        });
    }

    assert_eq!(
        harness.method_count("_x.ai/mcp/list"),
        65,
        "每个 session 都必须实际发出独立 catalog request"
    );
    assert_eq!(
        harness.method_count("session/prompt"),
        65,
        "跨越 64 项账本上限后仍必须允许最后一次 prompt"
    );
    assert_eq!(
        harness
            .events()
            .iter()
            .filter(|event| matches!(&event.block, KitBlock::Status { code, .. } if code == "mcp_failed"))
            .count(),
        65,
        "每次永久无响应都必须按 catalog 失败降级"
    );
}

/// TC-IDLE / TC-AUTO：idle kill 后 Send 必须冷 load+replay 再写 prompt，load 失败可观察。
#[test]
fn idle_restart_auto_loads_before_prompt_and_reports_session_not_found() {
    let idle = Harness::configured("basic", [], Duration::from_millis(40));
    let session_id = idle.new_session("scope-a");
    wait_for_file(&idle.exited);
    let initial_starts = fs::read_to_string(&idle.started)
        .expect("必须能读取启动标记")
        .lines()
        .count();

    assert_send(
        idle.runtime
            .dispatch(send(&session_id, "after-idle", "恢复后发送", None))
            .expect("idle 后 Send 必须在 auto-load 完成后写 prompt"),
        false,
        &session_id,
        "after-idle",
    );
    wait_until(|| {
        fs::read_to_string(&idle.started)
            .map(|started| started.lines().count() > initial_starts)
            .unwrap_or(false)
    });
    // Send 只等待 Host stdin 写入；显式等 fake 消费 prompt，避免文件观察落后于 pipe flush。
    idle.wait_for_method("session/prompt");
    let wire = idle.wire();
    let load_position = wire
        .iter()
        .position(|item| item["method"] == "session/load")
        .expect("idle 后旧 session 必须先 session/load");
    assert_eq!(
        wire[load_position]["params"]["_meta"],
        json!({ "modelId": "byok" }),
        "session/load 只能发送 ACP Channel 槽名，而不是供应商模型标识"
    );
    let prompt_position = wire
        .iter()
        .rposition(|item| item["method"] == "session/prompt")
        .expect("load 后必须写 prompt");
    assert!(
        load_position < prompt_position,
        "auto-load/replay 必须先于 prompt"
    );

    let missing = Harness::configured("load_fail", [], Duration::from_secs(60));
    let error = missing
        .runtime
        .dispatch(send(CANONICAL_SESSION_ID, "gone-turn", "找不到会话", None))
        .expect_err("自动 load 失败必须返回 session_not_found");
    assert_eq!(error.code, "session_not_found");
    assert_eq!(
        missing.method_count("session/prompt"),
        0,
        "load 失败不得写 prompt"
    );
}

/// TC-CHANNEL：新配置先提交和失效旧代，再重启所有活跃 scope；失败仍保留 committed view。
#[test]
fn channel_change_restarts_live_scopes_and_preserves_committed_view_after_restart_failure() {
    let harness = Harness::configured("basic", [], Duration::from_secs(60));
    let _first = harness.new_session("scope-a");
    let _second = harness.new_session("scope-b");
    wait_until(|| {
        fs::read_to_string(&harness.started)
            .map(|started| started.lines().count() == 2)
            .unwrap_or(false)
    });

    let reply = harness
        .runtime
        .dispatch(KitCommand::SetLlmChannel {
            kind: None,
            base_url: None,
            model_id: None,
            relay_base_url: None,
            app_key: None,
            api_key: Some("rotated-test-key".to_string()),
            access_token: None,
            client_request_id: Some("channel-change".to_string()),
        })
        .expect("已配置 scope 的 Channel 变更必须重启所有 live scope");
    assert!(matches!(
        reply,
        KitReply::LlmChannelView {
            channel: efflab_agent_host::LlmChannelView {
                kind: Some(LlmChannelKind::Byok),
                key_present: true,
                ..
            }
        }
    ));
    wait_until(|| {
        fs::read_to_string(&harness.started)
            .map(|started| started.lines().count() == 4)
            .unwrap_or(false)
    });

    // 第二次测试用故意移除 executable 模拟部分 restart 失败；新 view 不得回退。
    let failing = Harness::configured("basic", [], Duration::from_secs(60));
    let _ = failing.new_session("scope-a");
    let original_sidecar = fs::read(&failing.sidecar).expect("必须能备份测试 executable");
    fs::remove_file(&failing.sidecar).expect("必须能移除二次 spawn 的测试 executable");
    let error = failing
        .runtime
        .dispatch(KitCommand::SetLlmChannel {
            kind: None,
            base_url: None,
            model_id: None,
            relay_base_url: None,
            app_key: None,
            api_key: Some("new-test-key".to_string()),
            access_token: None,
            client_request_id: Some("channel-failure".to_string()),
        })
        .expect_err("restart 失败必须返回可重试错误");
    assert!(error.retryable, "已提交但 restart 失败必须可重试");
    assert_eq!(
        failing
            .runtime
            .dispatch(KitCommand::GetLlmChannelView)
            .expect("失败后仍必须读取 committed 新 view"),
        KitReply::LlmChannelView {
            channel: efflab_agent_host::LlmChannelView {
                kind: Some(LlmChannelKind::Byok),
                key_present: true,
                token_present: false,
                model_selectable: true,
                base_url: Some("https://8.8.8.8/v1".to_string()),
                model_id: Some("fake-byok-model".to_string()),
            }
        }
    );

    // 相同请求在配置已提交后仍必须重试此前失败的 live scope，而不是空操作。
    let starts_before_retry = fs::read_to_string(&failing.started)
        .expect("必须能读取失败前启动记录")
        .lines()
        .count();
    fs::write(&failing.sidecar, original_sidecar).expect("必须能恢复测试 executable");
    fs::set_permissions(&failing.sidecar, fs::Permissions::from_mode(0o700))
        .expect("恢复后的 fake sidecar 必须可执行");
    failing
        .runtime
        .dispatch(KitCommand::SetLlmChannel {
            kind: None,
            base_url: None,
            model_id: None,
            relay_base_url: None,
            app_key: None,
            api_key: Some("new-test-key".to_string()),
            access_token: None,
            client_request_id: Some("channel-failure".to_string()),
        })
        .expect("相同 Set 必须重试已失败的 scope restart");
    wait_until(|| {
        fs::read_to_string(&failing.started)
            .map(|started| started.lines().count() == starts_before_retry + 1)
            .unwrap_or(false)
    });
    assert_send(
        failing
            .runtime
            .dispatch(send(
                "sidecar-session",
                "recovered-after-retry",
                "恢复后可继续服务",
                None,
            ))
            .expect("重试后的 scope 必须可再次服务"),
        false,
        "sidecar-session",
        "recovered-after-retry",
    );
}
