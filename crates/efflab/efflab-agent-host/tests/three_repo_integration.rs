//! Task24 的最小三仓边界测试（Unix-only）。
//!
//! 该文件依赖 Unix FIFO 与 shell fake sidecar；Windows 不执行这些运行时测试，
//! Windows 的 capability/unavailable 门禁见 `pr0_windows_hardening.rs`。
//! 该文件只通过 HostRuntime 的公开 Kit 入口和 ACP stdio wire 观察契约；Web/Tauri
//! 的行为由各自仓库的分层测试验证，不在 Rust 中伪造执行。MCP 用例在 Host
//! catalog wire 处收口，真实 HTTP MCP transport 仍由 sidecar 分层测试负责。

#![cfg(unix)]

use std::collections::BTreeSet;
use std::ffi::CString;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;
use efflab_agent_host::{
    ApprovedMcpConfig, ApprovedMcpSpec, HostApp, HostRuntime, HostRuntimeConfig, KitBlock,
    KitCommand, KitEventSink, KitProductEvent, KitReply, LlmChannelConfig, LlmChannelKind,
    LlmSecretSlot, McpServerSpec, ScopeId, SealedSecret, SecretGuard,
};
use serde_json::{Value, json};

/// 单个 wire/事件条件的最长等待时间；所有等待都绑定到可观察条件。
const TEST_TIMEOUT: Duration = Duration::from_secs(8);
/// fake sidecar 返回的固定已知会话 ID；测试不能从请求值建立会话 oracle。
const CANONICAL_SESSION_ID: &str = "sidecar-session";
/// 测试适配器中的秘密哨兵；它只存在于 HostApp 内存配置。
const SECRET_SENTINEL: &str = "task24-secret-sentinel";

/// fake sidecar 的固定行为集合，避免测试把 ACP 组包复制到产品或 Web。
#[derive(Clone, Copy)]
enum SidecarScenario {
    /// 发送两回合、未知 update 与 cold replay 历史。
    Transcript,
    /// 只响应 Host 的基础 session/new，不触发 catalog。
    EmptyCatalog,
    /// catalog 返回一个已批准的 HTTP 工具。
    CatalogReady,
    /// catalog 缺少已批准工具。
    CatalogMissing,
    /// catalog 返回未批准工具，触发 Host 安全终止。
    CatalogExtra,
}

impl SidecarScenario {
    /// 把场景映射为 shell sidecar 使用的固定标识。
    fn as_str(self) -> &'static str {
        match self {
            Self::Transcript => "transcript",
            Self::EmptyCatalog => "empty_catalog",
            Self::CatalogReady => "catalog_ready",
            Self::CatalogMissing => "catalog_missing",
            Self::CatalogExtra => "catalog_extra",
        }
    }
}

/// 只保存测试需要的产品领域状态；凭据不进入文件、wire 或 Debug。
struct FakeApp {
    channel: Arc<Mutex<LlmChannelConfig>>,
    approved_mcp: ApprovedMcpSpec,
}

impl FakeApp {
    /// 构造一个带内存密封凭据和指定 MCP 批准集的最小产品端口。
    fn new(approved_mcp: ApprovedMcpSpec) -> Self {
        Self {
            channel: Arc::new(Mutex::new(LlmChannelConfig::Byok {
                base_url: "https://8.8.8.8/v1".to_string(),
                model_id: "task24-model".to_string(),
                api_key: SealedSecret::new(SECRET_SENTINEL.as_bytes().to_vec()),
            })),
            approved_mcp,
        }
    }
}

impl HostApp for FakeApp {
    /// 返回测试产品的稳定 app id，供 Host 生成 scope 私有路径。
    fn app_id(&self) -> &str {
        "task24-three-repo"
    }

    /// 在测试内存中更新 committed Channel，不模拟产品持久化实现。
    fn persist_llm_channel(&self, config: &LlmChannelConfig) -> Result<()> {
        *self.channel.lock().expect("测试 Channel 锁必须可用") = config.clone();
        Ok(())
    }

    /// 返回当前内存 Channel 配置，保持与产品 adapter 的读取边界一致。
    fn load_llm_channel(&self) -> Result<LlmChannelConfig> {
        Ok(self
            .channel
            .lock()
            .expect("测试 Channel 锁必须可用")
            .clone())
    }

    /// 测试密封端口只在内存中复制字节，便于验证 Host 不会自行落盘秘密。
    fn seal_secret(&self, plain: &[u8]) -> Result<SealedSecret> {
        Ok(SealedSecret::new(plain.to_vec()))
    }

    /// 测试解封端口只在受控 Host 调用期间返回短生命周期守卫。
    fn unseal_secret(&self, sealed: &SealedSecret) -> Result<SecretGuard> {
        Ok(SecretGuard::new(sealed.as_bytes().to_vec()))
    }

    /// 按 Channel 槽位复用内存密封边界，不引入产品或 sidecar 依赖。
    fn seal_llm_secret(&self, slot: LlmSecretSlot, plain: &[u8]) -> Result<SealedSecret> {
        match slot {
            LlmSecretSlot::Byok => self.seal_secret(plain),
            LlmSecretSlot::Relay => Err(anyhow::anyhow!("测试不启用 Relay 槽")),
        }
    }

    /// 按 Channel 槽位复用内存解封边界，不把明文送入 ACP wire。
    fn unseal_llm_secret(&self, slot: LlmSecretSlot, sealed: &SealedSecret) -> Result<SecretGuard> {
        match slot {
            LlmSecretSlot::Byok => self.unseal_secret(sealed),
            LlmSecretSlot::Relay => Err(anyhow::anyhow!("测试不启用 Relay 槽")),
        }
    }

    /// 返回当前 scope 的已审核 MCP 规格，和 Host 真实 spawn 路径相同。
    fn mcp_for_scope(&self, _scope: &ScopeId) -> Result<ApprovedMcpSpec> {
        Ok(self.approved_mcp.clone())
    }
}

/// 带条件变量的产品事件 sink；测试等待事件而不是轮询或固定睡眠。
struct RecordingSink {
    state: Arc<RecordedEvents>,
}

/// 记录已通过 Host 校验的 Kit 产品事件，并在每次写入时唤醒等待者。
struct RecordedEvents {
    events: Mutex<Vec<KitProductEvent>>,
    changed: Condvar,
}

impl RecordingSink {
    /// 构造一个共享事件观察器，供 Harness 和 Host sink 同时持有。
    fn new() -> (Self, Arc<RecordedEvents>) {
        let state = Arc::new(RecordedEvents {
            events: Mutex::new(Vec::new()),
            changed: Condvar::new(),
        });
        (
            Self {
                state: Arc::clone(&state),
            },
            state,
        )
    }
}

impl KitEventSink for RecordingSink {
    /// 记录产品可见事件；ValidatedKitEventSink 已在 Host 内层先执行协议校验。
    fn emit(&self, event: KitProductEvent) -> Result<()> {
        self.state
            .events
            .lock()
            .expect("事件锁必须可用")
            .push(event);
        self.state.changed.notify_all();
        Ok(())
    }
}

/// 真正由 HostRuntime 启动的 fake sidecar 与可观察 wire/事件产物。
struct Harness {
    runtime: Arc<HostRuntime>,
    _temporary: tempfile::TempDir,
    events: Arc<RecordedEvents>,
    captured: PathBuf,
    started: PathBuf,
    prompt_waiting: PathBuf,
    unknown_sent: PathBuf,
    cancel_gate: PathBuf,
    cancel_seen: PathBuf,
    load_waiting: PathBuf,
    load_gate: PathBuf,
    load_completed: PathBuf,
    catalog_seen: PathBuf,
    expected_session_cwd: PathBuf,
}

impl Harness {
    /// 构造一个采用真实 Host supervisor/ACP stdio 的最小场景。
    fn new(scenario: SidecarScenario, approved_mcp: ApprovedMcpSpec) -> Self {
        let temporary = tempfile::tempdir().expect("必须能创建 Task24 临时目录");
        let root = temporary.path();
        let sidecar = root.join("task24-sidecar.sh");
        let started = root.join("started");
        let captured = root.join("wire.jsonl");
        let prompt_waiting = root.join("prompt-waiting");
        let unknown_sent = root.join("unknown-sent");
        let cancel_gate = root.join("cancel-gate");
        let cancel_seen = root.join("cancel-seen");
        let load_waiting = root.join("load-waiting");
        let load_gate = root.join("load-gate");
        let load_completed = root.join("load-completed");
        let catalog_seen = root.join("catalog-seen");
        let canonical_root = fs::canonicalize(root).expect("测试临时根必须能 canonicalize");
        let expected_scope_root = canonical_root.join("app-data/task24-three-repo/scope-a");
        let expected_home = expected_scope_root.join("home");
        let expected_session_cwd = expected_scope_root.join("workspace");

        create_fifo(&cancel_gate);
        create_fifo(&load_gate);
        write_fake_sidecar(
            &sidecar,
            &started,
            &captured,
            &prompt_waiting,
            &unknown_sent,
            &cancel_gate,
            &cancel_seen,
            &load_waiting,
            &load_gate,
            &load_completed,
            &catalog_seen,
            &expected_home,
            &expected_session_cwd,
            scenario,
        );

        let (sink, events) = RecordingSink::new();
        let config = HostRuntimeConfig {
            home_root: root.join("app-data"),
            sidecar_bin: sidecar,
            sidecar_log_path: root.join("sidecar.log"),
            mcp_exec_root: root.join("mcp"),
            idle_after: Duration::from_secs(60),
            l3b: Default::default(),
        };
        let runtime = HostRuntime::new(FakeApp::new(approved_mcp), sink, config);

        Self {
            runtime: Arc::new(runtime),
            _temporary: temporary,
            events,
            captured,
            started,
            prompt_waiting,
            unknown_sent,
            cancel_gate,
            cancel_seen,
            load_waiting,
            load_gate,
            load_completed,
            catalog_seen,
            expected_session_cwd,
        }
    }

    /// 构造 transcript 场景，默认使用空 MCP 批准集。
    fn transcript() -> Self {
        Self::new(SidecarScenario::Transcript, ApprovedMcpSpec::default())
    }

    /// 构造指定 catalog 响应的 Host wire 场景。
    fn catalog(scenario: SidecarScenario, approved_mcp: ApprovedMcpSpec) -> Self {
        Self::new(scenario, approved_mcp)
    }

    /// 通过真实 HostRuntime 请求 session/new，并返回 sidecar 的 session id。
    fn new_session(&self) -> String {
        match self
            .runtime
            .dispatch(KitCommand::NewSession {
                scope_id: "scope-a".to_string(),
                client_request_id: Some("task24-new".to_string()),
            })
            .expect("session/new 必须得到 Host reply")
        {
            KitReply::NewSession { session_id } => {
                assert_eq!(session_id, CANONICAL_SESSION_ID);
                session_id
            }
            other => panic!("预期 NewSession reply，实际为 {other:?}"),
        }
    }

    /// 通过 Kit Send 写入一个 prompt，并返回 Host 的立即受理回执。
    fn send(&self, session_id: &str, submission_id: &str, text: &str) -> KitReply {
        self.runtime
            .dispatch(KitCommand::Send {
                scope_id: "scope-a".to_string(),
                session_id: session_id.to_string(),
                submission_id: submission_id.to_string(),
                text: text.to_string(),
                mentions: None,
            })
            .expect("Send 必须得到结构化 reply")
    }

    /// 等待指定 turn 终态，证明 prompt result 或 cancel 结算已经进入产品 sink。
    fn send_and_wait(&self, session_id: &str, submission_id: &str, text: &str) {
        assert_send_reply(self.send(session_id, submission_id, text), submission_id);
        self.wait_for_events(|events| {
            events.iter().any(|event| {
                event.turn_id.as_deref() == Some(submission_id)
                    && matches!(
                        &event.block,
                        KitBlock::Status { code, .. }
                            if matches!(code.as_str(), "turn_completed" | "cancelled" | "error")
                    )
            })
        });
    }

    /// 启动第二回合并等待 fake sidecar 已发送第一段 live transcript。
    fn start_stream(&self, session_id: &str, submission_id: &str, text: &str) {
        assert_send_reply(self.send(session_id, submission_id, text), submission_id);
        wait_for_path(&self.prompt_waiting);
    }

    /// 等待 sidecar 在两段 live chunk 之间发出未知 update。
    fn inject_unknown_update_between_chunks(&self) {
        wait_for_path(&self.unknown_sent);
    }

    /// 发送一次 cancel notification，释放 fake prompt gate，并等待 sidecar 消费该行。
    fn cancel_current_turn(&self, session_id: &str, submission_id: &str) {
        let reply = self
            .runtime
            .dispatch(KitCommand::Cancel {
                scope_id: "scope-a".to_string(),
                session_id: session_id.to_string(),
            })
            .expect("Cancel 必须得到立即回执");
        assert_eq!(reply, KitReply::Cancel { accepted: true });
        release_fifo(&self.cancel_gate);
        wait_for_path(&self.cancel_seen);
        self.wait_for_events(|events| {
            events.iter().any(|event| {
                event.turn_id.as_deref() == Some(submission_id)
                    && matches!(&event.block, KitBlock::Status { code, .. } if code == "cancelled")
            })
        });
    }

    /// 通过公开 Channel 变更入口重启 sidecar，再验证同一 cold load 的边界。
    fn restart_sidecar_and_resume(&self, session_id: &str) {
        let reply = self
            .runtime
            .dispatch(KitCommand::SetLlmChannel {
                kind: Some(LlmChannelKind::Byok),
                base_url: None,
                model_id: Some("task24-rotated-model".to_string()),
                relay_base_url: None,
                app_key: None,
                api_key: Some("task24-rotated-key".to_string()),
                access_token: None,
                client_request_id: Some("task24-rotate".to_string()),
            })
            .expect("Channel restart 必须完成旧 actor cleanup 和新 actor spawn");
        assert!(matches!(reply, KitReply::LlmChannelView { .. }));
        wait_for_started_count(&self.started, 2);

        // 两个真实调用方在屏障后同时请求同一 cold flight。
        let start = Arc::new(Barrier::new(3));
        let joins = (0..2)
            .map(|_| {
                let runtime = Arc::clone(&self.runtime);
                let start = Arc::clone(&start);
                let session_id = session_id.to_string();
                thread::spawn(move || {
                    start.wait();
                    runtime.dispatch(KitCommand::ResumeSession {
                        scope_id: "scope-a".to_string(),
                        session_id,
                    })
                })
            })
            .collect::<Vec<_>>();
        start.wait();
        wait_for_path(&self.load_waiting);
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
                    session_id: session_id.to_string(),
                }
            );
        }
        let busy = self.resume_error("other-session");
        assert_eq!(busy.code, "session_busy");

        // 先释放 fake sidecar，再等它明确消费并回复 load，最后才读取 wire 计数。
        release_fifo(&self.load_gate);
        wait_for_marker_count(&self.load_completed, 1);
        assert_eq!(
            self.session_load_count(),
            1,
            "同一 cold flight 只能写一条 session/load"
        );
        self.wait_for_events(|events| {
            events.iter().filter(|event| {
                matches!(&event.block, KitBlock::Status { code, .. } if code == "replay_complete")
            }).count() == 1
        });
    }

    /// 通过公开 ResumeSession 触发 hot resume，并等待本次 replay fence。
    fn hot_resume(&self, session_id: &str, expected_fences: usize) {
        assert_eq!(
            self.resume(session_id),
            KitReply::ResumeSession {
                accepted: true,
                session_id: session_id.to_string(),
            }
        );
        self.wait_for_events(|events| {
            events.iter().filter(|event| {
                matches!(&event.block, KitBlock::Status { code, .. } if code == "replay_complete")
            }).count() >= expected_fences
        });
    }

    /// 发送 ResumeSession 并返回立即 accepted reply。
    fn resume(&self, session_id: &str) -> KitReply {
        self.runtime
            .dispatch(KitCommand::ResumeSession {
                scope_id: "scope-a".to_string(),
                session_id: session_id.to_string(),
            })
            .expect("Resume 必须得到结构化 reply")
    }

    /// 发送 ResumeSession 并返回结构化失败，供 session/load busy 边界断言。
    fn resume_error(&self, session_id: &str) -> efflab_agent_host::KitError {
        self.runtime
            .dispatch(KitCommand::ResumeSession {
                scope_id: "scope-a".to_string(),
                session_id: session_id.to_string(),
            })
            .expect_err("预期指定 session 在 cold load 期间 busy")
    }

    /// 请求 session/list，强制 actor 在处理下一个命令前排空 catalog response。
    fn list_sessions(&self) -> KitReply {
        self.runtime
            .dispatch(KitCommand::ListSessions {
                scope_id: "scope-a".to_string(),
                cursor: None,
            })
            .expect("session/list 必须得到结构化 reply")
    }

    /// 返回当前完整 ACP JSONL 前缀快照；最终 wire 断言必须先等待完成 marker。
    ///
    /// 该快照只描述读取瞬间已经写完换行的内容，不代表 sidecar 已停止追加。
    fn wire(&self) -> Vec<Value> {
        wait_for_path(&self.captured);
        read_complete_json_lines(&self.captured)
    }

    /// 统计 Host 到 sidecar 的某个 method 写入次数。
    fn method_count(&self, method: &str) -> usize {
        self.wire()
            .iter()
            .filter(|item| item.get("method").and_then(Value::as_str) == Some(method))
            .count()
    }

    /// 统计 cold `session/load` request 数量。
    fn session_load_count(&self) -> usize {
        self.method_count("session/load")
    }

    /// 等待 fake sidecar 已收到 MCP catalog 请求。
    fn wait_for_catalog(&self) {
        wait_for_path(&self.catalog_seen);
    }

    /// 返回最近的 assistant 快照；这是 Host wire-visible 事件，不是 Web reducer 执行结果。
    fn visible_assistant_text(&self, submission_id: &str) -> String {
        self.events()
            .into_iter()
            .rev()
            .find_map(|event| match event.block {
                KitBlock::Assistant { markdown, .. }
                    if event.turn_id.as_deref() == Some(submission_id) =>
                {
                    Some(markdown)
                }
                _ => None,
            })
            .unwrap_or_default()
    }

    /// 返回当前 sink 收到的产品事件快照，供跨仓字段断言使用。
    fn visible_messages(&self) -> Vec<KitProductEvent> {
        self.events()
    }

    /// 统计产品可见的 completed terminal 数量。
    fn completed_turn_count(&self) -> usize {
        self.events().iter().filter(|event| {
            matches!(&event.block, KitBlock::Status { code, .. } if code == "turn_completed")
        }).count()
    }

    /// 判断指定 turn 是否出现 completed terminal。
    fn has_completed_turn(&self, submission_id: &str) -> bool {
        self.events().iter().any(|event| {
            event.turn_id.as_deref() == Some(submission_id)
                && matches!(&event.block, KitBlock::Status { code, .. } if code == "turn_completed")
        })
    }

    /// 统计每个 replay 批次的 assistant 快照数量，验证 hot resume 不重复组块。
    fn replay_assistant_count(&self, submission_id: &str) -> usize {
        self.events()
            .iter()
            .filter(|event| {
                event.origin == efflab_agent_host::Origin::Replay
                    && event.turn_id.as_deref() == Some(submission_id)
                    && matches!(&event.block, KitBlock::Assistant { .. })
            })
            .count()
    }

    /// 等待一个产品事件条件，使用条件变量而非固定时延。
    fn wait_for_events(&self, condition: impl Fn(&[KitProductEvent]) -> bool) {
        let deadline = Instant::now() + TEST_TIMEOUT;
        let mut events = self.events.events.lock().expect("事件锁必须可用");
        loop {
            if condition(&events) {
                return;
            }
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                panic!("等待产品事件条件超时");
            };
            if remaining.is_zero() {
                panic!("等待产品事件条件超时");
            }
            let (next, _) = self
                .events
                .changed
                .wait_timeout(events, remaining)
                .expect("事件条件变量必须可用");
            events = next;
        }
    }

    /// 扫描临时目录中的普通文件，确认秘密哨兵未进入 runtime config、wire 或日志。
    fn assert_secret_not_persisted(&self) {
        let contents = read_regular_files(&self._temporary.path().to_path_buf());
        assert!(
            !contents.contains(SECRET_SENTINEL),
            "secret sentinel 不得落盘或进入 sidecar wire/log"
        );
    }

    /// 返回 Host 生成的 scope runtime config，供 HTTP MCP URL 边界断言。
    fn runtime_config(&self) -> String {
        let path = self
            ._temporary
            .path()
            .join("app-data/task24-three-repo/scope-a/home/runtime-config.v1.toml");
        fs::read_to_string(path).expect("Host 必须生成 v1 runtime config")
    }

    /// 返回当前事件快照的独立副本，避免在条件等待外持有锁。
    fn events(&self) -> Vec<KitProductEvent> {
        self.events.events.lock().expect("事件锁必须可用").clone()
    }
}

/// 断言 Send 的立即回执只确认指定 submission 已写入 Host actor。
fn assert_send_reply(reply: KitReply, submission_id: &str) {
    assert!(matches!(
        reply,
        KitReply::Send {
            accepted: true,
            duplicate: false,
            ref turn_id,
            submission_id: ref returned_submission,
            ..
        } if turn_id == submission_id && returned_submission == submission_id
    ));
}

/// 对真实 capture 快照执行 ACP session 请求的闭集字段断言。
fn assert_session_wire_contract(
    wire: &[Value],
    expected_session_id: &str,
    expected_session_cwd: &Path,
) {
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
                assert_eq!(params["cwd"], expected_session_cwd.display().to_string());
                assert_eq!(params["mcpServers"], json!([]));
                assert_eq!(params["_meta"], json!({ "modelId": "byok" }));
            }
            "session/list" => {
                let expected = if params.get("cursor").is_some() {
                    ["cursor", "cwd"].as_slice()
                } else {
                    ["cwd"].as_slice()
                };
                assert_exact_object_fields(params, expected, method);
                assert_eq!(params["cwd"], expected_session_cwd.display().to_string());
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
                assert_eq!(params["cwd"], expected_session_cwd.display().to_string());
                assert_eq!(params["mcpServers"], json!([]));
                assert_eq!(params["_meta"], json!({ "modelId": "byok" }));
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
                assert_eq!(params["sessionId"], expected_session_id);
            }
            _ => panic!("未知 Host 出站 ACP method: {method}"),
        }
    }
}

/// 严格比较 JSON object 的 key 集合，旧参数和未知参数都必须被测试发现。
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

/// 构造一个已审核的 loopback HTTP MCP 规格和 qualified tool 集合。
fn approved_http_mcp() -> ApprovedMcpSpec {
    let mut servers = ApprovedMcpConfig::default();
    servers.servers.insert(
        "mcp".to_string(),
        McpServerSpec::Http {
            url: "http://127.0.0.1:4313/mcp".to_string(),
        },
    );
    ApprovedMcpSpec::from_approved(servers, BTreeSet::from(["mcp__search".to_string()]))
        .expect("合法 loopback HTTP MCP 规格必须通过审核")
}

/// 在指定时间上限内等待 sidecar 写出一个确定性 marker 文件。
fn wait_for_path(path: &Path) {
    let deadline = Instant::now() + TEST_TIMEOUT;
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "等待测试 marker 超时: {}",
            path.display()
        );
        thread::yield_now();
    }
}

/// 等待 fake sidecar 写出预期数量的完成 marker，避免在 gate 前读取计数。
fn wait_for_marker_count(path: &Path, expected: usize) {
    let deadline = Instant::now() + TEST_TIMEOUT;
    loop {
        let count = fs::read_to_string(path)
            .map(|contents| contents.lines().count())
            .unwrap_or(0);
        if count >= expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "等待完成 marker 超时: {}",
            path.display()
        );
        thread::yield_now();
    }
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

    OpenOptions::new()
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

/// 等待指定 sidecar generation 已启动，避免依赖固定启动时延。
fn wait_for_started_count(path: &Path, expected: usize) {
    let deadline = Instant::now() + TEST_TIMEOUT;
    loop {
        let count = fs::read_to_string(path)
            .map(|source| source.lines().count())
            .unwrap_or(0);
        if count >= expected {
            return;
        }
        assert!(Instant::now() < deadline, "等待 sidecar generation 超时");
        thread::yield_now();
    }
}

/// 向阻塞在读端的 FIFO 写入释放信号，让 fake sidecar 继续消费下一条 wire。
fn release_fifo(path: &Path) {
    let mut writer = OpenOptions::new()
        .write(true)
        .open(path)
        .expect("测试 FIFO 读端必须已经就绪");
    writer.write_all(b"release\n").expect("必须能释放测试 FIFO");
}

/// 创建只供当前测试进程使用的 FIFO；阻塞等待本身由 sidecar 条件消费完成。
fn create_fifo(path: &Path) {
    let c_path = CString::new(path.as_os_str().as_bytes()).expect("临时 FIFO 路径不能含 NUL");
    // SAFETY: c_path 指向当前进程拥有的临时路径，mkfifo 不借用 Rust 内存之外的数据。
    let result = unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) };
    assert_eq!(result, 0, "必须能创建测试 FIFO: {}", path.display());
}

/// 把临时路径编码为 POSIX shell 单引号字面量，避免路径参数注入 shell。
fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.display().to_string().replace('\'', "'\\''"))
}

#[test]
fn shell_quote_escapes_literal_apostrophe_without_changing_path() {
    assert_eq!(
        shell_quote(Path::new("task24/host's launch")),
        "'task24/host'\\''s launch'"
    );
}

/// 读取临时目录中的普通文件内容；跳过 FIFO，避免扫描秘密时阻塞测试。
fn read_regular_files(root: &Path) -> String {
    let mut contents = String::new();
    append_regular_files(root, &mut contents);
    contents
}

/// 递归收集普通文件字节，测试只把它们当不透明文本检查秘密哨兵。
fn append_regular_files(path: &Path, contents: &mut String) {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return;
    };
    if metadata.is_dir() {
        let Ok(entries) = fs::read_dir(path) else {
            return;
        };
        for entry in entries.flatten() {
            append_regular_files(&entry.path(), contents);
        }
    } else if metadata.is_file()
        && let Ok(bytes) = fs::read(path)
    {
        contents.push_str(&String::from_utf8_lossy(&bytes));
    }
}

/// 将固定行为写成可执行 stdio sidecar；脚本不包含 ACP 业务组包逻辑。
#[allow(clippy::too_many_arguments)]
fn write_fake_sidecar(
    sidecar: &Path,
    started: &Path,
    captured: &Path,
    prompt_waiting: &Path,
    unknown_sent: &Path,
    cancel_gate: &Path,
    cancel_seen: &Path,
    load_waiting: &Path,
    load_gate: &Path,
    load_completed: &Path,
    catalog_seen: &Path,
    expected_home: &Path,
    expected_session_cwd: &Path,
    scenario: SidecarScenario,
) {
    let script = r#"#!/bin/sh
mode='__MODE__'
started=__STARTED__
captured=__CAPTURED__
prompt_waiting=__PROMPT_WAITING__
unknown_sent=__UNKNOWN_SENT__
cancel_gate=__CANCEL_GATE__
cancel_seen=__CANCEL_SEEN__
load_waiting=__LOAD_WAITING__
load_gate=__LOAD_GATE__
load_completed=__LOAD_COMPLETED__
catalog_seen=__CATALOG_SEEN__
expected_home=__EXPECTED_HOME__
expected_session_cwd=__EXPECTED_SESSION_CWD__
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
# 启动前验证 Host 的 v1 配置和短生命周期 binding，绝不读取或落盘用户 Key。
test "$home" = "$expected_home" || exit 25
test "$session_cwd" = "$expected_session_cwd" || exit 26
test "$runtime_config" = "$expected_home/runtime-config.v1.toml" || exit 27
test -n "${EFFLAB_L3B_BIND:-}" || exit 41
test -n "$home" || exit 42
test -n "$runtime_config" || exit 43
test -f "$runtime_config" || exit 44
/usr/bin/grep -q '^schema_version = 1$' "$runtime_config" || exit 46
/usr/bin/grep -q '^backend = "chat_completions"$' "$runtime_config" || exit 47
/usr/bin/grep -q '^token_env = "EFFLAB_L3B_BIND"$' "$runtime_config" || exit 48
/usr/bin/grep -q '^session_cwd = ' "$runtime_config" || exit 49
/usr/bin/printf '%s\n' started >> "$started"
generation=$(/usr/bin/wc -l < "$started" | /usr/bin/tr -d '[:space:]')
new_session_count=0
load_count=0
canonical_session='sidecar-session'
while IFS= read -r line; do
  /usr/bin/printf '%s\n' "$line" >> "$captured"
  id=$(/usr/bin/printf '%s\n' "$line" | /usr/bin/sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  session=$(/usr/bin/printf '%s\n' "$line" | /usr/bin/sed -n 's/.*"sessionId":"\([^"]*\)".*/\1/p')
  cwd=$(/usr/bin/printf '%s\n' "$line" | /usr/bin/sed -n 's/.*"cwd":"\([^"]*\)".*/\1/p')
  prompt=$(/usr/bin/printf '%s\n' "$line" | /usr/bin/sed -n 's/.*"promptId":"\([^"]*\)".*/\1/p')
  case "$line" in
    *'"method":"initialize"'*)
      /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":1,"agentCapabilities":{"loadSession":true,"promptCapabilities":{"image":false,"audio":false,"embeddedContext":false},"mcpCapabilities":{"http":false,"sse":false},"sessionCapabilities":{"list":{}},"auth":{}},"authMethods":[],"_meta":{"efflabRuntime":"minimal-v1","efflabSchemaVersion":1,"efflabSessionStoreVersion":1}}}\n' "$id"
      ;;
    *'"method":"session/new"'*)
      new_session_count=$((new_session_count + 1))
      test "$cwd" = "$expected_session_cwd" || exit 51
      /usr/bin/printf '%s' "$line" | /usr/bin/grep -q '"mcpServers":\[\]' || exit 52
      /usr/bin/printf '%s' "$line" | /usr/bin/grep -q '"_meta":{"modelId":"byok"}' || exit 58
      /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{"sessionId":"%s"}}\n' "$id" "$canonical_session"
      ;;
    *'"method":"session/list"'*)
      test "$cwd" = "$expected_session_cwd" || exit 53
      case "$line" in
        *'"limit":'*|*'"_meta":'*) exit 59 ;;
      esac
      /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{"sessions":[],"nextCursor":null}}\n' "$id"
      ;;
    *'"method":"session/load"'*)
      load_count=$((load_count + 1))
      test "$cwd" = "$expected_session_cwd" || exit 54
      /usr/bin/printf '%s' "$line" | /usr/bin/grep -q '"mcpServers":\[\]' || exit 55
      /usr/bin/printf '%s' "$line" | /usr/bin/grep -q '"_meta":{"modelId":"byok"}' || exit 60
      test "$session" = "$canonical_session" || exit 56
      /usr/bin/printf '%s\n' waiting > "$load_waiting"
      IFS= read -r release < "$load_gate"
      # 该未知 replay update 只验证 Host projector 的内部跳过，不进入 Kit 产品事件。
      /usr/bin/printf '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"%s","update":{"sessionUpdate":"future_replay_update","payload":"old-diagnostic"},"_meta":{"isReplay":true,"promptId":"old-prompt","eventId":"old-event"}}}\n' "$session"
      /usr/bin/printf '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"%s","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"first-reply"}},"_meta":{"isReplay":true,"promptId":"prompt-a","eventId":"cold-first-event"}}}\n' "$session"
      /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id"
      # response 已写出，显式标记 fake sidecar 已消费本次 load。
      /usr/bin/printf '%s\n' consumed >> "$load_completed"
      ;;
    *'"method":"_x.ai/mcp/list"'*)
      # catalog 只能针对 fake 已知 session，不能从请求值建立 oracle。
      test "$session" = "$canonical_session" || exit 57
      case "$line" in
        *'"params":{"sessionId":"sidecar-session"}}') ;;
        *) exit 61 ;;
      esac
      /usr/bin/printf '%s\n' seen > "$catalog_seen"
      if [ "$mode" = "empty_catalog" ]; then
        # 空批准集禁止发送 catalog；收到 mcp/list 即让 fake sidecar fail-fast。
        exit 62
      elif [ "$mode" = "catalog_ready" ]; then
        /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{"result":{"servers":[{"name":"mcp","session":{"status":"ready","tools":[{"name":"search","enabled":true}]}}]}}}\n' "$id"
      elif [ "$mode" = "catalog_missing" ]; then
        /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{"result":{"servers":[{"name":"mcp","session":{"status":"unavailable","tools":[]}}]}}}\n' "$id"
      else
        /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{"result":{"servers":[{"name":"unexpected","session":{"status":"ready","tools":[{"name":"writeback","enabled":true}]}}]}}}\n' "$id"
      fi
      ;;
    *'"method":"session/prompt"'*)
      if [ "$prompt" = "prompt-a" ]; then
        /usr/bin/printf '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"%s","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"first-reply"}},"_meta":{"promptId":"prompt-a","eventId":"live-first-event"}}}\n' "$session"
        /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id"
      elif [ "$prompt" = "prompt-b" ]; then
        /usr/bin/printf '%s\n' waiting > "$prompt_waiting"
        /usr/bin/printf '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"%s","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"partial-b"}},"_meta":{"promptId":"prompt-b","eventId":"live-second-event"}}}\n' "$session"
        /usr/bin/printf '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"%s","update":{"sessionUpdate":"future_live_update","payload":"unknown-b"},"_meta":{"promptId":"prompt-b","eventId":"unknown-b-event"}}}\n' "$session"
        /usr/bin/printf '%s\n' sent > "$unknown_sent"
        IFS= read -r release < "$cancel_gate"
      fi
      ;;
    *'"method":"session/cancel"'*)
      /usr/bin/printf '%s\n' seen > "$cancel_seen"
      ;;
  esac
done
"#;
    let script = script
        .replace("__MODE__", scenario.as_str())
        .replace("__STARTED__", &shell_quote(started))
        .replace("__CAPTURED__", &shell_quote(captured))
        .replace("__PROMPT_WAITING__", &shell_quote(prompt_waiting))
        .replace("__UNKNOWN_SENT__", &shell_quote(unknown_sent))
        .replace("__CANCEL_GATE__", &shell_quote(cancel_gate))
        .replace("__CANCEL_SEEN__", &shell_quote(cancel_seen))
        .replace("__LOAD_WAITING__", &shell_quote(load_waiting))
        .replace("__LOAD_GATE__", &shell_quote(load_gate))
        .replace("__LOAD_COMPLETED__", &shell_quote(load_completed))
        .replace("__CATALOG_SEEN__", &shell_quote(catalog_seen))
        .replace("__EXPECTED_HOME__", &shell_quote(expected_home))
        .replace(
            "__EXPECTED_SESSION_CWD__",
            &shell_quote(expected_session_cwd),
        );
    fs::write(sidecar, script).expect("必须能写入 fake sidecar");
    fs::set_permissions(sidecar, fs::Permissions::from_mode(0o700))
        .expect("fake sidecar 必须可执行");
}

/// 未知 live/replay update 与 cancel 后不得变成旧诊断或可完成回合。
fn is_old_diagnostic(event: &KitProductEvent) -> bool {
    matches!(
        &event.block,
        KitBlock::Status { code, .. } if matches!(code.as_str(), "skipped_update" | "replay_skipped")
    )
}

/// 运行两回合、未知 update、取消、重启和 cold resume 的最小跨仓 transcript 合同。
#[test]
fn two_turns_cold_resume_cancel_and_unknown_update_have_one_visible_transcript() {
    let harness = Harness::transcript();
    let session_id = harness.new_session();
    harness.send_and_wait(&session_id, "prompt-a", "first");
    harness.start_stream(&session_id, "prompt-b", "second");
    harness.inject_unknown_update_between_chunks();
    harness.cancel_current_turn(&session_id, "prompt-b");
    harness.restart_sidecar_and_resume(&session_id);

    let wire = harness.wire();
    assert_session_wire_contract(&wire, &session_id, &harness.expected_session_cwd);
    assert_eq!(harness.session_load_count(), 1);
    assert_eq!(harness.visible_assistant_text("prompt-a"), "first-reply");
    assert_eq!(
        harness.replay_assistant_count("prompt-a"),
        1,
        "cold replay 只应向 Host 产品事件流投递一份历史 assistant 快照"
    );
    let visible_messages = harness.visible_messages();
    assert!(
        visible_messages.iter().all(|event| {
            !matches!(event.event_id.as_str(), "old-event" | "unknown-b-event")
                && !matches!(event.block_id.as_str(), "old-event" | "unknown-b-event")
        }),
        "未知 live/replay update 的 event_id 不得泄漏到产品事件"
    );
    assert!(
        visible_messages
            .iter()
            .all(|event| !matches!(&event.block, KitBlock::Unknown { .. })),
        "未知 live/replay update 不得泄漏为 KitBlock::Unknown"
    );
    assert!(
        !visible_messages.iter().any(is_old_diagnostic),
        "未知 update 不得伪造 skipped_update/replay_skipped 产品事件"
    );
    assert_eq!(harness.method_count("session/cancel"), 1);
    assert_eq!(harness.method_count("session/prompt"), 2);
    assert_eq!(harness.completed_turn_count(), 1);
    assert!(!harness.has_completed_turn("prompt-b"));

    let load = harness
        .wire()
        .into_iter()
        .find(|item| item.get("method").and_then(Value::as_str) == Some("session/load"))
        .expect("必须存在唯一 session/load");
    assert_eq!(load["params"]["sessionId"], session_id);
    assert_eq!(load["params"]["mcpServers"], json!([]));
    assert_eq!(load["params"]["_meta"]["modelId"], "byok");
    harness.assert_secret_not_persisted();
}

/// 同一 active session 的两次 hot resume 各自只投递一份 Host transcript 快照且不 cold load。
#[test]
fn hot_resume_emits_one_snapshot_per_fence_without_session_load() {
    let harness = Harness::transcript();
    let session_id = harness.new_session();
    harness.send_and_wait(&session_id, "prompt-a", "first");
    assert_eq!(harness.replay_assistant_count("prompt-a"), 0);

    harness.hot_resume(&session_id, 1);
    assert_eq!(harness.replay_assistant_count("prompt-a"), 1);
    assert_eq!(harness.session_load_count(), 0);

    harness.hot_resume(&session_id, 2);
    assert_eq!(harness.replay_assistant_count("prompt-a"), 2);
    assert_eq!(harness.session_load_count(), 0);
    assert_eq!(harness.visible_assistant_text("prompt-a"), "first-reply");
}

/// Host catalog 的 empty/HTTP ready/missing/extra 分支通过真实 ACP wire 分别收口。
#[test]
fn mcp_catalog_empty_http_ready_missing_and_extra_follow_host_wire_contract() {
    let empty = Harness::catalog(SidecarScenario::EmptyCatalog, ApprovedMcpSpec::default());
    let empty_session = empty.new_session();
    empty.send_and_wait(&empty_session, "prompt-a", "empty catalog");
    // prompt 终态是完成屏障；此处读取的 JSONL 仅是当时已写完的前缀快照。
    assert_eq!(empty.method_count("_x.ai/mcp/list"), 0);
    let new_wire = empty
        .wire()
        .into_iter()
        .find(|item| item.get("method").and_then(Value::as_str) == Some("session/new"))
        .expect("必须存在 session/new wire");
    assert_eq!(new_wire["params"]["mcpServers"], json!([]));
    assert_session_wire_contract(
        &empty.wire(),
        CANONICAL_SESSION_ID,
        &empty.expected_session_cwd,
    );

    let ready = Harness::catalog(SidecarScenario::CatalogReady, approved_http_mcp());
    ready.new_session();
    ready.wait_for_catalog();
    assert!(matches!(
        ready.list_sessions(),
        KitReply::ListSessions { .. }
    ));
    assert_session_wire_contract(
        &ready.wire(),
        CANONICAL_SESSION_ID,
        &ready.expected_session_cwd,
    );
    assert!(
        !ready.visible_messages().iter().any(|event| {
            matches!(&event.block, KitBlock::Status { code, .. } if code == "mcp_failed")
        }),
        "HTTP catalog ready 且工具齐全时不得产生 mcp_failed"
    );
    let runtime_config = ready.runtime_config();
    assert!(runtime_config.contains("http://127.0.0.1:4313/mcp"));
    assert!(!runtime_config.contains("command ="));
    assert!(!runtime_config.contains("args ="));

    let missing = Harness::catalog(SidecarScenario::CatalogMissing, approved_http_mcp());
    missing.new_session();
    missing.wait_for_catalog();
    assert!(matches!(
        missing.list_sessions(),
        KitReply::ListSessions { .. }
    ));
    assert_session_wire_contract(
        &missing.wire(),
        CANONICAL_SESSION_ID,
        &missing.expected_session_cwd,
    );
    missing.wait_for_events(|events| {
        events.iter().any(
            |event| matches!(&event.block, KitBlock::Status { code, .. } if code == "mcp_failed"),
        )
    });

    let extra = Harness::catalog(SidecarScenario::CatalogExtra, approved_http_mcp());
    extra.new_session();
    extra.wait_for_catalog();
    let error = extra
        .runtime
        .dispatch(KitCommand::ListSessions {
            scope_id: "scope-a".to_string(),
            cursor: None,
        })
        .expect_err("额外 MCP 工具必须让 Host scope 安全终止");
    assert_session_wire_contract(
        &extra.wire(),
        CANONICAL_SESSION_ID,
        &extra.expected_session_cwd,
    );
    assert_eq!(error.code, "sidecar_unavailable");
    assert!(
        !extra.visible_messages().iter().any(|event| {
            matches!(&event.block, KitBlock::Status { code, .. } if code == "mcp_failed")
        }),
        "未批准工具是安全违例，不得降级伪装为 missing"
    );
}

/// stdio MCP 在 Host 构造 runtime 规格前即被统一拒绝，不触发 sidecar process。
#[test]
fn stdio_mcp_is_rejected_before_runtime_config_can_be_rendered() {
    let mut servers = ApprovedMcpConfig::default();
    servers.servers.insert(
        "stdio".to_string(),
        McpServerSpec::Stdio {
            command: PathBuf::from("/bin/echo"),
            args: Vec::new(),
        },
    );
    let error = ApprovedMcpSpec::from_approved(servers, BTreeSet::new())
        .expect_err("stdio MCP 必须 fail-closed");
    assert!(error.to_string().contains("stdio_mcp_unavailable"));
}
