//! HostRuntime dispatch 闭环的真实 stdio 集成测试。
//!
//! 每个用例都启动临时 shell sidecar，通过真实 stdin/stdout 收发 JSON-RPC；测试
//! 只观察非敏感 wire 和 Kit 产品事件，避免 mock 掩盖进程、握手与反向 RPC 接线。

#![cfg(unix)]

use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;
use efflab_agent_host::{
    ApprovedMcpSpec, HostApp, HostRuntime, HostRuntimeConfig, KitBlock, KitCommand, KitEventSink,
    KitProductEvent, KitReply, LlmChannelConfig, LlmChannelKind, LlmSecretSlot, MentionId, Origin,
    ScopeId, SealedSecret, SecretGuard,
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
/// MCP catalog 合同 deadline 为 20 秒；为调度保留少量余量但必须早于 25 秒 API 超时。
const MCP_CATALOG_REPLY_TIMEOUT: Duration = Duration::from_secs(22);

/// 已配置或未配置 Channel 的最小产品端口；测试凭据只停留在内存中。
struct FakeApp {
    config: Arc<Mutex<LlmChannelConfig>>,
    expected_tools: BTreeSet<String>,
}

impl FakeApp {
    /// 构造可启动 L3b 的公开 HTTPS BYOK 配置。
    fn byok(expected_tools: impl IntoIterator<Item = String>) -> Self {
        Self {
            config: Arc::new(Mutex::new(LlmChannelConfig::Byok {
                base_url: "https://8.8.8.8/v1".to_string(),
                model_id: "fake-byok-model".to_string(),
                api_key: SealedSecret::new(b"test-key".to_vec()),
            })),
            expected_tools: expected_tools.into_iter().collect(),
        }
    }

    /// 构造未配置 Channel，验证所有对话命令在 spawn 前 fail-closed。
    fn unconfigured() -> Self {
        Self {
            config: Arc::new(Mutex::new(LlmChannelConfig::Unconfigured)),
            expected_tools: BTreeSet::new(),
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
}

/// 内存事件运输端口；可注入延迟以验证 actor 不会把产品回执绑在同步投影上。
struct MemorySink {
    events: Arc<Mutex<Vec<KitProductEvent>>>,
    delay: Duration,
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
        }
    }
}

impl KitEventSink for MemorySink {
    fn emit(&self, event: KitProductEvent) -> Result<()> {
        if !self.delay.is_zero() {
            thread::sleep(self.delay);
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
    events: Arc<Mutex<Vec<KitProductEvent>>>,
    sidecar: PathBuf,
}

impl Harness {
    /// 构造已配置 Channel 的运行时；mode 决定 fake sidecar 的 ACP 行为。
    fn configured(
        mode: &str,
        expected_tools: impl IntoIterator<Item = String>,
        idle_after: Duration,
    ) -> Self {
        Self::configured_with_sink_delay(mode, expected_tools, idle_after, Duration::from_millis(0))
    }

    /// 构造带可控同步 sink 的运行时，用于锁定产品回执与事件投影的顺序。
    fn configured_with_sink_delay(
        mode: &str,
        expected_tools: impl IntoIterator<Item = String>,
        idle_after: Duration,
        sink_delay: Duration,
    ) -> Self {
        let temporary = tempfile::tempdir().expect("必须能创建 dispatch loop 临时目录");
        let root = temporary.path();
        let sidecar = root.join("fake-sidecar.sh");
        let started = root.join("sidecar-started");
        let exited = root.join("sidecar-exited");
        let captured = root.join("sidecar-wire.jsonl");
        write_fake_sidecar(&sidecar, &started, &exited, &captured, mode);

        let sink = MemorySink::with_delay(sink_delay);
        let events = Arc::clone(&sink.events);
        let runtime = Arc::new(HostRuntime::new(
            FakeApp::byok(expected_tools),
            sink,
            HostRuntimeConfig {
                home_root: root.join("app-data"),
                sidecar_bin: sidecar.clone(),
                mcp_exec_root: root.join("mcp"),
                idle_after,
                l3b: Default::default(),
            },
        ));
        Self {
            _temporary: temporary,
            runtime,
            started,
            exited,
            captured,
            events,
            sidecar,
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

    /// 返回已捕获的 Host→sidecar JSON-RPC 行；等待至少一行避免启动竞态。
    fn wire(&self) -> Vec<Value> {
        wait_for_file(&self.captured);
        let source = fs::read_to_string(&self.captured).expect("必须能读取 fake sidecar wire");
        source
            .lines()
            .map(|line| serde_json::from_str(line).expect("Host wire 必须是 JSON-RPC"))
            .collect()
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
        wait_until(|| {
            self.captured.exists()
                && fs::read_to_string(&self.captured)
                    .map(|source| source.contains(&format!("\"method\":\"{method}\"")))
                    .unwrap_or(false)
        });
    }

    /// 返回测试当前已经收到的事件快照。
    fn events(&self) -> Vec<KitProductEvent> {
        self.events.lock().expect("事件锁必须可用").clone()
    }
}

/// 把任意临时路径变成 POSIX shell 单引号字面量；临时目录通常不含引号，仍保持安全。
fn shell_quote(path: &Path) -> String {
    format!(
        "'{}'",
        path.display().to_string().replace('\'', "'\\\"'\\\"'")
    )
}

/// 写出受控 shell sidecar；它不解析任何产品输入，只按 ACP method 返回固定测试响应。
fn write_fake_sidecar(sidecar: &Path, started: &Path, exited: &Path, captured: &Path, mode: &str) {
    let script = r#"#!/bin/sh
mode='__MODE__'
started=__STARTED__
exited=__EXITED__
captured=__CAPTURED__
home=""
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--grok-home" ]; then
    home="$2"
    break
  fi
  shift
done
# spawn 前必须已有由 Host 写入的权威配置与本代 binding；绝不落盘 token 本体。
test -n "$EFFLAB_L3B_BIND" || exit 41
test -f "$home/config.toml" || exit 42
/usr/bin/grep -q 'load_envrc = false' "$home/config.toml" || exit 43
/usr/bin/grep -q 'api_backend = "chat_completions"' "$home/config.toml" || exit 44
/usr/bin/printf '%s\n' 'started' >> "$started"
pending_prompt_id=""
pending_permission_session=""
new_session_count=0
permission_method='session/request_permission'
case "$mode" in
  *_wrapper) permission_method='_x.ai/session/request_permission' ;;
esac
while IFS= read -r line; do
  /usr/bin/printf '%s\n' "$line" >> "$captured"
  id=$(/usr/bin/printf '%s\n' "$line" | /usr/bin/sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  session=$(/usr/bin/printf '%s\n' "$line" | /usr/bin/sed -n 's/.*"sessionId":"\([^"]*\)".*/\1/p')
  if [ -n "$pending_prompt_id" ] && /usr/bin/printf '%s' "$line" | /usr/bin/grep -q '"id":900'; then
    /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$pending_prompt_id"
    pending_prompt_id=""
    continue
  fi
  case "$line" in
    *'"method":"initialize"'*)
      /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id"
      ;;
    *'"method":"session/new"'*)
      new_session_count=$((new_session_count + 1))
      if [ "$new_session_count" -eq 1 ]; then
        new_session_id='sidecar-session'
      else
        new_session_id="sidecar-session-$new_session_count"
      fi
      /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{"sessionId":"%s"}}\n' "$id" "$new_session_id"
      ;;
    *'"method":"session/list"'*)
      /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{"sessions":[{"sessionId":"sidecar-session","title":"来自 sidecar 的标题","updatedAt":"2026-08-14T00:00:00Z"},{"sessionId":"untitled-session","updatedAt":"2026-08-13T00:00:00Z"}],"nextCursor":"next-page"}}\n' "$id"
      ;;
    *'"method":"session/load"'*)
      if [ "$mode" = "load_fail" ]; then
        /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"error":{"code":-32004,"message":"not found"}}\n' "$id"
      else
        /usr/bin/printf '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"%s","update":{"sessionUpdate":"plan","entries":[]},"_meta":{"isReplay":true}}}\n' "$session"
        /usr/bin/printf '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"%s","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"历史回答"}},"_meta":{"isReplay":true,"promptId":"historic-turn","eventId":"history-event"}}}\n' "$session"
        if [ "$mode" = "load_hold" ]; then /bin/sleep 0.7; fi
        /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id"
      fi
      ;;
    *'"method":"_x.ai/mcp/list"'*)
      case "$mode" in
        mcp_extra)
          /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{"result":{"servers":[{"name":"builtin","session":{"status":"ready","tools":[{"name":"GrokBuild:efflab_noop","enabled":true}]}},{"name":"unexpected","session":{"status":"ready","tools":[{"name":"writeback","enabled":true}]}}]}}}\n' "$id"
          ;;
        mcp_missing)
          /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{"result":{"servers":[{"name":"purelab","session":{"status":"unavailable","tools":[]}}]}}}\n' "$id"
          ;;
        mcp_error)
          /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"error":{"code":-32001,"message":"catalog failed"}}\n' "$id"
          ;;
        mcp_late)
          # 晚于 20 秒 catalog deadline、早于原有 25 秒调用上限，验证 Host 自行降级。
          /bin/sleep 23
          /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{"result":{"servers":[]}}}\n' "$id"
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
      if { [ "$mode" = "permission_after_cancel" ] || [ "$mode" = "permission_after_cancel_wrapper" ]; } && [ -n "$pending_prompt_id" ]; then
        /usr/bin/printf '{"jsonrpc":"2.0","id":900,"method":"%s","params":{"sessionId":"%s","toolCall":{"toolCallId":"tool-1","title":"GrokBuild:efflab_noop"},"options":[{"optionId":"allow-once","name":"Allow once","kind":"allow_once"},{"optionId":"reject-once","name":"Reject once","kind":"reject_once"},{"optionId":"enable-always-approve","name":"Always","kind":"allow_once"}]}}\n' "$permission_method" "$pending_permission_session"
      fi
      ;;
    *'"method":"session/prompt"'*)
      if [ "$mode" = "permission_after_cancel" ] || [ "$mode" = "permission_after_cancel_wrapper" ]; then
        pending_prompt_id="$id"
        pending_permission_session="$session"
      elif [ "$mode" = "permission" ] || [ "$mode" = "permission_wrapper" ] || [ "$mode" = "permission_unknown" ] || [ "$mode" = "permission_unknown_wrapper" ]; then
        pending_prompt_id="$id"
        title='GrokBuild:efflab_noop'
        if [ "$mode" = "permission_unknown" ] || [ "$mode" = "permission_unknown_wrapper" ]; then title='unexpected_tool'; fi
        /usr/bin/printf '{"jsonrpc":"2.0","id":900,"method":"%s","params":{"sessionId":"%s","toolCall":{"toolCallId":"tool-1","title":"%s"},"options":[{"optionId":"allow-once","name":"Allow once","kind":"allow_once"},{"optionId":"reject-once","name":"Reject once","kind":"reject_once"},{"optionId":"enable-always-approve","name":"Always","kind":"allow_once"}]}}\n' "$permission_method" "$session" "$title"
      else
        /usr/bin/printf '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"%s","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"实时回答"}},"_meta":{"promptId":"%s","eventId":"live-event"}}}\n' "$session" "$(/usr/bin/printf '%s\n' "$line" | /usr/bin/sed -n 's/.*"promptId":"\([^"]*\)".*/\1/p')"
        if [ "$mode" = "hold_prompt" ]; then /bin/sleep 0.7; fi
        /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id"
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
        .replace("__CAPTURED__", &shell_quote(captured));
    fs::write(sidecar, script).expect("必须能写入 fake sidecar");
    fs::set_permissions(sidecar, fs::Permissions::from_mode(0o700))
        .expect("fake sidecar 必须可执行");
}

/// 等待 child 留下非敏感启动或 wire 观察文件，避免测试依赖线程调度时序。
fn wait_for_file(path: &Path) {
    wait_until(|| path.exists());
}

/// 在固定上限内轮询异步 actor 的可观察结果。
fn wait_until(condition: impl Fn() -> bool) {
    let deadline = Instant::now() + TEST_TIMEOUT;
    while !condition() {
        assert!(Instant::now() < deadline, "等待 dispatch loop 异步结果超时");
        thread::sleep(Duration::from_millis(5));
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

/// TC-LAUNCH / TC-HP：真实 child 只会在 Host 完成 L3b、token 和 TOML 前置后启动。
#[test]
fn launch_handshake_new_session_and_empty_mcp_catalog_are_wired_through_real_stdio() {
    let harness = Harness::configured("noop_only", [], Duration::from_secs(60));
    let session_id = harness.new_session("scope-a");

    assert_eq!(session_id, "sidecar-session");
    wait_for_file(&harness.started);
    harness.wait_for_method("_x.ai/mcp/list");

    let home = harness
        ._temporary
        .path()
        .join("app-data/dispatch-loop-test/scope-a/home");
    let config = fs::read_to_string(home.join("config.toml"))
        .expect("Host 必须在 fake sidecar spawn 前写入权威 config.toml");
    assert!(config.contains("load_envrc = false"));
    assert!(config.contains("api_backend = \"chat_completions\""));

    let wire = harness.wire();
    let initialize = wire
        .iter()
        .find(|item| item["method"] == "initialize")
        .expect("必须先发送 initialize");
    assert_eq!(
        initialize["params"]["capabilities"],
        json!({ "terminal": false, "fs": false })
    );
    assert_eq!(initialize["params"]["client"]["mcpServers"], json!([]));
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
    assert!(
        new_session["params"]["cwd"]
            .as_str()
            .expect("session/new cwd 必须是字符串")
            .ends_with("/workspace")
    );
    let catalog = wire
        .iter()
        .find(|item| item["method"] == "_x.ai/mcp/list")
        .expect("NewSession 后必须请求 MCP catalog");
    assert_eq!(catalog["params"]["sessionId"], json!(&session_id));
    assert!(
        catalog["params"].get("_meta").is_none(),
        "MCP catalog 不得携带任何 _meta"
    );

    // 批准集为空时只有 noop 的 catalog 必须通过，随后对话仍可继续。
    assert_send(
        harness
            .runtime
            .dispatch(send(&session_id, "launch-turn", "你好", None))
            .expect("空 MCP catalog 不得阻断 prompt"),
        false,
        &session_id,
        "launch-turn",
    );
    harness.wait_for_method("session/prompt");
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

    let error = harness
        .runtime
        .dispatch(send(&session_id, "turn-two", "并发轮", None))
        .expect_err("同一 session 的第二轮 prompt 必须被拒绝");
    assert_eq!(error.code, "turn_in_progress");

    wait_for_status(&harness, "turn_completed");
    assert!(harness.events().iter().any(|event| {
        matches!(
            &event.block,
            KitBlock::Assistant { markdown, streaming } if markdown == "实时回答" && *streaming
        ) && event.turn_id.as_deref() == Some("turn-one")
    }));
}

/// TC-NOKEY：未配置时绝不能启动 L3b 或 sidecar；设置页读取仍必须成功。
#[test]
fn unconfigured_channel_rejects_all_conversation_commands_without_spawning() {
    let (runtime, _temporary, home_root) = Harness::unconfigured();
    let commands = [
        KitCommand::GetCapability,
        send("session-a", "submission-a", "hello", None),
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

/// TC-IDEMP / TC-CANCEL：稳定 submission 指纹、无 id cancel notification 与取消竞态。
#[test]
fn idempotency_mentions_and_cancel_keep_prompt_wire_and_inflight_state_correct() {
    let harness = Harness::configured("hold_prompt", [], Duration::from_secs(60));
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
    thread::sleep(DELAYED_PROMPT_RESULT + Duration::from_millis(80));
    assert_send(
        harness
            .runtime
            .dispatch(send(&session_id, "after-result", "结果后新轮", None))
            .expect("prompt result 到达后必须释放 in-flight"),
        false,
        &session_id,
        "after-result",
    );

    // 无 in-flight 的 cancel 会被下一次 Send 消费：不得向 sidecar 写 prompt，仍要发 cancelled。
    thread::sleep(DELAYED_PROMPT_RESULT + Duration::from_millis(80));
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
    thread::sleep(Duration::from_millis(50));
    assert_eq!(
        harness.method_count("session/prompt"),
        prompt_count,
        "预先 cancel 后不得写 prompt"
    );
    wait_until(|| {
        harness.events().iter().any(|event| {
            matches!(&event.block, KitBlock::Status { code, .. } if code == "cancelled")
                && event.turn_id.as_deref() == Some("pre-cancel")
                && event.submission_id.as_deref() == Some("pre-cancel")
        })
    });
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

    let list_wire = harness
        .wire()
        .into_iter()
        .find(|item| item["method"] == "session/list")
        .expect("必须调用标准 session/list");
    assert_eq!(list_wire["params"]["cursor"], "cursor-1");
    assert!(list_wire["params"].get("limit").is_none());
    assert!(
        list_wire["params"]["cwd"]
            .as_str()
            .expect("list cwd 必须为字符串")
            .ends_with("/workspace")
    );
}

/// TC-RESUME / TC-HOT / TC-SKIP：冷恢复有 replay fence，热恢复不重写 load 且不打断 prompt。
#[test]
fn cold_and_hot_resume_obey_load_timing_replay_fence_and_session_busy_rules() {
    let cold = Harness::configured("load_hold", [], Duration::from_secs(60));
    let (resume_reply, resume_result) = mpsc::sync_channel(1);
    let cold_runtime = Arc::clone(&cold.runtime);
    thread::spawn(move || {
        let _ = resume_reply.send(cold_runtime.dispatch(KitCommand::ResumeSession {
            scope_id: "scope-a".to_string(),
            session_id: "stored-session".to_string(),
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
            session_id: "stored-session".to_string(),
        }
    );
    let replay_complete = wait_for_status(&cold, "replay_complete");
    assert_eq!(replay_complete.turn_id, None);
    assert_eq!(replay_complete.submission_id, None);
    assert_eq!(replay_complete.event_id, replay_complete.block_id);
    assert!(
        replay_complete
            .event_id
            .starts_with("stored-session:host:replay_complete:")
    );
    let replay_skipped = wait_for_status(&cold, "replay_skipped");
    assert_eq!(replay_skipped.turn_id, None);
    assert_eq!(replay_skipped.submission_id, None);
    assert_eq!(replay_skipped.origin, Origin::Replay);

    let hot = Harness::configured("hold_prompt", [], Duration::from_secs(60));
    let session_id = hot.new_session("scope-a");
    // 第二个 session 已在同一个 actor 中 active；它不是冷恢复路径的占位值。
    let other_active_session = hot.new_session("scope-a");
    assert_ne!(session_id, other_active_session);
    assert_send(
        hot.runtime
            .dispatch(send(&session_id, "hot-turn", "正在生成", None))
            .expect("第一轮 prompt 必须可写入"),
        false,
        &session_id,
        "hot-turn",
    );
    hot.wait_for_method("session/prompt");
    wait_until(|| {
        hot.events().iter().any(|event| {
            matches!(
                &event.block,
                KitBlock::Assistant { markdown, streaming }
                    if markdown == "实时回答" && *streaming
            ) && event.origin == Origin::Live
        })
    });
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

    let extra = Harness::configured("mcp_extra", [], Duration::from_secs(60));
    let _ = extra.new_session("scope-a");
    wait_for_file(&extra.exited);
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

/// TC-HP deadline：catalog 无响应时按空 catalog 降级，迟到 response 不得释放第二次 prompt。
#[test]
fn mcp_catalog_timeout_degrades_before_late_response_and_preserves_submission_idempotency() {
    let harness = Harness::configured(
        "mcp_late",
        ["purelab__search_tracks".to_string()],
        Duration::from_secs(60),
    );
    let session_id = harness.new_session("scope-a");

    let start = Instant::now();
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
        start.elapsed() < MCP_CATALOG_REPLY_TIMEOUT,
        "catalog timeout 必须早于迟到 response 和 API 调用上限"
    );
    let failure = wait_for_status(&harness, "mcp_failed");
    assert_eq!(failure.turn_id, None);
    assert_eq!(failure.submission_id, None);

    // fake 在 23 秒后才消费已写入的 prompt；此时 catalog response 已不属于 pending 请求。
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
        .dispatch(send("gone-session", "gone-turn", "找不到会话", None))
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
