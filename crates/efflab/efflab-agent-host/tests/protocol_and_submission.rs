//! Kit 线协议与提交幂等的冻结测试。
//!
//! 本文件先于 host crate 实现创建，以锁定 M0 协议形状和最小 dispatch 行为。

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use efflab_agent_host::{
    ApprovedMcpSpec, HostApp, HostRuntime, HostRuntimeConfig, KitBlock, KitCommand, KitError,
    KitEventSink, KitProductEvent, KitReply, LlmChannelConfig, LlmChannelView, ResolvedMention,
    ScopeId, SealedSecret, SecretGuard, ValidatedKitEventSink,
};

/// 构造仅供协议测试使用的运行时配置；骨架阶段不会访问这些路径。
fn runtime_config() -> HostRuntimeConfig {
    HostRuntimeConfig {
        home_root: PathBuf::from("/tmp/efflab-agent-host-test/home"),
        sidecar_bin: PathBuf::from("/tmp/efflab-agent-host-test/sidecar"),
        mcp_exec_root: PathBuf::from("/tmp/efflab-agent-host-test/mcp"),
        idle_after: Duration::from_secs(60),
        l3b: efflab_agent_host::L3bRuntimeConfig::default(),
    }
}

/// 最小 HostApp 假实现，确保 HostRuntime 的端口可在不连接产品的情况下构造。
struct FakeApp;

impl HostApp for FakeApp {
    fn app_id(&self) -> &str {
        "test-app"
    }

    fn persist_llm_channel(&self, _cfg: &LlmChannelConfig) -> Result<()> {
        Ok(())
    }

    fn load_llm_channel(&self) -> Result<LlmChannelConfig> {
        Ok(LlmChannelConfig::default())
    }

    fn seal_secret(&self, plain: &[u8]) -> Result<SealedSecret> {
        Ok(SealedSecret::new(plain.to_vec()))
    }

    fn unseal_secret(&self, sealed: &SealedSecret) -> Result<SecretGuard> {
        Ok(SecretGuard::new(sealed.as_bytes().to_vec()))
    }

    fn mcp_for_scope(&self, _scope: &ScopeId) -> Result<ApprovedMcpSpec> {
        Ok(ApprovedMcpSpec::default())
    }
}

/// 内存事件 sink；仅实现冻结的产品运输 trait，校验由 Host 包装器负责。
#[derive(Default)]
struct MemorySink {
    events: Arc<Mutex<Vec<KitProductEvent>>>,
}

impl KitEventSink for MemorySink {
    fn emit(&self, ev: KitProductEvent) -> Result<()> {
        self.events.lock().expect("事件锁必须可用").push(ev);
        Ok(())
    }
}

/// 构造 host skeleton，避免每个测试重复端口接线。
fn runtime() -> HostRuntime {
    HostRuntime::new(FakeApp, MemorySink::default(), runtime_config())
}

/// KitCommand 必须使用邻接 `cmd` 标签和 snake_case 字段。
#[test]
fn kit_command_serde_is_adjacent_cmd_snake_case() {
    let cmd = KitCommand::Send {
        scope_id: "scope".to_string(),
        session_id: "session".to_string(),
        submission_id: "submission".to_string(),
        text: "hello".to_string(),
        mentions: None,
    };

    let value = serde_json::to_value(&cmd).expect("Send 必须可序列化");
    assert_eq!(value["cmd"], "send");
    assert_eq!(value["scope_id"], "scope");
    assert!(value.get("Send").is_none());
}

/// Capability reply 必须保持协议规定的扁平字段形状，而非嵌套 `capability` 对象。
#[test]
fn kit_reply_capability_is_flattened_under_kind() {
    let reply = KitReply::Capability(efflab_agent_host::Capability {
        sidecar: "available".to_string(),
        reason: None,
        kit_version: "0.1.0".to_string(),
        schema_version: 1,
        features: vec!["send".to_string()],
        channel: LlmChannelView::default(),
        limits: efflab_agent_host::CapabilityLimits {
            max_prompt_chars: 32_000,
        },
    });

    let value = serde_json::to_value(reply).expect("Capability reply 必须可序列化");
    assert_eq!(value["kind"], "capability");
    assert_eq!(value["sidecar"], "available");
    assert!(value.get("capability").is_none());
}

/// Send reply 必须携带重复位，方便调用方区分首次投递和幂等命中。
#[test]
fn kit_reply_send_has_duplicate_bit() {
    let reply = KitReply::Send {
        accepted: true,
        duplicate: false,
        session_id: "session".to_string(),
        turn_id: "submission".to_string(),
        submission_id: "submission".to_string(),
    };

    let value = serde_json::to_value(reply).expect("Send reply 必须可序列化");
    assert_eq!(value["kind"], "send");
    assert_eq!(value["accepted"], true);
    assert_eq!(value["duplicate"], false);
}

/// 未知命令必须保留 cmd，随后由 dispatch 返回结构化 KitError。
#[test]
fn unknown_cmd_reaches_structured_unsupported() {
    let raw = serde_json::json!({ "cmd": "future_command", "scope_id": "scope" });
    let cmd = KitCommand::from_json_value(raw).expect("未知 cmd 不得在 JSON 层失败");
    assert!(matches!(cmd, KitCommand::Unknown { ref cmd } if cmd == "future_command"));

    let error = runtime()
        .dispatch(cmd)
        .expect_err("未知 cmd 必须由 dispatch 拒绝");
    assert_eq!(error.code, "unsupported");
    assert!(!error.retryable);
}

/// 错误码在线上是开放字符串，未知码仍必须携带可展示消息。
#[test]
fn unknown_error_code_keeps_message() {
    let raw = include_str!("fixtures/kit_wire/unknown_error.json");
    let error: KitError = serde_json::from_str(raw).expect("未知错误码必须能解码");

    assert_eq!(error.code, "future_error");
    assert_eq!(error.message, "future failure");
    assert!(!error.retryable);
}

/// 未知 block kind 要降级为固定 unknown 形状，且有意丢弃未知原 payload。
#[test]
fn kit_block_unknown_kind_round_trips_to_unknown_shape() {
    let raw = include_str!("fixtures/kit_wire/unknown_block_event.json");
    let event: KitProductEvent = serde_json::from_str(raw).expect("未知 block 不得丢弃整条事件");
    assert!(
        matches!(event.block, KitBlock::Unknown { ref unknown_kind } if unknown_kind == "plan")
    );

    let round_trip = serde_json::to_value(event).expect("未知 block 必须可重序列化");
    assert_eq!(
        round_trip["block"],
        serde_json::json!({ "kind": "unknown", "unknown_kind": "plan" })
    );
}

/// session 级状态事件可没有 turn/submission，并按 Host 事件 ID 合成规则编码。
#[test]
fn session_level_status_allows_null_turn_id() {
    let raw = include_str!("fixtures/kit_wire/session_status_event.json");
    let event: KitProductEvent = serde_json::from_str(raw).expect("session 级状态事件必须可解码");

    assert_eq!(event.turn_id, None);
    assert_eq!(event.submission_id, None);
    assert_eq!(event.event_id, "sid:host:replay_complete:0");
    assert_eq!(event.block_id, event.event_id);
}

/// Kit 事件是产品协议，不能混入 ACP 的 method 包装。
#[test]
fn kit_event_json_has_no_acp_method() {
    let raw = include_str!("fixtures/kit_wire/session_status_event.json");
    let value: serde_json::Value = serde_json::from_str(raw).expect("golden 必须是 JSON");

    assert!(value.get("method").is_none());
    assert_eq!(value["block"]["kind"], "status");
}

/// LLM channel view 的 reply 标签在外层，通道字段只嵌套在 channel 内。
#[test]
fn llm_channel_view_is_nested_and_never_serializes_credentials() {
    let raw = include_str!("fixtures/kit_wire/llm_channel_view_empty.json");
    let expected: serde_json::Value = serde_json::from_str(raw).expect("golden 必须是 JSON");
    let reply = KitReply::LlmChannelView {
        channel: LlmChannelView::default(),
    };
    let value = serde_json::to_value(reply).expect("view reply 必须可序列化");

    assert_eq!(value, expected);
    assert!(value.get("api_key").is_none());
    assert!(value.get("access_token").is_none());
}

/// Get/Set channel 命令必须使用固定 snake_case `cmd` wire；仅 Set 请求本身可携带一次性秘密。
#[test]
fn llm_channel_commands_have_frozen_wire_shapes() {
    let get = serde_json::to_value(KitCommand::GetLlmChannelView)
        .expect("GetLlmChannelView 必须可序列化");
    assert_eq!(get, serde_json::json!({ "cmd": "get_llm_channel_view" }));

    let raw = include_str!("fixtures/kit_wire/set_llm_channel.json");
    let expected: serde_json::Value = serde_json::from_str(raw).expect("golden 必须是 JSON");
    let set = KitCommand::from_json_value(expected.clone()).expect("Set wire 必须可解码");
    assert_eq!(
        serde_json::to_value(set).expect("Set wire 必须可重序列化"),
        expected
    );
}

/// Set channel 请求可携带一次性秘密，但回传线协议不能泄露其明文。
#[test]
fn set_llm_channel_wire_keeps_secret_out_of_responses() {
    let raw = include_str!("fixtures/kit_wire/set_llm_channel.json");
    let command =
        KitCommand::from_json_value(serde_json::from_str(raw).expect("golden 必须是 JSON"))
            .expect("SetLlmChannel wire 必须可解码");
    assert!(matches!(
        command,
        KitCommand::SetLlmChannel {
            ref api_key,
            ref access_token,
            ..
        } if api_key.as_deref() == Some("test-api-key") && access_token.is_none()
    ));

    let error = runtime()
        .dispatch(command)
        .expect_err("SetLlmChannel 在 skeleton 阶段必须 unsupported");
    let value = serde_json::to_value(error).expect("错误必须可序列化");
    let rendered = value.to_string();
    assert!(!rendered.contains("test-api-key"));
    assert!(!rendered.contains("access_token"));
}

/// 全空 Set 请求是 Channel 语义的合法 no-op，闭环 runtime 直接回当前无凭据 view。
#[test]
fn empty_set_llm_channel_decodes_and_returns_current_view() {
    let command = KitCommand::from_json_value(serde_json::json!({ "cmd": "set_llm_channel" }))
        .expect("全空 SetLlmChannel 必须能解码");
    assert!(matches!(
        command,
        KitCommand::SetLlmChannel {
            kind: None,
            client_request_id: None,
            ..
        }
    ));

    assert_eq!(
        runtime()
            .dispatch(command)
            .expect("全空 SetLlmChannel 必须返回当前 view"),
        KitReply::LlmChannelView {
            channel: LlmChannelView::default(),
        }
    );
}

/// 仅带请求幂等标识的 Set 请求也是合法 no-op，不能在 serde 或 dispatch 边界失败。
#[test]
fn request_id_only_set_llm_channel_decodes_and_returns_current_view() {
    let command = KitCommand::from_json_value(serde_json::json!({
        "cmd": "set_llm_channel",
        "client_request_id": "request-1"
    }))
    .expect("仅 client_request_id 的 SetLlmChannel 必须能解码");
    assert!(matches!(
        command,
        KitCommand::SetLlmChannel {
            kind: None,
            client_request_id: Some(ref request_id),
            ..
        } if request_id == "request-1"
    ));

    assert_eq!(
        runtime()
            .dispatch(command)
            .expect("仅 client_request_id 的 SetLlmChannel 必须返回当前 view"),
        KitReply::LlmChannelView {
            channel: LlmChannelView::default(),
        }
    );
}

/// 命令调试输出是潜在日志路径，必须对请求中所有一次性秘密脱敏。
#[test]
fn set_llm_channel_debug_redacts_credentials() {
    let command = KitCommand::from_json_value(serde_json::json!({
        "cmd": "set_llm_channel",
        "kind": "byok",
        "base_url": "https://example.test/v1?api_key=debug-url-secret",
        "relay_base_url": "https://relay.test/v1?token=debug-relay-url-secret",
        "app_key": "debug-relay-app-key-secret",
        "api_key": "debug-api-key-secret",
        "access_token": "debug-access-token-secret"
    }))
    .expect("携带秘密的 SetLlmChannel 必须能解码");

    let rendered = format!("{command:?}");
    assert!(!rendered.contains("debug-api-key-secret"));
    assert!(!rendered.contains("debug-access-token-secret"));
    assert!(
        !rendered.contains("debug-relay-app-key-secret"),
        "Relay app key 同样可能是产品凭据，调试路径不得回显"
    );
    assert!(
        !rendered.contains("debug-url-secret") && !rendered.contains("debug-relay-url-secret"),
        "未验证的 URL query 同样可能携带秘密，调试路径不得回显"
    );

    let persisted = LlmChannelConfig::Byok {
        base_url: "https://example.test/v1?api_key=persisted-debug-url-secret".to_string(),
        model_id: "test-model".to_string(),
        api_key: SealedSecret::new(b"persisted-debug-key-secret".to_vec()),
    };
    let persisted_debug = format!("{persisted:?}");
    assert!(
        !persisted_debug.contains("persisted-debug-url-secret")
            && !persisted_debug.contains("persisted-debug-key-secret"),
        "持久化 Channel 配置的调试输出也不得回显任何可用凭据"
    );
}

/// 事件在输入 serde 层应保持宽容；Host 出站边界再拒绝回合标识不变量违例。
#[test]
fn kit_product_event_validate_rejects_invalid_turn_and_session_id_combinations() {
    let cases = [
        (
            "turn 级 user 缺少标识",
            event(
                KitBlock::User {
                    text: "hello".to_string(),
                },
                None,
                None,
            ),
        ),
        (
            "turn 级 assistant 标识不相等",
            event(
                KitBlock::Assistant {
                    markdown: "hello".to_string(),
                    streaming: false,
                },
                Some("turn"),
                Some("submission"),
            ),
        ),
        (
            "turn 终态缺少 submission_id",
            event(
                KitBlock::Status {
                    code: "turn_completed".to_string(),
                    message: "done".to_string(),
                },
                Some("turn"),
                None,
            ),
        ),
        (
            "session 状态不得携带 synthetic turn id",
            event(
                KitBlock::Status {
                    code: "replay_complete".to_string(),
                    message: "0".to_string(),
                },
                Some("synthetic"),
                Some("synthetic"),
            ),
        ),
        (
            "prompt 无关 skipped_update 不得携带 synthetic turn id",
            event(
                KitBlock::Status {
                    code: "skipped_update".to_string(),
                    message: "1".to_string(),
                },
                Some("synthetic"),
                Some("synthetic"),
            ),
        ),
    ];

    for (name, event) in cases {
        assert!(event.validate().is_err(), "{name} 必须被 Host 出站边界拒绝");
    }
}

/// 合法 session/process Status 保持 null 标识，既有 golden 也须通过显式边界校验。
#[test]
fn kit_product_event_validate_allows_session_status_with_null_ids() {
    let raw = include_str!("fixtures/kit_wire/session_status_event.json");
    let event: KitProductEvent = serde_json::from_str(raw).expect("session 状态 golden 必须能解码");

    event
        .validate()
        .expect("session/process Status 的 null turn/submission 必须通过 Host 出站校验");
}

/// 入站 serde 保持宽容，但统一 emit 边界必须在产品运输前拒绝非法回合标识。
#[test]
fn kit_event_sink_rejects_invalid_inbound_event_before_transport() {
    let event: KitProductEvent = serde_json::from_value(serde_json::json!({
        "schema_version": 1,
        "scope_id": "scope",
        "session_id": "session",
        "turn_id": "turn",
        "submission_id": "different-submission",
        "event_id": "event",
        "sequence": 0,
        "origin": "live",
        "block_id": "block",
        "block": { "kind": "user", "text": "hello" }
    }))
    .expect("入站 serde 不得因异常标识失败");
    let events = Arc::new(Mutex::new(Vec::new()));
    let sink = ValidatedKitEventSink::new(MemorySink {
        events: Arc::clone(&events),
    });

    sink.emit(event)
        .expect_err("非法事件必须在 Host emit 边界被拒绝");
    assert!(
        events.lock().expect("事件锁必须可用").is_empty(),
        "被拒绝的事件不得进入产品运输"
    );
}

/// 闭环 runtime 在未配置 Channel 时必须在 SubmissionMap 和 sidecar 之前拒绝 Send。
/// 幂等指纹的完整真实 sidecar 覆盖位于 dispatch_loop 集成测试，避免本测试依赖假进程。
#[test]
fn unconfigured_runtime_rejects_send_before_submission_or_sidecar_work() {
    let error = runtime()
        .dispatch(KitCommand::Send {
            scope_id: "scope".to_string(),
            session_id: "session".to_string(),
            submission_id: "submission".to_string(),
            text: "same text".to_string(),
            mentions: None,
        })
        .expect_err("未配置 Channel 时 Send 必须 fail-closed");
    assert_eq!(error.code, "llm_channel_unconfigured");
}

/// host 只能依赖 contract 与小依赖，禁止反向链接 sidecar 或 grok shell。
#[test]
fn host_crate_does_not_depend_on_sidecar_or_grok_shell() {
    let manifest = include_str!("../Cargo.toml");
    assert!(!manifest.contains("efflab-agent-sidecar"));
    assert!(!manifest.contains("xai-grok-shell"));
    assert!(!manifest.contains("xai-grok-tools"));
}

/// 构造最小事件，专门覆盖 Host 出站前的回合/会话标识校验。
fn event(block: KitBlock, turn_id: Option<&str>, submission_id: Option<&str>) -> KitProductEvent {
    KitProductEvent {
        schema_version: 1,
        scope_id: "scope".to_string(),
        session_id: "session".to_string(),
        turn_id: turn_id.map(str::to_string),
        submission_id: submission_id.map(str::to_string),
        event_id: "event".to_string(),
        sequence: 0,
        origin: efflab_agent_host::Origin::Live,
        block_id: "block".to_string(),
        block,
    }
}

/// 将 trait 导入保持在测试编译时可见，避免未来端口签名悄然漂移。
#[allow(dead_code)]
fn _resolved_mention_type_marker(_: ResolvedMention) {}
