//! 共享 Host 合同 fixture 的直接校验。
//!
//! sidecar 保留同名测试以覆盖兼容 re-export；本测试让 contract crate 自身也能
//! 按发布契约运行相同 fixture，避免只验证转发层。
//!
//! 本 target 校验 fixture 的 allow/reject 结果，并对 Task21 named pins 保留结构化断言。

use efflab_agent_contract::{HostPolicy, HostRejection, validate_host_request};
use tempfile::TempDir;

/// fixture 中使用的路径占位符。
const CWD_PLACEHOLDER: &str = "{{CWD}}";
const OTHER_PLACEHOLDER: &str = "{{OTHER}}";

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
enum Expect {
    Allow,
    Reject,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
enum Rejection {
    UnknownMethod,
    UnknownMetaKey,
    CwdMismatch,
    ClientMcpServersNotAllowed,
    ForbiddenField,
    UnknownField,
    UnknownNestedField,
    InvalidFieldType,
    MissingRequiredField,
    UnsupportedProtocolVersion,
    TerminalCapabilityEnabled,
    FsCapabilityEnabled,
    ModelIdNotAllowed,
}

#[derive(serde::Deserialize)]
struct FixtureCase {
    name: String,
    method: String,
    params: serde_json::Value,
    expect: Expect,
    // 保持 fixture 消费兼容：旧/跨语言消费者可以继续忽略该可选字段。
    #[serde(default)]
    rejection: Option<Rejection>,
}

#[test]
fn fixture_cases_follow_contract() {
    // 复用跨语言 Host 的唯一 fixture，避免 contract 与 sidecar 各自漂移。
    let raw = include_str!("../../efflab-agent-sidecar/tests/fixtures/host_contract_cases.json");
    let cases: Vec<FixtureCase> =
        serde_json::from_str(raw).expect("fixture JSON 必须能解析为用例数组");
    let cwd_dir = TempDir::new().expect("创建临时 cwd 目录");
    let other_dir = TempDir::new().expect("创建临时 other 目录");
    let policy = HostPolicy::new(cwd_dir.path().to_path_buf())
        // 只有会话方法接收 modelId，prompt 只接收 submission 对应的 promptId。
        .with_meta_key_for("session/new", "modelId")
        .with_meta_key_for("session/load", "modelId")
        .with_meta_key_for("session/prompt", "promptId")
        .with_model_id("byok");

    for case in cases {
        let params = replace_paths(
            &case.params,
            cwd_dir.path().to_str().expect("cwd 必须是 UTF-8 路径"),
            other_dir
                .path()
                .to_str()
                .expect("other cwd 必须是 UTF-8 路径"),
        );
        let result = validate_host_request(&case.method, &params, &policy);
        match case.expect {
            Expect::Allow => assert!(
                result.is_ok(),
                "允许用例 '{}'（method={}）结果不符：{result:?}",
                case.name,
                case.method
            ),
            Expect::Reject => assert!(
                result.is_err(),
                "拒绝用例 '{}'（method={}）结果不符：{result:?}",
                case.name,
                case.method
            ),
        }
    }
}

/// Task21：四个精确命名的 fixture 对齐用例必须存在且语义固定。
///
/// 这是跨语言 Host 与 crate 内部共用的“按名对齐”测试：即便未来增删普通用例或
/// 调整校验，这四条公共名称与各自的 allow/reject 语义也不能漂移。缺失或语义
/// 变化时在这里直接失败并给出用例名，比依赖整表断言更容易定位。
#[test]
fn task21_named_alignment_cases_pinned() {
    let raw = include_str!("../../efflab-agent-sidecar/tests/fixtures/host_contract_cases.json");
    let cases: Vec<FixtureCase> =
        serde_json::from_str(raw).expect("fixture JSON 必须能解析为用例数组");
    let cwd_dir = TempDir::new().expect("创建临时 cwd 目录");
    let other_dir = TempDir::new().expect("创建临时 other 目录");
    let policy = HostPolicy::new(cwd_dir.path().to_path_buf())
        .with_meta_key_for("session/new", "modelId")
        .with_meta_key_for("session/load", "modelId")
        .with_meta_key_for("session/prompt", "promptId")
        .with_model_id("byok");

    // 对齐契约：case 名 → (method, 期望语义, rejection 变体)。
    let pinned: [(&str, &str, Expect, Option<Rejection>); 4] = [
        (
            "session_prompt_with_flat_text",
            "session/prompt",
            Expect::Reject,
            Some(Rejection::UnknownField),
        ),
        (
            "session_prompt_with_prompt_id",
            "session/prompt",
            Expect::Allow,
            None,
        ),
        (
            "session_list_with_limit",
            "session/list",
            Expect::Reject,
            Some(Rejection::UnknownField),
        ),
        (
            "ext_session_list_is_rejected",
            "_x.ai/session/list",
            Expect::Reject,
            Some(Rejection::UnknownMethod),
        ),
    ];

    for (name, method, expect, rejection) in pinned {
        let matching_count = cases.iter().filter(|case| case.name == name).count();
        assert_eq!(
            matching_count, 1,
            "fixture 中对齐用例 {name:?} 必须恰好出现一次"
        );
        let case = cases
            .iter()
            .find(|case| case.name == name)
            .expect("已确认的对齐用例必须能读取");
        assert_eq!(case.method, method, "对齐用例 {name:?} 的 method 漂移");
        assert_eq!(
            case.expect, expect,
            "对齐用例 {name:?} 的 allow/reject 语义漂移"
        );
        assert_eq!(
            case.rejection, rejection,
            "对齐用例 {name:?} 的 rejection 变体漂移"
        );
        assert_task21_shape(name, &case.params, &case.method);

        let params = replace_paths(
            &case.params,
            cwd_dir.path().to_str().expect("cwd 必须是 UTF-8 路径"),
            other_dir
                .path()
                .to_str()
                .expect("other cwd 必须是 UTF-8 路径"),
        );
        let result = validate_host_request(&case.method, &params, &policy);
        match expect {
            Expect::Allow => assert!(result.is_ok(), "对齐用例 {name:?} 必须放行: {result:?}"),
            Expect::Reject => assert!(result.is_err(), "对齐用例 {name:?} 必须拒绝: {result:?}"),
        }
    }
}

/// 钉住 Task21 四条 fixture 的关键字段，避免只按名称和结果放行。
fn assert_task21_shape(name: &str, params: &serde_json::Value, method: &str) {
    match name {
        "session_prompt_with_flat_text" => {
            assert_eq!(method, "session/prompt");
            let object = params.as_object().expect("flat text pin params 必须是对象");
            assert_eq!(object.len(), 4);
            assert_eq!(
                params.get("text").and_then(serde_json::Value::as_str),
                Some("不能使用扁平 text")
            );
            assert_single_text_content_block(params, "正常内容");
            assert_eq!(
                params
                    .get("_meta")
                    .and_then(serde_json::Value::as_object)
                    .and_then(|meta| meta.get("promptId"))
                    .and_then(serde_json::Value::as_str),
                Some("submission-1")
            );
        }
        "session_prompt_with_prompt_id" => {
            assert_eq!(method, "session/prompt");
            assert!(params.get("text").is_none());
            assert_single_text_content_block(params, "使用 promptId 的正常 prompt");
            assert_eq!(
                params
                    .get("_meta")
                    .and_then(serde_json::Value::as_object)
                    .and_then(|meta| meta.get("promptId"))
                    .and_then(serde_json::Value::as_str),
                Some("submission-1")
            );
        }
        "session_list_with_limit" => {
            assert_eq!(method, "session/list");
            let object = params
                .as_object()
                .expect("session/list pin params 必须是对象");
            assert_eq!(object.len(), 2);
            assert_eq!(
                params.get("limit").and_then(serde_json::Value::as_u64),
                Some(30)
            );
            assert_eq!(
                params.get("cwd").and_then(serde_json::Value::as_str),
                Some(CWD_PLACEHOLDER)
            );
        }
        "ext_session_list_is_rejected" => {
            assert_eq!(method, "_x.ai/session/list");
            assert_eq!(method.strip_prefix('_'), Some("x.ai/session/list"));
            assert_eq!(
                params
                    .get("allowRelax")
                    .and_then(serde_json::Value::as_bool),
                Some(true)
            );
        }
        other => panic!("未知 Task21 pin {other:?}"),
    }
}

/// 校验 prompt pin 使用唯一的 text ContentBlock 形状且不带额外字段。
fn assert_single_text_content_block(params: &serde_json::Value, expected_text: &str) {
    let prompt = params
        .get("prompt")
        .and_then(serde_json::Value::as_array)
        .expect("prompt pin 必须使用 ContentBlock 数组");
    assert_eq!(prompt.len(), 1);
    let block = prompt[0]
        .as_object()
        .expect("prompt ContentBlock 必须是对象");
    assert_eq!(block.len(), 2);
    assert_eq!(
        block.get("type").and_then(serde_json::Value::as_str),
        Some("text")
    );
    assert_eq!(
        block.get("text").and_then(serde_json::Value::as_str),
        Some(expected_text)
    );
}

#[test]
fn host_policy_builder_rejects_contract_forbidden_meta_combinations() {
    let forbidden = [
        ("initialize", "modelId"),
        ("initialize", "promptId"),
        ("session/list", "modelId"),
        ("session/list", "promptId"),
        ("session/cancel", "modelId"),
        ("session/cancel", "promptId"),
        ("session/new", "promptId"),
        ("session/load", "promptId"),
        ("session/prompt", "modelId"),
        ("unknown/method", "modelId"),
    ];

    for (method, key) in forbidden {
        let policy = HostPolicy::new("/tmp/efflab-contract").with_meta_key_for(method, key);
        assert!(
            policy.meta_keys_for(method).is_empty(),
            "禁止的 method/key 组合不得进入 HostPolicy: {method}/{key}"
        );
    }

    let cwd = TempDir::new().expect("创建 HostPolicy 测试 cwd");
    let cwd = cwd.path().to_str().expect("测试 cwd 必须是 UTF-8 路径");
    let policy = HostPolicy::new(cwd)
        .with_meta_key_for("session/new", "promptId")
        .with_meta_key_for("session/load", "promptId")
        .with_meta_key_for("session/prompt", "modelId")
        .with_meta_key_for("initialize", "modelId")
        .with_meta_key_for("session/list", "modelId")
        .with_meta_key_for("session/cancel", "modelId")
        .with_model_id("byok");

    let cases = [
        (
            "initialize",
            serde_json::json!({
                "protocolVersion": 1,
                "clientCapabilities": {
                    "terminal": false,
                    "fs": {"readTextFile": false, "writeTextFile": false}
                },
                "clientInfo": {"name": "test", "version": "test"},
                "_meta": {"modelId": "byok"}
            }),
            "modelId",
        ),
        (
            "session/new",
            serde_json::json!({
                "cwd": cwd,
                "mcpServers": [],
                "_meta": {"promptId": "prompt-1"}
            }),
            "promptId",
        ),
        (
            "session/load",
            serde_json::json!({
                "sessionId": "session-1",
                "cwd": cwd,
                "mcpServers": [],
                "_meta": {"promptId": "prompt-1"}
            }),
            "promptId",
        ),
        (
            "session/prompt",
            serde_json::json!({
                "sessionId": "session-1",
                "prompt": [{"type": "text", "text": "hello"}],
                "_meta": {"modelId": "byok"}
            }),
            "modelId",
        ),
        (
            "session/list",
            serde_json::json!({"cwd": cwd, "_meta": {"modelId": "byok"}}),
            "_meta",
        ),
        (
            "session/cancel",
            serde_json::json!({"sessionId": "session-1", "_meta": {"modelId": "byok"}}),
            "_meta",
        ),
    ];

    for (method, params, key) in cases {
        assert!(
            matches!(
                validate_host_request(method, &params, &policy),
                Err(HostRejection::UnknownMetaKey(_, _) | HostRejection::UnknownField { .. })
            ),
            "合同禁止的 method/key 组合必须在请求校验时拒绝: {method}/{key}"
        );
    }
}

/// 递归替换 fixture 内的临时路径占位符。
fn replace_paths(value: &serde_json::Value, cwd: &str, other: &str) -> serde_json::Value {
    match value {
        serde_json::Value::String(value) => serde_json::Value::String(
            value
                .replace(CWD_PLACEHOLDER, cwd)
                .replace(OTHER_PLACEHOLDER, other),
        ),
        serde_json::Value::Array(items) => serde_json::Value::Array(
            items
                .iter()
                .map(|item| replace_paths(item, cwd, other))
                .collect(),
        ),
        serde_json::Value::Object(values) => serde_json::Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), replace_paths(value, cwd, other)))
                .collect(),
        ),
        value => value.clone(),
    }
}
