//! host_contract 的 fixture 驱动测试（P3.1）。
//!
//! 读取 `tests/fixtures/host_contract_cases.json`，将 `{{CWD}}` / `{{OTHER}}`
//! 占位符替换为真实临时目录后逐条断言 allow/reject。fixture 同时作为
//! 生产 Host（非 Rust 语言）跑同一套契约用例的权威数据源。

use efflab_agent_sidecar::host_contract::{HostPolicy, HostRejection, validate_host_request};
use tempfile::TempDir;

/// fixture 中使用的路径占位符。
const CWD_PLACEHOLDER: &str = "{{CWD}}";
const OTHER_PLACEHOLDER: &str = "{{OTHER}}";

/// 受限期望结果，避免 fixture 拼写错误被默默视为 reject。
#[derive(Debug, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum Expect {
    Allow,
    Reject,
}

/// 可选的拒绝原因类型，用于跨语言 fixture 的精确行为断言。
#[derive(Debug, serde::Deserialize)]
#[allow(non_camel_case_types)]
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
    #[serde(default)]
    rejection: Option<Rejection>,
}

#[test]
fn fixture_cases_all_pass() {
    // 读取 fixture（编译期嵌入，路径相对本文件）。
    let raw = include_str!("fixtures/host_contract_cases.json");
    let cases: Vec<FixtureCase> =
        serde_json::from_str(raw).expect("fixture JSON 必须能解析为用例数组");
    assert!(!cases.is_empty(), "fixture 至少包含一个用例");

    // 准备真实临时目录。
    let cwd_dir = TempDir::new().expect("创建临时 cwd 目录");
    let other_dir = TempDir::new().expect("创建临时 other 目录");

    // 构造策略：允许会话 Channel 槽名 byok，cwd 指向 cwd_dir。
    let policy = HostPolicy::new(cwd_dir.path().to_path_buf())
        // initialize 不承载产品 modelId；只有会话方法使用 modelId，prompt 使用 promptId。
        .with_meta_key_for("session/new", "modelId")
        .with_meta_key_for("session/load", "modelId")
        .with_meta_key_for("session/prompt", "promptId")
        .with_model_id("byok".to_string());

    for case in &cases {
        // 替换路径占位符。
        let params = replace_paths(
            &case.params,
            cwd_dir.path().to_str().unwrap(),
            other_dir.path().to_str().unwrap(),
        );
        let result = validate_host_request(&case.method, &params, &policy);

        match case.expect {
            Expect::Allow => {
                assert!(
                    case.rejection.is_none(),
                    "允许用例 '{}' 不得声明 rejection",
                    case.name
                );
                assert!(
                    result.is_ok(),
                    "用例 '{}'（method={}）结果不符：expect=allow, actual={result:?}",
                    case.name,
                    case.method,
                );
            }
            Expect::Reject => {
                let actual = result.expect_err(&format!(
                    "用例 '{}'（method={}）结果不符：expect=reject",
                    case.name, case.method,
                ));
                if let Some(expected) = case.rejection.as_ref() {
                    assert!(
                        rejection_matches(&actual, expected),
                        "用例 '{}'（method={}）拒绝类型不符：expect={expected:?}, actual={actual:?}",
                        case.name,
                        case.method,
                    );
                }
            }
        }
    }
}

/// 判断实际拒绝是否与 fixture 声明的拒绝变体一致，不绑定动态字段内容。
fn rejection_matches(actual: &HostRejection, expected: &Rejection) -> bool {
    match expected {
        Rejection::UnknownMethod => matches!(actual, HostRejection::UnknownMethod(..)),
        Rejection::UnknownMetaKey => matches!(actual, HostRejection::UnknownMetaKey(..)),
        Rejection::CwdMismatch => matches!(actual, HostRejection::CwdMismatch { .. }),
        Rejection::ClientMcpServersNotAllowed => {
            matches!(actual, HostRejection::ClientMcpServersNotAllowed(..))
        }
        Rejection::ForbiddenField => matches!(actual, HostRejection::ForbiddenField(..)),
        Rejection::UnknownField => matches!(actual, HostRejection::UnknownField { .. }),
        Rejection::UnknownNestedField => matches!(actual, HostRejection::UnknownNestedField { .. }),
        Rejection::InvalidFieldType => matches!(actual, HostRejection::InvalidFieldType { .. }),
        Rejection::MissingRequiredField => {
            matches!(actual, HostRejection::MissingRequiredField { .. })
        }
        Rejection::UnsupportedProtocolVersion => {
            matches!(actual, HostRejection::UnsupportedProtocolVersion { .. })
        }
        Rejection::TerminalCapabilityEnabled => {
            matches!(actual, HostRejection::TerminalCapabilityEnabled(..))
        }
        Rejection::FsCapabilityEnabled => matches!(actual, HostRejection::FsCapabilityEnabled(..)),
        Rejection::ModelIdNotAllowed => matches!(actual, HostRejection::ModelIdNotAllowed(..)),
    }
}

/// 递归替换 params 中所有字符串的路径占位符。
fn replace_paths(value: &serde_json::Value, cwd: &str, other: &str) -> serde_json::Value {
    match value {
        serde_json::Value::String(s) => serde_json::Value::String(
            s.replace(CWD_PLACEHOLDER, cwd)
                .replace(OTHER_PLACEHOLDER, other),
        ),
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(|v| replace_paths(v, cwd, other)).collect())
        }
        serde_json::Value::Object(map) => serde_json::Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), replace_paths(v, cwd, other)))
                .collect(),
        ),
        other => other.clone(),
    }
}
