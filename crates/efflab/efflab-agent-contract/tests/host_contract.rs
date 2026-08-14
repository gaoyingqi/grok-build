//! 共享 Host 合同 fixture 的直接校验。
//!
//! sidecar 保留同名测试以覆盖兼容 re-export；本测试让 contract crate 自身也能
//! 按发布契约运行相同 fixture，避免只验证转发层。

use efflab_agent_contract::{HostPolicy, validate_host_request};
use tempfile::TempDir;

/// fixture 中使用的路径占位符。
const CWD_PLACEHOLDER: &str = "{{CWD}}";
const OTHER_PLACEHOLDER: &str = "{{OTHER}}";

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
enum Expect {
    Allow,
    Reject,
}

#[derive(serde::Deserialize)]
struct FixtureCase {
    name: String,
    method: String,
    params: serde_json::Value,
    expect: Expect,
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
        // 既有方法保留 modelId，新 prompt 方法只允许 submission 对应的 promptId。
        .with_meta_key_for("initialize", "modelId")
        .with_meta_key_for("session/new", "modelId")
        .with_meta_key_for("session/load", "modelId")
        .with_meta_key_for("x.ai/mcp/list", "modelId")
        .with_meta_key_for("session/prompt", "promptId")
        .with_model_id("grok-code-fast");

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
