//! host_contract 的 fixture 驱动测试（P3.1）。
//!
//! 读取 `tests/fixtures/host_contract_cases.json`，将 `{{CWD}}` / `{{OTHER}}`
//! 占位符替换为真实临时目录后逐条断言 allow/reject。fixture 同时作为
//! 生产 Host（非 Rust 语言）跑同一套契约用例的权威数据源。

use efflab_agent_sidecar::host_contract::{HostPolicy, validate_host_request};
use tempfile::TempDir;

/// fixture 中使用的路径占位符。
const CWD_PLACEHOLDER: &str = "{{CWD}}";
const OTHER_PLACEHOLDER: &str = "{{OTHER}}";

#[derive(serde::Deserialize)]
struct FixtureCase {
    name: String,
    method: String,
    params: serde_json::Value,
    expect: String,
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

    // 构造策略：允许 modelId=grok-code-fast，cwd 指向 cwd_dir。
    let policy = HostPolicy::new(cwd_dir.path().to_path_buf())
        .with_meta_key("modelId".to_string())
        .with_model_id("grok-code-fast".to_string());

    for case in &cases {
        // 替换路径占位符。
        let params = replace_paths(
            &case.params,
            cwd_dir.path().to_str().unwrap(),
            other_dir.path().to_str().unwrap(),
        );

        let result = validate_host_request(&case.method, &params, &policy);
        let expected_allow = case.expect == "allow";
        let actual_allow = result.is_ok();

        assert_eq!(
            actual_allow, expected_allow,
            "用例 '{}'（method={}）结果不符：expect={}, actual={:?}",
            case.name, case.method, case.expect, result
        );
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
