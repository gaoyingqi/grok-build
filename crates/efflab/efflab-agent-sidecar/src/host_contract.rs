//! Host 请求字段白名单校验（P3.1 / 方案 v3 R7'）。
//!
//! 职责：sidecar 只接受可信 Host 的 ACP 请求。本模块对每个入站请求做
//! **字段白名单**（非黑名单）校验：
//! - `initialize`：`capabilities.terminal` / `capabilities.fs` 必须为 false；
//!   `client.mcpServers` 必须为空数组（MCP 全部来自 `--mcp-config`）。
//! - `session/new` / `session/load`：`cwd` 精确匹配策略指定值；`mcpServers`
//!   必须为空数组；`_meta` 仅允许白名单键；**拒绝** `agentProfile` /
//!   `pluginDirs` / `x.ai/hooks` / `yoloMode` / `capability` 覆盖。
//! - `modelId`（params 或 `_meta` 中）必须在模型白名单内。
//! - 未知 method 默认拒绝（fail-closed）。
//!
//! 字段拼写遵循 ACP wire 协议（camelCase）：`_meta`、`cwd`、`mcpServers`、
//! `capabilities`、`agentProfile`、`pluginDirs`、`yoloMode`、`modelId`。

use std::path::{Path, PathBuf};

use serde_json::Value;
use thiserror::Error;

/// Host 契约策略：可信 Host 必须满足的边界。
#[derive(Debug, Clone)]
pub struct HostPolicy {
    /// `_meta` 中允许出现的键白名单（如 `modelId`）。
    pub allowed_meta_keys: Vec<String>,
    /// 允许的模型 id 白名单（`modelId`）。
    pub allowed_model_ids: Vec<String>,
    /// 期望的 session cwd（canonical 绝对路径）。
    pub expected_cwd: PathBuf,
    /// 允许的 MCP server 名（来自 `--mcp-config`）。
    pub allowed_mcp_servers: Vec<String>,
}

impl HostPolicy {
    /// 构造策略：至少需要期望的 cwd。
    pub fn new(expected_cwd: impl Into<PathBuf>) -> Self {
        Self {
            allowed_meta_keys: Vec::new(),
            allowed_model_ids: Vec::new(),
            expected_cwd: expected_cwd.into(),
            allowed_mcp_servers: Vec::new(),
        }
    }

    /// 追加允许的 `_meta` 键。
    pub fn with_meta_key(mut self, key: impl Into<String>) -> Self {
        self.allowed_meta_keys.push(key.into());
        self
    }

    /// 追加允许的模型 id。
    pub fn with_model_id(mut self, id: impl Into<String>) -> Self {
        self.allowed_model_ids.push(id.into());
        self
    }

    /// 追加允许的 MCP server 名。
    pub fn with_mcp_server(mut self, name: impl Into<String>) -> Self {
        self.allowed_mcp_servers.push(name.into());
        self
    }
}

/// Host 请求被拒绝的原因（含 method 与具体违规点）。
#[derive(Debug, Error, PartialEq, Eq)]
pub enum HostRejection {
    #[error("method {0}: unknown ACP method (fail-closed)")]
    UnknownMethod(String),
    #[error("method {0}: unknown _meta key {1}")]
    UnknownMetaKey(String, String),
    #[error("method {method}: cwd mismatch (expected {expected:?}, got {got:?})")]
    CwdMismatch {
        method: String,
        expected: String,
        got: String,
    },
    #[error("method {0}: client-side mcpServers must be empty")]
    ClientMcpServersNotAllowed(String),
    #[error("method {0}: forbidden field {1}")]
    ForbiddenField(String, String),
    #[error("method {0}: terminal capability must be false")]
    TerminalCapabilityEnabled(String),
    #[error("method {0}: fs capability must be false")]
    FsCapabilityEnabled(String),
    #[error("method {0}: modelId {1} not allowed")]
    ModelIdNotAllowed(String, String),
}

/// 校验一个 ACP 请求（method + params）。通过返回 `Ok(())`，否则返回拒绝原因。
pub fn validate_host_request(
    method: &str,
    params: &Value,
    policy: &HostPolicy,
) -> Result<(), HostRejection> {
    // 先校验 params 顶层结构必须是对象（fail-closed）。
    if !params.is_object() {
        return Err(HostRejection::ForbiddenField(
            method.to_string(),
            "non-object params".to_string(),
        ));
    }
    match method {
        "initialize" => validate_initialize(params, policy),
        "session/new" | "session/load" => validate_session_request(method, params, policy),
        // 只读协议方法：仍校验 _meta 与未知字段，但放宽 cwd/mcpServers 约束。
        "x.ai/mcp/list" => validate_meta_only(method, params, policy),
        _ => Err(HostRejection::UnknownMethod(method.to_string())),
    }
}

/// `initialize`：terminal/fs capability 必须为 false；client mcpServers 为空。
fn validate_initialize(params: &Value, policy: &HostPolicy) -> Result<(), HostRejection> {
    let method = "initialize";

    // capabilities.terminal / capabilities.fs
    if let Some(caps) = params.get("capabilities") {
        if let Some(terminal) = caps.get("terminal")
            && terminal.as_bool().unwrap_or(false)
        {
            return Err(HostRejection::TerminalCapabilityEnabled(method.to_string()));
        }
        if let Some(fs) = caps.get("fs")
            && fs.as_bool().unwrap_or(false)
        {
            return Err(HostRejection::FsCapabilityEnabled(method.to_string()));
        }
    }

    // client.mcpServers 必须为空数组
    if let Some(client) = params.get("client")
        && let Some(servers) = client.get("mcpServers")
        && !servers.as_array().is_some_and(|arr| arr.is_empty())
    {
        return Err(HostRejection::ClientMcpServersNotAllowed(
            method.to_string(),
        ));
    }

    // _meta 白名单 + modelId 校验
    validate_meta_and_model(method, params, policy)
}

/// `session/new` / `session/load`：cwd 精确匹配、mcpServers 空、禁危险字段。
fn validate_session_request(
    method: &str,
    params: &Value,
    policy: &HostPolicy,
) -> Result<(), HostRejection> {
    // cwd 必须存在且与策略一致（canonical 绝对路径比较）。
    let cwd =
        params
            .get("cwd")
            .and_then(Value::as_str)
            .ok_or_else(|| HostRejection::CwdMismatch {
                method: method.to_string(),
                expected: policy.expected_cwd.display().to_string(),
                got: "<missing>".to_string(),
            })?;
    if !cwd_matches(Path::new(cwd), &policy.expected_cwd) {
        return Err(HostRejection::CwdMismatch {
            method: method.to_string(),
            expected: policy.expected_cwd.display().to_string(),
            got: cwd.to_string(),
        });
    }

    // client mcpServers 必须为空（session 级 mcpServers 也不允许）。
    for key in ["mcpServers", "clientMcpServers"] {
        if let Some(v) = params.get(key)
            && !v.as_array().is_some_and(|arr| arr.is_empty())
        {
            return Err(HostRejection::ClientMcpServersNotAllowed(
                method.to_string(),
            ));
        }
    }

    // 危险字段出现即拒绝（字段白名单，fail-closed）。
    for forbidden in [
        "agentProfile",
        "pluginDirs",
        "x.ai/hooks",
        "yoloMode",
        "capability",
        "permissionMode",
    ] {
        if params.get(forbidden).is_some() {
            return Err(HostRejection::ForbiddenField(
                method.to_string(),
                forbidden.to_string(),
            ));
        }
    }

    // _meta 白名单 + modelId 校验。
    validate_meta_and_model(method, params, policy)
}

/// 只读方法：仅校验 `_meta` 白名单与 modelId。
fn validate_meta_only(
    method: &str,
    params: &Value,
    policy: &HostPolicy,
) -> Result<(), HostRejection> {
    validate_meta_and_model(method, params, policy)
}

/// 校验 `_meta` 键白名单，并在 `_meta`/params 中校验 `modelId`。
fn validate_meta_and_model(
    method: &str,
    params: &Value,
    policy: &HostPolicy,
) -> Result<(), HostRejection> {
    // _meta 白名单（未声明任何白名单键时，_meta 必须缺失或为空对象）。
    if let Some(meta) = params.get("_meta") {
        if let Some(obj) = meta.as_object() {
            for key in obj.keys() {
                if !policy.allowed_meta_keys.iter().any(|k| k == key) {
                    return Err(HostRejection::UnknownMetaKey(
                        method.to_string(),
                        key.clone(),
                    ));
                }
            }
        } else if !meta.is_null() {
            return Err(HostRejection::ForbiddenField(
                method.to_string(),
                "_meta".to_string(),
            ));
        }
    }

    // modelId：出现在 _meta 或 params 顶层时必须在白名单。
    let model_id = params
        .get("_meta")
        .and_then(|m| m.get("modelId"))
        .or_else(|| params.get("modelId"))
        .and_then(Value::as_str);
    if let Some(id) = model_id
        && !policy.allowed_model_ids.iter().any(|allowed| allowed == id)
    {
        return Err(HostRejection::ModelIdNotAllowed(
            method.to_string(),
            id.to_string(),
        ));
    }

    Ok(())
}

/// cwd 匹配：canonical 化 Host 提供的 cwd 后与期望值精确比较。
fn cwd_matches(got: &Path, expected: &Path) -> bool {
    match dunce::canonicalize(got) {
        Ok(abs) => abs == expected,
        // canonicalize 失败（路径不存在等）→ 不匹配（fail-closed）。
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// 构造一个默认策略：cwd 指向临时目录。
    fn policy_with(cwd: &Path) -> HostPolicy {
        HostPolicy::new(cwd.to_path_buf())
            .with_meta_key("modelId".to_string())
            .with_model_id("grok-code-fast".to_string())
    }

    #[test]
    fn initialize_ok() {
        let dir = TempDir::new().unwrap();
        let p = policy_with(dir.path());
        let params = serde_json::json!({
            "protocolVersion": 1,
            "capabilities": { "terminal": false, "fs": false },
            "client": { "mcpServers": [] },
            "_meta": { "modelId": "grok-code-fast" }
        });
        assert!(validate_host_request("initialize", &params, &p).is_ok());
    }

    #[test]
    fn initialize_rejects_terminal_capability() {
        let dir = TempDir::new().unwrap();
        let p = policy_with(dir.path());
        let params = serde_json::json!({
            "capabilities": { "terminal": true, "fs": false }
        });
        assert_eq!(
            validate_host_request("initialize", &params, &p),
            Err(HostRejection::TerminalCapabilityEnabled(
                "initialize".into()
            ))
        );
    }

    #[test]
    fn initialize_rejects_fs_capability() {
        let dir = TempDir::new().unwrap();
        let p = policy_with(dir.path());
        let params = serde_json::json!({
            "capabilities": { "terminal": false, "fs": true }
        });
        assert_eq!(
            validate_host_request("initialize", &params, &p),
            Err(HostRejection::FsCapabilityEnabled("initialize".into()))
        );
    }

    #[test]
    fn initialize_rejects_client_mcp_servers() {
        let dir = TempDir::new().unwrap();
        let p = policy_with(dir.path());
        let params = serde_json::json!({
            "client": { "mcpServers": [{ "name": "evil" }] }
        });
        assert_eq!(
            validate_host_request("initialize", &params, &p),
            Err(HostRejection::ClientMcpServersNotAllowed(
                "initialize".into()
            ))
        );
    }

    #[test]
    fn session_new_ok() {
        let dir = TempDir::new().unwrap();
        let p = policy_with(dir.path());
        let params = serde_json::json!({
            "cwd": dir.path().to_str().unwrap(),
            "mcpServers": [],
            "_meta": { "modelId": "grok-code-fast" }
        });
        assert!(validate_host_request("session/new", &params, &p).is_ok());
    }

    #[test]
    fn session_new_rejects_cwd_mismatch() {
        let dir = TempDir::new().unwrap();
        let other = TempDir::new().unwrap();
        let p = policy_with(dir.path());
        let params = serde_json::json!({ "cwd": other.path().to_str().unwrap() });
        let err = validate_host_request("session/new", &params, &p).unwrap_err();
        assert!(matches!(err, HostRejection::CwdMismatch { .. }));
    }

    #[test]
    fn session_new_rejects_agent_profile() {
        let dir = TempDir::new().unwrap();
        let p = policy_with(dir.path());
        let params = serde_json::json!({
            "cwd": dir.path().to_str().unwrap(),
            "agentProfile": { "name": "evil" }
        });
        assert!(matches!(
            validate_host_request("session/new", &params, &p),
            Err(HostRejection::ForbiddenField(..))
        ));
    }

    #[test]
    fn session_new_rejects_plugin_dirs() {
        let dir = TempDir::new().unwrap();
        let p = policy_with(dir.path());
        let params = serde_json::json!({
            "cwd": dir.path().to_str().unwrap(),
            "pluginDirs": ["/tmp/evil"]
        });
        assert!(matches!(
            validate_host_request("session/new", &params, &p),
            Err(HostRejection::ForbiddenField(..))
        ));
    }

    #[test]
    fn session_new_rejects_hooks() {
        let dir = TempDir::new().unwrap();
        let p = policy_with(dir.path());
        let params = serde_json::json!({
            "cwd": dir.path().to_str().unwrap(),
            "x.ai/hooks": { "hooks": [] }
        });
        assert!(matches!(
            validate_host_request("session/new", &params, &p),
            Err(HostRejection::ForbiddenField(..))
        ));
    }

    #[test]
    fn session_new_rejects_yolo() {
        let dir = TempDir::new().unwrap();
        let p = policy_with(dir.path());
        let params = serde_json::json!({
            "cwd": dir.path().to_str().unwrap(),
            "yoloMode": true
        });
        assert!(matches!(
            validate_host_request("session/new", &params, &p),
            Err(HostRejection::ForbiddenField(..))
        ));
    }

    #[test]
    fn session_new_rejects_unknown_meta_key() {
        let dir = TempDir::new().unwrap();
        let p = policy_with(dir.path());
        let params = serde_json::json!({
            "cwd": dir.path().to_str().unwrap(),
            "_meta": { "modelId": "grok-code-fast", "sneaky": 1 }
        });
        assert_eq!(
            validate_host_request("session/new", &params, &p),
            Err(HostRejection::UnknownMetaKey(
                "session/new".into(),
                "sneaky".into()
            ))
        );
    }

    #[test]
    fn session_new_rejects_disallowed_model_id() {
        let dir = TempDir::new().unwrap();
        let p = policy_with(dir.path());
        let params = serde_json::json!({
            "cwd": dir.path().to_str().unwrap(),
            "_meta": { "modelId": "grok-code-evil" }
        });
        assert_eq!(
            validate_host_request("session/new", &params, &p),
            Err(HostRejection::ModelIdNotAllowed(
                "session/new".into(),
                "grok-code-evil".into()
            ))
        );
    }

    #[test]
    fn unknown_method_rejected() {
        let dir = TempDir::new().unwrap();
        let p = policy_with(dir.path());
        let params = serde_json::json!({});
        assert_eq!(
            validate_host_request("session/steal", &params, &p),
            Err(HostRejection::UnknownMethod("session/steal".into()))
        );
    }
}
