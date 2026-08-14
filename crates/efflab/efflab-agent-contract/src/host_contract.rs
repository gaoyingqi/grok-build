//! Host 请求字段白名单校验（P3.1 / 方案 v3 R7'）。
//!
//! 职责：可信 Host 在写入 sidecar stdin 前，必须对每个 ACP 请求执行
//! **字段白名单**（非黑名单）校验：
//! - `initialize` 仅允许协议、客户端、能力、认证与 `_meta` 字段；
//!   `capabilities.terminal` / `capabilities.fs` 必须为 false，
//!   `client.mcpServers` 必须为空数组（MCP 全部来自 `--mcp-config`）。
//! - `session/new` / `session/load` 仅允许会话、cwd、MCP 与 `_meta` 字段；
//!   `cwd` 精确匹配策略指定值，`mcpServers` 必须为空数组。
//! - `session/prompt` 只接受纯文本 ContentBlock，并在写入前阻断 grok-shell
//!   可解析的文件引用文本；`session/cancel` 与 `session/list` 只开放最小字段面。
//! - `_meta` 白名单按 method 隔离；顶层 `modelId` 不在方法字段白名单，
//!   因此一律拒绝。
//! - 未知字段与未知 method 默认拒绝（fail-closed）。
//!
//! 字段拼写遵循 ACP wire 协议（camelCase）：`_meta`、`cwd`、`mcpServers`、
//! `capabilities`、`authentication`、`sessionId`、`modelId`。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde_json::Value;
use thiserror::Error;

/// `initialize` 顶层 params 的唯一允许字段集合。
const INITIALIZE_ALLOWED_FIELDS: &[&str] = &[
    "protocolVersion",
    "client",
    "capabilities",
    "authentication",
    "_meta",
];

/// `session/new` 与 `session/load` 顶层 params 的唯一允许字段集合。
const SESSION_ALLOWED_FIELDS: &[&str] = &["sessionId", "cwd", "mcpServers", "_meta"];

/// `session/prompt` 顶层 params 的唯一允许字段集合。
const SESSION_PROMPT_ALLOWED_FIELDS: &[&str] = &["sessionId", "prompt", "_meta"];

/// `session/cancel` 顶层 params 的唯一允许字段集合。
const SESSION_CANCEL_ALLOWED_FIELDS: &[&str] = &["sessionId"];

/// `session/list` 顶层 params 的唯一允许字段集合。
const SESSION_LIST_ALLOWED_FIELDS: &[&str] = &["cwd", "cursor"];

/// `x.ai/mcp/list` 顶层 params 的唯一允许字段集合。
const MCP_LIST_ALLOWED_FIELDS: &[&str] = &["sessionId", "_meta"];

/// Host 契约策略：可信 Host 必须满足的边界。
#[derive(Debug, Clone)]
pub struct HostPolicy {
    /// 按 method 隔离的 `_meta` 键白名单，防止一个方法的元数据泄漏到另一个方法。
    allowed_meta_keys_by_method: BTreeMap<String, Vec<String>>,
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
            allowed_meta_keys_by_method: BTreeMap::new(),
            allowed_model_ids: Vec::new(),
            expected_cwd: expected_cwd.into(),
            allowed_mcp_servers: Vec::new(),
        }
    }

    /// 为一个指定 method 追加允许的 `_meta` 键。
    pub fn with_meta_key_for(mut self, method: impl Into<String>, key: impl Into<String>) -> Self {
        self.allowed_meta_keys_by_method
            .entry(method.into())
            .or_default()
            .push(key.into());
        self
    }

    /// 返回指定 method 的 `_meta` 白名单；未登记的 method 没有可用元数据键。
    pub fn meta_keys_for(&self, method: &str) -> &[String] {
        self.allowed_meta_keys_by_method
            .get(method)
            .map(Vec::as_slice)
            .unwrap_or(&[])
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

/// 独立文本语义门的拒绝原因，供 Host 展开 mentions 后复用。
#[derive(Debug, Error, PartialEq, Eq)]
pub enum PromptTextRejection {
    #[error("prompt text contains an @ file reference")]
    AtFileReference,
    #[error("prompt text contains a file URI")]
    FileUri,
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
    #[error("method {method}: unknown top-level field {field}")]
    UnknownField { method: String, field: String },
    #[error("method {method}: unknown nested field {field}")]
    UnknownNestedField { method: String, field: String },
    #[error("method {method}: field {field} has an invalid type")]
    InvalidFieldType { method: String, field: String },
    #[error("method {method}: missing required field {field}")]
    MissingRequiredField { method: String, field: String },
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

    // 在进入各 method 的细节校验前，先阻断不在该 method 白名单中的顶层字段。
    validate_top_level_fields(method, params)?;
    // `_meta` 一旦出现必须是对象，null 也不能绕过校验。
    validate_meta_field_type(method, params)?;

    match method {
        "initialize" => validate_initialize(params, policy),
        "session/new" | "session/load" => validate_session_request(method, params, policy),
        "session/prompt" => validate_session_prompt(params, policy),
        "session/cancel" => validate_session_cancel(params),
        "session/list" => validate_session_list(params, policy),
        // 只读协议方法：仍校验 _meta 与未知字段，但放宽 cwd/mcpServers 约束。
        "x.ai/mcp/list" => validate_meta_only(method, params, policy),
        // 已由 validate_top_level_fields 拒绝；保留分支防止未来修改绕过 fail-closed。
        _ => Err(HostRejection::UnknownMethod(method.to_string())),
    }
}

/// 按 method 对顶层 params 字段执行白名单校验。
fn validate_top_level_fields(method: &str, params: &Value) -> Result<(), HostRejection> {
    let allowed_fields = match method {
        "initialize" => INITIALIZE_ALLOWED_FIELDS,
        "session/new" | "session/load" => SESSION_ALLOWED_FIELDS,
        "session/prompt" => SESSION_PROMPT_ALLOWED_FIELDS,
        "session/cancel" => SESSION_CANCEL_ALLOWED_FIELDS,
        "session/list" => SESSION_LIST_ALLOWED_FIELDS,
        "x.ai/mcp/list" => MCP_LIST_ALLOWED_FIELDS,
        _ => return Err(HostRejection::UnknownMethod(method.to_string())),
    };

    let params = params.as_object().ok_or_else(|| {
        HostRejection::ForbiddenField(method.to_string(), "non-object params".to_string())
    })?;
    for field in params.keys() {
        if !allowed_fields.contains(&field.as_str()) {
            return Err(HostRejection::UnknownField {
                method: method.to_string(),
                field: field.clone(),
            });
        }
    }

    Ok(())
}

/// 校验 `_meta` 一旦出现即为对象，防止 null、标量或数组绕过字段类型边界。
fn validate_meta_field_type(method: &str, params: &Value) -> Result<(), HostRejection> {
    if params.get("_meta").is_some_and(|meta| !meta.is_object()) {
        return Err(HostRejection::ForbiddenField(
            method.to_string(),
            "_meta".to_string(),
        ));
    }

    Ok(())
}

/// `initialize`：安全能力与 MCP 字段必须完整且精确，遗漏即 fail-closed。
fn validate_initialize(params: &Value, policy: &HostPolicy) -> Result<(), HostRejection> {
    let method = "initialize";

    // `capabilities` 是安全边界：必须存在、为对象，且只含 terminal/fs 两个显式关闭项。
    // client 的标准 `name` 为描述性字段，除此和 `mcpServers` 外的嵌套键均拒绝。
    let capabilities = params
        .get("capabilities")
        .ok_or_else(|| HostRejection::MissingRequiredField {
            method: method.to_string(),
            field: "capabilities".to_string(),
        })?
        .as_object()
        .ok_or_else(|| HostRejection::InvalidFieldType {
            method: method.to_string(),
            field: "capabilities".to_string(),
        })?;
    validate_nested_fields(method, capabilities, "capabilities", &["terminal", "fs"])?;
    validate_disabled_capability(method, capabilities, "terminal")?;
    validate_disabled_capability(method, capabilities, "fs")?;

    // `client` 是 MCP 注入边界：必须存在、为对象，且只允许固定空 server 数组。
    let client = params
        .get("client")
        .ok_or_else(|| HostRejection::MissingRequiredField {
            method: method.to_string(),
            field: "client".to_string(),
        })?
        .as_object()
        .ok_or_else(|| HostRejection::InvalidFieldType {
            method: method.to_string(),
            field: "client".to_string(),
        })?;
    validate_nested_fields(method, client, "client", &["name", "mcpServers"])?;
    let servers = client
        .get("mcpServers")
        .ok_or_else(|| HostRejection::MissingRequiredField {
            method: method.to_string(),
            field: "client.mcpServers".to_string(),
        })?;
    if !servers.as_array().is_some_and(|array| array.is_empty()) {
        return Err(HostRejection::ClientMcpServersNotAllowed(
            method.to_string(),
        ));
    }

    // _meta 白名单 + modelId 校验。
    validate_meta_and_model(method, params, policy)
}

/// 校验固定安全对象的嵌套白名单，防止未来新能力字段被静默透传。
fn validate_nested_fields(
    method: &str,
    object: &serde_json::Map<String, Value>,
    prefix: &str,
    allowed_fields: &[&str],
) -> Result<(), HostRejection> {
    for field in object.keys() {
        if !allowed_fields.contains(&field.as_str()) {
            return Err(HostRejection::UnknownNestedField {
                method: method.to_string(),
                field: format!("{prefix}.{field}"),
            });
        }
    }

    Ok(())
}

/// 校验 terminal/fs 必须存在且为 false；true 使用既有精确拒绝类型。
fn validate_disabled_capability(
    method: &str,
    capabilities: &serde_json::Map<String, Value>,
    name: &str,
) -> Result<(), HostRejection> {
    let field = format!("capabilities.{name}");
    let value = capabilities
        .get(name)
        .ok_or_else(|| HostRejection::MissingRequiredField {
            method: method.to_string(),
            field: field.clone(),
        })?;

    match value.as_bool() {
        Some(false) => Ok(()),
        Some(true) if name == "terminal" => {
            Err(HostRejection::TerminalCapabilityEnabled(method.to_string()))
        }
        Some(true) if name == "fs" => Err(HostRejection::FsCapabilityEnabled(method.to_string())),
        Some(true) => unreachable!("capability 白名单仅允许 terminal 或 fs"),
        None => Err(HostRejection::InvalidFieldType {
            method: method.to_string(),
            field,
        }),
    }
}

/// `session/new` / `session/load`：cwd 精确匹配、mcpServers 空、_meta 合规。
fn validate_session_request(
    method: &str,
    params: &Value,
    policy: &HostPolicy,
) -> Result<(), HostRejection> {
    // session/load 必须指定要加载的 session；session/new 的 sessionId 可选。
    if method == "session/load" && params.get("sessionId").is_none() {
        return Err(HostRejection::MissingRequiredField {
            method: method.to_string(),
            field: "sessionId".to_string(),
        });
    }

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

    // session 级 MCP 是同一安全边界：字段必须存在且精确为空数组，不能靠缺省透传。
    let servers = params
        .get("mcpServers")
        .ok_or_else(|| HostRejection::MissingRequiredField {
            method: method.to_string(),
            field: "mcpServers".to_string(),
        })?;
    if !servers.as_array().is_some_and(|array| array.is_empty()) {
        return Err(HostRejection::ClientMcpServersNotAllowed(
            method.to_string(),
        ));
    }

    // 顶层危险字段已经由统一白名单在分派前拒绝；此处仅校验 _meta。
    validate_meta_and_model(method, params, policy)
}

/// `session/prompt`：只接受带 submission id 的非空纯文本块。
fn validate_session_prompt(params: &Value, policy: &HostPolicy) -> Result<(), HostRejection> {
    const METHOD: &str = "session/prompt";

    validate_required_non_empty_string(METHOD, params, "sessionId")?;
    let prompt = params
        .get("prompt")
        .ok_or_else(|| HostRejection::MissingRequiredField {
            method: METHOD.to_string(),
            field: "prompt".to_string(),
        })?
        .as_array()
        .ok_or_else(|| HostRejection::InvalidFieldType {
            method: METHOD.to_string(),
            field: "prompt".to_string(),
        })?;
    if prompt.is_empty() {
        return Err(HostRejection::ForbiddenField(
            METHOD.to_string(),
            "prompt".to_string(),
        ));
    }

    for (index, block) in prompt.iter().enumerate() {
        let prefix = format!("prompt[{index}]");
        let block = block
            .as_object()
            .ok_or_else(|| HostRejection::InvalidFieldType {
                method: METHOD.to_string(),
                field: prefix.clone(),
            })?;
        // ContentBlock 收窄为唯一安全的 text 变体，未知键不能静默透传给 grok-shell。
        validate_nested_fields(METHOD, block, &prefix, &["type", "text"])?;

        let block_type = block
            .get("type")
            .ok_or_else(|| HostRejection::MissingRequiredField {
                method: METHOD.to_string(),
                field: format!("{prefix}.type"),
            })?
            .as_str()
            .ok_or_else(|| HostRejection::InvalidFieldType {
                method: METHOD.to_string(),
                field: format!("{prefix}.type"),
            })?;
        if block_type != "text" {
            return Err(HostRejection::ForbiddenField(
                METHOD.to_string(),
                format!("{prefix}.type"),
            ));
        }

        let text = block
            .get("text")
            .ok_or_else(|| HostRejection::MissingRequiredField {
                method: METHOD.to_string(),
                field: format!("{prefix}.text"),
            })?
            .as_str()
            .ok_or_else(|| HostRejection::InvalidFieldType {
                method: METHOD.to_string(),
                field: format!("{prefix}.text"),
            })?;
        if text.is_empty() {
            return Err(HostRejection::ForbiddenField(
                METHOD.to_string(),
                format!("{prefix}.text"),
            ));
        }
        // 复用独立文本门，阻断 shell 对 @ 文件引用和 file URI 的隐式读盘路径。
        validate_prompt_text(text).map_err(|_| {
            HostRejection::ForbiddenField(METHOD.to_string(), format!("{prefix}.text"))
        })?;
    }

    validate_meta_and_model(METHOD, params, policy)?;
    let prompt_id = params
        .get("_meta")
        .and_then(Value::as_object)
        .and_then(|meta| meta.get("promptId"))
        .ok_or_else(|| HostRejection::MissingRequiredField {
            method: METHOD.to_string(),
            field: "_meta.promptId".to_string(),
        })?
        .as_str()
        .ok_or_else(|| HostRejection::InvalidFieldType {
            method: METHOD.to_string(),
            field: "_meta.promptId".to_string(),
        })?;
    if prompt_id.is_empty() {
        return Err(HostRejection::InvalidFieldType {
            method: METHOD.to_string(),
            field: "_meta.promptId".to_string(),
        });
    }

    Ok(())
}

/// `session/cancel` 是无 reply 的通知，只允许关联到一个已知 session。
fn validate_session_cancel(params: &Value) -> Result<(), HostRejection> {
    validate_required_non_empty_string("session/cancel", params, "sessionId")
}

/// `session/list` 只能列出当前 scope 的 session，并只接受标准 cursor 分页字段。
fn validate_session_list(params: &Value, policy: &HostPolicy) -> Result<(), HostRejection> {
    const METHOD: &str = "session/list";
    let cwd =
        params
            .get("cwd")
            .and_then(Value::as_str)
            .ok_or_else(|| HostRejection::CwdMismatch {
                method: METHOD.to_string(),
                expected: policy.expected_cwd.display().to_string(),
                got: "<missing>".to_string(),
            })?;
    if !cwd_matches(Path::new(cwd), &policy.expected_cwd) {
        return Err(HostRejection::CwdMismatch {
            method: METHOD.to_string(),
            expected: policy.expected_cwd.display().to_string(),
            got: cwd.to_string(),
        });
    }
    if params
        .get("cursor")
        .is_some_and(|cursor| cursor.as_str().is_none())
    {
        return Err(HostRejection::InvalidFieldType {
            method: METHOD.to_string(),
            field: "cursor".to_string(),
        });
    }

    Ok(())
}

/// 校验必填字符串字段，空值和非字符串都不能绕过 session 关联边界。
fn validate_required_non_empty_string(
    method: &str,
    params: &Value,
    field: &str,
) -> Result<(), HostRejection> {
    let value = params
        .get(field)
        .ok_or_else(|| HostRejection::MissingRequiredField {
            method: method.to_string(),
            field: field.to_string(),
        })?;
    if value.as_str().is_some_and(|value| !value.is_empty()) {
        Ok(())
    } else {
        Err(HostRejection::InvalidFieldType {
            method: method.to_string(),
            field: field.to_string(),
        })
    }
}

/// 校验 Host 组包或展开 mentions 后的单段 prompt 文本。
///
/// grok-shell 的 `prompt_parser::collect_file_references` 会从每个 `@` 后的
/// 剩余文本取 `split_whitespace().next()`，再交给 `FileReference::parse`。
/// 因此此处使用同一 token 边界，只要 token 会成为解析候选就拒绝；单独的
/// 尾随 `@` 或仅跟空白不会触发文件读取。`file://` 同样可能指向本地文件，
/// 所以按 ASCII 大小写不敏感的 URI scheme 拒绝。
pub fn validate_prompt_text(text: &str) -> Result<(), PromptTextRejection> {
    if contains_file_uri(text) {
        return Err(PromptTextRejection::FileUri);
    }
    if contains_at_file_reference(text) {
        return Err(PromptTextRejection::AtFileReference);
    }

    Ok(())
}

/// 判断文本中的任一 `@` 是否会在上游成为可解析的 FileReference token。
fn contains_at_file_reference(text: &str) -> bool {
    text.match_indices('@').any(|(offset, _)| {
        let token = text[offset + '@'.len_utf8()..]
            .split_whitespace()
            .next()
            .unwrap_or_default();
        let token = token.strip_prefix('@').unwrap_or(token);
        // 上游正则允许一个可选 @，但文件路径的首字符不能再是 @。
        token.chars().next().is_some_and(|first| first != '@')
    })
}

/// 判断文本是否含有独立的、不区分 ASCII 大小写的 `file://` URI scheme。
fn contains_file_uri(text: &str) -> bool {
    const FILE_URI: &[u8] = b"file://";
    text.as_bytes()
        .windows(FILE_URI.len())
        .enumerate()
        .any(|(offset, candidate)| {
            candidate.eq_ignore_ascii_case(FILE_URI)
                && (offset == 0 || !is_uri_scheme_byte(text.as_bytes()[offset - 1]))
        })
}

/// URI scheme 允许的 ASCII 字节；避免将 `profile://` 的后缀误判为 `file://`。
fn is_uri_scheme_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'-' | b'.')
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
    // _meta 白名单按 method 隔离：未登记键时，_meta 必须缺失或为空对象。
    // 顶层类型已由 validate_meta_field_type 在分派前校验；此处保留既有语义。
    if let Some(meta) = params.get("_meta") {
        if let Some(obj) = meta.as_object() {
            for key in obj.keys() {
                if !policy
                    .meta_keys_for(method)
                    .iter()
                    .any(|allowed| allowed == key)
                {
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
            // 保留 Task 1/2 已有 method 的 modelId 行为，同时把 promptId 限在 prompt。
            .with_meta_key_for("initialize", "modelId")
            .with_meta_key_for("session/new", "modelId")
            .with_meta_key_for("session/load", "modelId")
            .with_meta_key_for("x.ai/mcp/list", "modelId")
            .with_meta_key_for("session/prompt", "promptId")
            .with_model_id("grok-code-fast".to_string())
    }

    #[test]
    fn whitelisted_top_level_fields_are_allowed() {
        let dir = TempDir::new().unwrap();
        let p = policy_with(dir.path());
        let cwd = dir.path().to_str().unwrap();
        let cases = vec![
            (
                "initialize",
                "initialize",
                serde_json::json!({
                    "protocolVersion": 1,
                    "capabilities": { "terminal": false, "fs": false },
                    "client": { "mcpServers": [] },
                    "authentication": {},
                    "_meta": { "modelId": "grok-code-fast" }
                }),
            ),
            (
                "session/new",
                "session/new",
                serde_json::json!({
                    "sessionId": "new-session",
                    "cwd": cwd,
                    "mcpServers": [],
                    "_meta": { "modelId": "grok-code-fast" }
                }),
            ),
            (
                "session/load",
                "session/load",
                serde_json::json!({
                    "sessionId": "existing-session",
                    "cwd": cwd,
                    "mcpServers": [],
                    "_meta": { "modelId": "grok-code-fast" }
                }),
            ),
            (
                "x.ai/mcp/list",
                "x.ai/mcp/list",
                serde_json::json!({
                    "sessionId": "existing-session",
                    "_meta": { "modelId": "grok-code-fast" }
                }),
            ),
            (
                "session/prompt",
                "session/prompt",
                serde_json::json!({
                    "sessionId": "existing-session",
                    "prompt": [{ "type": "text", "text": "处理 /Volumes/Music/Inbox" }],
                    "_meta": { "promptId": "submission-1" }
                }),
            ),
            (
                "session/cancel",
                "session/cancel",
                serde_json::json!({ "sessionId": "existing-session" }),
            ),
            (
                "session/list",
                "session/list",
                serde_json::json!({ "cwd": cwd, "cursor": "next-page" }),
            ),
        ];

        for (name, method, params) in cases {
            assert!(
                validate_host_request(method, &params, &p).is_ok(),
                "白名单用例 {name} 应通过"
            );
        }
    }

    #[test]
    fn meta_keys_are_isolated_by_method() {
        let dir = TempDir::new().unwrap();
        let policy = policy_with(dir.path());
        let prompt_keys: Vec<&str> = policy
            .meta_keys_for("session/prompt")
            .iter()
            .map(String::as_str)
            .collect();
        let new_session_keys: Vec<&str> = policy
            .meta_keys_for("session/new")
            .iter()
            .map(String::as_str)
            .collect();

        assert_eq!(prompt_keys, ["promptId"]);
        assert_eq!(new_session_keys, ["modelId"]);
        assert!(policy.meta_keys_for("session/cancel").is_empty());
        assert!(policy.meta_keys_for("session/list").is_empty());
    }

    #[test]
    fn prompt_text_gate_matches_grok_file_reference_tokens() {
        for text in [
            "普通中文请求",
            "请处理 /Volumes/Music/Inbox 这批歌曲",
            "末尾 @",
            "末尾 @ \n\t",
            "profile://不是本地 file URI",
        ] {
            assert!(validate_prompt_text(text).is_ok(), "文本应允许: {text:?}");
        }

        for (text, expected) in [
            ("请读取 @secret.txt", PromptTextRejection::AtFileReference),
            ("请读取 @foo/bar", PromptTextRejection::AtFileReference),
            (
                "请读取 @../secret.txt",
                PromptTextRejection::AtFileReference,
            ),
            ("请读取 @~/secret.txt", PromptTextRejection::AtFileReference),
            (
                "请读取 @C:\\secret.txt",
                PromptTextRejection::AtFileReference,
            ),
            (
                "请读取 @\\\\server\\share\\secret.txt",
                PromptTextRejection::AtFileReference,
            ),
            (
                "请读取 @\\\\?\\C:\\secret.txt",
                PromptTextRejection::AtFileReference,
            ),
            (
                "引号边界 \"@secret.txt\"",
                PromptTextRejection::AtFileReference,
            ),
            ("标点边界（@foo/bar）", PromptTextRejection::AtFileReference),
            (
                "换行边界\n@../secret.txt",
                PromptTextRejection::AtFileReference,
            ),
            ("file:///etc/passwd", PromptTextRejection::FileUri),
            ("FILE:///etc/passwd", PromptTextRejection::FileUri),
        ] {
            assert_eq!(
                validate_prompt_text(text),
                Err(expected),
                "文本应拒绝: {text:?}"
            );
        }
    }

    #[test]
    fn prompt_text_gate_can_be_reused_after_host_mention_expansion() {
        // Host 展开曲库 mention 后仍必须使用同一纯函数，而不是自行复制匹配规则。
        let safe_expansion = "曲目《晚风》已加入待整理列表";
        let unsafe_expansion = "展开结果包含 @../private/secret.txt";

        assert!(validate_prompt_text(safe_expansion).is_ok());
        assert_eq!(
            validate_prompt_text(unsafe_expansion),
            Err(PromptTextRejection::AtFileReference)
        );
    }

    #[test]
    fn initialize_rejects_terminal_capability() {
        let dir = TempDir::new().unwrap();
        let p = policy_with(dir.path());
        let params = serde_json::json!({
            "capabilities": { "terminal": true, "fs": false },
            "client": { "mcpServers": [] }
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
            "capabilities": { "terminal": false, "fs": true },
            "client": { "mcpServers": [] }
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
            "capabilities": { "terminal": false, "fs": false },
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
        assert_eq!(
            validate_host_request("session/new", &params, &p),
            Err(HostRejection::UnknownField {
                method: "session/new".into(),
                field: "agentProfile".into(),
            })
        );
    }

    #[test]
    fn session_new_rejects_plugin_dirs() {
        let dir = TempDir::new().unwrap();
        let p = policy_with(dir.path());
        let params = serde_json::json!({
            "cwd": dir.path().to_str().unwrap(),
            "pluginDirs": ["/tmp/evil"]
        });
        assert_eq!(
            validate_host_request("session/new", &params, &p),
            Err(HostRejection::UnknownField {
                method: "session/new".into(),
                field: "pluginDirs".into(),
            })
        );
    }

    #[test]
    fn session_new_rejects_hooks() {
        let dir = TempDir::new().unwrap();
        let p = policy_with(dir.path());
        let params = serde_json::json!({
            "cwd": dir.path().to_str().unwrap(),
            "x.ai/hooks": { "hooks": [] }
        });
        assert_eq!(
            validate_host_request("session/new", &params, &p),
            Err(HostRejection::UnknownField {
                method: "session/new".into(),
                field: "x.ai/hooks".into(),
            })
        );
    }

    #[test]
    fn session_new_rejects_yolo() {
        let dir = TempDir::new().unwrap();
        let p = policy_with(dir.path());
        let params = serde_json::json!({
            "cwd": dir.path().to_str().unwrap(),
            "yoloMode": true
        });
        assert_eq!(
            validate_host_request("session/new", &params, &p),
            Err(HostRejection::UnknownField {
                method: "session/new".into(),
                field: "yoloMode".into(),
            })
        );
    }

    #[test]
    fn session_new_rejects_unknown_meta_key() {
        let dir = TempDir::new().unwrap();
        let p = policy_with(dir.path());
        let params = serde_json::json!({
            "cwd": dir.path().to_str().unwrap(),
            "mcpServers": [],
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
            "mcpServers": [],
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

    #[test]
    fn rejects_whitelist_and_type_violations_table_driven() {
        let dir = TempDir::new().unwrap();
        let p = policy_with(dir.path());
        let cwd = dir.path().to_str().unwrap();
        let cases = vec![
            (
                "initialize 未知顶层字段",
                "initialize",
                serde_json::json!({ "sneakyField": 1 }),
                HostRejection::UnknownField {
                    method: "initialize".into(),
                    field: "sneakyField".into(),
                },
            ),
            (
                "session/new permissionMode",
                "session/new",
                serde_json::json!({ "cwd": cwd, "permissionMode": "yolo" }),
                HostRejection::UnknownField {
                    method: "session/new".into(),
                    field: "permissionMode".into(),
                },
            ),
            (
                "session/new capability",
                "session/new",
                serde_json::json!({ "cwd": cwd, "capability": "terminal" }),
                HostRejection::UnknownField {
                    method: "session/new".into(),
                    field: "capability".into(),
                },
            ),
            (
                "数组 params",
                "initialize",
                serde_json::json!([]),
                HostRejection::ForbiddenField("initialize".into(), "non-object params".into()),
            ),
            (
                "字符串 params",
                "session/new",
                serde_json::json!("not-an-object"),
                HostRejection::ForbiddenField("session/new".into(), "non-object params".into()),
            ),
            (
                "null params",
                "x.ai/mcp/list",
                serde_json::json!(null),
                HostRejection::ForbiddenField("x.ai/mcp/list".into(), "non-object params".into()),
            ),
            (
                "capabilities 非对象",
                "initialize",
                serde_json::json!({
                    "capabilities": [],
                    "client": { "mcpServers": [] }
                }),
                HostRejection::InvalidFieldType {
                    method: "initialize".into(),
                    field: "capabilities".into(),
                },
            ),
            (
                "client 非对象",
                "initialize",
                serde_json::json!({
                    "capabilities": { "terminal": false, "fs": false },
                    "client": "not-an-object"
                }),
                HostRejection::InvalidFieldType {
                    method: "initialize".into(),
                    field: "client".into(),
                },
            ),
            (
                "terminal 为字符串 false",
                "initialize",
                serde_json::json!({
                    "capabilities": { "terminal": "false", "fs": false },
                    "client": { "mcpServers": [] }
                }),
                HostRejection::InvalidFieldType {
                    method: "initialize".into(),
                    field: "capabilities.terminal".into(),
                },
            ),
            (
                "fs 为数字 0",
                "initialize",
                serde_json::json!({
                    "capabilities": { "terminal": false, "fs": 0 },
                    "client": { "mcpServers": [] }
                }),
                HostRejection::InvalidFieldType {
                    method: "initialize".into(),
                    field: "capabilities.fs".into(),
                },
            ),
            (
                "顶层 modelId",
                "session/new",
                serde_json::json!({ "cwd": cwd, "modelId": "grok-code-fast" }),
                HostRejection::UnknownField {
                    method: "session/new".into(),
                    field: "modelId".into(),
                },
            ),
            (
                "_meta 为标量",
                "initialize",
                serde_json::json!({
                    "capabilities": { "terminal": false, "fs": false },
                    "client": { "mcpServers": [] },
                    "_meta": "invalid"
                }),
                HostRejection::ForbiddenField("initialize".into(), "_meta".into()),
            ),
            (
                "_meta 为数组",
                "session/new",
                serde_json::json!({ "cwd": cwd, "_meta": [] }),
                HostRejection::ForbiddenField("session/new".into(), "_meta".into()),
            ),
            (
                "_meta 为 null",
                "x.ai/mcp/list",
                serde_json::json!({ "_meta": null }),
                HostRejection::ForbiddenField("x.ai/mcp/list".into(), "_meta".into()),
            ),
            (
                "session/load 缺 sessionId",
                "session/load",
                serde_json::json!({ "cwd": cwd, "mcpServers": [] }),
                HostRejection::MissingRequiredField {
                    method: "session/load".into(),
                    field: "sessionId".into(),
                },
            ),
            (
                "session/load 未知顶层字段",
                "session/load",
                serde_json::json!({
                    "sessionId": "s1",
                    "cwd": cwd,
                    "sneakyField": 1
                }),
                HostRejection::UnknownField {
                    method: "session/load".into(),
                    field: "sneakyField".into(),
                },
            ),
            (
                "session/load mcpServers 非空",
                "session/load",
                serde_json::json!({
                    "sessionId": "s1",
                    "cwd": cwd,
                    "mcpServers": [{ "name": "evil" }]
                }),
                HostRejection::ClientMcpServersNotAllowed("session/load".into()),
            ),
            (
                "session/new 缺 cwd",
                "session/new",
                serde_json::json!({ "mcpServers": [] }),
                HostRejection::CwdMismatch {
                    method: "session/new".into(),
                    expected: dir.path().display().to_string(),
                    got: "<missing>".into(),
                },
            ),
            (
                "mcp/list 未知 meta 键",
                "x.ai/mcp/list",
                serde_json::json!({ "_meta": { "sneaky": 1 } }),
                HostRejection::UnknownMetaKey("x.ai/mcp/list".into(), "sneaky".into()),
            ),
            (
                "mcp/list 不允许的 meta modelId",
                "x.ai/mcp/list",
                serde_json::json!({ "_meta": { "modelId": "grok-code-evil" } }),
                HostRejection::ModelIdNotAllowed("x.ai/mcp/list".into(), "grok-code-evil".into()),
            ),
            (
                "mcp/list 顶层 modelId",
                "x.ai/mcp/list",
                serde_json::json!({ "modelId": "grok-code-fast" }),
                HostRejection::UnknownField {
                    method: "x.ai/mcp/list".into(),
                    field: "modelId".into(),
                },
            ),
        ];

        for (name, method, params, expected) in cases {
            assert_eq!(
                validate_host_request(method, &params, &p),
                Err(expected),
                "用例 {name} 应被精确拒绝"
            );
        }
    }
}
