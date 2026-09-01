//! Host 请求字段白名单校验（P3.1 / 方案 v3 R7'）。
//!
//! 职责：可信 Host 在写入 sidecar stdin 前，必须对每个 ACP 请求执行
//! **字段白名单**（非黑名单）校验：
//! - `initialize` 仅允许固定协议版本、客户端能力、客户端信息与 `_meta` 字段；
//!   `protocolVersion` 必须精确等于 Host 固定 ACP 版本，
//!   `clientCapabilities.terminal`、`clientCapabilities.fs.readTextFile` 与
//!   `clientCapabilities.fs.writeTextFile` 必须为 false。
//! - `session/new` / `session/load` 仅允许会话、cwd、MCP 与 `_meta` 字段；
//!   `cwd` 精确匹配策略指定值，`mcpServers` 必须为空数组。
//! - `session/prompt` 只接受纯文本 ContentBlock，并在写入前阻断 grok-shell
//!   可解析的文件引用文本；`session/cancel` 与 `session/list` 只开放最小字段面。
//! - `_meta` 白名单按 method 隔离；顶层 `modelId` 不在方法字段白名单，
//!   因此一律拒绝。
//! - 未知字段与未知 method 默认拒绝（fail-closed）。
//!
//! 字段拼写遵循 ACP wire 协议（camelCase）：`_meta`、`cwd`、`mcpServers`、
//! `clientCapabilities`、`clientInfo`、`sessionId`、`modelId`。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde_json::Value;
use thiserror::Error;

/// Host 支持且写入每个 `initialize` 请求的唯一 ACP 协议版本。
pub const HOST_ACP_PROTOCOL_VERSION: u64 = 1;

/// promptId 的共享持久化边界；按 UTF-8 字节计算，而不是 Unicode 字符数。
const MAX_PROMPT_ID_BYTES: usize = 1024;

/// 校验 ACP `_meta.promptId` 的非空、无控制字符和字节上限合同。
pub fn is_prompt_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_PROMPT_ID_BYTES && !value.chars().any(char::is_control)
}

/// `initialize` 顶层 params 的唯一允许字段集合。
const INITIALIZE_ALLOWED_FIELDS: &[&str] = &[
    "protocolVersion",
    "clientCapabilities",
    "clientInfo",
    "_meta",
];

/// `session/new` 顶层 params 的唯一允许字段集合。
const SESSION_NEW_ALLOWED_FIELDS: &[&str] = &["cwd", "mcpServers", "_meta"];

/// `session/load` 顶层 params 的唯一允许字段集合。
const SESSION_LOAD_ALLOWED_FIELDS: &[&str] = &["sessionId", "cwd", "mcpServers", "_meta"];

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

    /// 为一个指定 method 追加合同允许的 `_meta` 键。
    ///
    /// 保持 fluent API 兼容，但非法组合会被忽略，不能通过 builder 改写固定合同。
    pub fn with_meta_key_for(mut self, method: impl Into<String>, key: impl Into<String>) -> Self {
        let method = method.into();
        let key = key.into();
        let allowed = matches!(
            (method.as_str(), key.as_str()),
            ("session/new", "modelId")
                | ("session/load", "modelId")
                | ("session/prompt", "promptId")
        );
        if allowed {
            self.allowed_meta_keys_by_method
                .entry(method)
                .or_default()
                .push(key);
        }
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
    #[error("method {method}: unsupported protocolVersion (expected {expected}, got {got})")]
    UnsupportedProtocolVersion {
        method: String,
        expected: u64,
        got: u64,
    },
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
        // 只读协议方法：要求 sessionId，并只允许空 _meta。
        "x.ai/mcp/list" => validate_mcp_list(params),
        // 已由 validate_top_level_fields 拒绝；保留分支防止未来修改绕过 fail-closed。
        _ => Err(HostRejection::UnknownMethod(method.to_string())),
    }
}

/// 按 method 对顶层 params 字段执行白名单校验。
fn validate_top_level_fields(method: &str, params: &Value) -> Result<(), HostRejection> {
    let allowed_fields = match method {
        "initialize" => INITIALIZE_ALLOWED_FIELDS,
        "session/new" => SESSION_NEW_ALLOWED_FIELDS,
        "session/load" => SESSION_LOAD_ALLOWED_FIELDS,
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

/// `initialize`：按真实 ACP `InitializeRequest` 校验客户端能力与身份，遗漏即 fail-closed。
fn validate_initialize(params: &Value, _policy: &HostPolicy) -> Result<(), HostRejection> {
    let method = "initialize";

    // 先固定协议版本，保证缺失或类型错误返回稳定的 protocolVersion 路径。
    let protocol_version = params
        .get("protocolVersion")
        .ok_or_else(|| HostRejection::MissingRequiredField {
            method: method.to_string(),
            field: "protocolVersion".to_string(),
        })?
        .as_u64()
        .ok_or_else(|| HostRejection::InvalidFieldType {
            method: method.to_string(),
            field: "protocolVersion".to_string(),
        })?;
    if protocol_version != HOST_ACP_PROTOCOL_VERSION {
        return Err(HostRejection::UnsupportedProtocolVersion {
            method: method.to_string(),
            expected: HOST_ACP_PROTOCOL_VERSION,
            got: protocol_version,
        });
    }

    // Host 明确声明不提供文件与终端能力；这些字段属于 clientCapabilities，而非 agentCapabilities。
    let client_capabilities = params
        .get("clientCapabilities")
        .ok_or_else(|| HostRejection::MissingRequiredField {
            method: method.to_string(),
            field: "clientCapabilities".to_string(),
        })?
        .as_object()
        .ok_or_else(|| HostRejection::InvalidFieldType {
            method: method.to_string(),
            field: "clientCapabilities".to_string(),
        })?;
    validate_nested_fields(
        method,
        client_capabilities,
        "clientCapabilities",
        &["fs", "terminal"],
    )?;
    validate_disabled_capability(method, client_capabilities, "terminal")?;

    let fs = client_capabilities
        .get("fs")
        .ok_or_else(|| HostRejection::MissingRequiredField {
            method: method.to_string(),
            field: "clientCapabilities.fs".to_string(),
        })?
        .as_object()
        .ok_or_else(|| HostRejection::InvalidFieldType {
            method: method.to_string(),
            field: "clientCapabilities.fs".to_string(),
        })?;
    validate_nested_fields(
        method,
        fs,
        "clientCapabilities.fs",
        &["readTextFile", "writeTextFile"],
    )?;
    validate_disabled_capability(method, fs, "readTextFile")?;
    validate_disabled_capability(method, fs, "writeTextFile")?;

    // clientInfo 是 ACP 的实现身份；Host 固定发送 name/version，不携带可选 title 或 _meta。
    let client_info = params
        .get("clientInfo")
        .ok_or_else(|| HostRejection::MissingRequiredField {
            method: method.to_string(),
            field: "clientInfo".to_string(),
        })?
        .as_object()
        .ok_or_else(|| HostRejection::InvalidFieldType {
            method: method.to_string(),
            field: "clientInfo".to_string(),
        })?;
    validate_nested_fields(method, client_info, "clientInfo", &["name", "version"])?;
    validate_required_nonempty_string(method, client_info, "name")?;
    validate_required_nonempty_string(method, client_info, "version")?;

    // initialize 只允许标准空 _meta，避免旧产品 modelId 语义通过策略配置重新进入。
    validate_meta_keys(method, params, &[])
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

/// 校验 ACP 客户端能力字段必须存在且为 false；true 使用既有精确拒绝类型。
fn validate_disabled_capability(
    method: &str,
    capabilities: &serde_json::Map<String, Value>,
    name: &str,
) -> Result<(), HostRejection> {
    let field = if name == "terminal" {
        "clientCapabilities.terminal".to_string()
    } else {
        format!("clientCapabilities.fs.{name}")
    };
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
        Some(true) => Err(HostRejection::FsCapabilityEnabled(method.to_string())),
        None => Err(HostRejection::InvalidFieldType {
            method: method.to_string(),
            field,
        }),
    }
}

/// 校验 ACP `clientInfo` 的必填字符串字段，拒绝空字符串与非字符串值。
fn validate_required_nonempty_string(
    method: &str,
    object: &serde_json::Map<String, Value>,
    name: &str,
) -> Result<(), HostRejection> {
    let field = format!("clientInfo.{name}");
    let value = object
        .get(name)
        .ok_or_else(|| HostRejection::MissingRequiredField {
            method: method.to_string(),
            field: field.clone(),
        })?;
    if !value.as_str().is_some_and(|value| !value.is_empty()) {
        return Err(if value.is_string() {
            HostRejection::ForbiddenField(method.to_string(), field)
        } else {
            HostRejection::InvalidFieldType {
                method: method.to_string(),
                field,
            }
        });
    }
    Ok(())
}

/// `session/new` / `session/load`：cwd 精确匹配、mcpServers 空、_meta 合规。
fn validate_session_request(
    method: &str,
    params: &Value,
    policy: &HostPolicy,
) -> Result<(), HostRejection> {
    // session/load 必须指定非空 sessionId；session/new 不接受该字段。
    if method == "session/load" {
        validate_required_non_empty_string(method, params, "sessionId")?;
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
    // 与 grok-shell 的 collect_file_references 保持同一单游标推进方式：
    // 每次提取候选后前移游标，避免再次检查同一 token 内的后续 @。
    let mut cursor = 0;
    while cursor < text.len() {
        if !text.is_char_boundary(cursor) {
            cursor += 1;
            continue;
        }

        let Some(at_symbol_offset) = text[cursor..].find('@') else {
            break;
        };
        let start = cursor + at_symbol_offset + '@'.len_utf8();
        if start >= text.len() || !text.is_char_boundary(start) {
            break;
        }

        let rest = &text[start..];
        let token = rest.split_whitespace().next().unwrap_or_default();
        // FileReference::parse 最多剥离一个前导 @，随后路径首字符不能再是 @。
        let path = token.strip_prefix('@').unwrap_or(token);
        if path.chars().next().is_some_and(|first| first != '@') {
            return true;
        }

        // 与上游相同地跳过本次候选，确保连续 @ 只作为一个 token 判定。
        cursor = start + token.len().max(1);
    }

    false
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

/// `x.ai/mcp/list`：要求非空 sessionId，且 `_meta` 只能缺失或为空对象。
fn validate_mcp_list(params: &Value) -> Result<(), HostRejection> {
    const METHOD: &str = "x.ai/mcp/list";

    validate_required_non_empty_string(METHOD, params, "sessionId")?;
    validate_meta_keys(METHOD, params, &[])
}

/// 校验 `_meta` 键白名单，并在允许的会话方法中校验 Channel 槽名 `modelId`。
fn validate_meta_and_model(
    method: &str,
    params: &Value,
    policy: &HostPolicy,
) -> Result<(), HostRejection> {
    // _meta 白名单按 method 隔离：未登记键时，_meta 必须缺失或为空对象。
    validate_meta_keys(method, params, policy.meta_keys_for(method))?;

    // 允许的 _meta.promptId 必须先满足共享边界，不能让空值、控制字符或超长值进入 ACP。
    if let Some(prompt_id) = params.get("_meta").and_then(|meta| meta.get("promptId")) {
        let prompt_id = prompt_id
            .as_str()
            .ok_or_else(|| HostRejection::InvalidFieldType {
                method: method.to_string(),
                field: "_meta.promptId".to_string(),
            })?;
        if !is_prompt_id(prompt_id) {
            return Err(HostRejection::ForbiddenField(
                method.to_string(),
                "_meta.promptId".to_string(),
            ));
        }
    }

    // 顶层 modelId 已被字段白名单拒绝；允许的 _meta.modelId 必须先是字符串，
    // 再比较策略白名单，不能让标量或数组静默绕过 fail-closed 校验。
    if let Some(model_id) = params.get("_meta").and_then(|meta| meta.get("modelId")) {
        let id = model_id
            .as_str()
            .ok_or_else(|| HostRejection::InvalidFieldType {
                method: method.to_string(),
                field: "_meta.modelId".to_string(),
            })?;
        if !policy.allowed_model_ids.iter().any(|allowed| allowed == id) {
            return Err(HostRejection::ModelIdNotAllowed(
                method.to_string(),
                id.to_string(),
            ));
        }
    }

    Ok(())
}

/// 按调用方提供的白名单校验 `_meta`，用于 initialize 的固定空元数据合同。
fn validate_meta_keys(
    method: &str,
    params: &Value,
    allowed_meta_keys: &[String],
) -> Result<(), HostRejection> {
    if let Some(meta) = params.get("_meta") {
        if let Some(obj) = meta.as_object() {
            for key in obj.keys() {
                if !allowed_meta_keys.iter().any(|allowed| allowed == key) {
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
            // 仅会话方法接收 modelId；prompt 只接收 promptId，initialize 不携带产品元数据。
            .with_meta_key_for("session/new", "modelId")
            .with_meta_key_for("session/load", "modelId")
            .with_meta_key_for("session/prompt", "promptId")
            .with_model_id("byok".to_string())
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
                    "clientCapabilities": {
                        "fs": { "readTextFile": false, "writeTextFile": false },
                        "terminal": false
                    },
                    "clientInfo": { "name": "efflab-test-client", "version": "0.1.0" }
                }),
            ),
            (
                "session/new",
                "session/new",
                serde_json::json!({
                    "cwd": cwd,
                    "mcpServers": [],
                    "_meta": { "modelId": "byok" }
                }),
            ),
            (
                "session/load",
                "session/load",
                serde_json::json!({
                    "sessionId": "existing-session",
                    "cwd": cwd,
                    "mcpServers": [],
                    "_meta": { "modelId": "byok" }
                }),
            ),
            (
                "x.ai/mcp/list",
                "x.ai/mcp/list",
                serde_json::json!({ "sessionId": "existing-session" }),
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

    /// initialize 必须固定使用 Host 支持的 ACP 版本，并拒绝任何认证注入字段。
    #[test]
    fn initialize_requires_pinned_protocol_version_and_rejects_authentication() {
        let directory = TempDir::new().expect("必须能创建契约临时目录");
        let policy = policy_with(directory.path());
        let valid_fields = serde_json::json!({
            "clientCapabilities": {
                        "fs": { "readTextFile": false, "writeTextFile": false },
                        "terminal": false
                    },
            "clientInfo": { "name": "efflab-test-client", "version": "0.1.0" },
        });

        assert_eq!(
            validate_host_request("initialize", &valid_fields, &policy),
            Err(HostRejection::MissingRequiredField {
                method: "initialize".to_string(),
                field: "protocolVersion".to_string(),
            })
        );
        assert_eq!(
            validate_host_request(
                "initialize",
                &serde_json::json!({
                    "protocolVersion": "1",
                    "clientCapabilities": {
                        "fs": { "readTextFile": false, "writeTextFile": false },
                        "terminal": false
                    },
                    "clientInfo": { "name": "efflab-test-client", "version": "0.1.0" },
                }),
                &policy,
            ),
            Err(HostRejection::InvalidFieldType {
                method: "initialize".to_string(),
                field: "protocolVersion".to_string(),
            })
        );
        assert_eq!(
            validate_host_request(
                "initialize",
                &serde_json::json!({
                    "protocolVersion": 2,
                    "clientCapabilities": {
                        "fs": { "readTextFile": false, "writeTextFile": false },
                        "terminal": false
                    },
                    "clientInfo": { "name": "efflab-test-client", "version": "0.1.0" },
                }),
                &policy,
            ),
            Err(HostRejection::UnsupportedProtocolVersion {
                method: "initialize".to_string(),
                expected: HOST_ACP_PROTOCOL_VERSION,
                got: 2,
            })
        );
        assert_eq!(
            validate_host_request(
                "initialize",
                &serde_json::json!({
                    "protocolVersion": HOST_ACP_PROTOCOL_VERSION,
                    "clientCapabilities": {
                        "fs": { "readTextFile": false, "writeTextFile": false },
                        "terminal": false
                    },
                    "clientInfo": { "name": "efflab-test-client", "version": "0.1.0" },
                    "authentication": {},
                }),
                &policy,
            ),
            Err(HostRejection::UnknownField {
                method: "initialize".to_string(),
                field: "authentication".to_string(),
            })
        );
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
        assert!(policy.meta_keys_for("x.ai/mcp/list").is_empty());
        assert!(policy.meta_keys_for("session/cancel").is_empty());
        assert!(policy.meta_keys_for("session/list").is_empty());
    }

    #[test]
    fn session_new_rejects_session_id() {
        let dir = TempDir::new().unwrap();
        let policy = policy_with(dir.path());
        let params = serde_json::json!({
            "sessionId": "must-not-be-forwarded",
            "cwd": dir.path().to_str().unwrap(),
            "mcpServers": []
        });

        assert_eq!(
            validate_host_request("session/new", &params, &policy),
            Err(HostRejection::UnknownField {
                method: "session/new".into(),
                field: "sessionId".into(),
            })
        );
    }

    #[test]
    fn session_load_requires_non_empty_string_session_id() {
        let dir = TempDir::new().unwrap();
        let policy = policy_with(dir.path());
        for session_id in [
            serde_json::json!(null),
            serde_json::json!(123),
            serde_json::json!({}),
            serde_json::json!(""),
        ] {
            let params = serde_json::json!({
                "sessionId": session_id,
                "cwd": dir.path().to_str().unwrap(),
                "mcpServers": []
            });
            assert_eq!(
                validate_host_request("session/load", &params, &policy),
                Err(HostRejection::InvalidFieldType {
                    method: "session/load".into(),
                    field: "sessionId".into(),
                })
            );
        }
    }

    #[test]
    fn mcp_list_requires_non_empty_string_session_id() {
        let dir = TempDir::new().unwrap();
        let policy = policy_with(dir.path());
        for params in [
            serde_json::json!({}),
            serde_json::json!({ "sessionId": null }),
            serde_json::json!({ "sessionId": 123 }),
            serde_json::json!({ "sessionId": {} }),
            serde_json::json!({ "sessionId": "" }),
        ] {
            let expected = if params.get("sessionId").is_none() {
                HostRejection::MissingRequiredField {
                    method: "x.ai/mcp/list".into(),
                    field: "sessionId".into(),
                }
            } else {
                HostRejection::InvalidFieldType {
                    method: "x.ai/mcp/list".into(),
                    field: "sessionId".into(),
                }
            };
            assert_eq!(
                validate_host_request("x.ai/mcp/list", &params, &policy),
                Err(expected)
            );
        }

        assert!(
            validate_host_request(
                "x.ai/mcp/list",
                &serde_json::json!({ "sessionId": "s1", "_meta": {} }),
                &policy,
            )
            .is_ok()
        );
    }

    #[test]
    fn initialize_rejects_unstable_client_capabilities() {
        let dir = TempDir::new().unwrap();
        let policy = policy_with(dir.path());
        for (field, value) in [
            ("auth", serde_json::json!({ "terminal": false })),
            ("elicitation", serde_json::json!({ "form": {} })),
            ("nes", serde_json::json!({ "jump": {} })),
            ("positionEncodings", serde_json::json!(["utf-16"])),
        ] {
            let mut params = serde_json::json!({
                "protocolVersion": HOST_ACP_PROTOCOL_VERSION,
                "clientCapabilities": {
                    "fs": { "readTextFile": false, "writeTextFile": false },
                    "terminal": false
                },
                "clientInfo": { "name": "efflab-test-client", "version": "0.1.0" }
            });
            params
                .get_mut("clientCapabilities")
                .unwrap()
                .as_object_mut()
                .unwrap()
                .insert(field.to_string(), value);
            assert_eq!(
                validate_host_request("initialize", &params, &policy),
                Err(HostRejection::UnknownNestedField {
                    method: "initialize".into(),
                    field: format!("clientCapabilities.{field}"),
                })
            );
        }

        let nested_meta = serde_json::json!({
            "protocolVersion": HOST_ACP_PROTOCOL_VERSION,
            "clientCapabilities": {
                "fs": {
                    "readTextFile": false,
                    "writeTextFile": false,
                    "_meta": {}
                },
                "terminal": false
            },
            "clientInfo": { "name": "efflab-test-client", "version": "0.1.0" }
        });
        assert_eq!(
            validate_host_request("initialize", &nested_meta, &policy),
            Err(HostRejection::UnknownNestedField {
                method: "initialize".into(),
                field: "clientCapabilities.fs._meta".into(),
            })
        );
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
    fn prompt_text_gate_matches_parser_single_token_cursor() {
        // grok-shell 每次候选后都会越过整个 token；只有一个可选前导 @。
        assert_eq!(
            validate_prompt_text("@@foo"),
            Err(PromptTextRejection::AtFileReference)
        );
        assert!(validate_prompt_text("@@@foo").is_ok());

        // 长连续 @ 必须仍被当作一个不可解析 token，而不是逐个 @ 重新扫描。
        let long_at_token = format!("{}foo", "@".repeat(16_384));
        assert!(validate_prompt_text(&long_at_token).is_ok());
    }

    #[test]
    fn session_request_rejects_non_string_meta_model_id() {
        let dir = TempDir::new().unwrap();
        let p = policy_with(dir.path());
        let params = serde_json::json!({
            "cwd": dir.path().to_str().unwrap(),
            "mcpServers": [],
            "_meta": { "modelId": 123 }
        });

        assert_eq!(
            validate_host_request("session/new", &params, &p),
            Err(HostRejection::InvalidFieldType {
                method: "session/new".into(),
                field: "_meta.modelId".into(),
            })
        );
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
            "protocolVersion": HOST_ACP_PROTOCOL_VERSION,
            "clientCapabilities": {
                "fs": { "readTextFile": false, "writeTextFile": false },
                "terminal": true
            },
            "clientInfo": { "name": "efflab-test-client", "version": "0.1.0" }
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
            "protocolVersion": HOST_ACP_PROTOCOL_VERSION,
            "clientCapabilities": {
                "fs": { "readTextFile": true, "writeTextFile": false },
                "terminal": false
            },
            "clientInfo": { "name": "efflab-test-client", "version": "0.1.0" }
        });
        assert_eq!(
            validate_host_request("initialize", &params, &p),
            Err(HostRejection::FsCapabilityEnabled("initialize".into()))
        );
    }

    #[test]
    fn initialize_rejects_legacy_client_and_capabilities_fields() {
        let dir = TempDir::new().unwrap();
        let p = policy_with(dir.path());
        for (field, value) in [
            ("client", serde_json::json!({ "mcpServers": [] })),
            (
                "capabilities",
                serde_json::json!({ "terminal": false, "fs": false }),
            ),
        ] {
            let mut params = serde_json::json!({
                "protocolVersion": HOST_ACP_PROTOCOL_VERSION,
                "clientCapabilities": {
                    "fs": { "readTextFile": false, "writeTextFile": false },
                    "terminal": false
                },
                "clientInfo": { "name": "efflab-test-client", "version": "0.1.0" }
            });
            params
                .as_object_mut()
                .expect("initialize params 必须为对象")
                .insert(field.to_string(), value);
            assert_eq!(
                validate_host_request("initialize", &params, &p),
                Err(HostRejection::UnknownField {
                    method: "initialize".into(),
                    field: field.into(),
                })
            );
        }
    }

    #[test]
    fn initialize_rejects_model_id_as_unknown_meta_key() {
        let dir = TempDir::new().unwrap();
        let policy = policy_with(dir.path());
        let params = serde_json::json!({
            "protocolVersion": HOST_ACP_PROTOCOL_VERSION,
            "clientCapabilities": {
                "fs": { "readTextFile": false, "writeTextFile": false },
                "terminal": false
            },
            "clientInfo": { "name": "efflab-test-client", "version": "0.1.0" },
            "_meta": { "modelId": "grok-code-fast" }
        });

        assert_eq!(
            validate_host_request("initialize", &params, &policy),
            Err(HostRejection::UnknownMetaKey(
                "initialize".into(),
                "modelId".into(),
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
            "_meta": { "modelId": "byok", "sneaky": 1 }
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
                "clientCapabilities 非对象",
                "initialize",
                serde_json::json!({
                    "protocolVersion": HOST_ACP_PROTOCOL_VERSION,
                    "clientCapabilities": [],
                    "clientInfo": { "name": "efflab-test-client", "version": "0.1.0" }
                }),
                HostRejection::InvalidFieldType {
                    method: "initialize".into(),
                    field: "clientCapabilities".into(),
                },
            ),
            (
                "clientInfo 非对象",
                "initialize",
                serde_json::json!({
                    "protocolVersion": HOST_ACP_PROTOCOL_VERSION,
                    "clientCapabilities": {
                        "fs": { "readTextFile": false, "writeTextFile": false },
                        "terminal": false
                    },
                    "clientInfo": "not-an-object"
                }),
                HostRejection::InvalidFieldType {
                    method: "initialize".into(),
                    field: "clientInfo".into(),
                },
            ),
            (
                "terminal 为字符串 false",
                "initialize",
                serde_json::json!({
                    "protocolVersion": HOST_ACP_PROTOCOL_VERSION,
                    "clientCapabilities": {
                        "fs": { "readTextFile": false, "writeTextFile": false },
                        "terminal": "false"
                    },
                    "clientInfo": { "name": "efflab-test-client", "version": "0.1.0" }
                }),
                HostRejection::InvalidFieldType {
                    method: "initialize".into(),
                    field: "clientCapabilities.terminal".into(),
                },
            ),
            (
                "fs 为数字 0",
                "initialize",
                serde_json::json!({
                    "protocolVersion": HOST_ACP_PROTOCOL_VERSION,
                    "clientCapabilities": {
                        "fs": 0,
                        "terminal": false
                    },
                    "clientInfo": { "name": "efflab-test-client", "version": "0.1.0" }
                }),
                HostRejection::InvalidFieldType {
                    method: "initialize".into(),
                    field: "clientCapabilities.fs".into(),
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
                    "clientCapabilities": {
                        "fs": { "readTextFile": false, "writeTextFile": false },
                        "terminal": false
                    },
                    "clientInfo": { "name": "efflab-test-client", "version": "0.1.0" },
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
                serde_json::json!({ "sessionId": "s1", "_meta": null }),
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
                serde_json::json!({ "sessionId": "s1", "_meta": { "sneaky": 1 } }),
                HostRejection::UnknownMetaKey("x.ai/mcp/list".into(), "sneaky".into()),
            ),
            (
                "mcp/list 顶层 modelId",
                "x.ai/mcp/list",
                serde_json::json!({ "sessionId": "s1", "modelId": "grok-code-fast" }),
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
