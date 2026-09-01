//! Kit L1 产品线协议的机器真源。
//!
//! 本模块只描述与运输无关的 serde 形状。所有字段采用 snake_case；未知 command
//! 和 block 必须被保留为显式降级形状，而不能让整条产品请求或事件解析失败。

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use crate::MentionId;

/// Kit 协议版本；语义不兼容变更必须递增此值。
pub const KIT_SCHEMA_VERSION: u32 = 1;

/// Kit 命令。标准命令采用内标 `cmd`，未知命令显式保存其原始命令名。
#[derive(Clone, PartialEq)]
pub enum KitCommand {
    /// 查询 Kit 能力。
    GetCapability,
    /// 提交用户文本；本任务只实现其进程内幂等边界。
    Send {
        scope_id: String,
        session_id: String,
        submission_id: String,
        text: String,
        mentions: Option<Vec<MentionId>>,
    },
    /// 请求取消当前会话的回合。
    Cancel {
        scope_id: String,
        session_id: String,
    },
    /// 创建新会话。
    NewSession {
        scope_id: String,
        client_request_id: Option<String>,
    },
    /// 列出一个作用域内的会话。
    ListSessions {
        scope_id: String,
        cursor: Option<String>,
    },
    /// 恢复指定会话。
    ResumeSession {
        scope_id: String,
        session_id: String,
    },
    /// 查询不会回显秘密的 Channel view。
    GetLlmChannelView,
    /// 设置产品全局 Channel；秘密只允许存在于该请求内。
    SetLlmChannel {
        kind: Option<LlmChannelKind>,
        base_url: Option<String>,
        model_id: Option<String>,
        relay_base_url: Option<String>,
        app_key: Option<String>,
        api_key: Option<String>,
        access_token: Option<String>,
        client_request_id: Option<String>,
    },
    /// 未知命令的兼容降级形状。
    Unknown { cmd: String },
}

impl fmt::Debug for KitCommand {
    /// 手写调试格式，确保 Set 请求中的一次性秘密不会进入日志。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GetCapability => formatter.write_str("GetCapability"),
            Self::Send {
                scope_id,
                session_id,
                submission_id,
                text,
                mentions,
            } => formatter
                .debug_struct("Send")
                .field("scope_id", scope_id)
                .field("session_id", session_id)
                .field("submission_id", submission_id)
                // prompt 可含用户隐私；Debug 只显示固定脱敏标记和 Unicode 字符数。
                .field("text", &redacted_text_debug(text))
                .field("mentions", mentions)
                .finish(),
            Self::Cancel {
                scope_id,
                session_id,
            } => formatter
                .debug_struct("Cancel")
                .field("scope_id", scope_id)
                .field("session_id", session_id)
                .finish(),
            Self::NewSession {
                scope_id,
                client_request_id,
            } => formatter
                .debug_struct("NewSession")
                .field("scope_id", scope_id)
                .field("client_request_id", client_request_id)
                .finish(),
            Self::ListSessions { scope_id, cursor } => formatter
                .debug_struct("ListSessions")
                .field("scope_id", scope_id)
                .field("cursor", cursor)
                .finish(),
            Self::ResumeSession {
                scope_id,
                session_id,
            } => formatter
                .debug_struct("ResumeSession")
                .field("scope_id", scope_id)
                .field("session_id", session_id)
                .finish(),
            Self::GetLlmChannelView => formatter.write_str("GetLlmChannelView"),
            Self::SetLlmChannel {
                kind,
                base_url,
                model_id,
                relay_base_url,
                app_key,
                api_key,
                access_token,
                client_request_id,
            } => formatter
                .debug_struct("SetLlmChannel")
                .field("kind", kind)
                // URL 在请求校验前也可能含 query 秘密，日志只标记字段存在。
                .field("base_url", &base_url.as_ref().map(|_| "[REDACTED]"))
                .field("model_id", model_id)
                .field(
                    "relay_base_url",
                    &relay_base_url.as_ref().map(|_| "[REDACTED]"),
                )
                .field("app_key", &app_key.as_ref().map(|_| "[REDACTED]"))
                .field("api_key", &api_key.as_ref().map(|_| "[REDACTED]"))
                .field("access_token", &access_token.as_ref().map(|_| "[REDACTED]"))
                .field("client_request_id", client_request_id)
                .finish(),
            Self::Unknown { cmd } => formatter.debug_struct("Unknown").field("cmd", cmd).finish(),
        }
    }
}

/// 生成不含原文的 prompt 调试摘要；长度只按 Unicode 标量计数。
fn redacted_text_debug(text: &str) -> String {
    format!("[REDACTED; len={}]", text.chars().count())
}

impl KitCommand {
    /// 从 Kit JSON 值解码命令，并把未知 `cmd` 转为显式 `Unknown`。
    pub fn from_json_value(value: Value) -> Result<Self, serde_json::Error> {
        let cmd = value
            .get("cmd")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_default();

        match cmd.as_str() {
            "get_capability" => {
                serde_json::from_value::<GetCapabilityCommand>(value).map(|_| Self::GetCapability)
            }
            "send" => serde_json::from_value::<SendCommand>(value).map(Into::into),
            "cancel" => serde_json::from_value::<CancelCommand>(value).map(Into::into),
            "new_session" => serde_json::from_value::<NewSessionCommand>(value).map(Into::into),
            "list_sessions" => serde_json::from_value::<ListSessionsCommand>(value).map(Into::into),
            "resume_session" => {
                serde_json::from_value::<ResumeSessionCommand>(value).map(Into::into)
            }
            "get_llm_channel_view" => serde_json::from_value::<GetLlmChannelViewCommand>(value)
                .map(|_| Self::GetLlmChannelView),
            "set_llm_channel" => {
                serde_json::from_value::<SetLlmChannelCommand>(value).map(Into::into)
            }
            _ => Ok(Self::Unknown { cmd }),
        }
    }
}

impl<'de> Deserialize<'de> for KitCommand {
    /// serde 入口与显式解码保持一致，避免运输 adapter 忘记未知命令降级逻辑。
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        Self::from_json_value(value).map_err(serde::de::Error::custom)
    }
}

impl Serialize for KitCommand {
    /// 使用内标 `cmd` 序列化标准命令；Unknown 保留原始命令名供 dispatch 返回 unsupported。
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::GetCapability => GetCapabilityCommand::default().serialize(serializer),
            Self::Send {
                scope_id,
                session_id,
                submission_id,
                text,
                mentions,
            } => SendCommand {
                cmd: "send".to_string(),
                scope_id: scope_id.clone(),
                session_id: session_id.clone(),
                submission_id: submission_id.clone(),
                text: text.clone(),
                mentions: mentions.clone(),
            }
            .serialize(serializer),
            Self::Cancel {
                scope_id,
                session_id,
            } => CancelCommand {
                cmd: "cancel".to_string(),
                scope_id: scope_id.clone(),
                session_id: session_id.clone(),
            }
            .serialize(serializer),
            Self::NewSession {
                scope_id,
                client_request_id,
            } => NewSessionCommand {
                cmd: "new_session".to_string(),
                scope_id: scope_id.clone(),
                client_request_id: client_request_id.clone(),
            }
            .serialize(serializer),
            Self::ListSessions { scope_id, cursor } => ListSessionsCommand {
                cmd: "list_sessions".to_string(),
                scope_id: scope_id.clone(),
                cursor: cursor.clone(),
            }
            .serialize(serializer),
            Self::ResumeSession {
                scope_id,
                session_id,
            } => ResumeSessionCommand {
                cmd: "resume_session".to_string(),
                scope_id: scope_id.clone(),
                session_id: session_id.clone(),
            }
            .serialize(serializer),
            Self::GetLlmChannelView => GetLlmChannelViewCommand::default().serialize(serializer),
            Self::SetLlmChannel {
                kind,
                base_url,
                model_id,
                relay_base_url,
                app_key,
                api_key,
                access_token,
                client_request_id,
            } => SetLlmChannelCommand {
                cmd: "set_llm_channel".to_string(),
                kind: *kind,
                base_url: base_url.clone(),
                model_id: model_id.clone(),
                relay_base_url: relay_base_url.clone(),
                app_key: app_key.clone(),
                api_key: api_key.clone(),
                access_token: access_token.clone(),
                client_request_id: client_request_id.clone(),
            }
            .serialize(serializer),
            Self::Unknown { cmd } => UnknownCommand { cmd: cmd.clone() }.serialize(serializer),
        }
    }
}

/// 通道类型同时用于 Set 请求与不含秘密的 view。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LlmChannelKind {
    /// 用户自带模型凭据。
    Byok,
    /// Efflab Relay 凭据。
    Relay,
}

/// 不含明文秘密的 Channel 视图。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LlmChannelView {
    /// 当前通道类型；未配置时为 null。
    pub kind: Option<LlmChannelKind>,
    /// Host 是否有已配置的 Byok Key。
    pub key_present: bool,
    /// Host 是否有已配置的 Relay token。
    pub token_present: bool,
    /// 调用方是否可选择模型。
    pub model_selectable: bool,
    /// 非敏感 Byok 端点。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    /// 非敏感模型标识。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_id: Option<String>,
}

/// Capability 的提示词长度限制。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityLimits {
    /// 单次 prompt 最大字符数。
    pub max_prompt_chars: u32,
}

/// Capability reply 的完整产品线协议形状。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capability {
    /// sidecar 当前可用性。
    pub sidecar: String,
    /// sidecar 不可用原因。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// host crate semver。
    pub kit_version: String,
    /// Kit 线协议版本。
    pub schema_version: u32,
    /// 已启用能力名。
    pub features: Vec<String>,
    /// 不含秘密的 Channel view。
    pub channel: LlmChannelView,
    /// 提示词长度限制。
    pub limits: CapabilityLimits,
}

/// 会话摘要的最小产品可见形状。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSummary {
    /// sidecar 会话标识。
    pub session_id: String,
    /// 显示标题。
    pub title: String,
    /// UTC ISO-8601 更新时间。
    pub updated_at: String,
    /// Host 是否持有活跃进程会话。
    pub is_active: bool,
}

/// Kit 同步回复；统一使用内标 `kind`。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum KitReply {
    /// 能力快照。
    Capability(Capability),
    /// Send 的立即受理或幂等命中。
    Send {
        accepted: bool,
        duplicate: bool,
        session_id: String,
        turn_id: String,
        submission_id: String,
    },
    /// Cancel 的同步确认。
    Cancel { accepted: bool },
    /// 创建会话的结果。
    NewSession { session_id: String },
    /// 会话分页结果。
    ListSessions {
        sessions: Vec<SessionSummary>,
        #[serde(skip_serializing_if = "Option::is_none")]
        next_cursor: Option<String>,
    },
    /// 恢复会话的立即受理。
    ResumeSession { accepted: bool, session_id: String },
    /// 不含秘密的 Channel view；外层 tag 固定为 `llm_channel_view`。
    LlmChannelView { channel: LlmChannelView },
}

/// 在线上使用开放字符串错误码的统一错误形状。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KitError {
    /// 开放字符串错误码；未知码必须保留。
    pub code: String,
    /// 客户端可展示的非敏感消息。
    pub message: String,
    /// 可选的非敏感详情。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
    /// 可选关联请求标识。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// 客户端是否可以自动重试。
    pub retryable: bool,
    /// 可选重试建议延迟。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
}

impl KitError {
    /// 构造不可重试的结构化错误，不包含潜在敏感输入。
    pub fn non_retryable(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            details: None,
            request_id: None,
            retryable: false,
            retry_after_ms: None,
        }
    }
}

/// Kit 事件来源。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Origin {
    /// 当前 sidecar 流推送。
    Live,
    /// 会话恢复重放。
    Replay,
}

/// 工具生命周期状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolStatus {
    /// 尚未启动。
    Pending,
    /// 正在运行。
    Running,
    /// 成功完成。
    Completed,
    /// 执行失败。
    Failed,
    /// 已取消。
    Cancelled,
}

/// Kit 事件块；未知 kind 会降级为 `Unknown`。
#[derive(Debug, Clone, PartialEq)]
pub enum KitBlock {
    /// 用户文本。
    User { text: String },
    /// 助手 Markdown 快照。
    Assistant { markdown: String, streaming: bool },
    /// 模型思考文本。
    Thinking { text: String },
    /// 工具生命周期更新。
    Tool {
        tool_call_id: String,
        name: String,
        detail: String,
        status: ToolStatus,
    },
    /// 结构化错误块。
    Error(KitError),
    /// 重试提示。
    Retry { attempt: u32, reason_code: String },
    /// 会话或回合状态。
    Status { code: String, message: String },
    /// 未知类型的安全降级；原 payload 有意丢弃。
    Unknown { unknown_kind: String },
}

impl<'de> Deserialize<'de> for KitBlock {
    /// 解码已知 block；未知 kind 保留名称而非失败丢弃整条事件。
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let kind = value
            .get("kind")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_default();

        match kind.as_str() {
            "user" => serde_json::from_value::<UserBlock>(value).map(Into::into),
            "assistant" => serde_json::from_value::<AssistantBlock>(value).map(Into::into),
            "thinking" => serde_json::from_value::<ThinkingBlock>(value).map(Into::into),
            "tool" => serde_json::from_value::<ToolBlock>(value).map(Into::into),
            "error" => serde_json::from_value::<ErrorBlock>(value).map(Into::into),
            "retry" => serde_json::from_value::<RetryBlock>(value).map(Into::into),
            "status" => serde_json::from_value::<StatusBlock>(value).map(Into::into),
            "unknown" => serde_json::from_value::<UnknownBlock>(value).map(Into::into),
            _ => Ok(Self::Unknown { unknown_kind: kind }),
        }
        .map_err(serde::de::Error::custom)
    }
}

impl Serialize for KitBlock {
    /// 已知 block 使用内标 kind；Unknown 固定重写为安全降级 shape。
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::User { text } => UserBlock {
                kind: "user".to_string(),
                text: text.clone(),
            }
            .serialize(serializer),
            Self::Assistant {
                markdown,
                streaming,
            } => AssistantBlock {
                kind: "assistant".to_string(),
                markdown: markdown.clone(),
                streaming: *streaming,
            }
            .serialize(serializer),
            Self::Thinking { text } => ThinkingBlock {
                kind: "thinking".to_string(),
                text: text.clone(),
            }
            .serialize(serializer),
            Self::Tool {
                tool_call_id,
                name,
                detail,
                status,
            } => ToolBlock {
                kind: "tool".to_string(),
                tool_call_id: tool_call_id.clone(),
                name: name.clone(),
                detail: detail.clone(),
                status: *status,
            }
            .serialize(serializer),
            Self::Error(error) => ErrorBlock {
                kind: "error".to_string(),
                error: error.clone(),
            }
            .serialize(serializer),
            Self::Retry {
                attempt,
                reason_code,
            } => RetryBlock {
                kind: "retry".to_string(),
                attempt: *attempt,
                reason_code: reason_code.clone(),
            }
            .serialize(serializer),
            Self::Status { code, message } => StatusBlock {
                kind: "status".to_string(),
                code: code.clone(),
                message: message.clone(),
            }
            .serialize(serializer),
            Self::Unknown { unknown_kind } => UnknownBlock {
                kind: "unknown".to_string(),
                unknown_kind: unknown_kind.clone(),
            }
            .serialize(serializer),
        }
    }
}

/// 产品可消费的 Kit 事件，不包装 ACP method。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KitProductEvent {
    /// 当前线协议版本。
    pub schema_version: u32,
    /// 产品作用域。
    pub scope_id: String,
    /// sidecar 会话。
    pub session_id: String,
    /// turn 标识；session 级事件允许 null。
    pub turn_id: Option<String>,
    /// submit 标识；session 级事件允许 null。
    pub submission_id: Option<String>,
    /// 去重/合并用事件标识。
    pub event_id: String,
    /// 会话内单调序号。
    pub sequence: u64,
    /// live 或 replay。
    pub origin: Origin,
    /// block 合并标识。
    pub block_id: String,
    /// 产品协议块。
    pub block: KitBlock,
}

impl KitProductEvent {
    /// 在 Host 构造或向产品运输前校验回合与会话事件的标识不变量。
    ///
    /// 入站 serde 故意不调用此方法：未知或被禁用的 sidecar update 必须能够被后续
    /// projector 跳过或计数，而不是因一条异常事件中止整批处理。
    pub fn validate(&self) -> Result<(), KitProductEventValidationError> {
        match &self.block {
            KitBlock::User { .. }
            | KitBlock::Assistant { .. }
            | KitBlock::Thinking { .. }
            | KitBlock::Tool { .. } => self.validate_turn_identifiers(),
            KitBlock::Status { code, .. } if is_turn_terminal_status(code) => {
                self.validate_turn_identifiers()
            }
            KitBlock::Status { code, .. } if is_session_status(code) => {
                self.validate_session_identifiers()
            }
            _ => Ok(()),
        }
    }

    /// turn 级 block 必须携带相同的 turn_id 与 submission_id。
    fn validate_turn_identifiers(&self) -> Result<(), KitProductEventValidationError> {
        match (&self.turn_id, &self.submission_id) {
            (Some(turn_id), Some(submission_id)) if turn_id == submission_id => Ok(()),
            _ => Err(KitProductEventValidationError::TurnIdentifiersMustMatch),
        }
    }

    /// session/process 级 Status 不得伪造任何回合标识。
    fn validate_session_identifiers(&self) -> Result<(), KitProductEventValidationError> {
        if self.turn_id.is_none() && self.submission_id.is_none() {
            Ok(())
        } else {
            Err(KitProductEventValidationError::SessionIdentifiersMustBeNull)
        }
    }
}

/// 事件交给产品前的回合/会话标识不变量错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KitProductEventValidationError {
    /// turn 级 block 缺少标识，或 turn_id 与 submission_id 不相等。
    TurnIdentifiersMustMatch,
    /// session/process 级 Status 携带了不应存在的标识。
    SessionIdentifiersMustBeNull,
}

/// 判断 Status 是否结束一个具体 turn。
fn is_turn_terminal_status(code: &str) -> bool {
    matches!(code, "turn_completed" | "cancelled" | "error")
}

/// 判断 Status 是否属于会话/进程而不关联具体 prompt。
fn is_session_status(code: &str) -> bool {
    matches!(
        code,
        "replay_complete" | "replay_skipped" | "mcp_failed" | "skipped_update"
    )
}

/// 判断事件是否可以进入 Host 的可恢复 transcript；control/fence 与未知诊断永不入表。
///
/// 旧的 `skipped_update` / `replay_skipped` 仍保留为解析兼容的开放 status code，但不在
/// 此白名单中，因此新 Host 不会发送或恢复它们。
pub fn is_recoverable_product_event(event: &KitProductEvent) -> bool {
    match &event.block {
        KitBlock::User { .. }
        | KitBlock::Assistant { .. }
        | KitBlock::Thinking { .. }
        | KitBlock::Tool { .. } => true,
        KitBlock::Status { code, .. } => {
            matches!(code.as_str(), "turn_completed" | "cancelled" | "error")
        }
        KitBlock::Error(_) | KitBlock::Retry { .. } | KitBlock::Unknown { .. } => false,
    }
}

/// `get_capability` 命令的 wire 形状。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct GetCapabilityCommand {
    #[serde(default = "get_capability_cmd")]
    cmd: String,
}

impl Default for GetCapabilityCommand {
    /// 构造固定标签的空命令，避免默认 String 输出空 `cmd`。
    fn default() -> Self {
        Self {
            cmd: get_capability_cmd(),
        }
    }
}

/// `send` 命令的 wire 形状。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SendCommand {
    cmd: String,
    scope_id: String,
    session_id: String,
    submission_id: String,
    text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    mentions: Option<Vec<MentionId>>,
}

/// `cancel` 命令的 wire 形状。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CancelCommand {
    cmd: String,
    scope_id: String,
    session_id: String,
}

/// `new_session` 命令的 wire 形状。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct NewSessionCommand {
    cmd: String,
    scope_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    client_request_id: Option<String>,
}

/// `list_sessions` 命令的 wire 形状。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ListSessionsCommand {
    cmd: String,
    scope_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cursor: Option<String>,
}

/// `resume_session` 命令的 wire 形状。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ResumeSessionCommand {
    cmd: String,
    scope_id: String,
    session_id: String,
}

/// `get_llm_channel_view` 命令的 wire 形状。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct GetLlmChannelViewCommand {
    #[serde(default = "get_llm_channel_view_cmd")]
    cmd: String,
}

impl Default for GetLlmChannelViewCommand {
    /// 构造固定标签的空命令，避免默认 String 输出空 `cmd`。
    fn default() -> Self {
        Self {
            cmd: get_llm_channel_view_cmd(),
        }
    }
}

/// `set_llm_channel` 命令的 wire 形状。
#[derive(Clone, Serialize, Deserialize)]
struct SetLlmChannelCommand {
    cmd: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    kind: Option<LlmChannelKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    base_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    model_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    relay_base_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    app_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    api_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    access_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    client_request_id: Option<String>,
}

impl fmt::Debug for SetLlmChannelCommand {
    /// DTO 也可能被内部错误路径格式化，必须与公开命令一致地脱敏。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SetLlmChannelCommand")
            .field("cmd", &self.cmd)
            .field("kind", &self.kind)
            // serde DTO 可能在 URL 校验前被记录，不能回显其原始地址。
            .field("base_url", &self.base_url.as_ref().map(|_| "[REDACTED]"))
            .field("model_id", &self.model_id)
            .field(
                "relay_base_url",
                &self.relay_base_url.as_ref().map(|_| "[REDACTED]"),
            )
            .field("app_key", &self.app_key.as_ref().map(|_| "[REDACTED]"))
            .field("api_key", &self.api_key.as_ref().map(|_| "[REDACTED]"))
            .field(
                "access_token",
                &self.access_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("client_request_id", &self.client_request_id)
            .finish()
    }
}

/// 未知命令的最小安全 wire 形状。
#[derive(Debug, Clone, Serialize)]
struct UnknownCommand {
    cmd: String,
}

/// 内标 user block。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserBlock {
    kind: String,
    text: String,
}

/// 内标 assistant block。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct AssistantBlock {
    kind: String,
    markdown: String,
    streaming: bool,
}

/// 内标 thinking block。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ThinkingBlock {
    kind: String,
    text: String,
}

/// 内标 tool block。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ToolBlock {
    kind: String,
    tool_call_id: String,
    name: String,
    detail: String,
    status: ToolStatus,
}

/// 内标 error block，错误字段直接平铺在 block 内。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ErrorBlock {
    kind: String,
    #[serde(flatten)]
    error: KitError,
}

/// 内标 retry block。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RetryBlock {
    kind: String,
    attempt: u32,
    reason_code: String,
}

/// 内标 status block。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StatusBlock {
    kind: String,
    code: String,
    message: String,
}

/// 内标 unknown block。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct UnknownBlock {
    kind: String,
    unknown_kind: String,
}

/// `get_capability` 的固定 wire tag。
fn get_capability_cmd() -> String {
    "get_capability".to_string()
}

/// `get_llm_channel_view` 的固定 wire tag。
fn get_llm_channel_view_cmd() -> String {
    "get_llm_channel_view".to_string()
}

/// 将 Send wire DTO 转成公开命令。
impl From<SendCommand> for KitCommand {
    fn from(command: SendCommand) -> Self {
        Self::Send {
            scope_id: command.scope_id,
            session_id: command.session_id,
            submission_id: command.submission_id,
            text: command.text,
            mentions: command.mentions,
        }
    }
}

/// 将 Cancel wire DTO 转成公开命令。
impl From<CancelCommand> for KitCommand {
    fn from(command: CancelCommand) -> Self {
        Self::Cancel {
            scope_id: command.scope_id,
            session_id: command.session_id,
        }
    }
}

/// 将 NewSession wire DTO 转成公开命令。
impl From<NewSessionCommand> for KitCommand {
    fn from(command: NewSessionCommand) -> Self {
        Self::NewSession {
            scope_id: command.scope_id,
            client_request_id: command.client_request_id,
        }
    }
}

/// 将 ListSessions wire DTO 转成公开命令。
impl From<ListSessionsCommand> for KitCommand {
    fn from(command: ListSessionsCommand) -> Self {
        Self::ListSessions {
            scope_id: command.scope_id,
            cursor: command.cursor,
        }
    }
}

/// 将 ResumeSession wire DTO 转成公开命令。
impl From<ResumeSessionCommand> for KitCommand {
    fn from(command: ResumeSessionCommand) -> Self {
        Self::ResumeSession {
            scope_id: command.scope_id,
            session_id: command.session_id,
        }
    }
}

/// 将 SetLlmChannel wire DTO 转成公开命令。
impl From<SetLlmChannelCommand> for KitCommand {
    fn from(command: SetLlmChannelCommand) -> Self {
        Self::SetLlmChannel {
            kind: command.kind,
            base_url: command.base_url,
            model_id: command.model_id,
            relay_base_url: command.relay_base_url,
            app_key: command.app_key,
            api_key: command.api_key,
            access_token: command.access_token,
            client_request_id: command.client_request_id,
        }
    }
}

/// 将 user wire DTO 转成公开 block。
impl From<UserBlock> for KitBlock {
    fn from(block: UserBlock) -> Self {
        Self::User { text: block.text }
    }
}

/// 将 assistant wire DTO 转成公开 block。
impl From<AssistantBlock> for KitBlock {
    fn from(block: AssistantBlock) -> Self {
        Self::Assistant {
            markdown: block.markdown,
            streaming: block.streaming,
        }
    }
}

/// 将 thinking wire DTO 转成公开 block。
impl From<ThinkingBlock> for KitBlock {
    fn from(block: ThinkingBlock) -> Self {
        Self::Thinking { text: block.text }
    }
}

/// 将 tool wire DTO 转成公开 block。
impl From<ToolBlock> for KitBlock {
    fn from(block: ToolBlock) -> Self {
        Self::Tool {
            tool_call_id: block.tool_call_id,
            name: block.name,
            detail: block.detail,
            status: block.status,
        }
    }
}

/// 将 error wire DTO 转成公开 block。
impl From<ErrorBlock> for KitBlock {
    fn from(block: ErrorBlock) -> Self {
        Self::Error(block.error)
    }
}

/// 将 retry wire DTO 转成公开 block。
impl From<RetryBlock> for KitBlock {
    fn from(block: RetryBlock) -> Self {
        Self::Retry {
            attempt: block.attempt,
            reason_code: block.reason_code,
        }
    }
}

/// 将 status wire DTO 转成公开 block。
impl From<StatusBlock> for KitBlock {
    fn from(block: StatusBlock) -> Self {
        Self::Status {
            code: block.code,
            message: block.message,
        }
    }
}

/// 将 unknown wire DTO 转成公开 block。
impl From<UnknownBlock> for KitBlock {
    fn from(block: UnknownBlock) -> Self {
        Self::Unknown {
            unknown_kind: block.unknown_kind,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Send 的 Debug 只能暴露固定长度摘要，不能回显用户 prompt。
    #[test]
    fn send_debug_redacts_prompt_text() {
        let secret = "用户私密 prompt：不要写入日志";
        let command = KitCommand::Send {
            scope_id: "scope-a".to_string(),
            session_id: "session-a".to_string(),
            submission_id: "submission-a".to_string(),
            text: secret.to_string(),
            mentions: None,
        };

        let debug = format!("{command:?}");
        assert!(!debug.contains(secret));
        assert!(debug.contains("[REDACTED; len="));
    }
}
