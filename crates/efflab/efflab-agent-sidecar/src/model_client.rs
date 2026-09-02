//! Host L3b Chat Completions 的最小 HTTP client。
//!
//! 本模块只连接受控 loopback URL，不读取 ACP `_meta.modelId`。binding / Authorization
//! 不得写入日志、session 或 transcript。DEBUG 级别可以输出截断后的请求/响应/SSE 预览，
//! 便于排查合同失败；生产 ACP 错误码仍保持稳定分类。`turn_loop` 负责调用本 client；
//! 每次请求关闭自动重试，取消信号会中止请求头和 SSE 等待。

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

use efflab_agent_contract::{LoopbackModelSpec, RuntimeConfigV1, is_literal_loopback_http_url};
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::sync::Notify;

/// Chat Completions 请求体的最大字节数。
pub const MAX_L3B_REQUEST_BODY_BYTES: usize = 1_048_576;
/// 单条 SSE 物理行的最大字节数。
pub const MAX_SSE_LINE_BYTES: usize = 65_536;
/// SSE 响应累计读取的最大字节数。
pub const MAX_SSE_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

const MODEL_BACKEND: &str = "chat_completions";
const MODEL_TOKEN_ENV: &str = "EFFLAB_L3B_BIND";
const MODEL_BASE_PATH: &str = "/v1";
const MODEL_ENDPOINT_PATH: &str = "/v1/chat/completions";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const TOTAL_TIMEOUT: Duration = Duration::from_secs(60);
const FIRST_BYTE_TIMEOUT: Duration = Duration::from_secs(15);
const FRAME_TIMEOUT: Duration = Duration::from_secs(30);
/// DEBUG 日志正文预览上限，避免把整段 SSE/请求打进 sidecar.log。
pub(crate) const DEBUG_PREVIEW_BYTES: usize = 4096;

/// 按 UTF-8 字节边界截断，供 debug 日志输出内容预览。
pub(crate) fn truncate_for_debug(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_owned();
    }
    let mut end = max_bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…<truncated,total_bytes={}>", &text[..end], text.len())
}

/// 记录合同失败原因与内容预览，再返回稳定的 InvalidResponse。
/// 调用方不得传入 binding / Authorization。
fn model_contract_error(reason: &'static str, detail: &str) -> ModelError {
    tracing::debug!(
        event = "l3b_contract_error",
        reason,
        detail_bytes = detail.len(),
        detail = %truncate_for_debug(detail, DEBUG_PREVIEW_BYTES),
        "L3b 合同校验失败"
    );
    ModelError::InvalidResponse
}

/// 读取响应头里的 Content-Type，缺失时返回空串。
fn content_type_of(response: &reqwest::Response) -> String {
    response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_owned()
}

/// 有界读取响应正文供 debug 预览；失败时返回已读到的内容。
async fn read_body_preview(mut response: reqwest::Response, max_bytes: usize) -> String {
    let mut collected = Vec::new();
    let mut truncated = false;
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) if !chunk.is_empty() => {
                if collected.len() >= max_bytes {
                    truncated = true;
                    break;
                }
                let remaining = max_bytes - collected.len();
                if chunk.len() > remaining {
                    collected.extend_from_slice(&chunk[..remaining]);
                    truncated = true;
                    break;
                }
                collected.extend_from_slice(&chunk);
            }
            Ok(Some(_)) => continue,
            _ => break,
        }
    }
    let preview = String::from_utf8_lossy(&collected);
    if truncated {
        format!(
            "{}…<truncated,read_bytes={}>",
            truncate_for_debug(preview.as_ref(), max_bytes),
            collected.len()
        )
    } else {
        preview.into_owned()
    }
}

/// 一个可被取消的 turn 信号；取消操作幂等且不会携带任何凭据。
#[derive(Clone, Debug, Default)]
pub struct CancellationToken {
    state: Arc<CancellationState>,
}

#[derive(Debug, Default)]
struct CancellationState {
    cancelled: AtomicBool,
    notify: Notify,
}

impl CancellationToken {
    /// 创建尚未取消的 token。
    pub fn new() -> Self {
        Self::default()
    }

    /// 发出一次幂等取消信号，并唤醒正在等待 HTTP chunk 的任务。
    pub fn cancel(&self) {
        if !self.state.cancelled.swap(true, Ordering::AcqRel) {
            self.state.notify.notify_waiters();
            // 额外保留一个 permit，覆盖取消发生在 waiter 注册前的竞态。
            self.state.notify.notify_one();
        }
    }

    /// 检查 token 是否已经取消。
    pub fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::Acquire)
    }

    /// 等待取消；先检查状态以避免 notify 与注册 waiter 之间的竞态。
    pub(crate) async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        let notified = self.state.notify.notified();
        if self.is_cancelled() {
            return;
        }
        notified.await;
    }
}

/// 发送给 Host L3b 的 turn 请求；模型标识不属于该类型，避免 ACP 元数据覆盖配置。
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelTurnRequest {
    /// 已按 Chat Completions role 约束的消息对象。
    pub messages: Vec<Value>,
    /// 已通过 Host 审核的工具定义；无工具时省略该字段。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<Value>>,
    /// 工具选择策略；无工具时省略该字段。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<Value>,
}

impl ModelTurnRequest {
    /// 使用消息数组创建最小 turn 请求。
    pub fn new(messages: Vec<Value>) -> Self {
        Self {
            messages,
            tools: None,
            tool_choice: None,
        }
    }

    /// 设置非空工具定义，供后续 turn loop 传入审核后的 catalog。
    pub fn with_tools(mut self, tools: Vec<Value>) -> Self {
        self.tools = Some(tools);
        self
    }

    /// 设置工具选择策略。
    pub fn with_tool_choice(mut self, tool_choice: Value) -> Self {
        self.tool_choice = Some(tool_choice);
        self
    }

    /// 组装出站 Chat Completions 最小根键；入站解析不使用字段白名单。
    fn to_body(&self, model_id: &str) -> Result<Vec<u8>, ModelError> {
        if self.messages.is_empty() {
            return Err(model_contract_error("empty_messages", ""));
        }
        for message in &self.messages {
            validate_message(message)?;
        }

        let mut object = Map::new();
        object.insert("model".to_owned(), Value::String(model_id.to_owned()));
        object.insert("stream".to_owned(), Value::Bool(true));
        object.insert("messages".to_owned(), Value::Array(self.messages.clone()));

        if let Some(tools) = &self.tools
            && !tools.is_empty()
        {
            object.insert("tools".to_owned(), Value::Array(tools.clone()));
            if let Some(tool_choice) = &self.tool_choice {
                object.insert("tool_choice".to_owned(), tool_choice.clone());
            }
        }

        serde_json::to_vec(&Value::Object(object))
            .map_err(|error| model_contract_error("serialize_request", &error.to_string()))
    }
}

impl fmt::Debug for ModelTurnRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message_bytes = self
            .messages
            .iter()
            .map(|message| message.to_string().len())
            .sum::<usize>();
        formatter
            .debug_struct("ModelTurnRequest")
            .field("message_count", &self.messages.len())
            .field("message_bytes", &message_bytes)
            .field(
                "tool_count",
                &self.tools.as_ref().map(|tools| tools.len()).unwrap_or(0),
            )
            .field("has_tool_choice", &self.tool_choice.is_some())
            .finish()
    }
}

/// 模型返回的一个已归一化 delta。
#[derive(Clone, PartialEq, Eq)]
pub enum ModelDelta {
    /// 文本增量。
    Text(String),
    /// OpenAI 兼容推理字段 `reasoning_content` 的增量；只展示，不进入模型续写。
    Thought(String),
    /// 在 `[DONE]` 前按 index 聚合完成的工具调用。
    ToolCall(ModelToolCall),
    /// 上游正常结束。
    Done,
}

/// 聚合后的 Chat Completions function tool call。
#[derive(Clone, PartialEq, Eq)]
pub struct ModelToolCall {
    /// 上游 tool call index。
    pub index: u32,
    /// 上游调用 id。
    pub id: Option<String>,
    /// function 名称。
    pub name: Option<String>,
    /// 已拼接的 JSON arguments 字符串。
    pub arguments: String,
}

impl fmt::Debug for ModelDelta {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Text(text) => formatter
                .debug_struct("ModelDelta::Text")
                .field("length", &text.len())
                .finish(),
            Self::Thought(text) => formatter
                .debug_struct("ModelDelta::Thought")
                .field("length", &text.len())
                .finish(),
            Self::ToolCall(call) => formatter
                .debug_struct("ModelDelta::ToolCall")
                .field("index", &call.index)
                .field("id_present", &call.id.is_some())
                .field("name_present", &call.name.is_some())
                .field("arguments_length", &call.arguments.len())
                .finish(),
            Self::Done => formatter.write_str("ModelDelta::Done"),
        }
    }
}

impl fmt::Debug for ModelToolCall {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelToolCall")
            .field("index", &self.index)
            .field("id_present", &self.id.is_some())
            .field("name_present", &self.name.is_some())
            .field("arguments_length", &self.arguments.len())
            .finish()
    }
}

/// L3b HTTP/SSE client 的稳定错误分类；错误文本不包含响应正文或 binding。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelError {
    /// 响应头、SSE JSON 或 delta 结构不符合合同。
    InvalidResponse,
    /// HTTP 或传输失败；状态为 0 表示没有可用 HTTP 状态码。
    Http { status: u16 },
    /// 调用方取消了当前 turn。
    Cancelled,
    /// 请求体、单行或累计响应超过安全上限。
    ResponseTooLarge,
}

impl fmt::Display for ModelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidResponse => formatter.write_str("invalid model response"),
            Self::Http { status: 0 } => formatter.write_str("model HTTP transport failed"),
            Self::Http { status } => write!(formatter, "model HTTP status {status}"),
            Self::Cancelled => formatter.write_str("model turn cancelled"),
            Self::ResponseTooLarge => formatter.write_str("model response too large"),
        }
    }
}

impl std::error::Error for ModelError {}

/// 面向 Host L3b 的 Chat Completions client。
pub struct HttpModelClient {
    client: Option<reqwest::Client>,
    endpoint: Option<reqwest::Url>,
    model_id: String,
    binding: BindingSource,
    construction_error: Option<ModelError>,
}

#[derive(Clone)]
enum BindingSource {
    /// 生产路径只保存固定环境变量名，真正读取发生在请求构造点。
    Environment(String),
    /// 测试路径使用固定 binding，避免修改进程全局环境。
    Fixed(String),
}

impl HttpModelClient {
    /// 从 `RuntimeConfigV1.model` 构造 client，不接受 ACP 元数据中的模型覆盖。
    pub fn from_runtime_config(config: &RuntimeConfigV1) -> Result<Self, ModelError> {
        Self::from_model(&config.model)
    }

    /// 从已校验的 loopback 模型规格构造生产 client；生产调用只能经由 runtime config。
    fn from_model(model: &LoopbackModelSpec) -> Result<Self, ModelError> {
        Self::with_binding(model, BindingSource::Environment(model.token_env.clone()))
    }

    /// 为测试构造固定 `byok-user-model` 的 client；生产代码不得使用此入口。
    #[doc(hidden)]
    pub fn for_test(base_url: impl AsRef<str>, binding: impl Into<String>) -> Self {
        let model = test_model_spec(base_url.as_ref());
        let binding = BindingSource::Fixed(binding.into());
        match Self::with_binding(&model, binding.clone()) {
            Ok(client) => client,
            Err(error) => Self {
                client: build_http_client().ok(),
                endpoint: None,
                model_id: model.model_id,
                binding,
                construction_error: Some(error),
            },
        }
    }

    /// 为需要显式检查 URL 合同的测试提供可失败构造函数。
    #[doc(hidden)]
    pub fn try_for_test(
        base_url: impl AsRef<str>,
        binding: impl Into<String>,
    ) -> Result<Self, ModelError> {
        let model = test_model_spec(base_url.as_ref());
        Self::with_binding(&model, BindingSource::Fixed(binding.into()))
    }

    /// 发起一次不可重试的流式 turn；返回值只拥有该 turn 的 response body。
    pub async fn stream_turn(
        &self,
        request: ModelTurnRequest,
        cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        if cancellation.is_cancelled() {
            return Err(ModelError::Cancelled);
        }
        if let Some(error) = &self.construction_error {
            return Err(error.clone());
        }

        // 先物化并限制正文，确保超限请求在任何 HTTP 调用前失败。
        let body = request.to_body(&self.model_id)?;
        if body.len() > MAX_L3B_REQUEST_BODY_BYTES {
            tracing::debug!("拒绝超出上限的 L3b 模型请求正文");
            return Err(ModelError::ResponseTooLarge);
        }

        // binding 只在请求构造点借用；不写入 client 日志、session 或 transcript。
        let authorization = self.authorization_header()?;
        let client = self.client.as_ref().ok_or_else(|| {
            tracing::debug!(event = "l3b_http_client_missing", "L3b HTTP client 未构造");
            ModelError::Http { status: 0 }
        })?;
        let endpoint = self
            .endpoint
            .as_ref()
            .ok_or_else(|| model_contract_error("missing_endpoint", ""))?;
        tracing::debug!(
            event = "l3b_request_sent",
            model_id = %self.model_id,
            endpoint = %endpoint,
            body_bytes = body.len(),
            message_count = request.messages.len(),
            tool_count = request.tools.as_ref().map(Vec::len).unwrap_or(0),
            body = %truncate_for_debug(&String::from_utf8_lossy(&body), DEBUG_PREVIEW_BYTES),
            "发送 L3b Chat Completions 请求"
        );
        let send_future = client
            .post(endpoint.clone())
            .header(AUTHORIZATION, authorization)
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "text/event-stream")
            .body(body)
            .send();
        tokio::pin!(send_future);
        let response_result = tokio::select! {
            result = &mut send_future => result,
            _ = cancellation.cancelled() => {
                tracing::debug!("响应头到达前取消 L3b 模型请求");
                return Err(ModelError::Cancelled);
            }
        };
        let response = match response_result {
            Ok(response) => response,
            Err(_) if cancellation.is_cancelled() => return Err(ModelError::Cancelled),
            Err(error) => {
                tracing::debug!(
                    event = "l3b_http_transport_failed",
                    error = %error,
                    "L3b HTTP 传输失败"
                );
                return Err(ModelError::Http { status: 0 });
            }
        };

        let status = response.status().as_u16();
        let content_type = content_type_of(&response);
        tracing::debug!(
            event = "l3b_response_headers",
            status,
            content_type = %content_type,
            content_length = ?response.content_length(),
            "收到 L3b Chat Completions 响应"
        );
        if cancellation.is_cancelled() {
            drop(response);
            return Err(ModelError::Cancelled);
        }
        if !response.status().is_success() {
            let preview = read_body_preview(response, DEBUG_PREVIEW_BYTES).await;
            tracing::debug!(
                event = "l3b_http_error_body",
                status,
                content_type = %content_type,
                body = %preview,
                "L3b HTTP 错误响应正文"
            );
            return Err(ModelError::Http { status });
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_SSE_RESPONSE_BYTES as u64)
        {
            tracing::debug!(
                event = "l3b_response_too_large",
                content_length = ?response.content_length(),
                "拒绝已知长度超出上限的 L3b 模型响应"
            );
            drop(response);
            return Err(ModelError::ResponseTooLarge);
        }
        if !is_event_stream(response.headers()) {
            let preview = read_body_preview(response, DEBUG_PREVIEW_BYTES).await;
            return Err(model_contract_error(
                "not_event_stream",
                &format!("content_type={content_type}; body={preview}"),
            ));
        }

        Ok(ModelStream::new(response, cancellation))
    }

    /// 在请求构造点读取生产 binding 并转换为 Authorization header。
    fn authorization_header(&self) -> Result<HeaderValue, ModelError> {
        let binding = match &self.binding {
            BindingSource::Environment(name) => std::env::var(name)
                .map_err(|_| model_contract_error("missing_binding_env", name))?,
            BindingSource::Fixed(value) => value.clone(),
        };
        if binding.is_empty() {
            return Err(model_contract_error("empty_binding", ""));
        }
        let value = format!("Bearer {binding}");
        let mut header = HeaderValue::from_str(&value)
            .map_err(|_| model_contract_error("invalid_binding_header", ""))?;
        header.set_sensitive(true);
        Ok(header)
    }

    /// 构造固定 loopback client 与 endpoint。
    fn with_binding(model: &LoopbackModelSpec, binding: BindingSource) -> Result<Self, ModelError> {
        let endpoint = endpoint_for_model(model)?;
        let client = build_http_client()?;
        tracing::debug!(
            model_id_length = model.model_id.len(),
            "构造 L3b 模型 client"
        );
        Ok(Self {
            client: Some(client),
            endpoint: Some(endpoint),
            model_id: model.model_id.clone(),
            binding,
            construction_error: None,
        })
    }
}

/// 将 v1 base URL 变成固定 `/v1/chat/completions` endpoint。
fn endpoint_for_model(model: &LoopbackModelSpec) -> Result<reqwest::Url, ModelError> {
    if model.backend != MODEL_BACKEND {
        return Err(model_contract_error("invalid_backend", &model.backend));
    }
    if model.token_env != MODEL_TOKEN_ENV {
        return Err(model_contract_error("invalid_token_env", &model.token_env));
    }
    if model.model_id.is_empty() {
        return Err(model_contract_error("empty_model_id", ""));
    }
    if !model
        .model_id
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
    {
        return Err(model_contract_error(
            "invalid_model_id_charset",
            &model.model_id,
        ));
    }
    if model.model_id.chars().count() > 128 {
        return Err(model_contract_error(
            "model_id_too_long",
            &format!("len={}", model.model_id.chars().count()),
        ));
    }
    if !is_literal_loopback_http_url(&model.base_url) {
        return Err(model_contract_error(
            "base_url_not_loopback",
            &model.base_url,
        ));
    }

    let mut base = reqwest::Url::parse(&model.base_url)
        .map_err(|error| model_contract_error("base_url_parse", &error.to_string()))?;
    if base.path() != MODEL_BASE_PATH || base.query().is_some() || base.fragment().is_some() {
        return Err(model_contract_error(
            "invalid_base_path",
            &format!(
                "path={}; query={:?}; fragment={:?}",
                base.path(),
                base.query(),
                base.fragment()
            ),
        ));
    }
    base.set_path(MODEL_ENDPOINT_PATH);
    Ok(base)
}

/// 测试构造使用与 RuntimeConfig fixture 相同的模型标识。
fn test_model_spec(base_url: &str) -> LoopbackModelSpec {
    LoopbackModelSpec {
        model_id: "byok-user-model".to_owned(),
        base_url: base_url.to_owned(),
        backend: MODEL_BACKEND.to_owned(),
        token_env: MODEL_TOKEN_ENV.to_owned(),
    }
}

/// 构造关闭代理、重定向和自动重试的 reqwest client。
fn build_http_client() -> Result<reqwest::Client, ModelError> {
    reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(TOTAL_TIMEOUT)
        .build()
        .map_err(|error| {
            tracing::debug!(
                event = "l3b_http_client_build_failed",
                error = %error,
                "构造 L3b HTTP client 失败"
            );
            ModelError::Http { status: 0 }
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Authorization header 必须由 reqwest 标记为敏感值，避免诊断格式泄漏 binding。
    #[test]
    fn authorization_header_is_sensitive() {
        let model = test_model_spec("http://127.0.0.1:4312/v1");
        let client = HttpModelClient::with_binding(
            &model,
            BindingSource::Fixed("unit-test-binding".to_owned()),
        )
        .expect("测试模型 client 应构造成功");
        let header = client
            .authorization_header()
            .expect("测试 binding 应构造 Authorization");
        assert!(header.is_sensitive());
    }

    #[test]
    fn truncate_for_debug_keeps_short_text() {
        assert_eq!(truncate_for_debug("hello", 16), "hello");
    }

    #[test]
    fn truncate_for_debug_cuts_on_char_boundary() {
        let preview = truncate_for_debug("你好世界", 4);
        assert!(preview.contains("truncated,total_bytes=12"), "{preview}");
        assert!(preview.starts_with("你"), "{preview}");
    }
}

/// 只接受 event-stream media type，允许标准参数但拒绝其他媒体类型。
fn is_event_stream(headers: &reqwest::header::HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("text/event-stream"))
}

/// 校验请求消息的 role，避免把任意 ACP 结构直接转发给上游。
fn validate_message(message: &Value) -> Result<(), ModelError> {
    let object = message
        .as_object()
        .ok_or_else(|| model_contract_error("message_not_object", &message.to_string()))?;
    let role = object
        .get("role")
        .and_then(Value::as_str)
        .ok_or_else(|| model_contract_error("message_missing_role", &message.to_string()))?;
    if matches!(role, "system" | "user" | "assistant" | "tool") {
        Ok(())
    } else {
        Err(model_contract_error(
            "invalid_message_role",
            &format!("role={role}; message={message}"),
        ))
    }
}

/// SSE `choices[].delta` 中 sidecar 会消费的语义字段；其它键忽略。
const CONSUMED_DELTA_KEYS: &[&str] = &[
    "role",
    "content",
    "tool_calls",
    "reasoning_content",
    "reasoning",
    "refusal",
];

/// 一个 cancel-safe、单向消费的 SSE response stream。
pub struct ModelStream {
    response: Option<reqwest::Response>,
    cancellation: CancellationToken,
    line_buffer: Vec<u8>,
    frame_data: Vec<u8>,
    frame_has_data: bool,
    queued: VecDeque<ModelDelta>,
    tool_calls: BTreeMap<u32, ToolCallAccumulator>,
    done_seen: bool,
    response_bytes: usize,
    first_chunk_seen: bool,
    deadline: Instant,
    terminal_error: Option<ModelError>,
    terminal_error_reported: bool,
    cancel_reported: bool,
}

impl fmt::Debug for ModelStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelStream")
            .field("has_response", &self.response.is_some())
            .field("line_buffer_length", &self.line_buffer.len())
            .field("frame_data_length", &self.frame_data.len())
            .field("queued_count", &self.queued.len())
            .field("tool_call_count", &self.tool_calls.len())
            .field("response_bytes", &self.response_bytes)
            .field("done_seen", &self.done_seen)
            .field("first_chunk_seen", &self.first_chunk_seen)
            .field("terminal_error", &self.terminal_error)
            .field("cancel_reported", &self.cancel_reported)
            .finish()
    }
}

#[derive(Debug, Default)]
struct ToolCallAccumulator {
    id: Option<String>,
    name: Option<String>,
    arguments: String,
}

#[derive(Debug)]
struct ParsedToolCall {
    index: u32,
    id: Option<String>,
    name: Option<String>,
    arguments: String,
}

impl ModelStream {
    /// 保存 response 所有权，确保 stream drop/cancel 会释放底层连接。
    fn new(response: reqwest::Response, cancellation: CancellationToken) -> Self {
        Self {
            response: Some(response),
            cancellation,
            line_buffer: Vec::new(),
            frame_data: Vec::new(),
            frame_has_data: false,
            queued: VecDeque::new(),
            tool_calls: BTreeMap::new(),
            done_seen: false,
            response_bytes: 0,
            first_chunk_seen: false,
            deadline: Instant::now() + TOTAL_TIMEOUT,
            terminal_error: None,
            terminal_error_reported: false,
            cancel_reported: false,
        }
    }

    /// 读取下一个归一化 delta；取消或解析错误均只报告一次。
    pub async fn recv(&mut self) -> Result<Option<ModelDelta>, ModelError> {
        loop {
            if let Some(error) = self.terminal_error.clone() {
                if !self.terminal_error_reported {
                    self.terminal_error_reported = true;
                    return Err(error);
                }
                return Ok(None);
            }
            // 已解析的 delta（含 [DONE]）必须先交付，避免 cancel 丢弃已经到达的 completion。
            if let Some(delta) = self.queued.pop_front() {
                if self.done_seen {
                    self.response.take();
                }
                return Ok(Some(delta));
            }
            if self.done_seen {
                self.response.take();
                return Ok(None);
            }
            if self.cancellation.is_cancelled() {
                return self.cancel_once();
            }

            let chunk = match self.next_response_chunk().await {
                Ok(chunk) => chunk,
                Err(error) => return self.fail(error),
            };
            let Some(chunk) = chunk else {
                tracing::debug!(
                    event = "l3b_sse_eof_without_done",
                    done_seen = self.done_seen,
                    response_bytes = self.response_bytes,
                    line_buffer = %truncate_for_debug(
                        &String::from_utf8_lossy(&self.line_buffer),
                        DEBUG_PREVIEW_BYTES
                    ),
                    frame_data = %truncate_for_debug(
                        &String::from_utf8_lossy(&self.frame_data),
                        DEBUG_PREVIEW_BYTES
                    ),
                    "L3b SSE 在 [DONE] 前结束"
                );
                return self.fail(ModelError::Http { status: 0 });
            };
            self.first_chunk_seen = true;
            if let Err(error) = self.push_response_bytes(&chunk) {
                return self.fail(error);
            }
            // 刚读到的帧先入队；下一轮循环优先交付 queued Done，再处理 cancel。
        }
    }

    /// 以 cancel token 与阶段超时竞争读取下一个网络 chunk。
    async fn next_response_chunk(&mut self) -> Result<Option<Vec<u8>>, ModelError> {
        let Some(response) = self.response.as_mut() else {
            return Ok(None);
        };
        let phase_timeout = if self.first_chunk_seen {
            FRAME_TIMEOUT
        } else {
            FIRST_BYTE_TIMEOUT
        };
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(ModelError::Http { status: 0 });
        }
        let wait = phase_timeout.min(remaining);
        let chunk_future = response.chunk();
        tokio::pin!(chunk_future);
        let cancel = self.cancellation.clone();
        let timer = tokio::time::sleep(wait);
        tokio::pin!(timer);

        tokio::select! {
            biased;
            result = &mut chunk_future => match result {
                Ok(chunk) => Ok(chunk.map(|bytes| bytes.to_vec())),
                Err(error) => {
                    tracing::debug!(
                        event = "l3b_sse_chunk_failed",
                        error = %error,
                        first_chunk_seen = self.first_chunk_seen,
                        response_bytes = self.response_bytes,
                        "L3b SSE chunk 读取失败"
                    );
                    Err(ModelError::Http { status: 0 })
                }
            },
            _ = cancel.cancelled() => Err(ModelError::Cancelled),
            _ = &mut timer => {
                tracing::debug!(
                    event = "l3b_sse_timeout",
                    first_chunk_seen = self.first_chunk_seen,
                    response_bytes = self.response_bytes,
                    "L3b SSE 读取超时"
                );
                Err(ModelError::Http { status: 0 })
            }
        }
    }

    /// 累计读取并拆分完整物理行。
    fn push_response_bytes(&mut self, bytes: &[u8]) -> Result<(), ModelError> {
        if bytes.len() > MAX_SSE_RESPONSE_BYTES.saturating_sub(self.response_bytes) {
            return Err(ModelError::ResponseTooLarge);
        }
        self.response_bytes += bytes.len();
        self.line_buffer.extend_from_slice(bytes);

        while let Some(newline) = self.line_buffer.iter().position(|byte| *byte == b'\n') {
            let mut line = self.line_buffer.drain(..=newline).collect::<Vec<_>>();
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            if line.len() > MAX_SSE_LINE_BYTES {
                return Err(ModelError::ResponseTooLarge);
            }
            self.process_line(&line)?;
        }

        if self.line_buffer.len() > MAX_SSE_LINE_BYTES + 2 {
            return Err(ModelError::ResponseTooLarge);
        }
        Ok(())
    }

    /// 只接收 `data:` 行，其他 SSE 元字段保持忽略。
    fn process_line(&mut self, line: &[u8]) -> Result<(), ModelError> {
        if line.is_empty() {
            return self.finish_frame();
        }
        if line.starts_with(b"data:") {
            let mut data = &line[5..];
            if data.first() == Some(&b' ') {
                data = &data[1..];
            }
            if self.frame_has_data {
                self.frame_data.push(b'\n');
            }
            self.frame_data.extend_from_slice(data);
            self.frame_has_data = true;
        }
        Ok(())
    }

    /// 在空行处解析当前 SSE event frame。
    fn finish_frame(&mut self) -> Result<(), ModelError> {
        if !self.frame_has_data {
            return Ok(());
        }
        let data = std::mem::take(&mut self.frame_data);
        self.frame_has_data = false;
        if data.is_empty() {
            return Ok(());
        }

        let data = match String::from_utf8(data) {
            Ok(data) => data,
            Err(error) => {
                return Err(model_contract_error(
                    "sse_frame_not_utf8",
                    &String::from_utf8_lossy(error.as_bytes()),
                ));
            }
        };
        tracing::debug!(
            event = "l3b_sse_frame",
            frame_bytes = data.len(),
            done = data == "[DONE]",
            frame = %truncate_for_debug(&data, DEBUG_PREVIEW_BYTES),
            "收到 L3b SSE frame"
        );
        if data == "[DONE]" {
            if self.done_seen {
                return Err(model_contract_error("duplicate_done", &data));
            }
            let tool_calls = std::mem::take(&mut self.tool_calls);
            // [DONE] 前必须已经得到完整身份，避免把不确定调用交给执行层。
            if tool_calls.values().any(|call| {
                !call.id.as_deref().is_some_and(|id| !id.is_empty())
                    || !call.name.as_deref().is_some_and(|name| !name.is_empty())
            }) {
                return Err(model_contract_error(
                    "incomplete_tool_call_at_done",
                    &format!("{tool_calls:?}"),
                ));
            }
            self.done_seen = true;
            for (index, call) in tool_calls {
                self.queued.push_back(ModelDelta::ToolCall(ModelToolCall {
                    index,
                    id: call.id,
                    name: call.name,
                    arguments: call.arguments,
                }));
            }
            self.queued.push_back(ModelDelta::Done);
            return Ok(());
        }
        if self.done_seen {
            return Err(model_contract_error("data_after_done", &data));
        }
        self.parse_delta(&data)
    }

    /// 将一个 Chat Completions JSON frame 转换为文本、推理或聚合工具调用。
    /// 入站按 OpenAI 兼容协议解析：消费已知语义字段，扩展键忽略，不因白名单中断 stream。
    fn parse_delta(&mut self, data: &str) -> Result<(), ModelError> {
        let payload = serde_json::from_str::<Value>(data).map_err(|error| {
            model_contract_error("sse_json_parse", &format!("error={error}; frame={data}"))
        })?;
        let choices = match payload.get("choices") {
            None | Some(Value::Null) => {
                tracing::debug!(
                    event = "l3b_usage_or_empty_choices",
                    has_usage = payload.get("usage").is_some(),
                    "忽略无 choices 的 Chat Completions chunk"
                );
                return Ok(());
            }
            Some(Value::Array(choices)) => choices,
            Some(other) => {
                return Err(model_contract_error(
                    "choices_not_array",
                    &format!("choices={other}; frame={data}"),
                ));
            }
        };
        if choices.is_empty() {
            // OpenAI stream_options.include_usage：[DONE] 前会有空 choices + usage 尾帧。
            tracing::debug!(
                event = "l3b_usage_or_empty_choices",
                has_usage = payload.get("usage").is_some(),
                "忽略空 choices 的 Chat Completions 尾帧"
            );
            return Ok(());
        }

        for choice in choices {
            let choice = choice
                .as_object()
                .ok_or_else(|| model_contract_error("choice_not_object", data))?;
            let terminal_finish_reason =
                finish_reason_is_terminal(choice.get("finish_reason"), data)?;
            let delta = match choice.get("delta") {
                None | Some(Value::Null) if terminal_finish_reason => continue,
                Some(Value::Object(delta)) => delta,
                other => {
                    return Err(model_contract_error(
                        "missing_delta",
                        &format!("delta={other:?}; frame={data}"),
                    ));
                }
            };
            let ignored_keys: Vec<&str> = delta
                .keys()
                .map(String::as_str)
                .filter(|key| !CONSUMED_DELTA_KEYS.contains(key))
                .collect();
            if !ignored_keys.is_empty() {
                tracing::debug!(
                    event = "l3b_delta_ignored_keys",
                    keys = ?ignored_keys,
                    "忽略 Chat Completions 扩展 delta 键"
                );
            }

            if let Some(role) = delta.get("role")
                && !role.is_null()
                && role.as_str().is_none()
            {
                return Err(model_contract_error(
                    "invalid_delta_role",
                    &format!("role={role}; frame={data}"),
                ));
            }
            // 先排队推理，再排队正文，UI 先展示 thinking。
            if let Some(thought) = first_delta_text(
                delta
                    .get("reasoning_content")
                    .or_else(|| delta.get("reasoning")),
                "reasoning_content",
                data,
            )? {
                self.queued.push_back(ModelDelta::Thought(thought));
            }
            if let Some(text) = first_delta_text(delta.get("content"), "content", data)? {
                self.queued.push_back(ModelDelta::Text(text));
            }
            if let Some(text) = first_delta_text(delta.get("refusal"), "refusal", data)? {
                self.queued.push_back(ModelDelta::Text(text));
            }
            if let Some(tool_calls) = delta.get("tool_calls") {
                match tool_calls {
                    Value::Null => {}
                    Value::Array(tool_calls) => {
                        for tool_call in tool_calls {
                            if let Some(part) = parse_tool_call(tool_call)? {
                                self.merge_tool_call(part)?;
                            }
                        }
                    }
                    other => {
                        return Err(model_contract_error(
                            "tool_calls_not_array",
                            &format!("tool_calls={other}; frame={data}"),
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// 合并同一 index 的 id、name 和 arguments，并拒绝冲突标识。
    fn merge_tool_call(&mut self, part: ParsedToolCall) -> Result<(), ModelError> {
        let call = self.tool_calls.entry(part.index).or_default();
        if let Some(id) = part.id {
            if call.id.as_ref().is_some_and(|old| old != &id) {
                return Err(model_contract_error(
                    "conflicting_tool_call_id",
                    &format!("index={}; old={:?}; new={id}", part.index, call.id),
                ));
            }
            call.id = Some(id);
        }
        if let Some(name) = part.name {
            if call.name.as_ref().is_some_and(|old| old != &name) {
                return Err(model_contract_error(
                    "conflicting_tool_call_name",
                    &format!("index={}; old={:?}; new={name}", part.index, call.name),
                ));
            }
            call.name = Some(name);
        }
        call.arguments.push_str(&part.arguments);
        Ok(())
    }

    /// 只返回一次取消错误，并立即释放 response 所有权。
    fn cancel_once(&mut self) -> Result<Option<ModelDelta>, ModelError> {
        self.response.take();
        if self.cancel_reported {
            return Ok(None);
        }
        self.cancel_reported = true;
        Err(ModelError::Cancelled)
    }

    /// 记录终端错误并立即丢弃 response，阻断迟到 chunk。
    fn fail(&mut self, error: ModelError) -> Result<Option<ModelDelta>, ModelError> {
        self.response.take();
        if self.terminal_error.is_none() {
            self.terminal_error = Some(error.clone());
        }
        if self.terminal_error_reported {
            return Ok(None);
        }
        self.terminal_error_reported = true;
        Err(error)
    }
}

/// 任意字符串 finish_reason 都只是终止提示；真正完成仍由 [DONE] 产生。
fn finish_reason_is_terminal(value: Option<&Value>, data: &str) -> Result<bool, ModelError> {
    match value {
        None | Some(Value::Null) => Ok(false),
        Some(Value::String(_)) => Ok(true),
        Some(reason) => Err(model_contract_error(
            "invalid_finish_reason",
            &format!("finish_reason={reason}; frame={data}"),
        )),
    }
}

/// 解析可选文本增量：null/空串忽略；字符串或文本数组取出非空正文。
fn first_delta_text(
    value: Option<&Value>,
    field: &'static str,
    data: &str,
) -> Result<Option<String>, ModelError> {
    let Some(value) = value else {
        return Ok(None);
    };
    match value {
        Value::Null => Ok(None),
        Value::String(text) if text.is_empty() => Ok(None),
        Value::String(text) => Ok(Some(text.clone())),
        Value::Array(parts) => {
            let mut text = String::new();
            for part in parts {
                match part {
                    Value::String(piece) => text.push_str(piece),
                    Value::Object(object) => {
                        if let Some(Value::String(piece)) = object.get("text") {
                            text.push_str(piece);
                        }
                    }
                    _ => {}
                }
            }
            if text.is_empty() {
                Ok(None)
            } else {
                Ok(Some(text))
            }
        }
        other => Err(model_contract_error(
            "invalid_delta_text",
            &format!("{field}={other}; frame={data}"),
        )),
    }
}

/// 解析一个工具调用片段；非 function 类型忽略，扩展键忽略。
fn parse_tool_call(value: &Value) -> Result<Option<ParsedToolCall>, ModelError> {
    let detail = value.to_string();
    let object = value
        .as_object()
        .ok_or_else(|| model_contract_error("tool_call_not_object", &detail))?;
    if let Some(kind) = object.get("type")
        && !kind.is_null()
        && kind.as_str() != Some("function")
    {
        tracing::debug!(
            event = "l3b_tool_call_ignored",
            tool_type = %kind,
            "忽略非 function 的 Chat Completions tool_call；执行层只跑 App 审核工具"
        );
        return Ok(None);
    }
    let index = match object.get("index") {
        Some(Value::Number(number)) => number
            .as_u64()
            .and_then(|value| u32::try_from(value).ok())
            .ok_or_else(|| model_contract_error("invalid_tool_call_index", &detail))?,
        None | Some(_) => {
            return Err(model_contract_error("missing_tool_call_index", &detail));
        }
    };
    let function = match object.get("function") {
        None | Some(Value::Null) => {
            return Ok(Some(ParsedToolCall {
                index,
                id: parse_optional_id(object.get("id"), &detail)?,
                name: None,
                arguments: String::new(),
            }));
        }
        Some(Value::Object(function)) => function,
        Some(other) => {
            return Err(model_contract_error(
                "missing_function",
                &format!("function={other}; tool_call={detail}"),
            ));
        }
    };
    let id = parse_optional_id(object.get("id"), &detail)?;
    let name = match function.get("name") {
        None | Some(Value::Null) => None,
        Some(Value::String(name)) => Some(name.clone()),
        Some(name) => {
            return Err(model_contract_error(
                "invalid_tool_call_name",
                &format!("name={name}; tool_call={detail}"),
            ));
        }
    };
    let arguments = match function.get("arguments") {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(arguments)) => arguments.clone(),
        Some(arguments) => {
            return Err(model_contract_error(
                "invalid_tool_call_arguments",
                &format!("arguments={arguments}; tool_call={detail}"),
            ));
        }
    };
    Ok(Some(ParsedToolCall {
        index,
        id,
        name,
        arguments,
    }))
}

/// 解析可选 tool call id。
fn parse_optional_id(value: Option<&Value>, detail: &str) -> Result<Option<String>, ModelError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(id)) => Ok(Some(id.clone())),
        Some(id) => Err(model_contract_error(
            "invalid_tool_call_id",
            &format!("id={id}; tool_call={detail}"),
        )),
    }
}
