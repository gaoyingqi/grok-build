//! sidecar 本地最小 HTTP MCP client。
//!
//! 本模块只消费 Host 已审核的 loopback HTTP MCP server。每个 server 对应一个受控的
//! streamable HTTP 会话；stdio、认证、代理、重定向和模型 endpoint 均不属于这里。
//! HTTP 响应在 serde 解码前经过 Content-Length 与分块累计限制，避免 rmcp 默认
//! `response.json()` 在完整物化后才发现超限。

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::future::Future;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use bytes::Bytes;
use efflab_agent_contract::{
    ApprovedMcpConfig, McpServerSpec,
    is_literal_loopback_http_url as contract_is_literal_loopback_http_url,
    is_qualified_tool_name as contract_is_qualified_tool_name,
    is_server_name as contract_is_server_name,
};
use serde::{Serialize, Serializer, ser::SerializeStruct};
use serde_json::{Map, Value, json};

use crate::session_store::MAX_RECORD_ID_BYTES;

/// MCP initialize 和每次工具调用的生产超时。
pub const MCP_INITIALIZE_TIMEOUT: Duration = Duration::from_secs(20);
/// MCP 工具调用的生产超时；请求发送和响应读取共用这一预算。
pub const MCP_CALL_TIMEOUT: Duration = Duration::from_secs(20);
/// MCP 工具输出的最大序列化字节数。
pub const MAX_MCP_OUTPUT_BYTES: usize = 1_048_576;

/// JSON-RPC 请求和响应在解码前的严格物化上限。
const MAX_MCP_RESPONSE_BODY_BYTES: usize = MAX_MCP_OUTPUT_BYTES;
/// 限制请求参数在发送前的物化大小，防止模型参数形成无界 HTTP 请求。
const MAX_MCP_REQUEST_BODY_BYTES: usize = MAX_MCP_OUTPUT_BYTES;
const MCP_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
/// worker abort 后用于确认 JoinHandle 终止的第二道固定上限。
const MCP_ABORT_JOIN_TIMEOUT: Duration = Duration::from_millis(100);
/// Sealed cleanup 每个 candidate 的最大尝试次数；失败后进入显式终态，不做无限重试。
const MAX_SEALED_CLEANUP_ATTEMPTS: usize = 2;
/// Sealed cleanup 队列的固定容量，避免 candidate 风暴形成无界内存。
const MAX_SEALED_CLEANUP_QUEUE: usize = 128;
/// Sealed cleanup 终态审计记录的固定容量，避免错误风暴耗尽内存。
const MAX_TERMINAL_CLEANUP_FAILURES: usize = 1024;
/// tools/list 页数硬上限，防止恶意 cursor 永久延长初始化。
const MAX_TOOLS_LIST_PAGES: usize = 128;
/// 单个 server 的实际工具 catalog 固定资源上限；超过后不提交 ready session。
const MAX_MCP_CATALOG_TOOLS: usize = 1024;
/// catalog 元数据累计序列化大小上限，独立于单个 HTTP response body cap。
const MAX_MCP_CATALOG_BYTES: usize = 2 * 1024 * 1024;
/// opaque cursor 的单值上限，避免把 cursor 乘页数放大为无界状态。
const MAX_MCP_CATALOG_CURSOR_BYTES: usize = 4 * 1024;
/// cursor 总数上限与页数独立，避免后续放宽页数时失去状态边界。
const MAX_MCP_CATALOG_CURSORS: usize = 128;
const MCP_TOOL_SEPARATOR: &str = "__";
/// 与 contract qualified-name validator 不同，noop 是 sidecar 内置的全局工具例外。
const NOOP_TOOL: &str = "GrokBuild:efflab_noop";
const JSON_CONTENT_TYPE: &str = "application/json";
const SSE_CONTENT_TYPE: &str = "text/event-stream";
const SESSION_HEADER: &str = "Mcp-Session-Id";
const PROTOCOL_HEADER: &str = "MCP-Protocol-Version";
const MCP_PROTOCOL_VERSION: &str = "2025-06-18";

/// 本地 MCP runtime 的稳定错误分类；错误文本不携带 URL、参数或远端正文。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpError {
    /// 输入配置包含不能执行的 stdio MCP。
    StdioUnavailable,
    /// 输入 URL 不满足 contract 的字面量 loopback HTTP 约束。
    InvalidUrl,
    /// server 名称不能参与 qualified tool name。
    InvalidServerName,
    /// runtime 无法创建独立 HTTP client。
    HttpClientUnavailable,
    /// MCP initialize 或 tools/list 没有在时限内完成。
    InitializationTimeout,
    /// MCP initialize 或 tools/list 返回协议/传输错误。
    InitializationFailed,
    /// MCP tools/list 的累计工具元数据超过本地资源上限。
    CatalogTooLarge,
    /// 工具名称不是 contract 允许的 qualified 形式。
    InvalidToolName,
    /// 工具不在 Host 批准 expected tools 与实际 catalog 的交集内。
    ToolNotApproved,
    /// 工具所在 MCP session 当前不是 ready。
    ToolNotReady,
    /// 工具参数不是 JSON object。
    InvalidArguments,
    /// MCP call 在时限内未完成。
    CallTimeout,
    /// 调用方或 shutdown 取消了 MCP call。
    CallCancelled,
    /// MCP call 返回了协议错误或不可映射的结果。
    CallFailed,
    /// MCP call 的归一化结果超过 1 MiB。
    OutputTooLarge,
    /// 当前 MCP HTTP session 已过期，调用方只允许触发一次重握手。
    SessionExpired,
    /// runtime 已进入 shutdown，不再接收新操作。
    RuntimeShutdown,
    /// runtime 状态锁不可用。
    StateUnavailable,
    /// shutdown 的某个 session 未能在边界内关闭。
    ShutdownFailed,
}

impl McpError {
    /// 返回供日志和 sidecar 内部状态使用的稳定错误码。
    pub const fn code(self) -> &'static str {
        match self {
            Self::StdioUnavailable => "stdio_mcp_unavailable",
            Self::InvalidUrl => "mcp_url_invalid",
            Self::InvalidServerName => "mcp_server_name_invalid",
            Self::HttpClientUnavailable => "mcp_http_client_unavailable",
            Self::InitializationTimeout => "mcp_initialize_timeout",
            Self::InitializationFailed => "mcp_initialize_failed",
            Self::CatalogTooLarge => "mcp_catalog_too_large",
            Self::InvalidToolName => "mcp_tool_name_invalid",
            Self::ToolNotApproved => "mcp_tool_not_approved",
            Self::ToolNotReady => "mcp_tool_not_ready",
            Self::InvalidArguments => "mcp_arguments_invalid",
            Self::CallTimeout => "mcp_call_timeout",
            Self::CallCancelled => "mcp_call_cancelled",
            Self::CallFailed => "mcp_call_failed",
            Self::OutputTooLarge => "mcp_output_too_large",
            Self::SessionExpired => "mcp_session_expired",
            Self::RuntimeShutdown => "mcp_runtime_shutdown",
            Self::StateUnavailable => "mcp_state_unavailable",
            Self::ShutdownFailed => "mcp_shutdown_failed",
        }
    }
}

impl fmt::Display for McpError {
    /// 只输出稳定错误码，避免把底层 reqwest 错误链带入 ACP 或日志。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for McpError {}

/// 可由调用方或 runtime shutdown 触发的幂等 MCP call 取消信号。
#[derive(Clone, Debug, Default)]
pub struct McpCancellationToken {
    state: Arc<CancellationState>,
}

#[derive(Debug, Default)]
struct CancellationState {
    cancelled: std::sync::atomic::AtomicBool,
    notify: tokio::sync::Notify,
}

impl McpCancellationToken {
    /// 创建尚未取消的 token。
    pub fn new() -> Self {
        Self::default()
    }

    /// 发出幂等取消信号并唤醒当前等待者。
    pub fn cancel(&self) {
        if !self.state.cancelled.swap(true, Ordering::AcqRel) {
            self.state.notify.notify_waiters();
            self.state.notify.notify_one();
        }
    }

    /// 返回 token 是否已经取消。
    pub fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::Acquire)
    }

    /// 等待取消，同时覆盖检查和注册 waiter 之间的竞态。
    async fn cancelled(&self) {
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

/// 对 Host 暴露的一个 MCP tool；这里保留实际 catalog，不静默删除未批准工具。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct McpCatalogTool {
    /// MCP server 返回的实际 tool 名。
    pub name: String,
    /// Host catalog parser 使用的启用标记。
    pub enabled: bool,
    /// 可选的人类可读描述。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// 可传给模型 function tool 的 JSON Schema。
    #[serde(rename = "inputSchema", skip_serializing_if = "Option::is_none")]
    pub input_schema: Option<Value>,
}

/// 一个 MCP server 的 Host catalog session 视图。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct McpSessionCatalog {
    /// `ready` 或稳定的非 ready 状态。
    pub status: String,
    /// 实际 tools/list 返回的完整工具列表；模型/调用层另行计算审批交集。
    pub tools: Vec<McpCatalogTool>,
}

/// 一个 MCP server 的嵌套 Host catalog 视图。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct McpServerCatalog {
    /// Host 批准集中的 server 名。
    pub name: String,
    /// MCP session 状态与实际 tool 列表。
    pub session: McpSessionCatalog,
}

/// `_x.ai/mcp/list` 的内部 catalog；序列化后包含 Host parser 期待的 `result.servers`。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct McpCatalog {
    /// 按 server 名稳定排序的 catalog。
    pub servers: Vec<McpServerCatalog>,
}

impl Serialize for McpCatalog {
    /// 序列化为 ACP extension result 内部的嵌套 wire，而不是扁平 servers 表。
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("McpCatalog", 1)?;
        state.serialize_field("result", &CatalogResultRef(&self.servers))?;
        state.end()
    }
}

struct CatalogResultRef<'a>(&'a [McpServerCatalog]);

impl Serialize for CatalogResultRef<'_> {
    /// 输出 Host `parse_catalog` 读取的 `result.servers` 层级。
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("McpCatalogResult", 1)?;
        state.serialize_field("servers", self.0)?;
        state.end()
    }
}

impl McpCatalog {
    /// 返回可直接放入 ACP `ExtResponse` 的 Host wire。
    pub fn to_wire(&self) -> Value {
        json!({"result": {"servers": &self.servers}})
    }
}

/// MCP `tools/call` 的 sidecar-local 结果；不暴露远端 JSON-RPC 类型给 Host。
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct McpCallResult {
    /// 已限制大小并转换为 JSON 的 MCP content blocks。
    pub content: Vec<Value>,
    /// 可选的结构化工具结果。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub structured_content: Option<Value>,
    /// MCP tool-level error 标记；这不是协议 transport error。
    pub is_error: bool,
}

impl McpCallResult {
    /// 返回第一个 text content，便于 sidecar 内部构造有限 transcript。
    pub fn text_content(&self) -> Option<&str> {
        self.content.iter().find_map(|content| {
            (content.get("type").and_then(Value::as_str) == Some("text"))
                .then(|| content.get("text").and_then(Value::as_str))
                .flatten()
        })
    }
}

/// 一个已完成 initialize/tools/list 的 HTTP MCP session。
///
/// 该类型不实现 `Debug`，避免任何意外格式化把审核 URL 或 session id 写入日志。
#[derive(Clone)]
struct McpHttpSession {
    client: reqwest::Client,
    url: String,
    session_id: String,
    protocol_version: String,
    next_request_id: Arc<AtomicU64>,
    /// session 发生不可恢复错误后置为 false，阻止等待中的调用复用旧句柄。
    available: bool,
}

impl McpHttpSession {
    /// 为每个 HTTP session 分配单调 JSON-RPC request id，避免并发调用复用 id。
    fn next_request_id(&self) -> Result<Value, McpError> {
        let current = self
            .next_request_id
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .map_err(|_| McpError::StateUnavailable)?;
        Ok(Value::from(current))
    }
}

/// server catalog 与其可调用 session 的配对。
struct McpServerEntry {
    catalog: McpServerCatalog,
    /// 每个 server 只有一个 session 锁，串行化 call 与 session rollover。
    session: Option<Arc<tokio::sync::Mutex<McpHttpSession>>>,
}

/// provisional cleanup 的生命周期阶段；Sealed 后由独立 bounded owner 执行 DELETE。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CleanupPhase {
    Open,
    Draining,
    Cleaning,
    Sealed,
}

/// candidate 的单一 cleanup owner；shutdown claim 会一直保留到失败句柄最终归档。
#[derive(Clone, Debug)]
enum CandidateCleanupOwner {
    Candidate(Option<McpCancellationToken>),
    Shutdown,
}

/// 登记时决定 candidate 由当前 caller、shutdown 或 Sealed 后 worker 接管。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CleanupRegistration {
    Candidate,
    Shutdown,
    Sealed,
}

/// Sealed worker 空闲时等待的新 job/admission 事件。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SealedCleanupWorkerWait {
    Job,
    AdmissionClosed,
    Deadline,
}

/// Sealed cleanup job 的终态通知；原子状态避免 worker 先完成导致 waiter 丢通知。
struct CleanupCompletion {
    completed: std::sync::atomic::AtomicBool,
    notify: tokio::sync::Notify,
}

impl CleanupCompletion {
    /// 创建未完成的 job 通知。
    fn new() -> Self {
        Self {
            completed: std::sync::atomic::AtomicBool::new(false),
            notify: tokio::sync::Notify::new(),
        }
    }

    /// 发布终态并唤醒仍在等待的 caller。
    fn complete(&self) {
        self.completed.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    /// 在 caller 的 cleanup deadline 内等待 worker 发布终态，避免永久 waiter。
    async fn wait_until(&self, deadline: tokio::time::Instant) -> bool {
        loop {
            if self.completed.load(Ordering::Acquire) {
                return true;
            }
            let notified = self.notify.notified();
            if self.completed.load(Ordering::Acquire) {
                return true;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return false;
            }
        }
    }
}

/// Sealed cleanup 队列中的一个任务；session handle 始终由 supervisor 持有到终态。
#[derive(Clone)]
struct SealedCleanupJob {
    session: Arc<tokio::sync::Mutex<McpHttpSession>>,
    cause: McpError,
    completion: Arc<CleanupCompletion>,
}

/// 已耗尽有界 cleanup 尝试后的稳定终态记录，不保存 URL、session id 或远端正文。
#[derive(Clone, Copy)]
struct CleanupTerminalFailure {
    cause: McpError,
    error: McpError,
    attempts: usize,
}

/// shutdown 的一次性结果；后续调用必须等待同一结果，而不是提前返回成功。
struct ShutdownCompletion {
    result: Mutex<Option<Result<(), McpError>>>,
    notify: tokio::sync::Notify,
}

impl ShutdownCompletion {
    /// 创建尚未完成的 cleanup 状态。
    fn new() -> Self {
        Self {
            result: Mutex::new(None),
            notify: tokio::sync::Notify::new(),
        }
    }

    /// 发布首个 cleanup 结果并唤醒所有并发等待者。
    fn complete(&self, result: Result<(), McpError>) {
        let mut slot = match self.result.lock() {
            Ok(slot) => slot,
            Err(poisoned) => {
                tracing::debug!(
                    event = "mcp_shutdown_result_lock_poison_recovered",
                    error_code = McpError::StateUnavailable.code(),
                    "MCP shutdown 结果锁已 poison，恢复发布"
                );
                self.result.clear_poison();
                poisoned.into_inner()
            }
        };
        if slot.is_none() {
            *slot = Some(result);
            self.notify.notify_waiters();
        }
    }

    /// 返回 shared completion 是否已经发布。
    fn is_completed(&self) -> bool {
        self.result
            .lock()
            .map(|slot| slot.is_some())
            .unwrap_or(true)
    }

    /// 在 caller 的独立 deadline 内等待首个 cleanup 结果，避免永久 waiter。
    async fn wait_until(&self, deadline: tokio::time::Instant) -> Option<Result<(), McpError>> {
        loop {
            let notified = self.notify.notified();
            let result = match self.result.lock() {
                Ok(slot) => *slot,
                Err(poisoned) => {
                    tracing::debug!(
                        event = "mcp_shutdown_result_lock_poison_recovered",
                        error_code = McpError::StateUnavailable.code(),
                        "MCP shutdown waiter 恢复结果锁"
                    );
                    self.result.clear_poison();
                    *poisoned.into_inner()
                }
            };
            if let Some(result) = result {
                return Some(result);
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return None;
            }
        }
    }
}

/// shutdown leader 的 RAII owner；leader 被取消或 panic 时发布稳定失败并恢复 ledger。
struct ShutdownLeaderGuard {
    runtime: McpRuntime,
    completion: Arc<ShutdownCompletion>,
    armed: bool,
}

impl ShutdownLeaderGuard {
    /// 创建仍需负责 shared shutdown completion 的 leader owner。
    fn new(runtime: McpRuntime, completion: Arc<ShutdownCompletion>) -> Self {
        Self {
            runtime,
            completion,
            armed: true,
        }
    }

    /// 正常发布 completion 后解除异常恢复职责。
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ShutdownLeaderGuard {
    fn drop(&mut self) {
        if self.armed {
            tracing::debug!(
                event = "mcp_shutdown_leader_abandoned",
                error_code = McpError::ShutdownFailed.code(),
                "MCP shutdown leader 异常退出，发布稳定失败并恢复 owner"
            );
            self.runtime
                .recover_abandoned_shutdown(&self.completion, McpError::ShutdownFailed);
        }
    }
}

/// Sealed worker 的 RAII owner；任务取消或 panic 时归档 queue/in-flight 并通知 caller。
struct SealedCleanupWorkerGuard {
    runtime: McpRuntime,
    armed: bool,
}

impl SealedCleanupWorkerGuard {
    /// 创建仍需负责 Sealed cleanup recovery 的 worker owner。
    fn new(runtime: McpRuntime) -> Self {
        Self {
            runtime,
            armed: true,
        }
    }

    /// 正常消费完队列或显式归档后解除异常恢复职责。
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for SealedCleanupWorkerGuard {
    fn drop(&mut self) {
        if self.armed {
            tracing::debug!(
                event = "mcp_sealed_cleanup_worker_abandoned",
                error_code = McpError::ShutdownFailed.code(),
                "Sealed cleanup worker 异常退出，归档未完成 job"
            );
            self.runtime
                .recover_sealed_cleanup_worker(McpError::ShutdownFailed);
        }
    }
}

/// join 阶段的 runtime-owned 本地句柄 owner；future 取消时把未完成句柄放回 registry。
struct CleanupWorkerJoinGuard {
    runtime: McpRuntime,
    handles: Vec<tokio::task::JoinHandle<()>>,
    /// 当前 owner 是否占用 cleanup worker join 槽位。
    clears_joining: bool,
    /// reaper 被取消时同时清除其 admission 标记，避免 registry 进入假忙状态。
    clears_abandoned_reaper: bool,
}

impl CleanupWorkerJoinGuard {
    /// 从 state registry 接管一批 worker handle，直到显式 join 完成或重新归还。
    fn new(runtime: McpRuntime, handles: Vec<tokio::task::JoinHandle<()>>) -> Self {
        Self {
            runtime,
            handles,
            clears_joining: true,
            clears_abandoned_reaper: false,
        }
    }

    /// 构造 abandoned shutdown reaper 的 owner；future 未 poll 时也不会丢失 child handles。
    fn new_abandoned_reaper(
        runtime: McpRuntime,
        handles: Vec<tokio::task::JoinHandle<()>>,
    ) -> Self {
        Self {
            runtime,
            handles,
            clears_joining: false,
            clears_abandoned_reaper: true,
        }
    }
}

impl Drop for CleanupWorkerJoinGuard {
    fn drop(&mut self) {
        let count = self.handles.len();
        let (notify, restart_reaper) = {
            let mut state = self.runtime.lock_state_for_cleanup();
            state.cleanup_workers.append(&mut self.handles);
            if self.clears_joining {
                state.cleanup_worker_joining = false;
            }
            if self.clears_abandoned_reaper {
                state.abandoned_shutdown_reaper_running = false;
                state.abandoned_shutdown_reaper_starting = false;
            }
            (
                state.cleanup_worker_changed.clone(),
                count > 0
                    && self.clears_joining
                    && state.shutdown_abandoned
                    && state.shutdown.is_some(),
            )
        };
        notify.notify_waiters();
        if count > 0 {
            tracing::debug!(
                event = "mcp_sealed_cleanup_worker_handles_rehomed",
                handle_count = count,
                error_code = McpError::ShutdownFailed.code(),
                "cleanup join future 取消后已归还 runtime-owned worker handles"
            );
        }
        // abandoned shutdown 仍需由 runtime-owned reaper 接管归还的 child handle。
        if restart_reaper {
            self.runtime.maybe_start_abandoned_shutdown_reaper();
        }
    }
}

/// runtime 内部状态；同步锁只保护短暂快照和 session ownership 转移，不跨 HTTP await 持锁。
struct McpRuntimeState {
    shutting_down: bool,
    next_call_id: u64,
    active_calls: BTreeMap<u64, McpCancellationToken>,
    servers: BTreeMap<String, McpServerEntry>,
    /// 未完成的 provisional session 由 runtime 持有；Sealed worker 失败后转入显式终态。
    pending_cleanup_sessions: Vec<Arc<tokio::sync::Mutex<McpHttpSession>>>,
    /// candidate cleanup 与 shutdown cleanup 的 owner 仲裁阶段。
    cleanup_phase: CleanupPhase,
    /// completion 发布前后禁止穿透的 Sealed cleanup admission barrier。
    cleanup_admission_closed: bool,
    /// 已被 shutdown 接管的 candidate；未封存阶段失败保留 claim，Sealed job 终态会显式归档。
    shutdown_owned_cleanup_sessions: Vec<Arc<tokio::sync::Mutex<McpHttpSession>>>,
    /// shutdown 开始时取消仍在进行的 candidate DELETE，使 owner 能在边界内交接。
    cleanup_cancellation: McpCancellationToken,
    /// Sealed candidate 由单一 supervisor 串行消费，共享一个 2 秒 cleanup deadline。
    sealed_cleanup_queue: VecDeque<SealedCleanupJob>,
    /// supervisor 当前正在处理的任务；worker 异常时可从这里恢复 ownership。
    sealed_cleanup_in_flight: Option<SealedCleanupJob>,
    /// 防止多个 candidate 同时启动独立 supervisor，避免 gate deadline 彼此争抢。
    sealed_cleanup_worker_running: bool,
    /// worker 正在从空闲态收尾；该标记期间仍由当前 task 持有唯一 owner。
    sealed_cleanup_worker_exiting: bool,
    /// worker 已获 admission 但尚未完成 tokio::spawn；join 不得把该窗口视为损坏。
    sealed_cleanup_worker_starting: bool,
    /// runtime-owned worker handles；shutdown 必须在 runtime drop 前有界 join。
    cleanup_workers: Vec<tokio::task::JoinHandle<()>>,
    /// 防止并发 join waiter 在另一个 owner 持有 local handles 时归档 job。
    cleanup_worker_joining: bool,
    /// Sealed cleanup 失败后的显式、稳定终态审计记录，保留在固定容量内。
    terminal_cleanup_failures: VecDeque<CleanupTerminalFailure>,
    /// worker task panic/abort 等 JoinError；即使 job 已被 guard 归档也必须使 shutdown 失败。
    cleanup_worker_failed: bool,
    /// shutdown leader join worker 时共享的截止时间；完成后清除，迟到 candidate 获得新预算。
    shutdown_join_deadline: Option<tokio::time::Instant>,
    /// leader 已放弃但仍需等 runtime-owned worker join 后发布 shared failure。
    shutdown_abandoned: bool,
    /// abandoned recovery 是否正在启动独立的 runtime-owned join reaper。
    abandoned_shutdown_reaper_starting: bool,
    /// abandoned recovery reaper 尚未完成 child worker join。
    abandoned_shutdown_reaper_running: bool,
    active_changed: Arc<tokio::sync::Notify>,
    /// 唤醒等待 worker admission/退出状态变化的 bounded join。
    cleanup_worker_changed: Arc<tokio::sync::Notify>,
    shutdown: Option<Arc<ShutdownCompletion>>,
}

/// sidecar-local MCP runtime；不依赖模型 client，也不管理任何 MCP 子进程。
#[derive(Clone)]
pub struct McpRuntime {
    state: Arc<Mutex<McpRuntimeState>>,
    expected_tools: BTreeSet<String>,
    timeout: Duration,
    /// 串行化 candidate 与 shutdown 的 DELETE，避免快照后出现双重 owner。
    cleanup_gate: Arc<tokio::sync::Mutex<()>>,
    /// debug 构建中的异步测试接缝；release runtime 不包含该字段。
    #[cfg(debug_assertions)]
    test_seam: Option<crate::test_seam::TestSeam>,
}

/// 单次 MCP call 的内部结果；`invalidate_session` 保证失效句柄不会被并发调用复用。
struct RunCallOutcome {
    result: Result<McpCallResult, McpError>,
    invalidate_session: bool,
}

impl RunCallOutcome {
    /// 构造不改变共享 session 状态的结果。
    fn keep(result: Result<McpCallResult, McpError>) -> Self {
        Self {
            result,
            invalidate_session: false,
        }
    }

    /// 构造需要从 runtime 共享状态移除当前 session 的结果。
    fn invalidate(result: Result<McpCallResult, McpError>) -> Self {
        Self {
            result,
            invalidate_session: true,
        }
    }
}

impl fmt::Debug for McpRuntime {
    /// 只描述 session 数量和 shutdown 状态，不打印 URL 或 transport 内部对象。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Ok(state) = self.state.lock() else {
            return formatter.write_str("McpRuntime(<state-unavailable>)");
        };
        formatter
            .debug_struct("McpRuntime")
            .field("server_count", &state.servers.len())
            .field(
                "http_session_count",
                &state
                    .servers
                    .values()
                    .filter(|entry| entry.session.is_some())
                    .count()
                    .saturating_add(state.pending_cleanup_sessions.len()),
            )
            .field("shutting_down", &state.shutting_down)
            .finish()
    }
}

impl McpRuntime {
    /// 获取 cleanup 所需的 state lock；poison 后恢复已有 ownership，而不是丢弃 job。
    fn lock_state_for_cleanup(&self) -> std::sync::MutexGuard<'_, McpRuntimeState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                tracing::debug!(
                    event = "mcp_state_lock_poison_recovered",
                    error_code = McpError::StateUnavailable.code(),
                    "MCP cleanup state lock 已 poison，恢复已有状态"
                );
                self.state.clear_poison();
                poisoned.into_inner()
            }
        }
    }

    /// 创建无 HTTP session 的兼容 runtime；只用于没有 MCP 配置的单元测试/旧构造器。
    pub fn empty() -> Self {
        Self {
            state: Arc::new(Mutex::new(McpRuntimeState {
                shutting_down: false,
                next_call_id: 1,
                active_calls: BTreeMap::new(),
                servers: BTreeMap::new(),
                pending_cleanup_sessions: Vec::new(),
                cleanup_phase: CleanupPhase::Open,
                cleanup_admission_closed: false,
                shutdown_owned_cleanup_sessions: Vec::new(),
                cleanup_cancellation: McpCancellationToken::new(),
                sealed_cleanup_queue: VecDeque::new(),
                sealed_cleanup_in_flight: None,
                sealed_cleanup_worker_running: false,
                sealed_cleanup_worker_exiting: false,
                sealed_cleanup_worker_starting: false,
                cleanup_workers: Vec::new(),
                cleanup_worker_joining: false,
                terminal_cleanup_failures: VecDeque::new(),
                cleanup_worker_failed: false,
                shutdown_join_deadline: None,
                shutdown_abandoned: false,
                abandoned_shutdown_reaper_starting: false,
                abandoned_shutdown_reaper_running: false,
                active_changed: Arc::new(tokio::sync::Notify::new()),
                cleanup_worker_changed: Arc::new(tokio::sync::Notify::new()),
                shutdown: None,
            })),
            expected_tools: BTreeSet::new(),
            timeout: MCP_INITIALIZE_TIMEOUT,
            cleanup_gate: Arc::new(tokio::sync::Mutex::new(())),
            #[cfg(debug_assertions)]
            test_seam: None,
        }
    }

    /// 从 Host 审核的 MCP 集合创建 runtime；空审批不会构造 reqwest client。
    pub async fn new(
        approved: ApprovedMcpConfig,
        expected_tools: BTreeSet<String>,
    ) -> Result<Self, McpError> {
        Self::new_inner(approved, expected_tools, MCP_INITIALIZE_TIMEOUT).await
    }

    /// 仅供 debug 测试缩短等待窗口；生产 `new` 始终使用 20 秒。
    #[doc(hidden)]
    pub async fn new_with_timeout_for_test(
        approved: ApprovedMcpConfig,
        expected_tools: BTreeSet<String>,
        timeout: Duration,
    ) -> Result<Self, McpError> {
        Self::new_inner(approved, expected_tools, timeout).await
    }

    /// 为 debug 集成测试安装文件型异步 barrier；release 构建没有该接缝。
    #[cfg(debug_assertions)]
    #[doc(hidden)]
    pub fn set_test_seam_for_test(&mut self, root: std::path::PathBuf) {
        self.test_seam = Some(crate::test_seam::TestSeam::new(root));
    }

    /// 在 debug 构建中标记并等待指定测试窗口，不参与生产运行时。
    #[cfg(debug_assertions)]
    async fn test_wait_if_enabled(&self, name: &str) {
        if let Some(seam) = &self.test_seam {
            seam.mark(name);
            seam.wait_if_enabled(name).await;
        }
    }

    /// 为 debug 集成测试构造一个未提交 candidate；仅用于验证 shutdown ownership。
    #[cfg(debug_assertions)]
    #[doc(hidden)]
    pub async fn cleanup_candidate_for_test(&self, url: String, session_id: String) {
        let Ok(client) = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(self.timeout)
            .timeout(self.timeout)
            .build()
        else {
            return;
        };
        let candidate = provisional_http_session(&client, &url, session_id);
        self.cleanup_candidate_session(
            candidate,
            tokio::time::Instant::now() + self.timeout,
            McpError::CallFailed,
        )
        .await;
    }

    /// 返回 debug 测试可观察的 cleanup registry 与 shutdown claim 数量，不暴露 session 数据。
    #[cfg(debug_assertions)]
    #[doc(hidden)]
    pub fn cleanup_ownership_for_test(&self) -> (usize, usize) {
        self.state
            .lock()
            .map(|state| {
                (
                    state.pending_cleanup_sessions.len(),
                    state.shutdown_owned_cleanup_sessions.len(),
                )
            })
            .unwrap_or((usize::MAX, usize::MAX))
    }

    /// 返回当前 runtime registry 中的 cleanup worker handle 数量，供退出窗口回归使用。
    #[cfg(debug_assertions)]
    #[doc(hidden)]
    pub fn cleanup_worker_handle_count_for_test(&self) -> usize {
        self.state
            .lock()
            .map(|state| state.cleanup_workers.len())
            .unwrap_or(usize::MAX)
    }

    /// 返回 Sealed cleanup 的稳定终态记录，供测试验证失败不会停留在被动 ledger。
    #[cfg(debug_assertions)]
    #[doc(hidden)]
    pub fn cleanup_terminal_failures_for_test(&self) -> Vec<(String, String, usize)> {
        self.state
            .lock()
            .map(|state| {
                state
                    .terminal_cleanup_failures
                    .iter()
                    .map(|failure| {
                        (
                            failure.cause.code().to_owned(),
                            failure.error.code().to_owned(),
                            failure.attempts,
                        )
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// 执行配置验证、独立 HTTP client 构造和每个批准 server 的 MCP handshake。
    async fn new_inner(
        approved: ApprovedMcpConfig,
        expected_tools: BTreeSet<String>,
        timeout: Duration,
    ) -> Result<Self, McpError> {
        let server_count = approved.servers.len();
        tracing::debug!(
            event = "mcp_runtime_starting",
            server_count,
            expected_tool_count = expected_tools.len(),
            "启动 sidecar MCP runtime"
        );

        // expected_tools 是模型和 call gate 的输入，必须先经过共享 qualified-name 校验。
        // 内置 noop 不属于 server__tool wire 形状，保留精确字符串例外。
        if expected_tools
            .iter()
            .any(|name| name != NOOP_TOOL && !contract_is_qualified_tool_name(name))
        {
            tracing::debug!(
                event = "mcp_expected_tool_name_invalid",
                error_code = McpError::InvalidToolName.code(),
                "拒绝含非法 qualified tool name 的 MCP runtime"
            );
            return Err(McpError::InvalidToolName);
        }

        // 先验证整个集合，避免部分初始化后才发现 stdio/URL/名称不可接受。
        for (server_name, spec) in &approved.servers {
            if !contract_is_server_name(server_name) {
                return Err(McpError::InvalidServerName);
            }
            match spec {
                McpServerSpec::Http { url } => {
                    if !is_literal_loopback_http_url(url) {
                        return Err(McpError::InvalidUrl);
                    }
                }
                McpServerSpec::Stdio { .. } => return Err(McpError::StdioUnavailable),
            }
        }

        let state = Arc::new(Mutex::new(McpRuntimeState {
            shutting_down: false,
            next_call_id: 1,
            active_calls: BTreeMap::new(),
            servers: BTreeMap::new(),
            pending_cleanup_sessions: Vec::new(),
            cleanup_phase: CleanupPhase::Open,
            cleanup_admission_closed: false,
            shutdown_owned_cleanup_sessions: Vec::new(),
            cleanup_cancellation: McpCancellationToken::new(),
            sealed_cleanup_queue: VecDeque::new(),
            sealed_cleanup_in_flight: None,
            sealed_cleanup_worker_running: false,
            sealed_cleanup_worker_exiting: false,
            sealed_cleanup_worker_starting: false,
            cleanup_workers: Vec::new(),
            cleanup_worker_joining: false,
            terminal_cleanup_failures: VecDeque::new(),
            cleanup_worker_failed: false,
            shutdown_join_deadline: None,
            shutdown_abandoned: false,
            abandoned_shutdown_reaper_starting: false,
            abandoned_shutdown_reaper_running: false,
            active_changed: Arc::new(tokio::sync::Notify::new()),
            cleanup_worker_changed: Arc::new(tokio::sync::Notify::new()),
            shutdown: None,
        }));
        let runtime = Self {
            state,
            expected_tools,
            timeout,
            cleanup_gate: Arc::new(tokio::sync::Mutex::new(())),
            #[cfg(debug_assertions)]
            test_seam: None,
        };
        if approved.servers.is_empty() {
            return Ok(runtime);
        }

        // no_proxy 明确禁止环境代理；redirect none 防止 HTTP MCP 跨越审核 URL。
        let client = match reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(timeout)
            .timeout(timeout)
            .build()
        {
            Ok(client) => client,
            Err(_) => {
                tracing::debug!(
                    event = "mcp_http_client_unavailable",
                    error_code = McpError::HttpClientUnavailable.code(),
                    "MCP HTTP client 创建失败"
                );
                let entries = approved
                    .servers
                    .into_keys()
                    .map(|name| (name.clone(), error_catalog_entry(name)))
                    .collect();
                match runtime.state.lock() {
                    Ok(mut state) => state.servers = entries,
                    Err(_) => return Err(McpError::StateUnavailable),
                }
                return Ok(runtime);
            }
        };

        // 所有 server 共用本次 runtime 初始化入口的 deadline，避免多个 server 顺序相加。
        let deadline = tokio::time::Instant::now() + timeout;
        let mut entries = BTreeMap::new();
        for (server_name, spec) in approved.servers {
            let entry = match spec {
                McpServerSpec::Http { url } => {
                    match connect_http_session(&runtime, &client, &url, &server_name, deadline)
                        .await
                    {
                        Ok((session, tools)) => McpServerEntry {
                            catalog: McpServerCatalog {
                                name: server_name.clone(),
                                session: McpSessionCatalog {
                                    status: "ready".to_owned(),
                                    tools,
                                },
                            },
                            session: Some(Arc::new(tokio::sync::Mutex::new(session))),
                        },
                        Err(error) => {
                            tracing::debug!(
                                event = "mcp_http_session_not_ready",
                                error_code = error.code(),
                                "MCP HTTP session 非 ready"
                            );
                            error_catalog_entry(server_name)
                        }
                    }
                }
                McpServerSpec::Stdio { .. } => {
                    // 前置验证已经拒绝该分支；这里保持 runtime ownership 转移也 fail-closed。
                    return Err(McpError::StdioUnavailable);
                }
            };
            entries.insert(entry.catalog.name.clone(), entry);
        }
        if let Ok(mut state) = runtime.state.lock() {
            state.servers = entries;
        } else {
            return Err(McpError::StateUnavailable);
        }
        tracing::debug!(
            event = "mcp_http_sessions_initialized",
            server_count,
            http_session_count = runtime.http_session_count(),
            "MCP HTTP session 初始化完成"
        );
        Ok(runtime)
    }

    /// 返回 Host `parse_catalog` 所需的嵌套 catalog 快照。
    pub async fn catalog(&self) -> Result<McpCatalog, McpError> {
        let state = self.state.lock().map_err(|_| McpError::StateUnavailable)?;
        if state.shutting_down {
            return Err(McpError::RuntimeShutdown);
        }
        Ok(McpCatalog {
            servers: state
                .servers
                .values()
                .map(|entry| entry.catalog.clone())
                .collect(),
        })
    }

    /// 返回当前仍持有的 HTTP MCP session 数量；不统计任务或操作系统进程。
    pub fn http_session_count(&self) -> usize {
        self.state
            .lock()
            .map(|state| {
                state
                    .servers
                    .values()
                    .filter(|entry| entry.session.is_some())
                    .count()
                    .saturating_add(state.pending_cleanup_sessions.len())
            })
            .unwrap_or(0)
    }

    /// 返回实际 ready catalog 与 Host expected tools 的稳定交集。
    pub fn model_visible_tools(&self) -> Vec<String> {
        let Ok(state) = self.state.lock() else {
            return Vec::new();
        };
        if state.shutting_down {
            return Vec::new();
        }
        let mut tools = BTreeSet::new();
        for entry in state.servers.values() {
            if entry.catalog.session.status != "ready" {
                continue;
            }
            for tool in &entry.catalog.session.tools {
                if !is_ready_catalog_tool(entry, tool) {
                    continue;
                }
                let qualified = qualified_tool_name(&entry.catalog.name, &tool.name);
                if self.expected_tools.contains(&qualified) {
                    tools.insert(qualified);
                }
            }
        }
        tools.into_iter().collect()
    }

    /// 返回模型 function tool schema，只包含已批准并 ready 的 HTTP MCP tool。
    pub fn model_tool_schemas(&self) -> Vec<Value> {
        let Ok(state) = self.state.lock() else {
            return Vec::new();
        };
        if state.shutting_down {
            return Vec::new();
        }
        let mut schemas = Vec::new();
        for entry in state.servers.values() {
            if entry.catalog.session.status != "ready" {
                continue;
            }
            for tool in &entry.catalog.session.tools {
                if !is_ready_catalog_tool(entry, tool) {
                    continue;
                }
                let qualified = qualified_tool_name(&entry.catalog.name, &tool.name);
                if !self.expected_tools.contains(&qualified) {
                    continue;
                }
                let parameters = tool
                    .input_schema
                    .clone()
                    .unwrap_or_else(|| json!({"type": "object", "properties": {}}));
                let mut function = Map::new();
                function.insert("name".to_owned(), Value::String(qualified));
                if let Some(description) = &tool.description {
                    function.insert("description".to_owned(), Value::String(description.clone()));
                }
                function.insert("parameters".to_owned(), parameters);
                schemas.push(json!({"type": "function", "function": function}));
            }
        }
        schemas
    }

    /// 调用一个已通过 expected intersection 的 qualified MCP tool。
    pub async fn call(
        &self,
        qualified_name: &str,
        arguments: Value,
    ) -> Result<McpCallResult, McpError> {
        self.call_with_cancellation(qualified_name, arguments, McpCancellationToken::new())
            .await
    }

    /// 调用 MCP tool，并在 caller token 或 runtime shutdown 时取消远端 request。
    pub async fn call_with_cancellation(
        &self,
        qualified_name: &str,
        arguments: Value,
        cancellation: McpCancellationToken,
    ) -> Result<McpCallResult, McpError> {
        // deadline 在公开操作入口创建，覆盖 request body 物化、send 和响应 body 读取。
        let deadline = tokio::time::Instant::now() + self.timeout;
        let Some((server_name, tool_name)) = split_qualified_tool_name(qualified_name) else {
            return self.finish_call_error(McpError::InvalidToolName);
        };
        if cancellation.is_cancelled() {
            return self.finish_call_error(McpError::CallCancelled);
        }
        let Value::Object(arguments) = arguments else {
            return self.finish_call_error(McpError::InvalidArguments);
        };

        let (call_id, session) = {
            let mut state = self.state.lock().map_err(|_| McpError::StateUnavailable)?;
            if state.shutting_down {
                return self.finish_call_error(McpError::RuntimeShutdown);
            }
            let qualified = qualified_tool_name(server_name, tool_name);
            let session = {
                let Some(entry) = state.servers.get(server_name) else {
                    return self.finish_call_error(McpError::ToolNotApproved);
                };
                let is_approved_catalog_tool = self.expected_tools.contains(&qualified)
                    && entry.catalog.session.tools.iter().any(|tool| {
                        let catalog_qualified =
                            qualified_tool_name(&entry.catalog.name, &tool.name);
                        tool.name == tool_name
                            && tool.enabled
                            && is_tool_name_segment(&tool.name)
                            && contract_is_qualified_tool_name(&catalog_qualified)
                            && is_persistable_qualified_tool_name(&entry.catalog.name, &tool.name)
                    });
                if !is_approved_catalog_tool {
                    return self.finish_call_error(McpError::ToolNotApproved);
                }
                if entry.catalog.session.status != "ready" {
                    return self.finish_call_error(McpError::ToolNotReady);
                }
                let Some(session) = entry.session.as_ref() else {
                    return self.finish_call_error(McpError::ToolNotReady);
                };
                session.clone()
            };
            let call_id = state.next_call_id;
            let Some(next_call_id) = call_id.checked_add(1) else {
                return self.finish_call_error(McpError::StateUnavailable);
            };
            state.next_call_id = next_call_id;
            state.active_calls.insert(call_id, cancellation.clone());
            (call_id, session.clone())
        };

        tracing::debug!(
            event = "mcp_call_started",
            active_call_count = self.active_call_count()
        );
        let session_for_invalidation = session.clone();
        let outcome = self
            .run_call(
                session,
                tool_name,
                arguments,
                cancellation.clone(),
                deadline,
            )
            .await;
        if outcome.invalidate_session {
            self.invalidate_session(server_name, &session_for_invalidation);
        }
        self.finish_active_call(call_id);
        match outcome.result {
            Ok(result) => {
                tracing::debug!(
                    event = "mcp_call_succeeded",
                    output_bytes = serialized_call_result_size(&result)
                );
                Ok(result)
            }
            Err(error) => self.finish_call_error(error),
        }
    }

    /// 在同一 deadline 内发送 MCP call，并在 serde 解码前限制响应 body。
    async fn run_call(
        &self,
        session: Arc<tokio::sync::Mutex<McpHttpSession>>,
        tool_name: &str,
        arguments: Map<String, Value>,
        cancellation: McpCancellationToken,
        deadline: tokio::time::Instant,
    ) -> RunCallOutcome {
        // 每个 server 的 session 在整个请求/重握手期间保持独占，避免并发覆盖 session id。
        let mut session =
            match Self::lock_http_session(&session, deadline, Some(cancellation.clone())).await {
                Ok(session) => session,
                Err(error) => return RunCallOutcome::keep(Err(error)),
            };
        if !session.available {
            return RunCallOutcome::invalidate(Err(McpError::ToolNotReady));
        }
        let request_id = match session.next_request_id() {
            Ok(request_id) => request_id,
            Err(error) => return RunCallOutcome::keep(Err(error)),
        };
        let message = Self::call_request_message(request_id.clone(), tool_name, &arguments);
        let response = match post_json_rpc(
            &session.client,
            &session.url,
            Some(&session),
            &message,
            Some(&request_id),
            deadline,
            Some(cancellation.clone()),
            None,
        )
        .await
        {
            // session-bound 404 只允许在当前 server 的独占锁内重握手并重放一次。
            Err(McpError::SessionExpired) => {
                // 先阻止等待中的调用复用已知失效的旧句柄，再尝试构造 provisional session。
                session.available = false;
                let candidate = match self
                    .reinitialize_http_session(&session, deadline, cancellation.clone())
                    .await
                {
                    Ok(candidate) => candidate,
                    Err(error) => return RunCallOutcome::invalidate(Err(error)),
                };
                let retry_id = match candidate.next_request_id() {
                    Ok(retry_id) => retry_id,
                    Err(error) => {
                        self.cleanup_candidate_session(candidate, deadline, error)
                            .await;
                        return RunCallOutcome::invalidate(Err(error));
                    }
                };
                let retry_message =
                    Self::call_request_message(retry_id.clone(), tool_name, &arguments);
                let candidate_result = match post_json_rpc(
                    &candidate.client,
                    &candidate.url,
                    Some(&candidate),
                    &retry_message,
                    Some(&retry_id),
                    deadline,
                    Some(cancellation),
                    None,
                )
                .await
                {
                    Ok(Some(response)) => Self::decode_call_response(response),
                    Ok(None) => Err(McpError::CallFailed),
                    // 第二次 404 直接 fail-closed，不能递归触发新的握手。
                    Err(McpError::SessionExpired) => Err(McpError::CallFailed),
                    Err(error) => Err(error),
                };
                match candidate_result {
                    Ok(result) => {
                        // 只有 replay 的完整协议与结果校验成功才原子提交候选 session。
                        *session = candidate;
                        return RunCallOutcome::keep(Ok(result));
                    }
                    Err(error) => {
                        self.cleanup_candidate_session(candidate, deadline, error)
                            .await;
                        return RunCallOutcome::invalidate(Err(error));
                    }
                }
            }
            Ok(Some(response)) => response,
            Ok(None) => return RunCallOutcome::keep(Err(McpError::CallFailed)),
            Err(error) => return RunCallOutcome::keep(Err(error)),
        };
        RunCallOutcome::keep(Self::decode_call_response(response))
    }

    /// 将已通过 envelope 校验的 MCP call response 转换为本地有界结果。
    fn decode_call_response(response: McpRpcResponse) -> Result<McpCallResult, McpError> {
        let response_id = response.id.clone().ok_or(McpError::CallFailed)?;
        let result = response.into_result(response_id)?;
        normalize_call_result(result)
    }

    /// 构造带有限参数的 MCP tools/call 请求；参数只在请求体中使用，不进入日志。
    fn call_request_message(
        request_id: Value,
        tool_name: &str,
        arguments: &Map<String, Value>,
    ) -> Value {
        json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "tools/call",
            "params": {
                "name": tool_name,
                "arguments": arguments,
            }
        })
    }

    /// 获取单个 server 的 session 所有权；锁等待也受 caller deadline/cancel 约束。
    async fn lock_http_session<'a>(
        session: &'a Arc<tokio::sync::Mutex<McpHttpSession>>,
        deadline: tokio::time::Instant,
        cancellation: Option<McpCancellationToken>,
    ) -> Result<tokio::sync::MutexGuard<'a, McpHttpSession>, McpError> {
        let lock_future = session.lock();
        tokio::pin!(lock_future);
        if let Some(cancellation) = cancellation {
            let deadline_sleep = tokio::time::sleep_until(deadline);
            tokio::pin!(deadline_sleep);
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => Err(McpError::CallCancelled),
                _ = &mut deadline_sleep => Err(McpError::CallTimeout),
                guard = &mut lock_future => Ok(guard),
            }
        } else {
            match tokio::time::timeout_at(deadline, &mut lock_future).await {
                Ok(guard) => Ok(guard),
                Err(_) => Err(McpError::CallTimeout),
            }
        }
    }

    /// 在同一 session 锁内完成有限重握手；只有 caller 验证 replay 成功后才提交候选。
    async fn reinitialize_http_session(
        &self,
        session: &McpHttpSession,
        deadline: tokio::time::Instant,
        cancellation: McpCancellationToken,
    ) -> Result<McpHttpSession, McpError> {
        let initialize_id = session.next_request_id()?;
        let initialize = initialize_request_message(initialize_id.clone());
        let mut captured_session_id = None;
        let response = match post_json_rpc(
            &session.client,
            &session.url,
            None,
            &initialize,
            Some(&initialize_id),
            deadline,
            Some(cancellation.clone()),
            Some(&mut captured_session_id),
        )
        .await
        {
            Ok(Some(response)) => response,
            Ok(None) => {
                let error = McpError::CallFailed;
                return Err(self
                    .cleanup_reinitialize_candidate(
                        &session.client,
                        &session.url,
                        captured_session_id,
                        deadline,
                        error,
                    )
                    .await);
            }
            Err(error) => {
                return Err(self
                    .cleanup_reinitialize_candidate(
                        &session.client,
                        &session.url,
                        captured_session_id,
                        deadline,
                        error,
                    )
                    .await);
            }
        };
        let session_id = response
            .session_id
            .clone()
            .or(captured_session_id.take())
            .filter(|value| !value.is_empty());
        let Some(session_id) = session_id else {
            return Err(McpError::CallFailed);
        };
        let candidate = provisional_http_session(&session.client, &session.url, session_id);
        let result = match response.into_result(initialize_id) {
            Ok(result) => result,
            Err(error) => {
                self.cleanup_candidate_session(candidate, deadline, error)
                    .await;
                return Err(error);
            }
        };
        if result.get("protocolVersion").and_then(Value::as_str) != Some(MCP_PROTOCOL_VERSION) {
            let error = McpError::CallFailed;
            self.cleanup_candidate_session(candidate, deadline, error)
                .await;
            return Err(error);
        }
        let initialized = json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
            "params": {}
        });
        match post_json_rpc(
            &candidate.client,
            &candidate.url,
            Some(&candidate),
            &initialized,
            None,
            deadline,
            Some(cancellation),
            None,
        )
        .await
        {
            Ok(None) => Ok(candidate),
            Ok(Some(_)) => {
                // notification 不允许返回 JSON-RPC response；异常 response 不能提交 candidate。
                let error = McpError::CallFailed;
                self.cleanup_candidate_session(candidate, deadline, error)
                    .await;
                Err(error)
            }
            Err(McpError::SessionExpired) => {
                let error = McpError::CallFailed;
                self.cleanup_candidate_session(candidate, deadline, error)
                    .await;
                Err(error)
            }
            Err(error) => {
                self.cleanup_candidate_session(candidate, deadline, error)
                    .await;
                Err(error)
            }
        }
    }

    /// 将尚未提交的 reinitialize candidate 纳入 runtime，失败时交给 cleanup owner 重试。
    async fn cleanup_candidate_session(
        &self,
        candidate: McpHttpSession,
        deadline: tokio::time::Instant,
        cause: McpError,
    ) {
        let handle = Arc::new(tokio::sync::Mutex::new(candidate));
        let completion = Arc::new(CleanupCompletion::new());
        let registration = match self.register_pending_cleanup_session(
            handle.clone(),
            cause,
            completion.clone(),
        ) {
            Ok(registration) => registration,
            Err(error) => {
                tracing::debug!(
                    event = "mcp_initialization_cleanup_state_unavailable",
                    cause_code = cause.code(),
                    error_code = error.code(),
                    "MCP candidate cleanup 无法登记到 runtime"
                );
                return;
            }
        };
        #[cfg(debug_assertions)]
        self.test_wait_if_enabled("candidate-registered").await;

        if registration == CleanupRegistration::Sealed {
            // caller 只在本次 cleanup deadline 内等待；取消 caller 不会丢弃 runtime-owned worker。
            let _ = completion.wait_until(deadline).await;
            return;
        }

        // candidate 在 shutdown drain/cleaning 期间仍需等待 owner gate；不能提前丢弃
        // pending handle，否则 shutdown gate 失败后会留下无人接管的远端 session。
        if registration == CleanupRegistration::Shutdown
            || matches!(
                self.candidate_cleanup_owner(&handle),
                Ok(CandidateCleanupOwner::Shutdown) | Err(_)
            )
        {
            return;
        }
        #[cfg(debug_assertions)]
        self.test_wait_if_enabled("candidate-before-gate").await;

        // gate acquisition 与 phase re-check 共同覆盖登记后 shutdown 抢先转换阶段的交错。
        let gate_future = self.cleanup_gate.lock();
        tokio::pin!(gate_future);
        let cleanup_guard = match tokio::time::timeout_at(deadline, &mut gate_future).await {
            Ok(guard) => guard,
            Err(_) => {
                tracing::debug!(
                    event = "mcp_initialization_cleanup_failed",
                    cause_code = cause.code(),
                    error_code = McpError::CallTimeout.code(),
                    "MCP candidate cleanup gate 超时"
                );
                return;
            }
        };
        let cleanup_cancellation = match self.candidate_cleanup_owner(&handle) {
            Ok(CandidateCleanupOwner::Candidate(cancellation)) => cancellation,
            Ok(CandidateCleanupOwner::Shutdown) | Err(_) => {
                // shutdown 已经 claim 了该 handle，或状态不可用；无权再发起 candidate DELETE。
                return;
            }
        };
        #[cfg(debug_assertions)]
        self.test_wait_if_enabled("candidate-before-delete").await;

        let cleanup_result = match Self::lock_http_session(&handle, deadline, None).await {
            Ok(session) => close_http_session(&session, deadline, cleanup_cancellation).await,
            Err(error) => Err(error),
        };
        match cleanup_result {
            Ok(()) => {
                // 必须在释放全局 gate 前删除 registry，确保 shutdown snapshot 不会取到已完成句柄。
                self.remove_pending_cleanup_session(&handle);
                // candidate 可能在 gate 内被 shutdown 追认；成功后同时释放该 claim，避免留下过期 owner。
                release_shutdown_cleanup_claim(self, &handle);
            }
            Err(error) => {
                // 失败 candidate 仍保留在 registry，等待 shutdown 使用新的 bounded deadline。
                tracing::debug!(
                    event = "mcp_initialization_cleanup_failed",
                    cause_code = cause.code(),
                    error_code = error.code(),
                    "MCP 初始化失败后的 session cleanup 失败"
                );
            }
        }
        drop(cleanup_guard);
    }

    /// 登记 provisional session 并决定 cleanup owner。
    fn register_pending_cleanup_session(
        &self,
        session: Arc<tokio::sync::Mutex<McpHttpSession>>,
        cause: McpError,
        completion: Arc<CleanupCompletion>,
    ) -> Result<CleanupRegistration, McpError> {
        let (registration, worker_deadline, notify) = {
            let mut state = self.lock_state_for_cleanup();
            state.cleanup_workers.retain(|handle| !handle.is_finished());
            state.pending_cleanup_sessions.push(Arc::clone(&session));
            let notify = state.cleanup_worker_changed.clone();
            let registration = match state.cleanup_phase {
                CleanupPhase::Open => (CleanupRegistration::Candidate, None),
                CleanupPhase::Draining | CleanupPhase::Cleaning => {
                    // shutdown 前 candidate 只归 shutdown。
                    claim_shutdown_cleanup_sessions(&mut state, std::slice::from_ref(&session));
                    (CleanupRegistration::Shutdown, None)
                }
                CleanupPhase::Sealed => {
                    // 同锁完成 pending、claim、入队。
                    claim_shutdown_cleanup_sessions(&mut state, std::slice::from_ref(&session));
                    let job = SealedCleanupJob {
                        session: Arc::clone(&session),
                        cause,
                        completion,
                    };
                    if state.cleanup_admission_closed {
                        // admission 关闭后不可再启动 worker；当前 job 直接进入失败终态。
                        archive_sealed_cleanup_job_locked(
                            &mut state,
                            job,
                            McpError::ShutdownFailed,
                            0,
                        );
                        (CleanupRegistration::Sealed, None)
                    } else if state.sealed_cleanup_queue.len() >= MAX_SEALED_CLEANUP_QUEUE {
                        archive_sealed_cleanup_job_locked(
                            &mut state,
                            job,
                            McpError::ShutdownFailed,
                            0,
                        );
                        (CleanupRegistration::Sealed, None)
                    } else {
                        state.sealed_cleanup_queue.push_back(job);
                        let deadline = if state.sealed_cleanup_worker_running
                            || state.sealed_cleanup_worker_exiting
                            || state.sealed_cleanup_worker_starting
                        {
                            None
                        } else {
                            state.sealed_cleanup_worker_starting = true;
                            state.sealed_cleanup_worker_running = true;
                            Some(state.shutdown_join_deadline.unwrap_or_else(|| {
                                tokio::time::Instant::now() + MCP_SHUTDOWN_TIMEOUT
                            }))
                        };
                        (CleanupRegistration::Sealed, deadline)
                    }
                }
            };
            (registration.0, registration.1, notify)
        };
        // spawn 在 state lock 外执行。
        if let Some(deadline) = worker_deadline {
            self.start_sealed_cleanup_worker(deadline)?;
        }
        notify.notify_waiters();
        Ok(registration)
    }

    /// 在锁外启动唯一的 Sealed cleanup worker。
    fn start_sealed_cleanup_worker(&self, deadline: tokio::time::Instant) -> Result<(), McpError> {
        if tokio::runtime::Handle::try_current().is_err() {
            // 先用无副作用的 runtime 探测覆盖同步调用，避免把 panic 当作正常 admission。
            self.recover_sealed_cleanup_worker(McpError::ShutdownFailed);
            tracing::debug!(
                event = "mcp_sealed_cleanup_worker_spawn_failed",
                error_code = McpError::ShutdownFailed.code(),
                "Sealed cleanup supervisor 启动失败"
            );
            return Err(McpError::ShutdownFailed);
        }
        let runtime = self.clone();
        // owner 必须在 spawn 前构造；任务若尚未 poll 就被取消时仍能 recovery。
        let worker_guard = SealedCleanupWorkerGuard::new(runtime.clone());
        let spawn_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tokio::spawn(async move {
                runtime
                    .run_sealed_cleanup_worker_with_guard(deadline, worker_guard)
                    .await;
            })
        }));
        match spawn_result {
            Ok(handle) => {
                let (queue_size, should_abort, abort_handle, notify) = {
                    let mut state = self.lock_state_for_cleanup();
                    // worker 可能已完成，不能复活 running。
                    state.sealed_cleanup_worker_starting = false;
                    let should_abort =
                        state.cleanup_admission_closed && state.sealed_cleanup_worker_running;
                    let abort_handle = handle.abort_handle();
                    state.cleanup_workers.push(handle);
                    (
                        state.sealed_cleanup_queue.len(),
                        should_abort,
                        abort_handle,
                        state.cleanup_worker_changed.clone(),
                    )
                };
                if should_abort {
                    // 关闭 admission 时锁外取消。
                    abort_handle.abort();
                    self.mark_cleanup_worker_failed();
                }
                notify.notify_waiters();
                // leader 可能在 spawn 窗口放弃；此处把刚登记的真实 handle 交给 reaper。
                self.maybe_start_abandoned_shutdown_reaper();
                tracing::debug!(
                    event = "mcp_sealed_cleanup_worker_started",
                    queue_size,
                    "启动 Sealed cleanup supervisor"
                );
                Ok(())
            }
            Err(_) => {
                // spawn panic 后归档 queue/in-flight。
                tracing::debug!(
                    event = "mcp_sealed_cleanup_worker_spawn_failed",
                    error_code = McpError::ShutdownFailed.code(),
                    "Sealed cleanup supervisor 启动失败"
                );
                self.recover_sealed_cleanup_worker(McpError::ShutdownFailed);
                Err(McpError::ShutdownFailed)
            }
        }
    }

    /// 使用共享 cleanup gate 和单一 deadline 串行消费 Sealed candidate 队列。
    #[cfg(test)]
    async fn run_sealed_cleanup_worker(&self, deadline: tokio::time::Instant) {
        let worker_guard = SealedCleanupWorkerGuard::new(self.clone());
        self.run_sealed_cleanup_worker_with_guard(deadline, worker_guard)
            .await;
    }

    /// 执行带 RAII recovery owner 的 Sealed cleanup supervisor。
    async fn run_sealed_cleanup_worker_with_guard(
        &self,
        deadline: tokio::time::Instant,
        mut worker_guard: SealedCleanupWorkerGuard,
    ) {
        tracing::debug!(
            event = "mcp_sealed_cleanup_worker_running",
            "Sealed cleanup supervisor 开始运行"
        );
        let gate_future = self.cleanup_gate.lock();
        tokio::pin!(gate_future);
        let cleanup_guard = match tokio::time::timeout_at(deadline, &mut gate_future).await {
            Ok(guard) => guard,
            Err(_) => {
                self.recover_sealed_cleanup_worker(McpError::CallTimeout);
                worker_guard.disarm();
                return;
            }
        };

        loop {
            let job = match self.take_sealed_cleanup_job() {
                Ok(Some(job)) => job,
                Ok(None) => {
                    #[cfg(debug_assertions)]
                    self.test_wait_if_enabled("sealed-cleanup-before-exit")
                        .await;
                    // 空闲时只标记收尾，保持 running 直到 admission 关闭并真正退出。
                    let idle = {
                        let mut state = self.lock_state_for_cleanup();
                        let idle = state.sealed_cleanup_queue.is_empty()
                            && state.sealed_cleanup_in_flight.is_none();
                        if idle {
                            state.sealed_cleanup_worker_exiting = true;
                        }
                        idle
                    };
                    #[cfg(debug_assertions)]
                    if idle {
                        self.test_wait_if_enabled("sealed-cleanup-after-exit-state")
                            .await;
                    }
                    if !idle {
                        continue;
                    }
                    match self.wait_for_sealed_cleanup_job_or_close(deadline).await {
                        SealedCleanupWorkerWait::Job => {
                            let notify = {
                                let mut state = self.lock_state_for_cleanup();
                                state.sealed_cleanup_worker_exiting = false;
                                state.cleanup_worker_changed.clone()
                            };
                            notify.notify_waiters();
                            continue;
                        }
                        SealedCleanupWorkerWait::AdmissionClosed => {
                            let (idle, notify) = {
                                let mut state = self.lock_state_for_cleanup();
                                let idle = state.sealed_cleanup_queue.is_empty()
                                    && state.sealed_cleanup_in_flight.is_none();
                                if idle {
                                    state.sealed_cleanup_worker_running = false;
                                }
                                state.sealed_cleanup_worker_exiting = false;
                                (idle, state.cleanup_worker_changed.clone())
                            };
                            notify.notify_waiters();
                            if !idle {
                                continue;
                            }
                            drop(cleanup_guard);
                            worker_guard.disarm();
                            tracing::debug!(event = "mcp_sealed_cleanup_worker_finished");
                            return;
                        }
                        SealedCleanupWorkerWait::Deadline => {
                            self.recover_sealed_cleanup_worker(McpError::CallTimeout);
                            worker_guard.disarm();
                            return;
                        }
                    }
                }
                Err(error) => {
                    tracing::debug!(
                        event = "mcp_sealed_cleanup_worker_state_unavailable",
                        error_code = error.code(),
                        "Sealed cleanup supervisor 状态不可用"
                    );
                    self.recover_sealed_cleanup_worker(error);
                    worker_guard.disarm();
                    return;
                }
            };
            let (result, attempts) = self.execute_sealed_cleanup_job(&job, deadline).await;
            finish_sealed_cleanup_job(self, job, result, attempts);
        }
    }

    /// worker 异常退出时归档队列和 in-flight，并完成所有 Sealed caller 的通知。
    fn recover_sealed_cleanup_worker(&self, error: McpError) {
        let (archived_job_count, notify) = {
            let mut state = self.lock_state_for_cleanup();
            let mut jobs = Vec::new();
            if let Some(job) = state.sealed_cleanup_in_flight.take() {
                jobs.push(job);
            }
            jobs.extend(state.sealed_cleanup_queue.drain(..));
            state.sealed_cleanup_worker_running = false;
            state.sealed_cleanup_worker_exiting = false;
            state.sealed_cleanup_worker_starting = false;
            state.cleanup_worker_failed = true;
            for job in &jobs {
                archive_sealed_cleanup_job_locked(&mut state, job.clone(), error, 0);
            }
            (jobs.len(), state.cleanup_worker_changed.clone())
        };
        notify.notify_waiters();
        // 若 leader 恰好在 worker spawn 窗口被取消，worker recovery 完成后补发 abandoned join。
        self.maybe_start_abandoned_shutdown_reaper();
        let _ = self.publish_abandoned_shutdown_if_idle();
        tracing::debug!(
            event = "mcp_sealed_cleanup_worker_recovered",
            error_code = error.code(),
            archived_job_count,
            "Sealed cleanup worker 异常出口已归档并通知"
        );
    }

    /// shutdown leader 被取消或 panic 时封存 runtime，并交给独立 owner 有界 join worker。
    fn recover_abandoned_shutdown(&self, completion: &Arc<ShutdownCompletion>, error: McpError) {
        let (abort_handles, reaper_handles, complete_now, notify) = {
            let mut state = self.lock_state_for_cleanup();
            state.shutting_down = true;
            state.cleanup_phase = CleanupPhase::Sealed;
            state.cleanup_admission_closed = true;
            state.shutdown_join_deadline = None;
            state.shutdown_abandoned = true;
            state.cleanup_cancellation.cancel();
            for token in state.active_calls.values() {
                token.cancel();
            }

            let reaper_already_running =
                state.abandoned_shutdown_reaper_running || state.abandoned_shutdown_reaper_starting;
            // 先把当前 registry 的真实 handle 转移给 reaper；不会把 JoinHandle 留在被取消
            // 的 leader future 的局部变量中，也不会在 state lock 内启动 Tokio task。
            let reaper_handles = if reaper_already_running || state.cleanup_worker_joining {
                // 现有 join owner 可能持有 local handle；registry 中的迟到 handle 必须等待同一 owner 收敛。
                Vec::new()
            } else {
                std::mem::take(&mut state.cleanup_workers)
            };
            let abort_handles = reaper_handles
                .iter()
                .map(tokio::task::JoinHandle::abort_handle)
                .collect::<Vec<_>>();
            let has_worker_owner = state.sealed_cleanup_worker_starting
                || state.cleanup_worker_joining
                || !state.cleanup_workers.is_empty()
                || reaper_already_running
                || !reaper_handles.is_empty();
            let job_sessions = state
                .sealed_cleanup_in_flight
                .iter()
                .chain(state.sealed_cleanup_queue.iter())
                .map(|job| Arc::clone(&job.session))
                .collect::<Vec<_>>();
            let mut orphaned = state
                .servers
                .values_mut()
                .filter_map(|entry| entry.session.take())
                .collect::<Vec<_>>();

            if has_worker_owner {
                // 已有 worker 会自行归档 queue/in-flight；这里只归档不属于 job 的孤立 candidate。
                let mut pending_orphaned = Vec::new();
                state.pending_cleanup_sessions.retain(|session| {
                    if job_sessions
                        .iter()
                        .any(|job_session| Arc::ptr_eq(job_session, session))
                    {
                        true
                    } else {
                        pending_orphaned.push(Arc::clone(session));
                        false
                    }
                });
                orphaned.extend(pending_orphaned);
                let mut claimed_orphaned = Vec::new();
                state.shutdown_owned_cleanup_sessions.retain(|session| {
                    if job_sessions
                        .iter()
                        .any(|job_session| Arc::ptr_eq(job_session, session))
                    {
                        true
                    } else {
                        claimed_orphaned.push(Arc::clone(session));
                        false
                    }
                });
                orphaned.extend(claimed_orphaned);
                if !reaper_handles.is_empty() {
                    // reaper 的两个标记覆盖 spawn 前后的窗口，防止 waiter 把 registry
                    // 暂时为空误判为 idle 并提前发布 completion。
                    state.abandoned_shutdown_reaper_starting = true;
                    state.abandoned_shutdown_reaper_running = true;
                }
            } else {
                // 没有可执行 owner 时立即归档所有 job，避免把损坏 registry 变成永久 waiter。
                let mut jobs = Vec::new();
                if let Some(job) = state.sealed_cleanup_in_flight.take() {
                    jobs.push(job);
                }
                jobs.extend(state.sealed_cleanup_queue.drain(..));
                state.sealed_cleanup_worker_running = false;
                state.sealed_cleanup_worker_exiting = false;
                state.sealed_cleanup_worker_starting = false;
                for job in &jobs {
                    archive_sealed_cleanup_job_locked(&mut state, job.clone(), error, 0);
                }
                orphaned.extend(std::mem::take(&mut state.pending_cleanup_sessions));
                orphaned.extend(std::mem::take(&mut state.shutdown_owned_cleanup_sessions));
            }

            orphaned.sort_by_key(|session| Arc::as_ptr(session) as usize);
            orphaned.dedup_by(|left, right| Arc::ptr_eq(left, right));
            for _ in &orphaned {
                record_terminal_cleanup_failure(&mut state, McpError::ShutdownFailed, error, 0);
            }
            if !orphaned.is_empty() {
                tracing::debug!(
                    event = "mcp_shutdown_abandoned_ownership_archived",
                    error_code = error.code(),
                    archived_session_count = orphaned.len(),
                    "shutdown leader 异常退出后已归档未完成 session owner"
                );
            }
            (
                abort_handles,
                reaper_handles,
                !has_worker_owner,
                state.cleanup_worker_changed.clone(),
            )
        };

        // 释放 state lock 后再 abort，避免 worker recovery 在锁内同步重入。
        for handle in &abort_handles {
            handle.abort();
        }
        notify.notify_waiters();
        if !reaper_handles.is_empty() {
            if let Err(start_error) =
                self.start_abandoned_shutdown_reaper(completion.clone(), error, reaper_handles)
            {
                tracing::debug!(
                    event = "mcp_shutdown_reaper_spawn_failed",
                    error_code = start_error.code(),
                    "abandoned shutdown reaper 启动失败，保留 JoinHandle 等待后续 owner"
                );
            }
        } else if complete_now {
            completion.complete(Err(error));
            tracing::debug!(
                event = "mcp_shutdown_abandoned_recovered",
                error_code = error.code(),
                "shutdown leader 异常退出且无剩余 worker，已发布稳定失败"
            );
        } else {
            tracing::debug!(
                event = "mcp_shutdown_abandoned_waiting_for_workers",
                error_code = error.code(),
                "shutdown leader 异常退出，等待 runtime-owned worker join 后发布稳定失败"
            );
        }
    }

    /// 为 abandoned recovery 启动唯一的 runtime-owned reaper，避免 leader Drop 丢失局部句柄。
    fn start_abandoned_shutdown_reaper(
        &self,
        completion: Arc<ShutdownCompletion>,
        error: McpError,
        handles: Vec<tokio::task::JoinHandle<()>>,
    ) -> Result<(), McpError> {
        if handles.is_empty() {
            self.finish_abandoned_shutdown_reaper(&completion, error, false);
            return Ok(());
        }
        if tokio::runtime::Handle::try_current().is_err() {
            // 没有 executor 时不能伪造“已 join”；guard Drop 会把真实句柄放回 registry。
            let owner = CleanupWorkerJoinGuard::new_abandoned_reaper(self.clone(), handles);
            drop(owner);
            self.mark_cleanup_worker_failed();
            return Err(McpError::ShutdownFailed);
        }

        let runtime = self.clone();
        // 在 spawn 前构造 owner；即使 spawn panic 或 future 尚未 poll，也不会 detached child。
        let owner = CleanupWorkerJoinGuard::new_abandoned_reaper(runtime.clone(), handles);
        let spawn_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tokio::spawn(async move {
                runtime
                    .run_abandoned_shutdown_reaper(completion, error, owner)
                    .await;
            })
        }));
        match spawn_result {
            Ok(handle) => {
                let notify = {
                    let mut state = self.lock_state_for_cleanup();
                    // reaper 可能已经完成；这里仅清除 starting，不能把 running 复活。
                    state.abandoned_shutdown_reaper_starting = false;
                    state.cleanup_workers.push(handle);
                    state.cleanup_worker_changed.clone()
                };
                notify.notify_waiters();
                Ok(())
            }
            Err(_) => {
                // owner 的 Drop 已恢复真实 child handles；返回值让 admission caller 可观察。
                self.mark_cleanup_worker_failed();
                Err(McpError::ShutdownFailed)
            }
        }
    }

    /// 在独立 bounded deadline 内 join abandoned recovery 的 child handles。
    async fn run_abandoned_shutdown_reaper(
        &self,
        completion: Arc<ShutdownCompletion>,
        error: McpError,
        mut owner: CleanupWorkerJoinGuard,
    ) {
        let deadline = tokio::time::Instant::now() + MCP_ABORT_JOIN_TIMEOUT;
        let mut all_terminated = true;
        while !owner.handles.is_empty() {
            let index = owner.handles.len() - 1;
            match tokio::time::timeout_at(deadline, &mut owner.handles[index]).await {
                Ok(Ok(())) => {
                    owner.handles.pop();
                }
                Ok(Err(_)) => {
                    owner.handles.pop();
                    self.mark_cleanup_worker_failed();
                }
                Err(_) => {
                    all_terminated = false;
                    self.mark_cleanup_worker_failed();
                    break;
                }
            }
        }
        let owner_empty = owner.handles.is_empty();
        drop(owner);
        if !owner_empty {
            tracing::debug!(
                event = "mcp_shutdown_reaper_bounded_out",
                error_code = McpError::ShutdownFailed.code(),
                "abandoned shutdown reaper 未能在二次 deadline 内 join 所有 worker"
            );
            return;
        }
        self.finish_abandoned_shutdown_reaper(&completion, error, !all_terminated);
    }

    /// child worker 全部 join 后才发布 abandoned shutdown completion，并归档残余 job。
    fn finish_abandoned_shutdown_reaper(
        &self,
        completion: &Arc<ShutdownCompletion>,
        error: McpError,
        join_failed: bool,
    ) {
        let final_result = {
            let mut state = self.lock_state_for_cleanup();
            let mut jobs = Vec::new();
            if let Some(job) = state.sealed_cleanup_in_flight.take() {
                jobs.push(job);
            }
            jobs.extend(state.sealed_cleanup_queue.drain(..));
            for job in &jobs {
                archive_sealed_cleanup_job_locked(&mut state, job.clone(), error, 0);
            }
            state.abandoned_shutdown_reaper_starting = false;
            state.abandoned_shutdown_reaper_running = false;
            state.sealed_cleanup_worker_starting = false;
            state.sealed_cleanup_worker_running = false;
            state.sealed_cleanup_worker_exiting = false;
            state.cleanup_admission_closed = true;
            state.cleanup_phase = CleanupPhase::Sealed;
            state.shutdown_join_deadline = None;
            if join_failed
                || state.cleanup_worker_failed
                || !state.terminal_cleanup_failures.is_empty()
            {
                Err(McpError::ShutdownFailed)
            } else {
                Err(error)
            }
        };
        completion.complete(final_result);
        tracing::debug!(
            event = "mcp_shutdown_reaper_completed",
            error_code = final_result
                .err()
                .unwrap_or(McpError::ShutdownFailed)
                .code(),
            "abandoned shutdown worker 已确认终止并发布稳定失败"
        );
    }

    /// leader 在 worker admission 交错窗口放弃后，补接管刚登记的真实 JoinHandle。
    fn maybe_start_abandoned_shutdown_reaper(&self) {
        let (completion, handles, notify) = {
            let mut state = self.lock_state_for_cleanup();
            if !state.shutdown_abandoned
                || state.abandoned_shutdown_reaper_running
                || state.abandoned_shutdown_reaper_starting
                || state.sealed_cleanup_worker_starting
                || state.cleanup_worker_joining
                || state.cleanup_workers.is_empty()
            {
                return;
            }
            state.abandoned_shutdown_reaper_starting = true;
            state.abandoned_shutdown_reaper_running = true;
            (
                state.shutdown.clone(),
                std::mem::take(&mut state.cleanup_workers),
                state.cleanup_worker_changed.clone(),
            )
        };
        for handle in &handles {
            handle.abort();
        }
        notify.notify_waiters();
        if let Some(completion) = completion {
            if let Err(error) =
                self.start_abandoned_shutdown_reaper(completion, McpError::ShutdownFailed, handles)
            {
                tracing::debug!(
                    event = "mcp_shutdown_reaper_spawn_failed",
                    error_code = error.code(),
                    "abandoned shutdown reaper 交接失败"
                );
            }
        } else {
            // 状态缺少 shared completion 时仍归还真实句柄，禁止无 owner detached。
            let owner = CleanupWorkerJoinGuard::new_abandoned_reaper(self.clone(), handles);
            drop(owner);
            self.mark_cleanup_worker_failed();
            tracing::debug!(
                event = "mcp_shutdown_reaper_completion_missing",
                error_code = McpError::ShutdownFailed.code(),
                "abandoned shutdown 缺少 shared completion，已保留 child handle"
            );
        }
    }

    /// 等待队列新 job 或 admission 关闭，避免空闲 worker 提前释放唯一 owner。
    async fn wait_for_sealed_cleanup_job_or_close(
        &self,
        deadline: tokio::time::Instant,
    ) -> SealedCleanupWorkerWait {
        loop {
            let notify = self.lock_state_for_cleanup().cleanup_worker_changed.clone();
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let state = {
                let state = self.lock_state_for_cleanup();
                if !state.sealed_cleanup_queue.is_empty()
                    || state.sealed_cleanup_in_flight.is_some()
                {
                    Some(SealedCleanupWorkerWait::Job)
                } else if state.cleanup_admission_closed {
                    Some(SealedCleanupWorkerWait::AdmissionClosed)
                } else {
                    None
                }
            };
            if let Some(state) = state {
                return state;
            }
            if tokio::time::timeout_at(deadline, &mut notified)
                .await
                .is_err()
            {
                let state = self.lock_state_for_cleanup();
                if !state.sealed_cleanup_queue.is_empty()
                    || state.sealed_cleanup_in_flight.is_some()
                {
                    return SealedCleanupWorkerWait::Job;
                }
                if state.cleanup_admission_closed {
                    return SealedCleanupWorkerWait::AdmissionClosed;
                }
                return SealedCleanupWorkerWait::Deadline;
            }
        }
    }

    /// 从队列取出单个 job 并记录 in-flight，供 worker 异常/取消时恢复 owner。
    fn take_sealed_cleanup_job(&self) -> Result<Option<SealedCleanupJob>, McpError> {
        let (job, notify) = {
            let mut state = self.lock_state_for_cleanup();
            let job = state.sealed_cleanup_queue.pop_front();
            state.sealed_cleanup_in_flight = job.clone();
            (job, state.cleanup_worker_changed.clone())
        };
        notify.notify_waiters();
        Ok(job)
    }

    /// 在一个共享 deadline 内对单个 Sealed job 做有限 DELETE 重试。
    async fn execute_sealed_cleanup_job(
        &self,
        job: &SealedCleanupJob,
        deadline: tokio::time::Instant,
    ) -> (Result<(), McpError>, usize) {
        let mut attempts = 0_usize;
        let mut last_error = McpError::CallTimeout;
        while attempts < MAX_SEALED_CLEANUP_ATTEMPTS {
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            attempts = attempts.saturating_add(1);
            #[cfg(debug_assertions)]
            self.test_wait_if_enabled("sealed-cleanup-before-delete")
                .await;
            let result = match Self::lock_http_session(&job.session, deadline, None).await {
                Ok(session) => close_http_session(&session, deadline, None).await,
                Err(error) => Err(error),
            };
            match result {
                Ok(()) => return (Ok(()), attempts),
                Err(error) => last_error = error,
            }
        }
        (Err(last_error), attempts)
    }

    /// shutdown 无法取得 cleanup gate 时，仍保留所有 session 的 shutdown ownership。
    fn retain_shutdown_cleanup_ownership(&self) {
        let mut state = self.lock_state_for_cleanup();
        let mut sessions = state
            .servers
            .values_mut()
            .filter_map(|entry| entry.session.take())
            .collect::<Vec<_>>();
        sessions.extend(std::mem::take(&mut state.pending_cleanup_sessions));
        claim_shutdown_cleanup_sessions(&mut state, &sessions);
        state.pending_cleanup_sessions.extend(sessions);
        state.cleanup_phase = CleanupPhase::Sealed;
        tracing::debug!(
            event = "mcp_shutdown_cleanup_ownership_retained",
            ownership_count = state.shutdown_owned_cleanup_sessions.len(),
            "MCP shutdown 已保留 cleanup ownership"
        );
    }

    /// 判断 candidate 是否仍可由 caller 执行 DELETE，或已被 shutdown 接管。
    fn candidate_cleanup_owner(
        &self,
        session: &Arc<tokio::sync::Mutex<McpHttpSession>>,
    ) -> Result<CandidateCleanupOwner, McpError> {
        let state = self.lock_state_for_cleanup();
        if !state
            .pending_cleanup_sessions
            .iter()
            .any(|pending| Arc::ptr_eq(pending, session))
        {
            // shutdown 成功关闭并移除该 handle 后，迟到的 candidate caller 已没有 DELETE 所有权。
            return Ok(CandidateCleanupOwner::Shutdown);
        }
        if state
            .shutdown_owned_cleanup_sessions
            .iter()
            .any(|owned| Arc::ptr_eq(owned, session))
        {
            return Ok(CandidateCleanupOwner::Shutdown);
        }
        Ok(match state.cleanup_phase {
            CleanupPhase::Open => {
                CandidateCleanupOwner::Candidate(Some(state.cleanup_cancellation.clone()))
            }
            // Sealed candidate 已在登记时获得 shutdown claim，由独立 worker 执行 DELETE。
            CleanupPhase::Sealed => CandidateCleanupOwner::Shutdown,
            CleanupPhase::Draining | CleanupPhase::Cleaning => CandidateCleanupOwner::Shutdown,
        })
    }

    /// 清除已成功 DELETE 的 provisional session。
    fn remove_pending_cleanup_session(&self, session: &Arc<tokio::sync::Mutex<McpHttpSession>>) {
        let mut state = self.lock_state_for_cleanup();
        state
            .pending_cleanup_sessions
            .retain(|candidate| !Arc::ptr_eq(candidate, session));
    }

    /// reinitialize 收到 header 但尚未构造完整 session 时，也必须保留 cleanup ownership。
    async fn cleanup_reinitialize_candidate(
        &self,
        client: &reqwest::Client,
        url: &str,
        session_id: Option<String>,
        deadline: tokio::time::Instant,
        cause: McpError,
    ) -> McpError {
        if let Some(session_id) = session_id.filter(|value| !value.is_empty()) {
            let session = provisional_http_session(client, url, session_id);
            self.cleanup_candidate_session(session, deadline, cause)
                .await;
        }
        cause
    }

    /// 停止新调用、取消 active calls，并在有界时间内释放所有 HTTP MCP sessions。
    pub async fn shutdown(&self) -> Result<(), McpError> {
        let shutdown_deadline = tokio::time::Instant::now() + MCP_SHUTDOWN_TIMEOUT;
        let (completion, leader, tokens, server_count) = {
            let mut state = self.lock_state_for_cleanup();
            if let Some(completion) = state.shutdown.clone() {
                (completion, false, Vec::new(), state.servers.len())
            } else {
                let completion = Arc::new(ShutdownCompletion::new());
                state.shutdown = Some(completion.clone());
                state.shutting_down = true;
                state.cleanup_phase = CleanupPhase::Draining;
                state.shutdown_join_deadline = Some(shutdown_deadline);
                // 中止 candidate caller 正在进行的 DELETE，让 shutdown 在自己的 deadline 内接管。
                state.cleanup_cancellation.cancel();
                let tokens = state.active_calls.values().cloned().collect::<Vec<_>>();
                (completion, true, tokens, state.servers.len())
            }
        };

        if !leader {
            // 重复/并发调用只等待自己的 bounded 窗口，不能成为永久 waiter。
            let wait_deadline = tokio::time::Instant::now() + MCP_SHUTDOWN_TIMEOUT;
            if let Some(result) = completion.wait_until(wait_deadline).await {
                let _ = self
                    .join_cleanup_workers(tokio::time::Instant::now() + MCP_ABORT_JOIN_TIMEOUT)
                    .await;
                return result;
            }
            tracing::debug!(
                event = "mcp_shutdown_wait_timeout",
                error_code = McpError::ShutdownFailed.code(),
                "并发 MCP shutdown waiter 达到独立 deadline"
            );
            let abandoned = self
                .state
                .lock()
                .map(|state| state.shutdown_abandoned)
                .unwrap_or(true);
            if abandoned {
                let joined = self
                    .join_cleanup_workers(tokio::time::Instant::now() + MCP_SHUTDOWN_TIMEOUT)
                    .await;
                if joined {
                    let _ = self.try_publish_shutdown_completion(
                        &completion,
                        Err(McpError::ShutdownFailed),
                    );
                }
            }
            return Err(McpError::ShutdownFailed);
        }

        // leader 被取消或 panic 时由 owner Drop 发布稳定失败，避免 waiter 永久等待。
        let mut leader_guard = ShutdownLeaderGuard::new(self.clone(), completion.clone());
        let mut result = self
            .perform_shutdown(tokens, server_count, shutdown_deadline)
            .await;
        if !self.join_cleanup_workers(shutdown_deadline).await
            || self.has_terminal_cleanup_failure()
        {
            result = Err(McpError::ShutdownFailed);
        }
        loop {
            #[cfg(debug_assertions)]
            self.test_wait_if_enabled("shutdown-before-completion")
                .await;
            if let Some(result) = self.try_publish_shutdown_completion(&completion, result) {
                leader_guard.disarm();
                return result;
            }
            if tokio::time::Instant::now() >= shutdown_deadline {
                let result = self
                    .force_publish_shutdown_completion(&completion, result)
                    .await;
                // 未 join 的 worker 仍由 leader guard 负责转入 abandoned recovery；不得提前
                // 解除 guard，否则后续 waiter 无法再接管真实 JoinHandle。
                if completion.is_completed() {
                    leader_guard.disarm();
                }
                return result;
            }
            if !self.join_cleanup_workers(shutdown_deadline).await
                || self.has_terminal_cleanup_failure()
            {
                result = Err(McpError::ShutdownFailed);
            }
        }
    }

    /// 执行唯一一次 cleanup；active call drain 和每个 session close 都有同一总 deadline。
    async fn perform_shutdown(
        &self,
        tokens: Vec<McpCancellationToken>,
        server_count: usize,
        deadline: tokio::time::Instant,
    ) -> Result<(), McpError> {
        let cancelled_call_count = tokens.len();
        for token in &tokens {
            token.cancel();
        }

        let active_drained = self.wait_for_active_calls(deadline).await;
        if !active_drained {
            tracing::debug!(
                event = "mcp_active_call_drain_failed",
                error_code = McpError::ShutdownFailed.code(),
                "MCP active call 未在清理边界内结束"
            );
        }

        // cleanup gate 让 candidate caller 与 shutdown owner 互斥；获取 gate 后再做 snapshot，
        // 因而 snapshot 之前已经完成的 candidate 不会被重复 DELETE。
        let gate_future = self.cleanup_gate.lock();
        tokio::pin!(gate_future);
        let cleanup_guard = match tokio::time::timeout_at(deadline, &mut gate_future).await {
            Ok(guard) => guard,
            Err(_) => {
                self.retain_shutdown_cleanup_ownership();
                tracing::debug!(
                    event = "mcp_cleanup_gate_failed",
                    error_code = McpError::ShutdownFailed.code(),
                    "MCP shutdown 获取 cleanup owner gate 超时"
                );
                return Err(McpError::ShutdownFailed);
            }
        };

        let mut sessions = {
            let mut state = self.lock_state_for_cleanup();
            state.cleanup_phase = CleanupPhase::Cleaning;
            let mut sessions = state
                .servers
                .values_mut()
                .filter_map(|entry| entry.session.take())
                .collect::<Vec<_>>();
            // snapshot 脱离 state 前先登记 owner，leader 中途取消时仍可统一归档。
            claim_shutdown_cleanup_sessions(&mut state, &sessions);
            let pending = std::mem::take(&mut state.pending_cleanup_sessions);
            claim_shutdown_cleanup_sessions(&mut state, &pending);
            sessions.extend(pending);
            sessions
        };
        #[cfg(debug_assertions)]
        self.test_wait_if_enabled("shutdown-snapshot").await;

        let mut failures = usize::from(!active_drained);
        let mut failed_sessions = Vec::new();
        loop {
            let (close_failures, mut batch_failures) =
                self.close_sessions(sessions, deadline).await;
            failures = failures.saturating_add(close_failures);
            failed_sessions.append(&mut batch_failures);

            // 迟到 candidate 只能在这个 gate 内登记/被接管；空检查与 Sealed 转换在同一
            // state 锁内完成，登记若与此交错则必然发生在 Sealed 之后，由 caller 自己负责。
            let next_sessions = {
                let mut state = self.lock_state_for_cleanup();
                let pending = std::mem::take(&mut state.pending_cleanup_sessions);
                if pending.is_empty() {
                    state.pending_cleanup_sessions.append(&mut failed_sessions);
                    state.cleanup_phase = CleanupPhase::Sealed;
                    None
                } else {
                    claim_shutdown_cleanup_sessions(&mut state, &pending);
                    Some(pending)
                }
            };
            let Some(next_sessions) = next_sessions else {
                break;
            };
            sessions = next_sessions;
        }
        drop(cleanup_guard);
        tracing::debug!(
            event = "mcp_runtime_shutdown",
            server_count,
            cancelled_call_count,
            failure_count = failures,
            "MCP runtime shutdown 完成"
        );
        if failures == 0 {
            Ok(())
        } else {
            Err(McpError::ShutdownFailed)
        }
    }

    /// 在 shutdown owner gate 内有界关闭一批 session，并返回失败句柄供最终 ownership 保留。
    async fn close_sessions(
        &self,
        sessions: Vec<Arc<tokio::sync::Mutex<McpHttpSession>>>,
        deadline: tokio::time::Instant,
    ) -> (usize, Vec<Arc<tokio::sync::Mutex<McpHttpSession>>>) {
        let mut failures = 0_usize;
        let mut failed_sessions = Vec::new();
        for session_handle in sessions {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                failures = failures.saturating_add(1);
                failed_sessions.push(Arc::clone(&session_handle));
                tracing::debug!(
                    event = "mcp_session_close_failed",
                    error_code = McpError::ShutdownFailed.code(),
                    "MCP session close 超过清理 deadline"
                );
                continue;
            }
            let session = match Self::lock_http_session(&session_handle, deadline, None).await {
                Ok(session) => session,
                Err(error) => {
                    failures = failures.saturating_add(1);
                    failed_sessions.push(Arc::clone(&session_handle));
                    tracing::debug!(
                        event = "mcp_session_close_failed",
                        error_code = error.code(),
                        "MCP session ownership 获取失败"
                    );
                    continue;
                }
            };
            match close_http_session(&session, deadline, None).await {
                Ok(()) => {
                    drop(session);
                    self.remove_queued_sealed_cleanup_job(&session_handle);
                    release_shutdown_cleanup_claim(self, &session_handle);
                    self.remove_pending_cleanup_session(&session_handle);
                }
                Err(error) => {
                    failures = failures.saturating_add(1);
                    failed_sessions.push(Arc::clone(&session_handle));
                    tracing::debug!(
                        event = "mcp_session_close_failed",
                        error_code = error.code(),
                        "MCP session close 失败"
                    );
                }
            }
        }
        (failures, failed_sessions)
    }

    /// abandoned leader 的 worker 全部 join 后发布 shared failure。
    fn publish_abandoned_shutdown_if_idle(&self) -> bool {
        let completion = {
            let mut state = self.lock_state_for_cleanup();
            if !state.shutdown_abandoned
                || !state.cleanup_workers.is_empty()
                || state.sealed_cleanup_worker_running
                || state.sealed_cleanup_worker_starting
                || state.abandoned_shutdown_reaper_running
                || state.abandoned_shutdown_reaper_starting
                || state.cleanup_worker_joining
                || !state.sealed_cleanup_queue.is_empty()
                || state.sealed_cleanup_in_flight.is_some()
            {
                return false;
            }
            state.cleanup_admission_closed = true;
            state.cleanup_phase = CleanupPhase::Sealed;
            state.shutdown_join_deadline = None;
            state.shutdown.clone()
        };
        if let Some(completion) = completion {
            completion.complete(Err(McpError::ShutdownFailed));
            tracing::debug!(
                event = "mcp_shutdown_abandoned_recovered",
                error_code = McpError::ShutdownFailed.code(),
                "abandoned shutdown worker 已 join 并发布稳定失败"
            );
            true
        } else {
            false
        }
    }

    /// 有界 join runtime-owned cleanup worker，并在异常/超时后归档其剩余 jobs。
    async fn join_cleanup_workers(&self, deadline: tokio::time::Instant) -> bool {
        loop {
            let (handles, admission_notify) = {
                let mut state = self.lock_state_for_cleanup();
                if state.cleanup_worker_joining {
                    // 已有 owner 时，即使 registry 后来收到新 handle，也必须由同一 owner 统一接管。
                    (Vec::new(), None)
                } else {
                    let handles = std::mem::take(&mut state.cleanup_workers);
                    let needs_admission_close = !handles.is_empty()
                        || state.sealed_cleanup_worker_running
                        || state.sealed_cleanup_worker_starting
                        || !state.sealed_cleanup_queue.is_empty()
                        || state.sealed_cleanup_in_flight.is_some();
                    let admission_notify = needs_admission_close.then(|| {
                        // 已进入最终 join 阶段；同锁关闭 admission，防止 worker 退出窗口再入队。
                        state.cleanup_admission_closed = true;
                        state.cleanup_worker_changed.clone()
                    });
                    if !handles.is_empty() {
                        // 只有一个 join owner 能持有 local handle；其他 waiter 必须等待其归还。
                        state.cleanup_worker_joining = true;
                    }
                    (handles, admission_notify)
                }
            };
            if let Some(notify) = admission_notify {
                notify.notify_waiters();
            }
            if handles.is_empty() {
                let (idle, starting, joining, notify) = {
                    let state = self.lock_state_for_cleanup();
                    (
                        !state.sealed_cleanup_worker_running
                            && !state.sealed_cleanup_worker_starting
                            && !state.abandoned_shutdown_reaper_running
                            && !state.abandoned_shutdown_reaper_starting
                            && !state.cleanup_worker_joining
                            && state.sealed_cleanup_queue.is_empty()
                            && state.sealed_cleanup_in_flight.is_none(),
                        state.sealed_cleanup_worker_starting
                            || state.abandoned_shutdown_reaper_starting
                            || state.abandoned_shutdown_reaper_running,
                        state.cleanup_worker_joining,
                        state.cleanup_worker_changed.clone(),
                    )
                };
                if idle {
                    self.publish_abandoned_shutdown_if_idle();
                    return true;
                }
                if joining {
                    // 另一个 join owner 的 local handle 仍未归还；等待其明确释放，禁止抢先归档 job。
                    let notified = notify.notified();
                    if tokio::time::timeout_at(deadline, notified).await.is_err() {
                        tracing::debug!(
                            event = "mcp_sealed_cleanup_worker_join_wait_bounded_out",
                            error_code = McpError::ShutdownFailed.code(),
                            "已有 cleanup join owner 未在当前 join deadline 内释放"
                        );
                        return false;
                    }
                    continue;
                }
                if starting {
                    let notified = notify.notified();
                    if tokio::time::timeout_at(deadline, notified).await.is_err() {
                        self.mark_cleanup_worker_failed();
                        tracing::debug!(
                            event = "mcp_sealed_cleanup_worker_start_bounded_out",
                            error_code = McpError::ShutdownFailed.code(),
                            "Sealed cleanup supervisor 启动未在 join deadline 内结束"
                        );
                        return false;
                    }
                    continue;
                }
                // 没有可 join 的 handle 但仍有 job，说明 owner registry 已损坏；归档而不等待。
                archive_all_sealed_cleanup_jobs(self, McpError::StateUnavailable);
                return false;
            }

            let mut owner = CleanupWorkerJoinGuard::new(self.clone(), handles);
            let mut all_terminated = true;
            while !owner.handles.is_empty() {
                let index = owner.handles.len() - 1;
                match tokio::time::timeout_at(deadline, &mut owner.handles[index]).await {
                    Ok(Ok(())) => {
                        owner.handles.pop();
                    }
                    Ok(Err(_)) => {
                        owner.handles.pop();
                        self.mark_cleanup_worker_failed();
                        tracing::debug!(
                            event = "mcp_sealed_cleanup_worker_join_failed",
                            error_code = McpError::ShutdownFailed.code(),
                            "Sealed cleanup worker 异常结束"
                        );
                    }
                    Err(_) => {
                        self.mark_cleanup_worker_failed();
                        let abort_deadline = tokio::time::Instant::now() + MCP_ABORT_JOIN_TIMEOUT;
                        for handle in &owner.handles {
                            handle.abort();
                        }
                        while !owner.handles.is_empty() {
                            let index = owner.handles.len() - 1;
                            match tokio::time::timeout_at(abort_deadline, &mut owner.handles[index])
                                .await
                            {
                                Ok(Ok(())) | Ok(Err(_)) => {
                                    owner.handles.pop();
                                }
                                Err(_) => {
                                    all_terminated = false;
                                    break;
                                }
                            }
                        }
                        if !all_terminated {
                            tracing::debug!(
                                event = "mcp_sealed_cleanup_worker_join_bounded_out",
                                error_code = McpError::ShutdownFailed.code(),
                                "Sealed cleanup worker 二次 join 仍未在边界内结束"
                            );
                        }
                        break;
                    }
                }
            }
            let owner_empty = owner.handles.is_empty();
            drop(owner);
            if !owner_empty {
                return false;
            }
            // 只有所有 JoinHandle 已确认终止后，才允许回收 worker 的 in-flight 状态。
            if self.has_terminal_cleanup_failure() {
                archive_all_sealed_cleanup_jobs(self, McpError::ShutdownFailed);
            }
            self.publish_abandoned_shutdown_if_idle();
            if !all_terminated {
                return false;
            }
        }
    }

    /// 记录 worker 异常，确保无 job 的 panic/abort 也使 shutdown fail-closed。
    fn mark_cleanup_worker_failed(&self) {
        let notify = {
            let mut state = self.lock_state_for_cleanup();
            state.cleanup_worker_failed = true;
            state.cleanup_worker_changed.clone()
        };
        notify.notify_waiters();
    }

    /// 返回本轮 runtime 是否已经出现显式 cleanup 终态失败。
    fn has_terminal_cleanup_failure(&self) -> bool {
        self.state
            .lock()
            .map(|state| state.cleanup_worker_failed || !state.terminal_cleanup_failures.is_empty())
            .unwrap_or(true)
    }

    /// 从 worker 队列移除已被 shutdown 成功关闭的 job，并通知其 caller。
    fn remove_queued_sealed_cleanup_job(&self, session: &Arc<tokio::sync::Mutex<McpHttpSession>>) {
        let mut state = self.lock_state_for_cleanup();
        let mut removed = Vec::new();
        state.sealed_cleanup_queue.retain(|job| {
            if Arc::ptr_eq(&job.session, session) {
                removed.push(job.completion.clone());
                false
            } else {
                true
            }
        });
        drop(state);
        for completion in removed {
            completion.complete();
        }
    }

    /// 在没有未 join worker/job 时发布 shared shutdown completion，锁住最后竞态窗口。
    fn try_publish_shutdown_completion(
        &self,
        completion: &Arc<ShutdownCompletion>,
        result: Result<(), McpError>,
    ) -> Option<Result<(), McpError>> {
        let mut state = self.lock_state_for_cleanup();
        let idle = state.cleanup_workers.is_empty()
            && !state.sealed_cleanup_worker_running
            && !state.sealed_cleanup_worker_starting
            && !state.abandoned_shutdown_reaper_running
            && !state.abandoned_shutdown_reaper_starting
            && !state.cleanup_worker_joining
            && state.sealed_cleanup_queue.is_empty()
            && state.sealed_cleanup_in_flight.is_none();
        if !idle {
            return None;
        }
        // barrier、phase、deadline 和 completion 在同一 state lock 内完成，拒绝 late worker。
        state.cleanup_admission_closed = true;
        state.cleanup_phase = CleanupPhase::Sealed;
        state.shutdown_join_deadline = None;
        let published_result =
            if state.cleanup_worker_failed || !state.terminal_cleanup_failures.is_empty() {
                Err(McpError::ShutdownFailed)
            } else {
                result
            };
        completion.complete(published_result);
        drop(state);
        Some(published_result)
    }

    /// shutdown deadline 已耗尽时，先关闭 admission，再以独立短预算 bounded join。
    async fn force_publish_shutdown_completion(
        &self,
        completion: &Arc<ShutdownCompletion>,
        result: Result<(), McpError>,
    ) -> Result<(), McpError> {
        {
            let mut state = self.lock_state_for_cleanup();
            state.cleanup_admission_closed = true;
            state.cleanup_phase = CleanupPhase::Sealed;
        }
        let joined = self
            .join_cleanup_workers(tokio::time::Instant::now() + MCP_ABORT_JOIN_TIMEOUT)
            .await;
        if !joined {
            // 未确认 JoinHandle 终止前不得归档 in-flight 或发布 completion；leader guard
            // 会在返回时把 abandoned 状态交给后续 bounded shutdown waiter。
            tracing::debug!(
                event = "mcp_shutdown_completion_deferred",
                error_code = McpError::ShutdownFailed.code(),
                "MCP shutdown worker 未在二次 join deadline 内终止"
            );
            return Err(McpError::ShutdownFailed);
        }

        let final_result = {
            let mut state = self.lock_state_for_cleanup();
            let mut jobs = Vec::new();
            if let Some(job) = state.sealed_cleanup_in_flight.take() {
                jobs.push(job);
            }
            jobs.extend(state.sealed_cleanup_queue.drain(..));
            for job in &jobs {
                archive_sealed_cleanup_job_locked(
                    &mut state,
                    job.clone(),
                    McpError::CallTimeout,
                    0,
                );
            }
            state.sealed_cleanup_worker_running = false;
            state.sealed_cleanup_worker_exiting = false;
            state.sealed_cleanup_worker_starting = false;
            state.shutdown_join_deadline = None;
            if result.is_err() {
                result
            } else {
                Err(McpError::ShutdownFailed)
            }
        };
        completion.complete(final_result);
        tracing::debug!(
            event = "mcp_shutdown_completion_forced",
            error_code = final_result
                .err()
                .unwrap_or(McpError::ShutdownFailed)
                .code(),
            "MCP shutdown 已在有界 deadline 后发布最终结果"
        );
        final_result
    }

    /// 等待所有 active call 从 registry 移除，避免 close 与 in-flight request 并发竞态。
    async fn wait_for_active_calls(&self, deadline: tokio::time::Instant) -> bool {
        loop {
            let notify = self.lock_state_for_cleanup().active_changed.clone();
            let notified = notify.notified();
            let empty = self.lock_state_for_cleanup().active_calls.is_empty();
            if empty {
                return true;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return self.lock_state_for_cleanup().active_calls.is_empty();
            }
        }
    }

    /// 返回当前 active call 数量，只用于稳定数量日志。
    fn active_call_count(&self) -> usize {
        self.state
            .lock()
            .map(|state| state.active_calls.len())
            .unwrap_or(0)
    }

    /// 清除发生不可恢复错误的共享 session，并保留 catalog 作为 Host 审计快照。
    fn invalidate_session(
        &self,
        server_name: &str,
        session: &Arc<tokio::sync::Mutex<McpHttpSession>>,
    ) {
        let mut state = self.lock_state_for_cleanup();
        let Some(entry) = state.servers.get_mut(server_name) else {
            return;
        };
        let same_session = entry
            .session
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, session));
        if same_session {
            entry.session = None;
            // actual tools 保留用于 Host 审计，但 status 非 ready 后不再进入模型/调用交集。
            entry.catalog.session.status = "error".to_owned();
            tracing::debug!(
                event = "mcp_session_invalidated",
                error_code = McpError::ToolNotReady.code(),
                "MCP session 已标记为不可用"
            );
        }
    }

    /// 从 active registry 移除 call，并唤醒 shutdown drain；锁异常只记录稳定错误码。
    fn finish_active_call(&self, call_id: u64) {
        let notify = {
            let mut state = self.lock_state_for_cleanup();
            state.active_calls.remove(&call_id);
            state.active_changed.clone()
        };
        notify.notify_waiters();
    }

    /// 记录稳定失败事件，并不回显任何底层错误细节。
    fn finish_call_error<T>(&self, error: McpError) -> Result<T, McpError> {
        tracing::debug!(event = "mcp_call_failed", error_code = error.code());
        Err(error)
    }
}

/// 完成一个 Sealed job；成功和有限失败都必须移除 active ownership 并通知 caller。
fn finish_sealed_cleanup_job(
    runtime: &McpRuntime,
    job: SealedCleanupJob,
    result: Result<(), McpError>,
    attempts: usize,
) {
    let mut state = runtime.lock_state_for_cleanup();
    state
        .pending_cleanup_sessions
        .retain(|pending| !Arc::ptr_eq(pending, &job.session));
    state
        .shutdown_owned_cleanup_sessions
        .retain(|owned| !Arc::ptr_eq(owned, &job.session));
    if state
        .sealed_cleanup_in_flight
        .as_ref()
        .is_some_and(|in_flight| Arc::ptr_eq(&in_flight.session, &job.session))
    {
        state.sealed_cleanup_in_flight = None;
    }
    if let Err(error) = result {
        record_terminal_cleanup_failure(&mut state, job.cause, error, attempts);
        tracing::debug!(
            event = "mcp_sealed_candidate_cleanup_terminal",
            cause_code = job.cause.code(),
            error_code = error.code(),
            attempts,
            "Sealed candidate cleanup 进入有界终态"
        );
    } else {
        tracing::debug!(
            event = "mcp_sealed_candidate_cleanup_succeeded",
            attempts,
            "Sealed candidate cleanup 完成"
        );
    }
    drop(state);
    // 即使 caller 已取消，完成状态仍由 runtime-owned worker 可靠发布。
    job.completion.complete();
}

/// 将尚未完成的 Sealed jobs 统一归档，覆盖 gate/session lock/worker abort 等失败出口。
fn archive_all_sealed_cleanup_jobs(runtime: &McpRuntime, error: McpError) {
    let mut state = runtime.lock_state_for_cleanup();
    let mut jobs = Vec::new();
    if let Some(job) = state.sealed_cleanup_in_flight.take() {
        jobs.push(job);
    }
    jobs.extend(state.sealed_cleanup_queue.drain(..));
    state.sealed_cleanup_worker_running = false;
    state.sealed_cleanup_worker_exiting = false;
    for job in &jobs {
        archive_sealed_cleanup_job_locked(&mut state, job.clone(), error, 0);
    }
    tracing::debug!(
        event = "mcp_sealed_cleanup_jobs_archived",
        error_code = error.code(),
        archived_job_count = jobs.len(),
        "Sealed cleanup jobs 已统一归档"
    );
}

/// 在持有 state lock 时移除 pending/claim，并写入固定容量的终态审计记录。
fn archive_sealed_cleanup_job_locked(
    state: &mut McpRuntimeState,
    job: SealedCleanupJob,
    error: McpError,
    attempts: usize,
) {
    state
        .pending_cleanup_sessions
        .retain(|pending| !Arc::ptr_eq(pending, &job.session));
    state
        .shutdown_owned_cleanup_sessions
        .retain(|owned| !Arc::ptr_eq(owned, &job.session));
    if state
        .sealed_cleanup_in_flight
        .as_ref()
        .is_some_and(|in_flight| Arc::ptr_eq(&in_flight.session, &job.session))
    {
        state.sealed_cleanup_in_flight = None;
    }
    record_terminal_cleanup_failure(state, job.cause, error, attempts);
    job.completion.complete();
}

/// 以固定容量保留稳定 cleanup 失败记录，防止错误风暴耗尽 runtime 内存。
fn record_terminal_cleanup_failure(
    state: &mut McpRuntimeState,
    cause: McpError,
    error: McpError,
    attempts: usize,
) {
    if state.terminal_cleanup_failures.len() >= MAX_TERMINAL_CLEANUP_FAILURES {
        state.terminal_cleanup_failures.pop_front();
    }
    state
        .terminal_cleanup_failures
        .push_back(CleanupTerminalFailure {
            cause,
            error,
            attempts,
        });
}

/// 把一批 provisional handle 标记为 shutdown owner，并去重 pointer identity。
fn claim_shutdown_cleanup_sessions(
    state: &mut McpRuntimeState,
    sessions: &[Arc<tokio::sync::Mutex<McpHttpSession>>],
) {
    for session in sessions {
        if !state
            .shutdown_owned_cleanup_sessions
            .iter()
            .any(|owned| Arc::ptr_eq(owned, session))
        {
            state
                .shutdown_owned_cleanup_sessions
                .push(Arc::clone(session));
        }
    }
}

/// 释放成功关闭的 shutdown claim；失败 claim 保留以阻止 caller 越过 completion 重试。
fn release_shutdown_cleanup_claim(
    runtime: &McpRuntime,
    session: &Arc<tokio::sync::Mutex<McpHttpSession>>,
) {
    let mut state = runtime.lock_state_for_cleanup();
    state
        .shutdown_owned_cleanup_sessions
        .retain(|owned| !Arc::ptr_eq(owned, session));
}

/// 构造一个非 ready catalog；不把底层失败正文写给 Host。
fn error_catalog_entry(server_name: String) -> McpServerEntry {
    McpServerEntry {
        catalog: McpServerCatalog {
            name: server_name,
            session: McpSessionCatalog {
                status: "error".to_owned(),
                tools: Vec::new(),
            },
        },
        session: None,
    }
}

/// 建立一个 HTTP MCP session，并把 initialize、initialized、tools/list 限制在同一 deadline。
async fn connect_http_session(
    runtime: &McpRuntime,
    client: &reqwest::Client,
    url: &str,
    server_name: &str,
    deadline: tokio::time::Instant,
) -> Result<(McpHttpSession, Vec<McpCatalogTool>), McpError> {
    let initialize_id = Value::from(1_u64);
    let initialize = initialize_request_message(initialize_id.clone());
    tracing::debug!(
        event = "mcp_http_session_initializing",
        "初始化 MCP HTTP session"
    );

    // 先捕获 header，再校验 JSON；这样 malformed initialize 也能构造 cleanup session。
    let mut captured_session_id = None;
    let initialize_response = match post_json_rpc(
        client,
        url,
        None,
        &initialize,
        Some(&initialize_id),
        deadline,
        None,
        Some(&mut captured_session_id),
    )
    .await
    {
        Ok(Some(response)) => response,
        Ok(None) => {
            let error = McpError::InitializationFailed;
            if let Some(session_id) = captured_session_id.take() {
                let session = provisional_http_session(client, url, session_id);
                runtime
                    .cleanup_candidate_session(session, deadline, error)
                    .await;
            }
            return Err(error);
        }
        Err(error) => {
            let error = map_initialization_error(error);
            if let Some(session_id) = captured_session_id.take() {
                let session = provisional_http_session(client, url, session_id);
                runtime
                    .cleanup_candidate_session(session, deadline, error)
                    .await;
            }
            return Err(error);
        }
    };

    let Some(session_id) = initialize_response
        .session_id
        .clone()
        .or(captured_session_id.take())
        .filter(|value| !value.is_empty())
    else {
        return Err(McpError::InitializationFailed);
    };
    let session = provisional_http_session(client, url, session_id);
    let initialize_result = match initialize_response.into_result(initialize_id) {
        Ok(result) => result,
        Err(error) => {
            let error = map_initialization_error(error);
            runtime
                .cleanup_candidate_session(session, deadline, error)
                .await;
            return Err(error);
        }
    };
    if initialize_result
        .get("protocolVersion")
        .and_then(Value::as_str)
        != Some(MCP_PROTOCOL_VERSION)
    {
        let error = McpError::InitializationFailed;
        runtime
            .cleanup_candidate_session(session, deadline, error)
            .await;
        return Err(error);
    }

    let initialized = json!({
        "jsonrpc": "2.0",
        "method": "notifications/initialized",
        "params": {}
    });
    match post_json_rpc(
        &session.client,
        &session.url,
        Some(&session),
        &initialized,
        None,
        deadline,
        None,
        None,
    )
    .await
    .map_err(map_initialization_error)
    {
        Ok(None) => {}
        Ok(Some(_)) => {
            // notification 不允许返回 JSON-RPC response；异常 response 不能提交 ready session。
            let error = McpError::InitializationFailed;
            runtime
                .cleanup_candidate_session(session, deadline, error)
                .await;
            return Err(error);
        }
        Err(error) => {
            runtime
                .cleanup_candidate_session(session, deadline, error)
                .await;
            return Err(error);
        }
    }

    // MCP tools/list 的 cursor 是 opaque；只允许有界、非空且不重复的后续 cursor。
    let mut next_cursor: Option<String> = None;
    let mut seen_cursors = BTreeSet::new();
    let mut actual_tools = BTreeMap::new();
    let mut catalog_bytes = 0_usize;
    for _ in 0..MAX_TOOLS_LIST_PAGES {
        let tools_id = match session.next_request_id().map_err(map_initialization_error) {
            Ok(id) => id,
            Err(error) => {
                runtime
                    .cleanup_candidate_session(session, deadline, error)
                    .await;
                return Err(error);
            }
        };
        let mut params = Map::new();
        if let Some(cursor) = next_cursor.as_ref() {
            params.insert("cursor".to_owned(), Value::String(cursor.clone()));
        }
        let tools_request = json!({
            "jsonrpc": "2.0",
            "id": tools_id,
            "method": "tools/list",
            "params": params,
        });
        let tools_response = match post_json_rpc(
            &session.client,
            &session.url,
            Some(&session),
            &tools_request,
            Some(&tools_id),
            deadline,
            None,
            None,
        )
        .await
        {
            Ok(Some(response)) => response,
            Ok(None) => {
                let error = McpError::InitializationFailed;
                runtime
                    .cleanup_candidate_session(session, deadline, error)
                    .await;
                return Err(error);
            }
            Err(error) => {
                let error = map_initialization_error(error);
                runtime
                    .cleanup_candidate_session(session, deadline, error)
                    .await;
                return Err(error);
            }
        };
        let tools_result = match tools_response.into_result(tools_id) {
            Ok(result) => result,
            Err(error) => {
                let error = map_initialization_error(error);
                runtime
                    .cleanup_candidate_session(session, deadline, error)
                    .await;
                return Err(error);
            }
        };
        let (tools, returned_cursor) = match parse_tools_page(tools_result) {
            Ok(page) => page,
            Err(error) => {
                let error = map_initialization_error(error);
                runtime
                    .cleanup_candidate_session(session, deadline, error)
                    .await;
                return Err(error);
            }
        };
        for mut tool in tools {
            if actual_tools.contains_key(&tool.name) {
                let error = McpError::InitializationFailed;
                runtime
                    .cleanup_candidate_session(session, deadline, error)
                    .await;
                return Err(error);
            }
            if actual_tools.len() >= MAX_MCP_CATALOG_TOOLS {
                let error = McpError::CatalogTooLarge;
                tracing::debug!(
                    event = "mcp_catalog_tool_limit",
                    error_code = error.code(),
                    "MCP actual catalog 工具数量超过上限"
                );
                runtime
                    .cleanup_candidate_session(session, deadline, error)
                    .await;
                return Err(error);
            }
            let qualified = qualified_tool_name(server_name, &tool.name);
            if !contract_is_qualified_tool_name(&qualified) {
                // actual catalog 仍保留原始名称，但 disabled 使 Host/model/call 共用 contract gate。
                tool.enabled = false;
            }
            let qualified_length = server_name
                .len()
                .checked_add(MCP_TOOL_SEPARATOR.len())
                .and_then(|length| length.checked_add(tool.name.len()));
            if qualified_length.is_none_or(|length| length > MAX_RECORD_ID_BYTES) {
                // 持久化边界独立于 qualified-name syntax，超限名称仍只保留审计视图。
                tool.enabled = false;
            }
            let tool_bytes = match serde_json::to_vec(&tool) {
                Ok(bytes) => bytes,
                Err(_) => {
                    let error = McpError::InitializationFailed;
                    runtime
                        .cleanup_candidate_session(session, deadline, error)
                        .await;
                    return Err(error);
                }
            };
            let Some(next_catalog_bytes) = catalog_bytes.checked_add(tool_bytes.len()) else {
                let error = McpError::CatalogTooLarge;
                runtime
                    .cleanup_candidate_session(session, deadline, error)
                    .await;
                return Err(error);
            };
            if next_catalog_bytes > MAX_MCP_CATALOG_BYTES {
                let error = McpError::CatalogTooLarge;
                tracing::debug!(
                    event = "mcp_catalog_metadata_limit",
                    error_code = error.code(),
                    "MCP actual catalog 元数据累计大小超过上限"
                );
                runtime
                    .cleanup_candidate_session(session, deadline, error)
                    .await;
                return Err(error);
            }
            catalog_bytes = next_catalog_bytes;
            actual_tools.insert(tool.name.clone(), tool);
        }
        match returned_cursor {
            None => return Ok((session, actual_tools.into_values().collect())),
            Some(cursor) if cursor.is_empty() => {
                let error = McpError::InitializationFailed;
                runtime
                    .cleanup_candidate_session(session, deadline, error)
                    .await;
                return Err(error);
            }
            Some(cursor) if cursor.len() > MAX_MCP_CATALOG_CURSOR_BYTES => {
                let error = McpError::CatalogTooLarge;
                tracing::debug!(
                    event = "mcp_catalog_cursor_limit",
                    error_code = error.code(),
                    "MCP tools/list cursor 超过长度上限"
                );
                runtime
                    .cleanup_candidate_session(session, deadline, error)
                    .await;
                return Err(error);
            }
            Some(_cursor) if seen_cursors.len() >= MAX_MCP_CATALOG_CURSORS => {
                let error = McpError::CatalogTooLarge;
                tracing::debug!(
                    event = "mcp_catalog_cursor_count_limit",
                    error_code = error.code(),
                    "MCP tools/list cursor 数量超过上限"
                );
                runtime
                    .cleanup_candidate_session(session, deadline, error)
                    .await;
                return Err(error);
            }
            Some(cursor) if !seen_cursors.insert(cursor.clone()) => {
                let error = McpError::InitializationFailed;
                runtime
                    .cleanup_candidate_session(session, deadline, error)
                    .await;
                return Err(error);
            }
            Some(cursor) => {
                let Some(next_catalog_bytes) = catalog_bytes.checked_add(cursor.len()) else {
                    let error = McpError::CatalogTooLarge;
                    runtime
                        .cleanup_candidate_session(session, deadline, error)
                        .await;
                    return Err(error);
                };
                if next_catalog_bytes > MAX_MCP_CATALOG_BYTES {
                    let error = McpError::CatalogTooLarge;
                    tracing::debug!(
                        event = "mcp_catalog_cursor_bytes_limit",
                        error_code = error.code(),
                        "MCP tools/list cursor 累计大小超过上限"
                    );
                    runtime
                        .cleanup_candidate_session(session, deadline, error)
                        .await;
                    return Err(error);
                }
                catalog_bytes = next_catalog_bytes;
                next_cursor = Some(cursor);
            }
        }
    }

    let error = McpError::InitializationFailed;
    runtime
        .cleanup_candidate_session(session, deadline, error)
        .await;
    Err(error)
}

/// 构造固定 MCP initialize 请求，重握手与首次握手使用相同能力声明。
fn initialize_request_message(request_id: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "initialize",
        "params": {
            "protocolVersion": MCP_PROTOCOL_VERSION,
            "capabilities": {"tools": {}},
            "clientInfo": {
                "name": "efflab-agent-sidecar",
                "version": env!("CARGO_PKG_VERSION"),
            }
        }
    })
}

/// 用固定允许协议版本创建仅用于 bounded cleanup 的 provisional session。
fn provisional_http_session(
    client: &reqwest::Client,
    url: &str,
    session_id: String,
) -> McpHttpSession {
    McpHttpSession {
        client: client.clone(),
        url: url.to_owned(),
        session_id,
        protocol_version: MCP_PROTOCOL_VERSION.to_owned(),
        next_request_id: Arc::new(AtomicU64::new(2)),
        available: true,
    }
}

/// 将 runtime 初始化中的 call/transport 错误收敛为初始化稳定分类。
fn map_initialization_error(error: McpError) -> McpError {
    match error {
        McpError::CallTimeout | McpError::InitializationTimeout => McpError::InitializationTimeout,
        McpError::OutputTooLarge => McpError::OutputTooLarge,
        McpError::StdioUnavailable
        | McpError::InvalidUrl
        | McpError::InvalidServerName
        | McpError::HttpClientUnavailable
        | McpError::CatalogTooLarge => error,
        _ => McpError::InitializationFailed,
    }
}

/// 解析单页 tools/list；重复或畸形条目、非法 nextCursor 都使整个 server 非 ready。
fn parse_tools_page(result: Value) -> Result<(Vec<McpCatalogTool>, Option<String>), McpError> {
    let object = result.as_object().ok_or(McpError::InitializationFailed)?;
    let tools = object
        .get("tools")
        .and_then(Value::as_array)
        .ok_or(McpError::InitializationFailed)?;
    let next_cursor = match object.get("nextCursor") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) => Some(value.clone()),
        Some(_) => return Err(McpError::InitializationFailed),
    };
    let mut parsed = BTreeMap::new();
    for tool in tools {
        let object = tool.as_object().ok_or(McpError::InitializationFailed)?;
        let name = object
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty())
            .ok_or(McpError::InitializationFailed)?
            .to_owned();
        let description = match object.get("description") {
            None | Some(Value::Null) => None,
            Some(Value::String(value)) => Some(value.clone()),
            Some(_) => return Err(McpError::InitializationFailed),
        };
        let input_schema = match object.get("inputSchema") {
            None | Some(Value::Null) => Some(json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            })),
            Some(schema) if schema.is_object() => Some(schema.clone()),
            Some(_) => return Err(McpError::InitializationFailed),
        };
        let catalog_tool = McpCatalogTool {
            name: name.clone(),
            // 实际条目保留给 Host 审计，但非法 ASCII segment 不得进入 ready catalog。
            enabled: is_tool_name_segment(&name),
            description,
            input_schema,
        };
        if parsed.insert(name, catalog_tool).is_some() {
            return Err(McpError::InitializationFailed);
        }
    }
    Ok((parsed.into_values().collect(), next_cursor))
}

/// 发送一个 JSON-RPC HTTP message；body cap 位于 serde JSON response 解码之前。
async fn post_json_rpc(
    client: &reqwest::Client,
    url: &str,
    session: Option<&McpHttpSession>,
    message: &Value,
    expected_id: Option<&Value>,
    deadline: tokio::time::Instant,
    cancellation: Option<McpCancellationToken>,
    session_id_capture: Option<&mut Option<String>>,
) -> Result<Option<McpRpcResponse>, McpError> {
    let body = serde_json::to_vec(message).map_err(|_| McpError::CallFailed)?;
    if body.len() > MAX_MCP_REQUEST_BODY_BYTES {
        return Err(McpError::InvalidArguments);
    }
    let mut request = client
        .post(url)
        .header("Accept", format!("{JSON_CONTENT_TYPE}, {SSE_CONTENT_TYPE}"))
        .header("Content-Type", JSON_CONTENT_TYPE)
        .body(body);
    if let Some(session) = session {
        request = request
            .header(SESSION_HEADER, session.session_id.as_str())
            .header(PROTOCOL_HEADER, session.protocol_version.as_str());
    }
    let response = await_http_send(request.send(), deadline, cancellation.clone()).await?;
    // 先捕获响应 header，再处理 status/body，确保 malformed initialize 也能 cleanup。
    let session_id = response
        .headers()
        .get(SESSION_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    if let Some(capture) = session_id_capture {
        *capture = session_id.clone();
    }
    let status = response.status();
    validate_response_headers(&response)?;
    if status == reqwest::StatusCode::ACCEPTED || status == reqwest::StatusCode::NO_CONTENT {
        drain_limited_body(response, deadline, cancellation).await?;
        return Ok(None);
    }
    if !status.is_success() {
        let session_expired = session.is_some() && status == reqwest::StatusCode::NOT_FOUND;
        drain_limited_body(response, deadline, cancellation).await?;
        return Err(if session_expired {
            McpError::SessionExpired
        } else {
            McpError::CallFailed
        });
    }
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let parsed = read_response_message(
        response,
        content_type.as_deref(),
        expected_id,
        deadline,
        cancellation,
    )
    .await?;
    let parsed = parsed.map(|response| with_session_id(response, session_id));
    if let Some(response) = &parsed
        && let Some(expected_id) = expected_id
        && response.id.as_ref() != Some(expected_id)
    {
        return Err(McpError::CallFailed);
    }
    Ok(parsed)
}

/// HTTP response 的 stable JSON-RPC 视图；只保留结果、id 和 session header。
struct McpRpcResponse {
    id: Option<Value>,
    result: Option<Value>,
    is_error: bool,
    session_id: Option<String>,
}

impl McpRpcResponse {
    /// 验证 response id 并取出 result；远端 error 不穿透到 sidecar 日志或 ACP。
    fn into_result(self, expected_id: Value) -> Result<Value, McpError> {
        if self.id.as_ref() != Some(&expected_id) || self.is_error {
            return Err(McpError::CallFailed);
        }
        self.result.ok_or(McpError::CallFailed)
    }
}

/// 校验响应 header，覆盖普通、204、错误和 DELETE 等不解码路径。
fn validate_response_headers(response: &reqwest::Response) -> Result<(), McpError> {
    let declared_length = match response.headers().get(reqwest::header::CONTENT_LENGTH) {
        None => None,
        Some(value) => Some(
            value
                .to_str()
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or_else(|| {
                    tracing::debug!(
                        event = "mcp_response_content_length_invalid",
                        error_code = McpError::CallFailed.code(),
                        "MCP response Content-Length 无法解析"
                    );
                    McpError::CallFailed
                })?,
        ),
    };
    if declared_length.is_some_and(|length| length > MAX_MCP_RESPONSE_BODY_BYTES as u64) {
        tracing::debug!(
            event = "mcp_response_body_rejected",
            error_code = McpError::OutputTooLarge.code(),
            "MCP response Content-Length 超过前置 body cap"
        );
        return Err(McpError::OutputTooLarge);
    }
    if response.status() == reqwest::StatusCode::NO_CONTENT {
        if response
            .headers()
            .contains_key(reqwest::header::TRANSFER_ENCODING)
        {
            // 204 的未知传输长度不能绕过统一 body cap。
            tracing::debug!(
                event = "mcp_response_body_rejected",
                error_code = McpError::OutputTooLarge.code(),
                "MCP 204 response 带未知长度 body，按严格 cap 拒绝"
            );
            return Err(McpError::OutputTooLarge);
        }
        if declared_length.is_some_and(|length| length != 0) {
            // 204 明确禁止非零 Content-Length，避免把潜在 wire body 当成成功。
            tracing::debug!(
                event = "mcp_response_content_length_invalid",
                error_code = McpError::CallFailed.code(),
                "MCP 204 response Content-Length 必须为 0"
            );
            return Err(McpError::CallFailed);
        }
    }
    Ok(())
}

/// 在不物化 body 的前提下限制 status/DELETE shortcut 的 streaming 累积大小。
async fn drain_limited_body(
    mut response: reqwest::Response,
    deadline: tokio::time::Instant,
    cancellation: Option<McpCancellationToken>,
) -> Result<(), McpError> {
    let mut total_bytes = 0_usize;
    loop {
        let chunk = await_response_chunk(&mut response, deadline, cancellation.clone()).await?;
        let Some(chunk) = chunk else {
            return Ok(());
        };
        let Some(next_size) = total_bytes.checked_add(chunk.len()) else {
            return Err(McpError::OutputTooLarge);
        };
        if next_size > MAX_MCP_RESPONSE_BODY_BYTES {
            tracing::debug!(
                event = "mcp_response_body_rejected",
                error_code = McpError::OutputTooLarge.code(),
                "MCP shortcut response streaming body 超过 cap"
            );
            return Err(McpError::OutputTooLarge);
        }
        total_bytes = next_size;
    }
}

/// 在 HTTP body 解码前限制 Content-Length 与 streaming/chunked 累积大小。
async fn read_response_message(
    response: reqwest::Response,
    content_type: Option<&str>,
    expected_id: Option<&Value>,
    deadline: tokio::time::Instant,
    cancellation: Option<McpCancellationToken>,
) -> Result<Option<McpRpcResponse>, McpError> {
    let is_json = content_type
        .map(|value| value.split(';').next().unwrap_or_default().trim())
        .is_some_and(|value| value.eq_ignore_ascii_case(JSON_CONTENT_TYPE));
    let is_sse = content_type
        .map(|value| value.split(';').next().unwrap_or_default().trim())
        .is_some_and(|value| value.eq_ignore_ascii_case(SSE_CONTENT_TYPE));
    if !is_json && !is_sse {
        drop(response);
        return Err(McpError::CallFailed);
    }
    validate_response_headers(&response)?;
    if is_sse {
        return read_sse_message(response, expected_id, deadline, cancellation).await;
    }
    let body = read_limited_body(response, deadline, cancellation).await?;
    if body.is_empty() {
        return Ok(None);
    }
    match parse_json_rpc_message(&body)? {
        ParsedJsonRpcMessage::Response(response) => Ok(Some(response)),
        ParsedJsonRpcMessage::Notification => Ok(None),
        ParsedJsonRpcMessage::ServerRequest => Err(McpError::CallFailed),
    }
}

/// 读取 JSON body；每个 reqwest chunk 在追加前都经过累计 cap 检查。
async fn read_limited_body(
    mut response: reqwest::Response,
    deadline: tokio::time::Instant,
    cancellation: Option<McpCancellationToken>,
) -> Result<Vec<u8>, McpError> {
    let mut body = Vec::new();
    loop {
        let chunk = await_response_chunk(&mut response, deadline, cancellation.clone()).await?;
        let Some(chunk) = chunk else {
            break;
        };
        let Some(next_size) = body.len().checked_add(chunk.len()) else {
            return Err(McpError::OutputTooLarge);
        };
        if next_size > MAX_MCP_RESPONSE_BODY_BYTES {
            tracing::debug!(
                event = "mcp_response_body_rejected",
                error_code = McpError::OutputTooLarge.code(),
                "MCP streaming response 超过前置 body cap"
            );
            return Err(McpError::OutputTooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// 读取完整 SSE body，逐 frame 分类并限制整体大小。
async fn read_sse_message(
    mut response: reqwest::Response,
    expected_id: Option<&Value>,
    deadline: tokio::time::Instant,
    cancellation: Option<McpCancellationToken>,
) -> Result<Option<McpRpcResponse>, McpError> {
    let mut buffered = Vec::new();
    let mut total_bytes = 0_usize;
    let mut matching_response = None;
    loop {
        let chunk = await_response_chunk(&mut response, deadline, cancellation.clone()).await?;
        let Some(chunk) = chunk else {
            if !buffered.is_empty() {
                tracing::debug!(
                    event = "mcp_sse_truncated_frame",
                    error_code = McpError::CallFailed.code(),
                    "SSE EOF 前未消费完整 frame delimiter"
                );
                return Err(McpError::CallFailed);
            }
            return Ok(matching_response);
        };
        let Some(next_size) = total_bytes.checked_add(chunk.len()) else {
            return Err(McpError::OutputTooLarge);
        };
        if next_size > MAX_MCP_RESPONSE_BODY_BYTES {
            tracing::debug!(
                event = "mcp_response_body_rejected",
                error_code = McpError::OutputTooLarge.code(),
                "SSE response body 超过 cap"
            );
            return Err(McpError::OutputTooLarge);
        }
        total_bytes = next_size;
        let previous_len = buffered.len();
        buffered.extend_from_slice(&chunk);
        let mut search_from = previous_len.saturating_sub(3);
        while let Some(frame_end) = sse_frame_end_from(&buffered, search_from) {
            let frame = buffered.drain(..frame_end).collect::<Vec<_>>();
            search_from = 0;
            let message = parse_sse_frame(&frame)?;
            let message = validate_sse_response_id(message, expected_id)?;
            if matching_response.is_some() && message.is_some() {
                return Err(McpError::CallFailed);
            }
            if message.is_some() {
                matching_response = message;
            }
        }
    }
}

/// 读取单个 response chunk，并让 caller token/deadline 覆盖整个 await。
async fn await_response_chunk(
    response: &mut reqwest::Response,
    deadline: tokio::time::Instant,
    cancellation: Option<McpCancellationToken>,
) -> Result<Option<Bytes>, McpError> {
    let chunk_future = response.chunk();
    tokio::pin!(chunk_future);
    if let Some(cancellation) = cancellation {
        let deadline_sleep = tokio::time::sleep_until(deadline);
        tokio::pin!(deadline_sleep);
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(McpError::CallCancelled),
            _ = &mut deadline_sleep => Err(McpError::CallTimeout),
            result = &mut chunk_future => result.map_err(|_| McpError::CallFailed),
        }
    } else {
        match tokio::time::timeout_at(deadline, &mut chunk_future).await {
            Ok(result) => result.map_err(|_| McpError::CallFailed),
            Err(_) => Err(McpError::CallTimeout),
        }
    }
}

/// 让 send future 的创建/排队/响应头等待共享同一 deadline 与 cancellation。
async fn await_http_send<F>(
    request: F,
    deadline: tokio::time::Instant,
    cancellation: Option<McpCancellationToken>,
) -> Result<reqwest::Response, McpError>
where
    F: Future<Output = Result<reqwest::Response, reqwest::Error>>,
{
    tokio::pin!(request);
    if let Some(cancellation) = cancellation {
        let deadline_sleep = tokio::time::sleep_until(deadline);
        tokio::pin!(deadline_sleep);
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(McpError::CallCancelled),
            _ = &mut deadline_sleep => Err(McpError::CallTimeout),
            result = &mut request => result
                .map_err(|_| if cancellation.is_cancelled() { McpError::CallCancelled } else { McpError::CallFailed }),
        }
    } else {
        match tokio::time::timeout_at(deadline, &mut request).await {
            Ok(result) => result.map_err(|_| McpError::CallFailed),
            Err(_) => Err(McpError::CallTimeout),
        }
    }
}

/// 找到指定位置之后的 SSE 空行边界，兼容 LF 与 CRLF。
fn sse_frame_end_from(buffer: &[u8], search_from: usize) -> Option<usize> {
    let search_from = search_from.min(buffer.len());
    let suffix = &buffer[search_from..];
    let lf = suffix
        .windows(2)
        .position(|window| window == b"\n\n")
        .map(|index| search_from + index + 2);
    let crlf = suffix
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| search_from + index + 4);
    match (lf, crlf) {
        (Some(lf), Some(crlf)) => Some(lf.min(crlf)),
        (Some(end), None) | (None, Some(end)) => Some(end),
        (None, None) => None,
    }
}

/// 校验 SSE response 与当前 HTTP request 的 id，拒绝串线消息。
fn validate_sse_response_id(
    message: Option<McpRpcResponse>,
    expected_id: Option<&Value>,
) -> Result<Option<McpRpcResponse>, McpError> {
    if let (Some(message), Some(expected_id)) = (&message, expected_id)
        && message.id.as_ref() != Some(expected_id)
    {
        return Err(McpError::CallFailed);
    }
    Ok(message)
}

/// 解析一个 SSE frame 的 data 行；notification 继续等待，server request fail-closed。
fn parse_sse_frame(frame: &[u8]) -> Result<Option<McpRpcResponse>, McpError> {
    let text = std::str::from_utf8(frame).map_err(|_| McpError::CallFailed)?;
    let mut data = String::new();
    for line in text.lines() {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if let Some(value) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(value.strip_prefix(' ').unwrap_or(value));
        }
    }
    if data.trim().is_empty() {
        return Ok(None);
    }
    match parse_json_rpc_message(data.as_bytes())? {
        ParsedJsonRpcMessage::Response(message) => Ok(Some(message)),
        ParsedJsonRpcMessage::Notification => Ok(None),
        ParsedJsonRpcMessage::ServerRequest => Err(McpError::CallFailed),
    }
}

/// JSON-RPC envelope 的有限分类，区分 SSE 可忽略 notification 与不支持的 server request。
enum ParsedJsonRpcMessage {
    Response(McpRpcResponse),
    Notification,
    ServerRequest,
}

/// 只解析 JSON-RPC envelope 的稳定字段，禁止把未知正文作为错误文本传播。
fn parse_json_rpc_message(body: &[u8]) -> Result<ParsedJsonRpcMessage, McpError> {
    let value: Value = serde_json::from_slice(body).map_err(|_| McpError::CallFailed)?;
    let object = value.as_object().ok_or(McpError::CallFailed)?;
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Err(McpError::CallFailed);
    }
    let has_method = object.contains_key("method");
    let has_id = object.contains_key("id");
    let has_result = object.contains_key("result");
    let has_error = object.contains_key("error");
    if has_method {
        if has_result || has_error {
            return Err(McpError::CallFailed);
        }
        if object
            .get("method")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
        {
            return Err(McpError::CallFailed);
        }
        return Ok(if has_id {
            ParsedJsonRpcMessage::ServerRequest
        } else {
            ParsedJsonRpcMessage::Notification
        });
    }
    if has_result == has_error {
        return Err(McpError::CallFailed);
    }
    let id = object.get("id").cloned();
    let is_error = if has_error {
        if !object.get("error").is_some_and(Value::is_object) {
            return Err(McpError::CallFailed);
        }
        true
    } else {
        false
    };
    Ok(ParsedJsonRpcMessage::Response(McpRpcResponse {
        id,
        result: object.get("result").cloned(),
        is_error,
        session_id: None,
    }))
}

/// 关闭一个 HTTP MCP session；405 表示 server 明确不支持 DELETE，按 rmcp 兼容语义视为完成。
async fn close_http_session(
    session: &McpHttpSession,
    deadline: tokio::time::Instant,
    cancellation: Option<McpCancellationToken>,
) -> Result<(), McpError> {
    let request = session
        .client
        .delete(&session.url)
        .header(SESSION_HEADER, session.session_id.as_str())
        .header(PROTOCOL_HEADER, session.protocol_version.as_str());
    let response = match await_http_send(request.send(), deadline, cancellation.clone()).await {
        Ok(response) => response,
        Err(error) => {
            tracing::debug!(
                event = "mcp_session_close_response_failed",
                error_code = error.code(),
                "MCP session close 响应头获取失败"
            );
            return Err(error);
        }
    };
    let status = response.status();
    tracing::debug!(
        event = "mcp_session_close_response",
        status = status.as_u16(),
        "MCP session close 收到响应"
    );
    if let Err(error) = validate_response_headers(&response) {
        drop(response);
        tracing::debug!(
            event = "mcp_session_close_body_rejected",
            error_code = error.code(),
            "MCP session close response header/body 被拒绝"
        );
        return Err(McpError::ShutdownFailed);
    }
    if let Err(error) = drain_limited_body(response, deadline, cancellation).await {
        tracing::debug!(
            event = "mcp_session_close_body_read_failed",
            error_code = error.code(),
            "MCP session close response body 读取失败"
        );
        return Err(McpError::ShutdownFailed);
    }
    if status == reqwest::StatusCode::METHOD_NOT_ALLOWED || status.is_success() {
        Ok(())
    } else {
        Err(McpError::ShutdownFailed)
    }
}

/// 从 response headers 补入 session id；这里只从受控 header 读取，不写日志。
fn with_session_id(mut response: McpRpcResponse, session_id: Option<String>) -> McpRpcResponse {
    response.session_id = session_id;
    response
}

/// 只允许 contract 已接受的字面量 loopback HTTP URL，并做 transport 层二次校验。
pub fn is_literal_loopback_http_url(url: &str) -> bool {
    if !contract_is_literal_loopback_http_url(url) {
        return false;
    }
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return false;
    };
    parsed.scheme() == "http"
        // url::Url 的 IPv6 `host_str` 保留方括号，需同时匹配其规范字符串形状。
        && matches!(parsed.host_str(), Some("127.0.0.1" | "::1" | "[::1]"))
        && parsed.port().is_some_and(|port| port != 0)
        && parsed.username().is_empty()
        && parsed.password().is_none()
        && parsed.query().is_none()
        && parsed.fragment().is_none()
        && !parsed.path().is_empty()
}

/// 判断 actual catalog entry 是否能进入模型/调用的 ready 交集。
fn is_ready_catalog_tool(entry: &McpServerEntry, tool: &McpCatalogTool) -> bool {
    let qualified = qualified_tool_name(&entry.catalog.name, &tool.name);
    entry.catalog.session.status == "ready"
        && contract_is_server_name(&entry.catalog.name)
        && tool.enabled
        && is_tool_name_segment(&tool.name)
        && contract_is_qualified_tool_name(&qualified)
        && is_persistable_qualified_tool_name(&entry.catalog.name, &tool.name)
}

/// 判断 qualified tool 是否能在 session journal 的 identifier 边界内持久化。
fn is_persistable_qualified_tool_name(server_name: &str, tool_name: &str) -> bool {
    server_name
        .len()
        .checked_add(MCP_TOOL_SEPARATOR.len())
        .and_then(|length| length.checked_add(tool_name.len()))
        .is_some_and(|length| length <= MAX_RECORD_ID_BYTES)
}

/// 构造 contract 规定的 qualified tool name。
fn qualified_tool_name(server_name: &str, tool_name: &str) -> String {
    format!("{server_name}{MCP_TOOL_SEPARATOR}{tool_name}")
}

/// 按 contract 规则拆出 qualified tool 的 server/tool 两段。
fn split_qualified_tool_name(name: &str) -> Option<(&str, &str)> {
    if !contract_is_qualified_tool_name(name) {
        return None;
    }
    name.split_once(MCP_TOOL_SEPARATOR)
}

/// 校验 Host contract 的通用 tool/name segment ASCII 规则；tool 不设长度上限。
fn is_tool_name_segment(name: &str) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && (bytes[0].is_ascii_alphabetic() || bytes[0] == b'_')
        && bytes[1..]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

/// 将 rmcp 风格的 call result 限制在 1 MiB 后转换为本地 DTO。
fn normalize_call_result(result: Value) -> Result<McpCallResult, McpError> {
    let output_bytes = serde_json::to_vec(&result).map_err(|_| McpError::CallFailed)?;
    if output_bytes.len() > MAX_MCP_OUTPUT_BYTES {
        return Err(McpError::OutputTooLarge);
    }
    let object = result.as_object().ok_or(McpError::CallFailed)?;
    let content = object
        .get("content")
        .and_then(Value::as_array)
        .cloned()
        .ok_or(McpError::CallFailed)?;
    let is_error = match object.get("isError") {
        None | Some(Value::Null) => false,
        Some(Value::Bool(value)) => *value,
        Some(_) => return Err(McpError::CallFailed),
    };
    Ok(McpCallResult {
        content,
        structured_content: object.get("structuredContent").cloned(),
        is_error,
    })
}

/// 计算稳定的本地结果大小，用于数量/字节日志而不是输出正文。
fn serialized_call_result_size(result: &McpCallResult) -> usize {
    serde_json::to_vec(result)
        .map(|bytes| bytes.len())
        .unwrap_or(0)
}
