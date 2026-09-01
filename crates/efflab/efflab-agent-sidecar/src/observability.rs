//! sidecar 本地可观测性边界。
//!
//! 这里只发固定事件到 stderr；不记录请求内容、路径、令牌或 runtime config。

/// 记录 ACP runtime 已开始接管 stdio。
pub(crate) fn runtime_started() {
    tracing::debug!(event = "acp_runtime_started", "ACP runtime 已启动");
}

/// 记录 stdin reader 已看到 EOF。
pub(crate) fn stdin_eof() {
    tracing::debug!(event = "stdin_eof", "sidecar stdin 已到 EOF");
}

/// 记录 ACP gateway 已完成 EOF 收尾。
pub(crate) fn acp_eof() {
    tracing::debug!(event = "acp_eof", "ACP gateway 已完成 EOF 收尾");
}

/// 记录 ACP I/O 发生非敏感传输错误。
pub(crate) fn acp_io_failed() {
    tracing::debug!(event = "acp_io_failed", "ACP gateway I/O 失败");
}

/// 记录 gateway receiver 已停止。
pub(crate) fn gateway_stopped() {
    tracing::debug!(event = "acp_gateway_stopped", "ACP gateway receiver 已停止");
}

/// 记录 stdin bridge 因下游关闭而停止。
pub(crate) fn stdin_bridge_stopped() {
    tracing::debug!(event = "stdin_bridge_stopped", "stdin bridge 已停止");
}

/// 记录 runtime 已释放 ACP transport 与本地状态。
pub(crate) fn runtime_cleanup() {
    tracing::debug!(event = "acp_runtime_cleanup", "ACP runtime 清理完成");
}

/// 记录收到 initialize，但不记录客户端字段。
pub(crate) fn initialize_received() {
    tracing::debug!(event = "acp_initialize", "收到 ACP initialize");
}

/// 记录创建了一个内存 session。
pub(crate) fn session_created() {
    tracing::debug!(event = "session_created", "内存 session 已创建");
}

/// 记录 session/load 的结果，不记录 session 标识。
pub(crate) fn session_loaded(found: bool) {
    tracing::debug!(event = "session_loaded", found, "处理 ACP session/load");
}

/// 记录 session/list 返回的数量。
pub(crate) fn sessions_listed(count: usize) {
    tracing::debug!(event = "sessions_listed", count, "处理 ACP session/list");
}

/// 记录最小 prompt 已完成，不记录 prompt 内容。
pub(crate) fn prompt_completed() {
    tracing::debug!(event = "prompt_completed", "最小 ACP prompt 已完成");
}

/// 记录取消通知是否对应已知 session，不记录 session 标识。
pub(crate) fn cancel_received(known_session: bool) {
    tracing::debug!(
        event = "session_cancel",
        known_session,
        "收到 ACP session/cancel"
    );
}

/// 记录扩展方法已返回最小 catalog。
pub(crate) fn extension_served() {
    tracing::debug!(event = "mcp_catalog_served", "返回最小 MCP catalog");
}

/// 记录未知扩展被拒绝，不回显方法名。
pub(crate) fn extension_rejected() {
    tracing::debug!(
        event = "extension_method_not_found",
        "拒绝未知 ACP extension"
    );
}
