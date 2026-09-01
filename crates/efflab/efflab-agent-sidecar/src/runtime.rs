//! sidecar 的 ACP stdio runtime。
//!
//! 本模块把同步 stdin reader 桥接到 cancel-safe line reader，并在 current-thread
//! `LocalSet` 中维护 ACP connection、gateway 和清理顺序；stdout 只交给 ACP。

use std::{
    cell::{Cell, RefCell},
    future::Future,
    io,
    rc::Rc,
    time::Duration,
};

use agent_client_protocol as acp;
use anyhow::Result;
use tokio::io::AsyncWriteExt;
use tokio::sync::Notify;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use xai_acp_lib::{
    AcpTransportErrorKind, AcpTransportReader, AcpTransportState, AcpTransportWriter,
    LineBufferedRead, acp_gateway, spawn_stdin_line_reader_with_errors,
};

#[cfg(debug_assertions)]
use crate::test_seam::TestSeam;
use crate::{
    acp_agent::MinimalAgent, mcp_client::McpRuntime, model_client::HttpModelClient, observability,
    session_store::SessionRepository, sidecar_config::SidecarConfig,
};

/// stdin bridge 的缓冲容量；过大的 ACP 行会通过 backpressure 流过 duplex。
const STDIN_BRIDGE_CAPACITY: usize = 64 * 1024;
/// EOF/transport 清理等待 active prompt terminal journal 的最大时间。
const ACTIVE_PROMPT_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// ACP dispatcher 与其 request/notification handler 的 admission 计数器。
#[derive(Clone)]
struct HandlerTracker {
    state: Rc<RefCell<HandlerTrackerState>>,
}

struct HandlerTrackerState {
    admission_open: bool,
    dispatcher_admitted: bool,
    dispatcher_finished: bool,
    queued: usize,
    active: usize,
    changed: Rc<Notify>,
}

impl HandlerTracker {
    /// 创建仍允许接收 ACP handler 的 tracker。
    fn new() -> Self {
        Self {
            state: Rc::new(RefCell::new(HandlerTrackerState {
                admission_open: true,
                dispatcher_admitted: false,
                dispatcher_finished: false,
                queued: 0,
                active: 0,
                changed: Rc::new(Notify::new()),
            })),
        }
    }

    /// 记录最初的 dispatcher future；它在 EOF 后仍负责排空 EOF 前已入队的消息。
    fn register_dispatcher(&self) -> bool {
        let mut state = self.state.borrow_mut();
        if !state.admission_open || state.dispatcher_admitted {
            return false;
        }
        state.dispatcher_admitted = true;
        state.queued = state.queued.saturating_add(1);
        true
    }

    /// 记录 request/notification handler；EOF 后只允许已 admission 的 dispatcher 继续派生。
    fn register_handler(&self) -> bool {
        let mut state = self.state.borrow_mut();
        let allowed =
            state.admission_open || (state.dispatcher_admitted && !state.dispatcher_finished);
        if !allowed {
            return false;
        }
        state.queued = state.queued.saturating_add(1);
        true
    }

    /// 将已经排队的 future 标记为首次运行中的 handler。
    fn mark_handler_started(&self) {
        let mut state = self.state.borrow_mut();
        state.queued = state.queued.saturating_sub(1);
        state.active = state.active.saturating_add(1);
    }

    /// 将已完成或被取消的 handler 从 active 计数中移除。
    fn mark_handler_finished(&self) {
        let mut state = self.state.borrow_mut();
        state.active = state.active.saturating_sub(1);
        state.changed.notify_waiters();
    }

    /// 标记 dispatcher 结束，之后 EOF 清理不得再接收新 handler。
    fn mark_dispatcher_finished(&self) {
        let mut state = self.state.borrow_mut();
        state.dispatcher_finished = true;
        state.changed.notify_waiters();
    }

    /// 关闭新 admission，但保留已 admission dispatcher 排空入站队列的能力。
    fn close_admission(&self) {
        let mut state = self.state.borrow_mut();
        state.admission_open = false;
        state.changed.notify_waiters();
    }

    /// 返回 queued 与 active handler 数量，供测试和有界 drain 使用。
    fn handler_counts(&self) -> (usize, usize) {
        let state = self.state.borrow();
        (state.queued, state.active)
    }

    /// 只有 dispatcher 已结束且没有 queued/active handler 时才算完成 drain。
    fn is_empty(&self) -> bool {
        let state = self.state.borrow();
        (!state.dispatcher_admitted || state.dispatcher_finished)
            && state.queued == 0
            && state.active == 0
    }

    /// 在有界时间内等待 dispatcher 与所有 handler 退出。
    async fn wait_for_empty(&self, timeout: Duration) -> bool {
        let notify = self.state.borrow().changed.clone();
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.is_empty() {
                return true;
            }
            let notified = notify.notified();
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return self.is_empty();
            }
        }
    }
}

/// 将 ACP dispatcher/handler 包装为可计数、可等待的 LocalSet future。
fn spawn_tracked_handler<F>(tracker: HandlerTracker, future: F, dispatcher: bool)
where
    F: Future<Output = ()> + 'static,
{
    tokio::task::spawn_local(async move {
        tracker.mark_handler_started();
        future.await;
        tracker.mark_handler_finished();
        if dispatcher {
            tracker.mark_dispatcher_finished();
        }
    });
}

#[derive(Clone, Copy, Debug)]
enum StdinBridgeFailure {
    Read,
    Write,
}

fn bridge_failure_kind(failure: StdinBridgeFailure) -> AcpTransportErrorKind {
    match failure {
        StdinBridgeFailure::Read => AcpTransportErrorKind::StdinRead,
        StdinBridgeFailure::Write => AcpTransportErrorKind::StdinBridge,
    }
}

fn transport_failure(kind: Option<AcpTransportErrorKind>) -> anyhow::Error {
    match kind {
        Some(kind) => anyhow::anyhow!("ACP stdio transport failed: {}", kind.stable_code()),
        None => anyhow::anyhow!("ACP stdio transport failed"),
    }
}

/// 在 current-thread `LocalSet` 中运行最小 ACP agent，直到 stdio EOF 或 I/O 失败。
pub async fn run_acp(sidecar: SidecarConfig) -> Result<()> {
    observability::runtime_started();
    let transport_state = AcpTransportState::new();

    // v1 repository 与模型 client 只从已校验的 SidecarConfig 构造；失败不回显配置正文。
    let repository = SessionRepository::new(sidecar.home.clone());
    let model = HttpModelClient::from_runtime_config(&sidecar.runtime_config)
        .map_err(|_| anyhow::anyhow!("sidecar model client unavailable"))?;
    // MCP handshake 在 ACP dispatcher 创建前完成，避免未 ready 的 server 被错误广告；
    // runtime clone 会贯穿 agent、turn loop 和 EOF cleanup 三个生命周期边界。
    let mcp = McpRuntime::new(
        sidecar.runtime_config.approved_mcp.clone(),
        sidecar.runtime_config.expected_tools.clone(),
    )
    .await
    .map_err(|error| anyhow::anyhow!("sidecar mcp runtime unavailable: {}", error.code()))?;
    let agent = MinimalAgent::with_runtime_and_mcp(
        sidecar.session_cwd.clone(),
        repository,
        model,
        sidecar.runtime_config.expected_tools.clone(),
        mcp.clone(),
    );
    #[cfg(debug_assertions)]
    let test_seam = sidecar.test_seam_dir.clone().map(TestSeam::new);
    #[cfg(debug_assertions)]
    agent.install_test_seam(test_seam.clone());

    // 专用 OS reader 只负责阻塞 stdin；Tokio bridge 保留 EOF 与错误的区别。
    let mut line_rx = spawn_stdin_line_reader_with_errors();
    let (bridge_reader, mut bridge_writer) = tokio::io::duplex(STDIN_BRIDGE_CAPACITY);
    let bridge_state = transport_state.clone();
    let bridge_task = tokio::task::spawn_local(async move {
        while let Some(event) = line_rx.recv().await {
            match event {
                Ok(line) => {
                    if bridge_writer.write_all(&line).await.is_err() {
                        let failure = StdinBridgeFailure::Write;
                        bridge_state.fail(bridge_failure_kind(failure));
                        observability::stdin_bridge_stopped();
                        return Err(failure);
                    }
                }
                Err(error) => {
                    let kind = if error.kind() == io::ErrorKind::InvalidData {
                        AcpTransportErrorKind::StdinLineTooLong
                    } else {
                        AcpTransportErrorKind::StdinRead
                    };
                    bridge_state.fail(kind);
                    observability::stdin_bridge_stopped();
                    return Err(StdinBridgeFailure::Read);
                }
            }
        }
        observability::stdin_eof();
        Ok::<(), StdinBridgeFailure>(())
    });

    // 第三方 ACP line reader 只接收完整行；共享状态可在 stdout 失败后打断它。
    let incoming = AcpTransportReader::new(
        LineBufferedRead::spawn_local(bridge_reader.compat()),
        transport_state.clone(),
    );
    // stdout 句柄只交给 ACP connection，且包装为单一可观测 writer。
    let outgoing =
        AcpTransportWriter::new(tokio::io::stdout().compat_write(), transport_state.clone());

    // ACP connection 负责标准请求分发；自定义 spawn wrapper 记录 dispatcher、queued 与 active handler。
    let tracker = HandlerTracker::new();
    let first_spawn = Rc::new(Cell::new(true));
    let spawn_tracker = tracker.clone();
    let spawn_first = first_spawn.clone();
    let spawn = move |future| {
        let dispatcher = spawn_first.replace(false);
        let accepted = if dispatcher {
            spawn_tracker.register_dispatcher()
        } else {
            spawn_tracker.register_handler()
        };
        if accepted {
            spawn_tracked_handler(spawn_tracker.clone(), future, dispatcher);
        } else {
            tracing::debug!(
                event = "acp_handler_admission_closed",
                dispatcher,
                "EOF 后丢弃未 admission 的 ACP handler"
            );
        }
    };
    let (connection, io_future) =
        acp::AgentSideConnection::new(agent.clone(), outgoing, incoming, spawn);
    // gateway 保留 agent-to-client 更新与 permission reverse request 的唯一排队出口。
    let (gateway_sender, gateway_receiver) = acp_gateway::<acp::AgentSide, _>(connection);
    agent.install_gateway(gateway_sender);
    let gateway_task = tokio::task::spawn_local(gateway_receiver.run());

    // I/O future 必须持续 await；它返回即表示 ACP transport 已结束。
    let io_result = io_future.await;

    // EOF 是 admission barrier：不再接受新 handler，但允许已 admission dispatcher 排空队列。
    tracker.close_admission();
    agent.begin_shutdown();
    #[cfg(debug_assertions)]
    if let Some(test_seam) = &test_seam {
        test_seam.mark("admission_closed");
    }
    // 等待 dispatcher、queued handler 和 active handler 全部离开；prompt 自身负责先落 terminal journal。
    let drained = tracker.wait_for_empty(ACTIVE_PROMPT_DRAIN_TIMEOUT).await;
    if !drained {
        tracing::debug!(
            event = "acp_handler_drain_incomplete",
            queued = tracker.handler_counts().0,
            active = tracker.handler_counts().1,
            "ACP handler 未在有界时间内完成清理"
        );
    }
    // gateway 仍保持运行，给取消路径完成 terminal journal 和必要 update 的机会。
    let prompt_drained = agent
        .wait_for_active_prompts(ACTIVE_PROMPT_DRAIN_TIMEOUT)
        .await;
    if !prompt_drained {
        tracing::debug!(
            event = "active_prompt_drain_incomplete",
            "active prompt 未在有界时间内完成 terminal journal"
        );
    }
    // 清理路径固定停止 gateway/bridge，避免遗留本地任务继续触碰 stdout。
    gateway_task.abort();
    bridge_task.abort();
    let _ = gateway_task.await;
    let _ = bridge_task.await;

    let failure = transport_state.failure();
    let transport_result = match io_result {
        Ok(()) if failure.is_none() => {
            observability::acp_eof();
            Ok(())
        }
        Ok(()) | Err(_) => {
            observability::acp_io_failed();
            if let Some(kind) = failure {
                tracing::debug!(
                    event = "acp_transport_failure",
                    kind = kind.stable_code(),
                    "ACP transport 失败"
                );
            }
            Err(transport_failure(failure))
        }
    };
    // transport 与 prompt 已收尾后再关闭 MCP；cleanup 失败必须反映到 sidecar 结果。
    let mcp_cleanup = mcp.shutdown().await;
    if let Err(error) = &mcp_cleanup {
        tracing::debug!(
            event = "mcp_runtime_cleanup_failed",
            error_code = error.code(),
            "MCP runtime cleanup 失败"
        );
    }
    observability::gateway_stopped();
    observability::runtime_cleanup();
    match (transport_result, mcp_cleanup) {
        (Err(error), _) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
        (Ok(()), Err(error)) => Err(anyhow::anyhow!(
            "sidecar mcp runtime cleanup failed: {}",
            error.code()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::HandlerTracker;
    use std::time::Duration;

    /// EOF 关闭 admission 后不得再接收 handler，并必须等待 queued/active 全部离开。
    #[tokio::test]
    async fn eof_admission_barrier_drains_queued_and_active_handlers() {
        let tracker = HandlerTracker::new();
        assert!(tracker.register_handler());
        assert!(tracker.register_handler());
        assert_eq!(tracker.handler_counts(), (2, 0));

        tracker.close_admission();
        assert!(!tracker.register_handler(), "EOF 后不得接收新 handler");
        tracker.mark_handler_started();
        tracker.mark_handler_finished();
        assert_eq!(tracker.handler_counts(), (1, 0));
        tracker.mark_handler_started();
        tracker.mark_handler_finished();
        assert!(tracker.wait_for_empty(Duration::from_secs(1)).await);
        assert_eq!(tracker.handler_counts(), (0, 0));
    }

    /// dispatcher 尚未达到 terminal 时，即使计数归零也不能结束 EOF drain。
    #[tokio::test]
    async fn eof_drain_waits_for_dispatcher_terminal() {
        let tracker = HandlerTracker::new();
        assert!(tracker.register_dispatcher());
        tracker.close_admission();
        tracker.mark_handler_started();
        tracker.mark_handler_finished();

        assert!(!tracker.wait_for_empty(Duration::from_millis(20)).await);

        tracker.mark_dispatcher_finished();
        assert!(tracker.wait_for_empty(Duration::from_secs(1)).await);
    }
}
