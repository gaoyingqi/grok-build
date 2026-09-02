//! sidecar 的有限 ACP turn loop。
//!
//! 本模块把单个 prompt 的持久化、模型流、工具权限和终态收敛在一个有界回合中。
//! 每个实例只服务一个 prompt；跨 prompt 的状态通过 `prompt_id` 和 session journal 隔离。

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::rc::Rc;
use std::time::Duration;

use agent_client_protocol as acp;
use efflab_agent_contract::is_qualified_tool_name as contract_is_qualified_tool_name;
use serde_json::{Value, json};
use xai_acp_lib::AcpGatewaySender;

use crate::mcp_client::{McpCallResult, McpCancellationToken, McpError, McpRuntime};
use crate::model_client::{
    CancellationToken, DEBUG_PREVIEW_BYTES, HttpModelClient, ModelDelta, ModelError, ModelToolCall,
    ModelTurnRequest, truncate_for_debug,
};
use crate::session_store::{
    MAX_RECORD_ID_BYTES, MAX_RECORD_LINE_BYTES, SessionError, SessionRecord, SessionRepository,
};
#[cfg(debug_assertions)]
use crate::test_seam::TestSeam;

/// 单个 prompt 允许的最大工具回合数；达到上限后不再执行新工具。
pub const MAX_TOOL_ROUNDS: usize = 8;

const NOOP_TOOL: &str = "GrokBuild:efflab_noop";
const ALLOW_ONCE: &str = "allow-once";
const REJECT_ONCE: &str = "reject-once";
const TOOL_RESULT: &str = "efflab noop completed";
const MCP_TOOL_RESULT: &str = "mcp tool completed";
const MCP_TOOL_FAILURE: &str = "mcp tool failed";
const MCP_TOOL_CANCELLED: &str = "mcp tool cancelled";
/// MCP 返回值进入下一次模型请求时的固定上限；journal 只保存上面的稳定摘要。
const MAX_MCP_MODEL_RESULT_BYTES: usize = 64 * 1024;
const TERMINAL_COMPLETED: &str = "completed";
const TERMINAL_CANCELLED: &str = "cancelled";
const TERMINAL_FAILED: &str = "failed";
const TERMINAL_REFUSED: &str = "refused";
const TERMINAL_MAX_TURN_REQUESTS: &str = "max_turn_requests";
/// 单条 ACP update 等待同一 outgoing writer 完成的上限，避免 transport 异常卡住 turn。
const NOTIFICATION_DELIVERY_TIMEOUT: Duration = Duration::from_secs(5);
// 为 JSON line 的记录类型、id 和结构字段预留空间，避免正文刚好达到 line 上限。
const MAX_ASSISTANT_TEXT_BYTES: usize = MAX_RECORD_LINE_BYTES.saturating_sub(1024);

/// turn loop 的稳定错误分类；不携带 prompt、模型正文、工具参数或依赖错误正文。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnLoopError {
    /// session store 找不到当前 session。
    SessionNotFound,
    /// session store 读取或 append 失败。
    Session,
    /// 当前 session 是只读 legacy session。
    ReadOnly,
    /// 模型 client 返回了不可继续的错误。
    Model,
    /// ACP gateway 已关闭或通知无法入队。
    Transport,
    /// 模型请求的工具不在审核后的 ready 交集内。
    ToolRejected,
    /// Host permission 请求失败或返回了非法选择。
    Permission,
}

impl fmt::Display for TurnLoopError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let code = match self {
            Self::SessionNotFound => "turn_session_not_found",
            Self::Session => "turn_session_error",
            Self::ReadOnly => "turn_session_read_only",
            Self::Model => "turn_model_error",
            Self::Transport => "turn_transport_error",
            Self::ToolRejected => "turn_tool_rejected",
            Self::Permission => "turn_permission_error",
        };
        formatter.write_str(code)
    }
}

impl std::error::Error for TurnLoopError {}

/// 一个 prompt 的 admission、取消和 terminal 共享状态；cancel 与完成在此处线性化。
#[derive(Clone)]
pub struct TurnControl {
    state: Rc<RefCell<TurnControlState>>,
}

struct TurnControlState {
    cancellation: CancellationToken,
    terminal: TerminalState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TerminalState {
    Open,
    CancelRequested,
    Committed(TerminalKind),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TerminalKind {
    Completed,
    Cancelled,
    Failed,
    Refused,
    MaxTurnRequests,
}

/// 将内部 terminal kind 映射为固定 journal 状态与 ACP stop reason。
fn terminal_wire_values(kind: TerminalKind) -> (&'static str, acp::StopReason) {
    match kind {
        TerminalKind::Completed => (TERMINAL_COMPLETED, acp::StopReason::EndTurn),
        TerminalKind::Cancelled => (TERMINAL_CANCELLED, acp::StopReason::Cancelled),
        TerminalKind::Failed => (TERMINAL_FAILED, acp::StopReason::Refusal),
        TerminalKind::Refused => (TERMINAL_REFUSED, acp::StopReason::Refusal),
        TerminalKind::MaxTurnRequests => {
            (TERMINAL_MAX_TURN_REQUESTS, acp::StopReason::MaxTurnRequests)
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TerminalClaim {
    kind: TerminalKind,
    should_persist: bool,
}

impl TurnControl {
    /// 创建一个尚未提交终态的 prompt control。
    pub(crate) fn new() -> Self {
        Self {
            state: Rc::new(RefCell::new(TurnControlState {
                cancellation: CancellationToken::new(),
                terminal: TerminalState::Open,
            })),
        }
    }

    /// 复制底层取消 token，供模型流和 permission 等待共同监听。
    pub(crate) fn cancellation(&self) -> CancellationToken {
        self.state.borrow().cancellation.clone()
    }

    /// 申请取消；terminal 已线性化后取消不会改写既有终态。
    pub(crate) fn request_cancel(&self) {
        let cancellation = {
            let mut state = self.state.borrow_mut();
            if matches!(state.terminal, TerminalState::Open) {
                state.terminal = TerminalState::CancelRequested;
            }
            state.cancellation.clone()
        };
        cancellation.cancel();
    }

    /// 取得唯一 terminal 提交权；取消若先到达则完成方必须提交 cancelled。
    fn claim_terminal(&self, requested: TerminalKind) -> TerminalClaim {
        let mut state = self.state.borrow_mut();
        match state.terminal {
            TerminalState::Open => {
                state.terminal = TerminalState::Committed(requested);
                TerminalClaim {
                    kind: requested,
                    should_persist: true,
                }
            }
            TerminalState::CancelRequested => {
                state.terminal = TerminalState::Committed(TerminalKind::Cancelled);
                TerminalClaim {
                    kind: TerminalKind::Cancelled,
                    should_persist: true,
                }
            }
            TerminalState::Committed(kind) => TerminalClaim {
                kind,
                should_persist: false,
            },
        }
    }
}

impl From<SessionError> for TurnLoopError {
    fn from(error: SessionError) -> Self {
        match error {
            SessionError::LegacyReadOnly => Self::ReadOnly,
            SessionError::NotFound => Self::SessionNotFound,
            SessionError::InvalidRecord | SessionError::Corrupt | SessionError::Io => Self::Session,
        }
    }
}

/// 一个已经绑定 repository、model 和 ACP gateway 的有限 turn loop。
pub struct TurnLoop {
    repository: SessionRepository,
    model: Rc<HttpModelClient>,
    mcp: McpRuntime,
    gateway: AcpGatewaySender<acp::AgentSide>,
    expected_tools: BTreeSet<String>,
    ready_tools: BTreeSet<String>,
    /// debug 构建中的测试执行 spy；release 构建不携带测试接缝。
    #[cfg(debug_assertions)]
    test_seam: Option<TestSeam>,
}

impl TurnLoop {
    /// 构造只使用已注入依赖的 turn loop；不创建 MCP 子进程或读取外部配置。
    pub fn new(
        repository: SessionRepository,
        model: Rc<HttpModelClient>,
        mcp: McpRuntime,
        gateway: AcpGatewaySender<acp::AgentSide>,
        expected_tools: BTreeSet<String>,
        ready_tools: BTreeSet<String>,
    ) -> Self {
        Self {
            repository,
            model,
            mcp,
            gateway,
            expected_tools,
            ready_tools,
            #[cfg(debug_assertions)]
            test_seam: None,
        }
    }

    /// 安装 debug 构建专用测试 seam；release 构建不编译该接缝。
    #[cfg(debug_assertions)]
    pub(crate) fn with_test_seam(mut self, test_seam: Option<TestSeam>) -> Self {
        self.test_seam = test_seam;
        self
    }

    /// 执行一个 prompt：user 先落盘并通知，最后先落盘 terminal 再返回 response。
    pub async fn run_prompt(
        &self,
        session_id: &str,
        prompt_id: &str,
        user_text: &str,
        control: TurnControl,
    ) -> Result<acp::PromptResponse, TurnLoopError> {
        tracing::debug!(
            event = "turn_started",
            prompt_id_bytes = prompt_id.len(),
            user_text_bytes = user_text.len(),
            user_text = %truncate_for_debug(user_text, DEBUG_PREVIEW_BYTES),
            "sidecar turn loop 已进入 prompt"
        );
        let cancellation = control.cancellation();

        // 读取当前 session 只用于构造本轮模型快照；审核工具集合同时约束 legacy 读取。
        let session = self
            .repository
            .load_with_tool_policy(session_id, &self.expected_tools, &self.ready_tools)
            .await
            .map_err(|error| match error {
                SessionError::LegacyReadOnly => TurnLoopError::ReadOnly,
                other => TurnLoopError::from(other),
            })?;
        if session.read_only {
            tracing::debug!(event = "turn_rejected_read_only", "只读 session 拒绝 turn");
            return Err(TurnLoopError::ReadOnly);
        }

        let mut next_sequence = session
            .records
            .last()
            .map(SessionRecord::sequence)
            .unwrap_or(0);
        let user_sequence = allocate_sequence(&mut next_sequence)?;
        let user_record = SessionRecord::user(user_sequence, prompt_id, user_text);

        // 持久化成功是 user update 的前置条件，避免 Host 看到不可恢复的半个 prompt。
        self.repository
            .append(session_id, std::slice::from_ref(&user_record))
            .await
            .map_err(TurnLoopError::from)?;
        if let Err(error) = self
            .send_user_update(session_id, prompt_id, user_text)
            .await
        {
            tracing::debug!(event = "turn_user_update_failed", "user update 入队失败");
            return self
                .finish_failed(session_id, prompt_id, &mut next_sequence, &control, error)
                .await;
        }

        let mut records = session.records;
        records.push(user_record);
        let mut messages = transcript_messages(&records);
        let tool_definitions = self.tool_definitions();
        let mut assistant_text = String::new();
        let mut tool_rounds = 0_usize;

        loop {
            if cancellation.is_cancelled() {
                return self
                    .finish_cancelled(session_id, prompt_id, &mut next_sequence, &control)
                    .await;
            }

            // 每次循环只发起一次不可重试的模型调用；工具续回合由显式上限控制。
            let mut request = ModelTurnRequest::new(messages.clone());
            if !tool_definitions.is_empty() {
                request = request
                    .with_tools(tool_definitions.clone())
                    .with_tool_choice(json!("auto"));
            }
            let mut stream = match self.model.stream_turn(request, cancellation.clone()).await {
                Ok(stream) => stream,
                Err(ModelError::Cancelled) if cancellation.is_cancelled() => {
                    return self
                        .finish_cancelled(session_id, prompt_id, &mut next_sequence, &control)
                        .await;
                }
                Err(error) => {
                    tracing::debug!(
                        event = "turn_model_request_failed",
                        error = %error,
                        message_count = messages.len(),
                        "模型 turn 请求失败"
                    );
                    return self
                        .finish_failed(
                            session_id,
                            prompt_id,
                            &mut next_sequence,
                            &control,
                            TurnLoopError::Model,
                        )
                        .await;
                }
            };

            let mut model_text = String::new();
            let mut thought_text = String::new();
            let mut tool_calls = Vec::new();
            loop {
                match stream.recv().await {
                    Ok(Some(ModelDelta::Text(delta))) => {
                        if !append_bounded(&mut assistant_text, &delta) {
                            return self
                                .finish_failed(
                                    session_id,
                                    prompt_id,
                                    &mut next_sequence,
                                    &control,
                                    TurnLoopError::Model,
                                )
                                .await;
                        }
                        model_text.push_str(&delta);
                        let sequence = allocate_sequence(&mut next_sequence)?;
                        let snapshot = SessionRecord::assistant_snapshot(
                            sequence,
                            prompt_id,
                            "assistant-text",
                            &assistant_text,
                            true,
                        );
                        if let Err(error) = self
                            .repository
                            .append(session_id, std::slice::from_ref(&snapshot))
                            .await
                        {
                            return self
                                .finish_failed(
                                    session_id,
                                    prompt_id,
                                    &mut next_sequence,
                                    &control,
                                    TurnLoopError::from(error),
                                )
                                .await;
                        }
                        if let Err(error) = self
                            .send_assistant_delta(session_id, prompt_id, &delta)
                            .await
                        {
                            return self
                                .finish_failed(
                                    session_id,
                                    prompt_id,
                                    &mut next_sequence,
                                    &control,
                                    error,
                                )
                                .await;
                        }
                    }
                    Ok(Some(ModelDelta::Thought(delta))) => {
                        // thinking 只经 ACP 展示，不写入 journal，也不进入下一轮模型 messages。
                        if !append_bounded(&mut thought_text, &delta) {
                            return self
                                .finish_failed(
                                    session_id,
                                    prompt_id,
                                    &mut next_sequence,
                                    &control,
                                    TurnLoopError::Model,
                                )
                                .await;
                        }
                        if let Err(error) =
                            self.send_thought_delta(session_id, prompt_id, &delta).await
                        {
                            return self
                                .finish_failed(
                                    session_id,
                                    prompt_id,
                                    &mut next_sequence,
                                    &control,
                                    error,
                                )
                                .await;
                        }
                    }
                    Ok(Some(ModelDelta::ToolCall(call))) => tool_calls.push(call),
                    Ok(Some(ModelDelta::Done)) | Ok(None) => {
                        #[cfg(debug_assertions)]
                        if let Some(test_seam) = &self.test_seam {
                            test_seam.mark("model_done_consumed");
                        }
                        break;
                    }
                    Err(ModelError::Cancelled) if cancellation.is_cancelled() => {
                        return self
                            .finish_cancelled(session_id, prompt_id, &mut next_sequence, &control)
                            .await;
                    }
                    Err(error) => {
                        tracing::debug!(
                            event = "turn_model_stream_failed",
                            error = %error,
                            assistant_text_bytes = assistant_text.len(),
                            assistant_text = %truncate_for_debug(
                                &assistant_text,
                                DEBUG_PREVIEW_BYTES
                            ),
                            "模型 SSE stream 读取失败"
                        );
                        return self
                            .finish_failed(
                                session_id,
                                prompt_id,
                                &mut next_sequence,
                                &control,
                                TurnLoopError::Model,
                            )
                            .await;
                    }
                }
            }

            if tool_calls.is_empty() {
                #[cfg(debug_assertions)]
                if let Some(test_seam) = &self.test_seam {
                    // 证明 [DONE] 已被消费；cancel-first 测试在此暂停后再绑定取消。
                    test_seam.wait_if_enabled("after_model_done").await;
                }
                return self
                    .finish_completed(
                        session_id,
                        prompt_id,
                        &mut next_sequence,
                        &control,
                        &assistant_text,
                    )
                    .await;
            }
            if tool_rounds >= MAX_TOOL_ROUNDS {
                tracing::debug!(
                    event = "turn_tool_round_limit",
                    limit = MAX_TOOL_ROUNDS,
                    "工具回合达到上限"
                );
                return self
                    .finish_terminal(
                        session_id,
                        prompt_id,
                        &mut next_sequence,
                        &control,
                        TerminalKind::MaxTurnRequests,
                    )
                    .await
                    .map(|(response, _)| response);
            }

            let round = u32::try_from(tool_rounds).map_err(|_| TurnLoopError::Session)?;
            let tool_result = self
                .execute_tool_round(
                    session_id,
                    prompt_id,
                    round,
                    &model_text,
                    &tool_calls,
                    &cancellation,
                    &mut next_sequence,
                )
                .await;
            match tool_result {
                Ok(ToolRoundResult::Continue(tool_messages)) => {
                    let assistant_tool_message =
                        assistant_tool_message(&model_text, &thought_text, &tool_calls)?;
                    messages.push(assistant_tool_message);
                    messages.extend(tool_messages);
                    tool_rounds = tool_rounds.saturating_add(1);
                    // 工具 round 文本已由 AssistantToolCalls 承载；后续 snapshot 只记录普通 assistant 文本。
                    assistant_text.clear();
                    thought_text.clear();
                }
                Ok(ToolRoundResult::Cancelled) => {
                    return self
                        .finish_cancelled(session_id, prompt_id, &mut next_sequence, &control)
                        .await;
                }
                Ok(ToolRoundResult::Refused) => {
                    return self
                        .finish_terminal(
                            session_id,
                            prompt_id,
                            &mut next_sequence,
                            &control,
                            TerminalKind::Refused,
                        )
                        .await
                        .map(|(response, _)| response);
                }
                Err(error) => {
                    return self
                        .finish_failed(session_id, prompt_id, &mut next_sequence, &control, error)
                        .await;
                }
            }
        }
    }

    /// 计算唯一的 approved∩ready 工具集。
    /// Chat Completions 协议会完整解析 tool_calls；真正可执行的只有 App 审核通过的工具，不含 bash。
    fn allowed_tools(&self) -> BTreeSet<String> {
        self.expected_tools
            .intersection(&self.ready_tools)
            .cloned()
            .collect()
    }

    /// 只向模型广告 approved∩ready 的 MCP schema 与固定无副作用 noop。
    fn tool_definitions(&self) -> Vec<Value> {
        let allowed_tools = self.allowed_tools();
        let mut definitions = self
            .mcp
            .model_tool_schemas()
            .into_iter()
            .filter(|definition| {
                definition["function"]["name"]
                    .as_str()
                    .is_some_and(|name| allowed_tools.contains(name))
            })
            .collect::<Vec<_>>();
        if allowed_tools.contains(NOOP_TOOL) {
            definitions.push(json!({
                "type": "function",
                "function": {
                    "name": NOOP_TOOL,
                    "description": "Perform the Efflab no-op operation.",
                    "parameters": {
                        "type": "object",
                        "properties": {},
                        "additionalProperties": false
                    }
                }
            }));
        }
        definitions
    }

    /// 发送已落盘的 user 文本；通知顶层 `_meta` 固定绑定当前 promptId。
    async fn send_user_update(
        &self,
        session_id: &str,
        prompt_id: &str,
        text: &str,
    ) -> Result<(), TurnLoopError> {
        let notification = acp::SessionNotification::new(
            session_id.to_owned(),
            acp::SessionUpdate::UserMessageChunk(acp::ContentChunk::new(acp::ContentBlock::Text(
                acp::TextContent::new(text),
            ))),
        )
        .meta(prompt_meta(prompt_id));
        self.enqueue_notification(notification).await
    }

    /// 发送每个模型文本 delta；累积文本只保存在当前 prompt 的局部状态。
    async fn send_assistant_delta(
        &self,
        session_id: &str,
        prompt_id: &str,
        delta: &str,
    ) -> Result<(), TurnLoopError> {
        let notification = acp::SessionNotification::new(
            session_id.to_owned(),
            acp::SessionUpdate::AgentMessageChunk(acp::ContentChunk::new(acp::ContentBlock::Text(
                acp::TextContent::new(delta),
            ))),
        )
        .meta(prompt_meta(prompt_id));
        self.enqueue_notification(notification).await
    }

    /// 发送推理增量；Host 投影为 thinking block，不进入模型 transcript。
    async fn send_thought_delta(
        &self,
        session_id: &str,
        prompt_id: &str,
        delta: &str,
    ) -> Result<(), TurnLoopError> {
        let notification = acp::SessionNotification::new(
            session_id.to_owned(),
            acp::SessionUpdate::AgentThoughtChunk(acp::ContentChunk::new(acp::ContentBlock::Text(
                acp::TextContent::new(delta),
            ))),
        )
        .meta(prompt_meta(prompt_id));
        self.enqueue_notification(notification).await
    }

    /// 将 ACP 通知放入唯一 gateway 队列，并等待同一 outgoing writer 完成。
    async fn enqueue_notification(
        &self,
        notification: acp::SessionNotification,
    ) -> Result<(), TurnLoopError> {
        let completion = self.gateway.forward_with_completion(notification);
        let result = tokio::time::timeout(NOTIFICATION_DELIVERY_TIMEOUT, completion)
            .await
            .map_err(|_| TurnLoopError::Transport)?
            .map_err(|_| TurnLoopError::Transport)?;
        result.map_err(|_| TurnLoopError::Transport)
    }

    /// 执行一个有限工具回合，所有执行前均先通过当前 ACP permission request。
    async fn execute_tool_round(
        &self,
        session_id: &str,
        prompt_id: &str,
        round: u32,
        model_text: &str,
        calls: &[ModelToolCall],
        cancellation: &CancellationToken,
        next_sequence: &mut u64,
    ) -> Result<ToolRoundResult, TurnLoopError> {
        let allowed_tools = self.allowed_tools();

        // 先完整校验工具名、参数和调用 id，避免非法调用留下半个 round。
        let assistant_sequence = allocate_sequence(next_sequence)?;
        let mut planned = Vec::with_capacity(calls.len());
        for call in calls {
            let Some(name) = call.name.as_deref() else {
                return Err(TurnLoopError::ToolRejected);
            };
            if !allowed_tools.contains(name) {
                tracing::debug!(
                    event = "turn_tool_rejected",
                    error_code = "turn_tool_not_approved",
                    "模型工具不在 approved ready 交集"
                );
                return Err(TurnLoopError::ToolRejected);
            }
            let mcp_arguments = if name == NOOP_TOOL {
                if !valid_noop_arguments(&call.arguments) {
                    tracing::debug!(
                        event = "turn_tool_arguments_rejected",
                        error_code = "turn_noop_arguments_invalid",
                        "拒绝非法 noop 调用"
                    );
                    return Err(TurnLoopError::ToolRejected);
                }
                None
            } else {
                let raw_arguments = if call.arguments.trim().is_empty() {
                    "{}"
                } else {
                    call.arguments.as_str()
                };
                let parsed = serde_json::from_str::<Value>(raw_arguments).map_err(|_| {
                    tracing::debug!(
                        event = "turn_tool_arguments_rejected",
                        error_code = "turn_mcp_arguments_invalid",
                        "拒绝非 JSON MCP 参数"
                    );
                    TurnLoopError::ToolRejected
                })?;
                if !parsed.is_object() {
                    tracing::debug!(
                        event = "turn_tool_arguments_rejected",
                        error_code = "turn_mcp_arguments_not_object",
                        "拒绝非 object MCP 参数"
                    );
                    return Err(TurnLoopError::ToolRejected);
                }
                Some(parsed)
            };
            if call.id.is_none() {
                tracing::debug!(
                    event = "turn_tool_arguments_rejected",
                    error_code = "turn_tool_call_id_missing",
                    "拒绝缺少调用 id 的工具请求"
                );
                return Err(TurnLoopError::ToolRejected);
            }
            let pending_sequence = allocate_sequence(next_sequence)?;
            planned.push((
                call,
                format!("tool-{pending_sequence}"),
                pending_sequence,
                name.to_owned(),
                mcp_arguments,
            ));
        }

        // assistant tool_calls 只保存受控 name/id；原始 arguments 只进入后续模型请求。
        let safe_calls = planned
            .iter()
            .map(|(_, wire_call_id, _, name, _)| (wire_call_id.clone(), name.clone()))
            .collect::<Vec<_>>();
        let assistant = SessionRecord::assistant_tool_calls(
            assistant_sequence,
            prompt_id,
            round,
            safe_calls,
            model_text,
        );
        self.repository
            .append(session_id, std::slice::from_ref(&assistant))
            .await
            .map_err(TurnLoopError::from)?;

        let mut tool_messages = Vec::with_capacity(planned.len());
        for (call, wire_call_id, pending_sequence, name, mcp_arguments) in planned {
            let pending = SessionRecord::tool_in_round(
                pending_sequence,
                prompt_id,
                round,
                &wire_call_id,
                &name,
                "permission pending",
                "pending",
            );
            self.repository
                .append(session_id, std::slice::from_ref(&pending))
                .await
                .map_err(TurnLoopError::from)?;
            self.send_tool_call(session_id, prompt_id, &wire_call_id, &name)
                .await?;

            let decision = self
                .request_permission(session_id, prompt_id, &wire_call_id, &name, cancellation)
                .await?;
            if matches!(decision, PermissionDecision::Cancelled)
                || (matches!(decision, PermissionDecision::Allowed) && cancellation.is_cancelled())
            {
                self.send_tool_status(
                    session_id,
                    prompt_id,
                    &wire_call_id,
                    &name,
                    acp::ToolCallStatus::Failed,
                )
                .await?;
                let sequence = allocate_sequence(next_sequence)?;
                let record = SessionRecord::tool_in_round(
                    sequence,
                    prompt_id,
                    round,
                    &wire_call_id,
                    &name,
                    MCP_TOOL_CANCELLED,
                    "cancelled",
                );
                self.repository
                    .append(session_id, std::slice::from_ref(&record))
                    .await
                    .map_err(TurnLoopError::from)?;
                return Ok(ToolRoundResult::Cancelled);
            }
            if matches!(decision, PermissionDecision::Rejected) {
                self.send_tool_status(
                    session_id,
                    prompt_id,
                    &wire_call_id,
                    &name,
                    acp::ToolCallStatus::Failed,
                )
                .await?;
                let sequence = allocate_sequence(next_sequence)?;
                let record = SessionRecord::tool_in_round(
                    sequence,
                    prompt_id,
                    round,
                    &wire_call_id,
                    &name,
                    "permission rejected",
                    "rejected",
                );
                self.repository
                    .append(session_id, std::slice::from_ref(&record))
                    .await
                    .map_err(TurnLoopError::from)?;
                return Ok(ToolRoundResult::Refused);
            }

            self.send_tool_status(
                session_id,
                prompt_id,
                &wire_call_id,
                &name,
                acp::ToolCallStatus::InProgress,
            )
            .await?;

            let (model_result, journal_detail) = if name == NOOP_TOOL {
                // 内置 noop 没有副作用；permission 之后此处是唯一的执行点。
                #[cfg(debug_assertions)]
                if let Some(test_seam) = &self.test_seam {
                    test_seam.record_execution();
                }
                (TOOL_RESULT.to_owned(), TOOL_RESULT)
            } else {
                let arguments = mcp_arguments.ok_or(TurnLoopError::ToolRejected)?;
                match self.call_mcp(&name, arguments, cancellation).await {
                    Ok(result) => {
                        let detail = if result.is_error {
                            "mcp tool returned error"
                        } else {
                            MCP_TOOL_RESULT
                        };
                        (safe_mcp_result_for_model(&result), detail)
                    }
                    Err(McpError::CallCancelled | McpError::RuntimeShutdown) => {
                        self.send_tool_status(
                            session_id,
                            prompt_id,
                            &wire_call_id,
                            &name,
                            acp::ToolCallStatus::Failed,
                        )
                        .await?;
                        let sequence = allocate_sequence(next_sequence)?;
                        let record = SessionRecord::tool_in_round(
                            sequence,
                            prompt_id,
                            round,
                            &wire_call_id,
                            &name,
                            MCP_TOOL_CANCELLED,
                            "cancelled",
                        );
                        self.repository
                            .append(session_id, std::slice::from_ref(&record))
                            .await
                            .map_err(TurnLoopError::from)?;
                        return Ok(ToolRoundResult::Cancelled);
                    }
                    Err(error) => {
                        tracing::debug!(
                            event = "turn_mcp_call_failed",
                            error_code = error.code(),
                            "MCP 工具调用失败，向模型返回固定结果"
                        );
                        (MCP_TOOL_FAILURE.to_owned(), MCP_TOOL_FAILURE)
                    }
                }
            };

            // journal 只保存固定摘要；MCP 的真实结果仅进入当前模型回合。
            let completed_sequence = allocate_sequence(next_sequence)?;
            let completed = SessionRecord::tool_in_round(
                completed_sequence,
                prompt_id,
                round,
                &wire_call_id,
                &name,
                journal_detail,
                "completed",
            );
            self.repository
                .append(session_id, std::slice::from_ref(&completed))
                .await
                .map_err(TurnLoopError::from)?;
            self.send_tool_status(
                session_id,
                prompt_id,
                &wire_call_id,
                &name,
                acp::ToolCallStatus::Completed,
            )
            .await?;

            let model_call_id = call.id.as_deref().ok_or(TurnLoopError::ToolRejected)?;
            tool_messages.push(json!({
                "role": "tool",
                "tool_call_id": model_call_id,
                "content": model_result
            }));
        }

        Ok(ToolRoundResult::Continue(tool_messages))
    }

    /// 将 sidecar cancellation 转发为 MCP token，并等待 call 完成清理。
    async fn call_mcp(
        &self,
        name: &str,
        arguments: Value,
        cancellation: &CancellationToken,
    ) -> Result<McpCallResult, McpError> {
        let mcp_cancellation = McpCancellationToken::new();
        let call = self
            .mcp
            .call_with_cancellation(name, arguments, mcp_cancellation.clone());
        tokio::pin!(call);
        tokio::select! {
            biased;
            result = &mut call => result,
            _ = cancellation.cancelled() => {
                mcp_cancellation.cancel();
                // 等待 MCP future 收敛，记录其稳定结果，避免取消 cleanup 错误被静默吞掉。
                if let Err(error) = (&mut call).await {
                    tracing::debug!(
                        event = "mcp_call_cancel_cleanup",
                        error_code = error.code(),
                        "MCP call cancel cleanup 返回稳定结果"
                    );
                }
                Err(McpError::CallCancelled)
            }
        }
    }

    /// 发送真实 ACP `session/update` tool call，不携带原始 arguments。
    async fn send_tool_call(
        &self,
        session_id: &str,
        prompt_id: &str,
        tool_call_id: &str,
        tool_name: &str,
    ) -> Result<(), TurnLoopError> {
        let call = acp::ToolCall::new(tool_call_id.to_owned(), tool_name.to_owned())
            .kind(acp::ToolKind::Execute)
            .status(acp::ToolCallStatus::Pending)
            .meta(prompt_meta(prompt_id));
        let notification = acp::SessionNotification::new(
            session_id.to_owned(),
            acp::SessionUpdate::ToolCall(call),
        )
        .meta(prompt_meta(prompt_id));
        self.enqueue_notification(notification).await
    }

    /// 发送安全的 tool 状态更新；公开 detail 固定，不回显工具参数或模型正文。
    async fn send_tool_status(
        &self,
        session_id: &str,
        prompt_id: &str,
        tool_call_id: &str,
        tool_name: &str,
        status: acp::ToolCallStatus,
    ) -> Result<(), TurnLoopError> {
        let fields = acp::ToolCallUpdateFields::new()
            .title(tool_name.to_owned())
            .status(status);
        let update =
            acp::ToolCallUpdate::new(tool_call_id.to_owned(), fields).meta(prompt_meta(prompt_id));
        let notification = acp::SessionNotification::new(
            session_id.to_owned(),
            acp::SessionUpdate::ToolCallUpdate(update),
        )
        .meta(prompt_meta(prompt_id));
        self.enqueue_notification(notification).await
    }

    /// 发送 permission reverse request，并将取消与等待 Host 响应并行处理。
    async fn request_permission(
        &self,
        session_id: &str,
        prompt_id: &str,
        tool_call_id: &str,
        tool_name: &str,
        cancellation: &CancellationToken,
    ) -> Result<PermissionDecision, TurnLoopError> {
        let tool_call = acp::ToolCallUpdate::new(
            tool_call_id.to_owned(),
            acp::ToolCallUpdateFields::new()
                .title(tool_name.to_owned())
                .status(acp::ToolCallStatus::Pending),
        )
        .meta(prompt_meta(prompt_id));
        let request = acp::RequestPermissionRequest::new(
            session_id.to_owned(),
            tool_call,
            vec![
                acp::PermissionOption::new(
                    ALLOW_ONCE,
                    "Allow once",
                    acp::PermissionOptionKind::AllowOnce,
                ),
                acp::PermissionOption::new(
                    REJECT_ONCE,
                    "Reject once",
                    acp::PermissionOptionKind::RejectOnce,
                ),
            ],
        )
        .meta(prompt_meta(prompt_id));

        tracing::debug!(event = "turn_permission_requested", "等待 Host permission");
        let permission_future = acp::Client::request_permission(&self.gateway, request);
        tokio::pin!(permission_future);
        let response = tokio::select! {
            response = &mut permission_future => response.map_err(|_| TurnLoopError::Permission)?,
            _ = cancellation.cancelled() => {
                tracing::debug!(event = "turn_permission_cancelled", "permission 等待被取消");
                return Ok(PermissionDecision::Cancelled);
            }
        };

        match response.outcome {
            acp::RequestPermissionOutcome::Cancelled => Ok(PermissionDecision::Cancelled),
            acp::RequestPermissionOutcome::Selected(selected)
                if selected.option_id.to_string() == ALLOW_ONCE =>
            {
                Ok(PermissionDecision::Allowed)
            }
            acp::RequestPermissionOutcome::Selected(_) => Ok(PermissionDecision::Rejected),
            _ => Ok(PermissionDecision::Rejected),
        }
    }

    /// 追加完成快照后再以共享 control 提交 terminal，保证 journal 顺序可恢复。
    async fn finish_completed(
        &self,
        session_id: &str,
        prompt_id: &str,
        next_sequence: &mut u64,
        control: &TurnControl,
        assistant_text: &str,
    ) -> Result<acp::PromptResponse, TurnLoopError> {
        if !assistant_text.is_empty() {
            let sequence = allocate_sequence(next_sequence)?;
            let snapshot = SessionRecord::assistant_snapshot(
                sequence,
                prompt_id,
                "assistant-text",
                assistant_text,
                false,
            );
            if let Err(error) = self
                .repository
                .append(session_id, std::slice::from_ref(&snapshot))
                .await
            {
                return self
                    .finish_failed(
                        session_id,
                        prompt_id,
                        next_sequence,
                        control,
                        TurnLoopError::from(error),
                    )
                    .await;
            }
        }
        self.finish_terminal(
            session_id,
            prompt_id,
            next_sequence,
            control,
            TerminalKind::Completed,
        )
        .await
        .map(|(response, _)| response)
    }

    /// 所有终态先取得唯一提交权并写 TurnTerminal，再把 PromptResponse 交给 ACP dispatcher。
    async fn finish_terminal(
        &self,
        session_id: &str,
        prompt_id: &str,
        next_sequence: &mut u64,
        control: &TurnControl,
        requested: TerminalKind,
    ) -> Result<(acp::PromptResponse, TerminalKind), TurnLoopError> {
        let claim = control.claim_terminal(requested);
        if claim.should_persist {
            let sequence = allocate_sequence(next_sequence)?;
            let (status, stop_reason) = terminal_wire_values(claim.kind);
            let terminal = SessionRecord::turn_terminal(sequence, prompt_id, status);
            self.repository
                .append(session_id, std::slice::from_ref(&terminal))
                .await
                .map_err(TurnLoopError::from)?;
            tracing::debug!(
                event = "turn_terminal_persisted",
                "turn terminal 已写入 journal"
            );
            #[cfg(debug_assertions)]
            if let Some(test_seam) = &self.test_seam {
                test_seam.mark("terminal_committed");
                test_seam.wait_if_enabled("after_terminal_claim").await;
            }
            Ok((acp::PromptResponse::new(stop_reason), claim.kind))
        } else {
            // 防御性分支：同一 control 不重复追加 terminal，调用方复用已提交结果。
            let (_, stop_reason) = terminal_wire_values(claim.kind);
            Ok((acp::PromptResponse::new(stop_reason), claim.kind))
        }
    }

    /// 模型/transport 失败时仍先尝试写固定 failed terminal，错误正文不向 ACP 暴露。
    async fn finish_failed(
        &self,
        session_id: &str,
        prompt_id: &str,
        next_sequence: &mut u64,
        control: &TurnControl,
        cause: TurnLoopError,
    ) -> Result<acp::PromptResponse, TurnLoopError> {
        match self
            .finish_terminal(
                session_id,
                prompt_id,
                next_sequence,
                control,
                TerminalKind::Failed,
            )
            .await
        {
            Ok((response, TerminalKind::Cancelled)) => Ok(response),
            Ok(_) => Err(cause),
            Err(error) => Err(error),
        }
    }

    /// 取消不重试任何阶段，并将 cancelled terminal 作为 prompt 的唯一成功结果。
    async fn finish_cancelled(
        &self,
        session_id: &str,
        prompt_id: &str,
        next_sequence: &mut u64,
        control: &TurnControl,
    ) -> Result<acp::PromptResponse, TurnLoopError> {
        tracing::debug!(event = "turn_cancelled", "turn 收到取消信号");
        self.finish_terminal(
            session_id,
            prompt_id,
            next_sequence,
            control,
            TerminalKind::Cancelled,
        )
        .await
        .map(|(response, _)| response)
    }
}

/// 工具回合的安全结果；Rejected 不会继续调用模型，也不会执行工具。
enum ToolRoundResult {
    Continue(Vec<Value>),
    Cancelled,
    Refused,
}

/// permission 的内部判定只保留固定状态，不保留 Host 原始 reply。
#[derive(Clone, Copy)]
enum PermissionDecision {
    Allowed,
    Rejected,
    Cancelled,
}

/// 生成每个 update/permission 使用的顶层 `_meta.promptId`。
fn prompt_meta(prompt_id: &str) -> acp::Meta {
    let mut meta = acp::Meta::new();
    meta.insert("promptId".to_owned(), Value::String(prompt_id.to_owned()));
    meta
}

/// 单调分配 journal sequence，溢出时 fail-closed。
fn allocate_sequence(next_sequence: &mut u64) -> Result<u64, TurnLoopError> {
    let sequence = next_sequence.checked_add(1).ok_or(TurnLoopError::Session)?;
    *next_sequence = sequence;
    Ok(sequence)
}

/// 限制 assistant 累积快照大小，避免模型正文穿透 journal line 上限。
fn append_bounded(target: &mut String, delta: &str) -> bool {
    let Some(next_size) = target.len().checked_add(delta.len()) else {
        return false;
    };
    if next_size > MAX_ASSISTANT_TEXT_BYTES {
        return false;
    }
    target.push_str(delta);
    true
}

/// 只允许 noop 的空 object 参数；拒绝任何可能把敏感参数带入工具路径的值。
fn valid_noop_arguments(arguments: &str) -> bool {
    if arguments.trim().is_empty() {
        return true;
    }
    serde_json::from_str::<Value>(arguments)
        .ok()
        .and_then(|value| value.as_object().map(|object| object.is_empty()))
        .unwrap_or(false)
}

/// 为模型构造有界 MCP 结果；journal 只保存固定摘要，不写入真实返回正文。
fn safe_mcp_result_for_model(result: &McpCallResult) -> String {
    let Ok(encoded) = serde_json::to_string(result) else {
        return MCP_TOOL_FAILURE.to_owned();
    };
    if encoded.len() <= MAX_MCP_MODEL_RESULT_BYTES {
        encoded
    } else {
        MCP_TOOL_FAILURE.to_owned()
    }
}

/// 仅允许 noop 或 contract qualified MCP 名称进入可恢复 transcript。
pub(crate) fn is_safe_transcript_tool_name(name: &str) -> bool {
    (name == NOOP_TOOL || contract_is_qualified_tool_name(name))
        && name.len() <= MAX_RECORD_ID_BYTES
}

/// 将已持久化的白名单 records 转成模型上下文，并按 round 恢复成对消息。
fn transcript_messages(records: &[SessionRecord]) -> Vec<Value> {
    let mut messages = vec![json!({
        "role": "system",
        "content": crate::MINIMAL_SYSTEM_PROMPT
    })];
    let mut groups: Vec<(&SessionRecord, Vec<&SessionRecord>)> = Vec::new();

    // User 记录是 turn 边界；每个边界内独立恢复 assistant/tool 消息，避免跨 prompt 聚合。
    for record in records {
        if matches!(record, SessionRecord::User { .. }) {
            groups.push((record, Vec::new()));
        } else if let Some((_, events)) = groups.last_mut() {
            events.push(record);
        }
    }

    for (user, events) in groups {
        let SessionRecord::User { text, .. } = user else {
            continue;
        };
        messages.push(json!({ "role": "user", "content": text }));
        let mut rounds = BTreeMap::<u32, TranscriptRound>::new();
        // journal 已按 sequence 严格递增；marker 会消费它之前、同一 User 边界内的
        // snapshot run，因此同一工具 round 的文本只由 assistant tool_calls 消息承载。
        let mut pending_snapshot_text = None::<String>;

        for record in events {
            match record {
                SessionRecord::AssistantToolCalls {
                    round,
                    tool_calls,
                    text,
                    ..
                } => {
                    // journal 中 tool-call marker 位于流式 snapshot 之后；丢弃 marker 前的
                    // snapshot，避免 Chat Completions assistant 内容在恢复时重复。
                    pending_snapshot_text = None;
                    let entry = rounds.entry(*round).or_default();
                    if entry.assistant_calls.is_some() {
                        // 同一 round 出现多个 assistant marker 时无法证明唯一的调用集合。
                        entry.invalid = true;
                    }
                    let mut ids = BTreeSet::new();
                    let mut calls = Vec::with_capacity(tool_calls.len());
                    if tool_calls.is_empty() {
                        entry.invalid = true;
                    }
                    for call in tool_calls {
                        let safe = is_safe_transcript_tool_id(&call.tool_call_id)
                            && is_safe_transcript_tool_name(&call.name)
                            && ids.insert(call.tool_call_id.as_str());
                        if !safe {
                            entry.invalid = true;
                        }
                        calls.push((call.tool_call_id.clone(), call.name.clone()));
                    }
                    entry.assistant_calls = Some(calls);
                    entry.text = text.clone();
                }
                SessionRecord::Tool {
                    round,
                    tool_call_id,
                    name,
                    status,
                    ..
                } => {
                    let entry = rounds.entry(*round).or_default();
                    if !is_safe_transcript_tool_id(tool_call_id)
                        || !is_safe_transcript_tool_name(name)
                    {
                        // unsafe record 必须让整个 round 失效，不能只丢弃这一条。
                        entry.invalid = true;
                    }
                    if entry
                        .tool_records
                        .iter()
                        .any(|record| record.tool_call_id == *tool_call_id && record.name != *name)
                    {
                        // 同一调用 id 绑定多个工具名时，assistant/tool 配对不再可信。
                        entry.invalid = true;
                    }
                    entry.tool_records.push(TranscriptToolRecord {
                        tool_call_id: tool_call_id.clone(),
                        name: name.clone(),
                        status: status.clone(),
                    });
                }
                SessionRecord::AssistantSnapshot { text, .. } => {
                    pending_snapshot_text = Some(text.clone());
                }
                SessionRecord::User { .. } | SessionRecord::TurnTerminal { .. } => {}
            }
        }
        let final_text = pending_snapshot_text;

        // assistant marker 与所有 Tool record 必须组成一个完整、可持久化的 round。
        for round in rounds.into_values() {
            let calls = round
                .assistant_calls
                .clone()
                .unwrap_or_else(|| unique_tool_calls(&round.tool_records));
            if round.invalid
                || calls.is_empty()
                || round.tool_records.is_empty()
                || !tool_records_match_calls(&calls, &round.tool_records)
                || calls.iter().any(|(id, _)| {
                    !round.tool_records.iter().any(|record| {
                        record.tool_call_id == *id && record.status == TERMINAL_COMPLETED
                    })
                })
            {
                continue;
            }
            let tool_calls = calls
                .iter()
                .map(|(id, name)| {
                    json!({
                        "id": id,
                        "type": "function",
                        "function": { "name": name, "arguments": "{}" }
                    })
                })
                .collect::<Vec<_>>();
            messages.push(json!({
                "role": "assistant",
                "content": if round.text.is_empty() {
                    Value::Null
                } else {
                    Value::String(round.text)
                },
                "tool_calls": tool_calls
            }));
            for (tool_call_id, name) in calls {
                let content = if name == NOOP_TOOL {
                    TOOL_RESULT
                } else {
                    MCP_TOOL_RESULT
                };
                messages.push(json!({
                    "role": "tool",
                    "tool_call_id": tool_call_id,
                    "content": content
                }));
            }
        }

        if let Some(text) = final_text.filter(|text| !text.is_empty()) {
            // marker 之前的 snapshot 已在上面按 journal sequence 关联到工具 round；此处的
            // pending snapshot 位于最后一个 marker 之后，始终是后续普通 assistant 消息。
            messages.push(json!({ "role": "assistant", "content": text }));
        }
    }
    messages
}

/// tool-call id 复用 session journal 的 identifier 边界，防止恢复生成不可持久化消息。
fn is_safe_transcript_tool_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_RECORD_ID_BYTES && !value.chars().any(char::is_control)
}

/// 从旧 Tool-only journal 按首次出现顺序去重调用元数据。
fn unique_tool_calls(records: &[TranscriptToolRecord]) -> Vec<(String, String)> {
    let mut calls = Vec::new();
    for record in records {
        if !calls.iter().any(|(id, _)| id == &record.tool_call_id) {
            calls.push((record.tool_call_id.clone(), record.name.clone()));
        }
    }
    calls
}

/// 校验 assistant 调用集合与全部 Tool record 的 id/name 关系。
fn tool_records_match_calls(calls: &[(String, String)], records: &[TranscriptToolRecord]) -> bool {
    records.iter().all(|record| {
        calls
            .iter()
            .any(|(id, name)| id == &record.tool_call_id && name == &record.name)
    })
}

#[derive(Default)]
struct TranscriptRound {
    assistant_calls: Option<Vec<(String, String)>>,
    tool_records: Vec<TranscriptToolRecord>,
    invalid: bool,
    text: String,
}

struct TranscriptToolRecord {
    tool_call_id: String,
    name: String,
    status: String,
}

/// 构造当前模型回合的 assistant tool-call 消息；参数只留在模型内部请求，不进日志/journal。
fn assistant_tool_message(
    text: &str,
    reasoning: &str,
    calls: &[ModelToolCall],
) -> Result<Value, TurnLoopError> {
    let mut tool_calls = Vec::with_capacity(calls.len());
    for call in calls {
        let id = call.id.as_deref().ok_or(TurnLoopError::ToolRejected)?;
        let name = call.name.as_deref().ok_or(TurnLoopError::ToolRejected)?;
        let arguments = if call.arguments.trim().is_empty() {
            "{}"
        } else {
            call.arguments.as_str()
        };
        tool_calls.push(json!({
            "id": id,
            "type": "function",
            "function": {
                "name": name,
                "arguments": arguments
            }
        }));
    }
    let mut message = json!({
        "role": "assistant",
        "content": if text.is_empty() { Value::Null } else { Value::String(text.to_owned()) },
        "tool_calls": tool_calls
    });
    if !reasoning.is_empty() {
        message["reasoning_content"] = Value::String(reasoning.to_owned());
    }
    Ok(message)
}

#[cfg(test)]
mod tests {
    use super::{
        NOOP_TOOL, SessionRecord, TerminalClaim, TerminalKind, TurnControl,
        is_safe_transcript_tool_name, transcript_messages,
    };
    use crate::session_store::MAX_RECORD_ID_BYTES;

    /// completion 与 cancel 的先后顺序必须在共享 control 上线性化，且迟到 terminal 不再持久化。
    #[test]
    fn completion_cancel_race_has_one_winning_terminal_claim() {
        let completion_first = TurnControl::new();
        let completed = completion_first.claim_terminal(TerminalKind::Completed);
        assert_eq!(
            completed,
            TerminalClaim {
                kind: TerminalKind::Completed,
                should_persist: true
            }
        );
        completion_first.request_cancel();
        assert_eq!(
            completion_first.claim_terminal(TerminalKind::Cancelled),
            TerminalClaim {
                kind: TerminalKind::Completed,
                should_persist: false
            }
        );

        let cancel_first = TurnControl::new();
        cancel_first.request_cancel();
        assert_eq!(
            cancel_first.claim_terminal(TerminalKind::Completed),
            TerminalClaim {
                kind: TerminalKind::Cancelled,
                should_persist: true
            }
        );
        assert_eq!(
            cancel_first.claim_terminal(TerminalKind::Cancelled),
            TerminalClaim {
                kind: TerminalKind::Cancelled,
                should_persist: false
            }
        );
    }

    /// tool segment 允许超过 64 bytes，但 server 与 qualified identifier 仍有独立边界。
    #[test]
    fn transcript_tool_name_validation_keeps_server_and_storage_boundaries() {
        let long_tool = format!("tool-{}", "x".repeat(65));
        assert_eq!(long_tool.len(), 70);
        assert!(is_safe_transcript_tool_name(&format!(
            "server__{long_tool}"
        )));
        assert!(!is_safe_transcript_tool_name(&format!(
            "{}__{long_tool}",
            "s".repeat(65)
        )));
        assert!(!is_safe_transcript_tool_name(&format!(
            "server__{}",
            "x".repeat(1023)
        )));
    }

    #[test]
    fn transcript_tool_name_validation_rejects_non_contract_qualified_names() {
        for invalid in [
            "server",
            "server__",
            "server__bad.name",
            "server__bad name",
            "server__search__extra",
            "GrokBuild:*",
        ] {
            assert!(
                !is_safe_transcript_tool_name(invalid),
                "非法 qualified tool name 不得进入 transcript/replay: {invalid:?}"
            );
        }
        assert!(is_safe_transcript_tool_name(NOOP_TOOL));
    }

    /// syntax 非法的 v1 工具 round 必须从模型 transcript 中整体隐藏。
    #[test]
    fn transcript_recovery_hides_invalid_qualified_tool_round() {
        let invalid_name = "server__bad.name";
        let records = vec![
            SessionRecord::user(1, "prompt-invalid", "use historical tool"),
            SessionRecord::assistant_tool_calls(
                2,
                "prompt-invalid",
                0,
                [("tool-1".to_owned(), invalid_name.to_owned())],
                "",
            ),
            SessionRecord::tool_in_round(
                3,
                "prompt-invalid",
                0,
                "tool-1",
                invalid_name,
                "historical tool",
                "completed",
            ),
        ];

        let messages = transcript_messages(&records);
        assert_eq!(
            messages
                .iter()
                .filter(|message| message["role"] == "user")
                .count(),
            1
        );
        assert!(
            messages
                .iter()
                .all(|message| { message["role"] != "tool" && !message["tool_calls"].is_array() })
        );
    }

    /// 超过共享 journal identifier 边界的工具 round 必须从模型 transcript 中整体隐藏。
    #[test]
    fn transcript_recovery_hides_unpersistable_tool_round() {
        let name = format!("server__{}", "x".repeat(MAX_RECORD_ID_BYTES));
        let records = vec![
            SessionRecord::user(1, "prompt-boundary", "use boundary tool"),
            SessionRecord::assistant_tool_calls(
                2,
                "prompt-boundary",
                0,
                [("tool-1".to_owned(), name.clone())],
                "",
            ),
            SessionRecord::tool_in_round(
                3,
                "prompt-boundary",
                0,
                "tool-1",
                name,
                "mcp tool completed",
                "completed",
            ),
        ];

        let messages = transcript_messages(&records);
        assert!(
            messages
                .iter()
                .all(|message| { message["role"] != "tool" && !message["tool_calls"].is_array() })
        );
    }

    /// 同一 v1 assistant tool round 混入不可持久化名称时，不能只恢复安全调用。
    #[test]
    fn transcript_recovery_skips_mixed_v1_tool_round_as_one_unit() {
        let unsafe_name = format!("server__{}", "x".repeat(MAX_RECORD_ID_BYTES));
        let records = vec![
            SessionRecord::user(1, "prompt-v1-mixed", "use tools"),
            SessionRecord::assistant_snapshot(
                2,
                "prompt-v1-mixed",
                "before-tools",
                "before tool round",
                true,
            ),
            SessionRecord::assistant_tool_calls(
                3,
                "prompt-v1-mixed",
                1,
                [
                    ("safe-call".to_owned(), "GrokBuild:efflab_noop".to_owned()),
                    ("unsafe-call".to_owned(), unsafe_name.clone()),
                ],
                "tool round text",
            ),
            SessionRecord::tool_in_round(
                4,
                "prompt-v1-mixed",
                1,
                "safe-call",
                "GrokBuild:efflab_noop",
                "safe completed",
                "completed",
            ),
            SessionRecord::tool_in_round(
                5,
                "prompt-v1-mixed",
                1,
                "unsafe-call",
                unsafe_name,
                "unsafe completed",
                "completed",
            ),
            SessionRecord::assistant_snapshot(
                6,
                "prompt-v1-mixed",
                "after-tools",
                "independent follow-up",
                false,
            ),
        ];

        let messages = transcript_messages(&records);
        assert!(messages.iter().any(|message| {
            message["role"] == "assistant" && message["content"] == "independent follow-up"
        }));
        assert!(
            !messages.iter().any(|message| {
                message["role"] == "assistant" && message["tool_calls"].is_array()
            })
        );
        assert!(!messages.iter().any(|message| message["role"] == "tool"));
        assert!(!messages.iter().any(|message| {
            message["role"] == "assistant" && message["content"] == "before tool round"
        }));
    }

    /// 旧 Tool-only v1 round 混入不可持久化名称时，也必须按整组跳过。
    #[test]
    fn transcript_recovery_skips_mixed_tool_only_round_as_one_unit() {
        let unsafe_name = format!("server__{}", "x".repeat(MAX_RECORD_ID_BYTES));
        let records = vec![
            SessionRecord::user(1, "prompt-legacy-mixed", "use tools"),
            SessionRecord::tool_in_round(
                2,
                "prompt-legacy-mixed",
                3,
                "safe-call",
                "GrokBuild:efflab_noop",
                "safe completed",
                "completed",
            ),
            SessionRecord::tool_in_round(
                3,
                "prompt-legacy-mixed",
                3,
                "unsafe-call",
                unsafe_name,
                "unsafe completed",
                "completed",
            ),
            SessionRecord::assistant_snapshot(
                4,
                "prompt-legacy-mixed",
                "after-tools",
                "legacy follow-up",
                false,
            ),
        ];

        let messages = transcript_messages(&records);
        assert!(messages.iter().any(|message| {
            message["role"] == "assistant" && message["content"] == "legacy follow-up"
        }));
        assert!(
            !messages.iter().any(|message| {
                message["role"] == "assistant" && message["tool_calls"].is_array()
            })
        );
        assert!(!messages.iter().any(|message| message["role"] == "tool"));
    }

    /// transcript 必须按 journal sequence 保留多个 tool call 与 tool result 的成对顺序。
    #[test]
    fn transcript_recovery_preserves_tool_call_journal_order() {
        let records = vec![
            SessionRecord::user(1, "prompt-tools", "use tools"),
            SessionRecord::tool(
                2,
                "prompt-tools",
                "tool-2",
                "GrokBuild:efflab_noop",
                "permission pending",
                "pending",
            ),
            SessionRecord::tool(
                3,
                "prompt-tools",
                "tool-10",
                "GrokBuild:efflab_noop",
                "permission pending",
                "pending",
            ),
            SessionRecord::tool(
                4,
                "prompt-tools",
                "tool-2",
                "GrokBuild:efflab_noop",
                "efflab noop completed",
                "completed",
            ),
            SessionRecord::tool(
                5,
                "prompt-tools",
                "tool-10",
                "GrokBuild:efflab_noop",
                "efflab noop completed",
                "completed",
            ),
        ];

        let messages = transcript_messages(&records);
        let assistant = messages
            .iter()
            .find(|message| message["role"] == "assistant")
            .expect("工具 transcript 必须恢复 assistant 消息");
        let tool_calls = assistant["tool_calls"]
            .as_array()
            .expect("assistant 消息必须包含 tool_calls");
        assert_eq!(
            tool_calls
                .iter()
                .filter_map(|call| call["id"].as_str())
                .collect::<Vec<_>>(),
            ["tool-2", "tool-10"]
        );

        let tool_results = messages
            .iter()
            .filter(|message| message["role"] == "tool")
            .map(|message| message["tool_call_id"].as_str())
            .collect::<Option<Vec<_>>>();
        assert_eq!(tool_results, Some(vec!["tool-2", "tool-10"]));
    }

    /// 工具 round 后的普通 assistant snapshot 即使文本相同也必须独立恢复。
    #[test]
    fn transcript_recovery_keeps_identical_follow_up_text_after_tool_round() {
        let records = vec![
            SessionRecord::user(1, "prompt-mixed", "mixed"),
            SessionRecord::assistant_snapshot(
                2,
                "prompt-mixed",
                "assistant-text",
                "same-round text",
                true,
            ),
            SessionRecord::assistant_tool_calls(
                3,
                "prompt-mixed",
                0,
                [("tool-4".to_owned(), "GrokBuild:efflab_noop".to_owned())],
                "same-round text",
            ),
            SessionRecord::tool_in_round(
                4,
                "prompt-mixed",
                0,
                "tool-4",
                "GrokBuild:efflab_noop",
                "efflab noop completed",
                "completed",
            ),
            SessionRecord::assistant_snapshot(
                5,
                "prompt-mixed",
                "assistant-text",
                "same-round text",
                false,
            ),
            SessionRecord::turn_terminal(6, "prompt-mixed", "completed"),
        ];

        let messages = transcript_messages(&records);
        assert_eq!(
            messages
                .iter()
                .filter(|message| {
                    message["role"] == "assistant" && message["content"] == "same-round text"
                })
                .count(),
            2,
            "工具 round 文本与相同文本的后续普通 assistant snapshot 都必须保留: {messages:?}"
        );
        let tool_call_index = messages
            .iter()
            .position(|message| message["role"] == "assistant" && message["tool_calls"].is_array())
            .expect("必须恢复 assistant tool_calls");
        assert_eq!(messages[tool_call_index]["content"], "same-round text");
        assert_eq!(
            messages[tool_call_index]["tool_calls"][0]["function"]["arguments"],
            "{}"
        );
        assert_eq!(messages[tool_call_index + 1]["role"], "tool");
        assert_eq!(messages[tool_call_index + 1]["tool_call_id"], "tool-4");
        let follow_up = messages
            .iter()
            .skip(tool_call_index + 2)
            .find(|message| message["role"] == "assistant")
            .expect("相同文本的后续普通 assistant 不得被全局去重误删");
        assert_eq!(follow_up["content"], "same-round text");
        assert!(follow_up.get("tool_calls").is_none() || follow_up["tool_calls"].is_null());
    }

    /// 后续普通 assistant 文本与工具 round 文本不同时必须保留，且参数仍固定为安全 `{}`。
    #[test]
    fn transcript_recovery_keeps_follow_up_assistant_text() {
        let records = vec![
            SessionRecord::user(1, "prompt-mixed", "mixed"),
            SessionRecord::assistant_snapshot(
                2,
                "prompt-mixed",
                "assistant-text",
                "same-round text",
                true,
            ),
            SessionRecord::assistant_tool_calls(
                3,
                "prompt-mixed",
                0,
                [("tool-4".to_owned(), "GrokBuild:efflab_noop".to_owned())],
                "same-round text",
            ),
            SessionRecord::tool_in_round(
                4,
                "prompt-mixed",
                0,
                "tool-4",
                "GrokBuild:efflab_noop",
                "efflab noop completed",
                "completed",
            ),
            SessionRecord::assistant_snapshot(
                5,
                "prompt-mixed",
                "assistant-text",
                "follow-up assistant",
                false,
            ),
            SessionRecord::turn_terminal(6, "prompt-mixed", "completed"),
        ];

        let messages = transcript_messages(&records);
        let tool_call_index = messages
            .iter()
            .position(|message| message["role"] == "assistant" && message["tool_calls"].is_array())
            .expect("必须恢复 assistant tool_calls");
        assert_eq!(messages[tool_call_index]["content"], "same-round text");
        assert_eq!(messages[tool_call_index + 1]["role"], "tool");
        let follow_up = messages
            .iter()
            .skip(tool_call_index + 2)
            .find(|message| message["role"] == "assistant")
            .expect("后续普通 assistant 文本不得被去重误删");
        assert_eq!(follow_up["content"], "follow-up assistant");
        assert!(follow_up.get("tool_calls").is_none() || follow_up["tool_calls"].is_null());
        assert_eq!(
            messages[tool_call_index]["tool_calls"][0]["function"]["arguments"],
            "{}"
        );
    }
}
