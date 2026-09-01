//! sidecar 的最小 ACP Agent 实现。
//!
//! 本模块只负责 ACP 边界、session 生命周期和 prompt single-flight；实际模型/工具回合由
//! `turn_loop` 处理。它不读取 shell 配置、不启动 MCP 子进程，也不把敏感正文写入日志。

use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    rc::Rc,
    time::Duration,
};

use agent_client_protocol as acp;
use efflab_agent_contract::is_prompt_id;
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::Notify;
use xai_acp_lib::AcpGatewaySender;

use crate::mcp_client::McpRuntime;
use crate::model_client::HttpModelClient;
use crate::observability;
use crate::session_store::{Session, SessionError, SessionRecord, SessionRepository};
#[cfg(debug_assertions)]
use crate::test_seam::TestSeam;
use crate::turn_loop::{TurnControl, TurnLoop, TurnLoopError, is_safe_transcript_tool_name};

/// 当前唯一允许由 ACP wire 调用的扩展逻辑名称；wire 层会带一个前导下划线。
const MCP_LIST_METHOD: &str = "x.ai/mcp/list";
/// M1 prompt 的 Unicode scalar 总数上限，与 Host capability 合同保持一致。
const MAX_PROMPT_CHARS: usize = 32_000;
/// 当前 Host profile 唯一允许的 Channel 槽名，不接受供应商模型标识。
const ACP_BYOK_MODEL_SLOT: &str = "byok";
/// sidecar 当前唯一可执行的无副作用工具。
const NOOP_TOOL: &str = "GrokBuild:efflab_noop";
/// pending admission 与 cancel latch 都只覆盖短暂窗口，并设置全局上限防止输入耗尽内存。
const MAX_PENDING_ADMISSIONS: usize = 128;
const MAX_CANCEL_LATCHES: usize = 128;

/// 将所有请求拒绝收敛为无 data 的固定错误，并只在日志记录稳定原因。
fn rejected_params(reason: &'static str) -> acp::Error {
    tracing::debug!(event = "acp_request_rejected", reason, "拒绝 ACP 请求");
    acp::Error::invalid_params()
}

/// 进程内兼容 session；生产 runtime 使用 v1 SessionRepository。
#[derive(Debug, Clone)]
struct MemorySession {
    cwd: PathBuf,
}

/// 一个正在运行的 prompt 的 admission 槽位；control 同时负责取消与 terminal 线性化。
struct ActivePrompt {
    prompt_id: String,
    epoch: u64,
    control: TurnControl,
}

/// 一个已经确认存在、但尚未转入 active 的 prompt admission。
struct PendingAdmission {
    prompt_id: String,
    epoch: u64,
    /// 只有已确认真实 session 的 admission 才允许 cancel 创建 latch。
    confirmed: bool,
    cancelled: bool,
}

/// 只绑定某个 pending admission epoch 的取消闩；不会独立代表 session 取消。
#[derive(Clone, Copy)]
struct CancelLatch {
    epoch: u64,
}

/// MinimalAgent 的单线程可变状态。
struct RuntimeState {
    expected_cwd: PathBuf,
    next_session_number: u64,
    sessions: BTreeMap<String, MemorySession>,
    repository: Option<SessionRepository>,
    model: Option<Rc<HttpModelClient>>,
    expected_tools: BTreeSet<String>,
    ready_tools: BTreeSet<String>,
    mcp: Option<McpRuntime>,
    gateway: Option<AcpGatewaySender<acp::AgentSide>>,
    known_sessions: BTreeSet<String>,
    pending_admissions: BTreeMap<String, PendingAdmission>,
    active_prompts: BTreeMap<String, ActivePrompt>,
    cancel_latches: BTreeMap<String, CancelLatch>,
    next_admission_epoch: u64,
    shutting_down: bool,
    active_changed: Rc<Notify>,
    /// debug 构建中用于控制异步窗口并记录执行点的测试 seam。
    #[cfg(debug_assertions)]
    test_seam: Option<TestSeam>,
}

/// 从 agent 状态复制出的 runtime 依赖，确保 await 期间不持有 RefCell 借用。
struct RuntimeDependencies {
    repository: SessionRepository,
    model: Rc<HttpModelClient>,
    mcp: McpRuntime,
    gateway: Option<AcpGatewaySender<acp::AgentSide>>,
    expected_tools: BTreeSet<String>,
    ready_tools: BTreeSet<String>,
}

/// 在 ACP current-thread LocalSet 中运行的最小 Agent。
#[derive(Clone)]
pub struct MinimalAgent {
    state: Rc<RefCell<RuntimeState>>,
}

/// `_x.ai/mcp/list` 的最小参数；只接受当前进程已创建的 session。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct McpListParams {
    session_id: acp::SessionId,
}

impl MinimalAgent {
    /// 创建没有 runtime 依赖的兼容 Agent；该模式保留旧单元/黑盒边界。
    pub fn new(expected_cwd: PathBuf) -> Self {
        Self {
            state: Rc::new(RefCell::new(RuntimeState {
                expected_cwd,
                next_session_number: 1,
                sessions: BTreeMap::new(),
                repository: None,
                model: None,
                expected_tools: BTreeSet::new(),
                ready_tools: BTreeSet::new(),
                mcp: None,
                gateway: None,
                known_sessions: BTreeSet::new(),
                pending_admissions: BTreeMap::new(),
                active_prompts: BTreeMap::new(),
                cancel_latches: BTreeMap::new(),
                next_admission_epoch: 1,
                shutting_down: false,
                active_changed: Rc::new(Notify::new()),
                #[cfg(debug_assertions)]
                test_seam: None,
            })),
        }
    }

    /// 创建兼容 runtime Agent；未传入 MCP 时只保留固定 noop 工具。
    pub fn with_runtime(
        expected_cwd: PathBuf,
        repository: SessionRepository,
        model: HttpModelClient,
        expected_tools: BTreeSet<String>,
    ) -> Self {
        Self::with_runtime_and_mcp(
            expected_cwd,
            repository,
            model,
            expected_tools,
            McpRuntime::empty(),
        )
    }

    /// 创建生产 runtime Agent，并共享同一个可取消 MCP runtime 到每个 prompt loop。
    pub fn with_runtime_and_mcp(
        expected_cwd: PathBuf,
        repository: SessionRepository,
        model: HttpModelClient,
        expected_tools: BTreeSet<String>,
        mcp: McpRuntime,
    ) -> Self {
        // runtime config 的 expected_tools 只描述 Host 批准的 MCP；内置 noop 是固定批准成员。
        let mut approved_tools = expected_tools;
        approved_tools.insert(NOOP_TOOL.to_owned());
        // MCP runtime 已经完成 actual catalog 与 expected 的交集；这里仅加入固定 noop。
        let mut ready_tools = mcp
            .model_visible_tools()
            .into_iter()
            .collect::<BTreeSet<_>>();
        ready_tools.insert(NOOP_TOOL.to_owned());
        Self {
            state: Rc::new(RefCell::new(RuntimeState {
                expected_cwd,
                next_session_number: 1,
                sessions: BTreeMap::new(),
                repository: Some(repository),
                model: Some(Rc::new(model)),
                expected_tools: approved_tools,
                ready_tools,
                mcp: Some(mcp),
                gateway: None,
                known_sessions: BTreeSet::new(),
                pending_admissions: BTreeMap::new(),
                active_prompts: BTreeMap::new(),
                cancel_latches: BTreeMap::new(),
                next_admission_epoch: 1,
                shutting_down: false,
                active_changed: Rc::new(Notify::new()),
                #[cfg(debug_assertions)]
                test_seam: None,
            })),
        }
    }

    /// 在 ACP connection 创建后安装唯一的 agent-to-client gateway 出口。
    pub fn install_gateway(&self, gateway: AcpGatewaySender<acp::AgentSide>) {
        self.state.borrow_mut().gateway = Some(gateway);
        tracing::debug!(event = "acp_gateway_installed", "agent gateway 已安装");
    }

    /// 安装 debug 构建专用测试 seam；release 构建不存在该接缝。
    #[cfg(debug_assertions)]
    pub(crate) fn install_test_seam(&self, test_seam: Option<TestSeam>) {
        self.state.borrow_mut().test_seam = test_seam;
    }

    /// EOF 或 transport 失败时关闭 prompt admission，并请求 active/pending prompt 取消。
    pub fn begin_shutdown(&self) {
        let controls = {
            let mut state = self.state.borrow_mut();
            state.shutting_down = true;
            for pending in state.pending_admissions.values_mut() {
                pending.cancelled = true;
            }
            // shutdown 已经取消所有 pending admission，旧 latch 不得跨越清理边界保留。
            state.cancel_latches.clear();
            state
                .active_prompts
                .values()
                .map(|active| active.control.clone())
                .collect::<Vec<_>>()
        };
        let count = controls.len();
        for control in controls {
            control.request_cancel();
        }
        self.state.borrow().active_changed.notify_waiters();
        tracing::debug!(
            event = "prompt_admission_closed",
            active_count = count,
            "关闭 prompt admission 并请求取消"
        );
    }

    /// 保留旧调用名；清理语义统一经过 admission barrier。
    pub fn cancel_all(&self) {
        self.begin_shutdown();
    }

    /// 返回当前是否仍有 prompt admission 或 active prompt 未完成 terminal journal。
    fn has_active_prompts(&self) -> bool {
        let state = self.state.borrow();
        !state.active_prompts.is_empty() || !state.pending_admissions.is_empty()
    }

    /// 在有界时间内等待 prompt 完成并由 release_prompt 移除其 admission 槽位。
    pub async fn wait_for_active_prompts(&self, timeout: Duration) -> bool {
        let notify = self.state.borrow().active_changed.clone();
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if !self.has_active_prompts() {
                return true;
            }
            let notified = notify.notified();
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                let drained = !self.has_active_prompts();
                tracing::debug!(
                    event = "active_prompt_drain_timeout",
                    drained,
                    "active prompt 有界 drain 结束"
                );
                return drained;
            }
        }
    }

    /// 校验 session 请求没有越过固定 cwd、额外目录或 MCP 边界。
    fn validate_session_scope(
        &self,
        cwd: &std::path::Path,
        additional_directories: &[PathBuf],
        mcp_servers: &[acp::McpServer],
    ) -> acp::Result<()> {
        let state = self.state.borrow();
        if cwd != state.expected_cwd {
            return Err(rejected_params("session_cwd_mismatch"));
        }
        if !additional_directories.is_empty() {
            return Err(rejected_params("additional_directories_not_allowed"));
        }
        if !mcp_servers.is_empty() {
            return Err(rejected_params("mcp_servers_not_allowed"));
        }
        Ok(())
    }

    /// 校验 session 级 `_meta` 只携带可选的非空 Channel 槽名。
    fn validate_session_meta(meta: Option<&acp::Meta>) -> acp::Result<()> {
        let Some(meta) = meta else {
            return Ok(());
        };
        if meta.keys().any(|key| key != "modelId") {
            return Err(rejected_params("session_meta_key_not_allowed"));
        }
        if let Some(model_id) = meta.get("modelId") {
            let valid = model_id
                .as_str()
                .is_some_and(|value| value == ACP_BYOK_MODEL_SLOT);
            if !valid {
                return Err(rejected_params("session_model_id_invalid"));
            }
        }
        Ok(())
    }

    /// 校验 prompt 的顶层 `_meta` 必须是唯一且受限的非空 promptId。
    fn prompt_id(meta: Option<&acp::Meta>) -> acp::Result<String> {
        let Some(meta) = meta else {
            return Err(rejected_params("prompt_id_missing"));
        };
        let Some(value) = meta.get("promptId").and_then(Value::as_str) else {
            return Err(rejected_params("prompt_meta_invalid"));
        };
        if meta.len() != 1 || !is_prompt_id(value) {
            return Err(rejected_params("prompt_meta_invalid"));
        }
        Ok(value.to_owned())
    }

    /// 校验 prompt 只包含非空纯文本，并限制总 Unicode scalar 数量。
    fn validate_prompt_blocks(prompt: &[acp::ContentBlock]) -> acp::Result<()> {
        if prompt.is_empty() {
            return Err(rejected_params("prompt_empty"));
        }

        let mut total_chars = 0_usize;
        for block in prompt {
            let acp::ContentBlock::Text(text) = block else {
                return Err(rejected_params("prompt_content_type_not_allowed"));
            };
            if text.annotations.is_some() {
                return Err(rejected_params("prompt_annotations_not_allowed"));
            }
            if text.meta.is_some() {
                return Err(rejected_params("prompt_block_meta_not_allowed"));
            }
            if text.text.is_empty() {
                return Err(rejected_params("prompt_text_empty"));
            }
            let Some(next_chars) = total_chars.checked_add(text.text.chars().count()) else {
                return Err(rejected_params("prompt_too_large"));
            };
            if next_chars > MAX_PROMPT_CHARS {
                return Err(rejected_params("prompt_too_large"));
            }
            total_chars = next_chars;
        }
        Ok(())
    }

    /// 在已校验 prompt blocks 上拼接 user 文本，不重新解释 ACP union。
    fn prompt_text(prompt: &[acp::ContentBlock]) -> String {
        let mut text = String::new();
        for block in prompt {
            if let acp::ContentBlock::Text(content) = block {
                text.push_str(&content.text);
            }
        }
        text
    }

    /// 构造固定 handshake 的 `_meta`，不携带客户端输入或 runtime config 原文。
    fn handshake_meta() -> acp::Meta {
        let mut meta = acp::Meta::new();
        meta.insert(
            "efflabRuntime".to_owned(),
            Value::String("minimal-v1".to_owned()),
        );
        meta.insert("efflabSchemaVersion".to_owned(), Value::from(1_u64));
        meta.insert("efflabSessionStoreVersion".to_owned(), Value::from(1_u64));
        meta
    }

    /// 返回已复制的 runtime 依赖；调用方可以安全跨 await 使用。
    fn runtime_dependencies(&self) -> Option<RuntimeDependencies> {
        let state = self.state.borrow();
        Some(RuntimeDependencies {
            repository: state.repository.clone()?,
            model: state.model.clone()?,
            mcp: state.mcp.clone()?,
            gateway: state.gateway.clone(),
            expected_tools: state.expected_tools.clone(),
            ready_tools: state.ready_tools.clone(),
        })
    }

    /// 复制 debug 测试 seam，避免跨 await 持有状态借用。
    #[cfg(debug_assertions)]
    fn test_seam(&self) -> Option<TestSeam> {
        self.state.borrow().test_seam.clone()
    }

    /// 创建新的内存 session，并返回其稳定摘要。
    fn create_memory_session(&self, cwd: PathBuf) -> acp::Result<String> {
        let mut state = self.state.borrow_mut();
        let number = state.next_session_number;
        let next_number = number
            .checked_add(1)
            .ok_or_else(acp::Error::internal_error)?;
        let session_id = format!("memory-{number}");
        state.next_session_number = next_number;
        state
            .sessions
            .insert(session_id.clone(), MemorySession { cwd });
        state.known_sessions.insert(session_id.clone());
        Ok(session_id)
    }

    /// 记录一次已由 repository 或 session/new/load 确认的 session。
    fn remember_session(&self, session_id: &str) {
        self.state
            .borrow_mut()
            .known_sessions
            .insert(session_id.to_owned());
    }

    /// 判断 session 是否已经通过当前 runtime 的存在性确认。
    fn has_confirmed_session(&self, session_id: &str) -> bool {
        self.state.borrow().known_sessions.contains(session_id)
    }

    /// 确认 prompt 目标 session 真实存在，并把 pending admission 标记为已确认。
    async fn confirm_prompt_session(
        &self,
        session_id: &str,
        epoch: u64,
        dependencies: Option<&RuntimeDependencies>,
    ) -> bool {
        if self.has_confirmed_session(session_id) {
            self.mark_admission_confirmed(session_id, epoch);
            return true;
        }
        #[cfg(debug_assertions)]
        if let Some(test_seam) = self.test_seam() {
            // 测试只在明确启用时暂停；生产 release 不编译该分支。
            test_seam
                .wait_if_enabled("before_session_confirmation")
                .await;
        }
        let exists = self
            .session_exists(&acp::SessionId::new(session_id), dependencies)
            .await;
        if exists {
            self.remember_session(session_id);
            self.mark_admission_confirmed(session_id, epoch);
        }
        exists
    }

    /// 判断 session 是否存在且属于当前内存 runtime。
    fn has_memory_session(&self, session_id: &acp::SessionId) -> bool {
        self.state
            .borrow()
            .sessions
            .contains_key(session_id.0.as_ref())
    }

    /// 建立一个有界 pending admission；epoch 是 cancel latch 的唯一绑定条件。
    fn begin_prompt_admission(&self, session_id: &str, prompt_id: &str) -> acp::Result<u64> {
        let mut state = self.state.borrow_mut();
        if state.active_prompts.contains_key(session_id)
            || state.pending_admissions.contains_key(session_id)
        {
            return Err(rejected_params("prompt_already_active"));
        }
        if state.pending_admissions.len() >= MAX_PENDING_ADMISSIONS {
            return Err(rejected_params("prompt_admission_limit"));
        }
        let epoch = state.next_admission_epoch;
        state.next_admission_epoch = epoch
            .checked_add(1)
            .ok_or_else(acp::Error::internal_error)?;
        // 同一 session 的旧 latch 不能跨越新的 admission epoch。
        state.cancel_latches.remove(session_id);
        let cancelled = state.shutting_down;
        let confirmed = state.known_sessions.contains(session_id);
        state.pending_admissions.insert(
            session_id.to_owned(),
            PendingAdmission {
                prompt_id: prompt_id.to_owned(),
                epoch,
                confirmed,
                cancelled,
            },
        );
        Ok(epoch)
    }

    /// 在真实 session 存在后标记同一 admission epoch，供 cancel 安全建立 latch。
    fn mark_admission_confirmed(&self, session_id: &str, epoch: u64) {
        let mut state = self.state.borrow_mut();
        if let Some(pending) = state.pending_admissions.get_mut(session_id)
            && pending.epoch == epoch
        {
            pending.confirmed = true;
        }
    }

    /// session 确认失败时删除对应 pending admission 与其 epoch latch，并唤醒 EOF drain。
    fn reject_pending_admission(&self, session_id: &str, prompt_id: &str, epoch: u64) {
        let notify = {
            let mut state = self.state.borrow_mut();
            let should_remove = state
                .pending_admissions
                .get(session_id)
                .is_some_and(|pending| pending.prompt_id == prompt_id && pending.epoch == epoch);
            if should_remove {
                state.pending_admissions.remove(session_id);
                if state
                    .cancel_latches
                    .get(session_id)
                    .is_some_and(|latch| latch.epoch == epoch)
                {
                    state.cancel_latches.remove(session_id);
                }
            }
            state.active_changed.clone()
        };
        notify.notify_waiters();
    }

    /// 仅为同一 pending admission epoch 记录取消，并限制 latch 总量。
    fn latch_pending_admission(&self, session_id: &str, epoch: u64) -> bool {
        let mut state = self.state.borrow_mut();
        let valid_pending = state
            .pending_admissions
            .get(session_id)
            .is_some_and(|pending| pending.epoch == epoch && pending.confirmed);
        if !valid_pending {
            return false;
        }
        if state.shutting_down {
            if let Some(pending) = state.pending_admissions.get_mut(session_id) {
                pending.cancelled = true;
            }
            return false;
        }
        if state.cancel_latches.len() < MAX_CANCEL_LATCHES
            || state.cancel_latches.contains_key(session_id)
        {
            state
                .cancel_latches
                .insert(session_id.to_owned(), CancelLatch { epoch });
            true
        } else {
            // 达到上限时将取消状态压在现有 admission 上，不扩大 latch 内存。
            if let Some(pending) = state.pending_admissions.get_mut(session_id) {
                pending.cancelled = true;
            }
            false
        }
    }

    /// 在线性化点取消同一 admission epoch，避免异步 session 确认期间错过已 active 的 prompt。
    fn cancel_admission_epoch(&self, session_id: &str, epoch: u64) -> bool {
        let active_control = {
            let state = self.state.borrow();
            state
                .active_prompts
                .get(session_id)
                .filter(|active| active.epoch == epoch)
                .map(|active| active.control.clone())
        };
        if let Some(control) = active_control {
            control.request_cancel();
            return true;
        }
        self.latch_pending_admission(session_id, epoch)
    }

    /// 将 pending admission 转为 active prompt，并原子消费同 epoch 的 cancel latch。
    fn reserve_prompt(
        &self,
        session_id: &str,
        prompt_id: &str,
        epoch: u64,
        control: TurnControl,
    ) -> acp::Result<()> {
        let cancelled_before_reserve = {
            let mut state = self.state.borrow_mut();
            if state.active_prompts.contains_key(session_id) {
                return Err(rejected_params("prompt_already_active"));
            }
            let pending = state
                .pending_admissions
                .remove(session_id)
                .filter(|pending| pending.prompt_id == prompt_id && pending.epoch == epoch)
                .ok_or_else(|| rejected_params("prompt_admission_missing"))?;
            let cancelled = pending.cancelled
                || state
                    .cancel_latches
                    .get(session_id)
                    .is_some_and(|latch| latch.epoch == epoch);
            if state
                .cancel_latches
                .get(session_id)
                .is_some_and(|latch| latch.epoch == epoch)
            {
                state.cancel_latches.remove(session_id);
            }
            state.active_prompts.insert(
                session_id.to_owned(),
                ActivePrompt {
                    prompt_id: prompt_id.to_owned(),
                    epoch,
                    control: control.clone(),
                },
            );
            cancelled
        };
        if cancelled_before_reserve {
            control.request_cancel();
            tracing::debug!(
                event = "prompt_cancel_latch_consumed",
                "prompt reserve 消费绑定 admission epoch 的 cancel"
            );
        }
        tracing::debug!(event = "prompt_reserved", "为 prompt 预留 session turn");
        Ok(())
    }

    /// 只删除仍属于当前 promptId 的槽位，并清理其 epoch 绑定的 latch。
    fn release_prompt(&self, session_id: &str, prompt_id: &str) {
        let (should_remove, notify) = {
            let mut state = self.state.borrow_mut();
            let epoch = state
                .active_prompts
                .get(session_id)
                .filter(|active| active.prompt_id == prompt_id)
                .map(|active| active.epoch);
            let should_remove = epoch.is_some();
            if should_remove {
                state.active_prompts.remove(session_id);
                if state
                    .cancel_latches
                    .get(session_id)
                    .is_some_and(|latch| Some(latch.epoch) == epoch)
                {
                    state.cancel_latches.remove(session_id);
                }
            }
            (should_remove, state.active_changed.clone())
        };
        if should_remove {
            notify.notify_waiters();
            tracing::debug!(event = "prompt_released", "释放 session turn 槽位");
        }
    }

    /// 兼容内存 session 与 v1 repository 的存在性检查。
    async fn session_exists(
        &self,
        session_id: &acp::SessionId,
        dependencies: Option<&RuntimeDependencies>,
    ) -> bool {
        if let Some(dependencies) = dependencies {
            dependencies
                .repository
                .load(session_id.0.as_ref())
                .await
                .is_ok()
        } else {
            self.has_memory_session(session_id)
        }
    }

    /// 将 v1 journal 的可展示记录回放给 ACP client；assistant snapshot 按 promptId 折叠。
    async fn replay_session(
        &self,
        session: &Session,
        gateway: &AcpGatewaySender<acp::AgentSide>,
    ) -> acp::Result<()> {
        let mut latest_assistant = BTreeMap::<String, u64>::new();
        let mut latest_tools = BTreeMap::<String, u64>::new();
        for record in &session.records {
            match record {
                SessionRecord::AssistantSnapshot { prompt_id, .. } => {
                    latest_assistant.insert(prompt_id.clone(), record.sequence());
                }
                SessionRecord::Tool { tool_call_id, .. } => {
                    latest_tools.insert(tool_call_id.clone(), record.sequence());
                }
                SessionRecord::User { .. }
                | SessionRecord::AssistantToolCalls { .. }
                | SessionRecord::TurnTerminal { .. } => {}
            }
        }

        for record in &session.records {
            let notification = match record {
                SessionRecord::User {
                    prompt_id, text, ..
                } => Some(
                    acp::SessionNotification::new(
                        session.id.clone(),
                        acp::SessionUpdate::UserMessageChunk(acp::ContentChunk::new(
                            acp::ContentBlock::Text(acp::TextContent::new(text)),
                        )),
                    )
                    .meta(replay_prompt_meta(prompt_id)),
                ),
                SessionRecord::AssistantSnapshot {
                    sequence,
                    prompt_id,
                    text,
                    ..
                } if latest_assistant.get(prompt_id) == Some(sequence) => Some(
                    acp::SessionNotification::new(
                        session.id.clone(),
                        acp::SessionUpdate::AgentMessageChunk(acp::ContentChunk::new(
                            acp::ContentBlock::Text(acp::TextContent::new(text)),
                        )),
                    )
                    .meta(replay_prompt_meta(prompt_id)),
                ),
                SessionRecord::Tool {
                    sequence,
                    prompt_id,
                    tool_call_id,
                    name,
                    status,
                    ..
                } if latest_tools.get(tool_call_id) == Some(sequence)
                    && is_safe_transcript_tool_name(name) =>
                {
                    let status = replay_tool_status(status);
                    let update = acp::ToolCallUpdate::new(
                        tool_call_id.clone(),
                        acp::ToolCallUpdateFields::new()
                            .title(name.clone())
                            .status(status),
                    )
                    .meta(replay_prompt_meta(prompt_id));
                    Some(
                        acp::SessionNotification::new(
                            session.id.clone(),
                            acp::SessionUpdate::ToolCallUpdate(update),
                        )
                        .meta(replay_prompt_meta(prompt_id)),
                    )
                }
                SessionRecord::AssistantSnapshot { .. }
                | SessionRecord::AssistantToolCalls { .. }
                | SessionRecord::Tool { .. }
                | SessionRecord::TurnTerminal { .. } => None,
            };
            if let Some(notification) = notification {
                // completion receiver 与 live update 共用 AgentSideConnection writer；load
                // response 只有在该历史帧完成后才能返回。
                let completion = gateway.forward_with_completion(notification);
                let delivered = tokio::time::timeout(Duration::from_secs(5), completion).await;
                if !matches!(delivered, Ok(Ok(Ok(())))) {
                    tracing::debug!(
                        event = "session_replay_delivery_failed",
                        "session/load replay update 未完成写入"
                    );
                    return Err(acp::Error::internal_error());
                }
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl acp::Agent for MinimalAgent {
    /// 返回 ACP v1 与最小 runtime metadata，不广告 fs、terminal 或 MCP transport。
    async fn initialize(
        &self,
        _args: acp::InitializeRequest,
    ) -> acp::Result<acp::InitializeResponse> {
        observability::initialize_received();
        let capabilities = acp::AgentCapabilities::new()
            .load_session(true)
            .session_capabilities(
                acp::SessionCapabilities::new().list(acp::SessionListCapabilities::new()),
            );
        Ok(acp::InitializeResponse::new(acp::ProtocolVersion::V1)
            .agent_capabilities(capabilities)
            .meta(Self::handshake_meta()))
    }

    /// 当前 profile 不广告认证方法；未广告 methodId 必须固定返回 method_not_found。
    async fn authenticate(
        &self,
        _args: acp::AuthenticateRequest,
    ) -> acp::Result<acp::AuthenticateResponse> {
        tracing::debug!(
            event = "authenticate_rejected",
            reason = "method_not_advertised",
            "拒绝未广告的 ACP authenticate 方法"
        );
        Err(acp::Error::method_not_found())
    }

    /// 创建 v1 repository session；兼容模式仍只创建进程内 session。
    async fn new_session(
        &self,
        args: acp::NewSessionRequest,
    ) -> acp::Result<acp::NewSessionResponse> {
        self.validate_session_scope(&args.cwd, &args.additional_directories, &args.mcp_servers)?;
        Self::validate_session_meta(args.meta.as_ref())?;
        let session_id = if let Some(dependencies) = self.runtime_dependencies() {
            let session = dependencies
                .repository
                .create()
                .await
                .map_err(|_| acp::Error::internal_error())?;
            self.remember_session(&session.id);
            session.id
        } else {
            self.create_memory_session(args.cwd)?
        };
        observability::session_created();
        Ok(acp::NewSessionResponse::new(session_id))
    }

    /// 加载 v1/legacy session，并按真实 ACP session/update schema 回放安全快照。
    async fn load_session(
        &self,
        args: acp::LoadSessionRequest,
    ) -> acp::Result<acp::LoadSessionResponse> {
        self.validate_session_scope(&args.cwd, &args.additional_directories, &args.mcp_servers)?;
        Self::validate_session_meta(args.meta.as_ref())?;
        if let Some(dependencies) = self.runtime_dependencies() {
            let session = dependencies
                .repository
                .load_with_tool_policy(
                    args.session_id.0.as_ref(),
                    &dependencies.expected_tools,
                    &dependencies.ready_tools,
                )
                .await
                .map_err(|error| match error {
                    SessionError::NotFound => rejected_params("session_not_found"),
                    _ => acp::Error::internal_error(),
                })?;
            if let Some(gateway) = dependencies.gateway.as_ref() {
                self.replay_session(&session, gateway).await?;
            }
            self.remember_session(&session.id);
            observability::session_loaded(true);
        } else {
            let found = self.has_memory_session(&args.session_id);
            observability::session_loaded(found);
            if !found {
                return Err(rejected_params("session_not_found"));
            }
        }
        Ok(acp::LoadSessionResponse::new())
    }

    /// 列出固定 cwd 的 v1/legacy session，拒绝当前 profile 未启用的字段。
    async fn list_sessions(
        &self,
        args: acp::ListSessionsRequest,
    ) -> acp::Result<acp::ListSessionsResponse> {
        let Some(requested_cwd) = args.cwd.as_ref() else {
            return Err(rejected_params("session_list_cwd_missing"));
        };
        if !args.additional_directories.is_empty() {
            return Err(rejected_params(
                "session_list_additional_directories_not_allowed",
            ));
        }
        if args.meta.is_some() {
            return Err(rejected_params("session_list_meta_not_allowed"));
        }

        let (sessions, count) = if let Some(dependencies) = self.runtime_dependencies() {
            if requested_cwd != &self.state.borrow().expected_cwd {
                return Err(rejected_params("session_list_cwd_mismatch"));
            }
            let summaries = dependencies
                .repository
                .list()
                .await
                .map_err(|_| acp::Error::internal_error())?;
            let sessions = summaries
                .into_iter()
                .map(|summary| acp::SessionInfo::new(summary.id, requested_cwd.clone()))
                .collect::<Vec<_>>();
            let count = sessions.len();
            (sessions, count)
        } else {
            let sessions = {
                let state = self.state.borrow();
                if requested_cwd != &state.expected_cwd {
                    return Err(rejected_params("session_list_cwd_mismatch"));
                }
                state
                    .sessions
                    .iter()
                    .map(|(session_id, session)| {
                        acp::SessionInfo::new(session_id.clone(), session.cwd.clone())
                    })
                    .collect::<Vec<_>>()
            };
            let count = sessions.len();
            (sessions, count)
        };
        observability::sessions_listed(count);
        Ok(acp::ListSessionsResponse::new(sessions))
    }

    /// 启动一个 session turn；同一 session 只允许一个 active prompt，完成后 exactly-once 清理。
    async fn prompt(&self, args: acp::PromptRequest) -> acp::Result<acp::PromptResponse> {
        let session_id = args.session_id.0.as_ref().to_owned();
        tracing::debug!(event = "prompt_received", "收到 ACP prompt 请求");
        if args.message_id.is_some() {
            return Err(rejected_params("prompt_message_id_not_allowed"));
        }
        let prompt_id = Self::prompt_id(args.meta.as_ref())?;
        Self::validate_prompt_blocks(&args.prompt)?;
        let user_text = Self::prompt_text(&args.prompt);
        let dependencies = self.runtime_dependencies();
        #[cfg(debug_assertions)]
        if let Some(test_seam) = self.test_seam() {
            // 测试专用 admission 前屏障用于证明 EOF/cancel 的到达窗口；release 不编译。
            test_seam.wait_if_enabled("before_prompt_admission").await;
        }
        // 先建立 pending admission，再执行可能 yield 的 session 存在性检查；这样排在
        // prompt 后面的 cancel 能绑定本次 epoch，而未知 session 仍会在确认失败时清理。
        let epoch = self.begin_prompt_admission(&session_id, &prompt_id)?;
        #[cfg(debug_assertions)]
        if let Some(test_seam) = self.test_seam() {
            // admission 已创建但 reserve 尚未执行，供 pre-reserve cancel 做确定性验证。
            test_seam.wait_if_enabled("after_prompt_admission").await;
        }
        if !self
            .confirm_prompt_session(&session_id, epoch, dependencies.as_ref())
            .await
        {
            self.reject_pending_admission(&session_id, &prompt_id, epoch);
            return Err(rejected_params("session_not_found"));
        }
        let control = TurnControl::new();
        self.reserve_prompt(&session_id, &prompt_id, epoch, control.clone())?;

        let result = if let Some(dependencies) = dependencies {
            match dependencies.gateway {
                Some(gateway) => {
                    let loop_runner = TurnLoop::new(
                        dependencies.repository,
                        dependencies.model,
                        dependencies.mcp,
                        gateway,
                        dependencies.expected_tools,
                        dependencies.ready_tools,
                    );
                    #[cfg(debug_assertions)]
                    let loop_runner = loop_runner.with_test_seam(self.test_seam());
                    loop_runner
                        .run_prompt(&session_id, &prompt_id, &user_text, control)
                        .await
                }
                None => Err(TurnLoopError::Transport),
            }
        } else {
            // 兼容构造器不持有模型依赖；生产 run_acp 永远使用 with_runtime。
            observability::prompt_completed();
            Ok(acp::PromptResponse::new(acp::StopReason::EndTurn))
        };

        self.release_prompt(&session_id, &prompt_id);
        match result {
            Ok(response) => {
                observability::prompt_completed();
                Ok(response)
            }
            Err(error) => {
                tracing::debug!(event = "prompt_failed", error = %error, "prompt 以稳定错误结束");
                Err(turn_error_to_acp(error))
            }
        }
    }

    /// 接受已知或迟到的取消通知；取消 token 幂等且不会触发新的模型调用。
    async fn cancel(&self, args: acp::CancelNotification) -> acp::Result<()> {
        tracing::debug!(event = "cancel_received", "收到 ACP cancel 通知");
        if args.session_id.0.is_empty() {
            return Err(rejected_params("cancel_session_id_missing"));
        }
        if args.meta.is_some() {
            return Err(rejected_params("cancel_meta_not_allowed"));
        }
        let session_id = args.session_id.0.as_ref();
        #[cfg(debug_assertions)]
        if let Some(test_seam) = self.test_seam() {
            test_seam.mark("cancel_received");
        }
        let (mut known_session, control, pending) = {
            let state = self.state.borrow();
            let known = state.known_sessions.contains(session_id);
            if let Some(active) = state.active_prompts.get(session_id) {
                (known, Some(active.control.clone()), None)
            } else if let Some(pending) = state.pending_admissions.get(session_id) {
                // 未确认的冷启动 admission 仍有 epoch，但必须在 await 外确认 session 真实存在。
                (known, None, Some((pending.epoch, pending.confirmed)))
            } else {
                // 未知/idle session 不创建 latch，避免任意 session id 填满内存。
                (known, None, None)
            }
        };
        if let Some(control) = control {
            control.request_cancel();
            #[cfg(debug_assertions)]
            if let Some(test_seam) = self.test_seam() {
                test_seam.mark("cancel_bound");
            }
            tracing::debug!(
                event = "prompt_cancel_requested",
                "已通知 active prompt 取消"
            );
        } else if let Some((epoch, confirmed)) = pending {
            let confirmed = if confirmed || known_session {
                true
            } else {
                // 取消请求本身不为未知 session 建立状态；只有和 pending epoch 同时存在且
                // repository/memory 检查成功时，才允许进入 latch。
                #[cfg(debug_assertions)]
                if let Some(test_seam) = self.test_seam() {
                    test_seam.mark("cancel_confirmation_started");
                }
                let dependencies = self.runtime_dependencies();
                let exists = self
                    .session_exists(&acp::SessionId::new(session_id), dependencies.as_ref())
                    .await;
                if exists {
                    self.remember_session(session_id);
                    known_session = true;
                }
                exists
            };
            if confirmed {
                self.mark_admission_confirmed(session_id, epoch);
                if self.cancel_admission_epoch(session_id, epoch) {
                    #[cfg(debug_assertions)]
                    if let Some(test_seam) = self.test_seam() {
                        test_seam.mark("cancel_bound");
                    }
                    tracing::debug!(
                        event = "prompt_cancel_bound",
                        "已将 cancel 绑定到同一 admission epoch"
                    );
                }
            }
        }
        observability::cancel_received(known_session);
        #[cfg(debug_assertions)]
        if let Some(test_seam) = self.test_seam() {
            test_seam.mark("cancel_handler_completed");
        }
        Ok(())
    }

    /// 只服务真实 wire `_x.ai/mcp/list` 对应的逻辑扩展名。
    async fn ext_method(&self, args: acp::ExtRequest) -> acp::Result<acp::ExtResponse> {
        if args.method.as_ref() != MCP_LIST_METHOD {
            observability::extension_rejected();
            return Err(acp::Error::method_not_found());
        }
        let params: McpListParams = serde_json::from_str(args.params.get())
            .map_err(|_| rejected_params("mcp_list_params_invalid"))?;
        let dependencies = self.runtime_dependencies();
        if !self
            .session_exists(&params.session_id, dependencies.as_ref())
            .await
        {
            return Err(rejected_params("session_not_found"));
        }

        let response = if let Some(dependencies) = dependencies {
            dependencies
                .mcp
                .catalog()
                .await
                .map_err(|_| acp::Error::internal_error())?
                .to_wire()
        } else {
            // 兼容构造器没有 MCP 依赖，仍返回 Host 期待的嵌套空 catalog。
            serde_json::json!({ "result": { "servers": [] } })
        };
        let raw =
            serde_json::value::to_raw_value(&response).map_err(|_| acp::Error::internal_error())?;
        observability::extension_served();
        Ok(acp::ExtResponse::new(raw.into()))
    }
}

/// 将 turn loop 稳定错误映射到不含正文的 ACP error。
fn turn_error_to_acp(error: TurnLoopError) -> acp::Error {
    match error {
        TurnLoopError::SessionNotFound => rejected_params("session_not_found"),
        TurnLoopError::ReadOnly => rejected_params("session_read_only"),
        TurnLoopError::Session
        | TurnLoopError::Model
        | TurnLoopError::Transport
        | TurnLoopError::ToolRejected
        | TurnLoopError::Permission => acp::Error::internal_error(),
    }
}

/// 统一构造 replay session/update 的顶层 `_meta`，避免历史帧漏掉 isReplay。
fn replay_prompt_meta(prompt_id: &str) -> acp::Meta {
    let mut meta = acp::Meta::new();
    meta.insert("promptId".to_owned(), Value::String(prompt_id.to_owned()));
    meta.insert("isReplay".to_owned(), Value::Bool(true));
    meta
}

/// 将 journal 的安全状态映射到 ACP 工具状态；未知状态 fail-safe 为 failed。
fn replay_tool_status(status: &str) -> acp::ToolCallStatus {
    match status {
        "pending" => acp::ToolCallStatus::Pending,
        "in_progress" => acp::ToolCallStatus::InProgress,
        "completed" => acp::ToolCallStatus::Completed,
        _ => acp::ToolCallStatus::Failed,
    }
}

#[cfg(test)]
mod tests {
    use super::{MinimalAgent, RuntimeState};
    use crate::model_client::HttpModelClient;
    use crate::session_store::SessionRepository;
    use agent_client_protocol as acp;
    use std::cell::RefCell;
    use std::collections::BTreeSet;
    use std::path::PathBuf;
    use std::rc::Rc;

    /// 未知 session 的 cancel 不得因为 repository 已安装而分配 latch。
    #[tokio::test]
    async fn unknown_session_cancel_does_not_allocate_latch() {
        let temporary = tempfile::tempdir().expect("测试临时目录必须可创建");
        let repository = SessionRepository::new(temporary.path().join("home"));
        let agent = MinimalAgent::with_runtime(
            PathBuf::from("/tmp/session-cwd"),
            repository,
            HttpModelClient::for_test("http://127.0.0.1:43123/v1", "test-binding"),
            BTreeSet::new(),
        );
        let cancel = acp::CancelNotification::new(acp::SessionId::new("unknown-session"));
        <MinimalAgent as acp::Agent>::cancel(&agent, cancel)
            .await
            .expect("未知 session cancel 应保持幂等");
        assert!(agent.state.borrow().cancel_latches.is_empty());
    }

    /// idle session 的 cancel 不得影响后续尚未 admission 的 prompt。
    #[tokio::test]
    async fn idle_session_cancel_does_not_allocate_latch() {
        let agent = MinimalAgent::new(PathBuf::from("/tmp/session-cwd"));
        let session_id = agent
            .create_memory_session(PathBuf::from("/tmp/session-cwd"))
            .expect("测试 memory session 必须创建成功");
        let cancel = acp::CancelNotification::new(acp::SessionId::new(session_id));
        <MinimalAgent as acp::Agent>::cancel(&agent, cancel)
            .await
            .expect("idle session cancel 应保持幂等");
        assert!(agent.state.borrow().cancel_latches.is_empty());
    }

    /// admission 确认失败时必须清理同 epoch 的 latch，避免悬挂状态影响后续 turn。
    #[tokio::test]
    async fn rejected_pending_admission_clears_matching_latch() {
        let agent = MinimalAgent::new(PathBuf::from("/tmp/session-cwd"));
        let session_id = agent
            .create_memory_session(PathBuf::from("/tmp/session-cwd"))
            .expect("测试 memory session 必须创建成功");
        let epoch = agent
            .begin_prompt_admission(&session_id, "prompt-rejected")
            .expect("测试 pending admission 必须创建成功");
        <MinimalAgent as acp::Agent>::cancel(
            &agent,
            acp::CancelNotification::new(acp::SessionId::new(session_id.clone())),
        )
        .await
        .expect("pending admission cancel 应保持幂等");
        assert_eq!(agent.state.borrow().cancel_latches.len(), 1);

        agent.reject_pending_admission(&session_id, "prompt-rejected", epoch);

        assert!(agent.state.borrow().cancel_latches.is_empty());
        assert!(agent.state.borrow().pending_admissions.is_empty());
    }

    /// shutdown 清理必须移除 pending admission 的 cancel latch，避免保留无效状态。
    #[tokio::test]
    async fn shutdown_clears_pending_cancel_latches() {
        let agent = MinimalAgent::new(PathBuf::from("/tmp/session-cwd"));
        let session_id = agent
            .create_memory_session(PathBuf::from("/tmp/session-cwd"))
            .expect("测试 memory session 必须创建成功");
        let _epoch = agent
            .begin_prompt_admission(&session_id, "prompt-pending")
            .expect("测试 pending admission 必须创建成功");
        <MinimalAgent as acp::Agent>::cancel(
            &agent,
            acp::CancelNotification::new(acp::SessionId::new(session_id.clone())),
        )
        .await
        .expect("pending admission cancel 应保持幂等");
        assert_eq!(agent.state.borrow().cancel_latches.len(), 1);

        agent.begin_shutdown();

        assert!(agent.state.borrow().cancel_latches.is_empty());
    }

    /// shutdown 后迟到的异步确认不得重新创建 cancel latch。
    #[tokio::test]
    async fn shutdown_rejects_late_pending_cancel_latch() {
        let agent = MinimalAgent::new(PathBuf::from("/tmp/session-cwd"));
        let session_id = agent
            .create_memory_session(PathBuf::from("/tmp/session-cwd"))
            .expect("测试 memory session 必须创建成功");
        let epoch = agent
            .begin_prompt_admission(&session_id, "prompt-shutdown")
            .expect("测试 pending admission 必须创建成功");

        agent.begin_shutdown();

        assert!(!agent.latch_pending_admission(&session_id, epoch));
        assert!(agent.state.borrow().cancel_latches.is_empty());
        assert!(
            agent
                .state
                .borrow()
                .pending_admissions
                .get(&session_id)
                .is_some_and(|pending| pending.cancelled)
        );
    }

    /// 保留类型引用，确保测试只检查 agent 自身状态而不引入跨线程同步假设。
    #[allow(dead_code)]
    fn _state_type_is_local(_: Rc<RefCell<RuntimeState>>) {}
}
