//! HostRuntime 的每 scope ACP IO actor 闭环。
//!
//! 产品只通过 [`HostRuntime::dispatch`] 进入此模块。每个 scope 的 actor 独占
//! sidecar stdin/stdout、ACP reader、投影器和会话状态；同步 dispatch 只等待命令
//! 规定的回执时机，绝不在产品线程直接读取 sidecar stdout。

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender, SyncSender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use efflab_agent_contract::{
    HOST_ACP_PROTOCOL_VERSION, HostPolicy, is_prompt_id, is_qualified_tool_name, is_server_name,
    validate_prompt_text,
};
use serde_json::{Value, json};

use crate::acp_runtime::{
    AcpRuntime, Inbound, RequestId, RequestWriteFailure, RpcError, ValidatedReply,
};
use crate::app_port::{ApprovedMcpSpec, HostApp, ScopeId};
use crate::event_sink::KitEventSink;
use crate::llm_channel::{LaunchedScope, LlmChannelError, LlmChannelService, SetLlmChannelRequest};
use crate::projector::Projector;
use crate::protocol::{
    Capability, CapabilityLimits, KIT_SCHEMA_VERSION, KitBlock, KitCommand, KitError,
    KitProductEvent, KitReply, LlmChannelKind, Origin, SessionSummary,
    is_recoverable_product_event,
};
use crate::submission::{SendTicket, SendTicketState, SubmissionDecision, SubmissionMap};
use crate::supervisor::{SupervisorCapability, UnavailableReason, capability};
use crate::{METHOD_NOT_FOUND, ValidatedKitEventSink};

/// initialize 结果迟迟未到时的协议超时；不能让 New/List 永久卡住。
const INITIALIZE_TIMEOUT: Duration = Duration::from_secs(20);
/// 同步 Kit 调用等待 actor 回执的总上限，覆盖初始化超时和后续 ACP 请求。
const DISPATCH_REPLY_TIMEOUT: Duration = Duration::from_secs(25);
/// MCP catalog 必须在产品调用超时之前降级；超时不杀 sidecar。
const MCP_CATALOG_TIMEOUT: Duration = Duration::from_secs(20);
/// shutdown 回执已到达后只给 actor 极短宽限；未结束时保留 tombstone，不能无界 join。
const ACTOR_JOIN_GRACE: Duration = Duration::from_millis(100);
/// session/load 的单 flight deadline；迟到 response 必须被撤销并丢弃。
const LOAD_TIMEOUT: Duration = Duration::from_secs(60);
/// ACP 明确表示 session 不存在的错误码；其它 load error 不能伪装成 NotFound。
const ACP_SESSION_NOT_FOUND: i64 = -32004;
/// JSON-RPC invalid params；sidecar close 未知 session 使用该码。
const JSON_RPC_INVALID_PARAMS: i64 = -32602;
/// actor 空闲轮询间隔；stdout reader 独立运行，因此该值不影响 ACP 收包顺序。
const ACTOR_TICK: Duration = Duration::from_millis(5);
/// terminal sink 失败后的重试间隔；不让失败 sink 造成 actor 忙循环。
const TERMINAL_RETRY_DELAY: Duration = Duration::from_millis(100);
/// 每轮最多处理的入站项目数；达到上限后让出机会给 Cancel/Shutdown 等控制命令。
const MAX_INBOUND_DRAIN: usize = 8;
/// ACP `session/new` / `session/load` 使用固定 Channel 槽名，不得泄漏供应商模型标识。
const ACP_BYOK_MODEL_SLOT: &str = "byok";
/// 最小 sidecar 握手声明的运行时实现标识；缺失或变形都必须拒绝。
const EFFLAB_RUNTIME_ID: &str = "minimal-v1";
/// 最小 sidecar 握手声明所使用的 Kit schema 版本。
const EFFLAB_SCHEMA_VERSION: u64 = 1;
/// 最小 sidecar 握手声明所使用的 session store 版本。
const EFFLAB_SESSION_STORE_VERSION: u64 = 1;
/// MCP catalog 中始终可安全自动许可的内置无副作用工具。
const NOOP_TOOL: &str = "GrokBuild:efflab_noop";
/// Kit capability 与实际写入 sidecar 的单次 prompt 统一字符上限。
const MAX_PROMPT_CHARS: usize = 32_000;
/// 用户可见的回合失败提示；不得出现 sidecar 等实现名词。
const TURN_FAILED_USER_MESSAGE: &str = "回复未完成，请重试";

/// 把 sidecar 稳定错误码转成用户可读提示，不暴露内部实现名词。
fn turn_failure_user_message(code: &str) -> &'static str {
    match code {
        "turn_model_error" => "模型没有返回有效回复，请重试",
        "turn_session_not_found" | "turn_session_read_only" => "当前会话无法继续，请新建对话",
        "turn_session_error" => "会话保存失败，请重试",
        "turn_transport_error" => "连接中断，请重试",
        "turn_tool_rejected" => "这次工具调用未被允许",
        "turn_permission_error" => "工具授权未完成，请重试",
        _ => TURN_FAILED_USER_MESSAGE,
    }
}

/// 产品唯一调用入口的进程内状态。
pub struct HostRuntime {
    /// 产品领域端口。仅 runtime 读取 MCP 批准集和构造 Channel 服务时使用。
    app: Arc<dyn HostApp>,
    /// 所有产品事件都已包入校验边界，actor 不可绕开该运输路径。
    sink: Arc<dyn KitEventSink>,
    /// 运行时固定配置；每个新 actor 从此派生 scope 私有路径与 idle 策略。
    cfg: crate::HostRuntimeConfig,
    /// MCP catalog deadline；生产入口固定使用 20 秒，测试入口才可注入较短值。
    mcp_catalog_timeout: Duration,
    /// session/load deadline；生产入口固定使用 60 秒，测试入口才可注入较短值。
    load_timeout: Duration,
    /// Channel 服务构造也可能因历史配置不安全而失败；构造 API 不能 panic。
    channel: Result<Arc<LlmChannelService>, LlmChannelError>,
    /// 进程内 Send 幂等边界，跨 actor restart 保持。
    submissions: Mutex<SubmissionMap>,
    /// 每个 scope 一个唯一 IO actor；失败杀停的 actor 保留以 fail-closed 而非自动复活。
    actors: Mutex<BTreeMap<String, Arc<ActorHandle>>>,
    /// actor 退出后的 terminal pending 仍由 runtime 持有，等待后续 cleanup 边界重试。
    terminal_outbox: Arc<Mutex<TerminalOutbox>>,
    /// 全局换通道与新 actor launch 的互斥门，防止旧 revision 与新 revision 交错。
    channel_transition: Mutex<()>,
    /// 已提交配置下未能恢复的原 live scope；相同 Set 请求必须重试这些 scope。
    restart_retry_scopes: Mutex<BTreeSet<String>>,
}

impl HostRuntime {
    /// 构造进程内单例运行时；实际 L3b 监听和 sidecar spawn 均延迟到对话命令。
    pub fn new(
        app: impl HostApp + 'static,
        sink: impl KitEventSink + 'static,
        cfg: crate::HostRuntimeConfig,
    ) -> Self {
        Self::new_with_timeouts(app, sink, cfg, MCP_CATALOG_TIMEOUT, LOAD_TIMEOUT)
    }

    /// 仅供集成测试注入较短 catalog deadline；生产调用必须使用 [`Self::new`] 保持 20 秒合同。
    #[doc(hidden)]
    pub fn new_for_test_with_mcp_catalog_timeout(
        app: impl HostApp + 'static,
        sink: impl KitEventSink + 'static,
        cfg: crate::HostRuntimeConfig,
        mcp_catalog_timeout: Duration,
    ) -> Self {
        Self::new_with_timeouts(app, sink, cfg, mcp_catalog_timeout, LOAD_TIMEOUT)
    }

    /// 仅供集成测试注入较短 load deadline；生产调用必须使用 [`Self::new`] 保持 60 秒合同。
    #[doc(hidden)]
    pub fn new_for_test_with_load_timeout(
        app: impl HostApp + 'static,
        sink: impl KitEventSink + 'static,
        cfg: crate::HostRuntimeConfig,
        load_timeout: Duration,
    ) -> Self {
        Self::new_with_timeouts(app, sink, cfg, MCP_CATALOG_TIMEOUT, load_timeout)
    }

    /// 统一构造路径，避免测试 deadline 改变生产 `new` 的冻结协议语义。
    fn new_with_timeouts(
        app: impl HostApp + 'static,
        sink: impl KitEventSink + 'static,
        cfg: crate::HostRuntimeConfig,
        mcp_catalog_timeout: Duration,
        load_timeout: Duration,
    ) -> Self {
        let app = Arc::new(app);
        let channel = LlmChannelService::new(Arc::clone(&app), cfg.clone()).map(Arc::new);
        let app: Arc<dyn HostApp> = app;
        let sink: Arc<dyn KitEventSink> = Arc::new(ValidatedKitEventSink::new(sink));

        Self {
            app,
            sink,
            cfg,
            mcp_catalog_timeout,
            load_timeout,
            channel,
            submissions: Mutex::new(SubmissionMap::default()),
            actors: Mutex::new(BTreeMap::new()),
            terminal_outbox: Arc::new(Mutex::new(TerminalOutbox::default())),
            channel_transition: Mutex::new(()),
            restart_retry_scopes: Mutex::new(BTreeSet::new()),
        }
    }

    /// 分派 Kit 命令，并严格遵守各命令的 ACP 回执时机。
    pub fn dispatch(&self, cmd: KitCommand) -> Result<KitReply, KitError> {
        match cmd {
            KitCommand::GetCapability => self.dispatch_get_capability(),
            KitCommand::Send {
                scope_id,
                session_id,
                submission_id,
                text,
                mentions,
            } => self.dispatch_send(
                scope_id,
                session_id,
                submission_id,
                text,
                mentions.unwrap_or_default(),
            ),
            KitCommand::Cancel {
                scope_id,
                session_id,
            } => {
                self.require_conversation_channel()?;
                let actor = self.actor_for_scope(&scope_id)?;
                request_actor(&actor, |reply| ActorCommand::Cancel { session_id, reply })
            }
            KitCommand::NewSession {
                scope_id,
                client_request_id: _,
            } => {
                self.require_conversation_channel()?;
                let actor = self.actor_for_scope(&scope_id)?;
                request_actor(&actor, |reply| ActorCommand::NewSession { reply })
            }
            KitCommand::ListSessions { scope_id, cursor } => {
                self.require_conversation_channel()?;
                let actor = self.actor_for_scope(&scope_id)?;
                request_actor(&actor, |reply| ActorCommand::ListSessions { cursor, reply })
            }
            KitCommand::ResumeSession {
                scope_id,
                session_id,
            } => {
                self.require_conversation_channel()?;
                let actor = self.actor_for_scope(&scope_id)?;
                request_actor(&actor, |reply| ActorCommand::ResumeSession {
                    session_id,
                    reply,
                })
            }
            KitCommand::DeleteSession {
                scope_id,
                session_id,
            } => {
                self.require_conversation_channel()?;
                let actor = self.actor_for_scope(&scope_id)?;
                request_actor(&actor, |reply| ActorCommand::DeleteSession {
                    session_id,
                    reply,
                })
            }
            KitCommand::GetLlmChannelView => Ok(KitReply::LlmChannelView {
                channel: self.channel_service()?.view().map_err(channel_error)?,
            }),
            KitCommand::SetLlmChannel {
                kind,
                base_url,
                model_id,
                relay_base_url,
                app_key,
                api_key,
                access_token,
                client_request_id,
            } => self.dispatch_set_channel(SetLlmChannelRequest {
                kind,
                base_url,
                model_id,
                relay_base_url,
                app_key,
                api_key,
                access_token,
                client_request_id,
            }),
            KitCommand::Unknown { .. } => Err(KitError::non_retryable(
                "unsupported",
                "当前 Host 不支持该 Kit 命令",
            )),
        }
    }

    /// GetCapability 只读本地 committed view；平台硬化不可用优先于无 Channel 返回。
    fn dispatch_get_capability(&self) -> Result<KitReply, KitError> {
        // 先读取平台能力：Windows 这类硬化不可用的目标不能被 no-key 语义掩盖。
        let supervisor_capability = capability();
        let channel = self.channel_service()?.view().map_err(channel_error)?;
        if channel.kind.is_none() {
            if matches!(
                supervisor_capability,
                SupervisorCapability::Unavailable { .. }
            ) {
                return Err(sidecar_unavailable("当前平台不支持受硬化的 sidecar"));
            }
            return Err(LlmChannelError::Unconfigured.as_kit_error());
        }

        let (sidecar, reason) = match supervisor_capability {
            SupervisorCapability::Available => ("available".to_string(), None),
            SupervisorCapability::Unavailable { reason } => (
                "unavailable".to_string(),
                Some(unavailable_reason_name(reason).to_string()),
            ),
        };
        let mut features = vec![
            "send".to_string(),
            "cancel".to_string(),
            "new_session".to_string(),
            "list_sessions".to_string(),
            "resume_session".to_string(),
            "delete_session".to_string(),
            "llm_channel".to_string(),
        ];
        if self.app.mentions().is_some() {
            features.push("mentions".to_string());
        }

        Ok(KitReply::Capability(Capability {
            sidecar,
            reason,
            kit_version: env!("CARGO_PKG_VERSION").to_string(),
            schema_version: KIT_SCHEMA_VERSION,
            features,
            channel,
            limits: CapabilityLimits {
                max_prompt_chars: MAX_PROMPT_CHARS as u32,
            },
        }))
    }

    /// 先确认对话 Channel，再解析并门禁 mention，最后以原始字段登记稳定幂等键。
    fn dispatch_send(
        &self,
        scope_id: String,
        session_id: String,
        submission_id: String,
        text: String,
        mentions: Vec<crate::MentionId>,
    ) -> Result<KitReply, KitError> {
        validate_send_input(&scope_id, &session_id, &submission_id, &text)?;
        // 未配置 Channel 是所有对话命令的优先失败契约，不能被 mention 校验覆盖。
        self.require_conversation_channel()?;
        let prompt_text = self.resolve_send_prompt_text(&scope_id, &text, &mentions)?;
        let decision = self
            .submissions
            .lock()
            .map_err(|_| KitError::non_retryable("sidecar_unavailable", "提交映射不可用"))?
            // 指纹只依赖提交 wire 的原始 text 与排序后的 mention id，不能依赖可变展示文本。
            .record(&scope_id, &session_id, &submission_id, &text, &mentions);

        match decision {
            SubmissionDecision::Duplicate { turn_id } => Ok(KitReply::Send {
                accepted: true,
                duplicate: true,
                session_id,
                turn_id,
                submission_id,
            }),
            SubmissionDecision::FingerprintConflict => Err(KitError::non_retryable(
                "fingerprint_conflict",
                "同一 submission_id 的提交内容不一致",
            )),
            SubmissionDecision::Accepted { ticket, .. } => {
                let actor = match self.actor_for_scope(&scope_id) {
                    Ok(actor) => actor,
                    Err(error) => {
                        self.forget_submission(&scope_id, &session_id, &submission_id, &ticket);
                        return Err(error);
                    }
                };
                match request_send_actor(
                    &actor,
                    session_id.clone(),
                    submission_id.clone(),
                    prompt_text,
                    ticket.clone(),
                ) {
                    Ok(reply) => Ok(reply),
                    Err(SendRequestError::BeforePrompt(error)) => {
                        // actor 已确认 prompt 未写入，按 ticket 身份撤销本次登记。
                        self.forget_submission(&scope_id, &session_id, &submission_id, &ticket);
                        Err(error)
                    }
                    Err(SendRequestError::PromptMayHaveBeenWritten(error)) => {
                        // timeout 或部分写入不确定时保留幂等登记，避免重试制造第二次 prompt。
                        Err(error)
                    }
                }
            }
        }
    }

    /// 将非空 mention 解析成展示文本，并在创建 actor 前实施最终安全门禁。
    fn resolve_send_prompt_text(
        &self,
        scope_id: &str,
        text: &str,
        mentions: &[crate::MentionId],
    ) -> Result<String, KitError> {
        if mentions.is_empty() {
            return Ok(text.to_string());
        }

        let resolver = self.app.mentions().ok_or_else(invalid_mentions_request)?;
        let resolved = resolver
            .resolve_mentions(&ScopeId(scope_id.to_string()), mentions)
            .map_err(|_| invalid_mentions_request())?;
        if resolved.len() != mentions.len()
            || resolved.iter().zip(mentions).any(|(resolved, requested)| {
                resolved.id != *requested || !is_safe_mention_expansion(&resolved.text)
            })
        {
            return Err(invalid_mentions_request());
        }

        // 保持产品返回的已审核顺序，避免 Host 擅自重排用户选择的曲库条目。
        let expanded = resolved
            .iter()
            .map(|mention| mention.text.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        let prompt_text = format!("{text}\n\n{expanded}");
        // 原始 text 已在上方校验；这里覆盖 resolver 拼入后的完整长度与文本语义。
        if !prompt_text_within_limit(&prompt_text) || validate_prompt_text(&prompt_text).is_err() {
            return Err(invalid_mentions_request());
        }

        Ok(prompt_text)
    }

    /// 实施全局 Channel 事务：先提交/失效，再 drain 与重建全部先前存活 scope。
    fn dispatch_set_channel(&self, request: SetLlmChannelRequest) -> Result<KitReply, KitError> {
        let _transition = self
            .channel_transition
            .lock()
            .map_err(|_| KitError::non_retryable("sidecar_unavailable", "通道事务不可用"))?;
        // 先尝试交付此前 actor 退出后保留的终态，再决定是否允许本次换代继续。
        self.retry_terminal_outbox()?;
        let service = self.channel_service()?;
        let change = service
            .commit_and_invalidate(request)
            .map_err(channel_error)?;
        if !change.changed {
            // 已提交的相同请求不是空操作：它是此前失败 restart 的稳定重试入口。
            self.retry_failed_restart_scopes()?;
            return Ok(KitReply::LlmChannelView {
                channel: change.view,
            });
        }

        // token 已失效后再读取真实 child 状态；只重建此次事务开始时仍存活的 scope，
        // 不能把已经 idle 或被 MCP gate 杀停的 actor 意外复活。
        let live_scopes = service
            .live_scope_ids()
            .map_err(channel_error)?
            .into_iter()
            .collect::<BTreeSet<_>>();
        let mut scopes = self
            .restart_retry_scopes
            .lock()
            .map_err(|_| KitError::non_retryable("sidecar_unavailable", "restart 重试状态不可用"))?
            .clone();
        // 只复制旧 actor；cleanup 失败时原句柄仍留在 map 作为 tombstone，禁止新代并存。
        let previous = {
            let actors = self.actors.lock().map_err(|_| {
                KitError::non_retryable("sidecar_unavailable", "scope actor 注册表不可用")
            })?;
            actors
                .iter()
                .map(|(scope_id, actor)| (scope_id.clone(), Arc::clone(actor)))
                .collect::<Vec<_>>()
        };
        for (scope_id, _) in &previous {
            if live_scopes.contains(scope_id) {
                scopes.insert(scope_id.clone());
            }
        }

        let mut restart_failed = BTreeSet::new();
        let mut cleanup_failed = false;
        for (scope_id, actor) in previous {
            let cleanup = actor.shutdown();
            if !cleanup.is_success() {
                cleanup_failed = true;
                // cleanup 未确认完成时保留 tombstone，禁止同 scope 立即拉起新代。
                if live_scopes.contains(&scope_id) {
                    restart_failed.insert(scope_id);
                }
                continue;
            }
            let mut actors = self.actors.lock().map_err(|_| {
                KitError::non_retryable("sidecar_unavailable", "scope actor 注册表不可用")
            })?;
            if actors
                .get(&scope_id)
                .is_some_and(|current| Arc::ptr_eq(current, &actor))
            {
                actors.remove(&scope_id);
            }
        }

        // 即使单个旧 actor drain/restart 失败，也继续尝试其余 scope；新 committed view 不回滚。
        for scope_id in scopes {
            if restart_failed.contains(&scope_id) {
                continue;
            }
            match self.spawn_actor(&scope_id) {
                Ok(actor) => match self.actors.lock() {
                    Ok(mut actors) => {
                        actors.insert(scope_id.clone(), actor);
                        restart_failed.remove(&scope_id);
                    }
                    Err(_) => {
                        restart_failed.insert(scope_id);
                    }
                },
                Err(_) => {
                    restart_failed.insert(scope_id);
                }
            }
        }

        let has_restart_failure = cleanup_failed || !restart_failed.is_empty();
        *self.restart_retry_scopes.lock().map_err(|_| {
            KitError::non_retryable("sidecar_unavailable", "restart 重试状态不可用")
        })? = restart_failed;
        if has_restart_failure {
            return Err(LlmChannelError::RestartFailed.as_kit_error());
        }
        Ok(KitReply::LlmChannelView {
            channel: change.view,
        })
    }

    /// 重试上一次已提交但未恢复成功的 scope；不扫描或复活任何其它 idle/dead scope。
    fn retry_failed_restart_scopes(&self) -> Result<(), KitError> {
        let scopes = self
            .restart_retry_scopes
            .lock()
            .map_err(|_| KitError::non_retryable("sidecar_unavailable", "restart 重试状态不可用"))?
            .clone();
        if scopes.is_empty() {
            return Ok(());
        }

        let mut remaining = BTreeSet::new();
        for scope_id in scopes {
            let (already_recovered, previous) = {
                let actors = self.actors.lock().map_err(|_| {
                    KitError::non_retryable("sidecar_unavailable", "scope actor 注册表不可用")
                })?;
                match actors.get(&scope_id) {
                    Some(actor) if actor.accepting.load(Ordering::Acquire) => (true, None),
                    Some(actor) => (false, Some(Arc::clone(actor))),
                    None => (false, None),
                }
            };
            if already_recovered {
                continue;
            }
            if let Some(actor) = previous {
                let cleanup = actor.shutdown();
                if !cleanup.is_success() {
                    // cleanup 未完成时保留 tombstone，禁止同 scope 立即拉起新代。
                    remaining.insert(scope_id);
                    continue;
                }
                let mut actors = self.actors.lock().map_err(|_| {
                    KitError::non_retryable("sidecar_unavailable", "scope actor 注册表不可用")
                })?;
                if actors
                    .get(&scope_id)
                    .is_some_and(|current| Arc::ptr_eq(current, &actor))
                {
                    actors.remove(&scope_id);
                }
            }

            match self.spawn_actor(&scope_id) {
                Ok(actor) => match self.actors.lock() {
                    Ok(mut actors) => {
                        actors.insert(scope_id.clone(), actor);
                    }
                    Err(_) => {
                        remaining.insert(scope_id);
                    }
                },
                Err(_) => {
                    remaining.insert(scope_id);
                }
            }
        }

        let has_restart_failure = !remaining.is_empty();
        *self.restart_retry_scopes.lock().map_err(|_| {
            KitError::non_retryable("sidecar_unavailable", "restart 重试状态不可用")
        })? = remaining;
        if has_restart_failure {
            Err(LlmChannelError::RestartFailed.as_kit_error())
        } else {
            Ok(())
        }
    }

    /// 取得或新建一个 scope actor；本入口与 SetLlmChannel 共享全局换代门。
    fn actor_for_scope(&self, scope_id: &str) -> Result<Arc<ActorHandle>, KitError> {
        let _transition = self
            .channel_transition
            .lock()
            .map_err(|_| KitError::non_retryable("sidecar_unavailable", "通道事务不可用"))?;
        // 新命令触发旧 actor cleanup 时，先重试跨线程保留的 terminal event。
        self.retry_terminal_outbox()?;
        let previous = {
            let actors = self.actors.lock().map_err(|_| {
                KitError::non_retryable("sidecar_unavailable", "scope actor 注册表不可用")
            })?;
            if let Some(actor) = actors.get(scope_id) {
                if actor.restart_blocked.load(Ordering::Acquire) {
                    tracing::debug!(
                        scope = %scope_id,
                        "scope 因 MCP 安全违例保持 tombstone，拒绝自动复活"
                    );
                    return Err(sidecar_unavailable("scope 因 MCP 安全违例不可用"));
                }
                if actor.accepting.load(Ordering::Acquire) {
                    return Ok(Arc::clone(actor));
                }
                // 非 accepting actor 先保留在 map；cleanup 失败时必须作为 tombstone 阻止新代。
                Some(Arc::clone(actor))
            } else {
                None
            }
        };
        if let Some(actor) = previous {
            let cleanup = actor.shutdown();
            if !cleanup.is_success() {
                return Err(sidecar_unavailable("旧 scope cleanup 未完整完成"));
            }
            let mut actors = self.actors.lock().map_err(|_| {
                KitError::non_retryable("sidecar_unavailable", "scope actor 注册表不可用")
            })?;
            if actors
                .get(scope_id)
                .is_some_and(|current| Arc::ptr_eq(current, &actor))
            {
                actors.remove(scope_id);
            }
        }

        let actor = self.spawn_actor(scope_id)?;
        self.actors
            .lock()
            .map_err(|_| {
                KitError::non_retryable("sidecar_unavailable", "scope actor 注册表不可用")
            })?
            .insert(scope_id.to_string(), Arc::clone(&actor));
        Ok(actor)
    }

    /// 构造并启动 actor；真实 sidecar spawn 顺序只能经 LlmChannelService 进入。
    fn spawn_actor(&self, scope_id: &str) -> Result<Arc<ActorHandle>, KitError> {
        let service = self.channel_service()?;
        let approved_mcp = self
            .app
            .mcp_for_scope(&ScopeId(scope_id.to_string()))
            .map_err(|_| KitError::non_retryable("sidecar_unavailable", "MCP 批准规格不可用"))?;
        tracing::debug!(scope = %scope_id, "正在启动 scope ACP IO actor");
        let launched = service
            .launch_scope_with_stdio(scope_id, &approved_mcp)
            .map_err(channel_error)?;
        tracing::debug!(
            scope = %scope_id,
            generation = launched.info.generation,
            sidecar_pid = launched.info.pid,
            "scope sidecar 已移交给 ACP IO actor"
        );
        let policy = match host_policy(&launched) {
            Ok(policy) => policy,
            Err(error) => {
                // policy 失败时 stdio 即将关闭；同时让 Supervisor 立即回收已注册的 child/token。
                if service.stop_scope(scope_id).is_err() {
                    tracing::error!(
                        scope = %scope_id,
                        cleanup_failure = ?CleanupFailureKind::ScopeStop,
                        "sidecar policy 失败后的 scope cleanup 未完成"
                    );
                }
                return Err(error);
            }
        };
        let generation = launched.info.generation;
        let acp = AcpRuntime::new(launched.stdio.stdin, launched.stdio.stdout);
        let (sender, receiver) = mpsc::channel();
        let accepting = Arc::new(AtomicBool::new(true));
        let exit_intent = Arc::new(AtomicBool::new(false));
        let submission_lock = Arc::new(Mutex::new(()));
        let queued_commands = Arc::new(AtomicUsize::new(0));
        let restart_blocked = Arc::new(AtomicBool::new(false));
        let cleanup_result = Arc::new(Mutex::new(CleanupResult::default()));
        let finished = Arc::new(AtomicBool::new(false));
        let mut actor = ScopeActor::new(
            scope_id.to_string(),
            acp,
            policy,
            Arc::clone(&service),
            Arc::clone(&self.sink),
            receiver,
            Arc::clone(&accepting),
            Arc::clone(&exit_intent),
            Arc::clone(&submission_lock),
            Arc::clone(&queued_commands),
            Arc::clone(&restart_blocked),
            Arc::clone(&cleanup_result),
            Arc::clone(&self.terminal_outbox),
            generation,
            self.cfg.idle_after,
            self.mcp_catalog_timeout,
            self.load_timeout,
            approved_mcp,
        );
        let name = format!("efflab-acp-{}", scope_id);
        let actor_finished = Arc::clone(&finished);
        let join = match thread::Builder::new().name(name).spawn(move || {
            actor.run();
            actor_finished.store(true, Ordering::Release);
        }) {
            Ok(join) => join,
            Err(_) => {
                // closure 被释放时 AcpRuntime 会关闭 stdin；再显式回收 child，不能遗留 token。
                if service.stop_scope(scope_id).is_err() {
                    tracing::error!(
                        scope = %scope_id,
                        cleanup_failure = ?CleanupFailureKind::ScopeStop,
                        "sidecar actor thread 启动失败后的 scope cleanup 未完成"
                    );
                }
                return Err(KitError::non_retryable(
                    "sidecar_unavailable",
                    "无法启动 sidecar IO actor",
                ));
            }
        };
        Ok(Arc::new(ActorHandle {
            scope_id: scope_id.to_string(),
            sender,
            accepting,
            exit_intent,
            submission_lock,
            queued_commands,
            restart_blocked,
            shutdown_submitted: AtomicBool::new(false),
            shutdown_attempt: Mutex::new(None),
            cleanup_result,
            join: Mutex::new(Some(join)),
            finished,
        }))
    }

    /// 读取构造时的 Channel 服务；历史配置错误统一不暴露底层数据。
    fn channel_service(&self) -> Result<Arc<LlmChannelService>, KitError> {
        self.channel
            .as_ref()
            .map(Arc::clone)
            .map_err(|error| error.as_kit_error())
    }

    /// 对话命令的统一 NOKEY 门：在任何路径、监听或 spawn 之前 fail-closed。
    fn require_conversation_channel(&self) -> Result<(), KitError> {
        let view = self.channel_service()?.view().map_err(channel_error)?;
        match view.kind {
            Some(LlmChannelKind::Byok) => Ok(()),
            Some(LlmChannelKind::Relay) | None => Err(LlmChannelError::Unconfigured.as_kit_error()),
        }
    }

    /// 按 ticket 身份回滚未写入 sidecar 的首次 Send 登记，避免迟到错误删除新一代记录。
    fn forget_submission(
        &self,
        scope_id: &str,
        session_id: &str,
        submission_id: &str,
        ticket: &SendTicket,
    ) {
        if let Ok(mut submissions) = self.submissions.lock() {
            submissions.forget(scope_id, session_id, submission_id, ticket);
        }
    }

    /// 获取 terminal outbox 锁；poison 只表示持锁线程曾 panic，Map 本身仍可恢复。
    fn lock_terminal_outbox(&self) -> std::sync::MutexGuard<'_, TerminalOutbox> {
        match self.terminal_outbox.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                tracing::error!(
                    cleanup_failure = ?CleanupFailureKind::TerminalEvent,
                    "Host terminal outbox 锁曾异常中断，恢复内存 outbox 状态"
                );
                let guard = poisoned.into_inner();
                self.terminal_outbox.clear_poison();
                guard
            }
        }
    }

    /// 在 scope cleanup/restart 边界重试 actor 退出后遗留的 terminal outbox。
    fn retry_terminal_outbox(&self) -> Result<(), KitError> {
        let delivered_scopes = {
            let mut outbox = self.lock_terminal_outbox();
            outbox.retry_now(self.sink.as_ref())
        };
        if delivered_scopes.is_empty() {
            return Ok(());
        }

        let pending_scopes = self.lock_terminal_outbox().pending_scopes();
        let actors = self.actors.lock().map_err(|_| {
            KitError::non_retryable("sidecar_unavailable", "scope actor 注册表不可用")
        })?;
        for scope_id in delivered_scopes {
            if pending_scopes.contains(&scope_id) {
                continue;
            }
            if let Some(actor) = actors.get(&scope_id) {
                clear_cleanup_failure(&actor.cleanup_result, CleanupFailureKind::TerminalEvent);
                if !actor.clear_shutdown_failure(CleanupFailureKind::TerminalEvent) {
                    return Err(sidecar_unavailable("无法同步 terminal cleanup 结果"));
                }
            }
        }
        Ok(())
    }
}

impl Drop for HostRuntime {
    /// Runtime 生命周期结束时先关 actor stdin，再由 Supervisor 回收对应 child。
    fn drop(&mut self) {
        let actors = self
            .actors
            .get_mut()
            .map(std::mem::take)
            .unwrap_or_default();
        for (scope_id, actor) in actors {
            let cleanup = actor.shutdown();
            if !cleanup.is_success() {
                tracing::error!(scope = %scope_id, "runtime drop 的 sidecar cleanup 未完整完成");
            }
        }
        if self.retry_terminal_outbox().is_err() {
            tracing::error!("runtime drop 的 terminal outbox cleanup 未完成");
        }
    }
}

/// 一个 actor 的外部命令句柄；只有 actor thread 自己能持有 AcpRuntime。
struct ActorHandle {
    scope_id: String,
    sender: Sender<ActorCommand>,
    /// false 仅表示正常 idle/shutdown 退出，Host 下次命令可安全 spawn 新代。
    accepting: Arc<AtomicBool>,
    /// actor 已决定退出；与 accepting 在同一提交门下发布，避免迟到 shutdown 入队。
    exit_intent: Arc<AtomicBool>,
    /// 将 accepting 检查与 command 入队绑定，避免 Shutdown 与迟到 command 乱序。
    submission_lock: Arc<Mutex<()>>,
    /// 与 ScopeActor 共享的已入队 command 计数，供 idle close 做完整判定。
    queued_commands: Arc<AtomicUsize>,
    /// MCP 安全违例后的永久 tombstone；普通命令不得自动创建新 generation。
    restart_blocked: Arc<AtomicBool>,
    /// 关闭命令只允许入队一次，重复 shutdown 复用同一 actor 生命周期。
    shutdown_submitted: AtomicBool,
    /// 当前 Shutdown attempt 的共享完成状态；调用方超时后仍可观察 actor 的最终结果。
    shutdown_attempt: Mutex<Option<Arc<ShutdownAttempt>>>,
    /// actor 内部资源 cleanup 结果与外部 join 结果共享给 restart 协调器。
    cleanup_result: Arc<Mutex<CleanupResult>>,
    join: Mutex<Option<JoinHandle<()>>>,
    /// actor thread 完成后置位，用于区分正常自然退出和 command 投递失败。
    finished: Arc<AtomicBool>,
}

impl ActorHandle {
    /// 在同一提交临界区检查 actor 状态并入队，拒绝已开始换代的 command。
    fn submit(&self, command: ActorCommand) -> Result<(), ActorCommand> {
        let _submission = match self.submission_lock.lock() {
            Ok(guard) => guard,
            Err(_) => return Err(command),
        };
        if !self.accepting.load(Ordering::Acquire) {
            return Err(command);
        }
        // 先登记再发送，确保 actor 即使立即取走 command 也不会看到计数下溢。
        self.queued_commands.fetch_add(1, Ordering::AcqRel);
        match self.sender.send(command) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.queued_commands.fetch_sub(1, Ordering::AcqRel);
                Err(error.0)
            }
        }
    }

    /// 清除 Host outbox 已确认运输的 shutdown 暂时失败。
    fn clear_shutdown_failure(&self, failure: CleanupFailureKind) -> bool {
        let slot = match self.shutdown_attempt.lock() {
            Ok(slot) => slot,
            Err(poisoned) => {
                tracing::error!(
                    scope = %self.scope_id,
                    cleanup_failure = ?CleanupFailureKind::ResultUnavailable,
                    "Shutdown attempt 状态锁中毒，无法同步 terminal 运输结果"
                );
                let slot = poisoned.into_inner();
                self.shutdown_attempt.clear_poison();
                record_cleanup_failure(
                    &self.cleanup_result,
                    &self.scope_id,
                    CleanupFailureKind::ResultUnavailable,
                );
                drop(slot);
                return false;
            }
        };
        if let Some(attempt) = slot.as_ref() {
            attempt.clear_failure(failure);
        }
        true
    }

    /// 协调 actor 的有序关闭；结构化返回所有未完成的 cleanup 步骤。
    fn shutdown(&self) -> CleanupResult {
        self.shutdown_with_timeout(DISPATCH_REPLY_TIMEOUT)
    }

    /// 在调用方 deadline 内等待共享 Shutdown attempt；超时不取消 actor 的 cleanup。
    fn shutdown_with_timeout(&self, timeout: Duration) -> CleanupResult {
        let mut result = CleanupResult::default();
        let attempt = match self.submission_lock.lock() {
            Ok(_submission) => {
                let mut slot = match self.shutdown_attempt.lock() {
                    Ok(slot) => slot,
                    Err(poisoned) => {
                        tracing::error!(
                            scope = %self.scope_id,
                            cleanup_failure = ?CleanupFailureKind::ResultUnavailable,
                            "Shutdown attempt 状态锁中毒，无法可靠协调 cleanup"
                        );
                        let mut result = CleanupResult::default();
                        result.record(CleanupFailureKind::ResultUnavailable);
                        record_cleanup_failure(
                            &self.cleanup_result,
                            &self.scope_id,
                            CleanupFailureKind::ResultUnavailable,
                        );
                        drop(poisoned.into_inner());
                        return result;
                    }
                };

                // actor 尚未提交退出且上一个 attempt 已暴露可重试失败时，才创建下一代
                // attempt，避免调用方超时期间重复投递 Shutdown。
                if slot
                    .as_ref()
                    .and_then(|attempt| attempt.snapshot())
                    .is_some_and(|cleanup| {
                        cleanup.has_actor_retry_failure()
                            && !self.finished.load(Ordering::Acquire)
                            && !self.exit_intent.load(Ordering::Acquire)
                    })
                {
                    *slot = None;
                    self.shutdown_submitted.store(false, Ordering::Release);
                }

                if let Some(attempt) = slot.as_ref() {
                    Arc::clone(attempt)
                } else {
                    let attempt = Arc::new(ShutdownAttempt::new());
                    *slot = Some(Arc::clone(&attempt));
                    self.shutdown_submitted.store(true, Ordering::Release);
                    self.accepting.store(false, Ordering::Release);
                    if self.finished.load(Ordering::Acquire)
                        || self.exit_intent.load(Ordering::Acquire)
                    {
                        if self.exit_intent.load(Ordering::Acquire) {
                            tracing::debug!(
                                scope = %self.scope_id,
                                cleanup_state = "exit_committed",
                                "sidecar actor 已提交退出，拒绝迟到 shutdown 入队"
                            );
                        }
                        attempt.complete(snapshot_cleanup_result(
                            &self.cleanup_result,
                            &self.scope_id,
                        ));
                    } else {
                        // Shutdown 也占用同一队列计数，避免 actor 取出时错误下溢。
                        self.queued_commands.fetch_add(1, Ordering::AcqRel);
                        match self.sender.send(ActorCommand::Shutdown {
                            attempt: Arc::clone(&attempt),
                        }) {
                            Ok(()) => {}
                            Err(_) => {
                                self.queued_commands.fetch_sub(1, Ordering::AcqRel);
                                let mut command_result = CleanupResult::default();
                                command_result.record(CleanupFailureKind::ShutdownCommand);
                                record_cleanup_failure(
                                    &self.cleanup_result,
                                    &self.scope_id,
                                    CleanupFailureKind::ShutdownCommand,
                                );
                                attempt.complete(command_result);
                            }
                        }
                    }
                    attempt
                }
            }
            Err(_) => {
                self.accepting.store(false, Ordering::Release);
                record_cleanup_failure(
                    &self.cleanup_result,
                    &self.scope_id,
                    CleanupFailureKind::SubmissionLock,
                );
                result.record(CleanupFailureKind::SubmissionLock);
                return result;
            }
        };

        let completed = attempt.wait_timeout(timeout);
        if let Some(actor_result) = completed.as_ref() {
            result.merge(actor_result);
        } else {
            // 仅让本次调用失败；不能写入共享 cleanup 结果，否则会把调用方超时
            // 误当成 actor cleanup 失败并粘住后续重试。
            result.record(CleanupFailureKind::ShutdownAcknowledgement);
            tracing::debug!(
                scope = %self.scope_id,
                cleanup_failure = ?CleanupFailureKind::ShutdownAcknowledgement,
                "等待 sidecar shutdown attempt 超时，保留共享 attempt 供后续观察"
            );
        }

        // 只在 actor 已结束时取走 JoinHandle；运行中的旧代必须留在 tombstone 中，
        // 由调用方下次 cleanup 再尝试回收，绝不能阻塞当前线程或允许新代并存。
        self.join_if_finished(ACTOR_JOIN_GRACE, &mut result);

        // actor 可能在本次等待超时后才完成资源 cleanup；完成后才允许下一次调用重试。
        if let Some(cleanup) = attempt.snapshot()
            && cleanup.has_resource_failure()
            && !self.finished.load(Ordering::Acquire)
            && !self.exit_intent.load(Ordering::Acquire)
            && let Ok(_submission) = self.submission_lock.lock()
            && let Ok(mut slot) = self.shutdown_attempt.lock()
            && slot
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &attempt))
        {
            *slot = None;
            self.shutdown_submitted.store(false, Ordering::Release);
        }

        let actor_result = snapshot_cleanup_result(&self.cleanup_result, &self.scope_id);
        result.merge(&actor_result);
        result
    }

    /// 在有限宽限内只回收已结束的 actor；未结束时保留 JoinHandle 供后续重试。
    fn join_if_finished(&self, grace: Duration, result: &mut CleanupResult) {
        let deadline = Instant::now() + grace;
        let mut join = match self.join.lock() {
            Ok(join) => join,
            Err(_) => {
                record_cleanup_failure(
                    &self.cleanup_result,
                    &self.scope_id,
                    CleanupFailureKind::ActorJoin,
                );
                result.record(CleanupFailureKind::ActorJoin);
                return;
            }
        };
        loop {
            let Some(handle) = join.as_ref() else {
                return;
            };
            if handle.is_finished() {
                let Some(handle) = join.take() else {
                    return;
                };
                drop(join);
                if handle.join().is_err() {
                    record_cleanup_failure(
                        &self.cleanup_result,
                        &self.scope_id,
                        CleanupFailureKind::ActorJoin,
                    );
                    result.record(CleanupFailureKind::ActorJoin);
                } else {
                    clear_cleanup_failure(&self.cleanup_result, CleanupFailureKind::ActorJoin);
                    result.clear(CleanupFailureKind::ActorJoin);
                }
                return;
            }
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                break;
            };
            if remaining.is_zero() {
                break;
            }
            thread::sleep(remaining.min(ACTOR_TICK));
        }
        record_cleanup_failure(
            &self.cleanup_result,
            &self.scope_id,
            CleanupFailureKind::ActorJoin,
        );
        result.record(CleanupFailureKind::ActorJoin);
    }
}

/// 一次 Shutdown 的共享完成状态；调用方超时不能取消 actor 的 cleanup 结果。
struct ShutdownAttempt {
    result: Mutex<Option<CleanupResult>>,
    completed: Condvar,
}

impl ShutdownAttempt {
    /// 创建尚未完成的 Shutdown attempt。
    fn new() -> Self {
        Self {
            result: Mutex::new(None),
            completed: Condvar::new(),
        }
    }

    /// 发布一次 cleanup 结果，并唤醒所有等待同一 attempt 的调用方。
    fn complete(&self, result: CleanupResult) {
        let mut state = match self.result.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                tracing::error!(
                    cleanup_failure = ?CleanupFailureKind::ResultUnavailable,
                    "Shutdown attempt 状态锁中毒，恢复后继续发布 cleanup 结果"
                );
                let state = poisoned.into_inner();
                self.result.clear_poison();
                state
            }
        };
        if state.is_none() {
            *state = Some(result);
            self.completed.notify_all();
        }
    }

    /// 下游已确认 terminal 运输后，清除 attempt 中对应的暂时失败。
    fn clear_failure(&self, failure: CleanupFailureKind) {
        let mut state = match self.result.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                tracing::error!(
                    cleanup_failure = ?CleanupFailureKind::ResultUnavailable,
                    "Shutdown attempt 状态锁中毒，恢复后清除已确认的 cleanup 失败"
                );
                let state = poisoned.into_inner();
                self.result.clear_poison();
                state
            }
        };
        if let Some(result) = state.as_mut() {
            result.clear(failure);
            self.completed.notify_all();
        }
    }

    /// 无阻塞读取 attempt 是否已经完成。
    fn snapshot(&self) -> Option<CleanupResult> {
        match self.result.lock() {
            Ok(state) => state.clone(),
            Err(poisoned) => {
                tracing::error!(
                    cleanup_failure = ?CleanupFailureKind::ResultUnavailable,
                    "Shutdown attempt 状态锁中毒，读取时恢复"
                );
                let state = poisoned.into_inner();
                self.result.clear_poison();
                state.clone()
            }
        }
    }

    /// 在调用方 deadline 内等待 attempt；超时只影响本次等待，不改变共享状态。
    fn wait_timeout(&self, timeout: Duration) -> Option<CleanupResult> {
        let state = match self.result.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                tracing::error!(
                    cleanup_failure = ?CleanupFailureKind::ResultUnavailable,
                    "Shutdown attempt 状态锁中毒，等待时恢复"
                );
                let state = poisoned.into_inner();
                self.result.clear_poison();
                state
            }
        };
        if state.is_some() {
            return state.clone();
        }
        let state = match self.completed.wait_timeout(state, timeout) {
            Ok((state, _)) => state,
            Err(poisoned) => {
                tracing::error!(
                    cleanup_failure = ?CleanupFailureKind::ResultUnavailable,
                    "Shutdown attempt 条件变量锁中毒，恢复后读取结果"
                );
                let (state, _) = poisoned.into_inner();
                self.result.clear_poison();
                state
            }
        };
        state.clone()
    }
}

/// actor 处理的内部命令；同步关闭使用可广播的 Shutdown attempt。
enum ActorCommand {
    NewSession {
        reply: ReplySender,
    },
    ListSessions {
        cursor: Option<String>,
        reply: ReplySender,
    },
    ResumeSession {
        session_id: String,
        reply: ReplySender,
    },
    DeleteSession {
        session_id: String,
        reply: ReplySender,
    },
    Send {
        session_id: String,
        submission_id: String,
        text: String,
        ticket: SendTicket,
        reply: ReplySender,
    },
    Cancel {
        session_id: String,
        reply: ReplySender,
    },
    Shutdown {
        attempt: Arc<ShutdownAttempt>,
    },
}

type ReplySender = SyncSender<Result<KitReply, KitError>>;

/// cleanup 失败的稳定内部分类；不保存底层错误文本，避免将路径或 payload 带出。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum CleanupFailureKind {
    SubmissionLock,
    ShutdownCommand,
    TerminalEvent,
    ShutdownAcknowledgement,
    AcpShutdown,
    CancelNotification,
    ScopeStop,
    ActorJoin,
    ResultUnavailable,
}

/// 一次 actor cleanup 的结构化结果；调用方据此决定是否允许继续 restart。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct CleanupResult {
    failures: BTreeSet<CleanupFailureKind>,
}

impl CleanupResult {
    /// 只要任一资源或握手步骤失败，就禁止把该 scope 报告为成功恢复。
    fn is_success(&self) -> bool {
        self.failures.is_empty()
    }

    /// 仅资源 cleanup 失败时允许 actor 保持存活，等待下一次显式重试。
    fn has_resource_failure(&self) -> bool {
        self.failures.contains(&CleanupFailureKind::AcpShutdown)
            || self.failures.contains(&CleanupFailureKind::ScopeStop)
    }

    /// actor 尚未提交退出时，terminal 运输失败也必须重新投递 shutdown attempt。
    fn has_actor_retry_failure(&self) -> bool {
        self.has_resource_failure() || self.failures.contains(&CleanupFailureKind::TerminalEvent)
    }

    /// 合并 actor 内部和外部 handle 观察到的 cleanup 失败分类。
    fn merge(&mut self, other: &Self) {
        self.failures.extend(other.failures.iter().copied());
    }

    /// 记录新失败；详细底层错误不进入结构化结果。
    fn record(&mut self, failure: CleanupFailureKind) {
        self.failures.insert(failure);
    }

    /// 清除已在后续 cleanup 中成功完成的临时失败。
    fn clear(&mut self, failure: CleanupFailureKind) {
        self.failures.remove(&failure);
    }
}

/// 将 cleanup 失败写入共享结果并记录脱敏分类日志。
fn clear_cleanup_failure(result: &Arc<Mutex<CleanupResult>>, failure: CleanupFailureKind) {
    if let Ok(mut result) = result.lock() {
        result.clear(failure);
    }
}

/// 将 cleanup 失败写入共享结果并记录脱敏分类日志。
fn record_cleanup_failure(
    result: &Arc<Mutex<CleanupResult>>,
    scope_id: &str,
    failure: CleanupFailureKind,
) {
    match result.lock() {
        Ok(mut result) => {
            if result.failures.insert(failure) {
                tracing::error!(
                    scope = %scope_id,
                    cleanup_failure = ?failure,
                    "sidecar cleanup 失败"
                );
            }
        }
        Err(_) => {
            tracing::error!(
                scope = %scope_id,
                cleanup_failure = ?CleanupFailureKind::ResultUnavailable,
                "sidecar cleanup 结果不可用"
            );
        }
    }
}

/// 读取共享 cleanup 结果；锁损坏时以失败关闭而不是假报成功。
fn snapshot_cleanup_result(result: &Arc<Mutex<CleanupResult>>, scope_id: &str) -> CleanupResult {
    match result.lock() {
        Ok(result) => result.clone(),
        Err(_) => {
            tracing::error!(
                scope = %scope_id,
                cleanup_failure = ?CleanupFailureKind::ResultUnavailable,
                "sidecar cleanup 结果不可用"
            );
            let mut result = CleanupResult::default();
            result.record(CleanupFailureKind::ResultUnavailable);
            result
        }
    }
}

/// actor 中等待 sidecar response 的请求分类。
enum PendingRpc {
    NewSession {
        reply: ReplySender,
    },
    ListSessions {
        reply: ReplySender,
    },
    DeleteSession {
        session_id: String,
        reply: ReplySender,
    },
    Load {
        session_id: String,
        replay_epoch: u64,
        generation: u64,
    },
    Prompt {
        session_id: String,
        submission_id: String,
    },
    McpCatalog {
        session_id: String,
    },
}

/// 一个 scope 的唯一 cold-load；所有状态都只能由该 scope actor 线程访问。
struct LoadFlight {
    session_id: String,
    owner_request_id: RequestId,
    replay_epoch: u64,
    /// Resume 当前立即回执，因此此列表只保留未来需要延迟结算的扩展位。
    waiters: Vec<ReplySender>,
    /// 标记至少一个 cold resume 已受理，失败时必须发 session-level 终止事件。
    accepted_resume: bool,
    pending_send: Option<PendingSend>,
    deadline: Instant,
    generation: u64,
    state: LoadFlightState,
}

/// cold-load 的有限状态；Terminal 只在结算期间存在，之后从 actor 槽位移除。
enum LoadFlightState {
    Created,
    AcpWritten,
    Terminal,
}

/// load 结算原因；只用于 actor 内部 exactly-once 防护和脱敏日志。
#[derive(Clone, Copy, Debug)]
enum LoadOutcome {
    Success,
    /// ACP 明确返回 session-not-found，其它错误不得落入此类。
    SessionNotFound,
    LoadError,
    Timeout,
    Cancelled,
    TransportDeath,
    ScopeDead,
}

/// 自动 cold-load 完成后才可写入的 Send；其 reply 必须留到真实 prompt 写成功。
struct PendingSend {
    submission_id: String,
    text: String,
    ticket: SendTicket,
    reply: ReplySender,
}

/// 每个 session 的 catalog request 及其有限等待期限；迟到响应必须被忽略。
struct PendingCatalog {
    request_id: RequestId,
    deadline: Instant,
}

/// 尚未成功运输到产品层的 terminal event；重试必须复用原 event_id 与 sequence。
struct PendingTerminalEvent {
    event: KitProductEvent,
    retain: bool,
    next_attempt: Instant,
}

/// sequence 暂时无法分配时保留的 terminal 意图；成功分配后才生成稳定 event identity。
struct PendingTerminalIntent {
    code: String,
    message: String,
}

/// actor 退出后仍需运输的 terminal event 的稳定幂等键。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct TerminalOutboxKey {
    scope_id: String,
    session_id: String,
    submission_id: String,
}

/// 进程内 terminal outbox；它只延长 actor 退出后的内存生命周期，不提供持久化保证。
#[derive(Default)]
struct TerminalOutbox {
    pending: BTreeMap<TerminalOutboxKey, PendingTerminalEvent>,
}

impl TerminalOutbox {
    /// 按 scope/session/submission 接管终态，重复键保留最早的稳定 event identity。
    fn insert(&mut self, pending: PendingTerminalEvent) -> bool {
        let Some(submission_id) = pending.event.submission_id.as_deref() else {
            tracing::error!("无法为缺少 submission_id 的 terminal event 建立 outbox 键");
            return false;
        };
        let key = TerminalOutboxKey {
            scope_id: pending.event.scope_id.clone(),
            session_id: pending.event.session_id.clone(),
            submission_id: submission_id.to_string(),
        };
        self.pending.entry(key).or_insert(pending);
        true
    }

    /// 在 cleanup/restart 边界立即尝试所有 outbox 项，失败项保留并等待下一次边界。
    fn retry_now(&mut self, sink: &dyn KitEventSink) -> BTreeSet<String> {
        let now = Instant::now();
        for pending in self.pending.values_mut() {
            pending.next_attempt = now;
        }
        let ready = self.pending.keys().cloned().collect::<Vec<_>>();
        let mut delivered_scopes = BTreeSet::new();
        for key in ready {
            let Some(mut pending) = self.pending.remove(&key) else {
                continue;
            };
            if sink.emit(pending.event.clone()).is_ok() {
                delivered_scopes.insert(key.scope_id);
            } else {
                pending.next_attempt = Instant::now() + TERMINAL_RETRY_DELAY;
                tracing::debug!(scope = %key.scope_id, "Host terminal outbox 运输失败，将在后续 cleanup 边界重试");
                self.pending.insert(key, pending);
            }
        }
        delivered_scopes
    }

    /// 返回仍有 pending event 的 scope，用于决定是否可以清除旧 actor 的 terminal 失败标记。
    fn pending_scopes(&self) -> BTreeSet<String> {
        self.pending
            .keys()
            .map(|key| key.scope_id.clone())
            .collect()
    }
}

/// 已写 prompt 的回合状态；cancel 不得抢先清掉它。
struct InFlightTurn {
    submission_id: String,
    cancelled: bool,
}

/// 每 scope 的 IO actor，独占 runtime、投影器、会话和回放内存。
struct ScopeActor {
    scope_id: String,
    acp: AcpRuntime,
    policy: HostPolicy,
    service: Arc<LlmChannelService>,
    sink: Arc<dyn KitEventSink>,
    receiver: Receiver<ActorCommand>,
    accepting: Arc<AtomicBool>,
    /// 与 ActorHandle 共用的退出意图；必须和 accepting 在同一提交门下发布。
    exit_intent: Arc<AtomicBool>,
    /// 与 ActorHandle 共用的提交门；退出意图必须和迟到 command 原子排序。
    submission_lock: Arc<Mutex<()>>,
    /// 已成功入队但尚未被 actor 取出的 command 数；idle 关闭也必须观察它。
    queued_commands: Arc<AtomicUsize>,
    /// MCP 安全违例后的永久 tombstone；普通命令不得自动创建新 generation。
    restart_blocked: Arc<AtomicBool>,
    /// 与 ActorHandle 共用的 cleanup 结果；restart 不能忽略 actor 内部失败。
    cleanup_result: Arc<Mutex<CleanupResult>>,
    /// Supervisor 为该 sidecar 分配的代次；load response 必须匹配该代次。
    generation: u64,
    idle_after: Duration,
    /// 每个 actor 复制一份 catalog deadline，避免测试 seam 触碰生产全局常量。
    mcp_catalog_timeout: Duration,
    /// 每个 actor 复制一份 load deadline，生产值固定为 60 秒。
    load_timeout: Duration,
    expected_tools: BTreeSet<String>,
    projector: Projector,
    initialized: bool,
    initialize_id: Option<RequestId>,
    initialize_deadline: Instant,
    deferred: VecDeque<ActorCommand>,
    pending: BTreeMap<RequestId, PendingRpc>,
    active_sessions: BTreeSet<String>,
    current_session: Option<String>,
    /// 同一 scope 同时最多一个 cold load；其 epoch 与 ACP owner 一起校验。
    load_flight: Option<LoadFlight>,
    next_replay_epoch: u64,
    in_flight: BTreeMap<String, InFlightTurn>,
    cancel_requested: BTreeSet<String>,
    catalog_pending: BTreeMap<String, PendingCatalog>,
    /// 终态运输失败时保留固定 event，直到成功或 cleanup fail-closed。
    pending_terminal_events: BTreeMap<(String, String), PendingTerminalEvent>,
    /// sequence 暂时耗尽时保留 terminal 意图，避免终态随一次分配失败丢失。
    pending_terminal_intents: BTreeMap<(String, String), PendingTerminalIntent>,
    /// actor 退出时接管未成功运输的终态，避免 actor-local pending 随线程一起丢失。
    terminal_outbox: Arc<Mutex<TerminalOutbox>>,
    /// 只保存可恢复 transcript；replay/control/fence 事件不进入此结构。
    transcript: BTreeMap<String, Vec<KitProductEvent>>,
    /// 当前 catalog 失败状态；hot resume 时按状态重建 mcp_failed。
    mcp_failed_sessions: BTreeSet<String>,
    terminal_turns: BTreeSet<(String, String)>,
    last_activity: Instant,
    dead: bool,
    /// 显式 shutdown 已完成；run loop 必须在回执后退出而不是继续消费命令。
    exit_requested: bool,
    /// AcpRuntime shutdown 步骤已成功完成；失败时后续 cleanup 必须重试。
    acp_shutdown_done: bool,
    /// Supervisor scope stop 步骤已成功完成；失败时后续 cleanup 必须重试。
    scope_stop_done: bool,
    /// 仅当所有资源 cleanup 步骤都成功后置位，避免重复关闭已完成步骤。
    cleanup_done: bool,
}

impl ScopeActor {
    /// 建立 actor 的纯内存状态；initialize 必须在 thread 内先写入以保持单 writer。
    #[allow(clippy::too_many_arguments)]
    fn new(
        scope_id: String,
        acp: AcpRuntime,
        policy: HostPolicy,
        service: Arc<LlmChannelService>,
        sink: Arc<dyn KitEventSink>,
        receiver: Receiver<ActorCommand>,
        accepting: Arc<AtomicBool>,
        exit_intent: Arc<AtomicBool>,
        submission_lock: Arc<Mutex<()>>,
        queued_commands: Arc<AtomicUsize>,
        restart_blocked: Arc<AtomicBool>,
        cleanup_result: Arc<Mutex<CleanupResult>>,
        terminal_outbox: Arc<Mutex<TerminalOutbox>>,
        generation: u64,
        idle_after: Duration,
        mcp_catalog_timeout: Duration,
        load_timeout: Duration,
        approved: ApprovedMcpSpec,
    ) -> Self {
        Self {
            projector: Projector::new(scope_id.clone()),
            scope_id,
            acp,
            policy,
            service,
            sink,
            receiver,
            accepting,
            exit_intent,
            submission_lock,
            queued_commands,
            restart_blocked,
            cleanup_result,
            terminal_outbox,
            generation,
            idle_after,
            mcp_catalog_timeout,
            load_timeout,
            expected_tools: approved.expected_tools().clone(),
            initialized: false,
            initialize_id: None,
            initialize_deadline: Instant::now() + INITIALIZE_TIMEOUT,
            deferred: VecDeque::new(),
            pending: BTreeMap::new(),
            active_sessions: BTreeSet::new(),
            current_session: None,
            load_flight: None,
            next_replay_epoch: 0,
            in_flight: BTreeMap::new(),
            cancel_requested: BTreeSet::new(),
            catalog_pending: BTreeMap::new(),
            pending_terminal_events: BTreeMap::new(),
            pending_terminal_intents: BTreeMap::new(),
            transcript: BTreeMap::new(),
            mcp_failed_sessions: BTreeSet::new(),
            terminal_turns: BTreeSet::new(),
            last_activity: Instant::now(),
            dead: false,
            exit_requested: false,
            acp_shutdown_done: false,
            scope_stop_done: false,
            cleanup_done: false,
        }
    }

    /// actor 主循环：优先消费 stdout，再以短 tick 接收 Kit 命令和 idle 截止。
    fn run(&mut self) {
        if let Err(error) = self.begin_initialize() {
            self.enter_dead(error);
        }

        loop {
            self.retry_pending_terminal_events();
            if self.exit_requested {
                return;
            }
            if self.dead {
                match self.receiver.recv_timeout(ACTOR_TICK) {
                    Ok(command) => {
                        self.queued_commands.fetch_sub(1, Ordering::AcqRel);
                        match command {
                            ActorCommand::Shutdown { attempt } => {
                                let cleanup = self.shutdown_and_exit();
                                attempt.complete(cleanup);
                                if self.exit_requested {
                                    return;
                                }
                            }
                            command => self.reject_dead_command(command),
                        }
                    }
                    Err(RecvTimeoutError::Timeout) => continue,
                    Err(RecvTimeoutError::Disconnected) => {
                        self.shutdown_and_exit();
                        return;
                    }
                }
                continue;
            }

            if !self.drain_inbound() {
                continue;
            }
            if !self.initialized && Instant::now() >= self.initialize_deadline {
                self.enter_dead(sidecar_unavailable("sidecar initialize 超时"));
                continue;
            }
            self.expire_load_flight();
            if self.dead {
                continue;
            }
            self.expire_mcp_catalogs();
            if self.dead {
                continue;
            }
            if self.try_idle_stop_and_exit() {
                return;
            }

            match self.receiver.recv_timeout(ACTOR_TICK) {
                Ok(command) => {
                    self.queued_commands.fetch_sub(1, Ordering::AcqRel);
                    match command {
                        ActorCommand::Shutdown { attempt } => {
                            let cleanup = self.shutdown_and_exit();
                            attempt.complete(cleanup);
                            if self.exit_requested {
                                return;
                            }
                        }
                        command => {
                            self.last_activity = Instant::now();
                            if self.handle_command(command) {
                                return;
                            }
                        }
                    }
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => {
                    self.shutdown_and_exit();
                    return;
                }
            }
        }
    }

    /// 初始化请求在 actor 内发送，使整个 stdin 生命周期严格单线程拥有。
    fn begin_initialize(&mut self) -> Result<(), KitError> {
        let id = self
            .acp
            .request_validated(
                "initialize",
                json!({
                    "protocolVersion": HOST_ACP_PROTOCOL_VERSION,
                    "clientCapabilities": {
                        "fs": { "readTextFile": false, "writeTextFile": false },
                        "terminal": false,
                    },
                    "clientInfo": { "name": "efflab-agent-host", "version": env!("CARGO_PKG_VERSION") },
                }),
                &self.policy,
            )
            .map_err(|_| sidecar_unavailable("无法写入 sidecar initialize"))?;
        self.initialize_id = Some(id);
        Ok(())
    }

    /// 有界清空已到达的 stdout 项；达到预算后让出机会给 Cancel/Shutdown 命令。
    fn drain_inbound(&mut self) -> bool {
        for _ in 0..MAX_INBOUND_DRAIN {
            // 每次取队列前推进 deadline，过期后立即退休 transport，避免继续消费旧 replay。
            self.expire_load_flight();
            if self.dead {
                return false;
            }
            self.expire_mcp_catalogs();
            if self.dead {
                return false;
            }
            match self.acp.poll_inbound() {
                Ok(Some(inbound)) => {
                    // poll 与实际处理之间也可能跨过 deadline；此项防止 queued item 越界。
                    self.expire_load_flight();
                    if self.dead {
                        return false;
                    }
                    self.expire_mcp_catalogs();
                    if self.dead {
                        return false;
                    }
                    self.last_activity = Instant::now();
                    self.handle_inbound(inbound);
                    if self.dead {
                        return false;
                    }
                }
                Ok(None) => return true,
                Err(_) => {
                    // transport 终止时统一结算 load、active turn 和其它 pending。
                    self.finish_transport_death();
                    self.enter_dead(sidecar_unavailable("sidecar stdio 已终止"));
                    return false;
                }
            }
        }
        // 下一轮先经过 recv_timeout；命令通道与 stdout 队列相互独立，控制命令因此可达。
        true
    }

    /// 路由 response、notification 和 reverse request，绝不把 ACP payload 直出产品层。
    fn handle_inbound(&mut self, inbound: Inbound) {
        // 该边界同时覆盖未来新增的入站类型，避免调用方绕过 load deadline 门禁。
        self.expire_load_flight();
        if self.dead {
            return;
        }
        match inbound {
            Inbound::Response { id, result } => self.handle_response(id, result),
            Inbound::Notification { method, params } => self.handle_notification(&method, &params),
            Inbound::Request { id, method, params } => {
                self.handle_reverse_request(id, &method, &params)
            }
        }
    }

    /// 首先处理 initialize，再按出站 id 分类完成命令的后续状态机。
    fn handle_response(&mut self, id: RequestId, result: Result<Value, RpcError>) {
        if self.initialize_id == Some(id) {
            self.initialize_id = None;
            match result {
                Ok(result) if validate_initialize_result(&result) => {
                    tracing::debug!(
                        scope = %self.scope_id,
                        event = "sidecar_initialize_validated",
                        "sidecar initialize handshake 已通过最小 ACP 闭集校验"
                    );
                    self.initialized = true;
                    // 初始化完成后按原顺序执行调用方已经提交的命令。
                    while let Some(command) = self.deferred.pop_front() {
                        if self.handle_command(command) {
                            return;
                        }
                        if self.dead {
                            return;
                        }
                    }
                }
                Ok(_) => {
                    tracing::debug!(
                        scope = %self.scope_id,
                        event = "sidecar_initialize_rejected",
                        "sidecar initialize result 不符合 ACP 能力、认证或 Efflab metadata 闭集"
                    );
                    self.enter_dead(sidecar_unavailable("sidecar initialize 握手不受支持"));
                }
                Err(_) => self.enter_dead(sidecar_unavailable("sidecar initialize 被拒绝")),
            }
            return;
        }

        let Some(pending) = self.pending.remove(&id) else {
            // cancel 会使 AcpRuntime 自己移除帐本；迟到 response 不应改变新回合状态。
            return;
        };
        match pending {
            PendingRpc::NewSession { reply } => self.finish_new_session(reply, result),
            PendingRpc::ListSessions { reply } => self.finish_list_sessions(reply, result),
            PendingRpc::DeleteSession { session_id, reply } => {
                self.finish_delete_session(session_id, reply, result)
            }
            PendingRpc::Load {
                session_id,
                replay_epoch,
                generation,
            } => self.finish_load(id, session_id, replay_epoch, generation, result),
            PendingRpc::Prompt {
                session_id,
                submission_id,
            } => self.finish_prompt(session_id, submission_id, result),
            PendingRpc::McpCatalog { session_id } => self.finish_mcp_catalog(session_id, result),
        }
    }

    /// 投影标准 session/update；live 按 sessionId 归属，replay 只接受当前 load flight，未知控制事件不进入产品层。
    fn handle_notification(&mut self, method: &str, params: &Value) {
        // notification 也必须在投影前推进 deadline，避免排队 replay 越过 load 边界。
        self.expire_load_flight();
        if self.dead {
            return;
        }
        let is_replay = params
            .get("_meta")
            .and_then(Value::as_object)
            .and_then(|meta| meta.get("isReplay"))
            .and_then(Value::as_bool)
            == Some(true);
        let session_id = params.get("sessionId").and_then(Value::as_str);
        if is_replay
            && !session_id.is_some_and(|session_id| {
                self.load_flight.as_ref().is_some_and(|flight| {
                    flight.session_id == session_id
                        && matches!(
                            flight.state,
                            LoadFlightState::Created | LoadFlightState::AcpWritten
                        )
                })
            })
        {
            // 没有当前 load flight 的 replay 是超时、旧 generation 或已完成 load 的迟到包。
            tracing::debug!(scope = %self.scope_id, "已丢弃不属于当前 load epoch 的 replay notification");
            return;
        }
        match self.projector.apply_acp_notification(method, params) {
            Ok(events) => {
                for event in events {
                    // 已经终态的 live turn 不接受迟到包，避免 UI 复活已结束流。
                    if event.origin == Origin::Live
                        && event.turn_id.as_ref().is_some_and(|turn_id| {
                            self.terminal_turns
                                .contains(&(event.session_id.clone(), turn_id.clone()))
                        })
                    {
                        continue;
                    }
                    // Projector 可能兼容旧诊断，但 Host 的产品输出只允许 transcript 白名单。
                    if !is_recoverable_product_event(&event)
                        && !matches!(&event.block, KitBlock::Status { code, .. } if code == "mcp_failed")
                    {
                        tracing::debug!(scope = %self.scope_id, "已内部化不支持的 ACP product event");
                        continue;
                    }
                    // 冷 replay 只投影到当前事件流；仅 live 事件进入可供 hot resume 的 transcript。
                    let retain = event.origin == Origin::Live;
                    self.emit_event(event, retain);
                }
            }
            Err(_) => {
                // ACP update 异常不包含可安全展示的稳定 session 时不能构造 Kit event；
                // 仅记录固定诊断，避免回显 sidecar payload。
                tracing::debug!(scope = %self.scope_id, "已跳过无法投影的 ACP notification");
            }
        }
    }

    /// 处理 M1 必须回复的 reverse RPC，权限选择只使用本次 options 中的精确 id。
    fn handle_reverse_request(&mut self, id: RequestId, method: &str, params: &Value) {
        if is_permission_request(method) {
            let session_id = params
                .get("sessionId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let tool_name = params
                .get("toolCall")
                .and_then(Value::as_object)
                .and_then(|tool| tool.get("title"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            let options = params.get("options").and_then(Value::as_array);
            let cancelled = self.cancel_requested.contains(session_id);
            let selected = if cancelled {
                None
            } else if self.is_approved_tool(tool_name) {
                find_option(options, "allow-once")
            } else {
                find_option(options, "reject-once")
            };
            let result = match selected {
                Some(option_id) => json!({
                    "outcome": { "outcome": "selected", "optionId": option_id }
                }),
                None => json!({ "outcome": { "outcome": "cancelled" } }),
            };
            if self
                .acp
                .reply_validated(id, ValidatedReply::Result(result), &self.policy)
                .is_err()
            {
                self.enter_dead(sidecar_unavailable("无法回复 sidecar permission 请求"));
            }
            return;
        }

        if matches!(method, "x.ai/ask_user_question" | "x.ai/exit_plan_mode") {
            let session_id = params
                .get("sessionId")
                .and_then(Value::as_str)
                .or(self.current_session.as_deref())
                .unwrap_or("sidecar-session")
                .to_string();
            if self
                .acp
                .reply_validated(
                    id,
                    ValidatedReply::Result(json!({ "outcome": { "outcome": "cancelled" } })),
                    &self.policy,
                )
                .is_err()
            {
                tracing::debug!(
                    scope = %self.scope_id,
                    session = %session_id,
                    "无法回复不支持的 sidecar 反向请求"
                );
                self.enter_dead(sidecar_unavailable("无法回复不支持的 sidecar 反向请求"));
                return;
            }
            tracing::debug!(
                scope = %self.scope_id,
                session = %session_id,
                "已拒绝不支持的 sidecar 反向请求"
            );
            return;
        }

        if self
            .acp
            .reply_validated(
                id,
                ValidatedReply::Error {
                    code: METHOD_NOT_FOUND,
                    message: "Method not found".to_string(),
                },
                &self.policy,
            )
            .is_err()
        {
            self.enter_dead(sidecar_unavailable("无法回复未知 sidecar 请求"));
        }
    }

    /// 命令在 initialize 前排队，初始化成功后按到达顺序恢复；返回 true 表示 actor 应退出。
    fn handle_command(&mut self, command: ActorCommand) -> bool {
        match command {
            ActorCommand::Shutdown { attempt } => {
                let cleanup = self.shutdown_and_exit();
                attempt.complete(cleanup);
                self.exit_requested
            }
            command => {
                if !self.initialized {
                    self.deferred.push_back(command);
                    return false;
                }
                match command {
                    ActorCommand::NewSession { reply } => {
                        self.start_new_session(reply);
                        false
                    }
                    ActorCommand::ListSessions { cursor, reply } => {
                        self.start_list_sessions(cursor, reply);
                        false
                    }
                    ActorCommand::ResumeSession { session_id, reply } => {
                        self.resume_session(session_id, reply);
                        false
                    }
                    ActorCommand::DeleteSession { session_id, reply } => {
                        self.start_delete_session(session_id, reply);
                        false
                    }
                    ActorCommand::Send {
                        session_id,
                        submission_id,
                        text,
                        ticket,
                        reply,
                    } => {
                        self.send_prompt(session_id, submission_id, text, ticket, reply);
                        false
                    }
                    ActorCommand::Cancel { session_id, reply } => {
                        self.cancel_session(session_id, reply);
                        false
                    }
                    ActorCommand::Shutdown { attempt } => {
                        let cleanup = self.shutdown_and_exit();
                        attempt.complete(cleanup);
                        self.exit_requested
                    }
                }
            }
        }
    }

    /// session/new 必须等待真实 sidecar result 才回复产品 session_id。
    fn start_new_session(&mut self, reply: ReplySender) {
        if !self.in_flight.is_empty() {
            let _ = reply.send(Err(KitError::non_retryable(
                "session_busy",
                "当前 scope 正在生成，不能创建新会话",
            )));
            return;
        }
        let params = json!({
            "cwd": self.policy.expected_cwd,
            "mcpServers": [],
            "_meta": { "modelId": ACP_BYOK_MODEL_SLOT },
        });
        match self
            .acp
            .request_validated("session/new", params, &self.policy)
        {
            Ok(id) => {
                self.pending.insert(id, PendingRpc::NewSession { reply });
            }
            Err(_) => {
                let _ = reply.send(Err(sidecar_unavailable("无法写入 session/new")));
                self.enter_dead(sidecar_unavailable("sidecar stdin 不可用"));
            }
        }
    }

    /// session/list 只透传标准 summary 字段，绝不从 home/workspace 猜测会话内容。
    fn start_list_sessions(&mut self, cursor: Option<String>, reply: ReplySender) {
        let mut params = serde_json::Map::new();
        params.insert(
            "cwd".to_string(),
            Value::String(self.policy.expected_cwd.display().to_string()),
        );
        if let Some(cursor) = cursor {
            params.insert("cursor".to_string(), Value::String(cursor));
        }
        match self
            .acp
            .request_validated("session/list", Value::Object(params), &self.policy)
        {
            Ok(id) => {
                self.pending.insert(id, PendingRpc::ListSessions { reply });
            }
            Err(_) => {
                let _ = reply.send(Err(sidecar_unavailable("无法写入 session/list")));
                self.enter_dead(sidecar_unavailable("sidecar stdin 不可用"));
            }
        }
    }

    /// session/close 删除持久化历史；busy 时拒绝，避免打断正在生成或恢复的回合。
    fn start_delete_session(&mut self, session_id: String, reply: ReplySender) {
        if session_id.is_empty() {
            let _ = reply.send(Err(KitError::non_retryable(
                "invalid_request",
                "删除会话缺少 session_id",
            )));
            return;
        }
        if self.load_flight.is_some() || !self.in_flight.is_empty() {
            let _ = reply.send(Err(KitError::non_retryable(
                "session_busy",
                "当前 scope 正在生成或恢复，不能删除会话",
            )));
            return;
        }
        let params = json!({ "sessionId": session_id });
        match self
            .acp
            .request_validated("session/close", params, &self.policy)
        {
            Ok(id) => {
                self.pending
                    .insert(id, PendingRpc::DeleteSession { session_id, reply });
            }
            Err(_) => {
                let _ = reply.send(Err(sidecar_unavailable("无法写入 session/close")));
                self.enter_dead(sidecar_unavailable("sidecar stdin 不可用"));
            }
        }
    }

    /// 热 resume 重放 transcript；冷 resume 复用 actor 唯一 LoadFlight 并立即 accepted。
    fn resume_session(&mut self, session_id: String, reply: ReplySender) {
        if let Some(flight) = self.load_flight.as_ref() {
            if flight.session_id != session_id {
                let _ = reply.send(Err(KitError::non_retryable(
                    "session_busy",
                    "当前 scope 正在恢复其它会话",
                )));
                return;
            }
            if matches!(
                flight.state,
                LoadFlightState::Created | LoadFlightState::AcpWritten
            ) {
                // waiter 只在 actor 内短暂保存，随后立即排空，保持 resume 不等待 load result。
                self.accept_load_resume(&session_id, reply);
                return;
            }
        }
        if !self.in_flight.is_empty() {
            // Prompting 时仅允许恢复同一 active session；其它 active 或冷会话均不得绕过 busy 门。
            if self.in_flight.contains_key(&session_id)
                && self.active_sessions.contains(&session_id)
            {
                let _ = reply.send(Ok(KitReply::ResumeSession {
                    accepted: true,
                    session_id: session_id.clone(),
                }));
                // 产品回执必须先于可能被慢 sink 阻塞的 replay 投影。
                self.hot_resume(&session_id);
                return;
            }
            let _ = reply.send(Err(KitError::non_retryable(
                "session_busy",
                "当前 scope 正在生成，不能恢复其它会话",
            )));
            return;
        }
        if self.active_sessions.contains(&session_id) {
            let _ = reply.send(Ok(KitReply::ResumeSession {
                accepted: true,
                session_id: session_id.clone(),
            }));
            // 无 in-flight 的热恢复同样先回执，保持所有 hot path 的时序一致。
            self.hot_resume(&session_id);
            return;
        }
        match self.start_load(&session_id, None) {
            Ok(()) => self.accept_load_resume(&session_id, reply),
            Err(error) => {
                let _ = reply.send(Err(error.clone()));
                self.enter_dead(error);
            }
        }
    }

    /// 将 resume waiter 立即排空为 accepted；不把同步回执绑定到 load result。
    fn accept_load_resume(&mut self, session_id: &str, reply: ReplySender) {
        let Some(flight) = self.load_flight.as_mut() else {
            let _ = reply.send(Err(sidecar_unavailable("会话恢复状态已结束")));
            return;
        };
        flight.accepted_resume = true;
        flight.waiters.push(reply);
        let waiters = std::mem::take(&mut flight.waiters);
        for waiter in waiters {
            let _ = waiter.send(Ok(KitReply::ResumeSession {
                accepted: true,
                session_id: session_id.to_string(),
            }));
        }
    }

    /// Send 若当前进程没有 session，则先 cold-load、排空 replay，之后才能写 prompt。
    fn send_prompt(
        &mut self,
        session_id: String,
        submission_id: String,
        text: String,
        ticket: SendTicket,
        reply: ReplySender,
    ) {
        // API 调用已在 catalog/load 等待期间超时的命令绝不能在稍后写入 sidecar stdin。
        if ticket.is_abandoned() {
            return;
        }
        if self.in_flight.contains_key(&session_id) || self.load_flight.is_some() {
            ticket.mark_not_written();
            let _ = reply.send(Err(KitError::non_retryable(
                "turn_in_progress",
                "该会话已有正在处理的回合",
            )));
            return;
        }
        if !self.active_sessions.contains(&session_id) {
            // start_load 失败前不会接管外部 reply，因此保留一份 sender 立即回绝调用方。
            let error_reply = reply.clone();
            let pending_ticket = ticket.clone();
            let pending = PendingSend {
                submission_id,
                text,
                ticket,
                reply,
            };
            if let Err(error) = self.start_load(&session_id, Some(pending)) {
                // start_load 尚未写 prompt，调用方可安全将 SubmissionMap 回滚。
                pending_ticket.mark_not_written();
                let _ = error_reply.send(Err(error.clone()));
                self.enter_dead(error);
            }
            return;
        }
        // MCP catalog 只是能力探测；无论探测是否完成，基本聊天都必须继续可用。
        self.write_prompt(session_id, submission_id, text, ticket, reply);
    }

    /// 真正写 prompt 前消费 pre-cancel；已发 prompt 的 in-flight 只能在 result 后清除。
    fn write_prompt(
        &mut self,
        session_id: String,
        submission_id: String,
        text: String,
        ticket: SendTicket,
        reply: ReplySender,
    ) {
        // 与调用方 timeout 竞争时，只有抢到票据的 actor 可以进入 prompt 写入路径。
        if !ticket.claim_for_prompt() {
            if !ticket.is_abandoned() {
                ticket.mark_not_written();
                let _ = reply.send(Err(sidecar_unavailable("Send 写入权已失效")));
            }
            return;
        }
        if self.cancel_requested.remove(&session_id) {
            ticket.mark_not_written();
            self.emit_turn_status(&session_id, &submission_id, "cancelled", "回合已取消");
            let _ = reply.send(Ok(send_reply(&session_id, &submission_id, false)));
            return;
        }
        let params = json!({
            "sessionId": session_id,
            "prompt": [{ "type": "text", "text": text }],
            "_meta": { "promptId": submission_id },
        });
        match self
            .acp
            .request_validated_with_outcome("session/prompt", params, &self.policy)
        {
            Ok(id) => {
                ticket.mark_written();
                self.in_flight.insert(
                    session_id.clone(),
                    InFlightTurn {
                        submission_id: submission_id.clone(),
                        cancelled: false,
                    },
                );
                self.pending.insert(
                    id,
                    PendingRpc::Prompt {
                        session_id: session_id.clone(),
                        submission_id: submission_id.clone(),
                    },
                );
                // Send 只等 stdin 写入；绝不等 session/prompt JSON-RPC result。
                let _ = reply.send(Ok(send_reply(&session_id, &submission_id, false)));
            }
            Err(RequestWriteFailure::NotWritten(_)) => {
                ticket.mark_not_written();
                tracing::debug!(
                    scope = %self.scope_id,
                    session = %session_id,
                    "session/prompt 在写入前失败"
                );
                let _ = reply.send(Err(sidecar_unavailable("无法写入 session/prompt")));
                self.enter_dead(sidecar_unavailable("sidecar stdin 不可用"));
            }
            Err(RequestWriteFailure::MayHaveBeenWritten(_)) => {
                ticket.mark_may_have_been_written();
                tracing::debug!(
                    scope = %self.scope_id,
                    session = %session_id,
                    "session/prompt 写入结局无法确认"
                );
                let _ = reply.send(Err(sidecar_unavailable("无法确认 session/prompt 是否写入")));
                self.enter_dead(sidecar_unavailable("sidecar stdin 不可用"));
            }
        }
    }

    /// cancel 始终写无 id notification；cold load 时直接终结绑定的 LoadFlight。
    fn cancel_session(&mut self, session_id: String, reply: ReplySender) {
        match self.acp.notify_validated(
            "session/cancel",
            json!({ "sessionId": session_id }),
            &self.policy,
        ) {
            Ok(()) => {
                let load_identity = self.load_flight.as_ref().and_then(|flight| {
                    (flight.session_id == session_id
                        && matches!(
                            flight.state,
                            LoadFlightState::Created | LoadFlightState::AcpWritten
                        ))
                    .then_some((
                        flight.owner_request_id,
                        flight.session_id.clone(),
                        flight.replay_epoch,
                        flight.generation,
                    ))
                });
                // 先解除产品调用，再让同步 sink 投影取消状态，避免回执被事件运输阻塞。
                let _ = reply.send(Ok(KitReply::Cancel { accepted: true }));
                if let Some((owner_request_id, load_session_id, replay_epoch, generation)) =
                    load_identity
                {
                    // 取消通知成功后立即结算同一 flight；旧 load response/replay 不能再污染后续 Send。
                    let _ = self.finish_load_flight(
                        owner_request_id,
                        load_session_id,
                        replay_epoch,
                        generation,
                        LoadOutcome::Cancelled,
                    );
                    self.enter_dead(sidecar_unavailable("sidecar load 已取消"));
                    return;
                }

                // 只有通知已经写入 sidecar，才向本地状态和产品事件声明该回合已取消。
                let cancelled_submission =
                    self.in_flight.get_mut(&session_id).and_then(|in_flight| {
                        if in_flight.cancelled {
                            None
                        } else {
                            in_flight.cancelled = true;
                            Some(in_flight.submission_id.clone())
                        }
                    });
                if let Some(submission_id) = cancelled_submission {
                    self.cancel_requested.insert(session_id.clone());
                    self.emit_turn_status(&session_id, &submission_id, "cancelled", "回合已取消");
                } else if !self.in_flight.contains_key(&session_id) {
                    // 无 active turn 的 Cancel 仍保留一次性 pre-cancel 合同；BTreeSet 去重 marker。
                    self.cancel_requested.insert(session_id.clone());
                }
            }
            Err(_) => {
                let _ = reply.send(Err(sidecar_unavailable("无法写入 session/cancel")));
                self.enter_dead(sidecar_unavailable("sidecar stdin 不可用"));
            }
        }
    }

    /// 冷 load 只创建一个 scope-private flight，并在写入前建立新的 replay epoch。
    fn start_load(
        &mut self,
        session_id: &str,
        pending_send: Option<PendingSend>,
    ) -> Result<(), KitError> {
        if self.load_flight.is_some() {
            return Err(KitError::non_retryable(
                "session_busy",
                "当前 scope 已有正在进行的会话恢复",
            ));
        }
        self.next_replay_epoch = self
            .next_replay_epoch
            .checked_add(1)
            .ok_or_else(|| sidecar_unavailable("replay epoch 已耗尽"))?;
        let replay_epoch = self.next_replay_epoch;
        self.current_session = Some(session_id.to_string());
        self.projector.begin_replay(session_id);
        let params = json!({
            "sessionId": session_id,
            "cwd": self.policy.expected_cwd,
            "mcpServers": [],
            "_meta": { "modelId": ACP_BYOK_MODEL_SLOT },
        });
        let id = self
            .acp
            .request_validated("session/load", params, &self.policy)
            .map_err(|_| sidecar_unavailable("无法写入 session/load"))?;
        let mut flight = LoadFlight {
            session_id: session_id.to_string(),
            owner_request_id: id,
            replay_epoch,
            waiters: Vec::new(),
            accepted_resume: false,
            pending_send,
            deadline: Instant::now() + self.load_timeout,
            generation: self.generation,
            state: LoadFlightState::Created,
        };
        self.pending.insert(
            id,
            PendingRpc::Load {
                session_id: session_id.to_string(),
                replay_epoch,
                generation: self.generation,
            },
        );
        flight.state = LoadFlightState::AcpWritten;
        self.load_flight = Some(flight);
        Ok(())
    }

    /// 解析 session/new response，随后启动 MCP catalog 观察但不把它变成对话硬阻塞。
    fn finish_new_session(&mut self, reply: ReplySender, result: Result<Value, RpcError>) {
        let session_id = match result {
            Ok(result) => result
                .get("sessionId")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .map(str::to_string),
            Err(_) => None,
        };
        let Some(session_id) = session_id else {
            let _ = reply.send(Err(sidecar_unavailable("sidecar 未返回有效 sessionId")));
            return;
        };
        self.active_sessions.insert(session_id.clone());
        self.current_session = Some(session_id.clone());
        self.transcript.entry(session_id.clone()).or_default();
        self.start_mcp_catalog(&session_id);
        let _ = reply.send(Ok(KitReply::NewSession { session_id }));
    }

    /// 映射标准 sidecar session/list 的四字段产品摘要。
    fn finish_list_sessions(&mut self, reply: ReplySender, result: Result<Value, RpcError>) {
        let Ok(result) = result else {
            let _ = reply.send(Err(sidecar_unavailable("sidecar session/list 失败")));
            return;
        };
        let sessions = result
            .get("sessions")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| {
                        let session_id = item.get("sessionId").and_then(Value::as_str)?;
                        (!session_id.is_empty()).then(|| SessionSummary {
                            session_id: session_id.to_string(),
                            title: item
                                .get("title")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_string(),
                            updated_at: item
                                .get("updatedAt")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_string(),
                            is_active: self.active_sessions.contains(session_id),
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let next_cursor = result
            .get("nextCursor")
            .and_then(Value::as_str)
            .map(str::to_string);
        let _ = reply.send(Ok(KitReply::ListSessions {
            sessions,
            next_cursor,
        }));
    }

    /// 成功 close 后从 actor 内存摘掉该 session；找不到则映射为 session_not_found。
    fn finish_delete_session(
        &mut self,
        session_id: String,
        reply: ReplySender,
        result: Result<Value, RpcError>,
    ) {
        match result {
            Ok(_) => {
                self.active_sessions.remove(&session_id);
                self.transcript.remove(&session_id);
                self.mcp_failed_sessions.remove(&session_id);
                if self.current_session.as_deref() == Some(session_id.as_str()) {
                    self.current_session = None;
                }
                tracing::debug!(
                    scope = %self.scope_id,
                    event = "session_deleted",
                    "Kit delete_session 已删除 sidecar session"
                );
                let _ = reply.send(Ok(KitReply::DeleteSession { session_id }));
            }
            Err(error) if is_close_session_not_found(&error) => {
                let _ = reply.send(Err(KitError::non_retryable(
                    "session_not_found",
                    "sidecar 未找到指定会话",
                )));
            }
            Err(_) => {
                let _ = reply.send(Err(sidecar_unavailable("sidecar 删除会话失败")));
            }
        }
    }

    /// 按当前 actor 的唯一 owner 结算 load；取走 flight 后后续 response 只能被丢弃。
    fn finish_current_load(&mut self, outcome: LoadOutcome) -> bool {
        let Some((owner_request_id, session_id, replay_epoch, generation)) =
            self.load_flight.as_ref().map(|flight| {
                (
                    flight.owner_request_id,
                    flight.session_id.clone(),
                    flight.replay_epoch,
                    flight.generation,
                )
            })
        else {
            return false;
        };
        self.finish_load_flight(
            owner_request_id,
            session_id,
            replay_epoch,
            generation,
            outcome,
        )
    }

    /// transport 终止时只结算当前 flight 一次，避免将 load 错误混同为普通 scope cleanup。
    fn finish_transport_death(&mut self) {
        let _ = self.finish_current_load(LoadOutcome::TransportDeath);
    }

    /// load response 只结算匹配的 owner/epoch/generation，并保证 flight exactly-once。
    fn finish_load(
        &mut self,
        owner_request_id: RequestId,
        session_id: String,
        replay_epoch: u64,
        generation: u64,
        result: Result<Value, RpcError>,
    ) {
        let timed_out = self.load_flight.as_ref().is_some_and(|flight| {
            Self::load_flight_matches(
                flight,
                owner_request_id,
                &session_id,
                replay_epoch,
                generation,
            ) && flight.deadline <= Instant::now()
        });
        let outcome = if timed_out {
            LoadOutcome::Timeout
        } else {
            match result {
                Ok(_) => LoadOutcome::Success,
                Err(error) if is_session_not_found(&error) => LoadOutcome::SessionNotFound,
                Err(_) => LoadOutcome::LoadError,
            }
        };
        if self.finish_load_flight(
            owner_request_id,
            session_id,
            replay_epoch,
            generation,
            outcome,
        ) && should_retire_after_load(outcome)
        {
            // 失败 load 的 transport 不再承载同 session 新 flight，隔离没有 generation 的旧 replay。
            self.enter_dead(sidecar_unavailable("sidecar load 失败，transport 已退休"));
        }
    }

    /// 判断输入的 load metadata 是否仍对应当前 active flight。
    fn load_flight_matches(
        flight: &LoadFlight,
        owner_request_id: RequestId,
        session_id: &str,
        replay_epoch: u64,
        generation: u64,
    ) -> bool {
        flight.owner_request_id == owner_request_id
            && flight.session_id == session_id
            && flight.replay_epoch == replay_epoch
            && flight.generation == generation
            && matches!(
                flight.state,
                LoadFlightState::Created | LoadFlightState::AcpWritten
            )
    }

    /// 超时、正常 response 和 transport 失败共用同一条 load 结算路径。
    fn finish_load_flight(
        &mut self,
        owner_request_id: RequestId,
        session_id: String,
        replay_epoch: u64,
        generation: u64,
        outcome: LoadOutcome,
    ) -> bool {
        let matches_current = self.load_flight.as_ref().is_some_and(|flight| {
            Self::load_flight_matches(
                flight,
                owner_request_id,
                &session_id,
                replay_epoch,
                generation,
            )
        });
        if !matches_current {
            // 旧 epoch 或已进入 Terminal 的 response 不得再次改写当前会话状态。
            tracing::debug!(
                scope = %self.scope_id,
                session = %session_id,
                epoch = replay_epoch,
                generation,
                "已丢弃不匹配的 session/load 结算"
            );
            return false;
        }
        let Some(mut flight) = self.load_flight.take() else {
            return false;
        };
        flight.state = LoadFlightState::Terminal;
        self.pending.retain(|_, pending| {
            !matches!(
                pending,
                PendingRpc::Load {
                    session_id: pending_session,
                    replay_epoch: pending_epoch,
                    generation: pending_generation,
                } if pending_session == &session_id
                    && *pending_epoch == replay_epoch
                    && *pending_generation == generation
            )
        });
        tracing::debug!(
            scope = %self.scope_id,
            session = %session_id,
            epoch = replay_epoch,
            generation,
            outcome = ?outcome,
            accepted_resume = flight.accepted_resume,
            "session/load flight 已结算"
        );
        match outcome {
            LoadOutcome::Success => {
                self.active_sessions.insert(session_id.clone());
                self.current_session = Some(session_id.clone());
                self.transcript.entry(session_id.clone()).or_default();
                // Projector 的旧跳过计数只用于结束内部 replay 栅栏，不转成产品事件。
                let _ = self.projector.take_replay_skipped_count(&session_id);
                self.finish_replay(&session_id);
                self.start_mcp_catalog(&session_id);
                if let Some(pending) = flight.pending_send {
                    self.send_prompt(
                        session_id,
                        pending.submission_id,
                        pending.text,
                        pending.ticket,
                        pending.reply,
                    );
                }
            }
            LoadOutcome::SessionNotFound
            | LoadOutcome::LoadError
            | LoadOutcome::Timeout
            | LoadOutcome::Cancelled
            | LoadOutcome::TransportDeath
            | LoadOutcome::ScopeDead => {
                self.current_session = None;
                // 失败 flight 的取消意图只属于本次 flight，不能污染后续独立 Send。
                self.cancel_requested.remove(&session_id);
                let error = load_outcome_error(outcome);
                if flight.accepted_resume {
                    // Resume 已经 accepted，后续必须给产品一次可观察的 session-level 终止事件。
                    self.emit_session_error(&session_id, error.clone());
                }
                for waiter in flight.waiters {
                    let _ = waiter.send(Err(error.clone()));
                }
                if let Some(pending) = flight.pending_send {
                    // load 失败发生在 prompt 写入前，保留 Abandoned 终态，否则标记可安全重试。
                    pending.ticket.mark_not_written();
                    let _ = pending.reply.send(Err(error));
                }
            }
        }
        true
    }

    /// load deadline 到期时撤销 ACP owner 与 actor pending，确保迟到 response 只能被丢弃。
    fn expire_load_flight(&mut self) {
        let Some(flight) = self.load_flight.as_ref() else {
            return;
        };
        if !matches!(
            flight.state,
            LoadFlightState::Created | LoadFlightState::AcpWritten
        ) || flight.deadline > Instant::now()
        {
            return;
        }
        let request_id = flight.owner_request_id;
        let session_id = flight.session_id.clone();
        let replay_epoch = flight.replay_epoch;
        let generation = flight.generation;
        if self.acp.revoke_outbound_request(request_id).is_err() {
            self.enter_dead(sidecar_unavailable("无法撤销超时 session/load 请求"));
            return;
        }
        if self.finish_load_flight(
            request_id,
            session_id,
            replay_epoch,
            generation,
            LoadOutcome::Timeout,
        ) {
            // deadline 到期后退休整个 actor，避免没有 generation 的旧 replay 进入新 flight。
            self.enter_dead(sidecar_unavailable("session/load 已超时，transport 已退休"));
        }
    }

    /// prompt response 才释放 in-flight；cancel 已发状态时不可追加 turn_completed。
    fn finish_prompt(
        &mut self,
        session_id: String,
        submission_id: String,
        result: Result<Value, RpcError>,
    ) {
        let Some(in_flight) = self.in_flight.remove(&session_id) else {
            // transport cleanup 已经移除该 turn 时，迟到 response 不能制造第二个终态。
            return;
        };
        self.cancel_requested.remove(&session_id);
        if in_flight.cancelled {
            return;
        }
        match result {
            Ok(_) => {
                self.emit_turn_status(&session_id, &submission_id, "turn_completed", "回合已完成")
            }
            Err(error) => self.emit_turn_status(
                &session_id,
                &submission_id,
                "error",
                turn_failure_user_message(&error.message),
            ),
        }
    }

    /// 解析真实嵌套 result.result.servers catalog，执行额外工具 kill 与缺工具降级。
    fn finish_mcp_catalog(&mut self, session_id: String, result: Result<Value, RpcError>) {
        self.catalog_pending.remove(&session_id);
        let catalog = result.and_then(|result| {
            parse_catalog(&result).ok_or(RpcError {
                code: -1,
                message: "invalid catalog".to_string(),
                data: None,
            })
        });
        match catalog {
            Ok(tools) => {
                if tools.iter().any(|tool| !self.is_approved_tool(tool)) {
                    tracing::debug!(scope = %self.scope_id, "MCP catalog 包含未批准工具，已终止 sidecar");
                    // 安全违例不是普通 transport dead；在显式 Channel 换代前永久保留 tombstone。
                    self.restart_blocked.store(true, Ordering::Release);
                    self.enter_dead(sidecar_unavailable("MCP catalog 包含未批准工具"));
                    return;
                }
                if !self.expected_tools.is_empty() && !self.expected_tools.is_subset(&tools) {
                    self.mcp_failed_sessions.insert(session_id.clone());
                    self.emit_session_status(
                        &session_id,
                        "mcp_failed",
                        "部分已批准 MCP 工具未就绪",
                        Origin::Live,
                    );
                } else {
                    self.mcp_failed_sessions.remove(&session_id);
                }
            }
            Err(_) if !self.expected_tools.is_empty() => {
                self.mcp_failed_sessions.insert(session_id.clone());
                self.emit_session_status(
                    &session_id,
                    "mcp_failed",
                    "无法确认已批准 MCP 工具状态",
                    Origin::Live,
                );
            }
            Err(_) => {
                self.mcp_failed_sessions.remove(&session_id);
            }
        }
    }

    /// 请求每个新/冷恢复会话的 catalog；空批准集不启动可选能力探测。
    fn start_mcp_catalog(&mut self, session_id: &str) {
        if self.expected_tools.is_empty() {
            return;
        }
        let params = json!({ "sessionId": session_id });
        match self
            .acp
            .request_validated("x.ai/mcp/list", params, &self.policy)
        {
            Ok(id) => {
                self.pending.insert(
                    id,
                    PendingRpc::McpCatalog {
                        session_id: session_id.to_string(),
                    },
                );
                self.catalog_pending.insert(
                    session_id.to_string(),
                    PendingCatalog {
                        request_id: id,
                        deadline: Instant::now() + self.mcp_catalog_timeout,
                    },
                );
            }
            Err(_) => {
                // catalog 的 JSON-RPC error 可以降级，但连查询都写不进 stdin 时 sidecar
                // 已不可信，不能绕过 gate 继续写 prompt。
                self.enter_dead(sidecar_unavailable("无法写入 MCP catalog 请求"));
            }
        }
    }

    /// 到达冻结 deadline 后按 catalog error 降级，并同时撤销 actor 与 ACP 两侧账本以丢弃迟到响应。
    fn expire_mcp_catalogs(&mut self) {
        let now = Instant::now();
        let expired = self
            .catalog_pending
            .iter()
            .filter(|(_, pending)| pending.deadline <= now)
            .map(|(session_id, pending)| (session_id.clone(), pending.request_id))
            .collect::<Vec<_>>();
        for (session_id, request_id) in expired {
            self.catalog_pending.remove(&session_id);
            if self.acp.revoke_outbound_request(request_id).is_err() {
                // 无法取得 ACP 账本锁时不能确信后续请求是否会被正确限额，保守停掉 scope。
                self.enter_dead(sidecar_unavailable("无法撤销超时 MCP catalog 请求"));
                return;
            }
            if matches!(
                self.pending.get(&request_id),
                Some(PendingRpc::McpCatalog { session_id: pending_session }) if pending_session == &session_id
            ) {
                self.pending.remove(&request_id);
            }
            self.finish_mcp_catalog(
                session_id,
                Err(RpcError {
                    code: -1,
                    message: "catalog timeout".to_string(),
                    data: None,
                }),
            );
            if self.dead {
                return;
            }
        }
    }

    /// 热恢复不触碰 ACP；只重放 transcript，并为每次调用新生成 replay fence。
    fn hot_resume(&mut self, session_id: &str) {
        let snapshot = self.transcript.get(session_id).cloned().unwrap_or_default();
        let mut live_assistant = None;
        for mut event in snapshot {
            event.origin = Origin::Replay;
            if let KitBlock::Assistant { streaming, .. } = &mut event.block {
                *streaming = false;
                live_assistant = Some(event.clone());
            }
            self.emit_event(event, false);
        }
        self.emit_session_status(
            session_id,
            "replay_complete",
            "历史重放完成",
            Origin::Replay,
        );
        if self.mcp_failed_sessions.contains(session_id) {
            // mcp_failed 是当前状态，不从 transcript 重放；每次热恢复按状态重建一条 live 事件。
            self.emit_session_status(
                session_id,
                "mcp_failed",
                "已批准 MCP 工具仍未就绪",
                Origin::Live,
            );
        }
        // Prompting 热恢复后补一张 live 快照，避免 UI 在 replay fence 后永久显示非流式。
        if self.in_flight.contains_key(session_id)
            && let Some(mut event) = live_assistant
        {
            event.origin = Origin::Live;
            if let KitBlock::Assistant { streaming, .. } = &mut event.block {
                *streaming = true;
            }
            self.emit_event(event, false);
        }
    }

    /// 冷恢复的 replay fence；旧跳过诊断不再发送，fence 每次由当前调用新生成。
    fn finish_replay(&mut self, session_id: &str) {
        self.emit_session_status(
            session_id,
            "replay_complete",
            "历史重放完成",
            Origin::Replay,
        );
    }

    /// 发送 session/process 级状态，严格使用 null turn/submission 与 Host 固定 ID 形状。
    fn emit_session_status(&mut self, session_id: &str, code: &str, message: &str, origin: Origin) {
        let Ok(sequence) = self.projector.next_host_sequence(session_id) else {
            return;
        };
        let event_id = format!("{session_id}:host:{code}:{sequence}");
        self.emit_event(
            KitProductEvent {
                schema_version: KIT_SCHEMA_VERSION,
                scope_id: self.scope_id.clone(),
                session_id: session_id.to_string(),
                turn_id: None,
                submission_id: None,
                event_id: event_id.clone(),
                sequence,
                origin,
                block_id: event_id,
                block: KitBlock::Status {
                    code: code.to_string(),
                    message: message.to_string(),
                },
            },
            true,
        );
    }

    /// 发送 session 级错误；它不伪造 turn 标识，也不进入可恢复 transcript。
    fn emit_session_error(&mut self, session_id: &str, error: KitError) {
        let Ok(sequence) = self.projector.next_host_sequence(session_id) else {
            return;
        };
        let event_id = format!("{session_id}:host:{}:{sequence}", error.code);
        self.emit_event(
            KitProductEvent {
                schema_version: KIT_SCHEMA_VERSION,
                scope_id: self.scope_id.clone(),
                session_id: session_id.to_string(),
                turn_id: None,
                submission_id: None,
                event_id: event_id.clone(),
                sequence,
                origin: Origin::Live,
                block_id: event_id,
                block: KitBlock::Error(error),
            },
            false,
        );
    }

    /// 发送回合级终态；它必须携带真实 prompt/submission id，不能伪造 synthetic id。
    fn emit_turn_status(
        &mut self,
        session_id: &str,
        submission_id: &str,
        code: &str,
        message: &str,
    ) {
        let key = (session_id.to_string(), submission_id.to_string());
        // Cancel、transport cleanup 与迟到 prompt response 共享此门，终态只能投影一次。
        // 先检查而不提交 marker，避免 sequence 耗尽时把未生成的 terminal 永久吞掉。
        if self.terminal_turns.contains(&key)
            || self.pending_terminal_events.contains_key(&key)
            || self.pending_terminal_intents.contains_key(&key)
        {
            return;
        }
        let sequence = match self.projector.next_host_sequence(session_id) {
            Ok(sequence) => sequence,
            Err(_) => {
                self.pending_terminal_intents.insert(
                    key,
                    PendingTerminalIntent {
                        code: code.to_string(),
                        message: message.to_string(),
                    },
                );
                record_cleanup_failure(
                    &self.cleanup_result,
                    &self.scope_id,
                    CleanupFailureKind::TerminalEvent,
                );
                return;
            }
        };
        self.terminal_turns.insert(key.clone());
        let event_id = format!("{session_id}:host:{code}:{sequence}");
        self.pending_terminal_events.insert(
            key,
            PendingTerminalEvent {
                event: KitProductEvent {
                    schema_version: KIT_SCHEMA_VERSION,
                    scope_id: self.scope_id.clone(),
                    session_id: session_id.to_string(),
                    turn_id: Some(submission_id.to_string()),
                    submission_id: Some(submission_id.to_string()),
                    event_id: event_id.clone(),
                    sequence,
                    origin: Origin::Live,
                    block_id: event_id,
                    block: KitBlock::Status {
                        code: code.to_string(),
                        message: message.to_string(),
                    },
                },
                retain: true,
                next_attempt: Instant::now(),
            },
        );
        self.retry_pending_terminal_events();
    }

    /// 将 sequence 暂时耗尽的 terminal 意图物化为稳定 event；失败时意图继续保留。
    fn materialize_pending_terminal_intents(&mut self) {
        let keys = self
            .pending_terminal_intents
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            let Some(intent) = self.pending_terminal_intents.remove(&key) else {
                continue;
            };
            if self.terminal_turns.contains(&key) || self.pending_terminal_events.contains_key(&key)
            {
                continue;
            }
            let session_id = key.0.clone();
            let submission_id = key.1.clone();
            let sequence = match self.projector.next_host_sequence(&session_id) {
                Ok(sequence) => sequence,
                Err(_) => {
                    self.pending_terminal_intents.insert(key, intent);
                    record_cleanup_failure(
                        &self.cleanup_result,
                        &self.scope_id,
                        CleanupFailureKind::TerminalEvent,
                    );
                    continue;
                }
            };
            let event_id = format!("{session_id}:host:{}:{sequence}", intent.code);
            self.terminal_turns.insert(key.clone());
            self.pending_terminal_events.insert(
                key,
                PendingTerminalEvent {
                    event: KitProductEvent {
                        schema_version: KIT_SCHEMA_VERSION,
                        scope_id: self.scope_id.clone(),
                        session_id: session_id.clone(),
                        turn_id: Some(submission_id.clone()),
                        submission_id: Some(submission_id.clone()),
                        event_id: event_id.clone(),
                        sequence,
                        origin: Origin::Live,
                        block_id: event_id,
                        block: KitBlock::Status {
                            code: intent.code,
                            message: intent.message,
                        },
                    },
                    retain: true,
                    next_attempt: Instant::now(),
                },
            );
        }
    }

    /// 在关闭路径立即重试失败的回合终态，不受原定时器延迟影响。
    fn retry_pending_terminal_events_now(&mut self) {
        self.materialize_pending_terminal_intents();
        let now = Instant::now();
        for pending in self.pending_terminal_events.values_mut() {
            pending.next_attempt = now;
        }
        self.retry_pending_terminal_events();
    }

    /// 重试失败的回合终态；每次重试复用同一 event_id，兼容下游幂等去重。
    fn retry_pending_terminal_events(&mut self) {
        self.materialize_pending_terminal_intents();
        let now = Instant::now();
        let ready = self
            .pending_terminal_events
            .iter()
            .filter(|(_, pending)| pending.next_attempt <= now)
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in ready {
            let Some(mut pending) = self.pending_terminal_events.remove(&key) else {
                continue;
            };
            if self.sink.emit(pending.event.clone()).is_ok() {
                self.retain_event(pending.event, pending.retain);
                if self.pending_terminal_events.is_empty()
                    && self.pending_terminal_intents.is_empty()
                {
                    clear_cleanup_failure(&self.cleanup_result, CleanupFailureKind::TerminalEvent);
                }
            } else {
                pending.next_attempt = Instant::now() + TERMINAL_RETRY_DELAY;
                self.pending_terminal_events.insert(key, pending);
                tracing::debug!(scope = %self.scope_id, "Kit 回合终态运输失败，将重试");
            }
        }
    }

    /// actor 退出前把仍未送达的终态转交 Host outbox，保留稳定 event_id/sequence。
    fn handoff_pending_terminal_events(&mut self) {
        if self.pending_terminal_events.is_empty() {
            return;
        }
        // 先取得 outbox guard，再从 actor 移交所有权；即使锁曾 poison，也不能在 actor
        // 退出时让 pending event 随栈帧丢失。TerminalOutbox 只含可恢复的内存 Map。
        let mut outbox = match self.terminal_outbox.lock() {
            Ok(outbox) => outbox,
            Err(poisoned) => {
                tracing::error!(
                    scope = %self.scope_id,
                    cleanup_failure = ?CleanupFailureKind::TerminalEvent,
                    "terminal outbox 锁曾异常中断，恢复后继续接管终态"
                );
                let outbox = poisoned.into_inner();
                self.terminal_outbox.clear_poison();
                outbox
            }
        };
        let pending = std::mem::take(&mut self.pending_terminal_events);
        let pending_count = pending.len();
        let mut handed_off = 0usize;
        for (_, event) in pending {
            if outbox.insert(event) {
                handed_off += 1;
            } else {
                record_cleanup_failure(
                    &self.cleanup_result,
                    &self.scope_id,
                    CleanupFailureKind::TerminalEvent,
                );
            }
        }
        tracing::debug!(
            scope = %self.scope_id,
            pending_count,
            handed_off,
            "actor 退出前已转交 terminal outbox"
        );
    }

    /// 输出前再次走验证 sink；仅白名单事件写入 transcript，control/fence 单独运输。
    fn emit_event(&mut self, event: KitProductEvent, retain: bool) {
        if self.sink.emit(event.clone()).is_err() {
            tracing::debug!(scope = %self.scope_id, "Kit 事件运输失败");
            return;
        }
        self.retain_event(event, retain);
    }

    /// 在 sink 成功后保存可恢复事件；失败或重试期间绝不提前污染 transcript。
    fn retain_event(&mut self, event: KitProductEvent, retain: bool) {
        let session_id = event.session_id.clone();
        if retain
            && is_recoverable_product_event(&event)
            && (self.active_sessions.contains(&session_id)
                || self
                    .load_flight
                    .as_ref()
                    .is_some_and(|flight| flight.session_id == session_id))
        {
            self.transcript.entry(session_id).or_default().push(event);
        }
    }

    /// 是否处于用户已取消或批准 MCP 集合中的安全工具。
    fn is_approved_tool(&self, tool_name: &str) -> bool {
        tool_name == NOOP_TOOL
            || (is_qualified_tool_name(tool_name) && self.expected_tools.contains(tool_name))
    }

    /// 判断 actor 是否仍持有尚未成功物化或运输的 terminal。
    fn has_pending_terminal_events(&self) -> bool {
        !self.pending_terminal_events.is_empty() || !self.pending_terminal_intents.is_empty()
    }

    /// 不在 prompt/load/catalog 中且超过 idle 阈值时，才能关闭该 scope 私有进程。
    fn should_idle_stop(&self) -> bool {
        self.initialized
            && self.pending.is_empty()
            && self.in_flight.is_empty()
            && self.load_flight.is_none()
            && self.catalog_pending.is_empty()
            && !self.has_pending_terminal_events()
            && self.deferred.is_empty()
            && self.last_activity.elapsed() >= self.idle_after
    }

    /// 与 ActorHandle 共用提交门；自然退出不能和迟到 command 逆序。
    fn stop_accepting(&self) {
        match self.submission_lock.lock() {
            Ok(_submission) => self.accepting.store(false, Ordering::Release),
            Err(_) => {
                self.accepting.store(false, Ordering::Release);
                record_cleanup_failure(
                    &self.cleanup_result,
                    &self.scope_id,
                    CleanupFailureKind::SubmissionLock,
                );
            }
        }
    }

    /// 结束所有仍登记的 prompt，并撤销对应 PendingRpc，保证每个回合只有一个终态。
    fn finish_in_flight_turns(&mut self, code: &str, message: &str) {
        let turns = std::mem::take(&mut self.in_flight);
        self.pending.retain(|_, pending| {
            !matches!(
                pending,
                PendingRpc::Prompt {
                    session_id,
                    submission_id,
                } if turns.get(session_id).is_some_and(|turn| turn.submission_id == *submission_id)
            )
        });
        for (session_id, turn) in turns {
            self.cancel_requested.remove(&session_id);
            self.emit_turn_status(&session_id, &turn.submission_id, code, message);
        }
    }

    /// 执行一次 actor 资源清理；每个失败步骤既记录分类又参与 restart 成功判定。
    fn cleanup_resources(&mut self) -> CleanupResult {
        if self.cleanup_done {
            return snapshot_cleanup_result(&self.cleanup_result, &self.scope_id);
        }
        let mut result = CleanupResult::default();
        if !self.acp_shutdown_done {
            if self.acp.shutdown().is_err() {
                record_cleanup_failure(
                    &self.cleanup_result,
                    &self.scope_id,
                    CleanupFailureKind::AcpShutdown,
                );
                result.record(CleanupFailureKind::AcpShutdown);
            } else {
                self.acp_shutdown_done = true;
                clear_cleanup_failure(&self.cleanup_result, CleanupFailureKind::AcpShutdown);
                tracing::debug!(
                    scope = %self.scope_id,
                    cleanup_step = "acp_shutdown",
                    "sidecar ACP runtime cleanup 已完成"
                );
            }
        }
        if !self.scope_stop_done {
            if self.service.stop_scope(&self.scope_id).is_err() {
                record_cleanup_failure(
                    &self.cleanup_result,
                    &self.scope_id,
                    CleanupFailureKind::ScopeStop,
                );
                result.record(CleanupFailureKind::ScopeStop);
            } else {
                self.scope_stop_done = true;
                clear_cleanup_failure(&self.cleanup_result, CleanupFailureKind::ScopeStop);
                tracing::debug!(
                    scope = %self.scope_id,
                    cleanup_step = "scope_stop",
                    "sidecar Supervisor scope cleanup 已完成"
                );
            }
        }
        if self.acp_shutdown_done && self.scope_stop_done {
            self.cleanup_done = true;
        }
        result.merge(&snapshot_cleanup_result(
            &self.cleanup_result,
            &self.scope_id,
        ));
        result
    }

    /// 异常 sidecar 不自动重拉；先退休代次并结算 cold load/active turn，再拒绝其它命令。
    fn enter_dead(&mut self, error: KitError) {
        if self.dead {
            return;
        }
        self.stop_accepting();
        self.dead = true;
        // ScopeDead 结算必须发生在 ACP shutdown 前，确保 accepted resume 有终止事件。
        self.finish_current_load(LoadOutcome::ScopeDead);
        self.finish_in_flight_turns("error", TURN_FAILED_USER_MESSAGE);
        // 给 transient sink failure 一次额外机会；若仍失败则由 dead actor 后续有限重试。
        self.retry_pending_terminal_events();
        self.cleanup_resources();
        self.reject_pending(error);
    }

    /// 正常 idle 退出允许 Host 在下一条命令按旧 session cold-load 新起一代。
    fn try_idle_stop_and_exit(&mut self) -> bool {
        // idle 判定和 accepting=false 必须与外部 submit 共用同一临界区，避免命令
        // 在判定之后、actor 退出之前被错误接受而无人回复。
        let submission_lock = Arc::clone(&self.submission_lock);
        let _submission = match submission_lock.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                // submission lock 只保护本 actor 的提交临界区，没有可恢复的业务数据；
                // 恢复 guard 后继续做 idle cleanup，不能因锁中毒直接丢弃 sidecar 资源。
                tracing::error!(
                    scope = %self.scope_id,
                    cleanup_failure = ?CleanupFailureKind::SubmissionLock,
                    "submission lock 已中毒，恢复后继续 idle cleanup"
                );
                submission_lock.clear_poison();
                poisoned.into_inner()
            }
        };
        if !self.accepting.load(Ordering::Acquire)
            || !self.should_idle_stop()
            || self.queued_commands.load(Ordering::Acquire) != 0
        {
            return false;
        }

        self.accepting.store(false, Ordering::Release);
        let cleanup = self.cleanup_resources();
        if cleanup.is_success() {
            // 只有 cleanup 全部成功后才提交自然退出；失败时不设置该意图，
            // 让外部 shutdown 在释放提交门后仍能把重试命令送到 actor。
            self.exit_intent.store(true, Ordering::Release);
            self.exit_requested = true;
            tracing::debug!(scope = %self.scope_id, "scope actor 已按 idle 策略停止");
            true
        } else {
            tracing::error!(scope = %self.scope_id, "scope actor idle cleanup 未完整完成");
            false
        }
    }

    /// 在关闭 stdin 前尽力取消正在运行的回合，避免侧车继续执行已被 Host 放弃的 prompt。
    fn cancel_in_flight_before_shutdown(&mut self) -> CleanupResult {
        let sessions = self
            .in_flight
            .iter()
            .filter(|(_, turn)| !turn.cancelled)
            .map(|(session_id, _)| session_id.clone())
            .collect::<Vec<_>>();
        let mut result = CleanupResult::default();
        for session_id in sessions {
            // shutdown 路径不能因单个通知失败而跳过后续资源回收。
            if self
                .acp
                .notify_validated(
                    "session/cancel",
                    json!({ "sessionId": session_id }),
                    &self.policy,
                )
                .is_err()
            {
                record_cleanup_failure(
                    &self.cleanup_result,
                    &self.scope_id,
                    CleanupFailureKind::CancelNotification,
                );
                result.record(CleanupFailureKind::CancelNotification);
            }
        }
        result
    }

    /// 显式 shutdown 用于 Channel 全局换代和 Runtime Drop，并返回结构化 cleanup 结果。
    fn shutdown_and_exit(&mut self) -> CleanupResult {
        self.stop_accepting();
        if !self.cleanup_done {
            // 主动关闭也属于 ScopeDead；不要让已 accepted 的 cold resume 静默悬挂。
            self.finish_current_load(LoadOutcome::ScopeDead);
            let cancel_result = self.cancel_in_flight_before_shutdown();
            self.finish_in_flight_turns("cancelled", "回合已取消");
            // shutdown 不能等待原定时器；已失败的 terminal event 必须立即再试一次。
            self.retry_pending_terminal_events_now();
            let mut cleanup = self.cleanup_resources();
            let resources_cleaned = self.cleanup_done;
            if self.has_pending_terminal_events() {
                record_cleanup_failure(
                    &self.cleanup_result,
                    &self.scope_id,
                    CleanupFailureKind::TerminalEvent,
                );
                cleanup.record(CleanupFailureKind::TerminalEvent);
            }
            cleanup.merge(&cancel_result);
            self.reject_pending(sidecar_unavailable("sidecar 已关闭"));
            let terminal_intent_pending = !self.pending_terminal_intents.is_empty();
            self.handoff_pending_terminal_events();
            if resources_cleaned && !terminal_intent_pending {
                self.exit_intent.store(true, Ordering::Release);
                self.exit_requested = true;
            } else {
                tracing::error!(
                    scope = %self.scope_id,
                    "sidecar shutdown cleanup 未完成，保留 actor 等待后续重试"
                );
            }
            return cleanup;
        }
        // dead actor 的 cleanup 可能已完成；退出前仍必须给 pending terminal event 一次机会。
        self.retry_pending_terminal_events_now();
        let mut cleanup = self.cleanup_resources();
        if self.has_pending_terminal_events() {
            record_cleanup_failure(
                &self.cleanup_result,
                &self.scope_id,
                CleanupFailureKind::TerminalEvent,
            );
            cleanup.record(CleanupFailureKind::TerminalEvent);
        }
        let terminal_intent_pending = !self.pending_terminal_intents.is_empty();
        self.handoff_pending_terminal_events();
        if !terminal_intent_pending {
            self.exit_intent.store(true, Ordering::Release);
            self.exit_requested = true;
        } else {
            tracing::error!(
                scope = %self.scope_id,
                "terminal sequence 尚未分配，保留 actor 等待后续重试"
            );
        }
        cleanup
    }

    /// 死 actor 仍接收命令以 fail-closed；只有全局换代能创建同 scope 新代。
    fn reject_dead_command(&mut self, command: ActorCommand) {
        match command {
            ActorCommand::Shutdown { attempt } => {
                let cleanup = self.shutdown_and_exit();
                attempt.complete(cleanup);
            }
            command => send_command_error(command, sidecar_unavailable("sidecar 不可用")),
        }
    }

    /// transport 死亡时释放所有还在等待真实结果的 Kit 调用，避免同步 dispatch 超时悬挂。
    fn reject_pending(&mut self, error: KitError) {
        while let Some(command) = self.deferred.pop_front() {
            send_command_error(command, error.clone());
        }
        for (_, pending) in std::mem::take(&mut self.pending) {
            match pending {
                PendingRpc::NewSession { reply }
                | PendingRpc::ListSessions { reply }
                | PendingRpc::DeleteSession { reply, .. } => {
                    let _ = reply.send(Err(error.clone()));
                }
                PendingRpc::Load { .. } | PendingRpc::McpCatalog { .. } => {}
                PendingRpc::Prompt {
                    session_id,
                    submission_id,
                } => self.emit_turn_status(
                    &session_id,
                    &submission_id,
                    "error",
                    TURN_FAILED_USER_MESSAGE,
                ),
            }
        }
    }
}

/// 向 actor 发送命令并等待其语义规定的回执；actor 断开时不自动重启已失败 scope。
fn request_actor(
    actor: &ActorHandle,
    make: impl FnOnce(ReplySender) -> ActorCommand,
) -> Result<KitReply, KitError> {
    let (reply, receiver) = mpsc::sync_channel(1);
    actor
        .submit(make(reply))
        .map_err(|_| sidecar_unavailable("scope actor 已退出"))?;
    receiver
        .recv_timeout(DISPATCH_REPLY_TIMEOUT)
        .map_err(|_| sidecar_unavailable("等待 sidecar 回执超时"))?
}

/// Send timeout 的两种所有权结局：未写入时可撤销登记，已取得票据时必须保留幂等键。
enum SendRequestError {
    BeforePrompt(KitError),
    PromptMayHaveBeenWritten(KitError),
}

/// 发送 Send 并在调用者超时前后与 actor 原子竞争 prompt 写入票据。
fn request_send_actor(
    actor: &ActorHandle,
    session_id: String,
    submission_id: String,
    text: String,
    ticket: SendTicket,
) -> Result<KitReply, SendRequestError> {
    request_send_actor_with_timeout(
        actor,
        session_id,
        submission_id,
        text,
        ticket,
        DISPATCH_REPLY_TIMEOUT,
    )
}

/// 测试可注入回执等待上限；生产调用仍使用固定 dispatch 合同。
fn request_send_actor_with_timeout(
    actor: &ActorHandle,
    session_id: String,
    submission_id: String,
    text: String,
    ticket: SendTicket,
    reply_timeout: Duration,
) -> Result<KitReply, SendRequestError> {
    let (reply, receiver) = mpsc::sync_channel(1);
    actor
        .submit(ActorCommand::Send {
            session_id,
            submission_id,
            text,
            ticket: ticket.clone(),
            reply,
        })
        .map_err(|_| {
            ticket.mark_not_written();
            SendRequestError::BeforePrompt(sidecar_unavailable("scope actor 已退出"))
        })?;

    match receiver.recv_timeout(reply_timeout) {
        Ok(Ok(reply)) => Ok(reply),
        Ok(Err(error)) => Err(classify_send_error(&ticket, error)),
        Err(RecvTimeoutError::Timeout) | Err(RecvTimeoutError::Disconnected) => {
            let error = sidecar_unavailable("等待 sidecar 回执超时");
            if ticket.abandon() {
                // 票据先获撤销；迟到的 Send command 即使被 actor 取出也不能写 prompt。
                Err(SendRequestError::BeforePrompt(error))
            } else {
                Err(classify_send_error(&ticket, error))
            }
        }
    }
}

/// 只有 prompt 尚未写入时才允许调用方回滚 submission；其它状态必须保留幂等记录。
fn classify_send_error(ticket: &SendTicket, error: KitError) -> SendRequestError {
    match ticket.state() {
        SendTicketState::Waiting | SendTicketState::NotWritten | SendTicketState::Abandoned => {
            SendRequestError::BeforePrompt(error)
        }
        SendTicketState::Claimed
        | SendTicketState::Written
        | SendTicketState::MayHaveBeenWritten => SendRequestError::PromptMayHaveBeenWritten(error),
    }
}

/// 在 actor 异常终止或 dead 状态下统一回绝尚未完成的外部命令。
fn send_command_error(command: ActorCommand, error: KitError) {
    match command {
        ActorCommand::NewSession { reply }
        | ActorCommand::ListSessions { reply, .. }
        | ActorCommand::ResumeSession { reply, .. }
        | ActorCommand::DeleteSession { reply, .. }
        | ActorCommand::Cancel { reply, .. } => {
            let _ = reply.send(Err(error));
        }
        ActorCommand::Send { ticket, reply, .. } => {
            // command 尚未进入 prompt 写入，调用方可安全回滚该 submission。
            ticket.mark_not_written();
            let _ = reply.send(Err(error));
        }
        ActorCommand::Shutdown { attempt } => {
            let mut result = CleanupResult::default();
            result.record(CleanupFailureKind::ShutdownCommand);
            attempt.complete(result);
        }
    }
}

/// 生成 HostPolicy：所有 `_meta` 许可显式按 method 列出，不能跨 method 泄漏。
fn host_policy(launched: &LaunchedScope) -> Result<HostPolicy, KitError> {
    let cwd = std::fs::canonicalize(&launched.paths.workspace)
        .map_err(|_| sidecar_unavailable("scope workspace 不可用"))?;
    Ok(HostPolicy::new(cwd)
        .with_meta_key_for("session/new", "modelId")
        .with_meta_key_for("session/load", "modelId")
        .with_meta_key_for("session/prompt", "promptId")
        .with_model_id(ACP_BYOK_MODEL_SLOT))
}

/// 将 resolver 的失败或不可信返回统一收敛为不泄漏领域数据的客户端错误。
fn invalid_mentions_request() -> KitError {
    KitError::non_retryable(
        "invalid_request",
        "Send mention 无法解析或文本不符合安全策略",
    )
}

/// 校验产品提供的单个展示文本，补足通用 prompt 门刻意允许的正文绝对路径。
///
/// `@` 与 file URI 必须完全沿用 Task 3 的 prompt 语义，避免把不会形成文件引用的
/// 合法元数据误拒；resolver 文本仍额外禁止跨平台绝对路径形式。
fn is_safe_mention_expansion(text: &str) -> bool {
    !text.trim().is_empty()
        && validate_prompt_text(text).is_ok()
        && !contains_absolute_mention_path(text)
}

/// 检测 POSIX 根路径、Windows 根路径、UNC 和盘符路径，且不依赖当前宿主平台。
fn contains_absolute_mention_path(text: &str) -> bool {
    let bytes = text.as_bytes();
    for (offset, character) in text.char_indices() {
        let next_offset = offset + character.len_utf8();
        if matches!(character, '/' | '\\')
            && bytes
                .get(next_offset)
                .is_some_and(|next| !next.is_ascii_whitespace())
            && is_mention_path_boundary(text[..offset].chars().next_back())
        {
            return true;
        }
        if character.is_ascii_alphabetic()
            && bytes.get(offset + 1) == Some(&b':')
            && bytes
                .get(offset + 2)
                .is_some_and(|next| matches!(*next, b'/' | b'\\'))
        {
            return true;
        }
    }

    false
}

/// 路径起点只能出现在开头或文本边界，Unicode 字母/数字后的斜杠属于标题正文。
fn is_mention_path_boundary(previous: Option<char>) -> bool {
    match previous {
        None => true,
        Some(character) => !character.is_alphanumeric() && !matches!(character, '_' | '-' | '.'),
    }
}

/// 按 capability 宣告的统一上限计算 Unicode 标量字符数，而非 UTF-8 字节数。
fn prompt_text_within_limit(text: &str) -> bool {
    text.chars().count() <= MAX_PROMPT_CHARS
}

/// 在登记 submission 或拉起 sidecar 前拒绝明显无效的 Send，避免失败路径占用幂等键。
fn validate_send_input(
    scope_id: &str,
    session_id: &str,
    submission_id: &str,
    text: &str,
) -> Result<(), KitError> {
    if crate::supervisor::sanitize(scope_id).is_err()
        || session_id.is_empty()
        // Kit submission_id 与 sidecar promptId 共用 contract 的 fail-closed 边界。
        || !is_prompt_id(submission_id)
        || text.is_empty()
        || !prompt_text_within_limit(text)
        || validate_prompt_text(text).is_err()
    {
        return Err(KitError::non_retryable(
            "invalid_request",
            "Send 请求缺少有效标识或文本不符合安全策略",
        ));
    }
    Ok(())
}

/// 将 Channel 层不敏感错误转换成冻结 Kit 错误形状。
fn channel_error(error: LlmChannelError) -> KitError {
    error.as_kit_error()
}

/// sidecar 生命周期或 stdio 失败不能携带底层错误链、路径、payload 或任何凭据。
fn sidecar_unavailable(message: &str) -> KitError {
    KitError {
        code: "sidecar_unavailable".to_string(),
        message: message.to_string(),
        details: None,
        request_id: None,
        retryable: true,
        retry_after_ms: None,
    }
}

/// 只把明确的 ACP NotFound 错误码映射为 session_not_found。
fn is_session_not_found(error: &RpcError) -> bool {
    error.code == ACP_SESSION_NOT_FOUND
}

/// session/close 的 not-found 同时接受 ACP 专用码与 JSON-RPC invalid_params。
fn is_close_session_not_found(error: &RpcError) -> bool {
    error.code == ACP_SESSION_NOT_FOUND || error.code == JSON_RPC_INVALID_PARAMS
}

/// 失败 load 必须保持可观察且不泄漏 sidecar 原始错误内容。
fn load_outcome_error(outcome: LoadOutcome) -> KitError {
    match outcome {
        LoadOutcome::SessionNotFound => {
            KitError::non_retryable("session_not_found", "sidecar 未找到指定会话")
        }
        LoadOutcome::LoadError => sidecar_unavailable("sidecar 加载会话失败，可重试"),
        LoadOutcome::Timeout => sidecar_unavailable("session/load 超时，可重试"),
        LoadOutcome::Cancelled => KitError::non_retryable("cancelled", "会话恢复已取消"),
        LoadOutcome::TransportDeath | LoadOutcome::ScopeDead => {
            sidecar_unavailable("sidecar 不可用，可重试")
        }
        LoadOutcome::Success => sidecar_unavailable("session/load 状态无效"),
    }
}

/// 任何未成功的 load 都退休当前 transport，隔离无法携带 generation 的旧 replay。
fn should_retire_after_load(outcome: LoadOutcome) -> bool {
    !matches!(outcome, LoadOutcome::Success)
}

/// supervisor 不可用原因的稳定 wire 文本；仅平台分支可以进入该值。
fn unavailable_reason_name(reason: UnavailableReason) -> &'static str {
    match reason {
        UnavailableReason::SidecarHardeningUnavailable => "sidecar_hardening_unavailable",
    }
}

/// direct 与 `_x.ai/` wrapper 解码后的 permission 共享同一严格选择和回复逻辑。
fn is_permission_request(method: &str) -> bool {
    matches!(
        method,
        "session/request_permission" | "x.ai/session/request_permission"
    )
}

/// 只接受本次 permission options 明确出现的精确 optionId，不按 kind 或数组下标猜测。
fn find_option(options: Option<&Vec<Value>>, expected: &str) -> Option<String> {
    options?.iter().find_map(|option| {
        (option.get("optionId").and_then(Value::as_str) == Some(expected))
            .then(|| expected.to_string())
    })
}

/// 解析 `_x.ai/mcp/list` 的真实 `result.result.servers[]` 形状，重建完整工具名。
fn parse_catalog(result: &Value) -> Option<BTreeSet<String>> {
    let servers = result
        .get("result")
        .and_then(|result| result.get("servers"))
        .and_then(Value::as_array)?;

    // 先扫描所有 server 的原始 tool name，再应用 status/enabled 过滤；否则不可信
    // server 可用 non-ready、disabled 或异常 enabled 值伪造 Host-owned noop。
    for server in servers {
        let Some(session) = server.get("session").and_then(Value::as_object) else {
            continue;
        };
        let Some(server_tools) = session.get("tools").and_then(Value::as_array) else {
            continue;
        };
        if server_tools
            .iter()
            .any(|tool| tool.get("name").and_then(Value::as_str) == Some(NOOP_TOOL))
        {
            return None;
        }
    }

    // 该内置工具的 provenance 由 Host 本地固定策略提供，不来自任何 server 响应。
    let mut tools = BTreeSet::from([NOOP_TOOL.to_owned()]);
    for server in servers {
        let server_name = server.get("name").and_then(Value::as_str)?;
        if !is_server_name(server_name) {
            return None;
        }
        let session = server.get("session")?.as_object()?;
        if session.get("status").and_then(Value::as_str) != Some("ready") {
            continue;
        }
        let Some(server_tools) = session.get("tools").and_then(Value::as_array) else {
            continue;
        };
        for tool in server_tools {
            if tool.get("enabled").and_then(Value::as_bool) != Some(true) {
                continue;
            }
            let name = tool.get("name").and_then(Value::as_str)?;
            let qualified = format!("{server_name}__{name}");
            if !is_qualified_tool_name(&qualified) {
                return None;
            }
            tools.insert(qualified);
        }
    }
    Some(tools)
}

/// 严格检查 JSON 对象只含指定字段，防止 ACP 可选字段扩大 Host 信任边界。
fn has_exact_json_keys(object: &serde_json::Map<String, Value>, expected: &[&str]) -> bool {
    object.len() == expected.len() && expected.iter().all(|key| object.contains_key(*key))
}

/// 严格校验 sidecar 真实 initialize result 的 ACP 能力、认证与 metadata 最小闭集。
fn validate_initialize_result(result: &Value) -> bool {
    let Some(result) = result.as_object() else {
        return false;
    };

    // 顶层只接受真实 sidecar response 的四个字段，agentInfo 等可选字段也不进入本 profile。
    if !has_exact_json_keys(
        result,
        &[
            "protocolVersion",
            "agentCapabilities",
            "authMethods",
            "_meta",
        ],
    ) {
        return false;
    }
    // response 协议版本必须与 Host 发出的固定 ACP 版本一致。
    if result.get("protocolVersion").and_then(Value::as_u64) != Some(HOST_ACP_PROTOCOL_VERSION) {
        return false;
    }

    // runtime metadata 是固定握手身份，且不允许夹带未知键。
    let Some(meta) = result.get("_meta").and_then(Value::as_object) else {
        return false;
    };
    if !has_exact_json_keys(
        meta,
        &[
            "efflabRuntime",
            "efflabSchemaVersion",
            "efflabSessionStoreVersion",
        ],
    ) || meta.get("efflabRuntime").and_then(Value::as_str) != Some(EFFLAB_RUNTIME_ID)
        || meta.get("efflabSchemaVersion").and_then(Value::as_u64) != Some(EFFLAB_SCHEMA_VERSION)
        || meta
            .get("efflabSessionStoreVersion")
            .and_then(Value::as_u64)
            != Some(EFFLAB_SESSION_STORE_VERSION)
    {
        return false;
    }

    // 只接受 sidecar 实际广告的能力字段；fs、terminal、logout 和未知字段均被排除。
    let Some(agent_capabilities) = result.get("agentCapabilities").and_then(Value::as_object)
    else {
        return false;
    };
    if !has_exact_json_keys(
        agent_capabilities,
        &[
            "loadSession",
            "promptCapabilities",
            "mcpCapabilities",
            "sessionCapabilities",
            "auth",
        ],
    ) || agent_capabilities
        .get("loadSession")
        .and_then(Value::as_bool)
        != Some(true)
    {
        return false;
    }

    // sidecar 不接收图片、音频或 embedded context，三个字段必须显式为 false。
    let Some(prompt_capabilities) = agent_capabilities
        .get("promptCapabilities")
        .and_then(Value::as_object)
    else {
        return false;
    };
    if !has_exact_json_keys(prompt_capabilities, &["image", "audio", "embeddedContext"])
        || prompt_capabilities.get("image").and_then(Value::as_bool) != Some(false)
        || prompt_capabilities.get("audio").and_then(Value::as_bool) != Some(false)
        || prompt_capabilities
            .get("embeddedContext")
            .and_then(Value::as_bool)
            != Some(false)
    {
        return false;
    }

    // sidecar 不提供 HTTP/SSE MCP transport，不能接受其它 mcp capability 字段。
    let Some(mcp_capabilities) = agent_capabilities
        .get("mcpCapabilities")
        .and_then(Value::as_object)
    else {
        return false;
    };
    if !has_exact_json_keys(mcp_capabilities, &["http", "sse"])
        || mcp_capabilities.get("http").and_then(Value::as_bool) != Some(false)
        || mcp_capabilities.get("sse").and_then(Value::as_bool) != Some(false)
    {
        return false;
    }

    // session/list 能力只以空对象表示，不能借能力字段开放其它 session 方法。
    let Some(session_capabilities) = agent_capabilities
        .get("sessionCapabilities")
        .and_then(Value::as_object)
    else {
        return false;
    };
    let Some(session_list) = session_capabilities.get("list").and_then(Value::as_object) else {
        return false;
    };
    if !has_exact_json_keys(session_capabilities, &["list"]) || !session_list.is_empty() {
        return false;
    }

    // unstable auth 容器可由 schema 序列化为空对象，但绝不能广告 logout 或其它字段。
    let Some(auth_capabilities) = agent_capabilities.get("auth").and_then(Value::as_object) else {
        return false;
    };
    if !auth_capabilities.is_empty() {
        return false;
    }

    // 当前 sidecar 不提供认证入口；非空、缺失或非数组都必须拒绝。
    result
        .get("authMethods")
        .and_then(Value::as_array)
        .is_some_and(Vec::is_empty)
}

/// 标准立即 Send 回执，turn_id 与 submission_id 按 L1 协议恒等。
fn send_reply(session_id: &str, submission_id: &str, duplicate: bool) -> KitReply {
    KitReply::Send {
        accepted: true,
        duplicate,
        session_id: session_id.to_string(),
        turn_id: submission_id.to_string(),
        submission_id: submission_id.to_string(),
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use std::sync::mpsc;

    use anyhow::Result;

    use super::*;
    use crate::app_port::{
        ApprovedMcpSpec, HostApp, LlmChannelConfig, LlmSecretSlot, SealedSecret, SecretGuard,
    };
    use crate::config::L3bRuntimeConfig;
    use crate::event_sink::KitEventSink;

    #[test]
    fn turn_failure_user_message_hides_sidecar_and_maps_model_error() {
        assert_eq!(
            turn_failure_user_message("turn_model_error"),
            "模型没有返回有效回复，请重试"
        );
        assert_eq!(turn_failure_user_message("unknown"), "回复未完成，请重试");
        assert!(
            !turn_failure_user_message("turn_model_error").contains("sidecar"),
            "用户提示不得出现 sidecar"
        );
        assert!(
            !TURN_FAILED_USER_MESSAGE.contains("sidecar"),
            "默认失败提示不得出现 sidecar"
        );
    }

    struct LifecycleTestApp;

    impl HostApp for LifecycleTestApp {
        fn app_id(&self) -> &str {
            "runtime-lifecycle-test"
        }

        fn persist_llm_channel(&self, _config: &LlmChannelConfig) -> Result<()> {
            Ok(())
        }

        fn load_llm_channel(&self) -> Result<LlmChannelConfig> {
            Ok(LlmChannelConfig::Byok {
                base_url: "https://8.8.8.8/v1".to_string(),
                model_id: "runtime-lifecycle-test-model".to_string(),
                api_key: SealedSecret::new(b"test-key".to_vec()),
            })
        }

        fn seal_secret(&self, plain: &[u8]) -> Result<SealedSecret> {
            Ok(SealedSecret::new(plain.to_vec()))
        }

        fn unseal_secret(&self, sealed: &SealedSecret) -> Result<SecretGuard> {
            Ok(SecretGuard::new(sealed.as_bytes().to_vec()))
        }

        fn seal_llm_secret(&self, _slot: LlmSecretSlot, plain: &[u8]) -> Result<SealedSecret> {
            self.seal_secret(plain)
        }

        fn unseal_llm_secret(
            &self,
            _slot: LlmSecretSlot,
            sealed: &SealedSecret,
        ) -> Result<SecretGuard> {
            self.unseal_secret(sealed)
        }

        fn mcp_for_scope(&self, _scope: &ScopeId) -> Result<ApprovedMcpSpec> {
            Ok(ApprovedMcpSpec::default())
        }
    }

    struct NoopSink;

    impl KitEventSink for NoopSink {
        fn emit(&self, _event: KitProductEvent) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn parse_catalog_rejects_invalid_qualified_tool_names() {
        for (server_name, tool_name) in [
            ("bad__server", "search"),
            ("server", "bad.name"),
            ("server", "bad name"),
            ("server", "1search"),
        ] {
            let result = serde_json::json!({
                "result": {
                    "servers": [{
                        "name": server_name,
                        "session": {
                            "status": "ready",
                            "tools": [{"name": tool_name, "enabled": true}]
                        }
                    }]
                }
            });
            assert!(
                parse_catalog(&result).is_none(),
                "非法 catalog qualified tool name 必须 fail-closed: {server_name}__{tool_name}"
            );
        }
    }

    #[test]
    fn parse_catalog_adds_host_owned_noop_and_rejects_server_owned_copy() {
        let empty_catalog = serde_json::json!({
            "result": {"servers": []}
        });
        assert_eq!(
            parse_catalog(&empty_catalog),
            Some(BTreeSet::from([NOOP_TOOL.to_owned()])),
            "固定 noop 必须由 Host 解析器注入，而不是来自 server catalog"
        );

        let server_owned_noop = serde_json::json!({
            "result": {
                "servers": [{
                    "name": "builtin",
                    "session": {
                        "status": "ready",
                        "tools": [{"name": NOOP_TOOL, "enabled": true}]
                    }
                }]
            }
        });
        assert_eq!(
            parse_catalog(&server_owned_noop),
            None,
            "任意 server 携带 bare noop 都不得取得内置工具 provenance"
        );
    }

    #[test]
    fn parse_catalog_rejects_server_owned_noop_before_status_and_enabled_filters() {
        let cases = [
            (
                "non-ready server",
                serde_json::json!({
                    "status": "starting",
                    "tools": [{"name": NOOP_TOOL, "enabled": true}]
                }),
            ),
            (
                "disabled tool",
                serde_json::json!({
                    "status": "ready",
                    "tools": [{"name": NOOP_TOOL, "enabled": false}]
                }),
            ),
            (
                "missing enabled",
                serde_json::json!({
                    "status": "ready",
                    "tools": [{"name": NOOP_TOOL}]
                }),
            ),
            (
                "non-boolean enabled",
                serde_json::json!({
                    "status": "ready",
                    "tools": [{"name": NOOP_TOOL, "enabled": "true"}]
                }),
            ),
            (
                "valid tool before noop",
                serde_json::json!({
                    "status": "ready",
                    "tools": [
                        {"name": "before", "enabled": true},
                        {"name": NOOP_TOOL, "enabled": true}
                    ]
                }),
            ),
            (
                "valid tool after noop",
                serde_json::json!({
                    "status": "ready",
                    "tools": [
                        {"name": NOOP_TOOL, "enabled": true},
                        {"name": "after", "enabled": true}
                    ]
                }),
            ),
        ];

        for (case_name, session) in cases {
            let catalog = serde_json::json!({
                "result": {
                    "servers": [{"name": "untrusted", "session": session}]
                }
            });
            assert_eq!(
                parse_catalog(&catalog),
                None,
                "{case_name} 中的 server-owned noop 必须 fail-closed"
            );
        }
    }

    #[test]
    fn parse_catalog_injects_host_noop_and_filters_other_tools() {
        let catalog = serde_json::json!({
            "result": {
                "servers": [
                    {
                        "name": "starting-server",
                        "session": {
                            "status": "starting",
                            "tools": [{"name": "ignored", "enabled": true}]
                        }
                    },
                    {
                        "name": "mcp",
                        "session": {
                            "status": "ready",
                            "tools": [
                                {"name": "disabled", "enabled": false},
                                {"name": "missing_enabled"},
                                {"name": "non_boolean", "enabled": "true"},
                                {"name": "before", "enabled": true},
                                {"name": "after", "enabled": true}
                            ]
                        }
                    }
                ]
            }
        });

        assert_eq!(
            parse_catalog(&catalog),
            Some(BTreeSet::from([
                NOOP_TOOL.to_owned(),
                "mcp__after".to_owned(),
                "mcp__before".to_owned(),
            ])),
            "Host noop 必须本地注入，且仅保留 ready 且 enabled=true 的合法工具"
        );
    }

    /// 创建只响应 initialize 的 fake sidecar，使测试只观察 Host actor 的替换顺序。
    fn write_fake_sidecar(path: &Path) {
        let script = r#"#!/bin/sh
while IFS= read -r line; do
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id"
      ;;
  esac
done
"#;
        fs::write(path, script).expect("必须能写入 lifecycle fake sidecar");
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .expect("lifecycle fake sidecar 必须可执行");
    }

    /// 已成功入队的 command 必须阻止 idle close，并最终只交付一次回执。
    #[test]
    fn queued_command_prevents_idle_close_and_gets_one_reply() {
        let temporary = tempfile::tempdir().expect("必须能创建 idle race 测试目录");
        let config = crate::HostRuntimeConfig {
            home_root: temporary.path().join("app-data"),
            sidecar_bin: temporary.path().join("unused-sidecar"),
            sidecar_log_path: temporary.path().join("sidecar.log"),
            mcp_exec_root: temporary.path().join("mcp"),
            idle_after: Duration::from_millis(1),
            l3b: L3bRuntimeConfig::default(),
            system_prompt: String::new(),
        };
        let runtime = HostRuntime::new(LifecycleTestApp, NoopSink, config);
        let service = runtime
            .channel_service()
            .expect("测试 actor 必须取得 Channel service");
        let (_stdout_peer, stdout) =
            std::os::unix::net::UnixStream::pair().expect("idle race 测试必须创建 stdout pipe");
        let acp = AcpRuntime::new(std::io::sink(), stdout);
        let (sender, receiver) = mpsc::channel();
        let accepting = Arc::new(AtomicBool::new(true));
        let exit_intent = Arc::new(AtomicBool::new(false));
        let submission_lock = Arc::new(Mutex::new(()));
        let queued_commands = Arc::new(AtomicUsize::new(0));
        let restart_blocked = Arc::new(AtomicBool::new(false));
        let cleanup_result = Arc::new(Mutex::new(CleanupResult::default()));
        let mut actor = ScopeActor::new(
            "scope-a".to_string(),
            acp,
            HostPolicy::new(temporary.path()),
            service,
            Arc::new(NoopSink),
            receiver,
            Arc::clone(&accepting),
            Arc::clone(&exit_intent),
            Arc::clone(&submission_lock),
            Arc::clone(&queued_commands),
            Arc::clone(&restart_blocked),
            Arc::clone(&cleanup_result),
            Arc::clone(&runtime.terminal_outbox),
            1,
            Duration::from_millis(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
            ApprovedMcpSpec::default(),
        );
        actor.initialized = true;
        actor.last_activity = Instant::now() - Duration::from_secs(1);

        let handle = ActorHandle {
            scope_id: "scope-a".to_string(),
            sender,
            accepting: Arc::clone(&accepting),
            exit_intent: Arc::clone(&exit_intent),
            submission_lock,
            queued_commands: Arc::clone(&queued_commands),
            restart_blocked: Arc::clone(&restart_blocked),
            shutdown_submitted: AtomicBool::new(false),
            shutdown_attempt: Mutex::new(None),
            cleanup_result,
            join: Mutex::new(None),
            finished: Arc::new(AtomicBool::new(false)),
        };
        let (reply, reply_receiver) = mpsc::sync_channel(1);
        assert!(
            handle
                .submit(ActorCommand::Cancel {
                    session_id: "session-a".to_string(),
                    reply,
                })
                .is_ok(),
            "临界区内成功入队的 command 不得被拒绝"
        );

        assert!(
            !actor.try_idle_stop_and_exit(),
            "idle close 不得丢弃已成功入队的 command"
        );
        assert!(accepting.load(Ordering::Acquire));

        let command = actor
            .receiver
            .try_recv()
            .expect("已接受 command 必须仍可由 actor 消费");
        assert!(!actor.handle_command(command));
        assert_eq!(
            reply_receiver
                .recv_timeout(Duration::from_secs(1))
                .expect("已接受 command 必须交付回执")
                .expect("Cancel 回执必须成功"),
            KitReply::Cancel { accepted: true }
        );
        assert!(
            reply_receiver.try_recv().is_err(),
            "同一 command 不得交付第二次回执"
        );
    }

    /// 旧 actor 必须在 replacement actor 返回前完成 join，避免其迟到 cleanup 触碰新 generation。
    #[test]
    fn actor_replacement_joins_removed_actor_before_spawn_returns() {
        let temporary = tempfile::tempdir().expect("必须能创建 lifecycle 测试目录");
        let sidecar = temporary.path().join("fake-sidecar.sh");
        write_fake_sidecar(&sidecar);
        let runtime = HostRuntime::new(
            LifecycleTestApp,
            NoopSink,
            crate::HostRuntimeConfig {
                home_root: temporary.path().join("app-data"),
                sidecar_bin: sidecar,
                sidecar_log_path: temporary.path().join("sidecar.log"),
                mcp_exec_root: temporary.path().join("mcp"),
                idle_after: Duration::from_secs(60),
                l3b: L3bRuntimeConfig::default(),
                system_prompt: String::new(),
            },
        );

        let (sender, receiver) = mpsc::channel();
        let (cleanup_seen, cleanup_observed) = mpsc::sync_channel(1);
        let old_actor_thread = thread::spawn(move || match receiver.recv() {
            Ok(ActorCommand::Shutdown { attempt }) => {
                cleanup_seen
                    .send("shutdown")
                    .expect("测试必须观察到旧 actor shutdown");
                attempt.complete(CleanupResult::default());
            }
            Err(_) => {
                cleanup_seen
                    .send("disconnect")
                    .expect("测试必须观察到旧 actor disconnect cleanup");
            }
            Ok(_) => panic!("旧 actor 测试线程只接受 shutdown"),
        });
        let held_sender = sender.clone();
        let old_handle = Arc::new(ActorHandle {
            scope_id: "scope-a".to_string(),
            sender,
            accepting: Arc::new(AtomicBool::new(false)),
            exit_intent: Arc::new(AtomicBool::new(false)),
            submission_lock: Arc::new(Mutex::new(())),
            queued_commands: Arc::new(AtomicUsize::new(0)),
            restart_blocked: Arc::new(AtomicBool::new(false)),
            shutdown_submitted: AtomicBool::new(false),
            shutdown_attempt: Mutex::new(None),
            cleanup_result: Arc::new(Mutex::new(CleanupResult::default())),
            join: Mutex::new(Some(old_actor_thread)),
            finished: Arc::new(AtomicBool::new(false)),
        });
        runtime
            .actors
            .lock()
            .expect("测试 actor map 锁必须可用")
            .insert("scope-a".to_string(), old_handle);

        let replacement = runtime
            .actor_for_scope("scope-a")
            .expect("replacement actor 必须可启动");
        assert_eq!(
            cleanup_observed
                .recv_timeout(Duration::from_secs(1))
                .expect("旧 actor cleanup 必须先于 replacement 返回被观察到"),
            "shutdown",
            "被移除的旧 actor 必须通过 shutdown 路径结束，而不是等 sender 断开"
        );
        drop(held_sender);
        replacement.shutdown();
    }

    /// shutdown 取得提交门后，迟到 command 不得排在 Shutdown 之后进入无人消费队列。
    #[test]
    fn actor_shutdown_rejects_late_command_submission() {
        let (sender, receiver) = mpsc::channel();
        let (shutdown_seen, shutdown_observed) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel(1);
        let actor_thread = thread::spawn(move || match receiver.recv() {
            Ok(ActorCommand::Shutdown { attempt }) => {
                attempt.complete(CleanupResult::default());
                shutdown_seen
                    .send(())
                    .expect("测试必须观察到 shutdown 已提交");
                release_receiver.recv().expect("测试必须释放 actor thread");
            }
            Ok(_) => panic!("测试 actor 只接受 shutdown"),
            Err(_) => panic!("测试 actor 不应先收到 sender 断开"),
        });
        let handle = Arc::new(ActorHandle {
            scope_id: "scope-a".to_string(),
            sender,
            accepting: Arc::new(AtomicBool::new(true)),
            exit_intent: Arc::new(AtomicBool::new(false)),
            submission_lock: Arc::new(Mutex::new(())),
            queued_commands: Arc::new(AtomicUsize::new(0)),
            restart_blocked: Arc::new(AtomicBool::new(false)),
            shutdown_submitted: AtomicBool::new(false),
            shutdown_attempt: Mutex::new(None),
            cleanup_result: Arc::new(Mutex::new(CleanupResult::default())),
            join: Mutex::new(Some(actor_thread)),
            finished: Arc::new(AtomicBool::new(false)),
        });
        let shutdown_handle = Arc::clone(&handle);
        let (shutdown_done, shutdown_result) = mpsc::sync_channel(1);
        thread::spawn(move || {
            shutdown_done
                .send(shutdown_handle.shutdown())
                .expect("测试必须交付 shutdown 结果");
        });

        shutdown_observed
            .recv_timeout(Duration::from_secs(1))
            .expect("测试必须先观察到 shutdown 已进入 actor");
        let (reply, _reply_receiver) = mpsc::sync_channel(1);
        assert!(
            handle.submit(ActorCommand::NewSession { reply }).is_err(),
            "Shutdown 后的迟到 command 不得成功提交"
        );
        release_sender
            .send(())
            .expect("测试必须释放等待中的 shutdown actor");
        let cleanup = shutdown_result
            .recv_timeout(Duration::from_secs(1))
            .expect("测试 shutdown 必须完成");
        assert!(cleanup.is_success());
    }

    /// idle actor 已返回但尚未发布 finished 时，外部 shutdown 不得遗留无人消费的队列计数。
    #[test]
    fn idle_exit_and_shutdown_interleave_does_not_leak_shutdown_command() {
        let temporary = tempfile::tempdir().expect("必须能创建 idle shutdown 交错测试目录");
        let config = crate::HostRuntimeConfig {
            home_root: temporary.path().join("app-data"),
            sidecar_bin: temporary.path().join("unused-sidecar"),
            sidecar_log_path: temporary.path().join("sidecar.log"),
            mcp_exec_root: temporary.path().join("mcp"),
            idle_after: Duration::from_millis(1),
            l3b: L3bRuntimeConfig::default(),
            system_prompt: String::new(),
        };
        let runtime = HostRuntime::new(LifecycleTestApp, NoopSink, config);
        let service = runtime
            .channel_service()
            .expect("测试 actor 必须取得 Channel service");
        let (_stdout_peer, stdout) =
            std::os::unix::net::UnixStream::pair().expect("idle shutdown 测试必须创建 stdout pipe");
        let acp = AcpRuntime::new(std::io::sink(), stdout);
        let (sender, receiver) = mpsc::channel();
        let accepting = Arc::new(AtomicBool::new(true));
        let exit_intent = Arc::new(AtomicBool::new(false));
        let submission_lock = Arc::new(Mutex::new(()));
        let queued_commands = Arc::new(AtomicUsize::new(0));
        let restart_blocked = Arc::new(AtomicBool::new(false));
        let cleanup_result = Arc::new(Mutex::new(CleanupResult::default()));
        let finished = Arc::new(AtomicBool::new(false));
        let mut actor = ScopeActor::new(
            "scope-a".to_string(),
            acp,
            HostPolicy::new(temporary.path()),
            service,
            Arc::new(NoopSink),
            receiver,
            Arc::clone(&accepting),
            Arc::clone(&exit_intent),
            Arc::clone(&submission_lock),
            Arc::clone(&queued_commands),
            Arc::clone(&restart_blocked),
            Arc::clone(&cleanup_result),
            Arc::clone(&runtime.terminal_outbox),
            1,
            Duration::from_millis(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
            ApprovedMcpSpec::default(),
        );
        actor.initialized = true;
        actor.last_activity = Instant::now() - Duration::from_secs(1);

        let idle_returned = Arc::new(std::sync::Barrier::new(2));
        let (release_sender, release_receiver) = mpsc::sync_channel(1);
        let actor_finished = Arc::clone(&finished);
        let actor_idle_returned = Arc::clone(&idle_returned);
        let actor_thread = thread::spawn(move || {
            actor.run();
            // 故意延迟 finished 发布，固定复现自然退出与外部 shutdown 的交错窗口。
            actor_idle_returned.wait();
            release_receiver
                .recv_timeout(Duration::from_secs(1))
                .expect("测试必须释放已自然退出的 actor");
            // 旧实现会把 Shutdown 发到已返回的 actor；这里只取走该消息以唤醒
            // shutdown waiter，但不执行 actor 的队列计数扣减，模拟无人消费的遗留项。
            if let Ok(ActorCommand::Shutdown { attempt }) =
                actor.receiver.recv_timeout(Duration::from_millis(500))
            {
                attempt.complete(CleanupResult::default());
            }
            actor_finished.store(true, Ordering::Release);
        });

        idle_returned.wait();
        assert!(!accepting.load(Ordering::Acquire));
        let handle = Arc::new(ActorHandle {
            scope_id: "scope-a".to_string(),
            sender,
            accepting: Arc::clone(&accepting),
            exit_intent: Arc::clone(&exit_intent),
            submission_lock: Arc::clone(&submission_lock),
            queued_commands: Arc::clone(&queued_commands),
            restart_blocked: Arc::clone(&restart_blocked),
            shutdown_submitted: AtomicBool::new(false),
            shutdown_attempt: Mutex::new(None),
            cleanup_result,
            join: Mutex::new(None),
            finished,
        });

        // 让 shutdown 先等待提交门，再以 barrier 释放 actor，确保交错顺序稳定。
        let submission_guard = handle.submission_lock.lock().expect("测试提交门必须可用");
        let shutdown_handle = Arc::clone(&handle);
        let (shutdown_done, shutdown_result) = mpsc::sync_channel(1);
        let shutdown_thread = thread::spawn(move || {
            let result = shutdown_handle.shutdown();
            shutdown_done
                .send(result)
                .expect("测试必须交付 shutdown 结果");
        });
        drop(submission_guard);
        for _ in 0..1000 {
            if handle.shutdown_submitted.load(Ordering::Acquire) {
                break;
            }
            thread::yield_now();
        }
        assert!(
            handle.shutdown_submitted.load(Ordering::Acquire),
            "测试必须观察到 shutdown 已完成原子状态转换"
        );
        release_sender.send(()).expect("测试必须释放自然退出 actor");
        let _cleanup = shutdown_result
            .recv_timeout(Duration::from_secs(1))
            .expect("shutdown 必须在交错测试中完成");
        shutdown_thread
            .join()
            .expect("测试 shutdown 线程必须正常退出");
        actor_thread
            .join()
            .expect("测试自然退出 actor 线程必须正常退出");
        assert_eq!(
            queued_commands.load(Ordering::Acquire),
            0,
            "自然退出后的 shutdown 不得留下无人消费的 queued command"
        );
    }

    /// submission lock 中毒时，idle 退出仍必须执行资源 cleanup，不能直接丢弃 actor。
    #[test]
    fn poisoned_submission_lock_does_not_skip_idle_cleanup() {
        let temporary = tempfile::tempdir().expect("必须能创建 submission lock 测试目录");
        let config = crate::HostRuntimeConfig {
            home_root: temporary.path().join("app-data"),
            sidecar_bin: temporary.path().join("unused-sidecar"),
            sidecar_log_path: temporary.path().join("sidecar.log"),
            mcp_exec_root: temporary.path().join("mcp"),
            idle_after: Duration::from_millis(1),
            l3b: L3bRuntimeConfig::default(),
            system_prompt: String::new(),
        };
        let runtime = HostRuntime::new(LifecycleTestApp, NoopSink, config);
        let service = runtime
            .channel_service()
            .expect("测试 actor 必须取得 Channel service");
        let (_stdout_peer, stdout) = std::os::unix::net::UnixStream::pair()
            .expect("submission lock 测试必须创建 stdout pipe");
        let acp = AcpRuntime::new(std::io::sink(), stdout);
        let (_sender, receiver) = mpsc::channel();
        let accepting = Arc::new(AtomicBool::new(true));
        let exit_intent = Arc::new(AtomicBool::new(false));
        let submission_lock = Arc::new(Mutex::new(()));
        let poison_lock = Arc::clone(&submission_lock);
        thread::spawn(move || {
            let _guard = poison_lock.lock().expect("测试必须先取得 submission lock");
            panic!("测试故意让 submission lock 中毒");
        })
        .join()
        .expect_err("测试必须制造 poisoned submission lock");
        let queued_commands = Arc::new(AtomicUsize::new(0));
        let restart_blocked = Arc::new(AtomicBool::new(false));
        let cleanup_result = Arc::new(Mutex::new(CleanupResult::default()));
        let mut actor = ScopeActor::new(
            "scope-a".to_string(),
            acp,
            HostPolicy::new(temporary.path()),
            service,
            Arc::new(NoopSink),
            receiver,
            Arc::clone(&accepting),
            Arc::clone(&exit_intent),
            submission_lock,
            Arc::clone(&queued_commands),
            Arc::clone(&restart_blocked),
            cleanup_result,
            Arc::new(Mutex::new(TerminalOutbox::default())),
            1,
            Duration::from_millis(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
            ApprovedMcpSpec::default(),
        );
        actor.initialized = true;
        actor.last_activity = Instant::now() - Duration::from_secs(1);

        assert!(actor.try_idle_stop_and_exit());
        assert!(actor.cleanup_done, "poisoned lock 不得跳过 idle cleanup");
        assert!(actor.acp_shutdown_done, "idle cleanup 必须关闭 ACP runtime");
        assert!(
            actor.scope_stop_done,
            "idle cleanup 必须停止 Supervisor scope"
        );
        assert!(exit_intent.load(Ordering::Acquire));
        assert_eq!(queued_commands.load(Ordering::Acquire), 0);
    }

    /// unsupported reverse request 的回复写入失败时，actor 必须进入 dead 并执行 cleanup，而不能吞掉错误。
    #[test]
    fn unsupported_reverse_reply_write_failure_marks_actor_dead() {
        struct FailingWriter;

        impl std::io::Write for FailingWriter {
            fn write(&mut self, _buffer: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "测试故意让 reverse reply 写入失败",
                ))
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let temporary = tempfile::tempdir().expect("必须能创建 reverse reply 测试目录");
        let config = crate::HostRuntimeConfig {
            home_root: temporary.path().join("app-data"),
            sidecar_bin: temporary.path().join("unused-sidecar"),
            sidecar_log_path: temporary.path().join("sidecar.log"),
            mcp_exec_root: temporary.path().join("mcp"),
            idle_after: Duration::from_secs(60),
            l3b: L3bRuntimeConfig::default(),
            system_prompt: String::new(),
        };
        let runtime = HostRuntime::new(LifecycleTestApp, NoopSink, config);
        let service = runtime
            .channel_service()
            .expect("测试 actor 必须取得 Channel service");
        let (mut stdout_peer, stdout) =
            std::os::unix::net::UnixStream::pair().expect("reverse reply 测试必须创建 stdout pipe");
        let acp = AcpRuntime::new(FailingWriter, stdout);
        let (_sender, receiver) = mpsc::channel();
        let accepting = Arc::new(AtomicBool::new(true));
        let exit_intent = Arc::new(AtomicBool::new(false));
        let submission_lock = Arc::new(Mutex::new(()));
        let queued_commands = Arc::new(AtomicUsize::new(0));
        let restart_blocked = Arc::new(AtomicBool::new(false));
        let cleanup_result = Arc::new(Mutex::new(CleanupResult::default()));
        let mut actor = ScopeActor::new(
            "scope-a".to_string(),
            acp,
            HostPolicy::new(temporary.path()),
            service,
            Arc::new(NoopSink),
            receiver,
            Arc::clone(&accepting),
            Arc::clone(&exit_intent),
            Arc::clone(&submission_lock),
            Arc::clone(&queued_commands),
            Arc::clone(&restart_blocked),
            cleanup_result,
            Arc::new(Mutex::new(TerminalOutbox::default())),
            1,
            Duration::from_secs(60),
            Duration::from_secs(1),
            Duration::from_secs(1),
            ApprovedMcpSpec::default(),
        );

        std::io::Write::write_all(
            &mut stdout_peer,
            br#"{"jsonrpc":"2.0","id":7,"method":"_x.ai/ask_user_question","params":{"sessionId":"session-a"}}"#,
        )
        .and_then(|_| std::io::Write::write_all(&mut stdout_peer, b"\n"))
        .expect("测试必须能注入 unsupported reverse request");
        let (request_id, method, params) = loop {
            match actor
                .acp
                .poll_inbound()
                .expect("测试 reverse request 读取必须成功")
            {
                Some(Inbound::Request { id, method, params }) => break (id, method, params),
                Some(_) | None => thread::sleep(Duration::from_millis(1)),
            }
        };

        actor.handle_reverse_request(request_id, &method, &params);
        assert!(
            actor.dead,
            "unsupported reverse reply 写入失败必须杀停 actor"
        );
        assert!(
            !accepting.load(Ordering::Acquire),
            "reply 写入失败后不得继续接受新的 scope command"
        );
        assert!(actor.cleanup_done, "reply 写入失败后必须完成 scope cleanup");
        assert!(
            actor.acp_shutdown_done,
            "reply 写入失败后必须关闭 ACP runtime"
        );
        assert!(
            actor.scope_stop_done,
            "reply 写入失败后必须停止 Supervisor scope"
        );
        drop(stdout_peer);
    }

    /// catalog 请求写失败时 scope 必须 fail-closed；已成功创建的 session 回执仍保持真实结果。
    #[test]
    fn mcp_catalog_write_failure_stops_scope_without_lying_about_new_session() {
        struct FailingWriter;

        impl std::io::Write for FailingWriter {
            fn write(&mut self, _buffer: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "测试故意让 MCP catalog 写入失败",
                ))
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let temporary = tempfile::tempdir().expect("必须能创建 MCP 写失败测试目录");
        let config = crate::HostRuntimeConfig {
            home_root: temporary.path().join("app-data"),
            sidecar_bin: temporary.path().join("unused-sidecar"),
            sidecar_log_path: temporary.path().join("sidecar.log"),
            mcp_exec_root: temporary.path().join("mcp"),
            idle_after: Duration::from_secs(60),
            l3b: L3bRuntimeConfig::default(),
            system_prompt: String::new(),
        };
        let runtime = HostRuntime::new(LifecycleTestApp, NoopSink, config);
        let service = runtime
            .channel_service()
            .expect("测试 actor 必须取得 Channel service");
        let (_stdout_peer, stdout) =
            std::os::unix::net::UnixStream::pair().expect("MCP 写失败测试必须创建 stdout pipe");
        let acp = AcpRuntime::new(FailingWriter, stdout);
        let (_sender, receiver) = mpsc::channel();
        let accepting = Arc::new(AtomicBool::new(true));
        let exit_intent = Arc::new(AtomicBool::new(false));
        let submission_lock = Arc::new(Mutex::new(()));
        let queued_commands = Arc::new(AtomicUsize::new(0));
        let restart_blocked = Arc::new(AtomicBool::new(false));
        let cleanup_result = Arc::new(Mutex::new(CleanupResult::default()));
        let mut actor = ScopeActor::new(
            "scope-a".to_string(),
            acp,
            HostPolicy::new(temporary.path()),
            service,
            Arc::new(NoopSink),
            receiver,
            Arc::clone(&accepting),
            Arc::clone(&exit_intent),
            Arc::clone(&submission_lock),
            Arc::clone(&queued_commands),
            Arc::clone(&restart_blocked),
            cleanup_result,
            Arc::new(Mutex::new(TerminalOutbox::default())),
            1,
            Duration::from_secs(60),
            Duration::from_secs(1),
            Duration::from_secs(1),
            ApprovedMcpSpec::with_expected_tools(["purelab__search_tracks".to_string()]),
        );
        actor.initialized = true;
        let (reply, reply_receiver) = mpsc::sync_channel(1);

        actor.finish_new_session(reply, Ok(json!({ "sessionId": "session-a" })));

        assert_eq!(
            reply_receiver
                .recv_timeout(Duration::from_secs(1))
                .expect("session/new 成功结果必须仍交付给调用方")
                .expect("session/new 已成功创建的结果不得被 MCP 可选探测改写"),
            KitReply::NewSession {
                session_id: "session-a".to_string()
            }
        );
        assert!(actor.dead, "MCP catalog 写失败必须终止不可信 scope");
        assert!(
            !accepting.load(Ordering::Acquire),
            "MCP catalog 写失败后不得继续接受 scope command"
        );
        assert!(
            actor.pending.is_empty(),
            "写失败 cleanup 不得遗留 ACP pending"
        );
        assert!(
            actor.catalog_pending.is_empty(),
            "MCP catalog 写失败不得登记未发送成功的 pending"
        );
    }

    /// 构造只用于 request_send_actor 单元测试的外部 actor 句柄。
    fn test_actor_handle(sender: Sender<ActorCommand>) -> ActorHandle {
        ActorHandle {
            scope_id: "scope-a".to_string(),
            sender,
            accepting: Arc::new(AtomicBool::new(true)),
            exit_intent: Arc::new(AtomicBool::new(false)),
            submission_lock: Arc::new(Mutex::new(())),
            queued_commands: Arc::new(AtomicUsize::new(0)),
            restart_blocked: Arc::new(AtomicBool::new(false)),
            shutdown_submitted: AtomicBool::new(false),
            shutdown_attempt: Mutex::new(None),
            cleanup_result: Arc::new(Mutex::new(CleanupResult::default())),
            join: Mutex::new(None),
            finished: Arc::new(AtomicBool::new(false)),
        }
    }

    /// 调用方先超时且 actor 尚未 claim 时，submission 必须可安全回滚并再次登记。
    #[test]
    fn send_timeout_before_actor_claim_allows_submission_retry() {
        let (sender, receiver) = mpsc::channel();
        let gate = Arc::new(std::sync::Barrier::new(2));
        let worker_gate = Arc::clone(&gate);
        let worker = thread::spawn(move || {
            worker_gate.wait();
            let command = receiver
                .recv()
                .expect("测试 actor 必须收到已提交 Send command");
            if let ActorCommand::Send { ticket, .. } = command {
                assert_eq!(ticket.state(), SendTicketState::Abandoned);
                assert!(
                    !ticket.claim_for_prompt(),
                    "调用方已 abandon 的 Send 不得再取得 prompt 写入权"
                );
            } else {
                panic!("测试 actor 只接受 Send command");
            }
        });
        let handle = test_actor_handle(sender);
        let mut submissions = SubmissionMap::default();
        let ticket = match submissions.record("scope-a", "session-a", "submission-a", "hello", &[])
        {
            SubmissionDecision::Accepted { ticket, .. } => ticket,
            other => panic!("首次 submission 必须被接受，实际为 {other:?}"),
        };

        let result = request_send_actor_with_timeout(
            &handle,
            "session-a".to_string(),
            "submission-a".to_string(),
            "hello".to_string(),
            ticket.clone(),
            Duration::from_millis(10),
        );
        assert!(matches!(result, Err(SendRequestError::BeforePrompt(_))));
        assert_eq!(ticket.state(), SendTicketState::Abandoned);
        submissions.forget("scope-a", "session-a", "submission-a", &ticket);
        assert!(matches!(
            submissions.record("scope-a", "session-a", "submission-a", "hello", &[]),
            SubmissionDecision::Accepted { .. }
        ));

        gate.wait();
        worker.join().expect("测试 actor 线程必须正常退出");
    }

    /// 调用方超时但 actor 已完整写出 prompt 时，submission 必须保留重复抑制。
    #[test]
    fn send_timeout_after_prompt_write_keeps_duplicate_guard() {
        let (sender, receiver) = mpsc::channel();
        let (claimed_sender, claimed_receiver) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel(1);
        let worker = thread::spawn(move || {
            let command = receiver
                .recv()
                .expect("测试 actor 必须收到已提交 Send command");
            if let ActorCommand::Send { ticket, .. } = command {
                assert!(ticket.claim_for_prompt());
                assert!(ticket.mark_written());
                claimed_sender
                    .send(())
                    .expect("测试必须观察到 prompt 已写入");
                release_receiver.recv().expect("测试必须释放已写入 actor");
            } else {
                panic!("测试 actor 只接受 Send command");
            }
        });
        let handle = test_actor_handle(sender);
        let mut submissions = SubmissionMap::default();
        let ticket = match submissions.record("scope-a", "session-a", "submission-a", "hello", &[])
        {
            SubmissionDecision::Accepted { ticket, .. } => ticket,
            other => panic!("首次 submission 必须被接受，实际为 {other:?}"),
        };

        let result = request_send_actor_with_timeout(
            &handle,
            "session-a".to_string(),
            "submission-a".to_string(),
            "hello".to_string(),
            ticket.clone(),
            Duration::from_millis(10),
        );
        claimed_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("测试 actor 必须先取得 prompt 写入权");
        let result = result.expect_err("调用方应在 actor 回执前超时");
        assert!(matches!(
            result,
            SendRequestError::PromptMayHaveBeenWritten(error)
                if error.code == "sidecar_unavailable"
        ));
        assert_eq!(ticket.state(), SendTicketState::Written);
        assert!(matches!(
            submissions.record("scope-a", "session-a", "submission-a", "hello", &[]),
            SubmissionDecision::Duplicate { .. }
        ));

        release_sender.send(()).expect("测试必须释放已写入 actor");
        worker.join().expect("测试 actor 线程必须正常退出");
    }

    /// prompt 写阶段失败时只能报告可能已写入，不能把 submission 回滚为可重试。
    #[test]
    fn prompt_write_failure_marks_ticket_may_have_been_written() {
        struct FailingWriter;

        impl std::io::Write for FailingWriter {
            fn write(&mut self, _buffer: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "测试故意让 prompt 写入失败",
                ))
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let temporary = tempfile::tempdir().expect("必须能创建 prompt 写失败测试目录");
        let (_stdout_peer, stdout) =
            std::os::unix::net::UnixStream::pair().expect("prompt 写失败测试必须创建 stdout pipe");
        let acp = AcpRuntime::new(FailingWriter, stdout);
        let (_sender, receiver) = mpsc::channel();
        let accepting = Arc::new(AtomicBool::new(true));
        let exit_intent = Arc::new(AtomicBool::new(false));
        let submission_lock = Arc::new(Mutex::new(()));
        let queued_commands = Arc::new(AtomicUsize::new(0));
        let restart_blocked = Arc::new(AtomicBool::new(false));
        let cleanup_result = Arc::new(Mutex::new(CleanupResult::default()));
        let mut actor = ScopeActor::new(
            "scope-a".to_string(),
            acp,
            HostPolicy::new(temporary.path()).with_meta_key_for("session/prompt", "promptId"),
            HostRuntime::new(
                LifecycleTestApp,
                NoopSink,
                crate::HostRuntimeConfig {
                    home_root: temporary.path().join("app-data"),
                    sidecar_bin: temporary.path().join("unused-sidecar"),
                    sidecar_log_path: temporary.path().join("sidecar.log"),
                    mcp_exec_root: temporary.path().join("mcp"),
                    idle_after: Duration::from_secs(60),
                    l3b: L3bRuntimeConfig::default(),
                    system_prompt: String::new(),
                },
            )
            .channel_service()
            .expect("测试 actor 必须取得 Channel service"),
            Arc::new(NoopSink),
            receiver,
            Arc::clone(&accepting),
            Arc::clone(&exit_intent),
            Arc::clone(&submission_lock),
            Arc::clone(&queued_commands),
            Arc::clone(&restart_blocked),
            Arc::clone(&cleanup_result),
            Arc::new(Mutex::new(TerminalOutbox::default())),
            1,
            Duration::from_secs(60),
            Duration::from_secs(1),
            Duration::from_secs(1),
            ApprovedMcpSpec::default(),
        );
        actor.initialized = true;
        let ticket = SendTicket::new();
        let (reply, reply_receiver) = mpsc::sync_channel(1);
        actor.write_prompt(
            "session-a".to_string(),
            "submission-a".to_string(),
            "hello".to_string(),
            ticket.clone(),
            reply,
        );
        assert_eq!(ticket.state(), SendTicketState::MayHaveBeenWritten);
        assert!(matches!(
            reply_receiver
                .recv_timeout(Duration::from_secs(1))
                .expect("prompt 写失败必须回复调用方"),
            Err(error) if error.code == "sidecar_unavailable"
        ));
    }

    /// prompt 请求在真正触碰 stdin 前失败时，票据必须标记为明确未写入。
    #[test]
    fn prompt_validation_failure_marks_ticket_not_written() {
        let temporary = tempfile::tempdir().expect("必须能创建 prompt 校验失败测试目录");
        let (_stdout_peer, stdout) = std::os::unix::net::UnixStream::pair()
            .expect("prompt 校验失败测试必须创建 stdout pipe");
        let acp = AcpRuntime::new(std::io::sink(), stdout);
        let (_sender, receiver) = mpsc::channel();
        let accepting = Arc::new(AtomicBool::new(true));
        let exit_intent = Arc::new(AtomicBool::new(false));
        let submission_lock = Arc::new(Mutex::new(()));
        let queued_commands = Arc::new(AtomicUsize::new(0));
        let restart_blocked = Arc::new(AtomicBool::new(false));
        let cleanup_result = Arc::new(Mutex::new(CleanupResult::default()));
        let mut actor = ScopeActor::new(
            "scope-a".to_string(),
            acp,
            HostPolicy::new(temporary.path()).with_meta_key_for("session/prompt", "promptId"),
            HostRuntime::new(
                LifecycleTestApp,
                NoopSink,
                crate::HostRuntimeConfig {
                    home_root: temporary.path().join("app-data"),
                    sidecar_bin: temporary.path().join("unused-sidecar"),
                    sidecar_log_path: temporary.path().join("sidecar.log"),
                    mcp_exec_root: temporary.path().join("mcp"),
                    idle_after: Duration::from_secs(60),
                    l3b: L3bRuntimeConfig::default(),
                    system_prompt: String::new(),
                },
            )
            .channel_service()
            .expect("测试 actor 必须取得 Channel service"),
            Arc::new(NoopSink),
            receiver,
            Arc::clone(&accepting),
            Arc::clone(&exit_intent),
            Arc::clone(&submission_lock),
            Arc::clone(&queued_commands),
            Arc::clone(&restart_blocked),
            Arc::clone(&cleanup_result),
            Arc::new(Mutex::new(TerminalOutbox::default())),
            1,
            Duration::from_secs(60),
            Duration::from_secs(1),
            Duration::from_secs(1),
            ApprovedMcpSpec::default(),
        );
        actor.initialized = true;
        let ticket = SendTicket::new();
        let (reply, reply_receiver) = mpsc::sync_channel(1);
        actor.write_prompt(
            String::new(),
            "submission-a".to_string(),
            "hello".to_string(),
            ticket.clone(),
            reply,
        );
        assert_eq!(ticket.state(), SendTicketState::NotWritten);
        assert!(matches!(
            reply_receiver
                .recv_timeout(Duration::from_secs(1))
                .expect("prompt 校验失败必须回复调用方"),
            Err(error) if error.code == "sidecar_unavailable"
        ));
    }

    /// 显式 shutdown 的资源 cleanup 失败后，actor 必须保留到下一次可控重试。
    #[test]
    fn explicit_shutdown_retries_failed_resource_cleanup_before_exit() {
        struct PanicReader {
            stream: std::os::unix::net::UnixStream,
            read_started: Arc<AtomicBool>,
        }

        impl std::io::Read for PanicReader {
            fn read(&mut self, _buffer: &mut [u8]) -> std::io::Result<usize> {
                self.read_started.store(true, Ordering::Release);
                panic!("测试故意让 ACP reader worker 异常退出");
            }
        }

        impl std::os::unix::io::AsRawFd for PanicReader {
            fn as_raw_fd(&self) -> std::os::unix::io::RawFd {
                self.stream.as_raw_fd()
            }
        }

        let temporary = tempfile::tempdir().expect("必须能创建 shutdown cleanup 重试测试目录");
        let config = crate::HostRuntimeConfig {
            home_root: temporary.path().join("app-data"),
            sidecar_bin: temporary.path().join("unused-sidecar"),
            sidecar_log_path: temporary.path().join("sidecar.log"),
            mcp_exec_root: temporary.path().join("mcp"),
            idle_after: Duration::from_secs(60),
            l3b: L3bRuntimeConfig::default(),
            system_prompt: String::new(),
        };
        let runtime = HostRuntime::new(LifecycleTestApp, NoopSink, config);
        let service = runtime
            .channel_service()
            .expect("测试 actor 必须取得 Channel service");
        let (mut stdout_peer, stdout) = std::os::unix::net::UnixStream::pair()
            .expect("shutdown cleanup 重试测试必须创建 stdout pipe");
        let read_started = Arc::new(AtomicBool::new(false));
        let acp = AcpRuntime::new(
            std::io::sink(),
            PanicReader {
                stream: stdout,
                read_started: Arc::clone(&read_started),
            },
        );
        // 第一字节确保 reader 已进入 Read；之后 worker 的 panic 会让首次 join 失败。
        std::io::Write::write_all(&mut stdout_peer, b"x").expect("测试必须触发 ACP reader 异常");
        let read_deadline = Instant::now() + Duration::from_secs(1);
        while !read_started.load(Ordering::Acquire) {
            assert!(
                Instant::now() < read_deadline,
                "测试必须在期限内观察到 ACP reader 已进入 Read"
            );
            thread::sleep(Duration::from_millis(1));
        }

        let (sender, receiver) = mpsc::channel();
        let accepting = Arc::new(AtomicBool::new(true));
        let exit_intent = Arc::new(AtomicBool::new(false));
        let submission_lock = Arc::new(Mutex::new(()));
        let queued_commands = Arc::new(AtomicUsize::new(0));
        let restart_blocked = Arc::new(AtomicBool::new(false));
        let cleanup_result = Arc::new(Mutex::new(CleanupResult::default()));
        let finished = Arc::new(AtomicBool::new(false));
        let mut actor = ScopeActor::new(
            "scope-a".to_string(),
            acp,
            HostPolicy::new(temporary.path()),
            service,
            Arc::new(NoopSink),
            receiver,
            Arc::clone(&accepting),
            Arc::clone(&exit_intent),
            Arc::clone(&submission_lock),
            Arc::clone(&queued_commands),
            Arc::clone(&restart_blocked),
            Arc::clone(&cleanup_result),
            Arc::new(Mutex::new(TerminalOutbox::default())),
            1,
            Duration::from_secs(60),
            Duration::from_secs(1),
            Duration::from_secs(1),
            ApprovedMcpSpec::default(),
        );
        actor.initialized = true;
        let actor_finished = Arc::clone(&finished);
        let actor_thread = thread::spawn(move || {
            actor.run();
            actor_finished.store(true, Ordering::Release);
        });
        let handle = ActorHandle {
            scope_id: "scope-a".to_string(),
            sender,
            accepting,
            exit_intent,
            submission_lock,
            queued_commands,
            restart_blocked,
            shutdown_submitted: AtomicBool::new(false),
            shutdown_attempt: Mutex::new(None),
            cleanup_result,
            join: Mutex::new(Some(actor_thread)),
            finished: Arc::clone(&finished),
        };

        let first = handle.shutdown();
        assert!(
            !first.is_success() && first.failures.contains(&CleanupFailureKind::AcpShutdown),
            "首次 shutdown 必须暴露 reader cleanup 失败"
        );
        assert!(
            !finished.load(Ordering::Acquire),
            "资源 cleanup 失败后 actor 不得在重试前退出"
        );

        let second = handle.shutdown();
        assert!(
            second.is_success(),
            "reader worker 已由首次 shutdown 结算后，第二次 cleanup 必须成功"
        );
        assert!(
            finished.load(Ordering::Acquire),
            "资源 cleanup 成功后 actor 必须退出"
        );
    }

    /// 真实 actor 的 idle cleanup 失败不能销毁重试所需的 actor 生命周期。
    #[test]
    fn idle_cleanup_failure_remains_available_for_explicit_shutdown_retry() {
        let temporary = tempfile::tempdir().expect("必须能创建 idle cleanup 重试测试目录");
        let config = crate::HostRuntimeConfig {
            home_root: temporary.path().join("app-data"),
            sidecar_bin: temporary.path().join("unused-sidecar"),
            sidecar_log_path: temporary.path().join("sidecar.log"),
            mcp_exec_root: temporary.path().join("mcp"),
            idle_after: Duration::from_millis(1),
            l3b: L3bRuntimeConfig::default(),
            system_prompt: String::new(),
        };
        let runtime = HostRuntime::new(LifecycleTestApp, NoopSink, config);
        let service = runtime
            .channel_service()
            .expect("测试 actor 必须取得 Channel service");
        // 保持 stdout peer 存活，避免 EOF 先把 actor 转成 dead，破坏 idle cleanup 场景。
        let (_stdout_peer, stdout) = std::os::unix::net::UnixStream::pair()
            .expect("idle cleanup 重试测试必须创建 stdout pipe");
        let acp = AcpRuntime::new(std::io::sink(), stdout);
        let (sender, receiver) = mpsc::channel();
        let accepting = Arc::new(AtomicBool::new(true));
        let exit_intent = Arc::new(AtomicBool::new(false));
        let submission_lock = Arc::new(Mutex::new(()));
        let queued_commands = Arc::new(AtomicUsize::new(0));
        let restart_blocked = Arc::new(AtomicBool::new(false));
        let cleanup_result = Arc::new(Mutex::new(CleanupResult::default()));
        let finished = Arc::new(AtomicBool::new(false));
        let mut actor = ScopeActor::new(
            "invalid/scope".to_string(),
            acp,
            HostPolicy::new(temporary.path()),
            service,
            Arc::new(NoopSink),
            receiver,
            Arc::clone(&accepting),
            Arc::clone(&exit_intent),
            Arc::clone(&submission_lock),
            Arc::clone(&queued_commands),
            Arc::clone(&restart_blocked),
            Arc::clone(&cleanup_result),
            Arc::new(Mutex::new(TerminalOutbox::default())),
            1,
            Duration::from_millis(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
            ApprovedMcpSpec::default(),
        );
        actor.initialized = true;
        actor.last_activity = Instant::now() - Duration::from_secs(1);

        let actor_finished = Arc::clone(&finished);
        let actor_thread = thread::spawn(move || {
            actor.run();
            actor_finished.store(true, Ordering::Release);
        });
        let handle = ActorHandle {
            scope_id: "invalid/scope".to_string(),
            sender,
            accepting,
            exit_intent,
            submission_lock,
            queued_commands,
            restart_blocked,
            shutdown_submitted: AtomicBool::new(false),
            shutdown_attempt: Mutex::new(None),
            cleanup_result: Arc::clone(&cleanup_result),
            join: Mutex::new(Some(actor_thread)),
            finished: Arc::clone(&finished),
        };

        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let failed = cleanup_result
                .lock()
                .expect("测试 cleanup 结果锁必须可用")
                .failures
                .contains(&CleanupFailureKind::ScopeStop);
            if failed {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "idle cleanup 必须在测试期限内暴露 scope stop 失败"
            );
            thread::sleep(Duration::from_millis(5));
        }
        thread::sleep(Duration::from_millis(50));
        assert!(
            !finished.load(Ordering::Acquire),
            "idle cleanup 失败后 actor 必须保持存活，以便显式 shutdown 重试"
        );

        let cleanup = handle.shutdown();
        assert!(
            !cleanup.is_success() && cleanup.failures.contains(&CleanupFailureKind::ScopeStop),
            "显式 shutdown 必须可达并报告仍失败的 scope stop 步骤"
        );
        assert!(
            !finished.load(Ordering::Acquire),
            "scope stop 仍失败时 actor 必须保留到下一次 cleanup 边界"
        );

        // 测试中的 scope 标识始终非法，释放最后一个 sender 以模拟 runtime 终止并收尾 actor。
        drop(handle);
        let finish_deadline = Instant::now() + Duration::from_secs(1);
        while !finished.load(Ordering::Acquire) {
            assert!(
                Instant::now() < finish_deadline,
                "runtime 终止后 actor 必须在期限内结束"
            );
            thread::sleep(Duration::from_millis(5));
        }
    }

    /// cleanup 某一步失败后，后续调用必须只重试失败步骤并清除已恢复的失败状态。
    #[test]
    fn cleanup_retries_failed_scope_stop_step() {
        let temporary = tempfile::tempdir().expect("必须能创建 cleanup 重试测试目录");
        let config = crate::HostRuntimeConfig {
            home_root: temporary.path().join("app-data"),
            sidecar_bin: temporary.path().join("unused-sidecar"),
            sidecar_log_path: temporary.path().join("sidecar.log"),
            mcp_exec_root: temporary.path().join("mcp"),
            idle_after: Duration::from_secs(60),
            l3b: L3bRuntimeConfig::default(),
            system_prompt: String::new(),
        };
        let runtime = HostRuntime::new(LifecycleTestApp, NoopSink, config);
        let service = runtime
            .channel_service()
            .expect("测试 actor 必须取得 Channel service");
        let (_stdout_peer, stdout) =
            std::os::unix::net::UnixStream::pair().expect("cleanup 重试测试必须创建 stdout pipe");
        let acp = AcpRuntime::new(std::io::sink(), stdout);
        let (_sender, receiver) = mpsc::channel();
        let accepting = Arc::new(AtomicBool::new(true));
        let exit_intent = Arc::new(AtomicBool::new(false));
        let submission_lock = Arc::new(Mutex::new(()));
        let queued_commands = Arc::new(AtomicUsize::new(0));
        let restart_blocked = Arc::new(AtomicBool::new(false));
        let cleanup_result = Arc::new(Mutex::new(CleanupResult::default()));
        let mut actor = ScopeActor::new(
            "invalid/scope".to_string(),
            acp,
            HostPolicy::new(temporary.path()),
            service,
            Arc::new(NoopSink),
            receiver,
            Arc::clone(&accepting),
            Arc::clone(&exit_intent),
            Arc::clone(&submission_lock),
            Arc::clone(&queued_commands),
            Arc::clone(&restart_blocked),
            Arc::clone(&cleanup_result),
            Arc::new(Mutex::new(TerminalOutbox::default())),
            1,
            Duration::from_secs(60),
            Duration::from_secs(1),
            Duration::from_secs(1),
            ApprovedMcpSpec::default(),
        );

        let first = actor.cleanup_resources();
        assert!(
            !first.is_success(),
            "非法 scope 必须让第一次 stop cleanup 暴露失败"
        );
        actor.scope_id = "scope-a".to_string();
        let retry = actor.cleanup_resources();
        assert!(
            retry.is_success(),
            "第二次 cleanup 必须重试已失败的 scope stop 步骤"
        );
    }

    /// sequence 分配失败时不能先消费 terminal marker；恢复序号后同一回合仍应可结算。
    #[test]
    fn terminal_sequence_exhaustion_can_retry_without_losing_terminal_marker() {
        struct RecordingSink {
            events: Arc<Mutex<Vec<KitProductEvent>>>,
        }

        impl KitEventSink for RecordingSink {
            fn emit(&self, event: KitProductEvent) -> Result<()> {
                self.events.lock().expect("测试事件锁必须可用").push(event);
                Ok(())
            }
        }

        let temporary = tempfile::tempdir().expect("必须能创建 terminal sequence 测试目录");
        let config = crate::HostRuntimeConfig {
            home_root: temporary.path().join("app-data"),
            sidecar_bin: temporary.path().join("unused-sidecar"),
            sidecar_log_path: temporary.path().join("sidecar.log"),
            mcp_exec_root: temporary.path().join("mcp"),
            idle_after: Duration::from_secs(60),
            l3b: L3bRuntimeConfig::default(),
            system_prompt: String::new(),
        };
        let runtime = HostRuntime::new(LifecycleTestApp, NoopSink, config);
        let service = runtime
            .channel_service()
            .expect("测试 actor 必须取得 Channel service");
        let (_stdout_peer, stdout) = std::os::unix::net::UnixStream::pair()
            .expect("terminal sequence 测试必须创建 stdout pipe");
        let acp = AcpRuntime::new(std::io::sink(), stdout);
        let (_sender, receiver) = mpsc::channel();
        let accepting = Arc::new(AtomicBool::new(true));
        let exit_intent = Arc::new(AtomicBool::new(false));
        let submission_lock = Arc::new(Mutex::new(()));
        let queued_commands = Arc::new(AtomicUsize::new(0));
        let restart_blocked = Arc::new(AtomicBool::new(false));
        let cleanup_result = Arc::new(Mutex::new(CleanupResult::default()));
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut actor = ScopeActor::new(
            "scope-a".to_string(),
            acp,
            HostPolicy::new(temporary.path()),
            service,
            Arc::new(RecordingSink {
                events: Arc::clone(&events),
            }),
            receiver,
            Arc::clone(&accepting),
            Arc::clone(&exit_intent),
            Arc::clone(&submission_lock),
            Arc::clone(&queued_commands),
            Arc::clone(&restart_blocked),
            cleanup_result,
            Arc::new(Mutex::new(TerminalOutbox::default())),
            1,
            Duration::from_secs(60),
            Duration::from_secs(1),
            Duration::from_secs(1),
            ApprovedMcpSpec::default(),
        );

        actor
            .projector
            .set_next_sequence_for_test("session-a", u64::MAX);
        actor.in_flight.insert(
            "session-a".to_string(),
            InFlightTurn {
                submission_id: "submission-a".to_string(),
                cancelled: false,
            },
        );
        actor.finish_in_flight_turns("error", "终态错误");
        assert!(
            !actor
                .terminal_turns
                .contains(&("session-a".to_string(), "submission-a".to_string())),
            "sequence 分配失败不得永久消费 terminal marker"
        );
        assert!(
            events.lock().expect("测试事件锁必须可用").is_empty(),
            "sequence 分配失败时 terminal 不能假报已运输"
        );
        assert_eq!(
            actor.pending_terminal_intents.len(),
            1,
            "sequence 分配失败必须保留可重试的 terminal 意图"
        );

        actor.projector.set_next_sequence_for_test("session-a", 0);
        actor.retry_pending_terminal_events_now();
        let events = events.lock().expect("测试事件锁必须可用");
        assert_eq!(events.len(), 1, "恢复序号后同一 terminal 必须仍可运输");
        assert_eq!(events[0].event_id, "session-a:host:error:0");
        assert_eq!(events[0].sequence, 0);
    }

    /// 已报告 terminal cleanup 失败但 actor 仍存活时，下一次 shutdown 必须重新投递 attempt。
    #[test]
    fn shutdown_retries_terminal_failure_before_actor_exit() {
        let (sender, receiver) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::sync_channel(1);
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_thread = Arc::clone(&attempts);
        let finished = Arc::new(AtomicBool::new(false));
        let finished_thread = Arc::clone(&finished);
        let queued_commands = Arc::new(AtomicUsize::new(0));
        let queued_commands_thread = Arc::clone(&queued_commands);
        let actor_thread = thread::spawn(move || {
            match receiver.recv() {
                Ok(ActorCommand::Shutdown { attempt }) => {
                    queued_commands_thread.fetch_sub(1, Ordering::AcqRel);
                    attempts_thread.fetch_add(1, Ordering::AcqRel);
                    let mut result = CleanupResult::default();
                    result.record(CleanupFailureKind::TerminalEvent);
                    attempt.complete(result);
                }
                Ok(_) => panic!("测试 actor 首个 command 必须是 shutdown"),
                Err(_) => panic!("测试 actor 不应先收到 sender 断开"),
            }

            loop {
                if release_receiver.try_recv().is_ok() {
                    break;
                }
                match receiver.recv_timeout(Duration::from_millis(5)) {
                    Ok(ActorCommand::Shutdown { attempt }) => {
                        queued_commands_thread.fetch_sub(1, Ordering::AcqRel);
                        attempts_thread.fetch_add(1, Ordering::AcqRel);
                        attempt.complete(CleanupResult::default());
                        break;
                    }
                    Ok(_) => {}
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => break,
                }
            }
            finished_thread.store(true, Ordering::Release);
        });
        let handle = Arc::new(ActorHandle {
            scope_id: "scope-a".to_string(),
            sender,
            accepting: Arc::new(AtomicBool::new(true)),
            exit_intent: Arc::new(AtomicBool::new(false)),
            submission_lock: Arc::new(Mutex::new(())),
            queued_commands,
            restart_blocked: Arc::new(AtomicBool::new(false)),
            shutdown_submitted: AtomicBool::new(false),
            shutdown_attempt: Mutex::new(None),
            cleanup_result: Arc::new(Mutex::new(CleanupResult::default())),
            join: Mutex::new(Some(actor_thread)),
            finished,
        });

        let first = handle.shutdown_with_timeout(Duration::from_millis(100));
        assert!(first.failures.contains(&CleanupFailureKind::TerminalEvent));

        let second = handle.shutdown_with_timeout(Duration::from_millis(500));
        let _ = release_sender.send(());
        let mut join_result = CleanupResult::default();
        handle.join_if_finished(Duration::from_secs(1), &mut join_result);
        assert_eq!(attempts.load(Ordering::Acquire), 2);
        assert!(
            second.is_success(),
            "第二次 shutdown 必须取得新的成功 attempt"
        );
    }

    /// terminal outbox 已成功运输后，完成的 shutdown attempt 必须同步清除旧失败。
    #[test]
    fn delivered_terminal_event_clears_completed_shutdown_failure() {
        let attempt = ShutdownAttempt::new();
        let mut failure = CleanupResult::default();
        failure.record(CleanupFailureKind::TerminalEvent);
        attempt.complete(failure);

        attempt.clear_failure(CleanupFailureKind::TerminalEvent);

        assert_eq!(attempt.snapshot(), Some(CleanupResult::default()));
    }

    /// shutdown 收到 actor 回执后也不得无界 join 仍在运行的 actor thread。
    #[test]
    fn actor_shutdown_returns_bounded_failure_without_joining_running_thread() {
        let (sender, receiver) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::sync_channel(1);
        let actor_thread = thread::spawn(move || match receiver.recv() {
            Ok(ActorCommand::Shutdown { attempt }) => {
                attempt.complete(CleanupResult::default());
                release_receiver
                    .recv()
                    .expect("测试必须释放仍在运行的 actor thread");
            }
            Ok(_) => panic!("测试 actor 只接受 shutdown"),
            Err(_) => panic!("测试 actor 不应先收到 sender 断开"),
        });
        let handle = Arc::new(ActorHandle {
            scope_id: "scope-a".to_string(),
            sender,
            accepting: Arc::new(AtomicBool::new(true)),
            exit_intent: Arc::new(AtomicBool::new(false)),
            submission_lock: Arc::new(Mutex::new(())),
            queued_commands: Arc::new(AtomicUsize::new(0)),
            restart_blocked: Arc::new(AtomicBool::new(false)),
            shutdown_submitted: AtomicBool::new(false),
            shutdown_attempt: Mutex::new(None),
            cleanup_result: Arc::new(Mutex::new(CleanupResult::default())),
            join: Mutex::new(Some(actor_thread)),
            finished: Arc::new(AtomicBool::new(false)),
        });
        let shutdown_handle = Arc::clone(&handle);
        let (shutdown_done, shutdown_result) = mpsc::sync_channel(1);
        thread::spawn(move || {
            shutdown_done
                .send(shutdown_handle.shutdown())
                .expect("测试必须交付有界 shutdown 结果");
        });

        let early_result = shutdown_result.recv_timeout(Duration::from_millis(500));
        let returned_before_release = early_result.is_ok();
        release_sender
            .send(())
            .expect("测试必须释放阻塞的 actor thread");
        let cleanup = match early_result {
            Ok(cleanup) => cleanup,
            Err(_) => shutdown_result
                .recv_timeout(Duration::from_secs(1))
                .expect("释放 actor 后 shutdown thread 必须可收尾"),
        };
        assert!(
            returned_before_release,
            "shutdown 不得在回执后无界等待 actor join"
        );
        assert!(
            !cleanup.is_success(),
            "未确认 actor 已结束时必须 fail-closed 返回 cleanup 失败"
        );
    }

    /// 调用方超时后，后续 cleanup 必须观察同一个已完成的 Shutdown attempt，不能永久粘住失败。
    #[test]
    fn timed_out_shutdown_reuses_completed_attempt_without_duplicate_command() {
        let (sender, receiver) = mpsc::channel();
        let (shutdown_seen, shutdown_observed) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel(1);
        let finished = Arc::new(AtomicBool::new(false));
        let actor_finished = Arc::clone(&finished);
        let actor_thread = thread::spawn(move || {
            match receiver.recv() {
                Ok(ActorCommand::Shutdown { attempt }) => {
                    shutdown_seen
                        .send(())
                        .expect("测试必须观察到 shutdown command");
                    release_receiver
                        .recv()
                        .expect("测试必须释放待完成的 shutdown attempt");
                    attempt.complete(CleanupResult::default());
                }
                Ok(_) => panic!("测试 actor 只接受 shutdown"),
                Err(_) => {}
            }
            actor_finished.store(true, Ordering::Release);
        });
        let handle = ActorHandle {
            scope_id: "scope-a".to_string(),
            sender,
            accepting: Arc::new(AtomicBool::new(true)),
            exit_intent: Arc::new(AtomicBool::new(false)),
            submission_lock: Arc::new(Mutex::new(())),
            queued_commands: Arc::new(AtomicUsize::new(0)),
            restart_blocked: Arc::new(AtomicBool::new(false)),
            shutdown_submitted: AtomicBool::new(false),
            shutdown_attempt: Mutex::new(None),
            cleanup_result: Arc::new(Mutex::new(CleanupResult::default())),
            join: Mutex::new(Some(actor_thread)),
            finished,
        };

        let first = handle.shutdown_with_timeout(Duration::from_millis(10));
        shutdown_observed
            .recv_timeout(Duration::from_secs(1))
            .expect("首次 shutdown 必须已经进入 actor");
        assert!(
            !first.is_success(),
            "调用方超时必须先返回 cleanup 失败，而不是假报成功"
        );
        release_sender
            .send(())
            .expect("测试必须释放已提交的 shutdown attempt");
        let finish_deadline = Instant::now() + Duration::from_secs(1);
        while !handle.finished.load(Ordering::Acquire) {
            assert!(
                Instant::now() < finish_deadline,
                "已完成 shutdown attempt 必须在期限内结束 actor"
            );
            thread::sleep(Duration::from_millis(1));
        }

        let retry = handle.shutdown_with_timeout(Duration::from_millis(100));
        assert!(
            retry.is_success(),
            "后续 cleanup 观察到资源成功完成后不得被首次 timeout 的 ack 失败永久阻塞"
        );
        assert_eq!(
            handle.queued_commands.load(Ordering::Acquire),
            1,
            "已完成的 shutdown attempt 不得再次入队"
        );
    }

    /// 活跃 LoadFlight 的结算必须同时匹配 owner、session、replay epoch 与 generation。
    #[test]
    fn active_load_settlement_rejects_each_mismatched_identity_field() {
        let flight = LoadFlight {
            session_id: "session-1".to_string(),
            owner_request_id: RequestId::new(41),
            replay_epoch: 7,
            waiters: Vec::new(),
            accepted_resume: true,
            pending_send: None,
            deadline: Instant::now() + Duration::from_secs(1),
            generation: 3,
            state: LoadFlightState::AcpWritten,
        };

        assert!(ScopeActor::load_flight_matches(
            &flight,
            RequestId::new(41),
            "session-1",
            7,
            3,
        ));
        for (label, owner, session, epoch, generation) in [
            ("owner", RequestId::new(42), "session-1", 7, 3),
            ("session", RequestId::new(41), "session-2", 7, 3),
            ("replay_epoch", RequestId::new(41), "session-1", 8, 3),
            ("generation", RequestId::new(41), "session-1", 7, 4),
        ] {
            assert!(
                !ScopeActor::load_flight_matches(&flight, owner, session, epoch, generation),
                "active settlement must reject {label} mismatch"
            );
        }
    }

    /// outbox 锁不可用时，actor 仍须保留 terminal pending 以便后续 cleanup 重试。
    #[test]
    fn terminal_outbox_lock_failure_preserves_pending_event_for_retry() {
        let temporary = tempfile::tempdir().expect("必须能创建 terminal outbox 测试目录");
        let config = crate::HostRuntimeConfig {
            home_root: temporary.path().join("app-data"),
            sidecar_bin: temporary.path().join("unused-sidecar"),
            sidecar_log_path: temporary.path().join("sidecar.log"),
            mcp_exec_root: temporary.path().join("mcp"),
            idle_after: Duration::from_secs(60),
            l3b: L3bRuntimeConfig::default(),
            system_prompt: String::new(),
        };
        let runtime = HostRuntime::new(LifecycleTestApp, NoopSink, config);
        let service = runtime
            .channel_service()
            .expect("测试 actor 必须取得 Channel service");
        let (_stdout_peer, stdout) = std::os::unix::net::UnixStream::pair()
            .expect("terminal outbox 测试必须创建 stdout pipe");
        let acp = AcpRuntime::new(std::io::sink(), stdout);
        let (_sender, receiver) = mpsc::channel();
        let accepting = Arc::new(AtomicBool::new(true));
        let exit_intent = Arc::new(AtomicBool::new(false));
        let submission_lock = Arc::new(Mutex::new(()));
        let queued_commands = Arc::new(AtomicUsize::new(0));
        let restart_blocked = Arc::new(AtomicBool::new(false));
        let cleanup_result = Arc::new(Mutex::new(CleanupResult::default()));
        let terminal_outbox = Arc::new(Mutex::new(TerminalOutbox::default()));
        let mut actor = ScopeActor::new(
            "scope-a".to_string(),
            acp,
            HostPolicy::new(temporary.path()),
            service,
            Arc::new(NoopSink),
            receiver,
            accepting,
            exit_intent,
            submission_lock,
            queued_commands,
            restart_blocked,
            cleanup_result,
            Arc::clone(&terminal_outbox),
            1,
            Duration::from_secs(60),
            Duration::from_secs(1),
            Duration::from_secs(1),
            ApprovedMcpSpec::default(),
        );
        let event_id = "session-a:host:error:0".to_string();
        actor.pending_terminal_events.insert(
            ("session-a".to_string(), "submission-a".to_string()),
            PendingTerminalEvent {
                event: KitProductEvent {
                    schema_version: KIT_SCHEMA_VERSION,
                    scope_id: "scope-a".to_string(),
                    session_id: "session-a".to_string(),
                    turn_id: Some("submission-a".to_string()),
                    submission_id: Some("submission-a".to_string()),
                    event_id: event_id.clone(),
                    sequence: 0,
                    origin: Origin::Live,
                    block_id: event_id,
                    block: KitBlock::Status {
                        code: "error".to_string(),
                        message: "终态错误".to_string(),
                    },
                },
                retain: true,
                next_attempt: Instant::now(),
            },
        );

        let poisoned_outbox = Arc::clone(&terminal_outbox);
        let _ = thread::spawn(move || {
            let _guard = poisoned_outbox.lock().expect("测试必须先取得 outbox 锁");
            panic!("故意 poison terminal outbox 锁");
        })
        .join();

        actor.handoff_pending_terminal_events();

        assert!(
            actor.pending_terminal_events.is_empty(),
            "outbox 接管成功后 actor 不应继续持有 terminal pending"
        );
        let outbox = terminal_outbox
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(
            outbox.pending.len(),
            1,
            "poisoned outbox 仍必须接管 terminal event"
        );
        let event = outbox
            .pending
            .values()
            .next()
            .expect("outbox 必须保留 terminal event");
        assert_eq!(event.event.event_id, "session-a:host:error:0");
    }
}
