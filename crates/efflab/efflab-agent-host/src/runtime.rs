//! HostRuntime 的每 scope ACP IO actor 闭环。
//!
//! 产品只通过 [`HostRuntime::dispatch`] 进入此模块。每个 scope 的 actor 独占
//! sidecar stdin/stdout、ACP reader、投影器和会话状态；同步 dispatch 只等待命令
//! 规定的回执时机，绝不在产品线程直接读取 sidecar stdout。

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use efflab_agent_contract::{HostPolicy, validate_prompt_text};
use serde_json::{Value, json};

use crate::acp_runtime::{AcpRuntime, Inbound, RequestId, RpcError, ValidatedReply};
use crate::app_port::{ApprovedMcpSpec, HostApp, ScopeId};
use crate::event_sink::KitEventSink;
use crate::llm_channel::{LaunchedScope, LlmChannelError, LlmChannelService, SetLlmChannelRequest};
use crate::projector::Projector;
use crate::protocol::{
    Capability, CapabilityLimits, KIT_SCHEMA_VERSION, KitBlock, KitCommand, KitError,
    KitProductEvent, KitReply, LlmChannelKind, Origin, SessionSummary,
};
use crate::submission::{SubmissionDecision, SubmissionMap};
use crate::supervisor::{SupervisorCapability, UnavailableReason, capability};
use crate::{METHOD_NOT_FOUND, ValidatedKitEventSink};

/// initialize 结果迟迟未到时的协议超时；不能让 New/List 永久卡住。
const INITIALIZE_TIMEOUT: Duration = Duration::from_secs(20);
/// 同步 Kit 调用等待 actor 回执的总上限，覆盖初始化超时和后续 ACP 请求。
const DISPATCH_REPLY_TIMEOUT: Duration = Duration::from_secs(25);
/// actor 空闲轮询间隔；stdout reader 独立运行，因此该值不影响 ACP 收包顺序。
const ACTOR_TICK: Duration = Duration::from_millis(5);
/// MCP catalog 中始终可安全自动许可的内置无副作用工具。
const NOOP_TOOL: &str = "GrokBuild:efflab_noop";

/// 产品唯一调用入口的进程内状态。
pub struct HostRuntime {
    /// 产品领域端口。仅 runtime 读取 MCP 批准集和构造 Channel 服务时使用。
    app: Arc<dyn HostApp>,
    /// 所有产品事件都已包入校验边界，actor 不可绕开该运输路径。
    sink: Arc<dyn KitEventSink>,
    /// 运行时固定配置；每个新 actor 从此派生 scope 私有路径与 idle 策略。
    cfg: crate::HostRuntimeConfig,
    /// Channel 服务构造也可能因历史配置不安全而失败；构造 API 不能 panic。
    channel: Result<Arc<LlmChannelService>, LlmChannelError>,
    /// 进程内 Send 幂等边界，跨 actor restart 保持。
    submissions: Mutex<SubmissionMap>,
    /// 每个 scope 一个唯一 IO actor；失败杀停的 actor 保留以 fail-closed 而非自动复活。
    actors: Mutex<BTreeMap<String, Arc<ActorHandle>>>,
    /// 全局换通道与新 actor launch 的互斥门，防止旧 revision 与新 revision 交错。
    channel_transition: Mutex<()>,
}

impl HostRuntime {
    /// 构造进程内单例运行时；实际 L3b 监听和 sidecar spawn 均延迟到对话命令。
    pub fn new(
        app: impl HostApp + 'static,
        sink: impl KitEventSink + 'static,
        cfg: crate::HostRuntimeConfig,
    ) -> Self {
        let app = Arc::new(app);
        let channel = LlmChannelService::new(Arc::clone(&app), cfg.clone()).map(Arc::new);
        let app: Arc<dyn HostApp> = app;
        let sink: Arc<dyn KitEventSink> = Arc::new(ValidatedKitEventSink::new(sink));

        Self {
            app,
            sink,
            cfg,
            channel,
            submissions: Mutex::new(SubmissionMap::default()),
            actors: Mutex::new(BTreeMap::new()),
            channel_transition: Mutex::new(()),
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

    /// GetCapability 只读本地 committed view；未配置时不能伪装 sidecar 已可用。
    fn dispatch_get_capability(&self) -> Result<KitReply, KitError> {
        let channel = self.channel_service()?.view().map_err(channel_error)?;
        if channel.kind.is_none() {
            return Err(LlmChannelError::Unconfigured.as_kit_error());
        }

        let (sidecar, reason) = match capability() {
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
                max_prompt_chars: 32_000,
            },
        }))
    }

    /// 对 Send 执行稳定幂等登记后，把实际 sidecar 写入交给对应 scope actor。
    fn dispatch_send(
        &self,
        scope_id: String,
        session_id: String,
        submission_id: String,
        text: String,
        mentions: Vec<crate::MentionId>,
    ) -> Result<KitReply, KitError> {
        validate_send_input(&scope_id, &session_id, &submission_id, &text)?;
        self.require_conversation_channel()?;
        let decision = self
            .submissions
            .lock()
            .map_err(|_| KitError::non_retryable("sidecar_unavailable", "提交映射不可用"))?
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
            SubmissionDecision::Accepted { .. } => {
                let actor = match self.actor_for_scope(&scope_id) {
                    Ok(actor) => actor,
                    Err(error) => {
                        self.forget_submission(&scope_id, &session_id, &submission_id);
                        return Err(error);
                    }
                };
                let result = request_actor(&actor, |reply| ActorCommand::Send {
                    session_id: session_id.clone(),
                    submission_id: submission_id.clone(),
                    text,
                    reply,
                });
                if result.is_err() {
                    // 只有 actor 确认 prompt 已写入时才会返回 Send；其它错误可以安全重试。
                    self.forget_submission(&scope_id, &session_id, &submission_id);
                }
                result
            }
        }
    }

    /// 实施全局 Channel 事务：先提交/失效，再 drain 与重建全部先前存活 scope。
    fn dispatch_set_channel(&self, request: SetLlmChannelRequest) -> Result<KitReply, KitError> {
        let _transition = self
            .channel_transition
            .lock()
            .map_err(|_| KitError::non_retryable("sidecar_unavailable", "通道事务不可用"))?;
        let service = self.channel_service()?;
        let change = service
            .commit_and_invalidate(request)
            .map_err(channel_error)?;
        if !change.changed {
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
        // 先从 map 取走旧 actor，确保旧 binding 已失效后没有任何 actor 能继续使用旧 stdin。
        let previous = {
            let mut actors = self.actors.lock().map_err(|_| {
                KitError::non_retryable("sidecar_unavailable", "scope actor 注册表不可用")
            })?;
            std::mem::take(&mut *actors).into_iter().collect::<Vec<_>>()
        };
        let mut restart_failed = false;
        let scopes = previous
            .iter()
            .filter(|(scope_id, _)| live_scopes.contains(scope_id))
            .map(|(scope_id, _)| scope_id.clone())
            .collect::<Vec<_>>();
        for (scope_id, actor) in previous {
            // stale actor 同样要清理 reader thread，但它不属于“部分 live restart 失败”。
            if !actor.shutdown() && live_scopes.contains(&scope_id) {
                restart_failed = true;
            }
        }

        // 即使单个旧 actor drain 失败，也继续尝试其余 scope；新 committed view 不回滚。
        for scope_id in scopes {
            match self.spawn_actor(&scope_id) {
                Ok(actor) => {
                    if let Ok(mut actors) = self.actors.lock() {
                        actors.insert(scope_id, actor);
                    } else {
                        restart_failed = true;
                    }
                }
                Err(_) => restart_failed = true,
            }
        }

        if restart_failed {
            return Err(LlmChannelError::RestartFailed.as_kit_error());
        }
        Ok(KitReply::LlmChannelView {
            channel: change.view,
        })
    }

    /// 取得或新建一个 scope actor；本入口与 SetLlmChannel 共享全局换代门。
    fn actor_for_scope(&self, scope_id: &str) -> Result<Arc<ActorHandle>, KitError> {
        let _transition = self
            .channel_transition
            .lock()
            .map_err(|_| KitError::non_retryable("sidecar_unavailable", "通道事务不可用"))?;
        let mut actors = self.actors.lock().map_err(|_| {
            KitError::non_retryable("sidecar_unavailable", "scope actor 注册表不可用")
        })?;
        if let Some(actor) = actors.get(scope_id)
            && actor.accepting.load(Ordering::Acquire)
        {
            return Ok(Arc::clone(actor));
        }
        actors.remove(scope_id);

        let actor = self.spawn_actor(scope_id)?;
        actors.insert(scope_id.to_string(), Arc::clone(&actor));
        Ok(actor)
    }

    /// 构造并启动 actor；真实 sidecar spawn 顺序只能经 LlmChannelService 进入。
    fn spawn_actor(&self, scope_id: &str) -> Result<Arc<ActorHandle>, KitError> {
        let service = self.channel_service()?;
        let expected_tools = self
            .app
            .mcp_for_scope(&ScopeId(scope_id.to_string()))
            .map_err(|_| KitError::non_retryable("sidecar_unavailable", "MCP 批准规格不可用"))?;
        let model_id = service
            .view()
            .map_err(channel_error)?
            .model_id
            .ok_or_else(|| LlmChannelError::Unconfigured.as_kit_error())?;
        tracing::debug!(scope = %scope_id, "正在启动 scope ACP IO actor");
        let launched = service
            .launch_scope_with_stdio(scope_id)
            .map_err(channel_error)?;
        tracing::debug!(
            scope = %scope_id,
            generation = launched.info.generation,
            sidecar_pid = launched.info.pid,
            "scope sidecar 已移交给 ACP IO actor"
        );
        let policy = match host_policy(&launched, &model_id) {
            Ok(policy) => policy,
            Err(error) => {
                // policy 失败时 stdio 即将关闭；同时让 Supervisor 立即回收已注册的 child/token。
                let _ = service.stop_scope(scope_id);
                return Err(error);
            }
        };
        let acp = AcpRuntime::new(launched.stdio.stdin, launched.stdio.stdout);
        let (sender, receiver) = mpsc::channel();
        let accepting = Arc::new(AtomicBool::new(true));
        let actor = ScopeActor::new(
            scope_id.to_string(),
            acp,
            policy,
            model_id,
            service,
            Arc::clone(&self.sink),
            receiver,
            Arc::clone(&accepting),
            self.cfg.idle_after,
            expected_tools,
        );
        let name = format!("efflab-acp-{}", scope_id);
        let join = match thread::Builder::new().name(name).spawn(move || actor.run()) {
            Ok(join) => join,
            Err(_) => {
                // closure 被释放时 AcpRuntime 会关闭 stdin；再显式回收 child，不能遗留 token。
                let _ = self.channel_service()?.stop_scope(scope_id);
                return Err(KitError::non_retryable(
                    "sidecar_unavailable",
                    "无法启动 sidecar IO actor",
                ));
            }
        };
        Ok(Arc::new(ActorHandle {
            sender,
            accepting,
            join: Mutex::new(Some(join)),
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

    /// 回滚未写入 sidecar 的首次 Send 登记，避免无效 duplicate 锁死重试。
    fn forget_submission(&self, scope_id: &str, session_id: &str, submission_id: &str) {
        if let Ok(mut submissions) = self.submissions.lock() {
            submissions.forget(scope_id, session_id, submission_id);
        }
    }
}

impl Drop for HostRuntime {
    /// Runtime 生命周期结束时先关 actor stdin，再由 Supervisor 回收对应 child。
    fn drop(&mut self) {
        let actors = self
            .actors
            .get_mut()
            .map(|actors| std::mem::take(actors))
            .unwrap_or_default();
        for (_, actor) in actors {
            let _ = actor.shutdown();
        }
    }
}

/// 一个 actor 的外部命令句柄；只有 actor thread 自己能持有 AcpRuntime。
struct ActorHandle {
    sender: Sender<ActorCommand>,
    /// false 仅表示正常 idle/shutdown 退出，Host 下次命令可安全 spawn 新代。
    accepting: Arc<AtomicBool>,
    join: Mutex<Option<JoinHandle<()>>>,
}

impl ActorHandle {
    /// 协调 actor 的有序关闭；失败只表示上层应把全局 Channel restart 标为可重试。
    fn shutdown(&self) -> bool {
        let (reply, receiver) = mpsc::sync_channel(1);
        let sent = self.sender.send(ActorCommand::Shutdown { reply }).is_ok();
        let acknowledged = sent && receiver.recv_timeout(DISPATCH_REPLY_TIMEOUT).is_ok();
        self.accepting.store(false, Ordering::Release);
        if let Ok(mut join) = self.join.lock()
            && let Some(join) = join.take()
        {
            let _ = join.join();
        }
        acknowledged
    }
}

/// actor 处理的内部命令；各同步 Kit 调用都有独立一次性 reply 通道。
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
    Send {
        session_id: String,
        submission_id: String,
        text: String,
        reply: ReplySender,
    },
    Cancel {
        session_id: String,
        reply: ReplySender,
    },
    Shutdown {
        reply: SyncSender<()>,
    },
}

type ReplySender = SyncSender<Result<KitReply, KitError>>;

/// actor 中等待 sidecar response 的请求分类。
enum PendingRpc {
    NewSession {
        reply: ReplySender,
    },
    ListSessions {
        reply: ReplySender,
    },
    Load {
        session_id: String,
        after_load: Option<PendingSend>,
    },
    Prompt {
        session_id: String,
        submission_id: String,
    },
    McpCatalog {
        session_id: String,
    },
}

/// 自动 cold-load 完成后才可写入的 Send；其 reply 必须留到真实 prompt 写成功。
struct PendingSend {
    submission_id: String,
    text: String,
    reply: ReplySender,
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
    model_id: String,
    service: Arc<LlmChannelService>,
    sink: Arc<dyn KitEventSink>,
    receiver: Receiver<ActorCommand>,
    accepting: Arc<AtomicBool>,
    idle_after: Duration,
    expected_tools: BTreeSet<String>,
    projector: Projector,
    initialized: bool,
    initialize_id: Option<RequestId>,
    initialize_deadline: Instant,
    deferred: VecDeque<ActorCommand>,
    pending: BTreeMap<RequestId, PendingRpc>,
    active_sessions: BTreeSet<String>,
    current_session: Option<String>,
    loading_sessions: BTreeSet<String>,
    in_flight: BTreeMap<String, InFlightTurn>,
    cancel_requested: BTreeSet<String>,
    catalog_pending: BTreeSet<String>,
    catalog_waiting: BTreeMap<String, VecDeque<ActorCommand>>,
    buffers: BTreeMap<String, Vec<KitProductEvent>>,
    terminal_turns: BTreeSet<(String, String)>,
    last_activity: Instant,
    dead: bool,
}

impl ScopeActor {
    /// 建立 actor 的纯内存状态；initialize 必须在 thread 内先写入以保持单 writer。
    #[allow(clippy::too_many_arguments)]
    fn new(
        scope_id: String,
        acp: AcpRuntime,
        policy: HostPolicy,
        model_id: String,
        service: Arc<LlmChannelService>,
        sink: Arc<dyn KitEventSink>,
        receiver: Receiver<ActorCommand>,
        accepting: Arc<AtomicBool>,
        idle_after: Duration,
        approved: ApprovedMcpSpec,
    ) -> Self {
        Self {
            projector: Projector::new(scope_id.clone()),
            scope_id,
            acp,
            policy,
            model_id,
            service,
            sink,
            receiver,
            accepting,
            idle_after,
            expected_tools: approved.expected_tools().clone(),
            initialized: false,
            initialize_id: None,
            initialize_deadline: Instant::now() + INITIALIZE_TIMEOUT,
            deferred: VecDeque::new(),
            pending: BTreeMap::new(),
            active_sessions: BTreeSet::new(),
            current_session: None,
            loading_sessions: BTreeSet::new(),
            in_flight: BTreeMap::new(),
            cancel_requested: BTreeSet::new(),
            catalog_pending: BTreeSet::new(),
            catalog_waiting: BTreeMap::new(),
            buffers: BTreeMap::new(),
            terminal_turns: BTreeSet::new(),
            last_activity: Instant::now(),
            dead: false,
        }
    }

    /// actor 主循环：优先消费 stdout，再以短 tick 接收 Kit 命令和 idle 截止。
    fn run(mut self) {
        if let Err(error) = self.begin_initialize() {
            self.enter_dead(error);
        }

        loop {
            if self.dead {
                match self.receiver.recv_timeout(ACTOR_TICK) {
                    Ok(ActorCommand::Shutdown { reply }) => {
                        self.shutdown_and_exit();
                        let _ = reply.send(());
                        return;
                    }
                    Ok(command) => self.reject_dead_command(command),
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
            if self.should_idle_stop() {
                self.idle_stop_and_exit();
                return;
            }

            match self.receiver.recv_timeout(ACTOR_TICK) {
                Ok(ActorCommand::Shutdown { reply }) => {
                    self.shutdown_and_exit();
                    let _ = reply.send(());
                    return;
                }
                Ok(command) => {
                    self.last_activity = Instant::now();
                    self.handle_command(command);
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
                    "protocolVersion": 1,
                    "client": { "name": "efflab-agent-host", "mcpServers": [] },
                    "capabilities": { "terminal": false, "fs": false },
                    "_meta": { "modelId": self.model_id },
                }),
                &self.policy,
            )
            .map_err(|_| sidecar_unavailable("无法写入 sidecar initialize"))?;
        self.initialize_id = Some(id);
        Ok(())
    }

    /// 清空已到达的 stdout 项；reader 本身独立运行，command 等待不会吞掉 notification。
    fn drain_inbound(&mut self) -> bool {
        loop {
            match self.acp.poll_inbound() {
                Ok(Some(inbound)) => {
                    self.last_activity = Instant::now();
                    self.handle_inbound(inbound);
                    if self.dead {
                        return false;
                    }
                }
                Ok(None) => return true,
                Err(_) => {
                    self.enter_dead(sidecar_unavailable("sidecar stdio 已终止"));
                    return false;
                }
            }
        }
    }

    /// 路由 response、notification 和 reverse request，绝不把 ACP payload 直出产品层。
    fn handle_inbound(&mut self, inbound: Inbound) {
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
                Ok(_) => {
                    self.initialized = true;
                    // 初始化完成后按原顺序执行调用方已经提交的命令。
                    while let Some(command) = self.deferred.pop_front() {
                        self.handle_command(command);
                        if self.dead {
                            return;
                        }
                    }
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
            PendingRpc::Load {
                session_id,
                after_load,
            } => self.finish_load(session_id, after_load, result),
            PendingRpc::Prompt {
                session_id,
                submission_id,
            } => self.finish_prompt(session_id, submission_id, result),
            PendingRpc::McpCatalog { session_id } => self.finish_mcp_catalog(session_id, result),
        }
    }

    /// 投影标准 session/update；坏的单条 update 只记为不可用事件，不中断 stdio。
    fn handle_notification(&mut self, method: &str, params: &Value) {
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
                    self.emit_event(event, true);
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
        if method == "session/request_permission" {
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
            let _ = self.acp.reply_validated(
                id,
                ValidatedReply::Result(json!({ "outcome": { "outcome": "cancelled" } })),
                &self.policy,
            );
            self.emit_session_status(
                &session_id,
                "skipped_update",
                "已拒绝不支持的 sidecar 反向请求",
                Origin::Live,
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

    /// 命令在 initialize 前排队，初始化成功后按到达顺序恢复。
    fn handle_command(&mut self, command: ActorCommand) {
        if !self.initialized {
            self.deferred.push_back(command);
            return;
        }
        match command {
            ActorCommand::NewSession { reply } => self.start_new_session(reply),
            ActorCommand::ListSessions { cursor, reply } => self.start_list_sessions(cursor, reply),
            ActorCommand::ResumeSession { session_id, reply } => {
                self.resume_session(session_id, reply)
            }
            ActorCommand::Send {
                session_id,
                submission_id,
                text,
                reply,
            } => self.send_prompt(session_id, submission_id, text, reply),
            ActorCommand::Cancel { session_id, reply } => self.cancel_session(session_id, reply),
            ActorCommand::Shutdown { reply } => {
                self.shutdown_and_exit();
                let _ = reply.send(());
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
            "_meta": { "modelId": self.model_id },
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

    /// 热 resume 重放内存；冷 resume 写 load 后立刻 accepted，结果走事件栅栏。
    fn resume_session(&mut self, session_id: String, reply: ReplySender) {
        if self.active_sessions.contains(&session_id) {
            self.hot_resume(&session_id);
            let _ = reply.send(Ok(KitReply::ResumeSession {
                accepted: true,
                session_id,
            }));
            return;
        }
        if !self.in_flight.is_empty() {
            let _ = reply.send(Err(KitError::non_retryable(
                "session_busy",
                "当前 scope 正在生成，不能恢复其它会话",
            )));
            return;
        }
        match self.start_load(&session_id, None) {
            Ok(()) => {
                let _ = reply.send(Ok(KitReply::ResumeSession {
                    accepted: true,
                    session_id,
                }));
            }
            Err(error) => {
                let _ = reply.send(Err(error.clone()));
                self.enter_dead(error);
            }
        }
    }

    /// Send 若当前进程没有 session，则先 cold-load、排空 replay，之后才能写 prompt。
    fn send_prompt(
        &mut self,
        session_id: String,
        submission_id: String,
        text: String,
        reply: ReplySender,
    ) {
        if self.in_flight.contains_key(&session_id) || self.loading_sessions.contains(&session_id) {
            let _ = reply.send(Err(KitError::non_retryable(
                "turn_in_progress",
                "该会话已有正在处理的回合",
            )));
            return;
        }
        if !self.active_sessions.contains(&session_id) {
            // start_load 失败前不会接管外部 reply，因此保留一份 sender 立即回绝调用方。
            let error_reply = reply.clone();
            let pending = PendingSend {
                submission_id,
                text,
                reply,
            };
            if let Err(error) = self.start_load(&session_id, Some(pending)) {
                // start_load 尚未写 prompt，调用方可安全将 SubmissionMap 回滚。
                let _ = error_reply.send(Err(error.clone()));
                self.enter_dead(error);
            }
            return;
        }
        if self.catalog_pending.contains(&session_id) {
            self.catalog_waiting
                .entry(session_id.clone())
                .or_default()
                .push_back(ActorCommand::Send {
                    session_id: session_id.clone(),
                    submission_id,
                    text,
                    reply,
                });
            return;
        }
        self.write_prompt(session_id, submission_id, text, reply);
    }

    /// 真正写 prompt 前消费 pre-cancel；已发 prompt 的 in-flight 只能在 result 后清除。
    fn write_prompt(
        &mut self,
        session_id: String,
        submission_id: String,
        text: String,
        reply: ReplySender,
    ) {
        if self.cancel_requested.remove(&session_id) {
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
            .request_validated("session/prompt", params, &self.policy)
        {
            Ok(id) => {
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
            Err(_) => {
                let _ = reply.send(Err(sidecar_unavailable("无法写入 session/prompt")));
                self.enter_dead(sidecar_unavailable("sidecar stdin 不可用"));
            }
        }
    }

    /// cancel 始终写无 id notification；无 in-flight 时记录一次 pre-cancel 供下一 Send 消费。
    fn cancel_session(&mut self, session_id: String, reply: ReplySender) {
        match self.acp.notify_validated(
            "session/cancel",
            json!({ "sessionId": session_id }),
            &self.policy,
        ) {
            Ok(()) => {
                // 只有通知已经写入 sidecar，才向本地状态和产品事件声明该回合已取消。
                self.cancel_requested.insert(session_id.clone());
                if let Some(in_flight) = self.in_flight.get_mut(&session_id) {
                    in_flight.cancelled = true;
                    let submission_id = in_flight.submission_id.clone();
                    self.emit_turn_status(&session_id, &submission_id, "cancelled", "回合已取消");
                }
                let _ = reply.send(Ok(KitReply::Cancel { accepted: true }));
            }
            Err(_) => {
                let _ = reply.send(Err(sidecar_unavailable("无法写入 session/cancel")));
                self.enter_dead(sidecar_unavailable("sidecar stdin 不可用"));
            }
        }
    }

    /// 冷 load 在写入前建立 replay epoch；sidecar result 到达时才发 replay_complete。
    fn start_load(
        &mut self,
        session_id: &str,
        after_load: Option<PendingSend>,
    ) -> Result<(), KitError> {
        self.current_session = Some(session_id.to_string());
        self.loading_sessions.insert(session_id.to_string());
        self.projector.begin_replay(session_id);
        let params = json!({
            "sessionId": session_id,
            "cwd": self.policy.expected_cwd,
            "mcpServers": [],
            "_meta": { "modelId": self.model_id },
        });
        match self
            .acp
            .request_validated("session/load", params, &self.policy)
        {
            Ok(id) => {
                self.pending.insert(
                    id,
                    PendingRpc::Load {
                        session_id: session_id.to_string(),
                        after_load,
                    },
                );
                Ok(())
            }
            Err(_) => {
                self.loading_sessions.remove(session_id);
                Err(sidecar_unavailable("无法写入 session/load"))
            }
        }
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
        self.buffers.entry(session_id.clone()).or_default();
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

    /// load result 成功后先结束 replay 栅栏，再按需要继续自动 prompt。
    fn finish_load(
        &mut self,
        session_id: String,
        after_load: Option<PendingSend>,
        result: Result<Value, RpcError>,
    ) {
        self.loading_sessions.remove(&session_id);
        if result.is_err() {
            self.current_session = None;
            if let Some(pending) = after_load {
                let _ = pending.reply.send(Err(KitError::non_retryable(
                    "session_not_found",
                    "sidecar 无法加载指定会话",
                )));
            }
            return;
        }
        self.active_sessions.insert(session_id.clone());
        self.current_session = Some(session_id.clone());
        self.buffers.entry(session_id.clone()).or_default();
        self.finish_replay(&session_id);
        self.start_mcp_catalog(&session_id);
        if let Some(pending) = after_load {
            self.send_prompt(
                session_id,
                pending.submission_id,
                pending.text,
                pending.reply,
            );
        }
    }

    /// prompt response 才释放 in-flight；cancel 已发状态时不可追加 turn_completed。
    fn finish_prompt(
        &mut self,
        session_id: String,
        submission_id: String,
        result: Result<Value, RpcError>,
    ) {
        let in_flight = self.in_flight.remove(&session_id);
        self.cancel_requested.remove(&session_id);
        let cancelled = in_flight.is_some_and(|turn| turn.cancelled);
        if cancelled {
            return;
        }
        match result {
            Ok(_) => {
                self.emit_turn_status(&session_id, &submission_id, "turn_completed", "回合已完成")
            }
            Err(_) => {
                self.emit_turn_status(&session_id, &submission_id, "error", "sidecar 回合失败")
            }
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
                    self.enter_dead(sidecar_unavailable("MCP catalog 包含未批准工具"));
                    return;
                }
                if !self.expected_tools.is_empty() && !self.expected_tools.is_subset(&tools) {
                    self.emit_session_status(
                        &session_id,
                        "mcp_failed",
                        "部分已批准 MCP 工具未就绪",
                        Origin::Live,
                    );
                }
            }
            Err(_) if !self.expected_tools.is_empty() => self.emit_session_status(
                &session_id,
                "mcp_failed",
                "无法确认已批准 MCP 工具状态",
                Origin::Live,
            ),
            Err(_) => {}
        }
        self.release_catalog_waiters(&session_id);
    }

    /// 请求每个新/冷恢复会话的 catalog；空批准集的失败只是观察失败而非聊天阻塞。
    fn start_mcp_catalog(&mut self, session_id: &str) {
        self.catalog_pending.insert(session_id.to_string());
        let params = json!({
            "sessionId": session_id,
            "_meta": { "modelId": self.model_id },
        });
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
            }
            Err(_) => {
                self.catalog_pending.remove(session_id);
                // catalog 的 JSON-RPC error 可以降级，但连查询都写不进 stdin 时 sidecar
                // 已不可信，不能绕过 gate 继续写 prompt。
                self.enter_dead(sidecar_unavailable("无法写入 MCP catalog 请求"));
            }
        }
    }

    /// catalog 完成后恢复被 gate 的 Send；每条仍重新检查 turn/cancel 状态。
    fn release_catalog_waiters(&mut self, session_id: &str) {
        let mut waiting = self.catalog_waiting.remove(session_id).unwrap_or_default();
        while let Some(command) = waiting.pop_front() {
            self.handle_command(command);
            if self.dead {
                return;
            }
        }
    }

    /// 热恢复不触碰 ACP；将内存快照转为 replay，流式助手块明确冻结。
    fn hot_resume(&mut self, session_id: &str) {
        let snapshot = self.buffers.get(session_id).cloned().unwrap_or_default();
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

    /// 冷恢复的 replay fence：未知 update 只汇总一条 session 级状态。
    fn finish_replay(&mut self, session_id: &str) {
        let skipped = self.projector.take_replay_skipped_count(session_id);
        if skipped > 0 {
            self.emit_session_status(
                session_id,
                "replay_skipped",
                &format!("已跳过 {skipped} 条不支持的历史更新"),
                Origin::Replay,
            );
        }
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

    /// 发送回合级终态；它必须携带真实 prompt/submission id，不能伪造 synthetic id。
    fn emit_turn_status(
        &mut self,
        session_id: &str,
        submission_id: &str,
        code: &str,
        message: &str,
    ) {
        let Ok(sequence) = self.projector.next_host_sequence(session_id) else {
            return;
        };
        let event_id = format!("{session_id}:host:{code}:{sequence}");
        self.terminal_turns
            .insert((session_id.to_string(), submission_id.to_string()));
        self.emit_event(
            KitProductEvent {
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
            true,
        );
    }

    /// 输出前再次走验证 sink；成功事件才记入热恢复内存，防止坏事件污染下一次 replay。
    fn emit_event(&mut self, event: KitProductEvent, retain: bool) {
        let session_id = event.session_id.clone();
        if self.sink.emit(event.clone()).is_err() {
            tracing::debug!(scope = %self.scope_id, "Kit 事件运输失败");
            return;
        }
        if retain
            && (self.active_sessions.contains(&session_id)
                || self.loading_sessions.contains(&session_id))
        {
            // cold-load 的 replay 通知在 response 前先到，仍要留给后续热恢复使用。
            self.buffers.entry(session_id).or_default().push(event);
        }
    }

    /// 是否处于用户已取消或批准 MCP 集合中的工具。
    fn is_approved_tool(&self, tool_name: &str) -> bool {
        tool_name == NOOP_TOOL || self.expected_tools.contains(tool_name)
    }

    /// 不在 prompt/load/catalog 中且超过 idle 阈值时，才能关闭该 scope 私有进程。
    fn should_idle_stop(&self) -> bool {
        self.initialized
            && self.pending.is_empty()
            && self.in_flight.is_empty()
            && self.loading_sessions.is_empty()
            && self.catalog_pending.is_empty()
            && self.deferred.is_empty()
            && self.last_activity.elapsed() >= self.idle_after
    }

    /// 异常 sidecar 不自动重拉，保留 dead actor 使随后 Send 明确返回 sidecar_unavailable。
    fn enter_dead(&mut self, error: KitError) {
        if self.dead {
            return;
        }
        self.dead = true;
        let _ = self.acp.shutdown();
        let _ = self.service.stop_scope(&self.scope_id);
        self.reject_pending(error);
    }

    /// 正常 idle 退出允许 Host 在下一条命令按旧 session cold-load 新起一代。
    fn idle_stop_and_exit(&mut self) {
        self.accepting.store(false, Ordering::Release);
        let _ = self.acp.shutdown();
        let _ = self.service.stop_scope(&self.scope_id);
        tracing::debug!(scope = %self.scope_id, "scope actor 已按 idle 策略停止");
    }

    /// 在关闭 stdin 前尽力取消正在运行的回合，避免侧车继续执行已被 Host 放弃的 prompt。
    fn cancel_in_flight_before_shutdown(&mut self) {
        let sessions = self.in_flight.keys().cloned().collect::<Vec<_>>();
        for session_id in sessions {
            // shutdown 路径不能因单个通知失败而跳过后续资源回收。
            let _ = self.acp.notify_validated(
                "session/cancel",
                json!({ "sessionId": session_id }),
                &self.policy,
            );
        }
    }

    /// 显式 shutdown 用于 Channel 全局换代和 Runtime Drop。
    fn shutdown_and_exit(&mut self) {
        self.accepting.store(false, Ordering::Release);
        self.cancel_in_flight_before_shutdown();
        let _ = self.acp.shutdown();
        let _ = self.service.stop_scope(&self.scope_id);
    }

    /// 死 actor 仍接收命令以 fail-closed；只有全局换代能创建同 scope 新代。
    fn reject_dead_command(&mut self, command: ActorCommand) {
        match command {
            ActorCommand::Shutdown { reply } => {
                self.shutdown_and_exit();
                let _ = reply.send(());
            }
            command => send_command_error(command, sidecar_unavailable("sidecar 不可用")),
        }
    }

    /// transport 死亡时释放所有还在等待真实结果的 Kit 调用，避免同步 dispatch 超时悬挂。
    fn reject_pending(&mut self, error: KitError) {
        while let Some(command) = self.deferred.pop_front() {
            send_command_error(command, error.clone());
        }
        for (_, mut waiting) in std::mem::take(&mut self.catalog_waiting) {
            while let Some(command) = waiting.pop_front() {
                send_command_error(command, error.clone());
            }
        }
        for (_, pending) in std::mem::take(&mut self.pending) {
            match pending {
                PendingRpc::NewSession { reply } | PendingRpc::ListSessions { reply } => {
                    let _ = reply.send(Err(error.clone()));
                }
                PendingRpc::Load {
                    after_load: Some(pending),
                    ..
                } => {
                    let _ = pending.reply.send(Err(error.clone()));
                }
                PendingRpc::Load {
                    after_load: None, ..
                }
                | PendingRpc::Prompt { .. }
                | PendingRpc::McpCatalog { .. } => {}
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
        .sender
        .send(make(reply))
        .map_err(|_| sidecar_unavailable("scope actor 已退出"))?;
    receiver
        .recv_timeout(DISPATCH_REPLY_TIMEOUT)
        .map_err(|_| sidecar_unavailable("等待 sidecar 回执超时"))?
}

/// 在 actor 异常终止或 dead 状态下统一回绝尚未完成的外部命令。
fn send_command_error(command: ActorCommand, error: KitError) {
    match command {
        ActorCommand::NewSession { reply }
        | ActorCommand::ListSessions { reply, .. }
        | ActorCommand::ResumeSession { reply, .. }
        | ActorCommand::Send { reply, .. }
        | ActorCommand::Cancel { reply, .. } => {
            let _ = reply.send(Err(error));
        }
        ActorCommand::Shutdown { reply } => {
            let _ = reply.send(());
        }
    }
}

/// 生成 HostPolicy：所有 `_meta` 许可显式按 method 列出，不能跨 method 泄漏。
fn host_policy(launched: &LaunchedScope, model_id: &str) -> Result<HostPolicy, KitError> {
    let cwd = std::fs::canonicalize(&launched.paths.workspace)
        .map_err(|_| sidecar_unavailable("scope workspace 不可用"))?;
    Ok(HostPolicy::new(cwd)
        .with_meta_key_for("initialize", "modelId")
        .with_meta_key_for("session/new", "modelId")
        .with_meta_key_for("session/load", "modelId")
        .with_meta_key_for("x.ai/mcp/list", "modelId")
        .with_meta_key_for("session/prompt", "promptId")
        .with_model_id(model_id.to_string()))
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
        || submission_id.is_empty()
        || text.is_empty()
        || text.chars().count() > 32_000
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

/// supervisor 不可用原因的稳定 wire 文本；仅平台分支可以进入该值。
fn unavailable_reason_name(reason: UnavailableReason) -> &'static str {
    match reason {
        UnavailableReason::SidecarHardeningUnavailable => "sidecar_hardening_unavailable",
    }
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
    let mut tools = BTreeSet::new();
    for server in servers {
        let server_name = server.get("name").and_then(Value::as_str)?;
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
            // 无副作用 noop 是 sidecar 内置全局工具名，不能错误加上 server 前缀。
            if name == NOOP_TOOL {
                tools.insert(name.to_string());
            } else {
                tools.insert(format!("{server_name}__{name}"));
            }
        }
    }
    Some(tools)
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
