//! HostRuntime 的 M0 dispatch 骨架。
//!
//! 本任务只有 Send 走进程内 SubmissionMap；其余命令统一返回结构化 unsupported。
//! 不连接 ACP、不启动 sidecar、不读取或写入产品配置。

use std::sync::Mutex;

use crate::submission::{SubmissionDecision, SubmissionMap};
use crate::{HostApp, HostRuntimeConfig, KitCommand, KitError, KitEventSink, KitReply};

/// 产品唯一调用入口的最小运行时状态。
pub struct HostRuntime {
    /// 产品领域端口；本任务保留以冻结构造 API，不调用其业务方法。
    _app: Box<dyn HostApp>,
    /// 产品事件运输端口；本任务不发射事件。
    _sink: Box<dyn KitEventSink>,
    /// 运行配置；本任务不访问其中路径。
    _cfg: HostRuntimeConfig,
    /// 进程内 Send 幂等边界，必须跨多次 dispatch 保持。
    submissions: Mutex<SubmissionMap>,
}

impl HostRuntime {
    /// 构造进程内单例运行时；调用方应在 App 启动或测试 setup 时只创建一次。
    pub fn new(
        app: impl HostApp + 'static,
        sink: impl KitEventSink + 'static,
        cfg: HostRuntimeConfig,
    ) -> Self {
        Self {
            _app: Box::new(app),
            _sink: Box::new(sink),
            _cfg: cfg,
            submissions: Mutex::new(SubmissionMap::default()),
        }
    }

    /// 分派 Kit 命令。
    ///
    /// M0 仅记录 Send 的稳定提交指纹。所有其它命令故意返回结构化 unsupported，
    /// 避免在 Task 7b 之前错误接入产品路径。
    pub fn dispatch(&self, cmd: KitCommand) -> Result<KitReply, KitError> {
        match cmd {
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
            KitCommand::GetCapability
            | KitCommand::Cancel { .. }
            | KitCommand::NewSession { .. }
            | KitCommand::ListSessions { .. }
            | KitCommand::ResumeSession { .. }
            | KitCommand::GetLlmChannelView
            | KitCommand::SetLlmChannel { .. }
            | KitCommand::Unknown { .. } => Err(KitError::non_retryable(
                "unsupported",
                "该 Kit 命令在 HostRuntime skeleton 中尚未实现",
            )),
        }
    }

    /// 对 Send 执行唯一的 M0 行为：同指纹幂等、异指纹 fail-closed。
    fn dispatch_send(
        &self,
        scope_id: String,
        session_id: String,
        submission_id: String,
        text: String,
        mentions: Vec<crate::MentionId>,
    ) -> Result<KitReply, KitError> {
        let decision = self
            .submissions
            .lock()
            .map_err(|_| KitError::non_retryable("unsupported", "提交映射不可用"))?
            .record(&scope_id, &session_id, &submission_id, &text, &mentions);

        match decision {
            SubmissionDecision::Accepted { turn_id } => Ok(KitReply::Send {
                accepted: true,
                duplicate: false,
                session_id,
                turn_id,
                submission_id,
            }),
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
        }
    }
}
