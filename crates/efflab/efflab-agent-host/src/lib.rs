//! Efflab Agent Kit Host Runtime 的 M0 骨架。
//!
//! 本 crate 冻结 L1 Kit 线协议、提供最小提交幂等语义与 ACP stdio 传输；不启动
//! sidecar，也不实现 LLM Channel 业务。

mod acp_runtime;
mod app_port;
mod config;
mod event_sink;
mod projector;
mod protocol;
mod runtime;
mod submission;
mod supervisor;

pub use acp_runtime::{AcpRuntime, Inbound, METHOD_NOT_FOUND, RequestId, RpcError, ValidatedReply};
pub use app_port::{
    ApprovedMcpSpec, HostApp, HostAppMentions, LlmChannelConfig, MentionId, ResolvedMention,
    ScopeId, SealedSecret, SecretGuard,
};
pub use config::HostRuntimeConfig;
pub use event_sink::{KitEventSink, ValidatedKitEventSink};
pub use projector::{ProjectError, Projector, apply_acp_notification};
pub use protocol::{
    Capability, CapabilityLimits, KIT_SCHEMA_VERSION, KitBlock, KitCommand, KitError,
    KitProductEvent, KitProductEventValidationError, KitReply, LlmChannelKind, LlmChannelView,
    Origin, SessionSummary, ToolStatus,
};
pub use runtime::HostRuntime;
pub use supervisor::{
    ChildEnvironment, ChildLifecycle, ChildLifecycleOps, ProcessSlotMetadata, ProcessSlotState,
    STDIN_CLOSE_GRACE, ScopePaths, ScopeSlot, Supervisor, SupervisorCapability, SupervisorError,
    TERMINATE_GRACE, UnavailableReason, capability, sanitize,
};
