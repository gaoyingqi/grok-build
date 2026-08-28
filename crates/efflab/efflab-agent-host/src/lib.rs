//! Efflab Agent Kit Host Runtime 的 M0 骨架。
//!
//! 本 crate 冻结 L1 Kit 线协议、提供最小提交幂等语义与 ACP stdio 传输；不启动
//! sidecar，也不实现 LLM Channel 业务。

mod acp_runtime;
mod app_port;
mod config;
mod event_sink;
mod llm_channel;
mod llm_loopback;
mod projector;
mod protocol;
mod runtime;
mod submission;
mod supervisor;

pub use acp_runtime::{
    AcpRuntime, Inbound, MAX_ACP_LINE_BYTES, METHOD_NOT_FOUND, RequestId, RpcError, ValidatedReply,
};
pub use app_port::{
    ApprovedMcpSpec, ApprovedMcpSpecV1, HostApp, HostAppMentions, LlmChannelConfig, LlmSecretSlot,
    MentionId, ResolvedMention, ScopeId, SealedSecret, SecretGuard,
};
pub use config::{HostRuntimeConfig, L3bRuntimeConfig};
pub use efflab_agent_contract::{ApprovedMcpConfig, McpServerSpec};
pub use event_sink::{KitEventSink, ValidatedKitEventSink};
pub use llm_channel::{
    ChannelChange, LlmChannelError, LlmChannelManager, LlmChannelService, SetLlmChannelRequest,
};
pub use llm_loopback::{
    BindingContext, BindingToken, BindingTokenRegistry, L3bLoopback, L3bLoopbackError,
    MAX_BINDING_RECORDS, MAX_L3B_REQUEST_BODY_BYTES,
};
pub use projector::{ProjectError, Projector, apply_acp_notification};
pub use protocol::{
    Capability, CapabilityLimits, KIT_SCHEMA_VERSION, KitBlock, KitCommand, KitError,
    KitProductEvent, KitProductEventValidationError, KitReply, LlmChannelKind, LlmChannelView,
    Origin, SessionSummary, ToolStatus,
};
pub use runtime::HostRuntime;
pub use supervisor::{
    ChildEnvironment, ChildLifecycle, ChildLifecycleOps, ProcessSlotMetadata, ProcessSlotState,
    STDIN_CLOSE_GRACE, ScopePaths, ScopeSlot, SidecarProcessInfo, Supervisor, SupervisorCapability,
    SupervisorError, TERMINATE_GRACE, UnavailableReason, capability, sanitize,
};
