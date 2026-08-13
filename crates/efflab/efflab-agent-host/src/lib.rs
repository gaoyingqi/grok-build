//! Efflab Agent Kit Host Runtime 的 M0 骨架。
//!
//! 本 crate 冻结 L1 Kit 线协议并提供最小的提交幂等语义；不连接 ACP、不启动
//! sidecar，也不实现 LLM Channel 业务。

mod app_port;
mod config;
mod event_sink;
mod protocol;
mod runtime;
mod submission;

pub use app_port::{
    ApprovedMcpSpec, HostApp, HostAppMentions, LlmChannelConfig, MentionId, ResolvedMention,
    ScopeId, SealedSecret, SecretGuard,
};
pub use config::HostRuntimeConfig;
pub use event_sink::KitEventSink;
pub use protocol::{
    Capability, CapabilityLimits, KIT_SCHEMA_VERSION, KitBlock, KitCommand, KitError,
    KitProductEvent, KitReply, LlmChannelKind, LlmChannelView, Origin, SessionSummary, ToolStatus,
};
pub use runtime::HostRuntime;
