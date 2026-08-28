//! Efflab Agent Kit 的无运行时共享契约。
//!
//! 本 crate 只承载 Host 与 sidecar 共用的校验、DTO 和 TOML 渲染；不得引入
//! grok-shell、ACP runtime 或进程管理依赖。

pub mod host_contract;
pub mod mcp_config;
pub mod model;
pub mod render;
pub mod stdio_mcp;

pub use host_contract::{
    HOST_ACP_PROTOCOL_VERSION, HostPolicy, HostRejection, PromptTextRejection,
    validate_host_request, validate_prompt_text,
};
pub use mcp_config::{ApprovedMcpConfig, McpServerSpec};
pub use model::SidecarModelSpec;
pub use render::{render_authoritative_config, validate_authoritative_config};
pub use stdio_mcp::deny_stdio_mcp;
