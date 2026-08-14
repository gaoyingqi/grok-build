//! Efflab Agent Kit 的无运行时共享契约。
//!
//! 本 crate 只承载 Host 与 sidecar 共用的校验、DTO 和 TOML 渲染；不得引入
//! grok-shell、ACP runtime 或进程管理依赖。

pub mod host_contract;
pub mod mcp_config;
pub mod model;
pub mod render;

pub use host_contract::{HostPolicy, HostRejection, validate_host_request};
pub use mcp_config::{ApprovedMcpConfig, McpServerSpec};
pub use model::SidecarModelSpec;
pub use render::{render_authoritative_config, validate_authoritative_config};
