//! efflab-agent-sidecar 的最小运行时库。
//!
//! `sidecar_config` 负责 v1 CLI/config 校验，`hardening` 负责 Unix 私有 home、锁和环境
//! allowlist；`runtime` 负责 ACP stdio 生命周期，`acp_agent` 负责 session/prompt 边界，
//! `session_store` 负责 v1 journal，`model_client` 负责受控 loopback L3b，`turn_loop` 负责
//! 可取消的有限模型/工具回合。replay update 在 load response 前经同一 gateway 顺序交付，
//! active prompt 的取消与 terminal journal 通过共享 control 线性化；stdout 仅承载 ACP。
//! `host_contract` 保留共享 Host 合同的 re-export。

pub mod acp_agent;
pub mod hardening;
/// 复用无 grok runtime 的 Host 合同模块路径。
pub mod host_contract;
pub mod mcp_client;
pub mod model_client;
pub mod observability;
pub mod runtime;
pub mod session_store;
pub mod sidecar_config;
#[cfg(debug_assertions)]
pub(crate) mod test_seam;
pub mod turn_loop;

/// 当前最小 ACP turn loop 使用的编译期系统提示词。
pub const MINIMAL_SYSTEM_PROMPT: &str = include_str!("../assets/efflab-minimal-system-prompt.md");
