//! efflab-agent-sidecar 库根。
//!
//! 里程碑：macOS isolated runtime integration POC。
//!
//! 职责（按开发计划 P0→P4 依次填充）：
//! - `sidecar_config`：CLI / SidecarConfig / ApprovedMcpConfig 解析与校验（P1）
//! - `hardening`：私有 GROK_HOME、fs2 独占锁、原子写、权威 config 渲染、env 卫生（P1）
//! - `toolset`：内置占位工具 `GrokBuild:efflab_noop` 与注册（P2）
//! - `host_contract`：Host 请求字段白名单校验（P3）
//!
//! 设计约束：不修改任何 `xai-grok-*` 核心 crate；stdout 仅承载 ACP JSON-RPC。

pub mod hardening;
/// 向下兼容既有 sidecar 测试与调用方的 Host 合同模块路径。
pub mod host_contract;
pub mod sidecar_config;
pub mod toolset;
