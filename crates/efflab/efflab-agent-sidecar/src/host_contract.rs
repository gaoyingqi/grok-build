//! Sidecar Host 合同模块的向下兼容 re-export。
//!
//! 共享实现与单元测试已迁至 `efflab-agent-contract`；保留此文件路径，避免既有
//! sidecar 依赖方在 crate 边界拆分时发生路径破坏。

pub use efflab_agent_contract::host_contract::*;
