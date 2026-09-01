//! 集成测试共享基础设施。
//!
//! - [`process`]：sidecar 进程监督器（spawn / stdout/stderr 分离读取 / TERM→KILL 超时收尾）。
//! - [`acp_client`]：最小 ACP stdio JSON-RPC 客户端（保留 stdout 原始行）。
//! - [`fixtures`]：测试资源路径定位（mock MCP 脚本等）。

// 本模块是共享测试基础设施：每个集成测试 target 独立编译本模块，
// 某项能力（如 raw_lines / fixture_path）不被某个 target 使用时属于正常现象。
#![allow(dead_code)]

pub mod acp_client;
pub mod mock_l3b;
pub mod process;

/// 定位 `tests/fixtures/` 下的测试资源绝对路径。
pub fn fixture_path(name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}
