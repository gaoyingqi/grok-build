//! 集成测试共享基础设施。
//!
//! - [`process`]：sidecar 进程监督器（spawn / stdout/stderr 分离读取 / 超时收尾）。
//! - [`acp_client`]：最小 ACP stdio JSON-RPC 客户端。
//! - [`fixtures`]：测试资源路径定位（mock MCP 脚本等）。

pub mod acp_client;
pub mod process;

/// 定位 `tests/fixtures/` 下的测试资源绝对路径。
pub fn fixture_path(name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}
