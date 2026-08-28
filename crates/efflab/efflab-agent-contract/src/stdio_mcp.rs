//! 最小 runtime 的 stdio MCP 拒绝合同。

use anyhow::bail;

use crate::{ApprovedMcpConfig, McpServerSpec};

/// 拒绝所有 stdio MCP，仅允许已审核的 HTTP MCP 继续通过。
pub fn deny_stdio_mcp(servers: &ApprovedMcpConfig) -> anyhow::Result<()> {
    for spec in servers.servers.values() {
        if matches!(spec, McpServerSpec::Stdio { .. }) {
            bail!("stdio_mcp_unavailable");
        }
    }

    Ok(())
}
