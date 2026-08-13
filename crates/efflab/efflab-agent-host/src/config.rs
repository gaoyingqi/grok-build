//! Host Runtime 的不透明运行配置。

use std::path::PathBuf;
use std::time::Duration;

/// 产品提供的运行时路径与闲置策略；Host 后续会在 `home_root` 下拼接 app/scope。
#[derive(Debug, Clone)]
pub struct HostRuntimeConfig {
    /// 产品 App Data 根目录；不得预先假定已经包含 app_id。
    pub home_root: PathBuf,
    /// sidecar 可执行文件路径；本任务不启动该进程。
    pub sidecar_bin: PathBuf,
    /// 受控 MCP 可执行文件根目录；本任务不读取该目录。
    pub mcp_exec_root: PathBuf,
    /// 空闲回收阈值；本任务仅保留配置形状。
    pub idle_after: Duration,
}
