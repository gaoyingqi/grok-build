//! Host Runtime 的不透明运行配置。

use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;
use std::time::Duration;

/// 进程级 L3b 回环监听与用户上游连接配置。
#[derive(Debug, Clone)]
pub struct L3bRuntimeConfig {
    /// 只允许 IPv4 `127.0.0.1` 或 IPv6 `::1`；禁止公开或任意地址监听。
    pub bind_addr: IpAddr,
    /// `0` 表示让操作系统分配进程级 ephemeral port。
    pub port: u16,
    /// 兼容性开关；不限制用户填写的上游 URL。
    pub allow_loopback_llm: bool,
}

impl Default for L3bRuntimeConfig {
    /// 默认使用 IPv4 回环与 ephemeral port，避免端口冲突和公开监听。
    fn default() -> Self {
        Self {
            bind_addr: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port: 0,
            allow_loopback_llm: false,
        }
    }
}

/// 产品提供的运行时路径、闲置策略与 L3b 注入点；Host 后续会在 `home_root` 下拼接 app/scope。
#[derive(Debug, Clone)]
pub struct HostRuntimeConfig {
    /// 产品 App Data 根目录；不得预先假定已经包含 app_id。
    pub home_root: PathBuf,
    /// sidecar 可执行文件路径；Task 7 使用此路径执行受控子进程。
    pub sidecar_bin: PathBuf,
    /// sidecar stderr 独立日志文件的绝对路径。
    ///
    /// 由嵌入产品注入，Host 不内置任何产品日志目录或文件名。
    /// 以追加方式打开并重定向 child stderr；stdout 仍只承载 ACP JSON-RPC。
    pub sidecar_log_path: PathBuf,
    /// 受控 MCP 可执行文件根目录；Task 7b 才会用于实际 MCP 配置。
    pub mcp_exec_root: PathBuf,
    /// 空闲回收阈值；Task 7b 才会接入完整 idle 状态机。
    pub idle_after: Duration,
    /// L3b 回环监听与用户上游连接配置。
    pub l3b: L3bRuntimeConfig,
    /// 产品注入的系统提示词；空字符串表示 sidecar 使用内置最小提示词。
    pub system_prompt: String,
}
