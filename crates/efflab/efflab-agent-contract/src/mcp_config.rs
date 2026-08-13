//! 经审核的 MCP 配置类型与 TOML 读写边界。
//!
//! 此模块不依赖 sidecar runtime；它只负责将有限 MCP TOML 转换成 Host 与
//! sidecar 可共享的受控 DTO，并拒绝不受支持的输入形状。

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

const MCP_SERVERS_KEY: &str = "mcp_servers";

/// 已通过输入边界校验、可写入 sidecar 私有配置的 MCP server 集合。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ApprovedMcpConfig {
    /// 以稳定顺序保存的 server 名与受控规格。
    pub servers: BTreeMap<String, McpServerSpec>,
}

impl ApprovedMcpConfig {
    /// 读取并校验唯一允许的 MCP TOML 输入格式。
    pub fn load(path: &Path, exec_root: Option<&Path>) -> Result<Self> {
        let config_path = canonicalize_file(path, "--mcp-config")?;
        let source = fs::read_to_string(&config_path)
            .with_context(|| format!("读取 MCP 配置文件失败: {}", config_path.display()))?;
        let document: toml::Value = toml::from_str(&source)
            .with_context(|| format!("解析 MCP TOML 失败: {}", config_path.display()))?;
        validate_top_level_keys(&document, &config_path)?;

        // 再次反序列化为窄结构；重复 TOML key 会在此之前由 TOML 解析器拒绝。
        let raw: RawMcpConfig = toml::from_str(&source)
            .with_context(|| format!("反序列化 MCP server 配置失败: {}", config_path.display()))?;
        let exec_root = exec_root
            .map(|root| canonicalize_directory(root, "--mcp-exec-root"))
            .transpose()?;

        let mut servers = BTreeMap::new();
        for (name, raw_spec) in raw.mcp_servers {
            if name.trim().is_empty() {
                bail!("MCP server 名称不能为空: {}", config_path.display());
            }

            let spec = McpServerSpec::from_raw(&name, raw_spec, exec_root.as_deref())?;
            if servers.insert(name.clone(), spec).is_some() {
                bail!("MCP server 名称重复: {name}");
            }
        }

        Ok(Self { servers })
    }

    /// 将已审核 DTO 写成唯一允许的 MCP TOML 表，供 Host 物化配置时复用。
    pub fn write_toml(&self) -> Result<String> {
        let mut document = toml::map::Map::new();
        let mut servers = toml::map::Map::new();

        for (name, spec) in &self.servers {
            if name.trim().is_empty() {
                bail!("MCP server 名称不能为空");
            }

            let mut table = toml::map::Map::new();
            match spec {
                McpServerSpec::Stdio { command, args } => {
                    if !command.is_absolute() {
                        bail!(
                            "受控 stdio MCP server '{name}' 的 command 必须为绝对路径: {}",
                            command.display()
                        );
                    }
                    let command = command.to_str().context(format!(
                        "受控 stdio MCP server '{name}' 的 command 不是 UTF-8 路径: {}",
                        command.display()
                    ))?;
                    table.insert(
                        "command".to_string(),
                        toml::Value::String(command.to_string()),
                    );
                    table.insert(
                        "args".to_string(),
                        toml::Value::Array(args.iter().cloned().map(toml::Value::String).collect()),
                    );
                }
                McpServerSpec::Http { url } => {
                    if url.trim().is_empty() {
                        bail!("受控 HTTP MCP server '{name}' 的 url 不能为空");
                    }
                    table.insert("url".to_string(), toml::Value::String(url.clone()));
                }
            }
            servers.insert(name.clone(), toml::Value::Table(table));
        }

        document.insert(MCP_SERVERS_KEY.to_string(), toml::Value::Table(servers));
        toml::to_string(&toml::Value::Table(document)).context("序列化已审核 MCP TOML 失败")
    }
}

/// 单个已审核 MCP server 的传输规格。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpServerSpec {
    /// 子进程 stdio MCP，只保留归一化后的可执行文件路径与参数。
    Stdio {
        /// 位于受控执行根目录内的绝对可执行文件路径。
        command: PathBuf,
        /// 原样传递给受控可执行文件的参数。
        args: Vec<String>,
    },
    /// 仅允许 loopback 地址的 HTTP MCP。
    Http {
        /// 已验证为 localhost 或 127.0.0.1 的 HTTP URL。
        url: String,
    },
}

impl McpServerSpec {
    /// 将 TOML 原始条目转换为经过安全校验的传输规格。
    fn from_raw(name: &str, raw: RawMcpServerSpec, exec_root: Option<&Path>) -> Result<Self> {
        if raw.env.is_some() {
            bail!("MCP server '{name}' 不允许 env 字段（阶段 0）");
        }

        match (raw.command, raw.url) {
            (Some(command), None) => {
                if command.trim().is_empty() {
                    bail!("stdio MCP server '{name}' 的 command 不能为空");
                }

                let command_path = Path::new(&command);
                if !command_path.is_absolute() {
                    bail!(
                        "stdio MCP server '{name}' 的 command 必须为绝对路径: {}",
                        command_path.display()
                    );
                }

                let exec_root = exec_root.context(format!(
                    "stdio MCP server '{name}' 需要提供 --mcp-exec-root"
                ))?;
                let command = dunce::canonicalize(command_path).with_context(|| {
                    format!(
                        "无法归一化 stdio MCP server '{name}' 的 command: {}",
                        command_path.display()
                    )
                })?;
                if !command.starts_with(exec_root) {
                    bail!(
                        "stdio MCP server '{name}' 的 command 不在 --mcp-exec-root 内: {}",
                        command.display()
                    );
                }

                // 目录虽然也可能位于受控根目录内，但不能作为待启动的可执行文件。
                let metadata = fs::metadata(&command).with_context(|| {
                    format!(
                        "无法读取 stdio MCP server '{name}' 的 command 元数据: {}",
                        command.display()
                    )
                })?;
                if !metadata.is_file() {
                    bail!(
                        "stdio MCP server '{name}' 的 command 必须指向常规文件: {}",
                        command.display()
                    );
                }

                Ok(Self::Stdio {
                    command,
                    args: raw.args,
                })
            }
            (None, Some(url)) => {
                if url.trim().is_empty() {
                    bail!("HTTP MCP server '{name}' 的 url 不能为空");
                }
                if !is_loopback_http_url(&url) {
                    bail!("HTTP MCP server '{name}' 的 url 必须使用 localhost 或 127.0.0.1: {url}");
                }

                Ok(Self::Http { url })
            }
            (Some(_), Some(_)) => {
                bail!("MCP server '{name}' 不能同时配置 command 与 url");
            }
            (None, None) => {
                bail!("MCP server '{name}' 必须配置 command 或 url");
            }
        }
    }
}

/// MCP TOML 根结构；顶层键在反序列化前通过 `validate_top_level_keys` 严格检查。
#[derive(Debug, Deserialize)]
struct RawMcpConfig {
    #[serde(default)]
    mcp_servers: BTreeMap<String, RawMcpServerSpec>,
}

/// 仅提取阶段 0 支持的字段，其他字段不会进入最终受控配置。
#[derive(Debug, Deserialize)]
struct RawMcpServerSpec {
    command: Option<String>,
    url: Option<String>,
    #[serde(default)]
    args: Vec<String>,
    env: Option<toml::Value>,
}

/// 要求路径为绝对路径，并使用 dunce 归一化以避免平台路径前缀差异。
fn canonicalize_path(path: &Path, argument_name: &str) -> Result<PathBuf> {
    if !path.is_absolute() {
        bail!("{argument_name} 必须为绝对路径: {}", path.display());
    }

    dunce::canonicalize(path)
        .with_context(|| format!("无法归一化 {argument_name}: {}", path.display()))
}

/// 要求路径存在且为目录，再返回其归一化结果。
fn canonicalize_directory(path: &Path, argument_name: &str) -> Result<PathBuf> {
    let canonical = canonicalize_path(path, argument_name)?;
    let metadata = fs::metadata(&canonical)
        .with_context(|| format!("读取 {argument_name} 元数据失败: {}", canonical.display()))?;
    if !metadata.is_dir() {
        bail!("{argument_name} 必须指向目录: {}", canonical.display());
    }
    Ok(canonical)
}

/// 要求路径存在且为常规文件，再返回其归一化结果。
fn canonicalize_file(path: &Path, argument_name: &str) -> Result<PathBuf> {
    let canonical = canonicalize_path(path, argument_name)?;
    let metadata = fs::metadata(&canonical)
        .with_context(|| format!("读取 {argument_name} 元数据失败: {}", canonical.display()))?;
    if !metadata.is_file() {
        bail!("{argument_name} 必须指向常规文件: {}", canonical.display());
    }
    Ok(canonical)
}

/// MCP 配置只能含有 `mcp_servers` 这一顶层键。
fn validate_top_level_keys(document: &toml::Value, config_path: &Path) -> Result<()> {
    let table = document.as_table().context("MCP TOML 根节点必须是表")?;
    for key in table.keys() {
        if key != MCP_SERVERS_KEY {
            bail!("MCP TOML 包含未知顶层键 '{key}': {}", config_path.display());
        }
    }
    Ok(())
}

/// 严格解析 HTTP URL 的 authority，避免把带前缀或 userinfo 的非 loopback 主机误判为本地。
/// 校验 HTTP MCP URL 是否严格指向 loopback host，供 sidecar 回归测试复用。
pub fn is_loopback_http_url(url: &str) -> bool {
    if url.bytes().any(|byte| byte.is_ascii_whitespace()) {
        return false;
    }

    let Some((scheme, remainder)) = url.split_once("://") else {
        return false;
    };
    if !(scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https")) {
        return false;
    }

    let authority_end = remainder.find(['/', '?', '#']).unwrap_or(remainder.len());
    let authority = &remainder[..authority_end];
    if authority.is_empty() || authority.contains('@') {
        return false;
    }

    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) => (host, Some(port)),
        None => (authority, None),
    };
    if let Some(port) = port
        && port.parse::<u16>().is_err()
    {
        return false;
    }

    host == "127.0.0.1" || host.eq_ignore_ascii_case("localhost")
}
