//! Sidecar 启动前的受控 CLI 与 MCP 配置解析。
//!
//! 本模块只接受 sidecar 私有配置；不会读取通用 `GROK_HOME`，也不会合并
//! 外部 Grok 配置，从而在初始化 shell runtime 前建立最小信任边界。

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::Parser;
use serde::Deserialize;

const MCP_SERVERS_KEY: &str = "mcp_servers";

/// Sidecar 的固定命令行参数。
///
/// `grok_home` 只绑定 `EFFLAB_GROK_HOME`；故意不声明通用 `GROK_HOME` 环境变量，
/// 避免继承用户的全局 Grok 配置目录。
#[derive(Debug, Parser)]
#[command(
    name = "efflab-agent-sidecar",
    version,
    about = "Efflab 隔离 ACP stdio sidecar"
)]
pub struct Cli {
    /// 使用 ACP stdio 传输。
    #[arg(long, default_value_t = true)]
    pub stdio: bool,

    /// Sidecar 私有 GROK_HOME 的绝对路径，也可由 EFFLAB_GROK_HOME 提供。
    #[arg(long, env = "EFFLAB_GROK_HOME", value_name = "ABS")]
    pub grok_home: Option<PathBuf>,

    /// Host 创建的隔离会话目录绝对路径。
    #[arg(long, value_name = "ABS")]
    pub session_cwd: PathBuf,

    /// 经 Host 审核的 MCP TOML 文件绝对路径。
    #[arg(long, value_name = "ABS_TOML")]
    pub mcp_config: Option<PathBuf>,

    /// stdio MCP 可执行文件所在的受控根目录绝对路径。
    #[arg(long, value_name = "ABS_DIR")]
    pub mcp_exec_root: Option<PathBuf>,
}

/// 完成路径归一化和 MCP 白名单校验后的启动配置。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SidecarConfig {
    /// 是否使用固定的 ACP stdio 传输。
    pub stdio: bool,
    /// 归一化后的私有 GROK_HOME。
    pub grok_home: PathBuf,
    /// 归一化后的隔离会话目录。
    pub session_cwd: PathBuf,
    /// 仅包含已经审核通过的 MCP server 配置。
    pub mcp_config: ApprovedMcpConfig,
}

impl SidecarConfig {
    /// 从进程命令行解析并校验 sidecar 启动配置。
    pub fn from_cli() -> Result<Self> {
        Self::from_parsed_cli(Cli::parse())
    }

    /// 校验已经由 clap 解析的参数，供进程入口和文件内测试复用。
    pub fn from_parsed_cli(cli: Cli) -> Result<Self> {
        let session_cwd = canonicalize_directory(&cli.session_cwd, "--session-cwd")?;
        let grok_home_input = cli
            .grok_home
            .as_deref()
            .context("缺少 --grok-home；请传入绝对路径或设置 EFFLAB_GROK_HOME")?;
        let grok_home = canonicalize_private_home(grok_home_input, "--grok-home")?;

        reject_global_grok_home(&grok_home)?;
        if grok_home.starts_with(&session_cwd) {
            bail!(
                "拒绝位于隔离 workspace 内的 --grok-home: {}",
                grok_home.display()
            );
        }

        let mcp_exec_root = cli
            .mcp_exec_root
            .as_deref()
            .map(|path| canonicalize_directory(path, "--mcp-exec-root"))
            .transpose()?;
        let mcp_config = match cli.mcp_config.as_deref() {
            Some(path) => ApprovedMcpConfig::load(path, mcp_exec_root.as_deref())?,
            None => ApprovedMcpConfig::default(),
        };

        Ok(Self {
            stdio: cli.stdio,
            grok_home,
            session_cwd,
            mcp_config,
        })
    }
}

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

/// 归一化尚未创建的私有 home，同时保留已存在祖先目录的真实路径。
///
/// 私有 home 会在后续 hardening 阶段创建，因此此处不能把不存在本身视为错误；
/// 但会拒绝 `..`，并只在最邻近的既有祖先上调用 `dunce::canonicalize`，避免
/// 以词法替换跨越符号链接。
fn canonicalize_private_home(path: &Path, argument_name: &str) -> Result<PathBuf> {
    if !path.is_absolute() {
        bail!("{argument_name} 必须为绝对路径: {}", path.display());
    }
    if path
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        bail!("{argument_name} 不允许包含 ..: {}", path.display());
    }

    let mut current = path;
    let mut missing_components = Vec::new();
    loop {
        match dunce::canonicalize(current) {
            Ok(mut canonical) => {
                for component in missing_components.iter().rev() {
                    canonical.push(component);
                }
                return Ok(canonical);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let component = current.file_name().context(format!(
                    "无法为 {argument_name} 找到既有祖先目录: {}",
                    path.display()
                ))?;
                missing_components.push(component.to_os_string());
                current = current.parent().context(format!(
                    "无法为 {argument_name} 找到既有祖先目录: {}",
                    path.display()
                ))?;
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("无法归一化 {argument_name}: {}", current.display()));
            }
        }
    }
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

/// 拒绝默认用户 Grok 目录及其任何子目录，避免继承全局配置层。
fn reject_global_grok_home(grok_home: &Path) -> Result<()> {
    if grok_home
        .components()
        .any(|component| component.as_os_str() == ".grok")
    {
        bail!(
            "拒绝用户全局 Grok 配置目录作为 --grok-home: {}",
            grok_home.display()
        );
    }
    Ok(())
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
fn is_loopback_http_url(url: &str) -> bool {
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use super::{ApprovedMcpConfig, Cli, McpServerSpec, SidecarConfig};

    /// 构造最小合法 CLI，单个负例仅覆盖自己要验证的字段。
    fn cli(grok_home: PathBuf, session_cwd: &Path) -> Cli {
        Cli {
            stdio: true,
            grok_home: Some(grok_home),
            session_cwd: session_cwd.to_path_buf(),
            mcp_config: None,
            mcp_exec_root: None,
        }
    }

    /// 写入临时 MCP TOML，确保测试文件路径为绝对路径且实际存在。
    fn write_mcp_config(directory: &Path, content: &str) -> PathBuf {
        let path = directory.join("approved-mcp.toml");
        fs::write(&path, content).expect("写入临时 MCP TOML 应成功");
        path
    }

    #[test]
    fn relative_grok_home_is_rejected() {
        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let session_cwd = temporary.path().join("session");
        fs::create_dir(&session_cwd).expect("创建会话目录应成功");

        let result =
            SidecarConfig::from_parsed_cli(cli(PathBuf::from("relative-grok"), &session_cwd));

        assert!(result.is_err(), "相对 --grok-home 必须被拒绝");
    }

    #[test]
    fn tilde_global_grok_home_is_rejected() {
        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let session_cwd = temporary.path().join("session");
        fs::create_dir(&session_cwd).expect("创建会话目录应成功");

        let result = SidecarConfig::from_parsed_cli(cli(PathBuf::from("~/.grok"), &session_cwd));

        assert!(result.is_err(), "~/.grok 必须被拒绝");
    }

    #[test]
    fn global_grok_directory_component_is_rejected() {
        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let global_home = temporary.path().join("user").join(".grok");
        let session_cwd = temporary.path().join("session");
        fs::create_dir_all(&global_home).expect("创建模拟全局目录应成功");
        fs::create_dir(&session_cwd).expect("创建会话目录应成功");

        let result = SidecarConfig::from_parsed_cli(cli(global_home, &session_cwd));

        assert!(result.is_err(), "任何 .grok 全局目录都必须被拒绝");
    }

    #[test]
    fn nonexistent_private_grok_home_is_canonicalized() {
        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let session_cwd = temporary.path().join("session");
        fs::create_dir(&session_cwd).expect("创建会话目录应成功");
        let private_home = temporary.path().join("private-grok");

        let config = SidecarConfig::from_parsed_cli(cli(private_home, &session_cwd))
            .expect("尚未创建的私有 GROK_HOME 应可通过校验");

        let expected = dunce::canonicalize(temporary.path())
            .expect("临时目录应可归一化")
            .join("private-grok");
        assert_eq!(config.grok_home, expected);
    }

    #[test]
    fn grok_home_inside_session_workspace_is_rejected() {
        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let session_cwd = temporary.path().join("session");
        let private_home = session_cwd.join("private-grok");
        fs::create_dir_all(&private_home).expect("创建 workspace 内目录应成功");

        let result = SidecarConfig::from_parsed_cli(cli(private_home, &session_cwd));

        assert!(result.is_err(), "workspace 内的 --grok-home 必须被拒绝");
    }

    #[test]
    fn duplicate_mcp_server_name_is_rejected() {
        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let path = write_mcp_config(
            temporary.path(),
            "[mcp_servers.echo]\ncommand = \"/bin/echo\"\n\
             [mcp_servers.echo]\ncommand = \"/bin/echo\"\n",
        );

        let result = ApprovedMcpConfig::load(&path, None);

        assert!(result.is_err(), "重复 MCP server 名称必须被拒绝");
    }

    #[test]
    fn relative_stdio_command_is_rejected() {
        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let exec_root = temporary.path().join("executables");
        fs::create_dir(&exec_root).expect("创建执行根目录应成功");
        let path = write_mcp_config(
            temporary.path(),
            "[mcp_servers.echo]\ncommand = \"bin/echo\"\n",
        );

        let result = ApprovedMcpConfig::load(&path, Some(&exec_root));

        assert!(result.is_err(), "相对 stdio command 必须被拒绝");
    }

    #[test]
    fn non_loopback_http_server_is_rejected() {
        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let path = write_mcp_config(
            temporary.path(),
            "[mcp_servers.remote]\nurl = \"https://example.com/mcp\"\n",
        );

        let result = ApprovedMcpConfig::load(&path, None);

        assert!(result.is_err(), "非 loopback HTTP URL 必须被拒绝");
    }

    #[test]
    fn unknown_top_level_key_is_rejected() {
        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let path = write_mcp_config(temporary.path(), "[untrusted]\ncommand = \"/bin/echo\"\n");

        let result = ApprovedMcpConfig::load(&path, None);

        assert!(result.is_err(), "未知顶层键必须被拒绝");
    }

    #[test]
    fn stdio_server_requires_exec_root() {
        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let command = temporary.path().join("mcp-server");
        fs::write(&command, "#!/bin/sh\n").expect("写入临时命令应成功");
        let path = write_mcp_config(
            temporary.path(),
            &format!("[mcp_servers.local]\ncommand = {:?}\n", command),
        );

        let result = ApprovedMcpConfig::load(&path, None);

        assert!(result.is_err(), "stdio MCP 缺少执行根目录必须被拒绝");
    }

    #[test]
    fn env_field_is_rejected() {
        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let exec_root = temporary.path().join("executables");
        fs::create_dir(&exec_root).expect("创建执行根目录应成功");
        let command = exec_root.join("mcp-server");
        fs::write(&command, "#!/bin/sh\n").expect("写入临时命令应成功");
        let path = write_mcp_config(
            temporary.path(),
            &format!(
                "[mcp_servers.local]\ncommand = {:?}\nenv = {{ TOKEN = \"secret\" }}\n",
                command
            ),
        );

        let result = ApprovedMcpConfig::load(&path, Some(&exec_root));

        assert!(result.is_err(), "阶段 0 的 env 字段必须被拒绝");
    }

    #[test]
    fn approved_stdio_and_loopback_http_servers_are_retained() {
        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let exec_root = temporary.path().join("executables");
        fs::create_dir(&exec_root).expect("创建执行根目录应成功");
        let command = exec_root.join("mcp-server");
        fs::write(&command, "#!/bin/sh\n").expect("写入临时命令应成功");
        let path = write_mcp_config(
            temporary.path(),
            &format!(
                "[mcp_servers.stdio]\ncommand = {command:?}\nargs = [\"--serve\"]\n\
                 [mcp_servers.http]\nurl = \"http://localhost:8123/mcp\"\n"
            ),
        );

        let approved = ApprovedMcpConfig::load(&path, Some(&exec_root))
            .expect("受控 stdio 与 loopback HTTP MCP 应通过校验");

        let Some(McpServerSpec::Stdio {
            command: approved_command,
            args,
        }) = approved.servers.get("stdio")
        else {
            panic!("stdio MCP 应保留为 Stdio 规格");
        };
        assert_eq!(approved_command, &command);
        assert_eq!(args.len(), 1);
        assert_eq!(args[0], "--serve");
        assert!(matches!(
            approved.servers.get("http"),
            Some(McpServerSpec::Http { url }) if url == "http://localhost:8123/mcp"
        ));
    }
}
