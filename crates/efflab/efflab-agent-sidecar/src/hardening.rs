//! Sidecar 启动前的私有文件系统与环境加固。
//!
//! 本模块只能在任何 xai shell API、Tokio runtime 或其他可能触发
//! `xai_grok_config::grok_home()` 的代码之前调用。这样 `GROK_HOME` 的
//! `OnceLock` 才会缓存本 sidecar 明确指定的私有目录，而不会继承用户环境。

use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use fs2::FileExt;

use crate::sidecar_config::{ApprovedMcpConfig, McpServerSpec};

/// 私有目录的 Unix 权限：仅当前用户可访问。
const DIRECTORY_MODE: u32 = 0o700;
/// 私有文件的 Unix 权限：仅当前用户可读写。
const FILE_MODE: u32 = 0o600;
/// 同一私有 home 的 sidecar 进程互斥锁文件名。
const HOME_LOCK_FILENAME: &str = ".efflab-sidecar.lock";
/// 私有 home 中不得存在的上游策略层；其优先级可能覆盖本模块生成的配置。
const FORBIDDEN_PRIVATE_POLICY_FILES: [&str; 2] = ["managed_config.toml", "requirements.toml"];
/// 物化后的固定 AgentDefinition 文件名。
const DEFAULT_AGENT_FILENAME: &str = "efflab-default.md";
/// 物化 AgentDefinition 与权威配置中使用的固定 agent 名称。
const DEFAULT_AGENT_NAME: &str = "efflab-default";
/// 编译期嵌入的密封默认 AgentDefinition，运行时绝不从用户目录读取它。
const DEFAULT_AGENT_DEFINITION: &str = include_str!("../assets/efflab-default-agent.md");

/// `VendorCompat` 的全部供应商字段，必须与上游 compat 类型保持同步。
const COMPAT_VENDORS: [&str; 3] = ["claude", "cursor", "codex"];
/// `VendorCompat` 的全部 surface 字段，默认均为开启，故必须逐项显式关闭。
const COMPAT_SURFACES: [&str; 6] = ["skills", "rules", "agents", "mcps", "hooks", "sessions"];

/// 不允许继承的非 compat 环境变量。
const SANITIZED_ENV_VARS: [&str; 5] = [
    "GROK_EXTERNAL_OTEL",
    "GROK_SUBAGENTS",
    "GROK_STORAGE_MODE",
    "GROK_MANAGED_MCPS_ENABLED",
    "GROK_MANAGED_MCP_GATEWAY_TOOLS_ENABLED",
];

/// `COMPAT_CELLS` 当前登记的全部 18 个环境变量。
///
/// 此列表依据 `xai-grok-tools/src/types/compat.rs` 的 `COMPAT_CELLS` 固定，
/// 不使用宽泛的 `GROK_*` 黑名单，以免删除无关启动配置。
const COMPAT_ENV_VARS: [&str; 18] = [
    "GROK_CURSOR_SKILLS_ENABLED",
    "GROK_CURSOR_RULES_ENABLED",
    "GROK_CURSOR_AGENTS_ENABLED",
    "GROK_CURSOR_MCPS_ENABLED",
    "GROK_CURSOR_HOOKS_ENABLED",
    "GROK_CURSOR_SESSIONS_ENABLED",
    "GROK_CLAUDE_SKILLS_ENABLED",
    "GROK_CLAUDE_RULES_ENABLED",
    "GROK_CLAUDE_AGENTS_ENABLED",
    "GROK_CLAUDE_MCPS_ENABLED",
    "GROK_CLAUDE_HOOKS_ENABLED",
    "GROK_CLAUDE_SESSIONS_ENABLED",
    "GROK_CODEX_SKILLS_ENABLED",
    "GROK_CODEX_RULES_ENABLED",
    "GROK_CODEX_AGENTS_ENABLED",
    "GROK_CODEX_MCPS_ENABLED",
    "GROK_CODEX_HOOKS_ENABLED",
    "GROK_CODEX_SESSIONS_ENABLED",
];

/// 创建并校验仅属于 sidecar 的私有 `GROK_HOME`。
///
/// 本函数只使用传入的路径，刻意不读取通用 `GROK_HOME` 环境变量。目录会递归
/// 创建并收紧为 `0700`；若已存在任一私有 managed/requirements 策略层，则拒绝
/// 启动，避免其覆盖权威 `config.toml` 的安全字段。
pub fn prepare_private_home(grok_home: &Path) -> Result<()> {
    require_absolute_path(grok_home, "私有 GROK_HOME")?;
    create_private_directory(grok_home)?;
    reject_private_policy_layers(grok_home)
}

/// 获取私有 home 的非阻塞 fs2 独占锁。
///
/// 返回值是承载锁的 `std::fs::File`（`fs2` 提供的是 `FileExt` 扩展 trait）。
/// 调用方必须将其保留到进程退出；File drop 时锁自动释放。同一 home 已被另一
/// sidecar 进程锁定时，本函数立即失败而不会等待。
pub fn acquire_home_lock(grok_home: &Path) -> Result<File> {
    prepare_private_home(grok_home)?;

    let lock_path = grok_home.join(HOME_LOCK_FILENAME);
    reject_symlink_if_present(&lock_path, "私有 home 锁文件")?;
    let lock_file = open_private_file(&lock_path)?;

    FileExt::try_lock_exclusive(&lock_file).with_context(|| {
        format!(
            "拒绝并发启动：私有 GROK_HOME 已被另一 sidecar 占用: {}",
            grok_home.display()
        )
    })?;

    Ok(lock_file)
}

/// 以临时文件、文件同步、原子改名和父目录同步的顺序覆盖私有文件。
///
/// 临时文件始终创建在目标文件所在目录，因此 rename 不跨文件系统。函数从不读取
/// 或合并旧内容；成功返回时目标文件权限为 `0600`。当前 POC 仅支持 Unix/macOS，
/// 其他平台会 fail-closed。
pub fn atomic_write_private(path: &Path, content: &[u8]) -> Result<()> {
    require_absolute_path(path, "原子写目标")?;
    if path.file_name().is_none() {
        bail!("原子写目标必须是文件路径: {}", path.display());
    }

    let parent = path.parent().context("原子写目标缺少父目录")?;
    ensure_real_directory(parent, "原子写目标父目录")?;

    #[cfg(not(unix))]
    {
        let _ = content;
        bail!("私有原子写仅支持 Unix/macOS 文件权限模型");
    }

    #[cfg(unix)]
    {
        // 临时文件与目标文件同目录，rename 才具备本文件系统内的原子替换语义。
        let mut temporary = tempfile::NamedTempFile::new_in(parent)
            .with_context(|| format!("无法在私有目录创建原子写临时文件: {}", parent.display()))?;
        set_private_permissions(temporary.path(), FILE_MODE, "原子写临时文件")?;
        let temporary_path = temporary.path().to_path_buf();

        {
            let temporary_file = temporary.as_file_mut();
            temporary_file
                .write_all(content)
                .with_context(|| format!("写入原子写临时文件失败: {}", temporary_path.display()))?;
            temporary_file
                .sync_all()
                .with_context(|| format!("同步原子写临时文件失败: {}", temporary_path.display()))?;
        }

        fs::rename(&temporary_path, path).with_context(|| {
            format!(
                "原子替换私有文件失败: {} -> {}",
                temporary_path.display(),
                path.display()
            )
        })?;
        sync_parent_directory(parent)?;

        Ok(())
    }
}

/// 将编译期嵌入的默认 AgentDefinition 物化到私有 home。
///
/// 文件被原子覆盖到 `GROK_HOME/agents/efflab-default.md`，并返回已 canonicalize
/// 的绝对路径。该路径可同时供 `[agent].definition`、`agent_profile_path` 与
/// `GROK_AGENT` 使用，避免任何用户级 agent discovery。
pub fn materialize_agent_definition(grok_home: &Path) -> Result<PathBuf> {
    prepare_private_home(grok_home)?;
    let canonical_home = dunce::canonicalize(grok_home)
        .with_context(|| format!("无法归一化私有 GROK_HOME: {}", grok_home.display()))?;
    let agents_directory = canonical_home.join("agents");
    create_private_directory(&agents_directory)?;

    let agent_definition_path = agents_directory.join(DEFAULT_AGENT_FILENAME);
    atomic_write_private(&agent_definition_path, DEFAULT_AGENT_DEFINITION.as_bytes())?;

    Ok(agent_definition_path)
}

/// 完整渲染 sidecar 唯一权威的 `config.toml` 文本。
///
/// 函数不读取旧配置，因此调用方每次都可使用 [`atomic_write_private`] 覆盖
/// `GROK_HOME/config.toml`，不会继承任意旧字段。`mcp` 只能传入
/// `crate::sidecar_config::ApprovedMcpConfig`：其 `servers` 中每个条目必须已经由
/// sidecar 配置边界校验；stdio 条目写入 `command` 和 `args`，HTTP 条目写入 `url`。
pub fn render_authoritative_config(
    grok_home: &Path,
    agent_def_path: &Path,
    mcp: Option<&ApprovedMcpConfig>,
) -> Result<String> {
    require_absolute_path(grok_home, "私有 GROK_HOME")?;
    require_absolute_path(agent_def_path, "物化 AgentDefinition")?;
    let agent_definition = path_to_utf8(agent_def_path, "物化 AgentDefinition")?;

    // 所有默认开启的 compat cell 均在同一 `[compat]` 表中逐项关闭。
    let mut rendered = String::from("[features]\nremote_fetch = false\n\n[compat]\n");
    for vendor in COMPAT_VENDORS {
        for surface in COMPAT_SURFACES {
            rendered.push_str(vendor);
            rendered.push('.');
            rendered.push_str(surface);
            rendered.push_str(" = false\n");
        }
    }

    rendered.push_str("\n[subagents]\nenabled = false\n");
    rendered.push_str("\n[managed_mcps]\nenabled = false\ngateway_tools_enabled = false\n");
    rendered.push_str("\n[memory]\nenabled = false\n");
    rendered.push_str("\n[skills]\npaths = []\n");
    rendered.push_str("\n[agent]\nname = ");
    rendered.push_str(&toml_string_literal(DEFAULT_AGENT_NAME));
    rendered.push_str("\ndefinition = ");
    rendered.push_str(&toml_string_literal(&agent_definition));
    rendered.push_str("\n\n[mcp_servers]\n");

    if let Some(approved_mcp) = mcp {
        for (name, server) in &approved_mcp.servers {
            if name.trim().is_empty() {
                bail!("受控 MCP server 名称不能为空");
            }

            rendered.push('\n');
            rendered.push_str("[mcp_servers.");
            rendered.push_str(&toml_key_literal(name));
            rendered.push_str("]\n");

            match server {
                McpServerSpec::Stdio { command, args } => {
                    if !command.is_absolute() {
                        bail!(
                            "受控 stdio MCP server '{name}' 的 command 必须为绝对路径: {}",
                            command.display()
                        );
                    }

                    let command = path_to_utf8(command, "受控 stdio MCP command")?;
                    rendered.push_str("command = ");
                    rendered.push_str(&toml_string_literal(&command));
                    rendered.push_str("\nargs = ");
                    rendered.push_str(&toml_string_array_literal(args));
                    rendered.push('\n');
                }
                McpServerSpec::Http { url } => {
                    if url.trim().is_empty() {
                        bail!("受控 HTTP MCP server '{name}' 的 url 不能为空");
                    }

                    rendered.push_str("url = ");
                    rendered.push_str(&toml_string_literal(url));
                    rendered.push('\n');
                }
            }
        }
    }

    // 在写盘前先验证生成文本自身可被 TOML 解析，避免落盘无效权威配置。
    toml::from_str::<toml::Value>(&rendered)
        .context("内部错误：生成的权威 sidecar config.toml 不是合法 TOML")?;

    Ok(rendered)
}

/// 清除能够重新打开外部网络、compat、subagent、存储或 managed MCP 能力的环境变量。
///
/// 必须在创建 Tokio runtime 和调用任何 shell API 前调用。`std::env` 在 Unix 上是
/// 进程全局状态；调用方必须保证此时没有其他线程读取或修改环境变量。
pub fn sanitize_env() -> Result<()> {
    // 先快照所有 OTEL 前缀 key，随后再统一删除，避免在迭代环境时原地修改它。
    let otel_keys: Vec<_> = env::vars_os()
        .filter_map(|(key, _)| is_otel_environment_key(&key).then_some(key))
        .collect();

    // SAFETY: sidecar 的启动顺序要求本函数在 Tokio runtime 和任何 shell API 前调用，
    // 此时尚未创建并发读取环境变量的线程；测试也以本模块互斥锁串行化这些修改。
    unsafe {
        for name in SANITIZED_ENV_VARS {
            env::remove_var(name);
        }
        for name in COMPAT_ENV_VARS {
            env::remove_var(name);
        }
        for key in otel_keys {
            env::remove_var(key);
        }
    }

    Ok(())
}

/// 设置最终的私有 `GROK_HOME` 环境变量。
///
/// 必须在任何会触发 `xai_grok_config::grok_home()` 的 shell API 前调用，因为该 API
/// 用进程级 `OnceLock` 缓存首次解析结果。
pub fn set_grok_home(path: &Path) -> Result<()> {
    set_absolute_path_environment("GROK_HOME", path)
}

/// 设置最终 AgentDefinition 的绝对路径到 `GROK_AGENT`。
///
/// 调用顺序应位于 [`sanitize_env`] 之后，并与 `[agent].definition` 指向同一物化文件。
pub fn set_grok_agent(path: &Path) -> Result<()> {
    set_absolute_path_environment("GROK_AGENT", path)
}

/// 要求由启动边界传入的敏感路径均为绝对路径。
fn require_absolute_path(path: &Path, description: &str) -> Result<()> {
    if !path.is_absolute() {
        bail!("{description} 必须是绝对路径: {}", path.display());
    }
    Ok(())
}

/// 在 Unix 上递归创建并收紧一个私有目录，拒绝以符号链接作为最终目录。
#[cfg(unix)]
fn create_private_directory(path: &Path) -> Result<()> {
    use std::os::unix::fs::DirBuilderExt;

    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    builder.mode(DIRECTORY_MODE);
    builder
        .create(path)
        .with_context(|| format!("创建私有目录失败: {}", path.display()))?;

    ensure_real_directory(path, "私有目录")?;
    set_private_permissions(path, DIRECTORY_MODE, "私有目录")?;
    Ok(())
}

/// 非 Unix 平台无法表达本 POC 的私有目录权限契约，因此拒绝继续。
#[cfg(not(unix))]
fn create_private_directory(path: &Path) -> Result<()> {
    let _ = path;
    bail!("私有目录权限硬化仅支持 Unix/macOS 文件权限模型");
}

/// 拒绝私有 home 中可能以更高优先级覆盖安全配置的文件或符号链接。
fn reject_private_policy_layers(grok_home: &Path) -> Result<()> {
    for filename in FORBIDDEN_PRIVATE_POLICY_FILES {
        let policy_path = grok_home.join(filename);
        match fs::symlink_metadata(&policy_path) {
            Ok(_) => {
                bail!(
                    "拒绝启动：私有 GROK_HOME 中存在未受控策略层: {}",
                    policy_path.display()
                );
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("检查私有策略层失败: {}", policy_path.display()));
            }
        }
    }
    Ok(())
}

/// 确保一个目录真实存在且不是符号链接，避免私有写入被重定向。
fn ensure_real_directory(path: &Path, description: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("读取{description}元数据失败: {}", path.display()))?;
    if metadata.file_type().is_symlink() {
        bail!("{description} 不能是符号链接: {}", path.display());
    }
    if !metadata.is_dir() {
        bail!("{description} 必须是目录: {}", path.display());
    }
    Ok(())
}

/// 若锁文件已存在且是符号链接，则 fail-closed，避免锁被重定向到外部位置。
fn reject_symlink_if_present(path: &Path, description: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            bail!("{description} 不能是符号链接: {}", path.display());
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => {
            Err(error).with_context(|| format!("检查{description}失败: {}", path.display()))
        }
    }
}

/// 以创建时和事后两次设置确保锁文件为 `0600`。
#[cfg(unix)]
fn open_private_file(path: &Path) -> Result<File> {
    use std::os::unix::fs::OpenOptionsExt;

    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .mode(FILE_MODE)
        .open(path)
        .with_context(|| format!("打开私有文件失败: {}", path.display()))?;
    set_private_permissions(path, FILE_MODE, "私有文件")?;
    Ok(file)
}

/// 非 Unix 平台无法用 POSIX mode 表达本 POC 的权限契约，因此拒绝继续。
#[cfg(not(unix))]
fn open_private_file(path: &Path) -> Result<File> {
    let _ = path;
    bail!("私有文件权限硬化仅支持 Unix/macOS 文件权限模型");
}

/// 在 Unix 上把文件或目录权限收紧为指定 owner-only mode。
#[cfg(unix)]
fn set_private_permissions(path: &Path, mode: u32, description: &str) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(mode)).with_context(|| {
        format!(
            "设置{description}权限为 {mode:04o} 失败: {}",
            path.display()
        )
    })
}

/// rename 成功后同步父目录，持久化文件名到目录项的映射。
fn sync_parent_directory(parent: &Path) -> Result<()> {
    let directory = File::open(parent)
        .with_context(|| format!("打开原子写父目录失败: {}", parent.display()))?;
    directory
        .sync_all()
        .with_context(|| format!("同步原子写父目录失败: {}", parent.display()))
}

/// 将路径转换为可安全写入 TOML 的 Unicode 字符串；非 Unicode 路径 fail-closed。
fn path_to_utf8(path: &Path, description: &str) -> Result<String> {
    path.to_str().map(str::to_owned).with_context(|| {
        format!(
            "{description} 不是可写入 TOML 的 UTF-8 路径: {}",
            path.display()
        )
    })
}

/// 渲染 TOML 字符串值，同时屏蔽配置层对 `$VAR` 的二次环境展开。
fn toml_string_literal(value: &str) -> String {
    toml::Value::String(value.replace('$', "$$")).to_string()
}

/// 渲染 TOML table key；table key 不经过配置值的环境展开，因此保留原始名称。
fn toml_key_literal(value: &str) -> String {
    toml::Value::String(value.to_owned()).to_string()
}

/// 渲染 MCP stdio 参数数组，并对每个参数屏蔽配置层环境展开。
fn toml_string_array_literal(values: &[String]) -> String {
    let values = values
        .iter()
        .map(|value| toml::Value::String(value.replace('$', "$$")))
        .collect();
    toml::Value::Array(values).to_string()
}

/// 判断环境变量名是否匹配要求清除的 `OTEL_` ASCII 前缀。
fn is_otel_environment_key(key: &std::ffi::OsStr) -> bool {
    key.to_string_lossy().starts_with("OTEL_")
}

/// 设置仅接受绝对路径的环境变量，避免最终 shell 配置回退到相对路径。
fn set_absolute_path_environment(variable: &str, path: &Path) -> Result<()> {
    require_absolute_path(path, variable)?;

    // SAFETY: 启动主流程在创建 Tokio runtime 前串行调用；该时序与 sanitize_env 的
    // 约束相同，确保没有并发线程访问进程环境变量。
    unsafe {
        env::set_var(variable, path);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::env;
    use std::ffi::OsString;
    use std::fs;
    use std::sync::Mutex;

    use super::*;

    /// 本 crate 的环境变量测试共享同一把锁，避免相互污染进程全局状态。
    static ENVIRONMENT_TEST_LOCK: Mutex<()> = Mutex::new(());

    /// 在测试结束或断言 panic 时恢复所有被测试修改过的环境变量。
    struct EnvironmentRestore {
        previous: Vec<(OsString, Option<OsString>)>,
    }

    impl EnvironmentRestore {
        /// 快照候选环境变量的当前值，重复 key 只记录一次。
        fn capture(keys: &[OsString]) -> Self {
            let mut previous = Vec::new();
            for key in keys {
                if previous.iter().any(|(existing, _)| existing == key) {
                    continue;
                }
                previous.push((key.clone(), env::var_os(key)));
            }
            Self { previous }
        }
    }

    impl Drop for EnvironmentRestore {
        /// 无论测试是否 panic 都恢复进程环境，避免影响同一测试二进制的后续测试。
        fn drop(&mut self) {
            // SAFETY: 本模块的环境变量测试由 ENVIRONMENT_TEST_LOCK 串行化，且这些
            // key 只被本测试构造和恢复，不会与本 crate 的其他测试并发访问。
            unsafe {
                for (key, value) in &self.previous {
                    match value {
                        Some(value) => env::set_var(key, value),
                        None => env::remove_var(key),
                    }
                }
            }
        }
    }

    /// 为环境卫生测试列出所有应清除的 key，外加一个动态 `OTEL_` 恶意变量。
    fn sanitized_environment_keys() -> Vec<OsString> {
        let mut keys: Vec<_> = SANITIZED_ENV_VARS.iter().map(OsString::from).collect();
        keys.extend(COMPAT_ENV_VARS.iter().map(OsString::from));
        keys.extend(
            env::vars_os().filter_map(|(key, _)| is_otel_environment_key(&key).then_some(key)),
        );
        keys.push(OsString::from("OTEL_EFFLAB_MALICIOUS_ENDPOINT"));
        keys
    }

    /// 在替换已有文件后仍须只保留新内容并收紧为 `0600`。
    #[cfg(unix)]
    #[test]
    fn atomic_write_private_replaces_content_with_owner_only_mode() {
        use std::os::unix::fs::PermissionsExt;

        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let target = temporary.path().join("authoritative.toml");
        fs::write(&target, b"old = true\n").expect("写入旧配置应成功");

        atomic_write_private(&target, b"new = false\n").expect("原子写应成功");

        assert_eq!(
            fs::read(&target).expect("读取目标文件应成功"),
            b"new = false\n"
        );
        let mode = fs::metadata(&target)
            .expect("读取目标元数据应成功")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, FILE_MODE, "目标文件必须为 0600");
    }

    /// 权威配置必须显式包含所有防护字段和批准后的两种 MCP transport。
    #[test]
    fn render_authoritative_config_contains_all_required_fields() {
        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let grok_home = temporary.path().join("private-grok-home");
        let agent_definition =
            materialize_agent_definition(&grok_home).expect("物化默认 AgentDefinition 应成功");
        let approved_mcp = ApprovedMcpConfig {
            servers: BTreeMap::from([
                (
                    "local-stdio".to_string(),
                    McpServerSpec::Stdio {
                        command: PathBuf::from("/bin/echo"),
                        args: vec!["--safe".to_string()],
                    },
                ),
                (
                    "local-http".to_string(),
                    McpServerSpec::Http {
                        url: "http://127.0.0.1:43123/mcp".to_string(),
                    },
                ),
            ]),
        };

        let rendered =
            render_authoritative_config(&grok_home, &agent_definition, Some(&approved_mcp))
                .expect("渲染权威配置应成功");
        let parsed: toml::Value = toml::from_str(&rendered).expect("渲染结果必须是合法 TOML");

        assert_eq!(
            value_at(&parsed, &["features", "remote_fetch"]),
            Some(&toml::Value::Boolean(false))
        );
        for vendor in COMPAT_VENDORS {
            for surface in COMPAT_SURFACES {
                assert_eq!(
                    value_at(&parsed, &["compat", vendor, surface]),
                    Some(&toml::Value::Boolean(false)),
                    "compat.{vendor}.{surface} 必须显式关闭"
                );
            }
        }
        assert_eq!(
            value_at(&parsed, &["subagents", "enabled"]),
            Some(&toml::Value::Boolean(false))
        );
        assert_eq!(
            value_at(&parsed, &["managed_mcps", "enabled"]),
            Some(&toml::Value::Boolean(false))
        );
        assert_eq!(
            value_at(&parsed, &["managed_mcps", "gateway_tools_enabled"]),
            Some(&toml::Value::Boolean(false))
        );
        assert_eq!(
            value_at(&parsed, &["memory", "enabled"]),
            Some(&toml::Value::Boolean(false))
        );
        assert_eq!(
            value_at(&parsed, &["skills", "paths"]),
            Some(&toml::Value::Array(Vec::new()))
        );
        assert_eq!(
            value_at(&parsed, &["agent", "name"]),
            Some(&toml::Value::String(DEFAULT_AGENT_NAME.to_string()))
        );
        assert_eq!(
            value_at(&parsed, &["agent", "definition"]).and_then(toml::Value::as_str),
            agent_definition.to_str()
        );
        assert_eq!(
            value_at(&parsed, &["mcp_servers", "local-stdio", "command"])
                .and_then(toml::Value::as_str),
            Some("/bin/echo")
        );
        assert_eq!(
            value_at(&parsed, &["mcp_servers", "local-http", "url"]).and_then(toml::Value::as_str),
            Some("http://127.0.0.1:43123/mcp")
        );
    }

    /// 所有精确名单和动态 `OTEL_` 前缀变量均不得在清理后残留。
    #[test]
    fn sanitize_env_removes_constructed_malicious_variables() {
        let _test_lock = ENVIRONMENT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut malicious_keys = sanitized_environment_keys();
        let preserved_key = OsString::from("EFFLAB_ENVIRONMENT_SHOULD_SURVIVE");
        malicious_keys.push(preserved_key.clone());
        let _restore = EnvironmentRestore::capture(&malicious_keys);

        // SAFETY: ENVIRONMENT_TEST_LOCK 串行化本模块的环境测试，所有写入都由
        // EnvironmentRestore 在作用域结束时恢复。
        unsafe {
            for key in malicious_keys.iter().filter(|key| *key != &preserved_key) {
                env::set_var(key, "malicious");
            }
            env::set_var(&preserved_key, "preserved");
        }

        sanitize_env().expect("环境卫生应成功");

        for key in malicious_keys.iter().filter(|key| *key != &preserved_key) {
            assert!(
                env::var_os(key).is_none(),
                "恶意环境变量必须被清除: {}",
                key.to_string_lossy()
            );
        }
        assert_eq!(
            env::var_os(&preserved_key),
            Some(OsString::from("preserved"))
        );
    }

    /// 从 TOML 根节点按路径获取值，便于断言嵌套权威配置。
    fn value_at<'a>(value: &'a toml::Value, path: &[&str]) -> Option<&'a toml::Value> {
        let mut current = value;
        for key in path {
            current = current.get(*key)?;
        }
        Some(current)
    }
}
