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
/// 编译期嵌入的密封默认 AgentDefinition，运行时绝不从用户目录读取它。
const DEFAULT_AGENT_DEFINITION: &str = include_str!("../assets/efflab-default-agent.md");

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
    reject_symlink_if_present(path, "原子写目标")?;

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
    if path
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        bail!("{description} 不允许包含 ..: {}", path.display());
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
    use std::collections::{BTreeMap, BTreeSet};
    use std::env;
    use std::ffi::OsString;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;

    use efflab_agent_contract::{ApprovedMcpConfig, McpServerSpec, render_authoritative_config};

    use super::*;

    /// 渲染器的固定 agent 名称属于共享 contract，而 sidecar 测试仅验证其输出。
    const DEFAULT_AGENT_NAME: &str = "efflab-default";
    /// compat 全量显式关闭的供应商集合。
    const COMPAT_VENDORS: [&str; 3] = ["claude", "cursor", "codex"];
    /// compat 全量显式关闭的 surface 集合。
    const COMPAT_SURFACES: [&str; 6] = ["skills", "rules", "agents", "mcps", "hooks", "sessions"];

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

    /// 相对私有 home 不能绕过启动边界的绝对路径约束。
    #[test]
    fn prepare_private_home_rejects_relative_path() {
        let error = prepare_private_home(Path::new("relative-grok-home"))
            .expect_err("相对私有 home 必须被拒绝");

        assert!(
            error.to_string().contains("私有 GROK_HOME 必须是绝对路径"),
            "错误必须说明绝对路径约束: {error:#}"
        );
    }

    /// 含 `..` 的私有 home 会造成词法路径和真实路径不一致，必须 fail-closed。
    #[test]
    fn prepare_private_home_rejects_parent_directory_component() {
        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let grok_home = temporary
            .path()
            .join("parent")
            .join("..")
            .join("private-grok-home");

        let error =
            prepare_private_home(&grok_home).expect_err("包含 .. 组件的私有 home 必须被拒绝");

        assert!(
            error.to_string().contains("私有 GROK_HOME 不允许包含 .."),
            "错误必须说明拒绝 .. 组件: {error:#}"
        );
    }

    /// 已存在的未受控策略文件可能覆盖权威配置，必须拒绝启动。
    #[test]
    fn prepare_private_home_rejects_existing_forbidden_policy_files() {
        let temporary = tempfile::tempdir().expect("创建临时目录应成功");

        for filename in FORBIDDEN_PRIVATE_POLICY_FILES {
            let grok_home = temporary.path().join(filename).join("private-grok-home");
            fs::create_dir_all(&grok_home).expect("创建私有 home 应成功");
            let policy_path = grok_home.join(filename);
            fs::write(&policy_path, "unsafe = true\n").expect("写入未受控策略文件应成功");

            let error =
                prepare_private_home(&grok_home).expect_err("存在未受控策略文件时必须拒绝启动");
            assert!(
                error.to_string().contains("存在未受控策略层"),
                "错误必须说明策略层拒绝: {error:#}"
            );
            assert!(
                error.to_string().contains(filename),
                "错误必须包含被拒绝策略文件名 {filename}: {error:#}"
            );
        }
    }

    /// 符号链接形式的未受控策略文件同样必须拒绝，不能仅检查普通文件。
    #[cfg(unix)]
    #[test]
    fn prepare_private_home_rejects_symlinked_forbidden_policy_files() {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let outside_policy = temporary.path().join("outside-policy.toml");
        fs::write(&outside_policy, "unsafe = true\n").expect("写入外部策略文件应成功");

        for filename in FORBIDDEN_PRIVATE_POLICY_FILES {
            let grok_home = temporary.path().join(format!("symlink-{filename}"));
            fs::create_dir_all(&grok_home).expect("创建私有 home 应成功");
            let policy_path = grok_home.join(filename);
            symlink(&outside_policy, &policy_path).expect("创建策略符号链接应成功");

            let error = prepare_private_home(&grok_home).expect_err("符号链接策略文件必须被拒绝");
            assert!(
                error.to_string().contains("存在未受控策略层"),
                "错误必须说明策略层拒绝: {error:#}"
            );
            assert!(
                error.to_string().contains(filename),
                "错误必须包含符号链接策略文件名 {filename}: {error:#}"
            );
        }
    }

    /// 已存在的宽权限目录必须在准备后收紧为 owner-only `0700`。
    #[cfg(unix)]
    #[test]
    fn prepare_private_home_tightens_existing_directory_to_owner_only_mode() {
        use std::os::unix::fs::PermissionsExt;

        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let grok_home = temporary.path().join("wide-private-grok-home");
        fs::create_dir(&grok_home).expect("创建私有 home 应成功");
        fs::set_permissions(&grok_home, fs::Permissions::from_mode(0o777))
            .expect("放宽私有 home 权限应成功");

        prepare_private_home(&grok_home).expect("准备私有 home 应成功");

        assert_eq!(
            permissions_mode(&grok_home),
            DIRECTORY_MODE,
            "准备后的私有 home 必须为 0700"
        );
    }

    /// 锁文件在首次创建后必须收紧为 owner-only `0600`。
    #[cfg(unix)]
    #[test]
    fn acquire_home_lock_creates_owner_only_lock_file() {
        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let grok_home = temporary.path().join("private-grok-home");

        let _lock = acquire_home_lock(&grok_home).expect("获取私有 home 锁应成功");
        let lock_path = grok_home.join(HOME_LOCK_FILENAME);

        assert!(lock_path.is_file(), "锁文件必须被创建");
        assert_eq!(permissions_mode(&lock_path), FILE_MODE, "锁文件必须为 0600");
    }

    /// 锁文件符号链接会重定向锁目标，必须 fail-closed。
    #[cfg(unix)]
    #[test]
    fn acquire_home_lock_rejects_symlinked_lock_file() {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let grok_home = temporary.path().join("private-grok-home");
        let outside_lock = temporary.path().join("outside.lock");
        fs::create_dir_all(&grok_home).expect("创建私有 home 应成功");
        fs::write(&outside_lock, "outside").expect("写入外部锁文件应成功");
        symlink(&outside_lock, grok_home.join(HOME_LOCK_FILENAME))
            .expect("创建锁文件符号链接应成功");

        let error = acquire_home_lock(&grok_home).expect_err("锁文件符号链接必须被拒绝");

        assert!(
            error
                .to_string()
                .contains("私有 home 锁文件 不能是符号链接"),
            "错误必须说明锁文件符号链接被拒绝: {error:#}"
        );
    }

    /// fs2 的非阻塞独占锁必须阻止同一进程再次锁定同一 home。
    #[cfg(unix)]
    #[test]
    fn acquire_home_lock_rejects_second_lock_in_same_process() {
        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let grok_home = temporary.path().join("private-grok-home");

        let _first_lock = acquire_home_lock(&grok_home).expect("首次获取私有 home 锁应成功");
        let error = acquire_home_lock(&grok_home).expect_err("同一 home 的第二把锁必须失败");

        assert!(
            error.to_string().contains("拒绝并发启动"),
            "错误必须说明并发启动被拒绝: {error:#}"
        );
    }

    /// 在替换已有文件后仍须只保留新内容并收紧为 `0600`。
    #[cfg(unix)]
    #[test]
    fn atomic_write_private_replaces_content_with_owner_only_mode() {
        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let target = temporary.path().join("authoritative.toml");
        fs::write(&target, b"old = true\n").expect("写入旧配置应成功");

        atomic_write_private(&target, b"new = false\n").expect("原子写应成功");

        assert_eq!(
            fs::read(&target).expect("读取目标文件应成功"),
            b"new = false\n"
        );
        assert_eq!(permissions_mode(&target), FILE_MODE, "目标文件必须为 0600");
    }

    /// 原子写目标不能是相对路径，避免写入位置受当前工作目录影响。
    #[test]
    fn atomic_write_private_rejects_relative_target_path() {
        let error = atomic_write_private(Path::new("relative-config.toml"), b"safe = true\n")
            .expect_err("相对原子写目标必须被拒绝");

        assert!(
            error.to_string().contains("原子写目标 必须是绝对路径"),
            "错误必须说明绝对路径约束: {error:#}"
        );
    }

    /// 原子写不可隐式创建父目录，避免改变调用方指定的隔离边界。
    #[test]
    fn atomic_write_private_rejects_missing_parent_directory() {
        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let target = temporary.path().join("missing-parent").join("config.toml");

        let error = atomic_write_private(&target, b"safe = true\n")
            .expect_err("不存在的原子写父目录必须被拒绝");

        assert!(
            error.to_string().contains("读取原子写目标父目录元数据失败"),
            "错误必须说明父目录不存在: {error:#}"
        );
        assert!(
            error.chain().any(|cause| cause
                .downcast_ref::<std::io::Error>()
                .is_some_and(|io| { io.kind() == std::io::ErrorKind::NotFound })),
            "错误链必须保留 NotFound 分类: {error:#}"
        );
    }

    /// 原子写父目录若为符号链接，不能跟随到外部目录。
    #[cfg(unix)]
    #[test]
    fn atomic_write_private_rejects_symlinked_parent_directory() {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let outside_directory = temporary.path().join("outside-directory");
        let symlinked_parent = temporary.path().join("symlinked-parent");
        fs::create_dir(&outside_directory).expect("创建外部目录应成功");
        symlink(&outside_directory, &symlinked_parent).expect("创建父目录符号链接应成功");
        let target = symlinked_parent.join("config.toml");

        let error = atomic_write_private(&target, b"safe = true\n")
            .expect_err("符号链接原子写父目录必须被拒绝");

        assert!(
            error
                .to_string()
                .contains("原子写目标父目录 不能是符号链接"),
            "错误必须说明父目录符号链接被拒绝: {error:#}"
        );
        assert!(
            !outside_directory.join("config.toml").exists(),
            "拒绝后不得在符号链接指向的外部目录创建文件"
        );
    }

    /// 已存在的符号链接目标不能被原子替换，避免其语义被误解为跟随外部目标写入。
    #[cfg(unix)]
    #[test]
    fn atomic_write_private_rejects_symlinked_target_file() {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let outside_target = temporary.path().join("outside-config.toml");
        let target = temporary.path().join("config.toml");
        fs::write(&outside_target, b"outside = true\n").expect("写入外部配置应成功");
        symlink(&outside_target, &target).expect("创建目标文件符号链接应成功");

        let error = atomic_write_private(&target, b"safe = true\n")
            .expect_err("符号链接原子写目标必须被拒绝");

        assert!(
            error.to_string().contains("原子写目标 不能是符号链接"),
            "错误必须说明目标符号链接被拒绝: {error:#}"
        );
        assert_eq!(
            fs::read(&outside_target).expect("读取外部配置应成功"),
            b"outside = true\n",
            "拒绝后不得改写外部目标"
        );
    }

    /// 无法原子替换时，旧内容必须保留；macOS 上对目标父目录只读稳定返回 EACCES。
    #[cfg(target_os = "macos")]
    #[test]
    fn atomic_write_private_preserves_old_content_when_parent_is_read_only() {
        use std::os::unix::fs::PermissionsExt;

        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let parent = temporary.path().join("read-only-private-directory");
        let target = parent.join("config.toml");
        fs::create_dir(&parent).expect("创建私有目录应成功");
        fs::write(&target, b"old = true\n").expect("写入旧配置应成功");
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o500))
            .expect("收紧父目录为只读应成功");

        let result = atomic_write_private(&target, b"new = false\n");

        // 无论原子写结果如何，先恢复目录权限以允许 TempDir 在测试结束后清理。
        fs::set_permissions(&parent, fs::Permissions::from_mode(DIRECTORY_MODE))
            .expect("恢复父目录权限应成功");
        let error = result.expect_err("只读父目录必须使原子写失败");
        assert!(
            error.chain().any(|cause| cause
                .downcast_ref::<std::io::Error>()
                .is_some_and(|io| io.kind() == std::io::ErrorKind::PermissionDenied)),
            "错误链必须保留 PermissionDenied 分类: {error:#}"
        );
        assert_eq!(
            fs::read(&target).expect("读取旧配置应成功"),
            b"old = true\n",
            "原子写失败后旧内容必须保持不变"
        );
    }

    /// 原子写成功后，临时文件必须被 rename 消耗而非遗留在私有目录。
    #[test]
    fn atomic_write_private_leaves_no_temporary_file_after_success() {
        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let target = temporary.path().join("config.toml");

        atomic_write_private(&target, b"safe = true\n").expect("原子写应成功");

        let entries: Vec<_> = fs::read_dir(temporary.path())
            .expect("读取私有目录应成功")
            .map(|entry| entry.expect("读取目录项应成功").file_name())
            .collect();
        assert_eq!(entries, vec![OsString::from("config.toml")]);
        assert!(
            entries
                .iter()
                .all(|name| !name.to_string_lossy().contains(".tmp")),
            "成功后目录不得遗留 .tmp 类临时文件: {entries:?}"
        );
    }

    /// 物化内容必须与编译期嵌入的密封默认 AgentDefinition 完全一致。
    #[test]
    fn materialize_agent_definition_writes_exact_embedded_asset() {
        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let grok_home = temporary.path().join("private-grok-home");

        let definition =
            materialize_agent_definition(&grok_home).expect("物化默认 AgentDefinition 应成功");

        assert_eq!(
            fs::read_to_string(&definition).expect("读取物化 AgentDefinition 应成功"),
            include_str!("../assets/efflab-default-agent.md"),
            "物化内容必须精确等于嵌入 asset"
        );
    }

    /// 物化 AgentDefinition 的目录和文件都必须采用 owner-only 权限。
    #[cfg(unix)]
    #[test]
    fn materialize_agent_definition_sets_owner_only_directory_and_file_modes() {
        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let grok_home = temporary.path().join("private-grok-home");

        let definition =
            materialize_agent_definition(&grok_home).expect("物化默认 AgentDefinition 应成功");

        assert_eq!(
            permissions_mode(definition.parent().expect("AgentDefinition 必须有父目录")),
            DIRECTORY_MODE,
            "agents 目录必须为 0700"
        );
        assert_eq!(
            permissions_mode(&definition),
            FILE_MODE,
            "物化 AgentDefinition 必须为 0600"
        );
    }

    /// 再次物化必须覆盖恶意旧内容，而不是读取或合并该内容。
    #[test]
    fn materialize_agent_definition_overwrites_malicious_existing_content() {
        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let grok_home = temporary.path().join("private-grok-home");
        let agents_directory = grok_home.join("agents");
        fs::create_dir_all(&agents_directory).expect("创建 agents 目录应成功");
        let definition = agents_directory.join(DEFAULT_AGENT_FILENAME);
        fs::write(&definition, "# malicious agent definition\n")
            .expect("写入恶意 AgentDefinition 应成功");

        let materialized =
            materialize_agent_definition(&grok_home).expect("重复物化默认 AgentDefinition 应成功");

        assert_eq!(materialized, definition);
        assert_eq!(
            fs::read_to_string(&materialized).expect("读取覆盖后的 AgentDefinition 应成功"),
            include_str!("../assets/efflab-default-agent.md"),
            "重复物化必须恢复密封 asset 内容"
        );
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
        let stdio_args = value_at(&parsed, &["mcp_servers", "local-stdio", "args"])
            .and_then(toml::Value::as_array)
            .expect("stdio MCP 必须保留 args 数组");
        assert_eq!(stdio_args.len(), 1, "stdio MCP args 数量必须精确保留");
        assert_eq!(
            stdio_args[0].as_str(),
            Some("--safe"),
            "stdio MCP args 值必须精确保留"
        );
        assert_eq!(
            value_at(&parsed, &["mcp_servers", "local-http", "url"]).and_then(toml::Value::as_str),
            Some("http://127.0.0.1:43123/mcp")
        );

        let actual_top_level_keys: BTreeSet<_> = parsed
            .as_table()
            .expect("权威配置根节点必须是 TOML table")
            .keys()
            .cloned()
            .collect();
        let expected_top_level_keys = BTreeSet::from([
            "features".to_string(),
            "compat".to_string(),
            "subagents".to_string(),
            "managed_mcps".to_string(),
            "memory".to_string(),
            "skills".to_string(),
            "agent".to_string(),
            "mcp_servers".to_string(),
        ]);
        assert_eq!(
            actual_top_level_keys, expected_top_level_keys,
            "权威配置只能包含固定的顶层键，不能继承额外配置"
        );
    }

    /// `$` 必须在渲染文本中双写，且上游配置展开后仍恢复为原始字面值。
    #[test]
    fn render_authoritative_config_escapes_dollar_signs_without_changing_runtime_values() {
        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let grok_home = temporary.path().join("private-grok-home");
        let agent_definition =
            materialize_agent_definition(&grok_home).expect("物化默认 AgentDefinition 应成功");
        let approved_mcp = ApprovedMcpConfig {
            servers: BTreeMap::from([
                (
                    "stdio-dollar".to_string(),
                    McpServerSpec::Stdio {
                        command: PathBuf::from("/tmp/$HOME/mcp"),
                        args: vec!["$HOME".to_string(), "a$b".to_string()],
                    },
                ),
                (
                    "http-dollar".to_string(),
                    McpServerSpec::Http {
                        url: "http://127.0.0.1:43123/$HOME?a=a$b".to_string(),
                    },
                ),
            ]),
        };

        let rendered =
            render_authoritative_config(&grok_home, &agent_definition, Some(&approved_mcp))
                .expect("渲染含 $ 的权威配置应成功");
        assert!(
            rendered.contains("/tmp/$$HOME/mcp"),
            "command 中的 $ 必须双写: {rendered}"
        );
        assert!(
            rendered.contains("\"$$HOME\"") && rendered.contains("\"a$$b\""),
            "args 中的 $ 必须双写: {rendered}"
        );
        assert!(
            rendered.contains("http://127.0.0.1:43123/$$HOME?a=a$$b"),
            "url 中的 $ 必须双写: {rendered}"
        );

        let mut parsed: toml::Value = toml::from_str(&rendered).expect("渲染结果必须是合法 TOML");
        assert_eq!(
            value_at(&parsed, &["mcp_servers", "stdio-dollar", "command"])
                .and_then(toml::Value::as_str),
            Some("/tmp/$$HOME/mcp"),
            "TOML 解析必须保留防二次展开的双写 $"
        );
        let parsed_args = value_at(&parsed, &["mcp_servers", "stdio-dollar", "args"])
            .and_then(toml::Value::as_array)
            .expect("stdio MCP 必须包含 args 数组");
        assert_eq!(
            parsed_args
                .iter()
                .map(|value| value.as_str())
                .collect::<Vec<_>>(),
            vec![Some("$$HOME"), Some("a$$b")],
            "TOML 解析必须保留 args 的双写 $"
        );
        assert_eq!(
            value_at(&parsed, &["mcp_servers", "http-dollar", "url"]).and_then(toml::Value::as_str),
            Some("http://127.0.0.1:43123/$$HOME?a=a$$b"),
            "TOML 解析必须保留 url 的双写 $"
        );

        // 复用上游的公开展开函数验证 `$$` 在运行时只还原为字面 `$`，而不读取环境变量。
        xai_grok_shell::config::expand_env_vars_in_toml(&mut parsed);
        assert_eq!(
            value_at(&parsed, &["mcp_servers", "stdio-dollar", "command"])
                .and_then(toml::Value::as_str),
            Some("/tmp/$HOME/mcp"),
            "运行时展开后 command 必须恢复原始字面值"
        );
        let expanded_args = value_at(&parsed, &["mcp_servers", "stdio-dollar", "args"])
            .and_then(toml::Value::as_array)
            .expect("展开后 stdio MCP 必须包含 args 数组");
        assert_eq!(
            expanded_args
                .iter()
                .map(|value| value.as_str())
                .collect::<Vec<_>>(),
            vec![Some("$HOME"), Some("a$b")],
            "运行时展开后 args 必须恢复原始字面值"
        );
        assert_eq!(
            value_at(&parsed, &["mcp_servers", "http-dollar", "url"]).and_then(toml::Value::as_str),
            Some("http://127.0.0.1:43123/$HOME?a=a$b"),
            "运行时展开后 url 必须恢复原始字面值"
        );
    }

    /// 命令、参数和 URL 中的引号与换行必须由 TOML string literal 正确转义。
    #[test]
    fn render_authoritative_config_escapes_quotes_and_newlines_in_mcp_values() {
        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let grok_home = temporary.path().join("private-grok-home");
        let agent_definition =
            materialize_agent_definition(&grok_home).expect("物化默认 AgentDefinition 应成功");
        let approved_mcp = ApprovedMcpConfig {
            servers: BTreeMap::from([
                (
                    "stdio-escaped".to_string(),
                    McpServerSpec::Stdio {
                        command: PathBuf::from("/tmp/mcp-\"quoted\""),
                        args: vec!["line one\nline two".to_string(), "a\"b".to_string()],
                    },
                ),
                (
                    "http-escaped".to_string(),
                    McpServerSpec::Http {
                        url: "http://127.0.0.1:43123/mcp?quote=\"x\"\nnext".to_string(),
                    },
                ),
            ]),
        };

        let rendered =
            render_authoritative_config(&grok_home, &agent_definition, Some(&approved_mcp))
                .expect("渲染含引号和换行的权威配置应成功");
        let parsed: toml::Value = toml::from_str(&rendered).expect("转义后的配置必须是合法 TOML");
        assert_eq!(
            value_at(&parsed, &["mcp_servers", "stdio-escaped", "command"])
                .and_then(toml::Value::as_str),
            Some("/tmp/mcp-\"quoted\""),
            "command 必须按字面值保留引号"
        );
        let args = value_at(&parsed, &["mcp_servers", "stdio-escaped", "args"])
            .and_then(toml::Value::as_array)
            .expect("stdio MCP 必须包含 args 数组");
        assert_eq!(
            args.iter().map(|value| value.as_str()).collect::<Vec<_>>(),
            vec![Some("line one\nline two"), Some("a\"b")],
            "args 必须按字面值保留换行和引号"
        );
        assert_eq!(
            value_at(&parsed, &["mcp_servers", "http-escaped", "url"])
                .and_then(toml::Value::as_str),
            Some("http://127.0.0.1:43123/mcp?quote=\"x\"\nnext"),
            "url 必须按字面值保留换行和引号"
        );
    }

    /// MCP 名称中的引号、点号与数字开头必须使用 TOML literal key 正确解析。
    #[test]
    fn render_authoritative_config_preserves_special_mcp_names() {
        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let grok_home = temporary.path().join("private-grok-home");
        let agent_definition =
            materialize_agent_definition(&grok_home).expect("物化默认 AgentDefinition 应成功");
        let special_name = "1.leading.\"quoted\"".to_string();
        let approved_mcp = ApprovedMcpConfig {
            servers: BTreeMap::from([(
                special_name.clone(),
                McpServerSpec::Http {
                    url: "http://127.0.0.1:43123/mcp".to_string(),
                },
            )]),
        };

        let rendered =
            render_authoritative_config(&grok_home, &agent_definition, Some(&approved_mcp))
                .expect("渲染含特殊 MCP 名称的配置应成功");
        let parsed: toml::Value =
            toml::from_str(&rendered).expect("特殊 MCP 名称必须生成合法 TOML");

        assert_eq!(
            value_at(&parsed, &["mcp_servers"])
                .and_then(toml::Value::as_table)
                .and_then(|servers| servers.get(&special_name))
                .and_then(toml::Value::as_table)
                .and_then(|server| server.get("url"))
                .and_then(toml::Value::as_str),
            Some("http://127.0.0.1:43123/mcp"),
            "特殊 MCP 名称必须作为单个正确键保留"
        );
    }

    /// 手工绕过输入边界构造的空 MCP 名称必须在渲染层再次被拒绝。
    #[test]
    fn render_authoritative_config_rejects_empty_mcp_name() {
        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let grok_home = temporary.path().join("private-grok-home");
        let agent_definition =
            materialize_agent_definition(&grok_home).expect("物化默认 AgentDefinition 应成功");
        let approved_mcp = ApprovedMcpConfig {
            servers: BTreeMap::from([(
                "   ".to_string(),
                McpServerSpec::Http {
                    url: "http://127.0.0.1:43123/mcp".to_string(),
                },
            )]),
        };

        let error = render_authoritative_config(&grok_home, &agent_definition, Some(&approved_mcp))
            .expect_err("空 MCP 名称必须被拒绝");

        assert!(
            error.to_string().contains("受控 MCP server 名称不能为空"),
            "错误必须说明 MCP 名称为空: {error:#}"
        );
    }

    /// 手工构造相对 stdio command 不能绕过渲染层的绝对路径校验。
    #[test]
    fn render_authoritative_config_rejects_relative_stdio_command() {
        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let grok_home = temporary.path().join("private-grok-home");
        let agent_definition =
            materialize_agent_definition(&grok_home).expect("物化默认 AgentDefinition 应成功");
        let approved_mcp = ApprovedMcpConfig {
            servers: BTreeMap::from([(
                "stdio".to_string(),
                McpServerSpec::Stdio {
                    command: PathBuf::from("relative-mcp"),
                    args: Vec::new(),
                },
            )]),
        };

        let error = render_authoritative_config(&grok_home, &agent_definition, Some(&approved_mcp))
            .expect_err("相对 stdio command 必须被拒绝");

        assert!(
            error
                .to_string()
                .contains("受控 stdio MCP server 'stdio' 的 command 必须为绝对路径"),
            "错误必须说明 stdio command 的绝对路径约束: {error:#}"
        );
    }

    /// 手工构造空 HTTP url 不能绕过渲染层校验。
    #[test]
    fn render_authoritative_config_rejects_empty_http_url() {
        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let grok_home = temporary.path().join("private-grok-home");
        let agent_definition =
            materialize_agent_definition(&grok_home).expect("物化默认 AgentDefinition 应成功");
        let approved_mcp = ApprovedMcpConfig {
            servers: BTreeMap::from([(
                "http".to_string(),
                McpServerSpec::Http {
                    url: " \t\n ".to_string(),
                },
            )]),
        };

        let error = render_authoritative_config(&grok_home, &agent_definition, Some(&approved_mcp))
            .expect_err("空 HTTP url 必须被拒绝");

        assert!(
            error
                .to_string()
                .contains("受控 HTTP MCP server 'http' 的 url 不能为空"),
            "错误必须说明 HTTP url 为空: {error:#}"
        );
    }

    /// 非 UTF-8 的 AgentDefinition 路径无法安全写入 TOML，必须 fail-closed。
    #[cfg(unix)]
    #[test]
    fn render_authoritative_config_rejects_non_utf8_agent_definition_path() {
        use std::os::unix::ffi::{OsStrExt, OsStringExt};

        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let grok_home = temporary.path().join("private-grok-home");
        let non_utf8_agent_definition =
            PathBuf::from(OsString::from_vec(b"/tmp/agent-\xFF.md".to_vec()));

        let error = render_authoritative_config(&grok_home, &non_utf8_agent_definition, None)
            .expect_err("非 UTF-8 AgentDefinition 路径必须被拒绝");

        assert!(
            error
                .to_string()
                .contains("物化 AgentDefinition 不是可写入 TOML 的 UTF-8 路径"),
            "错误必须说明非 UTF-8 路径被拒绝: {error:#}"
        );
        assert_eq!(
            non_utf8_agent_definition.as_os_str().as_bytes(),
            b"/tmp/agent-\xFF.md",
            "测试必须实际构造非 UTF-8 路径"
        );
    }

    /// 相对 GROK_HOME 与 GROK_AGENT 都不能写入进程环境，绝对路径必须精确保留。
    #[test]
    fn set_grok_environment_variables_require_absolute_paths_and_preserve_absolute_values() {
        let _test_lock = ENVIRONMENT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let keys = [OsString::from("GROK_HOME"), OsString::from("GROK_AGENT")];
        let _restore = EnvironmentRestore::capture(&keys);
        let temporary = tempfile::tempdir().expect("创建临时目录应成功");
        let absolute_home = temporary.path().join("private-grok-home");
        let absolute_agent = absolute_home.join("agents").join(DEFAULT_AGENT_FILENAME);

        for (variable, setter, relative_path) in [
            (
                "GROK_HOME",
                set_grok_home as fn(&Path) -> Result<()>,
                Path::new("relative-home"),
            ),
            (
                "GROK_AGENT",
                set_grok_agent as fn(&Path) -> Result<()>,
                Path::new("relative-agent.md"),
            ),
        ] {
            let error = setter(relative_path).expect_err("相对环境路径必须被拒绝");
            assert!(
                error.to_string().contains("必须是绝对路径"),
                "{variable} 的错误必须说明绝对路径约束: {error:#}"
            );
        }

        set_grok_home(&absolute_home).expect("设置绝对 GROK_HOME 应成功");
        set_grok_agent(&absolute_agent).expect("设置绝对 GROK_AGENT 应成功");
        assert_eq!(
            env::var("GROK_HOME").expect("GROK_HOME 必须已设置"),
            absolute_home.to_str().expect("临时路径必须是 UTF-8"),
            "GROK_HOME 必须精确保留绝对路径"
        );
        assert_eq!(
            env::var("GROK_AGENT").expect("GROK_AGENT 必须已设置"),
            absolute_agent.to_str().expect("临时路径必须是 UTF-8"),
            "GROK_AGENT 必须精确保留绝对路径"
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

    /// 读取 Unix 权限的低九位，屏蔽文件类型等无关元数据。
    #[cfg(unix)]
    fn permissions_mode(path: &Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;

        fs::metadata(path)
            .expect("读取权限元数据应成功")
            .permissions()
            .mode()
            & 0o777
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
