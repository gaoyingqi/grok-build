//! sidecar 启动所需的私有 home、文件权限和环境边界硬化。
//!
//! 本模块只在 Unix 上开放启动能力。Windows/非 Unix 的等价权限与进程边界尚未
//! proven，因此直接返回 fail-closed 错误，不读取 runtime config 或创建目录。

use std::env;
use std::ffi::OsString;
use std::path::Path;

use anyhow::{Context, Result, bail};
#[cfg(unix)]
use fs2::FileExt;
#[cfg(unix)]
use std::ffi::CString;
#[cfg(unix)]
use std::fs::File;
#[cfg(not(unix))]
use std::fs::File;
#[cfg(unix)]
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd};
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(unix)]
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(unix)]
const DIRECTORY_MODE: u32 = 0o700;
#[cfg(unix)]
const FILE_MODE: u32 = 0o600;
#[cfg(unix)]
const HOME_LOCK_FILENAME: &str = ".efflab-sidecar.lock";
const L3B_BIND_ENV: &str = "EFFLAB_L3B_BIND";
/// RuntimeConfigV1 的固定读取上限，防止启动阶段无界分配。
pub const MAX_RUNTIME_CONFIG_BYTES: usize = 64 * 1024;

/// Windows/非 Unix capability 尚未 proven；在所有文件读取和 env 清理前执行。
#[cfg(unix)]
pub fn ensure_platform_supported() -> Result<()> {
    Ok(())
}

/// 非 Unix 不允许直接拉起 sidecar，避免把未证明的权限模型当作安全边界。
#[cfg(not(unix))]
pub fn ensure_platform_supported() -> Result<()> {
    bail!("sidecar_hardening_unavailable: Windows/非 Unix capability 尚未 proven")
}

/// 保存启动阶段已经 no-follow 校验过的 home 与 session 目录句柄。
///
/// 主入口必须在配置校验后继续使用这组句柄，避免按同名路径重新解析到被替换的目录。
#[cfg(unix)]
pub struct StartupHandles {
    home_directory: File,
    session_cwd_directory: File,
}

/// 非 Unix 不暴露任何可用的启动句柄。
#[cfg(not(unix))]
pub struct StartupHandles;

/// 为一次启动打开并保存 home/session 目录 fd；runtime config 后续从 home fd 读取。
#[cfg(unix)]
pub fn open_startup_handles(home: &Path, session_cwd: &Path) -> Result<StartupHandles> {
    let session_cwd_directory = match open_existing_private_directory(session_cwd, "--session-cwd")
    {
        Ok(directory) => directory,
        Err(error) => {
            tracing::debug!(
                event = "session_fd_open_failed",
                "打开 session 目录句柄失败"
            );
            return Err(error);
        }
    };
    let home_directory = match open_existing_private_home_directory(home) {
        Ok(directory) => directory,
        Err(error) => {
            tracing::debug!(event = "home_fd_open_failed", "打开 home 目录句柄失败");
            return Err(error);
        }
    };
    tracing::debug!(
        event = "startup_fds_opened",
        "启动 home 与 session 目录句柄已打开"
    );
    Ok(StartupHandles {
        home_directory,
        session_cwd_directory,
    })
}

/// 非 Unix 在创建任何启动句柄前 fail-closed。
#[cfg(not(unix))]
pub fn open_startup_handles(_home: &Path, _session_cwd: &Path) -> Result<StartupHandles> {
    ensure_platform_supported()?;
    bail!("sidecar_hardening_unavailable")
}

#[cfg(unix)]
impl StartupHandles {
    /// 从已打开的 home fd 读取固定 runtime config，避免按外部路径重新解析父目录。
    pub fn read_private_runtime_config(&self, path: &Path) -> Result<String> {
        require_absolute_path(path, "--runtime-config")?;
        let filename = path.file_name().context("--runtime-config 必须指向文件")?;
        if filename != std::ffi::OsStr::new("runtime-config.v1.toml") {
            bail!("--runtime-config 必须指向 runtime-config.v1.toml");
        }
        match read_private_runtime_config_at(&self.home_directory, filename) {
            Ok(source) => Ok(source),
            Err(error) => {
                tracing::debug!(
                    event = "runtime_config_read_failed",
                    "从受保护 home fd 读取 runtime config 失败"
                );
                Err(error)
            }
        }
    }

    /// 只检查 home fd 下旧配置目录项是否存在，不读取旧配置内容。
    pub fn legacy_config_present(&self) -> Result<bool> {
        match path_entry_exists_at(
            &self.home_directory,
            std::ffi::OsStr::new("config.toml"),
            "旧 config.toml",
        ) {
            Ok(present) => {
                tracing::debug!(
                    event = "legacy_config_checked",
                    present,
                    "旧配置目录项已检查"
                );
                Ok(present)
            }
            Err(error) => {
                tracing::debug!(event = "legacy_config_check_failed", "旧配置目录项检查失败");
                Err(error)
            }
        }
    }

    /// 在已打开的 home fd 下获取非阻塞独占锁，并由调用方保持返回句柄。
    pub fn acquire_home_lock(&self) -> Result<File> {
        let lock_file = match open_private_lock(&self.home_directory) {
            Ok(file) => file,
            Err(error) => {
                tracing::debug!(event = "home_lock_open_failed", "打开 home 锁文件失败");
                return Err(error);
            }
        };
        if let Err(error) = FileExt::try_lock_exclusive(&lock_file) {
            tracing::debug!(event = "home_lock_acquire_failed", "取得 home 独占锁失败");
            return Err(error).context("拒绝并发启动：私有 home 已被另一 sidecar 占用");
        }
        Ok(lock_file)
    }

    /// 用已打开的 session fd 切换 cwd，不重新解析 session 路径。
    pub fn set_current_dir_secure(&self) -> Result<()> {
        // SAFETY: session_cwd_directory 由 open_existing_directory 以目录 no-follow 方式取得。
        let result = unsafe { libc::fchdir(self.session_cwd_directory.as_raw_fd()) };
        if result != 0 {
            tracing::debug!(
                event = "session_cwd_fd_switch_failed",
                "切换 session cwd 失败"
            );
            return Err(std::io::Error::last_os_error()).context("切换 --session-cwd 失败");
        }
        tracing::debug!(
            event = "session_cwd_fd_switched",
            "已使用 session cwd 目录句柄切换 cwd"
        );
        Ok(())
    }
}

#[cfg(not(unix))]
impl StartupHandles {
    /// 非 Unix 不读取 runtime config。
    pub fn read_private_runtime_config(&self, _path: &Path) -> Result<String> {
        ensure_platform_supported()?;
        bail!("sidecar_hardening_unavailable")
    }

    /// 非 Unix 不检查旧配置。
    pub fn legacy_config_present(&self) -> Result<bool> {
        ensure_platform_supported()?;
        bail!("sidecar_hardening_unavailable")
    }

    /// 非 Unix 不获取 home 锁。
    pub fn acquire_home_lock(&self) -> Result<File> {
        ensure_platform_supported()?;
        bail!("sidecar_hardening_unavailable")
    }

    /// 非 Unix 不切换 sidecar cwd。
    pub fn set_current_dir_secure(&self) -> Result<()> {
        ensure_platform_supported()
    }
}

/// 校验 Host 注入的短生命周期绑定令牌，不在错误或日志中回显其值。
pub fn validate_l3b_bind() -> Result<()> {
    let Some(value) = env::var_os(L3B_BIND_ENV) else {
        bail!("l3b_bind_invalid")
    };
    let Some(value) = value.to_str() else {
        bail!("l3b_bind_invalid")
    };
    if value.is_empty() || value.chars().any(|character| character.is_control()) {
        bail!("l3b_bind_invalid")
    }
    Ok(())
}

/// 创建并校验仅属于 sidecar 的私有 home。
#[cfg(unix)]
pub fn prepare_private_home(home: &Path) -> Result<()> {
    let _home_directory = open_private_home_directory(home)?;
    Ok(())
}

/// 非 Unix 不创建任何 sidecar 文件系统状态。
#[cfg(not(unix))]
pub fn prepare_private_home(_home: &Path) -> Result<()> {
    ensure_platform_supported()
}

/// 获取私有 home 的非阻塞独占锁；返回的 File 必须保留到进程退出。
#[cfg(unix)]
pub fn acquire_home_lock(home: &Path) -> Result<File> {
    let home_directory = open_private_home_directory(home)?;
    let lock_file = open_private_lock(&home_directory)?;
    FileExt::try_lock_exclusive(&lock_file)
        .context("拒绝并发启动：私有 home 已被另一 sidecar 占用")?;
    Ok(lock_file)
}

/// 非 Unix 不打开或创建锁文件。
#[cfg(not(unix))]
pub fn acquire_home_lock(_home: &Path) -> Result<File> {
    ensure_platform_supported()?;
    bail!("sidecar_hardening_unavailable")
}

/// 从同一次受保护的 Unix 文件句柄读取 runtime config，避免路径检查与读取之间的替换。
#[cfg(unix)]
pub fn read_private_runtime_config(path: &Path) -> Result<String> {
    require_absolute_path(path, "--runtime-config")?;
    let parent = path.parent().context("--runtime-config 缺少父目录")?;
    let parent_directory = open_existing_directory(parent, "--runtime-config 父目录")?;
    let filename = path.file_name().context("--runtime-config 必须指向文件")?;
    read_private_runtime_config_at(&parent_directory, filename)
}

#[cfg(unix)]
fn read_private_runtime_config_at(parent: &File, filename: &std::ffi::OsStr) -> Result<String> {
    let file = open_file_at(parent, filename, "--runtime-config")?;
    verify_private_file(&file, FILE_MODE, "--runtime-config")?;

    // 先用 fd 元数据拒绝明显超限文件，再用有界读取覆盖并发增长场景。
    let file_size = file
        .metadata()
        .context("读取 --runtime-config 大小失败")?
        .len();
    if file_size > MAX_RUNTIME_CONFIG_BYTES as u64 {
        tracing::debug!(
            event = "runtime_config_rejected",
            reason = "size_limit",
            "runtime config 超过大小上限"
        );
        bail!("runtime_config_invalid");
    }

    let mut bytes = Vec::with_capacity(file_size as usize);
    (&file)
        .take(MAX_RUNTIME_CONFIG_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .context("读取 RuntimeConfigV1 TOML 失败")?;
    if bytes.len() > MAX_RUNTIME_CONFIG_BYTES {
        tracing::debug!(
            event = "runtime_config_rejected",
            reason = "size_limit",
            "runtime config 读取期间超过大小上限"
        );
        bail!("runtime_config_invalid");
    }

    let source = String::from_utf8(bytes).context("读取 RuntimeConfigV1 TOML 失败")?;
    tracing::debug!(
        event = "runtime_config_read",
        "runtime config 已从受保护 fd 读取"
    );
    Ok(source)
}

/// 非 Unix 在 capability 关闭期间不读取 runtime config。
#[cfg(not(unix))]
pub fn read_private_runtime_config(_path: &Path) -> Result<String> {
    ensure_platform_supported()?;
    bail!("sidecar_hardening_unavailable")
}

/// 校验可递归创建的 home 已有路径组件；缺失的最终叶子留给安全创建阶段。
#[cfg(all(unix, test))]
pub(crate) fn validate_private_home_path(path: &Path) -> Result<()> {
    require_absolute_path(path, "私有 home")?;
    reject_shared_home_root(path)?;
    let components = normal_components(path, "私有 home")?;
    let mut current = open_root_directory().context("打开根目录失败")?;
    for component in components {
        match try_open_directory_at(&current, &component) {
            Ok(next) => current = next,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                if component_is_symlink(&current, &component)? {
                    bail!("私有 home 路径组件不能是符号链接");
                }
                return Err(secure_component_error(error, "私有 home"));
            }
        }
    }
    verify_private_directory(&current, "私有 home")
}

/// 使用 no-follow 目录句柄切换 cwd，避免先检查路径再按路径重新打开。
#[cfg(unix)]
pub fn set_current_dir_secure(path: &Path) -> Result<()> {
    let directory = open_existing_private_directory(path, "--session-cwd")?;
    // SAFETY: directory 是本函数通过 O_DIRECTORY|O_NOFOLLOW 打开的有效目录 fd。
    let result = unsafe { libc::fchdir(directory.as_raw_fd()) };
    if result != 0 {
        return Err(std::io::Error::last_os_error()).context("切换 --session-cwd 失败");
    }
    Ok(())
}

/// 非 Unix 不切换 sidecar cwd。
#[cfg(not(unix))]
pub fn set_current_dir_secure(_path: &Path) -> Result<()> {
    ensure_platform_supported()
}

/// 只判断同一安全父目录下的最终目录项是否存在，不读取其内容。
#[cfg(unix)]
pub fn path_entry_exists(path: &Path) -> Result<bool> {
    require_absolute_path(path, "路径")?;
    let parent = path.parent().context("路径缺少父目录")?;
    let parent_directory = open_existing_directory(parent, "路径父目录")?;
    let filename = path.file_name().context("路径必须包含目录项")?;
    path_entry_exists_at(&parent_directory, filename, "路径")
}

#[cfg(unix)]
fn path_entry_exists_at(
    parent_directory: &File,
    filename: &std::ffi::OsStr,
    description: &str,
) -> Result<bool> {
    let name = component_name(filename, description)?;
    let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: parent_directory 是有效 fd，metadata 指向可写未初始化存储，name 是 NUL 终止字符串。
    let result = unsafe {
        libc::fstatat(
            parent_directory.as_raw_fd(),
            name.as_ptr(),
            metadata.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result == 0 {
        return Ok(true);
    }
    let error = std::io::Error::last_os_error();
    if error.kind() == std::io::ErrorKind::NotFound {
        Ok(false)
    } else {
        Err(error).context("检查路径目录项失败")
    }
}

/// 非 Unix 不检查 sidecar 文件系统目录项。
#[cfg(not(unix))]
pub fn path_entry_exists(_path: &Path) -> Result<bool> {
    ensure_platform_supported()?;
    bail!("sidecar_hardening_unavailable")
}

/// 在同一父目录中原子替换私有文件，并在 Unix 上固定为 owner-only `0600`。
///
/// Task 12 当前只读 Host 的 runtime config；该通用 helper 为后续 session journal 保留
/// 同目录临时文件、文件同步、rename 和父目录同步的安全写入语义。
#[cfg(unix)]
pub fn atomic_write_private(path: &Path, content: &[u8]) -> Result<()> {
    require_absolute_path(path, "原子写目标")?;
    let filename = path.file_name().context("原子写目标必须是文件路径")?;
    let parent = path.parent().context("原子写目标缺少父目录")?;
    let parent_directory = open_existing_directory(parent, "原子写目标父目录")?;
    atomic_write_private_at(&parent_directory, filename, content, "原子写目标")
}

#[cfg(unix)]
static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[cfg(unix)]
struct PrivateTemporaryFile {
    file: File,
    parent_fd: libc::c_int,
    name: CString,
    committed: bool,
}

#[cfg(unix)]
impl Drop for PrivateTemporaryFile {
    fn drop(&mut self) {
        if !self.committed {
            // SAFETY: parent_fd 与 name 来自同一次受保护的 openat；清理失败不应覆盖原始错误。
            unsafe {
                libc::unlinkat(self.parent_fd, self.name.as_ptr(), 0);
            }
        }
    }
}

#[cfg(unix)]
fn atomic_write_private_at(
    parent: &File,
    filename: &std::ffi::OsStr,
    content: &[u8],
    description: &str,
) -> Result<()> {
    reject_final_symlink(parent, filename, description)?;
    let mut temporary = create_private_temp_file(parent, description)?;
    set_private_permissions(&temporary.file, FILE_MODE, "原子写临时文件")?;
    verify_private_file(&temporary.file, FILE_MODE, "原子写临时文件")?;
    temporary
        .file
        .write_all(content)
        .context("写入原子写临时文件失败")?;
    temporary
        .file
        .sync_all()
        .context("同步原子写临时文件失败")?;

    let target_name = component_name(filename, description)?;
    // SAFETY: parent 与临时文件均由同一父目录 fd 管理；renameat 不按外部路径重解析。
    let result = unsafe {
        libc::renameat(
            parent.as_raw_fd(),
            temporary.name.as_ptr(),
            parent.as_raw_fd(),
            target_name.as_ptr(),
        )
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error()).context("原子替换私有文件失败");
    }
    temporary.committed = true;
    sync_parent_directory(parent)
}

#[cfg(unix)]
fn create_private_temp_file(parent: &File, description: &str) -> Result<PrivateTemporaryFile> {
    for _ in 0..128 {
        let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let name = CString::new(format!(".efflab-sidecar-tmp-{counter}"))
            .context("固定原子写临时文件名无效")?;
        // SAFETY: parent 是有效目录 fd，name 是单一 NUL 终止目录项；O_EXCL 防止名称碰撞覆盖。
        let fd = unsafe {
            libc::openat(
                parent.as_raw_fd(),
                name.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                FILE_MODE as libc::mode_t as libc::c_uint,
            )
        };
        if fd >= 0 {
            // SAFETY: fd 是刚由 openat 返回、尚未被其他 owner 管理的有效 fd。
            return Ok(PrivateTemporaryFile {
                file: unsafe { File::from_raw_fd(fd) },
                parent_fd: parent.as_raw_fd(),
                name,
                committed: false,
            });
        }

        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::EEXIST) {
            continue;
        }
        return Err(error).with_context(|| format!("无法创建 {description} 原子写临时文件"));
    }

    bail!("无法为 {description} 原子写临时文件分配唯一名称")
}

/// 非 Unix 不执行私有文件原子写。
#[cfg(not(unix))]
pub fn atomic_write_private(_path: &Path, _content: &[u8]) -> Result<()> {
    ensure_platform_supported()
}

/// 清空进程环境，只恢复固定平台变量和短生命周期 L3b 绑定令牌。
///
/// 调用方必须在创建 Tokio runtime 或任何并发任务之前调用；Rust 2024 的环境变量
/// 修改 API 因此集中在这个启动阶段的单线程边界内。
pub fn sanitize_env() -> Result<()> {
    let allowed: Vec<OsString> = runtime_environment_allowlist()
        .iter()
        .chain(std::iter::once(&L3B_BIND_ENV))
        .map(OsString::from)
        .collect();
    let existing_keys: Vec<OsString> = env::vars_os().map(|(key, _)| key).collect();

    // SAFETY: main 在创建 runtime 前调用本函数；该时序禁止其他 sidecar 线程访问环境。
    unsafe {
        for key in existing_keys {
            if !allowed.iter().any(|allowed_key| allowed_key == &key) {
                env::remove_var(key);
            }
        }
    }
    Ok(())
}

/// 进程启动所需的最小平台环境；代理、凭据、telemetry 和用户开关均不在此列。
#[cfg(target_os = "macos")]
pub(crate) fn runtime_environment_allowlist() -> &'static [&'static str] {
    &["PATH", "HOME", "TMPDIR", "LANG", "LC_ALL"]
}

#[cfg(all(unix, not(target_os = "macos")))]
pub(crate) fn runtime_environment_allowlist() -> &'static [&'static str] {
    &["PATH", "HOME", "TMPDIR", "LANG", "LC_ALL"]
}

#[cfg(windows)]
pub(crate) fn runtime_environment_allowlist() -> &'static [&'static str] {
    &[
        "PATH",
        "HOME",
        "USERPROFILE",
        "TMP",
        "TEMP",
        "SystemRoot",
        "WINDIR",
        "ComSpec",
        "PATHEXT",
    ]
}

#[cfg(not(any(unix, windows)))]
pub(crate) fn runtime_environment_allowlist() -> &'static [&'static str] {
    &[]
}

/// 只允许绝对路径、有效 UTF-8、有限长度且拒绝词法 `..`。
pub(crate) fn require_absolute_path(path: &Path, description: &str) -> Result<()> {
    let value = path
        .to_str()
        .with_context(|| format!("{description} 必须是有效 UTF-8 路径"))?;
    // 复用 contract 的纯字符串 shape 校验，确保 Host/sidecar 对 session_cwd 的边界一致。
    efflab_agent_contract::validate_session_cwd(value)
        .with_context(|| format!("{description} 路径格式无效"))?;
    Ok(())
}

#[cfg(unix)]
fn reject_shared_home_root(path: &Path) -> Result<()> {
    reject_shared_directory_root(path, "私有 home")
}

/// 拒绝已知共享系统根目录，避免仅凭路径存在就当作隔离目录。
#[cfg(unix)]
fn reject_shared_directory_root(path: &Path, description: &str) -> Result<()> {
    if [
        "/",
        "/tmp",
        "/var",
        "/var/tmp",
        "/private",
        "/private/tmp",
        "/private/var",
        "/private/var/tmp",
        "/shared",
        "/Users",
        "/Volumes",
    ]
    .into_iter()
    .map(Path::new)
    .any(|shared_root| path == shared_root)
    {
        bail!("{description} 必须是专用叶目录");
    }
    Ok(())
}

#[cfg(unix)]
fn open_private_home_directory(home: &Path) -> Result<File> {
    require_absolute_path(home, "私有 home")?;
    reject_shared_home_root(home)?;
    let (directory, created) = open_directory_chain(home, true, "私有 home")?;
    if created {
        set_private_permissions(&directory, DIRECTORY_MODE, "新建私有 home")?;
    }
    verify_private_directory(&directory, "私有 home")?;
    Ok(directory)
}

#[cfg(unix)]
fn open_existing_private_home_directory(home: &Path) -> Result<File> {
    require_absolute_path(home, "私有 home")?;
    reject_shared_home_root(home)?;
    let (directory, _) = open_directory_chain(home, false, "私有 home")?;
    verify_private_directory(&directory, "私有 home")?;
    Ok(directory)
}

#[cfg(unix)]
fn open_existing_directory(path: &Path, description: &str) -> Result<File> {
    require_absolute_path(path, description)?;
    let (directory, _) = open_directory_chain(path, false, description)?;
    verify_directory(&directory, description)?;
    Ok(directory)
}

/// 打开并验证 session cwd 的最终目录必须属于当前用户且为 0700。
#[cfg(unix)]
fn open_existing_private_directory(path: &Path, description: &str) -> Result<File> {
    require_absolute_path(path, description)?;
    reject_shared_directory_root(path, description)?;
    let (directory, _) = open_directory_chain(path, false, description)?;
    verify_private_directory(&directory, description)?;
    Ok(directory)
}

#[cfg(unix)]
fn open_directory_chain(
    path: &Path,
    allow_create: bool,
    description: &str,
) -> Result<(File, bool)> {
    let components = normal_components(path, description)?;
    let mut current = open_root_directory().context("打开根目录失败")?;
    let mut final_created = false;

    for (index, component) in components.iter().enumerate() {
        let is_final = index + 1 == components.len();
        match try_open_directory_at(&current, component) {
            Ok(next) => {
                verify_directory(&next, description)?;
                current = next;
            }
            Err(error) if allow_create && error.kind() == std::io::ErrorKind::NotFound => {
                let name = component_name(component, description)?;
                let mut created = false;
                // SAFETY: current 是本函数通过 no-follow 方式取得的目录 fd，name 已校验无 NUL。
                let result = unsafe {
                    libc::mkdirat(
                        current.as_raw_fd(),
                        name.as_ptr(),
                        DIRECTORY_MODE as libc::mode_t,
                    )
                };
                if result == 0 {
                    created = true;
                } else {
                    let mkdir_error = std::io::Error::last_os_error();
                    if mkdir_error.raw_os_error() != Some(libc::EEXIST) {
                        return Err(mkdir_error).context("创建私有 home 目录失败");
                    }
                }
                let next = match try_open_directory_at(&current, component) {
                    Ok(next) => next,
                    Err(open_error) => {
                        if component_is_symlink(&current, component)? {
                            bail!("{description} 路径组件不能是符号链接");
                        }
                        return Err(secure_component_error(open_error, description));
                    }
                };
                verify_directory(&next, description)?;
                current = next;
                if is_final {
                    final_created = created;
                }
            }
            Err(error) => {
                if component_is_symlink(&current, component)? {
                    bail!("{description} 路径组件不能是符号链接");
                }
                return Err(secure_component_error(error, description));
            }
        }
    }

    Ok((current, final_created))
}

#[cfg(unix)]
fn normal_components(path: &Path, description: &str) -> Result<Vec<OsString>> {
    let mut components = Vec::new();
    for component in path.components() {
        match component {
            std::path::Component::RootDir | std::path::Component::CurDir => {}
            std::path::Component::Normal(value) => components.push(value.to_os_string()),
            std::path::Component::ParentDir => bail!("{description} 不允许包含 .."),
            std::path::Component::Prefix(_) => bail!("{description} 路径前缀不受支持"),
        }
    }
    Ok(components)
}

#[cfg(unix)]
fn open_root_directory() -> std::io::Result<File> {
    let name = CString::new("/")
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "固定根路径无效"))?;
    // SAFETY: name 是 NUL 终止的固定路径，返回 fd 的所有权立即交给 File。
    let fd = unsafe {
        libc::open(
            name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
            0,
        )
    };
    if fd < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        // SAFETY: fd 是刚由 libc::open 返回、尚未被其他 owner 管理的有效 fd。
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}

#[cfg(unix)]
fn try_open_directory_at(parent: &File, component: &OsString) -> std::io::Result<File> {
    let name = CString::new(component.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "NUL"))?;
    // SAFETY: parent 是有效目录 fd，name 是 NUL 终止的单一目录项。
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            0,
        )
    };
    if fd < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        // SAFETY: fd 是刚由 openat 返回、尚未被其他 owner 管理的有效 fd。
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}

#[cfg(unix)]
fn component_name(component: &std::ffi::OsStr, description: &str) -> Result<CString> {
    CString::new(component.as_bytes()).with_context(|| format!("{description} 路径组件不合法"))
}

#[cfg(unix)]
fn component_is_symlink(parent: &File, component: &OsString) -> Result<bool> {
    let name = component_name(component.as_os_str(), "路径")?;
    let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: parent 是有效目录 fd，metadata 指向可写存储，AT_SYMLINK_NOFOLLOW 不解析目录项。
    let result = unsafe {
        libc::fstatat(
            parent.as_raw_fd(),
            name.as_ptr(),
            metadata.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result != 0 {
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::NotFound {
            return Ok(false);
        }
        return Err(error).context("检查路径组件失败");
    }
    // SAFETY: fstatat 成功初始化 metadata。
    let metadata = unsafe { metadata.assume_init() };
    Ok((metadata.st_mode as libc::mode_t & libc::S_IFMT) == libc::S_IFLNK)
}

#[cfg(unix)]
fn secure_component_error(error: std::io::Error, description: &str) -> anyhow::Error {
    if matches!(error.raw_os_error(), Some(libc::ELOOP) | Some(libc::EMLINK)) {
        anyhow::anyhow!("{description} 路径组件不能是符号链接")
    } else {
        anyhow::Error::new(error).context(format!("打开 {description} 路径组件失败"))
    }
}

#[cfg(unix)]
fn verify_directory(file: &File, description: &str) -> Result<()> {
    let metadata = file
        .metadata()
        .with_context(|| format!("读取 {description} 目录元数据失败"))?;
    if !metadata.is_dir() {
        bail!("{description} 必须是常规目录");
    }
    Ok(())
}

/// 仅验证 POSIX mode/owner；macOS/Linux 没有本边界可复用的统一扩展 ACL 检测，因此不宣称 ACL 已验证。
#[cfg(unix)]
fn verify_private_directory(file: &File, description: &str) -> Result<()> {
    verify_directory(file, description)?;
    let metadata = file
        .metadata()
        .with_context(|| format!("读取 {description} 私有元数据失败"))?;
    if metadata.uid() != unsafe { libc::geteuid() } {
        bail!("{description} 必须属于当前用户");
    }
    let mode = metadata.mode() & 0o7777;
    if mode != DIRECTORY_MODE {
        bail!("{description} 已存在时权限必须为 0700");
    }
    Ok(())
}

#[cfg(unix)]
fn open_file_at(parent: &File, filename: &std::ffi::OsStr, description: &str) -> Result<File> {
    let name = component_name(filename, description)?;
    // O_NONBLOCK 防止 FIFO 在完成 regular-file 检查前阻塞启动线程。
    // SAFETY: parent 是有效目录 fd，name 是单一 NUL 终止目录项。
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_NONBLOCK | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            0,
        )
    };
    if fd < 0 {
        return Err(secure_component_error(
            std::io::Error::last_os_error(),
            description,
        ));
    }
    // SAFETY: fd 是刚由 openat 返回、尚未被其他 owner 管理的有效 fd。
    Ok(unsafe { File::from_raw_fd(fd) })
}

#[cfg(unix)]
fn open_private_lock(home: &File) -> Result<File> {
    let name = CString::new(HOME_LOCK_FILENAME).context("固定 home 锁文件名无效")?;
    // 先使用 O_EXCL 创建新文件，避免把已有共享权限文件 chmod 成私有文件。
    // SAFETY: home 是有效私有目录 fd，name 是固定 NUL 终止目录项。
    let first_fd = unsafe {
        libc::openat(
            home.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDWR | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            FILE_MODE as libc::mode_t as libc::c_uint,
        )
    };
    let (file, created) = if first_fd >= 0 {
        // SAFETY: first_fd 是刚由 openat 返回、尚未被其他 owner 管理的有效 fd。
        (unsafe { File::from_raw_fd(first_fd) }, true)
    } else {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::EEXIST) {
            return Err(secure_component_error(error, "私有 home 锁文件"));
        }
        // SAFETY: home 是有效私有目录 fd，name 是固定 NUL 终止目录项。
        let existing_fd = unsafe {
            libc::openat(
                home.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDWR | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                0,
            )
        };
        if existing_fd < 0 {
            return Err(secure_component_error(
                std::io::Error::last_os_error(),
                "私有 home 锁文件",
            ));
        }
        // SAFETY: existing_fd 是刚由 openat 返回、尚未被其他 owner 管理的有效 fd。
        (unsafe { File::from_raw_fd(existing_fd) }, false)
    };

    if created {
        set_private_permissions(&file, FILE_MODE, "新建私有 home 锁文件")?;
    }
    verify_private_file(&file, FILE_MODE, "私有 home 锁文件")?;
    Ok(file)
}

#[cfg(unix)]
fn verify_private_file(file: &File, required_mode: u32, description: &str) -> Result<()> {
    let metadata = file
        .metadata()
        .with_context(|| format!("读取 {description} 元数据失败"))?;
    if !metadata.is_file() {
        bail!("{description} 必须是常规文件");
    }
    if metadata.uid() != unsafe { libc::geteuid() } {
        bail!("{description} 必须属于当前用户");
    }
    if metadata.nlink() != 1 {
        bail!("{description} 不能是硬链接");
    }
    let mode = metadata.mode() & 0o7777;
    if mode != required_mode {
        bail!("{description} 权限必须为 {required_mode:04o}");
    }
    Ok(())
}

#[cfg(unix)]
fn set_private_permissions(file: &File, mode: u32, description: &str) -> Result<()> {
    // SAFETY: file 借用有效 fd；fchmod 只修改该 fd 指向的 inode 权限，不重新解析路径。
    let result = unsafe { libc::fchmod(file.as_raw_fd(), mode as libc::mode_t) };
    if result != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("设置 {description} 权限失败"));
    }
    Ok(())
}

#[cfg(unix)]
fn reject_final_symlink(
    parent: &File,
    filename: &std::ffi::OsStr,
    description: &str,
) -> Result<()> {
    let name = component_name(filename, description)?;
    let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: parent 是有效 fd，metadata 指向可写存储，AT_SYMLINK_NOFOLLOW 不解析最终链接。
    let result = unsafe {
        libc::fstatat(
            parent.as_raw_fd(),
            name.as_ptr(),
            metadata.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result != 0 {
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::NotFound {
            return Ok(());
        }
        return Err(error).context("检查原子写目标失败");
    }
    // SAFETY: fstatat 成功初始化 metadata。
    let metadata = unsafe { metadata.assume_init() };
    if (metadata.st_mode as libc::mode_t & libc::S_IFMT) == libc::S_IFLNK {
        bail!("{description} 不能是符号链接");
    }
    Ok(())
}

#[cfg(unix)]
fn sync_parent_directory(parent: &File) -> Result<()> {
    parent.sync_all().context("同步原子写父目录失败")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::env;
    use std::ffi::OsString;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;

    use super::*;

    static ENVIRONMENT_TEST_LOCK: Mutex<()> = Mutex::new(());
    static CURRENT_DIRECTORY_TEST_LOCK: Mutex<()> = Mutex::new(());

    struct CurrentDirectoryRestore(PathBuf);

    impl Drop for CurrentDirectoryRestore {
        fn drop(&mut self) {
            let _ = env::set_current_dir(&self.0);
        }
    }

    struct EnvironmentRestore {
        previous: Vec<(OsString, Option<OsString>)>,
    }

    impl EnvironmentRestore {
        fn capture(keys: impl IntoIterator<Item = OsString>) -> Self {
            let mut previous = Vec::new();
            for key in keys {
                if previous.iter().any(|(existing, _)| existing == &key) {
                    continue;
                }
                previous.push((key.clone(), env::var_os(&key)));
            }
            Self { previous }
        }
    }

    impl Drop for EnvironmentRestore {
        fn drop(&mut self) {
            // SAFETY: 测试持有 ENVIRONMENT_TEST_LOCK，恢复阶段没有并发环境访问者。
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

    #[test]
    fn sanitize_env_keeps_only_platform_runtime_allowlist_and_l3b_bind() {
        let _lock = ENVIRONMENT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut keys: Vec<OsString> = env::vars_os().map(|(key, _)| key).collect();
        keys.extend(
            [
                "EFFLAB_L3B_BIND",
                "XAI_API_KEY",
                "GROK_HOME",
                "HTTP_PROXY",
                "HTTPS_PROXY",
                "ALL_PROXY",
                "NO_PROXY",
                "OTEL_EXPORTER_OTLP_ENDPOINT",
                "DYLD_LIBRARY_PATH",
                "DYLD_FALLBACK_LIBRARY_PATH",
                "DYLD_FRAMEWORK_PATH",
                "DYLD_INSERT_LIBRARIES",
                "LD_LIBRARY_PATH",
                "LD_PRELOAD",
                "RUST_LOG",
                "EFFLAB_UNREGISTERED_ENV",
            ]
            .into_iter()
            .map(OsString::from),
        );
        let _restore = EnvironmentRestore::capture(keys);

        // SAFETY: 测试持有环境锁，且 guard 会恢复所有被测试修改的变量。
        unsafe {
            env::set_var("EFFLAB_L3B_BIND", "bind-sentinel");
            env::set_var("XAI_API_KEY", "user-key-sentinel");
            env::set_var("GROK_HOME", "/user/home");
            env::set_var("HTTP_PROXY", "http://proxy.invalid");
            env::set_var("HTTPS_PROXY", "https://proxy.invalid");
            env::set_var("ALL_PROXY", "socks5://proxy.invalid");
            env::set_var("NO_PROXY", "localhost");
            env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel.invalid");
            env::set_var("DYLD_LIBRARY_PATH", "dyld-library-sentinel");
            env::set_var("DYLD_FALLBACK_LIBRARY_PATH", "dyld-fallback-sentinel");
            env::set_var("DYLD_FRAMEWORK_PATH", "dyld-framework-sentinel");
            env::set_var("DYLD_INSERT_LIBRARIES", "dyld-insert-sentinel");
            env::set_var("LD_LIBRARY_PATH", "ld-library-sentinel");
            env::set_var("LD_PRELOAD", "ld-preload-sentinel");
            env::set_var("RUST_LOG", "trace");
            env::set_var("EFFLAB_UNREGISTERED_ENV", "must-drop");
        }

        sanitize_env().expect("环境 allowlist 清理应成功");

        let allowed: BTreeSet<OsString> = runtime_environment_allowlist()
            .iter()
            .chain(std::iter::once(&L3B_BIND_ENV))
            .map(OsString::from)
            .collect();
        let unexpected: Vec<_> = env::vars_os()
            .map(|(key, _)| key)
            .filter(|key| !allowed.contains(key))
            .collect();
        assert!(
            unexpected.is_empty(),
            "环境清理只能保留固定平台变量与 L3b token，多出: {unexpected:?}"
        );
        assert_eq!(
            env::var_os(L3B_BIND_ENV),
            Some(OsString::from("bind-sentinel"))
        );
        for key in [
            "XAI_API_KEY",
            "GROK_HOME",
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "NO_PROXY",
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "DYLD_LIBRARY_PATH",
            "DYLD_FALLBACK_LIBRARY_PATH",
            "DYLD_FRAMEWORK_PATH",
            "DYLD_INSERT_LIBRARIES",
            "LD_LIBRARY_PATH",
            "LD_PRELOAD",
            "RUST_LOG",
            "EFFLAB_UNREGISTERED_ENV",
        ] {
            assert!(env::var_os(key).is_none(), "变量 {key} 不得保留");
        }
    }

    #[test]
    fn relative_home_is_rejected_without_creation() {
        let result = prepare_private_home(Path::new("relative-home"));
        assert!(result.is_err());
    }

    #[cfg(unix)]
    #[test]
    fn shared_system_roots_are_rejected_as_home() {
        for path in [Path::new("/"), Path::new("/tmp"), Path::new("/shared")] {
            assert!(
                validate_private_home_path(path).is_err(),
                "共享系统根目录必须在创建或配置读取前拒绝: {path:?}"
            );
            assert!(prepare_private_home(path).is_err());
        }
    }

    #[cfg(unix)]
    #[test]
    fn existing_home_and_lock_keep_private_modes() {
        use std::os::unix::fs::PermissionsExt;

        let temporary = tempfile::tempdir().expect("创建临时目录");
        let home = temporary.path().join("home");
        fs::create_dir(&home).expect("创建 home");
        fs::set_permissions(&home, fs::Permissions::from_mode(DIRECTORY_MODE))
            .expect("设置 home 私有权限");

        let _lock = acquire_home_lock(&home).expect("获取 home 锁");
        let home_mode = fs::metadata(&home).expect("读取 home").permissions().mode() & 0o777;
        let lock_mode = fs::metadata(home.join(HOME_LOCK_FILENAME))
            .expect("读取 lock")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(home_mode, DIRECTORY_MODE);
        assert_eq!(lock_mode, FILE_MODE);
    }

    #[cfg(unix)]
    #[test]
    fn existing_home_with_shared_permissions_is_rejected_without_chmod() {
        use std::os::unix::fs::PermissionsExt;

        let temporary = tempfile::tempdir().expect("创建临时目录");
        let home = temporary.path().join("shared-home");
        fs::create_dir(&home).expect("创建 shared home");
        fs::set_permissions(&home, fs::Permissions::from_mode(0o755))
            .expect("设置 shared home 权限");

        let error = prepare_private_home(&home).expect_err("共享 home 必须拒绝");

        assert!(format!("{error:#}").contains("0700"));
        assert_eq!(
            fs::metadata(&home)
                .expect("读取 shared home")
                .permissions()
                .mode()
                & 0o777,
            0o755,
            "拒绝共享 home 时不得 chmod"
        );
    }

    #[cfg(unix)]
    #[test]
    fn path_validation_rejects_existing_shared_home_before_config_access() {
        use std::os::unix::fs::PermissionsExt;

        let temporary = tempfile::tempdir().expect("创建临时目录");
        let home = temporary.path().join("shared-home");
        fs::create_dir(&home).expect("创建 shared home");
        fs::set_permissions(&home, fs::Permissions::from_mode(0o755))
            .expect("设置 shared home 权限");

        let error = validate_private_home_path(&home).expect_err("共享 home 路径必须预先拒绝");

        assert!(format!("{error:#}").contains("0700"));
        assert_eq!(
            fs::metadata(&home)
                .expect("读取 shared home")
                .permissions()
                .mode()
                & 0o777,
            0o755,
            "路径预校验拒绝时不得 chmod"
        );
    }

    #[cfg(unix)]
    #[test]
    fn existing_lock_with_shared_permissions_is_rejected_without_chmod() {
        use std::os::unix::fs::PermissionsExt;

        let temporary = tempfile::tempdir().expect("创建临时目录");
        let home = temporary.path().join("home");
        fs::create_dir(&home).expect("创建 home");
        fs::set_permissions(&home, fs::Permissions::from_mode(DIRECTORY_MODE))
            .expect("设置 home 权限");
        let lock_path = home.join(HOME_LOCK_FILENAME);
        fs::write(&lock_path, b"").expect("创建 lock");
        fs::set_permissions(&lock_path, fs::Permissions::from_mode(0o644)).expect("设置 lock 权限");

        let error = acquire_home_lock(&home).expect_err("共享 lock 必须拒绝");

        assert!(format!("{error:#}").contains("0600"));
        assert_eq!(
            fs::metadata(lock_path)
                .expect("读取 lock")
                .permissions()
                .mode()
                & 0o777,
            0o644,
            "拒绝共享 lock 时不得 chmod"
        );
    }

    #[cfg(unix)]
    #[test]
    fn absolute_path_validation_rejects_oversized_and_non_utf8_paths() {
        use std::os::unix::ffi::OsStringExt;

        let oversized = Path::new("/").join("x".repeat(4096));
        assert!(
            format!(
                "{:#}",
                require_absolute_path(&oversized, "path").unwrap_err()
            )
            .contains("4096")
        );

        let raw_path = OsString::from_vec(vec![b'/', 0xff]);
        let non_utf8 = Path::new(raw_path.as_os_str());
        let error = require_absolute_path(non_utf8, "path").expect_err("非 UTF-8 路径必须拒绝");
        assert!(format!("{error:#}").contains("UTF-8"));
    }

    #[cfg(unix)]
    #[test]
    fn atomic_write_private_replaces_content_with_0600_file() {
        use std::os::unix::fs::PermissionsExt;

        let temporary = tempfile::tempdir().expect("创建临时目录");
        let target = temporary.path().join("session-record");
        atomic_write_private(&target, b"safe = true\n").expect("原子写私有文件");

        assert_eq!(fs::read(&target).expect("读取原子写文件"), b"safe = true\n");
        assert_eq!(
            fs::metadata(&target)
                .expect("读取原子写文件权限")
                .permissions()
                .mode()
                & 0o777,
            FILE_MODE
        );
    }

    #[cfg(unix)]
    #[test]
    fn atomic_write_private_uses_open_parent_after_path_replacement() {
        use std::ffi::OsStr;

        let temporary = tempfile::tempdir().expect("创建临时目录");
        let parent = temporary.path().join("parent");
        let moved_parent = temporary.path().join("moved-parent");
        fs::create_dir(&parent).expect("创建原子写父目录");
        let parent_directory =
            open_existing_directory(&parent, "原子写父目录").expect("打开原子写父目录句柄");

        fs::rename(&parent, &moved_parent).expect("移动原子写父目录");
        fs::create_dir(&parent).expect("创建替换后的同名父目录");

        atomic_write_private_at(
            &parent_directory,
            OsStr::new("session-record"),
            b"safe = true\n",
            "原子写目标",
        )
        .expect("原子写必须使用已打开的父目录句柄");

        assert_eq!(
            fs::read(moved_parent.join("session-record")).expect("读取原父目录中的原子写文件"),
            b"safe = true\n"
        );
        assert!(
            !parent.join("session-record").exists(),
            "父目录路径被替换后不得写入新的同名目录"
        );
    }

    #[cfg(unix)]
    #[test]
    fn startup_handles_keep_home_config_and_session_fds_after_path_replacement() {
        use std::os::unix::fs::PermissionsExt;

        let _cwd_lock = CURRENT_DIRECTORY_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let temporary = tempfile::tempdir().expect("创建连续 fd fixture");
        let home = temporary.path().join("home");
        let session = temporary.path().join("session");
        let runtime_config = home.join("runtime-config.v1.toml");
        let moved_home = temporary.path().join("moved-home");
        let moved_session = temporary.path().join("moved-session");
        fs::create_dir(&home).expect("创建 home");
        fs::create_dir(&session).expect("创建 session");
        fs::set_permissions(&home, fs::Permissions::from_mode(DIRECTORY_MODE))
            .expect("设置 home 权限");
        fs::set_permissions(&session, fs::Permissions::from_mode(DIRECTORY_MODE))
            .expect("设置 session 权限");
        fs::write(&runtime_config, b"session_cwd = \"original\"\n").expect("创建 runtime config");
        fs::set_permissions(&runtime_config, fs::Permissions::from_mode(FILE_MODE))
            .expect("设置 runtime config 权限");

        let handles = open_startup_handles(&home, &session).expect("打开连续启动 fd");

        fs::rename(&home, &moved_home).expect("移动原 home");
        fs::create_dir(&home).expect("创建替换 home");
        fs::set_permissions(&home, fs::Permissions::from_mode(DIRECTORY_MODE))
            .expect("设置替换 home 权限");
        fs::rename(&session, &moved_session).expect("移动原 session");
        fs::create_dir(&session).expect("创建替换 session");
        fs::set_permissions(&session, fs::Permissions::from_mode(DIRECTORY_MODE))
            .expect("设置替换 session 权限");

        let source = handles
            .read_private_runtime_config(&runtime_config)
            .expect("runtime config 应从原 home fd 读取");
        assert_eq!(source, "session_cwd = \"original\"\n");

        let _lock = handles
            .acquire_home_lock()
            .expect("home lock 应从原 home fd 获取");
        assert!(
            moved_home.join(HOME_LOCK_FILENAME).exists(),
            "lock 必须创建在已打开的原 home 中"
        );
        assert!(
            !home.join(HOME_LOCK_FILENAME).exists(),
            "路径替换后的同名 home 不得收到 lock"
        );

        let original_cwd = env::current_dir().expect("读取测试 cwd");
        let _restore_cwd = CurrentDirectoryRestore(original_cwd);
        handles
            .set_current_dir_secure()
            .expect("session cwd 应从原 session fd 切换");
        assert_eq!(env::current_dir().expect("读取切换后的 cwd"), moved_session);
    }

    #[cfg(unix)]
    #[test]
    fn second_lock_is_rejected_without_blocking() {
        let temporary = tempfile::tempdir().expect("创建临时目录");
        let home = temporary.path().join("home");
        let _first = acquire_home_lock(&home).expect("获取第一把锁");
        let error = acquire_home_lock(&home).expect_err("第二把锁必须被拒绝");
        assert!(format!("{error:#}").contains("拒绝并发启动"));
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_home_lock_is_rejected() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let temporary = tempfile::tempdir().expect("创建临时目录");
        let home = temporary.path().join("home");
        let outside = temporary.path().join("outside.lock");
        fs::create_dir(&home).expect("创建 home");
        fs::set_permissions(&home, fs::Permissions::from_mode(DIRECTORY_MODE))
            .expect("设置 home 权限");
        fs::write(&outside, b"outside").expect("创建外部锁文件");
        symlink(&outside, home.join(HOME_LOCK_FILENAME)).expect("创建锁符号链接");

        let error = acquire_home_lock(&home).expect_err("符号链接锁必须拒绝");
        assert!(format!("{error:#}").contains("符号链接"));
    }
}
