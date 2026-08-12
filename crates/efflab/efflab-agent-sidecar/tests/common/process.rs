//! sidecar 进程监督器。
//!
//! 职责：以隔离 env 拉起 `efflab-agent-sidecar`，stdout/stderr 分离读取，
//! 保证测试结束前**总会** wait 或 kill 子进程（devplan R14'）。
//!
//! 注意：本文件针对 `std::process::Child` 的未读 stdout/stderr 处理做了局部
//! clippy 允许——`kill_on_drop` 语义由本模块的显式收尾保证，而非依赖 drop。

#![allow(clippy::zombie_processes)]

use std::io::{BufReader, Read};
use std::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// 二进制路径：由 cargo 在编译期注入（仅对 integration test target 生效）。
pub const SIDECAR_BIN: &str = env!("CARGO_BIN_EXE_efflab-agent-sidecar");

/// 测试用的 BYOK 假 key（P3 链路不依赖真实模型调用；仅证明启动/握手不触网）。
pub const FAKE_XAI_API_KEY: &str = "efflab-test-fake-key";
/// 优雅终止 sidecar 的最长等待时间，超时后升级为强制杀死。
const TERMINATE_GRACE_PERIOD: Duration = Duration::from_secs(5);

/// 返回经过显式白名单筛选的子进程环境。
///
/// 调用方必须先执行 `Command::env_clear()`；这里仅保留 sidecar 运行所需的平台
/// 变量及假 BYOK key，绝不继承调用测试进程中未登记的父环境。
pub fn isolated_env() -> Vec<(String, String)> {
    let mut env = Vec::new();
    for key in platform_environment_allowlist() {
        if let Some(value) = std::env::var_os(key) {
            env.push((key.to_string(), value.to_string_lossy().into_owned()));
        }
    }
    env.push(("XAI_API_KEY".to_string(), FAKE_XAI_API_KEY.to_string()));
    env
}

/// 返回各平台 `env_clear` 后仍可能被动态链接器或运行时需要的变量白名单。
fn platform_environment_allowlist() -> &'static [&'static str] {
    #[cfg(target_os = "macos")]
    {
        &[
            "PATH",
            "HOME",
            "TMPDIR",
            "LANG",
            "LC_ALL",
            "DYLD_LIBRARY_PATH",
        ]
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        &[
            "PATH",
            "HOME",
            "TMPDIR",
            "LANG",
            "LC_ALL",
            "LD_LIBRARY_PATH",
        ]
    }
    #[cfg(windows)]
    {
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
}

/// 对命令应用全量清空后的隔离环境，额外变量最后覆盖基线。
pub fn apply_isolated_env(cmd: &mut Command, extra_env: &[(String, String)]) {
    cmd.env_clear();
    cmd.envs(isolated_env());
    for (name, value) in extra_env {
        if value.is_empty() {
            cmd.env_remove(name);
        } else {
            cmd.env(name, value);
        }
    }
}

/// 运行中的 sidecar 进程句柄。
pub struct SidecarProcess {
    child: Child,
    /// stderr 后台 drain 线程，防止管道写满阻塞子进程。
    _stderr_drain: Option<JoinHandle<()>>,
    /// drain 线程结束后一次性送达的 stderr 字节。
    stderr_rx: Receiver<Vec<u8>>,
    /// 已获取的 stderr 文本缓存，使诊断可重复读取而不丢失首个结果。
    stderr_cache: std::sync::Mutex<Option<String>>,
}

impl SidecarProcess {
    /// 以隔离 env 启动 sidecar。
    ///
    /// `extra_env`：`(name, value)` 覆盖；空 value 表示删除该变量。
    pub fn spawn(
        grok_home: &std::path::Path,
        session_cwd: &std::path::Path,
        extra_args: &[String],
        extra_env: &[(String, String)],
    ) -> Self {
        let mut cmd = Command::new(SIDECAR_BIN);
        cmd.arg("--grok-home")
            .arg(grok_home)
            .arg("--session-cwd")
            .arg(session_cwd);
        for arg in extra_args {
            cmd.arg(arg);
        }
        // 先完全清空父环境，再仅注入白名单和测试显式覆盖项。
        apply_isolated_env(&mut cmd, extra_env);
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        // clippy.toml 禁用 Command::spawn（防未收编子进程逃逸会话）；测试中的
        // sidecar 子进程由本结构体的 Drop/wait/kill 兜底保证总是被回收（devplan R14'）。
        #[allow(clippy::disallowed_methods)]
        let mut child = cmd.spawn().expect("spawn sidecar 二进制失败");

        // stderr 后台 drain。
        let mut stderr: ChildStderr = child.stderr.take().expect("stderr piped");
        let (tx, rx) = mpsc::channel();
        let handle = std::thread::spawn(move || {
            let mut buf = Vec::new();
            let _ = stderr.read_to_end(&mut buf);
            let _ = tx.send(buf);
        });

        Self {
            child,
            _stderr_drain: Some(handle),
            stderr_rx: rx,
            stderr_cache: std::sync::Mutex::new(None),
        }
    }

    /// stdout 行读取器（take 所有权；调用方需保证只 take 一次）。
    pub fn stdout_reader(&mut self) -> BufReader<ChildStdout> {
        BufReader::new(self.child.stdout.take().expect("stdout piped"))
    }

    /// take 出 stdin（转移给 ACP 客户端）。
    pub fn take_stdin(&mut self) -> ChildStdin {
        self.child.stdin.take().expect("stdin piped")
    }

    /// 等待进程退出（超时返回 None；调用方应随后 kill）。
    pub fn wait_timeout(&mut self, timeout: Duration) -> Option<ExitStatus> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait().expect("try_wait") {
                self.join_stderr_drain();
                return Some(status);
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// 先发送 SIGTERM，等待有限时间；未退出时升级 SIGKILL 并回收。
    ///
    /// 返回 `Some(status)` 表示已经回收；`None` 仅表示底层终止/回收异常，
    /// 调用方仍可调用 [`Self::kill`] 进行最后兜底。
    pub fn terminate(&mut self) -> Option<ExitStatus> {
        if let Some(status) = self.child.try_wait().ok().flatten() {
            self.join_stderr_drain();
            return Some(status);
        }

        #[cfg(unix)]
        {
            // 本 crate 未直接依赖 libc/nix；使用 macOS/Unix 提供的 kill 工具请求 SIGTERM。
            let signal_status = Command::new("/bin/kill")
                .arg("-TERM")
                .arg(self.child.id().to_string())
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
            if !matches!(signal_status, Ok(status) if status.success()) {
                if let Some(status) = self.child.try_wait().ok().flatten() {
                    self.join_stderr_drain();
                    return Some(status);
                }
                self.kill();
                return None;
            }
        }
        #[cfg(not(unix))]
        {
            self.kill();
            return None;
        }

        if let Some(status) = self.wait_timeout(TERMINATE_GRACE_PERIOD) {
            self.join_stderr_drain();
            return Some(status);
        }

        self.kill();
        self.child.try_wait().ok().flatten()
    }

    /// 强制终止并回收（幂等，作为 `terminate` 的升级兜底）。
    pub fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.join_stderr_drain();
    }

    /// 等待 stderr drain 线程结束并缓存输出，确保进程回收后诊断可重复读取。
    fn join_stderr_drain(&mut self) {
        if let Some(handle) = self._stderr_drain.take() {
            let _ = handle.join();
        }
        let mut cache = self.stderr_cache.lock().expect("stderr 缓存锁不应中毒");
        if cache.is_none()
            && let Ok(bytes) = self.stderr_rx.try_recv()
        {
            *cache = Some(String::from_utf8_lossy(&bytes).into_owned());
        }
    }

    /// 已 drain 的 stderr 内容（进程退出后完整可用，可重复读取）。
    pub fn stderr_text(&self) -> String {
        let mut cache = self.stderr_cache.lock().expect("stderr 缓存锁不应中毒");
        if cache.is_none()
            && let Ok(bytes) = self.stderr_rx.try_recv()
        {
            *cache = Some(String::from_utf8_lossy(&bytes).into_owned());
        }
        cache.clone().unwrap_or_default()
    }
}

impl Drop for SidecarProcess {
    /// 兜底：测试失败/panic 时先请求优雅退出，再在有限时间后强制回收子进程。
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.terminate();
        }
    }
}
