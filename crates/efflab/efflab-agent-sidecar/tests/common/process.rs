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

/// 受控环境：删除一切可能污染隔离的全局变量，注入 BYOK key。
pub fn isolated_env() -> Vec<(String, String)> {
    let mut env: Vec<(String, String)> = Vec::new();
    for key in [
        "GROK_HOME",
        "GROK_AGENT",
        "EFFLAB_GROK_HOME",
        "GROK_STORAGE_MODE",
        "GROK_SUBAGENTS",
        "GROK_MANAGED_MCPS_ENABLED",
        "GROK_MANAGED_MCP_GATEWAY_TOOLS_ENABLED",
        "GROK_EXTERNAL_OTEL",
        "XAI_API_KEY",
        "GROK_CODE_XAI_API_KEY",
    ] {
        env.push((key.to_string(), String::new())); // 空串占位，spawn 时表示"删除"
    }
    env.push(("XAI_API_KEY".to_string(), FAKE_XAI_API_KEY.to_string()));
    env
}

/// 运行中的 sidecar 进程句柄。
pub struct SidecarProcess {
    child: Child,
    /// stderr 后台 drain 线程，防止管道写满阻塞子进程。
    _stderr_drain: Option<JoinHandle<()>>,
    /// 供测试读取已 drain 的 stderr 文本。
    stderr_rx: Receiver<Vec<u8>>,
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
        // 先清理全局污染变量，再应用额外 env。
        for (name, value) in isolated_env() {
            if value.is_empty() {
                cmd.env_remove(&name);
            } else {
                cmd.env(name, value);
            }
        }
        for (name, value) in extra_env {
            if value.is_empty() {
                cmd.env_remove(name);
            } else {
                cmd.env(name, value);
            }
        }
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
                return Some(status);
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// 强制终止并回收（幂等）。
    pub fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(handle) = self._stderr_drain.take() {
            let _ = handle.join();
        }
    }

    /// 已 drain 的 stderr 内容（进程退出后完整可用）。
    pub fn stderr_text(&self) -> String {
        match self.stderr_rx.try_recv() {
            Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
            Err(_) => String::new(),
        }
    }
}

impl Drop for SidecarProcess {
    /// 兜底：测试失败/panic 时也回收子进程，避免僵尸。
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            self.kill();
        }
    }
}
