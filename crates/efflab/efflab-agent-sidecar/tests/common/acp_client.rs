//! 最小 ACP stdio JSON-RPC 客户端。
//!
//! stdout 由后台线程持续读取并解析为 `serde_json::Value`，通过 channel 送达；
//! `request` 发送后循环接收直到拿到匹配 `id` 的响应（跳过期间的通知），
//! 全部等待带超时，防止测试挂死。

use std::io::Write;
use std::process::ChildStdin;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread::JoinHandle;
use std::time::Duration;

use serde_json::Value;

/// ACP 通信错误。
#[derive(Debug)]
pub enum AcpError {
    /// 超时未收到匹配响应。
    Timeout(String),
    /// 收到 JSON-RPC error 响应。
    RpcError(Value),
    /// stdout 通道关闭（子进程退出）。
    ChannelClosed,
    /// 写入 stdin 失败。
    Write(std::io::Error),
}

impl std::fmt::Display for AcpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timeout(method) => write!(f, "等待 {method} 响应超时"),
            Self::RpcError(e) => write!(f, "JSON-RPC error: {e}"),
            Self::ChannelClosed => write!(f, "stdout channel closed"),
            Self::Write(e) => write!(f, "stdin write 失败: {e}"),
        }
    }
}

impl std::error::Error for AcpError {}

/// ACP stdio 客户端。
pub struct AcpClient {
    stdin: Option<ChildStdin>,
    rx: Receiver<Value>,
    /// stdout 读线程句柄（join 兜底）。
    _reader: Option<JoinHandle<()>>,
    next_id: u64,
}

impl AcpClient {
    /// 从 sidecar 进程构造客户端；`stdout` 为已 take 的 ChildStdout。
    pub fn new(stdin: ChildStdin, stdout: std::process::ChildStdout) -> Self {
        let (tx, rx) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            use std::io::BufRead;
            let mut reader = std::io::BufReader::new(stdout);
            let mut line = String::new();
            loop {
                line.clear();
                match reader.read_line(&mut line) {
                    Ok(0) => break, // EOF
                    Ok(_) => {
                        let trimmed = line.trim();
                        if !trimmed.is_empty()
                            && let Ok(value) = serde_json::from_str::<Value>(trimmed)
                            && tx.send(value).is_err()
                        {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            stdin: Some(stdin),
            rx,
            _reader: Some(reader),
            next_id: 1,
        }
    }

    /// 关闭 stdin（drop 句柄 → 管道关闭 → sidecar 读到 EOF 走正常关闭）。
    pub fn close_stdin(&mut self) {
        drop(self.stdin.take());
    }

    /// 发送一个请求并等待匹配 id 的响应（自动跳过期间的通知）。
    pub fn request(
        &mut self,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> Result<Value, AcpError> {
        let id = self.next_id;
        self.next_id += 1;
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        let line = serde_json::to_string(&request).expect("序列化请求");
        let stdin = self.stdin.as_mut().expect("stdin 已关闭");
        stdin
            .write_all(line.as_bytes())
            .and_then(|_| stdin.write_all(b"\n"))
            .and_then(|_| stdin.flush())
            .map_err(AcpError::Write)?;

        let deadline = std::time::Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return Err(AcpError::Timeout(method.to_string()));
            }
            match self.rx.recv_timeout(remaining) {
                Ok(value) => {
                    // 响应：有 id 且与请求匹配。
                    if value.get("id").and_then(Value::as_u64) == Some(id) {
                        if let Some(error) = value.get("error") {
                            return Err(AcpError::RpcError(error.clone()));
                        }
                        return Ok(value);
                    }
                    // 通知（method 字段存在、无匹配 id）→ 跳过。
                }
                Err(RecvTimeoutError::Timeout) => {
                    return Err(AcpError::Timeout(method.to_string()));
                }
                Err(RecvTimeoutError::Disconnected) => {
                    return Err(AcpError::ChannelClosed);
                }
            }
        }
    }
}
