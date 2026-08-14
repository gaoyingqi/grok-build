//! 可信 Host 的 ACP stdio JSON-RPC 运行时。
//!
//! 此模块只负责拆分后的 stdin/stdout 传输、消息复用与反向 RPC 回复校验；
//! 不启动 sidecar，也不把 ACP 类型泄漏到产品层。

use std::collections::{BTreeMap, btree_map::Entry};
use std::fmt;
#[cfg(not(unix))]
use std::io::{BufRead, BufReader};
use std::io::{ErrorKind, Read, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::unix::net::UnixStream;

use anyhow::{Context, Result, anyhow, bail};
use efflab_agent_contract::{HostPolicy, validate_host_request};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// JSON-RPC 2.0 的标准 `method_not_found` 错误码。
pub const METHOD_NOT_FOUND: i64 = -32601;

/// 入站消息队列的硬上限；溢出时终止传输并通过 `poll_inbound` 报错。
const MAX_INBOUND_QUEUE: usize = 64;
/// Host 或 sidecar 侧单向在途 request 账本的硬上限。
const MAX_PENDING_REQUESTS: usize = 64;

/// Host 使用的数值 JSON-RPC request id。
///
/// sidecar stdio 测试使用无符号整数 id，因此 Host 保持相同 wire 形状，避免引入
/// 不必要的 ACP 依赖或额外的字符串 id 约定。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RequestId(u64);

impl RequestId {
    /// 从 JSON-RPC 数值构造 request id。
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// 返回写入 JSON-RPC wire 的数值 id。
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u64> for RequestId {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// 入站 JSON-RPC error object 的最小本地表示。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RpcError {
    /// JSON-RPC 数值错误码。
    pub code: i64,
    /// 可展示的简短错误消息。
    pub message: String,
    /// 可选的结构化错误上下文。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

/// sidecar stdout 上由 Host 消费的三种 JSON-RPC 消息。
#[derive(Debug, Clone, PartialEq)]
pub enum Inbound {
    /// 对 Host 先前 request 的成功或错误响应。
    Response {
        id: RequestId,
        result: std::result::Result<Value, RpcError>,
    },
    /// 没有 id 的 ACP notification。
    Notification { method: String, params: Value },
    /// 带 id 的 sidecar→Host reverse request，必须由调用方回复。
    Request {
        id: RequestId,
        method: String,
        params: Value,
    },
}

/// 经 Runtime 验证后可写入反向 RPC response 的两种形状。
#[derive(Debug, Clone, PartialEq)]
pub enum ValidatedReply {
    /// 成功 result；permission 结果会按保存的 request options 额外校验。
    Result(Value),
    /// JSON-RPC error response，用于未知或无法处理的 reverse request。
    Error { code: i64, message: String },
}

/// 仍在等待响应或回复的 request 上下文。
#[derive(Debug, Clone)]
struct SavedRequest {
    method: String,
    params: Value,
}

/// 读端的可中断关闭控制；Unix 上使用独立 fd 唤醒 `poll`，避免 Drop 等待读端 EOF。
struct ReaderShutdown {
    requested: Arc<AtomicBool>,
    #[cfg(unix)]
    writer: Mutex<Option<UnixStream>>,
    #[cfg(not(unix))]
    sender: Mutex<Option<mpsc::Sender<()>>>,
}

impl ReaderShutdown {
    /// 请求 reader 退出；重复调用保持幂等。
    fn request(&self) {
        self.requested.store(true, Ordering::Release);
        #[cfg(unix)]
        if let Ok(mut writer) = self.writer.lock()
            && let Some(mut writer) = writer.take()
        {
            // reader 正在 poll 时，写入一个字节立即唤醒它；写失败只表示 reader 已结束。
            let _ = writer.write_all(&[1]);
            let _ = writer.flush();
        }

        #[cfg(not(unix))]
        if let Ok(mut sender) = self.sender.lock()
            && let Some(sender) = sender.take()
        {
            let _ = sender.send(());
        }
    }
}

/// 解码失败的处置级别：重复 reverse id 可报告后继续，协议/传输污染则终止 reader。
enum DecodeFailure {
    Recoverable(anyhow::Error),
    Fatal(anyhow::Error),
}

impl From<anyhow::Error> for DecodeFailure {
    fn from(error: anyhow::Error) -> Self {
        Self::Fatal(error)
    }
}

/// 拆分 stdin 写端与 stdout 读循环的 ACP Runtime。
pub struct AcpRuntime {
    /// Host 是 sidecar stdin 的唯一写入者；锁保证多个 `&self` 调用不会交错字节。
    stdin: Mutex<Option<Box<dyn Write + Send>>>,
    /// stdout 读线程不断推送消息，调用方以非阻塞方式轮询。
    inbound: Mutex<Receiver<Result<Inbound>>>,
    /// reader 终止后的固定错误状态；有序消息消费完后由 `poll_inbound` 返回。
    terminal_error: Arc<Mutex<Option<String>>>,
    /// 读端的关闭控制，确保显式 shutdown 与 Drop 能唤醒 reader。
    reader_shutdown: ReaderShutdown,
    /// 保存 reader worker，关闭时显式 join，避免后台线程脱离 Runtime 生命周期。
    reader_handle: Mutex<Option<JoinHandle<()>>>,
    /// Host 自己发出的数值 request id 分配器。
    next_request_id: AtomicU64,
    /// Host→sidecar request 的在途上下文，直到对应 response 抵达。
    outbound_requests: Arc<Mutex<BTreeMap<RequestId, SavedRequest>>>,
    /// sidecar→Host reverse request 的上下文，直到 `reply_validated` 成功写出。
    inbound_requests: Arc<Mutex<BTreeMap<RequestId, SavedRequest>>>,
    /// 串行化 reverse reply，避免两个调用者同时回复同一个账本项。
    inbound_reply_lock: Mutex<()>,
}

impl AcpRuntime {
    /// 使用调用方提供的已拆分 sidecar stdin/stdout 构造 Runtime。
    ///
    /// Unix 上 stdout 需要实现 [`AsRawFd`]，Runtime 通过 fd poll 与内部关闭管道
    /// 可中断阻塞读；这保证 Drop 能在 sidecar 孤儿仍持有 stdout 时回收 reader。
    /// stdout 立即交给独立线程读取，因此即使长 prompt 尚未收到 result，调用方仍可
    /// 经 [`Self::poll_inbound`] 接收 notification 与 reverse request。
    #[cfg(unix)]
    pub fn new<W, R>(stdin: W, stdout: R) -> Self
    where
        W: Write + Send + 'static,
        R: Read + Send + AsRawFd + 'static,
    {
        let (sender, receiver) = mpsc::sync_channel(MAX_INBOUND_QUEUE);
        let inbound_requests = Arc::new(Mutex::new(BTreeMap::new()));
        let outbound_requests = Arc::new(Mutex::new(BTreeMap::new()));
        let terminal_error = Arc::new(Mutex::new(None));
        let requested = Arc::new(AtomicBool::new(false));
        let (shutdown_read, shutdown_write) =
            UnixStream::pair().expect("创建 ACP reader shutdown 管道必须成功");
        let reader_shutdown = ReaderShutdown {
            requested: Arc::clone(&requested),
            writer: Mutex::new(Some(shutdown_write)),
        };

        // stdout 由独立线程独占，避免任何 request 等待路径吞掉中途 notification。
        let reader_inbound_requests = Arc::clone(&inbound_requests);
        let reader_outbound_requests = Arc::clone(&outbound_requests);
        let reader_terminal_error = Arc::clone(&terminal_error);
        let reader_handle = std::thread::spawn(move || {
            read_stdout_loop(
                stdout,
                shutdown_read,
                requested,
                reader_inbound_requests,
                reader_outbound_requests,
                reader_terminal_error,
                sender,
            );
        });

        Self {
            stdin: Mutex::new(Some(Box::new(stdin))),
            inbound: Mutex::new(receiver),
            terminal_error,
            reader_shutdown,
            reader_handle: Mutex::new(Some(reader_handle)),
            // 侧车现有 stdio 测试的 Host request id 从 1 开始。
            next_request_id: AtomicU64::new(1),
            outbound_requests,
            inbound_requests,
            inbound_reply_lock: Mutex::new(()),
        }
    }

    /// 非 Unix 平台的兼容构造器；读端仍由保存的 worker 管理，正常 EOF 可被观测。
    #[cfg(not(unix))]
    pub fn new<W, R>(stdin: W, stdout: R) -> Self
    where
        W: Write + Send + 'static,
        R: Read + Send + 'static,
    {
        let (sender, receiver) = mpsc::sync_channel(MAX_INBOUND_QUEUE);
        let (shutdown_sender, shutdown_receiver) = mpsc::channel();
        let inbound_requests = Arc::new(Mutex::new(BTreeMap::new()));
        let outbound_requests = Arc::new(Mutex::new(BTreeMap::new()));
        let terminal_error = Arc::new(Mutex::new(None));
        let requested = Arc::new(AtomicBool::new(false));
        let reader_shutdown = ReaderShutdown {
            requested: Arc::clone(&requested),
            sender: Mutex::new(Some(shutdown_sender)),
        };

        let reader_inbound_requests = Arc::clone(&inbound_requests);
        let reader_outbound_requests = Arc::clone(&outbound_requests);
        let reader_terminal_error = Arc::clone(&terminal_error);
        let reader_handle = std::thread::spawn(move || {
            read_stdout_loop(
                stdout,
                shutdown_receiver,
                requested,
                reader_inbound_requests,
                reader_outbound_requests,
                reader_terminal_error,
                sender,
            );
        });

        Self {
            stdin: Mutex::new(Some(Box::new(stdin))),
            inbound: Mutex::new(receiver),
            terminal_error,
            reader_shutdown,
            reader_handle: Mutex::new(Some(reader_handle)),
            next_request_id: AtomicU64::new(1),
            outbound_requests,
            inbound_requests,
            inbound_reply_lock: Mutex::new(()),
        }
    }

    /// 校验逻辑 method 后发送一个有 id 的 Host→sidecar request。
    pub fn request_validated(
        &self,
        method: &str,
        params: Value,
        policy: &HostPolicy,
    ) -> Result<RequestId> {
        validate_host_request(method, &params, policy)
            .map_err(|error| anyhow!("ACP request {method} 未通过 Host contract: {error}"))?;
        if method == "session/cancel" {
            bail!("session/cancel 必须通过 notify_validated 作为 notification 发送");
        }

        let id = self.allocate_request_id()?;
        {
            let mut requests = self
                .outbound_requests
                .lock()
                .map_err(|_| anyhow!("ACP 出站 request 账本不可用"))?;
            if requests.len() >= MAX_PENDING_REQUESTS {
                bail!("ACP 出站 request 账本达到上限 {MAX_PENDING_REQUESTS}");
            }
            requests.insert(
                id,
                SavedRequest {
                    method: method.to_string(),
                    params: params.clone(),
                },
            );
        }

        let message = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": wire_method(method),
            "params": params,
        });
        if let Err(error) = self.write_message(&message) {
            // 写入失败时不保留永远无法收到 response 的在途记录。
            self.remove_outbound_request(id);
            return Err(error);
        }

        Ok(id)
    }

    /// 校验逻辑 method 后发送一个无 id 的 Host→sidecar notification。
    pub fn notify_validated(&self, method: &str, params: Value, policy: &HostPolicy) -> Result<()> {
        validate_host_request(method, &params, policy)
            .map_err(|error| anyhow!("ACP notification {method} 未通过 Host contract: {error}"))?;
        if method != "session/cancel" {
            bail!("{method} 不是当前 Host contract 允许的 notification");
        }

        let session_id = params
            .get("sessionId")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("session/cancel 缺少有效 sessionId"))?
            .to_string();
        let message = json!({
            "jsonrpc": "2.0",
            "method": wire_method(method),
            "params": params,
        });
        self.write_message(&message)?;
        // 只有 cancel 已成功写入 sidecar 后才释放同一 session 的在途 request；
        // 其它 session 的 request 保留，避免取消关联范围过宽。
        self.remove_outbound_requests_for_session(&session_id)
    }

    /// 校验已保存的 reverse request 后回复 sidecar。
    ///
    /// `HostPolicy` 保留在统一 API 中；反向 response 不是 Host request，不能直接交给
    /// `validate_host_request`，而是按原始 reverse request 的 method/params 校验。
    pub fn reply_validated(
        &self,
        id: RequestId,
        reply: ValidatedReply,
        policy: &HostPolicy,
    ) -> Result<()> {
        // 整个 reply 过程串行化，并在写成功前保留账本，避免并发回复或重复
        // reverse request 在校验/写入窗口中覆盖原始 options。
        let _reply_guard = self
            .inbound_reply_lock
            .lock()
            .map_err(|_| anyhow!("ACP reverse reply 锁不可用"))?;
        let saved = self
            .inbound_requests
            .lock()
            .map_err(|_| anyhow!("ACP reverse request 账本不可用"))?
            .get(&id)
            .cloned()
            .ok_or_else(|| anyhow!("未找到待回复的 ACP reverse request id {id}"))?;
        validate_reverse_reply(&saved, &reply, policy)?;

        let message = match &reply {
            ValidatedReply::Result(result) => json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": result,
            }),
            ValidatedReply::Error { code, message } => json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": { "code": code, "message": message },
            }),
        };
        self.write_message(&message)?;

        // 只有完整 wire 写入成功后才消费 reverse request；失败可以安全重试。
        self.inbound_requests
            .lock()
            .map_err(|_| anyhow!("ACP reverse request 账本不可用"))?
            .remove(&id);
        Ok(())
    }

    /// 非阻塞地取得一条 stdout 入站消息；暂无消息返回 `None`，传输终止返回 `Err`。
    pub fn poll_inbound(&self) -> Result<Option<Inbound>> {
        let receiver = self
            .inbound
            .lock()
            .map_err(|_| anyhow!("ACP 入站队列不可用"))?;
        match receiver.try_recv() {
            Ok(inbound) => inbound.map(Some),
            Err(TryRecvError::Empty) => self
                .terminal_error
                .lock()
                .map_err(|_| anyhow!("ACP reader 状态不可用"))?
                .as_ref()
                .map(|error| Err(anyhow!("{error}")))
                .unwrap_or(Ok(None)),
            Err(TryRecvError::Disconnected) => self
                .terminal_error
                .lock()
                .map_err(|_| anyhow!("ACP reader 状态不可用"))?
                .as_ref()
                .map(|error| Err(anyhow!("{error}")))
                .unwrap_or(Ok(None)),
        }
    }

    /// 主动关闭 stdin、唤醒并 join stdout reader；重复调用是幂等的。
    pub fn shutdown(&self) -> Result<()> {
        self.reader_shutdown.request();
        let join_result = self.join_reader();
        self.clear_pending_requests();
        self.close_stdin();
        join_result
    }

    /// 分配一个尚未使用的正整数 request id，避免溢出后重新使用旧 id。
    fn allocate_request_id(&self) -> Result<RequestId> {
        let id = self
            .next_request_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| anyhow!("ACP request id 已耗尽"))?;
        Ok(RequestId::new(id))
    }

    /// 唯一的 stdin 写入入口；调用方必须先完成对应的 request/reply 校验。
    fn write_message(&self, message: &Value) -> Result<()> {
        let encoded = serde_json::to_vec(message).context("序列化 ACP JSON-RPC 消息失败")?;
        let mut stdin = self
            .stdin
            .lock()
            .map_err(|_| anyhow!("ACP stdin 写锁不可用"))?;
        let stdin = stdin.as_mut().ok_or_else(|| anyhow!("ACP stdin 已关闭"))?;
        stdin
            .write_all(&encoded)
            .and_then(|_| stdin.write_all(b"\n"))
            .and_then(|_| stdin.flush())
            .context("写入 ACP stdin 失败")
    }

    /// 移除写入失败的 Host→sidecar 在途 request；锁中毒时保守地保留记录。
    fn remove_outbound_request(&self, id: RequestId) {
        if let Ok(mut requests) = self.outbound_requests.lock() {
            requests.remove(&id);
        }
    }

    /// 按 sessionId 释放被取消的 Host→sidecar 在途 request，保留其它 session 的账本。
    fn remove_outbound_requests_for_session(&self, session_id: &str) -> Result<()> {
        let mut requests = self
            .outbound_requests
            .lock()
            .map_err(|_| anyhow!("ACP 出站 request 账本不可用，无法处理 cancel"))?;
        requests.retain(|_, saved| {
            saved.params.get("sessionId").and_then(Value::as_str) != Some(session_id)
        });
        Ok(())
    }

    /// join 已保存的 reader worker；worker 只会在 EOF、错误或 shutdown 后结束。
    fn join_reader(&self) -> Result<()> {
        let handle = self
            .reader_handle
            .lock()
            .map_err(|_| anyhow!("ACP reader worker 状态不可用"))?
            .take();
        if let Some(handle) = handle {
            handle
                .join()
                .map_err(|_| anyhow!("ACP reader worker 异常退出"))?;
        }
        Ok(())
    }

    /// 清理 transport 生命周期结束后所有仍在途的 request 账本。
    fn clear_pending_requests(&self) {
        clear_pending_requests(&self.inbound_requests, &self.outbound_requests);
    }

    /// 关闭唯一 stdin 写端，让 sidecar 能观察到 Host 的正常 EOF。
    fn close_stdin(&self) {
        if let Ok(mut stdin) = self.stdin.lock() {
            stdin.take();
        }
    }
}

impl Drop for AcpRuntime {
    fn drop(&mut self) {
        // Drop 不能返回错误；仍尝试完整 shutdown，并只记录不含 payload 的生命周期错误。
        if let Err(error) = self.shutdown() {
            eprintln!("ACP runtime shutdown 失败: {error}");
        }
    }
}

/// 持续读取 sidecar stdout，每行解析为一条 JSON-RPC 消息后投递给 Runtime。
#[cfg(unix)]
fn read_stdout_loop<R>(
    mut stdout: R,
    shutdown_read: UnixStream,
    shutdown_requested: Arc<AtomicBool>,
    inbound_requests: Arc<Mutex<BTreeMap<RequestId, SavedRequest>>>,
    outbound_requests: Arc<Mutex<BTreeMap<RequestId, SavedRequest>>>,
    terminal_error: Arc<Mutex<Option<String>>>,
    sender: SyncSender<Result<Inbound>>,
) where
    R: Read + Send + AsRawFd + 'static,
{
    let stdout_fd = stdout.as_raw_fd();
    let shutdown_fd = shutdown_read.as_raw_fd();
    let mut pending = Vec::new();

    loop {
        if shutdown_requested.load(Ordering::Acquire) {
            return;
        }

        match wait_for_stdout_or_shutdown(stdout_fd, shutdown_fd) {
            Ok(true) => return,
            Ok(false) => {}
            Err(error) => {
                terminate_reader(
                    &terminal_error,
                    &inbound_requests,
                    &outbound_requests,
                    error,
                );
                return;
            }
        }

        if shutdown_requested.load(Ordering::Acquire) {
            return;
        }

        let mut chunk = [0_u8; 8192];
        let read = match stdout.read(&mut chunk) {
            Ok(read) => read,
            Err(error) if error.kind() == ErrorKind::Interrupted => continue,
            Err(error) if error.kind() == ErrorKind::WouldBlock => continue,
            Err(error) => {
                terminate_reader(
                    &terminal_error,
                    &inbound_requests,
                    &outbound_requests,
                    anyhow!(error).context("读取 ACP stdout 失败"),
                );
                return;
            }
        };

        if read == 0 {
            if shutdown_requested.load(Ordering::Acquire) {
                return;
            }
            terminate_reader(
                &terminal_error,
                &inbound_requests,
                &outbound_requests,
                anyhow!("ACP stdout EOF; transport terminated"),
            );
            return;
        }
        pending.extend_from_slice(&chunk[..read]);

        while let Some(newline) = pending.iter().position(|byte| *byte == b'\n') {
            let line_bytes: Vec<u8> = pending.drain(..=newline).collect();
            let line = match String::from_utf8(line_bytes) {
                Ok(line) => line,
                Err(error) => {
                    terminate_reader(
                        &terminal_error,
                        &inbound_requests,
                        &outbound_requests,
                        anyhow!(error).context("ACP stdout 包含非 UTF-8 内容"),
                    );
                    return;
                }
            };
            let line = line.trim_end_matches(['\n', '\r']);
            if line.trim().is_empty() {
                continue;
            }

            match decode_inbound(line, &inbound_requests, &outbound_requests) {
                Ok(inbound) => {
                    if !try_enqueue(
                        &sender,
                        Ok(inbound),
                        &terminal_error,
                        &inbound_requests,
                        &outbound_requests,
                    ) {
                        return;
                    }
                }
                Err(DecodeFailure::Recoverable(error)) => {
                    // 重复 reverse id 只拒绝当前消息并保留原账本，让调用方仍可安全回复第一次 request。
                    if !try_enqueue(
                        &sender,
                        Err(error),
                        &terminal_error,
                        &inbound_requests,
                        &outbound_requests,
                    ) {
                        return;
                    }
                }
                Err(DecodeFailure::Fatal(error)) => {
                    terminate_reader(
                        &terminal_error,
                        &inbound_requests,
                        &outbound_requests,
                        error,
                    );
                    return;
                }
            }
        }
    }
}

/// 非 Unix 平台的兼容读循环；标准 `Read` 无统一可中断 fd，仍保存并 join worker。
#[cfg(not(unix))]
fn read_stdout_loop<R>(
    stdout: R,
    shutdown_receiver: Receiver<()>,
    shutdown_requested: Arc<AtomicBool>,
    inbound_requests: Arc<Mutex<BTreeMap<RequestId, SavedRequest>>>,
    outbound_requests: Arc<Mutex<BTreeMap<RequestId, SavedRequest>>>,
    terminal_error: Arc<Mutex<Option<String>>>,
    sender: SyncSender<Result<Inbound>>,
) where
    R: Read + Send + 'static,
{
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    loop {
        if shutdown_requested.load(Ordering::Acquire) || shutdown_receiver.try_recv().is_ok() {
            return;
        }
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => {
                if shutdown_requested.load(Ordering::Acquire) {
                    return;
                }
                terminate_reader(
                    &terminal_error,
                    &inbound_requests,
                    &outbound_requests,
                    anyhow!("ACP stdout EOF; transport terminated"),
                );
                return;
            }
            Ok(_) => {
                let line = line.trim_end_matches(['\n', '\r']);
                if line.trim().is_empty() {
                    continue;
                }
                match decode_inbound(line, &inbound_requests, &outbound_requests) {
                    Ok(inbound) => {
                        if !try_enqueue(
                            &sender,
                            Ok(inbound),
                            &terminal_error,
                            &inbound_requests,
                            &outbound_requests,
                        ) {
                            return;
                        }
                    }
                    Err(DecodeFailure::Recoverable(error)) => {
                        if !try_enqueue(
                            &sender,
                            Err(error),
                            &terminal_error,
                            &inbound_requests,
                            &outbound_requests,
                        ) {
                            return;
                        }
                    }
                    Err(DecodeFailure::Fatal(error)) => {
                        terminate_reader(
                            &terminal_error,
                            &inbound_requests,
                            &outbound_requests,
                            error,
                        );
                        return;
                    }
                }
            }
            Err(error) => {
                terminate_reader(
                    &terminal_error,
                    &inbound_requests,
                    &outbound_requests,
                    anyhow!(error).context("读取 ACP stdout 失败"),
                );
                return;
            }
        }
    }
}

/// 等待 stdout 或 shutdown fd；返回 `true` 表示应立即结束 reader。
#[cfg(unix)]
fn wait_for_stdout_or_shutdown(
    stdout_fd: std::os::fd::RawFd,
    shutdown_fd: std::os::fd::RawFd,
) -> Result<bool> {
    let mut descriptors = [
        libc::pollfd {
            fd: stdout_fd,
            events: libc::POLLIN,
            revents: 0,
        },
        libc::pollfd {
            fd: shutdown_fd,
            events: libc::POLLIN,
            revents: 0,
        },
    ];

    loop {
        // poll 同时监听 stdout 与内部关闭管道，避免 Drop 依赖对端自行 EOF。
        let result = unsafe { libc::poll(descriptors.as_mut_ptr(), descriptors.len() as _, -1) };
        if result >= 0 {
            let shutdown_events = libc::POLLIN | libc::POLLHUP | libc::POLLERR;
            if descriptors[1].revents & shutdown_events != 0 {
                return Ok(true);
            }
            let stdout_events = libc::POLLIN | libc::POLLHUP | libc::POLLERR | libc::POLLNVAL;
            if descriptors[0].revents & stdout_events != 0 {
                return Ok(false);
            }
            continue;
        }

        let error = std::io::Error::last_os_error();
        if error.kind() == ErrorKind::Interrupted {
            continue;
        }
        return Err(anyhow!(error).context("等待 ACP stdout 可读失败"));
    }
}

/// 将 reader 的终止原因固定下来，并清理两份在途账本。
fn terminate_reader(
    terminal_error: &Arc<Mutex<Option<String>>>,
    inbound_requests: &Arc<Mutex<BTreeMap<RequestId, SavedRequest>>>,
    outbound_requests: &Arc<Mutex<BTreeMap<RequestId, SavedRequest>>>,
    error: anyhow::Error,
) {
    if let Ok(mut terminal) = terminal_error.lock()
        && terminal.is_none()
    {
        *terminal = Some(error.to_string());
    }
    clear_pending_requests(inbound_requests, outbound_requests);
}

/// 传输生命周期结束时清空 Host 与 reverse 两侧账本，避免保留 payload。
fn clear_pending_requests(
    inbound_requests: &Arc<Mutex<BTreeMap<RequestId, SavedRequest>>>,
    outbound_requests: &Arc<Mutex<BTreeMap<RequestId, SavedRequest>>>,
) {
    if let Ok(mut requests) = inbound_requests.lock() {
        requests.clear();
    }
    if let Ok(mut requests) = outbound_requests.lock() {
        requests.clear();
    }
}

/// 非阻塞投递入站项目；队列满时终止 transport 并报告 overflow，而不是静默丢弃。
fn try_enqueue(
    sender: &SyncSender<Result<Inbound>>,
    item: Result<Inbound>,
    terminal_error: &Arc<Mutex<Option<String>>>,
    inbound_requests: &Arc<Mutex<BTreeMap<RequestId, SavedRequest>>>,
    outbound_requests: &Arc<Mutex<BTreeMap<RequestId, SavedRequest>>>,
) -> bool {
    match sender.try_send(item) {
        Ok(()) => true,
        Err(TrySendError::Full(_)) => {
            terminate_reader(
                terminal_error,
                inbound_requests,
                outbound_requests,
                anyhow!("ACP 入站队列达到上限 {MAX_INBOUND_QUEUE}; transport terminated"),
            );
            false
        }
        Err(TrySendError::Disconnected(_)) => false,
    }
}

/// 将一行 JSON-RPC wire 消息分类为 response、notification 或 reverse request。
fn decode_inbound(
    line: &str,
    inbound_requests: &Arc<Mutex<BTreeMap<RequestId, SavedRequest>>>,
    outbound_requests: &Arc<Mutex<BTreeMap<RequestId, SavedRequest>>>,
) -> std::result::Result<Inbound, DecodeFailure> {
    let value: Value = serde_json::from_str(line).context("ACP stdout 包含非 JSON 内容")?;
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("ACP stdout JSON-RPC 消息必须是对象"))?;
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Err(DecodeFailure::Fatal(anyhow!(
            "ACP stdout 消息缺少 jsonrpc=2.0"
        )));
    }

    if let Some(method_value) = object.get("method") {
        let wire_method = method_value
            .as_str()
            .ok_or_else(|| anyhow!("ACP 入站 method 必须是字符串"))?;
        let method = logical_method(wire_method);
        let params = object.get("params").cloned().unwrap_or(Value::Null);

        if let Some(raw_id) = object.get("id") {
            let id = parse_request_id(raw_id)?;
            let mut requests = inbound_requests
                .lock()
                .map_err(|_| anyhow!("ACP reverse request 账本不可用"))?;
            let pending = requests.len();
            match requests.entry(id) {
                Entry::Occupied(_) => {
                    // 重复 id 不能覆盖第一次 request 的 options；保留原账本并只报告当前消息。
                    return Err(DecodeFailure::Recoverable(anyhow!(
                        "ACP reverse request id {id} 重复，原账本保留"
                    )));
                }
                Entry::Vacant(entry) => {
                    if pending >= MAX_PENDING_REQUESTS {
                        return Err(DecodeFailure::Fatal(anyhow!(
                            "ACP reverse request 账本达到上限 {MAX_PENDING_REQUESTS}"
                        )));
                    }
                    entry.insert(SavedRequest {
                        method: method.clone(),
                        params: params.clone(),
                    });
                }
            }
            return Ok(Inbound::Request { id, method, params });
        }

        return Ok(Inbound::Notification { method, params });
    }

    let id = parse_request_id(
        object
            .get("id")
            .ok_or_else(|| anyhow!("ACP response 缺少 id"))?,
    )?;
    // response 到达即结束 Host 发起 request 的在途生命周期；仍会完整交给调用方。
    if let Ok(mut requests) = outbound_requests.lock() {
        requests.remove(&id);
    }

    match (object.get("result"), object.get("error")) {
        (Some(result), None) => Ok(Inbound::Response {
            id,
            result: Ok(result.clone()),
        }),
        (None, Some(error)) => Ok(Inbound::Response {
            id,
            result: Err(parse_rpc_error(error)?),
        }),
        _ => Err(DecodeFailure::Fatal(anyhow!(
            "ACP response 必须且只能包含 result 或 error"
        ))),
    }
}

/// 将 sidecar 侧数值 JSON-RPC id 解码成 Host 的本地类型。
fn parse_request_id(value: &Value) -> Result<RequestId> {
    value
        .as_u64()
        .map(RequestId::new)
        .ok_or_else(|| anyhow!("ACP request id 必须是无符号整数"))
}

/// 解码 JSON-RPC error object，保留可选 data 但不打印其内容。
fn parse_rpc_error(value: &Value) -> Result<RpcError> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("ACP response error 必须是对象"))?;
    let code = object
        .get("code")
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow!("ACP response error.code 必须是整数"))?;
    let message = object
        .get("message")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("ACP response error.message 必须是字符串"))?
        .to_string();
    Ok(RpcError {
        code,
        message,
        data: object.get("data").cloned(),
    })
}

/// 仅将 ACP extension 的 `_x.ai/` wire 前缀还原为 Host 内部逻辑 method 名。
fn logical_method(wire_method: &str) -> String {
    wire_method.strip_prefix("_x.ai/").map_or_else(
        || wire_method.to_string(),
        |method| format!("x.ai/{method}"),
    )
}

/// 将 contract 使用的逻辑扩展 method 映射为 ACP stdin 所需的 wire 名。
fn wire_method(method: &str) -> String {
    if method.starts_with("x.ai/") {
        format!("_{method}")
    } else {
        method.to_string()
    }
}

/// 按保存的 sidecar reverse request 对 Host reply 做 fail-closed 校验。
fn validate_reverse_reply(
    saved: &SavedRequest,
    reply: &ValidatedReply,
    _policy: &HostPolicy,
) -> Result<()> {
    if let ValidatedReply::Result(result) = reply
        && saved.method == "session/request_permission"
    {
        validate_permission_result(result, &saved.params)?;
    }
    Ok(())
}

/// `session/request_permission` 只允许取消，或选择当前 request `options[]` 中的 optionId。
fn validate_permission_result(result: &Value, request_params: &Value) -> Result<()> {
    let outcome = result
        .get("outcome")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("permission reply 缺少 outcome 对象"))?;
    let outcome_kind = outcome
        .get("outcome")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("permission reply outcome.outcome 必须是字符串"))?;

    match outcome_kind {
        "cancelled" => Ok(()),
        "selected" => {
            let option_id = outcome
                .get("optionId")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("permission selected reply 缺少 optionId"))?;
            let options = request_params
                .get("options")
                .and_then(Value::as_array)
                .ok_or_else(|| anyhow!("保存的 permission request 缺少 options 数组"))?;
            if options
                .iter()
                .any(|option| option.get("optionId").and_then(Value::as_str) == Some(option_id))
            {
                Ok(())
            } else {
                bail!("permission optionId 不属于本次 reverse request options");
            }
        }
        _ => bail!("permission reply outcome 必须是 selected 或 cancelled"),
    }
}
