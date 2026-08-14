//! 可信 Host 的 ACP stdio JSON-RPC 运行时。
//!
//! 此模块只负责拆分后的 stdin/stdout 传输、消息复用与反向 RPC 回复校验；
//! 不启动 sidecar，也不把 ACP 类型泄漏到产品层。

use std::collections::BTreeMap;
use std::fmt;
use std::io::{BufRead, BufReader, Read, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};

use anyhow::{Context, Result, anyhow, bail};
use efflab_agent_contract::{HostPolicy, validate_host_request};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// JSON-RPC 2.0 的标准 `method_not_found` 错误码。
pub const METHOD_NOT_FOUND: i64 = -32601;

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

/// 拆分 stdin 写端与 stdout 读循环的 ACP Runtime。
pub struct AcpRuntime {
    /// Host 是 sidecar stdin 的唯一写入者；锁保证多个 `&self` 调用不会交错字节。
    stdin: Mutex<Box<dyn Write + Send>>,
    /// stdout 读线程不断推送消息，调用方以非阻塞方式轮询。
    inbound: Mutex<mpsc::Receiver<Result<Inbound>>>,
    /// Host 自己发出的数值 request id 分配器。
    next_request_id: AtomicU64,
    /// Host→sidecar request 的在途上下文，直到对应 response 抵达。
    outbound_requests: Arc<Mutex<BTreeMap<RequestId, SavedRequest>>>,
    /// sidecar→Host reverse request 的上下文，直到 `reply_validated` 成功写出。
    inbound_requests: Arc<Mutex<BTreeMap<RequestId, SavedRequest>>>,
}

impl AcpRuntime {
    /// 使用调用方提供的已拆分 sidecar stdin/stdout 构造 Runtime。
    ///
    /// stdout 立即交给独立线程读取，因此即使长 prompt 尚未收到 result，调用方仍可
    /// 经 [`Self::poll_inbound`] 接收 notification 与 reverse request。
    pub fn new(stdin: impl Write + Send + 'static, stdout: impl Read + Send + 'static) -> Self {
        let (sender, receiver) = mpsc::channel();
        let inbound_requests = Arc::new(Mutex::new(BTreeMap::new()));
        let outbound_requests = Arc::new(Mutex::new(BTreeMap::new()));

        // stdout 由独立线程独占，避免任何 request 等待路径吞掉中途 notification。
        let reader_inbound_requests = Arc::clone(&inbound_requests);
        let reader_outbound_requests = Arc::clone(&outbound_requests);
        std::thread::spawn(move || {
            read_stdout_loop(
                stdout,
                reader_inbound_requests,
                reader_outbound_requests,
                sender,
            );
        });

        Self {
            stdin: Mutex::new(Box::new(stdin)),
            inbound: Mutex::new(receiver),
            // 侧车现有 stdio 测试的 Host request id 从 1 开始。
            next_request_id: AtomicU64::new(1),
            outbound_requests,
            inbound_requests,
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
        self.outbound_requests
            .lock()
            .map_err(|_| anyhow!("ACP 出站 request 账本不可用"))?
            .insert(
                id,
                SavedRequest {
                    method: method.to_string(),
                    params: params.clone(),
                },
            );

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

        let message = json!({
            "jsonrpc": "2.0",
            "method": wire_method(method),
            "params": params,
        });
        self.write_message(&message)
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
        let saved = self.take_inbound_request(id)?;
        if let Err(error) = validate_reverse_reply(&saved, &reply, policy) {
            self.restore_inbound_request(id, saved)?;
            return Err(error);
        }

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
        if let Err(error) = self.write_message(&message) {
            // 仅在成功写入后消费 reverse request，失败时保留给调用方重试。
            self.restore_inbound_request(id, saved)?;
            return Err(error);
        }

        Ok(())
    }

    /// 非阻塞地取得一条 stdout 入站消息；暂无消息或 stdout EOF 时返回 `None`。
    pub fn poll_inbound(&self) -> Result<Option<Inbound>> {
        let receiver = self
            .inbound
            .lock()
            .map_err(|_| anyhow!("ACP 入站队列不可用"))?;
        match receiver.try_recv() {
            Ok(inbound) => inbound.map(Some),
            Err(mpsc::TryRecvError::Empty | mpsc::TryRecvError::Disconnected) => Ok(None),
        }
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

    /// 取出一个尚未回复的 reverse request，阻止并发调用重复响应同一个 id。
    fn take_inbound_request(&self, id: RequestId) -> Result<SavedRequest> {
        self.inbound_requests
            .lock()
            .map_err(|_| anyhow!("ACP reverse request 账本不可用"))?
            .remove(&id)
            .ok_or_else(|| anyhow!("未找到待回复的 ACP reverse request id {id}"))
    }

    /// 校验或写入失败后恢复 reverse request，让调用方可以修正后重试。
    fn restore_inbound_request(&self, id: RequestId, saved: SavedRequest) -> Result<()> {
        self.inbound_requests
            .lock()
            .map_err(|_| anyhow!("ACP reverse request 账本不可用，无法恢复 id {id}"))?
            .insert(id, saved);
        Ok(())
    }
}

/// 持续读取 sidecar stdout，每行解析为一条 JSON-RPC 消息后投递给 Runtime。
fn read_stdout_loop(
    stdout: impl Read,
    inbound_requests: Arc<Mutex<BTreeMap<RequestId, SavedRequest>>>,
    outbound_requests: Arc<Mutex<BTreeMap<RequestId, SavedRequest>>>,
    sender: mpsc::Sender<Result<Inbound>>,
) {
    let reader = BufReader::new(stdout);
    for line in reader.lines() {
        let line = match line {
            Ok(line) => line,
            Err(error) => {
                let _ = sender.send(Err(error).context("读取 ACP stdout 失败"));
                return;
            }
        };
        if line.trim().is_empty() {
            continue;
        }

        if sender
            .send(decode_inbound(&line, &inbound_requests, &outbound_requests))
            .is_err()
        {
            return;
        }
    }
}

/// 将一行 JSON-RPC wire 消息分类为 response、notification 或 reverse request。
fn decode_inbound(
    line: &str,
    inbound_requests: &Arc<Mutex<BTreeMap<RequestId, SavedRequest>>>,
    outbound_requests: &Arc<Mutex<BTreeMap<RequestId, SavedRequest>>>,
) -> Result<Inbound> {
    let value: Value = serde_json::from_str(line).context("ACP stdout 包含非 JSON 内容")?;
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("ACP stdout JSON-RPC 消息必须是对象"))?;
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        bail!("ACP stdout 消息缺少 jsonrpc=2.0");
    }

    if let Some(method_value) = object.get("method") {
        let wire_method = method_value
            .as_str()
            .ok_or_else(|| anyhow!("ACP 入站 method 必须是字符串"))?;
        let method = logical_method(wire_method);
        let params = object.get("params").cloned().unwrap_or(Value::Null);

        if let Some(raw_id) = object.get("id") {
            let id = parse_request_id(raw_id)?;
            inbound_requests
                .lock()
                .map_err(|_| anyhow!("ACP reverse request 账本不可用"))?
                .insert(
                    id,
                    SavedRequest {
                        method: method.clone(),
                        params: params.clone(),
                    },
                );
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
        _ => bail!("ACP response 必须且只能包含 result 或 error"),
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

/// 将 ACP extension 的单个 `_` wire 前缀还原为 Host 内部逻辑 method 名。
fn logical_method(wire_method: &str) -> String {
    wire_method
        .strip_prefix('_')
        .unwrap_or(wire_method)
        .to_string()
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
