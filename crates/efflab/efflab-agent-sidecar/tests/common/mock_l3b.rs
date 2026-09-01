//! Task 14 的本地 L3b HTTP mock。
//!
//! mock 只用于测试 sidecar 的请求边界与 SSE 读取顺序，不实现生产 Host 路由。

use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

/// mock 收到的一次 HTTP 请求；正文仅供测试断言，不进入生产日志。
#[derive(Debug, Clone)]
pub struct CapturedRequest {
    /// 请求方法。
    pub method: String,
    /// 请求路径。
    pub path: String,
    /// 按小写名称保存的请求头。
    pub headers: BTreeMap<String, Vec<String>>,
    /// 请求正文。
    pub body: Vec<u8>,
}

/// mock 返回的一段带延迟的响应正文。
#[derive(Debug, Clone)]
struct ResponseChunk {
    body: Vec<u8>,
    delay: Duration,
}

/// mock HTTP 响应计划。
#[derive(Debug, Clone)]
pub struct MockResponse {
    status: u16,
    reason: &'static str,
    content_type: Option<String>,
    extra_headers: Vec<(String, String)>,
    chunks: Vec<ResponseChunk>,
    advertised_length: Option<usize>,
    include_content_length: bool,
    header_gate: Option<Arc<Notify>>,
}

impl MockResponse {
    /// 构造指定 Content-Type 的完整响应。
    pub fn body(status: u16, content_type: Option<&str>, body: impl Into<Vec<u8>>) -> Self {
        Self {
            status,
            reason: reason_phrase(status),
            content_type: content_type.map(str::to_owned),
            extra_headers: Vec::new(),
            chunks: vec![ResponseChunk {
                body: body.into(),
                delay: Duration::ZERO,
            }],
            advertised_length: None,
            include_content_length: true,
            header_gate: None,
        }
    }

    /// 删除 Content-Length，模拟未知长度的响应并保留累计大小检查路径。
    pub fn without_content_length(mut self) -> Self {
        self.include_content_length = false;
        self
    }

    /// 让响应头等待显式通知，便于观察初始 HTTP 请求的取消竞态。
    pub fn with_response_header_gate(mut self, gate: Arc<Notify>) -> Self {
        self.header_gate = Some(gate);
        self
    }

    /// 构造一个不跟随的 HTTP redirect 响应。
    pub fn redirect(location: &str) -> Self {
        let mut response = Self::body(307, Some("text/plain"), Vec::<u8>::new());
        response
            .extra_headers
            .push(("Location".to_owned(), location.to_owned()));
        response
    }

    /// 构造分片发送的 SSE 响应。
    pub fn sse_chunks<I, S>(chunks: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::sse_chunks_with_delays(
            chunks
                .into_iter()
                .map(|chunk| (chunk.into(), Duration::ZERO)),
        )
    }

    /// 构造带逐片延迟的 SSE 响应，便于验证取消后不读取迟到 chunk。
    pub fn sse_chunks_with_delays<I, S>(chunks: I) -> Self
    where
        I: IntoIterator<Item = (S, Duration)>,
        S: Into<String>,
    {
        let chunks = chunks
            .into_iter()
            .map(|(chunk, delay)| {
                let chunk = chunk.into();
                let data = if chunk == "[DONE]" {
                    "data: [DONE]\n\n".to_owned()
                } else {
                    let escaped = serde_json::to_string(&chunk).expect("测试文本可序列化");
                    format!("data: {{\"choices\":[{{\"delta\":{{\"content\":{escaped}}}}}]}}\n\n")
                };
                ResponseChunk {
                    body: data.into_bytes(),
                    delay,
                }
            })
            .collect::<Vec<_>>();
        Self::sse_response(chunks)
    }

    /// 构造原始 SSE data 分片；调用方负责提供完整 data 行和空行。
    pub fn raw_sse_chunks<I, S>(chunks: I) -> Self
    where
        I: IntoIterator<Item = (S, Duration)>,
        S: Into<Vec<u8>>,
    {
        Self::sse_response(
            chunks
                .into_iter()
                .map(|(body, delay)| ResponseChunk {
                    body: body.into(),
                    delay,
                })
                .collect(),
        )
    }

    /// 宣布比实际正文更长的 Content-Length，模拟对端在 SSE 中途断流。
    pub fn with_truncated_body(mut self, extra_bytes: usize) -> Self {
        let actual = self
            .chunks
            .iter()
            .map(|chunk| chunk.body.len())
            .sum::<usize>();
        self.advertised_length = Some(actual.saturating_add(extra_bytes));
        self
    }

    fn sse_response(chunks: Vec<ResponseChunk>) -> Self {
        Self {
            status: 200,
            reason: reason_phrase(200),
            content_type: Some("text/event-stream".to_owned()),
            extra_headers: Vec::new(),
            chunks,
            advertised_length: None,
            include_content_length: true,
            header_gate: None,
        }
    }

    fn body_length(&self) -> usize {
        self.chunks
            .iter()
            .map(|chunk| chunk.body.len())
            .sum::<usize>()
    }
}

/// 一个只服务一次请求的本地 loopback mock。
pub struct MockL3b {
    url: String,
    captured: Arc<Mutex<Vec<CapturedRequest>>>,
    request_notify: Arc<Notify>,
    task: Option<JoinHandle<()>>,
}

impl MockL3b {
    /// 启动一个响应固定的 loopback HTTP mock。
    pub fn new(response: MockResponse) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("绑定测试 loopback");
        listener
            .set_nonblocking(true)
            .expect("设置测试 listener 为 nonblocking");
        let address = listener.local_addr().expect("读取测试 listener 地址");
        let listener = TcpListener::from_std(listener).expect("接管测试 listener");
        let captured = Arc::new(Mutex::new(Vec::new()));
        let captured_for_task = Arc::clone(&captured);
        let request_notify = Arc::new(Notify::new());
        let request_notify_for_task = Arc::clone(&request_notify);
        let task = tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                handle_connection(stream, response, captured_for_task, request_notify_for_task)
                    .await;
            }
        });

        Self {
            url: format!("http://{address}/v1"),
            captured,
            request_notify,
            task: Some(task),
        }
    }

    /// 使用普通文本 SSE 分片启动 mock。
    pub fn sse_chunks<I, S>(chunks: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::new(MockResponse::sse_chunks(chunks))
    }

    /// 使用带延迟的普通文本 SSE 分片启动 mock。
    pub fn sse_chunks_with_delays<I, S>(chunks: I) -> Self
    where
        I: IntoIterator<Item = (S, Duration)>,
        S: Into<String>,
    {
        Self::new(MockResponse::sse_chunks_with_delays(chunks))
    }

    /// 使用原始 SSE 分片启动 mock。
    pub fn raw_sse_chunks<I, S>(chunks: I) -> Self
    where
        I: IntoIterator<Item = (S, Duration)>,
        S: Into<Vec<u8>>,
    {
        Self::new(MockResponse::raw_sse_chunks(chunks))
    }

    /// 返回供 `HttpModelClient::for_test` 使用的基础 URL。
    pub fn loopback_url(&self) -> &str {
        &self.url
    }

    /// 返回收到的请求数。
    pub fn request_count(&self) -> usize {
        self.captured.lock().expect("读取 mock capture").len()
    }

    /// 等待 mock 完成一次请求捕获，用于避免取消测试依赖固定睡眠。
    pub async fn wait_for_request(&self) {
        loop {
            if self.request_count() > 0 {
                return;
            }
            self.request_notify.notified().await;
        }
    }

    /// 返回所有 Authorization 头值。
    pub fn authorization_values(&self) -> Vec<String> {
        self.captured
            .lock()
            .expect("读取 mock capture")
            .iter()
            .flat_map(|request| {
                request
                    .headers
                    .get("authorization")
                    .cloned()
                    .unwrap_or_default()
            })
            .collect()
    }

    /// 返回最近一次请求正文的 JSON。
    pub fn received_json(&self) -> serde_json::Value {
        let capture = self.captured.lock().expect("读取 mock capture");
        let request = capture.last().expect("mock 尚未收到请求");
        serde_json::from_slice(&request.body).expect("请求正文必须是 JSON")
    }

    /// 返回最近一次请求的完整捕获值。
    pub fn received_request(&self) -> CapturedRequest {
        self.captured
            .lock()
            .expect("读取 mock capture")
            .last()
            .cloned()
            .expect("mock 尚未收到请求")
    }
}

impl Drop for MockL3b {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// 读取请求头和固定长度正文，避免测试服务器依赖额外 HTTP crate。
async fn read_request(stream: &mut TcpStream) -> io::Result<CapturedRequest> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 4096];
    let header_end = loop {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "request EOF"));
        }
        bytes.extend_from_slice(&buffer[..read]);
        if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
        if bytes.len() > 64 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "request headers too large",
            ));
        }
    };

    let header_text = std::str::from_utf8(&bytes[..header_end])
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "request headers are not UTF-8"))?;
    let mut lines = header_text.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing request line"))?;
    let mut request_parts = request_line.split_ascii_whitespace();
    let method = request_parts
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing method"))?
        .to_owned();
    let path = request_parts
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing path"))?
        .to_owned();

    let mut headers: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for line in lines {
        if line.is_empty() {
            break;
        }
        let Some((name, value)) = line.split_once(':') else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid request header",
            ));
        };
        headers
            .entry(name.trim().to_ascii_lowercase())
            .or_default()
            .push(value.trim().to_owned());
    }

    let content_length = headers
        .get("content-length")
        .and_then(|values| values.last())
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid content length"))
        })
        .transpose()?
        .unwrap_or(0);
    while bytes.len() - header_end < content_length {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "body EOF"));
        }
        bytes.extend_from_slice(&buffer[..read]);
    }

    Ok(CapturedRequest {
        method,
        path,
        headers,
        body: bytes[header_end..header_end + content_length].to_vec(),
    })
}

/// 写响应并按计划保留 chunk 之间的时序。
async fn handle_connection(
    mut stream: TcpStream,
    response: MockResponse,
    captured: Arc<Mutex<Vec<CapturedRequest>>>,
    request_notify: Arc<Notify>,
) {
    let request = match read_request(&mut stream).await {
        Ok(request) => request,
        Err(_) => return,
    };
    captured.lock().expect("写入 mock capture").push(request);
    request_notify.notify_one();

    // 先捕获请求，再由测试决定何时放行响应头，以覆盖 send 与取消的竞态。
    if let Some(gate) = response.header_gate.as_ref() {
        gate.notified().await;
    }

    let mut headers = format!(
        "HTTP/1.1 {} {}\r\nConnection: close\r\n",
        response.status, response.reason
    );
    if let Some(content_type) = &response.content_type {
        headers.push_str("Content-Type: ");
        headers.push_str(content_type);
        headers.push_str("\r\n");
    }
    if response.include_content_length {
        let content_length = response
            .advertised_length
            .unwrap_or_else(|| response.body_length());
        headers.push_str(&format!("Content-Length: {content_length}\r\n"));
    }
    for (name, value) in &response.extra_headers {
        headers.push_str(name);
        headers.push_str(": ");
        headers.push_str(value);
        headers.push_str("\r\n");
    }
    headers.push_str("\r\n");
    if stream.write_all(headers.as_bytes()).await.is_err() {
        return;
    }
    for chunk in response.chunks {
        if !chunk.delay.is_zero() {
            tokio::time::sleep(chunk.delay).await;
        }
        if stream.write_all(&chunk.body).await.is_err() {
            return;
        }
        if stream.flush().await.is_err() {
            return;
        }
    }
}

/// 为测试状态码提供稳定的 reason phrase。
const fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        307 => "Temporary Redirect",
        401 => "Unauthorized",
        403 => "Forbidden",
        413 => "Payload Too Large",
        500 => "Internal Server Error",
        _ => "Test Response",
    }
}
