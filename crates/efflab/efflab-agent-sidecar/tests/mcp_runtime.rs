//! 最小 HTTP MCP runtime 的行为测试。
//!
//! 测试 server 只实现 rmcp streamable HTTP client 所需的 JSON-RPC 子集，不启用 rmcp
//! server feature；测试本身因此也能检查 sidecar 的进程与依赖边界。

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use efflab_agent_contract::{ApprovedMcpConfig, McpServerSpec, load_runtime_config_v1};
use efflab_agent_sidecar::mcp_client::{
    MAX_MCP_OUTPUT_BYTES, MCP_CALL_TIMEOUT, MCP_INITIALIZE_TIMEOUT, McpCancellationToken,
    McpRuntime, is_literal_loopback_http_url,
};
use efflab_agent_sidecar::session_store::MAX_RECORD_ID_BYTES;
use serde_json::{Value, json};
use tempfile::{NamedTempFile, tempdir};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);
#[derive(Clone)]
struct ToolsPage {
    request_cursor: Option<String>,
    tools: Vec<String>,
    next_cursor: Option<String>,
}

#[derive(Clone, Copy)]
enum ReinitializeFailure {
    WrongId,
    MissingResult,
    MissingProtocol,
    WrongProtocolVersion,
    MissingSession,
    InitializedFailure,
    InitializedMalformed,
    InitializedCancellation,
    CleanupFailure,
}

#[derive(Clone, Copy, Debug)]
enum InitialHandshakeFailure {
    InitializeMalformed,
    InitializeMissingResult,
    InitializeMissingProtocol,
    InitializeMissingSession,
    InitializedFailure,
    InitializedMalformed,
    InitializedTimeout,
    InitializedCancellation,
    ToolsMalformed,
    ToolsOversized,
    ToolsTimeout,
}

#[derive(Clone)]
enum ServerPlan {
    Tools(Vec<String>),
    Paginated(Vec<ToolsPage>),
    LargeMetadata {
        pages: usize,
        description_bytes: usize,
    },
    RepeatedCursor,
    EmptyCursor,
    SessionExpires {
        repeat_call_404: bool,
    },
    ReinitializeFailure(ReinitializeFailure),
    InitialHandshakeFailure(InitialHandshakeFailure),
    MalformedInitialize,
    Redirect(String),
    DelayInitialize,
    DelayCall,
    DelayShutdown,
    LateCandidateCleanupFailure,
    CallError,
    LargeResult(usize),
    LargeContentLength(usize),
    LargeChunked(usize),
    SseNotificationThenResponse,
    SseServerRequestThenResponse,
    SseResponseThenServerRequest,
    SseTruncatedResponse,
    SseWrongResponseId,
    SseOversizedChunkedBody,
    JsonNotificationCall,
    EmptyInitializedNoContentLength,
    NonZeroNoContentInitialized,
    NonZeroNoContentDelete,
    ExactResponseBody,
    LargeAcceptedCallBody,
    LargeAcceptedChunkedCallBody,
    LargeNoContentCallBody,
    LargeNoContentChunkedCallBody,
    LargeErrorCallBody,
    LargeErrorChunkedCallBody,
    LargeDeleteSuccessBody,
    LargeDeleteMethodNotAllowedBody,
    LargeDeleteNoContentChunkedBody,
}

#[derive(Clone, Debug)]
struct RequestHeaders {
    method: String,
    session_id: Option<String>,
    protocol_version: Option<String>,
    request_id: Option<Value>,
}

struct MockMcpServer {
    address: SocketAddr,
    plan: ServerPlan,
    requests: Arc<Mutex<Vec<String>>>,
    request_headers: Arc<Mutex<Vec<RequestHeaders>>>,
    tool_list_cursors: Arc<Mutex<Vec<Option<String>>>>,
    response_bytes_sent: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl MockMcpServer {
    /// 启动只绑定 loopback 的最小 streamable HTTP MCP 测试端点。
    fn start(plan: ServerPlan) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("测试 MCP 必须绑定 loopback");
        listener
            .set_nonblocking(true)
            .expect("测试 MCP listener 必须支持停止唤醒");
        let address = listener.local_addr().expect("必须能读取测试 MCP 地址");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let request_headers = Arc::new(Mutex::new(Vec::new()));
        let tool_list_cursors = Arc::new(Mutex::new(Vec::new()));
        let response_bytes_sent = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let requests_for_thread = Arc::clone(&requests);
        let request_headers_for_thread = Arc::clone(&request_headers);
        let tool_list_cursors_for_thread = Arc::clone(&tool_list_cursors);
        let response_bytes_sent_for_thread = Arc::clone(&response_bytes_sent);
        let stop_for_thread = Arc::clone(&stop);
        let plan_for_thread = plan.clone();
        let thread = thread::spawn(move || {
            let mut request_number = 0_usize;
            while !stop_for_thread.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        stream
                            .set_nonblocking(false)
                            .expect("测试 MCP accepted stream 必须是阻塞模式");
                        stream
                            .set_read_timeout(Some(Duration::from_secs(2)))
                            .expect("测试 MCP accepted stream 必须有读取上限");
                        stream
                            .set_write_timeout(Some(Duration::from_millis(20)))
                            .expect("测试 MCP accepted stream 必须有写入上限");
                        if let Some(request) = read_http_request(&mut stream) {
                            request_number = request_number.saturating_add(1);
                            let method = request.method.clone();
                            if let Ok(mut seen) = requests_for_thread.lock() {
                                seen.push(method.clone());
                            }
                            if let Ok(mut seen) = request_headers_for_thread.lock() {
                                seen.push(RequestHeaders {
                                    method: method.clone(),
                                    session_id: request.session_id.clone(),
                                    protocol_version: request.protocol_version.clone(),
                                    request_id: request.body.get("id").cloned(),
                                });
                            }
                            if method == "tools/list" {
                                let cursor = request
                                    .body
                                    .get("params")
                                    .and_then(|params| params.get("cursor"))
                                    .and_then(Value::as_str)
                                    .map(str::to_owned);
                                if let Ok(mut cursors) = tool_list_cursors_for_thread.lock() {
                                    cursors.push(cursor);
                                }
                            }
                            serve_http_request(
                                &mut stream,
                                &method,
                                &request.body,
                                request_number,
                                &plan_for_thread,
                                &response_bytes_sent_for_thread,
                                &stop_for_thread,
                            );
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            address,
            plan,
            requests,
            request_headers,
            tool_list_cursors,
            response_bytes_sent,
            stop,
            thread: Some(thread),
        }
    }

    /// 返回配置给 sidecar 的 HTTP MCP URL。
    fn url(&self) -> String {
        format!("http://{}/mcp", self.address)
    }

    /// 返回 mock server 已成功写入 socket 的响应字节数。
    fn response_bytes_sent(&self) -> usize {
        self.response_bytes_sent.load(Ordering::Acquire)
    }

    /// 等待测试 server 收到指定的 JSON-RPC 方法，避免依赖固定 sleep。
    async fn wait_for_method(&self, expected: &str) {
        let deadline = Instant::now() + TEST_TIMEOUT;
        loop {
            let found = self
                .requests
                .lock()
                .map(|requests| requests.iter().any(|method| method == expected))
                .unwrap_or(false);
            if found {
                return;
            }
            assert!(Instant::now() < deadline, "测试 MCP 未收到方法 {expected}");
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    /// 等待测试 server 收到某个方法的指定次数，区分首次 handshake 与 reinitialize。
    async fn wait_for_method_count(&self, expected: &str, count: usize) {
        let deadline = Instant::now() + TEST_TIMEOUT;
        loop {
            let actual = self
                .requests
                .lock()
                .map(|requests| requests.iter().filter(|method| method == &expected).count())
                .unwrap_or(0);
            if actual >= count {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "测试 MCP 未收到预期方法次数: method={expected}, expected={count}, actual={actual}"
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    /// 等待测试 server 收到指定数量的 tools/call，区分先前已取消的 call。
    async fn wait_for_call_count(&self, expected: usize) {
        let deadline = Instant::now() + TEST_TIMEOUT;
        loop {
            let actual = self
                .requests
                .lock()
                .map(|requests| {
                    requests
                        .iter()
                        .filter(|method| *method == "tools/call")
                        .count()
                })
                .unwrap_or(0);
            if actual >= expected {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "测试 MCP 未收到预期 tools/call 数量: expected={expected}, actual={actual}"
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }
}

impl Drop for MockMcpServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let _ = TcpStream::connect(self.address);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        let _ = &self.plan;
    }
}

struct HttpRequest {
    method: String,
    body: Value,
    session_id: Option<String>,
    protocol_version: Option<String>,
}

/// 读取测试请求的 method、headers 和固定长度 JSON body，不保存原始参数或秘密。
fn read_http_request(stream: &mut TcpStream) -> Option<HttpRequest> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 4096];
    let header_end = loop {
        let read = stream.read(&mut buffer).ok()?;
        if read == 0 {
            return None;
        }
        bytes.extend_from_slice(&buffer[..read]);
        if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
        if bytes.len() > 64 * 1024 {
            return None;
        }
    };
    let headers = String::from_utf8(bytes[..header_end].to_vec()).ok()?;
    let method = headers
        .lines()
        .next()?
        .split_whitespace()
        .next()?
        .to_owned();
    let session_id = header_value(&headers, "Mcp-Session-Id");
    let protocol_version = header_value(&headers, "MCP-Protocol-Version");
    if method == "DELETE" {
        return Some(HttpRequest {
            method,
            body: Value::Null,
            session_id,
            protocol_version,
        });
    }
    let content_length = headers.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        (name.eq_ignore_ascii_case("content-length"))
            .then(|| value.trim().parse::<usize>().ok())
            .flatten()
    })?;
    while bytes.len() < header_end + content_length {
        let read = stream.read(&mut buffer).ok()?;
        if read == 0 {
            return None;
        }
        bytes.extend_from_slice(&buffer[..read]);
        if bytes.len() > header_end + content_length + 64 * 1024 {
            return None;
        }
    }
    let body: Value =
        serde_json::from_slice(&bytes[header_end..header_end + content_length]).ok()?;
    let method = body
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or(method.as_str())
        .to_owned();
    Some(HttpRequest {
        method,
        body,
        session_id,
        protocol_version,
    })
}

/// 从测试请求 headers 中读取一个大小受控的 ASCII header 值。
fn header_value(headers: &str, expected_name: &str) -> Option<String> {
    headers.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case(expected_name)
            .then(|| value.trim().to_owned())
            .filter(|value| !value.is_empty())
    })
}

/// 按 MCP streamable HTTP 的 JSON 响应合同返回测试结果。
fn serve_http_request(
    stream: &mut TcpStream,
    method: &str,
    body: &Value,
    request_number: usize,
    plan: &ServerPlan,
    response_bytes_sent: &AtomicUsize,
    stop: &AtomicBool,
) {
    match method {
        "DELETE" => match plan {
            ServerPlan::LargeDeleteSuccessBody => {
                write_declared_response(
                    stream,
                    "200 OK",
                    "application/json",
                    MAX_MCP_OUTPUT_BYTES + 1,
                    "",
                    std::iter::empty::<(&str, &str)>(),
                );
            }
            ServerPlan::LargeDeleteMethodNotAllowedBody => {
                write_declared_response(
                    stream,
                    "405 Method Not Allowed",
                    "application/json",
                    MAX_MCP_OUTPUT_BYTES + 1,
                    "",
                    std::iter::empty::<(&str, &str)>(),
                );
            }
            ServerPlan::LargeDeleteNoContentChunkedBody => {
                write_chunked_response(
                    stream,
                    "204 No Content",
                    "application/json",
                    &oversized_response_body(),
                    response_bytes_sent,
                );
            }
            ServerPlan::NonZeroNoContentDelete => {
                write_declared_response(
                    stream,
                    "204 No Content",
                    "application/json",
                    1,
                    "x",
                    std::iter::empty::<(&str, &str)>(),
                );
            }
            ServerPlan::DelayShutdown => {
                thread::sleep(Duration::from_millis(150));
                write_response(
                    stream,
                    "204 No Content",
                    "",
                    std::iter::empty::<(&str, &str)>(),
                );
            }
            ServerPlan::LateCandidateCleanupFailure => {
                // 初始 shutdown 成功，Sealed 后到达的 candidate DELETE 固定失败。
                let status = if request_number >= 5 {
                    "500 Internal Server Error"
                } else {
                    "204 No Content"
                };
                write_response(stream, status, "", std::iter::empty::<(&str, &str)>());
            }
            ServerPlan::ReinitializeFailure(ReinitializeFailure::CleanupFailure)
            | ServerPlan::InitialHandshakeFailure(_) => {
                write_response(
                    stream,
                    "500 Internal Server Error",
                    "",
                    std::iter::empty::<(&str, &str)>(),
                );
            }
            _ => write_response(
                stream,
                "204 No Content",
                "",
                std::iter::empty::<(&str, &str)>(),
            ),
        },
        "initialize" => match plan {
            ServerPlan::Redirect(location) => {
                write_response(stream, "302 Found", "", [("Location", location.as_str())]);
            }
            ServerPlan::DelayInitialize => wait_until_stopped(Duration::from_secs(30), stop),
            ServerPlan::MalformedInitialize => write_json_response(
                stream,
                200,
                json_rpc_result(
                    request_id(body),
                    json!({
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": "test-mcp", "version": "1"}
                    }),
                ),
                true,
            ),
            ServerPlan::InitialHandshakeFailure(failure) => {
                let response = match failure {
                    InitialHandshakeFailure::InitializeMalformed => {
                        let body = format!(r#"{{"jsonrpc":"2.0","id":{},"#, request_id(body));
                        write_response(
                            stream,
                            "200 OK",
                            &body,
                            [("Mcp-Session-Id", "initial-session")],
                        );
                        return;
                    }
                    InitialHandshakeFailure::InitializeMissingResult => {
                        json!({"jsonrpc": "2.0", "id": request_id(body)})
                    }
                    InitialHandshakeFailure::InitializeMissingProtocol => json_rpc_result(
                        request_id(body),
                        json!({
                            "capabilities": {"tools": {}},
                            "serverInfo": {"name": "test-mcp", "version": "1"}
                        }),
                    ),
                    InitialHandshakeFailure::InitializeMissingSession
                    | InitialHandshakeFailure::InitializedFailure
                    | InitialHandshakeFailure::InitializedMalformed
                    | InitialHandshakeFailure::InitializedTimeout
                    | InitialHandshakeFailure::InitializedCancellation
                    | InitialHandshakeFailure::ToolsMalformed
                    | InitialHandshakeFailure::ToolsOversized
                    | InitialHandshakeFailure::ToolsTimeout => {
                        json_rpc_result(request_id(body), valid_initialize_result())
                    }
                };
                let session_id =
                    (!matches!(failure, InitialHandshakeFailure::InitializeMissingSession))
                        .then_some("initial-session");
                write_json_response_with_session(stream, 200, response, session_id);
            }
            ServerPlan::SessionExpires { .. } => {
                let session_id = if request_number == 1 {
                    "expired-session"
                } else {
                    "renewed-session"
                };
                write_json_response_with_session(
                    stream,
                    200,
                    json_rpc_result(request_id(body), valid_initialize_result()),
                    Some(session_id),
                );
            }
            ServerPlan::ReinitializeFailure(kind) => {
                let initial = request_number == 1;
                let session_id = if initial {
                    "expired-session"
                } else {
                    "renewed-session"
                };
                let response = if initial {
                    json_rpc_result(request_id(body), valid_initialize_result())
                } else {
                    match kind {
                        ReinitializeFailure::WrongId => {
                            json_rpc_result(Value::from(999_u64), valid_initialize_result())
                        }
                        ReinitializeFailure::MissingResult => {
                            json!({"jsonrpc": "2.0", "id": request_id(body)})
                        }
                        ReinitializeFailure::MissingProtocol => json_rpc_result(
                            request_id(body),
                            json!({
                                "capabilities": {"tools": {}},
                                "serverInfo": {"name": "test-mcp", "version": "1"}
                            }),
                        ),
                        ReinitializeFailure::WrongProtocolVersion => json_rpc_result(
                            request_id(body),
                            json!({
                                "protocolVersion": "2024-11-05",
                                "capabilities": {"tools": {}},
                                "serverInfo": {"name": "test-mcp", "version": "1"}
                            }),
                        ),
                        ReinitializeFailure::MissingSession
                        | ReinitializeFailure::InitializedFailure
                        | ReinitializeFailure::InitializedMalformed
                        | ReinitializeFailure::InitializedCancellation
                        | ReinitializeFailure::CleanupFailure => {
                            json_rpc_result(request_id(body), valid_initialize_result())
                        }
                    }
                };
                let response_session = (!matches!(kind, ReinitializeFailure::MissingSession)
                    || initial)
                    .then_some(session_id);
                write_json_response_with_session(stream, 200, response, response_session);
            }
            _ => write_json_response(
                stream,
                200,
                json_rpc_result(request_id(body), valid_initialize_result()),
                true,
            ),
        },
        "notifications/initialized" => match plan {
            ServerPlan::InitialHandshakeFailure(InitialHandshakeFailure::InitializedFailure) => {
                write_response(stream, "500 Internal Server Error", "", []);
            }
            ServerPlan::InitialHandshakeFailure(InitialHandshakeFailure::InitializedMalformed) => {
                write_json_response(
                    stream,
                    200,
                    json_rpc_result(Value::from(999_u64), json!({})),
                    true,
                );
            }
            ServerPlan::InitialHandshakeFailure(InitialHandshakeFailure::InitializedTimeout) => {
                wait_until_stopped(Duration::from_secs(30), stop);
            }
            ServerPlan::InitialHandshakeFailure(
                InitialHandshakeFailure::InitializedCancellation,
            ) => {
                wait_for_client_disconnect_or_timeout(stream, stop, Duration::from_secs(30));
            }
            ServerPlan::ReinitializeFailure(ReinitializeFailure::InitializedFailure)
                if request_number > 2 =>
            {
                write_response(stream, "500 Internal Server Error", "", [])
            }
            ServerPlan::ReinitializeFailure(ReinitializeFailure::InitializedMalformed)
                if request_number > 2 =>
            {
                write_json_response(
                    stream,
                    200,
                    json_rpc_result(Value::from(999_u64), json!({})),
                    true,
                )
            }
            ServerPlan::ReinitializeFailure(ReinitializeFailure::InitializedCancellation)
                if request_number > 2 =>
            {
                // 夹具必须释放当前连接，才能让单线程 listener 接收后续 cleanup DELETE。
                wait_for_client_disconnect_or_timeout(stream, stop, Duration::from_millis(250))
            }
            ServerPlan::EmptyInitializedNoContentLength => {
                // 用合法的 Content-Length: 0 形式验证可观察的空 no-content 响应。
                write_response(stream, "204 No Content", "", []);
            }
            ServerPlan::NonZeroNoContentInitialized => {
                write_declared_response(
                    stream,
                    "204 No Content",
                    "application/json",
                    1,
                    "x",
                    std::iter::empty::<(&str, &str)>(),
                );
            }
            _ => write_response(stream, "202 Accepted", "", []),
        },
        "tools/list" => {
            if let ServerPlan::InitialHandshakeFailure(InitialHandshakeFailure::ToolsTimeout) = plan
            {
                wait_until_stopped(Duration::from_secs(30), stop);
                return;
            }
            if let ServerPlan::InitialHandshakeFailure(InitialHandshakeFailure::ToolsMalformed) =
                plan
            {
                write_json_response(
                    stream,
                    200,
                    json_rpc_result(request_id(body), json!({"tools": "invalid"})),
                    true,
                );
                return;
            }
            if let ServerPlan::InitialHandshakeFailure(InitialHandshakeFailure::ToolsOversized) =
                plan
            {
                write_large_json_response(
                    stream,
                    request_id(body),
                    2 * 1024 * 1024,
                    false,
                    response_bytes_sent,
                );
                return;
            }
            if let ServerPlan::LargeMetadata {
                pages,
                description_bytes,
            } = plan
            {
                let requested_cursor = body
                    .get("params")
                    .and_then(|params| params.get("cursor"))
                    .and_then(Value::as_str);
                let page_index = requested_cursor
                    .and_then(|cursor| cursor.strip_prefix("page-"))
                    .and_then(|value| value.parse::<usize>().ok())
                    .unwrap_or(0);
                let mut result = json!({
                    "tools": [{
                        "name": format!("tool-{page_index}"),
                        "description": "x".repeat(*description_bytes),
                        "inputSchema": {"type": "object", "properties": {}}
                    }]
                });
                if page_index.saturating_add(1) < *pages {
                    result["nextCursor"] = Value::String(format!("page-{}", page_index + 1));
                }
                write_json_response(stream, 200, json_rpc_result(request_id(body), result), true);
                return;
            }
            let (tools, next_cursor) = match plan {
                ServerPlan::Tools(names) => (names.clone(), None),
                ServerPlan::Paginated(pages) => {
                    let requested_cursor = body
                        .get("params")
                        .and_then(|params| params.get("cursor"))
                        .and_then(Value::as_str);
                    let page = pages
                        .iter()
                        .find(|page| page.request_cursor.as_deref() == requested_cursor)
                        .cloned()
                        .unwrap_or_else(|| ToolsPage {
                            request_cursor: None,
                            tools: Vec::new(),
                            next_cursor: None,
                        });
                    (page.tools, page.next_cursor)
                }
                ServerPlan::RepeatedCursor => (vec!["first".to_owned()], Some("same".to_owned())),
                ServerPlan::EmptyCursor => (vec!["first".to_owned()], Some(String::new())),
                ServerPlan::SessionExpires { .. } => (vec!["ok".to_owned()], None),
                ServerPlan::ReinitializeFailure(_) => (vec!["ok".to_owned()], None),
                ServerPlan::InitialHandshakeFailure(_) => (vec!["ok".to_owned()], None),
                ServerPlan::LargeMetadata { .. } => (Vec::new(), None),
                ServerPlan::CallError
                | ServerPlan::DelayCall
                | ServerPlan::LargeResult(_)
                | ServerPlan::LargeContentLength(_)
                | ServerPlan::LargeChunked(_)
                | ServerPlan::SseNotificationThenResponse
                | ServerPlan::SseServerRequestThenResponse
                | ServerPlan::SseResponseThenServerRequest
                | ServerPlan::SseTruncatedResponse
                | ServerPlan::SseWrongResponseId
                | ServerPlan::SseOversizedChunkedBody
                | ServerPlan::JsonNotificationCall
                | ServerPlan::EmptyInitializedNoContentLength
                | ServerPlan::NonZeroNoContentInitialized
                | ServerPlan::NonZeroNoContentDelete
                | ServerPlan::ExactResponseBody
                | ServerPlan::LargeAcceptedCallBody
                | ServerPlan::LargeAcceptedChunkedCallBody
                | ServerPlan::LargeNoContentCallBody
                | ServerPlan::LargeNoContentChunkedCallBody
                | ServerPlan::LargeErrorCallBody
                | ServerPlan::LargeErrorChunkedCallBody
                | ServerPlan::LargeDeleteSuccessBody
                | ServerPlan::LargeDeleteMethodNotAllowedBody
                | ServerPlan::LargeDeleteNoContentChunkedBody
                | ServerPlan::DelayShutdown
                | ServerPlan::LateCandidateCleanupFailure => {
                    (vec!["ok".to_owned(), "bad".to_owned()], None)
                }
                ServerPlan::Redirect(_)
                | ServerPlan::DelayInitialize
                | ServerPlan::MalformedInitialize => (Vec::new(), None),
            };
            let tools = tools
                .into_iter()
                .map(|name| {
                    json!({
                        "name": name,
                        "description": "test tool",
                        "inputSchema": {"type": "object", "properties": {}}
                    })
                })
                .collect::<Vec<_>>();
            let mut result = json!({"tools": tools});
            if let Some(next_cursor) = next_cursor {
                result["nextCursor"] = Value::String(next_cursor);
            }
            write_json_response(stream, 200, json_rpc_result(request_id(body), result), true);
        }
        "tools/call" => match plan {
            ServerPlan::SessionExpires { repeat_call_404 }
                if request_number == 4 || (*repeat_call_404 && request_number == 7) =>
            {
                write_response(
                    stream,
                    "404 Not Found",
                    "",
                    std::iter::empty::<(&str, &str)>(),
                );
            }
            ServerPlan::ReinitializeFailure(kind)
                if request_number == 4
                    || (request_number == 7
                        && !matches!(kind, ReinitializeFailure::InitializedMalformed)) =>
            {
                write_response(
                    stream,
                    "404 Not Found",
                    "",
                    std::iter::empty::<(&str, &str)>(),
                );
            }
            ServerPlan::DelayCall => wait_for_client_disconnect(stream, stop),
            ServerPlan::CallError => {
                let tool_name = body
                    .get("params")
                    .and_then(|params| params.get("name"))
                    .and_then(Value::as_str);
                let (text, is_error) = if tool_name == Some("bad") {
                    ("tool failed", true)
                } else {
                    ("ok", false)
                };
                write_json_response(
                    stream,
                    200,
                    json_rpc_result(
                        request_id(body),
                        json!({"content": [{"type": "text", "text": text}], "isError": is_error}),
                    ),
                    true,
                );
            }
            ServerPlan::LargeResult(size) => {
                let text = "x".repeat(*size);
                write_json_response(
                    stream,
                    200,
                    json_rpc_result(
                        request_id(body),
                        json!({"content": [{"type": "text", "text": text}], "isError": false}),
                    ),
                    true,
                );
            }
            ServerPlan::LargeContentLength(size) => {
                write_large_json_response(
                    stream,
                    request_id(body),
                    *size,
                    false,
                    response_bytes_sent,
                );
            }
            ServerPlan::LargeChunked(size) => {
                write_large_json_response(
                    stream,
                    request_id(body),
                    *size,
                    true,
                    response_bytes_sent,
                );
            }
            ServerPlan::SseNotificationThenResponse => {
                write_sse_response(stream, request_id(body), false);
            }
            ServerPlan::SseServerRequestThenResponse => {
                write_sse_response(stream, request_id(body), true);
            }
            ServerPlan::SseResponseThenServerRequest => {
                write_sse_response_then_server_request(stream, request_id(body));
            }
            ServerPlan::SseTruncatedResponse => {
                write_sse_truncated_response(stream, request_id(body));
            }
            ServerPlan::SseWrongResponseId => {
                write_sse_response(stream, Value::from(999_u64), false);
            }
            ServerPlan::SseOversizedChunkedBody => {
                write_oversized_sse_chunked_response(stream, request_id(body), response_bytes_sent);
            }
            ServerPlan::JsonNotificationCall => {
                write_json_response(
                    stream,
                    200,
                    json!({
                        "jsonrpc": "2.0",
                        "method": "notifications/progress",
                        "params": {}
                    }),
                    true,
                );
            }
            ServerPlan::LargeAcceptedCallBody => {
                let body = oversized_response_body();
                write_declared_response(
                    stream,
                    "202 Accepted",
                    "application/json",
                    body.len(),
                    &body,
                    std::iter::empty::<(&str, &str)>(),
                );
            }
            ServerPlan::LargeAcceptedChunkedCallBody => {
                write_chunked_response(
                    stream,
                    "202 Accepted",
                    "application/json",
                    &oversized_response_body(),
                    response_bytes_sent,
                );
            }
            ServerPlan::LargeNoContentCallBody => {
                write_declared_response(
                    stream,
                    "204 No Content",
                    "application/json",
                    MAX_MCP_OUTPUT_BYTES + 1,
                    "",
                    std::iter::empty::<(&str, &str)>(),
                );
            }
            ServerPlan::LargeNoContentChunkedCallBody => {
                write_chunked_response(
                    stream,
                    "204 No Content",
                    "application/json",
                    &oversized_response_body(),
                    response_bytes_sent,
                );
            }
            ServerPlan::ExactResponseBody => {
                let body = exact_json_rpc_body(request_id(body), MAX_MCP_OUTPUT_BYTES);
                write_response(stream, "200 OK", &body, []);
            }
            ServerPlan::LargeErrorCallBody => {
                let body = oversized_response_body();
                write_declared_response(
                    stream,
                    "500 Internal Server Error",
                    "application/json",
                    body.len(),
                    &body,
                    std::iter::empty::<(&str, &str)>(),
                );
            }
            ServerPlan::LargeErrorChunkedCallBody => {
                write_chunked_response(
                    stream,
                    "500 Internal Server Error",
                    "application/json",
                    &oversized_response_body(),
                    response_bytes_sent,
                );
            }
            _ => write_json_response(
                stream,
                200,
                json_rpc_result(
                    request_id(body),
                    json!({"content": [{"type": "text", "text": "ok"}], "isError": false}),
                ),
                true,
            ),
        },
        _ => write_json_response(
            stream,
            200,
            json!({"jsonrpc": "2.0", "id": 0, "result": {}}),
            true,
        ),
    }
}

/// 在取消或测试 server 停止前阻塞，用于验证初始化超时。
fn wait_until_stopped(duration: Duration, stop: &AtomicBool) {
    let deadline = Instant::now() + duration;
    while !stop.load(Ordering::Acquire) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
    }
}

/// 在客户端关闭当前 HTTP 连接或测试 server 停止前阻塞，用于验证 cancel/shutdown。
fn wait_for_client_disconnect(stream: &TcpStream, stop: &AtomicBool) {
    let mut probe = [0_u8; 1];
    while !stop.load(Ordering::Acquire) {
        match stream.peek(&mut probe) {
            Ok(0) => return,
            Ok(_) => thread::sleep(Duration::from_millis(5)),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(_) => return,
        }
    }
}

/// 夹具在有限时间后释放连接，避免阻塞单线程 listener 的 cleanup 请求。
fn wait_for_client_disconnect_or_timeout(stream: &TcpStream, stop: &AtomicBool, timeout: Duration) {
    // peek 必须使用短读超时，否则 accepted stream 原有的 2 秒超时会遮蔽 fixture deadline。
    let _ = stream.set_read_timeout(Some(Duration::from_millis(5)));
    let deadline = Instant::now() + timeout;
    let mut probe = [0_u8; 1];
    while !stop.load(Ordering::Acquire) && Instant::now() < deadline {
        match stream.peek(&mut probe) {
            Ok(0) => return,
            Ok(_) => thread::sleep(Duration::from_millis(5)),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(_) => return,
        }
    }
}

fn request_id(body: &Value) -> Value {
    body.get("id").cloned().unwrap_or(Value::Null)
}

/// 生成测试用的合法 MCP initialize result，不携带测试外部输入。
fn valid_initialize_result() -> Value {
    json!({
        "protocolVersion": "2025-06-18",
        "capabilities": {"tools": {}},
        "serverInfo": {"name": "test-mcp", "version": "1"}
    })
}

fn json_rpc_result(id: Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn write_json_response(stream: &mut TcpStream, status: u16, body: Value, session: bool) {
    let session_id = session.then_some("test-session");
    write_json_response_with_session(stream, status, body, session_id);
}

/// 写入带指定 session id 的 JSON response，供 session rollover fixture 使用。
fn write_json_response_with_session(
    stream: &mut TcpStream,
    status: u16,
    body: Value,
    session_id: Option<&str>,
) {
    let body = serde_json::to_string(&body).expect("测试 MCP JSON 必须可序列化");
    if let Some(session_id) = session_id {
        write_response(
            stream,
            &format!("{status} OK"),
            &body,
            [("Mcp-Session-Id", session_id)],
        );
    } else {
        write_response(
            stream,
            &format!("{status} OK"),
            &body,
            std::iter::empty::<(&str, &str)>(),
        );
    }
}

fn write_response<'a, I>(stream: &mut TcpStream, status: &str, body: &str, headers: I)
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    let mut response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    for (name, value) in headers {
        response.push_str(name);
        response.push_str(": ");
        response.push_str(value);
        response.push_str("\r\n");
    }
    response.push_str("\r\n");
    response.push_str(body);
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.shutdown(Shutdown::Both);
}

/// 写入指定 Content-Length 的响应，允许测试验证状态快捷路径也执行 body cap。
fn write_declared_response<'a, I>(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    declared_content_length: usize,
    body: &str,
    headers: I,
) where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    let mut response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {declared_content_length}\r\nConnection: close\r\n"
    );
    for (name, value) in headers {
        response.push_str(name);
        response.push_str(": ");
        response.push_str(value);
        response.push_str("\r\n");
    }
    response.push_str("\r\n");
    response.push_str(body);
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.shutdown(Shutdown::Both);
}

/// 发送 chunked shortcut response，验证 202/204 也必须消费并限制未知长度 body。
fn write_chunked_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &str,
    response_bytes_sent: &AtomicUsize,
) {
    let headers = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
    );
    if write_counted(stream, headers.as_bytes(), response_bytes_sent).is_err() {
        return;
    }
    for chunk in body.as_bytes().chunks(4096) {
        let result = format!("{:x}\r\n", chunk.len());
        if write_counted(stream, result.as_bytes(), response_bytes_sent)
            .and_then(|_| write_counted(stream, chunk, response_bytes_sent))
            .and_then(|_| write_counted(stream, b"\r\n", response_bytes_sent))
            .is_err()
        {
            return;
        }
    }
    let _ = write_counted(stream, b"0\r\n\r\n", response_bytes_sent);
    let _ = stream.shutdown(Shutdown::Both);
}

/// 发送包含通知或 server request 的 SSE 流，随后发送匹配的 JSON-RPC response。
fn write_sse_response(stream: &mut TcpStream, id: Value, server_request: bool) {
    let prefix = if server_request {
        json!({
            "jsonrpc": "2.0",
            "id": 999,
            "method": "sampling/createMessage",
            "params": {}
        })
    } else {
        json!({
            "jsonrpc": "2.0",
            "method": "notifications/progress",
            "params": {"progress": 1}
        })
    };
    let response = json_rpc_result(
        id,
        json!({"content": [{"type": "text", "text": "ok"}], "isError": false}),
    );
    let prefix = serde_json::to_string(&prefix).expect("SSE 前置消息必须可序列化");
    let response = serde_json::to_string(&response).expect("SSE response 必须可序列化");
    let body = format!("data: {prefix}\n\ndata: {response}\n\n");
    write_declared_response(
        stream,
        "200 OK",
        "text/event-stream",
        body.len(),
        &body,
        std::iter::empty::<(&str, &str)>(),
    );
}

/// 发送匹配 response 后再发送 server request，验证整个 SSE body 都按 JSON-RPC 分类。
fn write_sse_response_then_server_request(stream: &mut TcpStream, id: Value) {
    let response = json_rpc_result(
        id,
        json!({"content": [{"type": "text", "text": "ok"}], "isError": false}),
    );
    let server_request = json!({
        "jsonrpc": "2.0",
        "id": 999,
        "method": "sampling/createMessage",
        "params": {}
    });
    let response = serde_json::to_string(&response).expect("SSE response 必须可序列化");
    let server_request =
        serde_json::to_string(&server_request).expect("SSE server request 必须可序列化");
    let body = format!("data: {response}\n\ndata: {server_request}\n\n");
    write_declared_response(
        stream,
        "200 OK",
        "text/event-stream",
        body.len(),
        &body,
        std::iter::empty::<(&str, &str)>(),
    );
}

/// 发送没有空行终止的 matching response，验证 EOF 不得伪造完整 frame。
fn write_sse_truncated_response(stream: &mut TcpStream, id: Value) {
    let response = json_rpc_result(
        id,
        json!({"content": [{"type": "text", "text": "ok"}], "isError": false}),
    );
    let response = serde_json::to_string(&response).expect("SSE response 必须可序列化");
    let body = format!("data: {response}\n");
    write_declared_response(
        stream,
        "200 OK",
        "text/event-stream",
        body.len(),
        &body,
        std::iter::empty::<(&str, &str)>(),
    );
}

/// 发送无 Content-Length 且匹配 response 在前的超大 SSE body，验证不能提前 shortcut。
fn write_oversized_sse_chunked_response(
    stream: &mut TcpStream,
    id: Value,
    response_bytes_sent: &AtomicUsize,
) {
    let response = json_rpc_result(
        id,
        json!({"content": [{"type": "text", "text": "ok"}], "isError": false}),
    );
    let response = serde_json::to_string(&response).expect("SSE response 必须可序列化");
    let body = format!(
        "data: {response}\n\n{}",
        "x".repeat(MAX_MCP_OUTPUT_BYTES + 1)
    );
    let headers = b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n";
    if write_counted(stream, headers, response_bytes_sent).is_err() {
        return;
    }
    for chunk in body.as_bytes().chunks(4096) {
        let chunk_result = format!("{:x}\r\n", chunk.len());
        let result = write_counted(stream, chunk_result.as_bytes(), response_bytes_sent)
            .and_then(|_| write_counted(stream, chunk, response_bytes_sent))
            .and_then(|_| write_counted(stream, b"\r\n", response_bytes_sent));
        if result.is_err() {
            return;
        }
    }
    let _ = write_counted(stream, b"0\r\n\r\n", response_bytes_sent);
    let _ = stream.shutdown(Shutdown::Both);
}

/// 构造超过 body cap 的原始响应，供状态/错误/DELETE shortcut 使用。
fn oversized_response_body() -> String {
    "x".repeat(MAX_MCP_OUTPUT_BYTES + 1)
}

/// 构造恰好达到 body cap 的合法 JSON-RPC response，验证等于上限仍可解码。
fn exact_json_rpc_body(id: Value, target: usize) -> String {
    let result = json!({
        "content": [{"type": "text", "text": "ok"}],
        "isError": false
    });
    let base = json!({"jsonrpc": "2.0", "id": id, "result": result, "padding": ""});
    let base_length = serde_json::to_vec(&base)
        .expect("精确 body 基线必须可序列化")
        .len();
    assert!(target >= base_length, "精确 body 目标必须不小于基线");
    let body = json!({
        "jsonrpc": "2.0",
        "id": base.get("id").cloned().expect("精确 body 必须有 id"),
        "result": base
            .get("result")
            .cloned()
            .expect("精确 body 必须有 result"),
        "padding": "x".repeat(target - base_length)
    });
    let bytes = serde_json::to_vec(&body).expect("精确 body 必须可序列化");
    assert_eq!(bytes.len(), target, "精确 body 必须命中目标字节数");
    String::from_utf8(bytes).expect("精确 body 必须是 UTF-8")
}

/// 分块发送大 MCP JSON body，使测试能够观察解码前 limiter 是否提前中止读取。
fn write_large_json_response(
    stream: &mut TcpStream,
    id: Value,
    text_size: usize,
    chunked: bool,
    response_bytes_sent: &AtomicUsize,
) {
    let text = "x".repeat(text_size);
    let body = serde_json::to_vec(&json_rpc_result(
        id,
        json!({"content": [{"type": "text", "text": text}], "isError": false}),
    ))
    .expect("测试大 MCP JSON 必须可序列化");
    let headers = if chunked {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
        )
    } else {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
    };
    if write_counted(stream, headers.as_bytes(), response_bytes_sent).is_err() {
        return;
    }
    for chunk in body.chunks(4096) {
        let result = if chunked {
            let prefix = format!("{:x}\r\n", chunk.len());
            write_counted(stream, prefix.as_bytes(), response_bytes_sent)
                .and_then(|_| write_counted(stream, chunk, response_bytes_sent))
                .and_then(|_| write_counted(stream, b"\r\n", response_bytes_sent))
        } else {
            write_counted(stream, chunk, response_bytes_sent)
        };
        if result.is_err() {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    if chunked {
        let _ = write_counted(stream, b"0\r\n\r\n", response_bytes_sent);
    }
    let _ = stream.shutdown(Shutdown::Both);
}

/// 累加成功写入 socket 的字节数，避免把未发送的 body 误算为已交付。
fn write_counted(
    stream: &mut TcpStream,
    bytes: &[u8],
    response_bytes_sent: &AtomicUsize,
) -> std::io::Result<()> {
    let mut offset = 0_usize;
    while offset < bytes.len() {
        let written = stream.write(&bytes[offset..])?;
        if written == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "测试 MCP socket 写入返回零字节",
            ));
        }
        offset += written;
        response_bytes_sent.fetch_add(written, Ordering::AcqRel);
    }
    Ok(())
}

fn approved_config(server_name: &str, url: String) -> ApprovedMcpConfig {
    let mut servers = BTreeMap::new();
    servers.insert(server_name.to_owned(), McpServerSpec::Http { url });
    ApprovedMcpConfig { servers }
}

/// 等待 debug seam 明确写入的事件文件；等待条件本身有固定上限，不猜测时序。
async fn wait_for_seam_event(path: std::path::PathBuf) -> bool {
    let deadline = Instant::now() + TEST_TIMEOUT;
    while !path.exists() {
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    true
}

#[cfg(debug_assertions)]
/// 等待 cleanup ownership 达到指定状态，避免用固定 sleep 猜测任务调度。
async fn wait_for_cleanup_ownership(runtime: &McpRuntime, expected: (usize, usize)) {
    let deadline = Instant::now() + TEST_TIMEOUT;
    loop {
        if runtime.cleanup_ownership_for_test() == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "cleanup ownership 未达到预期: expected={expected:?}, actual={:?}",
            runtime.cleanup_ownership_for_test()
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

async fn runtime_with_server(
    server_name: &str,
    server: &MockMcpServer,
    expected: impl IntoIterator<Item = &'static str>,
) -> McpRuntime {
    let expected = expected
        .into_iter()
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    McpRuntime::new(approved_config(server_name, server.url()), expected)
        .await
        .expect("测试 HTTP MCP runtime 必须可创建")
}

#[tokio::test]
async fn empty_approval_does_not_spawn_mcp_or_expose_tools() {
    let runtime = McpRuntime::new(ApprovedMcpConfig::default(), BTreeSet::new())
        .await
        .expect("空审批应构造空 runtime");
    assert!(
        runtime
            .catalog()
            .await
            .expect("空 catalog 应成功")
            .servers
            .is_empty()
    );
    assert_eq!(runtime.http_session_count(), 0);
    assert!(runtime.model_visible_tools().is_empty());
    runtime
        .shutdown()
        .await
        .expect("空 runtime shutdown 应幂等");
}

#[tokio::test]
async fn stdio_spec_never_reaches_runtime_or_load_boundary() {
    let mut config = ApprovedMcpConfig::default();
    config.servers.insert(
        "local".to_owned(),
        McpServerSpec::Stdio {
            command: "/bin/echo".into(),
            args: Vec::new(),
        },
    );
    let runtime_error = McpRuntime::new(config, BTreeSet::new())
        .await
        .expect_err("stdio 不得进入 MCP runtime");
    assert_eq!(runtime_error.code(), "stdio_mcp_unavailable");

    let runtime_config = r#"
schema_version = 1
runtime_revision = ""
session_store_version = 1
session_cwd = "/tmp/efflab-session"
expected_tools = []

[model]
model_id = "test-model"
base_url = "http://127.0.0.1:43123/v1"
backend = "chat_completions"
token_env = "EFFLAB_L3B_BIND"

[approved_mcp.servers.local]
command = "/bin/echo"
args = []
"#;
    let file = NamedTempFile::new().expect("必须能创建 runtime config 临时文件");
    fs::write(file.path(), runtime_config).expect("必须能写入 stdio runtime config");
    let load_error = load_runtime_config_v1(file.path()).expect_err("load 边界必须拒绝 stdio");
    assert!(format!("{load_error:#}").contains("stdio_mcp_unavailable"));
}

#[tokio::test]
async fn actual_catalog_retains_unapproved_tools_while_model_uses_intersection() {
    let server = MockMcpServer::start(ServerPlan::Tools(vec!["ok".to_owned(), "extra".to_owned()]));
    let runtime = runtime_with_server("approved", &server, ["approved__ok"]).await;
    let catalog = runtime.catalog().await.expect("catalog 应可读取");
    let tools = &catalog.servers[0].session.tools;
    assert_eq!(
        tools
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>(),
        vec!["extra", "ok"],
        "实际 catalog 必须保留 extra 作为 Host 审计证据"
    );
    assert_eq!(runtime.model_visible_tools(), vec!["approved__ok"]);
    assert_eq!(
        runtime.model_tool_schemas()[0]["function"]["name"],
        "approved__ok"
    );
}

#[tokio::test]
async fn tools_list_follows_cursor_and_merges_all_actual_pages() {
    let server = MockMcpServer::start(ServerPlan::Paginated(vec![
        ToolsPage {
            request_cursor: None,
            tools: vec!["first".to_owned()],
            next_cursor: Some("page-2".to_owned()),
        },
        ToolsPage {
            request_cursor: Some("page-2".to_owned()),
            tools: vec!["second".to_owned()],
            next_cursor: None,
        },
    ]));
    let runtime = runtime_with_server("server", &server, ["server__second"]).await;

    let catalog = runtime.catalog().await.expect("分页 catalog 应可读取");
    assert_eq!(
        catalog.servers[0]
            .session
            .tools
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>(),
        vec!["first", "second"]
    );
    assert_eq!(runtime.model_visible_tools(), vec!["server__second"]);
    assert_eq!(
        runtime.model_tool_schemas()[0]["function"]["name"],
        "server__second"
    );
    assert_eq!(
        runtime
            .call("server__second", json!({}))
            .await
            .expect("第二页 approved tool 必须可调用")
            .text_content(),
        Some("ok")
    );
    assert_eq!(
        server
            .tool_list_cursors
            .lock()
            .expect("分页 cursor 记录锁必须可用")
            .clone(),
        vec![None, Some("page-2".to_owned())]
    );
}

#[tokio::test]
async fn tools_list_cursor_anomalies_fail_closed_without_looping() {
    for plan in [ServerPlan::EmptyCursor, ServerPlan::RepeatedCursor] {
        let server = MockMcpServer::start(plan);
        let runtime = runtime_with_server("server", &server, ["server__first"]).await;
        let catalog = runtime.catalog().await.expect("失败 catalog 应可读取");
        assert_eq!(catalog.servers[0].session.status, "error");
        assert_eq!(runtime.http_session_count(), 0);
        assert!(runtime.model_visible_tools().is_empty());
    }
}

#[tokio::test]
async fn expired_session_reinitializes_once_and_replays_call() {
    let server = MockMcpServer::start(ServerPlan::SessionExpires {
        repeat_call_404: false,
    });
    let runtime = runtime_with_server("server", &server, ["server__ok"]).await;
    let result = runtime
        .call("server__ok", json!({}))
        .await
        .expect("session 404 后重握手并重放应成功");
    assert_eq!(result.text_content(), Some("ok"));
    let follow_up = runtime
        .call("server__ok", json!({}))
        .await
        .expect("replay 成功提交后后续 call 必须复用 renewed session");
    assert_eq!(follow_up.text_content(), Some("ok"));

    let requests = server
        .requests
        .lock()
        .expect("session rollover 请求记录锁必须可用")
        .clone();
    let headers = server
        .request_headers
        .lock()
        .expect("session rollover 请求 header 记录锁必须可用")
        .clone();
    let initialize_headers = headers
        .iter()
        .filter(|request| request.method == "initialize")
        .collect::<Vec<_>>();
    assert_eq!(initialize_headers.len(), 2);
    assert_eq!(initialize_headers[0].session_id, None);
    assert_eq!(initialize_headers[0].protocol_version, None);
    assert_eq!(initialize_headers[0].request_id, Some(Value::from(1_u64)));
    assert_eq!(initialize_headers[1].session_id, None);
    assert_eq!(initialize_headers[1].protocol_version, None);
    assert_eq!(initialize_headers[1].request_id, Some(Value::from(4_u64)));
    let initialized_headers = headers
        .iter()
        .filter(|request| request.method == "notifications/initialized")
        .collect::<Vec<_>>();
    assert_eq!(initialized_headers.len(), 2);
    assert!(initialized_headers.iter().all(|request| {
        request.session_id.as_deref() == Some("expired-session")
            || request.session_id.as_deref() == Some("renewed-session")
    }));
    assert!(
        initialized_headers
            .iter()
            .all(|request| request.protocol_version.as_deref() == Some("2025-06-18"))
    );
    assert!(
        initialized_headers
            .iter()
            .all(|request| request.request_id.is_none())
    );
    let call_headers = headers
        .iter()
        .filter(|request| request.method == "tools/call")
        .collect::<Vec<_>>();
    assert_eq!(call_headers.len(), 3);
    assert_eq!(
        call_headers[0].session_id.as_deref(),
        Some("expired-session")
    );
    assert_eq!(
        call_headers[0].protocol_version.as_deref(),
        Some("2025-06-18")
    );
    assert_eq!(call_headers[0].request_id, Some(Value::from(3_u64)));
    assert_eq!(
        call_headers[1].session_id.as_deref(),
        Some("renewed-session")
    );
    assert_eq!(
        call_headers[1].protocol_version.as_deref(),
        Some("2025-06-18")
    );
    assert_eq!(call_headers[1].request_id, Some(Value::from(2_u64)));
    assert_eq!(
        call_headers[2].session_id.as_deref(),
        Some("renewed-session")
    );
    assert_eq!(
        call_headers[2].protocol_version.as_deref(),
        Some("2025-06-18")
    );
    assert_eq!(call_headers[2].request_id, Some(Value::from(3_u64)));
    assert_eq!(
        requests
            .iter()
            .filter(|method| *method == "initialize")
            .count(),
        2,
        "session 404 只能触发一次重新 initialize"
    );
    assert_eq!(
        requests
            .iter()
            .filter(|method| *method == "tools/call")
            .count(),
        3,
        "原始 call 只允许重放一次，成功提交后的后续 call 不得重新握手"
    );
}

#[tokio::test]
async fn repeated_expired_session_fails_closed_without_retry_loop() {
    let server = MockMcpServer::start(ServerPlan::SessionExpires {
        repeat_call_404: true,
    });
    let runtime = McpRuntime::new_with_timeout_for_test(
        approved_config("server", server.url()),
        BTreeSet::from(["server__ok".to_owned()]),
        Duration::from_millis(100),
    )
    .await
    .expect("session rollover fixture 初始化必须成功");
    let started = Instant::now();
    let error = runtime
        .call("server__ok", json!({}))
        .await
        .expect_err("第二次 session 404 必须 fail-closed");
    assert_eq!(error.code(), "mcp_call_failed");
    assert!(
        started.elapsed() < Duration::from_millis(200),
        "session 404 重试不得超出短 deadline"
    );
    let requests = server
        .requests
        .lock()
        .expect("session rollover 请求记录锁必须可用")
        .clone();
    assert_eq!(
        requests
            .iter()
            .filter(|method| *method == "initialize")
            .count(),
        2
    );
    assert_eq!(
        requests
            .iter()
            .filter(|method| *method == "tools/call")
            .count(),
        2
    );
    assert_eq!(
        runtime.http_session_count(),
        0,
        "第二次 404 后共享 session 必须从 shutdown 集合和调用路径移除"
    );
    let catalog = runtime
        .catalog()
        .await
        .expect("失效 session catalog 应可读取");
    assert_eq!(catalog.servers[0].session.status, "error");
    assert!(runtime.model_visible_tools().is_empty());
    assert!(runtime.model_tool_schemas().is_empty());

    let request_count = requests.len();
    let follow_up = runtime
        .call("server__ok", json!({}))
        .await
        .expect_err("失效 session 不得再次携带 stale session 发起 call");
    assert_eq!(follow_up.code(), "mcp_tool_not_ready");
    assert_eq!(
        server.requests.lock().expect("请求记录锁必须可用").len(),
        request_count,
        "失效 session 后不得再次请求 MCP 或 rollover"
    );
}

#[tokio::test]
async fn reinitialize_failures_cleanup_every_captured_candidate() {
    let cases = [
        (ReinitializeFailure::WrongId, true),
        (ReinitializeFailure::MissingResult, true),
        (ReinitializeFailure::MissingProtocol, true),
        (ReinitializeFailure::WrongProtocolVersion, true),
        (ReinitializeFailure::MissingSession, false),
        (ReinitializeFailure::InitializedFailure, true),
        (ReinitializeFailure::InitializedMalformed, true),
    ];
    for (failure, should_delete) in cases {
        let server = MockMcpServer::start(ServerPlan::ReinitializeFailure(failure));
        let runtime = runtime_with_server("server", &server, ["server__ok"]).await;
        let error = runtime
            .call("server__ok", json!({}))
            .await
            .expect_err("reinitialize failure 必须 fail-closed");
        assert_eq!(error.code(), "mcp_call_failed");
        assert_eq!(runtime.http_session_count(), 0);

        let headers = server
            .request_headers
            .lock()
            .expect("MCP request header 记录锁必须可用")
            .clone();
        let deletes = headers
            .iter()
            .filter(|request| request.method == "DELETE")
            .collect::<Vec<_>>();
        assert_eq!(
            deletes.len(),
            usize::from(should_delete),
            "candidate cleanup 是否发出 DELETE 与 failure 分支必须一致"
        );
        if should_delete {
            let delete = deletes[0];
            assert_eq!(delete.session_id.as_deref(), Some("renewed-session"));
            assert_eq!(delete.protocol_version.as_deref(), Some("2025-06-18"));
        }
    }
}

#[tokio::test]
async fn reinitialize_cleanup_failure_keeps_candidate_for_shutdown_and_stable_error() {
    let server = MockMcpServer::start(ServerPlan::ReinitializeFailure(
        ReinitializeFailure::CleanupFailure,
    ));
    let runtime = McpRuntime::new_with_timeout_for_test(
        approved_config("server", server.url()),
        BTreeSet::from(["server__ok".to_owned()]),
        Duration::from_millis(100),
    )
    .await
    .expect("cleanup failure fixture 初始化必须成功");

    let error = runtime
        .call("server__ok", json!({}))
        .await
        .expect_err("replay 失败后 cleanup failure 必须保留失败");
    assert_eq!(error.code(), "mcp_call_failed");
    assert_eq!(
        runtime.http_session_count(),
        1,
        "DELETE 失败的 candidate 必须继续属于 shutdown active session 集合"
    );

    let shutdown_error = runtime
        .shutdown()
        .await
        .expect_err("shutdown 重试失败必须返回稳定错误");
    assert_eq!(shutdown_error.code(), "mcp_shutdown_failed");
    let deletes = server
        .request_headers
        .lock()
        .expect("cleanup 请求记录锁必须可用")
        .iter()
        .filter(|request| request.method == "DELETE")
        .count();
    assert_eq!(
        deletes, 2,
        "candidate 必须在 call cleanup 后由 shutdown 再尝试一次"
    );
    assert_eq!(
        runtime.http_session_count(),
        1,
        "shutdown cleanup 失败后 candidate handle 必须继续由 runtime 持有"
    );
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn shutdown_gate_timeout_retains_shutdown_ownership_for_all_sessions() {
    let server = MockMcpServer::start(ServerPlan::Tools(vec!["ok".to_owned()]));
    let mut runtime = runtime_with_server("server", &server, ["server__ok"]).await;
    let seam = tempdir().expect("必须能创建 MCP cleanup seam 目录");
    fs::write(
        seam.path().join("candidate-before-delete.enabled"),
        b"enabled",
    )
    .expect("必须能启用 candidate DELETE barrier");
    runtime.set_test_seam_for_test(seam.path().to_path_buf());

    let candidate_runtime = runtime.clone();
    let candidate_url = server.url();
    let candidate = tokio::spawn(async move {
        candidate_runtime
            .cleanup_candidate_for_test(candidate_url, "blocked-candidate".to_owned())
            .await;
    });
    assert!(
        wait_for_seam_event(seam.path().join("candidate-before-delete.entered")).await,
        "candidate 必须在 shutdown gate 上暂停"
    );

    let shutdown_runtime = runtime.clone();
    let shutdown = tokio::spawn(async move { shutdown_runtime.shutdown().await });
    let shutdown_result = tokio::time::timeout(Duration::from_secs(3), shutdown)
        .await
        .expect("shutdown gate timeout 必须有界返回")
        .expect("shutdown leader task 不得 panic")
        .expect_err("cleanup gate 超时必须返回稳定失败");
    assert_eq!(shutdown_result.code(), "mcp_shutdown_failed");
    assert_eq!(
        runtime.cleanup_ownership_for_test(),
        (2, 2),
        "gate timeout 后初始 session 与 candidate 都必须保留在 shutdown ownership ledger"
    );
    assert_eq!(
        server
            .request_headers
            .lock()
            .expect("MCP request header 记录锁必须可用")
            .iter()
            .filter(|request| request.method == "DELETE")
            .count(),
        0,
        "shutdown gate timeout 不得在 owner 不明确时发出额外 DELETE"
    );

    fs::write(
        seam.path().join("candidate-before-delete.release"),
        b"release",
    )
    .expect("必须释放 candidate DELETE barrier");
    tokio::time::timeout(TEST_TIMEOUT, candidate)
        .await
        .expect("candidate cleanup task 必须有界返回")
        .expect("candidate cleanup task 不得 panic");
    assert_eq!(runtime.http_session_count(), 2);
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn candidate_waiting_for_cleanup_gate_does_not_repeat_shutdown_delete() {
    let server = MockMcpServer::start(ServerPlan::Tools(vec!["ok".to_owned()]));
    let mut runtime = runtime_with_server("server", &server, ["server__ok"]).await;
    let seam = tempdir().expect("必须能创建 MCP cleanup seam 目录");
    fs::write(
        seam.path().join("candidate-before-gate.enabled"),
        b"enabled",
    )
    .expect("必须能启用 candidate gate 前 barrier");
    runtime.set_test_seam_for_test(seam.path().to_path_buf());

    let candidate_runtime = runtime.clone();
    let candidate_url = server.url();
    let candidate = tokio::spawn(async move {
        candidate_runtime
            .cleanup_candidate_for_test(candidate_url, "waiting-candidate".to_owned())
            .await;
    });
    assert!(
        wait_for_seam_event(seam.path().join("candidate-before-gate.entered")).await,
        "candidate 必须在首次 owner 判定后暂停"
    );

    let shutdown_runtime = runtime.clone();
    let shutdown = tokio::spawn(async move { shutdown_runtime.shutdown().await });
    shutdown
        .await
        .expect("shutdown leader task 不得 panic")
        .expect("shutdown 必须接管等待中的 candidate 并成功关闭");
    assert_eq!(
        server
            .request_headers
            .lock()
            .expect("MCP request header 记录锁必须可用")
            .iter()
            .filter(|request| request.method == "DELETE")
            .count(),
        2,
        "shutdown owner 应关闭初始 session 与 candidate，各发送一次 DELETE"
    );

    fs::write(
        seam.path().join("candidate-before-gate.release"),
        b"release",
    )
    .expect("必须释放 candidate gate 前 barrier");
    tokio::time::timeout(TEST_TIMEOUT, candidate)
        .await
        .expect("candidate cleanup task 必须有界返回")
        .expect("candidate cleanup task 不得 panic");
    assert_eq!(
        server
            .request_headers
            .lock()
            .expect("MCP request header 记录锁必须可用")
            .iter()
            .filter(|request| request.method == "DELETE")
            .count(),
        2,
        "candidate 观察到 shutdown 已成功关闭后不得重复 DELETE"
    );
    assert_eq!(runtime.http_session_count(), 0);
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn shutdown_leader_abort_publishes_stable_failure_to_waiter() {
    let server = MockMcpServer::start(ServerPlan::Tools(vec!["ok".to_owned()]));
    let mut runtime = runtime_with_server("server", &server, ["server__ok"]).await;
    let seam = tempdir().expect("必须能创建 MCP cleanup seam 目录");
    fs::write(seam.path().join("shutdown-snapshot.enabled"), b"enabled")
        .expect("必须能启用 shutdown snapshot barrier");
    runtime.set_test_seam_for_test(seam.path().to_path_buf());

    let leader_runtime = runtime.clone();
    let leader = tokio::spawn(async move { leader_runtime.shutdown().await });
    assert!(
        wait_for_seam_event(seam.path().join("shutdown-snapshot.entered")).await,
        "shutdown leader 必须到达可取消的 snapshot 窗口"
    );
    leader.abort();
    assert!(
        leader.await.is_err(),
        "被 abort 的 shutdown leader 必须结束为取消"
    );

    let waiter = tokio::time::timeout(Duration::from_millis(200), runtime.shutdown())
        .await
        .expect("shutdown waiter 不得永久等待 leader completion")
        .expect_err("leader 被取消后 waiter 必须收到稳定失败");
    assert_eq!(waiter.code(), "mcp_shutdown_failed");
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn non_leader_shutdown_wait_has_independent_bounded_deadline() {
    let server = MockMcpServer::start(ServerPlan::Tools(vec!["ok".to_owned()]));
    let mut runtime = runtime_with_server("server", &server, ["server__ok"]).await;
    let seam = tempdir().expect("必须能创建 MCP cleanup seam 目录");
    fs::write(
        seam.path().join("shutdown-before-completion.enabled"),
        b"enabled",
    )
    .expect("必须能启用 shutdown completion barrier");
    runtime.set_test_seam_for_test(seam.path().to_path_buf());

    let leader_runtime = runtime.clone();
    let leader = tokio::spawn(async move { leader_runtime.shutdown().await });
    assert!(
        wait_for_seam_event(seam.path().join("shutdown-before-completion.entered")).await,
        "shutdown leader 必须停在 completion 前窗口"
    );

    let waiter_runtime = runtime.clone();
    let waiter = tokio::spawn(async move { waiter_runtime.shutdown().await });
    let waiter_result = tokio::time::timeout(Duration::from_millis(2300), waiter)
        .await
        .expect("非 leader shutdown waiter 必须有独立 deadline")
        .expect("非 leader shutdown waiter task 不得 panic")
        .expect_err("leader 未发布 completion 时 waiter 必须返回稳定失败");
    assert_eq!(waiter_result.code(), "mcp_shutdown_failed");

    fs::write(
        seam.path().join("shutdown-before-completion.release"),
        b"release",
    )
    .expect("必须释放 shutdown completion barrier");
    let _ = tokio::time::timeout(TEST_TIMEOUT, leader)
        .await
        .expect("leader 释放后必须有界退出")
        .expect("leader task 不得 panic");
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn late_candidate_after_shutdown_completion_is_archived_without_new_worker() {
    let server = MockMcpServer::start(ServerPlan::Tools(vec!["ok".to_owned()]));
    let mut runtime = runtime_with_server("server", &server, ["server__ok"]).await;
    let seam = tempdir().expect("必须能创建 MCP cleanup seam 目录");
    runtime.set_test_seam_for_test(seam.path().to_path_buf());

    runtime
        .shutdown()
        .await
        .expect("candidate 登记前的 shutdown 必须完成并封存 cleanup phase");
    runtime
        .cleanup_candidate_for_test(server.url(), "late-after-completion".to_owned())
        .await;

    assert!(
        !seam
            .path()
            .join("sealed-cleanup-before-delete.entered")
            .exists(),
        "shutdown completion 发布后不得再 admission 新 Sealed worker"
    );
    assert_eq!(runtime.cleanup_ownership_for_test(), (0, 0));
}

#[cfg(debug_assertions)]
#[test]
fn sealed_cleanup_recovers_after_executor_drop() {
    let server = MockMcpServer::start(ServerPlan::LateCandidateCleanupFailure);
    let seam = tempdir().expect("必须能创建 MCP cleanup seam 目录");
    fs::write(
        seam.path().join("sealed-cleanup-before-delete.enabled"),
        b"enabled",
    )
    .expect("必须能启用 Sealed worker barrier");
    fs::write(
        seam.path().join("shutdown-before-completion.enabled"),
        b"enabled",
    )
    .expect("必须能启用 shutdown completion barrier");
    let executor = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .expect("必须能创建测试 executor");
    let runtime = executor.block_on(async {
        let mut runtime = McpRuntime::new(
            approved_config("server", server.url()),
            BTreeSet::from(["server__ok".to_owned()]),
        )
        .await
        .expect("executor-drop fixture 初始化必须成功");
        runtime.set_test_seam_for_test(seam.path().to_path_buf());
        let shutdown_runtime = runtime.clone();
        tokio::spawn(async move {
            let _ = shutdown_runtime.shutdown().await;
        });
        assert!(
            wait_for_seam_event(seam.path().join("shutdown-before-completion.entered")).await,
            "shutdown 必须在 completion barrier 前停住"
        );

        let candidate_runtime = runtime.clone();
        let candidate_url = server.url();
        tokio::spawn(async move {
            candidate_runtime
                .cleanup_candidate_for_test(candidate_url, "executor-drop".to_owned())
                .await;
        });
        assert!(
            wait_for_seam_event(seam.path().join("sealed-cleanup-before-delete.entered")).await,
            "worker 必须在 executor drop 前取得 in-flight job"
        );
        runtime
    });
    drop(executor);

    assert_eq!(runtime.cleanup_ownership_for_test(), (0, 0));
    assert_eq!(runtime.cleanup_terminal_failures_for_test().len(), 1);
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn sealed_late_candidate_delete_failure_is_archived_after_bounded_owner() {
    let server = MockMcpServer::start(ServerPlan::LateCandidateCleanupFailure);
    let mut runtime = runtime_with_server("server", &server, ["server__ok"]).await;
    let seam = tempdir().expect("必须能创建 MCP cleanup seam 目录");
    fs::write(
        seam.path().join("shutdown-before-completion.enabled"),
        b"enabled",
    )
    .expect("必须能启用 shutdown completion barrier");
    runtime.set_test_seam_for_test(seam.path().to_path_buf());
    let shutdown_runtime = runtime.clone();
    let shutdown = tokio::spawn(async move { shutdown_runtime.shutdown().await });
    assert!(
        wait_for_seam_event(seam.path().join("shutdown-before-completion.entered")).await,
        "shutdown 必须在 completion barrier 前停住"
    );

    // candidate 在 Sealed 但 completion 尚未发布时登记；worker 必须有限重试后归档。
    runtime
        .cleanup_candidate_for_test(server.url(), "sealed-late-candidate".to_owned())
        .await;
    server.wait_for_method_count("DELETE", 3).await;

    assert_eq!(
        runtime.cleanup_ownership_for_test(),
        (0, 0),
        "Sealed worker 失败后不得留下没有执行者的 pending/claim ledger"
    );
    assert_eq!(runtime.http_session_count(), 0);
    assert_eq!(
        runtime.cleanup_terminal_failures_for_test(),
        vec![(
            "mcp_call_failed".to_owned(),
            "mcp_shutdown_failed".to_owned(),
            2,
        )],
        "bounded worker 必须把失败记录为稳定终态并保留尝试次数"
    );
    fs::write(
        seam.path().join("shutdown-before-completion.release"),
        b"release",
    )
    .expect("必须释放 shutdown completion barrier");
    let shutdown_error = tokio::time::timeout(TEST_TIMEOUT, shutdown)
        .await
        .expect("shutdown leader 必须有界退出")
        .expect("shutdown leader task 不得 panic")
        .expect_err("candidate cleanup 终态失败必须反映到 shutdown");
    assert_eq!(shutdown_error.code(), "mcp_shutdown_failed");
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn sealed_cleanup_owner_survives_caller_cancel_and_runtime_drop() {
    let server = MockMcpServer::start(ServerPlan::LateCandidateCleanupFailure);
    let mut runtime = runtime_with_server("server", &server, ["server__ok"]).await;
    let seam = tempdir().expect("必须能创建 MCP cleanup seam 目录");
    fs::write(seam.path().join("candidate-registered.enabled"), b"enabled")
        .expect("必须能启用 candidate registration barrier");
    fs::write(
        seam.path().join("sealed-cleanup-before-delete.enabled"),
        b"enabled",
    )
    .expect("必须能启用 Sealed worker barrier");
    runtime.set_test_seam_for_test(seam.path().to_path_buf());
    fs::write(
        seam.path().join("shutdown-before-completion.enabled"),
        b"enabled",
    )
    .expect("必须能启用 shutdown completion barrier");
    let shutdown_runtime = runtime.clone();
    let shutdown = tokio::spawn(async move { shutdown_runtime.shutdown().await });
    assert!(
        wait_for_seam_event(seam.path().join("shutdown-before-completion.entered")).await,
        "shutdown 必须在 completion barrier 前停住"
    );

    let candidate_runtime = runtime.clone();
    let candidate_url = server.url();
    let candidate = tokio::spawn(async move {
        candidate_runtime
            .cleanup_candidate_for_test(candidate_url, "cancelled-candidate".to_owned())
            .await;
    });
    assert!(
        wait_for_seam_event(seam.path().join("candidate-registered.entered")).await,
        "candidate 必须先完成原子登记"
    );
    assert!(
        wait_for_seam_event(seam.path().join("sealed-cleanup-before-delete.entered")).await,
        "runtime-owned worker 必须到达 DELETE 前窗口"
    );

    // caller 被取消且最后一个外部 runtime handle 被丢弃后，worker 仍须持有并完成 owner。
    candidate.abort();
    let _ = candidate.await;
    drop(runtime);
    fs::write(
        seam.path().join("sealed-cleanup-before-delete.release"),
        b"release",
    )
    .expect("必须释放 Sealed worker barrier");
    server.wait_for_method_count("DELETE", 3).await;
    fs::write(
        seam.path().join("shutdown-before-completion.release"),
        b"release",
    )
    .expect("必须释放 shutdown completion barrier");
    let shutdown_error = tokio::time::timeout(TEST_TIMEOUT, shutdown)
        .await
        .expect("shutdown leader 必须有界退出")
        .expect("shutdown leader task 不得 panic")
        .expect_err("candidate cleanup 终态失败必须反映到 shutdown");
    assert_eq!(shutdown_error.code(), "mcp_shutdown_failed");
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn sealed_cleanup_worker_exit_window_does_not_open_second_owner() {
    let server = MockMcpServer::start(ServerPlan::LateCandidateCleanupFailure);
    let mut runtime = runtime_with_server("server", &server, ["server__ok"]).await;
    let seam = tempdir().expect("必须能创建 MCP cleanup seam 目录");
    fs::write(
        seam.path().join("shutdown-before-completion.enabled"),
        b"enabled",
    )
    .expect("必须能启用 shutdown completion barrier");
    fs::write(
        seam.path().join("sealed-cleanup-after-exit-state.enabled"),
        b"enabled",
    )
    .expect("必须能启用 worker state-lock 后 exit barrier");
    runtime.set_test_seam_for_test(seam.path().to_path_buf());

    let shutdown_runtime = runtime.clone();
    let shutdown = tokio::spawn(async move { shutdown_runtime.shutdown().await });
    assert!(
        wait_for_seam_event(seam.path().join("shutdown-before-completion.entered")).await,
        "shutdown 必须在 completion 发布前停住"
    );

    let first_runtime = runtime.clone();
    let first_url = server.url();
    let first = tokio::spawn(async move {
        first_runtime
            .cleanup_candidate_for_test(first_url, "first-exit-window".to_owned())
            .await;
    });
    assert!(
        wait_for_seam_event(seam.path().join("sealed-cleanup-after-exit-state.entered")).await,
        "首个 worker 必须在 state lock 释放后进入真实退出窗口"
    );

    let second_runtime = runtime.clone();
    let second_url = server.url();
    let second = tokio::spawn(async move {
        second_runtime
            .cleanup_candidate_for_test(second_url, "second-exit-window".to_owned())
            .await;
    });
    wait_for_cleanup_ownership(&runtime, (1, 1)).await;
    assert_eq!(
        runtime.cleanup_worker_handle_count_for_test(),
        1,
        "旧 worker 尚未退出时新 candidate 不得启动第二个 cleanup owner"
    );

    fs::write(
        seam.path().join("sealed-cleanup-after-exit-state.release"),
        b"release",
    )
    .expect("必须释放 worker state-lock 后 exit barrier");
    first.await.expect("首个 candidate caller 不得 panic");
    second.await.expect("第二个 candidate caller 不得 panic");
    fs::write(
        seam.path().join("shutdown-before-completion.release"),
        b"release",
    )
    .expect("必须释放 shutdown completion barrier");
    let shutdown_error = tokio::time::timeout(TEST_TIMEOUT, shutdown)
        .await
        .expect("shutdown 必须有界退出")
        .expect("shutdown task 不得 panic")
        .expect_err("candidate cleanup 终态失败必须反映到 shutdown");
    assert_eq!(shutdown_error.code(), "mcp_shutdown_failed");
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn sealed_cleanup_serializes_concurrent_candidates_with_one_bounded_owner() {
    let server = MockMcpServer::start(ServerPlan::LateCandidateCleanupFailure);
    let mut runtime = runtime_with_server("server", &server, ["server__ok"]).await;
    let seam = tempdir().expect("必须能创建 MCP cleanup seam 目录");
    fs::write(
        seam.path().join("sealed-cleanup-before-delete.enabled"),
        b"enabled",
    )
    .expect("必须能启用 Sealed worker barrier");
    runtime.set_test_seam_for_test(seam.path().to_path_buf());
    fs::write(
        seam.path().join("shutdown-before-completion.enabled"),
        b"enabled",
    )
    .expect("必须能启用 shutdown completion barrier");
    let shutdown_runtime = runtime.clone();
    let shutdown = tokio::spawn(async move { shutdown_runtime.shutdown().await });
    assert!(
        wait_for_seam_event(seam.path().join("shutdown-before-completion.entered")).await,
        "shutdown 必须在 completion barrier 前停住"
    );

    let first_runtime = runtime.clone();
    let first_url = server.url();
    let first = tokio::spawn(async move {
        first_runtime
            .cleanup_candidate_for_test(first_url, "first-candidate".to_owned())
            .await;
    });
    assert!(
        wait_for_seam_event(seam.path().join("sealed-cleanup-before-delete.entered")).await,
        "首个 Sealed candidate 必须由 worker 取得 cleanup gate"
    );

    let second_runtime = runtime.clone();
    let second_url = server.url();
    let second = tokio::spawn(async move {
        second_runtime
            .cleanup_candidate_for_test(second_url, "second-candidate".to_owned())
            .await;
    });
    // 第二个 candidate 只能排队，不能创建绕过共享 gate 的并发 DELETE。
    wait_for_cleanup_ownership(&runtime, (2, 2)).await;
    assert_eq!(
        server
            .request_headers
            .lock()
            .expect("MCP 请求 header 记录锁必须可用")
            .iter()
            .filter(|request| request.method == "DELETE")
            .count(),
        1,
        "共享 gate 下第二个 candidate 不得在首个 worker 未释放前发送 DELETE"
    );

    fs::write(
        seam.path().join("sealed-cleanup-before-delete.release"),
        b"release",
    )
    .expect("必须释放 Sealed worker barrier");
    server.wait_for_method_count("DELETE", 5).await;
    first
        .await
        .expect("首个 candidate worker caller 不得 panic");
    second
        .await
        .expect("第二个 candidate worker caller 不得 panic");
    fs::write(
        seam.path().join("shutdown-before-completion.release"),
        b"release",
    )
    .expect("必须释放 shutdown completion barrier");
    let shutdown_error = tokio::time::timeout(TEST_TIMEOUT, shutdown)
        .await
        .expect("shutdown leader 必须有界退出")
        .expect("shutdown leader task 不得 panic")
        .expect_err("candidate cleanup 终态失败必须反映到 shutdown");
    assert_eq!(shutdown_error.code(), "mcp_shutdown_failed");
    assert_eq!(runtime.cleanup_ownership_for_test(), (0, 0));
    assert_eq!(runtime.cleanup_terminal_failures_for_test().len(), 2);
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn shutdown_owns_candidate_registered_after_cleanup_snapshot() {
    let server = MockMcpServer::start(ServerPlan::Tools(vec!["ok".to_owned()]));
    let mut runtime = runtime_with_server("server", &server, ["server__ok"]).await;
    let seam = tempdir().expect("必须能创建 MCP cleanup seam 目录");
    fs::write(seam.path().join("shutdown-snapshot.enabled"), b"enabled")
        .expect("必须能启用 shutdown snapshot barrier");
    fs::write(
        seam.path().join("candidate-before-delete.enabled"),
        b"enabled",
    )
    .expect("必须能启用 candidate DELETE barrier");
    runtime.set_test_seam_for_test(seam.path().to_path_buf());

    let shutdown_runtime = runtime.clone();
    let shutdown = tokio::spawn(async move { shutdown_runtime.shutdown().await });
    assert!(
        wait_for_seam_event(seam.path().join("shutdown-snapshot.entered")).await,
        "shutdown 必须到达 ownership snapshot barrier"
    );
    assert_eq!(
        server
            .request_headers
            .lock()
            .expect("MCP 请求记录锁必须可用")
            .iter()
            .filter(|request| request.method == "DELETE")
            .count(),
        0,
        "snapshot barrier 未释放前不能关闭初始 session"
    );

    let candidate_runtime = runtime.clone();
    let candidate_url = server.url();
    let candidate = tokio::spawn(async move {
        candidate_runtime
            .cleanup_candidate_for_test(candidate_url, "late-candidate".to_owned())
            .await;
    });
    assert!(
        wait_for_seam_event(seam.path().join("candidate-registered.entered")).await,
        "candidate 必须在 shutdown snapshot 后登记"
    );

    assert!(
        !seam.path().join("candidate-before-delete.entered").exists(),
        "shutdown 持有 cleanup gate 时迟到 candidate 不得进入独立 DELETE"
    );
    assert_eq!(
        server
            .request_headers
            .lock()
            .expect("MCP 请求记录锁必须可用")
            .iter()
            .filter(|request| request.method == "DELETE")
            .count(),
        0,
        "迟到 candidate 必须由 shutdown owner 处理"
    );

    fs::write(seam.path().join("shutdown-snapshot.release"), b"release")
        .expect("必须释放 shutdown snapshot barrier");
    shutdown
        .await
        .expect("shutdown leader task 不得 panic")
        .expect("shutdown 必须接管并关闭迟到 candidate");
    tokio::time::timeout(TEST_TIMEOUT, candidate)
        .await
        .expect("shutdown 已 claim 的 candidate 不得卡在 caller cleanup")
        .expect("candidate cleanup task 不得 panic");
    assert!(!seam.path().join("candidate-before-delete.entered").exists());
    server.wait_for_method_count("DELETE", 2).await;
    assert_eq!(runtime.http_session_count(), 0);
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn shutdown_claims_candidate_registered_before_cleanup_snapshot() {
    let server = MockMcpServer::start(ServerPlan::Tools(vec!["ok".to_owned()]));
    let mut runtime = runtime_with_server("server", &server, ["server__ok"]).await;
    let seam = tempdir().expect("必须能创建 MCP cleanup seam 目录");
    fs::write(seam.path().join("candidate-registered.enabled"), b"enabled")
        .expect("必须能启用 candidate registration barrier");
    fs::write(
        seam.path().join("candidate-before-delete.enabled"),
        b"enabled",
    )
    .expect("必须能启用 candidate DELETE barrier");
    fs::write(seam.path().join("shutdown-snapshot.enabled"), b"enabled")
        .expect("必须能启用 shutdown snapshot barrier");
    runtime.set_test_seam_for_test(seam.path().to_path_buf());

    let candidate_runtime = runtime.clone();
    let candidate_url = server.url();
    let candidate = tokio::spawn(async move {
        candidate_runtime
            .cleanup_candidate_for_test(candidate_url, "early-candidate".to_owned())
            .await;
    });
    assert!(
        wait_for_seam_event(seam.path().join("candidate-registered.entered")).await,
        "candidate 必须先于 shutdown snapshot 登记"
    );

    let shutdown_runtime = runtime.clone();
    let shutdown = tokio::spawn(async move { shutdown_runtime.shutdown().await });
    assert!(
        wait_for_seam_event(seam.path().join("shutdown-snapshot.entered")).await,
        "shutdown 必须在 candidate 已登记后到达 snapshot barrier"
    );
    fs::write(seam.path().join("candidate-registered.release"), b"release")
        .expect("必须释放 candidate registration barrier");
    assert!(
        !seam.path().join("candidate-before-delete.entered").exists(),
        "snapshot 前登记的 candidate 也不得再次取得独立 DELETE owner"
    );
    assert_eq!(
        server
            .request_headers
            .lock()
            .expect("MCP 请求记录锁必须可用")
            .iter()
            .filter(|request| request.method == "DELETE")
            .count(),
        0,
        "shutdown snapshot barrier 未释放前不能执行 DELETE"
    );

    fs::write(seam.path().join("shutdown-snapshot.release"), b"release")
        .expect("必须释放 shutdown snapshot barrier");
    shutdown
        .await
        .expect("shutdown leader task 不得 panic")
        .expect("shutdown 必须关闭初始 session 与已登记 candidate");
    tokio::time::timeout(TEST_TIMEOUT, candidate)
        .await
        .expect("shutdown 已 claim 的 candidate 不得卡在 caller cleanup")
        .expect("candidate cleanup task 不得 panic");
    assert!(!seam.path().join("candidate-before-delete.entered").exists());
    server.wait_for_method_count("DELETE", 2).await;
    assert_eq!(runtime.http_session_count(), 0);
}

#[tokio::test]
async fn reinitialize_initialized_cancellation_cleans_candidate_before_return() {
    let server = MockMcpServer::start(ServerPlan::ReinitializeFailure(
        ReinitializeFailure::InitializedCancellation,
    ));
    let runtime = runtime_with_server("server", &server, ["server__ok"]).await;
    let cancellation = McpCancellationToken::new();
    let call_runtime = runtime.clone();
    let call_cancellation = cancellation.clone();
    let call = tokio::spawn(async move {
        call_runtime
            .call_with_cancellation("server__ok", json!({}), call_cancellation)
            .await
    });
    server
        .wait_for_method_count("notifications/initialized", 2)
        .await;
    cancellation.cancel();
    let error = call
        .await
        .expect("取消中的 MCP call task 不得 panic")
        .expect_err("initialized 取消必须返回稳定错误");
    assert_eq!(error.code(), "mcp_call_cancelled");
    server.wait_for_method("DELETE").await;
    let delete = server
        .request_headers
        .lock()
        .expect("MCP request header 记录锁必须可用")
        .iter()
        .find(|request| request.method == "DELETE")
        .cloned()
        .expect("candidate cleanup 必须发出 DELETE");
    assert_eq!(delete.session_id.as_deref(), Some("renewed-session"));
    assert_eq!(runtime.http_session_count(), 0);
}

#[tokio::test]
async fn invalid_tool_name_stays_audit_only_and_cannot_be_called() {
    let tool_name = "bad.name".to_owned();
    let qualified_name = format!("server__{tool_name}");
    let server = MockMcpServer::start(ServerPlan::Tools(vec![tool_name.clone()]));
    let runtime = McpRuntime::new(
        approved_config("server", server.url()),
        BTreeSet::from(["server__allowed".to_owned()]),
    )
    .await
    .expect("实际 catalog 中的非法工具名不应阻止 runtime 创建");

    let catalog = runtime
        .catalog()
        .await
        .expect("非法工具名 catalog 应可读取");
    let tool = &catalog.servers[0].session.tools[0];
    assert_eq!(tool.name, tool_name);
    assert!(!tool.enabled, "非法工具名必须在 Host catalog 中 disabled");
    assert!(runtime.model_visible_tools().is_empty());
    assert!(runtime.model_tool_schemas().is_empty());
    let error = runtime
        .call(&qualified_name, json!({}))
        .await
        .expect_err("非法工具名不得进入 MCP call");
    assert_eq!(error.code(), "mcp_tool_name_invalid");
    assert_eq!(
        server
            .requests
            .lock()
            .expect("非法工具请求记录锁必须可用")
            .iter()
            .filter(|method| *method == "tools/call")
            .count(),
        0,
    );
}

#[tokio::test]
async fn runtime_rejects_invalid_expected_qualified_tool_names_before_handshake() {
    for invalid in [
        "server",
        "server__",
        "server__bad.name",
        "server__bad name",
        "server__search__extra",
        "GrokBuild:*",
    ] {
        let error = McpRuntime::new(
            ApprovedMcpConfig::default(),
            BTreeSet::from([invalid.to_owned()]),
        )
        .await
        .expect_err("非法 expected tool name 必须在 runtime 入口拒绝");
        assert_eq!(
            error.code(),
            "mcp_tool_name_invalid",
            "非法 expected tool name 错误分类必须稳定: {invalid:?}"
        );
    }

    McpRuntime::new(
        ApprovedMcpConfig::default(),
        BTreeSet::from(["GrokBuild:efflab_noop".to_owned()]),
    )
    .await
    .expect("合同内置 noop 例外必须保留");
}

#[tokio::test]
async fn tool_over_record_id_limit_stays_audit_only_and_cannot_be_called() {
    let tool_name = format!("tool-{}", "x".repeat(MAX_RECORD_ID_BYTES));
    let qualified_name = format!("server__{tool_name}");
    let server = MockMcpServer::start(ServerPlan::Tools(vec![tool_name.clone()]));
    let runtime = McpRuntime::new(
        approved_config("server", server.url()),
        BTreeSet::from([qualified_name.clone()]),
    )
    .await
    .expect("极端工具 runtime 必须可创建");

    let catalog = runtime.catalog().await.expect("极端工具 catalog 应可读取");
    let tool = &catalog.servers[0].session.tools[0];
    assert_eq!(tool.name, tool_name);
    assert!(
        !tool.enabled,
        "超过 journal identifier 边界的实际工具必须 disabled"
    );
    assert!(runtime.model_visible_tools().is_empty());
    assert!(runtime.model_tool_schemas().is_empty());
    let error = runtime
        .call(&qualified_name, json!({}))
        .await
        .expect_err("极端工具不得进入 MCP call");
    assert_eq!(error.code(), "mcp_tool_not_approved");
    assert_eq!(
        server
            .requests
            .lock()
            .expect("极端工具请求记录锁必须可用")
            .iter()
            .filter(|method| *method == "tools/call")
            .count(),
        0,
        "不可持久化工具不得发出 tools/call"
    );
}

#[tokio::test]
async fn tools_list_rejects_aggregate_tool_count_before_ready_commit() {
    let names = (0..1025).map(|index| format!("tool-{index}")).collect();
    let server = MockMcpServer::start(ServerPlan::Tools(names));
    let runtime = runtime_with_server("server", &server, ["server__tool-0"]).await;
    let catalog = runtime
        .catalog()
        .await
        .expect("aggregate cap 失败后 catalog 仍应可读取");
    assert_eq!(catalog.servers[0].session.status, "error");
    assert_eq!(runtime.http_session_count(), 0);
    server.wait_for_method("DELETE").await;
    assert!(runtime.model_visible_tools().is_empty());
}

#[tokio::test]
async fn tools_list_rejects_aggregate_metadata_before_ready_commit() {
    let server = MockMcpServer::start(ServerPlan::LargeMetadata {
        pages: 5,
        description_bytes: 900_000,
    });
    let runtime = runtime_with_server("server", &server, ["server__tool-4"]).await;
    let catalog = runtime
        .catalog()
        .await
        .expect("aggregate metadata cap 失败后 catalog 仍应可读取");
    assert_eq!(catalog.servers[0].session.status, "error");
    assert_eq!(runtime.http_session_count(), 0);
    server.wait_for_method("DELETE").await;
    assert!(runtime.model_tool_schemas().is_empty());
}

#[tokio::test]
async fn tools_list_rejects_oversized_cursor_before_follow_up_request() {
    let server = MockMcpServer::start(ServerPlan::Paginated(vec![ToolsPage {
        request_cursor: None,
        tools: vec!["first".to_owned()],
        next_cursor: Some("x".repeat(4097)),
    }]));
    let runtime = runtime_with_server("server", &server, ["server__first"]).await;
    let catalog = runtime
        .catalog()
        .await
        .expect("oversized cursor 失败后 catalog 仍应可读取");
    assert_eq!(catalog.servers[0].session.status, "error");
    assert_eq!(runtime.http_session_count(), 0);
    let cursors = server
        .tool_list_cursors
        .lock()
        .expect("cursor 记录锁必须可用")
        .clone();
    assert_eq!(cursors, vec![None]);
    server.wait_for_method("DELETE").await;
}

#[tokio::test]
async fn malformed_initialize_with_session_header_is_deleted() {
    let server = MockMcpServer::start(ServerPlan::MalformedInitialize);
    let runtime = McpRuntime::new(
        approved_config("malformed", server.url()),
        BTreeSet::from(["malformed__ok".to_owned()]),
    )
    .await
    .expect("malformed initialize 应保留非 ready catalog");
    let catalog = runtime.catalog().await.expect("非 ready catalog 应可读取");
    assert_eq!(catalog.servers[0].session.status, "error");
    server.wait_for_method("DELETE").await;
    assert_eq!(runtime.http_session_count(), 0);
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn initial_handshake_cleanup_failure_remains_owned_until_shutdown() {
    let server = MockMcpServer::start(ServerPlan::InitialHandshakeFailure(
        InitialHandshakeFailure::InitializedFailure,
    ));
    let runtime = McpRuntime::new_with_timeout_for_test(
        approved_config("initial", server.url()),
        BTreeSet::from(["initial__ok".to_owned()]),
        Duration::from_millis(250),
    )
    .await
    .expect("初始 handshake 失败应返回保留 error catalog 的 runtime");

    let catalog = runtime
        .catalog()
        .await
        .expect("初始 handshake 失败后的 catalog 应可读取");
    assert_eq!(catalog.servers[0].session.status, "error");
    assert_eq!(runtime.http_session_count(), 1);
    assert_eq!(
        runtime.cleanup_ownership_for_test(),
        (1, 0),
        "初始 session DELETE 失败后必须保留 pending cleanup owner"
    );
    let deletes = server
        .request_headers
        .lock()
        .expect("初始 cleanup 请求记录锁必须可用")
        .iter()
        .filter(|request| request.method == "DELETE")
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        deletes.len(),
        1,
        "初始化失败分支必须先尝试一次 session DELETE"
    );
    assert_eq!(deletes[0].session_id.as_deref(), Some("initial-session"));
    assert_eq!(deletes[0].protocol_version.as_deref(), Some("2025-06-18"));

    let shutdown_error = runtime
        .shutdown()
        .await
        .expect_err("shutdown 重试 DELETE 失败时不得虚报成功");
    assert_eq!(shutdown_error.code(), "mcp_shutdown_failed");
    assert_eq!(
        server
            .request_headers
            .lock()
            .expect("shutdown cleanup 请求记录锁必须可用")
            .iter()
            .filter(|request| request.method == "DELETE")
            .count(),
        2,
        "shutdown 必须恰好再次接管同一个 pending session"
    );
    assert_eq!(runtime.http_session_count(), 1);
    assert_eq!(
        runtime.cleanup_ownership_for_test(),
        (1, 1),
        "shutdown 失败后 pending handle 必须继续由 shutdown owner 持有"
    );

    let repeated_error = runtime
        .shutdown()
        .await
        .expect_err("重复 shutdown 必须复用首次真实失败结果");
    assert_eq!(repeated_error.code(), "mcp_shutdown_failed");
    assert_eq!(
        server
            .request_headers
            .lock()
            .expect("重复 shutdown 请求记录锁必须可用")
            .iter()
            .filter(|request| request.method == "DELETE")
            .count(),
        2,
        "重复 shutdown 不得重复 DELETE 或丢失 handle"
    );
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn initial_handshake_failure_matrix_tracks_only_captured_sessions() {
    let cases = [
        (InitialHandshakeFailure::InitializeMalformed, true),
        (InitialHandshakeFailure::InitializeMissingResult, true),
        (InitialHandshakeFailure::InitializeMissingProtocol, true),
        (InitialHandshakeFailure::InitializeMissingSession, false),
        (InitialHandshakeFailure::InitializedFailure, true),
        (InitialHandshakeFailure::InitializedMalformed, true),
        (InitialHandshakeFailure::InitializedTimeout, true),
        (InitialHandshakeFailure::InitializedCancellation, true),
        (InitialHandshakeFailure::ToolsMalformed, true),
        (InitialHandshakeFailure::ToolsOversized, true),
        (InitialHandshakeFailure::ToolsTimeout, true),
    ];

    for (failure, has_captured_session) in cases {
        let server = MockMcpServer::start(ServerPlan::InitialHandshakeFailure(failure));
        let runtime = McpRuntime::new_with_timeout_for_test(
            approved_config("initial", server.url()),
            BTreeSet::from(["initial__ok".to_owned()]),
            Duration::from_millis(75),
        )
        .await
        .expect("初始 handshake 失败应返回 error catalog runtime");
        let catalog = runtime
            .catalog()
            .await
            .expect("初始 handshake 失败后的 catalog 应可读取");
        assert_eq!(catalog.servers[0].session.status, "error");

        let expected_sessions = usize::from(has_captured_session);
        assert_eq!(
            runtime.http_session_count(),
            expected_sessions,
            "failure={failure:?} 只有真实 captured header 才能进入 cleanup registry"
        );
        assert_eq!(
            runtime.cleanup_ownership_for_test(),
            (expected_sessions, 0),
            "failure={failure:?} 不得伪造无 header session"
        );
    }
}

#[tokio::test]
async fn legal_tool_name_over_64_bytes_survives_catalog_intersection_and_call() {
    let tool_name = format!("tool_{}", "x".repeat(64));
    let server = MockMcpServer::start(ServerPlan::Tools(vec![tool_name.clone()]));
    let qualified_name = format!("server__{tool_name}");
    let runtime = McpRuntime::new(
        approved_config("server", server.url()),
        BTreeSet::from([qualified_name.clone()]),
    )
    .await
    .expect("超过 64 bytes 的合法 tool 名称不应被 server 限制误拒绝");
    let catalog = runtime.catalog().await.expect("catalog 应可读取");
    assert_eq!(catalog.servers[0].session.tools[0].name, tool_name);
    assert_eq!(runtime.model_visible_tools(), vec![qualified_name.clone()]);
    assert_eq!(
        runtime.model_tool_schemas()[0]["function"]["name"],
        qualified_name
    );
    assert_eq!(
        runtime
            .call(&qualified_name, json!({}))
            .await
            .expect("超过 64 bytes 的合法 tool 必须可调用")
            .text_content(),
        Some("ok")
    );
}

#[tokio::test]
async fn catalog_uses_nested_host_wire_shape_and_qualified_tool_names() {
    let server = MockMcpServer::start(ServerPlan::Tools(vec!["search".to_owned()]));
    let runtime = runtime_with_server("approved", &server, ["approved__search"]).await;
    let catalog = runtime.catalog().await.expect("catalog 应可读取");
    let wire = catalog.to_wire();
    assert_eq!(wire["result"]["servers"][0]["name"], "approved");
    assert_eq!(wire["result"]["servers"][0]["session"]["status"], "ready");
    assert_eq!(
        wire["result"]["servers"][0]["session"]["tools"][0]["name"],
        "search"
    );
    assert_eq!(runtime.model_visible_tools(), vec!["approved__search"]);
}

#[test]
fn literal_loopback_validator_rejects_non_contract_urls() {
    let accepted = ["http://127.0.0.1:43123/mcp", "http://[::1]:43123/mcp"];
    for url in accepted {
        assert!(is_literal_loopback_http_url(url), "应接受 {url}");
    }
    let rejected = [
        "https://127.0.0.1:43123/mcp",
        "http://localhost:43123/mcp",
        "http://127.0.0.2:43123/mcp",
        "http://127.0.0.1/mcp",
        "http://127.0.0.1:43123/mcp?secret=1",
        "http://127.0.0.1:43123/mcp#fragment",
        "http://user:pass@127.0.0.1:43123/mcp",
        "http://127.0.0.1:43123/mcp with-space",
    ];
    for url in rejected {
        assert!(!is_literal_loopback_http_url(url), "不得接受 {url}");
    }
}

#[tokio::test]
async fn redirect_is_rejected_without_following() {
    let target = MockMcpServer::start(ServerPlan::Tools(vec!["unexpected".to_owned()]));
    let server = MockMcpServer::start(ServerPlan::Redirect(target.url()));
    let runtime = McpRuntime::new(
        approved_config("redirect", server.url()),
        BTreeSet::from(["redirect__tool".to_owned()]),
    )
    .await
    .expect("HTTP failure 应转换为非 ready server，而不是 panic");
    let catalog = runtime
        .catalog()
        .await
        .expect("失败 server 仍应返回 catalog");
    assert_eq!(catalog.servers[0].session.status, "error");
    assert_eq!(runtime.http_session_count(), 0);
    assert!(
        !runtime
            .model_visible_tools()
            .iter()
            .any(|name| name == "redirect__tool")
    );
    assert!(
        target
            .requests
            .lock()
            .map(|requests| requests.is_empty())
            .unwrap_or(false),
        "单跳 redirect 目标不得收到第二次请求"
    );
}

#[tokio::test]
async fn no_proxy_client_reaches_loopback_even_when_proxy_environment_is_set() {
    static ENV_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    let _guard = ENV_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    let keys = [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ];
    let previous = keys
        .iter()
        .map(|key| (*key, std::env::var(key).ok()))
        .collect::<Vec<_>>();
    for key in keys {
        // Rust 2024 将进程环境变更标记为 unsafe；测试用全局锁将该窗口收窄。
        unsafe { std::env::set_var(key, "http://127.0.0.1:9") };
    }

    let server = MockMcpServer::start(ServerPlan::Tools(vec!["ok".to_owned()]));
    let runtime = runtime_with_server("proxy", &server, ["proxy__ok"]).await;
    assert_eq!(runtime.http_session_count(), 1);
    assert_eq!(runtime.model_visible_tools(), vec!["proxy__ok"]);

    for (key, value) in previous {
        if let Some(value) = value {
            unsafe { std::env::set_var(key, value) };
        } else {
            unsafe { std::env::remove_var(key) };
        }
    }
}

#[tokio::test]
async fn call_returns_tool_result_and_tool_level_error() {
    let server = MockMcpServer::start(ServerPlan::CallError);
    let runtime = runtime_with_server("server", &server, ["server__ok", "server__bad"]).await;

    let success = runtime
        .call("server__ok", json!({}))
        .await
        .expect("成功工具调用应返回结果");
    assert!(!success.is_error);
    assert_eq!(success.text_content(), Some("ok"));

    let error = runtime
        .call("server__bad", json!({}))
        .await
        .expect("MCP tool-level error 仍是合法 call result");
    assert!(error.is_error);
    assert_eq!(error.text_content(), Some("tool failed"));
}

#[tokio::test]
async fn output_over_one_mib_fails_closed_without_returning_payload() {
    let server = MockMcpServer::start(ServerPlan::LargeResult(1_048_577));
    let runtime = runtime_with_server("large", &server, ["large__ok"]).await;
    let error = runtime
        .call("large__ok", json!({}))
        .await
        .expect_err("超过 1 MiB 的 MCP 结果必须拒绝");
    assert_eq!(error.code(), "mcp_output_too_large");
}

#[tokio::test]
async fn server_name_boundary_matches_contract() {
    let server = MockMcpServer::start(ServerPlan::Tools(vec!["ok".to_owned()]));
    let valid_name = format!("a{}", "x".repeat(63));
    let runtime = McpRuntime::new(
        approved_config(&valid_name, server.url()),
        BTreeSet::from([format!("{valid_name}__ok")]),
    )
    .await
    .expect("64 bytes 的 server name 必须接受");
    assert_eq!(
        runtime.model_visible_tools(),
        vec![format!("{valid_name}__ok")]
    );

    // 只提供合法的 expected_tools 集合，隔离 server name 校验错误分类。
    let invalid_name = format!("a{}", "x".repeat(64));
    let error = McpRuntime::new(
        approved_config(&invalid_name, server.url()),
        BTreeSet::new(),
    )
    .await
    .expect_err("超过 64 bytes 的 server name 必须拒绝");
    assert_eq!(error.code(), "mcp_server_name_invalid");

    let error = McpRuntime::new(
        approved_config("server__name", server.url()),
        BTreeSet::new(),
    )
    .await
    .expect_err("包含 qualified 分隔符的 server name 必须拒绝");
    assert_eq!(error.code(), "mcp_server_name_invalid");
}

#[tokio::test]
async fn call_timeout_uses_short_fixture_and_bounds_full_operation() {
    let server = MockMcpServer::start(ServerPlan::DelayCall);
    let runtime = McpRuntime::new_with_timeout_for_test(
        approved_config("short", server.url()),
        BTreeSet::from(["short__ok".to_owned()]),
        Duration::from_millis(50),
    )
    .await
    .expect("短 timeout fixture 初始化必须成功");
    let started = Instant::now();
    let error = runtime
        .call("short__ok", json!({}))
        .await
        .expect_err("短 timeout 内未完成的 call 必须失败");
    assert_eq!(error.code(), "mcp_call_timeout");
    assert!(
        started.elapsed() < Duration::from_millis(200),
        "完整 call deadline 不得因 request handle 阶段额外等待"
    );
}

#[tokio::test]
async fn content_length_and_chunked_bodies_are_limited_before_full_decode() {
    for plan in [
        ServerPlan::LargeContentLength(2 * 1024 * 1024),
        ServerPlan::LargeChunked(2 * 1024 * 1024),
    ] {
        let server = MockMcpServer::start(plan);
        let runtime = runtime_with_server("large", &server, ["large__ok"]).await;
        let error = runtime
            .call("large__ok", json!({}))
            .await
            .expect_err("超出 body cap 的 MCP body 必须在解码前拒绝");
        assert_eq!(error.code(), "mcp_output_too_large");
        assert!(
            server.response_bytes_sent() < 1_200_000,
            "body limiter 不得完整接收 2 MiB body，实际已接收 {} bytes",
            server.response_bytes_sent()
        );
    }
}

#[tokio::test]
async fn response_body_over_exact_one_mib_is_rejected_before_decode() {
    let server = MockMcpServer::start(ServerPlan::LargeContentLength(MAX_MCP_OUTPUT_BYTES + 1));
    let runtime = runtime_with_server("large", &server, ["large__ok"]).await;
    let error = runtime
        .call("large__ok", json!({}))
        .await
        .expect_err("超过严格 1 MiB body cap 的响应必须拒绝");
    assert_eq!(error.code(), "mcp_output_too_large");
    assert!(
        server.response_bytes_sent() < MAX_MCP_OUTPUT_BYTES,
        "严格 body cap 必须在读取完整响应前拒绝，实际已发送 {} bytes",
        server.response_bytes_sent()
    );
}

#[tokio::test]
async fn sse_notification_is_ignored_until_matching_response() {
    let server = MockMcpServer::start(ServerPlan::SseNotificationThenResponse);
    let runtime = runtime_with_server("sse", &server, ["sse__ok"]).await;
    let result = runtime
        .call("sse__ok", json!({}))
        .await
        .expect("SSE notification 不得阻断后续匹配 response");
    assert!(!result.is_error);
    assert_eq!(result.text_content(), Some("ok"));
    runtime.shutdown().await.expect("SSE session 必须可关闭");
}

#[tokio::test]
async fn sse_server_request_fails_closed_without_executing_it() {
    let server = MockMcpServer::start(ServerPlan::SseServerRequestThenResponse);
    let runtime = runtime_with_server("sse", &server, ["sse__ok"]).await;
    let error = runtime
        .call("sse__ok", json!({}))
        .await
        .expect_err("不支持的 SSE server request 必须 fail-closed");
    assert_eq!(error.code(), "mcp_call_failed");
    runtime.shutdown().await.expect("SSE session 必须可关闭");
}

#[tokio::test]
async fn sse_response_with_unmatched_id_fails_closed() {
    let server = MockMcpServer::start(ServerPlan::SseWrongResponseId);
    let runtime = runtime_with_server("sse", &server, ["sse__ok"]).await;
    let error = runtime
        .call("sse__ok", json!({}))
        .await
        .expect_err("SSE response id 不匹配时必须 fail-closed");
    assert_eq!(error.code(), "mcp_call_failed");
    runtime.shutdown().await.expect("SSE session 必须可关闭");
}

#[tokio::test]
async fn sse_server_request_after_matching_response_fails_closed() {
    let server = MockMcpServer::start(ServerPlan::SseResponseThenServerRequest);
    let runtime = runtime_with_server("sse", &server, ["sse__ok"]).await;
    let error = runtime
        .call("sse__ok", json!({}))
        .await
        .expect_err("匹配 response 后的 SSE server request 也必须 fail-closed");
    assert_eq!(error.code(), "mcp_call_failed");
    runtime.shutdown().await.expect("SSE session 必须可关闭");
}

#[tokio::test]
async fn truncated_sse_matching_response_at_eof_fails_closed() {
    let server = MockMcpServer::start(ServerPlan::SseTruncatedResponse);
    let runtime = runtime_with_server("sse", &server, ["sse__ok"]).await;
    let error = runtime
        .call("sse__ok", json!({}))
        .await
        .expect_err("EOF 前未消费空行的 SSE response 必须 fail-closed");
    assert_eq!(error.code(), "mcp_call_failed");
    runtime
        .shutdown()
        .await
        .expect("截断 SSE session 必须可关闭");
}

#[tokio::test]
async fn json_notification_body_cannot_be_used_as_call_response() {
    let server = MockMcpServer::start(ServerPlan::JsonNotificationCall);
    let runtime = runtime_with_server("json", &server, ["json__ok"]).await;
    let error = runtime
        .call("json__ok", json!({}))
        .await
        .expect_err("JSON notification 不得被当作 tools/call response 成功返回");
    assert_eq!(error.code(), "mcp_call_failed");
    runtime.shutdown().await.expect("JSON session 必须可关闭");
}

#[tokio::test]
async fn empty_content_length_204_notification_response_is_accepted() {
    let server = MockMcpServer::start(ServerPlan::EmptyInitializedNoContentLength);
    let runtime = runtime_with_server("empty", &server, ["empty__ok"]).await;
    let catalog = runtime
        .catalog()
        .await
        .expect("Content-Length: 0 的 204 后 catalog 应可读取");
    assert_eq!(catalog.servers[0].session.status, "ready");
    runtime
        .shutdown()
        .await
        .expect("正常 Content-Length: 0 的 204 cleanup response 必须允许");
}

#[tokio::test]
async fn nonzero_content_length_204_json_rpc_response_fails_closed() {
    let server = MockMcpServer::start(ServerPlan::NonZeroNoContentInitialized);
    let runtime = runtime_with_server("status", &server, ["status__ok"]).await;
    let catalog = runtime
        .catalog()
        .await
        .expect("204 header 错误后 catalog 应可读取");
    assert_eq!(catalog.servers[0].session.status, "error");
    assert_eq!(runtime.http_session_count(), 0);
}

#[tokio::test]
async fn nonzero_content_length_204_delete_fails_closed() {
    let server = MockMcpServer::start(ServerPlan::NonZeroNoContentDelete);
    let runtime = runtime_with_server("delete", &server, ["delete__ok"]).await;
    let error = runtime
        .shutdown()
        .await
        .expect_err("DELETE 的 204 Content-Length: 1 必须 fail-closed");
    assert_eq!(error.code(), "mcp_shutdown_failed");
    assert_eq!(runtime.http_session_count(), 1);
}

#[tokio::test]
async fn chunked_sse_body_over_one_mib_is_rejected_after_matching_frame() {
    let server = MockMcpServer::start(ServerPlan::SseOversizedChunkedBody);
    let runtime = runtime_with_server("sse", &server, ["sse__ok"]).await;
    let error = runtime
        .call("sse__ok", json!({}))
        .await
        .expect_err("匹配 frame 后仍有超大 chunked SSE body 时必须 fail-closed");
    assert_eq!(error.code(), "mcp_output_too_large");
    runtime.shutdown().await.expect("SSE session 必须可关闭");
}

#[tokio::test]
async fn exact_one_mib_response_body_is_accepted() {
    let server = MockMcpServer::start(ServerPlan::ExactResponseBody);
    let runtime = runtime_with_server("exact", &server, ["exact__ok"]).await;
    let result = runtime
        .call("exact__ok", json!({}))
        .await
        .expect("恰好 1 MiB 的合法 JSON-RPC body 必须接受");
    assert_eq!(result.text_content(), Some("ok"));
    runtime
        .shutdown()
        .await
        .expect("精确 body session 必须可关闭");
}

#[tokio::test]
async fn accepted_and_no_content_call_bodies_are_capped_before_status_shortcut() {
    for plan in [
        ServerPlan::LargeAcceptedCallBody,
        ServerPlan::LargeNoContentCallBody,
    ] {
        let server = MockMcpServer::start(plan);
        let runtime = runtime_with_server("status", &server, ["status__ok"]).await;
        let error = runtime
            .call("status__ok", json!({}))
            .await
            .expect_err("202/204 响应 body 超限必须先触发 body cap");
        assert_eq!(error.code(), "mcp_output_too_large");
        runtime.shutdown().await.expect("status session 必须可关闭");
    }
}

#[tokio::test]
async fn chunked_shortcut_bodies_are_capped_before_status_shortcut() {
    for (label, plan) in [
        ("202 Accepted", ServerPlan::LargeAcceptedChunkedCallBody),
        ("204 No Content", ServerPlan::LargeNoContentChunkedCallBody),
    ] {
        let server = MockMcpServer::start(plan);
        let runtime = runtime_with_server("status", &server, ["status__ok"]).await;
        let error = runtime
            .call("status__ok", json!({}))
            .await
            .expect_err("无 Content-Length 的 202/204 body 超限必须 fail-closed");
        assert_eq!(
            error.code(),
            "mcp_output_too_large",
            "{label} chunked body 必须命中严格 body cap"
        );
        runtime
            .shutdown()
            .await
            .expect("shortcut body 测试 session 必须可关闭");
    }
}

#[tokio::test]
async fn oversized_error_body_is_capped_before_non_success_mapping() {
    let server = MockMcpServer::start(ServerPlan::LargeErrorCallBody);
    let runtime = runtime_with_server("error", &server, ["error__ok"]).await;
    let error = runtime
        .call("error__ok", json!({}))
        .await
        .expect_err("超大的非成功响应 body 必须先触发 body cap");
    assert_eq!(error.code(), "mcp_output_too_large");
    runtime
        .shutdown()
        .await
        .expect("错误 body session 必须可关闭");
}

#[tokio::test]
async fn chunked_error_body_is_capped_before_non_success_mapping() {
    let server = MockMcpServer::start(ServerPlan::LargeErrorChunkedCallBody);
    let runtime = runtime_with_server("error", &server, ["error__ok"]).await;
    let error = runtime
        .call("error__ok", json!({}))
        .await
        .expect_err("无 Content-Length 的超大错误 body 必须先触发 body cap");
    assert_eq!(error.code(), "mcp_output_too_large");
    runtime
        .shutdown()
        .await
        .expect("chunked 错误 body session 必须可关闭");
}

#[tokio::test]
async fn oversized_success_and_method_not_allowed_delete_bodies_fail_closed() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init();
    for plan in [
        ServerPlan::LargeDeleteSuccessBody,
        ServerPlan::LargeDeleteMethodNotAllowedBody,
        ServerPlan::LargeDeleteNoContentChunkedBody,
    ] {
        let server = MockMcpServer::start(plan);
        let runtime = runtime_with_server("delete", &server, ["delete__ok"]).await;
        let error = runtime
            .shutdown()
            .await
            .expect_err("DELETE 响应 body 超限必须使 shutdown 失败");
        assert_eq!(error.code(), "mcp_shutdown_failed");
        assert_eq!(runtime.http_session_count(), 1);
        assert!(
            server
                .request_headers
                .lock()
                .expect("DELETE 请求记录锁必须可用")
                .iter()
                .any(|request| request.method == "DELETE"),
            "DELETE body cap 回归必须真实发出 cleanup 请求"
        );
    }
}

#[tokio::test]
async fn concurrent_shutdown_waits_for_and_shares_first_cleanup_result() {
    let server = MockMcpServer::start(ServerPlan::DelayShutdown);
    let runtime = Arc::new(
        McpRuntime::new_with_timeout_for_test(
            approved_config("shutdown", server.url()),
            BTreeSet::from(["shutdown__ok".to_owned()]),
            Duration::from_millis(100),
        )
        .await
        .expect("shutdown fixture 初始化必须成功"),
    );
    let first_runtime = Arc::clone(&runtime);
    let first = tokio::spawn(async move { first_runtime.shutdown().await });
    server.wait_for_method("DELETE").await;
    let second_runtime = Arc::clone(&runtime);
    let mut second = tokio::spawn(async move { second_runtime.shutdown().await });
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut second)
            .await
            .is_err(),
        "并发 shutdown 不得在首个 cleanup 未完成时虚报成功"
    );
    let first_result = first.await.expect("首个 shutdown task 不得 panic");
    let second_result = second.await.expect("第二个 shutdown task 不得 panic");
    assert_eq!(first_result, second_result);
}

#[tokio::test]
async fn initialize_timeout_marks_server_not_ready() {
    assert_eq!(MCP_INITIALIZE_TIMEOUT, Duration::from_secs(20));
    let server = MockMcpServer::start(ServerPlan::DelayInitialize);
    let runtime = McpRuntime::new_with_timeout_for_test(
        approved_config("slow", server.url()),
        BTreeSet::from(["slow__tool".to_owned()]),
        Duration::from_millis(50),
    )
    .await
    .expect("初始化超时应保留非 ready catalog");
    let catalog = runtime.catalog().await.expect("非 ready catalog 应可读取");
    assert_eq!(catalog.servers[0].session.status, "error");
    assert_eq!(runtime.http_session_count(), 0);
}

#[tokio::test]
async fn active_call_can_be_cancelled_and_shutdown_is_bounded_and_idempotent() {
    assert_eq!(MCP_CALL_TIMEOUT, Duration::from_secs(20));
    let server = MockMcpServer::start(ServerPlan::DelayCall);
    let runtime = Arc::new(
        McpRuntime::new_with_timeout_for_test(
            approved_config("server", server.url()),
            BTreeSet::from(["server__ok".to_owned()]),
            Duration::from_millis(100),
        )
        .await
        .expect("延迟 call server 初始化应成功"),
    );

    let cancellation = McpCancellationToken::new();
    let call_runtime = Arc::clone(&runtime);
    let call_cancellation = cancellation.clone();
    let call_task = tokio::spawn(async move {
        call_runtime
            .call_with_cancellation("server__ok", json!({}), call_cancellation)
            .await
    });
    server.wait_for_method("tools/call").await;
    cancellation.cancel();
    let call_error = tokio::time::timeout(TEST_TIMEOUT, call_task)
        .await
        .expect("取消 call 必须有界返回")
        .expect("取消 call task 不得 panic")
        .expect_err("取消 call 必须返回稳定错误");
    assert_eq!(call_error.code(), "mcp_call_cancelled");

    let call_runtime = Arc::clone(&runtime);
    let active_call = tokio::spawn(async move { call_runtime.call("server__ok", json!({})).await });
    server.wait_for_call_count(2).await;
    tokio::time::timeout(TEST_TIMEOUT, runtime.shutdown())
        .await
        .expect("shutdown 必须有界")
        .expect("shutdown 不得失败");
    assert_eq!(runtime.http_session_count(), 0);
    assert!(
        tokio::time::timeout(TEST_TIMEOUT, active_call)
            .await
            .expect("shutdown 必须取消 active call")
            .expect("active call task 不得 panic")
            .is_err()
    );
    runtime.shutdown().await.expect("shutdown 必须幂等");
    let shutdown_error = runtime
        .call("server__ok", json!({}))
        .await
        .expect_err("shutdown 后不得接收新 call");
    assert_eq!(shutdown_error.code(), "mcp_runtime_shutdown");
}

#[test]
fn mcp_client_has_no_stdio_spawn_or_sensitive_process_helper() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("mcp_client.rs");
    let source = fs::read_to_string(path).expect("实现文件必须存在");
    for forbidden in [
        "Command::new",
        "std::process",
        "tokio::process",
        "spawned_server_count",
    ] {
        assert!(!source.contains(forbidden), "实现不得包含 {forbidden}");
    }
}
