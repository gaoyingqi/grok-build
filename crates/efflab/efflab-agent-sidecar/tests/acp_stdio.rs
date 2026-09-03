//! MinimalAgent ACP stdio 黑盒合同测试。
//!
//! 这些测试只通过真实 sidecar 二进制和 JSON-RPC stdio 验证 Task 13；启动配置、私有
//! home 与 Windows fail-closed 门禁由 `startup.rs` 继续覆盖。测试 fixture 不承载密钥。

mod common;

#[cfg(unix)]
mod task13 {
    use std::collections::{BTreeMap, VecDeque};
    use std::fs;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::path::{Path, PathBuf};
    use std::process::{Child, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc::{self, Receiver},
    };
    use std::thread::{self, JoinHandle};
    use std::time::{Duration, Instant};

    use serde_json::json;

    use super::common::acp_client::{AcpClient, AcpError};
    use efflab_agent_contract::{
        ApprovedMcpConfig, LoopbackModelSpec, McpServerSpec, RuntimeConfigV1,
        render_runtime_config_v1,
    };
    use serde_json::Value;
    use tempfile::TempDir;

    const SIDECAR_BIN: &str = env!("CARGO_BIN_EXE_efflab-agent-sidecar");
    const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
    const EXIT_TIMEOUT: Duration = Duration::from_secs(15);

    /// 为黑盒进程创建唯一的 v1 runtime config，不写入任何用户凭据。
    struct Fixture {
        _temporary: TempDir,
        session_cwd: PathBuf,
        home: PathBuf,
        runtime_config: PathBuf,
        test_seam: Option<PathBuf>,
    }

    impl Fixture {
        fn new() -> Self {
            Self::with_model_url("http://127.0.0.1:43123/v1")
        }

        /// 为 turn loop 黑盒测试注入受控 loopback 模型地址。
        fn with_model_url(model_url: impl Into<String>) -> Self {
            Self::with_model_url_and_expected_tools(model_url, std::iter::empty::<String>())
        }

        /// 为需要工具回合的黑盒测试注入模型地址和 Host 期待工具集合。
        fn with_model_url_and_expected_tools(
            model_url: impl Into<String>,
            expected_tools: impl IntoIterator<Item = String>,
        ) -> Self {
            Self::with_model_url_expected_tools_and_mcp(
                model_url,
                expected_tools,
                ApprovedMcpConfig::default(),
            )
        }

        /// 为真实 MCP ACP/模型回合注入已审核 HTTP MCP 配置。
        fn with_model_url_expected_tools_and_mcp(
            model_url: impl Into<String>,
            expected_tools: impl IntoIterator<Item = String>,
            approved_mcp: ApprovedMcpConfig,
        ) -> Self {
            let temporary = tempfile::tempdir().expect("创建黑盒测试临时目录");
            let session_cwd = temporary.path().join("session");
            let home = temporary.path().join("home");
            fs::create_dir(&session_cwd).expect("创建 session cwd");
            set_mode(&session_cwd, 0o700);
            fs::create_dir(&home).expect("创建 sidecar home");
            set_mode(&home, 0o700);

            let config = RuntimeConfigV1 {
                schema_version: 1,
                runtime_revision: String::new(),
                session_store_version: 1,
                session_cwd: session_cwd
                    .to_str()
                    .expect("测试 session cwd 必须是 UTF-8")
                    .to_owned(),
                model: LoopbackModelSpec {
                    model_id: "efflab-test-model".to_owned(),
                    base_url: model_url.into(),
                    backend: "chat_completions".to_owned(),
                    token_env: "EFFLAB_L3B_BIND".to_owned(),
                },
                approved_mcp,
                expected_tools: expected_tools.into_iter().collect(),
                system_prompt: String::new(),
            };
            let rendered = render_runtime_config_v1(&config).expect("生成 v1 runtime config");
            let runtime_config = home.join("runtime-config.v1.toml");
            fs::write(&runtime_config, rendered).expect("写入 v1 runtime config");
            set_mode(&runtime_config, 0o600);

            Self {
                _temporary: temporary,
                session_cwd,
                home,
                runtime_config,
                test_seam: None,
            }
        }

        /// 为需要精确控制 admission/cancel/执行点的测试启用 debug-only seam。
        fn with_test_seam(mut self) -> Self {
            let seam = self._temporary.path().join("test-seam");
            fs::create_dir(&seam).expect("创建测试 seam 目录");
            self.test_seam = Some(seam);
            self
        }

        /// 构造清空父环境后的 sidecar 命令，避免代理/遥测变量进入子进程。
        fn command(&self) -> Command {
            let mut command = Command::new(SIDECAR_BIN);
            command
                .arg("--runtime-config")
                .arg(&self.runtime_config)
                .arg("--home")
                .arg(&self.home)
                .arg("--session-cwd")
                .arg(&self.session_cwd)
                .current_dir(&self.session_cwd)
                .env_clear()
                .env("PATH", "/usr/bin:/bin")
                .env("EFFLAB_L3B_BIND", "efflab-test-l3b-bind")
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            if let Some(seam) = &self.test_seam {
                command.arg("--test-seam-dir").arg(seam);
            }
            command
        }

        /// 启动 sidecar，并把 stderr 放到独立 drain 线程，防止日志反压 ACP。
        #[allow(clippy::disallowed_methods)]
        fn spawn(&self) -> (TestProcess, AcpClient) {
            let mut child = self.command().spawn().expect("启动 sidecar 黑盒进程");
            let stdin = child.stdin.take().expect("sidecar stdin pipe");
            let stdout = child.stdout.take().expect("sidecar stdout pipe");
            let mut stderr = child.stderr.take().expect("sidecar stderr pipe");
            let (stderr_tx, stderr_rx) = mpsc::channel();
            let stderr_thread = std::thread::spawn(move || {
                let mut bytes = Vec::new();
                let _ = stderr.read_to_end(&mut bytes);
                let _ = stderr_tx.send(bytes);
            });
            let process = TestProcess {
                child,
                stderr_rx,
                stderr_thread: Some(stderr_thread),
                stderr_cache: None,
            };
            let client = AcpClient::new(stdin, stdout);
            (process, client)
        }

        /// 启动一个同步线客户端，用于在 prompt 未读回时发送取消通知。
        #[allow(clippy::disallowed_methods)]
        fn spawn_raw(&self) -> (TestProcess, RawClient) {
            let mut child = self.command().spawn().expect("启动 sidecar raw 黑盒进程");
            let stdin = child.stdin.take().expect("sidecar stdin pipe");
            let stdout = child.stdout.take().expect("sidecar stdout pipe");
            let mut stderr = child.stderr.take().expect("sidecar stderr pipe");
            let (stderr_tx, stderr_rx) = mpsc::channel();
            let stderr_thread = std::thread::spawn(move || {
                let mut bytes = Vec::new();
                let _ = stderr.read_to_end(&mut bytes);
                let _ = stderr_tx.send(bytes);
            });
            let process = TestProcess {
                child,
                stderr_rx,
                stderr_thread: Some(stderr_thread),
                stderr_cache: None,
            };
            let client = RawClient {
                stdin: Some(Arc::new(Mutex::new(stdin))),
                stdout: Some(BufReader::new(stdout)),
                next_id: 1,
                raw_lines: Vec::new(),
                pending_responses: BTreeMap::new(),
            };
            (process, client)
        }
    }

    /// 黑盒测试用的 loopback Chat Completions 响应计划。
    /// 可由测试显式释放、且在测试失败收尾时可被 stop 标志打断的响应闸门。
    #[derive(Clone)]
    struct ResponseGate {
        released: Arc<AtomicBool>,
    }

    impl ResponseGate {
        fn new() -> Self {
            Self {
                released: Arc::new(AtomicBool::new(false)),
            }
        }

        fn release(&self) {
            self.released.store(true, Ordering::Release);
        }

        fn wait(&self, stop: &AtomicBool) {
            while !self.released.load(Ordering::Acquire) && !stop.load(Ordering::Acquire) {
                thread::sleep(Duration::from_millis(2));
            }
        }
    }

    enum ScriptedResponse {
        Text(String),
        TextWithGate {
            text: String,
            gate: ResponseGate,
        },
        TextAndToolCall {
            text: String,
            name: String,
            arguments: String,
        },
        Blocked(String),
        ToolCall {
            name: String,
            arguments: String,
        },
        ReasoningAndText {
            reasoning: String,
            text: String,
        },
    }

    impl ScriptedResponse {
        /// 构造正常结束的单文本 SSE 响应。
        fn text(text: impl Into<String>) -> Self {
            Self::Text(text.into())
        }

        /// 构造由测试显式释放 Done 的 SSE 响应。
        fn text_with_gate(text: impl Into<String>, gate: ResponseGate) -> Self {
            Self::TextWithGate {
                text: text.into(),
                gate,
            }
        }

        /// 构造同一 Chat Completions 回合同时返回文本和工具调用的 SSE 响应。
        fn text_and_tool_call(
            text: impl Into<String>,
            name: impl Into<String>,
            arguments: impl Into<String>,
        ) -> Self {
            Self::TextAndToolCall {
                text: text.into(),
                name: name.into(),
                arguments: arguments.into(),
            }
        }

        /// 构造只发首个文本块、等待取消的 SSE 响应。
        fn blocked(text: impl Into<String>) -> Self {
            Self::Blocked(text.into())
        }

        /// 构造带单个 function tool call 的正常结束 SSE 响应。
        fn tool_call(name: impl Into<String>, arguments: impl Into<String>) -> Self {
            Self::ToolCall {
                name: name.into(),
                arguments: arguments.into(),
            }
        }

        /// 构造 MiMo 风格首帧 + 推理 + 正文的 SSE 响应。
        fn reasoning_and_text(reasoning: impl Into<String>, text: impl Into<String>) -> Self {
            Self::ReasoningAndText {
                reasoning: reasoning.into(),
                text: text.into(),
            }
        }
    }

    /// 为 sidecar 黑盒测试提供按请求顺序返回的最小 loopback HTTP server。
    struct ScriptedL3b {
        address: std::net::SocketAddr,
        base_url: String,
        request_count: Arc<AtomicUsize>,
        request_bodies: Arc<Mutex<Vec<Value>>>,
        stop: Arc<AtomicBool>,
        thread: Option<JoinHandle<()>>,
    }

    impl ScriptedL3b {
        /// 启动一个不继承代理、不记录敏感请求正文的测试模型端点。
        fn new(responses: impl IntoIterator<Item = ScriptedResponse>) -> Self {
            let listener =
                std::net::TcpListener::bind("127.0.0.1:0").expect("绑定测试 L3b loopback listener");
            listener
                .set_nonblocking(true)
                .expect("设置测试 L3b listener 为 nonblocking");
            let address = listener.local_addr().expect("读取测试 L3b listener 地址");
            let request_count = Arc::new(AtomicUsize::new(0));
            let request_count_for_thread = Arc::clone(&request_count);
            let request_bodies = Arc::new(Mutex::new(Vec::new()));
            let request_bodies_for_thread = Arc::clone(&request_bodies);
            let stop = Arc::new(AtomicBool::new(false));
            let stop_for_thread = Arc::clone(&stop);
            let mut responses = responses.into_iter().collect::<VecDeque<_>>();
            let thread = thread::spawn(move || {
                while !stop_for_thread.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            // listener 为 stop 唤醒使用 nonblocking；accepted stream 必须显式恢复阻塞，等待完整 HTTP headers。
                            stream
                                .set_nonblocking(false)
                                .expect("设置测试 L3b accepted stream 为 blocking");
                            let response = responses.pop_front();
                            if let Some(response) = response {
                                serve_response(
                                    &mut stream,
                                    response,
                                    &request_count_for_thread,
                                    &request_bodies_for_thread,
                                    &stop_for_thread,
                                );
                            }
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(5));
                        }
                        Err(_) => break,
                    }
                }
            });

            Self {
                address,
                base_url: format!("http://{address}/v1"),
                request_count,
                request_bodies,
                stop,
                thread: Some(thread),
            }
        }

        /// 返回符合 RuntimeConfigV1 的模型基础 URL。
        fn base_url(&self) -> &str {
            &self.base_url
        }

        /// 等待指定数量的模型请求，避免测试依赖固定 sleep 竞态。
        fn wait_for_requests(&self, expected: usize) {
            let deadline = Instant::now() + EXIT_TIMEOUT;
            while self.request_count.load(Ordering::Acquire) < expected {
                assert!(
                    Instant::now() < deadline,
                    "测试 L3b 未收到预期请求数: expected={expected}, actual={}",
                    self.request_count.load(Ordering::Acquire)
                );
                thread::sleep(Duration::from_millis(5));
            }
        }

        /// 返回 sidecar 已发出的模型请求次数。
        fn model_call_count(&self) -> usize {
            self.request_count.load(Ordering::Acquire)
        }

        /// 返回测试模型收到的请求快照，供 transcript 恢复断言使用。
        fn request_bodies(&self) -> Vec<Value> {
            self.request_bodies
                .lock()
                .map(|bodies| bodies.clone())
                .unwrap_or_default()
        }
    }

    impl Drop for ScriptedL3b {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Release);
            let _ = std::net::TcpStream::connect(self.address);
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
    }

    /// 真实 sidecar MCP 黑盒使用的最小 loopback HTTP server。
    struct ScriptedMcp {
        address: std::net::SocketAddr,
        requests: Arc<Mutex<Vec<String>>>,
        call_count: Arc<AtomicUsize>,
        stop: Arc<AtomicBool>,
        thread: Option<JoinHandle<()>>,
    }

    impl ScriptedMcp {
        /// 启动支持 initialize、tools/list、tools/call 和 DELETE 的 MCP fixture。
        fn new(tools: impl IntoIterator<Item = impl Into<String>>, block_calls: bool) -> Self {
            let listener =
                std::net::TcpListener::bind("127.0.0.1:0").expect("绑定测试 MCP loopback listener");
            listener
                .set_nonblocking(true)
                .expect("设置测试 MCP listener 为 nonblocking");
            let address = listener.local_addr().expect("读取测试 MCP 地址");
            let tools = tools.into_iter().map(Into::into).collect::<Vec<_>>();
            let requests = Arc::new(Mutex::new(Vec::new()));
            let requests_for_thread = Arc::clone(&requests);
            let call_count = Arc::new(AtomicUsize::new(0));
            let call_count_for_thread = Arc::clone(&call_count);
            let stop = Arc::new(AtomicBool::new(false));
            let stop_for_thread = Arc::clone(&stop);
            let thread = thread::spawn(move || {
                while !stop_for_thread.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            stream
                                .set_nonblocking(false)
                                .expect("设置测试 MCP accepted stream 为 blocking");
                            if let Some((method, body)) = read_mcp_request(&mut stream) {
                                if let Ok(mut requests) = requests_for_thread.lock() {
                                    requests.push(method.clone());
                                }
                                match method.as_str() {
                                    "initialize" => write_mcp_json(
                                        &mut stream,
                                        json_rpc_result(
                                            body.get("id").cloned().unwrap_or(Value::Null),
                                            json!({
                                                "protocolVersion": "2025-06-18",
                                                "capabilities": {"tools": {}},
                                                "serverInfo": {"name": "scripted-mcp", "version": "1"}
                                            }),
                                        ),
                                        true,
                                    ),
                                    "notifications/initialized" => {
                                        write_mcp_response(&mut stream, 202, b"", false)
                                    }
                                    "tools/list" => {
                                        let tool_values = tools
                                            .iter()
                                            .map(|name| {
                                                json!({
                                                    "name": name,
                                                    "description": "scripted MCP tool",
                                                    "inputSchema": {"type": "object", "properties": {}, "additionalProperties": false}
                                                })
                                            })
                                            .collect::<Vec<_>>();
                                        write_mcp_json(
                                            &mut stream,
                                            json_rpc_result(
                                                body.get("id").cloned().unwrap_or(Value::Null),
                                                json!({"tools": tool_values}),
                                            ),
                                            true,
                                        );
                                    }
                                    "tools/call" => {
                                        call_count_for_thread.fetch_add(1, Ordering::AcqRel);
                                        if block_calls {
                                            wait_for_client_disconnect(&stream, &stop_for_thread);
                                        } else {
                                            write_mcp_json(
                                                &mut stream,
                                                json_rpc_result(
                                                    body.get("id").cloned().unwrap_or(Value::Null),
                                                    json!({"content": [{"type": "text", "text": "mcp ok"}], "isError": false}),
                                                ),
                                                true,
                                            );
                                        }
                                    }
                                    "DELETE" => write_mcp_response(&mut stream, 204, b"", false),
                                    _ => write_mcp_response(&mut stream, 400, b"", false),
                                }
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
                requests,
                call_count,
                stop,
                thread: Some(thread),
            }
        }

        /// 返回当前 MCP fixture 的 loopback URL。
        fn url(&self) -> String {
            format!("http://{}/mcp", self.address)
        }

        /// 等待真实 sidecar 发出 MCP tools/call。
        fn wait_for_call(&self) {
            let deadline = Instant::now() + EXIT_TIMEOUT;
            while self.call_count.load(Ordering::Acquire) == 0 {
                assert!(Instant::now() < deadline, "sidecar 未发出 MCP tools/call");
                thread::sleep(Duration::from_millis(5));
            }
        }

        /// 返回已观察到的 MCP HTTP 方法序列。
        fn methods(&self) -> Vec<String> {
            self.requests
                .lock()
                .map(|requests| requests.clone())
                .unwrap_or_default()
        }

        /// 返回 MCP tools/call 次数。
        fn call_count(&self) -> usize {
            self.call_count.load(Ordering::Acquire)
        }
    }

    impl Drop for ScriptedMcp {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Release);
            let _ = std::net::TcpStream::connect(self.address);
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
    }

    /// 读取测试 MCP 的 HTTP method、headers 和 JSON body。
    fn read_mcp_request(stream: &mut std::net::TcpStream) -> Option<(String, Value)> {
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
        if method == "DELETE" {
            return Some((method, Value::Null));
        }
        let content_length = headers.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })?;
        while bytes.len() < header_end + content_length {
            let read = stream.read(&mut buffer).ok()?;
            if read == 0 {
                return None;
            }
            bytes.extend_from_slice(&buffer[..read]);
        }
        let body = serde_json::from_slice::<Value>(&bytes[header_end..header_end + content_length])
            .ok()?;
        let method = body
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or(method.as_str())
            .to_owned();
        Some((method, body))
    }

    /// 在客户端关闭当前 HTTP 连接或测试 server 停止前阻塞，用于验证 EOF cancel。
    fn wait_for_client_disconnect(stream: &std::net::TcpStream, stop: &AtomicBool) {
        let _ = stream.set_read_timeout(Some(Duration::from_millis(20)));
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

    /// 生成 MCP JSON-RPC response。
    fn json_rpc_result(id: Value, result: Value) -> Value {
        json!({"jsonrpc": "2.0", "id": id, "result": result})
    }

    /// 写入带 session header 的 MCP JSON response。
    fn write_mcp_json(stream: &mut std::net::TcpStream, body: Value, session: bool) {
        let body = serde_json::to_vec(&body).expect("测试 MCP response 必须可序列化");
        write_mcp_response(stream, 200, &body, session);
    }

    /// 写入 MCP HTTP response；测试 fixture 不模拟重定向或代理。
    fn write_mcp_response(
        stream: &mut std::net::TcpStream,
        status: u16,
        body: &[u8],
        session: bool,
    ) {
        let mut response = format!(
            "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
            body.len()
        );
        if session {
            response.push_str("Mcp-Session-Id: scripted-session\r\n");
        }
        response.push_str("\r\n");
        let _ = stream.write_all(response.as_bytes());
        let _ = stream.write_all(body);
        let _ = stream.shutdown(std::net::Shutdown::Both);
    }

    /// 读取一个测试 HTTP 请求的 headers 和 JSON 正文；不记录无法解析的正文。
    fn consume_request(stream: &mut std::net::TcpStream) -> std::io::Result<Vec<u8>> {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 4096];
        let header_end = loop {
            let read = stream.read(&mut buffer)?;
            if read == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "测试 L3b 请求提前 EOF",
                ));
            }
            bytes.extend_from_slice(&buffer[..read]);
            if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break index + 4;
            }
            if bytes.len() > 64 * 1024 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "测试 L3b 请求 headers 超限",
                ));
            }
        };

        let content_length = String::from_utf8_lossy(&bytes[..header_end - 4])
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.trim()
                    .eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .unwrap_or(0);
        if content_length > 2 * 1024 * 1024 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "测试 L3b 请求正文超限",
            ));
        }
        while bytes.len() - header_end < content_length {
            let read = stream.read(&mut buffer)?;
            if read == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "测试 L3b 请求正文提前 EOF",
                ));
            }
            bytes.extend_from_slice(&buffer[..read]);
        }
        Ok(bytes[header_end..header_end + content_length].to_vec())
    }

    /// 按计划写回完整或阻塞的 Chat Completions SSE 响应。
    fn serve_response(
        stream: &mut std::net::TcpStream,
        response: ScriptedResponse,
        request_count: &AtomicUsize,
        request_bodies: &Mutex<Vec<Value>>,
        stop: &AtomicBool,
    ) {
        let request_body = match consume_request(stream) {
            Ok(body) => body,
            Err(_) => return,
        };
        if let Ok(value) = serde_json::from_slice::<Value>(&request_body)
            && let Ok(mut bodies) = request_bodies.lock()
        {
            bodies.push(value);
        }
        request_count.fetch_add(1, Ordering::AcqRel);
        let first = match &response {
            ScriptedResponse::Text(text)
            | ScriptedResponse::TextWithGate { text, .. }
            | ScriptedResponse::TextAndToolCall { text, .. }
            | ScriptedResponse::Blocked(text) => {
                let encoded = serde_json::to_string(text).expect("测试 SSE 文本必须可序列化");
                format!("data: {{\"choices\":[{{\"delta\":{{\"content\":{encoded}}}}}]}}\n\n")
            }
            ScriptedResponse::ToolCall { name, arguments } => {
                let encoded_name =
                    serde_json::to_string(name).expect("测试 tool name 必须可序列化");
                let encoded_arguments =
                    serde_json::to_string(arguments).expect("测试 tool arguments 必须可序列化");
                format!(
                    "data: {{\"choices\":[{{\"delta\":{{\"tool_calls\":[{{\"index\":0,\"id\":\"call-1\",\"type\":\"function\",\"function\":{{\"name\":{encoded_name},\"arguments\":{encoded_arguments}}}}}]}}}}]}}\n\n"
                )
            }
            ScriptedResponse::ReasoningAndText { .. } => {
                // 与 MiMo 首帧同构：空 content、null reasoning、null tool_calls。
                "data: {\"choices\":[{\"index\":0,\"finish_reason\":null,\"delta\":{\"role\":\"assistant\",\"content\":\"\",\"reasoning_content\":null,\"tool_calls\":null}}]}\n\n".to_owned()
            }
        };
        if stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: text/event-stream\r\n\r\n",
            )
            .and_then(|_| stream.write_all(first.as_bytes()))
            .and_then(|_| stream.flush())
            .is_err()
        {
            return;
        }

        match response {
            ScriptedResponse::Text(_) | ScriptedResponse::ToolCall { .. } => {
                let _ = stream.write_all(b"data: [DONE]\n\n");
                let _ = stream.flush();
            }
            ScriptedResponse::TextWithGate { gate, .. } => {
                gate.wait(stop);
                let _ = stream.write_all(b"data: [DONE]\n\n");
                let _ = stream.flush();
            }
            ScriptedResponse::TextAndToolCall {
                name, arguments, ..
            } => {
                let encoded_name =
                    serde_json::to_string(&name).expect("测试 tool name 必须可序列化");
                let encoded_arguments =
                    serde_json::to_string(&arguments).expect("测试 tool arguments 必须可序列化");
                let tool_delta = format!(
                    "data: {{\"choices\":[{{\"delta\":{{\"tool_calls\":[{{\"index\":0,\"id\":\"call-1\",\"type\":\"function\",\"function\":{{\"name\":{encoded_name},\"arguments\":{encoded_arguments}}}}}]}}}}]}}\n\n"
                );
                if stream
                    .write_all(tool_delta.as_bytes())
                    .and_then(|_| stream.flush())
                    .is_err()
                {
                    return;
                }
                let _ = stream.write_all(b"data: [DONE]\n\n");
                let _ = stream.flush();
            }
            ScriptedResponse::Blocked(_) => {
                while !stop.load(Ordering::Acquire) {
                    thread::sleep(Duration::from_millis(10));
                }
            }
            ScriptedResponse::ReasoningAndText { reasoning, text } => {
                let encoded_reasoning =
                    serde_json::to_string(&reasoning).expect("测试推理文本必须可序列化");
                let encoded_text = serde_json::to_string(&text).expect("测试 SSE 文本必须可序列化");
                let reasoning_frame = format!(
                    "data: {{\"choices\":[{{\"delta\":{{\"reasoning_content\":{encoded_reasoning}}}}}]}}\n\n"
                );
                let text_frame = format!(
                    "data: {{\"choices\":[{{\"delta\":{{\"content\":{encoded_text}}}}}]}}\n\n"
                );
                if stream
                    .write_all(reasoning_frame.as_bytes())
                    .and_then(|_| stream.write_all(text_frame.as_bytes()))
                    .and_then(|_| stream.write_all(b"data: [DONE]\n\n"))
                    .and_then(|_| stream.flush())
                    .is_err()
                {
                    return;
                }
            }
        }
    }

    /// 只收尾测试子进程，不向生产 sidecar 引入第二套 transport。
    #[allow(clippy::zombie_processes)]
    struct TestProcess {
        child: Child,
        stderr_rx: Receiver<Vec<u8>>,
        stderr_thread: Option<JoinHandle<()>>,
        stderr_cache: Option<String>,
    }

    impl TestProcess {
        /// 关闭 stdin 后等待正常 EOF；超时会先 kill，避免测试泄漏进程。
        fn finish(&mut self, client: &mut AcpClient, label: &str) {
            client.close_stdin();
            let status = self.wait_or_kill();
            let stderr = self.stderr_text();
            assert!(
                status.success(),
                "{label}：sidecar 应正常退出；status={status:?}; stderr={stderr:?}"
            );
        }

        /// raw 客户端版本的有界 EOF 收尾。
        fn finish_raw(&mut self, client: &mut RawClient, label: &str) {
            drop(client.stdin.take());
            let status = self.wait_or_kill();
            let stderr = self.stderr_text();
            assert!(
                status.success(),
                "{label}：sidecar 应正常退出；status={status:?}; stderr={stderr:?}"
            );
        }

        fn wait_or_kill(&mut self) -> ExitStatus {
            let deadline = Instant::now() + EXIT_TIMEOUT;
            loop {
                match self.child.try_wait() {
                    Ok(Some(status)) => return status,
                    Ok(None) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(20));
                    }
                    Ok(None) => {
                        let _ = self.child.kill();
                        let _ = self.child.wait();
                        panic!("sidecar 未在 {EXIT_TIMEOUT:?} 内退出");
                    }
                    Err(error) => panic!("检查 sidecar 退出状态失败: {error}"),
                }
            }
        }

        fn stderr_text(&mut self) -> String {
            if let Some(stderr) = &self.stderr_cache {
                return stderr.clone();
            }
            if let Some(thread) = self.stderr_thread.take() {
                let _ = thread.join();
            }
            let stderr = self
                .stderr_rx
                .recv_timeout(Duration::from_secs(1))
                .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
                .unwrap_or_default();
            self.stderr_cache = Some(stderr.clone());
            stderr
        }
    }

    impl Drop for TestProcess {
        fn drop(&mut self) {
            if self.child.try_wait().ok().flatten().is_none() {
                let _ = self.child.kill();
                let _ = self.child.wait();
            }
            if let Some(thread) = self.stderr_thread.take() {
                let _ = thread.join();
            }
        }
    }

    /// 仅供取消合同使用的同步 JSON-RPC client；stdout 仍只有 sidecar gateway 写入。
    struct RawClient {
        stdin: Option<Arc<Mutex<ChildStdin>>>,
        stdout: Option<BufReader<ChildStdout>>,
        next_id: u64,
        raw_lines: Vec<String>,
        pending_responses: BTreeMap<u64, Value>,
    }

    impl RawClient {
        fn send_request(&mut self, method: &str, params: Value) -> u64 {
            let id = self.next_id;
            self.next_id += 1;
            self.write_message(serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": method,
                "params": params,
            }));
            id
        }

        fn send_notification(&mut self, method: &str, params: Value) {
            self.write_message(serde_json::json!({
                "jsonrpc": "2.0",
                "method": method,
                "params": params,
            }));
        }

        fn request(&mut self, method: &str, params: Value) -> Value {
            let id = self.send_request(method, params);
            self.read_response(id)
        }

        fn read_response(&mut self, id: u64) -> Value {
            if let Some(value) = self.pending_responses.remove(&id) {
                assert!(value.get("error").is_none(), "JSON-RPC 请求失败: {value}");
                return value;
            }

            loop {
                let value = self.read_message();
                let response_id = value.get("id").and_then(Value::as_u64);
                if response_id != Some(id) {
                    if let Some(response_id) = response_id
                        && value.get("method").is_none()
                    {
                        self.pending_responses.insert(response_id, value);
                    }
                    continue;
                }
                assert!(value.get("error").is_none(), "JSON-RPC 请求失败: {value}");
                return value;
            }
        }

        /// 读取一条完整 stdout JSON-RPC 行；测试可据此观察 reverse request。
        fn read_message(&mut self) -> Value {
            let mut line = String::new();
            loop {
                line.clear();
                let stdout = self.stdout.as_mut().expect("测试 stdout 已关闭");
                let bytes = stdout.read_line(&mut line).expect("读取 sidecar stdout");
                assert_ne!(bytes, 0, "sidecar stdout 提前 EOF");
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                self.raw_lines.push(trimmed.to_owned());
                return serde_json::from_str(trimmed).expect("stdout 必须是 JSON");
            }
        }

        /// 进程退出后继续读取 stdout，确保 response 后的迟到通知也进入断言集合。
        fn drain_stdout_after_process_exit(&mut self) {
            let Some(stdout) = self.stdout.as_mut() else {
                return;
            };
            let mut drained = Vec::new();
            let mut line = String::new();
            loop {
                line.clear();
                let bytes = stdout
                    .read_line(&mut line)
                    .expect("进程退出后读取剩余 stdout");
                if bytes == 0 {
                    break;
                }
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                drained.push(trimmed.to_owned());
            }
            self.raw_lines.extend(drained);
        }

        /// 回复测试中观察到的 ACP reverse request。
        fn send_response(&mut self, id: u64, result: Value) {
            self.write_message(serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": result,
            }));
        }

        fn close_stdout(&mut self) {
            drop(self.stdout.take());
        }

        fn write_message(&mut self, message: Value) {
            let line = serde_json::to_vec(&message).expect("序列化测试 JSON-RPC");
            let stdin = self.stdin.as_ref().expect("测试 stdin 已关闭");
            let mut stdin = stdin.lock().expect("测试 stdin 锁不应中毒");
            stdin.write_all(&line).expect("写入 sidecar stdin");
            stdin.write_all(b"\n").expect("写入 JSON-RPC 换行");
            stdin.flush().expect("刷新 sidecar stdin");
        }
    }

    /// 构造 sidecar runtime 使用的已审核 HTTP MCP 配置。
    fn approved_http_mcp(server_name: &str, url: String) -> ApprovedMcpConfig {
        let mut servers = BTreeMap::new();
        servers.insert(server_name.to_owned(), McpServerSpec::Http { url });
        ApprovedMcpConfig { servers }
    }

    /// 读取 JSON fixture，保持 fixture 的 method/params 与实际 wire 可对拍。
    fn fixture(raw: &str) -> Value {
        serde_json::from_str(raw).expect("ACP fixture 必须是 JSON object")
    }

    fn initialize_params() -> Value {
        fixture(include_str!("fixtures/acp_wire/initialize.json"))["params"].clone()
    }

    fn session_list_params(cwd: &Path) -> Value {
        let mut params =
            fixture(include_str!("fixtures/acp_wire/session_list.json"))["params"].clone();
        params["cwd"] = Value::String(cwd.display().to_string());
        params
    }

    /// 发送最小 initialize，并返回 typed ACP response 的 JSON 视图。
    fn initialize(client: &mut AcpClient) -> Value {
        client
            .request("initialize", initialize_params(), REQUEST_TIMEOUT)
            .expect("initialize 必须成功")
    }

    /// 创建一个不携带 MCP server 的内存 session。
    fn new_session(client: &mut AcpClient, cwd: &Path) -> Value {
        client
            .request(
                "session/new",
                serde_json::json!({
                    "cwd": cwd,
                    "mcpServers": []
                }),
                REQUEST_TIMEOUT,
            )
            .expect("session/new 必须成功")
    }

    fn session_id(response: &Value) -> String {
        response["result"]["sessionId"]
            .as_str()
            .expect("session/new 必须返回字符串 sessionId")
            .to_owned()
    }

    /// 构造带 ACP `_meta.promptId` 的标准 prompt 请求。
    fn prompt_params(session_id: &str, prompt: Value, meta: Option<Value>) -> Value {
        let mut params = serde_json::json!({
            "sessionId": session_id,
            "prompt": prompt
        });
        if let Some(meta) = meta {
            params["_meta"] = meta;
        }
        params
    }

    /// 断言 sidecar 以固定 invalid_params 拒绝请求，且错误中没有 prompt 正文。
    fn assert_invalid_prompt(error: AcpError, secret: &str) {
        let AcpError::RpcError(error) = error else {
            panic!("无效 prompt 应返回 JSON-RPC error，实际: {error:?}");
        };
        assert_eq!(error["code"], -32602);
        assert_eq!(error["message"], "Invalid params");
        if !secret.is_empty() {
            assert!(
                !error.to_string().contains(secret),
                "prompt 正文不得出现在错误响应中: {error}"
            );
        }
        if let Some(data) = error.get("data") {
            assert!(
                !data.to_string().contains(secret),
                "错误 data 不得回显 prompt 正文: {error}"
            );
        }
    }

    /// 断言 stdout 每一行都是完整 JSON-RPC，且不出现日志文本。
    fn assert_jsonrpc_lines(lines: &[String]) {
        assert!(!lines.is_empty(), "应至少收到一条 ACP JSON-RPC 响应");
        for (index, line) in lines.iter().enumerate() {
            let value: Value = serde_json::from_str(line)
                .unwrap_or_else(|error| panic!("stdout 第 {index} 行不是 JSON: {error}"));
            assert_eq!(value["jsonrpc"], "2.0", "stdout 第 {index} 行不是 JSON-RPC");
            assert!(
                value.get("id").is_some() || value.get("method").and_then(Value::as_str).is_some(),
                "stdout 第 {index} 行缺少 response id 或 notification method"
            );
            assert!(!line.contains("sidecar runtime"));
        }
    }

    /// 等待 sidecar 测试 seam 进入指定阶段，避免用固定 sleep 猜测调度顺序。
    fn wait_for_seam_event(seam: &Path, name: &str) {
        let marker = seam.join(format!("{name}.entered"));
        let deadline = Instant::now() + EXIT_TIMEOUT;
        while !marker.exists() {
            assert!(
                Instant::now() < deadline,
                "测试 seam 未进入阶段 {name}: {seam:?}"
            );
            thread::sleep(Duration::from_millis(5));
        }
    }

    /// 启用 sidecar 测试 seam 的指定阶段。
    fn enable_seam(seam: &Path, name: &str) {
        fs::write(seam.join(format!("{name}.enabled")), b"enable").expect("启用测试 seam 必须成功");
    }

    /// 释放 sidecar 测试 seam 的指定阶段。
    fn release_seam(seam: &Path, name: &str) {
        fs::write(seam.join(format!("{name}.release")), b"release")
            .expect("释放测试 seam 必须成功");
    }

    /// 读取仅测试执行 spy；缺失文件也视为零次执行。
    fn execution_count(seam: &Path) -> usize {
        fs::read_to_string(seam.join("execution-count"))
            .ok()
            .and_then(|value| value.trim().parse().ok())
            .unwrap_or(0)
    }

    /// 通过真实 ACP initialize 验证最小 runtime handshake 与能力闭集。
    #[test]
    fn initialize_advertises_minimal_runtime_handshake() {
        let fixture = Fixture::new();
        let (mut process, mut client) = fixture.spawn();
        let response = initialize(&mut client);
        let result = response["result"].clone();

        assert_eq!(result["protocolVersion"], 1);
        assert_eq!(result["_meta"]["efflabRuntime"], "minimal-v1");
        assert_eq!(result["_meta"]["efflabSchemaVersion"], 1);
        assert_eq!(result["_meta"]["efflabSessionStoreVersion"], 1);
        assert_eq!(result["agentCapabilities"]["loadSession"], true);
        assert_eq!(
            result["agentCapabilities"]["promptCapabilities"],
            serde_json::json!({ "image": false, "audio": false, "embeddedContext": false })
        );
        assert_eq!(
            result["agentCapabilities"]["mcpCapabilities"],
            serde_json::json!({ "http": false, "sse": false })
        );
        assert_eq!(
            result["agentCapabilities"]["sessionCapabilities"]["list"],
            serde_json::json!({})
        );
        // ACP 0.10.4 将 fs/terminal 放在 InitializeRequest.clientCapabilities；
        // AgentCapabilities 不包含这两个字段。unstable schema 的 auth 容器保持空对象。
        assert!(result["agentCapabilities"].get("fs").is_none());
        assert!(result["agentCapabilities"].get("terminal").is_none());
        assert_eq!(result["agentCapabilities"]["auth"], serde_json::json!({}));
        assert_eq!(result["authMethods"], serde_json::json!([]));
        assert_jsonrpc_lines(&client.raw_lines());
        process.finish(&mut client, "initialize handshake");
    }

    /// initialize 未广告认证方法时，任意 authenticate methodId 都必须 fail-closed。
    #[test]
    fn authenticate_rejects_unadvertised_method_when_auth_methods_are_empty() {
        let fixture = Fixture::new();
        let (mut process, mut client) = fixture.spawn();
        let initialized = initialize(&mut client);
        assert_eq!(initialized["result"]["authMethods"], serde_json::json!([]));

        for method_id in ["byok", "unknown-method", ""] {
            let error = client
                .request(
                    "authenticate",
                    serde_json::json!({ "methodId": method_id }),
                    REQUEST_TIMEOUT,
                )
                .expect_err("未广告的认证方法必须被拒绝");
            let AcpError::RpcError(error) = error else {
                panic!("authenticate 应返回 JSON-RPC error，实际: {error:?}");
            };
            assert_eq!(error["code"], -32601);
            assert_eq!(error["message"], "Method not found");
            assert!(error.get("data").is_none());
        }

        // 失败的 authenticate 不得改变仍为空的认证能力声明。
        let reinitialized = initialize(&mut client);
        assert_eq!(
            reinitialized["result"]["authMethods"],
            serde_json::json!([])
        );
        assert_jsonrpc_lines(&client.raw_lines());
        process.finish(&mut client, "authenticate rejection");
    }

    /// 标准 session/new 只接受空 mcpServers；非空数组必须落成 invalid_params。
    #[test]
    fn session_new_rejects_nonempty_mcp_servers() {
        let fixture = Fixture::new();
        let (mut process, mut client) = fixture.spawn();
        let _ = initialize(&mut client);
        let error = client
            .request(
                "session/new",
                serde_json::json!({
                    "cwd": fixture.session_cwd,
                    "mcpServers": [{
                        "name": "untrusted",
                        "command": "/tmp/untrusted-mcp",
                        "args": [],
                        "env": []
                    }]
                }),
                REQUEST_TIMEOUT,
            )
            .expect_err("非空 mcpServers 必须被拒绝");
        let AcpError::RpcError(error) = error else {
            panic!("非空 mcpServers 应返回 JSON-RPC error，实际: {error:?}");
        };
        assert_eq!(error["code"], -32602);
        assert_jsonrpc_lines(&client.raw_lines());
        process.finish(&mut client, "session/new MCP rejection");
    }

    /// 标准 session/list 使用 schema 真实的 wire method，并返回内存 session。
    #[test]
    fn session_list_uses_standard_wire_method_and_returns_sessions() {
        let fixture = Fixture::new();
        let (mut process, mut client) = fixture.spawn();
        let _ = initialize(&mut client);
        let created = new_session(&mut client, &fixture.session_cwd);
        let created_id = session_id(&created);
        let listed = client
            .request(
                "session/list",
                session_list_params(&fixture.session_cwd),
                REQUEST_TIMEOUT,
            )
            .expect("标准 session/list 必须成功");
        let sessions = listed["result"]["sessions"]
            .as_array()
            .expect("session/list result.sessions 必须是数组");
        assert!(sessions.iter().any(|session| {
            session["sessionId"].as_str() == Some(created_id.as_str())
                && session["cwd"] == fixture.session_cwd.display().to_string()
        }));
        assert_jsonrpc_lines(&client.raw_lines());
        process.finish(&mut client, "session/list");
    }

    /// session/close 删除已创建的 session，随后 list 不再返回该 id。
    #[test]
    fn session_close_removes_session_from_list() {
        let fixture = Fixture::new();
        let (mut process, mut client) = fixture.spawn();
        let _ = initialize(&mut client);
        let created = new_session(&mut client, &fixture.session_cwd);
        let created_id = session_id(&created);
        let closed = client
            .request(
                "session/close",
                serde_json::json!({ "sessionId": created_id }),
                REQUEST_TIMEOUT,
            )
            .expect("session/close 必须成功");
        assert!(closed["result"].is_object());
        let listed = client
            .request(
                "session/list",
                session_list_params(&fixture.session_cwd),
                REQUEST_TIMEOUT,
            )
            .expect("session/list 必须成功");
        let sessions = listed["result"]["sessions"]
            .as_array()
            .expect("session/list result.sessions 必须是数组");
        assert!(
            sessions
                .iter()
                .all(|session| session["sessionId"].as_str() != Some(created_id.as_str())),
            "已 close 的 session 不得再出现在 list"
        );
        let missing = client.request(
            "session/close",
            serde_json::json!({ "sessionId": created_id }),
            REQUEST_TIMEOUT,
        );
        assert!(missing.is_err(), "重复 close 未知 session 必须失败");
        assert_jsonrpc_lines(&client.raw_lines());
        process.finish(&mut client, "session/close");
    }

    /// 标准 session/load 可加载当前进程的最小内存 session，不触及持久化或模型。
    #[test]
    fn session_load_accepts_existing_memory_session() {
        let fixture = Fixture::new();
        let (mut process, mut client) = fixture.spawn();
        let _ = initialize(&mut client);
        let created = new_session(&mut client, &fixture.session_cwd);
        let loaded = client
            .request(
                "session/load",
                serde_json::json!({
                    "sessionId": session_id(&created),
                    "cwd": fixture.session_cwd,
                    "mcpServers": []
                }),
                REQUEST_TIMEOUT,
            )
            .expect("session/load 必须成功");
        assert!(loaded["result"].is_object());
        assert_jsonrpc_lines(&client.raw_lines());
        process.finish(&mut client, "session/load");
    }

    /// 当前 profile 允许 session/new 与 session/load 复用固定 Channel 槽名。
    #[test]
    fn session_model_meta_accepts_byok_channel_slot() {
        let fixture = Fixture::new();
        let (mut process, mut client) = fixture.spawn();
        let _ = initialize(&mut client);
        let created = client
            .request(
                "session/new",
                serde_json::json!({
                    "cwd": fixture.session_cwd,
                    "mcpServers": [],
                    "_meta": { "modelId": "byok" }
                }),
                REQUEST_TIMEOUT,
            )
            .expect("session/new 的 byok modelId 必须成功");
        let loaded = client
            .request(
                "session/load",
                serde_json::json!({
                    "sessionId": session_id(&created),
                    "cwd": fixture.session_cwd,
                    "mcpServers": [],
                    "_meta": { "modelId": "byok" }
                }),
                REQUEST_TIMEOUT,
            )
            .expect("session/load 的 byok modelId 必须成功");
        assert!(loaded["result"].is_object());
        assert_jsonrpc_lines(&client.raw_lines());
        process.finish(&mut client, "byok session metadata");
    }

    /// session 生命周期只接受当前 profile 的真实 ACP 字段与值，不放宽 scope 或 metadata。
    #[test]
    fn session_lifecycle_rejects_out_of_profile_fields() {
        let fixture = Fixture::new();
        let (mut process, mut client) = fixture.spawn();
        let _ = initialize(&mut client);
        let created = new_session(&mut client, &fixture.session_cwd);
        let created_id = session_id(&created);
        let cases = [
            (
                "session/new additionalDirectories",
                "session/new",
                serde_json::json!({
                    "cwd": fixture.session_cwd,
                    "mcpServers": [],
                    "additionalDirectories": [fixture.session_cwd]
                }),
            ),
            (
                "session/new promptId meta",
                "session/new",
                serde_json::json!({
                    "cwd": fixture.session_cwd,
                    "mcpServers": [],
                    "_meta": { "promptId": "session-prompt-secret" }
                }),
            ),
            (
                "session/new provider modelId",
                "session/new",
                serde_json::json!({
                    "cwd": fixture.session_cwd,
                    "mcpServers": [],
                    "_meta": { "modelId": "grok-code-fast" }
                }),
            ),
            (
                "session/new empty modelId",
                "session/new",
                serde_json::json!({
                    "cwd": fixture.session_cwd,
                    "mcpServers": [],
                    "_meta": { "modelId": "" }
                }),
            ),
            (
                "session/new non-string modelId",
                "session/new",
                serde_json::json!({
                    "cwd": fixture.session_cwd,
                    "mcpServers": [],
                    "_meta": { "modelId": 7 }
                }),
            ),
            (
                "session/load additionalDirectories",
                "session/load",
                serde_json::json!({
                    "sessionId": created_id,
                    "cwd": fixture.session_cwd,
                    "mcpServers": [],
                    "additionalDirectories": [fixture.session_cwd]
                }),
            ),
            (
                "session/load provider modelId",
                "session/load",
                serde_json::json!({
                    "sessionId": created_id,
                    "cwd": fixture.session_cwd,
                    "mcpServers": [],
                    "_meta": { "modelId": "grok-code-fast" }
                }),
            ),
            (
                "session/list missing cwd",
                "session/list",
                serde_json::json!({}),
            ),
            (
                "session/list other cwd",
                "session/list",
                serde_json::json!({ "cwd": "/not-the-session-cwd" }),
            ),
            (
                "session/list meta",
                "session/list",
                serde_json::json!({
                    "cwd": fixture.session_cwd,
                    "_meta": { "modelId": "model-meta-secret" }
                }),
            ),
        ];

        for (label, method, params) in cases {
            let error = client
                .request(method, params, REQUEST_TIMEOUT)
                .expect_err("越过 session profile 的请求必须被拒绝");
            let AcpError::RpcError(error) = error else {
                panic!("{label} 应返回 JSON-RPC error，实际: {error:?}");
            };
            assert_eq!(error["code"], -32602, "{label}");
            assert_eq!(error["message"], "Invalid params", "{label}");
        }

        assert_jsonrpc_lines(&client.raw_lines());
        process.finish(&mut client, "session profile rejection");
    }

    /// 真实 ACP 扩展必须返回非空 approved MCP catalog，并保持双层 result 包装。
    #[test]
    fn real_mcp_extension_lists_approved_catalog() {
        let mcp = ScriptedMcp::new(["search"], false);
        let fixture = Fixture::with_model_url_expected_tools_and_mcp(
            "http://127.0.0.1:43123/v1",
            ["approved__search".to_owned()],
            approved_http_mcp("approved", mcp.url()),
        );
        let (mut process, mut client) = fixture.spawn_raw();
        let _ = client.request("initialize", initialize_params());
        let session = session_id(&client.request(
            "session/new",
            json!({ "cwd": fixture.session_cwd, "mcpServers": [] }),
        ));
        let response = client.request("_x.ai/mcp/list", json!({ "sessionId": session }));
        assert_eq!(
            response["result"]["result"]["servers"][0]["name"],
            "approved"
        );
        assert_eq!(
            response["result"]["result"]["servers"][0]["session"]["tools"][0]["name"],
            "search"
        );
        assert_eq!(
            response["result"]["result"]["servers"][0]["session"]["status"],
            "ready"
        );
        assert!(mcp.methods().iter().any(|method| method == "tools/list"));
        assert_jsonrpc_lines(&client.raw_lines);
        process.finish_raw(&mut client, "real MCP extension catalog");
    }

    /// 实际 catalog 的未批准工具必须保留供 Host 审计，模型只能看到 approved 交集。
    #[test]
    fn real_mcp_extension_retains_actual_extra_for_fail_closed_host_audit() {
        let mcp = ScriptedMcp::new(["search", "extra"], false);
        let fixture = Fixture::with_model_url_expected_tools_and_mcp(
            "http://127.0.0.1:43123/v1",
            ["approved__search".to_owned()],
            approved_http_mcp("approved", mcp.url()),
        );
        let (mut process, mut client) = fixture.spawn_raw();
        let _ = client.request("initialize", initialize_params());
        let session = session_id(&client.request(
            "session/new",
            json!({ "cwd": fixture.session_cwd, "mcpServers": [] }),
        ));
        let response = client.request("_x.ai/mcp/list", json!({ "sessionId": session }));
        let tools = response["result"]["result"]["servers"][0]["session"]["tools"]
            .as_array()
            .expect("真实 catalog tools 必须是数组");
        assert!(tools.iter().any(|tool| tool["name"] == "extra"));
        assert!(
            tools
                .iter()
                .all(|tool| tool["enabled"].as_bool() == Some(true))
        );
        assert_jsonrpc_lines(&client.raw_lines);
        process.finish_raw(&mut client, "actual MCP catalog audit evidence");
    }

    /// 模型应收到 approved MCP function schema，并在 permission allow 后完成真实 HTTP call。
    #[test]
    fn model_mcp_tool_schema_and_call_complete_through_acp() {
        let mcp = ScriptedMcp::new(["search"], false);
        let model = ScriptedL3b::new([
            ScriptedResponse::tool_call("approved__search", r#"{"query":"mcp-secret"}"#),
            ScriptedResponse::text("mcp complete"),
        ]);
        let fixture = Fixture::with_model_url_expected_tools_and_mcp(
            model.base_url().to_owned(),
            ["approved__search".to_owned()],
            approved_http_mcp("approved", mcp.url()),
        );
        let (mut process, mut client) = fixture.spawn_raw();
        let _ = client.request("initialize", initialize_params());
        let session = session_id(&client.request(
            "session/new",
            json!({ "cwd": fixture.session_cwd, "mcpServers": [] }),
        ));
        let prompt_rpc_id = client.send_request(
            "session/prompt",
            prompt_params(
                &session,
                json!([{ "type": "text", "text": "call MCP" }]),
                Some(json!({ "promptId": "prompt-mcp-call" })),
            ),
        );
        model.wait_for_requests(1);
        let first_request = model
            .request_bodies()
            .into_iter()
            .next()
            .expect("模型必须收到首个 request");
        assert!(
            first_request["tools"]
                .as_array()
                .is_some_and(|tools| tools.iter().any(|tool| {
                    tool["type"] == "function" && tool["function"]["name"] == "approved__search"
                })),
            "模型 request 必须包含 approved MCP function schema: {first_request}"
        );
        let permission = loop {
            let message = client.read_message();
            if message["method"] == "session/request_permission"
                || message["method"] == "_x.ai/session/request_permission"
            {
                break message;
            }
            assert_ne!(
                message["id"].as_u64(),
                Some(prompt_rpc_id),
                "MCP call 必须在 permission reverse request 后执行: {message}"
            );
        };
        assert_eq!(
            permission["params"]["toolCall"]["title"],
            "approved__search"
        );
        let permission_id = permission["id"]
            .as_u64()
            .expect("MCP permission reverse request 必须有 id");
        client.send_response(
            permission_id,
            json!({ "outcome": { "outcome": "selected", "optionId": "allow-once" } }),
        );
        mcp.wait_for_call();
        let response = client.read_response(prompt_rpc_id);
        assert_eq!(response["result"]["stopReason"], "end_turn");
        assert_eq!(mcp.call_count(), 1);
        model.wait_for_requests(2);
        let records_path = fixture
            .home
            .join("efflab-sessions")
            .join("v1")
            .join(&session)
            .join("records.jsonl");
        let records = fs::read_to_string(records_path).expect("MCP prompt journal 必须存在");
        assert!(!records.contains("mcp-secret"));
        assert!(
            !client
                .raw_lines
                .iter()
                .any(|line| line.contains("mcp-secret"))
        );
        assert_jsonrpc_lines(&client.raw_lines);
        process.finish_raw(&mut client, "MCP model call");
    }

    /// permission reject 必须阻止 MCP HTTP call，而不是仅在模型层过滤工具。
    #[test]
    fn rejected_mcp_permission_does_not_execute_http_call() {
        let mcp = ScriptedMcp::new(["search"], false);
        let model = ScriptedL3b::new([ScriptedResponse::tool_call("approved__search", "{}")]);
        let fixture = Fixture::with_model_url_expected_tools_and_mcp(
            model.base_url().to_owned(),
            ["approved__search".to_owned()],
            approved_http_mcp("approved", mcp.url()),
        );
        let (mut process, mut client) = fixture.spawn_raw();
        let _ = client.request("initialize", initialize_params());
        let session = session_id(&client.request(
            "session/new",
            json!({ "cwd": fixture.session_cwd, "mcpServers": [] }),
        ));
        let prompt_rpc_id = client.send_request(
            "session/prompt",
            prompt_params(
                &session,
                json!([{ "type": "text", "text": "reject MCP" }]),
                Some(json!({ "promptId": "prompt-mcp-reject" })),
            ),
        );
        model.wait_for_requests(1);
        let permission = loop {
            let message = client.read_message();
            if message["method"] == "session/request_permission"
                || message["method"] == "_x.ai/session/request_permission"
            {
                break message;
            }
            assert_ne!(
                message["id"].as_u64(),
                Some(prompt_rpc_id),
                "reject 测试必须先观察到 permission reverse request: {message}"
            );
        };
        client.send_response(
            permission["id"].as_u64().expect("permission id 必须存在"),
            json!({ "outcome": { "outcome": "selected", "optionId": "reject-once" } }),
        );
        let response = client.read_response(prompt_rpc_id);
        assert_eq!(response["result"]["stopReason"], "refusal");
        assert_eq!(mcp.call_count(), 0, "reject 后不得发出 MCP tools/call");
        process.finish_raw(&mut client, "rejected MCP permission");
    }

    /// EOF 必须取消 active MCP call，并在关闭 HTTP session 后让 sidecar 正常退出。
    #[test]
    fn stdin_eof_cancels_active_mcp_call_before_exit() {
        let mcp = ScriptedMcp::new(["search"], true);
        let model = ScriptedL3b::new([ScriptedResponse::tool_call("approved__search", "{}")]);
        let fixture = Fixture::with_model_url_expected_tools_and_mcp(
            model.base_url().to_owned(),
            ["approved__search".to_owned()],
            approved_http_mcp("approved", mcp.url()),
        );
        let (mut process, mut client) = fixture.spawn_raw();
        let _ = client.request("initialize", initialize_params());
        let session = session_id(&client.request(
            "session/new",
            json!({ "cwd": fixture.session_cwd, "mcpServers": [] }),
        ));
        let prompt_rpc_id = client.send_request(
            "session/prompt",
            prompt_params(
                &session,
                json!([{ "type": "text", "text": "EOF MCP" }]),
                Some(json!({ "promptId": "prompt-mcp-eof" })),
            ),
        );
        model.wait_for_requests(1);
        let permission = loop {
            let message = client.read_message();
            if message["method"] == "session/request_permission"
                || message["method"] == "_x.ai/session/request_permission"
            {
                break message;
            }
            assert_ne!(
                message["id"].as_u64(),
                Some(prompt_rpc_id),
                "EOF 测试必须先观察到 permission reverse request: {message}"
            );
        };
        client.send_response(
            permission["id"].as_u64().expect("permission id 必须存在"),
            json!({ "outcome": { "outcome": "selected", "optionId": "allow-once" } }),
        );
        mcp.wait_for_call();
        drop(client.stdin.take());
        let status = process.wait_or_kill();
        let stderr = process.stderr_text();
        assert!(
            status.success(),
            "MCP active call 的 EOF 清理必须正常退出: {status:?}; stderr={stderr:?}"
        );
        assert_eq!(mcp.call_count(), 1);
        assert!(
            mcp.methods().iter().any(|method| method == "DELETE"),
            "EOF cleanup 必须尝试关闭 MCP HTTP session"
        );
        assert!(
            stderr.contains("mcp_call_cancel_cleanup"),
            "MCP cancel cleanup 必须记录稳定 cleanup 事件: {stderr:?}"
        );
        assert!(
            stderr.contains("error_code=\"mcp_call_cancelled\""),
            "MCP cancel cleanup 日志只能暴露稳定错误码: {stderr:?}"
        );
        assert!(
            !stderr.contains("hyper_util::")
                && !stderr.contains("connecting to ")
                && !stderr.contains("connected to "),
            "第三方 HTTP debug 日志不得泄露 MCP endpoint: {stderr:?}"
        );
    }

    /// 合法纯文本 prompt 经过真实模型回合后返回 end_turn。
    #[test]
    fn prompt_returns_end_turn_after_model_call() {
        let server = ScriptedL3b::new([ScriptedResponse::text("assistant")]);
        let fixture = Fixture::with_model_url(server.base_url().to_owned());
        let (mut process, mut client) = fixture.spawn();
        let _ = initialize(&mut client);
        let created = new_session(&mut client, &fixture.session_cwd);
        let response = client
            .request(
                "session/prompt",
                serde_json::json!({
                    "sessionId": session_id(&created),
                    "prompt": [{ "type": "text", "text": "hello" }],
                    "_meta": { "promptId": "prompt-minimal" }
                }),
                REQUEST_TIMEOUT,
            )
            .expect("最小 session/prompt 必须返回 response");
        assert_eq!(response["result"]["stopReason"], "end_turn");
        assert_jsonrpc_lines(&client.raw_lines());
        process.finish(&mut client, "minimal prompt");
    }

    /// OpenAI 兼容推理字段必须投影为 agent_thought_chunk，且不得让 turn 失败。
    #[test]
    fn prompt_streams_reasoning_content_as_thought_chunks() {
        let server = ScriptedL3b::new([ScriptedResponse::reasoning_and_text("先想一步", "你好")]);
        let fixture = Fixture::with_model_url(server.base_url().to_owned());
        let (mut process, mut client) = fixture.spawn();
        let _ = initialize(&mut client);
        let created = new_session(&mut client, &fixture.session_cwd);
        let response = client
            .request(
                "session/prompt",
                serde_json::json!({
                    "sessionId": session_id(&created),
                    "prompt": [{ "type": "text", "text": "hello" }],
                    "_meta": { "promptId": "prompt-reasoning" }
                }),
                REQUEST_TIMEOUT,
            )
            .expect("带 reasoning_content 的 session/prompt 必须返回 response");
        assert_eq!(response["result"]["stopReason"], "end_turn");
        let lines = client.raw_lines();
        assert_eq!(thought_snapshots(&lines, "prompt-reasoning"), ["先想一步"]);
        assert_eq!(assistant_snapshots(&lines, "prompt-reasoning"), ["你好"]);
        assert_jsonrpc_lines(&lines);
        process.finish(&mut client, "reasoning prompt");
    }

    /// 当前 profile 只接受 text；其它 ACP ContentBlock 变体和 text 扩展字段都拒绝。
    #[test]
    fn prompt_rejects_non_text_and_extended_text_blocks() {
        let fixture = Fixture::new();
        let (mut process, mut client) = fixture.spawn();
        let _ = initialize(&mut client);
        let created = new_session(&mut client, &fixture.session_cwd);
        let session_id = session_id(&created);
        let cases = [
            (
                "image",
                serde_json::json!({
                    "type": "image",
                    "data": "image-body-secret",
                    "mimeType": "image/png"
                }),
                "image-body-secret",
            ),
            (
                "audio",
                serde_json::json!({
                    "type": "audio",
                    "data": "audio-body-secret",
                    "mimeType": "audio/wav"
                }),
                "audio-body-secret",
            ),
            (
                "resource_link",
                serde_json::json!({
                    "type": "resource_link",
                    "name": "resource-link-secret",
                    "uri": "file:///resource-link-secret"
                }),
                "resource-link-secret",
            ),
            (
                "resource",
                serde_json::json!({
                    "type": "resource",
                    "resource": {
                        "uri": "file:///embedded-resource-secret",
                        "text": "embedded-resource-secret"
                    }
                }),
                "embedded-resource-secret",
            ),
            (
                "unknown",
                serde_json::json!({
                    "type": "future_content_block",
                    "text": "unknown-block-secret"
                }),
                "unknown-block-secret",
            ),
            (
                "text_annotations",
                serde_json::json!({
                    "type": "text",
                    "text": "annotation-body-secret",
                    "annotations": { "priority": 0.5 }
                }),
                "annotation-body-secret",
            ),
            (
                "text_meta",
                serde_json::json!({
                    "type": "text",
                    "text": "block-meta-body-secret",
                    "_meta": { "secret": "block-meta-secret" }
                }),
                "block-meta-body-secret",
            ),
        ];

        for (label, block, secret) in cases {
            let response = client.request(
                "session/prompt",
                prompt_params(
                    &session_id,
                    serde_json::json!([block]),
                    Some(serde_json::json!({
                        "promptId": format!("prompt-{label}")
                    })),
                ),
                REQUEST_TIMEOUT,
            );
            assert_invalid_prompt(
                response.expect_err("不允许的 ContentBlock 必须被拒绝"),
                secret,
            );
        }

        assert_jsonrpc_lines(&client.raw_lines());
        process.finish(&mut client, "ContentBlock rejection");
    }

    /// prompt 的文本、promptId 与顶层 `_meta` 都必须满足固定的 fail-closed 边界。
    #[test]
    fn prompt_rejects_empty_oversized_or_unscoped_input() {
        const OVERSIZED_SECRET: &str = "oversized-body-secret";

        let fixture = Fixture::new();
        let (mut process, mut client) = fixture.spawn();
        let _ = initialize(&mut client);
        let created = new_session(&mut client, &fixture.session_cwd);
        let session_id = session_id(&created);
        let oversized_text = format!(
            "{OVERSIZED_SECRET}{}",
            "a".repeat(32_001 - OVERSIZED_SECRET.chars().count())
        );
        let cases = [
            (
                "empty_prompt",
                serde_json::json!([]),
                Some(serde_json::json!({ "promptId": "prompt-empty" })),
                "empty-prompt-secret",
            ),
            (
                "empty_text",
                serde_json::json!([{ "type": "text", "text": "" }]),
                Some(serde_json::json!({ "promptId": "prompt-empty-text" })),
                "",
            ),
            (
                "oversized_text",
                serde_json::json!([{ "type": "text", "text": oversized_text }]),
                Some(serde_json::json!({ "promptId": "prompt-oversized" })),
                OVERSIZED_SECRET,
            ),
            (
                "missing_prompt_id",
                serde_json::json!([{ "type": "text", "text": "missing-id-secret" }]),
                None,
                "missing-id-secret",
            ),
            (
                "empty_prompt_id",
                serde_json::json!([{ "type": "text", "text": "empty-id-secret" }]),
                Some(serde_json::json!({ "promptId": "" })),
                "empty-id-secret",
            ),
            (
                "model_id_meta",
                serde_json::json!([{ "type": "text", "text": "model-meta-secret" }]),
                Some(serde_json::json!({
                    "promptId": "prompt-model-meta",
                    "modelId": "model-meta-secret"
                })),
                "model-meta-secret",
            ),
            (
                "unknown_meta",
                serde_json::json!([{ "type": "text", "text": "unknown-meta-secret" }]),
                Some(serde_json::json!({
                    "promptId": "prompt-unknown-meta",
                    "future": "unknown-meta-value"
                })),
                "unknown-meta-secret",
            ),
        ];

        for (label, prompt, meta, secret) in cases {
            let response = client.request(
                "session/prompt",
                prompt_params(&session_id, prompt, meta),
                REQUEST_TIMEOUT,
            );
            assert_invalid_prompt(response.expect_err("无效 prompt 输入必须被拒绝"), secret);
            assert!(!label.is_empty());
        }

        let unknown_session = client.request(
            "session/prompt",
            prompt_params(
                "missing-session",
                serde_json::json!([{ "type": "text", "text": "unknown-session-secret" }]),
                Some(serde_json::json!({ "promptId": "prompt-unknown-session" })),
            ),
            REQUEST_TIMEOUT,
        );
        assert_invalid_prompt(
            unknown_session.expect_err("未知 session 必须被拒绝"),
            "unknown-session-secret",
        );

        assert_jsonrpc_lines(&client.raw_lines());
        process.finish(&mut client, "prompt input rejection");
    }

    /// 有界纯文本 prompt 成功后 session 仍可通过标准 load 读取。
    #[test]
    fn prompt_accepts_only_bounded_text_and_keeps_session_usable() {
        let server = ScriptedL3b::new([ScriptedResponse::text("bounded")]);
        let fixture = Fixture::with_model_url(server.base_url().to_owned());
        let (mut process, mut client) = fixture.spawn();
        let _ = initialize(&mut client);
        let created = new_session(&mut client, &fixture.session_cwd);
        let session_id = session_id(&created);
        let response = client
            .request(
                "session/prompt",
                prompt_params(
                    &session_id,
                    serde_json::json!([
                        { "type": "text", "text": "第一段" },
                        { "type": "text", "text": "第二段" }
                    ]),
                    Some(serde_json::json!({ "promptId": "prompt-bounded" })),
                ),
                REQUEST_TIMEOUT,
            )
            .expect("有界纯文本 prompt 必须成功");
        assert_eq!(response["result"]["stopReason"], "end_turn");

        // 失败请求不应删除 session；后续仍可走标准 session/load。
        let loaded = client
            .request(
                "session/load",
                serde_json::json!({
                    "sessionId": session_id,
                    "cwd": fixture.session_cwd,
                    "mcpServers": []
                }),
                REQUEST_TIMEOUT,
            )
            .expect("prompt 后 session/load 仍必须可用");
        assert!(loaded["result"].is_object());
        assert_jsonrpc_lines(&client.raw_lines());
        process.finish(&mut client, "bounded prompt");
    }

    /// 当前 profile 拒绝 ACP unstable messageId，回合标识只使用 `_meta.promptId`。
    #[test]
    fn prompt_rejects_unstable_message_id() {
        let fixture = Fixture::new();
        let (mut process, mut client) = fixture.spawn();
        let _ = initialize(&mut client);
        let created = new_session(&mut client, &fixture.session_cwd);
        let response = client.request(
            "session/prompt",
            serde_json::json!({
                "sessionId": session_id(&created),
                "messageId": "00000000-0000-4000-8000-000000000000",
                "prompt": [{ "type": "text", "text": "message-id-body-secret" }],
                "_meta": { "promptId": "prompt-message-id" }
            }),
            REQUEST_TIMEOUT,
        );
        assert_invalid_prompt(
            response.expect_err("不允许的 messageId 必须被拒绝"),
            "message-id-body-secret",
        );
        assert_jsonrpc_lines(&client.raw_lines());
        process.finish(&mut client, "messageId rejection");
    }

    /// 空 sessionId 的取消通知被拒绝，且不会产生 response 污染。
    #[test]
    fn cancel_notification_rejects_empty_session_id_without_response() {
        let fixture = Fixture::new();
        let (mut process, mut client) = fixture.spawn_raw();
        let _ = client.request("initialize", initialize_params());
        client.send_notification("session/cancel", serde_json::json!({ "sessionId": "" }));
        let listed = client.request("session/list", session_list_params(&fixture.session_cwd));
        assert!(listed["result"]["sessions"].is_array());
        assert_jsonrpc_lines(&client.raw_lines);
        process.finish_raw(&mut client, "empty session/cancel rejection");
        assert!(
            process.stderr_text().contains("cancel_session_id_missing"),
            "空 sessionId 的拒绝原因必须以脱敏 debug 日志记录"
        );
    }

    /// 非法 session/cancel metadata 作为通知被拒绝，且不会产生 response 污染。
    #[test]
    fn cancel_notification_rejects_meta_without_response() {
        let fixture = Fixture::new();
        let (mut process, mut client) = fixture.spawn_raw();
        let _ = client.request("initialize", initialize_params());
        let created = client.request(
            "session/new",
            serde_json::json!({ "cwd": fixture.session_cwd, "mcpServers": [] }),
        );
        let session_id = session_id(&created);
        client.send_notification(
            "session/cancel",
            serde_json::json!({
                "sessionId": session_id,
                "_meta": { "promptId": "cancel-meta-secret" }
            }),
        );
        let listed = client.request("session/list", session_list_params(&fixture.session_cwd));
        assert!(listed["result"]["sessions"].is_array());
        assert_jsonrpc_lines(&client.raw_lines);
        process.finish_raw(&mut client, "session/cancel metadata rejection");
        assert!(
            process.stderr_text().contains("cancel_meta_not_allowed"),
            "拒绝原因必须以脱敏 debug 日志记录"
        );
    }

    /// session/cancel 仍是通知；其 payload 不携带自定义字段，也不伪造 prompt 结果。
    #[test]
    fn cancel_notification_keeps_current_session_boundary() {
        let fixture = Fixture::new();
        let (mut process, mut client) = fixture.spawn_raw();
        let _ = client.request("initialize", initialize_params());
        let created = client.request(
            "session/new",
            serde_json::json!({ "cwd": fixture.session_cwd, "mcpServers": [] }),
        );
        let session_id = session_id(&created);
        client.send_notification(
            "session/cancel",
            serde_json::json!({ "sessionId": session_id }),
        );
        let listed = client.request("session/list", session_list_params(&fixture.session_cwd));
        assert!(listed["result"]["sessions"].is_array());
        assert_jsonrpc_lines(&client.raw_lines);
        process.finish_raw(&mut client, "cancel boundary");
    }

    /// session/cancel 是通知；它不得产生伪造 response，且后续 ACP 请求仍可用。
    #[test]
    fn cancel_notification_is_accepted_without_extra_response() {
        let server = ScriptedL3b::new([ScriptedResponse::blocked("partial")]);
        let fixture = Fixture::with_model_url(server.base_url().to_owned());
        let (mut process, mut client) = fixture.spawn_raw();
        let _ = client.request("initialize", initialize_params());
        let created = client.request(
            "session/new",
            serde_json::json!({ "cwd": fixture.session_cwd, "mcpServers": [] }),
        );
        let session_id = session_id(&created);
        let prompt_id = client.send_request(
            "session/prompt",
            serde_json::json!({
                "sessionId": session_id,
                "prompt": [{ "type": "text", "text": "hello" }],
                "_meta": { "promptId": "prompt-cancel" }
            }),
        );
        server.wait_for_requests(1);
        client.send_notification(
            "session/cancel",
            serde_json::json!({ "sessionId": session_id }),
        );
        let prompt_response = client.read_response(prompt_id);
        assert_eq!(prompt_response["result"]["stopReason"], "cancelled");
        let listed = client.request("session/list", session_list_params(&fixture.session_cwd));
        assert!(listed["result"]["sessions"].is_array());
        assert_jsonrpc_lines(&client.raw_lines);
        process.finish_raw(&mut client, "session/cancel");
    }

    /// 取得指定 prompt 的 assistant 文本快照，按 ACP 通知到达顺序保留重复快照。
    fn assistant_snapshots(lines: &[String], prompt_id: &str) -> Vec<String> {
        lines
            .iter()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|value| value["method"] == "session/update")
            .filter(|value| value["params"]["_meta"]["promptId"] == prompt_id)
            .filter(|value| value["params"]["update"]["sessionUpdate"] == "agent_message_chunk")
            .filter_map(|value| {
                value["params"]["update"]["content"]["text"]
                    .as_str()
                    .map(str::to_owned)
            })
            .collect()
    }

    /// 取得指定 prompt 的 thought 文本，按 ACP 通知到达顺序保留。
    fn thought_snapshots(lines: &[String], prompt_id: &str) -> Vec<String> {
        lines
            .iter()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|value| value["method"] == "session/update")
            .filter(|value| value["params"]["_meta"]["promptId"] == prompt_id)
            .filter(|value| value["params"]["update"]["sessionUpdate"] == "agent_thought_chunk")
            .filter_map(|value| {
                value["params"]["update"]["content"]["text"]
                    .as_str()
                    .map(str::to_owned)
            })
            .collect()
    }

    /// 读取 v1 journal 的 terminal 状态，验证 prompt 结束先于 ACP result。
    fn persisted_terminal(home: &Path, session_id: &str, prompt_id: &str) -> Option<String> {
        let path = home
            .join("efflab-sessions")
            .join("v1")
            .join(session_id)
            .join("records.jsonl");
        fs::read_to_string(path)
            .ok()?
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .rfind(|record| record["kind"] == "turn_terminal" && record["prompt_id"] == prompt_id)
            .and_then(|record| record["status"].as_str().map(str::to_owned))
    }

    /// 统计指定 prompt 的 terminal journal 数量，验证重复 cancel 不会重复落盘。
    fn persisted_terminal_count(home: &Path, session_id: &str, prompt_id: &str) -> usize {
        let path = home
            .join("efflab-sessions")
            .join("v1")
            .join(session_id)
            .join("records.jsonl");
        fs::read_to_string(path)
            .ok()
            .map(|contents| {
                contents
                    .lines()
                    .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                    .filter(|record| {
                        record["kind"] == "turn_terminal" && record["prompt_id"] == prompt_id
                    })
                    .count()
            })
            .unwrap_or(0)
    }

    /// 返回指定 prompt 的持久化工具状态，验证 permission reject 没有执行点。
    fn persisted_tool_statuses(home: &Path, session_id: &str, prompt_id: &str) -> Vec<String> {
        let path = home
            .join("efflab-sessions")
            .join("v1")
            .join(session_id)
            .join("records.jsonl");
        fs::read_to_string(path)
            .ok()
            .map(|contents| {
                contents
                    .lines()
                    .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                    .filter(|record| record["kind"] == "tool" && record["prompt_id"] == prompt_id)
                    .filter_map(|record| record["status"].as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// 返回某个 prompt 发出的 ACP tool call id，保留 wire 到达顺序。
    fn tool_call_ids(lines: &[String], prompt_id: &str) -> Vec<String> {
        lines
            .iter()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|value| value["method"] == "session/update")
            .filter(|value| value["params"]["_meta"]["promptId"] == prompt_id)
            .filter(|value| value["params"]["update"]["sessionUpdate"] == "tool_call")
            .filter_map(|value| {
                value["params"]["update"]["toolCallId"]
                    .as_str()
                    .map(str::to_owned)
            })
            .collect()
    }

    /// 断言指定 prompt 的所有 live session/update 都已在 JSON-RPC response 前送达。
    fn assert_updates_before_response(lines: &[String], response_id: u64, start: usize) {
        let values = lines
            .iter()
            .enumerate()
            .skip(start)
            .filter_map(|(index, line)| {
                serde_json::from_str::<Value>(line)
                    .ok()
                    .map(|value| (index, value))
            })
            .collect::<Vec<_>>();
        let response_index = values
            .iter()
            .position(|(_, value)| value["id"].as_u64() == Some(response_id))
            .expect("目标 JSON-RPC response 必须存在");
        assert!(
            values
                .iter()
                .skip(response_index + 1)
                .all(|(_, value)| value["method"] != "session/update"),
            "session/update 不得晚于目标 response: {values:?}"
        );
        assert!(
            values
                .iter()
                .take(response_index)
                .any(|(_, value)| value["method"] == "session/update"),
            "目标 response 前应至少有一条 session/update: {values:?}"
        );
    }

    /// session/load 的历史 update 必须统一带 replay 标记，并在 load response 前完成写入。
    #[test]
    fn replay_updates_mark_is_replay_and_precede_load_response() {
        let server = ScriptedL3b::new([ScriptedResponse::text("replayed")]);
        let fixture = Fixture::with_model_url(server.base_url().to_owned());
        let (mut process, mut client) = fixture.spawn_raw();
        let _ = client.request("initialize", initialize_params());
        let session = session_id(&client.request(
            "session/new",
            json!({ "cwd": fixture.session_cwd, "mcpServers": [] }),
        ));
        let prompt_id = client.send_request(
            "session/prompt",
            prompt_params(
                &session,
                json!([{ "type": "text", "text": "persist me" }]),
                Some(json!({ "promptId": "prompt-replay" })),
            ),
        );
        let prompt_response = client.read_response(prompt_id);
        assert_eq!(prompt_response["result"]["stopReason"], "end_turn");

        let start = client.raw_lines.len();
        let load_id = client.send_request(
            "session/load",
            json!({
                "sessionId": session,
                "cwd": fixture.session_cwd,
                "mcpServers": []
            }),
        );
        let load_response = client.read_response(load_id);
        assert!(load_response["result"].is_object());
        let replay_lines = &client.raw_lines[start..];
        let replay_values = replay_lines
            .iter()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .collect::<Vec<_>>();
        let load_position = replay_values
            .iter()
            .position(|value| value["id"].as_u64() == Some(load_id))
            .expect("session/load response 必须存在");
        let updates = replay_values
            .iter()
            .take(load_position)
            .filter(|value| value["method"] == "session/update")
            .collect::<Vec<_>>();
        assert!(!updates.is_empty(), "session/load 应回放历史 update");
        for update in updates {
            assert_eq!(
                update["params"]["_meta"],
                json!({ "promptId": "prompt-replay", "isReplay": true })
            );
            assert!(
                update["params"]["update"].get("_meta").is_none()
                    || update["params"]["update"]["_meta"]
                        == json!({ "promptId": "prompt-replay", "isReplay": true }),
                "嵌套 update 的 replay metadata 也必须统一: {update}"
            );
        }
        assert_jsonrpc_lines(&client.raw_lines);
        process.finish_raw(&mut client, "replay ordering");
    }

    /// session/load replay 必须跳过可存储但不满足 contract 的历史工具名。
    #[test]
    fn replay_skips_invalid_qualified_tool_audit_record() {
        let fixture = Fixture::new();
        let (mut first_process, mut first_client) = fixture.spawn_raw();
        let _ = first_client.request("initialize", initialize_params());
        let session = session_id(&first_client.request(
            "session/new",
            json!({ "cwd": fixture.session_cwd, "mcpServers": [] }),
        ));
        first_process.finish_raw(&mut first_client, "create replay audit record");

        let invalid_tool = "approved__bad.name";
        let records = [
            json!({
                "kind": "user",
                "schema_version": 1,
                "sequence": 0,
                "prompt_id": "prompt-replay-invalid",
                "text": "historical prompt"
            }),
            json!({
                "kind": "assistant_tool_calls",
                "schema_version": 1,
                "sequence": 1,
                "prompt_id": "prompt-replay-invalid",
                "round": 0,
                "tool_calls": [{ "tool_call_id": "call-invalid", "name": invalid_tool }],
                "text": ""
            }),
            json!({
                "kind": "tool",
                "schema_version": 1,
                "sequence": 2,
                "prompt_id": "prompt-replay-invalid",
                "round": 0,
                "tool_call_id": "call-invalid",
                "name": invalid_tool,
                "detail": "historical tool",
                "status": "completed"
            }),
            json!({
                "kind": "turn_terminal",
                "schema_version": 1,
                "sequence": 3,
                "prompt_id": "prompt-replay-invalid",
                "status": "completed"
            }),
        ]
        .into_iter()
        .map(|record| serde_json::to_string(&record).expect("测试 v1 record 必须可序列化"))
        .collect::<Vec<_>>()
        .join("\n");
        let records_path = fixture
            .home
            .join("efflab-sessions")
            .join("v1")
            .join(&session)
            .join("records.jsonl");
        fs::write(records_path, format!("{records}\n")).expect("写入 replay 审计记录");

        let (mut second_process, mut second_client) = fixture.spawn_raw();
        let _ = second_client.request("initialize", initialize_params());
        let load_start = second_client.raw_lines.len();
        let load_id = second_client.send_request(
            "session/load",
            json!({
                "sessionId": session,
                "cwd": fixture.session_cwd,
                "mcpServers": []
            }),
        );
        let load_response = second_client.read_response(load_id);
        assert!(load_response["result"].is_object());

        let replay_values = second_client.raw_lines[load_start..]
            .iter()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .collect::<Vec<_>>();
        assert!(replay_values.iter().any(|value| {
            value["method"] == "session/update"
                && value["params"]["update"]["sessionUpdate"] == "user_message_chunk"
        }));
        assert!(replay_values.iter().all(|value| {
            !(value["method"] == "session/update"
                && value["params"]["update"]["sessionUpdate"] == "tool_call_update"
                && value["params"]["update"]["title"] == invalid_tool)
        }));
        assert_jsonrpc_lines(&second_client.raw_lines);
        second_process.finish_raw(&mut second_client, "skip invalid replay audit record");
    }

    /// live update 必须先于 prompt result 写入 stdout，且 runtime 仍可用时 response 后无迟到 update。
    #[test]
    fn live_updates_precede_prompt_result_while_runtime_is_alive() {
        let gate = ResponseGate::new();
        let server = ScriptedL3b::new([ScriptedResponse::text_with_gate("ordered", gate.clone())]);
        let fixture = Fixture::with_model_url(server.base_url().to_owned());
        let (mut process, mut client) = fixture.spawn_raw();
        let _ = client.request("initialize", initialize_params());
        let session = session_id(&client.request(
            "session/new",
            json!({ "cwd": fixture.session_cwd, "mcpServers": [] }),
        ));
        let start = client.raw_lines.len();
        let prompt_id = client.send_request(
            "session/prompt",
            prompt_params(
                &session,
                json!([{ "type": "text", "text": "ordered prompt" }]),
                Some(json!({ "promptId": "prompt-order" })),
            ),
        );

        // 先观察已经写出的 assistant update，再释放模型 Done；若 writer 顺序错误，这里会先读到 response。
        loop {
            let update = client.read_message();
            assert_ne!(
                update["id"].as_u64(),
                Some(prompt_id),
                "prompt response 不得早于 assistant update: {update}"
            );
            if update["method"] == "session/update"
                && update["params"]["_meta"]["promptId"] == "prompt-order"
                && update["params"]["update"]["sessionUpdate"] == "agent_message_chunk"
            {
                break;
            }
        }
        gate.release();
        let response = client.read_response(prompt_id);
        assert_eq!(response["result"]["stopReason"], "end_turn");
        assert_updates_before_response(&client.raw_lines, prompt_id, start);

        // 不关闭 runtime：后续 list response 作为 output drain barrier，覆盖 response 后的迟到 update。
        let list_id =
            client.send_request("session/list", session_list_params(&fixture.session_cwd));
        let list_response = client.read_response(list_id);
        assert!(list_response["result"]["sessions"].is_array());
        let response_index = client
            .raw_lines
            .iter()
            .position(|line| {
                serde_json::from_str::<Value>(line)
                    .ok()
                    .is_some_and(|value| value["id"].as_u64() == Some(prompt_id))
            })
            .expect("prompt response 必须存在");
        assert!(
            client.raw_lines[response_index + 1..]
                .iter()
                .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                .all(|value| {
                    !(value["method"] == "session/update"
                        && value["params"]["_meta"]["promptId"] == "prompt-order")
                }),
            "prompt response 后不得有迟到 update: {:?}",
            client.raw_lines
        );
        assert_jsonrpc_lines(&client.raw_lines);
        process.finish_raw(&mut client, "live update ordering");
    }

    /// 拒绝 permission 后不得执行 noop、不得重试模型，也必须落拒绝终态。
    #[test]
    fn noop_permission_reject_does_not_execute_or_retry() {
        let server = ScriptedL3b::new([ScriptedResponse::tool_call("GrokBuild:efflab_noop", "{}")]);
        let fixture = Fixture::with_model_url(server.base_url().to_owned()).with_test_seam();
        let seam = fixture.test_seam.as_deref().expect("测试 seam 必须存在");
        let (mut process, mut client) = fixture.spawn_raw();
        let _ = client.request("initialize", initialize_params());
        let session = session_id(&client.request(
            "session/new",
            json!({ "cwd": fixture.session_cwd, "mcpServers": [] }),
        ));
        let prompt_rpc_id = client.send_request(
            "session/prompt",
            prompt_params(
                &session,
                json!([{ "type": "text", "text": "reject noop" }]),
                Some(json!({ "promptId": "prompt-reject" })),
            ),
        );
        server.wait_for_requests(1);
        let permission = loop {
            let message = client.read_message();
            if message["method"] == "session/request_permission"
                || message["method"] == "_x.ai/session/request_permission"
            {
                break message;
            }
            assert_ne!(
                message["id"].as_u64(),
                Some(prompt_rpc_id),
                "reject 测试应先观察到 permission reverse request: {message}"
            );
        };
        let permission_id = permission["id"]
            .as_u64()
            .expect("permission reverse request 必须带数字 id");
        assert_eq!(execution_count(seam), 0, "拒绝前执行点计数必须为零");
        client.send_response(
            permission_id,
            json!({ "outcome": { "outcome": "selected", "optionId": "reject-once" } }),
        );
        let response = client.read_response(prompt_rpc_id);
        assert_eq!(response["result"]["stopReason"], "refusal");
        assert_eq!(server.model_call_count(), 1, "拒绝后不得发起第二次模型调用");
        assert_eq!(
            execution_count(seam),
            0,
            "Host reject 后不得进入 noop 执行点"
        );
        process.finish_raw(&mut client, "rejected permission");
        client.drain_stdout_after_process_exit();
        assert_eq!(
            persisted_terminal(&fixture.home, &session, "prompt-reject").as_deref(),
            Some("refused")
        );
        let tool_statuses = persisted_tool_statuses(&fixture.home, &session, "prompt-reject");
        assert!(
            tool_statuses.iter().all(|status| status != "completed"),
            "reject permission 后不得落 completed 工具记录: {tool_statuses:?}"
        );
        assert!(
            client.raw_lines.iter().all(|line| {
                serde_json::from_str::<Value>(line)
                    .ok()
                    .filter(|value| value["method"] == "session/update")
                    .is_none_or(|value| {
                        value["params"]["update"]["status"] != "in_progress"
                            && value["params"]["update"]["status"] != "completed"
                    })
            }),
            "reject permission 后不得发送执行状态 update"
        );
        assert_jsonrpc_lines(&client.raw_lines);
    }

    /// 合法 70-byte MCP tool 必须贯通真实 prompt、journal、重启后的 session/load 和模型 transcript。
    #[test]
    fn long_mcp_tool_prompt_reload_recovers_assistant_tool_round() {
        let tool_name = format!("search-{}", "x".repeat(63));
        assert_eq!(tool_name.len(), 70);
        let qualified_name = format!("approved__{tool_name}");
        let mcp = ScriptedMcp::new([tool_name], false);
        let model = ScriptedL3b::new([
            ScriptedResponse::tool_call(qualified_name.clone(), "{}"),
            ScriptedResponse::text("first long tool complete"),
            ScriptedResponse::text("after long tool restart"),
        ]);
        let fixture = Fixture::with_model_url_expected_tools_and_mcp(
            model.base_url().to_owned(),
            [qualified_name.clone()],
            approved_http_mcp("approved", mcp.url()),
        );
        let (mut first_process, mut first_client) = fixture.spawn_raw();
        let _ = first_client.request("initialize", initialize_params());
        let session = session_id(&first_client.request(
            "session/new",
            json!({ "cwd": fixture.session_cwd, "mcpServers": [] }),
        ));
        let first_prompt = first_client.send_request(
            "session/prompt",
            prompt_params(
                &session,
                json!([{ "type": "text", "text": "请调用 70-byte MCP tool" }]),
                Some(json!({ "promptId": "prompt-long-mcp" })),
            ),
        );
        model.wait_for_requests(1);
        let first_body = model
            .request_bodies()
            .into_iter()
            .next()
            .expect("长工具 prompt 必须发出首个模型请求");
        assert!(first_body["tools"].as_array().is_some_and(|tools| {
            tools
                .iter()
                .any(|tool| tool["function"]["name"] == qualified_name)
        }));
        let permission = loop {
            let message = first_client.read_message();
            if message["method"] == "session/request_permission"
                || message["method"] == "_x.ai/session/request_permission"
            {
                break message;
            }
            assert_ne!(
                message["id"].as_u64(),
                Some(first_prompt),
                "长工具必须在 permission reverse request 后执行: {message}"
            );
        };
        let permission_id = permission["id"]
            .as_u64()
            .expect("长工具 permission reverse request 必须有数字 id");
        first_client.send_response(
            permission_id,
            json!({ "outcome": { "outcome": "selected", "optionId": "allow-once" } }),
        );
        mcp.wait_for_call();
        let first_response = first_client.read_response(first_prompt);
        assert_eq!(first_response["result"]["stopReason"], "end_turn");
        first_process.finish_raw(&mut first_client, "long MCP tool first process");

        let (mut second_process, mut second_client) = fixture.spawn_raw();
        let _ = second_client.request("initialize", initialize_params());
        let load_start = second_client.raw_lines.len();
        let load_id = second_client.send_request(
            "session/load",
            json!({
                "sessionId": session,
                "cwd": fixture.session_cwd,
                "mcpServers": []
            }),
        );
        let load_response = second_client.read_response(load_id);
        assert!(load_response["result"].is_object());
        let replay_values = second_client.raw_lines[load_start..]
            .iter()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .collect::<Vec<_>>();
        assert!(
            replay_values.iter().any(|value| {
                value["method"] == "session/update"
                    && value["params"]["update"]["sessionUpdate"] == "tool_call_update"
                    && value["params"]["update"]["title"] == qualified_name
            }),
            "session/load replay 必须保留真实 MCP tool title: {replay_values:?}"
        );

        let second_prompt = second_client.send_request(
            "session/prompt",
            prompt_params(
                &session,
                json!([{ "type": "text", "text": "继续这个长工具会话" }]),
                Some(json!({ "promptId": "prompt-long-mcp-restart" })),
            ),
        );
        model.wait_for_requests(3);
        let second_response = second_client.read_response(second_prompt);
        assert_eq!(second_response["result"]["stopReason"], "end_turn");
        let bodies = model.request_bodies();
        let recovered_messages = bodies[2]["messages"]
            .as_array()
            .expect("reload 后的模型请求必须包含 messages");
        let assistant = recovered_messages
            .iter()
            .find(|message| message["role"] == "assistant" && message["tool_calls"].is_array())
            .expect("reload 后必须恢复 assistant 长工具 round");
        assert_eq!(
            assistant["tool_calls"][0]["function"]["name"],
            qualified_name
        );
        let call_id = assistant["tool_calls"][0]["id"]
            .as_str()
            .expect("恢复的长工具调用必须有 id");
        assert!(recovered_messages.iter().any(|message| {
            message["role"] == "tool"
                && message["tool_call_id"] == call_id
                && message["content"] == "mcp tool completed"
        }));
        assert_jsonrpc_lines(&second_client.raw_lines);
        second_process.finish_raw(&mut second_client, "long MCP tool reload");
    }

    /// 两个工具 prompt 的 wire id 必须唯一，第二次模型请求必须恢复 assistant tool_calls/tool result 对。
    #[test]
    fn tool_ids_are_unique_and_transcript_recovers_assistant_tool_calls() {
        let server = ScriptedL3b::new([
            ScriptedResponse::tool_call("GrokBuild:efflab_noop", "{}"),
            ScriptedResponse::text("first complete"),
            ScriptedResponse::tool_call("GrokBuild:efflab_noop", "{}"),
            ScriptedResponse::text("second complete"),
        ]);
        let fixture = Fixture::with_model_url(server.base_url().to_owned());
        let (mut process, mut client) = fixture.spawn_raw();
        let _ = client.request("initialize", initialize_params());
        let session = session_id(&client.request(
            "session/new",
            json!({ "cwd": fixture.session_cwd, "mcpServers": [] }),
        ));

        for (prompt_id, text) in [
            ("prompt-tool-a", "first tool"),
            ("prompt-tool-b", "second tool"),
        ] {
            let prompt_rpc_id = client.send_request(
                "session/prompt",
                prompt_params(
                    &session,
                    json!([{ "type": "text", "text": text }]),
                    Some(json!({ "promptId": prompt_id })),
                ),
            );
            server.wait_for_requests(if prompt_id == "prompt-tool-a" { 1 } else { 3 });
            let permission = loop {
                let message = client.read_message();
                if message["method"] == "session/request_permission"
                    || message["method"] == "_x.ai/session/request_permission"
                {
                    break message;
                }
                assert_ne!(
                    message["id"].as_u64(),
                    Some(prompt_rpc_id),
                    "工具 prompt 应先观察到 permission reverse request: {message}"
                );
            };
            let permission_id = permission["id"]
                .as_u64()
                .expect("permission reverse request 必须带数字 id");
            client.send_response(
                permission_id,
                json!({ "outcome": { "outcome": "selected", "optionId": "allow-once" } }),
            );
            let response = client.read_response(prompt_rpc_id);
            assert_eq!(response["result"]["stopReason"], "end_turn");
        }

        let first_ids = tool_call_ids(&client.raw_lines, "prompt-tool-a");
        let second_ids = tool_call_ids(&client.raw_lines, "prompt-tool-b");
        assert_eq!(
            first_ids.len(),
            1,
            "first prompt tool call wire: {:?}",
            client.raw_lines
        );
        assert_eq!(
            second_ids.len(),
            1,
            "second prompt tool call wire: {:?}",
            client.raw_lines
        );
        assert_ne!(first_ids[0], second_ids[0], "跨 prompt 工具 id 不得碰撞");

        server.wait_for_requests(4);
        let bodies = server.request_bodies();
        assert_eq!(bodies.len(), 4, "两个工具 prompt 应各产生两次模型调用");
        let second_prompt_messages = bodies[2]["messages"]
            .as_array()
            .expect("第二 prompt 模型请求必须包含 messages");
        let assistant_tool_call = second_prompt_messages
            .iter()
            .find(|message| message["role"] == "assistant" && message["tool_calls"].is_array())
            .expect("第二 prompt transcript 必须恢复 assistant tool_calls");
        let recovered_id = assistant_tool_call["tool_calls"][0]["id"]
            .as_str()
            .expect("恢复的 assistant tool_call 必须有 id");
        assert_eq!(
            assistant_tool_call["tool_calls"][0]["function"]["name"],
            "GrokBuild:efflab_noop"
        );
        assert_eq!(
            assistant_tool_call["tool_calls"][0]["function"]["arguments"],
            "{}"
        );
        assert!(second_prompt_messages.iter().any(|message| {
            message["role"] == "tool"
                && message["tool_call_id"] == recovered_id
                && message["content"] == "efflab noop completed"
        }));
        assert_jsonrpc_lines(&client.raw_lines);
        process.finish_raw(&mut client, "tool transcript recovery");
    }

    /// 已存在但 idle 的 session 收到 cancel 时不得污染下一次合法 prompt。
    #[test]
    fn idle_session_cancel_does_not_cancel_next_prompt() {
        let server = ScriptedL3b::new([ScriptedResponse::text("idle cancel must not apply")]);
        let fixture = Fixture::with_model_url(server.base_url().to_owned()).with_test_seam();
        let seam = fixture.test_seam.as_deref().expect("测试 seam 必须存在");
        let (mut process, mut client) = fixture.spawn_raw();
        let _ = client.request("initialize", initialize_params());
        let session = session_id(&client.request(
            "session/new",
            json!({ "cwd": fixture.session_cwd, "mcpServers": [] }),
        ));
        client.send_notification("session/cancel", json!({ "sessionId": session }));
        // ACP dispatcher 独立派生 cancel handler；stdin 顺序不等于 handler 完成。
        wait_for_seam_event(seam, "cancel_handler_completed");
        let prompt_rpc_id = client.send_request(
            "session/prompt",
            prompt_params(
                &session,
                json!([{ "type": "text", "text": "must run after idle cancel" }]),
                Some(json!({ "promptId": "prompt-after-idle-cancel" })),
            ),
        );
        let response = client.read_response(prompt_rpc_id);
        assert_eq!(response["result"]["stopReason"], "end_turn");
        server.wait_for_requests(1);
        assert_eq!(
            server.model_call_count(),
            1,
            "idle cancel 不得抑制合法 prompt"
        );
        assert_eq!(
            persisted_terminal(&fixture.home, &session, "prompt-after-idle-cancel").as_deref(),
            Some("completed")
        );
        assert_jsonrpc_lines(&client.raw_lines);
        process.finish_raw(&mut client, "idle session cancellation");
    }

    /// 在 prompt reserve 之前到达的 cancel 必须由 admission latch 消费，而不能变成 no-op。
    #[test]
    fn cancel_before_prompt_reserve_is_latched() {
        let server = ScriptedL3b::new([ScriptedResponse::text("must not run")]);
        let fixture = Fixture::with_model_url(server.base_url().to_owned()).with_test_seam();
        let seam = fixture.test_seam.as_deref().expect("测试 seam 必须存在");
        let (mut process, mut client) = fixture.spawn_raw();
        let _ = client.request("initialize", initialize_params());
        let session = session_id(&client.request(
            "session/new",
            json!({ "cwd": fixture.session_cwd, "mcpServers": [] }),
        ));
        enable_seam(seam, "after_prompt_admission");
        let prompt_rpc_id = client.send_request(
            "session/prompt",
            prompt_params(
                &session,
                json!([{ "type": "text", "text": "cancelled before admission" }]),
                Some(json!({ "promptId": "prompt-pre-cancel" })),
            ),
        );
        // seam 在 admission 已创建、reserve 尚未执行的位置暂停 prompt，先证明 cancel 已被同一 epoch 接收。
        wait_for_seam_event(seam, "after_prompt_admission");
        client.send_notification("session/cancel", json!({ "sessionId": session }));
        wait_for_seam_event(seam, "cancel_bound");
        release_seam(seam, "after_prompt_admission");
        let response = client.read_response(prompt_rpc_id);
        assert_eq!(response["result"]["stopReason"], "cancelled");
        assert_eq!(
            server.model_call_count(),
            0,
            "pre-reserve cancel 不得调用模型"
        );
        assert_eq!(
            persisted_terminal(&fixture.home, &session, "prompt-pre-cancel").as_deref(),
            Some("cancelled")
        );
        assert_jsonrpc_lines(&client.raw_lines);
        process.finish_raw(&mut client, "pre-reserve cancellation");
    }

    /// 未加载的真实 session 在确认存在期间收到 cancel 时，取消必须绑定本次 admission。
    #[test]
    fn cancel_during_session_confirmation_is_latched() {
        let server = ScriptedL3b::new([ScriptedResponse::blocked("confirmation cancel")]);
        let fixture = Fixture::with_model_url(server.base_url().to_owned()).with_test_seam();
        let seam = fixture.test_seam.as_deref().expect("测试 seam 必须存在");

        // 第一个进程只创建持久化 session；第二个进程不走 session/load，模拟冷启动直接 prompt。
        let (mut first_process, mut first_client) = fixture.spawn_raw();
        let _ = first_client.request("initialize", initialize_params());
        let session = session_id(&first_client.request(
            "session/new",
            json!({ "cwd": fixture.session_cwd, "mcpServers": [] }),
        ));
        first_process.finish_raw(&mut first_client, "create persisted session");

        let (mut second_process, mut second_client) = fixture.spawn_raw();
        let _ = second_client.request("initialize", initialize_params());
        enable_seam(seam, "before_session_confirmation");
        let prompt_rpc_id = second_client.send_request(
            "session/prompt",
            prompt_params(
                &session,
                json!([{ "type": "text", "text": "cancel while confirming" }]),
                Some(json!({ "promptId": "prompt-confirm-cancel" })),
            ),
        );
        wait_for_seam_event(seam, "before_session_confirmation");
        second_client.send_notification("session/cancel", json!({ "sessionId": session }));
        wait_for_seam_event(seam, "cancel_confirmation_started");
        release_seam(seam, "before_session_confirmation");
        wait_for_seam_event(seam, "cancel_bound");

        // 若 cancel 在确认阶段丢失，模型会进入 blocked stream；测试只等待有限窗口，不允许挂死。
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline && server.model_call_count() == 0 {
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(
            server.model_call_count(),
            0,
            "session 确认期间的 cancel 不得丢失并启动模型请求"
        );
        let response = second_client.read_response(prompt_rpc_id);
        assert_eq!(response["result"]["stopReason"], "cancelled");
        assert_eq!(
            persisted_terminal(&fixture.home, &session, "prompt-confirm-cancel").as_deref(),
            Some("cancelled")
        );
        assert_jsonrpc_lines(&second_client.raw_lines);
        second_process.finish_raw(&mut second_client, "session confirmation cancellation");
    }

    /// prompt 尚未完成 admission 时收到 EOF，也必须取消并落 terminal journal。
    #[test]
    fn stdin_eof_before_prompt_reserve_drains_queued_prompt_terminal() {
        let server = ScriptedL3b::new([ScriptedResponse::blocked("must not reach model")]);
        let fixture = Fixture::with_model_url(server.base_url().to_owned()).with_test_seam();
        let seam = fixture.test_seam.as_deref().expect("测试 seam 必须存在");
        let (mut process, mut client) = fixture.spawn_raw();
        let _ = client.request("initialize", initialize_params());
        let session = session_id(&client.request(
            "session/new",
            json!({ "cwd": fixture.session_cwd, "mcpServers": [] }),
        ));
        enable_seam(seam, "before_prompt_admission");
        let _prompt_rpc_id = client.send_request(
            "session/prompt",
            prompt_params(
                &session,
                json!([{ "type": "text", "text": "EOF before reserve" }]),
                Some(json!({ "promptId": "prompt-eof-before-reserve" })),
            ),
        );
        // 测试 seam 让 prompt handler 在 admission 前停住；先确认 handler 已到目标窗口，再发送 EOF。
        wait_for_seam_event(seam, "before_prompt_admission");
        drop(client.stdin.take());
        wait_for_seam_event(seam, "admission_closed");
        release_seam(seam, "before_prompt_admission");
        let status = process.wait_or_kill();
        assert!(status.success(), "queued prompt EOF 应正常退出: {status:?}");
        assert!(
            seam.join("before_prompt_admission.entered").exists()
                && seam.join("admission_closed.entered").exists(),
            "EOF 前后两个 seam 事件都必须留下确定性证据"
        );
        assert_eq!(
            server.model_call_count(),
            0,
            "reserve 前 EOF 不得启动模型调用"
        );
        assert_eq!(
            persisted_terminal(&fixture.home, &session, "prompt-eof-before-reserve").as_deref(),
            Some("cancelled"),
            "queued prompt 必须在 EOF 清理前落 cancelled terminal"
        );
    }

    /// stdin EOF 取消阻塞模型后，必须在有界 drain 内完成 cancelled journal，再退出。
    #[test]
    fn stdin_eof_drains_blocked_prompt_terminal_before_exit() {
        let server = ScriptedL3b::new([ScriptedResponse::blocked("partial")]);
        let fixture = Fixture::with_model_url(server.base_url().to_owned());
        let (mut process, mut client) = fixture.spawn_raw();
        let _ = client.request("initialize", initialize_params());
        let session = session_id(&client.request(
            "session/new",
            json!({ "cwd": fixture.session_cwd, "mcpServers": [] }),
        ));
        let _prompt_rpc_id = client.send_request(
            "session/prompt",
            prompt_params(
                &session,
                json!([{ "type": "text", "text": "EOF drain" }]),
                Some(json!({ "promptId": "prompt-eof-drain" })),
            ),
        );
        server.wait_for_requests(1);
        drop(client.stdin.take());
        let status = process.wait_or_kill();
        assert!(status.success(), "stdin EOF 应正常退出: {status:?}");
        assert_eq!(
            persisted_terminal(&fixture.home, &session, "prompt-eof-drain").as_deref(),
            Some("cancelled"),
            "进程退出前必须落 cancelled terminal"
        );
        let _ = process.stderr_text();
    }

    /// noop 工具的 permission 必须使用精确 qualified 名和当前 promptId。
    #[test]
    fn noop_tool_permission_uses_qualified_name_and_requires_allow_once() {
        let server = ScriptedL3b::new([
            ScriptedResponse::tool_call("GrokBuild:efflab_noop", "{}"),
            ScriptedResponse::text("after tool"),
        ]);
        let fixture = Fixture::with_model_url(server.base_url().to_owned()).with_test_seam();
        let seam = fixture.test_seam.as_deref().expect("测试 seam 必须存在");
        let (mut process, mut client) = fixture.spawn_raw();
        let _ = client.request("initialize", initialize_params());
        let session = session_id(&client.request(
            "session/new",
            json!({ "cwd": fixture.session_cwd, "mcpServers": [] }),
        ));
        let prompt_rpc_id = client.send_request(
            "session/prompt",
            prompt_params(
                &session,
                json!([{ "type": "text", "text": "use noop" }]),
                Some(json!({ "promptId": "prompt-tool" })),
            ),
        );
        server.wait_for_requests(1);

        // tool_call 通知先到；直到观察到真实 ACP reverse request 才回复 permission。
        let permission = loop {
            let message = client.read_message();
            if message["method"] == "session/request_permission"
                || message["method"] == "_x.ai/session/request_permission"
            {
                break message;
            }
            assert_ne!(
                message["id"].as_u64(),
                Some(prompt_rpc_id),
                "模型工具请求在 permission 前不应直接结束 prompt: {message}"
            );
        };
        assert_eq!(
            permission["params"]["toolCall"]["title"],
            "GrokBuild:efflab_noop"
        );
        assert_eq!(permission["params"]["_meta"]["promptId"], "prompt-tool");
        assert_eq!(
            permission["params"]["options"],
            json!([
                {
                    "optionId": "allow-once",
                    "name": "Allow once",
                    "kind": "allow_once"
                },
                {
                    "optionId": "reject-once",
                    "name": "Reject once",
                    "kind": "reject_once"
                }
            ])
        );
        let permission_rpc_id = permission["id"]
            .as_u64()
            .expect("permission reverse request 必须带数字 id");
        client.send_response(
            permission_rpc_id,
            json!({ "outcome": { "outcome": "selected", "optionId": "allow-once" } }),
        );

        let response = client.read_response(prompt_rpc_id);
        assert_eq!(response["result"]["stopReason"], "end_turn");
        assert_eq!(server.model_call_count(), 2);
        assert_eq!(
            execution_count(seam),
            1,
            "allow-once 后执行点应恰好调用一次"
        );
        assert_jsonrpc_lines(&client.raw_lines);
        process.finish_raw(&mut client, "noop permission");
    }

    /// 同一个 prompt 的多轮工具 transcript 必须在重启后按 round 成对恢复。
    #[test]
    fn same_prompt_multi_round_tool_transcript_survives_restart() {
        let server = ScriptedL3b::new([
            ScriptedResponse::tool_call("GrokBuild:efflab_noop", "{ }"),
            ScriptedResponse::tool_call("GrokBuild:efflab_noop", "{ }"),
            ScriptedResponse::text("first complete"),
            ScriptedResponse::text("after restart"),
        ]);
        let fixture = Fixture::with_model_url(server.base_url().to_owned());
        let (mut first_process, mut first_client) = fixture.spawn_raw();
        let _ = first_client.request("initialize", initialize_params());
        let session = session_id(&first_client.request(
            "session/new",
            json!({ "cwd": fixture.session_cwd, "mcpServers": [] }),
        ));
        let first_prompt = first_client.send_request(
            "session/prompt",
            prompt_params(
                &session,
                json!([{ "type": "text", "text": "two tool rounds" }]),
                Some(json!({ "promptId": "prompt-two-rounds" })),
            ),
        );

        for expected_requests in [1, 2] {
            server.wait_for_requests(expected_requests);
            let permission = loop {
                let message = first_client.read_message();
                if message["method"] == "session/request_permission"
                    || message["method"] == "_x.ai/session/request_permission"
                {
                    break message;
                }
                assert_ne!(
                    message["id"].as_u64(),
                    Some(first_prompt),
                    "多轮工具应在 prompt response 前完成 permission: {message}"
                );
            };
            let permission_id = permission["id"]
                .as_u64()
                .expect("permission reverse request 必须带数字 id");
            first_client.send_response(
                permission_id,
                json!({ "outcome": { "outcome": "selected", "optionId": "allow-once" } }),
            );
        }
        let first_response = first_client.read_response(first_prompt);
        assert_eq!(first_response["result"]["stopReason"], "end_turn");
        first_process.finish_raw(&mut first_client, "multi-round first process");

        // 第二个 sidecar 进程通过 session/load 读取同一份 journal，再发起新 prompt。
        let (mut second_process, mut second_client) = fixture.spawn_raw();
        let _ = second_client.request("initialize", initialize_params());
        let load_id = second_client.send_request(
            "session/load",
            json!({
                "sessionId": session,
                "cwd": fixture.session_cwd,
                "mcpServers": []
            }),
        );
        let load_response = second_client.read_response(load_id);
        assert!(load_response["result"].is_object());

        let second_prompt = second_client.send_request(
            "session/prompt",
            prompt_params(
                &session,
                json!([{ "type": "text", "text": "continue after restart" }]),
                Some(json!({ "promptId": "prompt-after-restart" })),
            ),
        );
        server.wait_for_requests(4);
        let second_response = second_client.read_response(second_prompt);
        assert_eq!(second_response["result"]["stopReason"], "end_turn");

        let bodies = server.request_bodies();
        let messages = bodies
            .get(3)
            .and_then(|body| body["messages"].as_array())
            .expect("重启后的模型请求必须包含 messages");
        let assistant_rounds = messages
            .iter()
            .filter(|message| message["role"] == "assistant" && message["tool_calls"].is_array())
            .collect::<Vec<_>>();
        assert_eq!(
            assistant_rounds.len(),
            2,
            "同一 prompt 的两轮工具必须恢复为两个 assistant tool_calls 消息: {messages:?}"
        );
        let mut paired_ids = Vec::new();
        for assistant in assistant_rounds {
            let calls = assistant["tool_calls"]
                .as_array()
                .expect("assistant tool_calls 必须是数组");
            assert_eq!(calls.len(), 1, "每轮 fixture 只产生一个工具调用");
            let call = &calls[0];
            assert_eq!(
                call["function"]["arguments"], "{}",
                "恢复只允许固定安全参数"
            );
            let call_id = call["id"].as_str().expect("恢复调用必须有 id");
            paired_ids.push(call_id.to_owned());
            let assistant_index = messages
                .iter()
                .position(|candidate| candidate == assistant)
                .expect("assistant 消息必须位于 transcript 中");
            let result = messages
                .get(assistant_index + 1)
                .expect("assistant tool_calls 后必须紧跟 tool result");
            assert_eq!(result["role"], "tool");
            assert_eq!(result["tool_call_id"], call_id);
            assert_eq!(result["content"], "efflab noop completed");
        }
        assert_ne!(paired_ids[0], paired_ids[1], "多轮恢复的工具 id 必须唯一");
        let records_path = fixture
            .home
            .join("efflab-sessions")
            .join("v1")
            .join(&session)
            .join("records.jsonl");
        let records = fs::read_to_string(records_path).expect("多轮工具 journal 必须可读取");
        assert!(
            !records.contains("arguments") && !records.contains("{ }"),
            "journal 只能保存安全工具形状，不得保存模型原始参数: {records:?}"
        );
        assert_jsonrpc_lines(&second_client.raw_lines);
        second_process.finish_raw(&mut second_client, "multi-round second process");
    }

    /// 同一 Chat Completions 回合的文本与 tool call 恢复时只能出现一份文本，并保持成对顺序。
    #[test]
    fn mixed_text_and_tool_call_recovery_does_not_duplicate_text() {
        let blocked_gate = ResponseGate::new();
        let server = ScriptedL3b::new([
            ScriptedResponse::text_and_tool_call("same-round text", "GrokBuild:efflab_noop", "{ }"),
            // 空文本只让第二次调用保持打开，不追加新的 assistant snapshot；
            // 因而旧恢复逻辑会把混合回合的 snapshot 重复成普通 assistant 消息。
            ScriptedResponse::text_with_gate("", blocked_gate.clone()),
            ScriptedResponse::text("after mixed restart"),
        ]);
        let fixture = Fixture::with_model_url(server.base_url().to_owned()).with_test_seam();
        let seam = fixture.test_seam.as_deref().expect("测试 seam 必须存在");
        let (mut first_process, mut first_client) = fixture.spawn_raw();
        let _ = first_client.request("initialize", initialize_params());
        let session = session_id(&first_client.request(
            "session/new",
            json!({ "cwd": fixture.session_cwd, "mcpServers": [] }),
        ));
        let first_prompt = first_client.send_request(
            "session/prompt",
            prompt_params(
                &session,
                json!([{ "type": "text", "text": "mixed response" }]),
                Some(json!({ "promptId": "prompt-mixed" })),
            ),
        );
        server.wait_for_requests(1);
        let permission = loop {
            let message = first_client.read_message();
            if message["method"] == "session/request_permission"
                || message["method"] == "_x.ai/session/request_permission"
            {
                break message;
            }
            assert_ne!(
                message["id"].as_u64(),
                Some(first_prompt),
                "混合文本工具回合必须先请求 permission: {message}"
            );
        };
        let permission_id = permission["id"]
            .as_u64()
            .expect("permission reverse request 必须带数字 id");
        first_client.send_response(
            permission_id,
            json!({ "outcome": { "outcome": "selected", "optionId": "allow-once" } }),
        );
        server.wait_for_requests(2);
        // 在同一 prompt 的工具 round 已完成、普通 assistant message 尚未产生时取消，
        // 让重启恢复只看到混合 text+tool_calls 这一轮。
        first_client.send_notification("session/cancel", json!({ "sessionId": session }));
        wait_for_seam_event(seam, "cancel_bound");
        blocked_gate.release();
        let first_response = first_client.read_response(first_prompt);
        assert_eq!(first_response["result"]["stopReason"], "cancelled");
        first_process.finish_raw(&mut first_client, "mixed text first process");

        let (mut second_process, mut second_client) = fixture.spawn_raw();
        let _ = second_client.request("initialize", initialize_params());
        let load_id = second_client.send_request(
            "session/load",
            json!({
                "sessionId": session,
                "cwd": fixture.session_cwd,
                "mcpServers": []
            }),
        );
        let load_response = second_client.read_response(load_id);
        assert!(load_response["result"].is_object());
        let second_prompt = second_client.send_request(
            "session/prompt",
            prompt_params(
                &session,
                json!([{ "type": "text", "text": "continue mixed" }]),
                Some(json!({ "promptId": "prompt-mixed-restart" })),
            ),
        );
        server.wait_for_requests(3);
        let second_response = second_client.read_response(second_prompt);
        assert_eq!(second_response["result"]["stopReason"], "end_turn");
        let bodies = server.request_bodies();
        let messages = bodies
            .get(2)
            .and_then(|body| body["messages"].as_array())
            .expect("重启后的模型请求必须包含 messages");
        let assistant_tool_index = messages
            .iter()
            .position(|message| message["role"] == "assistant" && message["tool_calls"].is_array())
            .expect("恢复必须包含 assistant tool_calls 消息");
        let assistant_tool = &messages[assistant_tool_index];
        assert_eq!(assistant_tool["content"], "same-round text");
        assert_eq!(
            messages
                .iter()
                .filter(|message| {
                    message["role"] == "assistant" && message["content"] == "same-round text"
                })
                .count(),
            1,
            "同一 round 的文本不得作为普通 assistant message 重复恢复"
        );
        let call_id = assistant_tool["tool_calls"][0]["id"]
            .as_str()
            .expect("恢复的 tool call 必须有 id");
        assert_eq!(
            assistant_tool["tool_calls"][0]["function"]["arguments"],
            "{}"
        );
        let result = messages
            .get(assistant_tool_index + 1)
            .expect("assistant tool_calls 后必须紧跟 tool result");
        assert_eq!(result["role"], "tool");
        assert_eq!(result["tool_call_id"], call_id);
        assert_eq!(result["content"], "efflab noop completed");
        assert_jsonrpc_lines(&second_client.raw_lines);
        second_process.finish_raw(&mut second_client, "mixed text restart");
    }

    /// 正常消费 [DONE] 的混合文本+tool 不得重复恢复，且后续普通 assistant 文本必须保留。
    #[test]
    fn mixed_text_and_tool_call_normal_completion_recovery_does_not_duplicate_text() {
        let server = ScriptedL3b::new([
            ScriptedResponse::text_and_tool_call("same-round text", "GrokBuild:efflab_noop", "{ }"),
            // 后续普通 assistant 故意复用工具 round 文本，验证恢复不能按全局字符串丢弃它。
            ScriptedResponse::text("same-round text"),
            ScriptedResponse::text("after mixed restart"),
        ]);
        let fixture = Fixture::with_model_url(server.base_url().to_owned());
        let (mut first_process, mut first_client) = fixture.spawn_raw();
        let _ = first_client.request("initialize", initialize_params());
        let session = session_id(&first_client.request(
            "session/new",
            json!({ "cwd": fixture.session_cwd, "mcpServers": [] }),
        ));
        let first_prompt = first_client.send_request(
            "session/prompt",
            prompt_params(
                &session,
                json!([{ "type": "text", "text": "mixed response" }]),
                Some(json!({ "promptId": "prompt-mixed-complete" })),
            ),
        );
        server.wait_for_requests(1);
        let permission = loop {
            let message = first_client.read_message();
            if message["method"] == "session/request_permission"
                || message["method"] == "_x.ai/session/request_permission"
            {
                break message;
            }
            assert_ne!(
                message["id"].as_u64(),
                Some(first_prompt),
                "混合文本工具回合必须先请求 permission: {message}"
            );
        };
        let permission_id = permission["id"]
            .as_u64()
            .expect("permission reverse request 必须带数字 id");
        first_client.send_response(
            permission_id,
            json!({ "outcome": { "outcome": "selected", "optionId": "allow-once" } }),
        );
        server.wait_for_requests(2);
        let first_response = first_client.read_response(first_prompt);
        assert_eq!(first_response["result"]["stopReason"], "end_turn");
        assert_eq!(
            persisted_terminal(&fixture.home, &session, "prompt-mixed-complete").as_deref(),
            Some("completed")
        );
        first_process.finish_raw(&mut first_client, "mixed complete first process");

        let (mut second_process, mut second_client) = fixture.spawn_raw();
        let _ = second_client.request("initialize", initialize_params());
        let load_id = second_client.send_request(
            "session/load",
            json!({
                "sessionId": session,
                "cwd": fixture.session_cwd,
                "mcpServers": []
            }),
        );
        let load_response = second_client.read_response(load_id);
        assert!(load_response["result"].is_object());
        let second_prompt = second_client.send_request(
            "session/prompt",
            prompt_params(
                &session,
                json!([{ "type": "text", "text": "continue mixed" }]),
                Some(json!({ "promptId": "prompt-mixed-complete-restart" })),
            ),
        );
        server.wait_for_requests(3);
        let second_response = second_client.read_response(second_prompt);
        assert_eq!(second_response["result"]["stopReason"], "end_turn");
        let bodies = server.request_bodies();
        let messages = bodies
            .get(2)
            .and_then(|body| body["messages"].as_array())
            .expect("重启后的模型请求必须包含 messages");
        let assistant_tool_index = messages
            .iter()
            .position(|message| message["role"] == "assistant" && message["tool_calls"].is_array())
            .expect("恢复必须包含 assistant tool_calls 消息");
        let assistant_tool = &messages[assistant_tool_index];
        assert_eq!(assistant_tool["content"], "same-round text");
        assert_eq!(
            messages
                .iter()
                .filter(|message| {
                    message["role"] == "assistant" && message["content"] == "same-round text"
                })
                .count(),
            2,
            "工具 round 文本与相同文本的后续普通 assistant snapshot 都必须保留: {messages:?}"
        );
        let call_id = assistant_tool["tool_calls"][0]["id"]
            .as_str()
            .expect("恢复的 tool call 必须有 id");
        assert_eq!(
            assistant_tool["tool_calls"][0]["function"]["arguments"],
            "{}"
        );
        let result = messages
            .get(assistant_tool_index + 1)
            .expect("assistant tool_calls 后必须紧跟 tool result");
        assert_eq!(result["role"], "tool");
        assert_eq!(result["tool_call_id"], call_id);
        assert_eq!(result["content"], "efflab noop completed");
        let follow_up = messages
            .iter()
            .skip(assistant_tool_index + 2)
            .find(|message| message["role"] == "assistant")
            .expect("后续普通 assistant 文本不得被去重误删");
        assert_eq!(
            follow_up["content"], "same-round text",
            "相同文本的后续普通 assistant 必须保持独立消息: {messages:?}"
        );
        assert!(
            follow_up.get("tool_calls").is_none()
                || follow_up["tool_calls"].is_null()
                || follow_up["tool_calls"]
                    .as_array()
                    .is_some_and(Vec::is_empty)
        );
        let records_path = fixture
            .home
            .join("efflab-sessions")
            .join("v1")
            .join(&session)
            .join("records.jsonl");
        let records = fs::read_to_string(records_path).expect("混合完成 journal 必须可读取");
        assert!(
            !records.contains("arguments") && !records.contains("{ }"),
            "journal 只能保存安全工具形状，不得保存模型原始参数: {records:?}"
        );
        assert_jsonrpc_lines(&second_client.raw_lines);
        second_process.finish_raw(&mut second_client, "mixed complete restart");
    }

    /// 两个连续 turn 的 assistant snapshot 必须分别绑定自己的 promptId。
    #[test]
    fn two_prompts_do_not_share_assistant_snapshot() {
        let server = ScriptedL3b::new([ScriptedResponse::text("A"), ScriptedResponse::text("B")]);
        let fixture = Fixture::with_model_url(server.base_url().to_owned());
        let (mut process, mut client) = fixture.spawn();
        let _ = initialize(&mut client);
        let session = session_id(&new_session(&mut client, &fixture.session_cwd));

        let first = client
            .request(
                "session/prompt",
                prompt_params(
                    &session,
                    json!([{ "type": "text", "text": "first" }]),
                    Some(json!({ "promptId": "prompt-a" })),
                ),
                REQUEST_TIMEOUT,
            )
            .expect("第一个 prompt 必须返回 response");
        assert_eq!(first["result"]["stopReason"], "end_turn");

        let second = client
            .request(
                "session/prompt",
                prompt_params(
                    &session,
                    json!([{ "type": "text", "text": "second" }]),
                    Some(json!({ "promptId": "prompt-b" })),
                ),
                REQUEST_TIMEOUT,
            )
            .expect("第二个 prompt 必须返回 response");
        assert_eq!(second["result"]["stopReason"], "end_turn");
        server.wait_for_requests(2);

        let lines = client.raw_lines();
        assert_eq!(assistant_snapshots(&lines, "prompt-a"), ["A"]);
        assert_eq!(assistant_snapshots(&lines, "prompt-b"), ["B"]);
        assert_jsonrpc_lines(&lines);
        process.finish(&mut client, "prompt snapshot isolation");
    }

    /// 取消中的 turn 只允许一次模型调用，并必须在 prompt response 前写入 cancelled terminal。
    #[test]
    fn cancel_writes_cancelled_terminal_and_does_not_retry() {
        let server = ScriptedL3b::new([ScriptedResponse::blocked("partial")]);
        let fixture = Fixture::with_model_url(server.base_url().to_owned());
        let (mut process, mut client) = fixture.spawn_raw();
        let _ = client.request("initialize", initialize_params());
        let session = session_id(&client.request(
            "session/new",
            json!({ "cwd": fixture.session_cwd, "mcpServers": [] }),
        ));
        let prompt_rpc_id = client.send_request(
            "session/prompt",
            prompt_params(
                &session,
                json!([{ "type": "text", "text": "cancel-me" }]),
                Some(json!({ "promptId": "prompt-cancel" })),
            ),
        );
        server.wait_for_requests(1);
        client.send_notification("session/cancel", json!({ "sessionId": session }));
        client.send_notification("session/cancel", json!({ "sessionId": session }));

        let response = client.read_response(prompt_rpc_id);
        assert_eq!(response["result"]["stopReason"], "cancelled");
        assert_eq!(server.model_call_count(), 1);
        assert_eq!(
            persisted_terminal(&fixture.home, &session, "prompt-cancel").as_deref(),
            Some("cancelled")
        );
        assert_eq!(
            persisted_terminal_count(&fixture.home, &session, "prompt-cancel"),
            1,
            "重复 cancel 不得重复追加 terminal journal"
        );
        assert_jsonrpc_lines(&client.raw_lines);
        process.finish_raw(&mut client, "cancelled terminal");
    }

    /// completion 已被消费并提交 terminal 后，迟到 cancel 不得改写终态，后续 prompt 仍可完成。
    #[test]
    fn completion_first_wins_over_late_cancel() {
        let gate = ResponseGate::new();
        let server = ScriptedL3b::new([
            ScriptedResponse::text_with_gate("completion first", gate.clone()),
            ScriptedResponse::text("after completion first"),
        ]);
        let fixture = Fixture::with_model_url(server.base_url().to_owned()).with_test_seam();
        let seam = fixture.test_seam.as_deref().expect("测试 seam 必须存在");
        enable_seam(seam, "after_terminal_claim");
        let (mut process, mut client) = fixture.spawn_raw();
        let _ = client.request("initialize", initialize_params());
        let session = session_id(&client.request(
            "session/new",
            json!({ "cwd": fixture.session_cwd, "mcpServers": [] }),
        ));
        let prompt_rpc_id = client.send_request(
            "session/prompt",
            prompt_params(
                &session,
                json!([{ "type": "text", "text": "completion first" }]),
                Some(json!({ "promptId": "prompt-completion-first" })),
            ),
        );
        server.wait_for_requests(1);
        loop {
            let message = client.read_message();
            assert_ne!(
                message["id"].as_u64(),
                Some(prompt_rpc_id),
                "Done 释放前不得返回 prompt response: {message}"
            );
            if message["method"] == "session/update"
                && message["params"]["_meta"]["promptId"] == "prompt-completion-first"
                && message["params"]["update"]["sessionUpdate"] == "agent_message_chunk"
            {
                break;
            }
        }
        gate.release();
        wait_for_seam_event(seam, "model_done_consumed");
        wait_for_seam_event(seam, "after_terminal_claim");
        assert_eq!(
            persisted_terminal(&fixture.home, &session, "prompt-completion-first").as_deref(),
            Some("completed")
        );
        client.send_notification("session/cancel", json!({ "sessionId": session }));
        wait_for_seam_event(seam, "cancel_bound");
        wait_for_seam_event(seam, "cancel_handler_completed");
        assert_eq!(
            persisted_terminal(&fixture.home, &session, "prompt-completion-first").as_deref(),
            Some("completed")
        );
        assert_eq!(
            persisted_terminal_count(&fixture.home, &session, "prompt-completion-first"),
            1,
            "completion-first 的迟到 cancel 不得追加或改写 terminal"
        );
        release_seam(seam, "after_terminal_claim");
        let response = client.read_response(prompt_rpc_id);
        assert_eq!(response["result"]["stopReason"], "end_turn");

        let next_prompt = client.send_request(
            "session/prompt",
            prompt_params(
                &session,
                json!([{ "type": "text", "text": "after completion first" }]),
                Some(json!({ "promptId": "prompt-after-completion-first" })),
            ),
        );
        let next_response = client.read_response(next_prompt);
        assert_eq!(next_response["result"]["stopReason"], "end_turn");
        assert_eq!(
            persisted_terminal(&fixture.home, &session, "prompt-after-completion-first").as_deref(),
            Some("completed")
        );
        assert_eq!(
            persisted_terminal_count(&fixture.home, &session, "prompt-completion-first"),
            1
        );
        assert_jsonrpc_lines(&client.raw_lines);
        process.finish_raw(&mut client, "completion-first late cancellation");
    }

    /// cancel 在 [DONE] 已消费但尚未提交 terminal 时到达，迟到 completion 不得改写 cancelled。
    #[test]
    fn cancel_first_wins_over_late_completion() {
        let gate = ResponseGate::new();
        let server = ScriptedL3b::new([
            ScriptedResponse::text_with_gate("cancel first", gate.clone()),
            ScriptedResponse::text("after cancel first"),
        ]);
        let fixture = Fixture::with_model_url(server.base_url().to_owned()).with_test_seam();
        let seam = fixture.test_seam.as_deref().expect("测试 seam 必须存在");
        enable_seam(seam, "after_model_done");
        let (mut process, mut client) = fixture.spawn_raw();
        let _ = client.request("initialize", initialize_params());
        let session = session_id(&client.request(
            "session/new",
            json!({ "cwd": fixture.session_cwd, "mcpServers": [] }),
        ));
        let prompt_rpc_id = client.send_request(
            "session/prompt",
            prompt_params(
                &session,
                json!([{ "type": "text", "text": "cancel first" }]),
                Some(json!({ "promptId": "prompt-cancel-first" })),
            ),
        );
        server.wait_for_requests(1);
        loop {
            let message = client.read_message();
            assert_ne!(
                message["id"].as_u64(),
                Some(prompt_rpc_id),
                "cancel-first 的模型 Done 释放前不得返回 prompt response: {message}"
            );
            if message["method"] == "session/update"
                && message["params"]["_meta"]["promptId"] == "prompt-cancel-first"
                && message["params"]["update"]["sessionUpdate"] == "agent_message_chunk"
            {
                break;
            }
        }
        gate.release();
        wait_for_seam_event(seam, "model_done_consumed");
        wait_for_seam_event(seam, "after_model_done");
        client.send_notification("session/cancel", json!({ "sessionId": session }));
        wait_for_seam_event(seam, "cancel_bound");
        wait_for_seam_event(seam, "cancel_handler_completed");
        release_seam(seam, "after_model_done");
        let response = client.read_response(prompt_rpc_id);
        assert_eq!(response["result"]["stopReason"], "cancelled");
        assert_eq!(
            persisted_terminal(&fixture.home, &session, "prompt-cancel-first").as_deref(),
            Some("cancelled")
        );
        assert_eq!(
            persisted_terminal_count(&fixture.home, &session, "prompt-cancel-first"),
            1,
            "cancel-first 即使收到迟到 Done 也只能有一个 terminal"
        );

        let next_prompt = client.send_request(
            "session/prompt",
            prompt_params(
                &session,
                json!([{ "type": "text", "text": "after cancel first" }]),
                Some(json!({ "promptId": "prompt-after-cancel-first" })),
            ),
        );
        let next_response = client.read_response(next_prompt);
        assert_eq!(next_response["result"]["stopReason"], "end_turn");
        assert_eq!(
            persisted_terminal(&fixture.home, &session, "prompt-after-cancel-first").as_deref(),
            Some("completed")
        );
        assert_eq!(
            persisted_terminal_count(&fixture.home, &session, "prompt-cancel-first"),
            1
        );
        assert_jsonrpc_lines(&client.raw_lines);
        process.finish_raw(&mut client, "cancel-first late completion");
    }

    /// prompt/cancel/list 交错到达时，gateway 仍按完整 ACP JSON-RPC 行单写 stdout。
    #[test]
    fn concurrent_prompt_cancel_and_list_keep_complete_jsonrpc_lines() {
        let server = ScriptedL3b::new([ScriptedResponse::blocked("partial")]);
        let fixture = Fixture::with_model_url(server.base_url().to_owned());
        let (mut process, mut client) = fixture.spawn_raw();
        let _ = client.request("initialize", initialize_params());
        let session = session_id(&client.request(
            "session/new",
            json!({ "cwd": fixture.session_cwd, "mcpServers": [] }),
        ));
        let prompt_rpc_id = client.send_request(
            "session/prompt",
            prompt_params(
                &session,
                json!([{ "type": "text", "text": "concurrent" }]),
                Some(json!({ "promptId": "prompt-concurrent" })),
            ),
        );
        server.wait_for_requests(1);
        client.send_notification("session/cancel", json!({ "sessionId": session }));
        let list_rpc_id =
            client.send_request("session/list", session_list_params(&fixture.session_cwd));

        let prompt_response = client.read_response(prompt_rpc_id);
        let list_response = client.read_response(list_rpc_id);
        assert_eq!(prompt_response["result"]["stopReason"], "cancelled");
        assert!(list_response["result"]["sessions"].is_array());
        assert_eq!(
            client
                .raw_lines
                .iter()
                .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                .filter(|value| value["id"].as_u64() == Some(prompt_rpc_id))
                .count(),
            1,
            "prompt result 必须 exactly-once"
        );
        assert_jsonrpc_lines(&client.raw_lines);
        process.finish_raw(&mut client, "concurrent prompt cancel list");
    }

    /// 唯一允许的扩展是 `_x.ai/mcp/list`，其 result 必须满足 Host catalog parser 的嵌套形状。
    #[test]
    fn mcp_list_uses_real_wire_extension_and_nested_result_shape() {
        let fixture = Fixture::new();
        let (mut process, mut client) = fixture.spawn();
        let _ = initialize(&mut client);
        let created = new_session(&mut client, &fixture.session_cwd);
        let response = client
            .request(
                "_x.ai/mcp/list",
                serde_json::json!({ "sessionId": session_id(&created) }),
                REQUEST_TIMEOUT,
            )
            .expect("_x.ai/mcp/list 必须成功");
        assert!(response["result"]["result"]["servers"].is_array());
        assert!(
            response["result"]["result"]["servers"]
                .as_array()
                .is_some_and(Vec::is_empty)
        );
        assert_jsonrpc_lines(&client.raw_lines());
        process.finish(&mut client, "mcp/list");
    }

    /// 未知扩展和旧的扩展 session/list 都必须返回 method_not_found，不得污染 stdout。
    #[test]
    fn unknown_ext_method_is_jsonrpc_error_not_stdout_log() {
        let fixture = Fixture::new();
        let (mut process, mut client) = fixture.spawn();
        let _ = initialize(&mut client);
        for method in ["_x.ai/unknown", "_x.ai/session/list"] {
            let error = client
                .request(method, serde_json::json!({}), REQUEST_TIMEOUT)
                .expect_err("未知扩展必须返回 JSON-RPC error");
            let AcpError::RpcError(error) = error else {
                panic!("未知扩展应返回 JSON-RPC error，实际: {error:?}");
            };
            assert_eq!(error["code"], -32601);
            assert_eq!(error["message"], "Method not found");
        }
        assert_jsonrpc_lines(&client.raw_lines());
        process.finish(&mut client, "unknown extension");
    }

    /// ACP EOF 必须关闭 gateway 并以零退出码结束。
    #[test]
    fn stdin_eof_triggers_clean_exit_zero() {
        let fixture = Fixture::new();
        let (mut process, mut client) = fixture.spawn();
        let _ = initialize(&mut client);
        assert_jsonrpc_lines(&client.raw_lines());
        process.finish(&mut client, "stdin EOF");
    }

    /// stdout 关闭后，ACP transport 必须以 runtime failure 结束，而不是继续读取请求。
    #[test]
    fn closed_stdout_triggers_runtime_failure() {
        let fixture = Fixture::new();
        let (mut process, mut client) = fixture.spawn_raw();
        let _ = client.request("initialize", initialize_params());

        // 先关闭客户端读取端，下一次 response 写入应触发统一清理。
        client.close_stdout();
        client.send_request("session/list", session_list_params(&fixture.session_cwd));

        let status = process.wait_or_kill();
        assert!(
            !status.success(),
            "stdout 写入失败必须返回非零退出码: {status:?}"
        );
        let stderr = process.stderr_text();
        assert!(
            !stderr.contains("Broken pipe") && !stderr.contains("broken pipe"),
            "stderr 不得泄露底层写入错误正文: {stderr:?}"
        );
    }

    fn set_mode(path: &Path, mode: u32) {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(mode)).expect("设置 fixture 权限");
    }
}
