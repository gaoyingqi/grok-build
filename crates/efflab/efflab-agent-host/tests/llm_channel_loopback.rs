//! LLM Channel 与 L3b 回环出口的集成契约测试。
//!
//! 测试只使用本地 TCP 上游，验证绑定令牌、凭据替换、流式转发和真实 sidecar 启动
//! 的安全边界；测试诊断中绝不输出用户 Key 或 binding token。

#![cfg(unix)]

use std::fs;
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;
use efflab_agent_host::{
    ApprovedMcpSpec, HostApp, HostRuntimeConfig, L3bLoopback, L3bRuntimeConfig, LlmChannelConfig,
    LlmChannelError, LlmChannelKind, LlmChannelManager, LlmChannelService, LlmSecretSlot,
    MAX_L3B_REQUEST_BODY_BYTES, ScopeId, SealedSecret, SecretGuard, SetLlmChannelRequest,
};

/// 本地网络测试的单次等待上限，避免回环异常时无限阻塞测试进程。
const TEST_TIMEOUT: Duration = Duration::from_secs(3);

/// 提供可计数密封/解封行为的产品端口假实现。
struct FakeApp {
    config: Mutex<LlmChannelConfig>,
    persist_calls: AtomicUsize,
    seal_calls: AtomicUsize,
    unseal_calls: AtomicUsize,
}

impl FakeApp {
    /// 用固定的 BYOK 配置构造测试产品端口；该 Key 仅是本地测试占位符。
    fn byok(base_url: String, model_id: &str, key: &str) -> Self {
        Self {
            config: Mutex::new(LlmChannelConfig::Byok {
                base_url,
                model_id: model_id.to_string(),
                api_key: SealedSecret::new(key.as_bytes().to_vec()),
            }),
            persist_calls: AtomicUsize::new(0),
            seal_calls: AtomicUsize::new(0),
            unseal_calls: AtomicUsize::new(0),
        }
    }

    /// 构造尚未设置 Channel 的产品端口，验证首次配置和 SSRF 输入边界。
    fn unconfigured() -> Self {
        Self {
            config: Mutex::new(LlmChannelConfig::Unconfigured),
            persist_calls: AtomicUsize::new(0),
            seal_calls: AtomicUsize::new(0),
            unseal_calls: AtomicUsize::new(0),
        }
    }
}

impl HostApp for FakeApp {
    fn app_id(&self) -> &str {
        "loopback-test-app"
    }

    fn persist_llm_channel(&self, cfg: &LlmChannelConfig) -> Result<()> {
        *self.config.lock().expect("测试配置锁不应中毒") = cfg.clone();
        self.persist_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn load_llm_channel(&self) -> Result<LlmChannelConfig> {
        Ok(self.config.lock().expect("测试配置锁不应中毒").clone())
    }

    fn seal_secret(&self, plain: &[u8]) -> Result<SealedSecret> {
        self.seal_calls.fetch_add(1, Ordering::SeqCst);
        Ok(SealedSecret::new(plain.to_vec()))
    }

    fn unseal_secret(&self, sealed: &SealedSecret) -> Result<SecretGuard> {
        self.unseal_calls.fetch_add(1, Ordering::SeqCst);
        Ok(SecretGuard::new(sealed.as_bytes().to_vec()))
    }

    /// 测试产品显式只声明 BYOK 槽；Relay 不能误用同一通用密封实现。
    fn seal_llm_secret(&self, slot: LlmSecretSlot, plain: &[u8]) -> Result<SealedSecret> {
        match slot {
            LlmSecretSlot::Byok => self.seal_secret(plain),
            LlmSecretSlot::Relay => Err(anyhow::anyhow!("测试产品未声明 Relay 密封槽")),
        }
    }

    /// 测试产品显式只声明 BYOK 槽；Relay 不能误用同一通用解封实现。
    fn unseal_llm_secret(&self, slot: LlmSecretSlot, sealed: &SealedSecret) -> Result<SecretGuard> {
        match slot {
            LlmSecretSlot::Byok => self.unseal_secret(sealed),
            LlmSecretSlot::Relay => Err(anyhow::anyhow!("测试产品未声明 Relay 解封槽")),
        }
    }

    fn mcp_for_scope(&self, _scope: &ScopeId) -> Result<ApprovedMcpSpec> {
        Ok(ApprovedMcpSpec::default())
    }
}

/// 按策略构造已加载的 Channel manager，避免每个测试复制安全配置。
fn manager(app: Arc<FakeApp>, allow_loopback_llm: bool) -> Arc<LlmChannelManager> {
    Arc::new(
        LlmChannelManager::new(app, allow_loopback_llm)
            .expect("测试 BYOK 配置必须可加载为 Channel manager"),
    )
}

/// 启动一口 IPv4 L3b 回环服务；真实本地上游测试显式允许 loopback。
fn loopback(manager: Arc<LlmChannelManager>, allow_loopback_llm: bool) -> L3bLoopback {
    L3bLoopback::start(
        manager,
        L3bRuntimeConfig {
            allow_loopback_llm,
            ..L3bRuntimeConfig::default()
        },
    )
    .expect("测试 L3b 回环服务必须启动")
}

/// 注册当前 Channel revision 的 scope binding，模拟 Host 即将 spawn sidecar 前的步骤。
fn register(
    loopback: &L3bLoopback,
    manager: &LlmChannelManager,
    scope: &str,
    generation: u64,
) -> String {
    loopback
        .register_binding(scope, generation, manager.revision())
        .expect("当前 Channel revision 必须可注册 binding token")
        .as_bearer()
}

/// 建立一次最小 Chat Completions 请求；调用方可决定何时读取和关闭下游连接。
fn open_chat_request(address: SocketAddr, token: &str, body: &str) -> TcpStream {
    let mut stream =
        TcpStream::connect_timeout(&address, TEST_TIMEOUT).expect("必须能连接本地 L3b 监听端口");
    stream
        .set_read_timeout(Some(TEST_TIMEOUT))
        .expect("必须能设置测试读取超时");
    let request = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(request.as_bytes())
        .expect("必须能写入本地 Chat Completions 请求");
    stream
        .flush()
        .expect("必须能刷新本地 Chat Completions 请求");
    stream
}

/// 读取直到对端关闭，用于内容长度已知或错误响应的短连接用例。
fn read_to_end(stream: &mut TcpStream) -> String {
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("本地 HTTP 响应必须可读完");
    response
}

/// 从 HTTP 状态行取得数值，避免测试依赖额外 HTTP 客户端库。
fn status_code(response: &str) -> u16 {
    response
        .split_whitespace()
        .nth(1)
        .expect("响应必须含 HTTP 状态码")
        .parse()
        .expect("HTTP 状态码必须是数字")
}

/// 读取上游收到的完整请求头和固定长度 body，确保后续 EOF 观察不误读请求体。
fn read_http_request(stream: &mut TcpStream) -> String {
    let mut bytes = Vec::new();
    let mut one = [0_u8; 1];
    while !bytes.ends_with(b"\r\n\r\n") {
        stream
            .read_exact(&mut one)
            .expect("上游必须收到完整 HTTP 请求头");
        bytes.push(one[0]);
    }
    let headers = String::from_utf8(bytes).expect("测试请求头必须是 UTF-8");
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then_some(value.trim())
        })
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or_default();
    let mut body = vec![0_u8; content_length];
    stream
        .read_exact(&mut body)
        .expect("上游必须收到固定长度请求体");
    headers
}

/// 启动只回固定成功响应的本地上游，并把收到的 Authorization 交给测试断言。
fn start_fixed_upstream() -> (SocketAddr, mpsc::Receiver<String>, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("必须能绑定本地测试上游");
    let address = listener.local_addr().expect("必须能读取测试上游地址");
    let (authorization_tx, authorization_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("上游必须接收 L3b 连接");
        let headers = read_http_request(&mut stream);
        let authorization = headers
            .lines()
            .find_map(|line| {
                line.strip_prefix("authorization: ")
                    .or_else(|| line.strip_prefix("Authorization: "))
            })
            .unwrap_or_default()
            .to_string();
        authorization_tx
            .send(authorization)
            .expect("测试必须接收上游 Authorization");
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}",
            )
            .expect("上游必须能返回成功响应");
        stream.flush().expect("上游成功响应必须刷新");
    });
    (address, authorization_rx, handle)
}

/// 等待限定时间内出现文件，避免 spawn 集成测试依赖任意 sleep。
fn wait_for_file(path: &Path) {
    let deadline = Instant::now() + TEST_TIMEOUT;
    while !path.exists() {
        assert!(Instant::now() < deadline, "等待 sidecar 测试产物超时");
        thread::sleep(Duration::from_millis(10));
    }
}

/// 已持久化的遗留配置也不得把 URL query 中的秘密带入 Channel view 或日志路径。
#[test]
fn loaded_channel_rejects_secret_bearing_url_before_exposing_a_view() {
    let app = Arc::new(FakeApp::byok(
        "https://8.8.8.8/v1?api_key=must-not-reach-view".to_string(),
        "test-model",
        "test-user-key",
    ));
    let error = LlmChannelManager::new(app, false)
        .err()
        .expect("带 query 的已持久化上游 URL 必须在加载阶段 fail-closed");
    assert_eq!(error, LlmChannelError::InvalidRequest);
}

/// 持久化的 IP literal 在加载时必须同步执行地址策略，不能等 sidecar 请求才拒绝。
#[test]
fn loaded_channel_rejects_unsafe_ip_literals_before_sidecar_can_start() {
    for blocked_url in [
        "https://192.168.1.10/v1",
        "https://169.254.169.254/v1",
        "https://[::127.0.0.1]/v1",
    ] {
        let app = Arc::new(FakeApp::byok(
            blocked_url.to_string(),
            "test-model",
            "test-user-key",
        ));
        let error = LlmChannelManager::new(app, false)
            .err()
            .expect("不安全 IP literal 不得形成可启动的已加载 Channel");
        assert_eq!(error, LlmChannelError::InvalidRequest);
    }
}

/// 依赖 DNS 的历史配置可保留给设置页修复，但在首次地址审查完成前不得启动 sidecar。
#[test]
fn loaded_dns_config_is_not_startable_before_address_validation() {
    let app = Arc::new(FakeApp::byok(
        "https://localhost/v1".to_string(),
        "test-model",
        "test-user-key",
    ));
    let manager =
        LlmChannelManager::new(app, false).expect("仅有语法正确的 DNS 配置仍应可读取设置页 view");

    assert!(
        !manager.has_active_byok().expect("Channel 状态锁必须可用"),
        "DNS 依赖的已加载配置在完整地址审查前不得被视为可启动"
    );
}

/// 公开 Set 请求的 Debug 形状也只能标记 Relay app key 是否存在，不能泄露凭据。
#[test]
fn set_llm_channel_request_debug_redacts_relay_app_key() {
    let request = SetLlmChannelRequest {
        app_key: Some("debug-relay-app-key-secret".to_string()),
        ..SetLlmChannelRequest::default()
    };

    assert!(
        !format!("{request:?}").contains("debug-relay-app-key-secret"),
        "公开 Set 请求的调试输出不得回显 Relay app key"
    );
}

/// 未知 binding 必须在任何解封或上游连接之前被拒绝。
#[test]
fn unknown_token_is_rejected_before_unseal_or_upstream() {
    let app = Arc::new(FakeApp::byok(
        "http://127.0.0.1:9/v1".to_string(),
        "test-model",
        "test-user-key",
    ));
    let manager = manager(Arc::clone(&app), true);
    let loopback = loopback(manager, true);

    let mut downstream = open_chat_request(
        loopback.local_addr(),
        "not-a-registered-binding-token",
        "{\"stream\":true}",
    );
    let response = read_to_end(&mut downstream);

    assert_eq!(status_code(&response), 401, "未知 binding 必须是未授权");
    assert_eq!(
        app.unseal_calls.load(Ordering::SeqCst),
        0,
        "未知 token 到达前不得调用产品解封端口"
    );
}

/// binding token 是 scope/generation/revision 身份，不能由请求体自报覆盖且旧代必须失效。
#[test]
fn binding_scope_generation_and_channel_revision_are_fail_closed() {
    let (upstream_address, authorization_rx, upstream) = start_fixed_upstream();
    let app = Arc::new(FakeApp::byok(
        format!("http://{upstream_address}/v1"),
        "test-model",
        "test-user-key",
    ));
    let manager = manager(Arc::clone(&app), true);
    let loopback = loopback(Arc::clone(&manager), true);
    let token = register(&loopback, &manager, "scope-a", 7);

    // 请求体携带的伪造 scope/channel 字段必须被忽略，身份只来自 binding token。
    let mut downstream = open_chat_request(
        loopback.local_addr(),
        &token,
        "{\"scope_id\":\"scope-b\",\"channel_revision\":999}",
    );
    let response = read_to_end(&mut downstream);
    assert_eq!(
        status_code(&response),
        200,
        "已注册 token 必须只按自身绑定转发"
    );
    assert!(
        authorization_rx.recv_timeout(TEST_TIMEOUT).is_ok(),
        "合法 token 必须到达上游"
    );
    upstream.join().expect("固定上游线程必须退出");

    loopback.registry().invalidate_generation("scope-a", 7);
    let mut stale_generation =
        open_chat_request(loopback.local_addr(), &token, "{\"stream\":true}");
    assert_eq!(
        status_code(&read_to_end(&mut stale_generation)),
        401,
        "旧 process generation 的 token 必须失败"
    );

    let current = register(&loopback, &manager, "scope-a", 8);
    manager
        .set(SetLlmChannelRequest {
            api_key: Some("rotated-test-key".to_string()),
            ..SetLlmChannelRequest::default()
        })
        .expect("仅轮换 Key 必须提交新 Channel revision");
    let mut stale_channel = open_chat_request(loopback.local_addr(), &current, "{\"stream\":true}");
    assert_eq!(
        status_code(&read_to_end(&mut stale_channel)),
        401,
        "换通道后的旧 revision token 必须失败"
    );
}

/// 上游只能看到用户 Key，绝不能看到 sidecar 的 binding token。
#[test]
fn upstream_authorization_uses_user_key_not_binding_token() {
    let (upstream_address, authorization_rx, upstream) = start_fixed_upstream();
    let user_key = "test-user-key-for-upstream";
    let app = Arc::new(FakeApp::byok(
        format!("http://{upstream_address}/v1"),
        "test-model",
        user_key,
    ));
    let manager = manager(app, true);
    let loopback = loopback(Arc::clone(&manager), true);
    let token = register(&loopback, &manager, "scope-a", 1);

    let mut downstream = open_chat_request(loopback.local_addr(), &token, "{\"stream\":false}");
    assert_eq!(status_code(&read_to_end(&mut downstream)), 200);
    let authorization = authorization_rx
        .recv_timeout(TEST_TIMEOUT)
        .expect("上游必须收到 L3b 转发请求");
    upstream.join().expect("固定上游线程必须退出");

    assert_eq!(authorization, format!("Bearer {user_key}"));
    assert_ne!(authorization, format!("Bearer {token}"));
}

/// SSE 的第一个块必须在上游结束前被下游看到，禁止把完整响应缓冲后再转发。
#[test]
fn streaming_first_chunk_arrives_before_upstream_finishes() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("必须能绑定流式测试上游");
    let upstream_address = listener.local_addr().expect("必须能读取流式上游地址");
    let (first_chunk_tx, first_chunk_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let upstream = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("流式上游必须接收连接");
        let _ = read_http_request(&mut stream);
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: 26\r\nConnection: close\r\n\r\ndata: first\n\n",
            )
            .expect("流式上游必须写出首块");
        stream.flush().expect("流式首块必须立即刷新");
        first_chunk_tx
            .send(())
            .expect("测试必须收到首块已写出的信号");
        let _ = release_rx.recv_timeout(TEST_TIMEOUT);
        stream
            .write_all(b"data: final\n\n")
            .expect("流式上游必须写出末块");
        stream.flush().expect("流式末块必须刷新");
    });

    let app = Arc::new(FakeApp::byok(
        format!("http://{upstream_address}/v1"),
        "test-model",
        "test-user-key",
    ));
    let manager = manager(app, true);
    let loopback = loopback(Arc::clone(&manager), true);
    let token = register(&loopback, &manager, "scope-a", 1);
    let mut downstream = open_chat_request(loopback.local_addr(), &token, "{\"stream\":true}");

    first_chunk_rx
        .recv_timeout(TEST_TIMEOUT)
        .expect("上游必须先写出 SSE 首块");
    let mut first_bytes = [0_u8; 512];
    let count = downstream
        .read(&mut first_bytes)
        .expect("下游必须在上游结束前收到首块");
    let observed = String::from_utf8_lossy(&first_bytes[..count]);
    assert!(observed.contains("first"), "首块必须已被逐块转发");

    release_tx.send(()).expect("测试必须允许上游完成流式响应");
    let _ = read_to_end(&mut downstream);
    upstream.join().expect("流式上游线程必须退出");
}

/// 下游断开时，L3b 必须 drop 上游流而非继续读取或缓冲未消费的响应。
#[test]
fn downstream_disconnect_stops_reading_upstream() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("必须能绑定断开测试上游");
    let upstream_address = listener.local_addr().expect("必须能读取断开上游地址");
    let (first_chunk_tx, first_chunk_rx) = mpsc::channel();
    let (stopped_tx, stopped_rx) = mpsc::channel();
    let upstream = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("断开测试上游必须接收连接");
        let _ = read_http_request(&mut stream);
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: 999999\r\nConnection: keep-alive\r\n\r\ndata: first\n\n",
            )
            .expect("断开测试上游必须写出首块");
        stream.flush().expect("断开测试首块必须刷新");
        first_chunk_tx.send(()).expect("测试必须收到断开首块信号");

        stream
            .set_read_timeout(Some(Duration::from_millis(50)))
            .expect("必须能设置上游 EOF 探测超时");
        let deadline = Instant::now() + TEST_TIMEOUT;
        let mut probe = [0_u8; 1];
        while Instant::now() < deadline {
            match stream.read(&mut probe) {
                Ok(0) => {
                    let _ = stopped_tx.send(());
                    return;
                }
                Ok(_) => continue,
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) => {}
                Err(_) => {
                    let _ = stopped_tx.send(());
                    return;
                }
            }
        }
    });

    let app = Arc::new(FakeApp::byok(
        format!("http://{upstream_address}/v1"),
        "test-model",
        "test-user-key",
    ));
    let manager = manager(app, true);
    let loopback = loopback(Arc::clone(&manager), true);
    let token = register(&loopback, &manager, "scope-a", 1);
    let mut downstream = open_chat_request(loopback.local_addr(), &token, "{\"stream\":true}");

    first_chunk_rx
        .recv_timeout(TEST_TIMEOUT)
        .expect("上游必须先开始发送流");
    let mut first_bytes = [0_u8; 512];
    let _ = downstream
        .read(&mut first_bytes)
        .expect("下游必须先收到流的首块");
    downstream
        .shutdown(Shutdown::Both)
        .expect("测试下游必须能主动断开");
    drop(downstream);

    stopped_rx
        .recv_timeout(TEST_TIMEOUT)
        .expect("下游断开后上游连接必须被停止读取并关闭");
    upstream.join().expect("断开测试上游线程必须退出");
}

/// 默认 SSRF 策略必须拒绝历史回环配置，且绝不能把请求送到上游。
#[test]
fn ssrf_rejects_loopback_before_connecting() {
    let app = Arc::new(FakeApp::byok(
        "http://127.0.0.1:9/v1".to_string(),
        "test-model",
        "test-user-key",
    ));
    // 仅允许构造历史 HTTP loopback 配置；实际 L3b 出站策略仍明确保持默认拒绝。
    let manager = manager(app, true);
    let loopback = loopback(Arc::clone(&manager), false);
    let token = register(&loopback, &manager, "scope-a", 1);
    let mut downstream = open_chat_request(loopback.local_addr(), &token, "{\"stream\":false}");
    let response = read_to_end(&mut downstream);
    assert_eq!(
        status_code(&response),
        403,
        "受限地址必须在连接前被 SSRF 策略拒绝"
    );
}

/// IPv4-compatible IPv6 必须在策略层降级为嵌入 IPv4，并在真实本地监听器前被拦截。
#[test]
fn ipv4_compatible_ipv6_ssrf_never_touches_real_listener() {
    let listener = TcpListener::bind("[::]:0").expect("必须能绑定真实 IPv6 本地监听器");
    listener
        .set_nonblocking(true)
        .expect("测试监听器必须能切换为非阻塞模式");
    let port = listener
        .local_addr()
        .expect("必须能读取测试监听端口")
        .port();
    let (touched_tx, touched_rx) = mpsc::channel();
    let upstream = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_millis(300);
        let mut touched = false;
        while Instant::now() < deadline {
            match listener.accept() {
                Ok((_stream, _)) => {
                    touched = true;
                    break;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(_) => break,
            }
        }
        let _ = touched_tx.send(touched);
    });

    let app = Arc::new(FakeApp::byok(
        format!("https://[::127.0.0.1]:{port}/v1"),
        "test-model",
        "test-user-key",
    ));
    // 加载时只为本回归夹具显式允许回环；实际请求路径仍必须按默认策略拒绝。
    let manager = manager(app, true);
    let loopback = loopback(Arc::clone(&manager), false);
    let token = register(&loopback, &manager, "scope-a", 1);
    let mut downstream = open_chat_request(loopback.local_addr(), &token, "{\"stream\":false}");

    assert_eq!(
        status_code(&read_to_end(&mut downstream)),
        403,
        "IPv4-compatible IPv6 必须在任何 TCP 连接前被拒绝"
    );
    assert!(
        !touched_rx
            .recv_timeout(TEST_TIMEOUT)
            .expect("真实本地监听器观察必须结束"),
        "SSRF 策略不得触碰 IPv4-compatible IPv6 所指向的监听器"
    );
    upstream.join().expect("IPv6 监听观察线程必须退出");
}

/// 显式开发开关必须让 IPv4-compatible IPv6 loopback 的 HTTP 设置与实际出站保持一致。
#[test]
fn http_ipv4_compatible_ipv6_loopback_is_forwarded_when_development_flag_enabled() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("必须能绑定本地测试上游");
    let port = listener
        .local_addr()
        .expect("必须能读取测试监听端口")
        .port();
    let (received_tx, received_rx) = mpsc::channel();
    let upstream = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("上游必须接收 L3b 连接");
        let headers = read_http_request(&mut stream);
        received_tx
            .send(headers)
            .expect("测试必须接收 IPv4-compatible IPv6 上游请求");
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}",
            )
            .expect("上游必须能返回成功响应");
        stream.flush().expect("上游成功响应必须刷新");
    });

    let app = Arc::new(FakeApp::byok(
        format!("http://[::127.0.0.1]:{port}/v1"),
        "test-model",
        "test-user-key",
    ));
    // 设置阶段已接受该精确开发例外；请求阶段也必须以同一分类规则转发。
    let manager = manager(app, true);
    let loopback = loopback(Arc::clone(&manager), true);
    let token = register(&loopback, &manager, "scope-a", 1);
    let mut downstream = open_chat_request(loopback.local_addr(), &token, "{\"stream\":false}");

    assert_eq!(
        status_code(&read_to_end(&mut downstream)),
        200,
        "显式允许时 IPv4-compatible IPv6 loopback HTTP 必须可用"
    );
    assert!(
        received_rx
            .recv_timeout(TEST_TIMEOUT)
            .expect("上游必须收到转发请求")
            .contains("POST /v1/chat/completions HTTP/1.1"),
        "L3b 必须把请求转发至 IPv4-compatible IPv6 loopback 上游"
    );
    upstream.join().expect("IPv4 上游线程必须退出");
}

/// 原生 IPv6 `::1` 必须按回环地址保留，不能按 IPv4-compatible 地址归一化。
#[test]
fn http_native_ipv6_loopback_is_forwarded_when_development_flag_enabled() {
    let listener = TcpListener::bind("[::1]:0").expect("必须能绑定原生 IPv6 测试上游");
    let port = listener
        .local_addr()
        .expect("必须能读取原生 IPv6 测试监听端口")
        .port();
    let (received_tx, received_rx) = mpsc::channel();
    let upstream = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("上游必须接收 L3b 连接");
        let headers = read_http_request(&mut stream);
        received_tx
            .send(headers)
            .expect("测试必须接收原生 IPv6 上游请求");
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}",
            )
            .expect("上游必须能返回成功响应");
        stream.flush().expect("上游成功响应必须刷新");
    });

    let app = Arc::new(FakeApp::byok(
        format!("http://[::1]:{port}/v1"),
        "test-model",
        "test-user-key",
    ));
    let manager = manager(app, true);
    let loopback = loopback(Arc::clone(&manager), true);
    let token = register(&loopback, &manager, "scope-a", 1);
    let mut downstream = open_chat_request(loopback.local_addr(), &token, "{\"stream\":false}");

    assert_eq!(
        status_code(&read_to_end(&mut downstream)),
        200,
        "显式允许时原生 IPv6 loopback HTTP 必须可用"
    );
    assert!(
        received_rx
            .recv_timeout(TEST_TIMEOUT)
            .expect("上游必须收到转发请求")
            .contains("POST /v1/chat/completions HTTP/1.1"),
        "L3b 必须把请求转发至原生 IPv6 loopback 上游"
    );
    upstream.join().expect("IPv6 上游线程必须退出");
}

/// SetLlmChannel 的空更新不可改变 committed view；端点、模型或类型变化必须带新 Key。
#[test]
fn channel_set_requires_new_secret_for_identity_changes_and_allows_key_rotation() {
    let app = Arc::new(FakeApp::byok(
        "https://8.8.8.8/v1".to_string(),
        "model-a",
        "original-test-key",
    ));
    let manager = manager(Arc::clone(&app), false);
    let revision = manager.revision();

    let no_op = manager
        .set(SetLlmChannelRequest::default())
        .expect("全空更新必须是 no-op");
    assert!(!no_op.changed, "全空请求不得创建新 Channel revision");
    assert_eq!(manager.revision(), revision);
    assert_eq!(app.persist_calls.load(Ordering::SeqCst), 0);

    let same_kind = manager
        .set(SetLlmChannelRequest {
            kind: Some(LlmChannelKind::Byok),
            ..SetLlmChannelRequest::default()
        })
        .expect("与现状相同的 kind 必须是 no-op");
    assert!(!same_kind.changed, "相同 kind 不得无故失效现有 token");
    assert_eq!(app.persist_calls.load(Ordering::SeqCst), 0);

    let url_change = manager.set(SetLlmChannelRequest {
        base_url: Some("https://1.1.1.1/v1".to_string()),
        ..SetLlmChannelRequest::default()
    });
    assert!(url_change.is_err(), "更换 URL 但不提供新 Key 必须被拒绝");
    assert_eq!(manager.revision(), revision, "失败不得更改 committed view");

    let model_change = manager.set(SetLlmChannelRequest {
        model_id: Some("model-b".to_string()),
        ..SetLlmChannelRequest::default()
    });
    assert!(
        model_change.is_err(),
        "更换 model 但不提供新 Key 必须被拒绝"
    );

    let kind_change = manager.set(SetLlmChannelRequest {
        kind: Some(LlmChannelKind::Relay),
        ..SetLlmChannelRequest::default()
    });
    assert!(kind_change.is_err(), "切换通道种类但不提供新秘密必须被拒绝");

    let rotated = manager
        .set(SetLlmChannelRequest {
            api_key: Some("rotated-test-key".to_string()),
            ..SetLlmChannelRequest::default()
        })
        .expect("只轮换 BYOK Key 必须允许保留 URL/model");
    assert!(
        rotated.changed,
        "Key 轮换必须创建新 revision 使旧 token 失效"
    );
    assert!(manager.revision() > revision);
    assert_eq!(app.persist_calls.load(Ordering::SeqCst), 1);
}

/// 初次写入无既有 Channel 可沿用，必须显式声明 BYOK 种类才允许密封和持久化。
#[test]
fn unconfigured_channel_requires_explicit_byok_kind() {
    let app = Arc::new(FakeApp::unconfigured());
    let manager = manager(Arc::clone(&app), false);
    let missing_kind = manager.set(SetLlmChannelRequest {
        base_url: Some("https://8.8.8.8/v1".to_string()),
        model_id: Some("test-model".to_string()),
        api_key: Some("test-user-key".to_string()),
        ..SetLlmChannelRequest::default()
    });
    assert_eq!(missing_kind, Err(LlmChannelError::InvalidRequest));
    assert_eq!(
        app.seal_calls.load(Ordering::SeqCst),
        0,
        "未声明种类时不得提前把用户 Key 交给密封端口"
    );
    assert_eq!(
        app.persist_calls.load(Ordering::SeqCst),
        0,
        "未声明种类时不得形成 committed Channel"
    );

    let changed = manager
        .set(SetLlmChannelRequest {
            kind: Some(LlmChannelKind::Byok),
            base_url: Some("https://8.8.8.8/v1".to_string()),
            model_id: Some("test-model".to_string()),
            api_key: Some("test-user-key".to_string()),
            ..SetLlmChannelRequest::default()
        })
        .expect("显式完整 BYOK 身份必须可首次提交");
    assert!(changed.changed);
    assert_eq!(app.persist_calls.load(Ordering::SeqCst), 1);
}

/// 已认证请求也必须受硬 body 上限约束，且过大 body 不得触发用户 Key 解封。
#[test]
fn body_limit_rejects_before_unseal_or_upstream() {
    let app = Arc::new(FakeApp::byok(
        "http://127.0.0.1:9/v1".to_string(),
        "test-model",
        "test-user-key",
    ));
    let manager = manager(Arc::clone(&app), true);
    let loopback = loopback(Arc::clone(&manager), true);
    let token = register(&loopback, &manager, "scope-a", 1);
    let too_large = "x".repeat(MAX_L3B_REQUEST_BODY_BYTES + 1);

    let mut downstream = open_chat_request(loopback.local_addr(), &token, &too_large);
    let response = read_to_end(&mut downstream);
    assert_eq!(status_code(&response), 413, "超过硬上限的请求必须被拒绝");
    assert_eq!(
        app.unseal_calls.load(Ordering::SeqCst),
        0,
        "超大请求不得在 body 限制前触发用户 Key 解封"
    );
}

/// 设置阶段就要拒绝私网和 metadata 上游，不能先持久化再等待请求路径失败。
#[test]
fn channel_set_rejects_ssrf_addresses_before_persisting() {
    for blocked_url in ["https://192.168.1.10/v1", "https://169.254.169.254/v1"] {
        let app = Arc::new(FakeApp::unconfigured());
        let manager = manager(Arc::clone(&app), false);
        let result = manager.set(SetLlmChannelRequest {
            kind: Some(LlmChannelKind::Byok),
            base_url: Some(blocked_url.to_string()),
            model_id: Some("test-model".to_string()),
            api_key: Some("test-user-key".to_string()),
            ..SetLlmChannelRequest::default()
        });
        assert!(result.is_err(), "SSRF 目标不得形成 committed Channel");
        assert_eq!(
            app.persist_calls.load(Ordering::SeqCst),
            0,
            "被拒绝 URL 不得调用产品持久化端口"
        );
    }
}

/// 非网络形状校验完成后必须立即密封明文 Key；地址审查失败时绝不能持久化候选配置。
#[test]
fn channel_set_seals_key_before_address_validation_and_never_persists_failure() {
    let app = Arc::new(FakeApp::unconfigured());
    let manager = manager(Arc::clone(&app), false);

    let result = manager.set(SetLlmChannelRequest {
        kind: Some(LlmChannelKind::Byok),
        base_url: Some("https://192.168.1.10/v1".to_string()),
        model_id: Some("test-model".to_string()),
        api_key: Some("test-user-key".to_string()),
        ..SetLlmChannelRequest::default()
    });

    assert_eq!(result, Err(LlmChannelError::InvalidRequest));
    assert_eq!(
        app.seal_calls.load(Ordering::SeqCst),
        1,
        "地址解析前的结构校验通过后，明文 Key 必须立即交给密封端口"
    );
    assert_eq!(
        app.persist_calls.load(Ordering::SeqCst),
        0,
        "地址审查失败的已密封候选配置不得被持久化"
    );
}

/// 更新 URL/model 时的新明文 Key 也必须在 DNS/IP 审查前立即密封，失败不得覆盖旧配置。
#[test]
fn channel_update_seals_key_before_address_validation_and_keeps_old_config() {
    let app = Arc::new(FakeApp::byok(
        "https://8.8.8.8/v1".to_string(),
        "original-model",
        "original-test-key",
    ));
    let manager = manager(Arc::clone(&app), false);
    let revision = manager.revision();

    let result = manager.set(SetLlmChannelRequest {
        base_url: Some("https://192.168.1.10/v1".to_string()),
        api_key: Some("replacement-test-key".to_string()),
        ..SetLlmChannelRequest::default()
    });

    assert_eq!(result, Err(LlmChannelError::InvalidRequest));
    assert_eq!(
        app.seal_calls.load(Ordering::SeqCst),
        1,
        "更新路径也必须在网络地址审查前密封新明文 Key"
    );
    assert_eq!(app.persist_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        manager.revision(),
        revision,
        "失败的地址审查不得覆盖既有 committed Channel"
    );
}

/// restart 局部失败时，新 Channel 已提交，调用方必须收到可重试错误而不是旧 view。
#[test]
fn channel_change_keeps_committed_view_when_live_scope_restart_fails() {
    let temporary = tempfile::tempdir().expect("必须能创建 restart 失败测试目录");
    let ready_path = temporary.path().join("sidecar-ready");
    let script_path = temporary.path().join("first-launch-only.sh");
    let script = format!("#!/bin/sh\n: > {ready_path:?}\nwhile IFS= read -r _; do :; done\n");
    fs::write(&script_path, script).expect("必须能写入 first launch sidecar");
    fs::set_permissions(&script_path, fs::Permissions::from_mode(0o700))
        .expect("first launch sidecar 必须可执行");

    let app = Arc::new(FakeApp::byok(
        "https://8.8.8.8/v1".to_string(),
        "restart-model",
        "original-test-key",
    ));
    let service = LlmChannelService::new(
        Arc::clone(&app),
        HostRuntimeConfig {
            home_root: temporary.path().join("app-data"),
            sidecar_bin: script_path.clone(),
            mcp_exec_root: temporary.path().join("mcp"),
            idle_after: Duration::from_secs(60),
            l3b: L3bRuntimeConfig::default(),
        },
    )
    .expect("已配置 BYOK 的 service 必须可构造");
    service
        .launch_scope("library-a")
        .expect("第一次 sidecar launch 必须成功");
    wait_for_file(&ready_path);
    fs::remove_file(&script_path).expect("删除二次 launch 可执行文件必须成功");

    let error = service
        .set(SetLlmChannelRequest {
            api_key: Some("rotated-test-key".to_string()),
            ..SetLlmChannelRequest::default()
        })
        .expect_err("已存活 scope 的二次 spawn 失败必须返回错误");
    assert_eq!(error, LlmChannelError::RestartFailed);
    assert!(error.as_kit_error().retryable, "restart 失败必须明确可重试");
    let view = service.view().expect("失败后仍必须可读取 committed view");
    assert_eq!(view.kind, Some(LlmChannelKind::Byok));
    assert!(view.key_present, "新提交的 Key view 不得回退为未配置");
    assert_eq!(
        app.persist_calls.load(Ordering::SeqCst),
        1,
        "restart 失败前新 Channel 必须已经持久化"
    );
}

/// 未配置 Channel 时不得监听 L3b，也不得因为 launch 请求创建 sidecar 目录或进程。
#[test]
fn unconfigured_channel_neither_listens_nor_spawns() {
    let temporary = tempfile::tempdir().expect("必须能创建未配置 Channel 测试目录");
    let home_root = temporary.path().join("app-data");
    let service = LlmChannelService::new(
        Arc::new(FakeApp::unconfigured()),
        HostRuntimeConfig {
            home_root: home_root.clone(),
            sidecar_bin: temporary.path().join("missing-sidecar"),
            mcp_exec_root: temporary.path().join("mcp"),
            idle_after: Duration::from_secs(60),
            l3b: L3bRuntimeConfig::default(),
        },
    )
    .expect("未配置 Channel 的 service 仍必须可构造以返回设置页 view");

    assert!(service.loopback_addr().is_none());
    assert_eq!(
        service.launch_scope("library-a"),
        Err(LlmChannelError::Unconfigured),
        "无 Channel 时 launch 必须在监听和 spawn 前 fail-closed"
    );
    assert!(service.loopback_addr().is_none());
    assert!(
        !home_root.exists(),
        "无 Channel 时不得提前创建 sidecar scope 的私有目录"
    );
}

/// 完整 spawn 必须在 child 开始前已有监听、binding 和权威 config，且环境没有用户 Key。
#[test]
fn real_launch_writes_loopback_config_and_keeps_user_key_out_of_sidecar_environment() {
    let temporary = tempfile::tempdir().expect("必须能创建 sidecar launch 临时目录");
    let capture_path = temporary.path().join("captured-env");
    let config_capture_path = temporary.path().join("captured-config");
    let script_path = temporary.path().join("fake-sidecar.sh");
    let user_key = "sidecar-must-not-see-this-test-key";
    let script = format!(
        "#!/bin/sh\n\
         home=\"\"\n\
         while [ \"$#\" -gt 0 ]; do\n\
           if [ \"$1\" = \"--grok-home\" ]; then home=\"$2\"; break; fi\n\
           shift\n\
         done\n\
         test -n \"$EFFLAB_L3B_BIND\" || exit 11\n\
         test -f \"$home/config.toml\" || exit 12\n\
         /usr/bin/env | /usr/bin/sed 's/=.*$//' | /usr/bin/sort > {capture:?}\n\
         /bin/cp \"$home/config.toml\" {config_capture:?}\n\
         while IFS= read -r _; do :; done\n",
        capture = capture_path,
        config_capture = config_capture_path,
    );
    fs::write(&script_path, script).expect("必须能写入 fake sidecar");
    fs::set_permissions(&script_path, fs::Permissions::from_mode(0o700))
        .expect("fake sidecar 必须可执行");

    let app = Arc::new(FakeApp::byok(
        "https://8.8.8.8/v1".to_string(),
        "launch-model",
        user_key,
    ));
    let runtime_config = HostRuntimeConfig {
        home_root: temporary.path().join("app-data"),
        sidecar_bin: script_path,
        mcp_exec_root: temporary.path().join("mcp"),
        idle_after: Duration::from_secs(60),
        l3b: L3bRuntimeConfig::default(),
    };
    let service =
        LlmChannelService::new(app, runtime_config).expect("已配置 BYOK 的 Host 服务必须可构造");
    assert!(
        service.loopback_addr().is_none(),
        "尚未启动 scope 时不得无故监听 L3b"
    );

    service
        .launch_scope("library-a")
        .expect("真实 launch 零件必须能按安全顺序启动 fake sidecar");
    wait_for_file(&capture_path);
    wait_for_file(&config_capture_path);

    let captured_env = fs::read_to_string(&capture_path).expect("必须能读取 child 环境快照");
    let captured_config = fs::read_to_string(&config_capture_path).expect("必须能读取权威配置快照");
    let loopback_address = service
        .loopback_addr()
        .expect("成功 launch 后必须存在进程级 L3b 监听");

    // 测试脚本仅落盘环境变量名，避免测试产物记录真实 binding token。
    assert!(captured_env.lines().any(|name| name == "EFFLAB_L3B_BIND"));
    assert!(!captured_env.lines().any(|name| name == "GROK_HOME"));
    assert!(!captured_env.lines().any(|name| name == "XAI_API_KEY"));
    assert!(!captured_env.contains(user_key));
    // `/bin/sh` 会自行生成 `_`、`PWD`、`SHLVL`；去掉这三项后，execve 收到的
    // 业务环境必须只剩 Host 生成的 binding token，不得继承平台或用户变量。
    let inherited_variables: Vec<_> = captured_env
        .lines()
        .filter(|name| !matches!(*name, "_" | "PWD" | "SHLVL"))
        .collect();
    assert_eq!(
        inherited_variables,
        vec!["EFFLAB_L3B_BIND"],
        "sidecar 除 shell 自生成变量外只能继承 binding token"
    );
    assert!(!captured_config.contains(user_key));
    assert!(!captured_config.contains("https://8.8.8.8/v1"));
    assert!(captured_config.contains(&format!(
        "base_url = \"http://127.0.0.1:{}/v1\"",
        loopback_address.port()
    )));
    assert!(captured_config.contains("model = \"launch-model\""));
    assert!(captured_config.contains("env_key = \"EFFLAB_L3B_BIND\""));
}
