//! L3b 本机回环 Chat Completions 出口。
//!
//! sidecar 只可用短生命周期 binding token 访问此服务；Host 在认证 token、复核 Channel
//! revision 后才会解封用户 Key，并以已验证地址连接用户上游。响应体直接流式转发，
//! 下游断开会 drop 上游流而不是继续缓冲。

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::{Request, State};
use axum::http::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ring::rand::{SecureRandom, SystemRandom};
use tokio::sync::oneshot;
use url::Url;

use crate::config::L3bRuntimeConfig;
use crate::llm_channel::{LlmChannelError, LlmChannelManager};

/// 入站 Chat Completions 请求的硬上限；超过时在任何上游访问前返回 413。
pub const MAX_L3B_REQUEST_BODY_BYTES: usize = 1_048_576;

/// 仅可监听的精确 IPv4/IPv6 loopback 地址。
const IPV4_LOOPBACK: Ipv4Addr = Ipv4Addr::LOCALHOST;
const IPV6_LOOPBACK: Ipv6Addr = Ipv6Addr::LOCALHOST;

/// 注册 binding token 后绑定的不可伪造身份上下文。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindingContext {
    /// sidecar 所属的 Host scope slot 标识。
    pub scope_id: String,
    /// sidecar 进程 generation；重启后旧代 token 必须失效。
    pub generation: u64,
    /// 注册时的产品全局 Channel revision。
    pub channel_revision: u64,
}

/// 至少 256 bit 的 sidecar binding token；永不实现会回显内容的 Debug。
#[derive(Clone, PartialEq, Eq)]
pub struct BindingToken([u8; 32]);

impl fmt::Debug for BindingToken {
    /// token 的调试形状只给出不可逆短指纹，避免日志意外泄露 bearer 值。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("BindingToken")
            .field(&format_args!("fingerprint:{}", self.fingerprint()))
            .finish()
    }
}

impl BindingToken {
    /// 从系统 CSPRNG 获取完整 256 bit 随机 token。
    fn generate() -> Result<Self, L3bLoopbackError> {
        let mut bytes = [0_u8; 32];
        SystemRandom::new()
            .fill(&mut bytes)
            .map_err(|_| L3bLoopbackError::RandomnessUnavailable)?;
        Ok(Self(bytes))
    }

    /// 生成 sidecar 环境变量及 HTTP Bearer 所用的 URL-safe 文本，不记录该文本。
    pub fn as_bearer(&self) -> String {
        URL_SAFE_NO_PAD.encode(self.0)
    }

    /// 用于允许的调试日志的不可逆短指纹。
    fn fingerprint(&self) -> String {
        let digest = blake3::hash(&self.0).to_hex().to_string();
        digest[..12].to_string()
    }
}

/// 仅在 registry 内保存的 token 与活动标志；不派生 Debug 以防遗漏脱敏。
struct BindingRecord {
    token: BindingToken,
    context: BindingContext,
    active: bool,
}

/// 进程级 binding token 注册表。
///
/// 即使逻辑上是 token → context 映射，验证时仍线性扫描全部记录并使用常量时间比较，
/// 避免把 bearer 命中与否泄露为普通哈希查找时间差。
pub struct BindingTokenRegistry {
    records: Mutex<Vec<BindingRecord>>,
}

impl Default for BindingTokenRegistry {
    /// 新进程无任何可信 sidecar，所有请求默认拒绝。
    fn default() -> Self {
        Self {
            records: Mutex::new(Vec::new()),
        }
    }
}

impl BindingTokenRegistry {
    /// 生成并注册某一 scope/generation/revision 的 binding token。
    pub fn register(
        &self,
        scope_id: impl Into<String>,
        generation: u64,
        channel_revision: u64,
    ) -> Result<BindingToken, L3bLoopbackError> {
        let scope_id = scope_id.into();
        if scope_id.is_empty() || generation == 0 || channel_revision == 0 {
            return Err(L3bLoopbackError::InvalidBindingContext);
        }
        let token = BindingToken::generate()?;
        let fingerprint = token.fingerprint();
        let context = BindingContext {
            scope_id,
            generation,
            channel_revision,
        };
        self.records
            .lock()
            .map_err(|_| L3bLoopbackError::RegistryUnavailable)?
            .push(BindingRecord {
                token: token.clone(),
                context: context.clone(),
                active: true,
            });
        tracing::debug!(
            token_fingerprint = %fingerprint,
            scope = %context.scope_id,
            generation = context.generation,
            channel_revision = context.channel_revision,
            "L3b binding token 已注册"
        );
        Ok(token)
    }

    /// 常量时间认证 bearer token，且不允许请求自报 scope、generation 或 Channel。
    pub fn authorize(&self, presented: &str) -> Option<BindingContext> {
        let (candidate, valid_encoding) = decode_binding_token(presented);
        let records = self.records.lock().ok()?;
        let mut authorized = None;
        for record in records.iter() {
            // 不允许命中后提前返回：全部活动或失效记录都必须参与同样的固定长度比较。
            let token_matches = constant_time_token_eq(&candidate, &record.token.0);
            if valid_encoding && token_matches && record.active && authorized.is_none() {
                authorized = Some(record.context.clone());
            }
        }
        authorized
    }

    /// 使一个旧进程代的 token 失效；抢锁、退出或 restart 都调用该入口。
    pub fn invalidate_generation(&self, scope_id: &str, generation: u64) -> usize {
        self.invalidate_where(|context| {
            context.scope_id == scope_id && context.generation == generation
        })
    }

    /// 使某个 scope 的所有历史 token 失效。
    pub fn invalidate_scope(&self, scope_id: &str) -> usize {
        self.invalidate_where(|context| context.scope_id == scope_id)
    }

    /// 通道切换/轮换后立即使全进程所有旧 token 失效。
    pub fn invalidate_all(&self) -> usize {
        self.invalidate_where(|_| true)
    }

    /// 按条件失活注册表记录，返回本次实际失效的数量。
    fn invalidate_where(&self, predicate: impl Fn(&BindingContext) -> bool) -> usize {
        let Ok(mut records) = self.records.lock() else {
            return 0;
        };
        let mut invalidated = 0;
        for record in records.iter_mut() {
            if record.active && predicate(&record.context) {
                record.active = false;
                invalidated += 1;
                tracing::debug!(
                    token_fingerprint = %record.token.fingerprint(),
                    scope = %record.context.scope_id,
                    generation = record.context.generation,
                    channel_revision = record.context.channel_revision,
                    "L3b binding token 已失效"
                );
            }
        }
        invalidated
    }
}

/// 将 bearer 文本解码为固定长度候选；格式错误也保留零值参与 registry 全量比较。
fn decode_binding_token(presented: &str) -> ([u8; 32], bool) {
    let mut candidate = [0_u8; 32];
    let Ok(decoded) = URL_SAFE_NO_PAD.decode(presented.as_bytes()) else {
        return (candidate, false);
    };
    if decoded.len() != candidate.len() {
        return (candidate, false);
    }
    candidate.copy_from_slice(&decoded);
    (candidate, true)
}

/// 对固定 256 bit token 做全字节 XOR 累积比较；不可在首个不等字节提前返回。
fn constant_time_token_eq(left: &[u8; 32], right: &[u8; 32]) -> bool {
    let mut difference = 0_u8;
    for index in 0..left.len() {
        difference |= left[index] ^ right[index];
    }
    // black_box 防止优化器把累计比较改写为可短路的常规相等比较。
    std::hint::black_box(difference) == 0
}

/// L3b 启动、注册或监听失败的非敏感分类。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum L3bLoopbackError {
    /// 尝试绑定非精确 loopback 地址。
    InvalidListenAddress,
    /// 未配置 BYOK 时不允许监听。
    ChannelUnconfigured,
    /// OS bind/listener 初始化失败。
    BindFailed,
    /// Tokio runtime 初始化失败。
    RuntimeUnavailable,
    /// CSPRNG 不可用。
    RandomnessUnavailable,
    /// token 的 scope/generation/revision 非法。
    InvalidBindingContext,
    /// registry 锁不可用。
    RegistryUnavailable,
}

impl fmt::Display for L3bLoopbackError {
    /// 永不把地址、token 或 OS 原始错误链回传到产品协议。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            Self::InvalidListenAddress => "L3b 只能绑定精确回环地址",
            Self::ChannelUnconfigured => "LLM Channel 未配置",
            Self::BindFailed => "L3b 监听失败",
            Self::RuntimeUnavailable => "L3b 运行时不可用",
            Self::RandomnessUnavailable => "无法生成 L3b binding token",
            Self::InvalidBindingContext => "L3b binding 上下文无效",
            Self::RegistryUnavailable => "L3b binding 注册表不可用",
        };
        formatter.write_str(text)
    }
}

impl std::error::Error for L3bLoopbackError {}

/// 进程级回环服务器；一口服务多个 scope，不为每个 sidecar 另开端口。
pub struct L3bLoopback {
    registry: Arc<BindingTokenRegistry>,
    local_addr: SocketAddr,
    shutdown: Mutex<Option<oneshot::Sender<()>>>,
    worker: Mutex<Option<thread::JoinHandle<()>>>,
}

impl L3bLoopback {
    /// 绑定一口严格 loopback 的服务并开始处理 Chat Completions 请求。
    pub fn start(
        manager: Arc<LlmChannelManager>,
        config: L3bRuntimeConfig,
    ) -> Result<Self, L3bLoopbackError> {
        if !manager
            .has_active_byok()
            .map_err(|_| L3bLoopbackError::ChannelUnconfigured)?
        {
            return Err(L3bLoopbackError::ChannelUnconfigured);
        }
        if !is_exact_loopback(config.bind_addr) {
            return Err(L3bLoopbackError::InvalidListenAddress);
        }

        let listener = TcpListener::bind(SocketAddr::new(config.bind_addr, config.port))
            .map_err(|_| L3bLoopbackError::BindFailed)?;
        listener
            .set_nonblocking(true)
            .map_err(|_| L3bLoopbackError::BindFailed)?;
        let local_addr = listener
            .local_addr()
            .map_err(|_| L3bLoopbackError::BindFailed)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|_| L3bLoopbackError::RuntimeUnavailable)?;
        let registry = Arc::new(BindingTokenRegistry::default());
        let state = Arc::new(LoopbackState {
            registry: Arc::clone(&registry),
            manager,
            allow_loopback_llm: config.allow_loopback_llm,
        });
        let router = Router::new()
            .route("/v1/chat/completions", post(chat_completions))
            .with_state(state);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let worker = thread::Builder::new()
            .name("efflab-l3b-loopback".to_string())
            .spawn(move || {
                runtime.block_on(async move {
                    let Ok(listener) = tokio::net::TcpListener::from_std(listener) else {
                        return;
                    };
                    // 收到 shutdown 时直接 drop server/futures，确保未消费的上游流随 body drop 取消。
                    tokio::select! {
                        result = axum::serve(listener, router) => {
                            if result.is_err() {
                                tracing::debug!("L3b 回环服务已停止");
                            }
                        }
                        _ = shutdown_rx => {}
                    }
                });
            })
            .map_err(|_| L3bLoopbackError::RuntimeUnavailable)?;

        tracing::debug!(listen_port = local_addr.port(), "L3b 回环服务已开始监听");
        Ok(Self {
            registry,
            local_addr,
            shutdown: Mutex::new(Some(shutdown_tx)),
            worker: Mutex::new(Some(worker)),
        })
    }

    /// 返回 sidecar TOML 使用的实际监听地址；不带用户上游信息。
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// 构造唯一可写入 sidecar TOML 的 Host 回环基础 URL。
    pub fn sidecar_base_url(&self) -> String {
        format!("http://{}/v1", self.local_addr)
    }

    /// 返回进程级 registry 供 Supervisor 在 spawn/restart/exit 时管理 token 生命周期。
    pub fn registry(&self) -> Arc<BindingTokenRegistry> {
        Arc::clone(&self.registry)
    }

    /// 注册即将启动的 sidecar generation；调用方将返回 token 唯一注入 child env。
    pub fn register_binding(
        &self,
        scope_id: impl Into<String>,
        generation: u64,
        channel_revision: u64,
    ) -> Result<BindingToken, L3bLoopbackError> {
        self.registry
            .register(scope_id, generation, channel_revision)
    }

    /// 主动关闭 listener，主要供进程 shutdown / 测试资源回收使用。
    pub fn shutdown(&self) {
        if let Ok(mut shutdown) = self.shutdown.lock()
            && let Some(sender) = shutdown.take()
        {
            let _ = sender.send(());
        }
        if let Ok(mut worker) = self.worker.lock()
            && let Some(worker) = worker.take()
        {
            let _ = worker.join();
        }
    }
}

impl Drop for L3bLoopback {
    /// Drop 保证不遗留监听线程或未注销的本地服务。
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// handler 共用状态；不会保存任何解封 Key。
struct LoopbackState {
    registry: Arc<BindingTokenRegistry>,
    manager: Arc<LlmChannelManager>,
    allow_loopback_llm: bool,
}

/// 唯一允许的 sidecar 入站路径：认证 token、限长读取、解封、已验证地址出站、直接流式返回。
async fn chat_completions(State(state): State<Arc<LoopbackState>>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let Some(presented_token) = bearer_token(parts.headers.get(AUTHORIZATION)) else {
        return status_response(StatusCode::UNAUTHORIZED);
    };
    // 所有解封和上游行为都严格位于认证之后，未知 token 不可触发这些副作用。
    let Some(binding) = state.registry.authorize(presented_token) else {
        return status_response(StatusCode::UNAUTHORIZED);
    };
    let body = match to_bytes(body, MAX_L3B_REQUEST_BODY_BYTES).await {
        Ok(body) => body,
        Err(_) => return status_response(StatusCode::PAYLOAD_TOO_LARGE),
    };
    let upstream = match state.manager.resolve_for_revision(binding.channel_revision) {
        Ok(upstream) => upstream,
        Err(LlmChannelError::StaleChannelRevision) => {
            return status_response(StatusCode::UNAUTHORIZED);
        }
        Err(_) => return status_response(StatusCode::BAD_GATEWAY),
    };
    let verified = match verify_upstream(&upstream.base_url, state.allow_loopback_llm).await {
        Ok(verified) => verified,
        Err(_) => return status_response(StatusCode::FORBIDDEN),
    };
    let authorization = match upstream_authorization_header(upstream.api_key.expose()) {
        Ok(header) => header,
        Err(()) => return status_response(StatusCode::BAD_GATEWAY),
    };

    // 每个请求新建 client 并在此前重解析全部地址：不复用旧 DNS 或连接池地址。
    // URL 的 hostname 未变，因此 reqwest/rustls 仍以原 hostname 做 HTTPS SNI/证书校验。
    let client = match reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(60))
        .resolve(verified.hostname.as_str(), verified.address)
        .build()
    {
        Ok(client) => client,
        Err(_) => return status_response(StatusCode::BAD_GATEWAY),
    };
    let mut outbound = client
        .post(verified.chat_completions_url)
        .header(AUTHORIZATION, authorization)
        .body(body);
    // 只保留 Chat Completions 语义必需的请求头，绝不转发 sidecar 的 Authorization/proxy 头。
    for header_name in [CONTENT_TYPE, ACCEPT] {
        if let Some(value) = parts.headers.get(&header_name) {
            outbound = outbound.header(header_name, value);
        }
    }

    let response = match outbound.send().await {
        Ok(response) => response,
        Err(_) => return status_response(StatusCode::BAD_GATEWAY),
    };
    let status = response.status();
    let content_type = response.headers().get(CONTENT_TYPE).cloned();
    let mut builder = Response::builder().status(status);
    if let Some(content_type) = content_type {
        builder = builder.header(CONTENT_TYPE, content_type);
    }
    // 不启动后台复制任务也不 collect body：下游关闭时 Axum drop 此 stream，reqwest 随即取消上游读取。
    builder
        .body(Body::from_stream(response.bytes_stream()))
        .unwrap_or_else(|_| status_response(StatusCode::BAD_GATEWAY))
}

/// 严格解析 `Authorization: Bearer <token>`，其它认证模式一律拒绝。
fn bearer_token(header: Option<&HeaderValue>) -> Option<&str> {
    let header = header?.to_str().ok()?;
    let token = header.strip_prefix("Bearer ")?;
    (!token.is_empty()).then_some(token)
}

/// 构建用户 Key 的出站 Authorization；暂存字节只活到 reqwest 复制 header 为止。
fn upstream_authorization_header(secret: &[u8]) -> Result<HeaderValue, ()> {
    let mut bytes = Vec::with_capacity(b"Bearer ".len() + secret.len());
    bytes.extend_from_slice(b"Bearer ");
    bytes.extend_from_slice(secret);
    HeaderValue::from_bytes(&bytes).map_err(|_| ())
}

/// 已通过 DNS 全量审查、并强制 connect 到指定地址的上游请求参数。
struct VerifiedUpstream {
    chat_completions_url: Url,
    hostname: String,
    address: SocketAddr,
}

/// 每次新出站请求都重新解析/验证所有 A 与 AAAA，并把连接钉在本次验证出的地址。
async fn verify_upstream(base_url: &str, allow_loopback_llm: bool) -> Result<VerifiedUpstream, ()> {
    let mut chat_completions_url = Url::parse(base_url).map_err(|_| ())?;
    if chat_completions_url.host_str().is_none()
        || !chat_completions_url.username().is_empty()
        || chat_completions_url.password().is_some()
        || chat_completions_url.query().is_some()
        || chat_completions_url.fragment().is_some()
    {
        return Err(());
    }
    let hostname = chat_completions_url.host_str().ok_or(())?.to_string();
    let port = chat_completions_url.port_or_known_default().ok_or(())?;
    let addresses: Vec<SocketAddr> = tokio::net::lookup_host((hostname.as_str(), port))
        .await
        .map_err(|_| ())?
        .collect();
    if addresses.is_empty()
        || addresses
            .iter()
            .any(|address| !is_allowed_upstream_ip(address.ip(), allow_loopback_llm))
    {
        return Err(());
    }

    match chat_completions_url.scheme() {
        "https" => {}
        // 明文 HTTP 仅支持显式开发开关下的纯 loopback 集合；private/metadata 永不例外。
        "http"
            if allow_loopback_llm && addresses.iter().all(|address| address.ip().is_loopback()) => {
        }
        _ => return Err(()),
    }

    let base_path = chat_completions_url.path().trim_end_matches('/');
    let path = if base_path.is_empty() {
        "/chat/completions".to_string()
    } else {
        format!("{base_path}/chat/completions")
    };
    chat_completions_url.set_path(&path);
    chat_completions_url.set_query(None);
    chat_completions_url.set_fragment(None);
    Ok(VerifiedUpstream {
        chat_completions_url,
        hostname,
        // 前面已审查所有结果；使用一个被核验的地址而不是让 HTTP client 自由重解析。
        address: addresses[0],
    })
}

/// 仅允许公网地址；显式开关只放行 loopback，绝不放行其它 private/link-local/metadata。
pub(crate) fn is_allowed_upstream_ip(ip: IpAddr, allow_loopback_llm: bool) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            if ip.is_loopback() {
                return allow_loopback_llm;
            }
            let octets = ip.octets();
            !(ip.is_private()
                || ip.is_link_local()
                || ip.is_unspecified()
                || ip.is_broadcast()
                || ip.is_multicast()
                || ip.is_documentation()
                // Carrier-grade NAT、benchmark 与保留高地址都不是外部 LLM 上游。
                || (octets[0] == 100 && (64..=127).contains(&octets[1]))
                || (octets[0] == 198 && matches!(octets[1], 18 | 19))
                || octets[0] >= 240
                // 显式列出云 metadata，便于审计而非只依赖 link-local 间接覆盖。
                || ip == Ipv4Addr::new(169, 254, 169, 254))
        }
        IpAddr::V6(ip) => {
            if ip.is_loopback() {
                return allow_loopback_llm;
            }
            let segments = ip.segments();
            let unique_local = (segments[0] & 0xfe00) == 0xfc00;
            let link_local = (segments[0] & 0xffc0) == 0xfe80;
            let ipv4_mapped = ip.to_ipv4_mapped().is_some_and(|mapped| {
                !is_allowed_upstream_ip(IpAddr::V4(mapped), allow_loopback_llm)
            });
            !(ip.is_unspecified() || ip.is_multicast() || unique_local || link_local || ipv4_mapped)
        }
    }
}

/// 监听必须是精确 `127.0.0.1` 或 `::1`，不接受 `0.0.0.0`、其它 127/8 或 private 地址。
fn is_exact_loopback(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => address == IPV4_LOOPBACK,
        IpAddr::V6(address) => address == IPV6_LOOPBACK,
    }
}

/// 构造不包含内部错误细节的空 HTTP 响应。
fn status_response(status: StatusCode) -> Response {
    (status, "").into_response()
}
