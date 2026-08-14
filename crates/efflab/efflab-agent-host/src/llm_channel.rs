//! 产品全局 LLM Channel 的密封配置、提交视图与 L3b 生命周期协调。
//!
//! 本模块不实现 ACP dispatch；Task 7b 会把它接到 `HostRuntime`。这里的边界是：
//! 用户 Key 只在受控出站时短暂解封，sidecar 永远只取得 L3b binding token。

use std::fmt;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::sync::{Arc, Mutex};

use url::Url;

use crate::app_port::{HostApp, LlmChannelConfig, LlmSecretSlot, SealedSecret, SecretGuard};
use crate::config::{HostRuntimeConfig, L3bRuntimeConfig};
use crate::llm_loopback::{L3bLoopback, embedded_ipv4_address, is_allowed_upstream_ip};
use crate::protocol::{KitError, LlmChannelKind, LlmChannelView};
use crate::supervisor::{SidecarProcessInfo, Supervisor, SupervisorError};

/// 一次 `set_llm_channel` 的受控输入；秘密只可在本对象生命周期内出现一次。
#[derive(Default)]
pub struct SetLlmChannelRequest {
    /// 目标 Channel 种类；缺省时沿用当前种类。
    pub kind: Option<LlmChannelKind>,
    /// BYOK 上游基础 URL。
    pub base_url: Option<String>,
    /// BYOK 模型标识。
    pub model_id: Option<String>,
    /// 预留 Relay 上游基础 URL。
    pub relay_base_url: Option<String>,
    /// 预留 Relay 产品应用标识。
    pub app_key: Option<String>,
    /// 一次性 BYOK API Key。
    pub api_key: Option<String>,
    /// 一次性 Relay access token。
    pub access_token: Option<String>,
    /// 产品请求幂等标识；当前 Channel 提交只把它当作外层运输信息。
    pub client_request_id: Option<String>,
}

impl fmt::Debug for SetLlmChannelRequest {
    /// 调试输出必须脱敏两个一次性秘密。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SetLlmChannelRequest")
            .field("kind", &self.kind)
            // URL 在验证前可能带 query 秘密，因此与 Key 一样只记录存在性。
            .field("base_url", &self.base_url.as_ref().map(|_| "[REDACTED]"))
            .field("model_id", &self.model_id)
            .field(
                "relay_base_url",
                &self.relay_base_url.as_ref().map(|_| "[REDACTED]"),
            )
            .field("app_key", &self.app_key.as_ref().map(|_| "[REDACTED]"))
            .field("api_key", &self.api_key.as_ref().map(|_| "[REDACTED]"))
            .field(
                "access_token",
                &self.access_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("client_request_id", &self.client_request_id)
            .finish()
    }
}

/// 已提交 Channel 变更的非敏感结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelChange {
    /// 是否已持久化并增加 Channel revision。
    pub changed: bool,
    /// 当前进程内的 Channel revision；binding token 必须精确匹配它。
    pub revision: u64,
    /// 不带凭据的 committed view。
    pub view: LlmChannelView,
}

/// Channel 层的稳定失败分类；展示文本和日志均不包含用户输入或秘密。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LlmChannelError {
    /// 还没有可用 BYOK Channel。
    Unconfigured,
    /// 请求缺字段、字段冲突或 URL 基础语法不安全。
    InvalidRequest,
    /// M1 仍没有 Relay 出站实现。
    RelayNotImplemented,
    /// 产品密封端口拒绝了新的明文秘密。
    SealFailed,
    /// 产品持久化端口失败，旧 committed view 保持不变。
    PersistFailed,
    /// 产品解封端口失败或返回空秘密。
    UnsealFailed,
    /// binding token 指向旧 Channel revision。
    StaleChannelRevision,
    /// Channel 内部并发状态不可用。
    StateUnavailable,
    /// sidecar 批量 restart 失败；新的 committed view 仍然有效，可重试。
    RestartFailed,
    /// L3b 监听或进程启动失败；新的 committed view 仍然有效，可重试。
    LifecycleFailed,
}

impl LlmChannelError {
    /// 将 Channel 失败转换为未来 runtime 可直接返还的结构化 KitError。
    pub fn as_kit_error(self) -> KitError {
        match self {
            Self::Unconfigured => {
                KitError::non_retryable("llm_channel_unconfigured", "尚未配置可用的大模型通道")
            }
            Self::InvalidRequest => {
                KitError::non_retryable("invalid_request", "LLM Channel 请求不完整或不符合安全策略")
            }
            Self::RelayNotImplemented => {
                KitError::non_retryable("unsupported", "Relay Channel 尚未实现")
            }
            Self::SealFailed
            | Self::PersistFailed
            | Self::UnsealFailed
            | Self::StateUnavailable => {
                KitError::non_retryable("missing_api_key", "LLM Channel 密封秘密不可用")
            }
            Self::StaleChannelRevision => KitError::non_retryable(
                "llm_channel_unconfigured",
                "LLM Channel 已更新，请重启 sidecar",
            ),
            Self::RestartFailed | Self::LifecycleFailed => KitError {
                code: "sidecar_unavailable".to_string(),
                message: "LLM Channel 已保存，但 sidecar 重启失败，可重试".to_string(),
                details: None,
                request_id: None,
                retryable: true,
                retry_after_ms: None,
            },
        }
    }
}

impl fmt::Display for LlmChannelError {
    /// 错误文本保持通用，避免把上游 URL、产品错误链或秘密带给调用方。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            Self::Unconfigured => "LLM Channel 未配置",
            Self::InvalidRequest => "LLM Channel 请求无效",
            Self::RelayNotImplemented => "Relay Channel 尚未实现",
            Self::SealFailed => "LLM Channel 密封失败",
            Self::PersistFailed => "LLM Channel 持久化失败",
            Self::UnsealFailed => "LLM Channel 解封失败",
            Self::StaleChannelRevision => "LLM Channel revision 已过期",
            Self::StateUnavailable => "LLM Channel 状态不可用",
            Self::RestartFailed => "sidecar 批量重启失败",
            Self::LifecycleFailed => "L3b 或 sidecar 生命周期失败",
        };
        formatter.write_str(text)
    }
}

impl std::error::Error for LlmChannelError {}

/// L3b 在认证 binding token 后临时取得的上游 Channel；秘密守卫不能 Debug/Clone。
pub(crate) struct ResolvedUpstream {
    /// 通过配置保存的用户基础 URL。
    pub base_url: String,
    /// 用户 API Key 的最短生命周期借用容器。
    pub api_key: SecretGuard,
}

/// 锁内的 committed 配置和当前进程 revision。
struct ChannelState {
    config: LlmChannelConfig,
    revision: u64,
    /// 遗留 DNS 配置在本次启动完成全量地址审查前不得拉起 L3b/sidecar。
    launch_ready: bool,
}

/// 产品全局 Channel 的并发安全管理器。
pub struct LlmChannelManager {
    /// 产品端口持有密封、解封和持久化职责。
    app: Arc<dyn HostApp>,
    /// 串行化 seal → persist → committed-view 的事务，防止并发 Set 覆盖 revision。
    operation_lock: Mutex<()>,
    /// 当前 committed view；绝不存放解封后的明文。
    state: Mutex<ChannelState>,
    /// 仅控制设置时是否允许 HTTP loopback 上游；默认 false。
    allow_loopback_llm: bool,
}

impl LlmChannelManager {
    /// 从产品全局持久化配置加载 manager；加载阶段不解封用户秘密。
    pub fn new(app: Arc<dyn HostApp>, allow_loopback_llm: bool) -> Result<Self, LlmChannelError> {
        let config = app
            .load_llm_channel()
            .map_err(|_| LlmChannelError::PersistFailed)?;
        let launch_ready = validate_loaded_config(&config, allow_loopback_llm)?;
        let revision = if matches!(config, LlmChannelConfig::Unconfigured) {
            0
        } else {
            1
        };

        Ok(Self {
            app,
            operation_lock: Mutex::new(()),
            state: Mutex::new(ChannelState {
                config,
                revision,
                launch_ready,
            }),
            allow_loopback_llm,
        })
    }

    /// 返回当前进程 revision；仅已注册相同 revision 的 token 可以使用 L3b。
    pub fn revision(&self) -> u64 {
        self.state
            .lock()
            .map(|state| state.revision)
            .unwrap_or_default()
    }

    /// 生成不含任何凭据的设置页视图。
    pub fn view(&self) -> Result<LlmChannelView, LlmChannelError> {
        let state = self
            .state
            .lock()
            .map_err(|_| LlmChannelError::StateUnavailable)?;
        Ok(view_for_config(&state.config))
    }

    /// 判断是否有已完成当前启动地址审查、可启动的 M1 BYOK Channel。
    pub fn has_active_byok(&self) -> Result<bool, LlmChannelError> {
        let state = self
            .state
            .lock()
            .map_err(|_| LlmChannelError::StateUnavailable)?;
        Ok(state.launch_ready && matches!(state.config, LlmChannelConfig::Byok { .. }))
    }

    /// 在 L3b 绑定端口或 sidecar spawn 前完成已加载配置的全量地址审查。
    ///
    /// 域名配置加载时只保留非敏感 view，不能直接认为可运行；本入口串行化 DNS 审查并在
    /// 成功后才把它标记为可启动。Set 成功的新候选已审查过，重复调用仍会复核地址。
    pub(crate) fn ensure_startable(&self) -> Result<(), LlmChannelError> {
        let _operation = self
            .operation_lock
            .lock()
            .map_err(|_| LlmChannelError::StateUnavailable)?;
        let config = self
            .state
            .lock()
            .map_err(|_| LlmChannelError::StateUnavailable)?
            .config
            .clone();
        match &config {
            LlmChannelConfig::Byok { .. } => {
                validate_byok_candidate_addresses(&config, self.allow_loopback_llm)?;
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| LlmChannelError::StateUnavailable)?;
                state.launch_ready = true;
                Ok(())
            }
            LlmChannelConfig::Relay { enabled: true, .. } => {
                Err(LlmChannelError::RelayNotImplemented)
            }
            LlmChannelConfig::Relay { enabled: false, .. } | LlmChannelConfig::Unconfigured => {
                Err(LlmChannelError::Unconfigured)
            }
        }
    }

    /// 为权威 sidecar TOML 提供当前模型与 revision；不返回用户上游 URL 或秘密。
    pub(crate) fn sidecar_model(&self) -> Result<(String, u64), LlmChannelError> {
        let state = self
            .state
            .lock()
            .map_err(|_| LlmChannelError::StateUnavailable)?;
        match &state.config {
            LlmChannelConfig::Byok { model_id, .. } if state.launch_ready => {
                Ok((model_id.clone(), state.revision))
            }
            LlmChannelConfig::Byok { .. }
            | LlmChannelConfig::Unconfigured
            | LlmChannelConfig::Relay { .. } => Err(LlmChannelError::Unconfigured),
        }
    }

    /// 认证成功后按绑定 revision 解封当前用户 Key；旧 token 必须在此之前已被拒绝。
    pub(crate) fn resolve_for_revision(
        &self,
        binding_revision: u64,
    ) -> Result<ResolvedUpstream, LlmChannelError> {
        let config = {
            let state = self
                .state
                .lock()
                .map_err(|_| LlmChannelError::StateUnavailable)?;
            if state.revision != binding_revision {
                return Err(LlmChannelError::StaleChannelRevision);
            }
            if !state.launch_ready {
                return Err(LlmChannelError::Unconfigured);
            }
            state.config.clone()
        };

        match config {
            LlmChannelConfig::Byok {
                base_url, api_key, ..
            } => {
                // 只在 L3b 认证和 revision 复核之后解封，避免未知 token 触发产品密钥访问。
                let api_key = self
                    .app
                    .unseal_llm_secret(LlmSecretSlot::Byok, &api_key)
                    .map_err(|_| LlmChannelError::UnsealFailed)?;
                if api_key.expose().is_empty() {
                    return Err(LlmChannelError::UnsealFailed);
                }
                Ok(ResolvedUpstream { base_url, api_key })
            }
            LlmChannelConfig::Relay { enabled: true, .. } => {
                Err(LlmChannelError::RelayNotImplemented)
            }
            LlmChannelConfig::Relay { enabled: false, .. } | LlmChannelConfig::Unconfigured => {
                Err(LlmChannelError::Unconfigured)
            }
        }
    }

    /// 执行产品全局 Set 事务：先 seal、再 persist，成功后才更新 committed view。
    pub fn set(&self, request: SetLlmChannelRequest) -> Result<ChannelChange, LlmChannelError> {
        let _operation = self
            .operation_lock
            .lock()
            .map_err(|_| LlmChannelError::StateUnavailable)?;
        let current = {
            let state = self
                .state
                .lock()
                .map_err(|_| LlmChannelError::StateUnavailable)?;
            (state.config.clone(), state.revision)
        };
        let request = NormalizedSetRequest::from(request);

        // 仅 request id 或所有业务字段为空时，严格保留现有 committed view。
        if request.is_empty() {
            return Ok(ChannelChange {
                changed: false,
                revision: current.1,
                view: view_for_config(&current.0),
            });
        }

        let current_config = current.0.clone();
        let next = match current.0 {
            LlmChannelConfig::Unconfigured => self.new_byok_config(request)?,
            LlmChannelConfig::Byok {
                base_url,
                model_id,
                api_key,
            } => self.update_byok_config(request, base_url, model_id, api_key)?,
            // M1 可读取 disabled Relay 配置但不能借此激活或切换到 Relay。
            LlmChannelConfig::Relay { enabled: true, .. } => {
                return Err(LlmChannelError::RelayNotImplemented);
            }
            LlmChannelConfig::Relay { enabled: false, .. } => self.new_byok_config(request)?,
        };
        // 显式传入与现状相同的 kind/URL/model 也不应制造无意义 revision 或重启。
        if next == current_config {
            return Ok(ChannelChange {
                changed: false,
                revision: current.1,
                view: view_for_config(&current_config),
            });
        }

        self.app
            .persist_llm_channel(&next)
            .map_err(|_| LlmChannelError::PersistFailed)?;

        // 持久化成功后，新配置就是唯一 committed view，绝不能在后续 restart 失败时回滚。
        let mut state = self
            .state
            .lock()
            .map_err(|_| LlmChannelError::StateUnavailable)?;
        state.revision = state.revision.saturating_add(1).max(1);
        state.config = next;
        // 新候选在 seal 后已完成全量地址审查；提交成功后可立即作为当前进程的启动配置。
        state.launch_ready = true;
        let change = ChannelChange {
            changed: true,
            revision: state.revision,
            view: view_for_config(&state.config),
        };
        tracing::debug!(
            channel_revision = change.revision,
            "LLM Channel 已提交新的非敏感配置"
        );
        Ok(change)
    }

    /// 首次配置或从 disabled Relay 恢复到 M1 BYOK 时要求完整 BYOK 身份和新明文 Key。
    fn new_byok_config(
        &self,
        request: NormalizedSetRequest,
    ) -> Result<LlmChannelConfig, LlmChannelError> {
        let NormalizedSetRequest {
            kind,
            base_url,
            model_id,
            relay_base_url,
            app_key,
            api_key,
            access_token,
        } = request;
        match kind {
            Some(LlmChannelKind::Relay) => return Err(LlmChannelError::RelayNotImplemented),
            Some(LlmChannelKind::Byok) => {}
            // 首次配置必须显式声明 kind，不能在缺字段时静默猜测 BYOK。
            None => return Err(LlmChannelError::InvalidRequest),
        }
        if relay_base_url.is_some() || app_key.is_some() || access_token.is_some() {
            return Err(LlmChannelError::InvalidRequest);
        }
        let base_url = base_url.ok_or(LlmChannelError::InvalidRequest)?;
        let model_id = model_id.ok_or(LlmChannelError::InvalidRequest)?;
        // URL/model 形状检查不触发 DNS；通过后立即把明文限制在此局部作用域交给产品密封端口。
        validate_byok_identity_shape(&base_url, &model_id, self.allow_loopback_llm)?;
        let api_key = {
            let plain_api_key = api_key.ok_or(LlmChannelError::InvalidRequest)?;
            self.app
                .seal_llm_secret(LlmSecretSlot::Byok, plain_api_key.as_bytes())
                .map_err(|_| LlmChannelError::SealFailed)?
        };
        let candidate = LlmChannelConfig::Byok {
            base_url,
            model_id,
            api_key,
        };
        // 只用已密封候选继续可能阻塞的 DNS/IP 审查；失败时不会触及持久化配置。
        validate_byok_candidate_addresses(&candidate, self.allow_loopback_llm)?;
        Ok(candidate)
    }

    /// 更新既有 BYOK；仅 Key 轮换可省略 URL/model，任何身份变化都必须带新 Key。
    fn update_byok_config(
        &self,
        request: NormalizedSetRequest,
        current_base_url: String,
        current_model_id: String,
        current_api_key: SealedSecret,
    ) -> Result<LlmChannelConfig, LlmChannelError> {
        let NormalizedSetRequest {
            kind,
            base_url,
            model_id,
            relay_base_url,
            app_key,
            api_key,
            access_token,
        } = request;
        if matches!(kind, Some(LlmChannelKind::Relay)) {
            // 切种类同样需要新 Relay 秘密，但 M1 尚未允许 Relay 出站。
            return Err(LlmChannelError::RelayNotImplemented);
        }
        if relay_base_url.is_some() || app_key.is_some() || access_token.is_some() {
            return Err(LlmChannelError::InvalidRequest);
        }

        let base_url = base_url.unwrap_or_else(|| current_base_url.clone());
        let model_id = model_id.unwrap_or_else(|| current_model_id.clone());
        let identity_changed = base_url != current_base_url || model_id != current_model_id;
        if identity_changed && api_key.is_none() {
            return Err(LlmChannelError::InvalidRequest);
        }
        // 与首次设置一致，先做无网络形状校验，再立即密封任何新明文 Key。
        validate_byok_identity_shape(&base_url, &model_id, self.allow_loopback_llm)?;
        let api_key = match api_key {
            Some(plain_api_key) => self
                .app
                .seal_llm_secret(LlmSecretSlot::Byok, plain_api_key.as_bytes())
                .map_err(|_| LlmChannelError::SealFailed)?,
            None => current_api_key,
        };
        let candidate = LlmChannelConfig::Byok {
            base_url,
            model_id,
            api_key,
        };
        validate_byok_candidate_addresses(&candidate, self.allow_loopback_llm)?;
        Ok(candidate)
    }
}

/// 将一次性请求统一归一化；空字符串按协议视为“未提供”。
struct NormalizedSetRequest {
    kind: Option<LlmChannelKind>,
    base_url: Option<String>,
    model_id: Option<String>,
    relay_base_url: Option<String>,
    app_key: Option<String>,
    api_key: Option<String>,
    access_token: Option<String>,
}

impl NormalizedSetRequest {
    /// 判断是否只有可选的外层 client request id，因而属于 no-op。
    fn is_empty(&self) -> bool {
        self.kind.is_none()
            && self.base_url.is_none()
            && self.model_id.is_none()
            && self.relay_base_url.is_none()
            && self.app_key.is_none()
            && self.api_key.is_none()
            && self.access_token.is_none()
    }
}

impl From<SetLlmChannelRequest> for NormalizedSetRequest {
    /// 空白 URL/model/token 不触发“修改旧配置”，保持协议规定的空字段 no-op 语义。
    fn from(request: SetLlmChannelRequest) -> Self {
        Self {
            kind: request.kind,
            base_url: non_empty_trimmed(request.base_url),
            model_id: non_empty_trimmed(request.model_id),
            relay_base_url: non_empty_trimmed(request.relay_base_url),
            app_key: non_empty_trimmed(request.app_key),
            api_key: non_empty_preserved(request.api_key),
            access_token: non_empty_preserved(request.access_token),
        }
    }
}

/// 文本配置去除意外前后空白；空值依协议等价于未提供。
fn non_empty_trimmed(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let value = value.trim().to_string();
        (!value.is_empty()).then_some(value)
    })
}

/// 秘密只判断空白，不改变任何有效字节，避免悄悄改变用户提供的 token。
fn non_empty_preserved(value: Option<String>) -> Option<String> {
    value.and_then(|value| (!value.trim().is_empty()).then_some(value))
}

/// 对持久化配置做不解封的 fail-closed 加载审查，返回它是否已可启动。
///
/// URL 形状错误和可立即判定的危险 IP literal 直接拒绝；DNS hostname 保留给设置页读取，
/// 但先标为不可启动，直到 L3b/sidecar 启动前的全量地址审查成功。
fn validate_loaded_config(
    config: &LlmChannelConfig,
    allow_loopback_llm: bool,
) -> Result<bool, LlmChannelError> {
    match config {
        LlmChannelConfig::Unconfigured => Ok(false),
        LlmChannelConfig::Byok {
            base_url, model_id, ..
        } => {
            validate_byok_identity_shape(base_url, model_id, allow_loopback_llm)?;
            let parsed = validate_byok_url_shape(base_url, allow_loopback_llm)?;
            match parsed.host().ok_or(LlmChannelError::InvalidRequest)? {
                // IP literal 无须等待 DNS；加载阶段就按与请求相同的 SSRF 分类直接 fail-closed。
                url::Host::Ipv4(address) => {
                    is_allowed_upstream_ip(IpAddr::V4(address), allow_loopback_llm)
                        .then_some(true)
                        .ok_or(LlmChannelError::InvalidRequest)
                }
                url::Host::Ipv6(address) => {
                    is_allowed_upstream_ip(IpAddr::V6(address), allow_loopback_llm)
                        .then_some(true)
                        .ok_or(LlmChannelError::InvalidRequest)
                }
                // 域名的 A/AAAA 结果不能仅凭 URL 形状信任；不启动，直到 ensure_startable 复核。
                url::Host::Domain(_) => Ok(false),
            }
        }
        LlmChannelConfig::Relay { enabled: true, .. } => Err(LlmChannelError::RelayNotImplemented),
        LlmChannelConfig::Relay { enabled: false, .. } => Ok(false),
    }
}

/// 先校验不会写入 view 或日志的 BYOK URL 形状；只允许 HTTPS，开发开关下才可用精确
/// IP loopback 的 HTTP。地址解析与全量 IP 审查由已密封候选和每次出站路径执行。
fn validate_byok_url_shape(
    base_url: &str,
    allow_loopback_llm: bool,
) -> Result<Url, LlmChannelError> {
    let parsed = Url::parse(base_url).map_err(|_| LlmChannelError::InvalidRequest)?;
    if parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(LlmChannelError::InvalidRequest);
    }
    match parsed.scheme() {
        "https" => Ok(parsed),
        "http" if allow_loopback_llm && parsed.host().is_some_and(is_loopback_host) => Ok(parsed),
        _ => Err(LlmChannelError::InvalidRequest),
    }
}

/// 仅做无网络的 BYOK 身份形状校验；成功后调用方必须立即密封任何新明文 Key。
fn validate_byok_identity_shape(
    base_url: &str,
    model_id: &str,
    allow_loopback_llm: bool,
) -> Result<(), LlmChannelError> {
    if model_id.trim().is_empty() {
        return Err(LlmChannelError::InvalidRequest);
    }
    validate_byok_url_shape(base_url, allow_loopback_llm).map(|_| ())
}

/// 对已密封候选执行全量 DNS/IP 审查；失败时该候选不得持久化或作为启动配置使用。
fn validate_byok_candidate_addresses(
    candidate: &LlmChannelConfig,
    allow_loopback_llm: bool,
) -> Result<(), LlmChannelError> {
    let LlmChannelConfig::Byok {
        base_url, model_id, ..
    } = candidate
    else {
        return Err(LlmChannelError::InvalidRequest);
    };
    validate_byok_identity_shape(base_url, model_id, allow_loopback_llm)?;
    let parsed = validate_byok_url_shape(base_url, allow_loopback_llm)?;
    let port = parsed
        .port_or_known_default()
        .ok_or(LlmChannelError::InvalidRequest)?;
    // URL 的 IPv6 literal host_str 不含方括号，不能可靠交给 ToSocketAddrs 当 hostname；
    // literal 直接构造 SocketAddr，只有域名才需要 DNS。
    let addresses: Vec<SocketAddr> = match parsed.host().ok_or(LlmChannelError::InvalidRequest)? {
        url::Host::Ipv4(address) => vec![SocketAddr::new(IpAddr::V4(address), port)],
        url::Host::Ipv6(address) => vec![SocketAddr::new(IpAddr::V6(address), port)],
        url::Host::Domain(host) => (host, port)
            .to_socket_addrs()
            .map_err(|_| LlmChannelError::InvalidRequest)?
            .collect(),
    };
    if addresses.is_empty()
        || addresses
            .iter()
            .any(|address| !is_allowed_upstream_ip(address.ip(), allow_loopback_llm))
    {
        return Err(LlmChannelError::InvalidRequest);
    }
    if parsed.scheme() == "http" && !addresses.iter().all(|address| is_loopback_ip(address.ip())) {
        return Err(LlmChannelError::InvalidRequest);
    }
    Ok(())
}

/// 判断原生或 IPv4-embedded 地址是否等价于回环，用于严格的开发 HTTP 例外。
pub(crate) fn is_loopback_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(address) => address.is_loopback(),
        IpAddr::V6(address) => {
            address.is_loopback()
                || embedded_ipv4_address(address).is_some_and(|embedded| embedded.is_loopback())
        }
    }
}

/// HTTP 明文只可用于显式开发测试的 IP 回环地址，不允许 hostname 绕过后续 DNS 复核。
fn is_loopback_host(host: url::Host<&str>) -> bool {
    match host {
        url::Host::Ipv4(address) => is_loopback_ip(IpAddr::V4(address)),
        url::Host::Ipv6(address) => is_loopback_ip(IpAddr::V6(address)),
        url::Host::Domain(_) => false,
    }
}

/// 由持久化配置生成设置页 view；密封载体存在不代表已经解封。
fn view_for_config(config: &LlmChannelConfig) -> LlmChannelView {
    match config {
        LlmChannelConfig::Unconfigured => LlmChannelView::default(),
        LlmChannelConfig::Byok {
            base_url, model_id, ..
        } => LlmChannelView {
            kind: Some(LlmChannelKind::Byok),
            key_present: true,
            token_present: false,
            model_selectable: true,
            base_url: Some(base_url.clone()),
            model_id: Some(model_id.clone()),
        },
        LlmChannelConfig::Relay {
            relay_base_url,
            enabled: _,
            ..
        } => LlmChannelView {
            kind: Some(LlmChannelKind::Relay),
            key_present: false,
            token_present: true,
            model_selectable: false,
            base_url: Some(relay_base_url.clone()),
            model_id: None,
        },
    }
}

/// LLM Channel、L3b 与真实 sidecar launch 的组合入口。
///
/// Task 7b 会将此对象持有在 `HostRuntime` 中；本任务保持它独立，避免提前实现 ACP
/// dispatch 状态机，同时使 Channel 全局变更可以真实重启已有 scope。
pub struct LlmChannelService {
    manager: Arc<LlmChannelManager>,
    supervisor: Arc<Supervisor>,
    l3b_config: L3bRuntimeConfig,
    loopback: Mutex<Option<Arc<L3bLoopback>>>,
    /// 串行化真实 launch 与全局换通道，避免 launch 在旧 revision 注册 token 后漏过 restart。
    lifecycle_lock: Mutex<()>,
}

impl LlmChannelService {
    /// 用产品端口和运行配置构造服务；未配置 Channel 时不会绑定任何网络端口。
    pub fn new<A>(app: Arc<A>, config: HostRuntimeConfig) -> Result<Self, LlmChannelError>
    where
        A: HostApp + 'static,
    {
        // 在公开构造边界保留具体产品类型的 Arc ergonomics，内部只持有领域 trait 对象。
        let app: Arc<dyn HostApp> = app;
        let manager = Arc::new(LlmChannelManager::new(
            Arc::clone(&app),
            config.l3b.allow_loopback_llm,
        )?);
        let supervisor = Supervisor::new(config.clone(), app.app_id())
            .map_err(map_supervisor_error_to_lifecycle)?;
        Ok(Self {
            manager,
            supervisor: Arc::new(supervisor),
            l3b_config: config.l3b,
            loopback: Mutex::new(None),
            lifecycle_lock: Mutex::new(()),
        })
    }

    /// 返回不含凭据的当前 committed view。
    pub fn view(&self) -> Result<LlmChannelView, LlmChannelError> {
        self.manager.view()
    }

    /// 返回已有 L3b 的监听地址；未配置或尚无 scope launch 时为 `None`。
    pub fn loopback_addr(&self) -> Option<std::net::SocketAddr> {
        self.loopback
            .lock()
            .ok()
            .and_then(|loopback| loopback.as_ref().map(|server| server.local_addr()))
    }

    /// 启动一个 scope：先确保 L3b 正在监听，随后由 Supervisor 注册 token、写 TOML、spawn。
    pub fn launch_scope(
        &self,
        scope: impl AsRef<str>,
    ) -> Result<SidecarProcessInfo, LlmChannelError> {
        let _lifecycle = self
            .lifecycle_lock
            .lock()
            .map_err(|_| LlmChannelError::StateUnavailable)?;
        let loopback = self.ensure_loopback()?;
        self.supervisor
            .launch_sidecar(scope.as_ref(), &loopback, &self.manager)
            .map_err(map_supervisor_error_to_lifecycle)
    }

    /// 提交全局 Channel，再使所有旧 token 失效并 drain/restart 所有活跃 scope。
    pub fn set(&self, request: SetLlmChannelRequest) -> Result<ChannelChange, LlmChannelError> {
        let _lifecycle = self
            .lifecycle_lock
            .lock()
            .map_err(|_| LlmChannelError::StateUnavailable)?;
        let change = self.manager.set(request)?;
        if !change.changed {
            return Ok(change);
        }

        // persist 后立即使旧 token 失效；后续 restart 失败也绝不恢复旧 view 或旧 token。
        if let Some(loopback) = self
            .loopback
            .lock()
            .map_err(|_| LlmChannelError::StateUnavailable)?
            .as_ref()
            .cloned()
        {
            loopback.registry().invalidate_all();
        }

        if self
            .supervisor
            .live_scope_ids()
            .map_err(map_supervisor_error_to_lifecycle)?
            .is_empty()
        {
            return Ok(change);
        }
        let loopback = self.ensure_loopback()?;
        loopback.registry().invalidate_all();
        self.supervisor
            .restart_live_scopes(&loopback, &self.manager)
            .map_err(|_| LlmChannelError::RestartFailed)?;
        Ok(change)
    }

    /// 配置有效时延迟创建唯一进程级监听；监听地址严格由 L3bRuntimeConfig 约束。
    fn ensure_loopback(&self) -> Result<Arc<L3bLoopback>, LlmChannelError> {
        // 对重启后加载的 DNS 配置先完成全量地址审查，禁止仅凭 URL 形状产生可运行 listener。
        self.manager.ensure_startable()?;
        if !self.manager.has_active_byok()? {
            return Err(LlmChannelError::Unconfigured);
        }
        let mut current = self
            .loopback
            .lock()
            .map_err(|_| LlmChannelError::StateUnavailable)?;
        if let Some(loopback) = current.as_ref() {
            return Ok(Arc::clone(loopback));
        }
        let loopback = Arc::new(
            L3bLoopback::start(Arc::clone(&self.manager), self.l3b_config.clone())
                .map_err(|_| LlmChannelError::LifecycleFailed)?,
        );
        *current = Some(Arc::clone(&loopback));
        Ok(loopback)
    }
}

/// 不保留 Supervisor I/O 错误链，避免上层透传子进程环境或路径诊断。
fn map_supervisor_error_to_lifecycle(error: SupervisorError) -> LlmChannelError {
    match error {
        SupervisorError::StateUnavailable => LlmChannelError::StateUnavailable,
        _ => LlmChannelError::LifecycleFailed,
    }
}

/// 保留 `IpAddr` 导入的编译期锚点，确保配置文档中地址语义不被意外替换为字符串。
#[allow(dead_code)]
fn _l3b_address_type_marker(_: IpAddr) {}
