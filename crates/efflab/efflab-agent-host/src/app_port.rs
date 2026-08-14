//! 产品领域端口与编译所需的最小 DTO。
//!
//! 这些类型定义产品领域端口；Task 7 使用其中的 Channel 密封接缝，MCP 启动与
//! mention 文本展开仍由后续专项任务实现。

use std::fmt;

use anyhow::Result;
use serde::{Deserialize, Serialize};

/// 产品定义的不透明作用域标识，Host 不将其解释为文件路径。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ScopeId(pub String);

/// 产品可解析的 mention 标识；提交指纹按 `(kind, id)` 字典序使用此 DTO。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct MentionId {
    /// 领域实体种类。
    pub kind: String,
    /// 领域实体不透明标识。
    pub id: String,
}

/// 产品将 mention 解析后的最小结果；具体文本拼装不属于本任务。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedMention {
    /// 原始领域标识。
    pub id: MentionId,
    /// 产品提供的安全展示或展开文本。
    pub text: String,
}

/// 受控 MCP 规格占位；实际 server DTO 由 contract crate 提供。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ApprovedMcpSpec;

/// 已密封秘密的产品存储载体；Host 不解释其内部字节。
///
/// 密封数据仍可能在测试 adapter 中是可逆字节，因此故意不派生 `Debug`，避免任何
/// 调试或错误链意外回显其内容。
#[derive(Clone, PartialEq, Eq)]
pub struct SealedSecret(Vec<u8>);

impl fmt::Debug for SealedSecret {
    /// 只标记密封载体存在，绝不暴露其长度或字节。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SealedSecret([REDACTED])")
    }
}

impl SealedSecret {
    /// 构造仅供产品 adapter 或测试实现使用的密封载体。
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// 将密封载体交还给产品 adapter；Host 不记录或序列化其中内容。
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// 短生命周期的解封秘密容器。
///
/// 故意不实现 `Debug`、`Clone` 与 serde，避免 Key/token 意外进入日志或线协议。
pub struct SecretGuard(Vec<u8>);

impl SecretGuard {
    /// 构造只供产品 adapter 或测试实现使用的秘密守卫。
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// 仅在受控 L3b 出站路径中借用秘密字节；不得用于日志、序列化或 sidecar 配置。
    pub fn expose(&self) -> &[u8] {
        &self.0
    }
}

/// Channel 密封秘密的产品存储用途。
///
/// 产品 adapter 可以覆写 [`HostApp::seal_llm_secret`] / [`HostApp::unseal_llm_secret`]
/// 并据此使用不同的存储槽与 KDF salt；默认实现仍兼容既有通用密封端口。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LlmSecretSlot {
    /// 用户自带模型的 API Key。
    Byok,
    /// 未来 Relay 的访问令牌。
    Relay,
}

/// 产品全局 LLM Channel 的持久化配置。
///
/// 用户端点和模型标识是非敏感配置；秘密只保存为产品提供的 [`SealedSecret`]。
/// M1 仅会激活 `Byok`，但 `Relay` 形状从第一天保留以免以后改变 Host/sidecar 合同。
#[derive(Clone, PartialEq, Eq)]
pub enum LlmChannelConfig {
    /// 尚未配置通道；此时 Host 不监听 L3b，也不得拉起 sidecar。
    Unconfigured,
    /// 用户自带 Chat Completions 端点、模型与密封 API Key。
    Byok {
        /// 用户设置页提供的上游基础 URL；永不写入 sidecar TOML。
        base_url: String,
        /// 用户选择的 Chat Completions 模型标识。
        model_id: String,
        /// 仅由 Host 在受控出站路径中短暂解封的用户 API Key。
        api_key: SealedSecret,
    },
    /// 预留的 Relay 通道；M1 保持 `enabled = false` 并 fail-closed。
    Relay {
        /// Relay Chat Completions 基础 URL。
        relay_base_url: String,
        /// 产品级应用标识，不是用户秘密。
        app_key: String,
        /// Relay 访问令牌的密封载体。
        access_token: SealedSecret,
        /// M1 固定为 false；true 必须报告 Relay 尚未实现。
        enabled: bool,
    },
}

impl fmt::Debug for LlmChannelConfig {
    /// Channel 配置可能来自尚未校验的持久化数据；调试形状不回显 URL、app key 或密封载体。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unconfigured => formatter.write_str("LlmChannelConfig::Unconfigured"),
            Self::Byok {
                model_id, api_key, ..
            } => formatter
                .debug_struct("LlmChannelConfig::Byok")
                .field("base_url", &"[REDACTED]")
                .field("model_id", model_id)
                .field("api_key", api_key)
                .finish(),
            Self::Relay {
                access_token,
                enabled,
                ..
            } => formatter
                .debug_struct("LlmChannelConfig::Relay")
                .field("relay_base_url", &"[REDACTED]")
                .field("app_key", &"[REDACTED]")
                .field("access_token", access_token)
                .field("enabled", enabled)
                .finish(),
        }
    }
}

impl Default for LlmChannelConfig {
    /// 缺省持久化状态是未配置，而不是任何内置模型回退。
    fn default() -> Self {
        Self::Unconfigured
    }
}

/// 领域端口。不含 emit；事件运输由 [`crate::KitEventSink`] 独立承载。
pub trait HostApp: Send + Sync {
    /// 返回稳定的产品标识。
    fn app_id(&self) -> &str;

    /// 持久化产品全局的 Channel 配置；Task 7 的提交事务只在密封成功后调用。
    fn persist_llm_channel(&self, cfg: &LlmChannelConfig) -> Result<()>;

    /// 读取产品全局的 Channel 配置；Task 7 构造 Channel manager 时调用。
    fn load_llm_channel(&self) -> Result<LlmChannelConfig>;

    /// 由产品密封一次性秘密；默认槽位实现供兼容的产品 adapter 使用。
    fn seal_secret(&self, plain: &[u8]) -> Result<SealedSecret>;

    /// 由产品解封秘密；L3b 认证并复核 revision 后才可调用。
    fn unseal_secret(&self, sealed: &SealedSecret) -> Result<SecretGuard>;

    /// 按 Channel 槽位密封秘密。
    ///
    /// 默认拒绝所有槽位。产品 adapter 必须显式声明自己如何把 Byok / Relay 隔离到不同的
    /// 存储槽或 KDF salt，不能因遗留通用端口而把两个凭据静默落到同一处。
    fn seal_llm_secret(&self, slot: LlmSecretSlot, _plain: &[u8]) -> Result<SealedSecret> {
        Err(anyhow::anyhow!(
            "HostApp 未声明 {slot:?} LLM 秘密槽的密封实现"
        ))
    }

    /// 按 Channel 槽位解封秘密。
    ///
    /// 默认拒绝所有槽位；覆写实现必须在槽位不匹配时 fail-closed，避免 Byok 和 Relay
    /// 秘密互换使用。
    fn unseal_llm_secret(
        &self,
        slot: LlmSecretSlot,
        _sealed: &SealedSecret,
    ) -> Result<SecretGuard> {
        Err(anyhow::anyhow!(
            "HostApp 未声明 {slot:?} LLM 秘密槽的解封实现"
        ))
    }

    /// 返回当前作用域可用的 MCP 规格；Task 1 不调用。
    fn mcp_for_scope(&self, scope: &ScopeId) -> Result<ApprovedMcpSpec>;

    /// 可选的领域 mention 解析端口；缺省表示产品不支持 mention。
    fn mentions(&self) -> Option<&dyn HostAppMentions> {
        None
    }
}

/// 产品按领域解析 `@` mention 的独立端口。
pub trait HostAppMentions: Send + Sync {
    /// 将已授权 scope 中的标识解析成安全文本；Task 1 不调用。
    fn resolve_mentions(&self, scope: &ScopeId, ids: &[MentionId]) -> Result<Vec<ResolvedMention>>;
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::{
        ApprovedMcpSpec, HostApp, LlmChannelConfig, LlmSecretSlot, ScopeId, SealedSecret,
        SecretGuard,
    };

    /// 仅实现旧通用密封端口的 legacy adapter；它没有声明任何 Channel 槽位绑定。
    struct LegacyUnslottedApp;

    impl HostApp for LegacyUnslottedApp {
        fn app_id(&self) -> &str {
            "legacy-unslotted-test"
        }

        fn persist_llm_channel(&self, _cfg: &LlmChannelConfig) -> Result<()> {
            Ok(())
        }

        fn load_llm_channel(&self) -> Result<LlmChannelConfig> {
            Ok(LlmChannelConfig::Unconfigured)
        }

        fn seal_secret(&self, plain: &[u8]) -> Result<SealedSecret> {
            Ok(SealedSecret::new(plain.to_vec()))
        }

        fn unseal_secret(&self, sealed: &SealedSecret) -> Result<SecretGuard> {
            Ok(SecretGuard::new(sealed.as_bytes().to_vec()))
        }

        fn mcp_for_scope(&self, _scope: &ScopeId) -> Result<ApprovedMcpSpec> {
            Ok(ApprovedMcpSpec::default())
        }
    }

    /// 未显式绑定槽位的产品 adapter 不能把 BYOK 与 Relay 静默落到同一秘密存储。
    #[test]
    fn default_llm_secret_slots_fail_closed_without_explicit_adapter_binding() {
        let app = LegacyUnslottedApp;
        let sealed = app
            .seal_secret(b"test-secret")
            .expect("legacy 通用密封桩必须可用");

        for slot in [LlmSecretSlot::Byok, LlmSecretSlot::Relay] {
            assert!(
                app.seal_llm_secret(slot, b"test-secret").is_err(),
                "未声明的 {slot:?} 密封槽必须 fail-closed"
            );
            assert!(
                app.unseal_llm_secret(slot, &sealed).is_err(),
                "未声明的 {slot:?} 解封槽必须 fail-closed"
            );
        }
    }
}
