//! 产品领域端口与编译所需的最小 DTO。
//!
//! 这些类型只建立 M0 接缝；Channel 密封、MCP 启动和 mention 文本展开均留待后续
//! 专项任务实现。

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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedSecret(Vec<u8>);

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

    /// 仅在后续受控 L3b 出站路径中借用秘密字节；本任务不使用该方法。
    pub fn expose(&self) -> &[u8] {
        &self.0
    }
}

/// 产品全局 LLM Channel 配置的最小占位类型；不包含具体业务语义。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LlmChannelConfig;

/// 领域端口。不含 emit；事件运输由 [`crate::KitEventSink`] 独立承载。
pub trait HostApp: Send + Sync {
    /// 返回稳定的产品标识。
    fn app_id(&self) -> &str;

    /// 持久化产品全局的 Channel 配置；Task 1 不调用。
    fn persist_llm_channel(&self, cfg: &LlmChannelConfig) -> Result<()>;

    /// 读取产品全局的 Channel 配置；Task 1 不调用。
    fn load_llm_channel(&self) -> Result<LlmChannelConfig>;

    /// 由产品密封一次性秘密；Task 1 不调用。
    fn seal_secret(&self, plain: &[u8]) -> Result<SealedSecret>;

    /// 由产品解封秘密；Task 1 不调用。
    fn unseal_secret(&self, sealed: &SealedSecret) -> Result<SecretGuard>;

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
