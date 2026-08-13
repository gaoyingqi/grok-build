//! sidecar 模型渲染所需的最小 DTO。

/// Host 写入权威模型配置时使用的描述；不承载用户密钥。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SidecarModelSpec {
    /// 配置中的模型标识。
    pub model: String,
    /// sidecar 应连接的受控基础 URL。
    pub base_url: String,
    /// 配置表名。
    pub name: String,
    /// 固定的 API 后端标识。
    pub api_backend: String,
    /// sidecar 读取短生命周期绑定令牌的环境变量名。
    pub env_key: String,
}
