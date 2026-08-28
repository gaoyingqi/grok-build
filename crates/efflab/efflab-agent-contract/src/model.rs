//! sidecar 模型与最小 runtime 配置所需的 DTO。

use std::collections::{BTreeMap, BTreeSet};

use serde::de::Error as DeError;
use serde::ser::{Error as SerError, SerializeMap, SerializeStruct};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::mcp_config::{ApprovedMcpConfig, McpServerSpec};

/// Host 写入旧版权威模型配置时使用的描述；不承载用户密钥。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SidecarModelSpec {
    /// 配置中的模型标识。
    pub model: String,
    /// sidecar 应连接的受控基础 URL。
    pub base_url: String,
    /// 模型展示名；当前权威合同固定为 `BYOK`。
    pub name: String,
    /// 固定的 API 后端标识。
    pub api_backend: String,
    /// sidecar 读取短生命周期绑定令牌的环境变量名。
    pub env_key: String,
}

/// S1 最小 sidecar 的完整启动配置；字段和嵌套表均为闭集。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeConfigV1 {
    /// 配置 schema 主版本。
    pub schema_version: u32,
    /// 不包含自身字段的规范化配置摘要。
    pub runtime_revision: String,
    /// 会话存储格式版本。
    pub session_store_version: u32,
    /// Host 已规范化的绝对 UTF-8 会话工作目录。
    pub session_cwd: String,
    /// sidecar 使用的 Host L3b 回环模型。
    pub model: LoopbackModelSpec,
    /// Host 审核后的 MCP server 集合；仅使用 runtime-only wire。
    #[serde(with = "runtime_approved_mcp")]
    pub approved_mcp: ApprovedMcpConfig,
    /// sidecar 可期待的、按字典序稳定化的工具名集合。
    pub expected_tools: BTreeSet<String>,
}

/// 只允许连接 Host L3b 的回环模型描述；不承载用户密钥。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoopbackModelSpec {
    /// 上游用户模型标识。
    pub model_id: String,
    /// Host L3b 的字面量回环基础 URL。
    pub base_url: String,
    /// 固定的模型后端。
    pub backend: String,
    /// 固定的绑定令牌环境变量名。
    pub token_env: String,
}

/// runtime `approved_mcp` 表的闭集 wire。
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeApprovedMcpWire {
    servers: BTreeMap<String, RuntimeMcpServerWire>,
}

/// runtime server 表的闭集 wire；`command`/`args` 仅用于识别并拒绝 stdio。
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeMcpServerWire {
    url: Option<String>,
    command: Option<String>,
    args: Option<Vec<String>>,
}

impl RuntimeApprovedMcpWire {
    /// 将 runtime-only wire 转为旧内部 DTO，随后由 loader 统一拒绝 stdio。
    fn into_config(self) -> Result<ApprovedMcpConfig, String> {
        let mut servers = BTreeMap::new();
        for (name, server) in self.servers {
            let server = server.into_spec(&name)?;
            servers.insert(name, server);
        }
        Ok(ApprovedMcpConfig { servers })
    }
}

impl RuntimeMcpServerWire {
    /// 根据字段形状恢复内部 transport，并对缺失关键字段给出字段级错误。
    fn into_spec(self, name: &str) -> Result<McpServerSpec, String> {
        match (self.url, self.command, self.args) {
            (Some(url), None, None) => Ok(McpServerSpec::Http { url }),
            (Some(_), Some(_), _) | (Some(_), None, Some(_)) => Err(format!(
                "approved_mcp.servers.{name} 的 url 不能与 command/args 同时存在"
            )),
            (None, Some(command), args) => Ok(McpServerSpec::Stdio {
                command: command.into(),
                args: args.unwrap_or_default(),
            }),
            (None, None, Some(_)) => Err(format!("approved_mcp.servers.{name} 缺失 command 字段")),
            (None, None, None) => Err(format!("approved_mcp.servers.{name} 缺失 url 字段")),
        }
    }
}

/// runtime-only MCP server 序列化视图；stdio 不得进入任何 runtime wire。
struct RuntimeMcpServerRef<'a>(&'a McpServerSpec);

impl Serialize for RuntimeMcpServerRef<'_> {
    /// 只序列化 HTTP URL，遇到 stdio 直接返回稳定拒绝错误。
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.0 {
            McpServerSpec::Stdio { .. } => Err(S::Error::custom("stdio_mcp_unavailable")),
            McpServerSpec::Http { url } => {
                let mut state = serializer.serialize_struct("RuntimeMcpServer", 1)?;
                state.serialize_field("url", url)?;
                state.end()
            }
        }
    }
}

/// runtime-only MCP server map 序列化视图。
struct RuntimeMcpServersRef<'a>(&'a BTreeMap<String, McpServerSpec>);

impl Serialize for RuntimeMcpServersRef<'_> {
    /// 按 server 名稳定排序输出，并隔离旧 DTO 的 command/args 字段。
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (name, server) in self.0 {
            map.serialize_entry(name, &RuntimeMcpServerRef(server))?;
        }
        map.end()
    }
}

/// runtime-only `approved_mcp` 序列化视图。
struct RuntimeApprovedMcpRef<'a>(&'a ApprovedMcpConfig);

impl Serialize for RuntimeApprovedMcpRef<'_> {
    /// 仅写出声明过的 `servers` 外壳。
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("RuntimeApprovedMcp", 1)?;
        state.serialize_field("servers", &RuntimeMcpServersRef(&self.0.servers))?;
        state.end()
    }
}

/// 只给 RuntimeConfigV1 的 approved_mcp 字段提供 serde，避免扩大旧 MCP DTO API。
mod runtime_approved_mcp {
    use super::*;

    /// 将旧内部 MCP DTO 编码为 runtime-only wire。
    pub fn serialize<S>(value: &ApprovedMcpConfig, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        RuntimeApprovedMcpRef(value).serialize(serializer)
    }

    /// 从 runtime-only wire 解码为内部 DTO，保留 stdio 供统一拒绝 helper 处理。
    pub fn deserialize<'de, D>(deserializer: D) -> Result<ApprovedMcpConfig, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = RuntimeApprovedMcpWire::deserialize(deserializer)?;
        wire.into_config().map_err(D::Error::custom)
    }
}
