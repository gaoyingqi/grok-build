//! 经审核的 MCP 配置类型与 TOML 读写边界。
//!
//! 此模块不依赖 sidecar runtime；它只负责将有限 MCP TOML 转换成 Host 与
//! sidecar 可共享的受控 DTO，并拒绝不受支持的输入形状。

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

const MCP_SERVERS_KEY: &str = "mcp_servers";

/// 已通过输入边界校验、可写入 sidecar 私有配置的 MCP server 集合。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ApprovedMcpConfig {
    /// 以稳定顺序保存的 server 名与受控规格。
    pub servers: BTreeMap<String, McpServerSpec>,
}

impl ApprovedMcpConfig {
    /// 读取并校验唯一允许的 MCP TOML 输入格式。
    pub fn load(path: &Path, exec_root: Option<&Path>) -> Result<Self> {
        let config_path = canonicalize_file(path, "--mcp-config")?;
        let source = fs::read_to_string(&config_path)
            .with_context(|| format!("读取 MCP 配置文件失败: {}", config_path.display()))?;
        let document: toml::Value = toml::from_str(&source)
            .with_context(|| format!("解析 MCP TOML 失败: {}", config_path.display()))?;
        validate_top_level_keys(&document, &config_path)?;

        // 再次反序列化为窄结构；重复 TOML key 会在此之前由 TOML 解析器拒绝。
        let raw: RawMcpConfig = toml::from_str(&source)
            .with_context(|| format!("反序列化 MCP server 配置失败: {}", config_path.display()))?;
        let exec_root = exec_root
            .map(|root| canonicalize_directory(root, "--mcp-exec-root"))
            .transpose()?;

        let mut servers = BTreeMap::new();
        for (name, raw_spec) in raw.mcp_servers {
            if name.trim().is_empty() {
                bail!("MCP server 名称不能为空: {}", config_path.display());
            }

            let spec = McpServerSpec::from_raw(&name, raw_spec, exec_root.as_deref())?;
            if servers.insert(name.clone(), spec).is_some() {
                bail!("MCP server 名称重复: {name}");
            }
        }

        Ok(Self { servers })
    }

    /// 将已审核 DTO 写成唯一允许的 MCP TOML 表，供 Host 物化配置时复用。
    pub fn write_toml(&self) -> Result<String> {
        let mut document = toml::map::Map::new();
        let mut servers = toml::map::Map::new();

        for (name, spec) in &self.servers {
            if name.trim().is_empty() {
                bail!("MCP server 名称不能为空");
            }

            let mut table = toml::map::Map::new();
            match spec {
                McpServerSpec::Stdio { command, args } => {
                    if !command.is_absolute() {
                        bail!(
                            "受控 stdio MCP server '{name}' 的 command 必须为绝对路径: {}",
                            command.display()
                        );
                    }
                    let command = command.to_str().context(format!(
                        "受控 stdio MCP server '{name}' 的 command 不是 UTF-8 路径: {}",
                        command.display()
                    ))?;
                    table.insert(
                        "command".to_string(),
                        toml::Value::String(command.to_string()),
                    );
                    table.insert(
                        "args".to_string(),
                        toml::Value::Array(args.iter().cloned().map(toml::Value::String).collect()),
                    );
                }
                McpServerSpec::Http { url } => {
                    if url.trim().is_empty() {
                        bail!("受控 HTTP MCP server '{name}' 的 url 不能为空");
                    }
                    table.insert("url".to_string(), toml::Value::String(url.clone()));
                }
            }
            servers.insert(name.clone(), toml::Value::Table(table));
        }

        document.insert(MCP_SERVERS_KEY.to_string(), toml::Value::Table(servers));
        toml::to_string(&toml::Value::Table(document)).context("序列化已审核 MCP TOML 失败")
    }
}

/// 单个已审核 MCP server 的传输规格。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpServerSpec {
    /// 子进程 stdio MCP，只保留归一化后的可执行文件路径与参数。
    Stdio {
        /// 位于受控执行根目录内的绝对可执行文件路径。
        command: PathBuf,
        /// 原样传递给受控可执行文件的参数。
        args: Vec<String>,
    },
    /// 仅允许 loopback 地址的 HTTP MCP。
    Http {
        /// 已验证为 localhost 或 127.0.0.1 的 HTTP URL。
        url: String,
    },
}

impl McpServerSpec {
    /// 将 TOML 原始条目转换为经过安全校验的传输规格。
    fn from_raw(name: &str, raw: RawMcpServerSpec, exec_root: Option<&Path>) -> Result<Self> {
        if raw.env.is_some() {
            bail!("MCP server '{name}' 不允许 env 字段（阶段 0）");
        }

        match (raw.command, raw.url) {
            (Some(command), None) => {
                if command.trim().is_empty() {
                    bail!("stdio MCP server '{name}' 的 command 不能为空");
                }

                let command_path = Path::new(&command);
                if !command_path.is_absolute() {
                    bail!(
                        "stdio MCP server '{name}' 的 command 必须为绝对路径: {}",
                        command_path.display()
                    );
                }

                let exec_root = exec_root.context(format!(
                    "stdio MCP server '{name}' 需要提供 --mcp-exec-root"
                ))?;
                let command = dunce::canonicalize(command_path).with_context(|| {
                    format!(
                        "无法归一化 stdio MCP server '{name}' 的 command: {}",
                        command_path.display()
                    )
                })?;
                if !command.starts_with(exec_root) {
                    bail!(
                        "stdio MCP server '{name}' 的 command 不在 --mcp-exec-root 内: {}",
                        command.display()
                    );
                }

                // 目录虽然也可能位于受控根目录内，但不能作为待启动的可执行文件。
                let metadata = fs::metadata(&command).with_context(|| {
                    format!(
                        "无法读取 stdio MCP server '{name}' 的 command 元数据: {}",
                        command.display()
                    )
                })?;
                if !metadata.is_file() {
                    bail!(
                        "stdio MCP server '{name}' 的 command 必须指向常规文件: {}",
                        command.display()
                    );
                }

                Ok(Self::Stdio {
                    command,
                    args: raw.args,
                })
            }
            (None, Some(url)) => {
                if url.trim().is_empty() {
                    bail!("HTTP MCP server '{name}' 的 url 不能为空");
                }
                if !is_loopback_http_url(&url) {
                    bail!("HTTP MCP server '{name}' 的 url 必须使用 localhost 或 127.0.0.1: {url}");
                }

                Ok(Self::Http { url })
            }
            (Some(_), Some(_)) => {
                bail!("MCP server '{name}' 不能同时配置 command 与 url");
            }
            (None, None) => {
                bail!("MCP server '{name}' 必须配置 command 或 url");
            }
        }
    }
}

/// MCP TOML 根结构；顶层键在反序列化前通过 `validate_top_level_keys` 严格检查。
#[derive(Debug, Deserialize)]
struct RawMcpConfig {
    #[serde(default)]
    mcp_servers: BTreeMap<String, RawMcpServerSpec>,
}

/// 仅提取阶段 0 支持的字段，其他字段不会进入最终受控配置。
#[derive(Debug, Deserialize)]
struct RawMcpServerSpec {
    command: Option<String>,
    url: Option<String>,
    #[serde(default)]
    args: Vec<String>,
    env: Option<toml::Value>,
}

/// 要求路径为绝对路径，并使用 dunce 归一化以避免平台路径前缀差异。
fn canonicalize_path(path: &Path, argument_name: &str) -> Result<PathBuf> {
    if !path.is_absolute() {
        bail!("{argument_name} 必须为绝对路径: {}", path.display());
    }

    dunce::canonicalize(path)
        .with_context(|| format!("无法归一化 {argument_name}: {}", path.display()))
}

/// 要求路径存在且为目录，再返回其归一化结果。
fn canonicalize_directory(path: &Path, argument_name: &str) -> Result<PathBuf> {
    let canonical = canonicalize_path(path, argument_name)?;
    let metadata = fs::metadata(&canonical)
        .with_context(|| format!("读取 {argument_name} 元数据失败: {}", canonical.display()))?;
    if !metadata.is_dir() {
        bail!("{argument_name} 必须指向目录: {}", canonical.display());
    }
    Ok(canonical)
}

/// 要求路径存在且为常规文件，再返回其归一化结果。
fn canonicalize_file(path: &Path, argument_name: &str) -> Result<PathBuf> {
    let canonical = canonicalize_path(path, argument_name)?;
    let metadata = fs::metadata(&canonical)
        .with_context(|| format!("读取 {argument_name} 元数据失败: {}", canonical.display()))?;
    if !metadata.is_file() {
        bail!("{argument_name} 必须指向常规文件: {}", canonical.display());
    }
    Ok(canonical)
}

/// MCP 配置只能含有 `mcp_servers` 这一顶层键。
fn validate_top_level_keys(document: &toml::Value, config_path: &Path) -> Result<()> {
    let table = document.as_table().context("MCP TOML 根节点必须是表")?;
    for key in table.keys() {
        if key != MCP_SERVERS_KEY {
            bail!("MCP TOML 包含未知顶层键 '{key}': {}", config_path.display());
        }
    }
    Ok(())
}

/// 严格解析 HTTP URL 的 authority，避免把带前缀或 userinfo 的非 loopback 主机误判为本地。
/// 校验 HTTP MCP URL 是否严格指向 loopback host，供 sidecar 回归测试复用。
pub fn is_loopback_http_url(url: &str) -> bool {
    if url.bytes().any(|byte| byte.is_ascii_whitespace()) {
        return false;
    }

    let Some((scheme, remainder)) = url.split_once("://") else {
        return false;
    };
    if !(scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https")) {
        return false;
    }

    let authority_end = remainder.find(['/', '?', '#']).unwrap_or(remainder.len());
    let authority = &remainder[..authority_end];
    if authority.is_empty() || authority.contains('@') {
        return false;
    }

    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) => (host, Some(port)),
        None => (authority, None),
    };
    if let Some(port) = port
        && port.parse::<u16>().is_err()
    {
        return false;
    }

    host == "127.0.0.1" || host.eq_ignore_ascii_case("localhost")
}

/// 计算 Host 侧审核 MCP 的稳定摘要；规范化 JSON 只保留 HTTP 类型和 literal URL。
pub fn approved_mcp_revision(
    servers: &ApprovedMcpConfig,
    expected_tools: &BTreeSet<String>,
) -> Result<String> {
    // 先调用 Task 3 的统一拒绝入口，stdio 不进入后续路径检查或摘要构造。
    crate::stdio_mcp::deny_stdio_mcp(servers)?;

    let mut normalized_names = BTreeSet::new();
    let mut canonical_servers = BTreeMap::new();
    for (name, server) in &servers.servers {
        if !is_server_name(name) {
            bail!("MCP server 名称非法");
        }
        if !normalized_names.insert(name.to_ascii_lowercase()) {
            bail!("MCP server 名称冲突");
        }

        let url = match server {
            McpServerSpec::Http { url } => url,
            // deny_stdio_mcp 已经处理该分支；这里保持独立调用的稳定错误语义。
            McpServerSpec::Stdio { .. } => return Err(anyhow::anyhow!("stdio_mcp_unavailable")),
        };
        if !crate::render::is_literal_loopback_http_url(url) {
            bail!("MCP server URL 必须是字面量 loopback HTTP 且 path 非空");
        }
        canonical_servers.insert(
            name.as_str(),
            ApprovedMcpRevisionServer {
                kind: "http",
                url: url.as_str(),
            },
        );
    }

    for tool in expected_tools {
        if !is_qualified_tool_name(tool) {
            bail!("MCP qualified tool 名称非法");
        }
    }

    let payload = ApprovedMcpRevisionPayload {
        servers: canonical_servers,
        expected_tools,
    };
    let canonical_json =
        serde_json::to_vec(&payload).context("规范化 ApprovedMcpSpecV1 JSON 失败")?;
    let digest = ApprovedMcpSha256::digest(&canonical_json);
    let hex = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Ok(format!("sha256:{hex}"))
}

/// 摘要中的 server 视图只允许固定 HTTP kind 与已校验 URL。
#[derive(Serialize)]
struct ApprovedMcpRevisionPayload<'a> {
    servers: BTreeMap<&'a str, ApprovedMcpRevisionServer<'a>>,
    expected_tools: &'a BTreeSet<String>,
}

#[derive(Serialize)]
struct ApprovedMcpRevisionServer<'a> {
    kind: &'static str,
    url: &'a str,
}

/// 校验 server 名称的 ASCII 正则、单段分隔语义和 64 字节上限。
pub fn is_server_name(name: &str) -> bool {
    is_name_segment(name) && !name.contains("__") && name.len() <= 64
}

/// 校验 server/tool segment 共用的 ASCII 标识规则。
fn is_name_segment(name: &str) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && (bytes[0].is_ascii_alphabetic() || bytes[0] == b'_')
        && bytes[1..]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

/// 校验 qualified tool 只有一个明确分隔，server 使用完整 validator，tool 保持无长度上限。
/// `approved_mcp_revision` 与 RuntimeConfigV1 loader 共用本规则；完整名称的 1024-byte
/// 持久化上限由 sidecar catalog/record 边界继续执行。
pub fn is_qualified_tool_name(name: &str) -> bool {
    let Some((server, tool)) = name.split_once("__") else {
        return false;
    };
    !tool.contains("__") && is_server_name(server) && is_name_segment(tool)
}

/// 只用于短小审核摘要的纯 Rust SHA-256，不接触秘密或网络数据。
struct ApprovedMcpSha256;

impl ApprovedMcpSha256 {
    /// 按 SHA-256 标准完成 padding、压缩和大端摘要输出。
    fn digest(input: &[u8]) -> [u8; 32] {
        const K: [u32; 64] = [
            0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
            0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
            0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
            0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
            0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
            0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
            0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
            0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
            0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
            0xc67178f2,
        ];
        let mut state = [
            0x6a09e667u32,
            0xbb67ae85,
            0x3c6ef372,
            0xa54ff53a,
            0x510e527f,
            0x9b05688c,
            0x1f83d9ab,
            0x5be0cd19,
        ];
        let bit_len = (input.len() as u64).wrapping_mul(8);
        let padded_len = (input.len() + 9).div_ceil(64) * 64;
        let mut padded = vec![0_u8; padded_len];
        padded[..input.len()].copy_from_slice(input);
        padded[input.len()] = 0x80;
        padded[padded_len - 8..].copy_from_slice(&bit_len.to_be_bytes());

        for chunk in padded.chunks_exact(64) {
            let mut words = [0_u32; 64];
            for (index, bytes) in chunk.chunks_exact(4).take(16).enumerate() {
                words[index] = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
            }
            for index in 16..64 {
                let s0 = words[index - 15].rotate_right(7)
                    ^ words[index - 15].rotate_right(18)
                    ^ (words[index - 15] >> 3);
                let s1 = words[index - 2].rotate_right(17)
                    ^ words[index - 2].rotate_right(19)
                    ^ (words[index - 2] >> 10);
                words[index] = words[index - 16]
                    .wrapping_add(s0)
                    .wrapping_add(words[index - 7])
                    .wrapping_add(s1);
            }

            let mut working = state;
            for index in 0..64 {
                let s1 = working[4].rotate_right(6)
                    ^ working[4].rotate_right(11)
                    ^ working[4].rotate_right(25);
                let choose = (working[4] & working[5]) ^ ((!working[4]) & working[6]);
                let temp1 = working[7]
                    .wrapping_add(s1)
                    .wrapping_add(choose)
                    .wrapping_add(K[index])
                    .wrapping_add(words[index]);
                let s0 = working[0].rotate_right(2)
                    ^ working[0].rotate_right(13)
                    ^ working[0].rotate_right(22);
                let majority = (working[0] & working[1])
                    ^ (working[0] & working[2])
                    ^ (working[1] & working[2]);
                let temp2 = s0.wrapping_add(majority);
                working[7] = working[6];
                working[6] = working[5];
                working[5] = working[4];
                working[4] = working[3].wrapping_add(temp1);
                working[3] = working[2];
                working[2] = working[1];
                working[1] = working[0];
                working[0] = temp1.wrapping_add(temp2);
            }
            for index in 0..8 {
                state[index] = state[index].wrapping_add(working[index]);
            }
        }

        let mut digest = [0_u8; 32];
        for (index, word) in state.iter().enumerate() {
            digest[index * 4..index * 4 + 4].copy_from_slice(&word.to_be_bytes());
        }
        digest
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{ApprovedMcpConfig, McpServerSpec, approved_mcp_revision};

    /// 构造仅含 HTTP server 的审核配置，测试不触发任何进程或网络访问。
    fn http_config<'a>(entries: impl IntoIterator<Item = (&'a str, &'a str)>) -> ApprovedMcpConfig {
        let mut config = ApprovedMcpConfig::default();
        for (name, url) in entries {
            config.servers.insert(
                name.to_string(),
                McpServerSpec::Http {
                    url: url.to_string(),
                },
            );
        }
        config
    }

    /// 计算空审核规格的固定摘要，确保协议没有隐藏的 stdio 形状。
    #[test]
    fn empty_approved_mcp_revision_is_stable() {
        let revision = approved_mcp_revision(&ApprovedMcpConfig::default(), &BTreeSet::new())
            .expect("空审核规格必须能计算摘要");

        assert_eq!(
            revision,
            "sha256:7095b2a0427c1cb2248ed6befc9d832fa7fb34c05aae78a8a083cdfa6eb3f09b"
        );
    }

    /// server 与工具集合按稳定顺序进入摘要，原始 URL/命令不应出现在摘要文本中。
    #[test]
    fn approved_mcp_revision_is_sorted_and_uses_http_only_canonical_shape() {
        let config = http_config([
            ("demo", "http://127.0.0.1:4313/mcp"),
            ("_demo-1", "http://[::1]:4314/mcp"),
        ]);
        let expected_tools = BTreeSet::from([
            "demo__search-tool".to_string(),
            "_demo-1__search".to_string(),
        ]);

        let revision = approved_mcp_revision(&config, &expected_tools)
            .expect("合法 HTTP 审核规格必须能计算摘要");

        assert_eq!(
            revision,
            "sha256:1034f9bb36a8f276fda3819cd7b96f7b020cc629f2d562416800c46a03d394e2"
        );
        assert!(!revision.contains("127.0.0.1"));
        assert!(!revision.contains("command"));
    }

    /// v1 MCP 只接受字面量 loopback HTTP，端口、路径和 URL 附加信息均须符合边界。
    #[test]
    fn approved_mcp_revision_validates_literal_loopback_http_urls() {
        for url in [
            "http://127.0.0.1:1/",
            "http://127.0.0.1:65535/mcp",
            "http://[::1]:4313/mcp",
        ] {
            let config = http_config([("demo", url)]);
            assert!(
                approved_mcp_revision(&config, &BTreeSet::new()).is_ok(),
                "合法 literal loopback URL 应通过: {url}"
            );
        }

        for url in [
            "http://localhost:4313/mcp",
            "https://127.0.0.1:4313/mcp",
            "http://user@127.0.0.1:4313/mcp",
            "http://127.0.0.2:4313/mcp",
            "http://127.0.0.1/mcp",
            "http://127.0.0.1:0/mcp",
            "http://127.0.0.1:65536/mcp",
            "http://127.0.0.1:4313",
            "http://127.0.0.1:4313/mcp?token=secret",
            "http://127.0.0.1:4313/mcp#fragment",
        ] {
            let config = http_config([("demo", url)]);
            assert!(
                approved_mcp_revision(&config, &BTreeSet::new()).is_err(),
                "非法 literal loopback URL 应拒绝: {url}"
            );
        }
    }

    /// server 与 qualified tool 名称必须是单段受控标识，归一化冲突也必须失败关闭。
    #[test]
    fn approved_mcp_revision_rejects_invalid_names_and_normalized_conflicts() {
        for name in [
            "",
            "1demo",
            "demo.name",
            "demo name",
            "demo/tool",
            "demo__server",
        ] {
            let config = http_config([(name, "http://127.0.0.1:4313/mcp")]);
            assert!(
                approved_mcp_revision(&config, &BTreeSet::new()).is_err(),
                "非法 server 名称应拒绝: {name:?}"
            );
        }

        let long_name = "a".repeat(65);
        let config = http_config([(long_name.as_str(), "http://127.0.0.1:4313/mcp")]);
        assert!(approved_mcp_revision(&config, &BTreeSet::new()).is_err());

        let config = http_config([
            ("Demo", "http://127.0.0.1:4313/mcp"),
            ("demo", "http://127.0.0.1:4314/mcp"),
        ]);
        assert!(approved_mcp_revision(&config, &BTreeSet::new()).is_err());

        for tool in [
            "demo",
            "demo__",
            "demo__search__extra",
            "1demo__search",
            "demo__1search",
            "demo__search.name",
            "demo__search name",
        ] {
            let expected_tools = BTreeSet::from([tool.to_string()]);
            let config = http_config([("demo", "http://127.0.0.1:4313/mcp")]);
            assert!(
                approved_mcp_revision(&config, &expected_tools).is_err(),
                "非法 qualified tool 名称应拒绝: {tool}"
            );
        }
    }

    /// 任一 stdio server 都必须在摘要校验前返回稳定禁用错误。
    #[test]
    fn approved_mcp_revision_rejects_stdio_without_inspecting_command() {
        let mut config = ApprovedMcpConfig::default();
        config.servers.insert(
            "bad name".to_string(),
            McpServerSpec::Stdio {
                command: "/path/that-must-not-be-inspected".into(),
                args: vec!["secret-argument".to_string()],
            },
        );

        let error = approved_mcp_revision(&config, &BTreeSet::new())
            .expect_err("stdio server 必须稳定拒绝");
        assert!(error.to_string().contains("stdio_mcp_unavailable"));
    }
}
