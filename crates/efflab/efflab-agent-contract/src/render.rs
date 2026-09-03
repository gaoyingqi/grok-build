//! sidecar 权威 TOML 的渲染与只读校验。
//!
//! 文件系统原子写与 sidecar 启动仍各自留在运行时 crate；这里保留 Host 和 sidecar
//! 均可复用的、无 grok-shell 依赖的确定性文本渲染，以及读取 Host 已写入文件的
//! fail-closed 校验。校验函数绝不修复、合并或覆盖配置。

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::Serialize;

use crate::mcp_config::{is_qualified_tool_name, is_server_name};
use crate::stdio_mcp::deny_stdio_mcp;
use crate::{
    ApprovedMcpConfig, LoopbackModelSpec, McpServerSpec, RuntimeConfigV1, SidecarModelSpec,
};

const RUNTIME_SCHEMA_VERSION: u32 = 1;
const RUNTIME_SESSION_STORE_VERSION: u32 = 1;
const RUNTIME_BACKEND: &str = "chat_completions";
const RUNTIME_TOKEN_ENV: &str = "EFFLAB_L3B_BIND";
const MAX_RUNTIME_PATH_BYTES: usize = 4096;
const MAX_RUNTIME_MODEL_ID_CHARS: usize = 128;
/// 产品系统提示词的字节上限，避免把无界文本写入 sidecar 启动配置。
const MAX_SYSTEM_PROMPT_BYTES: usize = 32_768;

/// 物化 AgentDefinition 与权威配置中使用的固定 agent 名称。
const DEFAULT_AGENT_NAME: &str = "efflab-default";
/// `VendorCompat` 的全部供应商字段，必须与上游 compat 类型保持同步。
const COMPAT_VENDORS: [&str; 3] = ["claude", "cursor", "codex"];
/// `VendorCompat` 的全部 surface 字段，默认均为开启，故必须逐项显式关闭。
const COMPAT_SURFACES: [&str; 6] = ["skills", "rules", "agents", "mcps", "hooks", "sessions"];
/// Host→sidecar 合同固定使用的 BYOK 配置表键。
const BYOK_MODEL_KEY: &str = "byok";
/// Host→sidecar 合同固定使用的模型展示名。
const BYOK_MODEL_NAME: &str = "BYOK";
/// 仅允许 Host 回环代理使用 Chat Completions 协议。
const CHAT_COMPLETIONS_BACKEND: &str = "chat_completions";
/// 仅允许 sidecar 从该环境变量读取 Host 注入的短生命周期绑定令牌。
const L3B_BIND_ENV_KEY: &str = "EFFLAB_L3B_BIND";
/// 长期保留会话，避免 sidecar 启动时清理产品管理的会话目录。
const SESSION_CLEANUP_TTL_DAYS: u32 = 36500;

/// 渲染 S1 最小 runtime 配置，并以不含自身的规范化 JSON 重新计算 revision。
pub fn render_runtime_config_v1(config: &RuntimeConfigV1) -> Result<String> {
    // 与 Task 3 使用同一 helper，避免 renderer 产生任何可执行 stdio 配置。
    deny_stdio_mcp(&config.approved_mcp)?;
    validate_runtime_config_v1(config)?;

    let mut materialized = config.clone();
    materialized.runtime_revision = calculate_runtime_revision(&materialized)?;
    let mut rendered = String::new();
    rendered.push_str("schema_version = ");
    rendered.push_str(&materialized.schema_version.to_string());
    rendered.push_str("\nruntime_revision = ");
    rendered.push_str(&runtime_toml_string(&materialized.runtime_revision));
    rendered.push_str("\nsession_store_version = ");
    rendered.push_str(&materialized.session_store_version.to_string());
    rendered.push_str("\nsession_cwd = ");
    rendered.push_str(&runtime_toml_string(&materialized.session_cwd));
    rendered.push_str("\nexpected_tools = ");
    rendered.push_str(&runtime_toml_string_array(&materialized.expected_tools));
    rendered.push_str("\nsystem_prompt = ");
    rendered.push_str(&runtime_toml_string(&materialized.system_prompt));
    rendered.push_str("\n\n[model]\nmodel_id = ");
    rendered.push_str(&runtime_toml_string(&materialized.model.model_id));
    rendered.push_str("\nbase_url = ");
    rendered.push_str(&runtime_toml_string(&materialized.model.base_url));
    rendered.push_str("\nbackend = ");
    rendered.push_str(&runtime_toml_string(&materialized.model.backend));
    rendered.push_str("\ntoken_env = ");
    rendered.push_str(&runtime_toml_string(&materialized.model.token_env));

    if materialized.approved_mcp.servers.is_empty() {
        rendered.push_str("\n\n[approved_mcp]\nservers = {}\n");
    } else {
        for (name, server) in &materialized.approved_mcp.servers {
            let McpServerSpec::Http { url } = server else {
                bail!("stdio_mcp_unavailable");
            };
            rendered.push_str("\n\n[approved_mcp.servers.");
            rendered.push_str(&runtime_toml_key_literal(name));
            rendered.push_str("]\nurl = ");
            rendered.push_str(&runtime_toml_string(url));
            rendered.push('\n');
        }
    }

    toml::from_str::<RuntimeConfigV1>(&rendered)
        .context("内部错误：生成的 RuntimeConfigV1 TOML 不是合法 schema")?;
    Ok(rendered)
}

/// 从 Host 写出的 v1 TOML 读取配置，并在任何后续使用前完成闭集与 revision 校验。
pub fn load_runtime_config_v1(path: &Path) -> Result<RuntimeConfigV1> {
    let source = fs::read_to_string(path).context("读取 RuntimeConfigV1 TOML 失败")?;
    load_runtime_config_v1_from_str(&source)
}

/// 校验 Host/sidecar 共用的绝对 UTF-8 session cwd 词法合同。
///
/// 该函数只做输入 shape 校验，不访问文件系统；实际目录存在性和 no-follow 约束由
/// sidecar 的 Unix hardening 层继续完成。`&str` 已保证 UTF-8，长度按字节计算。
pub fn validate_session_cwd(value: &str) -> Result<()> {
    if value.is_empty() {
        bail!("session_cwd 不能为空");
    }
    if value.len() > MAX_RUNTIME_PATH_BYTES {
        bail!("session_cwd 长度不能超过 {MAX_RUNTIME_PATH_BYTES} 字节");
    }
    if value.as_bytes().contains(&0) {
        bail!("session_cwd 不允许包含 NUL");
    }

    let path = Path::new(value);
    if !path.is_absolute() {
        bail!("session_cwd 必须是绝对 UTF-8 路径");
    }
    if path
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        bail!("session_cwd 不允许包含 ..");
    }
    Ok(())
}

/// 校验已经由调用方安全读取的 v1 TOML 文本。
///
/// sidecar 通过受保护的文件句柄读取配置后调用本函数，避免校验一次路径文件、
/// 再用另一次路径打开得到不同内容。所有 schema、stdio 和 revision 规则在此统一收口。
pub fn load_runtime_config_v1_from_str(source: &str) -> Result<RuntimeConfigV1> {
    // 丢弃 TOML parser 的原始错误；未知字段和 MCP server 名可能来自不可信 runtime wire。
    let config: RuntimeConfigV1 =
        toml::from_str(source).map_err(|_| anyhow::anyhow!("runtime_config_invalid"))?;

    // stdio 必须在 revision 等其他策略校验之前统一走 Task 3 helper，保持稳定错误码。
    deny_stdio_mcp(&config.approved_mcp)?;
    validate_runtime_config_v1(&config)?;
    let expected_revision = calculate_runtime_revision(&config)?;
    if config.runtime_revision != expected_revision {
        bail!("runtime_revision 校验失败：配置摘要与不含自身的规范化 JSON 不一致");
    }
    Ok(config)
}

/// 只接受字面量 IPv4/IPv6 loopback HTTP，并要求 URL 带显式端口和非空路径。
pub fn is_literal_loopback_http_url(url: &str) -> bool {
    literal_loopback_http_path(url).is_some()
}

/// 提取已通过字面量 loopback HTTP 校验的 URL path。
fn literal_loopback_http_path(url: &str) -> Option<&str> {
    if url.is_empty() || url.bytes().any(|byte| byte.is_ascii_whitespace()) {
        return None;
    }

    let remainder = url
        .strip_prefix("http://127.0.0.1:")
        .or_else(|| url.strip_prefix("http://[::1]:"))?;
    let path_start = remainder.find('/')?;
    let port = &remainder[..path_start];
    let path = &remainder[path_start..];
    let Ok(port_number) = port.parse::<u16>() else {
        return None;
    };
    if port.is_empty()
        || !port.bytes().all(|byte| byte.is_ascii_digit())
        || port_number == 0
        || path.contains('?')
        || path.contains('#')
    {
        return None;
    }

    Some(path)
}

/// 校验 RuntimeConfigV1 的固定版本、路径、模型和 MCP 传输约束。
fn validate_runtime_config_v1(config: &RuntimeConfigV1) -> Result<()> {
    if config.schema_version != RUNTIME_SCHEMA_VERSION {
        bail!("schema_version 必须为 {RUNTIME_SCHEMA_VERSION}");
    }
    if config.session_store_version != RUNTIME_SESSION_STORE_VERSION {
        bail!("session_store_version 必须为 {RUNTIME_SESSION_STORE_VERSION}");
    }
    validate_session_cwd(&config.session_cwd)?;

    // expected_tools 与 Host 审核摘要复用同一 qualified-name 语法。
    // tool 长度与完整名称的记录上限仍由 sidecar 运行时处理。
    for tool in &config.expected_tools {
        if !is_qualified_tool_name(tool) {
            bail!("expected_tools 包含非法 MCP qualified tool 名称");
        }
    }
    validate_system_prompt(&config.system_prompt)?;

    let model_id = &config.model.model_id;
    if model_id.is_empty() {
        bail!("model.model_id 不能为空");
    }
    if model_id.chars().count() > MAX_RUNTIME_MODEL_ID_CHARS
        || !model_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
    {
        bail!("model.model_id 必须匹配 ^[A-Za-z0-9._:-]+$ 且不超过 128 个字符");
    }
    if config.model.backend != RUNTIME_BACKEND {
        bail!("model.backend 必须为 {RUNTIME_BACKEND}");
    }
    if config.model.token_env != RUNTIME_TOKEN_ENV {
        bail!("model.token_env 必须为 {RUNTIME_TOKEN_ENV}");
    }
    if literal_loopback_http_path(&config.model.base_url) != Some("/v1") {
        bail!("model.base_url 必须是字面量 loopback HTTP 且 path 精确为 /v1");
    }

    for (name, server) in &config.approved_mcp.servers {
        // RuntimeConfigV1 的 map key 与 qualified name 共用同一 server 边界。
        if !is_server_name(name) {
            bail!("approved_mcp.servers.name_invalid");
        }
        match server {
            McpServerSpec::Http { url } => {
                if !is_literal_loopback_http_url(url) {
                    bail!("approved_mcp.servers.url_invalid");
                }
            }
            McpServerSpec::Stdio { .. } => {
                // load_runtime_config_v1 已在此函数前调用 deny_stdio_mcp。
                bail!("stdio_mcp_unavailable");
            }
        }
    }
    Ok(())
}

/// 校验 Host 注入的系统提示词：允许空值回退，但拒绝 NUL 和无界文本。
fn validate_system_prompt(value: &str) -> Result<()> {
    if value.len() > MAX_SYSTEM_PROMPT_BYTES {
        bail!("system_prompt 长度不能超过 {MAX_SYSTEM_PROMPT_BYTES} 字节");
    }
    if value.as_bytes().contains(&0) {
        bail!("system_prompt 不允许包含 NUL");
    }
    Ok(())
}

/// 生成 revision 使用的字段顺序固定、且不包含 runtime_revision 的 JSON 投影。
#[derive(Serialize)]
struct RuntimeRevisionPayload<'a> {
    schema_version: u32,
    session_store_version: u32,
    session_cwd: &'a str,
    model: &'a LoopbackModelSpec,
    approved_mcp: RuntimeRevisionMcp<'a>,
    expected_tools: &'a BTreeSet<String>,
    system_prompt: &'a str,
}

/// revision 专用 MCP 视图只包含 HTTP URL，避免 command/args 进入摘要。
#[derive(Serialize)]
struct RuntimeRevisionMcp<'a> {
    servers: BTreeMap<&'a str, RuntimeRevisionServer<'a>>,
}

#[derive(Serialize)]
struct RuntimeRevisionServer<'a> {
    url: &'a str,
}

/// 以纯 Rust SHA-256 计算 runtime revision，避免为 contract crate 扩大依赖闭包。
fn calculate_runtime_revision(config: &RuntimeConfigV1) -> Result<String> {
    let servers = config
        .approved_mcp
        .servers
        .iter()
        .map(|(name, server)| {
            let McpServerSpec::Http { url } = server else {
                return Err(anyhow::anyhow!("stdio_mcp_unavailable"));
            };
            Ok((name.as_str(), RuntimeRevisionServer { url: url.as_str() }))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let payload = RuntimeRevisionPayload {
        schema_version: config.schema_version,
        session_store_version: config.session_store_version,
        session_cwd: &config.session_cwd,
        model: &config.model,
        approved_mcp: RuntimeRevisionMcp { servers },
        expected_tools: &config.expected_tools,
        system_prompt: &config.system_prompt,
    };
    let canonical_json =
        serde_json::to_vec(&payload).context("规范化 RuntimeConfigV1 JSON 失败")?;
    let digest = Sha256::digest(&canonical_json);
    let hex = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Ok(format!("sha256:{hex}"))
}

/// 最小 SHA-256 实现；只用于短小配置摘要，不处理秘密或外部网络数据。
struct Sha256;

impl Sha256 {
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
        let mut padded = vec![0u8; padded_len];
        padded[..input.len()].copy_from_slice(input);
        padded[input.len()] = 0x80;
        padded[padded_len - 8..].copy_from_slice(&bit_len.to_be_bytes());

        for chunk in padded.chunks_exact(64) {
            let mut words = [0u32; 64];
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

        let mut digest = [0u8; 32];
        for (index, word) in state.iter().enumerate() {
            digest[index * 4..index * 4 + 4].copy_from_slice(&word.to_be_bytes());
        }
        digest
    }
}

/// 渲染 runtime schema 的 TOML 字符串，不应用旧权威配置的环境变量转义规则。
fn runtime_toml_string(value: &str) -> String {
    toml::Value::String(value.to_owned()).to_string()
}

/// 按 BTreeSet 顺序渲染 runtime schema 的字符串数组。
fn runtime_toml_string_array(values: &BTreeSet<String>) -> String {
    let values = values.iter().cloned().map(toml::Value::String).collect();
    toml::Value::Array(values).to_string()
}

/// 优先保留安全的裸 TOML key，否则退回引号 key 以支持任意 UTF-8 名称。
fn runtime_toml_key_literal(value: &str) -> String {
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        value.to_owned()
    } else {
        runtime_toml_string(value)
    }
}

/// 完整渲染 sidecar 唯一权威的 `config.toml` 文本。
///
/// 函数不读取旧配置，调用方可原子覆盖旧文件而不继承任意字段。`mcp` 只能传入
/// 已审核 DTO；stdio 条目写入 `command` 和 `args`，HTTP 条目写入 `url`。
/// `models` 由 Host 提供；空 slice 只生成安全骨架，非空时只能生成唯一的 `byok`
/// Chat Completions 模型，用户凭据绝不进入此文本。
pub fn render_authoritative_config(
    grok_home: &Path,
    agent_def_path: &Path,
    mcp: Option<&ApprovedMcpConfig>,
    models: &[SidecarModelSpec],
) -> Result<String> {
    require_absolute_path(grok_home, "私有 GROK_HOME")?;
    require_absolute_path(agent_def_path, "物化 AgentDefinition")?;
    let agent_definition = path_to_utf8(agent_def_path, "物化 AgentDefinition")?;
    if models.len() > 1 {
        bail!("权威 sidecar 配置一次只允许一个 BYOK 模型");
    }

    // 所有默认开启的 compat cell 均在同一 `[compat]` 表中逐项关闭。
    let mut rendered = String::from("[features]\nremote_fetch = false\n\n[compat]\n");
    for vendor in COMPAT_VENDORS {
        for surface in COMPAT_SURFACES {
            rendered.push_str(vendor);
            rendered.push('.');
            rendered.push_str(surface);
            rendered.push_str(" = false\n");
        }
    }

    rendered.push_str("\n[subagents]\nenabled = false\n");
    rendered.push_str("\n[managed_mcps]\nenabled = false\ngateway_tools_enabled = false\n");
    rendered.push_str("\n[memory]\nenabled = false\n");
    rendered.push_str("\n[skills]\npaths = []\n");
    rendered.push_str("\n[agent]\nname = ");
    rendered.push_str(&toml_string_literal(DEFAULT_AGENT_NAME));
    rendered.push_str("\ndefinition = ");
    rendered.push_str(&toml_string_literal(&agent_definition));
    rendered.push_str("\n\n[mcp_servers]\n");

    if let Some(approved_mcp) = mcp {
        for (name, server) in &approved_mcp.servers {
            if name.trim().is_empty() {
                bail!("受控 MCP server 名称不能为空");
            }

            rendered.push('\n');
            rendered.push_str("[mcp_servers.");
            rendered.push_str(&toml_key_literal(name));
            rendered.push_str("]\n");

            match server {
                McpServerSpec::Stdio { command, args } => {
                    if !command.is_absolute() {
                        bail!(
                            "受控 stdio MCP server '{name}' 的 command 必须为绝对路径: {}",
                            command.display()
                        );
                    }

                    let command = path_to_utf8(command, "受控 stdio MCP command")?;
                    rendered.push_str("command = ");
                    rendered.push_str(&toml_string_literal(&command));
                    rendered.push_str("\nargs = ");
                    rendered.push_str(&toml_string_array_literal(args));
                    rendered.push('\n');
                }
                McpServerSpec::Http { url } => {
                    if url.trim().is_empty() {
                        bail!("受控 HTTP MCP server '{name}' 的 url 不能为空");
                    }

                    rendered.push_str("url = ");
                    rendered.push_str(&toml_string_literal(url));
                    rendered.push('\n');
                }
            }
        }
    }

    if let Some(model) = models.first() {
        render_byok_model(&mut rendered, model)?;
    }
    // 空模型集合仍需产出固定安全骨架；sidecar 会将其视为未配置而拒绝启动。
    rendered.push_str("\n[storage]\ncleanup_ttl_days = ");
    rendered.push_str(&SESSION_CLEANUP_TTL_DAYS.to_string());
    rendered.push_str("\n\n[session]\nload_envrc = false\n");
    // 预置上游一次性迁移标记，阻止运行时写回 Host 唯一拥有的 config.toml。
    rendered.push_str("\n[marketplace]\ndefault_skills_installs_purged = true\n");

    // 在写盘前先验证生成文本自身可被 TOML 解析，避免落盘无效权威配置。
    toml::from_str::<toml::Value>(&rendered)
        .context("内部错误：生成的权威 sidecar config.toml 不是合法 TOML")?;

    Ok(rendered)
}

/// 写入唯一允许的 BYOK 模型段；该段只描述 sidecar 到 Host 回环的连接方式。
fn render_byok_model(rendered: &mut String, model: &SidecarModelSpec) -> Result<()> {
    validate_sidecar_model_spec(model)?;

    rendered.push_str("\n[models]\ndefault = ");
    rendered.push_str(&toml_string_literal(BYOK_MODEL_KEY));
    rendered.push_str("\n\n[model.byok]\nmodel = ");
    rendered.push_str(&toml_string_literal(&model.model));
    rendered.push_str("\nbase_url = ");
    rendered.push_str(&toml_string_literal(&model.base_url));
    rendered.push_str("\nname = ");
    rendered.push_str(&toml_string_literal(&model.name));
    rendered.push_str("\napi_backend = ");
    rendered.push_str(&toml_string_literal(&model.api_backend));
    rendered.push_str("\nenv_key = ");
    rendered.push_str(&toml_string_literal(&model.env_key));
    rendered.push('\n');
    Ok(())
}

/// 在 Host 写盘前校验唯一 BYOK 模型的固定协议、安全名称和回环地址。
fn validate_sidecar_model_spec(model: &SidecarModelSpec) -> Result<()> {
    if model.model.trim().is_empty() {
        bail!("BYOK 模型标识不能为空");
    }
    if model.api_backend != CHAT_COMPLETIONS_BACKEND {
        bail!("BYOK api_backend 必须为 {CHAT_COMPLETIONS_BACKEND}");
    }
    if model.name != BYOK_MODEL_NAME {
        bail!("BYOK name 必须为 {BYOK_MODEL_NAME}");
    }
    if model.env_key != L3B_BIND_ENV_KEY {
        bail!("BYOK env_key 必须为 {L3B_BIND_ENV_KEY}");
    }
    validate_l3b_base_url(&model.base_url)
}

/// 只接受 Host L3b 的精确 IPv4 或 IPv6 回环根地址，避免把用户上游 URL 落入 sidecar 配置。
fn validate_l3b_base_url(base_url: &str) -> Result<()> {
    let authority_and_path = base_url
        .strip_prefix("http://127.0.0.1:")
        .or_else(|| base_url.strip_prefix("http://[::1]:"))
        .ok_or_else(|| {
            anyhow::anyhow!("BYOK base_url 必须是 http://127.0.0.1:PORT/v1 或 http://[::1]:PORT/v1")
        })?;
    let Some((port, path)) = authority_and_path.split_once('/') else {
        bail!("BYOK base_url 必须是 http://127.0.0.1:PORT/v1 或 http://[::1]:PORT/v1");
    };
    let port = port
        .parse::<u16>()
        .context("BYOK base_url 的 PORT 必须是有效端口")?;
    if port == 0 || path != "v1" {
        bail!("BYOK base_url 必须是 http://127.0.0.1:PORT/v1 或 http://[::1]:PORT/v1");
    }
    Ok(())
}

/// 校验 Host 已写入的权威 `config.toml`；本函数只读文件，绝不修复或覆盖其内容。
pub fn validate_authoritative_config(config_path: &Path, agent_def_path: &Path) -> Result<()> {
    require_absolute_path(config_path, "权威 config.toml")?;
    require_absolute_path(agent_def_path, "物化 AgentDefinition")?;

    let metadata = fs::symlink_metadata(config_path)
        .with_context(|| format!("读取权威 config.toml 元数据失败: {}", config_path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("权威 config.toml 必须是常规文件: {}", config_path.display());
    }
    let source = fs::read_to_string(config_path)
        .with_context(|| format!("读取权威 config.toml 失败: {}", config_path.display()))?;
    let document: toml::Value = toml::from_str(&source)
        .with_context(|| format!("解析权威 config.toml 失败: {}", config_path.display()))?;
    validate_authoritative_config_value(&document, agent_def_path)
}

/// 按冻结的 Host→sidecar 合同逐表校验解析后的配置，避免 sidecar 自行再实现渲染。
fn validate_authoritative_config_value(
    document: &toml::Value,
    agent_def_path: &Path,
) -> Result<()> {
    let root = document
        .as_table()
        .context("权威 config.toml 根节点必须是 table")?;
    validate_exact_keys(
        root,
        &[
            "features",
            "compat",
            "subagents",
            "managed_mcps",
            "memory",
            "skills",
            "agent",
            "mcp_servers",
            "models",
            "model",
            "storage",
            "session",
            "marketplace",
        ],
        "权威 config.toml",
    )?;

    let features = required_table(root, "features", "权威 config.toml")?;
    validate_exact_keys(features, &["remote_fetch"], "[features]")?;
    require_false(features, "remote_fetch", "[features]")?;

    let compat = required_table(root, "compat", "权威 config.toml")?;
    validate_exact_keys(compat, &COMPAT_VENDORS, "[compat]")?;
    for vendor in COMPAT_VENDORS {
        let vendor_table = required_table(compat, vendor, "[compat]")?;
        validate_exact_keys(
            vendor_table,
            &COMPAT_SURFACES,
            &format!("[compat.{vendor}]"),
        )?;
        for surface in COMPAT_SURFACES {
            require_false(vendor_table, surface, &format!("[compat.{vendor}]"))?;
        }
    }

    let subagents = required_table(root, "subagents", "权威 config.toml")?;
    validate_exact_keys(subagents, &["enabled"], "[subagents]")?;
    require_false(subagents, "enabled", "[subagents]")?;

    let managed_mcps = required_table(root, "managed_mcps", "权威 config.toml")?;
    validate_exact_keys(
        managed_mcps,
        &["enabled", "gateway_tools_enabled"],
        "[managed_mcps]",
    )?;
    require_false(managed_mcps, "enabled", "[managed_mcps]")?;
    require_false(managed_mcps, "gateway_tools_enabled", "[managed_mcps]")?;

    let memory = required_table(root, "memory", "权威 config.toml")?;
    validate_exact_keys(memory, &["enabled"], "[memory]")?;
    require_false(memory, "enabled", "[memory]")?;

    let skills = required_table(root, "skills", "权威 config.toml")?;
    validate_exact_keys(skills, &["paths"], "[skills]")?;
    let skill_paths = required_array(skills, "paths", "[skills]")?;
    if !skill_paths.is_empty() {
        bail!("[skills].paths 必须为空数组");
    }

    let agent = required_table(root, "agent", "权威 config.toml")?;
    validate_exact_keys(agent, &["name", "definition"], "[agent]")?;
    if required_string(agent, "name", "[agent]")? != DEFAULT_AGENT_NAME {
        bail!("[agent].name 必须为 {DEFAULT_AGENT_NAME}");
    }
    let expected_agent_definition =
        path_to_utf8(agent_def_path, "物化 AgentDefinition")?.replace('$', "$$");
    if required_string(agent, "definition", "[agent]")? != expected_agent_definition {
        bail!("[agent].definition 必须指向 sidecar 物化的 AgentDefinition");
    }

    let mcp_servers = required_table(root, "mcp_servers", "权威 config.toml")?;
    validate_authoritative_mcp_servers(mcp_servers)?;

    let models = required_table(root, "models", "权威 config.toml")?;
    validate_exact_keys(models, &["default"], "[models]")?;
    if required_string(models, "default", "[models]")? != BYOK_MODEL_KEY {
        bail!("[models].default 必须为 {BYOK_MODEL_KEY}");
    }

    let model_tables = required_table(root, "model", "权威 config.toml")?;
    validate_exact_keys(model_tables, &[BYOK_MODEL_KEY], "[model]")?;
    let byok = required_table(model_tables, BYOK_MODEL_KEY, "[model]")?;
    validate_exact_keys(
        byok,
        &["model", "base_url", "name", "api_backend", "env_key"],
        "[model.byok]",
    )?;
    if required_string(byok, "model", "[model.byok]")?
        .trim()
        .is_empty()
    {
        bail!("[model.byok].model 不能为空");
    }
    validate_l3b_base_url(required_string(byok, "base_url", "[model.byok]")?)?;
    if required_string(byok, "name", "[model.byok]")? != BYOK_MODEL_NAME {
        bail!("[model.byok].name 必须为 {BYOK_MODEL_NAME}");
    }
    if required_string(byok, "api_backend", "[model.byok]")? != CHAT_COMPLETIONS_BACKEND {
        bail!("[model.byok].api_backend 必须为 {CHAT_COMPLETIONS_BACKEND}");
    }
    if required_string(byok, "env_key", "[model.byok]")? != L3B_BIND_ENV_KEY {
        bail!("[model.byok].env_key 必须为 {L3B_BIND_ENV_KEY}");
    }

    let storage = required_table(root, "storage", "权威 config.toml")?;
    validate_exact_keys(storage, &["cleanup_ttl_days"], "[storage]")?;
    let cleanup_ttl_days = required_integer(storage, "cleanup_ttl_days", "[storage]")?;
    if cleanup_ttl_days == 0 {
        bail!("[storage].cleanup_ttl_days 禁止为 0");
    }
    if cleanup_ttl_days != i64::from(SESSION_CLEANUP_TTL_DAYS) {
        bail!("[storage].cleanup_ttl_days 必须为 {SESSION_CLEANUP_TTL_DAYS}");
    }

    let session = required_table(root, "session", "权威 config.toml")?;
    validate_exact_keys(session, &["load_envrc"], "[session]")?;
    require_false(session, "load_envrc", "[session]")?;

    // 上游初始化会在缺少该标记时写回 config.toml；Host 必须预置以保持唯一写盘 owner。
    let marketplace = required_table(root, "marketplace", "权威 config.toml")?;
    validate_exact_keys(
        marketplace,
        &["default_skills_installs_purged"],
        "[marketplace]",
    )?;
    if !required_bool(
        marketplace,
        "default_skills_installs_purged",
        "[marketplace]",
    )? {
        bail!("[marketplace].default_skills_installs_purged 必须为 true");
    }
    Ok(())
}

/// 校验权威 MCP 表只能保留 renderer 支持的本地 stdio 或回环 HTTP 形状。
fn validate_authoritative_mcp_servers(servers: &toml::map::Map<String, toml::Value>) -> Result<()> {
    for (name, value) in servers {
        if name.trim().is_empty() {
            bail!("[mcp_servers] 不允许空 server 名称");
        }
        let server = value
            .as_table()
            .with_context(|| format!("[mcp_servers.{name}] 必须是 table"))?;
        let command = server.get("command");
        let url = server.get("url");
        match (command, url) {
            (Some(command), None) => {
                validate_exact_keys(
                    server,
                    &["command", "args"],
                    &format!("[mcp_servers.{name}]"),
                )?;
                let command = command
                    .as_str()
                    .with_context(|| format!("[mcp_servers.{name}].command 必须是字符串"))?;
                if !Path::new(command).is_absolute() {
                    bail!("[mcp_servers.{name}].command 必须为绝对路径");
                }
                for argument in required_array(server, "args", &format!("[mcp_servers.{name}]"))? {
                    if !argument.is_str() {
                        bail!("[mcp_servers.{name}].args 必须只含字符串");
                    }
                }
            }
            (None, Some(url)) => {
                validate_exact_keys(server, &["url"], &format!("[mcp_servers.{name}]"))?;
                let url = url
                    .as_str()
                    .with_context(|| format!("[mcp_servers.{name}].url 必须是字符串"))?;
                if !crate::mcp_config::is_loopback_http_url(url) {
                    bail!("[mcp_servers.{name}].url 必须指向回环 HTTP 地址");
                }
            }
            (Some(_), Some(_)) => {
                bail!("[mcp_servers.{name}] 不能同时包含 command 与 url");
            }
            (None, None) => {
                bail!("[mcp_servers.{name}] 必须包含 command 或 url");
            }
        }
    }
    Ok(())
}

/// 要求一个表精确包含合同规定的键，拒绝未知字段和缺失字段以避免策略注入。
fn validate_exact_keys(
    table: &toml::map::Map<String, toml::Value>,
    expected: &[&str],
    context: &str,
) -> Result<()> {
    for key in table.keys() {
        if !expected.contains(&key.as_str()) {
            bail!("{context} 包含未知键 {key:?}");
        }
    }
    for key in expected {
        if !table.contains_key(*key) {
            bail!("{context} 缺少必需键 {key:?}");
        }
    }
    Ok(())
}

/// 读取必需的 TOML 子表，保留准确的合同错误上下文。
fn required_table<'a>(
    table: &'a toml::map::Map<String, toml::Value>,
    key: &str,
    context: &str,
) -> Result<&'a toml::map::Map<String, toml::Value>> {
    table
        .get(key)
        .and_then(toml::Value::as_table)
        .with_context(|| format!("{context}.{key} 必须是 table"))
}

/// 读取必需字符串字段。
fn required_string<'a>(
    table: &'a toml::map::Map<String, toml::Value>,
    key: &str,
    context: &str,
) -> Result<&'a str> {
    table
        .get(key)
        .and_then(toml::Value::as_str)
        .with_context(|| format!("{context}.{key} 必须是字符串"))
}

/// 读取必需布尔字段。
fn required_bool(
    table: &toml::map::Map<String, toml::Value>,
    key: &str,
    context: &str,
) -> Result<bool> {
    table
        .get(key)
        .and_then(toml::Value::as_bool)
        .with_context(|| format!("{context}.{key} 必须是布尔值"))
}

/// 读取必需整数字段。
fn required_integer(
    table: &toml::map::Map<String, toml::Value>,
    key: &str,
    context: &str,
) -> Result<i64> {
    table
        .get(key)
        .and_then(toml::Value::as_integer)
        .with_context(|| format!("{context}.{key} 必须是整数"))
}

/// 读取必需数组字段。
fn required_array<'a>(
    table: &'a toml::map::Map<String, toml::Value>,
    key: &str,
    context: &str,
) -> Result<&'a Vec<toml::Value>> {
    table
        .get(key)
        .and_then(toml::Value::as_array)
        .with_context(|| format!("{context}.{key} 必须是数组"))
}

/// 要求指定布尔开关显式关闭。
fn require_false(
    table: &toml::map::Map<String, toml::Value>,
    key: &str,
    context: &str,
) -> Result<()> {
    if required_bool(table, key, context)? {
        bail!("{context}.{key} 必须为 false");
    }
    Ok(())
}

/// 要求由启动边界传入的敏感路径均为绝对路径。
fn require_absolute_path(path: &Path, description: &str) -> Result<()> {
    if !path.is_absolute() {
        bail!("{description} 必须是绝对路径: {}", path.display());
    }
    if path
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        bail!("{description} 不允许包含 ..: {}", path.display());
    }
    Ok(())
}

/// 将路径转换为可安全写入 TOML 的 Unicode 字符串；非 Unicode 路径 fail-closed。
fn path_to_utf8(path: &Path, description: &str) -> Result<String> {
    path.to_str().map(str::to_owned).with_context(|| {
        format!(
            "{description} 不是可写入 TOML 的 UTF-8 路径: {}",
            path.display()
        )
    })
}

/// 渲染 TOML 字符串值，同时屏蔽配置层对 `$VAR` 的二次环境展开。
fn toml_string_literal(value: &str) -> String {
    toml::Value::String(value.replace('$', "$$")).to_string()
}

/// 渲染 TOML table key；table key 不经过配置值的环境展开，因此保留原始名称。
fn toml_key_literal(value: &str) -> String {
    toml::Value::String(value.to_owned()).to_string()
}

/// 渲染 MCP stdio 参数数组，并对每个参数屏蔽配置层环境展开。
fn toml_string_array_literal(values: &[String]) -> String {
    let values = values
        .iter()
        .map(|value| toml::Value::String(value.replace('$', "$$")))
        .collect();
    toml::Value::Array(values).to_string()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;

    use tempfile::tempdir;

    use super::{render_authoritative_config, validate_authoritative_config};
    use crate::{ApprovedMcpConfig, McpServerSpec, SidecarModelSpec};

    /// 构造 Host 写入配置时使用的唯一 BYOK 模型，不携带用户上游凭据。
    fn byok_model() -> SidecarModelSpec {
        SidecarModelSpec {
            model: "test-chat-model".to_string(),
            base_url: "http://127.0.0.1:43123/v1".to_string(),
            name: "BYOK".to_string(),
            api_backend: "chat_completions".to_string(),
            env_key: "EFFLAB_L3B_BIND".to_string(),
        }
    }

    /// 建立 renderer 所需的绝对路径；AgentDefinition 由 sidecar 启动时物化。
    fn authoritative_paths() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
        let temporary = tempdir().expect("创建临时目录应成功");
        let grok_home = temporary.path().join("private-grok-home");
        let agent_definition = grok_home.join("agents").join("efflab-default.md");
        (temporary, grok_home, agent_definition)
    }

    /// 非空模型必须固定为 Host 回环 Chat Completions，并带会话与存储安全配置。
    #[test]
    fn render_authoritative_config_pins_byok_chat_completions_and_session_ttl() {
        let (_temporary, grok_home, agent_definition) = authoritative_paths();
        let rendered =
            render_authoritative_config(&grok_home, &agent_definition, None, &[byok_model()])
                .expect("合法 Host 模型应可渲染权威配置");

        assert!(
            rendered.contains("[models]\ndefault = \"byok\""),
            "必须固定默认模型为 byok: {rendered}"
        );
        assert!(
            rendered.contains("[model.byok]\nmodel = \"test-chat-model\""),
            "必须使用 Host 传入的 model_id: {rendered}"
        );
        assert!(
            rendered.contains("base_url = \"http://127.0.0.1:43123/v1\""),
            "必须只写 Host 回环 L3b URL: {rendered}"
        );
        assert!(
            rendered.contains("name = \"BYOK\"")
                && rendered.contains("api_backend = \"chat_completions\"")
                && rendered.contains("env_key = \"EFFLAB_L3B_BIND\""),
            "BYOK 模型必须使用固定后端和绑定令牌环境变量: {rendered}"
        );
        assert!(
            rendered.contains("[storage]\ncleanup_ttl_days = 36500")
                && rendered.contains("[session]\nload_envrc = false"),
            "必须固定长期 TTL 并禁止加载 .envrc: {rendered}"
        );
        assert!(
            rendered.contains("[marketplace]\ndefault_skills_installs_purged = true"),
            "必须预置上游迁移标记，防止运行时改写 Host 配置: {rendered}"
        );
        for forbidden in ["api_key =", "XAI_API_KEY", "grok-4.5", "responses", "sk-"] {
            assert!(
                !rendered.contains(forbidden),
                "权威配置不得包含 {forbidden:?}: {rendered}"
            );
        }
    }

    /// IPv6 回环监听同样只能把精确 `::1` 地址写入 sidecar 模型合同。
    #[test]
    fn render_authoritative_config_accepts_exact_ipv6_l3b_base_url() {
        let (_temporary, grok_home, agent_definition) = authoritative_paths();
        let mut model = byok_model();
        model.base_url = "http://[::1]:43123/v1".to_string();
        let rendered = render_authoritative_config(&grok_home, &agent_definition, None, &[model])
            .expect("精确 IPv6 L3b 回环 URL 必须可渲染");

        assert!(rendered.contains("base_url = \"http://[::1]:43123/v1\""));
    }

    /// renderer 的每种受支持 MCP 形状都必须被只读 validator 接受，防止字段列表漂移。
    #[test]
    fn rendered_authoritative_configs_round_trip_through_validator() {
        // 使用当前测试二进制的绝对路径，仅验证 stdio 字段合同而不实际启动它。
        let test_binary = std::env::current_exe().expect("读取当前测试二进制路径应成功");
        let cases = [
            ("无 MCP", None),
            (
                "stdio MCP",
                Some(ApprovedMcpConfig {
                    servers: BTreeMap::from([(
                        "local".to_string(),
                        McpServerSpec::Stdio {
                            command: test_binary,
                            args: vec!["--sidecar".to_string()],
                        },
                    )]),
                }),
            ),
            (
                "回环 HTTP MCP",
                Some(ApprovedMcpConfig {
                    servers: BTreeMap::from([(
                        "local".to_string(),
                        McpServerSpec::Http {
                            url: "http://127.0.0.1:43124/mcp".to_string(),
                        },
                    )]),
                }),
            ),
        ];

        for (case_name, mcp) in cases {
            let (_temporary, grok_home, agent_definition) = authoritative_paths();
            fs::create_dir_all(&grok_home).expect("创建 Host 私有 home 应成功");
            let config_path = grok_home.join("config.toml");
            let rendered = render_authoritative_config(
                &grok_home,
                &agent_definition,
                mcp.as_ref(),
                &[byok_model()],
            )
            .unwrap_or_else(|error| panic!("{case_name} 的权威配置应可渲染: {error:#}"));
            fs::write(&config_path, rendered).expect("Host 写入权威配置应成功");

            validate_authoritative_config(&config_path, &agent_definition).unwrap_or_else(
                |error| panic!("{case_name} 的 renderer 输出必须通过 validator: {error:#}"),
            );
        }
    }

    /// 非 Chat Completions 后端或上游 URL 都不能进入 Host→sidecar 配置合同。
    #[test]
    fn render_authoritative_config_rejects_non_chat_completions_and_upstream_url() {
        let (_temporary, grok_home, agent_definition) = authoritative_paths();
        let mut responses_model = byok_model();
        responses_model.api_backend = "responses".to_string();
        let backend_error =
            render_authoritative_config(&grok_home, &agent_definition, None, &[responses_model])
                .expect_err("responses 后端必须被拒绝");
        assert!(
            backend_error.to_string().contains("chat_completions"),
            "错误必须说明唯一允许的后端: {backend_error:#}"
        );

        let mut upstream_model = byok_model();
        upstream_model.base_url = "https://upstream.example/v1".to_string();
        let url_error =
            render_authoritative_config(&grok_home, &agent_definition, None, &[upstream_model])
                .expect_err("用户上游 URL 不得写入权威配置");
        assert!(
            url_error.to_string().contains("127.0.0.1"),
            "错误必须说明仅允许 Host 回环 URL: {url_error:#}"
        );
    }

    /// 未配置模型时只保留安全骨架，绝不能回退写入内置模型。
    #[test]
    fn render_authoritative_config_empty_models_has_no_builtin_model() {
        let (_temporary, grok_home, agent_definition) = authoritative_paths();
        let rendered = render_authoritative_config(&grok_home, &agent_definition, None, &[])
            .expect("空模型集合仍应产出安全骨架");

        assert!(rendered.contains("[storage]\ncleanup_ttl_days = 36500"));
        assert!(rendered.contains("[session]\nload_envrc = false"));
        assert!(!rendered.contains("grok-4.5"));
        assert!(!rendered.contains("[models]"));
        assert!(!rendered.contains("[model.byok]"));
    }

    /// sidecar 启动前必须拒绝缺失或篡改模型、TTL 与 .envrc 安全开关的磁盘配置。
    #[test]
    fn validate_authoritative_config_rejects_invalid_models_storage_and_session() {
        let (_temporary, grok_home, agent_definition) = authoritative_paths();
        fs::create_dir_all(&grok_home).expect("创建 Host 私有 home 应成功");
        let config_path = grok_home.join("config.toml");
        let rendered =
            render_authoritative_config(&grok_home, &agent_definition, None, &[byok_model()])
                .expect("合法 Host 配置应可渲染");
        fs::write(&config_path, &rendered).expect("Host 写入权威配置应成功");
        validate_authoritative_config(&config_path, &agent_definition)
            .expect("Host 写入的合法配置应通过 sidecar 校验");

        let cases = [
            (
                "非法 models.default",
                rendered.replacen("default = \"byok\"", "default = \"other\"", 1),
                "models",
            ),
            (
                "TTL 为零",
                rendered.replacen("cleanup_ttl_days = 36500", "cleanup_ttl_days = 0", 1),
                "storage",
            ),
            (
                "允许加载 envrc",
                rendered.replacen("load_envrc = false", "load_envrc = true", 1),
                "session",
            ),
            (
                "未预置上游迁移标记",
                rendered.replacen(
                    "default_skills_installs_purged = true",
                    "default_skills_installs_purged = false",
                    1,
                ),
                "marketplace",
            ),
        ];
        for (case_name, invalid, expected_context) in cases {
            fs::write(&config_path, invalid).expect("写入篡改配置应成功");
            let error = validate_authoritative_config(&config_path, &agent_definition)
                .expect_err("篡改配置必须被拒绝");
            assert!(
                error.to_string().contains(expected_context),
                "{case_name} 的错误必须包含 {expected_context:?}: {error:#}"
            );
        }
    }
}
