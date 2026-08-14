//! sidecar 权威 TOML 的纯渲染函数。
//!
//! 文件系统原子写与 sidecar 启动仍各自留在运行时 crate；这里仅保留 Host 和
//! sidecar 均可复用的、无 grok-shell 依赖的确定性文本渲染。

use std::fs;
use std::path::Path;

use anyhow::{Context, Result, bail};

use crate::{ApprovedMcpConfig, McpServerSpec, SidecarModelSpec};

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

/// 只接受 Host L3b 的精确 IPv4 回环根地址，避免把用户上游 URL 落入 sidecar 配置。
fn validate_l3b_base_url(base_url: &str) -> Result<()> {
    let Some(authority_and_path) = base_url.strip_prefix("http://127.0.0.1:") else {
        bail!("BYOK base_url 必须是 http://127.0.0.1:PORT/v1");
    };
    let Some((port, path)) = authority_and_path.split_once('/') else {
        bail!("BYOK base_url 必须是 http://127.0.0.1:PORT/v1");
    };
    let port = port
        .parse::<u16>()
        .context("BYOK base_url 的 PORT 必须是有效端口")?;
    if port == 0 || path != "v1" {
        bail!("BYOK base_url 必须是 http://127.0.0.1:PORT/v1");
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
    use std::fs;

    use tempfile::tempdir;

    use super::{render_authoritative_config, validate_authoritative_config};
    use crate::SidecarModelSpec;

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
