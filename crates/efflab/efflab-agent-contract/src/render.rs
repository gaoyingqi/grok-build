//! sidecar 权威 TOML 的纯渲染函数。
//!
//! 文件系统原子写与 sidecar 启动仍各自留在运行时 crate；这里仅保留 Host 和
//! sidecar 均可复用的、无 grok-shell 依赖的确定性文本渲染。

use std::path::Path;

use anyhow::{Context, Result, bail};

use crate::{ApprovedMcpConfig, McpServerSpec};

/// 物化 AgentDefinition 与权威配置中使用的固定 agent 名称。
const DEFAULT_AGENT_NAME: &str = "efflab-default";
/// `VendorCompat` 的全部供应商字段，必须与上游 compat 类型保持同步。
const COMPAT_VENDORS: [&str; 3] = ["claude", "cursor", "codex"];
/// `VendorCompat` 的全部 surface 字段，默认均为开启，故必须逐项显式关闭。
const COMPAT_SURFACES: [&str; 6] = ["skills", "rules", "agents", "mcps", "hooks", "sessions"];

/// 完整渲染 sidecar 唯一权威的 `config.toml` 文本。
///
/// 函数不读取旧配置，调用方可原子覆盖旧文件而不继承任意字段。`mcp` 只能传入
/// 已审核 DTO；stdio 条目写入 `command` 和 `args`，HTTP 条目写入 `url`。
/// 模型参数将在 Task 2 扩展，故本任务保持既有签名不变。
pub fn render_authoritative_config(
    grok_home: &Path,
    agent_def_path: &Path,
    mcp: Option<&ApprovedMcpConfig>,
) -> Result<String> {
    require_absolute_path(grok_home, "私有 GROK_HOME")?;
    require_absolute_path(agent_def_path, "物化 AgentDefinition")?;
    let agent_definition = path_to_utf8(agent_def_path, "物化 AgentDefinition")?;

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

    // 在写盘前先验证生成文本自身可被 TOML 解析，避免落盘无效权威配置。
    toml::from_str::<toml::Value>(&rendered)
        .context("内部错误：生成的权威 sidecar config.toml 不是合法 TOML")?;

    Ok(rendered)
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
