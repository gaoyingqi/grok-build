//! 内置占位工具：`GrokBuild:efflab_noop`（P2）。
//!
//! 目的（方案 v3 B2 / R5'）：
//! - 提供唯一、可编译、无副作用的内置工具，证明 runtime 可用；
//! - 通过 AgentDefinition `injectDefaultTools: false` 阻断 memory/web/lsp/image/
//!   plan-mode 等默认工具注入（见 xai-grok-agent/src/builder.rs 的注入分支）；
//! - 真实能力全部来自 Host 批准的 MCP server（`--mcp-config`），sidecar 不开放
//!   任何任意内置工具配置。
//!
//! 注册机制（xai-grok-tools/src/registry/types.rs）：
//! `register_tool_pack(fn(&mut ToolRegistryBuilder))` 是进程级注册入口，**必须**
//! 在首次 `ToolRegistryBuilder::new()` 之前调用（顺序契约），由 main.rs 在进程
//! 启动早期执行。注册后的全名 = `{namespace}:{short_id}` = `GrokBuild:efflab_noop`。

use std::sync::Once;

use xai_grok_tools::registry::types::{ToolRegistryBuilder, register_tool_pack};
use xai_grok_tools::types::output::{TextOutput, ToolOutput as GrokToolOutput};
use xai_grok_tools::types::tool::{ToolKind, ToolNamespace};
use xai_grok_tools::types::tool_io::ToolInput;
use xai_grok_tools::types::tool_metadata::ToolMetadata;
use xai_tool_protocol::ToolId;
use xai_tool_runtime::context::{ListToolsContext, ToolCallContext};
use xai_tool_runtime::error::ToolError;
use xai_tool_runtime::render::ToolOutput;
use xai_tool_runtime::tool::Tool;
use xai_tool_types::ToolDescription;

/// 固定工具全名（方案 v3 B2：`ToolNamespace` 为封闭枚举、无 Efflab 变体，
/// 必须使用 `GrokBuild` 命名空间）。
pub const EFFLAB_TOOL_ID: &str = "GrokBuild:efflab_noop";
/// 短 id：`Tool::id()` 的返回值，注册时由 registry 拼接为全名。
pub const EFFLAB_TOOL_SHORT_ID: &str = "efflab_noop";

/// noop 工具的参数：空结构，不接受任何输入。
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct EfflabNoopArgs {}

/// noop 工具的固定输出：标记 runtime 可用，无副作用。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EfflabNoopOutput {
    pub ok: bool,
    pub message: String,
}

// 使用默认 `model_output()`（返回空 Vec → 运行时自动从序列化 JSON 提取文本块）。
impl ToolOutput for EfflabNoopOutput {}

// 满足 `register` 约束：`T::Args: Into<ToolInput>`（空参数用 Dynamic 变体承载）。
impl From<EfflabNoopArgs> for ToolInput {
    fn from(_args: EfflabNoopArgs) -> Self {
        ToolInput::Dynamic(serde_json::json!({}))
    }
}

// 满足 `register` 约束：`T::Output: Into<ToolOutput>`（用 Text 变体承载输出文本）。
impl From<EfflabNoopOutput> for GrokToolOutput {
    fn from(out: EfflabNoopOutput) -> Self {
        GrokToolOutput::Text(TextOutput::from(format!("efflab_noop: {}", out.message)))
    }
}

/// 占位工具：无副作用，固定返回成功。
#[derive(Debug, Default)]
pub struct EfflabNoopTool;

impl Tool for EfflabNoopTool {
    type Args = EfflabNoopArgs;
    type Output = EfflabNoopOutput;

    fn id(&self) -> ToolId {
        ToolId::new(EFFLAB_TOOL_SHORT_ID).expect("efflab_noop 是合法的工具短 id")
    }

    fn description(&self, _ctx: &ListToolsContext) -> ToolDescription {
        ToolDescription::new(
            EFFLAB_TOOL_SHORT_ID,
            "Efflab runtime availability marker (no side effects).",
        )
        .with_namespace("GrokBuild")
        .with_title("Efflab Noop")
        .with_kind(ToolKind::Other.as_key())
        .with_arguments_schema(serde_json::json!({ "type": "object", "properties": {} }))
    }

    async fn run(
        &self,
        _ctx: ToolCallContext,
        _args: Self::Args,
    ) -> Result<Self::Output, ToolError> {
        // 固定成功返回，证明 runtime 工具执行链路可用。
        Ok(EfflabNoopOutput {
            ok: true,
            message: "runtime available".to_string(),
        })
    }
}

impl ToolMetadata for EfflabNoopTool {
    fn kind(&self) -> ToolKind {
        ToolKind::Other
    }

    fn tool_namespace(&self) -> ToolNamespace {
        ToolNamespace::GrokBuild
    }

    fn description_template(&self) -> &str {
        "Efflab runtime availability marker (no side effects)."
    }
}

/// 注册 noop 工具包（进程级，幂等）。
///
/// 必须在首次 `ToolRegistryBuilder::new()` 之前调用；main.rs 在 env/config
/// 准备完成后、Agent build 前调用。
pub fn register_efflab_tool_pack() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        register_tool_pack(|builder: &mut ToolRegistryBuilder| {
            builder.register::<EfflabNoopTool>();
        });
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_id_is_stable_and_registers_exactly_once() {
        // 注册必须在首次 ToolRegistryBuilder::new() 之前完成。
        register_efflab_tool_pack();
        // 重复调用是幂等的（Once）。
        register_efflab_tool_pack();

        let builder = ToolRegistryBuilder::new();
        let ids = builder.known_tool_ids();
        assert!(
            ids.contains(EFFLAB_TOOL_ID),
            "注册后 known_tool_ids() 应包含 {EFFLAB_TOOL_ID}，实际含 {ids:?}"
        );
    }

    #[test]
    fn default_agent_definition_parses_with_exact_single_tool() {
        // 物化 AgentDefinition：从嵌入的 asset 解析，验证工具集精确一个。
        let def = xai_grok_agent::AgentDefinition::parse(include_str!(
            "../assets/efflab-default-agent.md"
        ))
        .expect("默认 agent definition 必须能解析");

        assert!(!def.inject_default_tools, "必须阻断默认工具注入");
        assert!(!def.agents_md, "不应加载 AGENTS.md");
        assert!(!def.discover_skills, "不应发现 CWD 技能");
        assert!(!def.inherit_skills, "不应继承父会话技能");

        let ids: Vec<&str> = def
            .tool_config
            .tools
            .iter()
            .map(|t| t.id.as_str())
            .collect();
        assert_eq!(ids, vec![EFFLAB_TOOL_ID], "工具白名单必须精确为 noop 一个");
    }
}
