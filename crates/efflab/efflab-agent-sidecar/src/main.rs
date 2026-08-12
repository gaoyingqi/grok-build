//! efflab-agent-sidecar 进程入口（P1.3/P1.4 完整组装）。
//!
//! 里程碑：macOS isolated runtime integration POC。
//!
//! 启动顺序（不可颠倒，硬性约束）：
//! 1. CLI 解析与全部校验（`SidecarConfig::from_cli`，含 MCP 审核）。
//! 2. 私有 GROK_HOME 准备 + fs2 独占锁（同 home 并发 fail-closed）。
//! 3. 物化默认 AgentDefinition + 渲染权威 config → 原子写 `config.toml`。
//! 4. env 卫生：清 OTEL / compat / subagent / storage / managed-MCP 环境变量。
//! 5. 设最终 `GROK_HOME` / `GROK_AGENT`（必须早于任何 shell API 的 OnceLock）。
//! 6. `set_current_dir(session_cwd)`（进程 cwd 隔离）。
//! 7. tracing 初始化（固定 stderr，stdout 仅承载 ACP JSON-RPC）。
//! 8. 创建 Tokio runtime，执行异步主流程。
//!
//! 退出码契约：正常 EOF=0、启动策略拒绝=2、runtime 错误=1。

use std::path::Path;
use std::process::ExitCode;

use anyhow::Context;
use efflab_agent_sidecar::hardening;
use efflab_agent_sidecar::sidecar_config::SidecarConfig;
use efflab_agent_sidecar::toolset::register_efflab_tool_pack;

/// 默认 AgentDefinition 物化文件名（与 hardening::materialize_agent_definition 一致）。
const AGENT_DEF_FILENAME: &str = "efflab-default.md";

fn main() -> ExitCode {
    // === 阶段 1：CLI 解析与全部校验（任何 env mutation / runtime 之前） ===
    let sidecar = match SidecarConfig::from_cli() {
        Ok(cfg) => cfg,
        Err(err) => {
            // 启动策略拒绝：配置/参数不合法 → 退出码 2
            eprintln!("efflab-agent-sidecar: 启动策略拒绝: {err:#}");
            return ExitCode::from(2);
        }
    };
    // 当前里程碑仅支持 ACP stdio 传输。
    if !sidecar.stdio {
        eprintln!("efflab-agent-sidecar: 启动策略拒绝: 当前里程碑仅支持 --stdio");
        return ExitCode::from(2);
    }

    // === 阶段 2：私有 home 准备 + 独占锁（锁文件持有到进程退出） ===
    let _home_lock = match hardening::acquire_home_lock(&sidecar.grok_home) {
        Ok(lock) => lock,
        Err(err) => {
            eprintln!("efflab-agent-sidecar: 启动策略拒绝: 私有 home 锁: {err:#}");
            return ExitCode::from(2);
        }
    };

    // === 阶段 3：物化 AgentDefinition + 权威 config 原子落盘 ===
    let agent_def_path = match hardening::materialize_agent_definition(&sidecar.grok_home) {
        Ok(path) => path,
        Err(err) => {
            eprintln!("efflab-agent-sidecar: 启动策略拒绝: 物化 AgentDefinition: {err:#}");
            return ExitCode::from(2);
        }
    };
    let config_toml = match hardening::render_authoritative_config(
        &sidecar.grok_home,
        &agent_def_path,
        Some(&sidecar.mcp_config),
    ) {
        Ok(text) => text,
        Err(err) => {
            eprintln!("efflab-agent-sidecar: 启动策略拒绝: 渲染权威 config: {err:#}");
            return ExitCode::from(2);
        }
    };
    let config_path = sidecar.grok_home.join("config.toml");
    if let Err(err) = hardening::atomic_write_private(&config_path, config_toml.as_bytes()) {
        eprintln!("efflab-agent-sidecar: 启动策略拒绝: 写 config.toml: {err:#}");
        return ExitCode::from(2);
    }

    // === 阶段 4：env 卫生（清 OTEL / compat / subagent / storage / managed-MCP） ===
    if let Err(err) = hardening::sanitize_env() {
        eprintln!("efflab-agent-sidecar: 启动策略拒绝: env 卫生: {err:#}");
        return ExitCode::from(2);
    }
    // 设最终 GROK_HOME / GROK_AGENT：必须在任何 shell API / OnceLock 前。
    if let Err(err) = hardening::set_grok_home(&sidecar.grok_home) {
        eprintln!("efflab-agent-sidecar: 启动策略拒绝: GROK_HOME: {err:#}");
        return ExitCode::from(2);
    }
    if let Err(err) = hardening::set_grok_agent(&agent_def_path) {
        eprintln!("efflab-agent-sidecar: 启动策略拒绝: GROK_AGENT: {err:#}");
        return ExitCode::from(2);
    }

    // === 阶段 5：进程 cwd 隔离 ===
    if let Err(err) = std::env::set_current_dir(&sidecar.session_cwd) {
        eprintln!(
            "efflab-agent-sidecar: 启动策略拒绝: set_current_dir({}): {err:#}",
            sidecar.session_cwd.display()
        );
        return ExitCode::from(2);
    }

    // === 阶段 6：tracing（固定 stderr；stdout 仅承载 ACP JSON-RPC） ===
    if let Err(err) = init_tracing() {
        eprintln!("efflab-agent-sidecar: 启动策略拒绝: init_tracing: {err:#}");
        return ExitCode::from(2);
    }

    // === 阶段 7：Tokio runtime + 异步主流程 ===
    // current-thread 初选（devplan §5-3 待实现时验证；P3 集成测试核实）。
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(err) => {
            tracing::error!(%err, "failed to build tokio runtime");
            return ExitCode::from(2);
        }
    };
    match runtime.block_on(run(sidecar)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!(%err, "sidecar runtime error");
            ExitCode::from(1)
        }
    }
}

/// 初始化 tracing：日志固定输出到 stderr。
fn init_tracing() -> anyhow::Result<()> {
    let subscriber = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}

/// 异步主流程：注册占位工具 → 构造 AgentConfig → resolve runtime 字段 →
/// 硬化断言 → 运行 stdio agent（无 TUI）。
async fn run(sidecar: SidecarConfig) -> anyhow::Result<()> {
    // 注册占位工具：必须在任何 ToolRegistryBuilder::new() / Agent build 之前。
    register_efflab_tool_pack();

    // 加载有效 config（disk-only：不拉取 remote settings / campaign，不触网）。
    let raw_config =
        xai_grok_shell::config::load_effective_config_disk_only().context("加载有效配置失败")?;

    // 构造 AgentConfig（xai-grok-shell 运行时核心入口的类型）。
    let mut agent_config = xai_grok_shell::agent::config::Config::new_from_toml_cfg(&raw_config)
        .map_err(|e| anyhow::anyhow!("创建 AgentConfig 失败: {e}"))?;

    // B1：三处指向同一物化文件之一 —— agent_profile_path（优先于 [agent] / GROK_AGENT）。
    let agent_def_path = sidecar.grok_home.join("agents").join(AGENT_DEF_FILENAME);
    agent_config.agent_profile_path = Some(agent_def_path.clone());

    // B3 / R11'：resolve runtime 字段（storage=local、memory off、subagents off、
    // headless、web search off；remote_settings=None 确保不触网）。
    agent_config.resolve_runtime_fields(&xai_grok_shell::agent::config::RuntimeResolutionContext {
        raw_config: &raw_config,
        remote_settings: None,
        is_headless: true,
        cli_subagents: Some(false),
        cli_web_search_model: None,
        cli_session_summary_model: None,
        cli_experimental_memory: false,
        cli_no_memory: true,
        disable_web_search: true,
        todo_gate: false,
        laziness_debug_log: None,
        storage_mode: Some("local"),
    });

    // 硬断言：resolve 后全部安全字段必须符合隔离策略。
    assert_hardened(&agent_config, &agent_def_path)?;

    // 运行 stdio agent：stdout 仅 ACP JSON-RPC，内部 stdin EOF 触发关闭。
    xai_grok_shell::agent::app::run_stdio_agent(
        &agent_config,
        None,
        agent_config.memory_config.clone(),
    )
    .await
}

/// 硬化断言（P1.4）：resolve 后逐项核对安全字段，任何一项失败即启动失败。
fn assert_hardened(
    config: &xai_grok_shell::agent::config::Config,
    agent_def_path: &Path,
) -> anyhow::Result<()> {
    use xai_grok_shell::config::StorageMode;

    macro_rules! check {
        ($cond:expr, $msg:expr) => {
            if !$cond {
                anyhow::bail!("硬化断言失败: {}", $msg);
            }
        };
    }

    // 1) remote_fetch 必须为 false（从磁盘 config layers 重读，无 env 覆盖）。
    check!(
        !xai_grok_shell::util::config::resolve_remote_fetch_enabled(),
        "resolve_remote_fetch_enabled() 必须为 false（私有 GROK_HOME config.toml 需含 [features] remote_fetch=false）"
    );
    // 2) storage_mode 必须为 Local（B3：仅经 RuntimeResolutionContext 注入）。
    check!(
        config.storage_mode == StorageMode::Local,
        "storage_mode 必须为 Local"
    );
    // 3) subagents 必须关闭。
    check!(!config.subagents_enabled, "subagents_enabled 必须为 false");
    // 4) managed MCP 必须关闭（enabled 与 gateway_tools 均 false）。
    check!(
        !config.managed_mcps.enabled && !config.managed_mcps.gateway_tools_enabled,
        "managed_mcps.enabled / gateway_tools_enabled 必须为 false"
    );
    // 5) memory 必须关闭（resolve 后 memory_config 为 None）。
    check!(config.memory_config.is_none(), "memory_config 必须为 None");
    // 6) web search 必须关闭。
    check!(config.disable_web_search, "disable_web_search 必须为 true");
    // 7) agent_profile_path 必须指向物化文件（B1 三处指向之一）。
    check!(
        config.agent_profile_path.as_deref() == Some(agent_def_path),
        "agent_profile_path 必须指向物化 AgentDefinition"
    );

    Ok(())
}
