//! ACP stdio 集成测试（P3.2/3.3/3.4）。
//!
//! 覆盖：
//! - initialize / session/new 成功路径（BYOK fake key，不触网）
//! - `_x.ai/mcp/list` 在无 MCP、批准 MCP、失败 MCP 三种场景下的隔离行为
//! - stdin EOF 与 TERM→KILL 生命周期（正常 EOF 退出码 0）
//! - stdout 从启动到 EOF 的每个非空行均为合法 JSON-RPC
//! - Host 侧字段白名单在请求进入 wire 前拦截恶意字段
//! - 工具集组合证明：物化 AgentDefinition 工具列表精确为
//!   `GrokBuild:efflab_noop`，并验证冷启动运行时可用
//!
//! 约定：每个测试使用独立临时目录（私有 GROK_HOME + session_cwd），
//! 避免同 home 锁冲突；全部等待带超时；进程由 `SidecarProcess` 兜底回收。

mod common;

use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use common::acp_client::AcpClient;
use common::process::SidecarProcess;
use efflab_agent_sidecar::host_contract::{HostPolicy, HostRejection, validate_host_request};
use serde_json::Value;

/// 所有 ACP 请求的默认超时。
const REQ_TIMEOUT: Duration = Duration::from_secs(20);
/// EOF 关闭路径的当前集成测试上限；产品目标见 devplan R12' 的约 3.5 秒。
const EOF_EXIT_TIMEOUT: Duration = Duration::from_secs(10);
/// MCP 异步初始化或失败状态传播的轮询上限。
const MCP_POLL_TIMEOUT: Duration = Duration::from_secs(10);

/// 一个已启动的测试环境：sidecar 进程 + 临时目录。
struct TestEnv {
    proc: SidecarProcess,
    #[allow(dead_code)]
    dir: tempfile::TempDir,
    session_cwd: PathBuf,
}

/// 启动一个基础 sidecar（无 MCP 注入）。
fn spawn_base() -> TestEnv {
    let dir = tempfile::TempDir::new().expect("创建测试临时目录");
    let session_cwd = dir.path().join("cwd");
    fs::create_dir_all(&session_cwd).expect("创建 session cwd");
    let grok_home = dir.path().join("home");

    let proc = SidecarProcess::spawn(&grok_home, &session_cwd, &[], &[]);
    TestEnv {
        proc,
        dir,
        session_cwd,
    }
}

/// 启动带一个受控 stdio MCP fixture 的 sidecar。
fn spawn_with_fixture_mcp(server_name: &str, fixture_name: &str) -> TestEnv {
    let dir = tempfile::TempDir::new().expect("创建测试临时目录");
    let session_cwd = dir.path().join("cwd");
    fs::create_dir_all(&session_cwd).expect("创建 session cwd");
    let grok_home = dir.path().join("home");

    // fixture 必须落在受控 exec-root 中，才能通过 sidecar 的命令路径白名单。
    let exec_root = dir.path().join("exec-root");
    fs::create_dir_all(&exec_root).expect("创建 exec-root");
    let fixture_src = common::fixture_path(fixture_name);
    let fixture_dst = exec_root.join(fixture_name);
    fs::copy(&fixture_src, &fixture_dst).expect("复制 MCP fixture");
    let mut permissions = fs::metadata(&fixture_dst)
        .expect("读取 MCP fixture 元数据")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&fixture_dst, permissions).expect("设置 MCP fixture 可执行");

    let command = toml::Value::String(fixture_dst.display().to_string());
    let mcp_toml = format!("[mcp_servers.{server_name}]\ncommand = {command}\n");
    let mcp_config_path = dir.path().join("mcp.toml");
    fs::write(&mcp_config_path, mcp_toml).expect("写 MCP 配置");
    let extra_args = vec![
        "--mcp-config".to_string(),
        mcp_config_path.display().to_string(),
        "--mcp-exec-root".to_string(),
        exec_root.display().to_string(),
    ];

    TestEnv {
        proc: SidecarProcess::spawn(&grok_home, &session_cwd, &extra_args, &[]),
        dir,
        session_cwd,
    }
}

/// 启动带一个 loopback HTTP MCP 的 sidecar，用于验证 HTTP transport 失败隔离。
fn spawn_with_http_mcp(server_name: &str, url: &str) -> TestEnv {
    let dir = tempfile::TempDir::new().expect("创建测试临时目录");
    let session_cwd = dir.path().join("cwd");
    fs::create_dir_all(&session_cwd).expect("创建 session cwd");
    let grok_home = dir.path().join("home");

    let url = toml::Value::String(url.to_string());
    let mcp_toml = format!("[mcp_servers.{server_name}]\nurl = {url}\n");
    let mcp_config_path = dir.path().join("mcp.toml");
    fs::write(&mcp_config_path, mcp_toml).expect("写 HTTP MCP 配置");
    let extra_args = vec![
        "--mcp-config".to_string(),
        mcp_config_path.display().to_string(),
    ];

    TestEnv {
        proc: SidecarProcess::spawn(&grok_home, &session_cwd, &extra_args, &[]),
        dir,
        session_cwd,
    }
}

/// 连接 ACP 客户端并完成 initialize。
fn connect_initialize(env: &mut TestEnv) -> AcpClient {
    let stdin = env.proc.take_stdin();
    let stdout = env.proc.stdout_reader().into_inner();
    let mut client = AcpClient::new(stdin, stdout);
    let resp = client
        .request(
            "initialize",
            serde_json::json!({
                "protocolVersion": 1,
                "client": { "name": "efflab-test", "mcpServers": [] },
                "capabilities": { "terminal": false, "fs": false }
            }),
            REQ_TIMEOUT,
        )
        .expect("initialize 必须成功");
    assert!(
        resp.get("result").is_some(),
        "initialize 应有 result: {resp}"
    );
    client
}

/// 发送最小合法 session/new，并精确断言返回 sessionId。
fn create_session(client: &mut AcpClient, session_cwd: &Path) -> Value {
    let resp = client
        .request(
            "session/new",
            serde_json::json!({ "cwd": session_cwd, "mcpServers": [] }),
            REQ_TIMEOUT,
        )
        .expect("session/new 必须成功");
    let result = resp.get("result").expect("session/new 应有 result");
    assert!(
        result.get("sessionId").and_then(Value::as_str).is_some(),
        "session/new 必须返回字符串 sessionId: {resp}"
    );
    resp
}

/// 从 session/new 响应中提取必需的 sessionId。
fn session_id(resp: &Value) -> String {
    resp.get("result")
        .and_then(|result| result.get("sessionId"))
        .and_then(Value::as_str)
        .expect("session/new 响应必须包含字符串 sessionId")
        .to_string()
}

/// 请求 `_x.ai/mcp/list`，可选择关联一个已创建会话以取得运行时状态。
fn request_mcp_list(client: &mut AcpClient, session_id: Option<&str>) -> Value {
    let params = match session_id {
        Some(session_id) => serde_json::json!({ "sessionId": session_id }),
        None => serde_json::json!({}),
    };
    client
        .request("_x.ai/mcp/list", params, REQ_TIMEOUT)
        .expect("_x.ai/mcp/list 必须成功")
}

/// 从 `_x.ai/mcp/list` 响应中提取 servers 数组（响应为嵌套 result：`result.result.servers`）。
fn mcp_list_servers(resp: &Value) -> Option<&[Value]> {
    resp.get("result")
        .and_then(|result| result.get("result"))
        .and_then(|result| result.get("servers"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
}

/// 等待指定 MCP server 出现在 catalog 中，防止异步初始化造成瞬时假阴性。
fn wait_for_mcp_server(
    client: &mut AcpClient,
    session_id: Option<&str>,
    server_name: &str,
) -> Vec<Value> {
    let deadline = Instant::now() + MCP_POLL_TIMEOUT;
    loop {
        let response = request_mcp_list(client, session_id);
        let servers = mcp_list_servers(&response)
            .expect("mcp/list 响应应含 result.result.servers")
            .to_vec();
        if servers
            .iter()
            .any(|server| server.get("name").and_then(Value::as_str) == Some(server_name))
        {
            return servers;
        }
        assert!(
            Instant::now() < deadline,
            "等待 MCP server {server_name:?} 出现超时，最后 catalog: {servers:?}"
        );
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// 断言 catalog 的 server 名精确等于 Host 批准的集合，既不遗漏也不泄漏。
fn assert_exact_mcp_server_names(servers: &[Value], expected_names: &[&str], context: &str) {
    let actual_names: Vec<String> = servers
        .iter()
        .map(|server| {
            server
                .get("name")
                .and_then(Value::as_str)
                .expect("每个 MCP catalog 条目必须有字符串 name")
                .to_string()
        })
        .collect();
    let actual_set: BTreeSet<String> = actual_names.iter().cloned().collect();
    let expected_set: BTreeSet<String> = expected_names
        .iter()
        .map(|name| (*name).to_string())
        .collect();

    assert_eq!(
        actual_names.len(),
        expected_names.len(),
        "{context}：MCP 条目数量必须精确匹配，实际: {actual_names:?}"
    );
    assert_eq!(
        actual_set, expected_set,
        "{context}：MCP server 集合必须精确匹配，实际: {actual_names:?}"
    );
}

/// 关闭 stdin 并断言 sidecar 在当前可验证的上限内正常退出。
fn assert_clean_eof_exit(env: &mut TestEnv, client: &mut AcpClient, scenario: &str) {
    client.close_stdin();
    let status = match env.proc.wait_timeout(EOF_EXIT_TIMEOUT) {
        Some(status) => status,
        None => panic!(
            "{scenario}：关闭 stdin 后 sidecar 未在 {EOF_EXIT_TIMEOUT:?} 内退出；stderr: {}",
            env.proc.stderr_text()
        ),
    };
    assert!(
        status.success(),
        "{scenario}：正常 EOF 应退出码 0，实际 {status:?}；stderr: {}",
        env.proc.stderr_text()
    );
}

/// 等待 sidecar 退出后的 stdout reader 稳定，确保协议纯净性检查覆盖已刷出的全部行。
fn raw_lines_after_exit(client: &AcpClient) -> Vec<String> {
    let deadline = Instant::now() + Duration::from_secs(1);
    let mut previous: Option<Vec<String>> = None;
    loop {
        let current = client.raw_lines();
        if current.len() >= 2 && previous.as_ref() == Some(&current) {
            return current;
        }
        assert!(
            Instant::now() < deadline,
            "sidecar 退出后 stdout reader 未在超时内稳定，当前原始行: {current:?}"
        );
        previous = Some(current);
        std::thread::sleep(Duration::from_millis(25));
    }
}

/// 将完整恶意环境变量集合一次性交给隔离测试进程。
fn all_malicious_environment() -> Vec<(String, String)> {
    let compat_env_names = [
        "GROK_CURSOR_SKILLS_ENABLED",
        "GROK_CURSOR_RULES_ENABLED",
        "GROK_CURSOR_AGENTS_ENABLED",
        "GROK_CURSOR_MCPS_ENABLED",
        "GROK_CURSOR_HOOKS_ENABLED",
        "GROK_CURSOR_SESSIONS_ENABLED",
        "GROK_CLAUDE_SKILLS_ENABLED",
        "GROK_CLAUDE_RULES_ENABLED",
        "GROK_CLAUDE_AGENTS_ENABLED",
        "GROK_CLAUDE_MCPS_ENABLED",
        "GROK_CLAUDE_HOOKS_ENABLED",
        "GROK_CLAUDE_SESSIONS_ENABLED",
        "GROK_CODEX_SKILLS_ENABLED",
        "GROK_CODEX_RULES_ENABLED",
        "GROK_CODEX_AGENTS_ENABLED",
        "GROK_CODEX_MCPS_ENABLED",
        "GROK_CODEX_HOOKS_ENABLED",
        "GROK_CODEX_SESSIONS_ENABLED",
    ];
    let mut environment: Vec<(String, String)> = compat_env_names
        .into_iter()
        .map(|name| (name.to_string(), "true".to_string()))
        .collect();
    environment.extend([
        (
            "GROK_EXTERNAL_OTEL".to_string(),
            "http://127.0.0.1:4317".to_string(),
        ),
        ("GROK_SUBAGENTS".to_string(), "1".to_string()),
        ("GROK_STORAGE_MODE".to_string(), "writeback".to_string()),
        ("GROK_MANAGED_MCPS_ENABLED".to_string(), "true".to_string()),
        (
            "GROK_MANAGED_MCP_GATEWAY_TOOLS_ENABLED".to_string(),
            "true".to_string(),
        ),
        (
            "OTEL_EXPORTER_OTLP_ENDPOINT".to_string(),
            "http://127.0.0.1:4317".to_string(),
        ),
        ("OTEL_EFFLAB_MALICIOUS".to_string(), "true".to_string()),
        ("OTEL_SERVICE_NAME".to_string(), "efflab-evil".to_string()),
    ]);
    environment
}

/// 由可信 Host 模拟器执行“先校验、后发送”的唯一请求路径。
fn trusted_host_send(
    client: &mut AcpClient,
    policy: &HostPolicy,
    sent_methods: &mut Vec<String>,
    method: &str,
    params: Value,
) -> Result<Value, HostRejection> {
    validate_host_request(method, &params, policy)?;
    // 仅白名单校验成功后才写入 stdin，记录同时构成未发送恶意请求的可观测证据。
    sent_methods.push(method.to_string());
    Ok(client
        .request(method, params, REQ_TIMEOUT)
        .expect("经可信 Host 批准的 ACP 请求必须被 sidecar 正常处理"))
}

#[test]
fn initialize_and_session_new_succeed() {
    let mut env = spawn_base();
    let mut client = connect_initialize(&mut env);

    // session/new：cwd 精确匹配（canonical 后的绝对路径）。
    let resp = create_session(&mut client, &env.session_cwd);
    let result = resp.get("result").expect("session/new 应有 result");
    assert!(
        result.get("models").is_some(),
        "session/new 应返回 models 目录: {resp}"
    );
    assert_clean_eof_exit(&mut env, &mut client, "initialize/session-new 成功路径");
}

#[test]
fn mcp_list_empty_without_config() {
    let mut env = spawn_base();
    let mut client = connect_initialize(&mut env);
    let _ = create_session(&mut client, &env.session_cwd);

    // wire 层扩展方法需要 `_` 前缀（ACP decoder 要求）。
    let resp = request_mcp_list(&mut client, None);
    let servers = mcp_list_servers(&resp).expect("mcp/list 响应应含 result.result.servers");
    assert_exact_mcp_server_names(servers, &[], "未注入 MCP 时");
    assert_clean_eof_exit(&mut env, &mut client, "无 MCP catalog 路径");
}

#[test]
fn stdin_eof_triggers_clean_exit_zero() {
    let mut env = spawn_base();
    let mut client = connect_initialize(&mut env);

    // 当前测试实际采用 10 秒有界上限；产品目标约为 3.5 秒，不能误称本测试已验证该阈值。
    assert_clean_eof_exit(&mut env, &mut client, "stdin EOF 生命周期");
}

#[test]
fn mcp_config_injects_echo_server() {
    let mut env = spawn_with_fixture_mcp("echo", "mock_mcp_server.py");
    let mut client = connect_initialize(&mut env);
    let _ = create_session(&mut client, &env.session_cwd);

    // MCP 初始化是异步的；到达后必须没有默认、managed 或用户 MCP 泄漏。
    let servers = wait_for_mcp_server(&mut client, None, "echo");
    assert_exact_mcp_server_names(&servers, &["echo"], "批准 echo MCP 的运行时 catalog");

    assert_clean_eof_exit(&mut env, &mut client, "echo MCP 注入路径");
}

#[test]
fn toolset_static_proof_materialized_agent() {
    // 组合证明之静态部分：spawn 后读取物化 AgentDefinition，断言工具精确一个。
    let mut env = spawn_base();
    let mut client = connect_initialize(&mut env);

    let agent_def_path = env.dir.path().join("home/agents/efflab-default.md");
    let content = fs::read_to_string(&agent_def_path).expect("物化 agent 文件必须存在");
    let def = xai_grok_agent::AgentDefinition::parse(&content).expect("物化 agent 必须可解析");

    assert!(!def.inject_default_tools, "必须阻断默认工具注入");
    let ids: Vec<&str> = def
        .tool_config
        .tools
        .iter()
        .map(|tool| tool.id.as_str())
        .collect();
    assert_eq!(
        ids,
        vec!["GrokBuild:efflab_noop"],
        "物化 AgentDefinition 工具白名单必须精确为 noop 一个"
    );
    assert_clean_eof_exit(&mut env, &mut client, "工具集静态证明路径");
}

#[test]
fn malicious_env_cannot_reopen_capabilities() {
    // 保留原有门禁：若少量代表性恶意 env 破坏启动，测试必须立即报错。
    let dir = tempfile::TempDir::new().expect("创建测试临时目录");
    let session_cwd = dir.path().join("cwd");
    fs::create_dir_all(&session_cwd).expect("创建 session cwd");
    let grok_home = dir.path().join("home");
    let malicious_env = vec![
        ("GROK_CURSOR_MCPS_ENABLED".to_string(), "true".to_string()),
        ("GROK_CURSOR_HOOKS_ENABLED".to_string(), "true".to_string()),
        ("GROK_STORAGE_MODE".to_string(), "writeback".to_string()),
        ("GROK_SUBAGENTS".to_string(), "1".to_string()),
        ("GROK_MANAGED_MCPS_ENABLED".to_string(), "true".to_string()),
        (
            "GROK_EXTERNAL_OTEL".to_string(),
            "http://127.0.0.1:4317".to_string(),
        ),
        (
            "OTEL_EXPORTER_OTLP_ENDPOINT".to_string(),
            "http://127.0.0.1:4317".to_string(),
        ),
    ];
    let mut env = TestEnv {
        proc: SidecarProcess::spawn(&grok_home, &session_cwd, &[], &malicious_env),
        dir,
        session_cwd,
    };

    let mut client = connect_initialize(&mut env);
    let resp = create_session(&mut client, &env.session_cwd);
    assert!(
        resp.get("result")
            .and_then(|result| result.get("sessionId"))
            .is_some(),
        "恶意 env 下仍应返回 sessionId: {resp}"
    );
    assert_clean_eof_exit(&mut env, &mut client, "代表性恶意 env 路径");
}

#[test]
fn all_malicious_env_cannot_reopen_capabilities() {
    // P0-6：一次性覆盖三个 vendor 的 18 个 compat cell、5 个精确开关与多个 OTEL_ 前缀。
    let dir = tempfile::TempDir::new().expect("创建测试临时目录");
    let session_cwd = dir.path().join("cwd");
    fs::create_dir_all(&session_cwd).expect("创建 session cwd");
    let grok_home = dir.path().join("home");
    let malicious_env = all_malicious_environment();
    assert_eq!(
        malicious_env.len(),
        26,
        "测试必须一次性注入 18 个 compat、5 个精确变量和 3 个 OTEL_ 变量"
    );
    let mut env = TestEnv {
        proc: SidecarProcess::spawn(&grok_home, &session_cwd, &[], &malicious_env),
        dir,
        session_cwd,
    };

    let mut client = connect_initialize(&mut env);
    let session_response = create_session(&mut client, &env.session_cwd);
    assert!(
        session_response
            .get("result")
            .and_then(|result| result.get("sessionId"))
            .and_then(Value::as_str)
            .is_some(),
        "全量恶意 env 下 session/new 必须返回 sessionId: {session_response}"
    );

    // 权威 config.toml 不读取或合并恶意 env；解析后逐项确认安全字段。
    let config_path = env.dir.path().join("home/config.toml");
    let config_text = fs::read_to_string(&config_path).expect("启动后私有 config.toml 必须存在");
    let config: toml::Value =
        toml::from_str(&config_text).expect("私有 config.toml 必须是合法 TOML");
    for vendor in ["cursor", "claude", "codex"] {
        for surface in ["skills", "rules", "agents", "mcps", "hooks", "sessions"] {
            assert_eq!(
                config
                    .get("compat")
                    .and_then(toml::Value::as_table)
                    .and_then(|compat| compat.get(vendor))
                    .and_then(toml::Value::as_table)
                    .and_then(|vendor_config| vendor_config.get(surface))
                    .and_then(toml::Value::as_bool),
                Some(false),
                "compat.{vendor}.{surface} 必须在全量恶意 env 下保持 false"
            );
        }
    }
    assert_eq!(
        config
            .get("managed_mcps")
            .and_then(toml::Value::as_table)
            .and_then(|table| table.get("enabled"))
            .and_then(toml::Value::as_bool),
        Some(false),
        "managed_mcps.enabled 必须固定为 false"
    );
    assert_eq!(
        config
            .get("managed_mcps")
            .and_then(toml::Value::as_table)
            .and_then(|table| table.get("gateway_tools_enabled"))
            .and_then(toml::Value::as_bool),
        Some(false),
        "managed_mcps.gateway_tools_enabled 必须固定为 false"
    );
    assert_eq!(
        config
            .get("subagents")
            .and_then(toml::Value::as_table)
            .and_then(|table| table.get("enabled"))
            .and_then(toml::Value::as_bool),
        Some(false),
        "subagents.enabled 必须固定为 false"
    );
    assert_ne!(
        config
            .get("storage")
            .and_then(toml::Value::as_table)
            .and_then(|table| table.get("mode"))
            .and_then(toml::Value::as_str),
        Some("writeback"),
        "权威 config 不得出现 storage.mode=writeback"
    );
    assert_eq!(
        config
            .get("features")
            .and_then(toml::Value::as_table)
            .and_then(|table| table.get("remote_fetch"))
            .and_then(toml::Value::as_bool),
        Some(false),
        "features.remote_fetch 必须固定为 false"
    );

    let mcp_response = request_mcp_list(&mut client, None);
    let servers = mcp_list_servers(&mcp_response).expect("mcp/list 响应应含 result.result.servers");
    assert_exact_mcp_server_names(servers, &[], "全量恶意 env 且未注入 MCP 时");
    assert_clean_eof_exit(&mut env, &mut client, "全量恶意 env 路径");
}

#[test]
fn failing_mcp_isolated_and_runtime_remains_usable() {
    // failing fixture 输出非法 MCP 数据后退出，覆盖 stdio MCP 的启动失败隔离。
    let mut env = spawn_with_fixture_mcp("failing", "failing_mcp_server.py");
    let mut client = connect_initialize(&mut env);
    let first_session = create_session(&mut client, &env.session_cwd);
    let first_session_id = session_id(&first_session);

    // 产品可选择隐藏失败 server，或保留带 unavailable 状态/错误详情的条目；两者都可观测。
    let deadline = Instant::now() + MCP_POLL_TIMEOUT;
    let failure_observation = loop {
        let response = request_mcp_list(&mut client, Some(&first_session_id));
        let servers = mcp_list_servers(&response)
            .expect("mcp/list 响应应含 result.result.servers")
            .to_vec();
        let failing = servers
            .iter()
            .find(|server| server.get("name").and_then(Value::as_str) == Some("failing"));
        let observable_failure = failing.is_some_and(|server| {
            server
                .get("session")
                .and_then(|session| session.get("status"))
                .and_then(Value::as_str)
                == Some("unavailable")
                || server.get("error").is_some()
                || server.get("detail").is_some()
        });
        if failing.is_none() || observable_failure {
            break failing.cloned();
        }
        assert!(
            Instant::now() < deadline,
            "失败 MCP 在超时内既未隐藏也未暴露非 ready/error 状态，最后 catalog: {servers:?}"
        );
        std::thread::sleep(Duration::from_millis(250));
    };
    if let Some(server) = failure_observation {
        let status = server
            .get("session")
            .and_then(|session| session.get("status"))
            .and_then(Value::as_str);
        assert!(
            status == Some("unavailable")
                || server.get("error").is_some()
                || server.get("detail").is_some(),
            "保留失败 MCP 时必须暴露 unavailable 状态或错误详情: {server}"
        );
    }

    // 失败后再创建会话，证明 sidecar 没有崩溃且仍能处理核心 ACP 请求。
    let follow_up_session = create_session(&mut client, &env.session_cwd);
    assert!(
        follow_up_session
            .get("result")
            .and_then(|result| result.get("sessionId"))
            .and_then(Value::as_str)
            .is_some(),
        "失败 MCP 后新的 session/new 仍必须成功: {follow_up_session}"
    );
    assert_clean_eof_exit(&mut env, &mut client, "失败 stdio MCP 隔离路径");
}

#[test]
fn unreachable_loopback_http_mcp_does_not_crash_sidecar() {
    // 固定高位 loopback 端口无监听服务；本测试只验证 HTTP MCP 连接失败不会击穿 sidecar。
    let mut env = spawn_with_http_mcp("unreachable_http", "http://127.0.0.1:65534/mcp");
    let mut client = connect_initialize(&mut env);
    let session = create_session(&mut client, &env.session_cwd);
    let session_id = session_id(&session);

    let response = request_mcp_list(&mut client, Some(&session_id));
    let servers = mcp_list_servers(&response).expect("mcp/list 响应应含 result.result.servers");
    assert_exact_mcp_server_names(
        servers,
        &["unreachable_http"],
        "不可达 loopback HTTP MCP catalog",
    );

    // 连接失败后仍可完成另一个只读 ACP 请求，且 EOF 保持正常退出。
    let follow_up = request_mcp_list(&mut client, Some(&session_id));
    assert!(
        follow_up.get("result").is_some(),
        "不可达 HTTP MCP 后 sidecar 仍必须响应 mcp/list: {follow_up}"
    );
    assert_clean_eof_exit(&mut env, &mut client, "不可达 loopback HTTP MCP 路径");
}

#[test]
fn cold_start_registers_tool_pack_and_runtime_stays_usable() {
    // 新进程冷启动的 initialize 成功，证明注册发生在 Agent build 前且未阻断运行时。
    let mut env = spawn_base();
    let mut client = connect_initialize(&mut env);
    let session = create_session(&mut client, &env.session_cwd);
    assert!(
        session
            .get("result")
            .and_then(|result| result.get("sessionId"))
            .and_then(Value::as_str)
            .is_some(),
        "冷启动后 session/new 必须成功: {session}"
    );

    // ACP 没有直接的内置工具列表 API；结合静态工具白名单证明，此处确认运行时无默认 MCP 泄漏。
    let response = request_mcp_list(&mut client, None);
    let servers = mcp_list_servers(&response).expect("mcp/list 响应应含 result.result.servers");
    assert_exact_mcp_server_names(servers, &[], "冷启动 runtime MCP catalog");
    assert_clean_eof_exit(&mut env, &mut client, "冷启动工具包注册路径");
}

#[test]
fn terminate_stops_running_sidecar_with_nonzero_status() {
    // 先完成握手证明进程正在服务，再直接走测试基础设施的 TERM→KILL 回收路径。
    let mut env = spawn_base();
    let _client = connect_initialize(&mut env);
    let status = env
        .proc
        .terminate()
        .expect("terminate 必须在有界 TERM→KILL 路径后回收 sidecar");
    assert!(
        !status.success(),
        "运行中的 sidecar 收到 TERM 后通常应为非 0 退出，实际 {status:?}；stderr: {}",
        env.proc.stderr_text()
    );
}

#[test]
fn drop_cleanup_releases_home_lock_for_next_sidecar() {
    // Drop 内部复用 terminate()；先完成握手，确保首个进程已持有私有 home 锁并正在服务。
    let dir = tempfile::TempDir::new().expect("创建测试临时目录");
    let session_cwd = dir.path().join("cwd");
    fs::create_dir_all(&session_cwd).expect("创建 session cwd");
    let grok_home = dir.path().join("home");
    let mut first_env = TestEnv {
        proc: SidecarProcess::spawn(&grok_home, &session_cwd, &[], &[]),
        dir,
        session_cwd,
    };
    let first_client = connect_initialize(&mut first_env);
    assert!(
        grok_home.join(".efflab-sidecar.lock").exists(),
        "initialize 成功后首个 sidecar 必须持有私有 home 锁"
    );

    // client 仍持有 stdin，故 drop SidecarProcess 不能退化为 EOF，而会走其 TERM→KILL 兜底。
    let cleanup_started = Instant::now();
    let TestEnv {
        proc: first_proc,
        dir,
        session_cwd,
    } = first_env;
    drop(first_proc);
    assert!(
        cleanup_started.elapsed() <= Duration::from_secs(7),
        "Drop 的 TERM→KILL 兜底必须在有界时间内完成，实际 {:?}",
        cleanup_started.elapsed()
    );
    drop(first_client);

    // 锁可重新获取说明 Drop 未遗留运行中的同 home sidecar；不直接调用强杀。
    let mut env = TestEnv {
        proc: SidecarProcess::spawn(&grok_home, &session_cwd, &[], &[]),
        dir,
        session_cwd,
    };
    let mut client = connect_initialize(&mut env);
    assert_clean_eof_exit(&mut env, &mut client, "Drop 后私有 home 锁重用路径");
}

#[test]
fn stdout_is_pure_jsonrpc() {
    // 原有首响应门禁：启动后 stdout 不得出现 banner 或日志污染。
    let mut env = spawn_base();
    let stdin = env.proc.take_stdin();
    let stdout = env.proc.stdout_reader().into_inner();
    let mut client = AcpClient::new(stdin, stdout);

    let resp = client
        .request(
            "initialize",
            serde_json::json!({
                "protocolVersion": 1,
                "client": { "name": "efflab-test", "mcpServers": [] },
                "capabilities": { "terminal": false, "fs": false }
            }),
            REQ_TIMEOUT,
        )
        .expect("initialize 必须成功");
    assert_eq!(
        resp.get("jsonrpc").and_then(Value::as_str),
        Some("2.0"),
        "响应必须是合法 JSON-RPC: {resp}"
    );
    assert!(
        resp.get("result").is_some(),
        "响应必须有 result（无错误）: {resp}"
    );
    assert_clean_eof_exit(&mut env, &mut client, "stdout 首响应协议门禁");
}

#[test]
fn stdout_all_nonempty_lines_are_valid_jsonrpc_through_eof() {
    // 严格覆盖启动至 EOF 的全量 stdout，而非仅检查第一条 initialize 响应。
    let mut env = spawn_base();
    let mut client = connect_initialize(&mut env);
    let _ = create_session(&mut client, &env.session_cwd);
    assert_clean_eof_exit(&mut env, &mut client, "stdout 全量协议纯净性路径");

    let lines = raw_lines_after_exit(&client);
    assert!(
        lines.len() >= 2,
        "至少应捕获 initialize 与 session/new 两条 stdout JSON-RPC，实际: {lines:?}"
    );
    for (index, line) in lines.iter().enumerate() {
        let message: Value = serde_json::from_str(line).unwrap_or_else(|error| {
            panic!("stdout 第 {index} 条非空行必须是 JSON，实际 {line:?}，错误: {error}")
        });
        assert!(
            message.is_object(),
            "stdout 第 {index} 条必须是 JSON-RPC object，实际: {message}"
        );
        assert_eq!(
            message.get("jsonrpc").and_then(Value::as_str),
            Some("2.0"),
            "stdout 第 {index} 条必须声明 jsonrpc=2.0，实际: {message}"
        );
        let is_response = message.get("id").is_some()
            && (message.get("result").is_some() != message.get("error").is_some());
        let is_notification =
            message.get("id").is_none() && message.get("method").and_then(Value::as_str).is_some();
        assert!(
            is_response || is_notification,
            "stdout 第 {index} 条必须是合法 JSON-RPC 响应或通知，实际: {message}"
        );
    }
}

#[test]
fn trusted_host_gatekeeper_rejects_before_wire() {
    let mut env = spawn_base();
    let expected_cwd =
        dunce::canonicalize(&env.session_cwd).expect("session cwd 必须可 canonicalize");
    let policy = HostPolicy::new(expected_cwd.clone())
        .with_meta_key("modelId")
        .with_model_id("grok-code-fast");
    let stdin = env.proc.take_stdin();
    let stdout = env.proc.stdout_reader().into_inner();
    let mut client = AcpClient::new(stdin, stdout);
    let mut sent_methods = Vec::new();

    // 合法请求先通过 Host 校验，再进入 sidecar wire。
    let initialize_params = serde_json::json!({
        "protocolVersion": 1,
        "client": { "name": "trusted-efflab-host", "mcpServers": [] },
        "capabilities": { "terminal": false, "fs": false },
        "_meta": { "modelId": "grok-code-fast" }
    });
    let initialize = trusted_host_send(
        &mut client,
        &policy,
        &mut sent_methods,
        "initialize",
        initialize_params,
    )
    .expect("合法 initialize 必须通过 Host 校验");
    assert!(
        initialize.get("result").is_some(),
        "经 Host 校验的 initialize 必须被 sidecar 正常处理: {initialize}"
    );

    let legal_session_params = serde_json::json!({
        "cwd": expected_cwd,
        "mcpServers": [],
        "_meta": { "modelId": "grok-code-fast" }
    });
    let session = trusted_host_send(
        &mut client,
        &policy,
        &mut sent_methods,
        "session/new",
        legal_session_params.clone(),
    )
    .expect("合法 session/new 必须通过 Host 校验");
    assert!(
        session
            .get("result")
            .and_then(|result| result.get("sessionId"))
            .and_then(Value::as_str)
            .is_some(),
        "经 Host 校验的 session/new 必须返回 sessionId: {session}"
    );

    let mismatched_cwd = env.dir.path().join("untrusted-cwd");
    fs::create_dir_all(&mismatched_cwd).expect("创建不匹配 cwd");
    let malicious_cases = vec![
        (
            "session/new",
            serde_json::json!({
                "cwd": &expected_cwd,
                "mcpServers": [],
                "agentProfile": { "name": "evil" }
            }),
            HostRejection::UnknownField {
                method: "session/new".to_string(),
                field: "agentProfile".to_string(),
            },
        ),
        (
            "session/new",
            serde_json::json!({
                "cwd": &expected_cwd,
                "mcpServers": [],
                "permissionMode": "yolo"
            }),
            HostRejection::UnknownField {
                method: "session/new".to_string(),
                field: "permissionMode".to_string(),
            },
        ),
        (
            "initialize",
            serde_json::json!({
                "protocolVersion": 1,
                "client": { "name": "trusted-efflab-host", "mcpServers": [] },
                "capabilities": { "terminal": false, "fs": false },
                "unexpected": true
            }),
            HostRejection::UnknownField {
                method: "initialize".to_string(),
                field: "unexpected".to_string(),
            },
        ),
        (
            "session/new",
            serde_json::json!({ "cwd": &mismatched_cwd, "mcpServers": [] }),
            HostRejection::CwdMismatch {
                method: "session/new".to_string(),
                expected: policy.expected_cwd.display().to_string(),
                got: mismatched_cwd.display().to_string(),
            },
        ),
        (
            "initialize",
            serde_json::json!({
                "protocolVersion": 1,
                "client": { "name": "trusted-efflab-host", "mcpServers": [] },
                "capabilities": { "terminal": true, "fs": false }
            }),
            HostRejection::TerminalCapabilityEnabled("initialize".to_string()),
        ),
    ];

    for (method, params, expected_rejection) in malicious_cases {
        let sent_before = sent_methods.len();
        let actual_rejection =
            trusted_host_send(&mut client, &policy, &mut sent_methods, method, params)
                .expect_err("恶意请求必须在 Host 侧被拒绝，不能写入 sidecar stdin");
        assert_eq!(
            actual_rejection, expected_rejection,
            "Host 必须以精确拒绝原因拦截 method {method}"
        );
        assert_eq!(
            sent_methods.len(),
            sent_before,
            "Host 拒绝 {method} 后不得增加 wire 发送记录"
        );
    }
    assert_eq!(
        sent_methods,
        vec!["initialize".to_string(), "session/new".to_string()],
        "所有恶意请求均必须在 Host 侧止步，不能进入 sidecar wire"
    );

    // 继续发送一个合法请求，证明被 Host 拦截的恶意输入没有影响 sidecar 进程。
    let follow_up_session = trusted_host_send(
        &mut client,
        &policy,
        &mut sent_methods,
        "session/new",
        legal_session_params,
    )
    .expect("恶意请求被 Host 拦截后，后续合法 session/new 仍必须通过");
    assert!(
        follow_up_session
            .get("result")
            .and_then(|result| result.get("sessionId"))
            .and_then(Value::as_str)
            .is_some(),
        "后续合法 session/new 必须返回 sessionId: {follow_up_session}"
    );
    assert_eq!(
        sent_methods,
        vec![
            "initialize".to_string(),
            "session/new".to_string(),
            "session/new".to_string()
        ],
        "wire 仅应包含三条经 Host 明确批准的合法请求"
    );
    assert_clean_eof_exit(&mut env, &mut client, "可信 Host gatekeeper 链路");
}
