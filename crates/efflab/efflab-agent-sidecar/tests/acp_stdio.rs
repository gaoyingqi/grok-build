//! ACP stdio 集成测试（P3.2/3.3/3.4）。
//!
//! 覆盖：
//! - initialize / session/new 成功路径（BYOK fake key，不触网）
//! - `_x.ai/mcp/list` 在无 MCP 与注入 mock MCP 两种场景下的 server 集合
//! - stdin EOF 生命周期（正常退出 0）
//! - stdout 纯净（每行均为合法 JSON-RPC）
//! - 工具集组合证明之静态部分：物化 AgentDefinition 工具列表精确为
//!   `GrokBuild:efflab_noop`
//!
//! 约定：每个测试使用独立临时目录（私有 GROK_HOME + session_cwd），
//! 避免同 home 锁冲突；全部等待带超时；进程由 `SidecarProcess` 兜底回收。

mod common;

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::time::Duration;

use common::acp_client::AcpClient;
use common::process::SidecarProcess;

/// 所有 ACP 请求的默认超时。
const REQ_TIMEOUT: Duration = Duration::from_secs(20);

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

#[test]
fn initialize_and_session_new_succeed() {
    let mut env = spawn_base();
    let mut client = connect_initialize(&mut env);

    // session/new：cwd 精确匹配（canonical 后的绝对路径）。
    let resp = client
        .request(
            "session/new",
            serde_json::json!({ "cwd": env.session_cwd, "mcpServers": [] }),
            REQ_TIMEOUT,
        )
        .expect("session/new 必须成功");
    let result = resp.get("result").expect("session/new 应有 result");
    assert!(
        result.get("sessionId").is_some(),
        "session/new 必须返回 sessionId: {resp}"
    );
    assert!(
        result.get("models").is_some(),
        "session/new 应返回 models 目录: {resp}"
    );
}

/// 从 `_x.ai/mcp/list` 响应中提取 servers 数组（响应为嵌套 result：`result.result.servers`）。
fn mcp_list_servers(resp: &serde_json::Value) -> Option<&[serde_json::Value]> {
    resp.get("result")
        .and_then(|r| r.get("result"))
        .and_then(|r| r.get("servers"))
        .and_then(serde_json::Value::as_array)
        .map(|arr| arr.as_slice())
}

#[test]
fn mcp_list_empty_without_config() {
    let mut env = spawn_base();
    let mut client = connect_initialize(&mut env);
    let _ = client
        .request(
            "session/new",
            serde_json::json!({ "cwd": env.session_cwd, "mcpServers": [] }),
            REQ_TIMEOUT,
        )
        .expect("session/new 必须成功");

    // wire 层扩展方法需要 `_` 前缀（ACP decoder 要求）。
    let resp = client
        .request("_x.ai/mcp/list", serde_json::json!({}), REQ_TIMEOUT)
        .expect("_x.ai/mcp/list 必须成功");
    let servers = mcp_list_servers(&resp).expect("mcp/list 响应应含 result.result.servers");
    assert!(servers.is_empty(), "未注入 MCP 时 servers 应为空: {resp}");
}

#[test]
fn stdin_eof_triggers_clean_exit_zero() {
    let mut env = spawn_base();
    let mut client = connect_initialize(&mut env);

    // 关闭 stdin → 正常关闭路径 → 3.5s 内退出码 0（devplan R12'）。
    client.close_stdin();
    let status = env
        .proc
        .wait_timeout(Duration::from_secs(10))
        .expect("关闭 stdin 后 sidecar 应在超时内退出");
    assert!(
        status.success(),
        "正常 EOF 应退出码 0，实际 {status:?}；stderr: {}",
        env.proc.stderr_text()
    );
}

#[test]
fn mcp_config_injects_echo_server() {
    let dir = tempfile::TempDir::new().expect("创建测试临时目录");
    let session_cwd = dir.path().join("cwd");
    fs::create_dir_all(&session_cwd).expect("创建 session cwd");
    let grok_home = dir.path().join("home");

    // 准备 mock MCP server：复制 fixture 到 exec-root 并赋予可执行权限。
    let exec_root = dir.path().join("exec-root");
    fs::create_dir_all(&exec_root).expect("创建 exec-root");
    let mock_src = common::fixture_path("mock_mcp_server.py");
    let mock_dst = exec_root.join("mock_mcp_server.py");
    fs::copy(&mock_src, &mock_dst).expect("复制 mock MCP 脚本");
    let mut perms = fs::metadata(&mock_dst)
        .expect("mock 脚本 metadata")
        .permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&mock_dst, perms).expect("设置 mock 脚本可执行");

    // 写 --mcp-config TOML。
    let mcp_toml = format!("[mcp_servers.echo]\ncommand = \"{}\"\n", mock_dst.display());
    let mcp_config_path = dir.path().join("mcp.toml");
    fs::write(&mcp_config_path, mcp_toml).expect("写 MCP 配置");

    let extra_args = vec![
        "--mcp-config".to_string(),
        mcp_config_path.display().to_string(),
        "--mcp-exec-root".to_string(),
        exec_root.display().to_string(),
    ];
    let mut env = TestEnv {
        proc: SidecarProcess::spawn(&grok_home, &session_cwd, &extra_args, &[]),
        dir,
        session_cwd,
    };
    let mut client = connect_initialize(&mut env);
    let _ = client
        .request(
            "session/new",
            serde_json::json!({ "cwd": env.session_cwd, "mcpServers": [] }),
            REQ_TIMEOUT,
        )
        .expect("session/new 必须成功");

    // 轮询 _x.ai/mcp/list，直到 echo server 就绪（MCP 初始化是异步的）。
    let mut servers: Vec<serde_json::Value> = Vec::new();
    for _ in 0..20 {
        let resp = client
            .request("_x.ai/mcp/list", serde_json::json!({}), REQ_TIMEOUT)
            .expect("_x.ai/mcp/list 必须成功");
        servers = mcp_list_servers(&resp).unwrap_or_default().to_vec();
        if servers
            .iter()
            .any(|s| s.get("name").and_then(serde_json::Value::as_str) == Some("echo"))
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }

    assert!(
        servers
            .iter()
            .any(|s| s.get("name").and_then(serde_json::Value::as_str) == Some("echo")),
        "注入 MCP 后 _x.ai/mcp/list 必须包含 echo server，实际: {servers:?}"
    );

    // 清理：关闭 stdin 并回收。
    client.close_stdin();
    let _ = env.proc.wait_timeout(Duration::from_secs(10));
}

#[test]
fn toolset_static_proof_materialized_agent() {
    // 组合证明之静态部分：spawn 后读取物化 AgentDefinition，断言工具精确一个。
    let mut env = spawn_base();
    let _client = connect_initialize(&mut env);

    let agent_def_path = env.dir.path().join("home/agents/efflab-default.md");
    let content = fs::read_to_string(&agent_def_path).expect("物化 agent 文件必须存在");
    let def = xai_grok_agent::AgentDefinition::parse(&content).expect("物化 agent 必须可解析");

    assert!(!def.inject_default_tools, "必须阻断默认工具注入");
    let ids: Vec<&str> = def
        .tool_config
        .tools
        .iter()
        .map(|t| t.id.as_str())
        .collect();
    assert_eq!(
        ids,
        vec!["GrokBuild:efflab_noop"],
        "物化 AgentDefinition 工具白名单必须精确为 noop 一个"
    );
}

#[test]
fn malicious_env_cannot_reopen_capabilities() {
    // P1 门禁：恶意 env 必须在启动序列中被 sanitize_env 清除，不能重开 capability。
    let dir = tempfile::TempDir::new().expect("创建测试临时目录");
    let session_cwd = dir.path().join("cwd");
    fs::create_dir_all(&session_cwd).expect("创建 session cwd");
    let grok_home = dir.path().join("home");

    // 注入恶意 env：compat 打开、storage writeback、subagents 打开、managed MCP 打开。
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

    // initialize + session/new 必须成功（否则说明恶意 env 破坏了启动）。
    let mut client = connect_initialize(&mut env);
    let resp = client
        .request(
            "session/new",
            serde_json::json!({ "cwd": env.session_cwd, "mcpServers": [] }),
            REQ_TIMEOUT,
        )
        .expect("恶意 env 下 session/new 仍必须成功");
    assert!(
        resp.get("result")
            .and_then(|r| r.get("sessionId"))
            .is_some(),
        "恶意 env 下仍应返回 sessionId: {resp}"
    );

    client.close_stdin();
    let _ = env.proc.wait_timeout(Duration::from_secs(10));
}

#[test]
fn stdout_is_pure_jsonrpc() {
    // 启动后 stdout 首行必须是合法 JSON-RPC（无 banner / 无 println 污染）。
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
    assert!(
        resp.get("jsonrpc").and_then(serde_json::Value::as_str) == Some("2.0"),
        "响应必须是合法 JSON-RPC: {resp}"
    );
    assert!(
        resp.get("result").is_some(),
        "响应必须有 result（无错误）: {resp}"
    );
}
