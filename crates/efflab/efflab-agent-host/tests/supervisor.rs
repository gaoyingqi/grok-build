//! Supervisor 的稳定路径、进程槽、环境和生命周期契约测试。
//!
//! 本文件先于实现创建，锁定 Task 5 的 fail-closed 边界；不启动产品 sidecar。
//! Task19 的真实启动链只使用受控 fake 进程，不连接外部服务。

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[cfg(unix)]
use serde_json::Value;

use efflab_agent_host::{
    ApprovedMcpConfig, ApprovedMcpSpec, ChildEnvironment, ChildLifecycle, ChildLifecycleOps,
    HostApp, HostRuntime, HostRuntimeConfig, KitCommand, KitReply, LlmChannelConfig,
    LlmChannelService, McpServerSpec, ProcessSlotState, ScopeId, SealedSecret, SecretGuard,
    Supervisor, SupervisorError,
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

/// 构造只供 supervisor 测试使用的 Host 配置；Task 5 不会启动 sidecar。
fn config(home_root: PathBuf) -> HostRuntimeConfig {
    HostRuntimeConfig {
        sidecar_bin: home_root.join("sidecar"),
        sidecar_log_path: home_root.join("sidecar.log"),
        mcp_exec_root: home_root.join("mcp"),
        home_root,
        idle_after: Duration::from_secs(60),
        l3b: efflab_agent_host::L3bRuntimeConfig::default(),
    }
}

#[cfg(unix)]
/// 为 Task19 启动测试提供稳定的已批准 HTTP MCP 规格。
fn task19_mcp_spec() -> ApprovedMcpSpec {
    let mut servers = ApprovedMcpConfig::default();
    servers.servers.insert(
        "demo".to_string(),
        McpServerSpec::Http {
            url: "http://127.0.0.1:4313/mcp".to_string(),
        },
    );
    ApprovedMcpSpec::from_approved(servers, BTreeSet::from(["demo__search".to_string()]))
        .expect("Task19 测试规格必须通过 ApprovedMcpSpecV1 校验")
}

#[cfg(unix)]
/// Task19 测试产品端口只暴露一个内存中的 BYOK Channel 和批准 MCP 规格。
struct Task19App {
    channel: Mutex<LlmChannelConfig>,
    mcp: ApprovedMcpSpec,
}

#[cfg(unix)]
impl HostApp for Task19App {
    fn app_id(&self) -> &str {
        "task19-supervisor-test"
    }

    fn persist_llm_channel(&self, config: &LlmChannelConfig) -> anyhow::Result<()> {
        *self.channel.lock().expect("测试 Channel 锁必须可用") = config.clone();
        Ok(())
    }

    fn load_llm_channel(&self) -> anyhow::Result<LlmChannelConfig> {
        Ok(self
            .channel
            .lock()
            .expect("测试 Channel 锁必须可用")
            .clone())
    }

    fn seal_secret(&self, plain: &[u8]) -> anyhow::Result<SealedSecret> {
        Ok(SealedSecret::new(plain.to_vec()))
    }

    fn unseal_secret(&self, sealed: &SealedSecret) -> anyhow::Result<SecretGuard> {
        Ok(SecretGuard::new(sealed.as_bytes().to_vec()))
    }

    fn mcp_for_scope(&self, _scope: &ScopeId) -> anyhow::Result<ApprovedMcpSpec> {
        Ok(self.mcp.clone())
    }
}

#[cfg(unix)]
/// Task19 的 runtime 测试不需要产品事件，sink 仅确认事件运输不会反向阻塞 actor。
struct Task19Sink;

#[cfg(unix)]
impl efflab_agent_host::KitEventSink for Task19Sink {
    fn emit(&self, _event: efflab_agent_host::KitProductEvent) -> anyhow::Result<()> {
        Ok(())
    }
}

#[cfg(unix)]
/// 等待 fake sidecar 在 launch 返回后写出只含参数/字段的观测文件。
fn wait_for_file(path: &Path) {
    for _ in 0..200 {
        if path.is_file() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!(
        "fake sidecar 未在限定时间内写出观测文件: {}",
        path.display()
    );
}

#[cfg(unix)]
/// 等待 fake sidecar 捕获到固定数量的无敏感字段 ACP request 行。
fn wait_for_acp_request_count(path: &Path, expected: usize) {
    for _ in 0..200 {
        if fs::read_to_string(path)
            .map(|requests| requests.lines().count() >= expected)
            .unwrap_or(false)
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("fake sidecar 未在限定时间内捕获预期 ACP request 数量");
}

#[cfg(unix)]
/// 由父 Rust 独立解析 fake 捕获的 JSON-RPC 顶层结构，不依赖 shell method 匹配结果。
fn read_acp_requests(path: &Path) -> Vec<Value> {
    fs::read_to_string(path)
        .expect("必须能读取 fake sidecar ACP request 观测文件")
        .lines()
        .map(|line| {
            let value: Value = serde_json::from_str(line).expect("ACP request 行必须是 JSON");
            let object = value
                .as_object()
                .expect("ACP request 顶层必须是 JSON object");
            assert_eq!(
                object.get("jsonrpc"),
                Some(&Value::String("2.0".to_string()))
            );
            assert!(object.get("id").and_then(Value::as_u64).is_some());
            assert!(object.get("method").and_then(Value::as_str).is_some());
            assert!(object.get("params").is_some());
            value
        })
        .collect()
}

#[cfg(unix)]
/// 把临时路径编码为 POSIX shell 单引号字面量，避免空格和引号改变脚本语义。
fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.display().to_string().replace('\'', "'\\''"))
}

#[cfg(unix)]
/// 创建严格解析 v1 参数和固定 ACP 请求集合的 fake sidecar；观测文件不保存敏感值。
fn write_task19_fake_sidecar(
    script_path: &Path,
    args_path: &Path,
    env_path: &Path,
    binding_marker_path: &Path,
    acp_requests_path: &Path,
    sidecar_log_path: &Path,
) {
    let script = r#"#!/bin/sh
args_path=__ARGS_PATH__
env_path=__ENV_PATH__
binding_marker_path=__BINDING_MARKER_PATH__
acp_requests_path=__ACP_REQUESTS_PATH__
sidecar_log_path=__SIDECAR_LOG_PATH__
# 在内存中保留原始 argv，解析过程 shift 后仍可检查 token 是否误入命令行。
original_argv="$*"
args_tmp="$args_path.tmp.$$"
env_tmp="$env_path.tmp.$$"
marker_tmp="$binding_marker_path.tmp.$$"
home=""
runtime_config=""
session_cwd=""
runtime_config_count=0
home_count=0
session_cwd_count=0
stdio_count=0
while [ "$#" -gt 0 ]; do
  case "$1" in
    --runtime-config)
      runtime_config_count=$((runtime_config_count + 1))
      [ "$runtime_config_count" -eq 1 ] || exit 2
      [ "$#" -ge 2 ] || exit 2
      [ -n "$2" ] || exit 2
      case "$2" in --*) exit 2 ;; esac
      printf '%s\n' --runtime-config >> "$args_tmp" || exit 3
      runtime_config="$2"
      shift 2
      ;;
    --home)
      home_count=$((home_count + 1))
      [ "$home_count" -eq 1 ] || exit 2
      [ "$#" -ge 2 ] || exit 2
      [ -n "$2" ] || exit 2
      case "$2" in --*) exit 2 ;; esac
      printf '%s\n' --home >> "$args_tmp" || exit 3
      home="$2"
      shift 2
      ;;
    --session-cwd)
      session_cwd_count=$((session_cwd_count + 1))
      [ "$session_cwd_count" -eq 1 ] || exit 2
      [ "$#" -ge 2 ] || exit 2
      [ -n "$2" ] || exit 2
      case "$2" in --*) exit 2 ;; esac
      printf '%s\n' --session-cwd >> "$args_tmp" || exit 3
      session_cwd="$2"
      shift 2
      ;;
    --stdio)
      stdio_count=$((stdio_count + 1))
      [ "$stdio_count" -eq 1 ] || exit 2
      printf '%s\n' --stdio >> "$args_tmp" || exit 3
      shift
      ;;
    --grok-home|--stdio=*)
      exit 2
      ;;
    *)
      exit 2
      ;;
  esac
done
[ "$runtime_config_count" -eq 1 ] || exit 2
[ "$home_count" -eq 1 ] || exit 2
[ "$session_cwd_count" -eq 1 ] || exit 2
[ "$stdio_count" -eq 1 ] || exit 2
[ -n "$home" ] || exit 2
[ -n "$runtime_config" ] || exit 2
[ -n "$session_cwd" ] || exit 2
[ "$runtime_config" = "$home/runtime-config.v1.toml" ] || exit 2
[ -n "$EFFLAB_L3B_BIND" ] || exit 3
test -f "$runtime_config" || exit 3
/usr/bin/grep -q '^schema_version = 1$' "$runtime_config" || exit 3
/usr/bin/grep -q '^backend = "chat_completions"$' "$runtime_config" || exit 3
/usr/bin/grep -q '^token_env = "EFFLAB_L3B_BIND"$' "$runtime_config" || exit 3

# 通过 shell 内存中的固定字符类和长度检查 token，只输出固定布尔 marker。
binding_present=false
binding_length=${#EFFLAB_L3B_BIND}
case "$EFFLAB_L3B_BIND" in
  *[!A-Za-z0-9_-]*|'') ;;
  *)
    if [ "$binding_length" -eq 43 ]; then binding_present=true; fi
    ;;
esac
[ "$binding_present" = true ] || exit 4

# 只记录变量名，避免 binding 值或其它环境秘密进入测试产物。
/usr/bin/env | /usr/bin/sed 's/=.*$//' | /usr/bin/sort > "$env_tmp" || exit 3
/bin/mv -f "$args_tmp" "$args_path" || exit 3
/bin/mv -f "$env_tmp" "$env_path" || exit 3

# fake 自己在内存中检查 argv、v1 config 和既有 sidecar log，文件只保存固定布尔值。
argv_contains_binding=false
case " $original_argv " in *"$EFFLAB_L3B_BIND"*) argv_contains_binding=true ;; esac
config_text=$(/bin/cat "$runtime_config") || exit 3
config_contains_binding=false
case "$config_text" in *"$EFFLAB_L3B_BIND"*) config_contains_binding=true ;; esac
log_text=$(/bin/cat "$sidecar_log_path" 2>/dev/null || true)
log_contains_binding=false
case "$log_text" in *"$EFFLAB_L3B_BIND"*) log_contains_binding=true ;; esac
args_text=$(/bin/cat "$args_path") || exit 3
env_text=$(/bin/cat "$env_path") || exit 3
args_file_contains_binding=false
env_file_contains_binding=false
case "$args_text" in *"$EFFLAB_L3B_BIND"*) args_file_contains_binding=true ;; esac
case "$env_text" in *"$EFFLAB_L3B_BIND"*) env_file_contains_binding=true ;; esac
/usr/bin/printf 'binding_present=%s\nargv_contains_binding=%s\nconfig_contains_binding=%s\nlog_contains_binding=%s\nargs_file_contains_binding=%s\nenv_file_contains_binding=%s\n' \
  "$binding_present" "$argv_contains_binding" "$config_contains_binding" "$log_contains_binding" \
  "$args_file_contains_binding" "$env_file_contains_binding" > "$marker_tmp" || exit 3
/bin/mv -f "$marker_tmp" "$binding_marker_path" || exit 3
[ "$argv_contains_binding" = false ] || exit 5
[ "$config_contains_binding" = false ] || exit 5
[ "$log_contains_binding" = false ] || exit 5
[ "$args_file_contains_binding" = false ] || exit 5
[ "$env_file_contains_binding" = false ] || exit 5

initialized=0
session_created=0
# 仅接受严格的顶层 JSON-RPC v1 行；未知或伪造 method 立即 exit=2。
while IFS= read -r line; do
  # 原样保存请求行供父 Rust 独立解析；该 fixture 的请求仅含协议/路径，禁止写入敏感字段。
  /usr/bin/printf '%s\n' "$line" >> "$acp_requests_path" || exit 3
  # 字段顺序由 serde_json 决定，按字段值解析但只接受固定 JSON-RPC v1 形状。
  jsonrpc=$(/usr/bin/printf '%s\n' "$line" | /usr/bin/sed -n 's/.*"jsonrpc":"\([^\"]*\)".*/\1/p')
  id=$(/usr/bin/printf '%s\n' "$line" | /usr/bin/sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  method=$(/usr/bin/printf '%s\n' "$line" | /usr/bin/sed -n 's/.*"method":"\([^\"]*\)".*/\1/p')
  [ "$jsonrpc" = "2.0" ] || exit 2
  case "$id" in ''|*[!0-9]*) exit 2 ;; esac
  [ -n "$method" ] || exit 2
  case "$line" in
    *'"params":'*) ;;
    *) exit 2 ;;
  esac
  case "$method" in
    initialize)
      [ "$initialized" -eq 0 ] || exit 2
      initialized=1
      /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":1,"agentCapabilities":{"loadSession":true,"promptCapabilities":{"image":false,"audio":false,"embeddedContext":false},"mcpCapabilities":{"http":false,"sse":false},"sessionCapabilities":{"list":{}},"auth":{}},"authMethods":[],"_meta":{"efflabRuntime":"minimal-v1","efflabSchemaVersion":1,"efflabSessionStoreVersion":1}}}\n' "$id"
      ;;
    session/new)
      [ "$initialized" -eq 1 ] || exit 2
      [ "$session_created" -eq 0 ] || exit 2
      session_created=1
      /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{"sessionId":"task19-acp-session"}}\n' "$id"
      ;;
    _x.ai/mcp/list)
      [ "$session_created" -eq 1 ] || exit 2
      /usr/bin/printf '{"jsonrpc":"2.0","id":%s,"result":{"result":{"servers":[{"name":"demo","session":{"status":"ready","tools":[{"name":"search","enabled":true}]}}]}}}\n' "$id"
      ;;
    *)
      exit 2
      ;;
  esac
done
"#
    .replace("__ARGS_PATH__", &shell_quote(args_path))
    .replace("__ENV_PATH__", &shell_quote(env_path))
    .replace("__BINDING_MARKER_PATH__", &shell_quote(binding_marker_path))
    .replace("__ACP_REQUESTS_PATH__", &shell_quote(acp_requests_path))
    .replace("__SIDECAR_LOG_PATH__", &shell_quote(sidecar_log_path));
    fs::write(script_path, script).expect("必须能写入 Task19 fake sidecar");
    fs::set_permissions(script_path, fs::Permissions::from_mode(0o700))
        .expect("Task19 fake sidecar 必须可执行");
}

#[cfg(unix)]
#[test]
fn task19_fake_sidecar_rejects_legacy_home_and_config_fallback() {
    let temporary = tempfile::tempdir().expect("创建 legacy fixture 临时目录应成功");
    let home = temporary.path().join("legacy-home");
    fs::create_dir(&home).expect("创建 legacy home 应成功");
    fs::write(home.join("config.toml"), b"legacy config").expect("写入 legacy config 应成功");
    let sidecar_path = temporary.path().join("fake-sidecar.sh");
    let args_path = temporary.path().join("captured-args");
    let env_path = temporary.path().join("captured-env");
    let binding_marker_path = temporary.path().join("binding-marker");
    let acp_requests_path = temporary.path().join("captured-acp-requests");
    let sidecar_log_path = temporary.path().join("sidecar.log");
    write_task19_fake_sidecar(
        &sidecar_path,
        &args_path,
        &env_path,
        &binding_marker_path,
        &acp_requests_path,
        &sidecar_log_path,
    );

    let status = Command::new(&sidecar_path)
        .arg("--grok-home")
        .arg(&home)
        .arg("--session-cwd")
        .arg(temporary.path())
        .arg("--stdio")
        .stdin(std::process::Stdio::null())
        .status()
        .expect("legacy fixture sidecar 必须可执行");

    assert_eq!(
        status.code(),
        Some(2),
        "canonical v1 fake 拒绝 legacy --grok-home 时必须返回 exit=2"
    );

    // 单独验证旧 home 下的 config.toml 不会被当作 v1 runtime config 回退读取。
    let fallback_status = Command::new(&sidecar_path)
        .arg("--home")
        .arg(&home)
        .arg("--session-cwd")
        .arg(temporary.path())
        .arg("--stdio")
        .env("EFFLAB_L3B_BIND", "fixture-binding")
        .stdin(std::process::Stdio::null())
        .status()
        .expect("fallback fixture sidecar 必须可执行");
    assert_eq!(
        fallback_status.code(),
        Some(2),
        "canonical v1 fake 缺少 runtime-config.v1.toml 时必须返回 exit=2"
    );
}

/// fake sidecar 只接受当前 wire method；逻辑扩展名或未知 method 必须 fail-closed。
#[cfg(unix)]
#[test]
fn task19_fake_sidecar_rejects_unknown_or_forged_acp_method() {
    let temporary = tempfile::tempdir().expect("创建 ACP method fixture 临时目录应成功");
    let home = temporary.path().join("home");
    let session_cwd = temporary.path().join("workspace");
    fs::create_dir(&home).expect("创建 fake sidecar home 应成功");
    fs::create_dir(&session_cwd).expect("创建 fake sidecar workspace 应成功");
    fs::write(
        home.join("runtime-config.v1.toml"),
        b"schema_version = 1\nbackend = \"chat_completions\"\ntoken_env = \"EFFLAB_L3B_BIND\"\n",
    )
    .expect("写入最小 v1 runtime config 应成功");

    let sidecar_path = temporary.path().join("fake-sidecar.sh");
    let args_path = temporary.path().join("captured-args");
    let env_path = temporary.path().join("captured-env");
    let binding_marker_path = temporary.path().join("binding-marker");
    let acp_requests_path = temporary.path().join("captured-acp-requests");
    let sidecar_log_path = temporary.path().join("sidecar.log");
    write_task19_fake_sidecar(
        &sidecar_path,
        &args_path,
        &env_path,
        &binding_marker_path,
        &acp_requests_path,
        &sidecar_log_path,
    );

    let binding = "a".repeat(43);
    let mut child = Command::new(&sidecar_path)
        .args([
            "--runtime-config",
            home.join("runtime-config.v1.toml")
                .to_str()
                .expect("runtime config 路径必须是 UTF-8"),
            "--home",
            home.to_str().expect("home 路径必须是 UTF-8"),
            "--session-cwd",
            session_cwd.to_str().expect("session cwd 路径必须是 UTF-8"),
            "--stdio",
        ])
        .env_clear()
        .env("EFFLAB_L3B_BIND", &binding)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("unknown method fixture sidecar 必须可执行");
    child
        .stdin
        .take()
        .expect("unknown method fixture 必须有 stdin")
        .write_all(b"{\"id\":1,\"jsonrpc\":\"2.0\",\"method\":\"x.ai/mcp/list\",\"params\":{}}\n")
        .expect("unknown method request 必须能写入 fake sidecar");

    let status = child
        .wait()
        .expect("unknown method fixture sidecar 必须能退出");
    assert_eq!(
        status.code(),
        Some(2),
        "未知/伪造 ACP method 必须由严格 fake 以 exit=2 拒绝"
    );
    wait_for_file(&acp_requests_path);
    let requests = read_acp_requests(&acp_requests_path);
    assert_eq!(requests.len(), 1, "未知 method 只应被捕获一次");
    assert_eq!(
        requests[0]["method"],
        serde_json::json!("x.ai/mcp/list"),
        "父 Rust 必须独立观察到伪造的逻辑扩展 method"
    );
}

/// Supervisor 必须物化 v1 runtime config，并将用户端点、Key 和环境秘密隔离在 Host 内。
#[cfg(unix)]
#[test]
fn supervisor_passes_v1_config_and_never_passes_secret_or_user_endpoint() {
    let temporary = tempfile::tempdir().expect("创建 Task19 临时目录应成功");
    let root_with_space = temporary.path().join("app data");
    let expected_runtime_config = root_with_space
        .join("task19-supervisor-test")
        .join("scope-a/home/runtime-config.v1.toml");
    let args_path = temporary.path().join("captured-args");
    let env_path = temporary.path().join("captured-env");
    let binding_marker_path = temporary.path().join("binding-marker");
    let acp_requests_path = temporary.path().join("captured-acp-requests");
    let sidecar_path = temporary.path().join("fake-sidecar.sh");
    let sidecar_log_path = temporary.path().join("sidecar.log");
    fs::write(&sidecar_log_path, "preexisting-safe-log\n")
        .expect("必须能写入既有 sidecar 日志内容");
    write_task19_fake_sidecar(
        &sidecar_path,
        &args_path,
        &env_path,
        &binding_marker_path,
        &acp_requests_path,
        &sidecar_log_path,
    );

    let secret_sentinel = "task19-secret-sentinel";
    let user_endpoint = "https://user-endpoint.invalid/v1";
    let app = Arc::new(Task19App {
        channel: Mutex::new(LlmChannelConfig::Byok {
            base_url: user_endpoint.to_string(),
            model_id: "task19-model".to_string(),
            api_key: SealedSecret::new(secret_sentinel.as_bytes().to_vec()),
        }),
        mcp: task19_mcp_spec(),
    });
    let service = LlmChannelService::new(
        app,
        HostRuntimeConfig {
            home_root: root_with_space,
            sidecar_bin: sidecar_path.clone(),
            sidecar_log_path: sidecar_log_path.clone(),
            mcp_exec_root: temporary.path().join("mcp"),
            idle_after: Duration::from_secs(60),
            l3b: efflab_agent_host::L3bRuntimeConfig::default(),
        },
    )
    .expect("Task19 测试 Channel 必须可构造");

    service
        .launch_scope("scope-a")
        .expect("fake sidecar 必须能完成 v1 launch 观察");
    wait_for_file(&args_path);
    wait_for_file(&env_path);
    wait_for_file(&binding_marker_path);
    wait_for_file(&expected_runtime_config);

    let args = fs::read_to_string(&args_path).expect("必须能读取 fake sidecar 参数名");
    let child_env = fs::read_to_string(&env_path).expect("必须能读取 fake sidecar 环境名");
    let binding_marker =
        fs::read_to_string(&binding_marker_path).expect("必须能读取 token 固定布尔 marker");
    let runtime_config = fs::read_to_string(&expected_runtime_config)
        .expect("必须能读取 Host 写出的 v1 runtime config");
    let fake_source = fs::read_to_string(&sidecar_path).expect("必须能读取 fake sidecar 脚本");
    let sidecar_log = fs::read_to_string(&sidecar_log_path).expect("必须能读取 sidecar 日志");

    assert_eq!(
        binding_marker,
        "binding_present=true\nargv_contains_binding=false\nconfig_contains_binding=false\nlog_contains_binding=false\nargs_file_contains_binding=false\nenv_file_contains_binding=false\n",
        "fake 必须在内存中看到 token，但所有 argv/config/log/观测文件都不得包含 token"
    );
    assert_eq!(
        args.lines().collect::<Vec<_>>(),
        vec!["--runtime-config", "--home", "--session-cwd", "--stdio"],
        "fake sidecar 只能观察固定 v1 参数名"
    );
    assert!(runtime_config.contains("schema_version = 1"));
    assert!(runtime_config.contains("demo__search"));
    assert!(runtime_config.contains("http://127.0.0.1:4313/mcp"));
    assert!(child_env.lines().any(|name| name == "EFFLAB_L3B_BIND"));
    assert!(!child_env.lines().any(|name| name == "XAI_API_KEY"));
    assert!(
        !child_env
            .lines()
            .any(|name| name == "GROK_CODE_XAI_API_KEY")
    );
    assert!(!child_env.contains('='));
    assert!(sidecar_log.contains("preexisting-safe-log"));

    // 父进程只验证脱敏后的快照，绝不把 binding token 当作断言输入或错误文本。
    for observed in [
        &fake_source,
        &args,
        &child_env,
        &binding_marker,
        &runtime_config,
        &sidecar_log,
    ] {
        assert!(!observed.contains(secret_sentinel));
        assert!(!observed.contains(user_endpoint));
    }
}

/// HostRuntime 必须沿真实 launch_scope_with_stdio 路径把非空批准 MCP 写入 v1 配置。
#[cfg(unix)]
#[test]
fn host_runtime_dispatches_non_empty_mcp_through_real_v1_launch() {
    let temporary = tempfile::tempdir().expect("创建 Task19 runtime 临时目录应成功");
    let root_with_space = temporary.path().join("app data");
    let sidecar_path = temporary.path().join("fake-sidecar.sh");
    let args_path = temporary.path().join("captured-args");
    let env_path = temporary.path().join("captured-env");
    let binding_marker_path = temporary.path().join("binding-marker");
    let acp_requests_path = temporary.path().join("captured-acp-requests");
    let sidecar_log_path = temporary.path().join("sidecar.log");
    fs::write(&sidecar_log_path, "preexisting-safe-log\n")
        .expect("必须能写入既有 sidecar 日志内容");
    write_task19_fake_sidecar(
        &sidecar_path,
        &args_path,
        &env_path,
        &binding_marker_path,
        &acp_requests_path,
        &sidecar_log_path,
    );

    let secret_sentinel = "task19-runtime-secret";
    let user_endpoint = "https://runtime-user-endpoint.invalid/v1";
    let mcp = task19_mcp_spec();
    let runtime = HostRuntime::new(
        Task19App {
            channel: Mutex::new(LlmChannelConfig::Byok {
                base_url: user_endpoint.to_string(),
                model_id: "task19-runtime-model".to_string(),
                api_key: SealedSecret::new(secret_sentinel.as_bytes().to_vec()),
            }),
            mcp,
        },
        Task19Sink,
        HostRuntimeConfig {
            home_root: root_with_space.clone(),
            sidecar_bin: sidecar_path.clone(),
            sidecar_log_path: sidecar_log_path.clone(),
            mcp_exec_root: temporary.path().join("mcp"),
            idle_after: Duration::from_secs(60),
            l3b: efflab_agent_host::L3bRuntimeConfig::default(),
        },
    );

    let reply = runtime
        .dispatch(KitCommand::NewSession {
            scope_id: "scope-a".to_string(),
            client_request_id: None,
        })
        .expect("HostRuntime 必须经真实 stdio launch 完成 NewSession");
    assert_eq!(
        reply,
        KitReply::NewSession {
            session_id: "task19-acp-session".to_string(),
        }
    );

    let expected_runtime_config = root_with_space
        .join("task19-supervisor-test")
        .join("scope-a/home/runtime-config.v1.toml");
    wait_for_file(&args_path);
    wait_for_file(&env_path);
    wait_for_file(&binding_marker_path);
    wait_for_acp_request_count(&acp_requests_path, 3);
    wait_for_file(&expected_runtime_config);

    let fake_source = fs::read_to_string(&sidecar_path).expect("必须能读取 fake sidecar 脚本");
    let args = fs::read_to_string(&args_path).expect("必须能读取 fake sidecar 参数名");
    let child_env = fs::read_to_string(&env_path).expect("必须能读取 fake sidecar 环境名");
    let binding_marker =
        fs::read_to_string(&binding_marker_path).expect("必须能读取 token 固定布尔 marker");
    let raw_requests =
        fs::read_to_string(&acp_requests_path).expect("必须能读取无敏感字段的 ACP request 原文");
    let requests = read_acp_requests(&acp_requests_path);
    let runtime_config = fs::read_to_string(&expected_runtime_config)
        .expect("必须能读取 Host 写出的 v1 runtime config");
    let sidecar_log = fs::read_to_string(&sidecar_log_path).expect("必须能读取 sidecar 日志");

    assert_eq!(
        binding_marker,
        "binding_present=true\nargv_contains_binding=false\nconfig_contains_binding=false\nlog_contains_binding=false\nargs_file_contains_binding=false\nenv_file_contains_binding=false\n",
        "fake 必须在内存中看到 token，但所有 argv/config/log/观测文件都不得包含 token"
    );
    assert_eq!(
        args.lines().collect::<Vec<_>>(),
        vec!["--runtime-config", "--home", "--session-cwd", "--stdio"],
        "fake sidecar 只能观察固定 v1 参数名"
    );
    assert_eq!(requests.len(), 3, "NewSession 链路只应产生三条 ACP request");
    let wire_methods = requests
        .iter()
        .map(|request| request["method"].as_str().expect("method 必须是字符串"))
        .collect::<Vec<_>>();
    assert_eq!(
        wire_methods,
        vec!["initialize", "session/new", "_x.ai/mcp/list"],
        "ACP wire method 必须保持 initialize → session/new → _x.ai/mcp/list 顺序"
    );
    let logical_methods = wire_methods
        .iter()
        .map(|method| match *method {
            "_x.ai/mcp/list" => "x.ai/mcp/list",
            method => method,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        logical_methods,
        vec!["initialize", "session/new", "x.ai/mcp/list"],
        "Host logical extension method 必须映射为 ACP wire 下划线前缀"
    );

    let initialize_params = requests[0]["params"]
        .as_object()
        .expect("initialize params 必须是 object");
    assert_eq!(initialize_params["protocolVersion"], Value::from(1));
    assert_eq!(
        initialize_params["clientCapabilities"],
        serde_json::json!({
            "fs": { "readTextFile": false, "writeTextFile": false },
            "terminal": false,
        })
    );
    assert_eq!(
        initialize_params["clientInfo"],
        serde_json::json!({ "name": "efflab-agent-host", "version": env!("CARGO_PKG_VERSION") })
    );
    assert!(initialize_params.get("_meta").is_none());

    let new_params = requests[1]["params"]
        .as_object()
        .expect("session/new params 必须是 object");
    assert_eq!(new_params["mcpServers"], Value::Array(Vec::new()));
    assert_eq!(
        new_params["_meta"],
        serde_json::json!({ "modelId": "byok" })
    );
    assert_eq!(
        new_params["cwd"],
        Value::String(
            fs::canonicalize(
                root_with_space
                    .join("task19-supervisor-test")
                    .join("scope-a/workspace"),
            )
            .expect("Host workspace 必须已创建且可 canonicalize")
            .display()
            .to_string(),
        )
    );

    let mcp_params = requests[2]["params"]
        .as_object()
        .expect("MCP list params 必须是 object");
    assert_eq!(
        mcp_params["sessionId"],
        Value::String("task19-acp-session".to_string())
    );
    assert!(mcp_params.get("_meta").is_none());

    assert!(runtime_config.contains("expected_tools = [\"demo__search\"]"));
    assert!(runtime_config.contains("[approved_mcp.servers.demo]"));
    assert!(runtime_config.contains("url = \"http://127.0.0.1:4313/mcp\""));
    assert!(child_env.lines().any(|name| name == "EFFLAB_L3B_BIND"));
    assert!(!child_env.contains('='));
    assert!(sidecar_log.contains("preexisting-safe-log"));

    // 只对已落盘的脱敏观测做检查，绝不把 token 放入父进程断言或错误文本。
    for observed in [
        &fake_source,
        &args,
        &child_env,
        &binding_marker,
        &raw_requests,
        &runtime_config,
        &sidecar_log,
    ] {
        assert!(!observed.contains(secret_sentinel));
        assert!(!observed.contains(user_endpoint));
    }
}

/// 相同 scope 必须复用同一内存 slot，不能为第二次 acquire 生成第二个进程所有权。
#[test]
fn acquire_reuses_one_slot_per_scope_with_initial_metadata() {
    let temporary = tempfile::tempdir().expect("创建临时 App Data 根应成功");
    let supervisor = Supervisor::new(config(temporary.path().join("app-data")), "music-app")
        .expect("绝对 App Data 根与合法 app_id 必须可构造 supervisor");

    let first = supervisor
        .acquire("library-42")
        .expect("首次 acquire 必须创建 scope slot");
    let second = supervisor
        .acquire("library-42")
        .expect("同一 scope 的第二次 acquire 必须复用 slot");

    assert!(
        Arc::ptr_eq(&first, &second),
        "同一 scope 不得创建第二个 slot 或进程所有权"
    );
    let metadata = first.metadata().expect("slot metadata 锁必须可用");
    assert_eq!(metadata.scope_id, "library-42");
    assert_eq!(metadata.pid, None, "Task 5 尚未实际 spawn sidecar");
    assert_eq!(metadata.generation, 1, "新 slot 从第一代开始");
    assert!(
        metadata.session_ids.is_empty(),
        "新 slot 尚未 attach session"
    );
    assert_eq!(metadata.current_session, None);
    assert_eq!(metadata.state, ProcessSlotState::Idle);
}

/// 组件输入若能形成路径语义，必须在 join 前 fail-closed，避免 scope 或 app_id 逃逸。
#[test]
fn sanitize_rejects_empty_traversal_separators_and_drive_prefixes() {
    for invalid in [
        "",
        ".",
        "..",
        "name/child",
        r"name\child",
        "name..suffix",
        "C:temp",
    ] {
        let error = efflab_agent_host::sanitize(invalid)
            .expect_err("空、遍历、路径分隔符或 Windows 盘符前缀的组件必须被拒绝");
        assert!(
            matches!(error, SupervisorError::InvalidPathComponent),
            "{invalid:?} 必须报告组件非法，而不是被静默规范化: {error}"
        );
    }
}

/// 相对 App Data 根会使 child cwd 依赖当前工作目录，必须在构造时拒绝。
#[test]
fn supervisor_rejects_relative_home_root() {
    let error = Supervisor::new(config(PathBuf::from("relative-app-data")), "app")
        .err()
        .expect("相对 App Data 根不得用于稳定 home/cwd");
    assert!(
        matches!(error, SupervisorError::HomeRootMustBeAbsolute),
        "相对 home_root 必须返回专用错误: {error}"
    );
}

/// canonical 化前仍必须拒绝会改变固定 join 语义的父目录组件。
#[test]
fn supervisor_rejects_parent_directory_before_canonicalization() {
    let temporary = tempfile::tempdir().expect("创建 parent-dir 路径测试目录应成功");
    let mut home_root =
        fs::canonicalize(temporary.path()).expect("临时目录的现有前缀必须可 canonicalize");
    home_root.push("app-data");
    home_root.push("..");
    home_root.push("escaped");

    let error = Supervisor::new(config(home_root), "app")
        .err()
        .expect("包含 .. 的 App Data 根不得进入 canonical 化");
    assert!(
        matches!(error, SupervisorError::HomeRootContainsParentDirectory),
        "home_root 的 .. 必须在文件系统访问前被拒绝: {error}"
    );
}

/// Host 的 home_root 本身若是任意符号链接，必须在 canonicalize 前拒绝。
#[cfg(unix)]
#[test]
fn supervisor_rejects_arbitrary_home_root_symlink() {
    use std::os::unix::fs::symlink;

    let temporary = tempfile::tempdir().expect("创建 home_root 符号链接测试目录应成功");
    let physical_root = temporary.path().join("physical-home-root");
    let outside = temporary.path().join("outside-home-root");
    let root_alias = temporary.path().join("home-root-alias");
    fs::create_dir(&physical_root).expect("必须能创建物理 home_root");
    fs::create_dir(&outside).expect("必须能创建目录外目标");
    symlink(&outside, &root_alias).expect("必须能创建任意 home_root 符号链接");

    let mut runtime_config = config(physical_root.clone());
    runtime_config.home_root = root_alias;
    runtime_config.sidecar_log_path = temporary.path().join("safe-log").join("sidecar.log");
    let error = Supervisor::new(runtime_config, "app")
        .err()
        .expect("任意 home_root 符号链接必须 fail-closed");

    assert!(
        matches!(error, SupervisorError::Io { operation, .. } if operation == "解析 Host home_root"),
        "home_root 符号链接必须按根路径解析失败处理，实际: {error}"
    );
    assert!(
        !outside.join("app").exists(),
        "拒绝 home_root 符号链接时不得把 Host 目录写入链接目标"
    );
}

/// sidecar 日志父目录若是任意符号链接，必须在 canonicalize 前拒绝。
#[cfg(unix)]
#[test]
fn supervisor_rejects_arbitrary_sidecar_log_parent_symlink() {
    use std::os::unix::fs::symlink;

    let temporary = tempfile::tempdir().expect("创建日志父目录符号链接测试目录应成功");
    let home_root = temporary.path().join("home-root");
    let outside = temporary.path().join("outside-log-root");
    let log_parent_alias = temporary.path().join("log-parent-alias");
    fs::create_dir(&home_root).expect("必须能创建 Host home_root");
    fs::create_dir(&outside).expect("必须能创建日志目录外目标");
    symlink(&outside, &log_parent_alias).expect("必须能创建任意日志父目录符号链接");

    let mut runtime_config = config(home_root.clone());
    runtime_config.sidecar_log_path = log_parent_alias.join("sidecar.log");
    let error = Supervisor::new(runtime_config, "app")
        .err()
        .expect("任意 sidecar 日志父目录符号链接必须 fail-closed");

    assert!(
        matches!(error, SupervisorError::Io { operation, .. } if operation == "解析 sidecar 日志路径"),
        "日志父目录符号链接必须按路径解析失败处理，实际: {error}"
    );
    assert!(
        !outside.join("sidecar.log").exists(),
        "拒绝日志父目录符号链接时不得在目录外创建日志"
    );
}

/// macOS 默认临时根即使经过 `/var` 别名也必须保留缺失尾部处理。
#[cfg(target_os = "macos")]
#[test]
fn supervisor_accepts_macos_temp_path_and_system_aliases() {
    let temporary = tempfile::tempdir().expect("创建 macOS 临时目录应成功");
    let temporary_root = temporary.path().join("app-data");
    let canonical_temporary_root = fs::canonicalize(temporary.path())
        .expect("macOS 默认临时目录的现有前缀必须可 canonicalize")
        .join("app-data");
    let supervisor = Supervisor::new(config(temporary_root), "app")
        .expect("macOS 默认临时路径必须可构造 Supervisor");
    assert!(
        supervisor
            .paths_for("scope")
            .expect("合法 scope 必须能派生路径")
            .home
            .starts_with(&canonical_temporary_root),
        "默认临时路径的缺失尾部必须保留在 Host 根下"
    );

    for (alias, canonical) in [
        (Path::new("/var"), Path::new("/private/var")),
        (Path::new("/tmp"), Path::new("/private/tmp")),
        (Path::new("/etc"), Path::new("/private/etc")),
    ] {
        let suffix = format!("efflab-supervisor-alias-{}", std::process::id());
        let home_root = alias.join(format!("{suffix}-home"));
        let log_path = alias.join(format!("{suffix}-log")).join("sidecar.log");
        let mut runtime_config = config(home_root.clone());
        runtime_config.sidecar_log_path = log_path;
        let supervisor = Supervisor::new(runtime_config, "app")
            .expect("允许的 macOS 系统路径别名必须可构造 Supervisor");
        let paths = supervisor
            .paths_for("scope")
            .expect("允许的系统路径别名必须保留缺失尾部");
        assert_eq!(
            paths.home,
            canonical
                .join(format!("{suffix}-home"))
                .join("app")
                .join("scope")
                .join("home")
        );
    }
}

/// Host 必须把 app_id 追加到调用方给出的 App Data 根，而非信任调用方已预拼产品目录。
#[test]
fn paths_force_app_id_join_and_remain_absolute() {
    let temporary = tempfile::tempdir().expect("创建临时 App Data 根应成功");
    let supplied_root = temporary.path().join("caller-already-added-an-app-name");
    let supervisor = Supervisor::new(config(supplied_root.clone()), "authoritative-app")
        .expect("绝对 App Data 根与合法 app_id 必须可构造 supervisor");

    let slot = supervisor
        .acquire("scope-7")
        .expect("合法 scope 必须可取得 slot");
    let paths = slot.paths();
    let expected_scope_root = fs::canonicalize(temporary.path())
        .expect("Host App Data 根的现有前缀必须可 canonicalize")
        .join("caller-already-added-an-app-name")
        .join("authoritative-app")
        .join("scope-7");
    assert_eq!(paths.home, expected_scope_root.join("home"));
    assert_eq!(paths.workspace, expected_scope_root.join("workspace"));
    assert!(paths.home.is_absolute());
    assert!(paths.workspace.is_absolute());
    assert!(
        paths.home.starts_with(&expected_scope_root),
        "home 必须保持在 Host 派生的 app_id/scope 根目录内"
    );
    assert!(
        paths.workspace.starts_with(&expected_scope_root),
        "workspace 必须保持在 Host 派生的 app_id/scope 根目录内"
    );
}

/// sidecar 已拥有的私有 home lock 不得阻塞 Host 的独立 process-slot metadata。
#[test]
fn acquire_does_not_contend_with_sidecar_home_lock() {
    let temporary = tempfile::tempdir().expect("创建临时 App Data 根应成功");
    let root = temporary.path().join("app-data");
    let sidecar_home = root.join("app").join("scope").join("home");
    fs::create_dir_all(&sidecar_home).expect("创建模拟 sidecar home 应成功");
    let sidecar_lock = sidecar_home.join(".efflab-sidecar.lock");
    fs::write(&sidecar_lock, b"sidecar owns this lock").expect("写入模拟 sidecar lock 应成功");

    let supervisor = Supervisor::new(config(root), "app").expect("合法配置必须可构造 supervisor");
    let slot = supervisor
        .acquire("scope")
        .expect("Host process-slot metadata 不得争抢 sidecar home lock");

    assert_eq!(
        fs::read(&sidecar_lock).expect("sidecar lock 必须保持可读"),
        b"sidecar owns this lock",
        "Host 不得改写或替换 sidecar 的唯一 home lock"
    );
    assert_eq!(
        slot.paths().home,
        fs::canonicalize(&sidecar_home).expect("sidecar home 的现有前缀必须可 canonicalize")
    );
}

/// 环境构造必须拒绝已知的 sidecar 不安全开关和用户 Key 形态，且不回显其值。
#[test]
fn child_environment_rejects_forbidden_variables_and_user_key_values() {
    let temporary = tempfile::tempdir().expect("创建临时 App Data 根应成功");
    let grok_home = temporary.path().join("grok-home");

    for forbidden in ["GROK_CHAT_MODE", "XAI_API_KEY", "GROK_CODE_XAI_API_KEY"] {
        let error = ChildEnvironment::from_whitelist(
            &grok_home,
            [(forbidden.to_string(), OsString::from("not-a-real-secret"))],
        )
        .err()
        .expect("已知不安全环境变量必须被拒绝");
        assert!(
            matches!(error, SupervisorError::EnvironmentVariableNotAllowed { ref name } if name == forbidden),
            "{forbidden} 必须按变量名 fail-closed: {error}"
        );
    }

    let error = ChildEnvironment::from_whitelist(
        &grok_home,
        [("PATH".to_string(), OsString::from("sk-user-key-shape"))],
    )
    .err()
    .expect("sk- 前缀的用户 Key 值不得进入 child env");
    assert!(
        matches!(error, SupervisorError::EnvironmentValueNotAllowed { ref name } if name == "PATH"),
        "用户 Key 值只能按变量名报告，不能回显值: {error}"
    );
}

/// env_clear 后只能留下显式白名单和 Host 强制提供的 GROK_HOME。
#[cfg(unix)]
#[test]
fn child_environment_applies_env_clear_and_preserves_grok_home() {
    let temporary = tempfile::tempdir().expect("创建临时 App Data 根应成功");
    let grok_home = temporary.path().join("grok-home");
    let environment = ChildEnvironment::from_whitelist(
        &grok_home,
        [("PATH".to_string(), OsString::from("/usr/bin:/bin"))],
    )
    .expect("PATH 与 Host 提供的 GROK_HOME 必须可进入白名单");

    assert_eq!(
        environment.get("GROK_HOME"),
        Some(grok_home.as_os_str()),
        "GROK_HOME 必须由 Host 强制保留为该 scope 的私有 home"
    );
    assert!(
        environment.get("EFFLAB_L3B_BIND").is_none(),
        "Task 5 的 child env 不得提前注入 L3b binding token"
    );

    let mut command = Command::new("sh");
    command
        .arg("-c")
        .arg(
            "test -n \"$GROK_HOME\" && \\
             test -z \"${GROK_CHAT_MODE+x}\" && \\
             test -z \"${XAI_API_KEY+x}\" && \\
             test -z \"${GROK_CODE_XAI_API_KEY+x}\" && \\
             test -z \"${UNLISTED_PARENT_VALUE+x}\"",
        )
        .env("GROK_CHAT_MODE", "enabled")
        .env("XAI_API_KEY", "not-a-real-secret")
        .env("GROK_CODE_XAI_API_KEY", "not-a-real-secret")
        .env("UNLISTED_PARENT_VALUE", "must-not-survive");
    environment.apply(&mut command);

    let status = command
        .status()
        .expect("受控 child env 下 shell 必须可启动");
    assert!(
        status.success(),
        "env_clear 后只允许白名单与 GROK_HOME，禁止继承测试注入的变量"
    );
}

/// 生命周期 fake 只替代操作系统进程边界，验证 Drop 的真实顺序和固定超时。
struct RecordingChild {
    events: Arc<Mutex<Vec<String>>>,
    wait_results: Vec<bool>,
}

impl RecordingChild {
    /// 向共享记录写入一个生命周期步骤，供 Drop 后断言。
    fn record(&self, event: impl Into<String>) {
        self.events
            .lock()
            .expect("测试记录锁不应中毒")
            .push(event.into());
    }
}

impl ChildLifecycleOps for RecordingChild {
    fn cancel_in_flight(&mut self) -> Result<(), SupervisorError> {
        self.record("cancel");
        Ok(())
    }

    fn close_stdin(&mut self) -> Result<(), SupervisorError> {
        self.record("close-stdin");
        Ok(())
    }

    fn wait_for_exit(&mut self, timeout: Duration) -> Result<bool, SupervisorError> {
        self.record(format!("wait-{}ms", timeout.as_millis()));
        Ok(self.wait_results.remove(0))
    }

    fn terminate(&mut self) -> Result<(), SupervisorError> {
        self.record("term");
        Ok(())
    }

    fn kill(&mut self) -> Result<(), SupervisorError> {
        self.record("kill");
        Ok(())
    }
}

/// Drop 遇到 in-flight 回合必须先 cancel，再依次执行 stdin、TERM 和 KILL 兜底。
#[test]
fn child_lifecycle_drop_cancels_then_escalates_with_fixed_grace_periods() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let child = RecordingChild {
        events: Arc::clone(&events),
        wait_results: vec![false, false],
    };
    let lifecycle = ChildLifecycle::new(Box::new(child), true);

    drop(lifecycle);

    assert_eq!(
        *events.lock().expect("测试记录锁不应中毒"),
        vec![
            "cancel".to_string(),
            "close-stdin".to_string(),
            "wait-3500ms".to_string(),
            "term".to_string(),
            "wait-2000ms".to_string(),
            "kill".to_string(),
        ],
        "Drop 必须按 cancel → close stdin 3.5s → TERM 2s → KILL 的顺序执行"
    );
}

/// Windows 盘符相对组件会让 Path::join 丢弃左侧根目录，app_id 与 scope 均必须拒绝。
#[cfg(windows)]
#[test]
fn windows_rejects_drive_relative_app_id_and_scope() {
    let root = std::env::temp_dir().join("efflab-agent-host-windows-drive-prefix");

    let app_id_error = Supervisor::new(config(root.clone()), "C:temp")
        .err()
        .expect("盘符相对 app_id 不得越过 Host 强制根目录");
    assert!(matches!(
        app_id_error,
        SupervisorError::InvalidPathComponent
    ));

    let supervisor =
        Supervisor::new(config(root), "windows-app").expect("合法 app_id 必须可构造 supervisor");
    let scope_error = supervisor
        .paths_for("C:temp")
        .err()
        .expect("盘符相对 scope 不得越过 Host 强制根目录");
    assert!(matches!(scope_error, SupervisorError::InvalidPathComponent));
}

/// Windows 上无 Channel 与硬化不可用同时出现时，优先报告 sidecar 不可用而非缺少 Key。
/// 该断言只在 Windows CI 运行；非 Windows 目标不编译此平台能力分支。
#[cfg(windows)]
struct WindowsUnconfiguredApp;

#[cfg(windows)]
impl efflab_agent_host::HostApp for WindowsUnconfiguredApp {
    fn app_id(&self) -> &str {
        "windows-capability-test"
    }

    fn persist_llm_channel(
        &self,
        _cfg: &efflab_agent_host::LlmChannelConfig,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn load_llm_channel(&self) -> anyhow::Result<efflab_agent_host::LlmChannelConfig> {
        Ok(efflab_agent_host::LlmChannelConfig::Unconfigured)
    }

    fn seal_secret(&self, plain: &[u8]) -> anyhow::Result<efflab_agent_host::SealedSecret> {
        Ok(efflab_agent_host::SealedSecret::new(plain.to_vec()))
    }

    fn unseal_secret(
        &self,
        sealed: &efflab_agent_host::SealedSecret,
    ) -> anyhow::Result<efflab_agent_host::SecretGuard> {
        Ok(efflab_agent_host::SecretGuard::new(
            sealed.as_bytes().to_vec(),
        ))
    }

    fn mcp_for_scope(
        &self,
        _scope: &efflab_agent_host::ScopeId,
    ) -> anyhow::Result<efflab_agent_host::ApprovedMcpSpec> {
        Ok(efflab_agent_host::ApprovedMcpSpec::default())
    }
}

#[cfg(windows)]
struct DiscardWindowsSink;

#[cfg(windows)]
impl efflab_agent_host::KitEventSink for DiscardWindowsSink {
    fn emit(&self, _event: efflab_agent_host::KitProductEvent) -> anyhow::Result<()> {
        Ok(())
    }
}

#[cfg(windows)]
#[test]
fn windows_unconfigured_capability_prefers_sidecar_unavailable() {
    use efflab_agent_host::{HostRuntime, KitCommand};

    let root = std::env::temp_dir().join("efflab-agent-host-windows-capability");
    let runtime = HostRuntime::new(WindowsUnconfiguredApp, DiscardWindowsSink, config(root));
    let error = runtime
        .dispatch(KitCommand::GetCapability)
        .expect_err("Windows 硬化不可用必须优先于无 Channel 返回");

    assert_eq!(error.code, "sidecar_unavailable");
    assert!(error.retryable, "平台硬化不可用必须可重试");
}

/// Windows 必须保留 Supervisor、lifecycle 和 kill API 的编译形状，同时 fail-closed。
#[cfg(windows)]
#[test]
fn windows_reports_unavailable_and_keeps_kill_api_compilable() {
    use efflab_agent_host::{SupervisorCapability, UnavailableReason};

    let root = std::env::temp_dir().join("efflab-agent-host-windows-supervisor");
    let supervisor = Supervisor::new(config(root), "windows-app")
        .expect("绝对 Windows 临时目录必须可构造 supervisor");
    assert_eq!(
        supervisor.capability(),
        SupervisorCapability::Unavailable {
            reason: UnavailableReason::SidecarHardeningUnavailable,
        }
    );
    let error = supervisor
        .acquire("scope")
        .err()
        .expect("Windows supervisor 不得 spawn 或取得 scope slot");
    assert!(matches!(
        error,
        SupervisorError::Unavailable {
            reason: UnavailableReason::SidecarHardeningUnavailable
        }
    ));

    let events = Arc::new(Mutex::new(Vec::new()));
    let child = RecordingChild {
        events,
        wait_results: vec![false, false],
    };
    let mut lifecycle = ChildLifecycle::new(Box::new(child), false);
    lifecycle
        .shutdown()
        .expect("Windows 等价 kill API 必须保留可调用形状");
}
