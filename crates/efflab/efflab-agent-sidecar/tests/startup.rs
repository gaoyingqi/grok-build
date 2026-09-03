//! sidecar Task 12 启动边界的黑盒测试。
//!
//! 测试从真实二进制验证 v1 runtime config、home alias、私有权限、退出码和 stdout
//! 隔离；fixture 全部在临时目录中生成，不读取或修改仓库内配置。

use std::collections::BTreeSet;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use efflab_agent_contract::{
    ApprovedMcpConfig, LoopbackModelSpec, RuntimeConfigV1, render_runtime_config_v1,
};
use efflab_agent_sidecar::hardening::MAX_RUNTIME_CONFIG_BYTES;
use fs2::FileExt;
use tempfile::TempDir;

const SIDECAR_BIN: &str = env!("CARGO_BIN_EXE_efflab-agent-sidecar");
const HOME_LOCK_FILENAME: &str = ".efflab-sidecar.lock";
const RUNTIME_CONFIG_FILENAME: &str = "runtime-config.v1.toml";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);

#[test]
fn manifest_declares_minimal_runtime_dependencies_directly() {
    let manifest: toml::Value =
        toml::from_str(include_str!("../Cargo.toml")).expect("sidecar Cargo.toml 必须可解析");
    let dependencies = manifest
        .get("dependencies")
        .and_then(toml::Value::as_table)
        .expect("sidecar 必须声明正常依赖");
    for name in [
        "agent-client-protocol",
        "xai-acp-lib",
        "serde",
        "serde_json",
        "toml",
    ] {
        assert!(
            dependencies.contains_key(name),
            "{name} 必须是 sidecar 的直接生产依赖"
        );
    }
    let acp_features = dependencies["agent-client-protocol"]
        .get("features")
        .and_then(toml::Value::as_array)
        .expect("agent-client-protocol 必须声明 features");
    assert!(
        acp_features
            .iter()
            .any(|feature| feature.as_str() == Some("unstable")),
        "agent-client-protocol 必须启用 unstable"
    );

    let clap = dependencies
        .get("clap")
        .and_then(toml::Value::as_table)
        .expect("sidecar 必须声明 crate-local clap 配置");
    assert_eq!(
        clap.get("default-features").and_then(toml::Value::as_bool),
        Some(false),
        "sidecar clap 必须关闭默认 feature"
    );
    let clap_features = clap
        .get("features")
        .and_then(toml::Value::as_array)
        .expect("sidecar clap 必须显式声明 feature allowlist");
    let clap_feature_names = clap_features
        .iter()
        .filter_map(toml::Value::as_str)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        clap_feature_names,
        BTreeSet::from(["derive", "std", "help", "usage", "error-context"]),
        "sidecar clap 只允许无颜色/无建议的最小 feature 集"
    );
    for feature in ["color", "suggestions", "env"] {
        assert!(
            !clap_feature_names.contains(feature),
            "sidecar clap 不得启用继承 workspace 或引入 anstream 的 feature {feature}"
        );
    }
}

#[test]
fn xai_acp_lib_declares_minimal_tokio_features_for_production_and_dev() {
    let manifest: toml::Value =
        toml::from_str(include_str!("../../../codegen/xai-acp-lib/Cargo.toml"))
            .expect("xai-acp-lib Cargo.toml 必须可解析");

    let normal_tokio = manifest
        .get("dependencies")
        .and_then(toml::Value::as_table)
        .and_then(|dependencies| dependencies.get("tokio"))
        .and_then(toml::Value::as_table)
        .expect("xai-acp-lib 必须声明 normal Tokio 依赖");
    assert_eq!(
        normal_tokio
            .get("default-features")
            .and_then(toml::Value::as_bool),
        Some(false),
        "normal Tokio 依赖必须关闭默认 feature"
    );
    assert_eq!(
        normal_tokio
            .get("features")
            .and_then(toml::Value::as_array)
            .expect("normal Tokio 依赖必须显式声明 features")
            .iter()
            .filter_map(toml::Value::as_str)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["rt", "sync"]),
        "normal Tokio 只允许 production 实际使用的 rt/sync"
    );

    let dev_tokio = manifest
        .get("dev-dependencies")
        .and_then(toml::Value::as_table)
        .and_then(|dependencies| dependencies.get("tokio"))
        .and_then(toml::Value::as_table)
        .expect("xai-acp-lib 必须把测试 Tokio 放入 dev-dependencies");
    assert_eq!(
        dev_tokio
            .get("default-features")
            .and_then(toml::Value::as_bool),
        Some(false),
        "dev Tokio 依赖也必须关闭默认 feature"
    );
    assert_eq!(
        dev_tokio
            .get("features")
            .and_then(toml::Value::as_array)
            .expect("dev Tokio 依赖必须显式声明 features")
            .iter()
            .filter_map(toml::Value::as_str)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["macros", "rt", "sync"]),
        "测试 Tokio 只允许宏与 current-thread runtime 所需 feature"
    );

    for feature in ["full", "process", "rt-multi-thread"] {
        assert!(
            !normal_tokio
                .get("features")
                .and_then(toml::Value::as_array)
                .into_iter()
                .flatten()
                .any(|value| value.as_str() == Some(feature)),
            "normal Tokio 不得声明禁止 feature {feature}"
        );
    }
}

struct Fixture {
    _temporary: TempDir,
    session_cwd: PathBuf,
    home: PathBuf,
    runtime_config: PathBuf,
}

impl Fixture {
    /// 创建一个可被 Host 写入的 runtime-config.v1 fixture。
    fn new() -> Self {
        let temporary = tempfile::tempdir().expect("创建临时 fixture 目录");
        let session_cwd = temporary.path().join("session");
        let home = temporary.path().join("home");
        fs::create_dir(&session_cwd).expect("创建 session cwd");
        fs::create_dir(&home).expect("创建 sidecar home");
        set_mode(&session_cwd, 0o700);
        set_mode(&home, 0o700);
        let runtime_config = home.join(RUNTIME_CONFIG_FILENAME);
        write_valid_runtime_config(&runtime_config, &session_cwd, 0o600);

        Self {
            _temporary: temporary,
            session_cwd,
            home,
            runtime_config,
        }
    }

    /// 返回默认的 v1 CLI 参数；调用方可在其后追加 alias 参数。
    fn args(&self) -> Vec<String> {
        vec![
            "--runtime-config".to_owned(),
            self.runtime_config.display().to_string(),
            "--home".to_owned(),
            self.home.display().to_string(),
            "--session-cwd".to_owned(),
            self.session_cwd.display().to_string(),
        ]
    }

    /// 组合 sidecar 进程命令，并清空继承环境，只留下测试显式提供的值。
    fn command(&self, args: &[String]) -> Command {
        sidecar_command(&self.session_cwd, args)
    }

    /// 构造带指定 L3b 绑定值的 sidecar 命令，覆盖启动边界的异常输入。
    fn command_with_bind(&self, args: &[String], bind: Option<&str>) -> Command {
        let mut command = self.command(args);
        match bind {
            Some(bind) => {
                command.env("EFFLAB_L3B_BIND", bind);
            }
            None => {
                command.env_remove("EFFLAB_L3B_BIND");
            }
        }
        command
    }
}

/// 写入由 contract renderer 生成的合法 v1 配置，并显式设置 Unix 文件权限。
fn write_valid_runtime_config(path: &Path, session_cwd: &Path, mode: u32) {
    let config = RuntimeConfigV1 {
        schema_version: 1,
        runtime_revision: String::new(),
        session_store_version: 1,
        session_cwd: session_cwd
            .to_str()
            .expect("测试 session cwd 必须是 UTF-8")
            .to_owned(),
        model: LoopbackModelSpec {
            model_id: "efflab-test-model".to_owned(),
            base_url: "http://127.0.0.1:43123/v1".to_owned(),
            backend: "chat_completions".to_owned(),
            token_env: "EFFLAB_L3B_BIND".to_owned(),
        },
        approved_mcp: ApprovedMcpConfig::default(),
        expected_tools: BTreeSet::new(),
        system_prompt: String::new(),
    };
    let rendered = render_runtime_config_v1(&config).expect("生成合法 runtime config");
    fs::write(path, rendered).expect("写入 runtime config");
    set_mode(path, mode);
}

/// 统一构造隔离环境，确保测试进程的用户代理、代理和 telemetry 不会泄漏给 child。
fn sidecar_command(working_directory: &Path, args: &[String]) -> Command {
    let mut command = Command::new(SIDECAR_BIN);
    command
        .current_dir(working_directory)
        .args(args)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("EFFLAB_L3B_BIND", "test-bind-sentinel")
        .env("XAI_API_KEY", "user-key-sentinel")
        .env("HTTP_PROXY", "proxy-sentinel")
        .env("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-sentinel")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

/// 运行会在 EOF 结束的 sidecar，并在有界时间内收集输出。
fn run_to_completion(mut command: Command) -> (ExitStatus, String, String) {
    #[allow(clippy::disallowed_methods)]
    let child = command.spawn().expect("启动 sidecar 测试进程");
    wait_for_output(child)
}

/// 并行 drain stdout/stderr 后轮询 child，防止输出背压或错误实现造成测试永久挂起。
fn wait_for_output(mut child: Child) -> (ExitStatus, String, String) {
    let stdout = child.stdout.take().expect("sidecar stdout pipe 缺失");
    let stderr = child.stderr.take().expect("sidecar stderr pipe 缺失");
    let stdout_reader = std::thread::spawn(move || read_pipe(stdout));
    let stderr_reader = std::thread::spawn(move || read_pipe(stderr));
    let deadline = Instant::now() + STARTUP_TIMEOUT;

    let status = loop {
        if let Some(status) = child.try_wait().expect("检查 sidecar 进程状态") {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            let _ = stdout_reader.join();
            let _ = stderr_reader.join();
            panic!("sidecar 未在 {STARTUP_TIMEOUT:?} 内退出");
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let stdout = stdout_reader.join().expect("读取 sidecar stdout 线程");
    let stderr = stderr_reader.join().expect("读取 sidecar stderr 线程");
    (
        status,
        String::from_utf8_lossy(&stdout).into_owned(),
        String::from_utf8_lossy(&stderr).into_owned(),
    )
}

/// 读取子进程管道，保留原始字节供测试断言 UTF-8 文本。
fn read_pipe(mut stream: impl Read) -> Vec<u8> {
    let mut bytes = Vec::new();
    stream
        .read_to_end(&mut bytes)
        .expect("读取 sidecar 输出管道");
    bytes
}

/// 启动一个保留 stdin 的 sidecar，用于验证 home 锁在进程生命周期内保持。
fn spawn_blocking_fixture(fixture: &Fixture) -> Child {
    let mut command = fixture.command(&fixture.args());
    command.stdin(Stdio::piped());
    #[allow(clippy::disallowed_methods)]
    let mut child = command.spawn().expect("启动持锁 sidecar 测试进程");
    wait_for_lock(&mut child, &fixture.home.join(HOME_LOCK_FILENAME));
    child
}

/// 等待第一进程真正持有独占锁，避免只凭锁文件存在判断启动成功。
fn wait_for_lock(child: &mut Child, lock_path: &Path) {
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    loop {
        if let Some(status) = child.try_wait().expect("检查持锁 sidecar 状态") {
            panic!("持锁 sidecar 在取得锁前退出: {status}");
        }
        if lock_path.exists()
            && let Ok(lock_file) = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(lock_path)
        {
            match lock_file.try_lock_exclusive() {
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return,
                Ok(()) => {}
                Err(_) => {}
            }
        }
        assert!(
            Instant::now() < deadline,
            "sidecar 未在 {STARTUP_TIMEOUT:?} 内真正取得 home 锁"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// 启动拒绝必须精确返回 2、写 stderr 且不污染 ACP stdout。
fn assert_rejected(status: ExitStatus, stdout: &str, stderr: &str, context: &str) {
    assert_eq!(
        status.code(),
        Some(2),
        "启动策略拒绝必须为 exit=2；stdout={stdout:?}; stderr={stderr:?}"
    );
    assert!(stdout.is_empty(), "启动策略拒绝不得写 stdout：{stdout:?}");
    assert!(
        stderr.contains(context),
        "stderr 必须包含 {context:?}；实际为 {stderr:?}"
    );
}

/// 正常 EOF 必须为 0，且当前 Task 12 stub 不写 ACP stdout。
fn assert_clean_eof(status: ExitStatus, stdout: &str, stderr: &str) {
    assert_eq!(
        status.code(),
        Some(0),
        "合法 runtime config 的 EOF 必须为 exit=0；stderr={stderr:?}"
    );
    assert!(stdout.is_empty(), "正常启动仍不得写 stdout：{stdout:?}");
}

#[cfg(unix)]
fn set_mode(path: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(mode)).expect("设置测试 fixture 权限");
}

#[cfg(not(unix))]
fn set_mode(_path: &Path, _mode: u32) {}

#[cfg(unix)]
fn mode(path: &Path) -> u32 {
    use std::os::unix::fs::PermissionsExt;

    fs::metadata(path)
        .expect("读取 fixture 权限")
        .permissions()
        .mode()
        & 0o777
}

#[cfg(unix)]
#[test]
fn runtime_config_is_required_and_legacy_shell_config_is_not_read() {
    let fixture = Fixture::new();
    let legacy_path = fixture.home.join("config.toml");
    let legacy_content = "legacy_marker = \"must-not-be-read\"\n";
    fs::write(&legacy_path, legacy_content).expect("写入 legacy shell config");

    let args = vec![
        "--home".to_owned(),
        fixture.home.display().to_string(),
        "--session-cwd".to_owned(),
        fixture.session_cwd.display().to_string(),
    ];
    let (status, stdout, stderr) = run_to_completion(fixture.command(&args));

    assert_rejected(status, &stdout, &stderr, "runtime-config");
    assert_eq!(
        fs::read_to_string(&legacy_path).expect("读取 legacy shell config"),
        legacy_content,
        "缺少 v1 参数时不得读取、修复或覆盖 legacy config.toml"
    );
    assert!(
        !fixture.home.join(HOME_LOCK_FILENAME).exists(),
        "runtime-config 缺失时必须在 home lock 之前拒绝"
    );
}

#[cfg(unix)]
#[test]
fn valid_runtime_config_is_used_without_reading_legacy_shell_config() {
    let fixture = Fixture::new();
    let legacy_path = fixture.home.join("config.toml");
    let legacy_content = "not = [valid shell config\n";
    fs::write(&legacy_path, legacy_content).expect("写入无效 legacy shell config");

    let (status, stdout, stderr) = run_to_completion(fixture.command(&fixture.args()));

    assert_clean_eof(status, &stdout, &stderr);
    assert!(
        stderr.contains("ignored_legacy_config"),
        "存在旧 config.toml 时应只记录 ignored_legacy_config：{stderr:?}"
    );
    assert_eq!(
        fs::read_to_string(&legacy_path).expect("读取 legacy shell config"),
        legacy_content,
        "sidecar 不得覆盖 legacy config.toml"
    );
}

#[cfg(unix)]
#[test]
fn missing_l3b_bind_is_rejected_before_config_and_home_lock() {
    let fixture = Fixture::new();
    let (status, stdout, stderr) =
        run_to_completion(fixture.command_with_bind(&fixture.args(), None));

    assert_rejected(status, &stdout, &stderr, "l3b_bind_invalid");
    assert!(!fixture.home.join(HOME_LOCK_FILENAME).exists());
}

#[cfg(unix)]
#[test]
fn empty_l3b_bind_is_rejected_before_config_and_home_lock() {
    let fixture = Fixture::new();
    let (status, stdout, stderr) =
        run_to_completion(fixture.command_with_bind(&fixture.args(), Some("")));

    assert_rejected(status, &stdout, &stderr, "l3b_bind_invalid");
    assert!(!fixture.home.join(HOME_LOCK_FILENAME).exists());
}

#[cfg(unix)]
#[test]
fn control_character_l3b_bind_is_rejected_without_logging_the_value() {
    let fixture = Fixture::new();
    let invalid_bind = "bind\u{001f}sentinel";
    let (status, stdout, stderr) =
        run_to_completion(fixture.command_with_bind(&fixture.args(), Some(invalid_bind)));

    assert_rejected(status, &stdout, &stderr, "l3b_bind_invalid");
    assert!(!stderr.contains(invalid_bind));
    assert!(!fixture.home.join(HOME_LOCK_FILENAME).exists());
}

#[cfg(unix)]
#[test]
fn grok_home_alias_is_rejected_when_home_is_absent() {
    let fixture = Fixture::new();
    let mut args = fixture.args();
    let home_index = args
        .iter()
        .position(|arg| arg == "--home")
        .expect("默认参数应包含 --home");
    args.drain(home_index..=home_index + 1);
    args.extend(["--grok-home".to_owned(), fixture.home.display().to_string()]);

    let (status, stdout, stderr) = run_to_completion(fixture.command(&args));

    assert_rejected(status, &stdout, &stderr, "--grok-home");
    assert!(
        !fixture.home.join(HOME_LOCK_FILENAME).exists(),
        "仅提供旧 alias 时必须在取得 home lock 前拒绝"
    );
}

#[cfg(unix)]
#[test]
fn equal_home_and_grok_home_alias_are_rejected() {
    let fixture = Fixture::new();
    let mut args = fixture.args();
    args.extend(["--grok-home".to_owned(), fixture.home.display().to_string()]);

    let (status, stdout, stderr) = run_to_completion(fixture.command(&args));

    assert_rejected(status, &stdout, &stderr, "--grok-home");
    assert!(
        !fixture.home.join(HOME_LOCK_FILENAME).exists(),
        "同时提供旧 alias 时必须在取得 home lock 前拒绝"
    );
}

#[cfg(unix)]
#[test]
fn conflicting_home_aliases_are_rejected_before_either_home_is_locked() {
    let fixture = Fixture::new();
    let other_home = fixture
        ._temporary
        .path()
        .join("other-home-that-must-not-be-created");
    let mut args = fixture.args();
    args.extend(["--grok-home".to_owned(), other_home.display().to_string()]);

    let (status, stdout, stderr) = run_to_completion(fixture.command(&args));

    assert_rejected(status, &stdout, &stderr, "--home");
    assert!(
        stderr.contains("--grok-home") && stderr.contains("冲突"),
        "alias 冲突必须同时指出两个参数和冲突分类：{stderr:?}"
    );
    assert!(
        !fixture.home.join(HOME_LOCK_FILENAME).exists(),
        "alias 冲突必须在锁定 home 前拒绝"
    );
    assert!(
        !other_home.exists(),
        "alias 冲突不得创建第二个 home：{}",
        other_home.display()
    );
}

#[cfg(unix)]
#[test]
fn invalid_runtime_config_does_not_echo_config_contents_to_stderr() {
    let fixture = Fixture::new();
    let secret = "config-secret-sentinel";
    fs::write(
        &fixture.runtime_config,
        format!("schema_version = \\\"{secret}\\\"\\n"),
    )
    .expect("写入无效 runtime config");
    set_mode(&fixture.runtime_config, 0o600);

    let (status, stdout, stderr) = run_to_completion(fixture.command(&fixture.args()));

    assert_rejected(status, &stdout, &stderr, "启动策略拒绝");
    assert!(
        !stderr.contains(secret),
        "无效配置的原文不得进入 stderr: {stderr:?}"
    );
    assert!(!fixture.home.join(HOME_LOCK_FILENAME).exists());
}

#[cfg(unix)]
#[test]
fn malformed_runtime_config_does_not_echo_mcp_server_name_or_control_characters() {
    let fixture = Fixture::new();
    let malicious_name = "evil\u{001b}[31mserver";
    let server_key = toml::Value::String(malicious_name.to_owned()).to_string();
    let valid_source =
        fs::read_to_string(&fixture.runtime_config).expect("读取合法 runtime config");
    let malformed = valid_source.replace(
        "[approved_mcp]\nservers = {}",
        &format!("[approved_mcp.servers.{server_key}]\nargs = []"),
    );
    fs::write(&fixture.runtime_config, malformed).expect("写入 malformed runtime config");
    set_mode(&fixture.runtime_config, 0o600);

    let (status, stdout, stderr) = run_to_completion(fixture.command(&fixture.args()));

    assert_rejected(status, &stdout, &stderr, "启动策略拒绝");
    assert!(
        stderr.contains("runtime_config_invalid"),
        "malformed runtime config 应使用固定错误分类: {stderr:?}"
    );
    assert!(!stderr.contains(malicious_name));
    assert!(!stderr.contains('\u{001b}'));
}

#[cfg(unix)]
#[test]
fn valid_eof_has_exit_zero_and_invalid_runtime_config_has_exit_two() {
    let fixture = Fixture::new();
    let (status, stdout, stderr) = run_to_completion(fixture.command(&fixture.args()));
    assert_clean_eof(status, &stdout, &stderr);

    let valid_source = fs::read_to_string(&fixture.runtime_config).expect("读取合法 v1 config");
    let invalid_source = valid_source.replacen("schema_version = 1", "schema_version = 999", 1);
    fs::write(&fixture.runtime_config, invalid_source).expect("写入无效 v1 config");
    set_mode(&fixture.runtime_config, 0o600);

    let (invalid_status, invalid_stdout, invalid_stderr) =
        run_to_completion(fixture.command(&fixture.args()));
    assert_rejected(
        invalid_status,
        &invalid_stdout,
        &invalid_stderr,
        "runtime_config_invalid",
    );
}

#[cfg(unix)]
#[test]
fn oversized_runtime_config_is_rejected_with_stable_error() {
    let fixture = Fixture::new();
    let secret = "oversized-config-secret-sentinel";
    let mut oversized = fs::read(&fixture.runtime_config).expect("读取合法 runtime config");
    oversized.extend_from_slice(format!("\n# {secret}\n").as_bytes());
    while oversized.len() <= MAX_RUNTIME_CONFIG_BYTES {
        oversized.extend_from_slice(b"# padding\n");
    }
    fs::write(&fixture.runtime_config, oversized).expect("写入超大 runtime config");
    set_mode(&fixture.runtime_config, 0o600);

    let (status, stdout, stderr) = run_to_completion(fixture.command(&fixture.args()));

    assert_rejected(status, &stdout, &stderr, "runtime_config_invalid");
    assert!(
        !stderr.contains(secret),
        "超大配置正文不得进入 stderr: {stderr:?}"
    );
    assert!(
        !stderr.contains(fixture.runtime_config.to_str().expect("UTF-8 config")),
        "超大配置路径不得进入 stderr: {stderr:?}"
    );
    assert!(!fixture.home.join(HOME_LOCK_FILENAME).exists());
}

#[cfg(unix)]
#[test]
fn runtime_config_and_home_require_private_modes_without_chmod() {
    let fixture = Fixture::new();
    set_mode(&fixture.runtime_config, 0o644);

    let (status, stdout, stderr) = run_to_completion(fixture.command(&fixture.args()));
    assert_rejected(status, &stdout, &stderr, "0600");
    assert!(
        !fixture.home.join(HOME_LOCK_FILENAME).exists(),
        "runtime config 权限不安全时必须在 home lock 前拒绝"
    );

    set_mode(&fixture.runtime_config, 0o600);
    set_mode(&fixture.home, 0o755);
    let (status, stdout, stderr) = run_to_completion(fixture.command(&fixture.args()));
    assert_rejected(status, &stdout, &stderr, "0700");
    assert_eq!(mode(&fixture.home), 0o755, "拒绝共享 home 时不得 chmod");
    assert!(
        !fixture.home.join(HOME_LOCK_FILENAME).exists(),
        "共享 home 被拒绝时不得创建 home lock"
    );

    set_mode(&fixture.home, 0o700);
    let (status, stdout, stderr) = run_to_completion(fixture.command(&fixture.args()));
    assert_clean_eof(status, &stdout, &stderr);
    assert_eq!(
        mode(&fixture.runtime_config),
        0o600,
        "runtime config 必须保持 0600"
    );
    assert_eq!(
        mode(&fixture.home.join(HOME_LOCK_FILENAME)),
        0o600,
        "home lock 必须为 0600"
    );
}

#[cfg(unix)]
#[test]
fn relative_session_cwd_is_rejected_before_home_lock() {
    let fixture = Fixture::new();
    let mut args = fixture.args();
    let session_index = args
        .iter()
        .position(|argument| argument == "--session-cwd")
        .expect("默认参数应包含 --session-cwd");
    args[session_index + 1] = "relative-session-cwd".to_owned();

    let (status, stdout, stderr) = run_to_completion(fixture.command(&args));

    assert_rejected(status, &stdout, &stderr, "--session-cwd");
    assert!(!fixture.home.join(HOME_LOCK_FILENAME).exists());
}

#[cfg(unix)]
#[test]
fn session_cwd_requires_owner_only_permissions_without_chmod() {
    let fixture = Fixture::new();
    set_mode(&fixture.session_cwd, 0o755);

    let (status, stdout, stderr) = run_to_completion(fixture.command(&fixture.args()));

    assert_rejected(status, &stdout, &stderr, "0700");
    assert_eq!(
        mode(&fixture.session_cwd),
        0o755,
        "拒绝共享 session cwd 时不得 chmod"
    );
    assert!(!fixture.home.join(HOME_LOCK_FILENAME).exists());
}

#[cfg(unix)]
#[test]
fn session_cwd_rejects_non_directory_without_home_lock() {
    let fixture = Fixture::new();
    fs::remove_dir(&fixture.session_cwd).expect("删除 session cwd 目录");
    fs::write(&fixture.session_cwd, b"not-a-directory").expect("创建 session cwd 普通文件");

    let (status, stdout, stderr) =
        run_to_completion(sidecar_command(fixture._temporary.path(), &fixture.args()));

    assert_rejected(status, &stdout, &stderr, "--session-cwd");
    assert!(!fixture.home.join(HOME_LOCK_FILENAME).exists());
}

#[cfg(unix)]
#[test]
fn shared_system_directory_is_not_accepted_as_session_cwd() {
    let fixture = Fixture::new();
    let shared_directory = Path::new("/tmp");
    write_valid_runtime_config(&fixture.runtime_config, shared_directory, 0o600);

    let mut args = fixture.args();
    let session_index = args
        .iter()
        .position(|argument| argument == "--session-cwd")
        .expect("默认参数应包含 --session-cwd");
    args[session_index + 1] = shared_directory.display().to_string();

    let (status, stdout, stderr) = run_to_completion(fixture.command(&args));

    assert_rejected(status, &stdout, &stderr, "专用叶目录");
    assert!(!fixture.home.join(HOME_LOCK_FILENAME).exists());
}

#[cfg(unix)]
#[test]
fn final_session_cwd_symlink_is_rejected_before_home_lock() {
    use std::os::unix::fs::symlink;

    let fixture = Fixture::new();
    let target = fixture._temporary.path().join("real-session-cwd");
    let link = fixture._temporary.path().join("session-cwd-link");
    fs::create_dir(&target).expect("创建真实 session cwd");
    set_mode(&target, 0o700);
    symlink(&target, &link).expect("创建 session cwd 符号链接");

    let mut args = fixture.args();
    let session_index = args
        .iter()
        .position(|argument| argument == "--session-cwd")
        .expect("默认参数应包含 --session-cwd");
    args[session_index + 1] = link.display().to_string();

    let (status, stdout, stderr) = run_to_completion(fixture.command(&args));

    assert_rejected(status, &stdout, &stderr, "符号链接");
    assert!(!fixture.home.join(HOME_LOCK_FILENAME).exists());
}

#[cfg(unix)]
#[test]
fn same_home_cannot_run_two_sidecars_and_lock_releases_after_eof() {
    let fixture = Fixture::new();
    let mut first = spawn_blocking_fixture(&fixture);

    let (second_status, second_stdout, second_stderr) =
        run_to_completion(fixture.command(&fixture.args()));
    assert_rejected(second_status, &second_stdout, &second_stderr, "占用");

    drop(first.stdin.take());
    let (first_status, first_stdout, first_stderr) = wait_for_output(first);
    assert_clean_eof(first_status, &first_stdout, &first_stderr);

    let (third_status, third_stdout, third_stderr) =
        run_to_completion(fixture.command(&fixture.args()));
    assert_clean_eof(third_status, &third_stdout, &third_stderr);
}

#[cfg(unix)]
#[test]
fn stdout_remains_empty_for_startup_rejection_and_eof() {
    let fixture = Fixture::new();
    let mut args = fixture.args();
    args.extend(["--stdio=false".to_owned()]);

    let (status, stdout, stderr) = run_to_completion(fixture.command(&args));
    assert_rejected(status, &stdout, &stderr, "stdio");
}

#[cfg(unix)]
#[test]
fn non_stdio_rejects_before_reading_malicious_oversized_config() {
    let fixture = Fixture::new();
    let secret = "non-stdio-config-secret-sentinel";
    let mut oversized = fs::read(&fixture.runtime_config).expect("读取合法 runtime config");
    oversized.extend_from_slice(format!("\n# {secret}\n").as_bytes());
    while oversized.len() <= MAX_RUNTIME_CONFIG_BYTES {
        oversized.extend_from_slice(b"# padding\n");
    }
    fs::write(&fixture.runtime_config, oversized).expect("写入恶意超大 runtime config");
    set_mode(&fixture.runtime_config, 0o600);

    let mut args = fixture.args();
    args.push("--stdio=false".to_owned());
    let (status, stdout, stderr) = run_to_completion(fixture.command(&args));

    assert_rejected(status, &stdout, &stderr, "当前仅支持 --stdio");
    assert!(!stderr.contains("unexpected value"));
    assert!(!stderr.contains("runtime_config_invalid"));
    assert!(!stderr.contains(secret));
    assert!(!fixture.home.join(HOME_LOCK_FILENAME).exists());
}

#[cfg(unix)]
#[test]
fn runtime_config_symlink_is_rejected_before_home_lock() {
    use std::os::unix::fs::symlink;

    let fixture = Fixture::new();
    let target = fixture._temporary.path().join("runtime-config-target.toml");
    fs::copy(&fixture.runtime_config, &target).expect("复制 runtime config target");
    fs::remove_file(&fixture.runtime_config).expect("删除 runtime config link 位置");
    symlink(&target, &fixture.runtime_config).expect("创建 runtime config 符号链接");

    let (status, stdout, stderr) = run_to_completion(fixture.command(&fixture.args()));
    assert_rejected(status, &stdout, &stderr, "符号链接");
    assert!(
        !fixture.home.join(HOME_LOCK_FILENAME).exists(),
        "runtime config 符号链接必须在 home lock 前拒绝"
    );
}

#[cfg(unix)]
#[test]
fn symlinked_existing_session_component_is_rejected_before_home_lock() {
    use std::os::unix::fs::symlink;

    let fixture = Fixture::new();
    let real_parent = fixture._temporary.path().join("real-session-parent");
    let linked_parent = fixture._temporary.path().join("linked-session-parent");
    let real_session = real_parent.join("session");
    fs::create_dir(&real_parent).expect("创建真实 session 父目录");
    fs::create_dir(&real_session).expect("创建真实 session 目录");
    symlink(&real_parent, &linked_parent).expect("创建 session 父目录符号链接");
    write_valid_runtime_config(&fixture.runtime_config, &real_session, 0o600);

    let mut args = fixture.args();
    let session_index = args
        .iter()
        .position(|argument| argument == "--session-cwd")
        .expect("默认参数应包含 --session-cwd");
    args[session_index + 1] = linked_parent.join("session").display().to_string();
    let (status, stdout, stderr) = run_to_completion(fixture.command(&args));

    assert_rejected(status, &stdout, &stderr, "符号链接");
    assert!(!fixture.home.join(HOME_LOCK_FILENAME).exists());
}

#[cfg(unix)]
#[test]
fn symlinked_existing_home_component_is_rejected_before_home_lock() {
    use std::os::unix::fs::symlink;

    let fixture = Fixture::new();
    let real_parent = fixture._temporary.path().join("real-home-parent");
    let linked_parent = fixture._temporary.path().join("linked-home-parent");
    let real_home = real_parent.join("home");
    fs::create_dir(&real_parent).expect("创建真实 home 父目录");
    fs::create_dir(&real_home).expect("创建真实 home 目录");
    symlink(&real_parent, &linked_parent).expect("创建 home 父目录符号链接");
    let runtime_config = real_home.join(RUNTIME_CONFIG_FILENAME);
    write_valid_runtime_config(&runtime_config, &fixture.session_cwd, 0o600);

    let args = vec![
        "--runtime-config".to_owned(),
        runtime_config.display().to_string(),
        "--home".to_owned(),
        linked_parent.join("home").display().to_string(),
        "--session-cwd".to_owned(),
        fixture.session_cwd.display().to_string(),
    ];
    let (status, stdout, stderr) = run_to_completion(fixture.command(&args));

    assert_rejected(status, &stdout, &stderr, "符号链接");
    assert!(!real_home.join(HOME_LOCK_FILENAME).exists());
}

#[cfg(unix)]
#[test]
fn hardlinked_runtime_config_is_rejected_before_home_lock() {
    let fixture = Fixture::new();
    let outside = fixture
        ._temporary
        .path()
        .join("runtime-config-hardlink-target");
    fs::copy(&fixture.runtime_config, &outside).expect("复制 runtime config hardlink target");
    fs::remove_file(&fixture.runtime_config).expect("删除原 runtime config");
    fs::hard_link(&outside, &fixture.runtime_config).expect("创建 runtime config 硬链接");

    let (status, stdout, stderr) = run_to_completion(fixture.command(&fixture.args()));

    assert_rejected(status, &stdout, &stderr, "硬链接");
    assert!(!fixture.home.join(HOME_LOCK_FILENAME).exists());
}

#[cfg(unix)]
#[test]
fn help_and_version_use_stderr_without_stdout_pollution() {
    for flag in ["--help", "--version"] {
        let fixture = Fixture::new();
        let args = vec![flag.to_owned()];
        let (status, stdout, stderr) = run_to_completion(fixture.command(&args));
        assert_eq!(status.code(), Some(0), "{flag} 应成功退出: {stderr:?}");
        assert!(stdout.is_empty(), "{flag} 不得污染 ACP stdout: {stdout:?}");
        assert!(!stderr.is_empty(), "{flag} 应写入 stderr");
        assert!(!fixture.home.join(HOME_LOCK_FILENAME).exists());
    }
}

#[cfg(unix)]
#[test]
fn successful_startup_emits_redacted_debug_lifecycle_logs() {
    let fixture = Fixture::new();
    let (status, stdout, stderr) = run_to_completion(fixture.command(&fixture.args()));

    assert_clean_eof(status, &stdout, &stderr);
    for event in ["startup", "home_lock_acquired", "stdin_eof"] {
        assert!(
            stderr.contains(event),
            "stderr 缺少脱敏生命周期事件 {event}: {stderr:?}"
        );
    }
    assert!(!stderr.contains(fixture.home.to_str().expect("UTF-8 home")));
    assert!(!stderr.contains("test-bind-sentinel"));
}

#[cfg(windows)]
#[test]
fn windows_startup_rejects_before_reading_config_or_creating_lock() {
    let fixture = Fixture::new();
    let original = fs::read(&fixture.runtime_config).expect("读取测试 config");
    let (status, stdout, stderr) = run_to_completion(fixture.command(&fixture.args()));

    assert_rejected(status, &stdout, &stderr, "sidecar_hardening_unavailable");
    assert!(!fixture.home.join(HOME_LOCK_FILENAME).exists());
    assert_eq!(
        fs::read(&fixture.runtime_config).expect("再次读取测试 config"),
        original,
        "Windows capability 拒绝时不得读取后改写 runtime config"
    );
}
