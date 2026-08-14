//! sidecar 启动策略拒绝与私有 home 锁竞争集成测试。
//!
//! 每个拒绝场景均从真实二进制黑盒验证精确退出码、拒绝上下文、stdout 纯净，
//! 并检查失败前没有在目标私有 home 外留下 sidecar 物化文件或锁文件。

mod common;

use std::fs;
use std::path::Path;
use std::process::{Command, ExitStatus, Stdio};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use common::acp_client::AcpClient;
use common::process::{
    SIDECAR_BIN, SidecarProcess, apply_isolated_env, write_host_authoritative_config,
};
use efflab_agent_contract::render_authoritative_config;

/// 启动策略拒绝与锁竞争流程的最大等待时间。
///
/// 真实 sidecar 首次加载重量级运行时依赖时可能受 macOS 缓存与并发构建影响；
/// 30 秒仍是有界等待，同时避免将启动策略测试误判为挂死。
const EXIT_TIMEOUT: Duration = Duration::from_secs(30);
const ACP_TIMEOUT: Duration = Duration::from_secs(20);

/// 启动测试共用串行锁：避免多个真实 sidecar 同时争抢全局编译/运行时资源。
static STARTUP_TEST_LOCK: Mutex<()> = Mutex::new(());

/// 获取启动测试串行锁；中毒只意味着先前测试 panic，不应阻断后续清理验证。
fn startup_test_lock() -> MutexGuard<'static, ()> {
    STARTUP_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// 以 env_clear 白名单环境构造一个预期很快退出的 sidecar 命令。
fn rejected_command(working_directory: &Path, args: &[String]) -> Command {
    let mut command = Command::new(SIDECAR_BIN);
    command.current_dir(working_directory).args(args);
    apply_isolated_env(&mut command, &[]);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

/// 运行命令并在固定时间内收集退出状态、stdout 与 stderr。
///
/// stdout/stderr 分别由后台线程 drain，避免诊断输出背压；超时后显式 SIGKILL
/// 并回收子进程，确保失败测试不会留下后台 sidecar。
fn wait_rejected(mut command: Command) -> (ExitStatus, String, String) {
    #[allow(clippy::disallowed_methods)]
    let mut child = command.spawn().expect("启动拒绝场景 sidecar 失败");
    let stdout = child.stdout.take().expect("拒绝场景 stdout pipe 缺失");
    let stderr = child.stderr.take().expect("拒绝场景 stderr pipe 缺失");
    let stdout_reader = std::thread::spawn(move || read_child_output(stdout, "stdout"));
    let stderr_reader = std::thread::spawn(move || read_child_output(stderr, "stderr"));

    let deadline = std::time::Instant::now() + EXIT_TIMEOUT;
    let status = loop {
        if let Some(status) = child.try_wait().expect("检查拒绝场景进程状态失败") {
            break status;
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("启动策略拒绝进程未在 {EXIT_TIMEOUT:?} 内退出");
        }
        std::thread::sleep(Duration::from_millis(25));
    };

    let stdout = stdout_reader
        .join()
        .expect("拒绝场景 stdout 读取线程不应 panic");
    let stderr = stderr_reader
        .join()
        .expect("拒绝场景 stderr 读取线程不应 panic");
    (
        status,
        String::from_utf8_lossy(&stdout).into_owned(),
        String::from_utf8_lossy(&stderr).into_owned(),
    )
}

/// drain 一个 child 输出流，读取失败时保留错误上下文而非让后台线程 panic。
fn read_child_output(mut stream: impl std::io::Read, name: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    stream
        .read_to_end(&mut bytes)
        .unwrap_or_else(|error| panic!("读取拒绝场景 {name} 失败：{error}"));
    bytes
}

/// 所有 sidecar 启动策略拒绝均必须只返回 exit=2、写 stderr 且不污染 ACP stdout。
fn assert_startup_rejected(status: ExitStatus, stdout: &str, stderr: &str, context: &str) {
    assert_eq!(
        status.code(),
        Some(2),
        "启动策略拒绝必须精确退出码 2；stdout={stdout:?} stderr={stderr:?}"
    );
    assert!(
        stderr.contains("启动策略拒绝"),
        "stderr 必须包含启动策略拒绝上下文；实际：{stderr:?}"
    );
    assert!(
        stderr.contains(context),
        "stderr 必须包含场景上下文 {context:?}；实际：{stderr:?}"
    );
    assert!(
        stdout.is_empty(),
        "启动策略拒绝不得输出 ACP stdout：{stdout:?}"
    );
}

/// 失败在 CLI 校验前时，指定 home 及其外侧临时根目录不得出现 sidecar 物化物。
fn assert_no_sidecar_artifacts(root: &Path, home: &Path) {
    assert!(
        !home.exists(),
        "启动策略拒绝前不得创建私有 home：{}",
        home.display()
    );
    for artifact in [
        ".efflab-sidecar.lock",
        "config.toml",
        "agents/efflab-default.md",
    ] {
        assert!(
            !root.join(artifact).exists(),
            "启动策略拒绝不得在 home 外创建 {artifact}：{}",
            root.display()
        );
    }
}

#[test]
fn relative_grok_home_is_rejected_before_side_effects() {
    let _test_lock = startup_test_lock();
    let temporary = tempfile::tempdir().expect("创建临时目录");
    let session_cwd = temporary.path().join("session");
    fs::create_dir(&session_cwd).expect("创建 session cwd");
    let home = temporary.path().join("relative-home");
    let args = vec![
        "--grok-home".to_string(),
        "relative-home".to_string(),
        "--session-cwd".to_string(),
        session_cwd.display().to_string(),
    ];

    let (status, stdout, stderr) = wait_rejected(rejected_command(temporary.path(), &args));

    assert_startup_rejected(status, &stdout, &stderr, "--grok-home 必须为绝对路径");
    assert_no_sidecar_artifacts(temporary.path(), &home);
}

#[test]
fn parent_component_grok_home_is_rejected_before_side_effects() {
    let _test_lock = startup_test_lock();
    let temporary = tempfile::tempdir().expect("创建临时目录");
    let session_cwd = temporary.path().join("session");
    fs::create_dir(&session_cwd).expect("创建 session cwd");
    let home = temporary.path().join("home");
    let path_with_parent = temporary.path().join("parent").join("..").join("home");
    let args = vec![
        "--grok-home".to_string(),
        path_with_parent.display().to_string(),
        "--session-cwd".to_string(),
        session_cwd.display().to_string(),
    ];

    let (status, stdout, stderr) = wait_rejected(rejected_command(temporary.path(), &args));

    assert_startup_rejected(status, &stdout, &stderr, "--grok-home 不允许包含 ..");
    assert_no_sidecar_artifacts(temporary.path(), &home);
}

#[test]
fn unsupported_stdio_value_is_rejected_without_side_effects() {
    let _test_lock = startup_test_lock();
    let temporary = tempfile::tempdir().expect("创建临时目录");
    let session_cwd = temporary.path().join("session");
    fs::create_dir(&session_cwd).expect("创建 session cwd");
    let home = temporary.path().join("home");
    let args = vec![
        "--stdio=false".to_string(),
        "--grok-home".to_string(),
        home.display().to_string(),
        "--session-cwd".to_string(),
        session_cwd.display().to_string(),
    ];

    let (status, stdout, stderr) = wait_rejected(rejected_command(temporary.path(), &args));

    // 当前 Clap 定义只接受 flag 形式 `--stdio`；显式 false 在解析阶段 fail-closed。
    assert_eq!(
        status.code(),
        Some(2),
        "不支持的 --stdio 值必须精确退出码 2；stdout={stdout:?} stderr={stderr:?}"
    );
    assert!(
        stderr.contains("unexpected value 'false' for '--stdio'"),
        "stderr 必须保留 stdio 参数拒绝上下文：{stderr:?}"
    );
    assert!(
        stdout.is_empty(),
        "不支持的 --stdio 值不得输出 ACP stdout：{stdout:?}"
    );
    assert_no_sidecar_artifacts(temporary.path(), &home);
}

#[test]
fn missing_grok_home_is_rejected_before_side_effects() {
    let _test_lock = startup_test_lock();
    let temporary = tempfile::tempdir().expect("创建临时目录");
    let session_cwd = temporary.path().join("session");
    fs::create_dir(&session_cwd).expect("创建 session cwd");
    let home = temporary.path().join("home");

    // `--grok-home` 未出现于参数列表；通过 CLI 的唯一允许环境变量提供一个空值，
    // 使缺失项进入 sidecar 自身的启动策略拒绝分支，而不会依赖 Clap 帮助文本。
    let args = vec![
        "--session-cwd".to_string(),
        session_cwd.display().to_string(),
    ];
    let missing_home_environment = vec![("EFFLAB_GROK_HOME".to_string(), String::new())];
    let mut command = rejected_command(temporary.path(), &args);
    apply_isolated_env(&mut command, &missing_home_environment);
    let (status, stdout, stderr) = wait_rejected(command);

    assert_startup_rejected(status, &stdout, &stderr, "缺少 --grok-home");
    assert_no_sidecar_artifacts(temporary.path(), &home);
}

#[test]
fn relative_session_cwd_is_rejected_before_side_effects() {
    let _test_lock = startup_test_lock();
    let temporary = tempfile::tempdir().expect("创建临时目录");
    let home = temporary.path().join("home");
    let args = vec![
        "--grok-home".to_string(),
        home.display().to_string(),
        "--session-cwd".to_string(),
        "relative-session".to_string(),
    ];

    let (status, stdout, stderr) = wait_rejected(rejected_command(temporary.path(), &args));

    assert_startup_rejected(status, &stdout, &stderr, "--session-cwd 必须为绝对路径");
    assert_no_sidecar_artifacts(temporary.path(), &home);
}

#[test]
fn nonexistent_session_cwd_is_rejected_before_side_effects() {
    let _test_lock = startup_test_lock();
    let temporary = tempfile::tempdir().expect("创建临时目录");
    let home = temporary.path().join("home");
    let missing_session = temporary.path().join("missing-session");
    let args = vec![
        "--grok-home".to_string(),
        home.display().to_string(),
        "--session-cwd".to_string(),
        missing_session.display().to_string(),
    ];

    let (status, stdout, stderr) = wait_rejected(rejected_command(temporary.path(), &args));

    assert_startup_rejected(status, &stdout, &stderr, "无法归一化 --session-cwd");
    assert_no_sidecar_artifacts(temporary.path(), &home);
}

#[test]
fn private_home_lock_rejects_competition_and_releases_after_clean_exit() {
    let _test_lock = startup_test_lock();
    let temporary = tempfile::tempdir().expect("创建临时目录");
    let session_cwd = temporary.path().join("session");
    fs::create_dir(&session_cwd).expect("创建 session cwd");
    let grok_home = temporary.path().join("home");
    write_host_authoritative_config(&grok_home, None);

    // A 保持 stdin 打开，确保 stdio agent 不会因 EOF 而释放私有 home 锁。
    let mut process_a = SidecarProcess::spawn(&grok_home, &session_cwd, &[], &[]);
    let stdin_a = process_a.take_stdin();
    let stdout_a = process_a.stdout_reader().into_inner();
    let mut client_a = AcpClient::new(stdin_a, stdout_a);
    let initialized = client_a
        .request(
            "initialize",
            serde_json::json!({
                "protocolVersion": 1,
                "client": { "name": "startup-lock-test", "mcpServers": [] },
                "capabilities": { "terminal": false, "fs": false }
            }),
            ACP_TIMEOUT,
        )
        .expect("进程 A 应启动并持有私有 home 锁");
    assert!(
        initialized.get("result").is_some(),
        "A initialize 必须成功：{initialized}"
    );

    // B 使用同一 home，必须在启动阶段立即被锁竞争拒绝，且 stdout 完全为空。
    let mut process_b = SidecarProcess::spawn(&grok_home, &session_cwd, &[], &[]);
    let status_b = process_b
        .wait_timeout(EXIT_TIMEOUT)
        .expect("锁冲突进程 B 应在超时内退出");
    assert_eq!(
        status_b.code(),
        Some(2),
        "锁冲突必须精确退出码 2；stderr={}",
        process_b.stderr_text()
    );
    let mut stdout_b = process_b.stdout_reader();
    let mut stdout_b_text = String::new();
    use std::io::Read;
    stdout_b
        .read_to_string(&mut stdout_b_text)
        .expect("读取 B stdout");
    assert!(
        stdout_b_text.is_empty(),
        "锁冲突不得输出 ACP stdout：{stdout_b_text:?}"
    );
    assert!(
        process_b.stderr_text().contains("占用"),
        "锁冲突 stderr 必须说明 home 被占用：{}",
        process_b.stderr_text()
    );

    // A 以 stdin EOF 走正常退出，确认锁文件在该进程退出后可由 C 重获。
    client_a.close_stdin();
    let status_a = process_a
        .wait_timeout(EXIT_TIMEOUT)
        .expect("关闭 A stdin 后应在超时内正常退出");
    assert!(
        status_a.success(),
        "A 正常 EOF 应退出码 0，实际 {status_a:?}；stderr={}",
        process_a.stderr_text()
    );

    let mut process_c = SidecarProcess::spawn(&grok_home, &session_cwd, &[], &[]);
    let stdin_c = process_c.take_stdin();
    let stdout_c = process_c.stdout_reader().into_inner();
    let mut client_c = AcpClient::new(stdin_c, stdout_c);
    let initialized_c = client_c
        .request(
            "initialize",
            serde_json::json!({
                "protocolVersion": 1,
                "client": { "name": "startup-lock-reacquire-test", "mcpServers": [] },
                "capabilities": { "terminal": false, "fs": false }
            }),
            ACP_TIMEOUT,
        )
        .expect("A 正常退出后 C 必须可重新获取私有 home 锁");
    assert!(
        initialized_c.get("result").is_some(),
        "C initialize 必须成功：{initialized_c}"
    );
    client_c.close_stdin();
    let status_c = process_c
        .wait_timeout(EXIT_TIMEOUT)
        .expect("关闭 C stdin 后应在超时内正常退出");
    assert!(
        status_c.success(),
        "C 正常 EOF 应退出码 0，实际 {status_c:?}；stderr={}",
        process_c.stderr_text()
    );
}

/// Host 写出的空模型安全骨架必须在创建运行时前被 sidecar 拒绝。
#[test]
fn empty_model_skeleton_is_rejected_before_runtime_startup() {
    let _test_lock = startup_test_lock();
    let temporary = tempfile::tempdir().expect("创建临时目录");
    let session_cwd = temporary.path().join("session");
    let grok_home = temporary.path().join("home");
    let agent_definition = grok_home.join("agents").join("efflab-default.md");
    let config_path = grok_home.join("config.toml");
    fs::create_dir(&session_cwd).expect("创建 session cwd");
    fs::create_dir_all(&grok_home).expect("创建模拟 Host 私有 home");

    // Host 实际写入 renderer 的空模型输出；不能用手写不完整 TOML 代替该回归场景。
    let empty_model_skeleton =
        render_authoritative_config(&grok_home, &agent_definition, None, &[])
            .expect("空模型集合应渲染安全骨架");
    fs::write(&config_path, &empty_model_skeleton).expect("Host 写入空模型骨架应成功");
    let args = vec![
        "--grok-home".to_string(),
        grok_home.display().to_string(),
        "--session-cwd".to_string(),
        session_cwd.display().to_string(),
    ];

    let (status, stdout, stderr) = wait_rejected(rejected_command(temporary.path(), &args));

    assert_startup_rejected(status, &stdout, &stderr, "校验 Host 权威 config");
    assert_eq!(
        fs::read_to_string(&config_path).expect("读取 Host 空模型配置应成功"),
        empty_model_skeleton,
        "未配置模型时 sidecar 必须拒绝且不得覆写 Host 文件"
    );
    assert!(
        !grok_home.join(".config-init.lock").exists(),
        "空模型必须在进入 xai-grok-shell runtime 前被拒绝，不能创建 runtime 初始化锁"
    );
}

#[test]
fn missing_or_invalid_authoritative_config_is_rejected_without_overwrite() {
    let _test_lock = startup_test_lock();
    let temporary = tempfile::tempdir().expect("创建临时目录");
    let session_cwd = temporary.path().join("session");
    let grok_home = temporary.path().join("home");
    let config_path = grok_home.join("config.toml");
    fs::create_dir(&session_cwd).expect("创建 session cwd");
    let args = vec![
        "--grok-home".to_string(),
        grok_home.display().to_string(),
        "--session-cwd".to_string(),
        session_cwd.display().to_string(),
    ];

    // 缺文件时 sidecar 必须退出 2，且不能借启动路径补写 config.toml。
    let (status, stdout, stderr) = wait_rejected(rejected_command(temporary.path(), &args));
    assert_startup_rejected(status, &stdout, &stderr, "校验 Host 权威 config");
    assert!(
        !config_path.exists(),
        "缺失权威配置时 sidecar 不得创建或覆盖 config.toml"
    );

    let valid = write_host_authoritative_config(&grok_home, None);
    let invalid_cases = [
        (
            "非法 models.default",
            valid.replacen("default = \"byok\"", "default = \"other\"", 1),
        ),
        (
            "零 TTL",
            valid.replacen("cleanup_ttl_days = 36500", "cleanup_ttl_days = 0", 1),
        ),
        (
            "允许 .envrc",
            valid.replacen("load_envrc = false", "load_envrc = true", 1),
        ),
    ];
    for (case_name, invalid) in invalid_cases {
        fs::write(&config_path, &invalid).expect("写入篡改的 Host 配置应成功");
        let (status, stdout, stderr) = wait_rejected(rejected_command(temporary.path(), &args));
        assert_startup_rejected(status, &stdout, &stderr, "校验 Host 权威 config");
        assert_eq!(
            fs::read_to_string(&config_path).expect("读取篡改配置应成功"),
            invalid,
            "{case_name} 必须被拒绝且 sidecar 不得覆写 Host 文件"
        );
    }
}
