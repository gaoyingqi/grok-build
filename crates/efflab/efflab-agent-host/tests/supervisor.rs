//! Supervisor 的稳定路径、进程槽、环境和生命周期契约测试。
//!
//! 本文件先于实现创建，锁定 Task 5 的 fail-closed 边界；不启动真实 sidecar。

use std::ffi::OsString;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use efflab_agent_host::{
    ChildEnvironment, ChildLifecycle, ChildLifecycleOps, HostRuntimeConfig, ProcessSlotState,
    Supervisor, SupervisorError,
};

/// 构造只供 supervisor 测试使用的 Host 配置；Task 5 不会启动 sidecar。
fn config(home_root: PathBuf) -> HostRuntimeConfig {
    HostRuntimeConfig {
        sidecar_bin: home_root.join("sidecar"),
        mcp_exec_root: home_root.join("mcp"),
        home_root,
        idle_after: Duration::from_secs(60),
        l3b: efflab_agent_host::L3bRuntimeConfig::default(),
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
    let expected_scope_root = supplied_root.join("authoritative-app").join("scope-7");
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
    assert_eq!(slot.paths().home, sidecar_home);
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
