//! Scope 隔离的 sidecar 进程监督基础设施。
//!
//! 本模块冻结稳定 home/cwd、scope slot、受控子进程环境与退出生命周期；Task 7 在此
//! 执行监听后注册 token、写权威配置、再启动 sidecar 的完整顺序。Task 7b 才会接管
//! child 的 ACP stdio 读写。sidecar 私有 home 的 `.efflab-sidecar.lock` 始终由 sidecar
//! 自己持有，Host 只维护内存 process-slot metadata。

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use efflab_agent_contract::{LoopbackModelSpec, RuntimeConfigV1, render_runtime_config_v1};
use xai_tty_utils::{ProcessGroup, ProcessScope, detach_std_command};

use crate::HostRuntimeConfig;
use crate::app_port::ApprovedMcpSpecV1;
use crate::llm_channel::LlmChannelManager;
use crate::llm_loopback::{BindingToken, BindingTokenRegistry, L3bLoopback};

/// 关闭 stdin 后等待 sidecar 自然退出的固定宽限期。
pub const STDIN_CLOSE_GRACE: Duration = Duration::from_millis(3_500);
/// 发出终止请求后等待 sidecar 退出的固定宽限期。
pub const TERMINATE_GRACE: Duration = Duration::from_secs(2);
/// Host 与 v1 sidecar 之间唯一共享的 runtime 配置文件名。
const RUNTIME_CONFIG_FILENAME: &str = "runtime-config.v1.toml";
/// RuntimeConfigV1 固定 schema 版本。
const RUNTIME_SCHEMA_VERSION: u32 = 1;
/// RuntimeConfigV1 固定 session store 版本。
const RUNTIME_SESSION_STORE_VERSION: u32 = 1;
/// sidecar 连接 Host L3b 时唯一允许的后端。
const RUNTIME_BACKEND: &str = "chat_completions";
/// sidecar 从此环境变量读取本代 binding token。
const RUNTIME_TOKEN_ENV: &str = "EFFLAB_L3B_BIND";

/// Windows fail-closed 时对外暴露的不可用原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnavailableReason {
    /// Windows 尚无满足 sidecar 私有 home 硬化要求的实现。
    SidecarHardeningUnavailable,
}

impl fmt::Display for UnavailableReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SidecarHardeningUnavailable => {
                formatter.write_str("sidecar_hardening_unavailable")
            }
        }
    }
}

/// Supervisor 当前能否取得或启动 sidecar scope slot。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupervisorCapability {
    /// 当前平台允许后续 Task 7 为 slot 组装真实 sidecar。
    Available,
    /// 当前平台必须 fail-closed，禁止 acquire 或 spawn。
    Unavailable {
        /// 不可用的稳定 machine-readable 原因。
        reason: UnavailableReason,
    },
}

/// Supervisor、路径和子进程环境边界的失败类型。
#[derive(Debug)]
pub enum SupervisorError {
    /// app_id 或 scope 为空、含遍历语义或路径分隔符。
    InvalidPathComponent,
    /// `home_root` 或 `GROK_HOME` 不是绝对路径。
    HomeRootMustBeAbsolute,
    /// `home_root` 或 `GROK_HOME` 包含 `..`，会破坏词法稳定性。
    HomeRootContainsParentDirectory,
    /// 当前平台不允许 supervisor 取得 scope slot。
    Unavailable {
        /// 不可用的稳定 machine-readable 原因。
        reason: UnavailableReason,
    },
    /// child env 中出现明确禁止的变量名。
    EnvironmentVariableNotAllowed {
        /// 被拒绝的变量名；永不保存或显示变量值。
        name: String,
    },
    /// child env 中出现未登记的变量名。
    EnvironmentVariableNotWhitelisted {
        /// 未登记的变量名；永不保存或显示变量值。
        name: String,
    },
    /// child env 中的值形如用户 Key；错误绝不保存或显示该值。
    EnvironmentValueNotAllowed {
        /// 对应变量名，不包含敏感值。
        name: String,
    },
    /// L3b 或 Channel 未处于可安全启动 sidecar 的状态。
    LlmChannelUnavailable,
    /// Host 未能取得当前 scope 的已批准 MCP 规格。
    McpSpecUnavailable,
    /// 同一 scope 已有正在启动或存活的 sidecar，禁止双进程竞争私有 home。
    ScopeAlreadyRunning,
    /// contract renderer 拒绝构造权威 config.toml。
    ConfigRenderFailed,
    /// Host 不能安全原子写入权威 config.toml。
    ConfigWriteFailed,
    /// 批量 restart 至少一个 scope 失败；新 Channel 已提交，调用方可重试。
    RestartFailed,
    /// scope slot 的内部状态锁已中毒，不能安全继续复用。
    StateUnavailable,
    /// 已启动 child 的 ACP stdin/stdout 已被另一个 IO actor 接管或不可用。
    StdioUnavailable,
    /// 生命周期底层操作失败；保留 I/O 分类但不记录环境内容。
    Io {
        /// 失败的生命周期动作名称。
        operation: &'static str,
        /// 底层 I/O 错误。
        source: io::Error,
    },
}

impl fmt::Display for SupervisorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPathComponent => {
                formatter.write_str("路径组件为空、包含遍历语义或路径分隔符")
            }
            Self::HomeRootMustBeAbsolute => formatter.write_str("Host App Data 根必须是绝对路径"),
            Self::HomeRootContainsParentDirectory => {
                formatter.write_str("Host App Data 根不允许包含 ..")
            }
            Self::Unavailable { reason } => write!(formatter, "sidecar 不可用: {reason}"),
            Self::EnvironmentVariableNotAllowed { .. } => {
                formatter.write_str("子进程环境包含禁止变量")
            }
            Self::EnvironmentVariableNotWhitelisted { .. } => {
                formatter.write_str("子进程环境变量不在白名单")
            }
            Self::EnvironmentValueNotAllowed { .. } => {
                formatter.write_str("子进程环境值形如用户 Key")
            }
            Self::LlmChannelUnavailable => formatter.write_str("LLM Channel 或 L3b 不可用"),
            Self::McpSpecUnavailable => formatter.write_str("MCP 批准规格不可用"),
            Self::ScopeAlreadyRunning => formatter.write_str("scope 已有 sidecar 进程"),
            Self::ConfigRenderFailed => formatter.write_str("权威 sidecar 配置渲染失败"),
            Self::ConfigWriteFailed => formatter.write_str("权威 sidecar 配置写入失败"),
            Self::RestartFailed => formatter.write_str("sidecar 批量重启失败"),
            Self::StateUnavailable => formatter.write_str("scope slot 状态不可用"),
            Self::StdioUnavailable => formatter.write_str("sidecar ACP stdio 不可用"),
            Self::Io { operation, source } => write!(formatter, "子进程{operation}失败: {source}"),
        }
    }
}

impl std::error::Error for SupervisorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// 将 app_id 或 scope 约束为单一、不可遍历的目录组件。
///
/// 该函数不替换、截断或归一化输入，避免不同不透明 scope 映射到同一个目录；也拒绝
/// `:`，使 Windows 盘符前缀不能改变后续 `Path::join` 的固定根目录。
pub fn sanitize(component: &str) -> Result<String, SupervisorError> {
    if component.is_empty()
        || component == "."
        || component.contains("..")
        || component.contains('/')
        || component.contains('\\')
        // `:` 可构成 Windows 盘符前缀；跨平台稳定标识一律拒绝，避免 join 丢弃左侧根目录。
        || component.contains(':')
        || component.contains('\0')
    {
        return Err(SupervisorError::InvalidPathComponent);
    }

    Ok(component.to_owned())
}

/// 一个 scope 固定派生出的 sidecar 私有 home 与非产品库 workspace。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopePaths {
    /// 传给 sidecar `--home` 的私有、稳定目录。
    pub home: PathBuf,
    /// 传给 sidecar `--session-cwd` 的 Host 管理目录，绝不使用产品库根。
    pub workspace: PathBuf,
}

/// process-slot 的可观察生命周期状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessSlotState {
    /// 本 task 尚未启动 sidecar；或未来进程已空闲。
    Idle,
    /// 该 scope 正在处理 prompt，禁止闲置回收。
    Prompting,
    /// 该 scope 正在关闭或升级终止。
    Killing,
}

/// Host 独有的内存 process-slot metadata。
///
/// 它绝不对应、创建、打开或竞争 `{GROK_HOME}/.efflab-sidecar.lock`；后者是 sidecar
/// 进程的唯一资源锁。未启动或 child 已退出时 `pid` 保持 `None`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessSlotMetadata {
    /// 经校验后的不透明 scope 标识。
    pub scope_id: String,
    /// 真实 spawn 接线完成前为空；之后记录该代 sidecar pid。
    pub pid: Option<u32>,
    /// sidecar 进程生成代；新 scope slot 固定从 1 开始。
    pub generation: u64,
    /// 已 attach 到该 scope child 的 session 标识集合。
    pub session_ids: BTreeSet<String>,
    /// 当前活跃 session；Task 5 尚未 attach 时为 `None`。
    pub current_session: Option<String>,
    /// 当前进程槽的生命周期状态。
    pub state: ProcessSlotState,
}

/// 已启动 child 与其 binding token 的私有所有权；不向外暴露 token 或 stdio。
struct ManagedSidecar {
    child: Child,
    /// 强引用维持受验证的 process group/job；`ProcessScope` 仅保存 Weak 用于崩溃兜底回收。
    process_group: Arc<ProcessGroup>,
    /// registry 保留 token 本体；child 所有权只保留失效所需的 registry 与 generation。
    registry: Arc<BindingTokenRegistry>,
    /// 用于在不抢占 metadata 锁的情况下立即撤销本代 token。
    scope_id: String,
    generation: u64,
}

/// scope slot 中不进入产品可观察 metadata 的启动协调状态。
struct ScopeSlotRuntime {
    child: Option<ManagedSidecar>,
    has_started: bool,
    launching: bool,
    /// child 暂时被 stop 入口持有；watcher 必须等待，而不是误判为自然退出。
    stopping: bool,
    /// kill/wait 未确认 child 已退出时禁止下一次 launch，避免同 scope 双进程。
    restart_blocked: bool,
}

/// spawn 已成功但尚未挂回 slot 的 child 所有权守卫。
///
/// 只有 `attach` 成功把 process 移入 slot 后才会解除；任何错误路径都会在 Drop 中撤销
/// token、终止并回收 child，不能把无监督 sidecar 留在系统中。
struct SpawnedSidecarGuard {
    process: Option<ManagedSidecar>,
}

impl SpawnedSidecarGuard {
    /// 用刚刚 spawn 的 child 构造已武装的清理守卫。
    fn new(process: ManagedSidecar) -> Self {
        Self {
            process: Some(process),
        }
    }

    /// 原子地把 child 挂回 slot；挂接失败时重新武装 guard 以便调用方返回时自动清理。
    fn attach(&mut self, slot: &Arc<ScopeSlot>, pid: u32) -> Result<(), SupervisorError> {
        let process = self
            .process
            .take()
            .expect("spawn cleanup guard 在 attach 前必须持有 child");
        match complete_slot_launch(slot, process, pid) {
            Ok(()) => Ok(()),
            Err((error, process)) => {
                self.process = Some(process);
                Err(error)
            }
        }
    }
}

impl Drop for SpawnedSidecarGuard {
    /// 挂接失败时以同步 kill + wait 回收 child；此时不能再把它交给任何 watcher。
    fn drop(&mut self) {
        if let Some(process) = self.process.take() {
            terminate_and_reap_detached_process(process);
        }
    }
}

/// 无法安全挂回 slot 的 child 必须同步撤销 token、强制终止并 wait 回收。
fn terminate_and_reap_detached_process(mut process: ManagedSidecar) {
    process
        .registry
        .invalidate_generation(&process.scope_id, process.generation);
    let _ = process.child.stdin.take();
    // 先杀完整 process group/job，再回收 leader；避免 sidecar 的孙进程继承 stdout 或存活。
    let group_kill_result = process.process_group.kill();
    let kill_result = process.child.kill();
    let wait_result = process.child.wait();
    if group_kill_result.is_err() || kill_result.is_err() || wait_result.is_err() {
        tracing::debug!(
            scope = %process.scope_id,
            generation = process.generation,
            process_group_kill_succeeded = group_kill_result.is_ok(),
            kill_succeeded = kill_result.is_ok(),
            wait_succeeded = wait_result.is_ok(),
            "未挂接 sidecar 的强制回收出现系统错误"
        );
    }
}

/// leader 已被 `try_wait` 或 `wait` 回收后，仍显式终止同组 descendant。
///
/// Unix `ProcessGroup` 的 Drop 不发送信号；所以不能因 leader 自然退出而直接丢弃最后一个
/// `Arc`。失败只记 debug：`ESRCH` 代表 group 已空，其他错误仍保留既有 stop/natural-exit
/// 状态机的完成语义。
fn kill_process_group_after_leader_exit(process: &ManagedSidecar) {
    if let Err(source) = process.process_group.kill() {
        tracing::debug!(
            scope = %process.scope_id,
            generation = process.generation,
            error = %source,
            "sidecar leader 已回收，但 process group 清理失败"
        );
    }
}

/// 真实 launch 后向未来 runtime 返回的非敏感进程描述。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SidecarProcessInfo {
    /// 所属 scope 标识。
    pub scope_id: String,
    /// OS child pid。
    pub pid: u32,
    /// 本次 sidecar process generation。
    pub generation: u64,
    /// 注册 token 时的 Channel revision。
    pub channel_revision: u64,
}

/// 只交给该 scope 唯一 IO actor 的 ACP 管道端点。
///
/// child 本身仍留在 Supervisor slot 中负责 token 失效与进程回收；actor 只独占 stdin
/// 和 stdout，不能取得 child、环境或 binding token。
pub(crate) struct SidecarStdio {
    pub stdin: ChildStdin,
    pub stdout: ChildStdout,
}

/// 一个 scope 唯一对应的进程所有权槽。
pub struct ScopeSlot {
    paths: ScopePaths,
    metadata: Mutex<ProcessSlotMetadata>,
    runtime: Mutex<ScopeSlotRuntime>,
}

impl ScopeSlot {
    /// 返回该 slot 的稳定 home/cwd；此操作不创建任何目录或锁文件。
    pub fn paths(&self) -> &ScopePaths {
        &self.paths
    }

    /// 返回 metadata 快照，避免调用方持有内部状态锁。
    pub fn metadata(&self) -> Result<ProcessSlotMetadata, SupervisorError> {
        self.metadata
            .lock()
            .map(|metadata| metadata.clone())
            .map_err(|_| SupervisorError::StateUnavailable)
    }
}

/// 同一 Host 进程内的 scope slot 注册表。
///
/// `Supervisor` 在 Task 7 提供一 scope 一 child 的真实启动顺序、固定路径和 platform
/// capability gate；ACP stdin/stdout 的协议 actor 仍由 Task 7b 接入。
pub struct Supervisor {
    config: HostRuntimeConfig,
    app_id: String,
    slots: Mutex<BTreeMap<String, Arc<ScopeSlot>>>,
    /// 每个已启动 sidecar tree 都在此登记；Supervisor 释放时可统一回收未完成的子树。
    process_scope: ProcessScope,
}

impl Supervisor {
    /// 从产品 App Data 根和 Host 强制校验的 app_id 构造 supervisor。
    pub fn new(
        config: HostRuntimeConfig,
        app_id: impl AsRef<str>,
    ) -> Result<Self, SupervisorError> {
        validate_absolute_path(&config.home_root)?;
        validate_absolute_path(&config.sidecar_log_path)?;
        let app_id = sanitize(app_id.as_ref())?;
        let home_root = canonicalize_existing_path_prefix(&config.home_root).map_err(|source| {
            SupervisorError::Io {
                operation: "解析 Host home_root",
                source,
            }
        })?;
        let sidecar_log_path =
            canonicalize_sidecar_log_path(&config.sidecar_log_path).map_err(|source| {
                SupervisorError::Io {
                    operation: "解析 sidecar 日志路径",
                    source,
                }
            })?;
        let mut config = config;
        // 只解析安全的现有前缀；非系统符号链接已拒绝，后续 app/scope 和日志尾部仍走严格检查。
        config.home_root = home_root;
        config.sidecar_log_path = sidecar_log_path;
        tracing::debug!("Host 注入的 home_root 与 sidecar 日志路径前缀已 canonicalize");

        Ok(Self {
            config,
            app_id,
            slots: Mutex::new(BTreeMap::new()),
            process_scope: ProcessScope::new(),
        })
    }

    /// 返回当前平台的 sidecar 监督能力。
    pub fn capability(&self) -> SupervisorCapability {
        capability()
    }

    /// 派生一个 scope 的稳定、绝对 home 与 workspace 路径。
    ///
    /// app_id 始终在这里由 Host 追加，调用方给出的 `home_root` 即使已经包含产品目录也
    /// 不会改变该规则。
    pub fn paths_for(&self, scope: impl AsRef<str>) -> Result<ScopePaths, SupervisorError> {
        let scope_id = sanitize(scope.as_ref())?;
        let scope_root = self.config.home_root.join(&self.app_id).join(scope_id);

        Ok(ScopePaths {
            home: scope_root.join("home"),
            workspace: scope_root.join("workspace"),
        })
    }

    /// 取得 scope 的唯一 process slot；同一 scope 的重复调用必定复用既有 `Arc`。
    ///
    /// Windows 在任何 map 写入或进程行为前 fail-closed。该方法仅维护内存 metadata，
    /// 因此不会访问 sidecar 唯一拥有的 `.efflab-sidecar.lock`。
    pub fn acquire(&self, scope: impl AsRef<str>) -> Result<Arc<ScopeSlot>, SupervisorError> {
        if let SupervisorCapability::Unavailable { reason } = self.capability() {
            return Err(SupervisorError::Unavailable { reason });
        }

        let scope_id = sanitize(scope.as_ref())?;
        let paths = self.paths_for(&scope_id)?;
        let mut slots = self
            .slots
            .lock()
            .map_err(|_| SupervisorError::StateUnavailable)?;

        Ok(slots
            .entry(scope_id.clone())
            .or_insert_with(|| {
                Arc::new(ScopeSlot {
                    paths,
                    metadata: Mutex::new(ProcessSlotMetadata {
                        scope_id,
                        pid: None,
                        generation: 1,
                        session_ids: BTreeSet::new(),
                        current_session: None,
                        state: ProcessSlotState::Idle,
                    }),
                    runtime: Mutex::new(ScopeSlotRuntime {
                        child: None,
                        has_started: false,
                        launching: false,
                        stopping: false,
                        restart_blocked: false,
                    }),
                })
            })
            .clone())
    }

    /// 执行 Task 7 的真实 launch 顺序：L3b 已监听 → 注册 token → 写权威 TOML → spawn。
    pub fn launch_sidecar(
        &self,
        scope: &str,
        loopback: &L3bLoopback,
        channel: &LlmChannelManager,
        approved_mcp: &ApprovedMcpSpecV1,
    ) -> Result<SidecarProcessInfo, SupervisorError> {
        if let SupervisorCapability::Unavailable { reason } = self.capability() {
            return Err(SupervisorError::Unavailable { reason });
        }
        let (model_id, channel_revision) = channel
            .sidecar_model()
            .map_err(|_| SupervisorError::LlmChannelUnavailable)?;
        let scope_id = sanitize(scope)?;
        let slot = self.acquire(&scope_id)?;
        let (paths, generation) = prepare_slot_launch(&slot)?;

        // L3b 已经由 service 启动；先注册本代 token，后续任何失败都会立即使它失效。
        let token = match loopback.register_binding(&scope_id, generation, channel_revision) {
            Ok(token) => token,
            Err(_) => {
                clear_launching(&slot)?;
                return Err(SupervisorError::LlmChannelUnavailable);
            }
        };
        let registry = loopback.registry();
        let result = self.spawn_registered_sidecar(
            &slot,
            &scope_id,
            generation,
            channel_revision,
            &model_id,
            approved_mcp,
            loopback,
            token.clone(),
            Arc::clone(&registry),
            paths,
        );
        if result.is_err() {
            registry.invalidate_generation(&scope_id, generation);
            let _ = clear_launching(&slot);
        }
        result
    }

    /// 返回真实存活的 scope；自然退出 child 会在此处同步失效其 binding token。
    pub fn live_scope_ids(&self) -> Result<Vec<String>, SupervisorError> {
        let slots: Vec<(String, Arc<ScopeSlot>)> = self
            .slots
            .lock()
            .map_err(|_| SupervisorError::StateUnavailable)?
            .iter()
            .map(|(scope_id, slot)| (scope_id.clone(), Arc::clone(slot)))
            .collect();
        let mut live = Vec::new();
        for (scope_id, slot) in slots {
            if slot_is_live(&slot)? {
                live.push(scope_id);
            }
        }
        Ok(live)
    }

    /// drain 所有已存活 scope 后按当前 Channel revision 再启动；尽力覆盖所有 scope。
    pub fn restart_live_scopes<F>(
        &self,
        loopback: &L3bLoopback,
        channel: &LlmChannelManager,
        mut mcp_for_scope: F,
    ) -> Result<(), SupervisorError>
    where
        F: FnMut(&str) -> Result<ApprovedMcpSpecV1, SupervisorError>,
    {
        let scopes = self.live_scope_ids()?;
        let mut failed = false;
        for scope in &scopes {
            if let Err(error) = self.stop_scope(scope) {
                tracing::error!(scope = %scope, error = %error, "sidecar 停止失败");
                failed = true;
            }
        }
        for scope in &scopes {
            let approved_mcp = match mcp_for_scope(scope) {
                Ok(spec) => spec,
                Err(error) => {
                    tracing::error!(scope = %scope, error = %error, "sidecar 重启缺少 MCP 批准规格");
                    failed = true;
                    continue;
                }
            };
            if let Err(error) = self.launch_sidecar(scope, loopback, channel, &approved_mcp) {
                tracing::error!(scope = %scope, error = %error, "sidecar 重启启动失败");
                failed = true;
            }
        }
        if failed {
            Err(SupervisorError::RestartFailed)
        } else {
            Ok(())
        }
    }

    /// 关闭一 scope child 并立即使其 binding token 失效；不触碰 sidecar 的 home lock。
    pub fn stop_scope(&self, scope: &str) -> Result<(), SupervisorError> {
        let scope_id = sanitize(scope)?;
        let slot = self
            .slots
            .lock()
            .map_err(|_| SupervisorError::StateUnavailable)?
            .get(&scope_id)
            .cloned();
        let Some(slot) = slot else {
            return Ok(());
        };
        stop_slot(&slot)
    }

    /// 在已注册 token 后渲染/原子写 TOML，并以受控环境启动 sidecar。
    #[allow(clippy::too_many_arguments)]
    fn spawn_registered_sidecar(
        &self,
        slot: &Arc<ScopeSlot>,
        scope_id: &str,
        generation: u64,
        channel_revision: u64,
        model_id: &str,
        approved_mcp: &ApprovedMcpSpecV1,
        loopback: &L3bLoopback,
        token: BindingToken,
        registry: Arc<BindingTokenRegistry>,
        paths: ScopePaths,
    ) -> Result<SidecarProcessInfo, SupervisorError> {
        prepare_scope_directories(&paths)?;
        let rendered = render_runtime_config(&paths, model_id, loopback, approved_mcp)?;
        let runtime_config_path = paths.home.join(RUNTIME_CONFIG_FILENAME);
        write_authoritative_config(&runtime_config_path, rendered.as_bytes())?;

        // 仅此处把 binding token 注入 child；用户 Key 从不在 env、CLI 或 TOML 中出现。
        let environment = ChildEnvironment::for_sidecar_with_binding(&paths.home, &token)?;
        let mut log_file = open_sidecar_log_file(&self.config.sidecar_log_path)?;
        let spawned_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or(0);
        writeln!(
            log_file,
            "--- sidecar spawn scope={scope_id} generation={generation} unix={spawned_at} ---"
        )
        .map_err(|source| SupervisorError::Io {
            operation: "写入 sidecar 日志头",
            source,
        })?;
        let stderr = Stdio::from(log_file.try_clone().map_err(|source| SupervisorError::Io {
            operation: "复制 sidecar 日志句柄",
            source,
        })?);
        let mut command = Command::new(&self.config.sidecar_bin);
        command
            .arg("--runtime-config")
            .arg(&runtime_config_path)
            .arg("--home")
            .arg(&paths.home)
            .arg("--session-cwd")
            .arg(&paths.workspace)
            .arg("--stdio")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(stderr);
        environment.apply(&mut command);
        let (child, process_group) = match spawn_enrolled_sidecar(&mut command, &self.process_scope)
        {
            Ok(spawned) => spawned,
            Err(error) => {
                let _ = writeln!(log_file, "--- sidecar spawn failed: {error} ---");
                let _ = log_file.flush();
                return Err(error);
            }
        };
        let pid = child.id();
        let _ = writeln!(log_file, "--- sidecar pid={pid} ---");
        let _ = log_file.flush();
        drop(log_file);
        // 在 slot 挂接前始终由 guard 独占 child；complete_slot_launch 任何失败均会触发
        // 同步 token 撤销、kill 与 wait，不能把已启动进程遗留为无监督 child。
        let mut child_guard = SpawnedSidecarGuard::new(ManagedSidecar {
            child,
            process_group,
            registry,
            scope_id: scope_id.to_string(),
            generation,
        });
        child_guard.attach(slot, pid)?;
        // watcher 只观察这一 generation；child 自然退出后 token 无需等下一次 dispatch 即失效。
        watch_sidecar_exit(Arc::clone(slot), generation);
        tracing::debug!(
            scope = %scope_id,
            pid,
            generation,
            channel_revision,
            "sidecar 已在 L3b/config 注册完成后启动"
        );
        Ok(SidecarProcessInfo {
            scope_id: scope_id.to_string(),
            pid,
            generation,
            channel_revision,
        })
    }

    /// 把刚启动 child 的 ACP stdio 仅移交给该 scope 的唯一 IO actor。
    ///
    /// Supervisor 保留 child 所有权以负责 token 与生命周期；重复移交、旧 generation
    /// 或正在停止的 slot 一律 fail-closed，避免两个 actor 竞争同一 stdin/stdout。
    pub(crate) fn take_stdio(
        &self,
        scope: &str,
        generation: u64,
    ) -> Result<SidecarStdio, SupervisorError> {
        let scope_id = sanitize(scope)?;
        let slot = self
            .slots
            .lock()
            .map_err(|_| SupervisorError::StateUnavailable)?
            .get(&scope_id)
            .cloned()
            .ok_or(SupervisorError::StdioUnavailable)?;
        let mut runtime = slot
            .runtime
            .lock()
            .map_err(|_| SupervisorError::StateUnavailable)?;
        if runtime.stopping || runtime.launching {
            return Err(SupervisorError::StdioUnavailable);
        }
        let process = runtime
            .child
            .as_mut()
            .filter(|process| process.generation == generation)
            .ok_or(SupervisorError::StdioUnavailable)?;
        // 同一 runtime 锁内先检查两端，之后没有其它路径可抢走任一 pipe。
        if process.child.stdin.is_none() || process.child.stdout.is_none() {
            return Err(SupervisorError::StdioUnavailable);
        }
        Ok(SidecarStdio {
            stdin: process
                .child
                .stdin
                .take()
                .expect("已检查 sidecar stdin 必须存在"),
            stdout: process
                .child
                .stdout
                .take()
                .expect("已检查 sidecar stdout 必须存在"),
        })
    }
}

/// 用标准库 child 建立受控 process tree，并立即纳入 [`ProcessScope`] 的关闭边界。
///
/// `AcpRuntime` 需要同步 `std::process` 管道，不能直接使用 `ProcessScope::spawn` 的
/// Tokio child。因此唯一的 raw `Command::spawn` 被局限在这里：spawn 前创建独立
/// process group/job，spawn 后立刻把 group 注册到 scope。没有采用 Linux pdeathsig，
/// 因为它绑定的是任意产品 dispatch 线程而非整个 Host 进程；正常关闭由 scope/group
/// 完成，父进程异常退出则会关闭 stdio，sidecar 必须按 ACP EOF 退出。
fn spawn_enrolled_sidecar(
    command: &mut Command,
    process_scope: &ProcessScope,
) -> Result<(Child, Arc<ProcessGroup>), SupervisorError> {
    // Unix `setsid` / Windows CREATE_NO_WINDOW 令 ProcessGroup 只覆盖这棵 sidecar tree。
    detach_std_command(command);
    #[allow(clippy::disallowed_methods)]
    // 这是 std child 到 ProcessScope 的受控桥接；下一步必定 attach_std + register，
    // 所以 child 不会以未登记状态离开本函数。
    let mut child = command.spawn().map_err(|source| SupervisorError::Io {
        operation: "spawn",
        source,
    })?;

    let process_group = match ProcessGroup::new().and_then(|mut group| {
        group.attach_std(&child)?;
        Ok(Arc::new(group))
    }) {
        Ok(group) => group,
        Err(source) => {
            // group enrollment 失败后不可让刚启动的 child 脱离监督。
            let _ = child.kill();
            let _ = child.wait();
            return Err(SupervisorError::Io {
                operation: "绑定 sidecar 进程组",
                source,
            });
        }
    };

    if !process_scope.register(&process_group) {
        // closed scope 已对 group 发出 kill；仍要 wait leader，避免留下 zombie。
        let _ = process_group.kill();
        let _ = child.kill();
        let _ = child.wait();
        return Err(SupervisorError::Io {
            operation: "注册 sidecar 进程作用域",
            source: io::Error::other("process scope already closed"),
        });
    }

    Ok((child, process_group))
}

impl Drop for Supervisor {
    /// Host 释放时回收仍由本进程持有的 child，避免遗留获得过 binding token 的 sidecar。
    fn drop(&mut self) {
        // 析构路径不应在每个 scope 的 EOF 宽限期中拖延；先由 scope 杀掉完整子树，
        // 后续 stop 只负责撤销 token、回收 leader 与整理 metadata。
        self.process_scope.kill_all();
        let slots = self
            .slots
            .lock()
            .map(|slots| slots.values().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        for slot in slots {
            if stop_slot(&slot).is_err() {
                // Supervisor 即将释放 slot，不能让失败路径保留的 child 随 Child::drop 脱离监督。
                force_reap_slot_on_supervisor_drop(&slot);
            }
        }
    }
}

/// 把 scope slot 置为独占 launching，并为本次 child 预留单调 generation。
fn prepare_slot_launch(slot: &Arc<ScopeSlot>) -> Result<(ScopePaths, u64), SupervisorError> {
    let mut runtime = slot
        .runtime
        .lock()
        .map_err(|_| SupervisorError::StateUnavailable)?;
    if runtime.launching || runtime.stopping {
        return Err(SupervisorError::ScopeAlreadyRunning);
    }
    if let Some(process) = runtime.child.as_mut() {
        match process.child.try_wait() {
            Ok(None) => return Err(SupervisorError::ScopeAlreadyRunning),
            Ok(Some(_)) => {
                let process = runtime.child.take().expect("已检查 child 必定存在");
                process
                    .registry
                    .invalidate_generation(&process.scope_id, process.generation);
                // 已确认旧 child 自然退出后才允许解除此前 stop 失败留下的重启隔离。
                runtime.restart_blocked = false;
            }
            Err(source) => {
                return Err(SupervisorError::Io {
                    operation: "检查 child 状态",
                    source,
                });
            }
        }
    }
    if runtime.restart_blocked {
        return Err(SupervisorError::ScopeAlreadyRunning);
    }

    let mut metadata = slot
        .metadata
        .lock()
        .map_err(|_| SupervisorError::StateUnavailable)?;
    if runtime.has_started {
        metadata.generation = metadata.generation.saturating_add(1).max(1);
    } else {
        runtime.has_started = true;
    }
    metadata.pid = None;
    metadata.state = ProcessSlotState::Idle;
    runtime.launching = true;
    Ok((slot.paths.clone(), metadata.generation))
}

/// 失败路径解除 launching 标志，保留 generation 消耗以确保旧 token 无法复活。
fn clear_launching(slot: &Arc<ScopeSlot>) -> Result<(), SupervisorError> {
    let mut runtime = slot
        .runtime
        .lock()
        .map_err(|_| SupervisorError::StateUnavailable)?;
    runtime.launching = false;
    Ok(())
}

/// 完成 spawn 后把 child、token 与 metadata 一次性挂回 scope slot。
///
/// 锁或状态检查失败时把 process 原样返还给 RAII guard，确保 caller 可同步终止并回收它。
fn complete_slot_launch(
    slot: &Arc<ScopeSlot>,
    process: ManagedSidecar,
    pid: u32,
) -> Result<(), (SupervisorError, ManagedSidecar)> {
    let generation = process.generation;
    let mut runtime = match slot.runtime.lock() {
        Ok(runtime) => runtime,
        Err(_) => return Err((SupervisorError::StateUnavailable, process)),
    };
    if !runtime.launching || runtime.stopping || runtime.restart_blocked || runtime.child.is_some()
    {
        return Err((SupervisorError::ScopeAlreadyRunning, process));
    }
    // 先取得两个锁，之后的字段写入不再会产生可返回错误，避免 process 已移动后丢失。
    let mut metadata = match slot.metadata.lock() {
        Ok(metadata) => metadata,
        Err(_) => return Err((SupervisorError::StateUnavailable, process)),
    };
    runtime.child = Some(process);
    runtime.launching = false;
    runtime.stopping = false;
    runtime.restart_blocked = false;
    metadata.pid = Some(pid);
    metadata.generation = generation;
    metadata.state = ProcessSlotState::Idle;
    Ok(())
}

/// 查询 slot 是否仍有运行中的 child；退出时同步回收 token 和 pid metadata。
fn slot_is_live(slot: &Arc<ScopeSlot>) -> Result<bool, SupervisorError> {
    let generation = {
        let runtime = slot
            .runtime
            .lock()
            .map_err(|_| SupervisorError::StateUnavailable)?;
        runtime.child.as_ref().map(|process| process.generation)
    };
    match generation {
        Some(generation) => slot_generation_is_live(slot, generation),
        None => Ok(false),
    }
}

/// 只观察特定 generation，避免旧 child watcher 误管理已经重启出的新进程。
fn slot_generation_is_live(
    slot: &Arc<ScopeSlot>,
    generation: u64,
) -> Result<bool, SupervisorError> {
    let mut runtime = slot
        .runtime
        .lock()
        .map_err(|_| SupervisorError::StateUnavailable)?;
    let Some(process) = runtime.child.as_mut() else {
        // stop 入口临时持有 child 时，watcher 不能据此退出；否则 kill 失败恢复 child 后会
        // 失去自然退出监督。
        return Ok(runtime.stopping);
    };
    if process.generation != generation {
        return Ok(false);
    }
    match process.child.try_wait() {
        Ok(None) => Ok(true),
        Ok(Some(_)) => {
            let process = runtime.child.take().expect("已检查 child 必定存在");
            process
                .registry
                .invalidate_generation(&process.scope_id, process.generation);
            // leader 已被 try_wait 回收；在丢弃最后一个 group Arc 前必须终止仍存活的 descendant。
            kill_process_group_after_leader_exit(&process);
            runtime.stopping = false;
            runtime.restart_blocked = false;
            let mut metadata = slot
                .metadata
                .lock()
                .map_err(|_| SupervisorError::StateUnavailable)?;
            metadata.pid = None;
            metadata.state = ProcessSlotState::Idle;
            Ok(false)
        }
        Err(source) => Err(SupervisorError::Io {
            operation: "检查 child 状态",
            source,
        }),
    }
}

/// 子进程自然退出时尽快使 binding token 失效，不等待下一个产品 dispatch。
fn watch_sidecar_exit(slot: Arc<ScopeSlot>, generation: u64) {
    let _ = thread::Builder::new()
        .name("efflab-sidecar-exit-watch".to_string())
        .spawn(move || {
            loop {
                match slot_generation_is_live(&slot, generation) {
                    Ok(true) => thread::sleep(Duration::from_millis(50)),
                    Ok(false) | Err(_) => break,
                }
            }
        });
}

/// 关闭 child stdin 后给正常 EOF 固定宽限期，再升级为强制 kill。
fn stop_slot(slot: &Arc<ScopeSlot>) -> Result<(), SupervisorError> {
    stop_slot_with_kill(slot, STDIN_CLOSE_GRACE, |child| child.kill())
}

/// 执行 stop 状态机；测试可注入 kill 错误，以锁定 token/child 所有权的 fail-closed 语义。
fn stop_slot_with_kill(
    slot: &Arc<ScopeSlot>,
    eof_grace: Duration,
    kill: impl FnOnce(&mut Child) -> io::Result<()>,
) -> Result<(), SupervisorError> {
    let process = {
        let mut runtime = slot
            .runtime
            .lock()
            .map_err(|_| SupervisorError::StateUnavailable)?;
        if runtime.stopping {
            return Err(SupervisorError::ScopeAlreadyRunning);
        }
        runtime.launching = false;
        let process = runtime.child.take();
        if process.is_some() {
            // 从这里起到确认 wait 成功前，任何 launch 都必须被阻塞。
            runtime.stopping = true;
            runtime.restart_blocked = true;
        } else if !runtime.restart_blocked {
            runtime.stopping = false;
        }
        process
    };
    let Some(mut process) = process else {
        return if slot
            .runtime
            .lock()
            .map_err(|_| SupervisorError::StateUnavailable)?
            .restart_blocked
        {
            Err(SupervisorError::ScopeAlreadyRunning)
        } else {
            Ok(())
        };
    };

    // 先撤销认证能力，再做任何可能失败或阻塞的 child 操作；旧 sidecar 即使仍活着也不能出站。
    process
        .registry
        .invalidate_generation(&process.scope_id, process.generation);
    if let Err(error) = mark_slot_killing(slot) {
        restore_stopping_process(slot, process);
        return Err(error);
    }

    // Task 7b 会先发送 session/cancel；Task 7 尚无 ACP actor，因此只能先关闭 stdin。
    let _ = process.child.stdin.take();
    let deadline = Instant::now() + eof_grace;
    let mut exited = false;
    while Instant::now() < deadline {
        match process.child.try_wait() {
            Ok(Some(_)) => {
                exited = true;
                break;
            }
            Ok(None) => thread::sleep(Duration::from_millis(20)),
            Err(source) => {
                let error = SupervisorError::Io {
                    operation: "等待 child EOF 退出",
                    source,
                };
                restore_stopping_process(slot, process);
                return Err(error);
            }
        }
    }
    if !exited {
        if let Err(source) = kill(&mut process.child) {
            let error = SupervisorError::Io {
                operation: "终止 child",
                source,
            };
            // kill 失败时绝不能丢 child；保留所有权和 restart_blocked，等待后续显式恢复。
            restore_stopping_process(slot, process);
            return Err(error);
        }
        // leader 已收到 kill 但尚未 wait/reap，pgid 仍不可复用；此时再杀完整 tree。
        if let Err(source) = process.process_group.kill() {
            tracing::debug!(
                scope = %process.scope_id,
                generation = process.generation,
                error = %source,
                "sidecar leader 已终止，但 process group 清理失败"
            );
        }
        if let Err(source) = process.child.wait() {
            let error = SupervisorError::Io {
                operation: "回收 child",
                source,
            };
            // kill 已发出但未确认 reaped 时同样不能创建新代，直到后续检查确认退出。
            restore_stopping_process(slot, process);
            return Err(error);
        }
    } else {
        // 关闭 stdin 后 leader 正常 EOF 已由 try_wait 回收；仍要清理同组 descendant。
        kill_process_group_after_leader_exit(&process);
    }
    // `try_wait` 或 `wait` 已确认退出；现在才允许清除 pid 并解除 restart 隔离。
    finish_stopped_slot(slot)
}

/// 将 stop 中的 slot 标成 Killing；pid 保留到 child 已被确认退出。
fn mark_slot_killing(slot: &Arc<ScopeSlot>) -> Result<(), SupervisorError> {
    let mut metadata = slot
        .metadata
        .lock()
        .map_err(|_| SupervisorError::StateUnavailable)?;
    metadata.state = ProcessSlotState::Killing;
    Ok(())
}

/// stop 失败时恢复 child 所有权；若锁已不可用则立即同步回收，宁可失败也不能遗留孤儿。
fn restore_stopping_process(slot: &Arc<ScopeSlot>, process: ManagedSidecar) {
    match slot.runtime.lock() {
        Ok(mut runtime) if runtime.child.is_none() => {
            runtime.child = Some(process);
            runtime.launching = false;
            runtime.stopping = false;
            runtime.restart_blocked = true;
        }
        Ok(_) | Err(_) => terminate_and_reap_detached_process(process),
    }
}

/// child 已确认退出后清理 slot 的运行与可观察 metadata。
fn finish_stopped_slot(slot: &Arc<ScopeSlot>) -> Result<(), SupervisorError> {
    let mut runtime = slot
        .runtime
        .lock()
        .map_err(|_| SupervisorError::StateUnavailable)?;
    runtime.launching = false;
    runtime.stopping = false;
    runtime.restart_blocked = false;
    let mut metadata = slot
        .metadata
        .lock()
        .map_err(|_| SupervisorError::StateUnavailable)?;
    metadata.pid = None;
    metadata.state = ProcessSlotState::Idle;
    Ok(())
}

/// Supervisor Drop 无法向上返回 stop 错误时，仍必须尽力终止被失败路径保留的 child。
fn force_reap_slot_on_supervisor_drop(slot: &Arc<ScopeSlot>) {
    let mut runtime = match slot.runtime.lock() {
        Ok(runtime) => runtime,
        // Drop 的目标是防孤儿；即使先前 panic 使锁中毒，也要取得内部 child 并同步回收。
        Err(poisoned) => poisoned.into_inner(),
    };
    runtime.launching = false;
    runtime.stopping = false;
    runtime.restart_blocked = true;
    if let Some(process) = runtime.child.take() {
        drop(runtime);
        terminate_and_reap_detached_process(process);
    }
}

/// 用本代批准 MCP 规格构造 sidecar 唯一可读的 v1 runtime 配置。
fn render_runtime_config(
    paths: &ScopePaths,
    model_id: &str,
    loopback: &L3bLoopback,
    approved_mcp: &ApprovedMcpSpecV1,
) -> Result<String, SupervisorError> {
    let session_cwd = paths
        .workspace
        .to_str()
        .ok_or(SupervisorError::ConfigRenderFailed)?
        .to_owned();
    let config = RuntimeConfigV1 {
        schema_version: RUNTIME_SCHEMA_VERSION,
        runtime_revision: String::new(),
        session_store_version: RUNTIME_SESSION_STORE_VERSION,
        session_cwd,
        model: LoopbackModelSpec {
            model_id: model_id.to_owned(),
            base_url: loopback.sidecar_base_url(),
            backend: RUNTIME_BACKEND.to_owned(),
            token_env: RUNTIME_TOKEN_ENV.to_owned(),
        },
        approved_mcp: approved_mcp.servers().clone(),
        expected_tools: approved_mcp.expected_tools().clone(),
    };
    render_runtime_config_v1(&config).map_err(|_| {
        tracing::error!("RuntimeConfigV1 渲染失败，拒绝启动 sidecar");
        SupervisorError::ConfigRenderFailed
    })
}

/// Host 在 renderer 写盘前逐级创建自己的隔离目录，并拒绝祖先符号链接。
fn prepare_scope_directories(paths: &ScopePaths) -> Result<(), SupervisorError> {
    for directory in [&paths.home, &paths.workspace] {
        ensure_host_owned_directory(directory).map_err(|_| SupervisorError::ConfigWriteFailed)?;
        let metadata =
            fs::symlink_metadata(directory).map_err(|_| SupervisorError::ConfigWriteFailed)?;
        ensure_plain_directory(&metadata).map_err(|_| SupervisorError::ConfigWriteFailed)?;
        set_private_directory_mode(directory).map_err(|_| SupervisorError::ConfigWriteFailed)?;
    }
    Ok(())
}

/// 逐级检查并创建目录，避免 `create_dir_all` 沿祖先符号链接写到 Host 范围外。
///
/// 该检查只使用标准库元数据与单级 `create_dir`，因此不依赖某一平台的 no-follow API。
/// 检查和后续打开之间仍存在无法由当前抽象消除的 TOCTOU 窗口，调用方必须继续采用
/// fail-closed 和同目录原子替换策略。
fn ensure_host_owned_directory(path: &Path) -> io::Result<()> {
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) => ensure_plain_directory(&metadata)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                match fs::create_dir(&current) {
                    Ok(()) => {}
                    // 另一个创建者可能刚刚完成同一级目录；仍必须重新检查其类型。
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(error) => return Err(error),
                }
                let metadata = fs::symlink_metadata(&current)?;
                ensure_plain_directory(&metadata)?;
                set_private_directory_mode(&current)?;
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

/// 目录链中的每一级都必须是普通目录；符号链接和普通文件一律拒绝。
fn ensure_plain_directory(metadata: &fs::Metadata) -> io::Result<()> {
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(io::Error::other("Host 目录链必须由普通目录组成"));
    }
    Ok(())
}

/// 收紧 Host 新建/管理目录的 Unix 权限；Windows capability 未启用时不依赖此 API。
fn set_private_directory_mode(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

/// 原子物化 contract renderer 的完整 TOML；Host 是该文件的唯一写盘 owner。
fn write_authoritative_config(path: &Path, content: &[u8]) -> Result<(), SupervisorError> {
    let parent = path.parent().ok_or(SupervisorError::ConfigWriteFailed)?;
    ensure_host_owned_directory(parent).map_err(|_| SupervisorError::ConfigWriteFailed)?;
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err(SupervisorError::ConfigWriteFailed);
        }
        Ok(_) => {}
        // 只有不存在旧配置时才能继续创建；权限、I/O 等其它元数据错误必须 fail-closed。
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(_) => return Err(SupervisorError::ConfigWriteFailed),
    }
    let file_name = path
        .file_name()
        .and_then(OsStr::to_str)
        .ok_or(SupervisorError::ConfigWriteFailed)?;
    let temporary = parent.join(format!(".{file_name}.{}.tmp", std::process::id()));
    let _ = fs::remove_file(&temporary);
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&temporary)
        .map_err(|_| SupervisorError::ConfigWriteFailed)?;
    let write_result = (|| -> Result<(), SupervisorError> {
        file.write_all(content)
            .map_err(|_| SupervisorError::ConfigWriteFailed)?;
        file.sync_all()
            .map_err(|_| SupervisorError::ConfigWriteFailed)?;
        fs::rename(&temporary, path).map_err(|_| SupervisorError::ConfigWriteFailed)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(0o600))
                .map_err(|_| SupervisorError::ConfigWriteFailed)?;
        }
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| SupervisorError::ConfigWriteFailed)?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    write_result
}

/// 以追加方式打开 sidecar 独立日志文件；父目录不存在时创建。
///
/// 该文件只接收 sidecar stderr（tracing / 启动 eprintln），不得占用 ACP stdout。
fn open_sidecar_log_file(path: &Path) -> Result<File, SupervisorError> {
    validate_absolute_path(path)?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or(SupervisorError::InvalidPathComponent)?;
    ensure_host_owned_directory(parent).map_err(|source| SupervisorError::Io {
        operation: "创建 sidecar 日志目录",
        source,
    })?;
    set_private_directory_mode(parent).map_err(|source| SupervisorError::Io {
        operation: "收紧 sidecar 日志目录权限",
        source,
    })?;
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err(SupervisorError::Io {
                operation: "打开 sidecar 日志文件",
                source: io::Error::other("sidecar 日志路径必须是普通文件"),
            });
        }
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(SupervisorError::Io {
                operation: "读取 sidecar 日志元数据",
                source,
            });
        }
    }
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(path).map_err(|source| SupervisorError::Io {
        operation: "打开 sidecar 日志文件",
        source,
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(|source| {
            SupervisorError::Io {
                operation: "收紧 sidecar 日志权限",
                source,
            }
        })?;
    }
    Ok(file)
}

/// 返回当前编译目标的 sidecar 监督能力，供尚未持有 `Supervisor` 的调用方查询。
pub fn capability() -> SupervisorCapability {
    #[cfg(windows)]
    {
        SupervisorCapability::Unavailable {
            reason: UnavailableReason::SidecarHardeningUnavailable,
        }
    }

    #[cfg(not(windows))]
    {
        SupervisorCapability::Available
    }
}

/// 子进程环境的白名单表示；应用时始终先执行 `Command::env_clear()`。
///
/// `GROK_HOME` 只允许由 Host 提供的绝对私有 home 注入。Task 5 刻意没有
/// `EFFLAB_L3B_BIND`，也不会继承用户 Key 或 chat mode。
pub struct ChildEnvironment {
    variables: BTreeMap<OsString, OsString>,
}

impl ChildEnvironment {
    /// 从 Host 提供的私有 home 与显式平台白名单构造 child env。
    ///
    /// 调用方传入 `GROK_HOME` 也会被拒绝，防止覆盖 Host 派生的 scope 私有 home。
    /// 变量值若以 `sk-` 开头则视为用户 Key 形态并 fail-closed，错误中不保留该值。
    pub fn from_whitelist(
        grok_home: &Path,
        variables: impl IntoIterator<Item = (String, OsString)>,
    ) -> Result<Self, SupervisorError> {
        validate_absolute_path(grok_home)?;

        let mut retained = BTreeMap::new();
        retained.insert(
            OsString::from("GROK_HOME"),
            grok_home.as_os_str().to_os_string(),
        );

        for (name, value) in variables {
            if is_forbidden_environment_variable(&name) || name == "GROK_HOME" {
                return Err(SupervisorError::EnvironmentVariableNotAllowed { name });
            }
            if !is_platform_environment_variable(&name) {
                return Err(SupervisorError::EnvironmentVariableNotWhitelisted { name });
            }
            if resembles_user_key(&value) {
                return Err(SupervisorError::EnvironmentValueNotAllowed { name });
            }

            retained.insert(OsString::from(name), value);
        }

        Ok(Self {
            variables: retained,
        })
    }

    /// 从当前进程只读取平台运行时需要的变量，再以相同白名单规则构造 child env。
    ///
    /// 未登记的父进程变量不会读取，更不会在 `apply` 时继承给 sidecar。
    pub fn for_sidecar(grok_home: &Path) -> Result<Self, SupervisorError> {
        let inherited = platform_environment_allowlist()
            .iter()
            .filter_map(|name| env::var_os(name).map(|value| ((*name).to_owned(), value)));
        Self::from_whitelist(grok_home, inherited)
    }

    /// 在完整 launch 路径清空环境，仅保留平台运行时变量与本代 binding token。
    ///
    /// `--home` 已由 CLI 显式传递，因而不保留 `GROK_HOME`；用户 Key、XAI Key、代理
    /// 变量和未登记变量均不可进入 child。binding 参数只能来自 registry 生成的 token。
    pub fn for_sidecar_with_binding(
        grok_home: &Path,
        binding_token: &BindingToken,
    ) -> Result<Self, SupervisorError> {
        // 保留路径形状校验，确保该完整启动入口仍只接受 Host 派生的私有 home。
        validate_absolute_path(grok_home)?;
        let mut variables = platform_environment_values()?;
        variables.insert(
            OsString::from("EFFLAB_L3B_BIND"),
            OsString::from(binding_token.as_bearer()),
        );
        Ok(Self { variables })
    }

    /// 读取已批准变量的值；该 API 不提供整个 map，避免调用方意外记录完整环境。
    pub fn get(&self, name: &str) -> Option<&OsStr> {
        self.variables
            .iter()
            .find(|(key, _)| key.as_os_str() == OsStr::new(name))
            .map(|(_, value)| value.as_os_str())
    }

    /// 先完全清除父环境，再只写入已批准的白名单。
    pub fn apply(&self, command: &mut Command) {
        command.env_clear();
        for (name, value) in &self.variables {
            command.env(name, value);
        }
    }
}

/// 生命周期所需的 child 进程控制面。
///
/// 真实 spawn 接线属于 Task 7；该 trait 先冻结 cancel、stdin、等待、终止和 kill 的
/// 跨平台语义。Windows 实现必须映射到等价的 Job Object/TerminateProcess 行为，而不
/// 能因 `cfg(windows)` 缺失该 API。
pub trait ChildLifecycleOps: Send {
    /// in-flight prompt 时向 sidecar 发送 `session/cancel` 通知。
    fn cancel_in_flight(&mut self) -> Result<(), SupervisorError>;
    /// 关闭 sidecar stdin，使其先走协议规定的正常 EOF 退出。
    fn close_stdin(&mut self) -> Result<(), SupervisorError>;
    /// 等待 child 在给定宽限期内退出；`true` 表示已退出。
    fn wait_for_exit(&mut self, timeout: Duration) -> Result<bool, SupervisorError>;
    /// 请求第一阶段温和终止；Unix 为 TERM，Windows 为平台等价终止请求。
    fn terminate(&mut self) -> Result<(), SupervisorError>;
    /// 请求最终强制终止；该 API 在所有目标（含 Windows）必须存在。
    fn kill(&mut self) -> Result<(), SupervisorError>;
}

/// 将固定的 sidecar Drop 顺序封装为可在 Task 7 接入真实 child 的生命周期对象。
pub struct ChildLifecycle {
    child: Box<dyn ChildLifecycleOps>,
    in_flight: bool,
    shutdown_started: bool,
}

impl ChildLifecycle {
    /// 用 child 控制面与当前 prompt 状态构造生命周期所有者。
    pub fn new(child: Box<dyn ChildLifecycleOps>, in_flight: bool) -> Self {
        Self {
            child,
            in_flight,
            shutdown_started: false,
        }
    }

    /// 按 cancel（仅 in-flight）→ stdin 3.5s → TERM 2s → KILL 执行关闭。
    ///
    /// 任一阶段失败仍会尽力继续后续阶段，以避免因报告一个 I/O 错误而遗留 child。
    /// 手动调用可获得最先发生的错误；Drop 会执行相同清理但不能传播错误。
    pub fn shutdown(&mut self) -> Result<(), SupervisorError> {
        if self.shutdown_started {
            return Ok(());
        }
        self.shutdown_started = true;

        let mut first_error = None;
        if self.in_flight {
            remember_error(&mut first_error, self.child.cancel_in_flight());
        }
        remember_error(&mut first_error, self.child.close_stdin());

        let exited_after_stdin =
            wait_or_record_error(&mut *self.child, STDIN_CLOSE_GRACE, &mut first_error);
        if exited_after_stdin {
            return finish_lifecycle(first_error);
        }

        remember_error(&mut first_error, self.child.terminate());
        let exited_after_term =
            wait_or_record_error(&mut *self.child, TERMINATE_GRACE, &mut first_error);
        if !exited_after_term {
            remember_error(&mut first_error, self.child.kill());
        }

        finish_lifecycle(first_error)
    }
}

impl Drop for ChildLifecycle {
    /// 进程所有者释放时也必须完成同一关闭序列，避免 child 脱离 Host 生命周期。
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

/// 检查传入根路径是否绝对且不含会改变稳定 join 语义的父目录组件。
fn validate_absolute_path(path: &Path) -> Result<(), SupervisorError> {
    if !path.is_absolute() {
        return Err(SupervisorError::HomeRootMustBeAbsolute);
    }
    if path
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(SupervisorError::HomeRootContainsParentDirectory);
    }
    Ok(())
}

/// 将路径中最近的现有前缀解析为物理路径，并保留尚不存在的尾部组件。
///
/// 现有前缀只允许明确的 macOS 系统别名；Host 或产品目录的符号链接一律拒绝。
fn canonicalize_existing_path_prefix(path: &Path) -> io::Result<PathBuf> {
    let mut existing = path.to_path_buf();
    let mut missing = Vec::<OsString>::new();

    loop {
        match fs::symlink_metadata(&existing) {
            Ok(_) => {
                reject_unexpected_symlink_components(&existing)?;
                let mut canonical = fs::canonicalize(&existing)?;
                for component in missing.iter().rev() {
                    canonical.push(component);
                }
                return Ok(canonical);
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let component = existing.file_name().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, "路径没有可回溯的现有前缀")
                })?;
                missing.push(component.to_os_string());
                if !existing.pop() {
                    return Err(error);
                }
            }
            Err(error) => return Err(error),
        }
    }
}

/// 检查现有前缀的每一级，避免 canonicalize 跟随任意目录符号链接。
fn reject_unexpected_symlink_components(path: &Path) -> io::Result<()> {
    let mut current = PathBuf::new();
    for component in path.components() {
        // Windows 的盘符和根目录先组合，避免检查盘符相对路径。
        if matches!(component, Component::Prefix(_) | Component::RootDir) {
            current.push(component.as_os_str());
            continue;
        }
        current.push(component.as_os_str());
        let metadata = fs::symlink_metadata(&current)?;
        if metadata.file_type().is_symlink() && !is_allowed_macos_system_alias(&current)? {
            tracing::debug!("拒绝路径现有前缀中的非允许符号链接");
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "路径现有前缀包含不允许的符号链接",
            ));
        }
    }
    Ok(())
}

/// 仅允许 macOS 的 `/var`、`/tmp` 和 `/etc` 系统别名，并校验其真实目标。
fn is_allowed_macos_system_alias(path: &Path) -> io::Result<bool> {
    #[cfg(target_os = "macos")]
    {
        let Some((alias, expected_target)) = (match path {
            path if path == Path::new("/var") => Some(("var", Path::new("/private/var"))),
            path if path == Path::new("/tmp") => Some(("tmp", Path::new("/private/tmp"))),
            path if path == Path::new("/etc") => Some(("etc", Path::new("/private/etc"))),
            _ => None,
        }) else {
            return Ok(false);
        };
        let resolved = fs::canonicalize(path)?;
        if resolved == expected_target {
            tracing::debug!(alias, "允许受限 macOS 系统路径别名");
            return Ok(true);
        }
        tracing::debug!(alias, "拒绝目标不符的 macOS 系统路径别名");
    }
    #[cfg(not(target_os = "macos"))]
    let _ = path;
    Ok(false)
}

/// 只 canonicalize 日志路径的目录前缀，保留最终文件名以便 writer 检查文件符号链接。
fn canonicalize_sidecar_log_path(path: &Path) -> io::Result<PathBuf> {
    let Some(file_name) = path.file_name() else {
        return canonicalize_existing_path_prefix(path);
    };
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "日志路径缺少父目录"))?;
    Ok(canonicalize_existing_path_prefix(parent)?.join(file_name))
}

/// Task 5 必须拒绝的 sidecar 环境变量；用户 Key 绝不以环境形式传给 sidecar。
fn is_forbidden_environment_variable(name: &str) -> bool {
    matches!(
        name,
        "GROK_CHAT_MODE" | "XAI_API_KEY" | "GROK_CODE_XAI_API_KEY"
    )
}

/// 返回 `env_clear` 后仍可能被平台运行时或动态链接器需要的最小变量白名单。
fn platform_environment_allowlist() -> &'static [&'static str] {
    #[cfg(target_os = "macos")]
    {
        &[
            "PATH",
            "HOME",
            "TMPDIR",
            "LANG",
            "LC_ALL",
            "DYLD_LIBRARY_PATH",
        ]
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    {
        &[
            "PATH",
            "HOME",
            "TMPDIR",
            "LANG",
            "LC_ALL",
            "LD_LIBRARY_PATH",
        ]
    }

    #[cfg(windows)]
    {
        &[
            "PATH",
            "HOME",
            "USERPROFILE",
            "TMP",
            "TEMP",
            "SystemRoot",
            "WINDIR",
            "ComSpec",
            "PATHEXT",
        ]
    }

    #[cfg(not(any(unix, windows)))]
    {
        &["PATH"]
    }
}

/// 从当前进程读取平台运行时所需变量，并拒绝已知 Key 形态。
fn platform_environment_values() -> Result<BTreeMap<OsString, OsString>, SupervisorError> {
    let mut variables = BTreeMap::new();
    for name in platform_environment_allowlist() {
        let Some(value) = env::var_os(name) else {
            continue;
        };
        if resembles_user_key(&value) {
            return Err(SupervisorError::EnvironmentValueNotAllowed {
                name: (*name).to_owned(),
            });
        }
        variables.insert(OsString::from(*name), value);
    }
    Ok(variables)
}

/// 判断变量名是否属于固定的平台运行时白名单。
fn is_platform_environment_variable(name: &str) -> bool {
    platform_environment_allowlist()
        .iter()
        .any(|allowed| *allowed == name)
}

/// 识别用户 Key 的已知短前缀；仅检查形态，不保存或显示其内容。
fn resembles_user_key(value: &OsStr) -> bool {
    value.to_string_lossy().starts_with("sk-")
}

/// 记录最先发生的错误，同时允许后续清理动作继续执行。
fn remember_error(first_error: &mut Option<SupervisorError>, result: Result<(), SupervisorError>) {
    if first_error.is_none()
        && let Err(error) = result
    {
        *first_error = Some(error);
    }
}

/// 等待 child 退出；等待错误按 fail-safe 视为尚未退出并继续升级终止。
fn wait_or_record_error(
    child: &mut dyn ChildLifecycleOps,
    timeout: Duration,
    first_error: &mut Option<SupervisorError>,
) -> bool {
    match child.wait_for_exit(timeout) {
        Ok(exited) => exited,
        Err(error) => {
            if first_error.is_none() {
                *first_error = Some(error);
            }
            false
        }
    }
}

/// 将累计的最先错误转换为关闭结果。
fn finish_lifecycle(first_error: Option<SupervisorError>) -> Result<(), SupervisorError> {
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::io::{self, BufRead, BufReader};
    use std::process::{Child, Command, Stdio};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use super::{
        BindingTokenRegistry, ManagedSidecar, ProcessGroup, ProcessScope, ProcessSlotMetadata,
        ProcessSlotState, ScopePaths, ScopeSlot, ScopeSlotRuntime, SpawnedSidecarGuard,
        SupervisorError, prepare_slot_launch, slot_generation_is_live, spawn_enrolled_sidecar,
        stop_slot_with_kill, terminate_and_reap_detached_process,
    };

    /// 构造仅用于生命周期失败路径的内存 scope slot，不接入 Task 7b 的 ACP actor。
    fn test_slot(
        process: Option<ManagedSidecar>,
        pid: Option<u32>,
        launching: bool,
    ) -> Arc<ScopeSlot> {
        Arc::new(ScopeSlot {
            paths: ScopePaths {
                home: std::env::temp_dir().join("efflab-supervisor-test-home"),
                workspace: std::env::temp_dir().join("efflab-supervisor-test-workspace"),
            },
            metadata: Mutex::new(ProcessSlotMetadata {
                scope_id: "scope-test".to_string(),
                pid,
                generation: 1,
                session_ids: Default::default(),
                current_session: None,
                state: ProcessSlotState::Idle,
            }),
            runtime: Mutex::new(ScopeSlotRuntime {
                child: process,
                has_started: true,
                launching,
                stopping: false,
                restart_blocked: false,
            }),
        })
    }

    /// 把测试 child 放入独立 group，令生命周期断言能观测 leader 与孙进程一并回收。
    fn detached_test_child(command: &mut Command) -> (Child, Arc<ProcessGroup>) {
        xai_tty_utils::detach_std_command(command);
        #[allow(clippy::disallowed_methods)]
        // 测试 fixture：立即 attach 到 ProcessGroup，测试结束由受测生命周期函数回收。
        let child = command.spawn().expect("测试 sidecar 必须可启动");
        let mut process_group = ProcessGroup::new().expect("测试 process group 必须创建成功");
        process_group
            .attach_std(&child)
            .expect("测试 child 必须能加入 process group");
        (child, Arc::new(process_group))
    }

    /// 从测试 sidecar 的标准输出读取其创建的孙进程 PID。
    fn reported_descendant_pid(child: &mut Child) -> libc::pid_t {
        let stdout = child.stdout.take().expect("测试 sidecar 必须有 stdout");
        let mut descendant_pid = String::new();
        BufReader::new(stdout)
            .read_line(&mut descendant_pid)
            .expect("测试 sidecar 必须报告孙进程 pid");
        descendant_pid
            .trim()
            .parse::<libc::pid_t>()
            .expect("孙进程 pid 必须是整数")
    }

    /// 等待被杀孙进程消失；返回 `true` 表示 deadline 后仍可被信号探测。
    fn descendant_survived_until_deadline(descendant_pid: libc::pid_t) -> bool {
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut descendant_survived = unsafe { libc::kill(descendant_pid, 0) } == 0;
        while descendant_survived && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
            descendant_survived = unsafe { libc::kill(descendant_pid, 0) } == 0;
        }
        descendant_survived
    }

    /// 失败断言前回收仍由测试 slot 持有的 child，避免污染后续进程表。
    fn cleanup_test_slot_child(slot: &Arc<ScopeSlot>) {
        let process = slot
            .runtime
            .lock()
            .expect("测试 runtime 锁必须可用")
            .child
            .take();
        if let Some(mut process) = process {
            let _ = process.process_group.kill();
            let _ = process.child.kill();
            let _ = process.child.wait();
        }
    }

    /// 创建不读取 stdin 的测试 sidecar，以便稳定覆盖 kill 失败时的保留所有权分支。
    fn long_running_process() -> (ManagedSidecar, String, u32) {
        let registry = Arc::new(BindingTokenRegistry::default());
        let token = registry
            .register("scope-test", 1, 1)
            .expect("测试 binding 必须可注册");
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg("while :; do sleep 1; done")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let (child, process_group) = detached_test_child(&mut command);
        let pid = child.id();
        (
            ManagedSidecar {
                child,
                process_group,
                registry,
                scope_id: "scope-test".to_string(),
                generation: 1,
            },
            token.as_bearer(),
            pid,
        )
    }

    /// kill 报错时 token 必须已失效、child 仍被 slot 持有，且 launch 不得创建并行进程。
    #[test]
    fn stop_kill_failure_invalidates_token_and_blocks_restart() {
        let (process, token, pid) = long_running_process();
        let registry = Arc::clone(&process.registry);
        let slot = test_slot(Some(process), Some(pid), false);

        let error = stop_slot_with_kill(&slot, std::time::Duration::ZERO, |_child| {
            Err(io::Error::other("injected kill failure"))
        })
        .expect_err("注入 kill 失败必须向上报告");
        assert!(matches!(
            error,
            SupervisorError::Io {
                operation: "终止 child",
                ..
            }
        ));
        assert!(
            registry.authorize(&token).is_none(),
            "停止入口必须在 kill 前立即撤销旧 generation token"
        );
        {
            let runtime = slot.runtime.lock().expect("测试 runtime 锁必须可用");
            assert!(runtime.child.is_some(), "kill 失败时 child 所有权不得丢失");
            assert!(
                runtime.restart_blocked,
                "kill 失败时 scope 必须保持不可重启，避免双 sidecar"
            );
            assert!(!runtime.stopping, "失败后 slot 必须回到可观察的保留状态");
        }
        let metadata = slot.metadata().expect("测试 metadata 锁必须可用");
        assert_eq!(metadata.pid, Some(pid));
        assert_eq!(metadata.state, ProcessSlotState::Killing);
        assert!(matches!(
            prepare_slot_launch(&slot),
            Err(SupervisorError::ScopeAlreadyRunning)
        ));

        // 测试结束前由本测试直接强制回收故意保留的 child，避免留下系统进程。
        let mut process = slot
            .runtime
            .lock()
            .expect("测试 runtime 锁必须可用")
            .child
            .take()
            .expect("失败路径必须保留 child");
        let _ = process.process_group.kill();
        let _ = process.child.kill();
        let _ = process.child.wait();
    }

    /// spawn 后 slot 挂接失败时，RAII guard 必须撤销 token 并终止/回收未挂接 child。
    #[test]
    fn spawned_child_guard_reaps_child_when_slot_attachment_fails() {
        let (process, token, pid) = long_running_process();
        let registry = Arc::clone(&process.registry);
        let slot = test_slot(None, None, false);
        let mut guard = SpawnedSidecarGuard::new(process);

        assert!(matches!(
            guard.attach(&slot, pid),
            Err(SupervisorError::ScopeAlreadyRunning)
        ));
        drop(guard);

        assert!(
            registry.authorize(&token).is_none(),
            "挂接失败清理必须先撤销未受监督 child 的 token"
        );
        // guard 已 wait，pid 不能仍代表该测试 child；ESRCH 是 Unix 上已回收的可观测证据。
        let probe = unsafe { libc::kill(pid as libc::pid_t, 0) };
        assert_eq!(probe, -1, "挂接失败 child 必须已被终止并回收");
        assert_eq!(
            io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH),
            "挂接失败 child 的 pid 必须不可再被信号探测"
        );
    }

    /// sidecar 退出清理必须杀掉同一 detached process group 中的孙进程，不能只回收 leader。
    #[test]
    fn detached_cleanup_reaps_process_group_descendants() {
        let registry = Arc::new(BindingTokenRegistry::default());
        let _token = registry
            .register("scope-tree", 1, 1)
            .expect("测试 binding 必须可注册");
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg("sleep 60 & printf '%s\\n' \"$!\"; wait")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let (mut child, process_group) = detached_test_child(&mut command);
        let stdout = child.stdout.take().expect("测试 sidecar 必须有 stdout");
        let mut descendant_pid = String::new();
        BufReader::new(stdout)
            .read_line(&mut descendant_pid)
            .expect("测试 sidecar 必须报告孙进程 pid");
        let descendant_pid = descendant_pid
            .trim()
            .parse::<libc::pid_t>()
            .expect("孙进程 pid 必须是整数");
        let process = ManagedSidecar {
            child,
            process_group,
            registry,
            scope_id: "scope-tree".to_string(),
            generation: 1,
        };

        terminate_and_reap_detached_process(process);
        // `killpg` 已发信号后，孙进程可能短暂保持 zombie，交由 init 回收才会呈现 ESRCH。
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut descendant_survived = unsafe { libc::kill(descendant_pid, 0) } == 0;
        while descendant_survived && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
            descendant_survived = unsafe { libc::kill(descendant_pid, 0) } == 0;
        }
        if descendant_survived {
            // 失败路径也必须回收故意制造的孙进程，不能污染后续测试进程表。
            unsafe { libc::kill(descendant_pid, libc::SIGKILL) };
        }
        assert!(
            !descendant_survived,
            "sidecar leader 已回收后，孙进程不得继续存活"
        );
    }

    /// leader 自然退出后 watcher 路径必须显式回收同一 group 中的孙进程。
    #[test]
    fn natural_exit_reaps_process_group_descendant() {
        let registry = Arc::new(BindingTokenRegistry::default());
        let _token = registry
            .register("scope-natural-exit", 1, 1)
            .expect("测试 binding 必须可注册");
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg("sleep 60 & printf '%s\\n' \"$!\"")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let (mut child, process_group) = detached_test_child(&mut command);
        let child_pid = child.id();
        let descendant_pid = reported_descendant_pid(&mut child);
        let slot = test_slot(
            Some(ManagedSidecar {
                child,
                process_group: Arc::clone(&process_group),
                registry,
                scope_id: "scope-natural-exit".to_string(),
                generation: 1,
            }),
            Some(child_pid),
            false,
        );

        let deadline = Instant::now() + Duration::from_secs(1);
        let mut observed_exit = false;
        let mut lifecycle_error = None;
        while Instant::now() < deadline {
            match slot_generation_is_live(&slot, 1) {
                Ok(false) => {
                    observed_exit = true;
                    break;
                }
                Ok(true) => std::thread::sleep(Duration::from_millis(10)),
                Err(error) => {
                    lifecycle_error = Some(error);
                    break;
                }
            }
        }
        let descendant_survived = descendant_survived_until_deadline(descendant_pid);
        if descendant_survived {
            let _ = process_group.kill();
        }
        if !observed_exit {
            cleanup_test_slot_child(&slot);
        }

        assert!(
            lifecycle_error.is_none(),
            "自然退出 watcher 路径不得报告错误: {lifecycle_error:?}"
        );
        assert!(observed_exit, "自然退出必须被 slot watcher 检测并回收");
        assert!(
            !descendant_survived,
            "leader 自然退出后，其 process group 中的孙进程不得继续存活"
        );
    }

    /// 关闭 stdin 触发正常 EOF 时，stop 路径也必须回收同一 group 中的孙进程。
    #[test]
    fn normal_eof_reaps_process_group_descendant() {
        let registry = Arc::new(BindingTokenRegistry::default());
        let _token = registry
            .register("scope-normal-eof", 1, 1)
            .expect("测试 binding 必须可注册");
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg("sleep 60 & printf '%s\\n' \"$!\"; read _")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let (mut child, process_group) = detached_test_child(&mut command);
        let child_pid = child.id();
        let descendant_pid = reported_descendant_pid(&mut child);
        let slot = test_slot(
            Some(ManagedSidecar {
                child,
                process_group: Arc::clone(&process_group),
                registry,
                scope_id: "scope-normal-eof".to_string(),
                generation: 1,
            }),
            Some(child_pid),
            false,
        );
        let forced_kill = Arc::new(AtomicBool::new(false));
        let stop_result = stop_slot_with_kill(&slot, Duration::from_secs(1), {
            let forced_kill = Arc::clone(&forced_kill);
            move |child| {
                forced_kill.store(true, Ordering::Release);
                child.kill()
            }
        });
        let descendant_survived = descendant_survived_until_deadline(descendant_pid);
        if descendant_survived {
            let _ = process_group.kill();
        }
        if stop_result.is_err() {
            cleanup_test_slot_child(&slot);
        }

        assert!(
            stop_result.is_ok(),
            "正常 EOF 必须在宽限期内完成，不应落入强制终止: {stop_result:?}"
        );
        assert!(
            !forced_kill.load(Ordering::Acquire),
            "正常 EOF 不得调用强制终止回调"
        );
        assert!(
            !descendant_survived,
            "关闭 stdin 后 leader 正常 EOF 退出时，孙进程不得继续存活"
        );
    }

    /// std child 必须在 spawn 后立即登记到 ProcessScope，scope 关闭时可回收完整 tree。
    #[test]
    fn std_sidecar_spawn_enrolls_process_scope() {
        let process_scope = ProcessScope::new();
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg("while :; do sleep 1; done")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        let (mut child, _process_group) = spawn_enrolled_sidecar(&mut command, &process_scope)
            .expect("std sidecar 必须成功登记到 process scope");
        assert_eq!(
            process_scope.live_count(),
            1,
            "spawn 返回前必须已有一个可由 scope 回收的 process group"
        );

        process_scope.kill_all();
        child.wait().expect("scope 关闭后必须回收 sidecar leader");
    }
}

#[cfg(all(test, unix))]
mod config_write_tests {
    use super::{
        ScopePaths, Supervisor, SupervisorError, canonicalize_existing_path_prefix,
        open_sidecar_log_file, prepare_scope_directories, write_authoritative_config,
    };
    use crate::{HostRuntimeConfig, L3bRuntimeConfig};
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use std::time::Duration;

    /// 权威配置必须替换旧文件、保留完整内容，并在成功后不遗留同目录临时文件。
    #[test]
    fn authoritative_config_replaces_file_without_temp_residue() {
        let temporary = tempfile::tempdir().expect("必须能创建配置原子写测试目录");
        // writer 保持对祖先符号链接的严格拒绝；测试输入使用物理临时根以聚焦原子替换。
        let parent = fs::canonicalize(temporary.path())
            .expect("临时目录物理路径必须可解析")
            .join("scope-home");
        fs::create_dir(&parent).expect("必须能创建配置父目录");
        let path = parent.join("runtime-config.v1.toml");
        fs::write(&path, b"old-config\n").expect("必须能写入旧配置");
        let temp_path = parent.join(format!(
            ".{}.{}.tmp",
            path.file_name()
                .expect("配置文件名必须存在")
                .to_string_lossy(),
            std::process::id()
        ));

        write_authoritative_config(&path, b"schema_version = 1\n")
            .expect("权威配置必须成功原子替换");

        assert_eq!(
            fs::read(&path).expect("必须能读取替换后的配置"),
            b"schema_version = 1\n",
            "替换后只能看到完整的新配置"
        );
        assert!(
            !temp_path.exists(),
            "成功替换后同目录临时文件必须已被 rename 消费"
        );
        #[cfg(unix)]
        assert_eq!(
            fs::symlink_metadata(&path)
                .expect("必须能读取配置权限")
                .permissions()
                .mode()
                & 0o777,
            0o600,
            "权威配置必须保持 owner-only 权限"
        );
    }

    /// 原子写入口遇到目标符号链接必须拒绝，不能跟随链接覆盖目录外文件。
    #[test]
    fn authoritative_config_rejects_symlink_destination() {
        let temporary = tempfile::tempdir().expect("必须能创建配置链接测试目录");
        let root = fs::canonicalize(temporary.path()).expect("临时目录物理路径必须可解析");
        let target = root.join("outside-config");
        let path = root.join("runtime-config.v1.toml");
        fs::write(&target, b"untouched\n").expect("必须能写入链接目标");
        std::os::unix::fs::symlink(&target, &path).expect("必须能创建配置符号链接");

        let error = write_authoritative_config(&path, b"must-not-write\n")
            .expect_err("目标符号链接必须 fail-closed");
        assert!(matches!(error, SupervisorError::ConfigWriteFailed));
        assert_eq!(
            fs::read(&target).expect("链接目标必须保持可读"),
            b"untouched\n",
            "拒绝符号链接时不得改写目录外目标"
        );
    }

    /// 权威配置的父目录链中存在符号链接时，Host 不得沿链写到目录外。
    #[test]
    fn authoritative_config_rejects_symlinked_ancestor_directory() {
        let temporary = tempfile::tempdir().expect("必须能创建祖先链接测试目录");
        let root = fs::canonicalize(temporary.path()).expect("临时目录物理路径必须可解析");
        let outside = root.join("outside");
        let linked = root.join("linked");
        let nested = outside.join("nested");
        fs::create_dir(&outside).expect("必须能创建目录外目标");
        fs::create_dir(&nested).expect("必须能创建目录外嵌套目标");
        std::os::unix::fs::symlink(&outside, &linked).expect("必须能创建祖先目录符号链接");

        let path = linked.join("nested").join("runtime-config.v1.toml");
        let error = write_authoritative_config(&path, b"must-not-write\n")
            .expect_err("配置祖先符号链接必须 fail-closed");

        assert!(matches!(error, SupervisorError::ConfigWriteFailed));
        assert!(
            !nested.join("runtime-config.v1.toml").exists(),
            "拒绝祖先符号链接时不得在目录外创建配置"
        );
    }

    /// scope 的 home/workspace 目录必须逐级创建并在每一级拒绝符号链接。
    #[test]
    fn scope_directories_reject_symlinked_ancestor_directory() {
        let temporary = tempfile::tempdir().expect("必须能创建 scope 目录链接测试目录");
        let root = fs::canonicalize(temporary.path()).expect("临时目录物理路径必须可解析");
        let outside = root.join("outside");
        let linked = root.join("linked");
        fs::create_dir(&outside).expect("必须能创建 scope 目录外目标");
        std::os::unix::fs::symlink(&outside, &linked).expect("必须能创建 scope 祖先符号链接");

        let paths = ScopePaths {
            home: linked.join("nested").join("home"),
            workspace: temporary.path().join("workspace"),
        };
        let error =
            prepare_scope_directories(&paths).expect_err("scope 目录祖先符号链接必须 fail-closed");

        assert!(matches!(error, SupervisorError::ConfigWriteFailed));
        assert!(
            !outside.join("nested").exists(),
            "拒绝 scope 祖先符号链接时不得在目录外创建目录"
        );
    }

    /// 缺失尾部应接在最近现有前缀的物理路径后，不能要求调用方预先创建完整根。
    #[test]
    fn canonicalize_existing_prefix_preserves_missing_tail() {
        let temporary = tempfile::tempdir().expect("必须能创建路径 canonical 测试目录");
        let physical_root =
            fs::canonicalize(temporary.path()).expect("临时目录现有前缀必须可 canonicalize");
        let requested = physical_root.join("missing").join("tail");

        let canonical =
            canonicalize_existing_path_prefix(&requested).expect("现有前缀 canonical 化必须成功");
        assert_eq!(canonical, requested);
    }

    /// 任意 Host 根路径符号链接必须在 canonicalize 前拒绝。
    #[test]
    fn supervisor_rejects_arbitrary_root_symlink() {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir().expect("必须能创建 Supervisor 根链接测试目录");
        let physical_root = temporary.path().join("physical-root");
        let outside = temporary.path().join("outside");
        let root_alias = temporary.path().join("root-alias");
        fs::create_dir(&physical_root).expect("必须能创建物理 Host 根目录");
        fs::create_dir(&outside).expect("必须能创建目录外目标");
        symlink(&outside, &root_alias).expect("必须能创建任意 Host 根路径符号链接");

        let runtime_config = HostRuntimeConfig {
            home_root: root_alias,
            sidecar_bin: temporary.path().join("sidecar"),
            sidecar_log_path: temporary.path().join("sidecar.log"),
            mcp_exec_root: temporary.path().join("mcp"),
            idle_after: Duration::from_secs(60),
            l3b: L3bRuntimeConfig::default(),
        };
        let error = Supervisor::new(runtime_config, "app")
            .err()
            .expect("任意 Host 根路径符号链接必须 fail-closed");
        assert!(matches!(
            error,
            SupervisorError::Io { operation, .. } if operation == "解析 Host home_root"
        ));
        assert!(
            !outside.join("app").exists(),
            "拒绝 Host 根路径符号链接时不得把目录写入链接目标"
        );
    }

    /// 物理 Host 根下的 app_id 后缀符号链接仍必须拒绝。
    #[test]
    fn supervisor_rejects_suffix_symlink_after_safe_root_resolution() {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir().expect("必须能创建 Supervisor 路径测试目录");
        let physical_root = temporary.path().join("physical-root");
        fs::create_dir(&physical_root).expect("必须能创建物理 Host 根目录");
        let canonical_root =
            fs::canonicalize(&physical_root).expect("Host 根目录必须可 canonicalize");
        let outside = temporary.path().join("outside");
        fs::create_dir(&outside).expect("必须能创建目录外目标");
        symlink(&outside, physical_root.join("app")).expect("必须能创建 app_id 后缀符号链接");

        let supervisor = Supervisor::new(
            HostRuntimeConfig {
                home_root: physical_root,
                sidecar_bin: temporary.path().join("sidecar"),
                sidecar_log_path: temporary.path().join("sidecar.log"),
                mcp_exec_root: temporary.path().join("mcp"),
                idle_after: Duration::from_secs(60),
                l3b: L3bRuntimeConfig::default(),
            },
            "app",
        )
        .expect("物理根路径必须能构造 Supervisor");
        let paths = supervisor
            .paths_for("scope")
            .expect("合法 scope 必须能派生路径");

        assert_eq!(
            paths.home,
            canonical_root.join("app").join("scope").join("home")
        );
        let error = prepare_scope_directories(&paths)
            .expect_err("canonical 根下的 app_id 符号链接必须 fail-closed");
        assert!(matches!(error, SupervisorError::ConfigWriteFailed));
        assert!(
            !outside.join("scope").exists(),
            "拒绝 canonical 根下的后缀符号链接时不得在目录外创建 scope"
        );
    }

    /// 任意 sidecar 日志父目录符号链接必须在 canonicalize 前拒绝。
    #[test]
    fn supervisor_rejects_arbitrary_log_parent_symlink() {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir().expect("必须能创建日志父目录链接测试目录");
        let home_root = temporary.path().join("home-root");
        let outside = temporary.path().join("outside-log-root");
        let log_parent_alias = temporary.path().join("log-parent-alias");
        fs::create_dir(&home_root).expect("必须能创建 Host home 根目录");
        fs::create_dir(&outside).expect("必须能创建日志目录外目标");
        symlink(&outside, &log_parent_alias).expect("必须能创建任意日志父目录符号链接");

        let error = Supervisor::new(
            HostRuntimeConfig {
                home_root,
                sidecar_bin: temporary.path().join("sidecar"),
                sidecar_log_path: log_parent_alias.join("sidecar.log"),
                mcp_exec_root: temporary.path().join("mcp"),
                idle_after: Duration::from_secs(60),
                l3b: L3bRuntimeConfig::default(),
            },
            "app",
        )
        .err()
        .expect("任意日志父目录符号链接必须 fail-closed");
        assert!(matches!(
            error,
            SupervisorError::Io { operation, .. } if operation == "解析 sidecar 日志路径"
        ));
        assert!(
            !outside.join("sidecar.log").exists(),
            "拒绝日志父目录符号链接时不得在目录外创建日志"
        );
    }

    /// 物理日志父目录下的最终日志符号链接仍必须由 writer 拒绝。
    #[test]
    fn supervisor_preserves_final_log_symlink_rejection() {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir().expect("必须能创建日志路径测试目录");
        let physical_log_root = temporary.path().join("physical-log-root");
        fs::create_dir(&physical_log_root).expect("必须能创建物理日志目录");
        let target = physical_log_root.join("target.log");
        fs::write(&target, b"untouched\n").expect("必须能写入日志目标");
        let log_link = physical_log_root.join("sidecar.log");
        symlink(&target, &log_link).expect("必须能创建日志文件符号链接");
        let home_root = temporary.path().join("home-root");
        fs::create_dir(&home_root).expect("必须能创建 Host home 根目录");

        let supervisor = Supervisor::new(
            HostRuntimeConfig {
                home_root,
                sidecar_bin: temporary.path().join("sidecar"),
                sidecar_log_path: log_link,
                mcp_exec_root: temporary.path().join("mcp"),
                idle_after: Duration::from_secs(60),
                l3b: L3bRuntimeConfig::default(),
            },
            "app",
        )
        .expect("物理日志父目录必须能构造 Supervisor");
        let expected_log_path = fs::canonicalize(&physical_log_root)
            .expect("物理日志目录必须可 canonicalize")
            .join("sidecar.log");
        assert_eq!(supervisor.config.sidecar_log_path, expected_log_path);

        let error = open_sidecar_log_file(&supervisor.config.sidecar_log_path)
            .expect_err("最终日志符号链接必须 fail-closed");
        assert!(matches!(
            error,
            SupervisorError::Io { operation, .. } if operation == "打开 sidecar 日志文件"
        ));
        assert_eq!(
            fs::read(&target).expect("日志目标必须保持可读"),
            b"untouched\n"
        );
    }

    /// macOS `/var`、`/tmp` 和 `/etc` 系统别名必须保留缺失尾部处理。
    #[cfg(target_os = "macos")]
    #[test]
    fn canonicalize_existing_prefix_allows_macos_system_aliases() {
        for (alias, canonical_root) in [
            (Path::new("/var"), Path::new("/private/var")),
            (Path::new("/tmp"), Path::new("/private/tmp")),
            (Path::new("/etc"), Path::new("/private/etc")),
        ] {
            let suffix = format!("efflab-supervisor-alias-{}", std::process::id());
            let requested = alias.join(format!("{suffix}-missing")).join("tail");
            let canonical = canonicalize_existing_path_prefix(&requested)
                .expect("macOS 系统别名的缺失尾部必须可解析");
            assert_eq!(
                canonical,
                canonical_root
                    .join(format!("{suffix}-missing"))
                    .join("tail")
            );
        }
    }
}

#[cfg(test)]
mod sidecar_log_tests {
    use super::{SupervisorError, open_sidecar_log_file};
    use std::fs;
    use std::io::Write;
    use std::path::Path;

    /// 相对路径或含 `..` 的日志路径必须在打开前 fail-closed。
    #[test]
    fn sidecar_log_path_must_be_absolute_without_parent_dir() {
        let relative = open_sidecar_log_file(Path::new("sidecar.log"))
            .expect_err("相对 sidecar 日志路径必须拒绝");
        assert!(matches!(relative, SupervisorError::HomeRootMustBeAbsolute));

        let mut traversal = std::env::temp_dir();
        traversal.push("efflab-sidecar");
        traversal.push("..");
        traversal.push("sidecar.log");
        let traversal =
            open_sidecar_log_file(&traversal).expect_err("含 .. 的 sidecar 日志路径必须拒绝");
        assert!(matches!(
            traversal,
            SupervisorError::HomeRootContainsParentDirectory
        ));
    }

    /// 独立日志文件必须可创建父目录，并在再次打开时追加而不是截断。
    #[test]
    fn sidecar_log_file_creates_parent_and_appends() {
        let temporary = tempfile::tempdir().expect("必须能创建 sidecar 日志测试目录");
        let root = fs::canonicalize(temporary.path()).expect("临时目录物理路径必须可解析");
        let path = root.join("nested").join("sidecar.log");
        {
            let mut file = open_sidecar_log_file(&path).expect("首次打开 sidecar 日志必须成功");
            writeln!(file, "first").expect("写入 sidecar 日志必须成功");
        }
        {
            let mut file = open_sidecar_log_file(&path).expect("再次打开 sidecar 日志必须成功");
            writeln!(file, "second").expect("追加 sidecar 日志必须成功");
        }
        let text = std::fs::read_to_string(&path).expect("必须能读取 sidecar 日志");
        assert!(
            text.contains("first"),
            "独立日志应保留首次写入，实际: {text}"
        );
        assert!(
            text.contains("second"),
            "独立日志应追加第二次写入，实际: {text}"
        );
    }

    /// 新建 sidecar 日志必须是 owner-only，已有过宽权限文件必须被收紧。
    #[cfg(unix)]
    #[test]
    fn sidecar_log_file_is_owner_only_and_tightens_existing_mode() {
        use std::os::unix::fs::PermissionsExt;

        let temporary = tempfile::tempdir().expect("必须能创建 sidecar 权限测试目录");
        let root = fs::canonicalize(temporary.path()).expect("临时目录物理路径必须可解析");
        let created = root.join("created.log");
        open_sidecar_log_file(&created).expect("新建 sidecar 日志必须成功");
        let created_mode = std::fs::symlink_metadata(&created)
            .expect("必须能读取新建 sidecar 日志权限")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(created_mode, 0o600, "新建 sidecar 日志必须是 0o600");

        let existing = root.join("existing.log");
        std::fs::write(&existing, "old\n").expect("必须能写入预先存在的 sidecar 日志");
        std::fs::set_permissions(&existing, std::fs::Permissions::from_mode(0o644))
            .expect("必须能把既有 sidecar 日志设为过宽权限");
        open_sidecar_log_file(&existing).expect("打开既有 sidecar 日志必须成功");
        let existing_mode = std::fs::symlink_metadata(&existing)
            .expect("必须能读取收紧后的 sidecar 日志权限")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(
            existing_mode, 0o600,
            "已有过宽 sidecar 日志必须被收紧为 0o600"
        );
    }

    /// sidecar 日志路径若是符号链接必须 fail-closed，避免跟到产品目录外。
    #[cfg(unix)]
    #[test]
    fn sidecar_log_file_rejects_symlink() {
        let temporary = tempfile::tempdir().expect("必须能创建 sidecar symlink 测试目录");
        let root = fs::canonicalize(temporary.path()).expect("临时目录物理路径必须可解析");
        let target = root.join("target.log");
        let link = root.join("sidecar.log");
        std::fs::write(&target, "secret\n").expect("必须能写入 symlink 目标");
        std::os::unix::fs::symlink(&target, &link).expect("必须能创建 sidecar 日志 symlink");
        let error = open_sidecar_log_file(&link).expect_err("sidecar 日志 symlink 必须被拒绝");
        assert!(
            matches!(error, SupervisorError::Io { operation, .. } if operation == "打开 sidecar 日志文件"),
            "symlink 必须按日志文件打开失败处理，实际: {error}"
        );
    }

    /// sidecar 日志父目录链中存在符号链接时，不得把日志追加到目录外。
    #[cfg(unix)]
    #[test]
    fn sidecar_log_file_rejects_symlinked_ancestor_directory() {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir().expect("必须能创建 sidecar 祖先链接测试目录");
        let root = fs::canonicalize(temporary.path()).expect("临时目录物理路径必须可解析");
        let outside = root.join("outside");
        let linked = root.join("linked");
        let nested = outside.join("nested");
        fs::create_dir(&outside).expect("必须能创建日志目录外目标");
        fs::create_dir(&nested).expect("必须能创建日志目录外嵌套目标");
        symlink(&outside, &linked).expect("必须能创建日志祖先符号链接");

        let path = linked.join("nested").join("sidecar.log");
        let error =
            open_sidecar_log_file(&path).expect_err("sidecar 日志祖先符号链接必须 fail-closed");

        assert!(
            matches!(error, SupervisorError::Io { operation, .. } if operation == "创建 sidecar 日志目录"),
            "日志祖先链接必须在创建目录阶段拒绝，实际: {error}"
        );
        assert!(
            !nested.join("sidecar.log").exists(),
            "拒绝日志祖先符号链接时不得在目录外创建日志"
        );
    }
}
