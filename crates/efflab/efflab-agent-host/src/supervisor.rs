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
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use efflab_agent_contract::{SidecarModelSpec, render_authoritative_config};

use crate::HostRuntimeConfig;
use crate::llm_channel::LlmChannelManager;
use crate::llm_loopback::{BindingToken, BindingTokenRegistry, L3bLoopback};

/// 关闭 stdin 后等待 sidecar 自然退出的固定宽限期。
pub const STDIN_CLOSE_GRACE: Duration = Duration::from_millis(3_500);
/// 发出终止请求后等待 sidecar 退出的固定宽限期。
pub const TERMINATE_GRACE: Duration = Duration::from_secs(2);

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
            Self::ScopeAlreadyRunning => formatter.write_str("scope 已有 sidecar 进程"),
            Self::ConfigRenderFailed => formatter.write_str("权威 sidecar 配置渲染失败"),
            Self::ConfigWriteFailed => formatter.write_str("权威 sidecar 配置写入失败"),
            Self::RestartFailed => formatter.write_str("sidecar 批量重启失败"),
            Self::StateUnavailable => formatter.write_str("scope slot 状态不可用"),
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
    /// 传给 sidecar `--grok-home` 的私有、稳定目录。
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
    /// registry 保留 token 本体；child 所有权只保留失效所需的 registry 与 generation。
    registry: Arc<BindingTokenRegistry>,
    generation: u64,
}

/// scope slot 中不进入产品可观察 metadata 的启动协调状态。
struct ScopeSlotRuntime {
    child: Option<ManagedSidecar>,
    has_started: bool,
    launching: bool,
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
}

impl Supervisor {
    /// 从产品 App Data 根和 Host 强制校验的 app_id 构造 supervisor。
    pub fn new(
        config: HostRuntimeConfig,
        app_id: impl AsRef<str>,
    ) -> Result<Self, SupervisorError> {
        validate_absolute_path(&config.home_root)?;
        let app_id = sanitize(app_id.as_ref())?;

        Ok(Self {
            config,
            app_id,
            slots: Mutex::new(BTreeMap::new()),
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
    pub fn restart_live_scopes(
        &self,
        loopback: &L3bLoopback,
        channel: &LlmChannelManager,
    ) -> Result<(), SupervisorError> {
        let scopes = self.live_scope_ids()?;
        let mut failed = false;
        for scope in &scopes {
            if self.stop_scope(scope).is_err() {
                failed = true;
            }
        }
        for scope in &scopes {
            if self.launch_sidecar(scope, loopback, channel).is_err() {
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
        loopback: &L3bLoopback,
        token: BindingToken,
        registry: Arc<BindingTokenRegistry>,
        paths: ScopePaths,
    ) -> Result<SidecarProcessInfo, SupervisorError> {
        prepare_scope_directories(&paths)?;
        let agent_definition = paths.home.join("agents").join("efflab-default.md");
        let rendered = render_authoritative_config(
            &paths.home,
            &agent_definition,
            None,
            &[SidecarModelSpec {
                model: model_id.to_string(),
                base_url: loopback.sidecar_base_url(),
                name: "BYOK".to_string(),
                api_backend: "chat_completions".to_string(),
                env_key: "EFFLAB_L3B_BIND".to_string(),
            }],
        )
        .map_err(|_| SupervisorError::ConfigRenderFailed)?;
        write_authoritative_config(&paths.home.join("config.toml"), rendered.as_bytes())?;

        // 仅此处把 binding token 注入 child；用户 Key 从不在 env、CLI 或 TOML 中出现。
        let environment = ChildEnvironment::for_sidecar_with_binding(&paths.home, &token)?;
        let mut command = Command::new(&self.config.sidecar_bin);
        command
            .arg("--stdio")
            .arg("--grok-home")
            .arg(&paths.home)
            .arg("--session-cwd")
            .arg(&paths.workspace)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        environment.apply(&mut command);
        let child = command.spawn().map_err(|source| SupervisorError::Io {
            operation: "spawn",
            source,
        })?;
        let pid = child.id();
        complete_slot_launch(
            slot,
            ManagedSidecar {
                child,
                registry,
                generation,
            },
            pid,
        )?;
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
}

impl Drop for Supervisor {
    /// Host 释放时回收仍由本进程持有的 child，避免遗留获得过 binding token 的 sidecar。
    fn drop(&mut self) {
        let slots = self
            .slots
            .lock()
            .map(|slots| slots.values().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        for slot in slots {
            let _ = stop_slot(&slot);
        }
    }
}

/// 把 scope slot 置为独占 launching，并为本次 child 预留单调 generation。
fn prepare_slot_launch(slot: &Arc<ScopeSlot>) -> Result<(ScopePaths, u64), SupervisorError> {
    let mut runtime = slot
        .runtime
        .lock()
        .map_err(|_| SupervisorError::StateUnavailable)?;
    if runtime.launching {
        return Err(SupervisorError::ScopeAlreadyRunning);
    }
    if let Some(process) = runtime.child.as_mut() {
        match process.child.try_wait() {
            Ok(None) => return Err(SupervisorError::ScopeAlreadyRunning),
            Ok(Some(_)) => {
                let process = runtime.child.take().expect("已检查 child 必定存在");
                process
                    .registry
                    .invalidate_generation(&slot.metadata()?.scope_id, process.generation);
            }
            Err(source) => {
                return Err(SupervisorError::Io {
                    operation: "检查 child 状态",
                    source,
                });
            }
        }
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
fn complete_slot_launch(
    slot: &Arc<ScopeSlot>,
    process: ManagedSidecar,
    pid: u32,
) -> Result<(), SupervisorError> {
    let generation = process.generation;
    let mut runtime = slot
        .runtime
        .lock()
        .map_err(|_| SupervisorError::StateUnavailable)?;
    if !runtime.launching || runtime.child.is_some() {
        return Err(SupervisorError::ScopeAlreadyRunning);
    }
    runtime.child = Some(process);
    runtime.launching = false;
    let mut metadata = slot
        .metadata
        .lock()
        .map_err(|_| SupervisorError::StateUnavailable)?;
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
        return Ok(false);
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
                .invalidate_generation(&slot.metadata()?.scope_id, process.generation);
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
    let mut process = {
        let mut runtime = slot
            .runtime
            .lock()
            .map_err(|_| SupervisorError::StateUnavailable)?;
        runtime.launching = false;
        runtime.child.take()
    };
    let Some(process) = process.as_mut() else {
        return Ok(());
    };

    // Task 7b 会先发送 session/cancel；Task 7 尚无 ACP actor，因此只能先关闭 stdin。
    let _ = process.child.stdin.take();
    let deadline = Instant::now() + STDIN_CLOSE_GRACE;
    let mut exited = false;
    while Instant::now() < deadline {
        match process.child.try_wait() {
            Ok(Some(_)) => {
                exited = true;
                break;
            }
            Ok(None) => thread::sleep(Duration::from_millis(20)),
            Err(source) => {
                process
                    .registry
                    .invalidate_generation(&slot.metadata()?.scope_id, process.generation);
                return Err(SupervisorError::Io {
                    operation: "等待 child EOF 退出",
                    source,
                });
            }
        }
    }
    if !exited {
        process.child.kill().map_err(|source| SupervisorError::Io {
            operation: "终止 child",
            source,
        })?;
        let _ = process.child.wait();
    }
    process
        .registry
        .invalidate_generation(&slot.metadata()?.scope_id, process.generation);
    let mut metadata = slot
        .metadata
        .lock()
        .map_err(|_| SupervisorError::StateUnavailable)?;
    metadata.pid = None;
    metadata.state = ProcessSlotState::Idle;
    Ok(())
}

/// Host 在 renderer 写盘前创建自己的隔离目录，并拒绝被符号链接重定向。
fn prepare_scope_directories(paths: &ScopePaths) -> Result<(), SupervisorError> {
    for directory in [&paths.home, &paths.workspace] {
        fs::create_dir_all(directory).map_err(|_| SupervisorError::ConfigWriteFailed)?;
        let metadata =
            fs::symlink_metadata(directory).map_err(|_| SupervisorError::ConfigWriteFailed)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(SupervisorError::ConfigWriteFailed);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(directory, fs::Permissions::from_mode(0o700))
                .map_err(|_| SupervisorError::ConfigWriteFailed)?;
        }
    }
    Ok(())
}

/// 原子物化 contract renderer 的完整 TOML；Host 是该文件的唯一写盘 owner。
fn write_authoritative_config(path: &Path, content: &[u8]) -> Result<(), SupervisorError> {
    let parent = path.parent().ok_or(SupervisorError::ConfigWriteFailed)?;
    let parent_metadata =
        fs::symlink_metadata(parent).map_err(|_| SupervisorError::ConfigWriteFailed)?;
    if parent_metadata.file_type().is_symlink() || !parent_metadata.is_dir() {
        return Err(SupervisorError::ConfigWriteFailed);
    }
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err(SupervisorError::ConfigWriteFailed);
        }
        Ok(_) => {}
        // 只有不存在旧配置时才能继续创建；权限、I/O 等其它元数据错误必须 fail-closed。
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(_) => return Err(SupervisorError::ConfigWriteFailed),
    }
    let temporary = parent.join(format!(".config.toml.{}.tmp", std::process::id()));
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

    /// 在完整 launch 路径只注入唯一的 L3b binding token。
    ///
    /// 与 Task 5 的通用白名单不同，真实 sidecar 进程必须从完全清空的环境启动：
    /// `--grok-home` 已由 CLI 显式传递，因而不保留 `GROK_HOME` 或任何平台变量。
    /// 参数只能是 registry 生成的 [`BindingToken`]，避免调用方把任意用户文本当作
    /// 业务环境变量注入；用户 Key、XAI Key 和代理变量均不可进入 child。
    pub fn for_sidecar_with_binding(
        grok_home: &Path,
        binding_token: &BindingToken,
    ) -> Result<Self, SupervisorError> {
        // 保留路径形状校验，确保该完整启动入口仍只接受 Host 派生的私有 home。
        validate_absolute_path(grok_home)?;
        let mut variables = BTreeMap::new();
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
