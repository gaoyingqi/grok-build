# Task 5：Supervisor 实施报告

## 状态

完成。工作目录为 `/Volumes/work/documents/effilab-agent` 的 `lab_main` 分支；未执行 push。

本任务的唯一提交将使用：`feat(host): scope-isolated supervisor with stable home and cwd`。

## 实现内容

### 稳定路径与 scope slot

新增 `crates/efflab/efflab-agent-host/src/supervisor.rs`，提供：

- `sanitize`：fail-closed 拒绝空串、`.`、任何 `..`、`/`、`\\` 与 NUL；不做替换或归一化，避免不同不透明标识映射到同一目录。
- `Supervisor::new`：拒绝相对或含 `..` 的 `HostRuntimeConfig.home_root`，并在构造时校验 `app_id`。
- `Supervisor::paths_for`：固定派生
  - `{home_root}/{sanitize(app_id)}/{sanitize(scope)}/home`
  - `{home_root}/{sanitize(app_id)}/{sanitize(scope)}/workspace`
  Host 始终追加 `app_id`，不信任调用方是否预拼产品目录；没有任何产品库根 cwd 输入面。
- `Supervisor::acquire`：以互斥的内存 map 保证一 scope 只获得一个 `Arc<ScopeSlot>`，重复 acquire 复用同一 slot。
- `ProcessSlotMetadata`：包含 `scope_id`、`pid`、`generation`、`session_ids`、`current_session`、`state`。本任务不真实 spawn，因此新 slot 的 `pid=None`、`generation=1`、`state=Idle`。
- metadata 只在 Host 内存中维护，从不创建、打开、替换或竞争 `{GROK_HOME}/.efflab-sidecar.lock`。

### 平台能力与 child env

- 新增跨平台公开类型 `SupervisorCapability`、`UnavailableReason` 与 `SupervisorError`。
- `#[cfg(windows)] capability()` 返回 `Unavailable { reason: SidecarHardeningUnavailable }`；`acquire` 在写入 slot map 前返回同一 typed error，不会 spawn。
- 新增 `ChildEnvironment`：`apply` 先调用 `Command::env_clear()`，再仅写入固定平台白名单和 Host 强制提供的绝对 `GROK_HOME`。
- `ChildEnvironment::from_whitelist` 显式拒绝 `GROK_CHAT_MODE`、`XAI_API_KEY`、`GROK_CODE_XAI_API_KEY`、未登记变量、调用方覆盖 `GROK_HOME`，以及值以 `sk-` 开头的用户 Key 形态。错误仅保留变量名，不保存或回显值。
- 未注入 `EFFLAB_L3B_BIND`，未写 TOML，未启动 sidecar。

### 生命周期抽象

- 新增跨平台 `ChildLifecycleOps`（cancel、close stdin、wait、terminate、kill）与所有者 `ChildLifecycle`。
- `Drop` 与显式 `shutdown()` 使用同一顺序：若 in-flight，先 `cancel_in_flight`；关闭 stdin 并等待 3.5 秒；随后 terminate 并等待 2 秒；仍未退出才调用 `kill`。
- 任一阶段失败仍继续执行后续清理，显式 `shutdown` 返回最早错误；Drop 不传播错误。trait 的 `kill` API 在 Windows 编译单元同样存在，真实 Job Object/TerminateProcess 接线留给 Task 7。

## TDD Evidence

### RED

在创建 `supervisor.rs` 或导出任何 Task 5 生产 API 前，先新增 `crates/efflab/efflab-agent-host/tests/supervisor.rs` 和其 test-only `tempfile` 依赖，运行：

```text
cargo test -p efflab-agent-host --test supervisor
退出码: 101
```

预期失败为新增测试无法导入尚不存在的 `ChildEnvironment`、`ChildLifecycle`、`ChildLifecycleOps`、`ProcessSlotState`、`Supervisor`、`SupervisorError`，且找不到 `sanitize`。这证明 RED 原因是 Task 5 API 尚未实现，而不是测试断言或环境问题。

修正测试自身的 env_clear 断言后，在实现前再次运行同一命令，仍以相同的缺失 API 原因退出 `101`。

### GREEN

完成最小实现并修正一次编译期 `OsString`/`OsStr` 比较后，运行：

```text
cargo test -p efflab-agent-host --test supervisor
退出码: 0
结果: 8 passed, 0 failed
```

覆盖的新增行为：同 scope slot 复用和初始 metadata、非法 component、相对 home_root、强制 app_id join、sidecar home lock 不被争抢、child env 拒绝规则、真实 `env_clear` 行为、未注入 L3b binding token，以及 Drop 生命周期顺序与固定超时。

## 验证报告

```text
cargo test -p efflab-agent-host
退出码: 0
结果:
- acp_runtime.rs: 14 passed
- protocol_and_submission.rs: 21 passed
- supervisor.rs: 8 passed
- lib unit tests 与 doc-tests: 0 failed

cargo fmt --all -- --check
退出码: 0

 git diff --check
退出码: 0
```

Windows 交叉检查：已运行 `rustup target list --installed`；仅安装 `aarch64-apple-darwin`、`aarch64-unknown-linux-gnu`、`x86_64-unknown-linux-gnu`，没有 `x86_64-pc-windows-msvc`。因此未安装目标，未尝试下载或执行 `cargo check -p efflab-agent-host --target x86_64-pc-windows-msvc`。Windows `cfg` 分支与 kill trait API 已写入本次 source，并有 `#[cfg(windows)]` 编译形状测试；实际 Windows target 验证应在安装该 target 的 CI 或开发机补跑。

## 文件变更

- `crates/efflab/efflab-agent-host/src/supervisor.rs`
  - 新增路径派生、scope slot、Windows capability、child env 和 lifecycle 抽象。
- `crates/efflab/efflab-agent-host/src/lib.rs`
  - 注册并导出 Task 5 公开类型与函数。
- `crates/efflab/efflab-agent-host/tests/supervisor.rs`
  - 新增 TDD 集成测试。
- `crates/efflab/efflab-agent-host/Cargo.toml`
  - 增加 test-only `tempfile` workspace dev-dependency。
- `Cargo.lock`
  - 记录 host crate 的 test-only `tempfile` 依赖边。
- `.superpowers/sdd/2026-08-13-effilab-agent-kit-implementation-plan/task-5-report.md`
  - 本报告。

## 自审

### Completeness

- 简报 Step 1 的 scope 重用、`..` 拒绝、强制 app_id join、独立 metadata/sidecar lock、不在 Windows acquire 的要求均有对应实现和测试。
- stable home 与 workspace 都从绝对 `home_root` 派生；不存在用户产品库 cwd 参数。
- 本任务明确禁止的 L3b token、models TOML、真实 spawn integration 均未加入。
- 生命周期顺序、两个精确宽限期与 Windows 保留 kill API 都有明确类型边界；真实平台进程/Job Object 实现没有被提前猜测或接线。

### Quality / Security

- 新增生产逻辑的模块、类型与关键分支均有简体中文文档注释。
- child env 默认清空父环境；仅固定白名单可回填；已知 credential/chat-mode 名称和 `sk-` 值 fail-closed，错误文本不含值。
- Host 不依赖 sidecar library 或 grok shell；既有 `host_crate_does_not_depend_on_sidecar_or_grok_shell` 测试仍通过。
- 内存 metadata 避免了对 sidecar lock 文件的任何文件系统竞争；测试预置 lock 后验证其内容不变。

### YAGNI / 边界

- 未修改 `AcpRuntime`、`HostRuntime` dispatch、权威 config、projector、L3b、channel 或真实 sidecar spawn。
- 未提前实现 Windows Job Object、实际 `std::process::Child` 接线或 session 驱动状态迁移；只冻结 Task 7 所需的安全 API 与 lifecycle 顺序。

### Pristine output

- `cargo test -p efflab-agent-host`、`cargo fmt --all -- --check` 和 `git diff --check` 都在提交前通过。
- 除未安装的 Windows target 外，没有已知阻断问题；未发现未跟踪临时测试文件或敏感输出。

## Concerns

唯一环境限制是本机未安装 `x86_64-pc-windows-msvc`，所以无法执行指定 Windows target 的真实编译检查；这不会改变 Windows source gate 的 fail-closed 语义，但应由具备该 target 的 CI 补充验证。

## 证据清单

### 已读文件（关键）

- `.superpowers/sdd/2026-08-13-effilab-agent-kit-implementation-plan/task-5-brief.md`：固定路径、TDD、生命周期、提交边界。
- `.superpowers/sdd/2026-08-13-effilab-agent-kit-implementation-plan/task-5-context.md`：控制器对路径、Windows、env、lock 和范围的决议。
- `docs/plans/2026-08-13-effilab-agent-kit-host-architecture.md` §4、§8：产品嵌入路径合同与安全边界。
- `docs/host-acp-contract.md` §6：stdin 3.5 秒、TERM 2 秒、KILL 与 sidecar lock 所有权。
- `crates/efflab/efflab-agent-host/src/config.rs`、`runtime.rs`、`lib.rs`、`app_port.rs`：Host config、现有模块边界与 ScopeId 语义。
- `crates/efflab/efflab-agent-sidecar/src/hardening.rs`：sidecar 对 `.efflab-sidecar.lock` 的唯一持有实现。
- `crates/efflab/efflab-agent-sidecar/tests/common/process.rs`：既有平台 env_clear 白名单与测试进程生命周期风格。

### 已运行命令（关键）

- `cargo test -p efflab-agent-host --test supervisor`：实现前 RED（101）与实现后 GREEN（0）。
- `cargo test -p efflab-agent-host`：最终全量 host tests 通过。
- `cargo fmt --all -- --check`：最终格式检查通过。
- `git diff --check`：无空白错误。
- `rustup target list --installed`：确认 Windows target 未安装。

---

## Task 5 修复报告：Windows 盘符前缀路径边界

### 修复背景

复审发现 `sanitize` 先前允许 `C:temp`。该值在 Windows 上属于带盘符前缀、但不带根的路径组件，若传给 `Path::join`，可能丢弃 Host 已拼接的左侧根目录，破坏 `{home_root}/{app_id}/{scope}` 的稳定绝对路径合同。

### TDD Evidence

#### RED

先补充回归测试，再修改生产实现：

```text
cargo test -p efflab-agent-host --test supervisor sanitize_rejects_empty_traversal_separators_and_drive_prefixes
退出码: 101
失败断言: 空、遍历或含路径分隔符的组件必须被拒绝: "C:temp"
```

该失败符合预期：旧 `sanitize` 未将 `:` 视为非法组件字符，因此错误接受了 Windows 盘符相对前缀。

#### GREEN

将 `:` 加入 `sanitize` 的 fail-closed 拒绝条件，并补充中文注释说明其阻止 Windows `Path::join` 丢弃左侧根目录。随后运行：

```text
cargo test -p efflab-agent-host --test supervisor
退出码: 0
结果: 8 passed, 0 failed
```

### 变更内容

- `crates/efflab/efflab-agent-host/src/supervisor.rs`
  - `sanitize` 现在拒绝 `:`；该保守的跨平台规则覆盖 Windows 盘符前缀，同时保留原有空串、`.`、`..`、分隔符和 NUL 拒绝逻辑。
- `crates/efflab/efflab-agent-host/tests/supervisor.rs`
  - 通用非法组件表新增 `C:temp`，使非 Windows 开发机也能执行 RED/GREEN 回归。
  - 稳定路径测试继续断言 `home` 和 `workspace` 绝对、精确等于 `{home_root}/{app_id}/{scope}/{home|workspace}`，并新增两者都以派生 `app_id/scope` 根目录开头的断言。
  - 新增 `#[cfg(windows)] windows_rejects_drive_relative_app_id_and_scope`：分别断言 `app_id="C:temp"` 的构造失败，以及合法 app_id 下 `paths_for("C:temp")` 的 scope 失败；此用例与既有 Windows capability/kill API 用例共同在 Windows target 编译。

### 验证结果

```text
cargo test -p efflab-agent-host
退出码: 0
结果:
- acp_runtime.rs: 14 passed
- protocol_and_submission.rs: 21 passed
- supervisor.rs: 8 passed
- lib unit tests 与 doc-tests: 0 failed

cargo fmt --all -- --check
退出码: 0

git diff --check
退出码: 0
```

已运行 `rustup target list --installed`；本机仍未安装 `x86_64-pc-windows-msvc`，因此按任务要求未执行该 target 的 `cargo check`，也没有下载 target。Windows target 安装后应执行：

```text
cargo check -p efflab-agent-host --target x86_64-pc-windows-msvc
```

### 自审与范围

- `:` 拒绝发生在 `Supervisor::new` 的 app_id join 前、以及 `paths_for`/`acquire` 的 scope join 前，因此两个公开路径入口均 fail-closed。
- 未修改 `AcpRuntime`、真实 spawn、L3b、TOML、sidecar lock、child env 或任何 Task 6+ 内容。
- 未发现新的已知问题；唯一环境限制仍是本机缺少 Windows target 的实际编译验证。

提交：`fix(host): reject windows drive-prefix app_id and scope`（本修复报告与该提交一并提交；未 push）。
