# efflab-agent-sidecar 开发计划（终审定稿）

> 日期：2026-08-11 · 分支：`lab_main` · 基准 `SOURCE_REV=a51a1dc62fe20029ac39a665985bba78edbb870f`
> 来源：gpt-sol 终审（software-architect）对方案 v2 的最终评审 + 开发计划。方案文档：`docs/plans/2026-08-11-efflab-agent-sidecar-poc.md`（v3 合入本计划）。
> 里程碑命名：**macOS isolated runtime integration POC**（不宣称完整评估报告 §14 Go）。

---

## 1. 终审结论

**Go with changes** —— 满足以下范围可进入实施：

- **Go**：macOS、可信 Host、隔离环境下的 runtime 集成 POC。
- **No-Go**：v2 原样不可实施（4 个 Blocker）；不可宣称完整阶段 0 / 生产迁移 Go。

### 1.1 四个 Blocker（v2 原样实施会失败）与定稿决策

| # | Blocker | 定稿决策 |
|---|---|---|
| B1 | 嵌入的 AgentDefinition 无运行时装载路径（`agent_profile_path` 需文件路径；`GROK_AGENT` 仅绝对路径直接读文件，普通名称发现失败回退 `grok_build_plan()`） | **启动期原子物化**：`include_str!` 嵌入 → 原子写入 `GROK_HOME/agents/efflab-default.md` → `agent_profile_path` + `[agent].definition` + `GROK_AGENT`（绝对路径）三处指向同一物化文件 |
| B2 | `efflab:noop` 无法注册（`ToolNamespace` 封闭枚举，无 Efflab，`crates/codegen/xai-grok-tools/src/types/tool.rs:32-46`） | 占位工具固定 **`GrokBuild:efflab_noop`**（short ID `efflab_noop` + `ToolNamespace::GrokBuild`）；不改核心 `ToolNamespace` |
| B3 | `storage_mode="local"` 不会从 config.toml 解析（`StorageMode::resolve` 优先级：`ctx.storage_mode` → `GROK_STORAGE_MODE` → remote → Local，`xai-grok-shell/src/config/mod.rs:966-991`） | `RuntimeResolutionContext.storage_mode = Some("local")`；启动前清 `GROK_STORAGE_MODE`；resolve 后断言 `StorageMode::Local`；删除 config.toml 中无效 storage 字段 |
| B4 | 「session/created 后枚举完整工具集」无对应 ACP API（`NewSessionResponse` 不含工具表；`snapshot_tool_definitions()` 是 pub(crate)） | 改为**组合证明**：① 静态：AgentDefinition + registry 精确 `[GrokBuild:efflab_noop]`；② 动态：`x.ai/mcp/list` 校验 server/tool 精确集合（`<server>__<tool>`）；③ 集成测试捕获发往模型的 request body 断言 `tools` 精确集合。不新增 shell 公共 API（如需生产级快照 API 另立 ADR） |

### 1.2 其余修订（Major 级，已合入本计划）

- **R5'** 阶段 0 **删除 `--tools` / `--allowed-tools`**（防止重新打开 Bash/FS/Git/Web）；内置工具固定 `{GrokBuild:efflab_noop}`；app 只能提供受控 MCP server。
- **R6'** `GROK_HOME` 只接受 `--grok-home` / `EFFLAB_GROK_HOME`（绝对路径）；**不继承**通用 `GROK_HOME`；拒绝 `~/.grok` 及 workspace 内路径；在任何 shell API / Tokio runtime 前设 env。
- **R7'** Host 契约用**字段白名单**（非黑名单）：initialize terminal/fs capabilities=false；`session/new` cwd 精确匹配、`mcpServers=[]`、meta 白名单；拒绝 `agentProfile`/`pluginDirs`/`x.ai/hooks`/yolo/capability/cwd 覆盖；`session/load` 同规则；未知 meta 默认拒绝；sidecar stdin 只连可信 Host。
- **R8'** CWD 隔离加强：process CWD 与 `session/new.cwd` 为同一 canonical 绝对路径；目录 Host 创建、0700；启动前拒绝 `.git`/`.grok`/`.mcp.json`/`.claude`/`.cursor`/`AGENTS.md`；测试非 git 目录恶意 `.mcp.json`。
- **R9'** config 原子性与并发：每次**完整重建**受控 config（不 merge 旧字段）；目录 0700、文件 0600；临时文件 → `sync_all` → rename → 父目录 `sync_all`；`fs2` 独占锁 `.efflab-sidecar.lock` 至进程退出；同 home 并发拒绝；存在私有 `managed_config.toml`/`requirements.toml` 拒绝；写入后重读 TOML + 再断言 `resolve_remote_fetch_enabled()==false`。
- **R10'** MCP 输入唯一语法：`--mcp-config <ABSOLUTE_TOML_PATH>`；只允许 `[mcp_servers.<name>]`；拒绝重复名/空 command/url/未知顶层键；stdio command 绝对路径且位于 `--mcp-exec-root` 内；阶段 0 拒绝 env；HTTP 仅 loopback。
- **R11'** 显式关闭：`[managed_mcps] enabled=false`、`gateway_tools_enabled=false`、`[memory] enabled=false`；清 `GROK_MANAGED_MCPS_ENABLED`/`GROK_MANAGED_MCP_GATEWAY_TOOLS_ENABLED`/`GROK_SUBAGENTS`/全部 18 个 `GROK_{CURSOR,CLAUDE,CODEX}_*_ENABLED`；resolve 后断言全部关闭。
- **R12'** stdio 契约：stdout 仅 ACP JSON-RPC；tracing subscriber 固定 stderr；Host 并发 drain stderr；正常关闭：关 stdin → 等 3.5s；超时 TERM → 2s KILL；退出码：正常 EOF=0、启动策略拒绝=2、runtime 错误=1；stdout 非 JSON-RPC 行按协议污染处理。
- **R13'** 里程碑命名：**macOS isolated runtime integration POC**；Windows/双协议/OS canary/预算/许可证列为「完整阶段 0 Go」独立里程碑。
- **R14'** 测试与工程：`CARGO_BIN_EXE_efflab-agent-sidecar` 用 `std::env::var_os` 读取并断言；集成测试 spawn 局部 clippy allow（注明总会 wait/kill）；开发循环只跑定向 check/test/clippy；`cargo check --workspace` 仅 P4 门禁；`fork-sync-apply.sh` 固定根 `scripts/`，支持 `--check`/`--apply`，幂等。

---

## 2. 里程碑

| 里程碑 | 验收 |
|---|---|
| M0 可编译骨架 | 定向 check/clippy；无 TUI；stdout 纯净；依赖图无 pager |
| M1 密封启动 | private home、锁、原子 config、remote fetch false、全部 runtime 断言 |
| M2 精确工具 | 物化 agent 生效；唯一内置工具 `GrokBuild:efflab_noop` |
| M3 ACP/MCP 闭环 | Host guard、MCP list、模型实际 tools、echo tool call、生命周期 |
| M4 fork-ready POC | release build、网络证据、同步脚本、workspace check、文档与许可证记录 |

M4 = **macOS isolated runtime integration POC 通过**（≠ 完整 §14 Go）。

---

## 3. 分阶段开发计划

### P0：骨架与定向编译

**目标**：workspace 可识别、无 TUI、stdout 不污染的最小 sidecar。

**任务 0.1 创建 crate manifest**：新建 `crates/efflab/efflab-agent-sidecar/{Cargo.toml, src/lib.rs, src/main.rs}`。
依赖：`xai-grok-shell`、`xai-grok-tools`、`xai-tool-runtime`、`xai-tool-protocol`、`xai-grok-config-types`、`schemars`、`serde`、`serde_json`、`toml`、`tokio`、`anyhow`、`clap`、`tracing`、`tracing-subscriber`、`dirs="6"`、`dunce`、`fs2`、`tempfile`。保留 `edition.workspace=true`、`license="Apache-2.0"`、`[lints] workspace=true`。
**验证**：`cargo metadata --no-deps`；`cargo check -p efflab-agent-sidecar`。
**完成标准**：package 可识别；依赖图无 `xai-grok-pager(-bin)`；无核心源码修改。

**任务 0.2 同步脚本加入 workspace**：新建根 `scripts/fork-sync-apply.sh`（`--check` 只查不写 / `--apply` 幂等插入 member / 重复 member 失败 / 检查 manifest 存在 / 打印 SOURCE_REV / 不触碰 `crates/efflab/`）；根 `Cargo.toml` 仅新增一个 member。
**验证**：连续两次 `--apply` 第二次无 diff；`--check` 返回 0；`git diff -- Cargo.toml` 只有 member 一行。

**任务 0.3 进程入口和 tracing**：同步 `fn main() -> std::process::ExitCode` + 异步 `pub async fn run(config: SidecarConfig)` + `init_tracing()`（stderr writer）。不用 `#[tokio::main]`；CLI/env 解析与全部 env mutation 在 runtime 创建前；stdio 路径禁 `println!`；退出码 0/2/1。
**验证**：`--help`；启动后 stdout 首行只能是 ACP 输出或空；`cargo clippy -p efflab-agent-sidecar --all-targets -- -D warnings`。

**P0 门禁**：定向 check/clippy 通过；stdout 无 banner；根 Cargo 仅 1 个新增 member；SOURCE_REV 匹配。

---

### P1：配置落盘与 hardening

**目标**：在 shell 首次访问全局路径前，完成不可绕过的私有配置、锁、权限与 runtime 字段强制。

**任务 1.1 定义 SidecarConfig**（`src/sidecar_config.rs`）：
- `struct Cli` / `struct SidecarConfig` / `struct ApprovedMcpConfig`；`SidecarConfig::from_cli`；`load_mcp_config`。
- 固定 CLI：`--stdio`（默认 true）、`--grok-home <ABS>`（或 `EFFLAB_GROK_HOME`，必填二选一）、`--session-cwd <ABS>`（必填）、`--mcp-config <ABS_TOML>`（可选）、`--mcp-exec-root <ABS_DIR>`（有 stdio MCP 时必填）。
- **不提供**：`--tools`、`--allowed-tools`、任意 config merge 参数。
- MCP 校验：只允许 `mcp_servers` 顶层键；name 非空唯一；stdio command 绝对且在 exec-root；阶段 0 拒 env；HTTP 仅 loopback；拒绝空 command/url。
- 单测负例：相对 GROK_HOME / `~/.grok` / 重复 MCP 名 / 相对 command / 非 loopback HTTP / 未知顶层键 → 全部拒绝。

**任务 1.2 私有 home、锁和原子写**（`src/hardening.rs`）：
- `prepare_private_home`、`acquire_home_lock`（`fs2` 独占锁至进程退出）、`atomic_write_private`（临时文件→sync→rename→父目录 sync）、`render_authoritative_config`。
- home/session cwd 0700；config/agent 文件 0600；不 merge 旧 config；存在私有 managed/requirements.toml 拒绝；session cwd 为空且无项目配置入口。
- 权威 config 至少含：`[features] remote_fetch=false`、compat 全部 cell false、`[subagents] enabled=false`、`[managed_mcps] enabled=false`、`gateway_tools_enabled=false`、`[memory] enabled=false`、`[skills] paths=[]`、`[agent] name="efflab-default"`、`[agent] definition="<物化绝对路径>"`、受控 `[mcp_servers]`。

**任务 1.3 env 卫生与 OnceLock 时序**（`main.rs` + `hardening.rs`）：
顺序：解析 CLI → canonicalize → 获取锁 → 物化 config/agent → 清 OTEL/compat/subagent/storage/managed-MCP env → 设最终 `GROK_HOME` → 设绝对路径 `GROK_AGENT` → `set_current_dir(session_cwd)` → 创建 runtime → 异步组装。
负例：恶意 `GROK_CURSOR_MCPS_ENABLED=true` 启动仍关；`GROK_STORAGE_MODE=writeback` 最终仍 Local；同 home 二次启动非 0；写中断不留半文件。

**任务 1.4 组装与 post-resolve 断言**：
- `build_agent_config` + `assert_hardened`；调用链：`load_effective_config` → `new_from_toml_cfg` → 强制 `agent_profile_path` → `resolve_runtime_fields{is_headless:true, cli_subagents:Some(false), cli_no_memory:true, disable_web_search:true, storage_mode:Some("local"), remote_settings:None}`。
- resolve 后断言：`resolve_remote_fetch_enabled()==false`、`storage_mode==Local`、`subagents_enabled==false`、`managed_mcps_enabled==false`、`memory_config.is_none()`、`disable_web_search==true`、compat 全 false、profile path=物化文件、MCP server 集合精确匹配。

**P1 门禁**：不能使用 `~/.grok`；原子物化与权限测试通过；同 home 并发 fail-closed；resolve 后全部安全字段断言通过；恶意 env 不能重开 capability。

---

### P2：占位工具与 AgentDefinition

**目标**：可编译、精确唯一、不可被默认注入扩张的内置工具集。

**任务 2.1 占位 Tool**（`src/toolset.rs`）：
- `EfflabNoopTool` / `EfflabNoopArgs` / `EfflabNoopOutput`；`Tool::id()="efflab_noop"`；`ToolMetadata::tool_namespace()=ToolNamespace::GrokBuild`；`kind()=ToolKind::Other`；`run()` 无副作用固定返回；实现 `From<Args> for ToolInput`、`From<Output> for ToolOutput`。
- `register_efflab_tool_pack()` + `register_tools(builder)`；`std::sync::Once` 保证单次注册。

**任务 2.2 注册顺序与 ID 校验**：env/config 准备后、任何 `ToolRegistryBuilder::new`/Agent build 前调用注册；注册后 `known_tool_ids()` 含 `GrokBuild:efflab_noop`；重复调用不重复注册；`ToolConfig::from_id("GrokBuild:efflab_noop")` 通过 build。

**任务 2.3 物化默认 AgentDefinition**（`assets/efflab-default-agent.md` + hardening）：
frontmatter 固定：`injectDefaultTools:false`、`agentsMd:false`、`discoverSkills:false`、`inheritSkills:false`、`promptMode:full`、`toolConfig.tools=[GrokBuild:efflab_noop]`；不用 `tools` 字段；无 MCP inheritance/hooks/memory/subagent。
验证：`AgentDefinition::parse(include_str!())` 与 `from_file(物化路径)` 均成功且字段相等；工具列表精确一个；错误 ID 负例拒绝启动。

**P2 门禁**：ID 固定 `GrokBuild:efflab_noop`；注册先于首次 build；AgentDefinition 从实际物化路径加载；`injectDefaultTools=false` 且工具精确一个。

---

### P3：Host 契约、集成测试与验证

**目标**：可信 Host + sidecar + 本地模型桩 + approved MCP 完整链路 + 负向安全行为。

**任务 3.1 Host 契约**（`src/host_contract.rs` + `tests/fixtures/host_contract_cases.json` + `tests/host_contract.rs`）：
`HostPolicy` + `validate_host_request(method, params, policy)`；initialize terminal/fs=false；cwd 精确匹配；client MCP 空；meta 白名单；拒绝 agentProfile/pluginDirs/x.ai/hooks/yolo/capability；未知 meta 拒绝；modelId 白名单；session/load 同规则。生产 Host 非 Rust 时用同一 JSON fixture 跑其语言版本。

**任务 3.2 测试进程监督器**（`tests/common/process.rs` + `tests/common/acp_client.rs`）：
`CARGO_BIN_EXE_efflab-agent-sidecar` 取路径；stdout/stderr 分开读；stdout 每行必须合法 JSON-RPC；stderr 后台 drain；子进程总会 wait/kill；spawn 局部 clippy allow。
生命周期：关 stdin → 3.5s 内退出 0；超时 TERM → 2s KILL；非法配置 ACP 握手前非 0；异常 EOF → runtime failure。

**任务 3.3 mock MCP 与本地 SSE 桩**（`tests/common/mock_mcp.rs`、`tests/common/sse_stub.rs`、`tests/acp_stdio.rs`）：
SSE 桩 + mock MCP（server 名 `echo`）→ `--mcp-config` 注入 → ACP initialize → Host 校验 + session/new → `x.ai/mcp/list` → `echo` ready → 精确工具名 `echo__echo` → 发 prompt → SSE 返回 tool call → MCP echo 回填 → turn 完成。

**任务 3.4 实际工具集证明**：静态（AgentDefinition+registry 精确 noop）+ 动态（SSE 捕获模型请求 `tools`）：精确含 `GrokBuild:efflab_noop` 与 `echo__echo`；不含 bash/terminal/read/write/edit/git/web/task/memory/plugin；`x.ai/mcp/list` server 集合精确 `{echo}`；MCP tool change 后重核。

**任务 3.5 恶意输入负例**：agentProfile / client MCP stdio command / pluginDirs / x.ai/hooks / terminal-fs capability / yoloMode / cwd 替换 / 非 git CWD `.mcp.json` / `.grok/config.toml` 注入 / compat env=true / 未知 built-in ID / 同 home 并发 / 非 loopback MCP HTTP / MCP command 越 exec-root。由 Host guard 拒绝（不误描述为 sidecar 内部拒绝）。

**任务 3.6 网络与 SSE 协议验证**：`: keepalive`/SSE comment 不进业务事件；日志无 `/models`、`/v1/settings`；本地 sentinel 未收到；macOS 抓包无 `api.x.ai`/`grok.com`/`cli-chat-proxy.grok.com`；`GROK_EXTERNAL_OTEL`/`OTEL_*` 清理负例通过。

**P3 门禁**：模型请求 tools 精确匹配；MCP server/tool 集合精确匹配；危险 ACP meta 全被 Host 拒；mock MCP 完整调用链通过；stdout 协议纯净；退出语义符合契约；无隐性 x.ai 出网。

---

### P4：文档、同步脚本与交付门禁

**任务 4.1 同步脚本完成**（根 `scripts/fork-sync-apply.sh` + `FORK_BASE_REV`）：比较 FORK_BASE_REV 与根 SOURCE_REV；`--check` 不写；`--apply` 幂等插 member；SOURCE_REV 变化提示必须跑 contract tests；成功验证后再更新 FORK_BASE_REV；不自动 merge/rebase/push。
**任务 4.2 文档定稿**：方案定稿 v3；crate README；Host ACP contract 文档；记录 stdout/stderr 契约、exit code/shutdown、私有 home 所有权、MCP 输入 schema、Host 字段白名单、SOURCE_REV 升级流程、当前不支持 Windows/Responses/WebSocket/OS canary。
**任务 4.3 许可证与打包**：Apache-2.0；分发含根 LICENSE；THIRD-PARTY-NOTICES 策略；release 二进制许可证扫描；产品命名不暗示 Grok 官方联名。
**任务 4.4 最终集成验证**（顺序）：fmt check → 定向 check → clippy → test（panic=unwind）→ release build → `fork-sync-apply.sh --check` → `cargo check --workspace`。

**P4 门禁**：同步脚本幂等；全部定向测试/clippy/release 通过；workspace check 通过；Host contract 与实际 Host 对齐；LICENSE/NOTICE 有记录；文档不把 macOS isolated POC 误写成完整生产 Go。

---

## 4. 依赖顺序与并行

**硬依赖**：P0 → P1/P2（先可编译）→ P3（依赖稳定 config/AgentDefinition/工具 ID）→ P4。
**P0 后可并行**：A=`sidecar_config.rs`+`hardening.rs`；B=`toolset.rs`+asset；C=Host contract fixtures。单一所有者：根 Cargo/workspace member=P0；`main.rs` 最终组装=P1；`tests/acp_stdio.rs`=P1/P2 接口冻结后；`fork-sync-apply.sh`=单任务写入。

---

## 5. 待实现时验证（非设计决策）

1. BYOK endpoint / API-key 的最终 env 名；本地 SSE 桩的 model 配置。
2. API-key 模式下 proactive refresh 是否始终不触网。
3. current-thread vs multi-thread tokio runtime（shell 内部要求，env 固化后确定）。
4. `CARGO_BIN_EXE_efflab-agent-sidecar` 在 package integration test 的可用性。
5. `cargo test --config profile.test.panic=unwind` 在当前 toolchain 是否生效。
6. `x.ai/mcp/list` ready 状态与 `tools_changed` 到达顺序。
7. 模型请求中工具 ID 的最终 wire spelling（以集成测试实测为准）。
8. macOS 抓包是否需要 CI runner 特权。
9. BYOK 自定义模型返回的 `agent_type`（绝对路径 GROK_AGENT 已阻断 strict-harness，仍需负例）。
10. 完整阶段 0 的 Windows / OS sandbox-canary / 资源预算 / NOTICE 打包归属。

---

## 6. Go 门禁清单

**A. 允许开始编码前**：8 项修订门禁写入定稿；固定 SOURCE_REV + Cargo.lock；确认里程碑为 macOS isolated POC；明确可信 Host 代码归属与 contract test 位置；确认 POC 不开放任意内置工具配置。
**B. macOS isolated POC 验收**：release build；私有 home/原子写/锁/权限测试；remote_fetch 磁盘断言；resolve 后 Local/memory-off/subagent-off/managed-MCP-off/compat 全关；模型实际 tools 精确匹配；Host contract 拒绝全部注入面；mock MCP 完整调用链；stdout 纯净 + 生命周期；抓包无 x.ai 出网；同步脚本幂等；workspace check 通过。
**C. 完整阶段 0 / 生产 Go 仍需**：Windows；Chat Completions+Responses；WebSocket/reconnect/cancel/timeout；Music MCP 只读/Preview；Core entitlement 与无直接 writeback；macOS/Windows OS canary；体积/冷启动/RSS 预算；LICENSE/NOTICE 分发验收。
