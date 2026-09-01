# Efflab Agent Kit 与 Host Runtime 历史架构记录

- **日期**：2026-08-13
- **状态**：历史记录 / 不可执行（2026-09-01）；不自我声明 production Go
- **当前入口（优先）**：Host ↔ sidecar 合同见 [`../host-acp-contract.md`](../host-acp-contract.md)，最小 runtime 设计真源见 [2026-08-28 minimal-runtime design](../../../ai_music_organizer_br/docs/superpowers/specs/2026-08-28-efflab-sidecar-minimal-runtime-design.md)。
- **历史记录提示**：本文保留旧版 Kit / Host 架构、决策、目标、命令、步骤和验证记录；全文不可作为规范、当前待办或实施步骤，不得直接照抄。与当前入口或 on-disk 源码冲突时，以当前入口和源码为准。
- **历史期间的协议约定**：本文曾把 `crates/efflab/efflab-agent-host/src/protocol.rs` 作为线协议机器真源；该表述只记录当时约定，不把本文重新提升为当前规范导读。
- **配套历史记录**
  - sidecar ACP 契约：`docs/host-acp-contract.md`（当前入口，不是本文的旧配套副本）
  - 实现计划：`docs/plans/2026-08-13-effilab-agent-kit-implementation-plan.md`（历史记录，不可执行）
  - sidecar POC：`docs/plans/2026-08-11-efflab-agent-sidecar-poc.md`（历史记录，不可执行）
  - UI 源：`../../../effilab-agent-web`
  - LLM 中转：`../../../effilab_agent_server`
  - 第一个产品 adapter：[`ai_music_organizer_br` PureLab 设计](../../../ai_music_organizer_br/docs/plans/2026-08-13-effilab-agent-web-purelab-pilot-design.md)
- **用户已定**
  - 前端继续在 agent-web 上开发，不对齐 grok TUI
  - Host Runtime **写在本仓库** `crates/efflab/`，不进各个 App
  - 其他产品嵌入 Kit，而不是各自再做一遍 ACP / 投影 / LLM 通道
  - **会话真源是 sidecar**：恢复 / 续聊 / 历史走 `session/list` + `session/load` + `updates.jsonl`；产品不建第二套 session/transcript 账本
  - 产品只负责套餐/BYOK/Relay 配置、挂面板、领域 MCP/Confirm
  - **产品里旧 Agent 路径（含 Rig）只作对照，对照完整段删除。** Kit 全部写新代码、新命令、新事件通道。不为旧路径做兼容、对账、迁历史、共用表。删的时候只删产品旧入口与旧实现，不改 Kit 合同。

## 1. 要解决的问题

若每个桌面产品自己写「web → Rust command → 转发 ACP → sidecar → LLM」，第二个 App 会重复：

- sidecar 拉起、stdio、host_contract、supervisor
- session / `submission_id` / 事件投影
- BYOK / Relay
- agent-web 接线

那不是嵌入，是每个产品再做一次 Agent Host。

目标：**Kit 做厚，产品做薄。** 第二个 App 只挂面板、提供订阅/BYOK、给 `scope_id`、注册领域 MCP。会话恢复不由产品实现。产品 adapter **禁止**组装 `AcpClient` / `session/prompt`。

对外口径：M1 是 **Kit 试点 / 对照壳**，不是「智能助手上线」。工程 M1 与产品发布承诺分开写，见第一个 adapter 文档。

## 2. 仓库与 crate 布局

```
effilab-agent                          # 本仓库：运行时真源
├── crates/efflab/efflab-agent-contract  # 新增：无 grok-shell 的校验 / DTO / 渲染
├── crates/efflab/efflab-agent-sidecar   # 隔离 ACP 进程（最小 runtime；仅依赖 contract 与已验证的运行时组件）
├── crates/efflab/efflab-agent-host      # 新增：通用 Host Runtime（只依赖 contract）
└── docs/

effilab-agent-web                      # UI 包真源（无 ACP、无 Connect 客户端）
effilab_agent_server                   # Relay：OpenAI Chat Completions + 订阅
<product-app>                          # 只写 Adapter：HostApp + KitEventSink + 挂载 + scope + MCP + Confirm
```

**依赖方向（gpt-sol 终审后冻结，禁止 Task 1 先链整包再拆）：**

| crate | 依赖 | 禁止 |
|---|---|---|
| `efflab-agent-contract` | serde / json / toml / anyhow 等小依赖 | `xai-grok-*`、`agent-client-protocol` 运行时、tokio 进程 |
| `efflab-agent-sidecar` | contract + 最小 ACP/HTTP/session runtime 依赖（**仅此二进制**） | 被 host 或产品 `use` / 链进 App；`xai-grok-shell` 完整闭包 |
| `efflab-agent-host` | **只** contract + ACP 客户端类型（若需要，只引 schema crate）+ 进程监督 | `efflab-agent-sidecar` 库、`xai-grok-shell`、`xai-grok-tools` |
| 产品 | host crate + sidecar **二进制** + `@efflab/agent-web` | `efflab-agent-sidecar` 库、`xai_grok_*`、ACP 类型 |

contract 装：`validate_host_request`、`ApprovedMcpConfig`、`RuntimeConfigV1`、`render_runtime_config_v1`、`load_runtime_config_v1` 与统一的 server/tool validator。**Host 是 `<home>/runtime-config.v1.toml` 的唯一写盘 owner**：spawn 前由 Host 调 contract renderer 原子物化完整 v1 配置，并以 `--runtime-config` 传给 sidecar；sidecar 只读校验，缺失、malformed 或不合规时以退出码 2 拒绝启动，绝不回退旧 `config.toml` 或自行补默认配置。禁止 Host 与 sidecar 各写一份渲染逻辑。sidecar `initialize` 返回的 `mcpCapabilities.http=false` / `sse=false` 只表示 ACP 不广告 transport；sidecar 仍消费 Host 审批的 loopback HTTP MCP，不能据此删除或绕过 `approved_mcp`。

**grok-shell 不得编进产品进程。** 它只活在 sidecar **可执行文件**里。host / `rust_lib` / `src-tauri` 的 `cargo tree` 不得出现 `xai-grok-shell` / `xai-grok-tools`。产品 grep 门禁除 ACP 字符串外，还禁 `efflab_agent_sidecar::` / `xai_grok_`。sidecar 里的 Shell / 通用 FS / Git / Web **能力关闭**（`injectDefaultTools: false`），不是「编进来再用」。

crate 形状（边界，实施按实现计划落文件）：

```
crates/efflab/efflab-agent-contract/   # 无 grok；fixture 与 sidecar 共用
crates/efflab/efflab-agent-host/
  src/
    lib.rs
    protocol.rs        # KitCommand / KitReply / KitProductEvent / KitError（运输无关；机器真源）
    runtime.rs         # HostRuntime::dispatch —— 产品只调这里
    projector.rs
    acp_runtime.rs     # 出站 request/notify + 入站 Response/Notification/**Request** + reply
    supervisor.rs
    llm_channel.rs     # Byok | Relay；M1 走 L3b 回环，用户 Key 不进 sidecar env
    app_port.rs        # HostApp（领域）
    event_sink.rs      # KitEventSink（运输）
    config.rs          # HostRuntimeConfig
    secret.rs
```

`host_contract` 继续是 **发送前白名单库**。sidecar 入站不再验一遍；因此 Host 必须是 stdin 的唯一写入者。`AcpRuntime` 的 `request_validated` / `notify_validated` / `reply_validated` 是 **唯一** 写 stdin 的入口，禁止 `pub` 裸 write；reply 必须支持 `ValidatedReply::Result` 与 `ValidatedReply::Error`，并按保存的反向 request method/params 校验。

Supervisor 分阶段：Task 5 只交付路径/sanitize/scope slot/Windows unavailable/child lifecycle 与独立 process-slot metadata，不注入 `EFFLAB_L3B_BIND`、不写 models TOML、不做完整 spawn。sidecar 继续唯一持有 `.efflab-sidecar.lock`；Host 禁止竞争同一把 home lock。Task 7 依赖 Task 5 + Task 2，按“L3b 监听 → token 注册 → Host 写 config → spawn”完成接线。

根 `Cargo.toml` 仍是生成物，新 member 走 `scripts/fork-sync-apply.sh`。

## 3. 协议分层（所有产品共用）

不要把 web↔Host 叫某个 App 的私有协议。那是 **Kit 产品协议**。Tauri 只是一种运输，不是协议本身。

```
[L0] 视图            @efflab/agent-web
                     AgentPanel + useAgentKit({ scopeId }) + 归约器
                       │  不发 ACP；不手写 invoke / listen / 引导
[L1] Kit 产品协议    版本化 JSON：KitCommand / KitReply / KitProductEvent / KitError
                       │  protocol.rs 为机器真源；TS 对拍或生成
[L2] 运输            Tauri 8 个 agent_kit_* + agent-kit-event  |  测试内存总线
                       │  命令名属于运输绑定；payload schema 不属于 Tauri
[L3] HostRuntime     dispatch(KitCommand) → KitReply（返回时机见 §6；Send 不等 prompt result）
                       ├─ [L3a] ACP stdio          Host ↔ sidecar（含反向 request）
                       ├─ [L3b] LLM 出口           sidecar → 127.0.0.1 Host → BYOK/Relay
                       ├─ [L3c] HostApp + Sink     密封 / emit / mcp /（以后）confirm
                       └─ [L3d] 进程监督           一 ScopeId 一进程；每 scope 一个 IO actor
[L4] sidecar 内部    updates.jsonl / session/load / 工具循环
[L5] 领域            产品 MCP（只读）+ 产品 Confirm（写，不经 sidecar）
```

| 层 | 协议 | 所有者 | 产品要不要改 |
|---|---|---|---|
| L0 视图 | props + `useAgentKit` + 归约器 | `@efflab/agent-web` | 否（只挂面板、给 `scopeId`） |
| L1 Kit 协议 | command / 事件 / 错误 | host `protocol.rs` | 否 |
| L2 运输 | 8×`agent_kit_*` + `agent-kit-event` | 产品一行 serde 转发；host **无**默认 `tauri` feature | 换壳才改 |
| L3a ACP | JSON-RPC stdio | sidecar + host | 否 |
| L3b LLM | Chat Completions（本机回环） | Host 出口 + Channel | 否（只配通道） |
| L3c HostApp | Rust trait | 产品 | **是（领域端口）** |
| L3c Sink | emit | 产品运输层 | **是（一行 emit）** |
| L4 会话存储 | `updates.jsonl` / `summary.json` | sidecar | 否 |
| L5 MCP / Confirm | MCP + 产品 API | 产品 | **是** |

web 不讲 ACP，也不讲 Connect/protobuf。Connect 只是 agent-web 复刻对象的原厂线，Kit 不实现。

会话分工：

- sidecar：`updates.jsonl` / `summary.json` / `session/new` / `session/load` / `session/list` 是对话历史与续聊的唯一真源。
- Host：翻译 live 与 replay；内存 `submission_id` 只防同一进程内同一点击重复 `session/prompt`。**不**在产品 SQLite 再建 session / transcript / receipt。
- 产品：不实现恢复算法，不读 `updates.jsonl`，不持久化 Kit `session_id` 当账本。
- 旧产品 Agent：对照期可并列显示，**零共享**。对照结束即删，Kit 合同不变。

ACP 比 Connect 更适合 Host↔sidecar。ACP **不是** web 协议。

### 3.1 扩展纪律

同主版本（`schema_version = 1`）只做加法。四条方向**相反**，不要混用：

```text
Kit 请求     ：Host 忽略未知字段；新可选参数靠缺省（旧 Host 不炸）
Kit 响应/事件：客户端忽略未知字段与未知 kind
ACP 出站     ：未知 method / 未知字段 fail-closed
ACP 入站     ：未知 update 跳过 + Status；反向 request 必须回复（见 §6.2）
```

- 新 command：加 `KitCommand` 变体 + `capability.features[]`；解码层必须把未知 `cmd` 保留为 `KitCommand::Unknown { cmd }`，再由 `dispatch` 返回 `KitError { code: "unsupported" }`。禁止在产品 adapter 用 `serde_json::from_value::<KitCommand>` 让未知命令先炸成 Tauri `Err(String)`。
- 新 `KitBlock.kind`：解码未知 kind → `KitBlock::Unknown { unknown_kind }`；再序列化固定为 `{"kind":"unknown","unknown_kind":"plan"}`。未知原始 payload **有意丢弃**，禁止前端把 Unknown 当扩展载体，也禁止闭集 enum + `deny_unknown_fields` 把整条事件丢掉。
- 改语义 / 删字段 / 改合并规则：`schema_version += 1`。
- 新 ACP method：先改 `docs/host-acp-contract.md` + fixture，再写 Host。禁止产品直接长 ACP。
- 客户端必须先 `get_capability`；`schema_version` 不匹配则拒绝发送。
- `kit_version` = host crate **semver**，与 sidecar 版本号无关。
- `KitError.code` 的 wire 类型是字符串。§5.4 列表是已知码清单，不是会让未知值反序列化失败的闭集；旧客户端遇到未知 code 仍保留并展示 `message`，且不按未知码自动重试。Rust 内部可用 enum + `#[serde(other)]` 辅助分类，但不能丢原始 wire 语义。

## 4. 产品嵌入合同

产品实现 **两个** 端口，并调用 `HostRuntime::dispatch`。禁止产品仓出现 `AcpClient`、`validate_host_request`、`session/prompt` 拼包、Chat Completions 客户端。

```rust
/// 领域端口。不含 emit（emit 是运输）。
pub trait HostApp: Send + Sync {
    fn app_id(&self) -> &str;
    fn persist_llm_channel(&self, cfg: &LlmChannelConfig) -> Result<()>;
    fn load_llm_channel(&self) -> Result<LlmChannelConfig>;
    fn seal_secret(&self, plain: &[u8]) -> Result<SealedSecret>;
    fn unseal_secret(&self, sealed: &SealedSecret) -> Result<SecretGuard>;
    fn mcp_for_scope(&self, scope: &ScopeId) -> Result<ApprovedMcpSpec>;
    fn mentions(&self) -> Option<&dyn HostAppMentions> { None }
}

/// 运输端口。Tauri / 测试总线 / 以后非 Tauri 各自实现。
pub trait KitEventSink: Send + Sync {
    fn emit(&self, ev: KitProductEvent) -> Result<()>;
}

/// 写回专项再实现；M1 不要假装可用。
pub trait HostAppConfirm: Send + Sync {
    fn on_preview(&self, preview: PreviewIssuance) -> Result<()>;
    fn confirm(&self, req: ConfirmRequest) -> Result<ConfirmOutcome>;
}

/// Host 封装的 @ / 以后命令。不走 grok-shell Skills / 斜杠。
/// 产品按 App 提供数据源；M1 PureLab = 当前音乐库列表。
pub trait HostAppMentions: Send + Sync {
    fn resolve_mentions(&self, scope: &ScopeId, ids: &[MentionId]) -> Result<Vec<ResolvedMention>>;
}
```

`HostAppMentions` 保持独立 trait，但通过 `HostApp::mentions()` 注入；`HostRuntime::new(app, sink, cfg)` 签名不变。Send 带 `mentions` 时，`app.mentions()` 为 `None` → `invalid_request`，否则调用 `resolve_mentions`。`capability.features` 含 `"mentions"` 当且仅当 `mentions().is_some()`，这也是 agent-web 是否展示 `@` 的后端真源。

Secret seam（M1）：产品实现 `seal_secret` / `unseal_secret` 即可；**可继续用现有密封**（PureLab 现网 SQLite envelope）。`SecretGuard` 禁止 Debug/序列化/Clone，生命周期最短化；unseal 失败 fail-closed，不回退内置模型。Byok 与 Relay 分槽分 salt。复制 SQLite 可能恢复 Key 是已知风险，**不挡** Task 1–7b / 试点接线。OS Credential Store（Keychain / Credential Manager）是发布前产品决策，另开，不进 M1 完成条件。

```rust
pub struct HostRuntimeConfig {
    pub home_root: PathBuf,      // 产品给 App Data **根**；Host 再拼 app_id
    pub sidecar_bin: PathBuf,    // 开发：CARGO 产物；打包：产品解析后注入。禁止 crate 内写死绝对 path
    // 现行 v1 不提供 mcp_exec_root；MCP 只通过 RuntimeConfigV1 的 loopback HTTP 审批集传递。
    pub idle_after: Duration,    // 默认 15min；in-flight 禁止 idle-kill
}

impl HostRuntime {
    pub fn new(app: impl HostApp, sink: impl KitEventSink, cfg: HostRuntimeConfig) -> Self { /* … */ }
    /// 返回时机见架构 §6。Send / Cancel / 热 Resume 不得等 session/prompt result。
    pub fn dispatch(&self, cmd: KitCommand) -> Result<KitReply, KitError> { /* … */ }
}
```

**进程内单例。** `new` 只在 App 启动（或测试 setup）调一次。Tauri：`app.manage(runtime)`，8 个 command 只 `State<HostRuntime>` 再 `dispatch`。测试：一份 Runtime 跑完整闭环。

禁止每条 invoke `HostRuntime::new`。否则 L3b 口、绑定令牌、`is_active`、热 resume 缓冲、`SubmissionMap`、一 scope 一进程全碎。进程退出靠 Drop 关 sidecar。

`home_root` 由 Host **强制**拼 `app_id`：

```
{cfg.home_root}/{sanitize(app_id)}/{sanitize(scope)}/home       → --home
{cfg.home_root}/{sanitize(app_id)}/{sanitize(scope)}/workspace  → --session-cwd
```

两个产品不得共享同一 GROK_HOME 树。不要相信调用方「已经含 app_id」。

产品侧清单：

| 做 | 不要做 |
|---|---|
| 依赖 `@efflab/agent-web`、`efflab-agent-host`、sidecar 二进制 | ACP 客户端、stdio、host_contract、sidecar **库** |
| 挂 `<AgentPanel />` + `useAgentKit({ scopeId })` | 产品自己折 `KitBlock → Message`；产品手写引导 / listen |
| 提供已授权的 `ScopeId` | 把曲库路径当 sidecar cwd；把任意字符串当 scope |
| 实现 `HostApp` + `KitEventSink` + 领域 MCP | Connect/protobuf、localhost:9999 |
| Confirm：以后 Preview 之后改文件 | 把 Key/token 交给前端 |
| 对照期可并列挂旧面板 | 为旧路径写兼容层、统一信封、迁历史 |

验收：**第二个产品的 diff 里不应出现 `session/prompt`、`AcpClient`、手写 JSON-RPC、Chat Completions 客户端、自写事件归约、`use xai_grok_`、`efflab_agent_sidecar::`。** 出现即 Kit 不够厚。

M1 验收 = **开发机 path 依赖 + `tauri dev`**，不是安装包 / notarization / MSIX。打包合同另开计划，见 §13。

## 5. Kit 产品协议（web 只认这个）

运输绑定（冻结，新代码，不复用任何旧产品 Agent 命令/频道）：

- M1 运输是 **8 个** `agent_kit_*` 命令 + 通道 `agent-kit-event`；PureLab 冻结受信窗口 label 为 `main`，其它窗口无权调用
- `feature = "tauri"` **默认关**。host crate 不依赖 `tauri`。产品 `src-tauri` 每个 command 只做：受信窗口/当前 scope 授权检查 + Kit JSON 解码（未知 `cmd` → `KitCommand::Unknown { cmd }`）+ `dispatch`。禁止 `serde_json::from_value::<KitCommand>` 把未知命令变成 Tauri `Err(String)`；不要再发明第九个 `agent_kit_dispatch`，也不要在 host 里再抄一份命令表。
- JSON 字段：L1 **全程 `snake_case`**
- 通道 payload：**就是** `KitProductEvent`，不套产品长任务 Envelope
- L0 TS 组件 API 可用 camelCase；**host-client（`useAgentKit`）负责 `sessionId ↔ session_id`**。`invoke` 载荷必须是 `KitCommand` 的 serde JSON。禁止 `rename_all = "camelCase"` 加回 `protocol.rs`。

### 5.1 serde 与 `KitReply`（gpt-sol 终审后冻结）

邻接 / 内标，禁止 Rust 默认 internally tagged `{"Send":{...}}`（TS 对拍极差）：

```text
KitCommand  {"cmd":"send","scope_id":"...","session_id":"...","submission_id":"...","text":"..."}
KitBlock    {"kind":"assistant","markdown":"...","streaming":true}
KitReply    {"kind":"send","accepted":true,"duplicate":false,"session_id":"...","turn_id":"...","submission_id":"..."}
KitError    {"code":"turn_in_progress","message":"...","retryable":false}
```

`KitReply`（internally tagged `kind`）：

| kind | 字段 |
|---|---|
| `capability` | 见 §5.4 |
| `send` | `accepted`, `duplicate`, `session_id`, `turn_id`, `submission_id` |
| `cancel` | `accepted` |
| `new_session` | `session_id` |
| `list_sessions` | `sessions`, `next_cursor?` |
| `resume_session` | `accepted`, `session_id` |
| `llm_channel_view` | `channel: { kind: "byok" \| "relay" \| null, key_present, token_present, model_selectable, base_url?, model_id? }`（无明文；外层仅有 reply tag `kind="llm_channel_view"`） |

`llm_channel_view` 的完整形状固定为：

```json
{"kind":"llm_channel_view","channel":{"kind":"byok","key_present":true,"token_present":false,"model_selectable":true,"base_url":"https://example.invalid/v1","model_id":"model-id"}}
```

未配置时 `channel.kind=null`、`key_present=false`。请求 `SetLlmChannel` 仍使用顶层 `kind: byok|relay`；那是命令字段，不与 reply tag 冲突。

`duplicate=true`：L1 命中同一 `(scope, session, submission_id)` 同指纹，**不得**再向 sidecar 发 `session/prompt`，仍回原 `turn_id`。这不是错误。

turn 级 `turn_id` **等于** `submission_id`（即 ACP `_meta.promptId`）。replay 的 turn 级块从 sidecar `_meta.promptId` 取值；session/process 级事件不伪造回合 id，见 §5.3。

### 5.2 命令（M1 核）

`send` / `cancel` / `resume` **必须带 `session_id`**。Host 可缓存「当前 session」，但 wire 上要有 id。一 scope 一进程 ≠ 一 scope 一会话。

| command | 入参 | 同步 `KitReply` | 说明 |
|---|---|---|---|
| `agent_kit_get_capability` | — | `capability` 或 `KitError` | 未配 Key → **错误** `llm_channel_unconfigured`，不是成功的 capability。见 §5.5 |
| `agent_kit_send` | `{ scope_id, session_id, submission_id, text, mentions? }` | `send` | 写入 `session/prompt` 后立即 `accepted`；**不等** prompt result。终态只走事件 `Status`。`mentions` 见 §5.8 |
| `agent_kit_cancel` | `{ scope_id, session_id }` | `cancel` | 发出 ACP **通知**后立即 `accepted`（无 result 可等）；UI 等投影停 |
| `agent_kit_new_session` | `{ scope_id }` | `new_session` | **等** `session/new` result；`KitReply` 必须带 sidecar 的 `session_id` |
| `agent_kit_list_sessions` | `{ scope_id, cursor? }` | `list_sessions` | **等** `session/list` result；**无 `limit` 字段**；禁止扫盘 |
| `agent_kit_resume_session` | `{ scope_id, session_id }` | `resume_session` | **热**：已 `is_active` → 禁止 `session/load`，立刻 `accepted`，再重放内存缓冲。**冷**：写出 `session/load` 后立刻 `accepted`（**不等** load result）；结束见 `replay_complete` |
| `agent_kit_get_llm_channel_view` | — | `llm_channel_view` | 无明文凭据 |
| `agent_kit_set_llm_channel` | 见下（**无 `scope_id`，产品全局**） | `llm_channel_view` | 一次性明文立即 seal；响应永不回显；成功后重启所有存活 scope |

`set_llm_channel` 请求（现在就进 schema，不要「预留」）：

```text
kind: byok | relay
base_url? / model_id? / relay_base_url? / app_key?
api_key? / access_token?     # 一次性明文；响应永不回显
client_request_id?           # 幂等
```

**缺省 = 整单不改，不是「改 URL 沿用旧 Key」。**

| 请求里有什么 | 行为 |
|---|---|
| 只有 `client_request_id` / 全空 | no-op，回当前 view |
| 只带 `api_key`（或 Relay 只带 `access_token`），URL/model/`kind` 与现网相同或未传 | 轮换秘密，保留原 URL/model |
| 任何 `base_url` / `model_id` / `relay_base_url` / `app_key` / `kind` 变化 | **必须**同时带对应明文（Byok=`api_key`，Relay=`access_token`），否则 `invalid_request` |
| 首次配置 | `kind` + URL + model + 明文，缺一不可 |

禁止：只改上游地址还拿已封存 Key 去打。切 Byok↔Relay 视为新通道，必须带新秘密。明文立刻 seal，响应 / 日志 / view 永不回显。上述 no-op、轮换、改 URL 必须带 Key 的业务语义在 Task 7 实现和红测；Task 1 只冻结 Set/Get 的 wire 形状并让 stub 返回 `unsupported`。

M1 **不注册**：`agent_kit_get_snapshot`、`agent_kit_prepare` / `agent_kit_confirm`。capability.features 不含它们。

`session` 摘要只暴露：`session_id`、`title`、`updated_at`（UTC ISO-8601）、`is_active`。不把 cwd / grok 内部 facet 传给 web。`title`：sidecar `session/list` 有则用；否则直接空串，由 UI 显示「新对话」。Host **禁止**扫盘或读取 transcript 猜首条 user 文本。

`is_active` = Host 认为该 session 已 `session/new` 或 `session/load` **且** 该 scope 进程还活着。list 来自磁盘时，idle-kill 后全是 `false`，直到 resume / 自动 load。

同一 `(scope, session)` 同时只允许一个 in-flight prompt；第二次 send 返回错误 `turn_in_progress`。  
`session_busy` = 进程槽不可用（正在 spawn / kill / 换 MCP），不是「已有 prompt」。

`submission_id`：不透明字符串，推荐 UUID v4；Host 不解析格式。  
`text` 空串或超过 `capability.limits.max_prompt_chars`（M1 = 32000）→ `invalid_request`。  
`text` 语义门见 §8。

### 5.3 事件

```text
KitProductEvent
  schema_version: 1
  scope_id
  session_id
  submission_id?       # turn 级 = 本轮 submission_id；session/process 级为 null
  turn_id?             # Option<String>；turn 级 = submission_id / promptId；session/process 级为 null
  event_id             # sidecar 事件优先 _meta.eventId；Host 合成见下
  sequence             # 每 session 单调（进程内）；每次 load 重放从 0
  origin: live | replay
  block_id
  block: KitBlock
```

`KitBlock`（M1 闭集 + 未知）：

- `user` / `assistant { markdown, streaming }` / `thinking` / `tool` / `error` / `retry` / `status` / **`unknown { unknown_kind }`**
- 线协议 `kind` 内标。未知 block kind 解码为 `Unknown { unknown_kind }`，再序列化固定为 `{"kind":"unknown","unknown_kind":"<原 kind>"}`。原始 payload 有意丢弃；UI 只画 Status 或跳过，禁止解释未知 payload，禁止整事件反序列化失败。Task 1 必须做双向 golden。
- 事件 `error` 块 = 同一 `KitError` shape（`code/message/details?/request_id?/retryable/retry_after_ms?`）。
- turn 级块（`user` / `assistant` / `thinking` / `tool` / turn 终态 `Status`）的 `turn_id` 与 `submission_id` 必填，且两者都等于 Kit `submission_id` / ACP `promptId`。
- session/process 级 `Status`（`replay_complete` / `mcp_failed` 等现行状态）的 `turn_id` 与 `submission_id` 均为 `null`。未知或禁用 update 只做内部计数和 debug 汇总，不产生 `replay_skipped` / `skipped_update` 可见事件；Host **禁止**伪造 synthetic turn id。
- Host 合成事件的 `event_id = "{session_id}:host:{code}:{sequence}"`，`block_id = event_id`；sidecar 投影事件仍优先复用 `_meta.eventId`。
- `Tool.status`：`pending | running | completed | failed | cancelled`。M1 无 Confirm，不要 `waiting` / Allow/Block。

合并规则：

- `Assistant.streaming=true`：`markdown` 为 **该 `block_id` 的累计快照**（不是 delta）。
- 同 `block_id` 后到的 `sequence` 覆盖先到的。
- `origin=replay`：web **整表替换**该 `session_id`，禁止 append 进 live。replay / 非当前 in-flight `turn_id` 必须按 `streaming=false` 渲染，禁止打字机。
- **replay 栅栏**：`resume` 的 `accepted` = 批次开始。冷路径：写出 `session/load` 后立即 `accepted`，sidecar 排空 replay 通知后再回 load result，然后 Host 发 `replay_complete`。热路径：不写 ACP，把该 session 的进程内缓冲按 `origin=replay` 重放完立刻 `replay_complete`。归约器见到新 replay epoch（`origin=replay` 且 sequence 从 0）丢掉该 session 的旧 live。`useAgentKit` ready 后再调 resume。
- Host 在 session `is_active` 期间必须留一份该 session 的事件缓冲（折后快照即可）。切栏卸挂会丢掉 React 状态；热 resume 靠这份缓冲补屏，**不要**为此再 `session/load`（会关 gateway，正在流的字会丢）。
- 热 resume 时若仍在 Prompting：缓冲按 `streaming=false` 重放；`replay_complete` 后再发一条 live Assistant 快照（`streaming=true`），不要打断 in-flight prompt。
- 已 attach 的 session 上再 `Resume` 同一 id = 热路径。Resume **另一个** session 时若当前 Prompting → `session_busy`（先 Stop）。
- replay/live 期间未知或禁用 update 仅内部计数并限频记录 debug 汇总；不分配 Kit 事件、不清空快照、不进入恢复缓冲。replay 批次只发 `replay_complete`。
- 一轮结束：`Status { code: "turn_completed" | "cancelled" | "error" }`。该 `turn_id` 一旦终态，后续同 turn 的 `origin=live` **丢弃**（迟到包是新 `event_id`，去重不够）。
- 用户气泡：面板可乐观插入，`block_id = submission_id`；sidecar user 回显用同一 `block_id` 去重。禁止两边各画一条。
- 线协议 M1 **不发射** `todo` / `subagent` / `plan`。

归约 `KitProductEvent* → Message[]` 与引导状态机 **属于 agent-web**（`reduceKitEvents` + `useAgentKit`），不属于产品。Task 8 禁止产品再喂 `messages?: Message[]` 当真源（该 prop M1 标 deprecated，第二产品清单禁止传）。

### 5.4 错误

command 失败与事件 `Error` 块同一 shape：

```text
KitError { code, message, details?, request_id?, retryable, retry_after_ms? }
```

`KitError.code: String`。M1 **已知** code 清单：

`sidecar_unavailable` / `llm_channel_unconfigured` / `missing_api_key` / `session_not_found` / `session_busy` / `turn_in_progress` / `cancelled` / `fingerprint_conflict` / `idempotency_conflict` / `rejected_by_host_contract` / `model_unavailable` / `llm_timeout` / `mcp_failed` / `replay_truncated` / **`unsupported`** / **`invalid_request`**。

- `unsupported`：未知 `cmd`（解码为 `KitCommand::Unknown { cmd }` 后由 dispatch 返回）；也用于 Task 骨架未实现命令，但 Task 7b 前不得接产品。
- `invalid_request`：空 text、超长、语义门拒、缺 `session_id`，或 Send 带 mentions 但 `HostApp::mentions()` 为 `None`。
- `missing_api_key` 只表示 view 声称有 Key、但 unseal 时秘密缺失或损坏；“从未配置”统一是 `llm_channel_unconfigured`。
- 客户端遇到未知 code 仍须成功解码并保留/展示 `message`，不得按未知码自动重试。已知码可以在内部分类，但 wire 不得因闭集 enum 丢事件。Task 1 必须有“未知 code 仍解码并保留 message”的 golden。
- 禁止 Tauri `Err(String)` 当 Kit 错误。`message` / 日志不得含 Key / token。
- M2 把 Relay 401/402/403/429 映为字符串 code；未知 reason fail-closed，但仍保留可展示的 `message`。

### 5.5 Capability

```text
{
  sidecar: "available" | "unavailable",
  reason?: "sidecar_hardening_unavailable",
  kit_version: string,          # host crate semver
  schema_version: 1,
  features: ["send","cancel","new_session","list_sessions","resume_session","llm_channel", "mentions"?],
  channel: { kind: "byok"|"relay"|null, key_present, token_present, model_selectable },
  limits: { max_prompt_chars: 32000 }
}
```

`reason` 只表示 **sidecar 进程**为什么不可用。未知 reason 当 `sidecar_unavailable`。未知 `features[]` 项客户端忽略。`features` 含 `mentions` 当且仅当 `HostApp::mentions().is_some()`。

**未配 Key / 未配通道不是 capability 成功。** 对话面一律 `KitError { code: llm_channel_unconfigured }`，文案「没有配置」；`missing_api_key` 仅用于 view 声称有 Key、但 unseal 时秘密缺失或损坏：

- `GetCapability` / `Send` / `NewSession` / `ListSessions` / `Resume` / `Cancel`
- **不 spawn**，不听 L3b，**禁止**掉进内置 `grok-4.5`

设置面除外：`GetLlmChannelView` 成功回嵌套 `channel` 空视图（`channel.kind=null`、`key_present=false`），否则设置页没法配；`SetLlmChannel` 负责写入。

Windows / hardening 不可用：`GetCapability` 回 `KitReply::Capability { sidecar: unavailable, reason: sidecar_hardening_unavailable }`（这是平台不可用，不是「额度/能力已就绪」）。既没 Key 又不可用：优先 `sidecar_unavailable`，不要先骗去填 Key。

`initialize` 入站 result 里 sidecar 广告的 `embedded_context` / MCP HTTP / `pluginDirs` / slash commands **一律丢弃**，禁止映射进本 capability。

### 5.6 幂等

```text
L1  Kit command   submission_id     防同一 Host 进程内 IPC 重放；重启后 UI 必须换新 id
L2  ACP           不假设 sidecar 幂等；重复 prompt = 新 turn
L3  Relay（M2）   Host 持久化 Idempotency-Key 至 turn 终态；落在 {home_root}/... Host 目录，不进产品 DB
```

`new_session` / `set_llm_channel` / 以后的 `confirm` 带 `client_request_id`。

指纹：`canonicalize(scope_id, session_id, text, mentions_sorted)` 的稳定字节。`mentions_sorted` 按 `(kind, id)` 字典序排序后进入 canonical input；key = `(scope_id, session_id, submission_id)`。同指纹 → `duplicate`；同 submission/text 但 mentions 不同也属于异指纹 → `fingerprint_conflict`。

### 5.7 Scope、路径与客户端引导

`ScopeId` 是不透明字符串，语义由产品定（PureLab = 已存在且当前用户已授权的 `library.id`）。Host **不把 ScopeId 当用户路径解析**。产品必须在 dispatch 前拒绝未知 / 未打开的库。

Supervisor 路径见 §4。`sanitize` 拒绝 `..`、空串、分隔符。库根 / 项目根 **不得**当 `session_cwd`。领域路径只进 MCP args。

**M1 客户端引导（`useAgentKit` 独占，产品不要再写一份）：**

```text
mount / 切栏再挂 → get_capability
  Err llm_channel_unconfigured → 「没有配置 / 去设置页」，不 spawn
  Err sidecar_unavailable 或 Capability.sidecar=unavailable → 不可用空态，不 spawn
  Ok capability → list_sessions
    一条或多条 is_active → resume(updated_at 最新；并列时取 list 顺序最后一条)  // 热：Host 重放内存，禁止 session/load
    无 is_active 且 list 空 → new_session
    无 is_active 且 list 非空 → resume(updated_at 最新；并列时取 list 顺序最后一条)  // 冷：session/load
之后 send / cancel 必须带当前 session_id

Host 重启 / idle-kill：is_active 全 false，走冷路径
  send(已知 session_id) 且进程没有它 → Host 先冷 load（等 replay_complete）再写 prompt
  自动 load 失败 → session_not_found

正确性靠上述 resume 分流，不靠面板 keep-alive。允许切栏闪一下。
```

刚 `new_session` 的空会话在同一进程里就是 `is_active`，切栏回来走热路径，**不会**被「最新非空」盖掉。只有进程没了才按磁盘 `updated_at` 冷恢复。

### 5.8 Host 封装能力（@ 与以后的命令）

`@` **不是** grok-shell 的文件引用。是 Host 封装、产品供数的能力。Shell / 任意路径 / Skills / 斜杠命令 **不开放**。

```text
Composer @
    → 产品按当前 App 弹出数据源（M1 PureLab = 当前音乐库列表）
    → 不能 @ 出库外文件、系统路径、其它 App 内容
    → send.mentions = [{ kind: "track", id: library_item_id }]
        │
        ▼
Host 先取 app.mentions()，再调 HostAppMentions::resolve
    → 未提供端口但 Send 带 mentions：invalid_request
    → id 必须属于该 scope 的库，否则 invalid_request
    → 扩成普通中文（曲名 / 艺人）；label 与展开文本禁止绝对路径、`@/`、`file://`
    → 展开后的完整文本再次通过 §8 / ACP §3.1 同一道 prompt_parser 对齐文本门
    → 再交给 sidecar text block
```

- 点选列表用产品已有检索 / 库表（当前 App 设计），M1 **不**为此加第九个 `agent_kit_*`。
- agent-web 的 `useAgentKit` / `AgentPanel` 接受可选 `searchMentions?: (q) => { kind, id, label }[]`；未提供 callback 时隐藏 `@`。候选 `label` 同样禁止绝对路径、`@/`、`file://`。
- 进模型的内容由 Host 校验、改写。这才是「Host 封装」。
- 以后 PureLab 高级命令（批量整理、写回预览等）同样走 **新的 HostApp* trait + `capability.features[]` + 新 Kit 命令**，在 PureLab 里慢慢加。不要去开 sidecar Shell / Skills。
- `HostApp::mentions()` 返回 `None` 的产品：capability 不含 `mentions`；Composer 不显示 `@`；若调用方仍发送 mentions，Host 返回 `invalid_request`。

## 6. Host Runtime 职责

`HostRuntime` **独占**：

1. `dispatch(KitCommand)`：产品唯一入口。每进程 **一份** `HostRuntime`。每 `ScopeId` **一个 IO actor** 独占 stdin/stdout。cancel 不得与 prompt 抢同一把阻塞锁。

   **返回时机（按命令，不要「一律入队即返回」）：**

   | 命令 | 何时回 `KitReply` |
   |---|---|
   | `Send` | 写入 `session/prompt` 后立刻。**禁止**等 prompt 的 JSON-RPC result |
   | `Cancel` | 写出 `session/cancel` 通知后立刻 |
   | `Resume` 热 | 立刻（无 ACP）；随后重放内存缓冲 + `replay_complete` |
   | `Resume` 冷 | 写出 `session/load` 后立刻。load result / replay 走事件 |
   | `NewSession` | **等到** `session/new` result，否则没有 `session_id` |
   | `ListSessions` | **等到** `session/list` result |
   | `GetCapability` | 未配通道 → 立刻 `KitError.llm_channel_unconfigured`；已配 → Host 本地立刻回 Capability |
   | `GetLlmChannelView` | Host 本地立刻回 view（可无 Key，给设置页） |
   | `SetLlmChannel` | 产品全局事务：seal + persist 后新配置即成为 committed view；先使全部旧 L3b binding token 失效，再 drain/重启**所有存活 scope**，等它们起来（或确认无需 spawn）才回 view。部分重启失败时仍以已 persist 的新配置为 committed view，并返回可重试 `KitError`，不得假装仍是旧通道 |

   `SetLlmChannel` / `persist_llm_channel` 都没有 `scope_id`；这不是 per-scope 设置。

   读循环始终独立跑。禁止「同一线程阻塞等 ACP result 而不 `poll_inbound`」。`NewSession` / `ListSessions` 等 result 时，通知仍进 projector。
2. 拉起 / 监督 sidecar（一 `ScopeId` 一进程；稳定 home/cwd）。Task 5 只做 slot/路径/lifecycle；Task 7 完整 spawn 时才在 `env_clear` 后只注入 **L3b 绑定令牌**，不注入用户 Key。
3. 发送前 `validate_host_request`；扩展 method **校验用** `x.ai/...`，**写入 stdin 用** `_x.ai/...`。
4. stdout 独立读循环：`Inbound = Response | Notification | Request`。`session/prompt` 期间通知与反向 request 必须进 projector / 回复器。
5. `session/cancel` 按 **JSON-RPC 通知**发送（无 id、无 result）。
6. ACP `session/update`（含 load 重放）→ Kit 事件。未知 / 禁用类型 → 见 §5.3。**出站**请求仍 fail-closed。
7. `_meta.isReplay` → `origin=replay`。`event_id` 复用 sidecar `eventId`。
8. `submission_id` 写入 ACP `_meta.promptId`。
9. 列会话只走标准 ACP `session/list`（`cwd` 强制等于该 scope 的 session_cwd；可选 `cursor`；**不传 `limit`**）。禁止扫盘。禁止 `_x.ai/session/list`。
10. LLM Channel：Host 通过 contract renderer 写入 RuntimeConfigV1 的 `[model]`、`approved_mcp` 与 `expected_tools`，backend 固定为 `chat_completions`。sidecar `base_url` 指向 Host L3b；不恢复旧 `[models].default` 或 `api_backend` 配置表。
11. capability：Windows sidecar fail-closed 仍报入口可见、不可用。`HostRuntimeConfig` / kill API 在 Windows **编译单元必须存在**，返回 `Unavailable`，禁止 `#[cfg(unix)]` 把类型摘掉。
12. 不提供默认 `feature = "tauri"`。

**状态机（每 scope）：**

```text
Absent → Spawning → Ready ⇄ Prompting
Ready → Replaying（load）→ Ready
任意非 Prompting → Idle(15min) → Stopping → Absent（保留 GROK_HOME）
Prompting 禁止 idle-kill
```

**cancel 竞态：**

- Host 对 `(scope, session)` 设 `cancel_requested`。
- 若 cancel 时无 inflight prompt：记住；prompt 写入 stdin **之前**若已请求则不发 prompt，只 emit `Status{cancelled}`。
- 若已发出：notify `session/cancel`，**仍等** prompt 的 JSON-RPC result（或进程死）才清 in-flight。禁止对 prompt 自动重试。
- Drop：in-flight 先 cancel，再关 stdin 3.5s → TERM 2s → KILL（Windows：Job Object + `TerminateProcess` + `CREATE_NO_WINDOW`；API 必须存在）。
- 孤儿 sidecar：子进程组 / parent-death kill。`{GROK_HOME}/.efflab-sidecar.lock` 的唯一 owner 是 sidecar 现有 home lock；Host 禁止抢同一把锁。Host 只维护独立于 home lock 的 process-slot metadata，用于 scope slot、pid/generation 与 child lifecycle；stale slot 回收不改变 sidecar home lock 所有权。

**反向 RPC（M1 就必须回，否则工具回合挂死）：**

```rust
enum ValidatedReply { Result(Value), Error { code: i64, message: String } }
fn reply_validated(&self, id: RequestId, reply: ValidatedReply, policy: &HostPolicy) -> Result<()>;
```

`AcpRuntime` 保存 `request_id → { method, params }`，reply 校验按保存的反向 method 与原始 params 执行。`session/request_permission` 回的是 ACP `Selected { optionId }`，**不是**字符串 `"allow_once"`；`optionId` 必须属于 **本次** inbound `options[]`。未知 inbound request 必须用 `ValidatedReply::Error` 回复 `method_not_found` 或契约指定固定错误，禁止静默丢。direct `session/request_permission` 与 `_x.ai/...` wrapper 两种 wire 形态都必须解包并测试。

| 情况 | Host 回 |
|---|---|
| 工具名 ∈ 批准集 ∪ `{GrokBuild:efflab_noop}` | 在 options 里找 `optionId == "allow-once"`（连字符）。**不要** `enable-always-approve`（kind 也是 AllowOnce）。回 `Selected` |
| 批准该批但 options 里没有 `allow-once` | `Cancelled`，禁止瞎选 `options[0]` |
| 不在批准集 / 未知工具 | 找 `reject-once`；没有则 `Cancelled` |
| 已 `cancel_requested` / 用户 Stop | 一律 `Cancelled` |
| `x.ai/ask_user_question` / `x.ai/exit_plan_mode` | 拒绝/取消；只增加内部 unsupported reverse-request 计数和 debug 日志，不生成可见 Status |
| 其它带 `id` 的未知 request | 错误 result，禁止静默丢 |

M1 不弹窗。自动批只读工具 ≠ `HostAppConfirm`，不把原始 ACP 甩给 web；可投影 `KitBlock::Tool`。reply 走 `reply_validated`。

**禁止**放进 host crate：产品 schema / 写回 / 商店验签 / React / Music MCP 实现。

RuntimeConfigV1 必须由 Host 通过 `render_runtime_config_v1` 原子写入 `<home>/runtime-config.v1.toml`，并由 sidecar 通过 `--runtime-config` 只读加载；缺失、malformed、未知字段或 revision 不匹配均拒绝启动。配置只允许固定的模型 loopback、Host 审批的 HTTP MCP 与 `expected_tools`，不包含用户凭据、command/args 或旧 shell 配置；stdio 条目统一返回 `stdio_mcp_unavailable`。

## 7. LLM Channel

产品设置页配的是 **用户大模型**（或以后自家 Relay）。sidecar 磁盘上的
`runtime-config.v1.toml` **不写那把 Key**。这是两套凭据，不要混。

```text
设置页 / HostApp（用户看得见）
  M1 Byok   endpoint + model_id + api_key     ← PureLab 现有设置卡；密封只在产品库
  M2 Relay  relay_base_url + app_key + token  ← 自家 LLM 订阅；与 Byok 分槽分 salt
                                              ADR-12：用户 Key 不经 Relay
        │
        │  agent_kit_set_llm_channel（明文只出现这一次，立刻 seal）
        ▼
Host 进程（unseal）
  L3b 回环  http://127.0.0.1:<port>/v1
        │  入站 Authorization = 绑定令牌
        │  出站 Authorization = 当前通道的用户 Key 或 Relay token
        ▼
sidecar RuntimeConfigV1（磁盘，无私密）
  [model]       base_url = 上面这个回环
                backend  = "chat_completions"
                token_env = "EFFLAB_L3B_BIND"  ← 只指向绑定令牌，不是用户 Key
  sidecar 环境  完整 spawn 时只注入 EFFLAB_L3B_BIND；禁止用户 Key
```

- 两通道都强制 `chat_completions`。Host 是 `runtime-config.v1.toml` 的唯一写盘 owner；sidecar 缺文件或校验失败以退出码 2 拒绝，不得自补默认模型。
- RuntimeConfigV1 的 `[model]`、`[approved_mcp]` 与 `expected_tools` 均为闭集字段；空 `expected_tools` 合法。Host 写入的 runtime config 不含用户 Key/Relay token，也不含 command/args。
- 空通道：不写模型配置，且 **不得 spawn 后允许 prompt**。
- 文档用词：**产品套餐** vs **Relay 额度**，不要都叫「订阅」。
- 通道是**产品全局**，`SetLlmChannel` / `persist_llm_channel` 不带 `scope_id`。M1 启用 Byok；Relay **类型第一天进** `LlmChannel`，`enabled=false`。切到 Relay = 改 Channel + 使全部旧 binding token 失效 + drain/重启所有存活 scope；sidecar 仍打同一个 L3b，Host 换出站目标。web / ACP / HostApp 形状不变。
- **用户 Key / Relay token 不写 TOML，也不进 sidecar 环境。** sidecar 只持短寿命绑定令牌；Host L3b 在出站到用户配置的模型 endpoint 时才使用产品解封的凭据。绑定令牌只证明「我是这个 scope 的 sidecar」，**不是**设置页那把大模型 Key。

RuntimeConfigV1 的 `model.token_env` 固定为 `EFFLAB_L3B_BIND`，仅用于 sidecar 读取 Host 注入的绑定令牌；这不是 ACP 能力广告，也不是用户凭据字段。sidecar `initialize` 的 `mcpCapabilities.http=false` / `sse=false` 只表示 ACP 不广告 transport；sidecar 仍消费 Host 审批的 loopback HTTP MCP。

**L3b 本机回环（M1 必做）：**

```text
sidecar base_url = http://127.0.0.1:<ephemeral>/v1
sidecar 环境     = 完整 spawn integration 时仅 EFFLAB_L3B_BIND；领域 MCP 仅允许 Host 审批的 loopback HTTP，不创建 stdio 子进程
Host 出口        = 只绑 127.0.0.1 / ::1，禁止 0.0.0.0
                 只转发 POST /v1/chat/completions；禁 CONNECT / 非 POST / 环境 HTTP(S)_PROXY
                 SSE/stream 逐块转发，禁止整包 buffer；下游断开或 Cancel 立即停止读上游
                 不跟随 302；请求体有硬上限；只转发必要 headers
                 入站 Bearer 只查 Host 注册表，不接受 sidecar 自报 scope/channel
未配置通道       = 不听端口、不 spawn
M1               = 出站打设置页的 BYOK URL + 用户 Key
M2               = 同一出口改打 Relay，注幂等头，映射 401/402/403
```

用户侧 `base_url`（设置页）校验：用户填写的 BYOK `base_url` 视为可信目标，只做「`Url::parse` 可解析、含 host、scheme 为 `http` 或 `https`」的语法校验；允许明文 HTTP、localhost / 回环、LAN / 私网 / metadata 地址以及 query / userinfo / fragment（按用户原文保存与回显）。出站按用户原文直连，每次请求解析 DNS 供连接使用，不再做 DNS/IP 安全分级审查；`allow_loopback_llm` 保留为兼容字段，不再决定 BYOK URL 保存与出站。L3b 自身监听仍只绑定 `127.0.0.1` / `::1`；API Key 仍即时密封、日志脱敏、view 不回显明文。

L3b 注册表固定为 `binding_token → scope slot → sidecar process generation → channel revision`；请求不得覆盖这些绑定。token 使用密码学随机数（至少 256 bit）、常量时间比较，日志只记不可逆短指纹。未知 token、旧 generation、抢锁后旧 token、sidecar 退出后的 token、通道变更前的旧 token都必须在 unseal 与任何上游请求前失败。

切 BYOK↔Relay：改全局 Channel + 使全部旧 token 立即失效 + **drain/重启所有存活 scope 进程**。磁盘会话还在，UI 走 list/resume。sidecar 的 `base_url` / `env_key` 形状不变。

launch 顺序：Host 先监听 L3b、注册本代 binding token、调用 contract 的 `render_runtime_config_v1` 原子写 `{home}/runtime-config.v1.toml`，再以 `--runtime-config` spawn sidecar。sidecar CLI 不增加用户 Key 参数；端口与模型只存在于 Host 写出的 RuntimeConfigV1 `model.base_url` / `model.model_id`。sidecar 只读校验，不回退旧 `config.toml`，也不启动 stdio MCP。

## 8. 安全边界

- React / agent-web 不理解 ACP。`@efflab/agent-web` 的 `react` / `react-dom` 是覆盖 18/19 的 peerDependencies，library build externalize React；CSS 必须根作用域隔离，导入后不得全局 reset 宿主 button/textarea。
- sidecar 最小 runtime 不编译 Shell / 通用 FS / Git / Web / 外部 MCP / Skills / Subagent / YOLO 能力；同时不读取旧 shell 配置或执行 workspace `.envrc`，而不是依靠运行时开关补救编译闭包。
- MCP 只来自 `HostApp::mcp_for_scope`；client `mcpServers` 必须 `[]`。改 MCP = 重启该 scope 的 sidecar。
- M1 只读 MCP，**可选**。产品可以不走 MCP（批准集空）；对话不依赖 MCP 起来。现行 v1 仅允许 Host 审批的字面量 loopback HTTP MCP；stdio MCP spawn/wrapper 是旧历史方案，明确不可执行，RuntimeConfigV1 遇到 stdio 统一返回 `stdio_mcp_unavailable`。
- PureLab 的 8 个运输 command 只允许冻结的主窗口 label `main` 调用；其它窗口 fail-closed，不 spawn、不 unseal。`agent-kit-event` 也只投递给有对应 scope 权限的受信主窗口。
- PureLab Task 10 接线前必须关闭旧 `agent_test_provider` 旁路：删除前端调用与 Tauri 注册，或改为走 Host/L3b 同一 Endpoint/SSRF policy，并遵守“改 URL 必须带新 Key”。禁止 `api_key=null` + 任意 URL 解密已存 Key外送。设置页保存必须统一到 `agent_kit_set_llm_channel`，或与 Host 事务形成同一成功态。
- `session/new` 后仍打 `_x.ai/mcp/list`（带 `sessionId`），只做观察。真实 response shape 是 `servers[].session.{status,tools[]}`；local `tools[].name` 可能已去掉 server 前缀，Host 必须用 `server.name + "__" + tool.name` 重建合格名。只比较 `status=ready` 且 tool `enabled=true` 的集合：
  - **批准集空**（本产品 / 其它产品不挂 MCP）：空 catalog、只有 `GrokBuild:efflab_noop`、list 失败 → **都通过**，继续 prompt。
  - **批准集非空但 MCP 启动失败 / 超时 / 不 Ready / list 出错**：不杀 sidecar、不挡对话；发 session 级 `Status { code: "mcp_failed" }`；模型看不到那些工具。
  - **批准集非空且 Ready**：多出批准集没有的工具（例如 writeback）→ **kill**。缺工具 → `mcp_failed`，不杀。
- 批准集是 `HostApp::mcp_for_scope` 的期望合格名。空集 = 明确不走 MCP，不是「等任意非空」。
- `_meta` **按 method 分表**（`HostPolicy::meta_keys_for(method)`）。永远不放行 `yoloMode` / `permissionMode` / `agentProfile`。
- `host_contract` 只保护 Host 出站；真正边界是私有 home、默认工具关、受控 MCP、L3b、stdin 唯一写入者。
- Windows sidecar hardening fail-closed；UI 显示不可用，不隐藏入口。
- **prompt 文本语义门**（组包前，且 fixture 与 grok `prompt_parser` 对拍）：只挡 grok-shell **文件引用**，不挡曲库话术。
  - 上游会把任意 `@` 后的首个非空白 token 当成 FileReference 候选；因此拒绝所有这种 raw token，包括 `@secret.txt`、`@foo/bar`、`@../`、`@~/`、`@C:\`、UNC / extended Windows path，以及大小写任意的 `file://`。不得只匹配 `@/` 四个样例。
  - **放行**：没有 `@` 前缀的正文绝对路径仍按普通文本处理，例如「`/Volumes/Music/Inbox` 这批怎么标」。
  - `@` 曲库条目只走 §5.8 `mentions[]`；Host 展开后必须再次通过同一道门，禁止变成 `@/用户曲库路径` 或 `file://`。
  - ContentBlock 仅 `[{type:"text",text}]`，拒 image/resource/未知 type/块级 `_meta`。
- Host 永不发送 `getApiKey` / `setApiKey` / `getBearerToken`（保持 UnknownMethod）。
- CSP：不得因 Kit 再放宽；webview 不得打 `localhost:9999` / Connect。

## 9. 里程碑（Kit 视角）

### M0 — Host + contract 骨架

- 建立 `efflab-agent-contract` 与 `efflab-agent-host`
- `protocol` + `HostRuntime` + `HostApp` + `KitEventSink` + `HostRuntimeConfig` + submission map
- 无产品业务、不链 grok-shell 即可编译

### M1 — BYOK 对话 + 续聊合同

- Host 用 contract renderer 写出完整 RuntimeConfigV1（`[model]`、`approved_mcp`、`expected_tools`），backend 固定为 `chat_completions`；sidecar 只校验，不读取旧 `[models].default` 或 shell 配置
- `host_contract` 放行 prompt / cancel（通知）/ list；**不**放行 `session/set_model`；无顶层 `limit`
- Host：supervisor + AcpRuntime（含反向 request）+ projector + **L3b** + Channel
- **HostRuntime 闭环**（假 sidecar）：acquire → initialize → new/load → 读循环 → emit。没有这一环不得接产品
- agent-web：可 path 依赖的包；`useAgentKit` + 归约；只留 Agent pill
- 第一个产品：`HostApp` + `KitEventSink`，挂面板。对照期可并列旧入口，**不是** M1 完成条件
- 列会话 / 恢复走 Kit → ACP
- 只读 MCP 可后置；若做，优先 HTTP loopback，工具合格名进批准集
- Task 12 是 grep 门禁，**不是**「第二个产品已嵌入」的发布验收

### M2 — Relay

- 打开 L3b 的幂等头与 402/403/401 映射（出口已在 M1）
- 产品只接激活 / `app_key` / token
- 切回 BYOK 不丢 Key

对照结束后删除产品旧 Agent 入口与实现：单独开删除任务，不改 Kit 协议。删除时必须留下 **一个** 可见助手入口（Activity ITEMS），不要连入口测试一起删没。

## 10. PR 切分（按仓库）

| PR | 仓库 | 内容 |
|---|---|---|
| C0 | `effilab-agent` | `efflab-agent-contract`：从 sidecar 挪出 host_contract / MCP DTO / render |
| K1 | `effilab-agent` | host crate：`protocol` + `HostRuntime` + 端口 + submission；**不**依赖 sidecar 库 |
| S1 | `effilab-agent` | RuntimeConfigV1 `[model]` + `[approved_mcp]` / `expected_tools` + `chat_completions`，由 contract renderer 统一生成 |
| S2 | `effilab-agent` | `host_contract`：prompt / cancel 通知 / list；wire `_`；per-method `_meta`；文本语义门 |
| K2 | `effilab-agent` | supervisor + AcpRuntime（含 reply）+ projector + **dispatch 闭环** |
| K3 | `effilab-agent` | LLM Channel + L3b 回环出口 + secret seam |
| W1 | `effilab-agent-web` | 抽出可依赖包；`useAgentKit`；归约；禁用 Chat pill |
| A1 | 产品仓库 | `HostApp` + Sink + 8 个一行转发 + 挂面板；对照入口可选 |

产品 PR 不得把 ACP 类型引进 UI 或 adapter 业务层。

## 11. Key Decisions

| 编号 | 决策 | 理由 |
|---|---|---|
| KD-K1 | Host Runtime 放 `effilab-agent/crates/efflab/efflab-agent-host` | 用户指定；避免每个 App 复制 Host |
| KD-K2 | web↔Host 是 Kit 产品协议 | 否则嵌入成本等于重做 Host |
| KD-K3 | web 不讲 ACP / Connect；Host 讲 ACP | sidecar 已是 ACP；Connect 无实现 |
| KD-K4 | 产品只实现端口 + 调 `dispatch` | 第二个 App 验收 |
| KD-K5 | `Byok \| Relay` 第一天进类型；**M1 就走 L3b** | M2 不推翻 spawn；MCP 看不到用户 Key |
| KD-K6 | `ScopeId` 通用；Host 创建隔离 home/cwd，并强制拼 `app_id` | 不绑 library；会话分桶稳定 |
| KD-K7 | 前端真源 agent-web，自带归约与 `useAgentKit` | 引导不落产品 |
| KD-K8 | 旧 AIMO dual-agent 长文整份作废 | 不以该文为规范或安全附录 |
| KD-K9 | 续聊只用 sidecar；list 走 **标准** ACP `session/list` | 不建产品账本；不扫盘；不发明 `limit` |
| KD-K10 | 旧产品 Agent / Rig 对照完即删 | 全部写新代码；不为旧路径做兼容 |
| KD-K11 | 产品只提供套餐与通道配置（及 MCP/Confirm） | 不实现对话恢复 |
| KD-K12 | 运输名冻结为 8×`agent_kit_*` + `agent-kit-event`；tauri feature 默认关 | 新协议；不复用旧产品命令/频道；不双表 |
| KD-K13 | 出站 ACP fail-closed；入站未知 update 跳过；反向 request 必须回 | 恢复路径不能被 `plan` 打死；工具回合不能挂死 |
| KD-K14 | `session/cancel` 是通知；turn 级 `turn_id` = `_meta.promptId` = submission，session/process 级为 null | 对齐 grok-shell 真源且不伪造回合 |
| KD-K15 | M1 不放行 `session/set_model` | 换模型 = 改 Channel + 新 session |
| KD-K16 | Chat/Agent 双 pill 只留 Agent | Kit 能力面收束 |
| KD-K17 | host **不**依赖 sidecar 库；抽出 `efflab-agent-contract` | 产品进程不得链 / 编译 grok-shell |
| KD-K20 | `@` 与以后命令是 Host 封装 + 产品数据源 | 不走 grok-shell `@path` / Skills；M1 只能 @ 当前库 |
| KD-K18 | 线协议机器真源是 `protocol.rs` | 避免三份散文互漂 |
| KD-K19 | M1 验收是开发机 path，不是安装版 | 打包/签名另开计划 |

## 12. 开放问题

1. UI 包：M1 先 workspace / path 依赖；第二个 App 再私有 npm。
2. ~~`feature = "tauri"` 默认关还是开~~ **已关**（KD-K12）。
3. Relay `app_key` 由各产品自己配置，Kit 不写死。拍板人：产品 owner（PureLab 另文跟踪）。
4. ~~`cleanup_ttl_days` 数字~~ **已关**：36500，禁止 0。
5. ~~`session/list` 用哪条~~ **已关**：标准 ACP `session/list`；不用 `_x.ai/session/list`。

## 13. 打包（不进 M1 任务，合同先写在这里）

以后做安装版时抄产品现网嵌套 bin 合同，不要另发明：

- macOS：`.app` + `Resources`/`Helpers` + 公证；sidecar 只接收 `--runtime-config` 与隔离路径，旧 `--mcp-exec-root` 不属于 v1 合同
- Windows：exe 旁路 / MSIX 资源映射；SmartScreen
- Host **不**调用 `Command::new_sidecar`（无 tauri feature）。产品解析 `PathBuf` 交给 `HostRuntimeConfig.sidecar_bin`
- Linux 产品不交付，Kit 不承诺 Linux host

## 14. 证据

以下路径用于定位当前实现与合同；本节不把旧 shell 方案、旧配置格式或 stdio wrapper 当作可执行依据：

- Host 出站校验：`crates/efflab/efflab-agent-contract/src/host_contract.rs`、`docs/host-acp-contract.md`
- RuntimeConfigV1 与 MCP server/tool 校验：`crates/efflab/efflab-agent-contract/src/model.rs`、`src/render.rs`、`src/mcp_config.rs`、`src/stdio_mcp.rs`
- 最小 sidecar ACP/runtime：`crates/efflab/efflab-agent-sidecar/src/acp_agent.rs`、`src/runtime.rs`
- 标准 `session/list`、v1 会话与回放：`crates/efflab/efflab-agent-sidecar/src/acp_agent.rs`、`src/session_store.rs`
- L3b Chat Completions 回环：`crates/efflab/efflab-agent-host/src/llm_loopback.rs`
- agent-web 只消费 Kit 协议：`../../../effilab-agent-web/app/src/hostTypes.ts`、`app/src/useAgentKit.ts`
- 第一个产品 adapter：[`ai_music_organizer_br` PureLab 设计](../../../ai_music_organizer_br/docs/plans/2026-08-13-effilab-agent-web-purelab-pilot-design.md)
- Relay 的 Chat Completions 与幂等：`../../../effilab_agent_server`
- 2026-08-13 R1：分层对、协议未冻。R2：crate 依赖 / 反向 RPC / dispatch 闭环 / list schema / 引导与 replay 栅栏。gpt-sol 终审修订：L1 前向兼容、可空 turn、mentions 注入/指纹、全局 Channel、Host 写盘 launch contract、ValidatedReply、prompt gate、L3b/MCP/产品安全门。
