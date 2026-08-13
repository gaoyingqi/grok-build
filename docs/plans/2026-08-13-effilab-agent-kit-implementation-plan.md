# Efflab Agent Kit M0–M1 Implementation Plan

> **状态**：Draft — 2026-08-13 gpt-sol 终审后修订；不自我声明 production Go。
>
> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 做出可被多个桌面产品嵌入的 Agent Kit：产品只调 `HostRuntime::dispatch`，web 只认 Kit 协议；Host 用 ACP 管 sidecar。M1 用 BYOK Chat Completions 跑通对话，并走 sidecar 自带的列会话 / `session/load` 续聊。

**Architecture:** 规范导读是 `docs/plans/2026-08-13-effilab-agent-kit-host-architecture.md`（gpt-sol 终审后修订稿）。线协议机器真源落地后是 `protocol.rs`。web 不讲 ACP。会话真源是 sidecar。产品只提供 BYOK/套餐并挂面板。旧产品 Agent（含 Rig）只作对照，对照完删除；本计划 **全部写新代码**，不为旧路径做兼容。C5 写回、fd 生产通道、Windows 真跑、安装包 **不进本计划**。

**Tech Stack:** Rust workspace（`effilab-agent`）、ACP JSON-RPC stdio、`agent-client-protocol 0.10.4`、React 18/19 peer-compatible + Vite（`@efflab/agent-web`）、Tauri v2（产品运输）、OpenAI Chat Completions。

**规范依据:**

- Kit 架构：`docs/plans/2026-08-13-effilab-agent-kit-host-architecture.md`
- sidecar ACP：`docs/host-acp-contract.md`（R2 方法面 + 反向 request）
- 旧 dual-agent 长文已 **作废**
- PureLab adapter：`/Volumes/work/documents/ai_music_organizer_br/docs/plans/2026-08-13-effilab-agent-web-purelab-pilot-design.md`

**开工顺序：** L1 线协议已按 2026-08-13 gpt-sol 终审结论钉死，Task 1（含 contract crate）可以立刻 TDD。Task 3 开工前 `session/list` 已在契约钉死（标准 ACP，无 `limit`）。Task 5 只交付 slot/路径/lifecycle 抽象，不依赖 Task 7 的完整 spawn；Task 7 依赖 Task 5 的 slot/路径与 Task 2 renderer，负责完整 launch integration。Task 1 golden 稳定后，Task 8 可与 Rust Host 实现并行。**禁止在 Task 7b 之前开 Task 10。**

## Global Constraints

- 全中文注释写在 crate/产品新增的函数与核心段落；日志脱敏，禁止打印 Key/token。
- web / 产品 UI / 产品 adapter **禁止**出现 ACP method 名、`session/prompt` 拼包、`AcpClient`、Connect/protobuf、`efflab_agent_sidecar::`、`xai_grok_`。产品只 `HostRuntime::dispatch`。
- 运输冻结：8 个 `agent_kit_*` 命令 + `agent-kit-event`。JSON `snake_case`。host **无**默认 `tauri` feature。
- sidecar stdout 只许 ACP JSON-RPC；日志走 stderr。
- `host_contract` 出站 fail-closed；`capabilities.terminal`/`fs` 必须 false；client `mcpServers` 必须空数组。
- 扩展 method：校验用 `x.ai/...`，写 stdin 用 `_x.ai/...`。
- `session/cancel` 是 **通知**。`KitProductEvent.turn_id: Option<String>`：turn 级块必填且 `turn_id = _meta.promptId = submission_id`；session/process 级 Status 的 `turn_id` / `submission_id` 为 null，禁止 synthetic turn id。
- M1 **不**放行 `session/set_model`。列会话只用标准 `session/list`（无顶层 `limit`）。
- LLM：`Byok | Relay` 类型第一天存在；M1 只启用 `Byok`，强制 `api_backend = "chat_completions"`，权威 config 必须写 `[models] default`。禁止默认 `grok-4.5` + `responses`。
- **M1 走 L3b**：用户 Key 不进 sidecar env；sidecar `base_url` = `http://127.0.0.1:<port>/v1`。
- BYOK Key 与 Relay token 分字段（ADR-12）。
- 一 `ScopeId` 一 sidecar 进程。路径 = `{home_root}/{sanitize(app_id)}/{sanitize(scope)}/{home|workspace}`。禁止用产品库根当 cwd。
- Windows sidecar capability = unavailable，入口仍可显示；类型在 Windows 编译单元必须存在。
- M1 只允许只读/预览 MCP。写回/rename/trash/transcode 走产品自己的 UI。
- 根 `Cargo.toml` 是生成物；新 member 必须改 `scripts/fork-sync-apply.sh`。
- 对照入口不是本计划完成条件。
- projector：未知 / 禁用 ACP update → 计数 / `skipped_update`，禁止整轮 fail-closed。
- host crate **禁止** path 依赖 `efflab-agent-sidecar` 库。只依赖 `efflab-agent-contract`。
- `dispatch` 返回时机见架构 §6：`Send` / `Cancel` / 热 Resume / 冷 Resume 写出后立刻回；`NewSession` / `ListSessions` **必须等** ACP result。禁止等 `session/prompt` result。

## 与旧设计 PR 的映射（本计划做 / 不做）

| 旧 PR（作废文） | 本计划 |
|---|---|
| F1 基础 ACP fixture | Task 3 + Task 4：prompt/cancel/list 白名单 |
| F1B/F1C session + prompt receipt | Task 1/6：内存 `submission_id`；`promptId` 对齐 |
| F2 POC env bearer | Task 7 L3b 绑定令牌；用户 Key 不进 sidecar；生产 fd 不做 |
| C1–C3 产品 session 大 schema | **不做。** 恢复用 sidecar list/load |
| C4/C5 Preview / mutation | **另开计划** |
| H2 supervisor/ACP | Task 4–5 / 7b，归属 host crate |
| U1 TUI | **不做。** Task 8 用 agent-web |
| 双 runtime 对账 / 统一信封 | **不做。** 旧路径对照完删除 |
| F3A/F1D/H3/F5/H5/R1/R2/D1 | 本计划不做 |

## File Map

```
effilab-agent
  crates/efflab/efflab-agent-contract/   host_contract + MCP DTO + render_* + write_toml
  crates/efflab/efflab-agent-host/
    src/lib.rs
    src/protocol.rs                     机器真源
    src/runtime.rs                      HostRuntime::dispatch（Task 1 stub，Task 7b 闭环）
    src/app_port.rs
    src/event_sink.rs
    src/config.rs
    src/submission.rs
    src/acp_runtime.rs
    src/supervisor.rs
    src/projector.rs
    src/llm_channel.rs
    src/llm_loopback.rs
    tests/*.rs
  crates/efflab/efflab-agent-sidecar/    依赖 contract；不再自持 host_contract 副本
  scripts/fork-sync-apply.sh
  docs/host-acp-contract.md

effilab-agent-web
  抽出可 path 依赖的面板包
  useAgentKit + Kit 事件 → Message[] 归约
  Composer 只留 Agent

<product>
  HostApp + KitEventSink + 启动时 new 一次 HostRuntime（Tauri manage）
  8 个 command 只借引用 dispatch + 挂面板
  不写 ACP
```

---

### Task 1: `efflab-agent-contract` + host 骨架、Kit 类型、端口

**Files:**

- Create: `crates/efflab/efflab-agent-contract/`（从 sidecar 挪 `host_contract` / `ApprovedMcpConfig` / `SidecarModelSpec` 的类型面；本 Task 可先搬文件、测原 fixture 仍绿）
- Create: `crates/efflab/efflab-agent-host/Cargo.toml` 及 `src/{lib,protocol,runtime,app_port,event_sink,config,submission}.rs`
- Create: `crates/efflab/efflab-agent-host/tests/protocol_and_submission.rs`
- Create: golden JSON `crates/efflab/efflab-agent-host/tests/fixtures/kit_wire/*.json`
- Modify: sidecar 改为依赖 contract；删除重复模块或 `pub use`
- Modify: `scripts/fork-sync-apply.sh`（member：sidecar + contract + host）
- 根 `Cargo.toml` 仅通过脚本写入

**禁止：** host `Cargo.toml` 出现 `efflab-agent-sidecar`。`cargo tree -p efflab-agent-host` 不得出现 `xai-grok-shell`。

**Interfaces（serde 冻结，见架构 §5.1）：**

```rust
// KitCommand: {"cmd":"send", ...}
pub enum KitCommand { /* GetCapability, Send, Cancel, NewSession, ListSessions, ResumeSession, GetLlmChannelView, SetLlmChannel, Unknown { cmd } */ }

// KitReply: {"kind":"send", accepted, duplicate, ...}
pub enum KitReply { /* Capability, Send, Cancel, NewSession, ListSessions, ResumeSession, LlmChannelView { channel: LlmChannelView } */ }
// wire: {"kind":"llm_channel_view","channel":{"kind":"byok"|"relay"|null,...}}

pub enum KitBlock {
    User { text: String },
    Assistant { markdown: String, streaming: bool },
    Thinking { text: String },
    Tool { tool_call_id: String, name: String, detail: String, status: ToolStatus },
    Error(KitError),
    Retry { attempt: u32, reason_code: String },
    Status { code: String, message: String },
    Unknown { unknown_kind: String },
}

pub enum ToolStatus { Pending, Running, Completed, Failed, Cancelled }

pub struct KitError {
    pub code: String, // wire 是字符串；已知码清单见架构 §5.4，未知码仍须保留 message
    pub message: String,
    pub details: Option<serde_json::Value>,
    pub request_id: Option<String>,
    pub retryable: bool,
    pub retry_after_ms: Option<u64>,
}

pub struct HostRuntimeConfig { pub home_root: PathBuf, pub sidecar_bin: PathBuf, pub mcp_exec_root: PathBuf, pub idle_after: Duration }

pub trait HostApp: Send + Sync {
    /* app_id, persist/load channel, seal/unseal, mcp_for_scope — 无 emit */
    fn mentions(&self) -> Option<&dyn HostAppMentions> { None }
}

pub trait HostAppMentions: Send + Sync { /* resolve_mentions */ }
pub trait KitEventSink: Send + Sync { fn emit(&self, ev: KitProductEvent) -> anyhow::Result<()>; }

impl HostRuntime {
    pub fn new(app: impl HostApp, sink: impl KitEventSink, cfg: HostRuntimeConfig) -> Self;
    pub fn dispatch(&self, cmd: KitCommand) -> Result<KitReply, KitError>;
}
```

`SetLlmChannel` / `GetLlmChannelView` 字段按架构 §5.1–§5.2。Task 1 **只测 wire**：reply 外层 `kind="llm_channel_view"`，通道嵌套在 `channel`；请求/响应 JSON 不回显 key/token；未知 `cmd` 经 Kit JSON 解码为 `Unknown { cmd }` 后由 dispatch 返回结构化 `unsupported`。本 Task 不实现、不测试 Channel 业务语义。

`SubmissionMap`：key = `(scope_id, session_id, submission_id)`；指纹 = `canonicalize(scope_id, session_id, text, mentions_sorted)`，mentions 按 `(kind, id)` 字典序。同指纹幂等回 `KitReply::Send { duplicate: true, ..同一 turn_id }`；同 submission/text 但 mentions 不同 → `fingerprint_conflict`。

本 Task `dispatch` 对所有未实现命令（含 Set/Get channel）返回 `KitError { code: "unsupported" }`——**仅单测可见**；产品路径在 Task 7b 之前不得接线。改 URL 必须带 Key、全空 no-op、轮换秘密等红测全部在 Task 7。

- [ ] **Step 1: 写失败测试**

除下列示例外，还必须有 `llm_channel_view` 嵌套 channel golden：`{"kind":"llm_channel_view","channel":{"kind":null,"key_present":false,"token_present":false,"model_selectable":false}}`；Set/Get channel 仅验证 wire 与秘密不回显，dispatch stub 仍回 unsupported。

```rust
#[test]
fn kit_command_serde_is_adjacent_cmd_snake_case() {
    let cmd = KitCommand::Send { /* ... */ };
    let v: serde_json::Value = serde_json::to_value(&cmd).unwrap();
    assert_eq!(v["cmd"], "send");
    assert!(v.get("session_id").is_some());
    assert!(v.get("Send").is_none());
}

#[test]
fn kit_reply_send_has_duplicate_bit() { /* kind=send, accepted, duplicate */ }

#[test]
fn unknown_cmd_reaches_structured_unsupported() {
    // 先按 Kit JSON 解码为 KitCommand::Unknown { cmd }，再 dispatch；禁止 serde/Tauri Err(String)。
}

#[test]
fn unknown_error_code_keeps_message() {
    // 未知 code 仍成功解码，保留 message，且客户端不据此自动重试。
}

#[test]
fn kit_block_unknown_kind_round_trips_to_unknown_shape() {
    let raw = r#"{"schema_version":1,"scope_id":"s","session_id":"sid","turn_id":"t","submission_id":"t","event_id":"e","sequence":0,"origin":"live","block_id":"b","block":{"kind":"plan","steps":[]}}"#;
    let ev: KitProductEvent = serde_json::from_str(raw).unwrap();
    assert!(matches!(ev.block, KitBlock::Unknown { unknown_kind } if unknown_kind == "plan"));
    // 再序列化的 block 必须是 {"kind":"unknown","unknown_kind":"plan"}，原 payload 有意丢弃。
}

#[test]
fn session_level_status_allows_null_turn_id() {
    // replay_complete 等 turn_id/submission_id 均 null；event_id={session_id}:host:{code}:{sequence}，block_id=event_id。
}

#[test]
fn kit_event_json_has_no_acp_method() { /* 对 golden 事件断言 */ }

#[test]
fn same_submission_id_same_fingerprint_is_duplicate_reply() { /* 首次 accepted+duplicate=false；二次 duplicate=true 同 turn_id */ }

#[test]
fn same_submission_id_different_bytes_fail_closed() { /* fingerprint_conflict */ }

#[test]
fn same_submission_and_text_with_different_mentions_conflicts() { /* mentions 按(kind,id)排序后进入指纹 */ }

#[test]
fn host_crate_does_not_depend_on_sidecar_or_grok_shell() {
    // cargo tree 或 include_str Cargo.toml 断言
}
```

- [ ] **Step 2:** `cargo test -p efflab-agent-host --test protocol_and_submission` → FAIL（crate 不存在）
- [ ] **Step 3:** 最小实现。`fork-sync-apply.sh` 改成 member 数组（contract + sidecar + host）。sidecar 改依赖 contract。
- [ ] **Step 4:** 测试 PASS；`scripts/fork-sync-apply.sh --check`；`cargo tree -p efflab-agent-host` 无 grok-shell
- [ ] **Step 5: Commit** `feat(host): add contract crate and kit protocol HostRuntime skeleton`

---

### Task 2: sidecar 权威 config：`chat_completions` + `[models].default` + TTL

**Files:** contract 的 `render_authoritative_config`、sidecar `hardening.rs` / `main.rs`、现有 hardening 测试

签名改为必须传入 `models: &[SidecarModelSpec]`。空 slice **不得**写入内置 `grok-4.5`。

**Host→sidecar launch contract（本 Task 冻结，Task 7 接线）：** `{GROK_HOME}/config.toml` 的唯一写盘 owner 是 Host。Host 在 spawn 前调用本 renderer 写出完整 TOML；sidecar 只校验已有文件符合本合同，**不得**再覆盖 `[models]` / `[storage]` / `[session]`，缺文件或不合规 → 退出码 2。sidecar CLI 不增加用户 Key 参数；端口与 model 只从 Host 写出的 TOML `base_url` / `model` 读取。

**两把钥匙（不要写进同一份 TOML）：**

| 谁 | 配什么 | 写哪 |
|---|---|---|
| PureLab 设置页 | 用户大模型 `endpoint` / `model` / `api_key`（以后 Relay token） | 产品密封库；经 `set_llm_channel` 进 Host |
| 本 Task 渲染的权威 TOML | sidecar **怎么打 Host 回环** | `{GROK_HOME}/config.toml` |

非空时必须同时写：

```toml
[models]
default = "byok"

[model.byok]
model = "..."                          # 设置页的 model_id，原样给 Chat Completions
base_url = "http://127.0.0.1:PORT/v1"  # 永远是 Host L3b，不是设置页那个 URL
name = "BYOK"
api_backend = "chat_completions"
env_key = "EFFLAB_L3B_BIND"            # 绑定令牌；禁止写成 XAI_API_KEY
# 禁止 api_key = "..."                 # 用户 Key / Relay token 不准落盘

[storage]
cleanup_ttl_days = 36500

[session]
load_envrc = false
```

`api_backend != "chat_completions"` → `bail!`。**禁止 TTL=0**。测试断言：

- 有 `default = "byok"`、`chat_completions`、`env_key = "EFFLAB_L3B_BIND"`
- 有 `[session] load_envrc = false`，sidecar 校验最终解析值为 false；workspace 放写 marker 的 `.envrc` 时，`session/new` / 冷 `session/load` 都不得执行
- 文本 **无** `api_key`、`XAI_API_KEY`、`grok-4.5`、`responses`、用户 Key 形如 `sk-` 的字面量
- `base_url` 是 `127.0.0.1` 回环，不是设置页传入的上游 URL（上游 URL 只进 Host Channel）
- Host 写出的合法文件 sidecar 只校验；缺文件、非法 `[models]` / `[storage]` / `[session]` 均退出码 2，且 sidecar 不覆写

sidecar 与 Host 都只调用 contract 的 `render_*` / validator，禁止第二份渲染逻辑；**实际写盘只在 Host**。

- [ ] **Step 1: 测试** 含：写出 `default = "byok"` 与 `chat_completions`；拒绝 `responses`；文本不含 `grok-4.5`；有 `[storage]` 与 `load_envrc = false`；无 `api_key`；sidecar 校验而不覆写
- [ ] **Step 2–4:** 红 / 改所有调用点 / 绿
- [ ] **Step 5: Commit** `feat(sidecar): pin chat_completions default model and session ttl`

未配置模型时 **不要** spawn。Task 7b：对话面（含 `GetCapability`）一律 `llm_channel_unconfigured`，不是成功的 capability。

---

### Task 3: `host_contract` 放行 prompt / cancel / list

对照 `docs/host-acp-contract.md` §3。实现前 `rg` ACP schema / grok-shell，**禁止猜字段**。fixture 方言是 **`allow` / `reject`**，每条必须有 `params`。

放开：

- `session/prompt`：`sessionId` + `prompt: [{type:text,text}]` + `_meta.promptId` only；嵌套 schema + **与 grok `prompt_parser` 对拍的文本语义门**：拒任意 `@`+首个非空白 token（含 `@secret.txt`、`@foo/bar`、`@../`、`@~/`、`@C:\\`、UNC）与大小写 `file://`；放行无 `@` 前缀的正文绝对路径 `/Volumes/...`。Host 展开后的 mentions 文本再走同一道门
- `session/cancel`：校验字段 `sessionId`（发送时是通知）
- `session/list`：`cwd` + 可选 `cursor`；**拒绝 `limit`**、`additional_directories`、`allowRelax`

**不要**放开 `session/set_model`、`_x.ai/session/list`。

`HostPolicy` 改为 `meta_keys_for(method)`。禁止 `with_meta_key` 全局表。

- [ ] **Step 1:** 往 `host_contract_cases.json` 加正例/反例（契约 §8 清单）
- [ ] **Step 2:** `cargo test -p efflab-agent-contract --test host_contract` → FAIL
- [ ] **Step 3:** 扩 `validate_top_level_fields` + 嵌套 prompt + 文本门 + per-method `_meta`
- [ ] **Step 4:** host_contract + sidecar acp_stdio 绿
- [ ] **Step 5: Commit** `feat(contract): allowlist session prompt cancel list with per-method meta`

---

### Task 4: `AcpRuntime`（请求 / 通知 / 读循环 / 反向 request）

不要把唯一 API 做成阻塞的 `send_validated -> Value` 然后在 prompt 期间丢通知。

```rust
pub enum Inbound {
    Response { id: RequestId, result: Result<Value, RpcError> },
    Notification { method: String, params: Value },
    Request { id: RequestId, method: String, params: Value },
}

pub enum ValidatedReply {
    Result(Value),
    Error { code: i64, message: String },
}

impl AcpRuntime {
    pub fn request_validated(&self, method: &str, params: Value, policy: &HostPolicy) -> Result<RequestId>;
    pub fn notify_validated(&self, method: &str, params: Value, policy: &HostPolicy) -> Result<()>;
    pub fn reply_validated(&self, id: RequestId, reply: ValidatedReply, policy: &HostPolicy) -> Result<()>;
    pub fn poll_inbound(&self) -> Result<Option<Inbound>>;
}
```

所有权：内部用拆开的 stdin 写端 + stdout 读端（Mutex / channel），**不要** `&mut self` 堵死 `dispatch(&self)`。

- 发送前 `validate_host_request`（逻辑名）；写 stdin 时给扩展 method 加 `_`。
- 唯一写 stdin 入口；禁止 `pub` 裸 write。
- `AcpRuntime` 保存 `request_id → { method, params }`；permission reply 按保存的本次 `options[]` 校验 `optionId`。
- 单测：`initialize` + `terminal:true` 在写 IO 前拒绝；`notify_validated("session/cancel", …)` 写出的 JSON **无 `id`**；`x.ai/mcp/list` 校验过、stdin 为 `_x.ai/mcp/list`。
- 单测：direct `session/request_permission` 与 `_x.ai/...` wrapper 都解成 `Inbound::Request`；`reply_validated(ValidatedReply::Result)` 写出带同一 `id` 的 result，且 `optionId` ∈ 本次 `options`（正例用 `allow-once`，禁止 `enable-always-approve`）。
- 单测：未知 inbound request 用 `ValidatedReply::Error { code: method_not_found, ... }` 回 JSON-RPC error，不静默丢。

- [ ] **Step 1–5:** 红 / 实现（可用 sink）/ 绿 / commit  
  `feat(host): acp runtime multiplexes notify request and reverse rpc`

---

### Task 5: Supervisor（稳定路径 + 一 scope 一进程）

```text
home      = {home_root}/{sanitize(app_id)}/{sanitize(scope)}/home
workspace = {home_root}/{sanitize(app_id)}/{sanitize(scope)}/workspace
```

`HostRuntimeConfig.home_root` 是 App Data **根**；Host **强制**再拼 `app_id`。绝对路径。拒绝 `..`。不要接收「产品库根」当 cwd。

Windows：`capability() = Unavailable { sidecar_hardening_unavailable }`，`acquire` 同错，不 spawn。**类型与 kill API 在 Windows 编译单元必须存在**。

本 Task **只做**路径、sanitize、强制拼 `app_id`、scope slot、Windows unavailable、child lifecycle 抽象，以及**独立于 sidecar home lock** 的 Host process-slot metadata。`{GROK_HOME}/.efflab-sidecar.lock` 的唯一 owner 仍是 sidecar；Host 禁止抢同一把锁。

本 Task **不**注入 `EFFLAB_L3B_BIND`，不写 models TOML，不做完整 spawn integration。child env 抽象先规定 `env_clear` + 白名单并拒绝 `GROK_CHAT_MODE` / `XAI_API_KEY` / `GROK_CODE_XAI_API_KEY` / 用户 Key；Task 7 在 L3b 监听、binding token 注册、Task 2 config 写盘完成后再组装真实 spawn。Drop：in-flight 时先 cancel；关 stdin 3.5s → TERM 2s → KILL（Windows 对等）。保留 GROK_HOME。

- [ ] **Step 1:** `acquire` 两次同 scope 复用 slot；sanitize `..` 拒绝；强制拼 `app_id`；process-slot metadata 不竞争 `.efflab-sidecar.lock`；`#[cfg(windows)]` unavailable 仍能编译
- [ ] **Step 2–5:** 红 / 实现 / 绿 / commit  
  `feat(host): scope-isolated supervisor with stable home and cwd`

---

### Task 6: Projector（未知 update 跳过 + replay 栅栏）

```rust
pub fn apply_acp_notification(...) -> Result<Vec<KitProductEvent>, ProjectError>;
```

映射（名称以 schema / 仓库抓包为准，先 `rg SessionNotification`）：

| ACP | KitBlock |
|---|---|
| agent_message_chunk | Assistant { streaming: true } 同一 `block_id` 累计快照；replay 时 streaming=false |
| agent_thought_chunk | Thinking |
| tool_call / tool_call_update | Tool（status 用冻结五值） |
| user 回显 | User；`block_id` 优先 `promptId` |
| `_meta.isReplay` | `origin=Replay` |
| 未知 / plan / todo / `_x.ai/session/update` | 计数；live → `Status { skipped_update }`；replay → 批次结束一条 `replay_skipped` |

sidecar 投影事件的 `event_id` 优先 `_meta.eventId`，否则 `"{session_id}:{origin}:{sequence}"`。Host 合成的 session/process 级 Status 固定 `event_id="{session_id}:host:{code}:{sequence}"`、`block_id=event_id`、`turn_id=null`、`submission_id=null`。从 `crates/codegen` 或 `tests/acp_stdio.rs` 抄真实 JSON。

另测：一条未知 update **不得** `Err`。replay 批次结束由 Task 7b 发 `replay_complete`；turn 终态 Status 必须带 `turn_id=submission_id=promptId`。

- [ ] **Step 1–5:** 红 / 实现 / 绿 / commit  
  `feat(host): project acp updates into kit events without failing replay`

---

### Task 7: LLM Channel + L3b 回环出口

`LlmChannelConfig::Byok | Relay { enabled: false }`。`enabled=true` → `RelayNotImplemented`。

**M1 必做 L3b。** 设置页的用户 URL/Key **不**写进 sidecar；sidecar 只打回环。以后切自家 Relay 只换 Host 出站，不改 sidecar TOML 形状。Task 7 依赖 Task 5 的 slot/路径和 Task 2 renderer，并在本 Task 完成真实 launch integration：**先听 L3b → 发/注册 binding token → Host 写 config.toml → spawn**。

- Host 在 `127.0.0.1` / `::1` 听一口（进程级，不是每 scope 一口）；禁止 `0.0.0.0`；未配置通道不听
- 注册表：`binding_token → scope slot → sidecar process generation → channel revision`；token 至少 256 bit、常量时间比较、日志只记短指纹。请求不得自报或覆盖 scope/channel
- 完整 spawn 的 sidecar 环境 **只**有 `EFFLAB_L3B_BIND`；Task 5 不注入。sidecar `base_url` = `http://127.0.0.1:{port}/v1`，model/port 只来自 Host 写出的 TOML
- 入站：`Authorization: Bearer <绑定令牌>`；path 仅 `POST /v1/chat/completions`；禁环境 HTTP(S)_PROXY / CONNECT / 非 POST；不跟随 302
- SSE/stream **逐块转发**，禁止整包 buffer。测试必须证明首块在上游结束前到达；下游断开或 Cancel 后停止读上游；请求体有硬上限，保留正确 status/content-type/stream body
- 出站：**换成**当前 Channel 凭据（M1 = 设置页 unseal 后的用户 Key + 设置页 URL；M2 = Relay token + relay URL）。绑定令牌不得当 Bearer 转给上游
- URL 语法校验后解析全部 A/AAAA；任一地址为 loopback/private/link-local/metadata/unspecified 即整单拒绝。连接用已验证地址，并以原 hostname 做 TLS SNI/证书校验；每次新连接或 DNS TTL 到期重新验证；`allow_loopback_llm` 默认 false
- 测试：A token 不能走 B channel；旧 generation / 抢锁后旧 token / 换通道旧 token失败；未知 token 在 unseal 和任何上游请求前失败；sidecar 环境无 `XAI_API_KEY` / 用户 Key；上游 Authorization = 用户 Key ≠ binding token
- `SetLlmChannel` 是**产品全局**设置，无 `scope_id`：改 URL/model/`kind` 必须带新明文；全空 no-op；只轮换 Key 可只传 `api_key`。persist 后新配置即 committed view，先失效全部旧 token，再 drain/重启**所有存活 scope**，等成功或确认无需 spawn 后回 view。部分重启失败返回可重试 `KitError`，但不得假装仍是旧配置

`HostApp` 的 `persist_llm_channel` / `load_llm_channel` / `seal_secret` / `unseal_secret` 均为产品全局；Byok 与 Relay 分槽。M1 Host 测用 Fake seal 即可；产品可继续用现有密封。`SecretGuard` 禁 Debug/序列化/Clone，unseal 失败 fail-closed。OS Credential Store 迁移不进本 Task，也不挡 Task 10。view 使用嵌套 `channel`，禁止带回 Authorization / key / token。

- [ ] **Step 1–5:** 红 / 实现 / 绿 / commit  
  `feat(host): byok channel via loopback export, key never in sidecar env`

---

### Task 7b: `HostRuntime` 闭环（假 sidecar / sink）— **产品接线前门禁**

**Files:** `runtime.rs`（本 Task **必须**改）、`tests/dispatch_loop.rs`

前置：Task 1–7 零件绿。没有本 Task，`dispatch` 仍是 `unsupported`，Task 10 会把握手写进产品仓。

每 scope 一个 IO actor。状态机见架构 §6。

- [ ] **Step 1: 测试（假 sidecar stdio）**

```text
TC-LAUNCH  L3b 已监听 → 本代 token 已注册 → Host 已写合法 config.toml → 才 spawn；sidecar 不覆写 config
TC-HP      acquire → initialize(20s) → NewSession
           **等** session/new result 才回 KitReply（带 session_id）
           session/new(cwd=session_cwd, mcpServers=[], _meta.modelId=byok)
           → _x.ai/mcp/list（真实 shape=servers[].session.{status,tools[]}；local 用 server+"__"+tool 重建合格名；只比 ready+enabled）
           批准集空：空 catalog / 仅 noop / list 失败都通过，继续 prompt，不杀
           多出未批准工具 kill；缺工具 / 未 Ready → session 级 mcp_failed，不挡对话
TC-SEND    dispatch(Send) 写出 prompt 后立即 KitReply::Send { accepted, duplicate:false }
           读循环投影 Assistant + Status{turn_completed}
           禁止等 prompt result 才返回
TC-NOKEY   未配 Channel：不 spawn、不听 L3b
           GetCapability / Send / NewSession / List / Resume / Cancel
             → KitError { code: llm_channel_unconfigured }（不是 Capability 成功）
           GetLlmChannelView 仍成功（channel.kind=null, key_present=false），给设置页
TC-IDEMP   同 submission + 同指纹（含排序后 mentions）→ duplicate=true 且不二次写 prompt；同 text 不同 mentions → fingerprint_conflict
TC-TURN    同 (scope,session) 另一 submission 的第二次 Send → turn_in_progress
TC-CANCEL  notify session/cancel 无 id；先 cancel 再写 prompt → 不发 prompt + Status{cancelled}
           已发出则仍等 prompt result 再清 in-flight
TC-LIST    dispatch(List) **等** session/list result 才回；cwd=该 scope session_cwd；摘要只有四字段；JSON 无 cwd
           title 只用 sidecar list 值；缺失则空串，Host 不扫盘猜首条 user 文本
TC-RESUME  冷：未 attach 时写出 session/load → 立刻 accepted → origin=replay
           → load result 之后才 replay_complete；禁止等 result 才回 accepted
TC-HOT     已 is_active 再 Resume 同一 id：stdin **无** session/load
           重放内存缓冲 + replay_complete；若 Prompting 不打断
           Resume 另一个 session 且 Prompting → session_busy
TC-SKIP    replay 中未知 update → 一条 session 级 replay_skipped，不 fail
           replay_complete/replay_skipped/mcp_failed：turn_id=null、submission_id=null、event_id={session_id}:host:{code}:{sequence}、block_id=event_id
TC-PERM    inbound request_permission：批准集/noop → result.optionId=="allow-once"（∈ 本次 options）
           未知工具 → reject-once 或 Cancelled；cancel_requested → Cancelled
           禁止字符串 "allow_once"、禁止选 enable-always-approve
TC-IDLE    idle-kill 后 Send 旧 session_id → 先冷 load 等到 replay_complete 再写 prompt
TC-AUTO    自动 load 失败 → session_not_found
TC-CHANNEL 全局变更使全部旧 token 失效并重启所有存活 scope；部分失败返回 retryable error，但 Get view 仍是 committed 新配置
```

- [ ] **Step 2–4:** 红 / 把零件串进 `runtime.rs` / 绿
- [ ] **Step 5: Commit** `feat(host): dispatch loop handshake project and emit`

**没有本 Task 绿，禁止开 Task 10。**

---

### Task 8: agent-web 成为可嵌入包 + Kit 归约 + `useAgentKit`

**Files:** `effilab-agent-web` — `hostTypes.ts`（对拍 Task 1 golden）、`reduceKitEvents`、`useAgentKit`、`AgentPanel`、`package.json` exports、样式隔离（面板根作用域，避免 `:root` 污染宿主）。包名固定 `@efflab/agent-web`；`react` / `react-dom` 放 `peerDependencies`，范围覆盖 React 18 与 19；demo 使用 `devDependencies`；library build externalize React。

```ts
export function useAgentKit(opts: {
  scopeId: string;
  invoke: (cmd: string, args: Record<string, unknown>) => Promise<unknown>;
  subscribe: (cb: (ev: KitProductEvent) => void) => () => void;
  searchMentions?: (q: string) => Array<{ kind: string; id: string; label: string }>;
}): {
  capability: Capability;
  sessions: SessionSummary[];
  events: KitProductEvent[];
  sending: boolean;
  sessionId: string | null;
  send: (text: string) => Promise<KitReplySend>;
  stop: () => Promise<void>;
  newSession: () => Promise<void>;
  resume: (sessionId: string) => Promise<void>;
};

export type AgentPanelProps = {
  scopeId: string;
  kit: ReturnType<typeof useAgentKit>; // 推荐
  searchMentions?: (q: string) => Array<{ kind: string; id: string; label: string }>;
  // messages?: Message[]  — M1 deprecated，产品禁止传当真源
};
```

`useAgentKit` **独占**架构 §5.7 引导。`invoke` 载荷必须是 snake_case `KitCommand` JSON。组件内部可用 camel。`send` 必须把 `KitReply.send`（含 `accepted`/`duplicate`）交回，禁止 `Promise<void>`。`KitError.code` 在 TS 侧保持 string；未知 code 展示 `message`，不触发自动重试。

`onSend` 无 `scopeId` 入参：由 hook 闭包注入。

M1 可执行 UI：Send / Stop / New Chat / 会话列表。ComposerToolbar **只渲染 Agent**。Plan / grok skills / 任意文件附件：hidden。  
`@`：**不是** grok 文件引用。仅当提供 `searchMentions` 且 capability.features 含 `mentions` 时显示 `@`，从**当前音乐库**拉列表并插入 `mentions[{kind:track,id}]`；未提供 callback 或 Host `mentions()` 为 None 则 hidden。候选 `label` 禁止绝对路径、`@/`、`file://`。不新增第九个 `agent_kit_*`。拆掉 waiting/Allow/Block。

未知 `KitBlock.kind`：TS 非穷举解析 → `unknown { unknown_kind }` 后画 Status / 跳过，禁止 zod strict 丢整事件；原始 payload 已由 Host 有意丢弃，前端禁止解释为扩展数据。

replay：`origin=replay` 或非当前 in-flight `turn_id` → `streaming=false`、零 animation。见到 `replay_complete` 再允许 live append。

`sending`：跟 `Status{cancelled|turn_completed|error}` 或 `sidecar_unavailable` 变 false，并带超时。

`submissionId`：每次新 Send `crypto.randomUUID()`；未 accepted 前重试复用；Host 重启后必须换新。

工具结果：预留 `renderToolResult` 插槽（Task 11 不得改 props 形状）。

- [ ] **Step 1:** 测引导：空 list → new；一条或多条 `is_active` → resume `updated_at` 最新，并列时取 list 顺序最后一条；无 `is_active` 且 list 非空也按同一排序恢复。onSend 带 session_id+submission_id+mentions snake_case；unavailable 不发送；replay 整表替换；未知 kind 不丢事件；未提供 `searchMentions` 隐藏 `@`
- [ ] **Step 2–4:** `npm test` / `npm run lint` / `npm run build`；exports 指向真实面板，不是演示壳；React 19 consumer smoke 无双 React/invalid hook call；导入面板 CSS 后宿主 button/textarea 不受全局 reset
- [ ] **Step 5: Commit**（web 仓）`feat(ui): useAgentKit reducer and embeddable agent panel`

---

### Task 9: 对照入口（可选，非完成条件）

产品若需要并排看旧面板与 Kit 面板：加一个临时 activity 挂 Kit。**不是**双 runtime 架构。对照结束删除旧入口时，只删产品旧代码，不改 Kit 命令名；**必须留下一个可见 Activity 入口**。

若不做对照，本 Task 可跳过，直接 Task 10 只挂 Kit。

- [ ] 若做：改 WorkActivityBar 测试，增加 sidecar 入口 testid；旧入口保持原样即可
- [ ] Commit（产品仓，可选）`feat(work): temporary kit assistant entry for comparison`

---

### Task 10: 产品 `HostApp` + `KitEventSink` + `dispatch`（禁止产品拼 ACP）

**前置：Task 7b 与 Task 8 均绿；并先关闭旧 `agent_test_provider` 密钥外送旁路。**

**Files:** 产品 `rust/src/api/agent_kit/`（`HostApp` 领域；**沿用现有密封**，不在本 Task 迁 Keychain）+ `src-tauri`（启动装配 + `KitEventSink` + 8 个 Kit JSON adapter）；设置页/wrapper 统一 Channel 成功态并关闭旧 test-provider；前端挂 `useAgentKit`，不要自写 listen/引导。

```rust
impl HostApp for PureLabHostApp { /* app_id / persist·load / seal / mcp 可空 / mentions() */ }

// setup（只一次）：
//   let runtime = HostRuntime::new(app, sink, cfg);
//   app.manage(runtime);
// 每个 command：
//   runtime: State<HostRuntime>
//   先按 Kit JSON 解码（未知 cmd → KitCommand::Unknown { cmd }），再 runtime.dispatch(cmd)
//   禁止 serde_json::from_value::<KitCommand> 让未知 cmd 变成 Tauri Err(String)
// 禁止：每条 invoke HostRuntime::new
// 禁止：Supervisor::acquire / AcpClient / session/prompt
```

运输名必须与架构一致。事件：`agent-kit-event`，payload = `KitProductEvent`。8 个 command 只允许冻结的主窗口 label `main`；Basket 与其它窗口 fail-closed，不 spawn、不 unseal。事件也只投递给有对应 scope 权限的 `main`。

`scope_id` = **已存在且当前已授权** 的 `library.id`。未知 / 未打开 → 产品侧 fail-closed，不要进 Host。cwd **不用** `library.root_path`。

`HostRuntimeConfig`：`home_root` = App Data 根；`sidecar_bin` = 开发机 CARGO / `tauri dev` 解析出的 path。M1 **不**做安装包。

依赖 path（兄妹目录，相对 path）：

```toml
efflab-agent-host = { path = "../../effilab-agent/crates/efflab/efflab-agent-host" }
```

禁止提交本机绝对 path。禁止依赖 sidecar 库。

`text` 长度上限跟 capability；产品也可先拒。解密失败 fail-closed。

`Send.mentions`（可选）：只允许当前 `scope` 库内曲目 id。PureLab `HostApp::mentions()` 返回独立 `HostAppMentions` 端口；Host `resolve` 后扩成中文再组 prompt，展开文本再次通过 prompt gate。label / text 禁止 `library.root_path`、绝对路径、`@/`、`file://`。未实现端口时带 `mentions` → `invalid_request`。

设置页保存必须调用 `agent_kit_set_llm_channel`；若保留旧保存函数，必须与 Host persist/restart 处于同一事务，禁止两套独立成功态。**接线前关闭旧 `agent_test_provider` 旁路**：删除前端调用 + Tauri 注册，或改为走 Host/L3b 同一 Endpoint policy，并遵守“改 URL 必须带新 Key”。禁止 `api_key=null` + 任意 URL 解密已存 Key外送。

- [ ] **Step 1:** 单测：每个 product command 只调用一次 dispatch、原样转发且不自行重试；产品仓 `rg "HostRuntime::new"` 只出现在 setup 一处；未知 cmd 得到结构化 `unsupported`；非 `main` 窗口和未知 scope 在 spawn/unseal 前失败；设置保存只有一个成功态；旧 test provider 无法旁路 L3b。幂等/二次 prompt 的 ACP 次数只在 Task 7b 测，不在产品层断言
- [ ] **Step 2–4:** 接线 / `npm test` + `npm run build` + 目标 Rust test / `tauri dev` smoke / 绿
- [ ] **Step 5: Commit** `feat(agent-kit): hostapp dispatch only, no product acp`

手动验收（**开发机**，不是安装版）：

1. BYOK 已配时发一句，看到流式 assistant；运行中修改全局 Endpoint/model/Key 后，全部存活 scope 完成 token 失效与重启，返回 committed view。
2. Stop 能停（投影落下）。停后再发新 turn。
3. New Chat 得到新 `session_id`；list 能看到；重启 Host 后 resume 重放且 `origin=replay`（不打字机）；有 `replay_complete`。
4. 未配 Key → **只接受** `llm_channel_unconfigured`，不 spawn、不打 grok-4.5。`missing_api_key` 仅用于 view 声称有 Key但 unseal 失败。
5. 对照旧入口若还在：互不影响；不是本 Task 必过项。

对外仍叫 **Kit 试点**，不要叫智能助手上线。用户语言验收写在 adapter 文档，不挡本 Task 工程门。

---

### Task 11: 只读领域 MCP（可后置）

优先 **HTTP loopback** `http://127.0.0.1:{port}/mcp/{scope}/{nonce}`，nonce 进 URL path，无 headers，无孙进程。

若 stdio：只能使用固定、版本化且身份可验证的 exec-root wrapper，`env_clear` 后再 exec；测试断言子进程看不到 `EFFLAB_L3B_BIND` / `XAI_API_KEY` / `GROK_*` / proxy / 用户 Key，且替换 wrapper、symlink、用户可写 helper 都启动失败。args 只传 `--scope-id`；schema **禁止** `root`。

冻结名：

| 项 | 值 |
|---|---|
| server | `purelab` |
| 裸名 | `search_tracks` |
| 模型可见 | `purelab__search_tracks` |
| 禁止 | `music_search`、双 `__`、合格名 >64 |

批准集 = 上述合格名 ∪ `GrokBuild:efflab_noop`。`_x.ai/mcp/list` 带 `sessionId`，按真实 `servers[].session.{status,tools[]}` 解析；local 工具用 `server.name + "__" + tool.name` 重建合格名，只比较 `status=ready && enabled`。本 Task 若做：多出 writeback/rename → kill；缺工具 / 不 Ready → session 级 `mcp_failed`，**不挡对话、不杀 sidecar**。不做本 Task = 批准集空，走 Task 7b TC-HP。

超过半天允许 Fake MCP + 路径断言。不得开放写回。权益门控：若做真搜索，走产品现有 search entitlement；Fake 不得进用户范围。

- [ ] **Step 1–5:** 红 / 注入 / 绿 / commit  
  `feat(agent-kit): readonly purelab__search_tracks mcp`

---

### Task 12: 嵌入清单 + grep 门禁（不是「第二产品已上线」）

**Files:** `effilab-agent/docs/embed-checklist.md`；可选产品 `tools/check-no-acp-in-product.sh`

清单：

1. 依赖 host crate + sidecar **二进制** + agent-web 包（含 `useAgentKit`）
2. 实现 `HostApp` + `KitEventSink`；注入 `HostRuntimeConfig`；**启动时 `new` 一次**并持有到进程退出
3. 只调 `HostRuntime::dispatch`；挂 `<AgentPanel />`。禁止 per-invoke `new`
4. 提供已授权 `ScopeId`；不要传库路径当 cwd
5. 只读 MCP（可空）；HTTP 优先
6. 产品仓 **禁止**：`session/prompt`、`AcpClient`、`validate_host_request`、`reqwest` 打 `chat/completions`、自写 Kit 归约、`efflab_agent_sidecar::`、`xai_grok_`

```bash
set +e
rg -n "session/prompt|session/update|AcpClient|validate_host_request|efflab_agent_sidecar::|xai_grok_" \
  src src-tauri rust --glob '!**/target/**'
status=$?
case "$status" in
  0) echo "发现禁止项" >&2; exit 1 ;;
  1) exit 0 ;;
  *) echo "rg 门禁自身失败，exit=$status" >&2; exit "$status" ;;
esac
```

本 Task **不是**第二个产品的发布验收，只是门禁文档。

- [ ] **Step 1–5:** 文档 + 脚本；产品仓期望 exit 0；commit  
  `docs: embed checklist and no-acp product gate`

---

## 本计划之后（不要写进上述 Task）

| 后续计划 | 内容 |
|---|---|
| Kit M2 Relay | L3b 已在；补 Idempotency-Key、402/403、产品双槽密封 |
| Production secret | fd3/fd4，禁 env；OS Credential Store 迁密封（不进 M1） |
| 写回 MCP | Preview + `HostAppConfirm`；写工具不进 sidecar |
| Windows cohort | hardening 完成前保持 unavailable |
| 安装包 / 签名 | 抄产品现网嵌套 bin；改 `bundle.externalBin` 时同步 MSIX / dev 旁路 |
| 删除产品旧 Agent | 对照结束后单独删入口与旧实现；**不改** Kit 协议名；留下一个 Activity |

## Self-review

1. **Spec coverage:** L1 Unknown cmd/error/block、可空 turn、mentions 注入/指纹、全局 Channel、Host 写盘 launch contract、contract crate、L3b streaming/SSRF/token 生命周期、ValidatedReply、Task 7b 闭环、MCP 真实 shape、React 18/19/CSS、文本语义门。
2. **旧路径：** 不为 Rig 做兼容；Task 9 可选对照；Task 10 前关闭旧 `agent_test_provider` 凭据/Endpoint 旁路。
3. **Types:** 以架构 §5 与 Task 1 golden 为准，后续 Task 不得再发明第三套命令名。
4. **依赖：** host 不链 sidecar / grok-shell。

## 验证（整计划结束后）

```bash
# effilab-agent
scripts/fork-sync-apply.sh --check
cargo test -p efflab-agent-contract
cargo test -p efflab-agent-host
cargo test -p efflab-agent-sidecar
set +e
cargo tree -p efflab-agent-host | grep -i grok
status=$?
case "$status" in 0) exit 1 ;; 1) ;; *) exit "$status" ;; esac
cargo check -p efflab-agent-contract -p efflab-agent-host -p efflab-agent-sidecar
cargo clippy -p efflab-agent-contract -p efflab-agent-host -p efflab-agent-sidecar --all-targets

# effilab-agent-web
cd app && npm test && npm run lint && npm run build
# React 19 consumer smoke + CSS isolation smoke

# 产品
# HostApp / dispatch / main-window authorization / old test-provider closure 相关 rust/前端 test
# npm run build
# tools/check-no-acp-in-product.sh
```
