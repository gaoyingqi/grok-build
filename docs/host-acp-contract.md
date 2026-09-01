# efflab-agent-sidecar Host ACP 契约

> 版本：2026-08-31 · 同步为现行 v1 合同（RuntimeConfigV1 / Kit Host / sidecar 现有实现）；不自我声明 production Go
> 适用范围：`efflab-agent-sidecar` 与可信 Host 之间的 ACP stdio 通信。
> 配套实现：`crates/efflab/efflab-agent-contract`（校验 / DTO / 渲染）与
> `tests/fixtures/host_contract_cases.json`（跨语言 Host 复用同一 fixture）。
>
> 通用 Host Runtime 在 `crates/efflab/efflab-agent-host`，发送前调用本契约。
> 各产品 App **不要**再抄一份，也不要直接写 stdin。见
> [plans/2026-08-13-effilab-agent-kit-host-architecture.md](plans/2026-08-13-effilab-agent-kit-host-architecture.md)。

## 1. 信任模型

- sidecar 的 **stdin 只连接可信 Host**（本契约的约束方）。
- `validate_host_request` 是 **Host 侧校验库**（住在 `efflab-agent-contract`）：每个**出站**请求/通知/**reply** 发送前字段白名单（fail-closed）。
- sidecar 进程入站 **不再**跑同一份白名单。真正边界：私有 GROK_HOME、env 卫生、
  `injectDefaultTools: false`、受控 MCP、L3b（用户 Key 不进 sidecar env）。因此 Host 必须是 stdin 的唯一写入者。
- `AcpRuntime::{request,notify,reply}_validated` 是 **唯一** 写 stdin 的函数。`reply_validated` 接收 `ValidatedReply::Result | ValidatedReply::Error`，并按保存的反向 request method/params 校验。禁止 `pub` 裸 write。
- 产品仓出现手写 ACP / `session/prompt` / `use efflab_agent_sidecar` 即违反 Kit 嵌入合同。
- Host **永不**发送 `getApiKey` / `setApiKey` / `getBearerToken`（保持 UnknownMethod）。

## 2. 传输与格式

- ACP stdio：每行一条 JSON-RPC 2.0（`\n` 结尾）。
- **扩展方法 wire 名必须带 `_` 前缀**（ACP decoder 要求），例如 `_x.ai/mcp/list`。
- **校验名**（`validate_host_request` 的 `method` 参数）使用去掉前导 `_` 的逻辑名，
  例如 `x.ai/mcp/list`。`AcpRuntime` 必须：校验用逻辑名，写 stdin 用 wire 名。
  不得「先校验后原样发送」把 `_` 带进校验器（会 `UnknownMethod`），也不得把无 `_` 的扩展名写进 stdin。
- stdout 只承载 ACP JSON-RPC；sidecar 日志一律走 stderr。
- 请求有 `id`，等 result。**通知无 `id`、无 result**（`session/cancel`）。
- 长请求（`session/prompt`）期间 stdout 会穿插 `session/update` 通知 **以及** 反向 request
  （`session/request_permission` 等）：读循环必须独立于「等 result」，且必须能 `reply`。
- `Inbound = Response | Notification | Request`。缺少 `Request` 会把工具回合挂死。

## 3. 出站方法面（M1）

`expect` 方言与 fixture 一致：`allow` / `reject`（不是 `ok`）。

`_meta` **按 method 分表**。实现必须是 `meta_keys_for(method)`，禁止一份全局 `allowed_meta_keys` 打天下。

| method（逻辑名） | 类型 | 允许字段 | `_meta`（M1） | 拒绝 |
|---|---|---|---|---|
| `initialize` | request | `protocolVersion`（须与 Host 钉的 ACP 版本一致）、`clientCapabilities.fs.readTextFile=false`、`clientCapabilities.fs.writeTextFile=false`、`clientCapabilities.terminal=false`、`clientInfo.name`、`clientInfo.version` | 空 | 任一客户端 fs/terminal 能力为 true、真实 unstable 能力 `auth` / `elicitation` / `nes` / `positionEncodings`、未知键、任何嵌套 `_meta` 键 |
| `session/new` | request | `cwd`（= `--session-cwd` canonical 精确匹配）、`mcpServers=[]` | **仅** `modelId`（Channel 槽名，如 `byok`） | `sessionId`、`agentProfile`、`pluginDirs`、`x.ai/hooks`、`yoloMode`、`capability`、`permissionMode`、cwd 不匹配、MCP 非空、`promptId` |
| `session/load` | request | `sessionId`（必填且为非空字符串）、`cwd`（同上）、`mcpServers=[]` | **仅** `modelId` | 同 `session/new`；缺失或非法 `sessionId`、cwd 不匹配、MCP 非空、`promptId` |
| `session/list` | request | `cwd`（强制 = session_cwd）、可选 `cursor` | 空 | **顶层 `limit`**（ACP 0.10.4 / schema 0.11.4 的 `ListSessionsRequest` 无此字段）；真实 wire 字段 `additionalDirectories`（本产品 profile 拒绝）；web/产品传入的其它 cwd；`allowRelax`；`mcpServers`；facet / 远程 roster；任何 `_meta` 键 |
| `session/prompt` | request（长） | `sessionId`、`prompt`（**ContentBlock 数组**，见 §3.1） | **仅 `promptId`**（= Kit `submission_id`） | 扁平 `text`、image/resource、未知顶层字段、`modelId`、把 ACP union 暴露给 web |
| `session/cancel` | **notification** | `sessionId` | 空 | 当成 request 等 result；任何 `_meta`（含 `yolo` / `rewindIfNoOutput` / `cancelSubagents` / `promptId`） |
| `_x.ai/mcp/list` | request | `sessionId`（**M1 必填**；无 session 时 catalog 的 local tools 为 `None`） | 空 | 未知 `_meta` 键；`cache` 字段（不在白名单；默认 true 可接受） |

**M1 明确拒绝（保持 UnknownMethod）：**

- `session/set_model`（换模型 = 改 Channel + `session/new`）
- `session/resume`（用 `session/load`）
- `x.ai/session/update_mcp_servers` 及热更新
- `_x.ai/session/list` / `x.ai/sessions/list`（更宽：`allowRelax` / facet / conversations）
- `getApiKey` / `setApiKey` / `getBearerToken`
- 其它未知 method

`modelId` 白名单 = Channel 槽名（`byok` / 以后 `relay`），不是供应商模型字符串。

列会话 **只走标准 ACP `session/list`**。默认页大小走 sidecar/unified_list（约 30），Host 用 `cursor` 翻页。禁止 Host 把扫 `GROK_HOME/**/summary.json` 当成对外协议。

supervisor 环境白名单 **拒绝** `GROK_CHAT_MODE`（chat mode 下标准 `session/list` 会 `method_not_found`；本快照恒为 false，不要注入）。权威 config 固定 `[session] load_envrc = false`，sidecar 启动校验最终解析值；Host request / `_meta` 不得重新开启 envrc。

### 3.1 `session/prompt` 嵌套与文本语义门

每个 `prompt[]` 元素 **恰好** `type=="text"` 与 `text:string`。禁止：

- `resource` / `resource_link` / `image` / 未知 type
- 块级额外键（含 `_meta`）
- 扁平 `text` 顶层字段

**文本语义门**（Host 组包前 **且** contract 对 `text` 再验，fixture 与 grok `prompt_parser` 对拍）：

只挡 grok-shell 文件引用，不挡曲库话术。上游会把任意 `@` 后的首个非空白 token 当作 FileReference 候选，因此必须按真实 tokenization 拒绝，而不是只匹配少数路径前缀。

- **拒**：任意会被上游解析的 raw `@token`，至少包括 `@secret.txt`、`@foo/bar`、`@../`、`@..\\`、`@~/`、`@C:\\`、UNC / extended Windows path；大小写任意的 `file://`；引号、标点、换行边界也必须覆盖。
- **放行**：没有 `@` 前缀的正文绝对路径，例如「`/Volumes/Music/Inbox` 这批怎么标」。
- 曲库 `@` 只走 `mentions[]`。Host 展开后的普通中文必须再次通过同一道门；label / 展开文本禁止绝对路径、`@/`、`file://`。见架构 §5.8。

`clientCapabilities.fs.readTextFile=false` 与 `clientCapabilities.fs.writeTextFile=false` **不等于**模型输入安全门。它们只表示 ACP 客户端不提供文件读写请求；空 workspace 也挡不住绝对路径文本，所以必须靠本门 + Host 展开。

正例：纯中文；含 `/Volumes/Music/Inbox` 的句子。反例：`resource_link`、混 `image`、`@secret.txt`、`@foo/bar`、`@../x`、`@C:\\secret.txt`、UNC、`FILE:///…`。

## 4. 入站（Host 消费）

sidecar → Host，**不**走 `validate_host_request`（那是出站库）。

### 4.1 通知

| 入站 | M1 处理 |
|---|---|
| `session/update`：`agent_message_chunk` / `agent_thought_chunk` / `tool_call` / `tool_call_update` / user 回显 | 译成 `KitBlock` |
| 同上，且 `_meta.isReplay=true` | 同译，Kit 事件 `origin=replay` |
| `plan` / `todo` / `available_commands_update` 等禁用或非 M1 块 | **跳过并内部计数**（projector 计数器 + debug 日志），**不再发送** `replay_skipped` / `skipped_update` 可见 Status。**不失败整轮** |
| 未知 `sessionUpdate` 变体 | 同上，禁止把原始 ACP 当 generic data 甩给 web |
| `_x.ai/session/update` 扩展 | M1 **丢弃**并内部计数（debug 日志），不算错误；不生成可见 Status |
| 非 JSON / 非 ACP stdout | 视为污染：杀进程 + Kit `Error` |

`session/update` 的 `sessionId` 只用于将事件归属到对应会话；一个 scope 进程可同时维护多个 active session，`current_session` 只是最近一次 `session/new` / `session/load` 指针，不限制其它 active session 的 live update、transcript 或 hot resume。

`_meta.eventId` 作为 Kit `event_id` 去重。缺省 `"{session_id}:{origin}:{sequence}"`。

`initialize` **result** 由 Host 按 **严格能力闭集** 校验，任何未知能力字段一律拒绝握手、不映射进 Kit capability。`agentCapabilities` 不存在 `fs` 或 `terminal` 字段；这两个字段只属于请求侧 `clientCapabilities`。`authMethods` 必须为空数组。完整握手合同见 §4.3。

ACP schema 0.11.4 将 `protocolVersion` 定义为必填，将 `clientCapabilities` 定义为带默认值的可选字段，将 `clientInfo` 定义为可选实现信息；本 Host 出站合同为避免能力默认值绕过安全边界，要求显式提供 `clientCapabilities.fs.readTextFile=false`、`clientCapabilities.fs.writeTextFile=false`、`clientCapabilities.terminal=false`，并要求 `clientInfo.name` 与 `clientInfo.version` 为非空字符串。`clientInfo` 的可选 `title`、嵌套 `_meta` 以及 `clientCapabilities` 的其它扩展能力不属于本产品 profile，统一拒绝并返回稳定字段路径。

load 重放结束后 Host 发 Kit session 级 `Status { code: "replay_complete" }`（这是 Kit 事件，不是 ACP method）：`turn_id=null`、`submission_id=null`，`event_id="{session_id}:host:replay_complete:{sequence}"`，`block_id=event_id`。旧的 `replay_skipped` / `skipped_update` 只保留为解析兼容的开放 status code，现行 Host **不再发送或恢复**它们（不在 recoverable 白名单）：未知/不支持 update 只内部计数，不产生可见 Status。`mcp_failed` 仍按同一 session/process 级规则由当前状态重建（live），禁止伪造 turn id。

### 4.2 反向 request（有 `id`，必须回）

`optionId` 从本次 `params.options[]` 取，禁止自造、禁止 `options[0]`。

允许（工具 ∈ 批准集 ∪ `GrokBuild:efflab_noop`，且 options 含 `allow-once`）：

```json
{"jsonrpc":"2.0","id":1,"result":{"outcome":{"outcome":"selected","optionId":"allow-once"}}}
```

拒绝（不在批准集；优先 `reject-once`）：

```json
{"jsonrpc":"2.0","id":1,"result":{"outcome":{"outcome":"selected","optionId":"reject-once"}}}
```

取消（Stop / 找不到对应 option / 不要 YOLO）：

```json
{"jsonrpc":"2.0","id":1,"result":{"outcome":{"outcome":"cancelled"}}}
```

| method | M1 回复 | 投影 |
|---|---|---|
| `session/request_permission` | 上表。禁止回字符串 `"allow_once"`。禁止选 `enable-always-approve` | 可 `KitBlock::Tool`；不甩原始 ACP |
| `x.ai/ask_user_question` | 取消/拒绝 | 仅内部计数（debug 日志），**不生成可见 Status** |
| `x.ai/exit_plan_mode` | 拒绝 | 仅内部计数（debug 日志），**不生成可见 Status** |
| 其它带 `id` 的未知 request | 错误 result（不要静默丢） | 仅内部计数（debug 日志），**不生成可见 Status** |

回复走以下唯一接口：

```rust
enum ValidatedReply { Result(Value), Error { code: i64, message: String } }
fn reply_validated(&self, id: RequestId, reply: ValidatedReply, policy: &HostPolicy) -> Result<()>;
```

`AcpRuntime` 必须保存 `request_id → { method, params }`；permission 的 `optionId` 必须属于保存的本次 `options[]`。其它带 `id` 的未知 inbound request 使用 `ValidatedReply::Error`（`method_not_found` 或本契约指定固定错误），禁止静默丢。fixture 同时覆盖 direct `session/request_permission` 与 `_x.ai/...` wrapper。自动批只读工具 **不是** `HostAppConfirm`，也不进 web。

### 4.3 `initialize` 握手（严格能力闭集）

请求侧由 `validate_host_request` 白名单校验（§3 表格）：固定 `protocolVersion`（= 当前 ACP v1）、`clientCapabilities.fs.readTextFile=false` / `.writeTextFile=false`、`clientCapabilities.terminal=false`、`clientInfo.name` / `.version` 非空字符串。Host 只发送这一固定形状（`clientInfo.name="efflab-agent-host"`、`version` 取 host crate 版本），不携带产品元数据，任何额外的 fs/terminal 能力、`_meta` 键或未知顶层字段都会在发送前被拒绝。

sidecar 的 `initialize` result 是 **固定闭集**，Host 用 `has_exact_json_keys` 逐层校验；任何多余、缺失或类型不符的字段都导致握手失败（`sidecar_unavailable`），绝不按“尽量容忍 schema”放行：

- 顶层 **恰好** `protocolVersion` / `agentCapabilities` / `authMethods` / `_meta` 四键。
- `protocolVersion` 必须等于 Host 钉住的 ACP 版本；`authMethods` 必须为空数组。
- `_meta` **恰好** `efflabRuntime="minimal-v1"`、`efflabSchemaVersion=1`、`efflabSessionStoreVersion=1`（固定握手身份，不夹带 runtime config 原文）。
- `agentCapabilities` **恰好** `loadSession` / `promptCapabilities` / `mcpCapabilities` / `sessionCapabilities` / `auth`：
  - `loadSession=true`；
  - `promptCapabilities` 恰好 `image=false` / `audio=false` / `embeddedContext=false`；
  - `mcpCapabilities` 恰好 `http=false` / `sse=false`（ACP 对外不广告 MCP transport；sidecar 内部仍可按 RuntimeConfigV1 消费已批准的字面量 loopback HTTP MCP）；
  - `sessionCapabilities` 恰好 `{ "list": {} }`（空对象，不借能力字段开放其它 session 方法）；
  - `auth` 必须为空对象（出现 `logout` 或任何其它字段都拒绝）。

sidecar 的 `agentCapabilities` **不存在** `fs` / `terminal`（这两个字段只属于请求侧 `clientCapabilities`）。未广告的 `authenticate` 固定返回 `method_not_found`。

**失败行为**：握手 result 校验失败或应答错误 → Host 进入 dead（`sidecar_unavailable`）；握手通过前已提交的命令保持 deferred，不写出站 ACP。错误消息不回显 sidecar payload。

## 5. 会话与工具

- MCP server 只能来自 RuntimeConfigV1 的 `approved_mcp`（见 §5.1），**只允许字面量 loopback HTTP**；能够解析为 `McpServerSpec::Stdio` 的 RuntimeConfigV1 条目统一稳定错误 `stdio_mcp_unavailable`，无法解析的 malformed config 仍归类 `runtime_config_invalid`。旧 `--mcp-config` / `--mcp-exec-root` / `env_clear` stdio wrapper 不再是 v1 合同（已由 `--runtime-config` 取代）。Host 请求中任何 `mcpServers` 必须为空数组。
- sidecar 唯一内置工具：`GrokBuild:efflab_noop`（无副作用占位）。
- 模型可见工具 = 内置工具 + 受控 MCP（wire 名 `<server>__<tool>`，恰好一段 `__`；server 名本身不得包含 `__`，且 ≤64 字节；server/tool 两段均为非空且匹配 `^[a-zA-Z_][a-zA-Z0-9_-]*$`；tool segment 不受 64 字节总长限制，完整名称进入 sidecar catalog/记录时仍受 1024 字节持久化上限）。
- `ApprovedMcpSpecV1`（`HostApp::mcp_for_scope` 返回）先拒绝 stdio，再校验字面量 loopback HTTP、期望工具名并计算稳定审核 `revision`；进 RuntimeConfigV1 时只携带 HTTP `url`，不含 command/args 或长尾字段。`ApprovedMcpConfig::write_toml` 与 `load` 的字段闭集对偶仍只用于旧权威 config 路径。产品禁止手写 TOML 字符串。
- `session/new` 之后 Host 应 `_x.ai/mcp/list`（**带 `sessionId`**）。真实 response shape 是 `servers[].session.{status,tools[]}`；local `tools[].name` 可能已去掉 server 前缀，Host 必须用 `server.name + "__" + tool.name` 重建合格名。只比较 `status=ready` 且 `enabled=true` 的工具。MCP **可选**，不挡对话：
  1. 批准集空：空 catalog / 仅 `GrokBuild:efflab_noop` / list 失败 → 通过，继续 prompt
  2. 批准集非空但启动失败、超时、不 Ready、list 出错 → **不杀** sidecar；session 级 `Status { mcp_failed }`；模型看不到那些工具
  3. 批准集非空且 Ready：重建后比较合格名。多出未批准工具 → **kill**；缺工具 → `mcp_failed`，不杀
- 批准集来自 `HostApp::mcp_for_scope` 的期望合格名。空集 = 该产品不走 MCP。
- 改 MCP 配置 = 重启该 scope 进程，不热更新；这与产品全局 `SetLlmChannel` 的“重启所有存活 scope”是不同语境。
- M1 领域 MCP **只允许字面量 loopback HTTP**（`http://127.0.0.1:PORT/path` 或 `http://[::1]:PORT/path`：显式非零端口、非空 path、禁止 `?` / `#`）。无孙进程，无从继承 sidecar env；tool schema **禁止** `root`。stdio（含旧 `env_clear` wrapper）不是 v1 传输。

### 5.1 RuntimeConfigV1 启动配置（v1 合同）

Host 在 spawn 前渲染唯一权威启动文件 `<home>/runtime-config.v1.toml` 并以 `--runtime-config` 传入；sidecar **不回退**旧 `config.toml`。启动参数只有：

```text
--runtime-config <home>/runtime-config.v1.toml --home <私有home> --session-cwd <隔离会话目录> --stdio
```

- `--stdio`：当前唯一支持（其它传输启动即拒绝）；stdout 只承载 ACP JSON-RPC，日志固定写 stderr。
- `--home` / `--session-cwd`：私有 home 与隔离会话目录，必须不同且互不为祖先；v1 sidecar 拒绝旧 `--grok-home` alias。
- 渲染与校验在 `crates/efflab/efflab-agent-contract/src/render.rs`；加载闭包与路径硬化在 sidecar `sidecar_config.rs` / `hardening.rs`。

RuntimeConfigV1 字段为闭集（`deny_unknown_fields`）：

| 字段 | 约束 |
|---|---|
| `schema_version` | 固定 `1` |
| `runtime_revision` | 不含自身的规范化 JSON 之 SHA-256 摘要（`sha256:…`）；sidecar 加载时按固定字段顺序重算，不匹配即拒绝启动 |
| `session_store_version` | 固定 `1` |
| `session_cwd` | 绝对 UTF-8、无 `..`、≤4096 字节，必须与 `--session-cwd` 精确一致 |
| `model.model_id` | 匹配 `^[A-Za-z0-9._:-]+$` 且 ≤128 字符 |
| `model.base_url` | 字面量 loopback HTTP 且 path 精确为 `/v1` |
| `model.backend` | 固定 `chat_completions` |
| `model.token_env` | 固定 `EFFLAB_L3B_BIND` |
| `approved_mcp.servers.*` | 只允许字面量 loopback HTTP `url`（显式非零端口 + 非空 path，无 `?` / `#`）；能够解析为 `McpServerSpec::Stdio` 的条目为 `stdio_mcp_unavailable`，malformed config 为 `runtime_config_invalid` |
| `expected_tools` | 字典序 qualified 工具名集合；loader 与 `approved_mcp_revision` 复用同一 qualified-name 语法，空集合法；tool segment 的长名与 sidecar 记录上限分开处理 |

`runtime_revision` 是防篡改摘要：字段顺序固定、不含自身字段、不包含任何秘密或 command/args；`load_runtime_config_v1_from_str` 在 schema / stdio / revision 全部通过后才返回配置。缺失、非法或 revision 不匹配的配置文件以启动拒绝失败（退出码 `2`），运行时错误退出码 `1`。

## 6. 生命周期

- 关闭：Host 关闭 stdin → sidecar 约 3.5s 内退出（码 0）。
- 测试平台边界：使用 FIFO/Unix shell fake sidecar 的 stdio 集成测试明确为 Unix-only；当前 Windows 只验证 capability=unavailable 与 Windows API 编译单元，不把 Unix 运行结果表述为 Windows fake sidecar 通过。
- 超时兜底：TERM → 2s → KILL（Windows 对等 API 必须存在：Job Object + `TerminateProcess`）。
- 退出码：正常 EOF=`0`、启动策略拒绝=`2`、runtime 错误=`1`。
- in-flight `session/prompt` 时禁止 idle-kill；先发 `session/cancel` 通知，再关 stdin。
- KILL 后保留 `GROK_HOME`（会话真源在磁盘）。
- 孤儿：子进程组 / parent-death kill。`{GROK_HOME}/.efflab-sidecar.lock` 由 sidecar 唯一持有；Host 禁止竞争同一把 home lock，只维护独立 process-slot metadata（scope / pid / generation / lifecycle）。

建议超时（Host 侧，可测）：

| 调用 | 超时 |
|---|---|
| `initialize` / `session/new` | 20s |
| `session/load` | 60s |
| `session/prompt` result | 无硬超时或 ≥ 10min；UI 用 cancel |
| `_x.ai/mcp/list` | 20s；超时当 `mcp_failed`，不杀进程 |
| stdin EOF | 目标 3.5s |

禁止对 `session/prompt` 自动重试。cancel 之后仍要收 prompt result（或进程死）才清 in-flight。

## 7. 扩展四件套

每新增一个出站 method、reply shape，或收紧/放宽字段：

1. 改本文件表格
2. 改 `tests/fixtures/host_contract_cases.json`（正例 + 未知字段反例，`allow`/`reject`）
3. Host `AcpRuntime` 只经 `validate_host_request` 发送，并做 `_` 映射
4. projector / 反向 request 表更新；未列的 x.ai 扩展默认 drop 或 deny reply

## 8. 测试

```bash
cargo test -p efflab-agent-contract --test host_contract
cargo test -p efflab-agent-sidecar --test host_contract
cargo test -p efflab-agent-sidecar --test acp_stdio
```

跨语言 Host：读 `host_contract_cases.json`，按 `expect` 断言。用例数量以 fixture 为准，本文不再写死条数。

Task 3 fixture 至少覆盖：

- `session/prompt` 允许 `_meta.promptId`，拒绝 `_meta.modelId` / 扁平 `text` / `@secret.txt` / `@foo/bar` / `@../x` / `@C:\\secret.txt` / UNC / 大小写 `file://`
- `session/new` 允许 `_meta.modelId=byok`，拒绝 `promptId`
- `session/cancel` / `session/list` 拒绝任何 `_meta` 键
- `session/list` 拒绝 `limit`、`allowRelax`、非本 scope cwd、真实 wire 字段 `additionalDirectories`
- `session/cancel` 拒绝 `rewindIfNoOutput`
- workspace `.envrc` marker 在 `session/new` / 冷 `session/load` 均不执行，最终 `load_envrc=false`

Task21 对齐用例（精确命名，供跨语言 Host 与 crate 测试按名断言）：

- `session_prompt_with_flat_text`：reject（扁平顶层 `text`）
- `session_prompt_with_prompt_id`：allow（`_meta.promptId` + ContentBlock 数组）
- `session_list_with_limit`：reject（顶层 `limit`）
- `ext_session_list_is_rejected`：reject（`_x.ai/session/list`）
