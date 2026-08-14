# Task 4：AcpRuntime 实施报告

## 状态

完成。已创建且仅创建一次提交，未执行 push。

- 提交：`7f938f7 feat(host): acp runtime multiplexes notify request and reverse rpc`
- 工作分支：`lab_main`

## 实现内容

在 `efflab-agent-host` 中新增 `AcpRuntime`，以拆分后的 sidecar stdin/stdout 构造，公开 API 均为 `&self`：

- `request_validated`：先以逻辑 method 调用 `validate_host_request`，分配数值 `RequestId`，记录 `{ method, params }`，再写入 JSON-RPC request。
- `notify_validated`：先完成同一 Host contract 校验；当前只允许契约规定的 `session/cancel` notification，并保证 wire 中不含 `id`。
- `reply_validated`：只允许回复已保存的 reverse request；校验失败或 stdin 写入失败时恢复账本，允许调用方修正/重试。`session/request_permission` 的 selected `optionId` 必须出现在该次 request 的 `params.options[]`。
- `poll_inbound`：非阻塞读取独立 stdout 线程投递的 `Inbound::{Response, Notification, Request}`，不会因为长 request 等待而吞掉 notification 或 reverse RPC。
- 出站扩展 method 用逻辑名校验、wire 自动加 `_`（`x.ai/mcp/list` → `_x.ai/mcp/list`）；入站 ACP 扩展 wire method 去掉单个 `_` 前缀，恢复为逻辑名。
- 新增本地最小 JSON-RPC 类型：数值 `RequestId`、`RpcError`、`Inbound`、`ValidatedReply` 和 `METHOD_NOT_FOUND = -32601`；未链接 `agent-client-protocol`。
- stdin 只有私有 `write_message` 一个写入入口；无公共裸写 API，也没有输出任何 key/token。

## TDD Evidence

### RED

命令：

```bash
cargo test -p efflab-agent-host --test acp_runtime
```

结果：退出码 `101`。

关键失败输出：

```text
error[E0432]: unresolved imports `efflab_agent_host::AcpRuntime`,
`efflab_agent_host::Inbound`, `efflab_agent_host::RequestId`,
`efflab_agent_host::ValidatedReply`, `efflab_agent_host::METHOD_NOT_FOUND`
```

原因符合预期：测试先于实现写入，而 host crate 当时尚未定义或导出 Task 4 的运行时与 JSON-RPC 类型。

### GREEN

先执行新增 target：

```bash
cargo test -p efflab-agent-host --test acp_runtime
```

结果：退出码 `0`；6/6 测试通过。

最终定向验证：

```bash
cargo test -p efflab-agent-host
cargo fmt --all -- --check
```

结果：两条命令均退出码 `0`。

- `tests/acp_runtime.rs`：6 passed, 0 failed。
- `tests/protocol_and_submission.rs`：21 passed, 0 failed。
- 单元测试和 doc-tests：均通过；未运行整个 workspace。

## 测试覆盖

新测试使用真实子进程的 stdin/stdout 管道：子进程 stdout 注入 sidecar JSON-RPC，stderr 捕获 Host 写入，未 mock JSON-RPC wire。

1. `initialize.capabilities.terminal=true` 在任何 stdin 写入前被 contract 拒绝。
2. `session/cancel` notification 的 JSON 不含 `id`。
3. 逻辑 `x.ai/mcp/list` 经验证后写出 `_x.ai/mcp/list`。
4. 一个 request 在 result 前收到 `session/update` notification 时，二者以 `Inbound` 队列顺序保留。
5. direct `session/request_permission` 与 wire `_x.ai/ask_user_question` 都解码为 `Inbound::Request`；permission 的未提供 option 被拒绝，`allow-once`（本次 options 成员）成功写回相同 id 的 result。
6. 未知扩展 reverse request 使用 `ValidatedReply::Error { code: METHOD_NOT_FOUND, ... }` 写回同 id 的 JSON-RPC error，而非静默丢弃。

按控制器决议，Task 4 只验证 permission `optionId` 属于本次 options；测试正例固定使用 `allow-once`，没有将 `enable-always-approve` 作为正例。产品层的选择策略留给 Task 7b。

## 变更文件

- `crates/efflab/efflab-agent-host/src/acp_runtime.rs`
  - 新增 multiplexed ACP stdio Runtime、账本、入站分类、reply 校验和唯一写入边界。
- `crates/efflab/efflab-agent-host/src/lib.rs`
  - 注册并导出 Task 4 Runtime 类型；同步 crate 模块说明。
- `crates/efflab/efflab-agent-host/tests/acp_runtime.rs`
  - 新增真实管道的 TDD 集成测试。

## 自审

### Completeness

- 已逐项覆盖简报 API、逻辑名/`_` wire 映射、contract 前置校验、数值 request id、response/notification/reverse request 复用、permission options 成员校验和未知 request error 回复。
- `session/cancel` 被额外约束为 notification，避免通过 request API 错误发送带 id 的 cancel。
- 回复账本在校验或写失败时恢复，避免一次失败导致待回复 reverse request 丢失；成功后才消费。

### Quality / Security

- stdin 由 `Mutex<Box<dyn Write + Send>>` 独占，stdout 由后台线程独占；所有公开发送接口均为 `&self`。
- 新增逻辑均有简体中文注释；没有日志输出 params、key 或 token。
- host crate 未增加 sidecar/grok-shell/agent-client-protocol 依赖；未实现 supervisor、spawn、projector 或其他 Task 5+ 范围。
- `git diff --cached --check` 在提交前通过；提交前也再次通过 `cargo fmt --all -- --check`。

### 已知边界

- 测试文件使用 `cfg(unix)` 和 `sh` 子进程取得真实 pipe 行为；这与现有 sidecar stdio 测试的 Unix/macOS 基线一致。Runtime 本身仅依赖 `Read + Write + Send`，未引入 Unix-only 库 API。
- 未发现阻断问题或待修复项。

## 证据清单

### 已读文件（关键）

- `.superpowers/sdd/2026-08-13-effilab-agent-kit-implementation-plan/task-4-brief.md`：固定 API、TDD 与测试要求。
- `.superpowers/sdd/2026-08-13-effilab-agent-kit-implementation-plan/task-4-context.md`：类型、所有权、wire、测试和提交决议。
- `crates/efflab/efflab-agent-sidecar/tests/acp_stdio.rs`：真实 sidecar stdio request/notification JSON-RPC 形状与 `_x.ai/mcp/list` wire 前缀。
- `crates/efflab/efflab-agent-sidecar/tests/common/acp_client.rs`：数值 request id、逐行 JSON-RPC 和分离读线程行为。
- `crates/efflab/efflab-agent-contract/src/host_contract.rs`：`validate_host_request` 与 `HostPolicy` API、`session/cancel` 和逻辑扩展 method 规则。
- `docs/host-acp-contract.md`：反向 request、permission reply 与 unknown-request error 合同。

### 已运行命令（关键）

- `rg ...`：检索 sidecar 测试、contract API 与反向 RPC wire 证据。
- `cargo test -p efflab-agent-host --test acp_runtime`：RED 后、GREEN 前后验证。
- `cargo test -p efflab-agent-host`：最终 27 个集成测试通过。
- `cargo fmt --all -- --check`：最终通过。
- `git diff --check` / `git diff --cached --check`：提交前通过。

### 关键定位

- `crates/efflab/efflab-agent-host/src/acp_runtime.rs:111-235`：拆分 I/O、validated request/notification/reply、账本恢复。
- `crates/efflab/efflab-agent-host/src/acp_runtime.rs:237-323`：非阻塞入站轮询与独立 stdout 读循环。
- `crates/efflab/efflab-agent-host/src/acp_runtime.rs:326-480`：JSON-RPC 分类、扩展名还原和 permission options 校验。
- `crates/efflab/efflab-agent-host/tests/acp_runtime.rs:128-322`：真实管道 TDD 测试。

## Task 4 Review Fix Report

### 修复内容

- 读端生命周期：Unix/macOS reader 使用独立 shutdown fd 与 `poll` 同时监听 stdout，`AcpRuntime` 保存 `JoinHandle`；新增 `shutdown()` 与 `Drop`，先唤醒并 join reader，再关闭 stdin。stdout EOF、读错误、协议错误和队列溢出均写入固定终止状态，`poll_inbound` 在队列消费完后返回 `Err`，并清理 Host/reverse 两份 pending 账本。
- 重复 reverse id：解码 reverse request 时使用 `BTreeMap::entry`；已存在的 id 只投递可观察错误，不覆盖第一次保存的 method/params/options，原 request 仍可用第一次 options 回复。
- 有界资源：入站 channel 改为容量 64 的 `sync_channel`，入站 reverse request 与出站 Host request 账本均限制为 64；超限 fail-closed 并清理账本，队列超限不静默丢弃通知。
- method 还原：仅将 `_x.ai/` 还原为 `x.ai/`，其它带前导 `_` 的 method 原样保留。
- 依赖：Host crate 增加 workspace `libc`，仅用于 Unix fd `poll` 的可中断 reader shutdown。

### TDD 与测试

先补充失败测试后实现：

- `cargo test -p efflab-agent-host --test acp_runtime`（实现前退出码 `101`，新增 `shutdown` API 尚不存在）。
- `stdout_eof_is_reported_as_transport_error`：stdout EOF 不再伪装为 `None`。
- `runtime_drop_closes_stdout_reader_before_worker_join`：Drop 后 descendant 写 stdout 得到 `reader-closed`，验证 reader fd 已关闭且 worker 已回收。
- `duplicate_reverse_id_preserves_original_permission_options`：相同 id、不同 options 被拒绝，第一次 `first-option` 仍可回复。
- `outbound_request_ledger_is_bounded_before_write`、`inbound_reverse_request_ledger_is_bounded_and_fails_closed`：验证 pending 上限、超限错误和终止清理。
- `inbound_notification_queue_overflow_is_observable`：验证队列溢出可观察且未静默吞掉通知。
- `non_extension_leading_underscore_is_preserved`：验证非扩展 method 的 `_` 不被移除。

### 最终验证命令与输出

```text
cargo test -p efflab-agent-host
退出码: 0
结果: 13 个 acp_runtime 集成测试通过；21 个 protocol_and_submission 集成测试通过；unit tests 0 个、doc-tests 0 个，均通过。

cargo fmt --all -- --check
退出码: 0
结果: 格式检查通过。
```

### 交付边界与注意事项

- 未执行 push，未修改 Task 5+；所有修复、测试脚手架、Cargo.lock 与本报告应在同一修复提交中。
- Unix/macOS 路径提供可中断 fd reader；非 Unix 保留兼容读循环，但通用阻塞 `Read` 在没有平台关闭能力时仍依赖其自身 EOF 才能结束 worker，这是当前跨平台实现边界。
