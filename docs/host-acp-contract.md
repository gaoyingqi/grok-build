# efflab-agent-sidecar Host ACP 契约

> 版本：2026-08-11 · 里程碑：macOS isolated runtime integration POC
> 适用范围：`efflab-agent-sidecar` 与可信 Host 之间的 ACP stdio 通信。
> 配套实现：`crates/efflab/efflab-agent-sidecar/src/host_contract.rs` 与
> `tests/fixtures/host_contract_cases.json`（非 Rust 语言 Host 可复用同一 fixture）。

## 1. 信任模型

- sidecar 的 **stdin 只连接可信 Host**（本契约的约束方）。
- `host_contract.rs` 是 **Host 侧校验库**：Host 在发送每个请求前用它做字段白名单
  校验（fail-closed），避免越界字段进入 shell 运行时。
- sidecar 进程自身的边界由私有 GROK_HOME、env 卫生、`injectDefaultTools: false`、
  受控 MCP 输入共同保证，不依赖 Host 的自觉。

## 2. 传输与格式

- ACP stdio：每行一条 JSON-RPC 2.0（`\n` 结尾）。
- **扩展方法 wire 名必须带 `_` 前缀**（ACP decoder 要求）：
  - `_x.ai/mcp/list`
  - `_x.ai/mcp/servers_updated`（通知）
  - `_x.ai/mcp_initialized`（通知）
- stdout 只承载 ACP JSON-RPC；sidecar 日志一律走 stderr。

## 3. 请求字段白名单

| method | 允许字段 | 拒绝字段（出现即拒绝） |
|---|---|---|
| `initialize` | `protocolVersion`、`client.name`、`client.mcpServers=[]`、`capabilities.terminal=false`、`capabilities.fs=false`、`_meta`（仅白名单键） | `capabilities.terminal=true`、`capabilities.fs=true`、`client.mcpServers` 非空、`_meta` 未知键 |
| `session/new` | `cwd`（= `--session-cwd` canonical 精确匹配）、`mcpServers=[]`、`_meta`（仅白名单键） | `agentProfile`、`pluginDirs`、`x.ai/hooks`、`yoloMode`、`capability`、`permissionMode`、cwd 不匹配、client MCP 非空 |
| `session/load` | 同 `session/new` | 同 `session/new` |
| `_x.ai/mcp/list` | 任意（只读） | `_meta` 未知键 |
| 其他 | — | 未知 method（fail-closed） |

- `_meta` 键白名单与 `modelId` 白名单由 HostPolicy 配置（`with_meta_key` /
  `with_model_id`）。
- `modelId` 出现在 `_meta` 或 params 顶层时必须在白名单内。

## 4. 会话与工具

- MCP server 只能来自 `--mcp-config`（stdio command 限定在 `--mcp-exec-root`，
  HTTP 仅 loopback）；Host 请求中任何 `mcpServers` 必须为空数组。
- sidecar 唯一内置工具：`GrokBuild:efflab_noop`（无副作用占位）。
- 模型可见工具 = 内置工具 + 受控 MCP server 的工具（wire 名 `<server>__<tool>`）。

## 5. 生命周期

- 关闭：Host 关闭 stdin → sidecar 在约 3.5s 内正常退出（退出码 0）。
- 超时兜底：TERM → 2s → KILL。
- 退出码：正常 EOF=`0`、启动策略拒绝=`2`、runtime 错误=`1`。

## 6. 测试

```bash
cargo test -p efflab-agent-sidecar --test host_contract   # fixture 用例（17 条）
cargo test -p efflab-agent-sidecar --test acp_stdio       # 端到端链路
```

跨语言 Host 对齐方式：读取 `tests/fixtures/host_contract_cases.json`，逐条断言
自己的校验实现与 `expect` 一致。
