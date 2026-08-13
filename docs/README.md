# effilab-agent 文档

本仓库交付 **Efflab Agent Kit** 的运行时部分：隔离 sidecar + 通用 Host Runtime。UI 在 `effilab-agent-web`，LLM 中转在 `effilab_agent_server`。各桌面产品只写 adapter。

## 先读

| 文档 | 内容 |
|---|---|
| [plans/2026-08-13-effilab-agent-kit-host-architecture.md](plans/2026-08-13-effilab-agent-kit-host-architecture.md) | **现行** Kit / Host 架构（R2）：`HostRuntime`、contract crate、L3b、反向 RPC |
| [plans/2026-08-13-effilab-agent-kit-implementation-plan.md](plans/2026-08-13-effilab-agent-kit-implementation-plan.md) | M0–M1 实现计划（TDD；Task 7b 闭环后门禁 Task 10） |
| [host-acp-contract.md](host-acp-contract.md) | Host ↔ sidecar ACP：prompt / cancel / 标准 `session/list`、反向 `request_permission` |
| [plans/2026-08-11-efflab-agent-sidecar-poc.md](plans/2026-08-11-efflab-agent-sidecar-poc.md) | sidecar 隔离 POC 方案 |
| [plans/2026-08-11-efflab-agent-sidecar-devplan.md](plans/2026-08-11-efflab-agent-sidecar-devplan.md) | sidecar 开发计划 |

## crate

| crate | 状态 | 职责 |
|---|---|---|
| `crates/efflab/efflab-agent-contract` | 设计中 | 无 grok-shell：host_contract / MCP DTO / 权威 config 渲染 |
| `crates/efflab/efflab-agent-sidecar` | 已有 | 隔离 ACP 进程（依赖 contract + grok-shell） |
| `crates/efflab/efflab-agent-host` | 设计中，尚未建 crate | 产品协议、ACP 客户端、supervisor、L3b；**不**依赖 sidecar 库 |

不要在产品仓库复制 Host。上游 `xai-grok-*` 仍视为只读快照。
