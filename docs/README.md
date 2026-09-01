# Efflab Agent 文档入口

本仓库交付 **Efflab Agent Kit** 的运行时部分：隔离 sidecar 与通用 Host Runtime。UI 在 `effilab-agent-web`，LLM 中转在 `effilab_agent_server`；各桌面产品只写 adapter。

## 当前入口（优先阅读）

| 文档 | 定位 |
|---|---|
| [host-acp-contract.md](host-acp-contract.md) | **当前 Host ↔ sidecar ACP v1 合同**：方法面、字段白名单、MCP catalog、回放与平台边界 |
| [2026-08-28 minimal-runtime design](../../ai_music_organizer_br/docs/superpowers/specs/2026-08-28-efflab-sidecar-minimal-runtime-design.md) | **当前最小 runtime 设计真源**：sidecar 编译闭包、PR0 门禁、会话恢复与跨仓边界 |
| [../AGENTS.md](../AGENTS.md) | 当前仓库 ownership、依赖方向、验证命令与安全约束 |

当前实现应以以上合同、设计真源及 on-disk 源码为准。文档中的候选依赖、未通过门禁或未验证平台不得视为已锁定事实。

## 历史记录（不可执行）

以下文档保留设计演进、评审结论和当时的实施记录；其中的目标、命令、步骤、验证、清单和提交文本均不可照抄或执行，不是当前待办：

| 文档 | 定位 |
|---|---|
| [2026-08-13 Host architecture](plans/2026-08-13-effilab-agent-kit-host-architecture.md) | 历史架构导读与决策记录；不再作为规范入口 |
| [2026-08-13 implementation plan](plans/2026-08-13-effilab-agent-kit-implementation-plan.md) | 历史 M0–M1 任务记录；不再作为实施计划 |
| [2026-08-11 sidecar POC](plans/2026-08-11-efflab-agent-sidecar-poc.md) | 历史 POC 方案与验证记录 |
| [2026-08-11 sidecar devplan](plans/2026-08-11-efflab-agent-sidecar-devplan.md) | 历史 sidecar 开发记录 |

旧文档若与当前合同、minimal-runtime design 或源码冲突，以当前入口为准；不删除历史内容，也不从旧记录恢复已废弃的 CLI、配置格式或 stdio MCP spawn 路径。

## crate 位置

| crate | 当前职责/源码位置 |
|---|---|
| `crates/efflab/efflab-agent-contract` | Host/sidecar 共用的无 grok runtime 校验、DTO 与 RuntimeConfigV1 合同 |
| `crates/efflab/efflab-agent-sidecar` | 隔离 ACP 进程与最小 sidecar runtime |
| `crates/efflab/efflab-agent-host` | Kit 产品协议、ACP client、supervisor、L3b 与 HostRuntime |

不要在产品仓库复制 Host。`xai-grok-*` 等 workspace 旧 crate 仅按各自源码与当前编译闭包使用，不因历史计划获得新的 sidecar 或产品入口。
