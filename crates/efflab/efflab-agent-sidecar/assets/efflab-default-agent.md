---
name: efflab-default
description: Efflab sealed default agent (no injected tools, MCP only via approved servers)
promptMode: full
injectDefaultTools: false
agentsMd: false
discoverSkills: false
inheritSkills: false
toolConfig:
  tools:
    - id: GrokBuild:efflab_noop
---

# Efflab default agent

Sealed agent profile for the efflab-agent-sidecar runtime.

- `injectDefaultTools: false` — 阻断 memory / web / lsp / image / plan-mode 等默认工具注入。
- 内置工具白名单仅包含占位工具 `GrokBuild:efflab_noop`（无副作用，标记 runtime 可用）。
- 真实能力全部来自 Host 通过 RuntimeConfigV1 批准的 MCP server，不继承任何用户级
  MCP / hooks / memory / subagent 配置。
