# crates/efflab

与上游 `xai-grok-*` 隔离的 Efflab 适配层。

| crate | 职责 |
|---|---|
| `efflab-agent-contract` | 无 grok-shell 的校验 / DTO / 渲染（设计中） |
| `efflab-agent-sidecar` | 隔离 ACP sidecar 进程（已有；依赖 contract + grok-shell） |
| `efflab-agent-host` | 通用 Host Runtime：`HostRuntime::dispatch`、Kit 产品协议、ACP、supervisor、L3b（设计中；**只**依赖 contract，见 `docs/plans/2026-08-13-effilab-agent-kit-host-architecture.md`） |

产品 App 只依赖 host crate + sidecar **二进制**并实现 `HostApp` + `KitEventSink`。不要复制 Host，不要在产品仓拼 ACP，不要 `use` sidecar 库或 `xai_grok_*`。
