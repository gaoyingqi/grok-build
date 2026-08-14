# Efflab Agent Kit 嵌入清单

本清单用于第二个桌面产品接入 Efflab Agent Kit。它只检查产品与 Kit 的边界，不是第二个产品的发布验收。

## 1. 依赖真实 Kit 产物

- [ ] Rust 产品依赖 `crates/efflab/efflab-agent-host`（crate 名为 `efflab-agent-host`），使用其中的 `HostRuntime`、`HostApp`、`KitEventSink` 与 `HostRuntimeConfig`。
- [ ] 产品随包提供 `efflab-agent-sidecar` **可执行二进制**，并通过 `HostRuntimeConfig.sidecar_bin` 注入路径；不要把 `efflab-agent-sidecar` 当 Rust 库链入产品进程。
- [ ] 前端依赖 `@efflab/agent-web`，使用其 `useAgentKit` 与 `AgentPanel`；产品不复制 web 端协议归约或引导逻辑。

## 2. 启动时组装 Host，并保持单例

- [ ] 产品实现 `HostApp`（领域能力）与 `KitEventSink`（事件运输），再注入完整的 `HostRuntimeConfig`。
- [ ] 在应用启动 / setup 阶段调用 `HostRuntime::new(...)` **一次**，将得到的 runtime 放入应用状态并一直持有到进程退出。
- [ ] 禁止在每次 invoke、每次切栏或每次发送消息时重新调用 `HostRuntime::new`；由 runtime 的生命周期负责 scope actor、会话缓冲与 sidecar 生命周期。

## 3. 只走 dispatch，并挂载标准面板

- [ ] 产品运输层只做受信窗口 / scope 授权、Kit JSON 解码和结果转发；所有 Kit 命令只调用 `HostRuntime::dispatch`。
- [ ] 前端挂载 `<AgentPanel />`，通过 `useAgentKit({ scopeId })` 接入；bootstrap、事件订阅、会话引导和 `KitBlock` 归约由 `@efflab/agent-web` 负责。
- [ ] 产品不得直连 ACP / stdio、手写监听器、手写消息归约器或另造一套 Agent 面板协议。

## 4. 提供已授权 ScopeId，隔离路径

- [ ] 只向 Kit 提供产品已经验证、当前用户已经授权且当前仍打开的 `ScopeId`；`ScopeId` 是不透明标识（例如已授权的 `library.id`），不是用户输入的任意字符串。
- [ ] 不把曲库根路径、项目路径或其他库路径当作 `ScopeId`，也绝不把库根路径传作 sidecar 的 cwd / `session_cwd`。
- [ ] `home_root` 传入应用数据根，由 Host 按 app 与 scope 派生隔离 home / workspace；领域路径如需使用，只能进入经过批准的领域 MCP 参数。

## 5. 只读 MCP，可空且 HTTP 优先

- [ ] `HostApp::mcp_for_scope` 只返回当前 scope 已批准的只读 MCP；没有领域工具时返回空批准集也可以，对话不依赖 MCP 启动。
- [ ] 需要提供 MCP 时优先采用受控 HTTP loopback / HTTP transport；不要在产品中自行拼 ACP 或把写回、任意执行能力塞进 sidecar。

## 6. 产品仓 no-ACP 门禁

产品仓禁止出现以下边界泄漏：

- [ ] 直接组装 `session/prompt` / `session/update`，或等待 ACP prompt 结果作为产品协议。
- [ ] `AcpClient`、`validate_host_request` 或其他产品侧 ACP 客户端 / host-contract 复刻。
- [ ] 产品用 `reqwest` 直接请求 Chat Completions；LLM 出口由 Host Kit 负责。
- [ ] 手写 `KitBlock` → `Message` 归约、bootstrap 或事件投影。
- [ ] `efflab_agent_sidecar::`（sidecar 库链接）或 `xai_grok_`（上游 grok 命名空间泄漏）。

产品仓可运行：

```bash
bash tools/check-no-acp-in-product.sh
```

门禁脚本覆盖 ACP / sidecar / grok 命名空间的静态残留；上面的 `reqwest` 直连和手写归约仍需按本清单做代码审阅。
