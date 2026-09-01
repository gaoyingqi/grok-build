# efflab-agent-sidecar

独立的最小 ACP stdio sidecar。sidecar 只承载产品 Host 注入的版本化 runtime 配置、v1 session journal、有限 turn loop 和受控 loopback L3b 模型调用；不复用旧 shell runtime。

## 定位与边界

- **最小 runtime**：由 `sidecar_config`、`hardening`、`runtime`、`acp_agent`、`session_store`、`model_client` 和 `turn_loop` 组成。
- **启动输入**：Host 通过 `--runtime-config` 指定 `RuntimeConfigV1`，通过 `--home` 指定隔离私有 home，通过 `--session-cwd` 指定隔离会话目录。runtime config 必须位于 `<home>/runtime-config.v1.toml`，不会回退或读取旧 `config.toml`。
- **ACP stdio**：当前只支持 `--stdio`。stdout 仅输出 ACP JSON-RPC，日志固定写 stderr；stdout 由 ACP transport 单写者负责。
- **模型边界**：`model_client` 只连接 runtime config 校验后的 loopback L3b Chat Completions endpoint；取消、请求大小、SSE 行和总响应大小均有界，binding 不进入日志、journal 或 ACP 错误。
- **工具边界**：当前唯一可执行工具是经 permission 后运行的无副作用 `GrokBuild:efflab_noop`。不启动 stdio MCP 子进程，不读取 MCP command/env，也不把未审核工具传给模型。
- **旧 runtime 隔离**：sidecar 不链接 `xai-grok-shell`、`xai-grok-tools` 或旧 shell 的认证、遥测、远程更新闭包；ACP 仅在 Host 与 sidecar 的内部 stdio 边界使用。

## 构建

```bash
cargo build -p efflab-agent-sidecar
cargo build -p efflab-agent-sidecar --release
```

## 运行

```bash
efflab-agent-sidecar \
  --stdio \
  --runtime-config <home>/runtime-config.v1.toml \
  --home <private-home> \
  --session-cwd <session-cwd>
```

参数说明：

- `--runtime-config`：Host 生成的 `RuntimeConfigV1` 文件；必须是 `<home>/runtime-config.v1.toml`。
- `--home`：sidecar 的隔离私有 home；用于 home lock、v1 session journal 和 runtime config。
- `--session-cwd`：Host 创建并校验的隔离会话目录。
- `--stdio`：ACP stdio 传输开关；当前关闭时启动会被拒绝。

退出码：正常 stdin EOF 为 `0`，启动策略拒绝为 `2`，runtime 或 ACP I/O 错误为 `1`。

## 运行流程

启动顺序固定为：CLI 与平台门禁 → L3b binding 门禁 → 环境 allowlist → runtime config、home 和 session cwd 校验 → 私有 home lock → current-thread Tokio runtime 与 `LocalSet` → ACP stdio runtime。

prompt 采用有限流程：先建立 session admission，再原子追加 user record 和发送 user update；模型文本按当前 `promptId` 追加受限 snapshot；工具调用先经过 approved/ready 交集与 Host permission；terminal journal 在 prompt response 之前提交。EOF 和取消路径使用有界 drain，session/load replay update 与 response 共用同一 gateway 顺序出口。

## 模块

| 模块 | 职责 |
|---|---|
| `sidecar_config.rs` | CLI、路径和 `RuntimeConfigV1` 白名单校验 |
| `hardening.rs` | Unix 私有目录、owner-only 权限、home lock、环境 allowlist |
| `runtime.rs` | current-thread Tokio、stdin bridge、ACP connection、EOF drain |
| `acp_agent.rs` | ACP session/prompt admission、cancel latch、permission gateway、replay |
| `session_store.rs` | v1 manifest、records journal 和 legacy 只读导入 |
| `model_client.rs` | loopback L3b Chat Completions、SSE 解析、取消和大小限制 |
| `turn_loop.rs` | prompt snapshot、有限工具回合、permission、terminal 线性化 |
| `main.rs` | 启动顺序和退出码映射 |

## 测试与检查

```bash
cargo test -p efflab-agent-sidecar --test acp_stdio
cargo test -p efflab-agent-sidecar --test session_compat
cargo test -p efflab-agent-sidecar --all-targets
cargo check -p efflab-agent-sidecar
cargo clippy -p efflab-agent-sidecar --all-targets
cargo fmt --all -- --check
```

集成测试通过真实 sidecar ACP stdio 与 loopback fixture 覆盖 initialize、session 生命周期、prompt/update 顺序、取消、EOF drain、permission、工具 transcript 恢复和 stdout 纯净性。

## 已知限制

- 当前只验证 macOS 运行环境；Windows 的 owner-only hardening 和真机 sidecar 流程仍需单独验证，未将 Windows 构建结果假定为已验证。
- 当前不提供 stdio MCP spawn、外部 MCP transport、旧 shell command、远程补全、认证交互或 Responses 双协议。
- L3b endpoint 必须由 Host 在版本化 runtime config 中注入并通过 loopback 校验；sidecar 不自行发现 endpoint 或读取旧 shell 配置。
