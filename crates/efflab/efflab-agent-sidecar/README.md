# efflab-agent-sidecar

复用 [`xai-grok-shell`](../../codegen/xai-grok-shell/) 完整运行时行为的薄 ACP stdio
sidecar。里程碑：**macOS isolated runtime integration POC**（不等于完整生产迁移）。

## 定位

- **薄 sidecar**：不修改任何 `xai-grok-*` 核心 crate；通过私有 GROK_HOME +
  受控 config + env 卫生，把 xai-grok-shell 的完整运行时（会话、ACP、MCP、模型
  编排）装进一个隔离、fail-closed 的进程边界。
- **屏蔽 x.ai 登录依赖**：`[features] remote_fetch=false` 落盘到私有 home（启动后
  硬断言 `resolve_remote_fetch_enabled()==false`）；config 用 disk-only 加载；
  BYOK 走 `XAI_API_KEY`。
- **屏蔽内置工具**：默认 AgentDefinition `injectDefaultTools: false`，唯一内置工具
  为占位 `GrokBuild:efflab_noop`；真实能力全部来自 Host 批准注入的 MCP server。
- **MCP 可用**：`--mcp-config` 提供唯一受控入口（stdio command 限定在
  `--mcp-exec-root` 内、HTTP 仅 loopback、阶段 0 拒绝 env）。

## 构建

```bash
cargo build -p efflab-agent-sidecar
# 或
cargo build -p efflab-agent-sidecar --release
```

## 运行

```bash
efflab-agent-sidecar \
  --grok-home /abs/private/grok-home \
  --session-cwd /abs/isolated/workspace \
  [--mcp-config /abs/mcp.toml --mcp-exec-root /abs/exec-root]
```

- `--grok-home`（或 `EFFLAB_GROK_HOME`）：私有 GROK_HOME 绝对路径，拒绝 `~/.grok`
  与 session workspace 内路径。
- `--session-cwd`：进程工作目录（canonical 绝对路径，必须存在）。
- `--mcp-config`：MCP TOML，仅允许 `[mcp_servers.<name>]`。
- `--mcp-exec-root`：stdio MCP command 的允许根目录。

退出码：正常 EOF=`0`、启动策略拒绝=`2`、runtime 错误=`1`。

## 架构

| 模块 | 职责 |
|---|---|
| `sidecar_config.rs` | CLI / SidecarConfig / ApprovedMcpConfig 解析与白名单校验 |
| `hardening.rs` | 私有 home、fs2 独占锁、原子写（0600）、权威 config 渲染、env 卫生 |
| `toolset.rs` | 占位工具 `GrokBuild:efflab_noop` 与注册（`register_efflab_tool_pack`） |
| `host_contract.rs` | Host 请求字段白名单校验（`validate_host_request`） |
| `main.rs` | 启动序列（CLI→锁→物化→env→cwd→tracing→runtime）与 `assert_hardened` |

启动序列顺序（不可颠倒）：CLI 校验 → 私有 home + 锁 → 物化 AgentDefinition +
权威 config 原子落盘 → `sanitize_env` → 设 `GROK_HOME`/`GROK_AGENT` →
`set_current_dir` → tracing → Tokio runtime → `run_stdio_agent`。

## 测试

```bash
cargo test -p efflab-agent-sidecar                 # lib 单测 + host_contract + acp_stdio 集成
cargo clippy -p efflab-agent-sidecar --all-targets -- -D warnings
cargo check -p efflab-agent-sidecar
```

集成测试覆盖：initialize / session/new / `_x.ai/mcp/list`（wire 需 `_` 前缀）/
stdin EOF 生命周期 / mock MCP 注入 / 恶意 env / stdout 纯净 / 无出网。

## 上游同步

根 `Cargo.toml` 是上游生成物。每次同步官方代码后执行：

```bash
scripts/fork-sync-apply.sh --apply   # 幂等重注册 member；SOURCE_REV 变化时提示
```

## 已知限制（完整阶段 0 / 生产 Go 仍需）

Windows；Chat Completions+Responses 双协议；WebSocket/reconnect/cancel/timeout；
mock 模型完整 prompt 链路（真实模型调用需有效 BYOK key）；macOS 抓包级网络审计
（需特权）；体积/冷启动/RSS 预算；LICENSE/NOTICE 分发验收。
