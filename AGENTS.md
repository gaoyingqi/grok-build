# effilab-agent / AGENTS

## Ownership 与仓库角色
- 本仓是团队自有的 Agent Kit 运行时仓库，属于与 `../ai_music_organizer_br`、`../effilab-agent-web` 同一项目体系；不是“不可修改的外部 fork”。跨仓任务可以直接修改本仓，但必须遵守共享契约、记录影响面并分别验证。
- 本仓负责 `efflab-agent-host`、`efflab-agent-sidecar`、Kit L1 协议、ACP stdio、supervisor、L3b 和 Host/sidecar 安全边界。
- `../ai_music_organizer_br` 负责 Tauri adapter、产品 `HostApp` 端口、权限门禁、运行时/资源路径、设置和面板挂载。
- `../effilab-agent-web` 负责 `@efflab/agent-web` 的 Agent Kit UI、事件 reducer、`useAgentKit`、TypeScript 对照类型和通用样式。

## 目录与事实来源
| 位置 | 职责 |
| --- | --- |
| `crates/efflab/efflab-agent-contract/` | Host/sidecar 共用的无 grok runtime 校验、DTO 与配置渲染 |
| `crates/efflab/efflab-agent-host/` | `HostRuntime::dispatch`、Kit 协议、ACP client、projector、supervisor、L3b |
| `crates/efflab/efflab-agent-host/src/protocol.rs` | Kit JSON 命令、回复、事件和错误的机器真源 |
| `crates/efflab/efflab-agent-sidecar/` | 独立 ACP stdio sidecar。最小 sidecar 禁止依赖 `xai-grok-shell` 及其认证/遥测/远程/更新闭包；workspace 其他 binary（如 Pager）可继续保留旧 crate |
| `docs/host-acp-contract.md` | Host ↔ sidecar ACP 方法面与字段约束 |
| `crates/efflab/efflab-agent-sidecar/tests/fixtures/host_contract_cases.json` | ACP Host 契约正反例 fixture |
| `scripts/fork-sync-apply.sh` | 同步后检查/重放 Efflab workspace member 的入口 |

- `crates/codegen/`、`crates/common/`、`prod/` 与 `third_party/` 含同步或 vendored 源码。该来源属性不改变本仓 ownership，但修改这些区域前必须记录 `SOURCE_REV` / 同步重放影响并优先保持 Efflab 变更收敛在 `crates/efflab/`。
- 根 `Cargo.toml` 是生成物。不要手工维护 workspace member；使用 `scripts/fork-sync-apply.sh` 约定的流程，并运行 `--check`。

## Crate 边界
- `efflab-agent-host` 可以依赖 `efflab-agent-contract`，不得依赖 `efflab-agent-sidecar` 库、`xai-grok-shell` 或 `xai-grok-tools`。产品只链接 Host crate，并把 sidecar 当独立 release 二进制运输。
- `efflab-agent-sidecar` 最小 runtime 禁止 `xai-grok-shell` 及其认证/遥测/远程/更新闭包；stdout 只输出 ACP JSON-RPC，日志只写 stderr。Host 是 sidecar stdin 的唯一写入者。workspace 其他 binary 可保留旧 crate。本规则在 sidecar runtime 实现（PR0/Task 12）之前生效，不得回退为“sidecar 是唯一允许依赖 `xai-grok-shell` 的 crate”。
- ACP 只存在于 Host ↔ sidecar 内部。产品 adapter 与 Web 包不得拼 `session/prompt`、直接读写 stdio 或复制 ACP client。
- L3b、用户凭据代理、binding token、sidecar 生命周期与会话投影属于 Host；用户 Key/token 不写 sidecar TOML、不进 sidecar 环境、不进日志。
- `HostRuntime` 在产品进程内按应用生命周期创建一次；产品命令只调用 `dispatch`，不得每次 invoke 重建 runtime。

## 三仓联动规则
- 三仓均可直接修改；“产品负责 adapter”“Host 负责 ACP”“Web 负责 reducer”是事实来源边界，不是禁止修改 sibling 的规则。不得因问题跨仓就复制实现、绕过契约或留下不一致版本。
- 修改 Kit 命令、回复、事件、错误码、capability、会话状态、replay/stream 合并语义时，必须同步检查：
  1. 本仓 `protocol.rs`、golden fixture 与 Host tests；
  2. `../effilab-agent-web/app/src/hostTypes.ts`、`reduceKitEvents.ts`、`useAgentKit.ts` 与 UI tests；
  3. `../ai_music_organizer_br` 的 Tauri adapter、前端 wrapper、产品面板/设置、命令注册与集成测试。
- 修改 ACP 方法或字段时，先同步 `docs/host-acp-contract.md`、fixture、contract/Host/sidecar 测试；再检查是否会改变 Kit 投影。ACP 内部变化即使不改 Web wire，也必须记录 Web 与产品为何无需修改。
- TypeScript 对照类型不是第二个协议真源；它必须与 `protocol.rs` 的 golden 对拍。产品 adapter 只运输 Kit JSON，不定义第三套命令或事件 schema。
- 每个跨仓变更记录必须列出：涉及仓库、契约字段或状态、生产/打包调用方、依赖闭包、迁移/兼容要求、各仓验证结果与回滚顺序。

## 路径与依赖规则
- 仓库依赖、脚本参数和文档指针使用相对路径：产品为 `../ai_music_organizer_br`，Web 为 `../effilab-agent-web`。禁止提交 `/Users/...`、`/Volumes/...` 等机器绝对路径。
- `home_root`、`sidecar_bin`、`sidecar_log_path`、`mcp_exec_root` 等运行时路径由产品或平台 API 注入；Host 不内置任何产品目录。
- 依赖、workspace member、feature、sidecar 运输或构建脚本变化时，必须同时检查依赖树、测试和构建闭包；不得只凭单个 crate 编译通过就结束。
- 路径、进程、信号、权限和打包规则必须评估 macOS 与 Windows。若某平台按现行契约 fail-closed/unavailable，必须验证该行为并在报告中标明未覆盖的真机门禁。

## 最小验证
```bash
# 生成物与依赖边界
scripts/fork-sync-apply.sh --check
cargo tree -p efflab-agent-host
cargo tree -p efflab-agent-sidecar
# Host tree 中不得出现 efflab-agent-sidecar、xai-grok-shell 或 xai-grok-tools。

# 测试与静态检查
cargo test -p efflab-agent-contract -p efflab-agent-host -p efflab-agent-sidecar
cargo check -p efflab-agent-contract -p efflab-agent-host -p efflab-agent-sidecar
cargo clippy -p efflab-agent-contract -p efflab-agent-host -p efflab-agent-sidecar --all-targets

# sidecar 运输或打包闭包变化时
cargo build -p efflab-agent-sidecar --release --locked

# 跨仓 Kit 协议/状态变化时（均从本仓根执行）
(cd ../effilab-agent-web/app && npm test && npm run lint && npm run build)
(cd ../ai_music_organizer_br && npm test && npm run build)
```

依赖闭包变化时还要在 Web 运行 `cd ../effilab-agent-web/app && npm ls react react-dom`，在产品仓运行 `npm ls @efflab/agent-web react react-dom`、`cd rust && cargo tree -p rust_lib_ai_music_organizer` 和 `cd src-tauri && cargo tree -p app`。无法执行的平台构建、签名或打包验证必须明确报告，不得写成“已通过”。
