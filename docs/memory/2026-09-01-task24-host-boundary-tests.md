# Task24：Host 边界测试门禁收口

日期：2026-09-01
项目：`effilab-agent`

## 范围

本轮只增强 Host 测试与 ACP 合同边界文档，不改 Host/sidecar 生产逻辑，不复制产品 Web/Tauri 实现。Unix fake sidecar 仍只用于 Host supervisor/ACP stdio 的黑盒观察；Windows 不伪造 Unix shell/FIFO 测试通过。

## 评审意见适用性与处理

- `shell_quote` 意见适用：`dispatch_loop.rs` 与 `three_repo_integration.rs` 的 POSIX 单引号路径必须把 `'` 编码为 `\''` 形式；两文件均加入含 apostrophe 的回归测试。真实 launch loopback 用例也使用含 apostrophe 的目录名。
- fake sidecar argv 意见适用：目标 harness 按固定位置、出现次数和值校验 `--runtime-config <home>/runtime-config.v1.toml --home <private-home> --session-cwd <scope-workspace> --stdio`，拒绝未知参数及 `--grok-home`、`--mcp-config`、`--mcp-exec-root` 旧入口。
- ACP scope 意见适用：`session/new`、`session/list`、`session/load` 的 `cwd` 必须精确等于当前 scope 的 canonical workspace；`_x.ai/mcp/list` 的 `sessionId` 必须精确等于 fake sidecar 当前 active session，不再用后缀匹配。
- 秘密隔离意见部分适用：真实 launch 测试同时检查初代与轮换后的 child 环境名集合、`XAI_API_KEY`/`GROK_CODE_XAI_API_KEY` 缺失、初代与当前 runtime config/log 无用户 Key；sidecar 只把变量名和 generation marker 写入临时文件。
- 并发/teardown 意见适用：cold resume 使用 Barrier 启动三个真实并发 `dispatch`，逐个 `join`；通知洪水场景断言 `session/cancel` 恰到达一次，并等待旧 child `exited` 与下一代 started marker。
- JSONL 竞态意见适用：测试 wire reader 只接受以换行结束且每行 JSON 完整可解析的快照；半行在补齐换行前返回 `None`，加入回归测试。等待使用条件、FIFO、条件变量或 `yield_now`，未新增固定 wall-clock sleep；迟到 load、cold load 占用与 load 中诊断场景均用 FIFO 阻塞读，另留 marker 保存跨 generation 状态。
- `terminal_commit_then_error`、重复 harness 与 teardown 只做局部可信度增强：保留现有稳定 event identity/outbox/repeated-cancel 测试，不引入大抽象。

## 平台边界

- FIFO、POSIX shell 和 Unix stdio fake sidecar 测试文件明确标记 `Unix-only`。
- `pr0_windows_hardening.rs` 在 Windows 分支加入 `SupervisorCapability::Unavailable { SidecarHardeningUnavailable }` 的 fail-closed 断言，并保留 Windows API capability 编译单元。
- 当前 macOS 工作区没有 Windows Rust target，未运行真实 Windows fake sidecar；Unix 测试结果不代表 Windows 运行通过。`docs/host-acp-contract.md` 已写明该边界。

## 验证

- `cargo test -p efflab-agent-host`：退出码 0；68 个 unit、18 个 ACP runtime、52 个 dispatch loop、23 个 LLM loopback、1 个 Unix capability、18 个 projector、19 个 protocol/submission、16 个 supervisor、6 个三仓边界测试通过，合计 221 个测试通过，doc test 为 0。
- 关键测试连续执行：partial-line、dispatch cold resume、`submission_id` 无副作用、terminal retry、three-repo cold resume 各 10 轮，共 50 次，退出码 0；此前的 Host 关键 target 重复验证也均未观察到测试失败。
- `cargo test -p efflab-agent-host --test llm_channel_loopback -- real_launch_and_rotation_keep_user_keys_out_of_sidecar_environment`：退出码 0，1/1 通过。
- `cargo test -p efflab-agent-contract -p efflab-agent-sidecar`：退出码 0；contract 64 个测试、sidecar 239 个测试通过。
- `cargo check -p efflab-agent-contract -p efflab-agent-host -p efflab-agent-sidecar`：退出码 0。
- `cargo clippy -p efflab-agent-contract -p efflab-agent-host -p efflab-agent-sidecar --all-targets`：退出码 0；保留既有 dead-code、disallowed `canonicalize` 及若干风格 warning，未因本轮修改失败。
- `cargo fmt --package efflab-agent-contract --package efflab-agent-host -- --check`：退出码 0；本轮四个 Host 测试文件的 `rustfmt --edition 2024 --check`：退出码 0。
- `cargo fmt --package efflab-agent-sidecar -- --check`：退出码 1，差异仅位于工作树既有 `src/mcp_client.rs` 与 `tests/mcp_runtime.rs` 格式，不为本任务扩大修改范围；sidecar 测试本身仍通过。
- `scripts/fork-sync-apply.sh --check`：退出码 0；Host/sidecar `cargo tree` 检查退出码 0，Host 树未出现 `efflab-agent-sidecar`、`xai-grok-shell` 或 `xai-grok-tools`。

## 本轮复审收口

1. **cold resume 的计数读取必须晚于消费完成。** 并发线程先由 `Barrier` 同步进入 dispatch；释放 FIFO gate 后，测试等待 fake sidecar 写出 `load-completed` marker，再读取最终 wire 快照或 `session/load` 次数。只等待 gate 释放或线程返回，不能证明 sidecar 已消费完请求。
2. **fake sidecar 不得用输入建立协议 oracle。** `session/new/list/load` 的允许字段、精确 `_meta`、固定 canonical session ID、scope cwd 与空 MCP 都由独立常量和 Rust wire 断言校验；`_x.ai/mcp/list` 只接受固定 session ID，并继续拒绝旧参数及未知参数。
3. **partial-line 回归要穿过真实生产管线。** `AcpRuntime` 测试使用可控 child 与真实 pipe：child 先输出半行，等待 Host 发出的 cancel 被消费后再补换行；Host 最终只接收一条完整 notification。同步靠 stderr marker，不靠 wall-clock sleep。
4. **终态重试用尝试记录和 teardown 快照证明。** 测试等待第一次真实投递尝试，随后触发确定性的 runtime shutdown，最后检查 outbox drain 后恰有两次尝试且 `(event_id, sequence)` identity 相同，并验证产品侧只有一个终态。
5. **`submission_id` 非法输入要做生命周期前后快照。** 已建立 actor 与未建立 actor 均记录 child、ACP 方法、lifecycle、terminal 和事件总数；非法 ID 必须零副作用，之后合法 Send 仍可建立/使用回合。

## 回滚

回滚本文件、`docs/host-acp-contract.md` 的平台说明以及本轮 Host 测试改动即可；不需要恢复旧 argv/旧 MCP 入口，也不得删除现有 probe。
