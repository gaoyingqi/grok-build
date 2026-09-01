# Efflab sidecar 最小运行时：Host/sidecar 仓收口记忆

日期：2026-09-01
范围：`efflab-agent-contract`、`efflab-agent-host`、`efflab-agent-sidecar`、Task17–24 的 ACP/HTTP MCP/恢复一致性和发布闭包门禁。

## 结论

- sidecar 已裁剪为最小 runtime：ACP stdio gateway、受限 turn loop、自有 session v1、Host L3b loopback Chat Completions、审批后的 literal loopback HTTP MCP；没有 MCP 子进程、OAuth、远程更新、遥测或完整 shell/tool runtime。
- Host 是 ACP stdin 唯一写者、BYOK 唯一解封者和 MCP 审批者；Web 只消费 Kit JSON 并负责订阅 ready、resume candidate buffer 与 reducer。
- 一 scope 进程可以同时维护多个 active session。`session/update` 的 `sessionId` 只决定事件归属；`current_session` 只是最近一次 `session/new` / `session/load` 指针，不是 live update admission gate。

## Task17–21 实现边界

- Task17：turn loop 将 prompt 绑定到 `prompt_id`，同一 prompt 才追加 assistant snapshot；未知 update 只增加内部计数，不产生产品事件、不清快照、不消耗 sequence。
- Task18：MCP 仅允许经过 Host 审批的字面量 loopback HTTP；stdio 在 contract、Host 和 sidecar load/runtime 边界均返回 `stdio_mcp_unavailable`。HTTP response、SSE、chunked、Content-Length、EOF/truncation 和取消均 fail-closed。
- Task19：Supervisor 使用 v1 runtime config、固定 argv、generation/revision 和 initialize handshake；cold load 采用 `LoadFlight` 单飞，迟到响应按 identity/epoch 丢弃，终态投递保持幂等。
- Task20：workspace parser、normal/build 闭包、reverse dependency 和真实 release binary strings certification 均有版本化门禁；probe 目录保留但不再作为 workspace member。
- Task21：qualified tool name、prompt id、唯一 Host-owned noop provenance 和 ACP method/status fixture 贯穿 contract、Host、sidecar 与 Web 对照层。
- Task24：`three_repo_integration.rs` 通过真实 Host supervisor、临时 Unix fake sidecar 和 ACP wire 验证两回合 transcript、cold/hot resume、cancel、unknown update、MCP 审批和秘密隔离；不把 ACP 或 Web 实现复制到测试 harness。

## Task20 closure gate 锁等待修复

- 根因：`_MISSING_PACKAGE_RE` 原本要求 stderr 在去掉末尾换行后完全是一条 missing package 诊断；Cargo 在并发访问 package cache 时可能先输出 `Blocking waiting for file lock on package cache`，导致正常的“固定 denylist 候选不存在”被误判为 fatal。
- 最小修复：`scripts/check_sidecar_closure.py` 只允许开头出现零条或多条完全匹配的固定锁等待行，然后仍要求恰剩一条、候选名完全一致的 missing 诊断；stdout 有内容、重复诊断、其它错误或候选名不一致继续拒绝。
- 新增回归：`scripts/test_check_sidecar_closure.py` 的 `test_reverse_dependency_ignores_cargo_lock_wait_before_missing_candidate`；修复前错误退出，修复后通过。
- 这不是泛化 `search()`：识别器仍保持精确、受限和 fail-closed。Cargo 版本变化产生的未知状态行仍会被拒绝，便于人工调查。

## 最新验证证据（2026-09-01）

- `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p 'test_*.py'`：110 tests passed，退出码 0。
- closure 专项详细测试：69 tests passed，退出码 0；新增锁等待回归先以预期错误退出，再以 `OK` 通过。
- `scripts/fork-sync-apply.sh --check`、`git diff --check`、三个核心 package 的 `cargo fmt --package ... -- --check`：均退出码 0。
- `cargo test --locked -p efflab-agent-contract -p efflab-agent-sidecar -p efflab-agent-host -- --test-threads=1`：退出码 0；contract、sidecar、Host 全部测试 target 无失败。Task24 定向结果为 `dispatch_loop` 54 passed、`three_repo_integration` 6 passed。
- `cargo check --locked -p efflab-agent-contract -p efflab-agent-sidecar -p efflab-agent-host`：退出码 0。
- `cargo clippy --locked -p efflab-agent-contract -p efflab-agent-sidecar -p efflab-agent-host --all-targets`：退出码 0；仅有既有 dead-code、`canonicalize` policy 和风格类 warning，未用 `-D warnings` 把既有 warning 伪装成新失败。
- `cargo build --locked -p efflab-agent-sidecar --target aarch64-apple-darwin --release` 后执行 `check_sidecar_closure.py --mode release-certification`：退出码 0，报告 `binary_scanned=true`、`edge_kind=normal,build`、`denylist_hits=[]`。
- Web：6 个测试文件、108 tests passed；`npm run lint` 0 warnings/0 errors；`npm run build` 退出码 0。

## 计数和旧记录纠正

- 当前 `three_repo_integration.rs` 有 6 个测试；此前同名 memory 中的 4/4 是早期快照，不能作为当前覆盖数。
- 当前 `dispatch_loop` 定向测试为 54 个；不同复审轮次的 52/54 计数属于不同工作树快照，最终应以本轮全量命令为准。
- 旧记忆曾记录 `mcp_runtime.rs` 相邻重复 `#[tokio::test]` 导致重编译失败；本轮核心 Rust 全量命令已退出码 0，当前文件不再以该旧故障作为现状。该旧故障的具体修复提交未单独记录，不能虚构 commit。

## 未验证项与平台边界

- `x86_64-apple-darwin` 和 `x86_64-pc-windows-msvc` 的真实 release binary build/strings certification 未执行；当前 arm64 结果不能外推。
- 没有 Windows runner/真机，`pr0_windows_hardening` 只保留非 Windows unproven 记录；Windows capability 不得改为 Available。
- 没有 matched Host + sidecar + Web/lock/hash tuple；产品仓仍保持 S0 expected-rev，不能声称 S4。
- Host 的 Unix fake sidecar 测试不等于真实 Windows sidecar、真实 Web/Tauri 联动或发布包 smoke；真实产品 Tauri/MSIX bundle 仍未验证。

## 保护项与回滚

- 不删除、不修改、不移动 `crates/efflab/efflab-pr0-http-probe/`；`scripts/fork-sync-apply.sh --check` 继续验证它不在生成的 workspace member 列表中。
- ACP/sidecar 回滚必须与对应 Host contract 一起回滚；MCP 出现异常时退回空 `ApprovedMcpSpecV1` 并重启 scope；Windows 始终保持 unavailable。
