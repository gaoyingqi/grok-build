# Task24 独立复审：Host 边界局部修复

日期：2026-09-01
项目：`effilab-agent`

## 结论

现行 Host 架构明确一 scope 一进程可以同时拥有多个 active session，`current_session` 只是最近一次 `session/new` 或 `session/load` 的指针，不能作为 live update 的 admission 条件。标准 `session/update` 的未知/禁用变体仍由 projector 内部计数并跳过；带 `_meta.isReplay=true` 的消息继续由当前 `session/load` flight/epoch 门禁隔离。active session 的事件缓冲由 `active_sessions` 维护，支持非 current session 的 live update 与 hot resume。

## 本轮修复

- 按复审结论删除 `ScopeActor::handle_notification` 中错误的 `sessionId == current_session` live admission gate；保留 replay flight/epoch 门禁和未知 update 的产品输出白名单。
- `dispatch_loop.rs` 将旧 wrong-session 回归改为建立两个 active session，验证非 current 的 active session 仍能投影 live update 并通过 hot resume 重放。
- `three_repo_integration.rs` 的 transcript 断言同时拒绝未知 live/replay update 的对应 `event_id`、`block_id`、`KitBlock::Unknown` 和旧诊断 Status。
- 两个 Host ACP wire oracle 对 `session/prompt` 强制校验 canonical `sessionId`、非空 text ContentBlock 数组、ContentBlock 字段闭集、`_meta.promptId` 字段闭集，并对未知出站 method fail-closed。
- fake sidecar 的 `hold_prompt` 改为后台 FIFO 等待：主读循环仍可消费 `session/cancel`，result 只在测试显式释放后发送；load deadline 场景使用测试已建立的合法 session blocker，不依赖固定 wall-clock sleep。
- `docs/host-acp-contract.md` 恢复为多 active session 语义，不新增 current-session-only admission 条款。

## 复审纠正

此前版本把“避免未知 event 泄漏”和“禁止非 current active session 的 live update”错误合并；后者已撤销。旧测试因依赖该 gate 而失败后，改成验证非 current active session 的投影与 hot resume。

## 验证

- `cargo test -p efflab-agent-host --test dispatch_loop -- --nocapture`：54 个测试通过（包含新增未激活 session 归属与 transcript 负断言）。
- `cargo test -p efflab-agent-host --test dispatch_loop live_update_for_active_non_current_session_is_projected_and_hot_resumable --quiet -- --exact`：连续 10 次各 1 个测试通过；active/non-current 事件可 live 投影并 hot resume，未激活 session 事件不进入 replay transcript。
- `cargo test -p efflab-agent-host --test three_repo_integration -- --nocapture`：6 个测试通过；`two_turns_cold_resume_cancel_and_unknown_update_have_one_visible_transcript` 使用 `--quiet -- --exact` 连续 10 次通过。
- 本轮其它 Task24 受影响的 dispatch 场景此前连续执行 10 次，均通过。
- `cargo test -p efflab-agent-host -- --nocapture`：Host 全部测试通过（68 unit、18 ACP runtime、54 dispatch loop、23 LLM loopback、1 capability、18 projector、19 protocol/submission、16 supervisor、6 three-repo，doc tests 0）。
- `cargo fmt --package efflab-agent-host -- --check`、`git diff --check`、`scripts/fork-sync-apply.sh --check`：通过。
- `cargo clippy -p efflab-agent-host --all-targets`：通过；仅保留工作树既有 warning，未因本轮修改失败。
- `cargo tree -p efflab-agent-host` 依赖闭包检查通过，未发现 `efflab-agent-sidecar`、`xai-grok-shell` 或 `xai-grok-tools`。

## 边界

FIFO、POSIX shell fake sidecar 和相关 stdio 集成测试仍为 Unix-only；本机为 macOS，未运行真实 Windows fake sidecar。未实现 Web/Tauri E2E，未修改 `crates/efflab/efflab-pr0-http-probe/`，未执行 git add、commit、merge、rebase、push、reset 或 clean。
