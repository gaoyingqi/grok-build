# Task21：qualified tool name 与 Host metadata 收口

日期：2026-09-01

## 结论

- `efflab-agent-contract` 公开导出 `is_qualified_tool_name` 与 `is_prompt_id`；Host catalog、sidecar MCP runtime/catalog/model/call、transcript 和 replay 入口复用 qualified-name gate。
- `GrokBuild:efflab_noop` 是唯一内置工具例外；不得推广为 `GrokBuild:*`。
- v1 journal 与 legacy importer 保留通用 identifier 形式的历史工具名以维持审计只读兼容；该值不因此获得模型、MCP call 或 replay 资格。安全消费层必须再次通过 qualified-name gate；legacy policy 还要求 expected/ready 交集。
- `HostPolicy::with_meta_key_for` 只接受合同固定组合：`session/new`、`session/load` 的 `modelId`，以及 `session/prompt` 的 `promptId`。`initialize`、`session/list`、`session/cancel` 和其它非法组合被忽略，调用方不能借 builder 放宽合同。
- Host catalog 的 `GrokBuild:efflab_noop` 由 `parse_catalog` 本地注入；server 返回同名启用工具时 fail-closed，禁止伪造内置工具 provenance。
- Host `Send` 在 Channel、mention、`SubmissionMap`、actor 和 sidecar 之前直接调用共享 `is_prompt_id`；空值、控制字符和超长 UTF-8 标识在入口拒绝且不产生生命周期副作用。
- POC 与旧 plan 文档顶部标注为历史追溯、不可执行，并指向现行 ACP 合同与 minimal-runtime 设计。

## 验证

- `cargo test -p efflab-agent-contract -p efflab-agent-host`：退出码 0；contract 35 unit + 3 host_contract + 3 mcp_stdio_unavailable + 23 runtime_config，Host 66 unit、17 acp_runtime、49 dispatch_loop、23 loopback、1 Windows hardening、18 projector、19 protocol/submission、16 supervisor、4 three_repo_integration 及 doc tests 均通过。
- `cargo test -p efflab-agent-sidecar`：退出码 0；39 unit、45 ACP stdio、1 host_contract、62 MCP runtime、25 model client、38 session compatibility、29 startup 及 doc tests 均通过。
- `cargo check -p efflab-agent-contract -p efflab-agent-host -p efflab-agent-sidecar`：退出码 0。
- `cargo fmt --package efflab-agent-contract -- --check`、`cargo fmt --package efflab-agent-host -- --check` 与本轮修改的 sidecar 文件 `rustfmt --edition 2024 --check`：退出码 0。
- sidecar package 级 `cargo fmt --package efflab-agent-sidecar -- --check` 仍被工作树中既有 `mcp_client.rs`/`mcp_runtime.rs` 格式差异阻断；本轮修改的 `turn_loop.rs`、`tests/acp_stdio.rs`、`tests/session_compat.rs` 已单独通过格式检查，未为格式化而扩大无关改动。
- `scripts/fork-sync-apply.sh --check`：退出码 0。

## 回滚

回滚本条记忆、POC 文档顶部说明以及 Task21 对应源码/测试改动即可；不得删除 sidecar 测试 probe 或恢复旧 CLI/MCP 启动路径。
