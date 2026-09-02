# Efflab sidecar 发布收口 Follow-up：Host/sidecar 仓记忆

日期：2026-09-02  
仓库：`effilab-agent`  
范围：N5/N6 收口阶段的 Host/sidecar 定向证据与跨仓限制。

## 本次实测证据

以下命令从本仓根执行，均实际退出码 0：

| 测试目标 | 命令 | 结果 |
| --- | --- | --- |
| Host `dispatch_loop` | `cargo test --locked -p efflab-agent-host --test dispatch_loop -- --test-threads=1` | 54 passed |
| sidecar `acp_stdio` | `cargo test --locked -p efflab-agent-sidecar --test acp_stdio -- --test-threads=1` | 45 passed |
| sidecar `mcp_runtime` | `cargo test --locked -p efflab-agent-sidecar --test mcp_runtime -- --test-threads=1` | 62 passed |

这些是 Host/sidecar 定向测试证据，不是 workspace 全量发布认证，也不是正式产品 `.app` 动态 smoke。Host 的 Unix fake sidecar、ACP stdio 和 loopback 证据不能外推到 Windows runner/真机、MSIX 或真实 BYOK。

## N5 限制

- 正式 macOS `.app` 动态 Host/Tauri/ACP 链路的现场既有证据曾被 production provisioning profile 阻断；本仓定向测试不能把该链路写成通过。
- 后续人工验收由用户在具备正式 provisioning profile 的 macOS 环境中执行，产品入口为 `../ai_music_organizer_br` 的 `npm run tauri:build:macos -- --no-install`，验收范围以产品 `docs/superpowers/plans/2026-09-01-efflab-sidecar-release-followup-plan.md` N5 六项为准。
- 浏览器人工验收不可用；产品 Web/native 测试是替代证据，不是浏览器或 WebView 端到端证明。
- Windows runner/真机、MSIX 和真实 BYOK 继续 deferred。x86_64 或 Windows 的真实运行结果不能由本次定向测试外推。

## N6 限制与跨仓决策

- 产品 lint 的非零基线（314 problems、282 errors、32 warnings）由产品仓单独以 `../ai_music_organizer_br/docs/reports/2026-09-02-product-lint-baseline-waiver.md` 记录；它不是本仓测试通过或失败的证据。
- 产品 Rust 默认并行全量唯一失败涉及 LanceDB `music_vectors.lance/_versions`；该用例的串行 exact 1/1 通过不能替代并行全量结果。
- 产品 Rust 串行全量随后以退出码 101 结束：lib test 单元测试 `1530 passed, 0 failed, 2 ignored`，但 `basic_audio_matches_official_golden` 的 `mood_top8` 第 2 名出现 `melodic` 与 `soundscape` 不一致；产品报告保留 CPU-only/CoreML 策略差异与未修改 golden 的决策。
- 本记忆只记录本仓实际测试与边界；本次文档同步不修改 Host/sidecar 源码、Cargo.lock、受保护 probe 或 Git 状态控制项。
- 本条 memory 提交会改变本仓 `HEAD`；产品 N6 文档/矩阵提交和 Web memory 提交也会分别改变对应仓库 `HEAD`。当前 ignored matched tuple 仍绑定 N4 基线的三仓 revision，其中产品 revision 为 `5bf88941df7e9433cd21cc8bb65cd1a6193b94cb`；因此三仓工作区即使 clean，也不自动等于 tuple matched。后续产品 N5 人工验收前，必须在新的 clean 三仓 `HEAD` 上重新走 N3/N4 并重新绑定 tuple/expected-rev。

## 回滚

回滚本条 memory 和产品/Web 对应 follow-up 文档即可；Host/sidecar 的 ACP、MCP、session 和 supervisor 实现不得因 N5 未完成而回退到旧 shell 或 stdio MCP 路径。
