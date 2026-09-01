# Rust toolchain 1.94.0 → 1.97.1 与 x86_64 macOS 认证

日期：2026-09-01
范围：`rust-toolchain.toml`、Rust workspace 验证、`efflab-agent-sidecar` 的 x86_64 macOS release 闭包认证。

## 根因

- 升级前 `rust-toolchain.toml` 固定为 `channel = "1.94.0"`，声明的 targets 只有 `x86_64-unknown-linux-gnu` 和 `aarch64-unknown-linux-gnu`；文件注释说明 host target 会自动加入。
- `rustup run 1.94.0-aarch64-apple-darwin rustc -Vv` 的 host 是 `aarch64-apple-darwin`。该精确 toolchain 的已安装 target 为 `aarch64-apple-darwin`、`aarch64-unknown-linux-gnu`、`x86_64-unknown-linux-gnu`，不包含 `x86_64-apple-darwin`。
- 因此 1.94 基线缺少 x86_64 macOS target 的原因是 target 未在 pinned 配置中声明、也未为该精确 toolchain 安装，不是 Windows 或编译器结果被伪造。

## 本次变更

- 仅将 `rust-toolchain.toml` 的 `channel` 从 `1.94.0` 改为固定版本 `1.97.1`；`components`、`profile` 和已有 Linux targets 保持不变。
- 安装 `1.97.1-aarch64-apple-darwin`，并为该精确 toolchain 安装 `x86_64-apple-darwin` target。
- 未安装 Windows target；未修改产品仓库、产品 expected-rev 或 matched tuple。
- `Cargo.lock` 在任务开始前已经是工作区修改状态；任务开始与结束哈希均为 `1c8df239cacc40ac975441dd6c34c78722baa63ae9cc76b303f08f1f1b37418d`，本次没有修改它。
- `crates/efflab/efflab-pr0-http-probe/` 未修改、删除或移动。

## 验证结果

以下命令均从本仓库根执行，除首次全 workspace check 达到单次 300 秒工具上限外，最终命令均取得真实退出码：

- `rustup run 1.97.1-aarch64-apple-darwin rustc -Vv`：退出码 0；`rustc 1.97.1 (8bab26f4f 2026-07-14)`，host 为 `aarch64-apple-darwin`，LLVM `22.1.6`。
- `cargo check --locked --all-targets --workspace`：最终退出码 0；`Finished dev profile`。
- `cargo clippy --locked --all-targets --workspace`：退出码 0；存在既有 warning（包括 `io_other_error`、`question_mark`、`unnecessary_sort_by` 等），没有将 warning 当作失败。
- `scripts/fork-sync-apply.sh --check`：退出码 0；报告 `SOURCE_REV: a51a1dc62fe20029ac39a665985bba78edbb870f`，三个 Efflab member 均已存在。
- `CARGO_BUILD_JOBS=1 cargo build -p efflab-agent-sidecar --target x86_64-apple-darwin --release --locked`：退出码 0；完成 x86_64 release 构建。
- `python3 scripts/check_sidecar_closure.py --package efflab-agent-sidecar --target x86_64-apple-darwin --profile release --mode release-certification --edges normal,build --binary target/x86_64-apple-darwin/release/efflab-agent-sidecar --out "$TMPDIR/efflab-agent-sidecar-release-certification.json"`：退出码 0；`binary_scanned: true`、`denylist_hits: []`、`edge_kind: normal,build`、`binary_scan_status: scanned`。
- `cargo test --locked -p efflab-agent-contract -p efflab-agent-host -p efflab-agent-sidecar -- --test-threads=1`：退出码 0；contract、Host、sidecar 测试及 doc-tests 均通过。
- `git diff --check`：退出码 0。

## 未验证与边界

- 没有 Windows runner 或真机；Windows target、Windows release binary、Windows closure certification 和 Windows capability 仍未验证，不能由本次 macOS 结果外推。
- x86_64 macOS binary 是在当前 macOS arm64 主机上的交叉构建；本次 closure certification 扫描真实 release binary，但没有 x86_64 真机执行 smoke，也不等于产品 Tauri bundle 或 matched Host + sidecar + Web tuple。
- release certification 报告写入 `$TMPDIR`，没有把生成报告加入仓库。
