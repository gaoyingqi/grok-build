# tools — 编译脚本

本目录提供 **Mac / Linux** 与 **Windows** 的一键编译脚本，约束与 `AGENTS.md` 一致：

- 根 `Cargo.toml` 为生成物，workspace 成员由 `scripts/fork-sync-apply.sh` 维护，脚本不改 workspace。
- 只编译 Efflab 三件套：`efflab-agent-contract` / `efflab-agent-host` / `efflab-agent-sidecar`（仅 sidecar 为可执行二进制，host/contract 为库）。
- 默认 `--locked`，与 CI 保持一致。

## 前置要求

- **Rust**：由 `rust-toolchain.toml` 钉住版本（当前 `1.97.1`），`rustup` 会自动切换。
- **DotSlash**：`bin/protoc` 通过 DotSlash 拉取 `protoc`，建议 `cargo install dotslash` 并确保 `dotslash` 在 `PATH`。
- **protoc**：若 PATH 上有 `protoc` 则直接使用，否则走 `bin/protoc` 的 DotSlash。

## Mac / Linux

```bash
tools/build.sh                          # 默认 release，本机架构
tools/build.sh --all                    # Mac 上同时产出 Mac + Windows（需 cargo-xwin）
tools/build.sh --all --dist             # Mac + Windows 加固发布双编译
tools/build.sh --dist --check --test    # 加固发布（release-dist）+ 静态检查 + 单测
tools/build.sh --target aarch64-apple-darwin --release
tools/build.sh --universal --dist       # macOS 通用二进制（aarch64 + x86_64 + lipo）
tools/build.sh --target x86_64-pc-windows-msvc --release  # Mac 上单编 Windows（需 cargo-xwin）
tools/build.sh --debug
tools/build.sh --profile x-prod --target x86_64-unknown-linux-gnu
```

选项：`--release` / `--debug` / `--dist` / `--profile <name>` / `--target <triple>` / `--universal` / `--all` / `--check` / `--test` / `--clean` / `--no-locked` / `-h, --help`。

产物：`target/release/efflab-agent-sidecar`、`target/<triple>/release/efflab-agent-sidecar(.exe)`、`target/universal-apple-darwin/release/efflab-agent-sidecar`（universal 时）。

> **Mac 上编译 Windows 版**：本项目含 `aws-lc-sys`/`ring`/`blake3` 等 C 依赖，裸 `cargo --target x86_64-pc-windows-msvc` 会因缺少 `windows.h`/`ml64.exe` 失败，需 `cargo install cargo-xwin`，脚本会自动改用 `cargo xwin build`。`--all` 即 Mac 本机 + `x86_64-pc-windows-msvc` 双编译。

## Windows

在 PowerShell 中执行：

```powershell
tools/build.ps1                                  # 默认 release
tools/build.ps1 -Dist -Check -Test
tools/build.ps1 -Target x86_64-pc-windows-msvc -Release
tools/build.ps1 -Target aarch64-pc-windows-msvc
tools/build.ps1 -Help
```

参数：`-Release` / `-Debug` / `-Dist` / `-Profile <name>` / `-Target <triple>` / `-Check` / `-Test` / `-Clean` / `-NoLocked` / `-Help`。

产物：`target/release/efflab-agent-sidecar.exe`、`target/<triple>/release/efflab-agent-sidecar.exe`。

## 常见等价 Cargo 命令

脚本本质是以下命令的封装（均带 `--locked`）：

```bash
cargo build -p efflab-agent-contract -p efflab-agent-host -p efflab-agent-sidecar --release --locked
cargo build -p efflab-agent-sidecar --release --locked --target aarch64-apple-darwin
cargo check -p efflab-agent-contract -p efflab-agent-host -p efflab-agent-sidecar --locked
cargo clippy -p efflab-agent-contract -p efflab-agent-host -p efflab-agent-sidecar --all-targets --locked
cargo test -p efflab-agent-contract -p efflab-agent-host -p efflab-agent-sidecar --locked
# 加固发布
cargo build -p efflab-agent-sidecar --profile release-dist --locked
```

## 依赖边界

`--check` 会额外校验 `cargo tree -p efflab-agent-host` 中不出现 `efflab-agent-sidecar` / `xai-grok-shell` / `xai-grok-tools`，与 `AGENTS.md` 的 crate 边界一致。

## 故障排查

- `workspace 成员检查失败`：执行 `scripts/fork-sync-apply.sh --check` 查看缺失成员，按提示在 Git Bash/WSL 或 macOS 终端执行 `scripts/fork-sync-apply.sh --apply`（需 `FORK_BASE_REV` 与 `SOURCE_REV` 一致）。
- `未找到 cargo/rustc`：安装 `rustup` 后执行 `rustup show`。
- Windows 上 `scripts/fork-sync-apply.sh --check` 跳过：未找到 `bash/sh`，请在 Git Bash 或 WSL 中手动执行该检查。
