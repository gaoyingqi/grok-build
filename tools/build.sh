#!/usr/bin/env bash
# tools/build.sh — macOS / Linux 编译脚本（Efflab Agent Kit）
# 职责：校验工具链 → 校验 workspace 成员 → 编译 efflab 三件套 → 可选验证
# 产物：target/{debug,release}/efflab-agent-sidecar（仅 sidecar 为可执行二进制，host/contract 为库）
# 约束：根 Cargo.toml 为生成物，成员注册由 scripts/fork-sync-apply.sh 维护；本脚本不改 workspace

set -euo pipefail

# ---------- 配置默认值 ----------
PROFILE="release"          # debug | release | release-dist | x-prod | release-dist-jemalloc
TARGET=""                  # 例如 aarch64-apple-darwin，留空则为本机默认
UNIVERSAL=0                # 1=同时编 aarch64 + x86_64 并尝试 lipo 合并（仅 Darwin）
ALL=0                      # 1=在 Mac 上同时编译 Mac + Windows
DO_CHECK=0                 # 1=额外执行 cargo check + clippy
DO_TEST=0                  # 1=执行 cargo test
DO_CLEAN=0                 # 1=先 cargo clean
LOCKED="--locked"          # 固定使用 --locked，保证 Cargo.lock 一致

# ---------- 辅助函数 ----------
# 打印信息日志
info() { printf '[tools/build] %s\n' "$*"; }
# 打印错误日志
die()  { printf '[tools/build] ERROR: %s\n' "$*" >&2; exit 2; }
# 打印用法
usage() {
  cat <<'EOF'
用法: tools/build.sh [选项]

选项:
  --release              以 --release 编译（默认）
  --debug                以 debug 编译（不加 --release）
  --dist                 以 --profile release-dist 编译（加固发布版，含 LTOebug 符号）
  --profile <name>       指定任意 Cargo profile（release / release-dist / x-prod 等）
  --target <triple>      指定 --target（例如 aarch64-apple-darwin）
  --universal            macOS 通用二进制：分别编译 aarch64/x86_64 并 lipo 合并（仅 Darwin）
  --all                  在 Mac 上同时编译 Mac + Windows（需 cargo-xwin，见说明）
  --check                编译后执行 cargo check + clippy（efflab 三件套）
  --test                 编译后执行 cargo test（efflab 三件套）
  --clean                编译前执行 cargo clean
  --no-locked            不传 --locked（不推荐，CI 必须 --locked）
  -h, --help             显示本帮助

示例:
  tools/build.sh                          # 默认 release，本机架构
  tools/build.sh --all                    # Mac 上同时产出 Mac + Windows（需 cargo-xwin）
  tools/build.sh --all --dist             # Mac + Windows 加固发布双编译
  tools/build.sh --dist --check --test    # 加固发布 + 静态检查 + 单测
  tools/build.sh --target aarch64-apple-darwin --release
  tools/build.sh --universal --dist       # macOS 产出通用二进制
  tools/build.sh --target x86_64-pc-windows-msvc --release  # Mac 上单编 Windows 版（需 cargo-xwin）

说明:
  本项目依赖 aws-lc-sys/ring 等含 C 编译的 crate，在 Mac 上裸 cargo --target
  x86_64-pc-windows-msvc 会因缺少 Windows SDK（windows.h / ml64.exe）而失败。
  因此 --all / Windows 交叉编译需安装 cargo-xwin（cargo install cargo-xwin），
  本脚本会自动改用 cargo xwin build。若未安装，会给出安装指引后退出。
  若需指定 Mac 架构，请用 --target；--all 与 --target/--universal 互斥。
EOF
}

# 解析参数
while [[ $# -gt 0 ]]; do
  case "$1" in
    --release) PROFILE="release"; shift ;;
    --debug) PROFILE="debug"; shift ;;
    --dist) PROFILE="release-dist"; shift ;;
    --profile) PROFILE="${2:?--profile 需要参数}"; shift 2 ;;
    --target) TARGET="${2:?--target 需要参数}"; shift 2 ;;
    --universal) UNIVERSAL=1; shift ;;
    --all) ALL=1; shift ;;
    --check) DO_CHECK=1; shift ;;
    --test) DO_TEST=1; shift ;;
    --clean) DO_CLEAN=1; shift ;;
    --no-locked) LOCKED=""; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "未知参数: $1（用 --help 查看）" ;;
  esac
done

# --all 与 --target/--universal 互斥校验
if [[ "${ALL}" -eq 1 && -n "${TARGET}" ]]; then
  die "--all 与 --target 互斥；--all 已包含 Windows 目标，如需单平台请直接用 --target"
fi
if [[ "${ALL}" -eq 1 && "${UNIVERSAL}" -eq 1 ]]; then
  die "--all 与 --universal 互斥；如需 Mac 通用 + Windows，请先用 --universal 单独编译 Mac，再用 --target x86_64-pc-windows-msvc 编译 Windows"
fi

# 检测是否为 Windows 目标（需 xwin）
is_windows_target() {
  case "$1" in
    *windows-msvc*|*windows-gnu*) return 0 ;;
    *) return 1 ;;
  esac
}

# ---------- 定位仓库根 ----------
# 脚本位于 tools/，仓库根为上一级
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT}"

# 校验根 manifest 存在
if [[ ! -f "${ROOT}/Cargo.toml" ]]; then
  die "未找到 Cargo.toml: ${ROOT}/Cargo.toml"
fi

# ---------- 工具链检查 ----------
# 检查 rustc 是否可用，并提示 rust-toolchain.toml 钉住的版本
check_toolchain() {
  if ! command -v cargo >/dev/null 2>&1; then
    die "未找到 cargo；请先安装 rustup（https://rustup.rs）并执行 rustup show"
  fi
  if ! command -v rustc >/dev/null 2>&1; then
    die "未找到 rustc"
  fi
  # 读取 rust-toolchain.toml 期望版本，仅作提示，不强行阻断
  local expected
  expected="$(awk -F'"' '/channel *=/ {print $2; exit}' rust-toolchain.toml 2>/dev/null || true)"
  local actual
  actual="$(rustc --version 2>/dev/null || true)"
  info "rustc: ${actual:-unknown}（期望 channel=${expected:-unknown}，见 rust-toolchain.toml）"
  if [[ -n "${expected}" && "${actual}" != *"${expected}"* ]]; then
    info "提示: 当前 rustc 与 rust-toolchain.toml 不一致，cargo 会按 toolchain 文件自动切换"
  fi
  # 检查 DotSlash（bin/protoc 需要），缺失仅警告
  if ! command -v dotslash >/dev/null 2>&1; then
    info "提示: 未找到 dotslash，bin/protoc 将尝试回退到 PATH 上的 protoc；建议 cargo install dotslash"
    info "      安装: cargo install dotslash && dotslash --help"
  else
    info "dotslash: $(dotslash --help 2>&1 | head -n 1 || true)"
  fi
  # 检查 protoc 回退
  if command -v protoc >/dev/null 2>&1; then
    info "protoc: $(protoc --version 2>&1 | head -n 1 || true)"
  else
    info "提示: PATH 上未找到 protoc，将由 bin/protoc 的 DotSlash 按需拉取"
  fi
  # Windows 交叉编译提示
  if [[ "${ALL}" -eq 1 ]] || is_windows_target "${TARGET}"; then
    if ! cargo xwin --version >/dev/null 2>&1; then
      info "提示: Windows 交叉编译需 cargo-xwin（本项目含 aws-lc-sys/ring 等 C 依赖，裸 cargo 无法交叉）"
      info "      安装: cargo install cargo-xwin"
    else
      info "cargo-xwin: $(cargo xwin --version 2>&1 | head -n 1)"
    fi
  fi
}

# ---------- workspace 成员检查 ----------
# 根 Cargo.toml 为生成物，成员由 scripts/fork-sync-apply.sh 维护
check_workspace() {
  if [[ -x "${ROOT}/scripts/fork-sync-apply.sh" ]]; then
    info "检查 workspace 成员（scripts/fork-sync-apply.sh --check）"
    if ! "${ROOT}/scripts/fork-sync-apply.sh" --check; then
      die "workspace 成员检查失败；请按提示执行 scripts/fork-sync-apply.sh --apply 或修复 Cargo.toml"
    fi
  else
    info "跳过 workspace 成员检查（未找到 scripts/fork-sync-apply.sh）"
  fi
}

# ---------- 构建 ----------
# 执行单次 cargo build，参数由调用方传入；Windows 目标自动改用 cargo xwin
cargo_build_once() {
  local profile="$1"
  local target="$2"
  local use_xwin=0
  if is_windows_target "${target}"; then
    use_xwin=1
    if ! cargo xwin --version >/dev/null 2>&1; then
      die "Windows 目标 ${target} 需 cargo-xwin，请先执行: cargo install cargo-xwin（本项目 C 依赖无法用裸 cargo 交叉编译）"
    fi
  fi
  local cargo_cmd="cargo"
  if [[ "${use_xwin}" -eq 1 ]]; then
    cargo_cmd="cargo xwin"
  fi
  local args=(build -p efflab-agent-contract -p efflab-agent-host -p efflab-agent-sidecar)
  # profile 映射为 cargo 参数
  if [[ "${profile}" == "release" ]]; then
    args+=(--release)
  elif [[ "${profile}" == "debug" ]]; then
    : # debug 不加额外 flag
  else
    args+=(--profile "${profile}")
  fi
  if [[ -n "${LOCKED}" ]]; then
    args+=(${LOCKED})
  fi
  if [[ -n "${target}" ]]; then
    args+=(--target "${target}")
  fi
  info "执行: ${cargo_cmd} ${args[*]}"
  if [[ "${use_xwin}" -eq 1 ]]; then
    cargo xwin "${args[@]}"
  else
    cargo "${args[@]}"
  fi
}

# 确保 target 已安装
ensure_target() {
  local t="$1"
  if ! rustup target list --installed 2>/dev/null | grep -q "${t}"; then
    info "安装 target: ${t}"
    rustup target add "${t}" || info "警告: rustup target add ${t} 失败，尝试直接编译"
  fi
}

# 清理
if [[ "${DO_CLEAN}" -eq 1 ]]; then
  info "执行: cargo clean"
  cargo clean
fi

check_toolchain
check_workspace

# ---------- --all 双平台分支 ----------
if [[ "${ALL}" -eq 1 ]]; then
  info "模式: --all（Mac 本机 + Windows x86_64-pc-windows-msvc） profile=${PROFILE}"
  WIN_TARGET="x86_64-pc-windows-msvc"
  ensure_target "${WIN_TARGET}"
  # 1) Mac 本机（纯 Rust + 本机 C 工具链，无需 xwin）
  info "步骤 1/2: 编译 Mac 本机版本"
  cargo_build_once "${PROFILE}" ""
  # 2) Windows（需 cargo-xwin，会在 cargo_build_once 中校验）
  info "步骤 2/2: 编译 Windows 版本（${WIN_TARGET}，via cargo xwin）"
  cargo_build_once "${PROFILE}" "${WIN_TARGET}"
  info "双平台编译完成"
  # 产物预览
  info "产物预览:"
  if [[ "${PROFILE}" == "release" ]]; then
    ls -lh target/release/efflab-agent-sidecar 2>/dev/null || true
    ls -lh "target/${WIN_TARGET}/release/efflab-agent-sidecar.exe" 2>/dev/null || true
  elif [[ "${PROFILE}" == "debug" ]]; then
    ls -lh target/debug/efflab-agent-sidecar 2>/dev/null || true
    ls -lh "target/${WIN_TARGET}/debug/efflab-agent-sidecar.exe" 2>/dev/null || true
  else
    ls -lh "target/${PROFILE}/efflab-agent-sidecar" 2>/dev/null || true
    ls -lh "target/${WIN_TARGET}/${PROFILE}/efflab-agent-sidecar.exe" 2>/dev/null || true
  fi
else
  # 通用二进制分支（仅 Darwin 有意义）
  if [[ "${UNIVERSAL}" -eq 1 ]]; then
    if [[ "$(uname -s)" != "Darwin" ]]; then
      die "--universal 仅支持 macOS（Darwin）"
    fi
    if [[ -n "${TARGET}" ]]; then
      die "--universal 与 --target 互斥"
    fi
    info "模式: universal（aarch64-apple-darwin + x86_64-apple-darwin） profile=${PROFILE}"
    # 确保两个 target 已安装
    for t in aarch64-apple-darwin x86_64-apple-darwin; do
      ensure_target "${t}"
    done
    cargo_build_once "${PROFILE}" "aarch64-apple-darwin"
    cargo_build_once "${PROFILE}" "x86_64-apple-darwin"
    # 尝试 lipo 合并 sidecar 二进制
    if command -v lipo >/dev/null 2>&1; then
      if [[ "${PROFILE}" == "release" ]]; then
        rel_a="target/aarch64-apple-darwin/release/efflab-agent-sidecar"
        rel_x="target/x86_64-apple-darwin/release/efflab-agent-sidecar"
        out="target/universal-apple-darwin/release/efflab-agent-sidecar"
      elif [[ "${PROFILE}" == "debug" ]]; then
        rel_a="target/aarch64-apple-darwin/debug/efflab-agent-sidecar"
        rel_x="target/x86_64-apple-darwin/debug/efflab-agent-sidecar"
        out="target/universal-apple-darwin/debug/efflab-agent-sidecar"
      else
        rel_a="target/aarch64-apple-darwin/${PROFILE}/efflab-agent-sidecar"
        rel_x="target/x86_64-apple-darwin/${PROFILE}/efflab-agent-sidecar"
        out="target/universal-apple-darwin/${PROFILE}/efflab-agent-sidecar"
      fi
      if [[ -f "${rel_a}" && -f "${rel_x}" ]]; then
        mkdir -p "$(dirname "${out}")"
        info "合并通用二进制: lipo -create ${rel_a} ${rel_x} -output ${out}"
        lipo -create "${rel_a}" "${rel_x}" -output "${out}"
        info "产物: ${out}"
        lipo -info "${out}" || true
      else
        info "跳过 lipo：未找到双架构产物（${rel_a} / ${rel_x}）"
      fi
    else
      info "跳过 lipo：未找到 lipo 工具"
    fi
  else
    # 单 target / 本机构建
    if [[ -n "${TARGET}" ]]; then
      info "模式: 单 target=${TARGET} profile=${PROFILE}"
      ensure_target "${TARGET}"
    else
      info "模式: 本机 target profile=${PROFILE}"
    fi
    cargo_build_once "${PROFILE}" "${TARGET}"
  fi

  # ---------- 产物提示（非 --all 分支） ----------
  info "编译完成，产物预览:"
  if [[ -n "${TARGET}" ]]; then
    ls -lh "target/${TARGET}/${PROFILE}/efflab-agent-sidecar" 2>/dev/null || ls -lh "target/${TARGET}/release/efflab-agent-sidecar" 2>/dev/null || ls -lh "target/${TARGET}/debug/efflab-agent-sidecar" 2>/dev/null || true
    # Windows 产物带 .exe
    ls -lh "target/${TARGET}/${PROFILE}/efflab-agent-sidecar.exe" 2>/dev/null || ls -lh "target/${TARGET}/release/efflab-agent-sidecar.exe" 2>/dev/null || ls -lh "target/${TARGET}/debug/efflab-agent-sidecar.exe" 2>/dev/null || true
    ls -lh "target/${TARGET}/${PROFILE}/" 2>/dev/null | head -n 20 || true
  else
    # 兼容 --release 实际目录为 target/release
    if [[ "${PROFILE}" == "release" ]]; then
      ls -lh target/release/efflab-agent-sidecar 2>/dev/null || true
    elif [[ "${PROFILE}" == "debug" ]]; then
      ls -lh target/debug/efflab-agent-sidecar 2>/dev/null || true
    else
      ls -lh "target/${PROFILE}/efflab-agent-sidecar" 2>/dev/null || ls -lh "target/release/efflab-agent-sidecar" 2>/dev/null || true
    fi
  fi
fi

# ---------- 可选验证 ----------
if [[ "${DO_CHECK}" -eq 1 ]]; then
  info "执行静态检查: cargo check + clippy（efflab 三件套）"
  # cargo check（本机）
  cargo check -p efflab-agent-contract -p efflab-agent-host -p efflab-agent-sidecar ${LOCKED} || die "cargo check 失败"
  # cargo clippy（若可用）
  if cargo clippy --version >/dev/null 2>&1; then
    cargo clippy -p efflab-agent-contract -p efflab-agent-host -p efflab-agent-sidecar --all-targets ${LOCKED} || die "cargo clippy 失败"
  else
    info "跳过 clippy：未安装 clippy 组件（rustup component add clippy）"
  fi
  # 依赖边界：Host 不得依赖 sidecar / xai-grok-shell / xai-grok-tools
  info "检查依赖边界: cargo tree -p efflab-agent-host"
  if cargo tree -p efflab-agent-host 2>/dev/null | grep -qE 'efflab-agent-sidecar|xai-grok-shell|xai-grok-tools'; then
    die "依赖边界违规：efflab-agent-host 不应依赖 efflab-agent-sidecar / xai-grok-shell / xai-grok-tools"
  fi
fi

if [[ "${DO_TEST}" -eq 1 ]]; then
  info "执行测试: cargo test -p efflab-agent-contract -p efflab-agent-host -p efflab-agent-sidecar"
  cargo test -p efflab-agent-contract -p efflab-agent-host -p efflab-agent-sidecar ${LOCKED} || die "cargo test 失败"
fi

info "全部完成"
