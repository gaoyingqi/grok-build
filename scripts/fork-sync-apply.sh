#!/usr/bin/env bash
#
# fork-sync-apply.sh — 把 lab_main 的 fork 改动重放到上游同步后的 workspace。
#
# 背景：根 Cargo.toml 是上游生成物（README 明示只读），上游同步会覆盖我们新增的
# workspace member。本脚本在每次同步后把 `crates/efflab/efflab-agent-sidecar`
# 重新注册进 workspace，并跟踪 FORK_BASE_REV（上次成功 apply 的上游基线）。
#
# 用法：
#   scripts/fork-sync-apply.sh --check   # 只检查，不写文件
#   scripts/fork-sync-apply.sh --apply   # 幂等插入 member + 按需推进 FORK_BASE_REV
#
# 行为约束：
#   - 只新增一个 member 行，绝不触碰 `crates/efflab/` 内任何文件
#   - 检测到 member 重复出现（异常状态）时报错退出
#   - 打印根 SOURCE_REV；SOURCE_REV 与 FORK_BASE_REV 不一致时提示必须跑 contract tests
#   - 不自动 merge / rebase / push
set -euo pipefail

# 解析仓库根目录（脚本固定位于根 scripts/ 下）
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MANIFEST="$ROOT/Cargo.toml"
MEMBER="crates/efflab/efflab-agent-sidecar"
FORK_BASE_REV_FILE="$ROOT/FORK_BASE_REV"

# 模式参数
if [[ $# -ne 1 || ( "$1" != "--check" && "$1" != "--apply" ) ]]; then
  echo "usage: $0 --check|--apply" >&2
  exit 2
fi
MODE="$1"

# 打印 SOURCE_REV；缺失视为异常
if [[ -f "$ROOT/SOURCE_REV" ]]; then
  CURRENT="$(cat "$ROOT/SOURCE_REV")"
  echo "SOURCE_REV: $CURRENT"
else
  echo "error: SOURCE_REV missing: $ROOT/SOURCE_REV" >&2
  exit 2
fi

# 前置校验：根 manifest 与 member manifest 必须存在
if [[ ! -f "$MANIFEST" ]]; then
  echo "error: root manifest not found: $MANIFEST" >&2
  exit 2
fi
if [[ ! -f "$ROOT/$MEMBER/Cargo.toml" ]]; then
  echo "error: member manifest not found: $ROOT/$MEMBER/Cargo.toml" >&2
  exit 2
fi

# FORK_BASE_REV 管理：缺失时 --apply 创建；--check 报告缺失
if [[ ! -f "$FORK_BASE_REV_FILE" ]]; then
  if [[ "$MODE" == "--check" ]]; then
    echo "check: FORK_BASE_REV missing (would create from SOURCE_REV)"
    exit 1
  fi
  cp "$ROOT/SOURCE_REV" "$FORK_BASE_REV_FILE"
  echo "created FORK_BASE_REV: $CURRENT"
else
  BASE="$(cat "$FORK_BASE_REV_FILE")"
  if [[ "$BASE" != "$CURRENT" ]]; then
    echo "WARNING: SOURCE_REV changed: $BASE -> $CURRENT"
    echo "  Fork contract tests MUST be re-run before advancing FORK_BASE_REV"
    echo "  (cargo test -p efflab-agent-sidecar + workspace check)."
  fi
fi

# 统计 member 当前出现次数（异常重复 → 失败）
COUNT="$(grep -c "\"$MEMBER\"" "$MANIFEST" || true)"
if [[ "$COUNT" -gt 1 ]]; then
  echo "error: member '$MEMBER' appears $COUNT times in $MANIFEST (corrupt state)" >&2
  exit 2
fi

if [[ "$COUNT" -eq 1 ]]; then
  echo "member already present: $MEMBER"
  # member 已就位：若 SOURCE_REV 变化，--apply 推进 FORK_BASE_REV（本地记录）。
  if [[ "$MODE" == "--apply" && -f "$FORK_BASE_REV_FILE" ]]; then
    BASE="$(cat "$FORK_BASE_REV_FILE")"
    if [[ "$BASE" != "$CURRENT" ]]; then
      cp "$ROOT/SOURCE_REV" "$FORK_BASE_REV_FILE"
      echo "advanced FORK_BASE_REV: $BASE -> $CURRENT"
    fi
  fi
  exit 0
fi

if [[ "$MODE" == "--check" ]]; then
  echo "check: member missing: $MEMBER (would add)"
  exit 1
fi

# --apply：在 members 数组的字母序位置（"prod/mc/..." 之前）插入 member 行。
# 幂等：下次再跑会命中 COUNT=1 分支直接退出。
TMP="$MANIFEST.tmp.$$"
trap 'rm -f "$TMP"' EXIT
awk -v member="    \"$MEMBER\"," '
  BEGIN { inserted = 0 }
  /^    "prod\/mc\/cli-chat-proxy-types",$/ && !inserted {
    print member
    inserted = 1
  }
  { print }
' "$MANIFEST" > "$TMP"
mv "$TMP" "$MANIFEST"
trap - EXIT

echo "applied: added member '$MEMBER'"
# 首次 apply 成功后推进 FORK_BASE_REV（若尚未创建/已过期）。
if [[ -f "$FORK_BASE_REV_FILE" ]]; then
  BASE="$(cat "$FORK_BASE_REV_FILE")"
  if [[ "$BASE" != "$CURRENT" ]]; then
    cp "$ROOT/SOURCE_REV" "$FORK_BASE_REV_FILE"
    echo "advanced FORK_BASE_REV: $BASE -> $CURRENT"
  fi
else
  cp "$ROOT/SOURCE_REV" "$FORK_BASE_REV_FILE"
  echo "created FORK_BASE_REV: $CURRENT"
fi
