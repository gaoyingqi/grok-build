#!/usr/bin/env bash
#
# fork-sync-apply.sh — 把 lab_main 的 fork 改动重放到上游同步后的 workspace。
#
# 背景：根 Cargo.toml 是上游生成物（README 明示只读），上游同步会覆盖我们新增的
# workspace member。本脚本在每次同步后把 Efflab Agent Kit 的三个 crate
# 重新注册进 workspace，并跟踪 FORK_BASE_REV（上次成功 apply 的上游基线）。
#
# 用法：
#   scripts/fork-sync-apply.sh --check   # 只检查，不写文件
#   scripts/fork-sync-apply.sh --apply   # 基线已验证后收敛 member
#
# 行为约束：
#   - 只维护声明的 Efflab member 行；绝不触碰 `crates/efflab/` 内任何文件
#   - 只删除已知、受管的过期 probe member，保留所有其它上游 member
#   - 检测到任一受管 member 重复出现（异常状态）时报错退出
#   - 仅接受生成的多行 workspace members 数组；其它布局 fail-closed
#   - 打印根 SOURCE_REV；FORK_BASE_REV 缺失或漂移时检查失败并拒绝 apply
#   - 不自动 merge / rebase / push
set -euo pipefail

# 解析仓库根目录（脚本固定位于根 scripts/ 下）
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MANIFEST="$ROOT/Cargo.toml"
MEMBERS=(
  "crates/efflab/efflab-agent-contract"
  "crates/efflab/efflab-agent-host"
  "crates/efflab/efflab-agent-sidecar"
)
ANCHOR_MEMBER="prod/mc/cli-chat-proxy-types"
# 仅允许收敛这个已知的 PR0 临时 probe；其它 workspace member 不是本脚本的删除对象。
MANAGED_STALE_MEMBERS=(
  "crates/efflab/efflab-pr0-http-probe"
)
FORK_BASE_REV_FILE="$ROOT/FORK_BASE_REV"

# 模式参数
if [[ $# -ne 1 || ( "$1" != "--check" && "$1" != "--apply" ) ]]; then
  echo "usage: $0 --check|--apply" >&2
  exit 2
fi
MODE="$1"

# 只接受非空、单行、完整的 40 位小写 ASCII 十六进制 commit SHA；多余内容一律拒绝。
# 仓库中的 SOURCE_REV/FORK_BASE_REV 与 git rev-parse 输出均采用小写，故不做大小写归一化。
read_revision() {
  local file="$1"
  awk '
    NR == 1 { value = $0 }
    END {
      if (NR != 1 || length(value) != 40 || value ~ /[^0-9a-f]/) {
        exit 1
      }
      print value
    }
  ' "$file"
}

# 打印并严格校验 SOURCE_REV；缺失或格式异常均视为异常。
if [[ ! -f "$ROOT/SOURCE_REV" ]]; then
  echo "error: SOURCE_REV missing: $ROOT/SOURCE_REV" >&2
  exit 2
fi
if ! CURRENT="$(read_revision "$ROOT/SOURCE_REV")"; then
  echo "error: SOURCE_REV invalid: expected one non-empty line with a 40-character lowercase hexadecimal commit SHA" >&2
  exit 2
fi
echo "SOURCE_REV: $CURRENT"

# 前置校验：根 manifest 与全部 member manifest 必须存在。
if [[ ! -f "$MANIFEST" ]]; then
  echo "error: root manifest not found: $MANIFEST" >&2
  exit 2
fi
for member in "${MEMBERS[@]}"; do
  if [[ ! -f "$ROOT/$member/Cargo.toml" ]]; then
    echo "error: member manifest not found: $ROOT/$member/Cargo.toml" >&2
    exit 2
  fi
done

# 解析限制：根 Cargo.toml 由 Cargo 生成并保持受限的多行 members 数组；其它布局统一 fail-closed。
# 词法扫描先忽略普通字符串和注释，拒绝三引号字符串及 workspace 相关歧义布局，避免伪造表头进入状态机。
validate_workspace_members() {
  awk '
    function trim(value) {
      sub(/^[[:space:]]*/, "", value)
      sub(/[[:space:]]*$/, "", value)
      return value
    }
    function strip_comment(value,    result, quote, escaped, i, character) {
      result = ""
      quote = ""
      escaped = 0
      for (i = 1; i <= length(value); i++) {
        character = substr(value, i, 1)
        if (quote == "basic") {
          result = result character
          if (escaped) {
            escaped = 0
          } else if (character == "\\") {
            escaped = 1
          } else if (character == "\"") {
            if (substr(value, i, 3) == "\"\"\"") {
              invalid = 1
              return result
            }
            quote = ""
          }
          continue
        }
        if (quote == "literal") {
          result = result character
          if (character == sprintf("%c", 39)) {
            if (substr(value, i, 3) == sprintf("%c%c%c", 39, 39, 39)) {
              invalid = 1
              return result
            }
            quote = ""
          }
          continue
        }
        if (character == "#") {
          break
        }
        if (character == "\"") {
          if (substr(value, i, 3) == "\"\"\"") {
            invalid = 1
            return result
          }
          quote = "basic"
          result = result character
          continue
        }
        if (character == sprintf("%c", 39)) {
          if (substr(value, i, 3) == sprintf("%c%c%c", 39, 39, 39)) {
            invalid = 1
            return result
          }
          quote = "literal"
          result = result character
          continue
        }
        result = result character
      }
      if (quote != "") {
        invalid = 1
      }
      return result
    }
    # 归一化表头点号两侧空白，避免等价 workspace 表头绕过状态检查。
    function normalize_table(value) {
      value = trim(value)
      gsub(/[[:space:]]*\.[[:space:]]*/, ".", value)
      return value
    }
    function assignment_key(value, equal_sign) {
      equal_sign = index(value, "=")
      if (equal_sign == 0) {
        return ""
      }
      return trim(substr(value, 1, equal_sign - 1))
    }
    function is_workspace_assignment(value, key, quote) {
      key = assignment_key(value)
      quote = sprintf("%c", 34)
      return key ~ /^workspace([[:space:]]*(\.|$))/ ||
             key ~ ("^" quote "workspace" quote "([[:space:]]*(\\.|$))") ||
             key ~ ("^" sprintf("%c", 39) "workspace" sprintf("%c", 39) "([[:space:]]*(\\.|$))")
    }
    function is_quoted_assignment(value, key, quote_chars) {
      key = assignment_key(value)
      quote_chars = "[" sprintf("%c", 34) sprintf("%c", 39) "]"
      return key ~ ("^" quote_chars)
    }
    function is_members_dotted_assignment(value, key) {
      key = assignment_key(value)
      return key ~ /^members[[:space:]]*\./
    }
    # quoted dotted key 不属于受限生成布局；拒绝 workspace 前缀和任意 quoted 首段，
    # 这样无需解码转义就能对等价写法保持 fail-closed，未知布局也不会被放宽。
    function is_unsupported_quoted_dotted_key(value, quote_chars, quoted_body) {
      quote_chars = "[" sprintf("%c", 34) sprintf("%c", 39) "]"
      quoted_body = "[^" sprintf("%c", 34) sprintf("%c", 39) "]+"
      return value ~ ("^workspace[[:space:]]*\\.[[:space:]]*" quote_chars) ||
             value ~ ("^" quote_chars quoted_body quote_chars "[[:space:]]*\\.")
    }
    {
      line = trim(strip_comment($0))
      if (line == "") {
        next
      }
      if (in_members) {
        if (line ~ /^\][[:space:]]*$/) {
          in_members = 0
          next
        }
        if (last_item_without_comma) {
          invalid = 1
          next
        }
        # 受限生成路径不含转义；无法安全解码的 basic string 一律拒绝。
        if (index(line, sprintf("%c", 92)) > 0) {
          invalid = 1
          next
        }
        if (line ~ /^\"[^\"]+\"[[:space:]]*,[[:space:]]*$/) {
          last_item_without_comma = 0
          next
        }
        if (line ~ /^\"[^\"]+\"[[:space:]]*$/) {
          last_item_without_comma = 1
          next
        }
        invalid = 1
        next
      }
      if (((section == "" || section == "workspace") && is_workspace_assignment(line)) ||
          is_unsupported_quoted_dotted_key(line) ||
          (section == "" && is_quoted_assignment(line))) {
        invalid = 1
        next
      }
      if (line ~ /^\[\[/) {
        invalid = 1
        next
      }
      if (substr(line, 1, 1) == "[") {
        if (line !~ /^\[[^]]+\][[:space:]]*$/) {
          invalid = 1
          next
        }
        table = substr(line, 2, length(line) - 2)
        normalized_table = normalize_table(table)
        if (index(table, "\"") > 0 || index(table, sprintf("%c", 39)) > 0 ||
            normalized_table == "" || normalized_table ~ /(^\.|\.$|\.\.)/ ||
            normalized_table == "workspace.members" ||
            normalized_table ~ /^workspace\.members\./) {
          invalid = 1
        }
        if (normalized_table == "workspace") {
          if (workspace_seen) {
            invalid = 1
          }
          workspace_seen = 1
          section = "workspace"
        } else {
          section = normalized_table
        }
        next
      }
      if (section == "workspace") {
        key = assignment_key(line)
        if (key == "members") {
          if (members_seen || line !~ /^members[[:space:]]*=[[:space:]]*\[[[:space:]]*$/) {
            invalid = 1
          } else {
            members_seen = 1
            in_members = 1
            last_item_without_comma = 0
          }
        } else if (is_members_dotted_assignment(line) || is_quoted_assignment(line)) {
          invalid = 1
        }
      }
    }
    END {
      if (in_members || !workspace_seen || members_seen != 1) {
        invalid = 1
      }
      exit invalid ? 1 : 0
    }
  ' "$MANIFEST"
}

if ! validate_workspace_members; then
  echo "error: workspace members layout is unsupported or ambiguous (expected one closed multi-line array); refusing $MODE" >&2
  exit 2
fi

# 只按 workspace members 数组内的完整 manifest 行计数，避免注释或相似路径伪造 member 状态。
CHECK_FAILED=0
member_count() {
  local member="$1"
  awk -v needle="$member" '
    function trim(value) {
      sub(/^[[:space:]]*/, "", value)
      sub(/[[:space:]]*$/, "", value)
      return value
    }
    # 与前置 validator 使用相同的表头归一化，确保 [ workspace ] 可被计数。
    function normalize_table(value) {
      value = trim(value)
      gsub(/[[:space:]]*\.[[:space:]]*/, ".", value)
      return value
    }
    function is_member_line(value, member_name, token, rest) {
      value = trim(value)
      token = "\"" member_name "\""
      if (substr(value, 1, length(token)) != token) {
        return 0
      }
      rest = trim(substr(value, length(token) + 1))
      if (substr(rest, 1, 1) == ",") {
        rest = trim(substr(rest, 2))
      }
      return rest == "" || substr(rest, 1, 1) == "#"
    }
    {
      line = trim($0)
      if (substr(line, 1, 1) == "[") {
        section_end = index(line, "]")
        if (section_end > 1) {
          suffix = trim(substr(line, section_end + 1))
          if (suffix == "" || substr(suffix, 1, 1) == "#") {
            section = "[" normalize_table(substr(line, 2, section_end - 2)) "]"
            in_members = 0
            next
          }
        }
      }
      if (!in_members && section == "[workspace]" &&
          line ~ /^members[[:space:]]*=[[:space:]]*\[/) {
        in_members = 1
      }
      if (in_members && is_member_line(line, needle)) {
        count++
      }
      if (in_members && line ~ /^\][[:space:]]*(#.*)?$/) {
        in_members = 0
      }
    }
    END { print count + 0 }
  ' "$MANIFEST"
}

# 统计期望 member 当前出现次数（异常重复 → 失败），并收集缺失项。
MISSING=()
for member in "${MEMBERS[@]}"; do
  count="$(member_count "$member")"
  if [[ "$count" -gt 1 ]]; then
    echo "error: member '$member' appears $count times in $MANIFEST (corrupt state)" >&2
    exit 2
  fi
  if [[ "$count" -eq 0 ]]; then
    MISSING+=("$member")
  else
    echo "member already present: $member"
  fi
done

# 只收集脚本明确管理的 stale member，任何其它上游 member 都原样保留。
STALE=()
for member in "${MANAGED_STALE_MEMBERS[@]}"; do
  count="$(member_count "$member")"
  if [[ "$count" -gt 1 ]]; then
    echo "error: managed stale member '$member' appears $count times in $MANIFEST (corrupt state)" >&2
    exit 2
  fi
  if [[ "$count" -eq 1 ]]; then
    STALE+=("$member")
    echo "managed stale member present: $member"
  fi
done

# 所有模式都先确认 anchor 状态；无效 anchor 时不得触碰 manifest 或 FORK_BASE_REV。
ANCHOR_COUNT="$(member_count "$ANCHOR_MEMBER")"
if [[ "$ANCHOR_COUNT" -ne 1 ]]; then
  echo "error: manifest anchor '$ANCHOR_MEMBER' appears $ANCHOR_COUNT times in $MANIFEST; refusing $MODE" >&2
  exit 2
fi

# 读取并分类基线但不写入；SOURCE_REV 漂移必须由外部 contract tests 验证后再处理。
BASE=""
BASE_STATUS="missing"
if [[ -f "$FORK_BASE_REV_FILE" ]]; then
  if ! BASE="$(read_revision "$FORK_BASE_REV_FILE")"; then
    BASE_STATUS="invalid"
  elif [[ "$BASE" == "$CURRENT" ]]; then
    BASE_STATUS="current"
  else
    BASE_STATUS="stale"
  fi
fi

if [[ "$MODE" == "--check" ]]; then
  case "$BASE_STATUS" in
    missing)
      echo "check: FORK_BASE_REV missing: $FORK_BASE_REV_FILE"
      CHECK_FAILED=1
      ;;
    invalid)
      echo "check: FORK_BASE_REV invalid: expected one non-empty line with a 40-character lowercase hexadecimal commit SHA"
      CHECK_FAILED=1
      ;;
    stale)
      echo "check: FORK_BASE_REV drift: $BASE -> $CURRENT"
      echo "  Fork contract tests MUST be re-run before reconciling FORK_BASE_REV"
      echo "  (cargo test -p efflab-agent-sidecar + workspace check)."
      CHECK_FAILED=1
      ;;
  esac
  if [[ "${#MISSING[@]}" -gt 0 ]]; then
    printf 'check: member missing: %s (would add)\n' "${MISSING[@]}"
    CHECK_FAILED=1
  fi
  if [[ "${#STALE[@]}" -gt 0 ]]; then
    printf 'check: stale member: %s (would remove)\n' "${STALE[@]}"
    CHECK_FAILED=1
  fi
  exit "$CHECK_FAILED"
fi

# --apply 在任何 manifest 改写前拒绝缺失/漂移基线，避免未验证时静默推进状态。
if [[ "$BASE_STATUS" != "current" ]]; then
  case "$BASE_STATUS" in
    missing)
      echo "error: FORK_BASE_REV missing; refusing --apply until contract tests pass" >&2
      ;;
    invalid)
      echo "error: FORK_BASE_REV invalid: expected one non-empty line with a 40-character lowercase hexadecimal commit SHA" >&2
      ;;
    stale)
      echo "error: FORK_BASE_REV drift: $BASE -> $CURRENT; refusing --apply" >&2
      echo "  Fork contract tests MUST be re-run before reconciling FORK_BASE_REV" >&2
      echo "  (cargo test -p efflab-agent-sidecar + workspace check)." >&2
      ;;
  esac
  exit 2
fi

# 基线已经验证且 manifest 无需收敛时直接返回，不改写任何状态文件。
if [[ "${#MISSING[@]}" -eq 0 && "${#STALE[@]}" -eq 0 ]]; then
  exit 0
fi

# --apply：只删除受管 stale 行，并在固定 anchor 前插入缺失 member；其它行全部透传。
# 使用 manifest 临时副本后原子替换，根 Cargo.toml 仍由本脚本生成。
TMP="$MANIFEST.tmp.$$"
trap 'rm -f "$TMP"' EXIT
if ! awk \
  -v additions="$(IFS=:; echo "${MISSING[*]-}")" \
  -v stale="$(IFS=:; echo "${STALE[*]-}")" \
  -v anchor="$ANCHOR_MEMBER" \
  '
  function trim(value) {
    sub(/^[[:space:]]*/, "", value)
    sub(/[[:space:]]*$/, "", value)
    return value
  }
  # 与前置 validator 使用相同的表头归一化，确保 [ workspace ] 可被重写。
  function normalize_table(value) {
    value = trim(value)
    gsub(/[[:space:]]*\.[[:space:]]*/, ".", value)
    return value
  }
  function is_member_line(value, member_name, token, rest) {
    value = trim(value)
    token = "\"" member_name "\""
    if (substr(value, 1, length(token)) != token) {
      return 0
    }
    rest = trim(substr(value, length(token) + 1))
    if (substr(rest, 1, 1) == ",") {
      rest = trim(substr(rest, 2))
    }
    return rest == "" || substr(rest, 1, 1) == "#"
  }
  BEGIN {
    addition_count = split(additions, addition, ":")
    stale_count = split(stale, stale_member, ":")
  }
  {
    original = $0
    line = trim(original)
    if (substr(line, 1, 1) == "[") {
      section_end = index(line, "]")
      if (section_end > 1) {
        suffix = trim(substr(line, section_end + 1))
        if (suffix == "" || substr(suffix, 1, 1) == "#") {
          section = "[" normalize_table(substr(line, 2, section_end - 2)) "]"
          in_members = 0
          print original
          next
        }
      }
    }
    if (!in_members && section == "[workspace]" &&
        line ~ /^members[[:space:]]*=[[:space:]]*\[/) {
      in_members = 1
      members_value = substr(line, index(line, "[") + 1)
    } else if (in_members) {
      members_value = line
    }
    remove = 0
    if (in_members) {
      for (i = 1; i <= stale_count; i++) {
        if (stale_member[i] != "" && is_member_line(line, stale_member[i])) {
          remove = 1
        }
      }
      if (!remove && is_member_line(line, anchor)) {
        for (i = 1; i <= addition_count; i++) {
          if (addition[i] != "") {
            print "    \"" addition[i] "\",";
          }
        }
      }
    }
    if (!remove) {
      print original
    }
    if (in_members && line ~ /^\][[:space:]]*(#.*)?$/) {
      in_members = 0
    }
  }
' "$MANIFEST" > "$TMP"; then
  echo "error: failed to rewrite generated workspace manifest" >&2
  exit 2
fi
mv "$TMP" "$MANIFEST"
trap - EXIT

if [[ "${#STALE[@]}" -gt 0 ]]; then
  printf 'applied: removed stale member %s\n' "${STALE[@]}"
fi
if [[ "${#MISSING[@]}" -gt 0 ]]; then
  printf 'applied: added member %s\n' "${MISSING[@]}"
fi
# FORK_BASE_REV 已在改写前确认与 SOURCE_REV 一致，此处保持基线文件不变。
