#!/usr/bin/env python3
"""检查 efflab-agent-sidecar 的发布 normal/build 依赖闭包。

该门禁只读取 Cargo 的锁定解析结果，不改变 workspace，也不读取 debug 产物。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence


# 简体中文注释：Task20 合同固定为 v1；新增向后兼容报告字段不得改动该版本。
SIDECAR_CLOSURE_GATE_VERSION = 1
GATE_VERSION = SIDECAR_CLOSURE_GATE_VERSION
SIDECAR_PACKAGE = "efflab-agent-sidecar"
SCAN_MODES = ("closure-only", "release-certification")
PUBLISHED_TARGETS = frozenset(
    {
        "aarch64-apple-darwin",
        "x86_64-apple-darwin",
        "x86_64-pc-windows-msvc",
    }
)

# 简体中文注释：这些 crate 会把旧 shell、认证、遥测、远程更新或完整工具运行时带入 sidecar。
FORBIDDEN_PACKAGES = frozenset(
    {
        "xai-grok-shell",
        "xai-grok-agent",
        "xai-grok-sampler",
        "xai-grok-auth",
        "xai-grok-telemetry",
        "xai-mixpanel",
        "xai-tracing",
        "fastrace-opentelemetry",
        "xai-grok-mcp",
        "oauth2",
        "webbrowser",
        "xai-grok-update",
        "gcloud-storage",
        "xai-grok-tools",
        "xai-tool-runtime",
        "xai-tool-protocol",
        "xai-tool-types",
    }
)
# 简体中文注释：Cargo 中的 opentelemetry、opentelemetry_sdk 等名称都必须被同一前缀规则拦截。
FORBIDDEN_PACKAGE_PREFIXES = ("opentelemetry",)
ALLOWED_EDGE_KINDS = frozenset({"normal", "build"})
WORKSPACE_MEMBER_SNAPSHOT = Path(__file__).resolve().with_name("sidecar_workspace_members.txt")

_PACKAGE_RE = re.compile(
    r"^(?P<name>[A-Za-z0-9][A-Za-z0-9_-]*)\s+v(?P<version>[^\s(]+)(?:\s|$)"
)
_STRICT_PACKAGE_RE = re.compile(
    r"^(?P<name>[A-Za-z0-9][A-Za-z0-9_-]*)\s+v(?P<version>[^\s(]+)(?P<tail>.*)$"
)
_FEATURE_RE = re.compile(
    r'^(?P<name>[A-Za-z0-9][A-Za-z0-9_-]*)\s+feature\s+"(?P<feature>[^\"]+)"'
)
_STRICT_FEATURE_RE = re.compile(
    r'^(?P<name>[A-Za-z0-9][A-Za-z0-9_-]*)\s+feature\s+'
    r'"(?P<feature>[^\"\s]+)"(?P<tail>.*)$'
)
# 简体中文注释：Cargo feature 节点是合法的 TOML feature key：首字符不能是连字符，
# 其余字符仅允许 ASCII 字母、数字、下划线和连字符；`?` 等依赖值语法不会出现在节点名中。
_FEATURE_TOKEN_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_-]*$")
_UTF8_TREE_PREFIX_RE = re.compile(r"^(?:(?:│   |    )*(?:├── |└── ))")
_UTF8_TREE_CONTINUATION_RE = re.compile(r"^(?:(?:│   |    )+)")
_ASCII_TREE_PREFIX_RE = re.compile(r"^(?:(?:\|   |    )*(?:\|-- |`-- |\\-- ))")
_ASCII_TREE_CONTINUATION_RE = re.compile(r"^(?:(?:\|   |    )+)")
_TREE_PREFIX_LEAD_CHARS = frozenset("│├└─┬|+`\\-")
_TREE_SECTION_RE = re.compile(r"^\[(?:dependencies|build-dependencies|dev-dependencies)\]$")
_MISSING_PACKAGE_RE = re.compile(
    r"^error: package ID specification `(?P<package_id>[^`\r\n]+)` "
    r"did not match any packages$"
)
# 简体中文注释：Cargo 并发访问 package cache 时可能先输出这一条固定状态行。
_CARGO_PACKAGE_CACHE_LOCK_WAIT = "Blocking waiting for file lock on package cache"


class ClosureGateError(RuntimeError):
    """表示 Cargo 输入或门禁参数不可用，而不是依赖闭包命中。"""


@dataclass(frozen=True)
class StringPolicy:
    """版本化 release binary 字符串扫描策略。"""

    denylist: tuple[str, ...]
    allowlist: tuple[str, ...]


STRING_POLICY_FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "crates"
    / "efflab"
    / "efflab-agent-sidecar"
    / "tests"
    / "fixtures"
    / "denylist_strings.txt"
)
# 简体中文注释：版本化默认 fixture 必须持续覆盖这些已确认的出网/遥测字符串。
REQUIRED_DEFAULT_DENYLIST = frozenset(
    {"grok.com", "x.com", "api.x.ai", "mixpanel", "otlp", "trace upload"}
)
PACKAGE_HIT_PREFIX = "package:"
BINARY_HIT_PREFIX = "binary:"


def _repo_root() -> Path:
    """简体中文注释：从脚本路径解析 sibling 根，避免依赖调用者当前目录。"""
    return Path(__file__).resolve().parents[1]


def _same_path(left: Path, right: Path) -> bool:
    """比较规范化路径，阻止 policy fixture 被当成 binary 或 strings 输入。"""
    try:
        return left.resolve() == right.resolve()
    except OSError as error:
        raise ClosureGateError(f"cannot resolve path for strings scan: {error}") from error


def _is_default_string_policy_fixture(fixture: Path) -> bool:
    """判断策略路径是否指向版本化默认 fixture。"""
    try:
        return fixture.resolve() == Path(STRING_POLICY_FIXTURE).resolve()
    except OSError as error:
        raise ClosureGateError(f"cannot resolve strings policy fixture {fixture}: {error}") from error


def load_string_policy(path: Path | None = None) -> StringPolicy:
    """读取严格的 denylist/allowlist fixture，并拒绝不完整或重复策略。"""
    fixture = Path(path) if path is not None else STRING_POLICY_FIXTURE
    if not fixture.is_file():
        raise ClosureGateError(
            f"strings policy fixture is missing or not a file: {fixture}; "
            "restore the versioned denylist_strings.txt fixture"
        )

    entries: dict[str, list[str]] = {"denylist": [], "allowlist": []}
    seen_sections: set[str] = set()
    section: str | None = None
    try:
        lines = fixture.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise ClosureGateError(f"cannot read strings policy fixture {fixture}: {error}") from error

    for line_number, raw_line in enumerate(lines, start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("[") and line.endswith("]"):
            candidate = line[1:-1].strip()
            if candidate not in entries:
                raise ClosureGateError(
                    f"strings policy fixture {fixture}:{line_number} has unknown section {candidate!r}"
                )
            if candidate in seen_sections:
                raise ClosureGateError(
                    f"strings policy fixture {fixture}:{line_number} repeats section {candidate!r}"
                )
            seen_sections.add(candidate)
            section = candidate
            continue
        if section is None:
            raise ClosureGateError(
                f"strings policy fixture {fixture}:{line_number} has an entry outside a section"
            )
        entries[section].append(line)

    if not entries["denylist"] or seen_sections != {"denylist", "allowlist"}:
        raise ClosureGateError(
            f"strings policy fixture {fixture} must contain non-empty [denylist] and [allowlist] sections"
        )

    normalized: dict[str, tuple[str, ...]] = {}
    for name, values in entries.items():
        seen: set[str] = set()
        ordered: list[str] = []
        for value in values:
            key = value.casefold()
            if key in seen:
                raise ClosureGateError(
                    f"strings policy fixture {fixture} contains duplicate {name} entry {value!r}"
                )
            seen.add(key)
            ordered.append(value)
        normalized[name] = tuple(sorted(ordered, key=str.casefold))

    if _is_default_string_policy_fixture(fixture):
        required = {entry.casefold() for entry in REQUIRED_DEFAULT_DENYLIST}
        actual = {entry.casefold() for entry in normalized["denylist"]}
        missing = sorted(required - actual)
        if missing:
            raise ClosureGateError(
                f"strings policy fixture {fixture} is missing required default denylist entries: "
                + ", ".join(missing)
            )

    for deny_entry in normalized["denylist"]:
        deny_folded = deny_entry.casefold()
        for allow_entry in normalized["allowlist"]:
            allow_folded = allow_entry.casefold()
            if deny_folded in allow_folded or allow_folded in deny_folded:
                raise ClosureGateError(
                    f"strings policy fixture {fixture} has overlapping denylist/allowlist entries: "
                    f"{deny_entry!r} and {allow_entry!r}"
                )

    return StringPolicy(
        denylist=normalized["denylist"],
        allowlist=normalized["allowlist"],
    )


def scan_strings_text(strings_text: str, policy: StringPolicy) -> list[str]:
    """扫描固定 strings 文本，返回稳定排序的 canonical denylist 命中项。"""
    if not isinstance(strings_text, str):
        raise ClosureGateError("strings input must be text")

    allowlist = {entry.casefold() for entry in policy.allowlist}
    hits: set[str] = set()
    for raw_line in strings_text.splitlines():
        line = raw_line.strip()
        if not line or line.casefold() in allowlist:
            continue
        folded = line.casefold()
        for entry in policy.denylist:
            if entry.casefold() in folded:
                hits.add(entry)
    return sorted(hits, key=str.casefold)


def _read_strings_command(binary: Path) -> str:
    """调用外部 strings 工具读取 release binary；工具缺失或失败均拒绝。"""
    tool = shutil.which("strings")
    if tool is None:
        raise ClosureGateError("strings tool is unavailable; install strings before scanning the release binary")
    try:
        result = subprocess.run(
            [tool, "-a", str(binary)],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
    except OSError as error:
        raise ClosureGateError(f"cannot launch strings tool: {error}") from error
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise ClosureGateError(
            f"strings scan failed with exit code {result.returncode}: {detail or 'no diagnostic'}"
        )
    return result.stdout


def _release_binary_name(target: str) -> str:
    """按 target 返回 Cargo release binary 的稳定文件名。"""
    return SIDECAR_PACKAGE + (".exe" if target.endswith("-windows-msvc") else "")


def _reject_linked_path_components(
    path: Path, description: str, boundary: Path | None = None
) -> None:
    """拒绝 workspace 内路径中的 symlink 或 Windows reparse point。"""
    candidate = Path(path)
    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate
    candidate = Path(os.path.abspath(candidate))

    if boundary is not None:
        boundary_path = Path(boundary)
        if not boundary_path.is_absolute():
            boundary_path = Path.cwd() / boundary_path
        boundary_path = Path(os.path.abspath(boundary_path))
        try:
            relative = candidate.relative_to(boundary_path)
        except ValueError as error:
            raise ClosureGateError(
                f"{description} is outside workspace root {boundary_path}: {candidate}"
            ) from error
        current = boundary_path
        components = relative.parts
    else:
        current = Path(candidate.anchor) if candidate.anchor else Path.cwd()
        start = 1 if candidate.anchor else 0
        components = candidate.parts[start:]

    for component in components:
        if component == ".":
            continue
        if component == "..":
            current = current.parent
            continue
        current /= component
        try:
            file_info = current.lstat()
        except FileNotFoundError:
            continue
        except OSError as error:
            raise ClosureGateError(
                f"cannot inspect {description} path component {current}: {error}"
            ) from error
        is_reparse = bool(
            getattr(file_info, "st_file_attributes", 0)
            & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)
        )
        if stat.S_ISLNK(file_info.st_mode) or is_reparse:
            raise ClosureGateError(
                f"{description} contains a symlink or reparse point: {current}"
            )


def _expected_release_binary_path(repo_root: Path, target: str) -> Path:
    """构造并校验显式 Cargo target 的唯一 release binary 路径。"""
    if target not in PUBLISHED_TARGETS or target == "all":
        allowed = ", ".join(sorted(PUBLISHED_TARGETS))
        raise ClosureGateError(f"unsupported release target {target!r}; use one of: {allowed}")

    supplied_root = Path(repo_root)
    try:
        root = supplied_root.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise ClosureGateError(f"cannot resolve workspace root {supplied_root}: {error}") from error
    if not root.is_dir():
        raise ClosureGateError(f"workspace root is not a directory: {supplied_root}")
    _reject_linked_path_components(root, "workspace root")

    # 产品构建总是传入 --target；认证不得按当前 Python host 改用 target/release。
    release_directory = root / "target" / target / "release"
    expected = release_directory / _release_binary_name(target)
    _reject_linked_path_components(expected, "release binary", boundary=root)
    return expected


def _validate_release_binary_path(binary_path: Path, target: str, repo_root: Path) -> Path:
    """只允许 Cargo 约定的 release binary，并校验解析后仍位于该路径。"""
    expected = _expected_release_binary_path(repo_root, target)
    binary = Path(binary_path)
    _reject_linked_path_components(binary, "release binary", boundary=Path(repo_root))
    try:
        resolved_binary = binary.resolve(strict=False)
    except (OSError, RuntimeError) as error:
        raise ClosureGateError(f"cannot resolve release binary {binary}: {error}") from error

    if resolved_binary != expected:
        if "debug" in binary.parts or "debug" in resolved_binary.parts:
            raise ClosureGateError(
                f"debug binary is not eligible for release strings scan: {binary}; "
                f"expected {expected}"
            )
        if binary.name != expected.name:
            raise ClosureGateError(
                f"release binary {binary.name!r} does not match expected target filename "
                f"{expected.name!r}"
            )
        raise ClosureGateError(
            f"release binary must be Cargo target {target!r} under {expected.parent}, not {binary}"
        )
    if not binary.is_file():
        raise ClosureGateError(f"release binary is missing or not a file: {binary}")
    try:
        file_info = binary.stat()
    except OSError as error:
        raise ClosureGateError(f"cannot inspect release binary {binary}: {error}") from error
    if file_info.st_size <= 0:
        raise ClosureGateError(f"release binary is empty: {binary}")
    if not target.endswith("-windows-msvc") and not (
        file_info.st_mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    ):
        raise ClosureGateError(f"Unix release binary is not executable: {binary}")
    return binary


def scan_release_binary(
    binary_path: Path,
    strings_output: str | None = None,
    string_fixture_path: Path | None = None,
    *,
    target: str,
    repo_root: Path,
) -> list[str]:
    """扫描 Cargo 约定的真实 release binary，或扫描显式提供的固定 strings 文本。"""
    binary = Path(binary_path)
    fixture = Path(string_fixture_path) if string_fixture_path is not None else STRING_POLICY_FIXTURE
    if _same_path(binary, fixture):
        raise ClosureGateError(
            f"strings policy fixture cannot be used as release binary: {binary}"
        )
    binary = _validate_release_binary_path(binary, target, Path(repo_root))

    policy = load_string_policy(fixture)
    output = strings_output if strings_output is not None else _read_strings_command(binary)
    return scan_strings_text(output, policy)


def _read_strings_input(path: Path, string_fixture_path: Path | None = None) -> str:
    """读取 CLI 的固定 strings 文本输入，但不允许读取版本化 policy fixture。"""
    strings_input = Path(path)
    fixture = Path(string_fixture_path) if string_fixture_path is not None else STRING_POLICY_FIXTURE
    if _same_path(strings_input, fixture):
        raise ClosureGateError(
            f"strings policy fixture cannot be used as strings input: {strings_input}"
        )
    if not strings_input.is_file():
        raise ClosureGateError(f"strings input is missing or not a file: {strings_input}")
    try:
        return strings_input.read_text(encoding="utf-8")
    except OSError as error:
        raise ClosureGateError(f"cannot read strings input {strings_input}: {error}") from error


def _strip_tree_prefix(line: str) -> str:
    """只移除 Cargo 生成的完整 UTF-8/ASCII 分支前缀，伪造前缀原样保留以便拒绝。"""
    for prefix_re in (_UTF8_TREE_PREFIX_RE, _ASCII_TREE_PREFIX_RE):
        prefix_match = prefix_re.match(line)
        if prefix_match:
            return line[prefix_match.end() :].strip()
    for continuation_re in (_UTF8_TREE_CONTINUATION_RE, _ASCII_TREE_CONTINUATION_RE):
        continuation_match = continuation_re.match(line)
        if continuation_match:
            continuation = line[continuation_match.end() :].strip()
            # Cargo 只会给依赖分组标题保留无 branch 的 continuation 前缀。
            if _TREE_SECTION_RE.fullmatch(continuation):
                return continuation
            return line

    stripped = line.strip()
    if line == stripped and (not stripped or stripped[0] not in _TREE_PREFIX_LEAD_CHARS):
        return stripped
    # 简体中文注释：根节点不能带缩进，未知的树字符不能被当作可选前缀吞掉。
    return line


def _remove_tree_dedupe_marker(line: str) -> str:
    """移除 Cargo tree 在 package/feature 节点后追加的 `(*)` 标记。"""
    if line.endswith(" (*)"):
        return line[:-4].rstrip()
    return line


def _is_feature_list(value: str) -> bool:
    """校验 `{f}` 展开的逗号分隔 feature 列表。"""
    value = value.strip()
    if not value:
        return True
    return all(_FEATURE_TOKEN_RE.fullmatch(item) for item in value.split(","))


def _is_known_source_note(note: str) -> bool:
    """识别 Cargo 的 path/source 注记，拒绝任意括号文本。"""
    if len(note) < 3 or not note.startswith("(") or not note.endswith(")"):
        return False
    value = note[1:-1]
    if not value or "\r" in value or "\n" in value:
        return False
    if value.startswith(("/", "//", "\\\\")):
        return True
    if re.fullmatch(r"[A-Za-z]:[\\/].+", value):
        return True
    source_prefixes = (
        "path+file://",
        "file://",
        "git+file://",
        "git+http://",
        "git+https://",
        "git+ssh://",
        "http://",
        "https://",
        "registry+http://",
        "registry+https://",
        "sparse+http://",
        "sparse+https://",
    )
    return value.startswith(source_prefixes) and len(value) > value.find("://") + 3


def _is_known_package_tail(tail: str) -> bool:
    """按 Cargo 实际顺序校验 source、proc-macro、feature list 和单个末尾 marker。"""
    if tail and not tail[0].isspace():
        return False
    remainder = tail.strip()
    if not remainder:
        return True

    # `(*)` 只能出现一次且必须是整个 tail 的最后一个标记。
    if remainder == "(*)":
        remainder = ""
    elif remainder.endswith(" (*)"):
        remainder = remainder[:-4].rstrip()
    if "(*)" in remainder:
        return False

    source_seen = False
    proc_macro_seen = False
    while remainder.startswith("("):
        closing = remainder.find(")")
        if closing <= 1:
            return False
        note = remainder[: closing + 1]
        remainder = remainder[closing + 1 :]
        if remainder and not remainder[0].isspace():
            return False
        remainder = remainder.lstrip()
        if note == "(proc-macro)":
            if proc_macro_seen:
                return False
            proc_macro_seen = True
        elif _is_known_source_note(note):
            # Cargo 的 `{p}` source 注记位于 proc-macro 注记之前，且最多一个。
            if source_seen or proc_macro_seen:
                return False
            source_seen = True
        else:
            return False

    if not remainder:
        return True
    return _is_feature_list(remainder)


def _parse_strict_package_line(line: str) -> str | None:
    """严格解析生产 tree 的 package 行，未知尾部一律拒绝。"""
    match = _STRICT_PACKAGE_RE.fullmatch(line)
    if not match or not _is_known_package_tail(match.group("tail")):
        return None
    return f"{match.group('name')} {match.group('version')}"


def _parse_strict_feature_line(line: str) -> tuple[str, str] | None:
    """严格解析 package feature 节点，并只允许 Cargo 的 command-line/去重注记。"""
    normalized = _remove_tree_dedupe_marker(line)
    match = _STRICT_FEATURE_RE.fullmatch(normalized)
    if not match:
        return None
    raw_tail = match.group("tail")
    if raw_tail and not raw_tail[0].isspace():
        return None
    tail = raw_tail.strip()
    if tail and tail != "(command-line)":
        return None
    feature = match.group("feature")
    if _FEATURE_TOKEN_RE.fullmatch(feature) is None:
        return None
    return match.group("name"), feature


def _parse_production_tree(tree: str, *, allow_features: bool, description: str) -> list[str]:
    """严格解析生产 Cargo tree，确保根节点前后没有被静默忽略的非空行。"""
    packages: list[str] = []
    root_seen = False
    for line_number, raw_line in enumerate(tree.splitlines(), start=1):
        if not raw_line.strip():
            continue
        # Cargo 根节点没有树前缀；先校验原始首行，避免伪造 branch 被剥成合法 root。
        if not root_seen:
            root_package = _parse_strict_package_line(raw_line)
            if root_package is None:
                raise ClosureGateError(
                    f"sidecar {description} dependency tree root must be an unprefixed "
                    f"package line; malformed line {line_number}: {raw_line!r}"
                )
            root_seen = True
            packages.append(root_package)
            continue

        line = _strip_tree_prefix(raw_line)
        if not line:
            raise ClosureGateError(
                f"sidecar {description} dependency tree has malformed line {line_number}: "
                f"{raw_line!r}"
            )

        normalized = _remove_tree_dedupe_marker(line)
        if not normalized:
            raise ClosureGateError(
                f"sidecar {description} dependency tree has malformed line {line_number}: "
                f"{raw_line!r}"
            )

        # 解析器必须看到完整 package 行，才能让 package tail 校验重复/错序 marker。
        package = _parse_strict_package_line(line)
        if package is not None:
            root_seen = True
            packages.append(package)
            continue

        if _TREE_SECTION_RE.fullmatch(line):
            if not root_seen:
                raise ClosureGateError(
                    f"sidecar {description} dependency tree has content before its root "
                    f"on line {line_number}"
                )
            continue

        if allow_features and _parse_strict_feature_line(line) is not None:
            if not root_seen:
                raise ClosureGateError(
                    f"sidecar {description} dependency tree has content before its root "
                    f"on line {line_number}"
                )
            continue

        raise ClosureGateError(
            f"sidecar {description} dependency tree has malformed line {line_number}: "
            f"{raw_line!r}"
        )
    return packages


def _package_parts(package: str) -> tuple[str, str]:
    """简体中文注释：把公开的 `name version` 解析成可比较的两个字段。"""
    name, separator, version = package.partition(" ")
    if not separator or not name or not version:
        raise ValueError(f"invalid parsed package: {package!r}")
    return name, version


def parse_tree_packages(tree: str) -> list[str]:
    """解析 cargo tree 每行的 `{p}`，返回稳定的 `name version` 列表。"""
    packages: list[str] = []
    for raw_line in tree.splitlines():
        line = _strip_tree_prefix(raw_line)
        match = _PACKAGE_RE.match(line)
        if match:
            packages.append(f"{match.group('name')} {match.group('version')}")
    return packages


def _is_forbidden_package(name: str) -> bool:
    """简体中文注释：同时覆盖固定 denylist 和 opentelemetry* 前缀规则。"""
    return name in FORBIDDEN_PACKAGES or any(
        name.startswith(prefix) for prefix in FORBIDDEN_PACKAGE_PREFIXES
    )


def check_sidecar_tree(tree: str) -> list[str]:
    """返回 sidecar tree 的 denylist 命中和多版本 package 命中。

    非 sidecar 根节点的 tree 被视为 unrelated workspace member，不参与本门禁。
    """
    packages = parse_tree_packages(tree)
    if not packages:
        return []

    root_name, _ = _package_parts(packages[0])
    scan_denylist = root_name == SIDECAR_PACKAGE

    hits: list[str] = []
    versions_by_name: dict[str, set[str]] = {}
    for package in packages:
        name, version = _package_parts(package)
        versions_by_name.setdefault(name, set()).add(version)
        if scan_denylist and _is_forbidden_package(name) and name not in hits:
            hits.append(name)

    if not scan_denylist:
        return []

    # 简体中文注释：只报告 sidecar 自身闭包的重复版本，避免污染 unrelated member 的结果。
    for name in sorted(versions_by_name):
        if len(versions_by_name[name]) > 1:
            marker = f"{name}@duplicate"
            if marker not in hits:
                hits.append(marker)
    return hits


def _require_main_tree(tree: str, package_name: str) -> list[str]:
    """生产门禁要求 normal/build tree 严格可解析且根 package 为 sidecar。"""
    if package_name != SIDECAR_PACKAGE:
        raise ClosureGateError(
            f"sidecar normal/build dependency tree expects package {SIDECAR_PACKAGE!r}"
        )
    packages = _parse_production_tree(
        tree,
        allow_features=False,
        description="normal/build",
    )
    if not packages:
        raise ClosureGateError(
            "sidecar normal/build dependency tree is empty or has no parseable package"
        )
    root_name, _ = _package_parts(packages[0])
    if root_name != SIDECAR_PACKAGE:
        raise ClosureGateError(
            f"sidecar normal/build dependency tree root is {root_name!r}, "
            f"expected {SIDECAR_PACKAGE!r}"
        )
    return packages


def _require_feature_tree(tree: str) -> None:
    """生产门禁要求 feature tree 有 sidecar 根和至少一个合法 feature 节点。"""
    packages = _parse_production_tree(
        tree,
        allow_features=True,
        description="feature",
    )
    if not packages:
        raise ClosureGateError("sidecar feature dependency tree is empty or has no parseable package")
    root_name, _ = _package_parts(packages[0])
    if root_name != SIDECAR_PACKAGE:
        raise ClosureGateError(
            f"sidecar feature dependency tree root is {root_name!r}, "
            f"expected {SIDECAR_PACKAGE!r}"
        )
    feature_nodes = _parse_feature_tree(tree)
    if not feature_nodes:
        raise ClosureGateError(
            "sidecar feature dependency tree has no valid feature node; "
            "a package-only tree is not sufficient"
        )


def _parse_feature_tree(tree: str) -> list[str]:
    """提取 `cargo tree -e features` 中的 package/feature 对。"""
    features: set[str] = set()
    for raw_line in tree.splitlines():
        line = _strip_tree_prefix(raw_line)
        parsed = _parse_strict_feature_line(line)
        if parsed is not None:
            features.add(f"{parsed[0]}/{parsed[1]}")
    return sorted(features)


def _run_cargo(repo_root: Path, args: Sequence[str]) -> subprocess.CompletedProcess[str]:
    """简体中文注释：集中执行 cargo，所有闭包查询强制使用 locked 和捕获输出。"""
    try:
        return subprocess.run(
            ["cargo", *args],
            cwd=repo_root,
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as error:
        raise ClosureGateError(
            f"cannot launch cargo: {error}; install Cargo and retry the closure gate"
        ) from error


def _require_cargo_output(
    repo_root: Path, args: Sequence[str], description: str
) -> str:
    """简体中文注释：主 tree/metadata 查询失败时立即给出可执行的诊断。"""
    result = _run_cargo(repo_root, args)
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise ClosureGateError(
            f"{description} failed with exit code {result.returncode}: {detail}; "
            "run the same command from ../effilab-agent and fix Cargo before retrying"
        )
    return result.stdout


def _parse_edges(raw_edges: str) -> tuple[str, list[str]]:
    """校验生产闭包必须同时包含 normal/build 边，并保留 CLI 顺序。"""
    edges = [item.strip() for item in raw_edges.split(",")]
    if not edges or any(not item or item not in ALLOWED_EDGE_KINDS for item in edges):
        allowed = ", ".join(sorted(ALLOWED_EDGE_KINDS))
        raise ClosureGateError(
            f"unsupported --edges {raw_edges!r}; use only {allowed} for the release closure"
        )
    if len(set(edges)) != len(edges):
        raise ClosureGateError(f"duplicate edge kind in --edges {raw_edges!r}")
    if set(edges) != ALLOWED_EDGE_KINDS:
        missing = ", ".join(sorted(ALLOWED_EDGE_KINDS - set(edges)))
        raise ClosureGateError(
            f"--edges must include both normal and build; missing {missing} in {raw_edges!r}"
        )
    return ",".join(edges), edges


def _load_metadata(metadata_text: str) -> dict[str, Any]:
    """解析 cargo metadata JSON，并把损坏输入转换成可读错误。"""
    try:
        metadata = json.loads(metadata_text)
    except json.JSONDecodeError as error:
        raise ClosureGateError(f"cargo metadata returned invalid JSON: {error}") from error
    if not isinstance(metadata, dict):
        raise ClosureGateError("cargo metadata root must be a JSON object")
    return metadata


def _read_workspace_member_baseline(snapshot_path: Path) -> tuple[str, ...]:
    """读取版本控制中的 workspace member manifest 路径基线。"""
    try:
        lines = snapshot_path.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise ClosureGateError(
            f"cannot read workspace member baseline {snapshot_path}: {error}; "
            "restore the tracked snapshot before retrying the closure gate"
        ) from error

    entries: list[str] = []
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        relative = Path(line)
        if relative.is_absolute() or ".." in relative.parts:
            raise ClosureGateError(
                f"workspace member baseline contains an invalid path {line!r}; "
                "use repository-relative Cargo.toml paths"
            )
        normalized = relative.as_posix()
        if normalized != line:
            raise ClosureGateError(
                f"workspace member baseline path is not normalized: {line!r}; "
                "rewrite it with forward-slash relative paths"
            )
        entries.append(normalized)

    if not entries or len(set(entries)) != len(entries):
        raise ClosureGateError(
            f"workspace member baseline {snapshot_path} must contain unique Cargo.toml paths"
        )
    return tuple(sorted(entries))


def _workspace_snapshot(
    metadata: dict[str, Any],
    repo_root: Path | None = None,
    snapshot_path: Path | None = None,
) -> tuple[str, ...]:
    """把 metadata member 路径与版本控制基线逐项比较，禁止同次 metadata 自证。"""
    packages = metadata.get("packages")
    members = metadata.get("workspace_members")
    if not isinstance(packages, list) or not isinstance(members, list) or not members:
        raise ClosureGateError(
            "cargo metadata has no workspace member snapshot; use a valid workspace checkout"
        )

    package_by_id = {
        package.get("id"): package
        for package in packages
        if isinstance(package, dict) and isinstance(package.get("id"), str)
    }
    member_ids = {member for member in members if isinstance(member, str)}
    if len(member_ids) != len(members):
        raise ClosureGateError("cargo metadata workspace_members contains a non-string id")
    missing = sorted(member_ids - set(package_by_id))
    if missing:
        raise ClosureGateError(
            "workspace member snapshot references packages absent from metadata: "
            + ", ".join(missing)
        )

    metadata_root = metadata.get("workspace_root")
    if not isinstance(metadata_root, str) or not metadata_root:
        raise ClosureGateError(
            "cargo metadata has no workspace_root; use a complete metadata response"
        )
    workspace_root = Path(metadata_root).resolve()
    if repo_root is not None and workspace_root != repo_root.resolve():
        raise ClosureGateError(
            f"cargo metadata workspace_root is {workspace_root}, expected {repo_root.resolve()}; "
            "run the closure gate from the sibling workspace root"
        )

    member_paths: list[str] = []
    for member_id in members:
        package = package_by_id[member_id]
        manifest_path = package.get("manifest_path")
        if not isinstance(manifest_path, str) or not manifest_path:
            raise ClosureGateError(
                f"workspace member {member_id!r} has no manifest_path in cargo metadata"
            )
        try:
            relative = Path(manifest_path).resolve().relative_to(workspace_root)
        except ValueError as error:
            raise ClosureGateError(
                f"workspace member manifest escapes workspace root: {manifest_path}; "
                "repair the workspace metadata before retrying"
            ) from error
        member_paths.append(relative.as_posix())

    actual = tuple(sorted(member_paths))
    baseline = _read_workspace_member_baseline(snapshot_path or WORKSPACE_MEMBER_SNAPSHOT)
    if actual != baseline:
        missing_from_metadata = sorted(set(baseline) - set(actual))
        unexpected_in_metadata = sorted(set(actual) - set(baseline))
        details: list[str] = []
        if missing_from_metadata:
            details.append("missing=" + ",".join(missing_from_metadata))
        if unexpected_in_metadata:
            details.append("unexpected=" + ",".join(unexpected_in_metadata))
        raise ClosureGateError(
            "workspace member baseline mismatch ("
            + "; ".join(details)
            + "); review the tracked sidecar_workspace_members.txt baseline and rerun "
            "fork-sync-apply.sh --check before changing it"
        )
    return actual


def _find_package(metadata: dict[str, Any], package_name: str) -> dict[str, Any]:
    """找到 workspace 中指定 package 的唯一 metadata 记录。"""
    packages = metadata.get("packages")
    members = set(metadata.get("workspace_members", []))
    matches = [
        package
        for package in packages or []
        if isinstance(package, dict)
        and package.get("name") == package_name
        and package.get("id") in members
    ]
    if len(matches) != 1:
        raise ClosureGateError(
            f"expected exactly one workspace package named {package_name!r}, found {len(matches)}"
        )
    return matches[0]


def _stable_package_id(package: dict[str, Any], repo_root: Path) -> str | None:
    """把 Cargo 的本机 path package id 转成不含机器绝对路径的报告值。"""
    package_id = package.get("id")
    if not isinstance(package_id, str):
        return None

    manifest_path = package.get("manifest_path")
    if isinstance(manifest_path, str) and package_id.startswith("path+file://"):
        try:
            relative_manifest = Path(manifest_path).resolve().relative_to(repo_root.resolve())
        except (OSError, ValueError):
            relative_manifest = None
        if relative_manifest is not None:
            prefix, separator, suffix = package_id.partition("#")
            if separator and prefix.startswith("path+file://"):
                return f"path+file://./{relative_manifest.parent.as_posix()}#{suffix}"

    # 无法安全还原本机路径时，使用 package name/version 作为稳定回退值。
    name = package.get("name")
    version = package.get("version")
    if isinstance(name, str) and isinstance(version, str):
        return f"{name}@{version}"
    if "file://" in package_id:
        return None
    return package_id if not Path(package_id).is_absolute() else None


def _stable_package_source(package: dict[str, Any]) -> Any:
    """避免把 metadata 中可能存在的本机 file source 写进 JSON。"""
    source = package.get("source")
    if isinstance(source, str) and ("file://" in source or Path(source).is_absolute()):
        return None
    return source


def _reverse_dependency_candidates(tree: str) -> list[str]:
    """返回 tree 中需要用 cargo tree -i 复核的 denylist package 名。"""
    names = {
        _package_parts(package)[0]
        for package in parse_tree_packages(tree)
        if _is_forbidden_package(_package_parts(package)[0])
    }
    names.update(FORBIDDEN_PACKAGES)
    return sorted(names)


def _is_missing_reverse_candidate(
    result: subprocess.CompletedProcess[str], candidate: str
) -> bool:
    """只识别 stderr 中与当前候选完全一致的单条无路径诊断。"""
    if result.returncode != 101:
        return False
    # 简体中文注释：stdout 必须没有实际内容，避免把反向路径和 missing 诊断拼成假跳过。
    if (result.stdout or "").strip():
        return False
    diagnostic = result.stderr or ""
    diagnostic = diagnostic.rstrip("\r\n")
    if not diagnostic:
        return False
    # 简体中文注释：仅允许固定锁等待行带 ASCII 空格或制表符，且只能位于 missing 诊断之前。
    lines = diagnostic.splitlines()
    while len(lines) > 1 and lines[0].strip(" \t") == _CARGO_PACKAGE_CACHE_LOCK_WAIT:
        lines.pop(0)
    if len(lines) != 1:
        return False
    match = _MISSING_PACKAGE_RE.fullmatch(lines[0])
    return match is not None and match.group("package_id") == candidate


def _reverse_dependency_hits(
    repo_root: Path,
    package_name: str,
    target: str,
    edge_kind: str,
    candidates: Iterable[str],
) -> set[str]:
    """逐项执行反向 tree，仅忽略 Cargo 明确报告的无路径候选。"""
    hits: set[str] = set()
    for candidate in candidates:
        result = _run_cargo(
            repo_root,
            [
                "tree",
                "-p",
                package_name,
                "--locked",
                "--target",
                target,
                "-e",
                edge_kind,
                "-i",
                candidate,
            ],
        )
        if result.returncode == 0:
            stdout = result.stdout or ""
            if not stdout.strip():
                raise ClosureGateError(
                    f"reverse dependency tree for candidate {candidate!r} returned empty output "
                    "with exit code 0"
                )
            try:
                reverse_packages = _parse_production_tree(
                    stdout,
                    allow_features=False,
                    description=f"reverse dependency for candidate {candidate!r}",
                )
            except ClosureGateError as error:
                raise ClosureGateError(
                    f"reverse dependency tree for candidate {candidate!r} is malformed: {error}"
                ) from error
            package_names = {_package_parts(package)[0] for package in reverse_packages}
            if candidate not in package_names:
                raise ClosureGateError(
                    f"reverse dependency tree for candidate {candidate!r} is malformed: "
                    "tree does not contain the queried candidate"
                )
            hits.add(candidate)
            continue
        if _is_missing_reverse_candidate(result, candidate):
            # 简体中文注释：候选不在解析结果中是正常的“无路径”，不阻断主门禁。
            continue
        detail = (result.stderr or result.stdout or "").strip()
        raise ClosureGateError(
            f"reverse dependency tree for candidate {candidate!r} failed with exit code "
            f"{result.returncode}: {detail or 'no diagnostic'}"
        )
    return hits


def _lockfile_sha256(repo_root: Path) -> str:
    """计算 sibling 根 Cargo.lock 的 SHA-256，绑定本次闭包解析结果。"""
    lockfile = repo_root / "Cargo.lock"
    try:
        content = lockfile.read_bytes()
    except OSError as error:
        raise ClosureGateError(
            f"cannot read {lockfile}: {error}; restore Cargo.lock and retry with --locked"
        ) from error
    return hashlib.sha256(content).hexdigest()


def _default_release_binary(repo_root: Path, target: str, profile: str) -> Path:
    """构造供调用方显式传入的唯一 Cargo release binary 路径。"""
    if profile != "release":
        raise ClosureGateError(
            f"unsupported release profile {profile!r}; this gate only certifies --profile release"
        )
    return _expected_release_binary_path(repo_root, target)


def build_report(
    package_name: str,
    target: str,
    profile: str,
    raw_edges: str,
    repo_root: Path | None = None,
    binary_path: Path | None = None,
    strings_output: str | None = None,
    string_fixture_path: Path | None = None,
    *,
    scan_mode: str = "closure-only",
    test_only_strings: bool = False,
) -> dict[str, Any]:
    """运行锁定 Cargo 查询，并明确区分 closure-only 与 release certification。"""
    if scan_mode not in SCAN_MODES:
        allowed = ", ".join(SCAN_MODES)
        raise ClosureGateError(f"unsupported scan mode {scan_mode!r}; use one of: {allowed}")
    if strings_output is not None and binary_path is None:
        raise ClosureGateError("test strings input requires an explicitly provided --binary")
    if strings_output is not None and not test_only_strings:
        raise ClosureGateError(
            "injected strings output is test-only; release certification must invoke the strings tool"
        )
    if test_only_strings and strings_output is None:
        raise ClosureGateError("test-only strings mode requires an injected strings output")
    if scan_mode == "release-certification" and binary_path is None:
        raise ClosureGateError(
            "release-certification requires an explicitly provided --binary; "
            "closure-only does not scan a release binary"
        )
    if scan_mode == "closure-only" and binary_path is not None:
        raise ClosureGateError(
            "closure-only cannot accept --binary; use --mode release-certification "
            "or --require-binary"
        )
    if package_name != SIDECAR_PACKAGE:
        raise ClosureGateError(
            f"this gate only accepts --package {SIDECAR_PACKAGE}; unrelated workspace members "
            "must be checked by their own gates"
        )
    if target not in PUBLISHED_TARGETS or target == "all":
        allowed = ", ".join(sorted(PUBLISHED_TARGETS))
        raise ClosureGateError(f"unsupported release target {target!r}; use one of: {allowed}")
    if profile != "release":
        raise ClosureGateError(
            f"unsupported release profile {profile!r}; this gate only certifies --profile release"
        )

    root = repo_root or _repo_root()
    edge_kind, _ = _parse_edges(raw_edges)
    policy_path = Path(string_fixture_path) if string_fixture_path is not None else STRING_POLICY_FIXTURE
    binary_scan_status = "not-requested"
    binary_scanned = False
    if binary_path is None:
        # closure-only 只证明 Cargo 闭包，明确记录没有扫描发布 binary。
        binary_string_hits: list[str] = []
    else:
        binary_string_hits = scan_release_binary(
            Path(binary_path),
            strings_output=strings_output,
            string_fixture_path=policy_path,
            target=target,
            repo_root=root,
        )
        if strings_output is None:
            binary_scan_status = "scanned"
            binary_scanned = True
        else:
            # 该分支只供 Python 测试注入，不能被报告为真实发布扫描。
            binary_scan_status = "test-input-only"
    tree_args = [
        "tree",
        "-p",
        package_name,
        "--locked",
        "-e",
        edge_kind,
        "--target",
        target,
        "--format",
        "{p} {f}",
        "--prefix",
        "none",
    ]
    tree = _require_cargo_output(root, tree_args, "sidecar normal/build dependency tree")
    _require_main_tree(tree, package_name)
    feature_tree = _require_cargo_output(
        root,
        ["tree", "-p", package_name, "--locked", "-e", "features", "--target", target],
        "sidecar feature dependency tree",
    )
    _require_feature_tree(feature_tree)
    metadata_text = _require_cargo_output(
        root,
        ["metadata", "--locked", "--format-version", "1"],
        "cargo metadata",
    )
    metadata = _load_metadata(metadata_text)
    _workspace_snapshot(metadata, repo_root=root)
    package = _find_package(metadata, package_name)

    tree_hits = check_sidecar_tree(tree)
    reverse_hits = _reverse_dependency_hits(
        root,
        package_name,
        target,
        edge_kind,
        _reverse_dependency_candidates(tree),
    )
    package_hits = {
        f"{PACKAGE_HIT_PREFIX}{hit}" for hit in set(tree_hits) | reverse_hits
    }
    binary_hits = {f"{BINARY_HIT_PREFIX}{hit}" for hit in binary_string_hits}
    # 保持原有 denylist_hits 列表字段，同时用稳定前缀区分来源。
    denylist_hits = sorted(package_hits | binary_hits)

    return {
        # 简体中文注释：schema_version 与 gate_version 同步，兼容报告消费者的两种命名。
        "schema_version": GATE_VERSION,
        "gate_version": GATE_VERSION,
        "package_id": _stable_package_id(package, root),
        "enabled_features": _parse_feature_tree(feature_tree),
        "edge_kind": edge_kind,
        "source": _stable_package_source(package),
        "target": target,
        "profile": profile,
        "scan_mode": scan_mode,
        "binary_scanned": binary_scanned,
        "binary_scan_status": binary_scan_status,
        "lockfile_sha256": _lockfile_sha256(root),
        "denylist_hits": denylist_hits,
    }


def _write_report(path: Path, report: dict[str, Any]) -> None:
    """简体中文注释：原子替换 JSON，避免 CI 读取到半截闭包报告。"""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.tmp")
        temporary.write_text(
            json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)
    except OSError as error:
        raise ClosureGateError(
            f"cannot write closure report {path}: {error}; choose a writable --out path"
        ) from error


def _argument_parser() -> argparse.ArgumentParser:
    """构造 CLI 参数，帮助文本明确发布 target、profile 和输出合同。"""
    parser = argparse.ArgumentParser(
        description=(
            "Check the locked normal/build closure of efflab-agent-sidecar; "
            "closure-only does not scan a release binary."
        )
    )
    parser.add_argument("--package", required=True, help="workspace package to certify")
    parser.add_argument(
        "--target",
        required=True,
        choices=sorted(PUBLISHED_TARGETS),
        help="release target triple; --target all is intentionally unsupported",
    )
    parser.add_argument(
        "--profile",
        required=True,
        choices=("release",),
        help="Cargo profile recorded by this release gate",
    )
    parser.add_argument(
        "--mode",
        choices=SCAN_MODES,
        default="closure-only",
        help="closure-only (default) or release-certification with a real --binary",
    )
    parser.add_argument(
        "--require-binary",
        action="store_true",
        help="alias for --mode release-certification; requires an explicit --binary",
    )
    parser.add_argument(
        "--edges",
        required=True,
        help="comma-separated production edge kinds, normally normal,build",
    )
    parser.add_argument("--out", required=True, type=Path, help="JSON report output path")
    parser.add_argument(
        "--binary",
        type=Path,
        help="release binary to scan; required by release-certification",
    )
    parser.add_argument(
        "--strings-input",
        type=Path,
        help=argparse.SUPPRESS,
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """执行门禁：0 表示无命中，1 表示闭包违规，2 表示输入/工具错误。"""
    args = _argument_parser().parse_args(argv)
    try:
        if args.strings_input is not None:
            raise ClosureGateError(
                "--strings-input is Python test API only; release certification must scan the binary"
            )
        scan_mode = "release-certification" if args.require_binary else args.mode
        report = build_report(
            package_name=args.package,
            target=args.target,
            profile=args.profile,
            raw_edges=args.edges,
            binary_path=args.binary,
            scan_mode=scan_mode,
        )
        _write_report(args.out, report)
    except ClosureGateError as error:
        print(f"sidecar closure gate error: {error}", file=sys.stderr)
        return 2

    hits = report["denylist_hits"]
    if hits:
        print(
            "sidecar closure denied: " + ", ".join(hits) + f"; report={args.out}",
            file=sys.stderr,
        )
        return 1
    print(f"sidecar closure accepted; report={args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
