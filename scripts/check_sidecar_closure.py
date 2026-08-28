#!/usr/bin/env python3
"""检查 efflab-agent-sidecar 的发布 normal/build 依赖闭包。

该门禁只读取 Cargo 的锁定解析结果，不改变 workspace，也不读取 debug 产物。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable, Sequence


# 简体中文注释：门禁版本进入 JSON，后续字段变化必须显式升级该版本。
GATE_VERSION = 1
SIDECAR_CLOSURE_GATE_VERSION = GATE_VERSION
SIDECAR_PACKAGE = "efflab-agent-sidecar"
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
_FEATURE_RE = re.compile(
    r'^(?P<name>[A-Za-z0-9][A-Za-z0-9_-]*)\s+feature\s+"(?P<feature>[^"]+)"'
)


class ClosureGateError(RuntimeError):
    """表示 Cargo 输入或门禁参数不可用，而不是依赖闭包命中。"""


def _repo_root() -> Path:
    """简体中文注释：从脚本路径解析 sibling 根，避免依赖调用者当前目录。"""
    return Path(__file__).resolve().parents[1]


def _strip_tree_prefix(line: str) -> str:
    """简体中文注释：移除 cargo tree 的 Unicode 分支符和缩进，保留 package 文本。"""
    return re.sub(r"^[\s│├└─┬]+", "", line).strip()


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


def _parse_feature_tree(tree: str) -> list[str]:
    """提取 `cargo tree -e features` 中的 package/feature 对。"""
    features: set[str] = set()
    for raw_line in tree.splitlines():
        line = _strip_tree_prefix(raw_line)
        match = _FEATURE_RE.match(line)
        if match:
            features.add(f"{match.group('name')}/{match.group('feature')}")
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
    edges = [item.strip() for item in raw_edges.split(",") if item.strip()]
    if not edges or any(item not in ALLOWED_EDGE_KINDS for item in edges):
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


def _reverse_dependency_candidates(tree: str) -> list[str]:
    """返回 tree 中需要用 cargo tree -i 复核的 denylist package 名。"""
    names = {
        _package_parts(package)[0]
        for package in parse_tree_packages(tree)
        if _is_forbidden_package(_package_parts(package)[0])
    }
    names.update(FORBIDDEN_PACKAGES)
    return sorted(names)


def _reverse_dependency_hits(
    repo_root: Path, package_name: str, target: str, candidates: Iterable[str]
) -> set[str]:
    """逐项执行反向 tree，只有 cargo 返回路径时才记录命中。"""
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
                "-i",
                candidate,
            ],
        )
        # 简体中文注释：未进入闭包的 -i 查询会返回非零；那是正常的“无路径”，不阻断主门禁。
        if result.returncode == 0 and result.stdout.strip():
            hits.add(candidate)
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


def build_report(
    package_name: str,
    target: str,
    profile: str,
    raw_edges: str,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    """运行所有锁定 Cargo 查询并构造 schema_version 1 报告。"""
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
    feature_tree = _require_cargo_output(
        root,
        ["tree", "-p", package_name, "--locked", "-e", "features", "--target", target],
        "sidecar feature dependency tree",
    )
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
        _reverse_dependency_candidates(tree),
    )
    denylist_hits = sorted(set(tree_hits) | reverse_hits)

    return {
        # 简体中文注释：schema_version 与 gate_version 同步，兼容报告消费者的两种命名。
        "schema_version": GATE_VERSION,
        "gate_version": GATE_VERSION,
        "package_id": package.get("id"),
        "enabled_features": _parse_feature_tree(feature_tree),
        "edge_kind": edge_kind,
        "source": package.get("source"),
        "target": target,
        "profile": profile,
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
        description="Check the locked normal/build closure of efflab-agent-sidecar."
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
        "--edges",
        required=True,
        help="comma-separated production edge kinds, normally normal,build",
    )
    parser.add_argument("--out", required=True, type=Path, help="JSON report output path")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """执行门禁：0 表示无命中，1 表示闭包违规，2 表示输入/工具错误。"""
    args = _argument_parser().parse_args(argv)
    try:
        report = build_report(
            package_name=args.package,
            target=args.target,
            profile=args.profile,
            raw_edges=args.edges,
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
