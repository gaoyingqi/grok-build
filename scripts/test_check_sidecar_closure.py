#!/usr/bin/env python3
"""闭包门禁的可执行单元测试。"""

from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from check_sidecar_closure import (
    ClosureGateError,
    GATE_VERSION,
    SIDECAR_CLOSURE_GATE_VERSION,
    _FEATURE_TOKEN_RE,
    _argument_parser,
    _is_known_package_tail,
    _is_missing_reverse_candidate,
    _parse_edges,
    _parse_strict_feature_line,
    _release_binary_name,
    _require_feature_tree,
    _require_main_tree,
    _reverse_dependency_hits,
    _workspace_snapshot,
    build_report,
    check_sidecar_tree,
    load_string_policy,
    main,
    parse_tree_packages,
    scan_release_binary,
    scan_strings_text,
)


class SidecarClosureTest(unittest.TestCase):
    """验证 sidecar 闭包解析的最小合同。"""

    @staticmethod
    def _release_binary_path(root: Path, target: str, name: str | None = None) -> Path:
        """按显式 Cargo target 构造测试 binary 路径。"""
        return (
            root
            / "target"
            / target
            / "release"
            / (name or _release_binary_name(target))
        )

    @classmethod
    def _write_release_binary(cls, root: Path, target: str, name: str | None = None) -> Path:
        """在临时 workspace root 的 Cargo release 目录创建测试 binary。"""
        binary = cls._release_binary_path(root, target, name)
        binary.parent.mkdir(parents=True, exist_ok=True)
        binary.write_bytes(b"release binary")
        if not target.endswith("-windows-msvc"):
            binary.chmod(0o755)
        return binary

    def test_rejects_forbidden_package_in_sidecar_tree(self) -> None:
        tree = "efflab-agent-sidecar v0.1.0\n└── xai-grok-shell v0.1.0"
        self.assertEqual(check_sidecar_tree(tree), ["xai-grok-shell"])

    def test_does_not_scan_unrelated_workspace_member(self) -> None:
        tree = "pager v0.1.0\n└── xai-grok-shell v0.1.0"
        self.assertEqual(check_sidecar_tree(tree), [])

    def test_duplicate_reqwest_versions_fail(self) -> None:
        tree = "efflab-agent-sidecar v0.1.0\n├── reqwest v0.12.24\n└── reqwest v0.13.4"
        hits = check_sidecar_tree(tree)
        self.assertIn("reqwest@duplicate", hits)
        self.assertEqual(
            len([package for package in parse_tree_packages(tree) if package.split(" ")[0] == "reqwest"]),
            2,
        )

    def test_unrelated_duplicate_reqwest_versions_are_ignored(self) -> None:
        tree = "pager v0.1.0\n├── reqwest v0.12.24\n└── reqwest v0.13.4"
        self.assertEqual(check_sidecar_tree(tree), [])

    def test_feature_token_allows_leading_underscore_but_rejects_malformed(self) -> None:
        """固定 Cargo `{f}` feature token 的合法边界。"""
        for feature in ("__rustls", "__rustls-aws-lc-rs", "feature_1"):
            with self.subTest(feature=feature):
                self.assertIsNotNone(_FEATURE_TOKEN_RE.fullmatch(feature))

        for malformed in (
            "",
            "-rustls",
            "rustls,,json",
            "rustls=invalid",
            "a,b",
            "foo?",
            "foo/bar",
            "foo+bar",
            "foo.bar",
        ):
            with self.subTest(malformed=malformed):
                self.assertIsNone(_FEATURE_TOKEN_RE.fullmatch(malformed))

    def test_strict_feature_line_applies_cargo_feature_token_grammar(self) -> None:
        """feature 节点的引号内容必须经过与 `{f}` 相同的 token 校验。"""
        self.assertEqual(
            _parse_strict_feature_line('demo feature "__rustls-aws-lc-rs"'),
            ("demo", "__rustls-aws-lc-rs"),
        )
        for malformed in ("rustls=invalid", "a,b", "foo?"):
            with self.subTest(malformed=malformed):
                self.assertIsNone(_parse_strict_feature_line(f'demo feature "{malformed}"'))

    def test_package_tail_accepts_observed_cargo_combinations_only(self) -> None:
        """package tail 只接受 source/proc-macro、feature list 与单个末尾 marker。"""
        for tail in (
            " (/workspace/sidecar)",
            " (registry+https://github.com/rust-lang/crates.io-index)",
            " (git+https://github.com/example/demo?rev=abc)",
            " (proc-macro)",
            " (proc-macro) default",
            " default (*)",
            " (proc-macro) default (*)",
            " (/workspace/sidecar) (proc-macro) default (*)",
        ):
            with self.subTest(tail=tail):
                self.assertTrue(_is_known_package_tail(tail))

        for tail in (
            " (arbitrary-note)",
            " (proc-macro) (proc-macro)",
            " default (*) (*)",
            " (*) default",
            " (proc-macro) (/workspace/sidecar)",
        ):
            with self.subTest(tail=tail):
                self.assertFalse(_is_known_package_tail(tail))

    def test_production_tree_rejects_pseudo_prefix(self) -> None:
        for prefix in ("+", "-", "`", "\\"):
            with self.subTest(prefix=prefix):
                with self.assertRaisesRegex(ClosureGateError, "malformed"):
                    _require_main_tree(f"{prefix}efflab-agent-sidecar v0.1.0", "efflab-agent-sidecar")

    def test_production_tree_requires_unprefixed_utf8_root(self) -> None:
        for prefix in ("└── ", "├── ", "│   "):
            with self.subTest(prefix=prefix):
                with self.assertRaisesRegex(ClosureGateError, "root|malformed"):
                    _require_main_tree(
                        f"{prefix}efflab-agent-sidecar v0.1.0\n"
                        "└── dependency v1.0.0\n",
                        "efflab-agent-sidecar",
                    )

    def test_production_tree_requires_unprefixed_ascii_root(self) -> None:
        for prefix in ("|-- ", "`-- ", "\\-- ", "|   "):
            with self.subTest(prefix=prefix):
                with self.assertRaisesRegex(ClosureGateError, "root|malformed"):
                    _require_main_tree(
                        f"{prefix}efflab-agent-sidecar v0.1.0\n"
                        "|-- dependency v1.0.0\n",
                        "efflab-agent-sidecar",
                    )

    def test_production_tree_rejects_standalone_dedupe_marker(self) -> None:
        with self.assertRaisesRegex(ClosureGateError, "malformed"):
            _require_main_tree("efflab-agent-sidecar v0.1.0\n(*)", "efflab-agent-sidecar")

    def test_production_tree_rejects_repeated_dedupe_marker(self) -> None:
        with self.assertRaisesRegex(ClosureGateError, "malformed"):
            _require_main_tree(
                "efflab-agent-sidecar v0.1.0\n"
                "└── dependency v1.0.0 (*) (*)\n",
                "efflab-agent-sidecar",
            )

    def test_production_main_tree_rejects_malformed_before_root(self) -> None:
        tree = "not a cargo tree\nefflab-agent-sidecar v0.1.0"
        with self.assertRaisesRegex(ClosureGateError, "malformed|before its root"):
            _require_main_tree(tree, "efflab-agent-sidecar")

    def test_production_main_tree_rejects_malformed_after_root(self) -> None:
        tree = "efflab-agent-sidecar v0.1.0\nnot a cargo tree"
        with self.assertRaisesRegex(ClosureGateError, "malformed"):
            _require_main_tree(tree, "efflab-agent-sidecar")

    def test_production_trees_allow_cargo_structure_lines(self) -> None:
        _require_main_tree(
            "\nefflab-agent-sidecar v0.1.0\n"
            "├── dependency v1.0.0 (*)\n"
            "[build-dependencies]\n"
            "└── build-helper v1.0.0 (proc-macro) default,build\n",
            "efflab-agent-sidecar",
        )
        _require_feature_tree(
            "\nefflab-agent-sidecar v0.1.0 (path+file:///workspace/sidecar)\n"
            "├── efflab-agent-sidecar feature \"default\" (command-line)\n"
            "│   ├── dependency v1.0.0 (*)\n"
            "│   └── efflab-agent-sidecar feature \"extra\" (*)\n"
            "└── dependency v1.0.0 (*)\n",
        )

    def test_production_trees_accept_cargo_ascii_prefix_and_sections(self) -> None:
        _require_main_tree(
            "efflab-agent-sidecar v0.1.0\n"
            "|-- dependency v1.0.0 (*)\n"
            "[build-dependencies]\n"
            "`-- build-helper v1.0.0 (proc-macro) default,build\n",
            "efflab-agent-sidecar",
        )
        _require_feature_tree(
            "efflab-agent-sidecar v0.1.0 (/workspace/sidecar)\n"
            "|-- dependency feature \"default\"\n"
            "|   `-- dependency v1.0.0\n"
            "|       [build-dependencies]\n",
        )

    def test_production_tree_rejects_indented_fake_root(self) -> None:
        with self.assertRaisesRegex(ClosureGateError, "malformed"):
            _require_main_tree("    efflab-agent-sidecar v0.1.0", "efflab-agent-sidecar")

    def test_production_feature_tree_rejects_wrong_root(self) -> None:
        tree = "pager v0.1.0\n└── pager feature \"default\""
        with self.assertRaisesRegex(ClosureGateError, "feature.*root"):
            _require_feature_tree(tree)

    def test_production_feature_tree_requires_a_feature_node(self) -> None:
        with self.assertRaisesRegex(ClosureGateError, "feature.*node"):
            _require_feature_tree("efflab-agent-sidecar v0.1.0")

    def test_production_feature_tree_rejects_malformed_feature_line(self) -> None:
        tree = "efflab-agent-sidecar v0.1.0\n└── efflab-agent-sidecar feature default"
        with self.assertRaisesRegex(ClosureGateError, "malformed"):
            _require_feature_tree(tree)

    def test_edges_require_normal_and_build(self) -> None:
        with self.assertRaises(ClosureGateError):
            _parse_edges("normal")
        with self.assertRaises(ClosureGateError):
            _parse_edges("build")
        for malformed in ("normal,,build", ",normal,build", "normal,build,"):
            with self.subTest(malformed=malformed):
                with self.assertRaisesRegex(ClosureGateError, "unsupported --edges"):
                    _parse_edges(malformed)
        self.assertEqual(_parse_edges("normal,build"), ("normal,build", ["normal", "build"]))

    def test_reverse_dependency_accepts_normal_sidecar_path(self) -> None:
        result = subprocess.CompletedProcess(
            ["cargo"],
            0,
            stdout="efflab-agent-sidecar v0.1.0\n└── xai-grok-shell v0.1.0\n",
            stderr="",
        )
        with patch("check_sidecar_closure._run_cargo", return_value=result) as cargo_mock:
            self.assertEqual(
                _reverse_dependency_hits(
                    Path.cwd(),
                    "efflab-agent-sidecar",
                    "aarch64-apple-darwin",
                    "normal,build",
                    ["xai-grok-shell"],
                ),
                {"xai-grok-shell"},
            )
        args = cargo_mock.call_args.args[1]
        self.assertIn("--locked", args)
        self.assertIn("--target", args)
        edge_index = args.index("-e")
        self.assertEqual(args[edge_index + 1], "normal,build")
        self.assertLess(edge_index, args.index("-i"))

    def test_reverse_dependency_zero_exit_with_empty_stdout_fails_closed(self) -> None:
        for stdout in ("", "\n  \t"):
            with self.subTest(stdout=repr(stdout)):
                result = subprocess.CompletedProcess(
                    ["cargo"],
                    0,
                    stdout=stdout,
                    stderr="",
                )
                with patch("check_sidecar_closure._run_cargo", return_value=result):
                    with self.assertRaisesRegex(ClosureGateError, "reverse dependency.*empty"):
                        _reverse_dependency_hits(
                            Path.cwd(),
                            "efflab-agent-sidecar",
                            "aarch64-apple-darwin",
                            "normal,build",
                            ["xai-grok-shell"],
                        )

    def test_reverse_dependency_ignores_only_explicit_missing_candidate(self) -> None:
        result = subprocess.CompletedProcess(
            ["cargo"],
            101,
            stdout="",
            stderr="error: package ID specification `not-installed` did not match any packages\n",
        )
        with patch("check_sidecar_closure._run_cargo", return_value=result):
            self.assertEqual(
                _reverse_dependency_hits(
                    Path.cwd(),
                    "efflab-agent-sidecar",
                    "aarch64-apple-darwin",
                    "normal,build",
                    ["not-installed"],
                ),
                set(),
            )

    def test_reverse_dependency_ignores_cargo_lock_wait_before_missing_candidate(self) -> None:
        """Cargo 等待 package cache 锁时的固定状态行不应污染缺失候选判断。"""
        result = subprocess.CompletedProcess(
            ["cargo"],
            101,
            stdout="",
            stderr=(
                "Blocking waiting for file lock on package cache\n"
                "error: package ID specification `not-installed` did not match any packages\n"
            ),
        )
        with patch("check_sidecar_closure._run_cargo", return_value=result):
            self.assertEqual(
                _reverse_dependency_hits(
                    Path.cwd(),
                    "efflab-agent-sidecar",
                    "aarch64-apple-darwin",
                    "normal,build",
                    ["not-installed"],
                ),
                set(),
            )

    def test_reverse_dependency_ignores_indented_repeated_cargo_lock_wait(self) -> None:
        """真实 Cargo 会为连续的 package cache 锁等待状态行保留四个前导空格。"""
        result = subprocess.CompletedProcess(
            ["cargo"],
            101,
            stdout="",
            stderr=(
                "    Blocking waiting for file lock on package cache\n"
                "    Blocking waiting for file lock on package cache\n"
                "    Blocking waiting for file lock on package cache\n"
                "error: package ID specification `not-installed` did not match any packages\n"
            ),
        )
        with patch("check_sidecar_closure._run_cargo", return_value=result):
            self.assertEqual(
                _reverse_dependency_hits(
                    Path.cwd(),
                    "efflab-agent-sidecar",
                    "aarch64-apple-darwin",
                    "normal,build",
                    ["not-installed"],
                ),
                set(),
            )

    def test_missing_reverse_candidate_rejects_misplaced_or_unknown_lock_wait(self) -> None:
        """锁等待行只能是前置固定状态，任何其它位置或内容都必须拒绝。"""
        missing = "error: package ID specification `not-installed` did not match any packages\n"
        invalid_diagnostics = (
            missing + "    Blocking waiting for file lock on package cache\n",
            "    Blocking waiting for file lock on package cache (retrying)\n" + missing,
            "    Blocking waiting for file lock on package cache\n"
            "warning: unrelated cargo diagnostic\n"
            + missing,
        )
        for stderr in invalid_diagnostics:
            with self.subTest(stderr=stderr):
                result = subprocess.CompletedProcess(
                    ["cargo"],
                    101,
                    stdout="",
                    stderr=stderr,
                )
                self.assertFalse(_is_missing_reverse_candidate(result, "not-installed"))

    def test_reverse_dependency_missing_plus_other_error_is_not_ignored(self) -> None:
        result = subprocess.CompletedProcess(
            ["cargo"],
            101,
            stdout="",
            stderr=(
                "error: package ID specification `not-installed` did not match any packages\n"
                "error: failed to resolve dependency graph\n"
            ),
        )
        with patch("check_sidecar_closure._run_cargo", return_value=result):
            with self.assertRaisesRegex(ClosureGateError, "reverse dependency.*failed"):
                _reverse_dependency_hits(
                    Path.cwd(),
                    "efflab-agent-sidecar",
                    "aarch64-apple-darwin",
                    "normal,build",
                    ["not-installed"],
                )

    def test_reverse_dependency_wrong_missing_candidate_is_not_ignored(self) -> None:
        result = subprocess.CompletedProcess(
            ["cargo"],
            101,
            stdout="",
            stderr="error: package ID specification `other-candidate` did not match any packages\n",
        )
        with patch("check_sidecar_closure._run_cargo", return_value=result):
            with self.assertRaisesRegex(ClosureGateError, "reverse dependency.*failed"):
                _reverse_dependency_hits(
                    Path.cwd(),
                    "efflab-agent-sidecar",
                    "aarch64-apple-darwin",
                    "normal,build",
                    ["not-installed"],
                )

    def test_reverse_dependency_duplicate_missing_diagnostics_are_not_ignored(self) -> None:
        diagnostic = "error: package ID specification `not-installed` did not match any packages\n"
        result = subprocess.CompletedProcess(
            ["cargo"],
            101,
            stdout="",
            stderr=diagnostic + diagnostic,
        )
        with patch("check_sidecar_closure._run_cargo", return_value=result):
            with self.assertRaisesRegex(ClosureGateError, "reverse dependency.*failed"):
                _reverse_dependency_hits(
                    Path.cwd(),
                    "efflab-agent-sidecar",
                    "aarch64-apple-darwin",
                    "normal,build",
                    ["not-installed"],
                )

    def test_reverse_dependency_path_on_stdout_blocks_missing_skip(self) -> None:
        result = subprocess.CompletedProcess(
            ["cargo"],
            101,
            stdout="efflab-agent-sidecar v0.1.0\n└── dependency v1.0.0\n",
            stderr="error: package ID specification `not-installed` did not match any packages\n",
        )
        self.assertFalse(_is_missing_reverse_candidate(result, "not-installed"))
        with patch("check_sidecar_closure._run_cargo", return_value=result):
            with self.assertRaisesRegex(ClosureGateError, "reverse dependency.*failed"):
                _reverse_dependency_hits(
                    Path.cwd(),
                    "efflab-agent-sidecar",
                    "aarch64-apple-darwin",
                    "normal,build",
                    ["not-installed"],
                )

    def test_reverse_dependency_zero_exit_with_malformed_stdout_fails_closed(self) -> None:
        """reverse tree 即使非空，无法解析时也不能被当作命中。"""
        result = subprocess.CompletedProcess(
            ["cargo"],
            0,
            stdout="not a cargo tree\n",
            stderr="",
        )
        with patch("check_sidecar_closure._run_cargo", return_value=result):
            with self.assertRaisesRegex(ClosureGateError, "reverse dependency.*malformed"):
                _reverse_dependency_hits(
                    Path.cwd(),
                    "efflab-agent-sidecar",
                    "aarch64-apple-darwin",
                    "normal,build",
                    ["xai-grok-shell"],
                )

    def test_reverse_dependency_rejects_other_cargo_errors(self) -> None:
        result = subprocess.CompletedProcess(
            ["cargo"],
            2,
            stdout="",
            stderr="error: failed to resolve dependency graph\n",
        )
        with patch("check_sidecar_closure._run_cargo", return_value=result):
            with self.assertRaisesRegex(ClosureGateError, "reverse dependency.*failed"):
                _reverse_dependency_hits(
                    Path.cwd(),
                    "efflab-agent-sidecar",
                    "aarch64-apple-darwin",
                    "normal,build",
                    ["xai-grok-shell"],
                )

    def test_workspace_snapshot_compares_external_baseline(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace_root = Path(directory)
            manifest_path = workspace_root / "crates" / "fixture" / "Cargo.toml"
            manifest_path.parent.mkdir(parents=True)
            manifest_path.write_text("[package]\nname = \"fixture\"\n", encoding="utf-8")
            metadata = {
                "workspace_root": str(workspace_root),
                "packages": [
                    {
                        "id": "path+file:///fixture#0.1.0",
                        "name": "fixture",
                        "manifest_path": str(manifest_path),
                    }
                ],
                "workspace_members": ["path+file:///fixture#0.1.0"],
            }
            baseline = workspace_root / "members.snapshot"
            baseline.write_text("crates/other/Cargo.toml\n", encoding="utf-8")
            with self.assertRaises(ClosureGateError):
                _workspace_snapshot(metadata, snapshot_path=baseline)

    def test_versioned_workspace_snapshot_exists(self) -> None:
        snapshot = Path(__file__).with_name("sidecar_workspace_members.txt")
        self.assertTrue(snapshot.is_file())
        self.assertIn("crates/efflab/efflab-agent-sidecar/Cargo.toml", snapshot.read_text())

    def test_string_fixture_covers_sensitive_values_and_safe_allowlist(self) -> None:
        policy = load_string_policy()
        for required in ("grok.com", "x.com", "api.x.ai", "mixpanel", "otlp", "trace upload"):
            self.assertIn(required, policy.denylist)
        self.assertIn("http://127.0.0.1", policy.allowlist)

    def test_scan_fixed_strings_text_is_case_insensitive_and_stable(self) -> None:
        policy = load_string_policy()
        output = "\n".join(
            (
                "http://127.0.0.1",
                "API.X.AI/v1",
                "trace upload disabled",
                "grok.com",
                "MIXPANEL",
                "OTLP exporter",
                "x.com",
                "x.com",
            )
        )
        self.assertEqual(
            scan_strings_text(output, policy),
            ["api.x.ai", "grok.com", "mixpanel", "otlp", "trace upload", "x.com"],
        )

    def test_policy_rejects_unknown_or_malformed_fixture(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory) / "bad-policy.txt"
            fixture.write_text("[denylist]\ngrok.com\n[unexpected]\nvalue\n[allowlist]\n", encoding="utf-8")
            with self.assertRaisesRegex(ClosureGateError, "unknown section"):
                load_string_policy(fixture)

    def test_policy_rejects_case_insensitive_cross_section_overlap(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory) / "overlap-policy.txt"
            fixture.write_text(
                "[denylist]\nAPI.X.AI\n\n[allowlist]\napi.x.ai/v1\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ClosureGateError, "overlapping"):
                load_string_policy(fixture)

    def test_policy_rejects_allowlist_substring_of_denylist(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory) / "reverse-overlap-policy.txt"
            fixture.write_text(
                "[denylist]\napi.x.ai/v1\n\n[allowlist]\napi.x.ai\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ClosureGateError, "overlapping"):
                load_string_policy(fixture)

    def test_default_policy_rejects_missing_fixed_denylist_entry(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory) / "default-policy.txt"
            fixture.write_text("[denylist]\ngrok.com\n\n[allowlist]\nlocal\n", encoding="utf-8")
            with patch("check_sidecar_closure.STRING_POLICY_FIXTURE", fixture):
                with self.assertRaisesRegex(ClosureGateError, "required default denylist"):
                    load_string_policy()

    def test_fixture_cannot_be_scanned_as_release_binary(self) -> None:
        fixture = Path(__file__).resolve().parents[1] / "crates" / "efflab" / "efflab-agent-sidecar" / "tests" / "fixtures" / "denylist_strings.txt"
        self.assertTrue(fixture.is_file())
        with self.assertRaisesRegex(ClosureGateError, "fixture"):
            scan_release_binary(
                fixture,
                strings_output=fixture.read_text(encoding="utf-8"),
                target="aarch64-apple-darwin",
                repo_root=Path(__file__).resolve().parents[1],
            )

    def test_missing_binary_fails_closed(self) -> None:
        target = "aarch64-apple-darwin"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            missing = self._release_binary_path(root, target)
            with self.assertRaisesRegex(ClosureGateError, "binary"):
                scan_release_binary(
                    missing,
                    strings_output="",
                    target=target,
                    repo_root=root,
                )

    def test_empty_release_binary_fails_closed(self) -> None:
        target = "aarch64-apple-darwin"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = self._release_binary_path(root, target)
            binary.parent.mkdir(parents=True)
            binary.write_bytes(b"")
            binary.chmod(0o755)
            with self.assertRaisesRegex(ClosureGateError, "empty"):
                scan_release_binary(
                    binary,
                    strings_output="",
                    target=target,
                    repo_root=root,
                )

    def test_unix_release_binary_requires_execute_bit(self) -> None:
        target = "aarch64-apple-darwin"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = self._release_binary_path(root, target)
            binary.parent.mkdir(parents=True)
            binary.write_bytes(b"release binary")
            binary.chmod(0o644)
            with self.assertRaisesRegex(ClosureGateError, "executable"):
                scan_release_binary(
                    binary,
                    strings_output="",
                    target=target,
                    repo_root=root,
                )

    def test_debug_binary_is_not_eligible_for_release_scan(self) -> None:
        target = "aarch64-apple-darwin"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "target" / target / "debug" / _release_binary_name(target)
            binary.parent.mkdir(parents=True)
            binary.write_bytes(b"debug binary")
            with self.assertRaisesRegex(ClosureGateError, "debug"):
                scan_release_binary(
                    binary,
                    strings_output="",
                    target=target,
                    repo_root=root,
                )

    def test_missing_strings_tool_fails_closed(self) -> None:
        target = "aarch64-apple-darwin"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = self._write_release_binary(root, target)
            with patch("check_sidecar_closure.shutil.which", return_value=None):
                with self.assertRaisesRegex(ClosureGateError, "strings"):
                    scan_release_binary(binary, target=target, repo_root=root)

    def test_cli_exposes_explicit_closure_and_release_modes(self) -> None:
        parser = _argument_parser()
        closure_args = parser.parse_args(
            [
                "--package",
                "efflab-agent-sidecar",
                "--target",
                "aarch64-apple-darwin",
                "--profile",
                "release",
                "--edges",
                "normal,build",
                "--out",
                "report.json",
            ]
        )
        self.assertEqual(closure_args.mode, "closure-only")
        self.assertFalse(closure_args.require_binary)

        release_args = parser.parse_args(
            [
                "--package",
                "efflab-agent-sidecar",
                "--target",
                "aarch64-apple-darwin",
                "--profile",
                "release",
                "--edges",
                "normal,build",
                "--out",
                "report.json",
                "--mode",
                "release-certification",
                "--require-binary",
                "--binary",
                "target/aarch64-apple-darwin/release/efflab-agent-sidecar",
            ]
        )
        self.assertEqual(release_args.mode, "release-certification")
        self.assertTrue(release_args.require_binary)
        self.assertEqual(
            release_args.binary,
            Path("target/aarch64-apple-darwin/release/efflab-agent-sidecar"),
        )

        windows_release_args = parser.parse_args(
            [
                "--package",
                "efflab-agent-sidecar",
                "--target",
                "x86_64-pc-windows-msvc",
                "--profile",
                "release",
                "--edges",
                "normal,build",
                "--out",
                "report.json",
                "--mode",
                "release-certification",
                "--binary",
                "target/x86_64-pc-windows-msvc/release/efflab-agent-sidecar.exe",
            ]
        )
        self.assertEqual(windows_release_args.mode, "release-certification")
        self.assertEqual(
            windows_release_args.binary,
            Path("target/x86_64-pc-windows-msvc/release/efflab-agent-sidecar.exe"),
        )

    def test_main_rejects_empty_edge_tokens_before_running_cargo(self) -> None:
        """CLI 的 --edges 空 token 必须在任何 Cargo 查询前拒绝。"""
        for raw_edges in ("normal,,build", ",normal,build", "normal,build,"):
            with self.subTest(raw_edges=raw_edges), tempfile.TemporaryDirectory() as directory:
                report_path = Path(directory) / "report.json"
                with patch("check_sidecar_closure._require_cargo_output") as cargo_mock:
                    status = main(
                        [
                            "--package",
                            "efflab-agent-sidecar",
                            "--target",
                            "aarch64-apple-darwin",
                            "--profile",
                            "release",
                            "--edges",
                            raw_edges,
                            "--out",
                            str(report_path),
                        ]
                    )
                self.assertEqual(status, 2)
                self.assertFalse(report_path.exists())
                cargo_mock.assert_not_called()

    def test_report_without_binary_keeps_binary_hits_empty_and_paths_stable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "crates" / "fixture" / "Cargo.toml"
            manifest.parent.mkdir(parents=True)
            manifest.write_text('[package]\nname = "fixture"\n', encoding="utf-8")
            (root / "Cargo.lock").write_text("lock", encoding="utf-8")
            metadata = {
                "workspace_root": str(root),
                "packages": [
                    {
                        "id": f"path+{manifest.parent.as_uri()}#0.1.0",
                        "name": "efflab-agent-sidecar",
                        "version": "0.1.0",
                        "manifest_path": str(manifest),
                    }
                ],
                "workspace_members": [f"path+{manifest.parent.as_uri()}#0.1.0"],
            }
            baseline = root / "members.snapshot"
            baseline.write_text("crates/fixture/Cargo.toml\n", encoding="utf-8")
            with patch(
                "check_sidecar_closure._require_cargo_output",
                side_effect=(
                    "efflab-agent-sidecar v0.1.0\n└── xai-grok-shell v0.1.0",
                    "efflab-agent-sidecar v0.1.0\n"
                    "└── efflab-agent-sidecar feature \"default\"\n",
                    json.dumps(metadata),
                ),
            ) as cargo_mock, patch("check_sidecar_closure._workspace_snapshot", return_value=("crates/fixture/Cargo.toml",)), patch(
                "check_sidecar_closure._reverse_dependency_hits", return_value={"xai-grok-shell"}
            ), patch("check_sidecar_closure.scan_release_binary") as scan_mock, patch(
                "check_sidecar_closure._read_strings_command"
            ) as strings_mock:
                report = build_report(
                    "efflab-agent-sidecar",
                    "aarch64-apple-darwin",
                    "release",
                    "normal,build",
                    repo_root=root,
                )

            scan_mock.assert_not_called()
            strings_mock.assert_not_called()
            self.assertEqual(cargo_mock.call_count, 3)
            for cargo_call in cargo_mock.call_args_list:
                self.assertIn("--locked", cargo_call.args[1])
            self.assertEqual(report["denylist_hits"], ["package:xai-grok-shell"])
            self.assertFalse(any(hit.startswith("binary:") for hit in report["denylist_hits"]))
            self.assertEqual(report["schema_version"], 1)
            self.assertEqual(report["gate_version"], 1)
            self.assertEqual(report["scan_mode"], "closure-only")
            self.assertFalse(report["binary_scanned"])
            self.assertEqual(report["binary_scan_status"], "not-requested")
            self.assertNotIn(root.as_posix(), json.dumps(report))

    def test_build_report_requires_binary_for_release_certification(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ClosureGateError, "release-certification.*binary"):
                build_report(
                    "efflab-agent-sidecar",
                    "aarch64-apple-darwin",
                    "release",
                    "normal,build",
                    repo_root=Path(directory),
                    scan_mode="release-certification",
                )

    def test_build_report_rejects_binary_in_closure_only_mode(self) -> None:
        target = "aarch64-apple-darwin"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = self._write_release_binary(root, target)
            with self.assertRaisesRegex(ClosureGateError, "closure-only.*binary"):
                build_report(
                    "efflab-agent-sidecar",
                    target,
                    "release",
                    "normal,build",
                    repo_root=root,
                    binary_path=binary,
                )

    def test_build_report_requires_explicit_test_only_for_injected_strings(self) -> None:
        target = "aarch64-apple-darwin"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = self._write_release_binary(root, target)
            with self.assertRaisesRegex(ClosureGateError, "test-only"):
                build_report(
                    "efflab-agent-sidecar",
                    target,
                    "release",
                    "normal,build",
                    repo_root=root,
                    binary_path=binary,
                    strings_output="grok.com\\n",
                    scan_mode="release-certification",
                )

    def test_build_report_marks_injected_strings_as_not_scanned(self) -> None:
        target = "aarch64-apple-darwin"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "Cargo.lock").write_text("lock", encoding="utf-8")
            binary = self._write_release_binary(root, target)
            with patch(
                "check_sidecar_closure._require_cargo_output",
                side_effect=(
                    "efflab-agent-sidecar v0.1.0\n",
                    "efflab-agent-sidecar v0.1.0\n"
                    "└── efflab-agent-sidecar feature \"default\"\n",
                    json.dumps({}),
                ),
            ), patch("check_sidecar_closure._workspace_snapshot"), patch(
                "check_sidecar_closure._reverse_dependency_hits", return_value=set()
            ):
                # metadata/package 校验在本用例不属于字符串注入状态断言。
                with patch(
                    "check_sidecar_closure._load_metadata",
                    return_value={
                        "workspace_root": str(root),
                        "packages": [
                            {
                                "id": "fixture-id",
                                "name": "efflab-agent-sidecar",
                                "version": "0.1.0",
                                "manifest_path": str(root / "Cargo.toml"),
                            }
                        ],
                        "workspace_members": ["fixture-id"],
                    },
                ), patch("check_sidecar_closure.scan_release_binary", return_value=["grok.com"]):
                    report = build_report(
                        "efflab-agent-sidecar",
                        target,
                        "release",
                        "normal,build",
                        repo_root=root,
                        binary_path=binary,
                        strings_output="grok.com\n",
                        scan_mode="release-certification",
                        test_only_strings=True,
                    )
            self.assertEqual(report["scan_mode"], "release-certification")
            self.assertFalse(report["binary_scanned"])
            self.assertEqual(report["binary_scan_status"], "test-input-only")

    def test_build_report_rejects_empty_main_tree(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with patch("check_sidecar_closure._require_cargo_output", return_value="") as cargo_mock:
                with self.assertRaisesRegex(ClosureGateError, "normal/build.*empty"):
                    build_report(
                        "efflab-agent-sidecar",
                        "aarch64-apple-darwin",
                        "release",
                        "normal,build",
                        repo_root=Path(directory),
                    )
            cargo_mock.assert_called_once()

    def test_build_report_rejects_wrong_main_tree_root(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with patch(
                "check_sidecar_closure._require_cargo_output",
                return_value="pager v0.1.0\n└── efflab-agent-sidecar v0.1.0",
            ) as cargo_mock:
                with self.assertRaisesRegex(ClosureGateError, "tree root"):
                    build_report(
                        "efflab-agent-sidecar",
                        "aarch64-apple-darwin",
                        "release",
                        "normal,build",
                        repo_root=Path(directory),
                    )
            cargo_mock.assert_called_once()

    def test_build_report_rejects_empty_feature_tree(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with patch(
                "check_sidecar_closure._require_cargo_output",
                side_effect=("efflab-agent-sidecar v0.1.0\n", ""),
            ) as cargo_mock:
                with self.assertRaisesRegex(ClosureGateError, "feature.*empty"):
                    build_report(
                        "efflab-agent-sidecar",
                        "aarch64-apple-darwin",
                        "release",
                        "normal,build",
                        repo_root=Path(directory),
                    )
            self.assertEqual(cargo_mock.call_count, 2)

    def test_strings_input_requires_explicit_binary(self) -> None:
        with self.assertRaisesRegex(ClosureGateError, "--binary"):
            build_report(
                "efflab-agent-sidecar",
                "aarch64-apple-darwin",
                "release",
                "normal,build",
                repo_root=Path(tempfile.gettempdir()),
                strings_output="grok.com\n",
            )

    def test_main_strings_input_without_binary_returns_2_without_report(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            strings_input = root / "captured-strings.txt"
            report_path = root / "report.json"
            strings_input.write_text("grok.com\n", encoding="utf-8")
            with patch("check_sidecar_closure._require_cargo_output") as cargo_mock:
                status = main(
                    [
                        "--package",
                        "efflab-agent-sidecar",
                        "--target",
                        "aarch64-apple-darwin",
                        "--profile",
                        "release",
                        "--edges",
                        "normal,build",
                        "--out",
                        str(report_path),
                        "--strings-input",
                        str(strings_input),
                    ]
                )
            self.assertEqual(status, 2)
            self.assertFalse(report_path.exists())
            cargo_mock.assert_not_called()

    def test_main_release_certification_requires_binary_without_running_cargo(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report_path = Path(directory) / "report.json"
            with patch("check_sidecar_closure._require_cargo_output") as cargo_mock:
                status = main(
                    [
                        "--package",
                        "efflab-agent-sidecar",
                        "--target",
                        "aarch64-apple-darwin",
                        "--profile",
                        "release",
                        "--edges",
                        "normal,build",
                        "--out",
                        str(report_path),
                        "--require-binary",
                    ]
                )
            self.assertEqual(status, 2)
            self.assertFalse(report_path.exists())
            cargo_mock.assert_not_called()

    def test_main_strings_input_with_binary_is_not_release_certification(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = self._write_release_binary(root, "aarch64-apple-darwin")
            strings_input = root / "captured-strings.txt"
            report_path = root / "report.json"
            strings_input.write_text("grok.com\n", encoding="utf-8")
            with patch("check_sidecar_closure._require_cargo_output") as cargo_mock:
                status = main(
                    [
                        "--package",
                        "efflab-agent-sidecar",
                        "--target",
                        "aarch64-apple-darwin",
                        "--profile",
                        "release",
                        "--edges",
                        "normal,build",
                        "--out",
                        str(report_path),
                        "--mode",
                        "release-certification",
                        "--binary",
                        str(binary),
                        "--strings-input",
                        str(strings_input),
                    ]
                )
            self.assertEqual(status, 2)
            self.assertFalse(report_path.exists())
            cargo_mock.assert_not_called()

    def test_binary_outside_release_layout_is_rejected(self) -> None:
        target = "aarch64-apple-darwin"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "artifacts" / _release_binary_name(target)
            binary.parent.mkdir()
            binary.write_bytes(b"release binary")
            with self.assertRaisesRegex(ClosureGateError, "release"):
                scan_release_binary(
                    binary,
                    strings_output="",
                    target=target,
                    repo_root=root,
                )

    def test_release_binary_rejects_unpublished_target(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ClosureGateError, "unsupported release target"):
                scan_release_binary(
                    Path(directory) / "target" / "release" / "efflab-agent-sidecar",
                    strings_output="",
                    target="aarch64-unknown-linux-gnu",
                    repo_root=Path(directory),
                )

    def test_release_binary_wrong_target_is_rejected(self) -> None:
        target = "aarch64-apple-darwin"
        wrong_target = "x86_64-apple-darwin"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = self._write_release_binary(root, wrong_target)
            with self.assertRaisesRegex(ClosureGateError, "target"):
                scan_release_binary(
                    binary,
                    strings_output="",
                    target=target,
                    repo_root=root,
                )

    def test_release_binary_never_uses_host_target_directory_fallback(self) -> None:
        target = "aarch64-apple-darwin"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            legacy = root / "target" / "release" / _release_binary_name(target)
            legacy.parent.mkdir(parents=True)
            legacy.write_bytes(b"release binary")
            legacy.chmod(0o755)
            with self.assertRaisesRegex(ClosureGateError, "target"):
                scan_release_binary(
                    legacy,
                    strings_output="",
                    target=target,
                    repo_root=root,
                )

    def test_release_binary_symlink_to_debug_is_rejected(self) -> None:
        target = "aarch64-apple-darwin"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            expected = self._release_binary_path(root, target)
            debug_binary = root / "target" / "debug" / _release_binary_name(target)
            debug_binary.parent.mkdir(parents=True)
            debug_binary.write_bytes(b"debug binary")
            expected.parent.mkdir(parents=True)
            try:
                expected.symlink_to(debug_binary)
            except OSError as error:
                self.skipTest(f"symlink creation unavailable: {error}")
            with self.assertRaisesRegex(ClosureGateError, "symlink"):
                scan_release_binary(
                    expected,
                    strings_output="",
                    target=target,
                    repo_root=root,
                )

    def test_release_binary_nested_debug_and_wrong_name_are_rejected(self) -> None:
        target = "aarch64-apple-darwin"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            nested_debug = root / "target" / target / "release" / "debug" / _release_binary_name(target)
            nested_debug.parent.mkdir(parents=True)
            nested_debug.write_bytes(b"debug binary")
            with self.assertRaisesRegex(ClosureGateError, "debug"):
                scan_release_binary(
                    nested_debug,
                    strings_output="",
                    target=target,
                    repo_root=root,
                )

            wrong_name = self._release_binary_path(root, target, "not-efflab-agent-sidecar")
            wrong_name.parent.mkdir(parents=True, exist_ok=True)
            wrong_name.write_bytes(b"release binary")
            with self.assertRaisesRegex(ClosureGateError, "efflab-agent-sidecar"):
                scan_release_binary(
                    wrong_name,
                    strings_output="",
                    target=target,
                    repo_root=root,
                )

    def test_explicit_binary_without_strings_output_calls_strings_command(self) -> None:
        target = "aarch64-apple-darwin"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = self._write_release_binary(root, target)
            with patch("check_sidecar_closure._read_strings_command", return_value="") as strings_mock:
                self.assertEqual(
                    scan_release_binary(binary, target=target, repo_root=root),
                    [],
                )
            strings_mock.assert_called_once_with(binary)

    def test_windows_release_binary_requires_exe_name(self) -> None:
        target = "x86_64-pc-windows-msvc"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            wrong_name = self._release_binary_path(root, target, "efflab-agent-sidecar")
            wrong_name.parent.mkdir(parents=True)
            wrong_name.write_bytes(b"release binary")
            with self.assertRaisesRegex(ClosureGateError, "efflab-agent-sidecar.exe"):
                scan_release_binary(
                    wrong_name,
                    strings_output="",
                    target=target,
                    repo_root=root,
                )

            binary = self._write_release_binary(root, target)
            self.assertEqual(
                scan_release_binary(
                    binary,
                    strings_output="",
                    target=target,
                    repo_root=root,
                ),
                [],
            )

    def test_report_tags_dependency_and_binary_hits_without_changing_list_shape(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "crates" / "fixture" / "Cargo.toml"
            manifest.parent.mkdir(parents=True)
            manifest.write_text('[package]\nname = "fixture"\n', encoding="utf-8")
            (root / "Cargo.lock").write_text("lock", encoding="utf-8")
            metadata = {
                "workspace_root": str(root),
                "packages": [
                    {
                        "id": "path+file:///fixture#0.1.0",
                        "name": "efflab-agent-sidecar",
                        "manifest_path": str(manifest),
                    }
                ],
                "workspace_members": ["path+file:///fixture#0.1.0"],
            }
            baseline = root / "members.snapshot"
            baseline.write_text("crates/fixture/Cargo.toml\n", encoding="utf-8")
            fixture = root / "denylist_strings.txt"
            fixture.write_text("[denylist]\ngrok.com\n\n[allowlist]\nhttp://127.0.0.1\n", encoding="utf-8")
            binary = self._write_release_binary(root, "aarch64-apple-darwin")
            with patch(
                "check_sidecar_closure._require_cargo_output",
                side_effect=(
                    "efflab-agent-sidecar v0.1.0\n└── xai-grok-shell v0.1.0",
                    "efflab-agent-sidecar v0.1.0\n"
                    "└── efflab-agent-sidecar feature \"default\"\n",
                    json.dumps(metadata),
                ),
            ), patch("check_sidecar_closure._workspace_snapshot", return_value=("crates/fixture/Cargo.toml",)), patch(
                "check_sidecar_closure._reverse_dependency_hits", return_value={"xai-grok-shell"}
            ):
                report = build_report(
                    "efflab-agent-sidecar",
                    "aarch64-apple-darwin",
                    "release",
                    "normal,build",
                    repo_root=root,
                    binary_path=binary,
                    strings_output="grok.com\n",
                    string_fixture_path=fixture,
                    scan_mode="release-certification",
                    test_only_strings=True,
                )
            self.assertIsInstance(report["denylist_hits"], list)
            self.assertEqual(
                report["denylist_hits"],
                ["binary:grok.com", "package:xai-grok-shell"],
            )

    def test_gate_version_remains_contract_version_1(self) -> None:
        self.assertEqual(GATE_VERSION, 1)
        self.assertEqual(SIDECAR_CLOSURE_GATE_VERSION, 1)


if __name__ == "__main__":
    unittest.main()
