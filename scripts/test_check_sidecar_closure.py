#!/usr/bin/env python3
"""闭包门禁的可执行单元测试。"""

import tempfile
import unittest
from pathlib import Path

from check_sidecar_closure import (
    ClosureGateError,
    GATE_VERSION,
    _parse_edges,
    _workspace_snapshot,
    check_sidecar_tree,
    parse_tree_packages,
)


class SidecarClosureTest(unittest.TestCase):
    """验证 sidecar 闭包解析的最小合同。"""

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

    def test_edges_require_normal_and_build(self) -> None:
        with self.assertRaises(ClosureGateError):
            _parse_edges("normal")
        with self.assertRaises(ClosureGateError):
            _parse_edges("build")
        self.assertEqual(_parse_edges("normal,build"), ("normal,build", ["normal", "build"]))

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

    def test_gate_version_is_1(self) -> None:
        self.assertEqual(GATE_VERSION, 1)


if __name__ == "__main__":
    unittest.main()
