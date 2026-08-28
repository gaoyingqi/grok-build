#!/usr/bin/env python3
"""闭包门禁的可执行单元测试。"""

import unittest

from check_sidecar_closure import GATE_VERSION, check_sidecar_tree, parse_tree_packages


class SidecarClosureTest(unittest.TestCase):
    """验证 sidecar 闭包解析的最小合同。"""

    def test_rejects_forbidden_package_in_sidecar_tree(self) -> None:
        tree = "efflab-agent-sidecar v0.1.0\n└── xai-grok-shell v0.1.0"
        self.assertEqual(check_sidecar_tree(tree), ["xai-grok-shell"])

    def test_does_not_scan_unrelated_workspace_member(self) -> None:
        tree = "pager v0.1.0\n└── xai-grok-shell v0.1.0"
        self.assertEqual(check_sidecar_tree(tree), [])

    def test_duplicate_reqwest_versions_fail(self) -> None:
        tree = "reqwest v0.12.24\nreqwest v0.13.4"
        hits = check_sidecar_tree(tree)
        self.assertTrue(
            any(item == "reqwest@duplicate" or item.startswith("reqwest") for item in hits)
        )
        self.assertEqual(
            len([package for package in parse_tree_packages(tree) if package.split(" ")[0] == "reqwest"]),
            2,
        )

    def test_gate_version_is_1(self) -> None:
        self.assertEqual(GATE_VERSION, 1)


if __name__ == "__main__":
    unittest.main()
