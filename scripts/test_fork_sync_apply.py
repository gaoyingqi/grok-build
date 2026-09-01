#!/usr/bin/env python3
"""fork-sync apply 的临时 workspace 回归测试。"""

from __future__ import annotations

import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("fork-sync-apply.sh")
ANCHOR = "prod/mc/cli-chat-proxy-types"
MEMBERS = (
    "crates/efflab/efflab-agent-contract",
    "crates/efflab/efflab-agent-host",
    "crates/efflab/efflab-agent-sidecar",
)
SOURCE_REV = "0123456789abcdef0123456789abcdef01234567"
STALE_BASE_REV = "fedcba9876543210fedcba9876543210fedcba98"
STALE_PROBE = "crates/efflab/efflab-pr0-http-probe"
UNRELATED_MEMBER = "upstream/unrelated-member"
# git rev-parse 与仓库内 revision 文件均为小写，故大写 SHA 也按非规范输入拒绝。
INVALID_REVISIONS = ("", "not-a-commit", f"{SOURCE_REV}\nextra")
NON_CANONICAL_REVISIONS = (*INVALID_REVISIONS, SOURCE_REV.upper())
COMPLETE_MEMBERS = (ANCHOR, *MEMBERS)


def _manifest_text(anchor_lines: tuple[str, ...], manifest_extra: str = "") -> str:
    """构造供临时 workspace 使用的生成式 manifest 文本。"""
    members_text = "\n".join(f'    "{line}",' for line in anchor_lines)
    return (
        "[workspace]\nmembers = [\n"
        + members_text
        + "\n]\n"
        + manifest_extra
    )


class ForkSyncApplyTest(unittest.TestCase):
    """验证 manifest anchor 与 FORK_BASE_REV 门禁的 fail-closed 行为。"""

    @staticmethod
    def _workspace(
        anchor_lines: tuple[str, ...],
        *,
        source_rev: str | None = SOURCE_REV,
        fork_base_rev: str | None = SOURCE_REV,
        manifest_extra: str = "",
    ) -> tuple[tempfile.TemporaryDirectory[str], Path]:
        """创建只供复制脚本运行的临时 workspace，不触碰真实仓库。"""
        temporary = tempfile.TemporaryDirectory(prefix="efflab-fork-sync-")
        root = Path(temporary.name)
        scripts = root / "scripts"
        scripts.mkdir()
        shutil.copy2(SCRIPT, scripts / SCRIPT.name)

        (root / "Cargo.toml").write_text(
            _manifest_text(anchor_lines, manifest_extra),
            encoding="utf-8",
        )
        if source_rev is not None:
            (root / "SOURCE_REV").write_text(source_rev + "\n", encoding="utf-8")
        if fork_base_rev is not None:
            (root / "FORK_BASE_REV").write_text(fork_base_rev + "\n", encoding="utf-8")
        for member in MEMBERS:
            manifest = root / member / "Cargo.toml"
            manifest.parent.mkdir(parents=True)
            manifest.write_text("[package]\nname = \"fixture\"\n", encoding="utf-8")
        return temporary, root

    @staticmethod
    def _run(root: Path, mode: str) -> subprocess.CompletedProcess[str]:
        """运行临时复制的脚本并保留真实退出状态。"""
        return subprocess.run(
            [str(root / "scripts" / SCRIPT.name), mode],
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
        )

    @classmethod
    def _apply(cls, root: Path) -> subprocess.CompletedProcess[str]:
        """运行临时复制的 apply 脚本。"""
        return cls._run(root, "--apply")

    @classmethod
    def _check(cls, root: Path) -> subprocess.CompletedProcess[str]:
        """运行临时复制的 check 脚本。"""
        return cls._run(root, "--check")

    def _assert_manifest_layout_rejected(self, manifest_text: str) -> None:
        """验证不支持的 workspace 布局在两种模式均拒绝且不写状态。"""
        for mode in ("--check", "--apply"):
            with self.subTest(mode=mode):
                temporary, root = self._workspace(COMPLETE_MEMBERS)
                try:
                    manifest = root / "Cargo.toml"
                    state = root / "FORK_BASE_REV"
                    manifest.write_text(manifest_text, encoding="utf-8")
                    before = manifest.read_bytes()
                    before_state = state.read_bytes()
                    result = self._run(root, mode)
                    self.assertEqual(result.returncode, 2, result.stderr)
                    self.assertIn("workspace members", (result.stderr + result.stdout).lower())
                    self.assertEqual(manifest.read_bytes(), before)
                    self.assertEqual(state.read_bytes(), before_state)
                finally:
                    temporary.cleanup()

    def test_check_missing_base_fails_closed_and_preserves_manifest(self) -> None:
        temporary, root = self._workspace((ANCHOR,), fork_base_rev=None)
        try:
            manifest = root / "Cargo.toml"
            before = manifest.read_bytes()
            result = self._check(root)
            self.assertEqual(result.returncode, 1)
            self.assertIn("FORK_BASE_REV missing", result.stdout)
            self.assertEqual(manifest.read_bytes(), before)
            self.assertFalse((root / "FORK_BASE_REV").exists())
        finally:
            temporary.cleanup()

    def test_check_stale_base_fails_closed_and_preserves_manifest_and_state(self) -> None:
        temporary, root = self._workspace(
            COMPLETE_MEMBERS, fork_base_rev=STALE_BASE_REV
        )
        try:
            manifest = root / "Cargo.toml"
            state = root / "FORK_BASE_REV"
            before = manifest.read_bytes()
            before_state = state.read_bytes()
            result = self._check(root)
            self.assertEqual(result.returncode, 1)
            self.assertIn("FORK_BASE_REV drift", result.stdout)
            self.assertEqual(manifest.read_bytes(), before)
            self.assertEqual(state.read_bytes(), before_state)
        finally:
            temporary.cleanup()

    def test_apply_missing_base_fails_closed_and_preserves_manifest(self) -> None:
        temporary, root = self._workspace((ANCHOR,), fork_base_rev=None)
        try:
            manifest = root / "Cargo.toml"
            before = manifest.read_bytes()
            result = self._apply(root)
            self.assertEqual(result.returncode, 2)
            self.assertIn("FORK_BASE_REV missing", result.stderr)
            self.assertEqual(manifest.read_bytes(), before)
            self.assertFalse((root / "FORK_BASE_REV").exists())
        finally:
            temporary.cleanup()

    def test_apply_stale_base_fails_closed_and_preserves_manifest_and_state(self) -> None:
        temporary, root = self._workspace(
            (ANCHOR,), fork_base_rev=STALE_BASE_REV
        )
        try:
            manifest = root / "Cargo.toml"
            state = root / "FORK_BASE_REV"
            before = manifest.read_bytes()
            before_state = state.read_bytes()
            result = self._apply(root)
            self.assertEqual(result.returncode, 2)
            self.assertIn("FORK_BASE_REV drift", result.stderr)
            self.assertEqual(manifest.read_bytes(), before)
            self.assertEqual(state.read_bytes(), before_state)
        finally:
            temporary.cleanup()

    def test_check_missing_source_fails_closed_and_preserves_manifest_and_state(self) -> None:
        temporary, root = self._workspace((ANCHOR,), source_rev=None)
        try:
            manifest = root / "Cargo.toml"
            state = root / "FORK_BASE_REV"
            before = manifest.read_bytes()
            before_state = state.read_bytes()
            result = self._check(root)
            self.assertEqual(result.returncode, 2)
            self.assertIn("SOURCE_REV missing", result.stderr)
            self.assertEqual(manifest.read_bytes(), before)
            self.assertEqual(state.read_bytes(), before_state)
        finally:
            temporary.cleanup()

    def test_apply_missing_source_fails_closed_and_preserves_manifest_and_state(self) -> None:
        temporary, root = self._workspace((ANCHOR,), source_rev=None)
        try:
            manifest = root / "Cargo.toml"
            state = root / "FORK_BASE_REV"
            before = manifest.read_bytes()
            before_state = state.read_bytes()
            result = self._apply(root)
            self.assertEqual(result.returncode, 2)
            self.assertIn("SOURCE_REV missing", result.stderr)
            self.assertEqual(manifest.read_bytes(), before)
            self.assertEqual(state.read_bytes(), before_state)
        finally:
            temporary.cleanup()

    def test_check_invalid_source_revision_fails_closed(self) -> None:
        for invalid in NON_CANONICAL_REVISIONS:
            with self.subTest(invalid=repr(invalid)):
                temporary, root = self._workspace(
                    (ANCHOR,), source_rev=invalid
                )
                try:
                    manifest = root / "Cargo.toml"
                    state = root / "FORK_BASE_REV"
                    before = manifest.read_bytes()
                    before_state = state.read_bytes()
                    result = self._check(root)
                    self.assertEqual(result.returncode, 2)
                    self.assertIn("SOURCE_REV invalid", result.stderr)
                    self.assertEqual(manifest.read_bytes(), before)
                    self.assertEqual(state.read_bytes(), before_state)
                finally:
                    temporary.cleanup()

    def test_apply_invalid_source_revision_fails_closed(self) -> None:
        for invalid in NON_CANONICAL_REVISIONS:
            with self.subTest(invalid=repr(invalid)):
                temporary, root = self._workspace(
                    (ANCHOR,), source_rev=invalid
                )
                try:
                    manifest = root / "Cargo.toml"
                    state = root / "FORK_BASE_REV"
                    before = manifest.read_bytes()
                    before_state = state.read_bytes()
                    result = self._apply(root)
                    self.assertEqual(result.returncode, 2)
                    self.assertIn("SOURCE_REV invalid", result.stderr)
                    self.assertEqual(manifest.read_bytes(), before)
                    self.assertEqual(state.read_bytes(), before_state)
                finally:
                    temporary.cleanup()

    def test_check_invalid_base_revision_fails_closed(self) -> None:
        for invalid in NON_CANONICAL_REVISIONS:
            with self.subTest(invalid=repr(invalid)):
                temporary, root = self._workspace(
                    (ANCHOR,), fork_base_rev=invalid
                )
                try:
                    manifest = root / "Cargo.toml"
                    state = root / "FORK_BASE_REV"
                    before = manifest.read_bytes()
                    before_state = state.read_bytes()
                    result = self._check(root)
                    self.assertEqual(result.returncode, 1)
                    self.assertIn("FORK_BASE_REV invalid", result.stdout)
                    self.assertEqual(manifest.read_bytes(), before)
                    self.assertEqual(state.read_bytes(), before_state)
                finally:
                    temporary.cleanup()

    def test_apply_invalid_base_revision_fails_closed(self) -> None:
        for invalid in NON_CANONICAL_REVISIONS:
            with self.subTest(invalid=repr(invalid)):
                temporary, root = self._workspace(
                    (ANCHOR,), fork_base_rev=invalid
                )
                try:
                    manifest = root / "Cargo.toml"
                    state = root / "FORK_BASE_REV"
                    before = manifest.read_bytes()
                    before_state = state.read_bytes()
                    result = self._apply(root)
                    self.assertEqual(result.returncode, 2)
                    self.assertIn("FORK_BASE_REV invalid", result.stderr)
                    self.assertEqual(manifest.read_bytes(), before)
                    self.assertEqual(state.read_bytes(), before_state)
                finally:
                    temporary.cleanup()

    def test_equal_base_check_is_clean_and_preserves_state(self) -> None:
        temporary, root = self._workspace(COMPLETE_MEMBERS)
        try:
            manifest = root / "Cargo.toml"
            state = root / "FORK_BASE_REV"
            before = manifest.read_bytes()
            before_state = state.read_bytes()
            result = self._check(root)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertNotIn("FORK_BASE_REV ", result.stdout)
            self.assertEqual(manifest.read_bytes(), before)
            self.assertEqual(state.read_bytes(), before_state)
        finally:
            temporary.cleanup()

    def test_equal_base_apply_is_noop_and_preserves_state(self) -> None:
        temporary, root = self._workspace(COMPLETE_MEMBERS)
        try:
            manifest = root / "Cargo.toml"
            state = root / "FORK_BASE_REV"
            before = manifest.read_bytes()
            before_state = state.read_bytes()
            result = self._apply(root)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(manifest.read_bytes(), before)
            self.assertEqual(state.read_bytes(), before_state)
        finally:
            temporary.cleanup()

    def test_missing_anchor_check_fails_closed_and_preserves_manifest_and_state(self) -> None:
        temporary, root = self._workspace((UNRELATED_MEMBER,))
        try:
            manifest = root / "Cargo.toml"
            state = root / "FORK_BASE_REV"
            before = manifest.read_bytes()
            before_state = state.read_bytes()
            result = self._check(root)
            self.assertEqual(result.returncode, 2)
            self.assertIn("anchor", (result.stderr + result.stdout).lower())
            self.assertEqual(manifest.read_bytes(), before)
            self.assertEqual(state.read_bytes(), before_state)
        finally:
            temporary.cleanup()

    def test_manifest_convergence_removes_stale_line_and_preserves_probe_and_upstream_members(self) -> None:
        temporary, root = self._workspace(
            (UNRELATED_MEMBER, STALE_PROBE, ANCHOR, "upstream/after-member")
        )
        try:
            probe = root / STALE_PROBE
            probe.mkdir(parents=True)
            probe_content = b"probe content must remain\n"
            probe_file = probe / "sentinel.txt"
            probe_file.write_bytes(probe_content)
            manifest = root / "Cargo.toml"
            state = root / "FORK_BASE_REV"
            before_state = state.read_bytes()
            result = self._apply(root)
            self.assertEqual(result.returncode, 0, result.stderr)
            content = manifest.read_text(encoding="utf-8")
            self.assertNotIn(f'"{STALE_PROBE}",', content)
            self.assertIn(f'"{UNRELATED_MEMBER}",', content)
            self.assertIn('"upstream/after-member",', content)
            anchor_index = content.index(f'"{ANCHOR}",')
            for member in MEMBERS:
                self.assertEqual(content.count(f'"{member}",'), 1)
                self.assertLess(content.index(f'"{member}",'), anchor_index)
            self.assertTrue(probe.is_dir())
            self.assertEqual(probe_file.read_bytes(), probe_content)
            self.assertEqual(state.read_bytes(), before_state)
        finally:
            temporary.cleanup()

    def test_inline_workspace_members_array_fails_closed_and_preserves_state(self) -> None:
        temporary, root = self._workspace(COMPLETE_MEMBERS)
        try:
            manifest = root / "Cargo.toml"
            state = root / "FORK_BASE_REV"
            before_state = state.read_bytes()
            inline_members = ", ".join(f'"{member}"' for member in COMPLETE_MEMBERS)
            manifest.write_text(
                f"[workspace]\nmembers = [{inline_members}]\n",
                encoding="utf-8",
            )
            before = manifest.read_bytes()
            result = self._check(root)
            self.assertEqual(result.returncode, 2)
            self.assertIn("workspace members", (result.stderr + result.stdout).lower())
            self.assertEqual(manifest.read_bytes(), before)
            self.assertEqual(state.read_bytes(), before_state)
        finally:
            temporary.cleanup()

    def test_unclosed_workspace_members_array_fails_closed_for_check_and_apply(self) -> None:
        """未闭合数组在两种模式都必须拒绝且不写 manifest/base。"""
        for mode in ("--check", "--apply"):
            with self.subTest(mode=mode):
                temporary, root = self._workspace(COMPLETE_MEMBERS)
                try:
                    manifest = root / "Cargo.toml"
                    state = root / "FORK_BASE_REV"
                    manifest.write_text(
                        manifest.read_text(encoding="utf-8").replace("]\n", "", 1),
                        encoding="utf-8",
                    )
                    before = manifest.read_bytes()
                    before_state = state.read_bytes()
                    result = self._run(root, mode)
                    self.assertEqual(result.returncode, 2, result.stderr)
                    self.assertIn("workspace members", (result.stderr + result.stdout).lower())
                    self.assertEqual(manifest.read_bytes(), before)
                    self.assertEqual(state.read_bytes(), before_state)
                finally:
                    temporary.cleanup()

    def test_triple_quoted_basic_workspace_markup_is_rejected_without_writes(self) -> None:
        """三引号 basic string 中的伪造 workspace 不能驱动同步。"""
        fake_members = "\n".join(f'    "{member}",' for member in COMPLETE_MEMBERS)
        manifest = (
            'metadata = """\n'
            "[workspace]\n"
            "members = [\n"
            f"{fake_members}\n"
            "]\n"
            '"""\n'
        )
        self._assert_manifest_layout_rejected(manifest)

    def test_triple_quoted_literal_workspace_markup_is_rejected_without_writes(self) -> None:
        """三引号 literal string 中的伪造 workspace 不能驱动同步。"""
        fake_members = "\n".join(f'    "{member}",' for member in COMPLETE_MEMBERS)
        manifest = (
            "metadata = '''\n"
            "[workspace]\n"
            "members = [\n"
            f"{fake_members}\n"
            "]\n"
            "'''\n"
        )
        self._assert_manifest_layout_rejected(manifest)

    def test_adjacent_triple_quotes_after_ordinary_strings_fail_closed(self) -> None:
        """普通字符串结尾的相邻三引号不能被误判为合法布局。"""
        for quote in ('"', "'"):
            with self.subTest(quote=quote):
                manifest = _manifest_text(COMPLETE_MEMBERS) + (
                    "description = " + quote + "x" + quote * 3 + "\n"
                )
                self._assert_manifest_layout_rejected(manifest)

    def test_duplicate_workspace_table_is_rejected_without_writes(self) -> None:
        """重复 [workspace] 不能让 parser 继续收敛 manifest。"""
        invalid_layout = _manifest_text(COMPLETE_MEMBERS) + '[workspace]\nresolver = "2"\n'
        self._assert_manifest_layout_rejected(invalid_layout)

    def test_workspace_array_table_is_rejected_without_writes(self) -> None:
        """[[workspace]] 不能被当作普通 workspace 语法忽略。"""
        invalid_layout = _manifest_text(COMPLETE_MEMBERS) + "[[workspace]]\nresolver = \"2\"\n"
        self._assert_manifest_layout_rejected(invalid_layout)

    def test_duplicate_workspace_members_key_is_rejected_without_writes(self) -> None:
        """重复 bare members key 不能绕过 workspace 数组唯一性检查。"""
        invalid_layout = _manifest_text(COMPLETE_MEMBERS) + "members = [\n]\n"
        self._assert_manifest_layout_rejected(invalid_layout)

    def test_workspace_members_table_is_rejected_without_writes(self) -> None:
        """[workspace.members] 与 members 数组冲突时必须拒绝。"""
        invalid_layout = _manifest_text(COMPLETE_MEMBERS) + "[workspace.members]\nvalue = true\n"
        self._assert_manifest_layout_rejected(invalid_layout)

    def test_quoted_members_key_is_rejected_without_writes(self) -> None:
        """quoted members key 不能绕过 workspace members 重复检测。"""
        invalid_layout = _manifest_text(COMPLETE_MEMBERS) + '"members" = []\n'
        self._assert_manifest_layout_rejected(invalid_layout)

    def test_literal_quoted_members_key_is_rejected_without_writes(self) -> None:
        """literal quoted members key 也不能绕过 workspace members 检查。"""
        invalid_layout = _manifest_text(COMPLETE_MEMBERS) + "'members' = []\n"
        self._assert_manifest_layout_rejected(invalid_layout)

    def test_quoted_dotted_workspace_members_keys_are_rejected_without_writes(self) -> None:
        """workspace.members 的 quoted/dotted 等价写法必须在两种模式拒绝。"""
        dotted_keys = (
            'workspace."members" = []\n',
            '"workspace".members = []\n',
            "workspace.'members' = []\n",
            "'workspace'.members = []\n",
            '"workspace"."members" = []\n',
            "'workspace'.'members' = []\n",
            '"work\\u0073pace".members = []\n',
            'workspace."mem\\u0062ers" = []\n',
            'workspace . "members" = []\n',
            '"workspace" . members = []\n',
        )
        for dotted_key in dotted_keys:
            with self.subTest(dotted_key=dotted_key):
                self._assert_manifest_layout_rejected(
                    dotted_key + _manifest_text(COMPLETE_MEMBERS)
                )

    def test_quoted_dotted_workspace_members_tables_are_rejected_without_writes(self) -> None:
        """quoted dotted workspace.members table 变体不能改变严格布局门禁。"""
        table_variants = (
            '[workspace."members"]\nvalue = true\n',
            '["workspace".members]\nvalue = true\n',
            "[workspace.'members']\nvalue = true\n",
            "['workspace'.'members']\nvalue = true\n",
        )
        for table in table_variants:
            with self.subTest(table=table):
                self._assert_manifest_layout_rejected(
                    _manifest_text(COMPLETE_MEMBERS) + table
                )

    def test_workspace_table_whitespace_variant_is_normalized(self) -> None:
        """唯一的 [ workspace ] 表头按 workspace 归一化且保持合法布局语义。"""
        for mode in ("--check", "--apply"):
            with self.subTest(mode=mode):
                temporary, root = self._workspace(COMPLETE_MEMBERS)
                try:
                    manifest = root / "Cargo.toml"
                    manifest.write_text(
                        manifest.read_text(encoding="utf-8").replace(
                            "[workspace]\n", "[ workspace ]\n", 1
                        ),
                        encoding="utf-8",
                    )
                    state = root / "FORK_BASE_REV"
                    before = manifest.read_bytes()
                    before_state = state.read_bytes()
                    result = self._run(root, mode)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(manifest.read_bytes(), before)
                    self.assertEqual(state.read_bytes(), before_state)
                finally:
                    temporary.cleanup()

    def test_workspace_table_whitespace_and_top_level_key_conflicts_fail_closed(self) -> None:
        """workspace 表头空白变体与顶层同名 key 不能绕过冲突检查。"""
        invalid_layouts = (
            _manifest_text(COMPLETE_MEMBERS) + "[ workspace ]\nresolver = \"2\"\n",
            _manifest_text(COMPLETE_MEMBERS) + "[workspace . members]\nvalue = true\n",
            "workspace = true\n" + _manifest_text(COMPLETE_MEMBERS),
            '"workspace" = true\n' + _manifest_text(COMPLETE_MEMBERS),
            "'workspace' = true\n" + _manifest_text(COMPLETE_MEMBERS),
        )
        for invalid_layout in invalid_layouts:
            with self.subTest(invalid_layout=invalid_layout):
                self._assert_manifest_layout_rejected(invalid_layout)

    def test_unicode_equivalent_quoted_members_key_fails_closed(self) -> None:
        """basic string Unicode escape 不能伪装成重复的 members key。"""
        invalid_layout = _manifest_text(COMPLETE_MEMBERS) + '"mem\\u0062ers" = []\n'
        self._assert_manifest_layout_rejected(invalid_layout)

    def test_members_dotted_key_with_whitespace_fails_closed(self) -> None:
        """members 的额外 dotted key 即使带合法空白也必须拒绝。"""
        invalid_layout = _manifest_text(COMPLETE_MEMBERS) + "members . extra = true\n"
        self._assert_manifest_layout_rejected(invalid_layout)

    def test_unicode_escaped_stale_probe_member_fails_closed(self) -> None:
        """无法安全归一化的 Unicode-escaped stale member 不得让 --check 假绿。"""
        escaped_probe = "crates/efflab/efflab-pr0-http-\\u0070robe"
        self._assert_manifest_layout_rejected(
            _manifest_text(COMPLETE_MEMBERS).replace(
                "\n]\n", f'\n    "{escaped_probe}",\n]\n', 1
            )
        )

    def test_last_member_without_comma_remains_clean_and_byte_stable(self) -> None:
        """合法无尾逗号布局在 check/apply 中保持成功且不改写状态。"""
        for mode in ("--check", "--apply"):
            with self.subTest(mode=mode):
                temporary, root = self._workspace(COMPLETE_MEMBERS)
                try:
                    manifest = root / "Cargo.toml"
                    state = root / "FORK_BASE_REV"
                    manifest.write_text(
                        manifest.read_text(encoding="utf-8").replace(
                            f'    "{MEMBERS[-1]}",\n', f'    "{MEMBERS[-1]}"\n'
                        ),
                        encoding="utf-8",
                    )
                    before = manifest.read_bytes()
                    before_state = state.read_bytes()
                    result = self._run(root, mode)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(manifest.read_bytes(), before)
                    self.assertEqual(state.read_bytes(), before_state)
                finally:
                    temporary.cleanup()

    def test_ordinary_strings_and_comments_do_not_trigger_workspace_parser(self) -> None:
        """普通字符串与注释中的 workspace 标记不应改变合法 manifest。"""
        temporary, root = self._workspace(COMPLETE_MEMBERS)
        try:
            manifest = root / "Cargo.toml"
            valid = manifest.read_text(encoding="utf-8")
            prefix = (
                '# """ [workspace] members = [\n'
                'basic = "[workspace] members = [\\\"fake\\\"] # text"\n'
                "literal = '[workspace] members = [\\\"fake\\\"]'\n"
            )
            manifest.write_text(prefix + valid, encoding="utf-8")
            before = manifest.read_bytes()
            result = self._check(root)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(manifest.read_bytes(), before)
        finally:
            temporary.cleanup()

    def test_last_workspace_member_without_comma_is_managed_and_preserved(self) -> None:
        """合法无尾逗号的最后一项可被识别，并只收敛受管 stale probe。"""
        for mode in ("--check", "--apply"):
            with self.subTest(mode=mode):
                temporary, root = self._workspace(COMPLETE_MEMBERS)
                try:
                    manifest = root / "Cargo.toml"
                    content = manifest.read_text(encoding="utf-8")
                    manifest.write_text(
                        content.replace("\n]\n", f'\n    "{STALE_PROBE}"\n]\n', 1),
                        encoding="utf-8",
                    )
                    probe = root / STALE_PROBE
                    probe.mkdir(parents=True)
                    sentinel = probe / "sentinel.txt"
                    probe_content = b"probe content must remain\n"
                    sentinel.write_bytes(probe_content)
                    before = manifest.read_bytes()
                    state = root / "FORK_BASE_REV"
                    before_state = state.read_bytes()
                    result = self._run(root, mode)
                    if mode == "--check":
                        self.assertEqual(result.returncode, 1, result.stderr)
                        self.assertIn("stale member", result.stdout)
                        self.assertEqual(manifest.read_bytes(), before)
                    else:
                        self.assertEqual(result.returncode, 0, result.stderr)
                        self.assertNotIn(f'"{STALE_PROBE}"', manifest.read_text(encoding="utf-8"))
                    self.assertTrue(probe.is_dir())
                    self.assertEqual(sentinel.read_bytes(), probe_content)
                    self.assertEqual(state.read_bytes(), before_state)
                finally:
                    temporary.cleanup()

    def test_missing_anchor_fails_closed_and_preserves_manifest_and_state(self) -> None:
        temporary, root = self._workspace(
            ("upstream/other-member",), fork_base_rev=STALE_BASE_REV
        )
        try:
            manifest = root / "Cargo.toml"
            state = root / "FORK_BASE_REV"
            before = manifest.read_bytes()
            before_state = state.read_bytes()
            result = self._apply(root)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("anchor", (result.stderr + result.stdout).lower())
            self.assertEqual(manifest.read_bytes(), before)
            self.assertEqual(state.read_bytes(), before_state)
            for member in MEMBERS:
                self.assertNotIn(member, manifest.read_text(encoding="utf-8"))
        finally:
            temporary.cleanup()

    def test_missing_anchor_does_not_create_state_file(self) -> None:
        temporary, root = self._workspace(("upstream/other-member",), fork_base_rev=None)
        try:
            manifest = root / "Cargo.toml"
            before = manifest.read_bytes()
            result = self._apply(root)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse((root / "FORK_BASE_REV").exists())
            self.assertEqual(manifest.read_bytes(), before)
        finally:
            temporary.cleanup()

    def test_duplicate_anchor_fails_closed_and_preserves_manifest_and_state(self) -> None:
        temporary, root = self._workspace(
            (ANCHOR, ANCHOR), fork_base_rev=STALE_BASE_REV
        )
        try:
            manifest = root / "Cargo.toml"
            state = root / "FORK_BASE_REV"
            before = manifest.read_bytes()
            before_state = state.read_bytes()
            result = self._apply(root)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("anchor", (result.stderr + result.stdout).lower())
            self.assertEqual(manifest.read_bytes(), before)
            self.assertEqual(state.read_bytes(), before_state)
        finally:
            temporary.cleanup()

    def test_single_anchor_still_allows_atomic_member_apply(self) -> None:
        temporary, root = self._workspace((ANCHOR,))
        try:
            manifest = root / "Cargo.toml"
            result = self._apply(root)
            self.assertEqual(result.returncode, 0, result.stderr)
            content = manifest.read_text(encoding="utf-8")
            for member in MEMBERS:
                self.assertEqual(content.count(f'"{member}"'), 1)
            self.assertEqual(content.count(f'"{ANCHOR}"'), 1)
        finally:
            temporary.cleanup()

    def test_member_count_accepts_workspace_inline_comment(self) -> None:
        temporary, root = self._workspace((ANCHOR,))
        try:
            manifest = root / "Cargo.toml"
            content = manifest.read_text(encoding="utf-8")
            content = content.replace(
                "members = [\n",
                f'members = [ # opening inline comment\n    # similar "{ANCHOR}", ] in a comment\n',
            )
            manifest.write_text(
                content.replace(f'    "{ANCHOR}",', f'    "{ANCHOR}", # keep anchor'),
                encoding="utf-8",
            )
            result = self._apply(root)
            self.assertEqual(result.returncode, 0, result.stderr)
            content = manifest.read_text(encoding="utf-8")
            for member in MEMBERS:
                self.assertEqual(content.count(f'"{member}"'), 1)
            self.assertIn(f'"{ANCHOR}", # keep anchor', content)
        finally:
            temporary.cleanup()

    def test_member_count_ignores_comments_and_strings_outside_workspace_array(self) -> None:
        temporary, root = self._workspace(
            ("upstream/other-member",),
            fork_base_rev=STALE_BASE_REV,
            manifest_extra=(
                f'\n#     "{ANCHOR}", # comment only\n'
                "[package.metadata]\n"
                "paths = [\n"
                f'    "{ANCHOR}",\n'
                "]\n"
            ),
        )
        try:
            manifest = root / "Cargo.toml"
            state = root / "FORK_BASE_REV"
            before = manifest.read_bytes()
            before_state = state.read_bytes()
            result = self._apply(root)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("anchor", (result.stderr + result.stdout).lower())
            self.assertEqual(manifest.read_bytes(), before)
            self.assertEqual(state.read_bytes(), before_state)
        finally:
            temporary.cleanup()


if __name__ == "__main__":
    unittest.main()
