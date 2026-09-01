

## 2026-09-01 continuation：Cargo 锁等待诊断回归

上一轮认证重试发现，Cargo 查询不存在的固定 denylist 候选时，stderr 可能先出现 `Blocking waiting for file lock on package cache`，再出现唯一的 `package ID specification ... did not match any packages`。原有严格全量匹配因此把正常的“无反向路径”误报为 fatal。

- 根因定位：`_MISSING_PACKAGE_RE` 已由 `_is_missing_reverse_candidate()` 实际调用；问题在于 stderr 还包含 Cargo 的固定锁等待状态行，而不是正则未接线。
- TDD：新增 `test_reverse_dependency_ignores_cargo_lock_wait_before_missing_candidate`；修复前按预期抛出 `ClosureGateError`，修复后 focused test、closure 专项 69 tests 和 Python 全量 110 tests 均通过。随后真实并发复现发现 Cargo 会为锁等待行加 4 个前导空格，第一版解析仍会间歇性误报。
- 二次 TDD：先新增覆盖连续 3 条带 4 个前导空格的锁等待行的测试并确认 RED，再修复 `_is_missing_reverse_candidate()`：仅允许固定 `_CARGO_PACKAGE_CACHE_LOCK_WAIT` 行两侧出现 ASCII 空格或制表符，并且只能位于唯一 missing 诊断之前；未知文本、错误位置、重复 missing、候选不一致和 stdout 路径仍 fail-closed。
- 二次修复验证：2 个 focused 回归和 Python 全量 71 tests 均通过；受控 8 路真实 Cargo 并发查询全部捕获到缩进锁等待状态，8/8 被 classifier 正确识别为候选不存在；修复后串行 arm64 release certification 退出码 0，报告 `binary_scanned=true`、`edge_kind=normal,build`、`denylist_hits=[]`。
- 串行仍是发布认证的推荐执行方式：并发 Cargo 会制造锁等待状态噪声；解析器只兼容已确认的固定状态行，不应改成宽泛 `strip()` 或模糊搜索。

x86_64 macOS、Windows runner/真机、matched tuple 和真实产品 bundle 仍未验证；closure-only 不能替代 release-certification。
