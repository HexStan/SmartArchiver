"""白盒单元测试：规则引擎 FileFilterPolicy。

为什么这里必须白盒（见 tests/README.md 原则二）：本文件覆盖的契约
从 handler.execute() 入口无法观察或观察成本畸高——

- 惰性求值：大小计算是否被触发、触发几次，是性能契约，只能靠计数探针验证；
- include 继承：included_dirs 在遍历中累积，目录必须先于子文件被评估（顺序契约）；
- 阈值边界：lt 严格小于 / ge 大于等于；
- 父目录规则级联：parent_dir_sizes 参数的语义。

规则系统对目录树的最终影响由 test_greybox_move_copy.py 从入口验证。
"""

from src.core.filters import FileFilterPolicy
from src.core.types import FileAction

TRANSFER = FileAction.TRANSFER
SKIP = FileAction.SKIP
DELETE = FileAction.DELETE

MB = 1024 * 1024
KB = 1024


def build_policy(include=None, exclude=None, delete=None):
    return FileFilterPolicy(
        {
            "include_rules": include or {},
            "exclude_rules": exclude or {},
            "delete_rules": delete or {},
        }
    )


class SizeSpy:
    """惰性求值探针：记录大小被计算了几次（白盒专用，外部无此观察点）。"""

    def __init__(self, size):
        self.size = size
        self.calls = 0

    def __call__(self):
        self.calls += 1
        return self.size


class TestPipelineOrder:
    """流水线顺序：include → exclude → delete → TRANSFER，命中即停止。"""

    def test_not_included_returns_skip_even_if_delete_matches(self):
        policy = build_policy(
            include={"ge": {"*.doc": "10MB"}},
            delete={"ge": {"*.txt": "-1"}},
        )
        assert policy.decide("app.txt", 500) == SKIP

    def test_exclude_returns_skip_even_if_delete_matches(self):
        policy = build_policy(
            exclude={"lt": {"*.log": "10MB"}},
            delete={"lt": {"*.log": "1MB"}},
        )
        assert policy.decide("app.log", 500 * KB) == SKIP

    def test_delete_match_returns_delete(self):
        policy = build_policy(delete={"ge": {"*.tmp": "-1"}})
        assert policy.decide("temp.tmp", 123) == DELETE

    def test_no_rule_matches_returns_transfer(self):
        policy = build_policy(
            exclude={"lt": {"*.log": "1MB"}},
            delete={"ge": {"*.log": "100MB"}},
        )
        assert policy.decide("app.txt", 5 * MB) == TRANSFER

    def test_empty_rules_transfer_everything(self):
        policy = build_policy()
        assert policy.decide("any.file", 1000) == TRANSFER

    def test_readme_example_exclude_shields_from_delete(self):
        """README 文档化的用法：删除所有 .tmp 但保护 important.tmp。"""
        policy = build_policy(
            exclude={"ge": {"important.tmp": "-1"}},
            delete={"ge": {"*.tmp": "-1"}},
        )
        assert policy.decide("important.tmp", 100) == SKIP
        assert policy.decide("other.tmp", 100) == DELETE


class TestThresholdBoundary:
    """阈值边界契约：lt 为严格小于，ge 为大于等于。"""

    def test_lt_at_exact_threshold_misses(self):
        policy = build_policy(exclude={"lt": {"*.log": "1MB"}})
        assert policy.decide("app.log", 1 * MB) == TRANSFER

    def test_lt_below_threshold_hits(self):
        policy = build_policy(exclude={"lt": {"*.log": "1MB"}})
        assert policy.decide("app.log", 1 * MB - 1) == SKIP

    def test_ge_at_exact_threshold_hits(self):
        policy = build_policy(exclude={"ge": {"*.log": "1MB"}})
        assert policy.decide("app.log", 1 * MB) == SKIP

    def test_ge_below_threshold_misses(self):
        policy = build_policy(exclude={"ge": {"*.log": "1MB"}})
        assert policy.decide("app.log", 1 * MB - 1) == TRANSFER

    def test_zero_size_with_lt_threshold_hits(self):
        policy = build_policy(delete={"lt": {"*.tmp": "1KB"}})
        assert policy.decide("empty.tmp", 0) == DELETE

    def test_zero_size_with_ge_threshold_misses(self):
        policy = build_policy(delete={"ge": {"*.log": "1MB"}})
        assert policy.decide("empty.log", 0) == TRANSFER


class TestUnconditionalMinusOne:
    """ge 阈值 -1 无条件命中（含零大小）；lt 阈值 -1 永不命中。"""

    def test_ge_minus_one_matches_any_size(self):
        policy = build_policy(delete={"ge": {"*.tmp": "-1"}})
        assert policy.decide("a.tmp", 0) == DELETE
        assert policy.decide("b.tmp", 10 * MB) == DELETE

    def test_lt_minus_one_never_matches(self):
        policy = build_policy(delete={"lt": {"*.tmp": "-1"}})
        assert policy.decide("a.tmp", 0) == TRANSFER


class TestLazyEvaluation:
    """惰性求值契约：名称不匹配不求大小；每组规则至多求值一次；
    ge -1 命中时完全不求值。目录大小的递归计算开销大，这是性能契约。"""

    def test_size_not_computed_when_name_mismatches(self):
        policy = build_policy(exclude={"lt": {"*.zip": "1MB"}})
        spy = SizeSpy(0)
        assert policy.decide("other.txt", spy) == TRANSFER
        assert spy.calls == 0

    def test_size_computed_once_when_name_matches(self):
        policy = build_policy(exclude={"lt": {"*.log": "1MB"}})
        spy = SizeSpy(500 * KB)
        assert policy.decide("app.log", spy) == SKIP
        assert spy.calls == 1

    def test_ge_minus_one_never_computes_size(self):
        policy = build_policy(delete={"ge": {"*.tmp": "-1"}})
        spy = SizeSpy(0)
        assert policy.decide("temp.tmp", spy) == DELETE
        assert spy.calls == 0

    def test_size_computed_once_per_rule_set(self):
        """include / exclude / delete 各自独立求值一次，组内多条规则共享一次求值。"""
        policy = build_policy(
            include={"ge": {"*.log": "1MB"}},
            exclude={"lt": {"*.log": "1MB"}},
            delete={"lt": {"*.log": "3MB"}},
        )
        spy = SizeSpy(2 * MB)
        assert policy.decide("app.log", spy) == DELETE
        assert spy.calls == 3


class TestDirVsFileRules:
    """模式末尾斜杠区分目标：带 / 只匹配目录，不带 / 只匹配文件。"""

    def test_dir_rule_matches_directory(self):
        policy = build_policy(exclude={"lt": {"backup/": "100MB"}})
        assert policy.decide("backup", 50 * MB, is_dir=True) == SKIP

    def test_dir_rule_ignores_file_with_same_name(self):
        policy = build_policy(exclude={"lt": {"backup/": "100MB"}})
        assert policy.decide("backup", 50 * MB, is_dir=False) == TRANSFER

    def test_file_rule_ignores_directory(self):
        policy = build_policy(exclude={"ge": {"*.log": "1MB"}})
        assert policy.decide("app.log", 50 * MB, is_dir=True) == TRANSFER

    def test_dir_ge_minus_one_unconditional(self):
        policy = build_policy(delete={"ge": {"cache/": "-1"}})
        assert policy.decide("cache", 2 * 1024**3, is_dir=True) == DELETE


class TestIncludeInheritance:
    """include 继承：命中 include 的目录，其子项自动纳入。
    included_dirs 在遍历中累积——目录必须先被 decide，子文件才能继承（顺序契约）。"""

    def test_dir_always_passes_include_check(self):
        """目录即使不匹配 include 也放行，否则无法遍历到匹配的子项。"""
        policy = build_policy(include={"ge": {"*.mp4": "-1"}})
        assert policy.decide("mydir", 0, is_dir=True) == TRANSFER

    def test_matching_dir_registers_children_inheritance(self):
        policy = build_policy(include={"lt": {"docs/": "100MB"}})
        assert policy.decide("docs", 50 * MB, is_dir=True) == TRANSFER
        assert policy.decide("docs/sub/file.txt", 1000) == TRANSFER

    def test_inheritance_requires_dir_visited_first(self):
        """顺序契约：没有先 decide 目录，子文件不能继承纳入状态。"""
        policy = build_policy(include={"lt": {"docs/": "100MB"}})
        assert policy.decide("docs/sub/file.txt", 1000) == SKIP

    def test_child_of_unmatched_dir_not_included(self):
        policy = build_policy(include={"lt": {"docs/": "100MB"}})
        policy.decide("docs", 50 * MB, is_dir=True)
        assert policy.decide("other/file.txt", 1000) == SKIP

    def test_backslash_path_inherits(self):
        policy = build_policy(include={"lt": {"docs/": "100MB"}})
        policy.decide("docs", 50 * MB, is_dir=True)
        assert policy.decide("docs\\sub\\file.txt", 1000) == TRANSFER

    def test_deeply_nested_child_inherits(self):
        policy = build_policy(include={"lt": {"a/": "100MB"}})
        policy.decide("a", 50 * MB, is_dir=True)
        assert policy.decide("a/b/c/d/file.txt", 1000) == TRANSFER

    def test_no_include_rules_means_all_included(self):
        policy = build_policy(exclude={"lt": {"*.log": "1MB"}})
        assert policy.decide("other.txt", 1000) == TRANSFER

    def test_file_failing_include_size_check_is_skipped(self):
        policy = build_policy(include={"ge": {"*.doc": "10MB"}})
        assert policy.decide("doc.doc", 5 * MB) == SKIP


class TestParentDirCascade:
    """父目录规则级联：父目录的 exclude/delete 规则按父目录大小作用于子文件。
    parent_dir_sizes 由调用方（轮转模式）在扫描阶段预计算。"""

    def test_parent_exclude_cascades_to_file(self):
        policy = build_policy(exclude={"ge": {"backup/": "-1"}})
        sizes = {"backup": 1000}
        assert policy.decide("backup/file.txt", 500, parent_dir_sizes=sizes) == SKIP

    def test_parent_delete_cascades_to_file(self):
        policy = build_policy(delete={"ge": {"backup/": "-1"}})
        sizes = {"backup": 1000}
        assert policy.decide("backup/file.txt", 500, parent_dir_sizes=sizes) == DELETE

    def test_parent_exclude_beats_parent_delete(self):
        policy = build_policy(
            exclude={"ge": {"data/": "-1"}},
            delete={"ge": {"data/": "-1"}},
        )
        sizes = {"data": 5000}
        assert policy.decide("data/file.txt", 500, parent_dir_sizes=sizes) == SKIP

    def test_parent_threshold_uses_parent_size(self):
        """级联判断用父目录整体大小，而不是文件自身大小。"""
        policy = build_policy(exclude={"lt": {"backup/": "1KB"}})
        hit = policy.decide(
            "backup/file.txt", 500, parent_dir_sizes={"backup": 500}
        )
        miss = policy.decide(
            "backup/file.txt", 500, parent_dir_sizes={"backup": 2000}
        )
        assert hit == SKIP
        assert miss == TRANSFER

    def test_self_exclude_beats_parent_delete(self):
        policy = build_policy(
            exclude={"lt": {"*.txt": "10MB"}},
            delete={"ge": {"backup/": "-1"}},
        )
        sizes = {"backup": 1000}
        assert policy.decide("backup/file.txt", 500, parent_dir_sizes=sizes) == SKIP

    def test_deeply_nested_parent_cascade(self):
        policy = build_policy(exclude={"ge": {"backup/": "-1"}})
        sizes = {"backup": 10000, "backup/sub": 5000}
        assert (
            policy.decide("backup/sub/deep/file.txt", 500, parent_dir_sizes=sizes)
            == SKIP
        )

    def test_no_parent_entry_no_cascade(self):
        policy = build_policy(exclude={"ge": {"other/": "-1"}})
        sizes = {"backup": 1000}
        assert policy.decide("backup/file.txt", 500, parent_dir_sizes=sizes) == TRANSFER

    def test_none_parent_sizes_no_cascade(self):
        policy = build_policy(exclude={"ge": {"backup/": "-1"}})
        assert policy.decide("backup/file.txt", 500) == TRANSFER
        assert policy.decide("backup/file.txt", 500, parent_dir_sizes=None) == TRANSFER

    def test_dir_decision_ignores_parent_sizes(self):
        """目录自身的决策不受 parent_dir_sizes 影响。"""
        policy = build_policy(exclude={"lt": {"backup/": "1KB"}})
        sizes = {"parent": 0}
        assert (
            policy.decide("backup", 500, is_dir=True, parent_dir_sizes=sizes) == SKIP
        )


class TestGlobSemantics:
    """通配符语义（fnmatch 之上的一层路径归一化）。"""

    def test_single_level_pattern_matches_basename_at_any_depth(self):
        policy = build_policy(exclude={"ge": {"*.log": "-1"}})
        assert policy.decide("a/b/c/app.log", 100) == SKIP

    def test_multi_level_pattern_anchored_at_source_root(self):
        policy = build_policy(exclude={"ge": {"logs/*.log": "-1"}})
        assert policy.decide("logs/app.log", 100) == SKIP
        assert policy.decide("other/app.log", 100) == TRANSFER

    def test_star_in_multi_level_pattern_crosses_directories(self):
        """fnmatch 的 * 不感知路径分隔符——"logs/*.log" 能匹配 logs/a/b.log。
        钉死这一语义，防止将来误改为按单级匹配。"""
        policy = build_policy(exclude={"ge": {"logs/*.log": "-1"}})
        assert policy.decide("logs/a/b.log", 100) == SKIP

    def test_question_mark_matches_single_char(self):
        policy = build_policy(exclude={"lt": {"app.???": "1MB"}})
        assert policy.decide("app.log", 500 * KB) == SKIP
        assert policy.decide("app.html", 500 * KB) == TRANSFER

    def test_case_insensitive(self):
        policy = build_policy(exclude={"ge": {"*.LOG": "-1"}})
        assert policy.decide("APP.LOG", 100) == SKIP

    def test_backslash_normalized_in_name_and_pattern(self):
        policy = build_policy(exclude={"ge": {"logs\\*.log": "-1"}})
        assert policy.decide("logs\\a.log", 100) == SKIP
