"""白盒单元测试：轮转分组统计 RotateGroupManager。

为什么这里必须白盒（见 tests/README.md 原则二）：分组统计的增量扣减
（一个文件从所有关联分组中同时扣除）是外部观察成本畸高的契约——
从目录树反推需要精心构造多规则交叉场景，而直接验证统计接口一目了然。

轮转对目录树的最终影响由 test_greybox_rotate.py 从入口验证。
"""

from src.core.handlers.rotate import RotateGroupManager

GLOBAL = ("global", "global")


class TestGroupCreation:
    def test_global_group_holds_size_and_count_limits(self):
        mgr = RotateGroupManager(1000, 10, {}, {})
        assert mgr.group_stats[GLOBAL] == {
            "size": 0,
            "count": 0,
            "size_limit": 1000,
            "count_limit": 10,
        }

    def test_pattern_groups_created_from_rules(self):
        mgr = RotateGroupManager(0, 0, {"*.log": 1000}, {"*.tmp": 5})
        assert ("size", "pattern", "*.log") in mgr.group_stats
        assert ("count", "pattern", "*.tmp") in mgr.group_stats
        assert len(mgr.group_stats) == 3  # global + 2 个模式组


class TestAddFile:
    def test_every_file_counted_in_global_group(self):
        mgr = RotateGroupManager(1000, 10, {"*.log": 100}, {})
        groups = mgr.add_file("other.txt", 50)
        assert groups == [GLOBAL]
        assert mgr.group_stats[GLOBAL]["size"] == 50
        assert mgr.group_stats[GLOBAL]["count"] == 1

    def test_file_joins_every_matching_pattern_group(self):
        mgr = RotateGroupManager(0, 0, {"*.log": 100, "app.*": 200}, {"*.log": 5})
        groups = mgr.add_file("app.log", 50)
        assert groups == [
            GLOBAL,
            ("size", "pattern", "*.log"),
            ("size", "pattern", "app.*"),
            ("count", "pattern", "*.log"),
        ]

    def test_stats_accumulate_across_files(self):
        mgr = RotateGroupManager(2000, 10, {"*.log": 1000}, {})
        mgr.add_file("a.log", 300)
        mgr.add_file("b.log", 400)
        assert mgr.group_stats[GLOBAL]["size"] == 700
        assert mgr.group_stats[("size", "pattern", "*.log")]["size"] == 700
        assert mgr.group_stats[GLOBAL]["count"] == 2


class TestExceededJudgement:
    def test_global_size_exceeded(self):
        mgr = RotateGroupManager(100, 10, {}, {})
        mgr.add_file("f.txt", 200)
        assert mgr.is_any_group_exceeded()

    def test_global_count_exceeded(self):
        mgr = RotateGroupManager(0, 3, {}, {})
        for i in range(4):
            mgr.add_file(f"f{i}.txt", 1)
        assert mgr.is_any_group_exceeded()

    def test_pattern_group_exceeded_alone(self):
        mgr = RotateGroupManager(0, 0, {"*.log": 100}, {})
        mgr.add_file("app.log", 200)
        assert mgr.is_any_group_exceeded()

    def test_exact_limit_not_exceeded(self):
        """恰好等于限制不算超限——边界契约。"""
        mgr = RotateGroupManager(100, 0, {}, {})
        mgr.add_file("f.txt", 100)
        assert not mgr.is_any_group_exceeded()

    def test_zero_limit_never_triggers(self):
        """limit 为 0 表示不限制，不参与超限判定。"""
        mgr = RotateGroupManager(0, 0, {}, {})
        mgr.add_file("f.txt", 99999)
        for i in range(100):
            mgr.add_file(f"f{i}.txt", 1)
        assert not mgr.is_any_group_exceeded()

    def test_no_group_exceeded(self):
        mgr = RotateGroupManager(1000, 10, {"*.log": 500}, {"*.tmp": 3})
        mgr.add_file("app.log", 100)
        mgr.add_file("t.tmp", 1)
        assert not mgr.is_any_group_exceeded()


class TestFileNeedsRotation:
    """is_file_needs_rotation 只看该文件所属的分组是否超限——
    决定"处理哪些文件能消除超限"。"""

    def test_file_in_exceeded_group_needs_rotation(self):
        mgr = RotateGroupManager(100, 10, {}, {})
        mgr.add_file("big.txt", 200)
        groups = mgr.add_file("small.txt", 10)
        assert mgr.is_file_needs_rotation(groups)

    def test_file_not_in_exceeded_group_skipped(self):
        mgr = RotateGroupManager(500, 10, {}, {})
        mgr.add_file("a.txt", 100)
        groups = mgr.add_file("b.txt", 50)
        assert not mgr.is_file_needs_rotation(groups)

    def test_file_only_in_non_exceeded_groups_skipped(self):
        """全局和别的分组超限不关它的事——只轮转能消除超限的文件。"""
        mgr = RotateGroupManager(0, 0, {"*.log": 500, "*.txt": 100}, {})
        mgr.add_file("a.txt", 200)  # 令 *.txt 组超限
        groups = mgr.add_file("b.log", 10)
        assert not mgr.is_file_needs_rotation(groups)


class TestRemoveFile:
    """多分组扣减契约：一个文件从它所属的所有分组中同时扣除统计值。"""

    def test_removal_deducts_from_all_associated_groups(self):
        mgr = RotateGroupManager(2000, 10, {"*.log": 500}, {})
        groups = mgr.add_file("app.log", 300)
        mgr.remove_file(groups, 300)
        assert mgr.group_stats[GLOBAL]["size"] == 0
        assert mgr.group_stats[GLOBAL]["count"] == 0
        assert mgr.group_stats[("size", "pattern", "*.log")]["size"] == 0
        assert mgr.group_stats[("size", "pattern", "*.log")]["count"] == 0

    def test_single_removal_lifts_multiple_group_exceedances(self):
        """同一文件命中多个超限分组时，移除一次即同时解除所有超限。"""
        mgr = RotateGroupManager(0, 0, {"*.log": 100, "app.*": 100}, {})
        groups = mgr.add_file("app.log", 200)
        assert mgr.is_any_group_exceeded()
        mgr.remove_file(groups, 200)
        assert not mgr.is_any_group_exceeded()

    def test_removal_leaves_other_files_intact(self):
        mgr = RotateGroupManager(1000, 10, {}, {})
        mgr.add_file("a.txt", 100)
        groups = mgr.add_file("b.txt", 200)
        mgr.remove_file(groups, 200)
        assert mgr.group_stats[GLOBAL]["size"] == 100
        assert mgr.group_stats[GLOBAL]["count"] == 1


class TestPathMatching:
    def test_pattern_matches_subdirectory_path(self):
        mgr = RotateGroupManager(0, 0, {"logs/*.log": 100}, {})
        groups = mgr.add_file("logs/app.log", 50)
        assert ("size", "pattern", "logs/*.log") in groups

    def test_pattern_does_not_match_other_directory(self):
        mgr = RotateGroupManager(0, 0, {"logs/*.log": 100}, {})
        groups = mgr.add_file("other/app.log", 50)
        assert ("size", "pattern", "logs/*.log") not in groups

    def test_case_insensitive(self):
        mgr = RotateGroupManager(0, 0, {"*.LOG": 100}, {})
        groups = mgr.add_file("APP.LOG", 50)
        assert ("size", "pattern", "*.LOG") in groups

    def test_backslash_path_normalized(self):
        mgr = RotateGroupManager(0, 0, {"logs/*.log": 100}, {})
        groups = mgr.add_file("logs\\app.log", 50)
        assert ("size", "pattern", "logs/*.log") in groups
