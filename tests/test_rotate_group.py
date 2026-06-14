from src.core.handlers.rotate import RotateGroupManager


class TestRotateGroupManagerInit:
    def test_global_group_created(self):
        mgr = RotateGroupManager(1000, 10, {}, {})
        assert ("global", "global") in mgr.group_stats
        assert mgr.group_stats[("global", "global")]["size_limit"] == 1000
        assert mgr.group_stats[("global", "global")]["count_limit"] == 10
        assert mgr.group_stats[("global", "global")]["size"] == 0
        assert mgr.group_stats[("global", "global")]["count"] == 0

    def test_size_pattern_groups_created(self):
        mgr = RotateGroupManager(0, 0, {"*.log": 1000}, {})
        assert ("size", "pattern", "*.log") in mgr.group_stats
        assert mgr.group_stats[("size", "pattern", "*.log")]["limit"] == 1000

    def test_count_pattern_groups_created(self):
        mgr = RotateGroupManager(0, 0, {}, {"*.tmp": 5})
        assert ("count", "pattern", "*.tmp") in mgr.group_stats
        assert mgr.group_stats[("count", "pattern", "*.tmp")]["limit"] == 5

    def test_multiple_groups_created(self):
        mgr = RotateGroupManager(5000, 20, {"*.log": 1000, "*.zip": 500}, {"*.tmp": 5})
        assert len(mgr.group_stats) == 4


class TestAddFile:
    def test_add_file_updates_global(self):
        mgr = RotateGroupManager(1000, 10, {}, {})
        mgr.add_file("file.txt", 500)
        assert mgr.group_stats[("global", "global")]["size"] == 500
        assert mgr.group_stats[("global", "global")]["count"] == 1

    def test_add_file_matches_size_pattern(self):
        mgr = RotateGroupManager(0, 0, {"*.log": 1000}, {})
        groups = mgr.add_file("app.log", 600)
        assert ("size", "pattern", "*.log") in groups
        assert mgr.group_stats[("size", "pattern", "*.log")]["size"] == 600
        assert mgr.group_stats[("size", "pattern", "*.log")]["count"] == 1

    def test_add_file_matches_count_pattern(self):
        mgr = RotateGroupManager(0, 0, {}, {"*.tmp": 5})
        groups = mgr.add_file("temp.tmp", 100)
        assert ("count", "pattern", "*.tmp") in groups
        assert mgr.group_stats[("count", "pattern", "*.tmp")]["size"] == 100
        assert mgr.group_stats[("count", "pattern", "*.tmp")]["count"] == 1

    def test_add_file_always_in_global(self):
        mgr = RotateGroupManager(1000, 10, {"*.log": 100}, {})
        groups = mgr.add_file("other.txt", 50)
        assert groups == [("global", "global")]

    def test_add_file_multiple_patterns(self):
        mgr = RotateGroupManager(0, 0, {"*.log": 100, "app.*": 200}, {"*.log": 5})
        groups = mgr.add_file("app.log", 50)
        assert ("global", "global") in groups
        assert ("size", "pattern", "*.log") in groups
        assert ("size", "pattern", "app.*") in groups
        assert ("count", "pattern", "*.log") in groups
        assert len(groups) == 4

    def test_add_file_returns_group_ids(self):
        mgr = RotateGroupManager(1000, 10, {"*.log": 100}, {})
        groups = mgr.add_file("app.log", 50)
        assert groups == [("global", "global"), ("size", "pattern", "*.log")]

    def test_add_multiple_files(self):
        mgr = RotateGroupManager(2000, 10, {"*.log": 1000}, {})
        mgr.add_file("a.log", 300)
        mgr.add_file("b.log", 400)
        assert mgr.group_stats[("global", "global")]["size"] == 700
        assert mgr.group_stats[("global", "global")]["count"] == 2
        assert mgr.group_stats[("size", "pattern", "*.log")]["size"] == 700
        assert mgr.group_stats[("size", "pattern", "*.log")]["count"] == 2


class TestIsAnyGroupExceeded:
    def test_global_size_exceeded(self):
        mgr = RotateGroupManager(100, 10, {}, {})
        mgr.add_file("f.txt", 200)
        assert mgr.is_any_group_exceeded()

    def test_global_size_not_exceeded(self):
        mgr = RotateGroupManager(1000, 10, {}, {})
        mgr.add_file("f.txt", 200)
        assert not mgr.is_any_group_exceeded()

    def test_global_count_exceeded(self):
        mgr = RotateGroupManager(0, 3, {}, {})
        for i in range(4):
            mgr.add_file(f"f{i}.txt", 1)
        assert mgr.is_any_group_exceeded()

    def test_global_count_not_exceeded(self):
        mgr = RotateGroupManager(0, 5, {}, {})
        for i in range(3):
            mgr.add_file(f"f{i}.txt", 1)
        assert not mgr.is_any_group_exceeded()

    def test_size_pattern_exceeded(self):
        mgr = RotateGroupManager(0, 0, {"*.log": 100}, {})
        mgr.add_file("app.log", 200)
        assert mgr.is_any_group_exceeded()

    def test_count_pattern_exceeded(self):
        mgr = RotateGroupManager(0, 0, {}, {"*.tmp": 2})
        for i in range(3):
            mgr.add_file(f"f{i}.tmp", 1)
        assert mgr.is_any_group_exceeded()

    def test_none_exceeded(self):
        mgr = RotateGroupManager(1000, 10, {"*.log": 500}, {"*.tmp": 3})
        mgr.add_file("app.log", 100)
        mgr.add_file("t.tmp", 1)
        assert not mgr.is_any_group_exceeded()

    def test_zero_limit_not_exceeded(self):
        mgr = RotateGroupManager(0, 0, {}, {})
        mgr.add_file("f.txt", 99999)
        for i in range(100):
            mgr.add_file(f"f{i}.txt", 1)
        assert not mgr.is_any_group_exceeded()

    def test_exact_limit_not_exceeded(self):
        mgr = RotateGroupManager(100, 0, {}, {})
        mgr.add_file("f.txt", 100)
        assert not mgr.is_any_group_exceeded()


class TestIsFileNeedsRotation:
    def test_file_in_exceeded_group(self):
        mgr = RotateGroupManager(100, 10, {}, {})
        mgr.add_file("big.txt", 200)
        groups = mgr.add_file("small.txt", 10)
        assert mgr.is_file_needs_rotation(groups)

    def test_file_not_in_exceeded_group(self):
        mgr = RotateGroupManager(500, 10, {}, {})
        mgr.add_file("a.txt", 100)
        groups = mgr.add_file("b.txt", 50)
        assert not mgr.is_file_needs_rotation(groups)

    def test_file_in_exceeded_size_pattern(self):
        mgr = RotateGroupManager(0, 0, {"*.log": 100}, {})
        mgr.add_file("a.log", 200)
        groups = mgr.add_file("b.log", 10)
        assert mgr.is_file_needs_rotation(groups)

    def test_file_only_in_non_exceeded_groups(self):
        mgr = RotateGroupManager(0, 0, {"*.log": 500, "*.txt": 100}, {})
        mgr.add_file("a.txt", 200)
        groups = mgr.add_file("b.log", 10)
        assert not mgr.is_file_needs_rotation(groups)


class TestRemoveFile:
    def test_remove_file_updates_all_groups(self):
        mgr = RotateGroupManager(2000, 10, {"*.log": 500}, {})
        groups = mgr.add_file("app.log", 300)
        mgr.remove_file(groups, 300)
        assert mgr.group_stats[("global", "global")]["size"] == 0
        assert mgr.group_stats[("global", "global")]["count"] == 0
        assert mgr.group_stats[("size", "pattern", "*.log")]["size"] == 0
        assert mgr.group_stats[("size", "pattern", "*.log")]["count"] == 0

    def test_remove_one_of_multiple_files(self):
        mgr = RotateGroupManager(1000, 10, {}, {})
        mgr.add_file("a.txt", 100)
        groups = mgr.add_file("b.txt", 200)
        mgr.remove_file(groups, 200)
        assert mgr.group_stats[("global", "global")]["size"] == 100
        assert mgr.group_stats[("global", "global")]["count"] == 1

    def test_remove_file_reduces_group_stats(self):
        mgr = RotateGroupManager(500, 10, {}, {})
        mgr.add_file("a.txt", 200)
        mgr.add_file("b.txt", 200)
        groups = mgr.add_file("c.txt", 200)
        assert mgr.is_any_group_exceeded()
        mgr.remove_file(groups, 200)
        assert not mgr.is_any_group_exceeded()


class TestPathMatching:
    def test_pattern_matches_subdirectory(self):
        mgr = RotateGroupManager(0, 0, {"logs/*.log": 100}, {})
        groups = mgr.add_file("logs/app.log", 50)
        assert ("size", "pattern", "logs/*.log") in groups

    def test_pattern_does_not_match_different_dir(self):
        mgr = RotateGroupManager(0, 0, {"logs/*.log": 100}, {})
        groups = mgr.add_file("other/app.log", 50)
        assert ("size", "pattern", "logs/*.log") not in groups

    def test_pattern_case_insensitive(self):
        mgr = RotateGroupManager(0, 0, {"*.LOG": 100}, {})
        groups = mgr.add_file("APP.LOG", 50)
        assert ("size", "pattern", "*.LOG") in groups

    def test_pattern_with_windows_backslash(self):
        mgr = RotateGroupManager(0, 0, {"logs/*.log": 100}, {})
        groups = mgr.add_file("logs\\app.log", 50)
        assert ("size", "pattern", "logs/*.log") in groups
