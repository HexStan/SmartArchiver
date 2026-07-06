from src.core.types import FileAction
from src.core.filters import FileFilterPolicy


class TestExcludeRules:
    def test_exclude_lt_match(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {"lt": {"*.log": "1MB"}},
                "delete_rules": {},
            }
        )
        assert policy.decide("app.log", 500 * 1024) == FileAction.SKIP

    def test_exclude_lt_no_match(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {"lt": {"*.log": "1MB"}},
                "delete_rules": {},
            }
        )
        assert policy.decide("app.log", 2 * 1024 * 1024) == FileAction.TRANSFER

    def test_exclude_ge_match(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {"ge": {"*.log": "1MB"}},
                "delete_rules": {},
            }
        )
        assert policy.decide("app.log", 2 * 1024 * 1024) == FileAction.SKIP

    def test_exclude_ge_no_match(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {"ge": {"*.log": "1MB"}},
                "delete_rules": {},
            }
        )
        assert policy.decide("app.log", 500 * 1024) == FileAction.TRANSFER

    def test_exclude_ge_exact_boundary(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {"ge": {"*.log": "1MB"}},
                "delete_rules": {},
            }
        )
        assert policy.decide("app.log", 1 * 1024 * 1024) == FileAction.SKIP

    def test_exclude_lt_exact_boundary(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {"lt": {"*.log": "1MB"}},
                "delete_rules": {},
            }
        )
        assert policy.decide("app.log", 1 * 1024 * 1024) == FileAction.TRANSFER


class TestDeleteRules:
    def test_delete_lt_match(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {},
                "delete_rules": {"lt": {"*.tmp": "1KB"}},
            }
        )
        assert policy.decide("temp.tmp", 500) == FileAction.DELETE

    def test_delete_lt_no_match(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {},
                "delete_rules": {"lt": {"*.tmp": "1KB"}},
            }
        )
        assert policy.decide("temp.tmp", 2000) == FileAction.TRANSFER

    def test_delete_ge_minus_one_unconditional(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {},
                "delete_rules": {"ge": {"*.tmp": "-1"}},
            }
        )
        assert policy.decide("temp.tmp", 0) == FileAction.DELETE
        assert policy.decide("large.tmp", 10 * 1024 * 1024) == FileAction.DELETE

    def test_delete_ge_minus_one_with_lazy_callable(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {},
                "delete_rules": {"ge": {"*.tmp": "-1"}},
            }
        )
        called = []

        def get_size():
            called.append(True)
            return 0

        assert policy.decide("temp.tmp", get_size) == FileAction.DELETE
        assert len(called) == 0

    def test_delete_ge_match(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {},
                "delete_rules": {"ge": {"*.log": "10MB"}},
            }
        )
        assert policy.decide("big.log", 20 * 1024 * 1024) == FileAction.DELETE


class TestExcludeWinsOverDelete:
    def test_exclude_checked_before_delete(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {"lt": {"*.log": "10MB"}},
                "delete_rules": {"lt": {"*.log": "1MB"}},
            }
        )
        assert policy.decide("app.log", 500 * 1024) == FileAction.SKIP


class TestNoMatch:
    def test_no_rules_match(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {"lt": {"*.log": "1MB"}},
                "delete_rules": {"ge": {"*.log": "100MB"}},
            }
        )
        assert policy.decide("app.txt", 5 * 1024 * 1024) == FileAction.TRANSFER

    def test_empty_rules(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {},
                "delete_rules": {},
            }
        )
        assert policy.decide("any.file", 1000) == FileAction.TRANSFER


class TestGlobPatterns:
    def test_wildcard_pattern(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {"lt": {"*.log": "1MB"}},
                "delete_rules": {},
            }
        )
        assert policy.decide("app.log", 500 * 1024) == FileAction.SKIP
        assert policy.decide("error.log", 500 * 1024) == FileAction.SKIP

    def test_question_mark_pattern(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {"lt": {"app.???": "1MB"}},
                "delete_rules": {},
            }
        )
        assert policy.decide("app.log", 500 * 1024) == FileAction.SKIP
        assert policy.decide("app.txt", 500 * 1024) == FileAction.SKIP
        assert policy.decide("app.exe", 500 * 1024) == FileAction.SKIP
        assert policy.decide("app.html", 500 * 1024) == FileAction.TRANSFER

    def test_nested_path_pattern(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {"lt": {"subdir/*.log": "1MB"}},
                "delete_rules": {},
            }
        )
        assert policy.decide("subdir/app.log", 500 * 1024) == FileAction.SKIP
        assert policy.decide("app.log", 500 * 1024) == FileAction.TRANSFER


class TestDirectoryRules:
    def test_dir_exclude_lt(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {"lt": {"backup/": "100MB"}},
                "delete_rules": {},
            }
        )
        assert policy.decide("backup", 50 * 1024 * 1024, is_dir=True) == FileAction.SKIP

    def test_dir_delete_ge(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {},
                "delete_rules": {"ge": {"cache/": "1GB"}},
            }
        )
        assert policy.decide("cache", 2 * 1024**3, is_dir=True) == FileAction.DELETE

    def test_dir_pattern_no_match_for_files(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {"lt": {"backup/": "100MB"}},
                "delete_rules": {},
            }
        )
        assert (
            policy.decide("backup", 50 * 1024 * 1024, is_dir=False)
            == FileAction.TRANSFER
        )

    def test_dir_always_passes_include_check(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {"ge": {"*.mp4": "-1"}},
                "exclude_rules": {},
                "delete_rules": {},
            }
        )
        assert policy.decide("mydir", 0, is_dir=True) == FileAction.TRANSFER


class TestLazyEvaluation:
    def test_callable_not_called_when_no_match(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {"lt": {"*.zip": "1MB"}},
                "delete_rules": {},
            }
        )
        called = []

        def get_size():
            called.append(True)
            return 0

        assert policy.decide("other.txt", get_size) == FileAction.TRANSFER
        assert len(called) == 0

    def test_callable_called_when_pattern_matches(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {"lt": {"*.log": "1MB"}},
                "delete_rules": {},
            }
        )
        called = []

        def get_size():
            called.append(True)
            return 500 * 1024

        assert policy.decide("app.log", get_size) == FileAction.SKIP
        assert len(called) == 1

    def test_callable_called_once_per_ruleset(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {"lt": {"*.log": "1MB"}},
                "delete_rules": {"lt": {"*.log": "100KB"}},
            }
        )
        calls = []

        class Tracker:
            def __init__(self, calls):
                self.calls = calls

            def __call__(self):
                self.calls.append(True)
                return 50 * 1024

        policy.decide("app.log", Tracker(calls))
        assert len(calls) == 1


class TestIncludeRules:
    def test_file_in_include(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {"lt": {"*.doc": "10MB"}},
                "exclude_rules": {},
                "delete_rules": {},
            }
        )
        assert policy.decide("doc.doc", 5 * 1024 * 1024) == FileAction.TRANSFER

    def test_file_not_in_include(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {"lt": {"*.doc": "10MB"}},
                "exclude_rules": {},
                "delete_rules": {},
            }
        )
        assert policy.decide("other.txt", 1000) == FileAction.SKIP

    def test_dir_in_include_traverses(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {"lt": {"docs/": "100MB"}},
                "exclude_rules": {},
                "delete_rules": {},
            }
        )
        assert (
            policy.decide("docs", 50 * 1024 * 1024, is_dir=True) == FileAction.TRANSFER
        )

    def test_dir_always_passes_include_even_no_match(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {"lt": {"docs/": "100MB"}},
                "exclude_rules": {},
                "delete_rules": {},
            }
        )
        assert (
            policy.decide("other", 50 * 1024 * 1024, is_dir=True) == FileAction.TRANSFER
        )

    def test_child_inherits_parent_include(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {"lt": {"docs/": "100MB"}},
                "exclude_rules": {},
                "delete_rules": {},
            }
        )
        policy.decide("docs", 50 * 1024 * 1024, is_dir=True)
        assert policy.decide("docs/sub/file.txt", 1000) == FileAction.TRANSFER

    def test_child_not_inherit_unrelated_dir(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {"lt": {"docs/": "100MB"}},
                "exclude_rules": {},
                "delete_rules": {},
            }
        )
        policy.decide("docs", 50 * 1024 * 1024, is_dir=True)
        assert policy.decide("other/file.txt", 1000) == FileAction.SKIP

    def test_include_with_backslash_path(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {"lt": {"docs/": "100MB"}},
                "exclude_rules": {},
                "delete_rules": {},
            }
        )
        policy.decide("docs", 50 * 1024 * 1024, is_dir=True)
        assert policy.decide("docs\\sub\\file.txt", 1000) == FileAction.TRANSFER

    def test_include_deeply_nested_child(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {"lt": {"a/": "100MB"}},
                "exclude_rules": {},
                "delete_rules": {},
            }
        )
        policy.decide("a", 50 * 1024 * 1024, is_dir=True)
        assert policy.decide("a/b/c/d/file.txt", 1000) == FileAction.TRANSFER

    def test_include_fails_size_check_not_included(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {"ge": {"*.doc": "10MB"}},
                "exclude_rules": {},
                "delete_rules": {},
            }
        )
        assert policy.decide("doc.doc", 5 * 1024 * 1024) == FileAction.SKIP

    def test_include_ge_match(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {"ge": {"*.doc": "10MB"}},
                "exclude_rules": {},
                "delete_rules": {},
            }
        )
        assert policy.decide("doc.doc", 20 * 1024 * 1024) == FileAction.TRANSFER

    def test_include_with_exclude_rules(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {"lt": {"*.log": "500KB"}},
                "exclude_rules": {"lt": {"*.log": "1MB"}},
                "delete_rules": {},
            }
        )
        assert policy.decide("small.log", 100 * 1024) == FileAction.SKIP

    def test_include_with_delete_rules(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {"lt": {"*.log": "500KB"}},
                "exclude_rules": {},
                "delete_rules": {"ge": {"*.log": "-1"}},
            }
        )
        assert policy.decide("small.log", 100 * 1024) == FileAction.DELETE

    def test_include_with_exclude_and_delete(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {"lt": {"*.log": "500KB"}},
                "exclude_rules": {"lt": {"*.log": "1MB"}},
                "delete_rules": {"ge": {"*.log": "100MB"}},
            }
        )
        assert policy.decide("small.log", 100 * 1024) == FileAction.SKIP

    def test_no_include_rules_all_included(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {"lt": {"*.log": "1MB"}},
                "delete_rules": {},
            }
        )
        assert policy.decide("other.txt", 1000) == FileAction.TRANSFER

    def test_empty_include_rules_all_included(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {"lt": {}, "ge": {}},
                "exclude_rules": {},
                "delete_rules": {},
            }
        )
        assert policy.decide("any.txt", 1000) == FileAction.TRANSFER


class TestZeroSize:
    def test_zero_size_match_lt(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {},
                "delete_rules": {"lt": {"*.tmp": "1KB"}},
            }
        )
        assert policy.decide("empty.tmp", 0) == FileAction.DELETE

    def test_zero_size_no_match_ge(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {},
                "delete_rules": {"ge": {"*.log": "1MB"}},
            }
        )
        assert policy.decide("empty.log", 0) == FileAction.TRANSFER


class TestParentDirCascade:
    def test_file_excluded_by_parent_exclude_rule(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {"ge": {"backup/": "-1"}},
                "delete_rules": {},
            }
        )
        parent_dir_sizes = {"backup": 1000}
        assert (
            policy.decide("backup/file.txt", 500, parent_dir_sizes=parent_dir_sizes)
            == FileAction.SKIP
        )

    def test_file_deleted_by_parent_delete_rule(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {},
                "delete_rules": {"ge": {"backup/": "-1"}},
            }
        )
        parent_dir_sizes = {"backup": 1000}
        assert (
            policy.decide("backup/file.txt", 500, parent_dir_sizes=parent_dir_sizes)
            == FileAction.DELETE
        )

    def test_parent_exclude_wins_over_parent_delete(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {"ge": {"data/": "-1"}},
                "delete_rules": {"ge": {"data/": "-1"}},
            }
        )
        parent_dir_sizes = {"data": 5000}
        assert (
            policy.decide("data/file.txt", 500, parent_dir_sizes=parent_dir_sizes)
            == FileAction.SKIP
        )

    def test_parent_dir_size_check_exclude_lt_exceeded(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {"lt": {"backup/": "1KB"}},
                "delete_rules": {},
            }
        )
        parent_dir_sizes = {"backup": 2000}
        assert (
            policy.decide("backup/file.txt", 500, parent_dir_sizes=parent_dir_sizes)
            == FileAction.TRANSFER
        )

    def test_parent_dir_size_check_exclude_lt_matched(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {"lt": {"backup/": "1KB"}},
                "delete_rules": {},
            }
        )
        parent_dir_sizes = {"backup": 500}
        assert (
            policy.decide("backup/file.txt", 500, parent_dir_sizes=parent_dir_sizes)
            == FileAction.SKIP
        )

    def test_file_exclude_self_parent_delete(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {"lt": {"*.txt": "10MB"}},
                "delete_rules": {"ge": {"backup/": "-1"}},
            }
        )
        parent_dir_sizes = {"backup": 1000}
        assert (
            policy.decide("backup/file.txt", 500, parent_dir_sizes=parent_dir_sizes)
            == FileAction.SKIP
        )

    def test_deeply_nested_parent_exclude(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {"ge": {"backup/": "-1"}},
                "delete_rules": {},
            }
        )
        parent_dir_sizes = {"backup": 10000, "backup/sub": 5000}
        assert (
            policy.decide(
                "backup/sub/deep/file.txt", 500, parent_dir_sizes=parent_dir_sizes
            )
            == FileAction.SKIP
        )

    def test_no_matching_parent_dirs(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {"ge": {"other/": "-1"}},
                "delete_rules": {},
            }
        )
        parent_dir_sizes = {"backup": 1000}
        assert (
            policy.decide("backup/file.txt", 500, parent_dir_sizes=parent_dir_sizes)
            == FileAction.TRANSFER
        )

    def test_parent_dir_sizes_none(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {"ge": {"backup/": "-1"}},
                "delete_rules": {},
            }
        )
        assert policy.decide("backup/file.txt", 500) == FileAction.TRANSFER
        assert (
            policy.decide("backup/file.txt", 500, parent_dir_sizes=None)
            == FileAction.TRANSFER
        )

    def test_is_dir_true_ignores_parent_dir_sizes(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {},
                "exclude_rules": {"lt": {"backup/": "1KB"}},
                "delete_rules": {},
            }
        )
        parent_dir_sizes = {"parent": 0}
        assert (
            policy.decide("backup", 500, is_dir=True, parent_dir_sizes=parent_dir_sizes)
            == FileAction.SKIP
        )


class TestPipelineOrder:
    def test_include_exclude_pipeline(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {"ge": {"*.log": "-1"}},
                "exclude_rules": {"lt": {"*.log": "1MB"}},
                "delete_rules": {},
            }
        )
        assert policy.decide("app.log", 500 * 1024) == FileAction.SKIP
        assert policy.decide("app.log", 2 * 1024 * 1024) == FileAction.TRANSFER

    def test_include_delete_pipeline(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {"ge": {"*.log": "-1"}},
                "exclude_rules": {},
                "delete_rules": {"lt": {"*.log": "1MB"}},
            }
        )
        assert policy.decide("app.log", 500 * 1024) == FileAction.DELETE
        assert policy.decide("app.log", 2 * 1024 * 1024) == FileAction.TRANSFER

    def test_full_pipeline_all_match(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {"ge": {"*.log": "-1"}},
                "exclude_rules": {"lt": {"*.log": "10MB"}},
                "delete_rules": {"lt": {"*.log": "1MB"}},
            }
        )
        assert policy.decide("app.log", 500 * 1024) == FileAction.SKIP

    def test_not_included_file_skipped_even_if_matches_delete(self):
        policy = FileFilterPolicy(
            {
                "include_rules": {"ge": {"*.doc": "10MB"}},
                "exclude_rules": {},
                "delete_rules": {"ge": {"*.txt": "-1"}},
            }
        )
        assert policy.decide("app.txt", 500) == FileAction.SKIP
