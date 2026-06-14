from src.core.types import FileAction
from src.core.filters import FileFilterPolicy


class TestKeepRules:
    def test_keep_lt_match(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {"lt": {"*.log": "1MB"}},
                "delete_rules": {},
                "preferred_rule": "keep",
                "is_whitelist_mode": False,
            }
        )
        assert policy.decide("app.log", 500 * 1024) == FileAction.SKIP

    def test_keep_lt_no_match(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {"lt": {"*.log": "1MB"}},
                "delete_rules": {},
                "preferred_rule": "keep",
                "is_whitelist_mode": False,
            }
        )
        assert policy.decide("app.log", 2 * 1024 * 1024) == FileAction.TRANSFER

    def test_keep_ge_match(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {"ge": {"*.log": "1MB"}},
                "delete_rules": {},
                "preferred_rule": "keep",
                "is_whitelist_mode": False,
            }
        )
        assert policy.decide("app.log", 2 * 1024 * 1024) == FileAction.SKIP

    def test_keep_ge_no_match(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {"ge": {"*.log": "1MB"}},
                "delete_rules": {},
                "preferred_rule": "keep",
                "is_whitelist_mode": False,
            }
        )
        assert policy.decide("app.log", 500 * 1024) == FileAction.TRANSFER

    def test_keep_ge_exact_boundary(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {"ge": {"*.log": "1MB"}},
                "delete_rules": {},
                "preferred_rule": "keep",
                "is_whitelist_mode": False,
            }
        )
        assert policy.decide("app.log", 1 * 1024 * 1024) == FileAction.SKIP

    def test_keep_lt_exact_boundary(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {"lt": {"*.log": "1MB"}},
                "delete_rules": {},
                "preferred_rule": "keep",
                "is_whitelist_mode": False,
            }
        )
        assert policy.decide("app.log", 1 * 1024 * 1024) == FileAction.TRANSFER


class TestDeleteRules:
    def test_delete_lt_match(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {},
                "delete_rules": {"lt": {"*.tmp": "1KB"}},
                "preferred_rule": "keep",
                "is_whitelist_mode": False,
            }
        )
        assert policy.decide("temp.tmp", 500) == FileAction.DELETE

    def test_delete_lt_no_match(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {},
                "delete_rules": {"lt": {"*.tmp": "1KB"}},
                "preferred_rule": "keep",
                "is_whitelist_mode": False,
            }
        )
        assert policy.decide("temp.tmp", 2000) == FileAction.TRANSFER

    def test_delete_ge_minus_one_unconditional(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {},
                "delete_rules": {"ge": {"*.tmp": "-1"}},
                "preferred_rule": "keep",
                "is_whitelist_mode": False,
            }
        )
        assert policy.decide("temp.tmp", 0) == FileAction.DELETE
        assert policy.decide("large.tmp", 10 * 1024 * 1024) == FileAction.DELETE

    def test_delete_ge_minus_one_with_lazy_callable(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {},
                "delete_rules": {"ge": {"*.tmp": "-1"}},
                "preferred_rule": "keep",
                "is_whitelist_mode": False,
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
                "keep_rules": {},
                "delete_rules": {"ge": {"*.log": "10MB"}},
                "preferred_rule": "keep",
                "is_whitelist_mode": False,
            }
        )
        assert policy.decide("big.log", 20 * 1024 * 1024) == FileAction.DELETE


class TestConflictResolution:
    def test_both_match_prefer_keep(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {"lt": {"*.log": "10MB"}},
                "delete_rules": {"lt": {"*.log": "1MB"}},
                "preferred_rule": "keep",
                "is_whitelist_mode": False,
            }
        )
        assert policy.decide("app.log", 500 * 1024) == FileAction.SKIP

    def test_both_match_prefer_delete(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {"lt": {"*.log": "10MB"}},
                "delete_rules": {"lt": {"*.log": "1MB"}},
                "preferred_rule": "delete",
                "is_whitelist_mode": False,
            }
        )
        assert policy.decide("app.log", 500 * 1024) == FileAction.DELETE


class TestNoMatch:
    def test_no_rules_match(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {"lt": {"*.log": "1MB"}},
                "delete_rules": {"ge": {"*.log": "100MB"}},
                "preferred_rule": "keep",
                "is_whitelist_mode": False,
            }
        )
        assert policy.decide("app.txt", 5 * 1024 * 1024) == FileAction.TRANSFER

    def test_empty_rules(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {},
                "delete_rules": {},
                "preferred_rule": "keep",
                "is_whitelist_mode": False,
            }
        )
        assert policy.decide("any.file", 1000) == FileAction.TRANSFER


class TestGlobPatterns:
    def test_wildcard_pattern(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {"lt": {"*.log": "1MB"}},
                "delete_rules": {},
                "preferred_rule": "keep",
                "is_whitelist_mode": False,
            }
        )
        assert policy.decide("app.log", 500 * 1024) == FileAction.SKIP
        assert policy.decide("error.log", 500 * 1024) == FileAction.SKIP

    def test_question_mark_pattern(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {"lt": {"app.???": "1MB"}},
                "delete_rules": {},
                "preferred_rule": "keep",
                "is_whitelist_mode": False,
            }
        )
        assert policy.decide("app.log", 500 * 1024) == FileAction.SKIP
        assert policy.decide("app.txt", 500 * 1024) == FileAction.SKIP
        assert policy.decide("app.exe", 500 * 1024) == FileAction.SKIP
        assert policy.decide("app.html", 500 * 1024) == FileAction.TRANSFER

    def test_nested_path_pattern(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {"lt": {"subdir/*.log": "1MB"}},
                "delete_rules": {},
                "preferred_rule": "keep",
                "is_whitelist_mode": False,
            }
        )
        assert policy.decide("subdir/app.log", 500 * 1024) == FileAction.SKIP
        assert policy.decide("app.log", 500 * 1024) == FileAction.TRANSFER


class TestDirectoryRules:
    def test_dir_pattern_lt(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {"lt": {"backup/": "100MB"}},
                "delete_rules": {},
                "preferred_rule": "keep",
                "is_whitelist_mode": False,
            }
        )
        assert policy.decide("backup", 50 * 1024 * 1024, is_dir=True) == FileAction.SKIP

    def test_dir_pattern_ge(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {},
                "delete_rules": {"ge": {"cache/": "1GB"}},
                "preferred_rule": "keep",
                "is_whitelist_mode": False,
            }
        )
        assert policy.decide("cache", 2 * 1024**3, is_dir=True) == FileAction.DELETE

    def test_dir_pattern_no_match_for_files(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {"lt": {"backup/": "100MB"}},
                "delete_rules": {},
                "preferred_rule": "keep",
                "is_whitelist_mode": False,
            }
        )
        assert (
            policy.decide("backup", 50 * 1024 * 1024, is_dir=False)
            == FileAction.TRANSFER
        )


class TestLazyEvaluation:
    def test_callable_not_called_when_no_match(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {"lt": {"*.zip": "1MB"}},
                "delete_rules": {},
                "preferred_rule": "keep",
                "is_whitelist_mode": False,
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
                "keep_rules": {"lt": {"*.log": "1MB"}},
                "delete_rules": {},
                "preferred_rule": "keep",
                "is_whitelist_mode": False,
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
                "keep_rules": {"lt": {"*.log": "1MB"}},
                "delete_rules": {"lt": {"*.log": "100KB"}},
                "preferred_rule": "keep",
                "is_whitelist_mode": False,
            }
        )
        keep_calls = []
        # delete_calls = []

        class Tracker:
            def __init__(self, calls):
                self.calls = calls

            def __call__(self):
                self.calls.append(True)
                return 50 * 1024

        policy.decide("app.log", Tracker(keep_calls))
        assert len(keep_calls) == 2


class TestWhitelistMode:
    def test_file_in_whitelist(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {},
                "delete_rules": {},
                "preferred_rule": "keep",
                "whitelist_rules": {"lt": {"*.doc": "10MB"}},
                "is_whitelist_mode": True,
            }
        )
        assert policy.decide("doc.doc", 5 * 1024 * 1024) == FileAction.TRANSFER

    def test_file_not_in_whitelist(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {},
                "delete_rules": {},
                "preferred_rule": "keep",
                "whitelist_rules": {"lt": {"*.doc": "10MB"}},
                "is_whitelist_mode": True,
            }
        )
        assert policy.decide("other.txt", 1000) == FileAction.SKIP

    def test_dir_in_whitelist(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {},
                "delete_rules": {},
                "preferred_rule": "keep",
                "whitelist_rules": {"lt": {"docs/": "100MB"}},
                "is_whitelist_mode": True,
            }
        )
        assert (
            policy.decide("docs", 50 * 1024 * 1024, is_dir=True) == FileAction.TRANSFER
        )

    def test_dir_not_in_whitelist(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {},
                "delete_rules": {},
                "preferred_rule": "keep",
                "whitelist_rules": {"lt": {"docs/": "100MB"}},
                "is_whitelist_mode": True,
            }
        )
        assert (
            policy.decide("other", 50 * 1024 * 1024, is_dir=True) == FileAction.TRANSFER
        )

    def test_child_inherits_parent_whitelist(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {},
                "delete_rules": {},
                "preferred_rule": "keep",
                "whitelist_rules": {"lt": {"docs/": "100MB"}},
                "is_whitelist_mode": True,
            }
        )
        policy.decide("docs", 50 * 1024 * 1024, is_dir=True)
        assert policy.decide("docs/sub/file.txt", 1000) == FileAction.TRANSFER

    def test_child_not_inherit_unrelated_dir(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {},
                "delete_rules": {},
                "preferred_rule": "keep",
                "whitelist_rules": {"lt": {"docs/": "100MB"}},
                "is_whitelist_mode": True,
            }
        )
        policy.decide("docs", 50 * 1024 * 1024, is_dir=True)
        assert policy.decide("other/file.txt", 1000) == FileAction.SKIP

    def test_whitelist_with_backslash_path(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {},
                "delete_rules": {},
                "preferred_rule": "keep",
                "whitelist_rules": {"lt": {"docs/": "100MB"}},
                "is_whitelist_mode": True,
            }
        )
        policy.decide("docs", 50 * 1024 * 1024, is_dir=True)
        assert policy.decide("docs\\sub\\file.txt", 1000) == FileAction.TRANSFER

    def test_whitelist_deeply_nested_child(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {},
                "delete_rules": {},
                "preferred_rule": "keep",
                "whitelist_rules": {"lt": {"a/": "100MB"}},
                "is_whitelist_mode": True,
            }
        )
        policy.decide("a", 50 * 1024 * 1024, is_dir=True)
        assert policy.decide("a/b/c/d/file.txt", 1000) == FileAction.TRANSFER

    def test_whitelist_fails_size_check_not_whitelisted(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {},
                "delete_rules": {},
                "preferred_rule": "keep",
                "whitelist_rules": {"ge": {"*.doc": "10MB"}},
                "is_whitelist_mode": True,
            }
        )
        assert policy.decide("doc.doc", 5 * 1024 * 1024) == FileAction.SKIP

    def test_whitelist_ge_match(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {},
                "delete_rules": {},
                "preferred_rule": "keep",
                "whitelist_rules": {"ge": {"*.doc": "10MB"}},
                "is_whitelist_mode": True,
            }
        )
        assert policy.decide("doc.doc", 20 * 1024 * 1024) == FileAction.TRANSFER

    def test_whitelist_with_keep_delete_rules(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {"lt": {"*.log": "1MB"}},
                "delete_rules": {"ge": {"*.log": "100MB"}},
                "preferred_rule": "keep",
                "whitelist_rules": {"lt": {"*.log": "500KB"}},
                "is_whitelist_mode": True,
            }
        )
        assert policy.decide("small.log", 100 * 1024) == FileAction.SKIP

    def test_whitelist_keep_wins_over_whitelist_check(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {"lt": {"*.log": "1MB"}},
                "delete_rules": {},
                "preferred_rule": "keep",
                "whitelist_rules": {"ge": {"*.log": "10MB"}},
                "is_whitelist_mode": True,
            }
        )
        assert policy.decide("app.log", 500 * 1024) == FileAction.SKIP


class TestZeroSize:
    def test_zero_size_match_lt(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {},
                "delete_rules": {"lt": {"*.tmp": "1KB"}},
                "preferred_rule": "keep",
                "is_whitelist_mode": False,
            }
        )
        assert policy.decide("empty.tmp", 0) == FileAction.DELETE

    def test_zero_size_no_match_ge(self):
        policy = FileFilterPolicy(
            {
                "keep_rules": {},
                "delete_rules": {"ge": {"*.log": "1MB"}},
                "preferred_rule": "keep",
                "is_whitelist_mode": False,
            }
        )
        assert policy.decide("empty.log", 0) == FileAction.TRANSFER
