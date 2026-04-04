import os
import sys
import pytest
import tempfile
import shutil
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core import (
    match_pattern,
    FileAction,
    MoverStats,
    FileFilterPolicy,
    get_unique_dest,
    get_dir_size_and_mtime,
    clean_empty_dirs,
)


class TestMatchPattern:
    def test_exact_match(self):
        assert match_pattern("example.txt", "example.txt") is True
        assert match_pattern("example.txt", "other.txt") is False

    def test_wildcard_all(self):
        assert match_pattern("anything", "*") is True
        assert match_pattern("", "*") is True

    def test_prefix_match(self):
        assert match_pattern("example.txt", "example*") is True
        assert match_pattern("example_backup.txt", "example*") is True
        assert match_pattern("other.txt", "example*") is False

    def test_suffix_match(self):
        assert match_pattern("file.txt", "*.txt") is True
        assert match_pattern("document.txt", "*.txt") is True
        assert match_pattern("file.doc", "*.txt") is False

    def test_contains_match(self):
        assert match_pattern("my_example_file.txt", "*example*") is True
        assert match_pattern("example", "*example*") is True
        assert match_pattern("other.txt", "*example*") is False

    def test_case_insensitive(self):
        assert match_pattern("EXAMPLE.TXT", "example.txt") is True
        assert match_pattern("example.txt", "EXAMPLE.TXT") is True
        assert match_pattern("MyFile.TXT", "*.txt") is True

    def test_single_wildcard_edge_cases(self):
        assert match_pattern("", "**") is True
        assert match_pattern("a", "*a*") is True


class TestMoverStats:
    def test_initial_values(self):
        stats = MoverStats()
        assert stats.success == 0
        assert stats.error == 0
        assert stats.dropped == 0
        assert stats.kept == 0
        assert stats.deleted == 0
        assert stats.conflict_skipped == 0
        assert stats.locked_skipped == 0
        assert stats.total_bytes == 0

    def test_calculate_speed(self):
        stats = MoverStats()
        stats.total_bytes = 1024 * 1024

        duration_str, size_str, speed_str = stats.calculate_speed(0, 1)
        assert "1" in duration_str
        assert "MB" in size_str or "MiB" in size_str
        assert "/s" in speed_str

    def test_calculate_speed_zero_duration(self):
        stats = MoverStats()
        stats.total_bytes = 1024

        duration_str, size_str, speed_str = stats.calculate_speed(0, 0.0001)
        assert duration_str is not None
        assert speed_str is not None


class TestFileFilterPolicy:
    def test_whitelist_mode_transfer(self):
        config = {
            "is_whitelist_mode": True,
            "whitelist_rules": {"lt": {"*.txt": "ALL"}},
        }
        policy = FileFilterPolicy(config)
        assert policy.decide("file.txt", 100) == FileAction.TRANSFER

    def test_whitelist_mode_skip(self):
        config = {
            "is_whitelist_mode": True,
            "whitelist_rules": {"lt": {"*.txt": "ALL"}},
        }
        policy = FileFilterPolicy(config)
        assert policy.decide("file.pdf", 100) == FileAction.SKIP

    def test_delete_mode(self):
        config = {
            "is_whitelist_mode": False,
            "delete_rules": {"lt": {"*.tmp": "ALL"}},
            "keep_rules": {},
            "preferred_rule": "keep",
        }
        policy = FileFilterPolicy(config)
        assert policy.decide("file.tmp", 100) == FileAction.DELETE

    def test_keep_mode(self):
        config = {
            "is_whitelist_mode": False,
            "delete_rules": {},
            "keep_rules": {"lt": {"*.raw": "ALL"}},
            "preferred_rule": "keep",
        }
        policy = FileFilterPolicy(config)
        assert policy.decide("photo.raw", 100) == FileAction.SKIP

    def test_transfer_when_no_rules_match(self):
        config = {
            "is_whitelist_mode": False,
            "delete_rules": {},
            "keep_rules": {},
            "preferred_rule": "keep",
        }
        policy = FileFilterPolicy(config)
        assert policy.decide("file.txt", 100) == FileAction.TRANSFER

    def test_conflict_prefer_keep(self):
        config = {
            "is_whitelist_mode": False,
            "delete_rules": {"lt": {"*.txt": "ALL"}},
            "keep_rules": {"lt": {"*.txt": "ALL"}},
            "preferred_rule": "keep",
        }
        policy = FileFilterPolicy(config)
        assert policy.decide("file.txt", 100) == FileAction.SKIP

    def test_conflict_prefer_delete(self):
        config = {
            "is_whitelist_mode": False,
            "delete_rules": {"lt": {"*.txt": "ALL"}},
            "keep_rules": {"lt": {"*.txt": "ALL"}},
            "preferred_rule": "delete",
        }
        policy = FileFilterPolicy(config)
        assert policy.decide("file.txt", 100) == FileAction.DELETE

    def test_size_threshold_below(self):
        config = {
            "is_whitelist_mode": False,
            "delete_rules": {"lt": {"*.tmp": "1KB"}},
            "keep_rules": {},
            "preferred_rule": "keep",
        }
        policy = FileFilterPolicy(config)
        assert policy.decide("file.tmp", 500) == FileAction.DELETE

    def test_size_threshold_above(self):
        config = {
            "is_whitelist_mode": False,
            "delete_rules": {"lt": {"*.tmp": "1KB"}},
            "keep_rules": {},
            "preferred_rule": "keep",
        }
        policy = FileFilterPolicy(config)
        assert policy.decide("file.tmp", 2000) == FileAction.TRANSFER

    def test_dir_rules(self):
        config = {
            "is_whitelist_mode": False,
            "delete_rules": {"lt": {"*temp*/": "ALL"}},
            "keep_rules": {},
            "preferred_rule": "keep",
        }
        policy = FileFilterPolicy(config)
        assert policy.decide("my_temp_dir", 1000, is_dir=True) == FileAction.DELETE
        assert policy.decide("important_dir", 1000, is_dir=True) == FileAction.TRANSFER


class TestGetUniqueDest:
    def test_no_conflict(self, tmp_path):
        dest = tmp_path / "file.txt"
        result = get_unique_dest(str(dest))
        assert result == str(dest)

    def test_single_conflict(self, tmp_path):
        dest = tmp_path / "file.txt"
        dest.write_text("original")

        result = get_unique_dest(str(dest))
        assert result == str(tmp_path / "file-1.txt")

    def test_multiple_conflicts(self, tmp_path):
        (tmp_path / "file.txt").write_text("v1")
        (tmp_path / "file-1.txt").write_text("v2")
        (tmp_path / "file-2.txt").write_text("v3")

        result = get_unique_dest(str(tmp_path / "file.txt"))
        assert result == str(tmp_path / "file-3.txt")

    def test_with_extension(self, tmp_path):
        (tmp_path / "document.pdf").write_text("original")

        result = get_unique_dest(str(tmp_path / "document.pdf"))
        assert result == str(tmp_path / "document-1.pdf")


class TestGetDirSizeAndMtime:
    def test_empty_directory(self, tmp_path):
        size, mtime = get_dir_size_and_mtime(str(tmp_path))
        assert size == 0
        assert mtime > 0

    def test_directory_with_files(self, tmp_path):
        file1 = tmp_path / "file1.txt"
        file1.write_text("a" * 100)

        file2 = tmp_path / "file2.txt"
        file2.write_text("b" * 200)

        size, mtime = get_dir_size_and_mtime(str(tmp_path))
        assert size == 300

    def test_nested_directories(self, tmp_path):
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        (subdir / "file.txt").write_text("a" * 50)

        size, mtime = get_dir_size_and_mtime(str(tmp_path))
        assert size == 50

    def test_nonexistent_directory(self):
        size, mtime = get_dir_size_and_mtime("/nonexistent/path")
        assert size == 0
        assert mtime == 0


class TestCleanEmptyDirs:
    def test_removes_empty_directories(self, tmp_path):
        empty1 = tmp_path / "empty1"
        empty1.mkdir()
        empty2 = empty1 / "empty2"
        empty2.mkdir()

        clean_empty_dirs(str(tmp_path), None)

        assert not empty1.exists()
        assert not empty2.exists()

    def test_keeps_non_empty_directories(self, tmp_path):
        non_empty = tmp_path / "non_empty"
        non_empty.mkdir()
        (non_empty / "file.txt").write_text("content")

        clean_empty_dirs(str(tmp_path), None)

        assert non_empty.exists()

    def test_nested_empty_dirs(self, tmp_path):
        level1 = tmp_path / "level1"
        level1.mkdir()
        level2 = level1 / "level2"
        level2.mkdir()
        level3 = level2 / "level3"
        level3.mkdir()

        clean_empty_dirs(str(tmp_path), None)

        assert not level1.exists()
