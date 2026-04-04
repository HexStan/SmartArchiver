import os
import sys
import pytest
import tempfile
import shutil
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core import process_directory_pair, move_file, delete_file, MoverStats
from src.history import HistoryManager
from src.logger import setup_logger


class MockLogger:
    def __init__(self):
        self.messages = []

    def info(self, msg, raw=False, prepend_timestamp=False):
        self.messages.append(("INFO", msg))

    def debug(self, msg, raw=False):
        self.messages.append(("DEBUG", msg))

    def success(self, msg, raw=False):
        self.messages.append(("SUCCESS", msg))

    def warning(self, msg, raw=False):
        self.messages.append(("WARNING", msg))

    def error(self, msg, raw=False):
        self.messages.append(("ERROR", msg))


class TestMoveFile:
    def test_move_file_success(self, tmp_path):
        source_dir = tmp_path / "source"
        dest_dir = tmp_path / "dest"
        source_dir.mkdir()
        dest_dir.mkdir()

        src_file = source_dir / "test.txt"
        src_file.write_text("test content")

        stats = MoverStats()
        history_mgr = HistoryManager(str(tmp_path))
        logger = MockLogger()

        move_file(
            str(src_file),
            12,
            str(source_dir),
            str(dest_dir),
            logger,
            stats,
            history_mgr,
            "overwrite",
            "move",
        )

        assert not src_file.exists()
        assert (dest_dir / "test.txt").exists()
        assert stats.success == 1
        assert stats.total_bytes == 12

    def test_move_file_creates_subdirectories(self, tmp_path):
        source_dir = tmp_path / "source"
        dest_dir = tmp_path / "dest"
        source_dir.mkdir()
        dest_dir.mkdir()

        sub_dir = source_dir / "subdir"
        sub_dir.mkdir()
        src_file = sub_dir / "test.txt"
        src_file.write_text("test content")

        stats = MoverStats()
        history_mgr = HistoryManager(str(tmp_path))
        logger = MockLogger()

        move_file(
            str(src_file),
            12,
            str(source_dir),
            str(dest_dir),
            logger,
            stats,
            history_mgr,
            "overwrite",
            "move",
        )

        assert not src_file.exists()
        assert (dest_dir / "subdir" / "test.txt").exists()

    def test_move_file_conflict_skip(self, tmp_path):
        source_dir = tmp_path / "source"
        dest_dir = tmp_path / "dest"
        source_dir.mkdir()
        dest_dir.mkdir()

        src_file = source_dir / "test.txt"
        src_file.write_text("source content")
        dest_file = dest_dir / "test.txt"
        dest_file.write_text("dest content")

        stats = MoverStats()
        history_mgr = HistoryManager(str(tmp_path))
        logger = MockLogger()

        move_file(
            str(src_file),
            13,
            str(source_dir),
            str(dest_dir),
            logger,
            stats,
            history_mgr,
            "skip",
            "move",
        )

        assert src_file.exists()
        assert dest_file.read_text() == "dest content"
        assert stats.conflict_skipped == 1

    def test_move_file_conflict_overwrite(self, tmp_path):
        source_dir = tmp_path / "source"
        dest_dir = tmp_path / "dest"
        source_dir.mkdir()
        dest_dir.mkdir()

        src_file = source_dir / "test.txt"
        src_file.write_text("source content")
        dest_file = dest_dir / "test.txt"
        dest_file.write_text("dest content")

        stats = MoverStats()
        history_mgr = HistoryManager(str(tmp_path))
        logger = MockLogger()

        move_file(
            str(src_file),
            14,
            str(source_dir),
            str(dest_dir),
            logger,
            stats,
            history_mgr,
            "overwrite",
            "move",
        )

        assert not src_file.exists()
        assert dest_file.read_text() == "source content"
        assert stats.success == 1

    def test_move_file_conflict_copy(self, tmp_path):
        source_dir = tmp_path / "source"
        dest_dir = tmp_path / "dest"
        source_dir.mkdir()
        dest_dir.mkdir()

        src_file = source_dir / "test.txt"
        src_file.write_text("source content")
        dest_file = dest_dir / "test.txt"
        dest_file.write_text("dest content")

        stats = MoverStats()
        history_mgr = HistoryManager(str(tmp_path))
        logger = MockLogger()

        move_file(
            str(src_file),
            14,
            str(source_dir),
            str(dest_dir),
            logger,
            stats,
            history_mgr,
            "copy",
            "move",
        )

        assert not src_file.exists()
        assert dest_file.read_text() == "dest content"
        assert (dest_dir / "test-1.txt").exists()
        assert (dest_dir / "test-1.txt").read_text() == "source content"
        assert stats.success == 1

    def test_copy_mode_keeps_source(self, tmp_path):
        source_dir = tmp_path / "source"
        dest_dir = tmp_path / "dest"
        source_dir.mkdir()
        dest_dir.mkdir()

        src_file = source_dir / "test.txt"
        src_file.write_text("test content")

        stats = MoverStats()
        history_mgr = HistoryManager(str(tmp_path))
        logger = MockLogger()

        move_file(
            str(src_file),
            12,
            str(source_dir),
            str(dest_dir),
            logger,
            stats,
            history_mgr,
            "overwrite",
            "copy",
        )

        assert src_file.exists()
        assert (dest_dir / "test.txt").exists()
        assert stats.success == 1


class TestDeleteFile:
    def test_delete_file_success(self, tmp_path):
        source_dir = tmp_path / "source"
        source_dir.mkdir()

        src_file = source_dir / "test.txt"
        src_file.write_text("test content")

        stats = MoverStats()
        history_mgr = HistoryManager(str(tmp_path))
        logger = MockLogger()

        delete_file(str(src_file), 12, str(source_dir), logger, stats, history_mgr)

        assert not src_file.exists()
        assert stats.deleted == 1

    def test_delete_file_records_success_in_history(self, tmp_path):
        source_dir = tmp_path / "source"
        source_dir.mkdir()

        src_file = source_dir / "test.txt"
        src_file.write_text("test content")

        stats = MoverStats()
        history_mgr = HistoryManager(str(tmp_path))
        logger = MockLogger()

        history_mgr.record_failure(str(src_file))
        assert history_mgr.history[str(src_file)] == 1

        delete_file(str(src_file), 12, str(source_dir), logger, stats, history_mgr)

        assert str(src_file) not in history_mgr.history


class TestProcessDirectoryPair:
    def test_move_mode_basic(self, tmp_path):
        source_dir = tmp_path / "source"
        dest_dir = tmp_path / "dest"
        source_dir.mkdir()
        dest_dir.mkdir()

        (source_dir / "file1.txt").write_text("content1")
        (source_dir / "file2.txt").write_text("content2")

        task = {
            "source": str(source_dir),
            "dest": str(dest_dir),
            "mode": "move",
            "min_age_minutes": 0,
            "conflict_policy": "overwrite",
            "remove_empty_dirs": True,
        }
        config = {"max_retries": 3}
        logger = MockLogger()
        history_mgr = HistoryManager(str(tmp_path))

        process_directory_pair(task, config, logger, history_mgr)

        assert not (source_dir / "file1.txt").exists()
        assert not (source_dir / "file2.txt").exists()
        assert (dest_dir / "file1.txt").exists()
        assert (dest_dir / "file2.txt").exists()

    def test_copy_mode_basic(self, tmp_path):
        source_dir = tmp_path / "source"
        dest_dir = tmp_path / "dest"
        source_dir.mkdir()
        dest_dir.mkdir()

        (source_dir / "file1.txt").write_text("content1")

        task = {
            "source": str(source_dir),
            "dest": str(dest_dir),
            "mode": "copy",
            "min_age_minutes": 0,
            "conflict_policy": "overwrite",
            "remove_empty_dirs": True,
        }
        config = {"max_retries": 3}
        logger = MockLogger()
        history_mgr = HistoryManager(str(tmp_path))

        process_directory_pair(task, config, logger, history_mgr)

        assert (source_dir / "file1.txt").exists()
        assert (dest_dir / "file1.txt").exists()

    def test_age_threshold(self, tmp_path):
        source_dir = tmp_path / "source"
        dest_dir = tmp_path / "dest"
        source_dir.mkdir()
        dest_dir.mkdir()

        old_file = source_dir / "old.txt"
        old_file.write_text("old content")
        old_time = time.time() - 3600
        os.utime(str(old_file), (old_time, old_time))

        new_file = source_dir / "new.txt"
        new_file.write_text("new content")

        task = {
            "source": str(source_dir),
            "dest": str(dest_dir),
            "mode": "move",
            "min_age_minutes": 30,
            "conflict_policy": "overwrite",
            "remove_empty_dirs": True,
        }
        config = {"max_retries": 3}
        logger = MockLogger()
        history_mgr = HistoryManager(str(tmp_path))

        process_directory_pair(task, config, logger, history_mgr)

        assert not old_file.exists()
        assert new_file.exists()
        assert (dest_dir / "old.txt").exists()
        assert not (dest_dir / "new.txt").exists()

    def test_delete_rules(self, tmp_path):
        source_dir = tmp_path / "source"
        dest_dir = tmp_path / "dest"
        source_dir.mkdir()
        dest_dir.mkdir()

        (source_dir / "file.tmp").write_text("temp content")
        (source_dir / "file.txt").write_text("normal content")

        task = {
            "source": str(source_dir),
            "dest": str(dest_dir),
            "mode": "move",
            "min_age_minutes": 0,
            "conflict_policy": "overwrite",
            "remove_empty_dirs": True,
            "delete_rules": {"lt": {"*.tmp": "ALL"}},
        }
        config = {"max_retries": 3}
        logger = MockLogger()
        history_mgr = HistoryManager(str(tmp_path))

        process_directory_pair(task, config, logger, history_mgr)

        assert not (source_dir / "file.tmp").exists()
        assert not (source_dir / "file.txt").exists()
        assert not (dest_dir / "file.tmp").exists()
        assert (dest_dir / "file.txt").exists()

    def test_keep_rules(self, tmp_path):
        source_dir = tmp_path / "source"
        dest_dir = tmp_path / "dest"
        source_dir.mkdir()
        dest_dir.mkdir()

        (source_dir / "photo.raw").write_text("raw content")
        (source_dir / "photo.jpg").write_text("jpg content")

        task = {
            "source": str(source_dir),
            "dest": str(dest_dir),
            "mode": "move",
            "min_age_minutes": 0,
            "conflict_policy": "overwrite",
            "remove_empty_dirs": True,
            "keep_rules": {"lt": {"*.raw": "ALL"}},
        }
        config = {"max_retries": 3}
        logger = MockLogger()
        history_mgr = HistoryManager(str(tmp_path))

        process_directory_pair(task, config, logger, history_mgr)

        assert (source_dir / "photo.raw").exists()
        assert not (source_dir / "photo.jpg").exists()
        assert not (dest_dir / "photo.raw").exists()
        assert (dest_dir / "photo.jpg").exists()

    def test_whitelist_move_mode(self, tmp_path):
        source_dir = tmp_path / "source"
        dest_dir = tmp_path / "dest"
        source_dir.mkdir()
        dest_dir.mkdir()

        (source_dir / "doc.pdf").write_text("pdf content")
        (source_dir / "doc.txt").write_text("txt content")

        task = {
            "source": str(source_dir),
            "dest": str(dest_dir),
            "mode": "whitelist_move",
            "min_age_minutes": 0,
            "conflict_policy": "overwrite",
            "remove_empty_dirs": True,
            "whitelist_rules": {"lt": {"*.pdf": "ALL"}},
        }
        config = {"max_retries": 3}
        logger = MockLogger()
        history_mgr = HistoryManager(str(tmp_path))

        process_directory_pair(task, config, logger, history_mgr)

        assert not (source_dir / "doc.pdf").exists()
        assert (source_dir / "doc.txt").exists()
        assert (dest_dir / "doc.pdf").exists()
        assert not (dest_dir / "doc.txt").exists()

    def test_remove_empty_dirs(self, tmp_path):
        source_dir = tmp_path / "source"
        dest_dir = tmp_path / "dest"
        source_dir.mkdir()
        dest_dir.mkdir()

        empty_dir = source_dir / "empty"
        empty_dir.mkdir()
        (source_dir / "file.txt").write_text("content")

        task = {
            "source": str(source_dir),
            "dest": str(dest_dir),
            "mode": "move",
            "min_age_minutes": 0,
            "conflict_policy": "overwrite",
            "remove_empty_dirs": True,
        }
        config = {"max_retries": 3}
        logger = MockLogger()
        history_mgr = HistoryManager(str(tmp_path))

        process_directory_pair(task, config, logger, history_mgr)

        assert not empty_dir.exists()

    def test_missing_source_directory(self, tmp_path):
        source_dir = tmp_path / "nonexistent"
        dest_dir = tmp_path / "dest"
        dest_dir.mkdir()

        task = {
            "source": str(source_dir),
            "dest": str(dest_dir),
            "mode": "move",
            "min_age_minutes": 0,
            "conflict_policy": "overwrite",
            "remove_empty_dirs": True,
        }
        config = {"max_retries": 3}
        logger = MockLogger()
        history_mgr = HistoryManager(str(tmp_path))

        process_directory_pair(task, config, logger, history_mgr)

        error_messages = [msg for level, msg in logger.messages if level == "ERROR"]
        assert any("源目录不存在" in msg for msg in error_messages)

    def test_missing_dest_directory(self, tmp_path):
        source_dir = tmp_path / "source"
        dest_dir = tmp_path / "nonexistent"
        source_dir.mkdir()
        (source_dir / "file.txt").write_text("content")

        task = {
            "source": str(source_dir),
            "dest": str(dest_dir),
            "mode": "move",
            "min_age_minutes": 0,
            "conflict_policy": "overwrite",
            "remove_empty_dirs": True,
        }
        config = {"max_retries": 3}
        logger = MockLogger()
        history_mgr = HistoryManager(str(tmp_path))

        process_directory_pair(task, config, logger, history_mgr)

        error_messages = [msg for level, msg in logger.messages if level == "ERROR"]
        assert any("目标目录不存在" in msg for msg in error_messages)

    def test_rotate_mode_by_size(self, tmp_path):
        source_dir = tmp_path / "source"
        dest_dir = tmp_path / "dest"
        source_dir.mkdir()
        dest_dir.mkdir()

        # Create 3 files, 10 bytes each. Total 30 bytes.
        f1 = source_dir / "f1.txt"
        f1.write_text("1234567890")
        os.utime(str(f1), (1000, 1000))

        f2 = source_dir / "f2.txt"
        f2.write_text("1234567890")
        os.utime(str(f2), (2000, 2000))

        f3 = source_dir / "f3.txt"
        f3.write_text("1234567890")
        os.utime(str(f3), (3000, 3000))

        task = {
            "source": str(source_dir),
            "dest": str(dest_dir),
            "mode": "rotate",
            "size_limit": "20B",
            "conflict_policy": "overwrite",
            "remove_empty_dirs": True,
        }
        config = {"max_retries": 3}
        logger = MockLogger()
        history_mgr = HistoryManager(str(tmp_path))

        process_directory_pair(task, config, logger, history_mgr)

        # f1 is the oldest, should be moved.
        assert not f1.exists()
        assert f2.exists()
        assert f3.exists()
        assert (dest_dir / "f1.txt").exists()

    def test_rotate_mode_by_count(self, tmp_path):
        source_dir = tmp_path / "source"
        dest_dir = tmp_path / "dest"
        source_dir.mkdir()
        dest_dir.mkdir()

        # Create 3 files
        f1 = source_dir / "f1.txt"
        f1.write_text("a")
        os.utime(str(f1), (1000, 1000))

        f2 = source_dir / "f2.txt"
        f2.write_text("b")
        os.utime(str(f2), (2000, 2000))

        f3 = source_dir / "f3.txt"
        f3.write_text("c")
        os.utime(str(f3), (3000, 3000))

        task = {
            "source": str(source_dir),
            "dest": str(dest_dir),
            "mode": "rotate",
            "count_limit": 2,
            "conflict_policy": "overwrite",
            "remove_empty_dirs": True,
        }
        config = {"max_retries": 3}
        logger = MockLogger()
        history_mgr = HistoryManager(str(tmp_path))

        process_directory_pair(task, config, logger, history_mgr)

        # f1 is the oldest, should be moved.
        assert not f1.exists()
        assert f2.exists()
        assert f3.exists()
        assert (dest_dir / "f1.txt").exists()

    def test_rotate_mode_both_limits(self, tmp_path):
        source_dir = tmp_path / "source"
        dest_dir = tmp_path / "dest"
        source_dir.mkdir()
        dest_dir.mkdir()

        # Create 4 files, 10 bytes each. Total 40 bytes.
        f1 = source_dir / "f1.txt"
        f1.write_text("1234567890")
        os.utime(str(f1), (1000, 1000))

        f2 = source_dir / "f2.txt"
        f2.write_text("1234567890")
        os.utime(str(f2), (2000, 2000))

        f3 = source_dir / "f3.txt"
        f3.write_text("1234567890")
        os.utime(str(f3), (3000, 3000))

        f4 = source_dir / "f4.txt"
        f4.write_text("1234567890")
        os.utime(str(f4), (4000, 4000))

        task = {
            "source": str(source_dir),
            "dest": str(dest_dir),
            "mode": "rotate",
            "size_limit": "30B",  # Needs to remove 1 file to meet size limit
            "count_limit": 2,  # Needs to remove 2 files to meet count limit
            "conflict_policy": "overwrite",
            "remove_empty_dirs": True,
        }
        config = {"max_retries": 3}
        logger = MockLogger()
        history_mgr = HistoryManager(str(tmp_path))

        process_directory_pair(task, config, logger, history_mgr)

        # f1 and f2 are the oldest, should be moved to meet count limit.
        assert not f1.exists()
        assert not f2.exists()
        assert f3.exists()
        assert f4.exists()
        assert (dest_dir / "f1.txt").exists()
        assert (dest_dir / "f2.txt").exists()
