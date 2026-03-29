import os
import sys
import json
import pytest
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.history import HistoryManager


class TestHistoryManager:
    def test_init_creates_empty_history(self, tmp_path):
        manager = HistoryManager(str(tmp_path))
        assert manager.history == {}

    def test_load_existing_history(self, tmp_path):
        history_data = {"/path/to/file.txt": 2}
        history_file = tmp_path / "failure_history.json"
        history_file.write_text(json.dumps(history_data))

        manager = HistoryManager(str(tmp_path))
        assert manager.history == history_data

    def test_load_corrupted_history(self, tmp_path):
        history_file = tmp_path / "failure_history.json"
        history_file.write_text("invalid json {")

        manager = HistoryManager(str(tmp_path))
        assert manager.history == {}

    def test_record_failure_increments_count(self, tmp_path):
        manager = HistoryManager(str(tmp_path))

        count1 = manager.record_failure("/path/to/file.txt")
        assert count1 == 1
        assert manager.history["/path/to/file.txt"] == 1

        count2 = manager.record_failure("/path/to/file.txt")
        assert count2 == 2
        assert manager.history["/path/to/file.txt"] == 2

    def test_record_success_removes_entry(self, tmp_path):
        manager = HistoryManager(str(tmp_path))
        manager.record_failure("/path/to/file.txt")
        manager.record_failure("/path/to/other.txt")

        manager.record_success("/path/to/file.txt")
        assert "/path/to/file.txt" not in manager.history
        assert "/path/to/other.txt" in manager.history

    def test_should_skip_returns_false_when_below_max(self, tmp_path):
        manager = HistoryManager(str(tmp_path))
        manager.record_failure("/path/to/file.txt")

        should_skip, count = manager.should_skip("/path/to/file.txt", 3)
        assert should_skip is False
        assert count == 1

    def test_should_skip_returns_true_when_at_max(self, tmp_path):
        manager = HistoryManager(str(tmp_path))
        manager.record_failure("/path/to/file.txt")
        manager.record_failure("/path/to/file.txt")
        manager.record_failure("/path/to/file.txt")

        should_skip, count = manager.should_skip("/path/to/file.txt", 3)
        assert should_skip is True
        assert count == 3

    def test_should_skip_returns_false_for_unknown_file(self, tmp_path):
        manager = HistoryManager(str(tmp_path))

        should_skip, count = manager.should_skip("/unknown/file.txt", 3)
        assert should_skip is False
        assert count == 0

    def test_save_creates_file(self, tmp_path):
        manager = HistoryManager(str(tmp_path))
        manager.record_failure("/path/to/file.txt")
        manager.save()

        history_file = tmp_path / "failure_history.json"
        assert history_file.exists()

        with open(history_file) as f:
            data = json.load(f)
        assert data["/path/to/file.txt"] == 1

    def test_save_uses_temp_file(self, tmp_path):
        manager = HistoryManager(str(tmp_path))
        manager.record_failure("/path/to/file.txt")
        manager.save()

        temp_file = tmp_path / "failure_history.json.tmp"
        assert not temp_file.exists()

    def test_custom_filename(self, tmp_path):
        manager = HistoryManager(str(tmp_path), filename="custom_history.json")
        manager.record_failure("/path/to/file.txt")
        manager.save()

        custom_file = tmp_path / "custom_history.json"
        assert custom_file.exists()

    def test_unicode_paths(self, tmp_path):
        manager = HistoryManager(str(tmp_path))
        unicode_path = "/路径/到/文件.txt"

        count = manager.record_failure(unicode_path)
        assert count == 1
        assert unicode_path in manager.history

    def test_multiple_files_tracking(self, tmp_path):
        manager = HistoryManager(str(tmp_path))

        manager.record_failure("/file1.txt")
        manager.record_failure("/file2.txt")
        manager.record_failure("/file1.txt")

        assert manager.history["/file1.txt"] == 2
        assert manager.history["/file2.txt"] == 1
