import os
from src.history import HistoryManager


class TestHistoryManagerShouldSkip:
    def test_empty_history(self, tmp_path):
        hm = HistoryManager(str(tmp_path))
        should_skip, count = hm.should_skip("/some/file.txt", 3)
        assert not should_skip
        assert count == 0

    def test_below_threshold(self, tmp_path):
        hm = HistoryManager(str(tmp_path))
        hm.record_failure("/file.txt")
        hm.record_failure("/file.txt")
        should_skip, count = hm.should_skip("/file.txt", 3)
        assert not should_skip
        assert count == 2

    def test_at_threshold(self, tmp_path):
        hm = HistoryManager(str(tmp_path))
        for _ in range(3):
            hm.record_failure("/file.txt")
        should_skip, count = hm.should_skip("/file.txt", 3)
        assert should_skip
        assert count == 3

    def test_above_threshold(self, tmp_path):
        hm = HistoryManager(str(tmp_path))
        for _ in range(5):
            hm.record_failure("/file.txt")
        should_skip, count = hm.should_skip("/file.txt", 3)
        assert should_skip
        assert count == 5

    def test_different_paths_independent(self, tmp_path):
        hm = HistoryManager(str(tmp_path))
        for _ in range(3):
            hm.record_failure("/file_a.txt")
        should_skip, count = hm.should_skip("/file_b.txt", 3)
        assert not should_skip
        assert count == 0

    def test_record_success_clears_failure(self, tmp_path):
        hm = HistoryManager(str(tmp_path))
        hm.record_failure("/file.txt")
        hm.record_success("/file.txt")
        should_skip, count = hm.should_skip("/file.txt", 3)
        assert not should_skip
        assert count == 0


class TestHistoryManagerPersistence:
    def test_save_and_load(self, tmp_path):
        hm = HistoryManager(str(tmp_path), "test_history.json")
        hm.record_failure("/a.txt")
        hm.record_failure("/a.txt")
        hm.record_failure("/b.txt")
        hm.save()

        hm2 = HistoryManager(str(tmp_path), "test_history.json")
        should_skip, count = hm2.should_skip("/a.txt", 3)
        assert not should_skip
        assert count == 2
        should_skip2, count2 = hm2.should_skip("/b.txt", 3)
        assert not should_skip2
        assert count2 == 1

    def test_load_corrupted_json(self, tmp_path):
        file_path = os.path.join(str(tmp_path), "failure_history.json")
        with open(file_path, "w") as f:
            f.write("not valid json{{{")

        hm = HistoryManager(str(tmp_path))
        should_skip, count = hm.should_skip("/file.txt", 3)
        assert not should_skip

    def test_load_missing_file(self, tmp_path):
        hm = HistoryManager(str(tmp_path), "nonexistent.json")
        should_skip, count = hm.should_skip("/file.txt", 3)
        assert not should_skip

    def test_record_failure_initial_count(self, tmp_path):
        hm = HistoryManager(str(tmp_path))
        count = hm.record_failure("/new.txt")
        assert count == 1

    def test_record_failure_increments(self, tmp_path):
        hm = HistoryManager(str(tmp_path))
        hm.record_failure("/file.txt")
        count = hm.record_failure("/file.txt")
        assert count == 2
