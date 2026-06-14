import pytest
from src.core.types import FileAction, MoverStats
from src.fs_ops import StatResult, DirEntry


class TestFileAction:
    def test_values(self):
        assert FileAction.TRANSFER.value == "transfer"
        assert FileAction.DELETE.value == "delete"
        assert FileAction.SKIP.value == "skip"

    def test_members(self):
        assert len(FileAction) == 3


class TestMoverStats:
    def test_init_all_zero(self):
        stats = MoverStats()
        assert stats.success == 0
        assert stats.error == 0
        assert stats.dropped == 0
        assert stats.kept == 0
        assert stats.deleted == 0
        assert stats.conflict_skipped == 0
        assert stats.locked_skipped == 0
        assert stats.total_bytes == 0

    def test_record_success(self):
        stats = MoverStats()
        stats.record_success(1024)
        assert stats.success == 1
        assert stats.total_bytes == 1024

    def test_record_multiple_success(self):
        stats = MoverStats()
        stats.record_success(500)
        stats.record_success(500)
        assert stats.success == 2
        assert stats.total_bytes == 1000

    def test_record_success_zero_bytes(self):
        stats = MoverStats()
        stats.record_success()
        assert stats.success == 1
        assert stats.total_bytes == 0

    def test_record_error(self):
        stats = MoverStats()
        stats.record_error()
        assert stats.error == 1

    def test_record_dropped(self):
        stats = MoverStats()
        stats.record_dropped()
        assert stats.dropped == 1

    def test_record_kept(self):
        stats = MoverStats()
        stats.record_kept()
        assert stats.kept == 1

    def test_record_deleted(self):
        stats = MoverStats()
        stats.record_deleted()
        assert stats.deleted == 1

    def test_record_conflict_skipped(self):
        stats = MoverStats()
        stats.record_conflict_skipped()
        assert stats.conflict_skipped == 1

    def test_record_locked_skipped(self):
        stats = MoverStats()
        stats.record_locked_skipped()
        assert stats.locked_skipped == 1

    def test_accumulate_all(self):
        stats = MoverStats()
        stats.record_success(100)
        stats.record_error()
        stats.record_dropped()
        stats.record_kept()
        stats.record_deleted()
        stats.record_conflict_skipped()
        stats.record_locked_skipped()
        stats.record_success(200)
        assert stats.success == 2
        assert stats.error == 1
        assert stats.dropped == 1
        assert stats.kept == 1
        assert stats.deleted == 1
        assert stats.conflict_skipped == 1
        assert stats.locked_skipped == 1
        assert stats.total_bytes == 300

    def test_properties_are_readonly(self):
        stats = MoverStats()
        with pytest.raises(AttributeError):
            stats.success = 5


class TestStatResult:
    def test_defaults(self):
        r = StatResult()
        assert r.size == 0
        assert r.mtime == 0
        assert r.is_dir is False
        assert r.exists is False

    def test_full_init(self):
        r = StatResult(size=1024, mtime=1234567890.0, is_dir=True, exists=True)
        assert r.size == 1024
        assert r.mtime == 1234567890.0
        assert r.is_dir is True
        assert r.exists is True

    def test_no_new_attributes_allowed(self):
        r = StatResult()
        with pytest.raises(AttributeError):
            r.new_field = 100

    def test_existing_attributes_are_settable(self):
        r = StatResult(size=100)
        r.size = 200
        assert r.size == 200


class TestDirEntry:
    def test_defaults(self):
        e = DirEntry("file.txt")
        assert e.name == "file.txt"
        assert e.is_dir is False
        assert e.size == 0
        assert e.mtime == 0

    def test_full_init(self):
        e = DirEntry(name="folder", is_dir=True, size=0, mtime=1234567890.0)
        assert e.name == "folder"
        assert e.is_dir is True
        assert e.size == 0
        assert e.mtime == 1234567890.0

    def test_no_new_attributes_allowed(self):
        e = DirEntry("file.txt")
        with pytest.raises(AttributeError):
            e.new_field = 100

    def test_existing_attributes_are_settable(self):
        e = DirEntry("file.txt")
        e.name = "other.txt"
        assert e.name == "other.txt"
