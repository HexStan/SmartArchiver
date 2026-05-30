from enum import Enum


class FileAction(Enum):
    TRANSFER = "transfer"
    DELETE = "delete"
    SKIP = "skip"


class MoverStats:
    def __init__(self):
        self._success = 0
        self._error = 0
        self._dropped = 0
        self._kept = 0
        self._deleted = 0
        self._conflict_skipped = 0
        self._locked_skipped = 0
        self._total_bytes = 0

    @property
    def success(self):
        return self._success

    @property
    def error(self):
        return self._error

    @property
    def dropped(self):
        return self._dropped

    @property
    def kept(self):
        return self._kept

    @property
    def deleted(self):
        return self._deleted

    @property
    def conflict_skipped(self):
        return self._conflict_skipped

    @property
    def locked_skipped(self):
        return self._locked_skipped

    @property
    def total_bytes(self):
        return self._total_bytes

    def record_success(self, bytes_transferred=0):
        self._success += 1
        self._total_bytes += bytes_transferred

    def record_error(self):
        self._error += 1

    def record_dropped(self):
        self._dropped += 1

    def record_kept(self):
        self._kept += 1

    def record_deleted(self):
        self._deleted += 1

    def record_conflict_skipped(self):
        self._conflict_skipped += 1

    def record_locked_skipped(self):
        self._locked_skipped += 1
