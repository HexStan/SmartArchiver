from enum import Enum


class FileAction(Enum):
    TRANSFER = "transfer"
    DELETE = "delete"
    SKIP = "skip"


class TaskOutcome(Enum):
    """任务单轮执行的定性结果，供依赖编排判定"前置任务是否完成"。

    COMPLETED —— 本轮正常跑完（含静默期跳过这类"无事可做"的结束）；
    FAILED —— 本轮执行中断（源/目标目录缺失、外部工具失败、停止指令等）；
    SKIPPED —— 本轮未执行（配置校验失败、装配失败等）。
    """

    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class MoverStats:
    def __init__(self) -> None:
        self._success = 0
        self._error = 0
        self._dropped = 0
        self._kept = 0
        self._deleted = 0
        self._conflict_skipped = 0
        self._locked_skipped = 0
        self._total_bytes = 0

    @property
    def success(self) -> int:
        return self._success

    @property
    def error(self) -> int:
        return self._error

    @property
    def dropped(self) -> int:
        return self._dropped

    @property
    def kept(self) -> int:
        return self._kept

    @property
    def deleted(self) -> int:
        return self._deleted

    @property
    def conflict_skipped(self) -> int:
        return self._conflict_skipped

    @property
    def locked_skipped(self) -> int:
        return self._locked_skipped

    @property
    def total_bytes(self) -> int:
        return self._total_bytes

    def record_success(self, bytes_transferred: int = 0) -> None:
        self._success += 1
        self._total_bytes += bytes_transferred

    def record_error(self) -> None:
        self._error += 1

    def record_dropped(self) -> None:
        self._dropped += 1

    def record_kept(self) -> None:
        self._kept += 1

    def record_deleted(self) -> None:
        self._deleted += 1

    def record_conflict_skipped(self) -> None:
        self._conflict_skipped += 1

    def record_locked_skipped(self) -> None:
        self._locked_skipped += 1
