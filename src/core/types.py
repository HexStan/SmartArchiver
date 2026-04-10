from enum import Enum
from humanfriendly import format_size, format_timespan


# 定义操作枚举，提高代码可读性
class FileAction(Enum):
    TRANSFER = "transfer"  # 正常移动
    DELETE = "delete"  # 命中规则，且配置为删除
    SKIP = "skip"  # 命中规则（配置为保留）


class MoverStats:
    def __init__(self):
        self.success = 0
        self.error = 0
        self.dropped = 0
        self.kept = 0
        self.deleted = 0
        self.conflict_skipped = 0
        self.locked_skipped = 0
        self.total_bytes = 0

    def calculate_speed(self, start_time, end_time):
        """
        计算耗时、总大小和传输速度
        返回: (duration_str, total_size_str, speed_str)
        """
        duration = end_time - start_time

        # 防止除以零（如果执行极快）
        if duration < 0.001:
            duration = 0.001

        # 计算速度 (Bytes per second)
        speed_bps = self.total_bytes / duration

        total_size_str = format_size(self.total_bytes, binary=True)
        speed_str = format_size(speed_bps, binary=True) + "/s"
        duration_str = format_timespan(duration)

        return duration_str, total_size_str, speed_str
