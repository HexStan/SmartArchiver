"""统一的表现层：集中所有 humanfriendly 格式化调用和任务输出格式化"""

from typing import Any, Protocol

from humanfriendly import format_size, format_timespan

from src.output.logger import Logger


def fmt_size(size_bytes: float, binary: bool = True) -> str:
    return format_size(size_bytes, binary=binary)


def fmt_timespan(seconds: float) -> str:
    return format_timespan(seconds)


class TaskStats(Protocol):
    """任务统计接口（结构化协议）。

    生产实现为 core.types.MoverStats；output 层不反向依赖 core，
    故以协议声明本模块消费的最小统计面。
    """

    @property
    def success(self) -> int: ...

    @property
    def error(self) -> int: ...

    @property
    def dropped(self) -> int: ...

    @property
    def kept(self) -> int: ...

    @property
    def deleted(self) -> int: ...

    @property
    def conflict_skipped(self) -> int: ...

    @property
    def locked_skipped(self) -> int: ...


def print_task_header(task: dict[str, Any], logger: Logger) -> None:
    source_root = task.get("source")
    dest_root = task.get("dest")
    task_mode = task.get("mode", "").lower()
    task_name = task.get("name")

    if task_name:
        logger.info(f" - 名称：{task_name}")

    mode_labels = {
        "move": "移动",
        "copy": "复制",
        "rotate": "轮转",
        "sync": "同步",
    }
    mode_str = mode_labels.get(task_mode, task_mode)
    logger.info(f" - 任务模式: {mode_str}")
    logger.info(f" - 源目录: {source_root}")

    if not dest_root and task_mode != "sync":
        logger.info(" - 目标目录: 无")
    else:
        logger.info(f" - 目标目录: {dest_root}")

    if task_mode not in ["rotate", "sync"]:
        mtime_threshold_seconds = task.get("mtime_threshold_minutes", 0) * 60
        logger.info(f" - 时间阈值: {fmt_timespan(mtime_threshold_seconds)}")

    if task_mode == "rotate":
        from src.core.patterns import parse_size_string

        size_limit = task.get("size_limit")
        count_limit = task.get("count_limit")
        if size_limit and parse_size_string(size_limit) > 0:
            logger.info(f" - 大小限制: {size_limit}")
        if count_limit and int(count_limit) > 0:
            logger.info(f" - 数量限制: {count_limit}")


def print_task_summary(stats: TaskStats, duration: float, total_bytes: int, logger: Logger) -> None:
    duration_str = fmt_timespan(duration)
    total_size_str = fmt_size(total_bytes, binary=True)
    speed_str = fmt_size(total_bytes / max(duration, 0.001), binary=True) + "/s"

    tail_msg = [f"成功 {stats.success} 项"]
    if stats.conflict_skipped > 0:
        tail_msg.append(f"因重复而跳过 {stats.conflict_skipped} 项")
    if stats.locked_skipped > 0:
        tail_msg.append(f"因文件锁而跳过 {stats.locked_skipped} 项")
    if stats.kept > 0:
        tail_msg.append(f"根据规则跳过 {stats.kept} 项")
    if stats.deleted > 0:
        tail_msg.append(f"根据规则删除 {stats.deleted} 项")
    logger.info("，".join(tail_msg) + "。")

    tail_msg_2 = []
    if stats.error > 0:
        tail_msg_2.append(f"失败 {stats.error} 项")
    if stats.dropped > 0:
        tail_msg_2.append(f"因多次失败而跳过 {stats.dropped} 项")
    if tail_msg_2:
        logger.info("，".join(tail_msg_2) + "。")

    if stats.success > 0:
        logger.info(f"在 {duration_str} 内传输了 {total_size_str}，平均速度 {speed_str}。")
