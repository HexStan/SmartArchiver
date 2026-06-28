"""统一的表现层：集中所有 humanfriendly 格式化调用和任务输出格式化"""

from humanfriendly import format_size, format_timespan
from src.app_context import AppContext


def fmt_size(size_bytes, binary=True):
    return format_size(size_bytes, binary=binary)


def fmt_timespan(seconds):
    return format_timespan(seconds)


def print_task_header(task):
    ctx = AppContext.get()
    source_root = task.get("source")
    dest_root = task.get("dest")
    task_mode = task.get("mode", "").lower()
    task_name = task.get("name")

    if task_name:
        ctx.logger.info(f" - 名称：{task_name}")

    mode_labels = {
        "move": "移动",
        "copy": "复制",
        "whitelist_move": "移动 (白名单)",
        "whitelist_copy": "复制 (白名单)",
        "rotate": "轮转",
        "sync": "同步",
    }
    mode_str = mode_labels.get(task_mode, task_mode)
    ctx.logger.info(f" - 任务模式: {mode_str}")
    ctx.logger.info(f" - 源目录: {source_root}")

    if not dest_root and task_mode != "sync":
        ctx.logger.info(" - 目标目录: 无")
    else:
        ctx.logger.info(f" - 目标目录: {dest_root}")

    if task_mode not in ["rotate", "sync"]:
        mtime_threshold_seconds = task.get("mtime_threshold_minutes", 0) * 60
        ctx.logger.info(f" - 时间阈值: {fmt_timespan(mtime_threshold_seconds)}")

    if task_mode == "rotate":
        from src.utils import parse_size_string

        size_limit = task.get("size_limit")
        count_limit = task.get("count_limit")
        if size_limit and parse_size_string(size_limit) > 0:
            ctx.logger.info(f" - 大小限制: {size_limit}")
        if count_limit and int(count_limit) > 0:
            ctx.logger.info(f" - 数量限制: {count_limit}")


def print_task_summary(stats, duration, total_bytes):
    ctx = AppContext.get()
    duration_str = fmt_timespan(duration)
    total_size_str = fmt_size(total_bytes, binary=True)
    speed_str = fmt_size(total_bytes / max(duration, 0.001), binary=True) + "/s"

    tail_msg = [f"成功 {stats.success} 项"]
    if stats.conflict_skipped > 0:
        tail_msg.append(f"因重复而跳过 {stats.conflict_skipped} 项")
    if stats.locked_skipped > 0:
        tail_msg.append(f"因文件锁而跳过 {stats.locked_skipped} 项")
    if stats.kept > 0:
        tail_msg.append(f"根据规则保留 {stats.kept} 项")
    if stats.deleted > 0:
        tail_msg.append(f"根据规则删除 {stats.deleted} 项")
    ctx.logger.info("，".join(tail_msg) + "。")

    tail_msg_2 = []
    if stats.error > 0:
        tail_msg_2.append(f"失败 {stats.error} 项")
    if stats.dropped > 0:
        tail_msg_2.append(f"因多次失败而跳过 {stats.dropped} 项")
    if tail_msg_2:
        ctx.logger.info("，".join(tail_msg_2) + "。")

    if stats.success > 0:
        ctx.logger.info(
            f"在 {duration_str} 内传输了 {total_size_str}，平均速度 {speed_str}。"
        )
