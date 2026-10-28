"""统一的表现层：集中所有容量/时长格式化调用和任务输出格式化"""

from collections.abc import Sequence
from datetime import timedelta
from typing import Protocol

import humanize

from src.output.logger import Logger


def fmt_size(size_bytes: float, binary: bool = True) -> str:
    return humanize.naturalsize(size_bytes, binary=binary)


def fmt_timespan(seconds: float) -> str:
    if seconds < 0:
        seconds = 0.0  # precisedelta 对负时长取绝对值，展示会失真；防御性钳制为 0
    return humanize.precisedelta(timedelta(seconds=seconds), minimum_unit="seconds")


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


def print_task_header(
    *,
    logger: Logger,
    task_id: str | None,
    name: str | None,
    depends_on: Sequence[str] | None,
    mode: str,
    source: str,
    dest: str,
    source_quiet_seconds: float = 0,
    mtime_threshold_seconds: float | None = None,
    size_limit: int = 0,
    count_limit: int = 0,
) -> None:
    """打印任务头部信息（参数由处理器从类型化配置提取）。

    本模块不感知 core 的任务模型（output 层依赖边界），模式差异由
    调用方以可选项区分：mtime_threshold_seconds 仅 move/copy 传入；
    size_limit / count_limit 仅 rotate 传入。0 或 None 均不展示
    （0 = 无时间限制，与 source_quiet_seconds 的展示逻辑一致）。
    """
    if task_id:
        logger.info(f" - 标识: {task_id}")

    if name:
        logger.info(f" - 名称：{name}")

    if depends_on:
        logger.info(f" - 依赖: {'、'.join(depends_on)}")

    mode_labels = {
        "move": "移动",
        "copy": "复制",
        "rotate": "轮转",
        "sync": "同步",
    }
    mode_str = mode_labels.get(mode, mode)
    logger.info(f" - 任务模式: {mode_str}")
    logger.info(f" - 源目录: {source}")

    if not dest and mode != "sync":
        logger.info(" - 目标目录: 无")
    else:
        logger.info(f" - 目标目录: {dest}")

    if mtime_threshold_seconds:
        logger.info(f" - 时间阈值: {fmt_timespan(mtime_threshold_seconds)}")

    if source_quiet_seconds:
        logger.info(f" - 静默阈值: {fmt_timespan(source_quiet_seconds)}")

    # 限额展示用解析后的数值格式化（配置串不透传到展示层）
    if size_limit > 0:
        logger.info(f" - 大小限制: {fmt_size(size_limit)}")
    if count_limit > 0:
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
