import os
import time
from abc import ABC, abstractmethod

from src.app.context import AppContext
from src.core import dest
from src.core.task_config import (
    MoveCopyTaskConfig,
    RotateTaskConfig,
    TransferTaskConfig,
)
from src.core.task_plan import TaskSpec
from src.core.types import FileAction, MoverStats, TaskOutcome
from src.fs import TransferAbortedError, get_dir_size_and_mtime, is_file_locked
from src.fs import delete_file as _fs_delete
from src.output.presentation import fmt_size as _fmt_size
from src.output.presentation import fmt_timespan, print_task_header

# ============================================================
# 文件检查器
# ============================================================


class FileChecker:
    """文件检查器：负责历史失败次数、时间阈值、文件锁检查。"""

    def __init__(self, now: float | None = None) -> None:
        self.now = now or time.time()
        ctx = AppContext.get()
        self.max_retries = ctx.config.max_retries

    def should_skip(
        self,
        src_path: str,
        rel_path: str,
        mtime: float,
        mtime_threshold_seconds: float,
        stats: MoverStats,
    ) -> bool:
        ctx = AppContext.get()
        should_skip, _fail_count = ctx.history_mgr.should_skip(src_path, self.max_retries)
        if should_skip:
            stats.record_dropped()
            ctx.logger.warning(f"跳过文件 (多次失败): {src_path}")
            return True

        if (self.now - mtime) <= mtime_threshold_seconds:
            return True

        if is_file_locked(src_path):
            stats.record_locked_skipped()
            ctx.logger.warning(f"跳过文件 (被锁定): {rel_path}")
            return True

        return False


# ============================================================
# 动作执行器
# ============================================================


class ActionExecutor:
    """动作执行器：负责目标目录验证、冲突处理和动作分发。

    目标侧 I/O 一律经由 core.dest 转发层（dest_root 为原始 dest 协议串）。
    conflict_policy 由调用方从类型化配置传入（加载期已校验取值）。
    """

    def __init__(self, source_root: str, dest_root: str, conflict_policy: str = "skip") -> None:
        self.source_root = source_root
        self.dest_root = dest_root
        self.conflict_policy = conflict_policy
        self._dest_checked = False
        self._dest_valid = False

    def execute(
        self,
        action: FileAction,
        src_path: str,
        rel_path: str,
        size: int,
        stats: MoverStats,
        is_copy: bool,
    ) -> bool:
        """根据 FileAction 分发到对应的处理方法。"""
        ctx = AppContext.get()

        if action == FileAction.TRANSFER:
            # dest 允许为空：为空时 TRANSFER 没有可写入的目标，逐文件
            # warning 跳过，DELETE / SKIP 等其余动作不受影响。
            # 此处不预设 dest 为空的用途，交由配置组合决定。
            if not self.dest_root:
                ctx.logger.warning(f"跳过文件 (未配置目标目录): {rel_path}")
                return True

            if not self._dest_checked:
                self._dest_checked = True
                if not dest.is_dir(self.dest_root):
                    ctx.logger.critical("!!! 致命错误: 目标目录不存在 !!!")
                    self._dest_valid = False
                    return False
                self._dest_valid = True

            if not self._dest_valid:
                return False

            self._transfer(src_path, size, rel_path, stats, is_copy)
            return True

        elif action == FileAction.DELETE:
            self._delete(src_path, size, stats)
            return True

        elif action == FileAction.SKIP:
            stats.record_kept()
            ctx.logger.debug(f"跳过文件 (匹配规则): {rel_path}  ({_fmt_size(size)})")
            return True

        return True

    def _transfer(
        self, src_path: str, file_size: int, rel_path: str, stats: MoverStats, is_copy: bool
    ) -> None:
        """执行文件传输（复制或移动），处理冲突策略。"""
        ctx = AppContext.get()
        dest_path = dest.build_dest_path(self.dest_root, rel_path)
        action_name = "复制" if is_copy else "移动"

        try:
            file_exists = dest.exists(dest_path)
            new_dest_path = dest_path

            if file_exists:
                if self.conflict_policy == "skip":
                    stats.record_conflict_skipped()
                    ctx.logger.debug(f"跳过 (重复): {rel_path}")
                    return

                elif self.conflict_policy == "copy":
                    new_dest_path = dest.unique_dest(dest_path)

                elif self.conflict_policy == "overwrite":
                    # 目标由后端原子替换（os.replace / 服务端 rename），
                    # 预删除会留下"目标短暂缺失"的窗口
                    pass

            size_str = _fmt_size(file_size, binary=True)
            if file_exists and self.conflict_policy == "overwrite":
                ctx.logger.info(f"正在覆盖同名文件: {rel_path} ({size_str})")
            elif file_exists and self.conflict_policy == "copy":
                new_rel = dest.relpath(self.dest_root, new_dest_path)
                ctx.logger.info(f"目标存在，正在创建副本: {new_rel} ({size_str})")
            else:
                ctx.logger.info(f"正在{action_name}文件: {rel_path} ({size_str})")

            if is_copy:
                dest.copy_file(src_path, new_dest_path)
            else:
                dest.move_file(src_path, new_dest_path)

            stats.record_success(bytes_transferred=file_size)
            ctx.history_mgr.record_success(src_path)

        except TransferAbortedError:
            # stop --fast：任务被丢弃，不计入失败次数，向上传播以中止任务
            raise
        except Exception as e:  # noqa: BLE001
            count = ctx.history_mgr.record_failure(src_path)
            ctx.logger.error(f"{action_name}文件失败 ({count} 次): {e}")
            stats.record_error()

    def _delete(self, src_path: str, file_size: int, stats: MoverStats) -> None:
        """执行文件删除。"""
        ctx = AppContext.get()
        rel_path = os.path.relpath(src_path, self.source_root)
        try:
            size_str = _fmt_size(file_size, binary=True)
            ctx.logger.info(f"正在删除文件: {rel_path}  ({size_str})")
            _fs_delete(src_path)
            ctx.history_mgr.record_success(src_path)
            stats.record_deleted()
        except OSError as e:
            count = ctx.history_mgr.record_failure(src_path)
            ctx.logger.error(f"删除文件失败 ({count} 次): {e}")


# ============================================================
# 任务处理器基类
# ============================================================


class BaseTaskHandler(ABC):
    def __init__(
        self,
        spec: TaskSpec,
        now: float | None = None,
    ) -> None:
        config = spec.config
        assert config is not None, "config 为 None 的 TaskSpec 不应进入处理器构造"
        self.task = config
        self.depends_on = spec.depends_on
        self.now = now or time.time()
        self.stats = MoverStats()
        self.source_root: str = config.source
        # 原始 dest 协议串（可能带 {类型:别名}? 前缀），目标侧 I/O 经 core.dest
        self.dest_root: str = config.dest
        self.task_mode: str = config.mode
        self.remove_empty_dirs: bool = (
            config.remove_empty_dirs if isinstance(config, TransferTaskConfig) else False
        )
        self.checker = FileChecker(now=self.now)
        # sync 不使用执行器与冲突策略（整树同步由外部工具完成）
        self.executor = ActionExecutor(
            config.source,
            config.dest,
            conflict_policy=(
                config.conflict_policy if isinstance(config, TransferTaskConfig) else "skip"
            ),
        )

    def execute(self) -> TaskOutcome:
        """模板方法：任务头 → 源目录静默期检查 → 子类执行。

        三种处理器共有的前导步骤收敛于此，新增模式自动获得同一入口行为。
        返回本轮执行的定性结果（TaskOutcome）：配置校验失败的任务在加载期
        已被拒绝（不会构造处理器）；静默期跳过只是"本轮无事可做"，记
        COMPLETED（依赖方照常执行）；子类执行是否完整跑完由 _execute()
        的布尔结果定性。
        """
        ctx = AppContext.get()

        print_task_header(
            logger=ctx.logger,
            task_id=self.task.id,
            name=self.task.name,
            depends_on=self.depends_on,
            mode=self.task.mode,
            source=self.task.source,
            dest=self.task.dest,
            source_quiet_seconds=self.task.source_quiet_seconds,
            mtime_threshold_seconds=(
                self.task.mtime_threshold_seconds
                if isinstance(self.task, MoveCopyTaskConfig)
                else None
            ),
            size_limit=self.task.size_limit if isinstance(self.task, RotateTaskConfig) else 0,
            count_limit=self.task.count_limit if isinstance(self.task, RotateTaskConfig) else 0,
        )

        if self._source_in_quiet_period():
            return TaskOutcome.COMPLETED

        return TaskOutcome.COMPLETED if self._execute() else TaskOutcome.FAILED

    @abstractmethod
    def _execute(self) -> bool:
        """子类执行体；返回 False 表示本轮执行被致命中断（记 FAILED）。

        文件级错误不在此列——单文件失败由重试/历史机制兜底，任务完整
        跑完仍算 COMPLETED。停止指令导致的中断同样返回 False，此后任务
        循环会因停止检查整体退出，结果仅保持"中断 = 未完成"的一致性。
        """
        ...

    def _source_in_quiet_period(self) -> bool:
        """source_quiet_seconds 检查：源目录仍在静默期内则整任务跳过。

        以源目录的递归最新修改时间（get_dir_size_and_mtime，含目录自身
        与全部普通文件）为基准，距 now 不超过静默秒数即视为"仍在写入中"，
        本轮跳过；0 或缺省不启用，不产生额外遍历。源目录不存在时 mtime
        为 0、age 远超阈值，自然落入既有的"源目录不存在"错误路径。
        """
        quiet_seconds = self.task.source_quiet_seconds
        if not quiet_seconds:
            return False

        _total_size, latest_mtime = get_dir_size_and_mtime(self.source_root)
        quiet_age = self.now - latest_mtime
        # 边界与 FileChecker 的 mtime_threshold_seconds 对齐：恰好等于阈值
        # 仍视为"未静默足够久"
        if quiet_age > quiet_seconds:
            return False

        AppContext.get().logger.info(f"源目录最近修改于 {fmt_timespan(quiet_age)} 前，跳过该任务。")
        return True
