import os
import time
from abc import ABC, abstractmethod
from typing import Any

from src.app.context import AppContext
from src.core.backend import DestBackend
from src.core.filters import FileFilterPolicy
from src.core.types import FileAction, MoverStats
from src.fs import TransferAbortedError, is_file_locked
from src.fs import delete_file as _fs_delete
from src.output.presentation import fmt_size as _fmt_size

# ============================================================
# 配置验证
# ============================================================


def validate_task_config(task: dict[str, Any], task_mode: str) -> bool:
    """验证任务配置是否完整。"""
    ctx = AppContext.get()

    if task_mode == "sync":
        required_fields = ["mode", "source", "dest"]
        tool = task.get("tool", "auto").lower()
        if tool not in ("auto", "rsync", "rclone"):
            ctx.logger.error(
                f"sync 模式的 tool 配置值无效: '{task.get('tool')}'，"
                f"可选值: auto、rsync、rclone，跳过该任务。"
            )
            return False
    elif task_mode == "rotate":
        required_fields = [
            "mode",
            "source",
            "remove_empty_dirs",
        ]
    else:
        # move/copy 的 dest 选填：省略时任务合法但不执行传输，
        # 由 ActionExecutor 的守卫逐文件跳过，其余规则照常评估
        required_fields = [
            "mode",
            "source",
            "mtime_threshold_seconds",
            "remove_empty_dirs",
        ]

    missing_fields = [field for field in required_fields if field not in task]
    if missing_fields:
        ctx.logger.error(f"任务配置缺少必填项: {'、'.join(missing_fields)}，跳过该任务。")
        return False

    if task_mode in ("rotate", "sync") and task.get("include_rules"):
        ctx.logger.error(f"{task_mode} 模式下不支持配置 include_rules，跳过该任务。")
        return False

    return True


# ============================================================
# 文件检查器
# ============================================================


class FileChecker:
    """文件检查器：负责历史失败次数、时间阈值、文件锁检查。"""

    def __init__(self, task: dict[str, Any], now: float | None = None) -> None:
        self.now = now or time.time()
        ctx = AppContext.get()
        self.max_retries = ctx.config.get("max_retries", 3)

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

    整合了原 actions.py 中 transfer_file 和 delete_file 的业务逻辑。
    """

    def __init__(self, task: dict[str, Any], dest_backend: DestBackend) -> None:
        self.source_root: str = task.get("source", "")
        self.dest_backend = dest_backend
        self.conflict_policy: str = task.get("conflict_policy", "skip").lower()
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
            if not self.dest_backend.root_path:
                ctx.logger.warning(f"跳过文件 (未配置目标目录): {rel_path}")
                return True

            if not self._dest_checked:
                self._dest_checked = True
                if not self.dest_backend.is_dir(self.dest_backend.root_path):
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
        dest_path = self.dest_backend.build_dest_path(rel_path)
        action_name = "复制" if is_copy else "移动"

        try:
            file_exists = self.dest_backend.exists(dest_path)
            new_dest_path = dest_path

            if file_exists:
                if self.conflict_policy == "skip":
                    stats.record_conflict_skipped()
                    ctx.logger.debug(f"跳过 (重复): {rel_path}")
                    return

                elif self.conflict_policy == "copy":
                    new_dest_path = self.dest_backend.get_unique_dest(dest_path)

                elif self.conflict_policy == "overwrite":
                    # 目标由后端原子替换（os.replace / 服务端 rename），
                    # 预删除会留下"目标短暂缺失"的窗口
                    pass

                else:
                    return

            size_str = _fmt_size(file_size, binary=True)
            if file_exists and self.conflict_policy == "overwrite":
                ctx.logger.info(f"正在覆盖同名文件: {rel_path} ({size_str})")
            elif file_exists and self.conflict_policy == "copy":
                new_rel = self.dest_backend.relpath(new_dest_path)
                ctx.logger.info(f"目标存在，正在创建副本: {new_rel} ({size_str})")
            else:
                ctx.logger.info(f"正在{action_name}文件: {rel_path} ({size_str})")

            if is_copy:
                self.dest_backend.copy_file(src_path, new_dest_path)
            else:
                self.dest_backend.move_file(src_path, new_dest_path)

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
        task: dict[str, Any],
        dest_backend: DestBackend,
        now: float | None = None,
    ) -> None:
        self.task = task
        self.now = now or time.time()
        self.stats = MoverStats()
        self.source_root: str = task.get("source", "")
        self.dest_backend = dest_backend
        self.task_mode: str = task.get("mode", "").lower()
        self.remove_empty_dirs: bool = task.get("remove_empty_dirs", False)

        merged_config = {
            "include_rules": task.get("include_rules", {})
            if self.task_mode in ("move", "copy")
            else {},
            "exclude_rules": task.get("exclude_rules", {}),
            "delete_rules": task.get("delete_rules", {}),
        }
        self.policy = FileFilterPolicy(merged_config)
        self.checker = FileChecker(task, now)
        self.executor = ActionExecutor(task, dest_backend)

    def validate(self) -> bool:
        return validate_task_config(self.task, self.task_mode)

    @abstractmethod
    def execute(self) -> None: ...
