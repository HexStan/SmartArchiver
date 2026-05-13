from abc import ABC, abstractmethod
import os
import time

from src.app_context import AppContext
from src.presentation import fmt_size as _fmt_size
from src.utils import is_file_locked
from src.core.types import FileAction, MoverStats
from src.core.filters import FileFilterPolicy
from src.core.actions import validate_task_config, delete_file, transfer_file


class FileChecker:
    """文件检查器：负责历史失败次数、时间阈值、文件锁检查"""

    def __init__(self, task, now=None):
        self.now = now or time.time()
        ctx = AppContext.get()
        self.max_retries = ctx.config.get("max_retries", 3)

    def should_skip(self, src_path, rel_path, mtime, mtime_threshold_seconds, stats):
        ctx = AppContext.get()
        should_skip, fail_count = ctx.history_mgr.should_skip(
            src_path, self.max_retries
        )
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


class ActionExecutor:
    """动作执行器：负责目标目录验证和动作分发"""

    def __init__(self, task):
        self.source_root = task.get("source")
        self.dest_root = task.get("dest")
        self.conflict_policy = task.get("conflict_policy", "").lower()
        self._dest_checked = False
        self._dest_valid = False

    def execute(
        self, action, src_path, rel_path, size, stats, transfer_func, action_name
    ):
        ctx = AppContext.get()

        if action == FileAction.TRANSFER:
            if not self.dest_root:
                ctx.logger.warning(f"跳过文件 (目标目录非法): {rel_path}")
                return False

            if not self._dest_checked:
                self._dest_checked = True
                if not os.path.isdir(self.dest_root):
                    ctx.logger.critical("!!! CRUCIAL: 目标目录不存在 !!!")
                    self._dest_valid = False
                    return False
                self._dest_valid = True

            if not self._dest_valid:
                return False

            transfer_file(
                src_path,
                size,
                self.source_root,
                self.dest_root,
                stats,
                self.conflict_policy,
                transfer_func,
                action_name,
            )
            return True
        elif action == FileAction.DELETE:
            delete_file(src_path, size, self.source_root, stats)
            return True
        elif action == FileAction.SKIP:
            stats.record_kept()
            ctx.logger.debug(f"保留文件 (匹配规则): {rel_path}  ({_fmt_size(size)})")
            return True
        return True


class BaseTaskHandler(ABC):
    def __init__(self, task, now=None):
        self.task = task
        self.now = now or time.time()
        self.stats = MoverStats()
        self.source_root = task.get("source")
        self.dest_root = task.get("dest")
        self.task_mode = task.get("mode", "").lower()
        self.remove_empty_dirs = task.get("remove_empty_dirs", False)

        task_delete_rules = task.get("delete_rules", {})
        task_keep_rules = task.get("keep_rules", {})
        preferred_rule = task.get("preferred_rule", "keep")
        whitelist_rules = task.get("whitelist_rules", {})
        is_whitelist_mode = self.task_mode in ["whitelist_copy", "whitelist_move"]

        merged_config = {
            "delete_rules": task_delete_rules,
            "keep_rules": task_keep_rules,
            "preferred_rule": preferred_rule,
            "whitelist_rules": whitelist_rules,
            "is_whitelist_mode": is_whitelist_mode,
        }
        self.policy = FileFilterPolicy(merged_config)
        self.checker = FileChecker(task, now)
        self.executor = ActionExecutor(task)

    def validate(self):
        return validate_task_config(self.task, self.task_mode)

    @abstractmethod
    def execute(self):
        pass
