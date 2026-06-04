import os
import time
from abc import ABC, abstractmethod

from src.app_context import AppContext
from src.presentation import fmt_size as _fmt_size
from src.fs_ops import is_file_locked, delete_file as _fs_delete
from src.utils import parse_size_string
from src.core.types import FileAction, MoverStats
from src.core.filters import FileFilterPolicy


# ============================================================
# 配置验证
# ============================================================


def validate_task_config(task, task_mode):
    """验证任务配置是否完整。"""
    ctx = AppContext.get()

    if task_mode == "sync":
        required_fields = ["mode"]
    elif task_mode == "rotate":
        required_fields = [
            "mode",
            "remove_empty_dirs",
        ]
        size_limit = parse_size_string(str(task.get("size_limit", "0")))
        count_limit = int(task.get("count_limit", 0))
        rotate_rules = task.get("rotate_rules", {})
        rotate_size_rules = rotate_rules.get("size", {})
        rotate_count_rules = rotate_rules.get("count", {})

        if (
            size_limit == 0
            and count_limit == 0
            and not rotate_size_rules
            and not rotate_count_rules
        ):
            ctx.logger.error(
                "rotate 模式下必须配置 size_limit、count_limit、"
                "rotate_rules.size 或 rotate_rules.count 中的至少一项，跳过该任务。"
            )
            return False
    else:
        required_fields = [
            "mode",
            "mtime_threshold_minutes",
            "conflict_policy",
            "remove_empty_dirs",
        ]

    missing_fields = [field for field in required_fields if field not in task]
    if missing_fields:
        ctx.logger.error(
            f"任务配置缺少必填项: {'、'.join(missing_fields)}，跳过该任务。"
        )
        return False

    if task_mode in ["whitelist_copy", "whitelist_move"]:
        whitelist_rules = task.get("whitelist_rules", {})
        if not whitelist_rules:
            ctx.logger.error("白名单模式下必须配置 whitelist_rules，跳过该任务。")
            return False

    return True


# ============================================================
# 文件检查器
# ============================================================


class FileChecker:
    """文件检查器：负责历史失败次数、时间阈值、文件锁检查。"""

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


# ============================================================
# 动作执行器
# ============================================================


class ActionExecutor:
    """动作执行器：负责目标目录验证、冲突处理和动作分发。

    整合了原 actions.py 中 transfer_file 和 delete_file 的业务逻辑。
    """

    def __init__(self, task, dest_backend):
        self.source_root = task.get("source")
        self.dest_root = task.get("dest")
        self.dest_backend = dest_backend
        self.conflict_policy = task.get("conflict_policy", "").lower()
        self._dest_checked = False
        self._dest_valid = False

    def execute(self, action, src_path, rel_path, size, stats, is_copy):
        """根据 FileAction 分发到对应的处理方法。"""
        ctx = AppContext.get()

        if action == FileAction.TRANSFER:
            if not self.dest_root:
                ctx.logger.warning(f"跳过文件 (目标目录非法): {rel_path}")
                return False

            if not self._dest_checked:
                self._dest_checked = True
                if not self.dest_backend.is_dir(self.dest_backend.root_path):
                    ctx.logger.critical("!!! CRUCIAL: 目标目录不存在 !!!")
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
            ctx.logger.debug(f"保留文件 (匹配规则): {rel_path}  ({_fmt_size(size)})")
            return True

        return True

    def _transfer(self, src_path, file_size, rel_path, stats, is_copy):
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
                    self.dest_backend.remove_file(dest_path)

                else:
                    return

            if is_copy:
                self.dest_backend.copy_file(src_path, new_dest_path)
            else:
                self.dest_backend.move_file(src_path, new_dest_path)

            stats.record_success(bytes_transferred=file_size)
            ctx.history_mgr.record_success(src_path)

            size_str = _fmt_size(file_size, binary=True)
            if file_exists and self.conflict_policy == "overwrite":
                ctx.logger.success(f"覆盖同名文件: {rel_path} ({size_str})")
            elif file_exists and self.conflict_policy == "copy":
                new_rel = os.path.relpath(new_dest_path, self.dest_backend.root_path)
                ctx.logger.success(f"目标存在，创建副本: {new_rel} ({size_str})")
            else:
                ctx.logger.success(f"{action_name}文件: {rel_path} ({size_str})")

        except Exception as e:
            count = ctx.history_mgr.record_failure(src_path)
            ctx.logger.error(f"{action_name}文件失败 ({count} 次): {rel_path}\n{e}")
            stats.record_error()

    def _delete(self, src_path, file_size, stats):
        """执行文件删除。"""
        ctx = AppContext.get()
        rel_path = os.path.relpath(src_path, self.source_root)
        try:
            _fs_delete(src_path)
            ctx.logger.success(
                f"删除文件: {rel_path}  ({_fmt_size(file_size, binary=True)})"
            )
            ctx.history_mgr.record_success(src_path)
            stats.record_deleted()
        except OSError as e:
            count = ctx.history_mgr.record_failure(src_path)
            ctx.logger.error(f"删除文件失败 ({count} 次): {rel_path}\nError: {e}")


# ============================================================
# 任务处理器基类
# ============================================================


class BaseTaskHandler(ABC):
    def __init__(self, task, dest_backend, now=None):
        self.task = task
        self.now = now or time.time()
        self.stats = MoverStats()
        self.source_root = task.get("source")
        self.dest_root = task.get("dest")
        self.dest_backend = dest_backend
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
        self.executor = ActionExecutor(task, dest_backend)

    def validate(self):
        return validate_task_config(self.task, self.task_mode)

    @abstractmethod
    def execute(self):
        pass
