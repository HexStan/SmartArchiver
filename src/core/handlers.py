import os
import time
from humanfriendly import format_size

from src.utils import is_file_locked, get_dir_size_and_mtime, clean_empty_dirs
from src.core.types import FileAction, MoverStats
from src.core.filters import FileFilterPolicy
from src.core.actions import (
    validate_task_config,
    print_task_header,
    print_task_summary,
    delete_file,
    transfer_file,
)
from src.core.io_ops import copy_file, move_file, delete_dir


class BaseModeHandler:
    def __init__(self, task, config, logger, history_mgr, now=None):
        self.task = task
        self.config = config
        self.logger = logger
        self.history_mgr = history_mgr
        self.now = now or time.time()
        self.local_stats = MoverStats()
        self.source_root = task.get("source")
        self.dest_root = task.get("dest")
        self.task_mode = task.get("mode", "").lower()
        self.max_retries = config.get("max_retries", 3)
        self.conflict_policy = task.get("conflict_policy", "").lower()
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
        self._dest_checked = False
        self._dest_valid = False

    def validate(self):
        return validate_task_config(self.task, self.task_mode, self.logger)

    def check_file_common(
        self, src_path, rel_path, size, mtime, mtime_threshold_seconds=0
    ):
        """
        通用文件检查：历史失败次数、时间阈值、文件锁
        返回 True 表示应该跳过，False 表示可以继续处理
        """
        should_skip, fail_count = self.history_mgr.should_skip(
            src_path, self.max_retries
        )
        if should_skip:
            self.local_stats.dropped += 1
            self.logger.warning(f"跳过文件 (多次失败): {src_path}")
            return True

        if (self.now - mtime) <= mtime_threshold_seconds:
            return True

        if is_file_locked(src_path):
            self.local_stats.locked_skipped += 1
            self.logger.warning(f"跳过文件 (被锁定): {rel_path}")
            return True

        return False

    def execute_action(
        self, action, src_path, rel_path, size, transfer_func, action_name
    ):
        if action == FileAction.TRANSFER:
            if not self.dest_root:
                self.logger.warning(f"跳过文件 (目标目录非法): {rel_path}")
                return False

            if not self._dest_checked:
                self._dest_checked = True
                if not os.path.isdir(self.dest_root):
                    self.logger.critical("!!! CRUCIAL: 目标目录不存在 !!!")
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
                self.logger,
                self.local_stats,
                self.history_mgr,
                self.conflict_policy,
                transfer_func,
                action_name,
            )
            return True
        elif action == FileAction.DELETE:
            delete_file(
                src_path,
                size,
                self.source_root,
                self.logger,
                self.local_stats,
                self.history_mgr,
            )
            return True
        elif action == FileAction.SKIP:
            self.local_stats.kept += 1
            self.logger.debug(
                f"保留文件 (匹配规则): {rel_path}  ({format_size(size, binary=True)})"
            )
            return True
        return True


class StandardModeHandler(BaseModeHandler):
    """
    处理 move, copy, whitelist_move, whitelist_copy
    """

    def execute(self):
        if not self.validate():
            return

        mtime_threshold_minutes = self.task.get("mtime_threshold_minutes", 0)
        mtime_threshold_seconds = mtime_threshold_minutes * 60

        print_task_header(
            self.task,
            self.task_mode,
            self.source_root,
            self.dest_root,
            mtime_threshold_seconds,
            self.logger,
        )

        if not os.path.exists(self.source_root):
            self.logger.error(f"源目录不存在: {self.source_root}")
            return

        is_copy = self.task_mode in ["copy", "whitelist_copy"]
        transfer_func = copy_file if is_copy else move_file
        action_name = "复制" if is_copy else "移动"

        start_time = time.time()

        for root, dirs, files in os.walk(self.source_root):
            self._process_directories(dirs, root, mtime_threshold_seconds)
            success = self._process_files(
                files, root, mtime_threshold_seconds, transfer_func, action_name
            )
            if not success:
                break

        if self.remove_empty_dirs and not is_copy:
            clean_empty_dirs(self.source_root, self.logger)

        end_time = time.time()
        duration_str, total_size_str, speed_str = self.local_stats.calculate_speed(
            start_time, end_time
        )
        print_task_summary(
            self.local_stats, duration_str, total_size_str, speed_str, self.logger
        )

    def _process_directories(self, dirs, root, mtime_threshold_seconds):
        dirs_to_remove = []
        for d in dirs:
            dir_path = os.path.join(root, d)
            rel_dir_path = os.path.relpath(dir_path, self.source_root)

            dir_size_cache = []
            dir_mtime_cache = []

            def get_size():
                if not dir_size_cache:
                    s, m = get_dir_size_and_mtime(dir_path)
                    dir_size_cache.append(s)
                    dir_mtime_cache.append(m)
                return dir_size_cache[0]

            action = self.policy.decide(rel_dir_path, get_size, is_dir=True)

            if action == FileAction.DELETE:
                if not dir_mtime_cache:
                    s, m = get_dir_size_and_mtime(dir_path)
                    dir_size_cache.append(s)
                    dir_mtime_cache.append(m)
                dir_mtime = dir_mtime_cache[0]
                dir_size = dir_size_cache[0]

                if (self.now - dir_mtime) > mtime_threshold_seconds:
                    try:
                        delete_dir(dir_path)
                        self.logger.success(
                            f"删除目录: {rel_dir_path} ({format_size(dir_size, binary=True)})"
                        )
                        self.local_stats.deleted += 1
                    except OSError as e:
                        self.logger.error(f"删除目录失败: {rel_dir_path}\nError: {e}")
                dirs_to_remove.append(d)
            elif action == FileAction.SKIP:
                size_str = (
                    f" ({format_size(dir_size_cache[0], binary=True)})"
                    if dir_size_cache
                    else ""
                )
                self.logger.debug(f"保留目录 (匹配规则): {rel_dir_path}{size_str}")
                self.local_stats.kept += 1
                dirs_to_remove.append(d)

        for d in dirs_to_remove:
            dirs.remove(d)

    def _process_files(
        self, files, root, mtime_threshold_seconds, transfer_func, action_name
    ):
        for file in files:
            src_path = os.path.join(root, file)
            rel_path = os.path.relpath(src_path, self.source_root)

            try:
                file_stat = os.stat(src_path)
                mtime = file_stat.st_mtime
                size = file_stat.st_size
            except OSError:
                continue

            if self.check_file_common(
                src_path, rel_path, size, mtime, mtime_threshold_seconds
            ):
                continue

            action = self.policy.decide(rel_path, size)
            success = self.execute_action(
                action, src_path, rel_path, size, transfer_func, action_name
            )
            if not success:
                return False
        return True
