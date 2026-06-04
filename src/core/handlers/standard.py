import os
import time

from src.app_context import AppContext
from src.presentation import fmt_size, print_task_header, print_task_summary
from src.core.types import FileAction
from src.operations.fs_ops import (
    get_dir_size_and_mtime,
    clean_empty_dirs,
    delete_path,
)
from src.core.registry import register_handler
from src.core.handlers.base import BaseTaskHandler


@register_handler("move", "copy", "whitelist_move", "whitelist_copy")
class StandardHandler(BaseTaskHandler):
    def execute(self):
        if not self.validate():
            return

        mtime_threshold_minutes = self.task.get("mtime_threshold_minutes", 0)
        mtime_threshold_seconds = mtime_threshold_minutes * 60

        print_task_header(self.task)

        ctx = AppContext.get()

        if not os.path.exists(self.source_root):
            ctx.logger.error(f"源目录不存在: {self.source_root}")
            return

        is_copy = self.task_mode in ["copy", "whitelist_copy"]

        start_time = time.time()

        for root, dirs, files in os.walk(self.source_root):
            self._process_directories(dirs, root, mtime_threshold_seconds)
            success = self._process_files(files, root, mtime_threshold_seconds, is_copy)
            if not success:
                break

        if self.remove_empty_dirs and not is_copy:
            clean_empty_dirs(self.source_root, logger=ctx.logger)

        end_time = time.time()
        print_task_summary(
            self.stats,
            end_time - start_time,
            self.stats.total_bytes,
        )

    def _process_directories(self, dirs, root, mtime_threshold_seconds):
        ctx = AppContext.get()
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
                        delete_path(dir_path)
                        ctx.logger.success(
                            f"删除目录: {rel_dir_path} ({fmt_size(dir_size, binary=True)})"
                        )
                        self.stats.record_deleted()
                    except OSError as e:
                        ctx.logger.error(f"删除目录失败: {rel_dir_path}\nError: {e}")
                dirs_to_remove.append(d)
            elif action == FileAction.SKIP:
                size_str = (
                    f" ({fmt_size(dir_size_cache[0], binary=True)})"
                    if dir_size_cache
                    else ""
                )
                ctx.logger.debug(f"保留目录 (匹配规则): {rel_dir_path}{size_str}")
                self.stats.record_kept()
                dirs_to_remove.append(d)

        for d in dirs_to_remove:
            dirs.remove(d)

    def _process_files(self, files, root, mtime_threshold_seconds, is_copy):
        for file in files:
            src_path = os.path.join(root, file)
            rel_path = os.path.relpath(src_path, self.source_root)

            try:
                file_stat = os.stat(src_path)
                mtime = file_stat.st_mtime
                size = file_stat.st_size
            except OSError:
                continue

            if self.checker.should_skip(
                src_path, rel_path, mtime, mtime_threshold_seconds, self.stats
            ):
                continue

            action = self.policy.decide(rel_path, size)
            success = self.executor.execute(
                action, src_path, rel_path, size, self.stats, is_copy
            )
            if not success:
                return False
        return True
