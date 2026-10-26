import os
import time
from typing import override

from src import fs
from src.app.context import AppContext
from src.core.filters import FileFilterPolicy
from src.core.handlers.base import TransferTaskHandler
from src.core.registry import register_handler
from src.core.task_config import MoveCopyTaskConfig
from src.core.task_plan import TaskSpec
from src.core.types import FileAction
from src.fs import clean_empty_dirs, get_dir_size_and_mtime
from src.output.presentation import fmt_size, print_task_summary


@register_handler("move", "copy")
class StandardHandler(TransferTaskHandler):
    @override
    def __init__(self, spec: TaskSpec, now: float | None = None) -> None:
        super().__init__(spec, now)
        # 注册表分派保证（task_config.TASK_MODELS 与 registry 由同一组模式名驱动）
        assert isinstance(self.task, MoveCopyTaskConfig), "move/copy 模式的配置模型分派错误"
        self.config: MoveCopyTaskConfig = self.task
        self.policy = FileFilterPolicy(
            self.config.include_rules, self.config.exclude_rules, self.config.delete_rules
        )

    @override
    def _execute(self) -> bool:
        mtime_threshold_seconds = self.config.mtime_threshold_seconds

        ctx = AppContext.get()

        if not os.path.exists(self.source_root):
            ctx.logger.error(f"源目录不存在: {self.source_root}")
            return False

        is_copy = self.task_mode == "copy"

        start_time = time.time()

        completed = True
        for root, dirs, files in os.walk(self.source_root):
            # 文件系统返回的目录项顺序不保证有序（NTFS 按名称、ext4/APFS 按哈希），
            # 先排序再处理：处理顺序与平台无关，逐文件日志顺序可预测
            dirs.sort()
            files.sort()
            self._process_directories(dirs, root, mtime_threshold_seconds)
            success = self._process_files(files, root, mtime_threshold_seconds, is_copy)
            if not success:
                # 目标目录致命错误或停止指令导致的中断
                completed = False
                break

        if self.remove_empty_dirs and not is_copy:
            clean_empty_dirs(self.source_root)

        end_time = time.time()
        print_task_summary(
            self.stats,
            end_time - start_time,
            self.stats.total_bytes,
            ctx.logger,
        )
        return completed

    def _process_directories(
        self, dirs: list[str], root: str, mtime_threshold_seconds: float
    ) -> None:
        ctx = AppContext.get()
        dirs_to_remove: list[str] = []
        for d in dirs:
            dir_path = os.path.join(root, d)
            rel_dir_path = os.path.relpath(dir_path, self.source_root)

            dir_size_cache: list[int] = []
            dir_mtime_cache: list[float] = []

            def get_size() -> int:
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
                        ctx.logger.info(
                            f"正在删除目录: {rel_dir_path} ({fmt_size(dir_size, binary=True)})"
                        )
                        fs.delete_dir(dir_path)
                        self.stats.record_deleted()
                    except fs.FsOpError as e:
                        ctx.logger.error(f"删除目录失败: {e}")
                dirs_to_remove.append(d)
            elif action == FileAction.SKIP:
                size_str = (
                    f" ({fmt_size(dir_size_cache[0], binary=True)})" if dir_size_cache else ""
                )
                ctx.logger.debug(f"跳过目录 (匹配规则): {rel_dir_path}{size_str}")
                self.stats.record_kept()
                dirs_to_remove.append(d)

        for d in dirs_to_remove:
            dirs.remove(d)

    def _process_files(
        self, files: list[str], root: str, mtime_threshold_seconds: float, is_copy: bool
    ) -> bool:
        ctx = AppContext.get()

        for file in files:
            if ctx.lifecycle.should_stop_before_file():
                return False

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
            success = self.executor.execute(action, src_path, rel_path, size, self.stats, is_copy)
            if not success:
                return False
        return True
