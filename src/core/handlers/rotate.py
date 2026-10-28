import os
import time
from operator import itemgetter
from typing import TypedDict, override

from src.app.context import AppContext
from src.core.filters import FileFilterPolicy
from src.core.handlers.base import TransferTaskHandler
from src.core.patterns import match_pattern
from src.core.registry import register_handler
from src.core.task_config import RotateTaskConfig, RuleSetConfig
from src.core.task_plan import TaskSpec
from src.fs import clean_empty_dirs
from src.output.presentation import fmt_size, print_task_summary

# 分组 ID：全局组为 ("global", "global")，规则组为 (类型, "pattern", 模式)，
# 元数不齐，故用 tuple[str, ...]
GroupId = tuple[str, ...]


class _FileRecord(TypedDict):
    """轮转候选文件记录。"""

    name: str
    path: str
    rel_path: str
    norm_path: str
    size: int
    mtime: float
    groups: list[GroupId]


class RotateGroupManager:
    def __init__(
        self,
        size_limit: int,
        count_limit: int,
        rotate_size_rules: dict[str, int],
        rotate_count_rules: dict[str, int],
    ) -> None:
        self.group_stats: dict[GroupId, dict[str, int]] = {}
        self.group_stats[("global", "global")] = {
            "size": 0,
            "count": 0,
            "size_limit": size_limit,
            "count_limit": count_limit,
        }
        self.rotate_size_rules = rotate_size_rules
        self.rotate_count_rules = rotate_count_rules

        for pattern, limit in rotate_size_rules.items():
            self.group_stats[("size", "pattern", pattern)] = {
                "size": 0,
                "count": 0,
                "limit": limit,
            }
        for pattern, limit in rotate_count_rules.items():
            self.group_stats[("count", "pattern", pattern)] = {
                "size": 0,
                "count": 0,
                "limit": limit,
            }

    def _get_rotate_groups(self, rel_path: str) -> list[GroupId]:
        groups: list[GroupId] = []
        for pattern in self.rotate_size_rules:
            if match_pattern(rel_path, pattern):
                groups.append(("size", "pattern", pattern))
        for pattern in self.rotate_count_rules:
            if match_pattern(rel_path, pattern):
                groups.append(("count", "pattern", pattern))
        return groups

    def add_file(self, rel_path: str, size: int) -> list[GroupId]:
        groups: list[GroupId] = [("global", "global")]
        self.group_stats[("global", "global")]["size"] += size
        self.group_stats[("global", "global")]["count"] += 1

        matched_groups = self._get_rotate_groups(rel_path)
        for g_id in matched_groups:
            groups.append(g_id)
            if g_id not in self.group_stats:
                g_type, g_key = g_id[0], g_id[2]
                limit = (
                    self.rotate_size_rules[g_key]
                    if g_type == "size"
                    else self.rotate_count_rules[g_key]
                )
                self.group_stats[g_id] = {"size": 0, "count": 0, "limit": limit}
            self.group_stats[g_id]["size"] += size
            self.group_stats[g_id]["count"] += 1

        return groups

    def _is_group_exceeded(self, g_id: GroupId, stats: dict[str, int]) -> bool:
        if g_id == ("global", "global"):
            size_exceeded = stats["size_limit"] > 0 and stats["size"] > stats["size_limit"]
            count_exceeded = stats["count_limit"] > 0 and stats["count"] > stats["count_limit"]
            return size_exceeded or count_exceeded

        g_type = g_id[0]
        if g_type == "size":
            return stats["limit"] > 0 and stats["size"] > stats["limit"]
        elif g_type == "count":
            return stats["limit"] > 0 and stats["count"] > stats["limit"]
        return False

    def is_any_group_exceeded(self) -> bool:
        for g_id, stats in self.group_stats.items():
            if self._is_group_exceeded(g_id, stats):
                return True
        return False

    def is_file_needs_rotation(self, groups: list[GroupId]) -> bool:
        for g_id in groups:
            if self._is_group_exceeded(g_id, self.group_stats[g_id]):
                return True
        return False

    def remove_file(self, groups: list[GroupId], size: int) -> None:
        for g_id in groups:
            self.group_stats[g_id]["size"] -= size
            self.group_stats[g_id]["count"] -= 1

    def log_unmet_limits(self) -> None:
        ctx = AppContext.get()
        global_stats = self.group_stats[("global", "global")]
        if global_stats["size_limit"] > 0 and global_stats["size"] > global_stats["size_limit"]:
            ctx.logger.warning(
                f"已无可处理文件，但目录体积 ({fmt_size(global_stats['size'], binary=True)}) 仍未满足限制 ({fmt_size(global_stats['size_limit'], binary=True)})，请检查配置。"
            )
        if global_stats["count_limit"] > 0 and global_stats["count"] > global_stats["count_limit"]:
            ctx.logger.warning(
                f"已无可处理文件，但文件数量 ({global_stats['count']}) 仍未满足限制 ({global_stats['count_limit']})，请检查配置。"
            )

        for g_id, stats in self.group_stats.items():
            if g_id == ("global", "global"):
                continue
            if self._is_group_exceeded(g_id, stats):
                g_type, g_key = g_id[0], g_id[2]
                limit_str = (
                    fmt_size(stats["limit"], binary=True)
                    if g_type == "size"
                    else str(stats["limit"])
                )
                current_str = (
                    fmt_size(stats["size"], binary=True)
                    if g_type == "size"
                    else str(stats["count"])
                )
                ctx.logger.warning(
                    f"已无可处理文件，但规则 {g_key}: {g_type} 仍未满足限制 (当前 {current_str}，限制 {limit_str})，请检查配置。"
                )


@register_handler("rotate")
class RotateHandler(TransferTaskHandler):
    @override
    def __init__(self, spec: TaskSpec, now: float | None = None) -> None:
        super().__init__(spec, now)
        # 注册表分派保证（task_config.TASK_MODELS 与 registry 由同一组模式名驱动）
        assert isinstance(self.task, RotateTaskConfig), "rotate 模式的配置模型分派错误"
        self.config: RotateTaskConfig = self.task
        # 限额已由配置模型在加载期解析为字节数/数量，此处只装配；
        # include 不参与 rotate（模型层拒绝 include_rules），规则表只挂排除与删除
        self.policy = FileFilterPolicy(
            RuleSetConfig(), self.config.exclude_rules, self.config.delete_rules
        )

    @override
    def _execute(self) -> bool:
        ctx = AppContext.get()

        if not os.path.exists(self.source_root):
            ctx.logger.error(f"源目录不存在: {self.source_root}")
            return False

        rotate_mgr = RotateGroupManager(
            self.config.size_limit,
            self.config.count_limit,
            self.config.rotate_rules.size,
            self.config.rotate_rules.count,
        )

        start_time = time.time()

        all_files: list[_FileRecord] = []
        dir_sizes: dict[str, int] = {}
        for root, dirs, files in os.walk(self.source_root):
            for file in files:
                src_path = os.path.join(root, file)
                try:
                    file_stat = os.stat(src_path)
                    size = file_stat.st_size
                    mtime = file_stat.st_mtime
                    rel_path = os.path.relpath(src_path, self.source_root)

                    groups = rotate_mgr.add_file(rel_path, size)

                    normalized = rel_path.replace("\\", "/")
                    parent = os.path.dirname(normalized)
                    while parent:
                        dir_sizes[parent] = dir_sizes.get(parent, 0) + size
                        parent = os.path.dirname(parent)

                    all_files.append(
                        {
                            "name": file,
                            "path": src_path,
                            "rel_path": rel_path,
                            "norm_path": normalized,
                            "size": size,
                            "mtime": mtime,
                            "groups": groups,
                        }
                    )
                except OSError:
                    continue

        if not rotate_mgr.is_any_group_exceeded():
            ctx.logger.info("当前未超过限制，无需轮转。")
            end_time = time.time()
            print_task_summary(
                self.stats,
                end_time - start_time,
                self.stats.total_bytes,
                ctx.logger,
            )
            return True

        # mtime 升序（最旧优先）；相等 mtime 依归一化相对路径（正斜杠）排序，
        # 消除目录枚举顺序的平台差异，同 mtime 文件的处理顺序跨平台一致
        all_files.sort(key=itemgetter("mtime", "norm_path"))

        completed = True
        for f in all_files:
            if not rotate_mgr.is_any_group_exceeded():
                break

            if ctx.lifecycle.should_stop_before_file():
                completed = False
                break

            if not rotate_mgr.is_file_needs_rotation(f["groups"]):
                continue

            if self.checker.should_skip(f["path"], f["rel_path"], f["mtime"], 0, self.stats):
                continue

            action = self.policy.decide(f["rel_path"], f["size"], parent_dir_sizes=dir_sizes)

            success = self.executor.execute(
                action,
                f["path"],
                f["rel_path"],
                f["size"],
                self.stats,
                is_copy=False,
            )
            if not success:
                # 目标目录致命错误导致的中断
                completed = False
                break

            if not os.path.exists(f["path"]):
                rotate_mgr.remove_file(f["groups"], f["size"])

        rotate_mgr.log_unmet_limits()

        if self.remove_empty_dirs:
            clean_empty_dirs(self.source_root)

        end_time = time.time()
        print_task_summary(
            self.stats,
            end_time - start_time,
            self.stats.total_bytes,
            ctx.logger,
        )
        return completed
