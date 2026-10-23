import os
import time
from typing import Any, TypedDict, override

from src.app.context import AppContext
from src.core.handlers.base import BaseTaskHandler
from src.core.patterns import match_pattern, parse_size_string
from src.core.registry import register_handler
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


def _parse_count(label: str, value: Any) -> int:
    """数量配置值强转；无法解析时抛出中文 ValueError（构造期由 create_handler 捕获）。"""
    try:
        return int(value)
    except ValueError as e:
        raise ValueError(f"{label} 的数量配置值 {value!r} 无效（应为整数）") from e


@register_handler("rotate")
class RotateHandler(BaseTaskHandler):
    @override
    def __init__(self, task: dict[str, Any], now: float | None = None) -> None:
        super().__init__(task, now)

        # 限额在构造期一次解析并存储：容量值解析失败抛 InvalidSizeError、
        # 数量值强转失败抛 ValueError，均由 registry.create_handler 统一
        # 捕获并跳过任务——validate / execute / header 展示不再解析。
        self.size_limit = parse_size_string(str(task.get("size_limit", "0")))
        self.count_limit = _parse_count("count_limit", task.get("count_limit", 0))
        rotate_rules = task.get("rotate_rules", {})
        self.rotate_size_rules = {
            key: parse_size_string(v) for key, v in rotate_rules.get("size", {}).items()
        }
        self.rotate_count_rules = {
            key: _parse_count(f"rotate_rules.count 规则 '{key}'", v)
            for key, v in rotate_rules.get("count", {}).items()
        }

    @override
    def validate(self) -> bool:
        if not super().validate():
            return False

        if (
            self.size_limit == 0
            and self.count_limit == 0
            and not self.rotate_size_rules
            and not self.rotate_count_rules
        ):
            AppContext.get().logger.error(
                "rotate 模式下必须配置 size_limit、count_limit、"
                "rotate_rules.size 或 rotate_rules.count 中的至少一项，跳过该任务。"
            )
            return False
        return True

    @override
    def _execute(self) -> bool:
        ctx = AppContext.get()

        if not os.path.exists(self.source_root):
            ctx.logger.error(f"源目录不存在: {self.source_root}")
            return False

        rotate_mgr = RotateGroupManager(
            self.size_limit, self.count_limit, self.rotate_size_rules, self.rotate_count_rules
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

        all_files.sort(key=lambda x: x["mtime"])

        completed = True
        for f in all_files:
            if not rotate_mgr.is_any_group_exceeded():
                break

            if ctx.shutdown.should_stop_before_file():
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
