import os
import time

from src.app_context import AppContext
from src.presentation import fmt_size, print_task_header, print_task_summary
from src.core.types import FileAction
from src.utils import parse_size_string, match_pattern
from src.operations.fs_ops import clean_empty_dirs
from src.core.registry import register_handler
from src.core.handlers.base import BaseTaskHandler


class RotateGroupManager:
    def __init__(self, size_limit, count_limit, rotate_size_rules, rotate_count_rules):
        self.group_stats = {}
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

    def _get_rotate_groups(self, rel_path):
        groups = []
        for pattern, limit in self.rotate_size_rules.items():
            if match_pattern(rel_path, pattern):
                groups.append(("size", "pattern", pattern))
        for pattern, limit in self.rotate_count_rules.items():
            if match_pattern(rel_path, pattern):
                groups.append(("count", "pattern", pattern))
        return groups

    def add_file(self, rel_path, size):
        groups = [("global", "global")]
        self.group_stats[("global", "global")]["size"] += size
        self.group_stats[("global", "global")]["count"] += 1

        matched_groups = self._get_rotate_groups(rel_path)
        for g_id in matched_groups:
            groups.append(g_id)
            if g_id not in self.group_stats:
                g_type, g_subtype, g_key = g_id
                limit = (
                    self.rotate_size_rules.get(g_key)
                    if g_type == "size"
                    else self.rotate_count_rules.get(g_key)
                )
                self.group_stats[g_id] = {"size": 0, "count": 0, "limit": limit}
            self.group_stats[g_id]["size"] += size
            self.group_stats[g_id]["count"] += 1

        return groups

    def _is_group_exceeded(self, g_id, stats):
        if g_id == ("global", "global"):
            if stats["size_limit"] > 0 and stats["size"] > stats["size_limit"]:
                return True
            if stats["count_limit"] > 0 and stats["count"] > stats["count_limit"]:
                return True
            return False

        g_type = g_id[0]
        if g_type == "size":
            return stats["limit"] > 0 and stats["size"] > stats["limit"]
        elif g_type == "count":
            return stats["limit"] > 0 and stats["count"] > stats["limit"]
        return False

    def is_any_group_exceeded(self):
        for g_id, stats in self.group_stats.items():
            if self._is_group_exceeded(g_id, stats):
                return True
        return False

    def is_file_needs_rotation(self, groups):
        for g_id in groups:
            if self._is_group_exceeded(g_id, self.group_stats[g_id]):
                return True
        return False

    def remove_file(self, groups, size):
        for g_id in groups:
            self.group_stats[g_id]["size"] -= size
            self.group_stats[g_id]["count"] -= 1

    def log_unmet_limits(self):
        ctx = AppContext.get()
        global_stats = self.group_stats[("global", "global")]
        if (
            global_stats["size_limit"] > 0
            and global_stats["size"] > global_stats["size_limit"]
        ):
            ctx.logger.warning(
                f"已无可处理文件，但目录体积 ({fmt_size(global_stats['size'], binary=True)}) 仍未满足限制 ({fmt_size(global_stats['size_limit'], binary=True)})，请检查配置。"
            )
        if (
            global_stats["count_limit"] > 0
            and global_stats["count"] > global_stats["count_limit"]
        ):
            ctx.logger.warning(
                f"已无可处理文件，但文件数量 ({global_stats['count']}) 仍未满足限制 ({global_stats['count_limit']})，请检查配置。"
            )

        for g_id, stats in self.group_stats.items():
            if g_id == ("global", "global"):
                continue
            if self._is_group_exceeded(g_id, stats):
                g_type, g_subtype, g_key = g_id
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
class RotateHandler(BaseTaskHandler):
    def execute(self):
        if not self.validate():
            return

        print_task_header(self.task)

        ctx = AppContext.get()

        if not os.path.exists(self.source_root):
            ctx.logger.error(f"源目录不存在: {self.source_root}")
            return

        size_limit = parse_size_string(self.task.get("size_limit", "0"))
        count_limit = int(self.task.get("count_limit", 0))

        rotate_rules = self.task.get("rotate_rules", {})
        raw_rotate_size = rotate_rules.get("size", {})
        raw_rotate_count = rotate_rules.get("count", {})

        rotate_size_rules = {
            k: parse_size_string(v) for k, v in raw_rotate_size.items()
        }
        rotate_count_rules = {k: int(v) for k, v in raw_rotate_count.items()}

        rotate_mgr = RotateGroupManager(
            size_limit, count_limit, rotate_size_rules, rotate_count_rules
        )

        start_time = time.time()

        all_files = []
        for root, dirs, files in os.walk(self.source_root):
            for file in files:
                src_path = os.path.join(root, file)
                try:
                    file_stat = os.stat(src_path)
                    size = file_stat.st_size
                    mtime = file_stat.st_mtime
                    rel_path = os.path.relpath(src_path, self.source_root)

                    groups = rotate_mgr.add_file(rel_path, size)

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
            )
            return

        all_files.sort(key=lambda x: x["mtime"])

        for f in all_files:
            if not rotate_mgr.is_any_group_exceeded():
                break

            if not rotate_mgr.is_file_needs_rotation(f["groups"]):
                continue

            if self.checker.should_skip(
                f["path"], f["rel_path"], f["mtime"], 0, self.stats
            ):
                continue

            action = self.policy.decide(f["rel_path"], f["size"])

            if action == FileAction.TRANSFER and not self.dest_root:
                ctx.logger.warning(f"跳过 (未配置目标目录): {f['rel_path']}")
                continue

            success = self.executor.execute(
                action,
                f["path"],
                f["rel_path"],
                f["size"],
                self.stats,
                is_copy=False,
            )
            if not success:
                break

            if not os.path.exists(f["path"]):
                rotate_mgr.remove_file(f["groups"], f["size"])

        rotate_mgr.log_unmet_limits()

        if self.remove_empty_dirs:
            ctx = AppContext.get()
            clean_empty_dirs(self.source_root, logger=ctx.logger)

        end_time = time.time()
        print_task_summary(
            self.stats,
            end_time - start_time,
            self.stats.total_bytes,
        )
