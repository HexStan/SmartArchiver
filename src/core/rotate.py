import os
import time
from humanfriendly import format_size
from src.utils import (
    parse_size_string,
    match_pattern,
    clean_empty_dirs,
    get_dir_size_and_mtime,
    get_unique_dest,
)
from src.core.types import FileAction
from src.core.actions import print_task_header, print_task_summary
from src.core.handlers import BaseModeHandler
from src.core.io_ops import move_file, delete_dir


class RotateGroupManager:
    """
    管理轮转模式下的分组统计和限制逻辑
    """

    def __init__(self, size_limit, count_limit, rotate_size_rules, rotate_count_rules):
        self.group_stats = {}
        self.group_stats[("global", "global")] = {
            "size": 0,
            "count": 0,
            "size_limit": size_limit,
            "count_limit": count_limit,
        }

        self.dir_size_rules = {}
        self.file_size_rules = {}
        self.dir_count_rules = {}
        self.file_count_rules = {}

        # Initialize rule groups
        for pattern, limit in rotate_size_rules.items():
            if pattern.endswith("/"):
                self.dir_size_rules[pattern[:-1]] = limit
            else:
                self.file_size_rules[pattern] = limit
            self.group_stats[("size", "pattern", pattern)] = {
                "size": 0,
                "count": 0,
                "limit": limit,
            }

        for pattern, limit in rotate_count_rules.items():
            if pattern.endswith("/"):
                self.dir_count_rules[pattern[:-1]] = limit
            else:
                self.file_count_rules[pattern] = limit
            self.group_stats[("count", "pattern", pattern)] = {
                "size": 0,
                "count": 0,
                "limit": limit,
            }

    def _get_rotate_groups(self, rel_path, is_dir=False):
        """
        返回该文件或目录所属的所有轮转组。
        组的标识为 (group_type, group_subtype, group_key)
        """
        groups = []

        size_rules = self.dir_size_rules if is_dir else self.file_size_rules
        count_rules = self.dir_count_rules if is_dir else self.file_count_rules

        for pattern, limit in size_rules.items():
            if match_pattern(rel_path, pattern):
                original_pattern = pattern + "/" if is_dir else pattern
                groups.append(("size", "pattern", original_pattern))

        for pattern, limit in count_rules.items():
            if match_pattern(rel_path, pattern):
                original_pattern = pattern + "/" if is_dir else pattern
                groups.append(("count", "pattern", original_pattern))

        return groups

    def add_file(self, rel_path, size, is_dir=False):
        """
        将文件或目录加入统计，并返回其所属的组列表
        """
        groups = [("global", "global")]
        self.group_stats[("global", "global")]["size"] += size
        self.group_stats[("global", "global")]["count"] += 1

        matched_groups = self._get_rotate_groups(rel_path, is_dir)
        for g_id in matched_groups:
            groups.append(g_id)
            if g_id not in self.group_stats:
                g_type, g_subtype, g_key = g_id
                if g_type == "size":
                    limit = (
                        self.dir_size_rules.get(g_key[:-1])
                        if is_dir
                        else self.file_size_rules.get(g_key)
                    )
                else:
                    limit = (
                        self.dir_count_rules.get(g_key[:-1])
                        if is_dir
                        else self.file_count_rules.get(g_key)
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
        """
        检查是否任意一个组超出了限制
        """
        for g_id, stats in self.group_stats.items():
            if self._is_group_exceeded(g_id, stats):
                return True
        return False

    def is_file_needs_rotation(self, groups):
        """
        检查文件所属的组中是否有超出限制的
        """
        for g_id in groups:
            if self._is_group_exceeded(g_id, self.group_stats[g_id]):
                return True
        return False

    def remove_file(self, groups, size):
        """
        文件被轮转后，从统计中扣除
        """
        for g_id in groups:
            self.group_stats[g_id]["size"] -= size
            self.group_stats[g_id]["count"] -= 1

    def log_unmet_limits(self, logger):
        """
        记录最终仍未满足限制的警告信息
        """
        global_stats = self.group_stats[("global", "global")]
        if (
            global_stats["size_limit"] > 0
            and global_stats["size"] > global_stats["size_limit"]
        ):
            logger.warning(
                f"已无可处理文件，但目录体积 ({format_size(global_stats['size'], binary=True)}) 仍未满足限制 ({format_size(global_stats['size_limit'], binary=True)})，请检查配置。"
            )
        if (
            global_stats["count_limit"] > 0
            and global_stats["count"] > global_stats["count_limit"]
        ):
            logger.warning(
                f"已无可处理文件，但文件数量 ({global_stats['count']}) 仍未满足限制 ({global_stats['count_limit']})，请检查配置。"
            )

        for g_id, stats in self.group_stats.items():
            if g_id == ("global", "global"):
                continue
            if self._is_group_exceeded(g_id, stats):
                g_type, g_subtype, g_key = g_id
                limit_str = (
                    format_size(stats["limit"], binary=True)
                    if g_type == "size"
                    else str(stats["limit"])
                )
                current_str = (
                    format_size(stats["size"], binary=True)
                    if g_type == "size"
                    else str(stats["count"])
                )
                logger.warning(
                    f"已无可处理文件，但规则 {g_key}: {g_type} 仍未满足限制 (当前 {current_str}，限制 {limit_str})，请检查配置。"
                )


class RotateModeHandler(BaseModeHandler):
    def execute(self):
        if not self.validate():
            return

        print_task_header(
            self.task, self.task_mode, self.source_root, self.dest_root, 0, self.logger
        )

        if not os.path.exists(self.source_root):
            self.logger.error(f"源目录不存在: {self.source_root}")
            return

        if self.dest_root and not os.path.isdir(self.dest_root):
            self.logger.critical("!!! CRUCIAL: 目标目录不存在 !!!")
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

        all_items = []
        for root, dirs, files in os.walk(self.source_root):
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
                rotate_groups = rotate_mgr._get_rotate_groups(rel_dir_path, is_dir=True)

                is_candidate = bool(rotate_groups) or action != FileAction.TRANSFER

                if is_candidate:
                    if not dir_size_cache:
                        s, m = get_dir_size_and_mtime(dir_path)
                        dir_size_cache.append(s)
                        dir_mtime_cache.append(m)

                    size = dir_size_cache[0]
                    mtime = dir_mtime_cache[0]

                    groups = rotate_mgr.add_file(rel_dir_path, size, is_dir=True)

                    all_items.append(
                        {
                            "name": d,
                            "path": dir_path,
                            "rel_path": rel_dir_path,
                            "size": size,
                            "mtime": mtime,
                            "groups": groups,
                            "is_dir": True,
                        }
                    )
                    dirs_to_remove.append(d)

            for d in dirs_to_remove:
                dirs.remove(d)

            for file in files:
                src_path = os.path.join(root, file)
                try:
                    file_stat = os.stat(src_path)
                    size = file_stat.st_size
                    mtime = file_stat.st_mtime
                    rel_path = os.path.relpath(src_path, self.source_root)

                    groups = rotate_mgr.add_file(rel_path, size, is_dir=False)

                    all_items.append(
                        {
                            "name": file,
                            "path": src_path,
                            "rel_path": rel_path,
                            "size": size,
                            "mtime": mtime,
                            "groups": groups,
                            "is_dir": False,
                        }
                    )
                except OSError:
                    continue

        if not rotate_mgr.is_any_group_exceeded():
            self.logger.info("当前未超过限制，无需轮转。")
            end_time = time.time()
            duration_str, total_size_str, speed_str = self.local_stats.calculate_speed(
                start_time, end_time
            )
            print_task_summary(
                self.local_stats, duration_str, total_size_str, speed_str, self.logger
            )
            return

        all_items.sort(key=lambda x: x["mtime"])

        for item in all_items:
            if not rotate_mgr.is_any_group_exceeded():
                break

            if not rotate_mgr.is_file_needs_rotation(item["groups"]):
                continue

            if self.check_file_common(
                item["path"], item["rel_path"], item["size"], item["mtime"], 0
            ):
                continue

            action = self.policy.decide(
                item["rel_path"], item["size"], is_dir=item.get("is_dir", False)
            )

            if action == FileAction.TRANSFER and not self.dest_root:
                self.logger.warning(f"跳过 (未配置目标路径): {item['rel_path']}")
                continue

            if item.get("is_dir", False):
                if action == FileAction.DELETE:
                    try:
                        delete_dir(item["path"])
                        self.logger.success(
                            f"删除目录: {item['rel_path']} ({format_size(item['size'], binary=True)})"
                        )
                        self.history_mgr.record_success(item["path"])
                        self.local_stats.deleted += 1
                    except OSError as e:
                        count = self.history_mgr.record_failure(item["path"])
                        self.logger.error(
                            f"删除目录失败 ({count} 次): {item['rel_path']}\nError: {e}"
                        )
                elif action == FileAction.TRANSFER:
                    dest_path = os.path.join(self.dest_root, item["rel_path"])
                    try:
                        if os.path.exists(dest_path):
                            dest_path = get_unique_dest(dest_path)

                        move_file(item["path"], dest_path)
                        self.logger.success(
                            f"移动目录: {item['rel_path']} ({format_size(item['size'], binary=True)})"
                        )
                        self.history_mgr.record_success(item["path"])
                        self.local_stats.success += 1
                        self.local_stats.total_bytes += item["size"]
                    except Exception as e:
                        count = self.history_mgr.record_failure(item["path"])
                        self.logger.error(
                            f"移动目录失败 ({count} 次): {item['rel_path']}\nError: {e}"
                        )
                        self.local_stats.error += 1
                elif action == FileAction.SKIP:
                    self.local_stats.kept += 1
                    self.logger.debug(
                        f"保留目录 (匹配规则): {item['rel_path']}  ({format_size(item['size'], binary=True)})"
                    )
            else:
                self.execute_action(
                    action,
                    item["path"],
                    item["rel_path"],
                    item["size"],
                    move_file,
                    "移动",
                )

            if not os.path.exists(item["path"]):
                rotate_mgr.remove_file(item["groups"], item["size"])

        rotate_mgr.log_unmet_limits(self.logger)

        if self.remove_empty_dirs:
            clean_empty_dirs(self.source_root, self.logger)

        end_time = time.time()
        duration_str, total_size_str, speed_str = self.local_stats.calculate_speed(
            start_time, end_time
        )
        print_task_summary(
            self.local_stats, duration_str, total_size_str, speed_str, self.logger
        )


def handle_rotate_mode(
    task, config, logger, history_mgr, source_root, dest_root, task_mode
):
    handler = RotateModeHandler(task, config, logger, history_mgr)
    handler.execute()
