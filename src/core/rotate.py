import os
import time
from humanfriendly import format_size
from src.utils import parse_size_string, match_pattern, is_file_locked, clean_empty_dirs
from src.core.types import FileAction, MoverStats
from src.core.filters import FileFilterPolicy
from src.core.actions import (
    print_task_header,
    print_task_summary,
    delete_file,
    move_file,
    process_directories,
)


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
        self.rotate_size_rules = rotate_size_rules
        self.rotate_count_rules = rotate_count_rules

        # Initialize rule groups
        for pattern, limit in rotate_size_rules.items():
            if pattern not in ["*", "*/"]:
                self.group_stats[("size", "pattern", pattern)] = {
                    "size": 0,
                    "count": 0,
                    "limit": limit,
                }
        for pattern, limit in rotate_count_rules.items():
            if pattern not in ["*", "*/"]:
                self.group_stats[("count", "pattern", pattern)] = {
                    "size": 0,
                    "count": 0,
                    "limit": limit,
                }

    def _get_rotate_groups(self, rel_path):
        """
        返回该文件所属的所有轮转组。
        组的标识为 (group_type, group_subtype, group_key)
        """
        groups = []

        for pattern, limit in self.rotate_size_rules.items():
            if pattern == "*":
                groups.append(("size", "name", os.path.basename(rel_path)))
            elif pattern == "*/":
                dir_name = os.path.dirname(rel_path.replace("\\", "/"))
                dir_name = dir_name + "/" if dir_name else "/"
                groups.append(("size", "dir", dir_name))
            else:
                if match_pattern(rel_path, pattern):
                    groups.append(("size", "pattern", pattern))

        for pattern, limit in self.rotate_count_rules.items():
            if pattern == "*":
                groups.append(("count", "name", os.path.basename(rel_path)))
            elif pattern == "*/":
                dir_name = os.path.dirname(rel_path.replace("\\", "/"))
                dir_name = dir_name + "/" if dir_name else "/"
                groups.append(("count", "dir", dir_name))
            else:
                if match_pattern(rel_path, pattern):
                    groups.append(("count", "pattern", pattern))

        return groups

    def add_file(self, rel_path, size):
        """
        将文件加入统计，并返回其所属的组列表
        """
        groups = [("global", "global")]
        self.group_stats[("global", "global")]["size"] += size
        self.group_stats[("global", "global")]["count"] += 1

        matched_groups = self._get_rotate_groups(rel_path)
        for g_type, g_subtype, g_key in matched_groups:
            g_id = (g_type, g_subtype, g_key)
            groups.append(g_id)
            if g_id not in self.group_stats:
                limit = (
                    self.rotate_size_rules.get(g_key)
                    if g_type == "size"
                    else self.rotate_count_rules.get(g_key)
                )
                if g_subtype == "name":
                    limit = (
                        self.rotate_size_rules.get("*")
                        if g_type == "size"
                        else self.rotate_count_rules.get("*")
                    )
                elif g_subtype == "dir":
                    limit = (
                        self.rotate_size_rules.get("*/")
                        if g_type == "size"
                        else self.rotate_count_rules.get("*/")
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
                    f"已无可处理文件，但规则 {g_key} ({g_type}) 仍未满足限制 (当前: {current_str}, 限制: {limit_str})，请检查配置。"
                )


def handle_rotate_mode(
    task, config, logger, history_mgr, source_root, dest_root, task_mode
):
    local_stats = MoverStats()
    max_retries = config.get("max_retries", 3)
    conflict_policy = task["conflict_policy"].lower()
    remove_empty_dirs = task["remove_empty_dirs"]

    print_task_header(task, task_mode, source_root, dest_root, 0, logger)

    if not os.path.exists(source_root):
        logger.error(f"源目录不存在: {source_root}")
        return

    if dest_root and not os.path.isdir(dest_root):
        logger.critical("!!! CRUCIAL: 目标目录不存在 !!!")
        return

    task_delete_rules = task.get("delete_rules", {})
    task_keep_rules = task.get("keep_rules", {})
    preferred_rule = task.get("preferred_rule", "keep")
    whitelist_rules = task.get("whitelist_rules", {})
    is_whitelist_mode = task_mode in ["whitelist_copy", "whitelist_move"]

    merged_config = {
        "delete_rules": task_delete_rules,
        "keep_rules": task_keep_rules,
        "preferred_rule": preferred_rule,
        "whitelist_rules": whitelist_rules,
        "is_whitelist_mode": is_whitelist_mode,
    }
    policy = FileFilterPolicy(merged_config)

    size_limit = parse_size_string(task.get("size_limit", "0"))
    count_limit = int(task.get("count_limit", 0))

    rotate_rules = task.get("rotate_rules", {})
    raw_rotate_size = rotate_rules.get("size", {})
    raw_rotate_count = rotate_rules.get("count", {})

    rotate_size_rules = {}
    for k, v in raw_rotate_size.items():
        rotate_size_rules[k] = parse_size_string(v)

    rotate_count_rules = {}
    for k, v in raw_rotate_count.items():
        rotate_count_rules[k] = int(v)

    rotate_mgr = RotateGroupManager(
        size_limit, count_limit, rotate_size_rules, rotate_count_rules
    )

    now = time.time()
    start_time = time.time()

    all_files = []
    for root, dirs, files in os.walk(source_root):
        process_directories(
            dirs,
            root,
            source_root,
            policy,
            now,
            0,
            local_stats,
            logger,
        )
        for file in files:
            src_path = os.path.join(root, file)
            try:
                file_stat = os.stat(src_path)
                size = file_stat.st_size
                mtime = file_stat.st_mtime
                rel_path = os.path.relpath(src_path, source_root)

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
        logger.info("当前未超过限制，无需轮转。")
        end_time = time.time()
        duration_str, total_size_str, speed_str = local_stats.calculate_speed(
            start_time, end_time
        )
        print_task_summary(local_stats, duration_str, total_size_str, speed_str, logger)
        return

    all_files.sort(key=lambda x: x["mtime"])

    for f in all_files:
        if not rotate_mgr.is_any_group_exceeded():
            break

        if not rotate_mgr.is_file_needs_rotation(f["groups"]):
            continue

        should_skip, fail_count = history_mgr.should_skip(f["path"], max_retries)
        if should_skip:
            local_stats.dropped += 1
            logger.warning(f"跳过文件 (多次失败): {f['path']}")
            continue

        if is_file_locked(f["path"]):
            local_stats.locked_skipped += 1
            logger.warning(f"跳过文件 (被锁定): {f['rel_path']}")
            continue

        action = policy.decide(f["rel_path"], f["size"])

        if action == FileAction.SKIP:
            local_stats.kept += 1
            logger.debug(
                f"保留文件 (匹配规则): {f['rel_path']} ({format_size(f['size'], binary=True)})"
            )
            continue

        if action == FileAction.DELETE:
            delete_file(
                f["path"], f["size"], source_root, logger, local_stats, history_mgr
            )
        elif action == FileAction.TRANSFER:
            if not dest_root:
                logger.warning(f"跳过文件 (目标目录非法): {f['rel_path']}")
                continue
            move_file(
                f["path"],
                f["size"],
                source_root,
                dest_root,
                logger,
                local_stats,
                history_mgr,
                conflict_policy,
                "move",
            )

        if not os.path.exists(f["path"]):
            rotate_mgr.remove_file(f["groups"], f["size"])

    rotate_mgr.log_unmet_limits(logger)

    if remove_empty_dirs:
        clean_empty_dirs(source_root, logger)

    end_time = time.time()
    duration_str, total_size_str, speed_str = local_stats.calculate_speed(
        start_time, end_time
    )
    print_task_summary(local_stats, duration_str, total_size_str, speed_str, logger)
