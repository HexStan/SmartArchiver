import os
import shutil
import time
from humanfriendly import format_size

from src.utils import is_file_locked, get_dir_size_and_mtime, clean_empty_dirs
from src.core.types import FileAction, MoverStats
from src.core.filters import FileFilterPolicy
from src.core.sync import handle_sync_mode
from src.core.rotate import handle_rotate_mode
from src.core.actions import (
    _validate_task_config,
    _print_task_header,
    _print_task_summary,
    delete_file,
    move_file,
)


def _process_directories(
    dirs, root, source_root, policy, now, mtime_threshold_seconds, local_stats, logger
):
    dirs_to_remove = []
    for d in dirs:
        dir_path = os.path.join(root, d)
        rel_dir_path = os.path.relpath(dir_path, source_root)

        dir_size_cache = []
        dir_mtime_cache = []

        # 惰性获取目录大小和修改时间，仅在规则匹配需要时调用
        def get_size():
            if not dir_size_cache:
                s, m = get_dir_size_and_mtime(dir_path)
                dir_size_cache.append(s)
                dir_mtime_cache.append(m)
            return dir_size_cache[0]

        action = policy.decide(rel_dir_path, get_size, is_dir=True)

        if action == FileAction.DELETE:
            # 如果之前没获取过大小（例如命中 ALL 规则），则在此处获取以进行时间检查
            if not dir_mtime_cache:
                s, m = get_dir_size_and_mtime(dir_path)
                dir_size_cache.append(s)
                dir_mtime_cache.append(m)
            dir_mtime = dir_mtime_cache[0]
            dir_size = dir_size_cache[0]

            if (now - dir_mtime) > mtime_threshold_seconds:
                try:
                    shutil.rmtree(dir_path)
                    logger.success(
                        f"删除目录: {rel_dir_path} ({format_size(dir_size, binary=True)})"
                    )
                    local_stats.deleted += 1
                except OSError as e:
                    logger.error(f"删除目录失败: {rel_dir_path}\nError: {e}")
            dirs_to_remove.append(d)
        elif action == FileAction.SKIP:
            size_str = (
                f" ({format_size(dir_size_cache[0], binary=True)})"
                if dir_size_cache
                else ""
            )
            logger.debug(f"保留目录 (匹配规则): {rel_dir_path}{size_str}")
            local_stats.kept += 1
            dirs_to_remove.append(d)

    for d in dirs_to_remove:
        dirs.remove(d)


def _process_files(
    files,
    root,
    source_root,
    dest_root,
    policy,
    now,
    mtime_threshold_seconds,
    local_stats,
    history_mgr,
    max_retries,
    conflict_policy,
    task_mode,
    logger,
):
    for file in files:
        src_path = os.path.join(root, file)
        rel_path = os.path.relpath(src_path, source_root)

        # 检查是否需要跳过
        should_skip, fail_count = history_mgr.should_skip(src_path, max_retries)
        if should_skip:
            # 简单处理，视为跳过
            local_stats.dropped += 1
            logger.warning(f"跳过文件 (多次失败): {src_path}")
            continue

        try:
            file_stat = os.stat(src_path)
            mtime = file_stat.st_mtime
            size = file_stat.st_size
        except OSError:
            continue

        # 所有操作（包括删除垃圾文件）都必须满足时间阈值
        if (now - mtime) <= mtime_threshold_seconds:
            continue

        if is_file_locked(src_path):
            local_stats.locked_skipped += 1
            logger.warning(f"跳过文件 (被锁定): {rel_path}")
            continue

        # 核心决策逻辑
        action = policy.decide(rel_path, size)

        # 执行动作
        if action == FileAction.TRANSFER:
            move_file(
                src_path,
                size,
                source_root,
                dest_root,
                logger,
                local_stats,
                history_mgr,
                conflict_policy,
                task_mode,
            )
        elif action == FileAction.DELETE:
            delete_file(src_path, size, source_root, logger, local_stats, history_mgr)
        elif action == FileAction.SKIP:
            local_stats.kept += 1
            logger.debug(
                f"保留文件 (匹配规则): {rel_path}  ({format_size(size, binary=True)})"
            )


def process_directory_pair(task, config, logger, history_mgr, now=None):
    """
    处理单个目录对：遍历、移动文件
    """
    local_stats = MoverStats()

    source_root = task.get("source")
    dest_root = task.get("dest")
    task_mode = task.get("mode", "").lower()

    if not _validate_task_config(task, task_mode, logger):
        return

    if task_mode == "sync":
        handle_sync_mode(task, config, logger, source_root, dest_root)
        return

    if task_mode == "rotate":
        handle_rotate_mode(
            task, config, logger, history_mgr, source_root, dest_root, task_mode
        )
        return

    mtime_threshold_minutes = task.get("mtime_threshold_minutes", 0)
    max_retries = config.get("max_retries", 3)
    conflict_policy = task["conflict_policy"].lower()
    remove_empty_dirs = task["remove_empty_dirs"]

    if now is None:
        now = time.time()
    mtime_threshold_seconds = mtime_threshold_minutes * 60

    _print_task_header(
        task, task_mode, source_root, dest_root, mtime_threshold_seconds, logger
    )

    if not os.path.exists(source_root):
        logger.error(f"源目录不存在: {source_root}")
        return

    if not os.path.isdir(dest_root):
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

    # 使用合并后的配置初始化策略
    policy = FileFilterPolicy(merged_config)

    # 记录开始时间
    start_time = time.time()

    # 遍历文件
    for root, dirs, files in os.walk(source_root):
        _process_directories(
            dirs,
            root,
            source_root,
            policy,
            now,
            mtime_threshold_seconds,
            local_stats,
            logger,
        )
        _process_files(
            files,
            root,
            source_root,
            dest_root,
            policy,
            now,
            mtime_threshold_seconds,
            local_stats,
            history_mgr,
            max_retries,
            conflict_policy,
            task_mode,
            logger,
        )

    # 清理空目录 (对应 find -depth -empty -type d rmdir)
    if remove_empty_dirs and task_mode not in ["copy", "whitelist_copy"]:
        clean_empty_dirs(source_root, logger)

    # 记录结束时间
    end_time = time.time()

    # 计算传输速率
    duration_str, total_size_str, speed_str = local_stats.calculate_speed(
        start_time, end_time
    )

    _print_task_summary(local_stats, duration_str, total_size_str, speed_str, logger)
