import os
from src.core.io_ops import delete_file as io_delete_file
from humanfriendly import format_size, format_timespan
from src.utils import parse_size_string, get_unique_dest


def validate_task_config(task, task_mode, logger):
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
            logger.error(
                "rotate 模式下必须配置 size_limit、count_limit、rotate_rules.size 或 rotate_rules.count 中的至少一项，跳过该任务。"
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
        logger.error(f"任务配置缺少必填项: {'、'.join(missing_fields)}，跳过该任务。")
        return False

    if task_mode in ["whitelist_copy", "whitelist_move"]:
        whitelist_rules = task.get("whitelist_rules", {})
        if not whitelist_rules:
            logger.error("白名单模式下必须配置 whitelist_rules，跳过该任务。")
            return False

    return True


def print_task_header(
    task, task_mode, source_root, dest_root, mtime_threshold_seconds, logger
):
    task_name = task.get("name")
    if task_name:
        logger.info(f" - 名称：{task_name}")

    mode_str = "移动"
    if task_mode in ["copy", "whitelist_copy"]:
        mode_str = "复制"
    elif task_mode == "whitelist_move":
        mode_str = "移动 (白名单)"
    elif task_mode == "whitelist_copy":
        mode_str = "复制 (白名单)"
    elif task_mode == "rotate":
        mode_str = "轮转"
    elif task_mode == "sync":
        mode_str = "同步"

    logger.info(f" - 任务模式: {mode_str}")
    logger.info(f" - 源路径: {source_root}")
    if not dest_root and task_mode == "rotate":
        logger.info(" - 目标路径: 无")
    else:
        logger.info(f" - 目标路径: {dest_root}")
    if task_mode not in ["rotate", "sync"]:
        logger.info(f" - 时间阈值: {format_timespan(mtime_threshold_seconds)}")
    if task_mode == "rotate":
        size_limit = task.get("size_limit")
        count_limit = task.get("count_limit")
        if size_limit and parse_size_string(size_limit) > 0:
            logger.info(f" - 大小限制: {size_limit}")
        if count_limit and int(count_limit) > 0:
            logger.info(f" - 数量限制: {count_limit}")


def print_task_summary(local_stats, duration_str, total_size_str, speed_str, logger):
    tail_msg = [f"成功 {local_stats.success} 项"]
    if local_stats.conflict_skipped > 0:
        tail_msg.append(f"因重复而跳过 {local_stats.conflict_skipped} 项")
    if local_stats.locked_skipped > 0:
        tail_msg.append(f"因文件锁而跳过 {local_stats.locked_skipped} 项")
    if local_stats.kept > 0:
        tail_msg.append(f"根据规则保留 {local_stats.kept} 项")
    if local_stats.deleted > 0:
        tail_msg.append(f"根据规则删除 {local_stats.deleted} 项")
    logger.info("，".join(tail_msg) + "。")

    tail_msg_2 = []
    if local_stats.error > 0:
        tail_msg_2.append(f"失败 {local_stats.error} 项")
    if local_stats.dropped > 0:
        tail_msg_2.append(f"因多次失败而跳过 {local_stats.dropped} 项")
    if tail_msg_2:
        logger.info("，".join(tail_msg_2) + "。")

    if local_stats.success > 0:
        logger.info(
            f"在 {duration_str} 内传输了 {total_size_str}，平均速度 {speed_str}。"
        )


def delete_file(src_path, file_size, source_root, logger, stats, history_mgr):
    rel_path = os.path.relpath(src_path, source_root)
    """删除逻辑"""
    try:
        io_delete_file(src_path)
        logger.success(f"删除文件: {rel_path}  ({format_size(file_size, binary=True)})")
        history_mgr.record_success(src_path)
        stats.deleted += 1
    except OSError as e:
        count = history_mgr.record_failure(src_path)
        logger.error(f"删除文件失败 ({count} 次): {rel_path}\nError: {e}")


def transfer_file(
    src_path,
    file_size,
    source_root,
    dest_root,
    logger,
    stats,
    history_mgr,
    conflict_policy,
    transfer_func,
    action_name,
):
    """
    执行具体的移动或复制操作
    """
    # 计算相对路径
    rel_path = os.path.relpath(src_path, source_root)
    dest_path = os.path.join(dest_root, rel_path)

    try:
        file_exists = os.path.exists(dest_path)
        new_dest_path = dest_path

        if file_exists:
            if conflict_policy == "skip":
                stats.conflict_skipped += 1
                logger.debug(f"跳过 (重复): {rel_path}")
                # 不记录到 success，也不报错
                return

            elif conflict_policy == "copy":
                # 计算新的不冲突的路径
                new_dest_path = get_unique_dest(dest_path)

            elif conflict_policy == "overwrite":
                # 默认逻辑：删除目标
                io_delete_file(dest_path)

            else:
                # 未知策略默认跳过
                return

        # 使用传入的传输函数
        transfer_func(src_path, new_dest_path)

        # 统计传输流量
        stats.total_bytes += file_size

        # 成功后清理记录
        history_mgr.record_success(src_path)

        size_str = format_size(file_size, binary=True)
        if file_exists and conflict_policy == "overwrite":
            logger.success(f"覆盖同名文件: {rel_path} ({size_str})")
        elif file_exists and conflict_policy == "copy":
            new_rel = os.path.relpath(new_dest_path, dest_root)
            logger.success(f"目标存在，创建副本: {new_rel} ({size_str})")
        else:
            logger.success(f"{action_name}文件: {rel_path} ({size_str})")

        stats.success += 1

    except Exception as e:
        # 失败时记录
        count = history_mgr.record_failure(src_path)
        logger.error(f"{action_name}文件失败 ({count} 次): {rel_path}\n{e}")
        stats.error += 1
