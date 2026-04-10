import os
import shutil
import time
import subprocess
from datetime import datetime
from humanfriendly import format_timespan

def _setup_backup_dir(dest_root, max_backups, exclude_list, logger):
    backup_base = os.path.join(dest_root, ".smart-archiver.backups")
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_dir = os.path.join(backup_base, timestamp)

    # 确保备份目录本身被排除，防止无限递归或被删除
    exclude_list.append(".smart-archiver.backups/")

    # 清理旧备份
    if max_backups > 0 and os.path.exists(backup_base):
        try:
            backups = [
                os.path.join(backup_base, d)
                for d in os.listdir(backup_base)
                if os.path.isdir(os.path.join(backup_base, d))
            ]
            backups.sort()
            # 如果当前备份数已经达到或超过最大限制，删除最旧的，为新备份腾出空间
            if len(backups) >= max_backups:
                num_to_delete = len(backups) - max_backups + 1
                for b in backups[:num_to_delete]:
                    shutil.rmtree(b)
                    logger.debug(f"已删除旧备份: {b}")
        except Exception as e:
            logger.error(f"清理旧备份失败: {e}")

    return backup_dir


def _run_sync_command(cmd, logger, tool_name, prepend_timestamp=False):
    logger.info(f"正在使用 {tool_name} 进行同步……")
    try:
        start_time = time.time()
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        for line in process.stdout:
            if prepend_timestamp:
                logger.info(line.strip(), raw=True, prepend_timestamp=True)
            else:
                logger.info(line.strip())
        process.wait()
        if process.returncode != 0:
            logger.error(f"{tool_name} 同步失败，退出码: {process.returncode}")
        else:
            end_time = time.time()
            exec_time = end_time - start_time
            logger.success(
                f"{tool_name} 同步完成，耗时: {format_timespan(exec_time)}。"
            )
    except Exception as e:
        logger.error(f"执行 {tool_name} 时发生错误: {e}")


def _run_rclone_sync(source_root, dest_root, exclude_list, backup_dir, logger):
    if not shutil.which("rclone"):
        logger.error("未找到 rclone，无法执行 sync 模式，跳过该任务。")
        return

    cmd = ["rclone", "sync", source_root, dest_root]
    for ex in exclude_list:
        cmd.extend(["--exclude", ex])
    if backup_dir:
        cmd.extend(["--backup-dir", backup_dir])

    _run_sync_command(cmd, logger, "rclone")


def _run_rsync_sync(source_root, dest_root, exclude_list, backup_dir, logger):
    if not shutil.which("rsync"):
        logger.error("未找到 rsync，无法执行 sync 模式，跳过该任务。")
        return

    src = source_root if source_root.endswith("/") else source_root + "/"
    cmd = ["rsync", "-av", "--delete", src, dest_root]
    for ex in exclude_list:
        cmd.extend(["--exclude", ex])
    if backup_dir:
        cmd.extend(["--backup", f"--backup-dir={backup_dir}"])

    _run_sync_command(cmd, logger, "rsync", prepend_timestamp=True)


def handle_sync_mode(task, config, logger, source_root, dest_root):
    """
    处理 sync 模式：使用 rsync 或 rclone 镜像同步目录
    """
    task_name = task.get("name")
    if task_name:
        logger.info(f" - 名称：{task_name}")
    logger.info(" - 任务模式: 同步")
    logger.info(f" - 源路径: {source_root}")
    logger.info(f" - 目标路径: {dest_root}")

    if not os.path.exists(source_root):
        logger.error(f"源目录不存在: {source_root}")
        return

    if not os.path.isdir(dest_root):
        logger.critical("!!! CRUCIAL: 目标目录不存在 !!!")
        return

    is_windows = os.name == "nt"

    exclude_list = task.get("exclude", [])
    if isinstance(exclude_list, str):
        exclude_list = [exclude_list]

    backup_enabled = task.get("create_backups", False)
    max_backups = task.get("max_backups", 0)

    backup_dir = None
    if backup_enabled:
        backup_dir = _setup_backup_dir(dest_root, max_backups, exclude_list, logger)

    if is_windows:
        _run_rclone_sync(source_root, dest_root, exclude_list, backup_dir, logger)
    else:
        _run_rsync_sync(source_root, dest_root, exclude_list, backup_dir, logger)
