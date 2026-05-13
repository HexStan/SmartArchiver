from src.core.sync import handle_sync_mode
from src.core.rotate import handle_rotate_mode
from src.core.handlers import StandardModeHandler


def process_directory_pair(task, config, logger, history_mgr, now=None):
    """
    处理单个目录对：遍历、移动文件
    """
    task_mode = task.get("mode", "").lower()
    source_root = task.get("source")
    dest_root = task.get("dest")

    if task_mode == "sync":
        handle_sync_mode(task, config, logger, source_root, dest_root)
        return

    if task_mode == "rotate":
        handle_rotate_mode(
            task, config, logger, history_mgr, source_root, dest_root, task_mode
        )
        return

    # For move, copy, whitelist_move, whitelist_copy
    handler = StandardModeHandler(task, config, logger, history_mgr, now)
    handler.execute()
