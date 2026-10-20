"""目录清理：自底向上删除空目录。"""

import logging
import os

logger = logging.getLogger(__name__)


def clean_empty_dirs(source_root: str) -> None:
    """自底向上清理 source_root 下的空目录。

    跳过符号链接、挂载点，以及 source_root 本身。
    """
    if not os.path.exists(source_root):
        return

    for root, dirs, files in os.walk(source_root, topdown=False):
        if root == source_root:
            continue

        if os.path.islink(root) or os.path.ismount(root):
            logger.debug(f"跳过删除空目录 (符号链接或挂载点): {root}")
            continue

        try:
            with os.scandir(root) as it:
                if any(it):
                    continue
            os.rmdir(root)
            logger.info(f"删除空目录: {root}")
        except OSError as e:
            logger.info(f"跳过删除空目录 (出现错误): {root}\n{e}")
