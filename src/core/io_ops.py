import os
import shutil


def copy_file(src_path, dest_path):
    """
    复制文件，不包含任何业务逻辑
    """
    dest_dir = os.path.dirname(dest_path)
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir, exist_ok=True)
    shutil.copy2(src_path, dest_path)


def move_file(src_path, dest_path):
    """
    移动文件，不包含任何业务逻辑
    """
    dest_dir = os.path.dirname(dest_path)
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir, exist_ok=True)
    shutil.move(src_path, dest_path)


def delete_file(src_path):
    """
    删除文件，不包含任何业务逻辑
    """
    os.remove(src_path)


def delete_dir(dir_path):
    """
    删除目录，不包含任何业务逻辑
    """
    shutil.rmtree(dir_path)
