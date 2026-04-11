import os
import shutil

"""
基础IO操作模块，不应包含任何业务逻辑
"""


def copy_file(src_path, dest_path):
    dest_dir = os.path.dirname(dest_path)
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir, exist_ok=True)
    shutil.copy2(src_path, dest_path)


def move_file(src_path, dest_path):
    dest_dir = os.path.dirname(dest_path)
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir, exist_ok=True)
    shutil.move(src_path, dest_path)


def delete_file(src_path):
    os.remove(src_path)


def delete_dir(dir_path):
    shutil.rmtree(dir_path)
