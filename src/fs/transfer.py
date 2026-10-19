"""文件传输与删除：原子复制、移动、删除、路径去重与占用检测。"""

import errno
import logging
import os
import shutil

try:
    import fcntl
except ImportError:
    fcntl = None

from src.fs.abort import TransferAbortedError, abort_requested
from src.fs.atomic import atomic_writer
from src.fs.errors import FsOpError, PathNotFoundError, PermissionDeniedError
from src.fs.scan import ensure_dir

logger = logging.getLogger(__name__)

_COPY_CHUNK_SIZE = 1024 * 1024  # 1MB


def copy_file(src_path, dest_path):
    """原子复制文件，自动创建目标目录，保留元数据（含 mtime）。

    写入目标同目录的临时文件，fsync 后经 os.replace 原子改名。
    mtime 必须保留：归档阈值逻辑依赖它。
    """

    def _finalize(temp_path):
        shutil.copystat(src_path, temp_path)

    try:
        with open(src_path, "rb") as src, atomic_writer(dest_path, _finalize) as out:
            while True:
                # chunk 边界响应 fast 停止指令；中止时 atomic_writer
                # 的收尾逻辑会清理已写入的临时文件，最终路径不受影响
                if abort_requested():
                    raise TransferAbortedError(f"复制被中止: {src_path} -> {dest_path}")
                chunk = src.read(_COPY_CHUNK_SIZE)
                if not chunk:
                    break
                out.write(chunk)
    except PermissionError:
        raise PermissionDeniedError(f"权限不足，无法复制: {src_path} -> {dest_path}")
    except OSError as e:
        raise FsOpError(f"复制文件失败: {src_path} -> {dest_path} — {e}")


def move_file(src_path, dest_path):
    """原子移动文件，自动创建目标目录。

    优先 os.replace：同文件系统时为单次系统调用，瞬时且原子；
    跨文件系统（跨设备，errno.EXDEV）时回退为
    「原子复制到目标 + 复制成功后删除源」，源文件在复制期间始终完好。
    """
    dest_dir = os.path.dirname(dest_path) or "."
    ensure_dir(dest_dir)

    try:
        os.replace(src_path, dest_path)
        return
    except OSError as e:
        if e.errno != errno.EXDEV:
            if isinstance(e, PermissionError):
                raise PermissionDeniedError(f"权限不足，无法移动: {src_path} -> {dest_path}")
            raise FsOpError(f"移动文件失败: {src_path} -> {dest_path} — {e}")

    try:
        copy_file(src_path, dest_path)
        delete_file(src_path)
    except PermissionDeniedError:
        raise
    except FsOpError as e:
        raise FsOpError(f"移动文件失败: {src_path} -> {dest_path} — {e}")


def delete_file(path):
    """删除单个文件。

    如果路径是符号链接则跳过，防止误删链接目标。
    """
    if os.path.islink(path):
        logger.warning(f"跳过删除文件 (符号链接): {path}")
        return
    try:
        os.remove(path)
    except FileNotFoundError:
        raise PathNotFoundError(f"文件不存在: {path}")
    except PermissionError:
        raise PermissionDeniedError(f"权限不足，无法删除: {path}")
    except OSError as e:
        raise FsOpError(f"删除文件失败: {path} — {e}")


def delete_dir(path):
    """递归删除目录树。

    如果路径是符号链接则跳过，防止误删链接目标。
    """
    if os.path.islink(path):
        logger.warning(f"跳过删除目录 (符号链接): {path}")
        return
    if not os.path.isdir(path):
        raise FsOpError(f"不是目录: {path}")
    try:
        shutil.rmtree(path)
    except PermissionError:
        raise PermissionDeniedError(f"权限不足，无法删除目录: {path}")
    except OSError as e:
        raise FsOpError(f"删除目录失败: {path} — {e}")


def get_unique_path(dest_path):
    """如果目标路径已存在，生成带编号的新路径。

    例如: /path/file.txt -> /path/file-1.txt
    """
    if not os.path.exists(dest_path):
        return dest_path

    directory = os.path.dirname(dest_path)
    filename = os.path.basename(dest_path)
    name, ext = os.path.splitext(filename)

    counter = 1
    while True:
        new_filename = f"{name}-{counter}{ext}"
        new_path = os.path.join(directory, new_filename)
        if not os.path.exists(new_path):
            return new_path
        counter += 1


# ============================================================
# 文件锁
# ============================================================


def is_file_locked(filepath):
    """检查文件是否被 flock 锁定。

    仅在 Unix/Linux 系统有效，Windows 下始终返回 False。
    """
    if fcntl is None:
        return False

    try:
        with open(filepath, "r") as f:
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except OSError:
                return True
    except OSError:
        # 打开失败（文件不存在、无权限等）时保守地视为已锁定
        return True
    return False
