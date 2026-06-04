"""统一文件系统操作模块。

所有本地文件 I/O 原语集中于此，提供一致的日志、错误处理和返回类型。
供 LocalDestBackend、server.py 以及其他需要直接操作文件系统的模块使用。

原则：
- 不依赖 AppContext，不记录 history，不更新 stats
- 日志通过标准 logging 模块输出，调用方通过配置 handler 控制去向
- 每个函数只做一件事，不混合业务逻辑
"""

import logging
import os
import shutil

try:
    import fcntl
except ImportError:
    fcntl = None

logger = logging.getLogger(__name__)


# ============================================================
# 异常
# ============================================================


class FsOpError(OSError):
    """文件系统操作异常的基类。"""


class PathNotFoundError(FsOpError):
    """路径不存在。"""


class PermissionDeniedError(FsOpError):
    """权限不足。"""


# ============================================================
# 结构化返回类型
# ============================================================


class StatResult:
    """os.stat 的结构化返回。"""

    __slots__ = ("size", "mtime", "is_dir", "exists")

    def __init__(self, size=0, mtime=0, is_dir=False, exists=False):
        self.size = size
        self.mtime = mtime
        self.is_dir = is_dir
        self.exists = exists


class DirEntry:
    """目录条目信息。"""

    __slots__ = ("name", "is_dir", "size", "mtime")

    def __init__(self, name, is_dir=False, size=0, mtime=0):
        self.name = name
        self.is_dir = is_dir
        self.size = size
        self.mtime = mtime


# ============================================================
# 路径查询
# ============================================================


def path_exists(path):
    """检查路径是否存在。"""
    return os.path.exists(path)


def is_dir(path):
    """检查路径是否为目录。"""
    return os.path.isdir(path)


def get_stat(path):
    """获取路径的元数据。

    返回 StatResult，其中 exists 字段指示路径是否存在。
    """
    if not os.path.exists(path):
        return StatResult(exists=False)

    try:
        st = os.stat(path)
        return StatResult(
            size=st.st_size,
            mtime=st.st_mtime,
            is_dir=os.path.isdir(path),
            exists=True,
        )
    except PermissionError:
        raise PermissionDeniedError(f"权限不足，无法读取: {path}")
    except OSError as e:
        raise FsOpError(f"无法获取文件信息: {path} — {e}")


def list_dir(path):
    """列出目录内容，返回 DirEntry 列表。"""
    if not os.path.isdir(path):
        raise FsOpError(f"不是目录: {path}")

    entries = []
    try:
        with os.scandir(path) as it:
            for entry in it:
                try:
                    st = entry.stat()
                except OSError:
                    st = None

                entries.append(
                    DirEntry(
                        name=entry.name,
                        is_dir=entry.is_dir(),
                        size=st.st_size if st and entry.is_file() else 0,
                        mtime=st.st_mtime if st else 0,
                    )
                )
    except PermissionError:
        raise PermissionDeniedError(f"权限不足，无法列出目录: {path}")
    except OSError as e:
        raise FsOpError(f"列出目录失败: {path} — {e}")

    return entries


# ============================================================
# 目录操作
# ============================================================


def ensure_dir(path):
    """创建目录（含父目录），目录已存在时不报错。"""
    try:
        os.makedirs(path, exist_ok=True)
    except PermissionError:
        raise PermissionDeniedError(f"权限不足，无法创建目录: {path}")
    except OSError as e:
        raise FsOpError(f"创建目录失败: {path} — {e}")


def clean_empty_dirs(source_root):
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
            logger.debug(f"删除空目录: {root}")
        except OSError as e:
            logger.debug(f"跳过删除空目录 (出现错误): {root}\n{e}")


def get_dir_size_and_mtime(dir_path):
    """递归计算目录总大小和最新修改时间。

    返回 (total_size: int, latest_mtime: float)。
    """
    total_size = 0
    latest_mtime = 0

    try:
        latest_mtime = os.stat(dir_path).st_mtime

        for root, dirs, files in os.walk(dir_path):
            for f in files:
                fp = os.path.join(root, f)
                if os.path.islink(fp):
                    continue
                try:
                    st = os.stat(fp)
                    total_size += st.st_size
                    if st.st_mtime > latest_mtime:
                        latest_mtime = st.st_mtime
                except OSError:
                    pass
    except OSError:
        pass

    return total_size, latest_mtime


# ============================================================
# 文件操作
# ============================================================


def copy_file(src_path, dest_path):
    """复制文件，自动创建目标目录。

    使用 shutil.copy2 保留元数据。
    """
    dest_dir = os.path.dirname(dest_path)
    if not os.path.exists(dest_dir):
        ensure_dir(dest_dir)
    try:
        shutil.copy2(src_path, dest_path)
    except PermissionError:
        raise PermissionDeniedError(f"权限不足，无法复制: {src_path} -> {dest_path}")
    except OSError as e:
        raise FsOpError(f"复制文件失败: {src_path} -> {dest_path} — {e}")


def move_file(src_path, dest_path):
    """移动文件，自动创建目标目录。"""
    dest_dir = os.path.dirname(dest_path)
    if not os.path.exists(dest_dir):
        ensure_dir(dest_dir)
    try:
        shutil.move(src_path, dest_path)
    except PermissionError:
        raise PermissionDeniedError(f"权限不足，无法移动: {src_path} -> {dest_path}")
    except OSError as e:
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

    locked = False
    f = None
    try:
        f = open(filepath, "r")
        fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    except (IOError, OSError):
        locked = True
    finally:
        if f:
            try:
                f.close()
            except Exception:
                pass
    return locked
