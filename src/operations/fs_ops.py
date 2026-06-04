"""
纯文件系统操作模块。

所有函数仅执行底层 IO 操作，不记录日志、不操作历史、不更新统计。
日志/统计等横切关注点由调用方负责。
"""

import os
import shutil
import fnmatch

try:
    import fcntl
except ImportError:
    fcntl = None


# ============================================================
# 路径 / 存在性
# ============================================================


def file_exists(path):
    """检查路径是否存在（文件或目录）。"""
    return os.path.exists(path)


def is_directory(path):
    """检查路径是否为目录。"""
    return os.path.isdir(path)


def is_file(path):
    """检查路径是否为普通文件。"""
    return os.path.isfile(path)


# ============================================================
# 目录操作
# ============================================================


def create_directory(path):
    """递归创建目录（等效于 mkdir -p），目录已存在时不报错。"""
    os.makedirs(path, exist_ok=True)


# ============================================================
# 删除
# ============================================================


def delete_path(path):
    """
    删除文件或目录（目录递归删除）。

    对不存在的路径静默忽略（幂等）。
    """
    if os.path.isdir(path):
        shutil.rmtree(path)
    elif os.path.isfile(path):
        os.remove(path)


# ============================================================
# 状态查询
# ============================================================


def get_stat(path):
    """
    获取路径的元信息。

    返回 dict: {exists, size, mtime, is_dir}
    不存在的路径返回 exists=False 且其他字段为 0。
    """
    if not os.path.exists(path):
        return {"exists": False, "size": 0, "mtime": 0, "is_dir": False}

    st = os.stat(path)
    return {
        "exists": True,
        "size": st.st_size,
        "mtime": st.st_mtime,
        "is_dir": os.path.isdir(path),
    }


def list_directory(path):
    """
    列出目录内容。

    返回 list[dict]: 每个条目包含 {name, is_dir, size, mtime}
    非目录或路径不存在时抛出 OSError。
    """
    entries = []
    with os.scandir(path) as it:
        for entry in it:
            st = entry.stat()
            entries.append(
                {
                    "name": entry.name,
                    "is_dir": entry.is_dir(),
                    "size": st.st_size if entry.is_file() else 0,
                    "mtime": st.st_mtime,
                }
            )
    return entries


# ============================================================
# 文件传输
# ============================================================


def copy_file(src_path, dest_path):
    """复制文件到目标路径，自动创建目标目录。"""
    dest_dir = os.path.dirname(dest_path)
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir, exist_ok=True)
    shutil.copy2(src_path, dest_path)


def move_file(src_path, dest_path):
    """移动文件到目标路径，自动创建目标目录。"""
    dest_dir = os.path.dirname(dest_path)
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir, exist_ok=True)
    shutil.move(src_path, dest_path)


def write_stream(path, stream, chunk_size=65536):
    """
    将可读流的内容写入文件，自动创建目标目录。

    stream: 任意实现了 read(size) 方法的对象。
    chunk_size: 每次读取的字节数，默认 64KB。
    """
    dest_dir = os.path.dirname(path)
    if dest_dir:
        os.makedirs(dest_dir, exist_ok=True)

    with open(path, "wb") as f:
        while True:
            chunk = stream.read(chunk_size)
            if not chunk:
                break
            f.write(chunk)


# ============================================================
# 唯一路径
# ============================================================


def get_unique_dest(dest_path, exists_fn=None):
    """
    如果目标路径存在，生成带编号的新路径。

    exists_fn: 可选的路径存在性检查函数，默认使用 os.path.exists。
               传入自定义函数可支持远程/虚拟文件系统。

    例如: /path/file.txt -> /path/file-1.txt
    """
    if exists_fn is None:
        exists_fn = os.path.exists

    if not exists_fn(dest_path):
        return dest_path

    directory = os.path.dirname(dest_path)
    filename = os.path.basename(dest_path)
    name, ext = os.path.splitext(filename)

    counter = 1
    while True:
        new_filename = f"{name}-{counter}{ext}"
        new_path = os.path.join(directory, new_filename)
        if not exists_fn(new_path):
            return new_path
        counter += 1


# ============================================================
# 目录遍历与统计
# ============================================================


def get_dir_size_and_mtime(dir_path):
    """
    计算目录的总大小和最新修改时间。

    返回 (total_size_bytes, latest_mtime)。

    如果目录不存在或无法访问，返回 (0, 0)。
    """
    total_size = 0
    latest_mtime = 0

    try:
        latest_mtime = os.stat(dir_path).st_mtime

        for root, dirs, files in os.walk(dir_path):
            for f in files:
                fp = os.path.join(root, f)
                if not os.path.islink(fp):
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


def clean_empty_dirs(source_root, logger=None):
    """
    递归删除空目录。

    logger: 可选的日志对象（需实现 debug 方法），用于输出调试信息。
    """
    if not os.path.exists(source_root):
        return

    for root, dirs, files in os.walk(source_root, topdown=False):
        if root == source_root:
            continue

        if os.path.islink(root) or os.path.ismount(root):
            if logger:
                rel_dir = os.path.relpath(root, source_root)
                logger.debug(f"跳过删除空目录 (符号链接或挂载点): {rel_dir}")
            continue

        try:
            with os.scandir(root) as it:
                if any(it):
                    continue

            os.rmdir(root)
            if logger:
                rel_dir = os.path.relpath(root, source_root)
                logger.debug(f"删除空目录: {rel_dir}")
        except OSError as e:
            if logger:
                rel_dir = os.path.relpath(root, source_root)
                logger.debug(f"跳过删除空目录 (出现错误): {rel_dir}\n{e}")


# ============================================================
# 文件锁
# ============================================================


def is_file_locked(filepath):
    """
    检查文件是否被 flock 锁定。

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
