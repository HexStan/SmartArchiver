"""统一文件系统操作模块。

所有本地文件 I/O 原语集中于此，提供一致的日志、错误处理和返回类型。
供 LocalDestBackend、server.py 以及其他需要直接操作文件系统的模块使用。

原则：
- 不依赖 AppContext，不记录 history，不更新 stats
- 日志通过标准 logging 模块输出，调用方通过配置 handler 控制去向
- 每个函数只做一件事，不混合业务逻辑
"""

import contextlib
import errno
import logging
import os
import shutil
import tempfile

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


class TransferAbortedError(Exception):
    """文件传输被停止指令中止（stop --fast）。

    继承 Exception 而非 FsOpError：中止不是文件系统操作的失败，
    不应被各处的 ``except OSError`` 吞掉改写成操作错误。
    """


# 传输中止检查钩子：由运行入口注入（ShutdownCoordinator.is_aborted），
# 复制循环在每个 chunk 边界查询，实现 fast 停止的即时响应。
_abort_check = None


def set_abort_check(check):
    """注入传输中止检查函数（无参可调用，返回 True 表示应立即中止）。"""
    global _abort_check
    _abort_check = check


def abort_requested():
    """当前是否收到 fast 停止指令。"""
    return _abort_check is not None and _abort_check()


class PathNotFoundError(FsOpError):
    """路径不存在。"""


class PermissionDeniedError(FsOpError):
    """权限不足。"""


# ============================================================
# 结构化返回类型
# ============================================================


class StatResult:
    """os.stat 的结构化返回。"""

    __slots__ = ("exists", "is_dir", "mtime", "size")

    def __init__(self, size=0, mtime=0, is_dir=False, exists=False):
        self.size = size
        self.mtime = mtime
        self.is_dir = is_dir
        self.exists = exists


class DirEntry:
    """目录条目信息。"""

    __slots__ = ("is_dir", "mtime", "name", "size")

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
            logger.info(f"删除空目录: {root}")
        except OSError as e:
            logger.info(f"跳过删除空目录 (出现错误): {root}\n{e}")


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
                    latest_mtime = max(latest_mtime, st.st_mtime)
                except OSError:
                    pass
    except OSError:
        pass

    return total_size, latest_mtime


# ============================================================
# 原子写入
# ============================================================

# 临时文件后缀。采用确定性命名（目标同目录 + 固定后缀），
# 是「再次写入同名文件时清理上次崩溃残留」的前提。
_PARTIAL_SUFFIX = ".smartarchiver-partial"
_COPY_CHUNK_SIZE = 1024 * 1024  # 1MB


def _partial_temp_path(dest_path):
    """生成与目标同目录的确定性临时文件路径。

    与目标同目录保证二者位于同一文件系统，最后的 os.replace 才是原子操作。
    """
    dest_dir = os.path.dirname(dest_path) or "."
    filename = os.path.basename(dest_path)
    return os.path.join(dest_dir, f".{filename}{_PARTIAL_SUFFIX}")


def _discard_partial(temp_path):
    """尽力删除临时文件（本次写入失败或上次崩溃的残留）。"""
    try:
        os.remove(temp_path)
    except FileNotFoundError:
        pass
    except OSError as e:
        logger.debug(f"清理临时文件失败: {temp_path} — {e}")


def _create_temp_file(dest_path):
    """清理同名残留后独占创建临时文件，返回临时文件路径。

    优先使用确定性名字，残留清理依赖该名字（单实例锁保证同名残留
    只能来自上次崩溃的写入）；名字被占用时回退随机名。
    """
    dest_dir = os.path.dirname(dest_path) or "."
    ensure_dir(dest_dir)

    temp_path = _partial_temp_path(dest_path)
    _discard_partial(temp_path)

    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
    try:
        fd = os.open(temp_path, flags, 0o644)
        os.close(fd)
        return temp_path
    except OSError as e:
        logger.debug(f"按确定性名字创建临时文件失败，回退随机名: {temp_path} — {e}")

    fd, fallback_path = tempfile.mkstemp(
        dir=dest_dir,
        prefix=f".{os.path.basename(dest_path)}.",
        suffix=_PARTIAL_SUFFIX,
    )
    os.close(fd)
    return fallback_path


@contextlib.contextmanager
def atomic_writer(dest_path, finalize=None):
    """以原子方式写入 dest_path。

    用法::

        with atomic_writer(dest, finalize=fn) as f:
            f.write(data)

    临时文件创建于目标同目录，正常退出时按
    「flush → finalize（可选）→ fsync → os.replace」收尾，
    任何时刻观察者看到的都是完整的旧文件或完整的新文件；
    异常退出时关闭并删除临时文件，最终路径不受影响。

    注意 finalize 必须在 flush 之后：数据落盘会刷新 mtime，
    copystat/utime 等元数据回写只有在其后执行才能生效。

    Args:
        dest_path: 最终目标路径。
        finalize: 可选回调 finalize(temp_path)，在 fsync 之前执行，
            用于写入元数据（copystat、utime、chmod 等）。
    """
    temp_path = _create_temp_file(dest_path)
    finished = False
    try:
        with open(temp_path, "wb") as f:
            yield f
            f.flush()
            if finalize is not None:
                finalize(temp_path)
            os.fsync(f.fileno())
        os.replace(temp_path, dest_path)
        finished = True
    finally:
        if not finished:
            _discard_partial(temp_path)


# ============================================================
# 文件操作
# ============================================================


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
