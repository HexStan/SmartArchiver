"""路径查询与目录枚举：只读探测、元数据获取、目录创建与体量统计。"""

import os
import stat
from dataclasses import dataclass

from src.fs.errors import FsOpError, PermissionDeniedError

# ============================================================
# 结构化返回类型
# ============================================================


@dataclass(slots=True, frozen=True)
class StatResult:
    """os.stat 的结构化返回。"""

    size: int = 0
    mtime: float = 0
    is_dir: bool = False
    exists: bool = False


@dataclass(slots=True, frozen=True)
class DirEntry:
    """目录条目信息。"""

    name: str
    is_dir: bool = False
    size: int = 0
    mtime: float = 0


# ============================================================
# 路径查询
# ============================================================


def path_exists(path: str) -> bool:
    """检查路径是否存在。"""
    return os.path.exists(path)


def is_dir(path: str) -> bool:
    """检查路径是否为目录。"""
    return os.path.isdir(path)


def get_stat(path: str) -> StatResult:
    """获取路径的元数据。

    返回 StatResult，其中 exists 字段指示路径是否存在；
    除"路径不存在"外的错误（如权限不足）按对应 FsOpError 抛出。
    """
    try:
        st = os.stat(path)
    except FileNotFoundError:
        return StatResult(exists=False)
    except ValueError:
        # 路径含非法字符（如 NUL）等不可能存在的形式，与 os.path.exists 语义对齐，视同不存在
        return StatResult(exists=False)
    except PermissionError:
        raise PermissionDeniedError(f"权限不足，无法读取: {path}")
    except OSError as e:
        raise FsOpError(f"无法获取文件信息: {path} — {e}")

    return StatResult(
        size=st.st_size,
        mtime=st.st_mtime,
        is_dir=stat.S_ISDIR(st.st_mode),
        exists=True,
    )


def list_dir(path: str) -> list[DirEntry]:
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


def ensure_dir(path: str) -> None:
    """创建目录（含父目录），目录已存在时不报错。"""
    try:
        os.makedirs(path, exist_ok=True)
    except PermissionError:
        raise PermissionDeniedError(f"权限不足，无法创建目录: {path}")
    except OSError as e:
        raise FsOpError(f"创建目录失败: {path} — {e}")


def get_dir_size_and_mtime(dir_path: str) -> tuple[int, float]:
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
