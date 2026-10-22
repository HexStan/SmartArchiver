"""统一文件系统操作包。

本地文件 I/O 原语按职责拆分为六个模块：errors（异常）、abort（中止信号）、
scan（路径查询与目录枚举）、atomic（原子写入）、transfer（传输与删除）、
cleanup（空目录清理）。对外提供一致的日志、错误处理和返回类型，
供 core.dest 转发层、remote.server 以及其他需要直接操作本地文件系统的
模块使用。

**约定（DocStrings 契约）**：本包是本地路径专用原语——所有针对目标目录
（dest）的读写行为必须经由 src/core/dest.py 转发层；对带 {类型:别名}?
前缀的目标协议串调用本包函数会得到静默的错误结果。

原则：
- 不依赖 AppContext，不记录 history，不更新 stats
- 日志通过标准 logging 模块输出，调用方通过配置 handler 控制去向
- 每个函数只做一件事，不混合业务逻辑
"""

from src.fs.abort import TransferAbortedError, abort_requested, set_abort_check
from src.fs.atomic import atomic_writer
from src.fs.cleanup import clean_empty_dirs
from src.fs.errors import FsOpError, PathNotFoundError, PermissionDeniedError
from src.fs.scan import (
    DirEntry,
    StatResult,
    ensure_dir,
    get_dir_size_and_mtime,
    get_stat,
    is_dir,
    list_dir,
    path_exists,
)
from src.fs.transfer import (
    copy_file,
    delete_dir,
    delete_file,
    is_file_locked,
    move_file,
)

__all__ = [
    "DirEntry",
    "FsOpError",
    "PathNotFoundError",
    "PermissionDeniedError",
    "StatResult",
    "TransferAbortedError",
    "abort_requested",
    "atomic_writer",
    "clean_empty_dirs",
    "copy_file",
    "delete_dir",
    "delete_file",
    "ensure_dir",
    "get_dir_size_and_mtime",
    "get_stat",
    "is_dir",
    "is_file_locked",
    "list_dir",
    "move_file",
    "path_exists",
    "set_abort_check",
]
