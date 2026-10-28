"""文件系统操作异常。"""


class FsOpError(OSError):
    """文件系统操作异常的基类。"""


class PathNotFoundError(FsOpError):
    """路径不存在。"""


class PermissionDeniedError(FsOpError):
    """权限不足。"""
