"""传输中止信号：进程级注入缝，供 fast 停止即时响应。

由组合根（app.runtime 装配阶段）注入 ``LifecycleCoordinator.is_aborted``；
``fs.transfer`` 与 ``remote`` 的复制循环在每个 chunk 边界查询。
"""

from collections.abc import Callable


class TransferAbortedError(Exception):
    """文件传输被停止指令中止（stop --fast）。

    继承 Exception 而非 FsOpError：中止不是文件系统操作的失败，
    不应被各处的 ``except OSError`` 吞掉改写成操作错误。
    """


# 传输中止检查钩子：由运行入口注入（LifecycleCoordinator.is_aborted），
# 复制循环在每个 chunk 边界查询，实现 fast 停止的即时响应。
AbortCheck = Callable[[], bool]
_abort_check: AbortCheck | None = None


def set_abort_check(check: AbortCheck) -> None:
    """注入传输中止检查函数（无参可调用，返回 True 表示应立即中止）。"""
    global _abort_check
    _abort_check = check


def abort_requested() -> bool:
    """当前是否收到 fast 停止指令。"""
    return _abort_check is not None and _abort_check()
