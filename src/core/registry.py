"""处理器注册中心：模式名称 → 处理器类的映射"""

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from src.app.context import AppContext
from src.core.backend import create_dest_backend

if TYPE_CHECKING:
    from src.core.handlers.base import BaseTaskHandler

_registry: dict[str, type[BaseTaskHandler]] = {}


def register_handler[T: BaseTaskHandler](
    *mode_names: str,
) -> Callable[[type[T]], type[T]]:
    """注册处理器：把模式名称映射到被装饰的处理器类。

    BaseTaskHandler 经 TYPE_CHECKING 导入（PEP 695 边界惰性求值），
    避免 registry ↔ handlers 的导入环（handlers.* 需要导入本模块注册）。
    """

    def decorator(cls: type[T]) -> type[T]:
        for name in mode_names:
            _registry[name] = cls
        return cls

    return decorator


def get_handler_class(mode_name: str) -> type[BaseTaskHandler] | None:
    return _registry.get(mode_name)


def process_task(task: dict[str, Any], now: float | None = None) -> None:
    ctx = AppContext.get()
    mode = task.get("mode", "").lower()
    handler_cls = _registry.get(mode)
    if handler_cls is None:
        ctx.logger.error(f"不支持的任务模式: {mode}，跳过该任务。")
        return

    remote_clients = getattr(ctx, "remote_clients", {})
    ssh_remotes = getattr(ctx, "ssh_remotes", {})
    dest_root = task.get("dest", "")
    dest_backend = create_dest_backend(dest_root, remote_clients, ssh_remotes)

    handler = handler_cls(task, dest_backend, now)
    handler.execute()
