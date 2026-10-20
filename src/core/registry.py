"""处理器注册中心：模式名称 → 处理器类的映射"""

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from src.app.context import AppContext
from src.core.backend import create_dest_backend
from src.core.patterns import InvalidSizeError

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


def create_handler(
    task: dict[str, Any],
    now: float | None = None,
    remote_clients: dict[str, Any] | None = None,
    ssh_remotes: dict[str, Any] | None = None,
) -> BaseTaskHandler | None:
    """按 mode 从注册表取处理器并完成构造；无法构造时记错误日志并返回 None。

    规则的容量值在 handler.__init__ 里解析（FileFilterPolicy 构造），
    此处是构造阶段配置解析错误（InvalidSizeError / ValueError）的
    唯一捕获点，与"不支持的模式"同一模式：报错 + 跳过。测试入口
    （conftest.run_task）与本函数共用同一条装配路径，保证错误路径行为一致。
    """
    ctx = AppContext.get()
    mode = task.get("mode", "").lower()
    handler_cls = _registry.get(mode)
    if handler_cls is None:
        ctx.logger.error(f"不支持的任务模式: {mode}，跳过该任务。")
        return None

    remote_clients = (
        remote_clients if remote_clients is not None else getattr(ctx, "remote_clients", {})
    )
    ssh_remotes = ssh_remotes if ssh_remotes is not None else getattr(ctx, "ssh_remotes", {})
    dest_backend = create_dest_backend(task.get("dest", ""), remote_clients, ssh_remotes)

    try:
        return handler_cls(task, dest_backend, now)
    except InvalidSizeError as e:
        ctx.logger.error(f"任务的容量配置错误：{e}，跳过该任务。")
        return None
    except ValueError as e:
        # 数量等非容量配置值的强转失败——消息已由解析方（如 rotate 的
        # _parse_count）本地化为中文；构造链上其他意外 ValueError 也在此
        # 收口为"跳过任务"，而不是冒泡成"发生未知错误"。
        ctx.logger.error(f"{e}，跳过该任务。")
        return None


def process_task(task: dict[str, Any], now: float | None = None) -> None:
    handler = create_handler(task, now=now)
    if handler is not None:
        handler.execute()
