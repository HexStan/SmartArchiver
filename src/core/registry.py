"""处理器注册中心：模式名称 → 处理器类的映射"""

from collections.abc import Callable
from typing import TYPE_CHECKING

from src.app.context import AppContext
from src.core.types import TaskOutcome

if TYPE_CHECKING:
    from src.core.handlers.base import BaseTaskHandler
    from src.core.task_plan import TaskSpec

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
    spec: TaskSpec,
    now: float | None = None,
) -> BaseTaskHandler | None:
    """按 mode 从注册表取处理器并完成构造；无法构造时记错误日志并返回 None。

    配置合法性已在加载期收口（task_plan.resolve_tasks：模型校验、dest
    装配校验、依赖静态校验），此处只做 mode → 处理器的分派与构造。
    task_config.TASK_MODELS 与本注册表由同一组模式名驱动，二者不一致
    属编程错误，防御分支兜底记错误日志跳过。
    """
    config = spec.config
    if config is None:
        # 加载期已拒绝：错误日志已由 resolve_tasks 给出，此处静默跳过
        return None

    ctx = AppContext.get()
    handler_cls = _registry.get(config.mode)
    if handler_cls is None:
        ctx.logger.error(f"不支持的任务模式: {config.mode}，跳过该任务。")
        return None

    return handler_cls(spec, now)


def process_task(spec: TaskSpec, now: float | None = None) -> TaskOutcome:
    """执行单个任务并返回定性结果，供任务编排（依赖判定）消费。

    构造失败记 SKIPPED（任务未执行）；执行结果由 handler.execute()
    给出（COMPLETED / FAILED / SKIPPED，语义见 TaskOutcome）。
    """
    handler = create_handler(spec, now=now)
    if handler is None:
        return TaskOutcome.SKIPPED
    return handler.execute()
