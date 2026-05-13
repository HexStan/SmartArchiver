"""处理器注册中心：模式名称 → 处理器类的映射"""

_registry = {}


def register_handler(*mode_names):
    def decorator(cls):
        for name in mode_names:
            _registry[name] = cls
        return cls

    return decorator


def get_handler_class(mode_name):
    return _registry.get(mode_name)


def process_task(task, now=None):
    mode = task.get("mode", "").lower()
    handler_cls = _registry.get(mode)
    if handler_cls is None:
        ctx = __import__("src.app_context", fromlist=["AppContext"]).AppContext.get()
        ctx.logger.error(f"不支持的任务模式: {mode}，跳过该任务。")
        return
    handler = handler_cls(task, now)
    handler.execute()
