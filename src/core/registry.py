"""处理器注册中心：模式名称 → 处理器类的映射"""

from src.core.backend import create_dest_backend

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
    ctx = __import__("src.app_context", fromlist=["AppContext"]).AppContext.get()
    mode = task.get("mode", "").lower()
    handler_cls = _registry.get(mode)
    if handler_cls is None:
        ctx.logger.error(f"不支持的任务模式: {mode}，跳过该任务。")
        return

    # sync 模式只解析 ssh_remotes；其他模式只解析 http_remotes。
    # 两者路径完全隔离，因此允许别名重名。
    if mode == "sync":
        remote_clients = {}
        ssh_remotes = getattr(ctx, "ssh_remotes", {})
    else:
        remote_clients = getattr(ctx, "remote_clients", {})
        ssh_remotes = {}

    dest_root = task.get("dest", "")
    dest_backend = create_dest_backend(dest_root, remote_clients, ssh_remotes)

    handler = handler_cls(task, dest_backend, now)
    handler.execute()
