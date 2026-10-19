"""应用上下文：通过模块级单例解耦 Logger 和 HistoryManager 的传递"""

from src.shutdown import ShutdownCoordinator


class AppContext:
    _instance = None

    @classmethod
    def init(
        cls,
        logger,
        history_mgr,
        config,
        remote_clients=None,
        ssh_remotes=None,
        instance_name="",
    ):
        cls._instance = cls(logger, history_mgr, config, remote_clients, ssh_remotes, instance_name)

    @classmethod
    def get(cls):
        if cls._instance is None:
            raise RuntimeError("AppContext 未初始化，请先调用 AppContext.init()")
        return cls._instance

    def __init__(
        self,
        logger,
        history_mgr,
        config,
        remote_clients=None,
        ssh_remotes=None,
        instance_name="",
    ):
        self.logger = logger
        self.history_mgr = history_mgr
        self.config = config
        self.remote_clients = remote_clients or {}
        self.ssh_remotes = ssh_remotes or {}
        # 实例标识名：锁文件 / 控制文件 / PID 文件 / 日志前缀的公共基名
        self.instance_name = instance_name
        # 停止指令协调器：ControlWatcher 注入信号，业务循环在边界查询
        self.shutdown = ShutdownCoordinator()

    def update_config(self, config, remote_clients=None, ssh_remotes=None):
        self.config = config
        if remote_clients is not None:
            self.remote_clients = remote_clients
        if ssh_remotes is not None:
            self.ssh_remotes = ssh_remotes
