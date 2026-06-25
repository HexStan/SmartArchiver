"""应用上下文：通过模块级单例解耦 Logger 和 HistoryManager 的传递"""


class AppContext:
    _instance = None

    @classmethod
    def init(cls, logger, history_mgr, config, remote_clients=None, ssh_remotes=None):
        cls._instance = cls(logger, history_mgr, config, remote_clients, ssh_remotes)

    @classmethod
    def get(cls):
        if cls._instance is None:
            raise RuntimeError("AppContext 未初始化，请先调用 AppContext.init()")
        return cls._instance

    def __init__(self, logger, history_mgr, config, remote_clients=None, ssh_remotes=None):
        self.logger = logger
        self.history_mgr = history_mgr
        self.config = config
        self.remote_clients = remote_clients or {}
        self.ssh_remotes = ssh_remotes or {}

    def update_config(self, config, remote_clients=None, ssh_remotes=None):
        self.config = config
        if remote_clients is not None:
            self.remote_clients = remote_clients
        if ssh_remotes is not None:
            self.ssh_remotes = ssh_remotes
