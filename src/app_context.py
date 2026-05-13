"""应用上下文：通过模块级单例解耦 Logger 和 HistoryManager 的传递"""


class AppContext:
    _instance = None

    @classmethod
    def init(cls, logger, history_mgr, config):
        cls._instance = cls(logger, history_mgr, config)

    @classmethod
    def get(cls):
        if cls._instance is None:
            raise RuntimeError("AppContext 未初始化，请先调用 AppContext.init()")
        return cls._instance

    def __init__(self, logger, history_mgr, config):
        self.logger = logger
        self.history_mgr = history_mgr
        self.config = config
