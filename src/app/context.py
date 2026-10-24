"""应用上下文：通过模块级单例解耦 Logger 和 HistoryManager 的传递"""

from typing import TYPE_CHECKING

from src.app.shutdown import ShutdownCoordinator

if TYPE_CHECKING:
    # 类型级依赖（运行时零导入）：core 包的 __init__ 会级联注册 handlers，
    # handlers.base 反向依赖本模块，运行时导入会造成循环
    from src.app.config import GlobalConfig
    from src.core.history import HistoryManager
    from src.core.task_plan import TaskSpec
    from src.output.logger import Logger
    from src.remote.client import RemoteClient
    from src.ssh.remotes import SshRemote


class AppContext:
    _instance: AppContext | None = None

    @classmethod
    def init(
        cls,
        logger: Logger,
        history_mgr: HistoryManager,
        config: GlobalConfig,
        tasks: list[TaskSpec] | None = None,
        remote_clients: dict[str, RemoteClient] | None = None,
        ssh_remotes: dict[str, SshRemote] | None = None,
        instance_name: str = "",
    ) -> None:
        cls._instance = cls(
            logger, history_mgr, config, tasks, remote_clients, ssh_remotes, instance_name
        )

    @classmethod
    def get(cls) -> AppContext:
        if cls._instance is None:
            raise RuntimeError("AppContext 未初始化，请先调用 AppContext.init()")
        return cls._instance

    def __init__(
        self,
        logger: Logger,
        history_mgr: HistoryManager,
        config: GlobalConfig,
        tasks: list[TaskSpec] | None = None,
        remote_clients: dict[str, RemoteClient] | None = None,
        ssh_remotes: dict[str, SshRemote] | None = None,
        instance_name: str = "",
    ) -> None:
        self.logger = logger
        self.history_mgr = history_mgr
        # 全局配置（加载期已校验的 GlobalConfig 模型）
        self.config = config
        # 加载期解析完成的任务编排信息（config=None 表示被拒绝的任务）
        self.tasks: list[TaskSpec] = tasks if tasks is not None else []
        self.remote_clients = remote_clients or {}
        self.ssh_remotes = ssh_remotes or {}
        # 实例标识名：锁文件 / 控制文件 / PID 文件 / 日志前缀的公共基名
        self.instance_name = instance_name
        # 停止指令协调器：ControlWatcher 注入信号，业务循环在边界查询
        self.shutdown = ShutdownCoordinator()

    def update_config(
        self,
        config: GlobalConfig,
        tasks: list[TaskSpec],
        remote_clients: dict[str, RemoteClient] | None = None,
        ssh_remotes: dict[str, SshRemote] | None = None,
    ) -> None:
        self.config = config
        self.tasks = tasks
        if remote_clients is not None:
            self.remote_clients = remote_clients
        if ssh_remotes is not None:
            self.ssh_remotes = ssh_remotes
