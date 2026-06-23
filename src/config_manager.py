"""统一配置管理器：加载、验证、重载、备份。"""

import os
import shutil
import tomllib
from dataclasses import dataclass, field

from src.remote import parse_remote_config
from src.ssh import parse_ssh_remote_config


class ConfigError(Exception):
    """配置致命错误。"""

    pass


@dataclass
class ConfigData:
    """从配置文件加载的完整数据集。"""

    config: dict
    remote_clients: dict = field(default_factory=dict)
    ssh_remotes: dict = field(default_factory=dict)
    used_backup: bool = False


class ConfigManager:
    """统一的配置生命周期管理器。

    启动时使用 ``load()``，运行时使用 ``reload()``（返回 None 表示无变化）。
    两条路径共享相同的加载、验证和错误处理逻辑。
    """

    def __init__(self, config_path, logger):
        self._config_path = os.path.abspath(config_path)
        self._backup_path = os.path.splitext(self._config_path)[0] + ".backup.toml"
        self._logger = logger
        self._last_mtime = self._get_mtime()

    # -- public API ----------------------------------------------------------

    def load(self, config=None):
        """初始加载配置。

        若调用方已预先读取过配置文件，可传入 *config* 字典以避免重复读取。
        """
        if config is None:
            config = self._load_toml(self._config_path)
        return self._process(config, save_backup=True)

    def reload(self):
        """运行时重载配置。

        仅在文件 mtime 发生变化时执行；否则返回 ``None``。
        加载或验证失败时自动回退到备份配置。
        """
        if not self.has_changed():
            return None

        try:
            config = self._load_toml(self._config_path)
            return self._process(config, save_backup=True)
        except ConfigError as e:
            self._logger.warning(f"新配置文件验证失败: {e}，尝试加载备份配置……")
            self._last_mtime = self._get_mtime()
            return self._load_backup()

    def has_changed(self):
        """配置文件 mtime 是否自上次加载后发生了变化。"""
        return self._get_mtime() != self._last_mtime

    # -- static helpers ------------------------------------------------------

    @staticmethod
    def _load_toml(path):
        """从路径读取 TOML 文件，返回字典。"""
        with open(path, "rb") as f:
            return tomllib.load(f)

    @staticmethod
    def _validate_structure(config):
        """致命的结构验证——失败意味着配置文件不可用。"""
        if not isinstance(config, dict):
            raise ConfigError("配置文件内容不是有效的字典结构")

    # -- internal ------------------------------------------------------------

    def _process(self, config, save_backup=False):
        """加载流程核心：验证 → 解析远端 → 保存备份 → 更新 mtime。"""
        self._validate_structure(config)
        self._warn_task_structure(config)

        remote_clients = parse_remote_config(config)
        ssh_remotes = parse_ssh_remote_config(config)

        if save_backup:
            self._save_backup()

        self._last_mtime = self._get_mtime()
        self._logger.info("配置文件加载成功。")

        return ConfigData(
            config=config,
            remote_clients=remote_clients,
            ssh_remotes=ssh_remotes,
            used_backup=False,
        )

    def _load_backup(self):
        """加载备份配置（致命失败会向上抛出）。"""
        try:
            config = self._load_toml(self._backup_path)
        except Exception as e:
            self._logger.error(f"备份配置文件加载失败: {e}")
            raise
        result = self._process(config, save_backup=False)
        result.used_backup = True
        return result

    def _warn_task_structure(self, config):
        """逐任务检查基础字段——仅警告，不阻止加载。"""
        tasks = config.get("tasks", [])
        for i, task in enumerate(tasks):
            if not isinstance(task, dict):
                self._logger.warning(f"任务 #{i + 1} 不是有效的字典结构，将被跳过")
                continue
            if "mode" not in task:
                self._logger.warning(f"任务 #{i + 1} 缺少 mode 字段，将被跳过")
            if "source" not in task:
                self._logger.warning(f"任务 #{i + 1} 缺少 source 字段，将被跳过")

    def _save_backup(self):
        """将当前配置文件复制为备份。"""
        try:
            shutil.copy2(self._config_path, self._backup_path)
        except OSError as e:
            self._logger.warning(f"保存配置备份失败: {e}")

    def _get_mtime(self):
        try:
            return os.path.getmtime(self._config_path)
        except OSError:
            return -1
