import os
import shutil
import tomllib


class ConfigReloader:
    def __init__(self, config_path):
        self.config_path = os.path.abspath(config_path)
        self.backup_path = os.path.splitext(self.config_path)[0] + ".backup.toml"
        self._last_mtime = self._get_mtime()

    def _get_mtime(self):
        try:
            return os.path.getmtime(self.config_path)
        except OSError:
            return -1

    def has_changed(self):
        return self._get_mtime() != self._last_mtime

    def reload(self, logger):
        try:
            new_config = self._load_toml(self.config_path)
        except Exception as e:
            logger.warning(f"新配置文件加载失败: {e}，尝试加载备份配置……")
            self._last_mtime = self._get_mtime()
            return self._load_backup(logger), True

        if not self._basic_validate(new_config):
            logger.warning(
                "新配置文件基本验证失败，尝试加载备份配置……"
            )
            self._last_mtime = self._get_mtime()
            return self._load_backup(logger), True

        try:
            shutil.copy2(self.config_path, self.backup_path)
        except OSError as e:
            logger.warning(f"保存配置备份失败: {e}")

        self._last_mtime = self._get_mtime()
        logger.info("配置文件已自动重载。")
        return new_config, False

    def save_backup(self):
        try:
            shutil.copy2(self.config_path, self.backup_path)
        except OSError:
            pass

    def _load_backup(self, logger):
        try:
            return self._load_toml(self.backup_path)
        except Exception as e:
            logger.error(f"备份配置文件加载失败: {e}")
            raise

    @staticmethod
    def _load_toml(path):
        with open(path, "rb") as f:
            return tomllib.load(f)

    @staticmethod
    def _basic_validate(config):
        if not isinstance(config, dict):
            return False
        if "log_dir" not in config:
            return False
        tasks = config.get("tasks")
        if not tasks or not isinstance(tasks, list) or len(tasks) == 0:
            return False
        for task in tasks:
            if not isinstance(task, dict):
                return False
            if "mode" not in task or "source" not in task:
                return False
        return True
