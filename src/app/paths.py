"""应用运行时路径解析。

统一处理两种运行形态下的路径差异：
- 源码运行（python main.py）：应用根目录 = 仓库根目录；
- PyInstaller 打包运行（onedir）：应用根目录 = 可执行文件所在目录，
  用户可编辑的 config/、logs/ 等均位于该目录下。

打包时内置的第三方工具（rclone）由 PyInstaller 放入 _internal/rclone/，
bundled_rclone_path() 负责在运行时定位它们。
"""

import os
import sys


def is_frozen():
    """是否运行于 PyInstaller 打包环境中。"""
    return getattr(sys, "frozen", False)


def app_base_dir():
    """应用根目录：打包后为可执行文件所在目录，源码运行为仓库根目录。"""
    if is_frozen():
        return os.path.dirname(os.path.abspath(sys.executable))
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def config_path(is_server):
    """配置文件路径：客户端为 config/config.toml，服务器为 config/config.server.toml。"""
    name = "config/config.server.toml" if is_server else "config/config.toml"
    return os.path.join(app_base_dir(), name)


def resolve_log_dir(raw_config):
    """解析日志目录：相对路径以应用根目录为基准，与启动时的工作目录无关。"""
    log_dir = raw_config.get("log_dir", "./logs")
    if log_dir and not os.path.isabs(log_dir):
        return os.path.normpath(os.path.join(app_base_dir(), log_dir))
    return log_dir


def bundled_rclone_path():
    """打包版内置 rclone 的绝对路径；未打包或内置缺失时返回 None。"""
    if not is_frozen():
        return None

    base = getattr(sys, "_MEIPASS", None)
    if not base:
        return None

    exe_name = "rclone.exe" if os.name == "nt" else "rclone"
    path = os.path.join(base, "rclone", exe_name)
    if not os.path.isfile(path):
        return None

    _ensure_executable(path)
    return path


def _ensure_executable(path):
    """兜底确保二进制具有可执行权限（防打包/解压过程丢失执行位）。"""
    if os.name == "nt":
        return
    if not os.access(path, os.X_OK):
        try:
            os.chmod(path, 0o755)
        except OSError:
            pass
