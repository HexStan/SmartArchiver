import os
import re
import sys
import tomllib
import fnmatch

from humanfriendly import parse_size, InvalidSize

try:
    import fcntl
except ImportError:
    fcntl = None

# ============================================================
# 单实例 / 文件锁
# ============================================================


class SingleInstance:
    """
    文件锁上下文管理器，防止多实例运行
    """

    def __init__(self, lock_file_path, logger):
        self.lock_file_path = lock_file_path
        self.logger = logger
        self.fp = None
        self.is_windows = os.name == "nt"

    def __enter__(self):
        if self.lock_file_path is None or self.is_windows or fcntl is None:
            return self
        try:
            self.fp = open(self.lock_file_path, "w")
            # LOCK_EX: 排他锁, LOCK_NB: 非阻塞
            fcntl.flock(self.fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError:
            self.logger.info("另一个实例被启动了，现已自动退出。")
            sys.exit(1)
        except Exception as e:
            # 处理可能的权限问题或其他IO错误
            self.logger.error(f"无法创建/获取锁文件: {e}")
            sys.exit(1)
        return self

    def __exit__(self, _type, value, traceback):
        if self.lock_file_path is None or self.is_windows or fcntl is None:
            return
        if self.fp:
            try:
                fcntl.flock(self.fp, fcntl.LOCK_UN)
                self.fp.close()
            except Exception:
                pass


# ============================================================
# 配置加载
# ============================================================


def load_config(config_path):
    with open(config_path, "rb") as f:
        return tomllib.load(f)


# ============================================================
# 模式匹配
# ============================================================


def match_pattern(name, pattern):
    name = name.replace("\\", "/").lower()
    pattern = pattern.replace("\\", "/").lower()

    if "/" not in pattern:
        name = name.split("/")[-1]

    return fnmatch.fnmatch(name, pattern)


# ============================================================
# 大小解析
# ============================================================


def parse_size_string(size_str):
    if not size_str:
        return 0

    s = str(size_str).strip()

    if s == "-1":
        return -1

    try:
        return parse_size(s, binary=True)
    except (InvalidSize, ValueError):
        return 0


# ============================================================
# 远端配置
# ============================================================

_ALIAS_PATTERN = re.compile(r"^[a-zA-Z0-9\-_]+$")


def validate_remote_alias(alias):
    return bool(_ALIAS_PATTERN.match(alias))


def parse_remote_config(config):
    remotes = {}
    http_remotes = config.get("http_remotes", [])
    if not http_remotes:
        return remotes

    from src.remote.client import RemoteClient

    for entry in http_remotes:
        alias = entry.get("alias", "")
        address = entry.get("address", "").strip()
        key = entry.get("key", "")
        timeout = entry.get("timeout")

        if not alias or not address or not key:
            continue

        if not validate_remote_alias(alias):
            continue

        if timeout is not None:
            try:
                timeout = float(timeout)
            except (TypeError, ValueError):
                timeout = None

        remotes[alias] = RemoteClient(
            address=address,
            api_key=key,
            alias=alias,
            timeout=timeout,
        )

    return remotes
