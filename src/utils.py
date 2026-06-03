import os
import re
import shutil
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


def is_file_locked(filepath):
    """
    检查文件是否被 flock 锁定
    仅在 Unix/Linux 系统有效，Windows 下始终返回 False
    """
    if fcntl is None:
        return False

    locked = False
    f = None
    try:
        # 打开文件进行检查
        f = open(filepath, "r")
        # 尝试获取非阻塞的排他锁 (LOCK_EX | LOCK_NB)
        # 如果文件已被其他进程锁定 (共享锁或排他锁)，这里会抛出 IOError/BlockingIOError
        fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        # 如果能获取到锁，说明没被占用，立即解锁
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    except (IOError, OSError):
        locked = True
    finally:
        if f:
            try:
                f.close()
            except Exception:
                pass
    return locked


# ============================================================
# 配置加载
# ============================================================


def load_config(config_path):
    with open(config_path, "rb") as f:
        return tomllib.load(f)


# ============================================================
# 文件操作
# ============================================================


def copy_file(src_path, dest_path):
    dest_dir = os.path.dirname(dest_path)
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir, exist_ok=True)
    shutil.copy2(src_path, dest_path)


def move_file(src_path, dest_path):
    dest_dir = os.path.dirname(dest_path)
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir, exist_ok=True)
    shutil.move(src_path, dest_path)


def get_unique_dest(dest_path):
    """
    如果目标文件存在，生成一个带编号的新路径
    例如: /path/file.txt -> /path/file-1.txt
    """
    if not os.path.exists(dest_path):
        return dest_path

    directory = os.path.dirname(dest_path)
    filename = os.path.basename(dest_path)
    name, ext = os.path.splitext(filename)

    counter = 1
    while True:
        new_filename = f"{name}-{counter}{ext}"
        new_path = os.path.join(directory, new_filename)
        if not os.path.exists(new_path):
            return new_path
        counter += 1


# ============================================================
# 目录操作
# ============================================================


def get_dir_size_and_mtime(dir_path):
    """
    计算目录的总大小和最新修改时间
    """
    total_size = 0
    latest_mtime = 0

    try:
        # 获取目录本身的修改时间
        latest_mtime = os.stat(dir_path).st_mtime

        for root, dirs, files in os.walk(dir_path):
            for f in files:
                fp = os.path.join(root, f)
                if not os.path.islink(fp):
                    try:
                        stat = os.stat(fp)
                        total_size += stat.st_size
                        if stat.st_mtime > latest_mtime:
                            latest_mtime = stat.st_mtime
                    except OSError:
                        pass
    except OSError:
        pass

    return total_size, latest_mtime


def clean_empty_dirs(source_root):
    from src.app_context import AppContext

    ctx = AppContext.get()
    if not os.path.exists(source_root):
        return

    for root, dirs, files in os.walk(source_root, topdown=False):
        rel_dir = os.path.relpath(root, source_root)

        if root == source_root:
            continue

        if os.path.islink(root) or os.path.ismount(root):
            ctx.logger.debug(f"跳过删除空目录 (符号链接或挂载点): {rel_dir}")
            continue

        try:
            with os.scandir(root) as it:
                if any(it):
                    continue

            os.rmdir(root)
            ctx.logger.debug(f"删除空目录: {rel_dir}")
        except OSError as e:
            ctx.logger.debug(f"跳过删除空目录 (出现错误): {rel_dir}\n{e}")


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

        remotes[alias] = RemoteClient(
            address=address,
            api_key=key,
            alias=alias,
            timeout=timeout,
        )

    return remotes
