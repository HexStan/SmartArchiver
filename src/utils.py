import os
import sys
import tomllib
import fnmatch

from humanfriendly import parse_size, InvalidSize

try:  # 尝试导入 fcntl，Windows 下没有这个模块
    import fcntl
except ImportError:
    fcntl = None


class SingleInstance:
    """
    文件锁上下文管理器，防止多实例运行
    """

    def __init__(self, lock_file_path, logger):
        self.lock_file_path = lock_file_path
        self.logger = logger
        self.fp = None
        # 判断当前是否为 Windows 系统
        self.is_windows = os.name == "nt"

    def __enter__(self):
        # 如果是 Windows 或者 fcntl 导入失败，直接跳过加锁逻辑
        if self.is_windows or fcntl is None:
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
        # Windows 环境下无需解锁
        if self.is_windows or fcntl is None:
            return
        if self.fp:
            try:
                fcntl.flock(self.fp, fcntl.LOCK_UN)
                self.fp.close()
            except Exception:
                pass


def load_config(config_path):
    with open(config_path, "rb") as f:
        return tomllib.load(f)


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


def parse_size_string(size_str):
    """
    将人类可读的大小字符串转换为字节数
    包装 humanfriendly.parse_size，处理空值情况
    输入 "10 MB", "1g", "5KB" -> 输出 int (字节)
    """
    if not size_str:
        return 0

    # 转换为字符串并去除空格
    s = str(size_str).strip()

    # 特殊处理：支持 -1 作为全匹配标志
    if s == "-1":
        return -1

    try:
        # binary=True 表示使用 1024 进位 (MiB, KiB)
        return parse_size(s, binary=True)
    except (InvalidSize, ValueError):
        # 解析失败默认返回 0，或者按需抛出异常
        return 0


def match_pattern(name, pattern):
    """
    匹配模式：
    支持通配符 * 和 ?，支持多级目录匹配
    大小写不敏感
    """
    name = name.replace("\\", "/").lower()
    pattern = pattern.replace("\\", "/").lower()

    if "/" not in pattern:
        name = name.split("/")[-1]

    return fnmatch.fnmatch(name, pattern)


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


def clean_empty_dirs(source_root, logger):
    """
    递归深度优先删除空目录
    """
    # topdown=False 确保自底向上遍历 (对应 -depth)
    for root, dirs, files in os.walk(source_root, topdown=False):
        if root == source_root:
            continue
        try:
            # os.rmdir 只有在目录为空时才成功，如果不为空会抛异常（我们捕获并忽略）
            os.rmdir(root)
            if logger:
                rel_dir = os.path.relpath(root, source_root)
                logger.debug(f"删除空目录: {rel_dir}")
        except OSError:
            pass  # 目录非空，跳过

