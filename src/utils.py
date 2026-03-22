import os
import sys
import tomllib

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
        self.is_windows = os.name == 'nt'

    def __enter__(self):
        # 如果是 Windows 或者 fcntl 导入失败，直接跳过加锁逻辑
        if self.is_windows or fcntl is None:
            return self
        try:
            self.fp = open(self.lock_file_path, 'w')
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
    with open(config_path, 'rb') as f:
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
        f = open(filepath, 'r')
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

    # ALL 返回无穷大
    if s.upper() == "ALL":
        return float('inf')

    try:
        # binary=True 表示使用 1024 进位 (MiB, KiB)
        return parse_size(s, binary=True)
    except (InvalidSize, ValueError):
        # 解析失败默认返回 0，或者按需抛出异常
        return 0
