import fnmatch
import os
import sys
import tempfile

from humanfriendly import InvalidSize, parse_size

try:
    import fcntl
except ImportError:
    fcntl = None

try:
    import msvcrt
except ImportError:
    msvcrt = None

# ============================================================
# 单实例 / 文件锁
# ============================================================

# lock_file 未配置或非法时使用的默认锁文件名
DEFAULT_LOCK_NAME = "smartarchiver.lock"
DEFAULT_SERVER_LOCK_NAME = "smartarchiver-server.lock"


def _lock_available():
    """当前平台是否具备文件锁原语（POSIX 的 fcntl 或 Windows 的 msvcrt）。"""
    return fcntl is not None or msvcrt is not None


def _acquire_lock(fp):
    """在已打开的文件对象上加非阻塞排他锁，锁被占用时抛出 OSError。"""
    if fcntl is not None:
        # LOCK_EX: 排他锁, LOCK_NB: 非阻塞
        fcntl.flock(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
    elif msvcrt is not None:
        # Windows 的字节范围锁作用于当前位置起的若干字节，
        # 对空文件的首字节加锁同样有效
        fp.seek(0)
        msvcrt.locking(fp.fileno(), msvcrt.LK_NBLCK, 1)


def _release_lock(fp):
    """释放 _acquire_lock 加上的锁（Windows 必须对同一字节区间解锁）。"""
    if fcntl is not None:
        fcntl.flock(fp, fcntl.LOCK_UN)
    elif msvcrt is not None:
        fp.seek(0)
        msvcrt.locking(fp.fileno(), msvcrt.LK_UNLCK, 1)


def _resolve_lock_name(value, default_name):
    """把 lock_file 配置值解析为锁文件名，返回 (文件名, 是否非法回退)。

    配置值只接受纯文件名（不含任何路径成分），锁文件的存放目录由程序
    按平台决定，用户不可自定义。未配置时静默使用默认名；配置了但非法时
    回退默认名并标记非法，由调用方给出警告。
    """
    if isinstance(value, str):
        name = value.strip()
        if name and name not in (".", "..") and name == os.path.basename(name):
            return name, False
        if name:
            return default_name, True
    return default_name, False


class SingleInstance:
    """
    文件锁上下文管理器，防止多实例运行

    - Linux / macOS：fcntl.flock
    - Windows：msvcrt.locking（锁住文件首字节）
    - 锁由操作系统在进程退出时自动释放，不会残留死锁
    """

    def __init__(self, lock_name, logger, default_name=DEFAULT_LOCK_NAME):
        name, invalid = _resolve_lock_name(lock_name, default_name)
        if invalid:
            logger.warning(f"lock_file 配置值非法，已使用默认锁文件名 {default_name}。")
        self.lock_file_path = os.path.join(tempfile.gettempdir(), name)
        self.logger = logger
        self.fp = None

    def __enter__(self):
        if not _lock_available():
            self.logger.warning("当前平台没有可用的文件锁原语，实例锁未启用。")
            return self

        try:
            self.fp = open(self.lock_file_path, "w")
        except Exception as e:  # noqa: BLE001
            self.logger.error(f"无法创建锁文件 {self.lock_file_path}: {e}")
            sys.exit(1)

        try:
            _acquire_lock(self.fp)
        except OSError:
            self.logger.info("另一个实例被启动了，现已自动退出。")
            sys.exit(1)
        except Exception as e:  # noqa: BLE001
            self.logger.error(f"无法获取实例锁: {e}")
            sys.exit(1)

        return self

    def __exit__(self, _type, value, traceback):
        if self.fp is None:
            return
        try:
            _release_lock(self.fp)
        except Exception:  # noqa: BLE001, S110
            pass
        finally:
            try:
                self.fp.close()
            except Exception:  # noqa: BLE001, S110
                pass
            self.fp = None


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
