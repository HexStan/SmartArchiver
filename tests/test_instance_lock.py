"""实例锁测试：跨进程互斥契约 + lock_file 文件名解析契约。

单实例锁的本质是进程间互斥——只有当第二个真实进程拿不到锁时，"防止多实例"
才成立。因此锁行为以子进程为边界驱动真实锁原语（原则三：桩只放在进程边界，
这里连桩都不需要，子进程就是真实世界）。Linux/macOS 走 fcntl.flock，
Windows 走 msvcrt.locking，CI 矩阵天然覆盖两种实现。

文件名解析（_resolve_lock_name）是纯函数。它的"未填/非法 → 回退默认名"
契约若经由 SingleInstance 验证，会在系统临时目录真实创建默认名锁文件，
可能与维护者正在运行的实例互斥，因此直接对纯函数断言（白盒补盲）。
"""

import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

from src.utils import DEFAULT_LOCK_NAME, SingleInstance, _resolve_lock_name

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# 子进程脚本：以命令行参数为 lock_file 配置值尝试获取实例锁。
# 拿到锁 → 打印 locked 并正常退出（0）；锁被占用 → sys.exit(1)（退出码 1）。
CHILD_CODE = """
import sys

from src.utils import SingleInstance


class QuietLogger:
    def info(self, msg):
        pass

    def warning(self, msg):
        pass

    def error(self, msg):
        pass


with SingleInstance(sys.argv[1], QuietLogger()):
    print("locked")
"""


def _unique_lock_name():
    """系统临时目录是全局共享的，用随机名避免与其他进程/测试互扰。"""
    return f"smartarchiver-test-{uuid.uuid4().hex}.lock"


def _acquire_in_child(lock_name):
    return subprocess.run(
        [sys.executable, "-c", CHILD_CODE, lock_name],
        check=False,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=60,
    )


def test_second_process_cannot_acquire_held_lock(capture_log):
    """第一个实例持锁期间，第二个进程必须拿不到锁并以退出码 1 退出。"""
    lock_name = _unique_lock_name()
    lock_path = Path(tempfile.gettempdir()) / lock_name

    try:
        with SingleInstance(lock_name, capture_log):
            result = _acquire_in_child(lock_name)
    finally:
        lock_path.unlink(missing_ok=True)

    assert result.returncode == 1
    assert "locked" not in result.stdout


def test_lock_released_after_context_exit(capture_log):
    """with 块结束后锁被释放，后续进程可以正常获取。"""
    lock_name = _unique_lock_name()
    lock_path = Path(tempfile.gettempdir()) / lock_name

    try:
        with SingleInstance(lock_name, capture_log):
            pass
        result = _acquire_in_child(lock_name)
    finally:
        lock_path.unlink(missing_ok=True)

    assert result.returncode == 0
    assert "locked" in result.stdout


def test_lock_file_located_in_platform_temp_dir(capture_log):
    """锁文件位置由程序决定：统一存放在系统临时目录，配置值仅作为文件名。"""
    lock_name = _unique_lock_name()
    lock_path = Path(tempfile.gettempdir()) / lock_name

    try:
        with SingleInstance(lock_name, capture_log):
            assert lock_path.exists()
    finally:
        lock_path.unlink(missing_ok=True)


def test_valid_lock_name_is_kept():
    """合法纯文件名原样生效。"""
    assert _resolve_lock_name("my.lock", DEFAULT_LOCK_NAME) == ("my.lock", False)


def test_missing_lock_name_uses_default():
    """未配置 lock_file 时静默使用默认文件名。"""
    assert _resolve_lock_name(None, DEFAULT_LOCK_NAME) == (DEFAULT_LOCK_NAME, False)


def test_blank_lock_name_uses_default():
    """空白值视为未配置。"""
    assert _resolve_lock_name("", DEFAULT_LOCK_NAME) == (DEFAULT_LOCK_NAME, False)
    assert _resolve_lock_name("   ", DEFAULT_LOCK_NAME) == (DEFAULT_LOCK_NAME, False)


def test_path_style_lock_name_falls_back_to_default():
    """路径写法非法：回退默认文件名并标记（旧版路径配置借此平滑迁移）。"""
    assert _resolve_lock_name(
        "/tmp/smartarchiver.lock", DEFAULT_LOCK_NAME
    ) == (DEFAULT_LOCK_NAME, True)


def test_relative_path_lock_name_falls_back_to_default():
    """带子目录成分的名字同样非法。"""
    assert _resolve_lock_name(
        "sub/dir.lock", DEFAULT_LOCK_NAME
    ) == (DEFAULT_LOCK_NAME, True)


def test_dot_lock_name_falls_back_to_default():
    """当前目录 / 上级目录这类特殊名非法。"""
    assert _resolve_lock_name(".", DEFAULT_LOCK_NAME) == (DEFAULT_LOCK_NAME, True)
    assert _resolve_lock_name("..", DEFAULT_LOCK_NAME) == (DEFAULT_LOCK_NAME, True)
