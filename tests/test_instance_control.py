"""实例控制通道测试：存活探测、PID 文件、控制指令协议与进程树终止。

锁探测与 PID/控制文件的本质是跨进程协作——存活探测以真实锁原语为边界
（复用 test_instance_lock.py 的子进程范式），进程树终止以真实子进程为边界。
控制文件协议（写 → 轮询 → 分发 → 删除）与时间戳门限是纯文件行为，
用临时目录真实文件驱动。
"""

import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import psutil
import pytest

from src.instance import (
    STOP_FAST,
    STOP_GRACEFUL,
    STOP_SLOW,
    ControlWatcher,
    force_kill_tree,
    instance_paths,
    is_running,
    read_pid,
    resolve_identity,
    send_stop,
    terminate_process_tree,
    wait_for_exit,
)
from src.utils import DEFAULT_INSTANCE_NAME, DEFAULT_SERVER_INSTANCE_NAME, SingleInstance

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _unique_name():
    """系统临时目录全局共享，用随机名避免与其他进程/测试互扰。"""
    return f"smartarchiver-test-{uuid.uuid4().hex}"


def _paths(name):
    return instance_paths(name)


def _cleanup_files(name):
    for attr in ("lock", "pid", "ctl"):
        Path(getattr(_paths(name), attr)).unlink(missing_ok=True)


@pytest.fixture
def unique_name():
    """全局临时目录里的一次性实例标识，测试结束自动清理三件套文件。"""
    name = _unique_name()
    yield name
    _cleanup_files(name)


# ============================================================
# 实例标识解析
# ============================================================


def test_resolve_identity_defaults_for_client():
    assert resolve_identity({}) == (DEFAULT_INSTANCE_NAME, False)


def test_resolve_identity_defaults_for_server():
    assert resolve_identity({}, is_server=True) == (
        DEFAULT_SERVER_INSTANCE_NAME,
        False,
    )


def test_resolve_identity_uses_configured_name():
    assert resolve_identity({"instance_name": "nas-backup"}) == ("nas-backup", False)


def test_resolve_identity_rejects_path_style():
    name, invalid = resolve_identity({"instance_name": "sub/dir"})
    assert invalid
    assert name == DEFAULT_INSTANCE_NAME


# ============================================================
# 存活探测（真实锁原语）
# ============================================================

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


def test_is_running_false_without_lock_file(unique_name):
    assert not is_running(unique_name)


def test_is_running_tracks_held_lock(capture_log, unique_name):
    """持锁期间判定存活，退出后（锁由 OS 释放）判定不存在。"""
    try:
        with SingleInstance(unique_name, capture_log):
            assert is_running(unique_name)
        assert not is_running(unique_name)
    finally:
        _cleanup_files(unique_name)


def test_read_pid_written_by_running_instance(capture_log, unique_name):
    try:
        assert read_pid(unique_name) is None
        with SingleInstance(unique_name, capture_log):
            assert read_pid(unique_name) == os.getpid()
    finally:
        _cleanup_files(unique_name)


def test_wait_for_exit_returns_immediately_when_not_running(unique_name):
    assert wait_for_exit(unique_name, timeout=1.0)


# ============================================================
# 控制指令协议
# ============================================================


def test_send_stop_rejects_unknown_mode(unique_name):
    with pytest.raises(ValueError):
        send_stop(unique_name, "immediately")


def test_watcher_dispatches_valid_command(unique_name):
    ctl = Path(_paths(unique_name).ctl)
    received = []
    watcher = ControlWatcher(unique_name, received.append)

    send_stop(unique_name, STOP_FAST)
    assert ctl.exists()

    watcher.poll_once()

    assert received == [STOP_FAST]
    assert not ctl.exists()  # 指令消费后删除


def test_watcher_polls_all_modes(unique_name):
    received = []
    watcher = ControlWatcher(unique_name, received.append)

    for mode in (STOP_GRACEFUL, STOP_SLOW, STOP_FAST):
        send_stop(unique_name, mode)
        watcher.poll_once()

    assert received == [STOP_GRACEFUL, STOP_SLOW, STOP_FAST]


def test_watcher_ignores_command_written_before_it_started(unique_name):
    """上个实例崩溃遗留的指令（时间戳早于本实例启动）不得误停新实例。"""
    ctl = Path(_paths(unique_name).ctl)
    ctl.write_text(
        json.dumps({"cmd": "stop", "mode": STOP_FAST, "ts": 0}),
        encoding="utf-8",
    )

    received = []
    ControlWatcher(unique_name, received.append).poll_once()

    assert received == []
    assert ctl.exists()  # 未消费，留在原地


def test_watcher_ignores_garbage_and_unknown_commands(unique_name):
    ctl = Path(_paths(unique_name).ctl)
    received = []
    watcher = ControlWatcher(unique_name, received.append)

    # 损坏的 JSON（写入进行中的残缺内容）
    ctl.write_text('{"cmd": "st', encoding="utf-8")
    watcher.poll_once()
    assert received == []

    # 未知指令 / 未知模式
    ctl.write_text(json.dumps({"cmd": "reload", "ts": time.time()}), encoding="utf-8")
    watcher.poll_once()
    ctl.write_text(
        json.dumps({"cmd": "stop", "mode": "pause", "ts": time.time()}),
        encoding="utf-8",
    )
    watcher.poll_once()

    assert received == []


# ============================================================
# 进程树终止（真实子进程边界）
# ============================================================

SPAWN_SLEEPER_CODE = """
import subprocess
import sys
import time

child = subprocess.Popen(
    [sys.executable, "-c", "import time; time.sleep(120)"]
)
print(child.pid, flush=True)
time.sleep(120)
"""


def _spawn_sleeper_tree():
    """拉起「父进程 + 子进程」的进程树，返回 (Popen, 孙进程 PID)。"""
    proc = subprocess.Popen(
        [sys.executable, "-c", SPAWN_SLEEPER_CODE],
        cwd=PROJECT_ROOT,
        stdout=subprocess.PIPE,
        text=True,
    )
    grandchild_pid = int(proc.stdout.readline().strip())
    return proc, grandchild_pid


def test_terminate_process_tree_kills_descendants():
    """终止整棵进程树：直接子进程与其后代都不残留（不留孤儿）。"""
    parent, grandchild_pid = _spawn_sleeper_tree()
    try:
        terminate_process_tree(parent.pid, term_timeout=5.0)
        parent.wait(timeout=10)

        assert parent.poll() is not None
        assert not psutil.pid_exists(grandchild_pid)
    finally:
        if parent.poll() is None:
            parent.kill()
            parent.wait(timeout=10)


def test_force_kill_tree_noop_for_dead_pid():
    """目标已不存在时直接视为成功（无副作用）。"""
    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    pid = proc.pid
    proc.wait()

    assert force_kill_tree(pid)


def test_force_kill_tree_refuses_foreign_process(monkeypatch):
    """可执行文件名与当前程序不一致时不执行终止（防 PID 复用误杀）。"""

    class FakeForeignProcess:
        def exe(self):
            return "C:/Windows/notepad.exe"

    monkeypatch.setattr("src.instance.psutil.Process", lambda _pid: FakeForeignProcess())

    assert force_kill_tree(424242) is False
