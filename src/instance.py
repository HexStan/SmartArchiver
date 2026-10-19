"""实例标识与跨进程控制通道。

「实例标识名」（instance_name）是同一份程序副本的身份证，锁文件、
控制文件、PID 文件与日志前缀全部由它派生（前三者位于系统临时目录）：

- ``<标识>.lock`` —— 实例锁（见 ``src.utils.SingleInstance``），
  同时充当存活探测依据：锁被占用即存在运行中的实例；
- ``<标识>.ctl`` —— 控制文件，``stop`` 子命令与运行实例之间的指令通道；
- ``<标识>.pid`` —— 运行实例的 PID，供 ``stop --fast`` 强杀兜底。

多副本部署时为每份程序配置不同的实例标识，锁、控制与日志天然隔离。
"""

import json
import logging
import os
import sys
import tempfile
import threading
import time

import psutil

from src.utils import (
    DEFAULT_INSTANCE_NAME,
    DEFAULT_SERVER_INSTANCE_NAME,
    _acquire_lock,
    _release_lock,
    resolve_instance_name,
)

logger = logging.getLogger(__name__)

# stop 指令的三种模式
STOP_GRACEFUL = "graceful"  # 当前文件传输完成后停止
STOP_SLOW = "slow"  # 本轮任务结束后停止
STOP_FAST = "fast"  # 立即中止传输并退出

_STOP_MODES = (STOP_GRACEFUL, STOP_SLOW, STOP_FAST)

# 控制文件指令格式（JSON）：{"cmd": "stop", "mode": ..., "ts": ...}
_CMD_STOP = "stop"


def resolve_identity(raw_config, is_server=False):
    """从原始配置解析实例标识，返回 (标识, 是否非法回退默认)。"""
    default = DEFAULT_SERVER_INSTANCE_NAME if is_server else DEFAULT_INSTANCE_NAME
    return resolve_instance_name(raw_config.get("instance_name"), default)


class InstancePaths:
    """实例运行时文件的路径集合（锁 / 控制 / PID，均在系统临时目录）。"""

    def __init__(self, name):
        self.name = name
        self.directory = tempfile.gettempdir()

    @property
    def lock(self):
        return os.path.join(self.directory, f"{self.name}.lock")

    @property
    def ctl(self):
        return os.path.join(self.directory, f"{self.name}.ctl")

    @property
    def pid(self):
        return os.path.join(self.directory, f"{self.name}.pid")


def instance_paths(instance_name):
    return InstancePaths(instance_name)


def is_running(instance_name):
    """探测是否存在持有实例锁的运行实例。

    以「能否拿到锁」为准而不是检查 PID：锁由操作系统在进程退出
    （含崩溃被杀）时自动释放，不存在 PID 复用误判。
    """
    lock_path = instance_paths(instance_name).lock
    if not os.path.exists(lock_path):
        return False
    try:
        with open(lock_path, "a+") as fp:
            try:
                _acquire_lock(fp)
            except OSError:
                return True  # 锁被占用 → 实例存活
            _release_lock(fp)
            return False  # 锁空闲 → 无实例（仅剩崩溃残留的锁文件）
    except OSError:
        return False


def read_pid(instance_name):
    """读取运行实例的 PID；文件缺失或内容非法时返回 None。"""
    try:
        with open(instance_paths(instance_name).pid, encoding="utf-8") as f:
            return int(f.read().strip())
    except OSError, ValueError:
        return None


def send_stop(instance_name, mode):
    """原子写入停止指令到控制文件（临时文件 + os.replace）。"""
    if mode not in _STOP_MODES:
        raise ValueError(f"未知的停止模式: {mode}")
    path = instance_paths(instance_name).ctl
    payload = json.dumps({"cmd": _CMD_STOP, "mode": mode, "ts": time.time()})
    tmp_path = f"{path}.{os.getpid()}.tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(payload)
        os.replace(tmp_path, path)
    except OSError:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        raise


def wait_for_exit(instance_name, timeout=None, poll_interval=0.25, on_wait=None):
    """阻塞等待实例退出（基于锁探测轮询），返回 True 表示已退出。

    Args:
        timeout: 最长等待秒数；None 表示一直等待。
        on_wait: 每次轮询调用的回调 ``on_wait(elapsed)``，供展示等待进度。
    """
    started = time.time()
    while True:
        if not is_running(instance_name):
            return True
        elapsed = time.time() - started
        if timeout is not None and elapsed >= timeout:
            return False
        if on_wait is not None:
            on_wait(elapsed)
        time.sleep(poll_interval)


def force_kill_tree(pid, term_timeout=3.0):
    """强制结束进程及其全部后代（``stop --fast`` 的兜底手段）。

    动手前 best-effort 校验目标进程的可执行文件名与当前程序一致，
    防止 PID 文件残留遇到 PID 复用时误杀无关进程。

    Returns:
        False 表示校验未通过，未执行任何终止操作。
    """
    try:
        parent = psutil.Process(pid)
    except psutil.NoSuchProcess:
        return True
    except psutil.Error:
        return False

    if not _pid_belongs_to_us(parent):
        return False

    try:
        targets = [parent, *parent.children(recursive=True)]
    except psutil.Error:
        targets = [parent]
    _terminate_and_kill(targets, term_timeout)
    return True


def _pid_belongs_to_us(process):
    """best-effort 校验目标进程是本程序（可执行文件名一致）。"""
    try:
        exe = process.exe()
    except psutil.Error:
        return True  # 无法检查时放行：锁被占用已提供主要置信
    if not exe:
        return True
    expected = os.path.basename(sys.executable).lower()
    return os.path.basename(exe).lower() == expected


def _terminate_and_kill(targets, term_timeout):
    """先 terminate 后 kill：给进程留出优雅退出的窗口，超时者强杀。"""
    for proc in targets:
        try:
            proc.terminate()
        except psutil.Error:
            pass
    _gone, alive = psutil.wait_procs(targets, timeout=term_timeout)
    for proc in alive:
        try:
            proc.kill()
        except psutil.Error:
            pass


def terminate_process_tree(pid, term_timeout=3.0, logger=None):
    """终止进程树，不留孤儿进程。

    供 ``ShutdownCoordinator`` 在 fast 中止时终止外部同步进程
    （rsync/rclone 可能派生子进程，只杀直接子进程会留下孤儿）。
    """
    try:
        parent = psutil.Process(pid)
    except psutil.NoSuchProcess:
        return
    except psutil.Error as e:
        if logger:
            logger.warning(f"终止外部进程 (PID {pid}) 失败: {e}")
        return

    try:
        targets = [parent, *parent.children(recursive=True)]
    except psutil.Error:
        targets = [parent]
    _terminate_and_kill(targets, term_timeout)

    if logger:
        logger.warning(f"已终止外部同步进程树 (PID {pid})。")


class ControlWatcher(threading.Thread):
    """控制文件监视线程（守护线程）：轮询 ctl 指令并分发。

    实例获锁后启动；读到合法的 stop 指令即删除控制文件并回调
    ``on_command(mode)``。实例启动之前写入的遗留指令（上次实例
    崩溃残留）会被忽略，避免误停新实例。
    """

    def __init__(self, instance_name, on_command, poll_interval=0.5):
        super().__init__(daemon=True, name=f"control-watcher-{instance_name}")
        self._ctl_path = instance_paths(instance_name).ctl
        self._on_command = on_command
        self._poll_interval = poll_interval
        self._started_at = time.time()

    def run(self):
        while True:
            try:
                self.poll_once()
            except Exception as e:  # noqa: BLE001
                # 回调异常绝不能让监视线程死掉，否则实例将无法被 stop 控制
                logger.error(f"处理控制指令时发生错误: {e}")
            time.sleep(self._poll_interval)

    def poll_once(self):
        """单次轮询：读取、校验、删除控制文件并分发指令。"""
        try:
            with open(self._ctl_path, encoding="utf-8") as f:
                payload = json.load(f)
        except OSError, ValueError:
            return  # 不存在、写入未完成或内容损坏，下轮再试

        mode = payload.get("mode")
        if payload.get("cmd") != _CMD_STOP or mode not in _STOP_MODES:
            return

        try:
            ts = float(payload.get("ts", 0))
        except TypeError, ValueError:
            return
        if ts < self._started_at:
            return  # 上个实例的遗留指令，忽略

        try:
            os.remove(self._ctl_path)
        except OSError:
            return

        self._on_command(mode)
