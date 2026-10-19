"""控制子命令 start / log / stop。

一次性 CLI：只从配置解析实例标识与日志目录（宽松模式，配置异常时
回退默认值，保证 log/stop 始终可用），不初始化运行环境。start 以
分离式子进程拉起运行路径（src.runtime）。
"""

import os
import subprocess
import sys

from src import app_paths, instance
from src.config_manager import ConfigManager
from src.logtail import LogFollower

# stop --fast 发出指令后等待实例自行退出的时限，超时则强杀
_FAST_STOP_TIMEOUT = 8.0


def _load_control_context(is_server, strict=False):
    """控制子命令的配置读取，返回 (instance_name, log_dir)。

    与运行路径不同，这里不做完整校验，只解析实例标识与日志目录。
    strict=True（start）时配置缺失或解析失败直接退出；宽松模式
    （log/stop）回退默认实例标识与日志目录，保证配置异常时依然可用。
    """
    path = app_paths.config_path(is_server)
    raw_config = {}
    if os.path.exists(path):
        try:
            raw_config = ConfigManager.load_toml(path)
        except Exception as e:  # noqa: BLE001
            if strict:
                print(f"Error: 配置文件解析失败: {e}")
                sys.exit(1)
            print(f"警告: 配置文件解析失败（{e}），按默认实例标识处理。")
    else:
        if strict:
            print(f"Error: 配置文件 {path} 未找到。")
            sys.exit(1)
        print(f"警告: 配置文件 {path} 未找到，按默认实例标识处理。")

    instance_name, invalid = instance.resolve_identity(raw_config, is_server)
    if invalid:
        print(f"警告: instance_name 配置值非法，按默认实例标识 {instance_name} 处理。")
    return instance_name, app_paths.resolve_log_dir(raw_config)


def _command_hint(is_server):
    prog = os.path.basename(sys.argv[0]) or "main.py"
    suffix = " --server" if is_server else ""
    return f"{prog}{suffix}"


def _spawn_detached(is_server):
    """以脱离终端的方式拉起自身，返回子进程 PID。"""
    if app_paths.is_frozen():
        cmd = [sys.executable]
    else:
        cmd = [sys.executable, os.path.join(app_paths.app_base_dir(), "main.py")]
    if is_server:
        cmd.append("--server")

    env = os.environ.copy()
    env[instance.DETACHED_ENV] = "1"

    kwargs = {}
    if os.name == "nt":
        # CREATE_NO_WINDOW：进程带一个不可见的控制台。不用 DETACHED_PROCESS
        # 是因为源码模式下 venv 的 python.exe 是跳板程序，它重新 spawn 的
        # 真实解释器不继承该标志，会被 Windows 分配一个可见的空控制台窗口；
        # 而隐藏控制台会被正常继承，跳板链路同样无窗口。
        kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW | subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        kwargs["start_new_session"] = True

    try:
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=env,
            **kwargs,
        )
    except OSError as e:
        print(f"Error: 无法启动分离式实例: {e}")
        sys.exit(1)
    return proc.pid


def _follow_logs(is_server, lines=100):
    """滚动显示运行日志；Ctrl+C 结束跟随（不影响实例运行）。"""
    instance_name, log_dir = _load_control_context(is_server)
    follower = LogFollower(log_dir, instance_name)
    try:
        follower.follow(lines)
    except KeyboardInterrupt:
        pass


def cmd_start(is_server):
    """start 子命令：脱离终端启动实例，随后滚动显示运行日志。"""
    instance_name, _log_dir = _load_control_context(is_server, strict=True)
    if instance.is_running(instance_name):
        pid = instance.read_pid(instance_name)
        where = f" (PID {pid})" if pid else ""
        print(f"已有实例正在运行{where}，无需重复启动。")
        sys.exit(1)

    pid = _spawn_detached(is_server)
    print(f"实例已启动 (PID {pid})，已脱离终端，本窗口可随时关闭。")
    print(f"停止运行: {_command_hint(is_server)} stop")
    print("正在滚动显示运行日志（Ctrl+C 退出跟随，不影响实例运行）……\n")
    _follow_logs(is_server)


def cmd_log(is_server, lines):
    _follow_logs(is_server, lines)


def cmd_stop(is_server, fast, slow):
    """stop 子命令：向运行实例发送停止指令并等待退出。"""
    if is_server and (fast or slow):
        print("server 模式的 stop 不支持 --fast/--slow：行为固定为中止传输并尽快停止。")
        sys.exit(2)

    instance_name, _log_dir = _load_control_context(is_server)
    if not instance.is_running(instance_name):
        print("没有正在运行的实例。")
        return

    if is_server:
        mode = instance.STOP_FAST
        print("已发送停止指令，服务器将中止传输并尽快退出……")
    elif fast:
        mode = instance.STOP_FAST
        print("已发送停止指令 (fast)，等待实例退出（最多 8 秒，超时将强制结束）……")
    elif slow:
        mode = instance.STOP_SLOW
        print("已发送停止指令 (slow)，等待本轮任务结束后退出（Ctrl+C 放弃等待）……")
    else:
        mode = instance.STOP_GRACEFUL
        print("已发送停止指令，等待当前文件传输完成后退出（Ctrl+C 放弃等待）……")

    instance.send_stop(instance_name, mode)

    timeout = _FAST_STOP_TIMEOUT if mode == instance.STOP_FAST else None
    try:
        exited = instance.wait_for_exit(instance_name, timeout=timeout)
    except KeyboardInterrupt:
        print("\n已放弃等待；实例仍在后台执行停止流程，可用 log 查看进度。")
        return

    if exited:
        print("实例已退出。")
        return

    # fast 兜底：超时未退出 → 按进程树强杀
    pid = instance.read_pid(instance_name)
    if pid is None:
        print("Error: 实例未按时退出且 PID 文件缺失，无法强制结束。", file=sys.stderr)
        sys.exit(1)
    if instance.force_kill_tree(pid):
        print("实例未在时限内退出，已强制结束。")
    else:
        print(
            "Error: 强制结束失败：目标进程与当前程序不符，已放弃以避免误杀。",
            file=sys.stderr,
        )
        sys.exit(1)
