#!/usr/bin/env python3
"""入口：前台运行与 start / log / stop 三个控制子命令。

- 无子命令：前台运行（客户端模式读 config/config.toml，--server 读
  config.server.toml）；
- start：脱离终端运行实例，随后在本终端滚动显示运行日志（窗口可随时关闭）；
- log：滚动显示运行日志（自动跟随跨天轮转），不受运行状态影响；
- stop：停止运行中的实例（--fast 立即中止传输 / --slow 本轮结束后停止）。
"""

import argparse
import datetime
import os
import subprocess
import sys
import time

from croniter import croniter

from src import fs_ops, instance
from src.app_context import AppContext
from src.app_paths import app_base_dir, is_frozen
from src.config_manager import ConfigManager
from src.core import process_task
from src.history import HistoryManager
from src.logger import setup_logger
from src.logtail import LogFollower
from src.presentation import fmt_timespan
from src.shutdown import install_abort_check
from src.utils import SingleInstance

# start 子命令标记分离式子进程的环境变量：子进程据此关闭控制台输出
_DETACHED_ENV = "SMARTARCHIVER_DETACHED"

# stop --fast 发出指令后等待实例自行退出的时限，超时则强杀
_FAST_STOP_TIMEOUT = 8.0


def _config_path(is_server):
    name = "config/config.server.toml" if is_server else "config/config.toml"
    return os.path.join(app_base_dir(), name)


def _resolve_log_dir(raw_config):
    """解析日志目录：相对路径以应用根目录为基准，与启动时的工作目录无关。"""
    log_dir = raw_config.get("log_dir", "./logs")
    if log_dir and not os.path.isabs(log_dir):
        return os.path.normpath(os.path.join(app_base_dir(), log_dir))
    return log_dir


def _reload_config_if_changed(manager):
    cfg = manager.reload()
    if cfg is None:
        return

    ctx = AppContext.get()
    if cfg.used_backup:
        ctx.logger.warning("新配置加载失败，已回退到备份配置。")

    ctx.update_config(cfg.config, cfg.remote_clients, cfg.ssh_remotes)


def run_tasks():
    ctx = AppContext.get()
    coordinator = ctx.shutdown
    ctx.logger.info("=" * 80, raw=True)
    ctx.logger.info("脚本开始执行……")
    start_time = time.time()

    tasks = ctx.config.get("tasks", [])

    if not tasks:
        ctx.logger.error("配置文件没有任务可以执行。")
        sys.exit(1)

    for idx, task in enumerate(tasks):
        if coordinator.should_stop_before_task():
            ctx.logger.info("收到停止指令，停止执行后续任务。")
            break

        ctx.logger.info(f"-----开始 {len(tasks)} 个任务中的第 {idx + 1} 个-----")

        process_task(task, now=start_time)

        ctx.logger.info("当前任务结束。")

    end_time = time.time()
    exec_time = end_time - start_time
    if coordinator.stop_requested():
        ctx.logger.info(f"因停止指令提前结束本轮任务，总耗时: {fmt_timespan(exec_time)}。")
    else:
        ctx.logger.info(f"所有任务执行完毕，总耗时: {fmt_timespan(exec_time)}。")

    ctx.logger.info(f"{'=' * 80}", raw=True)


def _execute_tasks(manager):
    """执行一轮任务，支持配置热重载。"""
    try:
        _reload_config_if_changed(manager)
        ctx = AppContext.get()
        run_tasks()
        ctx.history_mgr.save()
    except fs_ops.TransferAbortedError:
        ctx = AppContext.get()
        ctx.logger.warning("本轮任务已被中止（stop --fast）。")
        ctx.history_mgr.save()
    except Exception as e:  # noqa: BLE001
        ctx = AppContext.get()
        ctx.logger.error(f"发生未知错误: {e}")
        ctx.history_mgr.save()


def _load_and_init(config_path, is_server=False, console=None):
    """加载配置文件并执行初始化流程。

    Args:
        console: 是否输出到控制台；None 时按环境变量判断（脱离终端
            运行的实例没有可用控制台，仅写日志文件）。
    Returns:
        ConfigManager 实例。
    """
    if not os.path.exists(config_path):
        print(f"Error: 配置文件 {config_path} 未找到。")
        sys.exit(1)

    raw_config = ConfigManager._load_toml(config_path)

    instance_name, invalid = instance.resolve_identity(raw_config, is_server)
    if console is None:
        console = os.environ.get(_DETACHED_ENV) != "1"

    max_log_files = raw_config.get("max_log_files", 0)
    log_level = raw_config.get("log_level", "INFO")
    log_dir = _resolve_log_dir(raw_config)
    logger = setup_logger(log_dir, max_log_files, log_level, prefix=instance_name, console=console)
    history_mgr = HistoryManager(log_dir)

    manager = ConfigManager(config_path, logger)
    cfg = manager.load(raw_config)

    AppContext.init(
        logger,
        history_mgr,
        cfg.config,
        cfg.remote_clients,
        cfg.ssh_remotes,
        instance_name=instance_name,
    )

    if invalid:
        logger.warning("instance_name 配置值非法，已使用默认实例标识。")
    return manager


# ============================================================
# 运行循环
# ============================================================


def _interruptible_sleep(seconds):
    """可被停止指令打断的睡眠。返回 True 表示睡眠期间收到停止指令。"""
    coordinator = AppContext.get().shutdown
    remaining = seconds
    while remaining > 0:
        if coordinator.stop_requested():
            return True
        chunk = min(remaining, 1.0)
        coordinator.wait_for_wake(chunk)
        remaining -= chunk
    return coordinator.stop_requested()


def _run_schedule(manager):
    """按 [schedule] 配置驱动任务；未配置则一次性运行。"""
    ctx = AppContext.get()
    schedule_config = ctx.config.get("schedule", {})
    mode = schedule_config.get("mode")

    if not mode:
        run_tasks()
    elif mode == "cron":
        _run_cron(manager, schedule_config)
    elif mode == "interval":
        _run_interval(manager, schedule_config)
    else:
        print(f"不支持的定时模式: {mode}")
        sys.exit(1)


def _run_cron(manager, schedule_config):
    ctx = AppContext.get()
    cron_expr = schedule_config.get("cron_expr")
    if not cron_expr:
        print("cron 模式需要配置 cron_expr。")
        sys.exit(1)

    ctx.logger.info(f"已设置定时任务 (cron 模式): {cron_expr}。")

    first_run_delay = schedule_config.get("first_run_delay", -1)
    if first_run_delay >= 0:
        ctx.logger.info(f"等待 {first_run_delay} 秒后开始执行任务。")
        if _interruptible_sleep(first_run_delay):
            return

    while True:
        now = datetime.datetime.now()
        cron = croniter(cron_expr, now)
        next_run = cron.get_next(datetime.datetime)
        sleep_seconds = (next_run - now).total_seconds()

        ctx.logger.info(f"下一次执行时间: {next_run.strftime('%Y-%m-%d %H:%M:%S')}。\n")
        if _interruptible_sleep(sleep_seconds):
            ctx.logger.info("收到停止指令，程序退出。")
            return

        _execute_tasks(manager)


def _run_interval(manager, schedule_config):
    interval_seconds = schedule_config.get("interval_seconds")
    if (
        not interval_seconds
        or not isinstance(interval_seconds, (int, float))
        or interval_seconds <= 0
    ):
        print("interval 模式需要配置有效的 interval_seconds (大于0的数字)。")
        sys.exit(1)

    ctx = AppContext.get()
    ctx.logger.info(f"已设置定时任务 (interval 模式): 每 {interval_seconds} 秒执行一次。")

    first_run_delay = schedule_config.get("first_run_delay", -1)
    if first_run_delay >= 0:
        ctx.logger.info(f"等待 {first_run_delay} 秒后开始执行任务。")
        if _interruptible_sleep(first_run_delay):
            return

    while True:
        next_run = datetime.datetime.now() + datetime.timedelta(seconds=interval_seconds)
        ctx.logger.info(f"下一次执行时间: {next_run.strftime('%Y-%m-%d %H:%M:%S')}。\n")
        if _interruptible_sleep(interval_seconds):
            ctx.logger.info("收到停止指令，程序退出。")
            return

        _execute_tasks(manager)


def _start_control_watcher(ctx):
    """启动控制文件监视线程，把 stop 指令映射到停止协调器。"""
    coordinator = ctx.shutdown
    logger = ctx.logger

    def _dispatch(mode):
        if mode == instance.STOP_FAST:
            logger.warning("收到停止指令 (fast)：中止当前传输并退出。")
            coordinator.request_abort(logger)
        elif mode == instance.STOP_SLOW:
            logger.info("收到停止指令 (slow)：本轮任务结束后退出。")
            coordinator.request_stop_after_round()
        else:
            logger.info("收到停止指令：当前文件传输完成后退出。")
            coordinator.request_stop_after_file()

    instance.ControlWatcher(ctx.instance_name, _dispatch).start()


def run_client():
    config_path = _config_path(is_server=False)
    manager = _load_and_init(config_path)

    ctx = AppContext.get()
    install_abort_check(ctx.shutdown)

    try:
        # 实例锁覆盖整个进程生命周期：运行中与调度睡眠期都属于"实例存活"，
        # stop 子命令靠锁探测判断目标是否存在
        with SingleInstance(ctx.instance_name, ctx.logger):
            _start_control_watcher(ctx)
            _run_schedule(manager)
    except fs_ops.TransferAbortedError:
        ctx.logger.warning("任务已被中止（stop --fast），程序退出。")
    finally:
        ctx.history_mgr.save()


def run_server():
    config_path = _config_path(is_server=True)
    _load_and_init(config_path, is_server=True)

    ctx = AppContext.get()
    install_abort_check(ctx.shutdown)

    from src.remote.server import run_server as start_server

    with SingleInstance(ctx.instance_name, ctx.logger):
        ctx.logger.info("以服务器模式启动，请从客户端连接。")
        start_server(ctx.config, ctx.logger, ctx.shutdown, ctx.instance_name)


# ============================================================
# 控制子命令
# ============================================================


def _load_control_context(is_server):
    """控制子命令（start/log/stop）的宽松配置读取。

    与运行路径不同，这里不做完整校验：配置缺失或损坏时回退默认
    实例标识与日志目录，保证 log/stop 在配置异常时依然可用。
    返回 (instance_name, log_dir)。
    """
    config_path = _config_path(is_server)
    raw_config = {}
    if os.path.exists(config_path):
        try:
            raw_config = ConfigManager._load_toml(config_path)
        except Exception as e:  # noqa: BLE001
            print(f"警告: 配置文件解析失败（{e}），按默认实例标识处理。")
    else:
        print(f"警告: 配置文件 {config_path} 未找到，按默认实例标识处理。")

    instance_name, invalid = instance.resolve_identity(raw_config, is_server)
    if invalid:
        print(f"警告: instance_name 配置值非法，按默认实例标识 {instance_name} 处理。")
    return instance_name, _resolve_log_dir(raw_config)


def _command_hint(is_server):
    prog = os.path.basename(sys.argv[0]) or "main.py"
    suffix = " --server" if is_server else ""
    return f"{prog}{suffix}"


def _spawn_detached(is_server):
    """以脱离终端的方式拉起自身，返回子进程 PID。"""
    if is_frozen():
        cmd = [sys.executable]
    else:
        cmd = [sys.executable, os.path.join(app_base_dir(), "main.py")]
    if is_server:
        cmd.append("--server")

    env = os.environ.copy()
    env[_DETACHED_ENV] = "1"

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
    config_path = _config_path(is_server)
    if not os.path.exists(config_path):
        print(f"Error: 配置文件 {config_path} 未找到。")
        sys.exit(1)
    try:
        ConfigManager._load_toml(config_path)
    except Exception as e:  # noqa: BLE001
        print(f"Error: 配置文件解析失败: {e}")
        sys.exit(1)

    instance_name, _log_dir = _load_control_context(is_server)
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


# ============================================================
# 命令行解析
# ============================================================


def _ensure_utf8_stdio():
    """管道/重定向场景下 stdout/stderr 默认跟随系统 ANSI 代码页，中文会直接
    UnicodeEncodeError（如 Windows 的 cp1252）；交互控制台走 WindowsConsoleIO，
    编码本就是 UTF-8，不受影响。此处将非 UTF-8 的流统一强制为 UTF-8，
    与日志文件编码保持一致。冻结产物会忽略 PYTHONUTF8/PYTHONIOENCODING
    环境变量，因此只能在入口代码处理。"""
    for stream in (sys.stdout, sys.stderr):
        if stream is None or not hasattr(stream, "reconfigure"):
            continue
        if (stream.encoding or "").lower().replace("-", "") != "utf8":
            stream.reconfigure(encoding="utf-8")


def main():
    _ensure_utf8_stdio()

    parser = argparse.ArgumentParser(
        description="SmartArchiver - 智能文件归档工具",
        epilog="子命令: start（脱离终端运行）/ log（滚动显示日志）/ stop（停止实例）",
    )
    parser.add_argument(
        "--server",
        action="store_true",
        help="以服务器模式启动，读取 config.server.toml，仅提供远程 API 服务",
    )
    subparsers = parser.add_subparsers(dest="command", metavar="子命令")

    subparsers.add_parser("start", help="脱离终端运行实例，并滚动显示运行日志")

    log_parser = subparsers.add_parser(
        "log", help="滚动显示运行日志（自动跟随跨天轮转的新日志文件）"
    )
    log_parser.add_argument(
        "-n",
        "--lines",
        type=int,
        default=100,
        metavar="N",
        help="初始显示的最近日志行数（默认 100）",
    )

    stop_parser = subparsers.add_parser("stop", help="停止正在运行的实例")
    stop_group = stop_parser.add_mutually_exclusive_group()
    stop_group.add_argument(
        "--fast",
        action="store_true",
        help="立即中止文件传输并退出（8 秒内未退出则强制结束）",
    )
    stop_group.add_argument("--slow", action="store_true", help="等待本轮任务结束后再退出")

    args = parser.parse_args()

    if args.command is None:
        if args.server:
            run_server()
        else:
            run_client()
    elif args.command == "start":
        cmd_start(args.server)
    elif args.command == "log":
        if args.lines < 0:
            parser.error("--lines 不能为负数")
        cmd_log(args.server, args.lines)
    elif args.command == "stop":
        cmd_stop(args.server, args.fast, args.slow)


if __name__ == "__main__":
    main()
