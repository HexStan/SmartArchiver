"""运行路径：初始化装配、任务执行与调度循环。

前台运行与 start 拉起的分离式实例最终都进入 run_client()（无
[schedule] 时一次性运行，否则按 cron/interval 常驻）或
run_server()（HTTP 远端 API）。
"""

import datetime
import os
import sys
import time

from croniter import croniter

from src import fs
from src.app import instance, paths
from src.app.config import ConfigManager
from src.app.context import AppContext
from src.app.instance import SingleInstance
from src.app.shutdown import install_abort_check
from src.core import process_task
from src.core.history import HistoryManager
from src.output.logger import setup_logger
from src.output.presentation import fmt_timespan


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
    except fs.TransferAbortedError:
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

    raw_config = ConfigManager.load_toml(config_path)

    instance_name, invalid = instance.resolve_identity(raw_config, is_server)
    if console is None:
        console = os.environ.get(instance.DETACHED_ENV) != "1"

    max_log_files = raw_config.get("max_log_files", 0)
    log_level = raw_config.get("log_level", "INFO")
    log_dir = paths.resolve_log_dir(raw_config)
    logger = setup_logger(
        log_dir,
        max_log_files,
        log_level,
        prefix=instance_name,
        console=console,
        # 桥接 fs 包与实例控制模块的 logger（组合根注入，output 层不感知业务模块）
        bridge_loggers=("src.fs", instance.__name__),
    )
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


def _run_first_round(manager, schedule_config):
    """按 first_run_delay 执行首轮任务；返回 False 表示调度应退出。

    -1（默认）不在此执行——等待至下个调度时间点的语义由调度主循环
    实现；0 表示启动后立即执行；正数表示等待指定秒数后执行首轮，
    之后的轮次仍按调度周期进行。
    """
    ctx = AppContext.get()
    first_run_delay = schedule_config.get("first_run_delay", -1)
    if first_run_delay < 0:
        return True

    ctx.logger.info(f"等待 {first_run_delay} 秒后开始执行任务。")
    if _interruptible_sleep(first_run_delay):
        return False

    _execute_tasks(manager)
    return True


def _run_cron(manager, schedule_config):
    ctx = AppContext.get()
    cron_expr = schedule_config.get("cron_expr")
    if not cron_expr:
        print("cron 模式需要配置 cron_expr。")
        sys.exit(1)

    ctx.logger.info(f"已设置定时任务 (cron 模式): {cron_expr}。")

    if not _run_first_round(manager, schedule_config):
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

    if not _run_first_round(manager, schedule_config):
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
    config_path = paths.config_path(is_server=False)
    manager = _load_and_init(config_path)

    ctx = AppContext.get()
    install_abort_check(ctx.shutdown)

    try:
        # 实例锁覆盖整个进程生命周期：运行中与调度睡眠期都属于"实例存活"，
        # stop 子命令靠锁探测判断目标是否存在
        with SingleInstance(ctx.instance_name, ctx.logger):
            _start_control_watcher(ctx)
            _run_schedule(manager)
    except fs.TransferAbortedError:
        ctx.logger.warning("任务已被中止（stop --fast），程序退出。")
    finally:
        ctx.history_mgr.save()


def run_server():
    config_path = paths.config_path(is_server=True)
    _load_and_init(config_path, is_server=True)

    ctx = AppContext.get()
    install_abort_check(ctx.shutdown)

    from src.remote.server import run_server as start_server

    with SingleInstance(ctx.instance_name, ctx.logger):
        ctx.logger.info("以服务器模式启动，请从客户端连接。")
        start_server(ctx.config, ctx.logger, ctx.shutdown, ctx.instance_name)
