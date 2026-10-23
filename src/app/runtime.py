"""运行路径：初始化装配、任务执行与调度循环。

前台运行与 start 拉起的分离式实例最终都进入 run_client()（无
[schedule] 时一次性运行，否则按 cron/interval 常驻）或
run_server()（HTTP 远端 API）。
"""

import datetime
import os
import sys
import time
from collections.abc import Callable
from typing import Any

from croniter import croniter

from src import fs
from src.app import instance, paths
from src.app.config import ConfigManager
from src.app.context import AppContext
from src.app.instance import SingleInstance
from src.app.shutdown import install_abort_check, install_sigterm_fast_stop
from src.core import process_task
from src.core.dependencies import prepare_run_plan
from src.core.history import HistoryManager
from src.core.types import TaskOutcome
from src.output.logger import Logger, setup_logger
from src.output.presentation import fmt_timespan


def _reload_config_if_changed(manager: ConfigManager) -> None:
    cfg = manager.reload()
    if cfg is None:
        return

    ctx = AppContext.get()
    if cfg.used_backup:
        ctx.logger.warning("新配置加载失败，已回退到备份配置。")

    ctx.update_config(cfg.config, cfg.remote_clients, cfg.ssh_remotes)


def run_tasks() -> None:
    ctx = AppContext.get()
    coordinator = ctx.shutdown
    ctx.logger.info("=" * 80, raw=True)
    ctx.logger.info("脚本开始执行……")
    start_time = time.time()

    tasks = ctx.config.get("tasks", [])

    if not tasks:
        ctx.logger.error("配置文件没有任务可以执行。")
        sys.exit(1)

    # 解析期：id 规范化与依赖静态校验（错误日志在此给出）；运行期只做
    # 依赖满足判定。finished 每轮重置——依赖语义为"同一轮内"，与
    # cron/interval 每轮全量重跑的调度语义一致。
    specs = prepare_run_plan(tasks, ctx.logger)
    finished: set[str] = set()

    for idx, task in enumerate(tasks):
        if coordinator.should_stop_before_task():
            break

        spec = specs[idx]

        if spec.blocked:
            # 具体错误日志已在解析期给出，此处静默跳过
            continue

        unmet = [dep for dep in spec.depends_on if dep not in finished]
        if unmet:
            ctx.logger.error(f"前置任务 {'、'.join(unmet)} 未完成，跳过该任务。")
            continue

        ctx.logger.info(f"-----开始 {len(tasks)} 个任务中的第 {idx + 1} 个-----")

        outcome = process_task(task, now=start_time)

        ctx.logger.info("当前任务结束。")

        if spec.task_id and outcome is TaskOutcome.COMPLETED:
            finished.add(spec.task_id)

    end_time = time.time()
    exec_time = end_time - start_time
    if not coordinator.stop_requested():
        ctx.logger.info(f"所有任务执行完毕，总耗时: {fmt_timespan(exec_time)}。")

    ctx.logger.info(f"{'=' * 80}", raw=True)


def _execute_tasks(manager: ConfigManager) -> None:
    """执行一轮任务，支持配置热重载。"""
    try:
        _reload_config_if_changed(manager)
        ctx = AppContext.get()
        run_tasks()
        ctx.history_mgr.save()
    except fs.TransferAbortedError:
        # stop --fast：任务被丢弃，不做完成/失败定性，仅落盘 history；
        # 停止确认与退出日志由指令分发点与 run_client 的 finally 负责
        AppContext.get().history_mgr.save()
    except Exception as e:  # noqa: BLE001
        ctx = AppContext.get()
        ctx.logger.error(f"发生未知错误: {e}")
        ctx.history_mgr.save()


def _load_and_init(
    config_path: str, is_server: bool = False, console: bool | None = None
) -> ConfigManager:
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


def _interruptible_sleep(seconds: float) -> bool:
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


def _run_schedule(manager: ConfigManager) -> None:
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


def _run_first_round(manager: ConfigManager, schedule_config: dict[str, Any]) -> bool:
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


def _run_cron(manager: ConfigManager, schedule_config: dict[str, Any]) -> None:
    ctx = AppContext.get()
    cron_expr = schedule_config.get("cron_expr")
    if not cron_expr:
        print("cron 模式需要配置 cron_expr。")
        sys.exit(1)

    ctx.logger.info(f"已设置定时任务 (cron 模式): {cron_expr}。")

    if not _run_first_round(manager, schedule_config):
        return

    while True:
        # 停止指令可能在上一轮任务执行期间到达：直接退出，不再预告下一轮
        if ctx.shutdown.stop_requested():
            return
        now = datetime.datetime.now()
        cron = croniter(cron_expr, now)
        next_run = cron.get_next(datetime.datetime)
        sleep_seconds = (next_run - now).total_seconds()

        ctx.logger.info(f"下一次执行时间: {next_run.strftime('%Y-%m-%d %H:%M:%S')}。\n")
        if _interruptible_sleep(sleep_seconds):
            return

        _execute_tasks(manager)


def _run_interval(manager: ConfigManager, schedule_config: dict[str, Any]) -> None:
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
        # 停止指令可能在上一轮任务执行期间到达：直接退出，不再预告下一轮
        if ctx.shutdown.stop_requested():
            return
        next_run = datetime.datetime.now() + datetime.timedelta(seconds=interval_seconds)
        ctx.logger.info(f"下一次执行时间: {next_run.strftime('%Y-%m-%d %H:%M:%S')}。\n")
        if _interruptible_sleep(interval_seconds):
            return

        _execute_tasks(manager)


def _log_stop_received(logger: Logger, mode: str) -> None:
    """停止指令接收的唯一确认日志（停止序列的第一条）。

    三种模式共用一套句式，不区分日志级别；graceful 即 stop 默认的
    中速停止。
    """
    if mode == instance.STOP_FAST:
        logger.info("收到快速停止指令，正在退出……")
    elif mode == instance.STOP_SLOW:
        logger.info("收到慢速停止指令，正在退出……")
    else:
        logger.info("收到中速停止指令，正在退出……")


def _make_stop_dispatcher(ctx: AppContext) -> Callable[[str], None]:
    """把停止模式映射为协调器请求；控制文件监视线程与 SIGTERM 共用。"""
    coordinator = ctx.shutdown
    logger = ctx.logger

    def _dispatch(mode: str) -> None:
        _log_stop_received(logger, mode)
        if mode == instance.STOP_FAST:
            coordinator.request_abort(logger)
        elif mode == instance.STOP_SLOW:
            coordinator.request_stop_after_round()
        else:
            coordinator.request_stop_after_file()

    return _dispatch


def run_client() -> None:
    config_path = paths.config_path(is_server=False)
    manager = _load_and_init(config_path)

    ctx = AppContext.get()
    install_abort_check(ctx.shutdown)
    dispatch = _make_stop_dispatcher(ctx)
    # 容器/系统停服的 SIGTERM 按 stop --fast 处理：处理器只派生线程，
    # 真正的停止动作与控制文件指令走同一条分发路径
    install_sigterm_fast_stop(lambda: dispatch(instance.STOP_FAST), ctx.logger)

    try:
        # 实例锁覆盖整个进程生命周期：运行中与调度睡眠期都属于"实例存活"，
        # stop 子命令靠锁探测判断目标是否存在
        with SingleInstance(ctx.instance_name, ctx.logger):
            instance.ControlWatcher(ctx.instance_name, dispatch).start()
            _run_schedule(manager)
    except fs.TransferAbortedError:
        # stop --fast：一次性运行被中止即退出，无需额外处理
        pass
    finally:
        ctx.history_mgr.save()
        ctx.logger.info("Smart Archiver 已退出。")


def _make_server_stop_watcher(
    ctx: AppContext,
) -> Callable[[str, Callable[[str], None]], None]:
    """构造供 remote.server 注入的停止指令装配器。

    控制文件监视线程与 SIGTERM 两个指令来源都收敛在 app 层装配；
    接收确认日志也在此统一输出，remote 层不感知停止文案。
    """

    def start(name: str, on_command: Callable[[str], None]) -> None:
        def _dispatch(mode: str) -> None:
            _log_stop_received(ctx.logger, mode)
            on_command(mode)

        instance.ControlWatcher(name, _dispatch).start()
        install_sigterm_fast_stop(lambda: _dispatch(instance.STOP_FAST), ctx.logger)

    return start


def run_server() -> None:
    config_path = paths.config_path(is_server=True)
    _load_and_init(config_path, is_server=True)

    ctx = AppContext.get()
    install_abort_check(ctx.shutdown)

    from src.remote.server import run_server as start_server

    try:
        with SingleInstance(ctx.instance_name, ctx.logger):
            ctx.logger.info("以服务器模式启动，请从客户端连接。")
            start_server(
                ctx.config,
                ctx.logger,
                ctx.shutdown,
                ctx.instance_name,
                # app 域能力注入:remote 层不感知实例控制与信号装配的实现
                stop_watcher=_make_server_stop_watcher(ctx),
                runtime_paths=instance.instance_paths,
            )
    finally:
        # 正常的 stop 走 _on_command 的 os._exit 并自带退出日志，
        # 此 finally 只覆盖异常退出路径
        ctx.logger.info("Smart Archiver 已退出。")
