#!/usr/bin/env python3
import argparse
import datetime
import os
import sys
import time

from croniter import croniter
from src.presentation import fmt_timespan
from src.app_context import AppContext
from src.core import process_task
from src.history import HistoryManager
from src.logger import setup_logger
from src.utils import SingleInstance
from src.config_manager import ConfigManager


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
    ctx.logger.info("=" * 80, raw=True)
    ctx.logger.info("脚本开始执行……")
    start_time = time.time()

    tasks = ctx.config.get("tasks", [])

    if not tasks:
        ctx.logger.error("配置文件没有任务可以执行。")
        sys.exit(1)

    for idx, task in enumerate(tasks):
        ctx.logger.info(f"-----开始 {len(tasks)} 个任务中的第 {idx + 1} 个-----")

        process_task(task, now=start_time)

        ctx.logger.info("当前任务结束。")

    end_time = time.time()
    exec_time = end_time - start_time
    ctx.logger.info(f"所有任务执行完毕，总耗时: {fmt_timespan(exec_time)}。")

    ctx.logger.info(f"{'=' * 80}", raw=True)


def _load_and_init(config_path, logger_prefix="smartarchiver"):
    """加载配置文件并执行初始化流程。"""
    if not os.path.exists(config_path):
        print(f"Error: 配置文件 {config_path} 未找到。")
        sys.exit(1)

    raw_config = ConfigManager._load_toml(config_path)

    max_log_files = raw_config.get("max_log_files", 0)
    log_level = raw_config.get("log_level", "INFO")
    log_dir = raw_config.get("log_dir", "./logs")
    logger = setup_logger(log_dir, max_log_files, log_level, prefix=logger_prefix)
    history_mgr = HistoryManager(log_dir)

    manager = ConfigManager(config_path, logger)
    cfg = manager.load(raw_config)

    AppContext.init(
        logger, history_mgr, cfg.config, cfg.remote_clients, cfg.ssh_remotes
    )
    return manager


def run_client():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, "config/config.toml")
    manager = _load_and_init(config_path)

    ctx = AppContext.get()
    config = ctx.config

    schedule_config = config.get("schedule", {})
    mode = schedule_config.get("mode")

    if not mode:
        try:
            with SingleInstance(config.get("lock_file"), ctx.logger):
                run_tasks()
        except SystemExit:
            pass
        except Exception as e:
            ctx.logger.error(f"发生未知错误: {e}")
            sys.exit(1)
        finally:
            ctx.history_mgr.save()
    else:
        if mode == "cron":
            cron_expr = schedule_config.get("cron_expr")
            if not cron_expr:
                print("cron 模式需要配置 cron_expr。")
                sys.exit(1)

            run_immediately = schedule_config.get("run_immediately", False)

            ctx.logger.info(f"已设置定时任务 (cron 模式): {cron_expr}。")

            if run_immediately:
                ctx.logger.debug("设置为启动后立即执行一次任务。")
                try:
                    _reload_config_if_changed(manager)
                    ctx = AppContext.get()
                    with SingleInstance(ctx.config.get("lock_file"), ctx.logger):
                        run_tasks()
                        ctx.history_mgr.save()
                except SystemExit:
                    ctx.logger.warning("获取锁失败，可能有其他实例在运行。")
                except Exception as e:
                    ctx.logger.error(f"发生未知错误: {e}")
                    ctx.history_mgr.save()

            while True:
                now = datetime.datetime.now()
                cron = croniter(cron_expr, now)
                next_run = cron.get_next(datetime.datetime)
                sleep_seconds = (next_run - now).total_seconds()

                ctx = AppContext.get()
                ctx.logger.info(
                    f"下一次执行时间: {next_run.strftime('%Y-%m-%d %H:%M:%S')}。\n"
                )
                time.sleep(sleep_seconds)

                try:
                    _reload_config_if_changed(manager)
                    ctx = AppContext.get()
                    with SingleInstance(ctx.config.get("lock_file"), ctx.logger):
                        run_tasks()
                        ctx.history_mgr.save()
                except SystemExit:
                    ctx.logger.warning("上次任务仍在执行，跳过本次执行。")
                except Exception as e:
                    ctx.logger.error(f"发生未知错误: {e}")
                    ctx.history_mgr.save()

        elif mode == "interval":
            interval_seconds = schedule_config.get("interval_seconds")
            if (
                not interval_seconds
                or not isinstance(interval_seconds, (int, float))
                or interval_seconds <= 0
            ):
                print("interval 模式需要配置有效的 interval_seconds (大于0的数字)。")
                sys.exit(1)

            run_immediately = schedule_config.get("run_immediately", True)

            ctx.logger.info(
                f"已设置定时任务 (interval 模式): 每 {interval_seconds} 秒执行一次。"
            )

            first_run = True
            while True:
                if not first_run or run_immediately:
                    try:
                        _reload_config_if_changed(manager)
                        ctx = AppContext.get()
                        with SingleInstance(ctx.config.get("lock_file"), ctx.logger):
                            run_tasks()
                            ctx.history_mgr.save()
                    except SystemExit:
                        ctx.logger.warning("获取锁失败，可能有其他实例在运行。")
                    except Exception as e:
                        ctx.logger.error(f"发生未知错误: {e}")
                        ctx.history_mgr.save()

                first_run = False

                next_run = datetime.datetime.now() + datetime.timedelta(
                    seconds=interval_seconds
                )
                ctx = AppContext.get()
                ctx.logger.info(
                    f"下一次执行时间: {next_run.strftime('%Y-%m-%d %H:%M:%S')}。\n"
                )
                time.sleep(interval_seconds)
        else:
            print(f"不支持的定时模式: {mode}")
            sys.exit(1)


def run_server():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, "config/config.server.toml")
    _ = _load_and_init(config_path, logger_prefix="smartarchiver-server")

    ctx = AppContext.get()

    from src.remote.server import run_server as start_server

    ctx.logger.info("以服务器模式启动，请从客户端连接。")
    start_server(ctx.config, ctx.logger)


def main():
    parser = argparse.ArgumentParser(description="SmartArchiver - 智能文件归档工具")
    parser.add_argument(
        "--server",
        action="store_true",
        help="以服务器模式启动，读取 config.server.toml，仅提供远程 API 服务",
    )
    args = parser.parse_args()

    if args.server:
        run_server()
    else:
        run_client()


if __name__ == "__main__":
    main()
