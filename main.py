#!/usr/bin/env python3
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
from src.utils import load_config, SingleInstance


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


def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, "config/config.toml")

    if not os.path.exists(config_path):
        print(f"Error: 配置文件 {config_path} 未找到。")
        sys.exit(1)

    config = load_config(config_path)

    max_log_files = config.get("max_log_files", 0)
    log_level = config.get("log_level", "INFO")
    logger = setup_logger(config["log_dir"], max_log_files, log_level)

    history_mgr = HistoryManager(config["log_dir"])

    AppContext.init(logger, history_mgr, config)

    schedule_config = config.get("schedule", {})
    mode = schedule_config.get("mode")

    if not mode:
        try:
            with SingleInstance(config.get("lock_file"), logger):
                run_tasks()
        except SystemExit:
            pass
        except Exception as e:
            logger.error(f"发生未知错误: {e}")
            sys.exit(1)
        finally:
            history_mgr.save()
    else:
        if mode == "cron":
            cron_expr = schedule_config.get("cron_expr")
            if not cron_expr:
                print("cron 模式需要配置 cron_expr。")
                sys.exit(1)

            run_immediately = schedule_config.get("run_immediately", False)

            logger.info(f"已设置定时任务 (cron 模式): {cron_expr}。")

            if run_immediately:
                logger.debug("设置为启动后立即执行一次任务。")
                try:
                    with SingleInstance(config.get("lock_file"), logger):
                        run_tasks()
                        history_mgr.save()
                except SystemExit:
                    logger.warning("获取锁失败，可能有其他实例在运行。")
                except Exception as e:
                    logger.error(f"发生未知错误: {e}")
                    history_mgr.save()

            while True:
                now = datetime.datetime.now()
                cron = croniter(cron_expr, now)
                next_run = cron.get_next(datetime.datetime)
                sleep_seconds = (next_run - now).total_seconds()

                logger.info(
                    f"下一次执行时间: {next_run.strftime('%Y-%m-%d %H:%M:%S')}。\n"
                )
                time.sleep(sleep_seconds)

                try:
                    with SingleInstance(config.get("lock_file"), logger):
                        run_tasks()
                        history_mgr.save()
                except SystemExit:
                    logger.warning("上次任务仍在执行，跳过本次执行。")
                except Exception as e:
                    logger.error(f"发生未知错误: {e}")
                    history_mgr.save()

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

            logger.info(
                f"已设置定时任务 (interval 模式): 每 {interval_seconds} 秒执行一次。"
            )

            first_run = True
            while True:
                if not first_run or run_immediately:
                    try:
                        with SingleInstance(config.get("lock_file"), logger):
                            run_tasks()
                            history_mgr.save()
                    except SystemExit:
                        logger.warning("获取锁失败，可能有其他实例在运行。")
                    except Exception as e:
                        logger.error(f"发生未知错误: {e}")
                        history_mgr.save()

                first_run = False

                next_run = datetime.datetime.now() + datetime.timedelta(
                    seconds=interval_seconds
                )
                logger.info(
                    f"下一次执行时间: {next_run.strftime('%Y-%m-%d %H:%M:%S')}。\n"
                )
                time.sleep(interval_seconds)
        else:
            print(f"不支持的定时模式: {mode}")
            sys.exit(1)


if __name__ == "__main__":
    main()
