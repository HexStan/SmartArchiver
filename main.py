#!/usr/bin/env python3
import os
import sys
import time
import datetime

try:
    from croniter import croniter
except ImportError:
    croniter = None

from src.core import process_directory_pair, MoverStats
from src.history import HistoryManager
from src.logger import setup_logger
from src.utils import load_config, SingleInstance


def run_tasks(config, logger, history_mgr):
    logger.info("=" * 80, raw=True)
    logger.info("脚本开始执行...")
    # 记录开始时间
    start_time = time.time()

    stats = MoverStats()
    tasks = config.get('tasks', [])

    if not tasks:
        logger.error("配置文件没有任务可以执行。")
        sys.exit(1)

    # 循环处理每个目录对
    for idx, task in enumerate(tasks):
        logger.info(f"-----开始 {len(tasks)} 个任务中的第 {idx + 1} 个-----")

        # 接收函数返回的局部 stats 对象
        process_directory_pair(task, stats, config, logger, history_mgr)

        logger.info("当前任务结束。")

    # 记录结束时间并计算耗时
    end_time = time.time()

    # 计算传输速率
    duration_str, total_size_str, speed_str = stats.calculate_speed(start_time, end_time)

    logger.info(f"所有任务执行完毕。")

    # 打印尾部统计
    tail_msg = [f"成功 {stats.success} 项"]
    if stats.conflict_skipped > 0:
        tail_msg.append(f"因重复而跳过 {stats.conflict_skipped} 项")
    if stats.locked_skipped > 0:
        tail_msg.append(f"因文件锁而跳过 {stats.locked_skipped} 项")
    if stats.kept > 0:
        tail_msg.append(f"根据规则保留 {stats.kept} 项")
    if stats.deleted > 0:
        tail_msg.append(f"根据规则删除 {stats.deleted} 项")
    logger.info("，".join(tail_msg) + "。")

    tail_msg_2 = []
    if stats.error > 0:
        tail_msg_2.append(f"失败 {stats.error} 项")
    if stats.dropped > 0:
        tail_msg_2.append(f"因多次失败而跳过 {stats.dropped} 项")
    if tail_msg_2:
        logger.info("，".join(tail_msg_2) + "。")

    logger.info(f"共计在 {duration_str} 内传输了 {total_size_str}，平均速度 {speed_str}。")

    logger.info(f"{'=' * 80}", raw=True)


def main():
    # 确定配置文件路径
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, 'config/config.toml')

    if not os.path.exists(config_path):
        print(f"Error: 配置文件 {config_path} 未找到")
        sys.exit(1)

    config = load_config(config_path)
    # 初始化日志
    # 获取最大日志数配置，默认为 0 (不清理)
    max_log_files = config.get('max_log_files', 0)
    log_level = config.get('log_level', 'INFO')
    # 初始化日志时传入 max_log_files 和 log_level
    logger = setup_logger(config['log_dir'], max_log_files, log_level)

    # 初始化历史管理器
    history_mgr = HistoryManager(config['log_dir'])

    schedule_config = config.get('schedule', {})
    mode = schedule_config.get('mode')

    if not mode:
        # 一次性执行
        try:
            with SingleInstance(config['lock_file'], logger):
                run_tasks(config, logger, history_mgr)
        except SystemExit:
            pass
        except Exception as e:
            logger.error(f"发生未知错误: {e}")
            sys.exit(1)
        finally:
            history_mgr.save()
    else:
        # 定时执行
        if mode == 'cron':
            if croniter is None:
                print("cron 模式需要 croniter 库，请执行: pip install croniter")
                sys.exit(1)
            cron_expr = schedule_config.get('cron_expr')
            if not cron_expr:
                print("cron 模式需要配置 cron_expr")
                sys.exit(1)
            
            logger.info(f"已设置定时任务 (cron 模式): {cron_expr}。")
            
            while True:
                now = datetime.datetime.now()
                cron = croniter(cron_expr, now)
                next_run = cron.get_next(datetime.datetime)
                sleep_seconds = (next_run - now).total_seconds()
                
                logger.info(f"下一次执行时间: {next_run.strftime('%Y-%m-%d %H:%M:%S')}。\n")
                time.sleep(sleep_seconds)
                
                try:
                    # 尝试获取锁，如果获取失败说明上次任务还在执行，跳过本次
                    with SingleInstance(config['lock_file'], logger):
                        run_tasks(config, logger, history_mgr)
                        history_mgr.save()
                except SystemExit:
                    logger.warning("上次任务仍在执行，跳过本次执行。")
                except Exception as e:
                    logger.error(f"发生未知错误: {e}")
                    history_mgr.save()

        elif mode == 'interval':
            interval_seconds = schedule_config.get('interval_seconds')
            if not interval_seconds or not isinstance(interval_seconds, (int, float)) or interval_seconds <= 0:
                print("interval 模式需要配置有效的 interval_seconds (大于0的数字)")
                sys.exit(1)
                
            logger.info(f"已设置定时任务 (interval 模式): 每 {interval_seconds} 秒执行一次。")
            
            while True:
                try:
                    with SingleInstance(config['lock_file'], logger):
                        run_tasks(config, logger, history_mgr)
                        history_mgr.save()
                except SystemExit:
                    logger.warning("获取锁失败，可能有其他实例在运行。")
                except Exception as e:
                    logger.error(f"发生未知错误: {e}")
                    history_mgr.save()
                
                next_run = datetime.datetime.now() + datetime.timedelta(seconds=interval_seconds)
                logger.info(f"下一次执行时间: {next_run.strftime('%Y-%m-%d %H:%M:%S')}。\n")
                time.sleep(interval_seconds)
        else:
            print(f"不支持的定时模式: {mode}")
            sys.exit(1)


if __name__ == "__main__":
    main()
