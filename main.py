#!/usr/bin/env python3
import os
import sys
import time

from src.core import process_directory_pair, MoverStats
from src.history import HistoryManager
from src.logger import setup_logger
from src.utils import load_config, SingleInstance


def main():
    # 解析命令行参数
    # parser = argparse.ArgumentParser(description="文件移动与归档脚本")
    # parser.add_argument('--clean-logs', action='store_true', help='仅清理旧日志，不执行移动任务')
    # args = parser.parse_args()

    # 确定配置文件路径
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, 'config/config.toml')

    if not os.path.exists(config_path):
        print(f"Error: 配置文件 {config_path} 未找到")
        sys.exit(1)

    config = load_config(config_path)
    # 初始化日志
    # 获取最大日志数配置，默认为 0 (不清理)
    # log_dir = config.get('log_dir', '/var/log/smartarchiver')
    max_log_files = config.get('max_log_files', 0)
    # 初始化日志时传入 max_log_files
    logger = setup_logger(config['log_dir'], max_log_files)

    # 如果指定了 --clean-logs，执行清理并退出
    # if args.clean_logs:
    #     retention_days = config.get('log_retention_days', 30)  # 默认30天
    #     clean_logs(log_dir, retention_days, logger)
    #     sys.exit(0)

    # 初始化历史管理器
    history_mgr = HistoryManager(config['log_dir'])

    # 执行主任务
    try:
        with SingleInstance(config['lock_file'], logger):
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
            # for task in tasks:
            for idx, task in enumerate(tasks):
                # src = task.get('source')
                # dest = task.get('dest')
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

            logger.info(f"{'=' * 80}\n", raw=True)

    except SystemExit:
        # SingleInstance内部可能会调用sys.exit(1)，无需额外处理
        pass
    except Exception as e:
        logger.error(f"发生未捕获异常: {e}")
        sys.exit(1)
    finally:
        # 确保脚本退出前保存失败记录
        history_mgr.save()


if __name__ == "__main__":
    main()
