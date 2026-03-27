import os
import shutil
import subprocess
import time
from datetime import datetime
from enum import Enum

from humanfriendly import format_size, format_timespan

from src.utils import parse_size_string, is_file_locked


# 定义操作枚举，提高代码可读性
class FileAction(Enum):
    TRANSFER = "transfer"  # 正常移动
    DELETE = "delete"  # 命中规则，且配置为删除
    SKIP = "skip"  # 命中规则（配置为保留）或隐藏文件


class MoverStats:
    def __init__(self):
        self.success = 0
        self.error = 0
        self.dropped = 0
        self.kept = 0
        self.deleted = 0
        self.conflict_skipped = 0
        self.locked_skipped = 0
        self.total_bytes = 0

    def calculate_speed(self, start_time, end_time):
        """
        计算耗时、总大小和传输速度
        返回: (duration_str, total_size_str, speed_str)
        """
        duration = end_time - start_time

        # 防止除以零（如果执行极快）
        if duration < 0.001:
            duration = 0.001

        # 计算速度 (Bytes per second)
        speed_bps = self.total_bytes / duration

        total_size_str = format_size(self.total_bytes, binary=True)
        speed_str = format_size(speed_bps, binary=True) + "/s"
        duration_str = format_timespan(duration)

        return duration_str, total_size_str, speed_str


class FileFilterPolicy:
    """
    负责解析过滤规则并决定文件/目录的处理方式
    """

    class _RuleSet:

        def __init__(self, rules_config):
            """内部辅助类，用于解析一组大小/名称匹配规则"""
            self.files_rules = self._parse_rules(rules_config.get('files', {}))
            self.dirs_rules = self._parse_rules(rules_config.get('dirs', {}))

        def _parse_rules(self, raw_rules):
            parsed = []
            for pattern, size_str in raw_rules.items():
                parsed.append({
                    'pattern': pattern.lower(),
                    'threshold': parse_size_string(size_str)
                })
            return parsed

        def _match_pattern(self, name, pattern):
            name = name.lower()
            if pattern == "*":
                return True
            
            if pattern.startswith("*") and pattern.endswith("*") and len(pattern) >= 2:
                return pattern[1:-1] in name
            elif pattern.startswith("*"):
                return name.endswith(pattern[1:])
            elif pattern.endswith("*"):
                return name.startswith(pattern[:-1])
            else:
                return name == pattern

        def matches(self, name, size_or_func, is_dir=False):
            """判断文件或目录是否命中该组规则"""
            rules = self.dirs_rules if is_dir else self.files_rules
            for rule in rules:
                if self._match_pattern(name, rule['pattern']):
                    if is_dir:
                        return True
                    size = size_or_func() if callable(size_or_func) else size_or_func
                    if size < rule['threshold']:
                        return True
            return False

    def __init__(self, config):
        # 分别加载 删除规则 和 保留规则
        self.delete_rules = self._RuleSet(config.get('delete_rules', {}))
        self.keep_rules = self._RuleSet(config.get('keep_rules', {}))

    def decide(self, name, size_or_func, is_dir=False):
        """
        根据名称和大小，返回 FileAction 决策
        优先级：删除规则 > 保留规则 > 正常传输
        """

        # 1. 检查是否命中删除规则
        if self.delete_rules.matches(name, size_or_func, is_dir):
            return FileAction.DELETE

        # 2. 检查是否命中保留规则 (Skip)
        if self.keep_rules.matches(name, size_or_func, is_dir):
            return FileAction.SKIP

        # 3. 都不命中，正常传输
        return FileAction.TRANSFER


def get_unique_dest(dest_path):
    """
    如果目标文件存在，生成一个带编号的新路径
    例如: /path/file.txt -> /path/file-1.txt
    """
    if not os.path.exists(dest_path):
        return dest_path

    directory = os.path.dirname(dest_path)
    filename = os.path.basename(dest_path)
    name, ext = os.path.splitext(filename)

    counter = 1
    while True:
        new_filename = f"{name}-{counter}{ext}"
        new_path = os.path.join(directory, new_filename)
        if not os.path.exists(new_path):
            return new_path
        counter += 1


def handle_sync_mode(task, config, logger, source_root, dest_root):
    """
    处理 sync 模式：使用 rsync 或 rclone 镜像同步目录
    """
    logger.info(f" - 源路径: {source_root}")
    logger.info(f" - 目标路径: {dest_root}")
    logger.info(f" - 任务模式: 同步")

    if not os.path.exists(source_root):
        logger.error(f"源目录不存在: {source_root}")
        return

    if not os.path.isdir(dest_root):
        logger.error(f"!!! CRUCIAL: 目标目录不存在 !!!")
        return

    is_windows = os.name == 'nt'

    exclude_list = task.get('exclude', [])
    if isinstance(exclude_list, str):
        exclude_list = [exclude_list]

    backup_enabled = task.get('create_backups', False)
    max_backups = task.get('max_backups', 0)

    backup_dir = None
    if backup_enabled:
        backup_base = os.path.join(dest_root, '.smart-archiver.backups')
        timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        backup_dir = os.path.join(backup_base, timestamp)

        # 确保备份目录本身被排除，防止无限递归或被删除
        exclude_list.append('.smart-archiver.backups/')

        # 清理旧备份
        if max_backups > 0 and os.path.exists(backup_base):
            try:
                backups = [os.path.join(backup_base, d) for d in os.listdir(backup_base) if
                           os.path.isdir(os.path.join(backup_base, d))]
                backups.sort()
                # 如果当前备份数已经达到或超过最大限制，删除最旧的，为新备份腾出空间
                if len(backups) >= max_backups:
                    num_to_delete = len(backups) - max_backups + 1
                    for b in backups[:num_to_delete]:
                        shutil.rmtree(b)
                        logger.debug(f"已删除旧备份: {b}")
            except Exception as e:
                logger.error(f"清理旧备份失败: {e}")

    if is_windows:
        # Windows 平台使用 rclone
        if not shutil.which('rclone'):
            logger.error("未找到 rclone，无法执行 sync 模式，跳过该任务。")
            return

        cmd = ['rclone', 'sync', source_root, dest_root]
        for ex in exclude_list:
            cmd.extend(['--exclude', ex])
        if backup_dir:
            cmd.extend(['--backup-dir', backup_dir])

        logger.info("将使用 rclone 进行同步。")
        try:
            # 记录开始时间
            start_time = time.time()

            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                                       encoding='utf-8', errors='replace')
            for line in process.stdout:
                logger.info(line.strip())
            process.wait()
            if process.returncode != 0:
                logger.error(f"rclone 同步失败，退出码: {process.returncode}")
            else:
                # 计算耗时
                end_time = time.time()
                exec_time = end_time - start_time
                logger.success(f"rclone 同步完成，耗时: {format_timespan(exec_time)}。")
        except Exception as e:
            logger.error(f"执行 rclone 时发生错误: {e}")

    else:
        # 非 Windows 平台使用 rsync
        if not shutil.which('rsync'):
            logger.error("未找到 rsync，无法执行 sync 模式，跳过该任务。")
            return

        # rsync 同步目录内容需要在源路径后加斜杠
        src = source_root if source_root.endswith('/') else source_root + '/'
        cmd = ['rsync', '-av', '--delete', src, dest_root]
        for ex in exclude_list:
            cmd.extend(['--exclude', ex])
        if backup_dir:
            cmd.extend(['--backup', f'--backup-dir={backup_dir}'])

        logger.info("将使用 rsync 进行同步。")
        try:
            # 记录开始时间
            start_time = time.time()
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                                       encoding='utf-8', errors='replace')
            for line in process.stdout:
                # 每一行的行首加入 logger 中定义的时间戳样式
                logger.info(line.strip(), raw=True, prepend_timestamp=True)
            process.wait()
            if process.returncode != 0:
                logger.error(f"rsync 同步失败，退出码: {process.returncode}")
            else:
                # 计算耗时
                end_time = time.time()
                exec_time = end_time - start_time
                logger.success(f"rsync 同步完成，耗时: {format_timespan(exec_time)}。")
        except Exception as e:
            logger.error(f"执行 rsync 时发生错误: {e}")


def process_directory_pair(task, config, logger, history_mgr):
    """
    处理单个目录对：遍历、移动文件
    """
    local_stats = MoverStats()

    source_root = task.get('source')
    dest_root = task.get('dest')
    task_mode = task.get('mode', '').lower()

    # 检查必填配置项
    if task_mode == 'sync':
        required_fields = ['mode']
    else:
        required_fields = ['mode', 'min_age_minutes', 'conflict_policy', 'remove_empty_dirs']

    missing_fields = [field for field in required_fields if field not in task]
    if missing_fields:
        logger.error(f"任务配置缺少必填项: {', '.join(missing_fields)}，跳过该任务。")
        return

    if task_mode == 'sync':
        handle_sync_mode(task, config, logger, source_root, dest_root)
        return

    min_age_minutes = task['min_age_minutes']

    max_retries = config.get('max_retries', 3)

    conflict_policy = task['conflict_policy'].lower()
    remove_empty_dirs = task['remove_empty_dirs']

    now = time.time()
    age_threshold_seconds = min_age_minutes * 60

    # 打印任务头部信息
    logger.info(f" - 源路径: {source_root}")
    logger.info(f" - 目标路径: {dest_root}")
    logger.info(f" - 时间阈值: {format_timespan(age_threshold_seconds)}")
    logger.info(f" - 任务模式: {'复制' if task_mode == 'copy' else '移动'}")

    if not os.path.exists(source_root):
        logger.error(f"源目录不存在: {source_root}")
        return

    if not os.path.isdir(dest_root):
        logger.error(f"!!! CRUCIAL: 目标目录不存在 !!!")
        return

    task_delete_rules = task.get('delete_rules', {})
    task_keep_rules = task.get('keep_rules', {})

    merged_config = {
        'delete_rules': task_delete_rules,
        'keep_rules': task_keep_rules
    }

    # 使用合并后的配置初始化策略
    policy = FileFilterPolicy(merged_config)

    # 记录开始时间
    start_time = time.time()

    # 遍历文件
    for root, dirs, files in os.walk(source_root):
        # 处理目录规则
        dirs_to_keep = []
        for d in dirs:
            dir_path = os.path.join(root, d)
            rel_path = os.path.relpath(dir_path, source_root)

            action = policy.decide(d, 0, is_dir=True)

            if action == FileAction.DELETE:
                # 检查是否需要跳过 (多次失败)
                should_skip, fail_count = history_mgr.should_skip(dir_path, max_retries)
                if should_skip:
                    local_stats.dropped += 1
                    logger.warning(f"跳过目录 (多次失败): {dir_path}")
                else:
                    perform_delete_dir(dir_path, source_root, logger, local_stats, history_mgr)
            elif action == FileAction.SKIP:
                local_stats.kept += 1
                logger.debug(f"保留目录 (匹配规则): {rel_path}")
            elif action == FileAction.TRANSFER:
                dirs_to_keep.append(d)

        dirs[:] = dirs_to_keep

        for file in files:
            src_path = os.path.join(root, file)
            rel_path = os.path.relpath(src_path, source_root)

            # 检查是否需要跳过
            should_skip, fail_count = history_mgr.should_skip(src_path, max_retries)
            if should_skip:
                # 简单处理，视为跳过
                local_stats.dropped += 1
                logger.warning(f"跳过 (多次失败): {src_path}")
                continue

            try:
                file_stat = os.stat(src_path)
                mtime = file_stat.st_mtime
                size = file_stat.st_size
            except OSError:
                continue

            # 所有操作（包括删除垃圾文件）都必须满足时间阈值
            if (now - mtime) <= age_threshold_seconds:
                continue

            if is_file_locked(src_path):
                local_stats.locked_skipped += 1
                logger.warning(f"跳过 (被锁定): {rel_path}")
                continue

            # 核心决策逻辑
            action = policy.decide(file, size, is_dir=False)

            # 执行动作
            if action == FileAction.TRANSFER:
                move_file(src_path, size, source_root, dest_root, logger, local_stats, history_mgr, conflict_policy,
                          task_mode)
            elif action == FileAction.DELETE:
                perform_delete(src_path, size, source_root, logger, local_stats, history_mgr)
            elif action == FileAction.SKIP:
                local_stats.kept += 1
                logger.debug(f"保留 (匹配规则): {rel_path}  ({format_size(size, binary=True)})")
                pass

    # 清理空目录 (对应 find -depth -empty -type d rmdir)
    if remove_empty_dirs and task_mode != 'copy':
        clean_empty_dirs(source_root, logger, policy)

    # 记录结束时间
    end_time = time.time()

    # 计算传输速率
    duration_str, total_size_str, speed_str = local_stats.calculate_speed(start_time, end_time)

    # 打印任务尾部统计
    tail_msg = [f"成功 {local_stats.success} 项"]
    if local_stats.conflict_skipped > 0:
        tail_msg.append(f"因重复而跳过 {local_stats.conflict_skipped} 项")
    if local_stats.locked_skipped > 0:
        tail_msg.append(f"因文件锁而跳过 {local_stats.locked_skipped} 项")
    if local_stats.kept > 0:
        tail_msg.append(f"根据规则保留 {local_stats.kept} 项")
    if local_stats.deleted > 0:
        tail_msg.append(f"根据规则删除 {local_stats.deleted} 项")
    logger.info("，".join(tail_msg) + "。")

    tail_msg_2 = []
    if local_stats.error > 0:
        tail_msg_2.append(f"失败 {local_stats.error} 项")
    if local_stats.dropped > 0:
        tail_msg_2.append(f"因多次失败而跳过 {local_stats.dropped} 项")
    if tail_msg_2:
        logger.info("，".join(tail_msg_2) + "。")

    if local_stats.success > 0:
        logger.info(f"在 {duration_str} 内传输了 {total_size_str}，平均速度 {speed_str}。")


def perform_delete_dir(dir_path, source_root, logger, stats, history_mgr):
    rel_path = os.path.relpath(dir_path, source_root)
    """删除目录逻辑"""
    try:
        shutil.rmtree(dir_path)
        logger.success(f"删除目录: {rel_path}")
        history_mgr.record_success(dir_path)
        stats.deleted += 1
    except OSError as e:
        count = history_mgr.record_failure(dir_path)
        logger.error(f"删除目录失败 ({count} 次): {rel_path}\n"
                     f"Error: {e}")


def perform_delete(src_path, file_size, source_root, logger, stats, history_mgr):
    rel_path = os.path.relpath(src_path, source_root)
    """删除逻辑"""
    try:
        os.remove(src_path)
        logger.success(f"删除: {rel_path}  ({format_size(file_size, binary=True)})")
        history_mgr.record_success(src_path)
        stats.deleted += 1
    except OSError as e:
        count = history_mgr.record_failure(src_path)
        logger.error(f"删除失败 ({count} 次): {rel_path}\n"
                     f"Error: {e}")


def move_file(src_path, file_size, source_root, dest_root, logger, stats, history_mgr, conflict_policy, task_mode):
    """
    执行具体的移动或复制操作
    """
    # 计算相对路径
    rel_path = os.path.relpath(src_path, source_root)
    dest_path = os.path.join(dest_root, rel_path)
    dest_dir = os.path.dirname(dest_path)

    try:
        # 创建目标目录
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir, exist_ok=True)

        file_exists = os.path.exists(dest_path)
        new_dest_path = dest_path

        if file_exists:
            if conflict_policy == 'skip':
                stats.conflict_skipped += 1
                logger.debug(f"跳过 (重复): {rel_path}")
                # 不记录到 success，也不报错
                return

            elif conflict_policy == 'copy':
                # 计算新的不冲突的路径
                new_dest_path = get_unique_dest(dest_path)

            elif conflict_policy == 'overwrite':
                # 默认逻辑：删除目标
                os.remove(dest_path)

            else:
                # 未知策略默认跳过
                return

        # 使用 shutil.move 或 shutil.copy2
        if task_mode == 'copy':
            shutil.copy2(src_path, new_dest_path)
        else:
            # 同一文件系统下为原子重命名，跨文件系统自动降级为复制后删除
            shutil.move(src_path, new_dest_path)

        # 统计传输流量
        stats.total_bytes += file_size

        # 成功后清理记录
        history_mgr.record_success(src_path)

        size_str = format_size(file_size, binary=True)
        action_str = "复制" if task_mode == 'copy' else "移动"
        if file_exists and conflict_policy == 'overwrite':
            logger.success(f"覆盖同名文件: {rel_path} ({size_str})")
        elif file_exists and conflict_policy == 'copy':
            new_rel = os.path.relpath(new_dest_path, dest_root)
            logger.success(f"目标存在，创建副本: {new_rel} ({size_str})")
        else:
            logger.success(f"{action_str}: {rel_path} ({size_str})")

        stats.success += 1

    except Exception as e:
        # 失败时记录
        count = history_mgr.record_failure(src_path)
        action_str = "复制" if task_mode == 'copy' else "移动"
        logger.error(f"{action_str}失败 ({count} 次): {rel_path}\n{e}")
        stats.error += 1


def clean_empty_dirs(source_root, logger, policy=None):
    """
    递归深度优先删除空目录
    """
    # topdown=False 确保自底向上遍历 (对应 -depth)
    for root, dirs, files in os.walk(source_root, topdown=False):
        if root == source_root:
            continue
            
        if policy:
            # 检查该目录或其任何父目录是否命中 keep_rules
            rel_path = os.path.relpath(root, source_root)
            parts = rel_path.split(os.sep)
            should_skip = False
            for part in parts:
                if policy.decide(part, 0, is_dir=True) == FileAction.SKIP:
                    should_skip = True
                    break
            if should_skip:
                continue

        try:
            # os.rmdir 只有在目录为空时才成功，如果不为空会抛异常（我们捕获并忽略）
            os.rmdir(root)
            rel_dir = os.path.relpath(root, source_root)
            logger.debug(f"删除空目录: {rel_dir}")
            # stats.success += 1
        except OSError:
            pass  # 目录非空，跳过
