import os
import shutil
import time
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

    def merge(self, other):
        self.success += other.success
        self.error += other.error
        self.dropped += other.dropped
        self.kept += other.kept
        self.deleted += other.deleted
        self.conflict_skipped += other.conflict_skipped
        self.locked_skipped += other.locked_skipped
        self.total_bytes += other.total_bytes

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
    负责解析过滤规则并决定文件的处理方式
    """

    class _RuleSet:

        def __init__(self, rules_config):
            """内部辅助类，用于解析一组大小/后缀匹配规则"""

            # 解析全局大小阈值 (默认为 0，即不启用全局过滤)
            # 大于此大小的不命中规则，小于此大小的命中规则
            self.global_threshold = parse_size_string(rules_config.get('global_min_size', "0B"))

            # 解析特定扩展名规则: {"ext": "size_limit"} -> {"ext": int_bytes}
            self.ext_rules = {}
            raw_extensions = rules_config.get('extensions', {})
            for ext, size_str in raw_extensions.items():
                self.ext_rules[ext.lower()] = parse_size_string(size_str)

        def matches(self, filename, file_size):
            """判断文件是否命中该组规则"""
            # 优先级 1: 如果全局阈值被设为无限大 (ALL)，默认用户期望命中所有文件，无视特定后缀规则
            if self.global_threshold == float('inf'):
                return True

            # 优先级 2: 检查是否存在针对该后缀的特定规则
            _, ext = os.path.splitext(filename)
            ext = ext.lower()

            if ext in self.ext_rules:
                threshold = self.ext_rules[ext]
                # 如果定义了特定规则，仅当文件小于该特定阈值时命中
                return file_size < threshold

            # 优先级 3: 如果没有特定规则，使用全局默认规则
            # 如果文件小于全局定义的最小尺寸，则命中
            # 如果定义了规则但文件大于阈值（例如 .log 是 20MB，限制是 10MB），
            # 这种情况下它被视为“有效文件”，不应该再掉落到全局规则去判断。
            # 所以这里不做 else 处理，matched_rule 保持 False。
            if file_size < self.global_threshold:
                return True

            return False

    def __init__(self, config):
        # 分别加载 删除规则 和 保留规则
        self.delete_rules = self._RuleSet(config.get('delete_rules', {}))
        self.keep_rules = self._RuleSet(config.get('keep_rules', {}))

    def decide(self, filename, file_size):
        """
        根据文件名和大小，返回 FileAction 决策
        优先级：删除规则 > 保留规则 > 系统隐藏文件 > 正常传输
        """

        # 1. 检查是否命中删除规则
        if self.delete_rules.matches(filename, file_size):
            return FileAction.DELETE

        # 2. 检查是否命中保留规则 (Skip)
        if self.keep_rules.matches(filename, file_size):
            return FileAction.SKIP

        # 3. 检查系统默认规则 (隐藏文件)
        # 如果没有命中上面的显式规则，且是隐藏文件，则默认跳过
        if filename.startswith('.'):
            return FileAction.SKIP

        # 4. 都不命中，正常传输
        return FileAction.TRANSFER


def merge_rule_config(base_rules, task_rules):
    """
    合并规则配置：任务级配置 深度合并/覆盖 全局配置
    """
    if not base_rules:
        base_rules = {}
    if not task_rules:
        return base_rules

    # 创建 base 的副本，避免修改全局配置
    merged = base_rules.copy()

    # 确保 extensions 字典也是副本，防止 update 污染全局对象
    merged['extensions'] = base_rules.get('extensions', {}).copy()

    # 1. 覆盖 global_min_size (如果任务有定义)
    if 'global_min_size' in task_rules:
        merged['global_min_size'] = task_rules['global_min_size']

    # 2. 合并 extensions (部分覆盖)
    if 'extensions' in task_rules:
        merged['extensions'].update(task_rules['extensions'])

    return merged


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


def process_directory_pair(task, global_stats, config, logger, history_mgr):
    """
    处理单个目录对：遍历、移动文件
    """
    local_stats = MoverStats()

    source_root = task.get('source')
    dest_root = task.get('dest')

    global_min_age = config.get('min_age_minutes', 1440)
    min_age_minutes = task.get('min_age_minutes', global_min_age)

    max_retries = config.get('max_retries', 3)

    conflict_policy = config.get('conflict_policy', 'overwrite').lower()
    remove_empty_dirs = config.get('remove_empty_dirs', True)

    now = time.time()
    age_threshold_seconds = min_age_minutes * 60

    # 打印任务头部信息
    # logger.info("==============================开始任务==============================")
    logger.info(f" - 源路径: {source_root}")
    logger.info(f" - 目标路径: {dest_root}")
    logger.info(f" - 时间阈值: {format_timespan(age_threshold_seconds)}")

    if not os.path.exists(source_root):
        logger.error(f"源目录不存在: {source_root}")
        return

    if not os.path.isdir(dest_root):
        logger.error(f"!!! CRUCIAL: 目标目录不存在 !!!")
        return

    global_delete_rules = config.get('delete_rules', {})
    global_keep_rules = config.get('keep_rules', {})

    task_delete_rules = task.get('delete_rules', {})
    task_keep_rules = task.get('keep_rules', {})

    merged_config = {
        'delete_rules': merge_rule_config(global_delete_rules, task_delete_rules),
        'keep_rules': merge_rule_config(global_keep_rules, task_keep_rules)
    }

    # 使用合并后的配置初始化策略
    policy = FileFilterPolicy(merged_config)

    # 记录开始时间
    start_time = time.time()

    # 遍历文件
    for root, dirs, files in os.walk(source_root):
        # 排除隐藏目录，防止进入遍历 (对应 -name '.*' -prune)
        dirs[:] = [d for d in dirs if not d.startswith('.')]

        for file in files:
            src_path = os.path.join(root, file)
            rel_path = os.path.relpath(src_path, source_root)

            # 检查是否需要跳过
            should_skip, fail_count = history_mgr.should_skip(src_path, max_retries)
            if should_skip:
                # 简单处理，视为跳过
                local_stats.dropped += 1
                logger.info(f"跳过 (多次失败): {src_path}")
                continue

            try:
                file_stat = os.stat(src_path)
                mtime = file_stat.st_mtime
                size = file_stat.st_size
            except OSError:
                continue

            # 检查时间阈值 (-mmin +$MIN_AGE)
            # if (now - mtime) > age_threshold_seconds:
            #     move_file(src_path, size, source_root, dest_root, logger, local_stats, history_mgr)

            # 所有操作（包括删除垃圾文件）都必须满足时间阈值
            if (now - mtime) <= age_threshold_seconds:
                continue

            if is_file_locked(src_path):
                local_stats.locked_skipped += 1
                logger.info(f"跳过 (被锁定): {rel_path}")
                continue

            # 核心决策逻辑
            action = policy.decide(file, size)

            # 执行动作
            if action == FileAction.TRANSFER:
                move_file(src_path, size, source_root, dest_root, logger, local_stats, history_mgr, conflict_policy)
            elif action == FileAction.DELETE:
                perform_delete(src_path, size, source_root, logger, local_stats, history_mgr)
            elif action == FileAction.SKIP:
                local_stats.kept += 1
                logger.debug(f"保留 (匹配规则): {rel_path}  ({format_size(size, binary=True)})")
                pass

    # 清理空目录 (对应 find -depth -empty -type d rmdir)
    if remove_empty_dirs:
        clean_empty_dirs(source_root, logger)

    # 记录结束时间
    end_time = time.time()

    # 计算传输速率
    duration_str, total_size_str, speed_str = local_stats.calculate_speed(start_time, end_time)

    # 打印任务尾部统计
    if local_stats.success > 0:
        logger.info(f"在 {duration_str} 内传输了 {total_size_str}，平均速度 {speed_str}。")
    # msg = f"成功 {local_stats.success} 项"
    # if local_stats.kept > 0:
    #     msg += f"，根据规则忽略 {local_stats.kept} 项"
    # if local_stats.only_deleted > 0:
    #     msg += f"，根据规则删除 {local_stats.only_deleted} 项"
    # if local_stats.error > 0:
    #     msg += f"，失败 {local_stats.error} 项"
    # if local_stats.skipped > 0:
    #     msg += f"，因多次失败跳过 {local_stats.skipped} 项"
    # logger.info(msg + "。")
    # logger.info("==============================任务完成==============================")

    # 将 局部统计数据 累加到 传入的全局对象 中
    global_stats.merge(local_stats)


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


def move_file(src_path, file_size, source_root, dest_root, logger, stats, history_mgr, conflict_policy):
    """
    执行具体的移动操作
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
                logger.info(f"跳过 (重复): {rel_path}")
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

        # 使用 shutil.move 直接移动
        # 同一文件系统下为原子重命名，跨文件系统自动降级为复制后删除
        shutil.move(src_path, new_dest_path)

        # 统计传输流量
        stats.total_bytes += file_size

        # 成功后清理记录
        history_mgr.record_success(src_path)

        size_str = format_size(file_size, binary=True)
        if file_exists and conflict_policy == 'overwrite':
            logger.success(f"覆盖同名文件: {rel_path} ({size_str})")
        elif file_exists and conflict_policy == 'copy':
            new_rel = os.path.relpath(new_dest_path, dest_root)
            logger.success(f"目标存在，创建副本: {new_rel} ({size_str})")
        else:
            logger.success(f"移动: {rel_path} ({size_str})")

        stats.success += 1

    except Exception as e:
        # 失败时记录
        count = history_mgr.record_failure(src_path)
        # 如果是删除失败但复制成功
        logger.error(f"移动失败 ({count} 次): {rel_path}\n{e}")
        stats.error += 1


def clean_empty_dirs(source_root, logger):
    """
    递归深度优先删除空目录
    """
    # topdown=False 确保自底向上遍历 (对应 -depth)
    for root, dirs, files in os.walk(source_root, topdown=False):
        if root == source_root:
            continue
        try:
            # os.rmdir 只有在目录为空时才成功，如果不为空会抛异常（我们捕获并忽略）
            os.rmdir(root)
            rel_dir = os.path.relpath(root, source_root)
            logger.debug(f"删除空目录: {rel_dir}")
            # stats.success += 1  # 原脚本将成功删除目录也计入日志里的 SUCCESS_LIST
        except OSError:
            pass  # 目录非空，跳过
