import os
import shutil
import time
import subprocess
from datetime import datetime
from enum import Enum

from humanfriendly import format_size, format_timespan

from src.utils import parse_size_string, is_file_locked


def match_pattern(name, pattern):
    """
    匹配模式：
    - "example": 完全匹配
    - "*example*": 部分连续匹配
    - "example*": 前缀匹配
    - "*example": 后缀匹配
    - "*": 匹配所有
    大小写不敏感
    """
    name = name.lower()
    pattern = pattern.lower()

    if pattern == "*":
        return True
    elif pattern.startswith("*") and pattern.endswith("*") and len(pattern) >= 2:
        return pattern[1:-1] in name
    elif pattern.startswith("*"):
        return name.endswith(pattern[1:])
    elif pattern.endswith("*"):
        return name.startswith(pattern[:-1])
    else:
        return name == pattern


# 定义操作枚举，提高代码可读性
class FileAction(Enum):
    TRANSFER = "transfer"  # 正常移动
    DELETE = "delete"  # 命中规则，且配置为删除
    SKIP = "skip"  # 命中规则（配置为保留）


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
    负责解析过滤规则并决定文件或目录的处理方式
    """

    class _RuleSet:
        def __init__(self, rules_config):
            """内部辅助类，用于解析目录和文件的匹配规则"""

            self.dir_rules_lt = {}
            self.dir_rules_ge = {}
            raw_dirs = rules_config.get("dirs", {})
            self._parse_rules(raw_dirs, self.dir_rules_lt, self.dir_rules_ge)

            self.file_rules_lt = {}
            self.file_rules_ge = {}
            raw_files = rules_config.get("files", {})
            self._parse_rules(raw_files, self.file_rules_lt, self.file_rules_ge)

        def _parse_rules(self, raw_rules, lt_dict, ge_dict):
            for pattern, size_str in raw_rules.get("lt", {}).items():
                lt_dict[pattern] = parse_size_string(size_str)
            for pattern, size_str in raw_rules.get("ge", {}).items():
                ge_dict[pattern] = parse_size_string(size_str)

        def matches(self, name, size_or_callable, is_dir=False):
            """
            判断文件或目录是否命中该组规则。
            支持惰性求值：如果命中 ge 且阈值为 0，则不调用 size_or_callable 获取大小。
            """
            rules_lt = self.dir_rules_lt if is_dir else self.file_rules_lt
            rules_ge = self.dir_rules_ge if is_dir else self.file_rules_ge

            matching_thresholds_lt = []
            matching_thresholds_ge = []

            for pattern, threshold in rules_lt.items():
                if match_pattern(name, pattern):
                    matching_thresholds_lt.append(threshold)

            for pattern, threshold in rules_ge.items():
                if match_pattern(name, pattern):
                    # 如果 ge 规则阈值为 0，必然命中，无需检查大小
                    if threshold == 0:
                        return True
                    matching_thresholds_ge.append(threshold)

            # 只有在没有命中 ge 0 且命中了其他有大小限制的规则时，才获取并检查大小
            if matching_thresholds_lt or matching_thresholds_ge:
                size = (
                    size_or_callable()
                    if callable(size_or_callable)
                    else size_or_callable
                )
                for threshold in matching_thresholds_lt:
                    if size < threshold:
                        return True
                for threshold in matching_thresholds_ge:
                    if size >= threshold:
                        return True

            return False

    def __init__(self, config):
        self.is_whitelist_mode = config.get("is_whitelist_mode", False)
        self.keep_rules = self._RuleSet(config.get("keep_rules", {}))
        self.delete_rules = self._RuleSet(config.get("delete_rules", {}))
        self.preferred_rule = config.get("preferred_rule", "keep")
        if self.is_whitelist_mode:
            self.whitelist_rules = self._RuleSet(config.get("whitelist_rules", {}))

    def decide(self, name, size_or_callable, is_dir=False):
        """
        根据名称和大小，返回 FileAction 决策。
        size_or_callable 可以是一个数值，也可以是一个返回数值的可调用对象（用于惰性求值）。
        """
        if self.is_whitelist_mode:
            match_whitelist = self.whitelist_rules.matches(
                name, size_or_callable, is_dir
            )
            if match_whitelist:
                match_keep = self.keep_rules.matches(name, size_or_callable, is_dir)
                match_delete = self.delete_rules.matches(name, size_or_callable, is_dir)
                if match_keep and match_delete:
                    if self.preferred_rule == "delete":
                        return FileAction.DELETE
                    else:
                        return FileAction.SKIP
                elif match_keep:
                    return FileAction.SKIP
                elif match_delete:
                    return FileAction.DELETE
                return FileAction.TRANSFER
            else:
                return FileAction.SKIP

        match_keep = self.keep_rules.matches(name, size_or_callable, is_dir)
        match_delete = self.delete_rules.matches(name, size_or_callable, is_dir)

        # 1. 如果同时命中保留和删除规则，根据配置项处理
        if match_keep and match_delete:
            if self.preferred_rule == "delete":
                return FileAction.DELETE
            else:
                return FileAction.SKIP

        # 2. 如果只命中保留规则，保留目标
        elif match_keep:
            return FileAction.SKIP

        # 3. 如果只命中删除规则，删除目标
        elif match_delete:
            return FileAction.DELETE

        # 4. 都不命中，正常传输
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


def _setup_backup_dir(dest_root, max_backups, exclude_list, logger):
    backup_base = os.path.join(dest_root, ".smart-archiver.backups")
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_dir = os.path.join(backup_base, timestamp)

    # 确保备份目录本身被排除，防止无限递归或被删除
    exclude_list.append(".smart-archiver.backups/")

    # 清理旧备份
    if max_backups > 0 and os.path.exists(backup_base):
        try:
            backups = [
                os.path.join(backup_base, d)
                for d in os.listdir(backup_base)
                if os.path.isdir(os.path.join(backup_base, d))
            ]
            backups.sort()
            # 如果当前备份数已经达到或超过最大限制，删除最旧的，为新备份腾出空间
            if len(backups) >= max_backups:
                num_to_delete = len(backups) - max_backups + 1
                for b in backups[:num_to_delete]:
                    shutil.rmtree(b)
                    logger.debug(f"已删除旧备份: {b}")
        except Exception as e:
            logger.error(f"清理旧备份失败: {e}")

    return backup_dir


def _run_sync_command(cmd, logger, tool_name, prepend_timestamp=False):
    logger.info(f"正在使用 {tool_name} 进行同步……")
    try:
        start_time = time.time()
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        for line in process.stdout:
            if prepend_timestamp:
                logger.info(line.strip(), raw=True, prepend_timestamp=True)
            else:
                logger.info(line.strip())
        process.wait()
        if process.returncode != 0:
            logger.error(f"{tool_name} 同步失败，退出码: {process.returncode}")
        else:
            end_time = time.time()
            exec_time = end_time - start_time
            logger.success(
                f"{tool_name} 同步完成，耗时: {format_timespan(exec_time)}。"
            )
    except Exception as e:
        logger.error(f"执行 {tool_name} 时发生错误: {e}")


def _run_rclone_sync(source_root, dest_root, exclude_list, backup_dir, logger):
    if not shutil.which("rclone"):
        logger.error("未找到 rclone，无法执行 sync 模式，跳过该任务。")
        return

    cmd = ["rclone", "sync", source_root, dest_root]
    for ex in exclude_list:
        cmd.extend(["--exclude", ex])
    if backup_dir:
        cmd.extend(["--backup-dir", backup_dir])

    _run_sync_command(cmd, logger, "rclone")


def _run_rsync_sync(source_root, dest_root, exclude_list, backup_dir, logger):
    if not shutil.which("rsync"):
        logger.error("未找到 rsync，无法执行 sync 模式，跳过该任务。")
        return

    src = source_root if source_root.endswith("/") else source_root + "/"
    cmd = ["rsync", "-av", "--delete", src, dest_root]
    for ex in exclude_list:
        cmd.extend(["--exclude", ex])
    if backup_dir:
        cmd.extend(["--backup", f"--backup-dir={backup_dir}"])

    _run_sync_command(cmd, logger, "rsync", prepend_timestamp=True)


def handle_sync_mode(task, config, logger, source_root, dest_root):
    """
    处理 sync 模式：使用 rsync 或 rclone 镜像同步目录
    """
    task_name = task.get("name")
    if task_name:
        logger.info(f" - 名称：{task_name}")
    logger.info(" - 任务模式: 同步")
    logger.info(f" - 源路径: {source_root}")
    logger.info(f" - 目标路径: {dest_root}")

    if not os.path.exists(source_root):
        logger.error(f"源目录不存在: {source_root}")
        return

    if not os.path.isdir(dest_root):
        logger.error("!!! CRUCIAL: 目标目录不存在 !!!")
        return

    is_windows = os.name == "nt"

    exclude_list = task.get("exclude", [])
    if isinstance(exclude_list, str):
        exclude_list = [exclude_list]

    backup_enabled = task.get("create_backups", False)
    max_backups = task.get("max_backups", 0)

    backup_dir = None
    if backup_enabled:
        backup_dir = _setup_backup_dir(dest_root, max_backups, exclude_list, logger)

    if is_windows:
        _run_rclone_sync(source_root, dest_root, exclude_list, backup_dir, logger)
    else:
        _run_rsync_sync(source_root, dest_root, exclude_list, backup_dir, logger)


def get_dir_size_and_mtime(dir_path):
    """
    计算目录的总大小和最新修改时间
    """
    total_size = 0
    latest_mtime = 0

    try:
        # 获取目录本身的修改时间
        latest_mtime = os.stat(dir_path).st_mtime

        for root, dirs, files in os.walk(dir_path):
            for f in files:
                fp = os.path.join(root, f)
                if not os.path.islink(fp):
                    try:
                        stat = os.stat(fp)
                        total_size += stat.st_size
                        if stat.st_mtime > latest_mtime:
                            latest_mtime = stat.st_mtime
                    except OSError:
                        pass
    except OSError:
        pass

    return total_size, latest_mtime


def _validate_task_config(task, task_mode, logger):
    if task_mode == "sync":
        required_fields = ["mode"]
    elif task_mode == "rotate":
        required_fields = [
            "mode",
            "conflict_policy",
            "remove_empty_dirs",
        ]
        size_limit = parse_size_string(task.get("size_limit", "0"))
        count_limit = int(task.get("count_limit", 0))
        if size_limit == 0 and count_limit == 0:
            logger.error(
                "rotate 模式下必须配置 size_limit 或 count_limit，且不能都为 0，跳过该任务。"
            )
            return False
    else:
        required_fields = [
            "mode",
            "min_age_minutes",
            "conflict_policy",
            "remove_empty_dirs",
        ]

    missing_fields = [field for field in required_fields if field not in task]
    if missing_fields:
        logger.error(f"任务配置缺少必填项: {'、'.join(missing_fields)}，跳过该任务。")
        return False

    if task_mode in ["whitelist_copy", "whitelist_move"]:
        whitelist_rules = task.get("whitelist_rules", {})
        if not whitelist_rules.get("dirs") and not whitelist_rules.get("files"):
            logger.error(
                "白名单模式下必须配置 whitelist_rules.dirs 或 whitelist_rules.files，跳过该任务。"
            )
            return False

    return True


def _print_task_header(
    task, task_mode, source_root, dest_root, age_threshold_seconds, logger
):
    task_name = task.get("name")
    if task_name:
        logger.info(f" - 名称：{task_name}")

    mode_str = "移动"
    if task_mode in ["copy", "whitelist_copy"]:
        mode_str = "复制"
    elif task_mode == "whitelist_move":
        mode_str = "移动 (白名单)"
    elif task_mode == "whitelist_copy":
        mode_str = "复制 (白名单)"
    elif task_mode == "rotate":
        mode_str = "轮转"

    logger.info(f" - 任务模式: {mode_str}")
    logger.info(f" - 源路径: {source_root}")
    logger.info(f" - 目标路径: {dest_root}")
    if task_mode != "rotate":
        logger.info(f" - 时间阈值: {format_timespan(age_threshold_seconds)}")
    if task_mode == "rotate":
        size_limit = task.get("size_limit")
        count_limit = task.get("count_limit")
        if size_limit and parse_size_string(size_limit) > 0:
            logger.info(f" - 大小限制: {size_limit}")
        if count_limit and int(count_limit) > 0:
            logger.info(f" - 数量限制: {count_limit}")


def _process_directories(
    dirs, root, source_root, policy, now, age_threshold_seconds, local_stats, logger
):
    dirs_to_remove = []
    for d in dirs:
        dir_path = os.path.join(root, d)
        rel_dir_path = os.path.relpath(dir_path, source_root)

        dir_size_cache = []
        dir_mtime_cache = []

        # 惰性获取目录大小和修改时间，仅在规则匹配需要时调用
        def get_size():
            if not dir_size_cache:
                s, m = get_dir_size_and_mtime(dir_path)
                dir_size_cache.append(s)
                dir_mtime_cache.append(m)
            return dir_size_cache[0]

        action = policy.decide(d, get_size, is_dir=True)

        if action == FileAction.DELETE:
            # 如果之前没获取过大小（例如命中 ALL 规则），则在此处获取以进行时间检查
            if not dir_mtime_cache:
                s, m = get_dir_size_and_mtime(dir_path)
                dir_size_cache.append(s)
                dir_mtime_cache.append(m)
            dir_mtime = dir_mtime_cache[0]
            dir_size = dir_size_cache[0]

            if (now - dir_mtime) > age_threshold_seconds:
                try:
                    shutil.rmtree(dir_path)
                    logger.success(
                        f"删除目录: {rel_dir_path} ({format_size(dir_size, binary=True)})"
                    )
                    local_stats.deleted += 1
                except OSError as e:
                    logger.error(f"删除目录失败: {rel_dir_path}\nError: {e}")
            dirs_to_remove.append(d)
        elif action == FileAction.SKIP:
            size_str = (
                f" ({format_size(dir_size_cache[0], binary=True)})"
                if dir_size_cache
                else ""
            )
            logger.debug(f"保留目录 (匹配规则): {rel_dir_path}{size_str}")
            local_stats.kept += 1
            dirs_to_remove.append(d)

    for d in dirs_to_remove:
        dirs.remove(d)


def _process_files(
    files,
    root,
    source_root,
    dest_root,
    policy,
    now,
    age_threshold_seconds,
    local_stats,
    history_mgr,
    max_retries,
    conflict_policy,
    task_mode,
    logger,
):
    for file in files:
        src_path = os.path.join(root, file)
        rel_path = os.path.relpath(src_path, source_root)

        # 检查是否需要跳过
        should_skip, fail_count = history_mgr.should_skip(src_path, max_retries)
        if should_skip:
            # 简单处理，视为跳过
            local_stats.dropped += 1
            logger.warning(f"跳过文件 (多次失败): {src_path}")
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
            logger.warning(f"跳过文件 (被锁定): {rel_path}")
            continue

        # 核心决策逻辑
        action = policy.decide(file, size)

        # 执行动作
        if action == FileAction.TRANSFER:
            move_file(
                src_path,
                size,
                source_root,
                dest_root,
                logger,
                local_stats,
                history_mgr,
                conflict_policy,
                task_mode,
            )
        elif action == FileAction.DELETE:
            delete_file(src_path, size, source_root, logger, local_stats, history_mgr)
        elif action == FileAction.SKIP:
            local_stats.kept += 1
            logger.debug(
                f"保留文件 (匹配规则): {rel_path}  ({format_size(size, binary=True)})"
            )


def _print_task_summary(local_stats, duration_str, total_size_str, speed_str, logger):
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
        logger.info(
            f"在 {duration_str} 内传输了 {total_size_str}，平均速度 {speed_str}。"
        )


def handle_rotate_mode(
    task, config, logger, history_mgr, source_root, dest_root, task_mode
):
    local_stats = MoverStats()
    max_retries = config.get("max_retries", 3)
    conflict_policy = task["conflict_policy"].lower()
    remove_empty_dirs = task["remove_empty_dirs"]

    _print_task_header(task, task_mode, source_root, dest_root, 0, logger)

    if not os.path.exists(source_root):
        logger.error(f"源目录不存在: {source_root}")
        return

    if not os.path.isdir(dest_root):
        logger.error("!!! CRUCIAL: 目标目录不存在 !!!")
        return

    task_delete_rules = task.get("delete_rules", {})
    task_keep_rules = task.get("keep_rules", {})
    preferred_rule = task.get("preferred_rule", "keep")
    whitelist_rules = task.get("whitelist_rules", {})
    is_whitelist_mode = task_mode in ["whitelist_copy", "whitelist_move"]

    merged_config = {
        "delete_rules": task_delete_rules,
        "keep_rules": task_keep_rules,
        "preferred_rule": preferred_rule,
        "whitelist_rules": whitelist_rules,
        "is_whitelist_mode": is_whitelist_mode,
    }
    policy = FileFilterPolicy(merged_config)

    size_limit = parse_size_string(task.get("size_limit", "0"))
    count_limit = int(task.get("count_limit", 0))

    start_time = time.time()

    all_files = []
    current_total_size = 0
    current_total_count = 0

    for root, dirs, files in os.walk(source_root):
        for file in files:
            src_path = os.path.join(root, file)
            try:
                file_stat = os.stat(src_path)
                size = file_stat.st_size
                mtime = file_stat.st_mtime
                all_files.append(
                    {"name": file, "path": src_path, "size": size, "mtime": mtime}
                )
                current_total_size += size
                current_total_count += 1
            except OSError:
                continue

    is_exceeded = False
    if size_limit > 0 and current_total_size > size_limit:
        is_exceeded = True
    if count_limit > 0 and current_total_count > count_limit:
        is_exceeded = True

    if not is_exceeded:
        logger.info("当前未超过限制，无需轮转。")
        end_time = time.time()
        duration_str, total_size_str, speed_str = local_stats.calculate_speed(
            start_time, end_time
        )
        _print_task_summary(
            local_stats, duration_str, total_size_str, speed_str, logger
        )
        return

    candidates = []
    for f in all_files:
        should_skip, fail_count = history_mgr.should_skip(f["path"], max_retries)
        if should_skip:
            local_stats.dropped += 1
            logger.warning(f"跳过文件 (多次失败): {f['path']}")
            continue

        if is_file_locked(f["path"]):
            local_stats.locked_skipped += 1
            logger.warning(
                f"跳过文件 (被锁定): {os.path.relpath(f['path'], source_root)}"
            )
            continue

        action = policy.decide(f["name"], f["size"])
        if action == FileAction.DELETE:
            delete_file(
                f["path"], f["size"], source_root, logger, local_stats, history_mgr
            )
            current_total_size -= f["size"]
            current_total_count -= 1
        elif action == FileAction.SKIP:
            local_stats.kept += 1
            logger.debug(
                f"保留文件 (匹配规则): {os.path.relpath(f['path'], source_root)} ({format_size(f['size'], binary=True)})"
            )
        elif action == FileAction.TRANSFER:
            candidates.append(f)

    candidates.sort(key=lambda x: x["mtime"])

    for f in candidates:
        size_ok = size_limit == 0 or current_total_size <= size_limit
        count_ok = count_limit == 0 or current_total_count <= count_limit
        if size_ok and count_ok:
            break

        move_file(
            f["path"],
            f["size"],
            source_root,
            dest_root,
            logger,
            local_stats,
            history_mgr,
            conflict_policy,
            "move",
        )

        if not os.path.exists(f["path"]):
            current_total_size -= f["size"]
            current_total_count -= 1

    if size_limit > 0 and current_total_size > size_limit:
        logger.warning(
            f"已无可处理文件，但目录体积 ({format_size(current_total_size, binary=True)}) 仍未满足限制 ({format_size(size_limit, binary=True)})，请检查配置。"
        )
    if count_limit > 0 and current_total_count > count_limit:
        logger.warning(
            f"已无可处理文件，但文件数量 ({current_total_count}) 仍未满足限制 ({count_limit})，请检查配置。"
        )

    if remove_empty_dirs:
        clean_empty_dirs(source_root, logger)

    end_time = time.time()
    duration_str, total_size_str, speed_str = local_stats.calculate_speed(
        start_time, end_time
    )
    _print_task_summary(local_stats, duration_str, total_size_str, speed_str, logger)


def process_directory_pair(task, config, logger, history_mgr):
    """
    处理单个目录对：遍历、移动文件
    """
    local_stats = MoverStats()

    source_root = task.get("source")
    dest_root = task.get("dest")
    task_mode = task.get("mode", "").lower()

    if not _validate_task_config(task, task_mode, logger):
        return

    if task_mode == "sync":
        handle_sync_mode(task, config, logger, source_root, dest_root)
        return

    if task_mode == "rotate":
        handle_rotate_mode(
            task, config, logger, history_mgr, source_root, dest_root, task_mode
        )
        return

    min_age_minutes = task.get("min_age_minutes", 0)
    max_retries = config.get("max_retries", 3)
    conflict_policy = task["conflict_policy"].lower()
    remove_empty_dirs = task["remove_empty_dirs"]

    now = time.time()
    age_threshold_seconds = min_age_minutes * 60

    _print_task_header(
        task, task_mode, source_root, dest_root, age_threshold_seconds, logger
    )

    if not os.path.exists(source_root):
        logger.error(f"源目录不存在: {source_root}")
        return

    if not os.path.isdir(dest_root):
        logger.error("!!! CRUCIAL: 目标目录不存在 !!!")
        return

    task_delete_rules = task.get("delete_rules", {})
    task_keep_rules = task.get("keep_rules", {})
    preferred_rule = task.get("preferred_rule", "keep")
    whitelist_rules = task.get("whitelist_rules", {})
    is_whitelist_mode = task_mode in ["whitelist_copy", "whitelist_move"]

    merged_config = {
        "delete_rules": task_delete_rules,
        "keep_rules": task_keep_rules,
        "preferred_rule": preferred_rule,
        "whitelist_rules": whitelist_rules,
        "is_whitelist_mode": is_whitelist_mode,
    }

    # 使用合并后的配置初始化策略
    policy = FileFilterPolicy(merged_config)

    # 记录开始时间
    start_time = time.time()

    # 遍历文件
    for root, dirs, files in os.walk(source_root):
        _process_directories(
            dirs,
            root,
            source_root,
            policy,
            now,
            age_threshold_seconds,
            local_stats,
            logger,
        )
        _process_files(
            files,
            root,
            source_root,
            dest_root,
            policy,
            now,
            age_threshold_seconds,
            local_stats,
            history_mgr,
            max_retries,
            conflict_policy,
            task_mode,
            logger,
        )

    # 清理空目录 (对应 find -depth -empty -type d rmdir)
    if remove_empty_dirs and task_mode not in ["copy", "whitelist_copy"]:
        clean_empty_dirs(source_root, logger)

    # 记录结束时间
    end_time = time.time()

    # 计算传输速率
    duration_str, total_size_str, speed_str = local_stats.calculate_speed(
        start_time, end_time
    )

    _print_task_summary(local_stats, duration_str, total_size_str, speed_str, logger)


def delete_file(src_path, file_size, source_root, logger, stats, history_mgr):
    rel_path = os.path.relpath(src_path, source_root)
    """删除逻辑"""
    try:
        os.remove(src_path)
        logger.success(f"删除文件: {rel_path}  ({format_size(file_size, binary=True)})")
        history_mgr.record_success(src_path)
        stats.deleted += 1
    except OSError as e:
        count = history_mgr.record_failure(src_path)
        logger.error(f"删除文件失败 ({count} 次): {rel_path}\nError: {e}")


def move_file(
    src_path,
    file_size,
    source_root,
    dest_root,
    logger,
    stats,
    history_mgr,
    conflict_policy,
    task_mode,
):
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
            if conflict_policy == "skip":
                stats.conflict_skipped += 1
                logger.debug(f"跳过 (重复): {rel_path}")
                # 不记录到 success，也不报错
                return

            elif conflict_policy == "copy":
                # 计算新的不冲突的路径
                new_dest_path = get_unique_dest(dest_path)

            elif conflict_policy == "overwrite":
                # 默认逻辑：删除目标
                os.remove(dest_path)

            else:
                # 未知策略默认跳过
                return

        # 使用 shutil.move 或 shutil.copy2
        if task_mode in ["copy", "whitelist_copy"]:
            shutil.copy2(src_path, new_dest_path)
        else:
            # 同一文件系统下为原子重命名，跨文件系统自动降级为复制后删除
            shutil.move(src_path, new_dest_path)

        # 统计传输流量
        stats.total_bytes += file_size

        # 成功后清理记录
        history_mgr.record_success(src_path)

        size_str = format_size(file_size, binary=True)
        action_str = "复制" if task_mode in ["copy", "whitelist_copy"] else "移动"
        if file_exists and conflict_policy == "overwrite":
            logger.success(f"覆盖同名文件: {rel_path} ({size_str})")
        elif file_exists and conflict_policy == "copy":
            new_rel = os.path.relpath(new_dest_path, dest_root)
            logger.success(f"目标存在，创建副本: {new_rel} ({size_str})")
        else:
            logger.success(f"{action_str}文件: {rel_path} ({size_str})")

        stats.success += 1

    except Exception as e:
        # 失败时记录
        count = history_mgr.record_failure(src_path)
        action_str = "复制" if task_mode in ["copy", "whitelist_copy"] else "文件"
        logger.error(f"{action_str}文件失败 ({count} 次): {rel_path}\n{e}")
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
            if logger:
                rel_dir = os.path.relpath(root, source_root)
                logger.debug(f"删除空目录: {rel_dir}")
                # stats.success += 1
        except OSError:
            pass  # 目录非空，跳过
