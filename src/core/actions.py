import os
from src.utils import parse_size_string
from src.app_context import AppContext
from src.presentation import fmt_size


def validate_task_config(task, task_mode):
    ctx = AppContext.get()

    if task_mode == "sync":
        required_fields = ["mode"]
    elif task_mode == "rotate":
        required_fields = [
            "mode",
            "remove_empty_dirs",
        ]
        size_limit = parse_size_string(str(task.get("size_limit", "0")))
        count_limit = int(task.get("count_limit", 0))
        rotate_rules = task.get("rotate_rules", {})
        rotate_size_rules = rotate_rules.get("size", {})
        rotate_count_rules = rotate_rules.get("count", {})

        if (
            size_limit == 0
            and count_limit == 0
            and not rotate_size_rules
            and not rotate_count_rules
        ):
            ctx.logger.error(
                "rotate 模式下必须配置 size_limit、count_limit、rotate_rules.size 或 rotate_rules.count 中的至少一项，跳过该任务。"
            )
            return False
    else:
        required_fields = [
            "mode",
            "mtime_threshold_minutes",
            "conflict_policy",
            "remove_empty_dirs",
        ]

    missing_fields = [field for field in required_fields if field not in task]
    if missing_fields:
        ctx.logger.error(
            f"任务配置缺少必填项: {'、'.join(missing_fields)}，跳过该任务。"
        )
        return False

    if task_mode in ["whitelist_copy", "whitelist_move"]:
        whitelist_rules = task.get("whitelist_rules", {})
        if not whitelist_rules:
            ctx.logger.error("白名单模式下必须配置 whitelist_rules，跳过该任务。")
            return False

    return True


def delete_file(src_path, file_size, source_root, stats):
    ctx = AppContext.get()
    rel_path = os.path.relpath(src_path, source_root)
    try:
        os.remove(src_path)
        ctx.logger.success(
            f"删除文件: {rel_path}  ({fmt_size(file_size, binary=True)})"
        )
        ctx.history_mgr.record_success(src_path)
        stats.record_deleted()
    except OSError as e:
        count = ctx.history_mgr.record_failure(src_path)
        ctx.logger.error(f"删除文件失败 ({count} 次): {rel_path}\nError: {e}")


def transfer_file(
    src_path,
    file_size,
    source_root,
    dest_backend,
    stats,
    conflict_policy,
    is_copy,
):
    ctx = AppContext.get()
    rel_path = os.path.relpath(src_path, source_root)
    dest_path = dest_backend.build_dest_path(rel_path)
    action_name = "复制" if is_copy else "移动"

    _POLICY_TO_ON_EXISTS = {
        "skip": "skip",
        "overwrite": "overwrite",
        "copy": "rename",
    }
    on_exists = _POLICY_TO_ON_EXISTS.get(conflict_policy, "error")

    try:
        action, _ = dest_backend.transfer_file(
            src_path, dest_path, on_exists, is_copy
        )
    except Exception as e:
        count = ctx.history_mgr.record_failure(src_path)
        ctx.logger.error(f"{action_name}文件失败 ({count} 次): {rel_path}\n{e}")
        stats.record_error()
        return

    size_str = fmt_size(file_size, binary=True)

    if action == "skipped":
        stats.record_conflict_skipped()
        ctx.logger.debug(f"跳过 (重复): {rel_path}")
    elif action in ("uploaded", "renamed", "overwritten"):
        stats.record_success(bytes_transferred=file_size)
        ctx.history_mgr.record_success(src_path)

        if action == "overwritten":
            ctx.logger.success(f"覆盖同名文件: {rel_path} ({size_str})")
        elif action == "renamed":
            ctx.logger.success(f"目标存在，创建副本: {rel_path} ({size_str})")
        else:
            ctx.logger.success(f"{action_name}文件: {rel_path} ({size_str})")
