import os
import shutil
import time
import subprocess
from datetime import datetime

from src.app_context import AppContext
from src.presentation import fmt_timespan, print_task_header
from src.core.registry import register_handler
from src.core.handlers.base import BaseTaskHandler
from src.core.backend import RemoteDestBackend, SshDestBackend
from src.remote.ssh_config import (
    build_ssh_options,
    build_ssh_dest,
    build_rclone_sftp_dest,
    resolve_sshpass,
)
from src import fs_ops


# ============================================================
# 同步工具注册与解析
# ============================================================


def _resolve_sync_tool(task):
    """根据配置和平台解析要使用的同步工具。

    Returns:
        str | None: 解析后的工具名（"rsync" 或 "rclone"），解析失败返回 None。
    """
    tool = task.get("tool", "auto").lower()

    if tool == "auto":
        return "rclone" if os.name == "nt" else "rsync"

    if tool in ("rsync", "rclone"):
        return tool

    ctx = AppContext.get()
    ctx.logger.error(
        f"sync 模式的 tool 配置值无效: '{tool}'，"
        f"可选值: auto、rsync、rclone，跳过该任务。"
    )
    return None


def _run_sync_command(cmd, tool_name, prepend_timestamp=False, env=None):
    ctx = AppContext.get()
    ctx.logger.info(f"正在使用 {tool_name} 进行同步……")
    try:
        start_time = time.time()
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=env,
        )
        for line in process.stdout:
            if prepend_timestamp:
                ctx.logger.info(line.strip(), raw=True, prepend_timestamp=True)
            else:
                ctx.logger.info(line.strip())
        process.wait()
        if process.returncode != 0:
            ctx.logger.error(f"{tool_name} 同步失败，退出码: {process.returncode}")
        else:
            end_time = time.time()
            exec_time = end_time - start_time
            ctx.logger.success(
                f"{tool_name} 同步完成，耗时: {fmt_timespan(exec_time)}。"
            )
    except Exception as e:
        ctx.logger.error(f"执行 {tool_name} 时发生错误: {e}")


# ============================================================
# rclone 同步
# ============================================================


def _run_rclone_sync(source_root, dest_root, exclude_list, backup_dir):
    ctx = AppContext.get()
    if not shutil.which("rclone"):
        ctx.logger.error("未找到 rclone，无法执行 sync 模式，跳过该任务。")
        return

    cmd = ["rclone", "sync", source_root, dest_root]
    for ex in exclude_list:
        cmd.extend(["--exclude", ex])
    if backup_dir:
        cmd.extend(["--backup-dir", backup_dir])

    _run_sync_command(cmd, "rclone")


# ============================================================
# rsync 同步
# ============================================================


def _run_rsync_sync(source_root, dest_root, exclude_list, backup_dir):
    ctx = AppContext.get()
    if not shutil.which("rsync"):
        ctx.logger.error("未找到 rsync，无法执行 sync 模式，跳过该任务。")
        return

    src = source_root if source_root.endswith("/") else source_root + "/"
    cmd = ["rsync", "-av", "--delete", src, dest_root]
    for ex in exclude_list:
        cmd.extend(["--exclude", ex])
    if backup_dir:
        cmd.extend(["--backup", f"--backup-dir={backup_dir}"])

    _run_sync_command(cmd, "rsync", prepend_timestamp=True)


# ============================================================
# SSH 辅助
# ============================================================


def _run_rclone_sync_ssh(source_root, ssh, remote_path, exclude_list, backup_dir):
    """使用 rclone SFTP on-the-fly 后端同步到远端。"""
    ctx = AppContext.get()
    if not shutil.which("rclone"):
        ctx.logger.error("未找到 rclone，无法执行 sync 模式，跳过该任务。")
        return

    dest = build_rclone_sftp_dest(ssh, remote_path)
    cmd = ["rclone", "sync", source_root, dest]
    for ex in exclude_list:
        cmd.extend(["--exclude", ex])
    if backup_dir:
        remote_backup = build_rclone_sftp_dest(ssh, backup_dir)
        cmd.extend(["--backup-dir", remote_backup])

    _run_sync_command(cmd, "rclone (SFTP)")


def _run_rsync_sync_ssh(source_root, ssh, remote_path, exclude_list, backup_dir):
    """使用 rsync over SSH 同步到远端。"""
    ctx = AppContext.get()
    if not shutil.which("rsync"):
        ctx.logger.error("未找到 rsync，无法执行 sync 模式，跳过该任务。")
        return

    # 构建 ssh 命令片段
    ssh_opts = build_ssh_options(ssh)

    # 决定是否需要 sshpass 前缀
    sshpass_path = None
    if ssh.password_file:
        sshpass_path = resolve_sshpass()
        if not sshpass_path:
            ctx.logger.error(
                "SSH 远端配置了 password_file 但未找到 sshpass，"
                "请安装 sshpass 或改用 credential_file 进行密钥认证，跳过该任务。"
            )
            return

    # rsync -e 参数
    ssh_cmd_parts = []
    if sshpass_path:
        ssh_cmd_parts.extend([sshpass_path, "-f", ssh.password_file])
    ssh_cmd_parts.append("ssh")
    ssh_cmd_parts.extend(ssh_opts)
    rsh_cmd = " ".join(ssh_cmd_parts)

    src = source_root if source_root.endswith("/") else source_root + "/"
    dest = build_ssh_dest(ssh, remote_path)

    cmd = ["rsync", "-av", "--delete", "-e", rsh_cmd, src, dest]
    for ex in exclude_list:
        cmd.extend(["--exclude", ex])
    if backup_dir:
        remote_backup = build_ssh_dest(ssh, backup_dir)
        cmd.extend(["--backup", f"--backup-dir={remote_backup}"])

    _run_sync_command(cmd, "rsync (SSH)", prepend_timestamp=True)


def _cleanup_ssh_backups(ssh, backup_base, max_backups):
    """通过 SSH 命令清理远端旧备份目录。"""
    ctx = AppContext.get()
    ssh_opts = build_ssh_options(ssh)

    sshpass_path = None
    if ssh.password_file:
        sshpass_path = resolve_sshpass()

    # 构建列出备份目录的命令
    list_cmd_parts = []
    if sshpass_path:
        list_cmd_parts.extend([sshpass_path, "-f", ssh.password_file])
    list_cmd_parts.extend(["ssh", *ssh_opts, f"{ssh.user}@{ssh.address}"])
    list_cmd_parts.append(
        f"ls -1d '{backup_base}'/*/ 2>/dev/null | sort | head -n -{max_backups - 1}"
    )

    try:
        result = subprocess.run(
            list_cmd_parts,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=30,
        )
        if result.returncode != 0 or not result.stdout.strip():
            return

        to_delete = [d.strip() for d in result.stdout.strip().splitlines() if d.strip()]
        for d in to_delete:
            rm_cmd_parts = []
            if sshpass_path:
                rm_cmd_parts.extend([sshpass_path, "-f", ssh.password_file])
            rm_cmd_parts.extend(
                ["ssh", *ssh_opts, f"{ssh.user}@{ssh.address}", f"rm -rf '{d}'"]
            )
            subprocess.run(
                rm_cmd_parts,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=30,
            )
            ctx.logger.debug(f"已删除远端旧备份: {d}")
    except Exception as e:
        ctx.logger.error(f"清理远端旧备份失败: {e}")


# ============================================================
# SyncHandler
# ============================================================


@register_handler("sync")
class SyncHandler(BaseTaskHandler):
    def execute(self):
        if not self.validate():
            return

        print_task_header(self.task)

        ctx = AppContext.get()

        # HTTP 远端仍然不支持 sync 模式
        if isinstance(self.dest_backend, RemoteDestBackend):
            ctx.logger.error(
                "sync 模式不支持 HTTP 远程目标目录，请改用 move/copy 模式，"
                "或在远程服务器上直接运行 sync，跳过该任务。"
            )
            return

        if not os.path.exists(self.source_root):
            ctx.logger.error(f"源目录不存在: {self.source_root}")
            return

        # 非 SSH 远端需要本地目标目录存在
        if not isinstance(self.dest_backend, SshDestBackend):
            if not self.dest_backend.is_dir(self.dest_backend.root_path):
                ctx.logger.critical("!!! CRUCIAL: 目标目录不存在 !!!")
                return

        tool = _resolve_sync_tool(self.task)
        if tool is None:
            return

        exclude_list = self.task.get("exclude", [])
        if isinstance(exclude_list, str):
            exclude_list = [exclude_list]

        backup_enabled = self.task.get("create_backups", False)
        max_backups = self.task.get("max_backups", 0)

        # ---- SSH 远端路径 ----
        if isinstance(self.dest_backend, SshDestBackend):
            ssh = self.dest_backend.ssh_config
            remote_path = self.dest_backend.root_path

            backup_dir = None
            if backup_enabled:
                backup_base = remote_path.rstrip("/") + "/.smart-archiver.backups"
                timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
                backup_dir = backup_base + "/" + timestamp
                exclude_list.append(".smart-archiver.backups/")

                # 清理远端旧备份
                if max_backups > 0:
                    _cleanup_ssh_backups(ssh, backup_base, max_backups)

            _ssh_runners = {
                "rsync": _run_rsync_sync_ssh,
                "rclone": _run_rclone_sync_ssh,
            }
            _ssh_runners[tool](self.source_root, ssh, remote_path, exclude_list, backup_dir)
            return

        # ---- 本地路径 ----
        backup_dir = None
        if backup_enabled:
            backup_dir = self._setup_backup_dir(
                self.dest_root, max_backups, exclude_list
            )

        _sync_runners = {
            "rsync": _run_rsync_sync,
            "rclone": _run_rclone_sync,
        }
        _sync_runners[tool](self.source_root, self.dest_root, exclude_list, backup_dir)

    def _setup_backup_dir(self, dest_root, max_backups, exclude_list):
        ctx = AppContext.get()
        backup_base = os.path.join(dest_root, ".smart-archiver.backups")
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup_dir = os.path.join(backup_base, timestamp)

        exclude_list.append(".smart-archiver.backups/")

        if max_backups > 0 and os.path.exists(backup_base):
            try:
                backups = [
                    os.path.join(backup_base, d)
                    for d in os.listdir(backup_base)
                    if os.path.isdir(os.path.join(backup_base, d))
                ]
                backups.sort()
                if len(backups) >= max_backups:
                    num_to_delete = len(backups) - max_backups + 1
                    for b in backups[:num_to_delete]:
                        fs_ops.delete_dir(b)
                        ctx.logger.debug(f"已删除旧备份: {b}")
            except Exception as e:
                ctx.logger.error(f"清理旧备份失败: {e}")

        return backup_dir
