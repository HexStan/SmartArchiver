import os
import posixpath
import shutil
import time
import subprocess
from datetime import datetime

from src.app_context import AppContext
from src.presentation import fmt_timespan, print_task_header
from src.core.registry import register_handler
from src.core.handlers.base import BaseTaskHandler
from src.core.backend import RemoteDestBackend, SshDestBackend
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


def _run_sync_command(cmd, tool_name, prepend_timestamp=False):
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


@register_handler("sync")
class SyncHandler(BaseTaskHandler):
    def execute(self):
        if not self.validate():
            return

        print_task_header(self.task)

        ctx = AppContext.get()

        if isinstance(self.dest_backend, RemoteDestBackend):
            ctx.logger.error(
                "sync 模式不支持 HTTP 远程目标目录，请改用 move/copy 模式，"
                "或在远程服务器上直接运行 sync，跳过该任务。"
            )
            return

        if not os.path.exists(self.source_root):
            ctx.logger.error(f"源目录不存在: {self.source_root}")
            return

        if not self.dest_backend.is_dir(self.dest_backend.root_path):
            ctx.logger.critical("!!! 致命错误: 目标目录不存在 !!!")
            return

        tool = _resolve_sync_tool(self.task)
        if tool is None:
            return

        exclude_list = self.task.get("exclude", [])
        if isinstance(exclude_list, str):
            exclude_list = [exclude_list]

        backup_enabled = self.task.get("create_backups", False)
        max_backups = self.task.get("max_backups", 0)

        if isinstance(self.dest_backend, SshDestBackend):
            self._execute_ssh_sync(tool, exclude_list, backup_enabled, max_backups)
        else:
            backup_dir = None
            if backup_enabled:
                backup_dir = self._setup_backup_dir(
                    self.dest_root, max_backups, exclude_list
                )

            _sync_runners = {
                "rsync": _run_rsync_sync,
                "rclone": _run_rclone_sync,
            }
            _sync_runners[tool](
                self.source_root, self.dest_root, exclude_list, backup_dir
            )

    # ============================================================
    # SSH 远端同步
    # ============================================================

    def _execute_ssh_sync(self, tool, exclude_list, backup_enabled, max_backups):
        """执行 SSH 远端同步（rsync 或 rclone）。"""
        remote = self.dest_backend.remote

        backup_dir = None
        if backup_enabled:
            backup_dir = self._setup_ssh_backup_dir(max_backups, exclude_list)

        if tool == "rsync":
            self._run_ssh_rsync(remote, exclude_list, backup_dir)
        else:
            self._run_ssh_rclone(remote, exclude_list, backup_dir)

    def _build_rsync_ssh_rsh(self, remote):
        """构建 rsync 的 -e 参数值（SSH 远程 shell 命令字符串）。"""
        opts = [
            "ssh",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-o",
            "LogLevel=ERROR",
        ]
        if remote.port != 22:
            opts.extend(["-p", str(remote.port)])
        if remote.key_file:
            opts.extend(["-i", remote.key_file])

        rsh = " ".join(opts)
        if remote.password_file:
            rsh = f"sshpass -f {remote.password_file} {rsh}"

        return rsh

    def _run_ssh_rsync(self, remote, exclude_list, backup_dir):
        """通过 rsync + SSH 同步到远端主机。"""
        ctx = AppContext.get()
        if not shutil.which("rsync"):
            ctx.logger.error("未找到 rsync，无法执行 sync 模式，跳过该任务。")
            return

        src = self.source_root
        src = src if src.endswith("/") else src + "/"
        dest = f"{remote.user}@{remote.host}:{self.dest_backend.root_path}"

        cmd = ["rsync", "-av", "--delete"]
        rsh = self._build_rsync_ssh_rsh(remote)
        cmd.extend(["-e", rsh])

        for ex in exclude_list:
            cmd.extend(["--exclude", ex])

        if backup_dir:
            # --backup-dir 只接受目录路径，不支持 user@host: 前缀。
            # rsync 已经知道备份放在接收端（远端主机），直接传绝对路径即可。
            cmd.extend(["--backup", f"--backup-dir={backup_dir}"])

        cmd.extend([src, dest])
        _run_sync_command(cmd, "rsync", prepend_timestamp=True)

    def _run_ssh_rclone(self, remote, exclude_list, backup_dir):
        """通过 rclone SFTP 同步到远端主机。"""
        ctx = AppContext.get()
        if not shutil.which("rclone"):
            ctx.logger.error("未找到 rclone，无法执行 sync 模式，跳过该任务。")
            return

        # 使用 :sftp: 后端，通过命令行参数指定连接信息，无需预配置
        dest = f":sftp:{self.dest_backend.root_path}"

        cmd = [
            "rclone",
            "sync",
            self.source_root,
            dest,
            "--sftp-host",
            remote.host,
            "--sftp-user",
            remote.user,
        ]

        if remote.port != 22:
            cmd.extend(["--sftp-port", str(remote.port)])

        if remote.key_file:
            cmd.extend(["--sftp-key-file", remote.key_file])

        for ex in exclude_list:
            cmd.extend(["--exclude", ex])

        if backup_dir:
            remote_dest = f":sftp:{backup_dir}"
            cmd.extend(["--backup-dir", remote_dest])

        # 处理密码文件：通过环境变量传入，避免密码出现在进程列表中
        env = os.environ.copy()
        if remote.password_file:
            try:
                with open(remote.password_file, "r") as f:
                    password = f.read().strip()
                env["RCLONE_SFTP_PASS"] = password
            except OSError as e:
                ctx.logger.error(f"读取密码文件失败: {e}，跳过该任务。")
                return

        ctx.logger.info("正在使用 rclone (SFTP) 进行同步……")
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
                ctx.logger.info(line.strip())
            process.wait()
            if process.returncode != 0:
                ctx.logger.error(f"rclone 同步失败，退出码: {process.returncode}")
            else:
                end_time = time.time()
                exec_time = end_time - start_time
                ctx.logger.success(
                    f"rclone 同步完成，耗时: {fmt_timespan(exec_time)}。"
                )
        except Exception as e:
            ctx.logger.error(f"执行 rclone 时发生错误: {e}")

    def _setup_ssh_backup_dir(self, max_backups, exclude_list):
        """为 SSH 远端目标设置备份目录。

        在远端计算备份路径，清理旧备份，返回远端备份目录路径。
        """
        ctx = AppContext.get()

        backup_base = posixpath.join(self.dest_backend.root_path, ".smart-archiver.backups")
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup_dir = posixpath.join(backup_base, timestamp)

        exclude_list.append(".smart-archiver.backups/")

        if max_backups > 0:
            try:
                all_dirs = self.dest_backend.remote_list_dir(backup_base)
                if all_dirs and len(all_dirs) >= max_backups:
                    num_to_delete = len(all_dirs) - max_backups + 1
                    for d in all_dirs[:num_to_delete]:
                        remote_path = posixpath.join(backup_base, d)
                        ok = self.dest_backend.remote_rmdir(remote_path)
                        if ok:
                            ctx.logger.info(f"已删除远端旧备份: {d}")
                        else:
                            ctx.logger.warning(f"删除远端旧备份失败: {d}")
            except Exception as e:
                ctx.logger.error(f"清理远端旧备份失败: {e}")

        return backup_dir

    # ============================================================
    # 本地备份目录
    # ============================================================

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
                        ctx.logger.info(f"已删除旧备份: {b}")
            except Exception as e:
                ctx.logger.error(f"清理旧备份失败: {e}")

        return backup_dir
