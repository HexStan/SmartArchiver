import os
import posixpath
import shutil
import subprocess
import sys
import time
from datetime import datetime
from typing import Any, NamedTuple, override

from src import fs
from src.app.context import AppContext
from src.app.paths import bundled_rclone_path
from src.core.backend import RemoteDestBackend, SshDestBackend
from src.core.handlers.base import BaseTaskHandler
from src.core.registry import register_handler
from src.output.presentation import fmt_timespan, print_task_header
from src.ssh.remotes import SshRemote

# ============================================================
# 同步工具注册与解析
# ============================================================


class SyncTool(NamedTuple):
    """同步工具解析结果：

    name —— 工具族名（"rclone" 或 "rsync"），决定命令构造方式；
    exec_path —— 实际调用的可执行文件（内置 rclone 为绝对路径，系统工具为命令名）；
    supports_backup_dir —— 是否支持 --backup/--backup-dir
        （macOS 的系统 rsync 自 Sequoia 起为 openrsync 实现，不支持这两个选项）。
    """

    name: str
    exec_path: str
    supports_backup_dir: bool


def _resolve_rclone() -> SyncTool | None:
    """解析 rclone 可执行文件：打包版内置优先，其次系统 PATH。

    Returns:
        SyncTool | None: 解析失败时已记录错误日志，返回 None。
    """
    bundled = bundled_rclone_path()
    if bundled:
        return SyncTool("rclone", bundled, True)

    ctx = AppContext.get()
    if shutil.which("rclone"):
        return SyncTool("rclone", "rclone", True)

    ctx.logger.error("未找到 rclone，无法执行 sync 模式，跳过该任务。")
    return None


def _resolve_rsync() -> SyncTool | None:
    """解析系统 PATH 中的 rsync。

    macOS 的系统 rsync 即 openrsync 实现，不支持 --backup/--backup-dir，
    以 supports_backup_dir=False 标记，由调用方忽略 create_backups 配置。

    Returns:
        SyncTool | None: 解析失败时已记录错误日志，返回 None。
    """
    ctx = AppContext.get()
    if shutil.which("rsync"):
        return SyncTool("rsync", "rsync", sys.platform != "darwin")

    ctx.logger.error("未找到 rsync，无法执行 sync 模式，跳过该任务。")
    return None


def _resolve_sync_tool(task: dict[str, Any]) -> SyncTool | None:
    """根据配置和平台解析要使用的同步工具。

    解析优先级：
    1. tool = "auto"（默认）或 "rclone"：打包版内置的 rclone → 系统 PATH 中的 rclone；
    2. tool = "auto" 且 rclone 不可用：POSIX 平台回退系统 rsync（Windows 不回退）；
    3. tool = "rsync"：系统 PATH 中的 rsync。

    Returns:
        SyncTool | None: 解析结果；解析失败时已记录错误日志，返回 None。
    """
    tool = task.get("tool", "auto").lower()

    if tool not in ("auto", "rsync", "rclone"):
        ctx = AppContext.get()
        ctx.logger.error(
            f"sync 模式的 tool 配置值无效: '{tool}'，可选值: auto、rsync、rclone，跳过该任务。"
        )
        return None

    if tool in ("auto", "rclone"):
        resolved = _resolve_rclone()
        if resolved is not None:
            return resolved
        if tool == "rclone" or os.name == "nt":
            # _resolve_rclone 已记录错误；Windows 的 auto 不回退 rsync
            return None

    return _resolve_rsync()


def _warn_backup_unsupported():
    """提示当前 rsync 为 openrsync 实现，create_backups 已被忽略。"""
    ctx = AppContext.get()
    ctx.logger.warning(
        "当前系统的 rsync 为 openrsync 实现，不支持 --backup/--backup-dir，"
        "已忽略 create_backups 配置。"
    )


def _run_sync_command(cmd: list[str], tool_name: str, prepend_timestamp: bool = False) -> None:
    ctx = AppContext.get()
    ctx.logger.info(f"正在使用 {tool_name} 进行同步……")
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
    except Exception as e:  # noqa: BLE001
        ctx.logger.error(f"执行 {tool_name} 时发生错误: {e}")
        return
    _consume_sync_process(process, tool_name, prepend_timestamp)


def _consume_sync_process(
    process: subprocess.Popen[str], tool_name: str, prepend_timestamp: bool = False
) -> None:
    """读取同步工具的输出直至其退出。

    进程在停止协调器登记，``stop --fast`` 时由协调器终止整棵进程树
    （rsync/rclone 可能派生子进程，只杀直接子进程会留下孤儿）。
    """
    ctx = AppContext.get()
    coordinator = ctx.shutdown
    coordinator.register_process(process)
    try:
        start_time = time.time()
        # stdout=PIPE 保证非空；收窄以满足类型检查
        stdout = process.stdout
        if stdout is not None:
            for line in stdout:
                if coordinator.is_aborted():
                    break
                if prepend_timestamp:
                    ctx.logger.info(line.strip(), raw=True, prepend_timestamp=True)
                else:
                    ctx.logger.info(line.strip())
        process.wait()
    finally:
        coordinator.unregister_process(process)

    if coordinator.is_aborted():
        ctx.logger.warning(f"{tool_name} 同步已被中止（stop --fast）。")
    elif process.returncode != 0:
        ctx.logger.error(f"{tool_name} 同步失败，退出码: {process.returncode}")
    else:
        exec_time = time.time() - start_time
        ctx.logger.success(f"{tool_name} 同步完成，耗时: {fmt_timespan(exec_time)}。")


def _run_rclone_sync(
    source_root: str,
    dest_root: str,
    exclude_list: list[str],
    backup_dir: str | None,
    rclone_cmd: str,
) -> None:
    cmd = [rclone_cmd, "sync", source_root, dest_root]
    for ex in exclude_list:
        cmd.extend(["--exclude", ex])
    if backup_dir:
        cmd.extend(["--backup-dir", backup_dir])

    _run_sync_command(cmd, "rclone")


def _run_rsync_sync(
    source_root: str,
    dest_root: str,
    exclude_list: list[str],
    backup_dir: str | None,
    rsync_cmd: str,
) -> None:
    src = source_root if source_root.endswith("/") else source_root + "/"
    cmd = [rsync_cmd, "-av", "--delete", src, dest_root]
    for ex in exclude_list:
        cmd.extend(["--exclude", ex])
    if backup_dir:
        cmd.extend(["--backup", f"--backup-dir={backup_dir}"])

    _run_sync_command(cmd, "rsync", prepend_timestamp=True)


@register_handler("sync")
class SyncHandler(BaseTaskHandler):
    @override
    def execute(self) -> None:
        if not self.validate():
            return

        ctx = AppContext.get()

        if ctx.shutdown.should_stop_before_file():
            ctx.logger.info("收到停止指令，跳过该任务。")
            return

        print_task_header(self.task, ctx.logger)

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

        exclude = self.task.get("exclude", [])
        exclude_list: list[str] = [exclude] if isinstance(exclude, str) else exclude

        backup_enabled = self.task.get("create_backups", False)
        max_backups = self.task.get("max_backups", 0)

        if isinstance(self.dest_backend, SshDestBackend):
            self._execute_ssh_sync(
                self.dest_backend, tool, exclude_list, backup_enabled, max_backups
            )
        else:
            backup_dir = None
            if backup_enabled:
                if tool.supports_backup_dir:
                    backup_dir = self._setup_backup_dir(
                        self.dest_backend.root_path, max_backups, exclude_list
                    )
                else:
                    _warn_backup_unsupported()

            _sync_runners = {
                "rclone": _run_rclone_sync,
                "rsync": _run_rsync_sync,
            }
            _sync_runners[tool.name](
                self.source_root,
                self.dest_backend.root_path,
                exclude_list,
                backup_dir,
                tool.exec_path,
            )

    # ============================================================
    # SSH 远端同步
    # ============================================================

    def _execute_ssh_sync(
        self,
        backend: SshDestBackend,
        tool: SyncTool,
        exclude_list: list[str],
        backup_enabled: bool,
        max_backups: int,
    ) -> None:
        """执行 SSH 远端同步（rsync 或 rclone）。"""
        remote = backend.remote

        backup_dir = None
        if backup_enabled:
            if tool.supports_backup_dir:
                backup_dir = self._setup_ssh_backup_dir(backend, max_backups, exclude_list)
            else:
                _warn_backup_unsupported()

        if tool.name == "rsync":
            self._run_ssh_rsync(tool, remote, exclude_list, backup_dir)
        else:
            self._run_ssh_rclone(tool, remote, exclude_list, backup_dir)

    def _build_rsync_ssh_rsh(self, remote: SshRemote) -> str:
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

    def _run_ssh_rsync(
        self, tool: SyncTool, remote: SshRemote, exclude_list: list[str], backup_dir: str | None
    ) -> None:
        """通过 rsync + SSH 同步到远端主机。"""
        src = self.source_root
        src = src if src.endswith("/") else src + "/"
        dest = f"{remote.user}@{remote.host}:{self.dest_backend.root_path}"

        cmd = [tool.exec_path, "-av", "--delete"]
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

    def _run_ssh_rclone(
        self, tool: SyncTool, remote: SshRemote, exclude_list: list[str], backup_dir: str | None
    ) -> None:
        """通过 rclone SFTP 同步到远端主机。"""
        ctx = AppContext.get()

        # 使用 :sftp: 后端，通过命令行参数指定连接信息，无需预配置
        dest = f":sftp:{self.dest_backend.root_path}"

        cmd = [
            tool.exec_path,
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
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                env=env,
            )
        except Exception as e:  # noqa: BLE001
            ctx.logger.error(f"执行 rclone 时发生错误: {e}")
            return
        _consume_sync_process(process, "rclone")

    def _setup_ssh_backup_dir(
        self, backend: SshDestBackend, max_backups: int, exclude_list: list[str]
    ) -> str:
        """为 SSH 远端目标设置备份目录。

        在远端计算备份路径，清理旧备份，返回远端备份目录路径。
        """
        ctx = AppContext.get()

        backup_base = posixpath.join(backend.root_path, ".smart-archiver.backups")
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup_dir = posixpath.join(backup_base, timestamp)

        exclude_list.append(".smart-archiver.backups/")

        if max_backups > 0:
            try:
                all_dirs = backend.remote_list_dir(backup_base)
                if all_dirs and len(all_dirs) >= max_backups:
                    num_to_delete = len(all_dirs) - max_backups + 1
                    for d in all_dirs[:num_to_delete]:
                        remote_path = posixpath.join(backup_base, d)
                        ok = backend.remote_rmdir(remote_path)
                        if ok:
                            ctx.logger.info(f"已删除远端旧备份: {d}")
                        else:
                            ctx.logger.warning(f"删除远端旧备份失败: {d}")
            except Exception as e:  # noqa: BLE001
                ctx.logger.error(f"清理远端旧备份失败: {e}")

        return backup_dir

    # ============================================================
    # 本地备份目录
    # ============================================================

    def _setup_backup_dir(self, dest_root: str, max_backups: int, exclude_list: list[str]) -> str:
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
                        fs.delete_dir(b)
                        ctx.logger.info(f"已删除旧备份: {b}")
            except Exception as e:  # noqa: BLE001
                ctx.logger.error(f"清理旧备份失败: {e}")

        return backup_dir
