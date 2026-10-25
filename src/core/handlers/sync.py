import os
import posixpath
import shutil
import subprocess
import sys
import time
from datetime import datetime
from typing import NamedTuple, override

from src import fs
from src.app.context import AppContext
from src.app.paths import bundled_rclone_path
from src.core import dest
from src.core.handlers.base import BaseTaskHandler
from src.core.registry import register_handler
from src.core.task_config import SyncTaskConfig
from src.core.task_plan import TaskSpec
from src.output.presentation import fmt_timespan
from src.ssh import ops
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


def _resolve_sync_tool(tool: str) -> SyncTool | None:
    """根据配置值和平台解析要使用的同步工具。

    tool 取值合法性已由配置模型在加载期收口，此处只做可用性解析
    （外部条件）。解析优先级：
    1. tool = "auto"（默认）或 "rclone"：打包版内置的 rclone → 系统 PATH 中的 rclone；
    2. tool = "auto" 且 rclone 不可用：POSIX 平台回退系统 rsync（Windows 不回退）；
    3. tool = "rsync"：系统 PATH 中的 rsync。

    Returns:
        SyncTool | None: 解析结果；解析失败时已记录错误日志，返回 None。
    """
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


def _run_sync_command(cmd: list[str], tool_name: str, prepend_timestamp: bool = False) -> bool:
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
        return False
    return _consume_sync_process(process, tool_name, prepend_timestamp)


def _consume_sync_process(
    process: subprocess.Popen[str], tool_name: str, prepend_timestamp: bool = False
) -> bool:
    """读取同步工具的输出直至其退出；返回本轮同步是否正常完成。

    进程在停止协调器登记，``stop --fast`` 时由协调器终止整棵进程树
    （rsync/rclone 可能派生子进程，只杀直接子进程会留下孤儿）。
    """
    ctx = AppContext.get()
    coordinator = ctx.lifecycle
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
        # stop --fast：外部进程已被整树终止，按"中断 = 未完成"定性
        return False
    if process.returncode != 0:
        ctx.logger.error(f"{tool_name} 同步失败，退出码: {process.returncode}")
        return False
    exec_time = time.time() - start_time
    ctx.logger.success(f"{tool_name} 同步完成，耗时: {fmt_timespan(exec_time)}。")
    return True


def _run_rclone_sync(
    source_root: str,
    dest_root: str,
    exclude_list: list[str],
    exclude_from: list[str],
    backup_dir: str | None,
    rclone_cmd: str,
) -> bool:
    cmd = [rclone_cmd, "sync", source_root, dest_root]
    for ex in exclude_list:
        cmd.extend(["--exclude", ex])
    for path in exclude_from:
        cmd.extend(["--exclude-from", path])
    if backup_dir:
        cmd.extend(["--backup-dir", backup_dir])

    return _run_sync_command(cmd, "rclone")


def _run_rsync_sync(
    source_root: str,
    dest_root: str,
    exclude_list: list[str],
    exclude_from: list[str],
    backup_dir: str | None,
    rsync_cmd: str,
) -> bool:
    src = source_root if source_root.endswith("/") else source_root + "/"
    cmd = [rsync_cmd, "-av", "--delete", src, dest_root]
    for ex in exclude_list:
        cmd.extend(["--exclude", ex])
    for path in exclude_from:
        cmd.append(f"--exclude-from={path}")
    if backup_dir:
        cmd.extend(["--backup", f"--backup-dir={backup_dir}"])

    return _run_sync_command(cmd, "rsync", prepend_timestamp=True)


@register_handler("sync")
class SyncHandler(BaseTaskHandler):
    @override
    def __init__(self, spec: TaskSpec, now: float | None = None) -> None:
        super().__init__(spec, now)
        # 注册表分派保证（task_config.TASK_MODELS 与 registry 由同一组模式名驱动）
        assert isinstance(self.task, SyncTaskConfig), "sync 模式的配置模型分派错误"
        self.config: SyncTaskConfig = self.task

    @override
    def _execute(self) -> bool:
        ctx = AppContext.get()

        if ctx.lifecycle.should_stop_before_file():
            return False

        if not os.path.exists(self.source_root):
            ctx.logger.error(f"源目录不存在: {self.source_root}")
            return False

        # exclude_from 规则文件是外部条件（与源目录存在性同级）：缺失即 fail fast，
        # 不发起同步——工具侧对缺失文件的报错信息不友好
        for path in self.config.exclude_from:
            if not os.path.isfile(path):
                ctx.logger.error(f"exclude_from 文件不存在: {path}")
                return False

        # 加载期 validate_dest 已保证 kind 组合合法（sync：本地或 SSH），
        # 此处解析只为取得 SSH 连接参数与远端根路径
        parsed = dest.parse_dest(self.dest_root)

        if parsed.kind == dest.SSH:
            remote = ctx.ssh_remotes.get(parsed.alias)
            if remote is None:
                # 防御：加载期已校验别名存在，正常不会到达
                ctx.logger.error(
                    f"SSH 远端别名 '{parsed.alias}' 未在 ssh_remotes 中配置，跳过该任务。"
                )
                return False
            dest_ready = ops.is_remote_dir(remote, parsed.path)
        else:
            remote = None
            dest_ready = dest.is_dir(self.dest_root)

        if not dest_ready:
            ctx.logger.critical("!!! 致命错误: 目标目录不存在 !!!")
            return False

        tool = _resolve_sync_tool(self.config.tool)
        if tool is None:
            return False

        exclude_list = self.config.exclude
        exclude_from = self.config.exclude_from

        backup_enabled = self.config.create_backups
        max_backups = self.config.max_backups

        if remote is not None:
            return self._execute_ssh_sync(
                remote, parsed.path, tool, exclude_list, exclude_from, backup_enabled, max_backups
            )

        backup_dir = None
        if backup_enabled:
            if tool.supports_backup_dir:
                backup_dir = self._setup_backup_dir(self.dest_root, max_backups, exclude_list)
            else:
                _warn_backup_unsupported()

        _sync_runners = {
            "rclone": _run_rclone_sync,
            "rsync": _run_rsync_sync,
        }
        return _sync_runners[tool.name](
            self.source_root,
            self.dest_root,
            exclude_list,
            exclude_from,
            backup_dir,
            tool.exec_path,
        )

    # ============================================================
    # SSH 远端同步
    # ============================================================

    def _execute_ssh_sync(
        self,
        remote: SshRemote,
        remote_root: str,
        tool: SyncTool,
        exclude_list: list[str],
        exclude_from: list[str],
        backup_enabled: bool,
        max_backups: int,
    ) -> bool:
        """执行 SSH 远端同步（rsync 或 rclone）。"""
        backup_dir = None
        if backup_enabled:
            if tool.supports_backup_dir:
                backup_dir = self._setup_ssh_backup_dir(
                    remote, remote_root, max_backups, exclude_list
                )
            else:
                _warn_backup_unsupported()

        if tool.name == "rsync":
            return self._run_ssh_rsync(
                tool, remote, remote_root, exclude_list, exclude_from, backup_dir
            )
        return self._run_ssh_rclone(
            tool, remote, remote_root, exclude_list, exclude_from, backup_dir
        )

    def _run_ssh_rsync(
        self,
        tool: SyncTool,
        remote: SshRemote,
        remote_root: str,
        exclude_list: list[str],
        exclude_from: list[str],
        backup_dir: str | None,
    ) -> bool:
        """通过 rsync + SSH 同步到远端主机。"""
        src = self.source_root
        src = src if src.endswith("/") else src + "/"
        dest_target = f"{remote.user}@{remote.host}:{remote_root}"

        cmd = [tool.exec_path, "-av", "--delete"]
        rsh = ops.build_rsync_ssh_rsh(remote)
        cmd.extend(["-e", rsh])

        for ex in exclude_list:
            cmd.extend(["--exclude", ex])
        for path in exclude_from:
            cmd.append(f"--exclude-from={path}")

        if backup_dir:
            # --backup-dir 只接受目录路径，不支持 user@host: 前缀。
            # rsync 已经知道备份放在接收端（远端主机），直接传绝对路径即可。
            cmd.extend(["--backup", f"--backup-dir={backup_dir}"])

        cmd.extend([src, dest_target])
        return _run_sync_command(cmd, "rsync", prepend_timestamp=True)

    def _run_ssh_rclone(
        self,
        tool: SyncTool,
        remote: SshRemote,
        remote_root: str,
        exclude_list: list[str],
        exclude_from: list[str],
        backup_dir: str | None,
    ) -> bool:
        """通过 rclone SFTP 同步到远端主机。"""
        ctx = AppContext.get()

        # 使用 :sftp: 后端，通过命令行参数指定连接信息，无需预配置
        dest_target = f":sftp:{remote_root}"

        cmd = [
            tool.exec_path,
            "sync",
            self.source_root,
            dest_target,
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
        for path in exclude_from:
            cmd.extend(["--exclude-from", path])

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
                return False

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
            return False
        return _consume_sync_process(process, "rclone")

    def _setup_ssh_backup_dir(
        self, remote: SshRemote, remote_root: str, max_backups: int, exclude_list: list[str]
    ) -> str:
        """为 SSH 远端目标设置备份目录。

        在远端计算备份路径，清理旧备份，返回远端备份目录路径。
        """
        ctx = AppContext.get()

        backup_base = posixpath.join(remote_root, ".smart-archiver.backups")
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup_dir = posixpath.join(backup_base, timestamp)

        exclude_list.append(".smart-archiver.backups/")

        if max_backups > 0:
            try:
                all_dirs = ops.list_remote_dirs(remote, backup_base)
                if all_dirs and len(all_dirs) >= max_backups:
                    num_to_delete = len(all_dirs) - max_backups + 1
                    for d in all_dirs[:num_to_delete]:
                        remote_path = posixpath.join(backup_base, d)
                        ctx.logger.info(f"正在删除远端旧备份: {d}")
                        ok = ops.remove_remote_tree(remote, remote_path)
                        if not ok:
                            ctx.logger.warning("删除远端旧备份失败")
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
                        # 日志事前输出：即将删除时打印；失败时由外层错误行保留异常信息
                        ctx.logger.info(f"正在删除旧备份: {b}")
                        fs.delete_dir(b)
            except Exception as e:  # noqa: BLE001
                ctx.logger.error(f"清理旧备份失败: {e}")

        return backup_dir
