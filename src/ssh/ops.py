"""SSH 远端目录操作与 rsync 连接参数构建。

本模块是 ssh 包唯一的远端执行层（I/O）：remotes.py 负责纯配置解析
与命令参数构建，实际执行统一经由本模块的 run_ssh_command。sync 模式
经由本模块探测远端目录、管理备份轮转并构造 rsync 的连接参数；逐文件
传输不经由 SSH——sync 的文件传输由 rsync/rclone 整树完成。
"""

import os
import shlex
import subprocess

from src.ssh.remotes import SshRemote, build_ssh_command, build_ssh_shell_args


def run_ssh_command(
    remote: SshRemote, remote_command: str, timeout: int = 30
) -> tuple[int, str, str]:
    """在远端主机上执行 shell 命令，返回 (returncode, stdout, stderr)。

    进程启动失败或超时返回 (-1, "", 中文失败原因)，不抛出异常。
    """
    cmd = build_ssh_shell_args(remote, remote_command)
    try:
        proc = subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return proc.returncode, proc.stdout.strip(), proc.stderr.strip()
    except subprocess.TimeoutExpired:
        return -1, "", f"SSH 命令执行超时（>{timeout}s）"
    except OSError as e:
        return -1, "", f"无法启动 SSH 进程：{e}"


def is_remote_dir(remote: SshRemote, path: str) -> bool:
    """检测远端路径是否为目录（test -d，经返回码区分）。

    进程启动失败或超时视为否。
    """
    returncode, _, _ = run_ssh_command(remote, f"test -d {shlex.quote(path)}", timeout=30)
    return returncode == 0


def list_remote_dirs(remote: SshRemote, path: str) -> list[str]:
    """列出远端目录的一级子目录名，按字母排序；失败返回空列表。"""
    returncode, stdout, _ = run_ssh_command(
        remote, f"ls -1d {shlex.quote(path)}/*/ 2>/dev/null", timeout=30
    )
    if returncode != 0:
        return []
    entries = []
    for line in stdout.split("\n"):
        line = line.strip()
        if line:
            entries.append(os.path.basename(line.rstrip("/")))
    entries.sort()
    return entries


def remove_remote_tree(remote: SshRemote, path: str) -> bool:
    """递归删除远端目录（rm -rf），返回是否成功。"""
    returncode, _, _ = run_ssh_command(remote, f"rm -rf {shlex.quote(path)}", timeout=60)
    return returncode == 0


def build_rsync_ssh_rsh(remote: SshRemote) -> str:
    """构建 rsync 的 -e 参数值（SSH 远程 shell 命令字符串）。

    SSH 选项策略（含 sshpass 包装）的单一来源是 remotes.build_ssh_command，
    此处仅做列表到字符串的形态转换。
    """
    return " ".join(build_ssh_command(remote))
