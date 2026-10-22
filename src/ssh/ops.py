"""SSH 远端目录操作与 rsync 连接参数构建。

与 remotes.py 的命令构建辅助同层：所有函数只依赖 SshRemote 数据类，
不依赖任何上层包。sync 模式经由本模块探测远端目录、管理备份轮转并
构造 rsync 的连接参数；逐文件传输不经由 SSH——sync 的文件传输由
rsync/rclone 整树完成。
"""

import os
import subprocess

from src.ssh.remotes import SshRemote, build_ssh_command, build_ssh_target


def _shell_quote(s: str) -> str:
    """用单引号包裹字符串，安全地用于 shell 命令参数。"""
    return "'" + s.replace("'", "'\\''") + "'"


def _run_remote(
    remote: SshRemote, remote_command: str, timeout: int
) -> subprocess.CompletedProcess[str] | None:
    """在远端执行 shell 命令；进程启动失败或超时返回 None。"""
    cmd = build_ssh_command(remote)
    cmd.extend([build_ssh_target(remote), remote_command])
    try:
        return subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired, FileNotFoundError, OSError:
        return None


def is_remote_dir(remote: SshRemote, path: str) -> bool:
    """检测远端路径是否为目录（test -d，经返回码区分）。

    进程启动失败或超时视为否。
    """
    result = _run_remote(remote, f"test -d {_shell_quote(path)}", timeout=30)
    return result is not None and result.returncode == 0


def list_remote_dirs(remote: SshRemote, path: str) -> list[str]:
    """列出远端目录的一级子目录名，按字母排序；失败返回空列表。"""
    result = _run_remote(remote, f"ls -1d {_shell_quote(path)}/*/ 2>/dev/null", timeout=30)
    if result is None or result.returncode != 0:
        return []
    entries = []
    for line in result.stdout.strip().split("\n"):
        line = line.strip()
        if line:
            entries.append(os.path.basename(line.rstrip("/")))
    entries.sort()
    return entries


def remove_remote_tree(remote: SshRemote, path: str) -> bool:
    """递归删除远端目录（rm -rf），返回是否成功。"""
    result = _run_remote(remote, f"rm -rf {_shell_quote(path)}", timeout=60)
    return result is not None and result.returncode == 0


def build_rsync_ssh_rsh(remote: SshRemote) -> str:
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
