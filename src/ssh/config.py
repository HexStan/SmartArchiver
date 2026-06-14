"""SSH 远端主机配置解析与 SSH 命令构建辅助函数。"""

import os
import re
import subprocess
from dataclasses import dataclass

_ALIAS_PATTERN = re.compile(r"^[a-zA-Z0-9\-_]+$")


def _validate_alias(alias: str) -> bool:
    """验证别名是否仅包含字母、数字、连字符和下划线。"""
    return bool(_ALIAS_PATTERN.match(alias))


# ============================================================
# 数据类
# ============================================================


@dataclass
class SshRemote:
    """SSH 远端主机配置。"""

    alias: str
    host: str
    user: str
    port: int = 22
    key_file: str | None = None
    password_file: str | None = None


# ============================================================
# 配置解析
# ============================================================


def parse_ssh_remote_config(config: dict) -> dict[str, SshRemote]:
    """从顶级配置字典中解析 [[ssh_remotes]] 配置块。

    与 `parse_remote_config`（http_remotes）完全隔离，
    允许两个命名空间中存在同名 alias。

    Returns:
        dict[str, SshRemote]: alias → SshRemote 映射。
    """
    remotes: dict[str, SshRemote] = {}
    ssh_remotes = config.get("ssh_remotes", [])
    if not ssh_remotes:
        return remotes

    for entry in ssh_remotes:
        alias = str(entry.get("alias", "")).strip()
        host = str(entry.get("host", "")).strip()
        user = str(entry.get("user", "")).strip()

        # 必填字段校验
        if not alias or not host or not user:
            continue

        if not _validate_alias(alias):
            continue

        # 端口：选填，默认 22
        port = 22
        raw_port = entry.get("port")
        if raw_port is not None:
            try:
                port = int(raw_port)
            except (TypeError, ValueError):
                port = 22

        # 凭据文件：选填
        key_file = entry.get("key_file")
        if key_file is not None:
            key_file = str(key_file).strip()
            if not key_file:
                key_file = None
            elif not os.path.isfile(key_file):
                # 文件不存在则忽略，后续操作时会由 SSH 自行报错
                pass

        # 明文密码文件：选填
        password_file = entry.get("password_file")
        if password_file is not None:
            password_file = str(password_file).strip()
            if not password_file:
                password_file = None
            elif not os.path.isfile(password_file):
                pass

        remotes[alias] = SshRemote(
            alias=alias,
            host=host,
            user=user,
            port=port,
            key_file=key_file,
            password_file=password_file,
        )

    return remotes


# ============================================================
# SSH 命令构建
# ============================================================


def build_ssh_command(remote: SshRemote) -> list[str]:
    """为给定的 SSH 远端配置构建 `ssh` 命令参数列表。

    返回的列表可直接用于 subprocess.Popen 的 cmd 参数：
        ssh -p PORT -i KEY_FILE -o StrictHostKeyChecking=no ... user@host

    如果配置了密码文件，则使用 sshpass 包装：
        sshpass -f PASSWORD_FILE ssh ...

    Returns:
        list[str]: 包括 "ssh" 本身在内的命令参数列表。
    """
    ssh_args = []

    # 基础选项
    ssh_args.extend(
        [
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-o",
            "LogLevel=ERROR",
        ]
    )

    # 端口
    if remote.port != 22:
        ssh_args.extend(["-p", str(remote.port)])

    # 密钥文件
    if remote.key_file:
        ssh_args.extend(["-i", remote.key_file])

    # 构建完整命令
    if remote.password_file:
        cmd = ["sshpass", "-f", remote.password_file, "ssh"] + ssh_args
    else:
        cmd = ["ssh"] + ssh_args

    return cmd


def build_ssh_target(remote: SshRemote) -> str:
    """构建 user@host 格式的 SSH 目标字符串。"""
    return f"{remote.user}@{remote.host}"


def build_ssh_shell_args(remote: SshRemote, remote_command: str) -> list[str]:
    """构建执行远端 shell 命令的完整 SSH 参数列表。

    Returns:
        list[str]: ["ssh", ..., "user@host", "remote_command"]
    """
    cmd = build_ssh_command(remote)
    cmd.append(build_ssh_target(remote))
    cmd.append(remote_command)
    return cmd


def run_ssh_command(
    remote: SshRemote, remote_command: str, timeout: int = 30
) -> tuple[int, str, str]:
    """在远端主机上执行 shell 命令并返回结果。

    Args:
        remote: SSH 远端配置。
        remote_command: 要在远端执行的命令。
        timeout: 超时秒数。

    Returns:
        tuple[int, str, str]: (returncode, stdout, stderr)
    """
    cmd = build_ssh_shell_args(remote, remote_command)

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return proc.returncode, proc.stdout.strip(), proc.stderr.strip()
    except subprocess.TimeoutExpired:
        return -1, "", "SSH command timed out"
    except FileNotFoundError:
        return -1, "", f"SSH command not found: {cmd[0]}"
    except Exception as e:
        return -1, "", str(e)
