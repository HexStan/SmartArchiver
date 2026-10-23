"""SSH 远端主机配置解析与 SSH 命令构建辅助函数。

本模块为纯逻辑（无 I/O）：只解析配置与构建命令参数；远端命令的
实际执行见 ops.run_ssh_command。
"""

from typing import TYPE_CHECKING, Annotated, Any

from pydantic import BaseModel, Field, StringConstraints, ValidationError, field_validator

from src.validation import RemoteAlias, format_validation_error

if TYPE_CHECKING:
    from src.output.logger import Logger


class SshRemote(BaseModel):
    """SSH 远端主机配置。

    alias/host/user 必填并自动去除首尾空白；port 限 1-65535（默认 22）；
    凭据文件路径去除首尾空白，空串视为未配置（None）。
    """

    alias: RemoteAlias
    host: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
    user: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
    port: int = Field(default=22, ge=1, le=65535)
    key_file: str | None = None
    password_file: str | None = None

    @field_validator("key_file", "password_file", mode="before")
    @classmethod
    def _strip_credential_path(cls, value: object) -> object:
        """凭据文件路径去空白；空串归 None（未配置）。非字符串交由字段类型校验报错。"""
        if isinstance(value, str):
            value = value.strip()
            return value or None
        return value


def parse_ssh_remote_config(config: dict[str, Any], logger: Logger) -> dict[str, SshRemote]:
    """从顶级配置字典中解析 [[ssh_remotes]] 配置块。

    与 `parse_remote_config`（http_remotes）完全隔离，
    允许两个命名空间中存在同名 alias。

    逐条目容错与重复 alias 语义同 http_remotes（warning 日志 + 跳过、
    先到先得）。

    Returns:
        dict[str, SshRemote]: alias → SshRemote 映射。
    """
    remotes: dict[str, SshRemote] = {}
    entries = config.get("ssh_remotes", [])
    if not entries:
        return remotes
    if not isinstance(entries, list):
        logger.warning("ssh_remotes 配置应为条目数组，已忽略全部条目。")
        return remotes

    for index, entry in enumerate(entries):
        try:
            item = SshRemote.model_validate(entry)
        except ValidationError as e:
            logger.warning(
                f"ssh_remotes 第 {index + 1} 个条目无效，已忽略：{format_validation_error(e)}"
            )
            continue

        if item.alias in remotes:
            logger.warning(
                f"ssh_remotes 中别名 '{item.alias}' 重复"
                f"（第 {index + 1} 个条目），保留先出现的条目。"
            )
            continue

        remotes[item.alias] = item

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
