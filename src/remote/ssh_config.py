"""SSH 远端配置：数据类、解析函数与命令行构建工具。

仅用于 sync 模式 — SshRemoteConfig 作为 SshDestBackend 的载荷，
由 sync handler 读取后拼入 rsync/rclone 子进程命令。
"""

from dataclasses import dataclass, field
import shutil

from src.remote.factory import validate_remote_alias


# ============================================================
# 数据类
# ============================================================


@dataclass
class SshRemoteConfig:
    """单条 [[ssh_remotes]] 配置的解析结果。"""

    alias: str
    address: str
    user: str
    port: int = 22
    credential_file: str | None = None
    password_file: str | None = None

    # ---- 派生字段 ----
    _password: str | None = field(default=None, repr=False, init=False)

    def get_password(self) -> str | None:
        """读取并缓存密码文件内容（自动 strip）。"""
        if self._password is not None:
            return self._password
        if not self.password_file:
            return None
        try:
            with open(self.password_file, "r", encoding="utf-8") as f:
                self._password = f.read().strip()
        except Exception:
            self._password = ""
        return self._password


# ============================================================
# SSH 命令行片段构建
# ============================================================


def build_ssh_options(ssh: SshRemoteConfig) -> list[str]:
    """构建供 rsync -e 使用的 ssh 命令参数字段（不含 "ssh" 动词）。

    示例返回: ["-p", "2222", "-i", "/path/key", "-o", "StrictHostKeyChecking=no"]
    """
    opts: list[str] = []

    if ssh.port and ssh.port != 22:
        opts.extend(["-p", str(ssh.port)])

    if ssh.credential_file:
        opts.extend(["-i", ssh.credential_file])

    # 自动接受主机密钥（非交互环境必须）
    opts.extend(["-o", "StrictHostKeyChecking=no"])
    # 禁用已知主机文件写入，避免非交互环境下的阻塞
    opts.extend(["-o", "UserKnownHostsFile=/dev/null"])

    return opts


def build_ssh_dest(ssh: SshRemoteConfig, remote_path: str) -> str:
    """构建 rsync 使用的远端路径: user@host:/path"""
    return f"{ssh.user}@{ssh.address}:{remote_path}"


def resolve_sshpass() -> str | None:
    """返回 sshpass 可执行文件路径，未安装则返回 None。"""
    return shutil.which("sshpass")


def build_sshpass_env(ssh: SshRemoteConfig) -> dict[str, str] | None:
    """如果需要通过 sshpass 注入密码，返回带有 SSH_ASKPASS 的环境变量字典。

    方案：使用 sshpass 的标准方式 — 直接以 sshpass 作为前缀命令。
    此函数仅为调用方提供判断依据；实际拼接由 sync handler 完成。
    """
    password = ssh.get_password()
    if not password or not ssh.password_file:
        return None
    return {"SSHPASS": password}


def build_rclone_sftp_dest(ssh: SshRemoteConfig, remote_path: str) -> str:
    """构建 rclone on-the-fly SFTP 后端的远端目标字符串。

    格式: :sftp,host=<h>,port=<p>,user=<u>[,key_file=<k>][,pass=<pw>]:/path

    rclone 1.55+ 支持此语法，无需 rclone config 交互式配置。
    """
    params = [f"host={ssh.address}"]

    if ssh.port and ssh.port != 22:
        params.append(f"port={ssh.port}")

    params.append(f"user={ssh.user}")

    if ssh.credential_file:
        # rclone SFTP 后端要求 key_file 指向 PEM 格式私钥
        params.append(f"key_file={ssh.credential_file}")

    password = ssh.get_password()
    if password:
        # 密码可能含特殊字符 — 用单引号包裹，内部的单引号转义为 '\''
        escaped = password.replace("'", "'\\''")
        params.append(f"pass='{escaped}'")

    param_str = ",".join(params)
    return f":sftp,{param_str}:{remote_path}"


# ============================================================
# 配置解析
# ============================================================


def parse_ssh_remotes(config: dict) -> dict[str, SshRemoteConfig]:
    """从配置字典中解析 [[ssh_remotes]] 列表。

    Returns:
        {alias: SshRemoteConfig} — 别名到配置对象的映射。
        别名不合法或必填字段缺失的条目会被跳过并记录日志。
    """
    remotes: dict[str, SshRemoteConfig] = {}

    ssh_remotes: list[dict] = config.get("ssh_remotes", [])
    if not ssh_remotes:
        return remotes

    for entry in ssh_remotes:
        alias = str(entry.get("alias", "")).strip()
        address = str(entry.get("address", "")).strip()
        user = str(entry.get("user", "")).strip()

        # ---- 必填校验 ----
        if not alias or not address or not user:
            continue

        if not validate_remote_alias(alias):
            continue

        # ---- 端口 ----
        port = 22
        raw_port = entry.get("port")
        if raw_port is not None:
            try:
                port = int(raw_port)
            except (TypeError, ValueError):
                port = 22

        # ---- 凭据 ----
        credential_file = entry.get("credential_file")
        if credential_file is not None:
            credential_file = str(credential_file).strip() or None

        password_file = entry.get("password_file")
        if password_file is not None:
            password_file = str(password_file).strip() or None

        remotes[alias] = SshRemoteConfig(
            alias=alias,
            address=address,
            user=user,
            port=port,
            credential_file=credential_file,
            password_file=password_file,
        )

    return remotes
