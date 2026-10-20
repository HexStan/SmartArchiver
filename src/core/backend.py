import os
import re
import subprocess
from abc import ABC, abstractmethod
from typing import override

from src import fs
from src.app.context import AppContext
from src.fs.scan import DirEntry, StatResult
from src.remote.client import RemoteClient
from src.ssh.remotes import SshRemote, build_ssh_command, build_ssh_target


class DestBackend(ABC):
    def __init__(self, root_path: str) -> None:
        self.root_path = root_path

    def build_dest_path(self, rel_path: str) -> str:
        return os.path.normpath(os.path.join(self.root_path, rel_path))

    @abstractmethod
    def exists(self, path: str) -> bool: ...

    @abstractmethod
    def is_dir(self, path: str) -> bool: ...

    @abstractmethod
    def makedirs(self, path: str) -> None: ...

    @abstractmethod
    def copy_file(self, src_local_path: str, dest_path: str) -> None: ...

    @abstractmethod
    def move_file(self, src_local_path: str, dest_path: str) -> None: ...

    @abstractmethod
    def stat(self, path: str) -> StatResult: ...

    @abstractmethod
    def list_dir(self, path: str) -> list[DirEntry]: ...

    def get_unique_dest(self, dest_path: str) -> str:
        if not self.exists(dest_path):
            return dest_path

        directory = os.path.dirname(dest_path)
        filename = os.path.basename(dest_path)
        name, ext = os.path.splitext(filename)

        counter = 1
        while True:
            new_filename = f"{name}-{counter}{ext}"
            new_path = os.path.join(directory, new_filename)
            if not self.exists(new_path):
                return new_path
            counter += 1


# ============================================================
# 本地后端
# ============================================================


class LocalDestBackend(DestBackend):
    @override
    def exists(self, path: str) -> bool:
        return fs.path_exists(path)

    @override
    def is_dir(self, path: str) -> bool:
        return fs.is_dir(path)

    @override
    def makedirs(self, path: str) -> None:
        fs.ensure_dir(path)

    @override
    def copy_file(self, src_local_path: str, dest_path: str) -> None:
        fs.copy_file(src_local_path, dest_path)

    @override
    def move_file(self, src_local_path: str, dest_path: str) -> None:
        fs.move_file(src_local_path, dest_path)

    @override
    def stat(self, path: str) -> StatResult:
        return fs.get_stat(path)

    @override
    def list_dir(self, path: str) -> list[DirEntry]:
        return fs.list_dir(path)


# ============================================================
# HTTP 远程后端
# ============================================================


class RemoteDestBackend(DestBackend):
    def __init__(self, client: RemoteClient, remote_root: str) -> None:
        super().__init__(remote_root)
        self._client = client

    @override
    def build_dest_path(self, rel_path: str) -> str:
        rel_normalized = rel_path.replace("\\", "/")
        return f"{self.root_path.rstrip('/')}/{rel_normalized}"

    @override
    def exists(self, path: str) -> bool:
        return self._client.exists(path)

    @override
    def is_dir(self, path: str) -> bool:
        return self._client.is_dir(path)

    @override
    def makedirs(self, path: str) -> None:
        self._client.mkdir(path)

    @override
    def copy_file(self, src_local_path: str, dest_path: str) -> None:
        self._client.upload(src_local_path, dest_path)

    @override
    def move_file(self, src_local_path: str, dest_path: str) -> None:
        self._client.upload(src_local_path, dest_path)
        fs.delete_file(src_local_path)

    @override
    def stat(self, path: str) -> StatResult:
        result = self._client.stat(path)
        return StatResult(
            exists=result["exists"],
            size=result["size"],
            mtime=result["mtime"],
            is_dir=result["is_dir"],
        )

    @override
    def list_dir(self, path: str) -> list[DirEntry]:
        entries = self._client.list_dir(path)
        return [
            DirEntry(
                name=e["name"],
                is_dir=e["is_dir"],
                size=e["size"],
                mtime=e["mtime"],
            )
            for e in entries
        ]


# ============================================================
# SSH 远程后端
# ============================================================


def _shell_quote(s: str) -> str:
    """用单引号包裹字符串，安全地用于 shell 命令参数。"""
    return "'" + s.replace("'", "'\\''") + "'"


class SshDestBackend(DestBackend):
    """SSH 远端目标后端。

    主要用于 sync 模式，携带 SSH 连接参数供 rsync/rclone 使用。
    非 sync 模式的 per-file 操作（exists/copy/move 等）均抛出 NotImplementedError。

    额外提供远端目录管理方法供 SyncHandler 使用。
    """

    def __init__(self, remote: SshRemote, remote_root: str) -> None:
        super().__init__(remote_root)
        self.remote = remote

    @override
    def is_dir(self, path: str) -> bool:
        """通过 SSH 检测远端路径是否为目录。"""
        cmd = build_ssh_command(self.remote)
        target = build_ssh_target(self.remote)
        # 使用 test -d 检测，通过返回码区分
        full_cmd = cmd + [target, f"test -d {_shell_quote(path)}"]
        try:
            result = subprocess.run(
                full_cmd,
                check=False,
                capture_output=True,
                text=True,
                timeout=30,
            )
            return result.returncode == 0
        except subprocess.TimeoutExpired, FileNotFoundError, OSError:
            return False

    def remote_list_dir(self, path: str) -> list[str]:
        """通过 SSH 列出远端目录内容，返回名称列表并按字母排序。"""
        cmd = build_ssh_command(self.remote)
        target = build_ssh_target(self.remote)
        full_cmd = cmd + [target, f"ls -1d {_shell_quote(path)}/*/ 2>/dev/null"]
        try:
            result = subprocess.run(
                full_cmd,
                check=False,
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode == 0:
                entries = []
                for line in result.stdout.strip().split("\n"):
                    line = line.strip()
                    if line:
                        entries.append(os.path.basename(line.rstrip("/")))
                entries.sort()
                return entries
        except subprocess.TimeoutExpired, FileNotFoundError, OSError:
            pass
        return []

    def remote_rmdir(self, path: str) -> bool:
        """通过 SSH 递归删除远端目录。"""
        cmd = build_ssh_command(self.remote)
        target = build_ssh_target(self.remote)
        full_cmd = cmd + [target, f"rm -rf {_shell_quote(path)}"]
        try:
            result = subprocess.run(
                full_cmd,
                check=False,
                capture_output=True,
                text=True,
                timeout=60,
            )
            return result.returncode == 0
        except subprocess.TimeoutExpired, FileNotFoundError, OSError:
            return False

    # ---- 非 sync 模式不支持的方法 ----

    @override
    def exists(self, path: str) -> bool:
        raise NotImplementedError

    @override
    def makedirs(self, path: str) -> None:
        raise NotImplementedError

    @override
    def copy_file(self, src_local_path: str, dest_path: str) -> None:
        raise NotImplementedError

    @override
    def move_file(self, src_local_path: str, dest_path: str) -> None:
        raise NotImplementedError

    @override
    def stat(self, path: str) -> StatResult:
        raise NotImplementedError

    @override
    def list_dir(self, path: str) -> list[DirEntry]:
        raise NotImplementedError


# ============================================================
# 创建后端
# ============================================================

_REMOTE_DEST_PATTERN = re.compile(r"^\{([a-z]+):([a-zA-Z0-9\-_]+)\}\?(.*)")

# Windows 盘符绝对路径：D:/Backup、D:\Backup，或裸盘符根 D:/ 与 D:
_WIN_REMOTE_PATH_PATTERN = re.compile(r"^[A-Za-z]:([\\/].*)?$")


def _normalize_http_remote_path(remote_path: str) -> str:
    """归一化 HTTP 远端根路径。

    POSIX 路径统一为单一前导 /；Windows 盘符路径（如 D:/Backup）本身
    已是绝对路径，不能补前导 /——否则 Windows 服务端的 normpath 会把
    /D:/Backup 变成当前盘根下的非法路径。盘符路径中的反斜杠统一为
    正斜杠，与 RemoteDestBackend.build_dest_path 的拼接规则保持一致。
    """
    if _WIN_REMOTE_PATH_PATTERN.match(remote_path):
        return remote_path.replace("\\", "/").rstrip("/")
    return "/" + remote_path.lstrip("/")


def create_dest_backend(
    dest_root: str,
    remote_clients: dict[str, RemoteClient],
    ssh_remotes: dict[str, SshRemote] | None = None,
) -> DestBackend:
    if dest_root and isinstance(dest_root, str):
        m = _REMOTE_DEST_PATTERN.match(dest_root)
        if m:
            remote_type = m.group(1)
            alias = m.group(2)
            remote_path = m.group(3)

            if remote_type == "http":
                client = remote_clients.get(alias)
                if client is not None:
                    return RemoteDestBackend(client, _normalize_http_remote_path(remote_path))
                ctx = AppContext.get()
                ctx.logger.error(
                    f"HTTP 远端别名 '{alias}' 未在 http_remotes 中配置，fallback 到本地路径"
                )
                return LocalDestBackend(dest_root)

            elif remote_type == "ssh":
                ssh_dict = ssh_remotes or {}
                ssh_remote = ssh_dict.get(alias)
                if ssh_remote is not None:
                    # SSH 远端为 POSIX 语义（rsync / rclone over SSH），统一补前导 /
                    return SshDestBackend(ssh_remote, "/" + remote_path.lstrip("/"))
                ctx = AppContext.get()
                ctx.logger.error(
                    f"SSH 远端别名 '{alias}' 未在 ssh_remotes 中配置，fallback 到本地路径"
                )
                return LocalDestBackend(dest_root)

            else:
                ctx = AppContext.get()
                ctx.logger.error(
                    f"未知的远端类型 '{remote_type}'（dest 路径: {dest_root}），"
                    f"仅支持 http 和 ssh，fallback 到本地路径"
                )
                return LocalDestBackend(dest_root)

    return LocalDestBackend(dest_root)
