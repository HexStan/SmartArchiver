import os
import subprocess
from abc import ABC, abstractmethod

from src import fs_ops
from src.ssh.config import SshRemote, build_ssh_command, build_ssh_target


class DestBackend(ABC):
    def __init__(self, root_path):
        self.root_path = root_path

    def build_dest_path(self, rel_path):
        return os.path.normpath(os.path.join(self.root_path, rel_path))

    @abstractmethod
    def exists(self, path):
        pass

    @abstractmethod
    def is_dir(self, path):
        pass

    @abstractmethod
    def remove_file(self, path):
        pass

    @abstractmethod
    def remove_dir(self, path):
        pass

    @abstractmethod
    def makedirs(self, path):
        pass

    @abstractmethod
    def copy_file(self, src_local_path, dest_path):
        pass

    @abstractmethod
    def move_file(self, src_local_path, dest_path):
        pass

    @abstractmethod
    def stat(self, path):
        pass

    @abstractmethod
    def list_dir(self, path):
        pass

    def get_unique_dest(self, dest_path):
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


class LocalDestBackend(DestBackend):
    def exists(self, path):
        return fs_ops.path_exists(path)

    def is_dir(self, path):
        return fs_ops.is_dir(path)

    def remove_file(self, path):
        fs_ops.delete_file(path)

    def remove_dir(self, path):
        fs_ops.delete_dir(path)

    def makedirs(self, path):
        fs_ops.ensure_dir(path)

    def copy_file(self, src_local_path, dest_path):
        fs_ops.copy_file(src_local_path, dest_path)

    def move_file(self, src_local_path, dest_path):
        fs_ops.move_file(src_local_path, dest_path)

    def stat(self, path):
        return fs_ops.get_stat(path)

    def list_dir(self, path):
        return fs_ops.list_dir(path)


class RemoteDestBackend(DestBackend):
    def __init__(self, client, remote_root):
        super().__init__(remote_root)
        self._client = client

    def build_dest_path(self, rel_path):
        rel_normalized = rel_path.replace("\\", "/")
        return f"{self.root_path.rstrip('/')}/{rel_normalized}"

    def exists(self, path):
        return self._client.exists(path)

    def is_dir(self, path):
        return self._client.is_dir(path)

    def remove_file(self, path):
        self._client.delete_file(path)

    def remove_dir(self, path):
        self._client.delete_dir(path)

    def makedirs(self, path):
        self._client.mkdir(path)

    def copy_file(self, src_local_path, dest_path):
        self._client.upload(src_local_path, dest_path)

    def move_file(self, src_local_path, dest_path):
        self._client.upload(src_local_path, dest_path)
        fs_ops.delete_file(src_local_path)

    def stat(self, path):
        result = self._client.stat(path)
        return fs_ops.StatResult(
            exists=result["exists"],
            size=result["size"],
            mtime=result["mtime"],
            is_dir=result["is_dir"],
        )

    def list_dir(self, path):
        entries = self._client.list_dir(path)
        return [
            fs_ops.DirEntry(
                name=e["name"],
                is_dir=e["is_dir"],
                size=e["size"],
                mtime=e["mtime"],
            )
            for e in entries
        ]


class SshDestBackend(DestBackend):
    """SSH 远端目标后端。

    主要用于 sync 模式，携带 SSH 连接参数供 rsync/rclone 使用。
    非 sync 模式的 per-file 操作（exists/copy/move 等）均抛出 NotImplementedError。

    额外提供远端目录管理方法供 SyncHandler 使用。
    """

    def __init__(self, remote: SshRemote, remote_root: str):
        super().__init__(remote_root)
        self.remote = remote

    def is_dir(self, path):
        """通过 SSH 检测远端路径是否为目录。"""
        cmd = build_ssh_command(self.remote)
        target = build_ssh_target(self.remote)
        # 使用 test -d 检测，通过返回码区分
        full_cmd = cmd + [target, f"test -d {_shell_quote(path)}"]
        try:
            result = subprocess.run(
                full_cmd,
                capture_output=True,
                text=True,
                timeout=30,
            )
            return result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
            return False

    def remote_list_dir(self, path):
        """通过 SSH 列出远端目录内容，返回名称列表并按字母排序。"""
        cmd = build_ssh_command(self.remote)
        target = build_ssh_target(self.remote)
        full_cmd = cmd + [target, f"ls -1d {_shell_quote(path)}/*/ 2>/dev/null"]
        try:
            result = subprocess.run(
                full_cmd,
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
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
            pass
        return []

    def remote_rmdir(self, path):
        """通过 SSH 递归删除远端目录。"""
        cmd = build_ssh_command(self.remote)
        target = build_ssh_target(self.remote)
        full_cmd = cmd + [target, f"rm -rf {_shell_quote(path)}"]
        try:
            result = subprocess.run(
                full_cmd,
                capture_output=True,
                text=True,
                timeout=60,
            )
            return result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
            return False

    # ---- 非 sync 模式不支持的方法 ----

    def exists(self, path):
        raise NotImplementedError

    def remove_file(self, path):
        raise NotImplementedError

    def remove_dir(self, path):
        raise NotImplementedError

    def makedirs(self, path):
        raise NotImplementedError

    def copy_file(self, src_local_path, dest_path):
        raise NotImplementedError

    def move_file(self, src_local_path, dest_path):
        raise NotImplementedError

    def stat(self, path):
        raise NotImplementedError

    def list_dir(self, path):
        raise NotImplementedError


def _shell_quote(s: str) -> str:
    """用单引号包裹字符串，安全地用于 shell 命令参数。"""
    return "'" + s.replace("'", "'\\''") + "'"


def create_dest_backend(dest_root, remote_clients, ssh_remotes=None):
    if dest_root and isinstance(dest_root, str):
        # 1. 先检查 HTTP remote 命名空间
        for alias, client in remote_clients.items():
            prefix = f"{{{alias}}}?"
            if dest_root.startswith(prefix):
                remote_path = dest_root[len(prefix) :]
                remote_path = "/" + remote_path.lstrip("/")
                return RemoteDestBackend(client, remote_path)

        # 2. 再检查 SSH remote 命名空间（与 HTTP 隔离）
        ssh_dict = ssh_remotes or {}
        for alias, ssh_remote in ssh_dict.items():
            prefix = f"{{{alias}}}?"
            if dest_root.startswith(prefix):
                remote_path = dest_root[len(prefix) :]
                remote_path = "/" + remote_path.lstrip("/")
                return SshDestBackend(ssh_remote, remote_path)

    return LocalDestBackend(dest_root)
