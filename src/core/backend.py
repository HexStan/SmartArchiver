import os
from abc import ABC, abstractmethod

from src import fs_ops


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
    """SSH 远端标记型后端 — 仅用于 sync 模式。

    不实现 DestBackend 的具体操作方法；sync handler 通过 isinstance
    识别该类型后直接从 ssh_config 读取连接信息来构建 rsync/rclone 命令。
    其他模式使用 SSH 远端时会在调用未实现方法时自然报错。
    """

    def __init__(self, ssh_config, remote_root):
        super().__init__(remote_root)
        self.ssh_config = ssh_config

    def _raise_not_supported(self, method_name):
        raise NotImplementedError(
            f"SSH 远端不支持 {method_name} 操作。"
            f"SSH 远端仅用于 sync 模式，请使用 rsync 或 rclone 进行同步。"
        )

    def exists(self, path):
        self._raise_not_supported("exists")

    def is_dir(self, path):
        self._raise_not_supported("is_dir")

    def remove_file(self, path):
        self._raise_not_supported("remove_file")

    def remove_dir(self, path):
        self._raise_not_supported("remove_dir")

    def makedirs(self, path):
        self._raise_not_supported("makedirs")

    def copy_file(self, src_local_path, dest_path):
        self._raise_not_supported("copy_file")

    def move_file(self, src_local_path, dest_path):
        self._raise_not_supported("move_file")

    def stat(self, path):
        self._raise_not_supported("stat")

    def list_dir(self, path):
        self._raise_not_supported("list_dir")


def _match_remote_prefix(dest_root, remotes, backend_ctor):
    """尝试将 dest_root 与 remotes 中的别名前缀匹配。

    Args:
        dest_root: 目标路径字符串，可能为 ``{alias}?/path`` 格式。
        remotes: ``{alias: config_or_client}`` 字典。
        backend_ctor: 单参数工厂，接收 ``(remote_config, remote_path)``。

    Returns:
        DestBackend | None: 匹配成功返回对应 backend，否则返回 None。
    """
    if not remotes:
        return None
    for alias, cfg in remotes.items():
        prefix = f"{{{alias}}}?"
        if dest_root.startswith(prefix):
            remote_path = "/" + dest_root[len(prefix) :].lstrip("/")
            return backend_ctor(cfg, remote_path)
    return None


def create_dest_backend(dest_root, remote_clients, ssh_remotes=None):
    if not dest_root or not isinstance(dest_root, str):
        return LocalDestBackend(dest_root)

    # 两种远端地位平等 — 调用方按 mode 分流，故至多一种非空
    http = _match_remote_prefix(
        dest_root,
        remote_clients,
        lambda client, path: RemoteDestBackend(client, path),
    )
    if http is not None:
        return http

    ssh = _match_remote_prefix(
        dest_root,
        ssh_remotes or {},
        lambda ssh_config, path: SshDestBackend(ssh_config, path),
    )
    if ssh is not None:
        return ssh

    return LocalDestBackend(dest_root)
