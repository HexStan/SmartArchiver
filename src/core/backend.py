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


def create_dest_backend(dest_root, remote_clients):
    if dest_root and isinstance(dest_root, str):
        for alias, client in remote_clients.items():
            prefix = f"{{{alias}}}?"
            if dest_root.startswith(prefix):
                remote_path = dest_root[len(prefix) :]
                remote_path = "/" + remote_path.lstrip("/")
                return RemoteDestBackend(client, remote_path)

    return LocalDestBackend(dest_root)
