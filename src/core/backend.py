import os
from abc import ABC, abstractmethod

from src.operations.fs_ops import (
    file_exists,
    is_directory,
    create_directory,
    delete_path,
    copy_file,
    move_file,
    get_unique_dest,
)


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
    def makedirs(self, path):
        pass

    @abstractmethod
    def copy_file(self, src_local_path, dest_path):
        pass

    @abstractmethod
    def move_file(self, src_local_path, dest_path):
        pass

    def get_unique_dest(self, dest_path):
        return get_unique_dest(dest_path, exists_fn=self.exists)


class LocalDestBackend(DestBackend):
    def exists(self, path):
        return file_exists(path)

    def is_dir(self, path):
        return is_directory(path)

    def remove_file(self, path):
        delete_path(path)

    def makedirs(self, path):
        create_directory(path)

    def copy_file(self, src_local_path, dest_path):
        copy_file(src_local_path, dest_path)

    def move_file(self, src_local_path, dest_path):
        move_file(src_local_path, dest_path)


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
        self._client.delete(path)

    def makedirs(self, path):
        self._client.mkdir(path)

    def copy_file(self, src_local_path, dest_path):
        self._client.upload(src_local_path, dest_path)

    def move_file(self, src_local_path, dest_path):
        self._client.upload(src_local_path, dest_path)
        os.remove(src_local_path)


def create_dest_backend(dest_root, remote_clients):
    if dest_root and isinstance(dest_root, str):
        for alias, client in remote_clients.items():
            prefix = f"{{{alias}}}?"
            if dest_root.startswith(prefix):
                remote_path = dest_root[len(prefix) :]
                remote_path = "/" + remote_path.lstrip("/")
                return RemoteDestBackend(client, remote_path)

    return LocalDestBackend(dest_root)
