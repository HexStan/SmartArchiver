import os
import shutil
from abc import ABC, abstractmethod

from src.utils import copy_file, move_file


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
    def transfer_file(self, src_local_path, dest_path, on_exists, is_copy):
        """传输文件到目标。on_exists: overwrite | skip | rename | error。返回 (action, final_dest_path)。"""
        pass


class LocalDestBackend(DestBackend):
    def exists(self, path):
        return os.path.exists(path)

    def is_dir(self, path):
        return os.path.isdir(path)

    def remove_file(self, path):
        os.remove(path)

    def makedirs(self, path):
        os.makedirs(path, exist_ok=True)

    def transfer_file(self, src_local_path, dest_path, on_exists, is_copy):
        file_existed = os.path.exists(dest_path)

        if file_existed:
            if on_exists == "skip":
                return "skipped", dest_path
            elif on_exists == "overwrite":
                if os.path.isfile(dest_path):
                    os.remove(dest_path)
                elif os.path.isdir(dest_path):
                    shutil.rmtree(dest_path)
            elif on_exists == "rename":
                dest_path = self._get_unique_dest_local(dest_path)

        dest_dir = os.path.dirname(dest_path)
        if dest_dir:
            os.makedirs(dest_dir, exist_ok=True)

        if is_copy:
            copy_file(src_local_path, dest_path)
        else:
            move_file(src_local_path, dest_path)

        if file_existed and on_exists == "overwrite":
            action = "overwritten"
        elif file_existed and on_exists == "rename":
            action = "renamed"
        else:
            action = "uploaded"

        return action, dest_path

    @staticmethod
    def _get_unique_dest_local(dest_path):
        if not os.path.exists(dest_path):
            return dest_path

        directory = os.path.dirname(dest_path)
        filename = os.path.basename(dest_path)
        name, ext = os.path.splitext(filename)

        counter = 1
        while True:
            new_filename = f"{name}-{counter}{ext}"
            new_path = os.path.join(directory, new_filename)
            if not os.path.exists(new_path):
                return new_path
            counter += 1


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

    def transfer_file(self, src_local_path, dest_path, on_exists, is_copy):
        action, final_path = self._client.transfer_file(
            src_local_path, dest_path, on_exists
        )
        if not is_copy and action not in ("skipped", "error"):
            os.remove(src_local_path)
        return action, final_path


def create_dest_backend(dest_root, remote_clients):
    if dest_root and isinstance(dest_root, str):
        for alias, client in remote_clients.items():
            prefix = f"{{{alias}}}?"
            if dest_root.startswith(prefix):
                remote_path = dest_root[len(prefix) :]
                remote_path = "/" + remote_path.lstrip("/")
                return RemoteDestBackend(client, remote_path)

    return LocalDestBackend(dest_root)
