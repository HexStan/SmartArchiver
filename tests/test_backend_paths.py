import os
import sys

import pytest

from src.core.backend import DestBackend, LocalDestBackend, create_dest_backend


class _TestBackend(DestBackend):
    def __init__(self, root_path, existing_paths=None):
        super().__init__(root_path)
        self._existing = set(existing_paths or [])

    def exists(self, path):
        return path in self._existing

    def is_dir(self, path):
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


class TestBuildDestPath:
    def test_simple_join(self):
        backend = _TestBackend("/dest")
        result = backend.build_dest_path("sub/file.txt")
        expected = os.path.normpath(os.path.join("/dest", "sub/file.txt"))
        assert result == expected

    def test_root_only(self):
        backend = _TestBackend("/dest")
        result = backend.build_dest_path("")
        expected = os.path.normpath("/dest")
        assert result == expected

    def test_nested_path(self):
        backend = _TestBackend("/dest")
        result = backend.build_dest_path("a/b/c/d.txt")
        expected = os.path.normpath("/dest/a/b/c/d.txt")
        assert result == expected

    def test_with_dot_dot(self):
        backend = _TestBackend("/dest/sub")
        result = backend.build_dest_path("../other/file.txt")
        expected = os.path.normpath("/dest/other/file.txt")
        assert result == expected

    @pytest.mark.skipif(sys.platform != "win32", reason="Windows-specific path test")
    def test_windows_path_separators(self):
        backend = _TestBackend("C:\\dest")
        result = backend.build_dest_path("sub\\file.txt")
        expected = os.path.normpath("C:\\dest\\sub\\file.txt")
        assert result == expected


class TestGetUniqueDest:
    def test_no_existing_file_returns_original(self):
        backend = _TestBackend("/dest")
        result = backend.get_unique_dest("/dest/file.txt")
        assert os.path.normpath(result) == os.path.normpath("/dest/file.txt")

    def test_existing_file_adds_number(self):
        backend = _TestBackend("/dest", {"/dest/file.txt"})
        result = backend.get_unique_dest("/dest/file.txt")
        assert os.path.normpath(result) == os.path.normpath("/dest/file-1.txt")

    def test_multiple_existing(self):
        base = os.path.normpath("/dest")
        p1 = os.path.join(base, "file.txt")
        p2 = os.path.join(base, "file-1.txt")
        p3 = os.path.join(base, "file-2.txt")
        backend = _TestBackend(base, {p1, p2, p3})
        result = backend.get_unique_dest(p1)
        assert os.path.normpath(result) == os.path.join(base, "file-3.txt")

    def test_no_extension(self):
        backend = _TestBackend("/dest", {"/dest/README"})
        result = backend.get_unique_dest("/dest/README")
        assert os.path.normpath(result) == os.path.normpath("/dest/README-1")

    def test_multiple_extensions(self):
        backend = _TestBackend("/dest", {"/dest/archive.tar.gz"})
        result = backend.get_unique_dest("/dest/archive.tar.gz")
        assert os.path.normpath(result) == os.path.normpath("/dest/archive.tar-1.gz")


class TestCreateDestBackend:
    def test_local_path(self):
        backend = create_dest_backend("/local/path", {})
        assert isinstance(backend, LocalDestBackend)
        assert backend.root_path == "/local/path"

    def test_empty_path(self):
        backend = create_dest_backend("", {})
        assert isinstance(backend, LocalDestBackend)
        assert backend.root_path == ""

    def test_none_path(self):
        backend = create_dest_backend(None, {})
        assert isinstance(backend, LocalDestBackend)
        assert backend.root_path is None

    def test_http_url_with_valid_alias(self, app_context):
        backend = create_dest_backend(
            "{http:my_nas}?/vol/backup",
            {"my_nas": object()},
        )
        from src.core.backend import RemoteDestBackend

        assert isinstance(backend, RemoteDestBackend)
        assert backend.root_path == "/vol/backup"

    def test_http_url_with_unknown_alias(self, app_context):
        backend = create_dest_backend(
            "{http:unknown}?/vol/backup",
            {},
        )
        assert isinstance(backend, LocalDestBackend)
        assert backend.root_path == "{http:unknown}?/vol/backup"

    def test_ssh_url_with_valid_alias(self, app_context):
        ssh_remote = object()
        backend = create_dest_backend(
            "{ssh:my_vps}?/var/data",
            {},
            {"my_vps": ssh_remote},
        )
        from src.core.backend import SshDestBackend

        assert isinstance(backend, SshDestBackend)
        assert backend.root_path == "/var/data"

    def test_ssh_url_with_unknown_alias(self, app_context):
        backend = create_dest_backend(
            "{ssh:unknown}?/var/data",
            {},
            {},
        )
        assert isinstance(backend, LocalDestBackend)
        assert backend.root_path == "{ssh:unknown}?/var/data"

    def test_unknown_remote_type(self, app_context):
        backend = create_dest_backend(
            "{ftp:server}?/path",
            {},
        )
        assert isinstance(backend, LocalDestBackend)
        assert backend.root_path == "{ftp:server}?/path"

    def test_http_and_ssh_namespaces_isolated(self, app_context):
        backend = create_dest_backend(
            "{http:same}?/http_path",
            {"same": object()},
            {"same": object()},
        )
        from src.core.backend import RemoteDestBackend

        assert isinstance(backend, RemoteDestBackend)
        assert backend.root_path == "/http_path"
