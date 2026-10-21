"""白盒单元测试：目标后端路径拼接与远端分派。

为什么直接单元测试（见 tests/README.md 原则二）：build_dest_path 与
get_unique_dest 决定文件最终落在哪里、冲突副本叫什么名字——路径错了
就是数据放错地方，这是数据安全边界，值得对每个分支单独钉死。
LocalDestBackend 的传输行为已由 test_greybox_move_copy.py 从入口覆盖。
"""

import os
import sys

import pytest

from src.core.backend import (
    LocalDestBackend,
    RemoteDestBackend,
    SshDestBackend,
    create_dest_backend,
)


class TestBuildDestPath:
    def test_joins_and_normalizes(self):
        backend = LocalDestBackend(os.path.join("dest", "root"))
        rel = os.path.join("sub", "file.txt")
        assert backend.build_dest_path(rel) == os.path.normpath(
            os.path.join("dest", "root", "sub", "file.txt")
        )

    def test_empty_rel_path_returns_root(self):
        backend = LocalDestBackend(os.path.join("dest"))
        assert backend.build_dest_path("") == os.path.normpath("dest")

    def test_parent_traversal_is_normalized_not_stripped(self):
        """normpath 会折叠 ../——钉死这一行为，路径归一化策略改变时能立刻发现。"""
        backend = LocalDestBackend(os.path.join("dest", "sub"))
        result = backend.build_dest_path(os.path.join("..", "other", "f.txt"))
        assert result == os.path.normpath(os.path.join("dest", "other", "f.txt"))

    @pytest.mark.skipif(sys.platform != "win32", reason="Windows 路径分隔符行为")
    def test_windows_separators(self):
        backend = LocalDestBackend("C:\\dest")
        assert backend.build_dest_path("sub\\file.txt") == "C:\\dest\\sub\\file.txt"


class TestRemoteBuildDestPath:
    """HTTP 远端的路径是 URL 片段，统一使用正斜杠。"""

    def test_backslash_converted_to_forward_slash(self):
        backend = RemoteDestBackend(client=None, remote_root="/vol/data")
        assert backend.build_dest_path("sub\\file.txt") == "/vol/data/sub/file.txt"

    def test_root_trailing_slash_not_duplicated(self):
        backend = RemoteDestBackend(client=None, remote_root="/vol/data/")
        assert backend.build_dest_path("a.txt") == "/vol/data/a.txt"

    def test_windows_root_joins_rel_path(self):
        """Windows 盘符根 + 反斜杠 rel_path：统一为正斜杠的完整拼接链。"""
        backend = RemoteDestBackend(client=None, remote_root="D:/Backup")
        assert backend.build_dest_path("sub\\file.txt") == "D:/Backup/sub/file.txt"


class TestGetUniqueDest:
    """冲突副本的编号规则：name-1.ext、name-2.ext……（真实文件系统）"""

    def test_missing_path_returned_as_is(self, tmp_path):
        backend = LocalDestBackend(str(tmp_path))
        assert backend.get_unique_dest(str(tmp_path / "f.txt")) == str(tmp_path / "f.txt")

    def test_existing_path_gets_number_one(self, tmp_path):
        (tmp_path / "f.txt").write_bytes(b"x")
        backend = LocalDestBackend(str(tmp_path))
        assert backend.get_unique_dest(str(tmp_path / "f.txt")) == str(tmp_path / "f-1.txt")

    def test_numbering_skips_taken_slots(self, tmp_path):
        (tmp_path / "f.txt").write_bytes(b"x")
        (tmp_path / "f-1.txt").write_bytes(b"x")
        (tmp_path / "f-2.txt").write_bytes(b"x")
        backend = LocalDestBackend(str(tmp_path))
        assert backend.get_unique_dest(str(tmp_path / "f.txt")) == str(tmp_path / "f-3.txt")

    def test_no_extension(self, tmp_path):
        (tmp_path / "README").write_bytes(b"x")
        backend = LocalDestBackend(str(tmp_path))
        assert backend.get_unique_dest(str(tmp_path / "README")) == str(tmp_path / "README-1")

    def test_only_last_extension_is_numbered(self, tmp_path):
        """archive.tar.gz → archive.tar-1.gz：扩展名取最后一段。"""
        (tmp_path / "archive.tar.gz").write_bytes(b"x")
        backend = LocalDestBackend(str(tmp_path))
        assert backend.get_unique_dest(str(tmp_path / "archive.tar.gz")) == str(
            tmp_path / "archive.tar-1.gz"
        )


class _ExistsStubClient:
    """最小 HTTP client 替身：仅回答 exists——get_unique_dest 唯一的外部依赖
    （替网络边界回答问题，见 tests/README.md 原则三、七）。"""

    def __init__(self, existing: set[str]) -> None:
        self._existing = existing

    def exists(self, path: str) -> bool:
        return path in self._existing


class TestRemoteGetUniqueDest:
    """远端冲突副本编号：posixpath 语义，与 build_dest_path 的正斜杠规则同模型。"""

    def test_missing_path_returned_as_is(self):
        backend = RemoteDestBackend(_ExistsStubClient(set()), "/vol")
        assert backend.get_unique_dest("/vol/f.txt") == "/vol/f.txt"

    def test_existing_path_gets_number_one(self):
        backend = RemoteDestBackend(_ExistsStubClient({"/vol/f.txt"}), "/vol")
        assert backend.get_unique_dest("/vol/f.txt") == "/vol/f-1.txt"

    def test_only_last_extension_is_numbered(self):
        backend = RemoteDestBackend(_ExistsStubClient({"/vol/archive.tar.gz"}), "/vol")
        assert backend.get_unique_dest("/vol/archive.tar.gz") == "/vol/archive.tar-1.gz"

    def test_windows_drive_root_numbered_without_extra_slash(self):
        """裸盘符根 D: 编号后还原为 D:/f-1.txt，不产生 D://。"""
        backend = RemoteDestBackend(_ExistsStubClient({"D:/f.txt"}), "D:")
        assert backend.get_unique_dest("D:/f.txt") == "D:/f-1.txt"


class TestRelpath:
    """完整目标路径 → 相对 root_path 的还原（日志展示用）。"""

    def test_local_uses_os_path(self, tmp_path):
        backend = LocalDestBackend(str(tmp_path))
        assert backend.relpath(str(tmp_path / "sub" / "f.txt")) == os.path.join("sub", "f.txt")

    def test_remote_strips_root_prefix(self):
        backend = RemoteDestBackend(client=None, remote_root="/vol/data")
        assert backend.relpath("/vol/data/sub/f.txt") == "sub/f.txt"

    def test_remote_windows_drive_root(self):
        backend = RemoteDestBackend(client=None, remote_root="D:/Backup")
        assert backend.relpath("D:/Backup/a.txt") == "a.txt"

    def test_remote_path_outside_root_returned_as_is(self):
        backend = RemoteDestBackend(client=None, remote_root="/vol/data")
        assert backend.relpath("/elsewhere/f.txt") == "/elsewhere/f.txt"


class TestCreateDestBackend:
    """dest 字符串 → 后端实例的分派规则与命名空间隔离。"""

    def test_plain_path_is_local(self):
        backend = create_dest_backend("/local/path", {})
        assert isinstance(backend, LocalDestBackend)
        assert backend.root_path == "/local/path"

    def test_empty_and_none_dest_are_local(self):
        assert isinstance(create_dest_backend("", {}), LocalDestBackend)
        assert create_dest_backend("", {}).root_path == ""
        assert isinstance(create_dest_backend(None, {}), LocalDestBackend)

    def test_http_alias_dispatches_to_remote(self):
        backend = create_dest_backend("{http:nas}?/vol/backup", {"nas": object()})
        assert isinstance(backend, RemoteDestBackend)
        assert backend.root_path == "/vol/backup"

    def test_http_path_normalized_with_leading_slash(self):
        backend = create_dest_backend("{http:nas}?relative/path", {"nas": object()})
        assert backend.root_path == "/relative/path"

    def test_http_windows_drive_path_keeps_drive(self):
        """Windows 盘符路径已是绝对路径，补前导 / 会被服务端 normpath 损坏成非法路径。"""
        backend = create_dest_backend("{http:pc}?D:/Backup", {"pc": object()})
        assert isinstance(backend, RemoteDestBackend)
        assert backend.root_path == "D:/Backup"

    def test_http_windows_backslash_root_normalized(self):
        """盘符根中的反斜杠统一为正斜杠，与 build_dest_path 的拼接规则一致。"""
        backend = create_dest_backend("{http:pc}?D:\\Backup", {"pc": object()})
        assert isinstance(backend, RemoteDestBackend)
        assert backend.root_path == "D:/Backup"

    def test_http_windows_bare_drive_root(self):
        """裸盘符根归一化为 D:，拼接时仍能还原为 D:/x。"""
        backend = create_dest_backend("{http:pc}?D:/", {"pc": object()})
        assert isinstance(backend, RemoteDestBackend)
        assert backend.root_path == "D:"
        assert backend.build_dest_path("f.txt") == "D:/f.txt"

    def test_unknown_http_alias_returns_none(self, app_context, capture_log):
        assert create_dest_backend("{http:ghost}?/vol", {}) is None
        assert capture_log.has("error", "ghost")
        assert capture_log.has("error", "http_remotes")

    def test_ssh_alias_dispatches_to_ssh_backend(self):
        backend = create_dest_backend("{ssh:vps}?/var/data", {}, {"vps": object()})
        assert isinstance(backend, SshDestBackend)
        assert backend.root_path == "/var/data"

    def test_unknown_ssh_alias_returns_none(self, app_context, capture_log):
        assert create_dest_backend("{ssh:ghost}?/var/data", {}, {}) is None
        assert capture_log.has("error", "ghost")
        assert capture_log.has("error", "ssh_remotes")

    def test_unknown_remote_type_returns_none(self, app_context, capture_log):
        assert create_dest_backend("{ftp:srv}?/path", {}) is None
        assert capture_log.has("error", "未知的远端类型")
        assert capture_log.has("error", "任务跳过")

    def test_http_and_ssh_namespaces_are_isolated(self):
        """同名别名在两个命名空间互不干扰——http 优先取 http 的。"""
        backend = create_dest_backend(
            "{http:same}?/http_path", {"same": object()}, {"same": object()}
        )
        assert isinstance(backend, RemoteDestBackend)
        assert backend.root_path == "/http_path"
