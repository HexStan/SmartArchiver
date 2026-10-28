"""白盒单元测试：dest 协议串解析、路径规则与转发分派。

为什么直接单元测试（见 tests/README.md 原则二）：build_dest_path 与
unique_dest 决定文件最终落在哪里、冲突副本叫什么名字——路径错了
就是数据放错地方，这是数据安全边界，值得对每个分支单独钉死。
本地目标的传输行为已由 test_greybox_move_copy.py 从入口覆盖；
加载期的 dest 模式兼容性校验（validate_dest）与 sync 的 backup_dir
位置校验（validate_backup_dir）也在此白盒钉死。
"""

import os
import sys

import pytest

from src.app.context import AppContext
from src.core import dest


class TestParseDest:
    """dest 串语法 → (kind, alias, path) 的解析规则。"""

    def test_plain_path_is_local(self):
        parsed = dest.parse_dest("/local/path")
        assert parsed.kind == dest.LOCAL
        assert parsed.alias == ""
        assert parsed.path == "/local/path"

    def test_empty_dest_is_local(self):
        parsed = dest.parse_dest("")
        assert parsed.kind == dest.LOCAL
        assert parsed.path == ""

    def test_http_prefix_parsed_and_normalized(self):
        parsed = dest.parse_dest("{http:nas}?relative/path")
        assert parsed.kind == dest.HTTP
        assert parsed.alias == "nas"
        assert parsed.path == "/relative/path"

    def test_http_windows_drive_path_keeps_drive(self):
        """Windows 盘符路径已是绝对路径，补前导 / 会被服务端 normpath 损坏。"""
        parsed = dest.parse_dest("{http:pc}?D:/Backup")
        assert parsed.kind == dest.HTTP
        assert parsed.path == "D:/Backup"

    def test_http_windows_backslash_root_normalized(self):
        """盘符根中的反斜杠统一为正斜杠，与 build_dest_path 的拼接规则一致。"""
        assert dest.parse_dest("{http:pc}?D:\\Backup").path == "D:/Backup"

    def test_http_windows_bare_drive_root(self):
        """裸盘符根归一化为 D:，拼接时仍能还原为 D:/x。"""
        assert dest.parse_dest("{http:pc}?D:/").path == "D:"

    def test_ssh_prefix_parsed_with_leading_slash(self):
        parsed = dest.parse_dest("{ssh:vps}?var/data")
        assert parsed.kind == dest.SSH
        assert parsed.alias == "vps"
        assert parsed.path == "/var/data"

    def test_unknown_remote_type_kept_for_validate_to_reject(self):
        parsed = dest.parse_dest("{ftp:srv}?/path")
        assert parsed.kind == "ftp"
        assert parsed.alias == "srv"
        assert parsed.path == "/path"

    def test_parse_is_idempotent_on_normalized_path(self):
        """解析幂等：转发原语可对完整协议串逐调用重复解析。"""
        once = dest.parse_dest("{http:nas}?/vol/data")
        rebuilt = dest.parse_dest("{http:nas}?" + once.path)
        assert once == rebuilt


class TestBuildDestPath:
    def test_joins_and_normalizes(self):
        root = os.path.join("dest", "root")
        rel = os.path.join("sub", "file.txt")
        assert dest.build_dest_path(root, rel) == os.path.normpath(
            os.path.join("dest", "root", "sub", "file.txt")
        )

    def test_empty_rel_path_returns_root(self):
        assert dest.build_dest_path(os.path.join("dest"), "") == os.path.normpath("dest")

    def test_parent_traversal_is_normalized_not_stripped(self):
        """normpath 会折叠 ../——钉死这一行为，路径归一化策略改变时能立刻发现。"""
        result = dest.build_dest_path(
            os.path.join("dest", "sub"), os.path.join("..", "other", "f.txt")
        )
        assert result == os.path.normpath(os.path.join("dest", "other", "f.txt"))

    @pytest.mark.skipif(sys.platform != "win32", reason="Windows 路径分隔符行为")
    def test_windows_separators(self):
        assert dest.build_dest_path("C:\\dest", "sub\\file.txt") == "C:\\dest\\sub\\file.txt"


class TestRemoteBuildDestPath:
    """HTTP 远端的路径是 URL 片段，统一使用正斜杠；结果保留协议前缀。"""

    def test_backslash_converted_to_forward_slash(self):
        assert (
            dest.build_dest_path("{http:nas}?/vol/data", "sub\\file.txt")
            == "{http:nas}?/vol/data/sub/file.txt"
        )

    def test_root_trailing_slash_not_duplicated(self):
        assert (
            dest.build_dest_path("{http:nas}?/vol/data/", "a.txt") == "{http:nas}?/vol/data/a.txt"
        )

    def test_windows_root_joins_rel_path(self):
        """Windows 盘符根 + 反斜杠 rel_path：统一为正斜杠的完整拼接链。"""
        assert (
            dest.build_dest_path("{http:pc}?D:/Backup", "sub\\file.txt")
            == "{http:pc}?D:/Backup/sub/file.txt"
        )

    def test_bare_drive_root_joins_without_double_slash(self):
        assert dest.build_dest_path("{http:pc}?D:", "f.txt") == "{http:pc}?D:/f.txt"


class TestUniqueDest:
    """冲突副本的编号规则：name-1.ext、name-2.ext……（真实文件系统）"""

    def test_missing_path_returned_as_is(self, tmp_path):
        assert dest.unique_dest(str(tmp_path / "f.txt")) == str(tmp_path / "f.txt")

    def test_existing_path_gets_number_one(self, tmp_path):
        (tmp_path / "f.txt").write_bytes(b"x")
        assert dest.unique_dest(str(tmp_path / "f.txt")) == str(tmp_path / "f-1.txt")

    def test_numbering_skips_taken_slots(self, tmp_path):
        for name in ["f.txt", "f-1.txt", "f-2.txt"]:
            (tmp_path / name).write_bytes(b"x")
        assert dest.unique_dest(str(tmp_path / "f.txt")) == str(tmp_path / "f-3.txt")

    def test_no_extension(self, tmp_path):
        (tmp_path / "README").write_bytes(b"x")
        assert dest.unique_dest(str(tmp_path / "README")) == str(tmp_path / "README-1")

    def test_only_last_extension_is_numbered(self, tmp_path):
        """archive.tar.gz → archive.tar-1.gz：扩展名取最后一段。"""
        (tmp_path / "archive.tar.gz").write_bytes(b"x")
        assert dest.unique_dest(str(tmp_path / "archive.tar.gz")) == str(
            tmp_path / "archive.tar-1.gz"
        )


class _ExistsStubClient:
    """最小 HTTP client 替身：仅回答 exists——unique_dest 唯一的外部依赖
    （替网络边界回答问题，见 tests/README.md 原则三、七）。"""

    def __init__(self, existing: set[str]) -> None:
        self._existing = existing

    def exists(self, path: str) -> bool:
        return path in self._existing


class TestRemoteUniqueDest:
    """远端冲突副本编号：posixpath 语义，与 build_dest_path 的正斜杠规则同模型。"""

    @staticmethod
    def _install(existing: set[str]) -> None:
        AppContext.get().remote_clients = {"nas": _ExistsStubClient(existing)}

    def test_missing_path_returned_as_is(self, app_context):
        self._install(set())
        assert dest.unique_dest("{http:nas}?/vol/f.txt") == "{http:nas}?/vol/f.txt"

    def test_existing_path_gets_number_one(self, app_context):
        self._install({"/vol/f.txt"})
        assert dest.unique_dest("{http:nas}?/vol/f.txt") == "{http:nas}?/vol/f-1.txt"

    def test_only_last_extension_is_numbered(self, app_context):
        self._install({"/vol/archive.tar.gz"})
        assert dest.unique_dest("{http:nas}?/vol/archive.tar.gz") == (
            "{http:nas}?/vol/archive.tar-1.gz"
        )

    def test_windows_drive_root_numbered_without_extra_slash(self, app_context):
        """裸盘符根 D: 编号后还原为 D:/f-1.txt，不产生 D://。"""
        AppContext.get().remote_clients = {"pc": _ExistsStubClient({"D:/f.txt"})}
        assert dest.unique_dest("{http:pc}?D:/f.txt") == "{http:pc}?D:/f-1.txt"


class TestRelpath:
    """完整目标路径 → 相对 root 的还原（日志展示用）。"""

    def test_local_uses_os_path(self, tmp_path):
        root = str(tmp_path)
        assert dest.relpath(root, os.path.join(root, "sub", "f.txt")) == os.path.join(
            "sub", "f.txt"
        )

    def test_remote_strips_root_prefix(self):
        assert dest.relpath("{http:nas}?/vol/data", "{http:nas}?/vol/data/sub/f.txt") == "sub/f.txt"

    def test_remote_windows_drive_root(self):
        assert dest.relpath("{http:pc}?D:/Backup", "{http:pc}?D:/Backup/a.txt") == "a.txt"

    def test_remote_path_outside_root_returned_as_is(self):
        full = "{http:nas}?/elsewhere/f.txt"
        assert dest.relpath("{http:nas}?/vol/data", full) == full


class TestValidateDest:
    """dest 串与任务模式的加载期校验（纯函数：合法返回 None，否则返回消息）。

    校验在配置加载期由 task_plan.resolve_tasks 调用；错误日志的拼装
    （任务序号上下文 + 跳过语义）属解析器职责，在此只钉消息本体。
    """

    def test_plain_path_is_accepted(self):
        assert dest.validate_dest("/local/path", "copy", http_aliases=(), ssh_aliases=()) is None

    def test_empty_dest_is_accepted(self):
        assert dest.validate_dest("", "move", http_aliases=(), ssh_aliases=()) is None

    def test_http_alias_accepted_for_transfer_modes(self):
        assert (
            dest.validate_dest(
                "{http:nas}?/vol/backup", "move", http_aliases={"nas"}, ssh_aliases=()
            )
            is None
        )

    def test_unknown_http_alias_rejected(self):
        reason = dest.validate_dest("{http:ghost}?/vol", "copy", http_aliases=(), ssh_aliases=())
        assert reason is not None
        assert "ghost" in reason
        assert "http_remotes" in reason

    def test_ssh_alias_accepted_for_sync(self):
        assert (
            dest.validate_dest("{ssh:vps}?/var/data", "sync", http_aliases=(), ssh_aliases={"vps"})
            is None
        )

    def test_unknown_ssh_alias_rejected(self):
        reason = dest.validate_dest(
            "{ssh:ghost}?/var/data", "sync", http_aliases=(), ssh_aliases=()
        )
        assert reason is not None
        assert "ghost" in reason
        assert "ssh_remotes" in reason

    def test_unknown_remote_type_rejected(self):
        reason = dest.validate_dest("{ftp:srv}?/path", "copy", http_aliases=(), ssh_aliases=())
        assert reason is not None
        assert "未知的远端类型" in reason

    def test_http_and_ssh_namespaces_are_isolated(self):
        """同名别名在两个命名空间互不干扰——各查各的。"""
        assert (
            dest.validate_dest(
                "{http:same}?/http_path", "copy", http_aliases={"same"}, ssh_aliases={"same"}
            )
            is None
        )
        assert (
            dest.validate_dest(
                "{ssh:same}?/ssh_path", "sync", http_aliases={"same"}, ssh_aliases={"same"}
            )
            is None
        )

    def test_sync_rejects_http_dest(self):
        reason = dest.validate_dest(
            "{http:nas}?/data", "sync", http_aliases={"nas"}, ssh_aliases=()
        )
        assert reason is not None
        assert "不支持 HTTP 远程" in reason

    @pytest.mark.parametrize("mode", ["move", "copy", "rotate"])
    def test_per_file_modes_reject_ssh_dest(self, mode):
        """逐文件传输模式拒绝 SSH 目标：在加载期即报配置错误，不等到运行时。"""
        reason = dest.validate_dest("{ssh:vps}?/data", mode, http_aliases=(), ssh_aliases={"vps"})
        assert reason is not None
        assert "不支持 SSH 远端目标" in reason


class TestValidateBackupDir:
    """sync 任务 backup_dir 的加载期校验（纯函数：合法返回 None，否则返回消息）。

    本地路径的包含判断统一走 tmp_path 真实绝对路径，保证在任意平台上
    行为一致（Windows 的大小写/斜杠归一单独用例钉死）。
    """

    def test_empty_backup_dir_is_accepted(self, tmp_path):
        assert dest.validate_backup_dir(str(tmp_path / "dest"), str(tmp_path / "src"), "") is None

    def test_backup_dir_outside_source_and_dest_is_accepted(self, tmp_path):
        reason = dest.validate_backup_dir(
            str(tmp_path / "dest"), str(tmp_path / "src"), str(tmp_path / "backups")
        )
        assert reason is None

    def test_backup_dir_equal_to_dest_rejected(self, tmp_path):
        d = str(tmp_path / "dest")
        reason = dest.validate_backup_dir(d, str(tmp_path / "src"), d)
        assert reason is not None
        assert "dest 之下" in reason

    def test_backup_dir_under_dest_rejected(self, tmp_path):
        reason = dest.validate_backup_dir(
            str(tmp_path / "dest"), str(tmp_path / "src"), str(tmp_path / "dest" / "backups")
        )
        assert reason is not None
        assert "dest 之下" in reason

    def test_backup_dir_under_source_rejected(self, tmp_path):
        reason = dest.validate_backup_dir(
            str(tmp_path / "dest"), str(tmp_path / "src"), str(tmp_path / "src" / "backups")
        )
        assert reason is not None
        assert "source 之下" in reason

    def test_relative_backup_dir_resolved_against_cwd(self, tmp_path, monkeypatch):
        """相对 backup_dir 与 source/dest 同规则按工作目录解析后再比较。"""
        monkeypatch.chdir(tmp_path)
        assert dest.validate_backup_dir("dest", "src", "backups") is None
        reason = dest.validate_backup_dir("dest", "src", os.path.join("dest", "backups"))
        assert reason is not None
        assert "dest 之下" in reason

    @pytest.mark.skipif(sys.platform != "win32", reason="Windows 专有：大小写与斜杠归一")
    def test_windows_case_and_slash_normalized(self, tmp_path):
        dest_dir = str(tmp_path / "dest")
        backup_dir = dest_dir.replace("\\", "/").upper() + "/backups"
        reason = dest.validate_backup_dir(dest_dir, str(tmp_path / "src"), backup_dir)
        assert reason is not None
        assert "dest 之下" in reason

    def test_ssh_relative_backup_dir_rejected(self):
        reason = dest.validate_backup_dir("{ssh:vps}?/data", "/src", "backups")
        assert reason is not None
        assert "绝对路径" in reason

    def test_ssh_backup_dir_under_remote_dest_rejected(self):
        reason = dest.validate_backup_dir("{ssh:vps}?/data", "/src", "/data/backups")
        assert reason is not None
        assert "dest 之下" in reason

    def test_ssh_backup_dir_equal_to_remote_dest_rejected(self):
        reason = dest.validate_backup_dir("{ssh:vps}?/data", "/src", "/data")
        assert reason is not None
        assert "dest 之下" in reason

    def test_ssh_backup_dir_outside_remote_dest_accepted(self):
        assert dest.validate_backup_dir("{ssh:vps}?/data", "/src", "/backups") is None

    def test_ssh_remote_root_dest_rejects_any_backup_dir(self):
        """dest 为远端根目录时任何绝对路径都在其下。"""
        reason = dest.validate_backup_dir("{ssh:vps}?/", "/src", "/backups")
        assert reason is not None
        assert "dest 之下" in reason

    @pytest.mark.parametrize("backup_dir", ["{ssh:vps}?/backups", "{http:nas}?/backups"])
    def test_prefix_syntax_rejected(self, tmp_path, backup_dir):
        reason = dest.validate_backup_dir(str(tmp_path / "dest"), str(tmp_path / "src"), backup_dir)
        assert reason is not None
        assert "协议前缀" in reason


class _RecordingClient:
    """记录 upload 调用的 HTTP client 替身。"""

    def __init__(self) -> None:
        self.uploads: list[tuple[str, str]] = []

    def upload(self, local_path: str, remote_path: str) -> None:
        self.uploads.append((local_path, remote_path))


class TestForwardingPrimitives:
    """转发原语按前缀分派：本地走 fs，HTTP 走 client（move = 上传 + 删源）。"""

    def test_local_exists_and_is_dir(self, tmp_path):
        (tmp_path / "d").mkdir()
        assert dest.exists(str(tmp_path / "d")) is True
        assert dest.is_dir(str(tmp_path / "d")) is True
        assert dest.is_dir(str(tmp_path / "missing")) is False

    def test_http_copy_uploads_to_normalized_path(self, tmp_path, app_context):
        client = _RecordingClient()
        AppContext.get().remote_clients = {"nas": client}
        src = tmp_path / "a.txt"
        src.write_bytes(b"x")

        dest.copy_file(str(src), "{http:nas}?/vol/a.txt")

        assert client.uploads == [(str(src), "/vol/a.txt")]

    def test_http_move_uploads_then_deletes_source(self, tmp_path, app_context):
        client = _RecordingClient()
        AppContext.get().remote_clients = {"nas": client}
        src = tmp_path / "a.txt"
        src.write_bytes(b"x")

        dest.move_file(str(src), "{http:nas}?/vol/a.txt")

        assert client.uploads == [(str(src), "/vol/a.txt")]
        assert not src.exists()

    def test_ssh_dest_rejected_loudly(self, app_context):
        """SSH 目标不参与逐文件转发，误用时大声失败而非静默出错。"""
        with pytest.raises(NotImplementedError):
            dest.exists("{ssh:vps}?/data")
