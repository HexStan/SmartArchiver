"""灰盒测试：sync 模式（SyncHandler）。

打桩只发生在外部进程边界（见 tests/README.md 原则三）：rsync/rclone
的真实同步语义不在我们的控制范围内，因此拦截 subprocess.Popen 记录
构造出的命令行、拦截 shutil.which 模拟工具存在性，断言"我们传给
外部工具的参数正确"。被测代码内部（命令构造、工具解析、备份目录
管理）全部真实执行。
"""

import os
import shutil
import subprocess
from types import SimpleNamespace

import pytest

from src.core.backend import SshDestBackend
from src.ssh.config import SshRemote
from tests.helpers import make_tree


def sync_task(tmp_path, **overrides):
    task = {
        "mode": "sync",
        "source": str(tmp_path / "src"),
        "dest": str(tmp_path / "dest"),
    }
    task.update(overrides)
    return task


@pytest.fixture
def stub_procs(monkeypatch):
    """进程边界打桩：记录每次外部进程调用的命令行与参数。

    - stub.calls：[SimpleNamespace(cmd, kwargs)]，按调用顺序追加；
    - stub.fail_with(code)：让后续进程以指定退出码结束。
    """
    stub = SimpleNamespace(calls=[], returncode=0)

    def fake_popen(cmd, **kwargs):
        stub.calls.append(SimpleNamespace(cmd=list(cmd), kwargs=kwargs))
        return SimpleNamespace(stdout=iter([]), wait=lambda: None, returncode=stub.returncode)

    monkeypatch.setattr(subprocess, "Popen", fake_popen)
    monkeypatch.setattr(shutil, "which", lambda name: f"/fake/bin/{name}")
    stub.fail_with = lambda code: setattr(stub, "returncode", code)
    return stub


@pytest.fixture
def prepared_dirs(tmp_path):
    """已存在的源目录与目标目录（sync 的前置条件）。"""
    src = str(tmp_path / "src")
    dest = str(tmp_path / "dest")
    make_tree(src, {"a.txt": b"x"})
    os.makedirs(dest)
    return src, dest


class TestLocalSyncCommands:
    def test_forced_rsync_command(self, run_task, tmp_path, stub_procs, prepared_dirs):
        src, dest = prepared_dirs
        run_task(sync_task(tmp_path, tool="rsync"))

        assert len(stub_procs.calls) == 1
        assert stub_procs.calls[0].cmd == ["rsync", "-av", "--delete", src + "/", dest]

    def test_forced_rclone_command(self, run_task, tmp_path, stub_procs, prepared_dirs):
        src, dest = prepared_dirs
        run_task(sync_task(tmp_path, tool="rclone"))

        assert stub_procs.calls[0].cmd == ["rclone", "sync", src, dest]

    def test_auto_resolves_rsync_on_posix(
        self, run_task, tmp_path, stub_procs, prepared_dirs, monkeypatch
    ):
        src, dest = prepared_dirs
        monkeypatch.setattr(os, "name", "posix")

        run_task(sync_task(tmp_path))  # tool 默认 auto

        assert stub_procs.calls[0].cmd[0] == "rsync"

    def test_auto_resolves_rclone_on_windows(
        self, run_task, tmp_path, stub_procs, prepared_dirs, monkeypatch
    ):
        src, dest = prepared_dirs
        monkeypatch.setattr(os, "name", "nt")

        run_task(sync_task(tmp_path))

        assert stub_procs.calls[0].cmd[0] == "rclone"

    def test_exclude_patterns_forwarded_in_order(
        self, run_task, tmp_path, stub_procs, prepared_dirs
    ):
        src, dest = prepared_dirs
        run_task(sync_task(tmp_path, tool="rsync", exclude=["*.tmp", "cache/"]))

        # 本地 rsync：位置参数（src/dest）在前，--exclude 追加在后
        assert stub_procs.calls[0].cmd == [
            "rsync",
            "-av",
            "--delete",
            src + "/",
            dest,
            "--exclude",
            "*.tmp",
            "--exclude",
            "cache/",
        ]

    def test_nonzero_exit_code_logs_error(
        self, run_task, tmp_path, stub_procs, prepared_dirs, capture_log
    ):
        stub_procs.fail_with(1)
        run_task(sync_task(tmp_path, tool="rsync"))

        assert capture_log.has("error", "退出码: 1")


class TestLocalBackupDir:
    def test_rsync_backup_flags_added(self, run_task, tmp_path, stub_procs, prepared_dirs):
        src, dest = prepared_dirs
        run_task(sync_task(tmp_path, tool="rsync", create_backups=True))

        cmd = stub_procs.calls[0].cmd
        assert "--backup" in cmd
        backup_dir = next(a for a in cmd if a.startswith("--backup-dir="))
        assert ".smart-archiver.backups" in backup_dir
        # 备份目录本身必须被排除在同步之外
        assert cmd[cmd.index("--exclude") + 1] == ".smart-archiver.backups/"

    def test_rclone_backup_dir_added(self, run_task, tmp_path, stub_procs, prepared_dirs):
        src, dest = prepared_dirs
        run_task(sync_task(tmp_path, tool="rclone", create_backups=True))

        cmd = stub_procs.calls[0].cmd
        assert "--backup-dir" in cmd
        assert ".smart-archiver.backups" in cmd[cmd.index("--backup-dir") + 1]

    def test_max_backups_prunes_oldest(self, run_task, tmp_path, stub_procs, prepared_dirs):
        """超出保留数量的旧备份从最旧开始清理（真实文件操作）。"""
        _src, dest = prepared_dirs
        base = os.path.join(dest, ".smart-archiver.backups")
        for name in ["20230101-000000", "20230202-000000", "20230303-000000"]:
            os.makedirs(os.path.join(base, name))

        run_task(sync_task(tmp_path, tool="rsync", create_backups=True, max_backups=2))

        remaining = set(os.listdir(base))
        assert "20230101-000000" not in remaining
        assert "20230202-000000" not in remaining
        assert "20230303-000000" in remaining


class TestSyncRejections:
    def test_tool_not_found_skips_task(
        self, run_task, tmp_path, stub_procs, prepared_dirs, monkeypatch, capture_log
    ):
        monkeypatch.setattr(shutil, "which", lambda _name: None)
        run_task(sync_task(tmp_path, tool="rsync"))

        assert stub_procs.calls == []
        assert capture_log.has("error", "未找到 rsync")

    def test_http_remote_dest_rejected(
        self, run_task, tmp_path, stub_procs, prepared_dirs, capture_log
    ):
        run_task(
            sync_task(tmp_path, dest="{http:nas}?/data"),
            remote_clients={"nas": object()},
        )

        assert stub_procs.calls == []
        assert capture_log.has("error", "不支持 HTTP 远程")

    def test_invalid_tool_value_skips_task(
        self, run_task, tmp_path, stub_procs, prepared_dirs, capture_log
    ):
        run_task(sync_task(tmp_path, tool="scp"))

        assert stub_procs.calls == []
        assert capture_log.has("error", "tool 配置值无效")

    def test_missing_dest_dir_aborts(
        self, run_task, tmp_path, stub_procs, capture_log
    ):
        make_tree(tmp_path / "src", {"a.txt": b"x"})
        run_task(sync_task(tmp_path))  # dest 目录未创建

        assert stub_procs.calls == []
        assert capture_log.has("critical", "目标目录不存在")


class TestSshSyncCommands:
    """SSH 远端同步：只验证传给外部工具的命令行与凭据处理，
    SSH 连接本身（SshDestBackend.is_dir 的远端探测）打桩跳过。"""

    SSH_REMOTE = SshRemote(alias="vps", host="192.168.1.100", user="root")

    @pytest.fixture
    def ssh_ready(self, monkeypatch):
        monkeypatch.setattr(SshDestBackend, "is_dir", lambda self, path: True)

    def test_rsync_ssh_command_with_port_and_key(
        self, run_task, tmp_path, stub_procs, ssh_ready
    ):
        src = make_tree(tmp_path / "src", {"a.txt": b"x"})
        remote = SshRemote(
            alias="vps", host="192.168.1.100", user="root", port=2222, key_file="/k"
        )
        run_task(
            sync_task(tmp_path, tool="rsync", dest="{ssh:vps}?/remote/data"),
            ssh_remotes={"vps": remote},
        )

        cmd = stub_procs.calls[0].cmd
        assert cmd[:3] == ["rsync", "-av", "--delete"]
        assert cmd[3:5] == [
            "-e",
            "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
            "-o LogLevel=ERROR -p 2222 -i /k",
        ]
        assert cmd[-2:] == [src + "/", "root@192.168.1.100:/remote/data"]

    def test_rsync_ssh_password_file_wraps_sshpass(
        self, run_task, tmp_path, stub_procs, ssh_ready
    ):
        make_tree(tmp_path / "src", {"a.txt": b"x"})
        remote = SshRemote(
            alias="vps", host="192.168.1.100", user="root", password_file="/pass"
        )
        run_task(
            sync_task(tmp_path, tool="rsync", dest="{ssh:vps}?/remote/data"),
            ssh_remotes={"vps": remote},
        )

        rsh = stub_procs.calls[0].cmd[4]
        assert rsh.startswith("sshpass -f /pass ssh ")

    def test_rclone_ssh_command(
        self, run_task, tmp_path, stub_procs, ssh_ready
    ):
        src = make_tree(tmp_path / "src", {"a.txt": b"x"})
        remote = SshRemote(
            alias="vps", host="192.168.1.100", user="root", port=2222, key_file="/k"
        )
        run_task(
            sync_task(tmp_path, tool="rclone", dest="{ssh:vps}?/remote/data"),
            ssh_remotes={"vps": remote},
        )

        assert stub_procs.calls[0].cmd == [
            "rclone",
            "sync",
            src,
            ":sftp:/remote/data",
            "--sftp-host",
            "192.168.1.100",
            "--sftp-user",
            "root",
            "--sftp-port",
            "2222",
            "--sftp-key-file",
            "/k",
        ]

    def test_rclone_ssh_password_passed_via_env_not_argv(
        self, run_task, tmp_path, stub_procs, ssh_ready
    ):
        """密码通过环境变量传给 rclone，避免出现在进程列表中。"""
        make_tree(tmp_path / "src", {"a.txt": b"x"})
        pass_file = tmp_path / "secret.txt"
        pass_file.write_text("  s3cret \n", encoding="utf-8")
        remote = SshRemote(
            alias="vps",
            host="192.168.1.100",
            user="root",
            password_file=str(pass_file),
        )
        run_task(
            sync_task(tmp_path, tool="rclone", dest="{ssh:vps}?/remote/data"),
            ssh_remotes={"vps": remote},
        )

        call = stub_procs.calls[0]
        assert "s3cret" not in " ".join(call.cmd)
        assert call.kwargs["env"]["RCLONE_SFTP_PASS"] == "s3cret"
