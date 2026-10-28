"""灰盒测试：sync 模式（SyncHandler）。

打桩只发生在外部进程边界（见 tests/README.md 原则三）：rsync/rclone
的真实同步语义不在我们的控制范围内，因此拦截 subprocess.Popen 记录
构造出的命令行、拦截 shutil.which 模拟工具存在性，断言"我们传给
外部工具的参数正确"。被测代码内部（命令构造、工具解析、备份目录
管理）全部真实执行。
"""

import os
import re
import shutil
import subprocess
import sys
from types import SimpleNamespace

import pytest

from src.platform_info import exe_name
from src.ssh.remotes import SshRemote
from tests.helpers import NOW, RECENT, make_tree, snapshot


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
        assert stub_procs.calls[0].cmd == [
            "rsync",
            "-av",
            "--delete",
            src + "/",
            dest,
            "--delete-excluded",
        ]

    def test_forced_rclone_command(self, run_task, tmp_path, stub_procs, prepared_dirs):
        src, dest = prepared_dirs
        run_task(sync_task(tmp_path, tool="rclone"))

        # -v：INFO 级日志，逐文件输出与 rsync -av 对齐；--transfers 1：串行传输；
        # --delete-excluded：排除项也从 dest 删除（不可配置）
        assert stub_procs.calls[0].cmd == [
            "rclone",
            "sync",
            src,
            dest,
            "-v",
            "--transfers",
            "1",
            "--delete-excluded",
        ]

    def test_auto_prefers_system_rsync_on_linux(
        self, run_task, tmp_path, stub_procs, prepared_dirs, monkeypatch
    ):
        _src, _dest = prepared_dirs
        monkeypatch.setattr(sys, "platform", "linux")

        run_task(sync_task(tmp_path))  # tool 默认 auto

        # Linux 的 auto 优先系统 rsync（链首，即使 rclone 也可用）
        assert stub_procs.calls[0].cmd[0] == "rsync"

    def test_auto_falls_back_to_system_rclone_on_linux_when_rsync_missing(
        self, run_task, tmp_path, stub_procs, prepared_dirs, monkeypatch, capture_log
    ):
        _src, _dest = prepared_dirs
        monkeypatch.setattr(sys, "platform", "linux")
        monkeypatch.setattr(
            shutil,
            "which",
            lambda name: None if name == "rsync" else f"/fake/bin/{name}",
        )

        run_task(sync_task(tmp_path))

        # 链首 rsync 缺失：记回退 warning 后使用系统 rclone
        assert stub_procs.calls[0].cmd[0] == "rclone"
        assert capture_log.has("warning", "未找到 rsync，回退使用 rclone")

    def test_auto_resolves_rclone_on_windows(
        self, run_task, tmp_path, stub_procs, prepared_dirs, monkeypatch
    ):
        _src, _dest = prepared_dirs
        monkeypatch.setattr(sys, "platform", "win32")

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
            "--delete-excluded",
        ]

    def test_exclude_from_forwarded_rsync(self, run_task, tmp_path, stub_procs, prepared_dirs):
        src, dest = prepared_dirs
        exclude_file = tmp_path / "exclude.txt"
        exclude_file.write_text("*.log\n", encoding="utf-8")
        run_task(sync_task(tmp_path, tool="rsync", exclude_from=str(exclude_file)))

        # 本地 rsync：exclude_from 以 = 连接的单参数透传（与 --backup-dir= 同风格）
        assert stub_procs.calls[0].cmd == [
            "rsync",
            "-av",
            "--delete",
            src + "/",
            dest,
            f"--exclude-from={exclude_file}",
            "--delete-excluded",
        ]

    def test_exclude_from_forwarded_rclone(self, run_task, tmp_path, stub_procs, prepared_dirs):
        src, dest = prepared_dirs
        exclude_file = tmp_path / "exclude.txt"
        exclude_file.write_text("*.log\n", encoding="utf-8")
        run_task(sync_task(tmp_path, tool="rclone", exclude_from=str(exclude_file)))

        # 本地 rclone：exclude_from 以分离参数透传（与 --exclude 同风格）
        assert stub_procs.calls[0].cmd == [
            "rclone",
            "sync",
            src,
            dest,
            "-v",
            "--transfers",
            "1",
            "--exclude-from",
            str(exclude_file),
            "--delete-excluded",
        ]

    def test_exclude_from_multiple_files_kept_in_order(
        self, run_task, tmp_path, stub_procs, prepared_dirs
    ):
        """多个规则文件按配置顺序逐个透传（rsync/rclone 均支持重复该选项）。"""
        _src, _dest = prepared_dirs
        first = tmp_path / "a.txt"
        second = tmp_path / "b.txt"
        first.write_text("*.log\n", encoding="utf-8")
        second.write_text("cache/\n", encoding="utf-8")
        run_task(sync_task(tmp_path, tool="rsync", exclude_from=[str(first), str(second)]))

        cmd = stub_procs.calls[0].cmd
        assert [a for a in cmd if a.startswith("--exclude-from=")] == [
            f"--exclude-from={first}",
            f"--exclude-from={second}",
        ]

    def test_exclude_from_combined_with_exclude_and_backups(
        self, run_task, tmp_path, stub_procs, prepared_dirs, capture_log
    ):
        _src, _dest = prepared_dirs
        exclude_file = tmp_path / "exclude.txt"
        exclude_file.write_text("*.log\n", encoding="utf-8")
        backup_base = str(tmp_path / "backups")
        run_task(
            sync_task(
                tmp_path,
                tool="rsync",
                exclude=["*.tmp"],
                exclude_from=str(exclude_file),
                backup_dir=backup_base,
            )
        )

        cmd = stub_procs.calls[0].cmd
        if sys.platform == "darwin":
            # macOS 系统 rsync 为 openrsync：备份段整体缺失并告警
            assert capture_log.has("warning", "openrsync")
        else:
            # 顺序：内联 --exclude → --exclude-from（跨平台一致）→ --backup/--backup-dir
            assert cmd.index("--exclude") < cmd.index(f"--exclude-from={exclude_file}")
            assert cmd.index(f"--exclude-from={exclude_file}") < cmd.index("--backup")
            backup_arg = next(a for a in cmd if a.startswith("--backup-dir="))
            value = backup_arg.removeprefix("--backup-dir=")
            assert os.path.dirname(value) == backup_base
            assert re.fullmatch(r"\d{8}-\d{6}", os.path.basename(value))
        # backup_dir 由加载期校验保证不在 dest 之下，不再注入排除项
        assert ".smart-archiver.backups/" not in cmd
        assert "--delete-excluded" in cmd

    def test_nonzero_exit_code_logs_error(
        self, run_task, tmp_path, stub_procs, prepared_dirs, capture_log
    ):
        stub_procs.fail_with(1)
        run_task(sync_task(tmp_path, tool="rsync"))

        assert capture_log.has("error", "退出码: 1")


class TestLocalBackupDir:
    @pytest.mark.skipif(
        sys.platform == "darwin",
        reason="macOS 系统 rsync 为 openrsync，不支持备份目录"
        "（openrsync 行为由 test_macos_rsync_is_openrsync_and_ignores_backups 覆盖）",
    )
    def test_rsync_backup_flags_added(self, run_task, tmp_path, stub_procs, prepared_dirs):
        _src, _dest = prepared_dirs
        backup_base = str(tmp_path / "backups")
        run_task(sync_task(tmp_path, tool="rsync", backup_dir=backup_base))

        cmd = stub_procs.calls[0].cmd
        assert "--backup" in cmd
        backup_arg = next(a for a in cmd if a.startswith("--backup-dir="))
        value = backup_arg.removeprefix("--backup-dir=")
        # 备份数据直接落在 [backup_dir]/[时间戳]，无预设固定目录名
        assert os.path.dirname(value) == backup_base
        assert re.fullmatch(r"\d{8}-\d{6}", os.path.basename(value))
        # backup_dir 不位于 dest 之下，无需排除项
        assert ".smart-archiver.backups/" not in cmd

    def test_rclone_backup_dir_added(self, run_task, tmp_path, stub_procs, prepared_dirs):
        _src, _dest = prepared_dirs
        backup_base = str(tmp_path / "backups")
        run_task(sync_task(tmp_path, tool="rclone", backup_dir=backup_base))

        cmd = stub_procs.calls[0].cmd
        assert "--backup-dir" in cmd
        value = cmd[cmd.index("--backup-dir") + 1]
        assert os.path.dirname(value) == backup_base
        assert re.fullmatch(r"\d{8}-\d{6}", os.path.basename(value))

    def test_max_backups_prunes_oldest(self, run_task, tmp_path, stub_procs, prepared_dirs):
        """超出保留数量的旧备份按子目录名排序从最旧开始清理（真实文件操作）。

        backup_dir 视为备份独占目录：轮转不校验子目录命名，任何子目录都
        参与排序。改用 rclone 使用例在 macOS（openrsync）同样可跑。
        """
        _src, _dest = prepared_dirs
        base = str(tmp_path / "backups")
        for name in ["m-3", "a-1", "z-4", "b-2"]:
            os.makedirs(os.path.join(base, name))

        run_task(sync_task(tmp_path, tool="rclone", backup_dir=base, max_backups=2))

        # 4 个子目录按名排序 [a-1, b-2, m-3, z-4]，保留 2 份需删 3 个
        assert set(os.listdir(base)) == {"z-4"}

    def test_max_backups_zero_keeps_all(self, run_task, tmp_path, stub_procs, prepared_dirs):
        """max_backups = 0（默认）不轮转：backup_dir 下的已有子目录全部保留。"""
        _src, _dest = prepared_dirs
        base = str(tmp_path / "backups")
        for name in ["20230101-000000", "20230202-000000"]:
            os.makedirs(os.path.join(base, name))

        run_task(sync_task(tmp_path, tool="rclone", backup_dir=base))

        assert set(os.listdir(base)) == {"20230101-000000", "20230202-000000"}


class TestBundledRcloneResolution:
    """内置 rclone 与 auto 平台链：打包版优先用内置二进制，
    链中缺失时按平台优先级回退（打桩只涉及 sys.frozen/_MEIPASS
    打包环境边界与 shutil.which 进程边界；平台以 sys.platform 桩固定）。"""

    @pytest.fixture
    def frozen_env(self, monkeypatch, tmp_path):
        """模拟 PyInstaller onedir 环境：sys.frozen + _MEIPASS 指向临时内部目录。"""
        internal = tmp_path / "_internal"
        (internal / "rclone").mkdir(parents=True)
        monkeypatch.setattr(sys, "frozen", True, raising=False)
        monkeypatch.setattr(sys, "_MEIPASS", str(internal), raising=False)
        return internal

    @staticmethod
    def bundle_rclone(internal):
        # 与生产端同源：exe_name 实时读取 sys.platform，与各用例的平台桩保持一致
        path = internal / "rclone" / exe_name("rclone")
        path.write_bytes(b"fake rclone binary")
        return str(path)

    def test_auto_prefers_bundled_rclone_on_macos(
        self, run_task, tmp_path, stub_procs, prepared_dirs, frozen_env, monkeypatch
    ):
        monkeypatch.setattr(sys, "platform", "darwin")
        bundled = self.bundle_rclone(frozen_env)
        _src, _dest = prepared_dirs

        run_task(sync_task(tmp_path))  # tool 默认 auto

        # macOS 的 auto 链首即 rclone：内置优先于系统 PATH 与系统 rsync
        assert stub_procs.calls[0].cmd[:2] == [bundled, "sync"]

    def test_forced_rclone_uses_bundled_first(
        self, run_task, tmp_path, stub_procs, prepared_dirs, frozen_env
    ):
        bundled = self.bundle_rclone(frozen_env)
        _src, _dest = prepared_dirs

        run_task(sync_task(tmp_path, tool="rclone"))

        assert stub_procs.calls[0].cmd[:2] == [bundled, "sync"]

    def test_auto_falls_back_to_system_rclone_when_bundled_missing(
        self, run_task, tmp_path, stub_procs, prepared_dirs, frozen_env, monkeypatch
    ):
        """打包环境但内置 rclone 缺失（分发损坏等）：Windows 回退系统 rclone。"""
        monkeypatch.setattr(sys, "platform", "win32")
        _src, _dest = prepared_dirs

        run_task(sync_task(tmp_path))

        assert stub_procs.calls[0].cmd[0] == "rclone"

    def test_auto_falls_back_to_bundled_rclone_on_linux_when_rsync_missing(
        self, run_task, tmp_path, stub_procs, prepared_dirs, frozen_env, monkeypatch, capture_log
    ):
        monkeypatch.setattr(sys, "platform", "linux")
        monkeypatch.setattr(
            shutil,
            "which",
            lambda name: None if name == "rsync" else f"/fake/bin/{name}",
        )
        bundled = self.bundle_rclone(frozen_env)
        _src, _dest = prepared_dirs

        run_task(sync_task(tmp_path))

        # Linux 链首 rsync 缺失：记回退 warning 后使用内置 rclone（优先于系统 rclone）
        assert stub_procs.calls[0].cmd[:2] == [bundled, "sync"]
        assert capture_log.has("warning", "未找到 rsync，回退使用 rclone")

    def test_macos_rsync_is_openrsync_and_ignores_backups(
        self,
        run_task,
        tmp_path,
        stub_procs,
        prepared_dirs,
        frozen_env,
        monkeypatch,
        capture_log,
    ):
        """macOS 的系统 rsync 即 openrsync：不支持备份目录，警告后继续同步。"""
        monkeypatch.setattr(sys, "platform", "darwin")
        monkeypatch.setattr(
            shutil,
            "which",
            lambda name: None if name == "rclone" else f"/fake/bin/{name}",
        )
        _src, _dest = prepared_dirs

        run_task(sync_task(tmp_path, backup_dir=str(tmp_path / "backups")))

        cmd = stub_procs.calls[0].cmd
        assert cmd[0] == "rsync"
        assert "--backup" not in cmd
        assert not any(a.startswith("--backup-dir") for a in cmd)
        assert capture_log.has("warning", "openrsync")

    def test_explicit_rsync_on_macos_ignores_backups(
        self, run_task, tmp_path, stub_procs, prepared_dirs, monkeypatch, capture_log
    ):
        monkeypatch.setattr(sys, "platform", "darwin")
        _src, _dest = prepared_dirs

        run_task(sync_task(tmp_path, tool="rsync", backup_dir=str(tmp_path / "backups")))

        cmd = stub_procs.calls[0].cmd
        assert "--backup" not in cmd
        assert not any(a.startswith("--backup-dir") for a in cmd)
        assert capture_log.has("warning", "openrsync")


class TestSyncRejections:
    def test_tool_not_found_skips_task(
        self, run_task, tmp_path, stub_procs, prepared_dirs, monkeypatch, capture_log
    ):
        monkeypatch.setattr(shutil, "which", lambda _name: None)
        run_task(sync_task(tmp_path, tool="rsync"))

        assert stub_procs.calls == []
        assert capture_log.has("error", "未找到 rsync")

    def test_auto_all_tools_missing_skips_task_on_linux(
        self, run_task, tmp_path, stub_procs, prepared_dirs, monkeypatch, capture_log
    ):
        monkeypatch.setattr(sys, "platform", "linux")
        monkeypatch.setattr(shutil, "which", lambda _name: None)
        _src, _dest = prepared_dirs
        run_task(sync_task(tmp_path))  # tool 默认 auto

        assert stub_procs.calls == []
        # 整条链失败只报一次 error，列出该平台尝试过的工具
        assert capture_log.has("error", "未找到 rsync 或 rclone，无法执行 sync 模式")

    def test_auto_rclone_missing_skips_task_on_windows(
        self, run_task, tmp_path, stub_procs, prepared_dirs, monkeypatch, capture_log
    ):
        """Windows 的 auto 链只有 rclone：缺失即报错，不尝试 rsync。"""
        monkeypatch.setattr(sys, "platform", "win32")
        monkeypatch.setattr(shutil, "which", lambda _name: None)
        _src, _dest = prepared_dirs
        run_task(sync_task(tmp_path))

        assert stub_procs.calls == []
        assert capture_log.has("error", "未找到 rclone，无法执行 sync 模式")

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

    def test_backup_dir_under_dest_rejected_at_load_time(
        self, run_task, tmp_path, stub_procs, prepared_dirs, capture_log
    ):
        """backup_dir 位于 dest 之下：加载期报错，任务被跳过（无进程调用）。"""
        _src, dest_dir = prepared_dirs
        run_task(sync_task(tmp_path, backup_dir=os.path.join(dest_dir, "backups")))

        assert stub_procs.calls == []
        assert capture_log.has("error", "backup_dir 配置无效")
        assert capture_log.has("error", "dest 之下")

    def test_backup_dir_under_source_rejected_at_load_time(
        self, run_task, tmp_path, stub_procs, prepared_dirs, capture_log
    ):
        """backup_dir 位于 source 之下：加载期报错，任务被跳过（无进程调用）。"""
        _src, _dest = prepared_dirs
        run_task(sync_task(tmp_path, backup_dir=os.path.join(str(tmp_path / "src"), "backups")))

        assert stub_procs.calls == []
        assert capture_log.has("error", "backup_dir 配置无效")
        assert capture_log.has("error", "source 之下")

    def test_missing_dest_dir_aborts(self, run_task, tmp_path, stub_procs, capture_log):
        make_tree(tmp_path / "src", {"a.txt": b"x"})
        run_task(sync_task(tmp_path))  # dest 目录未创建

        assert stub_procs.calls == []
        assert capture_log.has("critical", "目标目录不存在")

    def test_missing_exclude_from_file_aborts(
        self, run_task, tmp_path, stub_procs, prepared_dirs, capture_log
    ):
        """exclude_from 文件缺失即 fail fast：不发起同步（无进程调用）。"""
        run_task(sync_task(tmp_path, tool="rsync", exclude_from=str(tmp_path / "nope.txt")))

        assert stub_procs.calls == []
        assert capture_log.has("error", "exclude_from 文件不存在")
        assert capture_log.has("error", "nope.txt")


class TestSourceQuietPeriod:
    """source_quiet_seconds：源目录静默期内整任务跳过，连工具解析都
    不会发生（rclone/rsync 缺失时静默跳过依旧成立）。"""

    def test_source_in_quiet_period_skips_sync(self, run_task, tmp_path, stub_procs, capture_log):
        make_tree(tmp_path / "src", {"a.txt": (b"x", RECENT)})
        os.makedirs(tmp_path / "dest")

        run_task(sync_task(tmp_path, source_quiet_seconds=300), now=NOW)

        assert stub_procs.calls == []
        assert snapshot(tmp_path / "src") == {"a.txt": 1}
        assert capture_log.has("info", "源目录最近修改于")

    def test_quiet_source_syncs_normally(self, run_task, tmp_path, stub_procs, prepared_dirs):
        """源目录已静置超过阈值：同步照常发起。"""
        run_task(sync_task(tmp_path, source_quiet_seconds=30), now=NOW)

        assert len(stub_procs.calls) == 1


class TestSshSyncCommands:
    """SSH 远端同步：只验证传给外部工具的命令行与凭据处理，
    SSH 连接本身（ops.is_remote_dir 的远端探测）打桩跳过。"""

    SSH_REMOTE = SshRemote(alias="vps", host="192.168.1.100", user="root")

    @pytest.fixture
    def ssh_ready(self, monkeypatch):
        monkeypatch.setattr("src.ssh.ops.is_remote_dir", lambda remote, path: True)

    def test_rsync_ssh_command_with_port_and_key(self, run_task, tmp_path, stub_procs, ssh_ready):
        src = make_tree(tmp_path / "src", {"a.txt": b"x"})
        remote = SshRemote(alias="vps", host="192.168.1.100", user="root", port=2222, key_file="/k")
        run_task(
            sync_task(tmp_path, tool="rsync", dest="{ssh:vps}?/remote/data"),
            ssh_remotes={"vps": remote},
        )

        cmd = stub_procs.calls[0].cmd
        assert cmd[:3] == ["rsync", "-av", "--delete"]
        assert cmd[3:5] == [
            "-e",
            (
                "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
                "-o LogLevel=ERROR -p 2222 -i /k"
            ),
        ]
        assert cmd[-2:] == [src + "/", "root@192.168.1.100:/remote/data"]
        assert "--delete-excluded" in cmd

    def test_rsync_ssh_password_file_wraps_sshpass(self, run_task, tmp_path, stub_procs, ssh_ready):
        make_tree(tmp_path / "src", {"a.txt": b"x"})
        remote = SshRemote(alias="vps", host="192.168.1.100", user="root", password_file="/pass")
        run_task(
            sync_task(tmp_path, tool="rsync", dest="{ssh:vps}?/remote/data"),
            ssh_remotes={"vps": remote},
        )

        rsh = stub_procs.calls[0].cmd[4]
        assert rsh.startswith("sshpass -f /pass ssh ")

    def test_rsync_ssh_exclude_from_forwarded(self, run_task, tmp_path, stub_procs, ssh_ready):
        make_tree(tmp_path / "src", {"a.txt": b"x"})
        exclude_file = tmp_path / "exclude.txt"
        exclude_file.write_text("*.log\n", encoding="utf-8")
        remote = SshRemote(alias="vps", host="192.168.1.100", user="root")
        run_task(
            sync_task(
                tmp_path,
                tool="rsync",
                dest="{ssh:vps}?/remote/data",
                exclude_from=str(exclude_file),
            ),
            ssh_remotes={"vps": remote},
        )

        # 规则文件由本地 rsync 读取：路径仍是本地路径，与目标是否为远端无关
        assert f"--exclude-from={exclude_file}" in stub_procs.calls[0].cmd

    def test_rclone_ssh_exclude_from_forwarded(self, run_task, tmp_path, stub_procs, ssh_ready):
        make_tree(tmp_path / "src", {"a.txt": b"x"})
        exclude_file = tmp_path / "exclude.txt"
        exclude_file.write_text("*.log\n", encoding="utf-8")
        remote = SshRemote(alias="vps", host="192.168.1.100", user="root")
        run_task(
            sync_task(
                tmp_path,
                tool="rclone",
                dest="{ssh:vps}?/remote/data",
                exclude_from=str(exclude_file),
            ),
            ssh_remotes={"vps": remote},
        )

        cmd = stub_procs.calls[0].cmd
        assert cmd[cmd.index("--exclude-from") + 1] == str(exclude_file)

    def test_rclone_ssh_command(self, run_task, tmp_path, stub_procs, ssh_ready):
        src = make_tree(tmp_path / "src", {"a.txt": b"x"})
        remote = SshRemote(alias="vps", host="192.168.1.100", user="root", port=2222, key_file="/k")
        run_task(
            sync_task(tmp_path, tool="rclone", dest="{ssh:vps}?/remote/data"),
            ssh_remotes={"vps": remote},
        )

        assert stub_procs.calls[0].cmd == [
            "rclone",
            "sync",
            src,
            ":sftp:/remote/data",
            "-v",
            "--sftp-host",
            "192.168.1.100",
            "--sftp-user",
            "root",
            "--sftp-known-hosts-file",
            "none",
            "--sftp-shell-type",
            "none",
            "--sftp-port",
            "2222",
            "--sftp-key-file",
            "/k",
            "--transfers",
            "1",
            "--delete-excluded",
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


class _FakeSyncProc:
    """可编程的假同步进程：stdout 产出预置行并执行预置回调。

    回调用于在"进程运行期间"精确注入停止指令（模拟 stop 指令在
    外部工具执行中途到达），回调本身调用真实的停止协调器。
    """

    pid = 999_999_999  # 不存在的 PID；终止动作由边界桩记录
    returncode = 0

    def __init__(self, after_first_line=None):
        def _gen():
            yield "tool: working\n"
            if after_first_line:
                after_first_line()

        self.stdout = _gen()

    def wait(self):
        return None


class TestStopControl:
    """sync 模式与停止指令的交互（tests/README.md 原则一/三）。"""

    def test_stop_preset_skips_task(
        self, run_task, app_context, tmp_path, stub_procs, prepared_dirs
    ):
        app_context.lifecycle.request_stop_after_file()
        run_task(sync_task(tmp_path, tool="rclone"))

        assert stub_procs.calls == []

    def test_graceful_during_sync_lets_tool_finish(
        self, run_task, app_context, tmp_path, prepared_dirs, capture_log, monkeypatch
    ):
        """graceful 在 sync 执行期间到达：外部工具自然退出，不终止进程。"""
        killed = []
        monkeypatch.setattr(
            "src.app.lifecycle.terminate_process_tree",
            lambda pid, **kwargs: killed.append(pid),
        )

        def on_first_line():
            app_context.lifecycle.request_stop_after_file()

        monkeypatch.setattr(subprocess, "Popen", lambda cmd, **kwargs: _FakeSyncProc(on_first_line))
        monkeypatch.setattr(shutil, "which", lambda name: f"/fake/bin/{name}")

        run_task(sync_task(tmp_path, tool="rclone"))

        assert capture_log.has("success", "同步完成")
        assert killed == []

    def test_fast_during_sync_terminates_process_tree(
        self, run_task, app_context, tmp_path, prepared_dirs, capture_log, monkeypatch
    ):
        """fast 在 sync 执行期间到达：整棵进程树被终止（不留孤儿）。"""
        killed = []
        monkeypatch.setattr(
            "src.app.lifecycle.terminate_process_tree",
            lambda pid, **kwargs: killed.append(pid),
        )

        def on_first_line():
            app_context.lifecycle.request_abort()

        fake = _FakeSyncProc(on_first_line)
        monkeypatch.setattr(subprocess, "Popen", lambda cmd, **kwargs: fake)
        monkeypatch.setattr(shutil, "which", lambda name: f"/fake/bin/{name}")

        run_task(sync_task(tmp_path, tool="rclone"))

        assert killed == [fake.pid]
        # 被终止的同步不得被定性为"同步失败"（外部进程结果不再有意义）
        assert not capture_log.has("error", "同步失败")
