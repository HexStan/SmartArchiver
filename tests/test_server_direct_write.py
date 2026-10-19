"""白盒·HTTP 上传直写目标目录（不经过系统临时目录）。

为什么必须白盒（tests/README.md 原则二）：
「先写入系统临时目录、接收完毕再移动至目标目录」与「直写目标目录」的
最终目录树完全相同，事后快照无法区分——只能在传输过程中（消费请求体时
的文件系统状态）、落盘边界（改名调用及其瞬间的临时文件内容）与服务器
选型（启动装配）上观察。该契约是 ROADMAP「已放弃·迁移至生产服务器」
条目的核心：waitress 会把请求体先缓冲到系统分区再移动，造成写放大并
威胁系统分区空间，cd11a3a 为此回滚为 Flask 内置服务器。

打桩全部落在 OS / 系统临时目录 / 网络边界（原则三）：
- 探针流占据网络边界（wsgi.input），每次消费请求体前对目录结构做快照；
- os.replace / os.rename / tempfile.* 均为直通录制（记录不拦截），
  落盘瞬间回读临时文件内容——此时 fsync 已完成，内容完整且确定；
- tempfile.tempdir 重定向到受控目录：任何按「系统临时目录默认值」落盘
  的路径都会出现在该目录中，全程必须为空。

注意：传输中途不断言临时文件的可见字节数——写入方是 BufferedWriter，
大块写入在 flush 前对其他句柄不可见（用户态缓冲，行为跨 CPython 版本
不稳定）；确定性观察点是「目录结构（谁存在、在哪）」与「落盘瞬间的
完整内容」。

分三层钉死契约：
1. 应用层（create_app + test_client + 探针流）：接收目标自始至终是目标
   目录内的临时文件，落盘只靠目标目录内的一次 os.replace；
2. 回环层（make_server 起真实 werkzeug serving 栈 + 真实 RemoteClient）：
   相同断言穿过真实 socket 与真实客户端；
3. 装配层（run_server）：服务器必须以内置开发服务器启动——应用层与
   回环层都不经过 run_server 的选型（test_client 不经服务器，make_server
   直接指定 werkzeug），若换回 waitress 类缓冲型服务器，只有这一层能察觉。
"""

import os
import tempfile
import threading
from pathlib import Path

import pytest
from flask import Flask
from werkzeug.serving import make_server

from src import fs_ops
from src.remote.client import RemoteClient
from src.remote.concurrency import ConcurrencyGate
from src.remote.protocol import UPLOAD_PATH
from src.remote.server import create_app, run_server

API_KEY = "test-key"
# 与 src/remote/server.py::_receive_upload 的读取块一致；BODY 恰好横跨两个块
CHUNK_SIZE = 65536
BODY = bytes(range(256)) * 256 + b"tail-of-body"
PARTIAL_NAME = ".a.txt" + fs_ops._PARTIAL_SUFFIX
SOURCE_MTIME = 1000000000.0


def _same_dir(a, b):
    """目录路径等价比较（吸收分隔符方向与 Windows 大小写差异）。"""
    return os.path.normcase(os.path.abspath(a)) == os.path.normcase(os.path.abspath(b))


def _dir_names(dir_path):
    """目录条目名快照（只看结构，不读内容）。"""
    return sorted(p.name for p in Path(dir_path).iterdir())


class _DirectWriteProbe:
    """直写探针：捕获型替身组合（只记录不拦截，原则七）。

    - 把 tempfile.tempdir 重定向到受控目录：任何按「系统临时目录默认值」
      落盘的路径都会出现在该目录中，全程必须为空；
    - 直通录制 os.replace / os.rename，并在落盘瞬间回读临时文件内容；
    - 直通录制 tempfile 创建 API：凡未显式指定 dir 者，默认即系统临时目录；
    - read 事件由 _ProbingStream 写入，与改名事件共享同一时序。
    """

    def __init__(self, watch_dir):
        self.watch_dir = Path(watch_dir)
        self.watch_dir.mkdir(parents=True)
        self.events = []  # 有序事件：read / replace / rename
        self.tempfile_calls = []  # [(API 名, kwargs), ...]

    def install(self, monkeypatch):
        monkeypatch.setattr(tempfile, "tempdir", str(self.watch_dir))

        for name in ("replace", "rename"):
            real = getattr(os, name)

            def recorder(src, dst, _real=real, _kind=name):
                src_path = os.path.abspath(src)
                content = None
                if os.path.isfile(src_path):
                    content = Path(src_path).read_bytes()
                self.events.append(
                    {
                        "kind": _kind,
                        "src": src_path,
                        "dst": os.path.abspath(dst),
                        "src_content": content,
                    }
                )
                return _real(src, dst)

            monkeypatch.setattr(os, name, recorder)

        for name in (
            "mkstemp",
            "mkdtemp",
            "NamedTemporaryFile",
            "TemporaryFile",
            "SpooledTemporaryFile",
            "TemporaryDirectory",
        ):
            real = getattr(tempfile, name)

            def recorder(*args, _real=real, _name=name, **kwargs):
                self.tempfile_calls.append((_name, kwargs))
                return _real(*args, **kwargs)

            monkeypatch.setattr(tempfile, name, recorder)

    def record_read(self, delivered, dest_dir):
        """由探针流在消费请求体前调用，记录当时的目录结构。"""
        self.events.append(
            {
                "kind": "read",
                "delivered": delivered,
                "dest_names": _dir_names(dest_dir),
                "watch_names": sorted(p.name for p in self.watch_dir.iterdir()),
            }
        )

    @property
    def reads(self):
        return [e for e in self.events if e["kind"] == "read"]

    def assert_watch_dir_stays_empty(self):
        """受控系统临时目录在传输全程与结束后都不得出现任何条目。"""
        for i, e in enumerate(self.reads):
            assert e["watch_names"] == [], (
                f"第 {i + 1} 次读取请求体时系统临时目录出现: {e['watch_names']}"
            )
        assert sorted(p.name for p in self.watch_dir.iterdir()) == []

    def assert_tempfile_only_targets_dest_dir(self, dest_dir):
        """tempfile 创建 API 若被使用，dir 必须显式指向目标目录。"""
        for name, kwargs in self.tempfile_calls:
            d = kwargs.get("dir")
            assert d is not None, f"tempfile.{name} 未显式指定 dir，将落入系统临时目录"
            assert _same_dir(d, dest_dir), f"tempfile.{name} 的 dir 不是目标目录: {d}"

    def assert_finalized_by(self, dest_path, expected_content):
        """落盘只能由目标目录内的一次 os.replace 完成，且改名瞬间临时文件
        已含全部字节——数据物理产生于目标目录，不存在从别处搬运而来。"""
        kinds = [e["kind"] for e in self.events]
        assert kinds and kinds[-1] == "replace", f"事件序列应以落盘改名结束: {kinds}"
        assert kinds.count("replace") == 1, f"落盘改名发生了多次: {kinds}"
        assert "rename" not in kinds, "出现 os.rename（跨目录搬运的迹象）"

        final = self.events[-1]
        dest_dir = os.path.dirname(os.path.abspath(dest_path))
        assert os.path.basename(final["src"]).endswith(fs_ops._PARTIAL_SUFFIX)
        assert _same_dir(os.path.dirname(final["src"]), dest_dir), (
            f"落盘前的临时文件不在目标目录: {final['src']}"
        )
        assert _same_dir(final["dst"], dest_path), f"落盘目标不是预期路径: {final['dst']}"
        assert final["src_content"] == expected_content, "落盘瞬间临时文件内容不完整"


class _ProbingStream:
    """WSGI 输入流替身：按需吐出字节，并在每次真实消费前记录目录结构。

    只暴露 read/tell/seek，使 werkzeug 的 LimitedStream 走 read 回退路径，
    read 钩子恰好对应 _receive_upload 的每次 read(chunk_size)——机制与
    test_server_upload.py 的 _ShortReadStream 相同。快照只记录目录条目名，
    原因见模块 docstring（用户态缓冲导致中途字节数不可确定）。
    """

    def __init__(self, data, probe, dest_dir):
        self._data = data
        self._pos = 0
        self._probe = probe
        self._dest_dir = dest_dir

    def read(self, size=-1):
        if size is None or size < 0:
            size = len(self._data) - self._pos
        size = max(0, min(size, len(self._data) - self._pos))
        if size > 0:
            self._probe.record_read(self._pos, self._dest_dir)
        chunk = self._data[self._pos:self._pos + size]
        self._pos += len(chunk)
        return chunk

    def tell(self):
        return self._pos

    def seek(self, offset, whence=0):
        if whence == 2:
            self._pos = len(self._data)
        elif whence == 0:
            self._pos = max(0, offset)
        else:
            self._pos = max(0, min(self._pos + offset, len(self._data)))
        return self._pos


def _gate():
    return ConcurrencyGate(
        max_concurrent_metadata=4,
        max_concurrent_uploads=2,
        max_queue_metadata=8,
        max_queue_uploads=4,
    )


@pytest.fixture
def server(tmp_path, capture_log):
    """返回 (test_client, 存储根目录)；上传路径使用绝对路径写入该根目录。"""
    app = create_app(API_KEY, capture_log, _gate())
    app.config["TESTING"] = True
    return app.test_client(), tmp_path


@pytest.fixture
def probe(tmp_path, monkeypatch):
    """已安装的直写探针；watch 目录扮演系统临时目录。"""
    p = _DirectWriteProbe(tmp_path / "system-temp")
    p.install(monkeypatch)
    return p


def _upload_headers(root):
    return {
        "X-Api-Key": API_KEY,
        "X-Path": str(root / "dest" / "a.txt"),
        "Content-Type": "application/octet-stream",
    }


def _post_stream(client, root, probe):
    """以探针流为请求体发起上传（客户端替身即网络边界）。"""
    return client.post(
        UPLOAD_PATH,
        input_stream=_ProbingStream(BODY, probe, root / "dest"),
        headers=_upload_headers(root),
    )


# ============================================================
# 应用层：create_app + test_client + 探针流
# ============================================================


class TestStreamingPlacement:
    def test_temp_file_exists_in_dest_dir_before_first_byte(self, server, probe):
        """消费第一个字节之前，接收目标已锁定为目标目录内的临时文件。"""
        client, root = server

        resp = _post_stream(client, root, probe)

        assert resp.status_code == 200
        first = probe.reads[0]
        assert first["delivered"] == 0
        assert first["dest_names"] == [PARTIAL_NAME]
        assert first["watch_names"] == []

    def test_dest_file_absent_and_partial_persists_mid_transfer(self, server, probe):
        """已消费一整块、尚未接收完毕时：目标目录里仍只有临时文件，
        最终文件不提前出现，系统临时目录没有产生任何东西。"""
        client, root = server

        resp = _post_stream(client, root, probe)

        assert resp.status_code == 200
        second = probe.reads[1]
        assert second["delivered"] == CHUNK_SIZE
        assert second["dest_names"] == [PARTIAL_NAME]
        assert second["watch_names"] == []

    def test_system_temp_dir_receives_nothing(self, server, probe):
        """传输全程与结束后，受控系统临时目录始终为空。"""
        client, root = server

        resp = _post_stream(client, root, probe)

        assert resp.status_code == 200
        probe.assert_watch_dir_stays_empty()


class TestFinalizeBoundary:
    def test_finalized_by_single_replace_within_dest_dir(self, server, probe):
        """落盘 = 目标目录内一次 rename；改名瞬间临时文件已含完整数据。"""
        client, root = server

        resp = _post_stream(client, root, probe)

        assert resp.status_code == 200
        # 时序：读第一块 → 读尾块 → 改名落盘（读尽后不再触碰底层流，不记录）
        assert [e["kind"] for e in probe.events] == ["read", "read", "replace"]
        probe.assert_finalized_by(str(root / "dest" / "a.txt"), BODY)
        assert (root / "dest" / "a.txt").read_bytes() == BODY
        assert [p.name for p in (root / "dest").iterdir()] == ["a.txt"]

    def test_tempfile_apis_never_target_system_temp(self, server, probe):
        """常规路径不触碰 tempfile；即便被使用，dir 也必须显式指向目标目录。"""
        client, root = server

        resp = client.post(UPLOAD_PATH, data=b"hello", headers=_upload_headers(root))

        assert resp.status_code == 200
        probe.assert_tempfile_only_targets_dest_dir(root / "dest")
        probe.assert_finalized_by(str(root / "dest" / "a.txt"), b"hello")

    def test_random_name_fallback_stays_in_dest_dir(self, server, probe):
        """确定性名字被目录占用而回退 mkstemp 时，随机临时文件仍在目标目录。"""
        client, root = server
        (root / "dest").mkdir(parents=True)
        (root / "dest" / PARTIAL_NAME).mkdir()

        resp = client.post(UPLOAD_PATH, data=b"hello", headers=_upload_headers(root))

        assert resp.status_code == 200
        assert [name for name, _ in probe.tempfile_calls] == ["mkstemp"]
        probe.assert_tempfile_only_targets_dest_dir(root / "dest")
        probe.assert_finalized_by(str(root / "dest" / "a.txt"), b"hello")


# ============================================================
# 回环层：真实 werkzeug serving 栈 + 真实 RemoteClient
# ============================================================


@pytest.fixture
def loopback(capture_log):
    """127.0.0.1 随机端口上的真实 werkzeug 服务器，返回就绪的 RemoteClient。

    make_server 与 app.run() 同属 werkzeug.serving 一族（后者内部即
    run_simple → make_server），构造时同步完成 bind+listen，连接进入
    backlog 即可被受理，无需等待。驱动真实 socket 即网络边界（原则三）。
    """
    app = create_app(API_KEY, capture_log, _gate())
    srv = make_server("127.0.0.1", 0, app, threaded=True)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    client = RemoteClient(
        f"http://127.0.0.1:{srv.server_port}", API_KEY, logger=capture_log
    )
    yield client
    client._session.close()
    srv.shutdown()
    thread.join(timeout=10)


class TestLoopbackTransfer:
    def test_real_transfer_writes_directly_into_dest_dir(self, loopback, probe, tmp_path):
        """真实 socket → 真实 serving 栈 → 应用 → 目标目录，全程零系统临时目录介入。"""
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        src = src_dir / "a.txt"
        src.write_bytes(BODY)
        os.utime(src, (SOURCE_MTIME, SOURCE_MTIME))
        dest = tmp_path / "dest" / "a.txt"

        loopback.upload(str(src), str(dest).replace(os.sep, "/"))

        probe.assert_finalized_by(str(dest), BODY)
        probe.assert_tempfile_only_targets_dest_dir(tmp_path / "dest")
        probe.assert_watch_dir_stays_empty()
        assert dest.read_bytes() == BODY
        assert os.stat(dest).st_mtime == pytest.approx(SOURCE_MTIME, abs=1e-3)
        # 客户端只读源文件，不搬运、不留临时副本
        assert src.read_bytes() == BODY
        assert [p.name for p in src_dir.iterdir()] == ["a.txt"]


# ============================================================
# 装配层：run_server 的服务器选型（cd11a3a 的决策点）
# ============================================================


class TestServerAssembly:
    def test_run_server_boots_flask_builtin_dev_server(self, monkeypatch, capture_log):
        """run_server 必须以内置开发服务器（threaded）启动。

        waitress 类生产 WSGI 服务器会把请求体先缓冲到系统临时目录、接收
        完毕再移动至目标目录（ROADMAP「已放弃·迁移至生产服务器」条目、
        cd11a3a）。应用层与回环层都不经过 run_server 的选型，只有钉死
        启动路径才能守住该决策——任何选型变更都必须显式重审并更新本测试。
        打桩方式与 test_greybox_sync.py 拦截 Popen 同理：记录启动参数，
        不假装测试了监听行为。
        """
        boots = []
        monkeypatch.setattr(Flask, "run", lambda self, **kwargs: boots.append(kwargs))

        run_server({"api_key": API_KEY, "port": "23456"}, capture_log)

        assert len(boots) == 1
        assert boots[0] == {
            "host": "0.0.0.0",
            "port": 23456,
            "debug": False,
            "threaded": True,
        }
