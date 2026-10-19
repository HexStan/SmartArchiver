"""白盒·服务端上传原子化（进程/网络边界测试）。

为什么必须白盒：上传的字节校验、Content-Length 不一致、X-Mtime 元数据
都是 HTTP 协议契约，灰盒入口够不到。驱动方式为 create_app + Flask
test_client 构造真实请求——测试客户端即进程边界，符合 tests/README.md
原则三。文件系统的终态断言（内容、mtime、无临时文件残留）落在真实磁盘上。
"""

import os
import time

import pytest

from src.remote.concurrency import ConcurrencyGate
from src.remote.protocol import UPLOAD_PATH
from src.remote.server import create_app

API_KEY = "test-key"


@pytest.fixture
def server(tmp_path, capture_log):
    """返回 (test_client, 存储根目录)；上传路径使用绝对路径写入该根目录。"""
    gate = ConcurrencyGate(
        max_concurrent_metadata=4,
        max_concurrent_uploads=2,
        max_queue_metadata=8,
        max_queue_uploads=4,
    )
    app = create_app(API_KEY, capture_log, gate)
    app.config["TESTING"] = True
    return app.test_client(), tmp_path


def _post(client, root, rel="dest/a.txt", body=b"hello", extra_headers=None):
    headers = {
        "X-Api-Key": API_KEY,
        "X-Path": str(root / rel),
        "Content-Type": "application/octet-stream",
    }
    if extra_headers:
        headers.update(extra_headers)
    return client.post(UPLOAD_PATH, data=body, headers=headers)


class _ShortReadStream:
    """WSGI 输入流替身：声称有 10 字节（tell/seek 按完整数据报告），
    read 却只发出前 3 字节就提前 EOF。

    Werkzeug 测试客户端会依据流的真实长度回填 Content-Length，无法直接
    构造"谎报长度"的请求；且其 LimitedStream 会优先走底层 readinto，
    继承 BytesIO 重写 read 会被绕过。该替身只暴露 read/tell/seek，
    精确等价于"客户端声称发送 10 字节、实际发送 3 字节后断连"——
    Werkzeug 对短读抛 ClientDisconnected，服务端经 Flask 落成 400 响应。
    """

    def __init__(self, data, sent):
        self._data = data
        self._remaining = sent
        self._pos = 0

    def read(self, size=-1):
        if size is None or size < 0:
            size = self._remaining
        n = min(size, self._remaining)
        chunk = self._data[self._pos : self._pos + n]
        self._pos += len(chunk)
        self._remaining -= len(chunk)
        return chunk

    def tell(self):
        return self._pos

    def seek(self, offset, whence=0):
        if whence == 2:
            self._pos = len(self._data)
        else:
            self._pos = max(0, offset)
        return self._pos


class TestSuccessfulUpload:
    def test_writes_file_with_mtime_from_header(self, server):
        client, root = server
        mtime = 1000000000.0

        resp = _post(client, root, extra_headers={"X-Mtime": str(mtime)})

        assert resp.status_code == 200
        assert resp.get_json() == {"success": True}
        dest = root / "dest" / "a.txt"
        assert dest.read_bytes() == b"hello"
        assert os.stat(dest).st_mtime == pytest.approx(mtime, abs=1e-3)
        # 无临时文件残留
        assert [p.name for p in (root / "dest").iterdir()] == ["a.txt"]

    def test_without_mtime_header_writes_recently(self, server):
        """向后兼容：旧客户端不带 X-Mtime 时，文件照常落盘。"""
        client, root = server

        resp = _post(client, root)

        assert resp.status_code == 200
        dest = root / "dest" / "a.txt"
        assert dest.read_bytes() == b"hello"
        assert abs(time.time() - os.stat(dest).st_mtime) < 60

    def test_overwrites_existing_dest_atomically(self, server):
        """重复上传（重试场景）通过 rename 原子替换，不留下半截文件。"""
        client, root = server
        dest = root / "dest" / "a.txt"
        dest.parent.mkdir(parents=True)
        dest.write_bytes(b"old-content")

        resp = _post(client, root, body=b"new-content")

        assert resp.status_code == 200
        assert dest.read_bytes() == b"new-content"
        assert [p.name for p in (root / "dest").iterdir()] == ["a.txt"]


class TestFailedUpload:
    def test_truncated_upload_rejected_without_residue(self, server):
        """声称 10 字节实际只发 3 字节即断连：拒绝写入，最终路径不受影响。

        Werkzeug 对短读抛 ClientDisconnected，Flask 落成 400 响应；
        断连异常路径中 atomic_writer 的 finally 会清理临时文件。
        """
        client, root = server

        resp = client.post(
            UPLOAD_PATH,
            input_stream=_ShortReadStream(b"0123456789", sent=3),
            headers={
                "X-Api-Key": API_KEY,
                "X-Path": str(root / "dest" / "a.txt"),
                "Content-Type": "application/octet-stream",
            },
        )

        assert resp.status_code == 400
        assert not (root / "dest" / "a.txt").exists()
        assert list((root / "dest").iterdir()) == []

    def test_invalid_mtime_header_rejected(self, server):
        client, root = server

        resp = _post(client, root, extra_headers={"X-Mtime": "not-a-number"})

        assert resp.status_code == 400
        assert not (root / "dest" / "a.txt").exists()

    def test_wrong_api_key_rejected(self, server):
        client, root = server

        resp = _post(client, root, extra_headers={"X-Api-Key": "wrong"})

        assert resp.status_code == 403
        assert not (root / "dest" / "a.txt").exists()


class TestPermissions:
    @pytest.mark.skipif(os.name != "posix", reason="POSIX 权限位仅 POSIX 可断言")
    def test_uploaded_file_is_world_readable(self, server):
        """临时文件兜底权限 0600（mkstemp 回退路径）必须还原为常规 0644。"""
        client, root = server

        resp = _post(client, root)

        assert resp.status_code == 200
        dest = root / "dest" / "a.txt"
        assert os.stat(dest).st_mode & 0o777 == 0o644
