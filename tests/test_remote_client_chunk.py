"""白盒·HTTP 客户端上传分块（装配契约）。

为什么必须白盒（tests/README.md 原则二）：urllib3 按连接的 blocksize
分块读取文件型请求体并逐块 sendall，分块大小是纯性能契约——从任务入口
与最终目录树均不可观察；经真实回环观察收包大小又受 TCP 分段与时序影响
（一次 1MB 发送可能被服务端拆成多次读），不确定。因此钉死装配本身：
RemoteClient 的会话必须以 _UploadBlocksizeAdapter 挂载，且该适配器构造
出的真实连接携带 blocksize == UPLOAD_CHUNK_SIZE——urllib3 读取文件体
的实际粒度来源（body_to_chunks 的 blocksize 取自 connection.blocksize）。

1MB 分块契约的另一半（服务端接收侧）由 test_server_direct_write.py 的
探针流以确定性计数钉死。
"""

from src.remote.client import RemoteClient, _UploadBlocksizeAdapter
from src.remote.protocol import UPLOAD_CHUNK_SIZE

# 不会真正拨号：本文件只做装配断言，不发起任何连接
ADDRESS = "http://127.0.0.1:1"


def _client() -> RemoteClient:
    return RemoteClient(ADDRESS, "test-key")


def test_session_mounts_upload_blocksize_adapter():
    """http/https 前缀均挂载 _UploadBlocksizeAdapter（含上传请求在内的全部请求）。"""
    client = _client()
    try:
        for scheme in ("http://", "https://"):
            adapter = client._session.get_adapter(f"{scheme}example.org")
            assert isinstance(adapter, _UploadBlocksizeAdapter)
    finally:
        client._session.close()


def test_poolmanager_carries_upload_blocksize():
    """适配器把 UPLOAD_CHUNK_SIZE 写入连接池关键字。"""
    client = _client()
    try:
        adapter = client._session.get_adapter("http://example.org")
        assert isinstance(adapter, _UploadBlocksizeAdapter)
        assert adapter.poolmanager.connection_pool_kw.get("blocksize") == UPLOAD_CHUNK_SIZE
    finally:
        client._session.close()


def test_new_connection_reads_body_in_upload_chunk_size():
    """从适配器的连接池真实构造连接：blocksize 即 urllib3 分块读取
    文件型请求体的粒度，必须等于 UPLOAD_CHUNK_SIZE。

    _new_conn 只实例化连接对象，不建立 TCP 连接。
    """
    client = _client()
    try:
        adapter = client._session.get_adapter("http://example.org")
        assert isinstance(adapter, _UploadBlocksizeAdapter)
        pool = adapter.poolmanager.connection_from_url("http://example.org")
        conn = pool._new_conn()
        assert conn.blocksize == UPLOAD_CHUNK_SIZE
    finally:
        client._session.close()
