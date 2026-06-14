"""服务器端并发控制：独立的元数据/上传信号量 + 等待队列。

请求到达时先尝试非阻塞获取信号量；若槽位已满则进入 FIFO 队列等待。
队列满则立即拒绝；等待超时或客户端断连则静默移除。
"""

import time
import threading
from collections import deque
from contextlib import contextmanager


class QueueFullError(Exception):
    """等待队列已满，无法接纳新请求。"""


class QueueTimeoutError(Exception):
    """排队等待超时。"""


class ClientDisconnectedError(Exception):
    """客户端在排队期间断开连接。"""


class _Gate:
    """单个请求类型的门控（一个信号量 + 一个 FIFO 等待队列）。

    使用 threading.Event 避免 Condition.notify/wait 之间的竞态：
    - _wake_next() 获取信号量后设置等待者的 Event
    - 等待者通过 Event 获知自己已被授予槽位，无需再次竞争信号量
    """

    def __init__(self, max_concurrent, max_queue):
        self._sem = threading.BoundedSemaphore(max_concurrent)
        self._max_queue = max_queue
        self._lock = threading.Lock()
        # deque 元素: {"event": threading.Event, "addr": str, "arrived": float}
        self._waiters = deque()
        self._queue_count = 0

    def _wake_next(self):
        """尝试将信号量授予队列中的下一个等待者。

        调用方必须已经 release 信号量（或准备将信号量直接转交）。
        此方法获取信号量并设置等待者的 Event，等待者醒来时无需再竞争。
        """
        with self._lock:
            while self._waiters:
                entry = self._waiters[0]
                if self._sem.acquire(blocking=False):
                    self._waiters.popleft()
                    self._queue_count -= 1
                    entry["event"].set()
                    return
                # 信号量被其他线程抢先获取，退出（当前 release
                # 已增加可用计数，队头等待者会在下次 release 时被唤醒）
                return

    def _cleanup(self, event, got_slot):
        """从队列中移除等待者，若已获槽位则释放信号量并唤醒下一个。

        Args:
            event: 等待者的 threading.Event。
            got_slot: 布尔值，表示是否已被 _wake_next 授予槽位。
        """
        with self._lock:
            for i, e in enumerate(self._waiters):
                if e["event"] is event:
                    del self._waiters[i]
                    self._queue_count -= 1
                    break
        if got_slot:
            self._sem.release()
            self._wake_next()

    @contextmanager
    def acquire(self, client_addr, queue_timeout, check_disconnect=None):
        """尝试获取处理槽位。

        Args:
            client_addr: 客户端地址（日志用途）。
            queue_timeout: 最大排队等待秒数；0 表示不排队，无槽位时立即抛 QueueFullError。
            check_disconnect: 可选的无参可调用对象，返回 True 表示客户端可能已断连。

        Yields:
            None（上下文管理器仅用于 acquire/release 配对）。

        Raises:
            QueueFullError: 队列已满或 queue_timeout=0 且无可用槽位。
            QueueTimeoutError: 排队等待超时。
            ClientDisconnectedError: 客户端断连。
        """
        # 1. 尝试立即获取信号量
        if self._sem.acquire(blocking=False):
            try:
                yield
            finally:
                self._sem.release()
                self._wake_next()
            return

        # 2. 不排队模式 → 立即拒绝
        if queue_timeout <= 0:
            raise QueueFullError("服务器繁忙，未启用排队")

        # 3. 进入等待队列
        event = threading.Event()
        arrived = time.time()
        entry = {"event": event, "addr": client_addr, "arrived": arrived}

        with self._lock:
            if self._queue_count >= self._max_queue:
                raise QueueFullError(
                    f"队列已满 ({self._queue_count}/{self._max_queue})"
                )
            self._waiters.append(entry)
            self._queue_count += 1

        got_slot = False
        try:
            # 4. 轮询等待（每次最多 1 秒，便于断连检测和超时检查）
            while not event.is_set():
                elapsed = time.time() - arrived
                if elapsed >= queue_timeout:
                    raise QueueTimeoutError(
                        f"排队超时 ({queue_timeout}s)"
                    )

                # 检查客户端断连
                if check_disconnect is not None:
                    try:
                        if check_disconnect():
                            raise ClientDisconnectedError(
                                f"客户端 {client_addr} 已断开连接"
                            )
                    except ClientDisconnectedError:
                        raise
                    except Exception:
                        # check_disconnect 自身异常视为未断连
                        pass

                # 等待被唤醒或 1 秒后超时重试
                event.wait(timeout=1.0)

            # 5. Event 已被 _wake_next 设置 → 信号量已由 _wake_next 为我们获取
            got_slot = True
            try:
                yield
            finally:
                self._sem.release()
                self._wake_next()

        except (QueueTimeoutError, ClientDisconnectedError):
            self._cleanup(event, got_slot)
            raise
        except Exception:
            self._cleanup(event, got_slot)
            raise


class ConcurrencyGate:
    """服务器端并发门控。

    分离元数据请求和上传请求的并发控制，各自拥有独立的信号量和等待队列。
    """

    def __init__(
        self,
        max_concurrent_metadata,
        max_concurrent_uploads,
        max_queue_metadata,
        max_queue_uploads,
    ):
        self._metadata_gate = _Gate(max_concurrent_metadata, max_queue_metadata)
        self._upload_gate = _Gate(max_concurrent_uploads, max_queue_uploads)

    @contextmanager
    def metadata_context(self, client_addr, queue_timeout, check_disconnect=None):
        """元数据请求的并发控制上下文管理器。"""
        with self._metadata_gate.acquire(
            client_addr, queue_timeout, check_disconnect
        ):
            yield

    @contextmanager
    def upload_context(self, client_addr, queue_timeout, check_disconnect=None):
        """上传请求的并发控制上下文管理器。"""
        with self._upload_gate.acquire(
            client_addr, queue_timeout, check_disconnect
        ):
            yield

    @property
    def metadata_queue_depth(self):
        """当前元数据队列长度（仅用于信息展示）。"""
        return self._metadata_gate._queue_count

    @property
    def upload_queue_depth(self):
        """当前上传队列长度（仅用于信息展示）。"""
        return self._upload_gate._queue_count
