"""停止指令协调器。

运行实例收到停止指令后的行为由三级信号驱动：

- graceful（``stop`` 默认）——当前文件传输完成后停止；
- slow（``stop --slow``）——本轮任务全部结束后停止；
- fast（``stop --fast``）——立即中止文件传输、丢弃当前任务，
  清理已写入的临时文件后退出。

信号由 ``src.app.instance.ControlWatcher`` 从控制文件读入后注入；业务循环
在文件/任务边界查询，``fs.transfer`` 的复制循环在 chunk 边界查询 fast 中止。
sync 模式的外部同步进程在启动时登记，fast 中止时被整树终止，不留孤儿。
"""

import threading

from src import fs
from src.app.instance import terminate_process_tree


class ShutdownCoordinator:
    """三级停止信号的汇聚点与外部同步进程的登记处。

    每个运行实例持有唯一实例（经 AppContext 分发）：``ControlWatcher``
    线程负责注入信号，业务代码只读状态。
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._stop_after_file = False
        self._stop_after_round = False
        self._abort_event = threading.Event()
        # 任何停止指令都会触发，用于打断调度循环的长睡眠
        self._wake_event = threading.Event()
        self._children = []

    # ---- 指令注入（ControlWatcher 调用） ----------------------------

    def request_stop_after_file(self):
        """graceful：当前文件传输完后停止。"""
        self._stop_after_file = True
        self._wake_event.set()

    def request_stop_after_round(self):
        """slow：本轮任务结束后停止。"""
        self._stop_after_round = True
        self._wake_event.set()

    def request_abort(self, logger=None):
        """fast：立即中止传输，并终止已登记的外部同步进程树。"""
        self._abort_event.set()
        self._wake_event.set()
        self.terminate_children(logger)

    # ---- 状态查询（业务循环调用） ----------------------------------

    @property
    def stop_after_file(self):
        return self._stop_after_file

    @property
    def stop_after_round(self):
        return self._stop_after_round

    def is_aborted(self):
        """是否收到 fast 停止指令。"""
        return self._abort_event.is_set()

    def stop_requested(self):
        """是否收到任意停止指令（调度主循环的退出条件）。"""
        return self._stop_after_file or self._stop_after_round or self._abort_event.is_set()

    def should_stop_before_file(self):
        """文件级边界检查：当前文件传完后是否应停止。

        graceful 与 fast 在此生效（fast 通常已在 chunk 边界中止传输）；
        slow 不在此停止，本轮任务继续执行。
        """
        return self._stop_after_file or self._abort_event.is_set()

    # 任务循环与文件循环的停止语义一致，共用同一判定
    should_stop_before_task = should_stop_before_file

    def wait_for_wake(self, timeout):
        """阻塞等待任意停止指令或超时（调度睡眠用）。

        返回 True 表示期间收到了停止指令。
        """
        return self._wake_event.wait(timeout)

    # ---- 外部同步进程登记（sync 模式） ------------------------------

    def register_process(self, process):
        with self._lock:
            self._children.append(process)

    def unregister_process(self, process):
        with self._lock:
            if process in self._children:
                self._children.remove(process)

    def terminate_children(self, logger=None):
        """终止所有已登记的外部进程及其后代。"""
        with self._lock:
            children = list(self._children)
        for process in children:
            terminate_process_tree(process.pid, logger=logger)


def install_abort_check(coordinator):
    """把 fast 中止信号接入 fs.transfer 的 chunk 边界。"""
    fs.set_abort_check(coordinator.is_aborted)
