"""测试公共设施。

分层策略与编写原则见 tests/README.md。本文件只提供三类设施：
- 捕获型日志替身（记录不拦截）；
- AppContext 装配（真实 HistoryManager + 捕获日志，测试后清理单例）；
- run_task 灰盒入口（从 handler.execute() 驱动被测代码）。
"""

import pytest

from src.app.context import AppContext
from src.core.history import HistoryManager
from src.core.registry import create_handler


class CaptureLogger:
    """捕获型日志替身：接口与 setup_logger 返回的 logger 对齐，
    只记录消息不产生任何输出，供测试断言日志行为。"""

    def __init__(self):
        self.messages = []  # [(level, message), ...]

    def _log(self, level, msg, **kwargs):
        self.messages.append((level, msg))

    def debug(self, msg, **kwargs):
        self._log("debug", msg, **kwargs)

    def info(self, msg, **kwargs):
        self._log("info", msg, **kwargs)

    def success(self, msg, **kwargs):
        self._log("success", msg, **kwargs)

    def warning(self, msg, **kwargs):
        self._log("warning", msg, **kwargs)

    def error(self, msg, **kwargs):
        self._log("error", msg, **kwargs)

    def critical(self, msg, **kwargs):
        self._log("critical", msg, **kwargs)

    def has(self, level, substring):
        """是否存在包含指定子串的某级别日志。"""
        return any(lv == level and substring in m for lv, m in self.messages)


@pytest.fixture
def capture_log():
    return CaptureLogger()


@pytest.fixture
def app_context(tmp_path, capture_log):
    """装配全局 AppContext：捕获日志 + 真实 HistoryManager（写入 tmp 目录）。"""
    history_dir = tmp_path / "history"
    history_dir.mkdir()
    history = HistoryManager(str(history_dir))
    AppContext.init(capture_log, history, {"max_retries": 3})
    yield AppContext.get()
    AppContext._instance = None


@pytest.fixture
def run_task(app_context):
    """灰盒测试统一入口：以真实文件系统为环境执行一个任务。

    用法::

        handler = run_task({"mode": "move", "source": ..., ...}, now=NOW)

    返回 handler 实例，可读取 stats 做补充断言；任务在构造期被跳过
    （如容量规则配置错误）时返回 None。主断言应落在目录树快照上。
    远程后端测试通过 remote_clients / ssh_remotes 注入（见 test_greybox_sync.py）。
    """

    def _run(task, now=None, remote_clients=None, ssh_remotes=None):
        # 与生产 process_task 共用同一条装配路径（含构造期配置错误的
        # 捕获语义），错误路径测试才是在测生产行为而不是测试替身。
        handler = create_handler(
            task, now=now, remote_clients=remote_clients, ssh_remotes=ssh_remotes
        )
        if handler is None:
            return None
        handler.execute()
        return handler

    return _run
