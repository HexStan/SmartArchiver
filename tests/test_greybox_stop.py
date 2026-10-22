"""灰盒测试：停止指令下的任务行为（graceful / slow / fast）。

停止协调器像 now 一样由测试注入（AppContext 自动装配 ShutdownCoordinator）：
- graceful / slow / fast 预置在任务开始前，从 execute() 入口断言边界行为；
- "当前文件传完后停止"与"传输中途丢弃任务"借助 fs.abort 的中止检查钩子在
  chunk 边界精确触发——该钩子是生产注入缝（生产环境由停止协调器注入），
  不是业务打桩。
主断言落在目录树快照上（原则五）。
"""

import os

import pytest

from src import fs
from src.fs import TransferAbortedError
from tests.helpers import make_tree, snapshot


@pytest.fixture
def abort_hook(app_context):
    """fs.abort 中止检查钩子：handlers 列表中的回调按调用次序触发，
    返回 True 表示中止（测试后复位钩子）。"""
    calls = {"count": 0}
    handlers = []

    def check():
        calls["count"] += 1
        for handler in handlers:
            if handler(calls["count"]):
                return True
        return False

    fs.set_abort_check(check)
    yield handlers
    fs.set_abort_check(None)


def make_task(src, dest, mode):
    os.makedirs(dest, exist_ok=True)  # 传输前的前置检查要求目标目录已存在
    return {
        "mode": mode,
        "source": str(src),
        "dest": str(dest),
        "mtime_threshold_seconds": 0,
        "remove_empty_dirs": False,
    }


def _is_empty(path):
    return not path.exists() or os.listdir(path) == []


class TestStopBeforeTask:
    def test_graceful_preset_processes_nothing(self, run_task, app_context, tmp_path):
        """任务开始前已请求 graceful 停止：整个任务跳过，目录树不变。"""
        src = make_tree(tmp_path / "src", {"a.txt": b"a", "b.txt": b"b"})
        dest = tmp_path / "dest"

        app_context.shutdown.request_stop_after_file()
        run_task(make_task(src, dest, "copy"))

        assert snapshot(src) == {"a.txt": 1, "b.txt": 1}
        assert _is_empty(dest)

    def test_fast_preset_processes_nothing(self, run_task, app_context, tmp_path):
        """任务开始前已 fast 停止：文件边界干净拦下，无异常无残留。"""
        src = make_tree(tmp_path / "src", {"a.txt": b"a", "b.txt": b"b"})
        dest = tmp_path / "dest"

        app_context.shutdown.request_abort()
        run_task(make_task(src, dest, "copy"))

        assert snapshot(src) == {"a.txt": 1, "b.txt": 1}
        assert _is_empty(dest)

    def test_slow_preset_lets_whole_round_finish(self, run_task, app_context, tmp_path):
        """slow 语义不作用于文件/任务边界，本轮完整执行。"""
        src = make_tree(tmp_path / "src", {"a.txt": b"a", "b.txt": b"b"})
        dest = tmp_path / "dest"

        app_context.shutdown.request_stop_after_round()
        run_task(make_task(src, dest, "copy"))

        assert snapshot(dest) == {"a.txt": 1, "b.txt": 1}
        assert snapshot(src) == {"a.txt": 1, "b.txt": 1}  # copy 模式源文件保留


class TestStopMidFile:
    # chunk 边界只存在于复制循环中（同文件系统的 move 走 os.replace，
    # 单次系统调用即完成），因此本组全部使用 copy 模式。
    MODE = "copy"

    def test_graceful_stops_after_current_file_completes(
        self, run_task, app_context, tmp_path, abort_hook
    ):
        """graceful 语义：正在传输的文件完整落地，后续文件不再开始。

        钩子在第一个文件的 chunk 边界翻转 graceful 信号（文件本身
        正常完成），文件边界检查应拦下第二个文件。
        """
        src = make_tree(tmp_path / "src", {"a.txt": b"a", "b.txt": b"b"})
        dest = tmp_path / "dest"

        abort_hook.append(lambda _call: app_context.shutdown.request_stop_after_file() or False)
        run_task(make_task(src, dest, self.MODE))

        assert snapshot(src) == {"a.txt": 1, "b.txt": 1}
        assert snapshot(dest) == {"a.txt": 1}

    def test_abort_discards_in_progress_task(self, run_task, tmp_path, abort_hook, capture_log):
        """fast 中止：任务被整体丢弃——目标目录无文件、源文件完好、
        后续文件不再开始、不计入文件失败次数（无失败日志）。"""
        src = make_tree(tmp_path / "src", {"a.txt": b"a", "b.txt": b"b"})
        dest = tmp_path / "dest"

        abort_hook.append(lambda _call: True)  # 首个 chunk 边界即中止
        with pytest.raises(TransferAbortedError):
            run_task(make_task(src, dest, self.MODE))

        assert _is_empty(dest)
        assert snapshot(src) == {"a.txt": 1, "b.txt": 1}
        assert not capture_log.has("error", "复制文件失败")
        assert not capture_log.has("error", "移动文件失败")

    def test_abort_mid_file_leaves_no_temp_residue(self, run_task, tmp_path, abort_hook):
        """大文件传输到第 3 次 chunk 边界检查时中止：目标无最终文件、
        无临时文件残留。"""
        big = os.urandom(1024 * 1024 + 7)  # 超过 1 个复制 chunk
        src = make_tree(tmp_path / "src", {"a_big.bin": big, "b.txt": b"b"})
        dest = tmp_path / "dest"

        abort_hook.append(lambda call: call >= 3)
        with pytest.raises(TransferAbortedError):
            run_task(make_task(src, dest, self.MODE))

        assert snapshot(src) == {"a_big.bin": len(big), "b.txt": 1}
        assert _is_empty(dest)

    def test_aborted_transfer_not_recorded_as_history_failure(
        self, run_task, app_context, tmp_path, abort_hook
    ):
        """被丢弃的任务不得写入 history 失败记录（否则重试机制会惩罚它）。"""
        src = make_tree(tmp_path / "src", {"a.txt": b"a"})
        dest = tmp_path / "dest"
        src_path = str(tmp_path / "src" / "a.txt")

        abort_hook.append(lambda _call: True)
        with pytest.raises(TransferAbortedError):
            run_task(make_task(src, dest, self.MODE))

        should_skip, fail_count = app_context.history_mgr.should_skip(src_path, 3)
        assert not should_skip
        assert fail_count == 0
