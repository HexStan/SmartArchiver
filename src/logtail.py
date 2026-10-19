"""日志跟随器：``log`` 子命令的滚动显示实现。

语义对齐 ``tail -f``：
- 先输出日志文件末尾 N 行，随后持续轮询并输出新增内容；
- 日志按天轮转（``<前缀>.YYYY-MM-DD.log``），出现更新的文件名时自动切换；
- 完全不感知运行状态：没有实例、实例中途退出都不结束跟随，
  仅 Ctrl+C 可中断。
"""

import glob
import os
import sys
import time

# 回读末尾 N 行时按块向前扫描的块大小
_TAIL_BLOCK_SIZE = 8192


def newest_log_file(log_dir, prefix):
    """返回字典序最新的日志文件路径；没有任何日志时返回 None。

    文件名内嵌 YYYY-MM-DD，字典序即时间序，取 max 即最新。
    """
    pattern = os.path.join(log_dir, f"{prefix}.*.log")
    files = glob.glob(pattern)
    if not files:
        return None
    return max(files)


def tail_lines(path, count):
    """返回文件末尾 count 行（str 列表，不含换行符）。"""
    if count <= 0:
        return []
    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        position = f.tell()
        data = b""
        # 回读到凑够 count+1 个换行（保证最后 count 行完整）或文件头为止
        while position > 0 and data.count(b"\n") <= count:
            step = min(_TAIL_BLOCK_SIZE, position)
            position -= step
            f.seek(position)
            data = f.read(step) + data
    return data.decode("utf-8", errors="replace").splitlines()[-count:]


class LogFollower:
    """滚动显示日志的跟随器。

    Args:
        log_dir: 日志目录。
        prefix: 日志文件前缀（实例标识）。
        poll_interval: 轮询间隔秒数（测试可注入）。
        out: 输出流，默认标准输出。
    """

    def __init__(self, log_dir, prefix, poll_interval=0.5, out=None):
        self.log_dir = log_dir
        self.prefix = prefix
        self.poll_interval = poll_interval
        self.out = out or sys.stdout
        self._current = None
        self._offset = 0

    def follow(self, lines=100):
        """阻塞跟随日志；KeyboardInterrupt 由调用方处理。"""
        self._print_tail(lines)
        while True:
            self._step()
            time.sleep(self.poll_interval)

    # ---- 内部实现 ----------------------------------------------------

    def _print_tail(self, count):
        path = newest_log_file(self.log_dir, self.prefix)
        if path is None:
            self._emit(f"（{self.log_dir} 下暂无日志文件，等待新日志产生……）")
            return
        self._current = path
        for line in tail_lines(path, count):
            self._emit(line)
        try:
            self._offset = os.path.getsize(path)
        except OSError:
            self._offset = 0

    def _step(self):
        """单次轮询：切换到更新的日志文件并输出新增内容。"""
        newest = newest_log_file(self.log_dir, self.prefix)
        if newest is None:
            # 日志文件被清理殆尽等极端情况：回到等待新文件的状态
            self._current = None
            self._offset = 0
            return

        if self._current is None or newest > self._current:
            if self._current is not None:
                self._emit(f"---- 日志已切换: {os.path.basename(newest)} ----")
            self._current = newest
            self._offset = 0

        try:
            size = os.path.getsize(self._current)
        except OSError:
            self._current = None
            self._offset = 0
            return

        if size < self._offset:
            self._offset = 0  # 文件被截断/重建，从头读
        if size > self._offset:
            self._read_and_emit()

    def _read_and_emit(self):
        """从上次偏移读到文件尾，只输出完整行（半行留待下次轮询）。"""
        try:
            with open(self._current, "rb") as f:
                f.seek(self._offset)
                data = f.read()
        except OSError:
            return
        cut = data.rfind(b"\n")
        if cut < 0:
            return
        # splitlines 兼容 Windows 日志的 \r\n 行尾（logging 文本模式写入）
        text = data[:cut].decode("utf-8", errors="replace")
        for line in text.splitlines():
            self._emit(line)
        self._offset += cut + 1

    def _emit(self, text):
        print(text, file=self.out, flush=True)
