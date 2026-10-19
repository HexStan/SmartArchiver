"""日志跟随器测试：尾部回读、增量跟随、跨天轮转切换与文件异常恢复。

跟随器的对外契约是"输出到输出流的内容序列"，以真实日志文件为环境、
直接步进 _tick()/_step()（不打桩、不依赖真实时钟，见 tests/README.md
原则四）；状态栏本身的转义序列渲染在 test_statusbar.py 单测。
"""

import io

from src.output.logtail import LogFollower, newest_log_file, tail_lines


def make_follower(log_dir, prefix, **kwargs):
    out = io.StringIO()
    return LogFollower(str(log_dir), prefix, poll_interval=0, out=out, **kwargs), out


class _RecordingBar:
    """记录调用时序的状态栏替身（捕获型，见 tests/README.md 原则七）。"""

    def __init__(self):
        self.events = []

    def start(self):
        self.events.append("start")

    def refresh(self):
        self.events.append("refresh")

    def close(self):
        self.events.append("close")


# ============================================================
# newest_log_file / tail_lines
# ============================================================


def test_newest_log_file_picks_latest_date(tmp_path):
    (tmp_path / "sa.2026-01-01.log").write_text("old", encoding="utf-8")
    (tmp_path / "sa.2026-01-02.log").write_text("new", encoding="utf-8")
    (tmp_path / "other.2026-01-03.log").write_text("noise", encoding="utf-8")

    assert newest_log_file(str(tmp_path), "sa") == str(tmp_path / "sa.2026-01-02.log")


def test_newest_log_file_none_when_missing(tmp_path):
    assert newest_log_file(str(tmp_path), "sa") is None


def test_tail_lines_returns_last_n(tmp_path):
    path = tmp_path / "sa.2026-01-01.log"
    path.write_text("l1\nl2\nl3\nl4\nl5\n", encoding="utf-8")

    assert tail_lines(str(path), 3) == ["l3", "l4", "l5"]


def test_tail_lines_returns_all_when_fewer_than_n(tmp_path):
    path = tmp_path / "sa.2026-01-01.log"
    path.write_text("l1\nl2\n", encoding="utf-8")

    assert tail_lines(str(path), 10) == ["l1", "l2"]


def test_tail_lines_zero_count(tmp_path):
    path = tmp_path / "sa.2026-01-01.log"
    path.write_text("l1\n", encoding="utf-8")

    assert tail_lines(str(path), 0) == []


# ============================================================
# 跟随行为
# ============================================================


def test_follow_prints_waiting_hint_when_no_log_file(tmp_path):
    follower, out = make_follower(tmp_path, "sa")

    follower._print_tail(100)

    assert "暂无日志文件" in out.getvalue()


def test_follow_starts_with_tail_then_streams_appends(tmp_path):
    log = tmp_path / "sa.2026-01-01.log"
    log.write_text("l1\nl2\nl3\n", encoding="utf-8")
    follower, out = make_follower(tmp_path, "sa")

    follower._print_tail(2)
    assert out.getvalue() == "l2\nl3\n"

    # 追加完整行 → 下一次轮询输出
    with open(log, "a", encoding="utf-8") as f:
        f.write("l4\nl5\n")
    follower._step()
    assert out.getvalue() == "l2\nl3\nl4\nl5\n"


def test_follow_holds_back_partial_line_until_completed(tmp_path):
    """正在写入的半行不输出，补全换行后才输出（行完整性契约）。"""
    log = tmp_path / "sa.2026-01-01.log"
    log.write_text("complete\n", encoding="utf-8")
    follower, out = make_follower(tmp_path, "sa")

    follower._print_tail(100)

    with open(log, "a", encoding="utf-8") as f:
        f.write("half")
    follower._step()
    assert out.getvalue() == "complete\n"  # 半行被扣住

    with open(log, "a", encoding="utf-8") as f:
        f.write("-line\n")
    follower._step()
    assert out.getvalue() == "complete\nhalf-line\n"


def test_follow_switches_to_new_log_file_on_rotation(tmp_path):
    """跨天轮转产生更新文件名时自动切换并打分隔提示。"""
    old = tmp_path / "sa.2026-01-01.log"
    old.write_text("day1\n", encoding="utf-8")
    follower, out = make_follower(tmp_path, "sa")

    follower._print_tail(100)
    assert out.getvalue() == "day1\n"

    new = tmp_path / "sa.2026-01-02.log"
    new.write_text("day2-a\n", encoding="utf-8")
    follower._step()
    assert "日志已切换" in out.getvalue()
    assert out.getvalue().endswith("day2-a\n")

    # 后续增量读自新文件
    with open(new, "a", encoding="utf-8") as f:
        f.write("day2-b\n")
    follower._step()
    assert out.getvalue().endswith("day2-b\n")


def test_follow_recovers_when_current_file_disappears(tmp_path):
    """当前日志被清理删除后回到等待状态，新文件出现时恢复跟随。"""
    log = tmp_path / "sa.2026-01-01.log"
    log.write_text("only\n", encoding="utf-8")
    follower, out = make_follower(tmp_path, "sa")

    follower._print_tail(100)
    log.unlink()
    follower._step()  # 当前文件消失，不抛异常

    fresh = tmp_path / "sa.2026-01-02.log"
    fresh.write_text("fresh\n", encoding="utf-8")
    follower._step()

    assert "日志已切换" not in out.getvalue()  # 从等待状态进入，无切换提示
    assert out.getvalue().endswith("fresh\n")


def test_follow_resets_when_file_shrinks(tmp_path):
    """文件被截断重建（size < offset）时从头读取。"""
    log = tmp_path / "sa.2026-01-01.log"
    log.write_text("first content line\n", encoding="utf-8")
    follower, out = make_follower(tmp_path, "sa")

    follower._print_tail(100)
    log.write_text("new\n", encoding="utf-8")
    follower._step()

    assert out.getvalue() == "first content line\nnew\n"


# ============================================================
# 单轮跟随与状态栏协作
# ============================================================


def test_tick_refreshes_status_bar_before_reading_log(tmp_path):
    """单轮跟随 = 先刷新状态栏，再读取日志增量。"""
    log = tmp_path / "sa.2026-01-01.log"
    log.write_text("l1\n", encoding="utf-8")
    follower, out = make_follower(tmp_path, "sa", status_provider=lambda: (True, 7))
    bar = _RecordingBar()
    follower._bar = bar
    follower._print_tail(100)

    with open(log, "a", encoding="utf-8") as f:
        f.write("l2\n")
    follower._tick()

    assert bar.events == ["refresh"]
    assert out.getvalue() == "l1\nl2\n"


def test_tick_without_bar_keeps_plain_log_output(tmp_path):
    """非终端场景（未创建状态栏）下，单轮跟随行为与纯日志输出完全一致。"""
    log = tmp_path / "sa.2026-01-01.log"
    log.write_text("l1\n", encoding="utf-8")
    follower, out = make_follower(tmp_path, "sa", status_provider=lambda: (True, 7))

    follower._print_tail(100)
    with open(log, "a", encoding="utf-8") as f:
        f.write("l2\n")
    follower._tick()

    assert out.getvalue() == "l1\nl2\n"  # 输出中没有任何转义序列痕迹
