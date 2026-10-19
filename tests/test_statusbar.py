"""底部状态栏测试：滚动区域设置、反色绘制、变化重绘与环境降级。

状态栏的对外契约是"写入输出流的转义序列"，不存在可从外部观察的入口
（LogFollower.follow 是无限循环），只能白盒直测（见 tests/README.md
原则二）；终端尺寸查询与 Windows VT 开关属外部世界，注入替身（原则三）。
"""

import io
import os

from src.statusbar import StatusBar

RUNNING = (True, 1234)
STOPPED = (False, None)


class _FakeTerminal(io.StringIO):
    def isatty(self):
        return True


def make_bar(provider, columns=80, lines=24, vt=True):
    out = _FakeTerminal()
    bar = StatusBar.create(
        out,
        provider,
        vt_opener=lambda: vt,
        size_query=lambda: os.terminal_size((columns, lines)),
    )
    return bar, out


def clear(out):
    out.seek(0)
    out.truncate()
    return out


# ============================================================
# create()：环境探测与降级
# ============================================================


def test_create_returns_none_when_output_is_not_a_tty():
    out = io.StringIO()  # isatty() 为 False
    assert StatusBar.create(out, lambda: RUNNING, vt_opener=lambda: True) is None


def test_create_returns_none_when_output_stream_is_none():
    assert StatusBar.create(None, lambda: RUNNING, vt_opener=lambda: True) is None


def test_create_returns_none_when_vt_unavailable():
    out = _FakeTerminal()
    assert StatusBar.create(out, lambda: RUNNING, vt_opener=lambda: False) is None


def test_create_succeeds_on_supported_terminal():
    bar, out = make_bar(lambda: RUNNING)
    assert bar is not None
    assert out.getvalue() == ""  # 创建本身不输出，start() 才绘制


# ============================================================
# 绘制：滚动区域 + 反色状态行
# ============================================================


def test_start_pushes_existing_content_down_before_taking_over_screen():
    """启动先换行一次再设滚动区域：把既有内容（如 shell 命令行）上推，
    保证区域底行为空行，首次绘制不覆盖既有内容。"""
    bar, out = make_bar(lambda: RUNNING)

    bar.start()

    assert out.getvalue().startswith("\n")


def test_start_sets_scroll_region_and_draws_reverse_status():
    bar, out = make_bar(lambda: RUNNING)

    bar.start()

    seq = out.getvalue()
    assert "\x1b[1;23r" in seq  # 滚动区域 = 第 1..H-1 行
    assert "\x1b[24;1H" in seq  # 状态行钉在第 H 行
    assert "\x1b[7m" in seq  # 反色
    assert "● 实例运行中 (PID 1234)" in seq
    assert "Ctrl+C 退出跟随" in seq
    assert seq.endswith("\x1b[23;1H")  # 光标回到滚动区域底部行首


def test_start_draws_running_status_without_pid():
    bar, out = make_bar(lambda: (True, None))

    bar.start()

    assert "● 实例运行中" in out.getvalue()
    assert "PID" not in out.getvalue()


def test_start_draws_stopped_status():
    bar, out = make_bar(lambda: STOPPED)

    bar.start()

    assert "○ 实例未运行" in out.getvalue()


def test_start_disables_bar_on_degenerately_short_terminal():
    bar, out = make_bar(lambda: RUNNING, lines=1)

    bar.start()
    bar.refresh()
    bar.close()

    assert out.getvalue() == ""


# ============================================================
# refresh()：变化重绘与自我停用
# ============================================================


def test_refresh_skips_redraw_when_nothing_changed():
    bar, out = make_bar(lambda: RUNNING)
    bar.start()
    clear(out)

    bar.refresh()

    assert out.getvalue() == ""


def test_refresh_redraws_when_status_changes():
    status = [STOPPED]
    bar, out = make_bar(lambda: status[0])
    bar.start()
    clear(out)

    status[0] = RUNNING
    bar.refresh()

    seq = out.getvalue()
    assert "● 实例运行中 (PID 1234)" in seq
    assert "○ 实例未运行" not in seq


def test_refresh_resets_scroll_region_on_resize():
    out = _FakeTerminal()
    sizes = [os.terminal_size((80, 24))]
    bar = StatusBar.create(
        out, lambda: RUNNING, vt_opener=lambda: True, size_query=lambda: sizes[0]
    )
    bar.start()
    clear(out)

    sizes[0] = os.terminal_size((100, 30))
    bar.refresh()

    seq = out.getvalue()
    assert "\x1b[1;29r" in seq  # 新滚动区域 = 第 1..29 行
    assert "\x1b[30;1H" in seq  # 状态行移到新的第 30 行


def test_refresh_survives_provider_failure_and_disables_itself():
    calls = iter([RUNNING])

    def failing_provider():
        try:
            return next(calls)
        except StopIteration:
            raise OSError("探测失败") from None

    bar, out = make_bar(failing_provider)
    bar.start()
    clear(out)

    bar.refresh()  # 不抛出
    bar.refresh()

    assert out.getvalue() == ""  # 首次异常即停用，后续轮询不再尝试

    bar.close()
    assert "\x1b[r" in out.getvalue()  # 停用后仍恢复终端滚动区域


# ============================================================
# close()：恢复终端
# ============================================================


def test_close_restores_full_screen_region_and_clears_status_row():
    bar, out = make_bar(lambda: RUNNING)
    bar.start()
    clear(out)

    bar.close()

    seq = out.getvalue()
    assert seq.startswith("\x1b[r")  # 恢复全屏滚动区域
    assert "\x1b[2K" in seq  # 清除状态行

    bar.close()
    assert out.getvalue() == seq  # 二次关闭无输出


def test_close_does_nothing_when_never_started():
    bar, out = make_bar(lambda: RUNNING)

    bar.close()

    assert out.getvalue() == ""
