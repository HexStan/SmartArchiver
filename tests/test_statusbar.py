"""底部状态栏测试：滚动区域设置、反色绘制、缩放全屏重绘与环境降级。

状态栏的对外契约是"写入输出流的转义序列"，不存在可从外部观察的入口
（LogFollower.follow 是无限循环），只能白盒直测（见 tests/README.md
原则二）；终端尺寸查询、Windows VT 开关与重绘内容源属外部世界，
注入替身（原则三）。
"""

import io
import os

from src.output.statusbar import StatusBar

RUNNING = (True, 1234)
STOPPED = (False, None)


class _FakeTerminal(io.StringIO):
    def isatty(self):
        return True


def make_bar(provider, columns=80, lines=24, vt=True, content_provider=None):
    out = _FakeTerminal()
    bar = StatusBar.create(
        out,
        provider,
        vt_opener=lambda: vt,
        size_query=lambda: os.terminal_size((columns, lines)),
        content_provider=content_provider,
    )
    return bar, out


def sized_bar(provider, content_provider=None):
    """带可变尺寸查询的状态栏，返回 (bar, out, sizes)；改 sizes[0] 模拟缩放。"""
    out = _FakeTerminal()
    sizes = [os.terminal_size((80, 24))]
    bar = StatusBar.create(
        out,
        provider,
        vt_opener=lambda: True,
        size_query=lambda: sizes[0],
        content_provider=content_provider,
    )
    return bar, out, sizes


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
# 窄终端兜底：状态栏自身不折行
# ============================================================


def test_render_truncates_left_text_when_columns_too_narrow():
    """左侧状态文本放不下时按显示宽度截断，整行不超列数、不折行。"""
    bar, out = make_bar(lambda: RUNNING, columns=20)

    bar.start()

    # 20 列 = 截断后的左侧 "● 实"（4）+ 1 个分隔空格 + 提示（15）
    assert "\x1b[7m● 实 Ctrl+C 退出跟随\x1b[K" in out.getvalue()


def test_render_truncates_hint_when_columns_extremely_narrow():
    """列数连提示都放不下时，仅输出截断后的提示。"""
    bar, out = make_bar(lambda: RUNNING, columns=10)

    bar.start()

    assert "\x1b[7mCtrl+C 退\x1b[K" in out.getvalue()


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
    assert "\x1b[2J" not in seq  # 未注入内容源时不清屏——无法重绘日志内容


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
# 缩放全屏重绘：清屏 + 日志按新尺寸贴底重新铺排
# ============================================================


def test_start_keeps_append_semantics_even_with_content_provider():
    """首次启动不清屏：cmd_start / run-once 刚打印的提示行不能被抹掉。"""
    bar, out = make_bar(lambda: RUNNING, content_provider=lambda capacity: ["l1"])

    bar.start()

    assert "\x1b[2J" not in out.getvalue()


def test_resize_clears_screen_and_refills_content_glued_to_bottom():
    """跟随中缩放：清屏清掉旧状态栏残留与空行，内容顶部补空贴底重绘。"""
    bar, out, sizes = sized_bar(lambda: RUNNING, content_provider=lambda capacity: ["l1", "l2"])
    bar.start()
    clear(out)

    sizes[0] = os.terminal_size((80, 30))
    bar.refresh()

    seq = out.getvalue()
    assert "\x1b[r\x1b[2J\x1b[H" in seq  # 复位滚动区域 + 清屏归位
    # 29 行容量 - 2 行内容 = 顶部补 27 行空行，内容紧贴状态栏上方
    assert f"\x1b[1;29r{'\r\n' * 27}l1\r\nl2" in seq
    assert "\x1b[30;1H" in seq  # 状态行仍钉在新底部
    assert seq.endswith("\x1b[29;1H")  # 光标回到新滚动区域底部


def test_resize_reports_scroll_capacity_to_content_provider():
    """内容源收到的入参 = 滚动区可用行数（行数 - 1）。"""
    capacities = []

    def provider(capacity: int) -> list[str]:
        capacities.append(capacity)
        return ["l1"]

    bar, out, sizes = sized_bar(lambda: RUNNING, content_provider=provider)
    bar.start()
    clear(out)

    sizes[0] = os.terminal_size((80, 30))
    bar.refresh()

    assert capacities == [29]  # 仅缩放时调用；start 不清屏故不取内容


def test_resize_skips_padding_when_content_exceeds_capacity():
    """内容物理行数超出容量时不补空行，超出部分由滚动区域自然滚出。"""
    lines = [f"l{i}" for i in range(1, 31)]
    bar, out, sizes = sized_bar(lambda: RUNNING, content_provider=lambda capacity: lines)
    bar.start()
    clear(out)

    sizes[0] = os.terminal_size((80, 30))  # 30 行内容 > 29 行容量
    bar.refresh()

    seq = out.getvalue()
    assert "\x1b[1;29rl1\r\nl2" in seq  # 区域设置后立即输出，无补空
    assert "l29\r\nl30" in seq  # 尾部内容完整保留（贴底由滚动兜底）


def test_resize_counts_wrapped_rows_when_padding():
    """超宽行按列数折行计物理行数：100 列文本在 80 列终端占 2 行。"""
    bar, out, sizes = sized_bar(
        lambda: RUNNING, content_provider=lambda capacity: ["a" * 100, "l2"]
    )
    bar.start()
    clear(out)

    sizes[0] = os.terminal_size((80, 30))
    bar.refresh()

    # 29 行容量 - 3 行内容（长行折 2 行 + l2 一行）= 顶部补 26 行空行
    assert f"\x1b[1;29r{'\r\n' * 26}" + "a" * 100 + "\r\nl2" in out.getvalue()


def test_resize_counts_wide_chars_as_double_width():
    """全角字符计宽 2：60 个全角字符 = 120 列，在 80 列终端折 2 行。"""
    bar, out, sizes = sized_bar(
        lambda: RUNNING, content_provider=lambda capacity: ["一" * 60, "l2"]
    )
    bar.start()
    clear(out)

    sizes[0] = os.terminal_size((80, 30))
    bar.refresh()

    assert f"\x1b[1;29r{'\r\n' * 26}" + "一" * 60 + "\r\nl2" in out.getvalue()


def test_status_change_without_resize_does_not_repaint():
    """仅实例状态变化（尺寸未变）时只重画状态栏，不清屏。"""
    status = [STOPPED]
    bar, out, _sizes = sized_bar(lambda: status[0], content_provider=lambda capacity: ["l1"])
    bar.start()
    clear(out)

    status[0] = RUNNING
    bar.refresh()

    seq = out.getvalue()
    assert "● 实例运行中 (PID 1234)" in seq
    assert "\x1b[2J" not in seq


def test_resize_survives_content_provider_failure_and_disables_itself():
    """重绘内容源异常与状态源异常同样兜底：停用重绘，退出时仍恢复终端。"""

    def failing_provider(capacity: int) -> list[str]:
        raise OSError("历史不可用")

    bar, out, sizes = sized_bar(lambda: RUNNING, content_provider=failing_provider)
    bar.start()
    clear(out)

    sizes[0] = os.terminal_size((80, 30))
    bar.refresh()  # 不抛出：重绘异常被 _setup 兜底，自我停用
    bar.refresh()  # 停用后不再尝试

    assert out.getvalue() == ""

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
