"""白盒测试：调度主循环的首次执行语义（first_run_delay）与 run-once 唤醒语义。

调度循环（_run_interval / _run_cron）位于 handler.execute() 入口之外，
"启动后第几秒执行首轮任务""收到 run-once 指令后立即执行并重算下次
运行时间"这些契约从灰盒入口无法观察，只能直接驱动调度函数、以真实
文件系统与捕获日志为断言对象（tests/README.md 原则二：白盒补外部
不可见的盲区）。停止与 run-once 指令经 LifecycleCoordinator 注入——
与 ControlWatcher 注入的真实信号同源，不另设桩。
"""

import os
import threading
import time
import uuid
from pathlib import Path

import pytest
from pydantic import ValidationError

from src.app import runtime
from src.app.config import ConfigManager, GlobalConfig, ScheduleConfig
from src.app.context import AppContext
from src.app.instance import PHASE_RUNNING, PHASE_SLEEPING, instance_paths, read_state
from src.core.task_plan import resolve_tasks
from src.validation import format_validation_error
from tests.helpers import make_tree, snapshot

# 首轮任务应在此时间内出现结果；修复后的正常路径在 1 秒内完成，
# 超时上界只为给慢机器留余量，不参与精确时序断言
EXECUTE_TIMEOUT = 10.0
POLL_INTERVAL = 0.05


def wait_until(condition, timeout=EXECUTE_TIMEOUT):
    """轮询等待条件成立；超时返回 False。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(POLL_INTERVAL)
    return condition()


def move_task(tmp_path):
    """默认 move 任务：dest 预创建（生产前置条件）。"""
    task = {
        "mode": "move",
        "source": str(tmp_path / "src"),
        "dest": str(tmp_path / "dest"),
        "mtime_threshold_seconds": 0,
        "remove_empty_dirs": False,
    }
    os.makedirs(task["dest"], exist_ok=True)
    return task


def start_schedule(app_context, tmp_path, task, schedule_config):
    """在独立线程中驱动调度函数，返回 (线程, ConfigManager, schedule_config)。

    ctx 注入加载期解析完成的任务编排信息供 run_tasks() 读取；
    ConfigManager 指向临时配置文件（真实组件，mtime 稳定时热重载路径
    为空操作）。
    """
    ctx = AppContext.get()
    specs = resolve_tasks([task], logger=ctx.logger, http_aliases=(), ssh_aliases=())
    ctx.update_config(GlobalConfig(), specs, {}, {})

    config_path = tmp_path / "config.toml"
    source = task["source"].replace("\\", "/")
    dest = task["dest"].replace("\\", "/")
    mode = schedule_config["mode"]
    config_path.write_text(
        f'[schedule]\nmode = "{mode}"\n\n[[tasks]]\n'
        f'source = "{source}"\ndest = "{dest}"\nmode = "move"\n',
        encoding="utf-8",
    )
    manager = ConfigManager(str(config_path), app_context.logger)

    target = runtime._run_interval if mode == "interval" else runtime._run_cron
    # 与生产装配一致：调度函数接收模型（[schedule] 已在加载期经
    # GlobalConfig.schedule 校验，生产路径直接透传）
    schedule = ScheduleConfig.model_validate(schedule_config)
    thread = threading.Thread(target=target, args=(manager, schedule), daemon=True)
    thread.start()
    return thread, manager, schedule_config


def stop_and_join(app_context, thread):
    """注入停止指令并等待调度线程退出（长睡眠被 wake 事件打断）。"""
    app_context.lifecycle.request_stop_after_file()
    thread.join(timeout=EXECUTE_TIMEOUT)


@pytest.fixture
def unique_instance(app_context):
    """一次性实例标识并注入 AppContext：状态文件写入系统临时目录，测试后清理。"""
    name = f"smartarchiver-test-{uuid.uuid4().hex}"
    app_context.instance_name = name
    yield name
    paths = instance_paths(name)
    for path in (paths.lock, paths.pid, paths.ctl, paths.state):
        Path(path).unlink(missing_ok=True)


def _state_phase_is(name, phase):
    """状态文件是否处于指定相位（文件缺失视为不匹配）。"""
    state = read_state(name)
    return state is not None and state["phase"] == phase


class TestFirstRunDelay:
    def test_interval_first_run_delay_zero_executes_immediately(self, app_context, tmp_path):
        task = move_task(tmp_path)
        make_tree(task["source"], {"a.txt": b"alpha"})
        schedule = {"mode": "interval", "interval_seconds": 3600, "first_run_delay": 0}
        thread, _, _ = start_schedule(app_context, tmp_path, task, schedule)

        try:
            assert wait_until(lambda: snapshot(task["dest"]) == {"a.txt": 5}), (
                "first_run_delay=0 时首轮任务未立即执行"
            )
        finally:
            stop_and_join(app_context, thread)
        assert not thread.is_alive()
        assert snapshot(task["source"]) == {}

    def test_interval_first_run_delay_positive_executes_after_delay(self, app_context, tmp_path):
        task = move_task(tmp_path)
        make_tree(task["source"], {"a.txt": b"alpha"})
        schedule = {"mode": "interval", "interval_seconds": 3600, "first_run_delay": 2}
        thread, _, _ = start_schedule(app_context, tmp_path, task, schedule)

        try:
            assert wait_until(lambda: snapshot(task["dest"]) == {"a.txt": 5}), (
                "first_run_delay=2 时首轮任务未在延迟后执行"
            )
        finally:
            stop_and_join(app_context, thread)
        assert not thread.is_alive()
        assert snapshot(task["source"]) == {}

    def test_cron_first_run_delay_zero_executes_immediately(self, app_context, tmp_path):
        task = move_task(tmp_path)
        make_tree(task["source"], {"a.txt": b"alpha"})
        schedule = {"mode": "cron", "cron_expr": "* * * * *", "first_run_delay": 0}
        thread, _, _ = start_schedule(app_context, tmp_path, task, schedule)

        try:
            assert wait_until(lambda: app_context.logger.has("info", "脚本开始执行……")), (
                "first_run_delay=0 时首轮任务未立即执行"
            )
            # "脚本开始执行……"打在任务执行之前：若此刻立即停止，调度线程
            # 会有意跳过下轮预告直接退出（见 _run_cron 的循环前置检查）。
            # 因此等预告出现后再停止，末尾的顺序断言才不受线程调度快慢影响
            assert wait_until(lambda: app_context.logger.has("info", "下一次执行时间")), (
                "首轮执行后未预告下一次执行时间"
            )
        finally:
            stop_and_join(app_context, thread)
        assert not thread.is_alive()

        messages = [m for lv, m in app_context.logger.messages if lv == "info"]
        first_execution = messages.index("脚本开始执行……")
        next_tick = next(i for i, m in enumerate(messages) if m.startswith("下一次执行时间"))
        # 首轮执行发生在"下一次执行时间"提示之前，而非等到下个调度点
        assert first_execution < next_tick

    def test_interval_without_first_run_delay_waits_for_next_tick(self, app_context, tmp_path):
        task = move_task(tmp_path)
        make_tree(task["source"], {"a.txt": b"alpha"})
        schedule = {"mode": "interval", "interval_seconds": 3600}
        thread, _, _ = start_schedule(app_context, tmp_path, task, schedule)

        # 默认 -1：等待至下个调度点，不立即执行
        time.sleep(1.5)
        assert snapshot(task["dest"]) == {}
        assert not app_context.logger.has("info", "脚本开始执行……")
        stop_and_join(app_context, thread)
        assert not thread.is_alive()
        assert snapshot(task["dest"]) == {}


class TestRunOnce:
    """run-once 指令的调度语义：休眠期受理、立即执行、下次运行时间重算。"""

    def test_request_run_now_only_accepted_while_sleeping(self, app_context):
        """受理门槛：仅 sleeping 相位受理，starting / running 一律拒绝。"""
        lifecycle = app_context.lifecycle

        assert not lifecycle.request_run_now(time.time())  # 默认 starting
        lifecycle.set_phase(PHASE_RUNNING)
        assert not lifecycle.request_run_now(time.time())
        lifecycle.set_phase(PHASE_SLEEPING)
        assert lifecycle.request_run_now(time.time())

    def test_wait_for_wake_consumes_wake_event(self, app_context):
        """唤醒事件随 wait_for_wake 返回消费，不会让后续睡眠全部零时长。"""
        lifecycle = app_context.lifecycle
        lifecycle.set_phase(PHASE_SLEEPING)
        assert lifecycle.request_run_now(time.time())

        assert lifecycle.wait_for_wake(0.1) is True
        assert lifecycle.wait_for_wake(0.05) is False  # 事件已消费

    def test_run_once_interrupts_interval_sleep_and_recomputes_next_run(
        self, app_context, tmp_path, unique_instance
    ):
        task = move_task(tmp_path)
        make_tree(task["source"], {"a.txt": b"alpha"})
        schedule = {"mode": "interval", "interval_seconds": 3600}
        thread, _, _ = start_schedule(app_context, tmp_path, task, schedule)

        try:
            # 进入休眠期后注入 run-once，任务应立即执行而不等 3600 秒
            assert wait_until(lambda: _state_phase_is(unique_instance, PHASE_SLEEPING)), (
                "调度线程未进入休眠期（状态文件无 sleeping 相位）"
            )
            sent_ts = time.time()
            assert app_context.lifecycle.request_run_now(sent_ts)
            assert wait_until(lambda: snapshot(task["dest"]) == {"a.txt": 5}), (
                "run-once 未立即触发执行"
            )
            # 下次运行时间按执行后的当前时刻重算（约 now+3600），状态文件同步投影
            assert wait_until(lambda: _state_phase_is(unique_instance, PHASE_SLEEPING))
            state = read_state(unique_instance)
            assert state["next_run"] > time.time() + 3500
            # 受理回执持久化：即使执行轮次快于轮询间隔，发送方也能确认受理
            assert state["last_run_once"] == sent_ts
        finally:
            stop_and_join(app_context, thread)
        assert not thread.is_alive()
        assert snapshot(task["source"]) == {}

    def test_run_once_interrupts_cron_sleep(self, app_context, tmp_path, unique_instance):
        task = move_task(tmp_path)
        make_tree(task["source"], {"a.txt": b"alpha"})
        # 每月 1 日零点：休眠期足够长，执行与否完全取决于 run-once 唤醒
        schedule = {"mode": "cron", "cron_expr": "0 0 1 * *"}
        thread, _, _ = start_schedule(app_context, tmp_path, task, schedule)

        try:
            assert wait_until(lambda: _state_phase_is(unique_instance, PHASE_SLEEPING))
            assert app_context.lifecycle.request_run_now(time.time())
            assert wait_until(lambda: snapshot(task["dest"]) == {"a.txt": 5}), (
                "run-once 未立即触发执行"
            )
        finally:
            stop_and_join(app_context, thread)
        assert not thread.is_alive()
        assert snapshot(task["source"]) == {}

    def test_run_once_interrupts_first_run_delay(self, app_context, tmp_path, unique_instance):
        """first_run_delay 等待期视为休眠期：run-once 可提前触发首轮。"""
        task = move_task(tmp_path)
        make_tree(task["source"], {"a.txt": b"alpha"})
        schedule = {"mode": "interval", "interval_seconds": 3600, "first_run_delay": 30}
        thread, _, _ = start_schedule(app_context, tmp_path, task, schedule)

        try:
            assert wait_until(lambda: _state_phase_is(unique_instance, PHASE_SLEEPING))
            assert app_context.lifecycle.request_run_now(time.time())
            assert wait_until(lambda: snapshot(task["dest"]) == {"a.txt": 5}), (
                "run-once 未提前触发首轮任务"
            )
        finally:
            stop_and_join(app_context, thread)
        assert not thread.is_alive()
        assert snapshot(task["source"]) == {}


class TestScheduleConfig:
    """[schedule] 块的校验契约：合法形态 + 既有中文错误消息逐字保留。"""

    def test_absent_block_defaults_to_one_shot(self):
        schedule = ScheduleConfig.model_validate({})

        assert schedule.mode is None
        assert schedule.first_run_delay == -1

    def test_valid_cron(self):
        schedule = ScheduleConfig.model_validate(
            {"mode": "cron", "cron_expr": "0 * * * *", "first_run_delay": 0}
        )

        assert schedule.mode == "cron"
        assert schedule.cron_expr == "0 * * * *"

    def test_invalid_mode_keeps_legacy_message(self):
        with pytest.raises(ValidationError, match="不支持的定时模式: weekly"):
            ScheduleConfig.model_validate({"mode": "weekly"})

    def test_cron_requires_expr_keeps_legacy_message(self):
        with pytest.raises(ValidationError, match="cron 模式需要配置 cron_expr"):
            ScheduleConfig.model_validate({"mode": "cron"})

    def test_interval_requires_seconds_keeps_legacy_message(self):
        with pytest.raises(ValidationError, match="interval 模式需要配置有效的 interval_seconds"):
            ScheduleConfig.model_validate({"mode": "interval"})

    def test_interval_rejects_non_positive_seconds(self):
        with pytest.raises(ValidationError, match="interval 模式需要配置有效的 interval_seconds"):
            ScheduleConfig.model_validate({"mode": "interval", "interval_seconds": 0})

    def test_interval_seconds_type_error_translated_to_chinese(self):
        """数值类型错误经 format_validation_error 产出中文摘要，而非 pydantic 原文。"""
        with pytest.raises(ValidationError) as exc_info:
            ScheduleConfig.model_validate({"mode": "interval", "interval_seconds": "abc"})

        assert "数值格式无效" in format_validation_error(exc_info.value)
