"""白盒测试：调度主循环的首次执行语义（first_run_delay）。

调度循环（_run_interval / _run_cron）位于 handler.execute() 入口之外，
"启动后第几秒执行首轮任务"这一契约从灰盒入口无法观察，只能直接驱动
调度函数、以真实文件系统与捕获日志为断言对象（tests/README.md 原则二：
白盒补外部不可见的盲区）。停止指令经 ShutdownCoordinator 注入——与
ControlWatcher 注入的真实信号同源，不另设桩。
"""

import os
import threading
import time

import pytest
from pydantic import ValidationError

from src.app import runtime
from src.app.config import ConfigManager
from src.app.runtime import ScheduleConfig
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

    ctx.config 注入任务列表供 run_tasks() 读取；ConfigManager 指向
    临时配置文件（真实组件，mtime 稳定时热重载路径为空操作）。
    """
    app_context.update_config({"tasks": [task]}, {}, {})

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
    # 与生产装配一致：调度函数接收模型（生产路径在 _run_schedule 校验后传入）
    schedule = ScheduleConfig.model_validate(schedule_config)
    thread = threading.Thread(target=target, args=(manager, schedule), daemon=True)
    thread.start()
    return thread, manager, schedule_config


def stop_and_join(app_context, thread):
    """注入停止指令并等待调度线程退出（长睡眠被 wake 事件打断）。"""
    app_context.shutdown.request_stop_after_file()
    thread.join(timeout=EXECUTE_TIMEOUT)


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
