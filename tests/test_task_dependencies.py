"""任务依赖：id 规范化与 depends_on 校验（白盒）+ run_tasks 编排（灰盒）。

为什么有白盒部分：解析期校验的错误分类（id 非法 / 重复 / 未知引用 /
自引用 / 前向引用）只能从日志与 TaskSpec 归一化结果区分，从任务是否
执行的终态无法观察是哪条规则命中（tests/README.md 原则二）。

为什么驱动 run_tasks 而非 run_task：依赖是"任务间"的编排行为，单个
handler 的 execute() 入口观察不到；run_tasks 同步执行（无停止指令时
不睡眠），可直接在测试线程驱动，任务结果断言落在目录树快照上。
"""

import os
import time

import pytest

from src.app import runtime
from src.core.dependencies import prepare_run_plan
from tests.helpers import OLD, make_tree, snapshot

# ============================================================
# 白盒：prepare_run_plan 解析期校验
# ============================================================


class TestIdNormalization:
    def test_missing_id_yields_none_without_warning(self, capture_log):
        specs = prepare_run_plan([{"mode": "move"}], capture_log)
        assert specs[0] == (None, (), False)
        assert capture_log.messages == []

    def test_valid_id_registered(self, capture_log):
        specs = prepare_run_plan([{"mode": "move", "id": "stage-1_A"}], capture_log)
        assert specs[0] == ("stage-1_A", (), False)
        assert capture_log.messages == []

    def test_invalid_chars_warn_and_drop_id(self, capture_log):
        specs = prepare_run_plan([{"mode": "move", "id": "a.b"}], capture_log)
        assert specs[0] == (None, (), False)
        assert capture_log.has("warning", "含非法字符")

    def test_non_string_id_warns_and_drops_id(self, capture_log):
        specs = prepare_run_plan([{"mode": "move", "id": 42}], capture_log)
        assert specs[0] == (None, (), False)
        assert capture_log.has("warning", "id 配置值非法")

    def test_duplicate_id_errors_and_blocks_later_task(self, capture_log):
        tasks = [{"id": "dup"}, {"id": "dup"}]
        specs = prepare_run_plan(tasks, capture_log)
        assert specs[0] == ("dup", (), False)
        assert specs[1].blocked
        assert capture_log.has("error", "与任务 #1 重复")

    def test_invalid_id_never_collides_with_valid_id(self, capture_log):
        # 格式校验先于唯一性：非法 id 不注册，不触发重复判定
        specs = prepare_run_plan([{"id": "a.b"}, {"id": "a.b"}], capture_log)
        assert all(not spec.blocked for spec in specs)
        assert not capture_log.has("error", "重复")
        assert (
            capture_log.messages.count(
                (
                    "warning",
                    "任务 #1 的 id 'a.b' 含非法字符（仅允许 a-z、A-Z、0-9、-、_），视为无 id。",
                )
            )
            == 1
        )


class TestDependsOnNormalization:
    def test_single_string_normalized_to_one_element(self, capture_log):
        specs = prepare_run_plan([{"id": "a"}, {"depends_on": "a"}], capture_log)
        assert specs[1] == (None, ("a",), False)
        assert capture_log.messages == []

    def test_list_normalized(self, capture_log):
        specs = prepare_run_plan(
            [{"id": "a"}, {"id": "b"}, {"depends_on": ["a", "b"]}], capture_log
        )
        assert specs[2] == (None, ("a", "b"), False)
        assert capture_log.messages == []

    def test_empty_list_no_dependencies(self, capture_log):
        specs = prepare_run_plan([{"depends_on": []}], capture_log)
        assert specs[0] == (None, (), False)

    def test_non_string_type_errors_and_blocks(self, capture_log):
        specs = prepare_run_plan([{"depends_on": 3}], capture_log)
        assert specs[0].blocked
        assert capture_log.has("error", "depends_on 配置值非法")

    def test_non_string_element_errors_and_blocks(self, capture_log):
        specs = prepare_run_plan([{"depends_on": ["a", 3]}], capture_log)
        assert specs[0].blocked
        assert capture_log.has("error", "depends_on 配置值非法")


class TestReferenceValidation:
    def test_backward_reference_kept(self, capture_log):
        tasks = [{"id": "a"}, {"id": "b", "depends_on": ["a"]}]
        specs = prepare_run_plan(tasks, capture_log)
        assert specs[1] == ("b", ("a",), False)
        assert capture_log.messages == []

    def test_unknown_reference_errors_and_blocks(self, capture_log):
        specs = prepare_run_plan([{"id": "a", "depends_on": ["ghost"]}], capture_log)
        assert specs[0].blocked
        assert capture_log.has("error", "依赖的 id 'ghost' 不存在")

    def test_self_reference_errors_and_blocks(self, capture_log):
        specs = prepare_run_plan([{"id": "a", "depends_on": ["a"]}], capture_log)
        assert specs[0].blocked
        assert capture_log.has("error", "依赖了自身")

    def test_forward_reference_errors_and_blocks(self, capture_log):
        tasks = [{"id": "a", "depends_on": ["late"]}, {"id": "late"}]
        specs = prepare_run_plan(tasks, capture_log)
        assert specs[0].blocked
        assert specs[1] == ("late", (), False)
        assert capture_log.has("error", "位于其后方的任务 'late'")

    def test_duplicate_id_task_still_blocked_when_also_referenced(self, capture_log):
        # 重复 id 未注册，依赖解析到首个出现者；重复任务本身仍被跳过
        tasks = [{"id": "dup"}, {"id": "dup"}, {"depends_on": ["dup"]}]
        specs = prepare_run_plan(tasks, capture_log)
        assert specs[1].blocked
        assert specs[2] == (None, ("dup",), False)

    def test_non_dict_task_blocked(self, capture_log):
        specs = prepare_run_plan(["oops", {"id": "a"}], capture_log)
        assert specs[0].blocked
        assert specs[1] == ("a", (), False)


# ============================================================
# 灰盒：run_tasks 编排（目录树快照 + 日志断言）
# ============================================================


def _move_task(src, dest, **extra):
    task = {
        "mode": "move",
        "source": str(src),
        "dest": str(dest),
        "mtime_threshold_seconds": 0,
        "remove_empty_dirs": False,
    }
    task.update(extra)
    os.makedirs(dest, exist_ok=True)
    return task


@pytest.fixture
def run_round(app_context):
    """驱动一轮 run_tasks：注入任务列表并同步执行（无停止指令）。"""

    def _run(tasks):
        app_context.update_config({"tasks": tasks}, {}, {})
        runtime.run_tasks()
        return app_context

    return _run


class TestRunTasksOrchestration:
    def test_completed_dependency_runs_dependent(self, tmp_path, run_round):
        first = _move_task(tmp_path / "src-a", tmp_path / "dest-a", id="a")
        second = _move_task(tmp_path / "src-b", tmp_path / "dest-b", id="b", depends_on=["a"])
        make_tree(first["source"], {"a.txt": b"alpha"})
        make_tree(second["source"], {"b.txt": b"beta"})

        run_round([first, second])

        assert snapshot(first["dest"]) == {"a.txt": 5}
        assert snapshot(second["dest"]) == {"b.txt": 4}

    def test_failed_dependency_skips_dependent(self, tmp_path, run_round, capture_log):
        # 前置任务源目录不存在 → FAILED
        first = _move_task(tmp_path / "missing-src", tmp_path / "dest-a", id="a")
        second = _move_task(tmp_path / "src-b", tmp_path / "dest-b", id="b", depends_on=["a"])
        make_tree(second["source"], {"b.txt": b"beta"})

        run_round([first, second])

        assert capture_log.has("error", "前置任务 a 未完成")
        assert snapshot(second["source"]) == {"b.txt": 4}
        assert snapshot(second["dest"]) == {}

    def test_skipped_dependency_cascades(self, tmp_path, run_round, capture_log):
        # 前置任务缺 mode → 装配失败 SKIPPED；依赖链逐级跳过
        first = _move_task(tmp_path / "src-a", tmp_path / "dest-a", id="a")
        del first["mode"]
        second = _move_task(tmp_path / "src-b", tmp_path / "dest-b", id="b", depends_on=["a"])
        third = _move_task(tmp_path / "src-c", tmp_path / "dest-c", id="c", depends_on=["b"])
        for task, name in ((second, "b.txt"), (third, "c.txt")):
            make_tree(task["source"], {name: name.encode()})

        run_round([first, second, third])

        assert capture_log.has("error", "前置任务 a 未完成")
        assert capture_log.has("error", "前置任务 b 未完成")
        assert snapshot(second["source"]) == {"b.txt": 5}
        assert snapshot(third["source"]) == {"c.txt": 5}

    def test_quiet_period_skip_still_satisfies_dependency(self, tmp_path, run_round, capture_log):
        # 静默期跳过 = 本轮无事可做，记 COMPLETED，依赖方照常执行
        now = time.time()
        first = _move_task(tmp_path / "src-a", tmp_path / "dest-a", id="a")
        first["source_quiet_seconds"] = 300
        second = _move_task(tmp_path / "src-b", tmp_path / "dest-b", id="b", depends_on=["a"])
        make_tree(first["source"], {"a.txt": b"alpha"}, default_mtime=now)
        make_tree(second["source"], {"b.txt": b"beta"}, default_mtime=OLD)

        run_round([first, second])

        assert capture_log.has("info", "源目录最近修改于"), "前置任务应因静默期被跳过"
        assert snapshot(second["dest"]) == {"b.txt": 4}

    def test_dependency_on_later_task_blocks_self_only(self, tmp_path, run_round, capture_log):
        first = _move_task(tmp_path / "src-a", tmp_path / "dest-a", id="a", depends_on=["late"])
        second = _move_task(tmp_path / "src-b", tmp_path / "dest-b", id="late")
        make_tree(first["source"], {"a.txt": b"alpha"})
        make_tree(second["source"], {"b.txt": b"beta"})

        run_round([first, second])

        assert capture_log.has("error", "位于其后方的任务 'late'")
        assert snapshot(first["source"]) == {"a.txt": 5}
        assert snapshot(second["dest"]) == {"b.txt": 4}

    def test_duplicate_id_skipped_and_dependency_resolves_to_first(
        self, tmp_path, run_round, capture_log
    ):
        first = _move_task(tmp_path / "src-a", tmp_path / "dest-a", id="dup")
        duplicate = _move_task(tmp_path / "src-dup", tmp_path / "dest-dup", id="dup")
        third = _move_task(tmp_path / "src-c", tmp_path / "dest-c", depends_on=["dup"])
        make_tree(first["source"], {"a.txt": b"alpha"})
        make_tree(duplicate["source"], {"dup.txt": b"dup"})
        make_tree(third["source"], {"c.txt": b"charlie"})

        run_round([first, duplicate, third])

        assert capture_log.has("error", "与任务 #1 重复")
        assert snapshot(duplicate["source"]) == {"dup.txt": 3}, "重复 id 的任务应被跳过"
        assert snapshot(first["dest"]) == {"a.txt": 5}
        assert snapshot(third["dest"]) == {"c.txt": 7}, "依赖应解析到首个出现者"

    def test_invalid_id_task_runs_but_cannot_be_depended(self, tmp_path, run_round, capture_log):
        first = _move_task(tmp_path / "src-a", tmp_path / "dest-a", id="a!")
        second = _move_task(tmp_path / "src-b", tmp_path / "dest-b", depends_on=["a!"])
        make_tree(first["source"], {"a.txt": b"alpha"})
        make_tree(second["source"], {"b.txt": b"beta"})

        run_round([first, second])

        assert capture_log.has("warning", "含非法字符")
        assert capture_log.has("error", "依赖的 id 'a!' 不存在")
        assert snapshot(first["dest"]) == {"a.txt": 5}, "非法 id 的任务照常执行"
        assert snapshot(second["source"]) == {"b.txt": 4}

    def test_independent_task_unaffected_by_unmet_dependency(
        self, tmp_path, run_round, capture_log
    ):
        first = _move_task(tmp_path / "missing-src", tmp_path / "dest-a", id="a")
        second = _move_task(tmp_path / "src-b", tmp_path / "dest-b", id="b", depends_on=["a"])
        third = _move_task(tmp_path / "src-c", tmp_path / "dest-c")
        make_tree(second["source"], {"b.txt": b"beta"})
        make_tree(third["source"], {"c.txt": b"charlie"})

        run_round([first, second, third])

        assert capture_log.has("error", "前置任务 a 未完成")
        assert snapshot(third["dest"]) == {"c.txt": 7}, "无依赖任务不受影响"
