"""灰盒测试：rotate 模式（RotateHandler）。

驱动方式：run_task() 从 execute() 入口执行任务，断言落在"哪些文件被
轮转出目录、哪些留下"的目录树快照上。轮转的核心契约：最旧优先处理、
处理到所有分组不超限立即停止、exclude 保护文件的统计值不从分组扣除。
分组统计本身的增量扣减契约见 test_rotate_groups.py（白盒）。
"""

import os

from tests.helpers import NOW, OLD, RECENT, make_tree, snapshot


def rotate_task(tmp_path, create_dest=True, **overrides):
    """默认合法的 rotate 任务。dest 配置时默认预创建（生产前置条件）。"""
    task = {
        "mode": "rotate",
        "source": str(tmp_path / "src"),
        "remove_empty_dirs": False,
    }
    task.update(overrides)
    if create_dest and task.get("dest"):
        os.makedirs(task["dest"], exist_ok=True)
    return task


class TestGlobalLimits:
    def test_size_limit_rotates_oldest_until_under(self, run_task, tmp_path):
        """超限后从最旧的文件开始移出，恰好降到限制以下即停止。"""
        task = rotate_task(tmp_path, dest=str(tmp_path / "dest"), size_limit="5000B")
        make_tree(
            task["source"],
            {
                "a.bin": (b"a" * 3000, OLD),        # 最旧
                "b.bin": (b"b" * 2000, OLD + 100),
                "c.bin": (b"c" * 1000, OLD + 200),
            },
        )

        run_task(task, now=NOW)

        # 总量 6000 > 5000：移走最旧的 a.bin 后剩 3000，立即停止
        assert snapshot(task["source"]) == {"b.bin": 2000, "c.bin": 1000}
        assert snapshot(task["dest"]) == {"a.bin": 3000}

    def test_count_limit_rotates_oldest_files(self, run_task, tmp_path):
        task = rotate_task(tmp_path, dest=str(tmp_path / "dest"), count_limit=3)
        make_tree(
            task["source"],
            {f"f{i}.txt": (b"x", OLD + i) for i in range(5)},
        )

        run_task(task, now=NOW)

        assert snapshot(task["dest"]) == {"f0.txt": 1, "f1.txt": 1}
        assert snapshot(task["source"]) == {"f2.txt": 1, "f3.txt": 1, "f4.txt": 1}

    def test_exact_limit_triggers_no_rotation(self, run_task, tmp_path, capture_log):
        task = rotate_task(tmp_path, dest=str(tmp_path / "dest"), size_limit="3000B")
        make_tree(task["source"], {"a.bin": b"a" * 3000})

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {"a.bin": 3000}
        assert snapshot(task["dest"]) == {}
        assert capture_log.has("info", "无需轮转")

    def test_no_limits_fails_validation(self, run_task, tmp_path, capture_log):
        """所有限制都未配置时任务被跳过：错误日志 + 目录树零变化。"""
        task = rotate_task(tmp_path)
        make_tree(task["source"], {"a.txt": b"x"})

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {"a.txt": 1}
        assert capture_log.has("error", "至少一项")

    def test_include_rules_rejected_in_rotate(self, run_task, tmp_path, capture_log):
        task = rotate_task(tmp_path, size_limit="1KB")
        task["include_rules"] = {"ge": {"*.log": "-1"}}
        make_tree(task["source"], {"a.log": b"x"})

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {"a.log": 1}
        assert capture_log.has("error", "include_rules")

    def test_recent_files_are_not_exempt_from_rotation(self, run_task, tmp_path):
        """轮转不设 mtime 门槛——超限时新文件同样参与（与 move/copy 不同）。"""
        task = rotate_task(tmp_path, dest=str(tmp_path / "dest"), count_limit=1)
        make_tree(
            task["source"],
            {"a.txt": (b"a", RECENT), "b.txt": (b"b", RECENT + 1)},
        )

        run_task(task, now=NOW)

        assert snapshot(task["dest"]) == {"a.txt": 1}
        assert snapshot(task["source"]) == {"b.txt": 1}


class TestRotationWithoutDest:
    def test_delete_rules_delete_in_place_of_transfer(self, run_task, tmp_path):
        """无 dest 时轮转产生的动作走 delete_rules 直接删除。"""
        task = rotate_task(
            tmp_path, count_limit=2, delete_rules={"ge": {"*": "-1"}}
        )
        make_tree(
            task["source"],
            {f"f{i}.txt": (b"x", OLD + i) for i in range(4)},
        )

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {"f2.txt": 1, "f3.txt": 1}

    def test_no_dest_and_no_delete_rules_changes_nothing(
        self, run_task, tmp_path, capture_log
    ):
        """无可处理动作（无 dest、无 delete_rules）时文件原地不动，
        分组仍超限 → 输出警告日志。"""
        task = rotate_task(tmp_path, count_limit=1)
        make_tree(task["source"], {"a.txt": b"1", "b.txt": b"2"})

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {"a.txt": 1, "b.txt": 1}
        assert capture_log.has("warning", "仍未满足限制")


class TestRulesInRotation:
    def test_exclude_protects_file_from_rotation(self, run_task, tmp_path):
        """被 exclude 的文件留在原地，其统计值不从分组扣除——
        轮转只能通过处理其他文件来消除超限。"""
        task = rotate_task(
            tmp_path,
            dest=str(tmp_path / "dest"),
            count_limit=1,
            exclude_rules={"ge": {"*.keep": "-1"}},
        )
        make_tree(
            task["source"],
            {
                "n1.txt": (b"1", OLD),
                "solo.keep": (b"2", OLD + 100),
                "n2.txt": (b"3", OLD + 200),
            },
        )

        handler = run_task(task, now=NOW)

        assert snapshot(task["source"]) == {"solo.keep": 1}
        assert snapshot(task["dest"]) == {"n1.txt": 1, "n2.txt": 1}
        assert handler.stats.kept == 1

    def test_rotate_rules_only_constrain_matching_files(self, run_task, tmp_path):
        """规则级分组只约束匹配的文件——更旧的无关文件不被殃及。"""
        task = rotate_task(
            tmp_path,
            dest=str(tmp_path / "dest"),
            rotate_rules={"size": {"*.log": "100B"}},
        )
        make_tree(
            task["source"],
            {"older.txt": (b"x", OLD), "big.log": (b"y" * 200, OLD + 100)},
        )

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {"older.txt": 1}
        assert snapshot(task["dest"]) == {"big.log": 200}

    def test_single_file_lifts_multiple_group_exceedances(self, run_task, tmp_path):
        """一个文件同时命中多个超限分组时，处理一次即同时满足所有分组。"""
        task = rotate_task(
            tmp_path,
            dest=str(tmp_path / "dest"),
            rotate_rules={"size": {"*.log": "100B", "app.*": "100B"}},
        )
        make_tree(task["source"], {"app.log": b"z" * 200})

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {}
        assert snapshot(task["dest"]) == {"app.log": 200}


class TestStructurePreservation:
    def test_rotation_preserves_relative_paths(self, run_task, tmp_path):
        task = rotate_task(
            tmp_path,
            dest=str(tmp_path / "dest"),
            rotate_rules={"count": {"*.log": 1}},
        )
        make_tree(
            task["source"],
            {"sub/a.log": (b"x", OLD), "sub/b.log": (b"y", OLD + 100)},
        )

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {"sub/b.log": 1}
        assert snapshot(task["dest"]) == {"sub/a.log": 1}

    def test_remove_empty_dirs_after_rotation(self, run_task, tmp_path):
        task = rotate_task(
            tmp_path,
            dest=str(tmp_path / "dest"),
            remove_empty_dirs=True,
            size_limit="1B",
        )
        make_tree(task["source"], {"sub/a.log": b"x" * 10})

        run_task(task, now=NOW)

        assert snapshot(task["dest"]) == {"sub/a.log": 10}
        assert not os.path.exists(os.path.join(task["source"], "sub"))

    def test_missing_source_logs_error(self, run_task, tmp_path, capture_log):
        task = rotate_task(tmp_path, count_limit=1)

        run_task(task, now=NOW)

        assert capture_log.has("error", "源目录不存在")
