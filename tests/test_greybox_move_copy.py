"""灰盒测试：move / copy 模式（StandardHandler）。

驱动方式：run_task() 从 execute() 入口执行任务，输入是临时目录树 + 任务配置，
断言优先落在输出目录树快照上（见 tests/README.md 原则一、五）。
除捕获日志外不对被测代码内部打桩；配置校验失败等错误路径也在此验证。
"""

import os

from tests.helpers import NOW, RECENT, make_tree, snapshot

MB = 1024 * 1024


def move_task(tmp_path, create_dest=True, **overrides):
    """默认合法的 move 任务，字段可被 overrides 覆盖或追加。

    dest 目录默认预创建——目标目录必须已存在是生产前置条件
    （缺失时任务以致命错误中止）；测试该错误路径时传 create_dest=False。
    """
    task = {
        "mode": "move",
        "source": str(tmp_path / "src"),
        "dest": str(tmp_path / "dest"),
        "mtime_threshold_seconds": 3600,
        "remove_empty_dirs": False,
    }
    task.update(overrides)
    if create_dest and task.get("dest"):
        os.makedirs(task["dest"], exist_ok=True)
    return task


class TestBasicTransfer:
    def test_move_transfers_old_files_preserving_structure(self, run_task, tmp_path):
        task = move_task(tmp_path)
        make_tree(task["source"], {"a.txt": b"alpha", "sub/b.log": b"beta"})

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {}
        assert snapshot(task["dest"]) == {"a.txt": 5, "sub/b.log": 4}

    def test_copy_leaves_source_intact(self, run_task, tmp_path):
        task = move_task(tmp_path, mode="copy")
        make_tree(task["source"], {"a.txt": b"alpha"})

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {"a.txt": 5}
        assert snapshot(task["dest"]) == {"a.txt": 5}

    def test_stats_count_successful_transfers(self, run_task, tmp_path):
        task = move_task(tmp_path)
        make_tree(task["source"], {"a.txt": b"x", "b.txt": b"y"})

        handler = run_task(task, now=NOW)

        assert handler.stats.success == 2
        assert handler.stats.total_bytes == 2

    def test_unicode_filename_survives_transfer(self, run_task, tmp_path):
        task = move_task(tmp_path)
        make_tree(task["source"], {"数据备份/视频 01.mp4": b"video"})

        run_task(task, now=NOW)

        assert snapshot(task["dest"]) == {"数据备份/视频 01.mp4": 5}


class TestMtimeThreshold:
    """时间阈值同时约束传输和删除；边界为严格大于（now - mtime > 阈值才处理）。"""

    def test_recent_file_stays_old_file_moves(self, run_task, tmp_path):
        task = move_task(tmp_path)  # 阈值 3600 秒
        make_tree(
            task["source"],
            {"old.txt": b"o", "new.txt": (b"n", RECENT)},
        )

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {"new.txt": 1}
        assert snapshot(task["dest"]) == {"old.txt": 1}

    def test_file_exactly_at_threshold_is_skipped(self, run_task, tmp_path):
        task = move_task(tmp_path)  # 3600 秒
        make_tree(task["source"], {"edge.txt": (b"e", NOW - 3600)})

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {"edge.txt": 1}

    def test_file_just_past_threshold_is_processed(self, run_task, tmp_path):
        task = move_task(tmp_path)
        make_tree(task["source"], {"edge.txt": (b"e", NOW - 3601)})

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {}
        assert snapshot(task["dest"]) == {"edge.txt": 1}

    def test_zero_threshold_processes_everything(self, run_task, tmp_path):
        task = move_task(tmp_path, mtime_threshold_seconds=0)
        make_tree(task["source"], {"fresh.txt": (b"f", NOW - 1)})

        run_task(task, now=NOW)

        assert snapshot(task["dest"]) == {"fresh.txt": 1}

    def test_threshold_defaults_to_zero_when_omitted(self, run_task, tmp_path):
        """缺省即无时间限制：60 秒前的文件在任何正阈值下都会被扣住，
        却照常移动，证明缺省值是 0 而非其余正数。"""
        task = move_task(tmp_path)
        del task["mtime_threshold_seconds"]
        make_tree(task["source"], {"fresh.txt": (b"f", RECENT)})

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {}
        assert snapshot(task["dest"]) == {"fresh.txt": 1}

    def test_delete_rule_also_gated_by_mtime(self, run_task, tmp_path, capture_log):
        """README 明示的设计意图：delete_rules 同受 mtime 阈值约束，
        保护正在写入的文件。"""
        task = move_task(tmp_path, delete_rules={"ge": {"*.tmp": "0"}})
        make_tree(task["source"], {"old.tmp": b"x", "new.tmp": (b"y", RECENT)})

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {"new.tmp": 1}
        assert snapshot(task["dest"]) == {}


class TestSourceQuietPeriod:
    """source_quiet_seconds：源目录静默期内整任务跳过，一个文件都不处理。"""

    def test_recent_file_skips_whole_task(self, run_task, tmp_path, capture_log):
        """静默判定取源目录的递归最新 mtime：任一文件仍是新的，整个任务跳过。"""
        task = move_task(tmp_path, source_quiet_seconds=300)
        make_tree(task["source"], {"old.txt": b"o", "fresh.txt": (b"f", RECENT)})

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {"old.txt": 1, "fresh.txt": 1}
        assert snapshot(task["dest"]) == {}
        assert capture_log.has("info", "源目录最近修改于")

    def test_quiet_source_runs_normally(self, run_task, tmp_path):
        """源目录已静置超过阈值：任务照常执行，新配置不改变既有行为。"""
        task = move_task(tmp_path, source_quiet_seconds=30)
        make_tree(task["source"], {"a.txt": b"a"})

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {}
        assert snapshot(task["dest"]) == {"a.txt": 1}

    def test_source_quiet_exactly_at_threshold_skips(self, run_task, tmp_path):
        """边界与 mtime_threshold_seconds 对齐：距今恰好等于阈值仍视为未静默足够久。"""
        task = move_task(tmp_path, source_quiet_seconds=300)
        make_tree(task["source"], {"edge.txt": (b"e", NOW - 300)})

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {"edge.txt": 1}
        assert snapshot(task["dest"]) == {}

    def test_zero_value_disables_the_check(self, run_task, tmp_path):
        """0 显式禁用静默检查：刚修改过的文件也照常参与处理
        （逐文件 mtime 阈值一并设 0 以排除其干扰）。"""
        task = move_task(tmp_path, source_quiet_seconds=0, mtime_threshold_seconds=0)
        make_tree(task["source"], {"fresh.txt": (b"f", RECENT)})

        run_task(task, now=NOW)

        assert snapshot(task["dest"]) == {"fresh.txt": 1}


class TestRulesEndToEnd:
    """规则系统对目录树的最终影响——README 文档化行为的落地验证。"""

    def test_exclude_rule_keeps_matching_files_in_place(self, run_task, tmp_path):
        task = move_task(tmp_path, exclude_rules={"ge": {"*.log": "0"}})
        make_tree(task["source"], {"a.log": b"log", "b.txt": b"txt"})

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {"a.log": 3}
        assert snapshot(task["dest"]) == {"b.txt": 3}

    def test_delete_rule_deletes_instead_of_transferring(self, run_task, tmp_path):
        task = move_task(tmp_path, delete_rules={"ge": {"*.tmp": "0"}})
        make_tree(task["source"], {"a.tmp": b"x", "b.txt": b"y"})

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {}
        assert snapshot(task["dest"]) == {"b.txt": 1}

    def test_exclude_shields_file_from_delete(self, run_task, tmp_path):
        task = move_task(
            tmp_path,
            exclude_rules={"ge": {"important.tmp": "0"}},
            delete_rules={"ge": {"*.tmp": "0"}},
        )
        make_tree(task["source"], {"important.tmp": b"keep", "other.tmp": b"del"})

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {"important.tmp": 4}
        assert snapshot(task["dest"]) == {}

    def test_include_whitelist_only_transfers_matches(self, run_task, tmp_path):
        task = move_task(tmp_path, include_rules={"ge": {"*.mp4": "0"}})
        make_tree(task["source"], {"a.mp4": b"v", "b.txt": b"t"})

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {"b.txt": 1}
        assert snapshot(task["dest"]) == {"a.mp4": 1}

    def test_include_directory_grants_access_to_children(self, run_task, tmp_path):
        """目录命中 include 后子项自动继承纳入状态。"""
        task = move_task(tmp_path, include_rules={"ge": {"docs/": "0"}})
        make_tree(task["source"], {"docs/a.txt": b"x", "other/b.txt": b"y"})

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {"other/b.txt": 1}
        assert snapshot(task["dest"]) == {"docs/a.txt": 1}

    def test_directory_exclude_skips_whole_subtree(self, run_task, tmp_path):
        task = move_task(tmp_path, exclude_rules={"ge": {"videos/": "0"}})
        make_tree(task["source"], {"videos/v.mp4": b"v", "a.txt": b"a"})

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {"videos/v.mp4": 1}
        assert snapshot(task["dest"]) == {"a.txt": 1}

    def test_directory_delete_removes_old_directory(self, run_task, tmp_path):
        task = move_task(tmp_path, delete_rules={"ge": {"cache/": "0"}})
        make_tree(task["source"], {"cache/x.tmp": b"x", "keep.txt": b"k"})

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {}
        assert snapshot(task["dest"]) == {"keep.txt": 1}
        assert not os.path.exists(os.path.join(task["source"], "cache"))

    def test_directory_delete_gated_by_content_mtime(self, run_task, tmp_path):
        """目录的 mtime 取内容物最新时间——目录里有新文件时整个目录受保护。"""
        task = move_task(tmp_path, delete_rules={"ge": {"cache/": "0"}})
        make_tree(task["source"], {"cache/new.tmp": (b"n", RECENT)})

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {"cache/new.tmp": 1}
        assert snapshot(task["dest"]) == {}


class TestConflictPolicies:
    def test_conflict_skip_leaves_both_sides_untouched(self, run_task, tmp_path):
        task = move_task(tmp_path, conflict_policy="skip")
        make_tree(task["source"], {"a.txt": b"new-content"})
        make_tree(task["dest"], {"a.txt": b"old"})

        handler = run_task(task, now=NOW)

        assert snapshot(task["source"]) == {"a.txt": 11}
        assert snapshot(task["dest"]) == {"a.txt": 3}
        assert handler.stats.conflict_skipped == 1

    def test_conflict_overwrite_replaces_dest_content(self, run_task, tmp_path):
        task = move_task(tmp_path, conflict_policy="overwrite")
        make_tree(task["source"], {"a.txt": b"new-content"})
        make_tree(task["dest"], {"a.txt": b"old"})

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {}
        with open(os.path.join(task["dest"], "a.txt"), "rb") as f:
            assert f.read() == b"new-content"

    def test_conflict_copy_creates_numbered_copy(self, run_task, tmp_path):
        task = move_task(tmp_path, conflict_policy="copy")
        make_tree(task["source"], {"a.txt": b"new-content"})
        make_tree(task["dest"], {"a.txt": b"old"})

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {}
        dest = snapshot(task["dest"])
        assert set(dest) == {"a.txt", "a-1.txt"}
        with open(os.path.join(task["dest"], "a-1.txt"), "rb") as f:
            assert f.read() == b"new-content"

    def test_conflict_copy_chains_numbers(self, run_task, tmp_path):
        task = move_task(tmp_path, conflict_policy="copy")
        make_tree(task["source"], {"a.txt": b"third"})
        make_tree(task["dest"], {"a.txt": b"first", "a-1.txt": b"second"})

        run_task(task, now=NOW)

        assert set(snapshot(task["dest"])) == {"a.txt", "a-1.txt", "a-2.txt"}


class TestEmptyDirCleanup:
    def test_move_cleans_emptied_dirs_when_enabled(self, run_task, tmp_path):
        task = move_task(tmp_path, remove_empty_dirs=True)
        make_tree(task["source"], {"sub/a.txt": b"x"})

        run_task(task, now=NOW)

        assert os.path.isdir(task["source"])
        assert not os.path.exists(os.path.join(task["source"], "sub"))

    def test_move_keeps_empty_dirs_when_disabled(self, run_task, tmp_path):
        task = move_task(tmp_path, remove_empty_dirs=False)
        make_tree(task["source"], {"sub/a.txt": b"x"})

        run_task(task, now=NOW)

        assert os.path.isdir(os.path.join(task["source"], "sub"))

    def test_move_keeps_empty_dirs_by_default(self, run_task, tmp_path):
        """缺省不清理：remove_empty_dirs 显式开启才启用。"""
        task = move_task(tmp_path)
        del task["remove_empty_dirs"]
        make_tree(task["source"], {"sub/a.txt": b"x"})

        run_task(task, now=NOW)

        assert os.path.isdir(os.path.join(task["source"], "sub"))

    def test_copy_never_cleans_source_dirs(self, run_task, tmp_path):
        """源目录未被搬空，即使开关打开也不清理。"""
        task = move_task(tmp_path, mode="copy", remove_empty_dirs=True)
        make_tree(task["source"], {"sub/a.txt": b"x"})

        run_task(task, now=NOW)

        assert os.path.isdir(os.path.join(task["source"], "sub"))


class TestErrorPaths:
    def test_no_dest_only_executes_deletes(self, run_task, tmp_path, capture_log):
        """README 常见误区 2：未配置 dest 时不传输，但 delete_rules 照常生效。"""
        task = move_task(tmp_path)
        del task["dest"]
        task["delete_rules"] = {"ge": {"*.tmp": "0"}}
        make_tree(task["source"], {"a.tmp": b"x", "b.txt": b"y"})

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {"b.txt": 1}
        assert capture_log.has("warning", "未配置目标目录")

    def test_missing_dest_dir_aborts_before_any_transfer(self, run_task, tmp_path, capture_log):
        task = move_task(tmp_path, create_dest=False, dest=str(tmp_path / "nonexistent"))
        make_tree(task["source"], {"a.txt": b"x", "sub/b.txt": b"y"})

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {"a.txt": 1, "sub/b.txt": 1}
        assert snapshot(task["dest"]) == {}
        assert capture_log.has("critical", "目标目录不存在")

    def test_missing_source_logs_error(self, run_task, tmp_path, capture_log):
        task = move_task(tmp_path)

        run_task(task, now=NOW)

        assert capture_log.has("error", "源目录不存在")

    def test_invalid_task_config_skips_everything(self, run_task, tmp_path, capture_log):
        """配置非法（负阈值）的任务被整体跳过：错误日志 + 目录树零变化。
        （mtime_threshold_seconds 缺省已合法，负值是仍被加载期拒绝的输入。）"""
        task = move_task(tmp_path)
        task["mtime_threshold_seconds"] = -5
        make_tree(task["source"], {"a.txt": b"x"})

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {"a.txt": 1}
        assert snapshot(task["dest"]) == {}
        assert capture_log.has("error", "mtime_threshold_seconds")

    def test_invalid_rule_size_skips_everything(self, run_task, tmp_path, capture_log):
        """规则容量值无法解析时任务在构造阶段被跳过：错误日志带规则键
        上下文 + 目录树零变化。"""
        task = move_task(tmp_path, delete_rules={"lt": {"*.tmp": "banana"}})
        make_tree(task["source"], {"a.txt": b"x", "b.tmp": b"x"})

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {"a.txt": 1, "b.tmp": 1}
        assert snapshot(task["dest"]) == {}
        assert capture_log.has("error", "规则 '*.tmp' 的容量值无效")

    def test_invalid_source_quiet_value_skips_task(self, run_task, tmp_path, capture_log):
        """source_quiet_seconds 不可解析时配置校验报错跳过，而不是在比较时
        抛 TypeError 中断整轮任务（数字字符串经 lax 强转，与
        http_remotes 数值字段的决策一致）。"""
        task = move_task(tmp_path, source_quiet_seconds="abc")
        make_tree(task["source"], {"a.txt": b"x"})

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {"a.txt": 1}
        assert snapshot(task["dest"]) == {}
        assert capture_log.has("error", "source_quiet_seconds")

    def test_unknown_remote_alias_skips_task(self, run_task, tmp_path, capture_log):
        """远端别名未配置时加载期报错跳过（不回退本地路径），目录树零变化。"""
        # create_dest=False：dest 是远端写法且任务会在加载期被拒绝，
        # 本地不存在也不应存在对应目录
        task = move_task(tmp_path, create_dest=False, dest="{http:ghost}?/vol")
        make_tree(task["source"], {"a.txt": b"x"})

        run_task(task, now=NOW)

        assert snapshot(task["source"]) == {"a.txt": 1}
        assert capture_log.has("error", "ghost")
        assert capture_log.has("error", "跳过该任务")
