"""灰盒测试：失败历史 HistoryManager。

真实文件读写（tmp 目录），验证"多次失败的文件下次跳过"这一跨运行
契约——历史是唯一把两次独立运行连起来的状态，持久化必须可靠。

存储为记录数组，每条记录携带任务身份 (mode, source, dest) + 文件
路径 + 失败计数；计数按任务身份隔离，身份相同的任务共享历史。
"""

import json

from src.core.history import HistoryManager, TaskIdentity


def make_manager(tmp_path, name="h"):
    log_dir = tmp_path / name
    log_dir.mkdir(exist_ok=True)
    return HistoryManager(str(log_dir))


COPY = TaskIdentity("copy", "/source", "/dest")
MOVE = TaskIdentity("move", "/source-2", "/dest-2")


class TestFailureCounting:
    def test_clean_history_never_skips(self, tmp_path):
        hm = make_manager(tmp_path)
        assert hm.should_skip(COPY, "/f.txt", 3) == (False, 0)

    def test_skip_only_after_reaching_max_retries(self, tmp_path):
        hm = make_manager(tmp_path)
        hm.record_failure(COPY, "/f.txt")
        hm.record_failure(COPY, "/f.txt")
        assert hm.should_skip(COPY, "/f.txt", 3) == (False, 2)
        hm.record_failure(COPY, "/f.txt")
        assert hm.should_skip(COPY, "/f.txt", 3) == (True, 3)

    def test_success_clears_failure_count(self, tmp_path):
        hm = make_manager(tmp_path)
        for _ in range(3):
            hm.record_failure(COPY, "/f.txt")
        hm.record_success(COPY, "/f.txt")
        assert hm.should_skip(COPY, "/f.txt", 3) == (False, 0)

    def test_success_removes_empty_identity_bucket(self, tmp_path):
        """身份下的最后一条记录被成功清除后，空桶一并移除。"""
        hm = make_manager(tmp_path)
        hm.record_failure(COPY, "/f.txt")
        hm.record_success(COPY, "/f.txt")

        assert hm.history == {}

    def test_task_identities_are_independent(self, tmp_path):
        """同一路径在不同任务身份下计数互不影响：一个身份的失败不推进
        另一个身份的计数，一个身份的成功也不清除另一个身份的记录。"""
        hm = make_manager(tmp_path)
        for _ in range(3):
            hm.record_failure(COPY, "/f.txt")

        assert hm.should_skip(MOVE, "/f.txt", 3) == (False, 0)

        hm.record_failure(MOVE, "/f.txt")
        assert hm.should_skip(COPY, "/f.txt", 3) == (True, 3)
        assert hm.should_skip(MOVE, "/f.txt", 3) == (False, 1)

        hm.record_success(COPY, "/f.txt")
        assert hm.should_skip(MOVE, "/f.txt", 3) == (False, 1)

    def test_same_identity_different_paths_independent(self, tmp_path):
        hm = make_manager(tmp_path)
        for _ in range(3):
            hm.record_failure(COPY, "/a.txt")
        assert hm.should_skip(COPY, "/b.txt", 3) == (False, 0)


class TestPersistence:
    def test_failures_survive_across_instances(self, tmp_path):
        """跨运行契约：保存后的失败计数能被新实例读到。"""
        hm = make_manager(tmp_path, name="same")
        hm.record_failure(COPY, "/f.txt")
        hm.record_failure(COPY, "/f.txt")
        hm.save()

        reloaded = make_manager(tmp_path, name="same")
        assert reloaded.should_skip(COPY, "/f.txt", 2) == (True, 2)

    def test_saved_shape_is_record_list(self, tmp_path):
        """落盘形状钉死：记录数组，字段名与文档约定的结构一致。"""
        hm = make_manager(tmp_path)
        hm.record_failure(COPY, "/f.txt")
        hm.save()

        data = json.loads((tmp_path / "h" / "failure_history.json").read_text(encoding="utf-8"))
        assert data == [
            {
                "source": "/source",
                "dest": "/dest",
                "mode": "copy",
                "path": "/f.txt",
                "failure_count": 1,
            }
        ]

    def test_corrupted_history_starts_empty(self, tmp_path):
        (tmp_path / "broken").mkdir()
        (tmp_path / "broken" / "failure_history.json").write_text(
            "{ not valid json", encoding="utf-8"
        )

        hm = make_manager(tmp_path, name="broken")

        assert hm.should_skip(COPY, "/f.txt", 1) == (False, 0)

    def test_old_flat_format_starts_empty(self, tmp_path):
        """旧版扁平 path → 次数格式不被兼容：顶层不是数组即从空历史开始。"""
        (tmp_path / "legacy").mkdir()
        (tmp_path / "legacy" / "failure_history.json").write_text(
            json.dumps({"/f.txt": 2}), encoding="utf-8"
        )

        hm = make_manager(tmp_path, name="legacy")

        assert hm.should_skip(COPY, "/f.txt", 1) == (False, 0)

    def test_malformed_entries_skipped(self, tmp_path):
        """单条记录缺字段或类型不对时仅丢弃该条，其余照常生效。"""
        (tmp_path / "mixed").mkdir()
        good = {
            "source": "/source",
            "dest": "/dest",
            "mode": "copy",
            "path": "/f.txt",
            "failure_count": 2,
        }
        broken_entries = [
            "not-a-dict",
            {},
            {**good, "failure_count": "two"},
            {**good, "failure_count": True},
            {**good, "failure_count": -1},
            {key: value for key, value in good.items() if key != "dest"},
        ]
        (tmp_path / "mixed" / "failure_history.json").write_text(
            json.dumps([*broken_entries, good]), encoding="utf-8"
        )

        hm = make_manager(tmp_path, name="mixed")

        assert hm.should_skip(COPY, "/f.txt", 2) == (True, 2)

    def test_save_leaves_no_temp_file(self, tmp_path):
        hm = make_manager(tmp_path)
        hm.record_failure(COPY, "/f.txt")
        hm.save()

        assert not (tmp_path / "h" / ".failure_history.json.sa-part").exists()


class TestSaveDeterminism:
    def test_records_sorted_by_identity_and_path(self, tmp_path):
        """输出按身份与路径排序：文件内容与写入顺序无关，可 diff。"""
        hm = make_manager(tmp_path)
        hm.record_failure(MOVE, "/b.txt")
        hm.record_failure(COPY, "/b.txt")
        hm.record_failure(COPY, "/a.txt")
        hm.save()

        data = json.loads((tmp_path / "h" / "failure_history.json").read_text(encoding="utf-8"))
        assert [(r["mode"], r["source"], r["path"]) for r in data] == [
            ("copy", "/source", "/a.txt"),
            ("copy", "/source", "/b.txt"),
            ("move", "/source-2", "/b.txt"),
        ]
