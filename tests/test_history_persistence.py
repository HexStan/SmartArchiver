"""灰盒测试：失败历史 HistoryManager。

真实文件读写（tmp 目录），验证"多次失败的文件下次跳过"这一跨运行
契约——历史是唯一把两次独立运行连起来的状态，持久化必须可靠。
"""

import json

from src.history import HistoryManager


def make_manager(tmp_path, name="h"):
    log_dir = tmp_path / name
    log_dir.mkdir(exist_ok=True)
    return HistoryManager(str(log_dir))


class TestFailureCounting:
    def test_clean_history_never_skips(self, tmp_path):
        hm = make_manager(tmp_path)
        assert hm.should_skip("/f.txt", 3) == (False, 0)

    def test_skip_only_after_reaching_max_retries(self, tmp_path):
        hm = make_manager(tmp_path)
        hm.record_failure("/f.txt")
        hm.record_failure("/f.txt")
        assert hm.should_skip("/f.txt", 3) == (False, 2)
        hm.record_failure("/f.txt")
        assert hm.should_skip("/f.txt", 3) == (True, 3)

    def test_success_clears_failure_count(self, tmp_path):
        hm = make_manager(tmp_path)
        for _ in range(3):
            hm.record_failure("/f.txt")
        hm.record_success("/f.txt")
        assert hm.should_skip("/f.txt", 3) == (False, 0)

    def test_paths_are_independent(self, tmp_path):
        hm = make_manager(tmp_path)
        for _ in range(3):
            hm.record_failure("/a.txt")
        assert hm.should_skip("/b.txt", 3) == (False, 0)


class TestPersistence:
    def test_failures_survive_across_instances(self, tmp_path):
        """跨运行契约：保存后的失败计数能被新实例读到。"""
        hm = make_manager(tmp_path, name="same")
        hm.record_failure("/f.txt")
        hm.record_failure("/f.txt")
        hm.save()

        reloaded = make_manager(tmp_path, name="same")
        assert reloaded.should_skip("/f.txt", 2) == (True, 2)

    def test_corrupted_history_starts_empty(self, tmp_path):
        (tmp_path / "broken").mkdir()
        (tmp_path / "broken" / "failure_history.json").write_text(
            "{ not valid json", encoding="utf-8"
        )

        hm = make_manager(tmp_path, name="broken")

        assert hm.should_skip("/f.txt", 1) == (False, 0)

    def test_save_leaves_no_temp_file(self, tmp_path):
        hm = make_manager(tmp_path)
        hm.record_failure("/f.txt")
        hm.save()

        assert not (tmp_path / "h" / "failure_history.json.tmp").exists()
        # 保存的是合法 JSON
        data = json.loads(
            (tmp_path / "h" / "failure_history.json").read_text(encoding="utf-8")
        )
        assert data == {"/f.txt": 1}
