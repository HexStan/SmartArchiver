"""灰盒测试：原子写入行为（move / copy 经 ActionExecutor 落盘）。

驱动方式与 test_greybox_move_copy 相同：run_task() + 临时目录树，
断言真实文件系统终态——元数据保留、冲突覆盖的完整替换、无临时文件残留。
快照断言天然覆盖"无残留"：`.smartarchiver-partial` 临时文件一旦残留
就会出现在 snapshot() 中。EXDEV 回退等 OS 边界行为在
test_atomic_write.py 白盒侧覆盖。
"""

import os
from pathlib import Path

from tests.helpers import NOW, OLD, make_tree, snapshot


def transfer_task(tmp_path, create_dest=True, **overrides):
    """默认合法的 copy 任务，字段可被 overrides 覆盖或追加。"""
    task = {
        "mode": "copy",
        "source": str(tmp_path / "src"),
        "dest": str(tmp_path / "dest"),
        "mtime_threshold_minutes": 60,
        "remove_empty_dirs": False,
    }
    task.update(overrides)
    if create_dest and task.get("dest"):
        os.makedirs(task["dest"], exist_ok=True)
    return task


class TestMetadataPreservation:
    def test_copy_preserves_source_mtime(self, run_task, tmp_path):
        """mtime 保留是硬性契约：归档阈值逻辑依赖它。"""
        task = transfer_task(tmp_path)
        make_tree(task["source"], {"a.txt": (b"x", OLD)})

        run_task(task, now=NOW)

        assert os.stat(Path(task["dest"]) / "a.txt").st_mtime == OLD

    def test_move_preserves_source_mtime(self, run_task, tmp_path):
        task = transfer_task(tmp_path, mode="move")
        make_tree(task["source"], {"a.txt": (b"x", OLD)})

        run_task(task, now=NOW)

        assert os.stat(Path(task["dest"]) / "a.txt").st_mtime == OLD


class TestOverwriteAtomicity:
    def test_overwrite_replaces_dest_completely(self, run_task, tmp_path):
        """覆盖后内容、mtime、快照全部来自新文件，且无临时文件残留。"""
        task = transfer_task(tmp_path, conflict_policy="overwrite")
        make_tree(task["source"], {"a.txt": (b"new-content", OLD)})
        dest = Path(task["dest"])
        (dest / "a.txt").write_bytes(b"old-content")

        run_task(task, now=NOW)

        assert (dest / "a.txt").read_bytes() == b"new-content"
        assert os.stat(dest / "a.txt").st_mtime == OLD
        assert snapshot(task["dest"]) == {"a.txt": len(b"new-content")}

    def test_overwrite_dest_is_directory_reports_error_without_residue(
        self, run_task, tmp_path, capture_log
    ):
        """目标同名是目录时：报错、目录原样保留、无临时文件残留、源文件不动。"""
        task = transfer_task(tmp_path, conflict_policy="overwrite")
        make_tree(task["source"], {"a.txt": (b"x", OLD)})
        dest = Path(task["dest"])
        (dest / "a.txt").mkdir()

        run_task(task, now=NOW)

        assert capture_log.has("error", "a.txt")
        assert (dest / "a.txt").is_dir()
        assert snapshot(task["dest"]) == {}
        assert os.path.exists(Path(task["source"]) / "a.txt")
