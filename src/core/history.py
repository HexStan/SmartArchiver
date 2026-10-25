"""失败历史：跨运行持久化"某任务对某文件的连续失败次数"。

存储为记录数组（failure_history.json），每条记录携带任务身份
（mode + source + dest）、文件路径与失败计数。计数按任务身份隔离：
不同任务（不同 mode / source / dest）对同一文件的失败互不影响，
成功也只清除自己身份下的记录。

任务身份取配置原始字段（dest 为含协议前缀的原始串），不依赖可选的
任务 id——对配置重排序与 id 增删均稳定；mode + source + dest 完全
相同的任务本质是重复任务，共享同一份历史属合理语义。

旧版"扁平 path → 次数"格式不被兼容：加载时结构不符即从空历史开始。
"""

import json
import os
from typing import NamedTuple

from src.fs import atomic_writer


class TaskIdentity(NamedTuple):
    """任务身份三元组：失败历史的最小隔离单位。"""

    mode: str
    source: str
    dest: str


class HistoryManager:
    def __init__(self, log_dir: str, filename: str = "failure_history.json") -> None:
        self.file_path = os.path.join(log_dir, filename)
        # 身份 → (文件路径 → 连续失败次数)
        self.history: dict[TaskIdentity, dict[str, int]] = {}
        self.load()

    def load(self) -> None:
        """加载历史记录；文件缺失、解析失败或结构不符时从空历史开始。

        顶层必须是记录数组；单条记录缺字段或类型不对时仅丢弃该条，
        不影响其余记录。
        """
        self.history = {}
        if not os.path.exists(self.file_path):
            return
        try:
            with open(self.file_path, "r", encoding="utf-8", errors="surrogateescape") as f:
                data = json.load(f)
        except OSError, json.JSONDecodeError:
            return
        if not isinstance(data, list):
            return
        for entry in data:
            record = self._parse_entry(entry)
            if record is not None:
                identity, path, count = record
                self.history.setdefault(identity, {})[path] = count

    @staticmethod
    def _parse_entry(entry: object) -> tuple[TaskIdentity, str, int] | None:
        """单条记录解析：身份字段与路径须为字符串、计数须为非负整数
        （布尔显式排除），否则该条无效。"""
        if not isinstance(entry, dict):
            return None
        mode = entry.get("mode")
        source = entry.get("source")
        dest = entry.get("dest")
        path = entry.get("path")
        count = entry.get("failure_count")
        if not (
            isinstance(mode, str)
            and isinstance(source, str)
            and isinstance(dest, str)
            and isinstance(path, str)
        ):
            return None
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            return None
        return TaskIdentity(mode, source, dest), path, count

    def save(self) -> None:
        """保存历史记录到磁盘（原子写入：同目录临时文件 + fsync + os.replace）。

        记录按身份与路径排序输出——文件内容与写入顺序无关，可 diff。
        """
        entries: list[dict[str, str | int]] = []
        for identity in sorted(self.history):
            for path, count in sorted(self.history[identity].items()):
                entries.append(
                    {
                        "source": identity.source,
                        "dest": identity.dest,
                        "mode": identity.mode,
                        "path": path,
                        "failure_count": count,
                    }
                )
        try:
            payload = json.dumps(entries, indent=2, ensure_ascii=False)
            with atomic_writer(self.file_path) as f:
                f.write(payload.encode("utf-8", "surrogateescape"))
        except OSError:
            pass

    def record_failure(self, identity: TaskIdentity, path: str) -> int:
        """增加某任务身份下某文件的失败计数，返回当前计数。"""
        bucket = self.history.setdefault(identity, {})
        bucket[path] = bucket.get(path, 0) + 1
        return bucket[path]

    def record_success(self, identity: TaskIdentity, path: str) -> None:
        """某任务身份下某文件成功后，清除其失败记录。"""
        bucket = self.history.get(identity)
        if bucket is not None:
            bucket.pop(path, None)
            if not bucket:
                del self.history[identity]

    def should_skip(self, identity: TaskIdentity, path: str, max_retries: int) -> tuple[bool, int]:
        """判断某任务身份下是否应跳过该文件。"""
        count = self.history.get(identity, {}).get(path, 0)
        return count >= max_retries, count
