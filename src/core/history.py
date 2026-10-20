import json
import os


class HistoryManager:
    def __init__(self, log_dir: str, filename: str = "failure_history.json") -> None:
        self.file_path = os.path.join(log_dir, filename)
        self.history: dict[str, int] = {}
        self.load()

    def load(self) -> None:
        """加载历史记录"""
        if os.path.exists(self.file_path):
            try:
                with open(self.file_path, "r", encoding="utf-8", errors="surrogateescape") as f:
                    self.history = json.load(f)
            except OSError, json.JSONDecodeError:
                self.history = {}
        else:
            self.history = {}

    def save(self) -> None:
        """保存历史记录到磁盘"""
        try:
            temp_file = self.file_path + ".tmp"
            with open(temp_file, "w", encoding="utf-8", errors="surrogateescape") as f:
                json.dump(self.history, f, indent=2, ensure_ascii=False)
            os.replace(temp_file, self.file_path)
        except OSError:
            pass

    def record_failure(self, path: str) -> int:
        """增加某文件的失败计数"""
        path = str(path)
        if path not in self.history:
            self.history[path] = 0
        self.history[path] += 1
        return self.history[path]

    def record_success(self, path: str) -> None:
        """成功处理后，清除失败记录"""
        path = str(path)
        if path in self.history:
            del self.history[path]

    def should_skip(self, path: str, max_retries: int) -> tuple[bool, int]:
        """判断是否应该跳过该文件"""
        path = str(path)
        count = self.history.get(path, 0)
        return count >= max_retries, count
