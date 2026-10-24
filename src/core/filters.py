"""规则引擎：决策流水线 include → exclude → delete → TRANSFER。

规则表（RuleSetConfig）在配置加载期完成容量解析与目录/文件拆分，
本模块运行期只做匹配与决策，不再解析任何配置值。被 core.handlers
的各模式处理器消费。
"""

import os
from collections.abc import Callable, Mapping

from src.core.patterns import match_pattern
from src.core.task_config import RuleSetConfig
from src.core.types import FileAction


class FileFilterPolicy:
    """消费加载期解析完成的规则表，决定文件或目录的处理方式。"""

    def __init__(
        self,
        include: RuleSetConfig,
        exclude: RuleSetConfig,
        delete: RuleSetConfig,
    ) -> None:
        self.include_rules = include
        self.exclude_rules = exclude
        self.delete_rules = delete
        self.included_dirs: set[str] = set()

    def decide(
        self,
        name: str,
        size_or_callable: int | Callable[[], int],
        is_dir: bool = False,
        parent_dir_sizes: Mapping[str, int | Callable[[], int]] | None = None,
    ) -> FileAction:
        """
        根据名称和大小，返回 FileAction 决策。
        size_or_callable 可以是一个数值，也可以是一个返回数值的可调用对象（用于惰性求值）。
        parent_dir_sizes 可选，是一个 dict[str, int | callable]，
        用于在 is_dir=False 时将父目录的 keep/delete 目录规则级联到文件。
        """
        if self.include_rules.has_rules:
            is_included = self._is_included(name, size_or_callable, is_dir)
            if not is_included:
                return FileAction.SKIP

        if self._check_exclude(name, size_or_callable, is_dir, parent_dir_sizes):
            return FileAction.SKIP

        if self._check_delete(name, size_or_callable, is_dir, parent_dir_sizes):
            return FileAction.DELETE

        return FileAction.TRANSFER

    @staticmethod
    def _matches(
        rules: RuleSetConfig,
        name: str,
        size_or_callable: int | Callable[[], int],
        is_dir: bool = False,
    ) -> bool:
        """
        判断文件或目录是否命中该组规则。
        支持惰性求值：如果命中 ge 且阈值为 -1，则不调用 size_or_callable 获取大小。
        """
        rules_lt = rules.dir_rules_lt if is_dir else rules.file_rules_lt
        rules_ge = rules.dir_rules_ge if is_dir else rules.file_rules_ge

        matching_thresholds_lt = []
        matching_thresholds_ge = []

        for pattern, threshold in rules_lt.items():
            if match_pattern(name, pattern):
                matching_thresholds_lt.append(threshold)

        for pattern, threshold in rules_ge.items():
            if match_pattern(name, pattern):
                # 如果 ge 规则阈值为 -1，必然命中，无需检查大小
                if threshold == -1:
                    return True
                matching_thresholds_ge.append(threshold)

        # 只有在没有命中 ge -1 且命中了其他有大小限制的规则时，才获取并检查大小
        if matching_thresholds_lt or matching_thresholds_ge:
            size = size_or_callable() if callable(size_or_callable) else size_or_callable
            for threshold in matching_thresholds_lt:
                if size < threshold:
                    return True
            for threshold in matching_thresholds_ge:
                if size >= threshold:
                    return True

        return False

    def _is_included(
        self, name: str, size_or_callable: int | Callable[[], int], is_dir: bool
    ) -> bool:
        if is_dir:
            if self._matches(self.include_rules, name, size_or_callable, is_dir=True):
                normalized_name = name.replace("\\", "/")
                self.included_dirs.add(normalized_name)
                return True
            return True

        if self._matches(self.include_rules, name, size_or_callable, is_dir=False):
            return True

        normalized_name = name.replace("\\", "/")
        parent = os.path.dirname(normalized_name)
        while parent:
            if parent in self.included_dirs:
                return True
            parent = os.path.dirname(parent)

        return False

    def _check_exclude(
        self,
        name: str,
        size_or_callable: int | Callable[[], int],
        is_dir: bool,
        parent_dir_sizes: Mapping[str, int | Callable[[], int]] | None,
    ) -> bool:
        if self._matches(self.exclude_rules, name, size_or_callable, is_dir):
            return True
        if not is_dir and parent_dir_sizes:
            normalized_name = name.replace("\\", "/")
            parent = os.path.dirname(normalized_name)
            while parent:
                if parent in parent_dir_sizes:
                    dir_size = parent_dir_sizes[parent]
                    if self._matches(self.exclude_rules, parent, dir_size, is_dir=True):
                        return True
                parent = os.path.dirname(parent)
        return False

    def _check_delete(
        self,
        name: str,
        size_or_callable: int | Callable[[], int],
        is_dir: bool,
        parent_dir_sizes: Mapping[str, int | Callable[[], int]] | None,
    ) -> bool:
        if self._matches(self.delete_rules, name, size_or_callable, is_dir):
            return True
        if not is_dir and parent_dir_sizes:
            normalized_name = name.replace("\\", "/")
            parent = os.path.dirname(normalized_name)
            while parent:
                if parent in parent_dir_sizes:
                    dir_size = parent_dir_sizes[parent]
                    if self._matches(self.delete_rules, parent, dir_size, is_dir=True):
                        return True
                parent = os.path.dirname(parent)
        return False
