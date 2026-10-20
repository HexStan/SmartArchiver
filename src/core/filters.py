import os
from collections.abc import Callable, Mapping
from typing import Any

from src.core.patterns import InvalidSizeError, match_pattern, parse_size_string
from src.core.types import FileAction


class FileFilterPolicy:
    """
    负责解析过滤规则并决定文件或目录的处理方式。
    决策流水线: include → exclude → delete → TRANSFER
    """

    class _RuleSet:
        def __init__(self, rules_config: dict[str, Any]) -> None:
            """内部辅助类，用于解析目录和文件的匹配规则"""

            self.dir_rules_lt: dict[str, int] = {}
            self.dir_rules_ge: dict[str, int] = {}
            self.file_rules_lt: dict[str, int] = {}
            self.file_rules_ge: dict[str, int] = {}

            self._parse_rules(
                rules_config,
                self.dir_rules_lt,
                self.dir_rules_ge,
                self.file_rules_lt,
                self.file_rules_ge,
            )

        def _parse_rules(
            self,
            raw_rules: dict[str, Any],
            dir_lt: dict[str, int],
            dir_ge: dict[str, int],
            file_lt: dict[str, int],
            file_ge: dict[str, int],
        ) -> None:
            def parse_rule_size(pattern: str, size_str: Any) -> int:
                """补充规则键上下文，让用户能从错误日志定位到具体规则。"""
                try:
                    return parse_size_string(size_str)
                except InvalidSizeError as e:
                    raise InvalidSizeError(f"规则 '{pattern}' 的容量值无效：{e}") from e

            for pattern, size_str in raw_rules.get("lt", {}).items():
                size = parse_rule_size(pattern, size_str)
                if pattern.endswith("/"):
                    dir_lt[pattern[:-1]] = size
                else:
                    file_lt[pattern] = size

            for pattern, size_str in raw_rules.get("ge", {}).items():
                size = parse_rule_size(pattern, size_str)
                if pattern.endswith("/"):
                    dir_ge[pattern[:-1]] = size
                else:
                    file_ge[pattern] = size

        def matches(
            self, name: str, size_or_callable: int | Callable[[], int], is_dir: bool = False
        ) -> bool:
            """
            判断文件或目录是否命中该组规则。
            支持惰性求值：如果命中 ge 且阈值为 0，则不调用 size_or_callable 获取大小。
            """
            rules_lt = self.dir_rules_lt if is_dir else self.file_rules_lt
            rules_ge = self.dir_rules_ge if is_dir else self.file_rules_ge

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

        @property
        def has_rules(self) -> bool:
            return bool(
                self.dir_rules_lt or self.dir_rules_ge or self.file_rules_lt or self.file_rules_ge
            )

    def __init__(self, config: dict[str, Any]) -> None:
        self.include_rules = self._RuleSet(config.get("include_rules", {}))
        self.exclude_rules = self._RuleSet(config.get("exclude_rules", {}))
        self.delete_rules = self._RuleSet(config.get("delete_rules", {}))
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

    def _is_included(
        self, name: str, size_or_callable: int | Callable[[], int], is_dir: bool
    ) -> bool:
        if is_dir:
            if self.include_rules.matches(name, size_or_callable, is_dir=True):
                normalized_name = name.replace("\\", "/")
                self.included_dirs.add(normalized_name)
                return True
            return True

        if self.include_rules.matches(name, size_or_callable, is_dir=False):
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
        if self.exclude_rules.matches(name, size_or_callable, is_dir):
            return True
        if not is_dir and parent_dir_sizes:
            normalized_name = name.replace("\\", "/")
            parent = os.path.dirname(normalized_name)
            while parent:
                if parent in parent_dir_sizes:
                    dir_size = parent_dir_sizes[parent]
                    if self.exclude_rules.matches(parent, dir_size, is_dir=True):
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
        if self.delete_rules.matches(name, size_or_callable, is_dir):
            return True
        if not is_dir and parent_dir_sizes:
            normalized_name = name.replace("\\", "/")
            parent = os.path.dirname(normalized_name)
            while parent:
                if parent in parent_dir_sizes:
                    dir_size = parent_dir_sizes[parent]
                    if self.delete_rules.matches(parent, dir_size, is_dir=True):
                        return True
                parent = os.path.dirname(parent)
        return False
