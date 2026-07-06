import os
from src.utils import parse_size_string, match_pattern
from src.core.types import FileAction


class FileFilterPolicy:
    """
    负责解析过滤规则并决定文件或目录的处理方式。
    决策流水线: include → exclude → delete → TRANSFER
    """

    class _RuleSet:
        def __init__(self, rules_config):
            """内部辅助类，用于解析目录和文件的匹配规则"""

            self.dir_rules_lt = {}
            self.dir_rules_ge = {}
            self.file_rules_lt = {}
            self.file_rules_ge = {}

            self._parse_rules(
                rules_config,
                self.dir_rules_lt,
                self.dir_rules_ge,
                self.file_rules_lt,
                self.file_rules_ge,
            )

        def _parse_rules(self, raw_rules, dir_lt, dir_ge, file_lt, file_ge):
            for pattern, size_str in raw_rules.get("lt", {}).items():
                size = parse_size_string(size_str)
                if pattern.endswith("/"):
                    dir_lt[pattern[:-1]] = size
                else:
                    file_lt[pattern] = size

            for pattern, size_str in raw_rules.get("ge", {}).items():
                size = parse_size_string(size_str)
                if pattern.endswith("/"):
                    dir_ge[pattern[:-1]] = size
                else:
                    file_ge[pattern] = size

        def matches(self, name, size_or_callable, is_dir=False):
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
                size = (
                    size_or_callable()
                    if callable(size_or_callable)
                    else size_or_callable
                )
                for threshold in matching_thresholds_lt:
                    if size < threshold:
                        return True
                for threshold in matching_thresholds_ge:
                    if size >= threshold:
                        return True

            return False

        @property
        def has_rules(self):
            return bool(
                self.dir_rules_lt
                or self.dir_rules_ge
                or self.file_rules_lt
                or self.file_rules_ge
            )

    def __init__(self, config):
        self.include_rules = self._RuleSet(config.get("include_rules", {}))
        self.exclude_rules = self._RuleSet(config.get("exclude_rules", {}))
        self.delete_rules = self._RuleSet(config.get("delete_rules", {}))
        self.included_dirs = set()

    def decide(self, name, size_or_callable, is_dir=False, parent_dir_sizes=None):
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

    def _is_included(self, name, size_or_callable, is_dir):
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

    def _check_exclude(self, name, size_or_callable, is_dir, parent_dir_sizes):
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

    def _check_delete(self, name, size_or_callable, is_dir, parent_dir_sizes):
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
