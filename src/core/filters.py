import os
from src.utils import parse_size_string, match_pattern
from src.core.types import FileAction

class FileFilterPolicy:
    """
    负责解析过滤规则并决定文件或目录的处理方式
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

    def __init__(self, config):
        self.is_whitelist_mode = config.get("is_whitelist_mode", False)
        self.keep_rules = self._RuleSet(config.get("keep_rules", {}))
        self.delete_rules = self._RuleSet(config.get("delete_rules", {}))
        self.preferred_rule = config.get("preferred_rule", "keep")
        self.whitelisted_dirs = set()
        if self.is_whitelist_mode:
            self.whitelist_rules = self._RuleSet(config.get("whitelist_rules", {}))

    def decide(self, name, size_or_callable, is_dir=False):
        """
        根据名称和大小，返回 FileAction 决策。
        size_or_callable 可以是一个数值，也可以是一个返回数值的可调用对象（用于惰性求值）。
        """
        match_keep = self.keep_rules.matches(name, size_or_callable, is_dir)
        match_delete = self.delete_rules.matches(name, size_or_callable, is_dir)

        # 1. 如果同时命中保留和删除规则，根据配置项处理
        if match_keep and match_delete:
            if self.preferred_rule == "delete":
                return FileAction.DELETE
            else:
                return FileAction.SKIP

        # 2. 如果只命中保留规则，保留目标
        elif match_keep:
            return FileAction.SKIP

        # 3. 如果只命中删除规则，删除目标
        elif match_delete:
            return FileAction.DELETE

        if self.is_whitelist_mode:
            is_whitelisted = self.whitelist_rules.matches(
                name, size_or_callable, is_dir
            )

            if is_whitelisted and is_dir:
                # 记录命中的目录，以便其子文件/子目录继承白名单状态
                # 统一使用正斜杠以避免路径分隔符问题
                normalized_name = name.replace("\\", "/")
                self.whitelisted_dirs.add(normalized_name)

            if not is_whitelisted:
                # 检查父目录是否在白名单中
                normalized_name = name.replace("\\", "/")
                parent = os.path.dirname(normalized_name)
                while parent:
                    if parent in self.whitelisted_dirs:
                        is_whitelisted = True
                        break
                    parent = os.path.dirname(parent)

            if not is_whitelisted:
                if not is_dir:
                    return FileAction.SKIP
                else:
                    # 目录未命中白名单，但我们需要遍历它以寻找可能命中白名单的子项
                    return FileAction.TRANSFER
            else:
                return FileAction.TRANSFER

        # 4. 都不命中，正常传输
        return FileAction.TRANSFER
