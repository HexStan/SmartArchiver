"""规则引擎的领域词汇：文件名模式匹配与容量字符串解析。

被 core.filters 与 core.handlers.rotate 共用；与规则语义强相关，
故归属 core 而非通用工具层。
"""

import fnmatch

from humanfriendly import InvalidSize, parse_size


def match_pattern(name: str, pattern: str) -> bool:
    name = name.replace("\\", "/").lower()
    pattern = pattern.replace("\\", "/").lower()

    if "/" not in pattern:
        name = name.split("/")[-1]

    return fnmatch.fnmatch(name, pattern)


def parse_size_string(size_str: object) -> int:
    if not size_str:
        return 0

    s = str(size_str).strip()

    if s == "-1":
        return -1

    try:
        return parse_size(s, binary=True)
    except InvalidSize, ValueError:
        return 0
