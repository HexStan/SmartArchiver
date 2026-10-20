"""规则引擎的领域词汇：文件名模式匹配与容量字符串解析。

被 core.filters 与 core.handlers.rotate 共用；与规则语义强相关，
故归属 core 而非通用工具层。容量解析基于 pydantic 的 ByteSize，
单位语义遵循 IEC 80000-13（KB=1000，KiB=1024）。
"""

import fnmatch
import re

from pydantic import ByteSize, TypeAdapter, ValidationError

# ByteSize 的内部正则没有尾部锚（"10MB extra" 也会被截断接受），
# 严格模式下先用本模式锚定整串形状，再交给 ByteSize 解析单位表。
_SIZE_SHAPE = re.compile(r"\s*\d*\.?\d+\s*[a-zA-Z]*\s*")

_BYTE_SIZE_ADAPTER: TypeAdapter[ByteSize] = TypeAdapter(ByteSize)


class InvalidSizeError(ValueError):
    """容量字符串无法解析（配置错误）。

    由 parse_size_string 的严格模式抛出。调用方应在校验边界
    （registry.create_handler，构造期统一捕获）记日志跳过任务；
    构造完成后的下游（validate / execute / 展示）不再解析。
    """


def match_pattern(name: str, pattern: str) -> bool:
    name = name.replace("\\", "/").lower()
    pattern = pattern.replace("\\", "/").lower()

    if "/" not in pattern:
        name = name.split("/")[-1]

    return fnmatch.fnmatch(name, pattern)


def parse_size_string(size_str: object) -> int:
    """把容量字符串解析为字节数（IEC 80000-13：KB=1000，KiB=1024）。

    - 空值视为未配置，返回 0（= 不限制）；
    - "-1" 是"匹配所有大小"的哨兵值；
    - 其余无法解析的输入抛 InvalidSizeError：配置错误必须显式失败，
      静默降级为 0 会让规则被静默禁用。
    """
    if not size_str:
        return 0

    s = str(size_str).strip()

    if s == "-1":
        return -1

    if not _SIZE_SHAPE.fullmatch(s):
        raise InvalidSizeError(f"容量字符串 {s!r} 无法解析")

    try:
        return int(_BYTE_SIZE_ADAPTER.validate_python(s))
    except ValidationError as e:
        raise InvalidSizeError(f"容量字符串 {s!r} 无法解析") from e
