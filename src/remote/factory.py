"""HTTP 远端配置解析：[[http_remotes]] 条目 → RemoteClient。"""

from typing import TYPE_CHECKING, Annotated, Any

from pydantic import BaseModel, Field, StringConstraints, ValidationError

from src.remote.client import RemoteClient
from src.validation import RemoteAlias, format_validation_error

if TYPE_CHECKING:
    from src.output.logger import Logger


class HttpRemoteConfig(BaseModel):
    """单个 [[http_remotes]] 条目的配置模型。

    alias/address/key 必填（address/key 自动去除首尾空白）；timeout 与
    queue_time 接受数字或数字字符串（lax 强转），不允许负值。
    """

    alias: RemoteAlias
    address: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
    key: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
    timeout: float | None = Field(default=None, ge=0)
    queue_time: float = Field(default=0, ge=0)


def parse_remote_config(config: dict[str, Any], logger: Logger) -> dict[str, RemoteClient]:
    """解析 http_remotes 配置块为 {alias: RemoteClient} 映射。

    逐条目容错：单个条目非法（缺必填、格式错误、数值越界）记 warning
    日志后跳过，不影响其余条目。重复 alias 先到先得（与任务 id "先出现者
    获得 id" 的语义一致），重复者告警并忽略。
    """
    remotes: dict[str, RemoteClient] = {}
    entries = config.get("http_remotes", [])
    if not entries:
        return remotes
    if not isinstance(entries, list):
        logger.warning("http_remotes 配置应为条目数组，已忽略全部条目。")
        return remotes

    for index, entry in enumerate(entries):
        try:
            item = HttpRemoteConfig.model_validate(entry)
        except ValidationError as e:
            logger.warning(
                f"http_remotes 第 {index + 1} 个条目无效，已忽略：{format_validation_error(e)}"
            )
            continue

        if item.alias in remotes:
            logger.warning(
                f"http_remotes 中别名 '{item.alias}' 重复"
                f"（第 {index + 1} 个条目），保留先出现的条目。"
            )
            continue

        remotes[item.alias] = RemoteClient(
            address=item.address,
            api_key=item.key,
            alias=item.alias,
            timeout=item.timeout,
            queue_time=item.queue_time,
        )

    return remotes
