"""任务配置模型层：TOML 任务条目 → 类型化任务对象。

所有"仅凭配置文件本身即可判定"的任务级合法性校验收口于此：字段
存在性、类型与范围、模式级交叉约束（rotate 至少一项限额、sync 必填
dest 等）。需要外部条件的校验（目录存在、远端可达、依赖任务完成）
不在本层——前者由 handlers 在运行期检查，后者由 app.runtime 的任务
循环判定。

模型设计：
- 按 mode 判别：``TASK_MODELS`` 维护 mode → 模型类的分派表（解析器
  按 mode 选模型后手工分派，不用 pydantic discriminated union——
  为的是 mode 缺失/不支持时能逐字输出钉死的中文错误消息）；
- ``extra="forbid"``：未知字段（含拼写错误、不适配当前模式的字段）
  拒绝任务——配置文件驱动的工具里，字段拼错静默失效最难排查；
- ``frozen=True``：模型加载后不可变，处理器只读消费；
- 数字字段接受数字字符串（lax 强转，与 http_remotes 数值字段的
  规则一致）；布尔值一律拒绝——pydantic lax 会把 True 强转为 1.0，与
  既有"排除 bool"守卫不符，由 _reject_bool 显式排除。

错误消息约定：测试钉死的文案由 validator 抛中文 ValueError 原样
透出（经 format_validation_error 的 value_error 分支剥前缀）；其余
字段交给类型与范围约束，由 format_validation_error 统一翻译。
"""

from typing import Annotated, Literal, Self

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    PrivateAttr,
    StringConstraints,
    field_validator,
    model_validator,
)

from src.core.patterns import InvalidSizeError, parse_size_string


def _parse_size_value(value: object) -> int:
    """容量配置值 → 字节数（复用 parse_size_string 的完整语义）。

    经 str() 归一后交给 parse_size_string：空值视为 0（不限制）、"-1"
    为"匹配所有大小"哨兵、其余无法解析的值抛 InvalidSizeError（
    ValueError 子类）——静默降级为 0 会让规则被静默禁用，必须显式失败。
    """
    return parse_size_string(str(value))


SizeValue = Annotated[int, BeforeValidator(_parse_size_value)]


def _lower_str(value: object) -> object:
    """字符串统一转小写（取值大小写不敏感的既有行为），其余原样放行。"""
    return value.lower() if isinstance(value, str) else value


def _reject_bool(value: object) -> object:
    """数值字段拒绝布尔值（pydantic lax 会把 True 强转为 1.0，既有校验
    明确排除布尔，语义为配置错误而非数值）。"""
    if isinstance(value, bool):
        # pydantic validator 契约：仅 ValueError 会被转换为 ValidationError，
        # TypeError 会绕过模型校验直接冒泡，故此处必须抛 ValueError
        raise ValueError("配置值无效（不接受布尔值）")  # noqa: TRY004
    return value


class RuleSetConfig(BaseModel):
    """include/exclude/delete 规则表：lt / ge 两张"模式 → 字节阈值"映射。

    键以 / 结尾表示目录规则（匹配目录名，斜杠剥除）。目录/文件两张
    匹配视图的拆分在加载期由 model_validator 产出，运行期匹配零解析。
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    lt: dict[str, int] = Field(default_factory=dict)
    ge: dict[str, int] = Field(default_factory=dict)

    _dir_lt: dict[str, int] = PrivateAttr(default_factory=dict)
    _dir_ge: dict[str, int] = PrivateAttr(default_factory=dict)
    _file_lt: dict[str, int] = PrivateAttr(default_factory=dict)
    _file_ge: dict[str, int] = PrivateAttr(default_factory=dict)

    @field_validator("lt", "ge", mode="before")
    @classmethod
    def _parse_thresholds(cls, value: object) -> object:
        """逐规则解析容量值；失败时携带规则键上下文（既有错误文案）。"""
        if not isinstance(value, dict):
            return value  # 非字典交由字段类型校验报错
        parsed: dict[str, int] = {}
        for pattern, size_value in value.items():
            try:
                parsed[pattern] = parse_size_string(str(size_value))
            except InvalidSizeError as e:
                raise ValueError(f"规则 '{pattern}' 的容量值无效：{e}") from e
        return parsed

    @model_validator(mode="after")
    def _split_dir_file(self) -> Self:
        """按键尾部斜杠拆分目录/文件两张匹配视图（加载期一次完成）。"""
        for source, dir_view, file_view in (
            (self.lt, self._dir_lt, self._file_lt),
            (self.ge, self._dir_ge, self._file_ge),
        ):
            for pattern, threshold in source.items():
                if pattern.endswith("/"):
                    dir_view[pattern[:-1]] = threshold
                else:
                    file_view[pattern] = threshold
        return self

    @property
    def dir_rules_lt(self) -> dict[str, int]:
        return self._dir_lt

    @property
    def dir_rules_ge(self) -> dict[str, int]:
        return self._dir_ge

    @property
    def file_rules_lt(self) -> dict[str, int]:
        return self._file_lt

    @property
    def file_rules_ge(self) -> dict[str, int]:
        return self._file_ge

    @property
    def has_rules(self) -> bool:
        return bool(self._dir_lt or self._dir_ge or self._file_lt or self._file_ge)


class RotateRulesConfig(BaseModel):
    """rotate_rules：细粒度轮转规则（size 容量表 + count 数量表）。"""

    model_config = ConfigDict(extra="forbid", frozen=True)

    size: dict[str, SizeValue] = Field(default_factory=dict)
    count: dict[str, int] = Field(default_factory=dict)


def _coerce_exclude(value: object) -> object:
    """exclude / exclude_from 单字符串容错为单元素列表（既有语义），其余原样放行。"""
    if isinstance(value, str):
        return [value]
    return value


class TaskBase(BaseModel):
    """各模式任务的公共字段（mode 由子类以 Literal 收窄并参与分派）。"""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # id 的"非法降级为无 id"语义在解析器（task_plan）处理：降级不是
    # 校验失败，模型只接收归一后的干净值（非法值被写回为 None）
    id: str | None = None
    name: str | None = None
    # 前置任务 id 列表：单字符串 → 单元素元组的归一与跨任务引用校验在
    # 解析器完成，模型只声明归一后的字段形态
    depends_on: tuple[str, ...] = ()
    source: Annotated[str, StringConstraints(min_length=1)]
    # dest 允许为空：move/copy/rotate 省略目标时任务合法但不传输，
    # 逐文件动作由 ActionExecutor 的守卫跳过；sync 覆写为必填
    dest: str = ""
    source_quiet_seconds: Annotated[float, BeforeValidator(_reject_bool)] = Field(default=0, ge=0)


class TransferTaskConfig(TaskBase):
    """逐文件传输类任务（move/copy/rotate）的公共字段。"""

    # mode 在 TaskBase 层不声明（由各模式子类以 Literal 收窄并参与分派）；
    # 传输类装配层需以 str 读取 mode（失败历史的任务身份），此处补基线
    # 声明，子类仍以 Literal 覆写收窄，校验行为不变
    mode: str
    conflict_policy: Annotated[
        Literal["skip", "overwrite", "copy"], BeforeValidator(_lower_str)
    ] = "skip"
    # 缺省不清理：清空目录属删除类副作用，显式开启才启用
    remove_empty_dirs: bool = False
    # 单文件操作失败的最大重试次数：连续失败达到次数后，该文件在本任务
    # 身份下进入失败历史隔离，后续轮次跳过（成功一次即清零）。失败历史
    # 按 (mode, source, dest) 任务身份隔离计数，阈值随任务级配置逐任务
    # 判定。sync 整树交给外部工具，不消费此字段——挂在 TransferTaskConfig
    # 而非 TaskBase，extra=forbid 使 sync 任务配置它时在加载期被拒绝
    max_retries: Annotated[int, BeforeValidator(_reject_bool)] = Field(default=3, ge=0)


class MoveCopyTaskConfig(TransferTaskConfig):
    """move / copy 模式任务配置。"""

    mode: Literal["move", "copy"]
    # 0 的既有语义即"无时间限制"，缺省复用该语义
    mtime_threshold_seconds: Annotated[float, BeforeValidator(_reject_bool)] = Field(
        default=0, ge=0
    )
    include_rules: RuleSetConfig = Field(default_factory=RuleSetConfig)
    exclude_rules: RuleSetConfig = Field(default_factory=RuleSetConfig)
    delete_rules: RuleSetConfig = Field(default_factory=RuleSetConfig)


class RotateTaskConfig(TransferTaskConfig):
    """rotate 模式任务配置（限额在加载期解析为字节数 / 数量）。

    include 不参与 rotate：模型不声明 include_rules 字段，extra=forbid
    使"rotate 模式配置 include_rules"在加载期被拒绝（既有约束）。
    """

    mode: Literal["rotate"]
    exclude_rules: RuleSetConfig = Field(default_factory=RuleSetConfig)
    delete_rules: RuleSetConfig = Field(default_factory=RuleSetConfig)
    size_limit: SizeValue = 0
    count_limit: int = Field(default=0, ge=0)
    rotate_rules: RotateRulesConfig = Field(default_factory=RotateRulesConfig)

    @field_validator("count_limit", mode="before")
    @classmethod
    def _parse_count_limit(cls, value: object) -> object:
        """数量配置值强转；消息钉死既有文案（字段名由 loc 前缀再述一次）。

        布尔与不可强转的值一律报错；浮点沿用 int() 的截断语义。
        """
        if isinstance(value, bool) or not isinstance(value, (int, float, str)):
            # 同 _reject_bool：validator 内必须抛 ValueError 才能进入
            # ValidationError 通道，类型类错误不适用 TRY004 的建议
            raise ValueError(f"count_limit 的数量配置值 {value!r} 无效（应为整数）")  # noqa: TRY004
        try:
            return int(value)
        except ValueError as e:
            raise ValueError(f"count_limit 的数量配置值 {value!r} 无效（应为整数）") from e

    @model_validator(mode="after")
    def _require_any_limit(self) -> Self:
        """四类限额至少配置一项（消息逐字钉死）。"""
        if (
            self.size_limit == 0
            and self.count_limit == 0
            and not self.rotate_rules.size
            and not self.rotate_rules.count
        ):
            raise ValueError(
                "rotate 模式下必须配置 size_limit、count_limit、"
                "rotate_rules.size 或 rotate_rules.count 中的至少一项"
            )
        return self


class SyncTaskConfig(TaskBase):
    """sync 模式任务配置（整树同步，由外部工具执行）。"""

    mode: Literal["sync"]
    # 覆写基类默认值：sync 没有目标目录即无意义，dest 必填
    dest: str
    tool: str = "auto"
    exclude: Annotated[list[str], BeforeValidator(_coerce_exclude)] = Field(default_factory=list)
    # 排除规则文件路径列表（透传给工具的 --exclude-from）：只传路径不做内容解析，
    # 文件内注释行/模式语法由底层工具处理；文件存在性是外部条件，
    # 由 handler 在运行期校验，不在加载期判定
    exclude_from: Annotated[list[str], BeforeValidator(_coerce_exclude)] = Field(
        default_factory=list
    )
    # 备份目录：同步中被替换/删除的文件由底层工具移入 [backup_dir]/[时间戳] 目录。
    # 空（默认）表示不创建备份；该目录视为备份独占目录，max_backups 轮转
    # 会按子目录名排序删除其中的旧子目录，请勿混放非备份数据。
    # dest 为 SSH 远端时是同主机上的远端 POSIX 绝对路径。
    # "不得位于 source/dest 之下"属跨字段语义校验，由 task_plan 经
    # dest.validate_backup_dir 在加载期判定（需要 source/dest 的路径语义）。
    backup_dir: str = ""
    max_backups: int = Field(default=0, ge=0)

    @field_validator("tool")
    @classmethod
    def _validate_tool(cls, value: str) -> str:
        """取值校验（消息逐字钉死）；大小写不敏感，归一为小写。"""
        lowered = value.lower()
        if lowered not in ("auto", "rsync", "rclone"):
            raise ValueError(f"sync 模式的 tool 配置值无效: '{value}'，可选值: auto、rsync、rclone")
        return lowered


TaskConfig = MoveCopyTaskConfig | RotateTaskConfig | SyncTaskConfig

# mode → 配置模型的分派表（解析器使用；与 registry 的 mode → 处理器
# 映射由同一组模式名驱动，不一致属编程错误，registry 有防御分支兜底）
TASK_MODELS: dict[str, type[TaskConfig]] = {
    "move": MoveCopyTaskConfig,
    "copy": MoveCopyTaskConfig,
    "rotate": RotateTaskConfig,
    "sync": SyncTaskConfig,
}
