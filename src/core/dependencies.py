"""任务依赖解析：``tasks.id`` 规范化与 ``depends_on`` 静态校验。

职责边界：本模块只做"解析期"的纯检查与日志，产出与任务索引对齐的
TaskSpec 列表；"运行期"的依赖满足判定（前置任务是否进入已完成集合）
由 app.runtime 的任务循环完成。逐任务配置错误沿用项目惯例：错误日志
+ 跳过该任务（blocked），不致命。

id 规则：
- 选填；合法字符集为 a-z、A-Z、0-9、-、_（与远端 alias 一致）；
- 非法（类型错误/字符不符）视为无 id：任务照常执行，不可被依赖；
- 与前方任务重复时报错并跳过该任务——若静默降级为"无 id 照常跑"，
  后置任务的 depends_on 会解析到第一个同名任务，用户意图被篡改；
  跳过重复者让"先出现者获得 id"的解析结果无歧义。

depends_on 规则：
- 选填；字符串数组，单个字符串容错视为单元素列表；
- 仅允许引用配置顺序中更早任务的 id；引用不存在、自身或后置任务均
  报错并跳过该任务（配置顺序即执行顺序，依赖只是门禁）。
"""

import re
from typing import Any, NamedTuple

from src.output.logger import Logger

TASK_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")


class TaskSpec(NamedTuple):
    """单个任务在执行编排中的静态信息（与 tasks 列表索引对齐）。

    task_id —— 规范化后的任务标识；id 缺省或非法时为 None（不可被依赖，
        也不能进入已完成集合）。
    depends_on —— 校验通过的前置任务 id 列表。
    blocked —— 配置存在依赖类错误（非字典任务、id 重复、depends_on
        类型非法、引用不存在/自身/后置任务），本轮必须跳过；具体错误
        日志已在解析期给出，运行期静默跳过即可。
    """

    task_id: str | None
    depends_on: tuple[str, ...]
    blocked: bool = False


def _blocked() -> TaskSpec:
    return TaskSpec(task_id=None, depends_on=(), blocked=True)


def _normalize_id(task: dict[str, Any], index: int, logger: Logger) -> str | None:
    """规范化任务 id：非法时告警并返回 None（任务照常执行，不可被依赖）。"""
    raw_id = task.get("id")
    if raw_id is None:
        return None
    if not isinstance(raw_id, str):
        logger.warning(f"任务 #{index + 1} 的 id 配置值非法（应为字符串），视为无 id。")
        return None
    if not TASK_ID_PATTERN.match(raw_id):
        logger.warning(
            f"任务 #{index + 1} 的 id '{raw_id}' 含非法字符"
            "（仅允许 a-z、A-Z、0-9、-、_），视为无 id。"
        )
        return None
    return raw_id


def _normalize_depends_on(
    task: dict[str, Any], index: int, logger: Logger
) -> tuple[str, ...] | None:
    """规范化 depends_on；类型非法时记错误并返回 None（调用方置 blocked）。"""
    raw = task.get("depends_on")
    if raw is None:
        return ()
    if isinstance(raw, str):
        return (raw,)
    if isinstance(raw, list) and all(isinstance(item, str) for item in raw):
        return tuple(raw)
    logger.error(
        f"任务 #{index + 1} 的 depends_on 配置值非法（应为字符串或字符串数组），跳过该任务。"
    )
    return None


def _has_unresolvable_reference(
    spec: TaskSpec, index: int, registered: dict[str, int], logger: Logger
) -> bool:
    """依赖引用是否存在无法满足项（此时全部合法 id 已注册）。"""
    for dep in spec.depends_on:
        target = registered.get(dep)
        if target is None:
            logger.error(f"任务 #{index + 1} 依赖的 id '{dep}' 不存在，跳过该任务。")
            return True
        if target == index:
            logger.error(f"任务 #{index + 1} 依赖了自身 (id '{dep}')，跳过该任务。")
            return True
        if target > index:
            logger.error(
                f"任务 #{index + 1} 依赖了位于其后方的任务 '{dep}'"
                "（仅允许引用配置顺序中更早的任务），跳过该任务。"
            )
            return True
    return False


def prepare_run_plan(tasks: list[Any], logger: Logger) -> list[TaskSpec]:
    """解析任务列表，产出与索引对齐的 TaskSpec（含全部校验日志）。

    第一遍做 id 规范化与注册（仅合法 id，先到先得）、depends_on 类型
    归一；第二遍在全部合法 id 注册完毕后做引用校验（存在性 / 自引用 /
    前向引用）。两遍产出与 tasks 一一对应的 draft，最终按序返回。
    """
    drafts: list[TaskSpec] = []
    registered: dict[str, int] = {}  # id → 首个出现的任务索引
    for index, task in enumerate(tasks):
        if not isinstance(task, dict):
            # 非字典结构：_warn_task_structure 已告警"将被跳过"，运行期对齐
            drafts.append(_blocked())
            continue

        task_id = _normalize_id(task, index, logger)
        if task_id is not None and task_id in registered:
            logger.error(
                f"任务 #{index + 1} 的 id '{task_id}' 与任务 #{registered[task_id] + 1}"
                " 重复，跳过该任务。"
            )
            drafts.append(_blocked())
            continue
        if task_id is not None:
            registered[task_id] = index

        depends_on = _normalize_depends_on(task, index, logger)
        if depends_on is None:
            drafts.append(_blocked())
            continue

        drafts.append(TaskSpec(task_id, depends_on, blocked=False))

    specs: list[TaskSpec] = []
    for index, draft in enumerate(drafts):
        if draft.blocked:
            specs.append(draft)
        elif _has_unresolvable_reference(draft, index, registered, logger):
            specs.append(_blocked())
        else:
            specs.append(draft)
    return specs
