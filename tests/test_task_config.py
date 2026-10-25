"""白盒测试：任务配置模型边界（task_config）。

为什么必须白盒（tests/README.md 原则二）：任务模型的字段级校验契约
（类型 / 范围 / 模式级交叉约束 / 未知字段拒绝）在加载期一次性判定，
从 handler.execute() 入口只能看到"任务被跳过"的终态，无法区分是哪条
约束命中。参照 test_config_parsing.py 的风格：直接调用配置模型 +
format_validation_error 断言中文消息。

钉死的契约（Pydantic 模型校验的行为决策）：
- 数字字段接受数字字符串（lax，与 http_remotes 数值字段一致），
  布尔与不可解析值一律拒绝（pydantic v2 不做 bool → 数值强转之外，
  布尔被显式排除以对齐既有"排除 bool"守卫）；
- extra="forbid"：未知字段（拼写错误、不适配当前模式的字段）拒绝任务；
- 模式级交叉约束：rotate 四类限额至少一项、sync 必填 dest、
  rotate/sync 不接受 include_rules（模型不声明该字段）；
- 中文错误文案逐字钉死（tool 配置值无效 / 数量配置值 / 至少一项 /
  规则键上下文的容量错误）。
"""

import pytest
from pydantic import ValidationError

from src.core.task_config import (
    MoveCopyTaskConfig,
    RotateRulesConfig,
    RotateTaskConfig,
    RuleSetConfig,
    SyncTaskConfig,
)
from src.validation import format_validation_error

GIB = 1024**3


def _move(**extra):
    task = {
        "mode": "move",
        "source": "s",
        "dest": "d",
    }
    task.update(extra)
    return task


def _rotate(**extra):
    task = {"mode": "rotate", "source": "s", "count_limit": 1}
    task.update(extra)
    return task


def _sync(**extra):
    task = {"mode": "sync", "source": "s", "dest": "d"}
    task.update(extra)
    return task


def _format(model, data):
    with pytest.raises(ValidationError) as exc_info:
        model.model_validate(data)
    return format_validation_error(exc_info.value)


class TestMoveCopyTask:
    def test_minimal_fields(self):
        task = MoveCopyTaskConfig.model_validate(_move())

        assert task.mode == "move"
        assert task.id is None
        assert task.name is None
        assert task.dest == "d"
        assert task.source_quiet_seconds == 0
        assert task.conflict_policy == "skip"
        assert task.max_retries == 3
        # 两个原必填项改为选填后的默认值：无时间限制、不清理空目录
        assert task.mtime_threshold_seconds == 0
        assert task.remove_empty_dirs is False
        assert not task.include_rules.has_rules
        assert not task.exclude_rules.has_rules
        assert not task.delete_rules.has_rules

    def test_numeric_strings_coerced(self):
        """数字字符串经 lax 强转（与 http_remotes 数值字段同一规则）。"""
        task = MoveCopyTaskConfig.model_validate(
            _move(mtime_threshold_seconds="10800", source_quiet_seconds="300")
        )

        assert task.mtime_threshold_seconds == 10800.0
        assert task.source_quiet_seconds == 300.0

    def test_bool_rejected(self):
        """布尔值不是数值——"排除 bool"守卫同样在模型层生效。"""
        message = _format(MoveCopyTaskConfig, _move(source_quiet_seconds=True))
        assert "source_quiet_seconds" in message
        assert "不接受布尔值" in message

    def test_non_numeric_rejected(self):
        message = _format(MoveCopyTaskConfig, _move(source_quiet_seconds="abc"))
        assert "source_quiet_seconds" in message
        assert "数值格式无效" in message

    def test_negative_mtime_rejected(self):
        """负阈值加载期拒绝，不留隐晦语义。"""
        message = _format(MoveCopyTaskConfig, _move(mtime_threshold_seconds=-5))
        assert "mtime_threshold_seconds" in message

    def test_negative_quiet_rejected(self):
        message = _format(MoveCopyTaskConfig, _move(source_quiet_seconds=-1))
        assert "source_quiet_seconds" in message

    def test_missing_required_field(self):
        """mtime_threshold_seconds 等原必填项已有默认值，source 仍是必填。"""
        data = _move()
        del data["source"]

        message = _format(MoveCopyTaskConfig, data)
        assert "source" in message
        assert "缺少必填字段" in message

    def test_empty_source_rejected(self):
        message = _format(MoveCopyTaskConfig, _move(source=""))
        assert "source" in message
        assert "不能为空" in message

    def test_unknown_field_rejected(self):
        """extra="forbid"：拼写错误（如 souce）显式拒绝，不静默失效。"""
        message = _format(MoveCopyTaskConfig, _move(souce="x"))
        assert "souce" in message
        assert "不支持的字段" in message

    def test_conflict_policy_case_insensitive(self):
        task = MoveCopyTaskConfig.model_validate(_move(conflict_policy="Overwrite"))
        assert task.conflict_policy == "overwrite"

    def test_invalid_conflict_policy_rejected(self):
        """非法冲突策略加载期拒绝，不静默吞掉。"""
        message = _format(MoveCopyTaskConfig, _move(conflict_policy="merge"))
        assert "conflict_policy" in message
        assert "配置值无效" in message

    def test_max_retries_numeric_string_coerced(self):
        task = MoveCopyTaskConfig.model_validate(_move(max_retries="2"))
        assert task.max_retries == 2

    def test_negative_max_retries_rejected(self):
        """max_retries 已下放为任务级配置：负值在任务模型层拒绝。"""
        message = _format(MoveCopyTaskConfig, _move(max_retries=-1))
        assert "max_retries" in message

    def test_max_retries_bool_rejected(self):
        message = _format(MoveCopyTaskConfig, _move(max_retries=True))
        assert "max_retries" in message
        assert "不接受布尔值" in message


class TestRotateTask:
    def test_size_limit_parsed(self):
        task = RotateTaskConfig.model_validate(_rotate(size_limit="1GiB"))
        assert task.size_limit == GIB

    def test_size_limit_minus_one_sentinel(self):
        """-1 是"匹配所有大小"的哨兵值。"""
        task = RotateTaskConfig.model_validate(_rotate(size_limit=-1))
        assert task.size_limit == -1

    def test_size_limit_empty_means_unlimited(self):
        task = RotateTaskConfig.model_validate(_rotate(size_limit=""))
        assert task.size_limit == 0

    def test_invalid_size_limit_rejected(self):
        message = _format(RotateTaskConfig, _rotate(size_limit="banana"))
        assert "容量字符串 'banana' 无法解析" in message

    def test_count_limit_numeric_string_coerced(self):
        task = RotateTaskConfig.model_validate(_rotate(count_limit="20"))
        assert task.count_limit == 20

    def test_invalid_count_limit_message_pinned(self):
        message = _format(RotateTaskConfig, _rotate(count_limit="banana"))
        assert "count_limit 的数量配置值 'banana' 无效" in message

    def test_negative_count_limit_rejected(self):
        """负数显式拒绝（0 才是不限制）。"""
        message = _format(RotateTaskConfig, _rotate(count_limit=-5))
        assert "count_limit" in message

    def test_at_least_one_limit_message_pinned(self):
        data = _rotate()
        del data["count_limit"]

        message = _format(RotateTaskConfig, data)
        assert (
            "rotate 模式下必须配置 size_limit、count_limit、"
            "rotate_rules.size 或 rotate_rules.count 中的至少一项" in message
        )

    def test_rotate_rules_only_count_satisfies_requirement(self):
        task = RotateTaskConfig.model_validate(
            {
                "mode": "rotate",
                "source": "s",
                "rotate_rules": {"count": {"*.log": 10}},
            }
        )
        assert task.rotate_rules.count == {"*.log": 10}

    def test_rotate_rules_size_parsed(self):
        task = RotateTaskConfig.model_validate(_rotate(rotate_rules={"size": {"logs/*": "10MiB"}}))
        assert task.rotate_rules.size == {"logs/*": 10 * 1024 * 1024}

    def test_include_rules_rejected(self):
        """rotate 模式不接受 include_rules（模型不声明该字段，forbid 拒绝）。"""
        message = _format(RotateTaskConfig, _rotate(include_rules={"ge": {"*.log": "-1"}}))
        assert "include_rules" in message
        assert "不支持的字段" in message

    def test_conflict_policy_accepted(self):
        task = RotateTaskConfig.model_validate(_rotate(conflict_policy="copy"))
        assert task.conflict_policy == "copy"

    def test_max_retries_accepted(self):
        task = RotateTaskConfig.model_validate(_rotate(max_retries=5))
        assert task.max_retries == 5


class TestSyncTask:
    def test_minimal_fields(self):
        task = SyncTaskConfig.model_validate(_sync())

        assert task.tool == "auto"
        assert task.exclude == []
        assert task.exclude_from == []
        assert task.create_backups is False
        assert task.max_backups == 0

    def test_dest_required(self):
        data = _sync()
        del data["dest"]

        message = _format(SyncTaskConfig, data)
        assert "dest" in message
        assert "缺少必填字段" in message

    def test_tool_case_insensitive_and_normalized(self):
        task = SyncTaskConfig.model_validate(_sync(tool="Rsync"))
        assert task.tool == "rsync"

    def test_invalid_tool_message_pinned(self):
        message = _format(SyncTaskConfig, _sync(tool="scp"))
        assert "sync 模式的 tool 配置值无效: 'scp'" in message
        assert "可选值: auto、rsync、rclone" in message

    def test_exclude_single_string_coerced(self):
        task = SyncTaskConfig.model_validate(_sync(exclude="*.tmp"))
        assert task.exclude == ["*.tmp"]

    def test_exclude_list_kept(self):
        task = SyncTaskConfig.model_validate(_sync(exclude=["*.tmp", "node_modules/"]))
        assert task.exclude == ["*.tmp", "node_modules/"]

    def test_exclude_non_string_item_rejected(self):
        message = _format(SyncTaskConfig, _sync(exclude=["a", 123]))
        assert "exclude" in message

    def test_exclude_from_single_string_coerced(self):
        task = SyncTaskConfig.model_validate(_sync(exclude_from="rules/exclude.txt"))
        assert task.exclude_from == ["rules/exclude.txt"]

    def test_exclude_from_list_kept(self):
        task = SyncTaskConfig.model_validate(_sync(exclude_from=["a.txt", "b.txt"]))
        assert task.exclude_from == ["a.txt", "b.txt"]

    def test_exclude_from_non_string_item_rejected(self):
        message = _format(SyncTaskConfig, _sync(exclude_from=["a.txt", 123]))
        assert "exclude_from" in message

    def test_negative_max_backups_rejected(self):
        message = _format(SyncTaskConfig, _sync(max_backups=-1))
        assert "max_backups" in message

    def test_max_retries_rejected(self):
        """max_retries 属于传输类任务：sync 不消费该字段，配置即按未知字段拒绝。"""
        message = _format(SyncTaskConfig, _sync(max_retries=3))
        assert "max_retries" in message
        assert "不支持的字段" in message


class TestRuleSetConfig:
    def test_sizes_parsed(self):
        rules = RuleSetConfig.model_validate({"lt": {"*.log": "10MiB"}, "ge": {"*.doc": "1MB"}})

        assert rules.lt == {"*.log": 10 * 1024 * 1024}
        assert rules.ge == {"*.doc": 1000 * 1000}

    def test_minus_one_sentinel(self):
        rules = RuleSetConfig.model_validate({"ge": {"*.tmp": -1}})
        assert rules.ge == {"*.tmp": -1}

    def test_invalid_size_carries_pattern_context(self):
        """错误消息携带规则键上下文。"""
        message = _format(RuleSetConfig, {"lt": {"*.tmp": "banana"}})
        assert "规则 '*.tmp' 的容量值无效" in message
        assert "容量字符串 'banana' 无法解析" in message

    def test_dir_file_split_views(self):
        """键尾斜杠 = 目录规则（斜杠剥除）；否则文件规则——拆分在加载期完成。"""
        rules = RuleSetConfig.model_validate(
            {"lt": {"docs/": "100MB", "*.log": "1MB"}, "ge": {"cache/": "-1"}}
        )

        assert rules.dir_rules_lt == {"docs": 100 * 1000 * 1000}
        assert rules.file_rules_lt == {"*.log": 1000 * 1000}
        assert rules.dir_rules_ge == {"cache": -1}
        assert rules.file_rules_ge == {}

    def test_has_rules(self):
        assert not RuleSetConfig.model_validate({}).has_rules
        assert RuleSetConfig.model_validate({"lt": {"a": "1B"}}).has_rules

    def test_unknown_key_rejected(self):
        """lt/ge 之外的键（如直接把模式写在规则表顶层）拒绝。"""
        message = _format(RuleSetConfig, {"*.tmp": "-1"})
        assert "不支持的字段" in message


class TestRotateRulesConfig:
    def test_size_and_count_parsed(self):
        rules = RotateRulesConfig.model_validate(
            {"size": {"logs/*": "10MiB"}, "count": {"alpha/*": "10"}}
        )

        assert rules.size == {"logs/*": 10 * 1024 * 1024}
        assert rules.count == {"alpha/*": 10}

    def test_invalid_size_rejected(self):
        message = _format(RotateRulesConfig, {"size": {"logs/*": "banana"}})
        assert "容量字符串 'banana' 无法解析" in message

    def test_invalid_count_rejected(self):
        message = _format(RotateRulesConfig, {"count": {"alpha/*": "banana"}})
        assert "alpha/*" in message
        assert "数值格式无效" in message
