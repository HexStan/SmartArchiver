"""白盒测试：配置解析边界的容错契约（远端条目 / 服务器配置）。

为什么必须白盒（tests/README.md 原则二）：ConfigManager 的解析边界位于
灰盒入口（handler.execute()）之外——"单个条目非法记 warning 后跳过、
重复 alias 先到先得、非法值不崩溃且日志不泄露密钥"这些契约从处理器
入口无法观察，只能直接调用解析函数与配置模型，以捕获日志为断言对象。

钉死的契约（Pydantic 模型校验的行为决策）：
- 非字符串配置值判非法（不做 str() 强转收编）；
- 数值范围严格校验（port 1-65535、timeout/queue_time >= 0、并发 >= 1、队列 >= 0）；
- 重复 alias 先到先得 + 警告（与任务 id "先出现者获得 id" 语义一致）；
- 条目非法的 warning 日志绝不包含 api_key 原值。
"""

import pytest
from pydantic import ValidationError

from src.app.config import GlobalConfig
from src.remote.factory import parse_remote_config
from src.ssh.remotes import parse_ssh_remote_config


def _http_config(**overrides):
    """合法 http_remotes 条目，字段值刻意带空白以钉死 strip 行为。"""
    entry = {"alias": "nas", "address": " http://10.0.0.1:13579/ ", "key": "secret"}
    entry.update(overrides)
    return {"http_remotes": [entry]}


def _ssh_config(**overrides):
    entry = {"alias": "vps", "host": " 192.168.1.100 ", "user": "root"}
    entry.update(overrides)
    return {"ssh_remotes": [entry]}


class TestParseRemoteConfig:
    def test_valid_entry_maps_to_client(self, capture_log):
        remotes = parse_remote_config(_http_config(timeout="3600", queue_time=30), capture_log)

        assert set(remotes) == {"nas"}
        client = remotes["nas"]
        assert client.address == "http://10.0.0.1:13579"
        assert client.api_key == "secret"
        # 数字字符串经 lax 强转为 float
        assert client.timeout == 3600.0
        assert client.queue_time == 30.0

    def test_optional_fields_default(self, capture_log):
        remotes = parse_remote_config(_http_config(), capture_log)

        # timeout 为 None 时 RemoteClient 落默认 4 小时；queue_time 落 0
        assert remotes["nas"].timeout == 14400
        assert remotes["nas"].queue_time == 0.0

    def test_missing_required_field_skips_with_warning(self, capture_log):
        config = _http_config()
        del config["http_remotes"][0]["key"]

        assert parse_remote_config(config, capture_log) == {}
        assert capture_log.has("warning", "http_remotes 第 1 个条目无效")
        assert capture_log.has("warning", "key: 缺少必填字段")

    def test_invalid_alias_skips_with_warning(self, capture_log):
        assert parse_remote_config(_http_config(alias="我的 nas"), capture_log) == {}
        assert capture_log.has("warning", "alias: 格式不合法")

    def test_non_string_alias_rejected(self, capture_log):
        """非字符串判非法跳过，不做 str() 强转收编。"""
        assert parse_remote_config(_http_config(alias=123), capture_log) == {}
        assert capture_log.has("warning", "alias: 应为字符串")

    def test_negative_timeout_rejected(self, capture_log):
        """负 timeout 判非法，条目跳过。"""
        assert parse_remote_config(_http_config(timeout=-1), capture_log) == {}
        assert capture_log.has("warning", "timeout: 不能小于 0")

    def test_non_dict_entry_skips_without_crash(self, capture_log):
        """非 dict 条目不崩溃，钉死为跳过 + 警告。"""
        assert parse_remote_config({"http_remotes": ["oops"]}, capture_log) == {}
        assert capture_log.has("warning", "http_remotes 第 1 个条目无效")

    def test_non_list_remotes_ignored(self, capture_log):
        assert parse_remote_config({"http_remotes": "nas"}, capture_log) == {}
        assert capture_log.has("warning", "http_remotes 配置应为条目数组")

    def test_duplicate_alias_first_wins(self, capture_log):
        config = {
            "http_remotes": [
                {"alias": "nas", "address": "http://first", "key": "k1"},
                {"alias": "nas", "address": "http://second", "key": "k2"},
            ]
        }

        remotes = parse_remote_config(config, capture_log)

        assert remotes["nas"].address == "http://first"
        assert capture_log.has("warning", "'nas' 重复")

    def test_warning_never_contains_key_value(self, capture_log):
        """条目非法时的日志不得泄露 api_key 原值。"""
        config = _http_config(key="TOP-SECRET")
        del config["http_remotes"][0]["address"]

        parse_remote_config(config, capture_log)

        assert capture_log.has("warning", "http_remotes")
        assert not capture_log.has("warning", "TOP-SECRET")


class TestParseSshRemoteConfig:
    def test_defaults(self, capture_log):
        remotes = parse_ssh_remote_config(_ssh_config(), capture_log)

        remote = remotes["vps"]
        assert remote.host == "192.168.1.100"
        assert remote.user == "root"
        assert remote.port == 22
        assert remote.key_file is None
        assert remote.password_file is None

    def test_full_entry_strips_whitespace(self, capture_log):
        remotes = parse_ssh_remote_config(
            _ssh_config(port=2222, key_file=" /k ", password_file=" /p "), capture_log
        )

        remote = remotes["vps"]
        assert remote.port == 2222
        assert remote.key_file == "/k"
        assert remote.password_file == "/p"

    def test_empty_credential_path_becomes_none(self, capture_log):
        remotes = parse_ssh_remote_config(_ssh_config(key_file="   "), capture_log)

        assert remotes["vps"].key_file is None

    def test_alias_whitespace_stripped(self, capture_log):
        remotes = parse_ssh_remote_config(_ssh_config(alias=" vps "), capture_log)

        assert set(remotes) == {"vps"}

    def test_non_string_alias_rejected(self, capture_log):
        assert parse_ssh_remote_config(_ssh_config(alias=123), capture_log) == {}
        assert capture_log.has("warning", "alias: 应为字符串")

    def test_port_out_of_range_rejected(self, capture_log):
        assert parse_ssh_remote_config(_ssh_config(port=70000), capture_log) == {}
        assert capture_log.has("warning", "port: 不能大于 65535")

    def test_missing_required_field_skips_with_warning(self, capture_log):
        config = _ssh_config()
        del config["ssh_remotes"][0]["host"]

        assert parse_ssh_remote_config(config, capture_log) == {}
        assert capture_log.has("warning", "host: 缺少必填字段")

    def test_duplicate_alias_first_wins(self, capture_log):
        config = {
            "ssh_remotes": [
                {"alias": "vps", "host": "first", "user": "root"},
                {"alias": "vps", "host": "second", "user": "root"},
            ]
        }

        remotes = parse_ssh_remote_config(config, capture_log)

        assert remotes["vps"].host == "first"
        assert capture_log.has("warning", "'vps' 重复")

    def test_non_dict_entry_skips_without_crash(self, capture_log):
        assert parse_ssh_remote_config({"ssh_remotes": [42]}, capture_log) == {}
        assert capture_log.has("warning", "ssh_remotes 第 1 个条目无效")


class TestGlobalConfigServerFields:
    """GlobalConfig 的服务器数值段（加载期收口）。"""

    def test_defaults(self):
        config = GlobalConfig.model_validate({})

        assert config.api_key == ""
        assert config.port == 13579
        assert config.max_concurrent_metadata == 1
        assert config.max_concurrent_uploads == 1
        assert config.max_queue_metadata == 32
        assert config.max_queue_uploads == 32
        assert config.schedule is None

    def test_numeric_strings_coerced(self):
        """数值字符串经 lax 强转（与远端条目数值字段同一规则）。"""
        assert GlobalConfig.model_validate({"port": "23456"}).port == 23456

    def test_unknown_keys_ignored(self):
        """全局段只消费自己的配置键，其余（如 tasks、http_remotes）忽略，
        由各自的解析器负责。"""
        config = GlobalConfig.model_validate(
            {"tasks": [{"mode": "move"}], "http_remotes": [], "api_key": "k"}
        )

        assert config.api_key == "k"

    def test_port_out_of_range_rejected(self):
        with pytest.raises(ValidationError):
            GlobalConfig.model_validate({"port": 99999})

    def test_zero_concurrency_rejected(self):
        with pytest.raises(ValidationError):
            GlobalConfig.model_validate({"max_concurrent_uploads": 0})

    def test_negative_queue_rejected(self):
        with pytest.raises(ValidationError):
            GlobalConfig.model_validate({"max_queue_metadata": -1})

    def test_schedule_block_validated(self):
        config = GlobalConfig.model_validate(
            {"schedule": {"mode": "cron", "cron_expr": "0 * * * *"}}
        )

        assert config.schedule is not None
        assert config.schedule.cron_expr == "0 * * * *"

    def test_invalid_schedule_block_rejected(self):
        """[schedule] 非法 → 全局段校验失败（加载期致命，热重载回退备份）。"""
        with pytest.raises(ValidationError, match="不支持的定时模式"):
            GlobalConfig.model_validate({"schedule": {"mode": "weekly"}})
