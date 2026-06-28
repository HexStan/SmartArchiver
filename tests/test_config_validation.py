from src.core.handlers.base import validate_task_config


class TestSyncModeValidation:
    def test_valid_sync_minimal(self, app_context):
        assert validate_task_config(
            {"mode": "sync", "source": "/tmp/test", "dest": "/tmp/dest"}, "sync"
        )

    def test_valid_sync_with_tool_auto(self, app_context):
        assert validate_task_config(
            {"mode": "sync", "source": "/tmp/test", "dest": "/tmp/dest", "tool": "auto"}, "sync"
        )

    def test_valid_sync_with_tool_rsync(self, app_context):
        assert validate_task_config(
            {"mode": "sync", "source": "/tmp/test", "dest": "/tmp/dest", "tool": "rsync"}, "sync"
        )

    def test_valid_sync_with_tool_rclone(self, app_context):
        assert validate_task_config(
            {"mode": "sync", "source": "/tmp/test", "dest": "/tmp/dest", "tool": "rclone"}, "sync"
        )

    def test_invalid_tool(self, app_context, mock_logger):
        result = validate_task_config(
            {"mode": "sync", "source": "/tmp/test", "tool": "scp"}, "sync"
        )
        assert not result
        assert any("tool" in msg for _, msg in mock_logger.messages)

    def test_tool_case_insensitive(self, app_context):
        assert validate_task_config(
            {"mode": "sync", "source": "/tmp/test", "dest": "/tmp/dest", "tool": "RSYNC"}, "sync"
        )

    def test_missing_dest(self, app_context, mock_logger):
        result = validate_task_config({"mode": "sync", "source": "/tmp/test", "tool": "rsync"}, "sync")
        assert not result
        assert any("dest" in msg for _, msg in mock_logger.messages)

    def test_missing_source(self, app_context, mock_logger):
        result = validate_task_config({"mode": "sync", "dest": "/tmp/dest", "tool": "rsync"}, "sync")
        assert not result
        assert any("source" in msg for _, msg in mock_logger.messages)


class TestRotateModeValidation:
    def test_valid_rotate_with_size_limit(self, app_context):
        task = {
            "mode": "rotate",
            "source": "/tmp/test",
            "remove_empty_dirs": True,
            "size_limit": "1GB",
        }
        assert validate_task_config(task, "rotate")

    def test_valid_rotate_with_count_limit(self, app_context):
        task = {
            "mode": "rotate",
            "source": "/tmp/test",
            "remove_empty_dirs": False,
            "count_limit": 10,
        }
        assert validate_task_config(task, "rotate")

    def test_valid_rotate_with_rotate_rules_size(self, app_context):
        task = {
            "mode": "rotate",
            "source": "/tmp/test",
            "remove_empty_dirs": True,
            "rotate_rules": {"size": {"*.log": "100MB"}},
        }
        assert validate_task_config(task, "rotate")

    def test_valid_rotate_with_rotate_rules_count(self, app_context):
        task = {
            "mode": "rotate",
            "source": "/tmp/test",
            "remove_empty_dirs": True,
            "rotate_rules": {"count": {"*.tmp": 5}},
        }
        assert validate_task_config(task, "rotate")

    def test_rotate_no_limits(self, app_context, mock_logger):
        task = {
            "mode": "rotate",
            "source": "/tmp/test",
            "remove_empty_dirs": True,
            "size_limit": "0",
            "count_limit": 0,
        }
        result = validate_task_config(task, "rotate")
        assert not result
        assert any("至少一项" in msg for _, msg in mock_logger.messages)

    def test_rotate_missing_remove_empty_dirs(self, app_context, mock_logger):
        task = {
            "mode": "rotate",
            "source": "/tmp/test",
            "size_limit": "1GB",
        }
        result = validate_task_config(task, "rotate")
        assert not result
        assert any("remove_empty_dirs" in msg for _, msg in mock_logger.messages)

    def test_rotate_with_all_limits(self, app_context):
        task = {
            "mode": "rotate",
            "source": "/tmp/test",
            "remove_empty_dirs": True,
            "size_limit": "1GB",
            "count_limit": 20,
            "rotate_rules": {"size": {"*.log": "100MB"}, "count": {"*.tmp": 5}},
        }
        assert validate_task_config(task, "rotate")

    def test_rotate_missing_source(self, app_context, mock_logger):
        task = {
            "mode": "rotate",
            "remove_empty_dirs": True,
            "size_limit": "1GB",
        }
        result = validate_task_config(task, "rotate")
        assert not result
        assert any("source" in msg for _, msg in mock_logger.messages)


class TestStandardModeValidation:
    def test_valid_move(self, app_context):
        task = {
            "mode": "move",
            "source": "/tmp/test",
            "mtime_threshold_minutes": 180,
            "conflict_policy": "overwrite",
            "remove_empty_dirs": True,
        }
        assert validate_task_config(task, "move")

    def test_valid_copy(self, app_context):
        task = {
            "mode": "copy",
            "source": "/tmp/test",
            "mtime_threshold_minutes": 60,
            "conflict_policy": "skip",
            "remove_empty_dirs": False,
        }
        assert validate_task_config(task, "copy")

    def test_missing_mtime_threshold(self, app_context, mock_logger):
        task = {
            "mode": "move",
            "source": "/tmp/test",
            "conflict_policy": "overwrite",
            "remove_empty_dirs": True,
        }
        result = validate_task_config(task, "move")
        assert not result
        assert any("mtime_threshold_minutes" in msg for _, msg in mock_logger.messages)

    def test_missing_conflict_policy(self, app_context, mock_logger):
        task = {
            "mode": "move",
            "source": "/tmp/test",
            "mtime_threshold_minutes": 180,
            "remove_empty_dirs": True,
        }
        result = validate_task_config(task, "move")
        assert not result
        assert any("conflict_policy" in msg for _, msg in mock_logger.messages)

    def test_multiple_missing_fields(self, app_context, mock_logger):
        task = {"mode": "move"}
        result = validate_task_config(task, "move")
        assert not result
        messages = " ".join(msg for _, msg in mock_logger.messages)
        assert "source" in messages
        assert "mtime_threshold_minutes" in messages
        assert "conflict_policy" in messages
        assert "remove_empty_dirs" in messages

    def test_missing_source(self, app_context, mock_logger):
        task = {
            "mode": "move",
            "mtime_threshold_minutes": 180,
            "conflict_policy": "overwrite",
            "remove_empty_dirs": True,
        }
        result = validate_task_config(task, "move")
        assert not result
        assert any("source" in msg for _, msg in mock_logger.messages)


class TestWhitelistModeValidation:
    def test_valid_whitelist_move(self, app_context):
        task = {
            "mode": "whitelist_move",
            "source": "/tmp/test",
            "mtime_threshold_minutes": 180,
            "conflict_policy": "overwrite",
            "remove_empty_dirs": True,
            "whitelist_rules": {"lt": {"*.doc": "10MB"}},
        }
        assert validate_task_config(task, "whitelist_move")

    def test_valid_whitelist_copy(self, app_context):
        task = {
            "mode": "whitelist_copy",
            "source": "/tmp/test",
            "mtime_threshold_minutes": 60,
            "conflict_policy": "skip",
            "remove_empty_dirs": False,
            "whitelist_rules": {"ge": {"*.pdf": "1MB"}},
        }
        assert validate_task_config(task, "whitelist_copy")

    def test_whitelist_missing_whitelist_rules(self, app_context, mock_logger):
        task = {
            "mode": "whitelist_move",
            "source": "/tmp/test",
            "mtime_threshold_minutes": 180,
            "conflict_policy": "overwrite",
            "remove_empty_dirs": True,
        }
        result = validate_task_config(task, "whitelist_move")
        assert not result
        assert any("whitelist_rules" in msg for _, msg in mock_logger.messages)

    def test_whitelist_empty_rules(self, app_context, mock_logger):
        task = {
            "mode": "whitelist_move",
            "source": "/tmp/test",
            "mtime_threshold_minutes": 180,
            "conflict_policy": "overwrite",
            "remove_empty_dirs": True,
            "whitelist_rules": {},
        }
        result = validate_task_config(task, "whitelist_move")
        assert not result
        assert any("whitelist_rules" in msg for _, msg in mock_logger.messages)

    def test_whitelist_missing_source(self, app_context, mock_logger):
        task = {
            "mode": "whitelist_move",
            "mtime_threshold_minutes": 180,
            "conflict_policy": "overwrite",
            "remove_empty_dirs": True,
            "whitelist_rules": {"lt": {"*.doc": "10MB"}},
        }
        result = validate_task_config(task, "whitelist_move")
        assert not result
        assert any("source" in msg for _, msg in mock_logger.messages)
