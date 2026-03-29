import os
import sys
import pytest
import tempfile
import shutil

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils import parse_size_string, load_config, SingleInstance


class TestParseSizeString:
    def test_empty_string(self):
        assert parse_size_string("") == 0

    def test_none(self):
        assert parse_size_string(None) == 0

    def test_all_uppercase(self):
        assert parse_size_string("ALL") == float("inf")

    def test_all_lowercase(self):
        assert parse_size_string("all") == float("inf")

    def test_mixed_case(self):
        assert parse_size_string("All") == float("inf")

    def test_bytes(self):
        assert parse_size_string("1024B") == 1024

    def test_kilobytes(self):
        assert parse_size_string("1KB") == 1024

    def test_megabytes(self):
        assert parse_size_string("1MB") == 1024 * 1024

    def test_gigabytes(self):
        assert parse_size_string("1GB") == 1024 * 1024 * 1024

    def test_with_space(self):
        assert parse_size_string("10 MB") == 10 * 1024 * 1024

    def test_decimal(self):
        result = parse_size_string("1.5GB")
        expected = int(1.5 * 1024 * 1024 * 1024)
        assert result == expected

    def test_invalid_string(self):
        assert parse_size_string("invalid") == 0

    def test_whitespace_only(self):
        assert parse_size_string("   ") == 0


class TestLoadConfig:
    def test_load_valid_config(self, tmp_path):
        config_file = tmp_path / "test_config.toml"
        config_file.write_text("""
[global]
max_retries = 3

[[tasks]]
source = "/source"
dest = "/dest"
mode = "move"
""")
        config = load_config(str(config_file))
        assert config["global"]["max_retries"] == 3
        assert len(config["tasks"]) == 1

    def test_load_nonexistent_config(self):
        with pytest.raises(FileNotFoundError):
            load_config("/nonexistent/path/config.toml")


class TestSingleInstance:
    def test_windows_no_lock(self, tmp_path, monkeypatch):
        monkeypatch.setattr(os, "name", "nt")
        lock_file = tmp_path / "test.lock"
        logger = type(
            "MockLogger",
            (),
            {"info": lambda self, x: None, "error": lambda self, x: None},
        )()

        with SingleInstance(str(lock_file), logger):
            pass

    def test_unix_lock_release(self, tmp_path, monkeypatch):
        if os.name == "nt":
            pytest.skip("Unix-only test")

        monkeypatch.setattr(os, "name", "posix")
        lock_file = tmp_path / "test.lock"
        logger = type(
            "MockLogger",
            (),
            {"info": lambda self, x: None, "error": lambda self, x: None},
        )()

        with SingleInstance(str(lock_file), logger):
            pass

        assert lock_file.exists()
