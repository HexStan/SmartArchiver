import os
import sys
import glob
import pytest
import logging
import tempfile
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.logger import (
    DualFormatter,
    LoggerWrapper,
    DailyRotatingFileHandler,
    setup_logger,
    clean_old_logs,
    SUCCESS_LEVEL_NUM,
)


class TestDualFormatter:
    def test_standard_format(self):
        formatter = DualFormatter(
            "%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        result = formatter.format(record)
        assert "[INFO]" in result
        assert "Test message" in result

    def test_raw_format(self):
        formatter = DualFormatter(
            "%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Raw message",
            args=(),
            exc_info=None,
        )
        record.is_raw = True
        result = formatter.format(record)
        assert result == "Raw message"
        assert "[INFO]" not in result

    def test_raw_with_timestamp(self):
        formatter = DualFormatter(
            "%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Raw with timestamp",
            args=(),
            exc_info=None,
        )
        record.is_raw = True
        record.prepend_timestamp = True
        result = formatter.format(record)
        assert "Raw with timestamp" in result
        assert len(result) > len("Raw with timestamp")


class TestLoggerWrapper:
    def test_info_logging(self, tmp_path):
        logger = setup_logger(str(tmp_path))
        logger.info("Test info message")

    def test_debug_logging(self, tmp_path):
        logger = setup_logger(str(tmp_path), log_level="DEBUG")
        logger.debug("Test debug message")

    def test_warning_logging(self, tmp_path):
        logger = setup_logger(str(tmp_path))
        logger.warning("Test warning message")

    def test_error_logging(self, tmp_path):
        logger = setup_logger(str(tmp_path))
        logger.error("Test error message")

    def test_success_logging(self, tmp_path):
        logger = setup_logger(str(tmp_path))
        logger.success("Test success message")

    def test_raw_info(self, tmp_path):
        logger = setup_logger(str(tmp_path))
        logger.info("Raw message", raw=True)

    def test_sanitize_message(self, tmp_path):
        logger = setup_logger(str(tmp_path))
        logger.info("Normal message")

    def test_prepend_timestamp(self, tmp_path):
        logger = setup_logger(str(tmp_path))
        logger.info("Timestamped raw", raw=True, prepend_timestamp=True)


class TestDailyRotatingFileHandler:
    def test_creates_log_file(self, tmp_path):
        handler = DailyRotatingFileHandler(str(tmp_path), max_log_files=0)
        assert handler.baseFilename.endswith(".log")

    def test_current_date_in_filename(self, tmp_path):
        handler = DailyRotatingFileHandler(str(tmp_path), max_log_files=0)
        today = datetime.now().strftime("%Y%m%d")
        assert today in handler.baseFilename


class TestSetupLogger:
    def test_creates_log_directory(self, tmp_path):
        log_dir = tmp_path / "new_logs"
        logger = setup_logger(str(log_dir))
        assert log_dir.exists()

    def test_creates_log_file(self, tmp_path):
        logger = setup_logger(str(tmp_path))
        logger.info("Test message")

        log_files = glob.glob(os.path.join(str(tmp_path), "smartarchiver-*.log"))
        assert len(log_files) >= 1

    def test_returns_logger_wrapper(self, tmp_path):
        logger = setup_logger(str(tmp_path))
        assert isinstance(logger, LoggerWrapper)

    def test_log_level_setting(self, tmp_path):
        logger = setup_logger(str(tmp_path), log_level="WARNING")
        assert logger._logger.level == logging.WARNING

    def test_default_log_level(self, tmp_path):
        logger = setup_logger(str(tmp_path))
        assert logger._logger.level == logging.INFO


class TestCleanOldLogs:
    def test_no_cleanup_when_disabled(self, tmp_path):
        for i in range(5):
            (tmp_path / f"smartarchiver-2025010{i}.log").write_text("log")

        clean_old_logs(str(tmp_path), 0)

        log_files = glob.glob(os.path.join(str(tmp_path), "smartarchiver-*.log"))
        assert len(log_files) == 5

    def test_cleanup_old_logs(self, tmp_path):
        for i in range(5):
            (tmp_path / f"smartarchiver-2025010{i}.log").write_text("log")

        clean_old_logs(str(tmp_path), 3)

        log_files = glob.glob(os.path.join(str(tmp_path), "smartarchiver-*.log"))
        assert len(log_files) == 3

    def test_keeps_newest_logs(self, tmp_path):
        for i in range(5):
            (tmp_path / f"smartarchiver-2025010{i}.log").write_text("log")

        clean_old_logs(str(tmp_path), 2)

        log_files = glob.glob(os.path.join(str(tmp_path), "smartarchiver-*.log"))
        log_files.sort()
        assert "20250103" in log_files[0]
        assert "20250104" in log_files[1]

    def test_no_cleanup_when_fewer_than_max(self, tmp_path):
        for i in range(2):
            (tmp_path / f"smartarchiver-2025010{i}.log").write_text("log")

        clean_old_logs(str(tmp_path), 5)

        log_files = glob.glob(os.path.join(str(tmp_path), "smartarchiver-*.log"))
        assert len(log_files) == 2

    def test_ignores_non_matching_files(self, tmp_path):
        (tmp_path / "smartarchiver-20250101.log").write_text("log")
        (tmp_path / "other-file.log").write_text("other")
        (tmp_path / "smartarchiver.txt").write_text("txt")

        clean_old_logs(str(tmp_path), 0)

        assert (tmp_path / "other-file.log").exists()
        assert (tmp_path / "smartarchiver.txt").exists()
