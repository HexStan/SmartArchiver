import pytest
from src.app_context import AppContext


class MockLogger:
    def __init__(self):
        self.messages = []

    def error(self, msg):
        self.messages.append(("error", msg))

    def warning(self, msg):
        self.messages.append(("warning", msg))

    def debug(self, msg):
        self.messages.append(("debug", msg))

    def info(self, msg):
        self.messages.append(("info", msg))

    def success(self, msg):
        self.messages.append(("success", msg))

    def critical(self, msg):
        self.messages.append(("critical", msg))


class MockHistoryManager:
    def __init__(self, max_retries=3):
        self.max_retries = max_retries
        self._failures = {}
        self._successes = {}

    def should_skip(self, path, max_retries):
        count = self._failures.get(str(path), 0)
        return count >= max_retries, count

    def record_failure(self, path):
        path = str(path)
        self._failures[path] = self._failures.get(path, 0) + 1
        return self._failures[path]

    def record_success(self, path):
        path = str(path)
        if path in self._failures:
            del self._failures[path]
        self._successes[path] = True

    def save(self):
        pass


@pytest.fixture
def mock_logger():
    return MockLogger()


@pytest.fixture
def mock_history():
    return MockHistoryManager()


@pytest.fixture
def app_context(mock_logger, mock_history):
    config = {"max_retries": 3}
    AppContext.init(mock_logger, mock_history, config)
    yield AppContext.get()
    AppContext._instance = None
