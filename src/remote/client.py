import json
import os
import time
import urllib.request
import urllib.error
import urllib.parse

from src.remote.protocol import RemoteAction, API_PATH, UPLOAD_PATH

_RETRY_DELAYS = [2, 4, 8, 16]  # 指数退避重试间隔（秒），总计 30 秒
_DEFAULT_TIMEOUT = 14400       # 默认 HTTP 超时（秒）


class RemoteClientError(Exception):
    pass


class RemoteClient:
    def __init__(self, address, api_key, alias="", logger=None, timeout=None):
        self.address = address.rstrip("/")
        self.api_key = api_key
        self.alias = alias
        self.logger = logger
        self._timeout = timeout if timeout is not None else _DEFAULT_TIMEOUT
        self._endpoint = f"{self.address}{API_PATH}"
        self._upload_endpoint = f"{self.address}{UPLOAD_PATH}"

    def _log(self, level, msg):
        if self.logger:
            getattr(self.logger, level)(f"[remote:{self.alias}] {msg}")

    def _open_request(self, req_builder):
        """发送 HTTP 请求，对网络异常进行指数退避重试。

        req_builder 为可调用对象，每次调用返回一个全新的 urllib.request.Request 对象。
        重试期间会重新调用 req_builder 以生成新的请求体（例如：重新打开文件句柄）。
        """
        last_error = None
        total_attempts = len(_RETRY_DELAYS) + 1
        for attempt, delay in enumerate(_RETRY_DELAYS):
            try:
                return urllib.request.urlopen(req_builder(), timeout=self._timeout)
            except urllib.error.URLError as e:
                last_error = e
                self._log(
                    "warning",
                    f"连接失败，{delay}秒后进行第{attempt+1}次重试"
                    f"（共{total_attempts}次）: {e.reason}",
                )
                time.sleep(delay)
        # 最后一次尝试，不 sleep
        try:
            return urllib.request.urlopen(req_builder(), timeout=self._timeout)
        except urllib.error.URLError as e:
            last_error = e
        raise RemoteClientError(
            f"Connection error to {self.alias} ({self.address}): {last_error.reason}"
        )

    def _json_request(self, body):
        """发送 application/json 请求，返回解析后的 JSON 响应。"""
        data = json.dumps(body).encode("utf-8")

        def _build_req():
            return urllib.request.Request(
                self._endpoint,
                data=data,
                method="POST",
                headers={"Content-Type": "application/json"},
            )

        try:
            with self._open_request(_build_req) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw)
        except urllib.error.HTTPError as e:
            body_raw = e.read().decode("utf-8", errors="replace")
            raise RemoteClientError(f"HTTP {e.code} from {self.alias}: {body_raw}")
        except json.JSONDecodeError:
            raise RemoteClientError(f"Invalid JSON response from {self.alias}")

    def _action(self, action, **extra_fields):
        body = {"api_key": self.api_key, "action": action.value}
        body.update({k: str(v) for k, v in extra_fields.items() if v is not None})
        return self._json_request(body)

    def exists(self, path):
        result = self._action(RemoteAction.EXISTS, path=path)
        return result.get("exists", False)

    def is_dir(self, path):
        result = self._action(RemoteAction.IS_DIR, path=path)
        return result.get("is_dir", False)

    def mkdir(self, path):
        self._action(RemoteAction.MKDIR, path=path)

    def delete(self, path):
        self._action(RemoteAction.DELETE, path=path)

    def upload(self, local_path, remote_path):
        file_size = os.path.getsize(local_path)
        encoded_path = urllib.parse.quote(remote_path, safe="/")

        def _build_req():
            return urllib.request.Request(
                self._upload_endpoint,
                data=open(local_path, "rb"),
                method="POST",
                headers={
                    "X-Api-Key": self.api_key,
                    "X-Path": encoded_path,
                    "Content-Type": "application/octet-stream",
                    "Content-Length": str(file_size),
                },
            )

        try:
            with self._open_request(_build_req) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw)
        except urllib.error.HTTPError as e:
            body_raw = e.read().decode("utf-8", errors="replace")
            raise RemoteClientError(f"HTTP {e.code} from {self.alias}: {body_raw}")
        except json.JSONDecodeError:
            raise RemoteClientError(f"Invalid JSON response from {self.alias}")

    def stat(self, path):
        result = self._action(RemoteAction.STAT, path=path)
        return {
            "exists": result.get("exists", False),
            "size": result.get("size", 0),
            "mtime": result.get("mtime", 0),
            "is_dir": result.get("is_dir", False),
        }
