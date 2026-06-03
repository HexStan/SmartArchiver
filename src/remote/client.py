import json
import os
import urllib.request
import urllib.error

from src.remote.protocol import RemoteAction, API_PATH, UPLOAD_PATH


class RemoteClientError(Exception):
    pass


class RemoteClient:
    def __init__(self, address, api_key, alias="", logger=None):
        self.address = address.rstrip("/")
        self.api_key = api_key
        self.alias = alias
        self.logger = logger
        self._endpoint = f"{self.address}{API_PATH}"
        self._upload_endpoint = f"{self.address}{UPLOAD_PATH}"

    def _log(self, level, msg):
        if self.logger:
            getattr(self.logger, level)(f"[remote:{self.alias}] {msg}")

    def _json_request(self, body):
        """发送 application/json 请求，返回解析后的 JSON 响应。"""
        data = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(
            self._endpoint,
            data=data,
            method="POST",
            headers={"Content-Type": "application/json"},
        )

        try:
            with urllib.request.urlopen(req, timeout=14400) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw)
        except urllib.error.HTTPError as e:
            body_raw = e.read().decode("utf-8", errors="replace")
            raise RemoteClientError(f"HTTP {e.code} from {self.alias}: {body_raw}")
        except urllib.error.URLError as e:
            raise RemoteClientError(
                f"Connection error to {self.alias} ({self.address}): {e.reason}"
            )
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
        req = urllib.request.Request(
            self._upload_endpoint,
            data=open(local_path, "rb"),
            method="POST",
            headers={
                "X-Api-Key": self.api_key,
                "X-Path": remote_path,
                "Content-Type": "application/octet-stream",
                "Content-Length": str(file_size),
            },
        )

        try:
            with urllib.request.urlopen(req, timeout=14400) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw)
        except urllib.error.HTTPError as e:
            body_raw = e.read().decode("utf-8", errors="replace")
            raise RemoteClientError(f"HTTP {e.code} from {self.alias}: {body_raw}")
        except urllib.error.URLError as e:
            raise RemoteClientError(
                f"Connection error to {self.alias} ({self.address}): {e.reason}"
            )
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
