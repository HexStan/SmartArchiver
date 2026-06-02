import json
import os
import tempfile
import urllib.request
import urllib.error
import uuid

from src.remote.protocol import RemoteAction, API_PATH

_STREAM_CHUNK_SIZE = 8 * 1024 * 1024
_SPOOL_MEMORY_LIMIT = 10 * 1024 * 1024


class RemoteClientError(Exception):
    pass


class RemoteClient:
    def __init__(self, address, api_key, alias="", logger=None):
        self.address = address.rstrip("/")
        self.api_key = api_key
        self.alias = alias
        self.logger = logger
        self._endpoint = f"{self.address}{API_PATH}"

    def _log(self, level, msg):
        if self.logger:
            getattr(self.logger, level)(f"[remote:{self.alias}] {msg}")

    def _build_multipart(self, fields, file_field=None, file_path=None):
        boundary = uuid.uuid4().hex
        body_file = tempfile.SpooledTemporaryFile(max_size=_SPOOL_MEMORY_LIMIT)

        for name, value in fields.items():
            body_file.write(
                (
                    f"--{boundary}\r\n"
                    f'Content-Disposition: form-data; name="{name}"\r\n\r\n'
                    f"{value}\r\n"
                ).encode("utf-8")
            )

        if file_field and file_path:
            filename = os.path.basename(file_path)
            body_file.write(
                (
                    f"--{boundary}\r\n"
                    f'Content-Disposition: form-data; name="{file_field}"; filename="{filename}"\r\n'
                    f"Content-Type: application/octet-stream\r\n\r\n"
                ).encode("utf-8")
            )

            with open(file_path, "rb") as f:
                while True:
                    chunk = f.read(_STREAM_CHUNK_SIZE)
                    if not chunk:
                        break
                    body_file.write(chunk)

            body_file.write(b"\r\n")

        body_file.write(f"--{boundary}--\r\n".encode("utf-8"))
        body_file.seek(0)

        content_type = f"multipart/form-data; boundary={boundary}"
        return body_file, content_type

    def _request(self, fields, file_field=None, file_path=None):
        body_file, content_type = self._build_multipart(fields, file_field, file_path)

        try:
            req = urllib.request.Request(
                self._endpoint,
                data=body_file,
                method="POST",
                headers={"Content-Type": content_type},
            )

            try:
                with urllib.request.urlopen(req, timeout=14400) as resp:
                    raw = resp.read().decode("utf-8")
                    return json.loads(raw)
            except urllib.error.HTTPError as e:
                body = e.read().decode("utf-8", errors="replace")
                raise RemoteClientError(f"HTTP {e.code} from {self.alias}: {body}")
            except urllib.error.URLError as e:
                raise RemoteClientError(
                    f"Connection error to {self.alias} ({self.address}): {e.reason}"
                )
            except json.JSONDecodeError:
                raise RemoteClientError(f"Invalid JSON response from {self.alias}")
        finally:
            body_file.close()

    def _action(self, action, **extra_fields):
        fields = {"api_key": self.api_key, "action": action.value}
        fields.update({k: str(v) for k, v in extra_fields.items() if v is not None})
        return self._request(fields)

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
        self._request(
            {
                "api_key": self.api_key,
                "action": RemoteAction.UPLOAD.value,
                "path": remote_path,
            },
            file_field="file",
            file_path=local_path,
        )

    def transfer_file(self, local_path, remote_path, on_exists):
        """单次请求完成存在性检查和文件上传。on_exists: overwrite | skip | rename | error"""
        result = self._request(
            {
                "api_key": self.api_key,
                "action": RemoteAction.TRANSFER.value,
                "path": remote_path,
                "on_exists": on_exists,
            },
            file_field="file",
            file_path=local_path,
        )
        return result.get("action", "error"), result.get("path", remote_path)

    def stat(self, path):
        result = self._action(RemoteAction.STAT, path=path)
        return {
            "exists": result.get("exists", False),
            "size": result.get("size", 0),
            "mtime": result.get("mtime", 0),
            "is_dir": result.get("is_dir", False),
        }
