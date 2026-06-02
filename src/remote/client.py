import json
import os
import urllib.request
import urllib.error
import uuid

from src.remote.protocol import RemoteAction, API_PATH


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

    @staticmethod
    def _build_fields_body(fields, boundary):
        """Build multipart body for field-only requests (no file). Small, stays in memory."""
        parts = []
        for name, value in fields.items():
            part = (
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="{name}"\r\n\r\n'
                f"{value}\r\n"
            )
            parts.append(part.encode("utf-8"))
        parts.append(f"--{boundary}--\r\n".encode("utf-8"))
        return b"".join(parts)

    @staticmethod
    def _iter_file_upload(fields, file_field, file_path, boundary, chunk_size=65536):  # 64KB
        """Generate multipart/form-data chunks for file upload, streaming from disk."""
        # field parts
        for name, value in fields.items():
            yield (
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="{name}"\r\n\r\n'
                f"{value}\r\n"
            ).encode("utf-8")
        # file header
        filename = os.path.basename(file_path)
        yield (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="{file_field}"; filename="{filename}"\r\n'
            f"Content-Type: application/octet-stream\r\n\r\n"
        ).encode("utf-8")
        # file content, streamed in chunks
        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk
        # closing boundary
        yield f"\r\n--{boundary}--\r\n".encode("utf-8")

    def _request(self, fields, file_field=None, file_path=None):
        boundary = uuid.uuid4().hex
        content_type = f"multipart/form-data; boundary={boundary}"

        if file_field and file_path:
            data = self._iter_file_upload(fields, file_field, file_path, boundary)
        else:
            data = self._build_fields_body(fields, boundary)

        req = urllib.request.Request(
            self._endpoint,
            data=data,
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

    def stat(self, path):
        result = self._action(RemoteAction.STAT, path=path)
        return {
            "exists": result.get("exists", False),
            "size": result.get("size", 0),
            "mtime": result.get("mtime", 0),
            "is_dir": result.get("is_dir", False),
        }
