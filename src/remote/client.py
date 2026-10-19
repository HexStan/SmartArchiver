import json
import os
import time
import urllib.parse

import requests
import urllib3.exceptions

from src import fs_ops
from src.fs_ops import TransferAbortedError
from src.remote.protocol import API_PATH, UPLOAD_PATH, RemoteAction

# 退避重试：2s + 4s + 8s + 16s = 30s 总等待时间
_RETRY_BACKOFFS = (2, 4, 8, 16)
_RETRY_TOTAL_WAIT = sum(_RETRY_BACKOFFS)

_RETRYABLE_EXCEPTIONS = (
    requests.ConnectionError,
    requests.Timeout,
    requests.exceptions.ChunkedEncodingError,
    urllib3.exceptions.ProtocolError,
)


class RemoteClientError(Exception):
    pass


class _AbortingReader:
    """只读文件的包装器：每次 read 的边界响应 fast 停止指令。

    保持文件对象形态（提供 len 属性），使 requests/urllib3 仍以
    Content-Length 身份编码流式发送，与直接传入文件对象的旧路径一致；
    若传入无长度的生成器，urllib3 会退回 chunked 传输编码，与显式
    Content-Length 头冲突。
    """

    def __init__(self, fileobj, size, local_path, remote_path):
        self._fileobj = fileobj
        self.len = size  # 供 urllib3 super_len 计算 Content-Length
        self._local_path = local_path
        self._remote_path = remote_path

    def read(self, size=-1):
        if fs_ops.abort_requested():
            raise TransferAbortedError(f"上传被中止: {self._local_path} -> {self._remote_path}")
        return self._fileobj.read(size)


class RemoteClient:
    _DEFAULT_TIMEOUT = 14400

    def __init__(self, address, api_key, alias="", logger=None, timeout=None, queue_time=0):
        self.address = address.rstrip("/")
        self.api_key = api_key
        self.alias = alias
        self.logger = logger
        self.timeout = timeout if timeout is not None else self._DEFAULT_TIMEOUT
        self.queue_time = float(queue_time) if queue_time is not None else 0.0
        self._endpoint = f"{self.address}{API_PATH}"
        self._upload_endpoint = f"{self.address}{UPLOAD_PATH}"
        self._session = requests.Session()

    def _log(self, level, msg):
        if self.logger:
            getattr(self.logger, level)(f"[remote:{self.alias}] {msg}")

    def _retry_request(self, request_fn, description):
        """带指数退避重试的请求包装。

        request_fn: 无参可调用对象，返回 requests.Response
        description: 用于日志的描述字符串
        """
        last_error = None
        for attempt, wait in enumerate(_RETRY_BACKOFFS, start=1):
            try:
                return request_fn()
            except _RETRYABLE_EXCEPTIONS as e:
                last_error = e
                self._log(
                    "warning",
                    f"{description}失败 "
                    f"(第 {attempt}/{len(_RETRY_BACKOFFS)} 次重试): {e}，"
                    f"{wait}s 后重试",
                )
                time.sleep(wait)
            except Exception:
                # 非网络异常（如 HTTPError、JSONDecodeError）不重试
                raise

        self._log("error", f"{description}重试耗尽 ({_RETRY_TOTAL_WAIT}s)，记入失败")
        raise last_error

    def _json_request(self, body):
        """发送 application/json 请求，返回解析后的 JSON 响应。"""

        def _do():
            resp = self._session.post(
                self._endpoint,
                json=body,
                timeout=self.timeout,
                headers={"X-Queue-Timeout": str(self.queue_time)},
            )
            resp.raise_for_status()
            return resp.json()

        try:
            return self._retry_request(_do, "API 请求")
        except requests.HTTPError as e:
            if e.response is not None:
                status_code = e.response.status_code
                body_raw = e.response.text or ""
                if status_code == 503:
                    raise RemoteClientError(f"Server {self.alias} busy: {body_raw}")
                raise RemoteClientError(f"HTTP {status_code} from {self.alias}: {body_raw}")
            raise RemoteClientError(f"HTTP error from {self.alias}: {e}")
        except json.JSONDecodeError:
            raise RemoteClientError(f"Invalid JSON response from {self.alias}")
        except _RETRYABLE_EXCEPTIONS as e:
            raise RemoteClientError(f"Connection error to {self.alias} ({self.address}): {e}")

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

    def delete_file(self, path):
        self._action(RemoteAction.DELETE_FILE, path=path)

    def delete_dir(self, path):
        self._action(RemoteAction.DELETE_DIR, path=path)

    def upload(self, local_path, remote_path):
        """上传文件到远程实例。若网络中断则从头重传。"""
        file_stat = os.stat(local_path)
        file_size = file_stat.st_size
        mtime = file_stat.st_mtime
        encoded_path = urllib.parse.quote(remote_path, safe="/")

        def _do():
            # 每次重试重新打开文件，确保从头部开始传输
            with open(local_path, "rb") as f:
                body = _AbortingReader(f, file_size, local_path, remote_path)
                resp = self._session.post(
                    self._upload_endpoint,
                    data=body,
                    headers={
                        "X-Api-Key": self.api_key,
                        "X-Path": encoded_path,
                        "X-Mtime": str(mtime),
                        "X-Queue-Timeout": str(self.queue_time),
                        "Content-Type": "application/octet-stream",
                        "Content-Length": str(file_size),
                    },
                    timeout=self.timeout,
                )
            resp.raise_for_status()
            return resp.json()

        try:
            return self._retry_request(_do, "文件上传")
        except TransferAbortedError:
            raise
        except requests.HTTPError as e:
            if e.response is not None:
                status_code = e.response.status_code
                body_raw = e.response.text or ""
                if status_code == 503:
                    raise RemoteClientError(f"Server {self.alias} busy: {body_raw}")
                raise RemoteClientError(f"HTTP {status_code} from {self.alias}: {body_raw}")
            raise RemoteClientError(f"HTTP error from {self.alias}: {e}")
        except json.JSONDecodeError:
            raise RemoteClientError(f"Invalid JSON response from {self.alias}")
        except _RETRYABLE_EXCEPTIONS as e:
            if fs_ops.abort_requested():
                raise TransferAbortedError(f"上传被中止: {local_path} -> {remote_path}")
            raise RemoteClientError(f"Connection error to {self.alias} ({self.address}): {e}")
        except Exception:
            # 中止异常可能在流式传输过程中被 requests/urllib3 包装，
            # 只要中止信号在位，一律还原为中止语义而非连接错误
            if fs_ops.abort_requested():
                raise TransferAbortedError(f"上传被中止: {local_path} -> {remote_path}")
            raise

    def list_dir(self, path):
        result = self._action(RemoteAction.LIST_DIR, path=path)
        return result.get("entries", [])

    def stat(self, path):
        result = self._action(RemoteAction.STAT, path=path)
        return {
            "exists": result.get("exists", False),
            "size": result.get("size", 0),
            "mtime": result.get("mtime", 0),
            "is_dir": result.get("is_dir", False),
        }
