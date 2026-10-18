import os
import urllib.parse

from flask import Flask, jsonify, request

from src import fs_ops
from src.remote.concurrency import (
    ClientDisconnectedError,
    ConcurrencyGate,
    QueueFullError,
    QueueTimeoutError,
)
from src.remote.protocol import UPLOAD_PATH, RemoteAction


def _parse_queue_timeout():
    """从请求头解析客户端允许的最大排队秒数。"""
    raw = request.headers.get("X-Queue-Timeout", "0")
    try:
        value = float(raw)
        if value < 0:
            return 0.0
        return value
    except (TypeError, ValueError):
        return 0.0


def _make_disconnect_checker():
    """构造当前请求上下文的断连检测函数（best-effort）。

    返回一个无参可调用对象：返回 True 表示客户端可能已断连。
    """
    wsgi_input = request.environ.get("wsgi.input")

    def _check():
        try:
            if wsgi_input is not None:
                wsgi_input.read(0)
            return False
        except Exception:  # noqa: BLE001
            return True

    return _check


class _UploadSizeMismatch(ValueError):
    """接收到的字节数与 Content-Length 不一致。"""


def _parse_mtime():
    """解析可选的 X-Mtime 头（源文件 mtime，Unix 秒）。

    头缺失时返回 None；格式非法时抛 ValueError，由调用方返回 400。
    """
    raw = request.headers.get("X-Mtime")
    if not raw:
        return None
    return float(raw)


def _receive_upload(path, mtime):
    """把请求体原子写入 path：临时文件 + 字节数校验 + fsync + os.replace。

    任何失败（中断、字节数不符）都不会触碰最终路径，临时文件被清理。
    """
    expected_size = request.content_length

    def _finalize(temp_path):
        if mtime is not None:
            os.utime(temp_path, (mtime, mtime))
        if os.name == "posix":
            # mkstemp 回退路径默认 0600，还原 open(path, "wb") 的常规权限
            os.chmod(temp_path, 0o644)

    with fs_ops.atomic_writer(path, _finalize) as f:
        received = 0
        chunk_size = 65536  # 64KB
        while True:
            chunk = request.stream.read(chunk_size)
            if not chunk:
                break
            received += len(chunk)
            f.write(chunk)

        if expected_size is not None and received != expected_size:
            raise _UploadSizeMismatch(
                f"接收字节数与 Content-Length 不一致: "
                f"期望 {expected_size}，实际 {received}"
            )


def create_app(api_key, logger, gate):
    """创建 Flask 应用并注册路由。

    Args:
        api_key: API 鉴权密钥。
        logger: 日志记录器。
        gate: ConcurrencyGate 实例，用于并发控制。
    """
    app = Flask(__name__)

    @app.before_request
    def _log_request():
        client_ip = request.remote_addr
        logger.info(f"收到来自 {client_ip} 的连接请求")

    @app.route("/api/remote", methods=["POST"])
    def handle_remote():
        client_ip = request.remote_addr
        body = request.get_json(silent=True)

        if not body or not isinstance(body, dict):
            return jsonify({"error": "invalid JSON body"}), 400

        req_api_key = body.get("api_key", "")
        if req_api_key != api_key:
            logger.warning(f"来自 {client_ip} 的请求鉴权失败")
            return jsonify({"error": "unauthorized"}), 403

        action = body.get("action", "")
        if not RemoteAction.is_valid(action):
            logger.warning(f"来自 {client_ip} 的请求使用了未知操作: {action}")
            return jsonify({"error": f"unknown action: {action}"}), 400

        path = body.get("path", "")
        if not path:
            return jsonify({"error": "missing path"}), 400

        path = os.path.normpath(path)

        queue_timeout = _parse_queue_timeout()
        disconnect_checker = _make_disconnect_checker()

        try:
            with gate.metadata_context(client_ip, queue_timeout, disconnect_checker):
                # ---- 以下为原有处理逻辑（在获取并发槽位后执行） ----
                logger.info(f"来自 {client_ip} 的指令: {action} -> {path}")

                try:
                    if action == RemoteAction.EXISTS:
                        return jsonify({"exists": fs_ops.path_exists(path)})

                    elif action == RemoteAction.IS_DIR:
                        return jsonify({"is_dir": fs_ops.is_dir(path)})

                    elif action == RemoteAction.MKDIR:
                        fs_ops.ensure_dir(path)
                        logger.success(f"创建目录: {path}")
                        return jsonify({"success": True})

                    elif action == RemoteAction.DELETE_FILE:
                        fs_ops.delete_file(path)
                        logger.success(f"删除文件: {path}")
                        return jsonify({"success": True})

                    elif action == RemoteAction.DELETE_DIR:
                        fs_ops.delete_dir(path)
                        logger.success(f"删除目录: {path}")
                        return jsonify({"success": True})

                    elif action == RemoteAction.STAT:
                        st = fs_ops.get_stat(path)
                        if not st.exists:
                            return jsonify(
                                {
                                    "exists": False,
                                    "size": 0,
                                    "mtime": 0,
                                    "is_dir": False,
                                }
                            )
                        return jsonify(
                            {
                                "exists": True,
                                "size": st.size,
                                "mtime": st.mtime,
                                "is_dir": st.is_dir,
                            }
                        )

                    elif action == RemoteAction.LIST_DIR:
                        entries = fs_ops.list_dir(path)
                        return jsonify(
                            {
                                "entries": [
                                    {
                                        "name": e.name,
                                        "is_dir": e.is_dir,
                                        "size": e.size,
                                        "mtime": e.mtime,
                                    }
                                    for e in entries
                                ]
                            }
                        )

                except fs_ops.PermissionDeniedError as e:
                    logger.error(f"权限不足: {path} - {e}")
                    return jsonify({"error": "permission denied"}), 403
                except fs_ops.PathNotFoundError as e:
                    logger.error(f"路径不存在: {path} - {e}")
                    return jsonify({"error": "path not found"}), 404
                except fs_ops.FsOpError as e:
                    logger.error(f"操作失败: {path} - {e}")
                    return jsonify({"error": str(e)}), 500
                except OSError as e:
                    logger.error(f"操作失败: {path} - {e}")
                    return jsonify({"error": str(e)}), 500

        except QueueFullError as e:
            logger.warning(f"来自 {client_ip} 的请求被拒绝: {e}")
            return jsonify(
                {"error": "server busy", "detail": str(e), "queued": False}
            ), 503
        except QueueTimeoutError as e:
            logger.warning(f"来自 {client_ip} 的请求排队超时: {e}")
            return jsonify(
                {"error": "queue timeout", "detail": str(e), "queued": True}
            ), 503
        except ClientDisconnectedError:
            logger.info(f"来自 {client_ip} 的客户端在排队期间断开连接")
            return jsonify({"error": "client disconnected"}), 499

    @app.route(UPLOAD_PATH, methods=["POST"])
    def handle_upload():
        client_ip = request.remote_addr

        req_api_key = request.headers.get("X-Api-Key", "")
        if req_api_key != api_key:
            logger.warning(f"来自 {client_ip} 的上传请求鉴权失败")
            return jsonify({"error": "unauthorized"}), 403

        path = request.headers.get("X-Path", "")
        if not path:
            return jsonify({"error": "missing X-Path header"}), 400

        try:
            mtime = _parse_mtime()
        except ValueError:
            return jsonify({"error": "invalid X-Mtime header"}), 400

        path = os.path.normpath(urllib.parse.unquote(path))
        queue_timeout = _parse_queue_timeout()
        disconnect_checker = _make_disconnect_checker()

        try:
            with gate.upload_context(client_ip, queue_timeout, disconnect_checker):
                # ---- 以下为原有上传逻辑（在获取并发槽位后执行） ----
                logger.info(f"来自 {client_ip} 的上传: {path}")

                try:
                    _receive_upload(path, mtime)

                    logger.success(f"接收文件: {path}")
                    return jsonify({"success": True})

                except _UploadSizeMismatch as e:
                    logger.error(f"上传失败: {path} - {e}")
                    return jsonify({"error": str(e)}), 400
                except fs_ops.PermissionDeniedError as e:
                    logger.error(f"权限不足: {path} - {e}")
                    return jsonify({"error": "permission denied"}), 403
                except fs_ops.FsOpError as e:
                    logger.error(f"上传失败: {path} - {e}")
                    return jsonify({"error": str(e)}), 500

        except QueueFullError as e:
            logger.warning(f"来自 {client_ip} 的上传请求被拒绝: {e}")
            return jsonify(
                {"error": "server busy", "detail": str(e), "queued": False}
            ), 503
        except QueueTimeoutError as e:
            logger.warning(f"来自 {client_ip} 的上传请求排队超时: {e}")
            return jsonify(
                {"error": "queue timeout", "detail": str(e), "queued": True}
            ), 503
        except ClientDisconnectedError:
            logger.info(f"来自 {client_ip} 的客户端在上传排队期间断开连接")
            return jsonify({"error": "client disconnected"}), 499

    return app


def run_server(config, logger):
    api_key = config.get("api_key", "")
    port = int(config.get("port", "13579"))

    max_meta = int(config.get("max_concurrent_metadata", "1"))
    max_up = int(config.get("max_concurrent_uploads", "1"))
    max_q_meta = int(config.get("max_queue_metadata", "32"))
    max_q_up = int(config.get("max_queue_uploads", "32"))

    gate = ConcurrencyGate(
        max_concurrent_metadata=max_meta,
        max_concurrent_uploads=max_up,
        max_queue_metadata=max_q_meta,
        max_queue_uploads=max_q_up,
    )

    logger.info(f"监听端口: {port}")

    app = create_app(api_key, logger, gate)
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
