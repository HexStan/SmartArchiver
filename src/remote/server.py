import os
import urllib.parse
from flask import Flask, request, jsonify

from src import fs_ops
from src.remote.protocol import RemoteAction, UPLOAD_PATH


def create_app(api_key, logger):
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
                        {"exists": False, "size": 0, "mtime": 0, "is_dir": False}
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

        path = os.path.normpath(urllib.parse.unquote(path))
        logger.info(f"来自 {client_ip} 的上传: {path}")

        try:
            dest_dir = os.path.dirname(path)
            if dest_dir:
                fs_ops.ensure_dir(dest_dir)

            chunk_size = 65536  # 64KB
            with open(path, "wb") as f:
                while True:
                    chunk = request.stream.read(chunk_size)
                    if not chunk:
                        break
                    f.write(chunk)

            logger.success(f"接收文件: {path}")
            return jsonify({"success": True})

        except fs_ops.PermissionDeniedError as e:
            logger.error(f"权限不足: {path} - {e}")
            return jsonify({"error": "permission denied"}), 403
        except fs_ops.FsOpError as e:
            logger.error(f"上传失败: {path} - {e}")
            return jsonify({"error": str(e)}), 500

    return app


def run_server(config, logger):
    api_key = config.get("api_key", "")
    port = int(config.get("port", "13579"))

    logger.info(f"服务器模式启动，监听端口: {port}")

    app = create_app(api_key, logger)
    app.run(host="0.0.0.0", port=port, debug=False)
