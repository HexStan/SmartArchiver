import urllib.parse
from flask import Flask, request, jsonify

from src.remote.protocol import RemoteAction, UPLOAD_PATH
from src.utils import (
    path_exists,
    is_directory,
    ensure_dir,
    remove_path,
    get_stat,
    list_directory,
    write_file_stream,
)


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

        logger.info(f"来自 {client_ip} 的指令: {action} -> {path}")

        try:
            if action == RemoteAction.EXISTS:
                return jsonify({"exists": path_exists(path)})

            elif action == RemoteAction.IS_DIR:
                return jsonify({"is_dir": is_directory(path)})

            elif action == RemoteAction.MKDIR:
                ensure_dir(path)
                logger.success(f"创建目录: {path}")
                return jsonify({"success": True})

            elif action == RemoteAction.DELETE:
                remove_path(path)
                logger.success(f"删除: {path}")
                return jsonify({"success": True})

            elif action == RemoteAction.STAT:
                return jsonify(get_stat(path))

            elif action == RemoteAction.LIST_DIR:
                if not is_directory(path):
                    return jsonify({"error": "not a directory"}), 400
                return jsonify({"entries": list_directory(path)})

        except PermissionError as e:
            logger.error(f"权限不足: {path} - {e}")
            return jsonify({"error": "permission denied"}), 403
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

        path = urllib.parse.unquote(path)
        logger.info(f"来自 {client_ip} 的上传: {path}")

        try:
            write_file_stream(path, request.stream)
            logger.success(f"接收文件: {path}")
            return jsonify({"success": True})

        except PermissionError as e:
            logger.error(f"权限不足: {path} - {e}")
            return jsonify({"error": "permission denied"}), 403
        except OSError as e:
            logger.error(f"上传失败: {path} - {e}")
            return jsonify({"error": str(e)}), 500

    return app


def run_server(config, logger):
    api_key = config.get("api_key", "")
    port = int(config.get("port", "13579"))

    logger.info(f"服务器模式启动，监听端口: {port}")

    app = create_app(api_key, logger)
    app.run(host="0.0.0.0", port=port, debug=False)
