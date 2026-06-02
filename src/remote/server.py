import os
import shutil
from flask import Flask, request, jsonify

from src.remote.protocol import RemoteAction


def create_app(api_token, logger):
    app = Flask(__name__)

    @app.before_request
    def _log_request():
        client_ip = request.remote_addr
        logger.info(f"收到来自 {client_ip} 的连接请求")

    @app.route("/api/remote", methods=["POST"])
    def handle_remote():
        client_ip = request.remote_addr

        token = request.form.get("token", "")
        if token != api_token:
            logger.warning(f"来自 {client_ip} 的请求鉴权失败")
            return jsonify({"error": "unauthorized"}), 403

        action = request.form.get("action", "")
        if not RemoteAction.is_valid(action):
            logger.warning(f"来自 {client_ip} 的请求使用了未知操作: {action}")
            return jsonify({"error": f"unknown action: {action}"}), 400

        path = request.form.get("path", "")
        if not path:
            return jsonify({"error": "missing path"}), 400

        path = os.path.normpath(path)

        logger.info(f"来自 {client_ip} 的指令: {action} -> {path}")

        try:
            if action == RemoteAction.EXISTS:
                return jsonify({"exists": os.path.exists(path)})

            elif action == RemoteAction.IS_DIR:
                return jsonify({"is_dir": os.path.isdir(path)})

            elif action == RemoteAction.MKDIR:
                os.makedirs(path, exist_ok=True)
                logger.success(f"创建目录: {path}")
                return jsonify({"success": True})

            elif action == RemoteAction.DELETE:
                if os.path.isdir(path):
                    shutil.rmtree(path)
                    logger.success(f"删除目录: {path}")
                elif os.path.isfile(path):
                    os.remove(path)
                    logger.success(f"删除文件: {path}")
                else:
                    return jsonify({"error": "path not found"}), 404
                return jsonify({"success": True})

            elif action == RemoteAction.UPLOAD:
                uploaded = request.files.get("file")
                if not uploaded:
                    return jsonify({"error": "missing file"}), 400

                dest_dir = os.path.dirname(path)
                if dest_dir:
                    os.makedirs(dest_dir, exist_ok=True)

                uploaded.save(path)
                logger.success(f"接收文件: {path}")
                return jsonify({"success": True})

            elif action == RemoteAction.STAT:
                if not os.path.exists(path):
                    return jsonify({"exists": False, "size": 0, "mtime": 0, "is_dir": False})

                st = os.stat(path)
                return jsonify({
                    "exists": True,
                    "size": st.st_size,
                    "mtime": st.st_mtime,
                    "is_dir": os.path.isdir(path),
                })

            elif action == RemoteAction.LIST_DIR:
                if not os.path.isdir(path):
                    return jsonify({"error": "not a directory"}), 400
                entries = []
                with os.scandir(path) as it:
                    for entry in it:
                        entries.append({
                            "name": entry.name,
                            "is_dir": entry.is_dir(),
                            "size": entry.stat().st_size if entry.is_file() else 0,
                            "mtime": entry.stat().st_mtime,
                        })
                return jsonify({"entries": entries})

        except PermissionError as e:
            logger.error(f"权限不足: {path} - {e}")
            return jsonify({"error": "permission denied"}), 403
        except OSError as e:
            logger.error(f"操作失败: {path} - {e}")
            return jsonify({"error": str(e)}), 500

    return app


def run_server(config, logger):
    api_token = config.get("api_key", "")
    port = int(config.get("port", "13579"))

    logger.info(f"服务器模式启动，监听端口: {port}")

    app = create_app(api_token, logger)
    app.run(host="0.0.0.0", port=port, debug=False)
