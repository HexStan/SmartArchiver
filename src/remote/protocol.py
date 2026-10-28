from enum import Enum


class RemoteAction(str, Enum):
    EXISTS = "exists"
    IS_DIR = "is_dir"
    MKDIR = "mkdir"
    DELETE_FILE = "delete_file"
    DELETE_DIR = "delete_dir"
    STAT = "stat"
    LIST_DIR = "list_dir"

    @classmethod
    def is_valid(cls, value: object) -> bool:
        return value in cls._value2member_map_


API_PATH = "/api/remote"
UPLOAD_PATH = "/api/remote/upload"

# 上传流式传输的读取分块（字节）：客户端发送与服务端接收使用同一粒度，
# 与本地复制循环（fs.transfer 的 _COPY_CHUNK_SIZE）对齐
UPLOAD_CHUNK_SIZE = 1024 * 1024  # 1MB
