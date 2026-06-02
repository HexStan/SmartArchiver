from enum import Enum


class RemoteAction(str, Enum):
    EXISTS = "exists"
    IS_DIR = "is_dir"
    MKDIR = "mkdir"
    DELETE = "delete"
    UPLOAD = "upload"
    TRANSFER = "transfer"
    STAT = "stat"
    LIST_DIR = "list_dir"

    @classmethod
    def is_valid(cls, value):
        return value in cls._value2member_map_


API_PATH = "/api/remote"
