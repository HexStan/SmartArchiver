"""目标地址（dest）解析与目标侧 I/O 转发层。

dest 串语法（全系统唯一的解析权威）：

    {http:别名}?远端路径   —— HTTP 远端，转发至 RemoteClient
    {ssh:别名}?远端路径    —— SSH 远端，仅 sync 模式使用（rsync/rclone 整树传输）
    其他                   —— 本地路径，转发至 fs

**约定（DocStrings 契约）**：所有针对目标目录的读写行为必须经由本模块。
fs 包是本地路径专用原语——对带 {类型:别名}? 前缀的协议串调用 fs 只会得到
静默的错误结果；本模块是唯一按前缀分派 fs / RemoteClient 的转发点。

SSH 目标不参与逐文件转发：sync 模式的文件传输由外部工具整树完成，本模块
对 {ssh:别名}? 协议串的逐文件原语直接抛错；SSH 连接参数与远端目录操作
在 src/ssh/ops.py。

装配期校验（ensure_dest）保证：流入处理器的 dest 串要么是本地路径，要么
是"模式支持且别名已配置"的远端协议串，配置错误不会推迟到落盘阶段。
"""

import os
import posixpath
import re
from typing import NamedTuple

from src import fs
from src.app.context import AppContext
from src.remote.client import RemoteClient

# ============================================================
# dest 串解析
# ============================================================

LOCAL = "local"
HTTP = "http"
SSH = "ssh"

_REMOTE_DEST_PATTERN = re.compile(r"^\{([a-z]+):([a-zA-Z0-9\-_]+)\}\?(.*)")

# Windows 盘符绝对路径：D:/Backup、D:\Backup，或裸盘符根 D:/ 与 D:
_WIN_REMOTE_PATH_PATTERN = re.compile(r"^[A-Za-z]:([\\/].*)?$")


class ParsedDest(NamedTuple):
    """dest 串的解析结果。

    kind —— 目标类型：LOCAL / HTTP / SSH；未知远端类型保留原始串，
    由 ensure_dest 拒绝。
    alias —— 远端别名，本地目标为空串。
    path —— 归一化后的路径；本地目标为原串（路径形态交给 os.path）。
    """

    kind: str
    alias: str
    path: str


def _normalize_http_path(remote_path: str) -> str:
    """归一化 HTTP 远端根路径。

    POSIX 路径统一为单一前导 /；Windows 盘符路径（如 D:/Backup）本身
    已是绝对路径，不能补前导 /——否则 Windows 服务端的 normpath 会把
    /D:/Backup 变成当前盘根下的非法路径。盘符路径中的反斜杠统一为
    正斜杠，与 build_dest_path 的拼接规则保持一致。
    """
    if _WIN_REMOTE_PATH_PATTERN.match(remote_path):
        return remote_path.replace("\\", "/").rstrip("/")
    return "/" + remote_path.lstrip("/")


def parse_dest(dest: str) -> ParsedDest:
    """解析 dest 串为目标类型、别名与归一化路径（纯语法解析，不查别名存在性）。

    解析幂等：对已归一化的路径重复解析结果不变，转发原语因此可以
    对完整协议串逐调用解析，无需在调用方之间传递解析状态。
    """
    if not dest or not isinstance(dest, str):
        return ParsedDest(LOCAL, "", dest)
    m = _REMOTE_DEST_PATTERN.match(dest)
    if m is None:
        return ParsedDest(LOCAL, "", dest)

    remote_type, alias, remote_path = m.group(1), m.group(2), m.group(3)
    if remote_type == HTTP:
        return ParsedDest(HTTP, alias, _normalize_http_path(remote_path))
    if remote_type == SSH:
        # SSH 远端为 POSIX 语义（rsync / rclone over SSH），统一补前导 /
        return ParsedDest(SSH, alias, "/" + remote_path.lstrip("/"))
    return ParsedDest(remote_type, alias, remote_path)


def _prefix(parsed: ParsedDest) -> str:
    """重建协议前缀（"{类型:别名}?"）；本地目标为空串。"""
    if parsed.kind == LOCAL:
        return ""
    return f"{{{parsed.kind}:{parsed.alias}}}?"


# ============================================================
# 装配期校验
# ============================================================


def ensure_dest(dest: str, mode: str) -> bool:
    """装配期校验 dest 串与任务模式的兼容性（fail-fast，失败即跳过任务）。

    - 远端别名必须在对应命名空间中已配置（http 与 ssh 命名空间隔离，
      允许同名别名并存）；
    - move/copy/rotate 的逐文件传输仅支持本地与 HTTP 目标；
    - sync 的整树同步仅支持本地与 SSH 目标。

    合法（含 dest 为空——move/copy 允许省略目标，逐文件动作由调用方
    守卫跳过）返回 True；不合法时已记录错误日志并返回 False。不回退为
    本地目标：带前缀的原始串不是合法的本地路径，回退只会把配置错误
    推迟到落盘阶段才暴露。
    """
    ctx = AppContext.get()
    parsed = parse_dest(dest)

    if parsed.kind == LOCAL:
        return True

    if parsed.kind == HTTP:
        if parsed.alias not in ctx.remote_clients:
            ctx.logger.error(f"HTTP 远端别名 '{parsed.alias}' 未在 http_remotes 中配置，任务跳过")
            return False
        if mode == "sync":
            ctx.logger.error(
                "sync 模式不支持 HTTP 远程目标目录，请改用 move/copy 模式，"
                "或在远程服务器上直接运行 sync，跳过该任务。"
            )
            return False
        return True

    if parsed.kind == SSH:
        if parsed.alias not in ctx.ssh_remotes:
            ctx.logger.error(f"SSH 远端别名 '{parsed.alias}' 未在 ssh_remotes 中配置，任务跳过")
            return False
        if mode in ("move", "copy", "rotate"):
            ctx.logger.error(
                f"{mode} 模式不支持 SSH 远端目标目录"
                f"（逐文件传输仅支持本地与 HTTP，SSH 目标请使用 sync 模式），跳过该任务。"
            )
            return False
        return True

    ctx.logger.error(
        f"未知的远端类型 '{parsed.kind}'（dest 路径: {dest}），仅支持 http 和 ssh，任务跳过"
    )
    return False


# ============================================================
# 路径规则（纯函数）
# ============================================================


def build_dest_path(root: str, rel_path: str) -> str:
    """把相对路径拼接到目标根路径，返回完整目标协议串。

    本地目标使用 os.path 语义（normpath 折叠 ../）；远端目标使用 posix
    语义（反斜杠统一为正斜杠、不做 normpath），并保留 {类型:别名}? 前缀，
    使下游转发原语仍能按前缀分派。
    """
    parsed = parse_dest(root)
    if parsed.kind == LOCAL:
        return os.path.normpath(os.path.join(root, rel_path))
    rel = rel_path.replace("\\", "/")
    return f"{_prefix(parsed)}{parsed.path.rstrip('/')}/{rel}"


def unique_dest(dest_path: str) -> str:
    """冲突副本编号：name-1.ext、name-2.ext……存在性检查经由转发原语。

    本地用 os.path 语义，远端用 posixpath 语义（与 build_dest_path 的
    正斜杠拼接规则同模型），返回值保留协议前缀。
    """
    if not exists(dest_path):
        return dest_path

    parsed = parse_dest(dest_path)
    if parsed.kind == LOCAL:
        directory, filename = os.path.split(dest_path)
        name, ext = os.path.splitext(filename)
        counter = 1
        while True:
            candidate = os.path.join(directory, f"{name}-{counter}{ext}")
            if not exists(candidate):
                return candidate
            counter += 1

    directory = posixpath.dirname(parsed.path)
    name, ext = posixpath.splitext(posixpath.basename(parsed.path))
    counter = 1
    while True:
        candidate = posixpath.join(directory, f"{name}-{counter}{ext}")
        if not exists(f"{_prefix(parsed)}{candidate}"):
            return f"{_prefix(parsed)}{candidate}"
        counter += 1


def relpath(root: str, dest_path: str) -> str:
    """把完整目标路径还原为相对 root 的路径（用于日志展示）。"""
    parsed = parse_dest(root)
    if parsed.kind == LOCAL:
        return os.path.relpath(dest_path, root)

    # 远端：先剥协议前缀，再剥 root 的归一化路径；不在 root 之下时原样返回
    prefix = _prefix(parsed)
    stripped = dest_path.removeprefix(prefix)
    root_path = parsed.path.rstrip("/")
    if root_path and stripped.startswith(root_path + "/"):
        return stripped[len(root_path) + 1 :]
    return dest_path


# ============================================================
# I/O 转发原语
# ============================================================


def _http_client(alias: str) -> RemoteClient:
    """按别名取 HTTP 远端客户端（装配期 ensure_dest 已保证存在）。"""
    client = AppContext.get().remote_clients.get(alias)
    if client is None:
        raise RuntimeError(f"HTTP 远端别名 '{alias}' 未在 http_remotes 中配置")
    return client


def _ssh_unsupported(dest_path: str) -> NotImplementedError:
    return NotImplementedError(
        f"SSH 目标不参与逐文件转发: {dest_path}（sync 模式的文件传输由 rsync/rclone 整树完成）"
    )


def exists(path: str) -> bool:
    """目标路径存在性检查。"""
    parsed = parse_dest(path)
    if parsed.kind == HTTP:
        return _http_client(parsed.alias).exists(parsed.path)
    if parsed.kind == SSH:
        raise _ssh_unsupported(path)
    return fs.path_exists(path)


def is_dir(path: str) -> bool:
    """目标路径是否为目录。"""
    parsed = parse_dest(path)
    if parsed.kind == HTTP:
        return _http_client(parsed.alias).is_dir(parsed.path)
    if parsed.kind == SSH:
        raise _ssh_unsupported(path)
    return fs.is_dir(path)


def copy_file(src_local_path: str, dest_path: str) -> None:
    """把本地文件复制到目标路径（HTTP 远端为上传）。"""
    parsed = parse_dest(dest_path)
    if parsed.kind == HTTP:
        _http_client(parsed.alias).upload(src_local_path, parsed.path)
    elif parsed.kind == SSH:
        raise _ssh_unsupported(dest_path)
    else:
        fs.copy_file(src_local_path, dest_path)


def move_file(src_local_path: str, dest_path: str) -> None:
    """把本地文件移动到目标路径（HTTP 远端为上传 + 删除源文件）。"""
    parsed = parse_dest(dest_path)
    if parsed.kind == HTTP:
        _http_client(parsed.alias).upload(src_local_path, parsed.path)
        fs.delete_file(src_local_path)
    elif parsed.kind == SSH:
        raise _ssh_unsupported(dest_path)
    else:
        fs.move_file(src_local_path, dest_path)
