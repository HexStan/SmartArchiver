"""白盒·路径查询与元数据原语（fs.scan.get_stat）。

为什么必须白盒（tests/README.md 原则二）：
- 错误语义契约（缺失 → exists=False；权限不足 → PermissionDeniedError；
  其他 OS 错误 → FsOpError）从 execute() 灰盒入口不可观察——生产调用方
  只有远端 STAT 端点；
- "存在性判定与元数据读取出自同一次 os.stat"是性能契约，从返回值本身
  无法区分。
fs 包是无业务依赖的纯原语层，直接调用即为被测入口，全程真实文件系统、
不在 OS 边界打桩。
"""

import os

import pytest

from src import fs
from src.fs.errors import PermissionDeniedError

# ============================================================
# get_stat
# ============================================================


def test_get_stat_returns_metadata_for_existing_file(tmp_path):
    src = tmp_path / "a.txt"
    src.write_bytes(b"data")

    st = fs.get_stat(str(src))

    assert st.exists is True
    assert st.is_dir is False
    assert st.size == 4
    assert st.mtime == src.stat().st_mtime


def test_get_stat_marks_directory_as_dir(tmp_path):
    d = tmp_path / "sub"
    d.mkdir()

    st = fs.get_stat(str(d))

    assert st.exists is True
    assert st.is_dir is True


def test_get_stat_returns_exists_false_for_missing_path(tmp_path):
    st = fs.get_stat(str(tmp_path / "missing"))

    assert st.exists is False


def test_get_stat_returns_exists_false_for_null_byte_path():
    st = fs.get_stat("a\x00b")

    assert st.exists is False


@pytest.mark.skipif(os.name != "posix", reason="仅 POSIX 可用目录权限触发 stat 权限错误")
def test_get_stat_raises_permission_denied_on_unreadable_parent(tmp_path):
    guard = tmp_path / "guard"
    guard.mkdir()
    (guard / "secret.txt").write_bytes(b"data")
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        pytest.skip("root 不受目录权限限制")
    os.chmod(guard, 0o000)
    try:
        with pytest.raises(PermissionDeniedError):
            fs.get_stat(str(guard / "secret.txt"))
    finally:
        os.chmod(guard, 0o755)
