# -*- mode: python ; coding: utf-8 -*-
"""SmartArchiver 打包配置（PyInstaller，onedir 模式）。

由 scripts/build_executable.py 驱动构建：脚本会先下载并解包对应平台的
rclone，再通过环境变量 RCLONE_STAGE 把二进制路径传入本文件，rclone 被
打包进程序目录的 rclone/ 子目录（PyInstaller 6.x onedir 布局下位于
_internal/rclone/），运行时由 src/app_paths.py 解析。

产物布局（dist/SmartArchiver/）：
    SmartArchiver.exe        Windows 入口
    SmartArchiver            macOS 入口（无后缀裸二进制）
    _internal/               运行时与内置 rclone
"""

import os

rclone_stage = os.environ.get("RCLONE_STAGE")
if not rclone_stage or not os.path.isfile(rclone_stage):
    raise SystemExit(
        "缺少内置 rclone 二进制：请通过 scripts/build_executable.py 构建，"
        "或设置环境变量 RCLONE_STAGE 指向已解包的 rclone 可执行文件。"
    )

a = Analysis(
    ["main.py"],
    pathex=[],
    binaries=[(rclone_stage, "rclone")],
    datas=[],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="SmartArchiver",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=False,
    upx_exclude=[],
    name="SmartArchiver",
)
