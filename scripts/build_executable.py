#!/usr/bin/env python3
"""构建 SmartArchiver 可执行文件（PyInstaller onedir + 内置 rclone）。

用法:
    python scripts/build_executable.py                       # 自动识别当前平台
    python scripts/build_executable.py --target macos-arm64  # 指定目标平台

流程: 下载对应平台的 rclone 官方压缩包 → 解包暂存到 build/ 目录
      → 以 SmartArchiver.spec 执行 PyInstaller onedir 打包
      → 将示例配置复制进程序目录 → 压缩为发行 zip。

产物:
    dist/SmartArchiver/                          解包即用的程序目录
    dist/SmartArchiver-<版本>-<目标平台>.zip      发行压缩包（config/ 下附带示例配置）
"""

import argparse
import os
import platform
import shutil
import subprocess
import sys
import tomllib
import urllib.request
import zipfile

# 内置 rclone 的版本与官方下载地址（Windows/macOS × AMD64/ARM64）
RCLONE_VERSION = "1.75.2"
RCLONE_BASE_URL = f"https://downloads.rclone.org/v{RCLONE_VERSION}"

RCLONE_URLS = {
    "windows-amd64": f"{RCLONE_BASE_URL}/rclone-v{RCLONE_VERSION}-windows-amd64.zip",
    "windows-arm64": f"{RCLONE_BASE_URL}/rclone-v{RCLONE_VERSION}-windows-arm64.zip",
    "macos-amd64": f"{RCLONE_BASE_URL}/rclone-v{RCLONE_VERSION}-osx-amd64.zip",
    "macos-arm64": f"{RCLONE_BASE_URL}/rclone-v{RCLONE_VERSION}-osx-arm64.zip",
}

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SPEC_FILE = os.path.join(REPO_ROOT, "SmartArchiver.spec")


def detect_target():
    """从当前运行环境识别构建目标。"""
    machine = platform.machine().lower()
    arch = "arm64" if machine in ("arm64", "aarch64") else "amd64"

    system = platform.system()
    if system == "Windows":
        return f"windows-{arch}"
    if system == "Darwin":
        return f"macos-{arch}"

    raise SystemExit(
        f"不支持的平台: {system} {machine}。"
        f"可执行文件仅支持 Windows/macOS，可用 --target 显式指定（仅用于交叉暂存）。"
    )


def read_version():
    """从 pyproject.toml 的 [project].version 读取版本号（版本号唯一出处）。"""
    with open(os.path.join(REPO_ROOT, "pyproject.toml"), "rb") as f:
        return tomllib.load(f)["project"]["version"]


def download(url, dest, retries=3):
    for attempt in range(1, retries + 1):
        try:
            print(f"下载 rclone（第 {attempt}/{retries} 次）: {url}")
            part = dest + ".part"
            request = urllib.request.Request(url, headers={"User-Agent": "SmartArchiver-build"})
            with urllib.request.urlopen(request) as resp, open(part, "wb") as f:
                f.write(resp.read())
            os.replace(part, dest)
            return
        except Exception as e:  # noqa: BLE001
            if attempt == retries:
                raise SystemExit(f"下载失败: {url}\n{e}")
            print(f"下载失败，将重试: {e}")


def stage_rclone(target):
    """下载并解包对应平台的 rclone，返回暂存的二进制路径（已暂存则复用）。"""
    exe_name = "rclone.exe" if target.startswith("windows") else "rclone"
    stage_dir = os.path.join(REPO_ROOT, "build", "rclone", target)
    os.makedirs(stage_dir, exist_ok=True)
    staged = os.path.join(stage_dir, exe_name)

    if os.path.isfile(staged):
        print(f"复用已暂存的 rclone: {staged}")
        return staged

    zip_path = os.path.join(stage_dir, "rclone.zip")
    download(RCLONE_URLS[target], zip_path)

    with zipfile.ZipFile(zip_path) as zf:
        # 压缩包内为 rclone-vX.Y.Z-平台/rclone[.exe] 的嵌套结构
        member = next(n for n in zf.namelist() if os.path.basename(n) == exe_name)
        zf.extract(member, stage_dir)
    shutil.move(os.path.join(stage_dir, member), staged)
    os.remove(zip_path)

    if os.name != "nt":
        os.chmod(staged, 0o755)

    print(f"rclone 已暂存: {staged}")
    return staged


def verify_rclone(rclone_bin):
    """执行 rclone version 验证二进制可用。"""
    try:
        result = subprocess.run([rclone_bin, "version"], capture_output=True, text=True, check=True)
    except (OSError, subprocess.CalledProcessError) as e:
        raise SystemExit(f"内置 rclone 无法运行: {e}")

    first_line = result.stdout.splitlines()[0] if result.stdout else ""
    print(f"内置 rclone 校验通过: {first_line}")


def clean_dist():
    """清空上一次构建产物，避免手动运行残留（config.toml、logs 等）混入发行包。"""
    dist_dir = os.path.join(REPO_ROOT, "dist", "SmartArchiver")
    if os.path.isdir(dist_dir):
        shutil.rmtree(dist_dir)


def stage_example_configs():
    """将仓库 config/ 下的示例配置复制进程序目录，供用户改名后使用。"""
    config_src = os.path.join(REPO_ROOT, "config")
    config_dst = os.path.join(REPO_ROOT, "dist", "SmartArchiver", "config")
    os.makedirs(config_dst, exist_ok=True)
    for name in os.listdir(config_src):
        if name.endswith(".example.toml"):
            shutil.copy2(os.path.join(config_src, name), os.path.join(config_dst, name))


def run_pyinstaller(rclone_bin):
    env = os.environ.copy()
    env["RCLONE_STAGE"] = rclone_bin

    cmd = [
        sys.executable,
        "-m",
        "PyInstaller",
        SPEC_FILE,
        "--noconfirm",
        "--clean",
    ]
    print("+ " + " ".join(cmd))
    subprocess.run(cmd, cwd=REPO_ROOT, env=env, check=True)


def make_zip(target, version):
    """压缩 dist/SmartArchiver 为发行 zip（保留可执行权限位）。"""
    dist_dir = os.path.join(REPO_ROOT, "dist", "SmartArchiver")
    if not os.path.isdir(dist_dir):
        raise SystemExit(f"打包产物缺失: {dist_dir}")

    zip_path = os.path.join(REPO_ROOT, "dist", f"SmartArchiver-{version}-{target}.zip")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _dirs, files in os.walk(dist_dir):
            for name in files:
                full = os.path.join(root, name)
                arc = os.path.relpath(full, os.path.dirname(dist_dir))
                arc = arc.replace(os.sep, "/")
                zf.write(full, arc)
                # 显式标记为 0755，保证 macOS/Linux 解压后二进制可直接执行
                zf.getinfo(arc).external_attr = 0o100755 << 16

    return zip_path


def main():
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(description="构建 SmartArchiver 可执行文件")
    parser.add_argument(
        "--target",
        choices=sorted(RCLONE_URLS),
        help="目标平台，缺省自动识别当前平台",
    )
    args = parser.parse_args()

    target = args.target or detect_target()
    version = read_version()
    print(f"构建目标: {target}，版本: {version}")

    rclone_bin = stage_rclone(target)
    verify_rclone(rclone_bin)
    clean_dist()
    run_pyinstaller(rclone_bin)
    stage_example_configs()
    zip_path = make_zip(target, version)

    print(
        f"构建完成:\n  程序目录: dist{os.sep}SmartArchiver\n  发行包: {os.path.relpath(zip_path, REPO_ROOT)}"
    )


if __name__ == "__main__":
    main()
