# syntax=docker/dockerfile:1
FROM python:3.14-slim-trixie

# 设置环境变量，确保 Python 输出不被缓冲，以便日志能实时显示
ENV PYTHONUNBUFFERED=1

# 多架构构建的目标架构（BuildKit 注入，取值 amd64/arm64，与 rclone 发布包命名一致）
ARG TARGETARCH

# 安装 rsync、SSH 客户端及 sshpass（用于 SSH 远端同步），以及 curl（供下方下载 rclone）
RUN apt-get update && \
    apt-get install -y --no-install-recommends rsync openssh-client sshpass curl && \
    rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /app

# 先复制依赖清单（[tool.smartarchiver] 同时声明内置 rclone 版本，供下方读取；
# 此层在 apt 之后，pyproject.toml 变更不影响 apt 层缓存）
COPY pyproject.toml .

# 安装官方固定版本的 rclone（版本唯一出处：pyproject.toml 的 [tool.smartarchiver].rclone_version，
# 可执行文件构建（scripts/build_executable.py）读取同一处。Debian 源的 rclone 版本过旧，
# 故从官方发布地址下载 .deb 包并用 dpkg 安装（安装后位于 /usr/bin/rclone）。下载必须走
# curl——该站点的 Cloudflare 防护按 User-Agent 拦截，Python urllib 的默认 UA 会被 403）
RUN RCLONE_VERSION=$(python -c 'import tomllib; print(tomllib.load(open("pyproject.toml", "rb"))["tool"]["smartarchiver"]["rclone_version"])') && \
    curl -fsSL -o /tmp/rclone.deb "https://downloads.rclone.org/v${RCLONE_VERSION}/rclone-v${RCLONE_VERSION}-linux-${TARGETARCH}.deb" && \
    dpkg -i /tmp/rclone.deb && \
    rm -f /tmp/rclone.deb && \
    rclone version

# 安装依赖层（源码变更时此层缓存仍可复用）：
# uv 通过 BuildKit bind mount 从官方镜像临时挂载，仅本条 RUN 期间可见、不落入
# 任何镜像层（最终镜像不含 uv）；下载缓存经 cache mount 存于构建器而非镜像层
# （cache 目录按 key 持久复用，重建此层时免重新下载）。钉精确版本，与本地生成
# uv.lock 的 uv 版本一致，dependabot 不跟踪该引用，升级 uv 重锁后需手动同步此处。
# --locked 严格按 uv.lock 安装，锁与 pyproject.toml 不一致时构建失败；
# --only-group runtime 只装运行依赖组（uv sync 默认会装 dev 组）；
# --no-install-project 项目无 [build-system]（虚拟项目），显式声明不安装项目本身；
# UV_COMPILE_BYTECODE=1 预编译字节码，对齐 pip 安装的默认行为，首次启动免编译
COPY uv.lock ./
RUN --mount=from=ghcr.io/astral-sh/uv:0.13.5,source=/uv,target=/bin/uv \
    --mount=type=cache,target=/root/.cache/uv \
    UV_COMPILE_BYTECODE=1 uv sync --locked --only-group runtime --no-install-project

# venv 中的解释器与依赖优先于系统 Python（ENTRYPOINT 的 python 解析到 venv）
ENV PATH="/app/.venv/bin:$PATH"

# 复制项目文件
COPY . .

ENTRYPOINT ["python", "main.py"]
CMD []
