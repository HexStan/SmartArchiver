FROM python:3.14-slim-trixie

# 设置环境变量，确保 Python 输出不被缓冲，以便日志能实时显示
ENV PYTHONUNBUFFERED=1

# 多架构构建的目标架构（BuildKit 注入，取值 amd64/arm64，与 rclone 发布包命名一致）
ARG TARGETARCH

# 安装 rsync、SSH 客户端及 sshpass（用于 SSH 远端同步），以及 curl（供下方下载 rclone）
RUN apt-get update && \
    apt-get install -y --no-install-recommends rsync openssh-client sshpass curl && \
    rm -rf /var/lib/apt/lists/*

# 安装官方固定版本的 rclone（版本与 scripts/build_executable.py 的内置版本保持一致；
# Debian 源的 rclone 版本过旧，故从官方发布地址下载。下载必须走 curl——
# 该站点的 Cloudflare 防护按 User-Agent 拦截，Python urllib 的默认 UA 会被 403；
# 解压复用基础镜像自带的 Python，按发布包内固定的嵌套路径取出二进制）
RUN curl -fsSL -o /tmp/rclone.zip "https://downloads.rclone.org/v1.75.2/rclone-v1.75.2-linux-${TARGETARCH}.zip" && \
    python -c "import zipfile; zipfile.ZipFile('/tmp/rclone.zip').extract('rclone-v1.75.2-linux-${TARGETARCH}/rclone', '/tmp/rclone')" && \
    mv /tmp/rclone/rclone-v1.75.2-linux-${TARGETARCH}/rclone /usr/local/bin/rclone && \
    chmod 0755 /usr/local/bin/rclone && \
    rm -rf /tmp/rclone.zip /tmp/rclone && \
    rclone version

# 设置工作目录
WORKDIR /app

# 先复制依赖清单并安装依赖层（源码变更时此层缓存仍可复用）
COPY pyproject.toml .
RUN pip install --no-cache-dir --group runtime

# 复制项目文件
COPY . .

ENTRYPOINT ["python", "main.py"]
CMD []
