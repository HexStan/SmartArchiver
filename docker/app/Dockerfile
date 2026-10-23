FROM python:3.14-slim-trixie

# 设置环境变量，确保 Python 输出不被缓冲，以便日志能实时显示
ENV PYTHONUNBUFFERED=1

# 安装 rsync、SSH 客户端及 sshpass（用于 SSH 远端同步）
RUN apt-get update && \
    apt-get install -y --no-install-recommends rsync openssh-client sshpass && \
    rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /app

# 先复制依赖清单并安装依赖层（源码变更时此层缓存仍可复用）
COPY pyproject.toml .
RUN pip install --no-cache-dir --group runtime

# 复制项目文件
COPY . .

ENTRYPOINT ["python", "main.py"]
CMD []
