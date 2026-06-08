FROM alpine:latest

# 安装 OpenSSH 服务端和 rsync
# rsync 是 rsync-over-SSH 同步所必需的（远端必须有 rsync）
RUN apk add --no-cache openssh-server rsync

# 复制 SSH 服务端配置
COPY sshd_config /etc/ssh/sshd_config

# 复制入口脚本
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 22

ENTRYPOINT ["/entrypoint.sh"]
