FROM alpine:latest

# 安装 openssh-server 和 rsync（rsync 是 rsync-over-SSH 远端必需的二进制）
RUN apk add --no-cache openssh-server rsync

# 创建 syncer 用户（非 root，无密码，无 sudo 权限）
RUN adduser -D -h /home/syncer syncer

# 首次生成主机密钥并创建 sshd 运行时目录
RUN ssh-keygen -A && \
    mkdir -p /run/sshd && \
    chmod 755 /run/sshd

# 自定义 sshd 配置（仅允许 syncer 用户通过密钥登录）
COPY sshd_config /etc/ssh/sshd_config

# 入口脚本（处理 authorized_keys、UID/GID、端口等动态配置）
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

VOLUME /data

EXPOSE 2222
ENTRYPOINT ["/entrypoint.sh"]
