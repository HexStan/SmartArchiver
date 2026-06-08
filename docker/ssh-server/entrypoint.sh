#!/bin/sh
set -e

# ============================================================
# 1. 设置 syncer 用户的 authorized_keys
# ============================================================
mkdir -p /home/syncer/.ssh
chmod 700 /home/syncer/.ssh

if [ -n "$AUTHORIZED_KEYS" ]; then
    echo "$AUTHORIZED_KEYS" > /home/syncer/.ssh/authorized_keys
elif [ -f /ssh/authorized_keys ]; then
    cp /ssh/authorized_keys /home/syncer/.ssh/authorized_keys
else
    echo "ERROR: No authorized_keys provided."
    echo "Set AUTHORIZED_KEYS environment variable or mount /ssh/authorized_keys."
    exit 1
fi

chmod 600 /home/syncer/.ssh/authorized_keys
chown -R syncer:syncer /home/syncer/.ssh

# ============================================================
# 2. 可选：调整 syncer 的 UID/GID 以匹配主机文件权限
# ============================================================
if [ -n "$SYNCER_UID" ]; then
    usermod -u "$SYNCER_UID" syncer 2>/dev/null || true
fi
if [ -n "$SYNCER_GID" ]; then
    groupmod -g "$SYNCER_GID" syncer 2>/dev/null || true
fi

# ============================================================
# 3. 确保 syncer 拥有 /data 目录
# ============================================================
chown -R syncer:syncer /data

# ============================================================
# 4. 可选：覆盖 SSH 监听端口
# ============================================================
if [ -n "$SSH_PORT" ]; then
    sed -i "s/^Port .*/Port $SSH_PORT/" /etc/ssh/sshd_config
fi

# ============================================================
# 5. 确保主机密钥存在
# ============================================================
ssh-keygen -A

# ============================================================
# 6. 启动 sshd（前台运行，日志输出到 stderr）
# ============================================================
exec /usr/sbin/sshd -D -e
