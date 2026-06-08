#!/bin/sh
set -e

# ============================================================
# 1. 设置 root 用户的 authorized_keys
# ============================================================
mkdir -p /root/.ssh
chmod 700 /root/.ssh

if [ -n "$AUTHORIZED_KEYS" ]; then
    echo "$AUTHORIZED_KEYS" > /root/.ssh/authorized_keys
elif [ -f /ssh/authorized_keys ]; then
    cp /ssh/authorized_keys /root/.ssh/authorized_keys
else
    echo "ERROR: No authorized_keys provided."
    echo "Set AUTHORIZED_KEYS environment variable or mount /ssh/authorized_keys."
    exit 1
fi

chmod 600 /root/.ssh/authorized_keys

# ============================================================
# 2. 可选：覆盖 SSH 监听端口
# ============================================================
if [ -n "$SSH_PORT" ]; then
    sed -i "s/^Port .*/Port $SSH_PORT/" /etc/ssh/sshd_config
fi

# ============================================================
# 3. 确保主机密钥存在
# ============================================================
ssh-keygen -A

# ============================================================
# 4. 启动 sshd（前台运行，日志输出到 stderr）
# ============================================================
exec /usr/sbin/sshd -D -e
