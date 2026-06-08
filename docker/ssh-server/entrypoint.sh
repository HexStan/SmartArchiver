#!/bin/sh
set -e

# ============================================================
# SmartArchiver SSH Server — 入口脚本
#
# 启动时自动：
#   1. 生成缺失的 SSH 主机密钥
#   2. 根据环境变量设置 root 密码（ROOT_PASSWORD）
#   3. 根据环境变量写入 authorized_keys（AUTHORIZED_KEYS）
#   4. 前台启动 sshd
# ============================================================

# --- 生成主机密钥 ---
if [ ! -f /etc/ssh/ssh_host_rsa_key ]; then
    echo ">> 生成 SSH 主机密钥……"
    ssh-keygen -A
fi

# --- 设置 root 密码 ---
if [ -n "$ROOT_PASSWORD" ]; then
    echo ">> 设置 root 密码……"
    echo "root:$ROOT_PASSWORD" | chpasswd
fi

# --- 配置 authorized_keys ---
if [ -n "$AUTHORIZED_KEYS" ]; then
    echo ">> 写入 authorized_keys……"
    mkdir -p /root/.ssh
    chmod 700 /root/.ssh
    echo "$AUTHORIZED_KEYS" > /root/.ssh/authorized_keys
    chmod 600 /root/.ssh/authorized_keys
fi

# 确保 /root/.ssh 权限正确（用于挂载 authorized_keys 文件的场景）
if [ -d /root/.ssh ]; then
    chmod 700 /root/.ssh
    if [ -f /root/.ssh/authorized_keys ]; then
        chmod 600 /root/.ssh/authorized_keys
    fi
fi

echo ">> SSH 服务器启动，监听端口 22……"
exec /usr/sbin/sshd -D -e
