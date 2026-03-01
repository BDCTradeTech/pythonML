#!/bin/bash
# Script de deploy para DigitalOcean
# Configura estas variables según tu servidor:
DROPLET_USER="root"
DROPLET_IP="157.230.88.160"
REMOTE_PATH="/opt/pythonml"

set -e

echo "1. Subiendo codigo via git..."
git push origin main

echo "2. Subiendo base de datos app.db..."
scp app.db "${DROPLET_USER}@${DROPLET_IP}:${REMOTE_PATH}/app.db"

echo "3. Reiniciando app en el servidor..."
ssh "${DROPLET_USER}@${DROPLET_IP}" "cd $REMOTE_PATH && git pull && systemctl restart pythonml"

echo "Deploy completado!"
