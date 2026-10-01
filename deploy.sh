#!/bin/bash
# NUNCA copiar app.db al droplet: la base de producción vive solo ahí
# Script de deploy para DigitalOcean
# Configura estas variables según tu servidor:
DROPLET_USER="root"
DROPLET_IP="157.230.88.160"
REMOTE_PATH="/opt/pythonml"

set -e

echo "1. Subiendo codigo via git..."
git push origin main

echo "2. Reiniciando app en el servidor..."
ssh "${DROPLET_USER}@${DROPLET_IP}" "cd $REMOTE_PATH && git pull && systemctl restart pythonml"

echo "Deploy completado!"
