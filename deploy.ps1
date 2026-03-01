# Script de deploy para DigitalOcean
# Configurá estas variables según tu servidor:
$DROPLET_USER = "root"
$DROPLET_IP = "157.230.88.160"
$REMOTE_PATH = "/opt/pythonml"

$ErrorActionPreference = "Stop"

Write-Host "1. Subiendo codigo via git..." -ForegroundColor Cyan
git push origin main

Write-Host "2. Subiendo base de datos app.db..." -ForegroundColor Cyan
scp app.db "${DROPLET_USER}@${DROPLET_IP}:${REMOTE_PATH}/app.db"

Write-Host "3. Reiniciando app en el servidor..." -ForegroundColor Cyan
ssh "${DROPLET_USER}@${DROPLET_IP}" "cd $REMOTE_PATH && git pull && systemctl restart pythonml"
# Si usas otro metodo (pm2, screen, etc.) cambia el comando anterior

Write-Host "Deploy completado!" -ForegroundColor Green
