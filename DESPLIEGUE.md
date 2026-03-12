# Desplegar cambios al servidor

Cuando se modifica el código localmente, hay que subir los cambios al servidor para que se apliquen en la versión online.

## Opción 1: Git (recomendado)

```bash
# En tu PC (dentro de la carpeta del proyecto)
git add .
git commit -m "Descripción de los cambios"
git push origin main

# En el servidor (SSH)
cd /ruta/del/proyecto  # donde está tu app
git pull origin main
# Reiniciar la app (p. ej. si usás systemd)
sudo systemctl restart bdc  # o el nombre de tu servicio
```

## Opción 2: Copiar archivos manualmente

1. Copiá `main.py` (y cualquier otro archivo modificado) desde tu PC al servidor.
2. Usá SCP, SFTP o el método que uses normalmente.
3. Reiniciá la app en el servidor.

## Opción 3: SCP rápido

```bash
# Desde tu PC
scp main.py root@157.230.88.160:/ruta/del/proyecto/
```

Luego en el servidor reiniciá el proceso que ejecuta la app.
