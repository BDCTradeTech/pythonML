# Configurar dominio bdctechtrade.com (sin :8083)

## 1. Nginx en el servidor (DigitalOcean)

### Instalar Nginx (si no lo tenés)
```bash
sudo apt update
sudo apt install nginx -y
```

### Configurar el sitio
```bash
# En el servidor, dentro de la carpeta del proyecto:
sudo cp nginx-bdctechtrade.conf /etc/nginx/sites-available/bdctechtrade
sudo ln -sf /etc/nginx/sites-available/bdctechtrade /etc/nginx/sites-enabled/

# Quitar sitio por defecto (opcional)
sudo rm -f /etc/nginx/sites-enabled/default

# Probar configuración
sudo nginx -t

# Aplicar cambios
sudo systemctl reload nginx
```

### Verificar
Abrí http://bdctechtrade.com (sin :8083) — debería funcionar.

---

## 2. HTTPS con Let's Encrypt (recomendado)

MercadoLibre e Intuit prefieren HTTPS para los redirect URIs.

```bash
sudo apt install certbot python3-certbot-nginx -y
sudo certbot --nginx -d bdctechtrade.com -d www.bdctechtrade.com
```

Certbot configurará HTTPS automáticamente. Después podés usar:
- https://bdctechtrade.com
- https://www.bdctechtrade.com

---

## 3. Callbacks en el sistema BDC

### Variables de entorno (.env en el servidor)
Agregá o actualizá en `.env`:

```env
# Para producción con dominio
ML_REDIRECT_URI=https://bdctechtrade.com/ml/callback
QB_REDIRECT_URI=https://bdctechtrade.com/qb/callback
```

Si usás `www`:
```env
ML_REDIRECT_URI=https://www.bdctechtrade.com/ml/callback
QB_REDIRECT_URI=https://www.bdctechtrade.com/qb/callback
```

Reiniciá la app después de cambiar `.env`.

---

## 4. MercadoLibre Developers

1. Ir a https://developers.mercadolibre.com.ar/apps
2. Seleccionar tu app
3. En **Configuración** → **Redirect URI**
4. Agregar: `https://bdctechtrade.com/ml/callback` (o con www si usás eso)
5. Guardar

---

## 5. Intuit (QuickBooks) Developers

1. Ir a https://developer.intuit.com
2. **My Apps** → tu app QuickBooks
3. **Development** o **Production** → **Keys & credentials**
4. En **Redirect URIs** agregar: `https://bdctechtrade.com/qb/callback`
5. Guardar

---

## 6. En la app BDC (Configuración)

1. Entrá a https://bdctechtrade.com (o www)
2. **Configuración** → **MercadoLibre** → Redirect URI: `https://bdctechtrade.com/ml/callback`
3. **Configuración** → **QuickBooks** → Redirect URI: `https://bdctechtrade.com/qb/callback`
4. Hacé clic en **"Usar URL actual"** en cada uno para que se complete automáticamente
5. Guardar credenciales
6. Desvincular y volver a Conectar (para obtener nuevos tokens con las URLs correctas)

---

## Resumen de URLs

| Servicio | Redirect URI |
|----------|--------------|
| MercadoLibre | https://bdctechtrade.com/ml/callback |
| QuickBooks | https://bdctechtrade.com/qb/callback |

Si preferís usar `www`: reemplazá `bdctechtrade.com` por `www.bdctechtrade.com` en todas las URLs.
