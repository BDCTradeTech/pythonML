# Guía: Configurar tu aplicación de MercadoLibre DevCenter

Para que BDC systems funcione correctamente con la API de MercadoLibre (obtener precios, stock, vendedores, etc.) necesitás configurar bien tu aplicación en el DevCenter.

## 1. Accedé al DevCenter

- **Argentina:** https://developers.mercadolibre.com.ar/devcenter/
- Iniciá sesión con tu cuenta de MercadoLibre.
- Entrá a **Mis aplicaciones** y seleccioná tu aplicación (o creá una nueva).

---

## 2. Scopes / Permisos funcionales

Tu app necesita **lectura** (y opcionalmente escritura) de publicaciones para acceder a items, precios y búsquedas.

### Activar estos permisos

1. En la app, entrá a **Configurar** o **Editar aplicación**.
2. En la sección **Scopes** o **Permisos funcionales**:
   - **Usuarios** – Ya viene por defecto (lectura de usuarios).
   - **Publicación y sincronización** – **Lectura** y **Escritura**:
     - Da acceso a: items, pictures, prices, búsquedas, catálogo, etc.
     - Sin esto no podés consultar `/items`, `/items/{id}/sale_price`, `/items/{id}/prices`, etc.

3. Si querés más funciones:
   - **Ventas y envíos** – Lectura (para órdenes).
   - **Métricas del negocio** – Lectura (para tendencias, visitas).

4. Guardá los cambios.

**Importante:** Si cambiás los scopes, tenés que **volver a vincular** la cuenta en BDC systems (Configuración → Vincular de nuevo), porque el token anterior no tiene los nuevos permisos.

---

## 3. Redirect URI

El `redirect_uri` debe coincidir **exactamente** con lo configurado en tu app.

### En el DevCenter

1. En **URIs de redirect**, agregá la URL que usa tu app.
2. Ejemplos:
   - Local: `http://localhost:8083/ml/callback`
   - Ngrok: `https://tu-subdominio.ngrok-free.dev/ml/callback`

### En tu `.env`

```env
ML_REDIRECT_URI=https://tu-subdominio.ngrok-free.dev/ml/callback
```

La URL en `.env` debe ser **idéntica** a la del DevCenter (incluyendo `http`/`https`, puerto y path).

---

## 4. Configuración de IP (si está disponible)

MercadoLibre puede restringir el acceso por IP. Si tu app devuelve 403, puede ser por IP bloqueada.

### Comprobar si tenés la opción

1. En **Mis aplicaciones** → tu app.
2. Revisá si aparece **Configuración IP** en el menú.
3. Si **no aparece**, la gestión de IPs es solo para integradores whitelisteados. En ese caso, el 403 puede deberse a que tu IP (o la de ngrok) está bloqueada por políticas generales.

### Si tenés Configuración IP

1. Ejecutá el script para ver tu IP pública:
   ```bash
   python obtener_mi_ip.py
   ```
2. En el DevCenter, agregá esa IP en **Configuración IP**.
3. Si usás **ngrok**, la IP que hace las llamadas es la de los servidores de ngrok (no tu PC). Podés contactar a MercadoLibre para entender las restricciones de IP.

---

## 5. Autorización con `offline_access`

Para que el refresh token funcione y no tengas que vincular cada 6 horas:

1. En la URL de autorización, asegurate de pedir el scope `offline_access`.
2. En BDC systems ya está configurado si usás el enlace "Vincular de nuevo" desde Configuración.

---

## 6. Resumen de pasos

| Paso | Acción |
|------|--------|
| 1 | DevCenter → Tu app → Configurar |
| 2 | Activar **Publicación y sincronización** (lectura y escritura) |
| 3 | Verificar que **Redirect URI** coincida con `.env` |
| 4 | Si ves **Configuración IP**, agregar tu IP pública |
| 5 | Guardar cambios en la app |
| 6 | En BDC systems: Configuración → **Vincular de nuevo** |
| 7 | Probar de nuevo la búsqueda y consulta de items |

---

## 7. Si seguís teniendo 403

1. **Probá sin ngrok:** ejecutá la app en local (`python main.py`) y usá `http://localhost:8083` como redirect.
2. **Revisá el estado de la app:** que no esté bloqueada o deshabilitada.
3. **Validación de datos:** que la cuenta del vendedor no tenga datos pendientes de validación.
4. **Soporte:** si nada funciona, contactá a MercadoLibre Developers para consultar restricciones de IP o políticas de uso.
