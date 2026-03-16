## Panel MercadoLibre con NiceGUI y SQLite

Aplicación web en Python que usa **NiceGUI** para la interfaz gráfica, **SQLite** como base de datos local y que está pensada para integrarse con la **API de MercadoLibre**.

Actualmente incluye:

- **Registro e inicio de sesión de usuarios** (guardados en SQLite).
- **Panel principal** con pestañas:
  - Mis productos
  - Comparar precios
  - Historial de precios
  - Competencia
  - Configuración
- **Registro de consultas** en una tabla `queries` para poder auditar luego qué se consultó.
- Estructura lista para conectar con la API de MercadoLibre (por ahora con datos de ejemplo).

### Requisitos

- Python 3.9 o superior.

### Instalación

En una terminal, dentro de la carpeta del proyecto (`PythonML`):

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

> En PowerShell puede ser necesario: `.\.venv\Scripts\Activate.ps1`

### Ejecución

Con el entorno virtual activado:

```bash
python main.py
```

Luego abre el navegador en `http://localhost:8080/`.

### Configuración para enviar emails

El registro de usuarios y el reinicio de contraseña desde Admin envían emails. Para que funcione, configurá estas variables en tu archivo `.env`:

| Variable | Descripción | Ejemplo |
|----------|-------------|---------|
| `SMTP_HOST` | Servidor SMTP | `smtp.gmail.com` |
| `SMTP_PORT` | Puerto (normalmente 587) | `587` |
| `SMTP_USER` | Usuario para autenticación | `tu-email@gmail.com` |
| `SMTP_PASS` | Contraseña (en Gmail: contraseña de aplicación) | `xxxxxxxx` |
| `SMTP_FROM` | Dirección "De" en los emails | `noreply@tudominio.com` |
| `SMTP_USE_TLS` | Usar TLS (true/false) | `true` |

**Ejemplo para Gmail**: creá una [contraseña de aplicación](https://support.google.com/accounts/answer/185833) y usala en `SMTP_PASS`.

#### Emails en producción (servidor/VPS)

Si el email funciona en local pero no online:

1. **Verificá que `.env` exista en el servidor** — `.env` no se sube con git. Creálo en el servidor (copiá desde tu máquina o usá `.env.example` como plantilla) y asegurate de tener todas las variables SMTP.

2. **Gmail suele bloquear conexiones desde IPs de datacenter** — Si usás Gmail y falla en el servidor, considerá usar un servicio de email transaccional:
   - [SendGrid](https://sendgrid.com) (SMTP_HOST=smtp.sendgrid.net, puerto 587)
   - [Mailgun](https://mailgun.com) (SMTP_HOST=smtp.mailgun.org)
   - [Amazon SES](https://aws.amazon.com/ses/)
   - [Resend](https://resend.com)

3. **Puertos bloqueados** — Algunos proveedores bloquean SMTP saliente. Probá el puerto 587 (STARTTLS) en lugar de 465 (SSL).

### Próximos pasos

- Conectar realmente el login con la **API de MercadoLibre** (OAuth2).
- Guardar y refrescar los `access_token` en la tabla `ml_credentials`.
- Implementar las consultas reales:
  - Obtener publicaciones del vendedor.
  - Comparar precios con la competencia.
  - Historial de precios.
  - KPIs de competencia (cantidad de vendedores, productos, etc.).

