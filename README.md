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

### Próximos pasos

- Conectar realmente el login con la **API de MercadoLibre** (OAuth2).
- Guardar y refrescar los `access_token` en la tabla `ml_credentials`.
- Implementar las consultas reales:
  - Obtener publicaciones del vendedor.
  - Comparar precios con la competencia.
  - Historial de precios.
  - KPIs de competencia (cantidad de vendedores, productos, etc.).

