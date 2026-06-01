"""
db.py — Capa de datos SQLite para BDC systems.
Extraído de main.py — Fase 1 del refactor.

Contiene:
- Helpers de encriptación (_get_fernet, _encrypt_secret, _decrypt_secret)
- get_connection(), init_db()
- CRUD de credenciales ML/QB
- CRUD de settings, cotizador, usuarios, compras, pedidos, catálogos
- export/import de datos
- save_query()

NO contiene: funciones QB API (fetch_qb_*), auth (hash_password, authenticate_user),
ni funciones ML API. Esas van en qb_api.py, auth.py y ml_api.py respectivamente.
"""
from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from cryptography.fernet import Fernet

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

DB_PATH = Path(__file__).with_name("app.db")

# Pestañas del sistema (tab_key interno -> label visible). Replicado de main.py
# para que get_user_tab_permissions() funcione sin importar main.
TAB_KEYS = [
    ("home", "Home"),
    ("estadisticas", "Estadísticas"),
    ("ventas", "Ventas"),
    ("productos", "Productos"),
    ("cuotas", "Cuotas"),
    ("busqueda", "Busquedas"),
    ("balance", "Balance"),
    ("compras", "Invoices"),
    ("stock", "Stock"),
    ("compras_lista", "Compras"),
    ("pedidos", "Pedidos"),
    ("historicos", "Históricos"),
    ("importacion", "Importacion"),
    ("pesos", "Pesos"),
    ("datos", "Datos"),
    ("configuracion", "Configuración"),
    ("admin", "Admin"),
]

BACKUP_VERSION = 2


# ==========================
# ENCRIPTACIÓN DE SECRETS
# ==========================


def _get_fernet() -> Fernet:
    key = os.getenv("CREDENTIAL_ENCRYPTION_KEY", "")
    if not key:
        raise RuntimeError("CREDENTIAL_ENCRYPTION_KEY no configurado. Ver .env.example")
    return Fernet(key.encode())


def _encrypt_secret(plain: str) -> str:
    return _get_fernet().encrypt(plain.encode()).decode()


def _decrypt_secret(token: str) -> str:
    if not token.startswith("gAAAAA"):
        return token  # plaintext legacy: aún no migrado
    return _get_fernet().decrypt(token.encode()).decode()


# ==========================
# CAPA DE DATOS (SQLite)
# ==========================


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Crea las tablas si no existen."""
    conn = get_connection()
    cur = conn.cursor()

    # Usuarios de la app
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    # Migración: agregar columna email si no existe
    cur.execute("PRAGMA table_info(users)")
    user_cols = [r[1] for r in cur.fetchall()]
    if "email" not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN email TEXT")

    # Credenciales de la App de MercadoLibre por usuario (cada usuario puede tener su propia app)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ml_app_credentials (
            user_id INTEGER PRIMARY KEY,
            client_id TEXT NOT NULL,
            client_secret TEXT NOT NULL,
            redirect_uri TEXT,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
        """
    )

    # Credenciales de la app QuickBooks (Client ID, Client Secret, Redirect URI) — OAuth 2.0
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS qb_app_credentials (
            user_id INTEGER PRIMARY KEY,
            client_id TEXT NOT NULL,
            client_secret TEXT NOT NULL,
            redirect_uri TEXT,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
        """
    )

    # Tokens OAuth de QuickBooks (access_token, refresh_token, expires_at, realm_id)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS qb_tokens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            access_token TEXT,
            refresh_token TEXT,
            expires_at TEXT,
            realm_id TEXT,
            raw_data TEXT,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
        """
    )

    # Cliente QuickBooks por usuario (para multi-tenant: cada usuario = un Customer en QB)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS user_qb_customer (
            user_id INTEGER PRIMARY KEY,
            qb_customer_id TEXT NOT NULL,
            qb_customer_name TEXT,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
        """
    )

    # Clientes QuickBooks preasignados por email (no editables por el usuario)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS qb_customer_preasignado (
            email TEXT PRIMARY KEY,
            qb_customer_id TEXT NOT NULL,
            qb_customer_name TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        INSERT OR IGNORE INTO qb_customer_preasignado (email, qb_customer_id, qb_customer_name)
        VALUES
            ('diegolas@gmail.com', '101', 'CAMINIZA SRL CUIT 33-71851985-9 (id 101)'),
            ('info@dsmax.com.ar', '136', 'DSMAX TECH'),
            ('diegog@exxa.com.ar', '5', 'Exxa Store'),
            ('sanjustocentrocomputacion@gmail.com', '55', 'SAN JUSTO CENTRO COMPUTACION SRL 30-71777663-8')
        """
    )
    cur.execute(
        "UPDATE qb_customer_preasignado SET qb_customer_name = 'CAMINIZA SRL CUIT 33-71851985-9 (id 101)' WHERE qb_customer_id = '101'"
    )
    cur.execute(
        "UPDATE qb_customer_preasignado SET qb_customer_id = '55' WHERE LOWER(TRIM(email)) = 'sanjustocentrocomputacion@gmail.com'"
    )

    # Credenciales de MercadoLibre asociadas al usuario (tokens OAuth)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ml_credentials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            access_token TEXT,
            refresh_token TEXT,
            expires_at TEXT,
            raw_data TEXT,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
        """
    )

    # Historial de consultas que hagamos a la API
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS queries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            query_type TEXT NOT NULL,
            params TEXT,
            created_at TEXT NOT NULL,
            raw_response TEXT,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
        """
    )

    # Configuración global (Dolar Oficial, Dolar Blue, etc.)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value REAL NOT NULL
        )
        """
    )
    # Valores por defecto la primera vez
    cur.execute(
        "INSERT OR IGNORE INTO settings (key, value) VALUES ('dolar_oficial', 1475), ('dolar_blue', 1465)"
    )

    # Datos del cotizador de importaciones (por usuario: user_id, clave, valor)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS cotizador_datos (
            user_id INTEGER NOT NULL,
            clave TEXT NOT NULL,
            valor TEXT,
            PRIMARY KEY (user_id, clave),
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
        """
    )

    # Migración: si existe tabla antigua sin user_id, migrar datos al primer usuario
    cur.execute("PRAGMA table_info(cotizador_datos)")
    cols = [r[1] for r in cur.fetchall()]
    if cols and "user_id" not in cols:
            cur.execute("ALTER TABLE cotizador_datos RENAME TO cotizador_datos_old")
            cur.execute(
                """
                CREATE TABLE cotizador_datos (
                    user_id INTEGER NOT NULL,
                    clave TEXT NOT NULL,
                    valor TEXT,
                    PRIMARY KEY (user_id, clave),
                    FOREIGN KEY (user_id) REFERENCES users (id)
                )
                """
            )
            cur.execute("SELECT id FROM users ORDER BY id LIMIT 1")
            first_user = cur.fetchone()
            if first_user:
                uid = first_user["id"]
                cur.execute(
                    "INSERT INTO cotizador_datos (user_id, clave, valor) SELECT ?, clave, valor FROM cotizador_datos_old",
                    (uid,),
                )
            cur.execute("DROP TABLE cotizador_datos_old")

    # Filas de Importación guardadas (datos completos por fila)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS importacion_filas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            fila_orden INTEGER NOT NULL,
            datos_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
        """
    )

    # Lista de compras a cotizar (marca, producto, cantidad, precio_sugerido, estado, usuario_qb)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS compras_lista (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            fecha TEXT NOT NULL,
            marca TEXT,
            producto TEXT,
            cantidad TEXT,
            precio_sugerido TEXT,
            estado TEXT NOT NULL DEFAULT 'Cotizar',
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
        """
    )
    try:
        cur.execute("ALTER TABLE compras_lista ADD COLUMN usuario_qb TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        cur.execute("ALTER TABLE compras_lista ADD COLUMN sku TEXT")
    except sqlite3.OperationalError:
        pass

    # Marcas (catálogo global para Compras)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS marcas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre TEXT NOT NULL UNIQUE
        )
        """
    )

    # Despachantes (catálogo global)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS despachantes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre TEXT NOT NULL UNIQUE
        )
        """
    )
    cur.execute("SELECT COUNT(*) FROM despachantes")
    if cur.fetchone()[0] == 0:
        for nombre in ["LHS", "NC Supplies", "Sixtar", "Rosario"]:
            cur.execute("INSERT INTO despachantes (nombre) VALUES (?)", (nombre,))

    # Lista de pedidos (similar a compras + cliente)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS pedidos_lista (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            fecha TEXT NOT NULL,
            marca TEXT,
            producto TEXT,
            cantidad TEXT,
            precio_sugerido TEXT,
            estado TEXT NOT NULL DEFAULT 'Cotizar',
            cliente TEXT,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
        """
    )

    # Permisos por pestaña por usuario (Admin)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS user_tab_permissions (
            user_id INTEGER NOT NULL,
            tab_key TEXT NOT NULL,
            can_access INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY (user_id, tab_key),
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
        """
    )

    # invoice_extra: datos adicionales por invoice de QB (courier, guía, importe factura, estado, despachante)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS invoice_extra (
            user_id INTEGER NOT NULL,
            qb_invoice_id TEXT NOT NULL,
            courier TEXT,
            guia TEXT,
            importe_factura TEXT,
            estado TEXT,
            despachante TEXT,
            PRIMARY KEY (user_id, qb_invoice_id),
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
        """
    )

    # Productos: catálogo interno por SKU
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS productos (
            sku          TEXT NOT NULL,
            user_id      INTEGER NOT NULL,
            marca        TEXT,
            nombre       TEXT,
            color        TEXT,
            costo_usd    REAL,
            fob_usd      REAL DEFAULT NULL,
            tipo_iva     REAL DEFAULT 0.105,
            notas        TEXT,
            created_at   TEXT NOT NULL,
            updated_at   TEXT NOT NULL,
            costo_updated_at TEXT,
            PRIMARY KEY (sku, user_id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
        """
    )
    # Migración: agregar fob_usd si no existe
    cur.execute("PRAGMA table_info(productos)")
    _prod_cols = [r[1] for r in cur.fetchall()]
    if "fob_usd" not in _prod_cols:
        cur.execute("ALTER TABLE productos ADD COLUMN fob_usd REAL DEFAULT NULL")
    if "price_updated_at" not in _prod_cols:
        cur.execute("ALTER TABLE productos ADD COLUMN price_updated_at TEXT")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS revisiones_diarias (
            sku             TEXT NOT NULL,
            user_id         INTEGER NOT NULL,
            fecha           TEXT NOT NULL,
            precio_cambiado INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (sku, user_id, fecha)
        )
        """
    )

    # ML publicaciones: vínculo entre items de ML y SKU interno
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ml_publicaciones (
            ml_id           TEXT NOT NULL,
            user_id         INTEGER NOT NULL,
            sku             TEXT,
            titulo          TEXT,
            precio          REAL,
            stock           INTEGER,
            estado          TEXT,
            catalog_listing INTEGER DEFAULT 0,
            listing_type_id TEXT,
            sold_quantity   INTEGER,
            ultima_sync     TEXT,
            PRIMARY KEY (ml_id, user_id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ventas_datos (
            payment_id    TEXT NOT NULL,
            user_id       INTEGER NOT NULL,
            order_id      TEXT,
            gan_pesos     REAL,
            gan_vta_pct   REAL,
            gan_cos_pct   REAL,
            meli_fee      REAL,
            cuotas_fee    REAL,
            iva_total     REAL,
            deb_cred      REAL,
            iibb_ret      REAL,
            sirtac        REAL,
            envio_real    REAL,
            logistic_type TEXT,
            net_rcv       REAL,
            fetched_at    TEXT,
            pay_status    TEXT,
            PRIMARY KEY (payment_id, user_id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS flex_zonas (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id          INTEGER NOT NULL,
            nombre           TEXT NOT NULL,
            codigos_postales TEXT NOT NULL,
            tarifa           REAL NOT NULL,
            orden            INTEGER DEFAULT 0,
            created_at       TEXT DEFAULT (datetime('now'))
        )
        """
    )

    # Migración: agregar pay_status a ventas_datos si no existe
    cur.execute("PRAGMA table_info(ventas_datos)")
    vd_cols = [r[1] for r in cur.fetchall()]
    if "pay_status" not in vd_cols:
        cur.execute("ALTER TABLE ventas_datos ADD COLUMN pay_status TEXT")

    # Migración: agregar columna despachante a invoice_extra si no existe (tablas antiguas)
    cur.execute("PRAGMA table_info(invoice_extra)")
    inv_extra_cols = [r[1] for r in cur.fetchall()]
    if "despachante" not in inv_extra_cols:
        cur.execute("ALTER TABLE invoice_extra ADD COLUMN despachante TEXT")
    if "pa" not in inv_extra_cols:
        cur.execute("ALTER TABLE invoice_extra ADD COLUMN pa TEXT")
    if "factura_courier" not in inv_extra_cols:
        cur.execute("ALTER TABLE invoice_extra ADD COLUMN factura_courier TEXT")

    # ARCA: datos fiscales manuales por bloque (global, sin user_id)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS arca_datos (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            bloque     TEXT NOT NULL,
            campo      TEXT NOT NULL,
            valor      TEXT,
            updated_at TEXT,
            UNIQUE(bloque, campo)
        )
        """
    )

    # ARCA: Convenio Multilateral por provincia
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS arca_multilateral (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            provincia        TEXT NOT NULL,
            alicuota         REAL NOT NULL DEFAULT 0,
            a_favor_contrib  REAL NOT NULL DEFAULT 0,
            a_favor_fisco    REAL NOT NULL DEFAULT 0,
            a_pagar          REAL NOT NULL DEFAULT 0,
            updated_at       TEXT
        )
        """
    )
    # Migración: agregar columnas nuevas si la tabla ya existía con el esquema anterior
    for _col_def in (
        "alicuota REAL DEFAULT 0",
        "a_favor_contrib REAL DEFAULT 0",
        "a_favor_fisco REAL DEFAULT 0",
        "a_pagar REAL DEFAULT 0",
    ):
        try:
            cur.execute(f"ALTER TABLE arca_multilateral ADD COLUMN {_col_def}")
        except sqlite3.OperationalError:
            pass

    # Migración: dar permisos por defecto a usuarios existentes (admin para el usuario con id más bajo)
    cur.execute("SELECT MIN(id) FROM users")
    _admin_uid = cur.fetchone()[0] or 1
    cur.execute("SELECT id FROM users ORDER BY id")
    for row in cur.fetchall():
        uid = row["id"]
        for tab_key in ("home", "estadisticas", "ventas", "productos", "cuotas", "busqueda", "balance", "compras", "stock", "compras_lista", "pedidos", "importacion", "pesos", "arca", "datos", "configuracion", "admin"):
            can = 1 if tab_key != "admin" or uid == _admin_uid else 0
            cur.execute(
                "INSERT OR IGNORE INTO user_tab_permissions (user_id, tab_key, can_access) VALUES (?, ?, ?)",
                (uid, tab_key, can),
            )

    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# CRUD — Credenciales ML
# ---------------------------------------------------------------------------


def get_ml_app_credentials(user_id: int) -> Optional[Dict[str, str]]:
    """Obtiene las credenciales de la app ML del usuario (client_id, client_secret, redirect_uri)."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT client_id, client_secret, redirect_uri FROM ml_app_credentials WHERE user_id = ?",
            (user_id,),
        )
        row = cur.fetchone()
        if row and row["client_id"] and row["client_secret"]:
            return {
                "client_id": row["client_id"],
                "client_secret": _decrypt_secret(row["client_secret"]),
                "redirect_uri": (row["redirect_uri"] or "").strip() or None,
            }
        return None
    finally:
        conn.close()


def set_ml_app_credentials(user_id: int, client_id: str, client_secret: str, redirect_uri: Optional[str] = None) -> None:
    """Guarda las credenciales de la app ML del usuario."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        enc = _encrypt_secret(client_secret.strip())
        cur.execute(
            "INSERT INTO ml_app_credentials (user_id, client_id, client_secret, redirect_uri) VALUES (?, ?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET client_id=?, client_secret=?, redirect_uri=?",
            (user_id, client_id.strip(), enc, redirect_uri or "", client_id.strip(), enc, redirect_uri or ""),
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# CRUD — Credenciales QB
# ---------------------------------------------------------------------------


def get_qb_app_credentials(user_id: int) -> Optional[Dict[str, str]]:
    """Obtiene las credenciales de la app QuickBooks (client_id, client_secret, redirect_uri)."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT client_id, client_secret, redirect_uri FROM qb_app_credentials WHERE user_id = ?",
            (user_id,),
        )
        row = cur.fetchone()
        if row and row["client_id"] and row["client_secret"]:
            return {
                "client_id": row["client_id"],
                "client_secret": _decrypt_secret(row["client_secret"]),
                "redirect_uri": (row["redirect_uri"] or "").strip() or None,
            }
        return None
    finally:
        conn.close()


def set_qb_app_credentials(user_id: int, client_id: str, client_secret: str, redirect_uri: Optional[str] = None) -> None:
    """Guarda las credenciales de la app QuickBooks."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        enc = _encrypt_secret(client_secret.strip())
        cur.execute(
            "INSERT INTO qb_app_credentials (user_id, client_id, client_secret, redirect_uri) VALUES (?, ?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET client_id=?, client_secret=?, redirect_uri=?",
            (user_id, client_id.strip(), enc, redirect_uri or "", client_id.strip(), enc, redirect_uri or ""),
        )
        conn.commit()
    finally:
        conn.close()


def get_qb_tokens(user_id: int) -> Optional[Any]:
    """Obtiene tokens QB del usuario. Si no tiene, usa los del admin (user_id=1) como fallback — todos usan la misma cuenta QB."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT access_token, refresh_token, expires_at, realm_id FROM qb_tokens WHERE user_id = ? ORDER BY id DESC LIMIT 1",
            (user_id,),
        )
        row = cur.fetchone()
        if row and row["access_token"]:
            return dict(row)
        if user_id != 1:
            cur.execute(
                "SELECT access_token, refresh_token, expires_at, realm_id FROM qb_tokens WHERE user_id = 1 ORDER BY id DESC LIMIT 1"
            )
            row = cur.fetchone()
            if row and row["access_token"]:
                return dict(row)
        return None
    finally:
        conn.close()


def get_user_qb_customer(user_id: int) -> Optional[Dict[str, str]]:
    """Obtiene el Cliente QuickBooks: primero user_qb_customer (Config), sino qb_customer_preasignado por email."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT qb_customer_id, qb_customer_name FROM user_qb_customer WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        if row and row["qb_customer_id"]:
            return {"id": row["qb_customer_id"], "name": row["qb_customer_name"] or row["qb_customer_id"]}
        cur.execute("SELECT username FROM users WHERE id = ?", (user_id,))
        user_row = cur.fetchone()
        if not user_row or not user_row["username"]:
            return None
        email = (user_row["username"] or "").strip().lower()
        cur.execute(
            "SELECT qb_customer_id, qb_customer_name FROM qb_customer_preasignado WHERE LOWER(TRIM(email)) = ?",
            (email,),
        )
        row = cur.fetchone()
        if row and row["qb_customer_id"]:
            return {"id": row["qb_customer_id"], "name": row["qb_customer_name"] or row["qb_customer_id"]}
        return None
    finally:
        conn.close()


def set_user_qb_customer(user_id: int, qb_customer_id: str, qb_customer_name: Optional[str] = None) -> None:
    """Asigna el Cliente QuickBooks al usuario (para filtrar datos por cliente)."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO user_qb_customer (user_id, qb_customer_id, qb_customer_name) VALUES (?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET qb_customer_id=?, qb_customer_name=?",
            (user_id, qb_customer_id, qb_customer_name or qb_customer_id, qb_customer_id, qb_customer_name or qb_customer_id),
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# CRUD — Settings
# ---------------------------------------------------------------------------


def get_setting(key: str) -> Optional[float]:
    """Obtiene un valor numérico de settings. Devuelve None si no existe."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = cur.fetchone()
        return float(row["value"]) if row is not None else None
    finally:
        conn.close()


def set_setting(key: str, value: float) -> None:
    """Guarda un valor en settings."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = ?",
            (key, value, value),
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# CRUD — Cotizador
# ---------------------------------------------------------------------------


def get_cotizador_param(key: str, user_id: int) -> Optional[str]:
    """Obtiene un parámetro del cotizador (texto) para el usuario."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT valor FROM cotizador_datos WHERE user_id = ? AND clave = ?", (user_id, key))
        row = cur.fetchone()
        return row["valor"] if row and row["valor"] is not None else None
    finally:
        conn.close()


def set_cotizador_param(key: str, value: str, user_id: int) -> None:
    """Guarda un parámetro del cotizador para el usuario."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO cotizador_datos (user_id, clave, valor) VALUES (?, ?, ?) ON CONFLICT(user_id, clave) DO UPDATE SET valor = ?",
            (user_id, key, value, value),
        )
        conn.commit()
    finally:
        conn.close()


def delete_cotizador_param(key: str, user_id: int) -> None:
    """Elimina un parámetro del cotizador de la BD para el usuario."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM cotizador_datos WHERE user_id = ? AND clave = ?", (user_id, key))
        conn.commit()
    finally:
        conn.close()


def get_cotizador_tabla(nombre: str, user_id: int) -> List[Dict[str, Any]]:
    """Obtiene una tabla del cotizador (lista de dicts) para el usuario."""
    val = get_cotizador_param(f"tabla_{nombre}", user_id)
    if not val:
        return []
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return []


def set_cotizador_tabla(nombre: str, rows: List[Dict[str, Any]], user_id: int) -> None:
    """Guarda una tabla del cotizador para el usuario."""
    set_cotizador_param(f"tabla_{nombre}", json.dumps(rows, ensure_ascii=False), user_id)


# ---------------------------------------------------------------------------
# CRUD — Usuarios y permisos
# ---------------------------------------------------------------------------


def list_users_excluding(user_id: int) -> List[Dict[str, Any]]:
    """Lista usuarios excluyendo el indicado. Devuelve [{id, username}, ...]."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, username FROM users WHERE id != ? ORDER BY username", (user_id,))
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_all_users() -> List[Dict[str, Any]]:
    """Lista todos los usuarios. Devuelve [{id, username}, ...]."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, username FROM users ORDER BY username")
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_user_tab_permissions(user_id: int) -> Dict[str, bool]:
    """Devuelve {tab_key: can_access} para el usuario. Si no hay fila, default True salvo admin=False."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT tab_key, can_access FROM user_tab_permissions WHERE user_id = ?", (user_id,))
        rows = cur.fetchall()
        result: Dict[str, bool] = {}
        for r in rows:
            result[str(r["tab_key"])] = bool(r["can_access"])
        for tab_key, _ in TAB_KEYS:
            if tab_key not in result:
                result[tab_key] = True if tab_key != "admin" else False
        return result
    finally:
        conn.close()


def set_user_tab_permission(user_id: int, tab_key: str, can_access: bool) -> None:
    """Actualiza o inserta permiso de pestaña para un usuario."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO user_tab_permissions (user_id, tab_key, can_access) VALUES (?, ?, ?) ON CONFLICT(user_id, tab_key) DO UPDATE SET can_access=?",
            (user_id, tab_key, 1 if can_access else 0, 1 if can_access else 0),
        )
        conn.commit()
    finally:
        conn.close()


def _enable_tabs_for_user(user_id: int, tab_set: set) -> None:
    """Habilita un conjunto de tabs para un usuario solo si actualmente están en 0."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        for tab_key in tab_set:
            cur.execute(
                "INSERT INTO user_tab_permissions (user_id, tab_key, can_access) VALUES (?, ?, 1) "
                "ON CONFLICT(user_id, tab_key) DO UPDATE SET can_access=1 WHERE can_access=0",
                (user_id, tab_key),
            )
        conn.commit()
    finally:
        conn.close()


def user_can_access_tab(user_id: int, tab_key: str) -> bool:
    """Devuelve si el usuario puede acceder a la pestaña."""
    perms = get_user_tab_permissions(user_id)
    return perms.get(tab_key, True if tab_key != "admin" else False)


# ---------------------------------------------------------------------------
# CRUD — Compras lista
# ---------------------------------------------------------------------------


def get_compras_lista(user_id: int) -> List[Dict[str, Any]]:
    """Obtiene la lista de compras del usuario."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, user_id, fecha, marca, producto, sku, cantidad, precio_sugerido, estado, usuario_qb FROM compras_lista WHERE user_id = ? ORDER BY id",
            (user_id,),
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_compras_lista_all() -> List[Dict[str, Any]]:
    """Obtiene la lista de compras de TODOS los usuarios (para Pedidos consolidado)."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, user_id, fecha, marca, producto, sku, cantidad, precio_sugerido, estado, usuario_qb FROM compras_lista ORDER BY id DESC",
            (),
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_compras_lista_row(row_id: int, user_id: int) -> Optional[Dict[str, Any]]:
    """Obtiene una fila de compras_lista por id."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, user_id, fecha, marca, producto, sku, cantidad, precio_sugerido, estado, usuario_qb FROM compras_lista WHERE id = ? AND user_id = ?",
            (row_id, user_id),
        )
        r = cur.fetchone()
        return dict(r) if r else None
    finally:
        conn.close()


def insert_compras_lista(user_id: int, fecha: str, marca: str = "", producto: str = "", sku: str = "", cantidad: str = "", precio_sugerido: str = "", estado: str = "Cotizar", usuario_qb: str = "") -> int:
    """Inserta una fila en compras_lista. Devuelve el id insertado."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO compras_lista (user_id, fecha, marca, producto, sku, cantidad, precio_sugerido, estado, usuario_qb) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (user_id, fecha, marca or "", producto or "", sku or "", str(cantidad or ""), str(precio_sugerido or ""), estado or "Cotizar", usuario_qb or ""),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def update_compras_lista_row(row_id: int, user_id: int, **kwargs) -> None:
    """Actualiza una fila de compras_lista."""
    if not kwargs:
        return
    conn = get_connection()
    try:
        cur = conn.cursor()
        sets = []
        vals = []
        for k, v in kwargs.items():
            if k in ("fecha", "marca", "producto", "sku", "cantidad", "precio_sugerido", "estado", "usuario_qb"):
                sets.append(f"{k} = ?")
                vals.append(str(v or "") if k != "estado" else (str(v) if v is not None else "Cotizar"))
        if sets:
            vals.append(row_id)
            vals.append(user_id)
            # Seguro: las claves se validan contra whitelist antes de este punto
            cur.execute(
                f"UPDATE compras_lista SET {', '.join(sets)} WHERE id = ? AND user_id = ?",
                vals,
            )
            conn.commit()
    finally:
        conn.close()


def delete_compras_lista_row(row_id: int, user_id: int) -> None:
    """Elimina una fila de compras_lista."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM compras_lista WHERE id = ? AND user_id = ?", (row_id, user_id))
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# CRUD — Pedidos lista
# ---------------------------------------------------------------------------


def get_pedidos_lista(user_id: int) -> List[Dict[str, Any]]:
    """Obtiene la lista de pedidos del usuario."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, fecha, marca, producto, cantidad, precio_sugerido, estado, cliente FROM pedidos_lista WHERE user_id = ? ORDER BY id",
            (user_id,),
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def insert_pedidos_lista(user_id: int, fecha: str, marca: str = "", producto: str = "", cantidad: str = "", precio_sugerido: str = "", estado: str = "Cotizar", cliente: str = "") -> int:
    """Inserta una fila en pedidos_lista."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO pedidos_lista (user_id, fecha, marca, producto, cantidad, precio_sugerido, estado, cliente) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (user_id, fecha, marca or "", producto or "", str(cantidad or ""), str(precio_sugerido or ""), estado or "Cotizar", cliente or ""),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def update_pedidos_lista_row(row_id: int, user_id: int, **kwargs) -> None:
    """Actualiza una fila de pedidos_lista."""
    if not kwargs:
        return
    conn = get_connection()
    try:
        cur = conn.cursor()
        sets = []
        vals = []
        for k, v in kwargs.items():
            if k in ("fecha", "marca", "producto", "cantidad", "precio_sugerido", "estado", "cliente"):
                sets.append(f"{k} = ?")
                vals.append(str(v or "") if k != "estado" else (str(v) if v is not None else "Cotizar"))
        if sets:
            vals.append(row_id)
            vals.append(user_id)
            # Seguro: las claves se validan contra whitelist antes de este punto
            cur.execute(
                f"UPDATE pedidos_lista SET {', '.join(sets)} WHERE id = ? AND user_id = ?",
                vals,
            )
            conn.commit()
    finally:
        conn.close()


def delete_pedidos_lista_row(row_id: int, user_id: int) -> None:
    """Elimina una fila de pedidos_lista."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM pedidos_lista WHERE id = ? AND user_id = ?", (row_id, user_id))
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# CRUD — Catálogos (Marcas, Despachantes, Invoice Extra)
# ---------------------------------------------------------------------------


def get_marcas() -> List[Dict[str, Any]]:
    """Obtiene todas las marcas (id, nombre)."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, nombre FROM marcas ORDER BY nombre")
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def insert_marca(nombre: str) -> Optional[str]:
    """Inserta una marca. Devuelve None si OK, mensaje de error si falla."""
    nombre_clean = (nombre or "").strip()
    if not nombre_clean:
        return "El nombre no puede estar vacío"
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO marcas (nombre) VALUES (?)", (nombre_clean,))
        conn.commit()
        return None
    except sqlite3.IntegrityError:
        return "Ya existe una marca con ese nombre"
    finally:
        conn.close()


def update_marca(marca_id: int, nombre: str) -> Optional[str]:
    """Actualiza una marca. Devuelve None si OK, mensaje de error si falla."""
    nombre_clean = (nombre or "").strip()
    if not nombre_clean:
        return "El nombre no puede estar vacío"
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE marcas SET nombre = ? WHERE id = ?", (nombre_clean, marca_id))
        conn.commit()
        if cur.rowcount == 0:
            return "Marca no encontrada"
        return None
    except sqlite3.IntegrityError:
        return "Ya existe una marca con ese nombre"
    finally:
        conn.close()


def delete_marca(marca_id: int) -> Optional[str]:
    """Elimina una marca. Devuelve None si OK, mensaje de error si falla."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM marcas WHERE id = ?", (marca_id,))
        conn.commit()
        if cur.rowcount == 0:
            return "Marca no encontrada"
        return None
    finally:
        conn.close()


def get_despachantes() -> List[Dict[str, Any]]:
    """Obtiene todos los despachantes (id, nombre)."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, nombre FROM despachantes ORDER BY nombre")
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def insert_despachante(nombre: str) -> Optional[str]:
    """Inserta un despachante. Devuelve None si OK, mensaje de error si falla."""
    nombre_clean = (nombre or "").strip()
    if not nombre_clean:
        return "El nombre no puede estar vacío"
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO despachantes (nombre) VALUES (?)", (nombre_clean,))
        conn.commit()
        return None
    except sqlite3.IntegrityError:
        return "Ya existe un despachante con ese nombre"
    finally:
        conn.close()


def update_despachante(despachante_id: int, nombre: str) -> Optional[str]:
    """Actualiza un despachante. Devuelve None si OK, mensaje de error si falla."""
    nombre_clean = (nombre or "").strip()
    if not nombre_clean:
        return "El nombre no puede estar vacío"
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE despachantes SET nombre = ? WHERE id = ?", (nombre_clean, despachante_id))
        conn.commit()
        if cur.rowcount == 0:
            return "Despachante no encontrado"
        return None
    except sqlite3.IntegrityError:
        return "Ya existe un despachante con ese nombre"
    finally:
        conn.close()


def delete_despachante(despachante_id: int) -> Optional[str]:
    """Elimina un despachante. Devuelve None si OK, mensaje de error si falla."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM despachantes WHERE id = ?", (despachante_id,))
        conn.commit()
        if cur.rowcount == 0:
            return "Despachante no encontrado"
        return None
    finally:
        conn.close()


def get_invoice_extras(user_id: int, qb_invoice_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """Obtiene los datos extra de invoice_extra para una lista de qb_invoice_id. Retorna {qb_invoice_id: {courier, guia, importe_factura, pa, estado, despachante, factura_courier}}."""
    if not qb_invoice_ids:
        return {}
    conn = get_connection()
    try:
        cur = conn.cursor()
        placeholders = ",".join("?" * len(qb_invoice_ids))
        cur.execute(
            f"SELECT qb_invoice_id, courier, guia, importe_factura, pa, estado, despachante, factura_courier FROM invoice_extra WHERE user_id = ? AND qb_invoice_id IN ({placeholders})",
            [user_id] + list(qb_invoice_ids),
        )
        return {str(r["qb_invoice_id"]): dict(r) for r in cur.fetchall()}
    finally:
        conn.close()


def upsert_invoice_extra(user_id: int, qb_invoice_id: str, **kwargs) -> None:
    """Inserta o actualiza fila en invoice_extra. Merge con valores actuales para actualizaciones parciales."""
    allowed = {"courier", "guia", "importe_factura", "pa", "estado", "despachante", "factura_courier"}
    kv = {k: str(v or "") if v is not None else "" for k, v in kwargs.items() if k in allowed}
    if not kv:
        return
    extras = get_invoice_extras(user_id, [qb_invoice_id])
    current = extras.get(qb_invoice_id, {})
    merged: Dict[str, str] = {}
    for k in allowed:
        if k == "factura_courier":
            merged[k] = str(current.get(k) or "Impaga")
        else:
            merged[k] = str(current.get(k) or "")
    merged.update(kv)
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO invoice_extra (user_id, qb_invoice_id, courier, guia, importe_factura, pa, estado, despachante, factura_courier)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(user_id, qb_invoice_id) DO UPDATE SET courier=excluded.courier, guia=excluded.guia, importe_factura=excluded.importe_factura, pa=excluded.pa, estado=excluded.estado, despachante=excluded.despachante, factura_courier=excluded.factura_courier""",
            (
                user_id,
                qb_invoice_id,
                merged["courier"],
                merged["guia"],
                merged["importe_factura"],
                merged["pa"],
                merged["estado"],
                merged["despachante"],
                merged["factura_courier"],
            ),
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# CRUD — Datos de cotizador e importación
# ---------------------------------------------------------------------------


def copy_cotizador_datos(from_user_id: int, to_user_id: int) -> int:
    """Copia todos los datos del cotizador de un usuario a otro. Devuelve cantidad de claves copiadas."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO cotizador_datos (user_id, clave, valor) SELECT ?, clave, valor FROM cotizador_datos WHERE user_id = ?",
            (to_user_id, from_user_id),
        )
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def get_importacion_filas(user_id: int) -> List[Dict[str, Any]]:
    """Obtiene las filas guardadas de Importación para el usuario, ordenadas por fila_orden."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT datos_json FROM importacion_filas WHERE user_id = ? ORDER BY fila_orden",
            (user_id,),
        )
        rows = []
        for row in cur.fetchall():
            try:
                rows.append(json.loads(row["datos_json"]))
            except (json.JSONDecodeError, TypeError):
                pass
        return rows
    finally:
        conn.close()


def save_importacion_filas(user_id: int, rows: List[Dict[str, Any]]) -> None:
    """Guarda las filas completas de Importación en la BD. Reemplaza el último guardado del usuario."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM importacion_filas WHERE user_id = ?", (user_id,))
        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        for i, row in enumerate(rows):
            cur.execute(
                "INSERT INTO importacion_filas (user_id, fila_orden, datos_json, created_at) VALUES (?, ?, ?, ?)",
                (user_id, i, json.dumps(row, ensure_ascii=False), now),
            )
        conn.commit()
    finally:
        conn.close()


def export_user_db_data(user_id: int) -> bytes:
    """Exporta datos operativos: cotizador, catálogo de productos, publicaciones ML, importación. NO incluye credenciales, password, app id, client secret."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        ahora = datetime.now()
        data: Dict[str, Any] = {
            "version": BACKUP_VERSION,
            "exported_at": ahora.isoformat(),
            "fecha_descarga": ahora.strftime("%Y-%m-%d %H:%M"),
            "user_id_original": user_id,
            "cotizador_datos": [],
            "productos": [],
            "ml_publicaciones": [],
            "importacion_filas": [],
        }
        cur.execute("SELECT clave, valor FROM cotizador_datos WHERE user_id = ?", (user_id,))
        data["cotizador_datos"] = [{"clave": r["clave"], "valor": r["valor"]} for r in cur.fetchall()]
        cur.execute("SELECT sku, costo_usd, tipo_iva, costo_updated_at FROM productos WHERE user_id = ?", (user_id,))
        data["productos"] = [{"sku": r["sku"], "costo_usd": r["costo_usd"], "tipo_iva": r["tipo_iva"], "costo_updated_at": r["costo_updated_at"]} for r in cur.fetchall()]
        cur.execute("SELECT ml_id, sku, titulo, precio, stock, estado, catalog_listing, listing_type_id, sold_quantity, ultima_sync FROM ml_publicaciones WHERE user_id = ?", (user_id,))
        data["ml_publicaciones"] = [dict(r) for r in cur.fetchall()]
        cur.execute("SELECT fila_orden, datos_json FROM importacion_filas WHERE user_id = ? ORDER BY fila_orden", (user_id,))
        data["importacion_filas"] = [{"fila_orden": r["fila_orden"], "datos_json": r["datos_json"]} for r in cur.fetchall()]
        return json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    finally:
        conn.close()


def import_user_db_data(user_id: int, content: bytes) -> str:
    """Importa datos operativos desde un backup JSON. Reemplaza cotizador, precios, importación. No toca credenciales ni contraseña."""
    try:
        data = json.loads(content.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        return f"Archivo inválido: {e}"
    if not isinstance(data, dict):
        return "El archivo debe contener un objeto JSON válido."
    version = data.get("version", 0)
    if version > BACKUP_VERSION:
        return f"El backup es de una versión más nueva ({version}). Actualizá la app."
    uid = int(user_id)
    conn = get_connection()
    try:
        cur = conn.cursor()
        # cotizador_datos: borrar todo del usuario e insertar backup
        cur.execute("DELETE FROM cotizador_datos WHERE user_id = ?", (uid,))
        for item in data.get("cotizador_datos") or []:
            if isinstance(item, dict) and item.get("clave") is not None:
                val = item.get("valor")
                val_str = val if val is not None else ""
                if isinstance(val_str, (dict, list)):
                    val_str = json.dumps(val_str, ensure_ascii=False)
                else:
                    val_str = str(val_str) if val_str != "" else ""
                cur.execute("INSERT INTO cotizador_datos (user_id, clave, valor) VALUES (?, ?, ?)", (uid, str(item["clave"]), val_str))
        now_imp = datetime.now().isoformat()
        if version >= 2:
            # productos: upsert por (sku, user_id)
            for item in data.get("productos") or []:
                if not isinstance(item, dict) or not item.get("sku"):
                    continue
                cur.execute(
                    """INSERT INTO productos (sku, user_id, costo_usd, tipo_iva, created_at, updated_at, costo_updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(sku, user_id) DO UPDATE SET
                           costo_usd=excluded.costo_usd,
                           tipo_iva=excluded.tipo_iva,
                           costo_updated_at=excluded.costo_updated_at,
                           updated_at=excluded.updated_at""",
                    (str(item["sku"]), uid,
                     item.get("costo_usd"), float(item.get("tipo_iva") or 0.105),
                     now_imp, now_imp, item.get("costo_updated_at") or now_imp),
                )
            # ml_publicaciones: upsert por (ml_id, user_id)
            for item in data.get("ml_publicaciones") or []:
                if not isinstance(item, dict) or not item.get("ml_id"):
                    continue
                cur.execute(
                    """INSERT INTO ml_publicaciones (ml_id, user_id, sku, titulo, precio, stock, estado, catalog_listing, listing_type_id, sold_quantity, ultima_sync)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(ml_id, user_id) DO UPDATE SET
                           sku=excluded.sku, titulo=excluded.titulo, precio=excluded.precio,
                           stock=excluded.stock, estado=excluded.estado,
                           catalog_listing=excluded.catalog_listing,
                           listing_type_id=excluded.listing_type_id,
                           sold_quantity=excluded.sold_quantity,
                           ultima_sync=excluded.ultima_sync""",
                    (str(item["ml_id"]), uid, item.get("sku"), item.get("titulo"),
                     item.get("precio"), item.get("stock"), item.get("estado"),
                     int(item.get("catalog_listing") or 0), item.get("listing_type_id"),
                     item.get("sold_quantity"), item.get("ultima_sync") or now_imp),
                )
        else:
            # Migración legacy version=1: precios_producto → productos via ml_publicaciones
            migrados = 0
            sin_sku = 0
            for item in data.get("precios_producto") or []:
                if not isinstance(item, dict) or not item.get("id"):
                    continue
                ml_id = str(item["id"])
                tipo_iva = float(item.get("tipo_iva") or 0.105)
                costo_u = float(item.get("costo_u") or 0)
                cur.execute(
                    "SELECT sku FROM ml_publicaciones WHERE ml_id = ? AND user_id = ?",
                    (ml_id, uid),
                )
                pub_row = cur.fetchone()
                if pub_row and pub_row["sku"]:
                    cur.execute(
                        """INSERT INTO productos (sku, user_id, costo_usd, tipo_iva, created_at, updated_at, costo_updated_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?)
                           ON CONFLICT(sku, user_id) DO UPDATE SET
                               costo_usd=excluded.costo_usd,
                               tipo_iva=excluded.tipo_iva,
                               costo_updated_at=excluded.costo_updated_at,
                               updated_at=excluded.updated_at""",
                        (pub_row["sku"], uid, costo_u, tipo_iva, now_imp, now_imp, now_imp),
                    )
                    migrados += 1
                else:
                    sin_sku += 1
            if sin_sku:
                conn.commit()
                return f"ok (legacy v1: {migrados} migrados, {sin_sku} sin SKU resuelto)"
        # importacion_filas: borrar todo del usuario e insertar backup
        cur.execute("DELETE FROM importacion_filas WHERE user_id = ?", (uid,))
        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        for item in sorted(data.get("importacion_filas") or [], key=lambda x: x.get("fila_orden", 0)):
            if isinstance(item, dict) and "datos_json" in item:
                dj = item["datos_json"]
                if isinstance(dj, (dict, list)):
                    dj = json.dumps(dj, ensure_ascii=False)
                else:
                    dj = str(dj)
                cur.execute("INSERT INTO importacion_filas (user_id, fila_orden, datos_json, created_at) VALUES (?, ?, ?, ?)", (uid, int(item.get("fila_orden", 0)), dj, now))
        conn.commit()
        return "ok"
    except Exception as e:
        conn.rollback()
        return str(e)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Historial de queries
# ---------------------------------------------------------------------------


def save_query(
    user_id: int,
    query_type: str,
    params: Dict[str, Any],
    raw_response: Optional[Dict[str, Any]] = None,
) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO queries (user_id, query_type, params, created_at, raw_response)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                user_id,
                query_type,
                json.dumps(params, ensure_ascii=False),
                datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
                json.dumps(raw_response, ensure_ascii=False) if raw_response else None,
            ),
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# CRUD — ARCA (datos fiscales manuales)
# ---------------------------------------------------------------------------


def get_arca_datos(bloque: str) -> Dict[str, str]:
    """Devuelve {campo: valor} para un bloque ARCA."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT campo, valor FROM arca_datos WHERE bloque = ?", (bloque,))
        return {r["campo"]: (r["valor"] or "") for r in cur.fetchall()}
    finally:
        conn.close()


def save_arca_datos(bloque: str, datos: Dict[str, str]) -> None:
    """Guarda o actualiza los campos de un bloque ARCA."""
    now = datetime.now().isoformat(timespec="seconds")
    conn = get_connection()
    try:
        cur = conn.cursor()
        for campo, valor in datos.items():
            cur.execute(
                "INSERT INTO arca_datos (bloque, campo, valor, updated_at) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(bloque, campo) DO UPDATE SET valor=excluded.valor, updated_at=excluded.updated_at",
                (bloque, campo, str(valor), now),
            )
        conn.commit()
    finally:
        conn.close()


def get_arca_multilateral() -> List[Dict[str, Any]]:
    """Devuelve todas las filas del Convenio Multilateral."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, provincia, alicuota, a_favor_contrib, a_favor_fisco, a_pagar, updated_at "
            "FROM arca_multilateral ORDER BY id"
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def save_arca_multilateral(filas: List[Dict[str, Any]]) -> None:
    """Reemplaza todas las filas del Convenio Multilateral."""
    now = datetime.now().isoformat(timespec="seconds")
    conn = get_connection()

    def _f(v: Any) -> float:
        try:
            return float(v or 0)
        except (ValueError, TypeError):
            return 0.0

    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM arca_multilateral")
        for f in filas:
            cur.execute(
                "INSERT INTO arca_multilateral "
                "(provincia, alicuota, a_favor_contrib, a_favor_fisco, a_pagar, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    str(f.get("provincia") or ""),
                    _f(f.get("alicuota")),
                    _f(f.get("a_favor_contrib")),
                    _f(f.get("a_favor_fisco")),
                    _f(f.get("a_pagar")),
                    now,
                ),
            )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Valores por defecto del cotizador
# (fallback cuando get_cotizador_param() devuelve None)
# ---------------------------------------------------------------------------
COTIZADOR_DEFAULTS = {
    "dolar_oficial": "1475", "dolar_blue": "1450", "dolar_sistema": "1500", "dolar_despacho": "1475",
    "kilo": "60", "iva_105": "0.105", "iva_21": "0.21", "iibb_lhs": "0.03",
    "ml_comision": "0.15", "ml_debcre": "0.006", "ml_sirtac": "0.008", "ml_envios": "5823",
    "ml_iibb_per": "0.055", "ml_envios_gratuitos": "33000", "ml_comision_fija_menor": "2800", "ml_cobrado": "0.836",
    "ml_3cuotas": "1.12149", "ml_6cuotas": "1.21067",
    "ml_ganancia_neta_venta": "0.1000",
    "cuotas_3x": "0.094", "cuotas_6x": "0.151", "cuotas_9x": "0.207", "cuotas_12x": "0.259",
    "valor_kg_miami": "13.5", "almacenaje_miami_x2": "1.8", "dias_almacenaje_miami": "2", "almacenaje_dias_kg_miami": "0.9",
    "seguro_miami": "24.75", "descuento_lhs_kg": "1.33267522",
    "valor_kg_china": "27", "almacenaje_china_x3": "2.7", "dias_almacenaje_china": "3", "almacenaje_dias_kg_china": "0.9",
    "seguro_china": "29.35", "res_3244": "10", "gastos_operativos": "27", "gastos_origen": "0",
    "envio_domicilio": "10", "ajuste_valor_ana": "1.01",
}
