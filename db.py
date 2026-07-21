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
    ("catalogos", "Catálogos"),
    ("busqueda", "Busquedas"),
    ("balance", "Balance"),
    ("dashboard", "Dashboard"),
    ("compras", "Invoices"),
    ("stock", "Stock"),
    ("compras_lista", "Compras"),
    ("pedidos", "Pedidos"),
    ("historicos", "Históricos"),
    ("importacion", "Importacion"),
    ("pesos", "Pesos"),
    ("gastos", "Gastos"),
    ("analisis_ml", "Análisis ML"),
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


def init_competidores_snapshots_db() -> None:
    """Crea la tabla competidores_snapshots si no existe (snapshots diarios de competidores por catálogo, para ranking por período)."""
    conn = get_connection()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS competidores_snapshots (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id             INTEGER NOT NULL,
            catalog_product_id  TEXT NOT NULL,
            seller_id           TEXT NOT NULL,
            seller_nickname     TEXT,
            seller_total_ventas INTEGER,
            seller_level_id     TEXT,
            seller_power_status TEXT,
            price               REAL,
            item_id             TEXT,
            snapshot_date       DATE NOT NULL,
            created_at          DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_comp_snapshot
        ON competidores_snapshots(user_id, catalog_product_id, seller_id, snapshot_date)
        """
    )
    conn.commit()
    conn.close()


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

    # Migración: datos fiscales de MercadoLibre (company.corporate_name / brand_name / identification)
    for col in ("ml_cuit", "ml_doc_type", "ml_razon_social", "ml_nombre_fantasia", "ml_cust_type_id", "ml_billing_updated_at"):
        if col not in user_cols:
            cur.execute(f"ALTER TABLE users ADD COLUMN {col} TEXT")

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

    # Migration: agregar ml_nickname a ml_credentials
    cur.execute("PRAGMA table_info(ml_credentials)")
    if "ml_nickname" not in [r[1] for r in cur.fetchall()]:
        cur.execute("ALTER TABLE ml_credentials ADD COLUMN ml_nickname TEXT")

    # Registro de actividad de usuarios
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS activity_log (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario          TEXT NOT NULL,
            ml_username      TEXT,
            tab              TEXT NOT NULL,
            accion           TEXT NOT NULL,
            detalle          TEXT,
            tiempo_segundos  INTEGER,
            timestamp        TEXT NOT NULL
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
    if "gan_pesos" not in _prod_cols:
        cur.execute("ALTER TABLE productos ADD COLUMN gan_pesos REAL DEFAULT NULL")
    if "gan_pct" not in _prod_cols:
        cur.execute("ALTER TABLE productos ADD COLUMN gan_pct REAL DEFAULT NULL")
    if "stock" not in _prod_cols:
        cur.execute("ALTER TABLE productos ADD COLUMN stock INTEGER DEFAULT NULL")
    if "catalog_status" not in _prod_cols:
        cur.execute("ALTER TABLE productos ADD COLUMN catalog_status TEXT DEFAULT NULL")

    # Override de marca: corrige marcas mal cargadas en ML que no se pueden editar ahí
    # (ej. "AfterShokz" -> "Shokz"). Se aplica antes de grabar productos.marca en
    # cualquier flujo que la escriba (auto-populate, backfill), no como filtro de lectura.
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS marcas_override (
            marca_ml    TEXT NOT NULL,
            marca_real  TEXT NOT NULL,
            user_id     INTEGER NOT NULL,
            PRIMARY KEY (marca_ml, user_id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
        """
    )

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
            cuotas        TEXT,
            costo_pesos   REAL,
            costo_fijo    REAL,
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
    if "cuotas" not in vd_cols:
        cur.execute("ALTER TABLE ventas_datos ADD COLUMN cuotas TEXT")
    if "costo_pesos" not in vd_cols:
        cur.execute("ALTER TABLE ventas_datos ADD COLUMN costo_pesos REAL")
    if "costo_fijo" not in vd_cols:
        cur.execute("ALTER TABLE ventas_datos ADD COLUMN costo_fijo REAL")
    if "fee_origen" not in vd_cols:
        cur.execute("ALTER TABLE ventas_datos ADD COLUMN fee_origen TEXT DEFAULT 'api'")

    # Migración: agregar columna despachante a invoice_extra si no existe (tablas antiguas)
    cur.execute("PRAGMA table_info(invoice_extra)")
    inv_extra_cols = [r[1] for r in cur.fetchall()]
    if "despachante" not in inv_extra_cols:
        cur.execute("ALTER TABLE invoice_extra ADD COLUMN despachante TEXT")
    if "pa" not in inv_extra_cols:
        cur.execute("ALTER TABLE invoice_extra ADD COLUMN pa TEXT")
    if "factura_courier" not in inv_extra_cols:
        cur.execute("ALTER TABLE invoice_extra ADD COLUMN factura_courier TEXT")

    # ARCA: datos fiscales manuales por bloque (por usuario)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS arca_datos (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id    INTEGER NOT NULL DEFAULT 1,
            bloque     TEXT NOT NULL,
            campo      TEXT NOT NULL,
            valor      TEXT,
            updated_at TEXT,
            UNIQUE(user_id, bloque, campo)
        )
        """
    )
    # Migración: si la tabla ya existía sin user_id, recrear con constraint correcto
    cur.execute("PRAGMA table_info(arca_datos)")
    _ad_cols = [r[1] for r in cur.fetchall()]
    if "user_id" not in _ad_cols:
        cur.execute("ALTER TABLE arca_datos RENAME TO arca_datos_old")
        cur.execute(
            """
            CREATE TABLE arca_datos (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    INTEGER NOT NULL DEFAULT 1,
                bloque     TEXT NOT NULL,
                campo      TEXT NOT NULL,
                valor      TEXT,
                updated_at TEXT,
                UNIQUE(user_id, bloque, campo)
            )
            """
        )
        cur.execute(
            "INSERT INTO arca_datos (user_id, bloque, campo, valor, updated_at) "
            "SELECT 1, bloque, campo, valor, updated_at FROM arca_datos_old"
        )
        cur.execute("DROP TABLE arca_datos_old")

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
    # Migración: agregar user_id a arca_multilateral si no existe
    cur.execute("PRAGMA table_info(arca_multilateral)")
    _am_cols = [r[1] for r in cur.fetchall()]
    if "user_id" not in _am_cols:
        cur.execute("ALTER TABLE arca_multilateral ADD COLUMN user_id INTEGER NOT NULL DEFAULT 1")

    # Migración: dar permisos por defecto a usuarios existentes (admin para el usuario con id más bajo)
    cur.execute("SELECT MIN(id) FROM users")
    _admin_uid = cur.fetchone()[0] or 1
    cur.execute("SELECT id FROM users ORDER BY id")
    for row in cur.fetchall():
        uid = row["id"]
        for tab_key in ("home", "estadisticas", "ventas", "productos", "cuotas", "catalogos", "busqueda", "balance", "dashboard", "compras", "stock", "compras_lista", "pedidos", "importacion", "pesos", "arca", "datos", "configuracion", "admin", "actividad"):
            can = 1 if tab_key not in ("admin", "actividad") or uid == _admin_uid else 0
            cur.execute(
                "INSERT OR IGNORE INTO user_tab_permissions (user_id, tab_key, can_access) VALUES (?, ?, ?)",
                (uid, tab_key, can),
            )

    # Costos de financiación de cuotas de MercadoLibre (tabla de sistema)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS financiacion_cuotas_ml (
            cuotas INTEGER PRIMARY KEY,
            costo_financiacion REAL NOT NULL,
            fecha_modificacion TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        INSERT OR IGNORE INTO financiacion_cuotas_ml (cuotas, costo_financiacion, fecha_modificacion) VALUES
            (3, 0.084, '1976-12-23'),
            (6, 0.123, '1976-12-23'),
            (9, 0.157, '1976-12-23'),
            (12, 0.192, '1976-12-23')
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS app_config (
            key        TEXT PRIMARY KEY,
            value      TEXT,
            updated_at TEXT
        )
        """
    )

    # Catálogos ML asociados manualmente a cada SKU
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sku_catalogos (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id            INTEGER NOT NULL DEFAULT 1,
            sku                TEXT NOT NULL,
            catalog_product_id TEXT NOT NULL,
            catalog_name       TEXT,
            activo             INTEGER DEFAULT 1,
            agregado_at        TEXT,
            UNIQUE(user_id, sku, catalog_product_id),
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
        """
    )

    # Migration: si la tabla ya existía sin user_id, recrearla con user_id
    _sc_cols = [r[1] for r in conn.execute("PRAGMA table_info(sku_catalogos)").fetchall()]
    if "user_id" not in _sc_cols:
        conn.execute(
            """
            CREATE TABLE sku_catalogos_new (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id            INTEGER NOT NULL DEFAULT 1,
                sku                TEXT NOT NULL,
                catalog_product_id TEXT NOT NULL,
                catalog_name       TEXT,
                activo             INTEGER DEFAULT 1,
                agregado_at        TEXT,
                UNIQUE(user_id, sku, catalog_product_id),
                FOREIGN KEY (user_id) REFERENCES users (id)
            )
            """
        )
        conn.execute(
            "INSERT INTO sku_catalogos_new (id, sku, catalog_product_id, catalog_name, activo, agregado_at) "
            "SELECT id, sku, catalog_product_id, catalog_name, activo, agregado_at FROM sku_catalogos"
        )
        conn.execute("DROP TABLE sku_catalogos")
        conn.execute("ALTER TABLE sku_catalogos_new RENAME TO sku_catalogos")
        conn.commit()

    init_competidores_snapshots_db()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS comparador_competidores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            seller_id TEXT NOT NULL,
            seller_nickname TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, seller_id)
        )
        """
    )

    # Cache de competidores por catálogo ML
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS catalogo_competidores (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            catalog_product_id TEXT NOT NULL,
            item_id            TEXT NOT NULL,
            seller_id          TEXT,
            seller_nickname    TEXT,
            price              REAL,
            listing_type       TEXT,
            logistica          TEXT,
            free_shipping      INTEGER,
            updated_at         TEXT,
            UNIQUE(catalog_product_id, item_id)
        )
        """
    )

    # Migration: agregar columna origen a catalogo_competidores si no existe
    try:
        cur.execute("ALTER TABLE catalogo_competidores ADD COLUMN origen TEXT DEFAULT 'local'")
    except sqlite3.OperationalError:
        pass
    try:
        cur.execute("ALTER TABLE catalogo_competidores ADD COLUMN seller_level_id TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        cur.execute("ALTER TABLE catalogo_competidores ADD COLUMN seller_power_status TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        cur.execute("ALTER TABLE catalogo_competidores ADD COLUMN seller_total_ventas INTEGER")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE catalogo_competidores ADD COLUMN seller_ventas_60d INTEGER")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE catalogo_competidores ADD COLUMN seller_period_60d TEXT")
    except sqlite3.OperationalError:
        pass

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ml_orders_cache (
            order_id      TEXT NOT NULL,
            user_id       INTEGER NOT NULL,
            date_created  TEXT,
            date_closed   TEXT,
            total_amount  REAL,
            paid_amount   REAL,
            status        TEXT,
            items_json    TEXT,
            payments_json TEXT,
            PRIMARY KEY (order_id, user_id)
        )
        """
    )

    # Archivos de gastos impositivos por período y sección
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS gastos_archivos (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER NOT NULL,
            periodo     TEXT NOT NULL,
            seccion     TEXT NOT NULL,
            filename    TEXT NOT NULL,
            filepath    TEXT NOT NULL,
            size_bytes  INTEGER NOT NULL DEFAULT 0,
            uploaded_at TIMESTAMP NOT NULL,
            procesado   BOOLEAN NOT NULL DEFAULT 0,
            procesado_at TIMESTAMP
        )
        """
    )

    # Migración: nuevas columnas Gemini en gastos_archivos
    for _col_sql in [
        "ALTER TABLE gastos_archivos ADD COLUMN extracted_data TEXT",
        "ALTER TABLE gastos_archivos ADD COLUMN prompt_used TEXT",
        "ALTER TABLE gastos_archivos ADD COLUMN extraction_status TEXT DEFAULT 'pendiente'",
        "ALTER TABLE gastos_archivos ADD COLUMN extraction_error TEXT",
    ]:
        try:
            cur.execute(_col_sql)
        except Exception:
            pass  # columna ya existe

    # Tabla de prompts custom por sección
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS gastos_prompts (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id    INTEGER NOT NULL,
            seccion    TEXT NOT NULL,
            prompt     TEXT NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, seccion)
        )
        """
    )

    # Resultado guardado del análisis consolidado del período (tab Gastos)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS gastos_consolidado (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id        INTEGER NOT NULL,
            periodo        TEXT NOT NULL,
            resultado_json TEXT NOT NULL,
            generado_at    TIMESTAMP NOT NULL,
            UNIQUE(user_id, periodo)
        )
        """
    )

    # Migración: habilitar permiso "guias" para user_id=1 (admin) si aún no tiene registro
    cur.execute("SELECT 1 FROM users WHERE id = 1")
    if cur.fetchone():
        cur.execute(
            "INSERT OR IGNORE INTO user_tab_permissions (user_id, tab_key, can_access) VALUES (1, 'guias', 1)"
        )
        cur.execute(
            "INSERT OR IGNORE INTO user_tab_permissions (user_id, tab_key, can_access) VALUES (1, 'gastos', 1)"
        )
        cur.execute(
            "INSERT OR IGNORE INTO user_tab_permissions (user_id, tab_key, can_access) VALUES (1, 'analisis_ml', 1)"
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


def get_ml_nickname(user_id: int) -> Optional[str]:
    """Obtiene el nickname de MercadoLibre del usuario."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT ml_nickname FROM ml_credentials WHERE user_id = ? ORDER BY id DESC LIMIT 1",
            (user_id,),
        )
        row = cur.fetchone()
        return row["ml_nickname"] if row and row["ml_nickname"] else None
    finally:
        conn.close()


def set_ml_nickname(user_id: int, nickname: str) -> None:
    """Guarda el nickname de MercadoLibre en ml_credentials."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE ml_credentials SET ml_nickname = ? WHERE user_id = ?",
            (nickname, user_id),
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# CRUD — Catálogos ML (sku_catalogos + catalogo_competidores)
# ---------------------------------------------------------------------------


def get_sku_catalogos(user_id: int) -> List[Dict]:
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM sku_catalogos WHERE user_id=? ORDER BY sku, id",
            (user_id,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def add_sku_catalogo(user_id: int, sku: str, catalog_product_id: str, catalog_name: str = "") -> bool:
    """Agrega asociación SKU-catálogo. Retorna True si se insertó, False si ya existía."""
    now = datetime.now(timezone.utc).isoformat()
    conn = get_connection()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO sku_catalogos (user_id, sku, catalog_product_id, catalog_name, activo, agregado_at) VALUES (?, ?, ?, ?, 1, ?)",
            (user_id, sku.strip(), catalog_product_id.strip().upper(), catalog_name.strip(), now),
        )
        conn.commit()
        return conn.execute(
            "SELECT changes()"
        ).fetchone()[0] > 0
    finally:
        conn.close()


def update_sku_catalogo_name(id_: int, catalog_name: str, user_id: int) -> None:
    conn = get_connection()
    try:
        conn.execute("UPDATE sku_catalogos SET catalog_name=? WHERE id=? AND user_id=?", (catalog_name, id_, user_id))
        conn.commit()
    finally:
        conn.close()


def set_sku_catalogo_activo(id_: int, activo: int, user_id: int) -> None:
    conn = get_connection()
    try:
        conn.execute("UPDATE sku_catalogos SET activo=? WHERE id=? AND user_id=?", (activo, id_, user_id))
        conn.commit()
    finally:
        conn.close()


def delete_sku_catalogo(id_: int, user_id: int) -> None:
    conn = get_connection()
    try:
        conn.execute("DELETE FROM sku_catalogos WHERE id=? AND user_id=?", (id_, user_id))
        conn.commit()
    finally:
        conn.close()


def get_catalogo_competidores(catalog_product_id: str) -> List[Dict]:
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM catalogo_competidores WHERE catalog_product_id=? ORDER BY price",
            (catalog_product_id,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def upsert_catalogo_competidores(catalog_product_id: str, items: List[Dict]) -> None:
    """Reemplaza todos los competidores del catálogo con la lista recibida."""
    now = datetime.now(timezone.utc).isoformat()
    conn = get_connection()
    try:
        conn.execute("DELETE FROM catalogo_competidores WHERE catalog_product_id=?", (catalog_product_id,))
        for it in items:
            ship = it.get("shipping") or {}
            tags = it.get("tags", [])
            intl_mode = it.get("international_delivery_mode", "") or ship.get("international_delivery_mode", "")
            es_internacional = (
                "cbt_item" in tags
                or "cbt_fulfillment_us" in tags
                or (intl_mode and intl_mode != "none")
            )
            origen = "internacional" if es_internacional else "local"
            conn.execute(
                """INSERT INTO catalogo_competidores
                   (catalog_product_id, item_id, seller_id, seller_nickname, price,
                    listing_type, logistica, free_shipping, updated_at, origen,
                    seller_level_id, seller_power_status, seller_total_ventas,
                    seller_ventas_60d, seller_period_60d)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    catalog_product_id,
                    it.get("item_id", ""),
                    str(it.get("seller_id", "")),
                    it.get("seller_nickname", ""),
                    it.get("price"),
                    it.get("listing_type_id", ""),
                    ship.get("logistic_type", "") if isinstance(ship, dict) else "",
                    1 if (isinstance(ship, dict) and ship.get("free_shipping")) else 0,
                    now,
                    origen,
                    it.get("seller_level_id") or None,
                    it.get("seller_power_status") or None,
                    it.get("seller_total_ventas") if it.get("seller_total_ventas") is not None else None,
                    it.get("seller_ventas_60d") if it.get("seller_ventas_60d") is not None else None,
                    it.get("seller_period_60d") or None,
                ),
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


def get_user_ml_razon_social(user_id: int) -> Optional[str]:
    """Lee ml_razon_social directo de la tabla users (valor fresco, no el de la sesión)."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT ml_razon_social FROM users WHERE id = ?", (user_id,))
        row = cur.fetchone()
        return row["ml_razon_social"] if row and row["ml_razon_social"] else None
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
        _default_off = {"admin", "guias", "analisis_ml"}
        for tab_key, _ in TAB_KEYS:
            if tab_key not in result:
                result[tab_key] = False if tab_key in _default_off else True
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


def get_marca_override_map(user_id: int) -> Dict[str, str]:
    """Mapa marca_ml -> marca_real para corregir marcas mal cargadas en ML que no se
    pueden editar ahí (ej. "AfterShokz" -> "Shokz"). Aplicar SIEMPRE antes de grabar
    productos.marca, en cualquier flujo que la escriba."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT marca_ml, marca_real FROM marcas_override WHERE user_id=?",
            (user_id,),
        )
        return {r["marca_ml"]: r["marca_real"] for r in cur.fetchall()}
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


def get_arca_datos(bloque: str, user_id: int = 1) -> Dict[str, str]:
    """Devuelve {campo: valor} para un bloque ARCA del usuario."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT campo, valor FROM arca_datos WHERE user_id = ? AND bloque = ?",
            (user_id, bloque),
        )
        return {r["campo"]: (r["valor"] or "") for r in cur.fetchall()}
    finally:
        conn.close()


def save_arca_datos(bloque: str, datos: Dict[str, str], user_id: int = 1) -> None:
    """Guarda o actualiza los campos de un bloque ARCA para el usuario."""
    now = datetime.now().isoformat(timespec="seconds")
    conn = get_connection()
    try:
        cur = conn.cursor()
        for campo, valor in datos.items():
            cur.execute(
                "INSERT INTO arca_datos (user_id, bloque, campo, valor, updated_at) VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(user_id, bloque, campo) DO UPDATE SET valor=excluded.valor, updated_at=excluded.updated_at",
                (user_id, bloque, campo, str(valor), now),
            )
        conn.commit()
    finally:
        conn.close()


def get_arca_multilateral(user_id: int = 1) -> List[Dict[str, Any]]:
    """Devuelve todas las filas del Convenio Multilateral del usuario."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, provincia, alicuota, a_favor_contrib, a_favor_fisco, a_pagar, updated_at "
            "FROM arca_multilateral WHERE user_id = ? ORDER BY id",
            (user_id,),
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def save_arca_multilateral(filas: List[Dict[str, Any]], user_id: int = 1) -> None:
    """Reemplaza todas las filas del Convenio Multilateral del usuario."""
    now = datetime.now().isoformat(timespec="seconds")
    conn = get_connection()

    def _f(v: Any) -> float:
        try:
            return float(v or 0)
        except (ValueError, TypeError):
            return 0.0

    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM arca_multilateral WHERE user_id = ?", (user_id,))
        for f in filas:
            cur.execute(
                "INSERT INTO arca_multilateral "
                "(user_id, provincia, alicuota, a_favor_contrib, a_favor_fisco, a_pagar, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    user_id,
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
# CRUD — Gastos archivos
# ---------------------------------------------------------------------------


def get_gastos_archivos(user_id: int, periodo: str, seccion: str) -> List[Dict[str, Any]]:
    """Devuelve archivos de una sección/período ordenados por fecha de subida."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM gastos_archivos WHERE user_id=? AND periodo=? AND seccion=? ORDER BY uploaded_at",
            (user_id, periodo, seccion),
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


_GASTOS_TOTAL_SECCIONES = 6  # facturas_ml, retenciones, percepciones, pagos_arca, reportes_ml, analisis_ml


def calcular_estados_anio(user_id: int, year: int) -> Dict[int, Dict[str, Any]]:
    """calcular_estado_mes() para los 12 meses de un año en 1 sola query agrupada
    (evita 12 round-trips al armar el selector de período con semáforo por mes)."""
    hoy = datetime.now()
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT periodo, COUNT(DISTINCT seccion) FROM gastos_archivos "
            "WHERE user_id=? AND periodo LIKE ? AND extraction_status='procesado' "
            "GROUP BY periodo",
            (user_id, f"{year:04d}-%"),
        )
        procesadas_por_mes = {int(periodo.split("-")[1]): n for periodo, n in cur.fetchall()}
    finally:
        conn.close()

    resultado: Dict[int, Dict[str, Any]] = {}
    for month in range(1, 13):
        es_futuro = (year, month) > (hoy.year, hoy.month)
        procesadas = 0 if es_futuro else procesadas_por_mes.get(month, 0)
        if es_futuro:
            estado, label = "futuro", "Futuro"
        elif procesadas == 0:
            estado, label = "vacio", "Sin archivos"
        elif procesadas == _GASTOS_TOTAL_SECCIONES:
            estado, label = "completo", "Completo"
        else:
            estado, label = "parcial", f"{procesadas} de {_GASTOS_TOTAL_SECCIONES}"
        resultado[month] = {
            "total_secciones": _GASTOS_TOTAL_SECCIONES, "secciones_procesadas": procesadas,
            "estado": estado, "es_futuro": es_futuro, "label": label,
        }
    return resultado


def calcular_estado_mes(user_id: int, year: int, month: int) -> Dict[str, Any]:
    """Estado de procesamiento de un mes: cuántas de las 6 secciones de Gastos tienen
    al menos 1 archivo con extraction_status='procesado'. 'futuro' si el período es
    posterior al mes actual."""
    return calcular_estados_anio(user_id, year)[month]


def insert_gastos_archivo(
    user_id: int, periodo: str, seccion: str,
    filename: str, filepath: str, size_bytes: int,
) -> int:
    """Inserta un nuevo archivo y devuelve su id."""
    conn = get_connection()
    try:
        now = datetime.now().isoformat(timespec="seconds")
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO gastos_archivos (user_id, periodo, seccion, filename, filepath, size_bytes, uploaded_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (user_id, periodo, seccion, filename, filepath, size_bytes, now),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def delete_gastos_archivo(file_id: int) -> None:
    """Elimina un registro de archivo."""
    conn = get_connection()
    try:
        conn.execute("DELETE FROM gastos_archivos WHERE id=?", (file_id,))
        conn.commit()
    finally:
        conn.close()


def mark_gastos_procesado(file_id: int) -> None:
    """Marca un archivo como procesado."""
    conn = get_connection()
    try:
        now = datetime.now().isoformat(timespec="seconds")
        conn.execute(
            "UPDATE gastos_archivos SET procesado=1, procesado_at=?, extraction_status='procesado' WHERE id=?",
            (now, file_id),
        )
        conn.commit()
    finally:
        conn.close()


def update_gastos_extraccion(
    file_id: int,
    extracted_data: Optional[str],
    prompt_used: Optional[str],
    extraction_status: str,
    extraction_error: Optional[str] = None,
) -> None:
    """Guarda resultado de extracción Gemini; si status='procesado' también marca procesado=1."""
    conn = get_connection()
    try:
        now = datetime.now().isoformat(timespec="seconds")
        if extraction_status == "procesado":
            conn.execute(
                "UPDATE gastos_archivos SET extracted_data=?, prompt_used=?, extraction_status=?, "
                "extraction_error=?, procesado=1, procesado_at=? WHERE id=?",
                (extracted_data, prompt_used, extraction_status, extraction_error, now, file_id),
            )
        else:
            conn.execute(
                "UPDATE gastos_archivos SET extracted_data=?, prompt_used=?, extraction_status=?, "
                "extraction_error=? WHERE id=?",
                (extracted_data, prompt_used, extraction_status, extraction_error, file_id),
            )
        conn.commit()
    finally:
        conn.close()


def save_gastos_consolidado(user_id: int, periodo: str, resultado: Dict[str, Any]) -> None:
    """Guarda (o reemplaza) el resultado del análisis consolidado del período."""
    conn = get_connection()
    try:
        now = datetime.now().isoformat(timespec="seconds")
        conn.execute(
            "INSERT INTO gastos_consolidado (user_id, periodo, resultado_json, generado_at) "
            "VALUES (?,?,?,?) "
            "ON CONFLICT(user_id, periodo) DO UPDATE SET "
            "resultado_json=excluded.resultado_json, generado_at=excluded.generado_at",
            (user_id, periodo, json.dumps(resultado), now),
        )
        conn.commit()
    finally:
        conn.close()


def get_gastos_consolidado(user_id: int, periodo: str) -> Optional[Dict[str, Any]]:
    """Devuelve el último análisis consolidado guardado para el período, o None."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT resultado_json, generado_at FROM gastos_consolidado WHERE user_id=? AND periodo=?",
            (user_id, periodo),
        )
        row = cur.fetchone()
        if not row:
            return None
        resultado = json.loads(row["resultado_json"])
        resultado["_generado_at"] = row["generado_at"]
        return resultado
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
    "ml_ganancia_neta_venta": "0.1000",
    "cuotas_3x": "0.094", "cuotas_6x": "0.151", "cuotas_9x": "0.207", "cuotas_12x": "0.259",
    "valor_kg_miami": "13.5", "almacenaje_miami_x2": "1.8", "dias_almacenaje_miami": "2", "almacenaje_dias_kg_miami": "0.9",
    "seguro_miami": "24.75", "descuento_lhs_kg": "1.33267522",
    "valor_kg_china": "27", "almacenaje_china_x3": "2.7", "dias_almacenaje_china": "3", "almacenaje_dias_kg_china": "0.9",
    "seguro_china": "29.35", "res_3244": "10", "gastos_operativos": "27", "gastos_origen": "0",
    "envio_domicilio": "10", "ajuste_valor_ana": "1.01",
}


def get_financiacion_cuotas_ml() -> Dict[int, Dict]:
    """Devuelve {3: {"pct": 0.084, "fecha": "2026-06-08"}, 6: {...}, ...}"""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT cuotas, costo_financiacion, fecha_modificacion FROM financiacion_cuotas_ml ORDER BY cuotas")
        return {
            int(r["cuotas"]): {"pct": float(r["costo_financiacion"]), "fecha": str(r["fecha_modificacion"] or "")}
            for r in cur.fetchall()
        }
    finally:
        conn.close()


def get_app_config(key: str) -> Optional[str]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT value FROM app_config WHERE key = ?", (key,))
        row = cur.fetchone()
        return row["value"] if row else None
    finally:
        conn.close()


def set_app_config(key: str, value: str) -> None:
    from datetime import datetime as _dt
    conn = get_connection()
    try:
        conn.execute(
            """
            INSERT INTO app_config (key, value, updated_at) VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
            """,
            (key, value, _dt.utcnow().isoformat()),
        )
        conn.commit()
    finally:
        conn.close()


def get_cached(key: str, max_age_minutes: int) -> Optional[Any]:
    from datetime import datetime as _dt
    import json as _json
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT value, updated_at FROM app_config WHERE key = ?", (key,))
        row = cur.fetchone()
        if not row or not row["updated_at"]:
            return None
        age = (_dt.utcnow() - _dt.fromisoformat(row["updated_at"])).total_seconds() / 60
        if age > max_age_minutes:
            return None
        return _json.loads(row["value"])
    except Exception:
        return None
    finally:
        conn.close()


def set_cached(key: str, value: Any) -> None:
    import json as _json
    set_app_config(key, _json.dumps(value))


def get_cache_age_minutes(key: str) -> Optional[float]:
    """Antigüedad en minutos del valor cacheado bajo `key`, o None si no hay ningún valor
    cacheado todavía. Usado para mostrar honestamente "actualizado hace X min" en la UI
    cuando se sirve un dato stale-while-revalidate."""
    from datetime import datetime as _dt
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT updated_at FROM app_config WHERE key = ?", (key,))
        row = cur.fetchone()
        if not row or not row["updated_at"]:
            return None
        return (_dt.utcnow() - _dt.fromisoformat(row["updated_at"])).total_seconds() / 60
    except Exception:
        return None
    finally:
        conn.close()


def get_cached_stale_ok(key: str, max_age_minutes: Optional[int] = None) -> Optional[Any]:
    """Como get_cached pero sin chequear el TTL fresh — devuelve el valor cacheado si existe el
    registro. Usado para stale-while-revalidate: servir datos viejos mientras se refresca en bg.
    max_age_minutes=None (default, usado por ml_get_my_items): sin techo, devuelve el valor sin
    importar su antigüedad. Si se pasa un valor, actúa como techo de "stale" — más viejo que eso
    ya no se considera servible y devuelve None (para forzar una llamada bloqueante en el
    llamador en vez de servir un dato demasiado viejo)."""
    import json as _json
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT value, updated_at FROM app_config WHERE key = ?", (key,))
        row = cur.fetchone()
        if not row or not row["value"]:
            return None
        if max_age_minutes is not None and row["updated_at"]:
            from datetime import datetime as _dt
            age = (_dt.utcnow() - _dt.fromisoformat(row["updated_at"])).total_seconds() / 60
            if age > max_age_minutes:
                return None
        return _json.loads(row["value"])
    except Exception:
        return None
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Cache de órdenes ML
# ---------------------------------------------------------------------------


def get_orders_cache(user_id: int) -> List[Dict]:
    import json as _json
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM ml_orders_cache WHERE user_id = ? ORDER BY date_created DESC",
            (user_id,),
        ).fetchall()
        result = []
        for r in rows:
            order = dict(r)
            order["order_items"] = _json.loads(order.pop("items_json") or "[]")
            order["payments"] = _json.loads(order.pop("payments_json") or "[]")
            result.append(order)
        return result
    finally:
        conn.close()


def get_orders_cache_max_date(user_id: int) -> Optional[str]:
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT MAX(date_created) FROM ml_orders_cache WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        return row[0] if row and row[0] else None
    finally:
        conn.close()


def upsert_orders_cache(user_id: int, orders: List[Dict]) -> None:
    import json as _json
    conn = get_connection()
    try:
        for o in orders:
            oid = str(o.get("id") or o.get("order_id") or "")
            if not oid:
                continue
            items = o.get("order_items") or o.get("items") or []
            pays = o.get("payments") or []
            conn.execute(
                """
                INSERT OR REPLACE INTO ml_orders_cache
                    (order_id, user_id, date_created, date_closed, total_amount, paid_amount,
                     status, items_json, payments_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    oid, user_id,
                    o.get("date_created"), o.get("date_closed"),
                    o.get("total_amount"), o.get("paid_amount"),
                    o.get("status"),
                    _json.dumps(items), _json.dumps(pays),
                ),
            )
        conn.commit()
    finally:
        conn.close()


def get_gastos_prompt(user_id: int, seccion: str) -> Optional[str]:
    """Retorna el prompt custom guardado para (user_id, seccion), o None si no existe."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT prompt FROM gastos_prompts WHERE user_id=? AND seccion=?",
            (user_id, seccion),
        ).fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def upsert_gastos_prompt(user_id: int, seccion: str, prompt: str) -> None:
    """Guarda o actualiza el prompt custom para (user_id, seccion)."""
    conn = get_connection()
    try:
        now = datetime.now().isoformat(timespec="seconds")
        conn.execute(
            "INSERT INTO gastos_prompts (user_id, seccion, prompt, updated_at) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(user_id, seccion) DO UPDATE SET prompt=excluded.prompt, updated_at=excluded.updated_at",
            (user_id, seccion, prompt, now),
        )
        conn.commit()
    finally:
        conn.close()
