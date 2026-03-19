from __future__ import annotations

import asyncio

# Polyfill asyncio.to_thread para Python 3.8 (agregado en 3.9). Evita AttributeError en Históricos y otras búsquedas.
if not hasattr(asyncio, "to_thread"):
    def _to_thread_compat(fn, *args, **kwargs):
        import functools
        loop = asyncio.get_running_loop()
        return loop.run_in_executor(None, functools.partial(fn, *args, **kwargs))
    asyncio.to_thread = lambda fn, *args, **kwargs: _to_thread_compat(fn, *args, **kwargs)

import hashlib
import logging

logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
import html
import json
import sqlite3
import calendar
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import os
import secrets
import socket
import ssl
import smtplib
import subprocess
import tempfile
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Border, Side, Alignment
import time
import requests
from dotenv import load_dotenv
from fastapi import Request
from fastapi.responses import RedirectResponse
from nicegui import app, background_tasks, context, run, ui

DB_PATH = Path(__file__).with_name("app.db")

# Versión del sistema: actualizar manualmente (formato yymmddhh) cada vez que se modifica el código
VERSION = "2.260316.15"

# Pestañas del sistema (tab_key interno -> label visible). Usado en Admin para permisos.
# compras_lista (Compras) se quitó de la tabla de permisos.
TAB_KEYS = [
    ("home", "Home"),
    ("estadisticas", "Estadísticas"),
    ("ventas", "Ventas"),
    ("productos", "Productos"),
    ("precios", "Precios"),
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

    # Usuario sanjustocentrocomputacion: crear si no existe (contraseña provisoria Temp1234)
    _pw_temp = hashlib.sha256("Temp1234".encode("utf-8")).hexdigest()
    cur.execute(
        """INSERT OR IGNORE INTO users (username, password_hash, created_at, email) VALUES (?, ?, ?, ?)""",
        ("sanjustocentrocomputacion@gmail.com", _pw_temp, datetime.utcnow().isoformat(), "sanjustocentrocomputacion@gmail.com"),
    )
    cur.execute("SELECT id FROM users WHERE username = ?", ("sanjustocentrocomputacion@gmail.com",))
    _sanjusto_row = cur.fetchone()
    if _sanjusto_row:
        _uid = _sanjusto_row["id"]
        for tab_key in ("home", "estadisticas", "ventas", "productos", "precios", "busqueda", "balance", "compras", "stock", "compras_lista", "pedidos", "importacion", "pesos", "datos", "configuracion", "admin"):
            can = 1 if tab_key != "admin" else 0
            cur.execute("INSERT OR IGNORE INTO user_tab_permissions (user_id, tab_key, can_access) VALUES (?, ?, ?)", (_uid, tab_key, can))

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

    # Precios por producto (tipo_iva y costo u$ por id de publicación ML, por usuario)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS precios_producto (
            id TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            tipo_iva REAL NOT NULL,
            costo_u REAL NOT NULL,
            PRIMARY KEY (id, user_id),
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
        """
    )

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

    # Lista de compras a cotizar
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

    # Migración: agregar columna despachante a invoice_extra si no existe (tablas antiguas)
    cur.execute("PRAGMA table_info(invoice_extra)")
    inv_extra_cols = [r[1] for r in cur.fetchall()]
    if "despachante" not in inv_extra_cols:
        cur.execute("ALTER TABLE invoice_extra ADD COLUMN despachante TEXT")
    if "pa" not in inv_extra_cols:
        cur.execute("ALTER TABLE invoice_extra ADD COLUMN pa TEXT")

    # Migración: dar permisos por defecto a usuarios existentes (admin solo para user_id=1)
    cur.execute("SELECT id FROM users ORDER BY id")
    for row in cur.fetchall():
        uid = row["id"]
        for tab_key in ("home", "estadisticas", "ventas", "productos", "precios", "busqueda", "balance", "compras", "stock", "compras_lista", "pedidos", "importacion", "pesos", "datos", "configuracion", "admin"):
            can = 1 if tab_key != "admin" or uid == 1 else 0
            cur.execute(
                "INSERT OR IGNORE INTO user_tab_permissions (user_id, tab_key, can_access) VALUES (?, ?, ?)",
                (uid, tab_key, can),
            )

    conn.commit()
    conn.close()


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
                "client_secret": row["client_secret"],
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
        cur.execute(
            "INSERT INTO ml_app_credentials (user_id, client_id, client_secret, redirect_uri) VALUES (?, ?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET client_id=?, client_secret=?, redirect_uri=?",
            (user_id, client_id.strip(), client_secret.strip(), redirect_uri or "", client_id.strip(), client_secret.strip(), redirect_uri or ""),
        )
        conn.commit()
    finally:
        conn.close()


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
                "client_secret": row["client_secret"],
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
        cur.execute(
            "INSERT INTO qb_app_credentials (user_id, client_id, client_secret, redirect_uri) VALUES (?, ?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET client_id=?, client_secret=?, redirect_uri=?",
            (user_id, client_id.strip(), client_secret.strip(), redirect_uri or "", client_id.strip(), client_secret.strip(), redirect_uri or ""),
        )
        conn.commit()
    finally:
        conn.close()


def get_qb_tokens(user_id: int) -> Optional[Any]:
    """Obtiene los tokens OAuth de QuickBooks del usuario (access_token, refresh_token, expires_at, realm_id)."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT access_token, refresh_token, expires_at, realm_id FROM qb_tokens WHERE user_id = ? ORDER BY id DESC LIMIT 1",
            (user_id,),
        )
        row = cur.fetchone()
        return dict(row) if row and row["access_token"] else None
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


def fetch_qb_customers(user_id: int) -> tuple[List[Dict[str, str]], Optional[str]]:
    """
    Obtiene la lista de Customers de QuickBooks.
    Devuelve (lista, None) si OK, o ([], mensaje_error) si falla.
    """
    import base64
    from urllib.parse import quote

    qb_creds = get_qb_app_credentials(user_id)
    qb_tokens = get_qb_tokens(user_id)
    if not qb_creds or not qb_tokens:
        return [], "Credenciales o tokens de QuickBooks no configurados"
    if not qb_tokens.get("realm_id"):
        return [], "Falta realm_id. Volvé a Conectar cuenta en Configuración."

    realm_id = qb_tokens["realm_id"]
    access_token = qb_tokens["access_token"]
    refresh_token = qb_tokens.get("refresh_token")
    expires_at = qb_tokens.get("expires_at")

    # Refrescar token si está por vencer (menos de 5 min)
    needs_refresh = False
    if expires_at:
        try:
            from datetime import datetime as dt
            exp = dt.fromisoformat(expires_at.replace("Z", "+00:00"))
            if exp.tzinfo:
                from datetime import timezone
                now = dt.now(timezone.utc)
            else:
                now = dt.utcnow()
            if (exp - now).total_seconds() < 300:
                needs_refresh = True
        except Exception:
            needs_refresh = True
    elif refresh_token:
        needs_refresh = True

    if needs_refresh and refresh_token and qb_creds:
        auth_str = f"{qb_creds['client_id']}:{qb_creds['client_secret']}"
        auth_b64 = base64.b64encode(auth_str.encode()).decode()
        try:
            resp = requests.post(
                "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
                data={"grant_type": "refresh_token", "refresh_token": refresh_token},
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Authorization": f"Basic {auth_b64}",
                },
                timeout=15,
            )
            if resp.ok:
                data = resp.json()
                new_token = data.get("access_token")
                new_refresh = data.get("refresh_token") or refresh_token
                new_expires = data.get("expires_in")
                expires_at_new = None
                if isinstance(new_expires, (int, float)):
                    from datetime import datetime as dt, timedelta
                    expires_at_new = (dt.utcnow() + timedelta(seconds=int(new_expires))).isoformat()
                conn = get_connection()
                try:
                    cur = conn.cursor()
                    cur.execute(
                        "UPDATE qb_tokens SET access_token=?, refresh_token=?, expires_at=?, raw_data=? WHERE user_id=?",
                        (new_token, new_refresh, expires_at_new, json.dumps(data, ensure_ascii=False), user_id),
                    )
                    conn.commit()
                finally:
                    conn.close()
                access_token = new_token
        except Exception:
            pass

    base_url = "https://quickbooks.api.intuit.com"
    query = "SELECT Id, DisplayName FROM Customer MAXRESULTS 1000"
    url = f"{base_url}/v3/company/{realm_id}/query?query={quote(query)}"
    try:
        r = requests.get(
            url,
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
            timeout=15,
        )
        r.raise_for_status()
        text = (r.text or "").strip()
        if not text:
            return [], "La API respondió vacío. Probá reconectar la cuenta (Desvincular → Conectar)."
        try:
            data = json.loads(text)
        except (json.JSONDecodeError, ValueError):
            preview = text[:300] if len(text) > 300 else text
            return [], f"La API no devolvió JSON válido. Probá reconectar la cuenta. Preview: {preview!r}"
        customers = []
        raw_customers = data.get("QueryResponse", {}).get("Customer") or []
        if isinstance(raw_customers, dict):
            raw_customers = [raw_customers]
        for qbo in raw_customers:
            cid = str(qbo.get("Id", ""))
            name = (qbo.get("DisplayName") or qbo.get("FullyQualifiedName") or cid).strip()
            if cid:
                customers.append({"id": cid, "name": name or cid})
        return sorted(customers, key=lambda c: (c["name"].lower(), c["id"])), None
    except Exception as e:
        err_msg = str(e)
        try:
            if hasattr(e, "response") and e.response is not None:
                resp = e.response
                body = getattr(resp, "text", "") or ""
                if body:
                    err_msg = body[:300] if len(body) > 300 else body
                sc = getattr(resp, "status_code", None)
                if sc is not None:
                    err_msg = f"HTTP {sc}: {err_msg}"
        except Exception:
            pass
        if "Expecting value" in err_msg or "line 1 column 1" in err_msg:
            err_msg = "La API respondió vacío o no-JSON. Probá reconectar la cuenta (Desvincular → Conectar)."
        if "3100" in err_msg or "ApplicationAuthorizationFailed" in err_msg:
            err_msg = "Autorización fallida (error 3100). Completá la autorización de la app en developer.intuit.com o reconectá la cuenta."
        return [], err_msg


def _qb_raw_query(user_id: int, query_sql: str) -> tuple[Optional[dict], Optional[str]]:
    """
    Ejecuta una consulta SQL contra la API de QuickBooks.
    Retorna (data_json, None) si OK, o (None, mensaje_error) si falla.
    """
    import base64
    from urllib.parse import quote

    qb_creds = get_qb_app_credentials(user_id)
    qb_tokens = get_qb_tokens(user_id)
    if not qb_creds or not qb_tokens:
        return None, "Credenciales o tokens de QuickBooks no configurados"
    if not qb_tokens.get("realm_id"):
        return None, "Falta realm_id. Volvé a Conectar cuenta en Configuración."
    realm_id = qb_tokens["realm_id"]
    access_token = qb_tokens["access_token"]
    refresh_token = qb_tokens.get("refresh_token")
    expires_at = qb_tokens.get("expires_at")
    needs_refresh = False
    if expires_at:
        try:
            from datetime import datetime as dt
            exp = dt.fromisoformat(expires_at.replace("Z", "+00:00"))
            if exp.tzinfo:
                from datetime import timezone
                now = dt.now(timezone.utc)
            else:
                now = dt.utcnow()
            if (exp - now).total_seconds() < 300:
                needs_refresh = True
        except Exception:
            needs_refresh = True
    elif refresh_token:
        needs_refresh = True
    if needs_refresh and refresh_token and qb_creds:
        auth_str = f"{qb_creds['client_id']}:{qb_creds['client_secret']}"
        auth_b64 = base64.b64encode(auth_str.encode()).decode()
        try:
            resp = requests.post(
                "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
                data={"grant_type": "refresh_token", "refresh_token": refresh_token},
                headers={"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded", "Authorization": f"Basic {auth_b64}"},
                timeout=15,
            )
            if resp.ok:
                data = resp.json()
                conn = get_connection()
                try:
                    cur = conn.cursor()
                    cur.execute(
                        "UPDATE qb_tokens SET access_token=?, refresh_token=?, expires_at=?, raw_data=? WHERE user_id=?",
                        (data.get("access_token"), data.get("refresh_token") or refresh_token, None, json.dumps(data, ensure_ascii=False), user_id),
                    )
                    conn.commit()
                finally:
                    conn.close()
                access_token = data.get("access_token")
        except Exception:
            pass
    base_url = "https://quickbooks.api.intuit.com"
    url = f"{base_url}/v3/company/{realm_id}/query?query={quote(query_sql)}&minorversion=65"
    try:
        r = requests.get(url, headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"}, timeout=15)
        r.raise_for_status()
        text = (r.text or "").strip()
        if not text:
            return None, "La API respondió vacío."
        data = json.loads(text)
        return data, None
    except Exception as e:
        err = str(e)
        if hasattr(e, "response") and e.response is not None:
            body = getattr(e.response, "text", "") or ""
            if body:
                err = body[:250] if len(body) > 250 else body
            sc = getattr(e.response, "status_code", None)
            if sc is not None:
                err = f"HTTP {sc}: {err}"
        return None, err


def fetch_qb_company_info(user_id: int) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Obtiene la información de la empresa en QuickBooks."""
    qb_tokens = get_qb_tokens(user_id)
    if not qb_tokens or not qb_tokens.get("realm_id"):
        return None, "Sin tokens o realm_id"
    base_url = "https://quickbooks.api.intuit.com"
    realm_id = qb_tokens["realm_id"]
    access_token = qb_tokens["access_token"]
    url = f"{base_url}/v3/company/{realm_id}/companyinfo/{realm_id}"
    try:
        r = requests.get(url, headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"}, timeout=15)
        r.raise_for_status()
        data = r.json()
        ci = data.get("CompanyInfo", {})
        return ci, None
    except Exception as e:
        return None, str(e)


def fetch_qb_vendors(user_id: int) -> tuple[List[Dict[str, Any]], Optional[str]]:
    """Obtiene la lista de Vendors (proveedores) de QuickBooks."""
    data, err = _qb_raw_query(user_id, "SELECT Id, DisplayName FROM Vendor MAXRESULTS 500")
    if err:
        return [], err
    raw = data.get("QueryResponse", {}).get("Vendor") or []
    if isinstance(raw, dict):
        raw = [raw]
    vendors = []
    for v in raw:
        vid = str(v.get("Id", ""))
        name = (v.get("DisplayName") or v.get("FullyQualifiedName") or vid).strip()
        if vid:
            vendors.append({"id": vid, "name": name or vid})
    return sorted(vendors, key=lambda x: (x["name"].lower(), x["id"])), None


def fetch_qb_bills(user_id: int) -> tuple[List[Dict[str, Any]], Optional[str]]:
    """Obtiene Bills (facturas de compra a proveedores) de QuickBooks."""
    data, err = _qb_raw_query(user_id, "SELECT Id, DocNumber, TxnDate, DueDate, Balance, VendorRef FROM Bill MAXRESULTS 100")
    if err:
        return [], err
    raw = data.get("QueryResponse", {}).get("Bill") or []
    if isinstance(raw, dict):
        raw = [raw]
    bills = []
    for b in raw:
        vid = ""
        vname = ""
        vr = b.get("VendorRef") or {}
        if isinstance(vr, dict):
            vid = str(vr.get("value", ""))
            vname = str(vr.get("name", "")).strip()
        bid = str(b.get("Id", ""))
        doc = str(b.get("DocNumber", "")).strip()
        txn = str(b.get("TxnDate", ""))[:10] if b.get("TxnDate") else ""
        due = str(b.get("DueDate", ""))[:10] if b.get("DueDate") else ""
        bal = b.get("Balance")
        if bal is not None:
            try:
                bal = f"{float(bal):,.2f}"
            except (TypeError, ValueError):
                bal = str(bal)
        else:
            bal = ""
        bills.append({"id": bid, "doc": doc, "vendor": vname or vid, "txn_date": txn, "due_date": due, "balance": bal})
    return bills, None


def fetch_qb_items(user_id: int) -> tuple[List[Dict[str, Any]], Optional[str]]:
    """Obtiene Items (productos/inventario) de QuickBooks con stock, usando Description (Sales), Sku y UnitPrice."""
    data, err = _qb_raw_query(
        user_id,
        "SELECT * FROM Item MAXRESULTS 1000"
    )
    if err:
        return [], err
    raw = data.get("QueryResponse", {}).get("Item") or []
    if isinstance(raw, dict):
        raw = [raw]
    items = []
    for it in raw:
        iid = str(it.get("Id", ""))
        sales_desc = (it.get("Description") or it.get("Name") or "").strip()
        qty = it.get("QtyOnHand")
        try:
            qty_num = int(float(qty)) if qty is not None else 0
        except (TypeError, ValueError):
            qty_num = 0
        sku = str(it.get("Sku") or it.get("SKU") or "").strip()
        try:
            unit_price = float(it.get("UnitPrice") or 0)
        except (TypeError, ValueError):
            unit_price = 0.0
        if iid:
            items.append({"id": iid, "producto": sales_desc or "—", "qty": qty_num, "sku": sku, "sales_price": unit_price})
    return sorted(items, key=lambda x: (x["producto"].lower(), x["id"])), None


def fetch_qb_items_search(user_id: int, search_text: str) -> tuple[List[Dict[str, Any]], Optional[str], int]:
    """Busca Items (productos) en QuickBooks por texto. Obtiene todos los items con paginación y filtra en Python
    donde el texto buscado está contenido (case-insensitive) en Name, Sku o Sales Description.
    Retorna (items, err, total_revisados)."""
    search_clean = (search_text or "").strip()
    if not search_clean:
        return [], None, 0
    term_lower = search_clean.lower()
    # QB limita 1000 por query; paginamos para traer más items (máx 10 páginas = 10000 items)
    all_raw: List[dict] = []
    start = 1
    batch = 1000
    max_pages = 10
    for _ in range(max_pages):
        query = f"SELECT * FROM Item STARTPOSITION {start} MAXRESULTS {batch}"
        data, err = _qb_raw_query(user_id, query)
        if err:
            if start == 1:
                return [], err, 0
            break
        raw = data.get("QueryResponse", {}).get("Item") or []
        if isinstance(raw, dict):
            raw = [raw]
        if not raw:
            break
        all_raw.extend(raw)
        if len(raw) < batch:
            break
        start += batch
    total_revisados = len(all_raw)
    items: List[Dict[str, Any]] = []
    for it in all_raw:
        iid = str(it.get("Id", ""))
        if not iid:
            continue
        name = (it.get("DisplayName") or it.get("Name") or "").strip()
        fqn = str(it.get("FullyQualifiedName") or "").strip()
        sku = str(it.get("Sku") or it.get("SKU") or "").strip()
        # Sales description: top-level Description, SalesAndPurchase (Inventory) o SalesOrPurchase (Service/NonInventory)
        desc = str(it.get("Description") or "").strip()
        for nested in (it.get("SalesAndPurchase") or {}, it.get("SalesOrPurchase") or {}):
            if isinstance(nested, dict):
                desc = desc or str(nested.get("Description") or nested.get("SalesDesc") or nested.get("SalesOrPurchaseDesc") or "").strip()
        # Coincidencia por contenido (no exacta): term está contenido en Name, SKU o Sales Description
        name_ok = term_lower in (name or "").lower() or term_lower in (fqn or "").lower()
        sku_ok = term_lower in (sku or "").lower()
        desc_ok = term_lower in (desc or "").lower()
        if name_ok or sku_ok or desc_ok:
            # producto = Sales Description (desc); fallback a name si no hay desc
            producto = desc or name or "—"
            items.append({"id": iid, "name": name or "—", "producto": producto, "sku": sku})
    return items[:100], None, total_revisados  # Máximo 100 resultados


def fetch_qb_item_history(user_id: int, item_id: str, sku: str = "") -> tuple[List[Dict[str, Any]], Optional[str]]:
    """Obtiene el historial de sales price y cost de un Item de QuickBooks.
    Busca en Invoices (ventas), Bills (compras) y compras_lista (cotizaciones por SKU).
    Retorna lista de {tipo, fecha, doc, precio, cantidad, total} ordenada por fecha desc."""
    result: List[Dict[str, Any]] = []
    item_id_str = str(item_id or "").strip()
    sku_clean = (sku or "").strip()

    # 1. Invoices (ventas) - líneas con SalesItemLineDetail donde ItemRef = item_id
    inv_start = 1
    for _ in range(5):
        data, err = _qb_raw_query(
            user_id,
            f"SELECT Id, DocNumber, TxnDate, CustomerRef, Line FROM Invoice STARTPOSITION {inv_start} MAXRESULTS 100",
        )
        if err:
            break
        raw = data.get("QueryResponse", {}).get("Invoice") or []
        if isinstance(raw, dict):
            raw = [raw]
        if not raw:
            break
        for inv in raw:
            doc = str(inv.get("DocNumber", "")).strip()
            txn = str(inv.get("TxnDate", ""))[:10] if inv.get("TxnDate") else ""
            cust_ref = inv.get("CustomerRef") or {}
            cliente = str(cust_ref.get("name", "") if isinstance(cust_ref, dict) else "").strip() or "—"
            lines = inv.get("Line") or []
            if isinstance(lines, dict):
                lines = [lines]
            for ln in lines:
                detail = ln.get("SalesItemLineDetail") or {}
                if not isinstance(detail, dict):
                    continue
                ref = detail.get("ItemRef") or {}
                ref_val = str(ref.get("value", "") if isinstance(ref, dict) else "").strip()
                if ref_val != item_id_str:
                    continue
                try:
                    precio = float(detail.get("UnitPrice") or ln.get("Amount") or 0)
                except (TypeError, ValueError):
                    precio = 0.0
                try:
                    qty = float(detail.get("Qty") or 1)
                except (TypeError, ValueError):
                    qty = 1.0
                amt = ln.get("Amount")
                try:
                    total = float(amt) if amt is not None else precio * qty
                except (TypeError, ValueError):
                    total = precio * qty
                inv_id = str(inv.get("Id", "")).strip()
                result.append({"tipo": "Venta", "fecha": txn, "doc": doc or "—", "precio": precio, "cliente": cliente, "qb_id": inv_id, "qb_tipo": "invoice"})
        if len(raw) < 100:
            break
        inv_start += 100

    # 2. Bills (compras)
    bill_start = 1
    for _ in range(5):
        data, err = _qb_raw_query(
            user_id,
            f"SELECT Id, DocNumber, TxnDate, VendorRef, Line FROM Bill STARTPOSITION {bill_start} MAXRESULTS 100",
        )
        if err:
            break
        raw = data.get("QueryResponse", {}).get("Bill") or []
        if isinstance(raw, dict):
            raw = [raw]
        if not raw:
            break
        for bill in raw:
            doc = str(bill.get("DocNumber", "")).strip()
            txn = str(bill.get("TxnDate", ""))[:10] if bill.get("TxnDate") else ""
            vend_ref = bill.get("VendorRef") or {}
            cliente = str(vend_ref.get("name", "") if isinstance(vend_ref, dict) else "").strip() or "—"
            lines = bill.get("Line") or []
            if isinstance(lines, dict):
                lines = [lines]
            for ln in lines:
                detail = ln.get("ItemBasedExpenseLineDetail") or ln.get("PurchaseItemLineDetail") or {}
                if not isinstance(detail, dict):
                    continue
                ref = detail.get("ItemRef") or {}
                ref_val = str(ref.get("value", "") if isinstance(ref, dict) else "").strip()
                if ref_val != item_id_str:
                    continue
                try:
                    precio = float(detail.get("UnitPrice") or ln.get("Amount") or 0)
                except (TypeError, ValueError):
                    precio = 0.0
                try:
                    qty = float(detail.get("Qty") or 1)
                except (TypeError, ValueError):
                    qty = 1.0
                amt = ln.get("Amount")
                try:
                    total = float(amt) if amt is not None else precio * qty
                except (TypeError, ValueError):
                    total = precio * qty
                bill_id = str(bill.get("Id", "")).strip()
                result.append({"tipo": "Compra", "fecha": txn, "doc": doc or "—", "precio": precio, "cliente": cliente, "qb_id": bill_id, "qb_tipo": "bill"})
        if len(raw) < 100:
            break
        bill_start += 100

    # 3. compras_lista (cotizaciones) por SKU
    if sku_clean:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT fecha, producto, sku, cantidad, precio_sugerido, usuario_qb FROM compras_lista WHERE user_id = ? AND (sku = ? OR sku LIKE ?)",
                (user_id, sku_clean, f"%{sku_clean}%"),
            )
            for row in cur.fetchall():
                r = dict(row)
                try:
                    precio = float(str(r.get("precio_sugerido") or "0").replace(",", "."))
                except (TypeError, ValueError):
                    precio = 0.0
                cliente = str(r.get("usuario_qb") or "").strip() or "—"
                result.append({
                    "tipo": "Cotización",
                    "fecha": str(r.get("fecha") or "")[:10],
                    "doc": str(r.get("producto") or "")[:50] or "—",
                    "precio": precio,
                    "cliente": cliente,
                    "qb_id": "",
                    "qb_tipo": "",
                })
        finally:
            conn.close()

    # Ordenar por fecha desc
    result.sort(key=lambda x: (x.get("fecha") or ""), reverse=True)
    return result[:200], None  # Máx 200 registros


def fetch_qb_customer_detail(user_id: int, customer_id: str) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Obtiene el detalle de un Customer (Balance, etc.) por ID."""
    qb_tokens = get_qb_tokens(user_id)
    if not qb_tokens or not qb_tokens.get("realm_id"):
        return None, "Sin tokens o realm_id"
    base_url = "https://quickbooks.api.intuit.com"
    realm_id = qb_tokens["realm_id"]
    access_token = qb_tokens["access_token"]
    url = f"{base_url}/v3/company/{realm_id}/customer/{customer_id}"
    try:
        r = requests.get(url, headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"}, timeout=15)
        r.raise_for_status()
        data = r.json()
        return data.get("Customer", {}), None
    except Exception as e:
        return None, str(e)


def fetch_qb_invoice_detail(user_id: int, invoice_id: str) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Obtiene el detalle completo de una Invoice por ID."""
    qb_tokens = get_qb_tokens(user_id)
    if not qb_tokens or not qb_tokens.get("realm_id"):
        return None, "Sin tokens o realm_id"
    base_url = "https://quickbooks.api.intuit.com"
    realm_id = qb_tokens["realm_id"]
    access_token = qb_tokens["access_token"]
    url = f"{base_url}/v3/company/{realm_id}/invoice/{invoice_id}"
    try:
        r = requests.get(url, headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"}, timeout=15)
        r.raise_for_status()
        data = r.json()
        return data.get("Invoice", {}), None
    except Exception as e:
        return None, str(e)


def fetch_qb_invoice_pdf(user_id: int, invoice_id: str) -> tuple[Optional[bytes], Optional[str]]:
    """Descarga una Invoice como PDF. Retorna (pdf_bytes, None) o (None, mensaje_error)."""
    qb_tokens = get_qb_tokens(user_id)
    if not qb_tokens or not qb_tokens.get("realm_id"):
        return None, "Sin tokens o realm_id"
    base_url = "https://quickbooks.api.intuit.com"
    realm_id = qb_tokens["realm_id"]
    access_token = qb_tokens["access_token"]
    url = f"{base_url}/v3/company/{realm_id}/invoice/{invoice_id}/pdf"
    try:
        r = requests.get(url, headers={"Authorization": f"Bearer {access_token}", "Accept": "application/pdf"}, timeout=30)
        r.raise_for_status()
        return r.content, None
    except Exception as e:
        return None, str(e)


def fetch_qb_invoices(user_id: int, customer_id: str) -> tuple[tuple[List[Dict[str, Any]], float], Optional[str]]:
    """Obtiene Invoices (facturas de venta) de un Customer. Retorna (invoices, overdue_total), err."""
    data, err = _qb_raw_query(
        user_id,
        f"SELECT Id, DocNumber, TxnDate, DueDate, TotalAmt, Balance, CustomerRef FROM Invoice WHERE CustomerRef = '{customer_id}' MAXRESULTS 200",
    )
    if err:
        return ([], 0.0), err
    raw = data.get("QueryResponse", {}).get("Invoice") or []
    if isinstance(raw, dict):
        raw = [raw]
    invoices = []
    overdue_total = 0.0
    from datetime import datetime
    today = datetime.now().strftime("%Y-%m-%d")
    for inv in raw:
        iid = str(inv.get("Id", ""))
        doc = str(inv.get("DocNumber", "")).strip()
        txn = str(inv.get("TxnDate", ""))[:10] if inv.get("TxnDate") else ""
        due = str(inv.get("DueDate", ""))[:10] if inv.get("DueDate") else ""
        total = inv.get("TotalAmt")
        bal = inv.get("Balance")
        try:
            total_str = f"{float(total):,.2f}" if total is not None else ""
        except (TypeError, ValueError):
            total_str = str(total) if total is not None else ""
        try:
            bal_str = f"{float(bal):,.2f}" if bal is not None else ""
        except (TypeError, ValueError):
            bal_str = str(bal) if bal is not None else ""
        if bal is None:
            bal_str = ""
            bal = 0
        is_overdue = due and due < today and bal is not None and float(bal) != 0
        if is_overdue:
            try:
                overdue_total += float(bal)
            except (TypeError, ValueError):
                pass
        status = "Pagada" if (bal is not None and float(bal) == 0) else ("Vencida" if is_overdue else "Abierta")
        try:
            inv_bal = float(bal) if bal is not None else 0.0
        except (TypeError, ValueError):
            inv_bal = 0.0
        invoices.append({"id": iid, "doc": doc, "txn_date": txn, "due_date": due, "tipo": "Factura", "amount": bal_str, "amount_num": inv_bal, "balance": inv_bal, "status": status})
    return (invoices, overdue_total), None


def fetch_qb_invoice_detail(user_id: int, invoice_id: str) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Obtiene el detalle completo de una Invoice por ID (incluye Line items)."""
    qb_tokens = get_qb_tokens(user_id)
    if not qb_tokens or not qb_tokens.get("realm_id"):
        return None, "Sin tokens o realm_id"
    base_url = "https://quickbooks.api.intuit.com"
    realm_id = qb_tokens["realm_id"]
    access_token = qb_tokens["access_token"]
    url = f"{base_url}/v3/company/{realm_id}/invoice/{invoice_id}"
    try:
        r = requests.get(url, headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"}, timeout=15)
        r.raise_for_status()
        return r.json().get("Invoice"), None
    except Exception as e:
        return None, str(e)


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


def get_precios_producto(item_id: str, user_id: int) -> Optional[Dict[str, Any]]:
    """Obtiene tipo_iva y costo_u guardados para un producto (por id ML)."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT tipo_iva, costo_u FROM precios_producto WHERE id = ? AND user_id = ?",
            (item_id, user_id),
        )
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def set_precios_producto(item_id: str, user_id: int, tipo_iva: float, costo_u: float) -> None:
    """Guarda tipo_iva y costo_u para un producto."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO precios_producto (id, user_id, tipo_iva, costo_u) VALUES (?, ?, ?, ?) ON CONFLICT(id, user_id) DO UPDATE SET tipo_iva=?, costo_u=?",
            (item_id, user_id, tipo_iva, costo_u, tipo_iva, costo_u),
        )
        conn.commit()
    finally:
        conn.close()


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


def user_can_access_tab(user_id: int, tab_key: str) -> bool:
    """Devuelve si el usuario puede acceder a la pestaña."""
    perms = get_user_tab_permissions(user_id)
    return perms.get(tab_key, True if tab_key != "admin" else False)


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
    """Obtiene los datos extra de invoice_extra para una lista de qb_invoice_id. Retorna {qb_invoice_id: {courier, guia, importe_factura, pa, estado, despachante}}."""
    if not qb_invoice_ids:
        return {}
    conn = get_connection()
    try:
        cur = conn.cursor()
        placeholders = ",".join("?" * len(qb_invoice_ids))
        cur.execute(
            f"SELECT qb_invoice_id, courier, guia, importe_factura, pa, estado, despachante FROM invoice_extra WHERE user_id = ? AND qb_invoice_id IN ({placeholders})",
            [user_id] + list(qb_invoice_ids),
        )
        return {str(r["qb_invoice_id"]): dict(r) for r in cur.fetchall()}
    finally:
        conn.close()


def upsert_invoice_extra(user_id: int, qb_invoice_id: str, **kwargs) -> None:
    """Inserta o actualiza fila en invoice_extra. Merge con valores actuales para actualizaciones parciales."""
    allowed = {"courier", "guia", "importe_factura", "pa", "estado", "despachante"}
    kv = {k: str(v or "") if v is not None else "" for k, v in kwargs.items() if k in allowed}
    if not kv:
        return
    extras = get_invoice_extras(user_id, [qb_invoice_id])
    current = extras.get(qb_invoice_id, {})
    merged = {**{k: (current.get(k) or "") for k in allowed}, **kv}
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO invoice_extra (user_id, qb_invoice_id, courier, guia, importe_factura, pa, estado, despachante)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(user_id, qb_invoice_id) DO UPDATE SET courier=excluded.courier, guia=excluded.guia, importe_factura=excluded.importe_factura, pa=excluded.pa, estado=excluded.estado, despachante=excluded.despachante""",
            (user_id, qb_invoice_id, merged["courier"], merged["guia"], merged["importe_factura"], merged["pa"], merged["estado"], merged["despachante"]),
        )
        conn.commit()
    finally:
        conn.close()


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
        now = datetime.utcnow().isoformat()
        for i, row in enumerate(rows):
            cur.execute(
                "INSERT INTO importacion_filas (user_id, fila_orden, datos_json, created_at) VALUES (?, ?, ?, ?)",
                (user_id, i, json.dumps(row, ensure_ascii=False), now),
            )
        conn.commit()
    finally:
        conn.close()


BACKUP_VERSION = 1


def export_user_db_data(user_id: int) -> bytes:
    """Exporta solo datos operativos: cotizador (parámetros, tablas, pesos, gastos), precios por producto, importación. NO incluye credenciales, password, app id, client secret."""
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
            "precios_producto": [],
            "importacion_filas": [],
        }
        cur.execute("SELECT clave, valor FROM cotizador_datos WHERE user_id = ?", (user_id,))
        data["cotizador_datos"] = [{"clave": r["clave"], "valor": r["valor"]} for r in cur.fetchall()]
        cur.execute("SELECT id, tipo_iva, costo_u FROM precios_producto WHERE user_id = ?", (user_id,))
        data["precios_producto"] = [{"id": r["id"], "tipo_iva": r["tipo_iva"], "costo_u": r["costo_u"]} for r in cur.fetchall()]
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
        # precios_producto: borrar todo del usuario e insertar backup
        cur.execute("DELETE FROM precios_producto WHERE user_id = ?", (uid,))
        for item in data.get("precios_producto") or []:
            if isinstance(item, dict) and item.get("id") is not None:
                item_id = str(item["id"])
                tipo_iva = float(item.get("tipo_iva") or 0.105)
                costo_u = float(item.get("costo_u") or 0)
                cur.execute("INSERT INTO precios_producto (id, user_id, tipo_iva, costo_u) VALUES (?, ?, ?, ?)", (item_id, uid, tipo_iva, costo_u))
        # importacion_filas: borrar todo del usuario e insertar backup
        cur.execute("DELETE FROM importacion_filas WHERE user_id = ?", (uid,))
        now = datetime.utcnow().isoformat()
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


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()




def send_email(to_email: str, subject: str, body_plain: str) -> Optional[str]:
    """Envía un email vía SMTP. Devuelve None si OK, mensaje de error si falla.
    Variables: SMTP_HOST, SMTP_PORT (465=SSL, 587=STARTTLS), SMTP_USER, SMTP_PASS, SMTP_FROM, SMTP_FROM_NAME"""
    host = os.getenv("SMTP_HOST", "").strip()
    if not host:
        return "SMTP no configurado: falta SMTP_HOST en .env. Copiá .env.example a .env y completá las variables (ver README)."
    port = int(os.getenv("SMTP_PORT", "465"))
    user = os.getenv("SMTP_USER", "").strip()
    password_env = os.getenv("SMTP_PASS", "").strip().replace(" ", "")
    from_addr_raw = (os.getenv("SMTP_FROM", "") or user).strip() or user
    from_name = (os.getenv("SMTP_FROM_NAME", "") or "BDC systems").strip()
    from_header = f"{from_name} <{from_addr_raw}>" if from_name else from_addr_raw

    if not user or not password_env:
        return "SMTP no configurado: falta SMTP_USER o SMTP_PASS en .env. Copiá .env.example a .env y completá las variables."

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = from_header
        msg["To"] = to_email
        msg.attach(MIMEText(body_plain, "plain", "utf-8"))

        ctx = ssl.create_default_context()
        envelope_from = user
        _orig_getaddrinfo = socket.getaddrinfo

        def _ipv4_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
            return _orig_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)

        socket.getaddrinfo = _ipv4_getaddrinfo
        try:
            if port == 465:
                with smtplib.SMTP_SSL(host, port, context=ctx, timeout=30) as smtp:
                    smtp.login(user, password_env)
                    smtp.sendmail(envelope_from, to_email, msg.as_string())
            else:
                with smtplib.SMTP(host, port, timeout=30) as smtp:
                    smtp.starttls(context=ctx)
                    smtp.login(user, password_env)
                    smtp.sendmail(envelope_from, to_email, msg.as_string())
        finally:
            socket.getaddrinfo = _orig_getaddrinfo
        return None
    except Exception as e:
        return f"Error al enviar email: {str(e)}"


def get_user_email(user_id: int) -> Optional[str]:
    """Obtiene el email del usuario. Si no tiene, intenta usar username si parece email."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT email, username FROM users WHERE id = ?", (user_id,))
        row = cur.fetchone()
        if not row:
            return None
        email = (row["email"] or "").strip() if row["email"] else ""
        if email:
            return email
        uname = (row["username"] or "").strip()
        return uname if "@" in uname else None
    finally:
        conn.close()


def create_user(email: str) -> tuple[Optional[str], Optional[str]]:
    """Crea un usuario. Devuelve (mensaje_error, contraseña_si_email_falló). Si OK, (None, None)."""
    email_clean = (email or "").strip().lower()
    if not email_clean or "@" not in email_clean:
        return ("Ingresá un email válido.", None)
    try:
        new_password = secrets.token_urlsafe(8)
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT id FROM users ORDER BY id LIMIT 1")
        first_user = cur.fetchone()
        is_first = first_user is None

        cur.execute(
            "INSERT INTO users (username, password_hash, created_at, email) VALUES (?, ?, ?, ?)",
            (email_clean, hash_password(new_password), datetime.utcnow().isoformat(), email_clean),
        )
        conn.commit()
        uid = cur.lastrowid
        if uid:
            for tab_key, _ in TAB_KEYS:
                can = 1 if (tab_key != "admin" or is_first) else 0
                cur.execute(
                    "INSERT OR IGNORE INTO user_tab_permissions (user_id, tab_key, can_access) VALUES (?, ?, ?)",
                    (uid, tab_key, can),
                )
            conn.commit()

        # Enviar email con contraseña provisoria
        body = f"""Hola,

Te registraste en BDC systems. Tu contraseña provisoria es: {new_password}

Usuario (email): {email_clean}

Por favor iniciá sesión con tu email y contraseña provisoria, y cambiá tu contraseña en Configuración > Cambiar contraseña.
"""
        err = send_email(
            email_clean,
            "BDC systems - Tu contraseña provisoria",
            body,
        )
        if err:
            return (f"No se pudo enviar el email: {err}", new_password)
        return (None, None)
    except sqlite3.IntegrityError:
        return ("El usuario ya existe.", None)
    finally:
        conn.close()


def authenticate_user(username: str, password: str) -> Optional[Dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE username = ?", (username,))
        row = cur.fetchone()
        if not row:
            return None
        if row["password_hash"] != hash_password(password):
            return None
        return dict(row)
    finally:
        conn.close()


def update_user_password(user_id: int, current_password: str, new_password: str) -> Optional[str]:
    """Actualiza la contraseña del usuario. Devuelve mensaje de error o None si fue bien."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT password_hash FROM users WHERE id = ?", (user_id,))
        row = cur.fetchone()
        if not row:
            return "Usuario no encontrado."
        if row["password_hash"] != hash_password(current_password):
            return "Contraseña actual incorrecta."
        new_clean = (new_password or "").strip()
        if len(new_clean) < 4:
            return "La nueva contraseña debe tener al menos 4 caracteres."
        cur.execute("UPDATE users SET password_hash = ? WHERE id = ?", (hash_password(new_clean), user_id))
        conn.commit()
        return None
    finally:
        conn.close()


def admin_reset_user_password(target_user_id: int) -> tuple[Optional[str], bool, Optional[str], Optional[str]]:
    """Genera una nueva contraseña temporal, la guarda y la envía por email al usuario.
    Devuelve (mensaje_error, email_enviado, email_destino, nueva_contraseña).
    La contraseña se devuelve solo cuando el email falla, para mostrarla en un popup."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, username FROM users WHERE id = ?", (target_user_id,))
        row = cur.fetchone()
        if not row:
            return ("Usuario no encontrado.", False, None, None)
        to_email = get_user_email(target_user_id)
        if not to_email:
            return ("El usuario no tiene email registrado.", False, None, None)

        new_password = secrets.token_urlsafe(8)

        cur.execute("UPDATE users SET password_hash = ? WHERE id = ?", (hash_password(new_password), target_user_id))
        conn.commit()

        body = f"""Hola,

Un administrador reinició tu contraseña en BDC systems.

Usuario: {row['username']}
Nueva contraseña: {new_password}

Por favor iniciá sesión y cambiá tu contraseña en Configuración > Cambiar contraseña.
"""
        err = send_email(to_email, "BDC systems - Tu nueva contraseña", body)
        if err is None:
            return (None, True, to_email, None)
        return (f"No se pudo enviar el email: {err}", False, to_email, new_password)
    finally:
        conn.close()


def delete_user_and_all_data(target_user_id: int) -> Optional[str]:
    """Elimina un usuario y todos sus datos asociados. Devuelve mensaje de error o None si OK."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM users WHERE id = ?", (target_user_id,))
        if not cur.fetchone():
            return "Usuario no encontrado."
        # Borrar en orden: tablas que referencian user_id
        tables = [
            "ml_credentials",
            "qb_tokens",
            "ml_app_credentials",
            "qb_app_credentials",
            "user_qb_customer",
            "queries",
            "cotizador_datos",
            "precios_producto",
            "importacion_filas",
            "user_tab_permissions",
        ]
        for t in tables:
            try:
                cur.execute(f"DELETE FROM {t} WHERE user_id = ?", (target_user_id,))
            except sqlite3.OperationalError:
                pass  # tabla puede no existir
        cur.execute("DELETE FROM users WHERE id = ?", (target_user_id,))
        conn.commit()
        return None
    except Exception as e:
        conn.rollback()
        return str(e)
    finally:
        conn.close()


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
                datetime.utcnow().isoformat(),
                json.dumps(raw_response, ensure_ascii=False) if raw_response else None,
            ),
        )
        conn.commit()
    finally:
        conn.close()


# ==========================
# INTEGRACIÓN MERCADOLIBRE
# ==========================


def _ml_refresh_token(user_id: int, refresh_token: str) -> Optional[Dict[str, Any]]:
    """Refresca el access_token usando refresh_token. Usa credenciales del usuario o .env."""
    app_creds = get_ml_app_credentials(user_id)
    if app_creds:
        client_id = app_creds["client_id"]
        client_secret = app_creds["client_secret"]
    else:
        client_id = os.getenv("ML_CLIENT_ID")
        client_secret = os.getenv("ML_CLIENT_SECRET")
    if not client_id or not client_secret or not refresh_token:
        return None
    try:
        resp = requests.post(
            "https://api.mercadolibre.com/oauth/token",
            data={
                "grant_type": "refresh_token",
                "client_id": client_id,
                "client_secret": client_secret,
                "refresh_token": refresh_token,
            },
            headers={"Accept": "application/json"},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


def get_ml_access_token(user_id: int) -> Optional[str]:
    """Obtiene un access_token válido de MercadoLibre. Si está vencido, intenta refrescarlo automáticamente."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT access_token, refresh_token, expires_at FROM ml_credentials WHERE user_id = ? ORDER BY id DESC LIMIT 1",
            (user_id,),
        )
        row = cur.fetchone()
        if not row or not row["access_token"]:
            return None
        access_token = row["access_token"]
        refresh_token = row["refresh_token"]
        expires_at = row["expires_at"]

        # Comprobar si el token está vencido o vence en los próximos 5 minutos
        needs_refresh = False
        if expires_at:
            try:
                exp_str = expires_at[:19].replace("T", " ")
                exp_dt = datetime.strptime(exp_str, "%Y-%m-%d %H:%M:%S")
                if datetime.utcnow() >= exp_dt - timedelta(minutes=5):
                    needs_refresh = True
            except (ValueError, TypeError):
                needs_refresh = True  # Por si el formato es raro, intentar refresh

        if needs_refresh and refresh_token:
            data = _ml_refresh_token(user_id, refresh_token)
            if data and data.get("access_token"):
                new_token = data["access_token"]
                new_refresh = data.get("refresh_token") or refresh_token
                expires_in = data.get("expires_in")
                new_expires_at = None
                if isinstance(expires_in, (int, float)):
                    new_expires_at = (datetime.utcnow() + timedelta(seconds=int(expires_in))).isoformat()
                cur.execute(
                    "UPDATE ml_credentials SET access_token = ?, refresh_token = ?, expires_at = ?, raw_data = ? WHERE user_id = ?",
                    (new_token, new_refresh, new_expires_at, json.dumps(data, ensure_ascii=False), user_id),
                )
                conn.commit()
                return new_token
            return None  # Refresh falló; el usuario debe volver a vincular

        return access_token
    finally:
        conn.close()


def ml_get_my_items(access_token: str, include_paused: bool = False) -> Dict[str, Any]:
    """Obtiene las publicaciones del vendedor desde la API de MercadoLibre (paginado).
    include_paused=False (default): solo activas, carga más rápido.
    include_paused=True: incluye pausadas (sin stock), carga más lento."""
    base = "https://api.mercadolibre.com"
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}

    # 1. Obtener el user_id de ML del token
    me = requests.get(f"{base}/users/me", headers=headers, timeout=10)
    me.raise_for_status()
    ml_user_id = me.json().get("id")
    if not ml_user_id:
        return {"results": [], "paging": {"total": 0}, "error": "No se pudo obtener el usuario de ML"}

    # 2. Listar IDs: activas siempre; pausadas y closed solo si include_paused (catálogo vendido puede estar en closed)
    # ML limita offset a 1000; pasarlo devuelve 400 Bad Request
    item_ids = []
    seen: set = set()
    MAX_OFFSET = 1000
    statuses = ("active", "paused", "closed") if include_paused else ("active",)
    for status_val in statuses:
        offset = 0
        limit = 50
        while offset <= MAX_OFFSET:
            search = requests.get(
                f"{base}/users/{ml_user_id}/items/search",
                headers=headers,
                params={"limit": limit, "offset": offset, "status": status_val},
                timeout=15,
            )
            search.raise_for_status()
            search_data = search.json()
            chunk = search_data.get("results", [])
            for _id in chunk:
                if _id and _id not in seen:
                    seen.add(_id)
                    item_ids.append(_id)
            if len(chunk) < limit or offset + limit > MAX_OFFSET:
                break
            offset += limit

    paging = search_data.get("paging", {})
    total = paging.get("total", len(item_ids))

    if not item_ids:
        return {"results": [], "paging": {"total": total}, "seller_id": ml_user_id}

    # 3. Obtener detalles de cada ítem (la API acepta hasta 20 IDs por request)
    all_items = []
    for i in range(0, len(item_ids), 20):
        chunk = item_ids[i : i + 20]
        ids_param = ",".join(chunk)
        items_resp = requests.get(
            f"{base}/items",
            params={"ids": ids_param},
            headers=headers,
            timeout=15,
        )
        items_resp.raise_for_status()
        def _item_from_body(body: dict) -> dict:
            marca = ""
            color = ""
            seller_sku = ""
            for att in body.get("attributes") or []:
                aid = (att.get("id") or "").strip().upper()
                if aid in ("BRAND", "MARCA"):
                    val = att.get("value_name") or att.get("value_id")
                    marca = str(val) if val is not None else ""
                elif aid in ("COLOR", "COLOUR"):
                    val = att.get("value_name") or att.get("value_id")
                    if val:
                        color = str(val)
                        break
                elif aid == "SELLER_SKU":
                    v = att.get("value_name") or att.get("value") or att.get("value_id")
                    if v is None and att.get("values"):
                        v = (att["values"][0] or {}).get("name") or (att["values"][0] or {}).get("value_name")
                    if v is not None:
                        seller_sku = str(v).strip()
            if not seller_sku:
                seller_sku = (body.get("seller_custom_field") or "").strip()
            if not seller_sku:
                for var in body.get("variations") or []:
                    for vatt in (var.get("attribute_combinations") or var.get("attributes") or []):
                        if (vatt.get("id") or "").strip().upper() == "SELLER_SKU":
                            v = vatt.get("value_name") or vatt.get("value") or vatt.get("value_id")
                            if v is not None:
                                seller_sku = str(v).strip()
                                break
                    if seller_sku:
                        break
            if not color:
                tit = (body.get("title") or "").lower()
                colores = ["negro", "blanco", "azul", "rojo", "gris", "verde", "amarillo", "naranja", "rosa", "marron", "beige", "celeste", "plateado", "dorado", "violeta", "multicolor", "silver", "space gray", "space grey", "gold", "negro espacial", "midnight"]
                equiv = {"silver": "Plateado", "space gray": "Gris", "space grey": "Gris", "gold": "Dorado", "negro espacial": "Negro", "midnight": "Negro"}
                for c in colores:
                    if c in tit:
                        color = equiv.get(c, c.capitalize())
                        break
            catalog_listing = body.get("catalog_listing") is True
            # original_price existe cuando ML tiene precio promocional fijado
            original_price = body.get("original_price") or body.get("base_price")
            thumbnail = body.get("thumbnail") or ""
            if not thumbnail and body.get("pictures"):
                pic = (body.get("pictures") or [{}])[0]
                thumbnail = pic.get("secure_url") or pic.get("url") or ""
            return {
                "id": body.get("id"),
                "title": body.get("title", ""),
                "thumbnail": thumbnail,
                "price": body.get("price"),
                "sale_price": body.get("sale_price"),
                "original_price": original_price,
                "available_quantity": body.get("available_quantity"),
                "sold_quantity": body.get("sold_quantity"),
                "status": body.get("status", ""),
                "permalink": body.get("permalink", ""),
                "catalog_product_id": body.get("catalog_product_id"),
                "catalog_listing": catalog_listing,
                "listing_type_id": body.get("listing_type_id"),
                "sale_terms": body.get("sale_terms"),
                "seller_sku": seller_sku,
                "marca": marca or "—",
                "color": color or "—",
                "last_updated": body.get("last_updated"),
                "stop_time": body.get("stop_time"),
                "date_created": body.get("date_created"),
            }

        for item_data in items_resp.json():
            if isinstance(item_data, dict) and item_data.get("code") == 200:
                body = item_data.get("body", {})
                all_items.append(_item_from_body(body))
            elif isinstance(item_data, dict) and "body" in item_data:
                body = item_data["body"]
                all_items.append(_item_from_body(body))

    return {"results": all_items, "paging": {"total": total}, "seller_id": ml_user_id}


def _tipo_publicacion_desde_item(item: Dict[str, Any]) -> str:
    """Propia o Catálogo según catalog_listing (igual que en Ventas)."""
    if not item or not isinstance(item, dict):
        return "Propia"
    cl = item.get("catalog_listing")
    return "Catálogo" if (cl is True or str(cl or "").lower() in ("true", "1")) else "Propia"


def _cuotas_desde_item(item: Dict[str, Any]) -> str:
    """x1, x3, x6, x9 o x12 según listing_type_id y sale_terms/attributes (INSTALLMENTS_CAMPAIGN)."""
    listing_type_id = str(item.get("listing_type_id") or "").strip().lower()
    if listing_type_id == "gold_special":
        return "x1"
    if listing_type_id == "gold_pro":
        def _cuotas_desde_campaign(terms: list) -> str:
            for a in terms or []:
                if isinstance(a, dict) and (str(a.get("id") or "").upper() == "INSTALLMENTS_CAMPAIGN"):
                    vn = str(a.get("value_name") or "").lower()
                    if "12x" in vn:
                        return "x12"
                    if "9x" in vn:
                        return "x9"
                    if "6x" in vn:
                        return "x6"
                    if "3x" in vn or "3x_campaign" in vn or vn == "3x_campaign":
                        return "x3"
            return ""
        cuotas = _cuotas_desde_campaign(item.get("sale_terms")) or _cuotas_desde_campaign(item.get("attributes"))
        if cuotas:
            return cuotas
        return "x6"  # gold_pro por defecto: 6 cuotas
    return "x1"


def _body_to_precios_item(body: dict) -> dict:
    """Convierte el body de la API /items al formato usado en Precios (igual que _item_from_body en ml_get_my_items)."""
    marca = ""
    color = ""
    seller_sku = ""
    for att in body.get("attributes") or []:
        aid = (att.get("id") or "").strip().upper()
        if aid in ("BRAND", "MARCA"):
            val = att.get("value_name") or att.get("value_id")
            marca = str(val) if val is not None else ""
        elif aid in ("COLOR", "COLOUR"):
            val = att.get("value_name") or att.get("value_id")
            if val:
                color = str(val)
                break
        elif aid == "SELLER_SKU":
            v = att.get("value_name") or att.get("value") or att.get("value_id")
            if v is None and att.get("values"):
                v = (att["values"][0] or {}).get("name") or (att["values"][0] or {}).get("value_name")
            if v is not None:
                seller_sku = str(v).strip()
    if not seller_sku:
        seller_sku = (body.get("seller_custom_field") or "").strip()
    if not seller_sku:
        for var in body.get("variations") or []:
            for vatt in (var.get("attribute_combinations") or var.get("attributes") or []):
                if (vatt.get("id") or "").strip().upper() == "SELLER_SKU":
                    v = vatt.get("value_name") or vatt.get("value") or vatt.get("value_id")
                    if v is not None:
                        seller_sku = str(v).strip()
                        break
            if seller_sku:
                break
    if not color:
        tit = (body.get("title") or "").lower()
        colores = ["negro", "blanco", "azul", "rojo", "gris", "verde", "amarillo", "naranja", "rosa", "marron", "beige", "celeste", "plateado", "dorado", "violeta", "multicolor"]
        for c in colores:
            if c in tit:
                color = c.capitalize()
                break
    catalog_listing = body.get("catalog_listing") is True
    original_price = body.get("original_price") or body.get("base_price")
    thumbnail = body.get("thumbnail") or ""
    if not thumbnail and body.get("pictures"):
        pic = (body.get("pictures") or [{}])[0]
        thumbnail = pic.get("secure_url") or pic.get("url") or ""
    return {
        "id": body.get("id"),
        "title": body.get("title", ""),
        "thumbnail": thumbnail,
        "price": body.get("price"),
        "sale_price": body.get("sale_price"),
        "original_price": original_price,
        "available_quantity": body.get("available_quantity"),
        "sold_quantity": body.get("sold_quantity"),
        "status": body.get("status", ""),
        "permalink": body.get("permalink", ""),
        "catalog_product_id": body.get("catalog_product_id"),
        "catalog_listing": catalog_listing,
        "listing_type_id": body.get("listing_type_id"),
        "sale_terms": body.get("sale_terms"),
        "seller_sku": seller_sku,
        "marca": marca or "—",
        "color": color or "—",
        "last_updated": body.get("last_updated"),
        "stop_time": body.get("stop_time"),
        "date_created": body.get("date_created"),
    }


def ml_update_item_price(access_token: str, item_id: str, price: float) -> Dict[str, Any]:
    """Actualiza el precio de una publicación en MercadoLibre (PUT /items/{id}). Solo publicaciones propias."""
    base = "https://api.mercadolibre.com"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    resp = requests.put(
        f"{base}/items/{item_id}",
        headers=headers,
        json={"price": int(round(price))},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def ml_get_one_item_full(access_token: str) -> Optional[Dict[str, Any]]:
    """Obtiene el JSON completo de una publicación de ejemplo (la primera) para mostrar qué datos devuelve ML."""
    base = "https://api.mercadolibre.com"
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}

    me = requests.get(f"{base}/users/me", headers=headers, timeout=10)
    me.raise_for_status()
    ml_user_id = me.json().get("id")
    if not ml_user_id:
        return None

    search = requests.get(
        f"{base}/users/{ml_user_id}/items/search",
        headers=headers,
        params={"limit": 1, "offset": 0, "status": "active"},
        timeout=15,
    )
    search.raise_for_status()
    item_ids = search.json().get("results", [])
    if not item_ids:
        return None

    item_id = item_ids[0]
    item_resp = requests.get(
        f"{base}/items/{item_id}",
        headers=headers,
        timeout=15,
    )
    item_resp.raise_for_status()
    return item_resp.json()


def ml_get_item_sale_price(access_token: Optional[str], item_id: str) -> Optional[float]:
    """Obtiene el precio de venta actual de un ítem. API: GET /items/{id}/sale_price
    Requiere token. Usar cuando /items no devuelve price (deprecado)."""
    if not access_token or not str(item_id).strip():
        return None
    try:
        resp = requests.get(
            f"https://api.mercadolibre.com/items/{item_id}/sale_price",
            params={"context": "channel_marketplace"},
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict):
            amt = data.get("amount")
            if amt is not None:
                try:
                    return float(amt)
                except (TypeError, ValueError):
                    pass
    except Exception:
        pass
    return None


def ml_get_item_sale_price_full(access_token: Optional[str], item_id: str) -> Optional[Dict[str, Any]]:
    """Obtiene amount, regular_amount, promotion_id, promotion_type y campaign_id de GET /items/{id}/sale_price.
    promotion_id/type pueden estar en metadata (API ML a veces los pone ahí)."""
    if not access_token or not str(item_id).strip():
        return None
    try:
        resp = requests.get(
            f"https://api.mercadolibre.com/items/{item_id}/sale_price",
            params={"context": "channel_marketplace"},
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict):
            amt = data.get("amount")
            reg = data.get("regular_amount")
            if amt is not None:
                try:
                    metadata = data.get("metadata") or {}
                    meta = metadata if isinstance(metadata, dict) else {}
                    out = {
                        "amount": float(amt),
                        "regular_amount": float(reg) if reg is not None else None,
                        "promotion_id": data.get("promotion_id") or meta.get("promotion_id"),
                        "promotion_type": (data.get("promotion_type") or meta.get("promotion_type") or "").strip() or None,
                        "campaign_id": data.get("campaign_id") or meta.get("campaign_id"),
                    }
                    return out
                except (TypeError, ValueError):
                    pass
    except Exception:
        pass
    return None


def ml_get_promotion_item_discounts_by_user(
    access_token: Optional[str], item_id: str, user_id: str, total_discount_pct: float
) -> Optional[Dict[str, float]]:
    """Fallback: cuando sale_price no devuelve promotion_id, buscar en promociones del usuario."""
    if not access_token or not item_id or not user_id:
        return None
    try:
        resp = requests.get(
            "https://api.mercadolibre.com/seller-promotions/users/" + str(user_id),
            params={"app_version": "v2"},
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
            timeout=10,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        promos = data.get("results") or []
        item_id_str = str(item_id or "").strip()
        item_id_short = item_id_str[3:] if item_id_str.upper().startswith("MLA") and len(item_id_str) > 3 else item_id_str
        for p in promos:
            if not isinstance(p, dict):
                continue
            promo_id = p.get("id")
            promo_type = (p.get("type") or "").strip().upper()
            if not promo_id or not promo_type:
                continue
            try:
                items_resp = requests.get(
                    f"https://api.mercadolibre.com/seller-promotions/promotions/{promo_id}/items",
                    params={"promotion_type": promo_type, "item_id": item_id, "app_version": "v2"},
                    headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
                    timeout=10,
                )
                if items_resp.status_code != 200:
                    continue
                items_data = items_resp.json()
                results = items_data.get("results") or []
                for r in results:
                    rid = str(r.get("id", "")).strip() if isinstance(r, dict) else ""
                    rid_short = rid[3:] if rid.upper().startswith("MLA") and len(rid) > 3 else rid
                    if rid and (rid == item_id_str or rid_short == item_id_short):
                        meli = r.get("meli_percentage") or r.get("meli_percent")
                        seller = r.get("seller_percentage") or r.get("seller_percent")
                        if meli is not None or seller is not None:
                            meli_f = float(meli or 0)
                            seller_f = float(seller or 0)
                            return {"meli_pct": meli_f, "seller_pct": seller_f}
                        benefits = p.get("benefits") or {}
                        meli = benefits.get("meli_percent") or benefits.get("meli_percentage")
                        seller = benefits.get("seller_percent") or benefits.get("seller_percentage")
                        if meli is not None or seller is not None:
                            meli_f = float(meli or 0)
                            seller_f = float(seller or 0)
                            if meli_f + seller_f > 0.01:
                                if abs((meli_f + seller_f) - total_discount_pct) < 1:
                                    return {"meli_pct": meli_f, "seller_pct": seller_f}
                                if abs((meli_f + seller_f) - 100) < 1:
                                    return {"meli_pct": total_discount_pct * meli_f / 100, "seller_pct": total_discount_pct * seller_f / 100}
                                return {"meli_pct": meli_f, "seller_pct": seller_f}
                        break
            except Exception:
                continue
    except Exception:
        pass
    return None


def ml_get_promotion_item_discounts_by_campaign(
    access_token: Optional[str], campaign_id: str, item_id: str, total_discount_pct: float, user_id: str,
    promotion_type_hint: Optional[str] = None,
) -> Optional[Dict[str, float]]:
    """Usa campaign_id de metadata. Si sale_price dio promotion_type (ej. MARKETPLACE_CAMPAIGN), lo prueba primero."""
    if not access_token or not campaign_id or not item_id or not user_id:
        return None
    cid = str(campaign_id).strip()
    pt_hint = (promotion_type_hint or "").strip().upper() if promotion_type_hint else ""
    try:
        if pt_hint and pt_hint not in ("OFFER", "OFFER-"):
            out = ml_get_promotion_item_discounts(access_token, cid, pt_hint, item_id, total_discount_pct)
            if out:
                return out
        resp = requests.get(
            "https://api.mercadolibre.com/seller-promotions/users/" + str(user_id),
            params={"app_version": "v2"},
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            promos = data.get("results") or []
            cid_norm = (cid[2:] if cid.upper().startswith("P-") else cid).upper()
            for p in promos:
                if not isinstance(p, dict):
                    continue
                promo_id = str(p.get("id") or "").strip()
                promo_type = (p.get("type") or "").strip().upper()
                pid_norm = (promo_id[2:] if promo_id.upper().startswith("P-") else promo_id).upper()
                if promo_type and pid_norm == cid_norm:
                    out = ml_get_promotion_item_discounts(
                        access_token, cid, promo_type, item_id, total_discount_pct
                    )
                    if out:
                        return out
        if cid.upper().startswith("P-MLA"):
            for fallback_type in ("SMART", "MARKETPLACE_CAMPAIGN"):
                if fallback_type == pt_hint:
                    continue
                out = ml_get_promotion_item_discounts(access_token, cid, fallback_type, item_id, total_discount_pct)
                if out:
                    return out
    except Exception:
        pass
    return None


def _find_item_in_promo_results(
    results: List[Dict], item_id: str, total_discount_pct: float
) -> Optional[Dict[str, float]]:
    """Busca el item en results y devuelve {meli_pct, seller_pct} si tiene meli/seller."""
    item_id_str = str(item_id or "").strip()
    item_id_short = item_id_str[3:] if item_id_str.upper().startswith("MLA") and len(item_id_str) > 3 else item_id_str
    for r in results:
        rid = str(r.get("id", "")).strip() if isinstance(r, dict) else ""
        rid_short = rid[3:] if rid.upper().startswith("MLA") and len(rid) > 3 else rid
        if rid and (rid == item_id_str or rid_short == item_id_short):
            meli = r.get("meli_percentage") or r.get("meli_percent")
            seller = r.get("seller_percentage") or r.get("seller_percent")
            if meli is not None or seller is not None:
                meli_f = float(meli or 0)
                seller_f = float(seller or 0)
                if meli_f + seller_f > 0.01:
                    if abs((meli_f + seller_f) - total_discount_pct) < 1:
                        return {"meli_pct": meli_f, "seller_pct": seller_f}
                    if abs((meli_f + seller_f) - 100) < 1:
                        return {"meli_pct": total_discount_pct * meli_f / 100, "seller_pct": total_discount_pct * seller_f / 100}
                    return {"meli_pct": meli_f, "seller_pct": seller_f}
            break
    return None


def ml_get_promotion_item_discounts(
    access_token: Optional[str], promotion_id: str, promotion_type: str, item_id: str,
    total_discount_pct: float,
) -> Optional[Dict[str, float]]:
    """Obtiene meli y seller % del ítem en la promo. Los benefits pueden ser puntos % (meli+seller=total) o proporción (meli+seller=100)."""
    if not access_token or not promotion_id or not promotion_type or not item_id:
        return None
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    try:
        resp = requests.get(
            f"https://api.mercadolibre.com/seller-promotions/promotions/{promotion_id}/items",
            params={"promotion_type": promotion_type, "item_id": item_id, "app_version": "v2"},
            headers=headers,
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            results = data.get("results") or []
            out = _find_item_in_promo_results(results, item_id, total_discount_pct)
            if out:
                return out
            if not results:
                for offset in range(0, 200, 50):
                    r2 = requests.get(
                        f"https://api.mercadolibre.com/seller-promotions/promotions/{promotion_id}/items",
                        params={"promotion_type": promotion_type, "app_version": "v2", "limit": 50, "offset": offset},
                        headers=headers,
                        timeout=10,
                    )
                    if r2.status_code != 200:
                        break
                    data2 = r2.json()
                    results2 = data2.get("results") or []
                    out = _find_item_in_promo_results(results2, item_id, total_discount_pct)
                    if out:
                        return out
                    total = data2.get("paging", {}).get("total", 0)
                    if offset + len(results2) >= total or not results2:
                        break
        promo_resp = requests.get(
            f"https://api.mercadolibre.com/seller-promotions/promotions/{promotion_id}",
            params={"promotion_type": promotion_type, "app_version": "v2"},
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
            timeout=10,
        )
        if promo_resp.status_code == 200:
            promo_data = promo_resp.json()
            benefits = promo_data.get("benefits") or {}
            meli = benefits.get("meli_percent") or benefits.get("meli_percentage")
            seller = benefits.get("seller_percent") or benefits.get("seller_percentage")
            if meli is not None or seller is not None:
                meli_f = float(meli or 0)
                seller_f = float(seller or 0)
                if meli_f + seller_f > 0.01:
                    if abs((meli_f + seller_f) - total_discount_pct) < 1:
                        return {"meli_pct": meli_f, "seller_pct": seller_f}
                    if abs((meli_f + seller_f) - 100) < 1:
                        return {"meli_pct": total_discount_pct * meli_f / 100, "seller_pct": total_discount_pct * seller_f / 100}
                    return {"meli_pct": meli_f, "seller_pct": seller_f}
                elif meli_f > 0 or seller_f > 0:
                    if meli_f > 0 and seller_f == 0:
                        seller_inferred = max(0, total_discount_pct - meli_f)
                        return {"meli_pct": meli_f, "seller_pct": seller_inferred}
                    if seller_f > 0 and meli_f == 0:
                        meli_inferred = max(0, total_discount_pct - seller_f)
                        return {"meli_pct": meli_inferred, "seller_pct": seller_f}
    except Exception:
        pass
    return None


def ml_get_item_prices(access_token: Optional[str], item_id: str) -> Optional[float]:
    """Obtiene precios de un ítem. API: GET /items/{id}/prices. Fallback si sale_price falla."""
    if not access_token or not str(item_id).strip():
        return None
    try:
        resp = requests.get(
            f"https://api.mercadolibre.com/items/{item_id}/prices",
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict):
            prices = data.get("prices") or []
            for p in prices if isinstance(prices, list) else []:
                if isinstance(p, dict):
                    amt = p.get("amount")
                    if amt is not None:
                        try:
                            return float(amt)
                        except (TypeError, ValueError):
                            pass
    except Exception:
        pass
    return None


def ml_enriquecer_sale_price(items: List[Dict[str, Any]], access_token: Optional[str]) -> None:
    """Enriquece items con sale_price (precio real con promoción) si no lo tienen."""
    if not access_token:
        return
    for i in items:
        if i.get("sale_price") is not None:
            continue
        item_id = i.get("id")
        if not item_id:
            continue
        sp = ml_get_item_sale_price(access_token, str(item_id))
        if sp is not None:
            i["sale_price"] = sp


def ml_fetch_price_for_item(
    access_token: Optional[str], item_id: str, body: Optional[Dict[str, Any]] = None
) -> Optional[float]:
    """Obtiene el precio: primero del body, luego sale_price, luego prices."""
    if body is not None:
        for key in ("price", "base_price", "original_price"):
            val = body.get(key)
            if val is not None:
                try:
                    return float(val)
                except (TypeError, ValueError):
                    pass
    if not access_token:
        return None
    return ml_get_item_sale_price(access_token, item_id) or ml_get_item_prices(access_token, item_id)


def ml_get_product_detail(access_token: Optional[str], product_id: str) -> Optional[Dict[str, Any]]:
    """Obtiene detalle de producto de catálogo. Puede incluir buy_box_winner_price_range."""
    if not access_token or not str(product_id).strip():
        return None
    try:
        resp = requests.get(
            f"https://api.mercadolibre.com/products/{product_id}",
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json() if isinstance(resp.json(), dict) else None
    except Exception:
        return None


def _extraer_color_desde_texto(texto: str) -> str:
    """Busca palabras de color en un texto. Devuelve la primera coincidencia o ''."""
    if not texto or not isinstance(texto, str):
        return ""
    t = texto.lower()
    colores = ["negro", "blanco", "azul", "rojo", "gris", "verde", "amarillo", "naranja", "rosa", "marron", "beige", "celeste", "plateado", "dorado", "violeta", "multicolor", "black", "white", "blue", "red", "gray", "grey", "green", "yellow", "orange", "pink", "brown", "silver", "gold"]
    for c in colores:
        if c in t:
            return c.capitalize()
    return ""


def ml_get_item_description(access_token: Optional[str], item_id: str) -> str:
    """Obtiene el texto de la descripción del ítem. Devuelve '' si falla."""
    if not access_token or not str(item_id).strip():
        return ""
    try:
        resp = requests.get(
            f"https://api.mercadolibre.com/items/{item_id}/descriptions",
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
            timeout=8,
        )
        if not resp.ok:
            return ""
        data = resp.json()
        if not isinstance(data, list) or not data:
            return ""
        for d in data:
            if isinstance(d, dict):
                txt = d.get("plain_text") or d.get("text") or ""
                if txt:
                    return str(txt)
        return ""
    except Exception:
        return ""


def ml_get_item(access_token: Optional[str], item_id: str) -> Optional[Dict[str, Any]]:
    """Obtiene el detalle completo de un ítem (precio, stock, seller_id, etc.) por ID.
    Prueba con token y, si falla, sin token (GET /items/{id} a veces es público)."""
    item_id = str(item_id).strip()
    if not item_id:
        return None
    tries = [{"Accept": "application/json"}]
    if access_token:
        tries.insert(0, {"Accept": "application/json", "Authorization": f"Bearer {access_token}"})
    for headers in tries:
        try:
            resp = requests.get(
                f"https://api.mercadolibre.com/items/{item_id}",
                headers=headers,
                timeout=12,
            )
            resp.raise_for_status()
            data = resp.json()
            # La API puede devolver el ítem en "body" (multiget) o en la raíz
            if isinstance(data, dict) and "body" in data and data.get("code") == 200:
                return data.get("body") or data
            return data if isinstance(data, dict) else None
        except Exception:
            continue
    return None


def ml_get_items_multiget(access_token: Optional[str], item_ids: List[str]) -> List[Optional[Dict[str, Any]]]:
    """Obtiene varios ítem en una sola petición. API: GET /items?ids=ID1,ID2,ID3
    Documentación ML: la respuesta es un array en el mismo orden que los ids;
    cada elemento es { \"code\": 200, \"body\": { id, title, price, available_quantity, seller_id, permalink } }.
    Prueba sin token primero (listados públicos), luego con token."""
    if not item_ids:
        return []
    ids_clean = [str(i).strip() for i in item_ids if str(i).strip()][:20]
    if not ids_clean:
        return [None] * len(item_ids)
    ids_str = ",".join(ids_clean)
    # Sin attributes: ML está deprecando price en /items; sale_price se usa como fallback
    url = f"https://api.mercadolibre.com/items?ids={ids_str}"
    for headers in (
        ([{"Accept": "application/json", "Authorization": f"Bearer {access_token}"}] if access_token else []),
        [{"Accept": "application/json"}],
    ):
        if not headers:
            continue
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            continue
        # La API puede devolver array o un solo objeto { code, body } cuando hay un id
        if isinstance(data, dict) and "body" in data:
            data = [data]
        if not isinstance(data, list):
            continue
        out = []
        for elem in data:
            if isinstance(elem, dict) and elem.get("code") == 200:
                body = elem.get("body")
                out.append(body if isinstance(body, dict) else None)
            else:
                out.append(None)
        return out
    return [None] * len(ids_clean)


def ml_get_items_multiget_with_attributes(
    access_token: Optional[str], item_ids: List[str], attributes: str = "id,catalog_listing,catalog_product_id,attributes"
) -> List[Optional[Dict[str, Any]]]:
    """Obtiene ítems pidiendo atributos específicos (para catalog_listing). Máx 20 ids."""
    if not item_ids:
        return []
    ids_clean = [str(i).strip() for i in item_ids if str(i).strip()][:20]
    if not ids_clean:
        return [None] * len(item_ids)
    ids_str = ",".join(ids_clean)
    url = f"https://api.mercadolibre.com/items?ids={ids_str}&attributes={attributes}"
    if access_token:
        headers = {"Accept": "application/json", "Authorization": f"Bearer {access_token}"}
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            return [None] * len(ids_clean)
    else:
        return [None] * len(ids_clean)
    if isinstance(data, dict) and "body" in data:
        data = [data]
    if not isinstance(data, list):
        return [None] * len(ids_clean)
    out = []
    for elem in data:
        if isinstance(elem, dict) and elem.get("code") == 200:
            body = elem.get("body")
            out.append(body if isinstance(body, dict) else None)
        else:
            out.append(None)
    return out


def ml_get_items_multiget_all(
    access_token: Optional[str], item_ids: List[str]
) -> List[Optional[Dict[str, Any]]]:
    """Obtiene varios ítems en lotes de 20 (límite de la API). Devuelve lista en el mismo orden."""
    if not item_ids:
        return []
    ids_clean = [str(i).strip() for i in item_ids if str(i).strip()]
    out: List[Optional[Dict[str, Any]]] = []
    for i in range(0, len(ids_clean), 20):
        batch = ids_clean[i : i + 20]
        batch_bodies = ml_get_items_multiget(access_token, batch)
        out.extend(batch_bodies)
    return out


def ml_get_users_multiget(
    access_token: Optional[str], user_ids: List[str]
) -> Dict[str, str]:
    """Obtiene nicknames de usuarios. GET /users?ids=ID1,ID2. Devuelve {user_id: nickname}."""
    if not user_ids:
        return {}
    ids_clean = list(dict.fromkeys(str(i).strip() for i in user_ids if str(i).strip()))[:20]
    if not ids_clean:
        return {}
    ids_str = ",".join(ids_clean)
    url = f"https://api.mercadolibre.com/users?ids={ids_str}"
    headers_list = (
        [{"Accept": "application/json", "Authorization": f"Bearer {access_token}"}] if access_token else []
    ) + [{"Accept": "application/json"}]
    for h in headers_list:
        try:
            resp = requests.get(url, headers=h, timeout=12)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict) and "body" in data:
                data = [data]
            if not isinstance(data, list):
                continue
            out: Dict[str, str] = {}
            for elem in data:
                if isinstance(elem, dict) and elem.get("code") == 200:
                    body = elem.get("body")
                    if isinstance(body, dict):
                        uid = str(body.get("id", ""))
                        nick = (body.get("nickname") or "").strip()
                        if uid:
                            out[uid] = nick or f"ID {uid}"
            return out
        except Exception:
            continue
    return {}


def ml_get_user_id(access_token: str) -> Optional[str]:
    """Obtiene el user_id de MercadoLibre del token (seller_id)."""
    try:
        resp = requests.get(
            "https://api.mercadolibre.com/users/me",
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
            timeout=10,
        )
        resp.raise_for_status()
        return str(resp.json().get("id", "")) or None
    except Exception:
        return None


def ml_get_user_profile(access_token: str) -> Optional[Dict[str, Any]]:
    """Obtiene perfil completo (users/me + users/{id}) con reputación y métricas."""
    try:
        headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
        me = requests.get("https://api.mercadolibre.com/users/me", headers=headers, timeout=10)
        me.raise_for_status()
        data = me.json()
        user_id = data.get("id")
        if not user_id:
            return data
        full = requests.get(f"https://api.mercadolibre.com/users/{user_id}", headers=headers, timeout=10)
        if full.ok:
            prof = full.json()
            # Si metrics vacíos o todo 0, intentar global seller_reputation (multi-marketplace)
            rep = prof.get("seller_reputation") or {}
            metrics = rep.get("metrics") or {}
            has_data = any(
                (metrics.get(k) or {}).get("rate") or (metrics.get(k) or {}).get("value")
                or ((metrics.get(k) or {}).get("excluded") or {}).get("real_rate")
                for k in ["claims", "cancellations", "delayed_handling_time"]
            )
            if not has_data:
                try:
                    gr = requests.get(
                        "https://api.mercadolibre.com/global/users/seller_reputation",
                        headers=headers,
                        timeout=10,
                    )
                    if gr.ok:
                        glob = gr.json()
                        # Respuesta: { user_id, site_id, seller_reputation: [{ user_id, site_id, seller_reputation }] }
                        arr = (glob or {}).get("seller_reputation") or []
                        for item in arr:
                            if str(item.get("user_id")) == str(user_id):
                                sr = item.get("seller_reputation") or {}
                                if sr.get("metrics"):
                                    prof.setdefault("seller_reputation", {})["metrics"] = sr.get("metrics", {})
                                break
                        if not arr and (glob or {}).get("seller_reputation"):
                            sr = (glob.get("seller_reputation") or [{}])[0]
                            if isinstance(sr, dict) and sr.get("metrics"):
                                prof.setdefault("seller_reputation", {})["metrics"] = sr.get("metrics", {})
                except Exception:
                    pass
            return prof
        return data
    except Exception:
        return None


ORDERS_MAX_OFFSET = 100000  # ML puede limitar offset; si devuelve 400 se detiene antes


def ml_get_orders(
    access_token: str,
    seller_id: str,
    limit: int = 100,
    offset: int = 0,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> Dict[str, Any]:
    """Lista órdenes del vendedor. Pagina hasta `limit` (máx 50 por request, ML no acepta más).
    sort=date_desc para órdenes más recientes primero.
    date_from/date_to: ISO 8601 (ej. 2025-02-01T00:00:00.000-03:00) para filtrar por fecha."""
    import logging
    log = logging.getLogger(__name__)
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    page_size = 50
    date_params: Dict[str, str] = {}
    if date_from:
        date_params["order_created_from"] = date_from
    if date_to:
        date_params["order_created_to"] = date_to

    all_flat: List[Dict[str, Any]] = []
    seen_ids: set = set()

    def _do_fetch(use_date_filter: bool) -> None:
        nonlocal all_flat, seen_ids
        params_filter = date_params if use_date_filter else {}
        for url, extra in [
            ("https://api.mercadolibre.com/orders/search", {"seller": seller_id}),
            ("https://api.mercadolibre.com/orders/search", {"seller": seller_id, "caller.id": seller_id}),
            ("https://api.mercadolibre.com/marketplace/orders/search", {"seller.id": seller_id}),
            ("https://api.mercadolibre.com/marketplace/orders/search", {"seller.id": seller_id, "caller.id": seller_id}),
        ]:
            off = offset
            while len(all_flat) < limit and off <= ORDERS_MAX_OFFSET:
                params: Dict[str, Any] = {**extra, **params_filter, "limit": page_size, "offset": off, "sort": "date_desc"}
                try:
                    resp = requests.get(url, params=params, headers=headers, timeout=25)
                    if not resp.ok:
                        if off == offset:
                            try:
                                err_body = resp.json()
                            except Exception:
                                err_body = resp.text[:300]
                            log.debug("ML orders %s %s: %s", url.split("/")[-1], resp.status_code, err_body)
                        break
                    data = resp.json()
                    raw = data.get("results") or data.get("orders") or data.get("elements") or []

                    if not raw:
                        break

                    if isinstance(raw[0], (int, float)):
                        for oid in raw[:page_size]:
                            try:
                                r = requests.get(f"https://api.mercadolibre.com/orders/{int(oid)}", headers={**headers, "x-format-new": "true"}, timeout=10)
                                if r.status_code == 200:
                                    ob = r.json()
                                    oid_val = ob.get("id")
                                    if oid_val and str(oid_val) not in seen_ids:
                                        seen_ids.add(str(oid_val))
                                        all_flat.append(ob)
                            except Exception:
                                pass
                        off += len(raw)
                        if len(raw) < page_size:
                            break
                        continue

                    for o in _flatten_raw(raw):
                        oid_val = o.get("id")
                        if oid_val and str(oid_val) not in seen_ids:
                            seen_ids.add(str(oid_val))
                            all_flat.append(o)
                    off += len(raw)
                    if len(raw) < page_size:
                        break
                except Exception as ex:
                    log.debug("ML orders %s: %s", url.split("/")[-1], ex)
                    break

            if len(all_flat) >= limit:
                break

    def _flatten_raw(raw_list: list) -> list:
        out = []
        for r in raw_list:
            if not isinstance(r, dict):
                continue
            nested = r.get("orders") or []
            if nested:
                for o in nested:
                    if isinstance(o, dict):
                        out.append(o)
            else:
                out.append(r)
        return out

    for url, extra in [
        ("https://api.mercadolibre.com/orders/search", {"seller": seller_id}),
        ("https://api.mercadolibre.com/orders/search", {"seller": seller_id, "caller.id": seller_id}),
        ("https://api.mercadolibre.com/marketplace/orders/search", {"seller.id": seller_id}),
        ("https://api.mercadolibre.com/marketplace/orders/search", {"seller.id": seller_id, "caller.id": seller_id}),
    ]:
        off = offset
        while len(all_flat) < limit and off <= ORDERS_MAX_OFFSET:
            params: Dict[str, Any] = {**extra, **date_params, "limit": page_size, "offset": off, "sort": "date_desc"}
            try:
                resp = requests.get(url, params=params, headers=headers, timeout=25)
                if not resp.ok:
                    if off == offset:
                        try:
                            err_body = resp.json()
                        except Exception:
                            err_body = resp.text[:300]
                        log.debug("ML orders %s %s: %s", url.split("/")[-1], resp.status_code, err_body)
                    break
                data = resp.json()
                raw = data.get("results") or data.get("orders") or data.get("elements") or []

                if not raw:
                    break

                if isinstance(raw[0], (int, float)):
                    for oid in raw[:page_size]:
                        try:
                            r = requests.get(f"https://api.mercadolibre.com/orders/{int(oid)}", headers=headers, timeout=10)
                            if r.status_code == 200:
                                ob = r.json()
                                oid_val = ob.get("id")
                                if oid_val and str(oid_val) not in seen_ids:
                                    seen_ids.add(str(oid_val))
                                    all_flat.append(ob)
                        except Exception:
                            pass
                    off += len(raw)
                    if len(raw) < page_size:
                        break
                    continue

                for o in _flatten_raw(raw):
                    oid_val = o.get("id")
                    if oid_val and str(oid_val) not in seen_ids:
                        seen_ids.add(str(oid_val))
                        all_flat.append(o)
                off += len(raw)
                if len(raw) < page_size:
                    break
            except Exception as ex:
                log.debug("ML orders %s: %s", url.split("/")[-1], ex)
                break

        if len(all_flat) >= limit:
            break

    if all_flat:
        faltan_items = [o for o in all_flat[:limit] if not (o.get("order_items") or o.get("items")) and o.get("id")]
        fetches = 0
        max_enrich = min(2000, len(faltan_items))
        for o in faltan_items[:max_enrich]:
            if fetches >= 1000:
                break
            try:
                r = requests.get(f"https://api.mercadolibre.com/orders/{o['id']}", headers=headers, timeout=10)
                if r.status_code == 200:
                    full = r.json()
                    idx = next((i for i, x in enumerate(all_flat) if x.get("id") == o["id"]), -1)
                    if idx >= 0 and (full.get("order_items") or full.get("items")):
                        all_flat[idx] = full
                        fetches += 1
            except Exception:
                pass
        log.debug("ML orders: %d órdenes total", len(all_flat))
        return {"results": all_flat[:limit], "paging": {"total": len(all_flat)}}

    return {"results": [], "paging": {"total": 0}, "error": "No se pudo obtener órdenes"}


def ml_search_similar(
    query: str, limit: int = 20, access_token: Optional[str] = None, solo_propias: bool = False
) -> Dict[str, Any]:
    """Busca publicaciones en /sites/MLA/search (listados con precio, vendedor, stock).
    Solo devuelve resultados cuando hay datos completos. No usa catálogo (sin precio/vendedor)."""
    base = "https://api.mercadolibre.com"
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
        "Referer": "https://www.mercadolibre.com.ar/",
    }
    if access_token:
        headers["Authorization"] = f"Bearer {access_token}"

    search_params: Dict[str, Any] = {"q": query[:200], "limit": limit}

    for try_headers in (
        {**headers} if access_token else {},
        {"Accept": "application/json", "User-Agent": headers.get("User-Agent", "Mozilla/5.0")},
    ):
        if not try_headers:
            continue
        try:
            resp = requests.get(
                f"{base}/sites/MLA/search",
                params=search_params,
                headers=try_headers,
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results", [])
            if solo_propias:
                results = [r for r in results if isinstance(r, dict) and r.get("catalog_listing") is not True]
            return {"results": results, "paging": data.get("paging", {})}
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code in (401, 403):
                continue
            raise
        except Exception:
            continue

    # Fallback: usar catálogo (products/search) - trae nombre y enlace
    if access_token:
        last_403_msg = None
        for params in (
            {"site_id": "MLA", "status": "active", "q": query[:200], "limit": limit},
            {"site_id": "MLA", "q": query[:200], "limit": limit},
        ):
            try:
                prod_resp = requests.get(
                    f"{base}/products/search",
                    params=params,
                    headers=headers,
                    timeout=15,
                )
                prod_resp.raise_for_status()
                prod_data = prod_resp.json()
                raw = prod_data.get("results", [])
                results = []
                for r in raw:
                    if not isinstance(r, dict):
                        continue
                    row = dict(r)
                    if "name" in row and "title" not in row:
                        row["title"] = row["name"]
                    row["catalog_listing"] = True
                    row["permalink"] = f"https://www.mercadolibre.com.ar/p/{row.get('id', '')}"
                    results.append(row)
                return {"results": results, "paging": prod_data.get("paging", {}), "from_catalog": True}
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 403:
                    last_403_msg = (
                        "MercadoLibre bloqueó el acceso (403). Revisá: "
                        "IP en DevCenter, scopes de la app, y que no esté bloqueada. "
                        "Más info: developers.mercadolibre.com.ar/es_ar/error-403"
                    )
                continue
            except Exception:
                continue
        return {"results": [], "paging": {"total": 0}, "error": last_403_msg or "No se pudo conectar con el catálogo de MercadoLibre."}

    return {"results": [], "paging": {"total": 0}, "error": "Vincular la cuenta en Configuración para buscar."}


# ==========================
# SESIÓN DE USUARIO (NiceGUI)
# ==========================


def get_current_user() -> Optional[Dict[str, Any]]:
    return app.storage.user.get("user")  # type: ignore[no-any-return]


def set_current_user(user: Optional[Dict[str, Any]]) -> None:
    if user:
        app.storage.user["user"] = user
    else:
        app.storage.user.clear()


def require_login() -> Optional[Dict[str, Any]]:
    user = get_current_user()
    if not user:
        ui.notify("Debes iniciar sesión para continuar", color="negative")
    return user


# ==========================
# INTERFAZ NICEGUI
# ==========================


def show_login_screen(container) -> None:
    """Muestra la pantalla de login/registro dentro de un contenedor."""
    container.clear()

    with container:
        # Fila a ancho completo, contenido centrado horizontalmente y más cerca del borde superior
        with ui.row().classes("w-full justify-center q-mt-xl"):
            with ui.column().classes("items-center gap-6"):
                ui.label("BDC systems").classes("text-3xl font-bold")

                with ui.card().classes("w-full max-w-md"):
                    ui.label("Iniciar sesión").classes("text-xl font-semibold mb-4")
                    username = ui.input("Usuario").classes("w-full")
                    password = ui.input(
                        "Contraseña",
                        password=True,
                        password_toggle_button=True,
                    ).classes("w-full")

                    with ui.row().classes("justify-between w-full mt-4"):
                        def on_login() -> None:
                            if not username.value or not password.value:
                                ui.notify("Completa usuario y contraseña", color="negative")
                                return
                            user = authenticate_user(username.value, password.value)
                            if not user:
                                ui.notify("Credenciales inválidas", color="negative")
                                return
                            set_current_user(user)
                            ui.notify(f"Bienvenido {user['username']}", color="positive")
                            show_main_layout(container)

                        def on_register() -> None:
                            with ui.dialog() as dlg:
                                dlg.props("persistent")
                                with ui.card().classes("p-4 min-w-[320px]"):
                                    ui.label("Registrarse").classes("text-lg font-bold")
                                    reg_email = ui.input("Email").classes("w-full").props("type=email")

                                    def _submit_reg() -> None:
                                        e = (reg_email.value or "").strip()
                                        if not e or "@" not in e:
                                            ui.notify("Ingresá un email válido", color="negative")
                                            return
                                        err, new_pwd = create_user(e)
                                        if err:
                                            if new_pwd:
                                                dlg.close()
                                                with ui.dialog() as popup:
                                                    popup.props("persistent")
                                                    with ui.card().classes("p-6 min-w-[400px]"):
                                                        ui.label("Error al enviar el email").classes("text-lg font-semibold text-warning")
                                                        ui.label(err).classes("text-sm text-gray-600 mt-2")
                                                        ui.label("Tu contraseña provisoria (copiala para iniciar sesión):").classes("text-sm font-medium mt-4")
                                                        with ui.row().classes("mt-2 p-3 bg-gray-100 rounded font-mono text-lg select-all"):
                                                            ui.label(new_pwd)
                                                        ui.button("Cerrar popup", on_click=popup.close).props("flat color=primary").classes("mt-4")
                                                popup.open()
                                            else:
                                                ui.notify(err, color="negative")
                                            return
                                        dlg.close()
                                        ui.notify(
                                            "Te enviamos un email con tu contraseña provisoria. Iniciá sesión y cambiá tu contraseña en Configuración.",
                                            color="positive",
                                        )

                                    with ui.row().classes("mt-3 gap-2 justify-end"):
                                        ui.button("Cancelar", on_click=dlg.close)
                                        ui.button("Registrarme", on_click=_submit_reg, color="primary")
                            dlg.open()

                        ui.button("Entrar", on_click=on_login, color="primary")
                        ui.button("Registrarme", on_click=on_register, color="secondary")


def show_main_layout(container) -> None:
    """Muestra el panel principal dentro de un contenedor."""
    container.clear()
    user = get_current_user()

    if not user:
        show_login_screen(container)
        return

    with container:
        perms = get_user_tab_permissions(user["id"])
        ml_linked = bool(get_ml_access_token(user["id"]))
        qb_tokens = get_qb_tokens(user["id"])
        qb_linked = bool(qb_tokens and qb_tokens.get("access_token"))

        # Tabs ocultos (solo para binding con tab_panels)
        with ui.element("div").classes("hidden"):
            with ui.tabs() as tabs:
                tab_home = ui.tab("Home")
                tab_estadisticas = ui.tab("Estadísticas")
                tab_ventas = ui.tab("Ventas")
                tab_precios = ui.tab("Productos")
                tab_precios_detalle = ui.tab("Precios")
                tab_compras = ui.tab("Invoices")
                tab_stock = ui.tab("Stock")
                tab_compras_lista = ui.tab("Compras")
                tab_pedidos = ui.tab("Pedidos")
                tab_historicos = ui.tab("Históricos")
                tab_busqueda = ui.tab("Búsqueda")
                tab_importacion = ui.tab("Importacion")
                tab_datos = ui.tab("Datos")
                tab_pesos = ui.tab("Pesos")
                tab_balance = ui.tab("Balance")
                tab_config = ui.tab("Configuración")
                tab_admin = ui.tab("Admin")

        tab_map = {
            "Home": tab_home,
            "Estadísticas": tab_estadisticas,
            "Ventas": tab_ventas,
            "Productos": tab_precios,
            "Precios": tab_precios_detalle,
            "Invoices": tab_compras,
            "Stock": tab_stock,
            "Compras": tab_compras_lista,
            "Pedidos": tab_pedidos,
            "Históricos": tab_historicos,
            "Búsqueda": tab_busqueda,
            "Importacion": tab_importacion,
            "Datos": tab_datos,
            "Pesos": tab_pesos,
            "Balance": tab_balance,
            "Configuración": tab_config,
            "Admin": tab_admin,
        }
        label_to_key = {"Home": "home", "Estadísticas": "estadisticas", "Ventas": "ventas", "Productos": "productos", "Precios": "precios", "Invoices": "compras", "Stock": "stock", "Compras": "compras_lista", "Pedidos": "pedidos", "Históricos": "historicos", "Búsqueda": "busqueda", "Importacion": "importacion", "Datos": "datos", "Pesos": "pesos", "Balance": "balance", "Configuración": "configuracion", "Admin": "admin"}

        # Lazy-load state
        precios_cargado = [False]
        precios_detalle_cargado = [False]
        ventas_cargado = [False]
        estadisticas_cargado = [False]
        balance_cargado = [False]
        compras_cargado = [False]
        stock_cargado = [False]
        compras_lista_cargado = [False]
        pedidos_cargado = [False]
        historicos_cargado = [False]
        admin_cargado = [False]

        def _lazy_load(val: str) -> None:
            if val == "Invoices" and not compras_cargado[0]:
                compras_cargado[0] = True
                build_tab_compras(compras_container)
            elif val == "Stock" and not stock_cargado[0]:
                stock_cargado[0] = True
                build_tab_stock(stock_container)
            elif val == "Compras" and not compras_lista_cargado[0]:
                compras_lista_cargado[0] = True
                build_tab_compras_lista(compras_lista_container)
            elif val == "Pedidos" and not pedidos_cargado[0]:
                pedidos_cargado[0] = True
                build_tab_pedidos(pedidos_container)
            elif val == "Productos" and not precios_cargado[0]:
                precios_cargado[0] = True
                build_tab_precios(precios_container)
            elif val == "Precios" and not precios_detalle_cargado[0]:
                precios_detalle_cargado[0] = True
                build_tab_precios_detalle(precios_detalle_container)
            elif val == "Ventas" and not ventas_cargado[0]:
                ventas_cargado[0] = True
                build_tab_ventas(ventas_container)
            elif val == "Estadísticas" and not estadisticas_cargado[0]:
                estadisticas_cargado[0] = True
                build_tab_estadisticas(estadisticas_container)
            elif val == "Balance" and not balance_cargado[0]:
                balance_cargado[0] = True
                build_tab_balance(balance_container)
            elif val == "Históricos" and not historicos_cargado[0]:
                historicos_cargado[0] = True
                build_tab_historicos(historicos_container)
            elif val == "Admin" and not admin_cargado[0]:
                admin_cargado[0] = True
                build_tab_admin(admin_container)

        # Siempre arrancar en Home
        tab_inicial = "Home"

        def _go(lbl: str):
            def f():
                tab_panels.value = tab_map[lbl]
                app.storage.user["last_tab"] = lbl
                _lazy_load(lbl)
            return f

        # Barra gris: navegación principal + secundaria | semáforos, versión, usuario
        # Menús secundarios se abren al pasar el mouse (hover). No se cierran al mover hacia los items.
        # Se cierran al seleccionar una opción o al hacer clic fuera (Quasar).
        _open_menus: List[Any] = []  # Referencias a menús abiertos para cerrar otros al abrir uno nuevo

        def _open_and_close_others(menu_obj: Any) -> None:
            for m in _open_menus:
                if m is not menu_obj:
                    try:
                        m.close()
                    except Exception:
                        pass
            _open_menus.clear()
            _open_menus.append(menu_obj)
            menu_obj.open()

        with ui.row().classes("w-full items-center q-pa-md bg-grey-2 gap-2 flex-wrap"):
            with ui.row().classes("items-center gap-1 shrink-0"):
                _nav_font = "text-lg font-medium"
                if perms.get("home", True):
                    ui.button("HOME", on_click=_go("Home")).props("flat dense no-caps").classes(_nav_font)
                ml_subs = [("ESTADÍSTICAS", "Estadísticas", "estadisticas"), ("VENTAS", "Ventas", "ventas"), ("PRODUCTOS", "Productos", "productos"), ("PRECIOS", "Precios", "precios"), ("BÚSQUEDA", "Búsqueda", "busqueda"), ("BALANCE", "Balance", "balance")]
                if any(perms.get(k, True) for _, _, k in ml_subs):
                    with ui.element("div").classes("relative inline-block").on("mouseenter", lambda: _open_and_close_others(ml_menu)):
                        with ui.button("MERCADOLIBRE").props("flat dense no-caps").classes(_nav_font):
                            with ui.menu().props("auto-close content-class=text-lg") as ml_menu:
                                for lbl_display, lbl_map, key in ml_subs:
                                    if perms.get(key, True):
                                        def _ml_click(l=lbl_map):
                                            _lazy_load(l)
                                            tab_panels.value = tab_map[l]
                                            app.storage.user["last_tab"] = l
                                        ui.menu_item(lbl_display, _ml_click)
                if perms.get("compras", True) or perms.get("stock", True) or perms.get("compras_lista", True) or perms.get("pedidos", True) or perms.get("historicos", True):
                    with ui.element("div").classes("relative inline-block").on("mouseenter", lambda: _open_and_close_others(compras_menu)):
                        with ui.button("BDC").props("flat dense no-caps").classes(_nav_font):
                            with ui.menu().props("auto-close content-class=text-lg") as compras_menu:
                                if perms.get("compras", True):
                                    def _compras_click():
                                        _lazy_load("Invoices")
                                        tab_panels.value = tab_compras
                                        app.storage.user["last_tab"] = "Invoices"
                                    ui.menu_item("INVOICES", _compras_click)
                                if perms.get("stock", True):
                                    def _stock_click():
                                        _lazy_load("Stock")
                                        tab_panels.value = tab_stock
                                        app.storage.user["last_tab"] = "Stock"
                                    ui.menu_item("STOCK", _stock_click)
                                if perms.get("compras_lista", True):
                                    def _compras_lista_click():
                                        _lazy_load("Compras")
                                        tab_panels.value = tab_compras_lista
                                        app.storage.user["last_tab"] = "Compras"
                                    ui.menu_item("COMPRAS", _compras_lista_click)
                                if perms.get("pedidos", True):
                                    def _pedidos_click():
                                        _lazy_load("Pedidos")
                                        tab_panels.value = tab_pedidos
                                        app.storage.user["last_tab"] = "Pedidos"
                                    ui.menu_item("PEDIDOS", _pedidos_click)
                                if perms.get("historicos", True):
                                    def _historicos_click():
                                        _lazy_load("Históricos")
                                        tab_panels.value = tab_historicos
                                        app.storage.user["last_tab"] = "Históricos"
                                    ui.menu_item("HISTÓRICOS", _historicos_click)
                if perms.get("importacion", True) or perms.get("pesos", True):
                    with ui.element("div").classes("relative inline-block").on("mouseenter", lambda: _open_and_close_others(comex_menu)):
                        with ui.button("COMEX").props("flat dense no-caps").classes(_nav_font):
                            with ui.menu().props("auto-close content-class=text-lg") as comex_menu:
                                if perms.get("importacion", True):
                                    def _imp_click():
                                        _lazy_load("Importacion")
                                        tab_panels.value = tab_importacion
                                        app.storage.user["last_tab"] = "Importacion"
                                    ui.menu_item("IMPORTACION", _imp_click)
                                if perms.get("pesos", True):
                                    def _pesos_click():
                                        _lazy_load("Pesos")
                                        tab_panels.value = tab_pesos
                                        app.storage.user["last_tab"] = "Pesos"
                                    ui.menu_item("PESOS", _pesos_click)
                if perms.get("datos", True) or perms.get("configuracion", True):
                    with ui.element("div").classes("relative inline-block").on("mouseenter", lambda: _open_and_close_others(config_menu)):
                        with ui.button("CONFIG").props("flat dense no-caps").classes(_nav_font):
                            with ui.menu().props("auto-close content-class=text-lg") as config_menu:
                                if perms.get("datos", True):
                                    def _datos_click():
                                        _lazy_load("Datos")
                                        tab_panels.value = tab_datos
                                        app.storage.user["last_tab"] = "Datos"
                                    ui.menu_item("DATOS", _datos_click)
                                if perms.get("configuracion", True):
                                    def _config_click():
                                        _lazy_load("Configuración")
                                        tab_panels.value = tab_config
                                        app.storage.user["last_tab"] = "Configuración"
                                    ui.menu_item("CONFIGURACIÓN", _config_click)
                if perms.get("admin", False):
                    ui.button("ADMIN", on_click=_go("Admin")).props("flat dense no-caps").classes(_nav_font)
            ui.space()
            with ui.row().classes("items-center gap-3 shrink-0"):
                with ui.row().classes("items-center gap-2"):
                    ui.element("span").classes("w-2.5 h-2.5 rounded-full").style(f"background:{'#22c55e' if ml_linked else '#ef4444'}")
                    ui.label("ML").classes("text-xs text-gray-600")
                with ui.row().classes("items-center gap-2"):
                    ui.element("span").classes("w-2.5 h-2.5 rounded-full").style(f"background:{'#22c55e' if qb_linked else '#ef4444'}")
                    ui.label("BDC").classes("text-xs text-gray-600")
                ui.label(f"Ver {VERSION}").classes("text-sm text-gray-600")
                ui.label(user["username"]).classes("text-sm font-medium")
                def logout() -> None:
                    set_current_user(None)
                    ui.notify("Sesión cerrada", color="positive")
                    show_login_screen(container)
                ui.button("Cerrar sesión", on_click=logout, color="negative").props("flat dense")

        tab_panels = ui.tab_panels(tabs, value=tab_map.get(tab_inicial, tab_home)).classes("w-full")

        with tab_panels:
            with ui.tab_panel(tab_home):
                home_welcome_container = ui.column().classes("w-full")
            build_tab_home_welcome(home_welcome_container)
            with ui.tab_panel(tab_estadisticas):
                estadisticas_container = ui.column().classes("w-full")

            with ui.tab_panel(tab_ventas):
                ventas_container = ui.column().classes("w-full")

            with ui.tab_panel(tab_precios):
                precios_container = ui.column().classes("w-full")

            with ui.tab_panel(tab_precios_detalle):
                precios_detalle_container = ui.column().classes("w-full")

            with ui.tab_panel(tab_compras):
                compras_container = ui.column().classes("w-full")

            with ui.tab_panel(tab_stock):
                stock_container = ui.column().classes("w-full")

            with ui.tab_panel(tab_compras_lista):
                compras_lista_container = ui.column().classes("w-full")

            with ui.tab_panel(tab_pedidos):
                pedidos_container = ui.column().classes("w-full")

            with ui.tab_panel(tab_historicos):
                historicos_container = ui.column().classes("w-full")

            with ui.tab_panel(tab_busqueda):
                build_tab_busqueda()

            with ui.tab_panel(tab_importacion):
                build_tab_importacion()

            with ui.tab_panel(tab_datos):
                build_tab_datos()

            with ui.tab_panel(tab_pesos):
                build_tab_pesos()

            with ui.tab_panel(tab_balance):
                balance_container = ui.column().classes("w-full")

            with ui.tab_panel(tab_config):
                build_tab_config()

            with ui.tab_panel(tab_admin):
                admin_container = ui.column().classes("w-full")

        def on_tab_change(e) -> None:
            val = getattr(e, "value", None)
            if val:
                app.storage.user["last_tab"] = val
            if val == "Invoices" and not compras_cargado[0]:
                compras_cargado[0] = True
                build_tab_compras(compras_container)
            elif val == "Stock" and not stock_cargado[0]:
                stock_cargado[0] = True
                build_tab_stock(stock_container)
            elif val == "Compras" and not compras_lista_cargado[0]:
                compras_lista_cargado[0] = True
                build_tab_compras_lista(compras_lista_container)
            elif val == "Pedidos" and not pedidos_cargado[0]:
                pedidos_cargado[0] = True
                build_tab_pedidos(pedidos_container)
            elif val == "Productos" and not precios_cargado[0]:
                precios_cargado[0] = True
                build_tab_precios(precios_container)
            elif val == "Precios" and not precios_detalle_cargado[0]:
                precios_detalle_cargado[0] = True
                build_tab_precios_detalle(precios_detalle_container)
            elif val == "Ventas" and not ventas_cargado[0]:
                ventas_cargado[0] = True
                build_tab_ventas(ventas_container)
            elif val == "Estadísticas" and not estadisticas_cargado[0]:
                estadisticas_cargado[0] = True
                build_tab_estadisticas(estadisticas_container)
            elif val == "Balance" and not balance_cargado[0]:
                balance_cargado[0] = True
                build_tab_balance(balance_container)
            elif val == tab_historicos and not historicos_cargado[0]:
                historicos_cargado[0] = True
                build_tab_historicos(historicos_container)
            elif val == "Admin" and not admin_cargado[0]:
                admin_cargado[0] = True
                build_tab_admin(admin_container)

        tab_panels.on_value_change(on_tab_change)


# ==========================
# CONTENIDO DE PESTAÑAS
# ==========================


# Mapeo tab_key -> (label visible, descripción para Home). Usado para mostrar solo lo que el usuario puede hacer.
TAB_DESCRIPTIONS: Dict[str, str] = {
    "estadisticas": "ver reputación en MercadoLibre, ventas hoy/ayer/semana/mes.",
    "ventas": "gestión de ventas y órdenes.",
    "productos": "catálogo de productos.",
    "precios": "gestión de precios.",
    "busqueda": "buscar productos en el catálogo.",
    "balance": "gastos, ingresos y resultados.",
    "compras": "facturas de QuickBooks con saldo, estado y seguimiento (Invoices).",
    "stock": "inventario de QuickBooks (Items con cantidad disponible).",
    "compras_lista": "cargar y gestionar compras a cotizar (marca, producto, SKU, cantidad, precio).",
    "pedidos": "ver consolidado de compras de todos los clientes.",
    "importacion": "cargar datos desde archivos.",
    "pesos": "cotización del dólar.",
    "datos": "configuración de marcas, despachantes y otros datos.",
    "configuracion": "vincular MercadoLibre, QuickBooks y configurar email.",
    "admin": "gestión de usuarios y permisos (solo administradores).",
}

LABEL_BY_TAB: Dict[str, str] = {
    "estadisticas": "Estadísticas",
    "ventas": "Ventas",
    "productos": "Productos",
    "precios": "Precios",
    "busqueda": "Búsqueda",
    "balance": "Balance",
    "compras": "Invoices",
    "stock": "Stock",
    "compras_lista": "Compras",
    "pedidos": "Pedidos",
    "importacion": "Importación",
    "pesos": "Pesos",
    "datos": "Datos",
    "configuracion": "Configuración",
    "admin": "Admin",
}


def build_tab_home_welcome(container) -> None:
    """Pestaña Home: bienvenida. Muestra qué puede hacer según permisos del usuario."""
    user = require_login()
    if not user:
        return
    perms = get_user_tab_permissions(user["id"])
    lineas: List[str] = []
    for tab_key, _ in TAB_KEYS:
        if tab_key == "home":
            continue
        if perms.get(tab_key, False):
            label = LABEL_BY_TAB.get(tab_key, tab_key)
            desc = TAB_DESCRIPTIONS.get(tab_key, "")
            if desc:
                lineas.append(f"• {label}: {desc}")
    texto = "\n".join(lineas) if lineas else "No tenés permisos asignados. Contactá al administrador."
    with container:
        ui.label("Bienvenido").classes("text-3xl font-bold text-primary mb-4")
        ui.label(f"Hola, {user.get('username', 'Usuario')}").classes("text-xl text-gray-700 mb-2")
        with ui.column().classes("text-gray-600 mb-4 gap-2 max-w-2xl"):
            ui.label("¿Qué podés hacer en el sistema?").classes("text-base font-semibold text-gray-700")
            ui.label(texto).classes("text-sm whitespace-pre-line")


def build_tab_estadisticas(estadisticas_container) -> None:
    """Pestaña Estadísticas: datos de la cuenta ML, reputación y ventas. Carga síncrona con botón Actualizar."""
    user = require_login()
    if not user:
        return

    access_token = get_ml_access_token(user["id"])
    if not access_token:
        with estadisticas_container:
            with ui.column().classes("w-full max-w-2xl gap-4"):
                ui.label("Bienvenido a BDC systems").classes("text-2xl font-semibold")
                ui.label(
                    "Conectá tu cuenta de MercadoLibre en Configuración para ver aquí tu perfil, reputación y ventas."
                ).classes("text-gray-600")
        return

    def cargar_y_pintar() -> None:
        estadisticas_container.clear()
        with estadisticas_container:
            with ui.card().classes("w-full p-8 items-center gap-4"):
                ui.spinner(size="xl")
                ui.label("Cargando datos...").classes("text-xl text-gray-700")
        background_tasks.create(_cargar_estadisticas_async(), name="cargar_estadisticas")

    async def _cargar_estadisticas_async() -> None:
        try:
            profile = await run.io_bound(ml_get_user_profile, access_token)
            seller_id = (profile or {}).get("id") or await run.io_bound(ml_get_user_id, access_token)
            orders_data: Dict[str, Any] = {}
            items_data: Dict[str, Any] = {"results": []}
            if seller_id:
                limit_str = get_cotizador_param("estadisticas_limit_ordenes", user["id"]) or "1000"
                try:
                    limit_ordenes = int(limit_str)
                    if limit_ordenes not in (300, 500, 1000, 2000, 3000, 4000, 5000):
                        limit_ordenes = 1000
                except (ValueError, TypeError):
                    limit_ordenes = 1000
                orders_data = await run.io_bound(ml_get_orders, access_token, str(seller_id), limit_ordenes, 0)
            try:
                items_data = await run.io_bound(ml_get_my_items, access_token, False)
            except Exception:
                pass
        except Exception as e:
            estadisticas_container.clear()
            with estadisticas_container:
                ui.label(f"❌ Error al cargar datos: {e}").classes("text-negative")
            return
        estadisticas_container.clear()
        with estadisticas_container:
            _pintar_home_inline(estadisticas_container, profile, orders_data, user_id=user["id"], items_data=items_data, on_refresh=cargar_y_pintar)

    cargar_y_pintar()


def _pintar_home_inline(
    container, profile: Optional[Dict], orders_data: Dict[str, Any], user_id: Optional[int] = None, items_data: Optional[Dict[str, Any]] = None, on_refresh: Optional[Callable[[], None]] = None
) -> None:
    """Pinta el contenido del Home con los datos ya cargados. on_refresh permite actualizar datos al vuelo."""
    raw_orders = orders_data.get("results") or orders_data.get("orders") or orders_data.get("elements") or []
    results = [o for o in raw_orders if isinstance(o, dict)]
    rep = (profile or {}).get("seller_reputation") or {}
    today_local = datetime.now().date()
    primer_dia_mes = today_local.replace(day=1)
    hoy_unidades, hoy_monto = 0, 0.0
    ayer_unidades, ayer_monto = 0, 0.0
    semana_unidades, semana_monto = 0, 0.0
    d15_unidades, d15_monto = 0, 0.0
    d21_unidades, d21_monto = 0, 0.0
    mes_unidades, mes_monto = 0, 0.0
    ventas_mes_actual_unid, ventas_mes_actual_monto = 0, 0.0
    por_mes: Dict[str, Any] = {}
    top_productos: Dict[str, Dict[str, Any]] = {}  # item_id -> {title, units}

    for ord_item in results:
        dt_str = ord_item.get("date_created") or ord_item.get("date_closed") or ord_item.get("date_last_updated") or ""
        if not dt_str or not isinstance(dt_str, str):
            continue
        try:
            dt = datetime.strptime(dt_str[:10], "%Y-%m-%d").date()
        except Exception:
            continue
        total_amount = ord_item.get("total_amount") or ord_item.get("paid_amount")
        if total_amount is None and ord_item.get("payments"):
            pay = ord_item["payments"][0] if isinstance(ord_item["payments"], list) else {}
            total_amount = pay.get("total_amount") or pay.get("total_paid_amount") or pay.get("transaction_amount")
        try:
            total_amount = float(total_amount or 0)
        except (TypeError, ValueError):
            total_amount = 0.0
        items = ord_item.get("order_items") or ord_item.get("items") or []
        units = sum(int(it.get("quantity") or it.get("qty") or 0) for it in items if isinstance(it, dict))
        if units == 0 and total_amount > 0:
            units = 1
        if dt == today_local:
            hoy_unidades += units
            hoy_monto += total_amount
        ayer_local = today_local - timedelta(days=1)
        if dt == ayer_local:
            ayer_unidades += units
            ayer_monto += total_amount
        if (today_local - dt).days <= 6:
            semana_unidades += units
            semana_monto += total_amount
        if (today_local - dt).days <= 14:
            d15_unidades += units
            d15_monto += total_amount
        if (today_local - dt).days <= 20:
            d21_unidades += units
            d21_monto += total_amount
        if (today_local - dt).days <= 30:
            mes_unidades += units
            mes_monto += total_amount
        if primer_dia_mes <= dt <= today_local:
            ventas_mes_actual_unid += units
            ventas_mes_actual_monto += total_amount
            items = ord_item.get("order_items") or ord_item.get("items") or []
            for it in items:
                if not isinstance(it, dict):
                    continue
                obj = it.get("item") or it
                qty = int(it.get("quantity") or it.get("qty") or 0)
                if qty <= 0:
                    continue
                titulo = (obj.get("title") if isinstance(obj, dict) else None) or it.get("title") or "Sin nombre"
                iid = (str(obj.get("id") or it.get("item_id") or "") if isinstance(obj, dict) else str(it.get("item_id") or "")).strip()
                key_id = iid or titulo[:80]
                if key_id not in top_productos:
                    top_productos[key_id] = {"title": titulo, "units": 0}
                top_productos[key_id]["units"] += qty
        key = dt.strftime("%Y-%m")
        if key not in por_mes:
            por_mes[key] = {"units": 0, "total": 0.0}
        por_mes[key]["units"] += units
        por_mes[key]["total"] += total_amount

    # Incluir siempre el mes actual aunque no tenga ventas (para que el gráfico muestre marzo, etc.)
    mes_actual_key = today_local.strftime("%Y-%m")
    if mes_actual_key not in por_mes:
        por_mes[mes_actual_key] = {"units": 0, "total": 0.0}
    meses_orden = sorted(por_mes.keys(), reverse=True)[:6]  # Solo 6 meses para caber en pantalla

    container.clear()
    with container:
            with ui.column().classes("w-full gap-2"):
                # Barra amarilla MercadoLibre (#FFE600)
                with ui.element("div").classes("w-full min-h-[88px] py-5 px-6 rounded-xl shadow-xl").style("background: linear-gradient(135deg, #FFE600 0%, #ffed4d 50%, #FFE600 100%);"):
                    with ui.row().classes("w-full items-center gap-5"):
                        # Logo/foto del usuario ML (thumbnail, picture o logo)
                        prof = profile or {}
                        raw_pic = prof.get("thumbnail") or prof.get("picture") or prof.get("logo") or prof.get("avatar")
                        pic_url = None
                        if isinstance(raw_pic, str) and raw_pic.strip():
                            pic_url = raw_pic.strip()
                        elif isinstance(raw_pic, dict):
                            pic_url = (raw_pic.get("url") or raw_pic.get("secure_url") or raw_pic.get("data", {}).get("url") or "").strip() or None
                        if pic_url:
                            ui.image(pic_url).classes("w-20 h-20 rounded-full object-cover ring-4 ring-gray-800/30 shadow-lg")
                        else:
                            ui.icon("store", size="4rem").classes("text-gray-800 opacity-90")
                        with ui.column().classes("gap-1"):
                            ui.label(prof.get("nickname") or prof.get("first_name") or "Usuario ML").classes(
                                "text-2xl font-bold text-gray-900"
                            )
                            power = rep.get("power_seller_status")
                            if power:
                                with ui.badge(color="amber").classes("text-amber-900 font-medium"):
                                    ui.label(f"MercadoLíder {power.capitalize()}")
                        ui.element("div").classes("flex-1")
                        if on_refresh:
                            ui.button("Actualizar", on_click=lambda: on_refresh()).props("flat dense round icon=refresh").classes("text-gray-800 hover:bg-gray-800/10")
                # Grid: Reputación | Ventas | Gráfico | Históricas
                with ui.row().classes("w-full gap-2 flex-nowrap items-stretch overflow-hidden max-w-full"):
                    # Reputación
                    def _pct(val: Any) -> str:
                        if val is None:
                            return "—"
                        try:
                            v = float(val)
                            if 0 <= v <= 1:
                                return f"{v * 100:.2f}%"
                            if 0 < v <= 100:
                                return f"{v:.2f}%"
                            return str(val)
                        except (TypeError, ValueError):
                            return str(val) if val is not None else "—"

                    metrics = rep.get("metrics", {}) or rep.get("transactions", {}) or {}
                    sales_meta = metrics.get("sales", {}) or {}
                    completed = sales_meta.get("completed") or 0
                    claims = metrics.get("claims", {}) or metrics.get("disputes", {}) or {}
                    canc = metrics.get("cancellations", {}) or {}
                    delayed = metrics.get("delayed_handling_time", {}) or {}
                    mediat = metrics.get("mediations", {}) or metrics.get("disputes", {}) or {}

                    def _get_rate(m: Dict[str, Any], total_completed: float = 0) -> Any:
                        exc = m.get("excluded") or {}
                        if isinstance(exc.get("real_rate"), (int, float)):
                            return exc["real_rate"]
                        if isinstance(exc.get("real_value"), (int, float)) and total_completed > 0:
                            return exc["real_value"] / total_completed
                        if isinstance(m.get("rate"), (int, float)):
                            return m["rate"]
                        if isinstance(m.get("value"), (int, float)) and total_completed > 0:
                            return m["value"] / total_completed
                        return None

                    try:
                        tot = float(completed) if completed else 0
                    except (TypeError, ValueError):
                        tot = 0
                    rate_claims = _get_rate(claims, tot)
                    rate_canc = _get_rate(canc, tot)
                    rate_delayed = _get_rate(delayed, tot)
                    rate_mediat = _get_rate(mediat, tot) if mediat else None
                    level_id = rep.get("level_id") or "—"
                    level_label = {"1_red": "Rojo", "2_orange": "Naranja", "3_yellow": "Amarillo", "4_light_green": "Verde claro", "5_green": "Verde"}.get(str(level_id), str(level_id))
                    level_colors = {"1_red": "#ef4444", "2_orange": "#f97316", "3_yellow": "#eab308", "4_light_green": "#84cc16", "5_green": "#22c55e"}
                    level_color = level_colors.get(str(level_id), "#6b7280")
                    MAX_CLAIMS, MAX_MEDIAT, MAX_CANC, MAX_DELAYED = 0.01, 0.005, 0.005, 0.08

                    def _pct_to_float(v: Any) -> Optional[float]:
                        if v is None:
                            return None
                        try:
                            x = float(v)
                            return x if 0 < x <= 1 else x / 100.0
                        except (TypeError, ValueError):
                            return None

                    def _row_color(actual: Optional[float], max_val: float) -> str:
                        if actual is None or actual == 0:
                            return "text-emerald-600"
                        return "text-red-600 font-semibold" if actual > max_val else "text-emerald-600"

                    meses_nombres = {1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 5: "Mayo", 6: "Junio",
                                    7: "Julio", 8: "Agosto", 9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"}
                    mes_actual_nom = meses_nombres.get(today_local.month, today_local.strftime("%B"))

                    with ui.card().classes("flex-1 min-w-[200px] shrink-0 p-4 border-l-4 border-l-emerald-500"):
                        ui.label("Reputación").classes("text-lg font-semibold text-emerald-700 dark:text-emerald-400 mb-1")
                        with ui.row().classes("gap-2 items-center mb-2"):
                            ui.icon("lightbulb", size="sm").style(f"color: {level_color}")
                            ui.label(f"Nivel: {level_label}").classes("text-base").style(f"color: {level_color}; font-weight: 600")
                        with ui.column().classes("gap-1.5 text-base"):
                            r_c = _pct_to_float(rate_claims)
                            with ui.row().classes("gap-1 items-baseline"):
                                ui.label("• Reclamos:").classes("text-black")
                                ui.label(f"{_pct(rate_claims)} (máx 1%)").classes(_row_color(r_c, MAX_CLAIMS))
                            r_m = _pct_to_float(rate_mediat)
                            with ui.row().classes("gap-1 items-baseline"):
                                ui.label("• Mediaciones:").classes("text-black")
                                ui.label(f"{_pct(rate_mediat) if rate_mediat is not None else '—'} (máx 0,5%)").classes(_row_color(r_m, MAX_MEDIAT))
                            r_k = _pct_to_float(rate_canc)
                            with ui.row().classes("gap-1 items-baseline"):
                                ui.label("• Cancelaciones:").classes("text-black")
                                ui.label(f"{_pct(rate_canc)} (máx 0,5%)").classes(_row_color(r_k, MAX_CANC))
                            r_d = _pct_to_float(rate_delayed)
                            with ui.row().classes("gap-1 items-baseline"):
                                ui.label("• Demora envíos:").classes("text-black")
                                ui.label(f"{_pct(rate_delayed)} (máx 8%)").classes(_row_color(r_d, MAX_DELAYED))

                    # Ventas (2 filas: Hoy/Ayer/7d y 15d/21d/30d)
                    with ui.card().classes("flex-1 min-w-[200px] shrink-0 p-2 border-l-4 border-l-blue-500 overflow-hidden"):
                        ui.label("Ventas").classes("text-sm font-semibold text-blue-700 dark:text-blue-400 mb-1")
                        with ui.column().classes("gap-1 w-full"):
                            with ui.row().classes("gap-1 w-full flex-nowrap"):
                                with ui.column().classes("p-2 flex-1 min-w-0 rounded bg-blue-50 dark:bg-blue-900/40"):
                                    ui.label("Hoy").classes("text-xs text-blue-600")
                                    ui.label(str(hoy_unidades)).classes("text-base font-bold text-blue-800")
                                    ui.label(f"$ {hoy_monto:,.0f}".replace(",", ".")).classes("text-xs font-medium whitespace-nowrap")
                                with ui.column().classes("p-2 flex-1 min-w-0 rounded bg-slate-50 dark:bg-slate-900/40"):
                                    ui.label("Ayer").classes("text-xs text-slate-600")
                                    ui.label(str(ayer_unidades)).classes("text-base font-bold text-slate-800")
                                    ui.label(f"$ {ayer_monto:,.0f}".replace(",", ".")).classes("text-xs font-medium whitespace-nowrap")
                                with ui.column().classes("p-2 flex-1 min-w-0 rounded bg-emerald-50 dark:bg-emerald-900/40"):
                                    ui.label("7 días").classes("text-xs text-emerald-600")
                                    ui.label(str(semana_unidades)).classes("text-base font-bold text-emerald-800")
                                    ui.label(f"$ {semana_monto:,.0f}".replace(",", ".")).classes("text-xs font-medium whitespace-nowrap")
                            with ui.row().classes("gap-1 w-full flex-nowrap"):
                                with ui.column().classes("p-2 flex-1 min-w-0 rounded bg-teal-50 dark:bg-teal-900/40"):
                                    ui.label("15 días").classes("text-xs text-teal-600")
                                    ui.label(str(d15_unidades)).classes("text-base font-bold text-teal-800")
                                    ui.label(f"$ {d15_monto:,.0f}".replace(",", ".")).classes("text-xs font-medium whitespace-nowrap")
                                with ui.column().classes("p-2 flex-1 min-w-0 rounded bg-cyan-50 dark:bg-cyan-900/40"):
                                    ui.label("21 días").classes("text-xs text-cyan-600")
                                    ui.label(str(d21_unidades)).classes("text-base font-bold text-cyan-800")
                                    ui.label(f"$ {d21_monto:,.0f}".replace(",", ".")).classes("text-xs font-medium whitespace-nowrap")
                                with ui.column().classes("p-2 flex-1 min-w-0 rounded bg-amber-50 dark:bg-amber-900/40"):
                                    ui.label("30 días").classes("text-xs text-amber-600")
                                    ui.label(str(mes_unidades)).classes("text-base font-bold text-amber-800")
                                    ui.label(f"$ {mes_monto:,.0f}".replace(",", ".")).classes("text-xs font-medium whitespace-nowrap")

                    # Gráfico ventas por mes (valores en millones para eje Y legible)
                    if meses_orden:
                        orden_rev = list(reversed(meses_orden))
                        meses_abr = {"01": "ene", "02": "feb", "03": "mar", "04": "abr", "05": "may", "06": "jun",
                                     "07": "jul", "08": "ago", "09": "sep", "10": "oct", "11": "nov", "12": "dic"}
                        chart_labels = [f"{meses_abr.get(k[5:7], k[5:7])}-{k[2:4]}" for k in orden_rev]
                        chart_data = []
                        for i, k in enumerate(orden_rev):
                            val = round(por_mes[k]["total"] / 1e6, 2)
                            is_actual = i == len(orden_rev) - 1
                            lbl_m = f"${val:.2f}M"
                            chart_data.append({
                                "value": val,
                                "itemStyle": {"color": "#10b981" if is_actual else "#6366f1"},
                                "label": {"show": True, "position": "top", "formatter": lbl_m}
                            })
                        chart_options = {
                            "grid": {"left": 60, "right": 25, "top": 25, "bottom": 35},
                            "xAxis": {"type": "category", "data": chart_labels, "axisLabel": {"fontSize": 12, "interval": 0}},
                            "yAxis": {"type": "value", "name": "Millones de pesos", "axisLabel": {"fontSize": 12}},
                            "series": [{"type": "bar", "data": chart_data, "barWidth": "60%"}],
                        }
                        with ui.card().classes("flex-1 min-w-[280px] shrink-0 p-4 border-l-4 border-l-indigo-500").style("min-height: 185px"):
                            ui.label("Facturación Mensual").classes("text-base font-semibold text-indigo-600 mb-1 px-1")
                            ui.echart(chart_options).classes("w-full").style("height: 155px")
                    else:
                        with ui.card().classes("flex-1 min-w-[120px] shrink-0 p-4 border-l-4 border-l-indigo-500"):
                            ui.label("Facturación Mensual").classes("text-sm font-semibold")
                            ui.label("Sin datos").classes("text-xs text-gray-500")

                    # Ventas Históricas (tabla más grande)
                    with ui.card().classes("flex-1 min-w-[260px] shrink-0 p-4 border-l-4 border-l-indigo-500"):
                        ui.label("Ventas Históricas").classes("text-base font-semibold text-indigo-600 mb-2")
                        if not meses_orden:
                            trans = rep.get("transactions", {}) or {}
                            tot = trans.get("total") or trans.get("completed") or 0
                            ui.label(f"Sin datos (perfil: {tot} trans.)" if tot else "No hay órdenes").classes("text-gray-500 text-sm")
                        else:
                            dolar_str = get_cotizador_param("dolar_oficial", user_id) or COTIZADOR_DEFAULTS.get("dolar_oficial", "1475")
                            dolar_oficial = float(str(dolar_str).replace(",", ".").strip()) if dolar_str else 1475.0
                            if dolar_oficial <= 0:
                                dolar_oficial = 1475.0
                            with ui.element("div").classes("w-full border rounded overflow-hidden"):
                                with ui.row().classes("w-full font-semibold bg-indigo-600 text-white py-1.5 px-2 gap-2 items-center text-sm"):
                                    ui.label("Mes").classes("min-w-[70px]")
                                    ui.label("Unid").classes("w-14 text-right")
                                    ui.label("Facturación $").classes("w-24 text-right")
                                    ui.label("Facturación u$").classes("w-24 text-right")
                                for key in meses_orden:
                                    v = por_mes[key]
                                    total_usd = (v["total"] / dolar_oficial) if dolar_oficial else 0.0
                                    with ui.row().classes("w-full py-1 px-2 gap-2 items-center border-t border-gray-200 text-sm"):
                                        ui.label(key).classes("min-w-[70px]")
                                        ui.label(str(v["units"])).classes("w-14 text-right")
                                        ui.label(f"$ {v['total']:,.0f}".replace(",", ".")).classes("w-24 text-right")
                                        ui.label(f"u$ {total_usd:,.0f}".replace(",", ".")).classes("w-24 text-right")

                claims_val = (claims.get("value") or claims.get("excluded", {}).get("real_value") or 0)
                mediat_val = (mediat.get("value") or mediat.get("excluded", {}).get("real_value") or 0) if mediat else 0
                canc_val = (canc.get("value") or canc.get("excluded", {}).get("real_value") or 0)
                postventa_total = claims_val + mediat_val + canc_val

                # Unidades vendidas semanales (últimos 14 días: esta semana + semana pasada)
                ventas_por_dia: Dict[str, int] = {}
                dias_semana_es = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]
                for d in range(14):
                    fd = today_local - timedelta(days=d)
                    ventas_por_dia[fd.strftime("%Y-%m-%d")] = 0
                for ord_item in results:
                    dt_str = ord_item.get("date_created") or ord_item.get("date_closed") or ""
                    if not dt_str:
                        continue
                    try:
                        dt = datetime.strptime(dt_str[:10], "%Y-%m-%d").date()
                    except Exception:
                        continue
                    if (today_local - dt).days > 13:
                        continue
                    items = ord_item.get("order_items") or ord_item.get("items") or []
                    units = sum(int(it.get("quantity") or it.get("qty") or 0) for it in items if isinstance(it, dict))
                    if units == 0:
                        total_amount = ord_item.get("total_amount") or ord_item.get("paid_amount") or 0
                        if total_amount and float(total_amount or 0) > 0:
                            units = 1
                    key = dt.strftime("%Y-%m-%d")
                    if key in ventas_por_dia:
                        ventas_por_dia[key] += units

                with ui.row().classes("w-full gap-2 flex-nowrap items-stretch mt-1.5 overflow-x-auto"):
                    # Top ventas: 6 productos más vendidos del mes actual (sin scroll)
                    top_list = sorted(top_productos.values(), key=lambda x: x["units"], reverse=True)[:6]
                    total_unid_mes = ventas_mes_actual_unid if ventas_mes_actual_unid > 0 else 1

                    with ui.card().classes("flex-1 min-w-[200px] shrink-0 p-3 border-l-4 border-l-emerald-600"):
                        ui.label(f"Top ventas - {mes_actual_nom}").classes("text-base font-semibold text-emerald-800 mb-1")
                        if not top_list:
                            ui.label("Sin ventas este mes").classes("text-sm text-gray-500")
                        else:
                            with ui.row().classes("w-full py-0.5 font-semibold text-gray-600 border-b border-gray-200 text-sm"):
                                ui.label("#").classes("w-5 shrink-0")
                                ui.label("Producto").classes("flex-1 truncate min-w-0")
                                ui.label("Qty").classes("w-8 shrink-0 text-right")
                                ui.label("%").classes("w-8 shrink-0 text-right")
                            for i, p in enumerate(top_list):
                                pct = (100.0 * p["units"] / total_unid_mes) if total_unid_mes else 0
                                tit = (p["title"] or "—")[:35]
                                if len(p.get("title") or "") > 35:
                                    tit += "…"
                                with ui.row().classes("w-full py-0.5 gap-1 items-center border-b border-gray-100 text-sm"):
                                    ui.label(f"{i+1}.").classes("w-5 text-gray-500 shrink-0")
                                    ui.label(tit).classes("flex-1 truncate min-w-0")
                                    ui.label(str(p["units"])).classes("w-8 shrink-0 text-right font-medium")
                                    ui.label(f"{pct:.1f}%").classes("w-8 shrink-0 text-right text-emerald-600")

                    # Stock: misma lógica que Productos barra gris (tipo = Catalogo si catalog_listing True, sino Propia)
                    items_list = (items_data or {}).get("results") or []
                    propias = [it for it in items_list if isinstance(it, dict) and it.get("catalog_listing") is not True]
                    publicaciones_propias_con_stock = sum(1 for it in propias if (it.get("available_quantity") or 0) > 0)
                    unidades_propias_en_stock = sum(int(it.get("available_quantity") or 0) for it in propias)
                    marcas_propias = [str(it.get("marca") or "").strip() for it in propias]
                    marcas_distintas = len({m for m in marcas_propias if m and m != "—"})
                    def _orden_fecha(o):
                        ds = o.get("date_closed") or o.get("date_created") or o.get("date_last_updated") or ""
                        return ds[:10] if ds else ""

                    ultimas_5_ventas = sorted(results, key=_orden_fecha, reverse=True)[:5]

                    with ui.card().classes("flex-1 min-w-[200px] shrink-0 p-3 border-l-4 border-l-amber-500"):
                        ui.label("Stock").classes("text-base font-semibold text-amber-700 mb-1")
                        with ui.row().classes("w-full gap-3 flex-wrap text-sm text-gray-700"):
                            ui.label(f"Publicaciones: {publicaciones_propias_con_stock}")
                            ui.label(f"Unidades: {unidades_propias_en_stock:,.0f}".replace(",", "."))
                            ui.label(f"Marcas: {marcas_distintas}")
                        if ultimas_5_ventas:
                            ui.label("Últimas 5 ventas").classes("text-sm font-semibold text-amber-600 mt-1 mb-0.5")
                            for v in ultimas_5_ventas:
                                ds_raw = v.get("date_closed") or v.get("date_created") or v.get("date_last_updated") or ""
                                try:
                                    if "T" in ds_raw:
                                        dt = datetime.strptime(ds_raw[:19], "%Y-%m-%dT%H:%M:%S")
                                    elif " " in ds_raw:
                                        dt = datetime.strptime(ds_raw[:16], "%Y-%m-%d %H:%M")
                                    elif len(ds_raw) >= 10:
                                        dt = datetime.strptime(ds_raw[:10], "%Y-%m-%d")
                                    else:
                                        dt = None
                                    fecha_fmt = f"{dt.day:02d}-{dt.month:02d} {dt.hour:02d}:{dt.minute:02d}" if dt else "—"
                                except Exception:
                                    fecha_fmt = ds_raw[:16] if ds_raw else "—"
                                items_v = v.get("order_items") or v.get("items") or []
                                uds = sum(int(it.get("quantity") or it.get("qty") or 0) for it in items_v if isinstance(it, dict))
                                if uds == 0:
                                    total = v.get("total_amount") or v.get("paid_amount") or 0
                                    uds = 1 if float(total or 0) > 0 else 0
                                primer_item = items_v[0] if items_v else {}
                                obj = primer_item.get("item") or primer_item
                                tit = (obj.get("title") if isinstance(obj, dict) else primer_item.get("title")) or "—"
                                tit = (tit[:32] + "…") if len(str(tit)) > 32 else str(tit)
                                with ui.row().classes("w-full items-center gap-1 overflow-hidden py-0.5"):
                                    ui.label(f"• {tit}").classes("text-sm text-gray-700 truncate flex-1 min-w-0")
                                    ui.label(f"cant: {uds} · {fecha_fmt}").classes("text-sm text-gray-600 shrink-0")

                    # Unidades Vendidas Semanales (gráfico de barras, últimos 7 días)
                    dias_orden = sorted(ventas_por_dia.keys())[-7:]
                    uds_esta_semana = sum(ventas_por_dia.get((today_local - timedelta(days=d)).strftime("%Y-%m-%d"), 0) for d in range(7))
                    uds_semana_pasada = sum(ventas_por_dia.get((today_local - timedelta(days=d)).strftime("%Y-%m-%d"), 0) for d in range(7, 14))
                    var_pct = ((uds_esta_semana - uds_semana_pasada) / uds_semana_pasada * 100) if uds_semana_pasada > 0 else (100.0 if uds_esta_semana > 0 else 0.0)
                    if dias_orden:
                        chart_labels = []
                        chart_data = []
                        for i, key in enumerate(dias_orden):
                            fd = datetime.strptime(key, "%Y-%m-%d").date()
                            dia_sem = dias_semana_es[fd.weekday()]
                            chart_labels.append(f"{dia_sem} {fd.day}")
                            uds = ventas_por_dia.get(key, 0)
                            is_hoy = fd == today_local
                            chart_data.append({"value": uds, "itemStyle": {"color": "#14b8a6" if is_hoy else "#0d9488"}})
                        chart_options_sem = {
                            "grid": {"left": 50, "right": 25, "top": 25, "bottom": 35},
                            "xAxis": {"type": "category", "data": chart_labels, "axisLabel": {"fontSize": 11, "interval": 0}},
                            "yAxis": {"type": "value", "axisLabel": {"fontSize": 12}},
                            "series": [{"type": "bar", "data": chart_data, "barWidth": "60%", "label": {"show": True, "position": "top", "fontSize": 11}}],
                        }
                        with ui.card().classes("flex-1 min-w-[280px] shrink-0 p-4 border-l-4 border-l-teal-500").style("min-height: 185px"):
                            ui.label("Unidades Vendidas Semanales").classes("text-base font-semibold text-teal-700 mb-1 px-1")
                            ui.echart(chart_options_sem).classes("w-full").style("height: 155px")
                            with ui.column().classes("mt-2 gap-0.5 text-sm"):
                                ui.label(f"Unidades vendidas esta semana: {uds_esta_semana}").classes("text-gray-700")
                                ui.label(f"Unidades vendidas la semana pasada: {uds_semana_pasada}").classes("text-gray-700")
                                variacion_cls = "text-emerald-600 font-semibold" if var_pct >= 0 else "text-red-600 font-semibold"
                                ui.label(f"Variación semanal: {var_pct:+.1f}%").classes(variacion_cls)
                    else:
                        with ui.card().classes("flex-1 min-w-[120px] shrink-0 p-4 border-l-4 border-l-teal-500"):
                            ui.label("Unidades Vendidas Semanales").classes("text-sm font-semibold")
                            ui.label("Sin datos").classes("text-xs text-gray-500")

                    # Ventas del mes, estimaciones y ganancias
                    dias_transcurridos = (today_local - primer_dia_mes).days + 1
                    dias_del_mes = calendar.monthrange(today_local.year, today_local.month)[1]
                    venta_diaria = ventas_mes_actual_monto / dias_transcurridos if dias_transcurridos > 0 else 0
                    venta_estimada_mes = venta_diaria * dias_del_mes if dias_transcurridos > 0 else 0

                    with ui.card().classes("flex-1 min-w-[260px] shrink-0 p-4 border-l-4 border-l-violet-500"):
                        ui.label(f"Ventas - {mes_actual_nom}").classes("text-base font-semibold text-violet-700 mb-2")
                        with ui.column().classes("gap-1.5 text-sm"):
                            ui.label(f"Ventas a la fecha: $ {ventas_mes_actual_monto:,.0f}".replace(",", ".")).classes("text-gray-700")
                            ui.label(f"Cantidad de días: {dias_transcurridos}").classes("text-gray-700")
                            ui.label(f"Unidades vendidas: {ventas_mes_actual_unid}").classes("text-gray-700")
                            ui.element("div").classes("border-t border-gray-200 my-1")
                            ui.label(f"Ventas diarias: $ {venta_diaria:,.0f}".replace(",", ".")).classes("text-gray-700")
                            venta_diaria_u = ventas_mes_actual_unid / dias_transcurridos if dias_transcurridos > 0 else 0
                            ui.label(f"Venta diaria u: {venta_diaria_u:,.1f}".replace(",", ".")).classes("text-gray-700")
                            ticket_prom = (ventas_mes_actual_monto / ventas_mes_actual_unid) if ventas_mes_actual_unid > 0 else 0
                            ui.label(f"Ticket Promedio: $ {ticket_prom:,.0f}".replace(",", ".")).classes("text-gray-700")
                            ui.element("div").classes("border-t border-gray-200 my-1")
                            ui.label(f"Venta estimada mensual: $ {venta_estimada_mes:,.0f}".replace(",", ".")).classes("text-gray-700")
                            dolar_str = (get_cotizador_param("dolar_oficial", user_id) or COTIZADOR_DEFAULTS.get("dolar_oficial", "1475")) if user_id else "1475"
                            dolar_oficial = float(str(dolar_str).replace(",", ".").strip()) if dolar_str else 1475.0
                            venta_estimada_mes_usd = (venta_estimada_mes / dolar_oficial) if dolar_oficial > 0 else 0
                            ui.label(f"Venta estimada mensual: u$ {venta_estimada_mes_usd:,.0f}".replace(",", ".")).classes("text-gray-700")


def build_tab_ventas(container) -> None:
    """Pestaña Ventas: tabla de ventas desde el 1 del mes actual hasta hoy."""
    container.clear()
    user = require_login()
    if not user:
        return

    access_token = get_ml_access_token(user["id"])
    if not access_token:
        with container:
            ui.label("⚠️ No tienes MercadoLibre vinculado. Ve a Configuración y conecta tu cuenta.").classes("text-warning mb-4")
        return

    ventas_raw: List[Dict[str, Any]] = []
    all_orders_ref: Dict[str, List[Dict]] = {"orders": [], "item_id_to_catalog": {}, "item_id_to_sku": {}, "item_id_to_tipo_venta": {}, "item_id_to_cuotas": {}, "item_id_to_tipo_oferta": {}, "item_id_to_promo_display": {}}
    filtro_fecha_ref: Dict[str, str] = {"val": "mes_actual"}
    filtro_publicacion_ref: Dict[str, str] = {"val": "todas"}
    filtro_cuotas_ref: Dict[str, str] = {"val": "todas"}
    filtro_tipo_ref: Dict[str, str] = {"val": "todas"}
    filtro_estado_ref: Dict[str, str] = {"val": "pagada"}
    agrupar_ref: Dict[str, bool] = {"val": False}  # Por defecto desagrupado
    margenes_ref: Dict[str, str] = {}  # productos -> margen editable
    ganancia_neta_ref: Dict[str, float] = {"val": 0.0}

    sort_col_ventas: Dict[str, str] = {"val": "dt"}
    sort_asc_ventas: Dict[str, bool] = {"val": False}  # Fecha más reciente primero

    with container:
        header_card = ui.column().classes("w-full mb-2")
        filtro_row = ui.row().classes("w-full mb-2 items-center gap-4")
        result_area = ui.column().classes("w-full gap-2")

        def _cuotas_desde_item(body: Dict[str, Any]) -> str:
            """Devuelve x1, x3 o x6 según listing_type_id y INSTALLMENTS_CAMPAIGN (en sale_terms o attributes)."""
            listing_type_id = str(body.get("listing_type_id") or "").strip().lower()
            if listing_type_id == "gold_special":
                return "x1"
            if listing_type_id == "gold_pro":
                def _tiene_3x_campaign(items: list) -> bool:
                    for a in items or []:
                        if isinstance(a, dict) and (str(a.get("id") or "").upper() == "INSTALLMENTS_CAMPAIGN"):
                            vn = str(a.get("value_name") or "").lower()
                            if "3x_campaign" in vn or vn == "3x_campaign":
                                return True
                    return False
                if _tiene_3x_campaign(body.get("sale_terms")) or _tiene_3x_campaign(body.get("attributes")):
                    return "x3"
                return "x6"
            return "x1"

        def _tipo_base_desde_body(body: Dict[str, Any]) -> str:
            """Devuelve Propia o Catálogo. Solo catalog_listing=True es Catálogo; catalog_listing=false o ausente es Propia."""
            if not body or not isinstance(body, dict):
                return "Propia"
            cl = body.get("catalog_listing")
            return "Catálogo" if (cl is True or str(cl or "").lower() in ("true", "1")) else "Propia"

        def _update_btn_agrupar() -> None:
            if agrupar_ref.get("val"):
                btn_agrupar.text = "Desagrupar"
            else:
                btn_agrupar.text = "Agrupar"

        def _toggle_agrupar() -> None:
            agrupar_ref["val"] = not agrupar_ref.get("val", False)
            _update_btn_agrupar()
            _pintar_tabla()

        def _update_margen(productos_key: str, val: str) -> None:
            margenes_ref[productos_key] = val or ""

        def _calcular_ganancia() -> None:
            if not agrupar_ref.get("val"):
                ganancia_neta_ref["val"] = 0.0
                _pintar_tabla()
                return
            # Al agrupar, solo se consideran ventas con estado Concretada (paid)
            ventas_filtradas = [v for v in ventas_raw if (v.get("status_raw") or "").lower() == "paid"]
            grupos: Dict[str, Dict[str, Any]] = {}
            for v in ventas_filtradas:
                key = v.get("agrupar_key") or (v.get("productos") or v.get("title", "—"))
                if key not in grupos:
                    grupos[key] = {"productos": v.get("productos") or v.get("title", "—"), "cantidad": 0, "monto": 0.0}
                grupos[key]["cantidad"] += v["cantidad"]
                grupos[key]["monto"] += v["monto"]
            filas = list(grupos.values())
            total = 0.0
            for f in filas:
                productos_key = f["productos"]
                cantidad = int(f["cantidad"])
                try:
                    margen = float((margenes_ref.get(productos_key) or "0").replace(",", ".").strip())
                except (ValueError, TypeError):
                    margen = 0.0
                total += cantidad * margen
            ganancia_neta_ref["val"] = total
            _pintar_tabla()

        def _order_in_range(o: Dict, start: datetime.date, end: datetime.date) -> bool:
            dt_str = o.get("date_created") or o.get("date_closed") or o.get("date_last_updated") or ""
            if not dt_str or not isinstance(dt_str, str):
                return False
            try:
                dt = datetime.strptime(dt_str[:10], "%Y-%m-%d").date()
                return start <= dt <= end
            except Exception:
                return False

        def _tipo_oferta_desde_order_item(it: Dict, item_id: str, item_id_to_tipo_oferta: Dict[str, str]) -> tuple:
            """Detecta Promo desde order_item (gross_price/discounts). Retorna (tipo, tipo_display) donde tipo_display tiene % dto y precio orig para Promo."""
            fallback = item_id_to_tipo_oferta.get(item_id) or item_id_to_tipo_oferta.get(item_id.upper() or "") or item_id_to_tipo_oferta.get(item_id.lower() or "") or "Regular"
            qty = int(it.get("quantity") or it.get("qty") or 0)
            if qty == 0:
                return (fallback, None)
            unit_price = it.get("unit_price")
            gross_price = it.get("gross_price")
            if gross_price is not None and unit_price is not None:
                try:
                    gross_f = float(gross_price)
                    up_f = float(unit_price)
                    paid_total = up_f * qty
                    if gross_f > paid_total + 0.01:
                        pct = ((gross_f - paid_total) / gross_f * 100) if gross_f > 0 else 0
                        orig_fmt = f"$ {gross_f:,.0f}".replace(",", ".")
                        pct_str = f"{pct:.1f}".replace(".", ",")
                        return ("Promo", f"{orig_fmt} ({pct_str}% dto)")
                except (TypeError, ValueError):
                    pass
            discounts = it.get("discounts") or []
            if isinstance(discounts, list):
                for d in discounts:
                    if isinstance(d, dict):
                        amt = d.get("amounts") or {}
                        if isinstance(amt, dict):
                            full = amt.get("full")
                            if full is not None:
                                try:
                                    full_f = float(full)
                                    if full_f > 0.01:
                                        paid_total = float(unit_price or 0) * qty
                                        orig = paid_total + full_f
                                        pct = (full_f / orig * 100) if orig > 0 else 0
                                        orig_fmt = f"$ {orig:,.0f}".replace(",", ".")
                                        pct_str = f"{pct:.1f}".replace(".", ",")
                                        return ("Promo", f"{orig_fmt} ({pct_str}% dto)")
                                except (TypeError, ValueError):
                                    pass
            return (fallback, None)

        def _aplicar_filtro_fecha() -> None:
            fecha_val = filtro_fecha_ref.get("val", "mes_actual")
            if fecha_val not in ("mes_actual", "mes_anterior"):
                fecha_val = "mes_actual"
            # Si selecciona mes anterior y no lo tenemos, cargar
            if fecha_val == "mes_anterior" and not all_orders_ref.get("mes_anterior_cargado"):
                _cargar_ventas()
                return
            orders = all_orders_ref.get("orders") or []
            if not orders:
                return
            hoy = datetime.now().date()
            primer_dia = hoy.replace(day=1)
            ultimo_mes = primer_dia - timedelta(days=1)
            primer_dia_anterior = ultimo_mes.replace(day=1)
            if fecha_val == "mes_actual":
                orders_periodo = [o for o in orders if _order_in_range(o, primer_dia, hoy)]
            else:
                orders_periodo = [o for o in orders if _order_in_range(o, primer_dia_anterior, ultimo_mes)]
            _construir_ventas_desde_orders(orders_periodo)
            _pintar_tabla()

        def _construir_ventas_desde_orders(orders_periodo: List[Dict]) -> None:
            nonlocal ventas_raw
            item_id_to_catalog = all_orders_ref.get("item_id_to_catalog") or {}
            item_id_to_sku = all_orders_ref.get("item_id_to_sku") or {}
            item_id_to_cuotas = all_orders_ref.get("item_id_to_cuotas") or {}
            item_id_to_tipo_oferta = all_orders_ref.get("item_id_to_tipo_oferta") or {}
            item_id_to_promo_display = all_orders_ref.get("item_id_to_promo_display") or {}
            status_map = {"paid": "Concretada", "handling": "En preparación", "shipped": "Enviada", "delivered": "Entregada", "cancelled": "Cancelada", "canceled": "Cancelada"}
            ventas_mes = []
            for ord_item in orders_periodo:
                dt_str = ord_item.get("date_created") or ord_item.get("date_closed") or ord_item.get("date_last_updated") or ""
                if not dt_str or not isinstance(dt_str, str):
                    continue
                try:
                    dt = datetime.strptime(dt_str[:10], "%Y-%m-%d").date()
                except Exception:
                    continue
                ord_total = ord_item.get("total_amount") or ord_item.get("paid_amount")
                if ord_total is None and ord_item.get("payments"):
                    pay = ord_item["payments"][0] if isinstance(ord_item["payments"], list) else {}
                    ord_total = pay.get("total_amount") or pay.get("total_paid_amount") or pay.get("transaction_amount")
                try:
                    ord_total = float(ord_total or 0)
                except (TypeError, ValueError):
                    ord_total = 0.0
                status_raw = (ord_item.get("status") or "").strip().lower()
                status_display = status_map.get(status_raw, status_raw or "—")
                items = ord_item.get("order_items") or ord_item.get("items") or []
                ord_qty = sum(int(it.get("quantity") or it.get("qty") or 0) for it in items if isinstance(it, dict))
                for it in items:
                    if not isinstance(it, dict):
                        continue
                    obj = it.get("item") or it
                    qty = int(it.get("quantity") or it.get("qty") or 0)
                    if qty == 0:
                        continue
                    unit_price = it.get("unit_price")
                    if unit_price is None:
                        unit_price = ord_total / ord_qty if ord_qty > 0 else 0
                    try:
                        unit_price = float(unit_price or 0)
                    except (TypeError, ValueError):
                        unit_price = 0
                    item_monto = qty * unit_price
                    titulo = (obj.get("title") if isinstance(obj, dict) else str(obj)) or it.get("title") or "—"
                    item_id = (str(obj.get("id") or it.get("item_id") or "") if isinstance(obj, dict) else str(it.get("item_id") or "")).strip()
                    catalog_id = str(obj.get("catalog_product_id") or it.get("catalog_product_id") or "").strip() if isinstance(obj, dict) else str(it.get("catalog_product_id") or "")
                    cl = obj.get("catalog_listing") if isinstance(obj, dict) else it.get("catalog_listing")
                    if cl is None and isinstance(obj, dict):
                        cl = it.get("catalog_listing")
                    catalog = cl is True or str(cl or "").lower() in ("true", "1")
                    if (cl is None or not catalog) and item_id:
                        catalog = item_id_to_catalog.get(item_id, False) or item_id_to_catalog.get(item_id.upper(), False) or item_id_to_catalog.get(item_id.lower(), False)
                    tipo = "Catálogo" if catalog else "Propia"
                    sku = item_id_to_sku.get(item_id, "")
                    agrupar_key = catalog_id or (sku if tipo == "Propia" and sku else "") or item_id or titulo
                    cuotas = item_id_to_cuotas.get(item_id) or item_id_to_cuotas.get(item_id.upper()) or item_id_to_cuotas.get(item_id.lower()) or "x1"
                    tipo_oferta, tipo_display = _tipo_oferta_desde_order_item(it, item_id, item_id_to_tipo_oferta)
                    if tipo_display is None and (tipo_oferta or "").lower() == "promo":
                        tipo_display = item_id_to_promo_display.get(item_id) or item_id_to_promo_display.get(item_id.upper() or "") or item_id_to_promo_display.get(item_id.lower() or "") or "Promo"
                    ventas_mes.append({
                        "dt": dt, "fecha": dt.strftime("%d/%m/%Y"), "productos": titulo[:100], "title": titulo[:100],
                        "tipo_venta": tipo, "cuotas": cuotas, "tipo": tipo_oferta, "tipo_oferta": tipo_oferta,
                        "tipo_display": tipo_display or tipo_oferta,
                        "cantidad": qty, "monto": item_monto, "monto_fmt": f"$ {item_monto:,.0f}".replace(",", "."),
                        "status": status_display, "status_raw": status_raw, "agrupar_key": agrupar_key, "item_id": item_id or "—",
                    })
            ventas_raw = ventas_mes

        def _cargar_ventas() -> None:
            if filtro_controls_ref:
                filtro_controls_ref[0].set_visibility(False)
            result_area.clear()
            with result_area:
                with ui.card().classes("w-full p-8 items-center gap-4"):
                    ui.spinner(size="xl")
                    ui.label("Cargando ventas...").classes("text-xl text-gray-700")
            background_tasks.create(_cargar_ventas_async(), name="cargar_ventas")

        def _sort_key_ventas(row: Dict[str, Any], col: str) -> Any:
            if col == "dt":
                return row.get("dt") or ""
            if col == "fecha":
                return row.get("fecha") or ""
            if col == "productos":
                return str(row.get("productos") or row.get("title", "")).lower()
            if col == "cantidad":
                return int(row.get("cantidad") or 0)
            if col == "monto":
                return float(row.get("monto") or 0)
            if col == "status":
                return str(row.get("status") or "").lower()
            if col == "item_id":
                return str(row.get("item_id") or "")
            if col == "tipo":
                return str(row.get("tipo") or "").lower()
            if col == "tipo_venta":
                return str(row.get("tipo_venta") or "").lower()
            if col == "cuotas":
                return str(row.get("cuotas") or "").lower()
            return ""

        def _on_sort_ventas(col: str) -> None:
            if sort_col_ventas.get("val") == col:
                sort_asc_ventas["val"] = not sort_asc_ventas.get("val", True)
            else:
                sort_col_ventas["val"] = col
                sort_asc_ventas["val"] = True
            _pintar_tabla()

        def _pintar_tabla() -> None:
            """Pinta la tabla según ventas_raw, filtro y agrupar."""
            estado_val = str(filtro_estado_ref.get("val", "todas") or "todas")
            ventas_filtradas = ventas_raw
            if estado_val == "pagada":
                ventas_filtradas = [v for v in ventas_raw if (v.get("status_raw") or "").lower() in ("paid", "handling", "shipped", "delivered")]
            elif estado_val == "cancelada":
                ventas_filtradas = [v for v in ventas_raw if "cancel" in (v.get("status_raw") or "").lower()]
            pub_val = str(filtro_publicacion_ref.get("val", "todas") or "todas")
            if pub_val == "propias":
                ventas_filtradas = [v for v in ventas_filtradas if v.get("tipo") == "Propia"]
            elif pub_val == "catalogo":
                ventas_filtradas = [v for v in ventas_filtradas if v.get("tipo") == "Catálogo"]
            cuotas_val = str(filtro_cuotas_ref.get("val", "todas") or "todas")
            if cuotas_val in ("x1", "x3", "x6"):
                ventas_filtradas = [v for v in ventas_filtradas if (v.get("cuotas") or "x1") == cuotas_val]
            tipo_val = str(filtro_tipo_ref.get("val", "todas") or "todas")
            if tipo_val == "promo":
                ventas_filtradas = [v for v in ventas_filtradas if (v.get("tipo") or "").lower() == "promo"]
            elif tipo_val == "regular":
                ventas_filtradas = [v for v in ventas_filtradas if (v.get("tipo") or "").lower() == "regular"]
            ventas_ok = [v for v in ventas_raw if "cancel" not in (v.get("status_raw") or "").lower()]
            hoy = datetime.now().date()
            primer_dia = hoy.replace(day=1)
            ultimo_mes = primer_dia - timedelta(days=1)
            fecha_val = filtro_fecha_ref.get("val", "mes_actual")
            if fecha_val == "mes_anterior":
                dias_total = calendar.monthrange(ultimo_mes.year, ultimo_mes.month)[1]
            else:
                dias_total = (hoy - primer_dia).days + 1
            total_monto_ok = sum(v["monto"] for v in ventas_ok)
            total_unidades_ok = sum(v["cantidad"] for v in ventas_ok)
            n_ventas_ok = len(ventas_ok)
            ticket_promedio = total_monto_ok / n_ventas_ok if n_ventas_ok > 0 else 0
            header_card.clear()
            with header_card:
                with ui.card().classes("w-full p-4 bg-grey-2"):
                    with ui.row().classes("w-full gap-6 flex-wrap items-center"):
                        with ui.column().classes("gap-0"):
                            ui.label("Total Ventas $").classes("text-xs text-gray-600")
                            ui.label(f"$ {total_monto_ok:,.0f}".replace(",", ".")).classes("text-lg font-bold text-primary")
                        with ui.column().classes("gap-0"):
                            ui.label("Total Ventas U").classes("text-xs text-gray-600")
                            ui.label(str(n_ventas_ok)).classes("text-lg font-bold text-primary")
                        with ui.column().classes("gap-0"):
                            ui.label("Total de días").classes("text-xs text-gray-600")
                            ui.label(str(dias_total)).classes("text-lg font-bold text-primary")
                        ui.element("div").classes("w-px h-8 bg-gray-400 shrink-0")
                        with ui.column().classes("gap-0"):
                            ui.label("Ventas Diarias U").classes("text-xs text-gray-600")
                            ventas_diarias_u = total_unidades_ok / dias_total if dias_total > 0 else 0
                            ui.label(f"{ventas_diarias_u:,.1f}".replace(",", ".")).classes("text-lg font-bold text-primary")
                        with ui.column().classes("gap-0"):
                            ui.label("Ventas Diarias $").classes("text-xs text-gray-600")
                            ventas_diarias = total_monto_ok / dias_total if dias_total > 0 else 0
                            ui.label(f"$ {ventas_diarias:,.0f}".replace(",", ".")).classes("text-lg font-bold text-primary")
                        with ui.column().classes("gap-0"):
                            ui.label("Ticket promedio").classes("text-xs text-gray-600")
                            ui.label(f"$ {ticket_promedio:,.0f}".replace(",", ".")).classes("text-lg font-bold text-primary")
                        ui.element("div").classes("w-px h-8 bg-gray-400 shrink-0")
                        with ui.column().classes("gap-0"):
                            ui.label("Ganancia Neta Calculada").classes("text-xs text-gray-600")
                            ui.label(f"$ {ganancia_neta_ref.get('val', 0):,.0f}".replace(",", ".")).classes("text-lg font-bold text-primary")
                        ui.element("div").classes("w-px h-8 bg-gray-400 shrink-0")
                        ui.button("Actualizar", on_click=lambda: _cargar_ventas(), color="primary").props("icon=refresh no-caps").classes("rounded px-3")
            result_area.clear()
            with result_area:
                if not ventas_raw:
                    ui.label("No hay ventas desde el 1 de este mes.").classes("text-gray-500")
                elif not ventas_filtradas:
                    ui.label("No hay ventas con el filtro seleccionado.").classes("text-gray-500")
                else:
                    if agrupar_ref.get("val"):
                        # Solo agrupar ventas con estado Concretada (paid)
                        ventas_a_agrupar = [v for v in ventas_raw if (v.get("status_raw") or "").lower() == "paid"]
                        if not ventas_a_agrupar:
                            ui.label("No hay ventas Concretadas para agrupar.").classes("text-gray-500")
                        else:
                            grupos: Dict[str, Dict[str, Any]] = {}
                            for v in ventas_a_agrupar:
                                key = v.get("agrupar_key") or (v.get("productos") or v.get("title", "—"))
                                if key not in grupos:
                                    grupos[key] = {
                                        "productos": v.get("productos") or v.get("title", "—"),
                                        "tipos_venta": set(),
                                        "tipos_oferta": set(),
                                        "tipos_oferta_display": set(),
                                        "cuotas": set(),
                                        "item_ids": set(),
                                        "cantidad": 0,
                                        "monto": 0.0,
                                        "dt": v.get("dt"),
                                    }
                                tipo_oferta_val = v.get("tipo") or v.get("tipo_oferta") or "Regular"
                                grupos[key]["tipos_oferta"].add(str(tipo_oferta_val))
                                tipo_disp = v.get("tipo_display") or tipo_oferta_val
                                grupos[key]["tipos_oferta_display"].add(str(tipo_disp))
                                if v.get("tipo_venta") and v.get("tipo_venta") != "—":
                                    grupos[key]["tipos_venta"].add(str(v["tipo_venta"]))
                                if v.get("cuotas"):
                                    grupos[key]["cuotas"].add(str(v["cuotas"]))
                                if v.get("item_id") and v.get("item_id") != "—":
                                    grupos[key]["item_ids"].add(str(v["item_id"]))
                                grupos[key]["cantidad"] += v["cantidad"]
                                grupos[key]["monto"] += v["monto"]
                            filas = list(grupos.values())
                            sort_col = sort_col_ventas.get("val", "cantidad")
                            asc = sort_asc_ventas.get("val", False)
                            if sort_col == "productos":
                                filas.sort(key=lambda x: str(x.get("productos", "")).lower(), reverse=not asc)
                            elif sort_col == "monto":
                                filas.sort(key=lambda x: x["monto"], reverse=not asc)
                            else:
                                filas.sort(key=lambda x: x["cantidad"], reverse=not asc)
                            with ui.element("div").classes("w-full"):
                                with ui.element("table").classes("w-full border-collapse text-sm"):
                                    with ui.element("thead"):
                                        with ui.element("tr").classes("bg-primary text-white font-semibold"):
                                            with ui.element("th").classes("px-2 py-2 border text-center"):
                                                ui.label("#")
                                            with ui.element("th").classes("px-2 py-2 border text-center"):
                                                ui.label("ID publicación")
                                            with ui.element("th").classes("px-2 py-2 border text-center"):
                                                ui.label("Publicación")
                                            with ui.element("th").classes("px-2 py-2 border text-center"):
                                                ui.label("Cuotas")
                                            with ui.element("th").classes("px-2 py-2 border text-center"):
                                                ui.label("Tipo")
                                            with ui.element("th").classes("px-2 py-2 border text-center"):
                                                ui.button("Producto", on_click=lambda: _on_sort_ventas("productos")).props("flat dense no-caps").classes("text-white hover:bg-white/20 cursor-pointer font-semibold")
                                            with ui.element("th").classes("px-2 py-2 border text-center"):
                                                ui.button("Cant.", on_click=lambda: _on_sort_ventas("cantidad")).props("flat dense no-caps").classes("text-white hover:bg-white/20 cursor-pointer font-semibold")
                                            with ui.element("th").classes("px-2 py-2 border text-center"):
                                                ui.label("Margen")
                                            with ui.element("th").classes("px-2 py-2 border text-center"):
                                                ui.button("Monto total", on_click=lambda: _on_sort_ventas("monto")).props("flat dense no-caps").classes("text-white hover:bg-white/20 cursor-pointer font-semibold")
                                    with ui.element("tbody"):
                                        for idx, v in enumerate(filas, 1):
                                            productos_key = str(v["productos"])
                                            with ui.element("tr").classes("border-t border-gray-200 hover:bg-gray-50"):
                                                with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                                    ui.label(str(idx))
                                                with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center text-xs"):
                                                    item_ids = v.get("item_ids", set())
                                                    ids_list = sorted(item_ids)[:3]
                                                    ids_str = ", ".join(ids_list)
                                                    if len(item_ids) > 3:
                                                        ids_str += "..."
                                                    ui.label(ids_str or "—")
                                                with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                                    tipos_venta_str = ", ".join(sorted(v.get("tipos_venta", set()))) or "—"
                                                    ui.label(tipos_venta_str).classes("text-xs")
                                                with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                                    cuotas_str = ", ".join(sorted(v.get("cuotas", set()))) or "—"
                                                    ui.label(cuotas_str).classes("text-xs")
                                                with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                                    tipos_oferta_str = ", ".join(sorted(v.get("tipos_oferta_display", v.get("tipos_oferta", set())))) or "—"
                                                    ui.label(tipos_oferta_str).classes("text-xs")
                                                with ui.element("td").classes("px-2 py-1 border-b border-gray-100 max-w-[350px]"):
                                                    ui.label(productos_key[:80]).classes("truncate")
                                                with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                                    ui.label(str(v["cantidad"]))
                                                with ui.element("td").classes("px-2 py-1 border-b border-gray-100"):
                                                    _inp = ui.input(value=margenes_ref.get(productos_key, "")).props("dense").classes("w-20")
                                                    _inp.on_value_change(lambda e, k=productos_key: _update_margen(k, str(getattr(e, "value", "") or "")))
                                                with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-right font-medium"):
                                                    ui.label(f"$ {v['monto']:,.0f}".replace(",", "."))
                    else:
                        sort_col = sort_col_ventas.get("val", "dt")
                        asc = sort_asc_ventas.get("val", False)
                        ventas_orden = sorted(
                            ventas_filtradas,
                            key=lambda x: _sort_key_ventas(x, sort_col),
                            reverse=not asc,
                        )
                        with ui.element("div").classes("w-full"):
                            with ui.element("table").classes("w-full border-collapse text-sm"):
                                with ui.element("thead"):
                                    with ui.element("tr").classes("bg-primary text-white font-semibold"):
                                        cols_ventas = [
                                            ("#", "#", "text-center"),
                                            ("dt", "Fecha", "text-center"),
                                            ("item_id", "ID publicación", "text-center"),
                                            ("tipo_venta", "Publicacion", "text-center"),
                                            ("cuotas", "Cuotas", "text-center"),
                                            ("tipo", "Tipo", "text-center"),
                                            ("productos", "Producto", "text-center"),
                                            ("cantidad", "Cant.", "text-center"),
                                            ("monto", "Monto", "text-center"),
                                            ("status", "Estado", "text-center"),
                                        ]
                                        for col_key, h, align in cols_ventas:
                                            th_cls = f"px-2 py-2 border {align or 'text-left'}"
                                            with ui.element("th").classes(th_cls):
                                                if col_key == "#":
                                                    ui.label(h)
                                                else:
                                                    ui.button(h, on_click=lambda c=col_key: _on_sort_ventas(c)).props("flat dense no-caps").classes("text-white hover:bg-white/20 cursor-pointer font-semibold")
                                with ui.element("tbody"):
                                    for idx, v in enumerate(ventas_orden, 1):
                                        with ui.element("tr").classes("border-t border-gray-200 hover:bg-gray-50"):
                                            with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                                ui.label(str(idx))
                                            with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                                ui.label(v["fecha"])
                                            with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                                ui.label(v.get("item_id", "—"))
                                            with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                                ui.label(v.get("tipo_venta", "—"))
                                            with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                                ui.label(v.get("cuotas", "—"))
                                            with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                                ui.label(v.get("tipo_display", v.get("tipo", v.get("tipo_oferta", "Regular"))))
                                            with ui.element("td").classes("px-2 py-1 border-b border-gray-100 max-w-[300px]"):
                                                ui.label(v.get("productos", v.get("title", "—"))[:80]).classes("truncate")
                                            with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                                ui.label(str(v["cantidad"]))
                                            with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-right font-medium"):
                                                ui.label(v["monto_fmt"])
                                            with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                                ui.label(v["status"])

        async def _cargar_ventas_async() -> None:
            nonlocal ventas_raw
            try:
                profile = await run.io_bound(ml_get_user_profile, access_token)
                seller_id = (profile or {}).get("id") or await run.io_bound(ml_get_user_id, access_token)
                if not seller_id:
                    result_area.clear()
                    with result_area:
                        ui.label("No se pudo obtener el perfil del vendedor.").classes("text-negative")
                    if filtro_controls_ref:
                        filtro_controls_ref[0].set_visibility(True)
                    return
                hoy = datetime.now().date()
                primer_dia = hoy.replace(day=1)
                ultimo_mes = primer_dia - timedelta(days=1)
                primer_dia_anterior = ultimo_mes.replace(day=1)
                fecha_val = filtro_fecha_ref.get("val", "mes_actual")
                if fecha_val == "mes_actual":
                    date_from = primer_dia.strftime("%Y-%m-%dT00:00:00.000-03:00")
                    date_to = hoy.strftime("%Y-%m-%dT23:59:59.999-03:00")
                    orders_data = await run.io_bound(
                        ml_get_orders, access_token, str(seller_id), limit=2000, offset=0,
                        date_from=date_from, date_to=date_to,
                    )
                else:
                    date_from = primer_dia_anterior.strftime("%Y-%m-%dT00:00:00.000-03:00")
                    date_to = ultimo_mes.strftime("%Y-%m-%dT23:59:59.999-03:00")
                    orders_data = await run.io_bound(
                        ml_get_orders, access_token, str(seller_id), limit=2000, offset=0,
                        date_from=date_from, date_to=date_to,
                    )
            except Exception as e:
                result_area.clear()
                with result_area:
                    ui.label(f"❌ Error al cargar ventas: {e}").classes("text-negative")
                if filtro_controls_ref:
                    filtro_controls_ref[0].set_visibility(True)
                return
            raw_orders = orders_data.get("results") or orders_data.get("orders") or orders_data.get("elements") or []
            orders = [o for o in raw_orders if isinstance(o, dict)]
            hoy = datetime.now().date()
            primer_dia = hoy.replace(day=1)
            ultimo_mes = primer_dia - timedelta(days=1)
            primer_dia_anterior = ultimo_mes.replace(day=1)
            fecha_val = filtro_fecha_ref.get("val", "mes_actual")
            if fecha_val not in ("mes_actual", "mes_anterior"):
                fecha_val = "mes_actual"
            # Merge con órdenes ya cargadas (p.ej. mes anterior cuando cambian de mes)
            orders_existentes = all_orders_ref.get("orders") or []
            ids_existentes = {str(o.get("id")) for o in orders_existentes if o.get("id")}
            for o in orders:
                if o.get("id") and str(o.get("id")) not in ids_existentes:
                    orders_existentes.append(o)
                    ids_existentes.add(str(o.get("id")))
            all_orders_ref["orders"] = orders_existentes
            if fecha_val == "mes_actual":
                orders_periodo = [o for o in orders_existentes if _order_in_range(o, primer_dia, hoy)]
                all_orders_ref["mes_actual_cargado"] = True
            else:
                orders_periodo = [o for o in orders_existentes if _order_in_range(o, primer_dia_anterior, ultimo_mes)]
                all_orders_ref["mes_anterior_cargado"] = True
            item_ids_to_fetch: List[str] = []
            for o in orders_periodo:
                for it in o.get("order_items") or o.get("items") or []:
                    if isinstance(it, dict):
                        obj = it.get("item") or it
                        iid = (str(obj.get("id") or it.get("item_id") or "").strip() if isinstance(obj, dict) else str(it.get("item_id") or "").strip())
                        if iid and iid not in item_ids_to_fetch:
                            item_ids_to_fetch.append(iid)
            item_id_to_catalog: Dict[str, bool] = dict(all_orders_ref.get("item_id_to_catalog") or {})
            item_id_to_sku: Dict[str, str] = dict(all_orders_ref.get("item_id_to_sku") or {})
            item_id_to_tipo_venta: Dict[str, str] = dict(all_orders_ref.get("item_id_to_tipo_venta") or {})
            item_id_to_cuotas: Dict[str, str] = dict(all_orders_ref.get("item_id_to_cuotas") or {})
            item_id_to_tipo_oferta: Dict[str, str] = dict(all_orders_ref.get("item_id_to_tipo_oferta") or {})
            ids_pendientes = [iid for iid in item_ids_to_fetch if iid not in item_id_to_catalog]
            if ids_pendientes and access_token:
                def _fetch_catalog_info(ids: List[str]) -> List[Optional[Dict[str, Any]]]:
                    """Multiget para catalog_listing, cuotas, SKU. tipo_oferta se obtiene por sale_price."""
                    out: List[Optional[Dict[str, Any]]] = []
                    attrs = "id,catalog_listing,catalog_product_id,listing_type_id,attributes,sale_terms"
                    for i in range(0, len(ids), 20):
                        batch = ids[i : i + 20]
                        batch_bodies = ml_get_items_multiget_with_attributes(access_token, batch, attrs)
                        out.extend(batch_bodies)
                    return out
                bodies = await run.io_bound(_fetch_catalog_info, ids_pendientes)
                for b in bodies:
                    if b and isinstance(b, dict):
                        iid = str(b.get("id", "") or b.get("item_id", "")).strip()
                        if not iid:
                            continue
                        cl = b.get("catalog_listing")
                        is_catalog = cl is True or str(cl or "").lower() in ("true", "1")
                        item_id_to_catalog[iid] = is_catalog
                        item_id_to_tipo_venta[iid] = _tipo_base_desde_body(b)
                        item_id_to_cuotas[iid] = _cuotas_desde_item(b)
                        attrs_inner = b.get("attributes") or []
                        for a in attrs_inner:
                            if isinstance(a, dict) and (a.get("id") or "").upper() == "SELLER_SKU":
                                sku_val = (a.get("value_name") or a.get("value") or "").strip()
                                if sku_val:
                                    item_id_to_sku[iid] = sku_val
                                break
            # Tipo oferta: usar GET /items/{id}/sale_price (regular_amount != amount = Promo)
            item_id_to_promo_display: Dict[str, str] = dict(all_orders_ref.get("item_id_to_promo_display") or {})
            if item_ids_to_fetch and access_token:
                def _fetch_tipo_oferta_batch(ids: List[str]) -> tuple:
                    result: Dict[str, str] = {}
                    promo_display: Dict[str, str] = {}
                    max_workers = min(8, len(ids))
                    with ThreadPoolExecutor(max_workers=max_workers) as ex:
                        futures = {ex.submit(ml_get_item_sale_price_full, access_token, iid): iid for iid in ids}
                        for fut in as_completed(futures):
                            iid = futures[fut]
                            try:
                                data = fut.result()
                                if data is not None:
                                    amt = data.get("amount")
                                    reg = data.get("regular_amount")
                                    if reg is not None and amt is not None:
                                        try:
                                            reg_f = float(reg)
                                            amt_f = float(amt)
                                            if abs(reg_f - amt_f) > 0.01:
                                                result[iid] = "Promo"
                                                pct = ((reg_f - amt_f) / reg_f * 100) if reg_f > 0 else 0
                                                orig_fmt = f"$ {reg_f:,.0f}".replace(",", ".")
                                                pct_str = f"{pct:.1f}".replace(".", ",")
                                                promo_display[iid] = f"{orig_fmt} ({pct_str}% dto)"
                                            else:
                                                result[iid] = "Regular"
                                        except (TypeError, ValueError):
                                            result[iid] = "Regular"
                                    else:
                                        result[iid] = "Regular"
                                else:
                                    result[iid] = "Regular"
                            except Exception:
                                result[iid] = "Regular"
                    return result, promo_display
                tipo_oferta_map, promo_display_map = await run.io_bound(_fetch_tipo_oferta_batch, list(item_ids_to_fetch))
                for iid, val in tipo_oferta_map.items():
                    if iid:
                        item_id_to_tipo_oferta[iid] = val
                for iid, disp in promo_display_map.items():
                    if iid:
                        item_id_to_promo_display[iid] = disp
            all_orders_ref["item_id_to_promo_display"] = item_id_to_promo_display
            all_orders_ref["item_id_to_catalog"] = item_id_to_catalog
            all_orders_ref["item_id_to_sku"] = item_id_to_sku
            all_orders_ref["item_id_to_tipo_venta"] = item_id_to_tipo_venta
            all_orders_ref["item_id_to_cuotas"] = item_id_to_cuotas
            all_orders_ref["item_id_to_tipo_oferta"] = item_id_to_tipo_oferta
            ventas_mes: List[Dict[str, Any]] = []
            status_map = {"paid": "Concretada", "handling": "En preparación", "shipped": "Enviada", "delivered": "Entregada", "cancelled": "Cancelada", "canceled": "Cancelada"}
            dia_ini, dia_fin = (primer_dia, hoy) if fecha_val == "mes_actual" else (primer_dia_anterior, ultimo_mes) if fecha_val == "mes_anterior" else (None, None)
            for ord_item in orders_periodo:
                dt_str = ord_item.get("date_created") or ord_item.get("date_closed") or ord_item.get("date_last_updated") or ""
                if not dt_str or not isinstance(dt_str, str):
                    continue
                try:
                    dt = datetime.strptime(dt_str[:10], "%Y-%m-%d").date()
                except Exception:
                    continue
                if dia_ini is not None and (dt < dia_ini or dt > dia_fin):
                    continue
                ord_total = ord_item.get("total_amount") or ord_item.get("paid_amount")
                if ord_total is None and ord_item.get("payments"):
                    pay = ord_item["payments"][0] if isinstance(ord_item["payments"], list) else {}
                    ord_total = pay.get("total_amount") or pay.get("total_paid_amount") or pay.get("transaction_amount")
                try:
                    ord_total = float(ord_total or 0)
                except (TypeError, ValueError):
                    ord_total = 0.0
                status_raw = (ord_item.get("status") or "").strip().lower()
                status_display = status_map.get(status_raw, status_raw or "—")
                items = ord_item.get("order_items") or ord_item.get("items") or []
                ord_qty = sum(int(it.get("quantity") or it.get("qty") or 0) for it in items if isinstance(it, dict))
                for it in items:
                    if not isinstance(it, dict):
                        continue
                    obj = it.get("item") or it
                    qty = int(it.get("quantity") or it.get("qty") or 0)
                    if qty == 0:
                        continue
                    unit_price = it.get("unit_price")
                    if unit_price is None:
                        unit_price = ord_total / ord_qty if ord_qty > 0 else 0
                    try:
                        unit_price = float(unit_price or 0)
                    except (TypeError, ValueError):
                        unit_price = 0
                    item_monto = qty * unit_price
                    titulo = (obj.get("title") if isinstance(obj, dict) else str(obj)) or it.get("title") or "—"
                    item_id = (str(obj.get("id") or it.get("item_id") or "") if isinstance(obj, dict) else str(it.get("item_id") or "")).strip()
                    catalog_id = str(obj.get("catalog_product_id") or it.get("catalog_product_id") or "").strip() if isinstance(obj, dict) else str(it.get("catalog_product_id") or "").strip()
                    cl = obj.get("catalog_listing") if isinstance(obj, dict) else it.get("catalog_listing")
                    if cl is None and isinstance(obj, dict):
                        cl = it.get("catalog_listing")
                    catalog = cl is True or str(cl or "").lower() in ("true", "1")
                    if cl is None or (not catalog and item_id):
                        catalog = item_id_to_catalog.get(item_id, False) or item_id_to_catalog.get(item_id.upper(), False) or item_id_to_catalog.get(item_id.lower(), False)
                    tipo = "Catálogo" if catalog else "Propia"
                    # Para propias: usar SKU si existe (evita duplicados por mismo producto distinto id)
                    sku = item_id_to_sku.get(item_id) or item_id_to_sku.get(item_id.upper()) or item_id_to_sku.get(item_id.lower()) or ""
                    agrupar_key = catalog_id or (sku if tipo == "Propia" and sku else "") or item_id or titulo
                    cuotas = item_id_to_cuotas.get(item_id) or item_id_to_cuotas.get(item_id.upper()) or item_id_to_cuotas.get(item_id.lower()) or "x1"
                    tipo_oferta, tipo_display = _tipo_oferta_desde_order_item(it, item_id, item_id_to_tipo_oferta)
                    if tipo_display is None and (tipo_oferta or "").lower() == "promo":
                        tipo_display = item_id_to_promo_display.get(item_id) or item_id_to_promo_display.get(item_id.upper() or "") or item_id_to_promo_display.get(item_id.lower() or "") or "Promo"
                    ventas_mes.append({
                        "dt": dt,
                        "fecha": dt.strftime("%d/%m/%Y"),
                        "productos": titulo[:100],
                        "title": titulo[:100],
                        "tipo_venta": tipo,
                        "cuotas": cuotas,
                        "tipo": tipo_oferta,
                        "tipo_oferta": tipo_oferta,
                        "tipo_display": tipo_display or tipo_oferta,
                        "cantidad": qty,
                        "monto": item_monto,
                        "monto_fmt": f"$ {item_monto:,.0f}".replace(",", "."),
                        "status": status_display,
                        "status_raw": status_raw,
                        "agrupar_key": agrupar_key,
                        "item_id": item_id or "—",
                    })
            ventas_raw = ventas_mes
            if filtro_controls_ref:
                filtro_controls_ref[0].set_visibility(True)
            _pintar_tabla()

        filtro_controls_ref: List[Any] = []  # Referencia al row de controles para mostrar/ocultar

        with filtro_row:
            filtro_controls = ui.row().classes("items-center gap-4")
            filtro_controls.set_visibility(False)
            filtro_controls_ref.append(filtro_controls)
            with filtro_controls:
                filtro_fecha = ui.select(
                    {"mes_actual": "Mes actual", "mes_anterior": "Mes anterior"},
                    value=filtro_fecha_ref.get("val", "mes_actual"),
                    label="Fecha",
                ).classes("w-36").bind_value(filtro_fecha_ref, "val")
                filtro_fecha.on_value_change(lambda: _aplicar_filtro_fecha())
                filtro_publicacion = ui.select(
                    {"todas": "Todas", "propias": "Propias", "catalogo": "Catálogo"},
                    value=filtro_publicacion_ref.get("val", "todas"),
                    label="Publicación",
                ).classes("w-36").bind_value(filtro_publicacion_ref, "val")
                filtro_publicacion.on_value_change(lambda: _pintar_tabla())
                filtro_cuotas = ui.select(
                    {"todas": "Todas", "x1": "x1", "x3": "x3", "x6": "x6"},
                    value=filtro_cuotas_ref.get("val", "todas"),
                    label="Cuotas",
                ).classes("w-36").bind_value(filtro_cuotas_ref, "val")
                filtro_cuotas.on_value_change(lambda: _pintar_tabla())
                filtro_tipo = ui.select(
                    {"todas": "Todas", "promo": "Promo", "regular": "Regular"},
                    value=filtro_tipo_ref.get("val", "todas"),
                    label="Tipo",
                ).classes("w-36").bind_value(filtro_tipo_ref, "val")
                filtro_tipo.on_value_change(lambda: _pintar_tabla())
                filtro_estado = ui.select(
                    {"todas": "Todas", "pagada": "Concretada", "cancelada": "Cancelada"},
                    value=filtro_estado_ref.get("val", "pagada"),
                    label="Estado",
                ).classes("w-36").bind_value(filtro_estado_ref, "val")
                filtro_estado.on_value_change(lambda: _pintar_tabla())
                btn_agrupar = ui.button("Agrupar", on_click=lambda: _toggle_agrupar(), color="primary").props("no-caps")
                _update_btn_agrupar()
                ui.button("Calcular", on_click=lambda: _calcular_ganancia(), color="primary").props("no-caps")

        _cargar_ventas()


def build_tab_precios(container) -> None:
    """Pestaña Productos: clic en el cuadradito de la fila para editar precio."""
    container.clear()
    user = require_login()
    if not user:
        return

    with container:
        access_token = get_ml_access_token(user["id"])
        if not access_token:
            ui.label("⚠️ No tienes MercadoLibre vinculado. Ve a Configuración y conecta tu cuenta.").classes("text-warning mb-4")
            return

        result_area = ui.column().classes("w-full gap-2")
        include_paused_ref: Dict[str, bool] = {"val": True}  # Incluir pausadas (sin stock) para poder mostrarlas
        filtro_stock_ref: Dict[str, str] = {"val": "con_stock"}  # Por defecto mostrar solo con stock

        with result_area:
            with ui.card().classes("w-full p-8 items-center gap-4"):
                ui.spinner(size="xl")
                ui.label("Cargando productos...").classes("text-xl text-gray-700")

        def cargar_precios() -> None:
            result_area.clear()
            with result_area:
                with ui.card().classes("w-full p-8 items-center gap-4"):
                    ui.spinner(size="xl")
                    ui.label("Cargando productos...").classes("text-xl text-gray-700")
            background_tasks.create(_cargar_precios_async(result_area, access_token, user, cargar_precios, include_paused_ref, filtro_stock_ref), name="cargar_precios")

        async def _cargar_precios_async(area, token, usr, on_actualizar, inc_paused_ref, f_stock_ref) -> None:
            try:
                data = await run.io_bound(ml_get_my_items, token, inc_paused_ref.get("val", False))
            except requests.exceptions.HTTPError as e:
                area.clear()
                with area:
                    ui.label(f"❌ Error de la API de MercadoLibre: {e}").classes("text-negative mb-2")
                return
            except Exception as e:
                area.clear()
                with area:
                    ui.label(f"❌ Error al conectar: {e}").classes("text-negative")
                return
            try:
                _mostrar_tabla_precios(area, data, token, usr, on_actualizar, inc_paused_ref, f_stock_ref)
            except Exception as e:
                area.clear()
                with area:
                    ui.label(f"❌ Error al mostrar datos: {e}").classes("text-negative")

        background_tasks.create(_cargar_precios_async(result_area, access_token, user, cargar_precios, include_paused_ref, filtro_stock_ref), name="cargar_precios")


def _mostrar_tabla_precios(
    result_area, data: Dict[str, Any], access_token: str, user: Dict[str, Any], on_actualizar=None,
    include_paused_ref: Optional[Dict[str, bool]] = None, filtro_stock_ref: Optional[Dict[str, str]] = None,
) -> None:
    """Pinta la tabla de precios con celda de precio clickable para editar."""
    def fmt_moneda(val: Any) -> str:
        if val is None:
            return "$0"
        try:
            n = int(float(val))
            return "$" + f"{n:,}".replace(",", ".")
        except (TypeError, ValueError):
            return "$0"

    def fmt_miles(val: Any) -> str:
        if val is None:
            return "0"
        try:
            n = int(float(val))
            return f"{n:,}".replace(",", ".")
        except (TypeError, ValueError):
            return "0"

    items = data.get("results", [])
    result_area.clear()
    if not items:
        with result_area:
            ui.label("No tienes publicaciones en MercadoLibre o aún no se han cargado.").classes("text-gray-500")
        return

    # Grupos de duplicados conocidos: solo fusionar estos (mismo producto en distintas cuotas/promos).
    DUPLICATE_GROUPS = [
        ["MLA2815562106", "MLA1674890445", "MLA2957178650"],
        ["MLA2896534310", "MLA2896560238"],
        ["MLA1674709457", "MLA1674787133", "MLA1658462387"],
    ]
    id_to_group: Dict[str, int] = {}
    for gidx, grupo in enumerate(DUPLICATE_GROUPS):
        for iid in grupo:
            id_to_group[str(iid).strip().upper()] = gidx
            id_to_group[str(iid).strip()] = gidx

    grupos_merged: Dict[int, List[Dict[str, Any]]] = {}
    items_sueltos: List[Dict[str, Any]] = []
    for i in items:
        iid = str(i.get("id") or "").strip()
        gidx = id_to_group.get(iid) or id_to_group.get(iid.upper())
        if gidx is not None:
            grupos_merged.setdefault(gidx, []).append(i)
        else:
            items_sueltos.append(i)

    items_dedup: List[Dict[str, Any]] = list(items_sueltos)
    for gidx, grupo in grupos_merged.items():
        if len(grupo) == 1:
            items_dedup.append(grupo[0])
            continue
        total_stock = sum(int(x.get("available_quantity") or 0) for x in grupo)
        total_sold = sum(int(x.get("sold_quantity") or 0) for x in grupo)
        principal = max(grupo, key=lambda x: int(x.get("available_quantity") or 0))
        fusionado = dict(principal)
        fusionado["available_quantity"] = total_stock
        fusionado["sold_quantity"] = total_sold
        items_dedup.append(fusionado)

    items_loaded = []
    for i in items_dedup:
        precio = i.get("price") or 0
        sale_price = i.get("sale_price")
        precio_real = float(sale_price) if sale_price is not None else precio
        stock = i.get("available_quantity") or 0
        subtotal = precio * stock
        tipo = "Catalogo" if i.get("catalog_listing") is True else "Propia"
        tiene_promo = sale_price is not None and abs(float(sale_price) - float(precio or 0)) > 0.01
        # Última modificación: last_updated de la API (ej. "2025-02-15T19:30:00.000Z")
        def _fmt_fecha(s: Any) -> str:
            if not s or not isinstance(s, str):
                return "—"
            try:
                dt = datetime.strptime(s[:10], "%Y-%m-%d")
                return dt.strftime("%d/%m/%Y")
            except Exception:
                return str(s)[:10] if s else "—"

        last_upd = i.get("last_updated")
        raw_fecha = last_upd[:10] if last_upd and isinstance(last_upd, str) and len(last_upd) >= 10 else None
        ult_modif_fmt = _fmt_fecha(raw_fecha) if raw_fecha else "—"
        items_loaded.append({
            **i,
            "price_fmt": fmt_moneda(precio),
            "sale_price": sale_price,
            "sale_price_fmt": fmt_moneda(precio_real) if tiene_promo else "-",
            "stock_fmt": fmt_miles(stock),
            "subtotal": subtotal,
            "subtotal_fmt": fmt_moneda(subtotal),
            "tipo": tipo,
            "marca": i.get("marca") or "—",
            "color": i.get("color") or "—",
            "title": str(i.get("title") or ""),
            "ult_modif_fmt": ult_modif_fmt,
            "fecha_ult_modif": raw_fecha or "",  # YYYY-MM-DD para ordenar; vacío si no hay
        })

    publicaciones_totales = len(items_loaded)
    publicaciones_con_stock = sum(1 for i in items_loaded if (i.get("available_quantity") or 0) > 0)
    publicaciones_propias_con_stock = sum(1 for i in items_loaded if i.get("tipo") == "Propia" and (i.get("available_quantity") or 0) > 0)
    publicaciones_catalogo_con_stock = sum(1 for i in items_loaded if i.get("tipo") == "Catalogo" and (i.get("available_quantity") or 0) > 0)
    unidades_propias_en_stock = sum(i.get("available_quantity") or 0 for i in items_loaded if i.get("tipo") == "Propia")
    total_pesos_propias = sum(i.get("subtotal") or 0 for i in items_loaded if i.get("tipo") == "Propia")
    dolar_str = get_cotizador_param("dolar_oficial", user["id"]) or COTIZADOR_DEFAULTS.get("dolar_oficial", "1475")
    dolar_oficial = float(str(dolar_str).replace(",", ".").strip()) if dolar_str else 1475.0
    if dolar_oficial <= 0:
        dolar_oficial = 1475.0
    total_dolares_propias = (total_pesos_propias / dolar_oficial) if dolar_oficial else None

    def abrir_editar_precio(row: Dict[str, Any]) -> None:
        if row.get("tipo") not in ("Propia", "Prop Comb"):
            ui.notify("Solo se puede editar el precio de publicaciones propias.", color="warning")
            return
        item_id = str(row.get("id", ""))
        if not item_id:
            return
        try:
            precio_actual = float(row.get("price") or 0)
        except (TypeError, ValueError):
            precio_actual = 0.0
        dialog = ui.dialog()
        with dialog:
            with ui.card().classes("p-4 min-w-[320px]"):
                ui.label("Editar precio").classes("text-lg font-semibold mb-2")
                ui.label((row.get("title") or "")[:80] + ("..." if len(row.get("title") or "") > 80 else "")).classes("text-sm text-gray-600 mb-2")
                inp_precio = ui.input("Nuevo precio ($)", value=str(int(precio_actual))).classes("w-full")
                inp_precio.props("type=number min=1 step=1")

                def guardar() -> None:
                    try:
                        nuevo = float(inp_precio.value or 0)
                    except (TypeError, ValueError):
                        ui.notify("Precio inválido.", color="negative")
                        return
                    if nuevo < 1:
                        ui.notify("El precio debe ser al menos 1.", color="negative")
                        return
                    dialog.close()
                    ui.notify("Actualizando precio...", color="info")
                    client = context.client

                    async def _actualizar_precio() -> None:
                        try:
                            await run.io_bound(ml_update_item_price, access_token, item_id, nuevo)
                            with client:
                                ui.notify("Precio actualizado correctamente. Refrescando...", color="positive")
                                if on_actualizar:
                                    def _refrescar() -> None:
                                        with client:
                                            on_actualizar()
                                    ui.timer(0.3, _refrescar, once=True)
                        except requests.exceptions.HTTPError as err:
                            with client:
                                ui.notify(f"Error al actualizar: {err}", color="negative")
                        except Exception as err:
                            with client:
                                ui.notify(f"Error: {err}", color="negative")

                    background_tasks.create(_actualizar_precio())

                with ui.row().classes("w-full justify-end gap-2 mt-3"):
                    ui.button("Cancelar", on_click=lambda: dialog.close()).props("flat")
                    ui.button("Guardar", on_click=guardar, color="primary")

        dialog.open()

    current_filtrados: List[Dict[str, Any]] = []
    current_table: List[Any] = []
    sort_col_ref: Dict[str, Any] = {"val": "title"}
    sort_asc_ref: Dict[str, bool] = {"val": True}

    def _sort_key_precios(row: Dict[str, Any], col_name: str) -> Any:
        """Devuelve valor para ordenar según el tipo de columna."""
        if col_name in ("price", "subtotal"):
            return float(row.get(col_name) or 0)
        if col_name in ("available_quantity", "sold_quantity"):
            return int(row.get(col_name) or 0)
        if col_name == "fecha_ult_modif":
            return row.get("fecha_ult_modif") or ""
        return str(row.get(col_name) or "").lower()

    def _on_sort_click(col_name: str) -> None:
        """Ordena por columna al hacer clic en el encabezado."""
        if sort_col_ref.get("val") == col_name:
            sort_asc_ref["val"] = not sort_asc_ref.get("val", True)
        else:
            sort_col_ref["val"] = col_name
            sort_asc_ref["val"] = True
        filtrar_y_pintar()

    def _generar_jpg_precios(filtrados_actuales: List[Dict[str, Any]], include_ventas: bool = False) -> Optional[str]:
        """Genera un JPG con la tabla de stock. include_ventas=True agrega columna Ventas al final."""
        try:
            from PIL import Image, ImageDraw, ImageFont
        except ImportError:
            return None
        if not filtrados_actuales:
            return None
        ahora = datetime.now()
        header_nt = f"Stock {ahora.day:02d}-{ahora.month:02d}-{ahora.year % 100:02d}"
        # Columnas: Stock dd-mm-aa, Marca, Producto, Color, Stock [, Ventas]
        col_widths = [160, 130, 520, 100, 100]
        headers = [header_nt, "Marca", "Producto", "Color", "Stock"]
        if include_ventas:
            col_widths = [160, 130, 440, 100, 100, 100]
            headers.append("Ventas")
        row_h = 28
        header_h = 36
        pad = 12
        font_size = 12
        font_paths = [
            "arial.ttf",
            "Arial.ttf",
            os.path.join(os.environ.get("WINDIR", ""), "Fonts", "arial.ttf"),
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        ]
        font = font_bold = None
        for fp in font_paths:
            if not fp:
                continue
            try:
                if os.path.isfile(fp):
                    font = ImageFont.truetype(fp, font_size)
                    font_bold = font
                    break
            except Exception:
                continue
        if font is None:
            font = ImageFont.load_default()
            font_bold = font

        def _draw_centered(dx: float, dy: float, cw: float, ch: float, text: str, fill_color, fnt) -> None:
            bbox = draw.textbbox((0, 0), text, font=fnt)
            tw = bbox[2] - bbox[0]
            th = bbox[3] - bbox[1]
            tx = dx + (cw - tw) / 2
            ty = dy + (ch - th) / 2
            draw.text((tx, ty), text, fill=fill_color, font=fnt)

        def _draw_left(dx: float, dy: float, cw: float, ch: float, text: str, fill_color, fnt) -> None:
            bbox = draw.textbbox((0, 0), text, font=fnt)
            th = bbox[3] - bbox[1]
            tx = dx + 4
            ty = dy + (ch - th) / 2
            draw.text((tx, ty), text, fill=fill_color, font=fnt)

        w = sum(col_widths) + pad * 2
        h = header_h + len(filtrados_actuales) * row_h + pad * 2
        img = Image.new("RGB", (w, h), (255, 255, 255))
        draw = ImageDraw.Draw(img)
        border_thick = 3
        draw.rectangle([border_thick, border_thick, w - 1 - border_thick, h - 1 - border_thick], outline=(0, 0, 0), width=border_thick)
        header_border = 2
        draw.rectangle([pad, pad, w - pad, pad + header_h], outline=(0, 0, 0), width=header_border)
        x = pad
        for cw, title in zip(col_widths, headers):
            draw.rectangle([x, pad, x + cw, pad + header_h], fill=(25, 118, 210), outline=(0, 0, 0), width=header_border)
            _draw_centered(x, pad, cw, header_h, str(title), (255, 255, 255), font_bold)
            x += cw
        y = pad + header_h
        for r in filtrados_actuales:
            x = pad
            cells = [
                str(r.get("id", ""))[:18],
                str(r.get("marca", "—"))[:22],
                (r.get("title") or "")[:70],
                str(r.get("color", "—"))[:15],
                r.get("stock_fmt", "0"),
            ]
            cell_align = ["center", "center", "left", "center", "center"]
            if include_ventas:
                ventas_val = r.get("sold_quantity")
                try:
                    ventas_str = fmt_miles(ventas_val) if ventas_val is not None else "0"
                except Exception:
                    ventas_str = "0"
                cells.append(ventas_str)
                cell_align.append("center")
            for cw, cell, align in zip(col_widths, cells, cell_align):
                draw.rectangle([x, y, x + cw, y + row_h], outline=(200, 200, 200))
                if align == "left":
                    _draw_left(x, y, cw, row_h, str(cell), (0, 0, 0), font)
                else:
                    _draw_centered(x, y, cw, row_h, str(cell), (0, 0, 0), font)
                x += cw
            y += row_h
        out = tempfile.NamedTemporaryFile(suffix=".jpg", delete=False)
        out.close()
        img.save(out.name, "JPEG", quality=90)
        return out.name

    def imprimir_tabla(include_ventas: bool = False) -> None:
        client = context.client
        tbl = current_table[0] if current_table else None
        imprimir_ventas = include_ventas

        async def _imprimir_async() -> None:
            rows_to_print = current_filtrados
            if tbl:
                try:
                    rows_orden_pantalla = await tbl.get_filtered_sorted_rows(timeout=2)
                    if rows_orden_pantalla:
                        rows_to_print = rows_orden_pantalla
                except Exception:
                    pass
            if not rows_to_print:
                with client:
                    ui.notify("No hay datos para imprimir. Aplicá filtros y volvé a intentar.", color="warning")
                return
            profile = await run.io_bound(ml_get_user_profile, access_token)
            nickname = (profile or {}).get("nickname") or "Usuario"
            safe_name = "".join(c for c in str(nickname) if c.isalnum() or c in "_-").strip() or "Usuario"
            path = _generar_jpg_precios(rows_to_print, include_ventas=imprimir_ventas)
            if path:
                ahora = datetime.now()
                nombre_archivo = f"{safe_name}_{ahora.day:02d}-{ahora.month:02d}-{ahora.year % 100:02d}-{ahora.hour:02d}-{ahora.minute:02d}.jpg"
                with client:
                    ui.download(path, nombre_archivo)
                    def _borrar_despues() -> None:
                        try:
                            if path and os.path.exists(path):
                                os.unlink(path)
                        except Exception:
                            pass
                    ui.timer(5.0, _borrar_despues, once=True)
            else:
                with client:
                    ui.notify("No se pudo generar la imagen. ¿Tenés Pillow instalado? (pip install Pillow)", color="negative")

        background_tasks.create(_imprimir_async())

    header_style = "background-color: #1976d2; color: white; font-weight: 600;"
    fmt_num_js = "(val) => val != null && val !== '' ? Number(val).toLocaleString('de-DE').replace(/,/g, '.') : '0'"
    fmt_mon_js = "(val) => val != null && val !== '' ? '$' + Number(val).toLocaleString('de-DE').replace(/,/g, '.') : '$0'"
    columns_precios = [
        {"name": "id", "label": "ID", "field": "id", "sortable": True, "align": "left", "headerStyle": header_style, "style": "min-width: 90px"},
        {"name": "marca", "label": "Marca", "field": "marca", "sortable": True, "align": "left", "headerStyle": header_style, "style": "min-width: 100px"},
        {"name": "title", "label": "Producto", "field": "title", "sortable": True, "align": "left", "headerStyle": header_style, "style": "min-width: 220px", ":classes": "(val, row) => (row && row.tipo === 'Propia') ? 'text-primary cursor-pointer' : ''", ":sort": "(a, b, rowA, rowB) => (String(rowA.title||'').toLowerCase()).localeCompare(String(rowB.title||'').toLowerCase(), 'en')"},
        {"name": "color", "label": "Color", "field": "color", "sortable": True, "align": "left", "headerStyle": header_style, "style": "min-width: 90px"},
        {"name": "price", "label": "Precio", "field": "price", "sortable": True, "align": "right", "headerStyle": header_style, ":format": fmt_mon_js, ":classes": "(val, row) => { let c = (row && row.tipo === 'Propia') ? 'text-primary cursor-pointer font-medium' : ''; const hasPromo = row && row.sale_price != null && Math.abs(Number(row.sale_price) - Number(row.price || 0)) > 0.01; return hasPromo ? c + ' line-through' : c; }"},
        {"name": "sale_price_fmt", "label": "Promo", "field": "sale_price_fmt", "sortable": False, "align": "right", "headerStyle": header_style, "style": "min-width: 100px"},
        {"name": "available_quantity", "label": "Stock", "field": "available_quantity", "sortable": True, "align": "right", "headerStyle": header_style, ":format": fmt_num_js},
        {"name": "sold_quantity", "label": "Ventas", "field": "sold_quantity", "sortable": True, "align": "right", "headerStyle": header_style, ":format": fmt_num_js},
        {"name": "subtotal", "label": "Subtotal", "field": "subtotal", "sortable": True, "align": "right", "headerStyle": header_style, ":format": fmt_mon_js},
        {"name": "tipo", "label": "Tipo", "field": "tipo", "sortable": True, "align": "left", "headerStyle": header_style},
        {"name": "status", "label": "Estado", "field": "status", "sortable": True, "align": "left", "headerStyle": header_style, ":format": "(val) => (val || '').toLowerCase() === 'active' ? 'Activa' : 'Suspendida'"},
        {"name": "fecha_ult_modif", "label": "Última modificación", "field": "ult_modif_fmt", "sortable": True, "align": "center", "headerStyle": header_style, "style": "min-width: 110px", ":sort": "(a, b, rowA, rowB) => (rowA.fecha_ult_modif || '').localeCompare(rowB.fecha_ult_modif || '')"},
    ]

    def filtrar_y_pintar() -> None:
        filtrados = list(items_loaded)
        tipo_val = getattr(filtro_tipo, "value", None)
        if tipo_val == "propias":
            filtrados = [x for x in filtrados if x.get("tipo") == "Propia"]
        elif tipo_val == "catalogo":
            filtrados = [x for x in filtrados if x.get("tipo") == "Catalogo"]
        elif tipo_val == "combinadas":
            # Solo propias; ventas = propia + catálogo relacionado (mismo catalog_product_id)
            catalogos = [x for x in items_loaded if x.get("tipo") == "Catalogo"]
            ventas_por_catalog_id: Dict[str, int] = {}
            for c in catalogos:
                cpid = c.get("catalog_product_id")
                if cpid:
                    ventas_por_catalog_id[str(cpid)] = ventas_por_catalog_id.get(str(cpid), 0) + (c.get("sold_quantity") or 0)
            propias = [x for x in filtrados if x.get("tipo") == "Propia"]
            filtrados = []
            for p in propias:
                row = dict(p)
                propia_ventas = p.get("sold_quantity") or 0
                catalog_ventas = ventas_por_catalog_id.get(str(p.get("catalog_product_id") or ""), 0)
                row["sold_quantity"] = propia_ventas + catalog_ventas
                row["tipo"] = "Prop Comb"
                filtrados.append(row)
        stock_val = getattr(filtro_stock, "value", "con_stock")
        if stock_val == "con_stock":
            filtrados = [x for x in filtrados if (x.get("available_quantity") or 0) > 0]
        elif stock_val == "sin_stock":
            filtrados = [x for x in filtrados if (x.get("available_quantity") or 0) == 0]
        awei_val = getattr(filtro_awei, "value", "no_incluye")
        if awei_val == "no_incluye":
            filtrados = [x for x in filtrados if "awei" not in (x.get("marca") or "").lower()]
        period_val = getattr(filtro_periodo, "value", "historica")
        if period_val and period_val != "historica":
            hoy = datetime.now().date()
            dias_map = {"1_mes": 30, "3_meses": 90, "6_meses": 180, "1_anio": 365}
            dias = dias_map.get(period_val, 0)
            if dias > 0:
                desde = hoy - timedelta(days=dias)
                filtrados = [
                    x for x in filtrados
                    if x.get("fecha_ult_modif") and x["fecha_ult_modif"] >= desde.strftime("%Y-%m-%d")
                ]
        col_sort = sort_col_ref.get("val", "title")
        asc = sort_asc_ref.get("val", True)
        filtrados = sorted(filtrados, key=lambda r: _sort_key_precios(r, col_sort), reverse=not asc)
        current_filtrados.clear()
        current_filtrados.extend(filtrados)

        table_container.clear()
        with table_container:
            # Tabla custom (sin ui.table) para evitar error __call__ del slot; precio clickeable. Sin scroll interno (usa scroll de la página).
            with ui.element("div").classes("w-full"):
                with ui.element("table").classes("w-full border-collapse text-sm"):
                    with ui.element("thead"):
                        with ui.element("tr").classes("bg-primary text-white font-semibold"):
                            for col in columns_precios:
                                align = "text-right" if col.get("align") == "right" else "text-left"
                                col_name = col.get("name", col.get("field", ""))
                                sortable = col.get("sortable", True)
                                with ui.element("th").classes(f"px-2 py-2 border {align}"):
                                    if sortable:
                                        ui.button(col["label"], on_click=lambda c=col_name: _on_sort_click(c)).props("flat dense no-caps").classes("text-white hover:bg-white/20 cursor-pointer font-semibold")
                                    else:
                                        ui.label(col["label"])
                    with ui.element("tbody"):
                        for row in filtrados:
                            with ui.element("tr").classes("border-t border-gray-200 hover:bg-gray-50"):
                                for col in columns_precios:
                                    field = col.get("field", col["name"])
                                    val = row.get(field)
                                    if val is None:
                                        val = row.get(col["name"])
                                    align = "text-right" if col.get("align") == "right" else "text-center" if col.get("align") == "center" else "text-left"
                                    with ui.element("td").classes(f"px-2 py-1 border-b border-gray-100 {align} text-sm"):
                                        if col["name"] == "price" and row.get("tipo") in ("Propia", "Prop Comb"):
                                            precio_str = fmt_moneda(val) if val is not None else "$0"
                                            ui.button(precio_str, on_click=lambda r=row: abrir_editar_precio(r)).props("flat dense no-caps").classes("cursor-pointer font-medium text-primary hover:underline")
                                        elif col["name"] == "price":
                                            ui.label(fmt_moneda(val) if val is not None else "$0")
                                        elif col["name"] == "sale_price_fmt":
                                            ui.label(str(val) if val is not None else "-")
                                        elif col["name"] in ("available_quantity", "sold_quantity"):
                                            ui.label(fmt_miles(val) if val is not None else "0")
                                        elif col["name"] == "subtotal":
                                            ui.label(fmt_moneda(val) if val is not None else "$0")
                                        elif col["name"] == "status":
                                            s = str(val or "").lower()
                                            ui.label("Activa" if s == "active" else "Suspendida")
                                        else:
                                            ui.label(str(val) if val is not None else "—")
            current_table.clear()

    with result_area:
        with ui.card().classes("w-full mb-4 p-4 bg-grey-2"):
            with ui.row().classes("w-full justify-around flex-wrap gap-4"):
                with ui.column().classes("items-center"):
                    ui.label("Publicaciones Totales").classes("text-sm text-gray-600")
                    ui.label(str(publicaciones_totales)).classes("text-2xl font-bold text-primary")
                with ui.column().classes("items-center"):
                    ui.label("Publicaciones con stock").classes("text-sm text-gray-600")
                    ui.label(str(publicaciones_con_stock)).classes("text-2xl font-bold text-primary")
                with ui.column().classes("items-center"):
                    ui.label("Publicaciones propias con stock").classes("text-sm text-gray-600")
                    ui.label(str(publicaciones_propias_con_stock)).classes("text-2xl font-bold text-primary")
                with ui.column().classes("items-center"):
                    ui.label("Publicaciones catalogo con stock").classes("text-sm text-gray-600")
                    ui.label(str(publicaciones_catalogo_con_stock)).classes("text-2xl font-bold text-primary")
                with ui.column().classes("items-center"):
                    ui.label("Unidades propias en stock").classes("text-sm text-gray-600")
                    ui.label(fmt_miles(unidades_propias_en_stock)).classes("text-2xl font-bold text-primary")
                with ui.column().classes("items-center"):
                    ui.label("Total en $ (solo propias)").classes("text-sm text-gray-600")
                    ui.label(fmt_moneda(total_pesos_propias)).classes("text-2xl font-bold text-primary")
                with ui.column().classes("items-center"):
                    ui.label("Total en u$ (solo propias)").classes("text-sm text-gray-600")
                    total_usd_label = (
                        f"u$s {fmt_miles(int(round(total_dolares_propias)))}" if total_dolares_propias is not None
                        else "—"
                    )
                    ui.label(total_usd_label).classes("text-2xl font-bold text-primary")
        with ui.row().classes("items-center gap-4 mb-3 flex-wrap"):
            ui.label("Filtros:").classes("text-sm")
            filtro_stock = ui.select(
                {"con_stock": "Con stock", "todas": "Todas", "sin_stock": "Sin stock"},
                value=filtro_stock_ref.get("val", "con_stock") if filtro_stock_ref else "con_stock",
                label="Stock",
            ).classes("w-36")
            filtro_tipo = ui.select(
                {"ambas": "Ambas", "propias": "Propias", "catalogo": "Catalogo", "combinadas": "Combinadas"},
                value="combinadas",
                label="Tipo",
            ).classes("w-36")
            filtro_awei = ui.select(
                {"incluye": "Incluye", "no_incluye": "No incluye"},
                value="no_incluye",
                label="Awei",
            ).classes("w-36")
            filtro_periodo = ui.select(
                {"historica": "Histórica", "1_mes": "1 mes", "3_meses": "3 meses", "6_meses": "6 meses", "1_anio": "1 año"},
                value="historica",
                label="Última modificación",
            ).classes("w-36")
            ui.button("Imprimir stock", on_click=lambda: imprimir_tabla(include_ventas=False), color="primary").props("icon=print")
            ui.button("Imprimir ventas", on_click=lambda: imprimir_tabla(include_ventas=True), color="primary").props("icon=print")
        table_container = ui.column().classes("w-full")

    def on_filtro_stock_change(*args):
        e = args[0] if args else None
        val = getattr(e, "value", "con_stock") if e else "con_stock"
        if filtro_stock_ref:
            filtro_stock_ref["val"] = val
        if val in ("sin_stock", "todas") and include_paused_ref and not include_paused_ref.get("val"):
            include_paused_ref["val"] = True
            if on_actualizar:
                on_actualizar()
            return
        filtrar_y_pintar()

    filtro_stock.on_value_change(on_filtro_stock_change)
    filtro_tipo.on_value_change(lambda *a: filtrar_y_pintar())
    filtro_awei.on_value_change(lambda *a: filtrar_y_pintar())
    filtro_periodo.on_value_change(lambda *a: filtrar_y_pintar())
    filtrar_y_pintar()


def build_tab_precios_detalle(container) -> None:
    """Pestaña Precios: tabla con id, marca, producto, stock, precio, iva, costo, comision, cobrado, iibb, margen $, margen costo, margen venta."""
    container.clear()
    user = require_login()
    if not user:
        return

    uid = user["id"]
    access_token = get_ml_access_token(uid)
    if not access_token:
        with container:
            ui.label("⚠️ No tienes MercadoLibre vinculado. Ve a Configuración y conecta tu cuenta.").classes("text-warning mb-4")
        return

    def _parse_float(s: Any) -> float:
        if s is None or s == "":
            return 0.0
        try:
            raw = str(s).replace(".", "").replace(",", ".").strip()
            return float(raw) if raw else 0.0
        except (ValueError, TypeError):
            return 0.0

    def _parse_rate(s: Any) -> float:
        """Parsea tasas 0.15, 0,15 o 15 (como %). Valores entre 0 y 1.5 se usan tal cual; si > 1.5 se divide por 100."""
        if s is None or s == "":
            return 0.0
        try:
            raw = str(s).strip().replace(",", ".")
            v = float(raw) if raw else 0.0
            return v if v <= 1.5 else v / 100.0
        except (ValueError, TypeError):
            return 0.0

    ml_comision = _parse_rate(get_cotizador_param("ml_comision", uid) or COTIZADOR_DEFAULTS.get("ml_comision", "0.15"))
    cuotas_3x = _parse_rate(get_cotizador_param("cuotas_3x", uid) or COTIZADOR_DEFAULTS.get("cuotas_3x", "0.094"))
    cuotas_6x = _parse_rate(get_cotizador_param("cuotas_6x", uid) or COTIZADOR_DEFAULTS.get("cuotas_6x", "0.151"))
    cuotas_9x = _parse_rate(get_cotizador_param("cuotas_9x", uid) or COTIZADOR_DEFAULTS.get("cuotas_9x", "0.207"))
    cuotas_12x = _parse_rate(get_cotizador_param("cuotas_12x", uid) or COTIZADOR_DEFAULTS.get("cuotas_12x", "0.259"))
    ml_iibb_per = _parse_rate(get_cotizador_param("ml_iibb_per", uid) or COTIZADOR_DEFAULTS.get("ml_iibb_per", "0.055"))
    ml_debcre = _parse_rate(get_cotizador_param("ml_debcre", uid) or COTIZADOR_DEFAULTS.get("ml_debcre", "0.006"))
    ml_envios_val = get_cotizador_param("ml_envios", uid) or COTIZADOR_DEFAULTS.get("ml_envios", "5823")
    ml_envios = _parse_float(ml_envios_val) if ml_envios_val and _parse_float(ml_envios_val) > 100 else 5823.0
    ml_envios_grat_val = get_cotizador_param("ml_envios_gratuitos", uid) or COTIZADOR_DEFAULTS.get("ml_envios_gratuitos", "33000")
    ml_envios_gratuitos = _parse_float(ml_envios_grat_val) if ml_envios_grat_val else 33000.0
    dolar_str = get_cotizador_param("dolar_oficial", uid) or COTIZADOR_DEFAULTS.get("dolar_oficial", "1475")
    dolar_oficial = _parse_float(dolar_str) if dolar_str else 1475.0
    if dolar_oficial <= 0:
        dolar_oficial = 1475.0

    IVA_IMPORTACION_APROX = 0.09  # Aprox. IVA ya pagado en importación (sobre costo u$ * dolar)

    def _calc_iva(precio: float, tipo_iva: float, comision: float, costo_usd: float) -> tuple:
        """Devuelve (iva_total, iva_meli, iva_impor)."""
        iva_venta = precio * tipo_iva / (1 + tipo_iva)
        iva_meli = comision * 0.21 / 1.21  # IVA crédito fiscal de comisión ML
        iva_impor = IVA_IMPORTACION_APROX * costo_usd * dolar_oficial
        iva_total = iva_venta - iva_meli - iva_impor
        return (iva_total, iva_meli, iva_impor)

    def _envio_a_restar(precio: float) -> float:
        """Si precio < ml_envios_gratuitos, no se resta envío."""
        return 0.0 if precio < ml_envios_gratuitos else ml_envios

    items_loaded: List[Dict[str, Any]] = []
    filtro_fecha_ref: Dict[str, str] = {"val": "mes_actual"}
    ventas_por_periodo_ref: Dict[str, Dict[str, int]] = {}  # "historico"|"mes_actual"|"mes_anterior" -> {dedupe_key: ventas}
    filtro_stock_ref: Dict[str, str] = {"val": "con_stock"}
    filtro_awei_ref: Dict[str, str] = {"val": "no_incluye"}
    include_paused_ref: Dict[str, bool] = {"val": True}  # Incluir pausadas para traer todos los productos
    vista_modo_ref: Dict[str, str] = {"val": "minimo"}
    sort_col_ref: Dict[str, str] = {"val": "producto"}
    sort_asc_ref: Dict[str, bool] = {"val": True}
    table_container_ref: Dict[str, Any] = {}
    cargar_listo_ref: Dict[str, Any] = {"listo": False, "error": None, "totales": 0, "con_stock": 0}
    seller_id_ref: Dict[str, Any] = {"val": None}
    filtrados_actuales_ref: Dict[str, List[Dict[str, Any]]] = {"rows": []}
    calcular_labels_ref: Dict[str, Any] = {}

    def _pintar_ui_desde_ref():
        """Pinta la UI cuando los datos están listos. Se llama desde el timer en el main thread."""
        if not cargar_listo_ref.get("listo"):
            return
        cargar_listo_ref["listo"] = False
        err = cargar_listo_ref.get("error")
        if err:
            content_column.clear()
            with content_column:
                ui.label(f"❌ Error al cargar: {err}").classes("text-negative")
            timer_ref["t"].active = False
            return
        totales = cargar_listo_ref.get("totales", 0)
        content_column.clear()
        with content_column:
            with ui.card().classes("w-full mb-4 p-4 bg-grey-2"):
                with ui.row().classes("w-full justify-around flex-wrap gap-4"):
                    with ui.column().classes("items-center"):
                        ui.label("Publicaciones sin promociones").classes("text-sm text-gray-600")
                        lbl_sin_promo = ui.label("—").classes("text-2xl font-bold text-primary")
                        calcular_labels_ref["sin_promo"] = lbl_sin_promo
                    with ui.column().classes("items-center"):
                        ui.label("Publicaciones con promociones").classes("text-sm text-gray-600")
                        lbl_con_promo = ui.label("—").classes("text-2xl font-bold text-primary")
                        calcular_labels_ref["con_promo"] = lbl_con_promo
                    with ui.column().classes("items-center"):
                        ui.label("Publicaciones con cuotas").classes("text-sm text-gray-600")
                        lbl_con_cuotas = ui.label("—").classes("text-2xl font-bold text-primary")
                        calcular_labels_ref["con_cuotas"] = lbl_con_cuotas
                    with ui.column().classes("items-center"):
                        ui.label("Unidades vendidas").classes("text-sm text-gray-600")
                        lbl_uds = ui.label("—").classes("text-2xl font-bold text-primary")
                        calcular_labels_ref["unidades"] = lbl_uds
                    with ui.column().classes("items-center"):
                        ui.label("Facturación total").classes("text-sm text-gray-600")
                        lbl_fact = ui.label("—").classes("text-2xl font-bold text-primary")
                        calcular_labels_ref["facturacion"] = lbl_fact
                    with ui.column().classes("items-center"):
                        ui.label("Margen total").classes("text-sm text-gray-600")
                        lbl_margen = ui.label("—").classes("text-2xl font-bold text-primary")
                        calcular_labels_ref["margen"] = lbl_margen
                    with ui.column().classes("items-center"):
                        ui.label("Margen % sobre venta").classes("text-sm text-gray-600")
                        lbl_margen_pct = ui.label("—").classes("text-2xl font-bold text-primary")
                        calcular_labels_ref["margen_pct"] = lbl_margen_pct
                    with ui.column().classes("items-center"):
                        ui.label("Margen estimado (Datos)").classes("text-sm text-gray-600")
                        ganancia_neta = get_cotizador_param("ml_ganancia_neta_venta", uid) or COTIZADOR_DEFAULTS.get("ml_ganancia_neta_venta", "0.1000")
                        ganancia_pct = float(str(ganancia_neta).replace(",", ".").strip()) * 100
                        lbl_margen_est = ui.label(f"{ganancia_pct:.2f}%".replace(".", ",")).classes("text-2xl font-bold text-primary")
            with ui.row().classes("items-center gap-4 mb-3 flex-wrap w-full justify-between"):
                with ui.row().classes("items-center gap-4 flex-wrap"):
                    ui.label("Filtros:").classes("text-sm")
                    filtro_fecha = ui.select(
                        {"mes_actual": "Mes actual", "mes_anterior": "Mes anterior"},
                        value=filtro_fecha_ref.get("val", "mes_actual"),
                        label="Fecha",
                    ).classes("w-36")
                    filtro_stock = ui.select(
                        {"con_stock": "Con stock", "todas": "Todas", "sin_stock": "Sin stock"},
                        value=filtro_stock_ref.get("val", "con_stock"),
                        label="Stock",
                    ).classes("w-36")
                    filtro_awei = ui.select(
                        {"incluye": "Incluye", "no_incluye": "No incluye"},
                        value=filtro_awei_ref.get("val", "no_incluye"),
                        label="Awei",
                    ).classes("w-36")
                    btn_vista = ui.button("Completo" if vista_modo_ref.get("val") == "minimo" else "Mínimo", color="primary").props("icon=visibility")
                    btn_calcular = ui.button("Calcular", color="secondary").props("icon=calculate")
                ui.space()
                ui.button("QUIEBRE STOCK", on_click=lambda: _quiebre_stock_click(), color="primary").classes("uppercase").props("icon=print")

                def _quiebre_stock_click() -> None:
                    client = context.client
                    container = content_column
                    background_tasks.create(_quiebre_stock_async(client, container), name="quiebre_stock")

                async def _quiebre_stock_async(client, container) -> None:
                    """Genera Excel con productos vendidos en los últimos 60 días que no tienen stock."""
                    try:
                        with container:
                            ui.notify("Generando Quiebre Stock...", color="info")
                        profile = await run.io_bound(ml_get_user_profile, access_token)
                        seller_id = (profile or {}).get("id") or await run.io_bound(ml_get_user_id, access_token)
                        nickname = (profile or {}).get("nickname") or "Usuario"
                        safe_nick = "".join(c for c in str(nickname) if c.isalnum() or c in "_-").strip() or "Usuario"
                        if not seller_id:
                            with container:
                                ui.notify("No se pudo obtener el perfil del vendedor.", color="negative")
                            return
                        hoy = datetime.now().date()
                        hace_60 = hoy - timedelta(days=60)
                        date_from = hace_60.strftime("%Y-%m-%dT00:00:00.000-03:00")
                        date_to = hoy.strftime("%Y-%m-%dT23:59:59.999-03:00")
                        ord_res = await run.io_bound(
                            ml_get_orders, access_token, str(seller_id), limit=2000, offset=0,
                            date_from=date_from, date_to=date_to,
                        )
                        raw = ord_res.get("results") or ord_res.get("orders") or ord_res.get("elements") or []
                        orders_merged = list({str(o.get("id")): o for o in raw if isinstance(o, dict) and o.get("id")}.values())
                        ventas_quiebre: List[Dict[str, Any]] = []
                        item_ids_set: set = set()
                        for ord_item in orders_merged:
                            dt_str = ord_item.get("date_created") or ord_item.get("date_closed") or ""
                            if dt_str:
                                try:
                                    dt = datetime.strptime(dt_str[:10], "%Y-%m-%d").date()
                                    if dt < hace_60:
                                        continue
                                except Exception:
                                    pass
                            status_raw = (ord_item.get("status") or "").strip().lower()
                            if "cancel" in status_raw:
                                continue
                            for it in ord_item.get("order_items") or ord_item.get("items") or []:
                                if not isinstance(it, dict):
                                    continue
                                obj = it.get("item") or it
                                qty = int(it.get("quantity") or it.get("qty") or 0)
                                if qty == 0:
                                    continue
                                titulo = (obj.get("title") if isinstance(obj, dict) else str(obj)) or it.get("title") or "—"
                                item_id = (str(obj.get("id") or it.get("item_id") or "") if isinstance(obj, dict) else str(it.get("item_id") or "")).strip()
                                if not item_id:
                                    continue
                                ventas_quiebre.append({"productos": titulo[:200], "cantidad": qty, "item_id": item_id})
                                item_ids_set.add(item_id)
                        if not ventas_quiebre:
                            with container:
                                ui.notify("No hay ventas en los últimos 60 días.", color="warning")
                            return
                        item_ids_list = list(item_ids_set)
                        item_id_to_info: Dict[str, Dict[str, Any]] = {}
                        for i in range(0, len(item_ids_list), 20):
                            batch = item_ids_list[i : i + 20]
                            bodies = await run.io_bound(ml_get_items_multiget_with_attributes, access_token, batch, "id,title,available_quantity,catalog_product_id,attributes")
                            for b in (bodies or []):
                                if b and isinstance(b, dict):
                                    iid = str(b.get("id") or "").strip()
                                    if iid:
                                        marca, color = "", ""
                                        for att in b.get("attributes") or []:
                                            aid = (str(att.get("id") or "")).strip().upper()
                                            if aid in ("BRAND", "MARCA"):
                                                val = att.get("value_name") or att.get("value_id")
                                                marca = str(val) if val is not None else ""
                                            elif aid in ("COLOR", "COLOUR"):
                                                val = att.get("value_name") or att.get("value_id")
                                                color = str(val) if val is not None else ""
                                        catalog_id = str(b.get("catalog_product_id") or "").strip()
                                        item_id_to_info[iid] = {"stock": int(b.get("available_quantity") or 0), "marca": marca or "—", "color": color or "—", "catalog_product_id": catalog_id, "title": (b.get("title") or "")[:200]}
                        ids_sin_color = [iid for iid in item_ids_list if (item_id_to_info.get(iid) or {}).get("color") == "—"]
                        item_id_to_color_desc: Dict[str, str] = {}
                        for iid in ids_sin_color[:25]:
                            desc = await run.io_bound(ml_get_item_description, access_token, iid)
                            c = _extraer_color_desde_texto(desc)
                            if c:
                                item_id_to_color_desc[iid] = c
                        agg: Dict[tuple, int] = defaultdict(int)
                        prod_titulos: Dict[tuple, str] = {}
                        for v in ventas_quiebre:
                            iid = v.get("item_id", "")
                            info = item_id_to_info.get(iid) or item_id_to_info.get(iid.upper()) or item_id_to_info.get(iid.lower()) if iid else None
                            stock = info["stock"] if info else -1
                            marca = info["marca"] if info else "—"
                            color = (info["color"] if info else "—") or item_id_to_color_desc.get(iid) or item_id_to_color_desc.get(iid.upper()) or item_id_to_color_desc.get(iid.lower()) or "—"
                            if color == "—":
                                color = _extraer_color_desde_texto(v["productos"]) or "—"
                            if stock == 0:
                                catalog_id = (info or {}).get("catalog_product_id", "")
                                key = (catalog_id or v["productos"], marca, color)
                                agg[key] += v["cantidad"]
                                titulo_rep = (info or {}).get("title") or v["productos"]
                                if key not in prod_titulos or len(titulo_rep) > len(prod_titulos.get(key, "")):
                                    prod_titulos[key] = titulo_rep
                        if not agg:
                            with container:
                                ui.notify("Todos los productos vendidos tienen stock. No hay quiebre.", color="info")
                            return
                        filas = sorted(agg.items(), key=lambda x: (str(prod_titulos.get(x[0], x[0][0])).upper(), -x[1]))
                        ahora = datetime.now()
                        sheet_name = f"Quiebre stock {ahora.day:02d}-{ahora.month:02d}-{ahora.year % 100:02d}"
                        wb = Workbook()
                        ws = wb.active
                        ws.title = sheet_name[:31]
                        ws.column_dimensions["A"].width = 120
                        ws.column_dimensions["B"].width = 15
                        ws.column_dimensions["C"].width = 15
                        ws.column_dimensions["D"].width = 15
                        black_fill = PatternFill(start_color="000000", end_color="000000", fill_type="solid")
                        white_fill = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid")
                        header_font = Font(color="FFFFFF", bold=True)
                        thin_side = Side(border_style="thin")
                        all_borders = Border(left=thin_side, right=thin_side, top=thin_side, bottom=thin_side)
                        header_align = Alignment(horizontal="center", vertical="center")
                        center_align = Alignment(horizontal="center", vertical="center")
                        h1 = f"{str(nickname).upper()} - PRODUCTOS SIN STOCK"
                        for col, h in enumerate((h1, "MARCA", "COLOR", "VENTAS 60 DIAS"), start=1):
                            c = ws.cell(row=1, column=col, value=h)
                            c.fill = black_fill
                            c.font = header_font
                            c.border = all_borders
                            c.alignment = header_align
                        for idx, (key, ventas) in enumerate(filas, start=2):
                            prod = prod_titulos.get(key, key[0])
                            marca, color = key[1], key[2]
                            for col, val in enumerate((prod, marca, color, ventas), start=1):
                                cell = ws.cell(row=idx, column=col, value=val)
                                cell.fill = white_fill
                                cell.border = all_borders
                                if col == 4:
                                    cell.alignment = center_align
                        ahora = datetime.now()
                        yy = ahora.year % 100
                        nombre_archivo = f"Compra_{safe_nick}_{yy:02d}_{ahora.month:02d}_{ahora.day:02d}.xlsx"
                        fd, path = tempfile.mkstemp(suffix=".xlsx")
                        try:
                            os.close(fd)
                            wb.save(path)
                            with container:
                                ui.download(path, nombre_archivo)

                                def _cleanup() -> None:
                                    try:
                                        if path and os.path.exists(path):
                                            os.unlink(path)
                                    except Exception:
                                        pass

                                ui.timer(5.0, _cleanup, once=True)
                                ui.notify(f"Descargado: {nombre_archivo}", color="positive")
                        except Exception as e:
                            with container:
                                ui.notify(f"Error al guardar Excel: {e}", color="negative")
                    except Exception as e:
                        with container:
                            ui.notify(f"Error Quiebre Stock: {e}", color="negative")

                def calcular_totales() -> None:
                    filas = filtrados_actuales_ref.get("rows") or []
                    uds = sum(int(r.get("ventas") or 0) for r in filas)
                    facturacion = sum(float(r.get("precio") or 0) * int(r.get("ventas") or 0) for r in filas)
                    margen_total = sum(float(r.get("margen_pesos") or 0) * int(r.get("ventas") or 0) for r in filas)
                    margen_pct = (margen_total / facturacion * 100) if facturacion > 0 else 0.0
                    sin_promo = sum(1 for r in filas if r.get("price_original") is None)
                    con_promo = sum(1 for r in filas if r.get("price_original") is not None)
                    cuotas_val = lambda c: str(c or "x1").strip().lower()
                    con_cuotas = sum(1 for r in filas if cuotas_val(r.get("cuotas")) not in ("x1", "1", ""))
                    for k, lbl in calcular_labels_ref.items():
                        if not lbl:
                            continue
                        if k == "sin_promo":
                            lbl.text = str(sin_promo)
                        elif k == "con_promo":
                            lbl.text = str(con_promo)
                        elif k == "con_cuotas":
                            lbl.text = str(con_cuotas)
                        elif k == "unidades":
                            lbl.text = str(uds)
                        elif k == "facturacion":
                            lbl.text = fmt_moneda(facturacion)
                        elif k == "margen":
                            lbl.text = fmt_moneda(margen_total)
                        elif k == "margen_pct":
                            lbl.text = f"{margen_pct:.2f}%"

                calcular_labels_ref["_calcular_fn"] = calcular_totales
                btn_calcular.on_click(calcular_totales)
            # Wrapper con overlay de carga (permanece visible durante filtrar_y_pintar)
            with ui.column().classes("relative w-full").style("min-height: 200px;") as wrapper:
                overlay = ui.element("div").classes("absolute inset-0 bg-white/90 flex items-start justify-center pt-12 z-10 gap-3").style("min-height: 150px;")
                with overlay:
                    ui.spinner(size="lg")
                    overlay_label = ui.label("Actualizando filtros...").classes("text-gray-600 text-lg")
                overlay.set_visibility(False)
                table_container_ref["container"] = ui.column().classes("w-full")
                table_container_ref["overlay"] = overlay
                table_container_ref["overlay_label"] = overlay_label

            def _aplicar_calcular() -> None:
                fn = calcular_labels_ref.get("_calcular_fn")
                if fn:
                    fn()

            def _filtrar_con_indicador(msg: str = "Actualizando filtros...") -> None:
                """Muestra overlay con spinner, ejecuta filtrar_y_pintar y oculta overlay al terminar."""
                ov = table_container_ref.get("overlay")
                lbl = table_container_ref.get("overlay_label")
                if lbl:
                    lbl.text = msg
                if ov:
                    ov.set_visibility(True)

                def _pintar_despues() -> None:
                    filtrar_y_pintar(ov=ov)
                ui.timer(0.15, _pintar_despues, once=True)
            table_container_ref["_filtrar_fn"] = _filtrar_con_indicador

            def on_stock_change(e):
                val = getattr(e, "value", "con_stock")
                filtro_stock_ref["val"] = val
                if val in ("sin_stock", "todas"):
                    include_paused_ref["val"] = True
                    ov = table_container_ref.get("overlay")
                    lbl = table_container_ref.get("overlay_label")
                    if lbl:
                        lbl.text = "Cargando (incluye sin stock)..."
                    if ov:
                        ov.set_visibility(True)
                    client = context.client
                    background_tasks.create(_cargar(client), name="cargar_precios_detalle")
                else:
                    _filtrar_con_indicador()

            def on_awei_change(e):
                filtro_awei_ref["val"] = getattr(e, "value", "no_incluye")
                _filtrar_con_indicador()

            def toggle_vista():
                vista_modo_ref["val"] = "completo" if vista_modo_ref.get("val") == "minimo" else "minimo"
                btn_vista.text = "Completo" if vista_modo_ref["val"] == "minimo" else "Mínimo"
                _filtrar_con_indicador()

            def on_fecha_change(e):
                val = getattr(e, "value", "mes_actual")
                filtro_fecha_ref["val"] = val if val in ("mes_actual", "mes_anterior") else "mes_actual"
                periodo = filtro_fecha_ref["val"]
                ventas_dict = ventas_por_periodo_ref.get(periodo, {})
                for row in items_loaded:
                    grupo_ids = row.get("grupo_ids") or [str(row.get("id", ""))]
                    row["ventas"] = sum(ventas_dict.get("id:" + vid, 0) for vid in grupo_ids if vid)
                _filtrar_con_indicador()

            filtro_fecha.on_value_change(on_fecha_change)
            filtro_stock.on_value_change(on_stock_change)
            filtro_awei.on_value_change(on_awei_change)
            btn_vista.on_click(toggle_vista)

        if not items_loaded:
            content_column.clear()
            with content_column:
                ui.label("No hay publicaciones en MercadoLibre.").classes("text-gray-500")
        else:
            filtrar_y_pintar()  # Ya incluye actualizar totales al terminar
        timer_ref["t"].active = False

    timer_ref: Dict[str, Any] = {}
    with container:
        content_column = ui.column().classes("w-full gap-2")
        with content_column:
            with ui.card().classes("w-full p-8 items-center gap-4"):
                ui.spinner(size="xl")
                ui.label("Cargando productos y ventas...").classes("text-xl text-gray-700")
        timer_ref["t"] = ui.timer(0.3, _pintar_ui_desde_ref)

    def fmt_moneda(val: Any) -> str:
        if val is None:
            return "$0"
        try:
            n = int(round(float(val)))
            return "$" + f"{n:,}".replace(",", ".")
        except (TypeError, ValueError):
            return "$0"

    def fmt_usd(val: Any) -> str:
        """Formato para costo u$: u$ adelante, 2 decimales, punto para miles."""
        if val is None:
            return "u$0,00"
        try:
            n = float(val)
            s = f"{n:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            return "u$" + s
        except (TypeError, ValueError):
            return "u$0,00"

    def _parse_moneda(s: Any) -> float:
        """Parsea string como $1.234.567 -> float."""
        if s is None or s == "":
            return 0.0
        try:
            raw = str(s).replace("$", "").replace(".", "").replace(",", ".").strip()
            return float(raw) if raw else 0.0
        except (ValueError, TypeError):
            return 0.0

    def _parse_usd(s: Any) -> float:
        """Parsea string como u$1.234,56 o u$12.5 o u$12,5 -> float. Acepta . o , como decimal."""
        if s is None or s == "":
            return 0.0
        try:
            raw = str(s).replace("u$", "").replace("$", "").replace(",", ".").strip()
            # Si hay varios puntos, el último es decimal (1.234.56 -> 1234.56)
            if "." in raw:
                p = raw.split(".")
                raw = "".join(p[:-1]) + "." + p[-1]
            return float(raw) if raw else 0.0
        except (ValueError, TypeError):
            return 0.0

    def fmt_pct(val: Any) -> str:
        if val is None:
            return "—"
        try:
            n = float(val)
            return f"{n:.1f}%"
        except (TypeError, ValueError):
            return "—"

    def fmt_pct2(val: Any) -> str:
        """Porcentaje con 2 decimales (para margen costo y margen venta)."""
        if val is None:
            return "—"
        try:
            n = float(val)
            return f"{n:.2f}%"
        except (TypeError, ValueError):
            return "—"

    def _sort_key(row: Dict[str, Any], col: str) -> Any:
        if col in ("precio", "stock", "ventas", "iva_total", "iva_meli", "iva_impor", "costo", "comision", "cobrado", "iibb", "deb_cred", "envio", "margen_pesos", "margen_costo_pct", "margen_venta_pct", "tipo_iva"):
            return float(row.get(col) or 0)
        return str(row.get(col) or "").lower()

    COLUMNAS_COMPLETO = [
        ("id", "ID", "left", True),
        ("marca", "Marca", "left", True),
        ("producto", "Producto", "left", True),
        ("stock", "Stock", "center", True),
        ("ventas", "Ventas", "center", True),
        ("tipo_publicacion", "Tipo pub.", "left", True),
        ("cuotas", "Cuotas", "center", True),
        ("costo", "Costo u$ +IVA", "right", True),
        ("precio", "Precio", "right", True),
        ("tipo_iva", "Tipo IVA", "right", True),
        ("comision", "Comisión", "right", True),
        ("cobrado", "Cobrado", "right", True),
        ("iva_meli", "IVA Meli", "center", True),
        ("iva_impor", "IVA impor", "center", True),
        ("iva_total", "IVA total", "center", True),
        ("deb_cred", "Deb-Cred", "right", True),
        ("iibb", "IIBB", "right", True),
        ("envio", "Envío", "right", True),
        ("margen_pesos", "Gan $", "center", True),
        ("margen_venta_pct", "Gan Vta %", "center", True),
        ("margen_costo_pct", "Gan % Cos", "center", True),
    ]
    COLUMNAS_MINIMO = [
        ("id", "ID", "left", True),
        ("marca", "Marca", "left", True),
        ("producto", "Producto", "left", True),
        ("stock", "Stock", "center", True),
        ("ventas", "Ventas", "center", True),
        ("costo", "Costo u$ +IVA", "right", True),
        ("precio", "Precio", "right", True),
        ("margen_pesos", "Gan $", "center", True),
        ("margen_venta_pct", "Gan Vta %", "center", True),
        ("margen_costo_pct", "Gan % Cos", "center", True),
    ]

    def _on_row_click(row_base: Dict[str, Any]) -> None:
        """Fetch async sale_price+promo al clic (como antes). Si falla, usa datos pre-cargados del row."""
        client = context.client

        async def _fetch_and_show() -> None:
            item_id = str(row_base.get("id", "")).strip()
            row = dict(row_base)
            if not item_id or not access_token:
                with client:
                    show_row_dialog_impl(row)
                return
            try:
                with client:
                    ui.notify("Cargando detalles...", color="info", timeout=1)
                sp_data = await run.io_bound(ml_get_item_sale_price_full, access_token, item_id)
                bodies = await run.io_bound(ml_get_items_multiget_with_attributes, access_token, [item_id], "id,listing_type_id,attributes,sale_terms")
                cuotas_val = str(_cuotas_desde_item(bodies[0]) if bodies and bodies[0] else row.get("cuotas") or "x1").strip().lower()
                row["cuotas"] = cuotas_val
                guardado = get_precios_producto(item_id, uid)
                costo = float(guardado["costo_u"]) if guardado else 0.0
                tipo_iva = float(guardado["tipo_iva"]) if guardado else 0.105
                row["costo"] = costo
                row["tipo_iva"] = tipo_iva
                tiene_promo = False
                promo_ml_pct = row_base.get("promo_ml_pct")
                promo_yo_pct = row_base.get("promo_yo_pct")
                if sp_data and sp_data.get("amount") is not None:
                    amt_f = float(sp_data["amount"])
                    row["precio"] = amt_f
                    reg = sp_data.get("regular_amount")
                    if reg is not None and reg > 0 and abs(float(reg) - amt_f) > 0.01:
                        reg_f = float(reg)
                        tiene_promo = True
                        row["price_original"] = reg_f
                        row["promo_pct"] = ((reg_f - amt_f) / reg_f * 100)
                        total_pct = ((reg_f - amt_f) / reg_f * 100)
                        sid = seller_id_ref.get("val")
                        if not sid and access_token:
                            try:
                                profile = await run.io_bound(ml_get_user_profile, access_token)
                                sid = str((profile or {}).get("id") or "")
                                if sid:
                                    seller_id_ref["val"] = sid
                            except Exception:
                                pass
                        if sid:
                            def _fd():
                                cid = sp_data.get("campaign_id")
                                pid = sp_data.get("promotion_id")
                                pt = (sp_data.get("promotion_type") or "").strip().upper()
                                d = None
                                if cid:
                                    d = ml_get_promotion_item_discounts_by_campaign(
                                        access_token, str(cid), item_id, total_pct, sid, promotion_type_hint=pt
                                    )
                                if d is None and pid and pt and not (str(pid or "").upper().startswith("OFFER-")):
                                    d = ml_get_promotion_item_discounts(access_token, str(pid), pt, item_id, total_pct)
                                if d is None:
                                    d = ml_get_promotion_item_discounts_by_user(access_token, item_id, sid, total_pct)
                                return d
                            discounts = await run.io_bound(_fd)
                            if discounts:
                                promo_ml_pct = discounts.get("meli_pct", 0)
                                promo_yo_pct = discounts.get("seller_pct", 0)
                            elif promo_ml_pct is None or promo_yo_pct is None:
                                promo_ml_pct = 0.0
                                promo_yo_pct = total_pct
                        elif promo_ml_pct is None or promo_yo_pct is None:
                            promo_ml_pct = 0.0
                            promo_yo_pct = total_pct
                        row["promo_ml_pct"] = promo_ml_pct if promo_ml_pct is not None else 0.0
                        row["promo_yo_pct"] = promo_yo_pct if promo_yo_pct is not None else row["promo_pct"]
                        row["price_promo"] = reg_f * (1 - (row["promo_yo_pct"] or 0) / 100)
                else:
                    if row_base.get("price_original") is not None and (row_base.get("promo_yo_pct") is not None or row_base.get("promo_pct") is not None):
                        row["price_original"] = row_base.get("price_original")
                        row["promo_ml_pct"] = row_base.get("promo_ml_pct") if row_base.get("promo_ml_pct") is not None else 0.0
                        row["promo_yo_pct"] = row_base.get("promo_yo_pct") if row_base.get("promo_yo_pct") is not None else row_base.get("promo_pct", 0)
                        row["price_promo"] = row_base.get("price_promo")
                        tiene_promo = True
                precio_real = float(row.get("precio") or 0)
                precio_calc = row.get("price_promo") if tiene_promo and row.get("price_promo") else precio_real
                row["comision"] = precio_calc * ml_comision
                row["cobrado"] = precio_calc - row["comision"]
                iva_total, iva_meli, iva_impor = _calc_iva(precio_calc, tipo_iva, row["comision"], costo)
                row["iva_total"] = iva_total
                row["iva_meli"] = iva_meli
                row["iva_impor"] = iva_impor
                row["deb_cred"] = precio_calc * ml_debcre
                row["iibb"] = precio_calc * ml_iibb_per
                row["envio"] = _envio_a_restar(precio_calc)
                tasa_cuotas = {"x3": cuotas_3x, "x6": cuotas_6x, "x9": cuotas_9x, "x12": cuotas_12x}.get(cuotas_val, 0.0)
                row["costo_cuotas"] = precio_calc * tasa_cuotas if tasa_cuotas else 0.0
                costo_pesos = costo * dolar_oficial
                if costo_pesos <= 0:
                    row["margen_pesos"], row["margen_costo_pct"], row["margen_venta_pct"] = 0.0, 0.0, 0.0
                else:
                    row["margen_pesos"] = row["cobrado"] - costo_pesos - iva_total - row["iibb"] - row["deb_cred"] - row["envio"] - row["costo_cuotas"]
                    row["margen_costo_pct"] = (row["margen_pesos"] / costo_pesos * 100) if costo_pesos > 0 else 0.0
                    row["margen_venta_pct"] = (row["margen_pesos"] / precio_calc * 100) if precio_calc > 0 else 0.0
            except Exception:
                pass
            with client:
                show_row_dialog_impl(row)

        background_tasks.create(_fetch_and_show(), name="fetch_row_details")

    def show_row_dialog_impl(row: Dict[str, Any]) -> None:
        d = ui.dialog()
        inp_refs: Dict[str, Any] = {}
        recalc_container_ref: Dict[str, Any] = {}

        def _recalcular() -> None:
            precio_str = inp_refs.get("precio") and getattr(inp_refs["precio"], "value", None) or ""
            costo_str = inp_refs.get("costo") and getattr(inp_refs["costo"], "value", None) or ""
            tipo_iva_str = inp_refs.get("tipo_iva") and getattr(inp_refs["tipo_iva"], "value", None) or "0.105"
            precio = _parse_moneda(precio_str)
            costo = _parse_usd(costo_str)
            tipo_iva = float(tipo_iva_str) if tipo_iva_str else 0.105
            if precio < 1:
                precio = float(row.get("precio") or 0) or 1
            tiene_promo = row.get("price_original") is not None and row.get("promo_yo_pct") is not None
            precio_calc = precio
            if tiene_promo:
                price_orig = float(row.get("price_original") or 0)
                promo_yo = float(row.get("promo_yo_pct") or 0)
                precio_calc = price_orig * (1 - promo_yo / 100)
            comision = precio_calc * ml_comision
            cobrado = precio_calc - comision
            deb_cred = precio_calc * ml_debcre
            iibb = precio_calc * ml_iibb_per
            iva_venta = precio_calc * tipo_iva / (1 + tipo_iva)
            iva_total, iva_meli, iva_impor = _calc_iva(precio_calc, tipo_iva, comision, costo)
            envio = _envio_a_restar(precio_calc)
            costo_pesos = costo * dolar_oficial
            cuotas_val = str(row.get("cuotas") or "x1").strip().lower()
            tasa_cuotas = {"x3": cuotas_3x, "x6": cuotas_6x, "x9": cuotas_9x, "x12": cuotas_12x}.get(cuotas_val, 0.0)
            costo_cuotas = precio_calc * tasa_cuotas if tasa_cuotas else 0.0
            if costo_pesos <= 0:
                margen_pesos, margen_costo_pct, margen_venta_pct = 0.0, 0.0, 0.0
            else:
                margen_pesos = cobrado - costo_pesos - iva_total - iibb - deb_cred - envio - costo_cuotas
                margen_costo_pct = (margen_pesos / costo_pesos * 100) if costo_pesos > 0 else 0.0
                margen_venta_pct = (margen_pesos / precio_calc * 100) if precio_calc > 0 else 0.0
            if recalc_container_ref.get("costo_pesos_label"):
                recalc_container_ref["costo_pesos_label"].text = fmt_moneda(costo_pesos)
            data = {"comision": comision, "cobrado": cobrado, "costo_cuotas": costo_cuotas, "iva_venta": iva_venta, "iva_total": iva_total,
                    "iva_meli": iva_meli, "iva_impor": iva_impor, "deb_cred": deb_cred, "iibb": iibb, "envio": envio,
                    "costo_pesos": costo_pesos, "margen_pesos": margen_pesos,
                    "margen_costo_pct": margen_costo_pct, "margen_venta_pct": margen_venta_pct}
            _pintar_recalc(recalc_container_ref["container"], data)

        def _pintar_recalc(cont, data: Dict[str, Any]) -> None:
            costo_pesos = float(data.get("costo_pesos") or 0)
            mp = float(data.get("margen_pesos") or 0)
            if costo_pesos <= 0:
                margen_cls = "font-bold text-black"
            else:
                margen_cls = "font-bold " + ("text-positive" if mp > 0 else "text-negative")
            cont.clear()
            with cont:
                with ui.row().classes("w-full justify-between py-1 gap-4"):
                    ui.label("Comisión").classes("text-sm font-medium text-gray-600")
                    ui.label(fmt_moneda(data.get("comision"))).classes("text-sm text-negative")
                with ui.row().classes("w-full justify-between py-1 gap-4"):
                    ui.label("Cobrado").classes("text-sm font-medium text-gray-600")
                    ui.label(fmt_moneda(data.get("cobrado"))).classes("text-sm font-bold text-primary")
                with ui.row().classes("w-full justify-between py-1 gap-4"):
                    ui.label("Costo Cuotas").classes("text-sm font-medium text-gray-600")
                    ui.label(fmt_moneda(data.get("costo_cuotas"))).classes("text-sm text-negative")
                with ui.row().classes("w-full justify-between py-1 gap-4"):
                    ui.label("IVA venta").classes("text-sm font-medium text-gray-600")
                    ui.label(fmt_moneda(data.get("iva_venta"))).classes("text-sm")
                with ui.row().classes("w-full justify-between py-1 gap-4"):
                    with ui.row().classes("gap-4"):
                        ui.label("IVA Meli").classes("text-sm font-medium text-gray-600")
                        ui.label(fmt_moneda(data.get("iva_meli"))).classes("text-sm")
                    with ui.row().classes("gap-4"):
                        ui.label("IVA impor").classes("text-sm font-medium text-gray-600")
                        ui.label(fmt_moneda(data.get("iva_impor"))).classes("text-sm")
                with ui.row().classes("w-full justify-between py-1 gap-4"):
                    ui.label("IVA total (iva venta - iva meli - iva impor)").classes("text-sm font-medium text-gray-600")
                    ui.label(fmt_moneda(data.get("iva_total"))).classes("text-sm text-negative")
                with ui.row().classes("w-full justify-between py-1 gap-4"):
                    ui.label("Deb-Cred").classes("text-sm font-medium text-gray-600")
                    ui.label(fmt_moneda(data.get("deb_cred"))).classes("text-sm text-negative")
                with ui.row().classes("w-full justify-between py-1 gap-4"):
                    ui.label("IIBB").classes("text-sm font-medium text-gray-600")
                    ui.label(fmt_moneda(data.get("iibb"))).classes("text-sm text-negative")
                with ui.row().classes("w-full justify-between py-1 gap-4 border-b-2 border-gray-300"):
                    ui.label("Envío").classes("text-sm font-medium text-gray-600")
                    ui.label(fmt_moneda(data.get("envio"))).classes("text-sm text-negative")
                with ui.row().classes("w-full justify-between py-2 gap-4"):
                    ui.label("Gan $").classes("text-sm font-medium text-gray-600")
                    ui.label(fmt_moneda(data.get("margen_pesos"))).classes(margen_cls)
                with ui.row().classes("w-full justify-between py-1 gap-4"):
                    ui.label("Gan Vta %").classes("text-sm font-medium text-gray-600")
                    ui.label(fmt_pct2(data.get("margen_venta_pct"))).classes(margen_cls)
                with ui.row().classes("w-full justify-between py-1 gap-4"):
                    ui.label("Gan % Cos").classes("text-sm font-medium text-gray-600")
                    ui.label(fmt_pct2(data.get("margen_costo_pct"))).classes(margen_cls)

        with d:
            with ui.card().classes("p-6 min-w-[400px] max-w-[560px]"):
                with ui.row().classes("w-full gap-4 mb-4"):
                    thumb_url = row.get("thumbnail") or ""
                    if thumb_url:
                        ui.image(thumb_url).classes("w-24 h-24 object-contain rounded border").style("min-width: 96px; min-height: 96px;")
                    else:
                        with ui.column().classes("w-24 h-24 rounded border bg-gray-100 items-center justify-center").style("min-width: 96px; min-height: 96px;"):
                            ui.label("Sin foto").classes("text-xs text-gray-500")
                    with ui.column().classes("flex-1 min-w-0 gap-2"):
                        ui.label(str(row.get("id", "—"))).classes("text-sm font-mono text-gray-600")
                        ui.label(str(row.get("marca", "—"))).classes("text-sm font-medium")
                        txt = str(row.get("producto", ""))[:120] + ("..." if len(str(row.get("producto", ""))) > 120 else "")
                        ui.label(txt).classes("text-sm font-bold")
                        ui.label(f"Stock: {row.get('stock', '0')}").classes("text-sm text-gray-600")
                with ui.column().classes("w-full gap-0 border-b-2 border-gray-300 pb-3"):
                    with ui.row().classes("w-full justify-between py-2 items-center"):
                        ui.label("Precio").classes("text-sm font-medium text-gray-600")
                        inp_precio = ui.input(value=fmt_moneda(row.get("precio"))).classes("text-sm w-32").props("dense")
                        inp_refs["precio"] = inp_precio
                    with ui.row().classes("w-full justify-between py-2 items-center"):
                        ui.label("Tipo IVA").classes("text-sm font-medium text-gray-600")
                        tipo_val = float(row.get("tipo_iva") or 0.105)
                        sel = ui.select({"0.105": "10,5%", "0.21": "21%"}, value=str(tipo_val)).classes("text-sm w-32").props("dense")
                        inp_refs["tipo_iva"] = sel
                    with ui.row().classes("w-full justify-between py-2 items-center gap-4 border-b-2 border-gray-300"):
                        with ui.row().classes("items-center gap-2"):
                            ui.label("Costo +IVA u$").classes("text-sm font-medium text-gray-600")
                            _costo_val = row.get("costo")
                            _costo_inicial = f"{float(_costo_val):.2f}".replace(".", ",") if _costo_val is not None else "0"
                            _costo_wrap_id = f"costo-wrap-{row.get('id', 'x')}"
                            with ui.element("div").style("display: inline-block").props(f'id={_costo_wrap_id}'):
                                inp_costo = ui.input(value=_costo_inicial).classes("text-sm w-24").props("dense input-class=costo-usd-input")
                            inp_refs["costo"] = inp_costo

                            def _add_costo_filter():
                                _wid = _costo_wrap_id
                                ui.run_javascript(f'''
                                    var wrapper = document.getElementById("{_wid}");
                                    var inp = wrapper ? wrapper.querySelector("input") : null;
                                    if (inp && !inp.dataset.costoFilter) {{
                                        inp.dataset.costoFilter = "1";
                                        inp.addEventListener("input", function() {{
                                            var v = this.value;
                                            var f = v.replace(/[^0-9,.]/g, "");
                                            f = f.replace(/\\./g, ",");
                                            var decimals = (f.match(/,/g) || []);
                                            if (decimals.length > 1) {{
                                                var first = f.indexOf(",");
                                                f = f.substring(0, first+1) + f.substring(first+1).replace(/,/g, "");
                                            }}
                                            if (v !== f) {{ this.value = f; this.dispatchEvent(new Event("input", {{bubbles: true}})); }}
                                        }});
                                        inp.addEventListener("keypress", function(e) {{
                                            var k = e.key;
                                            if ((k === "," || k === ".") && /[,.]/.test(this.value)) {{ e.preventDefault(); return; }}
                                            if (/[0-9,.]/.test(k)) return;
                                            e.preventDefault();
                                        }});
                                    }}
                                ''')

                            ui.timer(0.15, _add_costo_filter, once=True)

                            def _filtrar_costo_usd(e=None):
                                ctrl = inp_refs.get("costo")
                                if not ctrl:
                                    return
                                val = str(getattr(e, "value", None) or ctrl.value or "")
                                filtrado = "".join(c for c in val if c in "0123456789,.")
                                filtrado = filtrado.replace(".", ",")
                                dec_count = filtrado.count(",")
                                if dec_count > 1:
                                    first_dec = filtrado.find(",")
                                    filtrado = filtrado[: first_dec + 1] + filtrado[first_dec + 1 :].replace(",", "")
                                if val != filtrado:
                                    ctrl.value = filtrado

                            inp_costo.on_value_change(lambda e: _filtrar_costo_usd(e))
                        with ui.row().classes("items-center gap-2"):
                            ui.label("Costo $").classes("text-sm font-medium text-gray-600")
                            costo_pesos = (float(row.get("costo") or 0) * dolar_oficial)
                            recalc_container_ref["costo_pesos_label"] = ui.label(fmt_moneda(costo_pesos)).classes("text-sm")
                    with ui.row().classes("w-full py-2 gap-6 border-b-2 border-gray-300 flex-wrap"):
                        with ui.column().classes("gap-0"):
                            ui.label("Cuotas").classes("text-xs text-gray-600")
                            cuotas_val = str(row.get("cuotas") or "x1").strip()
                            ui.label(cuotas_val).classes("text-sm font-medium")
                        with ui.column().classes("gap-0"):
                            ui.label("Promo ML").classes("text-xs text-gray-600")
                            promo_ml = row.get("promo_ml_pct")
                            promo_ml_txt = f"{promo_ml:.1f}%" if promo_ml is not None else "—"
                            ui.label(promo_ml_txt).classes("text-sm font-medium")
                        with ui.column().classes("gap-0"):
                            ui.label("Promo Yo %").classes("text-xs text-gray-600")
                            promo_yo = row.get("promo_yo_pct")
                            promo_yo_txt = f"{promo_yo:.1f}%" if promo_yo is not None else "—"
                            ui.label(promo_yo_txt).classes("text-sm font-medium")
                        with ui.column().classes("gap-0"):
                            ui.label("Promo Yo $").classes("text-xs text-gray-600")
                            promo_yo_pct = row.get("promo_yo_pct")
                            price_orig = row.get("price_original")
                            promo_yo_dolares = (price_orig * (promo_yo_pct or 0) / 100) if price_orig is not None and promo_yo_pct is not None else None
                            promo_yo_dol_txt = fmt_moneda(promo_yo_dolares) if promo_yo_dolares is not None else "—"
                            ui.label(promo_yo_dol_txt).classes("text-sm font-medium")
                        with ui.column().classes("gap-0"):
                            ui.label("Precio Original").classes("text-xs text-gray-600")
                            price_orig = row.get("price_original")
                            price_orig_txt = fmt_moneda(price_orig) if price_orig is not None else "—"
                            ui.label(price_orig_txt).classes("text-sm font-medium")
                        with ui.column().classes("gap-0"):
                            ui.label("Precio Promo").classes("text-xs text-gray-600")
                            price_promo = row.get("price_promo")
                            price_promo_txt = fmt_moneda(price_promo) if price_promo is not None else "—"
                            ui.label(price_promo_txt).classes("text-sm font-medium")
                    recalc_container_ref["container"] = ui.column().classes("w-full gap-0 pt-3")
                _recalcular()
                with ui.row().classes("w-full justify-end gap-2 mt-4"):
                    ui.button("Cerrar", on_click=lambda: d.close(), color="secondary").props("flat")
                    ui.button("Calcular", on_click=_recalcular, color="secondary")
                    ui.button("Guardar", on_click=lambda: _guardar_precio_popup(row, inp_refs, d), color="primary")
        d.open()

    def _guardar_precio_popup(row: Dict[str, Any], inp_refs: Dict[str, Any], dlg) -> None:
        """Guarda precio, iva, costo: actualiza items_loaded, ML y la tabla."""
        item_id = str(row.get("id", ""))
        if not item_id:
            ui.notify("ID de publicación no válido.", color="negative")
            return
        precio_str = inp_refs.get("precio") and getattr(inp_refs["precio"], "value", None) or ""
        costo_str = inp_refs.get("costo") and getattr(inp_refs["costo"], "value", None) or ""
        tipo_iva_str = inp_refs.get("tipo_iva") and getattr(inp_refs["tipo_iva"], "value", None) or "0.105"
        nuevo_precio = _parse_moneda(precio_str)
        nuevo_costo = _parse_usd(costo_str)
        nuevo_tipo_iva = float(tipo_iva_str) if tipo_iva_str else 0.105
        if nuevo_precio < 1:
            ui.notify("El precio debe ser al menos $1.", color="negative")
            return
        dlg.close()
        ui.notify("Actualizando precio en MercadoLibre...", color="info")
        client = context.client

        async def _actualizar() -> None:
            try:
                await run.io_bound(ml_update_item_price, access_token, item_id, nuevo_precio)
                await run.io_bound(set_precios_producto, item_id, uid, nuevo_tipo_iva, nuevo_costo)
                for it in items_loaded:
                    if str(it.get("id")) == item_id:
                        it["precio"] = nuevo_precio
                        it["tipo_iva"] = nuevo_tipo_iva
                        it["costo"] = nuevo_costo
                        tiene_promo = it.get("price_original") is not None and it.get("promo_yo_pct") is not None
                        precio_calc = nuevo_precio
                        if tiene_promo:
                            price_orig = float(it.get("price_original") or 0)
                            promo_yo = float(it.get("promo_yo_pct") or 0)
                            precio_calc = price_orig * (1 - promo_yo / 100)
                        comision = precio_calc * ml_comision
                        cobrado = precio_calc - comision
                        deb_cred = precio_calc * ml_debcre
                        iibb_monto = precio_calc * ml_iibb_per
                        iva_total, iva_meli, iva_impor = _calc_iva(precio_calc, nuevo_tipo_iva, comision, nuevo_costo)
                        envio_restar = _envio_a_restar(precio_calc)
                        costo_pesos = nuevo_costo * dolar_oficial
                        cuotas_val = str(it.get("cuotas") or "x1").strip().lower()
                        tasa_cuotas = {"x3": cuotas_3x, "x6": cuotas_6x, "x9": cuotas_9x, "x12": cuotas_12x}.get(cuotas_val, 0.0)
                        costo_cuotas = precio_calc * tasa_cuotas if tasa_cuotas else 0.0
                        if costo_pesos <= 0:
                            margen_pesos, margen_costo_pct, margen_venta_pct = 0.0, 0.0, 0.0
                        else:
                            margen_pesos = cobrado - costo_pesos - iva_total - iibb_monto - deb_cred - envio_restar - costo_cuotas
                            margen_costo_pct = (margen_pesos / costo_pesos * 100) if costo_pesos > 0 else 0.0
                            margen_venta_pct = (margen_pesos / precio_calc * 100) if precio_calc > 0 else 0.0
                        it["comision"] = comision
                        it["costo_cuotas"] = costo_cuotas
                        it["cobrado"] = cobrado
                        it["iva_total"] = iva_total
                        it["iva_meli"] = iva_meli
                        it["iva_impor"] = iva_impor
                        it["deb_cred"] = deb_cred
                        it["iibb"] = iibb_monto
                        it["envio"] = envio_restar
                        it["margen_pesos"] = margen_pesos
                        it["margen_costo_pct"] = margen_costo_pct
                        it["margen_venta_pct"] = margen_venta_pct
                        break
                with client:
                    filtrar_y_pintar()
                    ui.notify("Precio actualizado correctamente.", color="positive")
            except Exception as e:
                with client:
                    ui.notify(f"Error al actualizar: {e}", color="negative")

        background_tasks.create(_actualizar(), name="guardar_precio_popup")

    RENDER_CHUNK_SIZE = 25  # Evita bloquear event loop: ceder cada N filas para mantener WebSocket vivo

    async def _filtrar_y_pintar_async() -> None:
        filtrados = list(items_loaded)
        stock_val = filtro_stock_ref.get("val", "con_stock")
        periodo = filtro_fecha_ref.get("val", "mes_actual")
        ventas_dict = ventas_por_periodo_ref.get(periodo, {})
        if stock_val == "con_stock":
            filtrados = [x for x in filtrados if (x.get("stock") or 0) > 0]
        elif stock_val == "sin_stock":
            filtrados = [x for x in filtrados if (x.get("stock") or 0) == 0]
        awei_val = filtro_awei_ref.get("val", "no_incluye")
        if awei_val == "no_incluye":
            filtrados = [x for x in filtrados if "awei" not in (x.get("marca") or "").lower()]
        col_sort = sort_col_ref.get("val", "producto")
        asc = sort_asc_ref.get("val", True)
        filtrados = sorted(filtrados, key=lambda r: _sort_key(r, col_sort), reverse=not asc)
        filtrados_actuales_ref["rows"] = filtrados
        cols = COLUMNAS_MINIMO if vista_modo_ref.get("val") == "minimo" else COLUMNAS_COMPLETO
        tc = table_container_ref.get("container")
        if not tc:
            return
        tc.clear()
        es_completo = vista_modo_ref.get("val") == "completo"
        with tc:
            # Vista completo: tabla compacta que quepa en pantalla (texto más chico, columnas ajustadas)
            tab_cls = "border-collapse text-xs" if es_completo else "border-collapse text-sm"
            prod_width = "min-width: 120px; max-width: 180px;" if es_completo else "min-width: 220px;"
            cell_px = "px-1 py-0.5" if es_completo else "px-2 py-1"
            with ui.element("div").classes("w-full overflow-x-auto"):
                with ui.element("table").classes(tab_cls).style("table-layout: fixed; width: 100%; min-width: 100%" if es_completo else "width: max-content; min-width: 100%"):
                    with ui.element("thead"):
                        with ui.element("tr").classes("bg-primary text-white font-semibold sticky top-0"):
                            for field, label, align, sortable in cols:
                                th_style = prod_width if field == "producto" else "min-width: 60px;" if es_completo else ""
                                with ui.element("th").classes(f"{cell_px} border text-center whitespace-nowrap").style(th_style):
                                    if sortable:
                                        ui.button(label, on_click=lambda c=field: _on_sort_click(c)).props("flat dense no-caps").classes("text-white hover:bg-white/20 cursor-pointer font-semibold w-full")
                                    else:
                                        ui.label(label)
                    with ui.element("tbody"):
                        for i, r in enumerate(filtrados):
                            if i > 0 and i % RENDER_CHUNK_SIZE == 0:
                                await asyncio.sleep(0)  # Ceder event loop para mantener WebSocket vivo (evita "connection lost")
                            with ui.element("tr").classes("border-t border-gray-200 hover:bg-gray-50 cursor-pointer").on("click", lambda e, r=r: _on_row_click(r)):
                                for field, label, align, _ in cols:
                                    val = r.get(field)
                                    td_align = "text-center" if align == "center" else ("text-right" if align == "right" else "text-left")
                                    td_style = prod_width if field == "producto" else "min-width: 60px;" if es_completo else ""
                                    with ui.element("td").classes(f"{cell_px} border-b border-gray-100 {td_align}" + (" truncate" if es_completo and field == "producto" else "")).style(td_style):
                                        if field == "producto":
                                            txt = (str(val or "")[:80] + ("..." if len(str(val or "")) > 80 else "")) if es_completo else (str(val or "")[:100] + ("..." if len(str(val or "")) > 100 else ""))
                                            ui.label(txt)
                                        elif field == "costo":
                                            ui.label(fmt_usd(val) if val is not None else "u$0,00")
                                        elif field in ("precio", "iva_total", "iva_meli", "iva_impor", "comision", "cobrado", "deb_cred", "iibb", "envio"):
                                            ui.label(fmt_moneda(val) if val is not None else "$0")
                                        elif field == "margen_pesos":
                                            costo_r = float(r.get("costo") or 0)
                                            mp = float(val) if val is not None else 0
                                            lbl = ui.label(fmt_moneda(val) if val is not None else "$0")
                                            if costo_r <= 0:
                                                lbl.classes("font-bold text-black")
                                            else:
                                                lbl.classes("font-bold " + ("text-positive" if mp > 0 else "text-negative"))
                                        elif field == "tipo_iva":
                                            t = float(val) if val is not None else 0.105
                                            ui.label("10,5%" if abs(t - 0.105) < 0.001 else "21%")
                                        elif field in ("margen_costo_pct", "margen_venta_pct"):
                                            costo_r = float(r.get("costo") or 0)
                                            mp = float(r.get("margen_pesos") or 0)
                                            pct_str = fmt_pct(val) if es_completo else fmt_pct2(val)
                                            base_cls = "text-xs " if es_completo else ""
                                            lbl = ui.label(pct_str)
                                            if costo_r <= 0:
                                                lbl.classes(base_cls + "font-bold text-black")
                                            else:
                                                lbl.classes(base_cls + "font-bold " + ("text-positive" if mp > 0 else "text-negative"))
                                        elif field == "stock":
                                            ui.label(str(val) if val is not None else "0")
                                        elif field == "ventas":
                                            ui.label(str(val) if val is not None else "0")
                                        else:
                                            ui.label(str(val) if val is not None else "—")
        fn_calcular = calcular_labels_ref.get("_calcular_fn")
        if callable(fn_calcular):
            fn_calcular()

    def filtrar_y_pintar(ov=None) -> None:
        """Pinta la tabla en background. ov=overlay para ocultarlo al terminar. Evita bloqueo del event loop."""
        async def _do() -> None:
            await _filtrar_y_pintar_async()
            fn = calcular_labels_ref.get("_calcular_fn")
            if callable(fn):
                fn()
            if ov:
                ov.set_visibility(False)
        background_tasks.create(_do(), name="filtrar_precios_pintar")

    def _on_sort_click(col: str) -> None:
        if sort_col_ref.get("val") == col:
            sort_asc_ref["val"] = not sort_asc_ref.get("val", True)
        else:
            sort_col_ref["val"] = col
            sort_asc_ref["val"] = True
        fn = table_container_ref.get("_filtrar_fn")
        if fn:
            fn("Ordenando...")
        else:
            filtrar_y_pintar()

    async def _cargar(client) -> None:
        if timer_ref.get("t"):
            timer_ref["t"].active = True
        include_paused = include_paused_ref.get("val", False)
        try:
            # Cargar items primero para mostrar tabla rápido; órdenes en paralelo para ventas por período
            async def _fetch_items():
                return await run.io_bound(ml_get_my_items, access_token, include_paused)
            async def _fetch_orders():
                try:
                    profile = await run.io_bound(ml_get_user_profile, access_token)
                    seller_id = (profile or {}).get("id") or await run.io_bound(ml_get_user_id, access_token)
                    if seller_id:
                        hoy = datetime.now().date()
                        primer_dia_actual = hoy.replace(day=1)
                        ultimo_mes = primer_dia_actual - timedelta(days=1)
                        primer_dia_anterior = ultimo_mes.replace(day=1)
                        od_actual = await run.io_bound(
                            ml_get_orders, access_token, str(seller_id), 2000, 0,
                            date_from=primer_dia_actual.strftime("%Y-%m-%dT00:00:00.000-03:00"),
                            date_to=hoy.strftime("%Y-%m-%dT23:59:59.999-03:00"),
                        )
                        od_anterior = await run.io_bound(
                            ml_get_orders, access_token, str(seller_id), 2000, 0,
                            date_from=primer_dia_anterior.strftime("%Y-%m-%dT00:00:00.000-03:00"),
                            date_to=ultimo_mes.strftime("%Y-%m-%dT23:59:59.999-03:00"),
                        )
                        raw_a = od_actual.get("results") or od_actual.get("orders") or od_actual.get("elements") or []
                        raw_b = od_anterior.get("results") or od_anterior.get("orders") or od_anterior.get("elements") or []
                        seen = {str(o.get("id")) for o in raw_a if isinstance(o, dict) and o.get("id")}
                        merged = [o for o in raw_a if isinstance(o, dict)]
                        for o in raw_b:
                            if isinstance(o, dict) and o.get("id") and str(o.get("id")) not in seen:
                                seen.add(str(o.get("id")))
                                merged.append(o)
                        return ({"results": merged}, seller_id)
                except Exception:
                    pass
                return ({}, None)
            data, orders_result = await asyncio.gather(_fetch_items(), _fetch_orders())
            orders_data, seller_id = orders_result if isinstance(orders_result, tuple) else ({}, None)
            seller_id_ref["val"] = str(seller_id) if seller_id else None
            if not isinstance(orders_data, dict):
                orders_data = {}
        except Exception as e:
            cargar_listo_ref["error"] = str(e)
            cargar_listo_ref["listo"] = True
            return
        items = data.get("results", [])
        items_loaded.clear()

        def _id_num(id_val: Any) -> int:
            """Extrae la parte numérica del ID (ej. MLA1444322457 -> 1444322457) para ordenar."""
            s = str(id_val or "")
            num = "".join(c for c in s if c.isdigit()) or "0"
            try:
                return int(num)
            except ValueError:
                return 999999999

        items_ordenados = sorted(items, key=lambda x: _id_num(x.get("id")))
        # Mapeo item_id -> dedupe_key para todos los ítems (incl. los que deduplicamos)
        item_id_to_dedupe: Dict[str, str] = {}
        for i in items_ordenados:
            catalog_id = str(i.get("catalog_product_id") or "").strip()
            seller_sku = (i.get("seller_sku") or "").strip()
            item_id_str = str(i.get("id", ""))
            dk = ("c:" + catalog_id) if catalog_id else ("s:" + seller_sku if seller_sku else "id:" + item_id_str)
            item_id_to_dedupe[item_id_str] = dk
        # Ventas históricas (sold_quantity por item_id; no agrupar por catalog para evitar ventas cruzadas)
        ventas_historico: Dict[str, int] = {}
        for i in items_ordenados:
            item_id_str = str(i.get("id", ""))
            if item_id_str:
                sold = int(i.get("sold_quantity") or 0)
                ventas_historico["id:" + item_id_str] = ventas_historico.get("id:" + item_id_str, 0) + sold
        # Ventas por mes actual y mes anterior desde órdenes (ya cargadas en paralelo)
        ventas_mes_actual: Dict[str, int] = {}
        ventas_mes_anterior: Dict[str, int] = {}
        item_id_to_catalog_from_orders: Dict[str, str] = {}  # Para orden items sin catalog_product_id
        try:
            if seller_id and orders_data:
                raw_orders = orders_data.get("results") or orders_data.get("orders") or orders_data.get("elements") or []
                orders = [o for o in raw_orders if isinstance(o, dict)]
                hoy = datetime.now().date()
                primer_dia_actual = hoy.replace(day=1)
                ultimo_mes = primer_dia_actual - timedelta(days=1)
                primer_dia_anterior = ultimo_mes.replace(day=1)
                # Recolectar item_ids de orden sin catalog_product_id para fetchear
                ids_sin_catalog: List[str] = []
                for o in orders:
                    for it in o.get("order_items") or o.get("items") or []:
                        if not isinstance(it, dict):
                            continue
                        obj = it.get("item") or it
                        iid = str(obj.get("id") or it.get("item_id") or "").strip() if isinstance(obj, dict) else str(it.get("item_id") or "").strip()
                        if not iid:
                            continue
                        iid_mla = iid if iid.upper().startswith("MLA") else ("MLA" + iid if iid.isdigit() else iid)
                        cat_oi = str(obj.get("catalog_product_id") or it.get("catalog_product_id") or "").strip() if isinstance(obj, dict) else str(it.get("catalog_product_id") or "").strip()
                        if not cat_oi and iid_mla not in item_id_to_dedupe and iid_mla not in ids_sin_catalog:
                            ids_sin_catalog.append(iid_mla)
                if ids_sin_catalog and access_token:
                    def _fetch_catalog_ids(token: str, ids: List[str]) -> Dict[str, str]:
                        out: Dict[str, str] = {}
                        for batch_start in range(0, min(len(ids), 100), 20):
                            batch = ids[batch_start : batch_start + 20]
                            bodies = ml_get_items_multiget_with_attributes(token, batch, "id,catalog_product_id")
                            for b in (bodies or []):
                                if b and isinstance(b, dict):
                                    bid = str(b.get("id") or "").strip()
                                    cpid = str(b.get("catalog_product_id") or "").strip()
                                    if bid and cpid:
                                        out[bid] = cpid
                        return out
                    try:
                        item_id_to_catalog_from_orders.update(
                            await run.io_bound(_fetch_catalog_ids, access_token, ids_sin_catalog)
                        )
                    except Exception:
                        pass

                def _agg_ventas(orders_list: List[Dict], target: Dict[str, int]) -> None:
                    for order in orders_list:
                        dt_str = order.get("date_created") or order.get("date_closed") or order.get("date_last_updated") or ""
                        if not dt_str or not isinstance(dt_str, str):
                            continue
                        try:
                            dt = datetime.strptime(dt_str[:10], "%Y-%m-%d").date()
                        except Exception:
                            continue
                        for it in order.get("order_items") or order.get("items") or []:
                            if not isinstance(it, dict):
                                continue
                            qty = int(it.get("quantity") or it.get("qty") or 0)
                            if qty == 0:
                                continue
                            obj = it.get("item") or it
                            item_id_raw = obj.get("id") if isinstance(obj, dict) else None
                            if item_id_raw is None:
                                item_id_raw = it.get("item_id")
                            item_id = str(item_id_raw or "").strip()
                            if not item_id:
                                continue
                            item_id_mla = item_id if item_id.upper().startswith("MLA") else ("MLA" + item_id if item_id.isdigit() else item_id)
                            catalog_id_oi = str(obj.get("catalog_product_id") or it.get("catalog_product_id") or "") if isinstance(obj, dict) else str(it.get("catalog_product_id") or "")
                            catalog_id_oi = (catalog_id_oi or item_id_to_catalog_from_orders.get(item_id_mla) or item_id_to_catalog_from_orders.get(item_id) or "").strip()
                            target["id:" + item_id_mla] = target.get("id:" + item_id_mla, 0) + qty

                for o in orders:
                    dt_str = o.get("date_created") or o.get("date_closed") or ""
                    if not dt_str:
                        continue
                    try:
                        dt = datetime.strptime(dt_str[:10], "%Y-%m-%d").date()
                    except Exception:
                        continue
                    if primer_dia_actual <= dt <= hoy:
                        _agg_ventas([o], ventas_mes_actual)
                    elif primer_dia_anterior <= dt <= ultimo_mes:
                        _agg_ventas([o], ventas_mes_anterior)

                # Incluir items con ventas que no vinieron en ml_get_my_items (límite por status)
                ids_con_ventas: set = set()
                for k in list(ventas_mes_actual.keys()) + list(ventas_mes_anterior.keys()):
                    if isinstance(k, str) and k.startswith("id:") and len(k) > 3:
                        ids_con_ventas.add(k[3:])
                ids_en_items = {str(i.get("id", "")) for i in items_ordenados if i.get("id")}
                ids_faltantes = [x for x in ids_con_ventas if x and x not in ids_en_items]
                if ids_faltantes and access_token:
                    try:
                        bodies_extra = await run.io_bound(ml_get_items_multiget_all, access_token, ids_faltantes[:50])
                        for b in (bodies_extra or []):
                            if b and isinstance(b, dict):
                                item_extra = _body_to_precios_item(b)
                                if item_extra.get("id"):
                                    items_ordenados.append(item_extra)
                                    iid = str(item_extra["id"])
                                    cat = str(item_extra.get("catalog_product_id") or "").strip()
                                    sku = (item_extra.get("seller_sku") or "").strip()
                                    dk = ("c:" + cat) if cat else ("s:" + sku if sku else "id:" + iid)
                                    item_id_to_dedupe[iid] = dk
                                    ventas_historico["id:" + iid] = ventas_historico.get("id:" + iid, 0) + int(item_extra.get("sold_quantity") or 0)
                    except Exception:
                        pass
        except Exception:
            pass
        ventas_por_periodo_ref["historico"] = ventas_historico
        ventas_por_periodo_ref["mes_actual"] = ventas_mes_actual
        ventas_por_periodo_ref["mes_anterior"] = ventas_mes_anterior

        # sale_price y cuotas: cargar en segundo plano para mostrar tabla rápido
        item_id_to_sale_price: Dict[str, Dict[str, Any]] = {}
        item_id_to_cuotas_precios: Dict[str, str] = {}
        item_ids_precios = [str(i.get("id", "")).strip() for i in items_ordenados if i.get("id")]
        seller_id_precios = str(seller_id) if seller_id else None
        if not seller_id_precios and access_token:
            try:
                profile = await run.io_bound(ml_get_user_profile, access_token)
                seller_id_precios = str((profile or {}).get("id") or "")
            except Exception:
                pass

        def _fetch_sale_price_and_cuotas(token: str, ids: List[str], user_id: str) -> tuple:
                sale_price_map: Dict[str, Dict[str, Any]] = {}
                cuotas_map: Dict[str, str] = {}
                max_workers = min(8, len(ids))
                with ThreadPoolExecutor(max_workers=max_workers) as ex:
                    futures_sp = {ex.submit(ml_get_item_sale_price_full, token, iid): iid for iid in ids}
                    for fut in as_completed(futures_sp):
                        iid = futures_sp[fut]
                        try:
                            data = fut.result()
                            if data and data.get("amount") is not None:
                                reg_val = data.get("regular_amount")
                                amt_val = float(data["amount"])
                                entry: Dict[str, Any] = {"amount": amt_val, "regular_amount": float(reg_val) if reg_val is not None else None}
                                reg_f = entry.get("regular_amount")
                                tiene_promo = reg_f is not None and reg_f > 0 and abs(reg_f - amt_val) > 0.01
                                if tiene_promo:
                                    promo_id = data.get("promotion_id")
                                    promo_type = (data.get("promotion_type") or "").strip().upper()
                                    campaign_id = data.get("campaign_id")
                                    total_pct_val = ((reg_f - amt_val) / reg_f * 100)
                                    discounts = None
                                    if campaign_id and user_id:
                                        discounts = ml_get_promotion_item_discounts_by_campaign(
                                            token, str(campaign_id), iid, total_pct_val, user_id,
                                            promotion_type_hint=promo_type,
                                        )
                                    if discounts is None and promo_id and promo_type and not (str(promo_id or "").upper().startswith("OFFER-")):
                                        discounts = ml_get_promotion_item_discounts(token, str(promo_id), promo_type, iid, total_pct_val)
                                    if discounts is None and user_id:
                                        discounts = ml_get_promotion_item_discounts_by_user(token, iid, user_id, total_pct_val)
                                    if discounts is not None:
                                        entry["promo_ml_pct"] = discounts.get("meli_pct", 0)
                                        entry["promo_yo_pct"] = discounts.get("seller_pct", 0)
                                    else:
                                        entry["promo_ml_pct"] = 0.0
                                        entry["promo_yo_pct"] = total_pct_val
                                sale_price_map[iid] = entry
                        except Exception:
                            pass
                attrs = "id,listing_type_id,attributes,sale_terms"
                for i in range(0, len(ids), 20):
                    batch = ids[i : i + 20]
                    bodies = ml_get_items_multiget_with_attributes(token, batch, attrs)
                    for b in (bodies or []):
                        if b and isinstance(b, dict):
                            iid = str(b.get("id", "") or "").strip()
                            if iid:
                                cuotas_map[iid] = _cuotas_desde_item(b)
                return sale_price_map, cuotas_map

        seller_id_ref["val"] = seller_id_precios

        # Agrupar por dedupe_key; preferir catalog_listing=false (Propia), solo usar Catálogo si no hay Propia
        grupos_por_dedupe: Dict[str, List[Dict]] = {}
        for i in items_ordenados:
            catalog_id = str(i.get("catalog_product_id") or "").strip()
            seller_sku = (i.get("seller_sku") or "").strip()
            dedupe_key = ("c:" + catalog_id) if catalog_id else ("s:" + seller_sku if seller_sku else "")
            dk = dedupe_key or ("id:" + str(i.get("id", "")))
            if dk not in grupos_por_dedupe:
                grupos_por_dedupe[dk] = []
            grupos_por_dedupe[dk].append(i)
        periodo_activo = filtro_fecha_ref.get("val", "mes_actual")
        ventas_dict = ventas_por_periodo_ref.get(periodo_activo, ventas_historico)
        items_a_mostrar: List[tuple] = []
        for dk, grupo in grupos_por_dedupe.items():
            for i in grupo:
                items_a_mostrar.append((i, [i]))
        def _agregar_row(items_list: list, item_dict: Dict[str, Any], grupo_single: List[Dict]) -> None:
            i = item_dict
            catalog_id = str(i.get("catalog_product_id") or "").strip()
            seller_sku = (i.get("seller_sku") or "").strip()
            dedupe_key = ("c:" + catalog_id) if catalog_id else ("s:" + seller_sku if seller_sku else "")
            precio = float(i.get("price") or 0)
            sale_price = i.get("sale_price")
            item_id_str = str(i.get("id", ""))
            # Promo: preferir API sale_price (como Ventas) si el item no lo trae
            sp_data = item_id_to_sale_price.get(item_id_str) or item_id_to_sale_price.get(item_id_str.upper() or "") or item_id_to_sale_price.get(item_id_str.lower() or "")
            if sp_data and sp_data.get("regular_amount") is not None and sp_data.get("amount") is not None:
                reg_f = float(sp_data["regular_amount"])
                amt_f = float(sp_data["amount"])
                tiene_promo = reg_f > 0 and abs(reg_f - amt_f) > 0.01
                if tiene_promo:
                    price_original = reg_f
                    promo_pct = ((reg_f - amt_f) / reg_f * 100)
                    promo_ml_pct = sp_data.get("promo_ml_pct")
                    promo_yo_pct = sp_data.get("promo_yo_pct")
                    if promo_ml_pct is None:
                        promo_ml_pct = 0.0
                    if promo_yo_pct is None:
                        promo_yo_pct = promo_pct
                    precio_real = amt_f
                    price_promo = reg_f * (1 - (promo_yo_pct or 0) / 100)
                else:
                    price_original = None
                    price_promo = None
                    promo_pct = None
                    promo_ml_pct = None
                    promo_yo_pct = None
                    precio_real = float(sale_price) if sale_price is not None else precio
            else:
                precio_real = float(sale_price) if sale_price is not None else precio
                tiene_promo = sale_price is not None and precio > 0 and abs(precio - float(sale_price or 0)) > 0.01
                price_original = float(precio) if tiene_promo else None
                promo_pct = ((precio - float(sale_price or 0)) / precio * 100) if tiene_promo else None
                promo_ml_pct = 0.0 if tiene_promo else None
                promo_yo_pct = promo_pct if tiene_promo else None
                price_promo = (price_original * (1 - (promo_yo_pct or 0) / 100)) if tiene_promo and price_original is not None else None
            cuotas_val = str(item_id_to_cuotas_precios.get(item_id_str) or item_id_to_cuotas_precios.get(item_id_str.upper() or "") or item_id_to_cuotas_precios.get(item_id_str.lower() or "") or _cuotas_desde_item(i) or "x1").strip().lower()
            stock = int(i.get("available_quantity") or 0)
            guardado = get_precios_producto(item_id_str, uid)
            costo = float(guardado["costo_u"]) if guardado else 0.0
            tipo_iva = float(guardado["tipo_iva"]) if guardado else 0.105
            precio_calc = price_promo if tiene_promo and price_promo is not None else precio_real
            comision = precio_calc * ml_comision
            cobrado = precio_calc - comision
            iva_total, iva_meli, iva_impor = _calc_iva(precio_calc, tipo_iva, comision, costo)
            deb_cred = precio_calc * ml_debcre
            iibb_monto = precio_calc * ml_iibb_per
            envio_restar = _envio_a_restar(precio_calc)
            costo_pesos = costo * dolar_oficial
            tasa_cuotas = {"x3": cuotas_3x, "x6": cuotas_6x, "x9": cuotas_9x, "x12": cuotas_12x}.get(cuotas_val, 0.0)
            costo_cuotas = precio_calc * tasa_cuotas if tasa_cuotas else 0.0
            if costo_pesos <= 0:
                margen_pesos, margen_costo_pct, margen_venta_pct = 0.0, 0.0, 0.0
            else:
                margen_pesos = cobrado - costo_pesos - iva_total - iibb_monto - deb_cred - envio_restar - costo_cuotas
                margen_costo_pct = (margen_pesos / costo_pesos * 100) if costo_pesos > 0 else 0.0
                margen_venta_pct = (margen_pesos / precio_calc * 100) if precio_calc > 0 else 0.0
            dk_final = dedupe_key or ("id:" + item_id_str)
            ventas = sum(ventas_dict.get("id:" + str(it_g.get("id", "")), 0) for it_g in grupo_single)
            grupo_ids = [str(it_g.get("id", "")) for it_g in grupo_single if it_g.get("id")]
            items_list.append({
                "id": str(i.get("id", "")),
                "thumbnail": i.get("thumbnail") or "",
                "marca": i.get("marca") or "—",
                "producto": str(i.get("title") or ""),
                "stock": stock,
                "ventas": ventas,
                "dedupe_key": dk_final,
                "grupo_ids": grupo_ids or [str(i.get("id", ""))],
                "tipo_publicacion": _tipo_publicacion_desde_item(i),
                "cuotas": cuotas_val,
                "price_original": price_original,
                "price_promo": price_promo,
                "promo_pct": promo_pct,
                "promo_ml_pct": promo_ml_pct,
                "promo_yo_pct": promo_yo_pct,
                "precio": precio_real,
                "tipo_iva": tipo_iva,
                "iva_total": iva_total,
                "iva_meli": iva_meli,
                "iva_impor": iva_impor,
                "costo": costo,
                "comision": comision,
                "cobrado": cobrado,
                "costo_cuotas": costo_cuotas,
                "deb_cred": deb_cred,
                "iibb": iibb_monto,
                "envio": envio_restar,
                "margen_pesos": margen_pesos,
                "margen_costo_pct": margen_costo_pct,
                "margen_venta_pct": margen_venta_pct,
            })

        def _item_from_body_export(body: dict) -> dict:
            marca, color, seller_sku = "", "", ""
            for att in body.get("attributes") or []:
                aid = (str(att.get("id") or "")).strip().upper()
                if aid in ("BRAND", "MARCA"):
                    val = att.get("value_name") or att.get("value_id")
                    marca = str(val) if val is not None else ""
                elif aid in ("COLOR", "COLOUR"):
                    val = att.get("value_name") or att.get("value_id")
                    if val:
                        color = str(val)
                        break
                elif aid == "SELLER_SKU":
                    v = att.get("value_name") or att.get("value") or att.get("value_id")
                    if v is None and att.get("values"):
                        v = (att["values"][0] or {}).get("name") or (att["values"][0] or {}).get("value_name")
                    if v is not None:
                        seller_sku = str(v).strip()
            if not seller_sku:
                seller_sku = (body.get("seller_custom_field") or "").strip()
            catalog_listing = body.get("catalog_listing") is True
            thumbnail = body.get("thumbnail") or ""
            if not thumbnail and body.get("pictures"):
                pic = (body.get("pictures") or [{}])[0]
                thumbnail = pic.get("secure_url") or pic.get("url") or ""
            return {
                "id": body.get("id"),
                "title": body.get("title", ""),
                "thumbnail": thumbnail,
                "price": body.get("price"),
                "sale_price": body.get("sale_price"),
                "available_quantity": body.get("available_quantity"),
                "catalog_product_id": body.get("catalog_product_id"),
                "catalog_listing": catalog_listing,
                "listing_type_id": body.get("listing_type_id"),
                "sale_terms": body.get("sale_terms"),
                "seller_sku": seller_sku,
                "marca": marca or "—",
            }

        for i, grupo in items_a_mostrar:
            _agregar_row(items_loaded, i, grupo)

        ids_ya_incluidos = {str(r.get("id", "")) for r in items_loaded}
        item_ids_con_ventas = [k[3:] for k in ventas_dict if isinstance(k, str) and k.startswith("id:") and ventas_dict.get(k, 0) > 0]
        ids_faltantes = [x for x in item_ids_con_ventas if x and x not in ids_ya_incluidos]
        if ids_faltantes and access_token:
            try:
                attrs = "id,title,thumbnail,price,sale_price,available_quantity,catalog_product_id,catalog_listing,listing_type_id,sale_terms,attributes"
                for batch_start in range(0, min(len(ids_faltantes), 200), 20):
                    batch = ids_faltantes[batch_start : batch_start + 20]
                    bodies_extra = ml_get_items_multiget_with_attributes(access_token, batch, attrs)
                    for b in (bodies_extra or []):
                        if not b or not isinstance(b, dict):
                            continue
                        item_id_b = str(b.get("id") or "").strip()
                        if not item_id_b or item_id_b in ids_ya_incluidos:
                            continue
                        item_norm = _item_from_body_export(b)
                        _agregar_row(items_loaded, item_norm, [item_norm])
                        ids_ya_incluidos.add(item_id_b)
            except Exception:
                pass

        publicaciones_totales = len(items_loaded)
        publicaciones_con_stock = sum(1 for x in items_loaded if (x.get("stock") or 0) > 0)
        cargar_listo_ref["error"] = None
        cargar_listo_ref["totales"] = publicaciones_totales
        cargar_listo_ref["con_stock"] = publicaciones_con_stock
        cargar_listo_ref["listo"] = True

        def _fetch_and_update_rows() -> None:
            """Sync: fetchea sale_price+cuotas y actualiza items_loaded."""
            if not item_ids_precios or not access_token or not seller_id_precios:
                return
            try:
                sp_map, cuotas_map = _fetch_sale_price_and_cuotas(
                    access_token, item_ids_precios, seller_id_precios
                )
                item_id_to_sale_price.update(sp_map)
                item_id_to_cuotas_precios.update(cuotas_map)
                for row in items_loaded:
                    iid = str(row.get("id", "")).strip()
                    sp_data = item_id_to_sale_price.get(iid) or item_id_to_sale_price.get(iid.upper() or "") or item_id_to_sale_price.get(iid.lower() or "")
                    cuotas_nueva = item_id_to_cuotas_precios.get(iid) or item_id_to_cuotas_precios.get(iid.upper() or "") or item_id_to_cuotas_precios.get(iid.lower() or "") or row.get("cuotas") or "x1"
                    if sp_data and sp_data.get("regular_amount") is not None and sp_data.get("amount") is not None:
                        reg_f = float(sp_data["regular_amount"])
                        amt_f = float(sp_data["amount"])
                        tiene_promo = reg_f > 0 and abs(reg_f - amt_f) > 0.01
                        if tiene_promo:
                            row["price_original"] = reg_f
                            row["promo_pct"] = ((reg_f - amt_f) / reg_f * 100)
                            row["promo_ml_pct"] = sp_data.get("promo_ml_pct") if sp_data.get("promo_ml_pct") is not None else 0.0
                            row["promo_yo_pct"] = sp_data.get("promo_yo_pct") if sp_data.get("promo_yo_pct") is not None else row["promo_pct"]
                            row["price_promo"] = reg_f * (1 - (row["promo_yo_pct"] or 0) / 100)
                            row["precio"] = amt_f
                        else:
                            row["price_original"] = None
                            row["price_promo"] = None
                            row["promo_pct"] = None
                            row["promo_ml_pct"] = None
                            row["promo_yo_pct"] = None
                            row["precio"] = amt_f
                    row["cuotas"] = str(cuotas_nueva).strip().lower()
                    precio_calc = row.get("price_promo") if (row.get("price_original") and row.get("price_promo")) else row.get("precio", 0)
                    costo = float(row.get("costo") or 0)
                    tipo_iva = float(row.get("tipo_iva") or 0.105)
                    row["comision"] = precio_calc * ml_comision
                    row["cobrado"] = precio_calc - row["comision"]
                    iva_total, iva_meli, iva_impor = _calc_iva(precio_calc, tipo_iva, row["comision"], costo)
                    row["iva_total"] = iva_total
                    row["iva_meli"] = iva_meli
                    row["iva_impor"] = iva_impor
                    row["deb_cred"] = precio_calc * ml_debcre
                    row["iibb"] = precio_calc * ml_iibb_per
                    row["envio"] = _envio_a_restar(precio_calc)
                    tasa_cuotas = {"x3": cuotas_3x, "x6": cuotas_6x, "x9": cuotas_9x, "x12": cuotas_12x}.get(row["cuotas"], 0.0)
                    row["costo_cuotas"] = precio_calc * tasa_cuotas if tasa_cuotas else 0.0
                    costo_pesos = costo * dolar_oficial
                    if costo_pesos <= 0:
                        row["margen_pesos"], row["margen_costo_pct"], row["margen_venta_pct"] = 0.0, 0.0, 0.0
                    else:
                        row["margen_pesos"] = row["cobrado"] - costo_pesos - iva_total - row["iibb"] - row["deb_cred"] - row["envio"] - row["costo_cuotas"]
                        row["margen_costo_pct"] = (row["margen_pesos"] / costo_pesos * 100) if costo_pesos > 0 else 0.0
                        row["margen_venta_pct"] = (row["margen_pesos"] / precio_calc * 100) if precio_calc > 0 else 0.0
            except Exception:
                pass

        async def _task_enriquecer() -> None:
            await run.io_bound(_fetch_and_update_rows)
            if client:
                with client:
                    fn = table_container_ref.get("_filtrar_fn")
                    if fn:
                        fn("Actualizando precios...")

        background_tasks.create(_task_enriquecer(), name="enriquecer_precios")

    try:
        client = context.client
    except RuntimeError:
        client = None
    background_tasks.create(_cargar(client), name="cargar_precios_detalle")


def _fmt_fecha_compras(s: str) -> str:
    """Formato fecha: 'Lunes 16-03-26 09:30' (dia dd-mm-aa hora:minutos)."""
    if not s or not str(s).strip():
        return "—"
    s = str(s).strip()
    try:
        if " " in s:
            parts = s.split(" ", 1)
            date_str, time_str = parts[0], parts[1][:5] if len(parts) > 1 else ""
        else:
            date_str, time_str = s[:10], ""
        p = date_str.split("-")
        if len(p) >= 3:
            y, m, d = int(p[0]), int(p[1]), int(p[2])
            dt_obj = datetime(y, m, d)
            dia_nombre = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"][dt_obj.weekday()]
            dd = f"{d:02d}-{m:02d}-{y % 100:02d}"
            if time_str:
                return f"{dia_nombre} {dd} {time_str}"
            return f"{dia_nombre} {dd}"
        return s
    except Exception:
        return str(s)


def _fmt_precio_compras(val: str) -> str:
    """Formatea precio para pantalla: punto -> coma (ej: 1234.56 -> 1234,56)."""
    if not val:
        return ""
    s = str(val).strip()
    return s.replace(".", ",")


def _parse_precio_compras_input(s: str) -> str:
    """Parsea precio: acepta coma o punto como decimal, normaliza a punto para BD."""
    if not s or not str(s).strip():
        return ""
    s = str(s).strip().replace(",", ".")
    # Dejar solo dígitos y un punto
    parts = s.split(".")
    if len(parts) > 2:
        s = parts[0] + "." + "".join(parts[1:])
    return s


def _parse_fecha_compras_input(s: str) -> str:
    """Parsea 'Lun 16-03-26 09:30' o '16-03-26 09:30' a 'YYYY-MM-DD HH:MM'."""
    if not s or not str(s).strip():
        return ""
    s = str(s).strip()
    import re
    # Buscar dd-mm-yy (o yy) y opcional hh:mm
    m = re.search(r"(\d{1,2})-(\d{1,2})-(\d{2,4})\s*(\d{1,2}:\d{2})?", s)
    if m:
        d, m_val, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        y_full = 2000 + y if y < 100 else y
        time_part = m.group(4) or "00:00"
        return f"{y_full:04d}-{m_val:02d}-{d:02d} {time_part}"
    # Si ya está en YYYY-MM-DD
    if re.match(r"\d{4}-\d{2}-\d{2}", s):
        return s[:16] if len(s) > 10 else (s + " 00:00")
    return s


def _solo_numeros(val: str) -> str:
    """Filtra a solo dígitos (cantidad entera)."""
    if not val:
        return ""
    return "".join(c for c in str(val) if c.isdigit())


def _sort_key_compras(row: Dict[str, Any], col: str) -> Any:
    """Clave de ordenación para filas de compras_lista."""
    if col == "fecha":
        raw = row.get("fecha") or ""
        try:
            if " " in raw:
                ds, ts = raw.split(" ", 1)
                return ds + (ts[:5] if ts else "")
            return raw[:10] + " 00:00"
        except Exception:
            return ""
    if col in ("cantidad", "precio_sugerido"):
        try:
            return float(row.get(col) or 0)
        except (ValueError, TypeError):
            return 0.0
    return (row.get(col) or "").lower()


def build_tab_compras_lista(container) -> None:
    """Pestaña Compras Lista: tabla editable de compras a cotizar (marca, producto, cantidad, estado, usuario_qb)."""
    user = require_login()
    if not user:
        return

    container.clear()
    marcas_list = get_marcas()
    qb_cust = get_user_qb_customer(user["id"])
    cliente_default = (qb_cust or {}).get("name", "")

    with container:
        filtro_estado_ref: Dict[str, str] = {"val": "Todas"}
        sort_col_ref: List[str] = [""]
        sort_asc_ref: List[bool] = [True]
        # Filtro arriba de tabla (solo), tabla, botón debajo
        compras_header = ui.column().classes("w-full mb-2")
        filtro_row = ui.column().classes("w-full mb-2")
        tabla_container = ui.column().classes("w-full gap-2")
        boton_row = ui.row().classes("w-full mt-2 items-center")

        user_id_ref: List[int] = [user["id"]]
        tbody_el = None  # se asignará al crear la tabla

        def _filtrar_cantidad_on_input(inp) -> None:
            """Solo permite dígitos en cantidad."""
            if hasattr(inp, "value"):
                actual = getattr(inp, "value", "") or ""
                filtrado = _solo_numeros(actual)
                if filtrado != actual:
                    inp.value = filtrado

        def _filtrar_precio_on_input(inp) -> None:
            """Solo permite dígitos, punto y coma en precio; muestra coma como decimal."""
            if not hasattr(inp, "value"):
                return
            s = getattr(inp, "value", "") or ""
            s = "".join(c for c in str(s) if c.isdigit() or c in ".,")
            # Máximo un separador decimal; mantener primera parte entera y primera decimal
            if s.count(".") + s.count(",") > 1:
                parts = s.replace(",", ".").split(".")
                s = parts[0] + "," + (parts[1] if len(parts) > 1 else "")
            s = s.replace(".", ",")
            if s != (getattr(inp, "value", "") or ""):
                inp.value = s

        def _refrescar_tabla() -> None:
            """Limpia tbody y pinta todas las filas filtradas."""
            uid = user_id_ref[0]
            rows = get_compras_lista(uid)
            filtro_val = filtro_estado_ref.get("val", "Todas")
            if filtro_val and filtro_val != "Todas":
                if filtro_val == "No hay":
                    filtrados = [r for r in rows if (r.get("estado") or "") == ""]
                elif filtro_val == "Cotizar":
                    filtrados = [r for r in rows if r.get("estado") == "Cotizar"]
                elif filtro_val == "Buscando":
                    filtrados = [r for r in rows if r.get("estado") == "Buscando"]
                elif filtro_val == "Comprado":
                    filtrados = [r for r in rows if r.get("estado") == "Comprado"]
                else:
                    filtrados = rows
            else:
                filtrados = rows
            filtrados = sorted(filtrados, key=lambda r: _sort_key_compras(r, sort_col_ref[0] or "fecha"), reverse=not sort_asc_ref[0])
            n_pedidos = len(filtrados)
            total_cotizar = 0.0
            for r in filtrados:
                try:
                    cant = float(str(r.get("cantidad") or "0").replace(",", ".")) if r.get("cantidad") else 0
                except (ValueError, TypeError):
                    cant = 0
                try:
                    precio = float(str(r.get("precio_sugerido") or "0").replace(",", ".")) if r.get("precio_sugerido") else 0
                except (ValueError, TypeError):
                    precio = 0
                total_cotizar += cant * precio
            compras_header.clear()
            with compras_header:
                ui.label("Compras").classes("text-xl font-semibold mb-2")
                with ui.card().classes("w-full p-4 bg-grey-2"):
                    with ui.row().classes("w-full gap-6 flex-wrap items-center"):
                        with ui.column().classes("gap-0"):
                            ui.label("Cantidad de pedidos").classes("text-xs text-gray-600")
                            ui.label(str(n_pedidos)).classes("text-lg font-bold text-primary")
                        ui.element("div").classes("w-px h-8 bg-gray-400 shrink-0")
                        with ui.column().classes("gap-0"):
                            ui.label("Total a cotizar").classes("text-xs text-gray-600")
                            _ts = f"{total_cotizar:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                            ui.label(f"$ {_ts}").classes("text-lg font-bold text-primary")
                        ui.element("div").classes("w-px h-8 bg-gray-400 shrink-0")
                        ui.button("Refrescar", on_click=_refrescar_tabla).props("flat dense no-caps icon=refresh").classes("text-gray-800 hover:bg-gray-200 rounded px-3")
            tbody_el.clear()
            with tbody_el:
                for r in filtrados:
                    _crear_fila_tr(r, uid)

        def _guardar_campo(evt, row_id: int, uid: int, kw: Dict) -> None:
            for k, inp in kw.items():
                v = (getattr(inp, "value", "") or "").strip() if hasattr(inp, "value") else ""
                if k == "fecha":
                    v = _parse_fecha_compras_input(v)
                elif k == "cantidad":
                    v = _solo_numeros(v)
                elif k == "precio_sugerido":
                    v = _parse_precio_compras_input(v)
                update_compras_lista_row(row_id, uid, **{k: v})
            ui.notify("Guardado", color="positive")
            # No refrescar al guardar un campo: evita borrar el producto al pasar a cantidad/precio

        def _guardar_marca(e, row_id: int, uid: int) -> None:
            v = getattr(e, "value", "") or ""
            if not v or v == "(Otras)":
                v = ""
            update_compras_lista_row(row_id, uid, marca=str(v))
            ui.notify("Marca actualizada", color="positive")
            # No refrescar: evita borrar datos de la fila

        def _borrar_fila(row_id: int, uid: int) -> None:
            delete_compras_lista_row(row_id, uid)
            ui.notify("Fila eliminada", color="positive")
            _refrescar_tabla()

        def _on_filtro(e) -> None:
            v = getattr(e, "value", None)
            filtro_estado_ref["val"] = v if v is not None and v != "" else ("Todas" if v == "" else "Cotizar")
            _refrescar_tabla()

        def _agregar_fila() -> None:
            u = require_login()
            if not u:
                ui.notify("Debe iniciar sesión", color="negative")
                return
            qb = get_user_qb_customer(u["id"])
            cli = (qb or {}).get("name", "")
            now = datetime.now()
            fecha_str = now.strftime("%Y-%m-%d %H:%M")
            new_id = insert_compras_lista(u["id"], fecha_str, estado="Cotizar", usuario_qb=cli)
            new_row = get_compras_lista_row(new_id, u["id"])
            filtro_estado_ref["val"] = "Cotizar"
            if filtro_select_ref[0] is not None:
                filtro_select_ref[0].value = "Cotizar"
            _refrescar_tabla()
            ui.notify("Fila agregada", color="positive")

        filtro_select_ref: List = [None]
        with filtro_row:
            filtro_select_ref[0] = ui.select(
                {"Todas": "Todas", "Cotizar": "Cotizar", "No hay": "No hay", "Buscando": "Buscando", "Comprado": "Comprado"},
                value=filtro_estado_ref.get("val", "Cotizar"),
                label="Estado",
                on_change=lambda e: _on_filtro(e),
            ).classes("w-40").props("dense")
        with boton_row:
            ui.button("Agregar fila", on_click=_agregar_fila, color="primary").props("dense no-caps")

        def _crear_fila_tr(r: Dict, uid: int) -> None:
            """Crea una fila (tr) en el tbody actual."""
            rid = r["id"]
            user_id_row = r.get("user_id", uid)
            with ui.element("tr").classes("border-t hover:bg-gray-50"):
                with ui.element("td").classes("px-2 py-1 border"):
                    fecha_val = _fmt_fecha_compras(r.get("fecha", "")) if r.get("fecha") else ""
                    inp_f = ui.input(value=fecha_val).classes("w-36").props("dense")
                    inp_f.on("keydown.enter", lambda evt, row_id=rid, uid=user_id_row, kw={"fecha": inp_f}: _guardar_campo(evt, row_id, uid, kw))
                    inp_f.on("blur", lambda evt, row_id=rid, uid=user_id_row, kw={"fecha": inp_f}: _guardar_campo(evt, row_id, uid, kw))
                with ui.element("td").classes("px-2 py-1 border"):
                    marcas_opts = {m["nombre"]: m["nombre"] for m in marcas_list}
                    marca_actual = r.get("marca", "") or ""
                    if marca_actual and marca_actual not in marcas_opts:
                        marcas_opts = {marca_actual: marca_actual, **marcas_opts}
                    marcas_opts = {"": "(Otras)", **marcas_opts}
                    ui.select(marcas_opts, value=marca_actual or None, on_change=lambda e, row_id=rid, uid=user_id_row: _guardar_marca(e, row_id, uid)).classes("w-28").props("dense")
                with ui.element("td").classes("px-2 py-1 border"):
                    inp_p = ui.input(value=r.get("producto", "")).classes("w-56").props("dense")
                    inp_p.on("keydown.enter", lambda evt, row_id=rid, uid=user_id_row, kw={"producto": inp_p}: _guardar_campo(evt, row_id, uid, kw))
                    inp_p.on("blur", lambda evt, row_id=rid, uid=user_id_row, kw={"producto": inp_p}: _guardar_campo(evt, row_id, uid, kw))
                with ui.element("td").classes("px-2 py-1 border"):
                    inp_s = ui.input(value=r.get("sku", "")).classes("w-36").props("dense")
                    inp_s.on("keydown.enter", lambda evt, row_id=rid, uid=user_id_row, kw={"sku": inp_s}: _guardar_campo(evt, row_id, uid, kw))
                    inp_s.on("blur", lambda evt, row_id=rid, uid=user_id_row, kw={"sku": inp_s}: _guardar_campo(evt, row_id, uid, kw))
                with ui.element("td").classes("px-2 py-1 border text-center"):
                    cant_val = _solo_numeros(str(r.get("cantidad", "") or ""))
                    inp_c = ui.input(value=cant_val).classes("w-16").props("dense inputmode=numeric")
                    inp_c.on("input", lambda e, inp=inp_c: _filtrar_cantidad_on_input(inp))
                    inp_c.on("keydown.enter", lambda evt, row_id=rid, uid=user_id_row, kw={"cantidad": inp_c}: _guardar_campo(evt, row_id, uid, kw))
                    inp_c.on("blur", lambda evt, row_id=rid, uid=user_id_row, kw={"cantidad": inp_c}: _guardar_campo(evt, row_id, uid, kw))
                with ui.element("td").classes("px-2 py-1 border text-right"):
                    precio_val = _fmt_precio_compras(str(r.get("precio_sugerido", "") or ""))
                    with ui.row().classes("items-center justify-end gap-1"):
                        ui.label("u$").classes("text-gray-600 text-sm")
                        inp_ps = ui.input(value=precio_val).classes("w-20").props("dense")
                    inp_ps.on("input", lambda e, inp=inp_ps: _filtrar_precio_on_input(inp))
                    inp_ps.on("keydown.enter", lambda evt, row_id=rid, uid=user_id_row, kw={"precio_sugerido": inp_ps}: _guardar_campo(evt, row_id, uid, kw))
                    inp_ps.on("blur", lambda evt, row_id=rid, uid=user_id_row, kw={"precio_sugerido": inp_ps}: _guardar_campo(evt, row_id, uid, kw))
                with ui.element("td").classes("px-2 py-1 border"):
                    _est_display = {"": "No hay", "Buscando": "Buscando"}.get(r.get("estado") or "", r.get("estado") or "Cotizar")
                    ui.label(_est_display).classes("text-sm")
                with ui.element("td").classes("px-2 py-1 border"):
                    ui.button("Borrar", on_click=lambda row_id=rid, uid=user_id_row: _borrar_fila(row_id, uid)).props("flat dense no-caps").classes("text-negative")

        def _on_filtro(e) -> None:
            filtro_estado_ref["val"] = getattr(e, "value", "Cotizar") or "Cotizar"
            _refrescar_tabla()

        def _agregar_fila() -> None:
            u = require_login()
            if not u:
                ui.notify("Debe iniciar sesión", color="negative")
                return
            qb = get_user_qb_customer(u["id"])
            cli = (qb or {}).get("name", "")
            now = datetime.now()
            fecha_str = now.strftime("%Y-%m-%d %H:%M")
            new_id = insert_compras_lista(u["id"], fecha_str, estado="Cotizar", usuario_qb=cli)
            new_row = get_compras_lista_row(new_id, u["id"])
            if new_row and filtro_estado_ref.get("val") == "Cotizar":
                with tbody_el:
                    _crear_fila_tr(new_row, u["id"])
            ui.notify("Fila agregada", color="positive")

        def _th_classes(col_key: str) -> str:
            base = "px-2 py-1 border cursor-pointer hover:bg-primary/80"
            if col_key == "precio_sugerido":
                return f"{base} text-center"
            return f"{base} text-center"

        with tabla_container:
            with ui.element("table").classes("w-full border-collapse text-sm"):
                with ui.element("thead"):
                    with ui.element("tr").classes("bg-primary text-white font-semibold text-center"):
                        for col_key, h in [("fecha", "Fecha"), ("marca", "Marca"), ("producto", "Producto"), ("sku", "SKU"), ("cantidad", "Cantidad"), ("precio_sugerido", "Precio sugerido"), ("estado", "Estado"), ("", "Borrar")]:
                            th = ui.element("th").classes(_th_classes(col_key))
                            if col_key:
                                th.on("click", lambda c=col_key: (sort_col_ref.__setitem__(0, c) if sort_col_ref[0] != c else sort_asc_ref.__setitem__(0, not sort_asc_ref[0]), sort_col_ref.__setitem__(0, c), sort_asc_ref.__setitem__(0, True) if sort_col_ref[0] != c else None, _refrescar_tabla()))
                            with th:
                                ui.label(h)
                with ui.element("tbody") as tbody_el:
                    pass

        _refrescar_tabla()


def build_tab_pedidos(container) -> None:
    """Pestaña Pedidos: vista consolidada de compras de todos los clientes (usuario_qb = Cliente QB)."""
    user = require_login()
    if not user:
        return

    is_admin = user_can_access_tab(user["id"], "admin")
    qb_cust = get_user_qb_customer(user["id"])
    mi_cliente_qb = (qb_cust or {}).get("name", "")

    with container:
        filtro_estado_ref: Dict[str, str] = {"val": "Cotizar+Buscando"}
        filtro_cliente_ref: Dict[str, str] = {"val": ""}
        sort_col_ref: List[str] = [""]
        sort_asc_ref: List[bool] = [True]
        tabla_container = ui.column().classes("w-full gap-2")

        def _refrescar() -> None:
            tabla_container.clear()
            with tabla_container:
                rows = get_compras_lista_all()
                est_val = filtro_estado_ref.get("val", "Cotizar+Buscando")
                if est_val:
                    if est_val == "No hay":
                        rows = [r for r in rows if (r.get("estado") or "") == ""]
                    elif est_val == "Cotizar":
                        rows = [r for r in rows if r.get("estado") == "Cotizar"]
                    elif est_val == "Cotizar+Buscando":
                        rows = [r for r in rows if r.get("estado") in ("Cotizar", "Buscando")]
                    elif est_val == "Buscando":
                        rows = [r for r in rows if r.get("estado") == "Buscando"]
                    elif est_val == "Comprado":
                        rows = [r for r in rows if r.get("estado") == "Comprado"]
                clientes = sorted(set(r.get("usuario_qb") or "" for r in rows if r.get("usuario_qb")))
                cli_val = filtro_cliente_ref.get("val", "")
                if cli_val:
                    rows = [r for r in rows if (r.get("usuario_qb") or "") == cli_val]

                rows = sorted(rows, key=lambda r: _sort_key_compras(r, sort_col_ref[0] or "fecha"), reverse=not sort_asc_ref[0])

                n_pedidos = len(rows)
                total_cotizar = 0.0
                for r in rows:
                    try:
                        cant = float(str(r.get("cantidad") or "0").replace(",", ".")) if r.get("cantidad") else 0
                    except (ValueError, TypeError):
                        cant = 0
                    try:
                        precio = float(str(r.get("precio_sugerido") or "0").replace(",", ".")) if r.get("precio_sugerido") else 0
                    except (ValueError, TypeError):
                        precio = 0
                    total_cotizar += cant * precio

                ui.label("Pedidos").classes("text-xl font-semibold mb-2")
                with ui.card().classes("w-full p-4 bg-grey-2"):
                    with ui.row().classes("w-full gap-6 flex-wrap items-center"):
                        with ui.column().classes("gap-0"):
                            ui.label("Cantidad de pedidos").classes("text-xs text-gray-600")
                            ui.label(str(n_pedidos)).classes("text-lg font-bold text-primary")
                        ui.element("div").classes("w-px h-8 bg-gray-400 shrink-0")
                        with ui.column().classes("gap-0"):
                            ui.label("Total a cotizar").classes("text-xs text-gray-600")
                            total_str = f"{total_cotizar:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                            ui.label(f"$ {total_str}").classes("text-lg font-bold text-primary")
                        ui.element("div").classes("w-px h-8 bg-gray-400 shrink-0")
                        ui.button("Refrescar", on_click=_refrescar).props("flat dense no-caps icon=refresh").classes("text-gray-800 hover:bg-gray-200 rounded px-3")

                with ui.row().classes("w-full gap-2 mb-2 items-center"):
                    ui.select(
                        {"": "Todos", "Cotizar": "Cotizar", "Cotizar+Buscando": "Cotizar+Buscando", "No hay": "No hay", "Buscando": "Buscando", "Comprado": "Comprado"},
                        value=filtro_estado_ref.get("val", "Cotizar+Buscando"),
                        label="Estado",
                        on_change=lambda e: (_refrescar_assign(e, filtro_estado_ref, "val") or _refrescar()),
                    ).classes("w-36").props("dense")
                    ui.select(
                        {"": "Todos", **{c: c or "(sin cliente)" for c in clientes if c}},
                        value=filtro_cliente_ref.get("val", ""),
                        label="Cliente",
                        on_change=lambda e: (_refrescar_assign(e, filtro_cliente_ref, "val") or _refrescar()),
                    ).classes("w-72").props("dense")

                def _refrescar_assign(e, d: Dict, k: str) -> None:
                    d[k] = getattr(e, "value", "") or ""

                with ui.element("table").classes("w-full border-collapse text-sm"):
                    with ui.element("thead"):
                        with ui.element("tr").classes("bg-primary text-white font-semibold text-center"):
                            for col_key, h in [("fecha", "Fecha"), ("marca", "Marca"), ("producto", "Producto"), ("sku", "SKU"), ("cantidad", "Cantidad"), ("precio_sugerido", "Precio sugerido"), ("estado", "Estado"), ("usuario_qb", "Cliente")]:
                                th = ui.element("th").classes("px-2 py-1 border text-center cursor-pointer hover:bg-primary/80")
                                th.on("click", lambda c=col_key: (sort_col_ref.__setitem__(0, c) if sort_col_ref[0] != c else sort_asc_ref.__setitem__(0, not sort_asc_ref[0]), sort_col_ref.__setitem__(0, c), sort_asc_ref.__setitem__(0, True) if sort_col_ref[0] != c else None, _refrescar()))
                                with th:
                                    ui.label(h)
                    with ui.element("tbody"):
                        for r in rows:
                            with ui.element("tr").classes("border-t hover:bg-gray-50"):
                                with ui.element("td").classes("px-2 py-1 border text-center"):
                                    ui.label(_fmt_fecha_compras(r.get("fecha", "")))
                                with ui.element("td").classes("px-2 py-1 border text-center"):
                                    ui.label(r.get("marca", "—"))
                                with ui.element("td").classes("px-2 py-1 border text-center"):
                                    ui.label(r.get("producto", "—"))
                                with ui.element("td").classes("px-2 py-1 border text-center"):
                                    _sku = r.get("sku") or ""
                                    ui.label(_sku if _sku else "—")
                                with ui.element("td").classes("px-2 py-1 border text-center"):
                                    ui.label(str(r.get("cantidad", "—")))
                                with ui.element("td").classes("px-2 py-1 border text-right"):
                                    _ps = r.get("precio_sugerido") or "—"
                                    ui.label(f"u$ {_ps}" if _ps != "—" else "—")
                                with ui.element("td").classes("px-2 py-1 border"):
                                    _est_opts = {"Cotizar": "Cotizar", "No hay": "No hay", "Buscando": "Buscando", "Comprado": "Comprado"}
                                    _est_db = r.get("estado") or ""
                                    _est_actual = "No hay" if _est_db == "" else (_est_db if _est_db in _est_opts else _est_db)
                                    if _est_actual and _est_actual not in _est_opts:
                                        _est_opts[_est_actual] = _est_actual
                                    def _on_estado_pedido(e, rid, uid):
                                        v = getattr(e, "value", "") or ""
                                        estado_guardar = "" if v == "No hay" else v
                                        update_compras_lista_row(rid, uid, estado=estado_guardar)
                                        ui.notify("Estado actualizado", color="positive")
                                        _refrescar()
                                    ui.select(_est_opts, value=_est_actual, on_change=lambda e, rid=r["id"], uid=r.get("user_id") or user["id"]: _on_estado_pedido(e, rid, uid)).classes("w-28").props("dense")
                                with ui.element("td").classes("px-2 py-1 border"):
                                    ui.label(r.get("usuario_qb", "—"))

        _refrescar()


def build_tab_historicos(container) -> None:
    """Pestaña Históricos: buscador de productos en QuickBooks. Escribís una palabra y debajo se muestran todos los productos que la contienen."""
    user = require_login()
    if not user:
        return

    qb_tokens = get_qb_tokens(user["id"])
    if not qb_tokens or not qb_tokens.get("access_token"):
        with container:
            ui.label("Conectá QuickBooks en Configuración para usar el buscador de productos.").classes("text-gray-600")
        return

    with container:
        ui.label("Históricos").classes("text-xl font-semibold mb-4")
        with ui.row().classes("w-full gap-2 items-center"):
            search_input = ui.input("Buscar", placeholder="Escribí una palabra para buscar en QuickBooks...").classes("w-96 max-w-full").props("dense outlined clearable")
            ui.button("Buscar", on_click=lambda: _do_search(), color="primary").props("dense no-caps")
        results_container = ui.column().classes("w-full mt-4")

        def _do_search() -> None:
            txt = (search_input.value or "").strip()
            results_container.clear()
            with results_container:
                if not txt:
                    ui.label("Escribí al menos un carácter para buscar.").classes("text-gray-500 text-sm")
                    return
                ui.spinner(size="lg")
                ui.label("Buscando...").classes("text-gray-600")

            async def _buscar_async() -> None:
                # run.io_bound evita bloquear el event loop y es compatible con Python 3.8
                items, err, total_revisados = await run.io_bound(
                    fetch_qb_items_search, user["id"], txt
                )
                results_container.clear()
                with results_container:
                    if err:
                        ui.label(f"Error: {err}").classes("text-negative text-sm")
                        return
                    if not items:
                        msg = "No se encontraron productos."
                        if total_revisados > 0:
                            msg += f" (Se buscó en {total_revisados} productos de QuickBooks: Name, SKU y Sales Description)"
                        ui.label(msg).classes("text-gray-500 text-sm")
                        return
                    ui.label(f"Se encontraron {len(items)} productos").classes("text-sm font-medium text-gray-700 mb-2")
                    with ui.element("div").classes("w-full overflow-x-auto"):
                        with ui.element("table").classes("w-full border-collapse text-sm"):
                            with ui.element("thead"):
                                with ui.element("tr").classes("bg-primary text-white font-semibold"):
                                    with ui.element("th").classes("px-2 py-2 border text-left"):
                                        ui.label("ID")
                                    with ui.element("th").classes("px-2 py-2 border text-left"):
                                        ui.label("Productos")
                                    with ui.element("th").classes("px-2 py-2 border text-left"):
                                        ui.label("SKU")
                                    with ui.element("th").classes("px-2 py-2 border text-center min-w-[90px]"):
                                        ui.label("Buscar")
                            with ui.element("tbody"):
                                for it in items:
                                    with ui.element("tr").classes("border-t hover:bg-gray-50"):
                                        with ui.element("td").classes("px-2 py-1 border"):
                                            ui.label(str(it.get("id", "—")))
                                        with ui.element("td").classes("px-2 py-1 border"):
                                            ui.label(it.get("producto", it.get("name", "—")))
                                        with ui.element("td").classes("px-2 py-1 border"):
                                            ui.label(it.get("sku") or "—")
                                        with ui.element("td").classes("px-2 py-1 border text-center"):
                                            _uid, _iid = user["id"], it.get("id", "")
                                            _prod, _sku = it.get("producto", it.get("name", "—")), (it.get("sku") or "").strip()

                                            def _abrir_historial(uid, iid, prod, sku):
                                                d = ui.dialog().props("persistent")
                                                with d:
                                                    with ui.card().classes("p-6 min-w-[400px] max-w-[600px] max-h-[80vh] overflow-hidden flex flex-col"):
                                                        cont = ui.column().classes("w-full gap-2 flex-1 min-h-0")
                                                        with cont:
                                                            ui.spinner(size="lg")
                                                            ui.label("Buscando historial...").classes("text-gray-600")
                                                d.open()

                                                async def _cargar(uid=uid, iid=iid, prod=prod, sku=sku, cont=cont, dialog=d):
                                                    hist, err = await run.io_bound(fetch_qb_item_history, uid, iid, sku)
                                                    cont.clear()
                                                    with cont:
                                                        with ui.row().classes("w-full gap-4 mb-4 border-b-2 border-gray-300 pb-3"):
                                                            with ui.column().classes("flex-1 min-w-0 gap-1"):
                                                                ui.label(str(prod)[:80] + ("..." if len(str(prod)) > 80 else "")).classes("text-base font-bold")
                                                                ui.label(f"ID: {iid}").classes("text-sm font-mono text-gray-600")
                                                        if err:
                                                            ui.label(f"Error: {err}").classes("text-negative")
                                                            return
                                                        if not hist:
                                                            ui.label("No se encontraron ventas, compras ni cotizaciones.").classes("text-gray-500")
                                                            return
                                                        with ui.element("div").classes("w-full overflow-x-auto overflow-y-auto").style("max-height: 320px"):
                                                            with ui.element("table").classes("w-full border-collapse text-sm"):
                                                                with ui.element("thead"):
                                                                    with ui.element("tr").classes("bg-primary text-white font-semibold sticky top-0"):
                                                                        for hdr in ["Tipo", "Fecha", "Invoice", "P. venta u$"]:
                                                                            with ui.element("th").classes("px-2 py-1 border"):
                                                                                ui.label(hdr)
                                                                with ui.element("tbody"):
                                                                    for h in hist:
                                                                        with ui.element("tr").classes("border-t hover:bg-gray-50"):
                                                                            with ui.element("td").classes("px-2 py-1 border"):
                                                                                ui.label(h.get("tipo", "—"))
                                                                            with ui.element("td").classes("px-2 py-1 border"):
                                                                                ui.label(h.get("fecha", "—"))
                                                                            with ui.element("td").classes("px-2 py-1 border"):
                                                                                doc_txt = str(h.get("doc", "—"))[:40]
                                                                                qb_id = h.get("qb_id") or ""
                                                                                qb_tipo = h.get("qb_tipo") or ""
                                                                                if qb_tipo == "invoice" and qb_id:
                                                                                    async def _descargar_invoice(uid=uid, inv_id=qb_id, doc=doc_txt):
                                                                                        pdf_bytes, err = await run.io_bound(fetch_qb_invoice_pdf, uid, inv_id)
                                                                                        if err:
                                                                                            ui.notify(f"Error: {err}", color="negative")
                                                                                            return
                                                                                        import tempfile
                                                                                        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
                                                                                            f.write(pdf_bytes)
                                                                                            path = f.name
                                                                                        nombre = f"invoice_{doc}.pdf".replace(" ", "_")[:60]
                                                                                        ui.download(path, nombre)
                                                                                        ui.notify("Descarga iniciada", color="positive")
                                                                                    ui.button(doc_txt, on_click=_descargar_invoice).props("flat dense no-caps").classes("text-primary underline hover:no-underline cursor-pointer p-0 min-w-0 font-normal")
                                                                                else:
                                                                                    ui.label(doc_txt)
                                                                            _p = h.get("precio", 0)
                                                                            _tipo = h.get("tipo", "")
                                                                            _p_fmt = f"{_p:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                                                                            with ui.element("td").classes("px-2 py-1 border text-right"):
                                                                                ui.label(_p_fmt if _tipo == "Venta" else "—")
                                                        with ui.row().classes("w-full justify-end mt-4"):
                                                            ui.button("Cerrar", on_click=dialog.close, color="secondary").props("flat")

                                                background_tasks.create(_cargar(), name="historicos_historial")

                                            ui.button("Buscar", on_click=lambda uid=_uid, iid=_iid, prod=_prod, sku=_sku: _abrir_historial(uid, iid, prod, sku)).props("dense no-caps flat").classes("text-primary hover:bg-primary/10")

            background_tasks.create(_buscar_async(), name="historicos_search")

        search_input.on("keydown.enter", lambda: _do_search())


def build_tab_stock(container) -> None:
    """Pestaña Stock: inventario de QuickBooks (Items con QtyOnHand > 0)."""
    user = require_login()
    if not user:
        return

    qb_creds = get_qb_app_credentials(user["id"])
    qb_tokens = get_qb_tokens(user["id"])

    with container:
        if not qb_creds:
            ui.label(
                "Configurá QuickBooks en Configuración (Client ID, Client Secret, Redirect URI) y conectá tu cuenta."
            ).classes("text-gray-600")
            return

        if not qb_tokens:
            ui.label(
                "Credenciales configuradas. Andá a Configuración → QuickBooks y hacé clic en 'Conectar cuenta' para autorizar."
            ).classes("text-warning")
            return

        header_card = ui.column().classes("w-full mb-2")
        result_area = ui.column().classes("w-full gap-2")
        items_ref: List[Dict[str, Any]] = []
        sort_col_ref: Dict[str, str] = {"val": "producto"}
        sort_asc_ref: Dict[str, bool] = {"val": True}

        with result_area:
            with ui.card().classes("w-full p-8 items-center gap-4"):
                ui.spinner(size="xl")
                ui.label("Cargando stock de QuickBooks...").classes("text-xl text-gray-700")

        def _sort_key_stock(row: Dict[str, Any], col: str) -> Any:
            if col == "id":
                return str(row.get("id", "")).lower()
            if col == "producto":
                return str(row.get("producto", "")).lower()
            if col == "sku":
                return str(row.get("sku", "")).lower()
            if col == "sales_price":
                return row.get("sales_price", 0)
            if col == "qty":
                return row.get("qty", 0)
            return ""

        def _on_sort_stock(col: str) -> None:
            if sort_col_ref.get("val") == col:
                sort_asc_ref["val"] = not sort_asc_ref.get("val", True)
            else:
                sort_col_ref["val"] = col
                sort_asc_ref["val"] = True
            _pintar_tabla()

        def _pintar_tabla() -> None:
            items = items_ref
            sort_col = sort_col_ref.get("val", "producto")
            asc = sort_asc_ref.get("val", True)
            items_sorted = sorted(items, key=lambda x: _sort_key_stock(x, sort_col), reverse=not asc)
            n_skus = len(items)
            total_qty = sum(i.get("qty", 0) for i in items)
            stock_valorizado = sum((i.get("qty", 0) or 0) * (i.get("sales_price", 0) or 0) for i in items)
            header_card.clear()
            with header_card:
                ui.label("Stock").classes("text-xl font-semibold mb-2")
                with ui.card().classes("w-full p-4 bg-grey-2"):
                    with ui.row().classes("w-full gap-6 flex-wrap items-center"):
                        with ui.column().classes("gap-0"):
                            ui.label("Diferentes SKUs").classes("text-xs text-gray-600")
                            ui.label(str(n_skus)).classes("text-lg font-bold text-primary")
                        ui.element("div").classes("w-px h-8 bg-gray-400 shrink-0")
                        with ui.column().classes("gap-0"):
                            ui.label("Stock valorizado").classes("text-xs text-gray-600")
                            _sv_fmt = f"$ {stock_valorizado:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                            ui.label(_sv_fmt).classes("text-lg font-bold text-primary")
            result_area.clear()
            with result_area:
                with ui.element("table").classes("w-full border-collapse text-sm"):
                    with ui.element("thead"):
                        with ui.element("tr").classes("bg-primary text-white font-semibold text-center"):
                            for col_key, h in [("id", "ID"), ("producto", "Producto"), ("sku", "SKU"), ("sales_price", "Precio venta"), ("qty", "Cantidad"), ("buscar", "Buscar")]:
                                th = ui.element("th").classes("px-3 py-2 border cursor-pointer hover:bg-primary/80")
                                if col_key != "buscar":
                                    th.on("click", lambda c=col_key: _on_sort_stock(c))
                                with th:
                                    ui.label(h)
                    with ui.element("tbody"):
                        for it in items_sorted:
                            with ui.element("tr").classes("border-t hover:bg-gray-50"):
                                with ui.element("td").classes("px-3 py-1 border"):
                                    ui.label(str(it.get("id", "—")))
                                with ui.element("td").classes("px-3 py-1 border"):
                                    ui.label(str(it.get("producto", "—")))
                                with ui.element("td").classes("px-3 py-1 border"):
                                    _sku_val = (it.get("sku") or "").strip()
                                    ui.label(_sku_val if _sku_val else "—")
                                with ui.element("td").classes("px-3 py-1 border text-right"):
                                    _sp = it.get("sales_price") or 0
                                    ui.label(f"$ {_sp:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
                                with ui.element("td").classes("px-3 py-1 border font-medium text-center"):
                                    ui.label(f"{it.get('qty', 0):,}".replace(",", "."))
                                with ui.element("td").classes("px-3 py-1 border text-center"):
                                    def _abrir_historial(uid=user["id"], iid=it.get("id", ""), prod=it.get("producto", "—"), sku_val=(it.get("sku") or "").strip()):
                                        dialog = ui.dialog().props("persistent")
                                        with dialog:
                                            with ui.card().classes("p-6 min-w-[400px] max-w-[600px] max-h-[80vh] overflow-hidden flex flex-col"):
                                                hist_container = ui.column().classes("w-full gap-2 flex-1 min-h-0")
                                                with hist_container:
                                                    ui.spinner(size="lg")
                                                    ui.label("Buscando historial...").classes("text-gray-600")
                                        dialog.open()

                                        async def _cargar_y_mostrar():
                                            hist, err = await run.io_bound(fetch_qb_item_history, uid, iid, sku_val)
                                            hist_container.clear()
                                            with hist_container:
                                                with ui.row().classes("w-full gap-4 mb-4 border-b-2 border-gray-300 pb-3"):
                                                    with ui.column().classes("flex-1 min-w-0 gap-1"):
                                                        ui.label(str(prod)[:80] + ("..." if len(str(prod)) > 80 else "")).classes("text-base font-bold")
                                                        ui.label(f"ID: {iid}").classes("text-sm font-mono text-gray-600")
                                                if err:
                                                    ui.label(f"Error: {err}").classes("text-negative")
                                                    return
                                                if not hist:
                                                    ui.label("No se encontraron ventas, compras ni cotizaciones para este producto.").classes("text-gray-500")
                                                    return
                                                with ui.element("div").classes("w-full overflow-x-auto overflow-y-auto").style("max-height: 320px"):
                                                    with ui.element("table").classes("w-full border-collapse text-sm"):
                                                        with ui.element("thead"):
                                                            with ui.element("tr").classes("bg-primary text-white font-semibold sticky top-0"):
                                                                for hdr in ["Tipo", "Fecha", "Invoice", "P. venta u$"]:
                                                                    with ui.element("th").classes("px-2 py-1 border"):
                                                                        ui.label(hdr)
                                                        with ui.element("tbody"):
                                                            for h in hist:
                                                                with ui.element("tr").classes("border-t hover:bg-gray-50"):
                                                                    with ui.element("td").classes("px-2 py-1 border"):
                                                                        ui.label(h.get("tipo", "—"))
                                                                    with ui.element("td").classes("px-2 py-1 border"):
                                                                        ui.label(h.get("fecha", "—"))
                                                                    with ui.element("td").classes("px-2 py-1 border"):
                                                                        doc_txt = str(h.get("doc", "—"))[:40]
                                                                        qb_id = h.get("qb_id") or ""
                                                                        qb_tipo = h.get("qb_tipo") or ""
                                                                        if qb_tipo == "invoice" and qb_id:
                                                                            async def _descargar_inv(uid=uid, inv_id=qb_id, doc=doc_txt):
                                                                                pdf_bytes, err = await run.io_bound(fetch_qb_invoice_pdf, uid, inv_id)
                                                                                if err:
                                                                                    ui.notify(f"Error: {err}", color="negative")
                                                                                    return
                                                                                import tempfile
                                                                                with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
                                                                                    f.write(pdf_bytes)
                                                                                    path = f.name
                                                                                nombre = f"invoice_{doc}.pdf".replace(" ", "_")[:60]
                                                                                ui.download(path, nombre)
                                                                                ui.notify("Descarga iniciada", color="positive")
                                                                            ui.button(doc_txt, on_click=_descargar_inv).props("flat dense no-caps").classes("text-primary underline hover:no-underline cursor-pointer p-0 min-w-0 font-normal")
                                                                        else:
                                                                            ui.label(doc_txt)
                                                                    _p = h.get("precio", 0)
                                                                    _tipo = h.get("tipo", "")
                                                                    _p_fmt = f"{_p:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                                                                    with ui.element("td").classes("px-2 py-1 border text-right"):
                                                                        ui.label(_p_fmt if _tipo == "Venta" else "—")
                                                with ui.row().classes("w-full justify-end mt-4"):
                                                    ui.button("Cerrar", on_click=dialog.close, color="secondary").props("flat")

                                        background_tasks.create(_cargar_y_mostrar(), name="stock_historial")
                                    ui.button("Buscar", on_click=lambda uid=user["id"], iid=it.get("id", ""), prod=it.get("producto", "—"), sku_val=(it.get("sku") or "").strip(): _abrir_historial(uid, iid, prod, sku_val)).props("dense no-caps flat").classes("text-primary hover:bg-primary/10")

        def _cargar() -> None:
            items, err = fetch_qb_items(user["id"])
            if err:
                result_area.clear()
                with result_area:
                    ui.label(f"Error: {err}").classes("text-negative")
                return
            items_ref[:] = [i for i in (items or []) if (i.get("qty") or 0) > 0]
            if not items_ref:
                result_area.clear()
                with result_area:
                    ui.label("No hay items con stock en QuickBooks.").classes("text-gray-500")
                return
            _pintar_tabla()

        async def _cargar_async() -> None:
            await run.io_bound(_cargar)

        background_tasks.create(_cargar_async(), name="cargar_stock_qb")


def build_tab_compras(container) -> None:
    """Pestaña Invoices: conexión a QuickBooks para mostrar Invoices del cliente."""
    user = require_login()
    if not user:
        return

    qb_creds = get_qb_app_credentials(user["id"])
    qb_tokens = get_qb_tokens(user["id"])

    with container:
        if not qb_creds:
            ui.label(
                "Configurá QuickBooks en Configuración (Client ID, Client Secret, Redirect URI) y conectá tu cuenta."
            ).classes("text-gray-600")
            return

        if not qb_tokens:
            ui.label(
                "Credenciales configuradas. Andá a Configuración → QuickBooks y hacé clic en 'Conectar cuenta' para autorizar."
            ).classes("text-warning")
            return

        header_card = ui.column().classes("w-full mb-2")
        filtro_row = ui.row().classes("w-full mb-2 items-center gap-4")
        result_area = ui.column().classes("w-full gap-2")

        with result_area:
            with ui.card().classes("w-full p-8 items-center gap-4"):
                ui.spinner(size="xl")
                ui.label("Cargando invoices...").classes("text-xl text-gray-700")

        invoices_ref: Dict[str, List[Dict[str, Any]]] = {"data": []}
        header_data_ref: Dict[str, Any] = {}
        sort_col_compras: Dict[str, str] = {"val": "txn_date"}
        sort_asc_compras: Dict[str, bool] = {"val": False}

        def _fmt_fecha(s: str) -> str:
            """Convierte YYYY-MM-DD a dd-mm-yyyy."""
            if not s or len(str(s)) < 10:
                return str(s) if s else "—"
            p = str(s)[:10].split("-")
            return f"{p[2]}-{p[1]}-{p[0]}" if len(p) == 3 else str(s)

        def _mostrar_detalle_invoice(inv: Dict[str, Any]) -> None:
            """Abre un popup con el detalle de la factura, cargando desde QuickBooks."""
            dlg = ui.dialog()
            with dlg:
                with ui.card().classes("p-6 min-w-[650px] max-w-[90vw] max-h-[70vh] overflow-hidden flex flex-col"):
                    with ui.row().classes("items-center gap-2 shrink-0"):
                        ui.label("Detalle de la factura").classes("text-lg font-semibold")
                        ui.label("Invoice nro").classes("text-base text-gray-600")
                        ui.label(str(inv.get("doc", "—"))).classes("text-base font-medium")
                    ui.separator().classes("mb-3")
                    cont = ui.column().classes("gap-2 overflow-y-auto min-h-0 flex-1")

            async def _cargar_y_mostrar() -> None:
                with cont:
                    ui.spinner(size="md")
                    ui.label("Cargando detalle...").classes("text-gray-600")
                detail, err = await run.io_bound(fetch_qb_invoice_detail, user["id"], inv.get("id", ""))
                cont.clear()
                with cont:
                    if err:
                        ui.label(f"Error: {err}").classes("text-negative")
                    else:
                        inv_obj = detail or {}
                        def _fmt_dd_mm_aaaa(s: str) -> str:
                            if not s or len(str(s)) < 10:
                                return str(s) if s else "—"
                            parts = str(s)[:10].split("-")
                            return f"{parts[2]}-{parts[1]}-{parts[0]}" if len(parts) == 3 else str(s)
                        doc = inv_obj.get("DocNumber", inv.get("doc", "—"))
                        txn = _fmt_dd_mm_aaaa(str(inv_obj.get("TxnDate", inv.get("txn_date", ""))))
                        due = _fmt_dd_mm_aaaa(str(inv_obj.get("DueDate", inv.get("due_date", ""))))
                        total = inv_obj.get("TotalAmt", inv.get("amount_num"))
                        bal = inv_obj.get("Balance", inv.get("balance"))
                        try:
                            total_fmt = f"{float(total):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if total is not None else "—"
                        except (TypeError, ValueError):
                            total_fmt = str(total) or "—"
                        try:
                            bal_fmt = f"{float(bal):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if bal is not None else "—"
                        except (TypeError, ValueError):
                            bal_fmt = str(bal) or "—"
                        _label_w = "w-28"
                        with ui.element("table").classes("w-full text-sm table-fixed"):
                            with ui.element("colgroup"):
                                ui.element("col").classes(_label_w)
                                ui.element("col")
                            for lbl, val in [("Fecha", txn), ("Vencimiento", due)]:
                                with ui.element("tr"):
                                    with ui.element("td").classes(f"font-semibold pr-4 py-1 {_label_w}"):
                                        ui.label(lbl)
                                    with ui.element("td").classes("py-1"):
                                        ui.label(str(val))
                        ui.element("div").classes("border-t border-gray-300 my-2")
                        with ui.element("table").classes("w-full text-sm table-fixed"):
                            with ui.element("colgroup"):
                                ui.element("col").classes(_label_w)
                                ui.element("col")
                            for lbl, val in [
                                ("Total", f"u$ {total_fmt}" if total_fmt != "—" else "—"),
                                ("Saldo", f"u$ {bal_fmt}" if bal_fmt != "—" else "—"),
                            ]:
                                with ui.element("tr"):
                                    with ui.element("td").classes(f"font-semibold pr-4 py-1 {_label_w}"):
                                        ui.label(lbl)
                                    with ui.element("td").classes("py-1"):
                                        ui.label(str(val))
                        ui.separator().classes("my-2")
                        lines = inv_obj.get("Line") or []
                        if isinstance(lines, dict):
                            lines = [lines]
                        if lines:
                            ui.label("Ítems").classes("font-semibold mt-3 mb-1")
                            with ui.element("table").classes("w-full text-sm border"):
                                with ui.element("thead"):
                                    with ui.element("tr").classes("bg-gray-100"):
                                        with ui.element("th").classes("px-2 py-1 text-left"):
                                            ui.label("Descripción")
                                        with ui.element("th").classes("px-2 py-1 text-right"):
                                            ui.label("Cant.")
                                        with ui.element("th").classes("px-2 py-1 text-right"):
                                            ui.label("Importe")
                                with ui.element("tbody"):
                                    for idx, lin in enumerate(lines):
                                        sales = lin.get("SalesItemLineDetail") or {}
                                        desc = lin.get("Description", sales.get("ItemRef", {}).get("name", "—") if isinstance(sales, dict) else "—")
                                        if idx == len(lines) - 1 and (str(desc).strip() in ("-", "—", "")):
                                            desc = "Total"
                                        qty = sales.get("Qty", 1) if isinstance(sales, dict) else 1
                                        amt = lin.get("Amount", 0)
                                        try:
                                            amt_str = f"{float(amt):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                                        except (TypeError, ValueError):
                                            amt_str = str(amt)
                                        qty_display = "" if desc == "Total" else str(qty)
                                        with ui.element("tr").classes("border-t"):
                                            with ui.element("td").classes("px-2 py-1"):
                                                ui.label(str(desc)[:80])
                                            with ui.element("td").classes("px-2 py-1 text-right"):
                                                ui.label(qty_display)
                                            with ui.element("td").classes("px-2 py-1 text-right"):
                                                ui.label(f"u$ {amt_str}")

                    with ui.row().classes("mt-3 gap-2"):
                        def _descargar_pdf() -> None:
                            pdf_bytes, err = fetch_qb_invoice_pdf(user["id"], inv.get("id", ""))
                            if err:
                                ui.notify(f"Error al descargar PDF: {err}", color="negative")
                                return
                            if not pdf_bytes:
                                ui.notify("No se pudo obtener el PDF", color="warning")
                                return
                            doc_num = inv.get("doc", "invoice")
                            try:
                                fd, path = tempfile.mkstemp(suffix=".pdf")
                                os.write(fd, pdf_bytes)
                                os.close(fd)
                                nombre = f"invoice_{doc_num}.pdf"
                                with dlg:
                                    ui.download(path, nombre)
                                ui.notify("Descarga iniciada", color="positive")
                            except Exception as ex:
                                ui.notify(f"Error: {ex}", color="negative")

                        ui.button("Cerrar popup", on_click=dlg.close).props("dense no-caps")
                        ui.button("Descargar invoice", on_click=_descargar_pdf, color="secondary").props("dense no-caps icon=download")

            dlg.open()
            background_tasks.create(_cargar_y_mostrar(), name="invoice_detail")

        def _generar_excel_invoices() -> tuple[Optional[str], Optional[str], Optional[str]]:
            """Genera el Excel en un hilo. Retorna (path, nombre_archivo, None) si OK, o (None, None, error_msg)."""
            try:
                invs = invoices_ref.get("data", [])
                filtro_val = filtro_status_ref.get("val", "Abierta+Vencida")
                if filtro_val == "Abierta+Vencida":
                    invs = [i for i in invs if (i.get("status") or "").lower() in ("abierta", "vencida")]
                elif filtro_val != "Todas":
                    invs = [i for i in invs if (i.get("status") or "").lower() == filtro_val.lower()]
                if filtro_estado_ref.get("val", "Todos") != "Todos":
                    invs = [i for i in invs if (i.get("estado") or "En USA") == filtro_estado_ref["val"]]
                sc = sort_col_compras.get("val", "txn_date")
                asc = sort_asc_compras.get("val", False)

                def _sk(x: Dict, col: str):
                    v = x.get(col) or ""
                    if col in ("amount", "amount_num"):
                        try:
                            return float(x.get("amount_num") or 0)
                        except (ValueError, TypeError):
                            return 0
                    return str(v).lower()

                invs_exp = sorted(invs, key=lambda x: _sk(x, sc), reverse=not asc)
                if not invs_exp:
                    return (None, None, "No hay invoices para exportar")
                cust_name = (header_data_ref.get("cust_name") or "Cliente").replace("/", "-").replace("\\", "-")[:50]
                ahora = datetime.now()
                nombre_archivo = f"{cust_name}-{ahora.year:04d}-{ahora.month:02d}-{ahora.day:02d}-{ahora.hour:02d}{ahora.minute:02d}.xlsx"
                sheet_name = f"Invoices {ahora.day:02d}-{ahora.month:02d}-{ahora.year % 100:02d}"[:31]

                wb = Workbook()
                ws = wb.active
                ws.title = sheet_name
                black_fill = PatternFill(start_color="000000", end_color="000000", fill_type="solid")
                white_fill = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid")
                header_font = Font(color="FFFFFF", bold=True)
                thin_side = Side(border_style="thin")
                all_borders = Border(left=thin_side, right=thin_side, top=thin_side, bottom=thin_side)

                headers = ["Fecha", "Días", "Invoice", "Importe", "Status"]
                for col, h in enumerate(headers, 1):
                    c = ws.cell(row=1, column=col, value=h)
                    c.fill = black_fill
                    c.font = header_font
                    c.border = all_borders
                    c.alignment = Alignment(horizontal="center", vertical="center")

                today_date = datetime.now().date()
                for idx, inv in enumerate(invs_exp, 2):
                    txn = inv.get("txn_date", "") or ""
                    fecha_ddmm = _fmt_fecha(txn) if txn else "—"
                    try:
                        dt = datetime.strptime(str(txn)[:10], "%Y-%m-%d").date() if len(str(txn)) >= 10 else None
                        dias_val = (today_date - dt).days if dt else None
                    except (ValueError, TypeError):
                        dias_val = None
                    doc_raw = inv.get("doc", "") or ""
                    try:
                        invoice_num = int(str(doc_raw).strip()) if str(doc_raw).strip().isdigit() else None
                    except (ValueError, TypeError):
                        invoice_num = None
                    amt = inv.get("amount_num", 0) or 0
                    try:
                        importe_num = float(amt)
                    except (ValueError, TypeError):
                        importe_num = 0.0
                    status = inv.get("status", "—")

                    c_fecha = ws.cell(row=idx, column=1, value=fecha_ddmm)
                    c_fecha.fill = white_fill
                    c_fecha.border = all_borders

                    c_dias = ws.cell(row=idx, column=2, value=dias_val)
                    c_dias.fill = white_fill
                    c_dias.border = all_borders
                    c_dias.number_format = "0"

                    c_inv = ws.cell(row=idx, column=3, value=invoice_num if invoice_num is not None else doc_raw or "—")
                    c_inv.fill = white_fill
                    c_inv.border = all_borders
                    if invoice_num is not None:
                        c_inv.number_format = "0"

                    c_imp = ws.cell(row=idx, column=4, value=importe_num)
                    c_imp.fill = white_fill
                    c_imp.border = all_borders
                    c_imp.number_format = '$#,##0.00'

                    c_status = ws.cell(row=idx, column=5, value=status)
                    c_status.fill = white_fill
                    c_status.border = all_borders

                ws.column_dimensions["A"].width = 12
                ws.column_dimensions["B"].width = 8
                ws.column_dimensions["C"].width = 14
                ws.column_dimensions["D"].width = 14
                ws.column_dimensions["E"].width = 12

                fd, path = tempfile.mkstemp(suffix=".xlsx")
                os.close(fd)
                wb.save(path)
                return (path, nombre_archivo, None)
            except Exception as e:
                return (None, None, str(e))

        def _pintar_compras() -> None:
            header_card.clear()
            filtro_row.clear()
            result_area.clear()
            invs = invoices_ref.get("data", [])
            filtro_val = filtro_status_ref.get("val", "Abierta+Vencida")
            if filtro_val == "Abierta+Vencida":
                invs = [i for i in invs if (i.get("status") or "").lower() in ("abierta", "vencida")]
            elif filtro_val != "Todas":
                invs = [i for i in invs if (i.get("status") or "").lower() == filtro_val.lower()]
            filtro_estado_val = filtro_estado_ref.get("val", "Todos")
            if filtro_estado_val != "Todos":
                invs = [i for i in invs if (i.get("estado") or "En USA") == filtro_estado_val]
            sc = sort_col_compras.get("val", "txn_date")
            asc = sort_asc_compras.get("val", False)

            def _sort_key(x: Dict, col: str):
                v = x.get(col) or ""
                if col in ("amount", "amount_num"):
                    n = x.get("amount_num")
                    try:
                        return float(n) if n is not None else 0
                    except (ValueError, TypeError):
                        return 0
                return str(v).lower()

            invs_sorted = sorted(invs, key=lambda x: _sort_key(x, sc), reverse=not asc)
            total_importe = sum(i.get("amount_num", 0) for i in invs_sorted)
            total_fmt = f"{total_importe:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            header_data_ref["total_importe"] = f"u$ {total_fmt}"

            # Calcular días por invoice (para métricas de deuda)
            today_date = datetime.now().date()
            dias_list: List[int] = []
            for inv in invs_sorted:
                txn = inv.get("txn_date", "") or ""
                try:
                    if len(str(txn)) >= 10:
                        dt = datetime.strptime(str(txn)[:10], "%Y-%m-%d").date()
                        dias_list.append((today_date - dt).days)
                except (ValueError, TypeError):
                    pass
            deuda_promedio_dias = f"{sum(dias_list) / len(dias_list):.1f}" if dias_list else "—"
            deuda_mas_antigua = str(max(dias_list)) if dias_list else "—"
            cantidad_ordenes = len(invs_sorted)

            with header_card:
                if not header_data_ref:
                    ui.label("Cargando...").classes("text-gray-600")
                    return
                ui.label("Invoices").classes("text-xl font-semibold text-gray-800 mb-2")
                open_balance = header_data_ref.get("open_balance", "—")
                overdue = header_data_ref.get("overdue", "0.00")
                with ui.card().classes("w-full p-4 bg-grey-2"):
                    with ui.row().classes("w-full gap-6 flex-wrap items-center"):
                        with ui.column().classes("gap-0"):
                            ui.label("Total Deuda").classes("text-base font-semibold text-gray-800")
                            ui.label(open_balance).classes("text-sm text-gray-600")
                        ui.element("div").classes("w-px h-8 bg-gray-400 shrink-0")
                        with ui.column().classes("gap-0"):
                            ui.label("Deuda Vencida").classes("text-base font-semibold text-gray-800")
                            ui.label(overdue).classes("text-sm text-gray-600")
                        ui.element("div").classes("w-px h-8 bg-gray-400 shrink-0")
                        with ui.column().classes("gap-0"):
                            ui.label("Deuda Seleccionada").classes("text-base font-semibold text-gray-800")
                            ui.label(header_data_ref.get("total_importe", "u$ 0,00")).classes("text-sm text-gray-600")
                        ui.element("div").classes("w-px h-8 bg-gray-400 shrink-0")
                        with ui.column().classes("gap-0"):
                            ui.label("Deuda promedio días").classes("text-base font-semibold text-gray-800")
                            ui.label(deuda_promedio_dias).classes("text-sm text-gray-600")
                        ui.element("div").classes("w-px h-8 bg-gray-400 shrink-0")
                        with ui.column().classes("gap-0"):
                            ui.label("Deuda más antigua").classes("text-base font-semibold text-gray-800")
                            ui.label(deuda_mas_antigua).classes("text-sm text-gray-600")
                        ui.element("div").classes("w-px h-8 bg-gray-400 shrink-0")
                        with ui.column().classes("gap-0"):
                            ui.label("Órdenes Impagas").classes("text-base font-semibold text-gray-800")
                            ui.label(str(cantidad_ordenes)).classes("text-sm text-gray-600")
            with filtro_row:
                filtro_status_val = filtro_status_ref.get("val", "Abierta+Vencida")
                ui.label("Status:").classes("text-sm font-semibold text-gray-800")
                filtro_status = ui.select(
                    {"Abierta+Vencida": "Abierta+Vencida", "Todas": "Todas", "Abierta": "Abierta", "Vencida": "Vencida", "Pagada": "Pagada"},
                    value=filtro_status_val,
                    label="",
                ).classes("w-44").props("dense outlined")
                def _on_filtro_change(e):
                    val = getattr(e, "args", None) or getattr(e, "value", None)
                    filtro_status_ref["val"] = str(val) if val is not None else "Abierta+Vencida"
                    _pintar_compras()
                filtro_status.on_value_change(_on_filtro_change)
                ui.label("Estado:").classes("text-sm font-semibold text-gray-800 ml-4")
                filtro_estado = ui.select(
                    {"Todos": "Todos", "En USA": "En USA", "Viajando": "Viajando", "Recibida": "Recibida"},
                    value=filtro_estado_ref.get("val", "Todos"),
                    label="",
                ).classes("w-40").props("dense outlined")
                def _on_filtro_estado_change(e):
                    val = getattr(e, "args", None) or getattr(e, "value", None)
                    filtro_estado_ref["val"] = str(val) if val is not None else "Todos"
                    _pintar_compras()
                filtro_estado.on_value_change(_on_filtro_estado_change)
                async def _imprimir_invoices_async() -> None:
                    path, nombre_archivo, err = await run.io_bound(_generar_excel_invoices)
                    if err:
                        ui.notify(err, color="warning" if "No hay" in (err or "") else "negative")
                        return
                    if path and nombre_archivo:
                        ui.download(path, nombre_archivo)
                        ui.notify(f"Exportado: {nombre_archivo}", color="positive")
                        def _cleanup() -> None:
                            try:
                                if path and os.path.exists(path):
                                    os.unlink(path)
                            except Exception:
                                pass
                        ui.timer(5.0, _cleanup, once=True)
                ui.button("Imprimir invoices", on_click=_imprimir_invoices_async, color="primary").props("dense no-caps icon=print").classes("ml-4")

            with result_area:
                def _on_sort(c: str) -> None:
                    if sort_col_compras.get("val") == c:
                        sort_asc_compras["val"] = not sort_asc_compras.get("val", False)
                    else:
                        sort_col_compras["val"] = c
                        sort_asc_compras["val"] = False
                    _pintar_compras()

                def _save_inv_extra(inv: Dict, **kwargs) -> None:
                    qid = inv.get("id", "")
                    if not qid:
                        return
                    upsert_invoice_extra(user["id"], qid, **kwargs)
                    for k, v in kwargs.items():
                        inv[k] = str(v) if v is not None else ""
                    ui.notify("Guardado", color="positive")

                if not invs_sorted:
                    ui.label("No hay facturas." if not invs else "No hay facturas con ese Status.").classes("text-gray-500")
                else:
                    with ui.element("div").classes("w-full overflow-x-auto"):
                        with ui.element("table").classes("w-full border-collapse text-sm"):
                            with ui.element("thead"):
                                with ui.element("tr").classes("bg-primary text-white font-semibold"):
                                    estado_opts = {"En USA": "En USA", "Viajando": "Viajando", "Recibida": "Recibida"}
                                    cols = [("numero", "Numero"), ("txn_date", "Fecha"), ("dias", "Días"), ("tipo", "Tipo"), ("doc", "Nº"), ("guia", "Guía"), ("despachante", "Despachante"), ("importe_factura", "Importe factura"), ("pa", "PA"), ("estado", "Estado"), ("amount", "Importe"), ("status", "Status")]
                                    for col_key, h in cols:
                                        with ui.element("th").classes("px-2 py-2 border text-center"):
                                            if col_key in ("numero", "dias"):
                                                ui.label(h)
                                            else:
                                                ui.button(h, on_click=lambda c=col_key: _on_sort(c)).props("flat dense no-caps").classes("text-white hover:bg-white/20 cursor-pointer font-semibold")
                            with ui.element("tbody"):
                                for idx, inv in enumerate(invs_sorted, 1):
                                    row_el = ui.element("tr").classes("border-t border-gray-200 hover:bg-gray-50")
                                    with row_el:
                                        with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                            ui.label(str(idx))
                                        with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                            ui.label(_fmt_fecha(inv.get("txn_date", "—")))
                                        with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                            txn = inv.get("txn_date", "") or ""
                                            try:
                                                dt = datetime.strptime(str(txn)[:10], "%Y-%m-%d").date() if len(str(txn)) >= 10 else None
                                                dias = (datetime.now().date() - dt).days if dt else None
                                            except (ValueError, TypeError):
                                                dias = None
                                            dias_val = str(dias) if dias is not None else "—"
                                            dias_cls = "text-red-600 font-semibold" if dias is not None and dias > 30 else "text-gray-900"
                                            ui.label(dias_val).classes(dias_cls)
                                        with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                            ui.label(inv.get("tipo", "Factura"))
                                        with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                            with ui.row().classes("gap-1 items-center"):
                                                ui.label(inv.get("doc", "—"))
                                                ui.button("Ver", on_click=lambda inv=inv: _mostrar_detalle_invoice(inv)).props("flat dense size=sm")
                                        with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                            inp_guia = ui.input(value=inv.get("guia", "")).classes("w-40").props("dense")
                                            inp_guia.on("blur", lambda evt, inv=inv, inp=inp_guia: _save_inv_extra(inv, guia=inp.value))
                                            inp_guia.on("keydown.enter", lambda evt, inv=inv, inp=inp_guia: _save_inv_extra(inv, guia=inp.value))
                                        with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                            despachantes_list = get_despachantes()
                                            desp_opts = {"": "(otro)"}
                                            desp_opts.update({d["nombre"]: d["nombre"] for d in despachantes_list})
                                            desp_actual = inv.get("despachante", "") or ""
                                            if desp_actual and desp_actual not in desp_opts:
                                                desp_opts[desp_actual] = desp_actual
                                            ui.select(desp_opts, value=desp_actual or None, on_change=lambda e, inv=inv: _save_inv_extra(inv, despachante=e.value or "")).classes("w-40").props("dense")
                                        with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                            def _fmt_importe_factura(val):
                                                if not val:
                                                    return ""
                                                try:
                                                    s = str(val).replace(" ", "")
                                                    if "," in s:
                                                        s = s.replace(".", "").replace(",", ".")
                                                    n = float(s)
                                                    return f"{n:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                                                except (ValueError, TypeError):
                                                    return str(val)
                                            def _parse_imp(s):
                                                if not s:
                                                    return ""
                                                return str(s).replace(".", "").replace(",", ".").strip()
                                            _imp_val = _fmt_importe_factura(inv.get("importe_factura", ""))
                                            with ui.row().classes("items-center justify-center gap-0.5"):
                                                ui.label("$").classes("text-gray-600 text-sm")
                                                inp_imp = ui.input(value=_imp_val).classes("w-28").props("dense")
                                            inp_imp.on("blur", lambda evt, inv=inv, inp=inp_imp: _save_inv_extra(inv, importe_factura=_parse_imp(inp.value)))
                                            inp_imp.on("keydown.enter", lambda evt, inv=inv, inp=inp_imp: _save_inv_extra(inv, importe_factura=_parse_imp(inp.value)))
                                        with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                            def _fmt_pa(val):
                                                if not val:
                                                    return ""
                                                try:
                                                    s = str(val).replace(" ", "")
                                                    if "," in s:
                                                        s = s.replace(".", "").replace(",", ".")
                                                    n = float(s)
                                                    return f"{n:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                                                except (ValueError, TypeError):
                                                    return str(val)
                                            def _parse_pa(s):
                                                if not s:
                                                    return ""
                                                return str(s).replace(".", "").replace(",", ".").strip()
                                            _pa_val = _fmt_pa(inv.get("pa", ""))
                                            with ui.row().classes("items-center justify-center gap-0.5"):
                                                ui.label("u$").classes("text-gray-600 text-sm")
                                                inp_pa = ui.input(value=_pa_val, placeholder="u$").classes("w-24").props("dense")
                                            inp_pa.on("blur", lambda evt, inv=inv, inp=inp_pa: _save_inv_extra(inv, pa=_parse_pa(inp.value)))
                                            inp_pa.on("keydown.enter", lambda evt, inv=inv, inp=inp_pa: _save_inv_extra(inv, pa=_parse_pa(inp.value)))
                                        with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                            with ui.row().classes("justify-center w-full"):
                                                est_display = inv.get("estado", "") or "En USA"
                                                ui.select(estado_opts, value=est_display or "En USA", on_change=lambda e, inv=inv: _save_inv_extra(inv, estado=e.value)).classes("w-28").props("dense")
                                        with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-right"):
                                            amt_num = inv.get("amount_num", 0) or 0
                                            amt_str = f"{float(amt_num):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                                            ui.label(f"u$ {amt_str}")
                                        with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                            ui.label(inv.get("status", "—"))

        filtro_status_ref: Dict[str, str] = {"val": "Abierta+Vencida"}
        filtro_estado_ref: Dict[str, str] = {"val": "Todos"}

        def _cargar_compras() -> None:
            qb_cust = get_user_qb_customer(user["id"])
            cust_id = qb_cust["id"] if qb_cust else None
            cust_name = qb_cust["name"] if qb_cust else None
            if not cust_id:
                header_data_ref.clear()
                header_card.clear()
                with header_card:
                    ui.label("⚠️ Seleccioná tu cliente en Configuración → QuickBooks → 'Soy el cliente'.").classes("text-warning p-4")
                return
            cust_detail, _ = fetch_qb_customer_detail(user["id"], cust_id)
            inv_result, err_inv = fetch_qb_invoices(user["id"], cust_id)
            if err_inv:
                header_data_ref.clear()
                header_card.clear()
                with header_card:
                    ui.label(f"Error al cargar facturas: {err_inv}").classes("text-negative p-4")
                    if "403" in str(err_inv) and ("3100" in str(err_inv) or "AuthorizationFailed" in str(err_inv)):
                        ui.label(
                            "Sugerencia (error 403/3100):\n"
                            "• Verificá que las credenciales (Client ID, Secret) sean de Producción en developer.intuit.com y que la app esté autorizada.\n"
                            "• Desvincular → Conectar cuenta para obtener nuevos tokens."
                        ).classes("text-sm text-gray-700 mt-2 p-3 bg-gray-100 rounded whitespace-pre-line")
                return
            invoices, overdue_total = inv_result
            open_balance = ""
            if cust_detail:
                bal = cust_detail.get("Balance") or cust_detail.get("BalanceWithJobs")
                if bal is not None:
                    try:
                        open_balance = f"{float(bal):,.2f}"
                    except (TypeError, ValueError):
                        open_balance = str(bal)
            if not open_balance and invoices:
                total_bal = sum(float(inv.get("balance", 0) or 0) for inv in invoices)
                open_balance = f"{total_bal:,.2f}"
            overdue = f"{overdue_total:,.2f}" if overdue_total else "0.00"
            qb_cust_display = (cust_detail.get("DisplayName") or cust_detail.get("FullyQualifiedName") or cust_detail.get("CompanyName") or "").strip() if cust_detail else ""
            header_data_ref["cust_name"] = qb_cust_display or cust_name or cust_id
            header_data_ref["open_balance"] = open_balance or "—"
            header_data_ref["overdue"] = overdue
            inv_list = [
                {"id": inv.get("id", ""), "txn_date": inv.get("txn_date", ""), "due_date": inv.get("due_date", ""), "tipo": inv.get("tipo", "Factura"), "doc": inv.get("doc", ""), "amount": inv.get("amount", ""), "amount_num": inv.get("amount_num", 0), "balance": inv.get("balance", 0), "status": inv.get("status", "")}
                for inv in invoices
            ]
            qb_ids = [inv["id"] for inv in inv_list if inv.get("id")]
            extras = get_invoice_extras(user["id"], qb_ids) if qb_ids else {}
            for inv in inv_list:
                qid = inv.get("id", "")
                ex = extras.get(qid, {})
                inv["guia"] = ex.get("guia") or ""
                inv["despachante"] = ex.get("despachante") or ""
                inv["importe_factura"] = ex.get("importe_factura") or ""
                inv["pa"] = ex.get("pa") or ""
                est = ex.get("estado") or ""
                status_low = (inv.get("status") or "").lower()
                if status_low == "pagada":
                    inv["estado"] = est if est else "Recibida"
                    if not est:
                        upsert_invoice_extra(user["id"], qid, estado="Recibida")
                else:
                    inv["estado"] = est if est else "En USA"
                    if not est:
                        upsert_invoice_extra(user["id"], qid, estado="En USA")
            invoices_ref["data"] = inv_list
            _pintar_compras()

        ui.timer(0.3, _cargar_compras, once=True)


def build_tab_busqueda() -> None:
    """Pestaña Búsqueda: texto + botón, resultados en tabla (nombre, precio, vendedor, stock, tipo)."""
    user = require_login()
    if not user:
        return

    access_token = get_ml_access_token(user["id"])

    with ui.column().classes("w-full gap-4"):
        ui.label("Búsqueda en MercadoLibre").classes("text-xl font-semibold")
        with ui.row().classes("items-center gap-3"):
            input_busqueda = ui.input(
                "Texto o ID de publicación (ej: MLA1996852282)"
            ).classes("w-96").props("outlined dense")
            input_busqueda.on("keydown.enter", lambda: on_buscar())

            def on_buscar() -> None:
                background_tasks.create(_buscar_async(), name="busqueda")

            def on_borrar() -> None:
                results_container.clear()
                input_busqueda.value = ""
                solo_propias_switch.value = True
                solo_activas_stock_switch.value = True

            ui.button("Buscar", on_click=on_buscar, color="primary")
            ui.button("Borrar", on_click=on_borrar, color="secondary")
        with ui.row().classes("items-center gap-4"):
            solo_propias_switch = ui.checkbox("Solo publicaciones propias (no catálogo)", value=True).classes("text-sm")
            solo_activas_stock_switch = ui.checkbox("Solo activas con stock", value=True).classes("text-sm")
        results_container = ui.column().classes("w-full mt-2")

        def _norm_busqueda(r: dict, from_catalog: bool) -> dict:
            seller = r.get("seller") or {}
            seller_id = str(
                r.get("seller_id") or r.get("sellerId")
                or (seller.get("id") if isinstance(seller, dict) else None)
                or ""
            ).strip()
            seller_nick = (seller.get("nickname") or "").strip() if isinstance(seller, dict) else ""
            seller_display = seller_nick or (f"ID {seller_id}" if seller_id else "—")
            catalog = from_catalog or r.get("catalog_listing") is True or bool(r.get("catalog_product_id"))
            tipo = "Catálogo" if catalog else "Propia"
            price = r.get("price") or r.get("base_price")
            if price is None:
                prices = r.get("prices")
                if isinstance(prices, dict):
                    price = prices.get("amount") or prices.get("current_price")
                elif isinstance(prices, list) and prices and isinstance(prices[0], dict):
                    price = prices[0].get("amount") or prices[0].get("current_price")
                if price is None:
                    price = r.get("sale_price") or r.get("original_price")
            try:
                price = float(price) if price is not None else None
            except (TypeError, ValueError):
                price = None
            qty_raw = r.get("available_quantity") if r.get("available_quantity") is not None else r.get("availableQuantity") or r.get("quantity")
            if qty_raw is None:
                qty_display, qty_num = "—", 0
            elif isinstance(qty_raw, str):
                qty_display = qty_raw
                # API pública puede devolver rangos: RANGO_1_50, RANGO_51_100, etc.
                if qty_raw.startswith("RANGO_"):
                    try:
                        parts = qty_raw.replace("RANGO_", "").split("_")
                        qty_num = int(parts[0]) if parts else 0
                    except (ValueError, IndexError):
                        qty_num = 0
                else:
                    try:
                        qty_num = int(qty_raw)
                    except ValueError:
                        qty_num = 0
            else:
                try:
                    qty_num = int(qty_raw)
                    qty_display = str(qty_num)
                except (TypeError, ValueError):
                    qty_display, qty_num = "—", 0
            perm = (r.get("permalink") or "").strip()
            if not perm or perm == "#":
                wid = str(r.get("id") or r.get("product_id") or r.get("item_id") or "").strip()
                if wid:
                    perm = f"https://www.mercadolibre.com.ar/p/{wid}" if catalog else f"https://articulo.mercadolibre.com.ar/{wid}-_JM"
            return {
                "title": (r.get("title") or r.get("name") or "").strip(),
                "tipo": tipo,
                "price": price if price is not None else 999999999,
                "price_display": f"$ {int(price):,}".replace(",", ".") if price is not None else "—",
                "available_quantity": qty_num,
                "available_quantity_display": qty_display,
                "seller": seller_display,
                "permalink": perm or "#",
                "status": (r.get("status") or "").strip().lower(),
                "has_item_data": r.get("has_item_data", False),
                "has_active_listing": r.get("has_active_listing", True),
            }

        def _looks_like_ml_item_id(s: str) -> bool:
            """Detecta IDs tipo MLA1996852282 (3 letras + dígitos)."""
            s = s.strip().upper()
            return len(s) >= 10 and s[:3].isalpha() and s[3:].isdigit()

        async def _buscar_async() -> None:
            texto = (input_busqueda.value or "").strip()
            if not texto:
                ui.notify("Ingresá un texto o ID de publicación", color="warning")
                return
            # Si el usuario pega una URL de la API (ej: GET https://api.mercadolibre.com/items/MLA.../sale_price?context=...)
            if "api.mercadolibre.com" in texto.lower():
                metodo = "GET"
                url = texto
                if texto.upper().startswith("GET "):
                    metodo = "GET"
                    url = texto[4:].strip()
                elif texto.upper().startswith("POST "):
                    metodo = "POST"
                    url = texto[5:].strip()
                if not url.startswith("http"):
                    url = "https://" + url.lstrip("/")
                if url.startswith("http"):
                    results_container.clear()
                    with results_container:
                        ui.spinner(size="lg")
                        ui.label(f"Consultando {metodo} {url[:80]}...").classes("text-gray-600")
                    try:
                        def _fetch_api() -> Dict[str, Any]:
                            headers = {"Accept": "application/json"}
                            if access_token:
                                headers["Authorization"] = f"Bearer {access_token}"
                            if metodo.upper() == "GET":
                                r = requests.get(url, headers=headers, timeout=15)
                            else:
                                r = requests.request(metodo.upper(), url, headers=headers, timeout=15)
                            try:
                                return {"status": r.status_code, "body": r.json()}
                            except Exception:
                                return {"status": r.status_code, "body": r.text}
                        resp = await run.io_bound(_fetch_api)
                        results_container.clear()
                        with results_container:
                            ui.label(f"Respuesta ({resp.get('status', '—')})").classes("text-base font-semibold mb-2")
                            body = resp.get("body")
                            if isinstance(body, dict):
                                json_str = json.dumps(body, indent=2, ensure_ascii=False)
                            else:
                                json_str = str(body)
                            ui.html(
                                f'<pre class="p-4 bg-grey-2 rounded overflow-auto text-sm border" style="max-height: 500px;">{html.escape(json_str)}</pre>'
                            )
                            def _copiar_click(datos: str):
                                esc = json.dumps(datos)
                                ui.run_javascript(f'''
                                    (function() {{
                                        var texto = {esc};
                                        var done = function() {{ try {{ window.__copiadoOk = true; }} catch(e) {{}} }};
                                        if (navigator.clipboard && navigator.clipboard.writeText) {{
                                            navigator.clipboard.writeText(texto).then(done).catch(function() {{
                                                var ta = document.createElement("textarea");
                                                ta.value = texto;
                                                ta.style.position = "fixed";
                                                ta.style.left = "-9999px";
                                                document.body.appendChild(ta);
                                                ta.select();
                                                ta.setSelectionRange(0, 999999);
                                                try {{ document.execCommand("copy"); }} catch(e) {{}}
                                                document.body.removeChild(ta);
                                                done();
                                            }});
                                        }} else {{
                                            var ta = document.createElement("textarea");
                                            ta.value = texto;
                                            ta.style.position = "fixed";
                                            ta.style.left = "-9999px";
                                            document.body.appendChild(ta);
                                            ta.select();
                                            ta.setSelectionRange(0, 999999);
                                            try {{ document.execCommand("copy"); }} catch(e) {{}}
                                            document.body.removeChild(ta);
                                            done();
                                        }}
                                    }})();
                                ''')
                                ui.notify("Copiado al portapapeles", type="positive")
                            ui.button("Copiar respuesta", on_click=lambda d=json_str: _copiar_click(d), color="secondary").classes("mt-2").props("no-caps unelevated")
                    except Exception as err:
                        results_container.clear()
                        with results_container:
                            ui.label(f"Error: {err}").classes("text-negative")
                    return
            # Si el usuario ingresa solo números, intentar primero con MLA adelante
            texto_buscar = "MLA" + texto if texto.isdigit() else texto
            texto_fallback = texto if texto.isdigit() else None  # Para reintentar sin MLA si no hay resultados
            results_container.clear()
            with results_container:
                ui.spinner(size="lg")
                ui.label("Buscando en MercadoLibre...").classes("text-gray-600")
            # Si parece ID de publicación (ej MLA1996852282), obtener por ID; si no existe, buscar
            es_item_id = _looks_like_ml_item_id(texto_buscar)
            raw_item = None
            if es_item_id:
                try:
                    raw_item = await run.io_bound(ml_get_item, access_token, texto_buscar)
                except Exception:
                    raw_item = None
                if raw_item is not None:
                    mi_seller_id = None
                    if access_token:
                        try:
                            profile = await run.io_bound(ml_get_user_profile, access_token)
                            mi_seller_id = str((profile or {}).get("id") or "")
                        except Exception:
                            pass
                    seller_id = str(raw_item.get("seller_id") or "")
                    es_propia = mi_seller_id and seller_id and mi_seller_id == seller_id
                    results_container.clear()
                    with results_container:
                        lbl_tipo = "Tu publicación" if es_propia else "Publicación de otro vendedor"
                        ui.label(f"Datos que devuelve MercadoLibre para esta publicación ({lbl_tipo}):").classes(
                            "text-base font-semibold mb-2"
                        )
                        json_str = json.dumps(raw_item, indent=2, ensure_ascii=False)
                        ui.html(
                            f'<pre class="p-4 bg-grey-2 rounded overflow-auto text-sm border" style="max-height: 500px;">{html.escape(json_str)}</pre>'
                        )
                        perm = (raw_item.get("permalink") or "").strip()
                        with ui.row().classes("gap-2 mt-2"):
                            if perm:
                                ui.button("Ver en MercadoLibre", on_click=lambda p=perm: ui.run_javascript(f'window.open({json.dumps(p)})'), color="primary").classes("rounded px-4 py-2").props("no-caps unelevated")
                            def _copiar_click(datos: str):
                                esc = json.dumps(datos)
                                ui.run_javascript(f'''
                                    (function() {{
                                        var texto = {esc};
                                        var done = function() {{
                                            try {{ window.__copiadoOk = true; }} catch(e) {{}}
                                        }};
                                        if (navigator.clipboard && navigator.clipboard.writeText) {{
                                            navigator.clipboard.writeText(texto).then(done).catch(function() {{
                                                var ta = document.createElement("textarea");
                                                ta.value = texto;
                                                ta.style.position = "fixed";
                                                ta.style.left = "-9999px";
                                                document.body.appendChild(ta);
                                                ta.select();
                                                ta.setSelectionRange(0, 999999);
                                                try {{ document.execCommand("copy"); }} catch(e) {{}}
                                                document.body.removeChild(ta);
                                                done();
                                            }});
                                        }} else {{
                                            var ta = document.createElement("textarea");
                                            ta.value = texto;
                                            ta.style.position = "fixed";
                                            ta.style.left = "-9999px";
                                            document.body.appendChild(ta);
                                            ta.select();
                                            ta.setSelectionRange(0, 999999);
                                            try {{ document.execCommand("copy"); }} catch(e) {{}}
                                            document.body.removeChild(ta);
                                            done();
                                        }}
                                    }})();
                                ''')
                                ui.notify("Datos copiados al portapapeles. Pegá con Ctrl+V donde quieras.", type="positive")
                            ui.button("Copiar datos", on_click=lambda d=json_str: _copiar_click(d), color="secondary").classes("rounded px-4 py-2").props("no-caps unelevated")
                    return
            # Búsqueda por texto o por ID cuando ml_get_item no encontró nada
            try:
                solo_propias = getattr(solo_propias_switch, "value", True)
                data = await run.io_bound(ml_search_similar, texto_buscar, 50, access_token, solo_propias)
                # Para IDs: si no hay resultados con propias, probar sin filtrar por propias
                if es_item_id and (not data.get("results") or len(data.get("results", [])) == 0) and solo_propias:
                    data = await run.io_bound(ml_search_similar, texto_buscar, 50, access_token, False)
                # Si ingresó solo números y no hubo resultados con MLA, intentar sin MLA
                if texto_fallback and (not data.get("results") or len(data.get("results", [])) == 0):
                    data = await run.io_bound(ml_search_similar, texto_fallback, 50, access_token, solo_propias)
                    if (not data.get("results") or len(data.get("results", [])) == 0) and solo_propias:
                        data = await run.io_bound(ml_search_similar, texto_fallback, 50, access_token, False)
            except Exception as err:
                data = {"results": [], "error": str(err)}
            results = data.get("results", [])[:50]
            from_catalog = data.get("from_catalog", False)
            ids_to_fetch = [str(r.get("id") or r.get("product_id") or r.get("item_id") or "").strip() or None for r in results]
            ids_list = [x for x in ids_to_fetch if x]
            if results and ids_list:
                with results_container:
                    ui.label("Cargando detalles (precio, vendedor, stock)...").classes("text-gray-600")
                bodies = await run.io_bound(ml_get_items_multiget_all, access_token, ids_list)
                id_to_body = {str(b.get("id")): b for b in bodies if b and isinstance(b, dict)}
                for i, r in enumerate(results):
                    item_id = ids_to_fetch[i]
                    if not item_id:
                        continue
                    full = id_to_body.get(str(item_id))
                    if full is None:
                        full = await run.io_bound(ml_get_item, access_token, item_id)
                    if full and isinstance(full, dict):
                        r["_full_item"] = full  # Para mostrar JSON completo cuando es búsqueda por ID
                        if full.get("price") is not None:
                            r["price"] = full["price"]
                        elif access_token:
                            precio = await run.io_bound(ml_fetch_price_for_item, access_token, item_id, full)
                            if precio is not None:
                                r["price"] = precio
                        if full.get("available_quantity") is not None:
                            r["available_quantity"] = full["available_quantity"]
                        if full.get("seller_id") is not None:
                            r["seller_id"] = full["seller_id"]
                        if full.get("title") is not None:
                            r["title"] = full["title"]
                        if full.get("permalink") is not None:
                            r["permalink"] = full["permalink"]
                        if full.get("seller") is not None:
                            r["seller"] = full["seller"]
                        if full.get("status") is not None:
                            r["status"] = full["status"]
                        r["has_item_data"] = True
                    elif from_catalog and access_token:
                        prod = await run.io_bound(ml_get_product_detail, access_token, item_id)
                        if prod and isinstance(prod, dict):
                            if prod.get("status") is not None:
                                r["status"] = prod.get("status")
                            bw = prod.get("buy_box_winner")
                            r["has_active_listing"] = isinstance(bw, dict) and bool(bw.get("item_id"))
                            br = prod.get("buy_box_winner_price_range") or {}
                            if isinstance(br, dict):
                                amt = br.get("min") or br.get("max") or br.get("amount")
                                if amt is not None:
                                    try:
                                        r["price"] = float(amt)
                                    except (TypeError, ValueError):
                                        pass
                            if isinstance(bw, dict) and bw.get("item_id"):
                                iid = str(bw["item_id"])
                                precio = await run.io_bound(ml_fetch_price_for_item, access_token, iid, None)
                                if precio is not None:
                                    r["price"] = precio
                seller_ids = [
                    str(r.get("seller_id") or (r.get("seller", {}).get("id") if isinstance(r.get("seller"), dict) else ""))
                    for r in results
                    if r.get("seller_id") or (isinstance(r.get("seller"), dict) and r.get("seller", {}).get("id"))
                ]
                seller_ids = list(dict.fromkeys(s for s in seller_ids if s and s != "0"))
                if seller_ids and access_token:
                    nicknames = await run.io_bound(ml_get_users_multiget, access_token, seller_ids)
                    for r in results:
                        sid = str(r.get("seller_id") or "")
                        if sid and sid in nicknames:
                            r["seller"] = {"id": sid, "nickname": nicknames[sid]}
            # Para búsqueda por ID: mostrar JSON completo; para texto: tabla resumida
            mostrar_como_json = es_item_id and results
            rows = [_norm_busqueda(r, from_catalog) for r in results]
            filter_showed_all = False
            if not mostrar_como_json and getattr(solo_activas_stock_switch, "value", True):
                rows_filtradas = [
                    x for x in rows
                    if x.get("has_active_listing", True)
                    and (
                        not x.get("has_item_data")
                        or ((x.get("status") or "") == "active" and (x.get("available_quantity") or 0) > 0)
                    )
                ]
                if rows_filtradas:
                    rows = rows_filtradas
                elif rows:
                    filter_showed_all = True
            if not mostrar_como_json:
                rows.sort(key=lambda x: x["price"])
            results_container.clear()
            with results_container:
                if data.get("error"):
                    ui.label(f"Error: {data['error']}").classes("text-negative")
                    texto_busq = (input_busqueda.value or "").strip()
                    if texto_busq:
                        from urllib.parse import quote
                        busq_url = f"https://listado.mercadolibre.com.ar/{quote(texto_busq)}"
                        ui.button("Buscar en MercadoLibre", on_click=lambda u=busq_url: ui.run_javascript(f'window.open({json.dumps(u)})')).props("flat no-caps").classes("text-primary mt-2")
                elif not (rows if not mostrar_como_json else results):
                    ui.label("No se encontraron resultados.").classes("text-gray-500")
                elif mostrar_como_json:
                    ui.label("Datos que devuelve MercadoLibre para las publicaciones encontradas:").classes(
                        "text-base font-semibold mb-3"
                    )
                    with ui.element("div").classes("w-full overflow-auto").style("max-height: 70vh;"):
                        for i, r in enumerate(results):
                            full_display = r.get("_full_item")
                            if not full_display:
                                full_display = {k: v for k, v in r.items() if k != "_full_item"}
                            tit = (full_display.get("title") or full_display.get("name") or f"Resultado {i+1}")[:80]
                            with ui.card().classes("w-full mt-2"):
                                ui.label(tit).classes("font-semibold text-primary mb-2")
                                json_str_card = json.dumps(full_display, indent=2, ensure_ascii=False)
                                ui.html(f'<pre class="p-4 bg-grey-2 rounded overflow-auto text-sm border" style="max-height: 400px;">{html.escape(json_str_card)}</pre>')
                                perm = (full_display.get("permalink") or "").strip()
                                with ui.row().classes("gap-2 mt-1"):
                                    if perm:
                                        ui.button("Ver en MercadoLibre", on_click=lambda p=perm: ui.run_javascript(f'window.open({json.dumps(p)})'), color="primary").classes("rounded px-3 py-1.5").props("no-caps unelevated")
                                    def _copiar_card(js: str) -> None:
                                        esc = json.dumps(js)
                                        ui.run_javascript(f'''
                                            (function() {{
                                                var texto = {esc};
                                                if (navigator.clipboard && navigator.clipboard.writeText) {{
                                                    navigator.clipboard.writeText(texto).then(function() {{}}).catch(function() {{
                                                        var ta = document.createElement("textarea");
                                                        ta.value = texto;
                                                        ta.style.position = "fixed"; ta.style.left = "-9999px";
                                                        document.body.appendChild(ta);
                                                        ta.select();
                                                        ta.setSelectionRange(0, 999999);
                                                        try {{ document.execCommand("copy"); }} catch(e) {{}}
                                                        document.body.removeChild(ta);
                                                    }});
                                                }} else {{
                                                    var ta = document.createElement("textarea");
                                                    ta.value = texto;
                                                    ta.style.position = "fixed"; ta.style.left = "-9999px";
                                                    document.body.appendChild(ta);
                                                    ta.select();
                                                    ta.setSelectionRange(0, 999999);
                                                    try {{ document.execCommand("copy"); }} catch(e) {{}}
                                                    document.body.removeChild(ta);
                                                }}
                                            }})();
                                        ''')
                                        ui.notify("Datos copiados al portapapeles. Pegá con Ctrl+V.", type="positive")
                                    ui.button("Copiar datos", on_click=lambda j=json_str_card: _copiar_card(j), color="secondary").classes("rounded px-3 py-1.5").props("no-caps unelevated")
                else:
                    if filter_showed_all:
                        ui.label(
                            "No se encontraron publicaciones activas con stock. Mostrando todos los resultados."
                        ).classes("text-amber-600 text-sm mb-2")
                    with ui.element("div").classes("w-full overflow-x-auto border rounded-lg").style("min-width: 800px;"):
                        with ui.row().classes("w-full bg-blue-600 text-white py-2 px-3 font-semibold flex-nowrap"):
                            ui.label("Nombre del producto").classes("min-w-[280px] shrink-0 text-left")
                            ui.label("Precio").classes("min-w-[120px] shrink-0 text-right")
                            ui.label("Vendedor").classes("min-w-[150px] shrink-0 text-left")
                            ui.label("Stock disp.").classes("min-w-[90px] shrink-0 text-right")
                            ui.label("Tipo").classes("min-w-[90px] shrink-0 text-left")
                            ui.label("Acciones").classes("min-w-[180px] shrink-0 text-left")
                        for idx, r in enumerate(rows):
                            raw_for_copiar = results[idx] if idx < len(results) else {}
                            datos_api = raw_for_copiar.get("_full_item") or raw_for_copiar
                            json_para_copiar = json.dumps(datos_api, indent=2, ensure_ascii=False)
                            perm = r.get("permalink", "#")
                            with ui.row().classes("w-full py-2 px-3 border-b border-gray-200 hover:bg-gray-50 flex-nowrap"):
                                tit = (r.get("title") or "")[:80] + ("..." if len(r.get("title") or "") > 80 else "")
                                ui.label(tit).classes("min-w-[280px] shrink-0 text-left")
                                ui.label(r.get("price_display", "—")).classes("min-w-[120px] shrink-0 text-right font-medium")
                                ui.label(str(r.get("seller", "—"))).classes("min-w-[150px] shrink-0 text-left")
                                ui.label(str(r.get("available_quantity_display", r.get("available_quantity", "—")))).classes("min-w-[90px] shrink-0 text-right")
                                ui.label(r.get("tipo", "")).classes("min-w-[90px] shrink-0 text-left")
                                with ui.row().classes("min-w-[180px] shrink-0 gap-1"):
                                    if perm and perm != "#":
                                        ui.button("Ver en ML", on_click=lambda p=perm: ui.run_javascript(f'window.open({json.dumps(p)})'), color="primary").classes("rounded px-2 py-1").props("no-caps unelevated")
                                    def _copiar_tabla(js: str) -> None:
                                        esc = json.dumps(js)
                                        ui.run_javascript(f'''
                                            (function() {{
                                                var texto = {esc};
                                                if (navigator.clipboard && navigator.clipboard.writeText) {{
                                                    navigator.clipboard.writeText(texto).then(function() {{}}).catch(function() {{
                                                        var ta = document.createElement("textarea");
                                                        ta.value = texto;
                                                        ta.style.position = "fixed"; ta.style.left = "-9999px";
                                                        document.body.appendChild(ta);
                                                        ta.select();
                                                        ta.setSelectionRange(0, 999999);
                                                        try {{ document.execCommand("copy"); }} catch(e) {{}}
                                                        document.body.removeChild(ta);
                                                    }});
                                                }} else {{
                                                    var ta = document.createElement("textarea");
                                                    ta.value = texto;
                                                    ta.style.position = "fixed"; ta.style.left = "-9999px";
                                                    document.body.appendChild(ta);
                                                    ta.select();
                                                    ta.setSelectionRange(0, 999999);
                                                    try {{ document.execCommand("copy"); }} catch(e) {{}}
                                                    document.body.removeChild(ta);
                                                }}
                                            }})();
                                        ''')
                                        ui.notify("Datos copiados al portapapeles. Pegá con Ctrl+V.", type="positive")
                                    ui.button("Copiar datos", on_click=lambda j=json_para_copiar: _copiar_tabla(j), color="secondary").classes("rounded px-2 py-1").props("no-caps unelevated")



def build_tab_comparar_precios() -> None:
    user = require_login()
    if not user:
        return

    ui.label("Comparar precios con la competencia").classes("text-lg font-semibold mb-4")
    ui.label(
        "Aquí podrás buscar un producto y ver precios de otros vendedores. "
        "De momento es sólo una pantalla de diseño; luego conectamos con la API."
    ).classes("text-gray-600 mb-4")

    query_input = ui.input("Palabra clave o código de producto").classes("w-full max-w-lg")
    result_area = ui.column().classes("w-full gap-2 mt-4")

    def comparar() -> None:
        if not query_input.value:
            ui.notify("Ingresa un término de búsqueda", color="negative")
            return
        save_query(
            user_id=user["id"],
            query_type="comparar_precios",
            params={"query": query_input.value},
        )
        result_area.clear()
        with result_area:
            ui.label("Aquí mostraremos resultados de la competencia (pendiente de implementar).")

    ui.button("Comparar", on_click=comparar, color="primary")


def build_tab_historial_precios() -> None:
    user = require_login()
    if not user:
        return

    ui.label("Historial de precios").classes("text-lg font-semibold mb-4")
    ui.label(
        "En esta pestaña podrás ver cómo evolucionaron los precios de tus productos "
        "y los de la competencia. Más adelante conectaremos esta vista con la base de datos."
    ).classes("text-gray-600")


def build_tab_competencia() -> None:
    user = require_login()
    if not user:
        return

    ui.label("Análisis de competencia").classes("text-lg font-semibold mb-4")
    ui.label(
        "Aquí calcularemos cantidad de vendedores, cantidad de productos y otros KPIs "
        "de la competencia."
    ).classes("text-gray-600 mb-4")

    categoria = ui.input("Categoría o keyword").classes("w-full max-w-lg")

    def calcular() -> None:
        if not categoria.value:
            ui.notify("Ingresa una categoría o palabra clave", color="negative")
            return
        save_query(
            user_id=user["id"],
            query_type="competencia",
            params={"categoria": categoria.value},
        )
        ui.notify("Cálculo de competencia pendiente de implementar.", color="info")

    ui.button("Calcular", on_click=calcular, color="primary")


def build_tab_pesos() -> None:
    """Pestaña Pesos: tabla Pesario (Marca, Producto, Peso, Fuente, Total) en formato Excel."""
    user = require_login()
    if not user:
        return

    uid = user["id"]

    def _get_tabla(nombre: str, default: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        r = get_cotizador_tabla(nombre, uid)
        return r if r else default

    pesario_data = list(_get_tabla("pesario", TABLA_PESARIO_DEFAULT))
    sort_col_pesario: List[Optional[str]] = [None]
    sort_asc_pesario: List[bool] = [True]

    def _parse_peso(s: Any) -> float:
        if s is None or s == "": return 0.0
        try:
            return float(str(s).replace(".", "").replace(",", "."))
        except (TypeError, ValueError):
            return 0.0

    def _fmt_peso_display(val: Any) -> str:
        """Formatea peso para mostrar con punto como separador de miles."""
        n = _parse_peso(val)
        return f"{int(n):,}".replace(",", ".") if n == int(n) else f"{n:,.1f}".replace(",", ".")

    with ui.column().classes("gap-4 p-4 w-full"):
        cont = ui.column().classes("gap-2 w-full overflow-x-auto")
        edit_rows: List[Dict[str, Any]] = []
        row_to_inputs: List[tuple] = []

        def toggle_sort(col: str) -> None:
            if sort_col_pesario[0] == col:
                sort_asc_pesario[0] = not sort_asc_pesario[0]
            else:
                sort_col_pesario[0] = col
                sort_asc_pesario[0] = True
            repintar()

        def sync_inputs_to_rows() -> None:
            for row_ref, rinputs in row_to_inputs:
                if row_ref in pesario_data:
                    row_ref["marca"] = str(rinputs["marca"].value or "")
                    row_ref["producto"] = str(rinputs["producto"].value or "")
                    row_ref["peso"] = str(rinputs["peso"].value or "")
                    row_ref["fuente"] = str(rinputs["fuente"].value or "")

        def repintar() -> None:
            sync_inputs_to_rows()
            cont.clear()
            edit_rows.clear()
            row_to_inputs.clear()
            datos = list(pesario_data)
            if sort_col_pesario[0] == "marca":
                rev = not sort_asc_pesario[0]
                datos.sort(key=lambda r: str(r.get("marca", "")).lower(), reverse=rev)
            elif sort_col_pesario[0] == "producto":
                rev = not sort_asc_pesario[0]
                datos.sort(key=lambda r: str(r.get("producto", "")).lower(), reverse=rev)
            elif sort_col_pesario[0] == "peso":
                rev = not sort_asc_pesario[0]
                datos.sort(key=lambda r: _parse_peso(r.get("peso")), reverse=rev)
            elif sort_col_pesario[0] == "fuente":
                rev = not sort_asc_pesario[0]
                datos.sort(key=lambda r: _parse_peso(r.get("fuente")), reverse=rev)
            elif sort_col_pesario[0] == "total":
                rev = not sort_asc_pesario[0]
                datos.sort(key=lambda r: _parse_peso(r.get("peso")) + _parse_peso(r.get("fuente")), reverse=rev)
            with cont:
                col_widths = {"marca": "100px", "producto": "399px", "peso": "70px", "fuente": "70px", "total": "90px", "ordenar": "56px", "borrar": "48px"}
                with ui.element("table").classes("border-collapse text-xs shrink-0").style("table-layout: fixed; width: 833px; min-width: 833px; line-height: 1.2;"):
                    with ui.element("colgroup"):
                        ui.element("col").style("width: " + col_widths["marca"])
                        ui.element("col").style("width: " + col_widths["producto"])
                        ui.element("col").style("width: " + col_widths["peso"])
                        ui.element("col").style("width: " + col_widths["fuente"])
                        ui.element("col").style("width: " + col_widths["total"])
                        ui.element("col").style("width: " + col_widths["ordenar"])
                        ui.element("col").style("width: " + col_widths["borrar"])
                    with ui.element("thead"):
                        with ui.element("tr").classes("bg-blue-100 dark:bg-blue-900"):
                            for col_key, h in [("marca", "Marca"), ("producto", "Producto"), ("peso", "Peso (gr)"), ("fuente", "Fuente (gr)"), ("total", "Total (gr)"), (None, "Ordenar"), (None, "Borrar")]:
                                th_cls = "font-semibold px-1 py-0.5 border border-gray-300"
                                th_cls += " text-left" if col_key in ("marca", "producto") else " text-center"
                                if col_key:
                                    th_cls += " cursor-pointer hover:bg-blue-200"
                                th = ui.element("th").classes(th_cls)
                                if col_key:
                                    th.on("click", lambda c=col_key: toggle_sort(c))
                                with th:
                                    ui.label(h)
                    with ui.element("tbody"):
                        for row_idx, row in enumerate(datos):
                            rinputs: Dict[str, Any] = {}
                            row_ref = row
                            idx_in_data = pesario_data.index(row) if row in pesario_data else row_idx
                            with ui.element("tr"):
                                for col in ["marca", "producto", "peso", "fuente"]:
                                    val = str(row.get(col, ""))
                                    if col in ("peso", "fuente") and val and _parse_peso(val) != 0:
                                        val = _fmt_peso_display(val)
                                    td_el = ui.element("td").classes("border border-gray-200").style("padding: 2px 4px; vertical-align: middle;")
                                    td_align = "text-center" if col in ("peso", "fuente") else ""
                                    with td_el:
                                        inp = ui.input(value=val).classes("w-full border-0 text-xs " + td_align).props("dense")
                                        rinputs[col] = inp
                                with ui.element("td").classes("border border-gray-200 bg-gray-50 text-center").style("padding: 2px 4px; vertical-align: middle;"):
                                    p0 = _parse_peso(row.get("peso"))
                                    f0 = _parse_peso(row.get("fuente"))
                                    t0 = p0 + f0
                                    total_txt = _fmt_peso_display(str(int(t0)) if t0 == int(t0) else f"{t0:.1f}")
                                    lbl_total = ui.label(total_txt).classes("px-1")

                                    def actualizar_total(lbl=lbl_total, rinp=rinputs) -> None:
                                        p = _parse_peso(rinp["peso"].value)
                                        f = _parse_peso(rinp["fuente"].value)
                                        t = p + f
                                        txt = _fmt_peso_display(str(int(t)) if t == int(t) else f"{t:.1f}")
                                        lbl.text = txt

                                    rinputs["peso"].on_value_change(actualizar_total)
                                    rinputs["fuente"].on_value_change(actualizar_total)
                                with ui.element("td").classes("border border-gray-200 w-8 text-center").style("padding: 2px 4px; vertical-align: middle;"):
                                    def subir(i: int) -> None:
                                        if 0 <= i < len(pesario_data) and i > 0:
                                            sync_inputs_to_rows()
                                            pesario_data[i], pesario_data[i - 1] = pesario_data[i - 1], pesario_data[i]
                                            repintar()
                                    def bajar(i: int) -> None:
                                        if 0 <= i < len(pesario_data) and i < len(pesario_data) - 1:
                                            sync_inputs_to_rows()
                                            pesario_data[i], pesario_data[i + 1] = pesario_data[i + 1], pesario_data[i]
                                            repintar()
                                    with ui.row().classes("gap-0 justify-center"):
                                        ui.button("▲", on_click=lambda i=idx_in_data: subir(i)).classes("min-w-0 px-0.5 text-xs").props("flat dense no-caps")
                                        ui.button("▼", on_click=lambda i=idx_in_data: bajar(i)).classes("min-w-0 px-0.5 text-xs").props("flat dense no-caps")
                                with ui.element("td").classes("border border-gray-200 w-8 text-center").style("padding: 2px 4px; vertical-align: middle;"):
                                    def borrar_pesario(rref: Dict[str, Any]) -> None:
                                        sync_inputs_to_rows()
                                        if rref in pesario_data:
                                            pesario_data.remove(rref)
                                            repintar()
                                    ui.button("×", on_click=lambda r=row_ref: borrar_pesario(r)).classes("text-red-600 font-bold text-base min-w-0 px-0").props("flat dense no-caps")
                            row_to_inputs.append((row_ref, rinputs))
                            edit_rows.append(rinputs)

        repintar()

        def guardar() -> None:
            sync_inputs_to_rows()
            new_data = []
            for row_ref, rinputs in row_to_inputs:
                p = _parse_peso(rinputs["peso"].value)
                f = _parse_peso(rinputs["fuente"].value)
                t = p + f
                new_data.append({
                    "marca": str(rinputs["marca"].value or ""),
                    "producto": str(rinputs["producto"].value or ""),
                    "peso": str(rinputs["peso"].value or ""),
                    "fuente": str(rinputs["fuente"].value or ""),
                    "total": str(int(t)) if t == int(t) else f"{t:.2f}",
                })
            set_cotizador_tabla("pesario", new_data, uid)
            pesario_data.clear()
            pesario_data.extend(new_data)
            ui.notify("Pesario guardado", color="positive")

        def agregar_fila() -> None:
            pesario_data.append({"marca": "", "producto": "", "peso": "0", "fuente": "0", "total": "0"})
            repintar()

        with ui.row().classes("gap-2"):
            ui.button("Agregar fila", on_click=agregar_fila, color="primary")
            ui.button("Guardar Tabla", on_click=guardar, color="secondary")


def _compute_ingresos_from_orders(orders_data: Dict[str, Any], user_id: int, periodo: str = "mes_actual") -> Dict[str, float]:
    """Calcula ventas y ganancias desde órdenes ML. periodo: mes_actual, mes_anterior o historico."""
    hoy = datetime.now().date()
    primer_dia = hoy.replace(day=1)
    ultimo_mes = primer_dia - timedelta(days=1)
    primer_dia_anterior = ultimo_mes.replace(day=1)
    raw = orders_data.get("results") or orders_data.get("orders") or orders_data.get("elements") or []
    ventas_mes_actual_monto = 0.0
    for o in raw:
        if not isinstance(o, dict):
            continue
        dt_str = o.get("date_created") or o.get("date_closed") or o.get("date_last_updated") or ""
        if not dt_str:
            continue
        try:
            dt = datetime.strptime(dt_str[:10], "%Y-%m-%d").date()
        except Exception:
            continue
        if periodo == "mes_actual":
            if not (primer_dia <= dt <= hoy):
                continue
        elif periodo == "mes_anterior":
            if not (primer_dia_anterior <= dt <= ultimo_mes):
                continue
        # historico: sin filtro de fecha
        elif periodo != "historico":
            continue
        amt = o.get("total_amount") or o.get("paid_amount")
        if amt is None and o.get("payments"):
            p = o["payments"][0] if isinstance(o["payments"], list) else {}
            amt = p.get("total_amount") or p.get("total_paid_amount") or p.get("transaction_amount")
        try:
            ventas_mes_actual_monto += float(amt or 0)
        except (TypeError, ValueError):
            pass
    if periodo == "mes_actual":
        dias_transcurridos = (hoy - primer_dia).days + 1
        dias_del_mes = calendar.monthrange(hoy.year, hoy.month)[1]
    elif periodo == "mes_anterior":
        dias_transcurridos = (ultimo_mes - primer_dia_anterior).days + 1
        dias_del_mes = dias_transcurridos
    else:
        dias_transcurridos = 1
        dias_del_mes = 1
    venta_diaria = ventas_mes_actual_monto / dias_transcurridos if dias_transcurridos > 0 else 0
    venta_estimada_mes = venta_diaria * dias_del_mes if dias_transcurridos > 0 else ventas_mes_actual_monto
    try:
        m = get_cotizador_param("ml_ganancia_neta_venta", user_id) or COTIZADOR_DEFAULTS.get("ml_ganancia_neta_venta", "0.1000")
        margen_val = float(str(m).replace(",", ".").strip())
    except (ValueError, TypeError):
        margen_val = 0.1
    ganancia_a_fecha = ventas_mes_actual_monto * margen_val
    ganancia_estimada_mes = venta_estimada_mes * margen_val
    return {
        "venta_a_fecha": ventas_mes_actual_monto,
        "venta_estimada_mes": venta_estimada_mes,
        "ganancia_a_fecha": ganancia_a_fecha,
        "ganancia_estimada_mes": ganancia_estimada_mes,
    }


def build_tab_balance(container) -> None:
    """Pestaña Balance: Gastos (editable), Ingresos (ventas/ganancias) y Resultados."""
    user = require_login()
    if not user:
        return

    uid = user["id"]
    access_token = get_ml_access_token(uid)
    gastos_data: List[Dict[str, Any]] = list(get_cotizador_tabla("gastos", uid))
    sort_col_gastos: List[Optional[str]] = [None]
    sort_asc_gastos: List[bool] = [True]

    def _parse_importe(s: Any) -> float:
        if s is None or s == "":
            return 0.0
        try:
            # Quitar puntos de miles, coma decimal
            raw = str(s).replace(".", "").replace(",", ".").strip()
            return float(raw) if raw else 0.0
        except (ValueError, TypeError):
            return 0.0

    def _fmt_importe_display(val: Any) -> str:
        """Formatea importe para mostrar: 1.234.567 (punto miles, coma decimal)"""
        n = _parse_importe(val)
        if n == 0 and (val is None or str(val).strip() == ""):
            return ""
        if abs(n - int(n)) < 0.01:
            return f"{int(n):,}".replace(",", ".")
        entera = int(n)
        dec = round((n - entera) * 100)
        return f"{entera:,}".replace(",", ".") + f",{dec:02d}"

    with container:
        with ui.column().classes("w-full p-8 items-center gap-4"):
            ui.spinner(size="xl")
            ui.label("Cargando Balance...").classes("text-xl text-gray-700")
    ingresos_ref: Dict[str, Any] = {"data": None}
    orders_balance_ref: Dict[str, Any] = {"data": {}}
    filtro_fecha_balance_ref: Dict[str, str] = {"val": "mes_actual"}

    async def _cargar_y_pintar() -> None:
        orders_data: Dict[str, Any] = {}
        if access_token:
            try:
                profile = await run.io_bound(ml_get_user_profile, access_token)
                seller_id = (profile or {}).get("id") or await run.io_bound(ml_get_user_id, access_token)
                if seller_id:
                    orders_data = await run.io_bound(ml_get_orders, access_token, str(seller_id), 1000, 0)
                orders_balance_ref["data"] = orders_data
                periodo = filtro_fecha_balance_ref.get("val", "mes_actual")
                ingresos_ref["data"] = _compute_ingresos_from_orders(orders_data, uid, periodo)
            except Exception:
                ingresos_ref["data"] = None
                orders_balance_ref["data"] = {}
        else:
            ingresos_ref["data"] = None
            orders_balance_ref["data"] = {}
        _pintar_contenido()

    def _pintar_contenido() -> None:
        container.clear()
        with container:
            header_card = ui.column().classes("w-full mb-2 p-4")
            with ui.row().classes("w-full gap-4 p-4 items-start flex-wrap"):
                # Columna izquierda: Gastos (tabla + botones)
                with ui.column().classes("gap-2").style("max-width: 500px;"):
                    with ui.card().classes("w-full p-4"):
                        ui.label("Gastos").classes("text-lg font-semibold mb-2")
                        cont = ui.column().classes("w-full gap-2")
                        edit_rows_ref: List[Dict[str, Any]] = []
                        gastos_buttons_row = ui.row().classes("gap-2 mt-2")
                # Columna derecha: Ingresos y Resultados Netos (lado a lado)
                with ui.row().classes("gap-4 flex-wrap"):
                    ingresos_card = ui.column().classes("gap-1")
                    resultados_card = ui.column().classes("gap-1")
        def toggle_sort(col: str) -> None:
            if sort_col_gastos[0] == col:
                sort_asc_gastos[0] = not sort_asc_gastos[0]
            else:
                sort_col_gastos[0] = col
                sort_asc_gastos[0] = True
            repintar()

        row_to_inputs: List[tuple] = []  # (row, rinputs) para mapear al guardar

        def sync_inputs_to_rows() -> None:
            """Copia valores de inputs a row dicts antes de repintar."""
            for row, rinputs in row_to_inputs:
                row["gasto"] = str(rinputs["gasto"].value or "")
                row["importe"] = str(rinputs["importe"].value or "")

        def _pintar_header() -> None:
            sync_inputs_to_rows()
            total_importes = sum(_parse_importe(r.get("importe")) for r in gastos_data)
            inc = ingresos_ref["data"]
            venta_fecha = inc.get("venta_a_fecha", 0) if inc else 0
            ganancia_fecha = inc.get("ganancia_a_fecha", 0) if inc else 0
            resultado_fecha = ganancia_fecha - total_importes
            facturacion_est = inc.get("venta_estimada_mes", 0) if inc else 0
            ganancia_bruta_est = inc.get("ganancia_estimada_mes", 0) if inc else 0
            ganancia_neta_est = ganancia_bruta_est - total_importes
            dolar_str = get_cotizador_param("dolar_oficial", uid) or COTIZADOR_DEFAULTS.get("dolar_oficial", "1000")
            dolar_oficial = float(str(dolar_str).replace(",", ".").strip()) if dolar_str else 0
            if dolar_oficial <= 0:
                dolar_oficial = 1000
            venta_fecha_usd = venta_fecha / dolar_oficial
            ganancia_fecha_usd = ganancia_fecha / dolar_oficial
            total_importes_usd = total_importes / dolar_oficial
            resultado_fecha_usd = resultado_fecha / dolar_oficial
            facturacion_est_usd = facturacion_est / dolar_oficial
            ganancia_bruta_est_usd = ganancia_bruta_est / dolar_oficial
            ganancia_neta_est_usd = ganancia_neta_est / dolar_oficial
            header_card.clear()
            with header_card:
                with ui.card().classes("w-full p-4 bg-grey-2"):
                    with ui.row().classes("w-full gap-6 flex-wrap"):
                        # 1. Periodo (fecha)
                        with ui.column().classes("gap-0 border-r border-gray-300 pr-4"):
                            ui.label("Fecha").classes("text-xs text-gray-600 font-semibold mb-1")
                            _val_fecha = filtro_fecha_balance_ref.get("val", "mes_actual")
                            if _val_fecha not in ("mes_actual", "mes_anterior"):
                                _val_fecha = "mes_actual"
                            sel_fecha = ui.select(
                                {"mes_actual": "Mes actual", "mes_anterior": "Mes anterior"},
                                value=_val_fecha,
                                label="",
                            ).classes("w-36").props("dense outlined")

                            def on_fecha_balance_change(e):
                                filtro_fecha_balance_ref["val"] = getattr(e, "value", "mes_actual")
                                od = orders_balance_ref.get("data") or {}
                                if od:
                                    ingresos_ref["data"] = _compute_ingresos_from_orders(od, uid, filtro_fecha_balance_ref["val"])
                                    _pintar_header()
                                    _pintar_ingresos()
                                    _pintar_resultados()

                            sel_fecha.on_value_change(on_fecha_balance_change)
                        # 2. Datos Actuales Pesos
                        with ui.column().classes("gap-0 border-r border-gray-300 pr-4"):
                            ui.label("Datos Actuales (pesos)").classes("text-xs text-gray-600 font-semibold mb-1")
                            with ui.row().classes("gap-4 flex-wrap"):
                                with ui.column().classes("gap-0"):
                                    ui.label("Venta a la fecha").classes("text-xs text-gray-600")
                                    ui.label(f"$ {venta_fecha:,.0f}".replace(",", ".")).classes("text-base font-bold text-primary")
                                with ui.column().classes("gap-0"):
                                    ui.label("Ganancia bruta a la fecha").classes("text-xs text-gray-600")
                                    ui.label(f"$ {ganancia_fecha:,.0f}".replace(",", ".")).classes("text-base font-bold text-primary")
                                with ui.column().classes("gap-0"):
                                    ui.label("Total Gastos").classes("text-xs text-gray-600")
                                    ui.label(f"$ {total_importes:,.0f}".replace(",", ".")).classes("text-base font-bold text-negative")
                                with ui.column().classes("gap-0"):
                                    ui.label("Resultado neto a la fecha").classes("text-xs text-gray-600")
                                    ui.label(f"$ {resultado_fecha:,.0f}".replace(",", ".")).classes("text-base font-bold " + ("text-positive" if resultado_fecha >= 0 else "text-negative"))
                        # 3. Datos Estimados Pesos
                        with ui.column().classes("gap-0 border-r border-gray-300 pr-4"):
                            ui.label("Datos Estimados (pesos)").classes("text-xs text-gray-600 font-semibold mb-1")
                            with ui.row().classes("gap-4 flex-wrap"):
                                with ui.column().classes("gap-0"):
                                    ui.label("Venta estimada").classes("text-xs text-gray-600")
                                    ui.label(f"$ {facturacion_est:,.0f}".replace(",", ".")).classes("text-base font-bold text-primary")
                                with ui.column().classes("gap-0"):
                                    ui.label("Ganancia bruta estimada").classes("text-xs text-gray-600")
                                    ui.label(f"$ {ganancia_bruta_est:,.0f}".replace(",", ".")).classes("text-base font-bold text-primary")
                                with ui.column().classes("gap-0"):
                                    ui.label("Total Gastos").classes("text-xs text-gray-600")
                                    ui.label(f"$ {total_importes:,.0f}".replace(",", ".")).classes("text-base font-bold text-negative")
                                with ui.column().classes("gap-0"):
                                    ui.label("Resultado neto estimado").classes("text-xs text-gray-600")
                                    ui.label(f"$ {ganancia_neta_est:,.0f}".replace(",", ".")).classes("text-base font-bold " + ("text-positive" if ganancia_neta_est >= 0 else "text-negative"))
                        # 4. Datos Estimados en Dólares
                        with ui.column().classes("gap-0"):
                            ui.label("Datos Estimados (dólares)").classes("text-xs text-gray-600 font-semibold mb-1")
                            with ui.row().classes("gap-4 flex-wrap"):
                                with ui.column().classes("gap-0"):
                                    ui.label("Venta estimada").classes("text-xs text-gray-600")
                                    ui.label(f"u$s {facturacion_est_usd:,.0f}".replace(",", ".")).classes("text-base font-bold text-primary")
                                with ui.column().classes("gap-0"):
                                    ui.label("Ganancia bruta estimada").classes("text-xs text-gray-600")
                                    ui.label(f"u$s {ganancia_bruta_est_usd:,.0f}".replace(",", ".")).classes("text-base font-bold text-primary")
                                with ui.column().classes("gap-0"):
                                    ui.label("Total Gastos").classes("text-xs text-gray-600")
                                    ui.label(f"u$s {total_importes_usd:,.0f}".replace(",", ".")).classes("text-base font-bold text-negative")
                                with ui.column().classes("gap-0"):
                                    ui.label("Resultado neto estimado").classes("text-xs text-gray-600")
                                    ui.label(f"u$s {ganancia_neta_est_usd:,.0f}".replace(",", ".")).classes("text-base font-bold " + ("text-positive" if ganancia_neta_est_usd >= 0 else "text-negative"))
            _pintar_resultados()

        def _pintar_ingresos() -> None:
            ingresos_card.clear()
            with ingresos_card:
                with ui.card().classes("w-full p-4 border-l-4 border-l-emerald-500"):
                    ui.label("Ingresos").classes("text-lg font-semibold text-emerald-700 mb-2")
                    inc = ingresos_ref["data"]
                    if inc is None:
                        ui.label("Conectá MercadoLibre para ver ingresos.").classes("text-gray-500")
                    else:
                        with ui.element("table").classes("w-full border-collapse text-sm"):
                            with ui.element("tbody"):
                                for label, key in [
                                    ("Venta a la fecha", "venta_a_fecha"),
                                    ("Venta estimada del mes", "venta_estimada_mes"),
                                ]:
                                    sin_negrita = key in ("venta_a_fecha", "ganancia_a_fecha")
                                    con_negrita_valor = key in ("venta_estimada_mes", "ganancia_estimada_mes")
                                    with ui.element("tr").classes("border-t border-gray-200"):
                                        with ui.element("td").classes("px-2 py-1 " + ("font-normal" if sin_negrita else "font-medium")):
                                            ui.label(label)
                                        with ui.element("td").classes("px-2 py-1 text-right " + ("font-semibold" if con_negrita_valor else "font-normal" if sin_negrita else "")):
                                            ui.label(f"$ {inc.get(key, 0):,.0f}".replace(",", "."))

        def _pintar_resultados() -> None:
            sync_inputs_to_rows()
            total_gastos = sum(_parse_importe(r.get("importe")) for r in gastos_data)
            inc = ingresos_ref["data"]
            resultados_card.clear()
            with resultados_card:
                with ui.card().classes("w-full p-4 border-l-4 border-l-blue-500"):
                    ui.label("Resultados Netos").classes("text-lg font-semibold text-blue-700 mb-2")
                    if inc is None:
                        ui.label("Conectá MercadoLibre para ver resultados.").classes("text-gray-500")
                    else:
                        res_a_fecha = inc.get("ganancia_a_fecha", 0) - total_gastos
                        res_estimado = inc.get("ganancia_estimada_mes", 0) - total_gastos
                        with ui.element("table").classes("w-full border-collapse text-sm"):
                            with ui.element("tbody"):
                                with ui.element("tr").classes("border-t border-gray-200"):
                                    with ui.element("td").classes("px-2 py-1 font-normal"):
                                        ui.label("Resultado neto a la fecha")
                                    with ui.element("td").classes("px-2 py-1 text-right font-normal"):
                                        ui.label(f"$ {res_a_fecha:,.0f}".replace(",", "."))
                                with ui.element("tr").classes("border-t border-gray-200"):
                                    with ui.element("td").classes("px-2 py-1 font-medium"):
                                        ui.label("Resultado neto estimado del mes")
                                    with ui.element("td").classes("px-2 py-1 text-right font-semibold"):
                                        ui.label(f"$ {res_estimado:,.0f}".replace(",", "."))

        def repintar() -> None:
            sync_inputs_to_rows()
            cont.clear()
            edit_rows_ref.clear()
            row_to_inputs.clear()
            datos = list(gastos_data)
            if sort_col_gastos[0] == "gasto":
                rev = not sort_asc_gastos[0]
                datos.sort(key=lambda r: str(r.get("gasto", "")).lower(), reverse=rev)
            elif sort_col_gastos[0] == "importe":
                rev = not sort_asc_gastos[0]
                datos.sort(key=lambda r: _parse_importe(r.get("importe")), reverse=rev)
            with cont:
                with ui.element("div").classes("w-full overflow-auto").style("max-height: 70vh;"):
                    with ui.element("table").classes("w-full border-collapse text-sm").style("table-layout: fixed;"):
                        with ui.element("thead"):
                            with ui.element("tr").classes("bg-primary text-white font-semibold sticky top-0"):
                                with ui.element("th").classes("px-2 py-2 border text-center cursor-pointer hover:bg-primary/80").style("width: 60%;").on("click", lambda: toggle_sort("gasto")):
                                    ui.label("Gasto")
                                with ui.element("th").classes("px-2 py-2 border text-center cursor-pointer hover:bg-primary/80").style("width: 30%;").on("click", lambda: toggle_sort("importe")):
                                    ui.label("Importe")
                                with ui.element("th").classes("px-1 py-2 border text-center").style("width: 70px;"):
                                    ui.label("Ordenar")
                                with ui.element("th").classes("px-1 py-2 border text-center").style("width: 50px;"):
                                    ui.label("Borrar")
                        with ui.element("tbody"):
                            for idx, row in enumerate(datos):
                                rinputs: Dict[str, Any] = {}
                                row_idx_in_data = gastos_data.index(row) if row in gastos_data else idx
                                imp_raw = str(row.get("importe", ""))
                                imp_display = _fmt_importe_display(imp_raw) if imp_raw else ""
                                with ui.element("tr").classes("border-t border-gray-200 hover:bg-gray-50"):
                                    with ui.element("td").classes("px-2 py-1 border-b border-gray-100").style("width: 60%;"):
                                        inp_gasto = ui.input(value=str(row.get("gasto", ""))).classes("w-full border-0").props("dense")
                                        rinputs["gasto"] = inp_gasto
                                    with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-right").style("width: 30%;"):
                                        with ui.row().classes("w-full items-center gap-1 justify-end text-right"):
                                            ui.label("$").classes("text-gray-600 text-sm")
                                            inp_imp = ui.input(value=imp_display).classes("flex-1 min-w-0 border-0").props("dense").style("text-align: right;")
                                            rinputs["importe"] = inp_imp
                                    with ui.element("td").classes("px-1 py-1 border-b border-gray-100 text-center"):
                                        def subir(i: int) -> None:
                                            if 0 <= i < len(gastos_data) and i > 0:
                                                gastos_data[i], gastos_data[i - 1] = gastos_data[i - 1], gastos_data[i]
                                                repintar()
                                        def bajar(i: int) -> None:
                                            if 0 <= i < len(gastos_data) and i < len(gastos_data) - 1:
                                                gastos_data[i], gastos_data[i + 1] = gastos_data[i + 1], gastos_data[i]
                                                repintar()
                                        with ui.row().classes("gap-0 justify-center"):
                                            ui.button("▲", on_click=lambda i=row_idx_in_data: subir(i)).classes("min-w-0 px-0.5 text-xs").props("flat dense no-caps")
                                            ui.button("▼", on_click=lambda i=row_idx_in_data: bajar(i)).classes("min-w-0 px-0.5 text-xs").props("flat dense no-caps")
                                    with ui.element("td").classes("px-1 py-1 border-b border-gray-100 text-center"):
                                        def borrar_fila(r: Dict[str, Any]) -> None:
                                            if r in gastos_data:
                                                gastos_data.remove(r)
                                                repintar()
                                        ui.button("×", on_click=lambda r=row: borrar_fila(r)).classes("text-red-600 font-bold text-lg min-w-0 px-1").props("flat dense no-caps")
                                edit_rows_ref.append(rinputs)
                                row_to_inputs.append((row, rinputs))
            _pintar_header()

        repintar()
        _pintar_ingresos()
        _pintar_resultados()

        def agregar_fila() -> None:
            gastos_data.append({"gasto": "", "importe": ""})
            repintar()

        def guardar() -> None:
            for row, rinputs in row_to_inputs:
                row["gasto"] = str(rinputs["gasto"].value or "")
                row["importe"] = str(rinputs["importe"].value or "")
            set_cotizador_tabla("gastos", gastos_data, uid)
            repintar()
            _pintar_header()
            ui.notify("Gastos guardados en la base de datos", color="positive")

        gastos_buttons_row.clear()
        with gastos_buttons_row:
            ui.button("Agregar fila", on_click=agregar_fila, color="primary")
            ui.button("Guardar tabla", on_click=guardar, color="secondary")

    background_tasks.create(_cargar_y_pintar(), name="cargar_balance")


def build_tab_admin(container) -> None:
    """Pestaña Admin: tabla de usuarios con permisos por pestaña y estado ML/BDC."""
    container.clear()
    user = require_login()
    if not user:
        return
    if not user_can_access_tab(user["id"], "admin"):
        with container:
            ui.label("No tenés permiso para acceder a Admin.").classes("text-negative")
        return

    users_list = get_all_users()
    with container:
        with ui.column().classes("w-full gap-2 p-2"):
            # Tarjeta Permisos (usuarios y acceso por pestaña)
            with ui.card().classes("w-full p-2 bg-grey-2"):
                with ui.element("div").classes("w-full overflow-x-auto"):
                    with ui.element("table").classes("border-collapse text-xs").style("width: 100%; min-width: 100%"):
                        with ui.element("thead"):
                            with ui.element("tr").classes("bg-primary text-white font-semibold sticky top-0"):
                                with ui.element("th").classes("px-2 py-1 border text-left"):
                                    ui.label("Usuario")
                                with ui.element("th").classes("px-1 py-1 border text-center").style("min-width: 52px"):
                                    ui.label("Borrar")
                                with ui.element("th").classes("px-1 py-1 border text-center").style("min-width: 58px"):
                                    ui.label("Pass")
                                with ui.element("th").classes("px-1 py-1 border text-center").style("min-width: 42px"):
                                    ui.label("ML")
                                with ui.element("th").classes("px-1 py-1 border text-center").style("min-width: 42px"):
                                    ui.label("BDC")
                                for _tab_key, label in TAB_KEYS:
                                    with ui.element("th").classes("px-1 py-1 border text-center").style("min-width: 48px"):
                                        ui.label(label[:8] if len(label) > 8 else label)
                        with ui.element("tbody"):
                            for u in users_list:
                                uid = u["id"]
                                uname = u.get("username", "")
                                ml_linked = bool(get_ml_access_token(uid))
                                qb_tokens = get_qb_tokens(uid)
                                bdc_linked = bool(qb_tokens and qb_tokens.get("access_token"))
                                perms = get_user_tab_permissions(uid)
                                with ui.element("tr").classes("border-t border-gray-200 hover:bg-gray-50"):
                                    with ui.element("td").classes("px-2 py-0.5 border-b border-gray-100 font-medium"):
                                        ui.label(uname)
                                    with ui.element("td").classes("px-1 py-0.5 border-b border-gray-100 text-center"):
                                        def _do_delete(target_uid: int, target_uname: str):
                                            with ui.dialog() as dlg:
                                                dlg.props("persistent")
                                                with ui.card().classes("p-4 min-w-[300px]"):
                                                    ui.label("¿Estás seguro que querés borrarlo?").classes("text-lg font-bold")
                                                    ui.label(f"Se borrará el usuario {target_uname} y todos sus datos.").classes("text-sm text-gray-600 mt-1")
                                                    with ui.row().classes("mt-3 gap-2 justify-end"):
                                                        ui.button("Cancelar", on_click=dlg.close)
                                                        def _confirm():
                                                            if target_uid == user["id"]:
                                                                ui.notify("No podés borrarte a vos mismo.", color="negative")
                                                                dlg.close()
                                                                return
                                                            err = delete_user_and_all_data(target_uid)
                                                            dlg.close()
                                                            if err:
                                                                ui.notify(err, color="negative")
                                                            else:
                                                                ui.notify("Usuario borrado correctamente", color="positive")
                                                                build_tab_admin(container)
                                                        ui.button("Borrar", on_click=_confirm, color="negative").props("flat")
                                            dlg.open()
                                        ui.button("Borrar", on_click=lambda uid_inner=uid, uname_inner=uname: _do_delete(uid_inner, uname_inner)).props("flat dense").classes("text-xs text-red-600")
                                    with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                        def _do_reset(target_uid: int):
                                            err, email_sent, dest_email, new_pwd = admin_reset_user_password(target_uid)
                                            if err and not new_pwd:
                                                ui.notify(err, color="negative")
                                            elif email_sent and dest_email:
                                                ui.notify(f"Enviamos un email con la nueva contraseña a {dest_email}", color="positive")
                                            elif new_pwd:
                                                with ui.dialog() as dlg:
                                                    dlg.props("persistent")
                                                    with ui.card().classes("p-6 min-w-[400px]"):
                                                        ui.label("No se pudo enviar el email").classes("text-lg font-semibold text-warning")
                                                        ui.label(err or "Contraseña actualizada, pero el correo no llegó.").classes("text-sm text-gray-600 mt-2")
                                                        ui.label("Nueva contraseña generada (copiala y entregala al usuario):").classes("text-sm font-medium mt-4")
                                                        with ui.row().classes("mt-2 p-3 bg-gray-100 rounded font-mono text-lg select-all"):
                                                            ui.label(new_pwd)
                                                        ui.button("Cerrar popup", on_click=dlg.close).props("flat color=primary").classes("mt-4")
                                                dlg.open()
                                            else:
                                                ui.notify("Contraseña actualizada, pero no se pudo enviar el email.", color="warning")
                                        ui.button("Reiniciar", on_click=lambda uid_inner=uid: _do_reset(uid_inner)).props("flat dense").classes("text-xs")
                                    with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                        with ui.row().classes("items-center justify-center gap-1"):
                                            ui.element("span").classes("w-2.5 h-2.5 rounded-full").style(f"background:{'#22c55e' if ml_linked else '#ef4444'}")
                                            ui.label("Sí" if ml_linked else "No").classes("text-xs")
                                    with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                        with ui.row().classes("items-center justify-center gap-1"):
                                            ui.element("span").classes("w-2.5 h-2.5 rounded-full").style(f"background:{'#22c55e' if bdc_linked else '#ef4444'}")
                                            ui.label("Sí" if bdc_linked else "No").classes("text-xs")
                                    for tab_key, _label in TAB_KEYS:
                                        with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                            val = perms.get(tab_key, True if tab_key != "admin" else False)
                                            chk = ui.checkbox(value=val).classes("justify-center")

                                            def _on_toggle(uid_inner: int, tk: str, evt: Any) -> None:
                                                set_user_tab_permission(uid_inner, tk, bool(getattr(evt, "value", evt)))
                                                ui.notify("Permiso actualizado", color="positive")

                                            chk.on_value_change(lambda e, uid_inner=uid, tk=tab_key: _on_toggle(uid_inner, tk, e))
            ui.label("ML = MercadoLibre vinculado. BDC = QuickBooks vinculado. Marcá los checkboxes para permitir acceso a cada pestaña.").classes("text-xs text-gray-600")

            # Tarjetas Marcas y Despachantes lado a lado
            with ui.row().classes("w-full gap-6 flex-wrap"):
                # Tarjeta Marcas (catálogo global para Compras)
                with ui.column().classes("max-w-xl"):
                    marcas_table_container = ui.column().classes("w-full gap-2")

                    def _row_marca(m: Dict) -> None:
                        mid = m["id"]
                        nombre_actual = m.get("nombre", "")
                        with ui.element("tr").classes("border-t border-gray-200 hover:bg-gray-50"):
                            with ui.element("td").classes("px-3 py-1 border-b border-gray-100"):
                                inp = ui.input(value=nombre_actual).classes("w-full").props("dense")
                                def _on_enter(mid_inner=mid, inp_ref_inner=inp):
                                    nuevo = (inp_ref_inner.value or "").strip()
                                    if nuevo and nuevo != nombre_actual:
                                        err = update_marca(mid_inner, nuevo)
                                        if err:
                                            ui.notify(err, color="negative")
                                        else:
                                            ui.notify("Marca actualizada", color="positive")
                                            _refresh_marcas()
                                inp.on("keydown.enter", _on_enter)
                            with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                def _do_delete_marca(mid_inner: int):
                                    err = delete_marca(mid_inner)
                                    if err:
                                        ui.notify(err, color="negative")
                                    else:
                                        ui.notify("Marca eliminada", color="positive")
                                    _refresh_marcas()
                                ui.button("Borrar", on_click=lambda mid_inner=mid: _do_delete_marca(mid_inner)).props("flat dense").classes("text-xs text-red-600")

                    def _refresh_marcas() -> None:
                        marcas_table_container.clear()
                        with marcas_table_container:
                            marcas_data = get_marcas()

                            with ui.card().classes("w-full p-4 bg-grey-2"):
                                with ui.expansion(
                                    "Ver todas las marcas",
                                    icon="",
                                ).classes("w-full mb-2").props("expand-icon-toggle dense"):
                                    with ui.element("table").classes("border-collapse text-sm w-full").style("width: 100%; min-width: 300px"):
                                        with ui.element("thead"):
                                            with ui.element("tr").classes("bg-primary text-white font-semibold"):
                                                with ui.element("th").classes("px-3 py-2 border text-left"):
                                                    ui.label("Nombre")
                                                with ui.element("th").classes("px-2 py-2 border text-center").style("min-width: 80px"):
                                                    ui.label("Eliminar")
                                        with ui.element("tbody"):
                                            for m in marcas_data:
                                                _row_marca(m)
                                with ui.row().classes("gap-2 items-center mt-2"):
                                    inp_nueva = ui.input(placeholder="Nueva marca").props("dense")

                                    def _agregar():
                                        nombre = (inp_nueva.value or "").strip()
                                        if not nombre:
                                            ui.notify("Ingresá un nombre", color="warning")
                                            return
                                        err = insert_marca(nombre)
                                        if err:
                                            ui.notify(err, color="negative")
                                        else:
                                            inp_nueva.value = ""
                                            ui.notify("Marca agregada", color="positive")
                                            _refresh_marcas()

                                    ui.button("Agregar marca", on_click=_agregar, color="primary").props("dense no-caps")

                    _refresh_marcas()

                # Tarjeta Despachantes
                with ui.column().classes("max-w-xl"):
                    despachantes_table_container = ui.column().classes("w-full gap-2")

                    def _row_despachante(d: Dict) -> None:
                        did = d["id"]
                        nombre_actual = d.get("nombre", "")
                        with ui.element("tr").classes("border-t border-gray-200 hover:bg-gray-50"):
                            with ui.element("td").classes("px-3 py-1 border-b border-gray-100"):
                                inp = ui.input(value=nombre_actual).classes("w-full").props("dense")
                                def _on_enter(did_inner=did, inp_ref_inner=inp):
                                    nuevo = (inp_ref_inner.value or "").strip()
                                    if nuevo and nuevo != nombre_actual:
                                        err = update_despachante(did_inner, nuevo)
                                        if err:
                                            ui.notify(err, color="negative")
                                        else:
                                            ui.notify("Despachante actualizado", color="positive")
                                            _refresh_despachantes()
                                inp.on("keydown.enter", _on_enter)
                            with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                def _do_delete_despachante(did_inner: int):
                                    err = delete_despachante(did_inner)
                                    if err:
                                        ui.notify(err, color="negative")
                                    else:
                                        ui.notify("Despachante eliminado", color="positive")
                                        _refresh_despachantes()
                                ui.button("Borrar", on_click=lambda did_inner=did: _do_delete_despachante(did_inner)).props("flat dense").classes("text-xs text-red-600")

                    def _refresh_despachantes() -> None:
                        despachantes_table_container.clear()
                        with despachantes_table_container:
                            despachantes_data = get_despachantes()

                            with ui.card().classes("w-full p-4 bg-grey-2"):
                                with ui.expansion(
                                    "Ver todos los despachantes",
                                    icon="",
                                ).classes("w-full mb-2").props("expand-icon-toggle dense"):
                                    with ui.element("table").classes("border-collapse text-sm w-full").style("width: 100%; min-width: 300px"):
                                        with ui.element("thead"):
                                            with ui.element("tr").classes("bg-primary text-white font-semibold"):
                                                with ui.element("th").classes("px-3 py-2 border text-left"):
                                                    ui.label("Nombre")
                                                with ui.element("th").classes("px-2 py-2 border text-center").style("min-width: 80px"):
                                                    ui.label("Eliminar")
                                        with ui.element("tbody"):
                                            for d in despachantes_data:
                                                _row_despachante(d)
                                with ui.row().classes("gap-2 items-center mt-2"):
                                    inp_nuevo = ui.input(placeholder="Nuevo despachante").props("dense")

                                    def _agregar_desp():
                                        nombre = (inp_nuevo.value or "").strip()
                                        if not nombre:
                                            ui.notify("Ingresá un nombre", color="warning")
                                            return
                                        err = insert_despachante(nombre)
                                        if err:
                                            ui.notify(err, color="negative")
                                        else:
                                            inp_nuevo.value = ""
                                            ui.notify("Despachante agregado", color="positive")
                                            _refresh_despachantes()

                                    ui.button("Agregar despachante", on_click=_agregar_desp, color="primary").props("dense no-caps")

                    _refresh_despachantes()


def build_tab_config() -> None:
    user = require_login()
    if not user:
        return

    # MercadoLibre + Estado de la cuenta (tarjeta combinada)
    app_creds = get_ml_app_credentials(user["id"])
    default_redirect = os.getenv("ML_REDIRECT_URI", "http://localhost:8083/ml/callback")
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM ml_credentials WHERE user_id = ?", (user["id"],))
    ml_creds = cur.fetchone()
    conn.close()

    # Layout: [ ML | QB ] misma fila, mismo ancho. [ Est | Contraseña | BD ] columna angosta
    _card_class = "w-full p-3 overflow-auto"
    with ui.column().classes("w-full gap-4 p-4"):
        ui.label("Configuración").classes("text-xl font-bold text-gray-800 mb-2")
        with ui.row().classes("w-full gap-4 items-stretch flex-wrap"):
            # 1. MercadoLibre — mismo ancho que QB (ancho fijo para igualar)
            with ui.column().classes("w-[400px] flex-shrink-0"):
                with ui.card().classes(_card_class):
                    def _desvincular_ml_impl() -> None:
                        conn = get_connection()
                        try:
                            cur = conn.cursor()
                            cur.execute("DELETE FROM ml_credentials WHERE user_id = ?", (user["id"],))
                            conn.commit()
                        finally:
                            conn.close()
                        ui.notify("Cuenta desvinculada", color="positive")
                        ui.navigate.reload()

                    ui.label("MercadoLibre").classes("text-base font-semibold mb-2")
                    ui.label("App ID, Client Secret, Redirect → Guardar → Conectar").classes("text-xs text-gray-600 mb-1")
                    with ui.expansion("Credenciales", icon="key").classes("w-full").props("expand-icon-toggle dense"):
                        inp_client_id = ui.input("App ID", value=app_creds["client_id"] if app_creds else "").classes("w-full").props("type=password password-toggle dense")
                        inp_client_secret = ui.input("Client Secret", value=app_creds["client_secret"] if app_creds else "").classes("w-full").props("type=password password-toggle dense")
                        inp_redirect = ui.input("Redirect URI", value=(app_creds.get("redirect_uri") or "").strip() or default_redirect if app_creds else default_redirect).classes("w-full").props("type=password password-toggle dense")

                    def guardar_app_ml() -> None:
                        cid = (inp_client_id.value or "").strip()
                        csec = (inp_client_secret.value or "").strip()
                        redir = (inp_redirect.value or "").strip() or default_redirect
                        if not cid or not csec:
                            ui.notify("Ingresá App ID y Client Secret", color="warning")
                            return
                        set_ml_app_credentials(user["id"], cid, csec, redir or None)
                        ui.notify("Credenciales guardadas correctamente", color="positive")

                    def conectar_ml() -> None:
                        cid = (inp_client_id.value or "").strip()
                        csec = (inp_client_secret.value or "").strip()
                        redir = (inp_redirect.value or "").strip() or default_redirect
                        if not cid or not csec:
                            ui.notify("Ingresá App ID y Client Secret y guardá antes de conectar", color="warning")
                            return
                        set_ml_app_credentials(user["id"], cid, csec, redir or None)
                        from urllib.parse import quote
                        scope = quote("offline_access read write")
                        auth_url = f"https://auth.mercadolibre.com.ar/authorization?response_type=code&client_id={cid}&redirect_uri={quote(redir)}&scope={scope}"
                        ui.navigate.to(auth_url)

                    with ui.row().classes("gap-2 mt-2"):
                        ui.button("Guardar credenciales", on_click=guardar_app_ml, color="primary").props("dense no-caps")
                        if ml_creds:
                            ui.button("Desvincular cuenta", on_click=_desvincular_ml_impl, color="secondary").props("dense no-caps")
                        else:
                            ui.button("Conectar cuenta", on_click=conectar_ml, color="secondary").props("dense no-caps")

                    ui.separator().classes("my-2")
                    ui.label("Estado").classes("text-xs font-semibold mb-1")
                    if ml_creds:
                        with ui.row().classes("items-center gap-2"):
                            ui.icon("check_circle", color="positive", size="sm")
                            ui.label("Vinculada").classes("text-positive text-sm")
                        if ml_creds["expires_at"]:
                            try:
                                exp = ml_creds["expires_at"][:19].replace("T", " ")
                                ui.label(f"Token vence: {exp}").classes("text-xs text-gray-600")
                            except Exception:
                                pass
                    else:
                        ui.label("Sin vincular").classes("text-warning text-sm")

            # 2. QuickBooks — un poco más largo que ML para igualar tamaño
            with ui.column().classes("w-[420px] flex-shrink-0"):
                with ui.card().classes(_card_class):
                    ui.label("QuickBooks").classes("text-base font-semibold mb-2")
                    ui.label("Client ID, Secret, Redirect → Guardar credenciales → Conectar cuenta").classes("text-xs text-gray-600 mb-1")
                    qb_app_creds = get_qb_app_credentials(user["id"])
                    ml_redir = os.getenv("ML_REDIRECT_URI", "http://localhost:8083/ml/callback")
                    default_qb_redirect = os.getenv("QB_REDIRECT_URI") or (
                        ml_redir.replace("/ml/callback", "/qb/callback") if "/ml/callback" in ml_redir else "http://localhost:8083/qb/callback"
                    )
                    with ui.expansion("Credenciales QB", icon="account_balance").classes("w-full").props("expand-icon-toggle dense"):
                        inp_qb_cid = ui.input("Client ID", value=qb_app_creds["client_id"] if qb_app_creds else "").classes("w-full").props("type=password password-toggle dense")
                        inp_qb_csec = ui.input("Client Secret", value=qb_app_creds["client_secret"] if qb_app_creds else "").classes("w-full").props("type=password password-toggle dense")
                        inp_qb_redir = ui.input("Redirect URI", value=(qb_app_creds.get("redirect_uri") or "").strip() or default_qb_redirect if qb_app_creds else default_qb_redirect).classes("w-full").props("type=password password-toggle dense")
                        async def _usar_url_actual_qb():
                            try:
                                origin = await ui.run_javascript("window.location.origin")
                                if origin:
                                    inp_qb_redir.value = f"{origin}/qb/callback"
                                    ui.notify(f"Redirect URI: {inp_qb_redir.value}", color="positive")
                            except Exception:
                                ui.notify("No se pudo detectar la URL actual", color="warning")
                        ui.button("Usar URL actual", on_click=_usar_url_actual_qb, color="secondary").props("dense no-caps flat")

                    def guardar_qb_creds() -> None:
                        cid = (inp_qb_cid.value or "").strip()
                        csec = (inp_qb_csec.value or "").strip()
                        redir = (inp_qb_redir.value or "").strip() or default_qb_redirect
                        if not cid or not csec:
                            ui.notify("Ingresá Client ID y Client Secret", color="warning")
                            return
                        set_qb_app_credentials(user["id"], cid, csec, redir or None)
                        ui.notify("Credenciales guardadas", color="positive")

                    def conectar_qb() -> None:
                        cid = (inp_qb_cid.value or "").strip()
                        csec = (inp_qb_csec.value or "").strip()
                        redir = (inp_qb_redir.value or "").strip() or default_qb_redirect
                        if not cid or not csec:
                            ui.notify("Guardá credenciales antes de conectar", color="warning")
                            return
                        set_qb_app_credentials(user["id"], cid, csec, redir or None)
                        from urllib.parse import quote
                        scope = quote("com.intuit.quickbooks.accounting")
                        state = f"qb_{user['id']}"
                        # Intuit: redirect_uri debe coincidir EXACTAMENTE con developer.intuit.com (Keys)
                        # Probar codificación mínima (preserva :/) por si Intuit la prefiere
                        redir_encoded = quote(redir, safe=':/')
                        auth_url = f"https://appcenter.intuit.com/connect/oauth2?response_type=code&client_id={cid}&redirect_uri={redir_encoded}&scope={scope}&state={quote(state)}"
                        ui.navigate.to(auth_url)

                    conn = get_connection()
                    cur = conn.cursor()
                    cur.execute("SELECT * FROM qb_tokens WHERE user_id = ? ORDER BY id DESC LIMIT 1", (user["id"],))
                    qb_tokens = cur.fetchone()
                    conn.close()

                    def desvincular_qb() -> None:
                        conn = get_connection()
                        try:
                            cur = conn.cursor()
                            cur.execute("DELETE FROM qb_tokens WHERE user_id = ?", (user["id"],))
                            conn.commit()
                        finally:
                            conn.close()
                        ui.notify("QuickBooks desvinculado", color="positive")
                        ui.navigate.reload()

                    with ui.row().classes("gap-1 mt-1"):
                        ui.button("Guardar credenciales", on_click=guardar_qb_creds, color="primary").props("dense no-caps")
                        if qb_tokens and qb_tokens["access_token"]:
                            ui.button("Desvincular cuenta", on_click=desvincular_qb, color="secondary").props("dense no-caps")
                        else:
                            ui.button("Conectar cuenta", on_click=conectar_qb, color="secondary").props("dense no-caps")

                    ui.separator().classes("my-2")
                    ui.label("Estado").classes("text-xs font-semibold mb-1")
                    if qb_tokens and qb_tokens["access_token"]:
                        with ui.row().classes("items-center gap-2"):
                            ui.icon("check_circle", color="positive", size="xs")
                            ui.label("Vinculada").classes("text-positive text-sm")
                        if qb_tokens["expires_at"]:
                            try:
                                exp = qb_tokens["expires_at"][:19].replace("T", " ")
                                ui.label(f"Token vence: {exp}").classes("text-xs text-gray-600")
                            except Exception:
                                pass
                        ui.separator().classes("my-2")
                        ui.label("Soy el cliente (en QuickBooks):").classes("text-xs font-semibold mb-1")
                        qb_cust = get_user_qb_customer(user["id"])
                        cust_id = qb_cust.get("id", "") if qb_cust else ""
                        cust_name = "—"
                        if qb_cust and cust_id:
                            cust_detail, _ = fetch_qb_customer_detail(user["id"], cust_id)
                            cust_name = (cust_detail.get("DisplayName") or cust_detail.get("FullyQualifiedName") or cust_detail.get("CompanyName") or "").strip() if cust_detail else (qb_cust.get("name") or "—")
                            if not cust_name:
                                cust_name = qb_cust.get("name", "—")
                        cust_nombre = f"{cust_name} (id {cust_id})" if cust_id else "—"
                        ui.label(cust_nombre).classes("text-sm text-gray-800")
                    else:
                        ui.label("Sin vincular").classes("text-warning text-sm")

            # 3. Estadísticas, Contraseña, Base de datos — apiladas, mismo ancho que ML y QB
            with ui.column().classes("w-[420px] flex-shrink-0 gap-3"):
                with ui.card().classes(_card_class):
                    ui.label("Estadísticas").classes("text-sm font-semibold mb-1")
                    ui.label("Ventas a cargar (órdenes)").classes("text-xs text-gray-600 mb-1")
                    ui.label("Cantidad máxima de órdenes de MercadoLibre a traer en cada actualización. Más órdenes = datos más completos, pero la carga puede tardar más.").classes("text-xs text-gray-500 mb-1")
                    limit_actual = get_cotizador_param("estadisticas_limit_ordenes", user["id"]) or "1000"
                    opts_ventas = {"300": "300", "500": "500", "1000": "1000", "2000": "2000", "3000": "3000", "4000": "4000", "5000": "5000", "7500": "7500", "10000": "10000"}
                    sel_ventas = ui.select(opts_ventas, value=limit_actual, label="").classes("w-full").props("dense")

                    def guardar_limit_ventas() -> None:
                        val = str(sel_ventas.value or "1000").strip()
                        if val not in opts_ventas:
                            val = "1000"
                        set_cotizador_param("estadisticas_limit_ordenes", val, user["id"])
                        ui.notify("Guardado", color="positive")
                    ui.button("Guardar", on_click=guardar_limit_ventas, color="primary").classes("mt-1").props("dense no-caps")

                # Cambiar contraseña (debajo de Estadísticas)
                with ui.card().classes(_card_class):
                    with ui.expansion("Cambiar contraseña", icon="lock").classes("w-full").props("expand-icon-toggle dense"):
                        inp_actual = ui.input("Contraseña actual").classes("w-full").props("type=password password-toggle dense")
                        inp_nueva = ui.input("Nueva contraseña (mín. 4)").classes("w-full").props("type=password password-toggle dense")
                        inp_confirmar = ui.input("Confirmar nueva").classes("w-full").props("type=password password-toggle dense")

                        def cambiar_clave() -> None:
                            actual = (inp_actual.value or "").strip()
                            nueva = (inp_nueva.value or "").strip()
                            confirmar = (inp_confirmar.value or "").strip()
                            if not actual:
                                ui.notify("Ingresá tu contraseña actual", color="warning")
                                return
                            if nueva != confirmar:
                                ui.notify("La nueva contraseña y la confirmación no coinciden", color="negative")
                                return
                            error = update_user_password(user["id"], actual, nueva)
                            if error:
                                ui.notify(error, color="negative")
                                return
                            ui.notify("Contraseña cambiada correctamente", color="positive")
                            inp_actual.value = ""
                            inp_nueva.value = ""
                            inp_confirmar.value = ""

                        ui.button("Cambiar contraseña", on_click=cambiar_clave, color="primary").classes("mt-1").props("dense no-caps")

                # Base de datos
                with ui.card().classes(_card_class) as db_card:
                    ui.label("Base de datos").classes("text-sm font-semibold mb-1")
                    ui.label("Backup: productos, cotizador, importación. Sin credenciales.").classes("text-xs text-gray-600 mb-2")

                    def descargar_backup() -> None:
                        try:
                            content = export_user_db_data(user["id"])
                            fd, path = tempfile.mkstemp(suffix=".json")
                            os.write(fd, content)
                            os.close(fd)
                            nombre = f"backup_{user.get('username', 'user')}_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
                            with db_card:
                                ui.download(path, nombre)
                            ui.notify("Descarga iniciada", color="positive")
                            def _cleanup() -> None:
                                try:
                                    if path and os.path.exists(path):
                                        os.unlink(path)
                                except Exception:
                                    pass
                            ui.timer(5.0, _cleanup, once=True)
                        except Exception as ex:
                            ui.notify(f"Error: {ex}", color="negative")

                    def _formatear_fecha_backup(data: dict) -> str:
                        fd = data.get("fecha_descarga")
                        if fd:
                            return str(fd)
                        ea = data.get("exported_at", "")
                        if ea:
                            try:
                                d = datetime.fromisoformat(ea.replace("Z", "+00:00"))
                                return d.strftime("%Y-%m-%d %H:%M")
                            except Exception:
                                pass
                        return ea or "fecha desconocida"

                    async def on_upload(e) -> None:
                        try:
                            file_obj = getattr(e, "file", None) or (getattr(e, "files", [None])[0] if getattr(e, "files", None) else None) or getattr(e, "content", None)
                            if file_obj is None:
                                ui.notify("Error: no se pudo leer el archivo", color="negative")
                                upload_component.reset()
                                return
                            read_result = file_obj.read()
                            content = await read_result if asyncio.iscoroutine(read_result) else read_result
                            if isinstance(content, str):
                                content = content.encode("utf-8")
                            data = json.loads(content.decode("utf-8"))
                            if not isinstance(data, dict):
                                ui.notify("Error: archivo inválido", color="negative")
                                upload_component.reset()
                                return
                            fecha_str = _formatear_fecha_backup(data)
                            content_bytes = bytes(content)

                            def make_confirm(cb: bytes, upload_el):
                                def confirmar_sobrescribir() -> None:
                                    dlg.close()
                                    msg = import_user_db_data(user["id"], cb)
                                    if msg == "ok":
                                        ui.notify("Backup restaurado correctamente", color="positive")
                                        upload_el.reset()
                                    else:
                                        ui.notify(f"Error al restaurar: {msg}", color="negative")
                                        upload_el.reset()
                                return confirmar_sobrescribir

                            with ui.dialog().props("persistent") as dlg:
                                dlg.classes("w-full max-w-md")
                                with ui.card().classes("w-full p-6 gap-4"):
                                    ui.label("¿Sobrescribir datos?").classes("text-lg font-semibold")
                                    ui.label(
                                        f"¿Estás seguro que querés sobrescribir los datos actuales con el backup del {fecha_str}?"
                                    ).classes("text-gray-600")
                                    with ui.row().classes("w-full justify-end gap-2 pt-2"):
                                        def cancelar_y_reset() -> None:
                                            dlg.close()
                                            upload_component.reset()
                                        ui.button("Cancelar", on_click=cancelar_y_reset, color="secondary").props("flat")
                                        ui.button("Confirmar", on_click=make_confirm(content_bytes, upload_component), color="negative").props("unelevated")
                                dlg.open()
                        except json.JSONDecodeError as ex:
                            ui.notify(f"Error: archivo JSON inválido - {ex}", color="negative")
                            upload_component.reset()
                        except Exception as ex:
                            ui.notify(f"Error: {str(ex)}", color="negative")
                            upload_component.reset()

                    def handle_multi(e) -> None:
                        if getattr(e, "files", None) and len(e.files) > 0:
                            class _FakeEv:
                                file = e.files[0]
                            import asyncio
                            asyncio.create_task(on_upload(_FakeEv()))

                    upload_component = ui.upload(
                        on_upload=on_upload,
                        max_files=1,
                        max_file_size=10_000_000,
                        auto_upload=True,
                    ).props("accept=.json").classes("hidden")
                    with ui.row().classes("gap-3 items-center w-full"):
                        ui.button("Descargar backup", on_click=descargar_backup, color="primary").classes("flex-1").props("icon=download unelevated no-caps")
                        ui.button("Cargar backup", on_click=lambda: upload_component.run_method("pickFiles"), color="secondary").classes("flex-1").props("icon=upload outline no-caps")


# Valores por defecto del cotizador
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

TABLA_ORIGEN_DEFAULT = [
    {"origen": "Mia LHS", "posicion": "Cambio PA"},
    {"origen": "Mia Rosario", "posicion": "21I + 20D + 3E"},
    {"origen": "Mia Richard", "posicion": "10,5I + 10,8D + 0E"},
    {"origen": "China", "posicion": "10,5I + 0D + 0E"},
]
TABLA_CAMBIO_PA_DEFAULT = [{"valor": "$0"}, {"valor": "$100"}, {"valor": "$150"}, {"valor": "$200"}, {"valor": "$250"}, {"valor": "$300"}]
TABLA_DERECHOS_DEFAULT = [{"valor": "0,35"}, {"valor": "0,2"}, {"valor": "0,108"}, {"valor": "0"}]
TABLA_ESTADISTICAS_DEFAULT = [{"valor": "0"}, {"valor": "0,03"}]
TABLA_TRAFO_GRAMOS_DEFAULT = [
    {"trafo": "No", "gramos": "0"}, {"trafo": "Mi stick", "gramos": "28"}, {"trafo": "Roku", "gramos": "30"},
    {"trafo": "Chromecast", "gramos": "33"}, {"trafo": "Onn", "gramos": "58"}, {"trafo": "Echo", "gramos": "122"},
    {"trafo": "Mini PC", "gramos": "244"},
]

TABLA_PESARIO_DEFAULT = [
    {"marca": "Amazon", "producto": "Echo Buds 2", "peso": "181", "fuente": "0", "total": "181"},
    {"marca": "Amazon", "producto": "Echo Pop", "peso": "270", "fuente": "115", "total": "385"},
    {"marca": "Amazon", "producto": "Echo Dot 5", "peso": "409", "fuente": "115", "total": "524"},
    {"marca": "Amazon", "producto": "Echo Spot", "peso": "502", "fuente": "115", "total": "617"},
    {"marca": "Amazon", "producto": "Echo Dot Max", "peso": "696", "fuente": "115", "total": "811"},
    {"marca": "Amazon", "producto": "Echo Show 5 3ra", "peso": "554", "fuente": "124", "total": "678"},
    {"marca": "Amazon", "producto": "Echo Show 8 3ra", "peso": "1325", "fuente": "124", "total": "1449"},
    {"marca": "Amazon", "producto": "Fire TV Stick Lite", "peso": "214", "fuente": "0", "total": "214"},
    {"marca": "Amazon", "producto": "Kindle 11\" 4253", "peso": "223", "fuente": "0", "total": "223"},
    {"marca": "Amazon", "producto": "Kindle 6\" 16GB 2024", "peso": "223", "fuente": "0", "total": "223"},
    {"marca": "Google", "producto": "Chromecast 4K", "peso": "243", "fuente": "30", "total": "273"},
    {"marca": "Google", "producto": "Chromecast HD", "peso": "242", "fuente": "30", "total": "272"},
    {"marca": "Google", "producto": "Google TV Streamer", "peso": "350", "fuente": "30", "total": "380"},
    {"marca": "Onn", "producto": "Onn 4K", "peso": "299", "fuente": "49", "total": "348"},
    {"marca": "Onn", "producto": "Onn 4K Plus", "peso": "288", "fuente": "49", "total": "337"},
    {"marca": "Onn", "producto": "Onn Full HD", "peso": "220", "fuente": "49", "total": "269"},
    {"marca": "Onn", "producto": "Tablet Surf 7\"", "peso": "424", "fuente": "49", "total": "473"},
    {"marca": "Roku", "producto": "Express 3960", "peso": "0", "fuente": "0", "total": "0"},
    {"marca": "Roku", "producto": "Premiere 4K 3920", "peso": "0", "fuente": "0", "total": "0"},
    {"marca": "Roku", "producto": "Streaming Stick 3840", "peso": "161", "fuente": "0", "total": "161"},
    {"marca": "Roku", "producto": "Streaming Stick 4K", "peso": "0", "fuente": "0", "total": "0"},
    {"marca": "JBL", "producto": "Flip 7", "peso": "788", "fuente": "0", "total": "788"},
    {"marca": "JBL", "producto": "Go 4", "peso": "320", "fuente": "0", "total": "320"},
    {"marca": "JBL", "producto": "Charge 6", "peso": "1338", "fuente": "0", "total": "1338"},
    {"marca": "JBL", "producto": "Tune 720", "peso": "450", "fuente": "0", "total": "450"},
    {"marca": "JBL", "producto": "520C On Ear", "peso": "330", "fuente": "0", "total": "330"},
    {"marca": "JBL", "producto": "Endurance Run 3", "peso": "50", "fuente": "0", "total": "50"},
    {"marca": "Samsung", "producto": "SSD 970 Evo Plus", "peso": "82", "fuente": "0", "total": "82"},
    {"marca": "Samsung", "producto": "SSD 980 Pro", "peso": "72", "fuente": "0", "total": "72"},
    {"marca": "Samsung", "producto": "SSD 990 Evo", "peso": "83", "fuente": "0", "total": "83"},
    {"marca": "Xiaomi", "producto": "Mini Speaker", "peso": "43", "fuente": "0", "total": "43"},
    {"marca": "Xiaomi", "producto": "Mi Smart Scale 2", "peso": "1472", "fuente": "0", "total": "1472"},
    {"marca": "Xiaomi", "producto": "MI TV Stick 2k - MDZ-24", "peso": "220", "fuente": "0", "total": "220"},
    {"marca": "Xiaomi", "producto": "MI TV Stick 4k - MDZ-27", "peso": "260", "fuente": "0", "total": "260"},
    {"marca": "Xiaomi", "producto": "Redemi Buds 4 Lite", "peso": "76", "fuente": "0", "total": "76"},
    {"marca": "Xiaomi", "producto": "Redmi Buds 3", "peso": "92", "fuente": "0", "total": "92"},
    {"marca": "Xiaomi", "producto": "Redmi Buds Essential", "peso": "71", "fuente": "0", "total": "71"},
    {"marca": "Xiaomi", "producto": "Redmi Pad Pro 12\"", "peso": "942", "fuente": "100", "total": "1042"},
    {"marca": "Xiaomi", "producto": "Redmi Pad SE 11\"", "peso": "735", "fuente": "100", "total": "835"},
    {"marca": "Xiaomi", "producto": "Redmi Pad SE 8,7\"", "peso": "507", "fuente": "100", "total": "607"},
    {"marca": "Xiaomi", "producto": "Redmi Watch 2 Lite", "peso": "202", "fuente": "0", "total": "202"},
    {"marca": "Xiaomi", "producto": "Redmi Watch 3", "peso": "186", "fuente": "0", "total": "186"},
    {"marca": "Xiaomi", "producto": "Redmi Watch 5 Active", "peso": "124", "fuente": "0", "total": "124"},
    {"marca": "Xiaomi", "producto": "Redmi Watch 5 Lite", "peso": "123", "fuente": "0", "total": "123"},
    {"marca": "Xiaomi", "producto": "Smart Band 7", "peso": "112", "fuente": "0", "total": "112"},
    {"marca": "Xiaomi", "producto": "Smart Band 9 Active", "peso": "98", "fuente": "0", "total": "98"},
    {"marca": "Xiaomi", "producto": "TV Box S 3ra - MDZ-32", "peso": "370", "fuente": "0", "total": "370"},
    {"marca": "Xiaomi", "producto": "TV Box S 2da - MDZ-28", "peso": "415", "fuente": "0", "total": "415"},
]

TABLA_POSICION_DEFAULT = [
    {"posicion": "Cambio PA", "seguro": "0.02", "flete": "0.030", "derechos": "0.000", "estadisticas": "0", "iva": "0.105", "despachante": "0.214", "cambio_pa": "1"},
    {"posicion": "10,5I + 0D + 0E", "seguro": "0.02", "flete": "0.030", "derechos": "0.000", "estadisticas": "0", "iva": "0.105", "despachante": "0.214", "cambio_pa": "0"},
    {"posicion": "10,5I + 10,8D + 0E", "seguro": "0.02", "flete": "0.030", "derechos": "0.108", "estadisticas": "0", "iva": "0.105", "despachante": "0.214", "cambio_pa": "0"},
    {"posicion": "10,5I + 16D + 0E", "seguro": "0.02", "flete": "0.030", "derechos": "0.160", "estadisticas": "0", "iva": "0.105", "despachante": "0.214", "cambio_pa": "0"},
    {"posicion": "21I + 0D + 0E", "seguro": "0.02", "flete": "0.030", "derechos": "0.000", "estadisticas": "0", "iva": "0.21", "despachante": "0.214", "cambio_pa": "0"},
    {"posicion": "21I + 20D + 3E", "seguro": "0.02", "flete": "0.030", "derechos": "0.200", "estadisticas": "0.03", "iva": "0.21", "despachante": "0.214", "cambio_pa": "0"},
    {"posicion": "21I + 35D + 3E", "seguro": "0.02", "flete": "0.030", "derechos": "0.350", "estadisticas": "0.03", "iva": "0.21", "despachante": "0.214", "cambio_pa": "0"},
]

TABLA_ENVIOS_ML_DEFAULT = [
    {"envio": "Flex - Caba", "importe": "4611", "porc_10": "461", "costo": "4150"},
    {"envio": "Flex - 1er cordon", "importe": "7371", "porc_10": "737", "costo": "6634"},
    {"envio": "Flex - 2do cordon", "importe": "10246", "porc_10": "1025", "costo": "9221"},
    {"envio": "Correo", "importe": "11646", "porc_10": "-", "costo": "5823"},
]

TABLA_COURIER_DEFAULT = [
    {"courier": "Mia LHS", "valor_kg": "13.50", "descuento": "1.33267522", "kg_real": "10.13", "almacenaje": "1.80", "seguro": "24.75", "res_3244": "10.00", "gas_ope": "27.00", "env_dom": "10.00", "iibb": "0.03", "cif": "0"},
    {"courier": "Mia Rosario", "valor_kg": "26.00", "descuento": "1", "kg_real": "22.00", "almacenaje": "0", "seguro": "0", "res_3244": "0", "gas_ope": "0", "env_dom": "0", "iibb": "0", "cif": "0.7$+0.01%"},
    {"courier": "Mia Richard", "valor_kg": "9.50", "descuento": "1", "kg_real": "9.50", "almacenaje": "1.90", "seguro": "29.75", "res_3244": "5.00", "gas_ope": "25.00", "env_dom": "10.00", "iibb": "0", "cif": "3$+2%"},
    {"courier": "China", "valor_kg": "27.00", "descuento": "1.33267522", "kg_real": "20.26", "almacenaje": "2.70", "seguro": "29.35", "res_3244": "10.00", "gas_ope": "27.00", "env_dom": "10.00", "iibb": "0.03", "cif": "0"},
]


def _calc_courier_row(
    row: Dict[str, Any],
    params: Dict[str, float],
    posicion_by_name: Dict[str, Dict[str, float]],
    courier_by_origen: Dict[str, Dict[str, float]],
    origen_posicion: Dict[str, str],
) -> Dict[str, Any]:
    """Aplica la lógica del Excel Courier. row contiene: marca, familia, stock, productos, origen, fob, qty, peso_unitario, extras, trafo, cambio_pa."""
    def _f(s: Any) -> float:
        if s is None or s == "": return 0.0
        try:
            return float(str(s).replace(",", "."))
        except (TypeError, ValueError):
            return 0.0

    fob = _f(row.get("fob"))
    qty = _f(row.get("qty"))
    peso_unit = _f(row.get("peso_unitario"))
    origen = str(row.get("origen") or "").strip()
    extras = _f(row.get("extras"))
    cambio_pa_manual = _f(row.get("cambio_pa"))

    dolar_oficial = params.get("dolar_oficial", 1475)
    dolar_blue = params.get("dolar_blue", 1450)
    dolar_despacho = params.get("dolar_despacho", 1475)
    ajuste_ana = params.get("ajuste_valor_ana", 1.01)

    fob_total = fob * qty
    peso_total = qty * peso_unit if qty > 0 and peso_unit > 0 else 0

    posicion_nom = str(row.get("posicion") or "").strip()
    if not posicion_nom and origen:
        posicion_nom = origen_posicion.get(origen, "Cambio PA")
    if not posicion_nom:
        posicion_nom = "Cambio PA"

    posicion = posicion_by_name.get(posicion_nom, {})
    derechos_rate = posicion.get("derechos", 0)
    estad_rate = posicion.get("estadisticas", 0)
    iva_rate = posicion.get("iva", 0.105)

    courier = courier_by_origen.get(origen)
    if not courier:
        for k, v in courier_by_origen.items():
            if origen in k or k in origen:
                courier = v
                break
    if not courier:
        courier = {}

    kg_real = _f(courier.get("kg_real"))
    if kg_real <= 0:
        vk = _f(courier.get("valor_kg", 0))
        dc = max(0.001, _f(courier.get("descuento", 1)))
        kg_real = vk / dc if vk > 0 else 0
    almacenaje = _f(courier.get("almacenaje"))
    seguro = _f(courier.get("seguro"))
    res_3244 = _f(courier.get("res_3244"))
    gas_ope = _f(courier.get("gas_ope"))
    env_dom = _f(courier.get("env_dom"))
    iibb = _f(courier.get("iibb"))

    L = derechos_rate * fob_total * dolar_oficial  # Derechos = tasa × FOB Total (en USD × Dólar)
    M = estad_rate * fob_total * dolar_oficial     # Estadística = tasa × FOB Total
    N = kg_real * peso_total * dolar_despacho
    O_val = almacenaje * peso_total * dolar_oficial
    P = res_3244 * dolar_oficial
    Q = seguro * dolar_oficial
    R = gas_ope * dolar_oficial
    S = env_dom * dolar_despacho
    # IVA FOB: monto_flete = Peso(kg) × 2.5; monto_seguro = (FOB tot + monto_flete) × 0.01; CIF = FOB + monto_flete + monto_seguro; IVA = CIF × tipo_iva × dolar_despacho
    peso_kg = peso_total / 1000 if peso_total > 0 else 0  # peso_total en gramos
    monto_flete = peso_kg * 2.5
    monto_seguro = (fob_total + monto_flete) * 0.01
    cif = fob_total + monto_flete + monto_seguro
    iva_fob_pesos = iva_rate * cif * dolar_despacho
    T = ((0.21 * O_val) + (0.21 * P) + (0.21 * Q) + (0.21 * R) + iva_fob_pesos) * ajuste_ana
    U = iibb * R
    V = L + M + N + O_val + P + Q + R + S + T + U
    Z = V + extras + (cambio_pa_manual * dolar_blue) - T  # Excel: Datos!$B$2 = Dólar Blue
    AA = Z / (fob_total * dolar_oficial) if fob_total > 0 else 0
    AC = (fob * (AA + 1)) * dolar_oficial
    AD = AC / dolar_oficial if dolar_oficial > 0 else 0

    venta_ml = _f(row.get("venta_ml"))
    ml_3cuotas = params.get("ml_3cuotas", 1.12149)
    ml_6cuotas = params.get("ml_6cuotas", 1.21067)
    ml_comision = params.get("ml_comision", 0.15)
    ml_debcre = params.get("ml_debcre", 0.006)
    iva_21 = params.get("iva_21", 0.21)
    ml_envios = params.get("ml_envios", 5823)  # ML - Envíos desde Datos
    ml_iibb_per = params.get("ml_iibb_per", 0.055)

    cuotas3 = venta_ml * ml_3cuotas if venta_ml > 0 else 0
    cuotas6 = venta_ml * ml_6cuotas if venta_ml > 0 else 0
    markup = ((venta_ml / AC) - 1) if venta_ml > 0 and AC > 0 else 0
    comi_ml = venta_ml * ml_comision if venta_ml > 0 else 0
    cobrado_ml = venta_ml - comi_ml if venta_ml > 0 else 0
    iva_impor = (T / qty) if venta_ml > 0 and qty > 0 else 0
    iva_meli = comi_ml - (comi_ml / 1.21) if venta_ml > 0 else 0
    iva_venta = venta_ml - (venta_ml / (iva_rate + 1)) if venta_ml > 0 else 0
    iva_total = iva_venta - iva_meli - iva_impor
    deb_cred = venta_ml * ml_debcre if venta_ml > 0 else 0
    iibb_per = venta_ml * ml_iibb_per if venta_ml > 0 else 0
    envio = ml_envios
    costo_vta = (((venta_ml - cobrado_ml) + (iva_total if iva_total > 0 else 0) + deb_cred + iibb_per + envio) / venta_ml) if venta_ml > 0 else 0
    margen = (cobrado_ml - AC - iva_total - deb_cred - iibb_per - envio) if venta_ml > 0 else 0
    margen_vta = (margen / venta_ml) if venta_ml > 0 else 0
    margen_costo = (margen / AC) if AC > 0 else 0

    def _fmt(x: float, decimals: int = 0) -> str:
        s = f"{x:,.{decimals}f}"
        return s.replace(",", "X").replace(".", ",").replace("X", ".")

    traida_pct = AA * 100 if AA else 0

    def _mon(s: str) -> str:
        return "$ " + s if s else ""

    return {
        **row,
        "fob_total": "u$ " + _fmt(fob_total, 2),
        "peso_total": _fmt(peso_total, 2),
        "derechos": _mon(_fmt(L, 0)),
        "estadistica": _mon(_fmt(M, 0)),
        "flete_int": _mon(_fmt(N, 0)),
        "almacenaje": _mon(_fmt(O_val, 0)),
        "res_3244": _mon(_fmt(P, 0)),
        "seguro": _mon(_fmt(Q, 0)),
        "gas_ope": _mon(_fmt(R, 0)),
        "env_dom": _mon(_fmt(S, 0)),
        "iva_lhs": _mon(_fmt(T, 0)),
        "iibb": _mon(_fmt(U, 0)),
        "total_courier": _mon(_fmt(V, 0)),
        "total": _mon(_fmt(Z, 0)),
        "traida_excel": _fmt(traida_pct, 2) + "%",
        "traida_real": _fmt(traida_pct, 2) + "%",
        "costo_pesos": _mon(_fmt(AC, 0)),
        "costo_usd": "u$ " + _fmt(AD, 2),
        "cuotas3": _mon(_fmt(cuotas3, 0)),
        "cuotas6": _mon(_fmt(cuotas6, 0)),
        "markup": _fmt(markup * 100, 1) + "%",
        "cobrado_ml": _mon(_fmt(cobrado_ml, 0)),
        "comi_ml": _mon(_fmt(comi_ml, 0)),
        "iva_impor": _mon(_fmt(iva_impor, 0)),
        "iva_meli": _mon(_fmt(iva_meli, 0)),
        "iva_venta": _mon(_fmt(iva_venta, 0)),
        "iva_total": _mon(_fmt(iva_total, 0)),
        "deb_cred": _mon(_fmt(deb_cred, 0)),
        "iibb_per": _mon(_fmt(iibb_per, 0)),
        "envio": _mon(_fmt(envio, 0)),
        "costo_vta": _fmt(costo_vta * 100, 1) + "%",
        "margen": _mon(_fmt(margen, 0)),
        "margen_vta": _fmt(margen_vta * 100, 1) + "%",
        "margen_costo": _fmt(margen_costo * 100, 1) + "%",
        "margen_raw": margen,
        "margen_vta_raw": margen_vta,
        "margen_costo_raw": margen_costo,
    }


def build_tab_importacion() -> None:
    """Pestaña Importación: tabla tipo Courier del Excel. Ingresás datos y calcula el resto."""
    user = require_login()
    if not user:
        return

    uid = user["id"]

    def _get(key: str) -> str:
        v = get_cotizador_param(key, uid)
        if v is not None:
            return v
        return COTIZADOR_DEFAULTS.get(key, "")

    def _get_tabla(nombre: str, default: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        r = get_cotizador_tabla(nombre, uid)
        return r if r else default

    def _parse_float(s: Any) -> float:
        if s is None or s == "": return 0.0
        try:
            return float(str(s).replace(",", "."))
        except (TypeError, ValueError):
            return 0.0

    origen_data = _get_tabla("origen", TABLA_ORIGEN_DEFAULT)
    posicion_data = _get_tabla("posicion", TABLA_POSICION_DEFAULT)
    courier_data = _get_tabla("courier", TABLA_COURIER_DEFAULT)

    params = {k: _parse_float(_get(k)) for k in COTIZADOR_DEFAULTS}
    posicion_by_name = {str(r.get("posicion", "")).strip(): {c: _parse_float(r.get(c)) for c in ["seguro", "flete", "derechos", "estadisticas", "iva", "despachante", "cambio_pa"]} for r in posicion_data if r.get("posicion")}
    courier_by_origen = {str(r.get("courier", "")).strip(): {c: _parse_float(r.get(c)) for c in ["valor_kg", "descuento", "kg_real", "almacenaje", "seguro", "res_3244", "gas_ope", "env_dom", "iibb"]} for r in courier_data if r.get("courier")}

    origen_posicion = {str(r.get("origen", "")).strip(): str(r.get("posicion", "")).strip() for r in origen_data if r.get("origen")}

    # Cargar filas guardadas o empezar con una vacía
    importacion_rows: List[Dict[str, Any]] = get_importacion_filas(user["id"])
    if not importacion_rows:
        importacion_rows = []

    sort_col_importacion: List[Optional[str]] = [None]
    sort_asc_importacion: List[bool] = [True]

    def _parse_sort_val(v: Any, col: str) -> Any:
        """Valor para ordenar: numérico si aplica, sino string."""
        if v is None or v == "":
            return 0.0 if col in ["fob", "qty", "peso_unitario", "extras", "cambio_pa", "venta_ml"] else ""
        s = str(v).replace("$", "").replace(".", "").replace(",", ".").strip()
        try:
            return float(s)
        except (ValueError, TypeError):
            return str(v).lower()

    def toggle_sort_importacion(col: str) -> None:
        if sort_col_importacion[0] == col:
            sort_asc_importacion[0] = not sort_asc_importacion[0]
        else:
            sort_col_importacion[0] = col
            sort_asc_importacion[0] = True
        sync_inputs_to_rows()
        rev = not sort_asc_importacion[0]
        importacion_rows.sort(key=lambda r: _parse_sort_val(r.get(col), col), reverse=rev)
        repintar()

    with ui.column().classes("w-full gap-2 p-2 flex flex-col"):
        ui.label("Importación - Cotizador Courier").classes("text-xl font-semibold")

        cols_input = ["productos", "origen", "impuestos", "fob", "qty", "peso_unitario", "extras", "trafo", "cambio_pa", "venta_ml"]
        cols_calc = ["fob_total", "peso_total", "derechos", "estadistica", "flete_int", "almacenaje", "res_3244", "seguro", "gas_ope", "env_dom", "iva_lhs", "iibb", "total_courier", "total", "traida_excel", "costo_pesos", "costo_usd", "cuotas3", "cuotas6", "markup", "cobrado_ml", "comi_ml", "iva_impor", "iva_meli", "iva_venta", "iva_total", "deb_cred", "iibb_per", "envio", "costo_vta", "margen", "margen_vta", "margen_costo"]
        headers_calc = ["FOB Tot", "Peso", "Derech", "Estad", "Flete", "Almac", "Res3244", "Seguro", "GasOp", "EnvDom", "IVA LHS", "IIBB", "Courier", "Total", "Traída", "Costo$ s/iva", "Costo u$ s/iva", "3ctas", "6ctas", "MarkUp", "Cobrado", "Comision", "IVAImp", "IVAMel", "IVAVta", "IVA", "Deb/Cred", "IIBB+PER", "Envio", "Cos Vta", "Margen$", "MargVta", "MargCos"]
        headers_input = ["Productos", "Origen", "Impuestos", "FOB", "QTY", "Peso U", "Extras", "Trafo", "Cam.PA", "Venta"]

        opciones_origen = [r.get("origen", "") for r in origen_data if r.get("origen")]
        opciones_impuestos = [r.get("posicion", "") for r in posicion_data if r.get("posicion")]
        cols_ocultas = ["derechos", "estadistica", "flete_int", "almacenaje", "res_3244", "seguro", "gas_ope", "env_dom", "iva_lhs", "iibb", "cuotas3", "cuotas6", "iva_impor", "iva_meli", "iva_venta"]
        cols_input_ocultas = ["extras", "trafo"]
        vista_completa = [False]

        table_container = ui.column().classes("w-full overflow-auto")
        input_rows_ref: List[Dict[str, Any]] = []

        def col_visible(col: str) -> bool:
            if col in cols_input_ocultas:
                return vista_completa[0]
            if col in cols_input:
                return True
            return vista_completa[0] or col not in cols_ocultas

        def _fmt_imp_usd(val: Any, decimals: int = 2) -> str:
            """Formato u$ con punto miles. decimals=2 para FOB, 0 para Cam.PA."""
            if val is None or str(val).strip() == "": return ""
            try:
                s = str(val).replace("u$", "").replace("$", "").strip()
                if "," in s:
                    s = s.replace(".", "").replace(",", ".")
                n = float(s) if s else 0
                fmt = f"{n:,.{decimals}f}" if decimals else f"{int(n):,}"
                return "u$ " + fmt.replace(",", "X").replace(".", ",").replace("X", ".")
            except (TypeError, ValueError):
                return str(val)

        def _fmt_imp_pesos(val: Any, decimals: int = 0) -> str:
            """Formato $ con punto miles, sin decimales para Venta."""
            if val is None or str(val).strip() == "": return ""
            try:
                s = str(val).replace("u$", "").replace("$", "").strip()
                if "," in s:
                    s = s.replace(".", "").replace(",", ".")
                n = float(s) if s else 0
                fmt = f"{int(n):,}"
                return "$ " + fmt.replace(",", ".")
            except (TypeError, ValueError):
                return str(val)

        def _parse_imp_prefixed(v: Any) -> str:
            """Parsea 'u$ 1.234,56', '$ 64.990' o '$ 10.000' a '1234.56' o '64990'."""
            if v is None or v == "": return ""
            s = str(v).replace("u$", "").replace("$", "").strip()
            if not s: return ""
            if "," in s:
                s = s.replace(".", "").replace(",", ".")
            elif "." in s:
                parts = s.split(".")
                if len(parts) == 2 and len(parts[1]) == 3:
                    s = s.replace(".", "")
                elif len(parts) > 2:
                    s = s.replace(".", "")
            try:
                n = float(s)
                return str(int(n)) if n == int(n) else f"{n:.2f}"
            except (TypeError, ValueError):
                return str(v).strip()

        def aplicar_estilo_fob_ml(inp: Any, es_fob: bool = False) -> None:
            """Actualiza negrita y rojo según si el input tiene valor (al cargar/editar)."""
            v = (inp.value or "").strip()
            base = "min-w-[52px] text-right" if es_fob else "min-w-[60px] text-right"
            if v:
                inp.classes(replace=base + " font-bold text-red-600")
                inp.style("font-weight: bold; color: rgb(220, 38, 38);")
            else:
                inp.classes(replace=base)
                inp.style("font-weight: normal; color: inherit;")

        def repintar() -> None:
            table_container.clear()
            input_rows_ref.clear()
            all_cols = cols_input + cols_calc
            all_headers = headers_input + headers_calc
            with table_container:
                with ui.element("table").classes("w-full border-collapse text-xs").style("table-layout: auto; white-space: nowrap;"):
                    with ui.element("thead"):
                        with ui.element("tr"):
                            for j, (c, h) in enumerate(zip(all_cols, all_headers)):
                                if j < 10:
                                    bg = "bg-sky-100 dark:bg-sky-800"
                                elif j < 27:
                                    bg = "bg-teal-100 dark:bg-teal-800"
                                elif j < 40:
                                    bg = "bg-sky-100 dark:bg-sky-800"
                                else:
                                    bg = "bg-teal-100 dark:bg-teal-800"
                                th_cls = f"font-semibold px-1 py-1 text-center border border-gray-300 whitespace-nowrap text-xs cursor-pointer {bg}"
                                if not col_visible(c):
                                    th_cls += " hidden"
                                th = ui.element("th").classes(th_cls)
                                th.on("click", lambda col=c: toggle_sort_importacion(col))
                                with th:
                                    ui.label(h)
                            with ui.element("th").classes("font-semibold px-0.5 py-1 text-center border border-gray-300 text-xs bg-slate-100 dark:bg-slate-700").style("min-width: 48px;"):
                                ui.label("Ordenar")
                            with ui.element("th").classes("font-semibold px-1 py-1 border border-gray-300 bg-slate-100 dark:bg-slate-700").style("min-width: 40px;"):
                                ui.label("×")
                    with ui.element("tbody"):
                        for i, r in enumerate(importacion_rows):
                            r_in: Dict[str, Any] = {}
                            with ui.element("tr"):
                                for c in cols_input:
                                    raw_val = r.get(c, "")
                                    if c == "fob":
                                        val = _fmt_imp_usd(raw_val, decimals=2)
                                    elif c == "cambio_pa":
                                        val = _fmt_imp_usd(raw_val, decimals=0)
                                    elif c == "venta_ml":
                                        val = _fmt_imp_pesos(raw_val)
                                    else:
                                        val = str(raw_val)
                                    td_cls = "p-0.5 border border-gray-200 min-w-0"
                                    if c in ("fob", "cambio_pa", "venta_ml"):
                                        td_cls += " text-right"
                                    elif c in ("qty", "peso_unitario"):
                                        td_cls += " text-center"
                                    if not col_visible(c):
                                        td_cls += " hidden"
                                    with ui.element("td").classes(td_cls):
                                        if c == "origen":
                                            opts = {o: o for o in opciones_origen if o}
                                            inp = ui.select(opts, value=val or (opciones_origen[0] if opciones_origen else "")).classes("min-w-[120px]").props("dense outlined")
                                        elif c == "impuestos":
                                            opts = {p: p for p in opciones_impuestos if p}
                                            inp = ui.select(opts, value=val or (opciones_impuestos[0] if opciones_impuestos else "")).classes("min-w-[130px]").props("dense outlined")
                                        elif c == "productos":
                                            inp = ui.input(value=val).classes("min-w-[130px]").props("dense")
                                        elif c == "fob":
                                            inp_cls = "min-w-[52px] text-right"
                                            if val:
                                                inp_cls += " font-bold text-red-600"
                                            inp = ui.input(value=val).classes(inp_cls).props("dense")
                                            inp.on_value_change(lambda inp_ref=inp: aplicar_estilo_fob_ml(inp_ref, es_fob=True))
                                            aplicar_estilo_fob_ml(inp, es_fob=True)
                                        elif c in ("qty", "peso_unitario"):
                                            inp = ui.input(value=val).classes("min-w-[40px]").props("dense").style("text-align: center")
                                        elif c == "cambio_pa":
                                            inp = ui.input(value=val).classes("min-w-[52px] text-right").props("dense")
                                        elif c in ("extras", "trafo"):
                                            inp = ui.input(value=val).classes("min-w-[55px]").props("dense")
                                        elif c == "venta_ml":
                                            inp_cls = "min-w-[60px] text-right"
                                            if val:
                                                inp_cls += " font-bold text-red-600"
                                            inp = ui.input(value=val).classes(inp_cls).props("dense")
                                            inp.on_value_change(lambda inp_ref=inp: aplicar_estilo_fob_ml(inp_ref, es_fob=False))
                                            aplicar_estilo_fob_ml(inp, es_fob=False)
                                        else:
                                            inp = ui.input(value=val).classes("min-w-[80px]").props("dense")
                                        r_in[c] = inp
                                for c in cols_calc:
                                    txt = str(r.get(c, ""))
                                    td_classes = "px-0.5 py-0.5 border border-gray-200 bg-gray-50 text-right whitespace-nowrap text-xs"
                                    if not col_visible(c):
                                        td_classes += " hidden"
                                    if c == "costo_pesos" or c == "costo_usd":
                                        td_classes += " font-bold text-blue-600"
                                    elif c in ("margen", "margen_vta", "margen_costo"):
                                        td_classes += " font-bold"
                                        raw = r.get(f"{c}_raw")
                                        if raw is not None:
                                            td_classes += " text-green-600" if raw >= 0 else " text-red-600"
                                    with ui.element("td").classes(td_classes):
                                        ui.label(txt)
                                with ui.element("td").classes("p-0.5 border border-gray-200 text-center").style("min-width: 48px;"):
                                    def subir(idx: int) -> None:
                                        if idx > 0:
                                            sync_inputs_to_rows()
                                            importacion_rows[idx], importacion_rows[idx - 1] = importacion_rows[idx - 1], importacion_rows[idx]
                                            repintar()
                                    def bajar(idx: int) -> None:
                                        if idx < len(importacion_rows) - 1:
                                            sync_inputs_to_rows()
                                            importacion_rows[idx], importacion_rows[idx + 1] = importacion_rows[idx + 1], importacion_rows[idx]
                                            repintar()
                                    with ui.row().classes("gap-0 justify-center"):
                                        ui.button("▲", on_click=lambda idx=i: subir(idx)).classes("min-w-0 px-0.5 text-xs").props("flat dense no-caps")
                                        ui.button("▼", on_click=lambda idx=i: bajar(idx)).classes("min-w-0 px-0.5 text-xs").props("flat dense no-caps")
                                with ui.element("td").classes("p-0.5 border border-gray-200 text-center").style("min-width: 40px;"):
                                    def borrar(idx: int) -> None:
                                        if 0 <= idx < len(importacion_rows):
                                            importacion_rows.pop(idx)
                                            repintar()
                                    ui.button("×", on_click=lambda idx=i: borrar(idx)).classes("text-red-600 font-bold text-lg min-w-0 px-1").props("flat dense no-caps")
                            input_rows_ref.append(r_in)

        def recalcular() -> None:
            params_actual = {k: _parse_float(_get(k)) for k in COTIZADOR_DEFAULTS}
            posicion_actual = _get_tabla("posicion", TABLA_POSICION_DEFAULT)
            courier_actual = _get_tabla("courier", TABLA_COURIER_DEFAULT)
            origen_actual = _get_tabla("origen", TABLA_ORIGEN_DEFAULT)
            posicion_by_name_actual = {str(r.get("posicion", "")).strip(): {c: _parse_float(r.get(c)) for c in ["seguro", "flete", "derechos", "estadisticas", "iva", "despachante", "cambio_pa"]} for r in posicion_actual if r.get("posicion")}
            courier_by_origen_actual = {str(r.get("courier", "")).strip(): {c: _parse_float(r.get(c)) for c in ["valor_kg", "descuento", "kg_real", "almacenaje", "seguro", "res_3244", "gas_ope", "env_dom", "iibb"]} for r in courier_actual if r.get("courier")}
            origen_posicion_actual = {str(r.get("origen", "")).strip(): str(r.get("posicion", "")).strip() for r in origen_actual if r.get("origen")}
            for i, r_in in enumerate(input_rows_ref):
                row_data = {}
                for c in cols_input:
                    v = r_in[c].value
                    if c in ("fob", "cambio_pa", "venta_ml"):
                        row_data[c] = _parse_imp_prefixed(v) if v else ""
                    else:
                        row_data[c] = v if v is not None else ""
                row_data["posicion"] = str(row_data.get("impuestos", "")).strip() or origen_posicion_actual.get(str(row_data.get("origen", "")).strip(), "Cambio PA")
                try:
                    calc = _calc_courier_row(row_data, params_actual, posicion_by_name_actual, courier_by_origen_actual, origen_posicion_actual)
                    for k, v in calc.items():
                        if i < len(importacion_rows):
                            importacion_rows[i][k] = v
                except Exception as e:
                    if i < len(importacion_rows):
                        importacion_rows[i]["error"] = str(e)
            repintar()

        def add_row() -> None:
            row = {}
            for c in cols_input + cols_calc:
                row[c] = "0" if c in ("extras", "trafo") else ""
            importacion_rows.append(row)
            recalcular()

        def sync_inputs_to_rows() -> None:
            """Copia los valores actuales de los inputs a importacion_rows antes de repintar."""
            for i, r_in in enumerate(input_rows_ref):
                if i < len(importacion_rows):
                    for c in cols_input:
                        if c in r_in:
                            v = r_in[c].value
                            if c in ("fob", "cambio_pa", "venta_ml"):
                                importacion_rows[i][c] = _parse_imp_prefixed(v)
                            else:
                                importacion_rows[i][c] = str(v) if v is not None else ""

        def toggle_vista() -> None:
            sync_inputs_to_rows()
            vista_completa[0] = not vista_completa[0]
            btn_vista.text = "Mínimo" if vista_completa[0] else "Completo"
            repintar()

        def guardar_tabla_importacion() -> None:
            sync_inputs_to_rows()
            user = require_login()
            if not user:
                ui.notify("Debe iniciar sesión", color="negative")
                return
            try:
                save_importacion_filas(user["id"], importacion_rows)
                ui.notify(f"Guardadas {len(importacion_rows)} filas", color="positive")
            except Exception as e:
                ui.notify(f"Error al guardar: {e}", color="negative")

        if not importacion_rows:
            add_row()
        else:
            repintar()
            recalcular()

        with ui.row().classes("gap-2 order-first"):
            ui.button("Calcular", on_click=recalcular, color="secondary")
            ui.button("Agregar Fila", on_click=add_row, color="primary")
            btn_vista = ui.button("Completo", on_click=toggle_vista, color="secondary")
            ui.button("Guardar Tabla", on_click=guardar_tabla_importacion, color="secondary")


def build_tab_datos() -> None:
    """Pestaña Datos del cotizador de importaciones. Todos los valores son editables."""
    user = require_login()
    if not user:
        return

    uid = user["id"]

    def _get(key: str) -> str:
        v = get_cotizador_param(key, uid)
        if v is not None:
            return v
        return COTIZADOR_DEFAULTS.get(key, "")

    def _get_tabla(nombre: str, default: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        r = get_cotizador_tabla(nombre, uid)
        return r if r else default

    with ui.column().classes("w-full gap-4 p-4"):
        ui.label("Datos del cotizador de importaciones").classes("text-2xl font-semibold")

        with ui.row().classes("w-full gap-4 flex-wrap"):
            # Dolar
            def _fmt_dolar_display(v: str) -> str:
                """Formatea valor numérico con punto para miles."""
                if not v or not str(v).strip():
                    return ""
                try:
                    n = float(str(v).replace(".", "").replace(",", "."))
                    return f"{int(n):,}".replace(",", ".")
                except (ValueError, TypeError):
                    return str(v)

            def _parse_dolar(s: Any) -> str:
                """Parsea valor de input ($ 1.475 o 1475) a string sin formato para guardar."""
                if s is None or s == "":
                    return ""
                raw = str(s).replace("$", "").replace(".", "").replace(",", ".").strip()
                try:
                    n = float(raw)
                    return str(int(n)) if n == int(n) else f"{n:.2f}"
                except (ValueError, TypeError):
                    return str(s).strip()

            with ui.card().classes("p-4 w-fit min-w-[180px]"):
                ui.label("Dólar").classes("text-lg font-semibold mb-3")
                inputs_params: Dict[str, Any] = {}
                for label, key in [
                    ("Oficial", "dolar_oficial"), ("Blue", "dolar_blue"), ("Sistema", "dolar_sistema"), ("Despacho", "dolar_despacho"),
                ]:
                    with ui.row().classes("items-center gap-2 py-0.5"):
                        ui.label(label).classes("min-w-[70px] text-sm")
                        val_raw = _get(key)
                        val_fmt = _fmt_dolar_display(val_raw) if val_raw else ""
                        val_display = f"$ {val_fmt}" if val_fmt else ""
                        inputs_params[key] = ui.input(value=val_display).classes("flex-1 max-w-[100px]").props("dense")

            def _fmt_usd_display(v: str) -> str:
                """Formatea valor numérico: punto para miles, coma para decimales."""
                if not v or not str(v).strip():
                    return ""
                try:
                    s = str(v).strip()
                    n = float(s.replace(",", "."))  # asumir . o , como decimal
                    if n == int(n):
                        return f"{int(n):,}".replace(",", ".")
                    return f"{n:.2f}".rstrip("0").rstrip(".").replace(".", ",")
                except (ValueError, TypeError):
                    return str(v)

            def _parse_usd(s: Any) -> str:
                """Parsea valor con u$ a string para guardar."""
                if s is None or s == "":
                    return ""
                raw = str(s).replace("u$", "").replace("$", "").replace(".", "").replace(",", ".").strip()
                try:
                    n = float(raw)
                    return str(int(n)) if n == int(n) else f"{n:.2f}"
                except (ValueError, TypeError):
                    return str(s).strip()

            # Traida por Kilo
            with ui.card().classes("p-4 w-fit min-w-[140px]"):
                ui.label("Traida por Kilo").classes("text-lg font-semibold mb-3")
                with ui.row().classes("items-center gap-2 py-0.5"):
                    ui.label("Kilo").classes("min-w-[60px] text-sm")
                    val_kilo = _get("kilo")
                    val_kilo_disp = f"u$ {_fmt_usd_display(val_kilo)}" if val_kilo else ""
                    inputs_params["kilo"] = ui.input(value=val_kilo_disp).classes("flex-1 max-w-[80px]").props("dense")

            # Mercadolibre
            with ui.card().classes("p-4 w-fit min-w-[220px]"):
                ui.label("Mercadolibre").classes("text-lg font-semibold mb-3")
                for label, key in [
                    ("ML - Comisión", "ml_comision"), ("Comision Fija (menor)", "ml_comision_fija_menor"),
                    ("ML - Deb/Cre", "ml_debcre"), ("ML - Sirtac", "ml_sirtac"), ("ML - Envíos", "ml_envios"),
                    ("ML - IIBB + PER", "ml_iibb_per"), ("ML - Envíos grat.", "ml_envios_gratuitos"),
                    ("ML - Cobrado", "ml_cobrado"),
                    ("Ganancia Neta sobre Venta", "ml_ganancia_neta_venta"),
                ]:
                    with ui.row().classes("items-center gap-2 py-0.5"):
                        ui.label(label).classes("min-w-[100px] text-sm")
                        inputs_params[key] = ui.input(value=_get(key)).classes("flex-1 max-w-[100px]").props("dense")

            # Cuotas y Promociones
            inputs_cuotas: Dict[str, Any] = {}
            with ui.card().classes("p-4 w-fit min-w-[200px]"):
                ui.label("Cuotas y Promociones").classes("text-lg font-semibold mb-3")
                for label, key in [
                    ("Cuotas 3x", "cuotas_3x"), ("Cuotas 6x", "cuotas_6x"),
                    ("Cuotas 9x", "cuotas_9x"), ("Cuotas 12x", "cuotas_12x"),
                    ("ML 3 cuotas", "ml_3cuotas"), ("ML 6 cuotas", "ml_6cuotas"),
                ]:
                    with ui.row().classes("items-center gap-2 py-0.5"):
                        ui.label(label).classes("min-w-[80px] text-sm")
                        inputs_cuotas[key] = ui.input(value=_get(key)).classes("flex-1 max-w-[100px]").props("dense")

            # Miami
            usd_keys_miami = {"valor_kg_miami", "almacenaje_dias_kg_miami"}
            with ui.card().classes("p-4 w-fit min-w-[220px]"):
                ui.label("Miami").classes("text-lg font-semibold mb-3")
                inputs_miami: Dict[str, Any] = {}
                for label, key in [
                    ("Valor KG Miami", "valor_kg_miami"), ("Almac. Días x Kg", "almacenaje_dias_kg_miami"),
                    ("Seguro Miami", "seguro_miami"),
                ]:
                    with ui.row().classes("items-center gap-2 py-0.5"):
                        ui.label(label).classes("min-w-[120px] text-sm")
                        val_raw = _get(key)
                        val_disp = f"u$ {_fmt_usd_display(val_raw)}" if key in usd_keys_miami and val_raw else (val_raw or "")
                        inputs_miami[key] = ui.input(value=val_disp).classes("flex-1 max-w-[100px]").props("dense")

            # China
            usd_keys_china = {"valor_kg_china", "almacenaje_dias_kg_china"}
            with ui.card().classes("p-4 w-fit min-w-[220px]"):
                ui.label("China").classes("text-lg font-semibold mb-3")
                inputs_china: Dict[str, Any] = {}
                for label, key in [
                    ("Valor KG China", "valor_kg_china"), ("Almac. Días x Kg", "almacenaje_dias_kg_china"),
                    ("Seguro China", "seguro_china"), ("Res 3244", "res_3244"), ("Gastos Operativos", "gastos_operativos"),
                    ("Gastos Origen", "gastos_origen"), ("Envío Domicilio", "envio_domicilio"), ("Ajuste valor ANA", "ajuste_valor_ana"),
                ]:
                    with ui.row().classes("items-center gap-2 py-0.5"):
                        ui.label(label).classes("min-w-[120px] text-sm")
                        val_raw = _get(key)
                        val_disp = f"u$ {_fmt_usd_display(val_raw)}" if key in usd_keys_china and val_raw else (val_raw or "")
                        inputs_china[key] = ui.input(value=val_disp).classes("flex-1 max-w-[100px]").props("dense")

        def guardar_params() -> None:
            dolar_keys = {"dolar_oficial", "dolar_blue", "dolar_sistema", "dolar_despacho", "ml_comision_fija_menor"}
            usd_keys = {"kilo", "valor_kg_miami", "almacenaje_dias_kg_miami", "valor_kg_china", "almacenaje_dias_kg_china"}
            for key, inp in {**inputs_params, **inputs_cuotas, **inputs_miami, **inputs_china}.items():
                val = str(inp.value or "").strip()
                if key in dolar_keys:
                    val = _parse_dolar(val)
                elif key in usd_keys:
                    val = _parse_usd(val)
                set_cotizador_param(key, val, uid)
            ui.notify("Parámetros guardados", color="positive")

        ui.button("Guardar parámetros", on_click=guardar_params, color="primary").classes("mb-2")

        # Eliminar tablas obsoletas de la BD si existían
        for k in ["tabla_origen", "tabla_cambio_pa", "tabla_derechos", "tabla_estadisticas"]:
            delete_cotizador_param(k, uid)

        # Tablas editables (headers = encabezados de columnas)
        tabla_trafo_gramos_data = list(_get_tabla("trafo_gramos", TABLA_TRAFO_GRAMOS_DEFAULT))
        tabla_posicion_data = list(_get_tabla("posicion", TABLA_POSICION_DEFAULT))
        tabla_envios_data = list(_get_tabla("envios_ml", TABLA_ENVIOS_ML_DEFAULT))
        tabla_courier_data = list(_get_tabla("courier", TABLA_COURIER_DEFAULT))

        def _parse_num(s: Any) -> float:
            if s is None or s == "": return 0.0
            try:
                return float(str(s).replace(",", "."))
            except (TypeError, ValueError):
                return 0.0

        def _fmt_pesos_display(val: Any) -> str:
            """Formatea valor en pesos: $ y punto para miles."""
            if val is None or str(val).strip() == "":
                return ""
            try:
                n = float(str(val).replace(".", "").replace(",", "."))
                return f"$ {int(n):,}".replace(",", ".") if n == int(n) else f"$ {n:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            except (ValueError, TypeError):
                return str(val)

        def _parse_pesos_fmt(s: Any) -> str:
            """Parsea valor con $ y puntos a string para guardar."""
            if s is None or s == "":
                return ""
            raw = str(s).replace("$", "").replace(".", "").replace(",", ".").strip()
            try:
                n = float(raw)
                return str(int(n)) if n == int(n) else f"{n:.2f}"
            except (ValueError, TypeError):
                return str(s).strip()

        def _tabla_editable(nombre: str, cols: List[str], headers: List[str], data: List[Dict[str, Any]], titulo: str, compact: bool = False, col_widths: Optional[List[str]] = None, card_ancho: Optional[str] = None, computed: Optional[Dict[str, Any]] = None, computed_deps: Optional[Dict[str, List[str]]] = None, ordenable: bool = True, col_formato: Optional[Dict[str, str]] = None) -> None:
            card_classes = "p-4"
            if card_ancho:
                card_classes += f" {card_ancho}"
            elif compact:
                card_classes += " flex-1 min-w-[140px] max-w-[220px]"
            else:
                card_classes += " w-full"
            with ui.card().classes(card_classes):
                ui.label(titulo).classes("text-lg font-semibold mb-3")
                cont = ui.column().classes("w-full gap-2")
                edit_rows: List[Dict[str, Any]] = []

                def repintar() -> None:
                    cont.clear()
                    edit_rows.clear()
                    with cont:
                        with ui.element("table").classes("w-full border-collapse text-sm").style("table-layout: fixed;"):
                            with ui.element("thead"):
                                with ui.element("tr").classes("bg-blue-100 dark:bg-blue-900"):
                                    for j, h in enumerate(headers):
                                        th = ui.element("th").classes("font-semibold px-1.5 py-0.5 text-left border border-gray-300")
                                        if col_widths and j < len(col_widths):
                                            th.style(col_widths[j])
                                        with th:
                                            ui.label(h)
                                    if ordenable:
                                        with ui.element("th").classes("font-semibold px-0.5 py-0.5 text-center border border-gray-300 text-xs").style("min-width: 48px; width: 48px;"):
                                            ui.label("Ordenar")
                                    with ui.element("th").classes("font-semibold px-0.5 py-0.5 text-center border border-gray-300 text-xs").style("min-width: 52px; width: 52px;"):
                                        ui.label("Borrar")
                            with ui.element("tbody"):
                                for idx, row in enumerate(data):
                                    rinputs: Dict[str, Any] = {}
                                    with ui.element("tr"):
                                        for col in cols:
                                            val = str(row.get(col, ""))
                                            if col_formato and col in col_formato:
                                                val = _fmt_pesos_display(val) if val else ""
                                            with ui.element("td").classes("p-0.5 border border-gray-200"):
                                                if computed and col in computed:
                                                    disp = computed[col](row) if callable(computed[col]) else str(row.get(col, ""))
                                                    if col_formato and col in col_formato:
                                                        disp = _fmt_pesos_display(disp) if disp else ""
                                                    lbl = ui.label(disp).classes("text-xs")
                                                    rinputs[col] = lbl
                                                else:
                                                    inp = ui.input(value=val).classes("w-full border-0 text-xs").props("dense")
                                                    rinputs[col] = inp
                                        # Actualizar labels calculados cuando cambian las dependencias
                                        if computed and computed_deps:
                                            def make_updater(comp_col: str, lbl_ref: Any) -> None:
                                                def upd() -> None:
                                                    row = {}
                                                    for c in cols:
                                                        if c in (computed or {}):
                                                            continue
                                                        raw = str(rinputs[c].value or "")
                                                        if col_formato and c in col_formato:
                                                            raw = _parse_pesos_fmt(raw)
                                                        row[c] = raw
                                                    disp = computed[comp_col](row)
                                                    if col_formato and comp_col in col_formato:
                                                        disp = _fmt_pesos_display(disp) if disp else ""
                                                    lbl_ref.text = disp
                                                return upd
                                            for comp_col, deps in computed_deps.items():
                                                if comp_col in rinputs:
                                                    upd = make_updater(comp_col, rinputs[comp_col])
                                                    for d in deps:
                                                        if d in rinputs and hasattr(rinputs[d], "on_value_change"):
                                                            rinputs[d].on_value_change(upd)
                                        if ordenable:
                                            with ui.element("td").classes("p-0.5 border border-gray-200 text-center").style("min-width: 48px; width: 48px;"):
                                                def subir(i: int) -> None:
                                                    if i > 0:
                                                        data[i], data[i - 1] = data[i - 1], data[i]
                                                        repintar()
                                                def bajar(i: int) -> None:
                                                    if i < len(data) - 1:
                                                        data[i], data[i + 1] = data[i + 1], data[i]
                                                        repintar()
                                                with ui.row().classes("gap-0 justify-center"):
                                                    ui.button("▲", on_click=lambda i=idx: subir(i)).classes("min-w-0 px-0.5 text-xs").props("flat dense no-caps")
                                                    ui.button("▼", on_click=lambda i=idx: bajar(i)).classes("min-w-0 px-0.5 text-xs").props("flat dense no-caps")
                                        with ui.element("td").classes("p-0.5 border border-gray-200 text-center").style("min-width: 52px; width: 52px;"):
                                            def borrar_fila(i: int) -> None:
                                                if 0 <= i < len(data):
                                                    data.pop(i)
                                                    repintar()
                                            ui.button("×", on_click=lambda i=idx: borrar_fila(i)).classes("text-red-600 font-bold text-sm min-w-0 px-1").props("flat dense no-caps")
                                    edit_rows.append(rinputs)

                repintar()

                def agregar_fila() -> None:
                    data.append({c: "" for c in cols})
                    repintar()

                def guardar_tabla() -> None:
                    new_data = []
                    for rinputs in edit_rows:
                        row: Dict[str, Any] = {}
                        for c in cols:
                            if computed and c in computed:
                                continue
                            raw = str(rinputs[c].value or "")
                            if col_formato and c in col_formato:
                                raw = _parse_pesos_fmt(raw)
                            row[c] = raw
                        if computed:
                            for c in computed:
                                row[c] = computed[c](row)
                        new_data.append(row)
                    set_cotizador_tabla(nombre, new_data, uid)
                    data.clear()
                    data.extend(new_data)
                    repintar()
                    ui.notify(f"Tabla {titulo} guardada", color="positive")

                with ui.row().classes("gap-2"):
                    ui.button("Agregar Fila", on_click=agregar_fila, color="primary")
                    ui.button("Guardar tabla", on_click=guardar_tabla, color="secondary")

        with ui.row().classes("w-full gap-4 flex-wrap"):
            _tabla_editable("trafo_gramos", ["trafo", "gramos"], ["Trafo", "Gramos"], tabla_trafo_gramos_data, "Trafo y Gramos", card_ancho="w-fit")
            _tabla_editable("posicion", ["posicion", "seguro", "flete", "derechos", "estadisticas", "iva", "despachante", "cambio_pa"],
                ["Posicion", "Seguro", "Flete", "Derechos", "Estadisticas", "IVA", "Despachante", "Cambio PA"],
                tabla_posicion_data, "Tasas por Posición", card_ancho="w-fit")
            _tabla_editable("envios_ml", ["envio", "importe", "porc_10", "costo"],
                ["Envios ML", "Importe", "0,10", "Costo"], tabla_envios_data, "Costos envío MercadoLibre",
                computed={"costo": lambda r: str(int(_parse_num(r.get("importe")) + _parse_num(r.get("porc_10"))))},
                computed_deps={"costo": ["importe", "porc_10"]}, card_ancho="w-fit",
                col_formato={"importe": "$", "porc_10": "$", "costo": "$"})
            _tabla_editable("courier", ["courier", "valor_kg", "descuento", "kg_real", "almacenaje", "seguro", "res_3244", "gas_ope", "env_dom", "iibb", "cif"],
                ["Courier", "Valor KG", "Descuento", "KG Real", "Almacenaje", "Seguro", "Res 3244", "Gas Ope", "Env Dom", "IIBB", "CIF"],
                tabla_courier_data, "Costos por Courier",
                computed={"kg_real": lambda r: f"{_parse_num(r.get('valor_kg')) / max(0.001, _parse_num(r.get('descuento'))):.2f}"},
                computed_deps={"kg_real": ["valor_kg", "descuento"]}, card_ancho="w-fit")


# ==========================
# CALLBACK OAUTH (ruta HTTP directa para evitar 404 con NiceGUI)
# ==========================


def _get_base_url(request: Request) -> str:
    """Obtiene la URL base del request (para redirect_uri). Soporta proxy con X-Forwarded-*."""
    forwarded_proto = request.headers.get("X-Forwarded-Proto")
    forwarded_host = request.headers.get("X-Forwarded-Host")
    if forwarded_host:
        scheme = (forwarded_proto or "https").rstrip("/")
        return f"{scheme}://{forwarded_host.split(',')[0].strip()}"
    if request.url:
        return str(request.base_url).rstrip("/")
    return "http://localhost:8083"


async def _ml_callback_redirect(request: Request) -> RedirectResponse:
    """Ruta HTTP directa: redirige a / con el code para que la página principal procese el OAuth."""
    code = request.query_params.get("code")
    error_param = request.query_params.get("error")
    error_desc = request.query_params.get("error_description", "")
    # Pasar la URL recibida para depurar cuando falta el code
    url_recibida = str(request.url) if request.url else ""
    if error_param:
        return RedirectResponse(url=f"/?ml_oauth_error={error_param}&ml_oauth_error_desc={error_desc}", status_code=302)
    if code:
        return RedirectResponse(url=f"/?ml_oauth_code={code}", status_code=302)
    # No vino code: pasar la URL para mostrarla en el mensaje de error
    from urllib.parse import quote
    return RedirectResponse(
        url=f"/?ml_oauth_error=no_code&ml_oauth_error_desc={quote(url_recibida[:200])}",
        status_code=302,
    )


# Registrar la ruta ANTES de las páginas para que responda a GET /ml/callback
app.add_api_route("/ml/callback", _ml_callback_redirect, methods=["GET"])


async def _qb_callback_redirect(request: Request) -> RedirectResponse:
    """Callback OAuth de QuickBooks: redirige a / con el code para procesar el token. Usa URL absoluta para mantener el host (IP o ngrok)."""
    base = _get_base_url(request)
    code = request.query_params.get("code")
    realm_id = request.query_params.get("realmId")
    state = request.query_params.get("state")
    error_param = request.query_params.get("error")
    error_desc = request.query_params.get("error_description", "")
    if error_param:
        from urllib.parse import quote
        return RedirectResponse(url=f"{base}/?qb_oauth_error={error_param}&qb_oauth_error_desc={quote(error_desc[:300])}", status_code=302)
    if code:
        params = f"qb_oauth_code={code}"
        if realm_id:
            params += f"&qb_realm_id={realm_id}"
        if state:
            params += f"&qb_state={state}"
        return RedirectResponse(url=f"{base}/?{params}", status_code=302)
    from urllib.parse import quote
    url_recibida = str(request.url) if request.url else ""
    return RedirectResponse(url=f"{base}/?qb_oauth_error=no_code&qb_oauth_error_desc={quote(url_recibida[:200])}", status_code=302)


app.add_api_route("/qb/callback", _qb_callback_redirect, methods=["GET"])


# ==========================
# ARRANQUE DE LA APP
# ==========================


@ui.page("/")
def index(request: Request) -> None:  # type: ignore[override]
    root = ui.column().classes("w-full")

    # Procesar callback de OAuth
    ml_code = request.query_params.get("ml_oauth_code")
    ml_error = request.query_params.get("ml_oauth_error")
    qb_oauth_code = request.query_params.get("qb_oauth_code")
    qb_oauth_error = request.query_params.get("qb_oauth_error")
    qb_realm_id = request.query_params.get("qb_realm_id", "")
    if qb_oauth_error:
        with root:
            ui.label(f"❌ Error de QuickBooks: {qb_oauth_error}").classes("text-negative text-lg mb-4")
            if request.query_params.get("qb_oauth_error_desc"):
                from urllib.parse import unquote
                desc = unquote(request.query_params.get("qb_oauth_error_desc", ""))
                ui.label(f"Detalle: {desc}").classes("text-sm text-gray-600 mb-2")
            ui.link("Volver al inicio", "/").classes("text-primary")
        return
    if ml_error:
        with root:
            ui.label(f"❌ Error de MercadoLibre: {ml_error}").classes("text-negative text-lg mb-4")
            if request.query_params.get("ml_oauth_error_desc"):
                from urllib.parse import unquote
                desc = unquote(request.query_params.get("ml_oauth_error_desc", ""))
                ui.label(f"URL recibida: {desc}").classes("text-sm text-gray-600 mb-2")
            if ml_error == "no_code":
                ui.label(
                    "El parámetro 'code' no llegó al servidor. Posibles causas:\n"
                    "• Ngrok: si viste la página 'Visit Site', haz clic ahí y vuelve a intentar.\n"
                    "• Redirect URI: en MercadoLibre Developers debe ser EXACTAMENTE la misma URL que en tu .env (con /ml/callback).\n"
                    "• Prueba en ventana de incógnito o con otro navegador."
                ).classes("text-gray-600 mb-4 whitespace-pre-line")
            ui.link("Volver al inicio", "/").classes("text-primary")
        return
    if ml_code:
        user = get_current_user()
        if not user:
            with root:
                ui.label("Debes iniciar sesión en BDC systems antes de vincular MercadoLibre.").classes("text-lg mb-4")
                ui.link("Ir a inicio de sesión", "/").classes("text-primary")
            return
        app_creds = get_ml_app_credentials(user["id"])
        if app_creds:
            client_id = app_creds["client_id"]
            client_secret = app_creds["client_secret"]
            redirect_uri = app_creds.get("redirect_uri") or os.getenv("ML_REDIRECT_URI", "http://localhost:8083/ml/callback")
        else:
            client_id = os.getenv("ML_CLIENT_ID")
            client_secret = os.getenv("ML_CLIENT_SECRET")
            redirect_uri = os.getenv("ML_REDIRECT_URI", "http://localhost:8083/ml/callback")
        if not client_id or not client_secret:
            with root:
                ui.label("❌ Configurá tu App ID y Client Secret en Configuración antes de conectar.").classes("text-negative mb-4")
            return
        redirect_uri = (redirect_uri or "").strip() or "http://localhost:8083/ml/callback"
        try:
            resp = requests.post(
                "https://api.mercadolibre.com/oauth/token",
                data={
                    "grant_type": "authorization_code",
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "code": ml_code,
                    "redirect_uri": redirect_uri,
                },
                headers={"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"},
                timeout=10,
            )
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            resp_err = getattr(e, "response", None)
            err_msg = str(e)
            try:
                if resp_err is not None:
                    err_body = resp_err.json()
                    err_msg = err_body.get("message") or err_body.get("error") or str(err_body)
            except Exception:
                if resp_err is not None and resp_err.text:
                    err_msg = resp_err.text[:500]
            with root:
                ui.label(f"❌ Error al obtener token: {e}").classes("text-negative text-lg mb-2")
                ui.label(f"Detalle: {err_msg}").classes("text-sm text-gray-600 mb-2")
                causas = (
                    "Posibles causas:\n"
                    "• redirect_uri debe coincidir EXACTAMENTE con el configurado en MercadoLibre Developers.\n"
                    "• Si tu app tiene PKCE habilitado, desactivá PKCE en la app o recreá la app sin PKCE.\n"
                    "• El código de autorización se usa una sola vez; si recargaste la página, volvé a Conectar."
                )
                if "invalid" in err_msg.lower() or "validating grant" in err_msg.lower():
                    causas += (
                        "\n\n⚠️ ¿Intentabas conectar QuickBooks? Si es así, el Redirect URI en developer.intuit.com debe ser /qb/callback, NO /ml/callback. Cada app (ML y QB) tiene su propia URL."
                    )
                ui.label(causas).classes("text-sm text-gray-600 mb-4 whitespace-pre-line")
                ui.link("Volver a Configuración", "/").classes("text-primary")
            return
        except Exception as e:
            with root:
                ui.label(f"❌ Error al obtener token: {e}").classes("text-negative mb-4")
            return
        data = resp.json()
        access_token = data.get("access_token")
        refresh_token = data.get("refresh_token")
        expires_in = data.get("expires_in")
        if not access_token:
            with root:
                ui.label(f"❌ Respuesta inesperada: {data}").classes("text-negative mb-4")
            return
        expires_at = None
        if isinstance(expires_in, (int, float)):
            expires_at = (datetime.utcnow() + timedelta(seconds=int(expires_in))).isoformat()
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM ml_credentials WHERE user_id = ?", (user["id"],))
        cur.execute(
            "INSERT INTO ml_credentials (user_id, access_token, refresh_token, expires_at, raw_data) VALUES (?, ?, ?, ?, ?)",
            (user["id"], access_token, refresh_token, expires_at, json.dumps(data, ensure_ascii=False)),
        )
        conn.commit()
        conn.close()
        # Redirigir a / sin el code para limpiar la URL (el usuario verá el panel y una notificación)
        return RedirectResponse(url="/", status_code=302)

    if qb_oauth_code:
        user = get_current_user()
        if not user:
            with root:
                ui.label("Debes iniciar sesión en BDC systems antes de vincular QuickBooks.").classes("text-lg mb-4")
                ui.link("Ir a inicio de sesión", "/").classes("text-primary")
            return
        qb_app_creds = get_qb_app_credentials(user["id"])
        if not qb_app_creds:
            with root:
                ui.label("❌ Configurá Client ID y Client Secret de QuickBooks en Configuración antes de conectar.").classes("text-negative mb-4")
                ui.link("Volver a Configuración", "/").classes("text-primary")
            return
        client_id = qb_app_creds["client_id"]
        client_secret = qb_app_creds["client_secret"]
        base_url = _get_base_url(request)
        redirect_uri = base_url.rstrip("/") + "/qb/callback"
        import base64
        auth_str = f"{client_id}:{client_secret}"
        auth_b64 = base64.b64encode(auth_str.encode()).decode()
        try:
            resp = requests.post(
                "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
                data={
                    "grant_type": "authorization_code",
                    "code": qb_oauth_code,
                    "redirect_uri": redirect_uri,
                },
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Authorization": f"Basic {auth_b64}",
                },
                timeout=15,
            )
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            resp_err = getattr(e, "response", None)
            err_msg = str(e)
            try:
                if resp_err is not None:
                    err_body = resp_err.json()
                    err_msg = err_body.get("error_description") or err_body.get("message") or err_body.get("error") or str(err_body)
            except Exception:
                if resp_err is not None and resp_err.text:
                    err_msg = resp_err.text[:500]
            with root:
                ui.label("❌ Error al obtener token de QuickBooks").classes("text-negative text-lg mb-2")
                ui.label(f"Detalle: {err_msg}").classes("text-sm text-gray-600 mb-2")
                ui.label(
                    "Posibles causas:\n"
                    "• Redirect URI: en developer.intuit.com → Keys debe ser EXACTAMENTE la misma URL que en Configuración (con /qb/callback).\n"
                    "• NO uses /ml/callback para QuickBooks; debe ser /qb/callback.\n"
                    "• El código de autorización se usa una sola vez; si recargaste, volvé a Conectar."
                ).classes("text-sm text-gray-600 mb-4 whitespace-pre-line")
                ui.link("Volver a Configuración", "/").classes("text-primary")
            return
        except Exception as e:
            with root:
                ui.label(f"❌ Error al obtener token de QuickBooks: {e}").classes("text-negative mb-4")
                ui.link("Volver al inicio", "/").classes("text-primary")
            return
        data = resp.json()
        access_token = data.get("access_token")
        refresh_token = data.get("refresh_token")
        expires_in = data.get("expires_in")
        if not access_token:
            with root:
                ui.label(f"❌ Respuesta inesperada de Intuit: {data}").classes("text-negative mb-4")
            return
        expires_at = None
        if isinstance(expires_in, (int, float)):
            expires_at = (datetime.utcnow() + timedelta(seconds=int(expires_in))).isoformat()
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM qb_tokens WHERE user_id = ?", (user["id"],))
        cur.execute(
            "INSERT INTO qb_tokens (user_id, access_token, refresh_token, expires_at, realm_id, raw_data) VALUES (?, ?, ?, ?, ?, ?)",
            (user["id"], access_token, refresh_token, expires_at, qb_realm_id or None, json.dumps(data, ensure_ascii=False)),
        )
        conn.commit()
        conn.close()
        return RedirectResponse(url="/", status_code=302)

    user = get_current_user()
    if user:
        show_main_layout(root)
    else:
        show_login_screen(root)


def _iniciar_ngrok(port: int) -> None:
    """Lanza ngrok en segundo plano para exponer el puerto local."""
    try:
        subprocess.Popen(
            ["ngrok", "http", str(port)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0,
        )
        time.sleep(1.5)
        try:
            r = requests.get("http://127.0.0.1:4040/api/tunnels", timeout=2)
            if r.ok:
                data = r.json()
                tunnels = data.get("tunnels", [])
                for t in tunnels:
                    if t.get("public_url", "").startswith("https://"):
                        print(f"  Ngrok: {t['public_url']} -> http://127.0.0.1:{port}")
                        break
        except Exception:
            pass
    except FileNotFoundError:
        print("  Ngrok no encontrado en PATH. Ejecutá 'ngrok http', PORT manualmente si lo necesitás.")
    except Exception as e:
        print(f"  No se pudo iniciar ngrok: {e}")


def _arreglar_storage_nicegui() -> None:
    """Crea .nicegui y elimina archivos de storage corruptos para que NiceGUI los recree."""
    storage_dir = Path(__file__).parent / ".nicegui"
    storage_dir.mkdir(exist_ok=True)
    for f in storage_dir.glob("storage-*.json"):
        try:
            if f.stat().st_size == 0:
                f.unlink()
            else:
                json.loads(f.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            try:
                f.unlink()
            except OSError:
                pass


def main() -> None:
    # Cargar .env desde el directorio del script (importante cuando se ejecuta como servicio o desde otro CWD)
    env_path = Path(__file__).parent / ".env"
    load_dotenv(env_path)
    init_db()
    _arreglar_storage_nicegui()
    port = int(os.getenv("PORT", 8083))
    # En Render/cloud: PORT lo define la plataforma, no iniciar ngrok
    es_produccion = "PORT" in os.environ or os.getenv("RENDER") == "true"
    if not es_produccion and os.getenv("NGROK_AUTO_START", "0").strip().lower() in ("1", "true", "yes"):
        print("Iniciando ngrok...")
        _iniciar_ngrok(port)
    # host 0.0.0.0 necesario para que Render/cloud pueda acceder al servicio
    ui.run(
        title="BDC systems",
        reload=False,
        host="0.0.0.0" if es_produccion else "127.0.0.1",
        port=port,
        storage_secret=os.getenv("STORAGE_SECRET", "cambia-esta-clave"),
        reconnect_timeout=120,  # Evita "Connection lost" durante carga pesada (Precios con muchos productos)
        message_history_length=2000,  # Más mensajes al reconectar para restaurar UI
    )


if __name__ == "__main__":
    main()
