from __future__ import annotations

import hashlib
import logging

logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
from math import ceil
import html
import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import os
import subprocess
import tempfile
import time
import requests
from dotenv import load_dotenv
from fastapi import Request
from fastapi.responses import RedirectResponse
from nicegui import app, background_tasks, context, run, ui

DB_PATH = Path(__file__).with_name("app.db")


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


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def create_user(username: str, password: str) -> Optional[str]:
    """Crea un usuario. Devuelve mensaje de error o None si fue bien."""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO users (username, password_hash, created_at) VALUES (?, ?, ?)",
            (username, hash_password(password), datetime.utcnow().isoformat()),
        )
        conn.commit()
        return None
    except sqlite3.IntegrityError:
        return "El usuario ya existe."
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


def ml_get_my_items(access_token: str) -> Dict[str, Any]:
    """Obtiene TODAS las publicaciones del vendedor desde la API de MercadoLibre (paginado)."""
    base = "https://api.mercadolibre.com"
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}

    # 1. Obtener el user_id de ML del token
    me = requests.get(f"{base}/users/me", headers=headers, timeout=10)
    me.raise_for_status()
    ml_user_id = me.json().get("id")
    if not ml_user_id:
        return {"results": [], "paging": {"total": 0}, "error": "No se pudo obtener el usuario de ML"}

    # 2. Listar TODOS los IDs de publicaciones (paginado: 50 por página)
    item_ids = []
    offset = 0
    limit = 50
    while True:
        search = requests.get(
            f"{base}/users/{ml_user_id}/items/search",
            headers=headers,
            params={"limit": limit, "offset": offset, "status": "active"},
            timeout=15,
        )
        search.raise_for_status()
        search_data = search.json()
        chunk = search_data.get("results", [])
        if not chunk:
            break
        item_ids.extend(chunk)
        if len(chunk) < limit:
            break
        offset += limit

    paging = search_data.get("paging", {})
    total = paging.get("total", len(item_ids))

    if not item_ids:
        return {"results": [], "paging": {"total": total}}

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
            for att in body.get("attributes") or []:
                aid = att.get("id") or ""
                if aid in ("BRAND", "Marca"):
                    val = att.get("value_name") or att.get("value_id")
                    marca = str(val) if val is not None else ""
                elif aid in ("COLOR", "Color", "Colour"):
                    val = att.get("value_name") or att.get("value_id")
                    if val:
                        color = str(val)
                        break
            if not color:
                tit = (body.get("title") or "").lower()
                colores = ["negro", "blanco", "azul", "rojo", "gris", "verde", "amarillo", "naranja", "rosa", "marron", "beige", "celeste", "plateado", "dorado", "violeta", "multicolor"]
                for c in colores:
                    if c in tit:
                        color = c.capitalize()
                        break
            catalog_listing = body.get("catalog_listing") is True
            return {
                "id": body.get("id"),
                "title": body.get("title", ""),
                "price": body.get("price"),
                "sale_price": body.get("sale_price"),
                "available_quantity": body.get("available_quantity"),
                "sold_quantity": body.get("sold_quantity"),
                "status": body.get("status", ""),
                "permalink": body.get("permalink", ""),
                "catalog_product_id": body.get("catalog_product_id"),
                "catalog_listing": catalog_listing,
                "marca": marca or "—",
                "color": color or "—",
            }

        for item_data in items_resp.json():
            if isinstance(item_data, dict) and item_data.get("code") == 200:
                body = item_data.get("body", {})
                all_items.append(_item_from_body(body))
            elif isinstance(item_data, dict) and "body" in item_data:
                body = item_data["body"]
                all_items.append(_item_from_body(body))

    return {"results": all_items, "paging": {"total": total}}


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


def ml_get_orders(access_token: str, seller_id: str, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
    """Lista órdenes del vendedor. Pagina hasta `limit` (máx 50 por request).
    sort=date_desc para órdenes más recientes primero."""
    import logging
    log = logging.getLogger(__name__)
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    page_size = 50

    for url, extra in [
        ("https://api.mercadolibre.com/orders/search", {"seller": seller_id}),
        ("https://api.mercadolibre.com/orders/search", {"seller": seller_id, "caller.id": seller_id}),
        ("https://api.mercadolibre.com/marketplace/orders/search", {"seller.id": seller_id}),
        ("https://api.mercadolibre.com/marketplace/orders/search", {"seller.id": seller_id, "caller.id": seller_id}),
    ]:
        all_flat: List[Dict[str, Any]] = []
        off = offset
        while len(all_flat) < limit:
            params = {**extra, "limit": page_size, "offset": off, "sort": "date_desc"}
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

                # IDs sueltos (int): obtener cada orden
                if isinstance(raw[0], (int, float)):
                    for oid in raw[:page_size]:
                        try:
                            r = requests.get(f"https://api.mercadolibre.com/orders/{int(oid)}", headers=headers, timeout=10)
                            if r.status_code == 200:
                                all_flat.append(r.json())
                        except Exception:
                            pass
                    off += len(raw)
                    if len(raw) < page_size:
                        break
                    continue

                # Aplanar si hay orders anidados
                for r in raw:
                    if not isinstance(r, dict):
                        continue
                    nested = r.get("orders") or []
                    if nested:
                        for o in nested:
                            if isinstance(o, dict):
                                all_flat.append(o)
                    else:
                        all_flat.append(r)
                off += len(raw)
                if len(raw) < page_size:
                    break
            except Exception as ex:
                log.debug("ML orders %s: %s", url.split("/")[-1], ex)
                break

        if all_flat:
            log.debug("ML orders: %d órdenes desde %s", len(all_flat), url.split("/")[-1])
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
                            if not username.value or not password.value:
                                ui.notify("Completa usuario y contraseña", color="negative")
                                return
                            error = create_user(username.value, password.value)
                            if error:
                                ui.notify(error, color="negative")
                                return
                            ui.notify("Usuario creado, ahora puedes iniciar sesión", color="positive")

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
        # Barra gris con menús de navegación (Home, Mis productos, Configuración) y usuario
        with ui.row().classes("w-full items-center justify-between q-pa-md bg-grey-2"):
            with ui.tabs() as tabs:
                tab_home = ui.tab("Home")
                tab_precios = ui.tab("Precios")
                tab_busqueda = ui.tab("Búsqueda")
                tab_importacion = ui.tab("Importacion")
                tab_datos = ui.tab("Datos")
                tab_pesos = ui.tab("Pesos")
                tab_config = ui.tab("Configuración")
            with ui.row().classes("items-center gap-4"):
                version_str = datetime.now().strftime("%y%m%d%H")
                ui.label(f"Ver {version_str}").classes("text-sm text-gray-600")
                ui.label(user['username'])

                def logout() -> None:
                    set_current_user(None)
                    ui.notify("Sesión cerrada", color="positive")
                    show_login_screen(container)

                ui.button("Cerrar sesión", on_click=logout, color="negative").props("flat")

        tab_panels = ui.tab_panels(tabs, value=tab_home).classes("w-full")

        with tab_panels:
            with ui.tab_panel(tab_home):
                build_tab_home()

            with ui.tab_panel(tab_precios):
                precios_container = ui.column().classes("w-full")

            with ui.tab_panel(tab_busqueda):
                build_tab_busqueda()

            with ui.tab_panel(tab_importacion):
                build_tab_importacion()

            with ui.tab_panel(tab_datos):
                build_tab_datos()

            with ui.tab_panel(tab_pesos):
                build_tab_pesos()

            with ui.tab_panel(tab_config):
                build_tab_config()

        precios_cargado = [False]

        def on_tab_change(e) -> None:
            val = getattr(e, "value", None)
            if val == "Precios" and not precios_cargado[0]:
                precios_cargado[0] = True
                build_tab_precios(precios_container)

        tab_panels.on_value_change(on_tab_change)


# ==========================
# CONTENIDO DE PESTAÑAS
# ==========================


def build_tab_home() -> None:
    """Pestaña de inicio: datos de la cuenta ML, reputación y ventas."""
    user = require_login()
    if not user:
        return

    access_token = get_ml_access_token(user["id"])
    if not access_token:
        with ui.column().classes("w-full max-w-2xl gap-4"):
            ui.label("Bienvenido a BDC systems").classes("text-2xl font-semibold")
            ui.label(
                "Conectá tu cuenta de MercadoLibre en Configuración para ver aquí tu perfil, reputación y ventas."
            ).classes("text-gray-600")
        return

    # Cargar perfil y órdenes (puede ser lento)
    profile = ml_get_user_profile(access_token)
    seller_id = (profile or {}).get("id") or ml_get_user_id(access_token)
    orders_data: Dict[str, Any] = {}
    if seller_id:
        orders_data = ml_get_orders(access_token, str(seller_id), limit=600, offset=0)

    rep = (profile or {}).get("seller_reputation") or {}

    # Procesar órdenes para ventas
    raw_orders = orders_data.get("results") or orders_data.get("orders") or orders_data.get("elements") or []
    results = [o for o in raw_orders if isinstance(o, dict)]
    today_local = datetime.now().date()
    hoy_unidades, hoy_monto = 0, 0.0
    semana_unidades, semana_monto = 0, 0.0
    mes_unidades, mes_monto = 0, 0.0
    por_mes: Dict[str, Any] = {}

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
        if (today_local - dt).days <= 7:
            semana_unidades += units
            semana_monto += total_amount
        if (today_local - dt).days <= 30:
            mes_unidades += units
            mes_monto += total_amount
        key = dt.strftime("%Y-%m")
        if key not in por_mes:
            por_mes[key] = {"units": 0, "total": 0.0}
        por_mes[key]["units"] += units
        por_mes[key]["total"] += total_amount

    meses_orden = sorted(por_mes.keys(), reverse=True)[:6]  # Solo 6 meses para caber en pantalla

    # Layout compacto sin scroll - una sola pantalla
    with ui.column().classes("w-full gap-2 max-w-6xl overflow-hidden"):
        # Header con gradiente
        with ui.row().classes("w-full items-center gap-4 p-3 rounded-xl bg-gradient-to-r from-indigo-600 to-blue-700 text-white shadow-lg"):
            raw_pic = (profile or {}).get("thumbnail") or (profile or {}).get("picture") or (profile or {}).get("logo")
            pic_url: Optional[str] = None
            if isinstance(raw_pic, str) and raw_pic.strip():
                pic_url = raw_pic.strip()
            elif isinstance(raw_pic, dict):
                pic_url = (raw_pic.get("url") or raw_pic.get("secure_url") or "").strip() or None
            if pic_url:
                ui.image(pic_url).classes("w-14 h-14 rounded-full object-cover ring-2 ring-white/50")
            else:
                ui.icon("store", size="2.5rem").classes("opacity-90")
            ui.label((profile or {}).get("nickname") or (profile or {}).get("first_name") or "Usuario ML").classes(
                "text-xl font-semibold"
            )
            power = rep.get("power_seller_status")
            if power:
                with ui.badge(color="amber").classes("ml-2"):
                    ui.label(f"MercadoLíder {power.capitalize()}")

        # Grid: Reputación | Ventas | Gráfico | Históricas (todo en una fila compacta)
        with ui.row().classes("w-full gap-2 flex-nowrap items-stretch overflow-hidden"):
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

            with ui.card().classes("flex-1 min-w-[200px] shrink-0 p-4 border-l-4 border-l-emerald-500"):
                ui.label("Reputación").classes("text-base font-semibold text-emerald-700 dark:text-emerald-400 mb-1")
                ui.label(f"Nivel: {level_label}").classes("text-sm text-gray-600 mb-2")
                with ui.column().classes("gap-1.5 text-sm"):
                    ui.label(f"• Reclamos: {_pct(rate_claims)}").classes("text-gray-700")
                    ui.label(f"• Cancelaciones: {_pct(rate_canc)}").classes("text-gray-700")
                    ui.label(f"• Mediaciones: {_pct(rate_mediat) if rate_mediat is not None else '—'}").classes("text-gray-700")
                    ui.label(f"• Demora envíos: {_pct(rate_delayed)}").classes("text-gray-700")

            # Ventas (Hoy, 7d, 30d)
            with ui.card().classes("flex-1 min-w-[240px] shrink-0 p-4 border-l-4 border-l-blue-500"):
                ui.label("Ventas").classes("text-base font-semibold text-blue-700 dark:text-blue-400 mb-2")
                with ui.row().classes("gap-2 flex-wrap"):
                    with ui.column().classes("p-3 flex-1 min-w-[80px] rounded-lg bg-blue-50 dark:bg-blue-900/40"):
                        ui.label("Hoy").classes("text-sm text-blue-600")
                        ui.label(str(hoy_unidades)).classes("text-xl font-bold text-blue-800")
                        ui.label(f"$ {hoy_monto:,.0f}".replace(",", ".")).classes("text-sm font-medium")
                    with ui.column().classes("p-3 flex-1 min-w-[80px] rounded-lg bg-emerald-50 dark:bg-emerald-900/40"):
                        ui.label("7 días").classes("text-sm text-emerald-600")
                        ui.label(str(semana_unidades)).classes("text-xl font-bold text-emerald-800")
                        ui.label(f"$ {semana_monto:,.0f}".replace(",", ".")).classes("text-sm font-medium")
                    with ui.column().classes("p-3 flex-1 min-w-[80px] rounded-lg bg-amber-50 dark:bg-amber-900/40"):
                        ui.label("30 días").classes("text-sm text-amber-600")
                        ui.label(str(mes_unidades)).classes("text-xl font-bold text-amber-800")
                        ui.label(f"$ {mes_monto:,.0f}".replace(",", ".")).classes("text-sm font-medium")

            # Gráfico ventas por mes (valores en millones para eje Y legible)
            if meses_orden:
                chart_data = [round(por_mes[k]["total"] / 1e6, 2) for k in reversed(meses_orden)]
                chart_labels = list(reversed(meses_orden))
                chart_options = {
                    "grid": {"left": 50, "right": 25, "top": 25, "bottom": 35},
                    "xAxis": {"type": "category", "data": chart_labels, "axisLabel": {"fontSize": 12, "interval": 0}},
                    "yAxis": {
                        "type": "value",
                        "axisLabel": {"fontSize": 12, "formatter": "{value} M$"},
                        "name": "M$",
                        "nameTextStyle": {"fontSize": 11},
                    },
                    "series": [{"type": "bar", "data": chart_data, "itemStyle": {"color": "#6366f1"}, "barWidth": "60%"}],
                }
                with ui.card().classes("flex-1 min-w-[280px] shrink-0 p-3").style("min-height: 180px"):
                    ui.label("Ventas por mes").classes("text-base font-semibold text-indigo-600 mb-1 px-1")
                    ui.echart(chart_options).classes("w-full").style("height: 150px")
            else:
                with ui.card().classes("flex-1 min-w-[120px] shrink-0 p-3"):
                    ui.label("Ventas por mes").classes("text-sm font-semibold")
                    ui.label("Sin datos").classes("text-xs text-gray-500")

            # Ventas Históricas (tabla más grande)
            with ui.card().classes("flex-1 min-w-[260px] shrink-0 p-4 border-l-4 border-l-indigo-500"):
                ui.label("Ventas Históricas").classes("text-base font-semibold text-indigo-600 mb-2")
                if not meses_orden:
                    trans = rep.get("transactions", {}) or {}
                    tot = trans.get("total") or trans.get("completed") or 0
                    ui.label(f"Sin datos (perfil: {tot} trans.)" if tot else "No hay órdenes").classes("text-gray-500 text-sm")
                else:
                    with ui.element("div").classes("w-full border rounded overflow-hidden"):
                        with ui.row().classes("w-full font-semibold bg-indigo-600 text-white py-1.5 px-2 gap-2 items-center text-sm"):
                            ui.label("Mes").classes("min-w-[70px]")
                            ui.label("Unid").classes("w-14 text-right")
                            ui.label("Facturación").classes("w-24 text-right")
                        for key in meses_orden:
                            v = por_mes[key]
                            with ui.row().classes("w-full py-1 px-2 gap-2 items-center border-t border-gray-200 text-sm"):
                                ui.label(key).classes("min-w-[70px]")
                                ui.label(str(v["units"])).classes("w-14 text-right")
                                ui.label(f"$ {v['total']:,.0f}".replace(",", ".")).classes("w-24 text-right")

        # Segunda fila: Envíos, Flex, Postventa, Reclamos y Mediaciones
        envios_hoy: Dict[str, int] = {}
        flex_count = 0
        for ord_item in results:
            dt_str = ord_item.get("date_created") or ord_item.get("date_closed") or ""
            if not dt_str:
                continue
            try:
                dt = datetime.strptime(dt_str[:10], "%Y-%m-%d").date()
            except Exception:
                continue
            if dt != today_local:
                continue
            # Tipo de envío: logistic_type (self_service=Flex, drop_off, xd_drop_off, cross_docking, fulfillment=Full)
            ship = ord_item.get("shipping") or {}
            lt = ship.get("logistic_type") or ord_item.get("logistic_type") or ship.get("mode") or "otro"
            envios_hoy[lt] = envios_hoy.get(lt, 0) + 1
            if str(lt).lower() in ("self_service", "flex"):
                flex_count += 1

        claims_val = (claims.get("value") or claims.get("excluded", {}).get("real_value") or 0)
        mediat_val = (mediat.get("value") or mediat.get("excluded", {}).get("real_value") or 0) if mediat else 0
        canc_val = (canc.get("value") or canc.get("excluded", {}).get("real_value") or 0)
        postventa_total = claims_val + mediat_val + canc_val

        with ui.row().classes("w-full gap-2 flex-nowrap items-stretch mt-2 overflow-x-auto"):
            # Envíos hoy por tipo
            with ui.card().classes("flex-1 min-w-[220px] shrink-0 p-4 border-l-4 border-l-cyan-500"):
                ui.label("Envíos hoy (por tipo)").classes("text-base font-semibold text-cyan-700 mb-2")
                if not envios_hoy:
                    ui.label("Sin envíos hoy").classes("text-sm text-gray-500")
                else:
                    total_env = sum(envios_hoy.values())
                    ui.label(f"Total: {total_env}").classes("text-sm text-cyan-600 mb-1")
                    with ui.column().classes("gap-1 text-sm"):
                        for k, v in sorted(envios_hoy.items(), key=lambda x: -x[1]):
                            lbl = {"self_service": "Flex", "drop_off": "Drop-off", "xd_drop_off": "XD Drop-off",
                                   "cross_docking": "Coleta", "fulfillment": "Full", "default": "ME1", "otro": "Otro"}.get(str(k).lower(), str(k))
                            ui.label(f"• {lbl}: {v}").classes("text-gray-700")

            # Flex (órdenes self_service de hoy)
            with ui.card().classes("flex-1 min-w-[180px] shrink-0 p-4 border-l-4 border-l-violet-500"):
                ui.label("Flex (hoy)").classes("text-base font-semibold text-violet-700 mb-2")
                ui.label(str(flex_count)).classes("text-2xl font-bold text-violet-800")
                ui.label("órdenes").classes("text-sm text-gray-600")

            # Postventa (claims + mediaciones + cancelaciones)
            with ui.card().classes("flex-1 min-w-[200px] shrink-0 p-4 border-l-4 border-l-amber-500"):
                ui.label("Postventa").classes("text-base font-semibold text-amber-700 mb-2")
                ui.label(f"Reclamos: {claims_val}").classes("text-sm text-gray-700")
                ui.label(f"Mediaciones: {mediat_val}").classes("text-sm text-gray-700")
                ui.label(f"Cancelaciones: {canc_val}").classes("text-sm text-gray-700")
                ui.label(f"Total: {postventa_total}").classes("text-sm font-semibold mt-1")

            # Reclamos y Mediaciones (detalle)
            with ui.card().classes("flex-1 min-w-[240px] shrink-0 p-4 border-l-4 border-l-rose-500"):
                ui.label("Reclamos y Mediaciones").classes("text-base font-semibold text-rose-700 mb-2")
                with ui.column().classes("gap-1.5 text-sm"):
                    ui.label(f"Reclamos: {claims_val} ({_pct(rate_claims)})").classes("text-gray-700")
                    ui.label(f"Mediaciones: {mediat_val} ({_pct(rate_mediat) if rate_mediat is not None else '—'})").classes("text-gray-700")
                    ui.label(f"Cancelaciones: {canc_val} ({_pct(rate_canc)})").classes("text-gray-700")


def build_tab_precios(container) -> None:
    """Pestaña Precios: tabla similar a Mis productos con precio editable al hacer clic."""
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
            background_tasks.create(_cargar_precios_async(result_area, access_token, user, cargar_precios), name="cargar_precios")

        async def _cargar_precios_async(area, token, usr, on_actualizar) -> None:
            try:
                data = await run.io_bound(ml_get_my_items, token)
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
                await run.io_bound(ml_enriquecer_sale_price, data.get("results", []), token)
                _mostrar_tabla_precios(area, data, token, usr, on_actualizar)
            except Exception as e:
                area.clear()
                with area:
                    ui.label(f"❌ Error al mostrar datos: {e}").classes("text-negative")

        background_tasks.create(_cargar_precios_async(result_area, access_token, user, cargar_precios), name="cargar_precios")


def _mostrar_tabla_precios(
    result_area, data: Dict[str, Any], access_token: str, user: Dict[str, Any], on_actualizar=None
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

    items_loaded = []
    for i in items:
        precio = i.get("price") or 0
        sale_price = i.get("sale_price")
        precio_real = float(sale_price) if sale_price is not None else precio
        stock = i.get("available_quantity") or 0
        subtotal = precio * stock
        tipo = "Catalogo" if i.get("catalog_listing") is True else "Propia"
        tiene_promo = sale_price is not None and abs(float(sale_price) - float(precio or 0)) > 0.01
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
        })

    publicaciones_totales = len(items_loaded)
    publicaciones_con_stock = sum(1 for i in items_loaded if (i.get("available_quantity") or 0) > 0)
    publicaciones_propias_con_stock = sum(1 for i in items_loaded if i.get("tipo") == "Propia" and (i.get("available_quantity") or 0) > 0)
    publicaciones_catalogo_con_stock = sum(1 for i in items_loaded if i.get("tipo") == "Catalogo" and (i.get("available_quantity") or 0) > 0)
    unidades_propias_en_stock = sum(i.get("available_quantity") or 0 for i in items_loaded if i.get("tipo") == "Propia")
    total_pesos_propias = sum(i.get("subtotal") or 0 for i in items_loaded if i.get("tipo") == "Propia")
    dolar_oficial = get_setting("dolar_oficial") or 0
    total_dolares_propias = (total_pesos_propias / dolar_oficial) if dolar_oficial else None

    def abrir_editar_precio(row: Dict[str, Any]) -> None:
        if row.get("tipo") != "Propia":
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
                    ui.button("Cancelar", on_click=dialog.close).props("flat")
                    ui.button("Guardar", on_click=guardar, color="primary")

        dialog.open()

    current_filtrados: List[Dict[str, Any]] = []
    current_table: List[Any] = []

    def _generar_jpg_precios(filtrados_actuales: List[Dict[str, Any]]) -> Optional[str]:
        """Genera un JPG con la tabla de stock. Devuelve ruta del archivo o None si falla."""
        try:
            from PIL import Image, ImageDraw, ImageFont
        except ImportError:
            return None
        if not filtrados_actuales:
            return None
        ahora = datetime.now()
        header_nt = f"Stock {ahora.day:02d}-{ahora.month:02d}-{ahora.year % 100:02d}"
        # Columnas: Stock dd-mm-aa, Marca, Producto, Color, Stock
        col_widths = [160, 130, 520, 100, 100]
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
        headers = [header_nt, "Marca", "Producto", "Color", "Stock"]
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

    def imprimir_tabla() -> None:
        client = context.client
        tbl = current_table[0] if current_table else None

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
            path = _generar_jpg_precios(rows_to_print)
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
        {"name": "subtotal", "label": "Subtotal", "field": "subtotal", "sortable": True, "align": "right", "headerStyle": header_style, ":format": fmt_mon_js},
        {"name": "tipo", "label": "Tipo", "field": "tipo", "sortable": True, "align": "left", "headerStyle": header_style},
        {"name": "status", "label": "Estado", "field": "status", "sortable": True, "align": "left", "headerStyle": header_style, ":format": "(val) => (val || '').toLowerCase() === 'active' ? 'Activa' : 'Suspendida'"},
    ]

    def filtrar_y_pintar() -> None:
        filtrados = list(items_loaded)
        tipo_val = getattr(filtro_tipo, "value", None)
        if tipo_val == "propias":
            filtrados = [x for x in filtrados if x.get("tipo") == "Propia"]
        elif tipo_val == "catalogo":
            filtrados = [x for x in filtrados if x.get("tipo") == "Catalogo"]
        stock_val = getattr(filtro_stock, "value", "con_stock")
        if stock_val == "con_stock":
            filtrados = [x for x in filtrados if (x.get("available_quantity") or 0) > 0]
        elif stock_val == "sin_stock":
            filtrados = [x for x in filtrados if (x.get("available_quantity") or 0) == 0]
        awei_val = getattr(filtro_awei, "value", "no_incluye")
        if awei_val == "no_incluye":
            filtrados = [x for x in filtrados if "awei" not in (x.get("marca") or "").lower()]
        filtrados = sorted(filtrados, key=lambda r: (str(r.get("title") or "").lower()))
        current_filtrados.clear()
        current_filtrados.extend(filtrados)

        table_container.clear()
        with table_container:
            tbl_precios = ui.table(
                columns=columns_precios,
                rows=filtrados,
                row_key="id",
                pagination=0,
            ).classes("w-full").props("flat bordered dense")

            # Solo abrir editar al hacer clic en la celda Precio (columna index 4)
            PRECIO_COL_INDEX = 4
            def on_row_click_precios(e):
                args = getattr(e, "args", None)
                if not args or not isinstance(args, list) or len(args) < 2:
                    return
                evt, row = args[0], args[1]
                if not isinstance(row, dict) or row.get("tipo") != "Propia":
                    return
                # Verificar columna: evt.target.cellIndex o evt.column
                col_idx = None
                if isinstance(evt, dict):
                    t = evt.get("target") or evt.get("srcElement")
                    if isinstance(t, dict) and "cellIndex" in t:
                        col_idx = t.get("cellIndex")
                    elif isinstance(evt.get("column"), (int, float)):
                        col_idx = int(evt["column"])
                # Solo editar si se hizo clic en columna Precio; si evt no trae columna, permitir (fallback)
                if col_idx == PRECIO_COL_INDEX or col_idx is None:
                    abrir_editar_precio(row)

            tbl_precios.on("rowClick", on_row_click_precios)
            current_table.clear()
            current_table.append(tbl_precios)

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
                value="con_stock",
                label="Stock",
            ).classes("w-36")
            filtro_tipo = ui.select(
                {"ambas": "Ambas", "propias": "Propias", "catalogo": "Catalogo"},
                value="propias",
                label="Tipo",
            ).classes("w-36")
            filtro_awei = ui.select(
                {"incluye": "Incluye", "no_incluye": "No incluye"},
                value="no_incluye",
                label="Awei",
            ).classes("w-36")
            ui.button("Imprimir stock", on_click=imprimir_tabla, color="primary").props("icon=print")
        table_container = ui.column().classes("w-full")

    filtro_stock.on_value_change(lambda _: filtrar_y_pintar())
    filtro_tipo.on_value_change(lambda _: filtrar_y_pintar())
    filtro_awei.on_value_change(lambda _: filtrar_y_pintar())
    filtrar_y_pintar()


def build_tab_busqueda() -> None:
    """Pestaña Búsqueda: texto + botón, resultados en tabla (nombre, precio, vendedor, stock, tipo)."""
    user = require_login()
    if not user:
        return

    access_token = get_ml_access_token(user["id"])

    with ui.column().classes("w-full gap-4"):
        ui.label("Búsqueda en MercadoLibre").classes("text-xl font-semibold")
        with ui.row().classes("items-center gap-3"):
            input_busqueda = ui.input("Texto a buscar").classes("w-96").props("outlined dense")
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

        async def _buscar_async() -> None:
            texto = (input_busqueda.value or "").strip()
            if not texto:
                ui.notify("Ingresá un texto para buscar", color="warning")
                return
            results_container.clear()
            with results_container:
                ui.spinner(size="lg")
                ui.label("Buscando en MercadoLibre...").classes("text-gray-600")
            try:
                solo_propias = getattr(solo_propias_switch, "value", True)
                data = await run.io_bound(ml_search_similar, texto, 50, access_token, solo_propias)
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
            rows = [_norm_busqueda(r, from_catalog) for r in results]
            filter_showed_all = False
            if getattr(solo_activas_stock_switch, "value", True):
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
            rows.sort(key=lambda x: x["price"])
            results_container.clear()
            with results_container:
                if data.get("error"):
                    ui.label(f"Error: {data['error']}").classes("text-negative")
                    texto_busq = (input_busqueda.value or "").strip()
                    if texto_busq:
                        from urllib.parse import quote
                        busq_url = f"https://listado.mercadolibre.com.ar/{quote(texto_busq)}"
                        ui.link("Buscar en MercadoLibre", busq_url, new_tab=True).classes("text-primary mt-2")
                elif not rows:
                    ui.label("No se encontraron resultados.").classes("text-gray-500")
                else:
                    if filter_showed_all:
                        ui.label(
                            "No se encontraron publicaciones activas con stock. Mostrando todos los resultados."
                        ).classes("text-amber-600 text-sm mb-2")
                    with ui.element("div").classes("w-full overflow-x-auto border rounded-lg").style("min-width: 800px;"):
                        with ui.row().classes("w-full bg-blue-600 text-white py-2 px-3 font-semibold flex-nowrap"):
                            ui.label("Nombre del producto").classes("min-w-[350px] shrink-0 text-left")
                            ui.label("Precio").classes("min-w-[120px] shrink-0 text-right")
                            ui.label("Vendedor").classes("min-w-[180px] shrink-0 text-left")
                            ui.label("Stock disp.").classes("min-w-[90px] shrink-0 text-right")
                            ui.label("Tipo").classes("min-w-[90px] shrink-0 text-left")
                        for r in rows:
                            with ui.row().classes("w-full py-2 px-3 border-b border-gray-200 hover:bg-gray-50 flex-nowrap"):
                                tit = (r.get("title") or "")[:100] + ("..." if len(r.get("title") or "") > 100 else "")
                                link_url = r.get("permalink", "#")
                                ui.link(tit, link_url, new_tab=True).classes("min-w-[350px] shrink-0 text-left text-primary hover:underline cursor-pointer").props("target=_blank")
                                ui.label(r.get("price_display", "—")).classes("min-w-[120px] shrink-0 text-right font-medium")
                                ui.label(str(r.get("seller", "—"))).classes("min-w-[180px] shrink-0 text-left")
                                ui.label(str(r.get("available_quantity_display", r.get("available_quantity", "—")))).classes("min-w-[90px] shrink-0 text-right")
                                ui.label(r.get("tipo", "")).classes("min-w-[90px] shrink-0 text-left")



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

    with ui.column().classes("gap-4 p-4"):
        cont = ui.column().classes("gap-2")
        edit_rows: List[Dict[str, Any]] = []

        def toggle_sort(col: str) -> None:
            if sort_col_pesario[0] == col:
                sort_asc_pesario[0] = not sort_asc_pesario[0]
            else:
                sort_col_pesario[0] = col
                sort_asc_pesario[0] = True
            repintar()

        def repintar() -> None:
            cont.clear()
            edit_rows.clear()
            datos = list(pesario_data)
            if sort_col_pesario[0] in ("marca", "producto"):
                rev = not sort_asc_pesario[0]
                datos.sort(key=lambda r: str(r.get(sort_col_pesario[0], "")).lower(), reverse=rev)
            with cont:
                with ui.element("table").classes("border-collapse text-xs").style("table-layout: fixed; max-width: 640px; width: 640px; line-height: 1.2;"):
                    with ui.element("thead"):
                        with ui.element("tr").classes("bg-blue-100 dark:bg-blue-900"):
                            for col_key, h in [("marca", "Marca"), ("producto", "Producto"), (None, "Peso (gr)"), (None, "Fuente (gr)"), (None, "Total (gr)"), (None, "Borrar")]:
                                th_cls = "font-semibold px-1 py-0.5 border border-gray-300"
                                th_cls += " text-left" if col_key else " text-center"
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
                            with ui.element("tr"):
                                for col in ["marca", "producto", "peso", "fuente"]:
                                    val = str(row.get(col, ""))
                                    td_el = ui.element("td").classes("border border-gray-200").style("padding: 2px 4px; vertical-align: middle;")
                                    if col == "producto":
                                        td_el.style("min-width: 266px;")  # 33% más que 200px
                                    td_align = "text-center" if col in ("peso", "fuente") else ""
                                    with td_el:
                                        inp = ui.input(value=val).classes("w-full border-0 text-xs " + td_align).props("dense")
                                        rinputs[col] = inp
                                with ui.element("td").classes("border border-gray-200 bg-gray-50 text-center").style("padding: 2px 4px; vertical-align: middle;"):
                                    p0 = _parse_peso(row.get("peso"))
                                    f0 = _parse_peso(row.get("fuente"))
                                    t0 = p0 + f0
                                    total_txt = str(int(t0)) if t0 == int(t0) else f"{t0:.1f}"
                                    lbl_total = ui.label(total_txt).classes("px-1")

                                    def actualizar_total(lbl=lbl_total, rinp=rinputs) -> None:
                                        p = _parse_peso(rinp["peso"].value)
                                        f = _parse_peso(rinp["fuente"].value)
                                        t = p + f
                                        lbl.text = str(int(t)) if t == int(t) else f"{t:.1f}"

                                    rinputs["peso"].on_value_change(actualizar_total)
                                    rinputs["fuente"].on_value_change(actualizar_total)
                                with ui.element("td").classes("border border-gray-200 w-8 text-center").style("padding: 2px 4px; vertical-align: middle;"):
                                    def borrar_pesario(rref: Dict[str, Any]) -> None:
                                        if rref in pesario_data:
                                            pesario_data.remove(rref)
                                            repintar()
                                    ui.button("×", on_click=lambda r=row: borrar_pesario(r)).classes("text-red-600 font-bold text-base min-w-0 px-0").props("flat dense no-caps")
                            edit_rows.append(rinputs)

        repintar()

        def guardar() -> None:
            new_data = []
            for rinputs in edit_rows:
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


def build_tab_config() -> None:
    user = require_login()
    if not user:
        return

    ui.label("Configuración").classes("text-2xl font-semibold mb-6")

    # App de MercadoLibre (cada usuario puede tener su propia app con distinto App ID y Client Secret)
    app_creds = get_ml_app_credentials(user["id"])
    with ui.card().classes("w-full max-w-2xl"):
        ui.label("App de MercadoLibre").classes("text-lg font-semibold mb-3")
        ui.label("Cada usuario puede conectar su propia app de MercadoLibre (con su App ID y Client Secret).").classes("text-sm text-gray-600 mb-3")
        inp_client_id = ui.input("App ID (client_id)", value=app_creds["client_id"] if app_creds else "").classes("w-full max-w-md").props("type=text")
        inp_client_secret = ui.input("Client Secret", value=app_creds["client_secret"] if app_creds else "").classes("w-full max-w-md").props("type=password password-toggle")
        default_redirect = os.getenv("ML_REDIRECT_URI", "http://localhost:8083/ml/callback")
        inp_redirect = ui.input("Redirect URI (debe coincidir con la app en MercadoLibre Developers)", value=app_creds.get("redirect_uri") or default_redirect if app_creds else default_redirect).classes("w-full max-w-md")
        def guardar_app_ml() -> None:
            cid = (inp_client_id.value or "").strip()
            csec = (inp_client_secret.value or "").strip()
            redir = (inp_redirect.value or "").strip() or default_redirect
            if not cid or not csec:
                ui.notify("Ingresá App ID y Client Secret", color="warning")
                return
            set_ml_app_credentials(user["id"], cid, csec, redir or None)
            ui.notify("Credenciales de la app guardadas", color="positive")
        ui.button("Guardar credenciales de la app", on_click=guardar_app_ml, color="primary").classes("mt-2")

    # Estado de MercadoLibre (cuenta vinculada)
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM ml_credentials WHERE user_id = ?", (user["id"],))
    ml_creds = cur.fetchone()
    conn.close()

    with ui.card().classes("w-full max-w-2xl"):
        ui.label("MercadoLibre").classes("text-lg font-semibold mb-3")
        if ml_creds:
            with ui.row().classes("items-center gap-3 mb-2 flex-wrap"):
                ui.icon("check_circle", color="positive", size="sm")
                ui.label("Cuenta vinculada").classes("text-positive font-medium")

                app_link = get_ml_app_credentials(user["id"])
                if app_link:
                    client_id = app_link["client_id"]
                    redirect_uri = app_link.get("redirect_uri") or os.getenv("ML_REDIRECT_URI", "http://localhost:8083/ml/callback")
                else:
                    client_id = os.getenv("ML_CLIENT_ID")
                    redirect_uri = os.getenv("ML_REDIRECT_URI", "http://localhost:8083/ml/callback")
                if client_id:
                    from urllib.parse import quote
                    scope = quote("offline_access read write")
                    auth_url = f"https://auth.mercadolibre.com.ar/authorization?response_type=code&client_id={client_id}&redirect_uri={quote(redirect_uri)}&scope={scope}"
                    ui.link("Vincular de nuevo", auth_url, new_tab=True).classes("text-primary text-sm")

            if ml_creds["expires_at"]:
                try:
                    exp = ml_creds["expires_at"][:19].replace("T", " ")
                    ui.label(f"Token vence: {exp}").classes("text-sm text-gray-600")
                except Exception:
                    ui.label(f"Token vence: {ml_creds['expires_at']}").classes("text-sm text-gray-600")
        else:
            with ui.row().classes("items-center gap-3 mb-3"):
                ui.icon("warning", color="warning", size="sm")
                ui.label("Sin cuenta vinculada").classes("text-warning font-medium")

            app_creds_link = get_ml_app_credentials(user["id"])
            if app_creds_link:
                client_id = app_creds_link["client_id"]
                redirect_uri = app_creds_link.get("redirect_uri") or os.getenv("ML_REDIRECT_URI", "http://localhost:8083/ml/callback")
            else:
                client_id = os.getenv("ML_CLIENT_ID")
                redirect_uri = os.getenv("ML_REDIRECT_URI", "http://localhost:8083/ml/callback")

            if not client_id:
                ui.label("Guardá primero tu App ID y Client Secret en la sección de arriba.").classes("text-sm text-gray-600")
            else:
                from urllib.parse import quote
                scope = quote("offline_access read write")
                auth_url = (
                    f"https://auth.mercadolibre.com.ar/authorization"
                    f"?response_type=code&client_id={client_id}&redirect_uri={quote(redirect_uri)}&scope={scope}"
                )
                ui.link("Conectar con MercadoLibre", auth_url, new_tab=True).classes("text-primary font-medium")

    # Datos que devuelve MercadoLibre (ejemplo de una publicación)
    with ui.card().classes("w-full max-w-4xl mt-4"):
        ui.label("Datos que devuelve MercadoLibre sobre tus publicaciones").classes("text-lg font-semibold mb-3")
        ui.label(
            "Podés cargar el JSON completo de una de tus publicaciones para ver todos los campos que devuelve la API."
        ).classes("text-sm text-gray-600 mb-3")
        ml_data_area = ui.column().classes("w-full gap-2")
        example_visible = [False]  # ref para alternar botón

        def toggle_ejemplo_ml() -> None:
            if example_visible[0]:
                ml_data_area.clear()
                example_visible[0] = False
                btn_ml.text = "Cargar ejemplo de una publicación"
                return
            ml_data_area.clear()
            with ml_data_area:
                ui.spinner(size="sm")
                ui.label("Cargando...").classes("text-gray-600")
            access_token = get_ml_access_token(user["id"])
            if not access_token:
                ml_data_area.clear()
                with ml_data_area:
                    ui.label("No hay cuenta de MercadoLibre vinculada.").classes("text-warning")
                btn_ml.text = "Cargar ejemplo de una publicación"
                return
            try:
                raw = ml_get_one_item_full(access_token)
                ml_data_area.clear()
                with ml_data_area:
                    if raw is None:
                        ui.label("No tenés publicaciones o no se pudo obtener el ejemplo.").classes("text-gray-600")
                    else:
                        ui.label("Ejemplo (una publicación):").classes("text-sm font-medium")
                        json_str = html.escape(json.dumps(raw, indent=2, ensure_ascii=False))
                        ui.html(f'<pre class="p-4 bg-grey-2 rounded overflow-auto text-sm" style="max-height: 400px;">{json_str}</pre>')
                example_visible[0] = True
                btn_ml.text = "Cerrar ejemplo"
            except Exception as e:
                ml_data_area.clear()
                with ml_data_area:
                    ui.label(f"Error: {e}").classes("text-negative")
                btn_ml.text = "Cargar ejemplo de una publicación"

        btn_ml = ui.button("Cargar ejemplo de una publicación", on_click=toggle_ejemplo_ml, color="secondary")


# Valores por defecto del cotizador
COTIZADOR_DEFAULTS = {
    "dolar_oficial": "1475", "dolar_blue": "1450", "dolar_sistema": "1500", "dolar_despacho": "1475",
    "kilo": "60", "iva_105": "0.105", "iva_21": "0.21", "iibb_lhs": "0.03",
    "ml_comision": "0.15", "ml_debcre": "0.006", "ml_sirtac": "0.008", "ml_envios": "5823",
    "ml_iibb_per": "0.055", "ml_envios_gratuitos": "33000", "ml_cobrado": "0.836",
    "ml_3cuotas": "1.12149", "ml_6cuotas": "1.21067",
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
    peso_total = ceil(qty * peso_unit) if qty > 0 and peso_unit > 0 else 0

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

    L = derechos_rate * fob_total * dolar_despacho  # Derechos = tasa × FOB Total (en USD × Dólar)
    M = estad_rate * fob_total * dolar_despacho     # Estadística = tasa × FOB Total
    N = kg_real * peso_total * dolar_despacho
    O_val = almacenaje * peso_total * dolar_despacho
    P = res_3244 * dolar_despacho
    Q = seguro * dolar_despacho
    R = gas_ope * dolar_despacho
    S = env_dom * dolar_despacho
    T = ((0.21 * L) + (0.21 * M) + (0.21 * O_val) + (0.21 * P) + (0.21 * Q) + (0.21 * R) + (iva_rate * fob_total * dolar_despacho)) * ajuste_ana
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
    iva_21 = params.get("iva_21", 0.21)
    ml_envios_val = params.get("ml_envios", 5823)
    ml_envios = ml_envios_val if ml_envios_val > 100 else 5823  # B12 es monto fijo en pesos
    ml_envios_gratuitos = params.get("ml_envios_gratuitos", 33000)
    ml_cobrado = params.get("ml_cobrado", 0.836)
    ml_iibb_per = params.get("ml_iibb_per", 0.055)

    cuotas3 = venta_ml * ml_3cuotas if venta_ml > 0 else 0
    cuotas6 = venta_ml * ml_6cuotas if venta_ml > 0 else 0
    markup = ((venta_ml / AC) - 1) if venta_ml > 0 and AC > 0 else 0
    cobrado_ml = 0
    if venta_ml > 0:
        base = venta_ml * ml_cobrado  # ML - Cobrado × Venta ML
        # Si venta >= Envíos gratuitos: restar ML-Envíos. Si venta < Envíos gratuitos: no restar.
        cobrado_ml = base - ml_envios if venta_ml >= ml_envios_gratuitos else base
    iva_impor = (T / qty) if venta_ml > 0 and qty > 0 else 0
    iva_meli = venta_ml * ml_comision * iva_21 if venta_ml > 0 else 0
    iva_venta = venta_ml - (venta_ml / (iva_rate + 1)) if venta_ml > 0 else 0
    iva_total = iva_venta - iva_meli - iva_impor
    iibb_per = venta_ml * ml_iibb_per if venta_ml > 0 else 0
    costo_vta = (((venta_ml - cobrado_ml) + (iva_total if iva_total > 0 else 0) + iibb_per) / venta_ml) if venta_ml > 0 else 0
    margen = (cobrado_ml - AC - iva_total - iibb_per) if venta_ml > 0 else 0
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
        "peso_total": str(int(peso_total)),
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
        "iva_impor": _mon(_fmt(iva_impor, 0)),
        "iva_meli": _mon(_fmt(iva_meli, 0)),
        "iva_venta": _mon(_fmt(iva_venta, 0)),
        "iva_total": _mon(_fmt(iva_total, 0)),
        "iibb_per": _mon(_fmt(iibb_per, 0)),
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

    with ui.column().classes("w-full gap-2 p-2"):
        ui.label("Importación - Cotizador Courier").classes("text-xl font-semibold")

        cols_input = ["productos", "origen", "impuestos", "fob", "qty", "peso_unitario", "extras", "trafo", "cambio_pa", "venta_ml"]
        headers_input = ["Productos", "Origen", "Impuestos", "FOB", "QTY", "Peso U", "Extras", "Trafo", "Cam.PA", "Venta ML"]

        opciones_origen = [r.get("origen", "") for r in origen_data if r.get("origen")]
        opciones_impuestos = [r.get("posicion", "") for r in posicion_data if r.get("posicion")]
        cols_calc = ["fob_total", "peso_total", "derechos", "estadistica", "flete_int", "almacenaje", "res_3244", "seguro", "gas_ope", "env_dom", "iva_lhs", "iibb", "total_courier", "total", "traida_excel", "costo_pesos", "costo_usd", "cuotas3", "cuotas6", "markup", "cobrado_ml", "iva_impor", "iva_meli", "iva_venta", "iva_total", "iibb_per", "costo_vta", "margen", "margen_vta", "margen_costo"]
        headers_calc = ["FOB Tot", "Peso", "Derech", "Estad", "Flete", "Almac", "Res3244", "Seguro", "GasOp", "EnvDom", "IVA LHS", "IIBB", "Courier", "Total", "Traída", "Costo$", "Costo u$", "3ctas", "6ctas", "MarkUp", "CobrML", "IVAImp", "IVAMel", "IVAVta", "IVA", "IIBB+PER", "CstoVta", "Margen$", "MargVta", "MargCos"]
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

        def aplicar_estilo_fob_ml(inp: Any, es_fob: bool = False) -> None:
            """Actualiza negrita y rojo según si el input tiene valor (al cargar/editar)."""
            v = (inp.value or "").strip()
            base = "min-w-[42px]" if es_fob else "min-w-[50px]"
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
                        with ui.element("tr").classes("bg-blue-100 dark:bg-blue-900"):
                            for j, (c, h) in enumerate(zip(all_cols, all_headers)):
                                th_cls = "font-semibold px-1 py-1 text-center border border-gray-300 whitespace-nowrap text-xs"
                                if not col_visible(c):
                                    th_cls += " hidden"
                                with ui.element("th").classes(th_cls):
                                    ui.label(h)
                            with ui.element("th").classes("font-semibold px-0.5 py-1 text-center border border-gray-300 text-xs").style("min-width: 48px;"):
                                ui.label("Ordenar")
                            with ui.element("th").classes("font-semibold px-1 py-1 border border-gray-300").style("min-width: 40px;"):
                                ui.label("×")
                    with ui.element("tbody"):
                        for i, r in enumerate(importacion_rows):
                            r_in: Dict[str, Any] = {}
                            with ui.element("tr"):
                                for c in cols_input:
                                    val = str(r.get(c, ""))
                                    td_cls = "p-0.5 border border-gray-200 min-w-0"
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
                                            inp_cls = "min-w-[42px]"
                                            if val:
                                                inp_cls += " font-bold text-red-600"
                                            inp = ui.input(value=val).classes(inp_cls).props("dense")
                                            inp.on_value_change(lambda inp_ref=inp: aplicar_estilo_fob_ml(inp_ref, es_fob=True))
                                            aplicar_estilo_fob_ml(inp, es_fob=True)
                                        elif c in ("qty", "peso_unitario", "cambio_pa"):
                                            inp = ui.input(value=val).classes("min-w-[40px]").props("dense")
                                        elif c in ("extras", "trafo"):
                                            inp = ui.input(value=val).classes("min-w-[55px]").props("dense")
                                        elif c == "venta_ml":
                                            inp_cls = "min-w-[50px]"
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

        if not importacion_rows:
            add_row()
        else:
            repintar()
            recalcular()

        def sync_inputs_to_rows() -> None:
            """Copia los valores actuales de los inputs a importacion_rows antes de repintar."""
            for i, r_in in enumerate(input_rows_ref):
                if i < len(importacion_rows):
                    for c in cols_input:
                        if c in r_in:
                            v = r_in[c].value
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

        with ui.row().classes("gap-2"):
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
            with ui.card().classes("p-4 w-fit min-w-[180px]"):
                ui.label("Dólar").classes("text-lg font-semibold mb-3")
                inputs_params: Dict[str, Any] = {}
                for label, key in [
                    ("Oficial", "dolar_oficial"), ("Blue", "dolar_blue"), ("Sistema", "dolar_sistema"), ("Despacho", "dolar_despacho"),
                ]:
                    with ui.row().classes("items-center gap-2 py-0.5"):
                        ui.label(label).classes("min-w-[70px] text-sm")
                        inputs_params[key] = ui.input(value=_get(key)).classes("flex-1 max-w-[100px]").props("dense")

            # Traida por Kilo
            with ui.card().classes("p-4 w-fit min-w-[140px]"):
                ui.label("Traida por Kilo").classes("text-lg font-semibold mb-3")
                with ui.row().classes("items-center gap-2 py-0.5"):
                    ui.label("Kilo").classes("min-w-[60px] text-sm")
                    inputs_params["kilo"] = ui.input(value=_get("kilo")).classes("flex-1 max-w-[80px]").props("dense")

            # Mercadolibre
            with ui.card().classes("p-4 w-fit min-w-[220px]"):
                ui.label("Mercadolibre").classes("text-lg font-semibold mb-3")
                for label, key in [
                    ("ML - Comisión", "ml_comision"), ("ML - Deb/Cre", "ml_debcre"), ("ML - Sirtac", "ml_sirtac"), ("ML - Envíos", "ml_envios"),
                    ("ML - IIBB + PER", "ml_iibb_per"), ("ML - Envíos grat.", "ml_envios_gratuitos"), ("ML - Cobrado", "ml_cobrado"),
                    ("ML 3 cuotas", "ml_3cuotas"), ("ML 6 cuotas", "ml_6cuotas"),
                ]:
                    with ui.row().classes("items-center gap-2 py-0.5"):
                        ui.label(label).classes("min-w-[100px] text-sm")
                        inputs_params[key] = ui.input(value=_get(key)).classes("flex-1 max-w-[100px]").props("dense")

            # Miami
            with ui.card().classes("p-4 w-fit min-w-[220px]"):
                ui.label("Miami").classes("text-lg font-semibold mb-3")
                inputs_miami: Dict[str, Any] = {}
                for label, key in [
                    ("Valor KG Miami", "valor_kg_miami"), ("Almac. Días x Kg", "almacenaje_dias_kg_miami"),
                    ("Seguro Miami", "seguro_miami"),
                ]:
                    with ui.row().classes("items-center gap-2 py-0.5"):
                        ui.label(label).classes("min-w-[120px] text-sm")
                        inputs_miami[key] = ui.input(value=_get(key)).classes("flex-1 max-w-[100px]").props("dense")

            # China
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
                        inputs_china[key] = ui.input(value=_get(key)).classes("flex-1 max-w-[100px]").props("dense")

        def guardar_params() -> None:
            for key, inp in {**inputs_params, **inputs_miami, **inputs_china}.items():
                set_cotizador_param(key, str(inp.value or ""), uid)
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

        def _tabla_editable(nombre: str, cols: List[str], headers: List[str], data: List[Dict[str, Any]], titulo: str, compact: bool = False, col_widths: Optional[List[str]] = None, card_ancho: Optional[str] = None, computed: Optional[Dict[str, Any]] = None, computed_deps: Optional[Dict[str, List[str]]] = None, ordenable: bool = True) -> None:
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
                                            with ui.element("td").classes("p-0.5 border border-gray-200"):
                                                if computed and col in computed:
                                                    disp = computed[col](row) if callable(computed[col]) else str(row.get(col, ""))
                                                    lbl = ui.label(disp).classes("text-xs")
                                                    rinputs[col] = lbl
                                                else:
                                                    inp = ui.input(value=val).classes("w-full border-0 text-xs").props("dense")
                                                    rinputs[col] = inp
                                        # Actualizar labels calculados cuando cambian las dependencias
                                        if computed and computed_deps:
                                            def make_updater(comp_col: str, lbl_ref: Any) -> None:
                                                def upd() -> None:
                                                    row = {c: str(rinputs[c].value or "") for c in cols if c not in (computed or {})}
                                                    lbl_ref.text = computed[comp_col](row)
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
                            row[c] = str(rinputs[c].value or "")
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
                computed_deps={"costo": ["importe", "porc_10"]}, card_ancho="w-fit")
            _tabla_editable("courier", ["courier", "valor_kg", "descuento", "kg_real", "almacenaje", "seguro", "res_3244", "gas_ope", "env_dom", "iibb", "cif"],
                ["Courier", "Valor KG", "Descuento", "KG Real", "Almacenaje", "Seguro", "Res 3244", "Gas Ope", "Env Dom", "IIBB", "CIF"],
                tabla_courier_data, "Costos por Courier",
                computed={"kg_real": lambda r: f"{_parse_num(r.get('valor_kg')) / max(0.001, _parse_num(r.get('descuento'))):.2f}"},
                computed_deps={"kg_real": ["valor_kg", "descuento"]}, card_ancho="w-fit")


# ==========================
# CALLBACK OAUTH (ruta HTTP directa para evitar 404 con NiceGUI)
# ==========================


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


# ==========================
# ARRANQUE DE LA APP
# ==========================


@ui.page("/")
def index(request: Request) -> None:  # type: ignore[override]
    root = ui.column().classes("w-full")

    # Procesar callback de OAuth (cuando MercadoLibre redirige a /?ml_oauth_code=xxx)
    ml_code = request.query_params.get("ml_oauth_code")
    ml_error = request.query_params.get("ml_oauth_error")
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
                headers={"Accept": "application/json"},
                timeout=10,
            )
            resp.raise_for_status()
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


def main() -> None:
    load_dotenv()
    init_db()
    port = int(os.getenv("PORT", 8083))
    # En Render/cloud: PORT lo define la plataforma, no iniciar ngrok
    es_produccion = "PORT" in os.environ or os.getenv("RENDER") == "true"
    if not es_produccion and os.getenv("NGROK_AUTO_START", "1").strip().lower() in ("1", "true", "yes"):
        print("Iniciando ngrok...")
        _iniciar_ngrok(port)
    # host 0.0.0.0 necesario para que Render/cloud pueda acceder al servicio
    ui.run(
        title="BDC systems",
        reload=False,
        host="0.0.0.0" if es_produccion else "127.0.0.1",
        port=port,
        storage_secret=os.getenv("STORAGE_SECRET", "cambia-esta-clave"),
    )


if __name__ == "__main__":
    main()
