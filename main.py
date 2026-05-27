from __future__ import annotations

import asyncio

# Polyfill asyncio.to_thread para Python 3.8 (agregado en 3.9). Evita AttributeError en Históricos y otras búsquedas.
if not hasattr(asyncio, "to_thread"):
    def _to_thread_compat(fn, *args, **kwargs):
        import functools
        loop = asyncio.get_running_loop()
        return loop.run_in_executor(None, functools.partial(fn, *args, **kwargs))
    asyncio.to_thread = lambda fn, *args, **kwargs: _to_thread_compat(fn, *args, **kwargs)

import base64
import bcrypt
from cryptography.fernet import Fernet
import hashlib
import logging
import re
import unicodedata

logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
import html
import json
import sqlite3
import calendar
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import os
import sys
import secrets
import socket
import ssl
import smtplib
import subprocess
import tempfile
import threading
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

# --- Compatibilidad Fase 1: funciones ML movidas a ml_api.py ---
from ml_api import (
    get_ml_access_token,
    _parse_ml_item_body, _cuotas_desde_item, _body_to_precios_item,
    _tipo_publicacion_desde_item, _extraer_color_desde_texto,
    ml_get_my_items, ml_update_item_price, ml_get_one_item_full,
    ml_get_item_sale_price, ml_get_item_sale_price_full,
    ml_get_item_price_to_win, ml_get_item_performance,
    ml_get_promotion_item_discounts, ml_get_promotion_item_discounts_by_user,
    ml_get_promotion_item_discounts_by_campaign,
    ml_get_item_prices, ml_enriquecer_sale_price, ml_fetch_price_for_item,
    ml_get_product_detail, ml_get_item_description, ml_get_item,
    ml_get_items_multiget, ml_get_items_multiget_with_attributes, ml_get_items_multiget_all,
    ml_get_users_multiget, ml_get_user_id, ml_get_user_profile,
    ml_get_orders, ml_get_shipments_today, ml_search_similar,
)

# --- Compatibilidad Fase 1: funciones QB movidas a qb_api.py ---
from qb_api import (
    _refresh_qb_token_if_needed,
    fetch_qb_customers, _qb_raw_query, fetch_qb_company_info,
    fetch_qb_vendors, fetch_qb_bills, fetch_qb_items,
    fetch_qb_items_search, fetch_qb_item_history,
    fetch_qb_customer_detail, fetch_qb_invoice_pdf, fetch_qb_item_by_id,
    fetch_qb_invoices, fetch_qb_invoice_detail,
    patch_invoice_pdf_line_items,
)

# --- Compatibilidad Fase 1: funciones de auth movidas a auth.py ---
from auth import (
    hash_password, _is_bcrypt_hash, _verify_password,
    send_email, get_user_email,
    create_user, authenticate_user,
    update_user_password, admin_reset_user_password,
    delete_user_and_all_data,
)

# --- Compatibilidad Fase 1: funciones de DB movidas a db.py ---
from db import (
    get_connection, init_db, save_query,
    get_ml_app_credentials, set_ml_app_credentials,
    get_qb_app_credentials, set_qb_app_credentials,
    get_qb_tokens, get_user_qb_customer, set_user_qb_customer,
    get_setting, set_setting,
    get_cotizador_param, set_cotizador_param, delete_cotizador_param,
    get_cotizador_tabla, set_cotizador_tabla,
    list_users_excluding, get_all_users,
    get_user_tab_permissions, set_user_tab_permission,
    user_can_access_tab,
    get_compras_lista, get_compras_lista_all, get_compras_lista_row,
    insert_compras_lista, update_compras_lista_row, delete_compras_lista_row,
    get_pedidos_lista, insert_pedidos_lista, update_pedidos_lista_row, delete_pedidos_lista_row,
    get_marcas, insert_marca, update_marca, delete_marca,
    get_despachantes, insert_despachante, update_despachante, delete_despachante,
    get_invoice_extras, upsert_invoice_extra,
    copy_cotizador_datos,
    get_importacion_filas, save_importacion_filas,
    export_user_db_data, import_user_db_data,
    COTIZADOR_DEFAULTS,
)

# --- Fase 3: tabs extraídos a módulos separados ---
from tabs.pedidos import build_tab_pedidos
from tabs.estadisticas import build_tab_estadisticas
from tabs.config import build_tab_config
from tabs.compras import build_tab_compras
from tabs.ventas import build_tab_ventas
from tabs.cuotas import build_tab_cuotas
from tabs.precios import build_tab_precios
from tabs.precios_detalle import build_tab_precios_detalle
from tabs.stock import build_tab_stock
from tabs.balance import build_tab_balance
from tabs.busqueda import build_tab_busqueda
from tabs.admin import build_tab_admin
from tabs.importacion import build_tab_importacion
from tabs.historicos import build_tab_historicos
from tabs.pesos import build_tab_pesos
from tabs.datos import build_tab_datos

DB_PATH = Path(__file__).with_name("app.db")

# Versión del sistema: formato 2.aa.mm.dd.hh (aa=año, mm=mes, dd=día, hh=hora 00-23). Ej.: 2.26.04.14.12
VERSION = "2.26.05.27.46"

# Pestañas del sistema (tab_key interno -> label visible). Usado en Admin para permisos.
# compras_lista (Compras) se quitó de la tabla de permisos.
TAB_KEYS = [
    ("home", "Home"),
    ("estadisticas", "Estadísticas"),
    ("ventas", "Ventas"),
    ("productos", "Productos"),
    ("precios", "Precios"),
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

# Grupos de tabs para control de acceso por defecto
TABS_BASE = {"home", "pedidos", "importacion", "pesos", "datos", "configuracion"}
TABS_ML   = {"estadisticas", "ventas", "productos", "precios", "busqueda", "balance", "cuotas", "historicos", "stock"}
TABS_QB   = {"compras", "compras_lista"}


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












































def _invoice_line_patch_specs(
    inv_obj: Dict[str, Any],
    user_id: Optional[int] = None,
    item_detail_cache: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """Un dict por línea de venta: description, sku, qty, rate, amount (números en API)."""
    item_cache: Dict[str, Dict[str, Any]] = item_detail_cache if item_detail_cache is not None else {}
    lines = inv_obj.get("Line") or []
    if isinstance(lines, dict):
        lines = [lines]
    n = len(lines)
    out: List[Dict[str, Any]] = []
    for idx, lin in enumerate(lines):
        dt = str(lin.get("DetailType") or "").strip()
        if dt == "DescriptionOnly":
            extra = str(lin.get("Description") or "").strip()
            if extra and out:
                out[-1]["description"] = f"{out[-1]['description']} {extra}".strip()
            continue

        sales = lin.get("SalesItemLineDetail") or {}
        if not isinstance(sales, dict):
            continue
        ref = sales.get("ItemRef") or {}
        name = str(ref.get("name", "") if isinstance(ref, dict) else "").strip()
        item_id = str(ref.get("value", "") if isinstance(ref, dict) else "").strip()
        desc = str(lin.get("Description") or name or "").strip()
        if idx == n - 1 and desc in ("-", "””", ""):
            continue
        if not desc or desc == "Total":
            continue
        from_line = str(sales.get("Sku") or lin.get("Sku") or "").strip()
        item_row: Dict[str, Any] = {}
        if user_id and item_id:
            if item_id not in item_cache:
                got, _err = fetch_qb_item_by_id(user_id, item_id)
                item_cache[item_id] = got if isinstance(got, dict) else {}
            item_row = item_cache[item_id]
        from_item = str(item_row.get("Sku") or "").strip()
        sku_aliases: List[str] = []
        for s in (from_line, from_item, name):
            ss = str(s).strip()
            if ss and ss not in sku_aliases:
                sku_aliases.append(ss)
        for key in ("Sku", "Name", "FullyQualifiedName"):
            ss = str(item_row.get(key) or "").strip()
            if ss and ss not in sku_aliases:
                sku_aliases.append(ss)
        sku = sku_aliases[0] if sku_aliases else ""
        try:
            qty = float(sales.get("Qty", 1) or 1)
        except (TypeError, ValueError):
            qty = 1.0
        try:
            amt = float(lin.get("Amount", 0) or 0)
        except (TypeError, ValueError):
            amt = 0.0
        rate_val = sales.get("UnitPrice")
        try:
            rate = float(rate_val) if rate_val is not None else (amt / qty if qty else 0.0)
        except (TypeError, ValueError):
            rate = amt / qty if qty else 0.0
        out.append(
            {
                "description": desc,
                "sku": sku,
                "sku_aliases": sku_aliases,
                "qty": qty,
                "rate": rate,
                "amount": amt,
            }
        )
    return out


def _pdf_description_search_variants(old: str) -> List[str]:
    variants: List[str] = []
    seen: set[str] = set()
    o = str(old).strip()
    words = o.split()
    # PDF a veces usa guiones distintos o saltos; la API manda una sola línea
    o_hyphen = re.sub(r"[\u2010\u2011\u2012\u2013\u2014\u2212]", "-", o)
    o_flat = re.sub(r"\s+", " ", unicodedata.normalize("NFKC", o_hyphen)).strip()

    def add(v: str) -> None:
        v = (v or "").strip()
        if len(v) >= 3 and v not in seen:
            seen.add(v)
            variants.append(v)

    for v in (o, o_flat, o[:120], o_flat[:120], o[:80], o_flat[:80], o[:50], o_flat[:50], o[:30], o_flat[:30]):
        add(v)
    for n in (10, 8, 6, 4):
        if len(words) >= n:
            vw = " ".join(words[:n]).strip()
            add(vw)
            if len(vw) > 45:
                add(vw[:45])
    return variants


_PDF_ROW_Y_TOL = 5.0
# Banda vertical por fila al buscar QTY/RATE/AMOUNT: bastante para 2”“3 líneas de descripción,
# pero sin el exceso anterior (~100) que mezclaba columnas de filas vecinas.
_PDF_INVOICE_ROW_Y_SPAN = 40.0
# Separación mínima entre “filas de ítem” al agrupar rects de search_for (más que el interlineado ~11”“12).
_PDF_DESC_CLUSTER_ROW_SEP = 16.0
# Debajo del texto SKU/DESCRIPTION/… de cabecera: aire para la franja gris antes de borrar ítems.
_PDF_TABLE_BODY_TOP_BELOW_HEADER_PT = 14.0
# Afinado visual al redibujar tabla (pt): SKU más izq., descripción más der., QTY bajo el título.
_PDF_TABLE_NUDGE_SKU_LEFT = 7.0
_PDF_TABLE_NUDGE_DESC_RIGHT = 6.0
# Resta al borde derecho de anclaje de QTY (texto alineado a la derecha en x_qty_right).
_PDF_TABLE_NUDGE_QTY_LEFT = 5.0
_PDF_SKU_REDACT_PAD = 7.0
_PDF_SKU_REDACT_PAD_BOTTOM_EXTRA = 8.0
# Al parchear invoice PDF: misma tipografía en SKU, descripción, cant., rate y amount
_PDF_PATCH_FONTNAME = "helv"
_PDF_PATCH_FONTSIZE = 9.5
_PDF_PATCH_SKU_FS_MIN = 7.8
# SKU: correr el inicio a la izquierda (pt) para una sola línea con más aire
_PDF_PATCH_SKU_SHIFT_LEFT = 14.0


def _qb_invoice_pdf_download_basename(doc: Any) -> str:
    """Nombre de archivo sugerido para PDF de invoice: `{doc}.pdf` (sin prefijo invoice_)."""
    base = str(doc or "invoice").strip().replace(" ", "_")
    for c in '<>:"/\\|?*':
        base = base.replace(c, "_")
    return f"{base[:80]}.pdf"


def _sku_display_every_other_from_first(s: str) -> str:
    """Solo caracteres en posiciones 1ª, 3ª, 5ª… (índices 0, 2, 4…); la 2ª, 4ª… se omiten."""
    t = str(s)
    return "".join(t[i] for i in range(0, len(t), 2))


def _pdf_find_first_rect_global(doc: Any, variants: List[str]) -> Optional[tuple[int, Any]]:
    """Primera coincidencia en orden página, y0, x0 (lectura típica). Retorna (page_index, Rect) o None."""
    return _pdf_find_first_rect_global_after_row(doc, variants, 0, 0.0)


def _pdf_find_first_rect_global_after_y(
    doc: Any, variants: List[str], y_min: float
) -> Optional[tuple[int, Any]]:
    """Compat: solo y global (falla en multi-página). Preferir _pdf_find_first_rect_global_after_row."""
    return _pdf_find_first_rect_global_after_row(doc, variants, 0, float(y_min))


def _pdf_find_first_rect_global_after_row(
    doc: Any,
    variants: List[str],
    min_page: int,
    min_y: float,
    min_variant_len: int = 3,
) -> Optional[tuple[int, Any]]:
    """Primera fila siguiente: prueba cada variante por separado, de la más larga a la más corta.

    Si se mezclan todas las variantes y se toma el mínimo global, prefijos cortos (p. ej. 4 palabras
    iguales en varios ítems) vuelven a coincidir con filas ya procesadas y destruyen el PDF.
    """
    import fitz  # pymupdf

    if not variants:
        return None
    uniq: List[str] = []
    seen: set[str] = set()
    for v in variants:
        v = str(v).strip()
        if len(v) >= int(min_variant_len) and v not in seen:
            seen.add(v)
            uniq.append(v)
    # Más específico primero (misma longitud: orden estable del caller)
    sorted_v = sorted(uniq, key=len, reverse=True)
    mp = int(min_page)
    floor = float(min_y)
    for variant in sorted_v:
        best_key: Optional[tuple[int, float, float]] = None
        best: Optional[tuple[int, Any]] = None
        for pno in range(len(doc)):
            if pno < mp:
                continue
            page = doc[pno]
            y_floor = floor if pno == mp else 0.0
            for r in page.search_for(variant):
                rr = fitz.Rect(r)
                if float(rr.y0) < y_floor - 1.5:
                    continue
                key = (pno, rr.y0, rr.x0)
                if best_key is None or key < best_key:
                    best_key = key
                    best = (pno, rr)
        if best is not None:
            return best
    return None


def _pdf_duplicate_description_skip_count(
    specs: List[Dict[str, Any]], line_idx: int
) -> int:
    """Cuántas líneas anteriores tienen la misma descripción (p. ej. dos ítems Apple idénticos en el PDF)."""
    d = str(specs[line_idx].get("description") or "").strip()
    if not d:
        return 0
    return sum(
        1
        for j in range(line_idx)
        if str(specs[j].get("description") or "").strip() == d
    )


def _pdf_duplicate_sku_skip_count(specs: List[Dict[str, Any]], line_idx: int) -> int:
    """Cuántas líneas anteriores tienen el mismo SKU (p. ej. MR9U3LL/A en fila 1 y 2)."""
    s = str(specs[line_idx].get("sku") or "").strip()
    if not s:
        return 0
    return sum(
        1
        for j in range(line_idx)
        if str(specs[j].get("sku") or "").strip() == s
    )


def _pdf_cluster_search_hits_into_rows(
    hits: List[tuple[int, float, float, Any]], y_sep: Optional[float] = None
) -> List[List[tuple[int, float, float, Any]]]:
    """Agrupa rectángulos de search_for en filas de tabla (una descripción multilínea = una fila)."""
    if not hits:
        return []
    sep = float(y_sep) if y_sep is not None else float(_PDF_DESC_CLUSTER_ROW_SEP)
    hits = sorted(hits, key=lambda t: (t[0], t[1], t[2]))
    rows: List[List[tuple[int, float, float, Any]]] = []
    for h in hits:
        if not rows:
            rows.append([h])
            continue
        mx_y = max(t[1] for t in rows[-1])
        if float(h[1]) > mx_y + sep:
            rows.append([h])
        else:
            rows[-1].append(h)
    return rows


def _pdf_find_rect_global_after_row_skip_occurrence(
    doc: Any,
    variants: List[str],
    min_page: int,
    min_y: float,
    skip: int,
    min_variant_len: int = 3,
) -> Optional[tuple[int, Any]]:
    """Como _pdf_find_first_rect_global_after_row pero salta filas enteras con la misma descripción.

    search_for devuelve un rect por línea de texto; skip debe contar filas de ítem, no fragmentos.
    """
    import fitz  # pymupdf

    if not variants:
        return None
    sk = max(0, int(skip))
    uniq: List[str] = []
    seen: set[str] = set()
    for v in variants:
        v = str(v).strip()
        if len(v) >= int(min_variant_len) and v not in seen:
            seen.add(v)
            uniq.append(v)
    sorted_v = sorted(uniq, key=len, reverse=True)
    mp = int(min_page)
    floor = float(min_y)
    for variant in sorted_v:
        hits: List[tuple[int, float, float, Any]] = []
        for pno in range(len(doc)):
            if pno < mp:
                continue
            page = doc[pno]
            y_floor = floor if pno == mp else 0.0
            for r in page.search_for(variant):
                rr = fitz.Rect(r)
                if float(rr.y0) < y_floor - 1.5:
                    continue
                hits.append((pno, float(rr.y0), float(rr.x0), rr))
        rows = _pdf_cluster_search_hits_into_rows(hits)
        if len(rows) > sk:
            row0 = sorted(rows[sk], key=lambda t: (t[1], t[2]))
            _p, _y0, _x0, rect = row0[0]
            return (int(_p), rect)
    return None


def _pdf_description_redact_rect(
    d_rect: Any, qty_rect: Optional[Any], extra_right_pt: float = 28.0
) -> Any:
    """Amplía el rect de descripción hacia la derecha para cubrir texto largo hasta antes de QTY."""
    import fitz  # pymupdf

    r = fitz.Rect(d_rect)
    if qty_rect is not None:
        q = fitz.Rect(qty_rect)
        r.x1 = max(float(r.x1), float(q.x0) - 4.0)
    else:
        r.x1 = float(r.x1) + float(extra_right_pt)
    return r


def _pdf_description_full_redact_rect(
    page: Any,
    d_rect: Any,
    qty_x0: Optional[float],
    extra_right_if_no_qty: float = 52.0,
    y_max: Optional[float] = None,
    x_hi_cap: Optional[float] = None,
) -> Any:
    """Unión de todo el bloque de descripción (varias líneas y continuaciones sin SKU) hasta la siguiente fila."""
    import fitz  # pymupdf

    d = fitz.Rect(d_rect)
    if qty_x0 is not None:
        x_hi = float(qty_x0) - 4.0
    else:
        x_hi = float(d.x1) + float(extra_right_if_no_qty)
        if x_hi_cap is not None:
            x_hi = min(x_hi, float(x_hi_cap))
    # Borde izquierdo de la columna DESCRIPCIÓN (nunca invadir columna SKU)
    desc_x_min = max(float(page.rect.x0) + 6.0, float(d.x0) - 3.0)

    next_sku_y: Optional[float] = None
    dd = page.get_text("dict")
    for block in dd.get("blocks", []):
        if block.get("type") != 0:
            continue
        for line in block.get("lines", []):
            for span in line.get("spans", []):
                b = span.get("bbox")
                if not b or len(b) < 4:
                    continue
                st = str(span.get("text") or "").strip()
                sx0, sy0, sx1, sy1 = float(b[0]), float(b[1]), float(b[2]), float(b[3])
                if sx0 > float(d.x0) - 10.0:
                    continue
                if sy0 <= float(d.y0) + 12.0:
                    continue
                if len(st) < 4 and (sx1 - sx0) < 24.0:
                    continue
                if len(st) <= 6 and st.replace(".", "").replace("/", "").isdigit():
                    continue
                if next_sku_y is None or sy0 < next_sku_y:
                    next_sku_y = sy0
    y_cap = (float(next_sku_y) - 7.0) if next_sku_y is not None else min(float(page.rect.y1), float(d.y1) + 140.0)

    u = fitz.Rect(d)
    for block in dd.get("blocks", []):
        if block.get("type") != 0:
            continue
        for line in block.get("lines", []):
            for span in line.get("spans", []):
                b = span.get("bbox")
                if not b or len(b) < 4:
                    continue
                sx0, sy0, sx1, sy1 = float(b[0]), float(b[1]), float(b[2]), float(b[3])
                if sy0 < float(d.y0) - 4.0:
                    continue
                if sy0 > y_cap + 2.0:
                    continue
                if sx0 < desc_x_min - 1.0:
                    continue
                if sx0 > x_hi + 2.0:
                    continue
                if min(sx1, x_hi) <= max(sx0, desc_x_min) + 0.5:
                    continue
                u |= fitz.Rect(b)
    u.x0 = max(float(u.x0), desc_x_min)
    x_right = min(x_hi, float(page.rect.x1) - 6.0)
    u.x1 = min(max(float(u.x1), float(d.x1) + 1.0), x_right)
    if float(u.y1) < float(d.y1):
        u.y1 = float(d.y1) + 2.0
    if y_max is not None:
        u.y1 = min(float(u.y1), float(y_max))
    return u


def _pdf_next_row_content_y_floor(page: Any, d_rect: Any, min_step: float = 12.0) -> Optional[float]:
    """Menor y0 de texto en columna de descripción (x >= d.x0) por debajo de esta fila; separa filas del PDF."""
    import fitz  # pymupdf

    d = fitz.Rect(d_rect)
    thresh = max(float(d.y0) + min_step, float(d.y1) - 2.0)
    dd = page.get_text("dict")
    best: Optional[float] = None
    for block in dd.get("blocks", []):
        if block.get("type") != 0:
            continue
        for line in block.get("lines", []):
            for span in line.get("spans", []):
                b = span.get("bbox")
                if not b or len(b) < 4:
                    continue
                sy0 = float(b[1])
                sx0 = float(b[0])
                if sy0 <= thresh + 1.0:
                    continue
                if sx0 < float(d.x0) + 6.0:
                    continue
                if best is None or sy0 < best:
                    best = sy0
    return best


def _sku_search_variants(sku: str) -> List[str]:
    sku = str(sku).strip()
    if not sku:
        return []
    min_len = 1 if len(sku) == 1 else 2
    variants: List[str] = []
    seen: set[str] = set()
    for v in (sku, sku[:40], sku[:25]):
        v = v.strip()
        if len(v) >= min_len and v not in seen:
            seen.add(v)
            variants.append(v)
    return variants


def _pdf_sku_variants_from_aliases(aliases: List[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for a in aliases:
        for v in _sku_search_variants(str(a).strip()):
            if v not in seen:
                seen.add(v)
                out.append(v)
    return out


def _pdf_sku_multiline_search_parts(s: str) -> List[str]:
    """Fragmentos que el PDF puede partir en varias líneas (p. ej. 'SM-' y 'X620NLBETPA')."""
    s = str(s).strip()
    if not s:
        return []
    seen: set[str] = set()
    out: List[str] = []

    def add(x: str) -> None:
        x = x.strip()
        if len(x) >= 2 and x not in seen:
            seen.add(x)
            out.append(x)

    add(s)
    if len(s) > 6:
        add(s[: min(50, len(s))])
        add(s[:40])
        add(s[:25])
    if "-" in s:
        parts = s.split("-")
        for i in range(len(parts) - 1):
            left = "-".join(parts[: i + 1]) + "-"
            right = "-".join(parts[i + 1 :])
            add(left)
            add(right)
    if len(s) >= 8:
        for cut in (len(s) // 2, (len(s) + 1) // 2, max(3, len(s) // 3)):
            if 2 <= cut <= len(s) - 2:
                add(s[:cut])
                add(s[cut:])
    return out


def _pdf_sku_all_search_strings(aliases: List[str]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for a in aliases:
        for part in _pdf_sku_multiline_search_parts(str(a).strip()):
            for v in _sku_search_variants(part):
                if v not in seen:
                    seen.add(v)
                    out.append(v)
    return out


def _pdf_cluster_sku_rects_one_row(found: List[Any], d_rect: Any) -> List[Any]:
    """Mantiene solo fragmentos de la misma celda SKU (2 líneas), no la fila siguiente."""
    import fitz  # pymupdf

    if not found:
        return []
    d = fitz.Rect(d_rect)
    rects = [fitz.Rect(x) for x in found]
    left = [r for r in rects if r.x1 <= d.x0 + 16.0]
    pool = left if left else rects
    seed = min(pool, key=lambda r: (r.y0, r.x0))
    out: List[Any] = []
    for r in sorted(pool, key=lambda z: (z.y0, z.x0)):
        if abs(float(r.x0) - float(seed.x0)) > 52.0:
            continue
        if float(r.y0) < float(seed.y0) - 5.0:
            continue
        if float(r.y0) > float(seed.y0) + 70.0:
            continue
        out.append(r)
    return out if out else [seed]


def _pdf_find_sku_column_union(
    page: Any,
    d_rect: Any,
    y_ref: float,
    parts: List[str],
    x_min: float = 0.0,
    x_max: Optional[float] = None,
) -> Optional[Any]:
    """Une rectángulos de fragmentos del SKU en la misma celda (varias líneas), sin invadir la fila de abajo."""
    import fitz  # pymupdf

    d = fitz.Rect(d_rect)
    # SKU queda a la izquierda del texto de descripción; límite derecho = borde izq. descripción (no +28 que invadía poco o mal)
    right_lim = float(x_max) if x_max is not None else float(d.x0) - 4.0
    y_lo = float(y_ref) - 14.0
    next_line = _pdf_next_row_content_y_floor(page, d_rect)
    y_hi = float(y_ref) + 68.0
    if next_line is not None:
        gap_nl = float(next_line) - float(y_ref)
        if gap_nl > 22.0:
            y_hi = min(y_hi, float(next_line) - 3.0)
    y_hi = max(y_hi, float(y_ref) + 34.0)
    found: List[Any] = []
    has_long = any(len(str(p).strip()) >= 6 for p in parts)
    for part in parts:
        ps = str(part).strip()
        if len(ps) < 2:
            continue
        if has_long and len(ps) < 4:
            continue
        for r in page.search_for(part):
            rr = fitz.Rect(r)
            if float(rr.y0) < y_lo or float(rr.y0) > y_hi:
                continue
            if rr.x0 < x_min - 4:
                continue
            if rr.x1 > right_lim + 6:
                continue
            if _pdf_rect_matches_description_block(rr, d):
                continue
            found.append(rr)
    if not found:
        return None
    col = _pdf_cluster_sku_rects_one_row(found, d_rect)
    u: Any = col[0]
    for r in col[1:]:
        u |= r
    return u


def _pdf_split_sku_two_lines(text: str) -> tuple[str, str]:
    """Divide SKU en dos líneas: guión más cercano al centro (evita cortes tipo '...D' + 'OT5-')."""
    t = str(text).strip()
    if len(t) <= 22:
        return t, ""
    mid = len(t) // 2
    best_i = -1
    best_d = 10**6
    for i, c in enumerate(t):
        if c in "-_/":
            d = abs(i + 0.5 - mid)
            if d < best_d:
                best_d = d
                best_i = i + 1
    cut = best_i if best_i > 0 else mid
    a, b = t[:cut].strip(), t[cut:].strip()
    if not b:
        return t, ""
    return a, b


def _pdf_insert_sku_in_union(
    page: Any,
    union_rect: Any,
    text: str,
    y_row_align: Optional[float] = None,
) -> None:
    """Redibuja el SKU en ≤2 líneas. y_row_align = d_rect.y0 alinea con la primera línea de descripción (mismo baseline que el texto de descripción)."""
    import fitz  # pymupdf

    u = fitz.Rect(union_rect)
    text = str(text).strip()
    if not text:
        return
    fn = _PDF_PATCH_FONTNAME
    margin_l = float(page.rect.x0) + 8.0
    inner = fitz.Rect(
        max(margin_l, float(u.x0) - float(_PDF_PATCH_SKU_SHIFT_LEFT)),
        float(u.y0) + 0.5,
        float(u.x1) - 1.0,
        float(u.y1) - 0.5,
    )
    if inner.width < 8 or inner.height < 6:
        inner = fitz.Rect(u)
    line1, line2 = _pdf_split_sku_two_lines(text)
    fs_max = min(
        float(_PDF_PATCH_FONTSIZE),
        max(float(_PDF_PATCH_SKU_FS_MIN), (float(inner.height) - 4.0) / 2.45),
    )
    fs_min = float(_PDF_PATCH_SKU_FS_MIN)

    def _line_len_pt(s: str, fs: float) -> float:
        try:
            font = fitz.Font(fontname=fn)
            return float(font.text_length(s, fontsize=fs))
        except Exception:
            return len(s) * fs * 0.52

    base_y = float(y_row_align) if y_row_align is not None else float(inner.y0)
    fs = fs_max
    wlim = max(10.0, float(inner.width) - 3.0)
    while fs >= fs_min - 1e-6:
        if _line_len_pt(line1, fs) > wlim or (line2 and _line_len_pt(line2, fs) > wlim):
            fs -= 0.4
            continue
        if not line2:
            break
        y1 = base_y + fs * 0.72
        y2 = y1 + fs * 1.17
        if y2 + fs * 0.3 <= float(u.y1) + 2.0:
            break
        fs -= 0.35

    x0 = float(inner.x0)
    if not line2:
        y0 = base_y + fs * 0.72
        page.insert_text(fitz.Point(x0, y0), line1, fontsize=fs, fontname=fn, color=(0, 0, 0))
        return
    y1 = base_y + fs * 0.72
    y2 = y1 + fs * 1.17
    page.insert_text(fitz.Point(x0, y1), line1, fontsize=fs, fontname=fn, color=(0, 0, 0))
    page.insert_text(fitz.Point(x0, y2), line2, fontsize=fs, fontname=fn, color=(0, 0, 0))


def _pdf_horiz_overlap(a: Any, b: Any) -> float:
    import fitz  # pymupdf

    aa = fitz.Rect(a)
    bb = fitz.Rect(b)
    return float(max(0.0, min(aa.x1, bb.x1) - max(aa.x0, bb.x0)))


def _pdf_rect_matches_description_block(rr: Any, d_rect: Any) -> bool:
    """Evita confundir el bloque de descripción con la celda SKU (mismo texto)."""
    import fitz  # pymupdf

    r = fitz.Rect(rr)
    d = fitz.Rect(d_rect)
    w = float(r.width)
    if w < 1.0:
        return False
    ov = _pdf_horiz_overlap(r, d)
    return ov > 0.82 * w and abs(r.y0 - d.y0) <= 2.5


def _numeric_search_variants(value: Any) -> List[str]:
    try:
        f = float(value)
    except (TypeError, ValueError):
        s = str(value).strip()
        return [s] if len(s) >= 1 else []
    out: List[str] = []
    seen: set[str] = set()

    def add(x: str) -> None:
        x = x.strip()
        if x and x not in seen:
            seen.add(x)
            out.append(x)

    add(f"{f:.2f}")
    add(f"{f:.1f}")
    if abs(f - int(f)) < 1e-9:
        add(str(int(round(f))))
        add(f"{int(round(f))}.00")
        add(f"{int(round(f))}.0")
    add(f"{f:g}")
    add(f"{f:,.2f}")
    add(f"{f:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
    if abs(f) >= 1000:
        add(f"{int(round(f)):,}")
    return out


def _pdf_rect_inflate_clipped(rect: Any, pad: float, page_rect: Any) -> Any:
    import fitz  # pymupdf

    r = fitz.Rect(rect)
    pr = fitz.Rect(page_rect)
    r.x0 -= pad
    r.y0 -= pad
    r.x1 += pad
    r.y1 += pad
    return r & pr


def _pdf_inflate_sku_for_redact(rect: Any, page_rect: Any) -> Any:
    """Borrado SKU: padding uniforme + extra abajo para guiones/partículas residuales."""
    import fitz  # pymupdf

    r = _pdf_rect_inflate_clipped(rect, float(_PDF_SKU_REDACT_PAD), page_rect)
    r.y1 = min(float(fitz.Rect(page_rect).y1) - 2.0, float(r.y1) + float(_PDF_SKU_REDACT_PAD_BOTTOM_EXTRA))
    return r & fitz.Rect(page_rect)


def _pdf_find_rect_right_band(
    page: Any,
    y_lo: float,
    y_hi: float,
    variants: List[str],
    x_min: float,
    y_prefer: Optional[float] = None,
    max_y_dist: Optional[float] = None,
) -> Optional[Any]:
    """A la derecha de x_min en [y_lo,y_hi]: prioriza cercanía a y_prefer; max_y_dist evita tomar 142.11/amount de la fila de abajo."""
    import fitz  # pymupdf

    y_mid = (float(y_lo) + float(y_hi)) * 0.5
    y_tgt = float(y_prefer) if y_prefer is not None else y_mid
    best: Optional[Any] = None
    best_key: Optional[tuple[float, float]] = None
    myd = float(max_y_dist) if max_y_dist is not None else None
    for variant in variants:
        for r in page.search_for(variant):
            rr = fitz.Rect(r)
            cy = (float(rr.y0) + float(rr.y1)) * 0.5
            if cy < y_lo or cy > y_hi:
                continue
            if rr.x0 < x_min - 2:
                continue
            dy = abs(cy - y_tgt)
            if myd is not None and dy > myd + 1e-6:
                continue
            x0 = float(rr.x0)
            key = (dy, x0)
            if best_key is None or key < best_key:
                best_key = key
                best = rr
    return best


def _pdf_find_rect_row_after(
    page: Any,
    y_ref: float,
    variants: List[str],
    x_min: float,
    y_tol: float = _PDF_ROW_Y_TOL,
    max_y_dist: Optional[float] = None,
) -> Optional[Any]:
    """Primera coincidencia cerca de y_ref (centro vertical del rect) con x0 >= x_min."""
    import fitz  # pymupdf

    best: Optional[Any] = None
    best_key: Optional[tuple[float, float]] = None
    eff_tol = (
        min(float(y_tol), float(max_y_dist)) if max_y_dist is not None else float(y_tol)
    )
    for variant in variants:
        for r in page.search_for(variant):
            rr = fitz.Rect(r)
            cy = (float(rr.y0) + float(rr.y1)) * 0.5
            if abs(cy - y_ref) > eff_tol:
                continue
            if rr.x0 < x_min - 2:
                continue
            dy = abs(cy - y_ref)
            x0 = float(rr.x0)
            key = (dy, x0)
            if best_key is None or key < best_key:
                best_key = key
                best = rr
    return best


def _pdf_sku_anchor_search_variants(spec: Dict[str, Any], sku_parts: List[str]) -> List[str]:
    """Strings de búsqueda para anclar fila por SKU: aliases completos primero; nunca fragmentos tipo 'MX'."""
    seen: set[str] = set()
    out: List[str] = []

    def add(s: str) -> None:
        s = str(s).strip()
        if len(s) < 4 or s in seen:
            return
        seen.add(s)
        out.append(s)

    for a in list(spec.get("sku_aliases") or []):
        add(str(a).strip())
    add(str(spec.get("sku") or "").strip())
    for s in list(out):
        if "/" in s:
            add(s.replace("/", ""))
    for p in sku_parts:
        ps = str(p).strip()
        if len(ps) >= 6:
            add(ps)
    return sorted(out, key=len, reverse=True)


def _pdf_try_anchor_row_from_sku(
    doc: Any,
    sku_parts: List[str],
    last_pno: int,
    last_y: float,
    spec: Dict[str, Any],
    desc_col_x0: Optional[float],
    sku_occurrence_skip: int = 0,
) -> Optional[tuple[int, Any]]:
    """Si la descripción no aparece como en la API (texto partido), ancla la fila por el SKU y arma un rect de descripción."""
    import fitz  # pymupdf

    cand = _pdf_sku_anchor_search_variants(spec, sku_parts)
    if not cand:
        return None
    sk = max(0, int(sku_occurrence_skip))
    found_s = _pdf_find_rect_global_after_row_skip_occurrence(
        doc, cand, last_pno, last_y, sk, min_variant_len=5
    )
    if not found_s:
        found_s = _pdf_find_rect_global_after_row_skip_occurrence(
            doc, cand, last_pno, max(0.0, float(last_y) - 18.0), sk, min_variant_len=5
        )
    if not found_s:
        found_s = _pdf_find_rect_global_after_row_skip_occurrence(
            doc, cand, last_pno, last_y, sk, min_variant_len=4
        )
    if not found_s:
        return None
    pno, sku_rect = found_s
    page = doc[pno]
    sr = fitz.Rect(sku_rect)
    loose_qty_x = max(float(sr.x1) + 18.0, float(sr.x0) + 125.0)
    row_top = float(sr.y0)
    if pno == last_pno and last_y > 0:
        y_band_lo = max(row_top - 4.0, last_y + 0.5)
    else:
        y_band_lo = row_top - 4.0
    y_band_hi = y_band_lo + float(_PDF_INVOICE_ROW_Y_SPAN)
    y_row_center = (y_band_lo + y_band_hi) * 0.5
    qty_rect = _pdf_find_rect_right_band(
        page,
        y_band_lo,
        y_band_hi,
        _numeric_search_variants(spec["qty"]),
        loose_qty_x,
        y_prefer=y_row_center,
    )
    if not qty_rect:
        qty_rect = _pdf_find_rect_row_after(
            page,
            y_row_center,
            _numeric_search_variants(spec["qty"]),
            loose_qty_x,
            y_tol=13.0,
        )
    if not qty_rect:
        qty_rect = _pdf_find_rect_row_after(
            page,
            y_row_center,
            _numeric_search_variants(spec["qty"]),
            float(sr.x0) + 158.0,
            y_tol=13.0,
        )
    dx0 = float(desc_col_x0) if desc_col_x0 is not None else float(sr.x1) + 8.0
    qx0 = float(fitz.Rect(qty_rect).x0) if qty_rect is not None else float(sr.x1) + 260.0
    d_top = min(float(sr.y0), y_band_lo) - 2.0
    d_bot = max(float(sr.y1), float(fitz.Rect(qty_rect).y1) if qty_rect else float(sr.y1)) + 22.0
    d_rect = fitz.Rect(dx0, d_top, max(qx0 - 3.0, dx0 + 40.0), d_bot)
    return (pno, d_rect)


def _pdf_find_rect_row_before(
    page: Any, y_ref: float, variants: List[str], x_max: float
) -> Optional[Any]:
    """Última coincidencia a la izquierda de x_max en la misma fila (p. ej. SKU)."""
    import fitz  # pymupdf

    best: Optional[Any] = None
    best_x: Optional[float] = None
    for variant in variants:
        for r in page.search_for(variant):
            rr = fitz.Rect(r)
            if abs(rr.y0 - y_ref) > _PDF_ROW_Y_TOL:
                continue
            if rr.x1 > x_max + 2:
                continue
            if best is None or rr.x0 > best_x:  # type: ignore[operator]
                best = rr
                best_x = rr.x0
    return best


def _pdf_insert_black_text(
    page: Any, rect: Any, text: str, min_x0: Optional[float] = None
) -> None:
    import fitz  # pymupdf

    r = fitz.Rect(rect)
    fs = float(_PDF_PATCH_FONTSIZE)
    x = float(r.x0)
    if min_x0 is not None:
        x = max(x, float(min_x0))
    pt = fitz.Point(x, r.y0 + fs * 0.72)
    page.insert_text(
        pt, text, fontsize=fs, fontname=_PDF_PATCH_FONTNAME, color=(0, 0, 0)
    )


def _fmt_pdf_qty_for_insert(qty: float) -> str:
    if abs(qty - int(qty)) < 1e-6:
        return str(int(round(qty)))
    return f"{qty:.2f}"


def _fmt_pdf_money_for_insert(x: float) -> str:
    return f"{float(x):,.2f}"


def _pdf_insert_text_right(
    page: Any, x_right: float, y_baseline: float, text: str, fontsize: float, fontname: str
) -> None:
    import fitz  # pymupdf

    t = str(text)
    try:
        font = fitz.Font(fontname=fontname)
        w = float(font.text_length(t, fontsize=fontsize))
    except Exception:
        w = len(t) * fontsize * 0.52
    x = float(x_right) - w
    page.insert_text(
        fitz.Point(x, float(y_baseline)),
        t,
        fontsize=float(fontsize),
        fontname=fontname,
        color=(0, 0, 0),
    )


def _pdf_try_detect_invoice_header_layout(page: Any) -> Optional[Dict[str, Any]]:
    """Detecta fila de cabecera SKU/DESCRIPTION/QTY/RATE/AMOUNT (plantilla QuickBooks u similar)."""
    import fitz  # pymupdf

    pairs = [
        ("SKU", "sku"),
        ("DESCRIPTION", "description"),
        ("QTY", "qty"),
        ("RATE", "rate"),
        ("AMOUNT", "amount"),
    ]
    buckets: Dict[str, List[Any]] = {}
    for lab, _ in pairs:
        hits: List[Any] = []
        for v in (lab, lab.title(), lab.capitalize()):
            hits = page.search_for(v)
            if hits:
                break
        buckets[lab] = list(hits) if hits else []
    sku_opts = buckets.get("SKU") or []
    if not sku_opts:
        return None
    sku_sorted = sorted(sku_opts, key=lambda r: (fitz.Rect(r).y0, fitz.Rect(r).x0))
    for sku_raw in sku_sorted[:24]:
        sr = fitz.Rect(sku_raw)
        cy_s = (float(sr.y0) + float(sr.y1)) * 0.5
        row: Dict[str, Any] = {"sku": sr}
        ok = True
        for lab, key in pairs[1:]:
            cand = buckets.get(lab) or []
            best: Optional[Any] = None
            best_dy = 1e9
            for h in cand:
                hr = fitz.Rect(h)
                cy = (float(hr.y0) + float(hr.y1)) * 0.5
                dy = abs(cy - cy_s)
                if dy < best_dy and dy < 9.0:
                    best_dy = dy
                    best = hr
            if best is None:
                ok = False
                break
            row[key] = fitz.Rect(best)
        if not ok:
            continue
        x_order = [
            row["sku"].x0,
            row["description"].x0,
            row["qty"].x0,
            row["rate"].x0,
            row["amount"].x0,
        ]
        if x_order != sorted(x_order):
            continue
        y_h_max = max(float(row[k].y1) for k in ("sku", "description", "qty", "rate", "amount"))
        pr = fitz.Rect(page.rect)
        y_body = float(y_h_max) + float(_PDF_TABLE_BODY_TOP_BELOW_HEADER_PT)
        q_hdr = row["qty"]
        r_hdr = row["rate"]
        # Cantidad alineada a la derecha bajo "QTY" (ancla ~fin de etiqueta), sin invadir RATE.
        x_qty_right = min(float(q_hdr.x1) + 0.5, float(r_hdr.x0) - 8.0)
        return {
            "x_sku": float(row["sku"].x0),
            "x_desc": float(row["description"].x0),
            "x_qty_left": float(q_hdr.x0),
            "x_qty_right": x_qty_right,
            "x_rate_left": float(r_hdr.x0),
            "x_amt_left": float(row["amount"].x0),
            "x_amt_right": float(row["amount"].x1),
            "y_data_start": y_body,
            "x_margin_l": max(float(pr.x0) + 4.0, float(row["sku"].x0) - 4.0),
            "x_margin_r": min(float(pr.x1) - 6.0, float(row["amount"].x1) + 12.0),
        }
    return None


def _pdf_find_items_block_y_end(page: Any, y_min: float) -> float:
    import fitz  # pymupdf

    pr = fitz.Rect(page.rect)
    best = float(pr.y1) - 28.0
    for m in (
        "BALANCE DUE",
        "Balance Due",
        "SUBTOTAL",
        "Subtotal",
        "TOTAL",
        "Total",
        "TAX",
        "Tax",
    ):
        for r in page.search_for(m):
            rr = fitz.Rect(r)
            if float(rr.y0) > float(y_min) + 15.0 and float(rr.y0) < best:
                best = float(rr.y0)
    return best - 10.0


def _pdf_table_layout_from_first_data_row(
    doc: Any, specs: List[Dict[str, Any]]
) -> Optional[tuple[int, Dict[str, Any]]]:
    """Si no hay cabecera reconocible, usa la1ª línea de ítem para columnas X y tope superior."""
    import fitz  # pymupdf

    if not specs:
        return None
    d_variants = _pdf_description_search_variants(specs[0]["description"])
    found = _pdf_find_rect_global_after_row_skip_occurrence(
        doc, d_variants, 0, 0.0, 0, min_variant_len=3
    )
    if not found:
        s0 = specs[0]
        sv = str(s0.get("sku") or "").strip()
        al = list(s0.get("sku_aliases") or [])
        if not al and sv:
            al = [sv]
        sp = _pdf_sku_all_search_strings(al)
        found = _pdf_try_anchor_row_from_sku(
            doc, sp, 0, 0.0, s0, None, sku_occurrence_skip=0
        )
    if not found:
        return None
    pno, d_rect = found
    page = doc[pno]
    dr = fitz.Rect(d_rect)
    loose_qty_x = max(float(dr.x1) + 18.0, float(dr.x0) + 125.0)
    y_lo = float(dr.y0) - 4.0
    y_hi = y_lo + float(_PDF_INVOICE_ROW_Y_SPAN)
    y_mid = (y_lo + y_hi) * 0.5
    qty_rect = _pdf_find_rect_right_band(
        page,
        y_lo,
        y_hi,
        _numeric_search_variants(specs[0]["qty"]),
        loose_qty_x,
        y_prefer=y_mid,
    )
    if not qty_rect:
        return None
    qr = fitz.Rect(qty_rect)
    x_after = float(qr.x1) - 1.0
    yc = (float(qr.y0) + float(qr.y1)) * 0.5
    y_lr = (yc - 10.0, yc + 10.0)
    rate_rect = _pdf_find_rect_right_band(
        page,
        y_lr[0],
        y_lr[1],
        _numeric_search_variants(specs[0]["rate"]),
        x_after,
        y_prefer=yc,
        max_y_dist=14.0,
    )
    if not rate_rect:
        return None
    rr = fitz.Rect(rate_rect)
    x_after2 = float(rr.x1) - 1.0
    amt_rect = _pdf_find_rect_right_band(
        page,
        y_lr[0],
        y_lr[1],
        _numeric_search_variants(specs[0]["amount"]),
        x_after2,
        y_prefer=yc,
        max_y_dist=14.0,
    )
    if not amt_rect:
        return None
    ar = fitz.Rect(amt_rect)
    pr = fitz.Rect(page.rect)
    x_sku = max(float(pr.x0) + 6.0, float(dr.x0) - 92.0)
    x_qty_right_fb = min(float(qr.x1) + 1.0, float(rr.x0) - 8.0)
    layout = {
        "x_sku": x_sku,
        "x_desc": float(dr.x0),
        "x_qty_left": float(qr.x0),
        "x_qty_right": x_qty_right_fb,
        "x_rate_left": float(rr.x0),
        "x_amt_left": float(ar.x0),
        "x_amt_right": float(ar.x1),
        # Primera fila de datos: no subir el borrado (evita comer cabecera si el match sube de más).
        "y_data_start": float(dr.y0) + 5.0,
        "x_margin_l": max(float(pr.x0) + 4.0, x_sku - 4.0),
        "x_margin_r": min(float(pr.x1) - 6.0, float(ar.x1) + 14.0),
    }
    return pno, layout


def _patch_invoice_pdf_items_table_rewrite(
    pdf_bytes: bytes,
    inv_obj: Dict[str, Any],
    new_description: str,
    user_id: Optional[int],
    sku_interleaved_display: bool = False,
) -> tuple[Optional[bytes], Optional[str]]:
    """Borra el bloque de la tabla de ítems y redibuja todas las filas con datos de la API."""
    try:
        import fitz  # pymupdf
    except ImportError:
        return None, None

    specs = _invoice_line_patch_specs(inv_obj, user_id, {})
    if not specs:
        return None, None

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        layout: Optional[Dict[str, Any]] = None
        pno = 0
        for pn in range(len(doc)):
            ly = _pdf_try_detect_invoice_header_layout(doc[pn])
            if ly:
                layout = ly
                pno = pn
                break
        if layout is None:
            fb = _pdf_table_layout_from_first_data_row(doc, specs)
            if not fb:
                return None, None
            pno, layout = fb

        page = doc[pno]
        pr = fitz.Rect(page.rect)
        y0 = float(layout["y_data_start"])
        y1 = _pdf_find_items_block_y_end(page, y0)
        if y1 <= y0 + float(len(specs)) * 10.0:
            y1 = min(float(pr.y1) - 30.0, y0 + max(120.0, float(len(specs)) * 22.0 + 40.0))

        x_sku = max(
            float(pr.x0) + 4.0,
            float(layout["x_sku"]) - float(_PDF_TABLE_NUDGE_SKU_LEFT),
        )
        x_desc = float(layout["x_desc"]) + float(_PDF_TABLE_NUDGE_DESC_RIGHT)
        x0 = min(float(layout["x_margin_l"]), x_sku - 3.0)
        x1 = float(layout["x_margin_r"])
        # Sin y0-6: ese margen subía el rect y borraba la franja gris y los títulos de columna.
        red = fitz.Rect(x0, y0, x1, y1)
        page.add_redact_annot(red, fill=(1, 1, 1))
        page.apply_redactions()

        fs = float(_PDF_PATCH_FONTSIZE)
        fn = _PDF_PATCH_FONTNAME
        avail_h = float(y1) - float(y0) - 10.0
        row_h = max(15.0, min(22.0, avail_h / float(len(specs)))) if specs else 18.0

        qty_anchor = float(
            layout.get("x_qty_right") or (float(layout["x_rate_left"]) - 10.0)
        )
        qty_right = qty_anchor - float(_PDF_TABLE_NUDGE_QTY_LEFT)
        q_col_l = float(layout.get("x_qty_left") or (qty_anchor - 36.0))
        rate_l = float(layout["x_rate_left"])
        qty_right = max(qty_right, q_col_l + 12.0)
        qty_right = min(qty_right, rate_l - 4.0)
        rate_right = float(layout["x_amt_left"]) - 8.0
        amt_right = float(layout["x_amt_right"]) + 4.0
        desc_max_w = max(40.0, qty_right - x_desc - 10.0)

        y_base = float(y0) + fs * 0.72
        for spec in specs:
            sku = str(spec.get("sku") or "").strip()
            if not sku and spec.get("sku_aliases"):
                sku = str(spec["sku_aliases"][0]).strip()
            if sku_interleaved_display:
                raw_sku = sku
                sku = _sku_display_every_other_from_first(sku)
                logging.info("PDF patch SKU interleaved: %r -> %r", raw_sku, sku)
            line1, line2 = _pdf_split_sku_two_lines(sku)
            page.insert_text(
                fitz.Point(x_sku, y_base),
                line1,
                fontsize=fs,
                fontname=fn,
                color=(0, 0, 0),
            )
            row_extra = 0.0
            if line2:
                page.insert_text(
                    fitz.Point(x_sku, y_base + fs * 1.18),
                    line2,
                    fontsize=fs,
                    fontname=fn,
                    color=(0, 0, 0),
                )
                row_extra = fs * 1.15
            desc_txt = str(new_description).strip()
            if desc_max_w > 50 and len(desc_txt) > 90:
                desc_txt = desc_txt[:87] + "…"
            page.insert_text(
                fitz.Point(x_desc, y_base),
                desc_txt,
                fontsize=fs,
                fontname=fn,
                color=(0, 0, 0),
            )
            _pdf_insert_text_right(
                page, qty_right, y_base, _fmt_pdf_qty_for_insert(float(spec["qty"])), fs, fn
            )
            _pdf_insert_text_right(
                page, rate_right, y_base, _fmt_pdf_money_for_insert(float(spec["rate"])), fs, fn
            )
            _pdf_insert_text_right(
                page, amt_right, y_base, _fmt_pdf_money_for_insert(float(spec["amount"])), fs, fn
            )
            y_base += max(row_h, fs + row_extra + 4.0)

        return (
            doc.tobytes(deflate=True),
            "Tabla de ítems regenerada desde QuickBooks (bloque único).",
        )
    finally:
        doc.close()






























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


















































BACKUP_VERSION = 2














_email_lock = threading.Lock()


















# ==========================
# INTEGRACIÓN MERCADOLIBRE
# ==========================
































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






























ORDERS_MAX_OFFSET = 100000  # ML puede limitar offset; si devuelve 400 se detiene antes








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
                tab_cuotas = ui.tab("Cuotas")
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
            "Cuotas": tab_cuotas,
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
        label_to_key = {"Home": "home", "Estadísticas": "estadisticas", "Ventas": "ventas", "Productos": "productos", "Precios": "precios", "Cuotas": "cuotas", "Invoices": "compras", "Stock": "stock", "Compras": "compras_lista", "Pedidos": "pedidos", "Históricos": "historicos", "Búsqueda": "busqueda", "Importacion": "importacion", "Datos": "datos", "Pesos": "pesos", "Balance": "balance", "Configuración": "configuracion", "Admin": "admin"}

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
        cuotas_cargado = [False]

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
            elif val == "Cuotas" and not cuotas_cargado[0]:
                cuotas_cargado[0] = True
                build_tab_cuotas(cuotas_container)
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
            with ui.row().classes("items-center gap-1 flex-wrap"):
                _nav_font = "text-lg font-medium"
                if perms.get("home", True):
                    ui.button("HOME", on_click=_go("Home")).props("flat dense no-caps").classes(_nav_font)
                ml_subs = [("ESTADÍSTICAS", "Estadísticas", "estadisticas"), ("VENTAS", "Ventas", "ventas"), ("PRODUCTOS", "Productos", "productos"), ("PRECIOS", "Precios", "precios"), ("CUOTAS", "Cuotas", "cuotas"), ("BÚSQUEDA", "Búsqueda", "busqueda"), ("BALANCE", "Balance", "balance")]
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
            with ui.row().classes("items-center gap-3 flex-wrap"):
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

            with ui.tab_panel(tab_cuotas):
                cuotas_container = ui.column().classes("w-full")

            with ui.tab_panel(tab_config):
                build_tab_config()

            with ui.tab_panel(tab_admin):
                admin_container = ui.column().classes("w-full")

        def on_tab_change(e) -> None:
            val = getattr(e, "value", None)
            if val:
                app.storage.user["last_tab"] = val
            if val:
                _lazy_load(val)

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


def _fmt_fecha_compras(s: str) -> str:
    """Formato fecha: 'Lunes 16-03-26 09:30' (dia dd-mm-aa hora:minutos)."""
    if not s or not str(s).strip():
        return "””"
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
            expires_at = (datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=int(expires_in))).isoformat()
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM ml_credentials WHERE user_id = ?", (user["id"],))
        cur.execute(
            "INSERT INTO ml_credentials (user_id, access_token, refresh_token, expires_at, raw_data) VALUES (?, ?, ?, ?, ?)",
            (user["id"], access_token, refresh_token, expires_at, json.dumps(data, ensure_ascii=False)),
        )
        conn.commit()
        conn.close()
        _enable_tabs_for_user(user["id"], TABS_ML)
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
            expires_at = (datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=int(expires_in))).isoformat()
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
    try:
        import fitz  # noqa: F401  # pymupdf ”” Invoices «Otra»
    except ImportError:
        logging.warning(
            "PyMuPDF no instalado (pip install pymupdf). Invoices → botón «Otra» no funcionará hasta instalarlo "
            "en el mismo entorno que ejecuta esta app (p. ej. %s -m pip install pymupdf).",
            sys.executable or "python3",
        )
    init_db()
    _arreglar_storage_nicegui()
    port = int(os.getenv("PORT", 8083))
    # En Render/cloud: PORT lo define la plataforma, no iniciar ngrok
    es_produccion = "PORT" in os.environ or os.getenv("RENDER") == "true"
    if not es_produccion and os.getenv("NGROK_AUTO_START", "0").strip().lower() in ("1", "true", "yes"):
        print("Iniciando ngrok...")
        _iniciar_ngrok(port)
    _secret = os.getenv("STORAGE_SECRET", "")
    if not _secret:
        print("ERROR: STORAGE_SECRET no configurado. Ver .env.example")
        sys.exit(1)
    # host 0.0.0.0 necesario para que Render/cloud pueda acceder al servicio
    ui.run(
        title="BDC systems",
        reload=False,
        host="0.0.0.0" if es_produccion else "127.0.0.1",
        port=port,
        storage_secret=os.getenv("STORAGE_SECRET", ""),
        reconnect_timeout=120,  # Evita "Connection lost" durante carga pesada (Precios con muchos productos)
        message_history_length=2000,  # Más mensajes al reconectar para restaurar UI
    )


if __name__ == "__main__":
    main()
