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

DB_PATH = Path(__file__).with_name("app.db")

# Versión del sistema: formato 2.aa.mm.dd.hh (aa=año, mm=mes, dd=día, hh=hora 00-23). Ej.: 2.26.04.14.12
VERSION = "2.26.05.27.28"

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
        if idx == n - 1 and desc in ("-", "—", ""):
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
# Banda vertical por fila al buscar QTY/RATE/AMOUNT: bastante para 2–3 líneas de descripción,
# pero sin el exceso anterior (~100) que mezclaba columnas de filas vecinas.
_PDF_INVOICE_ROW_Y_SPAN = 40.0
# Separación mínima entre “filas de ítem” al agrupar rects de search_for (más que el interlineado ~11–12).
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



def build_tab_cuotas(container) -> None:
    """Pestaña Cuotas: lista deduplicada por SKU con info de cuotas de cada publicación."""
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
                ui.label("Cargando cuotas...").classes("text-xl text-gray-700")

        async def _cargar_cuotas_async() -> None:
            try:
                data = await run.io_bound(ml_get_my_items, access_token, False)
            except requests.exceptions.HTTPError as e:
                result_area.clear()
                with result_area:
                    ui.label(f"❌ Error de la API de MercadoLibre: {e}").classes("text-negative mb-2")
                return
            except Exception as e:
                result_area.clear()
                with result_area:
                    ui.label(f"❌ Error al conectar: {e}").classes("text-negative")
                return
            # Construir grupos para identificar el item representante por grupo
            items_raw = data.get("results", [])
            grps: Dict[tuple, list] = {}
            for _it in items_raw:
                grps.setdefault(_cuotas_key(_it), []).append(_it)
            rep_ids: list = []
            for _g in grps.values():
                _rid = ""
                for _it in _g:
                    if not _it.get("catalog_listing") and str(_it.get("listing_type_id") or "").lower() == "gold_special":
                        _rid = str(_it.get("id") or "")
                        break
                if not _rid:
                    for _it in _g:
                        if _it.get("catalog_listing"):
                            _rid = str(_it.get("id") or "")
                            break
                if not _rid and _g:
                    _rid = str(_g[0].get("id") or "")
                if _rid:
                    rep_ids.append(_rid)
            seller_id = ""
            try:
                profile = await run.io_bound(ml_get_user_profile, access_token)
                seller_id = str((profile or {}).get("id") or "")
            except Exception:
                pass
            total_grupos = len(rep_ids)
            result_area.clear()
            promo_lbl = None
            with result_area:
                with ui.card().classes("w-full p-8 items-center gap-4"):
                    ui.spinner(size="xl")
                    promo_lbl = ui.label(f"Cargando promociones 0/{total_grupos}...").classes("text-xl text-gray-700")
            promo_data: Dict[str, Dict] = {}
            for _i, _iid in enumerate(rep_ids):
                promo_data[_iid] = await run.io_bound(_get_promo_data, access_token, _iid, seller_id)
                if promo_lbl:
                    promo_lbl.set_text(f"Cargando promociones {_i + 1}/{total_grupos}...")
            try:
                _mostrar_tabla_cuotas(result_area, data, access_token, promo_data, container, user["id"])
            except Exception as e:
                result_area.clear()
                with result_area:
                    ui.label(f"❌ Error al mostrar datos: {e}").classes("text-negative")

        background_tasks.create(_cargar_cuotas_async(), name="cargar_cuotas")


def _cuotas_key(it: dict) -> tuple:
    sku = (it.get("seller_sku") or "").strip()
    if sku:
        return ("sku", sku)
    cpid = (it.get("catalog_product_id") or "").strip()
    if cpid:
        return ("catalog", cpid)
    return ("id", str(it.get("id") or ""))


def _cuotas_score(it: dict) -> tuple:
    status_score = {"active": 2, "paused": 1, "closed": 0}.get(
        str(it.get("status") or "").lower(), 0)
    cuotas_score = 1 if str(it.get("listing_type_id") or "").lower() == "gold_pro" else 0
    stock_score = int(it.get("available_quantity") or 0)
    return (status_score, cuotas_score, stock_score)


def _get_promo_data(access_token: str, item_id: str, seller_id: str = "") -> dict:
    """Obtiene precio promo, % ML y % vendedor. Misma cascada de 3 intentos que el popup de precios."""
    empty: dict = {"price_promo": None, "meli_pct": None, "seller_pct": None}
    sp = ml_get_item_sale_price_full(access_token, item_id)
    if not sp or sp.get("amount") is None:
        return empty
    amt_f = float(sp["amount"])
    reg = sp.get("regular_amount")
    if reg is None or float(reg) <= 0 or abs(float(reg) - amt_f) <= 0.01:
        return empty
    reg_f = float(reg)
    total_pct = (reg_f - amt_f) / reg_f * 100
    cid = sp.get("campaign_id")
    pid = sp.get("promotion_id")
    pt  = (sp.get("promotion_type") or "").strip().upper()
    d = None
    if cid:
        d = ml_get_promotion_item_discounts_by_campaign(
            access_token, str(cid), item_id, total_pct,
            seller_id, promotion_type_hint=pt
        )
    if d is None and pid and pt and not str(pid).upper().startswith("OFFER-"):
        d = ml_get_promotion_item_discounts(
            access_token, str(pid), pt, item_id, total_pct
        )
    if d is None and seller_id:
        d = ml_get_promotion_item_discounts_by_user(
            access_token, item_id, seller_id, total_pct
        )
    if d:
        return {"price_promo": amt_f, "meli_pct": d.get("meli_pct"), "seller_pct": d.get("seller_pct")}
    return {"price_promo": amt_f, "meli_pct": None, "seller_pct": None}


def _mostrar_tabla_cuotas(result_area, data: Dict[str, Any], access_token: str, promo_data: Optional[Dict[str, Dict]] = None, container=None, user_id: Optional[int] = None) -> None:
    """Pinta la tabla de cuotas con columnas agrupadas por tipo de publicación."""
    items = data.get("results", [])
    result_area.clear()

    if not items:
        with result_area:
            ui.label("No se encontraron publicaciones activas.").classes("text-gray-600")
        return

    uid = user_id or 1
    cuotas_3x  = float(get_cotizador_param("cuotas_3x",  uid) or 0.084)
    cuotas_6x  = float(get_cotizador_param("cuotas_6x",  uid) or 0.123)
    cuotas_9x  = float(get_cotizador_param("cuotas_9x",  uid) or 0.157)
    cuotas_12x = float(get_cotizador_param("cuotas_12x", uid) or 0.192)

    # ── Agrupación ────────────────────────────────────────────────────────────
    groups: Dict[tuple, List[Dict[str, Any]]] = {}
    for it in items:
        groups.setdefault(_cuotas_key(it), []).append(it)

    def _slot(it: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not it:
            return {"id": "", "permalink": "", "price": None}
        return {
            "id": str(it.get("id") or ""),
            "permalink": it.get("permalink") or "",
            "price": it.get("price"),
        }

    def _build_row(group: List[Dict[str, Any]]) -> Dict[str, Any]:
        propia = catalogo = x3 = x6 = x9 = x12 = None
        for it in group:
            cuotas = _cuotas_desde_item(it)
            is_cat = it.get("catalog_listing") is True
            lt = str(it.get("listing_type_id") or "").lower()
            if not is_cat and lt == "gold_special":
                if propia is None or _cuotas_score(it) > _cuotas_score(propia):
                    propia = it
            if is_cat:
                if catalogo is None or _cuotas_score(it) > _cuotas_score(catalogo):
                    catalogo = it
            if cuotas == "x3":
                if x3 is None or _cuotas_score(it) > _cuotas_score(x3):
                    x3 = it
            if cuotas == "x6":
                if x6 is None or _cuotas_score(it) > _cuotas_score(x6):
                    x6 = it
            if cuotas == "x9":
                if x9 is None or _cuotas_score(it) > _cuotas_score(x9):
                    x9 = it
            if cuotas == "x12":
                if x12 is None or _cuotas_score(it) > _cuotas_score(x12):
                    x12 = it
        rep = max(group, key=_cuotas_score)
        # SKU: usa el del representante; si vacío, busca en todos los items del grupo
        sku = (rep.get("seller_sku") or "").strip()
        if not sku:
            for it in group:
                candidate = (it.get("seller_sku") or "").strip()
                if not candidate:
                    candidate = (it.get("seller_custom_field") or "").strip()
                if not candidate:
                    for att in (it.get("attributes") or []):
                        if (att.get("id") or "").strip().upper() == "SELLER_SKU":
                            v = att.get("value_name") or att.get("value") or att.get("value_id")
                            if v:
                                candidate = str(v).strip()
                                break
                if candidate:
                    sku = candidate
                    break
        # Stock: prefiere propia, fallback catálogo
        stock_val = None
        if propia is not None:
            stock_val = propia.get("available_quantity")
        elif catalogo is not None:
            stock_val = catalogo.get("available_quantity")
        best = propia or catalogo or rep
        return {
            "marca":         rep.get("marca") or "—",
            "seller_sku":    sku,
            "title":         best.get("title") or "",
            "thumbnail":     best.get("thumbnail") or "",
            "stock":         stock_val,
            "promo_item_id": str(best.get("id") or ""),
            "propia":        _slot(propia),
            "catalogo":      _slot(catalogo),
            "x3":            _slot(x3),
            "x6":            _slot(x6),
            "x9":            _slot(x9),
            "x12":           _slot(x12),
        }

    rows_all = [_build_row(g) for g in groups.values()]
    rows_all.sort(key=lambda r: r.get("title", "").lower())
    _pd = promo_data or {}
    for _row in rows_all:
        _row["promo"] = _pd.get(_row.get("promo_item_id", ""), {"price_promo": None, "meli_pct": None, "seller_pct": None})

    filtrados_ref: Dict[str, list]  = {"val": list(rows_all)}
    sort_col_ref:  Dict[str, str]   = {"val": "title"}
    sort_asc_ref:  Dict[str, bool]  = {"val": True}

    def fmt_moneda(val: Any) -> str:
        if val is None:
            return "—"
        try:
            return "$" + f"{int(float(val)):,}".replace(",", ".")
        except (TypeError, ValueError):
            return "—"

    # (gkey, grupo_label, bg_even, bg_odd, border_color)
    GROUPS = [
        ("propia",   "Publicación Propia", "#F0F7FF", "#DCEEFB", "#C5DCFA"),
        ("catalogo", "Catálogo",           "#F1F8F1", "#D6EDD6", "#B8D9B8"),
        ("x3",       "3 Cuotas",           "#FFFAF0", "#FFF0CC", "#FFE0B2"),
        ("x6",       "6 Cuotas",           "#FAF5FC", "#EDD9F5", "#E1BEE7"),
        ("x9",       "9 Cuotas",           "#FFF5F5", "#FFE0E0", "#FFCCCC"),
        ("x12",      "12 Cuotas",          "#F5F5FF", "#E0E0FF", "#CCCCFF"),
    ]
    PROMO_BG_EVEN = "#FFF8E1"
    PROMO_BG_ODD  = "#FFECB3"
    PROMO_BORDER  = "#FFD54F"

    SORT_KEY: Dict[str, Any] = {
        "marca":          lambda r: (r.get("marca") or "").lower(),
        "seller_sku":     lambda r: (r.get("seller_sku") or "").lower(),
        "title":          lambda r: r.get("title", "").lower(),
        "stock":          lambda r: int(r.get("stock") or 0),
        "propia_price":   lambda r: r["propia"]["price"]   if r["propia"]["price"]   is not None else -1,
        "catalogo_price": lambda r: r["catalogo"]["price"] if r["catalogo"]["price"] is not None else -1,
        "x3_price":       lambda r: r["x3"]["price"]       if r["x3"]["price"]       is not None else -1,
        "x6_price":       lambda r: r["x6"]["price"]       if r["x6"]["price"]       is not None else -1,
        "x9_price":       lambda r: r["x9"]["price"]       if r["x9"]["price"]       is not None else -1,
        "x12_price":      lambda r: r["x12"]["price"]      if r["x12"]["price"]      is not None else -1,
    }

    TH_HDR1 = "font-weight:700;font-size:11px;padding:5px 6px;border:1px solid #1565c0;background:#1976d2;color:white;letter-spacing:0.05em;text-transform:uppercase;box-shadow:0 2px 4px rgba(0,0,0,0.15)"
    TH_HDR2 = "font-weight:500;font-size:10px;padding:4px 5px;border:1px solid #1565c0;background:#1565c0;color:rgba(255,255,255,0.85)"
    TD_BASE = "padding:3px 6px;border-bottom:1px solid #e5e7eb;font-size:10px"

    n_propios  = sum(1 for r in rows_all if r["propia"]["id"])
    n_catalogo = sum(1 for r in rows_all if r["catalogo"]["id"])
    n_x3       = sum(1 for r in rows_all if r["x3"]["id"])
    n_x6       = sum(1 for r in rows_all if r["x6"]["id"])
    n_x9       = sum(1 for r in rows_all if r["x9"]["id"])
    n_x12      = sum(1 for r in rows_all if r["x12"]["id"])
    n_promos   = sum(1 for r in rows_all if r.get("promo", {}).get("price_promo") is not None)
    _tot = len(rows_all)
    pct_x3  = n_x3  / _tot * 100 if _tot else 0
    pct_x6  = n_x6  / _tot * 100 if _tot else 0
    pct_x9  = n_x9  / _tot * 100 if _tot else 0
    pct_x12 = n_x12 / _tot * 100 if _tot else 0

    with result_area:
        with ui.card().classes("w-full mb-2 p-3 bg-grey-2"):
            with ui.row().classes("items-center gap-4 flex-wrap justify-between"):
                with ui.row().classes("items-center gap-4 flex-wrap"):
                    for label, count in [
                        ("Publicaciones únicas",          len(rows_all)),
                        ("Propios",                       n_propios),
                        ("Catálogo",                      n_catalogo),
                        (f"En 3 cuotas ({pct_x3:.0f}%)",  n_x3),
                        (f"En 6 cuotas ({pct_x6:.0f}%)",  n_x6),
                        (f"En 9 cuotas ({pct_x9:.0f}%)",  n_x9),
                        (f"En 12 cuotas ({pct_x12:.0f}%)", n_x12),
                        ("En promoción",                  n_promos),
                    ]:
                        with ui.element("div").style("display:flex;flex-direction:column;align-items:center;min-width:80px"):
                            ui.label(str(count)).classes("text-primary text-xl font-bold leading-tight")
                            ui.label(label).classes("text-xs text-gray-600 text-center")
                    ui.element("div").style("width:1px;height:48px;background:#bdbdbd;align-self:center;margin:0 4px")
                    for _label, _rate in [
                        ("3x_campaign",  cuotas_3x),
                        ("6x_campaign",  cuotas_6x),
                        ("9x_campaign",  cuotas_9x),
                        ("12x_campaign", cuotas_12x),
                    ]:
                        with ui.element("div").style("display:flex;flex-direction:column;align-items:center;min-width:70px"):
                            ui.label(f"{_rate*100:.1f}%").classes("text-secondary text-xl font-bold leading-tight")
                            ui.label(_label).classes("text-xs text-gray-600 text-center")
                if container is not None:
                    ui.element("div").style("width:1px;height:48px;background:#bdbdbd;align-self:center;margin:0 4px")
                    ui.button("Sincronizar", icon="sync", on_click=lambda: build_tab_cuotas(container)).props("flat dense")

        with ui.row().classes("items-center gap-3 mb-3"):
            filtro_cuotas_sel = ui.select(
                {"cualquiera": "Cualquiera", "sin_cuotas": "Sin cuotas", "con_cuotas": "Con cuotas"},
                value="cualquiera", label="Cuotas"
            ).classes("w-36").props("outlined dense")
            filtro_promo_sel = ui.select(
                {"cualquiera": "Cualquiera", "con_promo": "Con promoción", "sin_promo": "Sin promoción"},
                value="cualquiera", label="Promos"
            ).classes("w-36").props("outlined dense")
            filtro_check_sel = ui.select(
                {"todos": "Todos", "ok": "OK", "alto": "Precio alto", "bajo": "Precio bajo"},
                value="todos", label="Check%"
            ).classes("w-36").props("outlined dense")
            filtro_input = ui.input(placeholder="Filtrar por SKU o Nombre...").props("outlined dense clearable").classes("w-72")

        header_div = ui.element("div").style("width:100%;overflow:hidden")
        table_container = ui.element("div").style("width:100%;height:65vh;overflow-y:scroll;overflow-x:auto")
        _hid = header_div.id
        _cid = table_container.id
        async def _setup_sync_once() -> None:
            await ui.run_javascript(
                f"(function(){{"
                f"var body=document.getElementById('c{_cid}');"
                f"var hdr=document.getElementById('c{_hid}');"
                f"if(!body||!hdr)return;"
                f"body.addEventListener('scroll',function(){{hdr.scrollLeft=body.scrollLeft;}});"
                f"function _sg(){{hdr.style.paddingRight=(body.offsetWidth-body.clientWidth)+'px';}}"
                f"_sg();new ResizeObserver(_sg).observe(body);"
                f"}})();"
            )
        ui.timer(0.1, _setup_sync_once, once=True)

        def _sort_rows(rows: list) -> list:
            key_fn = SORT_KEY.get(sort_col_ref["val"], lambda r: r.get("title", "").lower())
            return sorted(rows, key=key_fn, reverse=not sort_asc_ref["val"])

        def _ind(col: str) -> str:
            if sort_col_ref["val"] != col:
                return ""
            return " ▲" if sort_asc_ref["val"] else " ▼"

        def _build_colgroup() -> None:
            with ui.element("colgroup"):
                ui.element("col").style("width:4%")
                ui.element("col").style("width:7%")
                ui.element("col").style("width:20%")
                ui.element("col").style("width:3%")
                ui.element("col").style("width:2%")
                ui.element("col").style("width:2%")
                ui.element("col").style("width:2%")
                for gkey, *_ in GROUPS:
                    if gkey == "propia":
                        ui.element("col").style("width:7%")
                        ui.element("col").style("width:4%")
                    elif gkey == "catalogo":
                        ui.element("col").style("width:4%")
                        ui.element("col").style("width:3%")
                    else:
                        ui.element("col").style("width:4%")
                        ui.element("col").style("width:3%")
                        ui.element("col").style("width:2%")

        def _render(rows: list) -> None:
            header_div.clear()
            table_container.clear()
            with header_div:
                with ui.element("table").style("table-layout:fixed;width:100%;border-collapse:separate;border-spacing:0"):
                    _build_colgroup()
                    with ui.element("thead"):
                        with ui.element("tr"):
                            with ui.element("th").props('rowspan="2"').style(f"{TH_HDR1};width:4%;text-align:center;cursor:pointer").on("click", lambda: _on_sort("marca")):
                                ui.label("Marca" + _ind("marca"))
                            with ui.element("th").props('rowspan="2"').style(f"{TH_HDR1};width:7%;text-align:center;cursor:pointer").on("click", lambda: _on_sort("seller_sku")):
                                ui.label("SKU" + _ind("seller_sku"))
                            with ui.element("th").props('rowspan="2"').style(f"{TH_HDR1};width:20%;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;text-align:center;cursor:pointer").on("click", lambda: _on_sort("title")):
                                ui.label("Nombre" + _ind("title"))
                            with ui.element("th").props('rowspan="2"').style(f"{TH_HDR1};width:3%;text-align:center;cursor:pointer").on("click", lambda: _on_sort("stock")):
                                ui.label("Stock" + _ind("stock"))
                            with ui.element("th").props('colspan="3"').style(f"{TH_HDR1};border-left:2px solid {PROMO_BORDER};text-align:center"):
                                ui.label("Promociones")
                            for gkey, glabel, gbg_e, gbg_o, gborder in GROUPS:
                                _cspan = "3" if gkey not in ("propia", "catalogo") else "2"
                                with ui.element("th").props(f'colspan="{_cspan}"').style(f"{TH_HDR1};border-left:2px solid {gborder};text-align:center"):
                                    ui.label(glabel)
                        with ui.element("tr"):
                            with ui.element("th").style(f"{TH_HDR2};width:2%;border-left:2px solid {PROMO_BORDER};text-align:center"):
                                ui.label("% ML")
                            with ui.element("th").style(f"{TH_HDR2};width:2%;text-align:center"):
                                ui.label("% Vend.")
                            with ui.element("th").style(f"{TH_HDR2};width:2%;text-align:center"):
                                ui.label("% vs Propia")
                            for gkey, glabel, gbg_e, gbg_o, gborder in GROUPS:
                                pcol = f"{gkey}_price"
                                if gkey == "propia":
                                    with ui.element("th").style(f"{TH_HDR2};width:7%;border-left:2px solid {gborder};text-align:center"):
                                        ui.label("Publicación")
                                    with ui.element("th").style(f"{TH_HDR2};width:4%;text-align:center;cursor:pointer").on("click", lambda pk=pcol: _on_sort(pk)):
                                        ui.label("Precio" + _ind(pcol))
                                else:
                                    with ui.element("th").style(f"{TH_HDR2};width:4%;border-left:2px solid {gborder};text-align:center;cursor:pointer").on("click", lambda pk=pcol: _on_sort(pk)):
                                        ui.label("Precio" + _ind(pcol))
                                    with ui.element("th").style(f"{TH_HDR2};width:3%;text-align:center"):
                                        ui.label("% vs Propia")
                                    if gkey != "catalogo":
                                        with ui.element("th").style(f"{TH_HDR2};width:2%;text-align:center"):
                                            ui.label("Check %")
            with table_container:
                if not rows:
                    ui.label("Sin resultados para el filtro aplicado.").classes("text-gray-500 mt-2")
                    return
                with ui.element("table").style("table-layout:fixed;width:100%;border-collapse:separate;border-spacing:0"):
                    _build_colgroup()
                    # ── Cuerpo ────────────────────────────────────────────────
                    with ui.element("tbody"):
                        for idx, row in enumerate(rows):
                            base_bg = "background:#ffffff" if idx % 2 == 0 else "background:#fafafa"
                            with ui.element("tr").style(base_bg).classes("hover:bg-blue-50"):
                                with ui.element("td").style(f"{TD_BASE};overflow:hidden;text-overflow:ellipsis;white-space:nowrap"):
                                    ui.label(row.get("marca") or "—")
                                with ui.element("td").style(f"{TD_BASE};overflow:hidden;text-overflow:ellipsis;white-space:nowrap"):
                                    ui.label(row.get("seller_sku") or "—")
                                with ui.element("td").style(f"{TD_BASE};overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:0"):
                                    title_text = row.get("title") or "—"
                                    def _abrir_detalle(r=row) -> None:
                                        dlg = ui.dialog()
                                        with dlg:
                                            with ui.card().classes("p-4 min-w-[500px] max-w-[700px]"):
                                                with ui.row().classes("items-start gap-3 mb-3"):
                                                    if r.get("thumbnail"):
                                                        ui.image(r["thumbnail"]).classes("w-16 h-16 object-contain rounded border")
                                                    with ui.column().classes("gap-1"):
                                                        ui.label(r.get("marca") or "—").classes("text-xs text-gray-500 uppercase tracking-wide")
                                                        ui.label(r.get("seller_sku") or "—").classes("text-sm font-mono text-gray-600")
                                                        ui.label(r.get("title") or "—").classes("text-base font-semibold")
                                                        sv = r.get("stock")
                                                        ui.label(f"Stock: {sv if sv is not None else '—'}").classes("text-sm text-gray-500")
                                                ui.separator().classes("my-2")
                                                ui.label("Publicaciones").classes("text-xs font-bold text-gray-500 uppercase mb-1")
                                                _pp = r["propia"]["price"]
                                                with ui.element("table").style("width:100%;border-collapse:collapse;font-size:11px"):
                                                    with ui.element("thead"):
                                                        with ui.element("tr"):
                                                            for _h in ["Tipo", "ID", "Precio", "% vs Propia"]:
                                                                with ui.element("th").style("padding:4px 8px;background:#1976d2;color:white;text-align:left;font-weight:600"):
                                                                    ui.label(_h)
                                                    with ui.element("tbody"):
                                                        for _tipo, _sk in [("Propia","propia"),("Catálogo","catalogo"),("3 Cuotas","x3"),("6 Cuotas","x6"),("9 Cuotas","x9"),("12 Cuotas","x12")]:
                                                            _s = r[_sk]; _sid = _s["id"]; _sp = _s["price"]; _slink = _s["permalink"]
                                                            with ui.element("tr").style("border-bottom:1px solid #e5e7eb"):
                                                                with ui.element("td").style("padding:3px 8px;font-weight:500"):
                                                                    ui.label(_tipo)
                                                                with ui.element("td").style("padding:3px 8px;font-family:monospace;font-size:10px"):
                                                                    if _sid and _slink:
                                                                        ui.link(_sid, _slink, new_tab=True).classes("text-blue-700 hover:underline")
                                                                    elif _sid:
                                                                        ui.label(_sid)
                                                                    elif _sk in ("x3", "x6", "x9", "x12"):
                                                                        ui.label("Crear desde panel ML").classes("text-gray-400 text-xs italic")
                                                                    else:
                                                                        ui.label("—").classes("text-gray-400")
                                                                with ui.element("td").style("padding:3px 8px;font-weight:600;text-align:right"):
                                                                    if _sk != "catalogo" and _sid and _sp is not None:
                                                                        def _edit_price(s2=_s, iid=_sid, tipo=_tipo, d_main=dlg, rr_main=r, af=_abrir_detalle) -> None:
                                                                            subdlg = ui.dialog()
                                                                            with subdlg:
                                                                                with ui.card().classes("p-4 min-w-[300px]"):
                                                                                    ui.label(f"Editar precio — {tipo}").classes("text-base font-semibold mb-2")
                                                                                    try:
                                                                                        _pa = float(s2["price"] or 0)
                                                                                    except (TypeError, ValueError):
                                                                                        _pa = 0.0
                                                                                    _inp = ui.input("Nuevo precio ($)", value=str(int(_pa))).classes("w-full")
                                                                                    _inp.props("type=number min=1 step=1")
                                                                                    def _guardar(sd=subdlg, i=_inp, s3=s2, item=iid, dm=d_main, rr=rr_main, afn=af) -> None:
                                                                                        try:
                                                                                            nuevo = float(i.value or 0)
                                                                                        except (TypeError, ValueError):
                                                                                            ui.notify("Precio inválido.", color="negative")
                                                                                            return
                                                                                        if nuevo < 1:
                                                                                            ui.notify("El precio debe ser al menos 1.", color="negative")
                                                                                            return
                                                                                        sd.close()
                                                                                        dm.close()
                                                                                        ui.notify("Actualizando precio...", color="info")
                                                                                        cl = context.client
                                                                                        async def _act(client_=cl, s4=s3, precio=nuevo, item2=item, rr2=rr, af2=afn) -> None:
                                                                                            try:
                                                                                                await run.io_bound(ml_update_item_price, access_token, item2, precio)
                                                                                                s4["price"] = precio
                                                                                                with client_:
                                                                                                    _render(_sort_rows(filtrados_ref["val"]))
                                                                                                    ui.notify("Precio actualizado.", color="positive")
                                                                                                    af2(rr2)
                                                                                            except Exception as err:
                                                                                                with client_:
                                                                                                    ui.notify(f"Error: {err}", color="negative")
                                                                                        background_tasks.create(_act())
                                                                                    with ui.row().classes("w-full justify-end gap-2 mt-3"):
                                                                                        ui.button("Cancelar", on_click=lambda sd=subdlg: sd.close()).props("flat")
                                                                                        ui.button("Guardar", on_click=_guardar, color="primary")
                                                                            subdlg.open()
                                                                        ui.button(fmt_moneda(_sp), on_click=_edit_price).props("flat dense").style(
                                                                            "font-weight:600;font-size:11px;padding:0 4px;min-height:0;color:inherit"
                                                                        )
                                                                    else:
                                                                        ui.label(fmt_moneda(_sp)).classes("" if _sp is not None else "text-gray-400")
                                                                with ui.element("td").style("padding:3px 8px;text-align:center"):
                                                                    if _sk != "propia" and _sp is not None and _pp is not None and float(_pp) != 0:
                                                                        _pct = (float(_sp) - float(_pp)) / float(_pp) * 100
                                                                        if abs(_pct) < 0.05:
                                                                            ui.label("=").style("color:#757575")
                                                                        elif _pct > 0:
                                                                            ui.label(f"+{_pct:.1f}%").style("color:#43a047;font-weight:500")
                                                                        else:
                                                                            ui.label(f"{_pct:.1f}%").style("color:#e53935;font-weight:500")
                                                                    else:
                                                                        ui.label("—").classes("text-gray-400")
                                                promo_d = r.get("promo", {})
                                                if promo_d.get("price_promo") is not None:
                                                    ui.separator().classes("my-2")
                                                    ui.label("Promoción").classes("text-xs font-bold text-gray-500 uppercase mb-1")
                                                    with ui.row().classes("gap-4 text-sm"):
                                                        ui.label(f"Precio promo: {fmt_moneda(promo_d['price_promo'])}").classes("font-semibold")
                                                        if promo_d.get("meli_pct") is not None:
                                                            ui.label(f"% ML: {promo_d['meli_pct']:.1f}%").style("color:#43a047")
                                                        if promo_d.get("seller_pct") is not None:
                                                            ui.label(f"% Vendedor: {promo_d['seller_pct']:.1f}%").style("color:#e65100")
                                                ui.separator().classes("my-2")
                                                with ui.row().classes("w-full justify-end"):
                                                    ui.button("Cerrar", on_click=dlg.close).props("flat")
                                        dlg.open()
                                    ui.button(title_text, on_click=_abrir_detalle).props("flat dense align=left").style(
                                        "font-size:10px;padding:0 2px;min-height:0;text-transform:none;color:inherit;"
                                        "font-weight:normal;width:100%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;text-align:left"
                                    )
                                with ui.element("td").style(f"{TD_BASE};text-align:center"):
                                    sv = row.get("stock")
                                    ui.label(str(sv) if sv is not None else "")
                                propia_price = row["propia"]["price"]
                                promo = row.get("promo", {})
                                promo_bg = PROMO_BG_EVEN if idx % 2 == 0 else PROMO_BG_ODD
                                promo_price = promo.get("price_promo")
                                _pb = f"background:{promo_bg};border-bottom:1px solid #e5e7eb;font-size:10px"
                                # % ML
                                with ui.element("td").style(f"{_pb};border-left:2px solid {PROMO_BORDER};padding:3px 6px;text-align:center"):
                                    meli_pct = promo.get("meli_pct")
                                    if meli_pct is not None:
                                        ui.label(f"{meli_pct:.1f}%").style("color:#43a047;font-weight:500")
                                    else:
                                        ui.label("")
                                # % Vendedor
                                with ui.element("td").style(f"{_pb};padding:3px 6px;text-align:center"):
                                    seller_pct = promo.get("seller_pct")
                                    if seller_pct is not None:
                                        ui.label(f"{seller_pct:.1f}%").style("color:#e65100;font-weight:500")
                                    else:
                                        ui.label("")
                                # % vs Propia (Promos) — positivo=verde, negativo=rojo
                                with ui.element("td").style(f"{_pb};padding:3px 6px;text-align:center"):
                                    if promo_price is not None and propia_price is not None and float(propia_price) != 0:
                                        pct_p = (float(promo_price) - float(propia_price)) / float(propia_price) * 100
                                        if abs(pct_p) < 0.05:
                                            ui.label("=").style("color:#757575")
                                        elif pct_p > 0:
                                            ui.label(f"+{pct_p:.1f}%").style("color:#43a047;font-weight:500")
                                        else:
                                            ui.label(f"{pct_p:.1f}%").style("color:#e53935;font-weight:500")
                                    else:
                                        ui.label("")
                                for gkey, glabel, gbg_e, gbg_o, gborder in GROUPS:
                                    gbg = gbg_e if idx % 2 == 0 else gbg_o
                                    slot = row[gkey]
                                    item_id   = slot["id"]
                                    permalink = slot["permalink"]
                                    price     = slot["price"]
                                    _gb = f"background:{gbg};border-bottom:1px solid #e5e7eb;font-size:10px"
                                    # Publicación — solo Propia
                                    if gkey == "propia":
                                        with ui.element("td").style(f"{_gb};border-left:2px solid {gborder};padding:3px 6px;font-size:9px;font-family:monospace;text-align:center;overflow:hidden;text-overflow:ellipsis;white-space:nowrap"):
                                            if item_id and permalink:
                                                ui.link(item_id, permalink, new_tab=True).classes("text-blue-700 hover:underline")
                                            elif item_id:
                                                ui.label(item_id)
                                            else:
                                                ui.label("")
                                    # Precio — todos (border-left en no-propia como separador de grupo)
                                    _brd = "" if gkey == "propia" else f"border-left:2px solid {gborder};"
                                    with ui.element("td").style(f"{_gb};{_brd}padding:3px 6px;font-weight:600;text-align:right"):
                                        if gkey != "catalogo" and item_id and price is not None:
                                            def _abrir_editar_cuota(gk=gkey, iid=item_id, sl=slot, r=row) -> None:
                                                dialog = ui.dialog()
                                                with dialog:
                                                    with ui.card().classes("p-4 min-w-[340px]"):
                                                        ui.label(f"Editar precio — {gk.upper()} — {r.get('seller_sku', '')}").classes("text-lg font-semibold mb-2")
                                                        ui.label(f"ID: {iid}").classes("text-sm text-gray-500 mb-3")
                                                        try:
                                                            precio_actual = float(sl["price"] or 0)
                                                        except (TypeError, ValueError):
                                                            precio_actual = 0.0
                                                        inp_precio = ui.input("Nuevo precio ($)", value=str(int(precio_actual))).classes("w-full")
                                                        inp_precio.props("type=number min=1 step=1")

                                                        def guardar(d=dialog, inp=inp_precio, s=sl, i=iid) -> None:
                                                            try:
                                                                nuevo = float(inp.value or 0)
                                                            except (TypeError, ValueError):
                                                                ui.notify("Precio inválido.", color="negative")
                                                                return
                                                            if nuevo < 1:
                                                                ui.notify("El precio debe ser al menos 1.", color="negative")
                                                                return
                                                            d.close()
                                                            ui.notify("Actualizando precio...", color="info")
                                                            cl = context.client

                                                            async def _actualizar(client_=cl, s2=s, precio=nuevo, item=i) -> None:
                                                                try:
                                                                    await run.io_bound(ml_update_item_price, access_token, item, precio)
                                                                    s2["price"] = precio
                                                                    with client_:
                                                                        ui.notify("Precio actualizado correctamente.", color="positive")
                                                                        _render(_sort_rows(filtrados_ref["val"]))
                                                                except requests.exceptions.HTTPError as err:
                                                                    with client_:
                                                                        ui.notify(f"Error al actualizar: {err}", color="negative")
                                                                except Exception as err:
                                                                    with client_:
                                                                        ui.notify(f"Error: {err}", color="negative")

                                                            background_tasks.create(_actualizar())

                                                        with ui.row().classes("w-full justify-end gap-2 mt-3"):
                                                            ui.button("Cancelar", on_click=lambda d=dialog: d.close()).props("flat")
                                                            ui.button("Guardar", on_click=guardar, color="primary")
                                                dialog.open()
                                            ui.button(fmt_moneda(price), on_click=_abrir_editar_cuota).props("flat dense").style("font-weight:600;font-size:10px;padding:0 4px;min-height:0;color:inherit")
                                        else:
                                            ui.label(fmt_moneda(price) if price is not None else "")
                                    # % vs Propia — todos menos Propia
                                    if gkey != "propia":
                                        with ui.element("td").style(f"{_gb};padding:3px 6px;text-align:center"):
                                            if price is not None and propia_price is not None and float(propia_price) != 0:
                                                pct = (float(price) - float(propia_price)) / float(propia_price) * 100
                                                if abs(pct) < 0.05:
                                                    ui.label("=").style("color:#757575")
                                                elif pct > 0:
                                                    ui.label(f"+{pct:.1f}%").style("color:#43a047;font-weight:500")
                                                else:
                                                    ui.label(f"{pct:.1f}%").style("color:#e53935;font-weight:500")
                                            else:
                                                ui.label("")
                                    # Check% — x3/x6/x9/x12
                                    if gkey not in ("propia", "catalogo"):
                                        with ui.element("td").style(f"{_gb};padding:3px 4px;text-align:center"):
                                            if price is not None and propia_price is not None and float(propia_price) != 0:
                                                _pct_c = (float(price) - float(propia_price)) / float(propia_price) * 100
                                                _tasa  = {"x3": cuotas_3x, "x6": cuotas_6x, "x9": cuotas_9x, "x12": cuotas_12x}.get(gkey, 0) * 100
                                                _diff  = _pct_c - _tasa
                                                if abs(_diff) <= 0.5:
                                                    ui.label("✓").style("color:#1976d2;font-weight:700;font-size:12px")
                                                elif _diff > 0.5:
                                                    ui.label("↑").style("color:#43a047;font-weight:700;font-size:12px")
                                                else:
                                                    ui.label("↓").style("color:#e53935;font-weight:700;font-size:12px")
                                            else:
                                                ui.label("")

        def _on_sort(col: str) -> None:
            if sort_col_ref["val"] == col:
                sort_asc_ref["val"] = not sort_asc_ref["val"]
            else:
                sort_col_ref["val"] = col
                sort_asc_ref["val"] = True
            _render(_sort_rows(filtrados_ref["val"]))

        def _on_filtro(e) -> None:
            txt = (getattr(filtro_input, "value", "") or "").strip().lower()
            cuotas_val = filtro_cuotas_sel.value
            promo_val  = filtro_promo_sel.value
            check_val  = filtro_check_sel.value
            result = list(rows_all)
            if txt:
                result = [
                    r for r in result
                    if txt in (r.get("seller_sku") or "").lower()
                    or txt in r.get("title", "").lower()
                ]
            if cuotas_val == "sin_cuotas":
                result = [r for r in result if not r["x3"]["id"] and not r["x6"]["id"] and not r["x9"]["id"] and not r["x12"]["id"]]
            elif cuotas_val == "con_cuotas":
                result = [r for r in result if r["x3"]["id"] or r["x6"]["id"] or r["x9"]["id"] or r["x12"]["id"]]
            if promo_val == "con_promo":
                result = [r for r in result if r.get("promo", {}).get("price_promo") is not None]
            elif promo_val == "sin_promo":
                result = [r for r in result if r.get("promo", {}).get("price_promo") is None]
            if check_val != "todos":
                _rates = {"x3": cuotas_3x, "x6": cuotas_6x, "x9": cuotas_9x, "x12": cuotas_12x}
                def _check_match(r: dict, target: str) -> bool:
                    pp = r["propia"]["price"]
                    if pp is None or float(pp) == 0:
                        return False
                    for gk, tasa in _rates.items():
                        p = r[gk]["price"]
                        if p is None:
                            continue
                        diff = (float(p) - float(pp)) / float(pp) * 100 - tasa * 100
                        if target == "ok"   and abs(diff) <= 0.5: return True
                        if target == "alto" and diff >  0.5:       return True
                        if target == "bajo" and diff < -0.5:       return True
                    return False
                result = [r for r in result if _check_match(r, check_val)]
            filtrados_ref["val"] = result
            _render(_sort_rows(filtrados_ref["val"]))

        filtro_input.on_value_change(_on_filtro)
        filtro_cuotas_sel.on_value_change(lambda *a: _on_filtro(None))
        filtro_promo_sel.on_value_change(lambda *a: _on_filtro(None))
        filtro_check_sel.on_value_change(lambda *a: _on_filtro(None))
        _render(_sort_rows(rows_all))


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
            n_items = len(data.get("results", []))
            area.clear()
            with area:
                with ui.card().classes("w-full p-8 items-center gap-4"):
                    ui.spinner(size="xl")
                    ui.label(f"Procesando {n_items} publicaciones...").classes("text-xl text-gray-700")
            await asyncio.sleep(0.1)
            try:
                _mostrar_tabla_precios(area, data, token, usr, on_actualizar, inc_paused_ref, f_stock_ref)
            except Exception as e:
                area.clear()
                with area:
                    ui.label(f"❌ Error al mostrar datos: {e}").classes("text-negative")

        background_tasks.create(_cargar_precios_async(result_area, access_token, user, cargar_precios, include_paused_ref, filtro_stock_ref), name="cargar_precios")


def _show_item_detail_dialog(
    row: Dict[str, Any],
    *,
    ml_comision: float,
    cuotas_3x: float, cuotas_6x: float, cuotas_9x: float, cuotas_12x: float,
    ml_debcre: float, ml_iibb_per: float,
    ml_envios: float, ml_envios_gratuitos: float,
    dolar_oficial: float,
    access_token: str,
    uid: int,
    items_loaded: List[Dict[str, Any]],
    on_saved=None,
    revisiones_hoy: Optional[Dict[str, bool]] = None,
) -> None:
    def fmt_moneda(val):
        if val is None: return "$0"
        try: return "$" + f"{int(float(val)):,}".replace(",", ".")
        except (TypeError, ValueError): return "$0"

    def fmt_usd(val):
        if val is None or val == "": return "u$0,00"
        try: return f"u${float(val):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        except (TypeError, ValueError): return "u$0,00"

    def fmt_pct2(val):
        if val is None: return "0,00%"
        try: return f"{float(val):.2f}%".replace(".", ",")
        except (TypeError, ValueError): return "0,00%"

    def _parse_moneda(s):
        if not s: return 0.0
        try: return float(str(s).replace("$", "").replace(".", "").replace(",", ".").strip())
        except (TypeError, ValueError): return 0.0

    def _calc_iva(precio, tipo_iva, comision, costo_usd):
        iva_venta = precio * tipo_iva / (1 + tipo_iva)
        iva_meli  = comision * 0.21 / 1.21
        iva_impor = 0.09 * costo_usd * dolar_oficial
        return iva_venta - iva_meli - iva_impor, iva_meli, iva_impor

    def _envio_a_restar(precio):
        return 0.0 if precio < ml_envios_gratuitos else ml_envios

    d = ui.dialog()
    _sku_rev = str(row.get("seller_sku") or row.get("id") or "").strip()
    _fecha_hoy = datetime.now().strftime("%Y-%m-%d")
    if revisiones_hoy is not None:
        _conn_r = get_connection()
        try:
            _conn_r.execute(
                "INSERT OR IGNORE INTO revisiones_diarias (sku, user_id, fecha, precio_cambiado) VALUES (?, ?, ?, 0)",
                (_sku_rev, uid, _fecha_hoy),
            )
            _conn_r.commit()
        finally:
            _conn_r.close()
        if _sku_rev not in revisiones_hoy:
            revisiones_hoy[_sku_rev] = False
            _cl_rev = context.client

            async def _refrescar_highlights():
                await asyncio.sleep(0.1)
                with _cl_rev:
                    if on_saved:
                        on_saved()

            background_tasks.create(_refrescar_highlights())
    inp_refs: Dict[str, Any] = {}
    recalc_ref: Dict[str, Any] = {}

    def _recalcular():
        precio_str = inp_refs.get("precio") and getattr(inp_refs["precio"], "value", None) or ""
        precio = _parse_moneda(precio_str)
        _iva_sel = inp_refs.get("tipo_iva")
        tipo_iva = float(getattr(_iva_sel, "value", None) or row.get("tipo_iva") or 0.105)
        costo    = float(row.get("costo") or 0)
        if precio < 1:
            precio = float(row.get("precio") or 0) or 1
        tiene_promo = row.get("price_original") is not None and row.get("promo_yo_pct") is not None
        precio_calc = precio
        if tiene_promo:
            precio_calc = float(row.get("price_original") or 0) * (1 - float(row.get("promo_yo_pct") or 0) / 100)
        comision = precio_calc * ml_comision
        cobrado  = precio_calc - comision
        deb_cred = precio_calc * ml_debcre
        iibb     = precio_calc * ml_iibb_per
        iva_venta = precio_calc * tipo_iva / (1 + tipo_iva)
        iva_total, iva_meli, iva_impor = _calc_iva(precio_calc, tipo_iva, comision, costo)
        envio       = _envio_a_restar(precio_calc)
        costo_pesos = costo * dolar_oficial
        cuotas_val  = str(row.get("cuotas") or "x1").strip().lower()
        tasa        = {"x3": cuotas_3x, "x6": cuotas_6x, "x9": cuotas_9x, "x12": cuotas_12x}.get(cuotas_val, 0.0)
        costo_cuotas = precio_calc * tasa if tasa else 0.0
        if costo_pesos <= 0:
            margen_pesos = margen_costo_pct = margen_venta_pct = 0.0
        else:
            margen_pesos     = cobrado - costo_pesos - iva_total - iibb - deb_cred - envio - costo_cuotas
            margen_costo_pct = (margen_pesos / costo_pesos * 100) if costo_pesos > 0 else 0.0
            margen_venta_pct = (margen_pesos / precio_calc * 100) if precio_calc > 0 else 0.0
        data = {
            "comision": comision, "cobrado": cobrado, "costo_cuotas": costo_cuotas,
            "iva_venta": iva_venta, "iva_total": iva_total, "iva_meli": iva_meli, "iva_impor": iva_impor,
            "deb_cred": deb_cred, "iibb": iibb, "envio": envio, "costo_pesos": costo_pesos,
            "margen_pesos": margen_pesos, "margen_costo_pct": margen_costo_pct, "margen_venta_pct": margen_venta_pct,
        }
        _pintar_recalc(recalc_ref["container"], data)

    def _pintar_recalc(cont, data):
        mp  = float(data.get("margen_pesos") or 0)
        cp  = float(data.get("costo_pesos") or 0)
        mcls = "font-bold text-black" if cp <= 0 else ("font-bold text-positive" if mp > 0 else "font-bold text-negative")
        cont.clear()
        with cont:
            for lbl_r, key_r, cls_r in [
                ("Comisión",  "comision",  "text-sm text-negative"),
                ("Cobrado",   "cobrado",   "text-sm font-bold text-primary"),
                ("Costo Cuotas", "costo_cuotas", "text-sm text-negative"),
                ("IVA venta", "iva_venta", "text-sm"),
                ("IVA neto", "iva_total", "text-sm text-negative"),
                ("Deb-Cred",  "deb_cred",  "text-sm text-negative"),
                ("IIBB",      "iibb",      "text-sm text-negative"),
                ("Envío",     "envio",     "text-sm text-negative"),
            ]:
                with ui.row().classes("w-full justify-between py-0.5 gap-4" + (" border-b-2 border-gray-300" if lbl_r == "Envío" else "")):
                    ui.label(lbl_r).classes("text-sm font-medium text-gray-600")
                    ui.label(fmt_moneda(data.get(key_r))).classes(cls_r)
            with ui.row().classes("w-full justify-between py-0.5 gap-4"):
                with ui.row().classes("gap-4"):
                    ui.label("IVA Meli").classes("text-sm font-medium text-gray-600")
                    ui.label(fmt_moneda(data.get("iva_meli"))).classes("text-sm")
                with ui.row().classes("gap-4"):
                    ui.label("IVA impor").classes("text-sm font-medium text-gray-600")
                    ui.label(fmt_moneda(data.get("iva_impor"))).classes("text-sm")
            with ui.row().classes("w-full justify-between py-1 gap-4"):
                ui.label("Gan $").classes("text-sm font-medium text-gray-600")
                ui.label(fmt_moneda(data.get("margen_pesos"))).classes(mcls)
            with ui.row().classes("w-full justify-between py-0.5 gap-4"):
                ui.label("Gan Vta %").classes("text-sm font-medium text-gray-600")
                ui.label(fmt_pct2(data.get("margen_venta_pct"))).classes(mcls)
            with ui.row().classes("w-full justify-between py-0.5 gap-4"):
                ui.label("Gan % Cos").classes("text-sm font-medium text-gray-600")
                ui.label(fmt_pct2(data.get("margen_costo_pct"))).classes(mcls)

    def _guardar(dlg):
        item_id  = str(row.get("id", ""))
        sku_grd  = str(row.get("seller_sku") or "").strip() or str(row.get("id") or "").strip()
        if not item_id:
            ui.notify("ID de publicación no válido.", color="negative"); return
        nuevo_precio   = _parse_moneda(getattr(inp_refs.get("precio"), "value", "") or "")
        nuevo_costo    = float(row.get("costo") or 0)
        _iva_ref       = inp_refs.get("tipo_iva")
        nuevo_tipo_iva = float(getattr(_iva_ref, "value", None) or row.get("tipo_iva") or 0.105)
        _fob_ref       = inp_refs.get("fob_usd")
        _fob_raw       = (getattr(_fob_ref, "value", "") or "").strip()
        try:
            nuevo_fob = float(_fob_raw) if _fob_raw else None
        except (ValueError, TypeError):
            nuevo_fob = None
        if nuevo_precio < 1:
            ui.notify("El precio debe ser al menos $1.", color="negative"); return
        dlg.close()
        ui.notify("Actualizando precio en MercadoLibre...", color="info")
        cl = context.client

        async def _actualizar():
            try:
                await run.io_bound(ml_update_item_price, access_token, item_id, nuevo_precio)
                if sku_grd:
                    def _save_db():
                        now_str = datetime.now().isoformat()
                        conn = get_connection()
                        try:
                            conn.execute(
                                """INSERT INTO productos (sku, user_id, costo_usd, fob_usd, tipo_iva, created_at, updated_at, costo_updated_at, price_updated_at)
                                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                                   ON CONFLICT(sku, user_id) DO UPDATE SET
                                       costo_usd=excluded.costo_usd, fob_usd=excluded.fob_usd, tipo_iva=excluded.tipo_iva,
                                       costo_updated_at=excluded.costo_updated_at, updated_at=excluded.updated_at,
                                       price_updated_at=excluded.price_updated_at""",
                                (sku_grd, uid, nuevo_costo, nuevo_fob, nuevo_tipo_iva, now_str, now_str, now_str, now_str),
                            )
                            if revisiones_hoy is not None:
                                conn.execute(
                                    """INSERT INTO revisiones_diarias (sku, user_id, fecha, precio_cambiado) VALUES (?, ?, ?, 1)
                                       ON CONFLICT(sku, user_id, fecha) DO UPDATE SET precio_cambiado=1""",
                                    (sku_grd, uid, datetime.now().strftime("%Y-%m-%d")),
                                )
                            conn.commit()
                        finally:
                            conn.close()
                    await run.io_bound(_save_db)
                    if revisiones_hoy is not None:
                        revisiones_hoy[sku_grd] = True
                for it in items_loaded:
                    if str(it.get("id")) == item_id:
                        it["precio"]    = nuevo_precio
                        it["price"]     = nuevo_precio
                        it["tipo_iva"]  = nuevo_tipo_iva
                        it["costo"]     = nuevo_costo
                        it["costo_usd"] = nuevo_costo
                        it["fob_usd"] = nuevo_fob
                        tiene_promo = it.get("price_original") is not None and it.get("promo_yo_pct") is not None
                        pc2 = nuevo_precio
                        if tiene_promo:
                            pc2 = float(it.get("price_original") or 0) * (1 - float(it.get("promo_yo_pct") or 0) / 100)
                        com2  = pc2 * ml_comision
                        cob2  = pc2 - com2
                        deb2  = pc2 * ml_debcre
                        iibb2 = pc2 * ml_iibb_per
                        it2, im2, ii2 = _calc_iva(pc2, nuevo_tipo_iva, com2, nuevo_costo)
                        env2  = _envio_a_restar(pc2)
                        cp2   = nuevo_costo * dolar_oficial
                        cv2   = pc2 * ({"x3": cuotas_3x, "x6": cuotas_6x, "x9": cuotas_9x, "x12": cuotas_12x}.get(str(it.get("cuotas") or "x1").lower(), 0.0))
                        if cp2 <= 0:
                            mg2 = mc2 = mv2 = 0.0
                        else:
                            mg2 = cob2 - cp2 - it2 - iibb2 - deb2 - env2 - cv2
                            mc2 = (mg2 / cp2 * 100) if cp2 > 0 else 0.0
                            mv2 = (mg2 / pc2 * 100) if pc2 > 0 else 0.0
                        it.update({"comision": com2, "cobrado": cob2, "costo_cuotas": cv2,
                                   "iva_total": it2, "iva_meli": im2, "iva_impor": ii2,
                                   "deb_cred": deb2, "iibb": iibb2, "envio": env2,
                                   "margen_pesos": mg2, "margen_costo_pct": mc2, "margen_venta_pct": mv2})
                        break
                with cl:
                    if on_saved:
                        on_saved()
                    ui.notify("Precio actualizado correctamente.", color="positive")
            except Exception as e:
                with cl:
                    ui.notify(f"Error al actualizar: {e}", color="negative")

        background_tasks.create(_actualizar(), name="guardar_precio_popup")

    with d:
        with ui.card().classes("p-4 min-w-[400px] max-w-[540px]"):
            with ui.row().classes("w-full gap-3 mb-2"):
                thumb_url = row.get("thumbnail") or ""
                if thumb_url:
                    ui.image(thumb_url).classes("w-16 h-16 object-contain rounded border").style("min-width: 64px; min-height: 64px;")
                else:
                    with ui.column().classes("w-16 h-16 rounded border bg-gray-100 items-center justify-center").style("min-width: 64px; min-height: 64px;"):
                        ui.label("Sin foto").classes("text-xs text-gray-500")
                with ui.column().classes("flex-1 min-w-0 gap-2"):
                    sku_txt = str(row.get("seller_sku") or row.get("id") or "")
                    ui.label(f"{row.get('id', '—')}  —  {sku_txt}").classes("text-sm font-mono text-gray-600")
                    ui.label(str(row.get("marca", "—"))).classes("text-sm font-medium")
                    txt = str(row.get("producto", ""))[:120] + ("..." if len(str(row.get("producto", ""))) > 120 else "")
                    ui.label(txt).classes("text-sm font-bold")
                    ui.label(f"Stock: {row.get('stock', '0')}").classes("text-sm text-gray-600")
            with ui.column().classes("w-full gap-0 border-b-2 border-gray-300 pb-3"):
                with ui.row().classes("w-full justify-between py-1 items-center"):
                    ui.label("FOB u$").classes("text-sm font-medium text-gray-600")
                    _fob_init = row.get("fob_usd")
                    if _fob_init is None:
                        _sku_dlg = str(row.get("seller_sku") or "").strip() or str(row.get("id") or "").strip()
                        _conn_tmp = get_connection()
                        try:
                            _r_dlg = _conn_tmp.execute(
                                "SELECT fob_usd FROM productos WHERE sku = ? AND user_id = ?",
                                (_sku_dlg, uid),
                            ).fetchone()
                            if _r_dlg and _r_dlg["fob_usd"] is not None:
                                _fob_init = _r_dlg["fob_usd"]
                        finally:
                            _conn_tmp.close()
                    _fob_str_dlg = f"{_fob_init:.2f}" if _fob_init is not None else ""
                    inp_fob_dlg  = ui.input(value=_fob_str_dlg).classes("text-sm w-24").props("dense type=number min=0 step=0.01")
                    inp_refs["fob_usd"] = inp_fob_dlg
                with ui.row().classes("w-full justify-between py-1 items-center"):
                    ui.label("Precio").classes("text-sm font-medium text-gray-600")
                    inp_precio = ui.input(value=fmt_moneda(row.get("precio")), on_change=lambda _: _recalcular()).classes("text-sm w-32").props("dense")
                    inp_refs["precio"] = inp_precio
                with ui.row().classes("w-full justify-between py-1 items-center"):
                    ui.label("Tipo IVA").classes("text-sm font-medium text-gray-600")
                    tipo_val = float(row.get("tipo_iva") or 0.105)
                    sel_iva  = ui.select({"0.105": "10,5%", "0.21": "21%"}, value=("0.21" if abs(tipo_val - 0.21) < 0.001 else "0.105"), on_change=lambda _: _recalcular()).classes("text-sm w-24").props("dense")
                    inp_refs["tipo_iva"] = sel_iva
                with ui.row().classes("w-full justify-between py-1 items-center gap-4 border-b-2 border-gray-300"):
                    with ui.row().classes("items-center gap-2"):
                        ui.label("Costo +IVA u$").classes("text-sm font-medium text-gray-600")
                        _cv = row.get("costo")
                        ui.label(fmt_usd(_cv) if _cv is not None else "u$0,00").classes("text-sm")
                    with ui.row().classes("items-center gap-2"):
                        ui.label("Costo $").classes("text-sm font-medium text-gray-600")
                        ui.label(fmt_moneda(float(row.get("costo") or 0) * dolar_oficial)).classes("text-sm")
                with ui.row().classes("w-full py-1 gap-4 border-b-2 border-gray-300 flex-wrap"):
                    for lbl_p, key_p, fmt_p in [
                        ("Cuotas",          "cuotas",         lambda v: str(v or "x1")),
                        ("Promo ML",        "promo_ml_pct",   lambda v: f"{v:.1f}%" if v is not None else "—"),
                        ("Promo Yo %",      "promo_yo_pct",   lambda v: f"{v:.1f}%" if v is not None else "—"),
                        ("Precio Original", "price_original", lambda v: fmt_moneda(v) if v is not None else "—"),
                        ("Precio Promo",    "price_promo",    lambda v: fmt_moneda(v) if v is not None else "—"),
                    ]:
                        with ui.column().classes("gap-0"):
                            ui.label(lbl_p).classes("text-xs text-gray-600")
                            ui.label(fmt_p(row.get(key_p))).classes("text-sm font-medium")
                    with ui.column().classes("gap-0"):
                        ui.label("Promo Yo $").classes("text-xs text-gray-600")
                        _pyo = row.get("promo_yo_pct"); _por = row.get("price_original")
                        ui.label(fmt_moneda(_por * _pyo / 100) if _por is not None and _pyo is not None else "—").classes("text-sm font-medium")
                recalc_ref["container"] = ui.column().classes("w-full gap-0 pt-3")
            _recalcular()
            with ui.row().classes("w-full justify-end gap-2 mt-2"):
                ui.button("Cerrar",   on_click=lambda: d.close(),   color="secondary").props("flat")
                ui.button("Calcular", on_click=_recalcular,         color="secondary")
                ui.button("Guardar",  on_click=lambda: _guardar(d), color="primary")
    d.open()


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

    # Agrupación dinámica por SKU (misma lógica que _mostrar_tabla_cuotas).
    groups_sku: Dict[tuple, List[Dict[str, Any]]] = {}
    for i in items:
        groups_sku.setdefault(_cuotas_key(i), []).append(i)

    items_dedup: List[Dict[str, Any]] = []
    for grupo in groups_sku.values():
        if len(grupo) == 1:
            items_dedup.append(grupo[0])
            continue
        total_sold = sum(int(x.get("sold_quantity") or 0) for x in grupo)
        principal = max(
            grupo,
            key=lambda x: (
                1 if not x.get("catalog_listing") and
                     str(x.get("listing_type_id") or "").lower() == "gold_special" else 0,
                int(x.get("available_quantity") or 0),
            ),
        )
        fusionado = dict(principal)
        fusionado["sold_quantity"] = total_sold
        catalog_item = next((x for x in grupo if x.get("catalog_listing") is True), None)
        fusionado["catalog_item_id"] = catalog_item["id"] if catalog_item else None
        fusionado["catalog_product_id"] = catalog_item.get("catalog_product_id") if catalog_item else principal.get("catalog_product_id")
        items_dedup.append(fusionado)

    _uid = user["id"]
    _skus_dedup = [i.get("seller_sku") for i in items_dedup if i.get("seller_sku")]
    _prod_map: Dict[str, Dict[str, Any]] = {}
    if _skus_dedup:
        _conn_prod = get_connection()
        try:
            _cur_prod = _conn_prod.cursor()
            _ph = ",".join("?" * len(_skus_dedup))
            _cur_prod.execute(
                f"SELECT sku, costo_usd, fob_usd, tipo_iva, price_updated_at FROM productos WHERE user_id = ? AND sku IN ({_ph})",
                [_uid] + _skus_dedup,
            )
            for _r in _cur_prod.fetchall():
                _prod_map[_r["sku"]] = {
                    "costo_usd":        _r["costo_usd"],
                    "fob_usd":          _r["fob_usd"],
                    "tipo_iva":         _r["tipo_iva"],
                    "price_updated_at": _r["price_updated_at"],
                }
        finally:
            _conn_prod.close()

    dolar_str = get_cotizador_param("dolar_oficial", user["id"]) or COTIZADOR_DEFAULTS.get("dolar_oficial", "1475")
    dolar_oficial = float(str(dolar_str).replace(",", ".").strip()) if dolar_str else 1475.0
    if dolar_oficial <= 0:
        dolar_oficial = 1475.0

    def _parse_rate_p(s: Any) -> float:
        if s is None or s == "":
            return 0.0
        try:
            v = float(str(s).strip().replace(",", "."))
            return v if v <= 1.5 else v / 100.0
        except (ValueError, TypeError):
            return 0.0

    def _parse_float_p(s: Any) -> float:
        if s is None or s == "":
            return 0.0
        try:
            return float(str(s).replace(".", "").replace(",", ".").strip()) or 0.0
        except (ValueError, TypeError):
            return 0.0

    _uid_m       = user["id"]
    ml_comision_p  = _parse_rate_p(get_cotizador_param("ml_comision",         _uid_m) or COTIZADOR_DEFAULTS.get("ml_comision",         "0.15"))
    cuotas_3x_p    = _parse_rate_p(get_cotizador_param("cuotas_3x",           _uid_m) or COTIZADOR_DEFAULTS.get("cuotas_3x",           "0.094"))
    cuotas_6x_p    = _parse_rate_p(get_cotizador_param("cuotas_6x",           _uid_m) or COTIZADOR_DEFAULTS.get("cuotas_6x",           "0.151"))
    cuotas_9x_p    = _parse_rate_p(get_cotizador_param("cuotas_9x",           _uid_m) or COTIZADOR_DEFAULTS.get("cuotas_9x",           "0.207"))
    cuotas_12x_p   = _parse_rate_p(get_cotizador_param("cuotas_12x",          _uid_m) or COTIZADOR_DEFAULTS.get("cuotas_12x",          "0.259"))
    ml_iibb_per_p  = _parse_rate_p(get_cotizador_param("ml_iibb_per",         _uid_m) or COTIZADOR_DEFAULTS.get("ml_iibb_per",         "0.055"))
    ml_debcre_p    = _parse_rate_p(get_cotizador_param("ml_debcre",           _uid_m) or COTIZADOR_DEFAULTS.get("ml_debcre",           "0.006"))
    ml_envios_p    = _parse_float_p(get_cotizador_param("ml_envios",          _uid_m) or COTIZADOR_DEFAULTS.get("ml_envios",           "5823"))
    if ml_envios_p <= 100:
        ml_envios_p = 5823.0
    ml_envios_grat_p = _parse_float_p(get_cotizador_param("ml_envios_gratuitos", _uid_m) or COTIZADOR_DEFAULTS.get("ml_envios_gratuitos", "33000"))
    if ml_envios_grat_p <= 0:
        ml_envios_grat_p = 33000.0

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
        _item_sku = i.get("seller_sku") or None
        _prod_row = _prod_map.get(_item_sku) if _item_sku else None
        # Calcular Gan $ y Gan Vta%
        _costo_c = float(_prod_row["costo_usd"]) if _prod_row and _prod_row.get("costo_usd") is not None else 0.0
        _tiva_c  = float(_prod_row["tipo_iva"])  if _prod_row and _prod_row.get("tipo_iva")  is not None else 0.105
        _lt_c    = str(i.get("listing_type_id") or "").lower()
        _tasa_c  = cuotas_6x_p if _lt_c == "gold_pro" else 0.0
        _pc_c    = float(precio)
        if _costo_c > 0 and _pc_c > 0:
            _com_c  = _pc_c * ml_comision_p
            _cob_c  = _pc_c - _com_c
            _ivav_c = _pc_c * _tiva_c / (1 + _tiva_c)
            _ivam_c = _com_c * 0.21 / 1.21
            _ivai_c = 0.09 * _costo_c * dolar_oficial
            _ivat_c = _ivav_c - _ivam_c - _ivai_c
            _deb_c  = _pc_c * ml_debcre_p
            _iibb_c = _pc_c * ml_iibb_per_p
            _env_c  = ml_envios_p if _pc_c >= ml_envios_grat_p else 0.0
            _ccuot_c = _pc_c * _tasa_c if _tasa_c else 0.0
            _cp_c   = _costo_c * dolar_oficial
            _mgn_c  = _cob_c - _cp_c - _ivat_c - _iibb_c - _deb_c - _env_c - _ccuot_c
            _mvta_c = _mgn_c / _pc_c * 100
        else:
            _mgn_c  = None
            _mvta_c = None
        _price_upd_at = _prod_row["price_updated_at"] if _prod_row else None
        _dias_sin_modif: Optional[int] = None
        _hoy_dt = datetime.now().date()
        if _price_upd_at:
            try:
                _dias_sin_modif = (_hoy_dt - datetime.strptime(_price_upd_at[:10], "%Y-%m-%d").date()).days
            except Exception:
                pass
        if _dias_sin_modif is None and raw_fecha:
            try:
                _dias_sin_modif = (_hoy_dt - datetime.strptime(raw_fecha, "%Y-%m-%d").date()).days
            except Exception:
                pass
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
            "fecha_ult_modif": raw_fecha or "",
            "costo_usd": _prod_row["costo_usd"] if _prod_row else None,
            "fob_usd":   _prod_row["fob_usd"]   if _prod_row else None,
            "tipo_iva":  _prod_row["tipo_iva"]   if _prod_row else 0.105,
            "margen_pesos":     _mgn_c,
            "margen_venta_pct": _mvta_c,
            "catalog_status":       None,
            "catalog_price_to_win": None,
            "catalog_visit_share":  None,
            "catalog_reason":       None,
            "catalog_competitors":  None,
            "quality_score":        None,
            "quality_level":        None,
            "price_updated_at":     _price_upd_at,
            "dias_sin_modificar":   _dias_sin_modif,
        })

    publicaciones_totales = len(items_loaded)
    publicaciones_con_stock = sum(1 for i in items_loaded if (i.get("available_quantity") or 0) > 0)
    publicaciones_propias_con_stock = sum(1 for i in items_loaded if i.get("tipo") == "Propia" and (i.get("available_quantity") or 0) > 0)
    publicaciones_catalogo_con_stock = sum(1 for i in items_loaded if i.get("tipo") == "Catalogo" and (i.get("available_quantity") or 0) > 0)
    unidades_propias_en_stock = sum(i.get("available_quantity") or 0 for i in items_loaded if i.get("tipo") == "Propia")
    total_pesos_propias = sum(i.get("subtotal") or 0 for i in items_loaded if i.get("tipo") == "Propia")
    total_dolares_propias = (total_pesos_propias / dolar_oficial) if dolar_oficial else None

    _items_para_ptw = [
        r for r in items_loaded
        if (r.get("catalog_listing") is True or r.get("catalog_item_id") or bool(r.get("catalog_product_id")))
        and str(r.get("status") or "").lower() == "active"
        and str(r.get("catalog_item_id") or r.get("id") or "").strip()
    ]
    _cat_ids = list({str(r.get("catalog_item_id") or r.get("id") or "") for r in _items_para_ptw})
    if _cat_ids and access_token:
        def _fetch_catalog_pos(ids: List[str]) -> Dict[str, Optional[Dict]]:
            res: Dict[str, Optional[Dict]] = {}
            with ThreadPoolExecutor(max_workers=min(8, len(ids))) as ex:
                futures = {ex.submit(ml_get_item_price_to_win, access_token, iid): iid for iid in ids}
                for fut in as_completed(futures):
                    iid = futures[fut]
                    try:
                        res[iid] = fut.result()
                    except Exception:
                        res[iid] = None
            return res
        _cat_pos_map = _fetch_catalog_pos(_cat_ids)
        for r in items_loaded:
            _rid = str(r.get("catalog_item_id") or r.get("id") or "")
            if _rid in _cat_pos_map:
                d = _cat_pos_map[_rid] or {}
                r["catalog_status"]       = d.get("status")
                r["catalog_price_to_win"] = d.get("price_to_win")
                r["catalog_visit_share"]  = d.get("visit_share")
                r["catalog_reason"]       = d.get("reason")
                r["catalog_competitors"]  = d.get("competitors")

    _items_para_quality = [
        r for r in items_loaded
        if str(r.get("status") or "").lower() == "active"
        and str(r.get("id") or "").strip()
    ]
    _quality_ids = list({str(r["id"]) for r in _items_para_quality if r.get("id")})
    if _quality_ids and access_token:
        def _fetch_quality(ids: List[str]) -> Dict[str, Dict]:
            res: Dict[str, Dict] = {}
            with ThreadPoolExecutor(max_workers=min(8, len(ids))) as ex:
                futures = {ex.submit(ml_get_item_performance, access_token, iid): iid for iid in ids}
                for fut in as_completed(futures):
                    iid = futures[fut]
                    try:
                        res[iid] = fut.result()
                    except Exception:
                        res[iid] = {}
            return res
        _quality_map = _fetch_quality(_quality_ids)
        for r in items_loaded:
            _qid = str(r.get("id") or "")
            if _qid in _quality_map:
                d = _quality_map[_qid] or {}
                r["quality_score"] = d.get("score")
                r["quality_level"] = d.get("level")

    _hoy_str = datetime.now().strftime("%Y-%m-%d")
    _conn_rev_clean = get_connection()
    try:
        _fecha_limite_rev = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        _conn_rev_clean.execute("DELETE FROM revisiones_diarias WHERE fecha < ?", (_fecha_limite_rev,))
        _conn_rev_clean.commit()
    finally:
        _conn_rev_clean.close()

    revisiones_hoy: Dict[str, bool] = {}
    _conn_rev_init = get_connection()
    try:
        _rows_rev = _conn_rev_init.execute(
            "SELECT sku, precio_cambiado FROM revisiones_diarias WHERE user_id = ? AND fecha = ?",
            (_uid, _hoy_str),
        ).fetchall()
        revisiones_hoy = {r["sku"]: bool(r["precio_cambiado"]) for r in _rows_rev}
    finally:
        _conn_rev_init.close()

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
                            _sku_cel = str(row.get("seller_sku") or "").strip()
                            if _sku_cel:
                                def _save_rev_cel() -> None:
                                    _now = datetime.now()
                                    _conn_cel = get_connection()
                                    try:
                                        _conn_cel.execute(
                                            """INSERT INTO revisiones_diarias (sku, user_id, fecha, precio_cambiado) VALUES (?, ?, ?, 1)
                                               ON CONFLICT(sku, user_id, fecha) DO UPDATE SET precio_cambiado=1""",
                                            (_sku_cel, _uid, _now.strftime("%Y-%m-%d")),
                                        )
                                        _conn_cel.execute(
                                            "UPDATE productos SET price_updated_at=?, updated_at=? WHERE sku=? AND user_id=?",
                                            (_now.isoformat(), _now.isoformat(), _sku_cel, _uid),
                                        )
                                        _conn_cel.commit()
                                    finally:
                                        _conn_cel.close()
                                    revisiones_hoy[_sku_cel] = True
                                    row["dias_sin_modificar"] = 0
                                    row["price_updated_at"] = _now.isoformat()
                                await run.io_bound(_save_rev_cel)
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

    def abrir_editar_fob_costo(row: Dict[str, Any]) -> None:
        _sku = str(row.get("seller_sku") or "").strip() or str(row.get("id") or "").strip()
        _fob_cur = row.get("fob_usd")
        _costo_cur = row.get("costo_usd")
        if _fob_cur is None or _costo_cur is None:
            _conn_tmp = get_connection()
            try:
                _r = _conn_tmp.execute(
                    "SELECT fob_usd, costo_usd FROM productos WHERE sku = ? AND user_id = ?",
                    (_sku, user["id"]),
                ).fetchone()
                if _r:
                    if _fob_cur is None:
                        _fob_cur = _r["fob_usd"]
                    if _costo_cur is None:
                        _costo_cur = _r["costo_usd"]
            finally:
                _conn_tmp.close()
        dialog = ui.dialog()
        with dialog:
            with ui.card().classes("p-3 min-w-[280px] max-w-[320px]"):
                ui.label("Editar FOB y Costo").classes("text-sm font-semibold mb-1")
                ui.label((row.get("title") or "")[:80]).classes("text-xs text-gray-600 mb-1")
                inp_fob = ui.input("FOB u$", value=f"{_fob_cur:.2f}" if _fob_cur is not None else "").classes("w-full")
                inp_fob.props("type=number min=0 step=0.01 dense")
                inp_costo = ui.input("Costo u$ s/IVA", value=f"{_costo_cur:.2f}" if _costo_cur is not None else "").classes("w-full")
                inp_costo.props("type=number min=0 step=0.01 dense")

                def guardar() -> None:
                    fob_raw = (inp_fob.value or "").strip()
                    costo_raw = (inp_costo.value or "").strip()
                    try:
                        nuevo_fob = float(fob_raw) if fob_raw else None
                    except (TypeError, ValueError):
                        ui.notify("FOB inválido.", color="negative"); return
                    try:
                        nuevo_costo = float(costo_raw) if costo_raw else None
                    except (TypeError, ValueError):
                        ui.notify("Costo inválido.", color="negative"); return
                    if nuevo_fob is not None and nuevo_fob < 0:
                        ui.notify("El FOB no puede ser negativo.", color="negative"); return
                    if nuevo_costo is not None and nuevo_costo < 0:
                        ui.notify("El Costo no puede ser negativo.", color="negative"); return
                    now_str = datetime.now().isoformat()
                    try:
                        conn = get_connection()
                        conn.execute(
                            """INSERT INTO productos (sku, user_id, fob_usd, costo_usd, tipo_iva, created_at, updated_at)
                               VALUES (?, ?, ?, ?, ?, ?, ?)
                               ON CONFLICT(sku, user_id) DO UPDATE SET
                                   fob_usd=COALESCE(excluded.fob_usd, fob_usd),
                                   costo_usd=COALESCE(excluded.costo_usd, costo_usd),
                                   updated_at=excluded.updated_at""",
                            (_sku, user["id"], nuevo_fob, nuevo_costo, row.get("tipo_iva") or 0.105, now_str, now_str),
                        )
                        conn.commit()
                        conn.close()
                    except Exception as e:
                        ui.notify(f"Error: {e}", color="negative"); return
                    if nuevo_fob is not None:
                        row["fob_usd"] = nuevo_fob
                    if nuevo_costo is not None:
                        row["costo_usd"] = nuevo_costo
                    dialog.close()
                    filtrar_y_pintar()

                with ui.row().classes("w-full justify-end gap-2 mt-2"):
                    ui.button("Cancelar", on_click=lambda: dialog.close()).props("flat dense")
                    ui.button("Guardar", on_click=guardar, color="primary").props("dense")
        dialog.open()

    def abrir_editar_iva(row: Dict[str, Any]) -> None:
        _sku = str(row.get("seller_sku") or "").strip() or str(row.get("id") or "").strip()
        _iva_cur = row.get("tipo_iva") or 0.105
        _iva_str = "0.21" if abs(_iva_cur - 0.21) < 0.001 else "0.105"
        dialog = ui.dialog()
        with dialog:
            with ui.card().classes("p-4 min-w-[280px]"):
                ui.label("Editar tipo de IVA").classes("text-lg font-semibold mb-2")
                ui.label((row.get("title") or "")[:80]).classes("text-sm text-gray-600 mb-2")
                ui.label("Tipo de IVA para cálculo de margen").classes("text-xs text-gray-500 mb-2")
                sel_iva = ui.select({"0.105": "10,5%", "0.21": "21%"}, value=_iva_str).classes("w-full")

                def guardar() -> None:
                    nuevo_iva = float(sel_iva.value or "0.105")
                    now_str = datetime.now().isoformat()
                    try:
                        conn = get_connection()
                        conn.execute(
                            """INSERT INTO productos (sku, user_id, fob_usd, costo_usd, tipo_iva, created_at, updated_at)
                               VALUES (?, ?, NULL, NULL, ?, ?, ?)
                               ON CONFLICT(sku, user_id) DO UPDATE SET
                                   tipo_iva=excluded.tipo_iva,
                                   updated_at=excluded.updated_at""",
                            (_sku, user["id"], nuevo_iva, now_str, now_str),
                        )
                        conn.commit()
                        conn.close()
                    except Exception as e:
                        ui.notify(f"Error: {e}", color="negative"); return
                    row["tipo_iva"] = nuevo_iva
                    _pc_r    = float(row.get("price") or row.get("precio") or 0)
                    _costo_r = float(row.get("costo_usd") or 0)
                    _lt_r    = str(row.get("listing_type_id") or "").lower()
                    _tasa_r  = cuotas_6x_p if _lt_r == "gold_pro" else 0.0
                    if _costo_r > 0 and _pc_r > 0:
                        _com_r   = _pc_r * ml_comision_p
                        _cob_r   = _pc_r - _com_r
                        _ivav_r  = _pc_r * nuevo_iva / (1 + nuevo_iva)
                        _ivam_r  = _com_r * 0.21 / 1.21
                        _ivai_r  = 0.09 * _costo_r * dolar_oficial
                        _ivat_r  = _ivav_r - _ivam_r - _ivai_r
                        _deb_r   = _pc_r * ml_debcre_p
                        _iibb_r  = _pc_r * ml_iibb_per_p
                        _env_r   = ml_envios_p if _pc_r >= ml_envios_grat_p else 0.0
                        _ccuot_r = _pc_r * _tasa_r if _tasa_r else 0.0
                        _cp_r    = _costo_r * dolar_oficial
                        _mgn_r   = _cob_r - _cp_r - _ivat_r - _iibb_r - _deb_r - _env_r - _ccuot_r
                        row["margen_pesos"]     = _mgn_r
                        row["margen_venta_pct"] = (_mgn_r / _pc_r * 100) if _pc_r > 0 else 0.0
                    dialog.close()
                    filtrar_y_pintar()

                with ui.row().classes("w-full justify-end gap-2 mt-3"):
                    ui.button("Cancelar", on_click=lambda: dialog.close()).props("flat")
                    ui.button("Guardar", on_click=guardar, color="primary")
        dialog.open()

    def _on_costo_blur(evt, sku_key: str, inp, row: Dict) -> None:
        raw = (inp.value or "").strip()
        if not raw:
            return
        try:
            nuevo = float(raw)
        except (TypeError, ValueError):
            ui.notify("Costo inválido.", color="negative")
            return
        if nuevo < 0:
            ui.notify("El costo no puede ser negativo.", color="negative")
            return
        now_str = datetime.now().isoformat()
        try:
            conn = get_connection()
            conn.execute(
                """INSERT INTO productos (sku, user_id, costo_usd, tipo_iva, created_at, updated_at, costo_updated_at)
                   VALUES (?, ?, ?, 0.105, ?, ?, ?)
                   ON CONFLICT(sku, user_id) DO UPDATE SET
                       costo_usd=excluded.costo_usd,
                       costo_updated_at=excluded.costo_updated_at,
                       updated_at=excluded.updated_at""",
                (sku_key, user["id"], nuevo, now_str, now_str, now_str),
            )
            conn.commit()
            conn.close()
            row["costo_usd"] = nuevo
        except Exception as e:
            ui.notify(f"Error: {e}", color="negative")

    def _on_fob_blur(evt, sku_key: str, inp, row: Dict) -> None:
        raw = (inp.value or "").strip()
        if not raw:
            return
        try:
            nuevo = float(raw)
        except (TypeError, ValueError):
            ui.notify("FOB inválido.", color="negative")
            return
        if nuevo < 0:
            ui.notify("El FOB no puede ser negativo.", color="negative")
            return
        now_str = datetime.now().isoformat()
        try:
            conn = get_connection()
            conn.execute(
                """INSERT INTO productos (sku, user_id, fob_usd, tipo_iva, created_at, updated_at)
                   VALUES (?, ?, ?, 0.105, ?, ?)
                   ON CONFLICT(sku, user_id) DO UPDATE SET
                       fob_usd=excluded.fob_usd,
                       updated_at=excluded.updated_at""",
                (sku_key, user["id"], nuevo, now_str, now_str),
            )
            conn.commit()
            conn.close()
            row["fob_usd"] = nuevo
        except Exception as e:
            ui.notify(f"Error: {e}", color="negative")

    def _on_iva_change(e, sku_key: str, row: Dict) -> None:
        try:
            nuevo_iva = float(e.value)
        except (TypeError, ValueError):
            return
        now_str = datetime.now().isoformat()
        try:
            conn = get_connection()
            conn.execute(
                """INSERT INTO productos (sku, user_id, tipo_iva, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(sku, user_id) DO UPDATE SET
                       tipo_iva=excluded.tipo_iva,
                       updated_at=excluded.updated_at""",
                (sku_key, user["id"], nuevo_iva, now_str, now_str),
            )
            conn.commit()
            conn.close()
            row["tipo_iva"] = nuevo_iva
        except Exception as e:
            ui.notify(f"Error: {e}", color="negative")

    def _abrir_detalle_catalogo(row: Dict[str, Any]) -> None:
        _STATUS_MAP = {
            "winning":             ("✓", "Ganando",               "text-positive font-bold"),
            "sharing_first_place": ("=", "Compartiendo 1° lugar", "text-blue-600 font-bold"),
            "competing":           ("↓", "Compitiendo",           "text-orange-500 font-bold"),
            "listed":              ("—", "Publicado sin ganar",   "text-gray-500"),
        }
        _REASON_ES = {
            "PRICE":           "Precio",
            "QUALITY":         "Calidad de publicación",
            "REVIEWS":         "Reseñas",
            "SALES":           "Ventas históricas",
            "SHIPPING":        "Envío",
            "REPUTATION":      "Reputación del vendedor",
            "CATALOG_QUALITY": "Calidad del catálogo",
            "CATALOG_SCORE":   "Puntuación en catálogo",
        }
        cs      = row.get("catalog_status")
        ptw     = row.get("catalog_price_to_win")
        vs      = row.get("catalog_visit_share")
        comps   = row.get("catalog_competitors")
        reasons = row.get("catalog_reason") or []

        d = ui.dialog()
        with d:
            with ui.card().classes("p-6 min-w-[380px] max-w-[520px] gap-0"):
                with ui.row().classes("w-full gap-4 mb-3"):
                    thumb = row.get("thumbnail") or ""
                    if thumb:
                        ui.image(thumb).classes("w-20 h-20 object-contain rounded border").style("min-width:80px;min-height:80px;")
                    else:
                        with ui.column().classes("w-20 h-20 rounded border bg-gray-100 items-center justify-center").style("min-width:80px;min-height:80px;"):
                            ui.label("Sin foto").classes("text-xs text-gray-500")
                    with ui.column().classes("flex-1 min-w-0 gap-1"):
                        sku_txt = str(row.get("seller_sku") or row.get("id") or "")
                        ui.label(f"{row.get('id','—')}  —  {sku_txt}").classes("text-xs font-mono text-gray-500")
                        ui.label(str(row.get("marca") or "—")).classes("text-sm font-medium")
                        ui.label((str(row.get("title") or ""))[:100]).classes("text-sm font-bold")
                        ui.label(f"Stock: {row.get('available_quantity', 0)}").classes("text-sm text-gray-500")
                ui.separator()
                with ui.row().classes("w-full justify-between py-2"):
                    ui.label("Precio actual").classes("text-sm font-medium text-gray-600")
                    ui.label(fmt_moneda(row.get("price"))).classes("text-sm font-bold")
                with ui.row().classes("w-full justify-between py-2"):
                    ui.label("Posición").classes("text-sm font-medium text-gray-600")
                    if cs and cs in _STATUS_MAP:
                        ico, lbl, cls = _STATUS_MAP[cs]
                        ui.label(f"{ico}  {lbl}").classes(f"text-sm {cls}")
                    else:
                        ui.label("Sin datos").classes("text-sm text-gray-400")
                if ptw is not None:
                    with ui.row().classes("w-full justify-between py-2"):
                        ui.label("Precio sugerido para ganar").classes("text-sm font-medium text-gray-600")
                        ui.label(fmt_moneda(ptw)).classes("text-sm font-bold text-orange-600")
                if vs is not None:
                    with ui.row().classes("w-full justify-between py-2"):
                        ui.label("Visibilidad (visit share)").classes("text-sm font-medium text-gray-600")
                        try:
                            ui.label(f"{float(vs) * 100:.1f}%".replace(".", ",")).classes("text-sm")
                        except (TypeError, ValueError):
                            ui.label(str(vs)).classes("text-sm")
                if comps is not None:
                    with ui.row().classes("w-full justify-between py-2"):
                        ui.label("Competidores compartiendo 1°").classes("text-sm font-medium text-gray-600")
                        ui.label(str(comps)).classes("text-sm")
                if reasons:
                    ui.separator()
                    ui.label("Razones").classes("text-xs font-medium text-gray-500 mt-1")
                    for rsn in (reasons if isinstance(reasons, list) else [reasons]):
                        r_es = _REASON_ES.get(str(rsn).upper(), str(rsn))
                        ui.label(f"• {r_es}").classes("text-sm text-gray-700")
                ui.separator()
                with ui.row().classes("w-full justify-end mt-2"):
                    ui.button("Cerrar", on_click=d.close).props("flat").classes("text-gray-600")
        d.open()

    current_filtrados: List[Dict[str, Any]] = []
    current_table: List[Any] = []
    sort_col_ref: Dict[str, Any] = {"val": "title"}
    sort_asc_ref: Dict[str, bool] = {"val": True}

    def _on_detalle_click(row_base: Dict[str, Any]) -> None:
        cl = context.client

        async def _fetch_det():
            item_id = str(row_base.get("id", "")).strip()
            row = {
                "id":             row_base.get("id"),
                "seller_sku":     row_base.get("seller_sku"),
                "thumbnail":      row_base.get("thumbnail") or row_base.get("secure_thumbnail") or "",
                "marca":          row_base.get("marca"),
                "producto":       str(row_base.get("title") or ""),
                "stock":          row_base.get("available_quantity", 0),
                "precio":         float(row_base.get("price") or 0),
                "costo":          float(row_base.get("costo_usd") or 0),
                "tipo_iva":       float(row_base.get("tipo_iva") or 0.105),
                "cuotas":         "x1",
                "price_original": None, "promo_ml_pct": None,
                "promo_yo_pct":   None, "price_promo":  None,
            }
            if item_id and access_token:
                try:
                    with cl:
                        ui.notify("Cargando detalles...", color="info", timeout=1)
                    sp_data = await run.io_bound(ml_get_item_sale_price_full, access_token, item_id)
                    bodies  = await run.io_bound(ml_get_items_multiget_with_attributes, access_token, [item_id], "id,listing_type_id,attributes,sale_terms")
                    row["cuotas"] = str(_cuotas_desde_item(bodies[0]) if bodies and bodies[0] else "x1").strip().lower()
                    _sku = str(row.get("seller_sku") or "").strip() or item_id
                    _conn = get_connection()
                    try:
                        _cur = _conn.cursor()
                        _cur.execute("SELECT costo_usd, tipo_iva FROM productos WHERE sku = ? AND user_id = ?", (_sku, user["id"]))
                        _pr = _cur.fetchone()
                    finally:
                        _conn.close()
                    if _pr is not None:
                        row["costo"]    = float(_pr["costo_usd"] or 0)
                        row["tipo_iva"] = float(_pr["tipo_iva"] or 0.105)
                    if sp_data and sp_data.get("amount") is not None:
                        amt_f = float(sp_data["amount"])
                        row["precio"] = amt_f
                        reg = sp_data.get("regular_amount")
                        if reg is not None and float(reg) > 0 and abs(float(reg) - amt_f) > 0.01:
                            reg_f = float(reg)
                            pct   = (reg_f - amt_f) / reg_f * 100
                            row["price_original"] = reg_f
                            row["promo_ml_pct"]   = 0.0
                            row["promo_yo_pct"]   = pct
                            row["price_promo"]    = reg_f * (1 - pct / 100)
                except Exception:
                    pass
            with cl:
                _show_item_detail_dialog(
                    row,
                    ml_comision=ml_comision_p, cuotas_3x=cuotas_3x_p, cuotas_6x=cuotas_6x_p,
                    cuotas_9x=cuotas_9x_p, cuotas_12x=cuotas_12x_p,
                    ml_debcre=ml_debcre_p, ml_iibb_per=ml_iibb_per_p,
                    ml_envios=ml_envios_p, ml_envios_gratuitos=ml_envios_grat_p,
                    dolar_oficial=dolar_oficial, access_token=access_token,
                    uid=user["id"], items_loaded=items_loaded,
                    on_saved=filtrar_y_pintar,
                    revisiones_hoy=revisiones_hoy,
                )

        background_tasks.create(_fetch_det(), name="fetch_productos_detalle")

    def _sort_key_precios(row: Dict[str, Any], col_name: str) -> Any:
        """Devuelve valor para ordenar según el tipo de columna."""
        if col_name in ("price", "subtotal", "costo_usd", "margen_pesos", "margen_venta_pct", "quality_score"):
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

    def _generar_pdf_stock(filtrados_actuales: List[Dict[str, Any]]) -> Optional[str]:
        """Genera un PDF A4 con columnas Marca/Producto/Color/Stock, ordenado por Marca+Producto."""
        try:
            from reportlab.lib.pagesizes import A4
            from reportlab.platypus import SimpleDocTemplate, Table, TableStyle
            from reportlab.lib import colors as rl_colors
            from reportlab.lib.units import cm as rl_cm
            from reportlab.pdfgen import canvas as rl_canvas
        except ImportError:
            return None
        if not filtrados_actuales:
            return None

        rows_sorted = sorted(
            filtrados_actuales,
            key=lambda x: (
                (x.get("marca") or "").lower(),
                (x.get("title") or "").lower(),
            ),
        )

        ahora = datetime.now()
        _fecha = f"{ahora.day:02d}/{ahora.month:02d}/{ahora.year}"

        from reportlab.pdfbase.pdfmetrics import stringWidth as _sw
        _col_prod_pts = 11.5 * rl_cm - 12   # 326pt − 12pt padding lateral

        def _trunc(s):
            if not s or s == "—":
                return s or "—"
            if _sw(s, "Helvetica", 8) <= _col_prod_pts:
                return s
            while len(s) > 0 and _sw(s + "...", "Helvetica", 8) > _col_prod_pts:
                s = s[:-1]
            return (s + "...") if s else "..."

        headers = ["Marca", "Producto", "Color", "Stock"]
        data = [headers]
        for r in rows_sorted:
            stock_val = r.get("available_quantity")
            stock_str = fmt_miles(stock_val) if stock_val is not None else "0"
            data.append([
                str(r.get("marca") or "—"),
                _trunc(str(r.get("title") or "—")),
                str(r.get("color") or "—"),
                stock_str,
            ])

        tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
        tmp.close()

        page_w, page_h = A4
        margin    = 1.5 * rl_cm   # referencia Y del header
        margin_lr = 0.8 * rl_cm   # left/right documento + X header
        margin_b  = 0.5 * rl_cm   # bottom documento

        class _PaginatedCanvas(rl_canvas.Canvas):
            def __init__(self, *args, **kwargs):
                rl_canvas.Canvas.__init__(self, *args, **kwargs)
                self._saved_page_states = []

            def showPage(self):
                self._saved_page_states.append(dict(self.__dict__))
                self._startPage()

            def save(self):
                num_pages = len(self._saved_page_states)
                for state in self._saved_page_states:
                    self.__dict__.update(state)
                    self._draw_header(num_pages)
                    rl_canvas.Canvas.showPage(self)
                rl_canvas.Canvas.save(self)

            def _draw_header(self, page_count):
                self.saveState()
                self.setFont("Helvetica-Bold", 11)
                self.setFillColorRGB(0.098, 0.463, 0.824)
                self.drawString(margin_lr, page_h - margin + 4, f"Stock {_fecha}")
                self.setFont("Helvetica", 9)
                self.setFillColorRGB(0.4, 0.4, 0.4)
                self.drawRightString(page_w - margin_lr, page_h - margin + 4,
                                     f"Página {self._pageNumber} de {page_count}")
                self.restoreState()

        doc = SimpleDocTemplate(
            tmp.name,
            pagesize=A4,
            leftMargin=margin_lr,
            rightMargin=margin_lr,
            topMargin=margin + 0.9 * rl_cm,
            bottomMargin=margin_b,
        )

        col_widths = [2.5 * rl_cm, 11.5 * rl_cm, 2.5 * rl_cm, 2.0 * rl_cm]

        table = Table(data, colWidths=col_widths, repeatRows=1)

        BLUE = rl_colors.HexColor("#1976d2")
        LIGHT_GRAY = rl_colors.HexColor("#f8f8f8")

        ts = TableStyle([
            ("BACKGROUND",   (0, 0), (-1,  0), BLUE),
            ("TEXTCOLOR",    (0, 0), (-1,  0), rl_colors.white),
            ("FONTNAME",     (0, 0), (-1,  0), "Helvetica-Bold"),
            ("FONTSIZE",     (0, 0), (-1,  0), 9),
            ("ALIGN",        (0, 0), (-1,  0), "CENTER"),
            ("VALIGN",       (0, 0), (-1, -1), "MIDDLE"),
            ("FONTNAME",     (0, 1), (-1, -1), "Helvetica"),
            ("FONTSIZE",     (0, 1), (-1, -1), 8),
            ("ALIGN",        (0, 1), ( 2, -1), "LEFT"),
            ("ALIGN",        (3, 1), ( 3, -1), "RIGHT"),
            ("GRID",         (0, 0), (-1, -1), 0.5, rl_colors.HexColor("#dddddd")),
            ("TOPPADDING",   (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING",(0, 0), (-1, -1), 3),
            ("ROWBACKGROUND",(0, 1), (-1, -1), [LIGHT_GRAY, rl_colors.white]),
        ])
        table.setStyle(ts)

        try:
            doc.build([table], canvasmaker=_PaginatedCanvas)
            return tmp.name
        except Exception:
            try:
                os.unlink(tmp.name)
            except Exception:
                pass
            return None

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
            path = await run.io_bound(_generar_pdf_stock, rows_to_print)
            if path:
                ahora = datetime.now()
                ts = f"{ahora.day:02d}-{ahora.month:02d}-{ahora.year % 100:02d}"
                with client:
                    ui.download(path, f"stock_{ts}.pdf")
                    def _borrar(p=path) -> None:
                        try:
                            if p and os.path.exists(p):
                                os.unlink(p)
                        except Exception:
                            pass
                    ui.timer(5.0, _borrar, once=True)
            else:
                with client:
                    ui.notify("No se pudo generar el PDF.", color="negative")

        background_tasks.create(_imprimir_async())

    header_style = "background-color: #1976d2; color: white; font-weight: 600;"
    fmt_num_js = "(val) => val != null && val !== '' ? Number(val).toLocaleString('de-DE').replace(/,/g, '.') : '0'"
    fmt_mon_js = "(val) => val != null && val !== '' ? '$' + Number(val).toLocaleString('de-DE').replace(/,/g, '.') : '$0'"
    columns_precios = [
        {"name": "seller_sku", "label": "SKU", "field": "seller_sku", "sortable": True, "align": "left", "headerStyle": header_style, "style": "min-width: 80px"},
        {"name": "marca", "label": "Marca", "field": "marca", "sortable": True, "align": "center", "headerStyle": header_style, "style": "min-width: 60px"},
        {"name": "title", "label": "Producto", "field": "title", "sortable": True, "align": "left", "headerStyle": header_style, "style": "min-width: 180px", ":classes": "(val, row) => (row && row.tipo === 'Propia') ? 'text-primary cursor-pointer' : ''", ":sort": "(a, b, rowA, rowB) => (String(rowA.title||'').toLowerCase()).localeCompare(String(rowB.title||'').toLowerCase(), 'en')"},
        {"name": "color", "label": "Color", "field": "color", "sortable": True, "align": "center", "headerStyle": header_style, "style": "min-width: 90px"},
        {"name": "fob_usd",   "label": "FOB u$",   "field": "fob_usd",   "sortable": True, "align": "right",  "headerStyle": header_style, "style": "min-width: 55px"},
        {"name": "costo_usd", "label": "Costo u$ s/IVA", "field": "costo_usd", "sortable": True, "align": "right",  "headerStyle": header_style, "style": "min-width: 80px"},
        {"name": "tipo_iva",   "label": "IVA",  "field": "tipo_iva",      "sortable": True, "align": "center", "headerStyle": header_style, "style": "min-width: 40px"},
        {"name": "quality_score", "label": "Calidad", "field": "quality_score", "sortable": True, "align": "center", "headerStyle": header_style, "style": "min-width: 38px"},
        {"name": "catalog_pos", "label": "Ganando", "field": "catalog_status", "sortable": True, "align": "center", "headerStyle": header_style, "style": "min-width: 55px"},
        {"name": "catalog_price_to_win", "label": "Precio Ganador", "field": "catalog_price_to_win", "sortable": True, "align": "right",  "headerStyle": header_style, "style": "min-width: 70px"},
        {"name": "price", "label": "Precio", "field": "price", "sortable": True, "align": "right", "headerStyle": header_style, ":format": fmt_mon_js, ":classes": "(val, row) => { let c = (row && row.tipo === 'Propia') ? 'text-primary cursor-pointer font-medium' : ''; const hasPromo = row && row.sale_price != null && Math.abs(Number(row.sale_price) - Number(row.price || 0)) > 0.01; return hasPromo ? c + ' line-through' : c; }"},
        {"name": "margen_pesos",     "label": "Gan $",  "field": "margen_pesos",     "sortable": True, "align": "right", "headerStyle": header_style, "style": "min-width: 65px"},
        {"name": "margen_venta_pct", "label": "Gan Vta%", "field": "margen_venta_pct", "sortable": True, "align": "right", "headerStyle": header_style, "style": "min-width: 50px"},
        {"name": "available_quantity", "label": "Stock", "field": "available_quantity", "sortable": True, "align": "center", "headerStyle": header_style, ":format": fmt_num_js},
        {"name": "sold_quantity", "label": "Ventas", "field": "sold_quantity", "sortable": True, "align": "center", "headerStyle": header_style, ":format": fmt_num_js},
        {"name": "subtotal", "label": "Subtotal", "field": "subtotal", "sortable": True, "align": "right", "headerStyle": header_style, ":format": fmt_mon_js},
        {"name": "dias_sin_modificar", "label": "Ult. Mod.", "field": "dias_sin_modificar", "sortable": True, "align": "center", "headerStyle": header_style, "style": "min-width: 38px"},
        {"name": "status", "label": "Estado", "field": "status", "sortable": True, "align": "center", "headerStyle": header_style, ":format": "(val) => (val || '').toLowerCase() === 'active' ? 'Activa' : 'Suspendida'"},
    ]

    def _build_colgroup_precios() -> None:
        _col_w = {
            "seller_sku": "95px", "marca": "60px", "title": "265px", "color": "50px",
            "fob_usd": "50px", "costo_usd": "60px", "tipo_iva": "40px",
            "quality_score": "55px", "catalog_pos": "55px",
            "catalog_price_to_win": "65px",
            "price": "65px", "margen_pesos": "60px", "margen_venta_pct": "50px",
            "available_quantity": "42px", "sold_quantity": "45px", "subtotal": "75px", "dias_sin_modificar": "38px", "status": "48px",
        }
        with ui.element("colgroup"):
            for col in columns_precios:
                ui.element("col").style(f"width:{_col_w.get(col['name'], '80px')}")

    def filtrar_y_pintar() -> None:
        filtrados = list(items_loaded)
        stock_val = getattr(filtro_stock, "value", "con_stock")
        if stock_val == "con_stock":
            filtrados = [x for x in filtrados if (x.get("available_quantity") or 0) > 0]
        elif stock_val == "sin_stock":
            filtrados = [x for x in filtrados if (x.get("available_quantity") or 0) == 0]
        estado_val = getattr(filtro_estado, "value", "todas")
        if estado_val == "activas":
            filtrados = [x for x in filtrados if str(x.get("status") or "").lower() == "active"]
        elif estado_val == "suspendidas":
            filtrados = [x for x in filtrados if str(x.get("status") or "").lower() != "active"]
        awei_val = getattr(filtro_awei, "value", "no_incluye")
        if awei_val == "no_incluye":
            filtrados = [x for x in filtrados if "awei" not in (x.get("marca") or "").lower()]
        ganando_val = getattr(filtro_ganando, "value", "todos")
        if ganando_val == "ganando":
            filtrados = [x for x in filtrados if x.get("catalog_status") == "winning"]
        elif ganando_val == "empatando":
            filtrados = [x for x in filtrados if x.get("catalog_status") == "sharing_first_place"]
        elif ganando_val == "perdiendo":
            filtrados = [x for x in filtrados if x.get("catalog_status") in ("competing", "listed")]
        sku_txt = (getattr(filtro_sku, "value", "") or "").strip().lower()
        if sku_txt:
            filtrados = [x for x in filtrados if sku_txt in (x.get("seller_sku") or "").lower() or sku_txt in (x.get("title") or "").lower()]
        rev_val = getattr(filtro_revision, "value", "todos")
        if rev_val != "todos":
            def _sku_rev_key(x):
                return str(x.get("seller_sku") or "").strip() or str(x.get("id") or "").strip()
            if rev_val == "pendientes":
                filtrados = [x for x in filtrados if _sku_rev_key(x) not in revisiones_hoy]
            elif rev_val == "revisados":
                filtrados = [x for x in filtrados if _sku_rev_key(x) in revisiones_hoy and not revisiones_hoy.get(_sku_rev_key(x), False)]
            elif rev_val == "precio_ok":
                filtrados = [x for x in filtrados if revisiones_hoy.get(_sku_rev_key(x), False)]
        col_sort = sort_col_ref.get("val", "title")
        asc = sort_asc_ref.get("val", True)
        filtrados = sorted(filtrados, key=lambda r: _sort_key_precios(r, col_sort), reverse=not asc)
        current_filtrados.clear()
        current_filtrados.extend(filtrados)

        lbl_totales.set_text(str(len(filtrados)))
        lbl_unidades.set_text(fmt_miles(sum(x.get("available_quantity") or 0 for x in filtrados if x.get("tipo") == "Propia")))
        _pesos = sum(x.get("subtotal") or 0 for x in filtrados if x.get("tipo") == "Propia")
        lbl_pesos.set_text(fmt_moneda(_pesos))
        lbl_usd.set_text(f"u$s {fmt_miles(int(round(_pesos / dolar_oficial)))}" if dolar_oficial else "—")
        lbl_marcas.set_text(str(len({
            str(x.get("marca") or "").strip()
            for x in filtrados
            if x.get("tipo") == "Propia"
            and str(x.get("marca") or "").strip()
            and str(x.get("marca") or "").strip() != "—"
        })))

        header_div_precios.clear()
        with header_div_precios:
            with ui.element("table").style("table-layout:fixed;width:100%;border-collapse:separate;border-spacing:0"):
                _build_colgroup_precios()
                with ui.element("thead"):
                    with ui.element("tr").classes("bg-primary text-white font-semibold"):
                        for col in columns_precios:
                            col_name = col.get("name", col.get("field", ""))
                            sortable  = col.get("sortable", True)
                            align = "text-center"
                            with ui.element("th").classes(f"px-2 py-2 border {align}"):
                                if sortable:
                                    ui.button(col["label"], on_click=lambda c=col_name: _on_sort_click(c)).props("flat dense no-caps").classes("text-white hover:bg-white/20 cursor-pointer font-semibold")
                                else:
                                    ui.label(col["label"])
        table_container.clear()
        with table_container:
            with ui.element("table").style("table-layout:fixed;width:100%;border-collapse:separate;border-spacing:0"):
                _build_colgroup_precios()
                with ui.element("tbody"):
                    for row in filtrados:
                            _sku_key = str(row.get("seller_sku") or "").strip() or str(row.get("id") or "").strip()
                            _precio_ok_r = revisiones_hoy.get(_sku_key, False)
                            _revisado_r  = _sku_key in revisiones_hoy
                            if _precio_ok_r:
                                _row_bg = "bg-green-50"
                            elif _revisado_r:
                                _row_bg = "bg-yellow-50"
                            else:
                                _row_bg = ""
                            with ui.element("tr").classes(f"border-t border-gray-200 hover:bg-gray-50 {_row_bg}"):
                                for col in columns_precios:
                                    field = col.get("field", col["name"])
                                    val = row.get(field)
                                    if val is None:
                                        val = row.get(col["name"])
                                    if col["name"] == "title":
                                        align = "text-left"
                                    else:
                                        align = "text-right" if col.get("align") == "right" else "text-center" if col.get("align") == "center" else "text-left"
                                    if col["name"] == "title":
                                        _td_el = ui.element("td").classes(f"px-2 py-1 border-b border-gray-100 {align} text-xs").style("white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:0")
                                    elif col["name"] == "seller_sku":
                                        _td_el = ui.element("td").classes(f"px-2 py-1 border-b border-gray-100 {align} text-xs").style("white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:0")
                                    else:
                                        _td_el = ui.element("td").classes(f"px-2 py-1 border-b border-gray-100 {align} text-xs")
                                    with _td_el:
                                        if col["name"] == "fob_usd":
                                            _fob_val = row.get("fob_usd")
                                            _fob_str = f"{_fob_val:.2f}" if _fob_val is not None else "—"
                                            ui.button(_fob_str, on_click=lambda r=row: abrir_editar_fob_costo(r)).props("flat dense no-caps").classes("cursor-pointer text-xs font-medium text-primary hover:underline")
                                        elif col["name"] == "costo_usd":
                                            _costo_val = row.get("costo_usd")
                                            _costo_str = f"{_costo_val:.2f}" if _costo_val is not None else "—"
                                            ui.button(_costo_str, on_click=lambda r=row: abrir_editar_fob_costo(r)).props("flat dense no-caps").classes("cursor-pointer text-xs font-medium text-primary hover:underline")
                                        elif col["name"] == "tipo_iva":
                                            _iva_val = row.get("tipo_iva") or 0.105
                                            _iva_lbl = "21%" if abs(_iva_val - 0.21) < 0.001 else "10,5%"
                                            ui.button(_iva_lbl, on_click=lambda r=row: abrir_editar_iva(r)).props("flat dense no-caps").classes("cursor-pointer text-xs font-medium text-primary hover:underline")
                                        elif col["name"] == "catalog_pos":
                                            cs = row.get("catalog_status")
                                            if cs == "winning":
                                                ui.label("Ganando").style("color:#27500A;font-size:11px;font-weight:500")
                                            elif cs == "sharing_first_place":
                                                ui.label("Empatando").style("color:#0C447C;font-size:11px;font-weight:500")
                                            elif cs == "competing":
                                                ui.label("Perdiendo").style("color:#791F1F;font-size:11px;font-weight:500")
                                            elif cs == "listed":
                                                ui.label("Listed").style("color:var(--color-text-secondary);font-size:11px")
                                        elif col["name"] == "catalog_price_to_win":
                                            ptw = row.get("catalog_price_to_win")
                                            ui.label(fmt_moneda(ptw) if ptw is not None else "—").classes("" if ptw is not None else "text-gray-400")
                                        elif col["name"] == "title":
                                            _ttxt = str(val or "—")
                                            if row.get("tipo") in ("Propia", "Prop Comb"):
                                                ui.button(_ttxt[:80], on_click=lambda r=row: _on_detalle_click(r)).props("flat dense no-caps align=left").classes("text-left text-xs text-primary cursor-pointer hover:underline font-normal w-full")
                                            elif row.get("tipo") == "Catalogo":
                                                ui.button(_ttxt[:80], on_click=lambda r=row: _abrir_detalle_catalogo(r)).props("flat dense no-caps align=left").classes("text-left text-xs text-blue-700 cursor-pointer hover:underline font-normal w-full")
                                            else:
                                                ui.label(_ttxt[:80]).classes("text-left text-xs w-full")
                                        elif col["name"] == "price" and row.get("tipo") in ("Propia", "Prop Comb"):
                                            precio_str = fmt_moneda(val) if val is not None else "$0"
                                            ui.button(precio_str, on_click=lambda r=row: abrir_editar_precio(r)).props("flat dense no-caps").classes("cursor-pointer text-xs font-medium text-primary hover:underline")
                                        elif col["name"] == "price":
                                            ui.label(fmt_moneda(val) if val is not None else "$0")
                                        elif col["name"] == "margen_pesos":
                                            v = row.get("margen_pesos")
                                            if v is None:
                                                ui.label("—").classes("text-gray-400 text-xs")
                                            else:
                                                ui.label(fmt_moneda(v)).classes("font-medium " + ("text-positive" if v > 0 else "text-negative"))
                                        elif col["name"] == "margen_venta_pct":
                                            v = row.get("margen_venta_pct")
                                            if v is None:
                                                ui.label("—").classes("text-gray-400 text-xs")
                                            else:
                                                ui.label(f"{v:.1f}%".replace(".", ",")).classes("font-medium " + ("text-positive" if v > 0 else "text-negative"))
                                        elif col["name"] in ("available_quantity", "sold_quantity"):
                                            ui.label(fmt_miles(val) if val is not None else "0").classes("text-center")
                                        elif col["name"] == "subtotal":
                                            ui.label(fmt_moneda(val) if val is not None else "$0")
                                        elif col["name"] == "seller_sku":
                                            ui.label(str(val) if val else "-").classes("text-xs")
                                        elif col["name"] == "status":
                                            s = str(val or "").lower()
                                            if s == "active":
                                                ui.label("Activa").classes("text-center")
                                            else:
                                                ui.label("Suspendida").classes("text-center text-red-500")
                                        elif col["name"] == "quality_score":
                                            qs = row.get("quality_score")
                                            if qs is None:
                                                ui.label("—").classes("text-gray-400 text-center w-full")
                                            else:
                                                qs_i = int(qs)
                                                _filled = round(qs_i / 20)
                                                if qs_i >= 65:
                                                    _cs, _cm, _ce, _cn = "#639922", "#C0DD97", "#EAF3DE", "#27500A"
                                                elif qs_i >= 50:
                                                    _cs, _cm, _ce, _cn = "#EF9F27", "#FAC775", "#FAEEDA", "#633806"
                                                else:
                                                    _cs, _cm, _ce, _cn = "#E24B4A", "#F09595", "#FCEBEB", "#791F1F"
                                                with ui.element("div").style("display:flex;align-items:center;gap:4px;width:100%"):
                                                    with ui.element("div").style("display:flex;gap:2px;flex:1"):
                                                        for _si in range(5):
                                                            if _si >= _filled:
                                                                _sc = _ce
                                                            elif _si == _filled - 1 and qs_i % 20 != 0:
                                                                _sc = _cm
                                                            else:
                                                                _sc = _cs
                                                            ui.element("div").style(f"height:8px;border-radius:1px;flex:1;background:{_sc}")
                                                    ui.label(str(qs_i)).style(f"font-size:11px;font-weight:500;color:{_cn};min-width:20px;text-align:right")
                                        elif col["name"] == "dias_sin_modificar":
                                            _dias = row.get("dias_sin_modificar")
                                            if _dias is None:
                                                ui.label("—").classes("text-gray-400 text-center")
                                            elif _dias == 0:
                                                ui.label("hoy").classes("text-positive font-medium text-center")
                                            elif _dias <= 7:
                                                ui.label(str(_dias)).classes("text-orange-500 font-medium text-center")
                                            else:
                                                ui.label(str(_dias)).classes("text-negative font-medium text-center")
                                        else:
                                            ui.label(str(val) if val is not None else "—")
            async def _recalc_padding() -> None:
                await ui.run_javascript(
                    f"(function(){{"
                    f"var body=document.getElementById('c{_cid_p}');"
                    f"var hdr=document.getElementById('c{_hid_p}');"
                    f"if(body&&hdr){{hdr.style.paddingRight=(body.offsetWidth-body.clientWidth)+'px';}}"
                    f"}})();"
                )
            background_tasks.create(_recalc_padding())
            current_table.clear()

    def _blanquear_revisiones():
        revisiones_hoy.clear()
        conn = get_connection()
        try:
            conn.execute(
                "DELETE FROM revisiones_diarias WHERE user_id=? AND fecha=date('now','localtime')",
                (_uid,),
            )
            conn.commit()
        finally:
            conn.close()
        filtrar_y_pintar()
        ui.notify("Revisiones del día borradas", color="info")

    with result_area:
        with ui.row().classes("w-full items-center gap-5 px-3 py-1 bg-grey-2 rounded mb-1"):
            with ui.row().classes("items-baseline gap-1"):
                ui.label("Publicaciones:").classes("text-xs text-gray-500")
                lbl_totales = ui.label("—").classes("text-sm font-bold text-primary")
            with ui.row().classes("items-baseline gap-1"):
                ui.label("Unidades:").classes("text-xs text-gray-500")
                lbl_unidades = ui.label("—").classes("text-sm font-bold text-primary")
            with ui.row().classes("items-baseline gap-1"):
                ui.label("Total $:").classes("text-xs text-gray-500")
                lbl_pesos = ui.label("—").classes("text-sm font-bold text-primary")
            with ui.row().classes("items-baseline gap-1"):
                ui.label("Total u$s:").classes("text-xs text-gray-500")
                lbl_usd = ui.label("—").classes("text-sm font-bold text-primary")
            with ui.row().classes("items-baseline gap-1"):
                ui.label("Marcas:").classes("text-xs text-gray-500")
                lbl_marcas = ui.label("—").classes("text-sm font-bold text-primary")
            ui.space()
            ui.button("Limpiar día", on_click=_blanquear_revisiones).props("icon=eraser dense flat no-caps color=warning").classes("text-xs").tooltip("Blanquear revisiones de hoy")
            if on_actualizar:
                ui.button("Actualizar", on_click=lambda: on_actualizar(), color="primary").props("icon=refresh dense flat no-caps").classes("text-xs")
            ui.button("Stock", on_click=lambda: imprimir_tabla(include_ventas=False), color="primary").props("icon=print dense flat no-caps").classes("text-xs")
            ui.button("Ventas", on_click=lambda: imprimir_tabla(include_ventas=True), color="primary").props("icon=print dense flat no-caps").classes("text-xs")
        with ui.row().classes("items-center gap-2 py-1 flex-wrap"):
            filtro_stock = ui.select(
                {"con_stock": "Con stock", "todas": "Todas", "sin_stock": "Sin stock"},
                value=filtro_stock_ref.get("val", "con_stock") if filtro_stock_ref else "con_stock",
                label="Stock",
            ).classes("w-32").props("outlined dense")
            filtro_estado = ui.select(
                {"activas": "Activas", "suspendidas": "Suspendidas", "todas": "Todas"},
                value="todas",
                label="Estado",
            ).classes("w-32").props("outlined dense")
            filtro_ganando = ui.select(
                {"todos": "Todos", "ganando": "Ganando", "empatando": "Empate 1ro", "perdiendo": "Perdiendo"},
                value="todos",
                label="Ganando",
            ).classes("w-36").props("outlined dense")
            filtro_awei = ui.select(
                {"incluye": "Incluye", "no_incluye": "No incluye"},
                value="no_incluye",
                label="Awei",
            ).classes("w-40").props("outlined dense")
            filtro_sku = ui.input(placeholder="SKU o Nombre...").props("outlined dense clearable").classes("w-56")
            filtro_revision = ui.select(
                {"todos": "Todos", "pendientes": "Sin revisar", "revisados": "Revisados", "precio_ok": "Precio cambiado"},
                value="todos",
                label="Revisión",
            ).classes("w-44").props("outlined dense")
        header_div_precios = ui.element("div").style("width:100%;overflow:hidden")
        table_container = ui.element("div").style("width:100%;height:65vh;overflow-y:scroll;overflow-x:auto")
        _hid_p = header_div_precios.id
        _cid_p = table_container.id
        async def _setup_sync_precios() -> None:
            await ui.run_javascript(
                f"(function(){{"
                f"var body=document.getElementById('c{_cid_p}');"
                f"var hdr=document.getElementById('c{_hid_p}');"
                f"if(!body||!hdr)return;"
                f"body.addEventListener('scroll',function(){{hdr.scrollLeft=body.scrollLeft;}});"
                f"function _sg(){{hdr.style.paddingRight=(body.offsetWidth-body.clientWidth)+'px';}}"
                f"_sg();new ResizeObserver(_sg).observe(body);"
                f"}})();"
            )
        background_tasks.create(_setup_sync_precios())

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
    filtro_estado.on_value_change(lambda *a: filtrar_y_pintar())
    filtro_awei.on_value_change(lambda *a: filtrar_y_pintar())
    filtro_ganando.on_value_change(lambda *a: filtrar_y_pintar())
    filtro_sku.on_value_change(lambda *a: filtrar_y_pintar())
    filtro_revision.on_value_change(lambda *a: filtrar_y_pintar())
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
    filtro_stock_ref: Dict[str, str] = {"val": "todas"}
    filtro_awei_ref: Dict[str, str] = {"val": "no_incluye"}
    filtro_ventas_ref: Dict[str, str] = {"val": "con_ventas"}
    include_paused_ref: Dict[str, bool] = {"val": True}  # Incluir pausadas para traer todos los productos
    vista_modo_ref: Dict[str, str] = {"val": "minimo"}
    sort_col_ref: Dict[str, str] = {"val": "ventas"}
    sort_asc_ref: Dict[str, bool] = {"val": False}
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
                        value=filtro_stock_ref.get("val", "todas"),
                        label="Stock",
                    ).classes("w-36")
                    filtro_awei = ui.select(
                        {"incluye": "Incluye", "no_incluye": "No incluye"},
                        value=filtro_awei_ref.get("val", "no_incluye"),
                        label="Awei",
                    ).classes("w-36")
                    filtro_ventas = ui.select(
                        {"con_ventas": "Con ventas", "sin_ventas": "Sin ventas", "todas": "Todas"},
                        value=filtro_ventas_ref.get("val", "con_ventas"),
                        label="Ventas",
                    ).classes("w-36")
                    btn_vista = ui.button("Completo" if vista_modo_ref.get("val") == "minimo" else "Mínimo", color="primary").props("icon=visibility")
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

            def on_ventas_change(e):
                filtro_ventas_ref["val"] = e.value
                _filtrar_con_indicador()

            filtro_fecha.on_value_change(on_fecha_change)
            filtro_stock.on_value_change(on_stock_change)
            filtro_awei.on_value_change(on_awei_change)
            filtro_ventas.on_value_change(on_ventas_change)
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
        ("seller_sku", "SKU", "left", True),
        ("id", "ID Meli", "left", False),
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
        ("seller_sku", "SKU", "left", True),
        ("id", "ID Meli", "left", False),
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
                _sku_dlg = str(row.get("seller_sku") or "").strip() or str(row.get("id") or "").strip()
                if _sku_dlg:
                    _conn_dlg = get_connection()
                    try:
                        _cur_dlg = _conn_dlg.cursor()
                        _cur_dlg.execute(
                            "SELECT costo_usd, tipo_iva FROM productos WHERE sku = ? AND user_id = ?",
                            (_sku_dlg, uid),
                        )
                        _prod_dlg = _cur_dlg.fetchone()
                    finally:
                        _conn_dlg.close()
                    if _prod_dlg is not None:
                        costo = float(_prod_dlg["costo_usd"] or 0)
                        tipo_iva = float(_prod_dlg["tipo_iva"] or 0.105)
                    else:
                        costo = 0.0
                        tipo_iva = 0.105
                else:
                    costo = 0.0
                    tipo_iva = 0.105
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
                _show_item_detail_dialog(
                    row,
                    ml_comision=ml_comision, cuotas_3x=cuotas_3x, cuotas_6x=cuotas_6x,
                    cuotas_9x=cuotas_9x, cuotas_12x=cuotas_12x,
                    ml_debcre=ml_debcre, ml_iibb_per=ml_iibb_per,
                    ml_envios=ml_envios, ml_envios_gratuitos=ml_envios_gratuitos,
                    dolar_oficial=dolar_oficial, access_token=access_token,
                    uid=uid, items_loaded=items_loaded,
                    on_saved=lambda: filtrar_y_pintar(),
                )

        background_tasks.create(_fetch_and_show(), name="fetch_row_details")


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
        ventas_val = filtro_ventas_ref.get("val", "con_ventas")
        if ventas_val == "con_ventas":
            filtrados = [x for x in filtrados if (x.get("ventas") or 0) > 0]
        elif ventas_val == "sin_ventas":
            filtrados = [x for x in filtrados if (x.get("ventas") or 0) == 0]
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
            with ui.element("div").classes("w-full").style("overflow: auto; max-height: calc(100vh - 320px);"):
                with ui.element("table").classes(tab_cls).style("table-layout: fixed; width: 100%; min-width: 100%" if es_completo else "width: max-content; min-width: 100%"):
                    with ui.element("thead").style("position: sticky; top: 0; z-index: 2;"):
                        with ui.element("tr").classes("bg-primary text-white font-semibold"):
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
                                        elif field == "seller_sku":
                                            ui.label(str(r.get("seller_sku") or r.get("id") or "—"))
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

        # Pre-fetch costos desde tabla productos para todos los SKUs del usuario
        _skus_precios = list({(str(i.get("seller_sku") or "").strip() or str(i.get("id") or "").strip()) for i in items_ordenados if (i.get("seller_sku") or i.get("id"))})
        _costos_sku_map: Dict[str, Dict[str, Any]] = {}
        if _skus_precios:
            _conn_prod = get_connection()
            try:
                _cur_prod = _conn_prod.cursor()
                _ph = ",".join("?" * len(_skus_precios))
                _cur_prod.execute(
                    f"SELECT sku, costo_usd, tipo_iva FROM productos WHERE user_id = ? AND sku IN ({_ph})",
                    [uid] + _skus_precios,
                )
                for _r in _cur_prod.fetchall():
                    _costos_sku_map[_r["sku"]] = {"costo_usd": _r["costo_usd"], "tipo_iva": _r["tipo_iva"]}
            finally:
                _conn_prod.close()

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
            _sku_key = seller_sku or str(i.get("id") or "").strip()
            _prod_row = _costos_sku_map.get(_sku_key) if _sku_key else None
            if _prod_row is not None:
                costo = float(_prod_row["costo_usd"] or 0)
                tipo_iva = float(_prod_row["tipo_iva"] or 0.105)
            else:
                costo = 0.0
                tipo_iva = 0.105
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
                "seller_sku": seller_sku,
                "thumbnail": i.get("thumbnail") or "",
                "marca": i.get("marca") or "—",
                "producto": str(i.get("title") or ""),
                "stock": stock,
                "ventas": ventas,
                "sold_quantity": int(i.get("sold_quantity") or 0),
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
                elif aid == "SELLER_SKU":
                    v = att.get("value_name") or att.get("value") or att.get("value_id")
                    if v is None and att.get("values"):
                        v = (att["values"][0] or {}).get("name") or (att["values"][0] or {}).get("value_name")
                    if v is not None:
                        seller_sku = str(v).strip()
                if marca and color and seller_sku:
                    break
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
                                                                                        nombre = _qb_invoice_pdf_download_basename(doc)
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
                                                                                nombre = _qb_invoice_pdf_download_basename(doc)
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

            # Tarjeta Asignación QuickBooks
            with ui.card().classes("w-full p-3 bg-grey-2"):
                ui.label("Asignación QuickBooks").classes("text-base font-semibold mb-2")
                ui.label("Asignar Customer QB a un usuario habilita automáticamente las tabs Invoices y Compras.").classes("text-xs text-gray-600 mb-3")

                _qb_assign_users = get_all_users()
                _qb_user_options = {str(u["id"]): u.get("username", str(u["id"])) for u in _qb_assign_users}
                _qb_sel_uid: Dict[str, Any] = {"val": None}
                _qb_current_label: Any = {"ref": None}
                _qb_customers_container: Any = {"ref": None}

                with ui.row().classes("items-center gap-3 flex-wrap mb-2"):
                    qb_user_select = ui.select(
                        options=_qb_user_options,
                        label="Seleccionar usuario...",
                        with_input=True,
                    ).props("use-input input-debounce=0 clearable").classes("min-w-[280px]")

                    lbl_current = ui.label("").classes("text-sm text-gray-700")
                    _qb_current_label["ref"] = lbl_current

                def _on_qb_user_select(e: Any) -> None:
                    uid_str = str(e.value) if e.value is not None else None
                    _qb_sel_uid["val"] = uid_str
                    if not uid_str:
                        _qb_current_label["ref"].text = ""
                        return
                    try:
                        uid_int = int(uid_str)
                    except (ValueError, TypeError):
                        return
                    cust = get_user_qb_customer(uid_int)
                    if cust:
                        _qb_current_label["ref"].text = f"Customer actual: {cust.get('name', '—')} (id {cust.get('id', '—')})"
                    else:
                        _qb_current_label["ref"].text = "Sin customer asignado"

                qb_user_select.on_value_change(_on_qb_user_select)

                qb_customers_container = ui.column().classes("w-full gap-1 mt-2")
                _qb_customers_container["ref"] = qb_customers_container

                def _buscar_customers_qb() -> None:
                    qb_customers_container.clear()
                    data, err = _qb_raw_query(user["id"], "SELECT Id, DisplayName, PrimaryEmailAddr FROM Customer MAXRESULTS 100")
                    if err or not data:
                        with qb_customers_container:
                            ui.label(f"Error: {err or 'Sin datos'}").classes("text-negative text-sm")
                        return
                    customers = (data.get("QueryResponse") or {}).get("Customer") or []
                    if not customers:
                        with qb_customers_container:
                            ui.label("No se encontraron customers en QB.").classes("text-sm text-gray-600")
                        return
                    with qb_customers_container:
                        ui.label(f"{len(customers)} customers encontrados. Click en un cliente para asignarlo al usuario seleccionado.").classes("text-xs text-gray-500 mb-1")
                        with ui.element("table").classes("border-collapse text-xs w-full"):
                            with ui.element("thead"):
                                with ui.element("tr").classes("bg-gray-100"):
                                    for col_h in ["ID", "Nombre", "Email"]:
                                        with ui.element("th").classes("px-2 py-1 border text-left"):
                                            ui.label(col_h)
                                    with ui.element("th").classes("px-2 py-1 border text-center"):
                                        ui.label("Asignar")
                            with ui.element("tbody"):
                                for c in customers:
                                    cid = str(c.get("Id", ""))
                                    cname = str(c.get("DisplayName") or c.get("FullyQualifiedName") or "—")
                                    cemail_obj = c.get("PrimaryEmailAddr") or {}
                                    cemail = str(cemail_obj.get("Address") or "—") if isinstance(cemail_obj, dict) else "—"
                                    with ui.element("tr").classes("border-t border-gray-200 hover:bg-blue-50"):
                                        with ui.element("td").classes("px-2 py-1 border-b border-gray-100"):
                                            ui.label(cid)
                                        with ui.element("td").classes("px-2 py-1 border-b border-gray-100"):
                                            ui.label(cname)
                                        with ui.element("td").classes("px-2 py-1 border-b border-gray-100"):
                                            ui.label(cemail)
                                        with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                            def _asignar(cid_inner=cid, cname_inner=cname) -> None:
                                                uid_str = _qb_sel_uid["val"]
                                                if not uid_str:
                                                    ui.notify("Seleccioná un usuario primero", color="warning")
                                                    return
                                                try:
                                                    uid_int = int(uid_str)
                                                except (ValueError, TypeError):
                                                    ui.notify("Usuario inválido", color="negative")
                                                    return
                                                set_user_qb_customer(uid_int, cid_inner, cname_inner)
                                                _enable_tabs_for_user(uid_int, TABS_QB)
                                                # Copiar credenciales QB del admin si el usuario no las tiene
                                                if uid_int != 1:
                                                    creds_admin = get_qb_app_credentials(1)
                                                    creds_usuario = get_qb_app_credentials(uid_int)
                                                    if creds_admin and not creds_usuario:
                                                        set_qb_app_credentials(uid_int, creds_admin["client_id"], creds_admin["client_secret"], creds_admin.get("redirect_uri"))
                                                _qb_current_label["ref"].text = f"Customer actual: {cname_inner} (id {cid_inner})"
                                                ui.notify(f"Asignado {cname_inner} → usuario {uid_str}. Tabs QB habilitadas.", color="positive")
                                            ui.button("Asignar", on_click=_asignar).props("flat dense no-caps").classes("text-xs text-blue-600")

                ui.button("Buscar clientes en QB", on_click=_buscar_customers_qb, color="primary").props("dense no-caps")

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

TABLA_IVA_VS_EXENTO_DEFAULT: List[Dict[str, Any]] = []


def _calc_courier_row(
    row: Dict[str, Any],
    params: Dict[str, float],
    posicion_by_name: Dict[str, Dict[str, float]],
    courier_by_origen: Dict[str, Dict[str, float]],
    origen_posicion: Dict[str, str],
    iva_vs_exento_by_courier: Optional[Dict[str, Dict[str, bool]]] = None,
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
    N = kg_real * peso_total * dolar_oficial  # Flete: dólar oficial
    O_val = almacenaje * peso_total * dolar_oficial
    P = res_3244 * dolar_oficial
    Q = seguro * dolar_oficial
    R = gas_ope * dolar_oficial
    S = env_dom * dolar_oficial  # Env Dom: dólar oficial
    # IVA FOB: (FOB + flete + seguro) × dolar_despacho × iva_rate; flete = Peso(total)×2.5; seguro = (FOB+flete)×0.01; CIF = FOB+flete+seguro
    monto_flete = peso_total * 2.5 if peso_total > 0 else 0  # Peso (columna total), no Peso U
    monto_seguro = (fob_total + monto_flete) * 0.01
    cif = fob_total + monto_flete + monto_seguro
    iva_fob_pesos = iva_rate * cif * dolar_despacho  # IVA FOB usa dólar despacho

    # IVA vs Exento: según Datos → IVA vs Exento, cada courier cobra IVA solo en los campos marcados (Origen = courier)
    def _iva_cobra(v: Any) -> bool:
        return v is True or v == "true" or (isinstance(v, str) and v.lower() == "true") or v == 1

    iva_cfg = None
    if iva_vs_exento_by_courier and origen:
        iva_cfg = iva_vs_exento_by_courier.get(origen)
        if not iva_cfg:
            for k, cfg in iva_vs_exento_by_courier.items():
                if origen in k or k in origen:
                    iva_cfg = cfg
                    break
    if iva_cfg is None:
        iva_cfg = {"almacenaje": True, "res_3244": True, "seguro": True, "gas_ope": True, "env_dom": True, "precio_con_iva": True}

    # Si Precio con IVA: IVA = monto - (monto / 1.21). Si no: IVA = monto × 0.21
    precio_con_iva = _iva_cobra(iva_cfg.get("precio_con_iva", True))

    def _calc_iva(monto: float) -> float:
        if monto <= 0:
            return 0
        if precio_con_iva:
            return monto - (monto / 1.21)
        return monto * 0.21

    iva_almacenaje = _calc_iva(O_val) if _iva_cobra(iva_cfg.get("almacenaje", True)) else 0
    iva_res_3244 = _calc_iva(P) if _iva_cobra(iva_cfg.get("res_3244", True)) else 0
    iva_seguro = _calc_iva(Q) if _iva_cobra(iva_cfg.get("seguro", True)) else 0
    iva_gas_ope = _calc_iva(R) if _iva_cobra(iva_cfg.get("gas_ope", True)) else 0
    iva_env_dom = _calc_iva(S) if _iva_cobra(iva_cfg.get("env_dom", True)) else 0
    total_iva_servicios = iva_almacenaje + iva_res_3244 + iva_seguro + iva_gas_ope + iva_env_dom
    T = (total_iva_servicios + iva_fob_pesos) * ajuste_ana
    subtotal_antes_ajuste = total_iva_servicios + iva_fob_pesos
    U = iibb * R
    V_raw = L + M + N + O_val + P + Q + R + S + T + U
    V = V_raw - total_iva_servicios if precio_con_iva else V_raw  # Si Precio con IVA: restar IVA servicios; si no, no restar
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
        "iva_lhs_detalle": {
            "lineas": [
                ["Almacenaje", O_val, iva_almacenaje, _iva_cobra(iva_cfg.get("almacenaje", True))],
                ["Res 3244", P, iva_res_3244, _iva_cobra(iva_cfg.get("res_3244", True))],
                ["Seguro", Q, iva_seguro, _iva_cobra(iva_cfg.get("seguro", True))],
                ["Gastos Operativos", R, iva_gas_ope, _iva_cobra(iva_cfg.get("gas_ope", True))],
                ["Envío a Domicilio", S, iva_env_dom, _iva_cobra(iva_cfg.get("env_dom", True))],
            ],
            "precio_con_iva": precio_con_iva,
            "total_iva_servicios": total_iva_servicios,
            "iva_fob": iva_fob_pesos,
            "iva_fob_calc": {"fob_total": fob_total, "monto_flete": monto_flete, "monto_seguro": monto_seguro, "cif": cif, "iva_rate": iva_rate, "dolar_despacho": dolar_despacho},
            "subtotal": subtotal_antes_ajuste,
            "ajuste": ajuste_ana,
            "total": T,
        },
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
        "margen_detalle": {
            "venta_ml": venta_ml,
            "comi_ml": comi_ml,
            "cobrado_ml": cobrado_ml,
            "costo_pesos": AC,
            "iva_total": iva_total,
            "deb_cred": deb_cred,
            "iibb_per": iibb_per,
            "envio": envio,
            "margen": margen,
        },
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
        headers_calc = ["FOB Tot", "Peso", "Derech", "Estad", "Flete", "Almac", "Res3244", "Seguro", "GasOp", "EnvDom", "IVA Total", "IIBB", "Courier", "Total", "Traída", "Costo$ s/iva", "Costo u$ s/iva", "3ctas", "6ctas", "MarkUp", "Cobrado", "Comision", "IVAImp", "IVAMel", "IVAVta", "IVA", "Deb/Cred", "IIBB+PER", "Envio", "Cos Vta", "Margen$", "MargVta", "MargCos"]
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
                                        if c == "iva_lhs":
                                            detalle = r.get("iva_lhs_detalle")

                                            def _abrir_popup_iva(det: Any) -> None:
                                                d = ui.dialog().props("persistent")
                                                with d:
                                                    with ui.card().classes("p-4 min-w-[360px]"):
                                                        ui.label("Cálculo IVA Total").classes("text-lg font-semibold mb-3")
                                                        if det:
                                                            def _fmt_mon(x: float) -> str:
                                                                s = f"{x:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")
                                                                return f"$ {s}"
                                                            def _fmt_usd(x: float) -> str:
                                                                s = f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                                                                return f"u$ {s}"
                                                            precio_con_iva_popup = det.get("precio_con_iva", True)
                                                            for linea in det.get("lineas", []):
                                                                concepto, monto_ivai, iva, aplica = linea[0], linea[1], linea[2], linea[3]
                                                                if aplica:
                                                                    if precio_con_iva_popup:
                                                                        ui.label(f"{concepto}: {_fmt_mon(monto_ivai)} IVA incl. → IVA = monto - (monto/1,21) = {_fmt_mon(iva)}").classes("text-sm")
                                                                    else:
                                                                        ui.label(f"{concepto}: {_fmt_mon(monto_ivai)} sin IVA → IVA = monto × 0,21 = {_fmt_mon(iva)}").classes("text-sm")
                                                                else:
                                                                    ui.label(f"{concepto}: Exento").classes("text-sm text-gray-500")
                                                            tot_serv = det.get("total_iva_servicios", 0)
                                                            ui.label(f"Total IVA servicios: {_fmt_mon(tot_serv)}").classes("text-sm font-medium mt-1")
                                                            ui.element("hr").classes("my-2 border-gray-300")
                                                            iva_fob_calc = det.get("iva_fob_calc") or {}
                                                            if iva_fob_calc:
                                                                fob = iva_fob_calc.get("fob_total", 0)
                                                                fl = iva_fob_calc.get("monto_flete", 0)
                                                                seg = iva_fob_calc.get("monto_seguro", 0)
                                                                cif_val = iva_fob_calc.get("cif", 0)
                                                                rate = iva_fob_calc.get("iva_rate", 0)
                                                                dol = iva_fob_calc.get("dolar_despacho", 0)
                                                                ui.label("IVA FOB:").classes("text-sm font-medium")
                                                                ui.label(f"  CIF = FOB + flete + seguro = {_fmt_usd(fob)} + {_fmt_usd(fl)} + {_fmt_usd(seg)} = {_fmt_usd(cif_val)}").classes("text-sm")
                                                                dol_str = f"{dol:,.0f}".replace(",", ".")
                                                                with ui.row().classes("gap-1"):
                                                                    ui.label("IVA FOB").classes("text-sm font-bold")
                                                                    ui.label(f" = {_fmt_usd(cif_val)} × {rate} × {dol_str} = ").classes("text-sm")
                                                                    ui.label(_fmt_mon(det.get('iva_fob', 0))).classes("text-sm font-bold")
                                                            else:
                                                                ui.label(f"IVA FOB: {_fmt_mon(det.get('iva_fob', 0))}").classes("text-sm")
                                                            with ui.row().classes("gap-2 mt-1"):
                                                                ui.label("Total IVA: IVA Total Servicios + IVA FOB =").classes("text-sm")
                                                                ui.label(_fmt_mon(det.get("total", 0))).classes("text-sm font-bold text-blue-600")
                                                        else:
                                                            ui.label("Recalculá para ver el detalle del IVA Total.").classes("text-sm text-gray-600")
                                                        ui.button("Cerrar", on_click=d.close).classes("mt-3")
                                                d.open()

                                            btn = ui.button(txt).classes("cursor-pointer underline hover:bg-gray-200 -m-1 px-1").props("flat dense no-caps no-wrap")
                                            btn.on_click(lambda det=detalle: _abrir_popup_iva(det))
                                        elif c == "margen":
                                            detalle_margen = r.get("margen_detalle")

                                            def _abrir_popup_margen(det: Any) -> None:
                                                d = ui.dialog().props("persistent")
                                                with d:
                                                    with ui.card().classes("p-4 min-w-[320px]"):
                                                        ui.label("Cálculo Margen").classes("text-lg font-semibold mb-3")
                                                        if det:
                                                            def _fmt_mon(x: float) -> str:
                                                                s = f"{x:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")
                                                                return f"$ {s}"
                                                            venta = det.get("venta_ml", 0)
                                                            comi = det.get("comi_ml", 0)
                                                            cob = det.get("cobrado_ml", 0)
                                                            costo = det.get("costo_pesos", 0)
                                                            iva = det.get("iva_total", 0)
                                                            deb = det.get("deb_cred", 0)
                                                            iibb = det.get("iibb_per", 0)
                                                            env = det.get("envio", 0)
                                                            marg = det.get("margen", 0)
                                                            with ui.row().classes("gap-2"):
                                                                ui.label("Venta:").classes("text-sm text-black")
                                                                ui.label(_fmt_mon(venta)).classes("text-sm text-blue-600")
                                                            with ui.row().classes("gap-2"):
                                                                ui.label("Comisiones:").classes("text-sm text-black")
                                                                ui.label(_fmt_mon(comi)).classes("text-sm text-negative")
                                                            with ui.row().classes("gap-2"):
                                                                ui.label("Cobrado:").classes("text-sm text-black")
                                                                ui.label(_fmt_mon(cob)).classes("text-sm text-blue-600")
                                                            with ui.row().classes("gap-2"):
                                                                ui.label("Costo sin iva:").classes("text-sm text-black")
                                                                ui.label(_fmt_mon(costo)).classes("text-sm text-negative")
                                                            with ui.row().classes("gap-2"):
                                                                ui.label("IVA:").classes("text-sm text-black")
                                                                ui.label(_fmt_mon(iva)).classes("text-sm text-negative")
                                                            with ui.row().classes("gap-2"):
                                                                ui.label("Deb/Cred:").classes("text-sm text-black")
                                                                ui.label(_fmt_mon(deb)).classes("text-sm text-negative")
                                                            with ui.row().classes("gap-2"):
                                                                ui.label("IIBB:").classes("text-sm text-black")
                                                                ui.label(_fmt_mon(iibb)).classes("text-sm text-negative")
                                                            with ui.row().classes("gap-2"):
                                                                ui.label("Envío:").classes("text-sm text-black")
                                                                ui.label(_fmt_mon(env)).classes("text-sm text-negative")
                                                            marg_cls = "text-positive" if marg >= 0 else "text-negative"
                                                            with ui.row().classes("gap-2 mt-2"):
                                                                ui.label("Margen:").classes("text-sm text-black font-bold")
                                                                ui.label(_fmt_mon(marg)).classes(f"text-sm font-bold {marg_cls}")
                                                        else:
                                                            ui.label("Recalculá para ver el detalle del margen.").classes("text-sm text-gray-600")
                                                        ui.button("Cerrar", on_click=d.close).classes("mt-3")
                                                d.open()

                                            marg_raw = r.get("margen_raw")
                                            btn_cls = "cursor-pointer underline hover:bg-gray-200 -m-1 px-1"
                                            if marg_raw is not None:
                                                btn_cls += " text-green-600" if marg_raw >= 0 else " text-red-600"
                                            btn_m = ui.button(txt).classes(btn_cls).props("flat dense no-caps no-wrap")
                                            btn_m.on_click(lambda det=detalle_margen: _abrir_popup_margen(det))
                                        else:
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

        def _parse_iva_bool(v: Any) -> bool:
            return v is True or v == "true" or (isinstance(v, str) and v.lower() == "true") or v == 1

        def recalcular() -> None:
            params_actual = {k: _parse_float(_get(k)) for k in COTIZADOR_DEFAULTS}
            posicion_actual = _get_tabla("posicion", TABLA_POSICION_DEFAULT)
            courier_actual = _get_tabla("courier", TABLA_COURIER_DEFAULT)
            origen_actual = _get_tabla("origen", TABLA_ORIGEN_DEFAULT)
            iva_vs_exento_actual = _get_tabla("iva_vs_exento", TABLA_IVA_VS_EXENTO_DEFAULT)
            posicion_by_name_actual = {str(r.get("posicion", "")).strip(): {c: _parse_float(r.get(c)) for c in ["seguro", "flete", "derechos", "estadisticas", "iva", "despachante", "cambio_pa"]} for r in posicion_actual if r.get("posicion")}
            courier_by_origen_actual = {str(r.get("courier", "")).strip(): {c: _parse_float(r.get(c)) for c in ["valor_kg", "descuento", "kg_real", "almacenaje", "seguro", "res_3244", "gas_ope", "env_dom", "iibb"]} for r in courier_actual if r.get("courier")}
            origen_posicion_actual = {str(r.get("origen", "")).strip(): str(r.get("posicion", "")).strip() for r in origen_actual if r.get("origen")}
            iva_vs_exento_by_courier_actual = {}
            for r in iva_vs_exento_actual:
                courier_nom = str(r.get("courier", "")).strip()
                if courier_nom:
                    iva_vs_exento_by_courier_actual[courier_nom] = {
                        "almacenaje": _parse_iva_bool(r.get("almacenaje", False)),
                        "res_3244": _parse_iva_bool(r.get("res_3244", False)),
                        "seguro": _parse_iva_bool(r.get("seguro", False)),
                        "gas_ope": _parse_iva_bool(r.get("gas_ope", False)),
                        "env_dom": _parse_iva_bool(r.get("env_dom", False)),
                        "precio_con_iva": _parse_iva_bool(r.get("precio_con_iva", True)),
                    }
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
                    calc = _calc_courier_row(row_data, params_actual, posicion_by_name_actual, courier_by_origen_actual, origen_posicion_actual, iva_vs_exento_by_courier_actual)
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

        # Tabla IVA vs Exento (debajo de Costos por Courier)
        tabla_iva_vs_exento_data = list(_get_tabla("iva_vs_exento", TABLA_IVA_VS_EXENTO_DEFAULT))
        iva_vs_exento_headers = ["Courier", "Almacenaje", "Res 3244", "Seguro", "Gastos Operativos", "Envio a Domicilio", "Precio con IVA"]

        def _parse_bool(v: Any) -> bool:
            if v is True or v == "true" or str(v).lower() == "true" or v == 1:
                return True
            return False

        with ui.card().classes("p-4 w-fit"):
            ui.label("IVA vs Exento").classes("text-lg font-semibold mb-3")
            iva_vs_exento_cont = ui.column().classes("w-full gap-2")
            iva_vs_exento_edit_rows: List[Dict[str, Any]] = []

            def repintar_iva() -> None:
                iva_vs_exento_cont.clear()
                iva_vs_exento_edit_rows.clear()
                with iva_vs_exento_cont:
                    with ui.element("table").classes("w-full border-collapse text-sm").style("table-layout: fixed;"):
                        with ui.element("thead"):
                            with ui.element("tr").classes("bg-blue-100 dark:bg-blue-900"):
                                for h in iva_vs_exento_headers:
                                    with ui.element("th").classes("font-semibold px-1.5 py-0.5 text-center border border-gray-300"):
                                        ui.label(h)
                                with ui.element("th").classes("font-semibold px-0.5 py-0.5 text-center border border-gray-300 text-xs").style("min-width: 52px; width: 52px;"):
                                    ui.label("Borrar")
                        with ui.element("tbody"):
                            for idx, row in enumerate(tabla_iva_vs_exento_data):
                                rinputs: Dict[str, Any] = {}
                                with ui.element("tr"):
                                    with ui.element("td").classes("p-0.5 border border-gray-200"):
                                        inp_courier = ui.input(value=str(row.get("courier", ""))).classes("w-full border-0 text-xs min-w-[100px]").props("dense")
                                        rinputs["courier"] = inp_courier
                                    for col in ["almacenaje", "res_3244", "seguro", "gas_ope", "env_dom", "precio_con_iva"]:
                                        with ui.element("td").classes("p-0.5 border border-gray-200 text-center"):
                                            default_val = True if col == "precio_con_iva" else False
                                            chk = ui.checkbox(value=_parse_bool(row.get(col, default_val)))
                                            rinputs[col] = chk
                                    with ui.element("td").classes("p-0.5 border border-gray-200 text-center").style("min-width: 52px; width: 52px;"):
                                        def borrar_iva(i: int) -> None:
                                            if 0 <= i < len(tabla_iva_vs_exento_data):
                                                for j, rinputs in enumerate(iva_vs_exento_edit_rows):
                                                    if j < len(tabla_iva_vs_exento_data):
                                                        tabla_iva_vs_exento_data[j] = {
                                                            "courier": str(rinputs["courier"].value or "").strip(),
                                                            "almacenaje": bool(rinputs["almacenaje"].value),
                                                            "res_3244": bool(rinputs["res_3244"].value),
                                                            "seguro": bool(rinputs["seguro"].value),
                                                            "gas_ope": bool(rinputs["gas_ope"].value),
                                                            "env_dom": bool(rinputs["env_dom"].value),
                                                            "precio_con_iva": bool(rinputs["precio_con_iva"].value),
                                                        }
                                                tabla_iva_vs_exento_data.pop(i)
                                                repintar_iva()
                                        ui.button("×", on_click=lambda i=idx: borrar_iva(i)).classes("text-red-600 font-bold text-sm min-w-0 px-1").props("flat dense no-caps")
                                iva_vs_exento_edit_rows.append(rinputs)

            repintar_iva()

            def agregar_fila_iva() -> None:
                # Sincronizar valores actuales de los inputs antes de repintar para no perder datos
                for i, rinputs in enumerate(iva_vs_exento_edit_rows):
                    if i < len(tabla_iva_vs_exento_data):
                        tabla_iva_vs_exento_data[i] = {
                            "courier": str(rinputs["courier"].value or "").strip(),
                            "almacenaje": bool(rinputs["almacenaje"].value),
                            "res_3244": bool(rinputs["res_3244"].value),
                            "seguro": bool(rinputs["seguro"].value),
                            "gas_ope": bool(rinputs["gas_ope"].value),
                            "env_dom": bool(rinputs["env_dom"].value),
                            "precio_con_iva": bool(rinputs["precio_con_iva"].value),
                        }
                tabla_iva_vs_exento_data.append({"courier": "", "almacenaje": False, "res_3244": False, "seguro": False, "gas_ope": False, "env_dom": False, "precio_con_iva": True})
                repintar_iva()

            def guardar_tabla_iva() -> None:
                new_data = []
                for rinputs in iva_vs_exento_edit_rows:
                    row: Dict[str, Any] = {
                        "courier": str(rinputs["courier"].value or "").strip(),
                        "almacenaje": bool(rinputs["almacenaje"].value),
                        "res_3244": bool(rinputs["res_3244"].value),
                        "seguro": bool(rinputs["seguro"].value),
                        "gas_ope": bool(rinputs["gas_ope"].value),
                        "env_dom": bool(rinputs["env_dom"].value),
                        "precio_con_iva": bool(rinputs["precio_con_iva"].value),
                    }
                    new_data.append(row)
                set_cotizador_tabla("iva_vs_exento", new_data, uid)
                tabla_iva_vs_exento_data.clear()
                tabla_iva_vs_exento_data.extend(new_data)
                repintar_iva()
                ui.notify("Tabla IVA vs Exento guardada", color="positive")

            with ui.row().classes("gap-2"):
                ui.button("Agregar Fila", on_click=agregar_fila_iva, color="primary")
                ui.button("Guardar tabla", on_click=guardar_tabla_iva, color="secondary")


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
        import fitz  # noqa: F401  # pymupdf — Invoices «Otra»
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
