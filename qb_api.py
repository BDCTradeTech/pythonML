from __future__ import annotations

import base64
import json
import logging
import os
import re
import sys
import unicodedata
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

from db import get_connection, get_qb_app_credentials, get_qb_tokens

# ==========================
# CONSTANTES PDF (Invoice patch)
# ==========================

_PDF_ROW_Y_TOL = 5.0
# Banda vertical por fila al buscar QTY/RATE/AMOUNT: bastante para 2–3 líneas de descripción,
# pero sin el exceso anterior (~100) que mezclaba columnas de filas vecinas.
_PDF_INVOICE_ROW_Y_SPAN = 40.0
# Separación mínima entre "filas de ítem" al agrupar rects de search_for (más que el interlineado ~11–12).
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


# ==========================
# QB TOKEN
# ==========================

def _refresh_qb_token_if_needed(user_id: int) -> Optional[str]:
    """Refresca el access_token QB si está por vencer (< 5 min) o si no hay expires_at.
    Actualiza qb_tokens en la BD con el expires_at calculado.
    Si el usuario usa el token del admin (fallback), guarda el refresh en user_id=1.
    Retorna el access_token vigente, o None si no hay tokens/credenciales configurados."""
    qb_creds = get_qb_app_credentials(user_id) or (get_qb_app_credentials(1) if user_id != 1 else None)
    qb_tokens = get_qb_tokens(user_id)
    if not qb_creds or not qb_tokens:
        return None
    # Determinar dónde guardar el token refrescado (propio o admin)
    conn_chk = get_connection()
    try:
        cur_chk = conn_chk.cursor()
        cur_chk.execute("SELECT id FROM qb_tokens WHERE user_id = ?", (user_id,))
        storage_user_id = user_id if cur_chk.fetchone() is not None else 1
    finally:
        conn_chk.close()
    access_token = qb_tokens["access_token"]
    refresh_token = qb_tokens.get("refresh_token")
    expires_at = qb_tokens.get("expires_at")
    needs_refresh = False
    if expires_at:
        try:
            exp = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc) if exp.tzinfo else datetime.now(timezone.utc).replace(tzinfo=None)
            if (exp - now).total_seconds() < 300:
                needs_refresh = True
        except Exception:
            needs_refresh = True
    elif refresh_token:
        needs_refresh = True
    if needs_refresh and refresh_token:
        auth_b64 = base64.b64encode(f"{qb_creds['client_id']}:{qb_creds['client_secret']}".encode()).decode()
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
                    expires_at_new = (datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=int(new_expires))).isoformat()
                conn = get_connection()
                try:
                    cur = conn.cursor()
                    cur.execute(
                        "UPDATE qb_tokens SET access_token=?, refresh_token=?, expires_at=?, raw_data=? WHERE user_id=?",
                        (new_token, new_refresh, expires_at_new, json.dumps(data, ensure_ascii=False), storage_user_id),
                    )
                    conn.commit()
                finally:
                    conn.close()
                access_token = new_token
        except Exception:
            pass
    return access_token


# ==========================
# QB API — QUERIES / RECURSOS
# ==========================

def fetch_qb_customers(user_id: int) -> tuple[List[Dict[str, str]], Optional[str]]:
    """
    Obtiene la lista de Customers de QuickBooks.
    Devuelve (lista, None) si OK, o ([], mensaje_error) si falla.
    """
    from urllib.parse import quote

    qb_tokens = get_qb_tokens(user_id)
    if not qb_tokens:
        return [], "Credenciales o tokens de QuickBooks no configurados"
    if not qb_tokens.get("realm_id"):
        return [], "Falta realm_id. Volvé a Conectar cuenta en Configuración."
    realm_id = qb_tokens["realm_id"]
    access_token = _refresh_qb_token_if_needed(user_id)
    if not access_token:
        return [], "Credenciales o tokens de QuickBooks no configurados"

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
    from urllib.parse import quote

    qb_tokens = get_qb_tokens(user_id)
    if not qb_tokens:
        return None, "Credenciales o tokens de QuickBooks no configurados"
    if not qb_tokens.get("realm_id"):
        return None, "Falta realm_id. Volvé a Conectar cuenta en Configuración."
    realm_id = qb_tokens["realm_id"]
    access_token = _refresh_qb_token_if_needed(user_id)
    if not access_token:
        return None, "Credenciales o tokens de QuickBooks no configurados"

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


def fetch_qb_item_by_id(user_id: int, item_id: str) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Obtiene un Item por Id (para leer Sku como en el PDF)."""
    item_id = str(item_id or "").strip()
    if not item_id:
        return None, "Sin item id"
    qb_tokens = get_qb_tokens(user_id)
    if not qb_tokens or not qb_tokens.get("realm_id"):
        return None, "Sin tokens o realm_id"
    base_url = "https://quickbooks.api.intuit.com"
    realm_id = qb_tokens["realm_id"]
    access_token = qb_tokens["access_token"]
    url = f"{base_url}/v3/company/{realm_id}/item/{item_id}"
    try:
        r = requests.get(url, headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"}, timeout=15)
        r.raise_for_status()
        data = r.json()
        return data.get("Item") or data.get("item"), None
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


# ==========================
# PDF INVOICE — HELPERS
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
    o_hyphen = re.sub(r"[‐‑‒–—−]", "-", o)
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


def patch_invoice_pdf_line_items(
    pdf_bytes: bytes,
    inv_obj: Dict[str, Any],
    new_description: str = "MOUSE",
    user_id: Optional[int] = None,
    prefer_table_rewrite: bool = True,
    sku_interleaved_display: bool = False,
) -> tuple[Optional[bytes], Optional[str]]:
    """Parchea líneas de venta: por defecto reescribe el bloque entero de la tabla desde la API.

    Si prefer_table_rewrite es False, usa el método anterior (búsqueda fila a fila).
    sku_interleaved_display: en el PDF solo se dibuja 1ª,3ª,5ª… letra del SKU (prueba Mouse/Smartwatch).
    """
    try:
        import fitz  # pymupdf
    except ImportError:
        py = sys.executable or "python3"
        return (
            None,
            f"Falta PyMuPDF. En el servidor: {py} -m pip install pymupdf "
            "(o activá el venv del proyecto y: pip install -r requirements.txt)",
        )

    specs = _invoice_line_patch_specs(inv_obj, user_id, {})
    if not specs:
        return None, "No hay líneas de ítem para parchear en el PDF"

    if prefer_table_rewrite:
        rw, rmsg = _patch_invoice_pdf_items_table_rewrite(
            pdf_bytes,
            inv_obj,
            new_description,
            user_id,
            sku_interleaved_display=sku_interleaved_display,
        )
        if rw is not None:
            return rw, rmsg

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        missed_lines: List[int] = []
        line_warnings: List[str] = []
        last_pno = 0
        last_y = 0.0
        desc_col_x0: Optional[float] = None
        # Fase 1: localizar todo sobre el PDF sin redactar (si no, al borrar fila 1 desaparece
        # el texto de las filas 2–N y search_for falla → "Sin fila completa" en cascada).
        plans: List[Optional[Dict[str, Any]]] = []

        for line_idx, spec in enumerate(specs):
            sku_val = str(spec.get("sku") or "").strip()
            aliases = list(spec.get("sku_aliases") or [])
            if not aliases and sku_val:
                aliases = [sku_val]
            sku_parts = _pdf_sku_all_search_strings(aliases)

            d_variants = _pdf_description_search_variants(spec["description"])
            _desc_skip = _pdf_duplicate_description_skip_count(specs, line_idx)
            _sku_skip = _pdf_duplicate_sku_skip_count(specs, line_idx)
            found_d = _pdf_find_rect_global_after_row_skip_occurrence(
                doc, d_variants, last_pno, last_y, _desc_skip, min_variant_len=3
            )
            if not found_d:
                found_d = _pdf_find_rect_global_after_row_skip_occurrence(
                    doc,
                    d_variants,
                    last_pno,
                    max(0.0, float(last_y) - 18.0),
                    _desc_skip,
                    min_variant_len=3,
                )
            anchor_used = False
            if not found_d:
                from_sku = _pdf_try_anchor_row_from_sku(
                    doc,
                    sku_parts,
                    last_pno,
                    last_y,
                    spec,
                    desc_col_x0,
                    sku_occurrence_skip=_sku_skip,
                )
                if from_sku:
                    found_d = from_sku
                    anchor_used = True
            if not found_d:
                missed_lines.append(line_idx + 1)
                if line_idx > 0 and last_pno < len(doc):
                    last_y = float(last_y) + 28.0
                plans.append(None)
                continue

            pno, d_rect = found_d
            page = doc[pno]
            if not anchor_used:
                dr0 = float(fitz.Rect(d_rect).x0)
                if desc_col_x0 is None:
                    desc_col_x0 = dr0
                else:
                    desc_col_x0 = min(desc_col_x0, dr0)
            y_ref = float(d_rect.y0)
            fields: List[tuple[Any, str, bool]] = []

            d_r = fitz.Rect(d_rect)

            loose_qty_x = max(float(d_rect.x1) + 18.0, float(d_rect.x0) + 125.0)
            row_top = float(d_rect.y0)
            if pno == last_pno and last_y > 0:
                y_band_lo = max(row_top - 4.0, last_y + 0.5)
            else:
                y_band_lo = row_top - 4.0
            d_bot = float(fitz.Rect(d_rect).y1)
            y_band_hi = max(
                y_band_lo + float(_PDF_INVOICE_ROW_Y_SPAN),
                d_bot + 14.0,
            )
            y_row_center = (y_band_lo + y_band_hi) * 0.5
            qty_yc: Optional[float] = None
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
                    float(d_rect.x0) + 158.0,
                    y_tol=13.0,
                )

            qty_x_max: Optional[float] = (
                float(fitz.Rect(qty_rect).x0) - 5.0 if qty_rect is not None else None
            )

            sku_union: Optional[Any] = None
            if sku_parts:
                sku_union = _pdf_find_sku_column_union(
                    page, d_rect, y_ref, sku_parts, x_max=qty_x_max
                )

            if not sku_union and sku_parts:
                if qty_rect:
                    q_r = fitz.Rect(qty_rect)
                    sku_union = _pdf_find_sku_column_union(
                        page,
                        d_rect,
                        y_ref,
                        sku_parts,
                        x_min=float(d_r.x1) - 8,
                        x_max=float(q_r.x0) + 12,
                    )
            if not sku_union and sku_parts:
                sku_union = _pdf_find_sku_column_union(
                    page,
                    d_rect,
                    y_ref,
                    sku_parts,
                    x_min=0.0,
                    x_max=qty_x_max if qty_x_max is not None else float(d_r.x0) - 0.5,
                )

            qx0 = float(fitz.Rect(qty_rect).x0) if qty_rect is not None else None
            desc_x_hi_cap: Optional[float] = None
            if qx0 is None:
                dcx = float(desc_col_x0) if desc_col_x0 is not None else float(d_rect.x0)
                desc_x_hi_cap = dcx + 300.0
            desc_y_max = float(y_band_hi) + 18.0
            if qty_rect is not None:
                desc_y_max = min(desc_y_max, float(fitz.Rect(qty_rect).y1) + 16.0)
            d_erase = _pdf_description_full_redact_rect(
                page,
                d_rect,
                qx0,
                extra_right_if_no_qty=52.0,
                y_max=desc_y_max,
                x_hi_cap=desc_x_hi_cap,
            )
            fields.append((d_erase, new_description, False))

            insert_sku = sku_val or (str(aliases[0]).strip() if aliases else "")
            if sku_interleaved_display and insert_sku:
                _raw_ins = insert_sku
                insert_sku = _sku_display_every_other_from_first(insert_sku)
                logging.info("PDF patch SKU interleaved: %r -> %r", _raw_ins, insert_sku)
            if sku_parts and insert_sku:
                if sku_union:
                    sku_red = _pdf_inflate_sku_for_redact(sku_union, page.rect)
                    fields.insert(0, (sku_red, insert_sku, True))
                else:
                    line_warnings.append(f"línea {line_idx + 1} SKU")
            qty_ins = _fmt_pdf_qty_for_insert(float(spec["qty"]))
            if qty_rect:
                fields.append((qty_rect, qty_ins, False))
                x_after = float(fitz.Rect(qty_rect).x1) - 1.0
            else:
                line_warnings.append(f"línea {line_idx + 1} qty")
                x_after = float(d_rect.x1) + 95.0

            if qty_rect:
                qc = fitz.Rect(qty_rect)
                yc = (float(qc.y0) + float(qc.y1)) * 0.5
                qty_yc = yc
                y_lo_ra = yc - 10.0
                y_hi_ra = yc + 10.0
            else:
                y_lo_ra, y_hi_ra = y_band_lo, y_band_hi

            _col_y_anchor = qty_yc if qty_yc is not None else y_row_center
            _same_row_y = 13.0 if qty_yc is not None else None

            rate_ins = _fmt_pdf_money_for_insert(float(spec["rate"]))
            rate_rect = _pdf_find_rect_right_band(
                page,
                y_lo_ra,
                y_hi_ra,
                _numeric_search_variants(spec["rate"]),
                x_after,
                y_prefer=_col_y_anchor,
                max_y_dist=_same_row_y,
            )
            if not rate_rect:
                rate_rect = _pdf_find_rect_row_after(
                    page,
                    _col_y_anchor,
                    _numeric_search_variants(spec["rate"]),
                    x_after,
                    y_tol=14.0,
                    max_y_dist=_same_row_y,
                )
            if rate_rect:
                fields.append((rate_rect, rate_ins, False))
                x_after = float(rate_rect.x1) - 1
            else:
                line_warnings.append(f"línea {line_idx + 1} rate")

            # Amount alineado a la misma fila que QTY (centro vertical _col_y_anchor)
            amt_rect = _pdf_find_rect_right_band(
                page,
                y_lo_ra,
                y_hi_ra,
                _numeric_search_variants(spec["amount"]),
                x_after,
                y_prefer=_col_y_anchor,
                max_y_dist=_same_row_y,
            )
            if not amt_rect:
                amt_rect = _pdf_find_rect_row_after(
                    page,
                    _col_y_anchor,
                    _numeric_search_variants(spec["amount"]),
                    x_after,
                    y_tol=15.0,
                    max_y_dist=_same_row_y,
                )
            amt_ins = _fmt_pdf_money_for_insert(float(spec["amount"]))
            if amt_rect:
                fields.append((amt_rect, amt_ins, False))
            else:
                line_warnings.append(f"línea {line_idx + 1} amount")

            desc_x_insert = float(d_rect.x0) - 2.0
            y_align_sku = float(d_rect.y0)
            if qty_rect is not None:
                qz = fitz.Rect(qty_rect)
                y_align_sku = min(y_align_sku, float(qz.y0) + 2.0)

            # last_y NO debe usar d_erase: el rect de descripción puede bajar varias líneas; si
            # max() incluye ese y1, last_y queda por debajo del y0 de la siguiente descripción y
            # search_for falla → "Sin fila completa" en ítems 3+ en fase 1.
            row_bt: List[float] = []
            if sku_union is not None:
                row_bt.append(float(fitz.Rect(sku_union).y1))
            if qty_rect is not None:
                row_bt.append(float(fitz.Rect(qty_rect).y1))
            if rate_rect is not None:
                row_bt.append(float(fitz.Rect(rate_rect).y1))
            if amt_rect is not None:
                row_bt.append(float(fitz.Rect(amt_rect).y1))
            if row_bt:
                row_bottom = max(row_bt)
            else:
                row_bottom = max(float(fitz.Rect(d_rect).y1), float(d_rect.y0) + 14.0)
            row_bottom = max(row_bottom, float(d_rect.y0) + 6.0)
            # Tope desde la 1ª línea de descripción: evita que un rate/amount mal asignado a la
            # fila siguiente inflen last_y y hagan fallar la búsqueda de los ítems 4+.
            _row_cap = float(d_rect.y0) + float(_PDF_INVOICE_ROW_Y_SPAN) + 36.0
            row_bottom = min(row_bottom, _row_cap)
            last_pno = pno
            last_y = row_bottom + 1.5

            plans.append(
                {
                    "pno": pno,
                    "fields": fields,
                    "new_description": new_description,
                    "desc_x_insert": desc_x_insert,
                    "y_align_sku": y_align_sku,
                }
            )

        replaced_lines = sum(1 for p in plans if p is not None)

        for plan in plans:
            if plan is None:
                continue
            page = doc[int(plan["pno"])]
            fields = plan["fields"]
            nd = str(plan["new_description"])
            desc_x_insert = float(plan["desc_x_insert"])
            y_align_sku = float(plan["y_align_sku"])
            by_x = sorted(fields, key=lambda t: float(fitz.Rect(t[0]).x0), reverse=True)
            for rect, _txt, _sku in by_x:
                page.add_redact_annot(fitz.Rect(rect))
            page.apply_redactions()
            for rect, txt, is_sku in sorted(fields, key=lambda t: float(fitz.Rect(t[0]).x0)):
                if is_sku:
                    _pdf_insert_sku_in_union(
                        page, fitz.Rect(rect), str(txt), y_row_align=y_align_sku
                    )
                elif str(txt) == nd:
                    _pdf_insert_black_text(page, fitz.Rect(rect), str(txt), min_x0=desc_x_insert)
                else:
                    _pdf_insert_black_text(page, fitz.Rect(rect), str(txt))

        if replaced_lines == 0:
            return None, "No se encontró ninguna descripción en el PDF (texto partido, plantilla distinta o escaneado)"
        warn_parts: List[str] = []
        if missed_lines:
            warn_parts.append(
                f"Sin fila completa: ítems {', '.join(map(str, missed_lines))}"
            )
        if line_warnings:
            warn_parts.append("Campos no encontrados: " + ", ".join(line_warnings[:12]))
            if len(line_warnings) > 12:
                warn_parts[-1] += "…"
        msg = "; ".join(warn_parts) if warn_parts else None
        return doc.tobytes(deflate=True), msg
    finally:
        doc.close()
