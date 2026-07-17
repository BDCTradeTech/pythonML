"""
Backfill de ventas: completa retroactivamente ventas de ML que no llegaron a
cargarse/enriquecerse en su momento por la pestaña Ventas (p.ej. porque el
usuario nunca abrió ese rango de fechas en la UI).

Replica EXACTAMENTE la misma lógica de "Completar datos" de tabs/ventas.py
(mismo armado de filas desde /orders/search, mismo cálculo de ganancia vía
MercadoPago charges_details, mismo guardado en ventas_datos) pero parametrizada
para poder correr sobre un rango de fechas arbitrario fuera de una sesión de UI.
"""
from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

import requests

from db import get_connection, get_cotizador_param
from ml_api import (
    _cuotas_desde_item,
    get_ml_access_token,
    ml_charge_neto,
    ml_clasificar_pago,
    ml_get_fixed_fee,
    ml_get_item_sale_price_full,
    ml_get_items_multiget_with_attributes,
    ml_get_orders,
    ml_get_user_id,
    ml_get_user_profile,
    ml_merge_payments,
)
from tabs.ventas import _get_flex_zona

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers de parseo / carga de parámetros (copiados de tabs/ventas.py)
# ---------------------------------------------------------------------------

def _vp_parse_rate(s) -> float:
    if s is None or s == "":
        return 0.0
    try:
        v = float(str(s).strip().replace(",", "."))
        return v if v <= 1.5 else v / 100.0
    except (ValueError, TypeError):
        return 0.0


def _vp_parse_float(s) -> float:
    if s is None or s == "":
        return 0.0
    try:
        return float(str(s).replace(".", "").replace(",", ".").strip() or 0)
    except (ValueError, TypeError):
        return 0.0


def _cargar_params_cotizador(user_id: int) -> Dict[str, Any]:
    return {
        "dolar_oficial": _vp_parse_float(get_cotizador_param("dolar_oficial", user_id) or "1475") or 1475.0,
        "ml_comision": _vp_parse_rate(get_cotizador_param("ml_comision", user_id) or "0.15"),
        "ml_debcre": _vp_parse_rate(get_cotizador_param("ml_debcre", user_id) or "0.006"),
        "ml_iibb_per": _vp_parse_rate(get_cotizador_param("ml_iibb_per", user_id) or "0.055"),
        "ml_envios": _vp_parse_float(get_cotizador_param("ml_envios", user_id) or "5823") or 5823.0,
        "ml_envios_gratuitos": _vp_parse_float(get_cotizador_param("ml_envios_gratuitos", user_id) or "33000") or 33000.0,
        "cuotas_3x": _vp_parse_rate(get_cotizador_param("cuotas_3x", user_id) or "0.094"),
        "cuotas_6x": _vp_parse_rate(get_cotizador_param("cuotas_6x", user_id) or "0.151"),
        "cuotas_9x": _vp_parse_rate(get_cotizador_param("cuotas_9x", user_id) or "0.207"),
        "cuotas_12x": _vp_parse_rate(get_cotizador_param("cuotas_12x", user_id) or "0.259"),
    }


def _cargar_costos_sku(user_id: int, skus: List[str]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    skus_ok = [s for s in skus if s]
    if not skus_ok:
        return out
    conn = get_connection()
    try:
        cur = conn.cursor()
        ph = ",".join("?" * len(skus_ok))
        cur.execute(
            f"SELECT sku, costo_usd, tipo_iva FROM productos WHERE user_id = ? AND sku IN ({ph})",
            [user_id] + skus_ok,
        )
        for r in cur.fetchall():
            out[r["sku"]] = {"costo_usd": float(r["costo_usd"] or 0), "tipo_iva": float(r["tipo_iva"] or 0.105)}
    finally:
        conn.close()
    return out


# ---------------------------------------------------------------------------
# Armado de filas desde /orders/search (copiado de _cargar_ventas_async)
# ---------------------------------------------------------------------------

def _tipo_oferta_desde_order_item(it: Dict, item_id: str, item_id_to_tipo_oferta: Dict[str, str]) -> tuple:
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


def _fetch_catalog_maps(access_token: str, item_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """Multiget de catalog_listing/cuotas/SKU + tipo_oferta (sale_price). Mismo mecanismo que _cargar_ventas_async."""
    item_id_to_catalog: Dict[str, bool] = {}
    item_id_to_sku: Dict[str, str] = {}
    item_id_to_cuotas: Dict[str, str] = {}
    item_id_to_tipo_oferta: Dict[str, str] = {}
    item_id_to_promo_display: Dict[str, str] = {}

    attrs = "id,catalog_listing,catalog_product_id,listing_type_id,attributes,sale_terms"
    for i in range(0, len(item_ids), 20):
        batch = item_ids[i:i + 20]
        bodies = ml_get_items_multiget_with_attributes(access_token, batch, attrs)
        for b in bodies:
            if b and isinstance(b, dict):
                iid = str(b.get("id", "") or b.get("item_id", "")).strip()
                if not iid:
                    continue
                cl = b.get("catalog_listing")
                item_id_to_catalog[iid] = cl is True or str(cl or "").lower() in ("true", "1")
                item_id_to_cuotas[iid] = _cuotas_desde_item(b)
                for a in (b.get("attributes") or []):
                    if isinstance(a, dict) and (a.get("id") or "").upper() == "SELLER_SKU":
                        sku_val = (a.get("value_name") or a.get("value") or "").strip()
                        if sku_val:
                            item_id_to_sku[iid] = sku_val
                        break

    if item_ids:
        max_workers = min(8, len(item_ids))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(ml_get_item_sale_price_full, access_token, iid): iid for iid in item_ids}
            for fut in futures:
                iid = futures[fut]
                try:
                    data = fut.result()
                    if data is not None:
                        amt = data.get("amount")
                        reg = data.get("regular_amount")
                        if reg is not None and amt is not None:
                            reg_f = float(reg)
                            amt_f = float(amt)
                            if abs(reg_f - amt_f) > 0.01:
                                item_id_to_tipo_oferta[iid] = "Promo"
                                pct = ((reg_f - amt_f) / reg_f * 100) if reg_f > 0 else 0
                                orig_fmt = f"$ {reg_f:,.0f}".replace(",", ".")
                                pct_str = f"{pct:.1f}".replace(".", ",")
                                item_id_to_promo_display[iid] = f"{orig_fmt} ({pct_str}% dto)"
                            else:
                                item_id_to_tipo_oferta[iid] = "Regular"
                        else:
                            item_id_to_tipo_oferta[iid] = "Regular"
                    else:
                        item_id_to_tipo_oferta[iid] = "Regular"
                except Exception:
                    item_id_to_tipo_oferta[iid] = "Regular"

    return {
        "catalog": item_id_to_catalog,
        "sku": item_id_to_sku,
        "cuotas": item_id_to_cuotas,
        "tipo_oferta": item_id_to_tipo_oferta,
        "promo_display": item_id_to_promo_display,
    }


def _construir_filas(orders: List[Dict], maps: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    item_id_to_catalog = maps["catalog"]
    item_id_to_sku = maps["sku"]
    item_id_to_cuotas = maps["cuotas"]
    item_id_to_tipo_oferta = maps["tipo_oferta"]
    item_id_to_promo_display = maps["promo_display"]
    status_map = {"paid": "Concretada", "handling": "En preparación", "shipped": "Enviada", "delivered": "Entregada", "cancelled": "Cancelada", "canceled": "Cancelada"}

    filas: List[Dict[str, Any]] = []
    for ord_item in orders:
        dt_str = ord_item.get("date_created") or ord_item.get("date_closed") or ord_item.get("date_last_updated") or ""
        if not dt_str or not isinstance(dt_str, str):
            continue
        try:
            try:
                dt = datetime.strptime(dt_str[:19], "%Y-%m-%dT%H:%M:%S")
            except (ValueError, TypeError):
                dt = datetime.strptime(dt_str[:10], "%Y-%m-%d")
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
        has_refund = bool(ord_item.get("refunds"))
        status_display = "Devolución" if has_refund else status_map.get(status_raw, status_raw or "—")
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
            titulo = (obj.get("title") if isinstance(obj, dict) else str(obj)) or it.get("title") or "—"
            item_id = (str(obj.get("id") or it.get("item_id") or "") if isinstance(obj, dict) else str(it.get("item_id") or "")).strip()
            cl = obj.get("catalog_listing") if isinstance(obj, dict) else it.get("catalog_listing")
            if cl is None and isinstance(obj, dict):
                cl = it.get("catalog_listing")
            catalog = cl is True or str(cl or "").lower() in ("true", "1")
            if (cl is None or not catalog) and item_id:
                catalog = item_id_to_catalog.get(item_id, False) or item_id_to_catalog.get(item_id.upper(), False) or item_id_to_catalog.get(item_id.lower(), False)
            tipo = "Catálogo" if catalog else "Propia"
            sku = item_id_to_sku.get(item_id) or item_id_to_sku.get(item_id.upper()) or item_id_to_sku.get(item_id.lower()) or ""
            cuotas = item_id_to_cuotas.get(item_id) or item_id_to_cuotas.get(item_id.upper()) or item_id_to_cuotas.get(item_id.lower()) or "x1"
            tipo_oferta, tipo_display = _tipo_oferta_desde_order_item(it, item_id, item_id_to_tipo_oferta)
            if tipo_display is None and (tipo_oferta or "").lower() == "promo":
                tipo_display = item_id_to_promo_display.get(item_id) or item_id_to_promo_display.get(item_id.upper() or "") or item_id_to_promo_display.get(item_id.lower() or "") or "Promo"
            _pays = ord_item.get("payments") or []
            _pays_ap = [p for p in _pays if str(p.get("status", "")).lower() == "approved"]
            _p_ap = (_pays_ap[0] if _pays_ap else None) or (_pays[0] if _pays else None)
            _payment_id = str(_p_ap.get("id") or "") if _p_ap else ""
            _payment_type = str(_p_ap.get("payment_type") or "") if _p_ap else ""
            _payment_ids_approved = [str(p.get("id")) for p in _pays_ap if p.get("id")]
            dt_v = dt + timedelta(hours=1)
            filas.append({
                "dt": dt_v,
                "tipo_venta": tipo, "cuotas": cuotas, "tipo_oferta": tipo_oferta, "tipo_display": tipo_display or tipo_oferta,
                "cantidad": qty, "status": status_display, "status_raw": status_raw,
                "item_id": item_id or "—",
                "unit_price": unit_price,
                "seller_sku": sku,
                "order_id": str(ord_item.get("id", "") or ""),
                "category_id": obj.get("category_id") or "",
                "listing_type_id": str(it.get("listing_type_id") or ""),
                "logistic_type": "",
                "shipping_id": str((ord_item.get("shipping") or {}).get("id") or ""),
                "payment_id": _payment_id,
                "payment_type": _payment_type,
                "payment_ids_approved": _payment_ids_approved,
                "sale_fee": float(it.get("sale_fee") or 0),
                "has_refund": has_refund,
                "titulo": titulo,
            })
    return filas


# ---------------------------------------------------------------------------
# Cálculo de ganancia (copiado verbatim de _compute en _enriquecer_ventas_async)
# ---------------------------------------------------------------------------

def _compute_venta(pay_data: Dict, v: Dict, zip_code: str, bonif_flex: float, params: Dict[str, Any], costos_sku: Dict[str, Dict[str, Any]], user_id: int, access_token: str, fixed_fee_cache: Dict[tuple, float]) -> Optional[Dict]:
    charges = pay_data.get("charges_details") or []
    estado = ml_clasificar_pago(
        pay_data,
        order_tiene_refund=bool(v.get("has_refund")),
        order_cancelada=(v.get("status_raw") or "") in ("cancelled", "canceled"),
    )
    is_rejected = estado == "rejected"
    if estado == "pendiente" and not charges:
        return None
    p = params
    unit_price = float(v.get("unit_price") or 0)
    cantidad = int(v.get("cantidad") or 1)
    total_price = unit_price * cantidad
    category_id = str(v.get("category_id") or "")
    listing_type_id = str(v.get("listing_type_id") or "")
    _fee_key = (round(unit_price), category_id, listing_type_id)
    if _fee_key not in fixed_fee_cache:
        fixed_fee_cache[_fee_key] = (
            ml_get_fixed_fee(access_token, unit_price, category_id, listing_type_id)
            if (category_id and listing_type_id) else 0.0
        )
    costo_fijo = fixed_fee_cache[_fee_key]
    sku = (v.get("seller_sku") or "").removeprefix("SKU ").strip()
    prod = costos_sku.get(sku) if sku else None
    costo_usd = float((prod or {}).get("costo_usd") or 0)
    tipo_iva = float((prod or {}).get("tipo_iva") or 0.105)
    dolar = float(p.get("dolar_oficial") or 1475)
    ml_iibb = float(p.get("ml_iibb_per") or 0.055)
    costo_pesos = costo_usd * dolar
    total_costo = costo_pesos * cantidad
    has_calc = total_price > 0 and costo_usd > 0
    meli_fee = ml_charge_neto(charges, name="meli_percentage_fee")
    cuotas_fee = ml_charge_neto(charges, name="financing_add_on_fee")
    deb_cred = ml_charge_neto(charges, contains="debitos_creditos")
    iibb_ret = ml_charge_neto(charges, contains="iibb")
    sirtac = ml_charge_neto(charges, contains="sirtac")
    net_rcv = (pay_data.get("transaction_details") or {}).get("net_received_amount")
    iibb_perc = total_price * ml_iibb
    iva_venta = total_price * tipo_iva / (1 + tipo_iva)
    iva_meli = meli_fee * 0.21 / 1.21
    iva_impor = 0.09 * costo_usd * dolar * cantidad
    iva_total = iva_venta - iva_meli - iva_impor
    shp_xd = ml_charge_neto(charges, name="shp_cross_docking")
    buyer_shipping = float(pay_data.get("shipping_amount") or 0)
    logistic_type = v.get("logistic_type") or ""
    if logistic_type in ("self_service", "flex"):
        _flex_t, _ = _get_flex_zona(user_id, zip_code)
        envio_real = _flex_t
        comprador_envio = 0.0
    elif shp_xd > 0:
        envio_real = max(0.0, shp_xd - buyer_shipping)
        comprador_envio = buyer_shipping
    else:
        envio_real = float(p.get("ml_envios") or 5823)
        comprador_envio = 0.0
    ml_env_grat_c = float(p.get("ml_envios_gratuitos") or 33000)
    envio_efectivo = 0.0 if unit_price < ml_env_grat_c else envio_real
    gan_pesos = gan_vta_pct = gan_cos_pct = None
    if estado in ("approved", "in_mediation") and has_calc:
        gan_pesos = total_price - meli_fee - cuotas_fee - iva_total - deb_cred - iibb_ret - sirtac - iibb_perc - envio_efectivo - total_costo + bonif_flex - costo_fijo
        gan_vta_pct = (gan_pesos / total_price * 100) if total_price > 0 else 0.0
        gan_cos_pct = (gan_pesos / total_costo * 100) if total_costo > 0 else 0.0
    elif estado == "refunded":
        gan_pesos = 0.0
        gan_vta_pct = 0.0
    elif estado == "charged_back" and has_calc:
        gan_pesos = -(total_costo + envio_efectivo + meli_fee + cuotas_fee)
        gan_vta_pct = (gan_pesos / total_price * 100) if total_price > 0 else 0.0
    _sale_fee_ml = float(v.get("sale_fee") or 0) * cantidad
    if _sale_fee_ml > 0 and estado in ("approved", "in_mediation"):
        _check_total = meli_fee + cuotas_fee + costo_fijo
        if abs(_check_total - _sale_fee_ml) > 1.0:
            log.warning(
                "[BACKFILL] _compute_venta: meli_fee+cuotas_fee+costo_fijo=%.2f no coincide con "
                "sale_fee*qty de ML=%.2f (diff=%.2f) order_id=%s payment_id=%s",
                _check_total, _sale_fee_ml, _check_total - _sale_fee_ml,
                v.get("order_id"), v.get("payment_id"),
            )
    return {
        "gan_pesos": gan_pesos, "gan_vta_pct": gan_vta_pct, "gan_cos_pct": gan_cos_pct,
        "meli_fee": meli_fee, "cuotas_fee": cuotas_fee, "iva_total": iva_total,
        "deb_cred": deb_cred, "iibb_ret": iibb_ret, "sirtac": sirtac,
        "envio_real": envio_real, "comprador_envio": comprador_envio,
        "logistic_type": logistic_type, "net_rcv": net_rcv,
        "costo_pesos": total_costo if (estado in ("approved", "in_mediation") and has_calc) else None,
        "costo_fijo": costo_fijo,
        "pay_status": None if estado == "approved" else estado,
        "_skip_overwrite": (
            (estado == "refunded" and pay_data.get("status") != "refunded")
            or (estado == "cancelled" and pay_data.get("status") != "cancelled")
        ),
    }


# ---------------------------------------------------------------------------
# Fetch con reintento ante rate limit (429)
# ---------------------------------------------------------------------------

def _get_with_retry(url: str, headers: Dict[str, str], timeout: int = 15, retries: int = 1) -> Dict:
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            if r.status_code == 429 and attempt < retries:
                log.warning("[BACKFILL] 429 rate limit en %s, esperando 30s...", url)
                time.sleep(30)
                continue
            return r.json() if r.status_code == 200 else {}
        except Exception:
            return {}
    return {}


# ---------------------------------------------------------------------------
# Persistencia (mismas queries que _save_batch / _save_placeholders / _insert_placeholders_bulk)
# ---------------------------------------------------------------------------

def _save_batch(db_rows: List[Dict]) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        for rd in db_rows:
            cols = ("payment_id, user_id, order_id, gan_pesos, gan_vta_pct, gan_cos_pct, "
                    "meli_fee, cuotas_fee, iva_total, deb_cred, iibb_ret, sirtac, "
                    "envio_real, comprador_envio, logistic_type, net_rcv, fetched_at, pay_status, order_date, cuotas, "
                    "costo_fijo")
            vals = (rd["payment_id"], rd["user_id"], rd.get("order_id"),
                     rd.get("gan_pesos"), rd.get("gan_vta_pct"), rd.get("gan_cos_pct"),
                     rd.get("meli_fee"), rd.get("cuotas_fee"), rd.get("iva_total"),
                     rd.get("deb_cred"), rd.get("iibb_ret"), rd.get("sirtac"),
                     rd.get("envio_real"), rd.get("comprador_envio"),
                     rd.get("logistic_type"), rd.get("net_rcv"),
                     rd.get("fetched_at"), rd.get("pay_status"), rd.get("order_date"),
                     rd.get("cuotas"), rd.get("costo_fijo"))
            verb = "INSERT OR IGNORE" if rd.get("_skip_overwrite") else "INSERT OR REPLACE"
            cur.execute(f"{verb} INTO ventas_datos ({cols}) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", vals)
            if rd.get("_skip_overwrite") and rd.get("pay_status"):
                cur.execute(
                    "UPDATE ventas_datos SET pay_status=? WHERE payment_id=? AND user_id=? AND pay_status IS NULL",
                    (rd["pay_status"], rd["payment_id"], rd["user_id"])
                )
        conn.commit()
    finally:
        conn.close()


def _save_placeholders(rows: List[Dict]) -> None:
    if not rows:
        return
    conn = get_connection()
    try:
        cur = conn.cursor()
        for rd in rows:
            cur.execute(
                "INSERT OR IGNORE INTO ventas_datos (payment_id, user_id, order_id, fetched_at, order_date, pay_status, costo_fijo) "
                "VALUES (?,?,?,?,?,?,?)",
                (rd["payment_id"], rd["user_id"], rd.get("order_id"), rd.get("fetched_at"), rd.get("order_date"), rd.get("pay_status"), 0.0),
            )
            if rd.get("pay_status"):
                cur.execute(
                    "UPDATE ventas_datos SET pay_status=? WHERE payment_id=? AND user_id=? AND pay_status IS NULL",
                    (rd["pay_status"], rd["payment_id"], rd["user_id"])
                )
        conn.commit()
    finally:
        conn.close()


def _insert_placeholders_bulk(rows: List[Dict], user_id: int) -> None:
    rows_ok = [r for r in rows if r.get("payment_id")]
    if not rows_ok:
        return
    conn = get_connection()
    try:
        cur = conn.cursor()
        now_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        for v in rows_ok:
            order_date = v["dt"].strftime("%Y-%m-%d") if v.get("dt") else None
            cur.execute(
                "INSERT OR IGNORE INTO ventas_datos (payment_id, user_id, order_id, fetched_at, order_date, costo_fijo) VALUES (?,?,?,?,?,?)",
                (v["payment_id"], user_id, v.get("order_id"), now_str, order_date, 0.0),
            )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Función principal
# ---------------------------------------------------------------------------

def backfill_ventas_periodo(
    user_id: int,
    fecha_desde: str,
    fecha_hasta: str,
    progress_callback: Optional[Callable[[int, int, int, int], None]] = None,
) -> Dict[str, Any]:
    """Completa (misma lógica que "Completar datos") todas las ventas ML del
    período [fecha_desde, fecha_hasta] (YYYY-MM-DD) que no estén ya completas
    en ventas_datos, sin depender de que la pestaña Ventas haya sido abierta
    para ese rango.

    progress_callback(procesadas, total_a_procesar, ya_completas, errores) se
    invoca luego de cada lote.

    Retorna un dict con el resumen: total_ventas, ya_completas, procesadas,
    errores, errores_detalle (lista de {payment_id, order_id, error}).
    """
    access_token = get_ml_access_token(user_id)
    if not access_token:
        raise RuntimeError(f"user_id={user_id} no tiene MercadoLibre vinculado")

    profile = ml_get_user_profile(access_token)
    seller_id = (profile or {}).get("id") or ml_get_user_id(access_token)
    if not seller_id:
        raise RuntimeError("No se pudo obtener el seller_id de MercadoLibre")

    date_from = f"{fecha_desde}T00:00:00.000-03:00"
    date_to = f"{fecha_hasta}T23:59:59.999-03:00"

    log.info("[BACKFILL] user=%s buscando ordenes %s -> %s", user_id, fecha_desde, fecha_hasta)
    orders_data = ml_get_orders(access_token, str(seller_id), limit=200000, offset=0, date_from=date_from, date_to=date_to)
    raw_orders = orders_data.get("results") or orders_data.get("orders") or orders_data.get("elements") or []
    orders = [o for o in raw_orders if isinstance(o, dict)]
    log.info("[BACKFILL] user=%s %d ordenes encontradas en el periodo", user_id, len(orders))

    item_ids: List[str] = []
    for o in orders:
        for it in (o.get("order_items") or o.get("items") or []):
            if isinstance(it, dict):
                obj = it.get("item") or it
                iid = (str(obj.get("id") or it.get("item_id") or "").strip() if isinstance(obj, dict) else str(it.get("item_id") or "").strip())
                if iid and iid not in item_ids:
                    item_ids.append(iid)

    maps = _fetch_catalog_maps(access_token, item_ids)
    filas = _construir_filas(orders, maps)
    total = len(filas)

    all_skus = [s for s in maps["sku"].values() if s]
    params = _cargar_params_cotizador(user_id)
    costos_sku = _cargar_costos_sku(user_id, all_skus)
    _fixed_fee_cache: Dict[tuple, float] = {}

    conn = get_connection()
    try:
        existing = {r["payment_id"]: r["gan_pesos"] for r in conn.execute(
            "SELECT payment_id, gan_pesos FROM ventas_datos WHERE user_id=?", (user_id,)
        ).fetchall()}
    finally:
        conn.close()

    ya_completas = 0
    rows_to_process: List[Dict] = []
    for v in filas:
        pid = v.get("payment_id") or ""
        if not pid:
            # Sin payment_id (p.ej. orden sin pago aun): igual que "Completar datos", no se procesa.
            continue
        if existing.get(pid) is not None:
            ya_completas += 1
            continue
        rows_to_process.append(v)

    _insert_placeholders_bulk(rows_to_process, user_id)

    procesadas = 0
    errores: List[Dict[str, Any]] = []

    def _fetch_one(pids: List[str]) -> Dict:
        return ml_merge_payments(access_token, pids)

    def _fetch_ship(sid: str) -> Dict:
        if not sid:
            return {}
        return _get_with_retry(
            f"https://api.mercadolibre.com/shipments/{sid}",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=15, retries=1,
        )

    def _fetch_ship_costs(sid: str) -> Dict:
        if not sid:
            return {}
        return _get_with_retry(
            f"https://api.mercadolibre.com/shipments/{sid}/costs",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=15, retries=1,
        )

    def _process_one(v: Dict) -> tuple:
        """Retorna ('db', row) | ('placeholder', row) | ('error', detalle). v siempre tiene payment_id (filtrado antes)."""
        pid = v["payment_id"]
        try:
            pay_data = _fetch_one(v.get("payment_ids_approved") or ([pid] if pid else []))
            ship_data = _fetch_ship(v.get("shipping_id") or "")
            ship_costs_data = {}
            if v.get("logistic_type") in ("self_service", "flex") and v.get("shipping_id"):
                ship_costs_data = _fetch_ship_costs(v.get("shipping_id"))
            lt = ship_data.get("logistic_type") or ""
            if lt:
                v["logistic_type"] = lt
            zip_code_v = (ship_data.get("receiver_address") or {}).get("zip_code") or ""
            senders_b = ship_costs_data.get("senders") or []
            bonif_flex_b = float((senders_b[0].get("save") if senders_b else 0) or 0)

            if not pay_data:
                sr = (v.get("status_raw") or "").lower()
                return ("placeholder", {
                    "payment_id": pid, "user_id": user_id, "order_id": v.get("order_id"),
                    "fetched_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                    "order_date": v["dt"].strftime("%Y-%m-%d") if v.get("dt") else None,
                    "pay_status": "cancelled" if "cancel" in sr else None,
                })

            calc = _compute_venta(pay_data, v, zip_code_v, bonif_flex_b, params, costos_sku, user_id, access_token, _fixed_fee_cache)
            if not calc:
                sr = (v.get("status_raw") or "").lower()
                return ("placeholder", {
                    "payment_id": pid, "user_id": user_id, "order_id": v.get("order_id"),
                    "fetched_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                    "order_date": v["dt"].strftime("%Y-%m-%d") if v.get("dt") else None,
                    "pay_status": "cancelled" if "cancel" in sr else None,
                })

            db_row = {
                "payment_id": pid, "user_id": user_id, "order_id": v.get("order_id"),
                "fetched_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "order_date": v["dt"].strftime("%Y-%m-%d") if v.get("dt") else None,
                "cuotas": str(v.get("cuotas") or "x1").strip().lower(),
                **calc,
            }
            return ("db", db_row)
        except Exception as e:
            return ("error", {"payment_id": pid, "order_id": v.get("order_id"), "error": str(e)})

    BATCH = 20
    for i in range(0, len(rows_to_process), BATCH):
        batch = rows_to_process[i:i + BATCH]
        max_w = min(16, max(1, len(batch)))
        with ThreadPoolExecutor(max_workers=max_w) as ex:
            results = list(ex.map(_process_one, batch))

        db_rows: List[Dict] = []
        placeholder_rows: List[Dict] = []
        for kind, payload in results:
            if kind == "db":
                db_rows.append(payload)
                procesadas += 1
            elif kind == "placeholder":
                placeholder_rows.append(payload)
                procesadas += 1
            elif kind == "error":
                errores.append(payload)
                procesadas += 1

        if db_rows:
            _save_batch(db_rows)
        if placeholder_rows:
            _save_placeholders(placeholder_rows)

        if progress_callback:
            progress_callback(procesadas, len(rows_to_process), ya_completas, len(errores))

        if (i // BATCH) % 3 == 0 or (i + BATCH) >= len(rows_to_process):
            log.info(
                "[BACKFILL] user=%s procesadas %d de %d (%d ya completas, %d errores)",
                user_id, procesadas, len(rows_to_process), ya_completas, len(errores),
            )

    return {
        "total_ventas": total,
        "ya_completas": ya_completas,
        "a_procesar": len(rows_to_process),
        "procesadas": procesadas - len(errores),
        "errores": len(errores),
        "errores_detalle": errores,
    }
