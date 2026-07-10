"""
Fase 3 — tabs/ventas.py
Pestaña Ventas: tabla de ventas desde el 1 del mes actual hasta hoy.
"""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
import calendar
from datetime import date as _date, datetime, timedelta
from typing import Any, Dict, List, Optional

from nicegui import app, background_tasks, context, run, ui

from db import get_connection, get_cotizador_param
from ml_api import (
    _cuotas_desde_item,
    get_ml_access_token,
    get_ml_session,
    ml_get_fixed_fee,
    ml_get_item_sale_price_full,
    ml_get_items_multiget_with_attributes,
    ml_get_orders,
    ml_get_user_id,
    ml_get_user_profile,
)


# ---------------------------------------------------------------------------
# Helper de sesión (copiado de main.py; se unificará en auth.py en Fase 4)
# ---------------------------------------------------------------------------

def _require_login() -> Optional[Dict[str, Any]]:
    user = app.storage.user.get("user")
    if not user:
        ui.notify("Debes iniciar sesión para continuar", color="negative")
    return user


# ---------------------------------------------------------------------------
# Flex tarifa lookup
# ---------------------------------------------------------------------------

def _get_flex_zona(user_id: int, zip_code: str) -> tuple:
    """Busca tarifa y nombre de zona Flex para un CP. Retorna (tarifa, nombre)."""
    if not zip_code or not zip_code.strip().isdigit():
        return 0.0, ""
    cp = zip_code.strip()
    conn = get_connection()
    zonas = conn.execute(
        "SELECT tarifa, nombre, codigos_postales FROM flex_zonas WHERE user_id=?", (user_id,)
    ).fetchall()
    conn.close()
    for zona in zonas:
        for tok in (zona["codigos_postales"] or "").split(","):
            tok = tok.strip()
            if not tok:
                continue
            if "-" in tok and tok.replace("-", "").isdigit():
                try:
                    lo, hi = tok.split("-", 1)
                    if int(lo) <= int(cp) <= int(hi):
                        return float(zona["tarifa"]), zona["nombre"] or ""
                except ValueError:
                    pass
            elif tok == cp:
                return float(zona["tarifa"]), zona["nombre"] or ""
    return 0.0, ""


# ---------------------------------------------------------------------------
# Tab principal
# ---------------------------------------------------------------------------

def build_tab_ventas(container) -> None:
    """Pestaña Ventas: tabla de ventas desde el 1 del mes actual hasta hoy."""
    container.clear()
    user = _require_login()
    if not user:
        return

    access_token = get_ml_access_token(user["id"])
    if not access_token:
        with container:
            ui.label("⚠️ No tienes MercadoLibre vinculado. Ve a Configuración y conecta tu cuenta.").classes("text-warning mb-4")
        return

    ventas_raw: List[Dict[str, Any]] = []
    all_orders_ref: Dict[str, List[Dict]] = {"orders": [], "item_id_to_catalog": {}, "item_id_to_sku": {}, "item_id_to_tipo_venta": {}, "item_id_to_cuotas": {}, "item_id_to_tipo_oferta": {}, "item_id_to_promo_display": {}}
    filtro_fecha_ref: Dict[str, str] = {"val": "hoy"}
    filtro_cuotas_ref: Dict[str, str] = {"val": "todas"}
    filtro_tipo_ref: Dict[str, str] = {"val": "todas"}
    filtro_estado_ref: Dict[str, str] = {"val": "pagada"}
    filtro_envio_ref: Dict[str, str] = {"val": "todos"}
    filtro_texto_ref: Dict[str, str] = {"val": ""}
    agrupar_ref: Dict[str, bool] = {"val": False}  # Por defecto desagrupado
    margenes_ref: Dict[str, str] = {}  # productos -> margen editable
    ganancia_neta_ref: Dict[str, float] = {"val": 0.0}
    params_ventas_ref: Dict[str, Any] = {}
    costos_sku_ref: Dict[str, Dict[str, Any]] = {}
    ventas_cache_ref: Dict[str, Dict[str, Any]] = {}

    sort_col_ventas: Dict[str, str] = {"val": "dt"}
    sort_asc_ventas: Dict[str, bool] = {"val": False}  # Fecha más reciente primero
    is_mobile_ref: Dict[str, bool] = {"val": False}

    with container:
        header_card = ui.column().classes("w-full mb-2")
        filtro_row = ui.row().classes("w-full mb-2 items-center gap-4")
        result_area = ui.column().classes("w-full gap-2")

        async def _detect_mobile() -> None:
            w = await ui.run_javascript("window.innerWidth")
            is_mobile_ref["val"] = int(w or 9999) < 768

        ui.timer(0, _detect_mobile, once=True)

        def _tipo_base_desde_body(body: Dict[str, Any]) -> str:
            """Devuelve Propia o Catálogo. Solo catalog_listing=True es Catálogo; catalog_listing=false o ausente es Propia."""
            if not body or not isinstance(body, dict):
                return "Propia"
            cl = body.get("catalog_listing")
            return "Catálogo" if (cl is True or str(cl or "").lower() in ("true", "1")) else "Propia"

        def _update_margen(productos_key: str, val: str) -> None:
            margenes_ref[productos_key] = val or ""

        def _calc_gan_row(unit_price: float, sku: str, cuotas_val: str) -> tuple:
            p = params_ventas_ref
            if not p:
                return None, None
            prod = costos_sku_ref.get(sku)
            if not prod or not prod.get("costo_usd"):
                return None, None
            costo_usd = float(prod["costo_usd"])
            tipo_iva = float(prod.get("tipo_iva") or 0.105)
            dolar = float(p.get("dolar_oficial") or 1475)
            ml_com = float(p.get("ml_comision") or 0.15)
            ml_deb = float(p.get("ml_debcre") or 0.006)
            ml_iibb = float(p.get("ml_iibb_per") or 0.055)
            ml_env = float(p.get("ml_envios") or 5823)
            ml_env_grat = float(p.get("ml_envios_gratuitos") or 33000)
            tasa_cuotas = {
                "x3": float(p.get("cuotas_3x") or 0),
                "x6": float(p.get("cuotas_6x") or 0),
                "x9": float(p.get("cuotas_9x") or 0),
                "x12": float(p.get("cuotas_12x") or 0),
            }.get(cuotas_val, 0.0)
            if unit_price <= 0:
                return None, None
            comision = unit_price * ml_com
            cobrado = unit_price - comision
            iva_venta = unit_price * tipo_iva / (1 + tipo_iva)
            iva_meli = comision * 0.21 / 1.21
            iva_impor = 0.09 * costo_usd * dolar
            iva_total = iva_venta - iva_meli - iva_impor
            deb_cred = unit_price * ml_deb
            iibb_monto = unit_price * ml_iibb
            envio = 0.0 if unit_price < ml_env_grat else ml_env
            costo_pesos = costo_usd * dolar
            costo_cuotas = unit_price * tasa_cuotas
            gan_pesos = cobrado - costo_pesos - iva_total - iibb_monto - deb_cred - envio - costo_cuotas
            gan_vta_pct = (gan_pesos / unit_price * 100) if unit_price > 0 else 0.0
            return gan_pesos, gan_vta_pct

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

        def _rango_desde_filtro(fecha_val: str, hoy) -> tuple:
            primer_dia = hoy.replace(day=1)
            dias_map = {"hoy": 1, "dias_2": 2, "dias_3": 3, "dias_5": 5, "dias_7": 7, "dias_15": 15, "dias_21": 21, "dias_30": 30}
            if fecha_val in dias_map:
                return hoy - timedelta(days=dias_map[fecha_val] - 1), hoy
            if fecha_val.startswith("mes_") and fecha_val != "mes_actual":
                try:
                    n = int(fecha_val[4:])
                    year, month = hoy.year, hoy.month
                    for _ in range(n):
                        month -= 1
                        if month == 0:
                            month = 12
                            year -= 1
                    return _date(year, month, 1), _date(year, month, calendar.monthrange(year, month)[1])
                except (ValueError, IndexError):
                    pass
            return primer_dia, hoy  # mes_actual y fallback

        def _aplicar_filtro_fecha() -> None:
            _cargar_ventas()

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
                    _pays_r1 = ord_item.get("payments") or []
                    _p_ap_r1 = next((p for p in _pays_r1 if str(p.get("status", "")).lower() == "approved"), None) or (_pays_r1[0] if _pays_r1 else None)
                    _payment_id = str(_p_ap_r1.get("id") or "") if _p_ap_r1 else ""
                    _payment_type = str(_p_ap_r1.get("payment_type") or "") if _p_ap_r1 else ""
                    dt = dt + timedelta(hours=1)
                    ventas_mes.append({
                        "dt": dt, "fecha": dt.strftime("%d/%m/%Y"), "hora": dt.strftime("%H:%M"), "productos": titulo[:100], "title": titulo[:100],
                        "tipo_venta": tipo, "cuotas": cuotas, "tipo": tipo_oferta, "tipo_oferta": tipo_oferta,
                        "tipo_display": tipo_display or tipo_oferta,
                        "cantidad": qty, "monto": item_monto, "monto_fmt": f"$ {item_monto:,.0f}".replace(",", "."),
                        "status": status_display, "status_raw": status_raw, "agrupar_key": agrupar_key, "item_id": item_id or "—",
                        "unit_price": unit_price,
                        "seller_sku": sku,
                        "order_id": str(ord_item.get("id", "") or ""),
                        "logistic_type": "",
                        "shipping_id": str((ord_item.get("shipping") or {}).get("id") or ""),
                        "buyer": (ord_item.get("buyer") or {}).get("nickname", "—"),
                        "payment_id": _payment_id,
                        "payment_type": _payment_type,
                        "gan_pesos": None,
                        "gan_vta_pct": None,
                    })
            ventas_raw = ventas_mes

        def _abrir_popup_venta(row: Dict[str, Any]) -> None:
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

            def _lbl(texto, es_real: bool):
                color_icon = "#27500A" if es_real else "#BA7517"
                icon = "ti-checks" if es_real else "ti-calculator"
                return ui.html(f'<span style="color:var(--color-text-secondary);font-size:12px">'
                               f'<i class="ti {icon}" style="font-size:12px;color:{color_icon};margin-right:4px" aria-hidden="true"></i>'
                               f'{texto}</span>')

            sku          = str(row.get("seller_sku") or "").removeprefix("SKU ").strip()
            unit_price   = float(row.get("unit_price") or 0)
            cantidad     = int(row.get("cantidad") or 1)
            cuotas_val   = str(row.get("cuotas") or "x1").strip().lower()
            p            = params_ventas_ref
            prod         = costos_sku_ref.get(sku) if sku else None
            costo_usd    = float((prod or {}).get("costo_usd") or 0)
            tipo_iva     = float((prod or {}).get("tipo_iva") or 0.105)
            dolar        = float(p.get("dolar_oficial") or 1475)
            ml_iibb      = float(p.get("ml_iibb_per") or 0.055)
            costo_pesos  = costo_usd * dolar
            total_price  = unit_price * cantidad
            total_costo  = costo_pesos * cantidad
            has_calc     = unit_price > 0 and costo_usd > 0

            _oid = str(row.get("order_id") or "")
            _iid = str(row.get("item_id") or "")
            raw_order = next((o for o in (all_orders_ref.get("orders") or []) if str(o.get("id", "")) == _oid), None)

            payment_id      = None
            shipping_id     = None
            thumb_url       = ""
            category_id     = ""
            listing_type_id = ""
            if raw_order:
                pays = raw_order.get("payments") or []
                _p_ap = next((p for p in pays if str(p.get("status", "")).lower() == "approved"), None) or (pays[0] if pays else None)
                if _p_ap:
                    payment_id = str(_p_ap.get("id") or "")
                ship = raw_order.get("shipping") or {}
                if ship.get("id"):
                    shipping_id = str(ship["id"])
                for _oit in raw_order.get("order_items") or []:
                    if not isinstance(_oit, dict): continue
                    _obj = _oit.get("item") or _oit
                    _iidd = str(_obj.get("id") or _oit.get("item_id") or "").strip() if isinstance(_obj, dict) else ""
                    if _iidd == _iid:
                        thumb_url = (_obj.get("thumbnail") or _obj.get("picture_url") or "") if isinstance(_obj, dict) else ""
                        category_id = str(_obj.get("category_id") or "") if isinstance(_obj, dict) else ""
                        listing_type_id = str(_oit.get("listing_type_id") or "")
                        break

            d = ui.dialog()
            with d:
                with ui.card().classes("p-4 min-w-[400px] max-w-[540px]"):
                    with ui.row().classes("w-full gap-3 mb-3 items-start"):
                        thumb_col = ui.column().classes("rounded border bg-gray-100 items-center justify-center flex-none").style("width:56px;height:56px;")
                        with thumb_col:
                            ui.label("...").classes("text-xs text-gray-400")
                        with ui.column().classes("flex-1 min-w-0 gap-0.5"):
                            _sku_txt = str(row.get("seller_sku") or "")
                            _iid_txt = str(row.get("item_id") or "—")
                            ui.label(f"{_iid_txt}  —  {_sku_txt}" if _sku_txt else _iid_txt).classes("text-xs font-mono text-gray-500")
                            _txt = str(row.get("title") or row.get("productos") or "—")
                            ui.label((_txt[:120] + "..." if len(_txt) > 120 else _txt)).classes("text-sm font-medium")
                            _fecha_hora = f"{row.get('fecha', '')}  {row.get('hora', '')}".strip()
                            ui.label(_fecha_hora or "—").classes("text-xs text-gray-500")
                    body_col = ui.column().classes("w-full gap-0")
                    with body_col:
                        with ui.row().classes("w-full items-center justify-center py-6 gap-3"):
                            ui.spinner(size="md")
                            ui.label("Cargando...").classes("text-sm text-gray-500")
                    with ui.row().classes("w-full justify-end mt-3"):
                        ui.button("Cerrar", on_click=lambda: d.close(), color="primary")
            d.open()
            d.on("hide", lambda: _pintar_tabla())

            cl = context.client

            async def _fetch_real() -> None:
                def _get_pay(tok, pid):
                    r = get_ml_session().get(f"https://api.mercadopago.com/v1/payments/{pid}",
                                     headers={"Authorization": f"Bearer {tok}"}, timeout=15)
                    return r.json() if r.status_code == 200 else {}

                def _get_item(tok, iid):
                    r = get_ml_session().get(f"https://api.mercadolibre.com/items/{iid}?attributes=thumbnail,pictures",
                                     headers={"Authorization": f"Bearer {tok}"}, timeout=15)
                    return r.json() if r.status_code == 200 else {}

                def _get_ship(tok, sid):
                    r = get_ml_session().get(f"https://api.mercadolibre.com/shipments/{sid}",
                                     headers={"Authorization": f"Bearer {tok}"}, timeout=15)
                    return r.json() if r.status_code == 200 else {}

                def _get_ship_costs(tok, sid):
                    r = get_ml_session().get(f"https://api.mercadolibre.com/shipments/{sid}/costs",
                                     headers={"Authorization": f"Bearer {tok}"}, timeout=15)
                    return r.json() if r.status_code == 200 else {}

                async def _noop():
                    return {}

                async def _noop0():
                    return 0.0

                _cached = ventas_cache_ref.get(payment_id or "")
                ml_env_grat = float(p.get("ml_envios_gratuitos") or 33000)

                bonif_flex = 0.0
                if _cached and payment_id:
                    item_coro2       = run.io_bound(_get_item,       access_token, _iid)        if _iid        else _noop()
                    ship_coro2       = run.io_bound(_get_ship,        access_token, shipping_id) if shipping_id else _noop()
                    ship_costs_coro2 = run.io_bound(_get_ship_costs,  access_token, shipping_id) if shipping_id else _noop()
                    item_data, ship_data, ship_costs_data = await asyncio.gather(item_coro2, ship_coro2, ship_costs_coro2)
                    zip_code = (ship_data.get("receiver_address") or {}).get("zip_code") or ""
                    has_api     = True
                    is_rejected = _cached.get("pay_status") == "rejected"
                    meli_fee    = float(_cached.get("meli_fee") or 0)
                    cuotas_fee  = float(_cached.get("cuotas_fee") or 0)
                    deb_cred    = float(_cached.get("deb_cred") or 0)
                    iibb_ret    = float(_cached.get("iibb_ret") or 0)
                    sirtac      = float(_cached.get("sirtac") or 0)
                    fixed_fee   = float(_cached.get("costo_fijo") or 0)
                    net_rcv     = _cached.get("net_rcv")
                    envio_real      = float(_cached.get("envio_real") or 0)
                    comprador_envio = float(_cached.get("comprador_envio") or 0)
                    _lt         = _cached.get("logistic_type") or ""
                    if _lt in ("cross_docking", "xd_drop_off", "drop_off", "me1", "me2"):
                        envio_lbl = "Envío Correo"
                    elif _lt in ("self_service", "flex"):
                        _flex_tarifa, _flex_zona = _get_flex_zona(user["id"], zip_code)
                        envio_real = _flex_tarifa
                        _zona_txt  = f" ({_flex_zona})" if _flex_zona else " (zona no encontrada)"
                        envio_lbl  = f"Envío Flex  {zip_code}{_zona_txt}" if zip_code else "Envío Flex"
                        _senders   = ship_costs_data.get("senders") or []
                        bonif_flex = float((_senders[0].get("save") if _senders else 0) or 0)
                    else:
                        envio_lbl = f"Envío Flex (CP {zip_code})" if zip_code else "Envío Flex"
                    envio_efectivo = 0.0 if unit_price < ml_env_grat else envio_real
                    iibb_perc   = total_price * ml_iibb
                    iva_venta   = total_price * tipo_iva / (1 + tipo_iva)
                    iva_meli    = meli_fee * 0.21 / 1.21
                    iva_impor   = 0.09 * costo_usd * dolar * cantidad
                    iva_total   = iva_venta - iva_meli - iva_impor
                    gan_pesos = gan_vta_pct = gan_cos_pct = None
                    if not is_rejected and has_calc:
                        gan_pesos   = total_price - meli_fee - cuotas_fee - iva_total - deb_cred - iibb_ret - sirtac - iibb_perc - envio_efectivo - total_costo + bonif_flex - fixed_fee
                        gan_vta_pct = (gan_pesos / total_price * 100) if total_price > 0 else 0.0
                        gan_cos_pct = (gan_pesos / total_costo * 100) if total_costo > 0 else 0.0
                    for _vr in ventas_raw:
                        if _vr.get("payment_id") == payment_id:
                            _vr["gan_pesos"] = gan_pesos
                            _vr["gan_vta_pct"] = gan_vta_pct
                            break
                else:
                    pay_coro        = run.io_bound(_get_pay,        access_token, payment_id) if payment_id  else _noop()
                    item_coro       = run.io_bound(_get_item,       access_token, _iid)        if _iid        else _noop()
                    ship_coro       = run.io_bound(_get_ship,        access_token, shipping_id) if shipping_id else _noop()
                    ship_costs_coro = run.io_bound(_get_ship_costs,  access_token, shipping_id) if shipping_id else _noop()
                    fee_coro        = run.io_bound(ml_get_fixed_fee, access_token, unit_price, category_id, listing_type_id) if (category_id and listing_type_id) else _noop0()
                    pay_data, item_data, ship_data, ship_costs_data, fixed_fee = await asyncio.gather(pay_coro, item_coro, ship_coro, ship_costs_coro, fee_coro)
                    zip_code = (ship_data.get("receiver_address") or {}).get("zip_code") or ""

                    is_rejected = pay_data.get("status") == "rejected"
                    charges    = pay_data.get("charges_details") or []
                    meli_fee   = sum(float((c.get("amounts") or {}).get("original", 0)) for c in charges if c.get("name") == "meli_percentage_fee")
                    cuotas_fee = sum(float((c.get("amounts") or {}).get("original", 0)) for c in charges if c.get("name") == "financing_add_on_fee")
                    deb_cred   = sum(float((c.get("amounts") or {}).get("original", 0)) for c in charges if "debitos_creditos" in (c.get("name") or ""))
                    iibb_ret   = sum(float((c.get("amounts") or {}).get("original", 0)) for c in charges if "iibb" in (c.get("name") or "").lower())
                    sirtac     = sum(float((c.get("amounts") or {}).get("original", 0)) for c in charges if "sirtac" in (c.get("name") or "").lower())
                    net_rcv    = (pay_data.get("transaction_details") or {}).get("net_received_amount")
                    has_api    = bool(charges) or is_rejected
                    iibb_perc  = total_price * ml_iibb
                    iva_venta  = total_price * tipo_iva / (1 + tipo_iva)
                    iva_meli   = meli_fee * 0.21 / 1.21
                    iva_impor  = 0.09 * costo_usd * dolar * cantidad
                    iva_total  = iva_venta - iva_meli - iva_impor

                    shp_xd = sum(float((c.get("amounts") or {}).get("original", 0)) for c in charges if c.get("name") == "shp_cross_docking")
                    buyer_shipping = float(pay_data.get("shipping_amount") or 0)
                    if shp_xd > 0:
                        comprador_envio = buyer_shipping
                        envio_real = max(0.0, shp_xd - buyer_shipping)
                        envio_lbl  = "Envío Correo"
                        _lt = "cross_docking"
                    elif has_api:
                        comprador_envio = 0.0
                        _lt = "self_service"
                        _flex_tarifa, _flex_zona = _get_flex_zona(user["id"], zip_code)
                        envio_real = _flex_tarifa
                        _zona_txt  = f" ({_flex_zona})" if _flex_zona else " (zona no encontrada)"
                        envio_lbl  = f"Envío Flex  {zip_code}{_zona_txt}" if zip_code else "Envío Flex"
                        _senders   = ship_costs_data.get("senders") or []
                        bonif_flex = float((_senders[0].get("save") if _senders else 0) or 0)
                    else:
                        comprador_envio = 0.0
                        envio_real = 0.0
                        envio_lbl  = None
                        _lt = ""

                    envio_efectivo = 0.0 if unit_price < ml_env_grat else envio_real
                    gan_pesos = gan_vta_pct = gan_cos_pct = None
                    if not is_rejected and has_calc:
                        gan_pesos   = total_price - meli_fee - cuotas_fee - iva_total - deb_cred - iibb_ret - sirtac - iibb_perc - envio_efectivo - total_costo + bonif_flex - fixed_fee
                        gan_vta_pct = (gan_pesos / total_price * 100) if total_price > 0 else 0.0
                        gan_cos_pct = (gan_pesos / total_costo * 100) if total_costo > 0 else 0.0

                    if payment_id and (has_api or has_calc):
                        _now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                        _ce = {
                            "payment_id": payment_id, "user_id": user["id"], "order_id": _oid,
                            "gan_pesos": gan_pesos, "gan_vta_pct": gan_vta_pct, "gan_cos_pct": gan_cos_pct,
                            "meli_fee": meli_fee, "cuotas_fee": cuotas_fee, "iva_total": iva_total,
                            "deb_cred": deb_cred, "iibb_ret": iibb_ret, "sirtac": sirtac,
                            "envio_real": envio_real, "comprador_envio": comprador_envio,
                            "logistic_type": _lt, "net_rcv": net_rcv,
                            "fetched_at": _now,
                            "pay_status": "rejected" if is_rejected else None,
                            "order_date": row["dt"].strftime("%Y-%m-%d") if row.get("dt") else None,
                            "cuotas": cuotas_val,
                            "costo_pesos": total_costo if (not is_rejected and has_calc) else None,
                            "costo_fijo": fixed_fee,
                        }
                        ventas_cache_ref[payment_id] = _ce
                        for _vr in ventas_raw:
                            if _vr.get("payment_id") == payment_id:
                                _vr["gan_pesos"] = gan_pesos
                                _vr["gan_vta_pct"] = gan_vta_pct
                                _vr["gan_cos_pct"] = gan_cos_pct
                                break

                        def _save_popup_pay(_ce=_ce):
                            conn = get_connection()
                            try:
                                cur = conn.cursor()
                                cur.execute(
                                    "INSERT OR REPLACE INTO ventas_datos "
                                    "(payment_id, user_id, order_id, gan_pesos, gan_vta_pct, gan_cos_pct, "
                                    "meli_fee, cuotas_fee, iva_total, deb_cred, iibb_ret, sirtac, "
                                    "envio_real, comprador_envio, logistic_type, net_rcv, fetched_at, pay_status, order_date, cuotas, "
                                    "costo_pesos, costo_fijo) "
                                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                    (_ce["payment_id"], _ce["user_id"], _ce.get("order_id"),
                                     _ce.get("gan_pesos"), _ce.get("gan_vta_pct"), _ce.get("gan_cos_pct"),
                                     _ce.get("meli_fee"), _ce.get("cuotas_fee"), _ce.get("iva_total"),
                                     _ce.get("deb_cred"), _ce.get("iibb_ret"), _ce.get("sirtac"),
                                     _ce.get("envio_real"), _ce.get("comprador_envio"),
                                     _ce.get("logistic_type"), _ce.get("net_rcv"),
                                     _ce.get("fetched_at"), _ce.get("pay_status"), _ce.get("order_date"),
                                     _ce.get("cuotas"), _ce.get("costo_pesos"), _ce.get("costo_fijo")),
                                )
                                conn.commit()
                            finally:
                                conn.close()
                        await run.io_bound(_save_popup_pay)

                _pay_type = row.get("payment_type") or ""
                envio_es_real = _pay_type != "account_money" and _lt in ("cross_docking", "xd_drop_off", "drop_off", "me1", "me2", "self_service", "flex")
                thumb_real = item_data.get("thumbnail") or ""
                if not thumb_real:
                    _pics = item_data.get("pictures") or []
                    if _pics:
                        thumb_real = _pics[0].get("secure_url") or _pics[0].get("url") or ""

                with cl:
                    if thumb_real:
                        thumb_col.clear()
                        with thumb_col:
                            ui.image(thumb_real).classes("object-contain rounded").style("width:56px;height:56px;")
                    body_col.clear()
                    with body_col:
                        # INFO BLOCK
                        with ui.row().classes("w-full flex-wrap gap-x-4 gap-y-1 border border-gray-200 rounded bg-gray-50 px-3 py-2 mb-3"):
                            for lbl_p, val_p in [
                                ("Cuotas",    str(row.get("cuotas") or "—")),
                                ("Tipo",      str(row.get("tipo_display") or row.get("tipo") or "—")),
                                ("Estado",    str(row.get("status") or "—")),
                                ("Cantidad",  str(row.get("cantidad") or "—")),
                                ("Comprador", str(row.get("buyer") or "—")),
                                ("Order ID",  str(row.get("order_id") or "—")),
                            ]:
                                with ui.column().classes("gap-0"):
                                    ui.label(lbl_p).classes("text-xs text-gray-500")
                                    ui.label(val_p).classes("text-xs font-medium")
                        if is_rejected:
                            ui.label("Pago rechazado — sin comisión cobrada").classes("text-negative text-sm font-medium py-3 text-center w-full")
                        elif not has_api:
                            _ml_com      = float(p.get("ml_comision") or 0.15)
                            _ml_deb      = float(p.get("ml_debcre") or 0.006)
                            _ml_env      = float(p.get("ml_envios") or 5823)
                            _ml_env_grat = float(p.get("ml_envios_gratuitos") or 33000)
                            _tasa_e = {
                                "x3": float(p.get("cuotas_3x") or 0), "x6": float(p.get("cuotas_6x") or 0),
                                "x9": float(p.get("cuotas_9x") or 0), "x12": float(p.get("cuotas_12x") or 0),
                            }.get(cuotas_val, 0.0)
                            _meli_fee_e   = total_price * _ml_com
                            _cuotas_fee_e = total_price * _tasa_e
                            _deb_cred_e   = total_price * _ml_deb
                            _iibb_perc_e  = total_price * ml_iibb
                            _iva_venta_e  = total_price * tipo_iva / (1 + tipo_iva)
                            _iva_meli_e   = _meli_fee_e * 0.21 / 1.21
                            _iva_impor_e  = 0.09 * costo_usd * dolar * cantidad
                            _iva_total_e  = _iva_venta_e - _iva_meli_e - _iva_impor_e
                            _envio_e      = 0.0 if unit_price < _ml_env_grat else _ml_env
                            _gan_pesos_e = _gan_vta_pct_e = _gan_cos_pct_e = None
                            if has_calc:
                                _gan_pesos_e   = total_price - _meli_fee_e - _cuotas_fee_e - _iva_total_e - _deb_cred_e - _iibb_perc_e - _envio_e - total_costo - fixed_fee
                                _gan_vta_pct_e = (_gan_pesos_e / total_price * 100) if total_price > 0 else 0.0
                                _gan_cos_pct_e = (_gan_pesos_e / total_costo * 100) if total_costo > 0 else 0.0
                            with ui.column().classes("w-full gap-0"):
                                with ui.row().classes("w-full justify-between items-center py-1.5 border-b border-gray-200 mb-1"):
                                    _pv_lbl = f"Precio de venta (×{cantidad})" if cantidad > 1 else "Precio de venta"
                                    ui.label(_pv_lbl).classes("text-sm text-gray-600")
                                    ui.label(fmt_moneda(total_price)).classes("text-sm font-medium")
                                if cantidad > 1:
                                    ui.label(f"{fmt_moneda(unit_price)} × {cantidad}").classes("text-xs text-gray-400 -mt-1 mb-0.5")
                                with ui.row().classes("w-full justify-between items-center py-0.5"):
                                    _lbl("Comisión ML", False)
                                    ui.label(fmt_moneda(_meli_fee_e)).classes("text-sm text-negative")
                                with ui.row().classes("w-full justify-between items-center py-0.5"):
                                    _lbl("Costo fijo", True)
                                    ui.label(fmt_moneda(fixed_fee)).classes("text-sm text-negative")
                                with ui.row().classes("w-full justify-between items-center py-0.5"):
                                    _lbl("Total cargo ML", False)
                                    ui.label(fmt_moneda(_meli_fee_e + fixed_fee)).classes("text-sm text-negative font-medium")
                                with ui.row().classes("w-full justify-between items-center py-0.5"):
                                    _cf_pct_e = f" ({_cuotas_fee_e / total_price * 100:.1f}%)".replace(".", ",") if _cuotas_fee_e and total_price else ""
                                    _lbl(f"Costo Cuotas{_cf_pct_e}", False)
                                    ui.label(fmt_moneda(_cuotas_fee_e)).classes("text-sm text-negative")
                                with ui.row().classes("w-full justify-between items-center py-0.5"):
                                    _lbl("IVA neto", False)
                                    ui.label(fmt_moneda(_iva_total_e)).classes("text-sm text-negative")
                                with ui.column().classes("w-full bg-gray-50 rounded px-2 py-1 mb-0.5 gap-0"):
                                    for lbl_s, val_s in [
                                        ("IVA venta",              _iva_venta_e),
                                        ("IVA Meli (crédito)",     _iva_meli_e),
                                        ("IVA importación (créd)", _iva_impor_e),
                                    ]:
                                        with ui.row().classes("w-full justify-between"):
                                            _lbl(lbl_s, False)
                                            ui.label(fmt_moneda(val_s)).classes("text-xs text-gray-600")
                                with ui.row().classes("w-full justify-between items-center py-0.5"):
                                    _lbl("Deb/Cred", False)
                                    ui.label(fmt_moneda(_deb_cred_e)).classes("text-sm text-negative")
                                with ui.row().classes("w-full justify-between items-center py-0.5"):
                                    _lbl("IIBB perc.", False)
                                    ui.label(fmt_moneda(_iibb_perc_e)).classes("text-sm text-negative")
                                if has_calc:
                                    with ui.column().classes("w-full py-0.5 gap-0"):
                                        with ui.row().classes("w-full justify-between items-center"):
                                            _cp_lbl = f"Costo producto (×{cantidad})" if cantidad > 1 else "Costo producto"
                                            _lbl(_cp_lbl, False)
                                            ui.label(fmt_moneda(total_costo)).classes("text-sm text-negative")
                                        if cantidad > 1:
                                            ui.label(f"{fmt_usd(costo_usd)} × {fmt_moneda(dolar)} × {cantidad}").classes("text-xs text-gray-400")
                                        else:
                                            ui.label(f"{fmt_usd(costo_usd)} × {fmt_moneda(dolar)}").classes("text-xs text-gray-400")
                                with ui.row().classes("w-full justify-between items-center py-0.5 border-b-2 border-gray-300 pb-2 mb-2"):
                                    _lbl("Envío", False)
                                    ui.label(fmt_moneda(_envio_e)).classes("text-sm text-negative")
                                if _gan_pesos_e is not None:
                                    _gcls = "text-positive" if _gan_pesos_e >= 0 else "text-negative"
                                    with ui.row().classes("w-full justify-between items-end pt-1 gap-2"):
                                        with ui.column().classes("gap-0"):
                                            ui.label("Gan $").classes("text-xs text-gray-500")
                                            ui.label(fmt_moneda(_gan_pesos_e)).classes(f"text-xl font-bold {_gcls}")
                                        with ui.column().classes("gap-0 items-end"):
                                            ui.label("Gan Vta %").classes("text-xs text-gray-500")
                                            ui.label(fmt_pct2(_gan_vta_pct_e)).classes(f"text-xl font-bold {_gcls}")
                                        with ui.column().classes("gap-0 items-end"):
                                            ui.label("Gan % Cos").classes("text-xs text-gray-500")
                                            ui.label(fmt_pct2(_gan_cos_pct_e)).classes(f"text-xl font-bold {_gcls}")
                                ui.separator()
                                with ui.row().classes("items-center gap-1 text-xs").style("color: var(--color-text-secondary)"):
                                    ui.html('<i class="ti ti-calculator" style="font-size:12px;color:#BA7517" aria-hidden="true"></i>')
                                    ui.label("Valores estimados con parámetros del cotizador")
                        else:
                            with ui.column().classes("w-full gap-0"):
                                # 1. Precio de venta
                                with ui.row().classes("w-full justify-between items-center py-1.5 border-b border-gray-200 mb-1"):
                                    _pv_lbl = f"Precio de venta (×{cantidad})" if cantidad > 1 else "Precio de venta"
                                    ui.label(_pv_lbl).classes("text-sm text-gray-600")
                                    ui.label(fmt_moneda(total_price)).classes("text-sm font-medium")
                                if cantidad > 1:
                                    ui.label(f"{fmt_moneda(unit_price)} × {cantidad}").classes("text-xs text-gray-400 -mt-1 mb-0.5")
                                # 2. Comisión ML
                                with ui.row().classes("w-full justify-between items-center py-0.5"):
                                    _lbl("Comisión ML", True)
                                    ui.label(fmt_moneda(meli_fee)).classes("text-sm text-negative")
                                # 2b. Costo fijo
                                with ui.row().classes("w-full justify-between items-center py-0.5"):
                                    _lbl("Costo fijo", True)
                                    ui.label(fmt_moneda(fixed_fee)).classes("text-sm text-negative")
                                with ui.row().classes("w-full justify-between items-center py-0.5"):
                                    _lbl("Total cargo ML", True)
                                    ui.label(fmt_moneda(meli_fee + fixed_fee)).classes("text-sm text-negative font-medium")
                                # 3. Costo Cuotas (siempre)
                                with ui.row().classes("w-full justify-between items-center py-0.5"):
                                    _cf_pct = f" ({cuotas_fee / total_price * 100:.1f}%)".replace(".", ",") if cuotas_fee and total_price else ""
                                    _lbl(f"Costo Cuotas{_cf_pct}", True)
                                    ui.label(fmt_moneda(cuotas_fee)).classes("text-sm text-negative")
                                # 4. IVA neto + sub-block
                                with ui.row().classes("w-full justify-between items-center py-0.5"):
                                    _lbl("IVA neto", False)
                                    ui.label(fmt_moneda(iva_total)).classes("text-sm text-negative")
                                with ui.column().classes("w-full bg-gray-50 rounded px-2 py-1 mb-0.5 gap-0"):
                                    for lbl_s, val_s in [
                                        ("IVA venta",              iva_venta),
                                        ("IVA Meli (crédito)",     iva_meli),
                                        ("IVA importación (créd)", iva_impor),
                                    ]:
                                        with ui.row().classes("w-full justify-between"):
                                            _lbl(lbl_s, False)
                                            ui.label(fmt_moneda(val_s)).classes("text-xs text-gray-600")
                                # 5. Deb/Cred
                                with ui.row().classes("w-full justify-between items-center py-0.5"):
                                    _lbl("Deb/Cred", True)
                                    ui.label(fmt_moneda(deb_cred)).classes("text-sm text-negative")
                                # 6. SIRTAC
                                with ui.row().classes("w-full justify-between items-center py-0.5"):
                                    _lbl("SIRTAC", True)
                                    ui.label(fmt_moneda(sirtac)).classes("text-sm text-negative")
                                # 7. IIBB combinado + sub-block
                                with ui.row().classes("w-full justify-between items-center py-0.5"):
                                    ui.label("IIBB").classes("text-sm text-gray-600")
                                    ui.label(fmt_moneda(iibb_ret + iibb_perc)).classes("text-sm text-negative")
                                with ui.column().classes("w-full bg-gray-50 rounded px-2 py-1 mb-0.5 gap-0"):
                                    with ui.row().classes("w-full justify-between"):
                                        _lbl("IIBB ret.", True)
                                        ui.label(fmt_moneda(iibb_ret)).classes("text-xs text-negative")
                                    with ui.row().classes("w-full justify-between"):
                                        _lbl("IIBB perc.", False)
                                        ui.label(fmt_moneda(iibb_perc)).classes("text-xs text-negative")
                                # 8. Costo producto
                                if has_calc:
                                    with ui.column().classes("w-full py-0.5 gap-0"):
                                        with ui.row().classes("w-full justify-between items-center"):
                                            _cp_lbl = f"Costo producto (×{cantidad})" if cantidad > 1 else "Costo producto"
                                            _lbl(_cp_lbl, False)
                                            ui.label(fmt_moneda(total_costo)).classes("text-sm text-negative")
                                        if cantidad > 1:
                                            ui.label(f"{fmt_usd(costo_usd)} × {fmt_moneda(dolar)} × {cantidad}").classes("text-xs text-gray-400")
                                        else:
                                            ui.label(f"{fmt_usd(costo_usd)} × {fmt_moneda(dolar)}").classes("text-xs text-gray-400")
                                # 9. Envío + separador
                                with ui.column().classes("w-full border-b-2 border-gray-300 pb-2 mb-2 gap-0"):
                                    with ui.row().classes("w-full justify-between items-center py-0.5"):
                                        _lbl(envio_lbl or "Envío", False if _lt in ("self_service", "flex") else envio_es_real)
                                        ui.label(fmt_moneda(envio_efectivo)).classes("text-sm text-negative")
                                    if comprador_envio > 0:
                                        with ui.row().classes("w-full justify-between items-center py-0.5"):
                                            _lbl("Comprador pagó envío", True)
                                            ui.label(f"+{fmt_moneda(comprador_envio)}").classes("text-sm text-positive")
                                    if bonif_flex > 0:
                                        with ui.row().classes("w-full justify-between items-center py-0.5"):
                                            _lbl("Bonificación envío", True)
                                            ui.label(f"+{fmt_moneda(bonif_flex)}").classes("text-sm text-positive")
                                # 10. Gan $ / Gan Vta % / Gan % Cos en una fila
                                if gan_pesos is not None:
                                    _gcls = "text-positive" if gan_pesos >= 0 else "text-negative"
                                    with ui.row().classes("w-full justify-between items-end pt-1 gap-2"):
                                        with ui.column().classes("gap-0"):
                                            ui.label("Gan $").classes("text-xs text-gray-500")
                                            ui.label(fmt_moneda(gan_pesos)).classes(f"text-xl font-bold {_gcls}")
                                        with ui.column().classes("gap-0 items-end"):
                                            ui.label("Gan Vta %").classes("text-xs text-gray-500")
                                            ui.label(fmt_pct2(gan_vta_pct)).classes(f"text-xl font-bold {_gcls}")
                                        with ui.column().classes("gap-0 items-end"):
                                            ui.label("Gan % Cos").classes("text-xs text-gray-500")
                                            ui.label(fmt_pct2(gan_cos_pct)).classes(f"text-xl font-bold {_gcls}")
                                ui.separator()
                                with ui.row().classes("gap-4 text-xs").style("color: var(--color-text-secondary)"):
                                    with ui.row().classes("items-center gap-1"):
                                        ui.html('<i class="ti ti-checks" style="font-size:12px;color:#27500A" aria-hidden="true"></i>')
                                        ui.label("Dato real de ML/MP")
                                    with ui.row().classes("items-center gap-1"):
                                        ui.html('<i class="ti ti-calculator" style="font-size:12px;color:#BA7517" aria-hidden="true"></i>')
                                        ui.label("Valor estimado")

            background_tasks.create(_fetch_real(), name="popup_venta_real")

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
                ventas_filtradas = [v for v in ventas_raw if (v.get("status_raw") or "").lower() in ("paid", "handling", "shipped", "delivered") and v.get("pay_status") != "rejected" and not v.get("has_refund")]
            elif estado_val == "cancelada":
                ventas_filtradas = [v for v in ventas_raw if "cancel" in (v.get("status_raw") or "").lower() or v.get("pay_status") == "rejected" or v.get("has_refund")]
            cuotas_val = str(filtro_cuotas_ref.get("val", "todas") or "todas")
            if cuotas_val in ("x1", "x3", "x6", "x9", "x12"):
                ventas_filtradas = [v for v in ventas_filtradas if (v.get("cuotas") or "x1") == cuotas_val]
            tipo_val = str(filtro_tipo_ref.get("val", "todas") or "todas")
            if tipo_val == "promo":
                ventas_filtradas = [v for v in ventas_filtradas if (v.get("tipo") or "").lower() == "promo"]
            elif tipo_val == "regular":
                ventas_filtradas = [v for v in ventas_filtradas if (v.get("tipo") or "").lower() == "regular"]
            envio_val = str(filtro_envio_ref.get("val", "todos") or "todos")
            if envio_val == "correo":
                ventas_filtradas = [v for v in ventas_filtradas
                    if (v.get("logistic_type") or "").lower()
                    in ("cross_docking", "xd_drop_off", "drop_off", "me1", "me2")]
            elif envio_val == "flex":
                ventas_filtradas = [v for v in ventas_filtradas
                    if (v.get("logistic_type") or "").lower() in ("self_service", "flex")]
            texto_val = (filtro_texto_ref.get("val") or "").strip().lower()
            if texto_val:
                ventas_filtradas = [v for v in ventas_filtradas if
                    texto_val in (v.get("productos") or "").lower() or
                    texto_val in (v.get("title") or "").lower() or
                    texto_val in (v.get("item_id") or "").lower()]
            hoy = datetime.now().date()
            fecha_val = filtro_fecha_ref.get("val", "hoy")
            date_ini_pt, date_fin_pt = _rango_desde_filtro(fecha_val, hoy)
            dias_total = (date_fin_pt - date_ini_pt).days + 1
            total_monto_ok = sum(v["monto"] for v in ventas_filtradas)
            total_unidades_ok = sum(v["cantidad"] for v in ventas_filtradas)
            n_ventas_ok = len(ventas_filtradas)
            ticket_promedio = total_monto_ok / n_ventas_ok if n_ventas_ok > 0 else 0
            ganancia_total = sum(v["gan_pesos"] for v in ventas_filtradas if v.get("gan_pesos") is not None)
            gan_prom_pct = (ganancia_total / total_monto_ok * 100) if total_monto_ok > 0 else 0.0
            ventas_diarias = total_monto_ok / dias_total if dias_total > 0 else 0
            ventas_diarias_u = total_unidades_ok / dias_total if dias_total > 0 else 0
            gan_prom_dia = ganancia_total / dias_total if dias_total > 0 else 0
            header_card.clear()
            with header_card:
                if is_mobile_ref.get("val"):
                    with ui.column().classes("w-full px-2 py-2 gap-2"):
                        _meses_es_m = ["Enero","Febrero","Marzo","Abril","Mayo","Junio",
                                       "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"]
                        _hoy_m = datetime.now().date()
                        _oy_m, _om_m = _hoy_m.year, _hoy_m.month
                        _opts_fecha_m = {
                            "hoy": "Hoy", "dias_2": "Últimos 2 días", "dias_3": "Últimos 3 días",
                            "dias_5": "Últimos 5 días", "dias_7": "Últimos 7 días",
                            "dias_15": "Últimos 15 días", "dias_21": "Últimos 21 días",
                            "dias_30": "Últimos 30 días", "mes_actual": "Mes actual",
                        }
                        for _i_m in range(1, 7):
                            _om_m -= 1
                            if _om_m == 0:
                                _om_m = 12; _oy_m -= 1
                            _opts_fecha_m[f"mes_{_i_m}"] = f"{_meses_es_m[_om_m - 1]} {_oy_m}"
                        _sel_m = ui.select(_opts_fecha_m, value=filtro_fecha_ref.get("val", "hoy"), label="Fecha").classes("w-full").bind_value(filtro_fecha_ref, "val")
                        _sel_m.on_value_change(lambda: _aplicar_filtro_fecha())
                        _inp_m = ui.input(placeholder="Buscar producto...").props("outlined dense clearable").classes("w-full")
                        _inp_m.bind_value(filtro_texto_ref, "val")
                        _inp_m.on_value_change(lambda: _pintar_tabla())
                        with ui.row().classes("w-full items-center gap-3"):
                            ui.button("Actualizar", on_click=lambda: _cargar_ventas(), color="primary").props("icon=refresh no-caps").classes("rounded px-3")
                            ui.button("Completar datos", on_click=lambda: _abrir_dialog_enriquecer()).props("icon=download no-caps").classes("rounded px-3")
                        with ui.row().classes("w-full flex-wrap gap-x-4 gap-y-1 px-1 py-1"):
                            with ui.column().classes("gap-0"):
                                ui.label("Facturación").classes("text-xs text-gray-500")
                                ui.label(f"$ {total_monto_ok:,.0f}".replace(",", ".")).classes("text-base font-bold text-primary")
                            with ui.column().classes("gap-0"):
                                ui.label("Ventas").classes("text-xs text-gray-500")
                                ui.label(str(n_ventas_ok)).classes("text-base font-bold text-primary")
                            with ui.column().classes("gap-0"):
                                ui.label("Ganancia").classes("text-xs text-gray-500")
                                _gt_cls_m = "text-positive" if ganancia_total >= 0 else "text-negative"
                                ui.label(f"$ {ganancia_total:,.0f}".replace(",", ".")).classes(f"text-base font-bold {_gt_cls_m}")
                            with ui.column().classes("gap-0"):
                                ui.label("Gan. prom. %").classes("text-xs text-gray-500")
                                if gan_prom_pct is not None:
                                    _gpp_cls_m = "text-positive" if gan_prom_pct >= 0 else "text-negative"
                                    ui.label(f"{gan_prom_pct:.2f}%".replace(".", ",")).classes(f"text-base font-bold {_gpp_cls_m}")
                                else:
                                    ui.label("—").classes("text-base font-bold text-gray-400")
                else:
                    with ui.card().classes("w-full p-0 bg-grey-2").style("border: 1px solid rgba(0,0,0,0.1); border-radius: 8px; overflow: hidden;"):
                        with ui.row().classes("w-full gap-0 items-stretch").style("flex-wrap: nowrap;"):
                            with ui.element("div").classes("px-4 py-2").style("flex: 1; min-width: 0;"):
                                ui.label("TOTALES").style("font-size: 10px; font-weight: 600; color: rgba(0,0,0,0.35); letter-spacing: 0.06em;")
                                with ui.row().classes("gap-5 mt-1 flex-wrap items-end"):
                                    with ui.column().classes("gap-0"):
                                        ui.label("Facturación").classes("text-xs text-gray-500")
                                        ui.label(f"$ {total_monto_ok:,.0f}".replace(",", ".")).classes("text-lg font-bold text-primary")
                                    with ui.column().classes("gap-0"):
                                        ui.label("Ventas").classes("text-xs text-gray-500")
                                        ui.label(str(n_ventas_ok)).classes("text-lg font-bold text-primary")
                                    with ui.column().classes("gap-0"):
                                        ui.label("Unidades vendidas").classes("text-xs text-gray-500")
                                        ui.label(str(total_unidades_ok)).classes("text-lg font-bold text-primary")
                                    with ui.column().classes("gap-0"):
                                        ui.label("Ganancia total").classes("text-xs text-gray-500")
                                        _gt_cls = "text-positive" if ganancia_total >= 0 else "text-negative"
                                        ui.label(f"$ {ganancia_total:,.0f}".replace(",", ".")).classes(f"text-lg font-bold {_gt_cls}")
                                    with ui.column().classes("gap-0"):
                                        ui.label("Días").classes("text-xs text-gray-500")
                                        ui.label(str(dias_total)).classes("text-lg font-bold text-primary")
                            ui.element("div").style("width: 2px; background: rgba(0,0,0,0.2); align-self: stretch; margin: 8px 4px;")
                            with ui.element("div").classes("px-4 py-2").style("flex: 1; min-width: 0;"):
                                ui.label("PROMEDIO DIARIO").style("font-size: 10px; font-weight: 600; color: rgba(0,0,0,0.35); letter-spacing: 0.06em;")
                                with ui.row().classes("gap-5 mt-1 flex-wrap items-end"):
                                    with ui.column().classes("gap-0"):
                                        ui.label("Facturación").classes("text-xs text-gray-500")
                                        ui.label(f"$ {ventas_diarias:,.0f}".replace(",", ".")).classes("text-lg font-bold text-primary")
                                    with ui.column().classes("gap-0"):
                                        ui.label("Ventas/día $").classes("text-xs text-gray-500")
                                        _vpd = n_ventas_ok / dias_total if dias_total > 0 else 0
                                        ui.label(f"{_vpd:,.2f}".replace(",", ".")).classes("text-lg font-bold text-primary")
                                    with ui.column().classes("gap-0"):
                                        ui.label("Unidades").classes("text-xs text-gray-500")
                                        ui.label(f"{ventas_diarias_u:,.2f}".replace(",", ".")).classes("text-lg font-bold text-primary")
                                    with ui.column().classes("gap-0"):
                                        ui.label("Ticket promedio").classes("text-xs text-gray-500")
                                        ui.label(f"$ {ticket_promedio:,.0f}".replace(",", ".")).classes("text-lg font-bold text-primary")
                                    with ui.column().classes("gap-0"):
                                        ui.label("Gan. prom. $").classes("text-xs text-gray-500")
                                        _gd_cls = "text-positive" if gan_prom_dia >= 0 else "text-negative"
                                        ui.label(f"$ {gan_prom_dia:,.0f}".replace(",", ".")).classes(f"text-lg font-bold {_gd_cls}")
                                    with ui.column().classes("gap-0"):
                                        ui.label("Gan. prom. %").classes("text-xs text-gray-500")
                                        if gan_prom_pct is not None:
                                            _gpp_cls = "text-positive" if gan_prom_pct >= 0 else "text-negative"
                                            ui.label(f"{gan_prom_pct:.2f}%".replace(".", ",")).classes(f"text-lg font-bold {_gpp_cls}")
                                        else:
                                            ui.label("—").classes("text-lg font-bold text-gray-400")
                            with ui.element("div").classes("flex items-center gap-2 px-3 py-2 shrink-0"):
                                ui.button("Actualizar", on_click=lambda: _cargar_ventas(), color="primary").props("icon=refresh no-caps").classes("rounded px-3")
                                ui.button("Completar datos", on_click=lambda: _abrir_dialog_enriquecer()).props("icon=download no-caps").classes("rounded px-3")
                                ui.button("Completar datos por lote", on_click=lambda: _abrir_dialog_backfill()).props("icon=history no-caps").classes("rounded px-3")
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
                                if v.get("gan_pesos") is not None:
                                    grupos[key]["gan_pesos"] = grupos[key].get("gan_pesos", 0.0) + v["gan_pesos"]
                            filas = list(grupos.values())
                            sort_col = sort_col_ventas.get("val", "cantidad")
                            asc = sort_asc_ventas.get("val", False)
                            if sort_col == "productos":
                                filas.sort(key=lambda x: str(x.get("productos", "")).lower(), reverse=not asc)
                            elif sort_col == "monto":
                                filas.sort(key=lambda x: x["monto"], reverse=not asc)
                            else:
                                filas.sort(key=lambda x: x["cantidad"], reverse=not asc)
                            if is_mobile_ref.get("val"):
                                with ui.element("div").style("width:100%;overflow-x:auto;max-height:75vh;overflow-y:scroll"):
                                    with ui.element("table").style("width:100%;border-collapse:collapse;font-size:12px"):
                                        with ui.element("thead"):
                                            with ui.element("tr").classes("bg-primary text-white font-semibold"):
                                                for _hm in ("Producto", "Cant", "Monto", "Gan $", "Gan %"):
                                                    with ui.element("th").classes("px-2 py-2 border text-center"):
                                                        ui.label(_hm)
                                        with ui.element("tbody"):
                                            for idx, v in enumerate(filas, 1):
                                                productos_key = str(v["productos"])
                                                _gp_g = v.get("gan_pesos")
                                                _gvp_g = (_gp_g / v["monto"] * 100) if (_gp_g is not None and v["monto"] > 0) else None
                                                with ui.element("tr").classes("border-t border-gray-200 hover:bg-gray-50"):
                                                    with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-xs"):
                                                        ui.label(productos_key[:60]).classes("truncate block max-w-[160px]")
                                                    with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center text-xs"):
                                                        ui.label(str(v["cantidad"]))
                                                    with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-right font-medium text-xs whitespace-nowrap"):
                                                        ui.label(f"$ {v['monto']:,.0f}".replace(",", "."))
                                                    with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-right text-xs whitespace-nowrap"):
                                                        if _gp_g is None:
                                                            ui.label("—").classes("text-gray-400")
                                                        else:
                                                            ui.label(f"$ {_gp_g:,.0f}".replace(",", ".")).classes(f"font-medium {'text-positive' if _gp_g >= 0 else 'text-negative'}")
                                                    with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-right text-xs whitespace-nowrap"):
                                                        if _gvp_g is None:
                                                            ui.label("—").classes("text-gray-400")
                                                        else:
                                                            ui.label(f"{_gvp_g:.2f}%".replace(".", ",")).classes(f"font-medium {'text-positive' if _gvp_g >= 0 else 'text-negative'}")
                            else:
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
                        if is_mobile_ref.get("val"):
                            # Mobile: tabla simple 6 columnas
                            with ui.element("div").style("width:100%;overflow-x:auto;max-height:75vh;overflow-y:scroll"):
                                with ui.element("table").style("width:100%;border-collapse:collapse;font-size:12px"):
                                    with ui.element("thead"):
                                        with ui.element("tr").classes("bg-primary text-white font-semibold"):
                                            for _hm in ("Fecha", "Producto", "Cant", "Monto", "Gan $", "Gan %"):
                                                with ui.element("th").classes("px-2 py-2 border text-center"):
                                                    ui.label(_hm)
                                    with ui.element("tbody"):
                                        for idx, v in enumerate(ventas_orden, 1):
                                            _is_cancelled_row = (v.get("status_raw") or "") in ("cancelled", "canceled")
                                            _is_rej_row = v.get("pay_status") == "rejected"
                                            _is_dev_row = bool(v.get("has_refund"))
                                            _hide_gan = _is_cancelled_row or _is_rej_row or _is_dev_row
                                            _tr_style = "color: #dc2626;" if (_is_cancelled_row or _is_rej_row or _is_dev_row) else ""
                                            with ui.element("tr").classes("border-t border-gray-200 hover:bg-gray-50").style(_tr_style):
                                                with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center text-xs whitespace-nowrap"):
                                                    ui.label(v["fecha"])
                                                with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-xs max-w-[150px]"):
                                                    _titulo = v.get("productos", v.get("title", "—"))[:50]
                                                    ui.button(_titulo, on_click=lambda row=v: _abrir_popup_venta(row)).props("flat dense no-caps align=left").classes("text-left text-xs text-blue-600 hover:underline cursor-pointer w-full truncate")
                                                with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center text-xs"):
                                                    ui.label(str(v["cantidad"]))
                                                with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-right font-medium text-xs whitespace-nowrap"):
                                                    ui.label(v["monto_fmt"])
                                                _gp = v.get("gan_pesos")
                                                with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-right text-xs whitespace-nowrap"):
                                                    if _hide_gan or _gp is None:
                                                        ui.label("—").classes("text-gray-400")
                                                    else:
                                                        ui.label(f"$ {_gp:,.0f}".replace(",", ".")).classes(f"font-medium {'text-positive' if _gp >= 0 else 'text-negative'}")
                                                _gvp = v.get("gan_vta_pct")
                                                with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-right text-xs whitespace-nowrap"):
                                                    if _hide_gan or _gvp is None:
                                                        ui.label("—").classes("text-gray-400")
                                                    else:
                                                        ui.label(f"{_gvp:.2f}%".replace(".", ",")).classes(f"font-medium {'text-positive' if _gvp >= 0 else 'text-negative'}")
                        else:
                            # Desktop: sticky header 14 columnas
                            _vhd = ui.element("div").style("width:100%;overflow:hidden")
                            _vtc = ui.element("div").style("width:100%;height:65vh;overflow-y:scroll;overflow-x:auto")
                            _vhid = _vhd.id
                            _vcid = _vtc.id
                            async def _setup_ventas_sync(_vhid=_vhid, _vcid=_vcid) -> None:
                                await ui.run_javascript(
                                    f"(function(){{"
                                    f"var body=document.getElementById('c{_vcid}');"
                                    f"var hdr=document.getElementById('c{_vhid}');"
                                    f"if(!body||!hdr)return;"
                                    f"body.addEventListener('scroll',function(){{hdr.scrollLeft=body.scrollLeft;}});"
                                    f"function _sg(){{hdr.style.paddingRight=(body.offsetWidth-body.clientWidth)+'px';}}"
                                    f"_sg();new ResizeObserver(_sg).observe(body);"
                                    f"}})();"
                                )
                            ui.timer(0.1, _setup_ventas_sync, once=True)
                            def _build_colgroup_ventas() -> None:
                                with ui.element("colgroup"):
                                    ui.element("col").style("width:3%")
                                    ui.element("col").style("width:7%")
                                    ui.element("col").style("width:9%")
                                    ui.element("col").style("width:9%")
                                    ui.element("col").style("width:5%")
                                    ui.element("col").style("width:8%")
                                    ui.element("col").style("width:5%")
                                    ui.element("col").style("width:5%")
                                    ui.element("col").style("width:20%")
                                    ui.element("col").style("width:4%")
                                    ui.element("col").style("width:7%")
                                    ui.element("col").style("width:7%")
                                    ui.element("col").style("width:6%")
                                    ui.element("col").style("width:5%")
                            cols_ventas = [
                                ("#", "#", "text-center"),
                                ("dt", "Fecha", "text-center"),
                                ("order_id", "Order ID", "text-center"),
                                ("item_id", "ID publicación", "text-center"),
                                ("envio_tipo", "Envío", "text-center"),
                                ("tipo_venta", "Publicacion", "text-center"),
                                ("cuotas", "Cuotas", "text-center"),
                                ("tipo", "Tipo", "text-center"),
                                ("productos", "Producto", "text-center"),
                                ("cantidad", "Cant.", "text-center"),
                                ("monto", "Monto", "text-center"),
                                ("gan_pesos", "Gan $", "text-center"),
                                ("gan_vta_pct", "Gan Vta%", "text-center"),
                                ("status", "Estado", "text-center"),
                            ]
                            # Tabla header (thead solamente)
                            with _vhd:
                                with ui.element("table").style("table-layout:fixed;width:100%;border-collapse:separate;border-spacing:0"):
                                    _build_colgroup_ventas()
                                    with ui.element("thead"):
                                        with ui.element("tr").classes("bg-primary text-white font-semibold"):
                                            for col_key, h, align in cols_ventas:
                                                th_cls = f"px-2 py-2 border {align or 'text-left'}"
                                                with ui.element("th").classes(th_cls):
                                                    if col_key == "#":
                                                        ui.label(h)
                                                    else:
                                                        ui.button(h, on_click=lambda c=col_key: _on_sort_ventas(c)).props("flat dense no-caps").classes("text-white hover:bg-white/20 cursor-pointer font-semibold")
                            # Tabla body (tbody solamente, scrolleable)
                            with _vtc:
                                with ui.element("table").style("table-layout:fixed;width:100%;border-collapse:separate;border-spacing:0"):
                                    _build_colgroup_ventas()
                                    with ui.element("tbody"):
                                        for idx, v in enumerate(ventas_orden, 1):
                                            _is_cancelled_row = (v.get("status_raw") or "") in ("cancelled", "canceled")
                                            _is_rej_row = v.get("pay_status") == "rejected"
                                            _is_dev_row = bool(v.get("has_refund"))
                                            _tr_style = "color: #dc2626;" if (_is_cancelled_row or _is_rej_row or _is_dev_row) else ""
                                            with ui.element("tr").classes("border-t border-gray-200 hover:bg-gray-50").style(_tr_style):
                                                with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center text-xs"):
                                                    ui.label(str(idx))
                                                with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center text-xs"):
                                                    ui.label(f'{v["fecha"]} - {v.get("hora", "")}')
                                                with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center text-xs"):
                                                    ui.label(v.get("order_id", "—"))
                                                with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center text-xs"):
                                                    ui.label(v.get("item_id", "—"))
                                                with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center text-xs"):
                                                    _lt = (v.get("logistic_type") or "").lower().strip()
                                                    if _lt in ("self_service", "flex"):
                                                        ui.html('<span style="display:inline-flex;align-items:center;gap:4px;font-size:11px;color:#16a34a"><i class="ti ti-motorbike" style="font-size:13px" aria-hidden="true"></i>Flex</span>')
                                                    elif _lt in ("cross_docking", "xd_drop_off", "drop_off", "me1", "me2", "correo"):
                                                        ui.html('<span style="display:inline-flex;align-items:center;gap:4px;font-size:11px;color:#ea580c"><i class="ti ti-package" style="font-size:13px" aria-hidden="true"></i>Correo</span>')
                                                    elif _lt == "fulfillment":
                                                        ui.html('<span style="display:inline-flex;align-items:center;gap:4px;font-size:11px;color:#2563eb"><i class="ti ti-building-warehouse" style="font-size:13px" aria-hidden="true"></i>Full</span>')
                                                    else:
                                                        ui.label("—").classes("text-gray-400 text-xs")
                                                with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center text-xs"):
                                                    ui.label(v.get("tipo_venta", "—"))
                                                with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center text-xs"):
                                                    ui.label(v.get("cuotas", "—"))
                                                with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center text-xs"):
                                                    ui.label(v.get("tipo_display", v.get("tipo", v.get("tipo_oferta", "Regular"))))
                                                with ui.element("td").classes("px-2 py-1 border-b border-gray-100 max-w-[300px] text-xs"):
                                                    _titulo = v.get("productos", v.get("title", "—"))[:80]
                                                    ui.button(_titulo, on_click=lambda row=v: _abrir_popup_venta(row)).props("flat dense no-caps align=left").classes("text-left text-xs text-blue-600 hover:underline cursor-pointer w-full truncate")
                                                with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center text-xs"):
                                                    ui.label(str(v["cantidad"]))
                                                with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-right font-medium text-xs"):
                                                    ui.label(v["monto_fmt"])
                                                with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-right text-xs"):
                                                    _gp = v.get("gan_pesos")
                                                    _hide_gan = _is_cancelled_row or _is_rej_row or _is_dev_row
                                                    if _hide_gan or _gp is None:
                                                        ui.label("—").classes("text-gray-400 text-xs")
                                                    else:
                                                        ui.label(f"$ {_gp:,.0f}".replace(",", ".")).classes(f"font-medium text-xs {'text-positive' if _gp >= 0 else 'text-negative'}")
                                                with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-right text-xs"):
                                                    _gvp = v.get("gan_vta_pct")
                                                    if _hide_gan or _gvp is None:
                                                        ui.label("—").classes("text-gray-400 text-xs")
                                                    else:
                                                        ui.label(f"{_gvp:.2f}%".replace(".", ",")).classes(f"font-medium text-xs {'text-positive' if _gvp >= 0 else 'text-negative'}")
                                                with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center text-xs"):
                                                    ui.label(v["status"])

        def _abrir_dialog_enriquecer() -> None:
            with ui.dialog().props("persistent") as dlg, ui.card().classes("w-96"):
                ui.label("Completando datos de ventas").classes("text-base font-semibold mb-2")
                lbl_progreso = ui.label("Iniciando...").classes("text-sm text-gray-600")
                barra = ui.linear_progress(value=0, show_value=False, size="20px").props("instant-feedback").classes("w-full my-2")
            dlg.open()
            background_tasks.create(
                _enriquecer_ventas_async(context.client, dlg, lbl_progreso, barra, force=True)
            )

        def _abrir_dialog_backfill() -> None:
            from ventas_backfill import backfill_ventas_periodo

            hoy_bf = datetime.now().date()
            with ui.dialog().props("persistent") as dlg_bf, ui.card().classes("w-[420px]"):
                ui.label("Completar datos por lote").classes("text-base font-semibold mb-2")
                ui.label(
                    "Busca todas las ventas del período en la API de ML, trae costos, "
                    "comisión real y calcula ganancia. Puede tardar varios minutos si son muchas ventas."
                ).classes("text-sm text-gray-600 mb-3")
                with ui.row().classes("w-full gap-3"):
                    inp_desde = ui.input("Fecha desde", value="2026-04-17").props("type=date outlined dense").classes("flex-1")
                    inp_hasta = ui.input("Fecha hasta", value=hoy_bf.strftime("%Y-%m-%d")).props("type=date outlined dense").classes("flex-1")
                resumen_area = ui.column().classes("w-full gap-1 mt-2")
                barra_bf = ui.linear_progress(value=0, show_value=False, size="20px").props("instant-feedback").classes("w-full my-2")
                barra_bf.set_visibility(False)
                with ui.row().classes("w-full justify-end gap-2 mt-2"):
                    btn_cancelar = ui.button("Cancelar", on_click=lambda: dlg_bf.close()).props("flat no-caps")
                    btn_iniciar = ui.button("Iniciar backfill", color="primary").props("no-caps")

                async def _iniciar() -> None:
                    fd, fh = inp_desde.value, inp_hasta.value
                    if not fd or not fh:
                        ui.notify("Completá ambas fechas.", type="warning")
                        return
                    btn_iniciar.disable()
                    inp_desde.disable()
                    inp_hasta.disable()
                    barra_bf.set_visibility(True)
                    resumen_area.clear()
                    with resumen_area:
                        lbl_bf = ui.label("Buscando órdenes en MercadoLibre...").classes("text-sm text-gray-600")

                    estado = {"procesadas": 0, "total": 0, "ya_completas": 0, "errores": 0}

                    def _cb(procesadas: int, total: int, ya_completas: int, errores: int) -> None:
                        estado.update(procesadas=procesadas, total=total, ya_completas=ya_completas, errores=errores)

                    cl_bf = context.client

                    async def _poll() -> None:
                        while True:
                            await asyncio.sleep(0.5)
                            t = estado["total"]
                            with cl_bf:
                                if t > 0:
                                    barra_bf.set_value(estado["procesadas"] / t)
                                    lbl_bf.set_text(f"Procesando {estado['procesadas']} de {t}... ({estado['errores']} errores)")

                    poll_task = background_tasks.create(_poll())
                    try:
                        resultado = await run.io_bound(backfill_ventas_periodo, user["id"], fd, fh, _cb)
                    except Exception as e:
                        poll_task.cancel()
                        with cl_bf:
                            resumen_area.clear()
                            with resumen_area:
                                ui.label(f"❌ Error: {e}").classes("text-sm text-negative")
                            btn_cancelar.set_text("Cerrar")
                        return
                    poll_task.cancel()
                    with cl_bf:
                        barra_bf.set_value(1.0)
                        resumen_area.clear()
                        with resumen_area:
                            ui.label("Backfill completado.").classes("text-sm font-semibold")
                            ui.label(f"Ventas a procesar: {resultado['a_procesar']}").classes("text-sm")
                            ui.label(f"Ventas nuevas completadas: {resultado['procesadas']}").classes("text-sm text-positive")
                            ui.label(f"Ventas ya completas (saltadas): {resultado['ya_completas']}").classes("text-sm")
                            ui.label(f"Errores: {resultado['errores']}").classes("text-sm" + (" text-negative" if resultado["errores"] else ""))
                        btn_cancelar.set_text("Cerrar")
                    _cargar_ventas()

                btn_iniciar.on_click(_iniciar)
            dlg_bf.open()

        async def _enriquecer_ventas_async(
            cl,
            dlg=None,
            lbl_progreso=None,
            barra=None,
            force: bool = False,
        ) -> None:
            rows_to_enrich = [
                v for v in ventas_raw
                if (pid := (v.get("payment_id") or "")) != ""
                and (force
                     or pid not in ventas_cache_ref
                     or ventas_cache_ref.get(pid, {}).get("gan_pesos") is None
                     or (v.get("payment_type") == "account_money"
                         and float(ventas_cache_ref.get(pid, {}).get("meli_fee") or 0) == 0))
            ]
            if not rows_to_enrich:
                if dlg and lbl_progreso:
                    with cl:
                        lbl_progreso.set_text("No hay ventas para completar.")
                    await asyncio.sleep(1.5)
                    with cl:
                        dlg.close()
                return

            _uid = user["id"]
            p = params_ventas_ref
            total = len(rows_to_enrich)

            def _fetch_one(pid: str) -> Dict:
                try:
                    r = get_ml_session().get(
                        f"https://api.mercadopago.com/v1/payments/{pid}",
                        headers={"Authorization": f"Bearer {access_token}"},
                        timeout=15,
                    )
                    return r.json() if r.status_code == 200 else {}
                except Exception:
                    return {}

            def _fetch_ship(sid: str) -> Dict:
                if not sid:
                    return {}
                try:
                    r = get_ml_session().get(
                        f"https://api.mercadolibre.com/shipments/{sid}",
                        headers={"Authorization": f"Bearer {access_token}"},
                        timeout=15,
                    )
                    return r.json() if r.status_code == 200 else {}
                except Exception:
                    return {}

            def _fetch_ship_costs(sid: str) -> Dict:
                if not sid:
                    return {}
                try:
                    r = get_ml_session().get(
                        f"https://api.mercadolibre.com/shipments/{sid}/costs",
                        headers={"Authorization": f"Bearer {access_token}"},
                        timeout=15,
                    )
                    return r.json() if r.status_code == 200 else {}
                except Exception:
                    return {}

            def _compute(pay_data: Dict, v: Dict, zip_code: str = "", bonif_flex: float = 0.0) -> Optional[Dict]:
                charges     = pay_data.get("charges_details") or []
                is_rejected = pay_data.get("status") == "rejected"
                is_cancelled = (v.get("status_raw") or "") in ("cancelled", "canceled")
                has_refund_v = bool(v.get("has_refund"))
                if not charges and not is_rejected:
                    return None
                unit_price  = float(v.get("unit_price") or 0)
                cantidad    = int(v.get("cantidad") or 1)
                total_price = unit_price * cantidad
                sku = (v.get("seller_sku") or "").removeprefix("SKU ").strip()
                prod = costos_sku_ref.get(sku) if sku else None
                costo_usd = float((prod or {}).get("costo_usd") or 0)
                tipo_iva = float((prod or {}).get("tipo_iva") or 0.105)
                dolar = float(p.get("dolar_oficial") or 1475)
                ml_iibb = float(p.get("ml_iibb_per") or 0.055)
                costo_pesos = costo_usd * dolar
                total_costo = costo_pesos * cantidad
                has_calc = total_price > 0 and costo_usd > 0
                meli_fee   = sum(float((c.get("amounts") or {}).get("original", 0)) for c in charges if c.get("name") == "meli_percentage_fee")
                cuotas_fee = sum(float((c.get("amounts") or {}).get("original", 0)) for c in charges if c.get("name") == "financing_add_on_fee")
                deb_cred   = sum(float((c.get("amounts") or {}).get("original", 0)) for c in charges if "debitos_creditos" in (c.get("name") or ""))
                iibb_ret   = sum(float((c.get("amounts") or {}).get("original", 0)) for c in charges if "iibb" in (c.get("name") or "").lower())
                sirtac     = sum(float((c.get("amounts") or {}).get("original", 0)) for c in charges if "sirtac" in (c.get("name") or "").lower())
                net_rcv    = (pay_data.get("transaction_details") or {}).get("net_received_amount")
                iibb_perc  = total_price * ml_iibb
                iva_venta  = total_price * tipo_iva / (1 + tipo_iva)
                iva_meli   = meli_fee * 0.21 / 1.21
                iva_impor  = 0.09 * costo_usd * dolar * cantidad
                iva_total  = iva_venta - iva_meli - iva_impor
                shp_xd = sum(float((c.get("amounts") or {}).get("original", 0)) for c in charges if c.get("name") == "shp_cross_docking")
                buyer_shipping = float(pay_data.get("shipping_amount") or 0)
                logistic_type = v.get("logistic_type") or ""
                if logistic_type in ("self_service", "flex"):
                    _flex_t, _ = _get_flex_zona(_uid, zip_code)
                    envio_real = _flex_t
                    comprador_envio = 0.0
                elif shp_xd > 0:
                    envio_real = max(0.0, shp_xd - buyer_shipping)
                    comprador_envio = buyer_shipping
                else:
                    envio_real = float(p.get("ml_envios") or 5823)
                    comprador_envio = 0.0
                ml_env_grat_c  = float(p.get("ml_envios_gratuitos") or 33000)
                envio_efectivo = 0.0 if unit_price < ml_env_grat_c else envio_real
                gan_pesos = gan_vta_pct = gan_cos_pct = None
                if not is_rejected and not is_cancelled and not has_refund_v and has_calc:
                    gan_pesos   = total_price - meli_fee - cuotas_fee - iva_total - deb_cred - iibb_ret - sirtac - iibb_perc - envio_efectivo - total_costo + bonif_flex
                    gan_vta_pct = (gan_pesos / total_price * 100) if total_price > 0 else 0.0
                    gan_cos_pct = (gan_pesos / total_costo * 100) if total_costo > 0 else 0.0
                return {
                    "gan_pesos": gan_pesos, "gan_vta_pct": gan_vta_pct, "gan_cos_pct": gan_cos_pct,
                    "meli_fee": meli_fee, "cuotas_fee": cuotas_fee, "iva_total": iva_total,
                    "deb_cred": deb_cred, "iibb_ret": iibb_ret, "sirtac": sirtac,
                    "envio_real": envio_real, "comprador_envio": comprador_envio,
                    "logistic_type": logistic_type, "net_rcv": net_rcv,
                    "pay_status": "rejected" if is_rejected else ("cancelled" if is_cancelled else None),
                    "costo_pesos": total_costo if (not is_rejected and not is_cancelled and not has_refund_v and has_calc) else None,
                    "_skip_overwrite": is_cancelled or has_refund_v,
                }

            def _save_batch(db_rows: List[Dict]) -> None:
                conn = get_connection()
                try:
                    cur = conn.cursor()
                    for rd in db_rows:
                        if rd.get("_skip_overwrite"):
                            cur.execute(
                                "INSERT OR IGNORE INTO ventas_datos "
                                "(payment_id, user_id, order_id, gan_pesos, gan_vta_pct, gan_cos_pct, "
                                "meli_fee, cuotas_fee, iva_total, deb_cred, iibb_ret, sirtac, "
                                "envio_real, comprador_envio, logistic_type, net_rcv, fetched_at, pay_status, order_date, cuotas, "
                                "costo_pesos) "
                                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                (rd["payment_id"], rd["user_id"], rd.get("order_id"),
                                 rd.get("gan_pesos"), rd.get("gan_vta_pct"), rd.get("gan_cos_pct"),
                                 rd.get("meli_fee"), rd.get("cuotas_fee"), rd.get("iva_total"),
                                 rd.get("deb_cred"), rd.get("iibb_ret"), rd.get("sirtac"),
                                 rd.get("envio_real"), rd.get("comprador_envio"),
                                 rd.get("logistic_type"), rd.get("net_rcv"),
                                 rd.get("fetched_at"), rd.get("pay_status"), rd.get("order_date"),
                                 rd.get("cuotas"), rd.get("costo_pesos")),
                            )
                            if rd.get("pay_status"):
                                cur.execute(
                                    "UPDATE ventas_datos SET pay_status=? WHERE payment_id=? AND user_id=? AND pay_status IS NULL",
                                    (rd["pay_status"], rd["payment_id"], rd["user_id"])
                                )
                        else:
                            cur.execute(
                                "INSERT OR REPLACE INTO ventas_datos "
                                "(payment_id, user_id, order_id, gan_pesos, gan_vta_pct, gan_cos_pct, "
                                "meli_fee, cuotas_fee, iva_total, deb_cred, iibb_ret, sirtac, "
                                "envio_real, comprador_envio, logistic_type, net_rcv, fetched_at, pay_status, order_date, cuotas, "
                                "costo_pesos) "
                                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                (rd["payment_id"], rd["user_id"], rd.get("order_id"),
                                 rd.get("gan_pesos"), rd.get("gan_vta_pct"), rd.get("gan_cos_pct"),
                                 rd.get("meli_fee"), rd.get("cuotas_fee"), rd.get("iva_total"),
                                 rd.get("deb_cred"), rd.get("iibb_ret"), rd.get("sirtac"),
                                 rd.get("envio_real"), rd.get("comprador_envio"),
                                 rd.get("logistic_type"), rd.get("net_rcv"),
                                 rd.get("fetched_at"), rd.get("pay_status"), rd.get("order_date"),
                                 rd.get("cuotas"), rd.get("costo_pesos")),
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
                            "INSERT OR IGNORE INTO ventas_datos "
                            "(payment_id, user_id, order_id, fetched_at, order_date, pay_status) "
                            "VALUES (?,?,?,?,?,?)",
                            (rd["payment_id"], rd["user_id"], rd.get("order_id"),
                             rd.get("fetched_at"), rd.get("order_date"), rd.get("pay_status")),
                        )
                        if rd.get("pay_status"):
                            cur.execute(
                                "UPDATE ventas_datos SET pay_status=? WHERE payment_id=? AND user_id=? AND pay_status IS NULL",
                                (rd["pay_status"], rd["payment_id"], rd["user_id"])
                            )
                    conn.commit()
                finally:
                    conn.close()

            def _update_financiacion_cuotas(cuotas_dict: Dict[int, Dict]) -> None:
                conn = get_connection()
                try:
                    cur = conn.cursor()
                    for n_cuotas, data in cuotas_dict.items():
                        cur.execute(
                            "UPDATE financiacion_cuotas_ml SET costo_financiacion=?, fecha_modificacion=? WHERE cuotas=?",
                            (data["pct"], data["fecha"], n_cuotas)
                        )
                    conn.commit()
                finally:
                    conn.close()

            cuotas_recientes: Dict[int, Dict] = {}
            BATCH = 20
            procesadas = 0
            for i in range(0, total, BATCH):
                batch = rows_to_enrich[i : i + BATCH]

                def _fetch_batch(batch_rows: List[Dict]) -> List[tuple]:
                    max_w = min(16, len(batch_rows) * 2)
                    with ThreadPoolExecutor(max_workers=max_w) as ex:
                        pay_futs        = [(v, ex.submit(_fetch_one,        v["payment_id"])) for v in batch_rows]
                        ship_futs       = [(v, ex.submit(_fetch_ship,       v.get("shipping_id") or "")) for v in batch_rows]
                        ship_costs_futs = [
                            (v, ex.submit(_fetch_ship_costs, v.get("shipping_id") or "")
                             if v.get("logistic_type") in ("self_service", "flex") and v.get("shipping_id")
                             else ex.submit(lambda: {}))
                            for v in batch_rows
                        ]
                    results = []
                    for (v, pf), (_, sf), (_, scf) in zip(pay_futs, ship_futs, ship_costs_futs):
                        try:
                            pay_data = pf.result()
                        except Exception:
                            pay_data = {}
                        try:
                            ship_data = sf.result()
                        except Exception:
                            ship_data = {}
                        try:
                            ship_costs_data = scf.result()
                        except Exception:
                            ship_costs_data = {}
                        results.append((v, pay_data, ship_data, ship_costs_data))
                    return results

                batch_results = await run.io_bound(_fetch_batch, batch)

                db_rows: List[Dict] = []
                placeholder_rows: List[Dict] = []
                for v, pay_data, ship_data, ship_costs_data in batch_results:
                    lt = ship_data.get("logistic_type") or ""
                    if lt:
                        v["logistic_type"] = lt
                    zip_code_v = (ship_data.get("receiver_address") or {}).get("zip_code") or ""
                    _senders_b   = ship_costs_data.get("senders") or []
                    bonif_flex_b = float((_senders_b[0].get("save") if _senders_b else 0) or 0)
                    pid = v["payment_id"]
                    if not pay_data:
                        _v_sr_nodata = (v.get("status_raw") or "").lower()
                        placeholder_rows.append({
                            "payment_id": pid, "user_id": _uid, "order_id": v.get("order_id"),
                            "fetched_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                            "order_date": v["dt"].strftime("%Y-%m-%d") if v.get("dt") else None,
                            "pay_status": "cancelled" if "cancel" in _v_sr_nodata else None,
                        })
                        continue
                    # CAMBIO 3: _compute en thread para no bloquear el event loop
                    calc = await run.io_bound(_compute, pay_data, v, zip_code_v, bonif_flex_b)
                    if not calc:
                        _v_sr_nc = (v.get("status_raw") or "").lower()
                        placeholder_rows.append({
                            "payment_id": pid, "user_id": _uid, "order_id": v.get("order_id"),
                            "fetched_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                            "order_date": v["dt"].strftime("%Y-%m-%d") if v.get("dt") else None,
                            "pay_status": "cancelled" if "cancel" in _v_sr_nc else None,
                        })
                        continue
                    db_row = {
                        "payment_id": pid, "user_id": _uid, "order_id": v.get("order_id"),
                        "fetched_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                        "order_date": v["dt"].strftime("%Y-%m-%d") if v.get("dt") else None,
                        "cuotas": str(v.get("cuotas") or "x1").strip().lower(),
                        **calc,
                    }
                    db_rows.append(db_row)
                    _c_str = str(v.get("cuotas") or "x1").strip().lower()
                    _cuotas_fee = calc.get("cuotas_fee") or 0.0
                    _up = float(v.get("unit_price") or 0)
                    if _c_str.startswith("x") and len(_c_str) > 1 and _c_str[1:].isdigit():
                        _n = int(_c_str[1:])
                        if _n > 1 and _cuotas_fee > 0 and _up > 0:
                            _pct = _cuotas_fee / _up
                            _fv = v["dt"].strftime("%Y-%m-%d") if v.get("dt") else None
                            if _fv and (_n not in cuotas_recientes or _fv > cuotas_recientes[_n]["fecha"]):
                                cuotas_recientes[_n] = {"pct": _pct, "fecha": _fv}
                    v["gan_pesos"] = calc["gan_pesos"]
                    v["gan_vta_pct"] = calc["gan_vta_pct"]
                    v["gan_cos_pct"] = calc["gan_cos_pct"]
                    v["logistic_type"] = calc["logistic_type"]
                    v["pay_status"] = calc.get("pay_status")
                    if calc.get("pay_status") == "rejected" and v.get("status") not in ("Cancelada", "Devolución"):
                        v["status"] = "Cancelada"
                    ventas_cache_ref[pid] = db_row
                    procesadas += 1

                if db_rows:
                    await run.io_bound(_save_batch, db_rows)
                if placeholder_rows:
                    await run.io_bound(_save_placeholders, placeholder_rows)

                # CAMBIO 2: actualizar dialog sin llamar _pintar_tabla en cada batch
                if dlg and lbl_progreso and barra:
                    with cl:
                        _pct = round(procesadas / total * 100) if total > 0 else 0
                        lbl_progreso.set_text(f"Actualizando {procesadas} de {total} ventas... ({_pct}%)")
                        barra.set_value(procesadas / total if total > 0 else 1.0)

                # CAMBIO 4: yield al event loop entre batches
                await asyncio.sleep(0)

            # Fetch shipments para account_money (excluidos del batch principal)
            _am_ship_candidates = [
                v for v in ventas_raw
                if v.get("payment_type") == "account_money"
                and v.get("shipping_id")
                and not v.get("logistic_type")
            ]
            if _am_ship_candidates:
                def _fetch_am_ships(rows):
                    with ThreadPoolExecutor(max_workers=min(16, len(rows))) as ex:
                        futs = [(v, ex.submit(_fetch_ship, v.get("shipping_id") or "")) for v in rows]
                    for v, fut in futs:
                        try:
                            sd = fut.result()
                            lt = sd.get("logistic_type") or ""
                            if lt:
                                v["logistic_type"] = lt
                            _cost = (sd.get("shipping_option") or {}).get("list_cost") or 0.0
                            if _cost:
                                v["envio_real_am"] = float(_cost)
                        except Exception:
                            pass
                await run.io_bound(_fetch_am_ships, _am_ship_candidates)

            # Persistir logistic_type actualizado para account_money (independiente de gan_pesos)
            _lt_updates = [
                {"payment_id": v.get("payment_id"), "logistic_type": v.get("logistic_type")}
                for v in ventas_raw
                if v.get("payment_type") == "account_money"
                and v.get("payment_id")
                and v.get("logistic_type")
            ]
            if _lt_updates:
                def _update_lt(rows):
                    conn = get_connection()
                    for r in rows:
                        conn.execute(
                            "UPDATE ventas_datos SET logistic_type=? WHERE payment_id=? AND user_id=?",
                            (r["logistic_type"], r["payment_id"], _uid)
                        )
                    conn.commit()
                    conn.close()
                await run.io_bound(_update_lt, _lt_updates)

            # Estimar gan$ para account_money usando params del cotizador
            _am_rows: List[Dict] = []
            for v in ventas_raw:
                if v.get("payment_type") == "account_money" and v.get("gan_pesos") is None:
                    _up   = float(v.get("unit_price") or 0)
                    _cant = int(v.get("cantidad") or 1)
                    _sk   = str(v.get("seller_sku") or "").removeprefix("SKU ").strip()
                    _cv   = str(v.get("cuotas") or "x1").strip().lower()
                    _gp_u, _ = _calc_gan_row(_up, _sk, _cv)
                    if _gp_u is not None:
                        _env = v.get("envio_real_am") or (
                            0.0 if _up < float(p.get("ml_envios_gratuitos") or 33000)
                            else float(p.get("ml_envios") or 5823)
                        )
                        _gp = _gp_u * _cant + _env * (_cant - 1)
                        v["gan_pesos"]   = _gp
                        v["gan_vta_pct"] = (_gp / (_up * _cant) * 100) if _up > 0 else 0.0
                        _pid = v.get("payment_id") or ""
                        if _pid:
                            _am_db = {
                                "payment_id": _pid, "user_id": _uid, "order_id": v.get("order_id"),
                                "gan_pesos": _gp, "gan_vta_pct": v["gan_vta_pct"], "gan_cos_pct": None,
                                "meli_fee": 0.0, "cuotas_fee": 0.0, "iva_total": 0.0,
                                "deb_cred": 0.0, "iibb_ret": 0.0, "sirtac": 0.0,
                                "envio_real": _env, "logistic_type": v.get("logistic_type") or "", "net_rcv": None,
                                "fetched_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                                "pay_status": None,
                                "order_date": v["dt"].strftime("%Y-%m-%d") if v.get("dt") else None,
                            }
                            ventas_cache_ref[_pid] = _am_db
                            _am_rows.append(_am_db)
            if _am_rows:
                await run.io_bound(_save_batch, _am_rows)

            if cuotas_recientes:
                try:
                    await run.io_bound(_update_financiacion_cuotas, cuotas_recientes)
                    _tasas_msg = ", ".join(
                        f"{n}x={round(d['pct'] * 100, 1)}%"
                        for n, d in sorted(cuotas_recientes.items())
                    )
                    with cl:
                        ui.notify(f"Tasas de financiación actualizadas: {_tasas_msg}", type="positive")
                except Exception:
                    pass

            # Al terminar: mostrar resultado, esperar 1.5s y cerrar automáticamente
            if dlg and lbl_progreso:
                with cl:
                    lbl_progreso.set_text(f"Completado: {procesadas} ventas actualizadas. (100%)")
                    if barra:
                        barra.set_value(1.0)
                await asyncio.sleep(1.5)
                with cl:
                    dlg.close()
                    _pintar_tabla()
            else:
                # Llamada sin dialog (carga inicial automática): _pintar_tabla al terminar
                with cl:
                    _pintar_tabla()

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
                        filtro_controls_ref[0].set_visibility(not is_mobile_ref.get("val"))
                    return
                hoy = datetime.now().date()
                fecha_val = filtro_fecha_ref.get("val", "hoy")
                date_ini, date_fin = _rango_desde_filtro(fecha_val, hoy)
                date_from = date_ini.strftime("%Y-%m-%dT00:00:00.000-03:00")
                date_to   = date_fin.strftime("%Y-%m-%dT23:59:59.999-03:00")
                orders_data = await run.io_bound(
                    ml_get_orders, access_token, str(seller_id), limit=2000, offset=0,
                    date_from=date_from, date_to=date_to,
                )
            except Exception as e:
                result_area.clear()
                with result_area:
                    ui.label(f"❌ Error al cargar ventas: {e}").classes("text-negative")
                if filtro_controls_ref:
                    filtro_controls_ref[0].set_visibility(not is_mobile_ref.get("val"))
                return
            raw_orders = orders_data.get("results") or orders_data.get("orders") or orders_data.get("elements") or []
            orders = [o for o in raw_orders if isinstance(o, dict)]
            all_orders_ref["orders"] = orders
            orders_periodo = [o for o in orders if _order_in_range(o, date_ini, date_fin)]
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
            # Cargar cotizador params y costos para cálculo Gan$
            def _vp_parse_rate(s) -> float:
                if s is None or s == "": return 0.0
                try:
                    v = float(str(s).strip().replace(",", "."))
                    return v if v <= 1.5 else v / 100.0
                except (ValueError, TypeError): return 0.0
            def _vp_parse_float(s) -> float:
                if s is None or s == "": return 0.0
                try:
                    return float(str(s).replace(".", "").replace(",", ".").strip() or 0)
                except (ValueError, TypeError): return 0.0
            _uid_v = user["id"]
            params_ventas_ref.update({
                "dolar_oficial": _vp_parse_float(get_cotizador_param("dolar_oficial", _uid_v) or "1475") or 1475.0,
                "ml_comision": _vp_parse_rate(get_cotizador_param("ml_comision", _uid_v) or "0.15"),
                "ml_debcre": _vp_parse_rate(get_cotizador_param("ml_debcre", _uid_v) or "0.006"),
                "ml_iibb_per": _vp_parse_rate(get_cotizador_param("ml_iibb_per", _uid_v) or "0.055"),
                "ml_envios": _vp_parse_float(get_cotizador_param("ml_envios", _uid_v) or "5823") or 5823.0,
                "ml_envios_gratuitos": _vp_parse_float(get_cotizador_param("ml_envios_gratuitos", _uid_v) or "33000") or 33000.0,
                "cuotas_3x": _vp_parse_rate(get_cotizador_param("cuotas_3x", _uid_v) or "0.094"),
                "cuotas_6x": _vp_parse_rate(get_cotizador_param("cuotas_6x", _uid_v) or "0.151"),
                "cuotas_9x": _vp_parse_rate(get_cotizador_param("cuotas_9x", _uid_v) or "0.207"),
                "cuotas_12x": _vp_parse_rate(get_cotizador_param("cuotas_12x", _uid_v) or "0.259"),
            })
            _all_skus_v = [s for s in item_id_to_sku.values() if s]
            if _all_skus_v:
                _conn_v = get_connection()
                try:
                    _cur_v = _conn_v.cursor()
                    _ph_v = ",".join("?" * len(_all_skus_v))
                    _cur_v.execute(
                        f"SELECT sku, costo_usd, tipo_iva FROM productos WHERE user_id = ? AND sku IN ({_ph_v})",
                        [_uid_v] + _all_skus_v,
                    )
                    for _r_v in _cur_v.fetchall():
                        costos_sku_ref[_r_v["sku"]] = {"costo_usd": float(_r_v["costo_usd"] or 0), "tipo_iva": float(_r_v["tipo_iva"] or 0.105)}
                finally:
                    _conn_v.close()
            ventas_mes: List[Dict[str, Any]] = []
            status_map = {"paid": "Concretada", "handling": "En preparación", "shipped": "Enviada", "delivered": "Entregada", "cancelled": "Cancelada", "canceled": "Cancelada"}
            dia_ini, dia_fin = date_ini, date_fin
            for ord_item in orders_periodo:
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
                _dt_date = dt.date() if isinstance(dt, datetime) else dt
                if dia_ini is not None and (_dt_date < dia_ini or _dt_date > dia_fin):
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
                    _pays_r2 = ord_item.get("payments") or []
                    _p_ap_r2 = next((p for p in _pays_r2 if str(p.get("status", "")).lower() == "approved"), None) or (_pays_r2[0] if _pays_r2 else None)
                    _payment_id = str(_p_ap_r2.get("id") or "") if _p_ap_r2 else ""
                    _payment_type = str(_p_ap_r2.get("payment_type") or "") if _p_ap_r2 else ""
                    dt = dt + timedelta(hours=1)
                    ventas_mes.append({
                        "dt": dt,
                        "fecha": dt.strftime("%d/%m/%Y"),
                        "hora": dt.strftime("%H:%M"),
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
                        "unit_price": unit_price,
                        "seller_sku": sku,
                        "order_id": str(ord_item.get("id", "") or ""),
                        "logistic_type": "",
                        "shipping_id": str((ord_item.get("shipping") or {}).get("id") or ""),
                        "buyer": (ord_item.get("buyer") or {}).get("nickname", "—"),
                        "payment_id": _payment_id,
                        "payment_type": _payment_type,
                        "gan_pesos": None,
                        "gan_vta_pct": None,
                        "has_refund": has_refund,
                    })
            def _insert_placeholders_bulk(rows: List[Dict]) -> None:
                if not rows:
                    return
                conn = get_connection()
                try:
                    cur = conn.cursor()
                    now_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                    for v in rows:
                        pid = v.get("payment_id") or ""
                        if not pid:
                            continue
                        order_date = v["dt"].strftime("%Y-%m-%d") if v.get("dt") else None
                        cur.execute(
                            "INSERT OR IGNORE INTO ventas_datos "
                            "(payment_id, user_id, order_id, fetched_at, order_date) "
                            "VALUES (?,?,?,?,?)",
                            (pid, _uid_v, v.get("order_id"), now_str, order_date),
                        )
                    conn.commit()
                finally:
                    conn.close()
            await run.io_bound(_insert_placeholders_bulk, ventas_mes)
            def _load_ventas_cache(uid: int) -> Dict[str, Dict]:
                conn = get_connection()
                try:
                    cur = conn.cursor()
                    cur.execute(
                        "SELECT payment_id, gan_pesos, gan_vta_pct, gan_cos_pct, meli_fee, cuotas_fee, "
                        "iva_total, deb_cred, iibb_ret, sirtac, envio_real, logistic_type, net_rcv, pay_status "
                        "FROM ventas_datos WHERE user_id=?",
                        (uid,)
                    )
                    return {r["payment_id"]: dict(r) for r in cur.fetchall()}
                finally:
                    conn.close()
            _new_cache = await run.io_bound(_load_ventas_cache, _uid_v)
            ventas_cache_ref.clear()
            ventas_cache_ref.update(_new_cache)
            for v in ventas_mes:
                pid = v.get("payment_id") or ""
                if pid and pid in ventas_cache_ref:
                    c = ventas_cache_ref[pid]
                    v["gan_pesos"] = c.get("gan_pesos")
                    v["gan_vta_pct"] = c.get("gan_vta_pct")
                    v["gan_cos_pct"] = c.get("gan_cos_pct")
                    v["pay_status"] = c.get("pay_status")
                    v["logistic_type"] = c.get("logistic_type") or v.get("logistic_type") or ""
                    if c.get("pay_status") == "rejected" and v.get("status") not in ("Cancelada", "Devolución"):
                        v["status"] = "Cancelada"
            ventas_raw = ventas_mes
            if filtro_controls_ref:
                filtro_controls_ref[0].set_visibility(not is_mobile_ref.get("val"))
            _pintar_tabla()
            _enrich_cl = context.client
            background_tasks.create(_enriquecer_ventas_async(_enrich_cl), name="enriquecer_ventas")

        filtro_controls_ref: List[Any] = []  # Referencia al row de controles para mostrar/ocultar

        with filtro_row:
            filtro_controls = ui.row().classes("items-center gap-4")
            filtro_controls.set_visibility(False)
            filtro_controls_ref.append(filtro_controls)
            with filtro_controls:
                _meses_es = ["Enero","Febrero","Marzo","Abril","Mayo","Junio",
                             "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"]
                _hoy_opts = datetime.now().date()
                _oy, _om = _hoy_opts.year, _hoy_opts.month
                _opciones_fecha = {
                    "hoy": "Hoy",
                    "dias_2": "Últimos 2 días",
                    "dias_3": "Últimos 3 días",
                    "dias_5": "Últimos 5 días",
                    "dias_7": "Últimos 7 días",
                    "dias_15": "Últimos 15 días",
                    "dias_21": "Últimos 21 días",
                    "dias_30": "Últimos 30 días",
                    "mes_actual": "Mes actual",
                }
                for _i in range(1, 7):
                    _om -= 1
                    if _om == 0:
                        _om = 12
                        _oy -= 1
                    _opciones_fecha[f"mes_{_i}"] = f"{_meses_es[_om - 1]} {_oy}"
                filtro_fecha = ui.select(
                    _opciones_fecha,
                    value=filtro_fecha_ref.get("val", "hoy"),
                    label="Fecha",
                ).classes("w-48").bind_value(filtro_fecha_ref, "val")
                filtro_fecha.on_value_change(lambda: _aplicar_filtro_fecha())
                filtro_cuotas = ui.select(
                    {"todas": "Todas", "x1": "x1", "x3": "x3", "x6": "x6", "x9": "x9", "x12": "x12"},
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
                filtro_envio = ui.select(
                    {"todos": "Todos", "correo": "Correo", "flex": "Flex"},
                    value=filtro_envio_ref.get("val", "todos"),
                    label="Envíos",
                ).classes("w-28").bind_value(filtro_envio_ref, "val")
                filtro_envio.on_value_change(lambda: _pintar_tabla())
                filtro_texto = ui.input(placeholder="Buscar producto...").props("outlined dense clearable").classes("w-64")
                filtro_texto.bind_value(filtro_texto_ref, "val")
                filtro_texto.on_value_change(lambda: _pintar_tabla())

        _cargar_ventas()
