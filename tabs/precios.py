"""
Fase 3 — tabs/precios.py
Pestaña Productos: tabla de precios con edición inline y calculadora de márgenes.
Funciones exportadas: build_tab_precios, _show_item_detail_dialog (usada en precios_detalle.py)
"""
from __future__ import annotations

import asyncio
import logging
import math
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests
from nicegui import app, background_tasks, context, run, ui

from db import get_connection, get_cotizador_param, COTIZADOR_DEFAULTS
from ml_api import (
    get_ml_access_token,
    _cuotas_desde_item,
    ml_get_my_items,
    ml_update_item_price,
    ml_get_item_sale_price_full,
    ml_get_item_price_to_win,
    ml_get_item_performance,
    ml_get_items_multiget_with_attributes,
    ml_get_orders,
)
from tabs.cuotas import _cuotas_key


# ---------------------------------------------------------------------------
# Helper de sesión (mismo patrón que otros tabs; se unificará en auth.py Fase 4)
# ---------------------------------------------------------------------------

def _require_login() -> Optional[Dict[str, Any]]:
    user = app.storage.user.get("user")
    if not user:
        ui.notify("Debes iniciar sesión para continuar", color="negative")
    return user


# ---------------------------------------------------------------------------
# Funciones exportadas
# ---------------------------------------------------------------------------

def build_tab_precios(container) -> None:
    """Pestaña Productos: clic en el cuadradito de la fila para editar precio."""
    container.clear()
    user = _require_login()
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
            with ui.row().classes("w-full justify-between py-0.5 gap-4"):
                ui.label("Comisión ML").classes("text-sm font-medium text-gray-600")
                ui.label(fmt_moneda(data.get("comision"))).classes("text-sm text-negative")
            with ui.row().classes("w-full justify-between py-0.5 gap-4"):
                ui.label("Costo Cuotas").classes("text-sm font-medium text-gray-600")
                ui.label(fmt_moneda(data.get("costo_cuotas"))).classes("text-sm text-negative")
            with ui.row().classes("w-full justify-between py-0.5 gap-4"):
                ui.label("IVA neto").classes("text-sm font-medium text-gray-600")
                ui.label(fmt_moneda(data.get("iva_total"))).classes("text-sm text-negative")
            with ui.column().classes("w-full bg-gray-50 rounded px-2 py-1 mb-0.5 gap-0"):
                for lbl_s, key_s in [
                    ("IVA venta",              "iva_venta"),
                    ("IVA Meli (crédito)",     "iva_meli"),
                    ("IVA importación (créd)", "iva_impor"),
                ]:
                    with ui.row().classes("w-full justify-between"):
                        ui.label(lbl_s).classes("text-xs font-medium text-gray-600")
                        ui.label(fmt_moneda(data.get(key_s))).classes("text-xs text-gray-600")
            with ui.row().classes("w-full justify-between py-0.5 gap-4"):
                ui.label("Deb/Cred").classes("text-sm font-medium text-gray-600")
                ui.label(fmt_moneda(data.get("deb_cred"))).classes("text-sm text-negative")
            with ui.row().classes("w-full justify-between py-0.5 gap-4"):
                ui.label("IIBB ret.").classes("text-sm font-medium text-gray-600")
                ui.label(fmt_moneda(data.get("iibb"))).classes("text-sm text-negative")
            with ui.row().classes("w-full justify-between py-0.5 gap-4 border-b-2 border-gray-300"):
                ui.label("Envío promedio Flex/Correo").classes("text-sm font-medium text-gray-600")
                ui.label(fmt_moneda(data.get("envio"))).classes("text-sm text-negative")
            with ui.row().classes("w-full justify-between py-1 gap-4"):
                ui.label("Gan $").classes("text-sm font-medium text-gray-600")
                ui.label(fmt_moneda(data.get("margen_pesos"))).classes(mcls)
            with ui.row().classes("w-full justify-between py-0.5 gap-4"):
                ui.label("Gan Vta %").classes("text-sm font-medium text-gray-600")
                ui.label(fmt_pct2(data.get("margen_venta_pct"))).classes(mcls)
            with ui.row().classes("w-full justify-between py-0.5 gap-4"):
                ui.label("Gan % Cos").classes("text-sm font-medium text-gray-600")
                ui.label(fmt_pct2(data.get("margen_costo_pct"))).classes(mcls)
            ui.separator()
            with ui.row().classes("items-center gap-1 text-xs").style("color: var(--color-text-secondary)"):
                ui.html('<i class="ti ti-calculator" style="font-size:12px;color:#BA7517" aria-hidden="true"></i>')
                ui.label("Valor estimado")

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
                    ui.label(f"{row.get('id', '')}  ''  {sku_txt}").classes("text-sm font-mono text-gray-600")
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
                    ui.label("Precio de Venta").classes("text-sm font-medium text-gray-600")
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

    _new_skus = [s for s in _skus_dedup if s not in _prod_map]
    if _new_skus:
        _now_create = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        _conn_ins = get_connection()
        try:
            _conn_ins.executemany(
                "INSERT OR IGNORE INTO productos (sku, user_id, created_at, updated_at) VALUES (?, ?, ?, ?)",
                [(s, _uid, _now_create, _now_create) for s in _new_skus],
            )
            _conn_ins.commit()
            for s in _new_skus:
                _prod_map[s] = {"costo_usd": None, "fob_usd": None, "tipo_iva": 0.105, "price_updated_at": None}
        finally:
            _conn_ins.close()

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
                return "''"
            try:
                dt = datetime.strptime(s[:10], "%Y-%m-%d")
                return dt.strftime("%d/%m/%Y")
            except Exception:
                return str(s)[:10] if s else "''"

        last_upd = i.get("last_updated")
        raw_fecha = last_upd[:10] if last_upd and isinstance(last_upd, str) and len(last_upd) >= 10 else None
        ult_modif_fmt = _fmt_fecha(raw_fecha) if raw_fecha else "''"
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
            "marca": i.get("marca") or "''",
            "color": i.get("color") or "''",
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

    _gan_rows = [
        (row["margen_pesos"], row.get("margen_venta_pct"), row.get("available_quantity"),
         str(row.get("title") or ""), row.get("seller_sku"))
        for row in items_loaded
        if row.get("margen_pesos") is not None and row.get("seller_sku")
    ]
    if _gan_rows:
        _now_gan = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        _conn_gan = get_connection()
        try:
            _conn_gan.executemany(
                "UPDATE productos SET gan_pesos=?, gan_pct=?, stock=?, nombre=?, updated_at=? WHERE sku=? AND user_id=?",
                [(g[0], g[1], g[2], g[3], _now_gan, g[4], _uid) for g in _gan_rows],
            )
            _conn_gan.commit()
        finally:
            _conn_gan.close()

    _no_costo_rows = [
        (row.get("available_quantity"), str(row.get("title") or ""), row.get("seller_sku"))
        for row in items_loaded
        if row.get("margen_pesos") is None and row.get("seller_sku")
    ]
    if _no_costo_rows:
        _now_nombre = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        _conn_nombre = get_connection()
        try:
            _conn_nombre.executemany(
                "UPDATE productos SET stock=?, nombre=COALESCE(NULLIF(nombre,''),?), updated_at=? WHERE sku=? AND user_id=?",
                [(n[0], n[1], _now_nombre, n[2], _uid) for n in _no_costo_rows],
            )
            _conn_nombre.commit()
        finally:
            _conn_nombre.close()

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

    _cs_rows = [
        (r.get("catalog_status"), r.get("seller_sku"))
        for r in items_loaded
        if r.get("seller_sku")
    ]
    if _cs_rows:
        _now_cs = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        _conn_cs = get_connection()
        try:
            _conn_cs.executemany(
                "UPDATE productos SET catalog_status=?, updated_at=? WHERE sku=? AND user_id=?",
                [(cs, _now_cs, sku, _uid) for cs, sku in _cs_rows],
            )
            _conn_cs.commit()
        finally:
            _conn_cs.close()

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
            "listed":              ("''", "Publicado sin ganar",   "text-gray-500"),
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
                        ui.label(f"{row.get('id','')}  ''  {sku_txt}").classes("text-xs font-mono text-gray-500")
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
    todos_items_ref: List[Dict[str, Any]] = list(items_loaded)
    seller_id_ref: str = str(data.get("seller_id") or "")
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

    def _generar_pdf_stock(filtrados_actuales: List[Dict[str, Any]], include_ventas: bool = False) -> Optional[str]:
        """Genera un PDF A4 con columnas SKU/Marca/Producto/Color/Stock[/Ventas], ordenado por Marca+Producto."""
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
        _col_sku_pts  = 3.0 * rl_cm - 8
        _col_prod_pts = (10.0 if include_ventas else 11.6) * rl_cm - 12

        def _trunc(s):
            if not s or s == "''":
                return s or "''"
            if _sw(s, "Helvetica", 7) <= _col_prod_pts:
                return s
            while len(s) > 0 and _sw(s + "...", "Helvetica", 7) > _col_prod_pts:
                s = s[:-1]
            return (s + "...") if s else "..."

        headers = ['SKU', 'Marca', 'Producto', 'Color', 'Stock']
        if include_ventas:
            headers.append('Ventas')
        data = [headers]
        sku_fontsizes = []
        for r in rows_sorted:
            stock_val = r.get('available_quantity')
            stock_str = fmt_miles(stock_val) if stock_val is not None else '0'
            sku_str = str(r.get('seller_sku') or r.get('id') or '')
            _sku_w = _sw(sku_str, 'Helvetica', 7)
            _sku_fs = 7 if _sku_w <= _col_sku_pts else max(5, round(7 * _col_sku_pts / _sku_w, 1))
            sku_fontsizes.append(_sku_fs)
            row_cells = [
                sku_str,
                str(r.get('marca') or ''),
                _trunc(str(r.get('title') or '')),
                str(r.get('color') or ''),
                stock_str,
            ]
            if include_ventas:
                row_cells.append(str(r.get('sold_quantity') or '0'))
            data.append(row_cells)

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
                self.drawString(margin_lr, page_h - margin + 4, f"{'Ventas' if include_ventas else 'Stock'} {_fecha}")
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

        if include_ventas:
            col_widths = [3.0 * rl_cm, 1.6 * rl_cm, 10.0 * rl_cm, 2.0 * rl_cm, 1.2 * rl_cm, 1.6 * rl_cm]
        else:
            col_widths = [3.0 * rl_cm, 1.6 * rl_cm, 11.6 * rl_cm, 2.0 * rl_cm, 1.2 * rl_cm]

        table = Table(data, colWidths=col_widths, repeatRows=1)

        BLUE = rl_colors.HexColor("#1976d2")
        LIGHT_GRAY = rl_colors.HexColor("#f8f8f8")

        ts = TableStyle([
            ("BACKGROUND",   (0, 0), (-1,  0), BLUE),
            ("TEXTCOLOR",    (0, 0), (-1,  0), rl_colors.white),
            ("FONTNAME",     (0, 0), (-1,  0), "Helvetica-Bold"),
            ("FONTSIZE",     (0, 0), (-1,  0), 8),
            ("ALIGN",        (0, 0), (-1,  0), "CENTER"),
            ("VALIGN",       (0, 0), (-1, -1), "MIDDLE"),
            ("FONTNAME",     (0, 1), (-1, -1), "Helvetica"),
            ("FONTSIZE",     (0, 1), (-1, -1), 7),
            ("ALIGN",        (0, 1), ( 3, -1), "LEFT"),
            ("ALIGN",        (4, 1), (-1, -1), "RIGHT"),
            ("GRID",         (0, 0), (-1, -1), 0.5, rl_colors.HexColor("#dddddd")),
            ("TOPPADDING",   (0, 0), (-1, -1), 2),
            ("BOTTOMPADDING",(0, 0), (-1, -1), 2),
            ("ROWBACKGROUND",(0, 1), (-1, -1), [LIGHT_GRAY, rl_colors.white]),
        ])
        table.setStyle(ts)
        _sku_extra = [("FONTSIZE", (0, i + 1), (0, i + 1), fs) for i, fs in enumerate(sku_fontsizes) if fs < 7]
        if _sku_extra:
            table.setStyle(TableStyle(_sku_extra))

        try:
            doc.build([table], canvasmaker=_PaginatedCanvas)
            return tmp.name
        except Exception:
            try:
                os.unlink(tmp.name)
            except Exception:
                pass
            return None

    def _generar_pdf_stock_usd(filtrados_actuales) -> None:
        """Genera PDF A4 de valuacion de stock en u$: SKU/Marca/Producto/Stock/Costo u$/Subtotal u$."""
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
        _col_sku_pts  = 3.0 * rl_cm - 8
        _col_prod_pts = 9.0 * rl_cm - 12

        def _trunc_usd(s):
            if not s:
                return s or ""
            if _sw(s, "Helvetica", 7) <= _col_prod_pts:
                return s
            while len(s) > 0 and _sw(s + "...", "Helvetica", 7) > _col_prod_pts:
                s = s[:-1]
            return (s + "...") if s else "..."

        headers = ["SKU", "Marca", "Producto", "Stock", "Costo u$", "Subtotal u$"]
        data = [headers]
        sku_fontsizes = []
        total_subtotal_usd = 0.0
        for r in rows_sorted:
            stock_val = float(r.get("available_quantity") or 0)
            stock_str = fmt_miles(int(stock_val)) if stock_val is not None else "0"
            sku_str = str(r.get("seller_sku") or r.get("id") or "")
            _sku_w = _sw(sku_str, "Helvetica", 7)
            _sku_fs = 7 if _sku_w <= _col_sku_pts else max(5, round(7 * _col_sku_pts / _sku_w, 1))
            sku_fontsizes.append(_sku_fs)
            costo_usd_base = float(r.get("costo_usd") or 0)
            iva_frac = float(r.get("tipo_iva") or 0)
            costo_usd = costo_usd_base * (1 + iva_frac)
            subtotal_usd = costo_usd * stock_val
            total_subtotal_usd += subtotal_usd
            data.append([
                sku_str,
                str(r.get("marca") or ""),
                _trunc_usd(str(r.get("title") or "")),
                stock_str,
                f"u$ {costo_usd:.2f}",
                f"u$ {subtotal_usd:.2f}",
            ])

        n_productos = len(rows_sorted)
        summary_idx = len(data)
        data.append([f"{n_productos} productos", "", "", "", "Total u$", f"u$ {total_subtotal_usd:.2f}"])

        tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
        tmp.close()

        page_w, page_h = A4
        margin    = 1.5 * rl_cm
        margin_lr = 0.8 * rl_cm
        margin_b  = 0.5 * rl_cm

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
                self.drawString(margin_lr, page_h - margin + 4, f"Stock $ {_fecha}")
                self.setFont("Helvetica", 9)
                self.setFillColorRGB(0.4, 0.4, 0.4)
                self.drawRightString(page_w - margin_lr, page_h - margin + 4,
                                     f"Pagina {self._pageNumber} de {page_count}")
                self.restoreState()

        doc = SimpleDocTemplate(
            tmp.name,
            pagesize=A4,
            leftMargin=margin_lr,
            rightMargin=margin_lr,
            topMargin=margin + 0.9 * rl_cm,
            bottomMargin=margin_b,
        )

        col_widths = [3.0 * rl_cm, 1.8 * rl_cm, 9.0 * rl_cm, 1.4 * rl_cm, 2.1 * rl_cm, 2.1 * rl_cm]

        table = Table(data, colWidths=col_widths, repeatRows=1)

        BLUE       = rl_colors.HexColor("#1976d2")
        LIGHT_GRAY = rl_colors.HexColor("#f8f8f8")
        SUMMARY_BG = rl_colors.HexColor("#e3f2fd")

        ts = TableStyle([
            ("BACKGROUND",    (0, 0), (-1,  0), BLUE),
            ("TEXTCOLOR",     (0, 0), (-1,  0), rl_colors.white),
            ("FONTNAME",      (0, 0), (-1,  0), "Helvetica-Bold"),
            ("FONTSIZE",      (0, 0), (-1,  0), 8),
            ("ALIGN",         (0, 0), (-1,  0), "CENTER"),
            ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
            ("FONTNAME",      (0, 1), (-1, -1), "Helvetica"),
            ("FONTSIZE",      (0, 1), (-1, -1), 7),
            ("ALIGN",         (0, 1), ( 2, -1), "LEFT"),
            ("ALIGN",         (3, 1), (-1, -1), "RIGHT"),
            ("GRID",          (0, 0), (-1, -1), 0.5, rl_colors.HexColor("#dddddd")),
            ("TOPPADDING",    (0, 0), (-1, -1), 2),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
            ("ROWBACKGROUND", (0, 1), (-1, summary_idx - 1), [LIGHT_GRAY, rl_colors.white]),
            ("LINEABOVE",     (0, summary_idx), (-1, summary_idx), 1.5, BLUE),
            ("BACKGROUND",    (0, summary_idx), (-1, summary_idx), SUMMARY_BG),
            ("FONTNAME",      (0, summary_idx), (-1, summary_idx), "Helvetica-Bold"),
            ("FONTSIZE",      (0, summary_idx), (-1, summary_idx), 8),
            ("SPAN",          (0, summary_idx), (3, summary_idx)),
            ("ALIGN",         (0, summary_idx), (3, summary_idx), "LEFT"),
            ("ALIGN",         (4, summary_idx), (-1, summary_idx), "RIGHT"),
        ])
        table.setStyle(ts)
        _sku_extra = [("FONTSIZE", (0, i + 1), (0, i + 1), fs) for i, fs in enumerate(sku_fontsizes) if fs < 7]
        if _sku_extra:
            table.setStyle(TableStyle(_sku_extra))

        try:
            doc.build([table], canvasmaker=_PaginatedCanvas)
            return tmp.name
        except Exception:
            try:
                os.unlink(tmp.name)
            except Exception:
                pass
            return None

    def _generar_pdf_compras(todos_items: List[Dict[str, Any]], token: str, sid: str) -> Optional[str]:
        """Genera PDF A4 de compras sugeridas: SKU/Marca/Producto/Stock/Ventas 90d/Ventas día/Compra 15d."""
        log = logging.getLogger(__name__)
        try:
            from reportlab.lib.pagesizes import A4
            from reportlab.platypus import SimpleDocTemplate, Table, TableStyle
            from reportlab.lib import colors as rl_colors
            from reportlab.lib.units import cm as rl_cm
            from reportlab.pdfgen import canvas as rl_canvas
        except ImportError:
            return None

        if not todos_items or not token or not sid:
            return None

        # Mapa item_id ML → datos del item
        item_map: Dict[str, Dict[str, Any]] = {}
        sku_info: Dict[str, Dict[str, Any]] = {}
        for it in todos_items:
            item_id = str(it.get("id") or "").strip()
            sku = str(it.get("seller_sku") or "").strip()
            if item_id:
                item_map[item_id] = it
            if sku and sku not in sku_info:
                sku_info[sku] = {
                    "sku":   sku,
                    "marca": str(it.get("marca") or ""),
                    "title": str(it.get("title") or ""),
                    "stock": int(it.get("available_quantity") or 0),
                }

        # Obtener órdenes de los últimos 90 días
        date_from = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%dT00:00:00.000-03:00")
        try:
            orders_resp = ml_get_orders(token, sid, limit=5000, date_from=date_from)
        except Exception:
            return None
        orders = orders_resp.get("results") or []
        log.warning("PDF Compras: %d órdenes en 90d (desde %s)", len(orders), date_from[:10])

        # Acumular unidades vendidas por SKU
        sku_ventas: Dict[str, int] = {}
        _sin_item_map = 0
        for order in orders:
            if not isinstance(order, dict):
                continue
            order_items = order.get("order_items") or order.get("items") or []
            for oit in order_items:
                if not isinstance(oit, dict):
                    continue
                obj = oit.get("item") or oit
                qty = int(oit.get("quantity") or oit.get("qty") or 0)
                if qty <= 0:
                    continue
                item_id = (
                    str(obj.get("id") or oit.get("item_id") or "") if isinstance(obj, dict)
                    else str(oit.get("item_id") or "")
                ).strip()
                if not item_id:
                    continue
                it_data = item_map.get(item_id)
                if not it_data:
                    # Fallback: ML incluye seller_custom_field en order_items
                    _sin_item_map += 1
                    sku_fb = ""
                    if isinstance(obj, dict):
                        sku_fb = str(obj.get("seller_custom_field") or obj.get("seller_sku") or "").strip()
                    if not sku_fb:
                        sku_fb = str(oit.get("seller_custom_field") or "").strip()
                    if sku_fb and sku_fb in sku_info:
                        sku_ventas[sku_fb] = sku_ventas.get(sku_fb, 0) + qty
                    continue
                sku = str(it_data.get("seller_sku") or "").strip()
                if not sku:
                    continue
                sku_ventas[sku] = sku_ventas.get(sku, 0) + qty

        log.warning("PDF Compras: %d SKUs con ventas, %d order_items sin match en item_map", len(sku_ventas), _sin_item_map)

        if not sku_ventas:
            return None

        _EXCLUDE_WORDS = {"caja", "cabierta", "cajaabierta", "abierto", "devolucion"}

        def _sku_excluido(s: str) -> bool:
            normalized = s.lower().replace(".", "").replace("_", "")
            return any(w in normalized for w in _EXCLUDE_WORDS)

        # Calcular filas del reporte
        rows_data = []
        for sku, ventas_90d in sku_ventas.items():
            if _sku_excluido(sku):
                continue
            info = sku_info.get(sku)
            if not info:
                continue
            stock = info["stock"]
            ventas_dia = ventas_90d / 90.0
            compra_15d = math.ceil(ventas_dia * 15 - stock)
            rows_data.append({
                "sku":        sku,
                "marca":      info["marca"],
                "title":      info["title"],
                "stock":      stock,
                "ventas_90d": ventas_90d,
                "ventas_dia": ventas_dia,
                "compra_15d": compra_15d,
            })

        if not rows_data:
            return None

        rows_sorted = sorted(rows_data, key=lambda x: ((x["marca"] or "").lower(), (x["title"] or "").lower()))

        ahora = datetime.now()
        _fecha = f"{ahora.day:02d}/{ahora.month:02d}/{ahora.year}"

        from reportlab.pdfbase.pdfmetrics import stringWidth as _sw
        _col_sku_pts  = 2.8 * rl_cm - 8
        _col_prod_pts = 7.8 * rl_cm - 12

        def _trunc_c(s):
            if not s:
                return s or ""
            if _sw(s, "Helvetica", 7) <= _col_prod_pts:
                return s
            while len(s) > 0 and _sw(s + "...", "Helvetica", 7) > _col_prod_pts:
                s = s[:-1]
            return (s + "...") if s else "..."

        headers = ["SKU", "Marca", "Producto", "Stock", "Ventas 90d", "Ventas/día", "Compra"]
        data_rows = [headers]
        sku_fontsizes = []
        for r in rows_sorted:
            sku_str = r["sku"]
            _sku_w = _sw(sku_str, "Helvetica", 7)
            _sku_fs = 7 if _sku_w <= _col_sku_pts else max(5, round(7 * _col_sku_pts / _sku_w, 1))
            sku_fontsizes.append(_sku_fs)
            data_rows.append([
                sku_str,
                str(r["marca"]),
                _trunc_c(str(r["title"])),
                fmt_miles(r["stock"]),
                fmt_miles(r["ventas_90d"]),
                f"{r['ventas_dia']:.2f}",
                str(r["compra_15d"]),
            ])

        tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
        tmp.close()

        page_w, page_h = A4
        margin    = 1.5 * rl_cm
        margin_lr = 0.8 * rl_cm
        margin_b  = 0.5 * rl_cm

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
                self.drawString(margin_lr, page_h - margin + 4, f"Compras {_fecha}")
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

        col_widths = [2.8 * rl_cm, 2.0 * rl_cm, 7.8 * rl_cm, 1.4 * rl_cm, 2.0 * rl_cm, 2.0 * rl_cm, 1.4 * rl_cm]

        table = Table(data_rows, colWidths=col_widths, repeatRows=1)

        BLUE       = rl_colors.HexColor("#1976d2")
        LIGHT_GRAY = rl_colors.HexColor("#f8f8f8")

        ts = TableStyle([
            ("BACKGROUND",    (0, 0), (-1,  0), BLUE),
            ("TEXTCOLOR",     (0, 0), (-1,  0), rl_colors.white),
            ("FONTNAME",      (0, 0), (-1,  0), "Helvetica-Bold"),
            ("FONTSIZE",      (0, 0), (-1,  0), 8),
            ("ALIGN",         (0, 0), (-1,  0), "CENTER"),
            ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
            ("FONTNAME",      (0, 1), (-1, -1), "Helvetica"),
            ("FONTSIZE",      (0, 1), (-1, -1), 7),
            ("ALIGN",         (0, 1), ( 2, -1), "LEFT"),
            ("ALIGN",         (3, 1), (-1, -1), "RIGHT"),
            ("GRID",          (0, 0), (-1, -1), 0.5, rl_colors.HexColor("#dddddd")),
            ("TOPPADDING",    (0, 0), (-1, -1), 2),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
            ("ROWBACKGROUND", (0, 1), (-1, -1), [LIGHT_GRAY, rl_colors.white]),
        ])
        table.setStyle(ts)
        _sku_extra = [("FONTSIZE", (0, i + 1), (0, i + 1), fs) for i, fs in enumerate(sku_fontsizes) if fs < 7]
        if _sku_extra:
            table.setStyle(TableStyle(_sku_extra))

        try:
            doc.build([table], canvasmaker=_PaginatedCanvas)
            return tmp.name
        except Exception:
            try:
                os.unlink(tmp.name)
            except Exception:
                pass
            return None

    def _generar_pdf_cotizacion(filtrados_actuales: List[Dict[str, Any]], margen: float, stock_umbral: int = 20) -> Optional[str]:
        """Genera PDF A4 lista de precios: SKU/Marca/Producto/Color/Stock/Precio $."""
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

        _EXCLUDE_WORDS_COT = {"caja", "cabierta", "cajaabierta", "abierto", "devolucion"}

        def _sku_excluido_cot(s: str) -> bool:
            normalized = s.lower().replace(".", "").replace("_", "")
            return any(w.replace(".", "").replace("_", "") in normalized for w in _EXCLUDE_WORDS_COT)

        rows_sorted = sorted(
            [r for r in filtrados_actuales if not _sku_excluido_cot(str(r.get('seller_sku') or r.get('id') or ''))],
            key=lambda x: ((x.get("marca") or "").lower(), (x.get("title") or "").lower()),
        )
        ahora = datetime.now()
        _fecha = f"{ahora.day:02d}/{ahora.month:02d}/{ahora.year}"
        _margen_str = f"{margen:.0f}%"
        _dolar_str = f"$ {round(dolar_oficial):,}".replace(",", ".")

        from reportlab.pdfbase.pdfmetrics import stringWidth as _sw
        _col_sku_pts  = 3.0 * rl_cm - 8
        _col_prod_pts = 9.0 * rl_cm - 12

        def _trunc(s):
            if not s or s == "''":
                return "—"
            if _sw(s, "Helvetica", 7) <= _col_prod_pts:
                return s
            while len(s) > 0 and _sw(s + "...", "Helvetica", 7) > _col_prod_pts:
                s = s[:-1]
            return (s + "...") if s else "..."

        data = [['SKU', 'Marca', 'Producto', 'Color', 'Stock', 'Precio $']]
        sku_fontsizes = []
        for r in rows_sorted:
            _costo_raw = r.get('costo_usd')
            _iva_raw   = r.get('tipo_iva')
            if not _costo_raw or not _iva_raw:
                continue
            stock_val = r.get('available_quantity') or 0
            if stock_val <= stock_umbral:
                stock_str = str(stock_val)
            else:
                stock_str = f"más de {stock_umbral}"
            sku_str = str(r.get('seller_sku') or r.get('id') or '')
            _sku_w = _sw(sku_str, 'Helvetica', 7)
            _sku_fs = 7 if _sku_w <= _col_sku_pts else max(5, round(7 * _col_sku_pts / _sku_w, 1))
            sku_fontsizes.append(_sku_fs)
            costo_usd = float(_costo_raw)
            tipo_iva  = float(_iva_raw)
            precio_pesos = costo_usd * (1 + tipo_iva) * (1 + margen / 100) * dolar_oficial
            precio_int = round(precio_pesos)
            precio_str = (
                f"$ {precio_int:,}".replace(",", ".")
                if precio_pesos > 0 else "—"
            )
            _marca_str = str(r.get('marca') or '')
            _color_str = str(r.get('color') or '')
            data.append([sku_str, "—" if _marca_str == "''" else _marca_str, _trunc(str(r.get('title') or '')),
                         "—" if _color_str == "''" else _color_str, stock_str, precio_str])

        tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
        tmp.close()
        page_w, page_h = A4
        margin    = 1.5 * rl_cm
        margin_lr = 0.8 * rl_cm
        margin_b  = 0.5 * rl_cm

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
                self.drawString(margin_lr, page_h - margin + 4,
                                f"Lista de Precios {_fecha}   |   Dólar {_dolar_str}")
                self.setFont("Helvetica", 9)
                self.setFillColorRGB(0.4, 0.4, 0.4)
                self.drawRightString(page_w - margin_lr, page_h - margin + 4,
                                     f"Página {self._pageNumber} de {page_count}")
                self.restoreState()

        doc = SimpleDocTemplate(tmp.name, pagesize=A4,
                                leftMargin=margin_lr, rightMargin=margin_lr,
                                topMargin=margin + 0.9 * rl_cm, bottomMargin=margin_b)
        col_widths = [3.0*rl_cm, 1.3*rl_cm, 9.0*rl_cm, 1.6*rl_cm, 1.9*rl_cm, 2.6*rl_cm]
        table = Table(data, colWidths=col_widths, repeatRows=1)
        BLUE = rl_colors.HexColor("#1976d2")
        LIGHT_GRAY = rl_colors.HexColor("#f8f8f8")
        ts = TableStyle([
            ("BACKGROUND",    (0, 0), (-1,  0), BLUE),
            ("TEXTCOLOR",     (0, 0), (-1,  0), rl_colors.white),
            ("FONTNAME",      (0, 0), (-1,  0), "Helvetica-Bold"),
            ("FONTSIZE",      (0, 0), (-1,  0), 8),
            ("ALIGN",         (0, 0), (-1,  0), "CENTER"),
            ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
            ("FONTNAME",      (0, 1), (-1, -1), "Helvetica"),
            ("FONTSIZE",      (0, 1), (-1, -1), 7),
            ("ALIGN",         (0, 1), ( 3, -1), "LEFT"),
            ("ALIGN",         (4, 1), (-1, -1), "RIGHT"),
            ("GRID",          (0, 0), (-1, -1), 0.5, rl_colors.HexColor("#dddddd")),
            ("TOPPADDING",    (0, 0), (-1, -1), 2),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
            ("ROWBACKGROUND", (0, 1), (-1, -1), [LIGHT_GRAY, rl_colors.white]),
        ])
        table.setStyle(ts)
        _sku_extra = [("FONTSIZE", (0, i+1), (0, i+1), fs) for i, fs in enumerate(sku_fontsizes) if fs < 7]
        if _sku_extra:
            table.setStyle(TableStyle(_sku_extra))
        try:
            doc.build([table], canvasmaker=_PaginatedCanvas)
            return tmp.name
        except Exception:
            try: os.unlink(tmp.name)
            except Exception: pass
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
            path = await run.io_bound(_generar_pdf_stock, rows_to_print, imprimir_ventas)
            if path:
                ahora = datetime.now()
                ts = f"{ahora.day:02d}-{ahora.month:02d}-{ahora.year % 100:02d}"
                fname = f"ventas_{ts}.pdf" if imprimir_ventas else f"stock_{ts}.pdf"
                with client:
                    ui.download(path, fname)
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

    def imprimir_tabla_usd() -> None:
        client = context.client
        tbl = current_table[0] if current_table else None

        async def _imprimir_usd_async() -> None:
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
                    ui.notify("No hay datos para imprimir. Aplica filtros y volve a intentar.", color="warning")
                return
            path = await run.io_bound(_generar_pdf_stock_usd, rows_to_print)
            if path:
                ahora = datetime.now()
                ts = f"{ahora.day:02d}-{ahora.month:02d}-{ahora.year % 100:02d}"
                with client:
                    ui.download(path, f"stock_usd_{ts}.pdf")
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

        background_tasks.create(_imprimir_usd_async())

    def imprimir_compras() -> None:
        client = context.client
        t_items = list(todos_items_ref)
        s_id = seller_id_ref

        async def _imprimir_compras_async() -> None:
            if not t_items:
                with client:
                    ui.notify("No hay productos cargados.", color="warning")
                return
            if not s_id:
                with client:
                    ui.notify("No se pudo obtener el ID de vendedor de MercadoLibre.", color="warning")
                return
            with client:
                ui.notify("Consultando ventas de los últimos 90 días...", color="info")
            path = await run.io_bound(_generar_pdf_compras, t_items, access_token, s_id)
            if path:
                ahora = datetime.now()
                ts = f"{ahora.day:02d}-{ahora.month:02d}-{ahora.year % 100:02d}"
                with client:
                    ui.download(path, f"compras_{ts}.pdf")
                    def _borrar(p=path) -> None:
                        try:
                            if p and os.path.exists(p):
                                os.unlink(p)
                        except Exception:
                            pass
                    ui.timer(5.0, _borrar, once=True)
            else:
                with client:
                    ui.notify("No hay productos a comprar o no se pudieron obtener las ventas.", color="warning")

        background_tasks.create(_imprimir_compras_async())

    def imprimir_cotizar() -> None:
        client = context.client
        rows_snapshot = list(current_filtrados)

        with ui.dialog() as dlg, ui.card().classes("w-72 gap-4"):
            ui.label("Lista de Precios").classes("text-lg font-bold")
            inp_margen = ui.number("Margen (%)", value=10, min=0, max=999, step=1).props("outlined dense")
            inp_umbral = ui.number("Mostrar stock mayor a:", value=20, min=0, max=99999, step=1).props("outlined dense")

            def _generar() -> None:
                margen_val = float(inp_margen.value or 10)
                umbral_val = int(inp_umbral.value if inp_umbral.value is not None else 20)
                dlg.close()
                if not rows_snapshot:
                    ui.notify("No hay productos visibles para cotizar.", color="warning")
                    return

                async def _gen_async() -> None:
                    path = await run.io_bound(_generar_pdf_cotizacion, rows_snapshot, margen_val, umbral_val)
                    if path:
                        ahora = datetime.now()
                        ts = f"{ahora.day:02d}-{ahora.month:02d}-{ahora.year % 100:02d}"
                        with client:
                            ui.download(path, f"lista_precios_{ts}.pdf")
                            def _borrar(p=path) -> None:
                                try:
                                    if p and os.path.exists(p): os.unlink(p)
                                except Exception: pass
                            ui.timer(5.0, _borrar, once=True)
                    else:
                        with client:
                            ui.notify("No se pudo generar el PDF.", color="negative")

                background_tasks.create(_gen_async())

            with ui.row().classes("w-full justify-end gap-2 mt-2"):
                ui.button("Cerrar", on_click=dlg.close).props("flat dense no-caps")
                ui.button("Imprimir PDF", on_click=_generar).props("unelevated dense no-caps icon=sell").style("background:#185FA5;color:#E6F1FB;")

        dlg.open()

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
        {"name": "status", "label": "Estado", "field": "status", "sortable": True, "align": "center", "headerStyle": header_style, ":format": "(val) => (val || '').toLowerCase() === 'active' ? 'Activa' : 'Pausada'"},
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
        _costo_final_usd = sum(
            float(x.get("costo_usd") or 0) * (1 + float(x.get("tipo_iva") or 0)) * float(x.get("available_quantity") or 0)
            for x in filtrados if x.get("tipo") == "Propia"
        )
        _costo_final_pesos = _costo_final_usd * dolar_oficial if dolar_oficial else 0.0
        lbl_pesos.set_text(fmt_moneda(_costo_final_pesos))
        lbl_usd.set_text(f"u$s {fmt_miles(int(round(_costo_final_usd)))}")
        lbl_marcas.set_text(str(len({
            str(x.get("marca") or "").strip()
            for x in filtrados
            if x.get("tipo") == "Propia"
            and str(x.get("marca") or "").strip()
            and str(x.get("marca") or "").strip() != "''"
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
                                                ui.label("Pausada").classes("text-center text-red-500")
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
                                            ui.label(str(val) if val is not None and str(val) != "''" else "—")
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
                lbl_totales = ui.label("''").classes("text-sm font-bold text-primary")
            with ui.row().classes("items-baseline gap-1"):
                ui.label("Unidades:").classes("text-xs text-gray-500")
                lbl_unidades = ui.label("''").classes("text-sm font-bold text-primary")
            with ui.row().classes("items-baseline gap-1"):
                ui.label("Costo Final $:").classes("text-xs text-gray-500")
                lbl_pesos = ui.label("''").classes("text-sm font-bold text-primary")
            with ui.row().classes("items-baseline gap-1"):
                ui.label("Costo Final u$s:").classes("text-xs text-gray-500")
                lbl_usd = ui.label("''").classes("text-sm font-bold text-primary")
            with ui.row().classes("items-baseline gap-1"):
                ui.label("Marcas:").classes("text-xs text-gray-500")
                lbl_marcas = ui.label("''").classes("text-sm font-bold text-primary")
            ui.space()
            if on_actualizar:
                ui.button("Actualizar", on_click=lambda: on_actualizar()).props("unelevated dense no-caps icon=refresh").style("background:#185FA5;color:#E6F1FB;").classes("text-xs")
            ui.button("Limpiar día", on_click=_blanquear_revisiones).props("unelevated dense no-caps icon=event_busy").style("background:#993C1D;color:#FAECE7;").classes("text-xs").tooltip("Blanquear revisiones de hoy")
            ui.element("div").style("width:1px;height:24px;background:rgba(0,0,0,0.15);align-self:center;")
            ui.button("Stock", on_click=lambda: imprimir_tabla(include_ventas=False)).props("unelevated dense no-caps icon=inventory_2").style("background:#185FA5;color:#E6F1FB;").classes("text-xs")
            ui.button("Stock $", on_click=lambda: imprimir_tabla_usd()).props("unelevated dense no-caps icon=request_quote").style("background:#185FA5;color:#E6F1FB;").classes("text-xs")
            ui.button("Ventas", on_click=lambda: imprimir_tabla(include_ventas=True)).props("unelevated dense no-caps icon=bar_chart").style("background:#185FA5;color:#E6F1FB;").classes("text-xs")
            ui.button("Compras", on_click=lambda: imprimir_compras()).props("unelevated dense no-caps icon=shopping_cart").style("background:#185FA5;color:#E6F1FB;").classes("text-xs")
            ui.button("Lista de Precios", on_click=lambda: imprimir_cotizar()).props("unelevated dense no-caps icon=sell").style("background:#185FA5;color:#E6F1FB;").classes("text-xs")
        with ui.row().classes("items-center gap-2 py-1 flex-wrap"):
            filtro_stock = ui.select(
                {"con_stock": "Con stock", "todas": "Todas", "sin_stock": "Sin stock"},
                value=filtro_stock_ref.get("val", "con_stock") if filtro_stock_ref else "con_stock",
                label="Stock",
            ).classes("w-32").props("outlined dense")
            filtro_estado = ui.select(
                {"activas": "Activas", "suspendidas": "Pausadas", "todas": "Todas"},
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

