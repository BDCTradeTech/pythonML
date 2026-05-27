"""
Fase 3 — tabs/precios_detalle.py
Pestaña Precios Detalle: tabla con id, marca, producto, stock, precio, iva, costo,
comision, cobrado, iibb, margen $, margen costo, margen venta.
Funciones exportadas: build_tab_precios_detalle
"""
from __future__ import annotations

import asyncio
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from nicegui import app, background_tasks, context, run, ui

from db import get_connection, get_cotizador_param, COTIZADOR_DEFAULTS
from ml_api import (
    get_ml_access_token,
    ml_get_my_items,
    ml_get_item_description,
    ml_get_item_sale_price_full,
    ml_get_items_multiget_with_attributes,
    ml_get_items_multiget_all,
    ml_get_promotion_item_discounts,
    ml_get_promotion_item_discounts_by_user,
    ml_get_promotion_item_discounts_by_campaign,
    ml_get_orders,
    ml_get_user_id,
    ml_get_user_profile,
)
from tabs.precios import _show_item_detail_dialog


# ---------------------------------------------------------------------------
# Helper de sesión (mismo patrón que otros tabs; se unificará en auth.py Fase 4)
# ---------------------------------------------------------------------------

def _require_login() -> Optional[Dict[str, Any]]:
    user = app.storage.user.get("user")
    if not user:
        ui.notify("Debes iniciar sesión para continuar", color="negative")
    return user


# ---------------------------------------------------------------------------
# Función exportada
# ---------------------------------------------------------------------------

def build_tab_precios_detalle(container) -> None:
    """Pestaña Precios: tabla con id, marca, producto, stock, precio, iva, costo, comision, cobrado, iibb, margen $, margen costo, margen venta."""
    container.clear()
    user = _require_login()
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
                        lbl_sin_promo = ui.label("””").classes("text-2xl font-bold text-primary")
                        calcular_labels_ref["sin_promo"] = lbl_sin_promo
                    with ui.column().classes("items-center"):
                        ui.label("Publicaciones con promociones").classes("text-sm text-gray-600")
                        lbl_con_promo = ui.label("””").classes("text-2xl font-bold text-primary")
                        calcular_labels_ref["con_promo"] = lbl_con_promo
                    with ui.column().classes("items-center"):
                        ui.label("Publicaciones con cuotas").classes("text-sm text-gray-600")
                        lbl_con_cuotas = ui.label("””").classes("text-2xl font-bold text-primary")
                        calcular_labels_ref["con_cuotas"] = lbl_con_cuotas
                    with ui.column().classes("items-center"):
                        ui.label("Unidades vendidas").classes("text-sm text-gray-600")
                        lbl_uds = ui.label("””").classes("text-2xl font-bold text-primary")
                        calcular_labels_ref["unidades"] = lbl_uds
                    with ui.column().classes("items-center"):
                        ui.label("Facturación total").classes("text-sm text-gray-600")
                        lbl_fact = ui.label("””").classes("text-2xl font-bold text-primary")
                        calcular_labels_ref["facturacion"] = lbl_fact
                    with ui.column().classes("items-center"):
                        ui.label("Margen total").classes("text-sm text-gray-600")
                        lbl_margen = ui.label("””").classes("text-2xl font-bold text-primary")
                        calcular_labels_ref["margen"] = lbl_margen
                    with ui.column().classes("items-center"):
                        ui.label("Margen % sobre venta").classes("text-sm text-gray-600")
                        lbl_margen_pct = ui.label("””").classes("text-2xl font-bold text-primary")
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
                                titulo = (obj.get("title") if isinstance(obj, dict) else str(obj)) or it.get("title") or "””"
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
                                        item_id_to_info[iid] = {"stock": int(b.get("available_quantity") or 0), "marca": marca or "””", "color": color or "””", "catalog_product_id": catalog_id, "title": (b.get("title") or "")[:200]}
                        ids_sin_color = [iid for iid in item_ids_list if (item_id_to_info.get(iid) or {}).get("color") == "””"]
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
                            marca = info["marca"] if info else "””"
                            color = (info["color"] if info else "””") or item_id_to_color_desc.get(iid) or item_id_to_color_desc.get(iid.upper()) or item_id_to_color_desc.get(iid.lower()) or "””"
                            if color == "””":
                                color = _extraer_color_desde_texto(v["productos"]) or "””"
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
                "marca": i.get("marca") or "””",
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
                "marca": marca or "””",
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
