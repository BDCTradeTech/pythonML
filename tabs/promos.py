"""
Fase 3 — tabs/promos.py
Pestaña Promos: tabla de publicaciones con descuentos activos de MercadoLibre.
Funciones exportadas: build_tab_promos
"""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional

import requests
from nicegui import app, background_tasks, run, ui

from db import get_connection, get_cotizador_param, COTIZADOR_DEFAULTS
from ml_api import get_ml_access_token, ml_get_my_items, ml_get_item_sale_price_full
from tabs.cuotas import _cuotas_key


def _require_login() -> Optional[Dict[str, Any]]:
    user = app.storage.user.get("user")
    if not user:
        ui.notify("Debes iniciar sesión para continuar", color="negative")
    return user


def build_tab_promos(container) -> None:
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
        with result_area:
            with ui.card().classes("w-full p-8 items-center gap-4"):
                ui.spinner(size="xl")
                ui.label("Cargando promos...").classes("text-xl text-gray-700")

        def cargar_promos() -> None:
            result_area.clear()
            with result_area:
                with ui.card().classes("w-full p-8 items-center gap-4"):
                    ui.spinner(size="xl")
                    ui.label("Cargando promos...").classes("text-xl text-gray-700")
            background_tasks.create(
                _cargar_async(result_area, access_token, user, cargar_promos), name="cargar_promos"
            )

        async def _cargar_async(area, token, usr, on_reload) -> None:
            try:
                data = await run.io_bound(ml_get_my_items, token, True)
            except requests.exceptions.HTTPError as e:
                area.clear()
                with area:
                    ui.label(f"❌ Error ML API: {e}").classes("text-negative mb-2")
                return
            except Exception as e:
                area.clear()
                with area:
                    ui.label(f"❌ Error: {e}").classes("text-negative")
                return
            n = len(data.get("results", []))
            area.clear()
            with area:
                with ui.card().classes("w-full p-8 items-center gap-4"):
                    ui.spinner(size="xl")
                    ui.label(f"Procesando {n} publicaciones...").classes("text-xl text-gray-700")
            await asyncio.sleep(0.1)
            try:
                _mostrar_tabla_promos(area, data, token, usr, on_reload)
            except Exception as e:
                area.clear()
                with area:
                    ui.label(f"❌ Error al mostrar datos: {e}").classes("text-negative")

        background_tasks.create(
            _cargar_async(result_area, access_token, user, cargar_promos), name="cargar_promos_init"
        )


def _mostrar_tabla_promos(
    result_area, data: Dict[str, Any], access_token: str, user: Dict[str, Any], on_reload=None,
) -> None:
    def fmt_m(v: Any) -> str:
        if v is None: return "$0"
        try: return "$" + f"{int(float(v)):,}".replace(",", ".")
        except: return "$0"

    def fmt_p1(v: Any) -> str:
        if v is None: return "—"
        try: return f"{float(v):.1f}%".replace(".", ",")
        except: return "—"

    def fmt_p2(v: Any) -> str:
        if v is None: return "0,00%"
        try: return f"{float(v):.2f}%".replace(".", ",")
        except: return "0,00%"

    items = data.get("results", [])
    if not items:
        with result_area:
            ui.label("No hay publicaciones.").classes("text-gray-500 p-4")
        return

    groups: Dict[Any, List[Dict]] = {}
    for i in items:
        groups.setdefault(_cuotas_key(i), []).append(i)
    items_dedup: List[Dict] = []
    for grupo in groups.values():
        if len(grupo) == 1:
            items_dedup.append(grupo[0])
            continue
        total_sold = sum(int(x.get("sold_quantity") or 0) for x in grupo)
        principal = max(
            grupo,
            key=lambda x: (
                1 if not x.get("catalog_listing") and str(x.get("listing_type_id") or "").lower() == "gold_special" else 0,
                int(x.get("available_quantity") or 0),
            ),
        )
        fusionado = dict(principal)
        fusionado["sold_quantity"] = total_sold
        items_dedup.append(fusionado)

    uid = user["id"]

    def _pr(s: Any) -> float:
        if s is None or s == "": return 0.0
        try:
            v = float(str(s).strip().replace(",", "."))
            return v if v <= 1.5 else v / 100.0
        except: return 0.0

    def _pf(s: Any) -> float:
        if s is None or s == "": return 0.0
        try: return float(str(s).replace(".", "").replace(",", ".").strip()) or 0.0
        except: return 0.0

    dolar_str = get_cotizador_param("dolar_oficial", uid) or COTIZADOR_DEFAULTS.get("dolar_oficial", "1475")
    dolar_oficial = max(float(str(dolar_str or "1475").replace(",", ".").strip() or "1475"), 0.01)
    ml_comision_p    = _pr(get_cotizador_param("ml_comision",          uid) or COTIZADOR_DEFAULTS.get("ml_comision",          "0.15"))
    cuotas_6x_p      = _pr(get_cotizador_param("cuotas_6x",            uid) or COTIZADOR_DEFAULTS.get("cuotas_6x",            "0.151"))
    ml_iibb_per_p    = _pr(get_cotizador_param("ml_iibb_per",          uid) or COTIZADOR_DEFAULTS.get("ml_iibb_per",          "0.055"))
    ml_debcre_p      = _pr(get_cotizador_param("ml_debcre",            uid) or COTIZADOR_DEFAULTS.get("ml_debcre",            "0.006"))
    ml_envios_p      = _pf(get_cotizador_param("ml_envios",            uid) or COTIZADOR_DEFAULTS.get("ml_envios",            "5823")) or 5823.0
    ml_envios_grat_p = _pf(get_cotizador_param("ml_envios_gratuitos",  uid) or COTIZADOR_DEFAULTS.get("ml_envios_gratuitos",  "33000")) or 33000.0
    if ml_envios_p < 100: ml_envios_p = 5823.0

    _skus = [i.get("seller_sku") for i in items_dedup if i.get("seller_sku")]
    _prod_map: Dict[str, Dict] = {}
    if _skus:
        conn = get_connection()
        try:
            ph = ",".join("?" * len(_skus))
            rows = conn.execute(
                f"SELECT sku, costo_usd, tipo_iva FROM productos WHERE sku IN ({ph}) AND user_id=?",
                _skus + [uid],
            ).fetchall()
            _prod_map = {r["sku"]: {"costo_usd": r["costo_usd"], "tipo_iva": r["tipo_iva"]} for r in rows}
        finally:
            conn.close()

    items_data: List[Dict] = []
    for i in items_dedup:
        _sku = i.get("seller_sku")
        _p = _prod_map.get(_sku) if _sku else None
        items_data.append({
            **i,
            "has_promo": False, "sale_price_amount": None, "sale_price_regular": None,
            "discount_total_pct": None, "discount_amount": None, "promo_type": None,
            "promo_meli_amount": None, "promo_seller_amount": None,
            "costo_usd": _p["costo_usd"] if _p else None,
            "tipo_iva": float(_p["tipo_iva"]) if _p and _p.get("tipo_iva") else 0.105,
        })

    _all_ids = [str(r["id"]) for r in items_data if r.get("id")]
    if _all_ids and access_token:
        def _batch(ids: List[str]) -> Dict[str, Any]:
            res: Dict[str, Any] = {}
            with ThreadPoolExecutor(max_workers=min(8, len(ids))) as ex:
                fs = {ex.submit(ml_get_item_sale_price_full, access_token, iid): iid for iid in ids}
                for fut in as_completed(fs):
                    iid = fs[fut]
                    try: res[iid] = fut.result()
                    except: res[iid] = None
            return res
        sp_map = _batch(_all_ids)
        for r in items_data:
            sp = sp_map.get(str(r.get("id") or ""))
            if not sp or sp.get("amount") is None or sp.get("regular_amount") is None:
                continue
            amt = float(sp["amount"]); reg = float(sp["regular_amount"])
            if amt >= reg - 0.01:
                continue
            disc = reg - amt
            disc_pct = disc / reg * 100 if reg > 0 else 0.0
            ptype = (sp.get("promotion_type") or "").lower().strip()
            r.update({
                "has_promo": True, "sale_price_amount": amt, "sale_price_regular": reg,
                "discount_amount": disc, "discount_total_pct": disc_pct, "promo_type": ptype,
            })
            if "marketplace_campaign" in ptype:
                r["promo_meli_amount"] = disc
                r["promo_seller_amount"] = 0.0

    conn2 = get_connection()
    try:
        prod_rows = conn2.execute(
            "SELECT sku, nombre FROM productos WHERE user_id=? AND nombre IS NOT NULL AND nombre != '' ORDER BY nombre",
            (uid,),
        ).fetchall()
    finally:
        conn2.close()
    prod_opts: Dict[str, str] = {r["sku"]: r["nombre"] for r in prod_rows if r["sku"]}

    n_total = len(items_data)
    n_promo = sum(1 for r in items_data if r.get("has_promo"))
    current_filtrados: List[Dict] = []
    sort_col_ref: Dict[str, Any] = {"val": "title"}
    sort_asc_ref: Dict[str, Any] = {"val": True}
    selected_ref: Dict[str, Any] = {"row": None}
    ui_refs: Dict[str, Any] = {}

    columns = [
        {"name": "seller_sku",      "label": "SKU",            "align": "left"},
        {"name": "title",           "label": "Producto",        "align": "left"},
        {"name": "precio_original", "label": "Precio Original", "align": "right"},
        {"name": "precio_promo",    "label": "Precio Promo",    "align": "right"},
        {"name": "descuento_pct",   "label": "Desc %",          "align": "right"},
        {"name": "meli_amount",     "label": "Desc ML $",       "align": "right"},
        {"name": "seller_amount",   "label": "Desc Yo $",       "align": "right"},
        {"name": "stock",           "label": "Stock",           "align": "center"},
    ]
    col_widths = {
        "seller_sku": "90px", "title": "280px", "precio_original": "90px",
        "precio_promo": "90px", "descuento_pct": "60px",
        "meli_amount": "88px", "seller_amount": "88px", "stock": "50px",
    }

    def _build_colgroup() -> None:
        with ui.element("colgroup"):
            for col in columns:
                ui.element("col").style(f"width:{col_widths.get(col['name'], '80px')}")

    def _sort_key(r: Dict, col: str) -> Any:
        m: Dict[str, Any] = {
            "seller_sku":      lambda r: str(r.get("seller_sku") or "").lower(),
            "title":           lambda r: str(r.get("title") or "").lower(),
            "precio_original": lambda r: float(r.get("price") or 0),
            "precio_promo":    lambda r: float(r.get("sale_price_amount") or r.get("price") or 0),
            "descuento_pct":   lambda r: float(r.get("discount_total_pct") or 0),
            "meli_amount":     lambda r: float(r.get("promo_meli_amount") or 0),
            "seller_amount":   lambda r: float(r.get("promo_seller_amount") or 0),
            "stock":           lambda r: int(r.get("available_quantity") or 0),
        }
        return m.get(col, lambda r: "")(r)

    def _on_sort(col_name: str) -> None:
        if sort_col_ref["val"] == col_name:
            sort_asc_ref["val"] = not sort_asc_ref["val"]
        else:
            sort_col_ref["val"] = col_name
            sort_asc_ref["val"] = True
        filtrar_y_pintar()

    def _close_detail() -> None:
        selected_ref["row"] = None
        dc = ui_refs.get("detail_col")
        if dc:
            dc.style("display:none")
            dc.clear()

    def _on_row_click(row: Dict) -> None:
        if selected_ref.get("row") is row:
            _close_detail()
            return
        selected_ref["row"] = row
        dc = ui_refs.get("detail_col")
        if dc:
            dc.style("display:block")
            dc.clear()
            with dc:
                _render_inline_detail(row)

    def _render_cell(col_name: str, row: Dict) -> None:
        has_p = row.get("has_promo", False)
        is_mc = "marketplace_campaign" in (row.get("promo_type") or "")
        if col_name == "seller_sku":
            ui.label(str(row.get("seller_sku") or "—")).classes("text-xs")
        elif col_name == "title":
            ui.label(str(row.get("title") or "—")[:80]).classes("text-xs")
        elif col_name == "precio_original":
            ui.label(fmt_m(row.get("price"))).classes("text-xs line-through text-gray-400" if has_p else "text-xs")
        elif col_name == "precio_promo":
            if has_p:
                ui.label(fmt_m(row.get("sale_price_amount"))).classes("text-xs font-medium").style("color:#E24B4A")
            else:
                ui.label("—").classes("text-xs text-gray-400")
        elif col_name == "descuento_pct":
            if has_p:
                ui.label(fmt_p1(row.get("discount_total_pct"))).classes("text-xs font-medium").style("color:#E24B4A")
            else:
                ui.label("—").classes("text-xs text-gray-400")
        elif col_name == "meli_amount":
            if has_p:
                ma = row.get("promo_meli_amount")
                if ma is not None:
                    with ui.column().classes("gap-0 items-end"):
                        ui.label(fmt_m(ma)).classes("text-xs font-medium")
                        if is_mc:
                            ui.label("ML 100%").classes("text-[10px] text-blue-600")
                else:
                    ui.label("—").classes("text-xs text-gray-400")
            else:
                ui.label("—").classes("text-xs text-gray-400")
        elif col_name == "seller_amount":
            if has_p:
                sa = row.get("promo_seller_amount")
                if sa is not None:
                    ui.label(fmt_m(sa)).classes("text-xs text-gray-400" if sa == 0 else "text-xs")
                else:
                    ui.label("—").classes("text-xs text-gray-400")
            else:
                ui.label("—").classes("text-xs text-gray-400")
        elif col_name == "stock":
            ui.label(str(row.get("available_quantity") or 0)).classes("text-xs")

    def _render_inline_detail(row: Dict) -> None:
        precio_calc = float(row.get("sale_price_amount") or row.get("price") or 0)
        costo_usd = float(row.get("costo_usd") or 0)
        tipo_iva = float(row.get("tipo_iva") or 0.105)
        tasa_cuotas = cuotas_6x_p if str(row.get("listing_type_id") or "").lower() == "gold_pro" else 0.0
        comision = precio_calc * ml_comision_p
        cobrado = precio_calc - comision
        deb_cred = precio_calc * ml_debcre_p
        iibb = precio_calc * ml_iibb_per_p
        iva_venta = precio_calc * tipo_iva / (1 + tipo_iva) if tipo_iva else 0.0
        iva_meli = comision * 0.21 / 1.21
        iva_impor = 0.09 * costo_usd * dolar_oficial
        iva_total = iva_venta - iva_meli - iva_impor
        envio = ml_envios_p if precio_calc >= ml_envios_grat_p else 0.0
        costo_cuotas = precio_calc * tasa_cuotas
        costo_pesos = costo_usd * dolar_oficial
        if costo_pesos > 0:
            margen = cobrado - costo_pesos - iva_total - iibb - deb_cred - envio - costo_cuotas
            margen_vta = margen / precio_calc * 100 if precio_calc > 0 else 0.0
            margen_cos = margen / costo_pesos * 100
        else:
            margen = margen_vta = margen_cos = 0.0
        mcls = "font-bold text-black" if costo_pesos <= 0 else ("font-bold text-positive" if margen > 0 else "font-bold text-negative")
        _ICO = '<i class="ti ti-calculator" style="font-size:13px;color:#BA7517"></i>'
        _ICO_XS = '<i class="ti ti-calculator" style="font-size:12px;color:#BA7517"></i>'
        with ui.card().classes("w-full p-4 mt-2"):
            with ui.row().classes("w-full gap-3 mb-2 items-start"):
                thumb = row.get("thumbnail") or ""
                if thumb:
                    ui.image(thumb).classes("w-14 h-14 object-contain rounded border")
                with ui.column().classes("flex-1 min-w-0 gap-1"):
                    ui.label(f"{row.get('id', '')} · {row.get('seller_sku', '—')}").classes("text-xs font-mono text-gray-500")
                    ui.label(str(row.get("title", ""))[:120]).classes("text-sm font-bold")
                    with ui.row().classes("gap-2 items-center flex-wrap"):
                        if row.get("has_promo"):
                            ui.label(fmt_m(row.get("price"))).classes("text-sm line-through text-gray-400")
                            ui.label(fmt_m(row.get("sale_price_amount"))).classes("text-sm font-bold").style("color:#E24B4A")
                            ui.label(f"↓ {fmt_p1(row.get('discount_total_pct'))}").classes("text-xs").style("color:#E24B4A")
                        else:
                            ui.label(fmt_m(row.get("price"))).classes("text-sm font-bold")
                        ui.label(f"Stock: {row.get('available_quantity') or 0}").classes("text-xs text-gray-500")
            ui.separator()
            with ui.column().classes("w-full gap-0 pt-2 max-w-md"):
                with ui.row().classes("w-full justify-between py-0.5 gap-4"):
                    with ui.row().classes("items-center gap-1"):
                        ui.html(_ICO)
                        ui.label("Precio de Venta").classes("text-sm font-medium text-gray-600")
                    ui.label(fmt_m(precio_calc)).classes("text-sm font-medium")
                for lbl, val, neg in [
                    ("Comisión ML", comision, True),
                    ("Costo Cuotas", costo_cuotas, True),
                    ("IVA neto", iva_total, True),
                ]:
                    with ui.row().classes("w-full justify-between py-0.5 gap-4"):
                        with ui.row().classes("items-center gap-1"):
                            ui.html(_ICO)
                            ui.label(lbl).classes("text-sm font-medium text-gray-600")
                        ui.label(fmt_m(val)).classes(f"text-sm {'text-negative' if neg else ''}")
                with ui.column().classes("w-full bg-gray-50 rounded px-2 py-1 mb-0.5 gap-0"):
                    for sl, sv in [
                        ("IVA venta", iva_venta),
                        ("IVA Meli (créd)", iva_meli),
                        ("IVA importación (créd)", iva_impor),
                    ]:
                        with ui.row().classes("w-full justify-between"):
                            with ui.row().classes("items-center gap-1"):
                                ui.html(_ICO_XS)
                                ui.label(sl).classes("text-xs font-medium text-gray-600")
                            ui.label(fmt_m(sv)).classes("text-xs text-gray-600")
                for lbl, val in [("Deb/Cred", deb_cred), ("IIBB ret.", iibb), ("Envío promedio Flex/Correo", envio)]:
                    with ui.row().classes("w-full justify-between py-0.5 gap-4"):
                        with ui.row().classes("items-center gap-1"):
                            ui.html(_ICO)
                            ui.label(lbl).classes("text-sm font-medium text-gray-600")
                        ui.label(fmt_m(val)).classes("text-sm text-negative")
                ui.separator()
                with ui.row().classes("w-full justify-between py-0.5 gap-4"):
                    with ui.row().classes("items-center gap-1"):
                        ui.html(_ICO)
                        ui.label("Costo producto").classes("text-sm font-medium text-gray-600")
                    ui.label(fmt_m(costo_pesos)).classes("text-sm text-negative")
                ui.separator()
                for lbl, val, is_p in [
                    ("Gan $", margen, False),
                    ("Gan Vta %", margen_vta, True),
                    ("Gan % Cos", margen_cos, True),
                ]:
                    with ui.row().classes("w-full justify-between py-0.5 gap-4"):
                        with ui.row().classes("items-center gap-1"):
                            ui.html(_ICO)
                            ui.label(lbl).classes("text-sm font-medium text-gray-600")
                        ui.label(fmt_p2(val) if is_p else fmt_m(val)).classes(mcls)
            with ui.row().classes("w-full justify-end mt-2"):
                ui.button("Cerrar", on_click=_close_detail).props("flat dense no-caps").classes("text-xs")

    def filtrar_y_pintar() -> None:
        filtrados = list(items_data)
        promo_v = getattr(ui_refs.get("sel_promo"), "value", "todas") or "todas"
        if promo_v == "con_promo":
            filtrados = [x for x in filtrados if x.get("has_promo")]
        elif promo_v == "sin_promo":
            filtrados = [x for x in filtrados if not x.get("has_promo")]
        prod_v = getattr(ui_refs.get("sel_producto"), "value", None)
        if prod_v:
            filtrados = [x for x in filtrados if (x.get("seller_sku") or "") == prod_v]
        filtrados = sorted(
            filtrados,
            key=lambda r: _sort_key(r, sort_col_ref["val"]),
            reverse=not sort_asc_ref["val"],
        )
        current_filtrados.clear()
        current_filtrados.extend(filtrados)
        lbl = ui_refs.get("lbl_filtrados")
        if lbl:
            lbl.set_text(str(len(filtrados)))
        hdr = ui_refs.get("header_div")
        if hdr:
            hdr.clear()
            with hdr:
                with ui.element("table").style("table-layout:fixed;width:100%;border-collapse:separate;border-spacing:0"):
                    _build_colgroup()
                    with ui.element("thead"):
                        with ui.element("tr").classes("bg-primary text-white font-semibold"):
                            for col in columns:
                                with ui.element("th").classes("px-2 py-2 border text-center"):
                                    ui.button(
                                        col["label"],
                                        on_click=lambda c=col["name"]: _on_sort(c),
                                    ).props("flat dense no-caps").classes("text-white hover:bg-white/20 cursor-pointer font-semibold")
        tbl = ui_refs.get("table_container")
        if tbl:
            tbl.clear()
            with tbl:
                with ui.element("table").style("table-layout:fixed;width:100%;border-collapse:separate;border-spacing:0"):
                    _build_colgroup()
                    with ui.element("tbody"):
                        for row in filtrados:
                            _bg = "bg-blue-50" if selected_ref.get("row") is row else ""
                            with ui.element("tr").classes(
                                f"border-t border-gray-200 hover:bg-gray-50 cursor-pointer {_bg}"
                            ).on("click", lambda r=row: _on_row_click(r)):
                                for col in columns:
                                    align = (
                                        "text-right" if col.get("align") == "right"
                                        else "text-center" if col.get("align") == "center"
                                        else "text-left"
                                    )
                                    td_style = (
                                        "white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:0"
                                        if col["name"] in ("title", "seller_sku") else ""
                                    )
                                    with ui.element("td").classes(
                                        f"px-2 py-1 border-b border-gray-100 {align} text-xs"
                                    ).style(td_style):
                                        _render_cell(col["name"], row)
        if selected_ref.get("row") is not None and selected_ref["row"] not in filtrados:
            _close_detail()

    # --- Build UI ---
    with result_area:
        with ui.row().classes("w-full items-center gap-4 px-3 py-1 bg-grey-2 rounded mb-1 flex-wrap"):
            with ui.row().classes("items-baseline gap-1"):
                ui.label("Total:").classes("text-xs text-gray-500")
                ui.label(str(n_total)).classes("text-sm font-bold text-primary")
            with ui.row().classes("items-baseline gap-1"):
                ui.label("Con promo:").classes("text-xs text-gray-500")
                ui.label(str(n_promo)).classes("text-sm font-bold").style("color:#E24B4A")
            with ui.row().classes("items-baseline gap-1"):
                ui.label("Filtrados:").classes("text-xs text-gray-500")
                ui_refs["lbl_filtrados"] = ui.label("—").classes("text-sm font-bold text-primary")
            ui.space()
            if on_reload:
                ui.button("Actualizar", on_click=on_reload).props(
                    "unelevated dense no-caps icon=refresh"
                ).style("background:#185FA5;color:#E6F1FB;").classes("text-xs")

        with ui.row().classes("items-center gap-3 py-1 flex-wrap"):
            ui_refs["sel_promo"] = ui.select(
                {"todas": "Todas", "con_promo": "Con promo", "sin_promo": "Sin promo"},
                value="todas",
                label="Promo",
            ).classes("w-36").props("outlined dense")
            ui_refs["sel_producto"] = ui.select(
                options=prod_opts if prod_opts else {"": "— Sin productos —"},
                value=None,
                with_input=True,
                clearable=True,
                label="Buscar producto",
            ).classes("w-72").props("outlined dense")

        ui_refs["header_div"] = ui.element("div").style("width:100%;overflow:hidden")
        ui_refs["table_container"] = ui.element("div").style(
            "width:100%;height:55vh;overflow-y:scroll;overflow-x:auto"
        )
        _hid = ui_refs["header_div"].id
        _cid = ui_refs["table_container"].id

        async def _setup_sync() -> None:
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
        background_tasks.create(_setup_sync())

        ui_refs["detail_col"] = ui.column().classes("w-full")
        ui_refs["detail_col"].style("display:none")

    ui_refs["sel_promo"].on_value_change(lambda *a: filtrar_y_pintar())
    ui_refs["sel_producto"].on_value_change(lambda *a: filtrar_y_pintar())
    filtrar_y_pintar()
