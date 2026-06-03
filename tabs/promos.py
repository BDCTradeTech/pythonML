"""
tabs/promos.py
Pestaña Promos: verifica descuentos ML por producto seleccionado.
Carga desde BD; consulta ML (2 calls) solo al seleccionar un ítem.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

from nicegui import app, background_tasks, run, ui

from db import get_connection, get_cotizador_param, COTIZADOR_DEFAULTS
from ml_api import get_ml_access_token, ml_get_item_sale_price_full, ml_get_item


def _require_login() -> Optional[Dict[str, Any]]:
    user = app.storage.user.get("user")
    if not user:
        ui.notify("Debes iniciar sesión", color="negative")
    return user


def build_tab_promos(container) -> None:
    container.clear()
    user = _require_login()
    if not user:
        return
    uid = user["id"]
    with container:
        access_token = get_ml_access_token(uid)
        if not access_token:
            ui.label("⚠️ No tienes MercadoLibre vinculado. Ve a Configuración.").classes("text-warning")
            return

        # ── Carga desde BD (sin API) ─────────────────────────────────────────
        conn = get_connection()
        try:
            rows = conn.execute(
                """SELECT mp.ml_id, p.sku, COALESCE(mp.titulo, '') AS titulo,
                          p.nombre, p.marca, p.costo_usd, p.tipo_iva,
                          COALESCE(mp.precio, 0) AS precio,
                          COALESCE(mp.stock, 0) AS stock,
                          COALESCE(mp.listing_type_id, '') AS listing_type_id,
                          COALESCE(mp.catalog_listing, 0) AS catalog_listing,
                          COALESCE(mp.sold_quantity, 0) AS sold_quantity
                   FROM productos p
                   LEFT JOIN ml_publicaciones mp ON mp.sku = p.sku AND mp.user_id = p.user_id
                   WHERE p.user_id = ?
                   ORDER BY COALESCE(p.nombre, p.marca, p.sku) COLLATE NOCASE""",
                (uid,),
            ).fetchall()
        finally:
            conn.close()

        if not rows:
            ui.label("No hay productos registrados. Cargá la pestaña Datos primero.").classes("text-gray-500 p-4")
            return

        # Dedup por sku: elegir item principal
        sku_groups: Dict[str, List[Dict]] = {}
        for r in rows:
            key = r["sku"] or r["ml_id"]
            sku_groups.setdefault(key, []).append(dict(r))

        items_by_sku: Dict[str, Dict] = {}
        for sku, grp in sku_groups.items():
            if len(grp) == 1:
                items_by_sku[sku] = grp[0]
            else:
                primary = max(
                    grp,
                    key=lambda x: (
                        1 if not x.get("catalog_listing") and
                             str(x.get("listing_type_id") or "").lower() == "gold_special" else 0,
                        int(x.get("stock") or 0),
                    ),
                )
                merged = dict(primary)
                merged["sold_quantity"] = sum(int(x.get("sold_quantity") or 0) for x in grp)
                items_by_sku[sku] = merged

        all_opts: Dict[str, str] = {
            sku: (item.get("nombre") or
                  (f"{item.get('marca', '')} {sku}".strip() if item.get("marca") else None) or
                  item.get("titulo") or sku)
            for sku, item in items_by_sku.items()
        }
        all_opts = dict(sorted(all_opts.items(), key=lambda x: x[1].lower()))
        n_total = len(all_opts)

        # ── Parámetros cotizador ─────────────────────────────────────────────
        def _pr(s: Any) -> float:
            if not s: return 0.0
            try:
                v = float(str(s).strip().replace(",", "."))
                return v if v <= 1.5 else v / 100.0
            except: return 0.0

        def _pf(s: Any) -> float:
            if not s: return 0.0
            try: return float(str(s).replace(".", "").replace(",", ".").strip()) or 0.0
            except: return 0.0

        dolar_oficial  = max(float(str(get_cotizador_param("dolar_oficial", uid) or COTIZADOR_DEFAULTS.get("dolar_oficial", "1475")).replace(",", ".")), 0.01)
        ml_comision_p  = _pr(get_cotizador_param("ml_comision",         uid) or COTIZADOR_DEFAULTS.get("ml_comision",         "0.15"))
        cuotas_6x_p    = _pr(get_cotizador_param("cuotas_6x",           uid) or COTIZADOR_DEFAULTS.get("cuotas_6x",           "0.151"))
        ml_iibb_per_p  = _pr(get_cotizador_param("ml_iibb_per",         uid) or COTIZADOR_DEFAULTS.get("ml_iibb_per",         "0.055"))
        ml_debcre_p    = _pr(get_cotizador_param("ml_debcre",           uid) or COTIZADOR_DEFAULTS.get("ml_debcre",           "0.006"))
        ml_envios_p    = max(_pf(get_cotizador_param("ml_envios",        uid) or COTIZADOR_DEFAULTS.get("ml_envios",          "5823")), 100.0)
        ml_envios_grat = _pf(get_cotizador_param("ml_envios_gratuitos", uid) or COTIZADOR_DEFAULTS.get("ml_envios_gratuitos", "33000")) or 33000.0

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

        # ── Estado ──────────────────────────────────────────────────────────
        promo_status: Dict[str, Optional[bool]] = {}
        n_promo_ref: Dict[str, int] = {"val": 0}
        ui_refs: Dict[str, Any] = {}

        def _filtered_opts() -> Dict[str, str]:
            pv = getattr(ui_refs.get("sel_promo"), "value", "con_promo") or "con_promo"
            if pv == "con_promo":
                return {s: n for s, n in all_opts.items() if promo_status.get(s) is not False}
            if pv == "sin_promo":
                return {s: n for s, n in all_opts.items() if promo_status.get(s) is not True}
            return all_opts

        def _refresh_opts() -> None:
            sel = ui_refs.get("sel_producto")
            if sel:
                opts = _filtered_opts()
                sel.options = opts
                sel.update()
                lbl = ui_refs.get("lbl_filtrados")
                if lbl:
                    lbl.set_text(str(len(opts)))

        def _update_promo_lbl() -> None:
            lbl = ui_refs.get("lbl_con_promo")
            if lbl:
                lbl.set_text(str(n_promo_ref["val"]) if n_promo_ref["val"] > 0 else "—")

        # ── Detalle inline ───────────────────────────────────────────────────
        def _render_detail(sku: str, item_info: Dict,
                           sp: Optional[Dict], item_full: Optional[Dict]) -> None:
            dc = ui_refs.get("detail_col")
            if not dc: return
            dc.clear()
            dc.style("display:block")

            has_promo = False
            sale_amt = disc_pct = disc_amt = meli_amt = seller_amt = None
            promo_type = ""
            if sp:
                a, r = sp.get("amount"), sp.get("regular_amount")
                if a is not None and r is not None:
                    af, rf = float(a), float(r)
                    if af < rf - 0.01:
                        has_promo = True
                        sale_amt = af
                        disc_amt = rf - af
                        disc_pct = disc_amt / rf * 100 if rf > 0 else 0.0
                        promo_type = (sp.get("promotion_type") or "").lower().strip()
                        if "marketplace_campaign" in promo_type:
                            meli_amt = disc_amt
                            seller_amt = 0.0

            old = promo_status.get(sku)
            promo_status[sku] = has_promo
            if has_promo and old is not True:
                n_promo_ref["val"] += 1
                _update_promo_lbl()
            elif not has_promo and old is True:
                n_promo_ref["val"] = max(0, n_promo_ref["val"] - 1)
                _update_promo_lbl()
            _refresh_opts()

            title    = (item_full or {}).get("title")             or item_info.get("titulo") or item_info.get("nombre") or sku
            price_o  = float((item_full or {}).get("price")        or item_info.get("precio") or 0)
            stock_v  = int((item_full or {}).get("available_quantity") or item_info.get("stock") or 0)
            thumb    = (item_full or {}).get("thumbnail")          or ""
            ltype    = str((item_full or {}).get("listing_type_id") or item_info.get("listing_type_id") or "").lower()
            ml_id    = item_info.get("ml_id") or ""

            pv           = float(sale_amt or price_o)
            costo_usd    = float(item_info.get("costo_usd") or 0)
            tipo_iva     = float(item_info.get("tipo_iva")  or 0.105)
            tasa_cuotas  = cuotas_6x_p if ltype == "gold_pro" else 0.0
            comision     = pv * ml_comision_p
            deb_cred     = pv * ml_debcre_p
            iibb         = pv * ml_iibb_per_p
            iva_venta    = pv * tipo_iva / (1 + tipo_iva) if tipo_iva else 0.0
            iva_meli     = comision * 0.21 / 1.21
            iva_impor    = 0.09 * costo_usd * dolar_oficial
            iva_total    = iva_venta - iva_meli - iva_impor
            envio        = ml_envios_p if pv >= ml_envios_grat else 0.0
            costo_cuotas = pv * tasa_cuotas
            costo_pesos  = costo_usd * dolar_oficial
            cobrado      = pv - comision
            if costo_pesos > 0:
                margen = cobrado - costo_pesos - iva_total - iibb - deb_cred - envio - costo_cuotas
                mgvta  = margen / pv * 100 if pv > 0 else 0.0
                mgcos  = margen / costo_pesos * 100
            else:
                margen = mgvta = mgcos = 0.0
            mcls = "font-bold text-black" if costo_pesos <= 0 else (
                "font-bold text-positive" if margen > 0 else "font-bold text-negative"
            )
            ICO   = '<i class="ti ti-calculator" style="font-size:13px;color:#BA7517"></i>'
            ICO_S = '<i class="ti ti-calculator" style="font-size:12px;color:#BA7517"></i>'

            with dc:
                with ui.card().classes("w-full p-4"):
                    with ui.row().classes("w-full gap-3 mb-2 items-start"):
                        if thumb:
                            ui.image(thumb).classes("w-16 h-16 object-contain rounded border")
                        with ui.column().classes("flex-1 min-w-0 gap-1"):
                            ui.label(f"{ml_id} · {sku}").classes("text-xs font-mono text-gray-500")
                            ui.label(title[:120]).classes("text-sm font-bold")
                            with ui.row().classes("gap-2 items-center flex-wrap"):
                                if has_promo:
                                    ui.label(fmt_m(price_o)).classes("text-sm line-through text-gray-400")
                                    ui.label(fmt_m(sale_amt)).classes("text-sm font-bold").style("color:#E24B4A")
                                    ui.label(f"↓ {fmt_p1(disc_pct)}").classes("text-xs").style("color:#E24B4A")
                                    if "marketplace_campaign" in promo_type:
                                        ui.label("ML 100%").classes("text-xs font-semibold text-blue-600 bg-blue-50 px-1.5 rounded")
                                    if meli_amt is not None:
                                        ui.label(f"Desc ML: {fmt_m(meli_amt)}").classes("text-xs text-blue-600")
                                    if seller_amt is not None:
                                        clr = "text-gray-400" if seller_amt == 0 else "text-orange-600"
                                        ui.label(f"Desc Yo: {fmt_m(seller_amt)}").classes(f"text-xs {clr}")
                                else:
                                    ui.label(fmt_m(price_o)).classes("text-sm font-bold")
                                    ui.label("Sin promo").classes("text-xs text-gray-400")
                                ui.label(f"Stock: {stock_v}").classes("text-xs text-gray-500")
                    ui.separator()
                    with ui.column().classes("gap-0 pt-2 max-w-sm"):
                        ui.label("Cotización" + (" con precio promo" if has_promo else "")).classes(
                            "text-xs font-semibold text-gray-500 mb-1"
                        )
                        with ui.row().classes("w-full justify-between py-0.5 gap-4"):
                            with ui.row().classes("items-center gap-1"):
                                ui.html(ICO)
                                ui.label("Precio de Venta").classes("text-sm font-medium text-gray-600")
                            ui.label(fmt_m(pv)).classes("text-sm font-medium")
                        for lbl, val, neg in [
                            ("Comisión ML",  comision,     True),
                            ("Costo Cuotas", costo_cuotas, True),
                            ("IVA neto",     iva_total,    True),
                        ]:
                            with ui.row().classes("w-full justify-between py-0.5 gap-4"):
                                with ui.row().classes("items-center gap-1"):
                                    ui.html(ICO)
                                    ui.label(lbl).classes("text-sm font-medium text-gray-600")
                                ui.label(fmt_m(val)).classes(f"text-sm {'text-negative' if neg else ''}")
                        with ui.column().classes("w-full bg-gray-50 rounded px-2 py-1 mb-0.5 gap-0"):
                            for sl, sv in [
                                ("IVA venta",             iva_venta),
                                ("IVA Meli (créd)",        iva_meli),
                                ("IVA importación (créd)", iva_impor),
                            ]:
                                with ui.row().classes("w-full justify-between"):
                                    with ui.row().classes("items-center gap-1"):
                                        ui.html(ICO_S)
                                        ui.label(sl).classes("text-xs font-medium text-gray-600")
                                    ui.label(fmt_m(sv)).classes("text-xs text-gray-600")
                        for lbl, val in [
                            ("Deb/Cred",                deb_cred),
                            ("IIBB ret.",               iibb),
                            ("Envío promedio Flex/Correo", envio),
                        ]:
                            with ui.row().classes("w-full justify-between py-0.5 gap-4"):
                                with ui.row().classes("items-center gap-1"):
                                    ui.html(ICO)
                                    ui.label(lbl).classes("text-sm font-medium text-gray-600")
                                ui.label(fmt_m(val)).classes("text-sm text-negative")
                        ui.separator()
                        with ui.row().classes("w-full justify-between py-0.5 gap-4"):
                            with ui.row().classes("items-center gap-1"):
                                ui.html(ICO)
                                ui.label("Costo producto").classes("text-sm font-medium text-gray-600")
                            ui.label(fmt_m(costo_pesos)).classes("text-sm text-negative")
                        ui.separator()
                        for lbl, val, isp in [
                            ("Gan $",      margen, False),
                            ("Gan Vta %",  mgvta,  True),
                            ("Gan % Cos",  mgcos,  True),
                        ]:
                            with ui.row().classes("w-full justify-between py-0.5 gap-4"):
                                with ui.row().classes("items-center gap-1"):
                                    ui.html(ICO)
                                    ui.label(lbl).classes("text-sm font-medium text-gray-600")
                                ui.label(fmt_p2(val) if isp else fmt_m(val)).classes(mcls)

        # ── Fetch al seleccionar producto ────────────────────────────────────
        async def _on_selected() -> None:
            sku = (ui_refs.get("sel_producto") and ui_refs["sel_producto"].value) or None
            dc = ui_refs.get("detail_col")
            if not sku:
                if dc:
                    dc.clear()
                    dc.style("display:none")
                return
            item_info = items_by_sku.get(str(sku)) or {}
            ml_id = item_info.get("ml_id")
            label = all_opts.get(str(sku), sku)
            if dc:
                dc.style("display:block")
                dc.clear()
                with dc:
                    with ui.row().classes("items-center gap-2 p-4"):
                        ui.spinner(size="sm")
                        ui.label(f"Consultando {label}...").classes("text-sm text-gray-600")
            if not ml_id:
                if dc:
                    dc.clear()
                    dc.style("display:block")
                    with dc:
                        ui.label(
                            f"⚠️ Sin item ML para '{sku}'. Sincronizá la pestaña Productos."
                        ).classes("text-warning p-4")
                return

            def _fetch() -> tuple:
                with ThreadPoolExecutor(max_workers=2) as ex:
                    f_sp = ex.submit(ml_get_item_sale_price_full, access_token, ml_id)
                    f_it = ex.submit(ml_get_item, access_token, ml_id)
                    sp_ = it_ = None
                    try: sp_ = f_sp.result(timeout=15)
                    except: pass
                    try: it_ = f_it.result(timeout=15)
                    except: pass
                return sp_, it_

            try:
                sp, it = await run.io_bound(_fetch)
            except Exception as err:
                if dc:
                    dc.clear()
                    dc.style("display:block")
                    with dc:
                        ui.label(f"❌ Error: {err}").classes("text-negative p-4")
                return
            _render_detail(str(sku), item_info, sp, it)

        # ── Construir UI ─────────────────────────────────────────────────────
        with ui.row().classes("w-full items-center gap-4 px-3 py-1 bg-grey-2 rounded mb-1 flex-wrap"):
            with ui.row().classes("items-baseline gap-1"):
                ui.label("Total:").classes("text-xs text-gray-500")
                ui.label(str(n_total)).classes("text-sm font-bold text-primary")
            with ui.row().classes("items-baseline gap-1"):
                ui.label("Con promo:").classes("text-xs text-gray-500")
                ui_refs["lbl_con_promo"] = ui.label("—").classes("text-sm font-bold").style("color:#E24B4A")
            with ui.row().classes("items-baseline gap-1"):
                ui.label("Filtrados:").classes("text-xs text-gray-500")
                ui_refs["lbl_filtrados"] = ui.label(str(n_total)).classes("text-sm font-bold text-primary")
            ui.space()
            ui.button("Actualizar", on_click=lambda: build_tab_promos(container)).props(
                "unelevated dense no-caps icon=refresh"
            ).style("background:#185FA5;color:#E6F1FB").classes("text-xs")

        with ui.row().classes("items-center gap-3 py-1 flex-wrap"):
            ui_refs["sel_promo"] = ui.select(
                {"todas": "Todas", "con_promo": "Con promo", "sin_promo": "Sin promo"},
                value="con_promo",
                label="Promo",
            ).classes("w-36").props("outlined dense")
            ui_refs["sel_producto"] = ui.select(
                options=all_opts,
                value=None,
                with_input=True,
                clearable=True,
                label="Buscar producto",
            ).classes("w-80").props("outlined dense")

        ui_refs["detail_col"] = ui.column().classes("w-full mt-2")
        ui_refs["detail_col"].style("display:none")

        ui_refs["sel_promo"].on_value_change(lambda *a: _refresh_opts())
        ui_refs["sel_producto"].on_value_change(
            lambda *a: background_tasks.create(_on_selected())
        )
