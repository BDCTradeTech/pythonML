"""
tabs/promos.py
Pestaña Promos: carga items ML + promo data al iniciar.
El filtro del primer select re-puebla el segundo en tiempo real.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from nicegui import app, background_tasks, run, ui

from db import get_connection, get_cotizador_param, COTIZADOR_DEFAULTS
from ml_api import get_ml_access_token, ml_get_my_items
from tabs.cuotas import _cuotas_key, _cuotas_score, _get_promo_data


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

        # ── Área principal ───────────────────────────────────────────────────
        main_area = ui.column().classes("w-full gap-2")
        with main_area:
            with ui.card().classes("w-full p-8 items-center gap-4"):
                ui.spinner(size="xl")
                ui.label("Cargando publicaciones...").classes("text-xl text-gray-700")

        async def _cargar_async() -> None:
            # Paso 1: obtener items de ML
            try:
                data = await run.io_bound(ml_get_my_items, access_token, False)
            except Exception as e:
                main_area.clear()
                with main_area:
                    ui.label(f"❌ Error al conectar con ML: {e}").classes("text-negative p-4")
                return

            items_raw: List[dict] = data.get("results", [])
            seller_id: str = str(data.get("seller_id") or "")

            if not items_raw:
                main_area.clear()
                with main_area:
                    ui.label("No se encontraron publicaciones activas en MercadoLibre.").classes("text-gray-500 p-4")
                return

            # Paso 2: dedup por SKU (igual que cuotas)
            grps: Dict[tuple, List[dict]] = {}
            for it in items_raw:
                grps.setdefault(_cuotas_key(it), []).append(it)
            rep_items: List[dict] = [max(grp, key=_cuotas_score) for grp in grps.values()]

            # Para cada grupo, usar gold_special para verificar promo (igual que cuotas.py)
            # gold_pro gana el score de display pero la promo se aplica sobre gold_special
            _promo_ids: Dict[str, str] = {}
            _check_items: Dict[str, dict] = {}
            for rep_it, grp in zip(rep_items, grps.values()):
                rep_id = str(rep_it.get("id") or "")
                check_id = rep_id
                check_item = rep_it
                for _it in grp:
                    if not _it.get("catalog_listing") and str(_it.get("listing_type_id") or "").lower() == "gold_special":
                        check_id = str(_it.get("id") or "")
                        check_item = _it
                        break
                _promo_ids[rep_id] = check_id
                _check_items[rep_id] = check_item

            # Paso 3: fetch promo data con progreso
            total = len(rep_items)
            main_area.clear()
            promo_lbl = None
            with main_area:
                with ui.card().classes("w-full p-8 items-center gap-4"):
                    ui.spinner(size="xl")
                    promo_lbl = ui.label(f"Verificando promos 0/{total}...").classes("text-xl text-gray-700")

            promo_by_id: Dict[str, dict] = {}
            for i, it in enumerate(rep_items):
                iid = str(it.get("id") or "")
                check_id = _promo_ids.get(iid, iid)
                if iid:
                    promo_by_id[iid] = await run.io_bound(_get_promo_data, access_token, check_id, seller_id)
                if promo_lbl:
                    promo_lbl.set_text(f"Verificando promos {i + 1}/{total}...")

            # Paso 4: costos desde BD
            prod_costs: Dict[str, dict] = {}
            try:
                conn = get_connection()
                for r in conn.execute(
                    "SELECT sku, costo_usd, tipo_iva FROM productos WHERE user_id = ?", (uid,)
                ).fetchall():
                    prod_costs[r["sku"]] = {
                        "costo_usd": float(r["costo_usd"] or 0),
                        "tipo_iva":  float(r["tipo_iva"]  or 0.105),
                    }
                conn.close()
            except Exception:
                pass

            # Paso 5: construir UI
            n_con_promo = sum(
                1 for it in rep_items
                if promo_by_id.get(str(it.get("id") or ""), {}).get("price_promo") is not None
            )

            def _build_opts(filtro: str) -> Dict[str, str]:
                opts: Dict[str, str] = {}
                for it in rep_items:
                    iid  = str(it.get("id") or "")
                    has_p = promo_by_id.get(iid, {}).get("price_promo") is not None
                    if filtro == "con_promo" and not has_p:
                        continue
                    if filtro == "sin_promo" and has_p:
                        continue
                    opts[iid] = it.get("title") or iid
                return dict(sorted(opts.items(), key=lambda x: x[1].lower()))

            main_area.clear()

            with main_area:
                # Stats bar
                with ui.row().classes("w-full items-center gap-4 px-3 py-1 bg-grey-2 rounded mb-1 flex-wrap"):
                    with ui.row().classes("items-baseline gap-1"):
                        ui.label("Total:").classes("text-xs text-gray-500")
                        ui.label(str(len(rep_items))).classes("text-sm font-bold text-primary")
                    with ui.row().classes("items-baseline gap-1"):
                        ui.label("Con promo:").classes("text-xs text-gray-500")
                        ui.label(str(n_con_promo)).classes("text-sm font-bold").style("color:#E24B4A")
                    ui.space()
                    ui.button("Actualizar", on_click=lambda: build_tab_promos(container)).props(
                        "unelevated dense no-caps icon=refresh"
                    ).style("background:#185FA5;color:#E6F1FB").classes("text-xs")

                # Filtros
                with ui.row().classes("items-center gap-3 py-1 flex-wrap"):
                    sel_promo = ui.select(
                        {"todas": "Todas", "con_promo": "Con promo", "sin_promo": "Sin promo"},
                        value="con_promo",
                        label="Promo",
                    ).classes("w-36").props("outlined dense")
                    sel_prod = ui.select(
                        options=_build_opts("con_promo"),
                        value=None,
                        with_input=True,
                        clearable=True,
                        label="Buscar producto",
                    ).classes("w-80").props("outlined dense")

                detail_col = ui.column().classes("w-full mt-2")
                detail_col.style("display:none")

            # ── Render detalle (100% desde memoria, sin API calls) ────────────
            def _render_detail(item_id: str) -> None:
                detail_col.clear()
                it = next((x for x in rep_items if str(x.get("id") or "") == item_id), None)
                if not it:
                    detail_col.style("display:none")
                    return

                pd          = promo_by_id.get(item_id, {})
                price_promo = pd.get("price_promo")
                has_promo   = price_promo is not None
                # Si hay promo, precio y tipo de publicación vienen del gold_special
                calc_item   = _check_items.get(item_id, it) if has_promo else it
                price_o     = float(pd.get("regular_amount") or calc_item.get("price") or 0)
                pv          = float(price_promo or price_o)
                disc_pct    = (price_o - pv) / price_o * 100 if has_promo and price_o > 0 else 0.0
                meli_pct    = pd.get("meli_pct")
                seller_pct  = pd.get("seller_pct")

                sku       = (it.get("seller_sku") or "").strip()
                costs     = prod_costs.get(sku, {})
                costo_usd = float(costs.get("costo_usd") or 0)
                tipo_iva  = float(costs.get("tipo_iva")  or 0.105)

                ltype        = str(calc_item.get("listing_type_id") or "").lower()
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

                thumb   = it.get("thumbnail") or ""
                title   = it.get("title") or item_id
                stock_v = int(it.get("available_quantity") or 0)
                ICO     = '<i class="ti ti-calculator" style="font-size:13px;color:#BA7517"></i>'
                ICO_S   = '<i class="ti ti-calculator" style="font-size:12px;color:#BA7517"></i>'

                detail_col.style("display:block")
                with detail_col:
                    with ui.card().classes("w-full p-4"):
                        with ui.row().classes("w-full gap-3 mb-2 items-start"):
                            if thumb:
                                ui.image(thumb).classes("w-16 h-16 object-contain rounded border")
                            with ui.column().classes("flex-1 min-w-0 gap-1"):
                                ui.label(f"{item_id}" + (f" · {sku}" if sku else "")).classes("text-xs font-mono text-gray-500")
                                ui.label(title[:120]).classes("text-sm font-bold")
                                with ui.row().classes("gap-2 items-center flex-wrap"):
                                    if has_promo:
                                        ui.label(fmt_m(price_o)).classes("text-sm line-through text-gray-400")
                                        ui.label(fmt_m(pv)).classes("text-sm font-bold").style("color:#E24B4A")
                                        ui.label(f"↓ {fmt_p1(disc_pct)}").classes("text-xs").style("color:#E24B4A")
                                        if meli_pct is not None:
                                            ui.label(f"ML: {fmt_p1(meli_pct)}").classes("text-xs text-blue-600")
                                        if seller_pct is not None:
                                            clr = "text-gray-400" if float(seller_pct or 0) == 0 else "text-orange-600"
                                            ui.label(f"Yo: {fmt_p1(seller_pct)}").classes(f"text-xs {clr}")
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
                            for lbl_t, val in [
                                ("Comisión ML",  comision),
                                ("Costo Cuotas", costo_cuotas),
                                ("IVA neto",     iva_total),
                            ]:
                                with ui.row().classes("w-full justify-between py-0.5 gap-4"):
                                    with ui.row().classes("items-center gap-1"):
                                        ui.html(ICO)
                                        ui.label(lbl_t).classes("text-sm font-medium text-gray-600")
                                    ui.label(fmt_m(val)).classes("text-sm text-negative")
                            with ui.column().classes("w-full bg-gray-50 rounded px-2 py-1 mb-0.5 gap-0"):
                                for sl, sv in [
                                    ("IVA venta",              iva_venta),
                                    ("IVA Meli (créd)",         iva_meli),
                                    ("IVA importación (créd)",  iva_impor),
                                ]:
                                    with ui.row().classes("w-full justify-between"):
                                        with ui.row().classes("items-center gap-1"):
                                            ui.html(ICO_S)
                                            ui.label(sl).classes("text-xs font-medium text-gray-600")
                                        ui.label(fmt_m(sv)).classes("text-xs text-gray-600")
                            for lbl_t, val in [
                                ("Deb/Cred",                  deb_cred),
                                ("IIBB ret.",                  iibb),
                                ("Envío promedio Flex/Correo", envio),
                            ]:
                                with ui.row().classes("w-full justify-between py-0.5 gap-4"):
                                    with ui.row().classes("items-center gap-1"):
                                        ui.html(ICO)
                                        ui.label(lbl_t).classes("text-sm font-medium text-gray-600")
                                    ui.label(fmt_m(val)).classes("text-sm text-negative")
                            ui.separator()
                            with ui.row().classes("w-full justify-between py-0.5 gap-4"):
                                with ui.row().classes("items-center gap-1"):
                                    ui.html(ICO)
                                    ui.label("Costo producto").classes("text-sm font-medium text-gray-600")
                                ui.label(fmt_m(costo_pesos)).classes("text-sm text-negative")
                            ui.separator()
                            for lbl_t, val, isp in [
                                ("Gan $",     margen, False),
                                ("Gan Vta %", mgvta,  True),
                                ("Gan % Cos", mgcos,  True),
                            ]:
                                with ui.row().classes("w-full justify-between py-0.5 gap-4"):
                                    with ui.row().classes("items-center gap-1"):
                                        ui.html(ICO)
                                        ui.label(lbl_t).classes("text-sm font-medium text-gray-600")
                                    ui.label(fmt_p2(val) if isp else fmt_m(val)).classes(mcls)

            # ── Event handlers ────────────────────────────────────────────────
            def _on_filter_change(_=None) -> None:
                opts = _build_opts(sel_promo.value or "todas")
                sel_prod.options = opts
                sel_prod.value = None
                sel_prod.update()
                detail_col.clear()
                detail_col.style("display:none")

            def _on_product_change(_=None) -> None:
                iid = sel_prod.value
                if not iid:
                    detail_col.clear()
                    detail_col.style("display:none")
                    return
                _render_detail(str(iid))

            sel_promo.on_value_change(_on_filter_change)
            sel_prod.on_value_change(_on_product_change)

        background_tasks.create(_cargar_async(), name="cargar_promos")
