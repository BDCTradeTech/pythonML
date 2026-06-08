"""
tabs/promos.py
Pestaña Promos: carga items ML + promo data al iniciar.
El filtro del primer select re-puebla el segundo en tiempo real.
"""
from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

from nicegui import app, background_tasks, run, ui

from db import get_connection, get_cotizador_param, COTIZADOR_DEFAULTS, get_financiacion_cuotas_ml
from ml_api import (
    get_ml_access_token,
    ml_get_my_items,
    _cuotas_desde_item,
    ml_get_seller_promotions_item,
    ml_get_smart_candidates,
)
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
        ml_iibb_per_p  = _pr(get_cotizador_param("ml_iibb_per",         uid) or COTIZADOR_DEFAULTS.get("ml_iibb_per",         "0.055"))
        ml_debcre_p    = _pr(get_cotizador_param("ml_debcre",           uid) or COTIZADOR_DEFAULTS.get("ml_debcre",           "0.006"))
        ml_envios_p    = max(_pf(get_cotizador_param("ml_envios",        uid) or COTIZADOR_DEFAULTS.get("ml_envios",          "5823")), 100.0)
        ml_envios_grat = _pf(get_cotizador_param("ml_envios_gratuitos", uid) or COTIZADOR_DEFAULTS.get("ml_envios_gratuitos", "33000")) or 33000.0
        fin_cuotas     = get_financiacion_cuotas_ml()  # {3: 0.084, 6: 0.123, 9: 0.157, 12: 0.192}

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

        def _tasa_cuotas(cuotas_str: str) -> float:
            if cuotas_str and cuotas_str.startswith("x") and cuotas_str[1:].isdigit():
                return fin_cuotas.get(int(cuotas_str[1:]), {}).get("pct", 0.0)
            return 0.0

        def _calc(pv: float, cuotas_str: str, costo_usd: float, tipo_iva: float) -> dict:
            tasa_q       = _tasa_cuotas(cuotas_str)
            comision     = pv * ml_comision_p
            costo_cuotas = pv * tasa_q
            deb_cred     = pv * ml_debcre_p
            iibb         = pv * ml_iibb_per_p
            iva_venta    = pv * tipo_iva / (1 + tipo_iva) if tipo_iva else 0.0
            iva_meli     = comision * 0.21 / 1.21
            iva_impor    = 0.09 * costo_usd * dolar_oficial
            iva_total    = iva_venta - iva_meli - iva_impor
            envio        = ml_envios_p if pv >= ml_envios_grat else 0.0
            costo_pesos  = costo_usd * dolar_oficial
            cobrado      = pv - comision
            if costo_pesos > 0:
                margen = cobrado - costo_pesos - iva_total - iibb - deb_cred - envio - costo_cuotas
                mgvta  = margen / pv * 100 if pv > 0 else 0.0
                mgcos  = margen / costo_pesos * 100
            else:
                margen = mgvta = mgcos = 0.0
            return dict(
                comision=comision, costo_cuotas=costo_cuotas, deb_cred=deb_cred, iibb=iibb,
                iva_venta=iva_venta, iva_meli=iva_meli, iva_impor=iva_impor, iva_total=iva_total,
                envio=envio, costo_pesos=costo_pesos, margen=margen, mgvta=mgvta, mgcos=mgcos,
            )

        # ── Área principal ───────────────────────────────────────────────────
        main_area = ui.column().classes("w-full gap-2")
        with main_area:
            with ui.card().classes("w-full p-8 items-center gap-4"):
                ui.spinner(size="xl")
                ui.label("Cargando promos...").classes("text-xl text-gray-700")

        # Estado compartido entre _cargar_async y _render_detail_async
        _state: Dict[str, Any] = {
            "seller_id":   "",
            "rep_items":   [],
            "grps_by_rep": {},
            "promo_by_id": {},
            "check_items": {},
            "prod_costs":  {},
        }

        # ── Tabla de promociones (render helper, sin estado UI) ──────────────
        STATUS_ORDER  = {"started": 0, "pending": 1, "candidate": 2, "finished": 3}
        STATUS_LABELS = {"started": "Activa", "pending": "Pendiente", "candidate": "Candidata", "finished": "Finalizada"}
        STATUS_STYLES = {
            "started":   "background:#1B7A3E;color:#fff",
            "pending":   "background:#E6A817;color:#fff",
            "candidate": "background:#888;color:#fff",
            "finished":  "background:#C0392B;color:#fff",
        }

        def _render_promos_table(all_promos: List[dict]) -> None:
            key_order: List[tuple] = []
            key_map: Dict[tuple, dict] = {}
            key_count: Dict[tuple, int] = {}
            for promo in all_promos:
                k = ((promo.get("status") or "").lower(), (promo.get("type") or ""), promo.get("price"))
                if k not in key_map:
                    key_order.append(k)
                    key_map[k] = promo
                    key_count[k] = 1
                else:
                    key_count[k] += 1
            sorted_keys = sorted(key_order, key=lambda k: STATUS_ORDER.get(k[0], 9))
            for k in sorted_keys:
                promo      = key_map[k]
                count      = key_count[k]
                status     = (promo.get("status") or "").lower()
                meli_pct   = promo.get("meli_percentage")
                seller_pct = promo.get("seller_percentage")
                price      = promo.get("price")
                start      = (promo.get("start_date") or "")[:10]
                finish     = (promo.get("finish_date") or "")[:10]
                vigencia   = f"{start} — {finish}" if (start or finish) else "—"
                if meli_pct is not None and seller_pct is not None:
                    split_txt = f"ML {fmt_p1(meli_pct)} / Yo {fmt_p1(seller_pct)}"
                    split_cls = "text-blue-700"
                elif meli_pct is None and seller_pct is None:
                    split_txt = "100% vendedor"
                    split_cls = "text-orange-600 font-medium"
                else:
                    split_txt = "—"
                    split_cls = "text-gray-400"
                name = promo.get("name") or "—"
                if count > 1:
                    name = f"{name} × {count}"
                with ui.row().classes("w-full items-center gap-2 py-1 border-b border-gray-100 flex-wrap"):
                    lbl = STATUS_LABELS.get(status, status or "—")
                    sty = STATUS_STYLES.get(status, "background:#888;color:#fff")
                    ui.label(lbl).style(
                        f"{sty};border-radius:4px;padding:1px 8px;font-size:11px;font-weight:600;white-space:nowrap"
                    )
                    ui.label(name).classes("text-xs font-medium flex-1 min-w-32")
                    ui.label(promo.get("type") or "—").classes("text-xs text-gray-400 w-20")
                    ui.label(vigencia).classes("text-xs text-gray-400 whitespace-nowrap")
                    ui.label(fmt_m(price) if price is not None else "—").classes("text-xs font-semibold w-20")
                    ui.label(split_txt).classes(f"text-xs {split_cls}")

        # ── Tarjeta de costos por variante ───────────────────────────────────
        def _render_variant_cost(items_v: List[dict], pd: dict, sp_list: List[dict]) -> None:
            it_v       = items_v[0]
            vids       = " / ".join(str(it.get("id") or "") for it in items_v)
            cuotas_str = _cuotas_desde_item(it_v)
            ltype      = str(it_v.get("listing_type_id") or "").lower()
            price_promo = pd.get("price_promo")
            has_promo   = price_promo is not None
            price_orig  = float(pd.get("regular_amount") or it_v.get("price") or 0)
            pv          = float(price_promo or price_orig)
            disc_pct    = (price_orig - pv) / price_orig * 100 if has_promo and price_orig > 0 else 0.0

            sku       = (it_v.get("seller_sku") or "").strip()
            costs     = _state["prod_costs"].get(sku, {})
            costo_usd = float(costs.get("costo_usd") or 0)
            tipo_iva  = float(costs.get("tipo_iva") or 0.105)

            c = _calc(pv, cuotas_str, costo_usd, tipo_iva)

            active_promos    = [p for p in sp_list if (p.get("status") or "").lower() == "started"]
            candidate_promos = [p for p in sp_list if (p.get("status") or "").lower() in ("candidate", "pending")]

            _act_ml  = next((p for p in active_promos if float(p.get("meli_percentage") or 0) > 0), None)
            bonif_ml = float(_act_ml["meli_percentage"]) * price_orig / 100 if (_act_ml and has_promo) else 0.0
            margen   = c["margen"] + bonif_ml
            mcls     = "font-bold text-black" if costo_usd <= 0 else (
                "font-bold text-positive" if margen > 0 else "font-bold text-negative"
            )

            ICO   = '<i class="ti ti-calculator" style="font-size:13px;color:#BA7517"></i>'
            ICO_S = '<i class="ti ti-calculator" style="font-size:12px;color:#BA7517"></i>'

            # Tipo badge
            if "pro" in ltype:
                type_sty = "background:#e3f2fd;color:#1565c0"
                type_base = "Premium"
            else:
                type_sty = "background:#f3e5f5;color:#6a1b9a"
                type_base = "Clásica"
            if cuotas_str and cuotas_str != "x1":
                type_lbl = f"{type_base} ({cuotas_str.lstrip('x')} cuotas)"
            else:
                type_lbl = f"{type_base} (sin cuotas)"

            with ui.card().classes("w-full p-2 border border-gray-200"):
                # Cabecera de variante
                with ui.row().classes("items-center gap-2 mb-0.5 flex-wrap"):
                    ui.label(vids).classes("text-xs font-mono text-gray-500")
                    ui.label(type_lbl).style(
                        f"{type_sty};border-radius:4px;padding:1px 6px;font-size:11px;font-weight:600"
                    )

                # Precio
                with ui.row().classes("items-baseline gap-2 mb-0.5 flex-wrap"):
                    if has_promo:
                        ui.label(fmt_m(price_orig)).classes("text-sm line-through text-gray-400")
                        ui.label(fmt_m(pv)).classes("text-sm font-bold").style("color:#E24B4A")
                        ui.label(f"↓ {fmt_p1(disc_pct)}").classes("text-xs").style("color:#E24B4A")
                    else:
                        ui.label(fmt_m(pv)).classes("text-sm font-bold text-gray-700")

                # Promos activas en esta variante
                for p in active_promos:
                    if not (p.get("name") or "").strip():
                        continue
                    _mp = p.get("meli_percentage")
                    _sp = p.get("seller_percentage")
                    with ui.row().classes("items-center gap-1 mb-0.5 flex-wrap"):
                        ui.label("▶").style("color:#1B7A3E;font-size:10px;line-height:1.2")
                        ui.label(p.get("name")).classes("text-xs text-gray-600").style("line-height:1.2")
                        if _mp is not None and _sp is not None:
                            with ui.row().classes("items-center gap-0.5"):
                                ui.label(f"ML {fmt_p1(_mp)}").style("font-size:11px;color:#1565c0;font-weight:500")
                                ui.label("/").style("font-size:11px;color:#9e9e9e")
                                ui.label(f"Yo {fmt_p1(_sp)}").style("font-size:11px;color:#E65100;font-weight:500")
                        elif float(_mp or 0) == 0 and _sp is not None:
                            ui.label("100% vendedor").style("font-size:11px;color:#E65100;font-weight:600")
                # Candidatas/pendientes
                for p in candidate_promos:
                    if not (p.get("name") or "").strip():
                        continue
                    with ui.row().classes("items-center gap-1 mb-0.5"):
                        ui.label("○").classes("text-xs text-gray-400").style("font-size:10px")
                        ui.label(p.get("name")).classes("text-xs text-gray-400 italic").style("line-height:1.2")

                ui.separator().classes("my-0.5")

                # Desglose de costos
                with ui.column().classes("gap-0.5 w-full"):
                    with ui.row().classes("w-full justify-between py-0"):
                        with ui.row().classes("items-center gap-1"):
                            ui.html(ICO)
                            ui.label("Precio Venta").classes("text-xs font-medium text-gray-600")
                        ui.label(fmt_m(pv)).classes("text-xs font-medium")
                    _n_q = int(cuotas_str[1:]) if cuotas_str and cuotas_str.startswith("x") and cuotas_str[1:].isdigit() else 0
                    _tasa_pct = fin_cuotas.get(_n_q, {}).get("pct", 0.0) if _n_q > 1 else 0.0
                    _cuotas_lbl = f"Costo Cuotas ({f'{_tasa_pct*100:.1f}'.replace('.', ',')}%)" if _tasa_pct > 0 else "Costo Cuotas"
                    for lbl_t, val in [
                        ("Comisión ML",  c["comision"]),
                        (_cuotas_lbl,    c["costo_cuotas"]),
                        ("IVA neto",     c["iva_total"]),
                    ]:
                        with ui.row().classes("w-full justify-between py-0"):
                            with ui.row().classes("items-center gap-1"):
                                ui.html(ICO)
                                ui.label(lbl_t).classes("text-xs font-medium text-gray-600")
                            ui.label(fmt_m(val)).classes("text-xs text-negative")
                    with ui.column().classes("w-full bg-gray-50 rounded px-2 py-0.5 mb-0.5 gap-0"):
                        for sl, sv in [
                            ("IVA venta",             c["iva_venta"]),
                            ("IVA Meli (créd)",        c["iva_meli"]),
                            ("IVA importación (créd)", c["iva_impor"]),
                        ]:
                            with ui.row().classes("w-full justify-between"):
                                with ui.row().classes("items-center gap-1"):
                                    ui.html(ICO_S)
                                    ui.label(sl).classes("text-xs font-medium text-gray-500")
                                ui.label(fmt_m(sv)).classes("text-xs text-gray-500")
                    for lbl_t, val in [
                        ("Deb/Cred",          c["deb_cred"]),
                        ("IIBB ret.",          c["iibb"]),
                        ("Envío Flex/Correo",  c["envio"]),
                    ]:
                        with ui.row().classes("w-full justify-between py-0"):
                            with ui.row().classes("items-center gap-1"):
                                ui.html(ICO)
                                ui.label(lbl_t).classes("text-xs font-medium text-gray-600")
                            ui.label(fmt_m(val)).classes("text-xs text-negative")
                    if bonif_ml > 0:
                        with ui.row().classes("w-full justify-between py-0"):
                            with ui.row().classes("items-center gap-1"):
                                ui.html(ICO)
                                ui.label("ML aporta").classes("text-xs font-medium text-gray-600")
                            ui.label("+" + fmt_m(bonif_ml)).classes("text-xs text-positive font-medium")
                    ui.separator().classes("my-0")
                    with ui.row().classes("w-full justify-between py-0.5"):
                        with ui.row().classes("items-center gap-1"):
                            ui.html(ICO)
                            ui.label("Costo producto").classes("text-xs font-medium text-gray-600")
                        ui.label(fmt_m(c["costo_pesos"])).classes("text-xs text-negative")
                    ui.separator().classes("my-0")
                    for lbl_t, val, isp in [
                        ("Gan $",     margen,       False),
                        ("Gan Vta %", c["mgvta"],   True),
                        ("Gan % Cos", c["mgcos"],   True),
                    ]:
                        with ui.row().classes("w-full justify-between py-0"):
                            with ui.row().classes("items-center gap-1"):
                                ui.html(ICO)
                                ui.label(lbl_t).classes("text-xs font-medium text-gray-600")
                            ui.label(fmt_p2(val) if isp else fmt_m(val)).classes(f"text-xs {mcls}")

        def _group_by_fingerprint(variants: List[tuple]) -> List[dict]:
            groups: Dict[tuple, dict] = {}
            for it_v, pd, sp_list in variants:
                price_orig  = round(float(pd.get("regular_amount") or it_v.get("price") or 0), 2)
                pp          = pd.get("price_promo")
                price_promo = round(float(pp), 2) if pp is not None else None
                listing     = str(it_v.get("listing_type_id") or "").lower()
                cuotas      = _cuotas_desde_item(it_v)
                key = (price_orig, price_promo, listing, cuotas)
                if key not in groups:
                    groups[key] = {"items": [], "pd": pd, "sp_lists": []}
                groups[key]["items"].append(it_v)
                groups[key]["sp_lists"].append(sp_list)
            result = []
            for g in groups.values():
                merged: Dict[str, dict] = {}
                for sl in g["sp_lists"]:
                    for promo in sl:
                        pid = str(promo.get("id") or promo.get("name") or id(promo))
                        if pid not in merged:
                            merged[pid] = promo
                result.append({"items": g["items"], "pd": g["pd"], "sp_list": list(merged.values())})
            return result

        # ── Carga asíncrona principal ─────────────────────────────────────────
        async def _cargar_async() -> None:
            try:
                data = await run.io_bound(ml_get_my_items, access_token, False)
            except Exception as e:
                main_area.clear()
                with main_area:
                    ui.label(f"❌ Error al conectar con ML: {e}").classes("text-negative p-4")
                return

            items_raw: List[dict] = data.get("results", [])
            _state["seller_id"] = str(data.get("seller_id") or "")

            if not items_raw:
                main_area.clear()
                with main_area:
                    ui.label("No se encontraron publicaciones activas en MercadoLibre.").classes("text-gray-500 p-4")
                return

            # Dedup por SKU
            grps: Dict[tuple, List[dict]] = {}
            for it in items_raw:
                grps.setdefault(_cuotas_key(it), []).append(it)

            rep_items: List[dict] = []
            grps_by_rep: Dict[str, List[dict]] = {}
            for grp in grps.values():
                rep = max(grp, key=_cuotas_score)
                rep_id = str(rep.get("id") or "")
                rep_items.append(rep)
                grps_by_rep[rep_id] = [_it for _it in grp if _it.get("id")]

            _state["rep_items"]   = rep_items
            _state["grps_by_rep"] = grps_by_rep

            # Fetch promo data con progreso
            total = len(rep_items)
            main_area.clear()
            promo_lbl = None
            with main_area:
                with ui.card().classes("w-full p-8 items-center gap-4"):
                    ui.spinner(size="xl")
                    promo_lbl = ui.label(f"Verificando promos 0/{total}...").classes("text-xl text-gray-700")

            _empty_pd: Dict = {"price_promo": None, "meli_pct": None, "seller_pct": None}
            promo_by_id: Dict[str, dict] = {}
            check_items: Dict[str, dict] = {}
            sid = _state["seller_id"]

            for i, it in enumerate(rep_items):
                iid = str(it.get("id") or "")
                if not iid:
                    continue
                _grp_items = grps_by_rep.get(iid, [it])
                _pds = await asyncio.gather(*[
                    run.io_bound(_get_promo_data, access_token, str(_it.get("id") or ""), sid)
                    for _it in _grp_items
                ])
                best_pd, best_price, best_item = _empty_pd, None, it
                for _it2, _pd in zip(_grp_items, _pds):
                    _pp = _pd.get("price_promo")
                    if _pp is not None and (best_price is None or float(_pp) < best_price):
                        best_price, best_pd, best_item = float(_pp), _pd, _it2
                promo_by_id[iid] = best_pd
                check_items[iid]  = best_item
                if promo_lbl:
                    promo_lbl.set_text(f"Verificando promos {i + 1}/{total}...")

            _state["promo_by_id"] = promo_by_id
            _state["check_items"] = check_items

            # Costos desde BD
            try:
                conn = get_connection()
                for r in conn.execute(
                    "SELECT sku, costo_usd, tipo_iva FROM productos WHERE user_id = ?", (uid,)
                ).fetchall():
                    _state["prod_costs"][r["sku"]] = {
                        "costo_usd": float(r["costo_usd"] or 0),
                        "tipo_iva":  float(r["tipo_iva"]  or 0.105),
                    }
                conn.close()
            except Exception:
                pass

            n_con_promo = sum(
                1 for it in rep_items
                if promo_by_id.get(str(it.get("id") or ""), {}).get("price_promo") is not None
            )

            def _build_opts(filtro: str) -> Dict[str, str]:
                opts: Dict[str, str] = {}
                for it in rep_items:
                    iid   = str(it.get("id") or "")
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
                        {"todas": "Todas", "con_promo": "Con promo", "sin_promo": "Sin promo", "candidatos": "Candidatos"},
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
                candidatos_col = ui.column().classes("w-full mt-2")
                candidatos_col.style("display:none")

            # ── Render detalle async ─────────────────────────────────────────
            async def _render_detail_async(item_id: str) -> None:
                it = next((x for x in rep_items if str(x.get("id") or "") == item_id), None)
                if not it:
                    detail_col.clear()
                    detail_col.style("display:none")
                    return

                grp_items = grps_by_rep.get(item_id, [it])

                # Fetch seller-promotions + promo data por variante en paralelo
                sp_results, pd_results = await asyncio.gather(
                    asyncio.gather(*[
                        run.io_bound(ml_get_seller_promotions_item, access_token, str(_it.get("id") or ""))
                        for _it in grp_items
                    ]),
                    asyncio.gather(*[
                        run.io_bound(_get_promo_data, access_token, str(_it.get("id") or ""), _state["seller_id"])
                        for _it in grp_items
                    ]),
                )
                sp_lists = list(sp_results)
                pd_list  = list(pd_results)

                # Dedup promos por ID (misma campaña puede aparecer en varias variantes)
                all_promos: Dict[str, dict] = {}
                for sp_list in sp_lists:
                    for promo in sp_list:
                        pid = str(promo.get("id") or promo.get("name") or id(promo))
                        if pid not in all_promos:
                            all_promos[pid] = promo

                # Clasificar variantes
                with_promo:    List[tuple] = []
                without_promo: List[tuple] = []
                for _it, pd, sp_list in zip(grp_items, pd_list, sp_lists):
                    has_active = (
                        pd.get("price_promo") is not None or
                        any((p.get("status") or "").lower() == "started" for p in sp_list)
                    )
                    if has_active:
                        with_promo.append((_it, pd, sp_list))
                    else:
                        without_promo.append((_it, pd, sp_list))

                # Info del rep-item para el header
                thumb   = it.get("thumbnail") or ""
                title   = it.get("title") or item_id
                sku_rep = (it.get("seller_sku") or "").strip()
                stock_v = int(it.get("available_quantity") or 0)
                pd0     = promo_by_id.get(item_id, {})

                detail_col.clear()
                detail_col.style("display:block")
                with detail_col:
                    all_variants = with_promo + without_promo
                    groups = _group_by_fingerprint(all_variants)
                    with ui.element("div").style(
                        "display:grid;grid-template-columns:repeat(4,1fr);gap:8px;width:100%;align-items:stretch"
                    ):
                        with ui.card().classes("w-full p-2 border border-gray-200"):
                            # Datos del producto
                            with ui.row().classes("w-full gap-3 mb-2 items-start"):
                                if thumb:
                                    ui.image(thumb).classes("w-14 h-14 object-contain rounded border flex-shrink-0")
                                with ui.column().classes("flex-1 min-w-0 gap-0.5"):
                                    ui.label(f"{item_id}" + (f" · {sku_rep}" if sku_rep else "")).classes(
                                        "text-xs font-mono text-gray-500"
                                    )
                                    ui.label(title[:120]).classes("text-xs font-bold leading-tight")
                                    with ui.row().classes("gap-2 items-center flex-wrap mt-0.5"):
                                        if pd0.get("price_promo") is not None:
                                            p0  = float(pd0.get("regular_amount") or it.get("price") or 0)
                                            pv0 = float(pd0["price_promo"])
                                            d0  = (p0 - pv0) / p0 * 100 if p0 > 0 else 0
                                            ui.label(fmt_m(p0)).classes("text-xs line-through text-gray-400")
                                            ui.label(fmt_m(pv0)).classes("text-xs font-bold").style("color:#E24B4A")
                                            ui.label(f"↓ {fmt_p1(d0)}").classes("text-xs").style("color:#E24B4A")
                                        else:
                                            ui.label(fmt_m(it.get("price"))).classes("text-xs font-bold text-gray-700")
                                            ui.label("Sin promo").classes("text-xs text-gray-400")
                                        ui.label(f"Stock: {stock_v}").classes("text-xs text-gray-500")
                            ui.separator().classes("my-1")
                            ui.label("Promociones").classes(
                                "text-xs font-bold text-gray-600 mb-1 uppercase tracking-wide"
                            )
                            if all_promos:
                                _render_promos_table(list(all_promos.values()))
                            else:
                                ui.label("Sin promociones registradas").classes(
                                    "text-xs text-gray-400 italic"
                                )
                        for grp in groups:
                            _render_variant_cost(grp["items"], grp["pd"], grp["sp_list"])

            # ── Candidatos async ─────────────────────────────────────────────
            async def _cargar_candidatos_async() -> None:
                from datetime import datetime as _dt

                candidatos_col.clear()
                candidatos_col.style("display:block")
                with candidatos_col:
                    with ui.card().classes("w-full p-8 items-center gap-4"):
                        ui.spinner(size="xl")
                        ui.label("Buscando oportunidades...").classes("text-xl text-gray-700")

                all_items_by_id: Dict[str, tuple] = {}
                for rid, grp in grps_by_rep.items():
                    for it in grp:
                        all_items_by_id[str(it.get("id") or "")] = (it, rid)
                rep_by_id = {str(it.get("id") or ""): it for it in rep_items}

                raw = await run.io_bound(ml_get_smart_candidates, access_token, _state["seller_id"])

                by_rep: Dict[str, dict] = {}
                for cand in raw:
                    item_id = cand["item_id"]
                    entry   = all_items_by_id.get(item_id)
                    rep_id  = entry[1] if entry else item_id
                    it_data = entry[0] if entry else {}
                    if rep_id not in by_rep:
                        by_rep[rep_id] = {"rep_it": rep_by_id.get(rep_id) or it_data, "mlas": set(), "best": cand}
                    by_rep[rep_id]["mlas"].add(item_id)
                    if cand["meli_pct"] > by_rep[rep_id]["best"]["meli_pct"]:
                        by_rep[rep_id]["best"] = cand

                rows = sorted(by_rep.values(), key=lambda x: x["best"]["meli_pct"], reverse=True)

                candidatos_col.clear()
                candidatos_col.style("display:block")
                with candidatos_col:
                    if not rows:
                        ui.label("No hay candidatos en promos co-financiadas actualmente.").classes(
                            "text-gray-500 p-4 italic"
                        )
                        return

                    n = len(rows)
                    ui.label(
                        f"{n} SKU{'s' if n != 1 else ''} con oportunidades en promos co-financiadas"
                    ).classes("text-sm font-bold text-primary mb-2")

                    def _fdate(d: str) -> str:
                        if not d: return ""
                        try:    return _dt.fromisoformat(d.replace("Z", "+00:00")).strftime("%d/%m")
                        except: return d[:5] if len(d) >= 5 else d

                    columns = [
                        {"name": "sku",        "label": "SKU",             "field": "sku",        "align": "left",   "sortable": True},
                        {"name": "mla",        "label": "MLA",             "field": "mla",        "align": "left"},
                        {"name": "producto",   "label": "Producto",        "field": "producto",   "align": "left"},
                        {"name": "precio_act", "label": "Precio actual",   "field": "precio_act", "align": "right",  "sortable": True},
                        {"name": "stock",      "label": "Stock",           "field": "stock",      "align": "center", "sortable": True},
                        {"name": "promo",      "label": "Promo",           "field": "promo",      "align": "left"},
                        {"name": "vigencia",   "label": "Vigencia",        "field": "vigencia",   "align": "center"},
                        {"name": "meli_pct",   "label": "ML aporta %",     "field": "meli_pct",   "align": "right",  "sortable": True},
                        {"name": "seller_pct", "label": "Yo pongo %",      "field": "seller_pct", "align": "right",  "sortable": True},
                        {"name": "precio_sug", "label": "Precio sugerido", "field": "precio_sug", "align": "right"},
                        {"name": "desc_total", "label": "Dto total %",     "field": "desc_total", "align": "right",  "sortable": True},
                    ]
                    table_rows = []
                    for row in rows:
                        rep_it = row["rep_it"] or {}
                        cand   = row["best"]
                        mlas   = sorted(row["mlas"])
                        sku    = (rep_it.get("seller_sku") or "").strip()
                        title  = (rep_it.get("title") or cand["item_id"])[:60]
                        p_act  = float(rep_it.get("price") or 0)
                        stock  = int(rep_it.get("available_quantity") or 0)
                        p_sug  = float(cand.get("price") or 0)
                        p_orig = float(cand.get("original_price") or p_act or 0)
                        desc   = round((p_orig - p_sug) / p_orig * 100, 1) if p_orig > 0 and p_sug > 0 else 0
                        sd = _fdate(cand.get("start_date") or "")
                        fd = _fdate(cand.get("finish_date") or "")
                        table_rows.append({
                            "sku":        sku or "—",
                            "mla":        " / ".join(mlas),
                            "producto":   title,
                            "precio_act": p_act,
                            "stock":      stock,
                            "promo":      cand.get("promo_name") or "",
                            "vigencia":   f"{sd} — {fd}" if (sd or fd) else "",
                            "meli_pct":   cand["meli_pct"],
                            "seller_pct": cand["seller_pct"],
                            "precio_sug": p_sug,
                            "desc_total": desc,
                        })

                    tbl = ui.table(columns=columns, rows=table_rows, row_key="mla").classes("w-full text-xs")
                    tbl.add_slot("body-cell-precio_act", """
                        <q-td :props="props">{{ props.value > 0 ? '$' + Number(props.value).toLocaleString('es-AR', {maximumFractionDigits:0}) : '—' }}</q-td>
                    """)
                    tbl.add_slot("body-cell-meli_pct", """
                        <q-td :props="props"><span style="color:#2e7d32;font-weight:700">{{ props.value }}%</span></q-td>
                    """)
                    tbl.add_slot("body-cell-seller_pct", """
                        <q-td :props="props"><span style="color:#e65100;font-weight:700">{{ props.value }}%</span></q-td>
                    """)
                    tbl.add_slot("body-cell-precio_sug", """
                        <q-td :props="props">{{ props.value > 0 ? '$' + Number(props.value).toLocaleString('es-AR', {maximumFractionDigits:0}) : '—' }}</q-td>
                    """)
                    tbl.add_slot("body-cell-desc_total", """
                        <q-td :props="props">{{ props.value > 0 ? props.value + '%' : '—' }}</q-td>
                    """)

            # ── Event handlers ────────────────────────────────────────────────
            def _on_filter_change(_=None) -> None:
                if sel_promo.value == "candidatos":
                    sel_prod.style("display:none")
                    detail_col.clear()
                    detail_col.style("display:none")
                    background_tasks.create(_cargar_candidatos_async(), name="cargar_candidatos")
                    return
                sel_prod.style("display:block")
                opts = _build_opts(sel_promo.value or "todas")
                sel_prod.options = opts
                sel_prod.value = None
                sel_prod.update()
                detail_col.clear()
                detail_col.style("display:none")
                candidatos_col.clear()
                candidatos_col.style("display:none")

            def _on_product_change(_=None) -> None:
                iid = sel_prod.value
                if not iid:
                    detail_col.clear()
                    detail_col.style("display:none")
                    return
                detail_col.clear()
                detail_col.style("display:block")
                with detail_col:
                    with ui.card().classes("w-full p-4 items-center gap-3"):
                        ui.spinner()
                        ui.label("Cargando detalle de promos...").classes("text-sm text-gray-500")
                background_tasks.create(_render_detail_async(str(iid)), name="render_promo_detail")

            sel_promo.on_value_change(_on_filter_change)
            sel_prod.on_value_change(_on_product_change)

        background_tasks.create(_cargar_async(), name="cargar_promos")
