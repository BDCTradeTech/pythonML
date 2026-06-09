"""
tabs/promos.py
Pestaña Promos: carga items ML + promo data al iniciar.
El filtro del primer select re-puebla el segundo en tiempo real.
"""
from __future__ import annotations

import asyncio
import html as _html_esc
import json as _json
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
                        {"con_promo": "Con promo", "candidatos": "Candidatos"},
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

                    def _fdate(d: str) -> str:
                        if not d: return ""
                        try:    return _dt.fromisoformat(d.replace("Z", "+00:00")).strftime("%d/%m")
                        except: return d[:5] if len(d) >= 5 else d

                    _prelim = []
                    for row in rows:
                        rep_it = row["rep_it"] or {}
                        cand   = row["best"]
                        mlas   = sorted(row["mlas"])
                        sku    = (rep_it.get("seller_sku") or "").strip()
                        title  = (rep_it.get("title") or cand["item_id"])[:60]
                        rep_id_str = str(rep_it.get("id") or "")
                        _all_vars  = grps_by_rep.get(rep_id_str, [rep_it])
                        _gs_items = [v for v in _all_vars if "special" in str(v.get("listing_type_id") or "").lower()]
                        if _gs_items:
                            _gs        = _gs_items[0]
                            p_act      = float(_gs.get("price") or 0)
                            gs_all_ids = [str(v.get("id") or "") for v in _gs_items if v.get("id")]
                        else:
                            _pvs = [(float(v.get("price") or 0), v) for v in _all_vars if float(v.get("price") or 0) > 0]
                            if _pvs:
                                p_act, _cv = min(_pvs, key=lambda x: x[0])
                                gs_all_ids = [str(_cv.get("id") or "")]
                            else:
                                p_act      = float(rep_it.get("price") or 0)
                                gs_all_ids = [rep_id_str]
                        if not sku or p_act <= 0:
                            continue
                        sd = _fdate(cand.get("start_date") or "")
                        fd = _fdate(cand.get("finish_date") or "")
                        _prelim.append({
                            "sku":         sku,
                            "mlas":        mlas,
                            "producto":    title,
                            "precio_act":  p_act,
                            "stock":       int(rep_it.get("available_quantity") or 0),
                            "gs_all_ids":  gs_all_ids,
                            "smart_price": float(cand.get("price") or 0),
                            "promo":       cand.get("promo_name") or "",
                            "vigencia":    f"{sd} — {fd}" if (sd or fd) else "",
                            "meli_pct":    cand["meli_pct"],
                            "seller_pct":  cand["seller_pct"],
                        })

                    _sp_tasks = [(i, iid) for i, pr in enumerate(_prelim) for iid in pr["gs_all_ids"]]
                    _sp_flat  = await asyncio.gather(*[
                        run.io_bound(ml_get_seller_promotions_item, access_token, iid)
                        for _, iid in _sp_tasks
                    ])
                    _sp_by_idx: dict = {}
                    for (_idx2, _), _sp_list2 in zip(_sp_tasks, _sp_flat):
                        _sp_by_idx.setdefault(_idx2, []).append(_sp_list2)

                    table_rows = []
                    for i, pr in enumerate(_prelim):
                        smart_p = 0.0
                        disc_p  = 0.0
                        for sp_list in _sp_by_idx.get(i, []):
                            for _p in (sp_list or []):
                                _ptype = str(_p.get("type") or "").upper()
                                if _ptype == "SMART":
                                    _v = float(_p.get("price") or 0)
                                    if _v > 0:
                                        smart_p = max(smart_p, _v)
                                elif _ptype in ("PRICE_DISCOUNT", "DEAL"):
                                    _v = float(_p.get("suggested_discounted_price") or 0)
                                    if _v > 0:
                                        disc_p = max(disc_p, _v)
                        precio_ml = smart_p if smart_p > 0 else disc_p

                        all_sp_promos = []
                        for sp_list in _sp_by_idx.get(i, []):
                            all_sp_promos.extend(sp_list or [])

                        mlas_detail = []
                        for mla_id in pr["mlas"]:
                            _entry = all_items_by_id.get(mla_id)
                            if _entry:
                                _it_d      = _entry[0]
                                _lt        = str(_it_d.get("listing_type_id") or "").lower()
                                _cuotas_s  = _cuotas_desde_item(_it_d)
                                _tipo_base = "Premium" if "pro" in _lt else "Clásica"
                                _tipo_lbl  = (
                                    f"{_tipo_base} ({_cuotas_s.lstrip('x')} cuotas)"
                                    if (_cuotas_s and _cuotas_s != "x1")
                                    else f"{_tipo_base} (sin cuotas)"
                                )
                                mlas_detail.append({
                                    "id":     mla_id,
                                    "tipo":   _tipo_lbl,
                                    "precio": float(_it_d.get("price") or 0),
                                    "status": str(_it_d.get("status") or "").lower(),
                                })
                            else:
                                mlas_detail.append({"id": mla_id, "tipo": "—", "precio": 0.0, "status": "—"})

                        table_rows.append({
                            "_idx":        len(table_rows),
                            "sku":         pr["sku"],
                            "mlas":        pr["mlas"],
                            "mlas_detail": mlas_detail,
                            "producto":    pr["producto"],
                            "precio_act":  pr["precio_act"],
                            "stock":       pr["stock"],
                            "precio_ml":   precio_ml,
                            "precio_rec":  round(precio_ml * 1.8) if precio_ml > 0 else 0,
                            "promo":       pr["promo"],
                            "vigencia":    pr["vigencia"],
                            "meli_pct":    pr["meli_pct"],
                            "seller_pct":  pr["seller_pct"],
                            "_sp_promos":  all_sp_promos,
                        })

                    # ── Sort state ──────────────────────────────────────────
                    _sort_state = {"col": None, "asc": True}
                    _NUMERIC    = {"precio_act", "precio_ml", "precio_rec", "stock", "meli_pct", "seller_pct"}

                    _TH = (
                        "background:#5898D4;color:#ffffff;font-weight:600;font-size:12px;"
                        "padding:5px 6px;white-space:nowrap;position:sticky;top:0;z-index:10;"
                        "cursor:pointer;user-select:none"
                    )
                    _TD = "padding:3px 6px;font-size:12px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap"

                    _col_defs = [
                        ("SKU",         "sku",        "8%",  "left"),
                        ("Producto",    "producto",   "24%", "left"),
                        ("Stock",       "stock",      "4%",  "center"),
                        ("Precio",      "precio_act", "8%",  "right"),
                        ("Precio ML",   "precio_ml",  "8%",  "right"),
                        ("Recomendado", "precio_rec", "9%",  "right"),
                        ("ML%",         "meli_pct",   "5%",  "right"),
                        ("Yo%",         "seller_pct", "5%",  "right"),
                        ("Promo",       "promo",      "12%", "left"),
                        ("Vigencia",    "vigencia",   "10%", "center"),
                    ]

                    def _pesos(v: float) -> str:
                        return "$" + f"{int(v):,}".replace(",", ".") if v > 0 else "—"

                    _filter_text = {"v": ""}

                    def _sorted_rows() -> list:
                        col  = _sort_state["col"]
                        base = list(table_rows)
                        ft   = _filter_text["v"].strip().lower()
                        if ft:
                            base = [r for r in base if ft in r["sku"].lower() or ft in r["producto"].lower()]
                        if col is None:
                            return base
                        rev = not _sort_state["asc"]
                        if col in _NUMERIC:
                            return sorted(base, key=lambda r: float(r.get(col) or 0), reverse=rev)
                        else:
                            return sorted(base, key=lambda r: str(r.get(col) or "").lower(), reverse=rev)

                    def _on_filter_text(e) -> None:
                        _filter_text["v"] = e.value or ""
                        _render_table()

                    ui.input(
                        placeholder="Buscar producto o SKU...",
                        on_change=_on_filter_text,
                    ).props("outlined dense clearable").classes("w-80 mb-1")

                    table_wrapper = ui.element("div").style(
                        "width:100%;overflow-x:auto;overflow-y:auto;max-height:calc(100vh - 200px)"
                    )

                    def _show_prod_dlg(r) -> None:
                        sku_d        = r["sku"]
                        sp_promos    = r.get("_sp_promos", [])
                        smart_promos = [p for p in sp_promos if str(p.get("type") or "").upper() == "SMART"]

                        ICO_API  = '<i class="ti ti-checks"     style="font-size:13px;color:#22C55E;flex-shrink:0"></i>'
                        ICO_CALC = '<i class="ti ti-calculator" style="font-size:13px;color:#BA7517;flex-shrink:0"></i>'

                        def _row(ico, lbl, val):
                            with ui.row().classes("items-center gap-2 py-0.5 w-full"):
                                ui.html(ico)
                                ui.label(lbl).classes("text-xs text-gray-500").style("min-width:215px")
                                ui.label(val).classes("text-xs font-semibold")

                        dlg = ui.dialog()
                        with dlg:
                            with ui.card().style(
                                "min-width:560px;max-width:720px;padding:16px;overflow-y:auto;max-height:88vh"
                            ):
                                # ── HEADER ──────────────────────────────────────────────────
                                ui.label(r["producto"]).classes("font-bold text-sm mb-1 leading-tight")
                                with ui.row().classes("items-center gap-3 flex-wrap mb-2"):
                                    ui.label(f"SKU: {sku_d}").classes("text-xs font-mono text-gray-600")
                                    ui.label(f"Stock: {r['stock']}").classes("text-xs font-bold text-gray-700")

                                ui.label("Publicaciones").classes(
                                    "text-xs font-bold text-gray-600 uppercase tracking-wide mb-1"
                                )
                                for md in r.get("mlas_detail", []):
                                    with ui.row().classes(
                                        "items-center gap-2 py-0.5 border-b border-gray-100 flex-wrap"
                                    ):
                                        ui.label(md["id"]).classes("text-xs font-mono text-primary w-28 shrink-0")
                                        ui.label(md["tipo"]).classes("text-xs text-gray-600 flex-1")
                                        ui.label(fmt_m(md["precio"])).classes("text-xs font-semibold w-20 text-right")
                                        _sc = {"active": "#1B7A3E", "paused": "#E6A817"}.get(md["status"], "#888")
                                        ui.label(md["status"]).style(
                                            f"color:{_sc};font-size:10px;font-weight:600;"
                                            "background:#f5f5f5;border-radius:3px;padding:1px 5px"
                                        )

                                ui.separator().classes("my-2")

                                # ── MEJOR PROMO CO-FINANCIADA ────────────────────────────
                                if smart_promos:
                                    ui.label("Mejor promo co-financiada").classes(
                                        "text-xs font-bold text-gray-700 uppercase tracking-wide mb-2"
                                    )
                                    for sp in smart_promos:
                                        orig_p  = float(sp.get("original_price") or 0)
                                        prom_p  = float(sp.get("price") or 0)
                                        ml_pct  = float(sp.get("meli_percentage") or 0)
                                        sel_pct = float(sp.get("seller_percentage") or 0)
                                        status  = str(sp.get("status") or "").lower()
                                        start_d = (sp.get("start_date") or "")[:10]
                                        end_d   = (sp.get("finish_date") or "")[:10]
                                        vig     = f"{start_d} — {end_d}" if (start_d or end_d) else "—"

                                        if orig_p > 0 and prom_p > 0:
                                            desc_abs   = orig_p - prom_p
                                            ml_aporte  = min(ml_pct / 100 * orig_p, 0.10 * desc_abs)
                                            yo_aporte  = desc_abs - ml_aporte
                                            desc_pct   = desc_abs / orig_p * 100
                                            precio_rec = round(prom_p * 1.8)
                                        else:
                                            ml_aporte = yo_aporte = desc_pct = precio_rec = 0.0

                                        sty_s = STATUS_STYLES.get(status, "background:#888;color:#fff")
                                        lbl_s = STATUS_LABELS.get(status, status or "—")

                                        with ui.card().classes("w-full p-2 border border-blue-200 mb-2"):
                                            _row(ICO_API, "Promo:",
                                                 f"{sp.get('name') or '—'} ({sp.get('type') or '—'})")
                                            with ui.row().classes("items-center gap-2 py-0.5 w-full"):
                                                ui.html(ICO_API)
                                                ui.label("Status:").classes("text-xs text-gray-500").style(
                                                    "min-width:215px"
                                                )
                                                ui.label(lbl_s).style(
                                                    f"{sty_s};border-radius:4px;padding:1px 8px;"
                                                    "font-size:11px;font-weight:600"
                                                )
                                            _row(ICO_API, "Precio listado (original_price):",
                                                 fmt_m(orig_p) if orig_p else "—")
                                            _row(ICO_API, "Precio ML (price):",
                                                 fmt_m(prom_p) if prom_p else "—")
                                            _row(ICO_API, "ML %:",  fmt_p1(ml_pct))
                                            _row(ICO_API, "Yo %:",  fmt_p1(sel_pct))
                                            _row(ICO_API, "Vigencia:", vig)
                                            ui.separator().classes("my-1")
                                            _row(ICO_CALC, "ML aporta $:",
                                                 fmt_m(ml_aporte) if ml_aporte else "—")
                                            _row(ICO_CALC, "Yo aporto $:",
                                                 fmt_m(yo_aporte) if yo_aporte else "—")
                                            _row(ICO_CALC, "Descuento total %:",
                                                 fmt_p1(desc_pct) if desc_pct else "—")
                                            _row(ICO_CALC, "Precio recomendado (×1.8):",
                                                 fmt_m(precio_rec) if precio_rec else "—")

                                ui.separator().classes("my-2")

                                # ── TODAS LAS PROMOS DISPONIBLES ─────────────────────────
                                ui.label("Todas las promos disponibles").classes(
                                    "text-xs font-bold text-gray-700 uppercase tracking-wide mb-1"
                                )
                                if sp_promos:
                                    _render_promos_table(sp_promos)
                                else:
                                    ui.label("Sin datos de promos.").classes("text-xs text-gray-400 italic")

                                ui.button("Cerrar", on_click=dlg.close).props(
                                    "unelevated dense no-caps"
                                ).style("background:#185FA5;color:#fff").classes("mt-3 self-end text-sm")
                        dlg.open()

                    def _render_table() -> None:
                        table_wrapper.clear()
                        with table_wrapper:
                            with ui.element("table").style(
                                "table-layout:fixed;width:100%;border-collapse:collapse"
                            ):
                                with ui.element("thead"):
                                    with ui.element("tr"):
                                        for _lbl, _ck, _w, _a in _col_defs:
                                            with ui.element("th").style(
                                                f"width:{_w};{_TH};text-align:{_a}"
                                            ).on("click", lambda ck=_ck: _on_sort(ck)):
                                                ui.label(_lbl)
                                with ui.element("tbody"):
                                    for _i, _r in enumerate(_sorted_rows()):
                                        _bg      = "#f5f8fd" if _i % 2 == 0 else "#ffffff"
                                        _pml     = _r.get("precio_ml", 0)
                                        _rec     = _r.get("precio_rec", 0)
                                        _rec_col = (
                                            "#2e7d32"
                                            if (_rec > 0 and _r["precio_act"] >= _rec)
                                            else "#e65100"
                                        )
                                        with ui.element("tr").style(
                                            f"background:{_bg};border-bottom:1px solid #e8e8e8"
                                        ):
                                            with ui.element("td").style(f"{_TD};text-align:left"):
                                                ui.label(_r["sku"])
                                            with ui.element("td").style(f"{_TD};text-align:left"):
                                                ui.label(_r["producto"]).style(
                                                    "color:#1565c0;text-decoration:underline;"
                                                    "cursor:pointer"
                                                ).on("click", lambda r=_r: _show_prod_dlg(r))
                                            with ui.element("td").style(f"{_TD};text-align:center"):
                                                ui.label(str(_r["stock"]))
                                            with ui.element("td").style(f"{_TD};text-align:right"):
                                                ui.label(_pesos(_r["precio_act"]))
                                            with ui.element("td").style(f"{_TD};text-align:right"):
                                                ui.label(_pesos(_pml))
                                            with ui.element("td").style(
                                                f"{_TD};text-align:right;"
                                                f"color:{_rec_col};font-weight:600"
                                            ):
                                                ui.label(_pesos(_rec))
                                            with ui.element("td").style(
                                                f"{_TD};text-align:right;"
                                                "color:#2e7d32;font-weight:700"
                                            ):
                                                ui.label(f"{_r['meli_pct']:.1f}%")
                                            with ui.element("td").style(
                                                f"{_TD};text-align:right;"
                                                "color:#e65100;font-weight:700"
                                            ):
                                                ui.label(f"{_r['seller_pct']:.1f}%")
                                            with ui.element("td").style(f"{_TD};text-align:left"):
                                                ui.label(_r["promo"] or "")
                                            with ui.element("td").style(f"{_TD};text-align:center"):
                                                ui.label(_r["vigencia"])

                    def _on_sort(col) -> None:
                        if _sort_state["col"] == col:
                            _sort_state["asc"] = not _sort_state["asc"]
                        else:
                            _sort_state["col"] = col
                            _sort_state["asc"] = True
                        _render_table()

                    _render_table()

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
