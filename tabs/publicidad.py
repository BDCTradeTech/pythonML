"""
Fase 2 — tabs/publicidad.py
Pestaña Publicidad (ML Ads / Product Ads): KPIs de campañas, TACOS y ranking de productos.
Página de SOLO LECTURA: lee el cache que arma ads_snapshot.py (cron diario), no hace
llamadas en vivo a la API de Ads — por eso siempre muestra cuándo fue la última sincronización.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from nicegui import app, background_tasks, run, ui

from ml_api import (
    get_ml_access_token, ml_get_user_profile, ml_get_user_id,
    ml_get_orders_incremental, compute_ventas_periodo,
)
from db import (
    get_ads_advertiser, get_ads_campaigns, get_ads_campaign_daily_range,
    get_ads_item_snapshot, get_ads_sync_freshness,
)
from tabs.estadisticas import fmt_m, fmt_n

PERIODOS = [("7d", "7 días"), ("30d", "30 días"), ("mes", "Mes actual")]

KPI_TOOLTIPS = {
    "inversion": "Lo que gastaste en publicidad (Product Ads) en el período.",
    "ventas": "Ventas atribuidas a publicidad: directas (compraron el producto anunciado) + "
              "indirectas (compraron otro producto tuyo después de hacer clic en el anuncio).",
    "acos": "ACOS = Inversión / Ventas por ads × 100. Cuánto de cada venta por ads se fue en "
            "publicidad. Más bajo es mejor.",
    "roas": "ROAS = Ventas por ads / Inversión. Cuánto vendiste por cada $1 invertido en "
            "publicidad. Más alto es mejor.",
    "tacos": "TACOS = Inversión en ads / Ventas TOTALES de la tienda en el período (no solo "
             "las de ads). Mide qué tan dependiente sos de la publicidad para vender.",
    "unidades": "Unidades vendidas atribuidas a publicidad (directas + indirectas) en el período.",
}


def _require_login() -> Optional[Dict[str, Any]]:
    user = app.storage.user.get("user")
    if not user:
        ui.notify("Debes iniciar sesión para continuar", color="negative")
    return user


def _rango_periodo(periodo: str, hoy: date) -> tuple:
    if periodo == "7d":
        return hoy - timedelta(days=6), hoy
    if periodo == "30d":
        return hoy - timedelta(days=29), hoy
    return hoy.replace(day=1), hoy  # "mes"


def _agrupar_daily_por_campania(daily_rows: List[Dict[str, Any]]) -> Dict[int, Dict[str, float]]:
    """Suma las filas diarias cacheadas (ml_ads_campaign_metrics_daily) por campaign_id."""
    out: Dict[int, Dict[str, float]] = {}
    campos = ("clicks", "prints", "cost", "direct_amount", "indirect_amount", "total_amount",
              "direct_units_quantity", "indirect_units_quantity", "units_quantity")
    for r in daily_rows:
        cid = r.get("campaign_id")
        if cid is None:
            continue
        acc = out.setdefault(cid, {c: 0.0 for c in campos})
        for c in campos:
            acc[c] += float(r.get(c) or 0)
    return out


def _info_icon(tooltip: str) -> None:
    ui.element("i").classes("ti ti-info-circle").style(
        "font-size:12px;color:#9ca3af;cursor:help;margin-left:3px"
    ).tooltip(tooltip)


def _kpi_tile(label: str, value: str, sub: str, color: str, tooltip: str) -> None:
    with ui.element("div").style(
        "flex:1;min-width:140px;background:#fff;border:1px solid #e0e2e7;border-radius:10px;padding:10px 14px"
    ):
        with ui.element("div").style("display:flex;align-items:center"):
            ui.label(label).style(
                "font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:.05em;font-weight:500"
            )
            _info_icon(tooltip)
        ui.label(value).style(f"font-size:20px;font-weight:700;color:{color};line-height:1.3;margin-top:2px")
        if sub:
            ui.label(sub).style("font-size:11px;color:#6b7280")


def build_tab_publicidad(container) -> None:
    """Pestaña Publicidad: KPIs de campañas ML Ads, TACOS y ranking de productos. Solo lectura
    — lee el cache que arma ads_snapshot.py (cron diario), no llama a la API de Ads en vivo."""
    user = _require_login()
    if not user:
        return
    uid = user["id"]
    access_token = get_ml_access_token(uid)
    if not access_token:
        with container:
            with ui.column().classes("w-full max-w-2xl gap-4"):
                ui.label("Publicidad").classes("text-2xl font-semibold")
                ui.label(
                    "Conectá tu cuenta de MercadoLibre en Configuración para ver aquí tus "
                    "campañas de Publicidad."
                ).classes("text-gray-600")
        return

    with container:
        with ui.column().classes("w-full p-8 items-center gap-4"):
            ui.spinner(size="xl")
            ui.label("Cargando Publicidad...").classes("text-xl text-gray-700")

    async def _cargar_async() -> None:
        advertiser = get_ads_advertiser(uid)
        campaigns_dim = get_ads_campaigns(uid) if advertiser else []
        freshness = get_ads_sync_freshness(uid)

        hoy = date.today()
        datos_periodo: Dict[str, Dict[str, Any]] = {}
        for periodo, _label in PERIODOS:
            d_from, d_to = _rango_periodo(periodo, hoy)
            daily_rows = get_ads_campaign_daily_range(uid, d_from.isoformat(), d_to.isoformat())
            datos_periodo[periodo] = {
                "por_campania": _agrupar_daily_por_campania(daily_rows),
                "items": get_ads_item_snapshot(uid, periodo),
                "d_from": d_from, "d_to": d_to,
            }

        # Ventas totales de la tienda (misma fuente/lógica que tabs/estadisticas.py) -- para TACOS.
        ventas_tienda_periodo: Dict[str, float] = {p: 0.0 for p, _ in PERIODOS}
        try:
            profile = await run.io_bound(ml_get_user_profile, access_token)
            seller_id = (profile or {}).get("id") or await run.io_bound(ml_get_user_id, access_token)
            if seller_id:
                orders_data = await run.io_bound(ml_get_orders_incremental, access_token, str(seller_id), uid)
                results = orders_data.get("results") or []
                for periodo, _label in PERIODOS:
                    d_from, d_to = datos_periodo[periodo]["d_from"], datos_periodo[periodo]["d_to"]
                    ventas_tienda_periodo[periodo] = compute_ventas_periodo(results, d_from, d_to)["monto"]
        except Exception:
            logging.getLogger(__name__).exception("[PUBLICIDAD] error calculando ventas totales para TACOS")

        _pintar(advertiser, campaigns_dim, datos_periodo, ventas_tienda_periodo, freshness)

    def _pintar(advertiser, campaigns_dim, datos_periodo, ventas_tienda_periodo, freshness) -> None:
        estado: Dict[str, Any] = {"periodo": "30d", "vista": "campana", "campania_expandida": None}
        campanias_por_id = {c["campaign_id"]: c for c in campaigns_dim}

        container.clear()
        with container:
            with ui.column().classes("w-full gap-3"):
                with ui.element("div").style(
                    "background:#eff6ff;border:1px solid #bfdbfe;border-radius:8px;padding:10px 14px"
                ):
                    ui.label(
                        "Publicidad (Product Ads): cuánto invertís, cuánto vendés por publicidad "
                        "y qué tan dependiente sos de ella (TACOS). Pasá el mouse sobre el ⓘ de "
                        "cada indicador para ver cómo se calcula."
                    ).style("font-size:12px;color:#1e40af")

                if not advertiser:
                    with ui.element("div").style(
                        "background:#fff;border:1px solid #e0e2e7;border-radius:10px;padding:24px;text-align:center"
                    ):
                        ui.label(
                            "Tu cuenta no tiene Publicidad (Product Ads) habilitada en MercadoLibre."
                        ).style("color:#6b7280")
                    return

                if not campaigns_dim:
                    with ui.element("div").style(
                        "background:#fff;border:1px solid #e0e2e7;border-radius:10px;padding:24px;text-align:center"
                    ):
                        ui.label("Todavía no hay datos sincronizados de Publicidad.").style(
                            "color:#6b7280;font-weight:600"
                        )
                        ui.label("La primera sincronización corre esta noche (cron diario).").style(
                            "color:#9ca3af;font-size:12px"
                        )
                    return

                freshness_str = freshness[:16].replace("T", " ") if freshness else "—"
                ui.label(f"Actualizado: {freshness_str} (cache diario, no en vivo)").style(
                    "font-size:11px;color:#9ca3af"
                )

                periodo_row = ui.row().classes("gap-2")
                kpi_row = ui.row().classes("w-full gap-2 flex-wrap items-stretch")
                vista_row = ui.row().classes("gap-2 mt-1")
                tabla_container = ui.column().classes("w-full mt-1")

                def _render_periodo_buttons() -> None:
                    periodo_row.clear()
                    with periodo_row:
                        for key, label in PERIODOS:
                            activo = estado["periodo"] == key
                            (ui.button(label, on_click=lambda k=key: _set_periodo(k))
                             .props("unelevated dense no-caps" if activo else "flat dense no-caps")
                             .style(f"background:{'#1d4ed8' if activo else 'transparent'};"
                                    f"color:{'#fff' if activo else '#374151'};"
                                    f"border:1px solid {'#1d4ed8' if activo else '#e0e2e7'}"))

                def _render_vista_buttons() -> None:
                    vista_row.clear()
                    with vista_row:
                        for key, label in [("campana", "Por campaña"), ("producto", "Por producto")]:
                            activo = estado["vista"] == key
                            (ui.button(label, on_click=lambda k=key: _set_vista(k))
                             .props("unelevated dense no-caps" if activo else "flat dense no-caps")
                             .style(f"background:{'#16a34a' if activo else 'transparent'};"
                                    f"color:{'#fff' if activo else '#374151'};"
                                    f"border:1px solid {'#16a34a' if activo else '#e0e2e7'}"))

                def _render_kpis() -> None:
                    kpi_row.clear()
                    d = datos_periodo[estado["periodo"]]
                    total_cost = sum(v["cost"] for v in d["por_campania"].values())
                    total_amount = sum(v["total_amount"] for v in d["por_campania"].values())
                    total_units = sum(v["units_quantity"] for v in d["por_campania"].values())
                    ventas_tienda = ventas_tienda_periodo.get(estado["periodo"], 0.0)
                    acos = (total_cost / total_amount * 100) if total_amount > 0 else 0.0
                    roas = (total_amount / total_cost) if total_cost > 0 else 0.0
                    tacos = (total_cost / ventas_tienda * 100) if ventas_tienda > 0 else 0.0
                    with kpi_row:
                        _kpi_tile("Inversión", fmt_m(total_cost), "", "#dc2626", KPI_TOOLTIPS["inversion"])
                        _kpi_tile("Ventas x ads", fmt_m(total_amount), "", "#16a34a", KPI_TOOLTIPS["ventas"])
                        _kpi_tile("ACOS", f"{acos:.1f}%".replace(".", ","), "", "#f59e0b", KPI_TOOLTIPS["acos"])
                        _kpi_tile("ROAS", f"{roas:.2f}x".replace(".", ","), "", "#1d4ed8", KPI_TOOLTIPS["roas"])
                        _kpi_tile("TACOS", f"{tacos:.1f}%".replace(".", ","),
                                  f"vs. {fmt_m(ventas_tienda)} facturado", "#7c3aed", KPI_TOOLTIPS["tacos"])
                        _kpi_tile("Unidades", fmt_n(total_units), "", "#374151", KPI_TOOLTIPS["unidades"])

                def _render_items_subtabla(items: List[Dict[str, Any]]) -> None:
                    if not items:
                        ui.label("Sin ítems con actividad en este período.").style(
                            "color:#9ca3af;font-size:11px;padding:10px 16px"
                        )
                        return
                    with ui.element("table").style("width:100%;border-collapse:collapse;font-size:11px"):
                        with ui.element("tbody"):
                            for it in items:
                                titulo = (it.get("title") or it.get("item_id") or "—")
                                titulo = titulo[:55] + ("…" if len(titulo) > 55 else "")
                                cost_i = float(it.get("cost") or 0)
                                amt_i = float(it.get("total_amount") or 0)
                                roas_i = float(it.get("roas") or 0)
                                with ui.element("tr").style("border-bottom:1px solid #f0f0f0"):
                                    with ui.element("td").style("padding:4px 16px 4px 32px;width:45%"):
                                        ui.label(titulo).style("color:#374151")
                                    with ui.element("td").style("padding:4px 10px;text-align:right"):
                                        ui.label(fmt_m(cost_i)).style("color:#dc2626")
                                    with ui.element("td").style("padding:4px 10px;text-align:right"):
                                        ui.label(fmt_m(amt_i)).style("color:#16a34a")
                                    with ui.element("td").style("padding:4px 10px 4px 4px;text-align:right;width:80px"):
                                        ui.label(f"{roas_i:.2f}x".replace(".", ",")).style(
                                            "color:#1d4ed8;font-weight:600"
                                        )

                def _render_tabla_campanias(d: Dict[str, Any]) -> None:
                    filas = [(cid, campanias_por_id.get(cid, {}), m) for cid, m in d["por_campania"].items()]
                    filas.sort(key=lambda x: x[2]["cost"], reverse=True)
                    if not filas:
                        ui.label("Sin actividad de campañas en este período.").style(
                            "color:#9ca3af;padding:16px"
                        )
                        return
                    with ui.element("div").style(
                        "background:#fff;border:1px solid #e0e2e7;border-radius:10px;overflow:hidden"
                    ):
                        with ui.element("table").style("width:100%;border-collapse:collapse;font-size:12px"):
                            with ui.element("thead"):
                                with ui.element("tr").style("background:#f9fafb"):
                                    for h, align in [("Campaña", "left"), ("Estado", "left"),
                                                      ("Inversión", "right"), ("Ventas", "right"),
                                                      ("ACOS", "right"), ("ROAS", "right"), ("Unid.", "right")]:
                                        with ui.element("th").style(
                                            f"padding:6px 10px;text-align:{align};font-weight:600;"
                                            f"color:#6b7280;border-bottom:1px solid #e0e2e7"
                                        ):
                                            ui.label(h)
                            with ui.element("tbody"):
                                for cid, dim, m in filas:
                                    acos_c = (m["cost"] / m["total_amount"] * 100) if m["total_amount"] > 0 else 0.0
                                    roas_c = (m["total_amount"] / m["cost"]) if m["cost"] > 0 else 0.0
                                    expandida = estado["campania_expandida"] == cid
                                    estado_color = "#16a34a" if dim.get("status") == "active" else "#9ca3af"
                                    with ui.element("tr").classes("hover:bg-gray-50 cursor-pointer").style(
                                        "border-bottom:1px solid #f3f4f6"
                                    ).on("click", lambda c=cid: _toggle_expandir(c)):
                                        with ui.element("td").style("padding:6px 10px"):
                                            ui.label(f"{'▾' if expandida else '▸'} {dim.get('name') or cid}").style(
                                                "font-weight:500"
                                            )
                                        with ui.element("td").style("padding:6px 10px"):
                                            ui.label((dim.get("status") or "—").capitalize()).style(
                                                f"color:{estado_color}"
                                            )
                                        with ui.element("td").style("padding:6px 10px;text-align:right"):
                                            ui.label(fmt_m(m["cost"]))
                                        with ui.element("td").style("padding:6px 10px;text-align:right"):
                                            ui.label(fmt_m(m["total_amount"]))
                                        with ui.element("td").style("padding:6px 10px;text-align:right"):
                                            ui.label(f"{acos_c:.1f}%".replace(".", ","))
                                        with ui.element("td").style("padding:6px 10px;text-align:right"):
                                            ui.label(f"{roas_c:.2f}x".replace(".", ","))
                                        with ui.element("td").style("padding:6px 10px;text-align:right"):
                                            ui.label(fmt_n(m["units_quantity"]))
                                    if expandida:
                                        items_camp = [it for it in d["items"] if it.get("campaign_id") == cid]
                                        items_camp.sort(key=lambda it: it.get("roas") or 0, reverse=True)
                                        with ui.element("tr"):
                                            with ui.element("td").props("colspan=7").style(
                                                "padding:0;background:#fafafa"
                                            ):
                                                _render_items_subtabla(items_camp)

                def _render_tabla_productos(d: Dict[str, Any]) -> None:
                    items = list(d["items"])
                    if not items:
                        ui.label("Sin actividad de productos en este período.").style(
                            "color:#9ca3af;padding:16px"
                        )
                        return
                    max_val = max(
                        (max(float(it.get("cost") or 0), float(it.get("total_amount") or 0)) for it in items),
                        default=0,
                    ) or 1
                    with ui.element("div").style(
                        "background:#fff;border:1px solid #e0e2e7;border-radius:10px;padding:12px 14px;"
                        "display:flex;flex-direction:column;gap:8px"
                    ):
                        for it in items[:50]:
                            titulo = (it.get("title") or it.get("item_id") or "—")
                            titulo = titulo[:60] + ("…" if len(titulo) > 60 else "")
                            cost_i = float(it.get("cost") or 0)
                            amt_i = float(it.get("total_amount") or 0)
                            roas_i = float(it.get("roas") or 0)
                            pct_cost = min(cost_i / max_val * 100, 100)
                            pct_amt = min(amt_i / max_val * 100, 100)
                            with ui.element("div").style(
                                "display:flex;flex-direction:column;gap:2px;padding-bottom:6px;"
                                "border-bottom:1px solid #f3f4f6"
                            ):
                                with ui.element("div").style(
                                    "display:flex;justify-content:space-between;align-items:baseline"
                                ):
                                    ui.label(titulo).style("font-size:12px;color:#111827;font-weight:500")
                                    ui.label(f"ROAS {roas_i:.2f}x".replace(".", ",")).style(
                                        "font-size:11px;color:#1d4ed8;font-weight:600"
                                    )
                                with ui.element("div").style("display:flex;align-items:center;gap:6px"):
                                    ui.label("Invertí").style("font-size:9px;color:#9ca3af;width:38px")
                                    with ui.element("div").style(
                                        "flex:1;height:6px;background:#f3f4f6;border-radius:3px;overflow:hidden"
                                    ):
                                        ui.element("div").style(f"height:6px;width:{pct_cost:.1f}%;background:#dc2626")
                                    ui.label(fmt_m(cost_i)).style("font-size:10px;color:#dc2626;width:80px;text-align:right")
                                with ui.element("div").style("display:flex;align-items:center;gap:6px"):
                                    ui.label("Vendí").style("font-size:9px;color:#9ca3af;width:38px")
                                    with ui.element("div").style(
                                        "flex:1;height:6px;background:#f3f4f6;border-radius:3px;overflow:hidden"
                                    ):
                                        ui.element("div").style(f"height:6px;width:{pct_amt:.1f}%;background:#16a34a")
                                    ui.label(fmt_m(amt_i)).style("font-size:10px;color:#16a34a;width:80px;text-align:right")
                        if len(items) > 50:
                            ui.label(
                                f"Mostrando los primeros 50 de {len(items)} productos (ordenados por ROAS)."
                            ).style("font-size:11px;color:#9ca3af;margin-top:4px")

                def _render_tabla() -> None:
                    tabla_container.clear()
                    d = datos_periodo[estado["periodo"]]
                    with tabla_container:
                        if estado["vista"] == "campana":
                            _render_tabla_campanias(d)
                        else:
                            _render_tabla_productos(d)

                def _set_periodo(k: str) -> None:
                    estado["periodo"] = k
                    estado["campania_expandida"] = None
                    _render_periodo_buttons()
                    _render_kpis()
                    _render_tabla()

                def _set_vista(k: str) -> None:
                    estado["vista"] = k
                    estado["campania_expandida"] = None
                    _render_vista_buttons()
                    _render_tabla()

                def _toggle_expandir(cid: int) -> None:
                    estado["campania_expandida"] = None if estado["campania_expandida"] == cid else cid
                    _render_tabla()

                _render_periodo_buttons()
                _render_kpis()
                _render_vista_buttons()
                _render_tabla()

    background_tasks.create(_cargar_async(), name="cargar_publicidad")
