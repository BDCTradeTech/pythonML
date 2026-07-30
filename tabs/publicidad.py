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
    get_ads_item_snapshot, get_ads_sync_freshness, get_last_cron_run_at, get_ads_items_stock,
    get_ganancia_real_por_item,
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
    "pub_ganancia": "Cuánto de tu ganancia real se va en publicidad: Inversión ads ÷ Ganancia "
                    "real del período (la ganancia ANTES de restar la publicidad). Ganancia "
                    "real = lo que efectivamente ganás después de costos, comisiones e "
                    "impuestos (no las ventas brutas). Si no hay ninguna venta con ganancia "
                    "calculada en el período (falta costo cargado en Productos), no se puede "
                    "calcular.",
}

# Columnas comunes a la tabla de campañas, las filas de ítem expandidas y "Por producto" --
# se comparten los mismos anchos para que quede todo alineado (punto 1 del refinamiento).
COL_WIDTHS = {"nombre": "34%", "inversion": "12%", "ventas": "12%", "unidades": "10%",
              "stock": "10%", "acos": "11%", "roas": "11%"}
# Solo "Por producto": agrega Ganancia real y Ganancia neta (= ganancia - inversión ads).
# "Por campaña" no lleva estas columnas.
COL_WIDTHS_PRODUCTO = {"nombre": "26%", "inversion": "9%", "ventas": "9%", "unidades": "8%",
                        "stock": "8%", "acos": "9%", "roas": "9%",
                        "ganancia": "11%", "ganancia_neta": "11%"}
N_COLS = 7  # para los colspan de filas "nota" (sin actividad / ocultos / cap de 50)
N_COLS_PRODUCTO = 9
ACOS_ALERTA_PCT = 20.0  # a partir de este ACOS, se pinta en ámbar (nivel "alto")

# Estado del anuncio (item-level, ads/search) o de la campaña (campaigns/search) -- documentado
# en https://developers.mercadolibre.com.ar/es_ar/pads-read. El puntito antes del nombre usa
# esto; el tooltip lo explica (punto 2 del refinamiento). Campañas solo usan active/paused;
# los ítems pueden traer además hold/idle/delegated/revoked.
STATUS_META = {
    "active":    ("#16a34a", "Activo"),
    "paused":    ("#9ca3af", "Pausado"),
    "hold":      ("#dc2626", "En espera: la publicación está pausada o sin stock a nivel Mercado Libre"),
    "idle":      ("#9ca3af", "Disponible para publicidad, pero no está en ninguna campaña"),
    "delegated": ("#9ca3af", "Delegado a otro anunciante"),
    "revoked":   ("#9ca3af", "Devuelto por el anunciante al que estaba delegado"),
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


def _acos_estado(cost: float, amt: float) -> tuple:
    """(texto, color, es_alerta). es_alerta=True cuando hubo gasto sin ninguna venta atribuida
    -- la fuga de plata en ads que el punto 3 del refinamiento pide remarcar en rojo (no
    ocultar). Si además no hubo gasto (item inactivo revelado con "Mostrar"), no es alerta."""
    if amt <= 0:
        if cost > 0:
            return "s/ventas", "#dc2626", True
        return "s/ventas", "#9ca3af", False
    acos = cost / amt * 100
    color = "#16a34a" if acos <= ACOS_ALERTA_PCT else "#f59e0b"
    return f"{acos:.1f}%".replace(".", ","), color, False


def _split_actividad(items: List[Dict[str, Any]]) -> tuple:
    """Separa los ítems con inversión o ventas del período de los que no tuvieron actividad
    (0 inversión y 0 ventas) -- estos últimos no se muestran por default (punto 4)."""
    activos = [it for it in items
               if float(it.get("cost") or 0) != 0 or float(it.get("total_amount") or 0) != 0]
    return activos, len(items) - len(activos)


def _dedupe_titulos(items: List[Dict[str, Any]]) -> Dict[Any, str]:
    """Publicaciones distintas (item_id distinto) pueden compartir título -- no es un bug de
    duplicados. Devuelve, por item_id, un sufijo tipo ' · pub 2' cuando el título se repite,
    para que quede claro que son ítems distintos (punto 6)."""
    por_titulo: Dict[str, List[Any]] = {}
    for it in items:
        t = (it.get("title") or it.get("item_id") or "—").strip()
        por_titulo.setdefault(t, []).append(it.get("item_id"))
    sufijos: Dict[Any, str] = {}
    for ids in por_titulo.values():
        if len(ids) <= 1:
            continue
        for i, item_id in enumerate(sorted(ids, key=str), start=1):
            sufijos[item_id] = f" · pub {i}"
    return sufijos


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


def _th(label: str, align: str, width: str, tooltip: Optional[str] = None) -> None:
    el = ui.element("th").style(
        f"padding:6px 10px;text-align:{align};font-weight:600;color:#6b7280;"
        f"border-bottom:1px solid #e0e2e7;width:{width}"
    )
    if tooltip:
        el = el.tooltip(tooltip)
    with el:
        ui.label(label)


def _render_header(nombre_label: str, widths: Dict[str, str] = COL_WIDTHS, con_ganancia: bool = False) -> None:
    with ui.element("thead"):
        with ui.element("tr").style("background:#f9fafb"):
            _th(nombre_label, "left", widths["nombre"])
            _th("Inversión", "right", widths["inversion"])
            _th("Ventas", "right", widths["ventas"])
            _th("Vendidas", "right", widths["unidades"], tooltip=KPI_TOOLTIPS["unidades"])
            _th("Stock", "right", widths["stock"])
            _th("ACOS", "right", widths["acos"])
            _th("ROAS", "right", widths["roas"])
            if con_ganancia:
                _th("Ganancia", "right", widths["ganancia"])
                _th("Ganancia neta", "right", widths["ganancia_neta"])


def _render_fila_metrica(nombre: str, status: Optional[str], cost: float, amt: float,
                          units: float, *, sufijo: str = "", stock: Optional[int] = None,
                          stock_resumen: Optional[tuple] = None, ganancia: Optional[float] = None,
                          ganancia_neta: Optional[float] = None, mostrar_ganancia: bool = False,
                          widths: Dict[str, str] = COL_WIDTHS, indent: bool = False,
                          muted: bool = False, font_size: str = "12px", on_click=None) -> None:
    """Fila con las columnas comunes Nombre | Inversión | Ventas | Unid. | Stock | ACOS | ROAS
    (+ Ganancia | Ganancia neta si mostrar_ganancia=True, solo usado por "Por producto"), usada
    tanto para filas de campaña como de ítem/producto -- así quedan alineadas entre sí y con el
    header (punto 1). El ACOS por ítem sale calculado y coloreado por nivel, y el gasto sin
    ventas se remarca en rojo + ⚠ en vez de leerse como fila neutra (punto 3). El stock no se
    puede sumar/promediar a nivel campaña (mezclaría productos distintos) -- filas de campaña
    pasan stock_resumen=(con_stock, con_dato) y se muestra "X/Y con stock"; filas de ítem/
    producto pasan stock (el número) como antes."""
    acos_txt, acos_color, es_alerta = _acos_estado(cost, amt)
    dot_color, dot_tooltip = STATUS_META.get(status, ("#9ca3af", f"Estado: {status}" if status else "Sin dato"))
    tr = ui.element("tr").style(f"border-bottom:1px solid {'#f5f5f5' if muted else '#f3f4f6'}")
    if on_click:
        tr = tr.classes("hover:bg-gray-50 cursor-pointer").on("click", on_click)
    with tr:
        with ui.element("td").style(
            f"padding:6px 10px 6px {'30px' if indent else '10px'};width:{widths['nombre']};max-width:0"
        ):
            with ui.element("div").style("display:flex;align-items:center;gap:6px;min-width:0"):
                ui.element("span").style(
                    "display:inline-block;flex:none;width:6px;height:6px;border-radius:50%;"
                    f"background:{dot_color}"
                ).tooltip(dot_tooltip)
                if es_alerta:
                    ui.label("⚠").style(
                        "flex:none;font-size:11px;color:#dc2626;line-height:1"
                    ).tooltip("Gasto sin ninguna venta atribuida en el período")
                with ui.element("div").style(
                    "min-width:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis"
                ):
                    ui.label(nombre + sufijo).style(
                        f"font-size:{font_size};color:{'#6b7280' if muted else '#111827'};"
                        f"font-weight:{'400' if muted else '500'}"
                    )
        with ui.element("td").style(f"padding:6px 10px;text-align:right;width:{widths['inversion']}"):
            ui.label(fmt_m(cost)).style(f"font-size:{font_size};color:#dc2626")
        with ui.element("td").style(f"padding:6px 10px;text-align:right;width:{widths['ventas']}"):
            ui.label(fmt_m(amt)).style(f"font-size:{font_size};color:#16a34a")
        with ui.element("td").style(f"padding:6px 10px;text-align:right;width:{widths['unidades']}"):
            ui.label(fmt_n(units)).style(f"font-size:{font_size};color:{'#6b7280' if muted else '#374151'}")
        with ui.element("td").style(f"padding:6px 10px;text-align:right;width:{widths['stock']}"):
            if stock_resumen is not None:
                con_stock, con_dato = stock_resumen
                if con_dato == 0:
                    ui.label("s/dato").style(f"font-size:{font_size};color:#9ca3af").tooltip(
                        "Ningún producto de esta campaña tiene snapshot de stock"
                    )
                else:
                    ui.label(f"{con_stock}/{con_dato} con stock").style(
                        f"font-size:{font_size};color:{'#dc2626' if con_stock == 0 else '#374151'}"
                    ).tooltip(
                        f"{con_dato} producto(s) de esta campaña tienen dato de stock conocido; "
                        f"de esos, {con_stock} tienen stock disponible (>0). El stock es un dato "
                        "por producto -- no se suma ni promedia a nivel campaña."
                    )
            elif stock is None:
                ui.label("—").style(f"font-size:{font_size};color:#d1d5db").tooltip(
                    "Sin snapshot de stock para este ítem"
                )
            else:
                ui.label(fmt_n(stock)).style(
                    f"font-size:{font_size};font-weight:{'700' if stock == 0 else '400'};"
                    f"color:{'#dc2626' if stock == 0 else ('#6b7280' if muted else '#374151')}"
                )
        with ui.element("td").style(f"padding:6px 10px;text-align:right;width:{widths['acos']}"):
            ui.label(acos_txt).style(f"font-size:{font_size};color:{acos_color};font-weight:600")
        roas = (amt / cost) if cost > 0 else 0.0
        with ui.element("td").style(f"padding:6px 10px;text-align:right;width:{widths['roas']}"):
            ui.label(f"{roas:.2f}x".replace(".", ",")).style(
                f"font-size:{font_size};color:{'#dc2626' if es_alerta else '#1d4ed8'};font-weight:600"
            )
        if mostrar_ganancia:
            with ui.element("td").style(f"padding:6px 10px;text-align:right;width:{widths['ganancia']}"):
                if ganancia is None:
                    ui.label("s/dato").style(f"font-size:{font_size};color:#9ca3af").tooltip(
                        "Sin ganancia real calculada para este producto en el período "
                        "(sin ventas reales registradas, o falta costo cargado en Productos)"
                    )
                else:
                    ui.label(fmt_m(ganancia)).style(f"font-size:{font_size};color:#374151;font-weight:500")
            with ui.element("td").style(f"padding:6px 10px;text-align:right;width:{widths['ganancia_neta']}"):
                if ganancia_neta is None:
                    ui.label("s/dato").style(f"font-size:{font_size};color:#9ca3af").tooltip(
                        "No se puede calcular sin la ganancia real de este producto"
                    )
                else:
                    ui.label(fmt_m(ganancia_neta)).style(
                        f"font-size:{font_size};font-weight:700;"
                        f"color:{'#16a34a' if ganancia_neta >= 0 else '#dc2626'}"
                    )


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
        last_sync_attempt = get_last_cron_run_at("ads", uid)
        stock_por_item = get_ads_items_stock(uid)

        hoy = date.today()
        datos_periodo: Dict[str, Dict[str, Any]] = {}
        for periodo, _label in PERIODOS:
            d_from, d_to = _rango_periodo(periodo, hoy)
            daily_rows = get_ads_campaign_daily_range(uid, d_from.isoformat(), d_to.isoformat())
            datos_periodo[periodo] = {
                "por_campania": _agrupar_daily_por_campania(daily_rows),
                "items": get_ads_item_snapshot(uid, periodo),
                "ganancia": get_ganancia_real_por_item(uid, d_from.isoformat(), d_to.isoformat()),
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

        _pintar(advertiser, campaigns_dim, datos_periodo, ventas_tienda_periodo,
                freshness, last_sync_attempt, stock_por_item)

    def _pintar(advertiser, campaigns_dim, datos_periodo, ventas_tienda_periodo,
                freshness, last_sync_attempt, stock_por_item) -> None:
        estado: Dict[str, Any] = {
            "periodo": "30d", "vista": "campana", "campania_expandida": None,
            "mostrar_ocultos": set(),  # claves "camp:<cid>" / "producto" -- ver punto 4
        }
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
                        if last_sync_attempt:
                            ui.label(
                                "Ya sincronizamos tu cuenta, pero no tiene Publicidad (Product Ads) "
                                "habilitada en MercadoLibre."
                            ).style("color:#6b7280;font-weight:600")
                        else:
                            ui.label("Todavía no sincronizamos tu cuenta de Publicidad.").style(
                                "color:#6b7280;font-weight:600"
                            )
                            ui.label("La primera sincronización corre esta noche (cron diario).").style(
                                "color:#9ca3af;font-size:12px"
                            )
                    return

                if not campaigns_dim:
                    synced_at = advertiser.get("synced_at")
                    synced_str = synced_at[:16].replace("T", " ") if synced_at else "—"
                    with ui.element("div").style(
                        "background:#fff;border:1px solid #e0e2e7;border-radius:10px;padding:24px;text-align:center"
                    ):
                        ui.label("Ya sincronizamos tu cuenta, pero no tenés campañas de Publicidad.").style(
                            "color:#6b7280;font-weight:600"
                        )
                        ui.label(f"Última sincronización: {synced_str}").style(
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
                    ganancia_total = d["ganancia"]["total"]
                    pub_gan_pct = (total_cost / ganancia_total * 100) if ganancia_total > 0 else None
                    with kpi_row:
                        _kpi_tile("Inversión", fmt_m(total_cost), "", "#dc2626", KPI_TOOLTIPS["inversion"])
                        _kpi_tile("Ventas x ads", fmt_m(total_amount), "", "#16a34a", KPI_TOOLTIPS["ventas"])
                        _kpi_tile("ACOS", f"{acos:.1f}%".replace(".", ","), "", "#f59e0b", KPI_TOOLTIPS["acos"])
                        _kpi_tile("ROAS", f"{roas:.2f}x".replace(".", ","), "", "#1d4ed8", KPI_TOOLTIPS["roas"])
                        _kpi_tile("TACOS", f"{tacos:.1f}%".replace(".", ","),
                                  f"vs. {fmt_m(ventas_tienda)} facturado", "#7c3aed", KPI_TOOLTIPS["tacos"])
                        _kpi_tile("Unidades", fmt_n(total_units), "", "#374151", KPI_TOOLTIPS["unidades"])
                        _kpi_tile(
                            "Ads / Ganancia",
                            f"{pub_gan_pct:.1f}%".replace(".", ",") if pub_gan_pct is not None else "s/dato",
                            f"vs. {fmt_m(ganancia_total)} ganancia real" if ganancia_total > 0
                                else "Sin ganancia real calculada en el período",
                            "#be185d", KPI_TOOLTIPS["pub_ganancia"],
                        )

                def _toggle_ocultos(key: str) -> None:
                    if key in estado["mostrar_ocultos"]:
                        estado["mostrar_ocultos"].discard(key)
                    else:
                        estado["mostrar_ocultos"].add(key)
                    _render_tabla()

                def _render_nota_ocultos(n_ocultos: int, key: str, n_cols: int = N_COLS) -> None:
                    if n_ocultos <= 0:
                        return
                    mostrando = key in estado["mostrar_ocultos"]
                    with ui.element("tr"):
                        with ui.element("td").props(f"colspan={n_cols}").style(
                            "padding:6px 10px 6px 30px;background:#fafafa"
                        ):
                            with ui.element("div").style(
                                "display:flex;align-items:center;gap:8px;font-size:11px;color:#9ca3af"
                            ):
                                plural = "producto" if n_ocultos == 1 else "productos"
                                if mostrando:
                                    ui.label(f"{n_ocultos} {plural} sin actividad en el período.")
                                    ui.link("Ocultar", "#").style(
                                        "color:#1d4ed8;font-size:11px"
                                    ).on("click.prevent", lambda k=key: _toggle_ocultos(k))
                                else:
                                    ui.label(f"{n_ocultos} {plural} sin actividad en el período — ocultos.")
                                    ui.link("Mostrar", "#").style(
                                        "color:#1d4ed8;font-size:11px"
                                    ).on("click.prevent", lambda k=key: _toggle_ocultos(k))

                def _render_items_filas(items_camp: List[Dict[str, Any]], cid: int) -> None:
                    key = f"camp:{cid}"
                    activos, n_ocultos = _split_actividad(items_camp)
                    if not activos and n_ocultos == 0:
                        with ui.element("tr"):
                            with ui.element("td").props(f"colspan={N_COLS}").style(
                                "padding:6px 10px 6px 30px;background:#fafafa"
                            ):
                                ui.label("Sin ítems con actividad en este período.").style(
                                    "color:#9ca3af;font-size:11px"
                                )
                        return
                    mostrando_ocultos = key in estado["mostrar_ocultos"]
                    a_mostrar = sorted(items_camp if mostrando_ocultos else activos,
                                        key=lambda it: it.get("roas") or 0, reverse=True)
                    sufijos = _dedupe_titulos(a_mostrar)
                    for it in a_mostrar:
                        titulo = it.get("title") or it.get("item_id") or "—"
                        _render_fila_metrica(
                            titulo, it.get("status"), float(it.get("cost") or 0),
                            float(it.get("total_amount") or 0), float(it.get("units_quantity") or 0),
                            sufijo=sufijos.get(it.get("item_id"), ""), stock=stock_por_item.get(it.get("item_id")),
                            indent=True, muted=True, font_size="11px",
                        )
                    if not mostrando_ocultos:
                        _render_nota_ocultos(n_ocultos, key)

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
                        with ui.element("table").style("width:100%;border-collapse:collapse;font-size:12px;table-layout:fixed"):
                            _render_header("Campaña")
                            with ui.element("tbody"):
                                for cid, dim, m in filas:
                                    expandida = estado["campania_expandida"] == cid
                                    nombre = f"{'▾' if expandida else '▸'} {dim.get('name') or cid}"
                                    items_camp = [it for it in d["items"] if it.get("campaign_id") == cid]
                                    stocks_camp = [stock_por_item.get(it.get("item_id")) for it in items_camp]
                                    con_dato = sum(1 for s in stocks_camp if s is not None)
                                    con_stock = sum(1 for s in stocks_camp if (s or 0) > 0)
                                    _render_fila_metrica(
                                        nombre, dim.get("status"), m["cost"], m["total_amount"],
                                        m["units_quantity"], stock_resumen=(con_stock, con_dato),
                                        on_click=lambda c=cid: _toggle_expandir(c),
                                    )
                                    if expandida:
                                        _render_items_filas(items_camp, cid)

                def _render_tabla_productos(d: Dict[str, Any]) -> None:
                    items = list(d["items"])
                    if not items:
                        ui.label("Sin actividad de productos en este período.").style(
                            "color:#9ca3af;padding:16px"
                        )
                        return
                    activos, n_ocultos = _split_actividad(items)
                    key = "producto"
                    mostrando_ocultos = key in estado["mostrar_ocultos"]
                    base = items if mostrando_ocultos else activos
                    base = sorted(base, key=lambda it: it.get("roas") or 0, reverse=True)
                    sufijos = _dedupe_titulos(base)
                    CAP = 50
                    capped = base[:CAP]
                    por_item_gan = d["ganancia"]["por_item"]
                    with ui.element("div").style(
                        "background:#fff;border:1px solid #e0e2e7;border-radius:10px;overflow:hidden"
                    ):
                        with ui.element("table").style("width:100%;border-collapse:collapse;font-size:12px;table-layout:fixed"):
                            _render_header("Producto", widths=COL_WIDTHS_PRODUCTO, con_ganancia=True)
                            with ui.element("tbody"):
                                if not capped:
                                    with ui.element("tr"):
                                        with ui.element("td").props(f"colspan={N_COLS_PRODUCTO}").style("padding:12px"):
                                            ui.label("Sin actividad de productos en este período.").style(
                                                "color:#9ca3af;font-size:11px"
                                            )
                                for it in capped:
                                    titulo = it.get("title") or it.get("item_id") or "—"
                                    cost_i = float(it.get("cost") or 0)
                                    gan_i = por_item_gan.get(it.get("item_id"))
                                    neta_i = (gan_i - cost_i) if gan_i is not None else None
                                    _render_fila_metrica(
                                        titulo, it.get("status"), cost_i,
                                        float(it.get("total_amount") or 0), float(it.get("units_quantity") or 0),
                                        sufijo=sufijos.get(it.get("item_id"), ""),
                                        stock=stock_por_item.get(it.get("item_id")),
                                        ganancia=gan_i, ganancia_neta=neta_i, mostrar_ganancia=True,
                                        widths=COL_WIDTHS_PRODUCTO,
                                    )
                                if not mostrando_ocultos:
                                    _render_nota_ocultos(n_ocultos, key, n_cols=N_COLS_PRODUCTO)
                                if len(base) > CAP:
                                    with ui.element("tr"):
                                        with ui.element("td").props(f"colspan={N_COLS_PRODUCTO}").style("padding:6px 10px"):
                                            ui.label(
                                                f"Mostrando los primeros {CAP} de {len(base)} productos "
                                                "(ordenados por ROAS)."
                                            ).style("font-size:11px;color:#9ca3af")

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
                    estado["mostrar_ocultos"] = set()
                    _render_periodo_buttons()
                    _render_kpis()
                    _render_tabla()

                def _set_vista(k: str) -> None:
                    estado["vista"] = k
                    estado["campania_expandida"] = None
                    estado["mostrar_ocultos"] = set()
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
