"""
Fase 3 — tabs/cuotas.py
Pestaña Cuotas: lista deduplicada por SKU con info de cuotas de cada publicación.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import asyncio

import re

import requests
from nicegui import app, background_tasks, context, run, ui

from db import get_connection, get_cotizador_param, get_financiacion_cuotas_ml
from ml_api import (
    get_ml_access_token,
    _cuotas_desde_item,
    ml_get_my_items,
    ml_get_user_profile,
    ml_get_item_sale_price_full,
    ml_get_promotion_item_discounts,
    ml_get_promotion_item_discounts_by_user,
    ml_get_promotion_item_discounts_by_campaign,
    ml_update_item_price,
)


# ---------------------------------------------------------------------------
# Helper de sesión (mismo patrón que otros tabs; se unificará en auth.py Fase 4)
# ---------------------------------------------------------------------------

def _require_login() -> Optional[Dict[str, Any]]:
    user = app.storage.user.get("user")
    if not user:
        ui.notify("Debes iniciar sesión para continuar", color="negative")
    return user


# ---------------------------------------------------------------------------
# Helpers de agrupación/clasificación (exportados para tabs/precios.py)
# ---------------------------------------------------------------------------

def _cuotas_key(it: dict) -> tuple:
    sku = (it.get("seller_sku") or "").strip()
    if sku:
        return ("sku", sku.lower())
    cpid = (it.get("catalog_product_id") or "").strip()
    if cpid:
        return ("catalog", cpid)
    return ("id", str(it.get("id") or ""))


def _cuotas_score(it: dict) -> tuple:
    status_score = {"active": 2, "paused": 1, "closed": 0}.get(
        str(it.get("status") or "").lower(), 0)
    cuotas_score = 1 if str(it.get("listing_type_id") or "").lower() == "gold_pro" else 0
    stock_score = int(it.get("available_quantity") or 0)
    return (status_score, cuotas_score, stock_score)


def _get_promo_data(access_token: str, item_id: str, seller_id: str = "") -> dict:
    """Obtiene precio promo, % ML y % vendedor. Misma cascada de 3 intentos que el popup de precios."""
    empty: dict = {"price_promo": None, "meli_pct": None, "seller_pct": None}
    sp = ml_get_item_sale_price_full(access_token, item_id)
    if not sp or sp.get("amount") is None:
        return empty
    amt_f = float(sp["amount"])
    reg = sp.get("regular_amount")
    if reg is None or float(reg) <= 0 or abs(float(reg) - amt_f) <= 0.01:
        return empty
    reg_f = float(reg)
    total_pct = (reg_f - amt_f) / reg_f * 100
    cid = sp.get("campaign_id")
    pid = sp.get("promotion_id")
    pt  = (sp.get("promotion_type") or "").strip().upper()
    d = None
    if cid:
        d = ml_get_promotion_item_discounts_by_campaign(
            access_token, str(cid), item_id, total_pct,
            seller_id, promotion_type_hint=pt
        )
    if d is None and pid and pt and not str(pid).upper().startswith("OFFER-"):
        d = ml_get_promotion_item_discounts(
            access_token, str(pid), pt, item_id, total_pct
        )
    if d is None and seller_id:
        d = ml_get_promotion_item_discounts_by_user(
            access_token, item_id, seller_id, total_pct
        )
    if d:
        return {"price_promo": amt_f, "regular_amount": reg_f, "meli_pct": d.get("meli_pct"), "seller_pct": d.get("seller_pct")}
    return {"price_promo": amt_f, "regular_amount": reg_f, "meli_pct": None, "seller_pct": None}


_REACOND_PATTERNS = re.compile(r'cajaabierta|c\.abierta|cabierta|reacondicionado|recond', re.IGNORECASE)


def _is_reacondicionado(row: dict) -> bool:
    sku = row.get("seller_sku") or ""
    if _REACOND_PATTERNS.search(sku):
        return True
    cond = (row.get("condition") or "").lower()
    return cond in ("used", "not_specified")


# ---------------------------------------------------------------------------
# Tabla de cuotas
# ---------------------------------------------------------------------------

def _mostrar_tabla_cuotas(result_area, data: Dict[str, Any], access_token: str, promo_data: Optional[Dict[str, Dict]] = None, container=None, user_id: Optional[int] = None) -> None:
    """Pinta la tabla de cuotas con columnas agrupadas por tipo de publicación."""
    items = data.get("results", [])
    result_area.clear()

    if not items:
        with result_area:
            ui.label("No se encontraron publicaciones activas.").classes("text-gray-600")
        return

    uid = user_id or 1
    cuotas_3x  = float(get_cotizador_param("cuotas_3x",  uid) or 0.084)
    cuotas_6x  = float(get_cotizador_param("cuotas_6x",  uid) or 0.123)
    cuotas_9x  = float(get_cotizador_param("cuotas_9x",  uid) or 0.157)
    cuotas_12x = float(get_cotizador_param("cuotas_12x", uid) or 0.192)

    def _pr(s: Any, d: float = 0.0) -> float:
        if s is None or str(s).strip() == "":
            return d
        try:
            v = float(str(s).strip().replace(",", "."))
            return v if v <= 1.5 else v / 100.0
        except (ValueError, TypeError):
            return d

    ml_comision         = _pr(get_cotizador_param("ml_comision",          uid), 0.15)
    ml_debcre           = _pr(get_cotizador_param("ml_debcre",            uid), 0.006)
    ml_iibb_per         = _pr(get_cotizador_param("ml_iibb_per",          uid), 0.055)
    ml_envios_gratuitos = float(str(get_cotizador_param("ml_envios_gratuitos", uid) or 33000).replace(",", "."))
    ml_envios_val       = float(str(get_cotizador_param("ml_envios",      uid) or 5823).replace(",", "."))
    if ml_envios_val <= 100:
        ml_envios_val = 5823.0
    dolar_oficial       = float(str(get_cotizador_param("dolar_oficial",  uid) or "1475").replace(",", ".")) or 1475.0
    if dolar_oficial <= 0:
        dolar_oficial = 1475.0

    # ── Agrupación ────────────────────────────────────────────────────────────
    groups: Dict[tuple, List[Dict[str, Any]]] = {}
    for it in items:
        groups.setdefault(_cuotas_key(it), []).append(it)

    def _slot(it: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not it:
            return {"id": "", "permalink": "", "price": None}
        return {
            "id": str(it.get("id") or ""),
            "permalink": it.get("permalink") or "",
            "price": it.get("price"),
        }

    def _build_row(group: List[Dict[str, Any]]) -> Dict[str, Any]:
        propia = catalogo = x3 = x6 = x9 = x12 = None
        for it in group:
            cuotas = _cuotas_desde_item(it)
            is_cat = it.get("catalog_listing") is True
            lt = str(it.get("listing_type_id") or "").lower()
            if lt == "gold_special":
                if propia is None or _cuotas_score(it) > _cuotas_score(propia):
                    propia = it
            if is_cat and lt != "gold_special":
                if catalogo is None or _cuotas_score(it) > _cuotas_score(catalogo):
                    catalogo = it
            if cuotas == "x3":
                if x3 is None or _cuotas_score(it) > _cuotas_score(x3):
                    x3 = it
            if cuotas == "x6":
                if x6 is None or _cuotas_score(it) > _cuotas_score(x6):
                    x6 = it
            if cuotas == "x9":
                if x9 is None or _cuotas_score(it) > _cuotas_score(x9):
                    x9 = it
            if cuotas == "x12":
                if x12 is None or _cuotas_score(it) > _cuotas_score(x12):
                    x12 = it
        rep = max(group, key=_cuotas_score)
        # SKU: usa el del representante; si vacío, busca en todos los items del grupo
        sku = (rep.get("seller_sku") or "").strip()
        if not sku:
            for it in group:
                candidate = (it.get("seller_sku") or "").strip()
                if not candidate:
                    candidate = (it.get("seller_custom_field") or "").strip()
                if not candidate:
                    for att in (it.get("attributes") or []):
                        if (att.get("id") or "").strip().upper() == "SELLER_SKU":
                            v = att.get("value_name") or att.get("value") or att.get("value_id")
                            if v:
                                candidate = str(v).strip()
                                break
                if candidate:
                    sku = candidate
                    break
        # Stock: prefiere propia, fallback catálogo
        stock_val = None
        if propia is not None:
            stock_val = propia.get("available_quantity")
        elif catalogo is not None:
            stock_val = catalogo.get("available_quantity")
        best = propia or catalogo or rep
        return {
            "marca":         rep.get("marca") or "—",
            "seller_sku":    sku,
            "title":         best.get("title") or "",
            "thumbnail":     best.get("thumbnail") or "",
            "stock":         stock_val,
            "promo_item_id": str(rep.get("id") or ""),
            "propia":        _slot(propia),
            "catalogo":      _slot(catalogo),
            "x3":            _slot(x3),
            "x6":            _slot(x6),
            "x9":            _slot(x9),
            "x12":           _slot(x12),
            "condition":     (rep.get("condition") or "").lower(),
            }

    rows_all = [_build_row(g) for g in groups.values()]
    rows_all.sort(key=lambda r: r.get("title", "").lower())
    _pd = promo_data or {}
    for _row in rows_all:
        _row["promo"] = _pd.get(_row.get("promo_item_id", ""), {"price_promo": None, "meli_pct": None, "seller_pct": None})

    filtrados_ref: Dict[str, list]  = {"val": list(rows_all)}
    sort_col_ref:  Dict[str, str]   = {"val": "title"}
    sort_asc_ref:  Dict[str, bool]  = {"val": True}

    def fmt_moneda(val: Any) -> str:
        if val is None:
            return "—"
        try:
            return "$" + f"{int(float(val)):,}".replace(",", ".")
        except (TypeError, ValueError):
            return "—"

    # (gkey, grupo_label, bg_even, bg_odd, border_color)
    GROUPS = [
        ("propia",     "Publicación Propia",  "#F0F7FF", "#DCEEFB", "#C5DCFA"),
        ("catalogo",   "Catálogo",            "#F1F8F1", "#D6EDD6", "#B8D9B8"),
        ("x3",         "3 Cuotas",            "#FFFAF0", "#FFF0CC", "#FFE0B2"),
        ("x6",         "6 Cuotas",            "#FAF5FC", "#EDD9F5", "#E1BEE7"),
        ("x9",         "9 Cuotas",            "#FFF5F5", "#FFE0E0", "#FFCCCC"),
        ("x12",        "12 Cuotas",           "#F5F5FF", "#E0E0FF", "#CCCCFF"),
    ]
    PROMO_BG_EVEN = "#FFF8E1"
    PROMO_BG_ODD  = "#FFECB3"
    PROMO_BORDER  = "#FFD54F"

    SORT_KEY: Dict[str, Any] = {
        "marca":          lambda r: (r.get("marca") or "").lower(),
        "seller_sku":     lambda r: (r.get("seller_sku") or "").lower(),
        "title":          lambda r: r.get("title", "").lower(),
        "stock":          lambda r: int(r.get("stock") or 0),
        "propia_price":   lambda r: r["propia"]["price"]   if r["propia"]["price"]   is not None else -1,
        "catalogo_price":  lambda r: r["catalogo"]["price"] if r["catalogo"]["price"] is not None else -1,
        "x3_price":        lambda r: r["x3"]["price"]       if r["x3"]["price"]       is not None else -1,
        "x6_price":       lambda r: r["x6"]["price"]       if r["x6"]["price"]       is not None else -1,
        "x9_price":       lambda r: r["x9"]["price"]       if r["x9"]["price"]       is not None else -1,
        "x12_price":      lambda r: r["x12"]["price"]      if r["x12"]["price"]      is not None else -1,
    }

    TH_HDR1 = "font-weight:700;font-size:11px;padding:5px 6px;border:1px solid #1565c0;background:#1976d2;color:white;letter-spacing:0.05em;text-transform:uppercase;box-shadow:0 2px 4px rgba(0,0,0,0.15)"
    TH_HDR2 = "font-weight:500;font-size:10px;padding:4px 5px;border:1px solid #1565c0;background:#1565c0;color:rgba(255,255,255,0.85)"
    TD_BASE = "padding:3px 6px;border-bottom:1px solid #e5e7eb;font-size:10px"

    n_propios  = sum(1 for r in rows_all if r["propia"]["id"])
    n_catalogo = sum(1 for r in rows_all if r["catalogo"]["id"])
    n_x3       = sum(1 for r in rows_all if r["x3"]["id"])
    n_x6       = sum(1 for r in rows_all if r["x6"]["id"])
    n_x9       = sum(1 for r in rows_all if r["x9"]["id"])
    n_x12      = sum(1 for r in rows_all if r["x12"]["id"])
    n_promos = sum(1 for r in rows_all if r.get("promo", {}).get("price_promo") is not None)
    _tot = len(rows_all)
    pct_x3  = n_x3  / _tot * 100 if _tot else 0
    pct_x6  = n_x6  / _tot * 100 if _tot else 0
    pct_x9  = n_x9  / _tot * 100 if _tot else 0
    pct_x12 = n_x12 / _tot * 100 if _tot else 0

    fin_cuotas_full = get_financiacion_cuotas_ml()

    def _fmt_fecha(s: str) -> str:
        try:
            parts = str(s)[:10].split("-")
            if len(parts) == 3:
                return f"{parts[2]}/{parts[1]}/{parts[0]}"
        except Exception:
            pass
        return ""

    with result_area:
        with ui.card().classes("w-full mb-2 p-3 bg-grey-2"):
            with ui.row().classes("items-center gap-4 flex-wrap justify-between"):
                with ui.row().classes("items-center gap-4 flex-wrap"):
                    for label, count in [
                        ("Publicaciones únicas",          len(rows_all)),
                        ("Propios",                       n_propios),
                        ("Catálogo",                      n_catalogo),
                        (f"En 3 cuotas ({pct_x3:.0f}%)",  n_x3),
                        (f"En 6 cuotas ({pct_x6:.0f}%)",  n_x6),
                        (f"En 9 cuotas ({pct_x9:.0f}%)",  n_x9),
                        (f"En 12 cuotas ({pct_x12:.0f}%)", n_x12),
                        ("En promoción",                  n_promos),
                    ]:
                        with ui.element("div").style("display:flex;flex-direction:column;align-items:center;min-width:80px"):
                            ui.label(str(count)).classes("text-primary text-xl font-bold leading-tight")
                            ui.label(label).classes("text-xs text-gray-600 text-center")
                    ui.element("div").style("width:1px;height:48px;background:#bdbdbd;align-self:center;margin:0 4px")
                    with ui.element("div").style("display:flex;flex-direction:row;align-items:center"):
                        for _i, (_nc, _mkp) in enumerate([
                            (3, cuotas_3x), (6, cuotas_6x), (9, cuotas_9x), (12, cuotas_12x)
                        ]):
                            if _i > 0:
                                ui.element("div").style("width:1px;height:56px;background:#e0e0e0;align-self:center;margin:0 4px")
                            _fi    = fin_cuotas_full.get(_nc, {})
                            _mlpct = _fi.get("pct", 0.0)
                            _fech  = _fmt_fecha(_fi.get("fecha", ""))
                            with ui.element("div").style("display:flex;flex-direction:column;align-items:center;padding:2px 8px"):
                                ui.label(f"{_mkp*100:.1f}%".replace(".", ",")).style("font-size:18px;font-weight:500;color:#1D9E75;line-height:1.2")
                                with ui.element("div").style("display:flex;align-items:center;gap:3px"):
                                    ui.label("ML").style("font-size:12px;color:#9e9e9e;line-height:1.3")
                                    ui.label(f"{_mlpct*100:.1f}%".replace(".", ",")).style("font-size:12px;font-weight:500;color:#185FA5;line-height:1.3")
                                ui.label(_fech or "—").style("font-size:10px;color:#bdbdbd;line-height:1.3")
                                ui.label(f"{_nc} cuotas").style("font-size:11px;color:#9e9e9e;line-height:1.3")
                if container is not None:
                    ui.element("div").style("width:1px;height:48px;background:#bdbdbd;align-self:center;margin:0 4px")
                    ui.button("Sincronizar", icon="sync", on_click=lambda: build_tab_cuotas(container, force_refresh=True)).props("flat dense")

        with ui.row().classes("items-center gap-3 mb-3"):
            filtro_cuotas_sel = ui.select(
                {"cualquiera": "Cualquiera", "sin_cuotas": "Sin cuotas", "con_cuotas": "Con cuotas"},
                value="cualquiera", label="Cuotas"
            ).classes("w-36").props("outlined dense")
            filtro_check_sel = ui.select(
                {"todos": "Todos", "alto": "Precio alto", "bajo": "Precio bajo"},
                value="todos", label="Check%"
            ).classes("w-36").props("outlined dense")
            filtro_input = ui.input(placeholder="Filtrar por SKU o Nombre...").props("outlined dense clearable").classes("w-72")

        header_div = ui.element("div").style("width:100%;overflow:hidden")
        table_container = ui.element("div").style("width:100%;height:65vh;overflow-y:scroll;overflow-x:auto")
        _hid = header_div.id
        _cid = table_container.id
        async def _setup_sync_once() -> None:
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
        ui.timer(0.1, _setup_sync_once, once=True)

        def _sort_rows(rows: list) -> list:
            key_fn = SORT_KEY.get(sort_col_ref["val"], lambda r: r.get("title", "").lower())
            return sorted(rows, key=key_fn, reverse=not sort_asc_ref["val"])

        def _ind(col: str) -> str:
            if sort_col_ref["val"] != col:
                return ""
            return " ▲" if sort_asc_ref["val"] else " ▼"

        def _build_colgroup() -> None:
            with ui.element("colgroup"):
                ui.element("col").style("width:4%")
                ui.element("col").style("width:14%")
                ui.element("col").style("width:26%")
                ui.element("col").style("width:2%")
                ui.element("col").style("width:3%")
                ui.element("col").style("width:3%")
                for gkey, *_ in GROUPS:
                    if gkey == "propia":
                        ui.element("col").style("width:7%")
                        ui.element("col").style("width:4%")
                    elif gkey == "catalogo":
                        ui.element("col").style("width:4%")
                        ui.element("col").style("width:3%")
                    else:
                        ui.element("col").style("width:4%")
                        ui.element("col").style("width:3%")
                        ui.element("col").style("width:2%")

        def _render(rows: list) -> None:
            header_div.clear()
            table_container.clear()
            with header_div:
                with ui.element("table").style("table-layout:fixed;width:100%;border-collapse:separate;border-spacing:0"):
                    _build_colgroup()
                    with ui.element("thead"):
                        with ui.element("tr"):
                            with ui.element("th").props('rowspan="2"').style(f"{TH_HDR1};width:4%;text-align:center;cursor:pointer").on("click", lambda: _on_sort("marca")):
                                ui.label("Marca" + _ind("marca"))
                            with ui.element("th").props('rowspan="2"').style(f"{TH_HDR1};width:14%;text-align:center;cursor:pointer").on("click", lambda: _on_sort("seller_sku")):
                                ui.label("SKU" + _ind("seller_sku"))
                            with ui.element("th").props('rowspan="2"').style(f"{TH_HDR1};width:26%;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;text-align:center;cursor:pointer").on("click", lambda: _on_sort("title")):
                                ui.label("Nombre" + _ind("title"))
                            with ui.element("th").props('rowspan="2"').style(f"{TH_HDR1};width:2%;text-align:center"):
                                ui.label("Fix")
                            with ui.element("th").props('rowspan="2"').style(f"{TH_HDR1};width:3%;text-align:center;cursor:pointer").on("click", lambda: _on_sort("stock")):
                                ui.label("Stock" + _ind("stock"))
                            with ui.element("th").props('rowspan="2"').style(f"{TH_HDR1};width:3%;text-align:center"):
                                ui.label("Tipo")
                            for gkey, glabel, gbg_e, gbg_o, gborder in GROUPS:
                                if gkey in ("propia", "catalogo"):
                                    _cspan = "2"
                                else:
                                    _cspan = "3"
                                with ui.element("th").props(f'colspan="{_cspan}"').style(f"{TH_HDR1};border-left:2px solid {gborder};text-align:center"):
                                    ui.label(glabel)
                        with ui.element("tr"):
                            for gkey, glabel, gbg_e, gbg_o, gborder in GROUPS:
                                pcol = f"{gkey}_price"
                                if gkey == "propia":
                                    with ui.element("th").style(f"{TH_HDR2};width:7%;border-left:2px solid {gborder};text-align:center"):
                                        ui.label("Publicación")
                                    with ui.element("th").style(f"{TH_HDR2};width:4%;text-align:center;cursor:pointer").on("click", lambda pk=pcol: _on_sort(pk)):
                                        ui.label("Precio" + _ind(pcol))
                                else:
                                    with ui.element("th").style(f"{TH_HDR2};width:4%;border-left:2px solid {gborder};text-align:center;cursor:pointer").on("click", lambda pk=pcol: _on_sort(pk)):
                                        ui.label("Precio" + _ind(pcol))
                                    with ui.element("th").style(f"{TH_HDR2};width:3%;text-align:center"):
                                        ui.label("% vs Propia")
                                    if gkey != "catalogo":
                                        with ui.element("th").style(f"{TH_HDR2};width:2%;text-align:center"):
                                            ui.label("Check %")
            with table_container:
                if not rows:
                    ui.label("Sin resultados para el filtro aplicado.").classes("text-gray-500 mt-2")
                    return
                with ui.element("table").style("table-layout:fixed;width:100%;border-collapse:separate;border-spacing:0"):
                    _build_colgroup()
                    # ── Cuerpo ────────────────────────────────────────────────
                    with ui.element("tbody"):
                        for idx, row in enumerate(rows):
                            base_bg = "background:#ffffff" if idx % 2 == 0 else "background:#fafafa"
                            with ui.element("tr").style(base_bg).classes("hover:bg-blue-50"):
                                with ui.element("td").style(f"{TD_BASE};overflow:hidden;text-overflow:ellipsis;white-space:nowrap"):
                                    ui.label(row.get("marca") or "—")
                                with ui.element("td").style(f"{TD_BASE};overflow:hidden;text-overflow:ellipsis;white-space:nowrap"):
                                    ui.label(row.get("seller_sku") or "—")
                                with ui.element("td").style(f"{TD_BASE};overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:0"):
                                    title_text = row.get("title") or "—"
                                    def _abrir_detalle(r=row) -> None:
                                        dlg = ui.dialog()
                                        with dlg:
                                            with ui.card().classes("p-4 min-w-[500px] max-w-[700px]"):
                                                with ui.row().classes("items-start gap-3 mb-3"):
                                                    if r.get("thumbnail"):
                                                        ui.image(r["thumbnail"]).classes("w-16 h-16 object-contain rounded border")
                                                    with ui.column().classes("gap-1"):
                                                        ui.label(r.get("marca") or "—").classes("text-xs text-gray-500 uppercase tracking-wide")
                                                        ui.label(r.get("seller_sku") or "—").classes("text-sm font-mono text-gray-600")
                                                        ui.label(r.get("title") or "—").classes("text-base font-semibold")
                                                        sv = r.get("stock")
                                                        ui.label(f"Stock: {sv if sv is not None else '—'}").classes("text-sm text-gray-500")
                                                ui.separator().classes("my-2")
                                                ui.label("Publicaciones").classes("text-xs font-bold text-gray-500 uppercase mb-1")
                                                _pp = r["propia"]["price"]
                                                if _pp is None:
                                                    _pp = r["catalogo"]["price"]
                                                with ui.element("table").style("width:100%;border-collapse:collapse;font-size:11px"):
                                                    with ui.element("thead"):
                                                        with ui.element("tr"):
                                                            for _h in ["Tipo", "ID", "Precio", "% vs Propia", "Recomendada"]:
                                                                with ui.element("th").style("padding:4px 8px;background:#1976d2;color:white;text-align:left;font-weight:600"):
                                                                    ui.label(_h)
                                                    with ui.element("tbody"):
                                                        for _tipo, _sk in [("Propia","propia"),("Catálogo","catalogo"),("3 Cuotas","x3"),("6 Cuotas","x6"),("9 Cuotas","x9"),("12 Cuotas","x12")]:
                                                            _s = r[_sk]; _sid = _s["id"]; _sp = _s["price"]; _slink = _s["permalink"]
                                                            with ui.element("tr").style("border-bottom:1px solid #e5e7eb"):
                                                                with ui.element("td").style("padding:3px 8px;font-weight:500"):
                                                                    ui.label(_tipo)
                                                                with ui.element("td").style("padding:3px 8px;font-family:monospace;font-size:10px"):
                                                                    if _sk == "propia":
                                                                        ui.label(_sid if _sid else "—").classes("" if _sid else "text-gray-400")
                                                                    elif _sid and _slink:
                                                                        ui.link(_sid, _slink, new_tab=True).classes("text-blue-700 hover:underline")
                                                                    elif _sid:
                                                                        ui.label(_sid)
                                                                    elif _sk in ("x3", "x6", "x9", "x12"):
                                                                        ui.label("Crear desde panel ML").classes("text-gray-400 text-xs italic")
                                                                    else:
                                                                        ui.label("—").classes("text-gray-400")
                                                                with ui.element("td").style("padding:3px 8px;font-weight:600;text-align:right"):
                                                                    if _sk not in ("catalogo", "propia") and _sid and _sp is not None:
                                                                        def _edit_price(s2=_s, iid=_sid, tipo=_tipo, d_main=dlg, rr_main=r, af=_abrir_detalle) -> None:
                                                                            subdlg = ui.dialog()
                                                                            with subdlg:
                                                                                with ui.card().classes("p-4 min-w-[300px]"):
                                                                                    ui.label(f"Editar precio — {tipo}").classes("text-base font-semibold mb-2")
                                                                                    try:
                                                                                        _pa = float(s2["price"] or 0)
                                                                                    except (TypeError, ValueError):
                                                                                        _pa = 0.0
                                                                                    _inp = ui.input("Nuevo precio ($)", value=str(int(_pa))).classes("w-full")
                                                                                    _inp.props("type=number min=1 step=1")
                                                                                    def _guardar(sd=subdlg, i=_inp, s3=s2, item=iid, dm=d_main, rr=rr_main, afn=af) -> None:
                                                                                        try:
                                                                                            nuevo = float(i.value or 0)
                                                                                        except (TypeError, ValueError):
                                                                                            ui.notify("Precio inválido.", color="negative")
                                                                                            return
                                                                                        if nuevo < 1:
                                                                                            ui.notify("El precio debe ser al menos 1.", color="negative")
                                                                                            return
                                                                                        sd.close()
                                                                                        dm.close()
                                                                                        ui.notify("Actualizando precio...", color="info")
                                                                                        cl = context.client
                                                                                        async def _act(client_=cl, s4=s3, precio=nuevo, item2=item, rr2=rr, af2=afn) -> None:
                                                                                            try:
                                                                                                await run.io_bound(ml_update_item_price, access_token, item2, precio)
                                                                                                s4["price"] = precio
                                                                                                with client_:
                                                                                                    _render(_sort_rows(filtrados_ref["val"]))
                                                                                                    ui.notify("Precio actualizado.", color="positive")
                                                                                                    af2(rr2)
                                                                                            except Exception as err:
                                                                                                with client_:
                                                                                                    ui.notify(f"Error: {err}", color="negative")
                                                                                        background_tasks.create(_act())
                                                                                    with ui.row().classes("w-full justify-end gap-2 mt-3"):
                                                                                        ui.button("Cancelar", on_click=lambda sd=subdlg: sd.close()).props("flat")
                                                                                        ui.button("Guardar", on_click=_guardar, color="primary")
                                                                            subdlg.open()
                                                                        ui.button(fmt_moneda(_sp), on_click=_edit_price).props("flat dense").style(
                                                                            "font-weight:600;font-size:11px;padding:0 4px;min-height:0;color:inherit"
                                                                        )
                                                                    else:
                                                                        ui.label(fmt_moneda(_sp)).classes("" if _sp is not None else "text-gray-400")
                                                                with ui.element("td").style("padding:3px 8px;text-align:center"):
                                                                    if _sk != "propia" and _sp is not None and _pp is not None and float(_pp) != 0:
                                                                        _pct = (float(_sp) - float(_pp)) / float(_pp) * 100
                                                                        if abs(_pct) < 0.05:
                                                                            ui.label("=").style("color:#757575")
                                                                        elif _pct > 0:
                                                                            ui.label(f"+{_pct:.1f}%").style("color:#43a047;font-weight:500")
                                                                        else:
                                                                            ui.label(f"{_pct:.1f}%").style("color:#e53935;font-weight:500")
                                                                    else:
                                                                        ui.label("—").classes("text-gray-400")
                                                                with ui.element("td").style("padding:3px 8px;text-align:right"):
                                                                    _tasa_map = {"x3": cuotas_3x, "x6": cuotas_6x, "x9": cuotas_9x, "x12": cuotas_12x}
                                                                    if _sk in _tasa_map and _pp is not None:
                                                                        try:
                                                                            _rec = round(float(_pp) * (1 + _tasa_map[_sk]))
                                                                            ui.label(fmt_moneda(_rec)).style("font-weight:500")
                                                                        except (TypeError, ValueError):
                                                                            ui.label("—").classes("text-gray-400")
                                                                    else:
                                                                        ui.label("—").classes("text-gray-400")
                                                promo_d = r.get("promo", {})
                                                if promo_d.get("price_promo") is not None:
                                                    ui.separator().classes("my-2")
                                                    ui.label("Promoción").classes("text-xs font-bold text-gray-500 uppercase mb-1")
                                                    with ui.row().classes("gap-4 text-sm"):
                                                        ui.label(f"Precio promo: {fmt_moneda(promo_d['price_promo'])}").classes("font-semibold")
                                                        if promo_d.get("meli_pct") is not None:
                                                            ui.label(f"% ML: {promo_d['meli_pct']:.1f}%").style("color:#43a047")
                                                        if promo_d.get("seller_pct") is not None:
                                                            ui.label(f"% Vendedor: {promo_d['seller_pct']:.1f}%").style("color:#e65100")
                                                ui.separator().classes("my-2")
                                                with ui.row().classes("w-full justify-end gap-2"):
                                                    def _corregir_click(rr_c=r, d_c=dlg, af_c=_abrir_detalle) -> None:
                                                        cl = context.client
                                                        async def _do_corregir(client_=cl, rr2=rr_c, d2=d_c, af2=af_c) -> None:
                                                            propia_p = rr2["propia"]["price"]
                                                            if propia_p is None:
                                                                propia_p = rr2["catalogo"]["price"]
                                                            if propia_p is None:
                                                                with client_:
                                                                    ui.notify("No hay precio de referencia (propia ni catálogo).", color="warning")
                                                                return
                                                            try:
                                                                propia_p = float(propia_p)
                                                            except (TypeError, ValueError):
                                                                with client_:
                                                                    ui.notify("Precio propia inválido.", color="warning")
                                                                return
                                                            if propia_p == 0:
                                                                with client_:
                                                                    ui.notify("Precio propia es 0.", color="warning")
                                                                return
                                                            tasas = [("x3", cuotas_3x), ("x6", cuotas_6x), ("x9", cuotas_9x), ("x12", cuotas_12x)]
                                                            to_correct = []
                                                            for gkey, tasa in tasas:
                                                                slot = rr2[gkey]
                                                                iid = slot.get("id")
                                                                cp = slot.get("price")
                                                                if not iid or cp is None:
                                                                    continue
                                                                try:
                                                                    cp = float(cp)
                                                                except (TypeError, ValueError):
                                                                    continue
                                                                pc = round(propia_p * (1 + tasa))
                                                                diff = (cp - propia_p) / propia_p * 100 - tasa * 100
                                                                if abs(diff) <= 0.5:
                                                                    continue
                                                                to_correct.append((gkey, iid, cp, pc))
                                                            if not to_correct:
                                                                with client_:
                                                                    ui.notify("Todas las variantes ya están correctas.", color="info")
                                                                return
                                                            promo_price_raw = rr2.get("promo", {}).get("price_promo")
                                                            if promo_price_raw is not None:
                                                                try:
                                                                    promo_f = float(promo_price_raw)
                                                                except (TypeError, ValueError):
                                                                    promo_f = None
                                                                if promo_f:
                                                                    conflictos = [x for x in to_correct if x[3] < promo_f]
                                                                    if conflictos:
                                                                        confirmed_ev = asyncio.Event()
                                                                        cancelled_ref = [False]
                                                                        with client_:
                                                                            conf_dlg = ui.dialog()
                                                                            with conf_dlg:
                                                                                with ui.card().classes("p-4 min-w-[320px]"):
                                                                                    ui.label("Advertencia: promo activa").classes("text-base font-semibold mb-2")
                                                                                    ui.label(
                                                                                        f"{len(conflictos)} variante(s) quedarían con precio "
                                                                                        f"menor al precio promo activo ({fmt_moneda(promo_f)}). "
                                                                                        "Esto podría cancelar la promoción en ML."
                                                                                    ).classes("text-sm text-gray-700 mb-3")
                                                                                    with ui.row().classes("w-full justify-end gap-2"):
                                                                                        def _conf_cancel(ev=confirmed_ev, cr=cancelled_ref, cd=conf_dlg) -> None:
                                                                                            cr[0] = True; cd.close(); ev.set()
                                                                                        def _conf_proceed(ev=confirmed_ev, cd=conf_dlg) -> None:
                                                                                            cd.close(); ev.set()
                                                                                        ui.button("Cancelar", on_click=_conf_cancel).props("flat")
                                                                                        ui.button("Corregir igual", on_click=_conf_proceed).style(
                                                                                            "background:#3B6D11;color:white;font-weight:600"
                                                                                        ).props("no-caps")
                                                                            conf_dlg.open()
                                                                        await confirmed_ev.wait()
                                                                        if cancelled_ref[0]:
                                                                            return
                                                            for gkey, iid, old_p, new_p in to_correct:
                                                                try:
                                                                    await run.io_bound(ml_update_item_price, access_token, iid, new_p)
                                                                    rr2[gkey]["price"] = new_p
                                                                    with client_:
                                                                        ui.notify(
                                                                            f"Corregido {iid} de {fmt_moneda(old_p)} a {fmt_moneda(new_p)}",
                                                                            color="positive", timeout=4000,
                                                                        )
                                                                except Exception as err:
                                                                    with client_:
                                                                        ui.notify(f"Error al corregir {iid}: {err}", color="negative")
                                                            with client_:
                                                                _render(_sort_rows(filtrados_ref["val"]))
                                                                d2.close()
                                                        background_tasks.create(_do_corregir())
                                                    ui.button("Corregir", on_click=_corregir_click).style(
                                                        "background:#3B6D11;color:white;font-weight:600;border-radius:4px;padding:4px 12px"
                                                    ).props("no-caps")
                                                    ui.button("Cerrar", on_click=dlg.close).style(
                                                        "background:#185FA5;color:white;font-weight:600;border-radius:4px;padding:4px 12px"
                                                    ).props("no-caps")
                                        dlg.open()
                                    ui.button(title_text, on_click=_abrir_detalle).props("flat dense align=left").style(
                                        "font-size:10px;padding:0 2px;min-height:0;text-transform:none;color:inherit;"
                                        "font-weight:normal;width:100%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;text-align:left"
                                    )
                                with ui.element("td").style(f"{TD_BASE};text-align:center;padding:2px"):
                                    _propia_p_fix = row["propia"]["price"]
                                    if _propia_p_fix is None:
                                        _propia_p_fix = row["catalogo"]["price"]
                                    _needs_fix = False
                                    if _propia_p_fix is not None:
                                        try:
                                            _propia_f = float(_propia_p_fix)
                                        except (TypeError, ValueError):
                                            _propia_f = 0.0
                                        if _propia_f != 0:
                                            for _gk_f, _tasa_f in [("x3", cuotas_3x), ("x6", cuotas_6x), ("x9", cuotas_9x), ("x12", cuotas_12x)]:
                                                _cp_f = row[_gk_f]["price"]
                                                if _cp_f is not None:
                                                    try:
                                                        _diff_f = (float(_cp_f) - _propia_f) / _propia_f * 100 - _tasa_f * 100
                                                        if abs(_diff_f) > 0.5:
                                                            _needs_fix = True
                                                            break
                                                    except (TypeError, ValueError):
                                                        pass
                                    if _needs_fix:
                                        def _fix_click(r_fix=row) -> None:
                                            cl = context.client
                                            async def _do_fix(client_=cl, rr=r_fix) -> None:
                                                _pp = rr["propia"]["price"]
                                                if _pp is None:
                                                    _pp = rr["catalogo"]["price"]
                                                if _pp is None or float(_pp) == 0:
                                                    with client_:
                                                        ui.notify("No hay precio de referencia (propia ni catálogo).", color="warning")
                                                    return
                                                _pp = float(_pp)
                                                _to_fix = []
                                                for _gk, _tasa in [("x3", cuotas_3x), ("x6", cuotas_6x), ("x9", cuotas_9x), ("x12", cuotas_12x)]:
                                                    _sl = rr[_gk]
                                                    _iid = _sl.get("id")
                                                    _cp = _sl.get("price")
                                                    if not _iid or _cp is None:
                                                        continue
                                                    try:
                                                        _cp = float(_cp)
                                                    except (TypeError, ValueError):
                                                        continue
                                                    _pc = round(_pp * (1 + _tasa))
                                                    if abs((_cp - _pp) / _pp * 100 - _tasa * 100) <= 0.5:
                                                        continue
                                                    _to_fix.append((_gk, _iid, _cp, _pc))
                                                if not _to_fix:
                                                    with client_:
                                                        ui.notify("Todas las variantes ya están correctas.", color="info")
                                                    return
                                                for _gk, _iid, _old, _new in _to_fix:
                                                    try:
                                                        await run.io_bound(ml_update_item_price, access_token, _iid, _new)
                                                        rr[_gk]["price"] = _new
                                                        with client_:
                                                            ui.notify(f"Corregido {_iid}: {fmt_moneda(_old)} → {fmt_moneda(_new)}", color="positive", timeout=4000)
                                                    except Exception as _err:
                                                        with client_:
                                                            ui.notify(f"Error al corregir {_iid}: {_err}", color="negative")
                                                with client_:
                                                    _render(_sort_rows(filtrados_ref["val"]))
                                            background_tasks.create(_do_fix())
                                        with ui.element("div").style("display:inline-flex;align-items:center;cursor:pointer").on("click", _fix_click):
                                            ui.html('<i class="ti ti-tool" style="color:#BA7517;font-size:16px"></i>')
                                    else:
                                        ui.label("—").classes("text-gray-400 text-xs")
                                with ui.element("td").style(f"{TD_BASE};text-align:center"):
                                    sv = row.get("stock")
                                    ui.label(str(sv) if sv is not None else "")
                                with ui.element("td").style(f"{TD_BASE};text-align:center"):
                                    if _is_reacondicionado(row):
                                        ui.html('<i class="ti ti-package" style="color:#e65100;font-size:14px" title="Caja abierta / Reacondicionado"></i>')
                                    else:
                                        ui.label("—").classes("text-gray-400 text-xs")
                                propia_price = row["propia"]["price"]
                                if propia_price is None:
                                    propia_price = row["catalogo"]["price"]
                                for gkey, glabel, gbg_e, gbg_o, gborder in GROUPS:
                                    gbg = gbg_e if idx % 2 == 0 else gbg_o
                                    _gb = f"background:{gbg};border-bottom:1px solid #e5e7eb;font-size:10px"
                                    slot = row[gkey]
                                    item_id   = slot["id"]
                                    permalink = slot["permalink"]
                                    price     = slot["price"]
                                    # Publicación — solo Propia
                                    if gkey == "propia":
                                        with ui.element("td").style(f"{_gb};border-left:2px solid {gborder};padding:3px 6px;font-size:9px;font-family:monospace;text-align:center;overflow:hidden;text-overflow:ellipsis;white-space:nowrap"):
                                            ui.label(item_id if item_id else "")
                                    # Precio — todos (border-left en no-propia como separador de grupo)
                                    _brd = "" if gkey == "propia" else f"border-left:2px solid {gborder};"
                                    with ui.element("td").style(f"{_gb};{_brd}padding:3px 6px;font-weight:600;text-align:right"):
                                        if gkey not in ("catalogo", "propia") and item_id and price is not None:
                                            def _abrir_editar_cuota(gk=gkey, iid=item_id, sl=slot, r=row) -> None:
                                                dialog = ui.dialog()
                                                with dialog:
                                                    with ui.card().classes("p-4 min-w-[340px]"):
                                                        ui.label(f"Editar precio — {gk.upper()} — {r.get('seller_sku', '')}").classes("text-lg font-semibold mb-2")
                                                        ui.label(f"ID: {iid}").classes("text-sm text-gray-500 mb-3")
                                                        try:
                                                            precio_actual = float(sl["price"] or 0)
                                                        except (TypeError, ValueError):
                                                            precio_actual = 0.0
                                                        inp_precio = ui.input("Nuevo precio ($)", value=str(int(precio_actual))).classes("w-full")
                                                        inp_precio.props("type=number min=1 step=1")

                                                        def guardar(d=dialog, inp=inp_precio, s=sl, i=iid) -> None:
                                                            try:
                                                                nuevo = float(inp.value or 0)
                                                            except (TypeError, ValueError):
                                                                ui.notify("Precio inválido.", color="negative")
                                                                return
                                                            if nuevo < 1:
                                                                ui.notify("El precio debe ser al menos 1.", color="negative")
                                                                return
                                                            d.close()
                                                            ui.notify("Actualizando precio...", color="info")
                                                            cl = context.client

                                                            async def _actualizar(client_=cl, s2=s, precio=nuevo, item=i) -> None:
                                                                try:
                                                                    await run.io_bound(ml_update_item_price, access_token, item, precio)
                                                                    s2["price"] = precio
                                                                    with client_:
                                                                        ui.notify("Precio actualizado correctamente.", color="positive")
                                                                        _render(_sort_rows(filtrados_ref["val"]))
                                                                except requests.exceptions.HTTPError as err:
                                                                    with client_:
                                                                        ui.notify(f"Error al actualizar: {err}", color="negative")
                                                                except Exception as err:
                                                                    with client_:
                                                                        ui.notify(f"Error: {err}", color="negative")

                                                            background_tasks.create(_actualizar())

                                                        with ui.row().classes("w-full justify-end gap-2 mt-3"):
                                                            ui.button("Cancelar", on_click=lambda d=dialog: d.close()).props("flat")
                                                            ui.button("Guardar", on_click=guardar, color="primary")
                                                dialog.open()
                                            ui.button(fmt_moneda(price), on_click=_abrir_editar_cuota).props("flat dense").style("font-weight:600;font-size:10px;padding:0 4px;min-height:0;color:inherit")
                                        else:
                                            ui.label(fmt_moneda(price) if price is not None else "")
                                    # % vs Propia — todos menos Propia
                                    if gkey != "propia":
                                        with ui.element("td").style(f"{_gb};padding:3px 6px;text-align:center"):
                                            if price is not None and propia_price is not None and float(propia_price) != 0:
                                                pct = (float(price) - float(propia_price)) / float(propia_price) * 100
                                                if abs(pct) < 0.05:
                                                    ui.label("=").style("color:#757575")
                                                elif pct > 0:
                                                    ui.label(f"+{pct:.1f}%").style("color:#43a047;font-weight:500")
                                                else:
                                                    ui.label(f"{pct:.1f}%").style("color:#e53935;font-weight:500")
                                            else:
                                                ui.label("")
                                    # Check% — x3/x6/x9/x12
                                    if gkey not in ("propia", "catalogo"):
                                        with ui.element("td").style(f"{_gb};padding:3px 4px;text-align:center"):
                                            if price is not None and propia_price is not None and float(propia_price) != 0:
                                                _pct_c  = (float(price) - float(propia_price)) / float(propia_price) * 100
                                                _tasa_v = {"x3": cuotas_3x, "x6": cuotas_6x, "x9": cuotas_9x, "x12": cuotas_12x}.get(gkey, 0)
                                                _diff   = _pct_c - _tasa_v * 100
                                                if abs(_diff) <= 0.5:
                                                    ui.label("✓").style("color:#1976d2;font-weight:700;font-size:12px")
                                                elif _diff > 0.5:
                                                    ui.label("↑").style("color:#43a047;font-weight:700;font-size:12px")
                                                else:
                                                    ui.label("↓").style("color:#e53935;font-weight:700;font-size:12px")
                                            else:
                                                ui.label("")

        def _on_sort(col: str) -> None:
            if sort_col_ref["val"] == col:
                sort_asc_ref["val"] = not sort_asc_ref["val"]
            else:
                sort_col_ref["val"] = col
                sort_asc_ref["val"] = True
            _render(_sort_rows(filtrados_ref["val"]))

        def _on_filtro(e) -> None:
            txt = (getattr(filtro_input, "value", "") or "").strip().lower()
            cuotas_val    = filtro_cuotas_sel.value
            check_val     = filtro_check_sel.value
            result = list(rows_all)
            if txt:
                result = [
                    r for r in result
                    if txt in (r.get("seller_sku") or "").lower()
                    or txt in r.get("title", "").lower()
                ]
            if cuotas_val == "sin_cuotas":
                result = [r for r in result if not r["x3"]["id"] and not r["x6"]["id"] and not r["x9"]["id"] and not r["x12"]["id"]]
            elif cuotas_val == "con_cuotas":
                result = [r for r in result if r["x3"]["id"] or r["x6"]["id"] or r["x9"]["id"] or r["x12"]["id"]]
            if check_val != "todos":
                _rates = {"x3": cuotas_3x, "x6": cuotas_6x, "x9": cuotas_9x, "x12": cuotas_12x}
                def _check_match(r: dict, target: str) -> bool:
                    pp = r["propia"]["price"]
                    if pp is None or float(pp) == 0:
                        return False
                    for gk, tasa in _rates.items():
                        p = r[gk]["price"]
                        if p is None:
                            continue
                        diff = (float(p) - float(pp)) / float(pp) * 100 - tasa * 100
                        if target == "alto" and diff >  0.5:       return True
                        if target == "bajo" and diff < -0.5:       return True
                    return False
                result = [r for r in result if _check_match(r, check_val)]
            filtrados_ref["val"] = result
            _render(_sort_rows(filtrados_ref["val"]))

        filtro_input.on_value_change(_on_filtro)
        filtro_cuotas_sel.on_value_change(lambda *a: _on_filtro(None))
        filtro_check_sel.on_value_change(lambda *a: _on_filtro(None))
        _render(_sort_rows(rows_all))


# ---------------------------------------------------------------------------
# Tab principal
# ---------------------------------------------------------------------------

def build_tab_cuotas(container, force_refresh: bool = False) -> None:
    """Pestaña Cuotas: lista deduplicada por SKU con info de cuotas de cada publicación."""
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
                ui.label("Cargando cuotas...").classes("text-xl text-gray-700")

        async def _cargar_cuotas_async() -> None:
            try:
                data = await run.io_bound(ml_get_my_items, access_token, False, force_refresh)
            except requests.exceptions.HTTPError as e:
                result_area.clear()
                with result_area:
                    ui.label(f"❌ Error de la API de MercadoLibre: {e}").classes("text-negative mb-2")
                return
            except Exception as e:
                result_area.clear()
                with result_area:
                    ui.label(f"❌ Error al conectar: {e}").classes("text-negative")
                return
            # Construir grupos para identificar el item representante por grupo
            items_raw = data.get("results", [])
            grps: Dict[tuple, list] = {}
            for _it in items_raw:
                grps.setdefault(_cuotas_key(_it), []).append(_it)
            _grp_list = list(grps.values())
            rep_ids: list = [str(max(_g, key=_cuotas_score).get("id") or "") for _g in _grp_list if _g]
            _all_iids_per_grp: List[List[str]] = [
                [str(_it.get("id") or "") for _it in _g if _it.get("id")]
                for _g in _grp_list if _g
            ]
            seller_id = ""
            try:
                profile = await run.io_bound(ml_get_user_profile, access_token)
                seller_id = str((profile or {}).get("id") or "")
            except Exception:
                pass
            total_grupos = len(rep_ids)
            result_area.clear()
            promo_lbl = None
            with result_area:
                with ui.card().classes("w-full p-8 items-center gap-4"):
                    ui.spinner(size="xl")
                    promo_lbl = ui.label(f"Cargando cuotas 0/{total_grupos}...").classes("text-xl text-gray-700")
            _empty_pd: Dict = {"price_promo": None, "meli_pct": None, "seller_pct": None}
            promo_data: Dict[str, Dict] = {}
            for _i, (_rep_id, _iids) in enumerate(zip(rep_ids, _all_iids_per_grp)):
                if not _iids:
                    promo_data[_rep_id] = _empty_pd
                    continue
                _pds = await asyncio.gather(*[
                    run.io_bound(_get_promo_data, access_token, _iid, seller_id)
                    for _iid in _iids
                ])
                best_pd, best_price = _empty_pd, None
                for _pd in _pds:
                    _pp = _pd.get("price_promo")
                    if _pp is not None and (best_price is None or float(_pp) < best_price):
                        best_price, best_pd = float(_pp), _pd
                promo_data[_rep_id] = best_pd
                if promo_lbl:
                    promo_lbl.set_text(f"Cargando cuotas {_i + 1}/{total_grupos}...")
            try:
                _mostrar_tabla_cuotas(result_area, data, access_token, promo_data, container, user["id"])
            except Exception as e:
                result_area.clear()
                with result_area:
                    ui.label(f"❌ Error al mostrar datos: {e}").classes("text-negative")

        background_tasks.create(_cargar_cuotas_async(), name="cargar_cuotas")
