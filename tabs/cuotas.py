"""
Fase 3 — tabs/cuotas.py
Pestaña Cuotas: lista deduplicada por SKU con info de cuotas de cada publicación.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import requests
from nicegui import app, background_tasks, context, run, ui

from db import get_cotizador_param
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
        return ("sku", sku)
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
        return {"price_promo": amt_f, "meli_pct": d.get("meli_pct"), "seller_pct": d.get("seller_pct")}
    return {"price_promo": amt_f, "meli_pct": None, "seller_pct": None}


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
            if not is_cat and lt == "gold_special":
                if propia is None or _cuotas_score(it) > _cuotas_score(propia):
                    propia = it
            if is_cat:
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
            "promo_item_id": str(best.get("id") or ""),
            "propia":        _slot(propia),
            "catalogo":      _slot(catalogo),
            "x3":            _slot(x3),
            "x6":            _slot(x6),
            "x9":            _slot(x9),
            "x12":           _slot(x12),
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
        ("propia",   "Publicación Propia", "#F0F7FF", "#DCEEFB", "#C5DCFA"),
        ("catalogo", "Catálogo",           "#F1F8F1", "#D6EDD6", "#B8D9B8"),
        ("x3",       "3 Cuotas",           "#FFFAF0", "#FFF0CC", "#FFE0B2"),
        ("x6",       "6 Cuotas",           "#FAF5FC", "#EDD9F5", "#E1BEE7"),
        ("x9",       "9 Cuotas",           "#FFF5F5", "#FFE0E0", "#FFCCCC"),
        ("x12",      "12 Cuotas",          "#F5F5FF", "#E0E0FF", "#CCCCFF"),
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
        "catalogo_price": lambda r: r["catalogo"]["price"] if r["catalogo"]["price"] is not None else -1,
        "x3_price":       lambda r: r["x3"]["price"]       if r["x3"]["price"]       is not None else -1,
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
    n_promos   = sum(1 for r in rows_all if r.get("promo", {}).get("price_promo") is not None)
    _tot = len(rows_all)
    pct_x3  = n_x3  / _tot * 100 if _tot else 0
    pct_x6  = n_x6  / _tot * 100 if _tot else 0
    pct_x9  = n_x9  / _tot * 100 if _tot else 0
    pct_x12 = n_x12 / _tot * 100 if _tot else 0

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
                    for _label, _rate in [
                        ("3x_campaign",  cuotas_3x),
                        ("6x_campaign",  cuotas_6x),
                        ("9x_campaign",  cuotas_9x),
                        ("12x_campaign", cuotas_12x),
                    ]:
                        with ui.element("div").style("display:flex;flex-direction:column;align-items:center;min-width:70px"):
                            ui.label(f"{_rate*100:.1f}%").classes("text-secondary text-xl font-bold leading-tight")
                            ui.label(_label).classes("text-xs text-gray-600 text-center")
                if container is not None:
                    ui.element("div").style("width:1px;height:48px;background:#bdbdbd;align-self:center;margin:0 4px")
                    ui.button("Sincronizar", icon="sync", on_click=lambda: build_tab_cuotas(container)).props("flat dense")

        with ui.row().classes("items-center gap-3 mb-3"):
            filtro_cuotas_sel = ui.select(
                {"cualquiera": "Cualquiera", "sin_cuotas": "Sin cuotas", "con_cuotas": "Con cuotas"},
                value="cualquiera", label="Cuotas"
            ).classes("w-36").props("outlined dense")
            filtro_promo_sel = ui.select(
                {"cualquiera": "Cualquiera", "con_promo": "Con promoción", "sin_promo": "Sin promoción"},
                value="cualquiera", label="Promos"
            ).classes("w-36").props("outlined dense")
            filtro_check_sel = ui.select(
                {"todos": "Todos", "ok": "OK", "alto": "Precio alto", "bajo": "Precio bajo"},
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
                ui.element("col").style("width:7%")
                ui.element("col").style("width:20%")
                ui.element("col").style("width:3%")
                ui.element("col").style("width:2%")
                ui.element("col").style("width:2%")
                ui.element("col").style("width:2%")
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
                            with ui.element("th").props('rowspan="2"').style(f"{TH_HDR1};width:7%;text-align:center;cursor:pointer").on("click", lambda: _on_sort("seller_sku")):
                                ui.label("SKU" + _ind("seller_sku"))
                            with ui.element("th").props('rowspan="2"').style(f"{TH_HDR1};width:20%;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;text-align:center;cursor:pointer").on("click", lambda: _on_sort("title")):
                                ui.label("Nombre" + _ind("title"))
                            with ui.element("th").props('rowspan="2"').style(f"{TH_HDR1};width:3%;text-align:center;cursor:pointer").on("click", lambda: _on_sort("stock")):
                                ui.label("Stock" + _ind("stock"))
                            with ui.element("th").props('colspan="3"').style(f"{TH_HDR1};border-left:2px solid {PROMO_BORDER};text-align:center"):
                                ui.label("Promociones")
                            for gkey, glabel, gbg_e, gbg_o, gborder in GROUPS:
                                _cspan = "3" if gkey not in ("propia", "catalogo") else "2"
                                with ui.element("th").props(f'colspan="{_cspan}"').style(f"{TH_HDR1};border-left:2px solid {gborder};text-align:center"):
                                    ui.label(glabel)
                        with ui.element("tr"):
                            with ui.element("th").style(f"{TH_HDR2};width:2%;border-left:2px solid {PROMO_BORDER};text-align:center"):
                                ui.label("% ML")
                            with ui.element("th").style(f"{TH_HDR2};width:2%;text-align:center"):
                                ui.label("% Vend.")
                            with ui.element("th").style(f"{TH_HDR2};width:2%;text-align:center"):
                                ui.label("% vs Propia")
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
                                                with ui.element("table").style("width:100%;border-collapse:collapse;font-size:11px"):
                                                    with ui.element("thead"):
                                                        with ui.element("tr"):
                                                            for _h in ["Tipo", "ID", "Precio", "% vs Propia"]:
                                                                with ui.element("th").style("padding:4px 8px;background:#1976d2;color:white;text-align:left;font-weight:600"):
                                                                    ui.label(_h)
                                                    with ui.element("tbody"):
                                                        for _tipo, _sk in [("Propia","propia"),("Catálogo","catalogo"),("3 Cuotas","x3"),("6 Cuotas","x6"),("9 Cuotas","x9"),("12 Cuotas","x12")]:
                                                            _s = r[_sk]; _sid = _s["id"]; _sp = _s["price"]; _slink = _s["permalink"]
                                                            with ui.element("tr").style("border-bottom:1px solid #e5e7eb"):
                                                                with ui.element("td").style("padding:3px 8px;font-weight:500"):
                                                                    ui.label(_tipo)
                                                                with ui.element("td").style("padding:3px 8px;font-family:monospace;font-size:10px"):
                                                                    if _sid and _slink:
                                                                        ui.link(_sid, _slink, new_tab=True).classes("text-blue-700 hover:underline")
                                                                    elif _sid:
                                                                        ui.label(_sid)
                                                                    elif _sk in ("x3", "x6", "x9", "x12"):
                                                                        ui.label("Crear desde panel ML").classes("text-gray-400 text-xs italic")
                                                                    else:
                                                                        ui.label("—").classes("text-gray-400")
                                                                with ui.element("td").style("padding:3px 8px;font-weight:600;text-align:right"):
                                                                    if _sk != "catalogo" and _sid and _sp is not None:
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
                                                with ui.row().classes("w-full justify-end"):
                                                    ui.button("Cerrar", on_click=dlg.close).props("flat")
                                        dlg.open()
                                    ui.button(title_text, on_click=_abrir_detalle).props("flat dense align=left").style(
                                        "font-size:10px;padding:0 2px;min-height:0;text-transform:none;color:inherit;"
                                        "font-weight:normal;width:100%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;text-align:left"
                                    )
                                with ui.element("td").style(f"{TD_BASE};text-align:center"):
                                    sv = row.get("stock")
                                    ui.label(str(sv) if sv is not None else "")
                                propia_price = row["propia"]["price"]
                                promo = row.get("promo", {})
                                promo_bg = PROMO_BG_EVEN if idx % 2 == 0 else PROMO_BG_ODD
                                promo_price = promo.get("price_promo")
                                _pb = f"background:{promo_bg};border-bottom:1px solid #e5e7eb;font-size:10px"
                                # % ML
                                with ui.element("td").style(f"{_pb};border-left:2px solid {PROMO_BORDER};padding:3px 6px;text-align:center"):
                                    meli_pct = promo.get("meli_pct")
                                    if meli_pct is not None:
                                        ui.label(f"{meli_pct:.1f}%").style("color:#43a047;font-weight:500")
                                    else:
                                        ui.label("")
                                # % Vendedor
                                with ui.element("td").style(f"{_pb};padding:3px 6px;text-align:center"):
                                    seller_pct = promo.get("seller_pct")
                                    if seller_pct is not None:
                                        ui.label(f"{seller_pct:.1f}%").style("color:#e65100;font-weight:500")
                                    else:
                                        ui.label("")
                                # % vs Propia (Promos) — positivo=verde, negativo=rojo
                                with ui.element("td").style(f"{_pb};padding:3px 6px;text-align:center"):
                                    if promo_price is not None and propia_price is not None and float(propia_price) != 0:
                                        pct_p = (float(promo_price) - float(propia_price)) / float(propia_price) * 100
                                        if abs(pct_p) < 0.05:
                                            ui.label("=").style("color:#757575")
                                        elif pct_p > 0:
                                            ui.label(f"+{pct_p:.1f}%").style("color:#43a047;font-weight:500")
                                        else:
                                            ui.label(f"{pct_p:.1f}%").style("color:#e53935;font-weight:500")
                                    else:
                                        ui.label("")
                                for gkey, glabel, gbg_e, gbg_o, gborder in GROUPS:
                                    gbg = gbg_e if idx % 2 == 0 else gbg_o
                                    slot = row[gkey]
                                    item_id   = slot["id"]
                                    permalink = slot["permalink"]
                                    price     = slot["price"]
                                    _gb = f"background:{gbg};border-bottom:1px solid #e5e7eb;font-size:10px"
                                    # Publicación — solo Propia
                                    if gkey == "propia":
                                        with ui.element("td").style(f"{_gb};border-left:2px solid {gborder};padding:3px 6px;font-size:9px;font-family:monospace;text-align:center;overflow:hidden;text-overflow:ellipsis;white-space:nowrap"):
                                            if item_id and permalink:
                                                ui.link(item_id, permalink, new_tab=True).classes("text-blue-700 hover:underline")
                                            elif item_id:
                                                ui.label(item_id)
                                            else:
                                                ui.label("")
                                    # Precio — todos (border-left en no-propia como separador de grupo)
                                    _brd = "" if gkey == "propia" else f"border-left:2px solid {gborder};"
                                    with ui.element("td").style(f"{_gb};{_brd}padding:3px 6px;font-weight:600;text-align:right"):
                                        if gkey != "catalogo" and item_id and price is not None:
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
                                                _pct_c = (float(price) - float(propia_price)) / float(propia_price) * 100
                                                _tasa  = {"x3": cuotas_3x, "x6": cuotas_6x, "x9": cuotas_9x, "x12": cuotas_12x}.get(gkey, 0) * 100
                                                _diff  = _pct_c - _tasa
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
            cuotas_val = filtro_cuotas_sel.value
            promo_val  = filtro_promo_sel.value
            check_val  = filtro_check_sel.value
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
            if promo_val == "con_promo":
                result = [r for r in result if r.get("promo", {}).get("price_promo") is not None]
            elif promo_val == "sin_promo":
                result = [r for r in result if r.get("promo", {}).get("price_promo") is None]
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
                        if target == "ok"   and abs(diff) <= 0.5: return True
                        if target == "alto" and diff >  0.5:       return True
                        if target == "bajo" and diff < -0.5:       return True
                    return False
                result = [r for r in result if _check_match(r, check_val)]
            filtrados_ref["val"] = result
            _render(_sort_rows(filtrados_ref["val"]))

        filtro_input.on_value_change(_on_filtro)
        filtro_cuotas_sel.on_value_change(lambda *a: _on_filtro(None))
        filtro_promo_sel.on_value_change(lambda *a: _on_filtro(None))
        filtro_check_sel.on_value_change(lambda *a: _on_filtro(None))
        _render(_sort_rows(rows_all))


# ---------------------------------------------------------------------------
# Tab principal
# ---------------------------------------------------------------------------

def build_tab_cuotas(container) -> None:
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
                data = await run.io_bound(ml_get_my_items, access_token, False)
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
            rep_ids: list = []
            for _g in grps.values():
                _rid = ""
                for _it in _g:
                    if not _it.get("catalog_listing") and str(_it.get("listing_type_id") or "").lower() == "gold_special":
                        _rid = str(_it.get("id") or "")
                        break
                if not _rid:
                    for _it in _g:
                        if _it.get("catalog_listing"):
                            _rid = str(_it.get("id") or "")
                            break
                if not _rid and _g:
                    _rid = str(_g[0].get("id") or "")
                if _rid:
                    rep_ids.append(_rid)
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
                    promo_lbl = ui.label(f"Cargando promociones 0/{total_grupos}...").classes("text-xl text-gray-700")
            promo_data: Dict[str, Dict] = {}
            for _i, _iid in enumerate(rep_ids):
                promo_data[_iid] = await run.io_bound(_get_promo_data, access_token, _iid, seller_id)
                if promo_lbl:
                    promo_lbl.set_text(f"Cargando promociones {_i + 1}/{total_grupos}...")
            try:
                _mostrar_tabla_cuotas(result_area, data, access_token, promo_data, container, user["id"])
            except Exception as e:
                result_area.clear()
                with result_area:
                    ui.label(f"❌ Error al mostrar datos: {e}").classes("text-negative")

        background_tasks.create(_cargar_cuotas_async(), name="cargar_cuotas")
