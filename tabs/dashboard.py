"""
tabs/dashboard.py
Pestaña Dashboard: resumen ejecutivo con alertas y KPIs de ML + ARCA.
Exporta: build_tab_dashboard
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from nicegui import app, background_tasks, run, ui

from db import get_connection, get_cotizador_param, get_arca_datos, get_arca_multilateral
from ml_api import get_ml_access_token, ml_get_user_profile, ml_get_my_items, _cuotas_desde_item

_GREEN  = "#3B6D11"
_YELLOW = "#BA7517"
_RED    = "#A32D2D"
_BLUE   = "#185FA5"
_BG     = {_RED: "#FCEBEB", _YELLOW: "#FAEEDA", _GREEN: "#EAF3DE"}

MAX_CLAIMS, MAX_MEDIAT, MAX_CANC, MAX_DELAYED = 0.01, 0.005, 0.005, 0.08

_MESES_ES = ["enero","febrero","marzo","abril","mayo","junio",
             "julio","agosto","septiembre","octubre","noviembre","diciembre"]


def _require_login():
    user = app.storage.user.get("user")
    if not user:
        ui.notify("Debes iniciar sesión para continuar", color="negative")
    return user


def _to_float(val, default=0.0):
    try:
        return float(val) if val is not None and str(val).strip() != "" else default
    except (ValueError, TypeError):
        return default


# ── Color helpers ─────────────────────────────────────────────────────────────

def _color_siper(cat: str) -> str:
    c = (cat or "").strip().upper()
    if not c:                                          return _YELLOW
    if c.startswith("A"):                              return _GREEN
    if c.startswith("B") or c.startswith("C"):        return _YELLOW
    return _RED

def _color_iva(tec: str, lib: str) -> str:
    if not (tec or "").strip() and not (lib or "").strip(): return _YELLOW
    t, l = _to_float(tec), _to_float(lib)
    if t >= 0 and l >= 0: return _GREEN
    if t < 0  and l >= 0: return _YELLOW
    return _RED

def _color_deuda(deu: str, intim: bool) -> str:
    if not intim and not (deu or "").strip():          return _YELLOW
    return _RED if intim or _to_float(deu) > 0 else _GREEN

def _color_multilateral(filas: List[Dict]) -> str:
    if not filas:                                      return _YELLOW
    vals = [_to_float(f.get("a_pagar")) for f in filas]
    if any(v > 10_000 for v in vals): return _RED
    if any(v > 0      for v in vals): return _YELLOW
    return _GREEN


# ── DB Queries ────────────────────────────────────────────────────────────────

def _query_productos(user_id: int) -> Dict[str, int]:
    dolar   = _to_float(get_cotizador_param("dolar_sistema", user_id), 1500.0)
    cobrado = _to_float(get_cotizador_param("ml_cobrado",    user_id), 0.836)
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM productos WHERE user_id=? AND (costo_usd IS NULL OR costo_usd=0)",
            (user_id,))
        sin_costo = cur.fetchone()[0]

        cur.execute(
            "SELECT COUNT(*) FROM productos WHERE user_id=? AND (fob_usd IS NULL OR fob_usd=0)",
            (user_id,))
        sin_fob = cur.fetchone()[0]

        cur.execute(
            "SELECT COUNT(DISTINCT pub.ml_id) FROM ml_publicaciones pub "
            "WHERE pub.user_id=? AND LOWER(pub.estado) LIKE '%suspend%'",
            (user_id,))
        stock_susp = cur.fetchone()[0]

        cur.execute(
            """SELECT COUNT(DISTINCT p.sku)
               FROM productos p
               JOIN ml_publicaciones pub ON pub.sku=p.sku AND pub.user_id=p.user_id
               WHERE p.user_id=? AND LOWER(pub.estado)='active'
                 AND p.costo_usd > 0
                 AND (pub.precio * ?) < (p.costo_usd * ?)""",
            (user_id, cobrado, dolar))
        gan_neg = cur.fetchone()[0]
        return {"sin_costo": sin_costo, "sin_fob": sin_fob, "stock_susp": stock_susp, "gan_neg": gan_neg}
    finally:
        conn.close()


def _query_ventas(user_id: int) -> Dict[str, int]:
    desde = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM ventas_datos WHERE user_id=? AND (gan_pesos IS NULL OR gan_pesos = 0) AND fetched_at >= ?",
            (user_id, desde))
        sin_revisar = cur.fetchone()[0]
        cur.execute(
            "SELECT COUNT(*) FROM ventas_datos WHERE user_id=? AND gan_pesos < 0 AND fetched_at >= ?",
            (user_id, desde))
        gan_neg = cur.fetchone()[0]
        return {"sin_revisar": sin_revisar, "gan_neg": gan_neg}
    finally:
        conn.close()


def _query_arca() -> Dict[str, Any]:
    return {
        "siper":   get_arca_datos("siper"),
        "iva":     get_arca_datos("iva"),
        "deuda":   get_arca_datos("deuda"),
        "ml_rows": get_arca_multilateral(),
    }


# ── Alert builders ────────────────────────────────────────────────────────────

def _arca_alerts(data: Dict) -> List[Tuple[str, str]]:
    alerts: List[Tuple[str, str]] = []
    siper_d, iva_d, deuda_d, ml_rows = data["siper"], data["iva"], data["deuda"], data["ml_rows"]

    siper_cat = (siper_d.get("categoria_siper") or "").strip().upper()
    if siper_cat and (siper_cat.startswith("D") or siper_cat.startswith("E") or "OBSERV" in siper_cat):
        alerts.append((_RED, "SIPER con observaciones — revisar categoría"))

    deuda = _to_float(deuda_d.get("deuda_exigible"))
    if deuda > 0:
        alerts.append((_RED, f"Deuda exigible pendiente: ${deuda:,.0f}"))

    if deuda_d.get("tiene_intimacion") == "true":
        alerts.append((_RED, "Intimación activa en ARCA"))

    libre_str = iva_d.get("saldo_libre_disponibilidad", "")
    libre_iva = _to_float(libre_str)
    if libre_str.strip() and libre_iva < 0:
        alerts.append((_RED, "Saldo IVA libre disponibilidad negativo"))

    prov_pagar = [r["provincia"] for r in ml_rows if _to_float(r.get("a_pagar")) > 0]
    if prov_pagar:
        n = len(prov_pagar)
        alerts.append((_RED, f"Convenio Multilateral: {n} provincia{'s' if n > 1 else ''} con saldo a pagar"))

    tec_str = iva_d.get("saldo_tecnico", "")
    tec_iva = _to_float(tec_str)
    if tec_str.strip() and tec_iva < 0 and libre_iva >= 0:
        alerts.append((_YELLOW, "Saldo IVA técnico negativo"))

    campos_req = {
        "SIPER":                    siper_d.get("categoria_siper", ""),
        "IVA saldo técnico":        iva_d.get("saldo_tecnico", ""),
        "IVA libre disponibilidad": iva_d.get("saldo_libre_disponibilidad", ""),
        "Deuda exigible":           deuda_d.get("deuda_exigible", ""),
    }
    faltantes = [k for k, v in campos_req.items() if not (v or "").strip()]
    if faltantes:
        alerts.append((_YELLOW, f"ARCA incompleto: faltan datos en {', '.join(faltantes)}"))

    if not ml_rows:
        alerts.append((_YELLOW, "Convenio Multilateral sin datos cargados"))

    if not alerts:
        alerts.append((_GREEN, "ARCA al día"))

    return alerts


def _rep_rate(m: Dict) -> float:
    exc = (m or {}).get("excluded") or {}
    r = exc.get("real_rate")
    if isinstance(r, (int, float)): return float(r)
    r = (m or {}).get("rate")
    return float(r) if isinstance(r, (int, float)) else 0.0


def _rep_alerts(metrics: Dict) -> List[Tuple[str, str]]:
    alerts: List[Tuple[str, str]] = []
    rc  = _rep_rate(metrics.get("claims")                or {})
    rm  = _rep_rate(metrics.get("mediations")            or {})
    rca = _rep_rate(metrics.get("cancellations")         or {})
    rd  = _rep_rate(metrics.get("delayed_handling_time") or {})
    if rc  > MAX_CLAIMS:  alerts.append((_RED, f"Reclamos ML: {rc*100:.1f}% (máx {MAX_CLAIMS*100:.0f}%)"))
    if rm  > MAX_MEDIAT:  alerts.append((_RED, f"Mediaciones ML: {rm*100:.1f}% (máx {MAX_MEDIAT*100:.1f}%)"))
    if rca > MAX_CANC:    alerts.append((_RED, f"Cancelaciones ML: {rca*100:.1f}% (máx {MAX_CANC*100:.1f}%)"))
    if rd  > MAX_DELAYED: alerts.append((_RED, f"Demora envíos ML: {rd*100:.0f}% (máx {MAX_DELAYED*100:.0f}%)"))
    return alerts


# ── UI helpers ────────────────────────────────────────────────────────────────

def _dot(color: str):
    return ui.element("span").style(
        f"display:inline-block;width:10px;height:10px;border-radius:9999px;"
        f"background:{color};flex-shrink:0")

def _card_header(title: str, color: str):
    with ui.row().classes("items-center gap-2 w-full mb-2"):
        _dot(color)
        ui.label(title).classes("font-bold text-base text-gray-800")

def _alert_row(container, color: str, msg: str):
    with container:
        with ui.row().classes("items-center gap-2 w-full px-3 py-2 rounded").style(
            f"background:{_BG.get(color, '#f9f9f9')}"):
            _dot(color)
            ui.label(msg).classes("text-sm text-gray-800")

def _progress_bar(label: str, pct: float, count: int, total: int):
    with ui.column().classes("w-full gap-1"):
        with ui.row().classes("w-full justify-between items-center"):
            ui.label(label).classes("text-sm text-gray-700")
            ui.label(f"{count} / {total}  ({pct:.0f}%)").classes("text-xs text-gray-500")
        outer = ui.element("div").classes("w-full rounded").style("background:#e5e7eb;height:8px")
        with outer:
            ui.element("div").style(
                f"width:{min(max(pct, 0), 100):.1f}%;height:100%;"
                f"background:{_BLUE};border-radius:4px;transition:width 0.3s")

def _stat_row(label: str, value: str, color: str):
    with ui.row().classes("items-center gap-2 w-full"):
        _dot(color)
        ui.label(label).classes("text-sm text-gray-700 flex-1")
        ui.label(value).classes("text-sm font-semibold").style(f"color:{color}")

def _rep_stat_row(label: str, rate: float, maxv: float):
    c = _RED if rate > maxv else (_YELLOW if rate > maxv * 0.7 else _GREEN)
    _stat_row(f"{label} (máx {maxv*100:.1f}%)", f"{rate*100:.2f}%", c)


# ── Función exportada ─────────────────────────────────────────────────────────

def build_tab_dashboard(container) -> None:
    user = _require_login()
    if not user:
        return
    uid = user["id"]

    prod         = _query_productos(uid)
    ventas       = _query_ventas(uid)
    arca_data    = _query_arca()
    arca_al      = _arca_alerts(arca_data)
    access_token = get_ml_access_token(uid)
    desde_dt     = datetime.now() - timedelta(days=30)
    desde_fmt    = desde_dt.strftime("%d/%m/%Y")

    # Alertas de DB (sin reputación todavía)
    db_alerts: List[Tuple[str, str]] = []
    if prod["sin_costo"]     > 0: db_alerts.append((_RED,    f"Productos sin costo u$s/IVA: {prod['sin_costo']}"))
    if prod["stock_susp"]    > 0: db_alerts.append((_RED,    f"Publicaciones suspendidas: {prod['stock_susp']}"))
    if ventas["gan_neg"]     > 0: db_alerts.append((_RED,    f"Ventas (últimos 30 días) con ganancia negativa: {ventas['gan_neg']}"))
    if prod["sin_fob"]       > 0: db_alerts.append((_YELLOW, f"Productos sin FOB u$: {prod['sin_fob']}"))
    if prod["gan_neg"]       > 0: db_alerts.append((_YELLOW, f"Publicaciones con ganancia negativa estimada: {prod['gan_neg']}"))
    if ventas["sin_revisar"] > 0: db_alerts.append((_YELLOW, f"Ventas sin revisar (últimos 30 días): {ventas['sin_revisar']}"))
    for ac, am in arca_al:
        if ac != _GREEN:
            db_alerts.append((ac, am))
    db_alerts.sort(key=lambda x: {_RED: 0, _YELLOW: 1}.get(x[0], 2))

    n_red_init    = sum(1 for c, _ in db_alerts if c == _RED)
    n_yellow_init = sum(1 for c, _ in db_alerts if c == _YELLOW)

    with container:
        with ui.column().classes("w-full gap-4 p-4").style("max-width:1200px"):

            # ── BARRA DE RESUMEN ──────────────────────────────────────────
            with ui.card().classes("w-full p-3 bg-grey-2").style("border:1px solid #e0e0e0"):
                with ui.row().classes("w-full items-center gap-4"):
                    with ui.row().classes("items-center gap-1"):
                        ui.icon("error", size="xs").style(f"color:{_RED}")
                        red_count_lbl = ui.label(str(n_red_init)).classes("font-bold text-sm").style(f"color:{_RED}")
                        ui.label("urgente(s)").classes("text-sm text-gray-600")
                    with ui.row().classes("items-center gap-1"):
                        ui.icon("warning", size="xs").style(f"color:{_YELLOW}")
                        yel_count_lbl = ui.label(str(n_yellow_init)).classes("font-bold text-sm").style(f"color:{_YELLOW}")
                        ui.label("importante(s)").classes("text-sm text-gray-600")
                    ui.element("div").classes("flex-1")
                    ui.button("Actualizar", icon="refresh",
                              on_click=lambda: (container.clear(), build_tab_dashboard(container))
                              ).props("flat dense")

            # ── ALERTAS ───────────────────────────────────────────────────
            with ui.card().classes("w-full").style("border:1px solid #e0e0e0"):
                ui.label("Alertas activas").classes("font-bold text-base text-gray-800 mb-2")
                alerts_col = ui.column().classes("w-full gap-1")
                for color, msg in db_alerts:
                    _alert_row(alerts_col, color, msg)
                rep_placeholder = ui.row().classes("items-center gap-2 w-full px-3 py-2")
                with rep_placeholder:
                    ui.spinner(size="xs")
                    ui.label("Cargando estadísticas ML...").classes("text-xs text-gray-400")

            # ── GRILLA 1: Productos | Ventas ──────────────────────────────
            with ui.grid(columns=2).classes("w-full gap-4"):

                prod_color = (_RED    if prod["sin_costo"]  > 0 or prod["stock_susp"] > 0
                              else _YELLOW if prod["sin_fob"]   > 0 or prod["gan_neg"]   > 0
                              else _GREEN)
                with ui.card().classes("w-full").style("border:1px solid #e0e0e0"):
                    _card_header("Productos", prod_color)
                    with ui.column().classes("w-full gap-2"):
                        _stat_row("Sin costo u$s/IVA",        str(prod["sin_costo"]),  _RED    if prod["sin_costo"]  > 0 else _GREEN)
                        _stat_row("Suspendidas con stock",    str(prod["stock_susp"]), _RED    if prod["stock_susp"] > 0 else _GREEN)
                        _stat_row("Sin FOB u$",               str(prod["sin_fob"]),    _YELLOW if prod["sin_fob"]   > 0 else _GREEN)
                        _stat_row("Gan$ negativa",            str(prod["gan_neg"]),    _YELLOW if prod["gan_neg"]   > 0 else _GREEN)

                ven_color = (_RED    if ventas["gan_neg"]     > 0
                             else _YELLOW if ventas["sin_revisar"] > 0
                             else _GREEN)
                with ui.card().classes("w-full").style("border:1px solid #e0e0e0"):
                    _card_header("Ventas — últimos 30 días", ven_color)
                    with ui.column().classes("w-full gap-2"):
                        _stat_row("Gan$ negativa", str(ventas["gan_neg"]),     _RED    if ventas["gan_neg"]     > 0 else _GREEN)
                        _stat_row("Sin revisar",   str(ventas["sin_revisar"]), _YELLOW if ventas["sin_revisar"] > 0 else _GREEN)
                    ui.label(f"Desde el {desde_fmt}").classes("text-xs text-gray-400 mt-2")

            # ── GRILLA 2: Cuotas y promos | Estadísticas ML ───────────────
            with ui.grid(columns=2).classes("w-full gap-4"):

                cuotas_card = ui.card().classes("w-full").style("border:1px solid #e0e0e0")
                with cuotas_card:
                    with ui.row().classes("items-center gap-2 mb-2"):
                        ui.spinner(size="sm")
                        ui.label("Cuotas y promos").classes("font-bold text-base text-gray-800")
                    ui.label("Cargando datos de cuotas...").classes("text-sm text-gray-400")

                rep_card = ui.card().classes("w-full").style("border:1px solid #e0e0e0")
                with rep_card:
                    with ui.row().classes("items-center gap-2 mb-2"):
                        ui.spinner(size="sm")
                        ui.label("Estadísticas ML").classes("font-bold text-base text-gray-800")
                    ui.label("Cargando reputación...").classes("text-sm text-gray-400")

            # ── ARCA (ancho completo) ──────────────────────────────────────
            arca_ov = _GREEN
            for ac, _ in arca_al:
                if ac == _RED:    arca_ov = _RED;    break
                if ac == _YELLOW: arca_ov = _YELLOW

            with ui.card().classes("w-full").style("border:1px solid #e0e0e0"):
                _card_header("ARCA — Resumen Fiscal", arca_ov)
                sd, id_, dd, mr = arca_data["siper"], arca_data["iva"], arca_data["deuda"], arca_data["ml_rows"]
                with ui.grid(columns=4).classes("w-full gap-4 mt-1"):

                    siper_v = sd.get("categoria_siper") or ""
                    with ui.column().classes("gap-1"):
                        with ui.row().classes("items-center gap-1 mb-1"):
                            _dot(_color_siper(siper_v))
                            ui.label("SIPER").classes("text-xs font-semibold text-gray-600")
                        ui.label(siper_v or "Sin datos").classes("text-sm text-gray-800")

                    tec_v = id_.get("saldo_tecnico", "")
                    lib_v = id_.get("saldo_libre_disponibilidad", "")
                    with ui.column().classes("gap-1"):
                        with ui.row().classes("items-center gap-1 mb-1"):
                            _dot(_color_iva(tec_v, lib_v))
                            ui.label("Saldo IVA").classes("text-xs font-semibold text-gray-600")
                        ui.label(f"Técnico: ${_to_float(tec_v):,.0f}" if tec_v else "Sin datos").classes("text-sm text-gray-800")
                        if lib_v:
                            ui.label(f"Libre disp: ${_to_float(lib_v):,.0f}").classes("text-xs text-gray-500")

                    deu_v   = dd.get("deuda_exigible", "")
                    intim_v = dd.get("tiene_intimacion") == "true"
                    with ui.column().classes("gap-1"):
                        with ui.row().classes("items-center gap-1 mb-1"):
                            _dot(_color_deuda(deu_v, intim_v))
                            ui.label("Deuda / Planes").classes("text-xs font-semibold text-gray-600")
                        ui.label(f"${_to_float(deu_v):,.0f}" if deu_v else "Sin datos").classes("text-sm text-gray-800")
                        if intim_v:
                            ui.label("Intimación activa").classes("text-xs font-semibold").style(f"color:{_RED}")

                    mc          = _color_multilateral(mr)
                    total_pagar = sum(_to_float(r.get("a_pagar")) for r in mr)
                    with ui.column().classes("gap-1"):
                        with ui.row().classes("items-center gap-1 mb-1"):
                            _dot(mc)
                            ui.label("Multilateral").classes("text-xs font-semibold text-gray-600")
                        if mr:
                            ui.label(f"{len(mr)} provincia(s)").classes("text-sm text-gray-800")
                            if total_pagar > 0:
                                ui.label(f"A pagar: ${total_pagar:,.0f}").classes("text-xs font-semibold").style(f"color:{_RED}")
                            else:
                                ui.label("Sin saldo a pagar").classes("text-xs text-gray-500")
                        else:
                            ui.label("Sin datos").classes("text-sm text-gray-400")

    # ── Async tasks ───────────────────────────────────────────────────────────

    if not access_token:
        rep_card.clear()
        with rep_card:
            _card_header("Estadísticas ML", "#6b7280")
            ui.label("Sin token ML configurado").classes("text-sm text-gray-400")
        rep_placeholder.delete()
        cuotas_card.clear()
        with cuotas_card:
            _card_header("Cuotas y promos", "#6b7280")
            ui.label("Sin token ML configurado").classes("text-sm text-gray-400")
        if not db_alerts:
            _alert_row(alerts_col, _GREEN, "Todo en orden — sin alertas activas")
        return

    async def _cargar_rep() -> None:
        try:
            profile  = await run.io_bound(ml_get_user_profile, access_token)
            rep      = (profile or {}).get("seller_reputation") or {}
            metrics  = rep.get("metrics") or {}
            level_id = rep.get("level_id") or "—"
            ra = _rep_alerts(metrics)

            rep_card.clear()
            with rep_card:
                rc_col = (_RED    if any(c == _RED for c, _ in ra)
                          else _YELLOW if ra
                          else _GREEN)
                _card_header("Estadísticas ML", rc_col)
                level_colors = {
                    "1_red": "#ef4444", "2_orange": "#f97316", "3_yellow": "#eab308",
                    "4_light_green": "#84cc16", "5_green": "#22c55e",
                }
                lc = level_colors.get(str(level_id), "#6b7280")
                ui.label(f"Nivel: {str(level_id).replace('_', ' ').title()}").classes(
                    "text-xs mb-2").style(f"color:{lc};font-weight:600")
                with ui.column().classes("w-full gap-2"):
                    for key, label, maxv in [
                        ("claims",                "Reclamos",      MAX_CLAIMS),
                        ("mediations",            "Mediaciones",   MAX_MEDIAT),
                        ("cancellations",         "Cancelaciones", MAX_CANC),
                        ("delayed_handling_time", "Demora envío",  MAX_DELAYED),
                    ]:
                        _rep_stat_row(label, _rep_rate(metrics.get(key) or {}), maxv)

            rep_placeholder.delete()
            if ra:
                for color, msg in ra:
                    _alert_row(alerts_col, color, msg)

            n_red_total    = (sum(1 for c, _ in db_alerts if c == _RED)
                              + sum(1 for c, _ in ra if c == _RED))
            n_yellow_total = (sum(1 for c, _ in db_alerts if c == _YELLOW)
                              + sum(1 for c, _ in ra if c == _YELLOW))
            red_count_lbl.set_text(str(n_red_total))
            yel_count_lbl.set_text(str(n_yellow_total))

            if not db_alerts and not ra:
                _alert_row(alerts_col, _GREEN, "Todo en orden — sin alertas activas")

        except Exception as exc:
            rep_card.clear()
            with rep_card:
                _card_header("Estadísticas ML", "#6b7280")
                ui.label(f"Error: {exc}").classes("text-xs text-gray-400")
            rep_placeholder.delete()
            if not db_alerts:
                _alert_row(alerts_col, _GREEN, "Todo en orden — sin alertas activas")

    async def _cargar_cuotas() -> None:
        try:
            data  = await run.io_bound(ml_get_my_items, access_token, False)
            items = (data or {}).get("results", [])

            groups: Dict[Any, List[Dict]] = {}
            for it in items:
                sku = (it.get("seller_sku") or "").strip()
                if sku:
                    key: Any = ("sku", sku)
                else:
                    cpid = (it.get("catalog_product_id") or "").strip()
                    key  = ("catalog", cpid) if cpid else ("id", str(it.get("id") or ""))
                groups.setdefault(key, []).append(it)

            total = len(groups) or 1
            n_x3 = n_x6 = n_x9 = n_x12 = n_promo = 0
            for grp in groups.values():
                tipos = {_cuotas_desde_item(it) for it in grp}
                if "x3"  in tipos: n_x3  += 1
                if "x6"  in tipos: n_x6  += 1
                if "x9"  in tipos: n_x9  += 1
                if "x12" in tipos: n_x12 += 1
                if any(len(it.get("promotions") or []) > 0 for it in grp):
                    n_promo += 1

            cuotas_card.clear()
            with cuotas_card:
                _card_header("Cuotas y promos", _BLUE)
                ui.label(f"Publicaciones activas: {total}").classes("text-xs text-gray-500 mb-2")
                with ui.column().classes("w-full gap-3"):
                    _progress_bar("3 cuotas",       n_x3   / total * 100, n_x3,   total)
                    _progress_bar("6 cuotas",       n_x6   / total * 100, n_x6,   total)
                    _progress_bar("9 cuotas",       n_x9   / total * 100, n_x9,   total)
                    _progress_bar("12 cuotas",      n_x12  / total * 100, n_x12,  total)
                    _progress_bar("Con promo activa", n_promo / total * 100, n_promo, total)

        except Exception as exc:
            cuotas_card.clear()
            with cuotas_card:
                _card_header("Cuotas y promos", "#6b7280")
                ui.label(f"Error: {exc}").classes("text-xs text-gray-400")

    background_tasks.create(_cargar_rep(),    name="dashboard_rep")
    background_tasks.create(_cargar_cuotas(), name="dashboard_cuotas")
