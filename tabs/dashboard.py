"""
tabs/dashboard.py
Pestaña Dashboard: resumen ejecutivo con alertas y KPIs de ML + ARCA.
Exporta: build_tab_dashboard
"""
from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from nicegui import app, background_tasks, context, run, ui

from db import get_connection, get_cotizador_param, get_arca_datos, get_arca_multilateral, get_cache_age_minutes
from ml_api import get_ml_access_token, ml_get_user_profile, ml_get_my_items, _cuotas_desde_item, ml_get_unanswered_questions, ml_delete_question
from helpers.cache_swr import cached_or_refresh_bulk, FRESH_MIN

_GREEN  = "#2E7D32"
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


def _pr(s: Any, d: float = 0.0) -> float:
    if s is None or str(s).strip() == "":
        return d
    try:
        v = float(str(s).strip().replace(",", "."))
        return v if v <= 1.5 else v / 100.0
    except (ValueError, TypeError):
        return d

def _load_params_prod(user_id: int) -> dict:
    ev = float(str(get_cotizador_param("ml_envios", user_id) or 5823).replace(",", "."))
    if ev <= 100:
        ev = 5823.0
    do = float(str(get_cotizador_param("dolar_oficial", user_id) or "1475").replace(",", ".")) or 1475.0
    if do <= 0:
        do = 1475.0
    return {
        "ml_comision":         _pr(get_cotizador_param("ml_comision",          user_id), 0.15),
        "ml_debcre":           _pr(get_cotizador_param("ml_debcre",            user_id), 0.006),
        "ml_iibb_per":         _pr(get_cotizador_param("ml_iibb_per",          user_id), 0.055),
        "ml_envios_gratuitos": float(str(get_cotizador_param("ml_envios_gratuitos", user_id) or 33000).replace(",", ".")),
        "ml_envios_val":       ev,
        "dolar_oficial":       do,
    }

def _calc_margen_prod(precio: float, costo_usd: float, tipo_iva: float, p: dict) -> Optional[float]:
    if precio <= 0 or costo_usd <= 0:
        return None
    comision    = precio * p["ml_comision"]
    cobrado     = precio - comision
    deb_cred    = precio * p["ml_debcre"]
    iibb        = precio * p["ml_iibb_per"]
    iva_meli    = comision * 0.21 / 1.21
    iva_impor   = 0.09 * costo_usd * p["dolar_oficial"]
    iva_total   = precio * tipo_iva / (1 + tipo_iva) - iva_meli - iva_impor
    envio       = 0.0 if precio < p["ml_envios_gratuitos"] else p["ml_envios_val"]
    costo_pesos = costo_usd * p["dolar_oficial"]
    return cobrado - costo_pesos - iva_total - iibb - deb_cred - envio


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
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM productos WHERE user_id=?"
            " AND (costo_usd IS NULL OR costo_usd=0) AND stock > 0",
            (user_id,))
        sin_costo = cur.fetchone()[0]

        cur.execute(
            "SELECT COUNT(*) FROM productos WHERE user_id=? AND (fob_usd IS NULL OR fob_usd=0) AND stock > 0",
            (user_id,))
        sin_fob = cur.fetchone()[0]

        cur.execute(
            "SELECT COUNT(DISTINCT pub.ml_id) FROM ml_publicaciones pub "
            "WHERE pub.user_id=? AND LOWER(pub.estado) LIKE '%suspend%' AND pub.stock > 0",
            (user_id,))
        stock_susp = cur.fetchone()[0]

        cur.execute(
            "SELECT COUNT(*) FROM productos WHERE user_id=? AND gan_pesos < 0 AND stock > 0",
            (user_id,))
        gan_neg = cur.fetchone()[0]

        cur.execute(
            "SELECT COUNT(*) FROM productos WHERE user_id=? AND catalog_status='winning' AND stock > 0",
            (user_id,))
        cat_ganando = cur.fetchone()[0]

        cur.execute(
            "SELECT COUNT(*) FROM productos WHERE user_id=? AND catalog_status='sharing_first_place' AND stock > 0",
            (user_id,))
        cat_empatando = cur.fetchone()[0]

        cur.execute(
            "SELECT COUNT(*) FROM productos WHERE user_id=? AND catalog_status IN ('competing','listed') AND stock > 0",
            (user_id,))
        cat_perdiendo = cur.fetchone()[0]

        return {"sin_costo": sin_costo, "sin_fob": sin_fob, "stock_susp": stock_susp, "gan_neg": gan_neg,
                "cat_ganando": cat_ganando, "cat_empatando": cat_empatando, "cat_perdiendo": cat_perdiendo}
    finally:
        conn.close()


def _query_ventas(user_id: int) -> Dict[str, int]:
    desde = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM ventas_datos WHERE user_id=? AND gan_pesos IS NULL"
            " AND (pay_status IS NULL OR (pay_status NOT IN ('rejected', 'cancelled'))) AND COALESCE(order_date, fetched_at) >= ?",
            (user_id, desde))
        sin_revisar = cur.fetchone()[0]
        cur.execute(
            "SELECT COUNT(*) FROM ventas_datos WHERE user_id=? AND gan_pesos < 0 AND gan_pesos IS NOT NULL AND COALESCE(order_date, fetched_at) >= ?",
            (user_id, desde))
        gan_neg = cur.fetchone()[0]
        cur.execute(
            "SELECT COUNT(*) FROM ventas_datos WHERE user_id=? "
            "AND (pay_status IS NULL OR pay_status NOT IN ('rejected', 'cancelled')) "
            "AND COALESCE(order_date, fetched_at) >= ?",
            (user_id, desde))
        total = cur.fetchone()[0]
        return {"sin_revisar": sin_revisar, "gan_neg": gan_neg, "total": total}
    finally:
        conn.close()


def _query_arca(user_id: int) -> Dict[str, Any]:
    return {
        "siper":   get_arca_datos("siper", user_id),
        "iva":     get_arca_datos("iva",   user_id),
        "deuda":   get_arca_datos("deuda", user_id),
        "ml_rows": get_arca_multilateral(user_id),
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


def _rep_rate(m: Dict) -> Optional[float]:
    exc = (m or {}).get("excluded") or {}
    r = exc.get("real_rate")
    if isinstance(r, (int, float)): return float(r)
    r = (m or {}).get("rate")
    return float(r) if isinstance(r, (int, float)) else None


def _rep_alerts(metrics: Dict) -> List[Tuple[str, str]]:
    alerts: List[Tuple[str, str]] = []
    rc  = _rep_rate(metrics.get("claims")                or {})
    rm  = _rep_rate(metrics.get("mediations")            or {})
    rca = _rep_rate(metrics.get("cancellations")         or {})
    rd  = _rep_rate(metrics.get("delayed_handling_time") or {})
    if rc  is not None and rc  > MAX_CLAIMS:  alerts.append((_RED, f"Reclamos ML: {rc*100:.1f}% (máx {MAX_CLAIMS*100:.0f}%)"))
    if rm  is not None and rm  > MAX_MEDIAT:  alerts.append((_RED, f"Mediaciones ML: {rm*100:.1f}% (máx {MAX_MEDIAT*100:.1f}%)"))
    if rca is not None and rca > MAX_CANC:    alerts.append((_RED, f"Cancelaciones ML: {rca*100:.1f}% (máx {MAX_CANC*100:.1f}%)"))
    if rd  is not None and rd  > MAX_DELAYED: alerts.append((_RED, f"Demora envíos ML: {rd*100:.0f}% (máx {MAX_DELAYED*100:.0f}%)"))
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

def _alert_row(container, color: str, msg: str, on_nav=None):
    with container:
        with ui.row().classes("items-center gap-2 w-full px-3 py-2 rounded").style(
            f"background:{_BG.get(color, '#f9f9f9')}"):
            _dot(color)
            ui.label(msg).classes("text-sm text-gray-800 flex-1")
            if on_nav:
                (ui.element("i")
                 .classes("ti ti-arrow-right cursor-pointer flex-shrink-0")
                 .style("font-size:14px;color:#9ca3af")
                 .on("click", on_nav))

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

def _cuotas_color(pct: float) -> str:
    if pct <= 25:  return "#A32D2D"
    if pct <= 34:  return "#E24B4A"
    if pct <= 49:  return "#BA7517"
    if pct <= 75:  return "#639922"
    return "#3B6D11"

def _cuotas_row(label: str, pct: float):
    color = _cuotas_color(pct)
    with ui.row().classes("w-full items-center gap-2"):
        ui.label(label).style("width:64px;min-width:64px;font-size:12px;color:#374151")
        outer = ui.element("div").style(
            "flex:1;background:#e5e7eb;height:8px;border-radius:4px;overflow:hidden")
        with outer:
            ui.element("div").style(
                f"width:{min(max(pct,0),100):.1f}%;height:100%;"
                f"background:{color};border-radius:4px;transition:width 0.3s")
        ui.label(f"{pct:.0f}%").style(
            f"min-width:32px;text-align:right;font-size:12px;font-weight:600;color:{color}")

def _stat_row(label: str, value: str, color: str):
    with ui.row().classes("items-center gap-2 w-full"):
        _dot(color)
        ui.label(label).classes("text-xs flex-1").style("color:#374151")
        ui.label(value).classes("text-xs font-semibold").style("color:#1a1a1a")

def _rep_stat_row(label: str, rate: Optional[float], maxv: float):
    if rate is None:
        _stat_row(f"{label} (máx {maxv*100:.1f}%)", "—", _GREEN)
        return
    c = _RED if rate > maxv else (_YELLOW if rate > maxv * 0.7 else _GREEN)
    _stat_row(f"{label} (máx {maxv*100:.1f}%)", f"{rate*100:.2f}%", c)


def _stat_row_popup(label: str, value: str, color: str, on_click) -> None:
    with ui.row().classes("items-center gap-2 w-full"):
        _dot(color)
        ui.label(label).classes("text-xs flex-1").style("color:#374151")
        ui.label(value).classes("text-xs font-semibold cursor-pointer hover:underline").style(
            "color:#1a1a1a").on("click", lambda: on_click())


def _open_questions_popup(q_list: list, access_token: str, on_deleted) -> None:
    """Popup de preguntas sin responder con botón de eliminar por fila."""
    dlg = ui.dialog()
    content_ref = [None]

    def _build_rows() -> None:
        c = content_ref[0]
        c.clear()
        with c:
            if not q_list:
                ui.label("Sin preguntas sin responder").classes("text-sm text-gray-400 p-2")
                return
            with ui.scroll_area().style("max-height:400px"):
                with ui.element("table").style("width:100%;border-collapse:collapse;font-size:11px"):
                    with ui.element("thead"):
                        with ui.element("tr"):
                            for hdr in ["Fecha", "Item", "Pregunta", ""]:
                                with ui.element("th").style(
                                        "padding:4px 8px;background:#1976d2;color:white;"
                                        "text-align:left;font-weight:600"):
                                    ui.label(hdr)
                    with ui.element("tbody"):
                        for i, q in enumerate(q_list):
                            bg = "#f9fafb" if i % 2 == 0 else "#ffffff"
                            with ui.element("tr").style(f"background:{bg}"):
                                for val in [
                                    (q.get("date_created") or "")[:10],
                                    str(q.get("item_id") or "—"),
                                    (q.get("text") or "—")[:100],
                                ]:
                                    with ui.element("td").style(
                                            "padding:3px 8px;border-bottom:1px solid #e5e7eb"):
                                        ui.label(val)
                                with ui.element("td").style(
                                        "padding:3px 8px;border-bottom:1px solid #e5e7eb;width:32px"):
                                    def _try_delete(q=q) -> None:
                                        conf = ui.dialog()
                                        with conf:
                                            with ui.card().classes("p-4"):
                                                ui.label("¿Eliminar esta pregunta?").classes("text-sm font-semibold mb-2")
                                                ui.label((q.get("text") or "")[:80]).classes("text-xs text-gray-500 mb-3")
                                                with ui.row().classes("gap-2 justify-end"):
                                                    ui.button("Cancelar", on_click=conf.close).props("flat dense")
                                                    async def _do_delete(q=q, conf=conf) -> None:
                                                        ok = await run.io_bound(
                                                            ml_delete_question, access_token, q["id"])
                                                        conf.close()
                                                        if ok:
                                                            if q in q_list:
                                                                q_list.remove(q)
                                                            _build_rows()
                                                            on_deleted(len(q_list))
                                                        else:
                                                            ui.notify("Error al eliminar la pregunta", color="negative")
                                                    ui.button("Eliminar", on_click=_do_delete, color="negative").props("dense")
                                        conf.open()
                                    ui.html(
                                        '<i class="ti ti-trash" style="font-size:14px;color:#dc2626;cursor:pointer" aria-hidden="true"></i>'
                                    ).on("click", _try_delete)

    with dlg:
        with ui.card().classes("p-4 min-w-[560px] max-w-[800px]"):
            with ui.row().classes("w-full justify-between items-center mb-3"):
                ui.label("Preguntas sin responder").classes("text-base font-semibold")
                ui.button("✕", on_click=dlg.close).props("flat dense")
            content_ref[0] = ui.column().classes("w-full gap-0")
            _build_rows()
    dlg.open()


def _open_popup_list(title: str, rows: list, col_defs: list) -> None:
    dlg = ui.dialog()
    with dlg:
        with ui.card().classes("p-4 min-w-[500px] max-w-[700px]"):
            with ui.row().classes("w-full justify-between items-center mb-3"):
                ui.label(title).classes("text-base font-semibold")
                ui.button("✕", on_click=dlg.close).props("flat dense")
            if not rows:
                ui.label("Sin datos").classes("text-sm text-gray-400")
            else:
                with ui.scroll_area().style("max-height:400px"):
                    with ui.element("table").style(
                            "width:100%;border-collapse:collapse;font-size:11px"):
                        with ui.element("thead"):
                            with ui.element("tr"):
                                for hdr, _ in col_defs:
                                    with ui.element("th").style(
                                            "padding:4px 8px;background:#1976d2;color:white;"
                                            "text-align:left;font-weight:600"):
                                        ui.label(hdr)
                        with ui.element("tbody"):
                            for i, row in enumerate(rows):
                                bg = "#f9fafb" if i % 2 == 0 else "#ffffff"
                                with ui.element("tr").style(f"background:{bg}"):
                                    for _, fn in col_defs:
                                        with ui.element("td").style(
                                                "padding:3px 8px;border-bottom:1px solid #e5e7eb"):
                                            ui.label(fn(row))
    dlg.open()


# ── Detail queries ─────────────────────────────────────────────────────────────

def _detail_sin_costo(user_id: int) -> List[Dict]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT sku, marca, nombre FROM productos"
            " WHERE user_id=? AND (costo_usd IS NULL OR costo_usd=0) AND stock > 0"
            " ORDER BY sku",
            (user_id,))
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _detail_sin_fob(user_id: int) -> List[Dict]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT sku, marca, nombre FROM productos"
            " WHERE user_id=? AND (fob_usd IS NULL OR fob_usd=0) AND stock > 0"
            " ORDER BY sku",
            (user_id,))
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _detail_cat_status(user_id: int, statuses: List[str]) -> List[Dict]:
    conn = get_connection()
    try:
        ph = ",".join("?" * len(statuses))
        cur = conn.cursor()
        cur.execute(
            f"SELECT sku, marca, nombre FROM productos"
            f" WHERE user_id=? AND catalog_status IN ({ph}) AND stock > 0 ORDER BY sku",
            [user_id] + statuses)
        return [{"sku": r[0], "marca": r[1], "nombre": r[2]} for r in cur.fetchall()]
    finally:
        conn.close()


def _detail_gan_neg_prod(user_id: int) -> List[Dict]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT sku, marca, nombre, gan_pesos FROM productos"
            " WHERE user_id=? AND gan_pesos < 0 AND stock > 0"
            " ORDER BY gan_pesos ASC",
            (user_id,))
        return [{"sku": r[0], "marca": r[1], "nombre": r[2], "gan": round(r[3], 2)}
                for r in cur.fetchall()]
    finally:
        conn.close()


def _detail_sin_revisar(user_id: int, desde: str) -> List[Dict]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT order_id, payment_id, fetched_at"
            " FROM ventas_datos"
            " WHERE user_id=? AND gan_pesos IS NULL"
            " AND (pay_status IS NULL OR (pay_status NOT IN ('rejected', 'cancelled'))) AND COALESCE(order_date, fetched_at) >= ?"
            " ORDER BY COALESCE(order_date, fetched_at) DESC",
            (user_id, desde))
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _detail_gan_neg_ventas(user_id: int, desde: str) -> List[Dict]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT order_id, payment_id, fetched_at, gan_pesos"
            " FROM ventas_datos"
            " WHERE user_id=? AND gan_pesos < 0 AND COALESCE(order_date, fetched_at) >= ?"
            " ORDER BY gan_pesos",
            (user_id, desde))
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


# ── Función exportada ─────────────────────────────────────────────────────────

def build_tab_dashboard(container, navigate_to=None) -> None:
    user = _require_login()
    if not user:
        return
    uid = user["id"]

    prod         = _query_productos(uid)
    ventas       = _query_ventas(uid)
    arca_data    = _query_arca(uid)
    arca_al      = _arca_alerts(arca_data)
    access_token = get_ml_access_token(uid)
    desde_dt     = datetime.now() - timedelta(days=30)
    desde_fmt    = desde_dt.strftime("%d/%m/%Y")

    # Alertas de DB (sin reputación todavía)
    db_alerts: List[Tuple[str, str, str]] = []
    if prod["sin_costo"]     > 0: db_alerts.append((_RED,    f"Productos sin costo u$: {prod['sin_costo']}",                     "Productos"))
    if prod["stock_susp"]    > 0: db_alerts.append((_RED,    f"Publicaciones pausadas: {prod['stock_susp']}",                    "Productos"))
    if ventas["gan_neg"]     > 0: db_alerts.append((_RED,    f"Ventas a pérdida (últimos 30 días): {ventas['gan_neg']}",         "Ventas"))
    if prod["gan_neg"]       > 0: db_alerts.append((_RED,    f"Publicaciones con ganancia negativa estimada: {prod['gan_neg']}", "Productos"))
    if prod["sin_fob"]       > 0: db_alerts.append((_YELLOW, f"Productos sin FOB u$: {prod['sin_fob']}",                        "Productos"))
    if ventas["sin_revisar"] > 0: db_alerts.append((_YELLOW, f"Ventas sin revisar (últimos 30 días): {ventas['sin_revisar']}",   "Ventas"))
    for ac, am in arca_al:
        if ac != _GREEN:
            db_alerts.append((ac, am, "ARCA"))
    db_alerts.sort(key=lambda x: {_RED: 0, _YELLOW: 1}.get(x[0], 2))

    n_red_init    = sum(1 for item in db_alerts if item[0] == _RED)
    n_yellow_init = sum(1 for item in db_alerts if item[0] == _YELLOW)
    dyn_ref = {"red": 0, "yellow": 0}
    _susp_items_ref: Dict[str, Any] = {"val": []}
    desde_sql = desde_dt.strftime("%Y-%m-%d")

    is_mobile_ref: Dict[str, bool] = {"val": False}
    # pre-declare para nonlocal en _render_cards
    prod_color      = _GREEN
    prod_header_row = None
    _susp_dot       = None
    _susp_lbl       = None
    cuotas_card     = None
    rep_card        = None
    ml_pubs_card    = None
    # cliente capturado sincrónicamente al crear cuotas_card, para poder re-entrar
    # de forma segura ("with cuotas_client:") desde el timer de polling de refresh
    # y desde _cargar_cuotas (background_tasks.create) — mismo patrón que el fix de
    # slot-stack-vacío de Productos (commits 319ea8b/c20fcde).
    cuotas_client   = None

    with container:
        with ui.column().classes("w-full gap-4 p-4").style("max-width:1200px"):

            # ── BARRA DE RESUMEN ──────────────────────────────────────────
            expanded_ref = {"val": False}
            with ui.card().classes("w-full p-3 bg-grey-2").style("border:1px solid #e0e0e0"):
                with ui.row().classes("w-full items-center gap-4"):
                    with ui.row().classes("items-center gap-1"):
                        ui.icon("error", size="xs").style(f"color:{_RED}")
                        red_count_lbl = ui.label(str(n_red_init)).classes("font-bold text-sm").style("color:#1a1a1a")
                        ui.label("urgente(s)").classes("text-sm text-gray-600")
                    with ui.row().classes("items-center gap-1"):
                        ui.icon("warning", size="xs").style(f"color:{_YELLOW}")
                        yel_count_lbl = ui.label(str(n_yellow_init)).classes("font-bold text-sm").style("color:#1a1a1a")
                        ui.label("importante(s)").classes("text-sm text-gray-600")
                    ui.element("div").classes("flex-1")
                    ui.button("Actualizar", icon="refresh",
                              on_click=lambda: (container.clear(), build_tab_dashboard(container, navigate_to))
                              ).props("flat dense")
                    arrow_btn = ui.button(icon="expand_more").props("flat dense round size=sm")

                detail_panel = ui.column().classes("w-full mt-2").style("display:none")
                with detail_panel:
                    alerts_col = ui.grid(columns=3)
                    for color, msg, tab in db_alerts:
                        _alert_row(alerts_col, color, msg,
                                   on_nav=(lambda t=tab: navigate_to(t)) if navigate_to else None)
                    rep_placeholder = ui.row()
                    with rep_placeholder:
                        ui.spinner(size="xs")

                def _toggle_alerts():
                    expanded_ref["val"] = not expanded_ref["val"]
                    if expanded_ref["val"]:
                        detail_panel.style("display:block")
                        arrow_btn.props("icon=expand_less")
                    else:
                        detail_panel.style("display:none")
                        arrow_btn.props("icon=expand_more")
                arrow_btn.on_click(_toggle_alerts)

            # ── GRILLA PRINCIPAL: responsive (3 col desktop / 2 col mobile) ─────
            # Fila 1: Productos | Ventas | Cuotas
            # Fila 2: Estadísticas ML | Publicaciones ML | ARCA
            cards_area = ui.column().classes("w-full")

            async def _detect_mobile() -> None:
                w = await ui.run_javascript("window.innerWidth")
                is_mobile_ref["val"] = int(w or 9999) < 768
                _render_cards()

            ui.timer(0, _detect_mobile, once=True)

            def _render_cards() -> None:
                nonlocal prod_color, prod_header_row, _susp_dot, _susp_lbl
                nonlocal cuotas_card, rep_card, ml_pubs_card, cuotas_client
                cols = 2 if is_mobile_ref["val"] else 3
                gap  = "gap-2" if is_mobile_ref["val"] else "gap-4"
                cards_area.clear()
                with cards_area:
                    with ui.grid(columns=cols).classes(f"w-full {gap}"):

                        # --- Fila 1, Col 1: Productos ---
                        prod_color = (_RED    if prod["sin_costo"]  > 0 or prod["stock_susp"] > 0 or prod["gan_neg"] > 0
                                      else _YELLOW if prod["sin_fob"]   > 0
                                      else _GREEN)
                        with ui.card().classes("w-full").style("border:1px solid #e0e0e0;padding:10px"):
                            prod_header_row = ui.row().classes("items-center gap-2 w-full mb-2")
                            with prod_header_row:
                                ui.spinner(size="sm")
                                ui.label("Productos").classes("font-bold text-base text-gray-800")
                            with ui.column().classes("w-full gap-2"):
                                _stat_row_popup(
                                    "Sin costo u$", str(prod["sin_costo"]),
                                    _RED if prod["sin_costo"] > 0 else _GREEN,
                                    lambda: _open_popup_list(
                                        "Sin costo u$", _detail_sin_costo(uid),
                                        [("SKU",      lambda r: r.get("sku")    or "—"),
                                         ("Marca",    lambda r: r.get("marca")  or "—"),
                                         ("Producto", lambda r: r.get("nombre") or "—")]))
                                with ui.row().classes("items-center gap-2 w-full"):
                                    _susp_dot = ui.element("span").style(
                                        "display:inline-block;width:10px;height:10px;border-radius:9999px;"
                                        "background:#9ca3af;flex-shrink:0")
                                    ui.label("Pausadas con stock").classes("text-xs text-gray-700 flex-1")
                                    _susp_lbl = (ui.label("...").classes(
                                        "text-xs font-semibold cursor-pointer hover:underline")
                                        .style("color:#9ca3af"))
                                    _susp_lbl.on("click", lambda: _open_popup_list(
                                        "Pausadas con stock",
                                        _susp_items_ref["val"],
                                        [("SKU",    lambda r: r.get("seller_sku") or "—"),
                                         ("ID ML",  lambda r: str(r.get("id")     or "—")),
                                         ("Estado", lambda r: str(r.get("status") or "—"))]))
                                _stat_row_popup(
                                    "Sin FOB u$", str(prod["sin_fob"]),
                                    _YELLOW if prod["sin_fob"] > 0 else _GREEN,
                                    lambda: _open_popup_list(
                                        "Sin FOB u$", _detail_sin_fob(uid),
                                        [("SKU",      lambda r: r.get("sku")    or "—"),
                                         ("Marca",    lambda r: r.get("marca")  or "—"),
                                         ("Producto", lambda r: r.get("nombre") or "—")]))
                                _stat_row_popup(
                                    "A pérdida", str(prod["gan_neg"]),
                                    _RED if prod["gan_neg"] > 0 else _GREEN,
                                    lambda: _open_popup_list(
                                        "A pérdida (Productos)", _detail_gan_neg_prod(uid),
                                        [("SKU",      lambda r: r.get("sku")    or "—"),
                                         ("Producto", lambda r: r.get("nombre") or "—"),
                                         ("Gan$",     lambda r: f"${r['gan']:,.0f}" if r.get("gan") is not None else "—")]))
                                _stat_row_popup(
                                    "Ganando", str(prod["cat_ganando"]),
                                    _GREEN,
                                    lambda: _open_popup_list(
                                        "Ganando en catálogo", _detail_cat_status(uid, ["winning"]),
                                        [("SKU",      lambda r: r.get("sku")    or "—"),
                                         ("Marca",    lambda r: r.get("marca")  or "—"),
                                         ("Producto", lambda r: r.get("nombre") or "—")]))
                                _stat_row_popup(
                                    "Empatando", str(prod["cat_empatando"]),
                                    _YELLOW if prod["cat_empatando"] > 0 else _GREEN,
                                    lambda: _open_popup_list(
                                        "Empatando en catálogo", _detail_cat_status(uid, ["sharing_first_place"]),
                                        [("SKU",      lambda r: r.get("sku")    or "—"),
                                         ("Marca",    lambda r: r.get("marca")  or "—"),
                                         ("Producto", lambda r: r.get("nombre") or "—")]))
                                _stat_row_popup(
                                    "Perdiendo", str(prod["cat_perdiendo"]),
                                    _RED if prod["cat_perdiendo"] > 0 else _GREEN,
                                    lambda: _open_popup_list(
                                        "Perdiendo en catálogo", _detail_cat_status(uid, ["competing", "listed"]),
                                        [("SKU",      lambda r: r.get("sku")    or "—"),
                                         ("Marca",    lambda r: r.get("marca")  or "—"),
                                         ("Producto", lambda r: r.get("nombre") or "—")]))

                        # --- Fila 1, Col 2: Ventas ---
                        ven_color = (_RED    if ventas["gan_neg"]     > 0
                                     else _YELLOW if ventas["sin_revisar"] > 0
                                     else _GREEN)
                        with ui.card().classes("w-full").style("border:1px solid #e0e0e0;padding:10px"):
                            _card_header("Ventas — últimos 30 días", ven_color)
                            with ui.column().classes("w-full gap-2"):
                                _vt = ventas.get("total", 0)
                                _pct_neg = f" ({ventas['gan_neg']/_vt*100:.0f}%)" if _vt else ""
                                _pct_sin = f" ({ventas['sin_revisar']/_vt*100:.0f}%)" if _vt else ""
                                _stat_row_popup(
                                    "A pérdida", f"{ventas['gan_neg']} / {_vt}{_pct_neg}",
                                    _RED if ventas["gan_neg"] > 0 else _GREEN,
                                    lambda: _open_popup_list(
                                        "A pérdida — Ventas",
                                        _detail_gan_neg_ventas(uid, desde_sql),
                                        [("Orden",  lambda r: str(r.get("order_id")   or "—")),
                                         ("Pago",   lambda r: str(r.get("payment_id") or "—")),
                                         ("Fecha",  lambda r: (r.get("fetched_at") or "")[:10] or "—"),
                                         ("Gan$",   lambda r: f"${r['gan_pesos']:,.0f}" if r.get("gan_pesos") is not None else "—")]))
                                _stat_row_popup(
                                    "Sin revisar", f"{ventas['sin_revisar']} / {_vt}{_pct_sin}",
                                    _YELLOW if ventas["sin_revisar"] > 0 else _GREEN,
                                    lambda: _open_popup_list(
                                        "Ventas sin revisar",
                                        _detail_sin_revisar(uid, desde_sql),
                                        [("Orden",  lambda r: str(r.get("order_id")   or "—")),
                                         ("Pago",   lambda r: str(r.get("payment_id") or "—")),
                                         ("Fecha",  lambda r: (r.get("fetched_at") or "")[:10] or "—")]))
                            ui.label(f"Desde el {desde_fmt}").classes("text-xs text-gray-400 mt-2")

                        # --- Fila 1, Col 3: Cuotas (placeholder async) ---
                        cuotas_card = ui.card().classes("w-full").style("border:1px solid #e0e0e0;padding:10px")
                        cuotas_client = context.client
                        with cuotas_card:
                            with ui.row().classes("items-center gap-2 mb-2"):
                                ui.spinner(size="sm")
                                ui.label("Cuotas").classes("font-bold text-base text-gray-800")
                            ui.label("Cargando datos de cuotas...").classes("text-xs text-gray-400")

                        # --- Fila 2, Col 1: Estadísticas ML (placeholder async) ---
                        rep_card = ui.card().classes("w-full").style("border:1px solid #e0e0e0;padding:10px")
                        with rep_card:
                            with ui.row().classes("items-center gap-2 mb-2"):
                                ui.spinner(size="sm")
                                ui.label("Estadísticas ML").classes("font-bold text-base text-gray-800")
                            ui.label("Cargando reputación...").classes("text-xs text-gray-400")

                        # --- Fila 2, Col 2: Publicaciones ML (placeholder async) ---
                        ml_pubs_card = ui.card().classes("w-full").style("border:1px solid #e0e0e0;padding:10px")
                        with ml_pubs_card:
                            with ui.row().classes("items-center gap-2 mb-2"):
                                ui.spinner(size="sm")
                                ui.label("Publicaciones ML").classes("font-bold text-base text-gray-800")
                            ui.label("Cargando estado de publicaciones...").classes("text-xs text-gray-400")

                        # --- Fila 2, Col 3: ARCA ---
                        arca_ov = _GREEN
                        for ac, _ in arca_al:
                            if ac == _RED:    arca_ov = _RED;    break
                            if ac == _YELLOW: arca_ov = _YELLOW

                        with ui.card().classes("w-full").style("border:1px solid #e0e0e0;padding:10px"):
                            _card_header("ARCA — Resumen Fiscal", arca_ov)
                            sd, id_, dd, mr = arca_data["siper"], arca_data["iva"], arca_data["deuda"], arca_data["ml_rows"]
                            with ui.grid(columns=2).classes("w-full gap-3 mt-1"):

                                siper_v = sd.get("categoria_siper") or ""
                                with ui.column().classes("gap-1"):
                                    with ui.row().classes("items-center gap-1 mb-1"):
                                        _dot(_color_siper(siper_v))
                                        ui.label("SIPER").classes("text-xs font-semibold text-gray-600")
                                    ui.label(siper_v or "Sin datos").classes("text-xs text-gray-800")

                                tec_v = id_.get("saldo_tecnico", "")
                                lib_v = id_.get("saldo_libre_disponibilidad", "")
                                with ui.column().classes("gap-1"):
                                    with ui.row().classes("items-center gap-1 mb-1"):
                                        _dot(_color_iva(tec_v, lib_v))
                                        ui.label("Saldo IVA").classes("text-xs font-semibold text-gray-600")
                                    ui.label(f"Técnico: ${_to_float(tec_v):,.0f}" if tec_v else "Sin datos").classes("text-xs text-gray-800")
                                    if lib_v:
                                        ui.label(f"Libre disp: ${_to_float(lib_v):,.0f}").classes("text-xs text-gray-500")

                                deu_v   = dd.get("deuda_exigible", "")
                                intim_v = dd.get("tiene_intimacion") == "true"
                                with ui.column().classes("gap-1"):
                                    with ui.row().classes("items-center gap-1 mb-1"):
                                        _dot(_color_deuda(deu_v, intim_v))
                                        ui.label("Deuda / Planes").classes("text-xs font-semibold text-gray-600")
                                    ui.label(f"${_to_float(deu_v):,.0f}" if deu_v else "Sin datos").classes("text-xs text-gray-800")
                                    if intim_v:
                                        ui.label("Intimación activa").classes("text-xs font-semibold").style("color:#374151")

                                mc          = _color_multilateral(mr)
                                total_pagar = sum(_to_float(r.get("a_pagar")) for r in mr)
                                with ui.column().classes("gap-1"):
                                    with ui.row().classes("items-center gap-1 mb-1"):
                                        _dot(mc)
                                        ui.label("Multilateral").classes("text-xs font-semibold text-gray-600")
                                    if mr:
                                        ui.label(f"{len(mr)} provincia(s)").classes("text-xs text-gray-800")
                                        if total_pagar > 0:
                                            ui.label(f"A pagar: ${total_pagar:,.0f}").classes("text-xs font-semibold").style("color:#374151")
                                        else:
                                            ui.label("Sin saldo a pagar").classes("text-xs text-gray-500")
                                    else:
                                        ui.label("Sin datos").classes("text-xs text-gray-400")

                if not access_token:
                    rep_card.clear()
                    with rep_card:
                        _card_header("Estadísticas ML", "#6b7280")
                        ui.label("Sin token ML configurado").classes("text-sm text-gray-400")
                    rep_placeholder.delete()
                    cuotas_card.clear()
                    with cuotas_card:
                        _card_header("Cuotas", "#6b7280")
                        ui.label("Sin token ML configurado").classes("text-sm text-gray-400")
                    ml_pubs_card.clear()
                    with ml_pubs_card:
                        _card_header("Publicaciones ML", "#6b7280")
                        ui.label("Sin token ML configurado").classes("text-sm text-gray-400")
                    _susp_lbl.set_text("—")
                    prod_header_row.clear()
                    with prod_header_row:
                        _dot(prod_color)
                        ui.label("Productos").classes("font-bold text-base text-gray-800")
                    if not db_alerts:
                        _alert_row(alerts_col, _GREEN, "Todo en orden — sin alertas activas")
                    return
                background_tasks.create(_cargar_rep(),    name="dashboard_rep")
                background_tasks.create(_cargar_cuotas(), name="dashboard_cuotas")

    # ── Async tasks ───────────────────────────────────────────────────────────

    def _bump_counters(r: int = 0, y: int = 0) -> None:
        dyn_ref["red"] += r
        dyn_ref["yellow"] += y
        red_count_lbl.set_text(str(n_red_init + dyn_ref["red"]))
        yel_count_lbl.set_text(str(n_yellow_init + dyn_ref["yellow"]))

    # _cargar_rep y _cargar_cuotas corren en paralelo y ambas necesitan el perfil ML
    # (para reputación y para seller_id respectivamente) — memoizado por carga de
    # dashboard para hacer un solo request en vez de dos. Nada que ver con el caché
    # SWR de 15/60min: esto vive solo mientras dura esta carga de la página.
    _profile_once_lock = asyncio.Lock()
    _profile_once_val: Dict[str, Any] = {}

    async def _get_profile_once() -> Dict[str, Any]:
        async with _profile_once_lock:
            if "data" not in _profile_once_val:
                _profile_once_val["data"] = await run.io_bound(ml_get_user_profile, access_token)
            return _profile_once_val["data"]

    async def _cargar_rep() -> None:
        try:
            # _get_profile_once: memoizado para esta carga del dashboard, evita pedir el
            # perfil dos veces (_cargar_rep y _cargar_cuotas corren en paralelo). [PERF]
            profile  = await _get_profile_once()
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
            _bump_counters(
                sum(1 for c, _ in ra if c == _RED),
                sum(1 for c, _ in ra if c == _YELLOW),
            )
            if ra:
                for color, msg in ra:
                    _alert_row(alerts_col, color, msg,
                               on_nav=(lambda: navigate_to("Estadísticas")) if navigate_to else None)
            if not db_alerts and not ra:
                _alert_row(alerts_col, _GREEN, "Todo en orden — sin alertas activas")

        except Exception:
            logging.exception(f"[DASHBOARD] error cargando Estadísticas ML (uid={uid})")
            rep_card.clear()
            with rep_card:
                _card_header("Estadísticas ML", "#6b7280")
                ui.label("Datos no disponibles").classes("text-xs text-gray-400")
            rep_placeholder.delete()
            if not db_alerts:
                _alert_row(alerts_col, _GREEN, "Todo en orden — sin alertas activas")

    async def _cargar_cuotas() -> None:
        try:
            access_token = get_ml_access_token(uid)
            if not access_token:
                ml_pubs_card.clear()
                with ml_pubs_card:
                    _card_header("Publicaciones ML", "#6b7280")
                    ui.label("Sin cuenta ML vinculada").classes("text-xs text-gray-400")
                cuotas_card.clear()
                with cuotas_card:
                    _card_header("Cuotas", "#6b7280")
                    ui.label("Sin cuenta ML vinculada").classes("text-xs text-gray-400")
                return

            from tabs.cuotas import _cuotas_key, _get_promo_data

            data      = await run.io_bound(ml_get_my_items, access_token, True)
            all_items = data.get("results", [])

            # ── Obtener seller_id y preguntas sin responder ───────────────────────
            seller_id = ""
            try:
                profile = await _get_profile_once()
                seller_id = str((profile or {}).get("id") or "")
            except Exception:
                logging.exception("[DASHBOARD] no se pudo obtener el perfil/seller_id ML (uid=%s)", uid)

            questions: Optional[List[Dict[str, Any]]] = None
            if seller_id:
                try:
                    questions = await run.io_bound(ml_get_unanswered_questions, access_token, seller_id)
                except Exception:
                    logging.exception(
                        "[DASHBOARD] no se pudo obtener preguntas sin responder (uid=%s, seller_id=%s)",
                        uid, seller_id,
                    )
            n_questions = len(questions) if questions is not None else None

            # ── Publicaciones ML (under_review) ──────────────────────────────
            ur_pend_doc = [it for it in all_items
                           if str(it.get("status", "")).lower() == "under_review"
                           and "pending_documentation" in (it.get("sub_status") or [])]
            ur_held     = [it for it in all_items
                           if str(it.get("status", "")).lower() == "under_review"
                           and "held" in (it.get("sub_status") or [])]
            active_count = sum(1 for it in all_items if str(it.get("status", "")).lower() == "active")

            ml_pubs_ov = (_RED if ur_pend_doc or (n_questions or 0) > 0 else _YELLOW if ur_held else _GREEN)
            _col_defs_ur = [
                ("ID ML",      lambda r: str(r.get("id") or "—")),
                ("Título",     lambda r: (r.get("title") or "—")[:45]),
                ("Estado",     lambda r: (r.get("status") or "—").replace("_", " ")),
                ("Sub-estado", lambda r: ", ".join((s or "").replace("_", " ") for s in (r.get("sub_status") or [])) or "—"),
                ("Precio",     lambda r: f"${r['price']:,.0f}" if r.get("price") else "—"),
                ("Stock",      lambda r: str(r.get("available_quantity") or 0)),
            ]
            ml_pubs_card.clear()
            with ml_pubs_card:
                _card_header("Publicaciones ML", ml_pubs_ov)
                with ui.column().classes("w-full gap-2"):
                    _stat_row_popup(
                        "Documentación pendiente", str(len(ur_pend_doc)),
                        _RED if ur_pend_doc else _GREEN,
                        lambda rows=ur_pend_doc: _open_popup_list(
                            "Documentación pendiente", rows, _col_defs_ur))
                    _stat_row_popup(
                        "Retenidas por ML", str(len(ur_held)),
                        _YELLOW if ur_held else _GREEN,
                        lambda rows=ur_held: _open_popup_list(
                            "Retenidas por ML", rows, _col_defs_ur))
                    _stat_row("Activas sin problemas", str(active_count), _GREEN)
                    q_list = list(questions or [])
                    q_row_container = ui.element("div").classes("w-full")

                    def _rebuild_q_row(n_q: Optional[int], tok: str = access_token, ql=q_list, cont=q_row_container) -> None:
                        cont.clear()
                        if n_q is None:
                            with cont:
                                with ui.row().classes("items-center gap-2 w-full"):
                                    _dot("#6b7280")
                                    ui.label("Preguntas sin responder").classes("text-xs text-gray-700 flex-1")
                                    ui.label("No disponible").classes("text-xs font-semibold").style("color:#6b7280")
                            return
                        color = _RED if n_q > 0 else _GREEN
                        with cont:
                            with ui.row().classes("items-center gap-2 w-full"):
                                _dot(color)
                                ui.label("Preguntas sin responder").classes("text-xs text-gray-700 flex-1")
                                ui.label(str(n_q)).classes(
                                    "text-xs font-semibold cursor-pointer hover:underline"
                                ).style("color:#1a1a1a").on(
                                    "click", lambda tok=tok, ql=ql, cont=cont: _open_questions_popup(
                                        ql, tok,
                                        lambda n, cont=cont, tok=tok, ql=ql: _rebuild_q_row(n, tok, ql, cont)))

                    _rebuild_q_row(n_questions)

            if ur_pend_doc:
                n = len(ur_pend_doc)
                _alert_row(alerts_col, _RED,
                           f"{n} publicación{'es' if n != 1 else ''} con documentación pendiente en ML",
                           on_nav=(lambda: navigate_to("Cuotas")) if navigate_to else None)
                _bump_counters(r=1)
            if ur_held:
                n = len(ur_held)
                _alert_row(alerts_col, _YELLOW,
                           f"{n} publicación{'es' if n != 1 else ''} retenida{'s' if n != 1 else ''} por ML")
                _bump_counters(y=1)

            # ── Pausadas con stock ────────────────────────────────────────────
            susp_items = [
                it for it in all_items
                if str(it.get("status", "")).lower() != "active"
                and (it.get("available_quantity") or 0) > 0
            ]
            cnt_susp = len(susp_items)
            _susp_items_ref["val"] = susp_items
            _c = _RED if cnt_susp > 0 else _GREEN
            _susp_dot.style(
                f"display:inline-block;width:10px;height:10px;border-radius:9999px;"
                f"background:{_c};flex-shrink:0")
            _susp_lbl.set_text(str(cnt_susp))
            _susp_lbl.style("color:#1a1a1a")
            if cnt_susp > 0:
                _alert_row(alerts_col, _RED, f"Publicaciones pausadas: {cnt_susp}",
                           on_nav=(lambda: navigate_to("Productos")) if navigate_to else None)
                _bump_counters(r=1)
            _pc = (_RED    if prod["sin_costo"] > 0 or cnt_susp > 0 or prod["gan_neg"] > 0
                   else _YELLOW if prod["sin_fob"] > 0
                   else _GREEN)
            prod_header_row.clear()
            with prod_header_row:
                _dot(_pc)
                ui.label("Productos").classes("font-bold text-base text-gray-800")
            items = [it for it in all_items if str(it.get("status", "")).lower() == "active"]

            # Deduplicar por SKU/catálogo — igual que cuotas.py
            groups: dict = {}
            for it in items:
                groups.setdefault(_cuotas_key(it), []).append(it)

            _tot  = len(groups)
            denom = _tot or 1

            n_x3  = sum(1 for g in groups.values() if any(_cuotas_desde_item(it) == "x3"  for it in g))
            n_x6  = sum(1 for g in groups.values() if any(_cuotas_desde_item(it) == "x6"  for it in g))
            n_x9  = sum(1 for g in groups.values() if any(_cuotas_desde_item(it) == "x9"  for it in g))
            n_x12 = sum(1 for g in groups.values() if any(_cuotas_desde_item(it) == "x12" for it in g))

            # Rep IDs por grupo — misma lógica que tab Cuotas
            rep_ids: list = []
            for g in groups.values():
                rid = ""
                for it in g:
                    if not it.get("catalog_listing") and str(it.get("listing_type_id") or "").lower() == "gold_special":
                        rid = str(it.get("id") or "")
                        break
                if not rid:
                    for it in g:
                        if it.get("catalog_listing"):
                            rid = str(it.get("id") or "")
                            break
                if not rid:
                    for it in g:
                        rid = str(it.get("id") or "")
                        if rid:
                            break
                if rid:
                    rep_ids.append(rid)

            # ── Cuotas/Promos: bloque más lento del dashboard (hasta 3 requests HTTP por
            # publicación, medido en catálogos grandes: 66s en frío y secuencial). Se aplica
            # el mismo patrón stale-while-revalidate de tabs/precios.py (fresh 15min/stale
            # 60min, ver helpers/cache_swr.py) más paralelización de la parte que sí hace
            # falta pedir (igual que _enriquecer_items en precios.py). [PERF-DASHBOARD]
            promo_cache_prefix = f"dash_promo_{uid}"

            def _fetch_promo_batch(ids_to_fetch: List[str]) -> Dict[str, Any]:
                out: Dict[str, Any] = {}
                with ThreadPoolExecutor(max_workers=min(16, len(ids_to_fetch) or 1)) as ex:
                    futures = {ex.submit(_get_promo_data, access_token, iid, seller_id): iid
                               for iid in ids_to_fetch}
                    for fut in as_completed(futures):
                        iid = futures[fut]
                        try:
                            out[iid] = fut.result()
                        except Exception:
                            logging.exception(f"[DASHBOARD] error _get_promo_data iid={iid} uid={uid}")
                return out

            def _promo_age_minutes() -> float:
                ages = [get_cache_age_minutes(f"{promo_cache_prefix}_{iid}") for iid in rep_ids]
                ages = [a for a in ages if a is not None]
                return max(ages) if ages else 0.0

            def _fmt_age(mins: float) -> str:
                if mins < 1:    return "hace instantes"
                if mins < 60:   return f"hace {mins:.0f} min"
                return f"hace {mins / 60:.1f} h"

            def _render_cuotas_card(data: dict, age_min: float, refreshing: bool) -> None:
                n_promos_l = sum(1 for iid in rep_ids if (data.get(iid) or {}).get("price_promo") is not None)
                cuotas_card.clear()
                with cuotas_card:
                    _card_header(f"Cuotas — {_tot} publicaciones", _BLUE)
                    with ui.column().classes("w-full gap-2"):
                        _cuotas_row("Promos",    n_promos_l / denom * 100)
                        ui.separator().style("margin:2px 0")
                        _cuotas_row("3 cuotas",  n_x3  / denom * 100)
                        _cuotas_row("6 cuotas",  n_x6  / denom * 100)
                        _cuotas_row("9 cuotas",  n_x9  / denom * 100)
                        _cuotas_row("12 cuotas", n_x12 / denom * 100)
                    with ui.row().classes("w-full gap-3 mt-2 flex-wrap"):
                        for col, rng in [("#A32D2D", "0–25%"),   ("#E24B4A", "26–34%"),
                                         ("#BA7517", "35–49%"),  ("#639922", "50–75%"),
                                         ("#3B6D11", "76–100%")]:
                            with ui.row().classes("items-center gap-1"):
                                ui.element("span").style(
                                    f"display:inline-block;width:10px;height:10px;"
                                    f"border-radius:2px;background:{col};flex-shrink:0")
                                ui.label(rng).style("font-size:12px;color:#6b7280")
                    with ui.row().classes("items-center gap-1 mt-2"):
                        if refreshing:
                            ui.spinner(size="xs")
                            ui.label(f"Actualizado {_fmt_age(age_min)} · actualizando…").classes(
                                "text-xs").style("color:#9ca3af")
                        else:
                            ui.label(f"Actualizado {_fmt_age(age_min)}").classes(
                                "text-xs").style("color:#9ca3af")

            promo_data: dict = await run.io_bound(
                cached_or_refresh_bulk, promo_cache_prefix, rep_ids, _fetch_promo_batch)
            oldest_age = _promo_age_minutes()
            is_stale = bool(rep_ids) and oldest_age > FRESH_MIN

            with cuotas_client:
                _render_cuotas_card(promo_data, oldest_age, refreshing=is_stale)

            if is_stale:
                # datos ya visibles (stale) mientras _cached_or_refresh_bulk refresca en un
                # thread de fondo; este timer sondea hasta que el dato quede fresco (o hasta
                # un techo razonable) y vuelve a pintar la card avisando que se actualizó.
                POLL_INTERVAL = 5.0
                MAX_POLL_ATTEMPTS = 24  # ~120s, cubre holgado el peor caso medido (catálogos grandes en frío)
                poll_state: Dict[str, Any] = {"attempts": 0, "timer": None}

                async def _poll_promo_refresh() -> None:
                    poll_state["attempts"] += 1
                    age = _promo_age_minutes()
                    if age <= FRESH_MIN:
                        try:
                            fresh_data = await run.io_bound(
                                cached_or_refresh_bulk, promo_cache_prefix, rep_ids, _fetch_promo_batch)
                        except Exception:
                            logging.exception(
                                f"[DASHBOARD] error re-leyendo caché de Cuotas/Promos (uid={uid})")
                            return
                        with cuotas_client:
                            _render_cuotas_card(fresh_data, _promo_age_minutes(), refreshing=False)
                        if poll_state["timer"] is not None:
                            poll_state["timer"].cancel()
                        return
                    if poll_state["attempts"] >= MAX_POLL_ATTEMPTS:
                        logging.warning(
                            f"[DASHBOARD] refresh de Cuotas/Promos no terminó tras "
                            f"{MAX_POLL_ATTEMPTS * POLL_INTERVAL:.0f}s (uid={uid}, "
                            f"n_items={len(rep_ids)}) — se sigue mostrando el dato con "
                            f"{age:.0f} min de antigüedad")
                        with cuotas_client:
                            ui.notify(
                                "No se pudo actualizar Cuotas/Promos, mostrando el último dato disponible",
                                color="warning")
                        if poll_state["timer"] is not None:
                            poll_state["timer"].cancel()

                with cuotas_client:
                    with cuotas_card:
                        poll_state["timer"] = ui.timer(POLL_INTERVAL, _poll_promo_refresh)

        except Exception:
            logging.exception(f"[DASHBOARD] error cargando Cuotas/Publicaciones ML (uid={uid})")
            _susp_lbl.set_text("—")
            prod_header_row.clear()
            with prod_header_row:
                _dot(prod_color)
                ui.label("Productos").classes("font-bold text-base text-gray-800")
            ml_pubs_card.clear()
            with ml_pubs_card:
                _card_header("Publicaciones ML", "#6b7280")
                ui.label("Datos no disponibles").classes("text-xs text-gray-400")
            cuotas_card.clear()
            with cuotas_card:
                _card_header("Cuotas", "#6b7280")
                ui.label("Datos no disponibles").classes("text-xs text-gray-400")
            with cuotas_client:
                ui.notify("No se pudieron cargar los datos de Cuotas/Publicaciones ML", color="negative")

