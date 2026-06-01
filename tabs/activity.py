"""tabs/activity.py — Registro de actividad de usuarios (solo admin)."""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

from nicegui import ui

from db import get_connection


# ---------------------------------------------------------------------------
# Data access
# ---------------------------------------------------------------------------

def _query_logs(desde: str, hasta: str, tab_f: str = "", accion_f: str = "") -> List[Dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        q = "SELECT * FROM activity_log WHERE timestamp >= ? AND timestamp <= ?"
        p: List[Any] = [desde + " 00:00:00", hasta + " 23:59:59"]
        if tab_f and tab_f != "todas":
            q += " AND tab = ?"; p.append(tab_f)
        if accion_f and accion_f != "todas":
            q += " AND accion = ?"; p.append(accion_f)
        q += " ORDER BY timestamp DESC"
        cur.execute(q, p)
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _distinct_tabs() -> List[str]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT tab FROM activity_log ORDER BY tab")
        return [r["tab"] for r in cur.fetchall()]
    finally:
        conn.close()


def _all_system_users() -> List[str]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT username FROM users ORDER BY username")
        return [r["username"] for r in cur.fetchall()]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BADGE_COLOR: Dict[str, str] = {
    "page_view":  "#185FA5",
    "time_spent": "#9ca3af",
    "guardar":    "#3B6D11",
    "imprimir":   "#BA7517",
    "actualizar": "#6b7280",
}


def _initials(name: str) -> str:
    base = (name or "?").split("@")[0]
    parts = base.replace(".", " ").split()[:2]
    return "".join(p[0].upper() for p in parts if p)[:2] or "?"


def _fmt_secs(secs: Any) -> str:
    if not secs:
        return "—"
    s = int(secs)
    if s < 60:
        return f"{s}s"
    m, r = divmod(s, 60)
    return f"{m}m {r}s" if r else f"{m}m"


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

def build_tab_actividad(container) -> None:
    container.clear()
    hoy   = datetime.now().date()
    hace7 = hoy - timedelta(days=7)
    all_users = _all_system_users()

    with container:
        ui.label("Actividad de usuarios").classes("text-xl font-bold mb-3")

        # ── A) FILTER BAR ─────────────────────────────────────────────────
        with ui.card().classes("w-full p-3 bg-grey-2 mb-4").style("border:1px solid #e0e0e0"):
            with ui.row().classes("items-end gap-4 flex-wrap"):
                desde_i  = ui.input("Desde", value=hace7.strftime("%Y-%m-%d")).props("type=date dense")
                hasta_i  = ui.input("Hasta",  value=hoy.strftime("%Y-%m-%d")).props("type=date dense")
                tab_opts = ["todas"] + _distinct_tabs()
                tab_s    = ui.select(tab_opts, value="todas", label="Sección").props("dense outlined").classes("w-44")
                accion_s = ui.select(
                    ["todas", "page_view", "imprimir", "guardar", "actualizar"],
                    value="todas", label="Acción",
                ).props("dense outlined").classes("w-40")

                def _refresh() -> None:
                    _render(
                        desde_i.value  or hace7.strftime("%Y-%m-%d"),
                        hasta_i.value  or hoy.strftime("%Y-%m-%d"),
                        tab_s.value    or "todas",
                        accion_s.value or "todas",
                    )

                ui.button("Aplicar", icon="filter_list", on_click=_refresh).props("dense")

        main_area = ui.column().classes("w-full gap-4")

        def _render(desde: str, hasta: str, tab_f: str, accion_f: str) -> None:
            main_area.clear()
            rows      = _query_logs(desde, hasta, tab_f, accion_f)
            view_rows = [r for r in rows if r["accion"] == "page_view"]
            act_rows  = [r for r in rows if r["accion"] not in ("page_view", "time_spent")]
            disp_rows = [r for r in rows if r["accion"] != "time_spent"]

            with main_area:
                # ── B) METRICS ────────────────────────────────────────────
                tabs_cnt: Dict[str, int] = defaultdict(int)
                for r in view_rows:
                    tabs_cnt[r["tab"]] += 1
                top_tab = max(tabs_cnt, key=tabs_cnt.get) if tabs_cnt else "—"

                with ui.grid(columns=4).classes("w-full gap-3"):
                    for lbl, val, ico, clr in [
                        ("Usuarios activos",   str(len({r["usuario"] for r in rows})), "people",     "#185FA5"),
                        ("Páginas vistas",      str(len(view_rows)),                   "visibility", "#3B6D11"),
                        ("Acciones realizadas", str(len(act_rows)),                    "touch_app",  "#BA7517"),
                        ("Tab más visitada",    top_tab,                               "star",       "#6b7280"),
                    ]:
                        with ui.card().classes("p-4").style("border:1px solid #e0e0e0"):
                            with ui.row().classes("items-center gap-2 mb-1"):
                                ui.icon(ico, size="sm").style(f"color:{clr}")
                                ui.label(lbl).classes("text-xs text-gray-500 uppercase tracking-wide")
                            ui.label(val).classes("text-2xl font-bold").style(f"color:{clr}")

                # ── C) USER SUMMARY ───────────────────────────────────────
                udata: Dict[str, Dict] = {
                    u: {"v": 0, "a": 0, "t": 0, "tabs": defaultdict(int), "last": "", "ml": ""}
                    for u in all_users
                }
                for r in rows:
                    u = r["usuario"]
                    if u not in udata:
                        udata[u] = {"v": 0, "a": 0, "t": 0, "tabs": defaultdict(int), "last": "", "ml": ""}
                    if r.get("ml_username"):
                        udata[u]["ml"] = r["ml_username"]
                    if r["accion"] == "page_view":
                        udata[u]["v"] += 1
                        udata[u]["tabs"][r["tab"]] += 1
                    elif r["accion"] == "time_spent" and r.get("tiempo_segundos"):
                        udata[u]["t"] += int(r["tiempo_segundos"])
                    elif r["accion"] not in ("page_view", "time_spent"):
                        udata[u]["a"] += 1
                    ts = r.get("timestamp") or ""
                    if ts > udata[u]["last"]:
                        udata[u]["last"] = ts

                def _th(c: str) -> str:
                    return (
                        f'<th style="padding:8px 12px;text-align:left;font-size:11px;'
                        f'text-transform:uppercase;letter-spacing:.05em;color:#6b7280;font-weight:500">{c}</th>'
                    )

                rows_html = ""
                for u, d in sorted(udata.items()):
                    has  = d["v"] > 0 or d["a"] > 0
                    dn   = d["ml"] or u.split("@")[0]
                    sub  = f'<div style="font-size:11px;color:#9ca3af">{u}</div>' if d["ml"] else ""
                    init = _initials(u)
                    pills = " ".join(
                        f'<span style="background:#6b7280;color:white;padding:1px 7px;'
                        f'border-radius:9999px;font-size:11px">{t} ×{c}</span>'
                        for t, c in sorted(d["tabs"].items(), key=lambda x: -x[1])[:5]
                    ) if has else '<span style="color:#9ca3af;font-style:italic;font-size:12px">sin actividad</span>'
                    last_fmt = d["last"][:16].replace("T", " ") if d["last"] else "—"
                    op = "" if has else "opacity:.45"
                    rows_html += (
                        f'<tr style="border-top:1px solid #f3f4f6;{op}">'
                        f'<td style="padding:8px 12px">'
                        f'<div style="display:flex;align-items:center;gap:8px">'
                        f'<div style="width:28px;height:28px;border-radius:50%;background:#185FA5;color:white;'
                        f'display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:bold;flex-shrink:0">{init}</div>'
                        f'<div><div style="font-size:14px;font-weight:500">{dn}</div>{sub}</div>'
                        f'</div></td>'
                        f'<td style="padding:6px 12px;font-size:13px">{"—" if not has else d["v"]}</td>'
                        f'<td style="padding:6px 12px;font-size:13px">{"—" if not has else d["a"]}</td>'
                        f'<td style="padding:6px 12px;font-size:13px">{"—" if not has else _fmt_secs(d["t"])}</td>'
                        f'<td style="padding:6px 12px"><div style="display:flex;flex-wrap:wrap;gap:3px">{pills}</div></td>'
                        f'<td style="padding:6px 12px;font-size:12px;color:#6b7280">{"—" if not has else last_fmt}</td>'
                        f'</tr>'
                    )

                cols_html = "".join(
                    _th(c) for c in ["Usuario ML", "Visitas", "Acciones", "Tiempo total", "Páginas visitadas", "Última actividad"]
                )
                ui.html(
                    f'<div style="border:1px solid #e0e0e0;border-radius:8px;overflow:hidden;background:white">'
                    f'<div style="padding:12px 16px;font-weight:700;font-size:15px;border-bottom:1px solid #f3f4f6">Resumen por usuario</div>'
                    f'<div style="overflow-x:auto"><table style="width:100%;border-collapse:collapse">'
                    f'<thead><tr style="background:#f9fafb">{cols_html}</tr></thead>'
                    f'<tbody>{rows_html}</tbody>'
                    f'</table></div></div>'
                )

                # ── D) CHARTS ─────────────────────────────────────────────
                with ui.grid(columns=2).classes("w-full gap-3"):
                    # D1) Horizontal bar chart
                    with ui.card().classes("p-4").style("border:1px solid #e0e0e0"):
                        ui.label("Visitas por sección").classes("font-bold text-base mb-3")
                        if tabs_cnt:
                            mx = max(tabs_cnt.values()) or 1
                            for tn, cnt in sorted(tabs_cnt.items(), key=lambda x: -x[1])[:10]:
                                pct = cnt / mx * 100
                                with ui.row().classes("items-center gap-2 mb-1"):
                                    ui.label(tn).classes("text-xs text-gray-600 text-right").style("min-width:80px")
                                    with ui.element("div").classes("flex-1 rounded overflow-hidden").style(
                                        "background:#f3f4f6;height:18px"
                                    ):
                                        ui.element("div").classes("h-full rounded").style(
                                            f"width:{pct:.0f}%;background:#185FA5"
                                        )
                                    ui.label(str(cnt)).classes("text-xs font-bold").style("min-width:24px")
                        else:
                            ui.label("Sin datos en el período").classes("text-sm text-gray-400")

                    # D2) Heatmap hora/día
                    with ui.card().classes("p-4").style("border:1px solid #e0e0e0"):
                        ui.label("Actividad por hora y día").classes("font-bold text-base mb-3")
                        DIAS  = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]
                        SLOTS = ["08-10", "10-12", "12-14", "14-16", "16-18+"]
                        heat: Dict[Tuple[int, int], int] = defaultdict(int)
                        for r in view_rows:
                            ts = r.get("timestamp") or ""
                            try:
                                dt   = datetime.fromisoformat(ts[:19])
                                slot = min((dt.hour - 8) // 2, 4)
                                if slot >= 0:
                                    heat[(dt.weekday(), slot)] += 1
                            except (ValueError, TypeError):
                                pass
                        mxh = max(heat.values()) if heat else 1
                        hdr = '<th style="width:36px"></th>' + "".join(
                            f'<th style="text-align:center;font-size:11px;color:#9ca3af;padding:2px 4px">{s}</th>'
                            for s in SLOTS
                        )
                        body = ""
                        for di, day in enumerate(DIAS):
                            cells = f'<td style="font-size:12px;color:#6b7280;padding-right:6px;text-align:right">{day}</td>'
                            for si in range(5):
                                v = heat.get((di, si), 0)
                                if v == 0:
                                    bg, txt, tc = "#f3f4f6", "", "transparent"
                                else:
                                    intens = min(int(v / mxh * 100), 100)
                                    g_val  = max(80, 255 - int(intens * 1.3))
                                    b_val  = min(255, 200 - intens)
                                    bg     = f"rgb({g_val},{b_val},255)"
                                    txt    = str(v)
                                    tc     = "#374151"
                                cells += (
                                    f'<td style="background:{bg};color:{tc};text-align:center;'
                                    f'font-size:11px;padding:5px 2px;border-radius:3px;min-width:42px">{txt}</td>'
                                )
                            body += f"<tr>{cells}</tr>"
                        ui.html(
                            f'<table style="border-collapse:separate;border-spacing:3px;width:100%">'
                            f'<thead><tr>{hdr}</tr></thead><tbody>{body}</tbody></table>'
                        )

                # ── E) EVENTS TABLE (paginada) ────────────────────────────
                PAGE_SIZE  = 50
                total_ev   = len(disp_rows)
                total_pages = max(1, (total_ev + PAGE_SIZE - 1) // PAGE_SIZE)
                pg         = [0]
                ev_area    = ui.column().classes("w-full")

                def _ev_hdr(c: str) -> str:
                    return (
                        f'<th style="padding:8px 12px;text-align:left;font-size:11px;'
                        f'text-transform:uppercase;letter-spacing:.05em;color:#6b7280;font-weight:500">{c}</th>'
                    )

                def _render_ev(p: int) -> None:
                    ev_area.clear()
                    chunk   = disp_rows[p * PAGE_SIZE:(p + 1) * PAGE_SIZE]
                    ev_html = ""
                    for r in chunk:
                        bc   = _BADGE_COLOR.get(r["accion"], "#6b7280")
                        ts   = (r.get("timestamp") or "")[:16].replace("T", " ")
                        ml   = r.get("ml_username") or ""
                        ud   = ml or (r["usuario"] or "").split("@")[0]
                        init = _initials(r["usuario"])
                        ev_html += (
                            f'<tr style="border-top:1px solid #f3f4f6">'
                            f'<td style="padding:6px 12px">'
                            f'<div style="display:flex;align-items:center;gap:6px">'
                            f'<div style="width:24px;height:24px;border-radius:50%;background:#185FA5;color:white;'
                            f'display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:bold;flex-shrink:0">{init}</div>'
                            f'<span style="font-size:13px">{ud}</span></div></td>'
                            f'<td style="padding:6px 12px;font-size:13px;color:#374151">{r["tab"]}</td>'
                            f'<td style="padding:6px 12px"><span style="background:{bc};color:white;'
                            f'padding:2px 8px;border-radius:9999px;font-size:11px;font-weight:500">{r["accion"]}</span></td>'
                            f'<td style="padding:6px 12px;font-size:12px;color:#6b7280">{r.get("detalle") or "—"}</td>'
                            f'<td style="padding:6px 12px;font-size:12px;color:#6b7280">{_fmt_secs(r.get("tiempo_segundos"))}</td>'
                            f'<td style="padding:6px 12px;font-size:12px;color:#6b7280">{ts}</td>'
                            f'</tr>'
                        )
                    ev_cols = "".join(
                        _ev_hdr(c) for c in ["Usuario", "Sección", "Acción", "Detalle", "Tiempo en tab", "Hora"]
                    )
                    with ev_area:
                        with ui.card().classes("w-full").style("border:1px solid #e0e0e0"):
                            with ui.row().classes("items-center justify-between px-4 pt-3 pb-2"):
                                ui.label(f"Eventos ({total_ev})").classes("font-bold text-base")
                                with ui.row().classes("items-center gap-2"):
                                    ui.label(f"Pág {p + 1} / {total_pages}").classes("text-sm text-gray-500")
                                    pb = ui.button(icon="chevron_left",  on_click=lambda: _go_prev()).props("flat dense round")
                                    nb = ui.button(icon="chevron_right", on_click=lambda: _go_next()).props("flat dense round")
                                    pb.enabled = p > 0
                                    nb.enabled = p < total_pages - 1
                            ui.html(
                                f'<div style="overflow-x:auto"><table style="width:100%;border-collapse:collapse">'
                                f'<thead><tr style="background:#f9fafb">{ev_cols}</tr></thead>'
                                f'<tbody>{ev_html}</tbody></table></div>'
                            )

                def _go_prev() -> None:
                    if pg[0] > 0:
                        pg[0] -= 1
                        _render_ev(pg[0])

                def _go_next() -> None:
                    if pg[0] < total_pages - 1:
                        pg[0] += 1
                        _render_ev(pg[0])

                _render_ev(0)

        _render(hace7.strftime("%Y-%m-%d"), hoy.strftime("%Y-%m-%d"), "todas", "todas")
