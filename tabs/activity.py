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


def _users_name_map() -> Dict[str, str]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT username, nombre FROM users")
        return {r["username"]: _derive_display_name(r["username"], r["nombre"] or "")
                for r in cur.fetchall()}
    finally:
        conn.close()


def _online_users() -> List[Dict[str, Any]]:
    cutoff = (datetime.now() - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%S")
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT usuario, tab, MAX(timestamp) as last_ts "
            "FROM activity_log WHERE timestamp >= ? "
            "GROUP BY usuario ORDER BY last_ts DESC",
            (cutoff,)
        )
        return [dict(r) for r in cur.fetchall()]
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

_TIME_SECTIONS = ["Dashboard", "Estadísticas", "Ventas", "Productos",
                  "Cuotas", "Promos", "Preguntas", "Importacion"]


def _derive_display_name(username: str, nombre: str) -> str:
    if nombre and nombre.strip():
        return nombre.strip()
    local = (username or "").split("@")[0]
    if "." in local:
        parts  = local.split(".", 1)
        first  = parts[0].capitalize()
        last_i = parts[1][0].upper() if parts[1] else ""
        return f"{first} {last_i}." if last_i else first
    return local.capitalize()


def _initials(name: str) -> str:
    base  = (name or "?").split("@")[0]
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


def _fmt_mins(secs: Any) -> str:
    if not secs:
        return "—"
    s = int(secs)
    if s < 60:
        return "<1m"
    m = s // 60
    if m < 60:
        return f"{m}m"
    h, rm = divmod(m, 60)
    return f"{h}h {rm}m" if rm else f"{h}h"


def _fmt_total_time(secs: int) -> str:
    if secs < 60:
        return f"{secs}s"
    m = secs // 60
    if m < 60:
        return f"{m}m"
    h, rm = divmod(m, 60)
    return f"{h}h {rm}m" if rm else f"{h}h"


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

        # ── ONLINE AHORA ──────────────────────────────────────────────────
        online_area = ui.column().classes("w-full mb-2")

        def _render_online() -> None:
            online_area.clear()
            online = _online_users()
            nmap   = _users_name_map()
            with online_area:
                with ui.card().classes("w-full p-3").style(
                    "border:1px solid #bbf7d0;background:#f0fdf4"
                ):
                    with ui.row().classes("items-center gap-2 mb-2"):
                        ui.icon("circle", size="xs").style("color:#22c55e")
                        ui.label("ONLINE AHORA").classes(
                            "text-xs font-bold tracking-wide text-gray-600"
                        )
                    if not online:
                        ui.label("Sin usuarios activos ahora").classes(
                            "text-sm text-gray-400 italic"
                        )
                    else:
                        with ui.row().classes("gap-3 flex-wrap"):
                            for o in online:
                                u    = o["usuario"]
                                dn   = nmap.get(u, _derive_display_name(u, ""))
                                init = _initials(u)
                                tab  = o.get("tab") or "—"
                                with ui.element("div").style(
                                    "display:flex;align-items:center;gap:8px;background:white;"
                                    "border:1px solid #bbf7d0;border-radius:8px;padding:6px 12px"
                                ):
                                    ui.html(
                                        f'<div style="width:28px;height:28px;border-radius:50%;'
                                        f'background:#22c55e;color:white;display:flex;'
                                        f'align-items:center;justify-content:center;'
                                        f'font-size:12px;font-weight:bold">{init}</div>'
                                        f'<div style="margin-left:8px">'
                                        f'<div style="font-size:13px;font-weight:600">{dn}</div>'
                                        f'<div style="font-size:11px;color:#6b7280">{tab}</div>'
                                        f'</div>'
                                    )

        _render_online()

        # ── A) FILTER BAR ─────────────────────────────────────────────────
        with ui.card().classes("w-full p-3 bg-grey-2 mb-4").style("border:1px solid #e0e0e0"):
            with ui.row().classes("items-end gap-4 flex-wrap"):
                desde_i  = ui.input("Desde", value=hace7.strftime("%Y-%m-%d")).props("type=date dense")
                hasta_i  = ui.input("Hasta",  value=hoy.strftime("%Y-%m-%d")).props("type=date dense")
                tab_opts = ["todas"] + _distinct_tabs()
                tab_s    = ui.select(tab_opts, value="todas", label="Sección").props(
                    "dense outlined"
                ).classes("w-44")
                accion_s = ui.select(
                    ["todas", "page_view", "time_spent"],
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
            names_map = _users_name_map()
            rows      = _query_logs(desde, hasta, tab_f, accion_f)
            view_rows = [r for r in rows if r["accion"] == "page_view"]
            time_rows = [r for r in rows if r["accion"] == "time_spent"]
            disp_rows = [r for r in rows if r["accion"] != "time_spent"]

            with main_area:
                # ── B) METRICS ────────────────────────────────────────────
                tabs_cnt:   Dict[str, int] = defaultdict(int)
                user_views: Dict[str, int] = defaultdict(int)
                for r in view_rows:
                    tabs_cnt[r["tab"]]       += 1
                    user_views[r["usuario"]] += 1

                top_tab        = max(tabs_cnt,   key=tabs_cnt.get)   if tabs_cnt   else "—"
                top_user_email = max(user_views, key=user_views.get) if user_views else None
                top_user       = (names_map.get(top_user_email,
                                  _derive_display_name(top_user_email, ""))
                                  if top_user_email else "—")
                total_secs     = sum(int(r.get("tiempo_segundos") or 0) for r in time_rows)

                with ui.grid(columns=4).classes("w-full gap-3"):
                    for lbl, val, ico, clr in [
                        ("Páginas vistas",       str(len(view_rows)),          "visibility",  "#3B6D11"),
                        ("Tiempo total",          _fmt_total_time(total_secs), "timer",        "#185FA5"),
                        ("Sección más visitada",  top_tab,                     "star",         "#BA7517"),
                        ("Usuario más activo",    top_user,                    "emoji_events", "#6b42c8"),
                    ]:
                        with ui.card().classes("p-4").style("border:1px solid #e0e0e0"):
                            with ui.row().classes("items-center gap-2 mb-1"):
                                ui.icon(ico, size="sm").style(f"color:{clr}")
                                ui.label(lbl).classes(
                                    "text-xs text-gray-500 uppercase tracking-wide"
                                )
                            ui.label(val).classes("text-2xl font-bold").style(f"color:{clr}")

                # ── C) USER SUMMARY ───────────────────────────────────────
                udata: Dict[str, Dict] = {
                    u: {"v": 0, "t": 0, "tab_secs": defaultdict(int), "last": "", "ml": ""}
                    for u in all_users
                }
                for r in rows:
                    u = r["usuario"]
                    if u not in udata:
                        udata[u] = {"v": 0, "t": 0, "tab_secs": defaultdict(int), "last": "", "ml": ""}
                    if r.get("ml_username"):
                        udata[u]["ml"] = r["ml_username"]
                    if r["accion"] == "page_view":
                        udata[u]["v"] += 1
                    elif r["accion"] == "time_spent" and r.get("tiempo_segundos"):
                        secs_v = int(r["tiempo_segundos"])
                        udata[u]["t"] += secs_v
                        udata[u]["tab_secs"][r["tab"]] += secs_v
                    ts = r.get("timestamp") or ""
                    if ts > udata[u]["last"]:
                        udata[u]["last"] = ts

                def _th(c: str, w: str = "auto") -> str:
                    return (
                        f'<th style="padding:8px 12px;text-align:left;font-size:11px;'
                        f'text-transform:uppercase;letter-spacing:.05em;color:#6b7280;'
                        f'font-weight:500;white-space:nowrap;min-width:{w}">{c}</th>'
                    )

                rows_html = ""
                for u, d in sorted(udata.items()):
                    has      = d["v"] > 0
                    dn       = names_map.get(u, _derive_display_name(u, d.get("ml") or ""))
                    init     = _initials(u)
                    last_fmt = d["last"][:16].replace("T", " ") if d["last"] else "—"
                    op       = "" if has else "opacity:.45"
                    sec_cells = "".join(
                        f'<td style="padding:6px 12px;font-size:12px;color:#374151;text-align:center">'
                        f'{_fmt_mins(d["tab_secs"].get(sec)) if d["tab_secs"].get(sec) else "—"}</td>'
                        for sec in _TIME_SECTIONS
                    )
                    rows_html += (
                        f'<tr style="border-top:1px solid #f3f4f6;{op}">'
                        f'<td style="padding:8px 12px">'
                        f'<div style="display:flex;align-items:center;gap:8px">'
                        f'<div style="width:28px;height:28px;border-radius:50%;background:#185FA5;'
                        f'color:white;display:flex;align-items:center;justify-content:center;'
                        f'font-size:12px;font-weight:bold;flex-shrink:0">{init}</div>'
                        f'<div><div style="font-size:14px;font-weight:500">{dn}</div>'
                        f'<div style="font-size:11px;color:#9ca3af">{u}</div></div>'
                        f'</div></td>'
                        f'<td style="padding:6px 12px;font-size:13px;text-align:center">'
                        f'{"—" if not has else d["v"]}</td>'
                        f'<td style="padding:6px 12px;font-size:13px;text-align:center">'
                        f'{"—" if not has else _fmt_mins(d["t"])}</td>'
                        f'{sec_cells}'
                        f'<td style="padding:6px 12px;font-size:12px;color:#6b7280">'
                        f'{"—" if not has else last_fmt}</td>'
                        f'</tr>'
                    )

                sec_hdrs  = "".join(_th(s, "60px") for s in _TIME_SECTIONS)
                cols_html = (
                    _th("Usuario", "140px") +
                    _th("Visitas", "60px") +
                    _th("T. total", "80px") +
                    sec_hdrs +
                    _th("Última actividad", "120px")
                )
                ui.html(
                    f'<div style="border:1px solid #e0e0e0;border-radius:8px;overflow:hidden;'
                    f'background:white;width:100%">'
                    f'<div style="padding:12px 16px;font-weight:700;font-size:15px;'
                    f'border-bottom:1px solid #f3f4f6">Resumen por usuario</div>'
                    f'<div style="overflow-x:auto">'
                    f'<table style="width:100%;border-collapse:collapse">'
                    f'<thead><tr style="background:#f9fafb">{cols_html}</tr></thead>'
                    f'<tbody>{rows_html}</tbody>'
                    f'</table></div></div>'
                )

                # ── D) CHARTS ─────────────────────────────────────────────
                with ui.grid(columns=2).classes("w-full gap-3"):
                    with ui.card().classes("p-4").style("border:1px solid #e0e0e0"):
                        ui.label("Visitas por sección").classes("font-bold text-base mb-3")
                        if tabs_cnt:
                            mx = max(tabs_cnt.values()) or 1
                            for tn, cnt in sorted(tabs_cnt.items(), key=lambda x: -x[1])[:10]:
                                pct = cnt / mx * 100
                                with ui.row().classes("items-center gap-2 mb-1"):
                                    ui.label(tn).classes(
                                        "text-xs text-gray-600 text-right"
                                    ).style("min-width:80px")
                                    with ui.element("div").classes(
                                        "flex-1 rounded overflow-hidden"
                                    ).style("background:#f3f4f6;height:18px"):
                                        ui.element("div").classes("h-full rounded").style(
                                            f"width:{pct:.0f}%;background:#185FA5"
                                        )
                                    ui.label(str(cnt)).classes("text-xs font-bold").style(
                                        "min-width:24px"
                                    )
                        else:
                            ui.label("Sin datos en el período").classes("text-sm text-gray-400")

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
                            f'<th style="text-align:center;font-size:11px;color:#9ca3af;'
                            f'padding:2px 4px">{s}</th>' for s in SLOTS
                        )
                        body = ""
                        for di, day in enumerate(DIAS):
                            cells = (
                                f'<td style="font-size:12px;color:#6b7280;'
                                f'padding-right:6px;text-align:right">{day}</td>'
                            )
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
                                    f'font-size:11px;padding:5px 2px;border-radius:3px;'
                                    f'min-width:42px">{txt}</td>'
                                )
                            body += f"<tr>{cells}</tr>"
                        ui.html(
                            f'<table style="border-collapse:separate;border-spacing:3px;width:100%">'
                            f'<thead><tr>{hdr}</tr></thead><tbody>{body}</tbody></table>'
                        )

                # ── E) EVENTS TABLE (paginada) ────────────────────────────
                PAGE_SIZE   = 50
                total_ev    = len(disp_rows)
                total_pages = max(1, (total_ev + PAGE_SIZE - 1) // PAGE_SIZE)
                pg          = [0]
                ev_area     = ui.column().classes("w-full")

                def _ev_hdr(c: str) -> str:
                    return (
                        f'<th style="padding:8px 12px;text-align:left;font-size:11px;'
                        f'text-transform:uppercase;letter-spacing:.05em;'
                        f'color:#6b7280;font-weight:500">{c}</th>'
                    )

                def _render_ev(p: int) -> None:
                    ev_area.clear()
                    chunk   = disp_rows[p * PAGE_SIZE:(p + 1) * PAGE_SIZE]
                    ev_html = ""
                    for r in chunk:
                        bc   = _BADGE_COLOR.get(r["accion"], "#6b7280")
                        ts   = (r.get("timestamp") or "")[:16].replace("T", " ")
                        u    = r["usuario"]
                        dn   = names_map.get(u, _derive_display_name(u, r.get("ml_username") or ""))
                        init = _initials(u)
                        ev_html += (
                            f'<tr style="border-top:1px solid #f3f4f6">'
                            f'<td style="padding:6px 12px">'
                            f'<div style="display:flex;align-items:center;gap:6px">'
                            f'<div style="width:24px;height:24px;border-radius:50%;'
                            f'background:#185FA5;color:white;display:flex;align-items:center;'
                            f'justify-content:center;font-size:11px;font-weight:bold;'
                            f'flex-shrink:0">{init}</div>'
                            f'<span style="font-size:13px">{dn}</span></div></td>'
                            f'<td style="padding:6px 12px;font-size:13px;color:#374151">'
                            f'{r["tab"]}</td>'
                            f'<td style="padding:6px 12px">'
                            f'<span style="background:{bc};color:white;padding:2px 8px;'
                            f'border-radius:9999px;font-size:11px;font-weight:500">'
                            f'{r["accion"]}</span></td>'
                            f'<td style="padding:6px 12px;font-size:12px;color:#6b7280">'
                            f'{r.get("detalle") or "—"}</td>'
                            f'<td style="padding:6px 12px;font-size:12px;color:#6b7280">'
                            f'{_fmt_secs(r.get("tiempo_segundos"))}</td>'
                            f'<td style="padding:6px 12px;font-size:12px;color:#6b7280">'
                            f'{ts}</td>'
                            f'</tr>'
                        )
                    ev_cols = "".join(
                        _ev_hdr(c) for c in
                        ["Usuario", "Sección", "Acción", "Detalle", "Tiempo en tab", "Hora"]
                    )
                    with ev_area:
                        with ui.card().classes("w-full").style("border:1px solid #e0e0e0"):
                            with ui.row().classes(
                                "items-center justify-between px-4 pt-3 pb-2"
                            ):
                                ui.label(f"Eventos ({total_ev})").classes("font-bold text-base")
                                with ui.row().classes("items-center gap-2"):
                                    ui.label(f"Pág {p + 1} / {total_pages}").classes(
                                        "text-sm text-gray-500"
                                    )
                                    pb = ui.button(
                                        icon="chevron_left", on_click=lambda: _go_prev()
                                    ).props("flat dense round")
                                    nb = ui.button(
                                        icon="chevron_right", on_click=lambda: _go_next()
                                    ).props("flat dense round")
                                    pb.enabled = p > 0
                                    nb.enabled = p < total_pages - 1
                            ui.html(
                                f'<div style="overflow-x:auto">'
                                f'<table style="width:100%;border-collapse:collapse">'
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

        def _refresh_data() -> None:
            _render_online()
            _render(
                desde_i.value  or hace7.strftime("%Y-%m-%d"),
                hasta_i.value  or hoy.strftime("%Y-%m-%d"),
                tab_s.value    or "todas",
                accion_s.value or "todas",
            )

        ui.timer(60, _refresh_data, once=False)

        _render(hace7.strftime("%Y-%m-%d"), hoy.strftime("%Y-%m-%d"), "todas", "todas")
