"""
tabs/cron_log.py
Página ADMIN → Log: corridas de los crons nocturnos (stock_snapshot, competidores_snapshot)
para todas las cuentas. Exporta: build_tab_log
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from nicegui import app, ui

from db import get_all_users, get_connection, user_can_access_tab

_GREEN  = "#2E7D32"
_YELLOW = "#BA7517"
_RED    = "#A32D2D"

_JOBS = [("stock", "Stock"), ("competidores", "Competidores")]


def _require_login() -> Optional[Dict[str, Any]]:
    user = app.storage.user.get("user")
    if not user:
        ui.notify("Debes iniciar sesión para continuar", color="negative")
    return user


def _dot(color: str):
    return ui.element("span").style(
        f"display:inline-block;width:10px;height:10px;border-radius:9999px;"
        f"background:{color};flex-shrink:0")


def _card_header(title: str, color: str):
    with ui.row().classes("items-center gap-2 w-full mb-2"):
        _dot(color)
        ui.label(title).classes("font-bold text-base text-gray-800")


def _query_cron_runs(desde: str, user_id: Optional[int] = None, job: Optional[str] = None) -> List[Dict[str, Any]]:
    conn = get_connection()
    try:
        sql = "SELECT * FROM cron_runs WHERE run_date >= ?"
        params: List[Any] = [desde]
        if user_id:
            sql += " AND user_id = ?"
            params.append(user_id)
        if job:
            sql += " AND job = ?"
            params.append(job)
        sql += " ORDER BY run_date DESC, user_id, job"
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()


def build_tab_log(container) -> None:
    """Pestaña ADMIN → Log: corridas de cron_runs de todas las cuentas."""
    container.clear()
    user = _require_login()
    if not user:
        return
    if not user_can_access_tab(user["id"], "admin"):
        with container:
            ui.label("No tenés permiso para acceder a Log.").classes("text-negative")
        return

    users = {u["id"]: u["username"] for u in get_all_users()}

    with container:
        with ui.column().classes("w-full gap-3 p-2"):
            ui.label("Log de corridas — Crons nocturnos").classes("text-xl font-bold")

            with ui.row().classes("items-center gap-3"):
                rango_sel = ui.select({7: "7 días", 14: "14 días", 30: "30 días"}, value=14, label="Rango").props(
                    "dense outlined").classes("w-32")
                user_sel = ui.select({0: "Todas las cuentas", **users}, value=0, label="Cuenta").props(
                    "dense outlined").classes("w-48")
                job_sel = ui.select({"": "Todos los jobs", **dict(_JOBS)}, value="", label="Job").props(
                    "dense outlined").classes("w-48")

            resumen_container = ui.column().classes("w-full")
            detalle_container = ui.column().classes("w-full")

            def _refresh() -> None:
                resumen_container.clear()
                detalle_container.clear()

                dias = int(rango_sel.value)
                desde = (datetime.now().date() - timedelta(days=dias - 1)).isoformat()
                uid_filter = user_sel.value or None
                job_filter = job_sel.value or None
                rows = _query_cron_runs(desde, uid_filter, job_filter)

                by_key: Dict[tuple, Dict[str, Any]] = {(r["user_id"], r["job"], r["run_date"]): r for r in rows}
                dias_list = [(datetime.now().date() - timedelta(days=i)).isoformat() for i in range(dias - 1, -1, -1)]

                with resumen_container:
                    with ui.card().classes("w-full").style("border:1px solid #e0e0e0;padding:10px"):
                        _card_header(f"Resumen — últimos {dias} días", _GREEN)
                        with ui.column().classes("w-full gap-2"):
                            for uid, uname in sorted(users.items(), key=lambda kv: kv[1]):
                                if uid_filter and uid != uid_filter:
                                    continue
                                for job_key, job_label in _JOBS:
                                    if job_filter and job_key != job_filter:
                                        continue
                                    with ui.row().classes("items-center gap-2 w-full"):
                                        ui.label(f"{uname} — {job_label}").classes(
                                            "text-xs w-48 flex-shrink-0").style("color:#374151")
                                        with ui.row().classes("items-center gap-1"):
                                            for d in dias_list:
                                                r = by_key.get((uid, job_key, d))
                                                c = (_GREEN if r and r["status"] == "ok"
                                                     else _YELLOW if r and r["status"] == "partial"
                                                     else _RED if r and r["status"] == "fail"
                                                     else "#d1d5db")
                                                dot = _dot(c)
                                                if r:
                                                    tip = f"{d}: {r['status']} — {r['count']} registros"
                                                    if r.get("error"):
                                                        tip += f" — {r['error']}"
                                                else:
                                                    tip = f"{d}: no corrió"
                                                dot.tooltip(tip)

                with detalle_container:
                    with ui.card().classes("w-full").style("border:1px solid #e0e0e0;padding:10px"):
                        _card_header("Detalle de corridas", _GREEN)
                        columns = [
                            {"name": "job", "label": "Job", "field": "job", "align": "left", "sortable": True},
                            {"name": "user", "label": "Cuenta", "field": "user", "align": "left", "sortable": True},
                            {"name": "run_date", "label": "Fecha", "field": "run_date", "align": "left", "sortable": True},
                            {"name": "run_datetime", "label": "Hora", "field": "run_datetime", "align": "left"},
                            {"name": "status", "label": "Estado", "field": "status", "align": "left", "sortable": True},
                            {"name": "count", "label": "Cantidad", "field": "count", "align": "right", "sortable": True},
                            {"name": "duration_seconds", "label": "Duración (s)", "field": "duration_seconds", "align": "right"},
                            {"name": "error", "label": "Error", "field": "error", "align": "left"},
                        ]
                        table_rows = [
                            {
                                "job": dict(_JOBS).get(r["job"], r["job"]),
                                "user": users.get(r["user_id"], str(r["user_id"])),
                                "run_date": r["run_date"],
                                "run_datetime": (r["run_datetime"] or "")[:19].replace("T", " "),
                                "status": r["status"],
                                "count": r["count"],
                                "duration_seconds": f"{r['duration_seconds']:.1f}" if r.get("duration_seconds") is not None else "—",
                                "error": r.get("error") or "",
                            }
                            for r in rows
                        ]
                        if table_rows:
                            ui.table(columns=columns, rows=table_rows, row_key="run_datetime").classes("w-full")
                        else:
                            ui.label("Sin corridas registradas en este rango.").classes("text-sm text-gray-400")

            rango_sel.on_value_change(lambda: _refresh())
            user_sel.on_value_change(lambda: _refresh())
            job_sel.on_value_change(lambda: _refresh())
            _refresh()
