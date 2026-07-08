"""
tabs/competidores.py
Ranking global de competidores — todos los vendedores de todos los catalogos
unificados en 5 tablas por periodo, ordenados por ventas.
"""
from __future__ import annotations
from datetime import date, timedelta
from typing import Dict, List, Optional
from nicegui import app, run, ui
from db import get_connection

_LVL_ICON = {
    "1_green":"🟢","2_green":"🟢","3_green":"🟡",
    "4_green":"⚪","5_yellow":"🟡","6_red":"🔴",
}


def _get_mis_seller_ids(user_id: int) -> set:
    import json
    conn = get_connection()
    rows = conn.execute("SELECT raw_data FROM ml_credentials WHERE raw_data IS NOT NULL").fetchall()
    conn.close()
    ids = set()
    for r in rows:
        try:
            d = json.loads(r[0]) if r[0] else {}
            sid = str(d.get("id") or "")
            if sid: ids.add(sid)
        except Exception:
            pass
    return ids


def _get_ranking_global(user_id: int, dias: Optional[int]) -> List[Dict]:
    """
    Devuelve lista de vendedores ordenados por ventas.
    dias=None  → historico (seller_total_ventas del ultimo snapshot)
    dias=N     → diferencia entre ultimo snapshot y hace N dias
    """
    conn = get_connection()
    # Subquery: un registro por seller (el mas reciente)
    latest = "(SELECT MAX(snapshot_date) FROM competidores_snapshots WHERE user_id=?)"

    if dias is None:
        rows = conn.execute(f"""
            SELECT seller_id, seller_nickname, seller_level_id,
                   MAX(seller_total_ventas) as ventas
            FROM competidores_snapshots
            WHERE user_id=? AND snapshot_date = {latest}
            GROUP BY seller_id
            ORDER BY ventas DESC NULLS LAST
        """, (user_id, user_id)).fetchall()
    else:
        fecha_desde = (date.today() - timedelta(days=dias)).isoformat()
        rows = conn.execute(f"""
            SELECT
                s1.seller_id,
                s1.seller_nickname,
                s1.seller_level_id,
                s1.ventas_hoy - COALESCE(s0.ventas_antes, s1.ventas_hoy) as ventas
            FROM (
                SELECT seller_id, seller_nickname, seller_level_id,
                       MAX(seller_total_ventas) as ventas_hoy
                FROM competidores_snapshots
                WHERE user_id=? AND snapshot_date = {latest}
                GROUP BY seller_id
            ) s1
            LEFT JOIN (
                SELECT seller_id, MAX(seller_total_ventas) as ventas_antes
                FROM competidores_snapshots
                WHERE user_id=? AND snapshot_date <= ?
                GROUP BY seller_id
            ) s0 ON s0.seller_id = s1.seller_id
            ORDER BY ventas DESC NULLS LAST
        """, (user_id, user_id, user_id, fecha_desde)).fetchall()

    conn.close()
    return [dict(r) for r in rows]


def _render_tabla(rows: List[Dict], mis_ids: set, titulo: str, nota: str):
    con_datos = any((r.get("ventas") or 0) > 0 for r in rows)

    with ui.element("div").style(
        "flex:1;min-width:0;border:0.5px solid #e2e8f0;border-radius:8px;"
        "overflow:hidden;display:flex;flex-direction:column"
    ):
        with ui.element("div").style("background:#2A7AC7;padding:8px 10px;flex-shrink:0"):
            ui.label(titulo).style("font-size:12px;font-weight:500;color:#fff;display:block")
            ui.label(nota).style("font-size:9px;color:rgba(255,255,255,.65);display:block")

        if not rows:
            ui.label("Sin datos").style("font-size:10px;color:#9ca3af;padding:12px;text-align:center;display:block")
            return

        if not con_datos and titulo != "Historica":
            ui.label("Datos disponibles cuando haya mas de 1 snapshot").style(
                "font-size:10px;color:#9ca3af;padding:12px;text-align:center;display:block"
            )
            return

        with ui.element("div").style("overflow-y:auto;max-height:calc(100vh - 230px)"):
            with ui.element("table").style("width:100%;border-collapse:collapse"):
                with ui.element("thead"):
                    with ui.element("tr"):
                        for h, w, a in [("#","22px","center"),("Vendedor","auto","left"),("Ventas","60px","right")]:
                            with ui.element("th").style(
                                f"padding:4px 6px;background:#EEF6FD;color:#185FA5;font-size:9px;"
                                f"font-weight:600;text-align:{a};width:{w};"
                                f"position:sticky;top:0;z-index:2;border-bottom:0.5px solid #d0e8f8"
                            ):
                                ui.html(h)
                with ui.element("tbody"):
                    for i, r in enumerate(rows, 1):
                        sid    = str(r.get("seller_id") or "")
                        es_mio = sid in mis_ids
                        ventas = r.get("ventas")
                        nick   = (r.get("seller_nickname") or f"ID {sid}")[:26]
                        icon   = _LVL_ICON.get(r.get("seller_level_id") or "", "")
                        bg     = "background:#EEF6FD;" if es_mio else ("background:#fafafa;" if i%2==0 else "")
                        pc     = "#ca6d00" if i==1 else "#7c6514" if i==2 else "#6b7280" if i==3 else ("#166534" if es_mio else "#9ca3af")
                        fw     = "600" if i<=3 or es_mio else "400"

                        with ui.element("tr").style(bg):
                            with ui.element("td").style(
                                f"padding:3px 4px;text-align:center;border-bottom:0.5px solid #f1f5f9;"
                                f"font-weight:{fw};color:{pc};font-size:10px"
                            ):
                                ui.html(str(i))
                            with ui.element("td").style(
                                f"padding:3px 6px;border-bottom:0.5px solid #f1f5f9;"
                                f"{'font-weight:500;color:#185FA5' if es_mio else 'color:#374151'}"
                            ):
                                ui.label(
                                    ("⭐ " if es_mio else (icon+" " if icon else "")) + nick
                                ).style(
                                    "font-size:10px;overflow:hidden;text-overflow:ellipsis;"
                                    "white-space:nowrap;display:block"
                                )
                            with ui.element("td").style(
                                f"padding:3px 6px;text-align:right;border-bottom:0.5px solid #f1f5f9;"
                                f"font-size:10px;{'font-weight:500;color:#185FA5' if es_mio else 'color:#374151'}"
                            ):
                                if ventas is not None and int(ventas) >= 0:
                                    ui.html(f"{int(ventas):,}".replace(",","."))
                                else:
                                    ui.html("<span style='color:#9ca3af'>—</span>")


def build_tab_competidores() -> None:
    user = app.storage.user.get("user")
    if not user:
        ui.label("Debes iniciar sesion").classes("text-red-500 p-4")
        return
    uid = user["id"]
    mis_ids = _get_mis_seller_ids(uid)

    PERIODOS = [
        ("Historica",  None, "acumulado de por vida"),
        ("Anual",      365,  "ultimos 365 dias"),
        ("Mensual",    30,   "ultimos 30 dias"),
        ("Semanal",    7,    "ultimos 7 dias"),
        ("Diaria",     1,    "ultimas 24 hs"),
    ]

    with ui.element("div").style("padding:12px 16px 0;display:flex;flex-direction:column;height:calc(100vh - 80px)"):
        # 5 tablas full width, todo el alto disponible
        with ui.element("div").style("display:flex;gap:8px;flex:1;min-height:0"):
            for titulo, dias, nota in PERIODOS:
                rows = _get_ranking_global(uid, dias)
                _render_tabla(rows, mis_ids, titulo, nota)
