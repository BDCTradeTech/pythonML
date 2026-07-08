"""
tabs/competidores.py
Ranking global de competidores: 5 tablas por periodo, full width.
# = rank por ventas, fijo independiente del orden de visualizacion.
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
    conn = get_connection()
    latest_sub = "(SELECT MAX(snapshot_date) FROM competidores_snapshots WHERE user_id=?)"
    if dias is None:
        rows = conn.execute(f"""
            SELECT seller_id, seller_nickname, seller_level_id,
                   MAX(seller_total_ventas) as ventas
            FROM competidores_snapshots
            WHERE user_id=? AND snapshot_date={latest_sub}
            GROUP BY seller_id
            ORDER BY ventas DESC NULLS LAST
        """, (user_id, user_id)).fetchall()
    else:
        fecha_desde = (date.today() - timedelta(days=dias)).isoformat()
        rows = conn.execute(f"""
            SELECT s1.seller_id, s1.seller_nickname, s1.seller_level_id,
                   s1.ventas_hoy - COALESCE(s0.ventas_antes, s1.ventas_hoy) as ventas
            FROM (
                SELECT seller_id, seller_nickname, seller_level_id,
                       MAX(seller_total_ventas) as ventas_hoy
                FROM competidores_snapshots
                WHERE user_id=? AND snapshot_date={latest_sub}
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

    # Agregar rank_ventas fijo (posicion por ventas, no cambia al ordenar)
    result = []
    for i, r in enumerate(rows, 1):
        d = dict(r)
        d["rank_ventas"] = i
        result.append(d)
    return result


def _render_tabla(rows_orig: List[Dict], mis_ids: set, titulo: str, nota: str):
    total = len(rows_orig)

    # Posicion del usuario (siempre por rank_ventas)
    mi_puesto = None
    for r in rows_orig:
        if str(r.get("seller_id") or "") in mis_ids:
            mi_puesto = r.get("rank_ventas")
            break

    sort_state = {"col": "ventas", "asc": False}
    tbody_ref: list = [None]

    def _sorted():
        col, asc = sort_state["col"], sort_state["asc"]
        if col == "nick":
            return sorted(rows_orig,
                          key=lambda r: (r.get("seller_nickname") or "").lower(),
                          reverse=not asc)
        # default: ventas DESC
        return sorted(rows_orig,
                      key=lambda r: (r.get("ventas") if r.get("ventas") is not None else -1),
                      reverse=not asc)

    def _render_body():
        tbody_ref[0].clear()
        with tbody_ref[0]:
            for r in _sorted():
                sid    = str(r.get("seller_id") or "")
                es_mio = sid in mis_ids
                ventas = r.get("ventas")
                rank   = r.get("rank_ventas", "—")   # rank fijo por ventas
                nick   = (r.get("seller_nickname") or f"ID {sid}")[:28]
                icon   = _LVL_ICON.get(r.get("seller_level_id") or "", "")
                bg     = "background:#EEF6FD;" if es_mio else ("background:#fafafa;" if int(rank)%2==0 else "") if isinstance(rank, int) else ""
                pc     = "#ca6d00" if rank==1 else "#7c6514" if rank==2 else "#6b7280" if rank==3 else ("#166534" if es_mio else "#9ca3af")
                fw     = "700" if es_mio else ("600" if isinstance(rank, int) and rank<=3 else "400")

                with ui.element("tr").style(bg):
                    # # fijo por ventas
                    with ui.element("td").style(
                        f"padding:3px 5px;text-align:center;border-bottom:0.5px solid #f1f5f9;"
                        f"font-weight:{fw};color:{pc};font-size:10px;white-space:nowrap"
                    ):
                        ui.html(str(rank))
                    # Vendedor
                    with ui.element("td").style(
                        f"padding:3px 8px;border-bottom:0.5px solid #f1f5f9;"
                        f"font-size:10px;font-weight:{fw};"
                        f"{'color:#185FA5' if es_mio else 'color:#374151'}"
                    ):
                        ui.label(
                            ("⭐ " if es_mio else (icon+" " if icon else "")) + nick
                        ).style(
                            "overflow:hidden;text-overflow:ellipsis;white-space:nowrap;display:block"
                        )
                    # Ventas
                    with ui.element("td").style(
                        f"padding:3px 8px;text-align:right;border-bottom:0.5px solid #f1f5f9;"
                        f"font-size:10px;font-weight:{fw};"
                        f"{'color:#185FA5' if es_mio else 'color:#374151'}"
                    ):
                        if ventas is not None and int(ventas) >= 0:
                            ui.html(f"{int(ventas):,}".replace(",","."))
                        else:
                            ui.html("<span style='color:#9ca3af'>—</span>")

    def _toggle_sort(col: str):
        if sort_state["col"] == col:
            sort_state["asc"] = not sort_state["asc"]
        else:
            sort_state["col"] = col
            sort_state["asc"] = (col == "nick")
        _render_body()

    # Color del puesto en header
    if mi_puesto and total:
        pct = mi_puesto / total
        pos_color = "#86EFAC" if pct <= 0.1 else "#FDE68A" if pct <= 0.3 else "rgba(255,255,255,.85)"
        pos_txt = f"#{mi_puesto} / {total}"
    else:
        pos_color = "rgba(255,255,255,.45)"
        pos_txt = f"— / {total}"

    TH = (
        "padding:4px 8px;background:#EEF6FD;color:#185FA5;font-size:9px;"
        "font-weight:600;position:sticky;top:0;z-index:2;"
        "border-bottom:0.5px solid #d0e8f8;cursor:pointer;user-select:none;white-space:nowrap"
    )

    with ui.element("div").style(
        "flex:1;min-width:180px;border:0.5px solid #e2e8f0;border-radius:8px;"
        "overflow:hidden;display:flex;flex-direction:column"
    ):
        # Header con posicion
        with ui.element("div").style("background:#2A7AC7;padding:8px 10px;flex-shrink:0"):
            with ui.element("div").style("display:flex;justify-content:space-between;align-items:flex-start"):
                with ui.element("div"):
                    ui.label(titulo).style("font-size:12px;font-weight:500;color:#fff;display:block")
                    ui.label(nota).style("font-size:9px;color:rgba(255,255,255,.65);display:block")
                ui.label(pos_txt).style(
                    f"font-size:12px;font-weight:700;color:{pos_color};white-space:nowrap;margin-left:8px;align-self:center"
                )

        if not rows_orig:
            ui.label("Sin datos").style(
                "font-size:10px;color:#9ca3af;padding:16px;text-align:center;display:block"
            )
            return

        # Siempre mostrar la tabla — incluso si todas las ventas son 0
        with ui.element("div").style("overflow-y:auto;max-height:calc(100vh - 230px)"):
            with ui.element("table").style("width:100%;border-collapse:collapse;table-layout:fixed"):
                with ui.element("thead"):
                    with ui.element("tr"):
                        with ui.element("th").style(TH + ";width:28px;text-align:center"):
                            ui.html("#")
                        with ui.element("th").style(TH + ";text-align:left").on(
                            "click", lambda: _toggle_sort("nick")
                        ):
                            ui.html("Vendedor <span style='opacity:.5'>↕</span>")
                        with ui.element("th").style(TH + ";width:72px;text-align:right").on(
                            "click", lambda: _toggle_sort("ventas")
                        ):
                            ui.html("Ventas <span style='opacity:.5'>↕</span>")

                tbody = ui.element("tbody")
                tbody_ref[0] = tbody
                _render_body()

        # Nota al pie si todos son 0
        hay_datos = any((r.get("ventas") or 0) > 0 for r in rows_orig)
        if not hay_datos and titulo != "Historica":
            with ui.element("div").style(
                "padding:4px 10px;background:#F8FAFC;border-top:0.5px solid #e2e8f0;"
                "font-size:9px;color:#9ca3af;flex-shrink:0"
            ):
                ui.html("Sin diferencias detectadas aun — los valores iran apareciendo a medida que se acumulen snapshots")


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

    with ui.element("div").style(
        "padding:12px 16px 0;display:flex;flex-direction:column;height:calc(100vh - 82px)"
    ):
        with ui.element("div").style("display:flex;gap:8px;flex:1;min-height:0"):
            for titulo, dias, nota in PERIODOS:
                rows = _get_ranking_global(uid, dias)
                _render_tabla(rows, mis_ids, titulo, nota)
