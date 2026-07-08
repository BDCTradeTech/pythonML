"""
tabs/competidores.py
Pagina Competidores: ranking de vendedores por catalogo, 5 periodos lado a lado.
Lista scrolleable de todos los catalogos, expandible por click.
"""
from __future__ import annotations
from datetime import date, timedelta
from typing import Any, Dict, List, Optional
from nicegui import app, run, ui
from db import get_connection

try:
    from ml_api import get_ml_access_token
except ImportError:
    from db import get_ml_access_token

_LVL = {
    "1_green":  "Platinum",
    "2_green":  "Gold",
    "3_green":  "Silver",
    "4_green":  "Bronze",
    "5_yellow": "Mercader",
    "6_red":    "Nuevo",
}

_LVL_ICON = {
    "1_green": "🟢", "2_green": "🟢", "3_green": "🟡",
    "4_green": "⚪", "5_yellow": "🟡", "6_red": "🔴",
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
            if sid:
                ids.add(sid)
        except Exception:
            pass
    return ids


def _get_catalogs_for_user(user_id: int) -> List[Dict]:
    conn = get_connection()
    rows = conn.execute("""
        SELECT sc.catalog_product_id,
               GROUP_CONCAT(DISTINCT sc.sku) as skus,
               p.nombre as nombre,
               COUNT(DISTINCT cs.seller_id) as n_vendedores,
               MAX(cs.snapshot_date) as ultimo_snap
        FROM sku_catalogos sc
        LEFT JOIN productos p ON p.sku = sc.sku AND p.user_id = sc.user_id
        LEFT JOIN competidores_snapshots cs
               ON cs.catalog_product_id = sc.catalog_product_id
              AND cs.user_id = sc.user_id
        WHERE sc.user_id = ?
          AND sc.catalog_product_id IS NOT NULL
          AND sc.catalog_product_id != ''
        GROUP BY sc.catalog_product_id
        ORDER BY p.nombre, sc.catalog_product_id
    """, (user_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _get_ranking(user_id: int, cpid: str, dias: Optional[int]) -> List[Dict]:
    conn = get_connection()
    if dias is None:
        rows = conn.execute("""
            SELECT seller_id, seller_nickname, seller_total_ventas as ventas,
                   seller_level_id, snapshot_date
            FROM competidores_snapshots
            WHERE user_id=? AND catalog_product_id=?
              AND snapshot_date = (
                SELECT MAX(snapshot_date) FROM competidores_snapshots
                WHERE user_id=? AND catalog_product_id=?
              )
            ORDER BY seller_total_ventas DESC NULLS LAST
        """, (user_id, cpid, user_id, cpid)).fetchall()
    else:
        fecha_desde = (date.today() - timedelta(days=dias)).isoformat()
        rows = conn.execute("""
            SELECT
                s1.seller_id, s1.seller_nickname, s1.seller_level_id,
                s1.snapshot_date,
                CASE
                    WHEN s0.seller_total_ventas IS NOT NULL
                    THEN s1.seller_total_ventas - s0.seller_total_ventas
                    ELSE NULL
                END as ventas
            FROM competidores_snapshots s1
            LEFT JOIN competidores_snapshots s0
                ON s0.user_id = s1.user_id
               AND s0.catalog_product_id = s1.catalog_product_id
               AND s0.seller_id = s1.seller_id
               AND s0.snapshot_date = (
                   SELECT MAX(snapshot_date) FROM competidores_snapshots
                   WHERE user_id=s1.user_id
                     AND catalog_product_id=s1.catalog_product_id
                     AND seller_id=s1.seller_id
                     AND snapshot_date <= ?
               )
            WHERE s1.user_id=? AND s1.catalog_product_id=?
              AND s1.snapshot_date = (
                SELECT MAX(snapshot_date) FROM competidores_snapshots
                WHERE user_id=? AND catalog_product_id=?
              )
            ORDER BY ventas DESC NULLS LAST
        """, (fecha_desde, user_id, cpid, user_id, cpid)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _render_tabla(rows: List[Dict], mis_ids: set, titulo: str, nota: str):
    mi_puesto_real = None
    yo_en_lista = False

    # Calcular mi puesto real
    for i, r in enumerate(rows, 1):
        if str(r.get("seller_id") or "") in mis_ids:
            yo_en_lista = True
            mi_puesto_real = i
            break

    con_datos = any(r.get("ventas") is not None and (r.get("ventas") or 0) > 0 for r in rows)

    with ui.element("div").style(
        "flex:1;min-width:0;border:0.5px solid #e2e8f0;border-radius:8px;overflow:hidden;display:flex;flex-direction:column"
    ):
        # Header tabla
        with ui.element("div").style("background:#2A7AC7;padding:7px 10px;flex-shrink:0"):
            ui.label(titulo).style("font-size:12px;font-weight:500;color:#fff;display:block")
            ui.label(nota).style("font-size:9px;color:rgba(255,255,255,.65);display:block")

        if not rows or not con_datos:
            msg = "Sin datos aun" if not rows else "Datos disponibles manana"
            ui.label(msg).style("font-size:10px;color:#9ca3af;padding:10px;text-align:center;display:block")
        else:
            with ui.element("div").style("overflow-y:auto;flex:1"):
                with ui.element("table").style("width:100%;border-collapse:collapse;font-size:10px"):
                    with ui.element("thead"):
                        with ui.element("tr"):
                            for h, w, a in [("#","24px","center"),("Vendedor","auto","left"),("Ventas","52px","right")]:
                                with ui.element("th").style(
                                    f"padding:4px 6px;background:#EEF6FD;color:#185FA5;font-size:9px;"
                                    f"font-weight:600;text-align:{a};width:{w};"
                                    f"position:sticky;top:0;z-index:2;border-bottom:0.5px solid #d0e8f8"
                                ):
                                    ui.html(h)
                    with ui.element("tbody"):
                        for i, r in enumerate(rows, 1):
                            sid = str(r.get("seller_id") or "")
                            es_mio = sid in mis_ids
                            ventas = r.get("ventas")
                            nick = (r.get("seller_nickname") or f"ID {sid}")[:22]
                            lvl = r.get("seller_level_id") or ""
                            icon = _LVL_ICON.get(lvl, "")
                            bg = "background:#EEF6FD;" if es_mio else ("background:#fafafa;" if i % 2 == 0 else "")
                            pos_color = "#ca6d00" if i==1 else "#7c6514" if i==2 else "#6b7280" if i==3 else ("#166534" if es_mio else "#9ca3af")

                            with ui.element("tr").style(bg):
                                with ui.element("td").style(
                                    f"padding:3px 4px;text-align:center;border-bottom:0.5px solid #f1f5f9;"
                                    f"font-weight:{'600' if i<=3 or es_mio else '400'};color:{pos_color};font-size:10px"
                                ):
                                    ui.html(str(i))
                                with ui.element("td").style(
                                    f"padding:3px 6px;border-bottom:0.5px solid #f1f5f9;"
                                    f"{'font-weight:500;color:#185FA5' if es_mio else 'color:#374151'}"
                                ):
                                    lbl = ("⭐ " if es_mio else (icon + " " if icon else "")) + nick
                                    ui.label(lbl).style(
                                        "font-size:10px;overflow:hidden;text-overflow:ellipsis;"
                                        "white-space:nowrap;display:block;max-width:160px"
                                    )
                                with ui.element("td").style(
                                    f"padding:3px 6px;text-align:right;border-bottom:0.5px solid #f1f5f9;"
                                    f"font-size:10px;{'font-weight:500;color:#185FA5' if es_mio else 'color:#374151'}"
                                ):
                                    if ventas is not None and int(ventas) >= 0:
                                        ui.html(f"{int(ventas):,}".replace(",","."))
                                    else:
                                        ui.html("<span style='color:#9ca3af'>—</span>")

        # Footer: mi posicion si no aparezco
        if rows and not yo_en_lista:
            with ui.element("div").style(
                "padding:4px 8px;background:#FEF9EC;border-top:0.5px solid #FDE68A;"
                "font-size:9px;color:#92400E;flex-shrink:0"
            ):
                ui.html("No apareces en este catalogo")


def build_tab_competidores() -> None:
    user = app.storage.user.get("user")
    if not user:
        ui.label("Debes iniciar sesion").classes("text-red-500 p-4")
        return
    uid = user["id"]
    mis_ids = _get_mis_seller_ids(uid)

    catalogs = _get_catalogs_for_user(uid)

    PERIODOS = [
        ("Historica",  None, "acumulado total"),
        ("Anual",      365,  "ultimos 365 dias"),
        ("Mensual",    30,   "ultimos 30 dias"),
        ("Semanal",    7,    "ultimos 7 dias"),
        ("Diaria",     1,    "ultimas 24 hs"),
    ]

    with ui.element("div").style("padding:10px 16px 0"):
        # Header
        with ui.element("div").style("display:flex;align-items:center;gap:10px;margin-bottom:10px"):
            ui.label("Competidores").style("font-size:14px;font-weight:500;color:var(--color-text-primary)")
            ui.label(f"{len(catalogs)} catalogos · {sum(c.get('n_vendedores') or 0 for c in catalogs)} vendedores totales").style(
                "font-size:11px;color:#9ca3af"
            )

        if not catalogs:
            ui.label("No hay catalogos registrados. Abri un popup de producto para sincronizar.").style(
                "font-size:12px;color:#ca6d00;padding:24px"
            )
            return

        # Lista scrolleable de catalogos
        with ui.element("div").style("display:flex;flex-direction:column;gap:10px;overflow-y:auto;max-height:calc(100vh - 120px)"):
            for cat in catalogs:
                cpid = cat["catalog_product_id"]
                nombre = cat.get("nombre") or cat.get("skus") or cpid
                n_vend = cat.get("n_vendedores") or 0
                ultimo = cat.get("ultimo_snap") or "—"

                with ui.expansion(
                    text=f"{nombre}  ·  {cpid}",
                    icon="ti-users",
                ).props("dense").style(
                    "border:0.5px solid #e2e8f0;border-radius:8px;background:var(--color-background-primary)"
                ) as exp:
                    # Caption debajo del titulo
                    with exp.add_slot("caption"):
                        ui.label(f"{n_vend} vendedores · ultimo snap: {ultimo}").style(
                            "font-size:9px;color:#9ca3af"
                        )

                    # 5 tablas lado a lado — se cargan al expandir
                    tablas_ref: list = [False]

                    def _cargar_tablas(cpid_=cpid):
                        if tablas_ref[0]:
                            return
                        tablas_ref[0] = True
                        with ui.element("div").style(
                            "display:flex;gap:6px;padding:8px 4px;width:100%;overflow-x:auto"
                        ):
                            for titulo, dias, nota in PERIODOS:
                                rows = _get_ranking(uid, cpid_, dias)
                                _render_tabla(rows, mis_ids, titulo, nota)

                    exp.on("update:model-value", lambda e, fn=_cargar_tablas: fn() if e.args else None)
