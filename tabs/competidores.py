"""
tabs/competidores.py
Ranking de competidores por catalogo. Buscador por nombre de producto,
5 tablas de periodo lado a lado full width.
"""
from __future__ import annotations
from datetime import date, timedelta
from typing import Dict, List, Optional
from nicegui import app, run, ui
from db import get_connection

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
               COALESCE(p.nombre, sc.sku, sc.catalog_product_id) as nombre,
               sc.sku,
               COUNT(DISTINCT cs.seller_id) as n_vend,
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
        ORDER BY nombre
    """, (user_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _get_ranking(user_id: int, cpid: str, dias: Optional[int]) -> List[Dict]:
    conn = get_connection()
    if dias is None:
        rows = conn.execute("""
            SELECT seller_id, seller_nickname, seller_level_id,
                   seller_total_ventas as ventas, snapshot_date
            FROM competidores_snapshots
            WHERE user_id=? AND catalog_product_id=?
              AND snapshot_date=(
                SELECT MAX(snapshot_date) FROM competidores_snapshots
                WHERE user_id=? AND catalog_product_id=?
              )
            ORDER BY seller_total_ventas DESC NULLS LAST
        """, (user_id, cpid, user_id, cpid)).fetchall()
    else:
        fecha_desde = (date.today() - timedelta(days=dias)).isoformat()
        rows = conn.execute("""
            SELECT s1.seller_id, s1.seller_nickname, s1.seller_level_id,
                   s1.snapshot_date,
                   CASE WHEN s0.seller_total_ventas IS NOT NULL
                        THEN s1.seller_total_ventas - s0.seller_total_ventas
                        ELSE NULL END as ventas
            FROM competidores_snapshots s1
            LEFT JOIN competidores_snapshots s0
                ON  s0.user_id=s1.user_id
                AND s0.catalog_product_id=s1.catalog_product_id
                AND s0.seller_id=s1.seller_id
                AND s0.snapshot_date=(
                    SELECT MAX(snapshot_date) FROM competidores_snapshots
                    WHERE user_id=s1.user_id
                      AND catalog_product_id=s1.catalog_product_id
                      AND seller_id=s1.seller_id
                      AND snapshot_date <= ?
                )
            WHERE s1.user_id=? AND s1.catalog_product_id=?
              AND s1.snapshot_date=(
                SELECT MAX(snapshot_date) FROM competidores_snapshots
                WHERE user_id=? AND catalog_product_id=?
              )
            ORDER BY ventas DESC NULLS LAST
        """, (fecha_desde, user_id, cpid, user_id, cpid)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _render_tabla(rows: List[Dict], mis_ids: set, titulo: str, nota: str):
    con_datos = any((r.get("ventas") or 0) > 0 for r in rows)
    yo_en_lista = any(str(r.get("seller_id") or "") in mis_ids for r in rows)

    with ui.element("div").style(
        "flex:1;min-width:0;border:0.5px solid #e2e8f0;border-radius:8px;"
        "overflow:hidden;display:flex;flex-direction:column"
    ):
        with ui.element("div").style("background:#2A7AC7;padding:7px 10px;flex-shrink:0"):
            ui.label(titulo).style("font-size:12px;font-weight:500;color:#fff;display:block")
            ui.label(nota).style("font-size:9px;color:rgba(255,255,255,.65);display:block")

        if not rows:
            ui.label("Sin datos").style("font-size:10px;color:#9ca3af;padding:12px;text-align:center;display:block")
            return

        if not con_datos:
            ui.label("Datos disponibles manana").style(
                "font-size:10px;color:#9ca3af;padding:12px;text-align:center;display:block"
            )
            return

        with ui.element("div").style("overflow-y:auto;max-height:calc(100vh - 280px)"):
            with ui.element("table").style("width:100%;border-collapse:collapse"):
                with ui.element("thead"):
                    with ui.element("tr"):
                        for h, w, a in [("#","22px","center"),("Vendedor","auto","left"),("Ventas","54px","right")]:
                            with ui.element("th").style(
                                f"padding:4px 6px;background:#EEF6FD;color:#185FA5;font-size:9px;"
                                f"font-weight:600;text-align:{a};width:{w};"
                                f"position:sticky;top:0;z-index:2;border-bottom:0.5px solid #d0e8f8"
                            ):
                                ui.html(h)
                with ui.element("tbody"):
                    for i, r in enumerate(rows, 1):
                        sid   = str(r.get("seller_id") or "")
                        es_mio = sid in mis_ids
                        ventas = r.get("ventas")
                        nick  = (r.get("seller_nickname") or f"ID {sid}")[:24]
                        icon  = _LVL_ICON.get(r.get("seller_level_id") or "", "")
                        bg = "background:#EEF6FD;" if es_mio else ("background:#fafafa;" if i%2==0 else "")
                        pc = "#ca6d00" if i==1 else "#7c6514" if i==2 else "#6b7280" if i==3 else ("#166534" if es_mio else "#9ca3af")
                        fw = "600" if i<=3 or es_mio else "400"

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
                                    "white-space:nowrap;display:block;max-width:170px"
                                )
                            with ui.element("td").style(
                                f"padding:3px 6px;text-align:right;border-bottom:0.5px solid #f1f5f9;"
                                f"font-size:10px;{'font-weight:500;color:#185FA5' if es_mio else 'color:#374151'}"
                            ):
                                if ventas is not None and int(ventas) >= 0:
                                    ui.html(f"{int(ventas):,}".replace(",","."))
                                else:
                                    ui.html("<span style='color:#9ca3af'>—</span>")

        if not yo_en_lista:
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

    if not catalogs:
        with ui.element("div").style("padding:24px"):
            ui.label("No hay catalogos registrados.").style("font-size:13px;color:#ca6d00")
        return

    # Mapa nombre → cpid para el selector
    opciones = {}
    for c in catalogs:
        nombre = c["nombre"] or c["sku"] or c["catalog_product_id"]
        n = c.get("n_vend") or 0
        label = f"{nombre}  ({n} vendedores)"
        opciones[c["catalog_product_id"]] = label

    estado = {"cpid": None}
    tablas_area_ref: list = [None]

    PERIODOS = [
        ("Historica",  None, "acumulado total"),
        ("Anual",      365,  "ultimos 365 dias"),
        ("Mensual",    30,   "ultimos 30 dias"),
        ("Semanal",    7,    "ultimos 7 dias"),
        ("Diaria",     1,    "ultimas 24 hs"),
    ]

    def _mostrar_tablas(cpid: str):
        tablas_area_ref[0].clear()
        with tablas_area_ref[0]:
            if not cpid:
                return
            with ui.element("div").style("display:flex;gap:6px;width:100%"):
                for titulo, dias, nota in PERIODOS:
                    rows = _get_ranking(uid, cpid, dias)
                    _render_tabla(rows, mis_ids, titulo, nota)

    async def _cargar():
        cpid = estado.get("cpid")
        if not cpid:
            tablas_area_ref[0].clear()
            return
        await run.io_bound(_mostrar_tablas, cpid)

    with ui.element("div").style("padding:12px 16px 0"):
        # Controles
        with ui.row().style("gap:8px;align-items:flex-end;flex-wrap:wrap;margin-bottom:12px"):
            with ui.column().style("gap:3px"):
                ui.label("Producto").style("font-size:11px;color:var(--color-text-secondary)")
                sel = ui.select(
                    options=opciones,
                    value=None,
                    label="",
                ).props("dense outlined clearable use-input input-debounce=200").style(
                    "min-width:380px;font-size:12px"
                )
                def _on_sel(e):
                    estado["cpid"] = e.value
                    ui.timer(0.05, _cargar, once=True)
                sel.on_value_change(_on_sel)

            # Info
            ui.label(
                f"{len(catalogs)} catalogos · primer snapshot: hoy"
            ).style("font-size:10px;color:#9ca3af;align-self:flex-end;padding-bottom:4px")

        # Area de 5 tablas
        tablas_area = ui.element("div").style("width:100%")
        tablas_area_ref[0] = tablas_area
        with tablas_area:
            ui.label("Selecciona un producto para ver el ranking de competidores.").style(
                "font-size:13px;color:#9ca3af;padding:24px"
            )

    # Auto-cargar el primero
    if catalogs:
        primer_cpid = catalogs[0]["catalog_product_id"]
        estado["cpid"] = primer_cpid
        sel.value = primer_cpid
