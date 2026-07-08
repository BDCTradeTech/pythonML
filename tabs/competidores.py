"""
tabs/competidores.py
Página Competidores: ranking de vendedores por catálogo en múltiples períodos.
"""
from __future__ import annotations
import json
from datetime import date, timedelta
from typing import Any, Dict, List, Optional
from nicegui import app, run, ui
from db import get_connection


MESES = ["Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic"]

_LVL = {
    "1_green":  "🟢 Platinum",
    "2_green":  "🟢 Gold",
    "3_green":  "🟡 Silver",
    "4_green":  "⚪ Bronze",
    "5_yellow": "🟡 Mercader",
    "6_red":    "🔴 Nuevo",
}


def _get_mis_seller_ids(user_id: int) -> set:
    conn = get_connection()
    rows = conn.execute(
        "SELECT raw_data FROM ml_credentials WHERE user_id=?",
        (user_id,)
    ).fetchall()
    conn.close()
    ids = set()
    for r in rows:
        raw = r["raw_data"]
        if not raw:
            continue
        try:
            uid = json.loads(raw).get("user_id")
            if uid:
                ids.add(str(uid))
        except Exception:
            pass
    return ids


def _get_catalogs_for_user(user_id: int) -> List[Dict]:
    conn = get_connection()
    rows = conn.execute("""
        SELECT sc.catalog_product_id, sc.sku,
               COUNT(DISTINCT cs.seller_id) as n_competidores,
               MAX(cs.snapshot_date) as ultimo_snapshot
        FROM sku_catalogos sc
        LEFT JOIN competidores_snapshots cs
            ON cs.catalog_product_id = sc.catalog_product_id AND cs.user_id = sc.user_id
        WHERE sc.user_id = ? AND sc.catalog_product_id IS NOT NULL AND sc.catalog_product_id != ''
        GROUP BY sc.catalog_product_id, sc.sku
        ORDER BY sc.sku
    """, (user_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _get_ranking(user_id: int, cpid: str, dias: Optional[int]) -> List[Dict]:
    """
    Ranking de vendedores para un catálogo.
    dias=None → histórico (último snapshot disponible)
    dias=N    → diferencia entre hoy y hace N días
    """
    conn = get_connection()

    if dias is None:
        # Histórico: usar el último snapshot, ordenar por seller_total_ventas
        rows = conn.execute("""
            SELECT seller_id, seller_nickname, seller_total_ventas as ventas,
                   seller_level_id, seller_power_status, price, item_id,
                   snapshot_date
            FROM competidores_snapshots
            WHERE user_id=? AND catalog_product_id=?
              AND snapshot_date = (
                  SELECT MAX(snapshot_date) FROM competidores_snapshots
                  WHERE user_id=? AND catalog_product_id=?
              )
            ORDER BY seller_total_ventas DESC
        """, (user_id, cpid, user_id, cpid)).fetchall()
    else:
        # Período: diferencia entre snapshot más reciente y el de hace N días
        fecha_desde = (date.today() - timedelta(days=dias)).isoformat()
        rows = conn.execute("""
            SELECT
                s1.seller_id,
                s1.seller_nickname,
                s1.seller_total_ventas - COALESCE(s0.seller_total_ventas, s1.seller_total_ventas) as ventas,
                s1.seller_level_id,
                s1.seller_power_status,
                s1.price,
                s1.item_id,
                s1.snapshot_date
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
            ORDER BY ventas DESC
        """, (fecha_desde, user_id, cpid, user_id, cpid)).fetchall()

    conn.close()
    return [dict(r) for r in rows]


def _render_ranking_table(rows: List[Dict], mis_ids: set, titulo: str, nota: str = ""):
    """Renderiza una tabla de ranking compacta."""
    if not rows:
        with ui.element("div").style(
            "border:0.5px solid #e2e8f0;border-radius:8px;overflow:hidden;flex:1;min-width:0"
        ):
            with ui.element("div").style("background:#2A7AC7;padding:6px 10px"):
                ui.label(titulo).style("font-size:11px;font-weight:500;color:#fff;display:block")
                ui.label(nota).style("font-size:9px;color:rgba(255,255,255,.7);display:block") if nota else None
            ui.label("Sin datos").style("font-size:11px;color:#9ca3af;padding:12px;display:block;text-align:center")
        return

    mi_puesto = None
    yo_en_lista = False

    with ui.element("div").style(
        "border:0.5px solid #e2e8f0;border-radius:8px;overflow:hidden;flex:1;min-width:0"
    ):
        # Header
        with ui.element("div").style("background:#2A7AC7;padding:6px 10px"):
            ui.label(titulo).style("font-size:11px;font-weight:500;color:#fff;display:block")
            if nota:
                ui.label(nota).style("font-size:9px;color:rgba(255,255,255,.7);display:block")

        # Tabla
        with ui.element("div").style("overflow-y:auto;max-height:calc(100vh - 300px)"):
            with ui.element("table").style("width:100%;border-collapse:collapse;font-size:11px"):
                with ui.element("thead"):
                    with ui.element("tr"):
                        for h, align in [("#","center"),("Vendedor","left"),("Ventas","right"),("Precio","right")]:
                            with ui.element("th").style(
                                f"padding:4px 6px;background:#EEF6FD;color:#185FA5;"
                                f"font-size:9px;font-weight:600;text-align:{align};"
                                f"position:sticky;top:0;z-index:2;border-bottom:0.5px solid #d0e8f8"
                            ):
                                ui.html(h)

                with ui.element("tbody"):
                    for i, r in enumerate(rows, 1):
                        sid = str(r.get("seller_id") or "")
                        es_mio = sid in mis_ids
                        if es_mio:
                            yo_en_lista = True
                            mi_puesto = i
                        ventas = r.get("ventas")
                        precio = r.get("price")
                        nick = r.get("seller_nickname") or f"ID {sid}"
                        nivel = _LVL.get(r.get("seller_level_id") or "", "")
                        bg = "background:#EEF6FD;" if es_mio else ("background:#f9fafb;" if i % 2 == 0 else "")

                        with ui.element("tr").style(bg):
                            # Posición
                            pos_color = "#ca6d00" if i == 1 else "#9b7e28" if i == 2 else "#7c7c7c" if i == 3 else ("#166534" if es_mio else "#9ca3af")
                            with ui.element("td").style(f"padding:3px 6px;text-align:center;border-bottom:0.5px solid #f1f5f9;font-weight:{'600' if i<=3 else '400'};color:{pos_color}"):
                                ui.html(str(i))
                            # Vendedor
                            with ui.element("td").style(f"padding:3px 6px;border-bottom:0.5px solid #f1f5f9;{'font-weight:500;color:#185FA5' if es_mio else ''}"):
                                lbl = ("⭐ " if es_mio else "") + nick[:25]
                                if nivel:
                                    lbl += f" {nivel}"
                                ui.label(lbl).style("font-size:10px;display:block;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:180px")
                            # Ventas
                            with ui.element("td").style(f"padding:3px 6px;text-align:right;border-bottom:0.5px solid #f1f5f9;font-weight:{'500' if es_mio else '400'}"):
                                if ventas is not None and ventas >= 0:
                                    ui.html(f"<span style='color:{'#185FA5' if es_mio else '#374151'}'>{int(ventas):,}".replace(",", ".") + "</span>")
                                else:
                                    ui.html("<span style='color:#9ca3af'>—</span>")
                            # Precio
                            with ui.element("td").style("padding:3px 6px;text-align:right;border-bottom:0.5px solid #f1f5f9;color:#6b7280"):
                                if precio:
                                    try:
                                        ui.html(f"${int(float(precio)):,}".replace(",","."))
                                    except Exception:
                                        ui.html("—")
                                else:
                                    ui.html("—")

        # Footer si no aparecemos
        if not yo_en_lista:
            with ui.element("div").style(
                "padding:5px 8px;background:#FEF3C7;border-top:0.5px solid #FDE68A;"
                "font-size:9px;color:#92400E"
            ):
                ui.html('<i class="ti ti-info-circle" style="font-size:10px;vertical-align:-1px"></i> No aparecés en este catálogo')


def build_tab_competidores() -> None:
    user = app.storage.user.get("user")
    if not user:
        ui.label("Debes iniciar sesión").classes("text-red-500 p-4")
        return
    uid = user["id"]
    mis_ids = _get_mis_seller_ids(uid)

    estado = {"cpid": None}
    contenido_ref: list = [None]

    catalogs = _get_catalogs_for_user(uid)

    PERIODOS = [
        ("Histórica",  None,  "Total acumulado"),
        ("Anual",      365,   "Últimos 365 días"),
        ("Mensual",    30,    "Últimos 30 días"),
        ("Semanal",    7,     "Últimos 7 días"),
        ("Diaria",     1,     "Últimas 24 hs"),
    ]

    def _pintar(cpid: str):
        contenido_ref[0].clear()
        with contenido_ref[0]:
            if not cpid:
                ui.label("Seleccioná un catálogo para ver el ranking.").style(
                    "font-size:13px;color:#9ca3af;padding:24px"
                )
                return

            # 5 tablas lado a lado
            with ui.element("div").style(
                "display:flex;gap:8px;align-items:flex-start;width:100%;overflow-x:auto"
            ):
                for titulo, dias, nota in PERIODOS:
                    rows = _get_ranking(uid, cpid, dias)
                    _render_ranking_table(rows, mis_ids, titulo, nota)

    async def _cargar():
        cpid = estado.get("cpid")
        _pintar(cpid or "")

    # ── Layout ────────────────────────────────────────────────────────────────
    with ui.element("div").style("padding:16px 20px 0"):
        with ui.row().style("gap:8px;align-items:flex-end;flex-wrap:wrap;margin-bottom:12px"):
            with ui.column().style("gap:3px"):
                ui.label("Catálogo / SKU").style("font-size:11px;color:var(--color-text-secondary)")
                opciones = {
                    c["catalog_product_id"]: f"{c['sku']} — {c['catalog_product_id']} ({c['n_competidores'] or 0} vendedores)"
                    for c in catalogs
                }
                sel = ui.select(
                    options=opciones,
                    value=None,
                    label="",
                ).props("dense outlined clearable use-input input-debounce=200").style(
                    "min-width:340px;font-size:12px"
                )
                def _on_sel(e):
                    estado["cpid"] = e.value
                    ui.timer(0.05, _cargar, once=True)
                sel.on_value_change(_on_sel)

            with ui.element("button").on(
                "click", lambda: ui.timer(0.05, _cargar, once=True)
            ).style(
                "height:34px;font-size:12px;font-weight:500;"
                "border:1px solid #2A7AC7;border-radius:4px;background:#2A7AC7;"
                "padding:0 14px;cursor:pointer;color:#FFFFFF;align-self:flex-end"
            ):
                ui.html('<i class="ti ti-refresh" style="font-size:13px;margin-right:4px"></i>Actualizar')

            if not catalogs:
                ui.label("No hay catálogos registrados. Abrí un popup de producto para que se sincronicen.").style(
                    "font-size:11px;color:#ca6d00;align-self:flex-end"
                )

    with ui.element("div").style("padding:0 20px 24px"):
        cont = ui.element("div").style("width:100%")
        contenido_ref[0] = cont
        with cont:
            ui.label("Seleccioná un catálogo para ver el ranking.").style(
                "font-size:13px;color:#9ca3af;padding:24px"
            )
