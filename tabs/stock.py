"""
tabs/stock.py
Página Stock: evolución histórica de stock por SKU.
"""
from __future__ import annotations
import json
from datetime import date, timedelta
from typing import Any, Dict, List, Optional
from nicegui import app, run, ui
from db import get_connection


def _get_skus(user_id: int) -> List[str]:
    conn = get_connection()
    rows = conn.execute("""
        SELECT DISTINCT seller_sku FROM ml_stock_snapshots
        WHERE user_id=? AND seller_sku IS NOT NULL AND seller_sku != ''
        ORDER BY seller_sku
    """, (user_id,)).fetchall()
    conn.close()
    return [r[0] for r in rows]


def _get_stock_history(user_id: int, sku: str, fecha_desde: str, fecha_hasta: str) -> List[Dict[str, Any]]:
    conn = get_connection()
    rows = conn.execute("""
        SELECT snapshot_date, MAX(available_qty) as stock, MAX(price) as price
        FROM ml_stock_snapshots
        WHERE user_id=? AND seller_sku=?
          AND snapshot_date BETWEEN ? AND ?
        GROUP BY snapshot_date
        ORDER BY snapshot_date ASC
    """, (user_id, sku, fecha_desde, fecha_hasta)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _calcular_metricas(rows: List[Dict]) -> Dict[str, Any]:
    if not rows:
        return {}
    ventas_total = 0
    dias_con_stock = 0
    for i in range(1, len(rows)):
        stock_prev = rows[i-1].get('stock') or 0
        stock_curr = rows[i].get('stock') or 0
        if stock_prev > stock_curr:
            ventas_total += stock_prev - stock_curr
        if stock_prev > 0:
            dias_con_stock += 1
    vel = round(ventas_total / dias_con_stock, 1) if dias_con_stock > 0 else 0
    stock_actual = rows[-1].get('stock') or 0
    dias_restantes = round(stock_actual / vel) if vel > 0 and stock_actual > 0 else None
    return {
        'ventas_total': ventas_total,
        'vel_diaria': vel,
        'dias_restantes': dias_restantes,
        'stock_actual': stock_actual,
        'precio_actual': rows[-1].get('price'),
    }


def _fmt_precio(v):
    if v is None:
        return '—'
    try:
        return f"${int(float(v)):,}".replace(',', '.')
    except Exception:
        return str(v)


def build_tab_stock() -> None:
    user = app.storage.user.get("user")
    if not user:
        ui.label("Debes iniciar sesión").classes("text-red-500 p-4")
        return
    user_id = user["id"]

    estado = {"sku": None, "desde": None, "hasta": None}
    contenido_ref: list = [None]

    skus = _get_skus(user_id)

    hoy = date.today()
    default_desde = (hoy - timedelta(days=29)).isoformat()
    default_hasta = hoy.isoformat()

    def _pintar(rows: List[Dict], metricas: Dict, sku: str):
        from datetime import datetime as _dt
        contenido_ref[0].clear()
        with contenido_ref[0]:
            if not rows:
                ui.label("No hay datos para este SKU y período.").style(
                    "font-size:13px;color:#9ca3af;padding:24px"
                )
                return

            vel       = metricas.get('vel_diaria', 0)
            dias_r    = metricas.get('dias_restantes')
            dias_col  = "#dc2626" if dias_r and dias_r < 7 else "#ca6d00" if dias_r and dias_r < 20 else "#166534"

            # ── Stats ──────────────────────────────────────────────────────
            stats = [
                ("Stock actual",        str(metricas.get('stock_actual', '—')), "#185FA5"),
                ("Vendidas en período",  str(metricas.get('ventas_total', '—')), "#dc2626"),
                ("Vel. promedio",       f"{vel}/día",                            "#374151"),
                ("Días restantes",      str(dias_r or '—'),                      dias_col),
                ("Precio actual",       _fmt_precio(metricas.get('precio_actual')), "#374151"),
            ]
            with ui.element("div").style(
                "display:flex;gap:0;border:0.5px solid #e2e8f0;border-radius:8px;"
                "overflow:hidden;margin-bottom:10px;background:var(--color-background-primary)"
            ):
                for i, (lbl, val, color) in enumerate(stats):
                    brd = "border-left:0.5px solid #e2e8f0;" if i else ""
                    with ui.element("div").style(f"flex:1;padding:8px 12px;text-align:center;{brd}"):
                        ui.label(val).style(f"font-size:16px;font-weight:500;color:{color};display:block;line-height:1.2")
                        ui.label(lbl).style("font-size:9px;color:#9ca3af;text-transform:uppercase;letter-spacing:0.04em;margin-top:2px;display:block")

            # ── Calcular ventas/repos en orden cronológico ─────────────────
            MESES = ["Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic"]
            data = []
            for i, r in enumerate(rows):
                stock_hoy  = r.get('stock') or 0
                stock_ayer = rows[i-1].get('stock') or 0 if i > 0 else stock_hoy
                if stock_hoy > stock_ayer:
                    repo, vend = stock_hoy - stock_ayer, 0
                else:
                    repo, vend = 0, max(0, stock_ayer - stock_hoy)
                data.append({**r, 'vend': vend, 'repo': repo})

            # ── Layout: tabla izquierda + gráfico derecha ─────────────────
            MESES_CORTO = ["Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic"]
            with ui.element("div").style(
                "display:grid;grid-template-columns:320px 1fr;gap:12px;align-items:start"
            ):
                # ── Tabla ─────────────────────────────────────────────────
                with ui.element("div").style(
                    "border:0.5px solid #e2e8f0;border-radius:8px;overflow:hidden"
                ):
                    with ui.element("div").style("overflow-y:auto;max-height:calc(100vh - 320px)"):
                        with ui.element("table").style("width:100%;border-collapse:collapse;font-size:11px"):
                            with ui.element("thead"):
                                with ui.element("tr"):
                                    for h in ["Día", "Stock", "Vendidas", "Vel. día", "Precio"]:
                                        with ui.element("th").style(
                                            "padding:5px 8px;background:#2A7AC7;color:#fff;"
                                            "font-weight:500;text-align:center;white-space:nowrap;"
                                            "border-right:0.5px solid rgba(255,255,255,0.15);"
                                            "position:sticky;top:0;z-index:2"
                                        ):
                                            ui.html(h)
                            with ui.element("tbody"):
                                from datetime import datetime as _dt2
                                cur_mes = None
                                es_primera = True
                                for r in reversed(data):
                                    fecha_str = r['snapshot_date']
                                    try:
                                        d = _dt2.strptime(fecha_str, "%Y-%m-%d")
                                        mes_key   = f"{d.year}-{d.month:02d}"
                                        mes_label = f"{MESES_CORTO[d.month-1]} {d.year}"
                                        dia_label = str(d.day)
                                    except Exception:
                                        mes_key = mes_label = fecha_str
                                        dia_label = fecha_str

                                    if mes_key != cur_mes:
                                        cur_mes = mes_key
                                        with ui.element("tr"):
                                            with ui.element("td").style(
                                                "padding:3px 8px;background:#EEF6FD;"
                                                "border-bottom:0.5px solid #d0e8f8;"
                                                "font-size:10px;font-weight:600;color:#185FA5"
                                            ).props('colspan="5"'):
                                                ui.html(mes_label)

                                    stock  = r.get('stock') or 0
                                    vend   = r['vend']
                                    repo   = r['repo']
                                    precio = _fmt_precio(r.get('price'))

                                    bg = "background:#F0FDF4;" if repo > 0 else ""
                                    with ui.element("tr").style(bg):
                                        with ui.element("td").style("padding:3px 8px;border-bottom:0.5px solid #f1f5f9;text-align:center;color:#6b7280"):
                                            ui.html(dia_label)
                                        with ui.element("td").style(f"padding:3px 8px;border-bottom:0.5px solid #f1f5f9;text-align:right;font-weight:500;color:{'#166534' if stock > 0 else '#9ca3af'}"):
                                            ui.html(str(stock))
                                        if repo > 0:
                                            vc, vt = "#166534", f"+{repo}"
                                        elif vend > 0:
                                            vc, vt = "#dc2626", f"−{vend}"
                                        else:
                                            vc, vt = "#9ca3af", "—"
                                        with ui.element("td").style(f"padding:3px 8px;border-bottom:0.5px solid #f1f5f9;text-align:right;font-weight:500;color:{vc}"):
                                            ui.html(vt)
                                        with ui.element("td").style("padding:3px 8px;border-bottom:0.5px solid #f1f5f9;text-align:right;color:#9ca3af"):
                                            ui.html(f"{vend}/d" if vend > 0 else "—")
                                        with ui.element("td").style("padding:3px 8px;border-bottom:0.5px solid #f1f5f9;text-align:right;color:#374151"):
                                            ui.html(precio)
                                    es_primera = False

                # ── Gráfico ────────────────────────────────────────────────
                with ui.element("div").style("display:flex;flex-direction:column;gap:8px"):
                    # Días restantes
                    if dias_r:
                        ui.label(
                            f"Con el stock actual ({metricas.get('stock_actual')} uds.) "
                            f"y una vel. de {vel}/día → quedan estimados {dias_r} días de stock."
                        ).style(f"font-size:11px;color:{dias_col};display:block")

                    # Pre-procesar etiquetas: mostrar solo cada N días
                    total_pts = len(rows)
                    intervalo = max(1, total_pts // 10)
                    chart_labels = []
                    for i, r in enumerate(rows):
                        if i % intervalo == 0 or i == total_pts - 1:
                            try:
                                d = _dt.strptime(r['snapshot_date'], "%Y-%m-%d")
                                chart_labels.append(f"{d.day} {MESES_CORTO[d.month-1]}")
                            except Exception:
                                chart_labels.append(r['snapshot_date'])
                        else:
                            chart_labels.append("")

                    valores = [r.get('stock') or 0 for r in rows]
                    ui.echart({
                        "grid": {"top": 16, "bottom": 32, "left": 42, "right": 8},
                        "xAxis": {
                            "type": "category",
                            "data": chart_labels,
                            "axisLabel": {
                                "fontSize": 10,
                                "color": "#9ca3af",
                                "interval": 0,
                                "rotate": 0,
                            },
                            "axisLine": {"lineStyle": {"color": "#e2e8f0"}},
                            "axisTick": {"show": False},
                        },
                        "yAxis": {
                            "type": "value",
                            "axisLabel": {"fontSize": 10, "color": "#9ca3af"},
                            "splitLine": {"lineStyle": {"color": "#f1f5f9", "type": "dashed"}},
                            "min": 0,
                        },
                        "series": [{
                            "type": "line",
                            "data": valores,
                            "smooth": 0.2,
                            "lineStyle": {"color": "#2A7AC7", "width": 2},
                            "itemStyle": {"color": "#2A7AC7"},
                            "areaStyle": {
                                "color": {
                                    "type": "linear", "x": 0, "y": 0, "x2": 0, "y2": 1,
                                    "colorStops": [
                                        {"offset": 0, "color": "rgba(42,122,199,0.20)"},
                                        {"offset": 1, "color": "rgba(42,122,199,0.01)"},
                                    ]
                                }
                            },
                            "symbolSize": 4,
                        }],
                        "tooltip": {
                            "trigger": "axis",
                            "formatter": "{b}<br/>Stock: <b>{c}</b>",
                        },
                    }).style("height:calc(100vh - 320px);width:100%")

    async def _cargar():
        sku = estado.get("sku")
        desde = estado.get("desde") or default_desde
        hasta = estado.get("hasta") or default_hasta
        if not sku:
            contenido_ref[0].clear()
            with contenido_ref[0]:
                ui.label("Seleccioná un SKU para ver el historial.").style(
                    "font-size:13px;color:#9ca3af;padding:24px"
                )
            return
        rows = await run.io_bound(_get_stock_history, user_id, sku, desde, hasta)
        metricas = _calcular_metricas(rows)
        _pintar(rows, metricas, sku)

    # Layout
    with ui.element("div").style("padding:16px 20px 0"):
        # Controles
        with ui.row().style("gap:8px;align-items:flex-end;flex-wrap:wrap;margin-bottom:16px"):
            with ui.column().style("gap:3px"):
                ui.label("SKU").style("font-size:11px;color:var(--color-text-secondary)")
                sel_sku = ui.select(
                    options=skus,
                    value=None,
                    label="",
                ).props("dense outlined clearable use-input input-debounce=200").style(
                    "width:220px;font-size:12px"
                )
                def _on_sku(e):
                    estado["sku"] = e.value
                    ui.timer(0.05, _cargar, once=True)
                sel_sku.on_value_change(_on_sku)

            with ui.column().style("gap:3px"):
                ui.label("Desde").style("font-size:11px;color:var(--color-text-secondary)")
                inp_desde = ui.input(value=default_desde).props("type=date dense outlined").style("width:140px")
                def _on_desde(e):
                    estado["desde"] = e.value
                inp_desde.on_value_change(_on_desde)

            with ui.column().style("gap:3px"):
                ui.label("Hasta").style("font-size:11px;color:var(--color-text-secondary)")
                inp_hasta = ui.input(value=default_hasta).props("type=date dense outlined").style("width:140px")
                def _on_hasta(e):
                    estado["hasta"] = e.value
                inp_hasta.on_value_change(_on_hasta)

            with ui.element("button").on(
                "click", lambda: ui.timer(0.05, _cargar, once=True)
            ).style(
                "height:34px;font-size:12px;font-weight:500;"
                "border:1px solid #2A7AC7;border-radius:4px;background:#2A7AC7;"
                "padding:0 16px;cursor:pointer;color:#FFFFFF;align-self:flex-end"
            ):
                ui.html('<i class="ti ti-refresh" style="font-size:13px;margin-right:4px"></i>Actualizar')

        contenido = ui.column().style("width:100%;gap:0")
        contenido_ref[0] = contenido
        with contenido:
            ui.label("Seleccioná un SKU para ver el historial.").style(
                "font-size:13px;color:#9ca3af;padding:24px"
            )
