"""
tabs/stock.py
P¡gina Stock: evoluciÃ³n histÃ³rica de stock por SKU.
"""
from __future__ import annotations
from datetime import date, timedelta
from typing import Any, Dict, List
from nicegui import app, run, ui
from db import get_connection

MESES = ["Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic"]


def _get_skus(user_id: int) -> List[str]:
    conn = get_connection()
    rows = conn.execute("""
        SELECT DISTINCT seller_sku FROM ml_stock_snapshots
        WHERE user_id=? AND seller_sku IS NOT NULL AND seller_sku != ''
        ORDER BY seller_sku
    """, (user_id,)).fetchall()
    conn.close()
    return [r[0] for r in rows]


def _get_stock_history(user_id: int, sku: str, desde: str, hasta: str) -> List[Dict[str, Any]]:
    conn = get_connection()
    rows = conn.execute("""
        SELECT snapshot_date,
               MAX(available_qty) as stock,
               MAX(price)         as price
        FROM ml_stock_snapshots
        WHERE user_id=? AND seller_sku=?
          AND snapshot_date BETWEEN ? AND ?
        GROUP BY snapshot_date
        ORDER BY snapshot_date ASC
    """, (user_id, sku, desde, hasta)).fetchall()
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
        return 'â'
    try:
        return f"${int(float(v)):,}".replace(',', '.')
    except Exception:
        return str(v)


def build_tab_stock() -> None:
    user = app.storage.user.get("user")
    if not user:
        ui.label("Debes iniciar sesiÃ³n").classes("text-red-500 p-4")
        return
    user_id = user["id"]

    hoy = date.today()
    estado = {
        "sku": None,
        "desde": (hoy - timedelta(days=29)).isoformat(),
        "hasta": hoy.isoformat(),
    }
    contenido_ref: list = [None]
    skus = _get_skus(user_id)

    def _pintar(rows: List[Dict], metricas: Dict, sku: str):
        from datetime import datetime as _dt
        contenido_ref[0].clear()
        with contenido_ref[0]:
            if not rows:
                ui.label("Sin datos para este SKU y perÃ­odo.").style(
                    "font-size:13px;color:#9ca3af;padding:24px"
                )
                return

            vel     = metricas.get('vel_diaria', 0)
            dias_r  = metricas.get('dias_restantes')
            dias_col = "#dc2626" if dias_r and dias_r < 7 else "#ca6d00" if dias_r and dias_r < 20 else "#166534"

            # ââ Stats ââââââââââââââââââââââââââââââââââââââââââââââââââââ
            stats = [
                ("Stock actual",             str(metricas.get('stock_actual', 'â')), "#185FA5"),
                ("Vendidas en perÃ­odo",       str(metricas.get('ventas_total', 'â')), "#dc2626"),
                ("Vel. venta promedio",       f"{vel}/dÃ­a",                           "#374151"),
                ("DÃ­as de stock restantes",   str(dias_r or 'â'),                     dias_col),
                ("Precio actual",             _fmt_precio(metricas.get('precio_actual')), "#374151"),
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

            # ââ Calcular ventas/repos + vel. acumulada cronolÃ³gicamente ââ
            data = []
            running_sales = 0
            running_stock_days = 0
            for i, r in enumerate(rows):
                stock_hoy  = r.get('stock') or 0
                stock_ayer = rows[i-1].get('stock') or 0 if i > 0 else stock_hoy
                if stock_hoy > stock_ayer:
                    repo, vend = stock_hoy - stock_ayer, 0
                else:
                    repo, vend = 0, max(0, stock_ayer - stock_hoy)
                if stock_ayer > 0:
                    running_stock_days += 1
                running_sales += vend
                vel_acum = round(running_sales / running_stock_days, 1) if running_stock_days > 0 else None
                data.append({**r, 'vend': vend, 'repo': repo, 'vel_acum': vel_acum})

            # ââ Layout: tabla izquierda (300px) + grÃ¡fico derecha âââââââââ
            with ui.element("div").style(
                "display:grid;grid-template-columns:300px 1fr;gap:10px;align-items:start"
            ):
                # ââ Tabla compacta con scroll ââââââââââââââââââââââââââââââ
                with ui.element("div").style(
                    "border:0.5px solid #e2e8f0;border-radius:8px;overflow:hidden"
                ):
                    with ui.element("div").style("overflow-y:auto;max-height:45vh"):
                        with ui.element("table").style("width:100%;border-collapse:collapse;font-size:11px"):
                            with ui.element("thead"):
                                with ui.element("tr"):
                                    for h in ["DÃ­a", "Stock", "Vendidas", "Vel. acum.", "Precio"]:
                                        with ui.element("th").style(
                                            "padding:5px 8px;background:#2A7AC7;color:#fff;"
                                            "font-weight:500;text-align:center;white-space:nowrap;"
                                            "border-right:0.5px solid rgba(255,255,255,0.15);"
                                            "position:sticky;top:0;z-index:2"
                                        ):
                                            ui.html(h)
                            with ui.element("tbody"):
                                cur_mes = None
                                for r in reversed(data):
                                    fecha_str = r['snapshot_date']
                                    try:
                                        d = _dt.strptime(fecha_str, "%Y-%m-%d")
                                        mes_key   = f"{d.year}-{d.month:02d}"
                                        mes_label = f"{MESES[d.month-1]} {d.year}"
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
                                    va     = r.get('vel_acum')
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
                                            vc, vt = "#dc2626", f"â{vend}"
                                        else:
                                            vc, vt = "#9ca3af", "â"
                                        with ui.element("td").style(f"padding:3px 8px;border-bottom:0.5px solid #f1f5f9;text-align:right;font-weight:500;color:{vc}"):
                                            ui.html(vt)
                                        with ui.element("td").style("padding:3px 8px;border-bottom:0.5px solid #f1f5f9;text-align:right;color:#6b7280"):
                                            ui.html(f"{va}/d" if va is not None else "â")
                                        with ui.element("td").style("padding:3px 8px;border-bottom:0.5px solid #f1f5f9;text-align:right;color:#374151"):
                                            ui.html(precio)

                # ââ GrÃ¡fico (todo el ancho restante, misma altura) âââââââââ
                with ui.element("div").style("display:flex;flex-direction:column;gap:6px"):
                    if dias_r:
                        ui.label(
                            f"Con {metricas.get('stock_actual')} uds. y vel. {vel}/dÃ­a â "
                            f"estimados {dias_r} dÃ­as de stock restantes."
                        ).style(f"font-size:11px;color:{dias_col};display:block")

                    # Etiquetas inteligentes: dÃ­a / "1 Jul" / "1 Ene 2026"
                    chart_labels = []
                    prev_m, prev_y = None, None
                    multi_anio = len(set(r['snapshot_date'][:4] for r in rows)) > 1

                    for i, r in enumerate(rows):
                        try:
                            d = _dt.strptime(r['snapshot_date'], "%Y-%m-%d")
                        except Exception:
                            chart_labels.append(""); continue
                        y_chg = prev_y is not None and d.year != prev_y
                        m_chg = prev_m is not None and d.month != prev_m

                        if i == 0:
                            lbl = f"{d.day} {MESES[d.month-1]} {d.year}"
                        elif y_chg:
                            lbl = f"{d.day} {MESES[d.month-1]} {d.year}"
                        elif m_chg:
                            lbl = f"{d.day} {MESES[d.month-1]}"
                        else:
                            lbl = str(d.day)

                        chart_labels.append(lbl)
                        prev_m, prev_y = d.month, d.year

                    valores = [r.get('stock') or 0 for r in rows]
                    ui.echart({
                        "grid": {"top": 16, "bottom": 40, "left": 42, "right": 8},
                        "xAxis": {
                            "type": "category",
                            "data": chart_labels,
                            "axisLabel": {"fontSize": 10, "color": "#9ca3af", "interval": 0},
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
                            "symbolSize": 5,
                        }],
                        "tooltip": {"trigger": "axis", "formatter": "{b}<br/>Stock: <b>{c}</b>"},
                    }).style("height:45vh;width:100%")

    async def _cargar():
        sku = estado.get("sku")
        if not sku:
            contenido_ref[0].clear()
            with contenido_ref[0]:
                ui.label("SeleccionÃ¡ un SKU para ver el historial.").style(
                    "font-size:13px;color:#9ca3af;padding:24px"
                )
            return
        rows = await run.io_bound(
            _get_stock_history, user_id, sku, estado["desde"], estado["hasta"]
        )
        met = _calcular_metricas(rows)
        _pintar(rows, met, sku)

    # ââ Layout principal ââââââââââââââââââââââââââââââââââââââââââââââââââ
    with ui.element("div").style("padding:16px 20px 0"):
        with ui.row().style("gap:8px;align-items:flex-end;flex-wrap:wrap;margin-bottom:12px"):
            with ui.column().style("gap:3px"):
                ui.label("SKU").style("font-size:11px;color:var(--color-text-secondary)")
                sel = ui.select(options=skus, value=None, label="").props(
                    "dense outlined clearable use-input input-debounce=200"
                ).style("width:240px;font-size:12px")
                def _on_sku(e):
                    estado["sku"] = e.value
                    ui.timer(0.05, _cargar, once=True)
                sel.on_value_change(_on_sku)

            with ui.column().style("gap:3px"):
                ui.label("Desde").style("font-size:11px;color:var(--color-text-secondary)")
                ui.input(value=estado["desde"]).props("type=date dense outlined").style(
                    "width:140px"
                ).on_value_change(lambda e: estado.update(desde=e.value))

            with ui.column().style("gap:3px"):
                ui.label("Hasta").style("font-size:11px;color:var(--color-text-secondary)")
                ui.input(value=estado["hasta"]).props("type=date dense outlined").style(
                    "width:140px"
                ).on_value_change(lambda e: estado.update(hasta=e.value))

            with ui.element("button").on(
                "click", lambda: ui.timer(0.05, _cargar, once=True)
            ).style(
                "height:34px;font-size:12px;font-weight:500;"
                "border:1px solid #2A7AC7;border-radius:4px;background:#2A7AC7;"
                "padding:0 16px;cursor:pointer;color:#FFFFFF;align-self:flex-end"
            ):
                ui.html('<i class="ti ti-refresh" style="font-size:13px;margin-right:4px"></i>Actualizar')

        cont = ui.element("div").style("width:100%")
        contenido_ref[0] = cont
        with cont:
            ui.label("SeleccionÃ¡ un SKU para ver el historial.").style(
                "font-size:13px;color:#9ca3af;padding:24px"
            )
