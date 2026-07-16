"""
tabs/stock.py
Pagina Stock: evolucion historica de stock por SKU.
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


def _get_marcas(user_id: int) -> List[str]:
    conn = get_connection()
    rows = conn.execute("""
        SELECT DISTINCT p.marca FROM productos p
        INNER JOIN ml_stock_snapshots s ON p.sku = s.seller_sku AND p.user_id = s.user_id
        WHERE p.user_id=? AND p.marca IS NOT NULL
        ORDER BY p.marca
    """, (user_id,)).fetchall()
    conn.close()
    return [r[0] for r in rows]


def _get_stock_history_marca(user_id: int, marca: str, desde: str, hasta: str) -> List[Dict[str, Any]]:
    """Igual que _get_stock_history pero sumado entre todos los SKUs de la marca.
    Colapsa primero por (seller_sku, snapshot_date) con MAX -- un mismo seller_sku puede
    tener mas de un item_id reportando stock el mismo dia -- y recien despues suma entre
    SKUs, para no sobrecontar (un SUM directo sobre ml_stock_snapshots llega a sobrecontar
    7-8x en SKUs con varios item_id). Precio = promedio ponderado por stock, no tiene
    sentido sumar precios."""
    conn = get_connection()
    rows = conn.execute("""
        WITH per_sku_dia AS (
            SELECT s.snapshot_date, s.seller_sku,
                   MAX(s.available_qty) AS stock,
                   MAX(s.price)         AS price
            FROM ml_stock_snapshots s
            INNER JOIN productos p ON s.seller_sku = p.sku AND s.user_id = p.user_id
            WHERE p.marca=? AND s.user_id=? AND s.snapshot_date BETWEEN ? AND ?
            GROUP BY s.snapshot_date, s.seller_sku
        )
        SELECT snapshot_date,
               SUM(stock) AS stock,
               SUM(price * stock) * 1.0 / NULLIF(SUM(stock), 0) AS price
        FROM per_sku_dia
        GROUP BY snapshot_date
        ORDER BY snapshot_date ASC
    """, (marca, user_id, desde, hasta)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _get_marca_n_skus(user_id: int, marca: str, desde: str, hasta: str) -> int:
    conn = get_connection()
    row = conn.execute("""
        SELECT COUNT(DISTINCT s.seller_sku)
        FROM ml_stock_snapshots s
        INNER JOIN productos p ON s.seller_sku = p.sku AND s.user_id = p.user_id
        WHERE p.marca=? AND s.user_id=? AND s.snapshot_date BETWEEN ? AND ?
    """, (marca, user_id, desde, hasta)).fetchone()
    conn.close()
    return row[0] if row else 0


def _calcular_metricas(rows: List[Dict]) -> Dict[str, Any]:
    if not rows:
        return {}
    ventas_total = 0
    dias_con_stock = 0
    for i in range(1, len(rows)):
        stock_prev = rows[i-1].get("stock") or 0
        stock_curr = rows[i].get("stock") or 0
        if stock_prev > stock_curr:
            ventas_total += stock_prev - stock_curr
        if stock_prev > 0:
            dias_con_stock += 1
    vel = round(ventas_total / dias_con_stock, 1) if dias_con_stock > 0 else 0
    stock_actual = rows[-1].get("stock") or 0
    dias_restantes = round(stock_actual / vel) if vel > 0 and stock_actual > 0 else None
    return {
        "ventas_total": ventas_total,
        "vel_diaria": vel,
        "dias_restantes": dias_restantes,
        "stock_actual": stock_actual,
        "precio_actual": rows[-1].get("price"),
        # Precio
        "precio_min":  min((float(r["price"]) for r in rows if r.get("price")), default=None),
        "precio_max":  max((float(r["price"]) for r in rows if r.get("price")), default=None),
        "precio_prom": (lambda vals: round(sum(vals)/len(vals)) if vals else None)(
            [float(r["price"]) for r in rows if r.get("price")]
        ),
        # Stock
        "stock_max":  max((r.get("stock") or 0 for r in rows), default=0),
        "stock_min":  min((r.get("stock") or 0 for r in rows), default=0),
        "stock_prom": round(sum(r.get("stock") or 0 for r in rows) / len(rows), 1) if rows else 0,
        # Dias
        "dias_con_stock":  sum(1 for r in rows if (r.get("stock") or 0) > 0),
        "dias_sin_stock":  sum(1 for r in rows if (r.get("stock") or 0) == 0),
        "n_reposiciones":  sum(1 for i in range(1, len(rows))
                              if (rows[i].get("stock") or 0) > (rows[i-1].get("stock") or 0)),
        "vel_max_dia":     max((
                              max(0, (rows[i-1].get("stock") or 0) - (rows[i].get("stock") or 0))
                              for i in range(1, len(rows))
                           ), default=0),
    }


def _fmt_precio(v):
    if v is None:
        return "—"
    try:
        return "$" + f"{int(float(v)):,}".replace(",", ".")
    except Exception:
        return str(v)


def build_tab_stock() -> None:
    user = app.storage.user.get("user")
    if not user:
        ui.label("Debes iniciar sesion").classes("text-red-500 p-4")
        return
    user_id = user["id"]

    hoy = date.today()
    estado = {
        "sku": None,
        "marca": None,
        "desde": (hoy - timedelta(days=29)).isoformat(),
        "hasta": hoy.isoformat(),
    }
    contenido_ref: list = [None]
    skus = _get_skus(user_id)
    marcas = _get_marcas(user_id)

    def _pintar(rows: List[Dict], metricas: Dict, sku: str = None, marca: str = None, n_skus: int = None):
        from datetime import datetime as _dt
        contenido_ref[0].clear()
        with contenido_ref[0]:
            if not rows:
                msg = "Sin datos para esta marca y periodo." if marca else "Sin datos para este SKU y periodo."
                ui.label(msg).style(
                    "font-size:13px;color:#9ca3af;padding:24px"
                )
                return

            if marca:
                ui.label(
                    f"Vista agregada — marca: {marca} · {n_skus} SKUs con historial · "
                    "Precio = promedio ponderado por stock."
                ).style("font-size:11px;color:#185FA5;margin-bottom:6px;display:block")

            vel    = metricas.get("vel_diaria", 0)
            dias_r = metricas.get("dias_restantes")
            dc     = "#dc2626" if dias_r and dias_r < 7 else "#ca6d00" if dias_r and dias_r < 20 else "#166534"
            dc_bg  = "#FEE2E2" if dias_r and dias_r < 7 else "#FEF3C7" if dias_r and dias_r < 20 else "#DCFCE7"
            dc_br  = "#FCA5A5" if dias_r and dias_r < 7 else "#FDE68A" if dias_r and dias_r < 20 else "#86EFAC"

            p_min  = _fmt_precio(metricas.get("precio_min"))
            p_max  = _fmt_precio(metricas.get("precio_max"))
            p_prom = _fmt_precio(metricas.get("precio_prom"))
            p_iguales = p_min == p_max

            # ── Opcion C: pills de colores + linea de detalle ────────────
            with ui.element("div").style(
                "background:var(--color-background-primary);border:0.5px solid #e2e8f0;"
                "border-radius:8px;padding:8px 12px;margin-bottom:8px"
            ):
                # Fila 1: pills
                with ui.element("div").style("display:flex;flex-wrap:wrap;gap:6px;align-items:center;margin-bottom:6px"):
                    for icon, val, lbl, bg, border, color in [
                        ("ti-package",      str(metricas.get("stock_actual", "\u2014")), "stock",    "#E6F1FB", "#85B7EB", "#0C447C"),
                        ("ti-shopping-cart",str(metricas.get("ventas_total", "\u2014")), "vendidas", "#FEE2E2", "#FCA5A5", "#991B1B"),
                        ("ti-trending-up",  f"{vel}/d",                                 "vel.",     "var(--color-background-secondary)", "#e2e8f0", "#374151"),
                        ("ti-clock",        str(dias_r or "\u2014"),                    "dias rest.",dc_bg,    dc_br,     dc),
                        ("ti-tag",          _fmt_precio(metricas.get("precio_actual")), "",         "var(--color-background-secondary)", "#e2e8f0", "#374151"),
                    ]:
                        with ui.element("div").style(
                            f"display:inline-flex;align-items:center;gap:4px;"
                            f"background:{bg};border:0.5px solid {border};"
                            f"border-radius:20px;padding:4px 10px"
                        ):
                            ui.html(f'<i class="ti {icon}" style="font-size:12px;color:{color}" aria-hidden="true"></i>')
                            ui.label(val).style(f"font-size:12px;font-weight:500;color:{color}")
                            if lbl:
                                ui.label(lbl).style(f"font-size:10px;color:{color};opacity:.7")

                # Fila 2: detalle compacto
                p_detalle = p_min if p_iguales else f"{p_min} min \u00b7 {p_max} max \u00b7 {p_prom} prom"
                s_detalle = f"{metricas.get('stock_max',0)} max \u00b7 {metricas.get('stock_prom',0)} prom \u00b7 <span style='color:#dc2626'>{metricas.get('vel_max_dia',0)}/d max</span>"
                d_detalle = f"<span style='color:#166534'>{metricas.get('dias_con_stock',0)}</span> c/stock \u00b7 <span style='color:#dc2626'>{metricas.get('dias_sin_stock',0)}</span> sin \u00b7 <span style='color:#185FA5'>{metricas.get('n_reposiciones',0)}</span> repos"
                ui.html(
                    f'<div style="font-size:10px;color:#9ca3af;display:flex;flex-wrap:wrap;gap:4px;align-items:center">'
                    f'<span style="font-weight:500;color:#185FA5">Precio:</span>'
                    f'<span style="color:#374151">{p_detalle}</span>'
                    f'<span style="color:#d0d0d0">\u00b7\u00b7</span>'
                    f'<span style="font-weight:500;color:#185FA5">Stock:</span>'
                    f'<span style="color:#374151">{s_detalle}</span>'
                    f'<span style="color:#d0d0d0">\u00b7\u00b7</span>'
                    f'<span style="font-weight:500;color:#185FA5">Dias:</span>'
                    f'<span>{d_detalle}</span>'
                    f'</div>'
                )

            # Calcular ventas/repos + vel acumulada
            data = []
            running_sales = 0
            running_stock_days = 0
            for i, r in enumerate(rows):
                stock_hoy  = r.get("stock") or 0
                stock_ayer = rows[i-1].get("stock") or 0 if i > 0 else stock_hoy
                if stock_hoy > stock_ayer:
                    repo, vend = stock_hoy - stock_ayer, 0
                else:
                    repo, vend = 0, max(0, stock_ayer - stock_hoy)
                if stock_ayer > 0:
                    running_stock_days += 1
                running_sales += vend
                vel_acum = round(running_sales / running_stock_days, 1) if running_stock_days > 0 else None
                data.append({**r, "vend": vend, "repo": repo, "vel_acum": vel_acum})

            # Etiquetas del grafico
            chart_labels = []
            prev_m, prev_y = None, None
            for i, r in enumerate(rows):
                try:
                    d = _dt.strptime(r["snapshot_date"], "%Y-%m-%d")
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

            valores  = [r.get("stock") or 0 for r in rows]
            precios  = [round(float(r["price"])/1000, 1) if r.get("price") else None for r in rows]

            # Serie de precio: etiquetar solo cuando cambia el valor
            precio_serie = []
            prev_p = None
            for p in precios:
                if p is not None and p != prev_p:
                    precio_serie.append({
                        "value": p,
                        "label": {
                            "show": True,
                            "formatter": f"${p}k",
                            "color": "#EF9F27",
                            "fontSize": 10,
                            "fontWeight": "bold",
                            "position": "insideEndTop",
                            "backgroundColor": "rgba(255,255,255,0.85)",
                            "padding": [2, 4],
                            "borderRadius": 3,
                        }
                    })
                    prev_p = p
                else:
                    precio_serie.append({"value": p, "label": {"show": False}})

            # Grid: tabla fija izquierda + grafico ancho completo derecha
            with ui.element("div").style(
                "display:grid;grid-template-columns:390px 1fr;gap:0;align-items:start;width:100%"
            ):
                # Tabla
                with ui.element("div").style(
                    "border:0.5px solid #e2e8f0;border-radius:8px 0 0 8px;overflow:hidden;margin-right:10px"
                ):
                    with ui.element("div").style("overflow-y:auto;max-height:calc(100vh - 450px)"):
                        with ui.element("table").style("width:100%;border-collapse:collapse;font-size:11px"):
                            with ui.element("thead"):
                                with ui.element("tr"):
                                    for h in ["Dia", "Stock", "Variacion", "Vel. acum.", "Precio"]:
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
                                    fecha_str = r["snapshot_date"]
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
                                    stock  = r.get("stock") or 0
                                    vend   = r["vend"]
                                    repo   = r["repo"]
                                    va     = r.get("vel_acum")
                                    precio = _fmt_precio(r.get("price"))
                                    bg = "background:#F0FDF4;" if repo > 0 else ""
                                    with ui.element("tr").style(bg):
                                        with ui.element("td").style("padding:3px 8px;border-bottom:0.5px solid #f1f5f9;text-align:center;color:#6b7280"):
                                            ui.html(dia_label)
                                        with ui.element("td").style(f"padding:3px 8px;border-bottom:0.5px solid #f1f5f9;text-align:right;font-weight:500;color:{'#166534' if stock > 0 else '#9ca3af'}"):
                                            ui.html(str(stock))
                                        if repo > 0:
                                            vc, vt = "#166534", f"+{repo}"
                                        elif vend > 0:
                                            vc, vt = "#dc2626", f"\u2212{vend}"
                                        else:
                                            vc, vt = "#9ca3af", "\u2014"
                                        with ui.element("td").style(f"padding:3px 8px;border-bottom:0.5px solid #f1f5f9;text-align:right;font-weight:500;color:{vc}"):
                                            ui.html(vt)
                                        with ui.element("td").style("padding:3px 8px;border-bottom:0.5px solid #f1f5f9;text-align:right;color:#6b7280"):
                                            ui.html(f"{va}/d" if va is not None else "\u2014")
                                        with ui.element("td").style("padding:3px 8px;border-bottom:0.5px solid #f1f5f9;text-align:right;color:#374151;min-width:80px"):
                                            ui.html(precio)

                # Grafico dual: stock (izq) + precio (der)
                with ui.element("div").style("display:flex;flex-direction:column;gap:4px;min-width:0"):
                    if dias_r:
                        ui.label(
                            f"Con {metricas.get('stock_actual')} unidades y vel. {vel}/d -> estimados {dias_r} dias de stock restantes."
                        ).style(f"font-size:11px;color:{dc};display:block")
                    ui.echart({
                        "grid": {"top": 20, "bottom": 40, "left": 46, "right": 60},
                        "legend": {
                            "data": ["Stock", "Precio"],
                            "top": 0, "right": 65,
                            "textStyle": {"fontSize": 10, "color": "#9ca3af"},
                            "itemWidth": 16, "itemHeight": 8,
                        },
                        "xAxis": {
                            "type": "category",
                            "data": chart_labels,
                            "axisLabel": {"fontSize": 10, "color": "#9ca3af", "interval": 1},
                            "axisLine": {"lineStyle": {"color": "#e2e8f0"}},
                            "axisTick": {"show": False},
                        },
                        "yAxis": [
                            {
                                "type": "value",
                                "axisLabel": {"fontSize": 10, "color": "#2A7AC7"},
                                "axisLine": {"show": True, "lineStyle": {"color": "#2A7AC7"}},
                                "splitLine": {"lineStyle": {"color": "#f1f5f9", "type": "dashed"}},
                                "min": 0,
                            },
                            {
                                "type": "value",
                                "position": "right",
                                "axisLabel": {
                                    "fontSize": 10,
                                    "color": "#EF9F27",
                                    "formatter": "${value}k",
                                },
                                "axisLine": {"show": True, "lineStyle": {"color": "#EF9F27"}},
                                "splitLine": {"show": False},
                            },
                        ],
                        "series": [
                            {
                                "name": "Stock",
                                "type": "line",
                                "yAxisIndex": 0,
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
                                "label": {"show": False},
                            },
                            {
                                "name": "Precio",
                                "type": "line",
                                "yAxisIndex": 1,
                                "data": precio_serie,
                                "smooth": False,
                                "lineStyle": {"color": "#EF9F27", "width": 2, "type": "dashed"},
                                "itemStyle": {"color": "#EF9F27"},
                                "symbolSize": 3,
                                "areaStyle": None,
                                "connectNulls": True,
                                "label": {"show": False},
                            },
                        ],
                        "tooltip": {
                            "trigger": "axis",
                            "formatter": "{b}<br/>Stock: <b>{c0}</b> unidades<br/>Precio: ${c1}k",
                        },
                    }).style("height:calc(100vh - 450px);width:100%")

    async def _cargar():
        sku = estado.get("sku")
        marca = estado.get("marca")
        if marca:
            rows = await run.io_bound(
                _get_stock_history_marca, user_id, marca, estado["desde"], estado["hasta"]
            )
            n_skus = await run.io_bound(
                _get_marca_n_skus, user_id, marca, estado["desde"], estado["hasta"]
            )
            met = _calcular_metricas(rows)
            _pintar(rows, met, marca=marca, n_skus=n_skus)
            return
        if not sku:
            contenido_ref[0].clear()
            with contenido_ref[0]:
                ui.label("Selecciona un SKU o una Marca para ver el historial.").style(
                    "font-size:13px;color:#9ca3af;padding:24px"
                )
            return
        rows = await run.io_bound(
            _get_stock_history, user_id, sku, estado["desde"], estado["hasta"]
        )
        met = _calcular_metricas(rows)
        _pintar(rows, met, sku=sku)

    # Layout principal
    with ui.element("div").style("padding:10px 20px 0"):
        with ui.row().style("gap:8px;align-items:flex-end;flex-wrap:wrap;margin-bottom:8px"):
            with ui.column().style("gap:3px"):
                ui.label("SKU").style("font-size:11px;color:var(--color-text-secondary)")
                sel = ui.select(options=skus, value=None, label="").props(
                    "dense outlined clearable use-input input-debounce=200"
                ).style("width:240px;font-size:12px")
            with ui.column().style("gap:3px"):
                ui.label("Marca").style("font-size:11px;color:var(--color-text-secondary)")
                sel_marca = ui.select(options=marcas, value=None, label="").props(
                    "dense outlined clearable use-input input-debounce=200"
                ).style("width:200px;font-size:12px")

            def _on_sku(e):
                estado["sku"] = e.value
                if e.value:
                    estado["marca"] = None
                    sel_marca.set_value(None)
                ui.timer(0.05, _cargar, once=True)
            sel.on_value_change(_on_sku)

            def _on_marca(e):
                estado["marca"] = e.value
                if e.value:
                    estado["sku"] = None
                    sel.set_value(None)
                ui.timer(0.05, _cargar, once=True)
            sel_marca.on_value_change(_on_marca)

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

    # Contenido (sin padding lateral para que el grafico llegue al borde)
    with ui.element("div").style("padding:0 0 0 20px;width:100%"):
        cont = ui.element("div").style("width:100%")
        contenido_ref[0] = cont
        with cont:
            ui.label("Selecciona un SKU o una Marca para ver el historial.").style(
                "font-size:13px;color:#9ca3af;padding:24px"
            )
