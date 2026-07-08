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
    for i in range(1, len(rows)):
        diff = (rows[i-1]['stock'] or 0) - (rows[i]['stock'] or 0)
        if diff > 0:
            ventas_total += diff
    dias = len(rows)
    vel = round(ventas_total / dias, 2) if dias > 0 else 0
    stock_actual = rows[-1]['stock'] or 0
    dias_restantes = round(stock_actual / vel) if vel > 0 else None
    precio_actual = rows[-1].get('price')
    return {
        'ventas_total': ventas_total,
        'vel_diaria': vel,
        'dias_restantes': dias_restantes,
        'stock_actual': stock_actual,
        'precio_actual': precio_actual,
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
        contenido_ref[0].clear()
        with contenido_ref[0]:
            if not rows:
                ui.label("No hay datos para este SKU y período.").style(
                    "font-size:13px;color:#9ca3af;padding:24px"
                )
                return

            # Métricas
            with ui.row().style("gap:8px;margin-bottom:12px;flex-wrap:wrap"):
                for lbl, val, color in [
                    ("Stock actual", str(metricas.get('stock_actual', '—')), "#185FA5"),
                    ("Vendidas en período", str(metricas.get('ventas_total', '—')), "#dc2626"),
                    ("Vel. promedio", f"{metricas.get('vel_diaria', 0)}/día", "#374151"),
                    ("Días restantes", str(metricas.get('dias_restantes') or '—'), "#166534"),
                    ("Precio actual", _fmt_precio(metricas.get('precio_actual')), "#374151"),
                ]:
                    with ui.element("div").style(
                        "background:var(--color-background-secondary);"
                        "border:0.5px solid var(--color-border-tertiary);"
                        "border-radius:6px;padding:6px 12px;text-align:center"
                    ):
                        ui.label(val).style(f"font-size:15px;font-weight:500;color:{color};display:block")
                        ui.label(lbl).style("font-size:10px;color:#6b7280")

            # Tabla
            with ui.element("div").style(
                "border:0.5px solid var(--color-border-secondary);"
                "border-radius:8px;overflow:hidden;margin-bottom:16px"
            ):
                with ui.element("table").style(
                    "width:100%;border-collapse:collapse;font-size:11px"
                ):
                    with ui.element("thead"):
                        with ui.element("tr"):
                            for h in ["Fecha", "Stock", "Vendidas", "Vel. diaria", "Precio", "Días restantes"]:
                                with ui.element("th").style(
                                    "padding:6px 10px;background:#2A7AC7;color:#fff;"
                                    "font-weight:500;text-align:center;border-right:0.5px solid rgba(255,255,255,0.15)"
                                ):
                                    ui.html(h)
                    with ui.element("tbody"):
                        prev_stock = None
                        for i, r in enumerate(reversed(rows)):
                            stock = r.get('stock') or 0
                            vendidas = (prev_stock - stock) if prev_stock is not None and prev_stock > stock else 0
                            prev_stock_next = rows[-(i+2)]['stock'] if i < len(rows)-1 else None
                            vel_dia = vendidas
                            precio = _fmt_precio(r.get('price'))
                            vel = rows[len(rows)-1-i].get('vel') if False else vel_dia
                            dias_rest = round(stock / metricas['vel_diaria']) if metricas.get('vel_diaria', 0) > 0 else None
                            _bg = "background:#f9fafb" if i % 2 == 1 else ""
                            with ui.element("tr").style(_bg):
                                for val, extra in [
                                    (r['snapshot_date'], "text-align:center"),
                                    (str(stock), "text-align:right;font-weight:500"),
                                    (
                                        f"−{vendidas}" if vendidas > 0 else "—",
                                        f"text-align:right;color:{'#dc2626' if vendidas > 0 else '#9ca3af'}"
                                    ),
                                    (
                                        f"{vendidas}/día" if vendidas > 0 else "—",
                                        "text-align:right;color:#6b7280"
                                    ),
                                    (precio, "text-align:right"),
                                    (
                                        str(dias_rest) if dias_rest else "—",
                                        "text-align:center;color:#166534;font-weight:500"
                                    ),
                                ]:
                                    with ui.element("td").style(
                                        f"padding:4px 10px;border-bottom:0.5px solid #f1f5f9;"
                                        f"font-size:11px;color:#374151;{extra}"
                                    ):
                                        ui.html(str(val))
                            prev_stock = stock

            # Gráfico con ui.echart (nativo NiceGUI)
            labels  = [r['snapshot_date'] for r in rows]
            valores = [r['stock'] or 0 for r in rows]
            with ui.element("div").style(
                "background:var(--color-background-secondary);"
                "border:0.5px solid var(--color-border-tertiary);"
                "border-radius:8px;padding:12px 14px;margin-top:4px"
            ):
                ui.label(f"Evolución de stock — {sku}").style(
                    "font-size:11px;font-weight:500;color:#185FA5;margin-bottom:8px;display:block"
                )
                ui.echart({
                    "grid": {"top": 20, "bottom": 30, "left": 50, "right": 16},
                    "xAxis": {
                        "type": "category",
                        "data": labels,
                        "axisLabel": {"fontSize": 10},
                        "axisLine": {"lineStyle": {"color": "#e2e8f0"}},
                    },
                    "yAxis": {
                        "type": "value",
                        "axisLabel": {"fontSize": 10},
                        "splitLine": {"lineStyle": {"color": "#f1f5f9"}},
                    },
                    "series": [{
                        "type": "line",
                        "data": valores,
                        "smooth": True,
                        "lineStyle": {"color": "#378ADD", "width": 2},
                        "itemStyle": {"color": "#378ADD"},
                        "areaStyle": {"color": "rgba(55,138,221,0.08)"},
                        "symbolSize": 6,
                    }],
                    "tooltip": {"trigger": "axis"},
                }).style("height:180px;width:100%")

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
