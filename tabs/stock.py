"""
tabs/stock.py
Pagina Stock: evolucion historica de stock por SKU.
"""
from __future__ import annotations
import os
import re
import tempfile
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional
from nicegui import app, run, ui
from db import get_connection, get_user_ml_razon_social

MESES = ["Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic"]


def _get_skus(user_id: int) -> List[Dict[str, Any]]:
    conn = get_connection()
    rows = conn.execute("""
        SELECT DISTINCT s.seller_sku, p.nombre
        FROM ml_stock_snapshots s
        LEFT JOIN productos p ON s.seller_sku = p.sku AND s.user_id = p.user_id
        WHERE s.user_id=? AND s.seller_sku IS NOT NULL AND s.seller_sku != ''
        ORDER BY s.seller_sku
    """, (user_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


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


def _get_fecha_minima(user_id: int) -> str | None:
    """Primer snapshot real del usuario, excluyendo el SKU de prueba TEST-PRODUCTO-DEMO."""
    conn = get_connection()
    row = conn.execute("""
        SELECT MIN(snapshot_date) FROM ml_stock_snapshots
        WHERE user_id=? AND seller_sku != 'TEST-PRODUCTO-DEMO'
    """, (user_id,)).fetchone()
    conn.close()
    return row[0] if row and row[0] else None


def _get_ultimo_snapshot_hasta(user_id: int, limite: str) -> str | None:
    """Ultimo dia con snapshot real disponible, <= limite (ISO). Usado como 'hasta' efectivo
    de los presets de Fecha cuando el snapshot de ayer todavia no corrio (cron puede correr
    mas tarde en el dia)."""
    conn = get_connection()
    row = conn.execute("""
        SELECT MAX(snapshot_date) FROM ml_stock_snapshots
        WHERE user_id=? AND snapshot_date <= ? AND seller_sku != 'TEST-PRODUCTO-DEMO'
    """, (user_id, limite)).fetchone()
    conn.close()
    return row[0] if row and row[0] else None


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


def _get_stock_history_marca(user_id: int, marca: str, desde: str, hasta: str) -> tuple:
    """Igual que _get_stock_history pero sumado entre todos los SKUs de la marca.
    Colapsa primero por (seller_sku, snapshot_date) con MAX -- un mismo seller_sku puede
    tener mas de un item_id reportando stock el mismo dia -- y recien despues suma entre
    SKUs, para no sobrecontar (un SUM directo sobre ml_stock_snapshots llega a sobrecontar
    7-8x en SKUs con varios item_id). El ticket (precio de venta) NO se pondera aca por
    stock -- ver _calcular_ticket_ventas, que pondera por unidades vendidas usando
    per_sku_series. Devuelve (rows_stock_sumado, per_sku_series)."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT s.snapshot_date, s.seller_sku,
               MAX(s.available_qty) AS stock,
               MAX(s.price)         AS price
        FROM ml_stock_snapshots s
        INNER JOIN productos p ON s.seller_sku = p.sku AND s.user_id = p.user_id
        WHERE p.marca=? AND s.user_id=? AND s.snapshot_date BETWEEN ? AND ?
        GROUP BY s.snapshot_date, s.seller_sku
        ORDER BY s.seller_sku, s.snapshot_date
    """, (marca, user_id, desde, hasta)).fetchall()
    conn.close()

    per_sku_series: Dict[str, List[Dict]] = defaultdict(list)
    stock_por_dia: Dict[str, int] = defaultdict(int)
    for r in rows:
        d = dict(r)
        per_sku_series[d["seller_sku"]].append(d)
        stock_por_dia[d["snapshot_date"]] += d["stock"] or 0

    rows_stock_sumado = [
        {"snapshot_date": d, "stock": s} for d, s in sorted(stock_por_dia.items())
    ]
    return rows_stock_sumado, dict(per_sku_series)


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


def _get_resumen_marcas(user_id: int, desde: str, hasta: str) -> List[Dict[str, Any]]:
    """Filas per-sku-dia (marca, snapshot_date, seller_sku, stock, price) de todas las marcas
    del usuario en el rango dado. Mismo patron anti-sobreconteo que _get_stock_history_marca --
    colapsa por (seller_sku, dia) con MAX para no inflar el stock en SKUs con varios item_id.
    No suma/pondera aca -- _calcular_resumen_marcas agrupa en Python (stock por dia para
    velocidad, y por sku para el ticket ponderado por ventas via _calcular_ticket_ventas)."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT p.marca, s.snapshot_date, s.seller_sku,
               MAX(s.available_qty) AS stock,
               MAX(s.price)         AS price
        FROM ml_stock_snapshots s
        INNER JOIN productos p ON s.seller_sku = p.sku AND s.user_id = p.user_id
        WHERE s.user_id=? AND p.marca IS NOT NULL AND s.snapshot_date BETWEEN ? AND ?
        GROUP BY p.marca, s.snapshot_date, s.seller_sku
        ORDER BY p.marca, s.seller_sku, s.snapshot_date
    """, (user_id, desde, hasta)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _calcular_metricas(rows: List[Dict]) -> Dict[str, Any]:
    if not rows:
        return {}
    ventas_total = 0
    dias_validos = 0
    for i in range(1, len(rows)):
        stock_prev = rows[i-1].get("stock") or 0
        stock_curr = rows[i].get("stock") or 0
        if stock_curr > stock_prev:
            continue  # restock: no cuenta ni en numerador ni en denominador
        if stock_prev > stock_curr:
            ventas_total += stock_prev - stock_curr
        dias_validos += 1  # venta o variacion cero: dia valido para el promedio
    vel = round(ventas_total / dias_validos, 1) if dias_validos > 0 else 0
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


_ESTADOS_VENTA_VALIDA = ("paid", "handling", "shipped", "delivered")


def _get_item_id_a_sku(user_id: int) -> Dict[str, str]:
    """Mapea item_id (ML) -> seller_sku. Fallback para order items que no traen
    item.seller_sku directo. Fuente: ml_stock_snapshots, unica tabla persistida con ambos
    campos. Si un item_id tuvo mas de un seller_sku historico (relist), se queda con el mas
    reciente (ORDER BY snapshot_date ASC + overwrite)."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT item_id, seller_sku FROM ml_stock_snapshots
        WHERE user_id=? AND item_id IS NOT NULL AND seller_sku IS NOT NULL AND seller_sku != ''
        ORDER BY snapshot_date ASC
    """, (user_id,)).fetchall()
    conn.close()
    out: Dict[str, str] = {}
    for r in rows:
        out[r["item_id"]] = r["seller_sku"]
    return out


def _get_ventas_reales_por_sku_dia(user_id: int, desde: str, hasta: str) -> Dict[str, Dict[str, List[tuple]]]:
    """sku -> dia (YYYY-MM-DD) -> [(qty, unit_price), ...], desde ordenes reales cacheadas en
    ml_orders_cache (misma fuente que Ventas). unit_price es el precio real cobrado al
    comprador, ya neto de descuentos -- incluidos los subsidiados por campanas de cuotas
    (confirmado en docs de ML: gross_price = (unit_price + discounts.full) * qty). Filtra por
    status valido (paid/handling/shipped/delivered, igual que Ventas) para que una orden
    cancelada no fije el precio real de ese dia."""
    import json as _json
    item_id_a_sku = _get_item_id_a_sku(user_id)
    conn = get_connection()
    rows = conn.execute("""
        SELECT date_created, status, items_json FROM ml_orders_cache
        WHERE user_id=? AND substr(date_created,1,10) BETWEEN ? AND ?
    """, (user_id, desde, hasta)).fetchall()
    conn.close()

    out: Dict[str, Dict[str, List[tuple]]] = defaultdict(lambda: defaultdict(list))
    for r in rows:
        status = (r["status"] or "").strip().lower()
        if status not in _ESTADOS_VENTA_VALIDA:
            continue
        dia = (r["date_created"] or "")[:10]
        if not dia:
            continue
        try:
            items = _json.loads(r["items_json"] or "[]")
        except Exception:
            continue
        for it in items:
            if not isinstance(it, dict):
                continue
            obj = it.get("item") or {}
            sku = (obj.get("seller_sku") or "").strip() if isinstance(obj, dict) else ""
            if not sku:
                item_id = (str(obj.get("id") or it.get("item_id") or "").strip()
                           if isinstance(obj, dict) else str(it.get("item_id") or "").strip())
                sku = item_id_a_sku.get(item_id, "")
            if not sku:
                continue
            qty = int(it.get("quantity") or it.get("qty") or 0)
            unit_price = it.get("unit_price")
            if qty <= 0 or unit_price is None:
                continue
            try:
                unit_price = float(unit_price)
            except (TypeError, ValueError):
                continue
            out[sku][dia].append((qty, unit_price))
    return {sku: dict(dias) for sku, dias in out.items()}


def _calcular_ticket_ventas(per_sku_series: Dict[str, List[Dict]],
                             ventas_reales: Optional[Dict[str, Dict[str, List[tuple]]]] = None) -> Dict[str, Any]:
    """Ticket ponderado por unidades VENDIDAS (no por stock). per_sku_series: sku -> filas
    {snapshot_date, stock, price} ordenadas por fecha. Fuente primaria del precio: unit_price
    real cobrado en ordenes (ventas_reales, ver _get_ventas_reales_por_sku_dia) para ese
    (sku, dia) -- pondera por la cantidad REAL de esas ordenes, no por el delta de stock (el
    delta puede traer ruido -- ajustes manuales, multiples item_id por SKU -- que no debe
    filtrarse al precio). Si un (sku, dia) con venta inferida por delta de stock no tiene
    ordenes cacheadas ese dia, fallback al price del snapshot (precio de PUBLICACION, puede
    estar inflado) ponderado por el delta -- mismo criterio que antes de este fix. Dias sin
    venta devuelven None (no se inventa un numero con el mix del stock)."""
    ventas_reales = ventas_reales or {}
    por_dia: Dict[str, List[tuple]] = defaultdict(list)  # date -> [(unidades, precio), ...]
    for sku, serie in per_sku_series.items():
        reales_sku = ventas_reales.get(sku) or {}
        for i in range(1, len(serie)):
            prev, curr = serie[i - 1], serie[i]
            stock_prev, stock_curr = prev.get("stock") or 0, curr.get("stock") or 0
            if not (stock_prev > stock_curr):
                continue
            dia = curr["snapshot_date"]
            lineas_reales = reales_sku.get(dia)
            if lineas_reales:
                por_dia[dia].extend(lineas_reales)
            elif curr.get("price"):
                por_dia[dia].append((stock_prev - stock_curr, float(curr["price"])))

    ticket_dia: Dict[str, Optional[float]] = {}
    tot_unidades, tot_valor = 0, 0.0
    for d, lineas in por_dia.items():
        u = sum(x[0] for x in lineas)
        val = sum(x[0] * x[1] for x in lineas)
        ticket_dia[d] = round(val / u, 2) if u else None
        tot_unidades += u
        tot_valor += val

    vals = [v for v in ticket_dia.values() if v is not None]
    return {
        "ticket_dia": ticket_dia,
        "ticket_prom_periodo": round(tot_valor / tot_unidades) if tot_unidades else None,
        "ticket_min": min(vals) if vals else None,
        "ticket_max": max(vals) if vals else None,
    }


def _calcular_resumen_marcas(rows: List[Dict],
                              ventas_reales: Optional[Dict[str, Dict[str, List[tuple]]]] = None) -> List[Dict[str, Any]]:
    """rows: filas per-sku-dia (marca, snapshot_date, seller_sku, stock, price) de
    _get_resumen_marcas. Agrupa por marca->dia (stock sumado, para _calcular_metricas sin
    cambios) y por marca->sku (para el ticket ponderado por ventas). Ordena por velocidad
    descendente."""
    stock_por_marca_dia: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    series_por_marca_sku: Dict[str, Dict[str, List[Dict]]] = defaultdict(lambda: defaultdict(list))
    for r in rows:
        stock_por_marca_dia[r["marca"]][r["snapshot_date"]] += r["stock"] or 0
        series_por_marca_sku[r["marca"]][r["seller_sku"]].append(r)

    resumen = []
    for marca, stock_dias in stock_por_marca_dia.items():
        serie = [{"snapshot_date": d, "stock": s} for d, s in sorted(stock_dias.items())]
        met = _calcular_metricas(serie)
        if not met:
            continue
        ticket_info = _calcular_ticket_ventas(series_por_marca_sku[marca], ventas_reales)
        resumen.append({
            "marca": marca,
            "stock": met.get("stock_actual", 0),
            "ventas": met.get("ventas_total", 0),
            "vel": met.get("vel_diaria", 0),
            "dias_restantes": met.get("dias_restantes"),
            "ticket_prom": ticket_info["ticket_prom_periodo"],
        })
    resumen.sort(key=lambda x: x["vel"], reverse=True)
    return resumen


def _fmt_num(n) -> str:
    """Formato argentino: punto para miles, coma para decimales (solo si hay). None -> '—'."""
    if n is None:
        return "—"
    try:
        n = float(n)
    except (TypeError, ValueError):
        return str(n)
    signo = "-" if n < 0 else ""
    n = abs(n)
    if n == int(n):
        cuerpo = f"{int(n):,}".replace(",", ".")
    else:
        entero, decimales = f"{n:,.2f}".split(".")
        cuerpo = entero.replace(",", ".") + "," + decimales
    return signo + cuerpo


def _fmt_precio(v):
    if v is None:
        return "—"
    return "$" + _fmt_num(v)


def _iso_a_ddmmyyyy(iso_str: str) -> str:
    try:
        return datetime.strptime(iso_str, "%Y-%m-%d").strftime("%d/%m/%Y")
    except Exception:
        return iso_str


def _iso_a_ddmm(iso_str: str) -> str:
    try:
        return datetime.strptime(iso_str, "%Y-%m-%d").strftime("%d/%m")
    except Exception:
        return iso_str


def _ddmmyyyy_a_iso(v: str):
    try:
        return datetime.strptime(v, "%d/%m/%Y").strftime("%Y-%m-%d")
    except Exception:
        return None


def _slug_nombre(s: str) -> str:
    s = re.sub(r"\s+", "-", (s or "").strip())
    s = re.sub(r"[^A-Za-z0-9_-]", "", s)
    return s or "Reporte"


def _render_stock_pdf_html(datos: Dict[str, Any], razon_social: str, chart_b64: Optional[str]) -> str:
    """Arma el HTML del reporte de Stock para exportar a PDF via WeasyPrint (A4 landscape).
    Reutiliza los mismos valores ya calculados que se muestran en pantalla (metricas +
    data por dia), asi el PDF y la vista en vivo nunca pueden desalinearse."""
    metricas = datos["metricas"]
    sku, marca, n_skus = datos.get("sku"), datos.get("marca"), datos.get("n_skus")
    desde_fmt, hasta_fmt = _iso_a_ddmmyyyy(datos["desde"]), _iso_a_ddmmyyyy(datos["hasta"])
    generado = datetime.now().strftime("%d/%m/%Y %H:%M")

    titulo = f"SKU: {sku}" if sku else f"Marca: {marca}"
    if marca and n_skus:
        titulo += f" ({n_skus} SKUs)"

    vel    = metricas.get("vel_diaria", 0)
    dias_r = metricas.get("dias_restantes")
    dc     = "#dc2626" if dias_r and dias_r < 7 else "#ca6d00" if dias_r and dias_r < 20 else "#166534"
    dc_bg  = "#FEE2E2" if dias_r and dias_r < 7 else "#FEF3C7" if dias_r and dias_r < 20 else "#DCFCE7"
    dc_br  = "#FCA5A5" if dias_r and dias_r < 7 else "#FDE68A" if dias_r and dias_r < 20 else "#86EFAC"

    p_min  = _fmt_precio(metricas.get("precio_min"))
    p_max  = _fmt_precio(metricas.get("precio_max"))
    p_prom = _fmt_precio(metricas.get("precio_prom"))
    p_detalle = p_min if p_min == p_max else f"{p_min} min &middot; {p_max} max &middot; {p_prom} prom"

    _header_html = (
        '<div style="display:flex;justify-content:space-between;align-items:flex-end;'
        'border-bottom:3px solid #113F72;padding-bottom:10px;margin-bottom:14px">'
        "<div>"
        '<div style="font-size:19px;font-weight:800;color:#185FA5;letter-spacing:0.02em">'
        "BDC systems</div>"
        '<div style="font-size:14px;font-weight:700;color:#222;margin-top:2px">'
        f"Reporte de Venta y Stock &mdash; {titulo}</div>"
        "</div>"
        '<div style="text-align:right;font-size:10px;color:#555;line-height:1.6">'
        f'<div style="font-size:12px;font-weight:700;color:#185FA5">{razon_social or ""}</div>'
        f'<div>Periodo: {desde_fmt} &ndash; {hasta_fmt}</div>'
        f'<div>Generado: {generado}</div>'
        "</div></div>"
    )

    pills = [
        ("vel.",       f"{vel}/d",                                 "#FFEDD5", "#FB923C", "#C2410C", True),
        ("stock",      _fmt_num(metricas.get("stock_actual")), "#E6F1FB", "#85B7EB", "#0C447C", False),
        ("vendidas",   _fmt_num(metricas.get("ventas_total")), "#FEE2E2", "#FCA5A5", "#991B1B", False),
        ("dias rest.", str(dias_r or "—"),                     dc_bg,     dc_br,     dc,        False),
        ("ticket prom.", _fmt_precio(metricas.get("precio_actual")), "#F1F5F9", "#e2e8f0", "#374151", False),
    ]
    _pills_html = '<div style="display:flex;gap:8px;margin-bottom:6px">'
    for lbl, val, bg, border, color, is_main in pills:
        _pills_html += (
            f'<div style="display:inline-flex;align-items:baseline;gap:5px;background:{bg};'
            f'border:{"1.5px" if is_main else "0.5px"} solid {border};border-radius:20px;padding:5px 12px;'
            f'{"min-width:112px;white-space:nowrap;" if is_main else ""}">'
            f'<span style="font-size:{"14px" if is_main else "12px"};font-weight:{"800" if is_main else "600"};'
            f'color:{color};white-space:nowrap">{val}</span>'
            f'<span style="font-size:9px;color:{color};opacity:.75">{lbl}</span>'
            "</div>"
        )
    _pills_html += "</div>"

    _detalle_html = (
        f'<div style="font-size:9px;color:#666;margin-bottom:14px">'
        f'Ticket: {p_detalle} &nbsp;&middot;&middot;&nbsp; '
        f"Stock: {_fmt_num(metricas.get('stock_max',0))} max &middot; {_fmt_num(metricas.get('stock_prom',0))} prom "
        f"&middot; {_fmt_num(metricas.get('vel_max_dia',0))}/d max &nbsp;&middot;&middot;&nbsp; "
        f"Dias: {_fmt_num(metricas.get('dias_con_stock',0))} c/stock &middot; "
        f"{_fmt_num(metricas.get('dias_sin_stock',0))} sin &middot; {_fmt_num(metricas.get('n_reposiciones',0))} repos"
        "</div>"
    )

    _chart_html = ""
    if chart_b64:
        _chart_html = (
            f'<img src="{chart_b64}" style="width:100%;max-height:230mm;object-fit:contain;'
            f'margin-bottom:14px" />'
        )

    rows_html = []
    cur_mes = None
    for r in reversed(datos["data"]):
        try:
            d = datetime.strptime(r["snapshot_date"], "%Y-%m-%d")
            mes_key, dia_label = f"{d.year}-{d.month:02d}", f"{d.day:02d}/{d.month:02d}/{d.year}"
        except Exception:
            mes_key = dia_label = r["snapshot_date"]
        if mes_key != cur_mes:
            cur_mes = mes_key
            rows_html.append(
                f'<tr><td colspan="5" style="background:#EEF6FD;color:#185FA5;'
                f'font-weight:700;padding:2px 6px;font-size:8px">{MESES[d.month-1]} {d.year}</td></tr>'
            )
        stock, vend, repo, va = r.get("stock") or 0, r["vend"], r["repo"], r.get("vel_acum")
        if repo > 0:
            vc, vt = "#166534", f"+{_fmt_num(repo)}"
        elif vend > 0:
            vc, vt = "#dc2626", f"−{_fmt_num(vend)}"
        else:
            vc, vt = "#9ca3af", "—"
        rows_html.append(
            "<tr>"
            f'<td style="padding:1.5px 6px;text-align:left;color:#6b7280">{dia_label}</td>'
            f'<td style="padding:1.5px 6px;text-align:right;font-weight:600;'
            f'color:{"#166534" if stock > 0 else "#9ca3af"}">{_fmt_num(stock)}</td>'
            f'<td style="padding:1.5px 6px;text-align:right;font-weight:600;color:{vc}">{vt}</td>'
            f'<td style="padding:1.5px 6px;text-align:right;color:#6b7280">'
            f'{f"{va}/d" if va is not None else "—"}</td>'
            f'<td style="padding:1.5px 6px;text-align:right;color:#374151">{_fmt_precio(r.get("price"))}</td>'
            "</tr>"
        )

    _tabla_cols = [("Dia", "left"), ("Stock", "right"), ("Variacion", "right"), ("Vel. acum.", "right"), ("Ticket prom.", "right")]
    _tabla_html = (
        '<table style="width:100%;max-width:340px;border-collapse:collapse;font-size:8.5px">'
        "<thead><tr>"
        + "".join(
            f'<th style="padding:3px 6px;background:#2A7AC7;color:#fff;font-weight:600;'
            f'text-align:{align}">{h}</th>'
            for h, align in _tabla_cols
        )
        + "</tr></thead><tbody>" + "".join(rows_html) + "</tbody></table>"
    )

    _desglose_html = ""
    desglose = datos.get("desglose")
    if marca and desglose:
        _desglose_cols = [("SKU — Nombre", "left"), ("Stock", "right"), ("Ventas", "right"),
                           ("Velocidad", "right"), ("Días Restantes", "right"), ("Ticket Promedio", "right")]
        _desglose_rows_html = []
        for row_i in desglose:
            dias_ri = row_i.get("dias_restantes")
            dc_i = "#dc2626" if dias_ri and dias_ri < 7 else "#ca6d00" if dias_ri and dias_ri < 20 else "#166534"
            _desglose_rows_html.append(
                "<tr>"
                f'<td style="padding:1.5px 6px;text-align:left;color:#374151">{row_i.get("nombre") or row_i["sku"]}</td>'
                f'<td style="padding:1.5px 6px;text-align:right;color:#0C447C">{_fmt_num(row_i["stock"])}</td>'
                f'<td style="padding:1.5px 6px;text-align:right;color:#991B1B">{_fmt_num(row_i.get("ventas", 0))}</td>'
                f'<td style="padding:1.5px 6px;text-align:right;font-weight:600;color:#C2410C">{row_i["vel"]}/d</td>'
                f'<td style="padding:1.5px 6px;text-align:right;font-weight:600;color:{dc_i}">{dias_ri if dias_ri else "—"}</td>'
                f'<td style="padding:1.5px 6px;text-align:right;color:#374151">{_fmt_precio(row_i.get("ticket_prom"))}</td>'
                "</tr>"
            )
        _desglose_html = (
            '<div style="font-size:9px;font-weight:700;color:#185FA5;margin:10px 0 4px">Desglose por SKU</div>'
            '<table style="width:100%;border-collapse:collapse;font-size:8.5px">'
            "<thead><tr>"
            + "".join(
                f'<th style="padding:3px 6px;background:#2A7AC7;color:#fff;font-weight:600;text-align:{align}">{h}</th>'
                for h, align in _desglose_cols
            )
            + "</tr></thead><tbody>" + "".join(_desglose_rows_html) + "</tbody></table>"
        )

    _style = (
        "@page { size: A4 landscape; margin: 12mm 10mm 16mm 10mm; "
        '@bottom-center { content: "Pagina " counter(page) " de " counter(pages); '
        "font-size: 8px; color: #999; } }"
        "body { font-family: 'Segoe UI', Arial, sans-serif; color: #222; margin: 0; }"
    )

    body = _header_html + _pills_html + _detalle_html + _chart_html + _tabla_html + _desglose_html
    return f"<html><head><style>{_style}</style></head><body>{body}</body></html>"


def _generar_pdf_stock(datos: Dict[str, Any], razon_social: str, chart_b64: Optional[str]) -> tuple:
    """Genera el PDF del reporte de Stock via WeasyPrint. Devuelve (path, filename)."""
    from weasyprint import HTML

    html = _render_stock_pdf_html(datos, razon_social, chart_b64)
    fd, path = tempfile.mkstemp(suffix=".pdf")
    os.close(fd)
    HTML(string=html).write_pdf(path)

    nombre_base = _slug_nombre(datos.get("sku") or datos.get("marca") or "Reporte")
    desde_fmt = datetime.strptime(datos["desde"], "%Y-%m-%d").strftime("%d%m%Y")
    hasta_fmt = datetime.strptime(datos["hasta"], "%Y-%m-%d").strftime("%d%m%Y")
    nombre = f"Stock_{nombre_base}_{desde_fmt}_{hasta_fmt}.pdf"
    return path, nombre


def _render_marcas_pdf_html(resumen: List[Dict[str, Any]], desde: str, hasta: str, razon_social: str) -> str:
    """Arma el HTML del resumen de Marcas para exportar a PDF via WeasyPrint (A4 landscape).
    Mismo patron visual que _render_stock_pdf_html (header BDC systems + razon social +
    periodo), pero con la tabla resumen de todas las marcas en vez del detalle dia a dia."""
    desde_fmt, hasta_fmt = _iso_a_ddmmyyyy(desde), _iso_a_ddmmyyyy(hasta)
    generado = datetime.now().strftime("%d/%m/%Y %H:%M")

    _header_html = (
        '<div style="display:flex;justify-content:space-between;align-items:flex-end;'
        'border-bottom:3px solid #113F72;padding-bottom:10px;margin-bottom:14px">'
        "<div>"
        '<div style="font-size:19px;font-weight:800;color:#185FA5;letter-spacing:0.02em">'
        "BDC systems</div>"
        '<div style="font-size:14px;font-weight:700;color:#222;margin-top:2px">'
        "Reporte de Venta y Stock &mdash; Resumen por Marca</div>"
        "</div>"
        '<div style="text-align:right;font-size:10px;color:#555;line-height:1.6">'
        f'<div style="font-size:12px;font-weight:700;color:#185FA5">{razon_social or ""}</div>'
        f'<div>Periodo: {desde_fmt} &ndash; {hasta_fmt}</div>'
        f'<div>Generado: {generado}</div>'
        "</div></div>"
    )

    marca_header = f"Marca ({_iso_a_ddmm(desde)} al {_iso_a_ddmm(hasta)})"
    cols = [(marca_header, "left"), ("Stock", "right"), ("Ventas", "right"), ("Velocidad", "right"), ("Dias Restantes", "right"), ("Ticket Promedio", "right")]

    rows_html = []
    for row in resumen:
        dias_r = row.get("dias_restantes")
        dc = "#dc2626" if dias_r and dias_r < 7 else "#ca6d00" if dias_r and dias_r < 20 else "#166534"
        rows_html.append(
            "<tr>"
            f'<td style="padding:3px 8px;text-align:left;font-weight:600;color:#374151">{row["marca"]}</td>'
            f'<td style="padding:3px 8px;text-align:right;color:#0C447C">{_fmt_num(row["stock"])}</td>'
            f'<td style="padding:3px 8px;text-align:right;color:#991B1B">{_fmt_num(row.get("ventas", 0))}</td>'
            f'<td style="padding:3px 8px;text-align:right;font-weight:600;color:#C2410C">{row["vel"]}/d</td>'
            f'<td style="padding:3px 8px;text-align:right;font-weight:600;color:{dc}">{dias_r if dias_r else "—"}</td>'
            f'<td style="padding:3px 8px;text-align:right;color:#374151">{_fmt_precio(row.get("ticket_prom"))}</td>'
            "</tr>"
        )

    _tabla_html = (
        '<table style="width:100%;border-collapse:collapse;font-size:10px">'
        "<thead><tr>"
        + "".join(
            f'<th style="padding:5px 8px;background:#2A7AC7;color:#fff;font-weight:600;'
            f'text-align:{align}">{h}</th>'
            for h, align in cols
        )
        + "</tr></thead><tbody>" + "".join(rows_html) + "</tbody></table>"
    )

    _style = (
        "@page { size: A4 landscape; margin: 12mm 10mm 16mm 10mm; "
        '@bottom-center { content: "Pagina " counter(page) " de " counter(pages); '
        "font-size: 8px; color: #999; } }"
        "body { font-family: 'Segoe UI', Arial, sans-serif; color: #222; margin: 0; }"
    )

    body = _header_html + _tabla_html
    return f"<html><head><style>{_style}</style></head><body>{body}</body></html>"


def _generar_pdf_marcas(resumen: List[Dict[str, Any]], desde: str, hasta: str, razon_social: str) -> tuple:
    """Genera el PDF del resumen de Marcas via WeasyPrint. Devuelve (path, filename)."""
    from weasyprint import HTML

    html = _render_marcas_pdf_html(resumen, desde, hasta, razon_social)
    fd, path = tempfile.mkstemp(suffix=".pdf")
    os.close(fd)
    HTML(string=html).write_pdf(path)

    desde_fmt = datetime.strptime(desde, "%Y-%m-%d").strftime("%d%m%Y")
    hasta_fmt = datetime.strptime(hasta, "%Y-%m-%d").strftime("%d%m%Y")
    nombre = f"Marcas_{desde_fmt}_{hasta_fmt}.pdf"
    return path, nombre


def build_tab_stock() -> None:
    user = app.storage.user.get("user")
    if not user:
        ui.label("Debes iniciar sesion").classes("text-red-500 p-4")
        return
    user_id = user["id"]

    hoy = date.today()
    fecha_minima = _get_fecha_minima(user_id)

    FECHA_PRESETS = [
        "Ayer", "Últimos 2 días", "Últimos 3 días", "Última semana",
        "Últimos 15 días", "Últimos 30 días", "Mes anterior", "Fecha predeterminada",
    ]
    _FECHA_PRESET_DIAS = {
        "Ayer": 1, "Últimos 2 días": 2, "Últimos 3 días": 3,
        "Última semana": 7, "Últimos 15 días": 15, "Últimos 30 días": 30,
    }

    def _calc_rango_preset(preset: str):
        """(desde, hasta) ISO para un preset de Fecha, o None si es 'Fecha predeterminada'
        (en ese caso los pickers manuales mandan, sin recalcular nada). 'hasta' parte de ayer,
        pero si el snapshot de ayer todavia no corrio cae al ultimo dia con snapshot real
        disponible. 'desde' nunca puede quedar antes de la fecha minima del usuario."""
        if preset == "Fecha predeterminada":
            return None
        iso_min = fecha_minima or "1970-01-01"
        ayer_iso = (hoy - timedelta(days=1)).isoformat()
        hasta_base = _get_ultimo_snapshot_hasta(user_id, ayer_iso) or ayer_iso

        if preset in _FECHA_PRESET_DIAS:
            hasta = hasta_base
            desde = (datetime.strptime(hasta, "%Y-%m-%d").date()
                      - timedelta(days=_FECHA_PRESET_DIAS[preset])).isoformat()
        elif preset == "Mes anterior":
            primer_dia_mes_actual = hoy.replace(day=1)
            ultimo_dia_mes_ant = primer_dia_mes_actual - timedelta(days=1)
            primer_dia_mes_ant = ultimo_dia_mes_ant.replace(day=1)
            dia_base = primer_dia_mes_ant - timedelta(days=1)
            hasta = ultimo_dia_mes_ant.isoformat()
            desde = dia_base.isoformat()
        else:
            return None

        if desde < iso_min:
            desde = iso_min
        if desde > hasta:
            desde = hasta
        return desde, hasta

    _rango_inicial = _calc_rango_preset("Ayer")
    if _rango_inicial:
        desde_default, hasta_default = _rango_inicial
    else:
        desde_default = (hoy - timedelta(days=29)).isoformat()
        if fecha_minima and desde_default < fecha_minima:
            desde_default = fecha_minima
        hasta_default = hoy.isoformat()
    estado = {
        "sku": None,
        "marca": None,
        "desde": desde_default,
        "hasta": hasta_default,
        "vista_resumen": True,
        "_syncing": False,
        "fecha_preset": "Ayer",
        "_load_seq": 0,
    }
    contenido_ref: list = [None]
    pdf_state: Dict[str, Any] = {"habilitado": False, "chart": None, "datos": None}
    skus_rows = _get_skus(user_id)
    sku_options = {
        r["seller_sku"]: (f'{r["seller_sku"]} — {r["nombre"]}' if r.get("nombre") else r["seller_sku"])
        for r in skus_rows
    }
    marcas = _get_marcas(user_id)

    # QDate no tiene props min/max: la restriccion de rango se hace con `options`,
    # una funcion JS que recibe cada fecha candidata en formato 'YYYY/MM/DD'
    # (fijo, independiente de la mask de visualizacion) y devuelve bool.
    _min_slash = (fecha_minima or "1970-01-01").replace("-", "/")
    _max_slash = hoy.isoformat().replace("-", "/")
    _date_range_props = (
        f"locale=es mask='DD/MM/YYYY' "
        f":options=\"date => date >= '{_min_slash}' && date <= '{_max_slash}'\" "
        f"navigation-min-year-month=\"{_min_slash[:7]}\" "
        f"navigation-max-year-month=\"{_max_slash[:7]}\""
    )

    def _pintar(rows: List[Dict], metricas: Dict, sku: str = None, marca: str = None, n_skus: int = None,
                per_sku_series: Optional[Dict[str, List[Dict]]] = None,
                ventas_reales: Optional[Dict[str, Dict[str, List[tuple]]]] = None):
        from datetime import datetime as _dt
        contenido_ref[0].clear()
        with contenido_ref[0]:
            if not rows:
                msg = "Sin datos para esta marca y periodo." if marca else "Sin datos para este SKU y periodo."
                ui.label(msg).style(
                    "font-size:13px;color:#9ca3af;padding:24px"
                )
                pdf_state["chart"] = None
                pdf_state["datos"] = None
                _set_pdf_habilitado(False)
                return

            if per_sku_series:
                ticket_info = _calcular_ticket_ventas(per_sku_series, ventas_reales)
                for r in rows:
                    r["price"] = ticket_info["ticket_dia"].get(r["snapshot_date"])
                metricas = {
                    **metricas,
                    "precio_actual": ticket_info["ticket_prom_periodo"],
                    "precio_prom":   ticket_info["ticket_prom_periodo"],
                    "precio_min":    ticket_info["ticket_min"],
                    "precio_max":    ticket_info["ticket_max"],
                }

            if marca:
                ui.label(
                    f"Vista agregada — marca: {marca} · {n_skus} SKUs con historial · "
                    "Ticket = promedio ponderado por ventas reales (no precio de publicacion)."
                ).style("font-size:11px;color:#185FA5;margin-bottom:6px;display:block")
            elif per_sku_series:
                ui.label(
                    "Ticket = precio real de venta en dias con venta (no el de publicacion)."
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
                    for icon, val, lbl, bg, border, color, is_main in [
                        ("ti-trending-up",  f"{vel}/d",                                 "vel.",     "#FFEDD5", "#FB923C", "#C2410C", True),
                        ("ti-package",      _fmt_num(metricas.get("stock_actual")), "stock",    "#E6F1FB", "#85B7EB", "#0C447C", False),
                        ("ti-shopping-cart",_fmt_num(metricas.get("ventas_total")), "vendidas", "#FEE2E2", "#FCA5A5", "#991B1B", False),
                        ("ti-clock",        str(dias_r or "\u2014"),                    "dias rest.",dc_bg,    dc_br,     dc,        False),
                        ("ti-tag",          _fmt_precio(metricas.get("precio_actual")), "",         "var(--color-background-secondary)", "#e2e8f0", "#374151", False),
                    ]:
                        with ui.element("div").style(
                            f"display:inline-flex;align-items:center;gap:4px;"
                            f"background:{bg};border:{'1.5px' if is_main else '0.5px'} solid {border};"
                            f"border-radius:20px;padding:4px 10px;"
                            f"{'min-width:92px;white-space:nowrap;' if is_main else ''}"
                        ):
                            ui.html(f'<i class="ti {icon}" style="font-size:12px;color:{color}" aria-hidden="true"></i>')
                            ui.label(val).style(f"font-size:{'13px' if is_main else '12px'};font-weight:{'700' if is_main else '500'};color:{color};white-space:nowrap")
                            if lbl:
                                ui.label(lbl).style(f"font-size:10px;color:{color};opacity:.7;white-space:nowrap")

                # Fila 2: detalle compacto
                p_detalle = p_min if p_iguales else f"{p_min} min \u00b7 {p_max} max \u00b7 {p_prom} prom"
                s_detalle = f"{_fmt_num(metricas.get('stock_max',0))} max \u00b7 {_fmt_num(metricas.get('stock_prom',0))} prom \u00b7 <span style='color:#dc2626'>{_fmt_num(metricas.get('vel_max_dia',0))}/d max</span>"
                d_detalle = f"<span style='color:#166534'>{_fmt_num(metricas.get('dias_con_stock',0))}</span> c/stock \u00b7 <span style='color:#dc2626'>{_fmt_num(metricas.get('dias_sin_stock',0))}</span> sin \u00b7 <span style='color:#185FA5'>{_fmt_num(metricas.get('n_reposiciones',0))}</span> repos"
                ui.html(
                    f'<div style="font-size:10px;color:#9ca3af;display:flex;flex-wrap:wrap;gap:4px;align-items:center">'
                    f'<span style="font-weight:500;color:#185FA5">Ticket:</span>'
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
                if i > 0 and repo == 0:
                    running_stock_days += 1
                running_sales += vend
                vel_acum = round(running_sales / running_stock_days, 1) if running_stock_days > 0 else None
                data.append({**r, "vend": vend, "repo": repo, "vel_acum": vel_acum})

            # Desglose por SKU (solo vista de marca) -- se calcula antes del layout porque
            # lo usan tanto la tabla en pantalla como el PDF (pdf_state["datos"]["desglose"]).
            desglose = None
            if marca and per_sku_series:
                desglose = []
                for sku_i, serie_i in per_sku_series.items():
                    met_i = _calcular_metricas(serie_i)
                    if not met_i:
                        continue
                    ticket_i = _calcular_ticket_ventas({sku_i: serie_i}, ventas_reales)
                    desglose.append({
                        "sku": sku_i,
                        "nombre": sku_options.get(sku_i, sku_i),
                        "stock": met_i.get("stock_actual", 0),
                        "ventas": met_i.get("ventas_total", 0),
                        "vel": met_i.get("vel_diaria", 0),
                        "dias_restantes": met_i.get("dias_restantes"),
                        "ticket_prom": ticket_i["ticket_prom_periodo"],
                    })
                desglose.sort(key=lambda x: x["vel"], reverse=True)

            pdf_state["datos"] = {
                "sku": sku, "marca": marca, "n_skus": n_skus,
                "desde": estado["desde"], "hasta": estado["hasta"],
                "metricas": metricas, "data": data, "desglose": desglose,
            }
            _set_pdf_habilitado(True)

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

            def _render_tabla_diaria():
                with ui.element("table").style("width:100%;border-collapse:collapse;font-size:10px"):
                    with ui.element("thead"):
                        with ui.element("tr"):
                            for h, align in [("Dia", "left"), ("Stock", "right"), ("Variacion", "right"), ("Vel. acum.", "right"), ("Ticket prom.", "right")]:
                                with ui.element("th").style(
                                    f"padding:3px 6px;background:#2A7AC7;color:#fff;"
                                    f"font-weight:500;text-align:{align};white-space:nowrap;"
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
                                        "padding:2px 6px;background:#EEF6FD;"
                                        "border-bottom:0.5px solid #d0e8f8;"
                                        "font-size:9px;font-weight:600;color:#185FA5"
                                    ).props('colspan="5"'):
                                        ui.html(mes_label)
                            stock  = r.get("stock") or 0
                            vend   = r["vend"]
                            repo   = r["repo"]
                            va     = r.get("vel_acum")
                            precio = _fmt_precio(r.get("price"))
                            bg = "background:#F0FDF4;" if repo > 0 else ""
                            with ui.element("tr").style(bg):
                                with ui.element("td").style("padding:2px 6px;border-bottom:0.5px solid #f1f5f9;text-align:left;color:#6b7280"):
                                    ui.html(dia_label)
                                with ui.element("td").style(f"padding:2px 6px;border-bottom:0.5px solid #f1f5f9;text-align:right;font-weight:500;color:{'#166534' if stock > 0 else '#9ca3af'}"):
                                    ui.html(_fmt_num(stock))
                                if repo > 0:
                                    vc, vt = "#166534", f"+{_fmt_num(repo)}"
                                elif vend > 0:
                                    vc, vt = "#dc2626", f"\u2212{_fmt_num(vend)}"
                                else:
                                    vc, vt = "#9ca3af", "\u2014"
                                with ui.element("td").style(f"padding:2px 6px;border-bottom:0.5px solid #f1f5f9;text-align:right;font-weight:500;color:{vc}"):
                                    ui.html(vt)
                                with ui.element("td").style("padding:2px 6px;border-bottom:0.5px solid #f1f5f9;text-align:right;color:#6b7280"):
                                    ui.html(f"{va}/d" if va is not None else "\u2014")
                                with ui.element("td").style("padding:2px 6px;border-bottom:0.5px solid #f1f5f9;text-align:right;color:#374151;min-width:60px"):
                                    ui.html(precio)

            def _render_tabla_desglose():
                with ui.element("table").style("width:100%;border-collapse:collapse;font-size:10px;table-layout:fixed"):
                    with ui.element("thead"):
                        with ui.element("tr"):
                            for h, align, w in [("SKU \u2014 Nombre", "left", None), ("Stock", "right", "68px"),
                                                 ("Ventas", "right", "68px"), ("Velocidad", "right", "78px"),
                                                 ("D\u00edas Restantes", "right", "100px"), ("Ticket Promedio", "right", "112px")]:
                                with ui.element("th").style(
                                    f"padding:5px 8px;background:#2A7AC7;color:#fff;"
                                    f"font-weight:500;text-align:{align};white-space:nowrap;"
                                    "border-right:0.5px solid rgba(255,255,255,0.15);"
                                    "position:sticky;top:0;z-index:2"
                                    + (f";width:{w}" if w else "")
                                ):
                                    ui.html(h)
                    with ui.element("tbody"):
                        for row_i in desglose:
                            dias_ri = row_i.get("dias_restantes")
                            dc_i = "#dc2626" if dias_ri and dias_ri < 7 else "#ca6d00" if dias_ri and dias_ri < 20 else "#166534"
                            with ui.element("tr").style("cursor:pointer").on(
                                "click", lambda sk=row_i["sku"]: sel.set_value(sk)
                            ):
                                with ui.element("td").style(
                                    "padding:3px 8px;border-bottom:0.5px solid #f1f5f9;text-align:left;"
                                    "font-weight:500;color:#374151;overflow:hidden;"
                                    "text-overflow:ellipsis;white-space:nowrap"
                                ):
                                    ui.html(row_i["nombre"])
                                with ui.element("td").style("padding:3px 8px;border-bottom:0.5px solid #f1f5f9;text-align:right;color:#0C447C"):
                                    ui.html(_fmt_num(row_i["stock"]))
                                with ui.element("td").style("padding:3px 8px;border-bottom:0.5px solid #f1f5f9;text-align:right;color:#991B1B"):
                                    ui.html(_fmt_num(row_i.get("ventas", 0)))
                                with ui.element("td").style("padding:3px 8px;border-bottom:0.5px solid #f1f5f9;text-align:right;font-weight:600;color:#C2410C"):
                                    ui.html(f"{row_i['vel']}/d")
                                with ui.element("td").style(f"padding:3px 8px;border-bottom:0.5px solid #f1f5f9;text-align:right;font-weight:500;color:{dc_i}"):
                                    ui.html(str(dias_ri) if dias_ri else "\u2014")
                                with ui.element("td").style("padding:3px 8px;border-bottom:0.5px solid #f1f5f9;text-align:right;color:#374151"):
                                    ui.html(_fmt_precio(row_i.get("ticket_prom")))

            _chart_option = {
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
                    }

            if marca:
                # Vista de marca: dos filas -- arriba tabla diaria (38%) + desglose por SKU
                # (62%) lado a lado con la misma altura fija, abajo el grafico a lo ancho
                # completo. El total de alto disponible es el mismo presupuesto (100vh menos
                # el resto de la UI) que antes usaban tabla+grafico lado a lado.
                _h_top = "calc((100vh - 450px) * 0.42)"
                _h_bottom = "calc((100vh - 450px) * 0.58 - 12px)"
                with ui.element("div").style("display:flex;flex-direction:column;gap:12px;width:100%"):
                    with ui.element("div").style(f"display:grid;grid-template-columns:38% 62%;gap:10px;height:{_h_top}"):
                        with ui.element("div").style(
                            "border:0.5px solid #e2e8f0;border-radius:8px;overflow:hidden;height:100%"
                        ):
                            with ui.element("div").style("overflow-y:auto;height:100%"):
                                _render_tabla_diaria()
                        with ui.element("div").style(
                            "border:0.5px solid #e2e8f0;border-radius:8px;overflow:hidden;height:100%"
                        ):
                            with ui.element("div").style("overflow-y:auto;height:100%"):
                                _render_tabla_desglose()
                    with ui.element("div").style(f"display:flex;flex-direction:column;gap:4px;height:{_h_bottom};width:100%"):
                        if dias_r:
                            ui.label(
                                f"Con {metricas.get('stock_actual')} unidades y vel. {vel}/d -> estimados {dias_r} dias de stock restantes."
                            ).style(f"font-size:11px;color:{dc};display:block")
                        pdf_state["chart"] = ui.echart(_chart_option).style("flex:1;min-height:0;width:100%")
            else:
                # Vista de SKU individual: layout anterior, tabla angosta + grafico lado a lado.
                with ui.element("div").style(
                    "display:grid;grid-template-columns:340px 1fr;gap:0;align-items:start;width:100%"
                ):
                    with ui.element("div").style(
                        "border:0.5px solid #e2e8f0;border-radius:8px 0 0 8px;overflow:hidden;margin-right:10px"
                    ):
                        with ui.element("div").style("overflow-y:auto;max-height:calc(100vh - 450px)"):
                            _render_tabla_diaria()
                    with ui.element("div").style("display:flex;flex-direction:column;gap:4px;min-width:0"):
                        if dias_r:
                            ui.label(
                                f"Con {metricas.get('stock_actual')} unidades y vel. {vel}/d -> estimados {dias_r} dias de stock restantes."
                            ).style(f"font-size:11px;color:{dc};display:block")
                        pdf_state["chart"] = ui.echart(_chart_option).style("height:calc(100vh - 450px);width:100%")

    def _pintar_resumen_marcas(resumen: List[Dict], desde: str, hasta: str):
        contenido_ref[0].clear()
        with contenido_ref[0]:
            if not resumen:
                ui.label("Sin datos de marcas para este periodo.").style(
                    "font-size:13px;color:#9ca3af;padding:24px"
                )
                return
            marca_header = f"Marca ({_iso_a_ddmm(desde)} al {_iso_a_ddmm(hasta)})"
            with ui.element("div").style(
                "border:0.5px solid #e2e8f0;border-radius:8px;overflow:hidden;max-width:820px"
            ):
                with ui.element("div").style("overflow-y:auto;max-height:calc(100vh - 260px)"):
                    with ui.element("table").style("width:100%;border-collapse:collapse;font-size:10px"):
                        with ui.element("thead"):
                            with ui.element("tr"):
                                for h, align in [(marca_header, "left"), ("Stock", "right"), ("Ventas", "right"), ("Velocidad", "right"), ("Dias Restantes", "right"), ("Ticket Promedio", "right")]:
                                    with ui.element("th").style(
                                        f"padding:5px 8px;background:#2A7AC7;color:#fff;"
                                        f"font-weight:500;text-align:{align};white-space:nowrap;"
                                        "border-right:0.5px solid rgba(255,255,255,0.15);"
                                        "position:sticky;top:0;z-index:2"
                                    ):
                                        ui.html(h)
                        with ui.element("tbody"):
                            for row in resumen:
                                dias_r = row.get("dias_restantes")
                                dc = "#dc2626" if dias_r and dias_r < 7 else "#ca6d00" if dias_r and dias_r < 20 else "#166534"
                                with ui.element("tr"):
                                    with ui.element("td").style("padding:3px 8px;border-bottom:0.5px solid #f1f5f9;text-align:left;font-weight:500;color:#374151"):
                                        ui.html(row["marca"])
                                    with ui.element("td").style("padding:3px 8px;border-bottom:0.5px solid #f1f5f9;text-align:right;color:#0C447C"):
                                        ui.html(_fmt_num(row["stock"]))
                                    with ui.element("td").style("padding:3px 8px;border-bottom:0.5px solid #f1f5f9;text-align:right;color:#991B1B"):
                                        ui.html(_fmt_num(row.get("ventas", 0)))
                                    with ui.element("td").style("padding:3px 8px;border-bottom:0.5px solid #f1f5f9;text-align:right;font-weight:600;color:#C2410C"):
                                        ui.html(f"{row['vel']}/d")
                                    with ui.element("td").style(f"padding:3px 8px;border-bottom:0.5px solid #f1f5f9;text-align:right;font-weight:500;color:{dc}"):
                                        ui.html(str(dias_r) if dias_r else "—")
                                    with ui.element("td").style("padding:3px 8px;border-bottom:0.5px solid #f1f5f9;text-align:right;color:#374151"):
                                        ui.html(_fmt_precio(row.get("ticket_prom")))

    async def _cargar():
        """Snapshotea desde/hasta al inicio (consistentes durante todo el llamado, aun con
        varios awaits de por medio) y usa un contador de generacion para descartar el pintado
        si mientras tanto se disparo un _cargar() mas nuevo (ej. el usuario cambia el preset
        de Fecha antes de que termine la consulta anterior) -- evita que una carga vieja y
        lenta pise en pantalla el resultado de una mas nueva."""
        estado["_load_seq"] = estado.get("_load_seq", 0) + 1
        mi_seq = estado["_load_seq"]
        desde, hasta = estado["desde"], estado["hasta"]

        if estado.get("vista_resumen"):
            rows = await run.io_bound(_get_resumen_marcas, user_id, desde, hasta)
            ventas_reales = await run.io_bound(_get_ventas_reales_por_sku_dia, user_id, desde, hasta)
            if estado["_load_seq"] != mi_seq:
                return
            resumen = _calcular_resumen_marcas(rows, ventas_reales)
            _pintar_resumen_marcas(resumen, desde, hasta)
            pdf_state["chart"] = None
            pdf_state["datos"] = None
            _set_pdf_habilitado(False)
            return
        sku = estado.get("sku")
        marca = estado.get("marca")
        if marca:
            rows, per_sku_series = await run.io_bound(_get_stock_history_marca, user_id, marca, desde, hasta)
            n_skus = await run.io_bound(_get_marca_n_skus, user_id, marca, desde, hasta)
            ventas_reales = await run.io_bound(_get_ventas_reales_por_sku_dia, user_id, desde, hasta)
            if estado["_load_seq"] != mi_seq:
                return
            met = _calcular_metricas(rows)
            _pintar(rows, met, marca=marca, n_skus=n_skus, per_sku_series=per_sku_series, ventas_reales=ventas_reales)
            return
        if not sku:
            contenido_ref[0].clear()
            with contenido_ref[0]:
                ui.label("Selecciona un SKU o una Marca para ver el historial.").style(
                    "font-size:13px;color:#9ca3af;padding:24px"
                )
            pdf_state["chart"] = None
            pdf_state["datos"] = None
            _set_pdf_habilitado(False)
            return
        rows = await run.io_bound(_get_stock_history, user_id, sku, desde, hasta)
        ventas_reales = await run.io_bound(_get_ventas_reales_por_sku_dia, user_id, desde, hasta)
        if estado["_load_seq"] != mi_seq:
            return
        met = _calcular_metricas(rows)
        _pintar(rows, met, sku=sku, per_sku_series=({sku: rows} if rows else None), ventas_reales=ventas_reales)

    # Layout principal
    with ui.element("div").style("padding:10px 20px 0"):
        with ui.row().style("gap:8px;align-items:flex-end;flex-wrap:wrap;margin-bottom:8px"):
            with ui.column().style("gap:3px"):
                ui.label("SKU").style("font-size:11px;color:var(--color-text-secondary)")
                sel = ui.select(options=sku_options, value=None, label="").props(
                    "dense outlined clearable use-input input-debounce=200"
                ).style("width:300px;font-size:12px")
            with ui.column().style("gap:3px"):
                ui.label("Marca").style("font-size:11px;color:var(--color-text-secondary)")
                sel_marca = ui.select(options=["Todas"] + marcas, value="Todas", label="").props(
                    "dense outlined use-input input-debounce=200"
                ).style("width:200px;font-size:12px")

            def _sync_select(el, value):
                """Cambia el value de un select sin disparar su propio on_value_change --
                evita que el reseteo programatico del select 'contrario' pise el estado
                que este handler acaba de setear (ej. elegir SKU resetea Marca a "Todas",
                pero eso no debe volver a activar vista_resumen)."""
                estado["_syncing"] = True
                el.set_value(value)
                estado["_syncing"] = False

            def _on_sku(e):
                if estado.get("_syncing"):
                    return
                estado["sku"] = e.value
                estado["marca"] = None
                estado["vista_resumen"] = not bool(e.value)
                _sync_select(sel_marca, "Todas")
                ui.timer(0.05, _cargar, once=True)
            sel.on_value_change(_on_sku)

            def _on_marca(e):
                if estado.get("_syncing"):
                    return
                if not e.value or e.value == "Todas":
                    estado["marca"] = None
                    estado["vista_resumen"] = True
                    if not e.value:
                        _sync_select(sel_marca, "Todas")
                else:
                    estado["marca"] = e.value
                    estado["sku"] = None
                    estado["vista_resumen"] = False
                    _sync_select(sel, None)
                ui.timer(0.05, _cargar, once=True)
            sel_marca.on_value_change(_on_marca)

            _pickers_disabled_inicial = estado["fecha_preset"] != "Fecha predeterminada"
            with ui.column().style("gap:3px"):
                ui.label("Fecha").style("font-size:11px;color:var(--color-text-secondary)")
                sel_fecha = ui.select(options=FECHA_PRESETS, value=estado["fecha_preset"], label="").props(
                    "dense outlined"
                ).style("width:190px;font-size:12px")
            with ui.column().style("gap:3px"):
                ui.label("Desde").style("font-size:11px;color:var(--color-text-secondary)")
                inp_desde = ui.input(value=_iso_a_ddmmyyyy(estado["desde"])).props(
                    "dense outlined mask='##/##/####'"
                    + (" disable" if _pickers_disabled_inicial else "")
                ).style("width:140px")
                def _on_desde(e):
                    iso = _ddmmyyyy_a_iso(e.value)
                    if iso:
                        iso_min = fecha_minima or "1970-01-01"
                        iso = max(iso_min, min(iso, hoy.isoformat()))
                        estado["desde"] = iso
                inp_desde.on_value_change(_on_desde)
                with inp_desde.add_slot("append"):
                    icon_desde = ui.icon("edit_calendar").classes("cursor-pointer").on(
                        "click", lambda: menu_desde.open() if estado["fecha_preset"] == "Fecha predeterminada" else None
                    ).style("opacity:0.35;pointer-events:none" if _pickers_disabled_inicial else "")
                with ui.menu().props("no-parent-event") as menu_desde:
                    ui.date(value=_iso_a_ddmmyyyy(estado["desde"])).props(
                        _date_range_props
                    ).bind_value(inp_desde)
                menu_desde.on("hide", lambda: ui.timer(0.05, _cargar, once=True))
            with ui.column().style("gap:3px"):
                ui.label("Hasta").style("font-size:11px;color:var(--color-text-secondary)")
                inp_hasta = ui.input(value=_iso_a_ddmmyyyy(estado["hasta"])).props(
                    "dense outlined mask='##/##/####'"
                    + (" disable" if _pickers_disabled_inicial else "")
                ).style("width:140px")
                def _on_hasta(e):
                    iso = _ddmmyyyy_a_iso(e.value)
                    if iso:
                        iso_min = fecha_minima or "1970-01-01"
                        iso = max(iso_min, min(iso, hoy.isoformat()))
                        estado["hasta"] = iso
                inp_hasta.on_value_change(_on_hasta)
                with inp_hasta.add_slot("append"):
                    icon_hasta = ui.icon("edit_calendar").classes("cursor-pointer").on(
                        "click", lambda: menu_hasta.open() if estado["fecha_preset"] == "Fecha predeterminada" else None
                    ).style("opacity:0.35;pointer-events:none" if _pickers_disabled_inicial else "")
                with ui.menu().props("no-parent-event") as menu_hasta:
                    ui.date(value=_iso_a_ddmmyyyy(estado["hasta"])).props(
                        _date_range_props
                    ).bind_value(inp_hasta)
                menu_hasta.on("hide", lambda: ui.timer(0.05, _cargar, once=True))

            def _set_pickers_disabled(disabled: bool) -> None:
                if disabled:
                    inp_desde.props("disable")
                    inp_hasta.props("disable")
                    icon_desde.style("opacity:0.35;pointer-events:none")
                    icon_hasta.style("opacity:0.35;pointer-events:none")
                else:
                    inp_desde.props(remove="disable")
                    inp_hasta.props(remove="disable")
                    icon_desde.style("opacity:1;pointer-events:auto")
                    icon_hasta.style("opacity:1;pointer-events:auto")

            def _on_fecha_preset(e):
                estado["fecha_preset"] = e.value
                rango = _calc_rango_preset(e.value)
                if rango:
                    estado["desde"], estado["hasta"] = rango
                    inp_desde.set_value(_iso_a_ddmmyyyy(estado["desde"]))
                    inp_hasta.set_value(_iso_a_ddmmyyyy(estado["hasta"]))
                    _set_pickers_disabled(True)
                else:
                    _set_pickers_disabled(False)
                ui.timer(0.05, _cargar, once=True)
            sel_fecha.on_value_change(_on_fecha_preset)

            pdf_spinner = ui.spinner(size="sm", color="#2A7AC7").style("display:none;align-self:flex-end;margin-bottom:8px")
            with ui.element("button").on(
                "click", lambda: ui.timer(0.05, _descargar_pdf, once=True)
            ).style(
                "height:34px;font-size:12px;font-weight:500;"
                "border:1px solid #2A7AC7;border-radius:4px;background:#FFFFFF;"
                "padding:0 16px;cursor:default;color:#2A7AC7;align-self:flex-end;"
                "opacity:0.4;pointer-events:none"
            ) as btn_pdf:
                ui.html('<i class="ti ti-download" style="font-size:13px;margin-right:4px"></i>Reporte')

            marcas_spinner = ui.spinner(size="sm", color="#2A7AC7").style("display:none;align-self:flex-end;margin-bottom:8px")
            with ui.element("button").on(
                "click", lambda: ui.timer(0.05, _descargar_pdf_marcas, once=True)
            ).style(
                "height:34px;font-size:12px;font-weight:500;"
                "border:1px solid #2A7AC7;border-radius:4px;background:#FFFFFF;"
                "padding:0 16px;cursor:pointer;color:#2A7AC7;align-self:flex-end"
            ):
                ui.html('<i class="ti ti-download" style="font-size:13px;margin-right:4px"></i>Marcas')

            with ui.element("button").on(
                "click", lambda: ui.timer(0.05, _cargar, once=True)
            ).style(
                "height:34px;font-size:12px;font-weight:500;"
                "border:1px solid #2A7AC7;border-radius:4px;background:#2A7AC7;"
                "padding:0 16px;cursor:pointer;color:#FFFFFF;align-self:flex-end"
            ):
                ui.html('<i class="ti ti-refresh" style="font-size:13px;margin-right:4px"></i>Actualizar')

            async def _descargar_pdf_marcas() -> None:
                marcas_spinner.style("display:inline-block")
                try:
                    rows = await run.io_bound(_get_resumen_marcas, user_id, estado["desde"], estado["hasta"])
                    ventas_reales = await run.io_bound(
                        _get_ventas_reales_por_sku_dia, user_id, estado["desde"], estado["hasta"]
                    )
                    resumen = _calcular_resumen_marcas(rows, ventas_reales)
                    razon_social = await run.io_bound(get_user_ml_razon_social, user_id) or user.get("username", "")
                    path, nombre = await run.io_bound(
                        _generar_pdf_marcas, resumen, estado["desde"], estado["hasta"], razon_social
                    )
                    ui.download(path, nombre)
                    ui.notify(f"Exportado: {nombre}", color="positive")

                    def _cleanup() -> None:
                        try:
                            if path and os.path.exists(path):
                                os.unlink(path)
                        except Exception:
                            pass
                    ui.timer(5.0, _cleanup, once=True)
                except Exception as ex:
                    ui.notify(f"Error generando PDF: {ex}", color="negative")
                finally:
                    marcas_spinner.style("display:none")

            def _set_pdf_habilitado(hab: bool) -> None:
                pdf_state["habilitado"] = hab
                if hab:
                    btn_pdf.style("opacity:1;pointer-events:auto;cursor:pointer")
                else:
                    btn_pdf.style("opacity:0.4;pointer-events:none;cursor:default")

            async def _descargar_pdf() -> None:
                if not pdf_state.get("habilitado") or not pdf_state.get("datos"):
                    return
                _set_pdf_habilitado(False)
                pdf_spinner.style("display:inline-block")
                try:
                    chart_b64 = None
                    chart_el = pdf_state.get("chart")
                    if chart_el is not None:
                        try:
                            chart_b64 = await chart_el.run_chart_method(
                                "getDataURL", {"type": "png", "pixelRatio": 2, "backgroundColor": "#ffffff"}
                            )
                        except Exception:
                            chart_b64 = None
                    razon_social = await run.io_bound(get_user_ml_razon_social, user_id) or user.get("username", "")
                    path, nombre = await run.io_bound(
                        _generar_pdf_stock, pdf_state["datos"], razon_social, chart_b64
                    )
                    ui.download(path, nombre)
                    ui.notify(f"Exportado: {nombre}", color="positive")

                    def _cleanup() -> None:
                        try:
                            if path and os.path.exists(path):
                                os.unlink(path)
                        except Exception:
                            pass
                    ui.timer(5.0, _cleanup, once=True)
                except Exception as ex:
                    ui.notify(f"Error generando PDF: {ex}", color="negative")
                finally:
                    pdf_spinner.style("display:none")
                    _set_pdf_habilitado(bool(pdf_state.get("datos")))

    # Contenido (sin padding lateral para que el grafico llegue al borde)
    with ui.element("div").style("padding:0 0 0 20px;width:100%"):
        cont = ui.element("div").style("width:100%")
        contenido_ref[0] = cont
        with cont:
            ui.label("Cargando...").style(
                "font-size:13px;color:#9ca3af;padding:24px"
            )

    # Vista de aterrizaje: "Todas" viene seleccionada por defecto -> mostrar el resumen
    # de marcas apenas carga la pagina, sin esperar a que el usuario interactue.
    ui.timer(0.1, _cargar, once=True)
