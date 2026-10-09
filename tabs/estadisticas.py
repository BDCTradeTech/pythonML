"""
Fase 3 — tabs/estadisticas.py
Pestaña Estadísticas: datos de la cuenta ML, reputación y ventas.
"""
from __future__ import annotations
import calendar
import json
import math
import re
import html as _html
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from nicegui import app, background_tasks, run, ui

from ml_api import (
    get_ml_access_token,
    get_ml_session,
    ml_get_user_profile,
    ml_get_user_id,
    ml_get_orders_incremental,
    ml_get_orders,
    ml_get_pending_labels,
    ml_get_my_items,
    ml_get_unanswered_questions,
    ml_get_response_time,
    ml_get_dispatch_schedule,
    ml_get_shipping_preferences,
    _parse_ml_item_body,
)
from db import get_connection, get_cotizador_param, get_marca_override_map, get_ads_campaign_daily_range, set_cotizador_param
from sales_core import es_venta, fecha_venta, monto_venta, unidades_venta


# ---------------------------------------------------------------------------
# Helpers de sesión
# ---------------------------------------------------------------------------

def _require_login() -> Optional[Dict[str, Any]]:
    user = app.storage.user.get("user")
    if not user:
        ui.notify("Debes iniciar sesión para continuar", color="negative")
    return user


# ---------------------------------------------------------------------------
# Helpers de formato (exclusivos de esta tab)
# ---------------------------------------------------------------------------

def fmt_m(val) -> str:
    try:
        return f"${int(round(float(val))):,}".replace(",", ".")
    except Exception:
        return "$0"


_PROMO_ROSA = "#DB2777"
_CUOTAS_AZUL = "#2563EB"
_PUB_VIOLETA = "#7C3AED"


def _promo_de_orden(items: List[Any], payments: List[Any]) -> Tuple[int, float, float, float]:
    """(unidades, importe, precio_lista, cupones) de una orden, contando solo los ítems con promo del vendedor.
    Un ítem tiene promo si gross_price (precio de lista de la línea) > unit_price * cantidad: es el mismo criterio
    que usa la pestaña Ventas para el tag "Promo". Los cupones son el coupon_amount de los pagos aprobados
    (cupones al comprador, financiados por ML o por el vendedor: el cache no distingue quién pone cada uno),
    solo si la orden tiene promo. (0, 0, 0, 0) si no hay promo."""
    uds, imp, lista = 0, 0.0, 0.0
    for it in items or []:
        if not isinstance(it, dict):
            continue
        try:
            q = int(it.get("quantity") or 0)
            up = float(it.get("unit_price") or 0)
            gross = float(it.get("gross_price") or 0)
        except (TypeError, ValueError):
            continue
        if q > 0 and gross > up * q + 0.01:
            uds += q
            imp += up * q
            lista += gross
    if not uds:
        return 0, 0.0, 0.0, 0.0
    aporte = 0.0
    for p in payments or []:
        if isinstance(p, dict) and p.get("status") in (None, "approved"):
            try:
                aporte += float(p.get("coupon_amount") or 0)
            except (TypeError, ValueError):
                pass
    return uds, imp, lista, aporte


def _margen_por_mes(user_id: Optional[int], ordenes: List[Dict[str, Any]], meses: List[str]) -> Dict[str, Dict[str, Any]]:
    """Margen ponderado por mes = SUM(ganancia real) / SUM(facturacion) de las ordenes que cuenta sales_core.es_venta
    (ordenes ya filtradas), con la ganancia de ventas_datos.gan_pesos (suma de los pagos de la orden) y la facturacion
    de sales_core.monto_venta (la misma de la columna $ ARS). Solo entran al cociente las ordenes con ganancia cargada
    (en numerador y denominador), para que una orden sin dato no baje el %. Devuelve {mes: {"pct", "falta", "ordenes"}};
    "falta" = ordenes sin ganancia o con ganancia estimada (fee_origen = 'estimada'). pct None = mes sin datos.
    "gan" = suma de la ganancia de las ordenes que entran al cociente."""
    if user_id is None:
        return {}
    try:
        conn = get_connection()
        try:
            rows = conn.execute("SELECT order_id, gan_pesos, fee_origen FROM ventas_datos WHERE user_id=?", (user_id,)).fetchall()
        finally:
            conn.close()
    except Exception:
        logging.exception("[ESTADISTICAS] no se pudo leer ventas_datos para el margen mensual")
        return {}
    vd: Dict[str, List[Any]] = {}  # order_id -> [ganancia, pagos, pagos sin ganancia, hay estimada]
    for oid, gan, fo in rows:
        d = vd.setdefault(str(oid), [0.0, 0, 0, False])
        d[1] += 1
        if gan is None:
            d[2] += 1
        else:
            d[0] += float(gan)
        if (fo or "") == "estimada":
            d[3] = True
    acc: Dict[str, Dict[str, float]] = {m: {"gan": 0.0, "fact": 0.0, "ordenes": 0, "falta": 0} for m in meses}
    for o in ordenes:
        dt = fecha_venta(o)
        a = acc.get(dt.strftime("%Y-%m")) if dt else None
        if a is None:
            continue
        a["ordenes"] += 1
        d = vd.get(str(o.get("order_id") or o.get("id") or ""))
        if d is None or d[2] == d[1]:
            a["falta"] += 1  # sin ganancia: no entra al cociente
            continue
        if d[2] > 0 or d[3]:
            a["falta"] += 1  # estimada o parcial: entra, pero cuenta para el aviso
        a["gan"] += d[0]
        a["fact"] += monto_venta(o)
    return {m: {"pct": (a["gan"] / a["fact"] * 100) if a["fact"] > 0 else None, "gan": float(a["gan"]),
                "falta": int(a["falta"]), "ordenes": int(a["ordenes"])} for m, a in acc.items()}


def _margen_visual(m: Optional[Dict[str, Any]]) -> Tuple[str, str, Optional[str]]:
    """(texto, color, tooltip) de un mes de _margen_por_mes: verde >=10%, naranja 0-10%, rojo <0; gris con
    tooltip si mas del 10% de las ordenes del mes no tienen ganancia real; "—" gris si no hay datos."""
    pct = (m or {}).get("pct")
    if pct is None:
        return "—", "#9ca3af", None
    falta, ordenes = int((m or {}).get("falta") or 0), int((m or {}).get("ordenes") or 0)
    txt = f"{_fmt_dec(pct, 1)}%"
    if ordenes and falta / ordenes > 0.10:
        return txt, "#9ca3af", f"{falta} órdenes sin ganancia real"
    return txt, ("#16a34a" if pct >= 10 else ("#ea580c" if pct >= 0 else "#dc2626")), None


_CAL_AZUL = [("#DBEAFE", "#1E3A8A"), ("#BFDBFE", "#1E3A8A"), ("#93C5FD", "#1E3A8A"),
             ("#60A5FA", "#1E3A8A"), ("#2563EB", "#FFFFFF"), ("#1D4ED8", "#FFFFFF")]
_CAL_GRIS = [("#F3F4F6", "#374151"), ("#E5E7EB", "#374151"), ("#D1D5DB", "#374151"),
             ("#9CA3AF", "#374151"), ("#6B7280", "#FFFFFF"), ("#4B5563", "#FFFFFF")]


def _abrev_pesos(v: float, corto: bool = False) -> str:
    """$9,3M / $224M / $850k / $0 (decimal con coma). corto: sin decimal ($16M) para celdas muy angostas."""
    if v >= 1_000_000 and corto:
        return f"${v / 1_000_000:.0f}M"
    if v >= 1_000_000:
        return f"${_fmt_dec(v / 1_000_000, 0 if v >= 100_000_000 else 1)}M"
    if v >= 1_000:
        return f"${v / 1_000:.0f}k"
    return f"${int(v)}"


def _tono_cal(uds: int, prom: float) -> int:
    """Indice 0-5 de la escala segun las unidades del dia relativas al promedio diario de la ventana."""
    r = uds / prom if prom > 0 else 0.0
    return 0 if r < 0.6 else 1 if r < 0.9 else 2 if r < 1.1 else 3 if r < 1.3 else 4 if r < 1.6 else 5


def _pintar_calendario(ventas: Dict[str, int], facturado: Dict[str, float], hoy: Any) -> None:
    """Calendario de ventas de los ultimos 30 dias (hoy + 30 hacia atras = 31 dias, igual que el cuadro "30 días" de Ventas por periodo): filas = semanas LUN-DOM + columna SEMANA.
    El alto de las celdas lo reparte el grid (filas 1fr) entre el alto disponible de la tarjeta, sea de 5 o 6 semanas."""
    ini = hoy - timedelta(days=30)
    dias = [ini + timedelta(days=i) for i in range(31)]
    uds_tot = sum(ventas.get(d.strftime("%Y-%m-%d"), 0) for d in dias)
    prom = uds_tot / 31
    lunes0 = ini - timedelta(days=ini.weekday())
    n_sem = (hoy - lunes0).days // 7 + 1
    celdas = ['<div class="cal-h">' + x + "</div>" for x in ("LUN", "MAR", "MIÉ", "JUE", "VIE", "SÁB", "DOM")]
    celdas.append('<div class="cal-h">SEMANA</div>')
    for w in range(n_sem):
        su, sf = 0, 0.0
        for c in range(7):
            d = lunes0 + timedelta(days=w * 7 + c)
            if d < ini or d > hoy:
                celdas.append("<div></div>")
                continue
            k = d.strftime("%Y-%m-%d")
            u, f = ventas.get(k, 0), facturado.get(k, 0.0)
            su += u
            sf += f
            bg, fg = (_CAL_AZUL if (d.year, d.month) == (hoy.year, hoy.month) else _CAL_GRIS)[_tono_cal(u, prom)]
            dia = f"<b>1<span class=\"cal-mes\"> {_MESES_ABR[d.strftime('%m')].upper()}</span></b>" if d.day == 1 else str(d.day)
            es_hoy = d == hoy
            sombra = "box-shadow:inset 0 0 0 2px #16A34A;" if es_hoy else ""
            tag_hoy = '<span class="cal-hoy">HOY</span>' if es_hoy else ""
            tip = f"{d.strftime('%d/%m/%Y')}: {u} u · ${fmt_n(f)}"
            celdas.append(
                f'<div class="cal-c" title="{tip}" style="background:{bg};color:{fg};{sombra}">'
                f'<span class="cal-d">{dia}</span>{tag_hoy}<span class="cal-u">{u}</span>'
                f'<span class="cal-f"><span class="fl">{_abrev_pesos(f)}</span><span class="fs">{_abrev_pesos(f, True)}</span></span></div>'
            )
        celdas.append(f'<div class="cal-s"><b>{fmt_n(su)}u</b><span>{_abrev_pesos(sf)}</span></div>')
    leyenda = ""
    if ini.month != hoy.month:
        leyenda += f'<span style="color:#9CA3AF">■</span> {_MESES_NOMBRE[ini.strftime("%m")]} '
    leyenda += f'<span style="color:#3B82F6">■</span> {_MESES_NOMBRE[hoy.strftime("%m")]}'
    escala = "".join(f'<i style="background:{bg}"></i>' for bg, _ in _CAL_AZUL)
    ui.add_css(
        ".cal-wrap{container-type:inline-size;flex:1;min-height:0;display:flex;flex-direction:column}"
        ".cal-g{display:grid;grid-template-columns:repeat(7,minmax(0,1fr)) minmax(44px,.9fr);gap:3px;flex:1;min-height:0}"
        ".cal-h{font-size:8px;line-height:10px;color:#9CA3AF;text-align:center;letter-spacing:.04em}"
        ".cal-c{position:relative;border-radius:4px;min-height:0;overflow:hidden}"
        ".cal-d{position:absolute;top:2px;left:3px;font-size:8px;line-height:9px}"
        ".cal-hoy{position:absolute;top:2px;right:3px;font-size:7px;line-height:9px;font-weight:700}"
        ".cal-u{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700}"
        ".cal-f{position:absolute;bottom:2px;left:0;right:0;text-align:center;font-size:8px;line-height:9px}"
        ".cal-f .fs{display:none}"
        ".cal-s{display:flex;flex-direction:column;align-items:center;justify-content:center;min-height:0}"
        ".cal-s b{font-size:11px;line-height:12px;color:#1D4ED8}"
        ".cal-s span{font-size:8px;line-height:9px;color:#6B7280}"
        ".cal-esc i{display:inline-block;width:9px;height:9px;border-radius:2px;margin:0 1px;vertical-align:-1px}"
        "@container (max-width:275px){.cal-f .fl{display:none}.cal-f .fs{display:inline}}"
        "@media (max-width:640px){"
        ".cal-wrap{flex:none}.cal-g{flex:none;grid-template-rows:10px repeat(var(--n),46px)!important}"
        ".cal-d{font-size:7px;line-height:8px}.cal-u{font-size:12px}.cal-f{font-size:7px;line-height:8px}"
        ".cal-mes,.cal-hoy{display:none}}"
    )
    ui.html(
        '<div class="cal-wrap">'
        f'<div class="cal-g" style="--n:{n_sem};grid-template-rows:10px repeat({n_sem},minmax(0,1fr))">{"".join(celdas)}</div>'
        '<div style="display:flex;justify-content:space-between;align-items:center;font-size:9px;color:#6B7280;margin-top:5px;white-space:nowrap">'
        f'<span>{leyenda}</span><span class="cal-esc">menos {escala} más</span></div>'
        "</div>"
    ).style("flex:1;min-height:0;display:flex;flex-direction:column")


def _media_movil(serie: List[float], n: int) -> List[float]:
    """Promedio movil de n dias: el punto i promedia serie[i-n+1..i] (los dias sin ventas ya vienen como 0)."""
    return [sum(serie[i - n + 1:i + 1]) / n for i in range(n - 1, len(serie))]


def _svg_velocimetro(pct: float, tip: str, ancho: int = 55) -> str:
    """Semicirculo de -40% a +40% (rojo -40/-10, gris -10/+10, verde +10/+40) con aguja que se corta en los extremos."""
    cx, cy, r = 35.0, 36.0, 29.0

    def _pt(ang: float, rad: float) -> str:
        return f"{cx + rad * math.cos(math.radians(ang)):.2f},{cy - rad * math.sin(math.radians(ang)):.2f}"

    def _arco(a0: float, a1: float, col: str) -> str:
        return (f'<path d="M{_pt(a0, r)} A{r},{r} 0 0 1 {_pt(a1, r)}" fill="none" stroke="{col}" stroke-width="7"/>')

    ang = 180.0 - (max(-40.0, min(40.0, pct)) + 40.0) / 80.0 * 180.0
    return (
        f'<svg viewBox="0 0 70 40" width="{ancho}" height="{ancho * 40 / 70:.1f}" style="display:block"><title>{tip}</title>'
        + _arco(180, 135, "#FCA5A5") + _arco(135, 45, "#E5E7EB") + _arco(45, 0, "#86EFAC")
        + f'<line x1="{cx}" y1="{cy}" x2="{_pt(ang, r - 6).split(",")[0]}" y2="{_pt(ang, r - 6).split(",")[1]}" '
          f'stroke="#111827" stroke-width="1.8" stroke-linecap="round"/>'
        + f'<circle cx="{cx}" cy="{cy}" r="2.6" fill="#111827"/></svg>'
    )


def _svg_aceleracion(fechas: List[Any], m7: List[float], prom: float, fmt: Callable[[float], str]) -> Tuple[str, float, float]:
    """Mini grafico de 90 dias: media movil 7d (azul), recta del promedio de 90 dias (gris punteada) y el area entre ambas
    (verde si 7d > promedio, roja si no, cortada en los cruces). SVG con viewBox fijo que se estira al contenedor
    (preserveAspectRatio none, trazos non-scaling), sin ejes ni grilla. Devuelve (svg, y del ultimo punto 7d en %, y de la recta en %)."""
    n = len(m7)
    w, h = 200.0, 60.0
    lo = min(min(m7), prom)
    hi = max(max(m7), prom)
    rng = (hi - lo) or 1.0
    lo -= rng * 0.08
    hi += rng * 0.08

    def _x(i: float) -> float:
        return i / (n - 1) * w if n > 1 else 0.0

    def _y(v: float) -> float:
        return h - (v - lo) / (hi - lo) * h

    verde: List[str] = []
    rojo: List[str] = []

    def _poli(lst: List[str], pts: List[Any]) -> None:
        lst.append("M" + " L".join(f"{x:.2f},{y:.2f}" for x, y in pts) + " Z")

    yp = _y(prom)
    for i in range(n - 1):
        d0, d1 = m7[i] - prom, m7[i + 1] - prom
        x0, x1 = _x(i), _x(i + 1)
        if d0 * d1 >= 0:
            _poli(verde if (d0 + d1) >= 0 else rojo, [(x0, _y(m7[i])), (x1, _y(m7[i + 1])), (x1, yp), (x0, yp)])
        else:
            t = d0 / (d0 - d1)
            xc = x0 + (x1 - x0) * t
            _poli(verde if d0 > 0 else rojo, [(x0, _y(m7[i])), (xc, yp), (x0, yp)])
            _poli(verde if d1 > 0 else rojo, [(xc, yp), (x1, _y(m7[i + 1])), (x1, yp)])
    l7 = "M" + " L".join(f"{_x(i):.2f},{_y(v):.2f}" for i, v in enumerate(m7))
    bw = w / n
    cols = "".join(
        f'<rect x="{_x(i) - bw / 2:.2f}" y="0" width="{bw:.2f}" height="{h}" fill="transparent">'
        f'<title>{fechas[i].strftime("%d/%m/%Y")}: prom. 7d {fmt(m7[i])} · prom. 90d {fmt(prom)}</title></rect>'
        for i in range(n)
    )
    svg = (
        f'<svg viewBox="0 0 {w:.0f} {h:.0f}" preserveAspectRatio="none" '
        f'style="position:absolute;inset:0;width:100%;height:100%;display:block">'
        f'<path d="{" ".join(verde)}" fill="#BBF7D0"/><path d="{" ".join(rojo)}" fill="#FECACA"/>'
        f'<line x1="0" y1="{yp:.2f}" x2="{w:.0f}" y2="{yp:.2f}" stroke="#6B7280" stroke-width="1.2" '
        f'stroke-dasharray="4 3" vector-effect="non-scaling-stroke"/>'
        f'<path d="{l7}" fill="none" stroke="#2563EB" stroke-width="1.8" vector-effect="non-scaling-stroke"/>'
        f'{cols}</svg>'
    )
    return svg, _y(m7[-1]) / h * 100, yp / h * 100


def _pintar_aceleracion(ventas: Dict[str, int], facturado: Dict[str, float], hoy: Any) -> None:
    """Tarjeta ACELERACION DE VENTAS: por cada serie (unidades, facturado) una frase, un velocimetro (promedio de la ultima
    semana vs promedio de 90 dias) y el grafico de 90 dias con la media movil de 7 dias contra la recta del promedio de 90.
    HOY no cuenta (dia incompleto): todas las ventanas terminan AYER; se leen 96 dias para que el primer punto de los 90
    tenga su promedio de 7 dias completo. Las filas se reparten el alto de la tarjeta (flex 1 con min-height 0)."""
    ayer = hoy - timedelta(days=1)
    fechas = [ayer - timedelta(days=i) for i in range(95, -1, -1)]  # 96 dias completos, del mas viejo al mas nuevo
    ui.add_css(
        ".ac-w{container-type:inline-size;flex:1;min-height:0;display:flex;flex-direction:column}"
        ".ac-r{--ach:42px;flex:1 1 0;min-height:0;display:flex;flex-direction:column;gap:2px;background:#F9FAFB;border:1px solid #F3F4F6;"
        "border-radius:6px;padding:4px 8px}"
        ".ac-f{font-size:9.5px;line-height:12px;color:#374151;white-space:nowrap}.ac-f .fp-l{display:none}"
        ".ac-f .fp-c{display:inline}"
        ".ac-b{flex:0 0 auto;display:flex;align-items:center;gap:8px}"
        ".ac-g{flex:0 0 56px;display:flex;flex-direction:column;align-items:center;text-align:center}"
        ".ac-c{flex:1;min-width:0;display:flex;flex-direction:column}"
        ".ac-cr{position:relative;height:var(--ach)}"
        ".ac-v{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));text-align:center;margin-top:2px}"
        ".ac-v span{display:block;font-size:8px;line-height:9px;color:#9CA3AF}"
        ".ac-v b{display:block;font-size:9.5px;line-height:11px;font-weight:700;white-space:nowrap}"
        "@container (max-width:340px){.ac-v b{font-size:8.5px}}"
        "@container (max-width:300px){.ac-b{flex-direction:column;align-items:stretch}.ac-g{flex:0 0 auto;align-self:center}"
        ".ac-c{display:block}.ac-r{--ach:70px}}"
        "@media (max-width:640px){.ac-w{flex:none}.ac-r{flex:none;--ach:70px}"
        ".ac-f{white-space:normal}.ac-f .fp-l{display:inline}.ac-f .fp-c{display:none}}"
    )
    filas = []
    for nombre, datos, fmt_v, fmt_dia in (
        ("Unidades", ventas, lambda v: _fmt_dec(v, 1), lambda v: f"{_fmt_dec(v, 1)} u/día"),
        ("Facturado", facturado, _abrev_pesos, lambda v: f"{_abrev_pesos(v)}/día"),
    ):
        serie = [float(datos.get(f.strftime("%Y-%m-%d"), 0) or 0) for f in fechas]
        m7 = _media_movil(serie, 7)  # 90 puntos (el primero promedia los 7 dias que terminan en el dia 90 hacia atras)
        a7 = m7[-1]
        a90 = sum(serie[-90:]) / 90
        sin_ventas = a90 <= 0
        pct = 0.0 if sin_ventas else (a7 / a90 - 1) * 100
        col = "#6B7280" if sin_ventas else ("#16A34A" if pct > 10 else ("#DC2626" if pct < -10 else "#6B7280"))
        pct_txt = f"{round(pct):+d}%".replace("-", "−")
        tip = f"Esta semana: {fmt_dia(a7)} · Promedio de 90 días: {fmt_dia(a90)}"
        if sin_ventas:
            frase = f"<b>{nombre}:</b> Sin ventas en los últimos 90 días"
            graf = ('<div style="position:absolute;inset:0;display:flex;align-items:center;justify-content:center;'
                    'font-size:10px;color:#9CA3AF">Sin ventas</div>')
        else:
            r = round(pct)
            rel = "sobre" if r > 0 else ("debajo de" if r < 0 else "en línea con")
            _p = f'<b style="color:{col}">{pct_txt}</b> {rel}'
            frase = (f'<b>{nombre}:</b> esta semana {fmt_dia(a7)}, {_p} '
                     f'<span class="fp-l">tu promedio de 90 días</span><span class="fp-c">prom. 90 días</span> ({fmt_v(a90)})')
            svg, y7, _yp = _svg_aceleracion(fechas[-90:], m7, a90, fmt_v)
            graf = (svg + f'<div style="position:absolute;right:0;top:{y7:.1f}%;width:6px;height:6px;border-radius:50%;'
                    'background:#2563EB;transform:translate(50%,-50%);pointer-events:none"></div>')

        celdas = []
        txt90 = fmt_v(a90)
        for rot, n_d in (("90d", 90), ("60d", 60), ("30d", 30), ("15d", 15), ("7d", 7), ("ayer", 1)):
            v = sum(serie[-n_d:]) / n_d
            txt = fmt_v(v) if not (n_d == 1 and v == int(v) and nombre == "Unidades") else str(int(v))
            c = "#6B7280" if n_d == 90 or fmt_v(v) == txt90 else ("#16A34A" if v > a90 else "#DC2626")
            ttl = "Vendido ayer" if n_d == 1 else f"Promedio por día de los últimos {n_d} días completos (hasta ayer)"
            celdas.append(f'<div title="{ttl}"><span>{rot}</span><b style="color:{c}">{txt}</b></div>')
        filas.append(
            '<div class="ac-r">'
            f'<div class="ac-f">{frase}</div>'
            f'<div class="ac-b"><div class="ac-g" title="{tip}">{_svg_velocimetro(pct, tip, 48)}'
            f'<div style="font-size:13px;line-height:14px;font-weight:700;color:{col}">{pct_txt}</div></div>'
            f'<div class="ac-c"><div class="ac-cr">{graf}</div><div class="ac-v">{"".join(celdas)}</div></div></div></div>'
        )
    ui.html(
        '<div class="ac-w"><div style="display:flex;flex-direction:column;gap:6px;flex:1;min-height:0">'
        + "".join(filas) + '</div></div>'
    ).style("flex:1;min-height:0;display:flex;flex-direction:column")


# Termometro de reputacion de ML: (level_id, nombre, color palido, color pleno), de izquierda (peor) a derecha (mejor).
_REP_NIVELES = [
    ("1_red", "Rojo", "#FEE2E2", "#DC2626"),
    ("2_orange", "Naranja", "#FFEDD5", "#EA580C"),
    ("3_yellow", "Amarillo", "#FEF9C3", "#EAB308"),
    ("4_light_green", "Verde claro", "#DCFCE7", "#4ADE80"),
    ("5_green", "Verde", "#DCFCE7", "#16A34A"),
]


def _fmt_minutos(m: float) -> str:
    """Duracion en minutos como '38 min' / '5 h 50 min' / '1 d 3 h'."""
    m = int(round(m or 0))
    if m < 60:
        return f"{m} min"
    if m < 1440:
        h, rem = divmod(m, 60)
        return f"{h} h {rem} min" if rem else f"{h} h"
    d, rem_min = divmod(m, 1440)
    h = rem_min // 60
    return f"{d} d {h} h" if h else f"{d} d"


def _fmt_hm(m: float) -> str:
    """Version corta para la linea por franja: '7 h 35' / '2 h' / '45 min' / '2 d 8 h'."""
    m = int(round(m or 0))
    if m < 60:
        return f"{m} min"
    if m < 1440:
        h, rem = divmod(m, 60)
        return f"{h} h {rem}" if rem else f"{h} h"
    d, rem_min = divmod(m, 1440)
    h = rem_min // 60
    return f"{d} d {h} h" if h else f"{d} d"


_RT_CLAVE = "ml_response_time_cache"
_RT_TTL_SEG = 30 * 60


def _tiempo_respuesta_ml(user_id: int, access_token: str, seller_id: str) -> Optional[Dict[str, Any]]:
    """Tiempo de respuesta OFICIAL de ML (GET /users/{seller_id}/questions/response_time, ventana de 14 dias que ML
    actualiza una vez por dia), en minutos: {"total", "laboral", "finde", "noche", "preguntas"} o
    {"sin_preguntas": True} si ML responde 404. Cache por usuario de 30 min en cotizador_datos (sobrevive a reinicios):
    con el cache vigente no llama a ML; si la llamada falla devuelve el ultimo valor guardado aunque sea viejo; sin
    ninguno, None. Pensada para correr en un hilo (run.io_bound), nunca en el render."""
    previo: Optional[Dict[str, Any]] = None
    try:
        raw = get_cotizador_param(_RT_CLAVE, user_id)
        previo = json.loads(raw) if raw else None
    except Exception:
        previo = None
    if previo and time.time() - float(previo.get("ts") or 0) < _RT_TTL_SEG:
        return previo.get("datos")
    res = ml_get_response_time(access_token, seller_id)
    datos: Optional[Dict[str, Any]] = None
    if res.get("status") == "ok":
        d = res.get("data") or {}
        tot = (d.get("total") or {}).get("response_time")
        if tot is not None:
            datos = {
                "total": float(tot),
                "laboral": (d.get("weekdays_working_hours") or {}).get("response_time"),
                "finde": (d.get("weekend") or {}).get("response_time"),
                "noche": (d.get("weekdays_extra_hours") or {}).get("response_time"),
                "preguntas": d.get("total_questions"),
            }
    elif res.get("status") == "not_found":
        datos = {"sin_preguntas": True}
    if datos is None:  # fallo real de ML: se muestra lo ultimo que se guardo
        return (previo or {}).get("datos")
    try:
        set_cotizador_param(_RT_CLAVE, json.dumps({"ts": time.time(), "datos": datos}), user_id)
    except Exception:
        logging.exception("[ESTADISTICAS] no se pudo guardar el cache de response_time (user_id=%s)", user_id)
    return datos


def _pintar_reputacion(
    level_id: Any, metricas: List[Tuple[str, Optional[float], float]], n_sin_responder: Optional[int],
    tiempo: Optional[Dict[str, Any]], lbl: str,
) -> None:
    """Contenido de la tarjeta REPUTACION (R1): nivel arriba a la derecha, termometro de 5 segmentos, una fila por metrica con
    barra del uso del limite de ML (valor / limite, tope 100%), preguntas sin responder y tiempo de respuesta oficial de ML.
    metricas = (nombre, tasa 0-1 o None, limite 0-1). Los valores van en dos columnas de ancho fijo a la derecha (valor
    alineado a la derecha, "(X% max)" a la izquierda); preguntas y tiempo comparten el borde derecho de la columna del valor."""
    actual = next((n for n in _REP_NIVELES if n[0] == str(level_id)), None)
    col_nivel = actual[3] if actual else "#6B7280"
    ui.add_css(
        ".rp-f{display:flex;align-items:center;gap:6px;margin-bottom:4px;line-height:14px;font-variant-numeric:tabular-nums}"
        ".rp-n{flex:0 0 84px;font-size:11.5px;color:#374151;white-space:nowrap}"
        ".rp-b{flex:1;min-width:24px;height:6px;border-radius:3px;background:#F3F4F6;overflow:hidden}"
        ".rp-b>div{height:100%;border-radius:3px}"
        ".rp-v1{flex:0 0 auto;min-width:40px;font-size:12px;font-weight:700;text-align:right;white-space:nowrap;"
        "font-variant-numeric:tabular-nums}"
        ".rp-v2{flex:0 0 54px;font-size:10px;font-weight:400;color:#9CA3AF;text-align:left;white-space:nowrap;"
        "font-variant-numeric:tabular-nums}"
        ".rp-s{font-size:9.5px;line-height:11px;color:#9CA3AF}"
    )
    with ui.element("div").style("display:flex;justify-content:space-between;align-items:baseline;margin-bottom:6px"):
        ui.label("REPUTACIÓN").style(lbl)
        ui.label(f"● {actual[1] if actual else 'Sin nivel'}").style(f"font-size:11px;font-weight:700;color:{col_nivel}")
    segs = "".join(
        '<div style="flex:1;display:flex;flex-direction:column;align-items:center">'
        f'<div style="width:100%;height:10px;border-radius:2px;background:{pleno if actual and lid == actual[0] else palido}"></div>'
        + (f'<div style="width:0;height:0;margin-top:1px;border-left:4px solid transparent;border-right:4px solid transparent;'
           f'border-bottom:5px solid {pleno}"></div>' if actual and lid == actual[0] else '<div style="height:6px"></div>')
        + '</div>'
        for lid, _nom, palido, pleno in _REP_NIVELES
    )
    filas = []
    for nombre, tasa, lim in metricas:
        lim_txt = f"{lim * 100:g}".replace(".", ",") + "%"
        if tasa is None:
            filas.append(f'<div class="rp-f"><span class="rp-n">{nombre}</span><div class="rp-b"></div>'
                         f'<span class="rp-v1" style="color:#9CA3AF">—</span><span class="rp-v2">({lim_txt} máx)</span></div>')
            continue
        uso = tasa / lim if lim > 0 else 1.0
        col = "#16A34A" if uso < 0.5 else ("#D97706" if uso < 0.8 else "#DC2626")
        val = f"{tasa * 100:.2f}%".replace(".", ",")
        filas.append(
            f'<div class="rp-f" title="Usás el {min(uso * 100, 999):.0f}% del límite de ML ({lim_txt})"><span class="rp-n">{nombre}</span>'
            f'<div class="rp-b"><div style="width:{min(uso * 100, 100):.1f}%;background:{col}"></div></div>'
            f'<span class="rp-v1" style="color:{col}">{val}</span><span class="rp-v2">({lim_txt} máx)</span></div>'
        )
    extra = '<div style="height:1px;background:#F3F4F6;margin:5px 0"></div>'
    if n_sin_responder is not None:
        cq = "#16A34A" if n_sin_responder == 0 else ("#D97706" if n_sin_responder <= 5 else "#DC2626")
        extra += (f'<div class="rp-f"><span class="rp-n" style="flex:1">Preguntas sin responder</span>'
                  f'<span class="rp-v1" style="color:{cq}">{n_sin_responder}</span></div>')
    if tiempo and tiempo.get("total") is not None:
        tot = float(tiempo["total"])
        ct = "#16A34A" if tot <= 60 else ("#D97706" if tot <= 360 else "#DC2626")
        franjas = " · ".join(f"{n} {_fmt_hm(tiempo[k])}" for k, n in (("laboral", "Laboral"), ("finde", "Finde"), ("noche", "Noche"))
                             if tiempo.get(k) is not None)
        extra += ('<div style="margin-bottom:3px"><div class="rp-f" style="margin-bottom:0">'
                  '<span class="rp-n" style="flex:1">Tiempo de respuesta</span>'
                  f'<span class="rp-v1" style="color:{ct}">{_fmt_minutos(tot)}</span></div>'
                  '<div class="rp-s">MercadoLibre · últimos 14 días</div>'
                  + (f'<div class="rp-s">{franjas}</div>' if franjas else '') + '</div>')
    else:
        sub = "sin preguntas en el período" if (tiempo or {}).get("sin_preguntas") else "sin datos"
        extra += ('<div style="margin-bottom:3px"><div class="rp-f" style="margin-bottom:0">'
                  '<span class="rp-n" style="flex:1">Tiempo de respuesta</span>'
                  '<span class="rp-v1" style="color:#9CA3AF">—</span></div>'
                  f'<div class="rp-s">MercadoLibre · últimos 14 días · {sub}</div></div>')
    ui.html(
        f'<div style="display:flex;gap:3px;margin-bottom:6px">{segs}</div>' + "".join(filas) + extra
        + '<div style="font-size:9.5px;line-height:11px;color:#9CA3AF;margin-top:5px">'
          'Barra = cuánto del límite de ML estás usando</div>'
    )


_MESES_ES = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"]


def _m1(v: float) -> str:
    """$333,0M (siempre un decimal, coma) para millones; $850k para miles; $0 debajo."""
    if v >= 1_000_000:
        return f"${v / 1_000_000:.1f}M".replace(".", ",")
    if v >= 1_000:
        return f"${v / 1_000:.0f}k"
    return f"${int(v)}"


def _ads_gasto_mes(user_id: Optional[int], hoy: Any) -> Optional[Dict[str, float]]:
    """Gasto de Ads (suma de cost de ml_ads_campaign_metrics_daily) del mes en curso y del mes anterior completo.
    Las metricas de Ads cierran el dia anterior (10:30), asi que la proyeccion usa los DIAS CON DATO del mes, no los
    transcurridos: {"gasto", "dias", "prev"}. None si el usuario no tiene Ads (ni este mes ni el anterior)."""
    if not user_id:
        return None
    prev_fin = hoy.replace(day=1) - timedelta(days=1)
    try:
        cur = get_ads_campaign_daily_range(int(user_id), hoy.replace(day=1).strftime("%Y-%m-%d"), hoy.strftime("%Y-%m-%d"))
        prev = get_ads_campaign_daily_range(int(user_id), prev_fin.replace(day=1).strftime("%Y-%m-%d"), prev_fin.strftime("%Y-%m-%d"))
    except Exception:
        logging.getLogger(__name__).exception("[ADS] no se pudo leer ml_ads_campaign_metrics_daily para Ventas del mes")
        return None
    g_cur = sum(float(x.get("cost") or 0) for x in cur)
    g_prev = sum(float(x.get("cost") or 0) for x in prev)
    if not (g_cur or g_prev):
        return None
    return {"gasto": g_cur, "dias": float(len({x.get("date") for x in cur if x.get("date")})), "prev": g_prev}


def _pills_moneda() -> Tuple[Any, Any, Callable[[str], None]]:
    """Selector "$ ARS | US$" (el de FACTURACION MENSUAL, compartido): crea las dos pastillas en el contenedor actual y
    devuelve (pill_ars, pill_usd, activar(moneda)) para pintar la activa. El estado lo lleva quien lo usa."""
    with ui.row().classes("gap-0 no-wrap").style("background:#f3f4f6;border-radius:999px;padding:2px;margin-left:auto"):
        pill_ars = ui.label("$ ARS")
        pill_usd = ui.label("US$")

    def activar(moneda: str) -> None:
        _base = "font-size:10px;font-weight:600;padding:2px 10px;border-radius:999px;cursor:pointer;"
        _on, _off = f"{_base}background:{_AZUL_BARRA};color:#fff", f"{_base}background:transparent;color:#6b7280"
        pill_ars.style(replace=_on if moneda == "ARS" else _off)
        pill_usd.style(replace=_on if moneda == "USD" else _off)

    return pill_ars, pill_usd, activar


def _mu(v: float) -> str:
    """Dolares compactos para labels y frase: u$ 1,23M / u$ 219k / u$ 31,5k / u$ 123."""
    if v >= 1_000_000:
        return f"u$ {v / 1_000_000:.2f}M".replace(".", ",")
    if v >= 100_000:
        return f"u$ {v / 1_000:.0f}k"
    if v >= 1_000:
        return f"u$ {v / 1_000:.1f}k".replace(".", ",")
    return f"u$ {int(v)}"


def _ventas_mes_partes(por_mes: Dict[str, Any], margen_mes: Dict[str, Any], hoy: Any, dolar: float,
                       ads: Optional[Dict[str, float]], moneda: str = "ARS") -> Tuple[str, str]:
    """Tarjeta VENTAS - <MES> (V1 + tabla Y3): devuelve (html de arriba, html de la tabla con su pie). Arriba: lo facturado
    vs el estimado de fin de mes, barra de proyeccion con las marcas del mes anterior y del mes record (sin contar el mes
    en curso) y la frase. Abajo: tabla hasta hoy / fin de mes / vs mes anterior, una linea por fila. `moneda` "USD" pasa
    TODOS los montos a dolares (dolar = cotizador_datos.dolar_oficial); los % y las cantidades no cambian. `ads` es el
    resultado de _ads_gasto_mes (se lee una sola vez, el selector no vuelve a pedir datos).
    El estimado es el de siempre: (facturado del mes / dias transcurridos, hoy incluido) x dias del mes. Todo sale de
    por_mes (la misma fuente que Ventas historicas) y de _margen_por_mes (ganancia)."""
    cur = hoy.strftime("%Y-%m")
    dias_t = hoy.day
    dias_m = calendar.monthrange(hoy.year, hoy.month)[1]
    c = por_mes.get(cur) or {}
    monto, unid = float(c.get("total") or 0.0), int(c.get("units") or 0)
    est = monto / dias_t * dias_m
    est_u = int(unid / dias_t * dias_m)
    prev_dt = hoy.replace(day=1) - timedelta(days=1)
    prev_key = prev_dt.strftime("%Y-%m")
    pv = por_mes.get(prev_key) or {}
    prev_tot, prev_u = float(pv.get("total") or 0.0), int(pv.get("units") or 0)
    hay_prev = prev_tot > 0
    otros = {k: float(v.get("total") or 0.0) for k, v in por_mes.items() if k != cur and float(v.get("total") or 0.0) > 0}
    rec_key = max(otros, key=lambda k: otros[k]) if otros else None
    rec = otros[rec_key] if rec_key else 0.0
    nom_prev = _MESES_ES[prev_dt.month - 1]
    usd = moneda == "USD"

    def _mc(v: float) -> str:
        """Monto compacto (labels de la barra y frase)."""
        return _mu(v / dolar) if usd else _m1(v)

    def _mt(v: float) -> str:
        """Monto de la tabla: abreviado en pesos, entero en dolares."""
        return f"u$ {fmt_n(v / dolar)}" if usd else _m1(v)

    def _corto(key: str) -> str:
        t = _MESES_ES[int(key[5:7]) - 1][:3]
        return t if key[:4] == str(hoy.year) or key == prev_key else f"{t} '{key[2:4]}"

    def _vs(a: Optional[float], b: Optional[float]) -> Optional[float]:
        return None if a is None or b is None or b <= 0 else (a / b - 1) * 100

    def _vs_html(v: Optional[float]) -> str:
        if v is None:
            return '<span style="color:#9CA3AF">—</span>'
        col, flecha = ("#16A34A", "▲") if v >= 0 else ("#DC2626", "▼")
        return f'<span style="color:{col};font-weight:600">{flecha} {f"{v:+.0f}%".replace("-", "−")}</span>'

    gm = margen_mes.get(cur) or {}
    gp = margen_mes.get(prev_key) or {}
    gan_hoy = float(gm["gan"]) if gm.get("pct") is not None else None
    gan_est = est * float(gm["pct"]) / 100 if gm.get("pct") is not None else None
    gan_prev = float(gp["gan"]) if (hay_prev and gp.get("pct") is not None) else None
    prom_d = monto / dias_t
    prom_prev = prev_tot / prev_dt.day if hay_prev else None
    tick = monto / unid if unid > 0 else None
    tick_prev = prev_tot / prev_u if (hay_prev and prev_u > 0) else None

    # Frase de proyeccion
    if dias_t < 2:
        frase = '<span style="color:#6B7280">Estimación disponible desde el día 2</span>'
    elif not hay_prev:
        frase = '<span style="color:#9CA3AF">—</span>'
    else:
        v = (est / prev_tot - 1) * 100
        if v < 0:
            frase = f'<span style="color:#DC2626">▼ {f"{v:+.0f}%".replace("-", "−")} vs {nom_prev}</span>'
        else:
            ref = max(rec, prev_tot)
            extra = (f" · superarías el récord por {_mc(est - ref)}" if est > ref
                     else f" · te faltan {_mc(ref - est)} para el récord")
            frase = f'<span style="color:#16A34A">▲ {v:+.0f}% vs {nom_prev}{extra}</span>'

    # Barra: 0 .. max(estimado, record) x 1,08
    escala = (max(est, rec) * 1.08) or 1.0

    def _pc(v: float) -> float:
        return max(0.0, min(v / escala * 100, 100.0))

    marcas: List[Tuple[float, str, str]] = []  # (pct, color, label)
    if hay_prev and rec_key and rec > prev_tot:
        marcas.append((_pc(prev_tot), "#6B7280", f"{_corto(prev_key)} {_mc(prev_tot)}"))
        marcas.append((_pc(rec), "#D97706", f"récord {_corto(rec_key)} {_mc(rec)}"))
    elif hay_prev:
        marcas.append((_pc(prev_tot), "#D97706", f"{_corto(prev_key)} (récord) {_mc(prev_tot)}"))
    elif rec_key:
        marcas.append((_pc(rec), "#D97706", f"récord {_corto(rec_key)} {_mc(rec)}"))
    rayas = "".join(
        f'<div style="position:absolute;left:{pc:.2f}%;top:-4px;height:24px;border-left:1.5px dashed {col}"></div>'
        for pc, col, _t in marcas)
    # Labels debajo (la marca del mes anterior, la del record y "hoy"): si dos se pisarian van en otra fila
    items = sorted([(pc, col, t, False) for pc, col, t in marcas] + [(_pc(monto), "#2563EB", "hoy", True)], key=lambda x: x[0])
    ancho_ref = 300.0  # ancho aproximado de la barra en px, solo para decidir las filas
    filas_fin: List[float] = []  # borde derecho (px) del ultimo label de cada fila
    labs = []
    for pc, col, t, neg in items:
        w = len(t) * 4.7 + 6
        izq = max(0.0, min(pc / 100 * ancho_ref - w / 2, ancho_ref - w))
        fila = next((i for i, fin in enumerate(filas_fin) if izq >= fin + 4), None)
        if fila is None:
            filas_fin.append(0.0)
            fila = len(filas_fin) - 1
        filas_fin[fila] = izq + w
        labs.append(
            f'<div style="position:absolute;top:{fila * 10}px;width:{w:.0f}px;text-align:center;white-space:nowrap;'
            f'left:clamp(0px,calc({pc:.2f}% - {w / 2:.0f}px),calc(100% - {w:.0f}px));font-size:8.5px;line-height:10px;'
            f'color:{col};font-weight:{700 if neg else 500}">{t}</div>')
    alto_labs = 10 * max(1, len(filas_fin))

    dash = '<span style="color:#9CA3AF;font-weight:400">—</span>'
    ord_m, ord_prev = int(c.get("orders") or 0), int(pv.get("orders") or 0)
    est_o = int(ord_m / dias_t * dias_m)
    pct_m, pct_p = gm.get("pct"), gp.get("pct")

    def _vs_pp(a: Optional[float], b: Optional[float]) -> str:
        if a is None or b is None or not hay_prev:
            return '<span style="color:#9CA3AF">—</span>'
        d = a - b
        col, flecha = ("#16A34A", "▲") if d >= 0 else ("#DC2626", "▼")
        return f'<span style="color:{col};font-weight:600">{flecha} {f"{d:+.1f}".replace(".", ",").replace("-", "−")} pp</span>'

    def _vs_celda(v: Optional[float], neutro: bool = False) -> str:
        if neutro and v is not None:
            return f'<span style="color:#6B7280;font-weight:600">{"▲" if v >= 0 else "▼"} {f"{v:+.0f}%".replace("-", "−")}</span>'
        return _vs_html(v)

    pct_txt = f"{_fmt_dec(pct_m, 1)}%" if pct_m is not None else dash
    prom_txt = (f"u$ {fmt_n(prom_d / dolar)}" if usd else
                (f"${prom_d / 1_000_000:.2f}M".replace(".", ",") if prom_d >= 1_000_000 else fmt_m(prom_d)))
    tick_txt = dash if tick is None else (f"u$ {fmt_n(tick / dolar)}" if usd else fmt_m(tick))
    filas_t: List[Tuple[str, str, str, str]] = [  # (nombre, hoy, fin de mes, vs mes anterior)
        ("Facturado", _mt(monto), _mt(est), _vs_celda(_vs(est, prev_tot if hay_prev else None))),
        ("Ganancia", _mt(gan_hoy) if gan_hoy is not None else dash, _mt(gan_est) if gan_est is not None else dash,
         _vs_celda(_vs(gan_est, gan_prev))),
        ("Margen", pct_txt, pct_txt, _vs_pp(pct_m, pct_p)),
        ("Órdenes", fmt_n(ord_m), fmt_n(est_o), _vs_celda(_vs(est_o, ord_prev if hay_prev else None))),
        ("Unidades", fmt_n(unid), fmt_n(est_u), _vs_celda(_vs(est_u, prev_u if hay_prev else None))),
        ("Ticket prom.", tick_txt, dash, _vs_celda(_vs(tick, tick_prev))),
        ("Prom. diario", prom_txt, dash, _vs_celda(_vs(prom_d, prom_prev))),
    ]
    if ads:
        proy = ads["gasto"] / ads["dias"] * dias_m if ads["dias"] > 0 else None
        filas_t.append(("Inversión ads", _mt(ads["gasto"]) if ads["gasto"] else dash, _mt(proy) if proy else dash,
                        _vs_celda(_vs(proy, ads["prev"]), neutro=True)))
    tabla = (
        '<div class="vm-g"><div class="vm-r" style="padding-top:0;padding-bottom:0"><div></div>'
        '<div class="vm-c">HASTA HOY</div><div class="vm-c">FIN DE MES</div>'
        f'<div class="vm-c">VS {nom_prev.upper() if hay_prev else "ANTERIOR"}</div></div>'
        + "".join(
            f'<div class="vm-r{"" if i % 2 else " vm-z"}"><div class="vm-t">{nom}</div>'
            f'<div class="vm-h">{h}</div><div class="vm-f">{fin}</div><div class="vm-h">{v}</div></div>'
            for i, (nom, h, fin, v) in enumerate(filas_t))
        + '</div>'
    )
    pie = ("Dólar " + f"${fmt_n(dolar)} (cotizador) · " if usd else "") + "Estimado = prom. diario × días del mes"
    arriba = (
        '<div class="vm-w">'
        '<div class="vm-n"><div><div class="vm-l">Llevás</div>'
        f'<div class="vm-v" style="color:#2563EB">{f"u$ {fmt_n(monto / dolar)}" if usd else fmt_m(monto)}</div></div>'
        '<div style="text-align:right"><div class="vm-l">Estimado fin de mes</div>'
        f'<div class="vm-v" style="color:#16A34A">{_mt(est)}</div></div></div>'
        '<div style="position:relative;height:16px;margin:4px 0 2px">'
        '<div style="position:absolute;inset:0;border-radius:8px;background:#F3F4F6;overflow:hidden">'
        f'<div style="position:absolute;left:0;top:0;bottom:0;width:{_pc(est):.2f}%;background:#DCFCE7"></div>'
        f'<div style="position:absolute;left:0;top:0;bottom:0;width:{_pc(monto):.2f}%;background:#2563EB;border-radius:8px"></div>'
        f'</div>{rayas}</div>'
        f'<div style="position:relative;height:{alto_labs}px;margin-bottom:4px">{"".join(labs)}</div>'
        f'<div style="font-size:10px;line-height:13px;font-weight:700">{frase}</div></div>'
    )
    abajo = (tabla + '<div style="font-size:8.5px;line-height:10px;color:#9CA3AF;margin-top:4px">' + pie + '</div>')
    return arriba, abajo


def _css_ventas_mes() -> None:
    ui.add_css(
        ".vm-w{container-type:inline-size}"
        ".vm-n{display:flex;justify-content:space-between;gap:8px;margin-bottom:6px}"
        ".vm-n>div{min-width:0}"
        ".vm-l{font-size:9px;line-height:11px;color:#6B7280}"
        ".vm-v{font-size:22px;line-height:26px;font-weight:700;white-space:nowrap}"
        ".vm-g{display:flex;flex-direction:column}"
        ".vm-r{display:grid;grid-template-columns:62px 1fr 1.1fr 1fr;column-gap:6px;align-items:center;padding:1px 4px}"
        ".vm-z{background:#F9FAFB}"
        ".vm-c{font-size:8.5px;line-height:10px;color:#9CA3AF;text-align:right;white-space:nowrap}"
        ".vm-t{font-size:10px;color:#6B7280;white-space:nowrap}"
        ".vm-h{font-size:10.5px;line-height:13px;color:#374151;text-align:right;white-space:nowrap}"
        ".vm-f{font-size:10.5px;line-height:13px;color:#16A34A;font-weight:700;text-align:right;white-space:nowrap}"
        "@container (max-width:290px){.vm-n{flex-direction:column;gap:6px}.vm-n>div:last-child{text-align:left!important}}"
    )


def _titulo_seccion(texto: str, color: str, margin_top: Any = 0) -> None:
    """Título de sección con un punto de color antes (estética B). margin_top="auto": absorbe el alto sobrante
    de una columna flex (min. 6px) para repartir las secciones."""
    _mt = "margin-top:auto;padding-top:6px" if margin_top == "auto" else f"margin-top:{margin_top}px"
    with ui.element("div").style(f"display:flex;align-items:center;gap:6px;{_mt};margin-bottom:3px"):
        ui.element("div").style(f"width:8px;height:8px;border-radius:50%;background:{color};flex-shrink:0")
        ui.label(texto).style("font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:.05em;font-weight:500")


def _kpi_b(cuadros: List[Tuple[str, str, str, Optional[float]]], color: str, a_contenido: bool = False) -> None:
    """Fila de cuadros estética B: etiqueta, número gris oscuro, subtexto y (opcional) barra de proporción.
    Todos del mismo alto (la barra reserva su lugar aunque no se muestre). cuadros = (etiqueta, valor, subtexto, % o None).
    a_contenido: el ancho de cada cuadro sigue a su texto (subtexto en 10px) en vez de ser parejo; para filas con textos largos."""
    with ui.row().classes("w-full flex-nowrap").style("gap:4px" if a_contenido else "gap:5px"):
        for _lx, _val, _sub, _pct in cuadros:
            _pl = "5px" if a_contenido else "6px"
            _w = max(0.0, min(100.0, _pct)) if _pct is not None else 0.0
            _barra = (
                f'<div style="height:4px;background:#E5E7EB;border-radius:2px;margin-top:3px">'
                f'<div style="height:4px;width:{_w:.1f}%;background:{color};border-radius:2px"></div></div>'
                if _pct is not None else '<div style="height:4px;margin-top:3px"></div>'
            )
            ui.html(
                f'<div style="background:#f9fafb;border:1px solid #e5e7eb;border-left:3px solid {color};border-radius:6px;'
                f'padding:4px 1px 4px {_pl};width:100%;box-sizing:border-box;overflow:hidden">'
                f'<div style="font-size:11px;color:#6b7280;white-space:nowrap">{_lx}</div>'
                f'<div style="font-size:13px;font-weight:500;color:#374151;line-height:1.2;white-space:nowrap">{_val}</div>'
                f'<div style="font-size:{10 if a_contenido else 11}px;color:#9ca3af;white-space:nowrap">{_sub}</div>'
                f'{_barra}'
                f'</div>'
            ).style("flex:1 1 auto;min-width:0" if a_contenido else "flex:1;min-width:0")


def _fmt_dec(val: float, dec: int) -> str:
    """Número con coma decimal (es-AR), sin separador de miles."""
    return f"{val:.{dec}f}".replace(".", ",")


def _fmt_corto_ads(val: float) -> str:
    """$1,4M / $329k / $850."""
    if val >= 1_000_000:
        return f"${_fmt_dec(val / 1_000_000, 1)}M"
    if val >= 1_000:
        return f"${val / 1_000:.0f}k"
    return f"${val:.0f}"


def _ads_mes_resumen(user_id: Optional[int], desde, hasta) -> Optional[Dict[str, float]]:
    """Suma de las métricas diarias de Ads (todas las campañas del usuario) en [desde, hasta], leídas de
    ml_ads_campaign_metrics_daily (la llena ads_snapshot.py). None si no hay datos o no hay actividad:
    la sección no se muestra."""
    if not user_id:
        return None
    try:
        rows = get_ads_campaign_daily_range(
            int(user_id), desde.strftime("%Y-%m-%d"), hasta.strftime("%Y-%m-%d"))
    except Exception:
        logging.getLogger(__name__).exception("[ADS] no se pudo leer ml_ads_campaign_metrics_daily para Estadísticas")
        return None
    if not rows:
        return None
    r = {k: sum(float(x.get(k) or 0) for x in rows) for k in (
        "cost", "direct_amount", "indirect_amount", "direct_units_quantity", "indirect_units_quantity")}
    r["unidades"] = r["direct_units_quantity"] + r["indirect_units_quantity"]
    r["importe"] = r["direct_amount"] + r["indirect_amount"]
    if not (r["cost"] or r["unidades"] or r["importe"]):
        return None
    return r


def fmt_n(val) -> str:
    try:
        return f"{int(round(float(val))):,}".replace(",", ".")
    except Exception:
        return "0"


def fmt_corto(val) -> str:
    """$296,9M / $450K / $850 -- formato corto para las barras del gráfico."""
    try:
        v = float(val)
    except Exception:
        return "$0"
    if abs(v) >= 1_000_000:
        return f"${v / 1_000_000:,.1f}M".replace(",", "X").replace(".", ",").replace("X", ".")
    if abs(v) >= 1_000:
        return f"${v / 1_000:,.0f}K".replace(",", ".")
    return f"${v:,.0f}".replace(",", ".")


def fmt_usd_corto(val) -> str:
    """US$ 197,5k / US$ 1,2M / US$ 850 -- formato corto en dólares (k miles, M millones)."""
    try:
        v = float(val)
    except Exception:
        return "US$ 0"
    if abs(v) >= 1_000_000:
        return f"US$ {v / 1_000_000:.1f}M".replace(".", ",")
    if abs(v) >= 1_000:
        return f"US$ {v / 1_000:.1f}k".replace(".", ",")
    return f"US$ {v:.0f}"


def fmt_usd(val) -> str:
    try:
        return f"US$ {int(round(float(val))):,}".replace(",", ".")
    except Exception:
        return "US$ 0"


_AZUL_BARRA = "#378ADD"
_MESES_ABR = {"01": "ene", "02": "feb", "03": "mar", "04": "abr", "05": "may", "06": "jun",
              "07": "jul", "08": "ago", "09": "sep", "10": "oct", "11": "nov", "12": "dic"}
_MESES_NOMBRE = {"01": "enero", "02": "febrero", "03": "marzo", "04": "abril", "05": "mayo", "06": "junio",
                 "07": "julio", "08": "agosto", "09": "septiembre", "10": "octubre", "11": "noviembre", "12": "diciembre"}


def _dolar_oficial_de(user_id: Optional[int]) -> float:
    """Misma cotización que la columna u$ USD de Ventas históricas: UNA sola, la actual del
    cotizador (cotizador_datos.dolar_oficial del usuario, 1475 si no hay), para todos los meses."""
    dolar_str = (get_cotizador_param("dolar_oficial", user_id) or "1475") if user_id else "1475"
    try:
        dolar = float(str(dolar_str).replace(",", ".").strip()) if dolar_str else 1475.0
    except ValueError:
        dolar = 1475.0
    return dolar if dolar > 0 else 1475.0


def _pastilla_pct(valores: List[float], i: int) -> str:
    """Pastilla rich-text de la variación % del índice i vs el anterior: ▲ verde / ▼ rojo; "—" gris
    para el primer mes o cuando el anterior es 0 (no se divide por cero)."""
    if i == 0 or valores[i - 1] <= 0:
        return "{nul|—}"
    pct = (valores[i] - valores[i - 1]) / valores[i - 1] * 100
    txt = f"{abs(pct):.1f}%" if abs(pct) < 100 else f"{abs(pct):.0f}%"
    return f"{{pos|▲ {txt}}}" if pct >= 0 else f"{{neg|▼ {txt}}}"


# Una barra más baja que esta fracción del eje Y no tiene lugar para el texto de unidades adentro:
# las unidades van arriba, como segunda línea sobre el label de $.
_FRACCION_BARRA_BAJA = 0.25


def _facturacion_mensual_options(por_mes: Dict[str, Any], today_local, ventas_mes_actual_monto: float,
                                 moneda: str = "ARS", dolar: float = 1.0,
                                 n_meses: int = 12) -> Tuple[Dict[str, Any], Optional[str]]:
    """Devuelve (opciones del echart, texto del promedio o None) de FACTURACIÓN MENSUAL: últimos 12
    meses (incluido el actual), todas las barras del mismo azul. El mes en curso es UNA sola barra
    apilada: lo facturado a hoy (lleno) + hasta el estimado (relleno suave con contorno punteado).
    Debajo de cada barra, pastilla con el % vs el mes anterior (el mes en curso compara el ESTIMADO).
    Línea punteada con el promedio de los meses cerrados. Las unidades van dentro de cada barra
    ("2.219u", blanco) o, si la barra es baja, arriba sobre el label de $; en el mes en curso, "N u est."
    dentro de la parte estimada. Las unidades NO dependen de la moneda. Mismo cálculo de facturación
    (por_mes) y de estimado de siempre; `moneda` "USD" divide la facturación por `dolar` (misma
    cotización fija de la tabla de Ventas históricas). `n_meses` (12, o 6 en pantallas angostas) solo
    recorta lo que se DIBUJA: promedio, línea punteada y % se calculan siempre sobre los 12 meses."""
    n_meses = max(1, min(12, int(n_meses)))
    usd = moneda == "USD"
    div = dolar if usd else 1.0
    corto = fmt_usd_corto if usd else fmt_corto
    completo = fmt_usd if usd else fmt_m
    keys: List[str] = []
    y, m = today_local.year, today_local.month
    for _ in range(12):
        keys.append(f"{y:04d}-{m:02d}")
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    keys.reverse()
    actual = keys[-1]

    # Estimado del mes en curso: misma fórmula de siempre (promedio diario a hoy x días del mes)
    dias_t = (today_local - today_local.replace(day=1)).days + 1
    dias_m = calendar.monthrange(today_local.year, today_local.month)[1]
    venta_est = None
    if dias_t < dias_m and ventas_mes_actual_monto > 0:
        venta_est = (ventas_mes_actual_monto / dias_t) * dias_m

    reales = [float((por_mes.get(k) or {}).get("total") or 0.0) / div for k in keys]
    unidades = [int((por_mes.get(k) or {}).get("units") or 0) for k in keys]
    ordenes = [int((por_mes.get(k) or {}).get("orders") or 0) for k in keys]
    est = venta_est / div if venta_est is not None else None
    efectivos = list(reales)  # para el % vs mes anterior: el mes en curso usa el estimado
    if est is not None:
        efectivos[-1] = est
    # Unidades estimadas del mes en curso: misma fórmula que la facturación
    unid_est = unidades[-1] / dias_t * dias_m if venta_est is not None else None
    cerrados = reales[:-1]
    promedio = sum(cerrados) / len(cerrados) if cerrados else 0.0
    promedio_txt = corto(promedio) if any(v > 0 for v in cerrados) else None
    # Eje Y: la barra más alta (contando el estimado) llega cerca del techo, con lugar para su label
    y_max = max(efectivos[-n_meses:] + [promedio, 1.0]) * 1.15
    # Poca muestra (hasta el día 7): la parte estimada baja a ~0.45 de opacidad. Se hace con alpha en
    # relleno y contorno (no con itemStyle.opacity) para que el texto siga legible.
    tenue_fm = {"color": "rgba(55,138,221,0.07)", "borderColor": "rgba(55,138,221,0.45)"} if dias_t <= 7 else None

    labels, real_data, est_data, tope_data = [], [], [], []
    for i in range(len(keys) - n_meses, len(keys)):
        k = keys[i]
        nombre = f"{_MESES_ABR.get(k[5:7], k[5:7])}-{k[2:4]}"
        labels.append(f"{nombre}\n{_pastilla_pct(efectivos, i)}")
        es_actual = k == actual
        real = round(reales[i], 0)
        ticket = reales[i] / ordenes[i] if ordenes[i] else 0.0
        titulo_mes = f"<b>{_MESES_NOMBRE.get(k[5:7], k[5:7])} {k[:4]}</b>"
        if es_actual and est is not None:
            tip = (f"{titulo_mes}<br/>Real a hoy: {completo(reales[i])}<br/>Estimado: {completo(est)}"
                   f"<br/>Unidades (a hoy): {fmt_n(unidades[i])}<br/>Ticket prom. (a hoy): {completo(ticket)}")
            real_data.append({"value": real, "label": {"show": False}, "tooltip": {"formatter": tip}})
            item_est = {
                "value": round(est - reales[i], 0),
                # unidades estimadas, en gris, adentro de la parte estimada punteada
                "label": {"show": True, "position": "inside", "color": "#6b7280", "fontSize": 10,
                          "formatter": f"{fmt_n(unid_est)}u\nest."},
                "tooltip": {"formatter": tip},
            }
            if tenue_fm:
                item_est["itemStyle"] = dict(tenue_fm)
            est_data.append(item_est)
            tope_fmt = f"{{m|{corto(est)} est.}}\n{{d|{dias_t}/{dias_m} días}}"
        else:
            tip = (f"{titulo_mes}<br/>Facturado: {completo(reales[i])}"
                   f"<br/>Unidades: {fmt_n(unidades[i])}<br/>Ticket prom.: {completo(ticket)}")
            u_txt = f"{fmt_n(unidades[i])}u"
            if reales[i] / y_max < _FRACCION_BARRA_BAJA:
                # barra baja: unidades arriba (gris oscuro) y $ debajo, sin superponerse
                real_data.append({"value": real, "label": {"show": False}, "tooltip": {"formatter": tip}})
                tope_fmt = f"{{u|{u_txt}}}\n{{m|{corto(reales[i])}}}"
            else:
                real_data.append({
                    "value": real, "tooltip": {"formatter": tip},
                    "label": {"show": True, "position": "insideBottom", "color": "#ffffff",
                              "fontSize": 10, "formatter": u_txt},
                })
                tope_fmt = f"{{m|{corto(reales[i])}}}"
            est_data.append({"value": 0, "label": {"show": False}, "tooltip": {"show": False}})
        # el label de $ (y, si corresponde, de unidades) va en una serie auxiliar invisible, así
        # una misma barra puede llevar el texto de arriba y el de adentro
        tope_data.append({"value": round(efectivos[i], 0), "label": {"show": True, "formatter": tope_fmt}})

    pastilla = {"padding": [1, 5, 1, 5], "fontSize": 9, "fontWeight": "bold", "borderRadius": 8}
    opciones = {
        "backgroundColor": "transparent",
        "grid": {"left": 5, "right": 16, "top": 30, "bottom": 40, "containLabel": False},
        "tooltip": {"trigger": "item"},
        "xAxis": {
            "type": "category", "data": labels, "axisTick": {"show": False},
            "axisLabel": {
                "fontSize": 10, "interval": 0, "lineHeight": 16,
                "rich": {
                    "pos": {**pastilla, "color": "#16a34a", "backgroundColor": "#dcfce7"},
                    "neg": {**pastilla, "color": "#dc2626", "backgroundColor": "#fee2e2"},
                    "nul": {**pastilla, "color": "#9ca3af", "backgroundColor": "#f3f4f6"},
                },
            },
        },
        "yAxis": {"show": False, "type": "value", "min": 0, "max": round(y_max, 0)},
        "series": [
            {
                "name": "real", "type": "bar", "stack": "mes", "barWidth": "78%",
                "itemStyle": {"color": _AZUL_BARRA},
                "data": real_data,
                "markLine": {
                    "silent": True, "symbol": "none", "animation": False,
                    "label": {"show": False},
                    "lineStyle": {"type": "dashed", "color": "#9ca3af", "width": 1},
                    "data": [{"yAxis": round(promedio, 0)}],
                },
            },
            {
                "name": "estimado", "type": "bar", "stack": "mes", "barWidth": "78%",
                "itemStyle": {"color": "rgba(55,138,221,0.15)", "borderColor": _AZUL_BARRA,
                              "borderType": "dashed", "borderWidth": 1.5},
                "data": est_data,
            },
            {
                "name": "tope", "type": "line", "silent": True, "symbol": "circle", "symbolSize": 0, "z": 5,
                "lineStyle": {"opacity": 0}, "tooltip": {"show": False},
                "label": {
                    "show": True, "position": "top", "fontSize": 9, "color": "#111827", "lineHeight": 12,
                    "rich": {"m": {"fontSize": 9, "color": "#111827", "align": "center"},
                             "u": {"fontSize": 9, "color": "#4b5563", "align": "center"},
                             "d": {"fontSize": 8, "color": "#d4a24c", "align": "center"}},
                },
                "data": tope_data,
            },
        ],
    }
    return opciones, promedio_txt


def _safe_str(val) -> str:
    if isinstance(val, str):
        return val.strip()
    if isinstance(val, dict):
        return (val.get("picture_url") or val.get("secure_url") or
                val.get("url") or val.get("data") or "").strip()
    return ""


def _cuotas_key(it: dict) -> tuple:
    sku = (it.get("seller_sku") or "").strip()
    if sku:
        return ("sku", sku)
    cpid = (it.get("catalog_product_id") or "").strip()
    if cpid:
        return ("catalog", cpid)
    return ("id", str(it.get("id") or ""))


def _cortar_titulo(text: str, limit: int = 60) -> str:
    """Corta a `limit` caracteres en el ultimo espacio, SIN puntos suspensivos."""
    text = (text or "").strip()
    if len(text) <= limit:
        return text
    cut = text[:limit]
    sp = cut.rfind(" ")
    return (cut[:sp] if sp > limit * 0.6 else cut).rstrip()


def _smart_truncate(text: str, limit: int = 60) -> str:
    """Trunca sin cortar una palabra a la mitad: corta en el último espacio
    dentro del límite (salvo que quede demasiado corto, ahí prioriza el límite)."""
    text = (text or "").strip()
    if len(text) <= limit:
        return text
    cut = text[:limit]
    sp = cut.rfind(" ")
    if sp > limit * 0.6:
        cut = cut[:sp]
    return cut.rstrip() + "…"


# ---------------------------------------------------------------------------
# Renderer principal (sólo llamado desde build_tab_estadisticas)
# ---------------------------------------------------------------------------

_LOGISTICA_CACHE: Dict[str, Tuple[str, str]] = {}  # shipment_id -> (logistic_type, dia); no cambia una vez asignado
_SHIP_DE_ORDEN_CACHE: Dict[str, Tuple[str, str]] = {}  # order_id -> (shipment_id, dia)
_LOGISTICA_FLEX = ("self_service",)
_LOGISTICA_CORREO = ("fulfillment", "xd_drop_off", "drop_off", "cross_docking", "me2")


def _logistica_hoy(access_token: str, seller_id: str, ordenes_hoy: List[Dict[str, Any]]) -> Dict[str, int]:
    """UNIDADES de las ventas de hoy (ordenes_hoy: ya filtradas con sales_core.es_venta + fecha de hoy)
    separadas por logistica: {"flex": u, "me": u, "otras": u, "sin_dato": ordenes}. flex = self_service;
    me (correo / Mercado Envios) = fulfillment, xd_drop_off, drop_off, cross_docking, me2; otras = retiro,
    sin envio u otro tipo (incluye las que no se pudieron consultar, contadas aparte en sin_dato).
    flex + me + otras == unidades de VENTAS HOY. ml_orders_cache no guarda `shipping`, asi que el id del
    envio sale de orders/search de hoy (ml_get_orders) y el tipo de GET /shipments/{id}."""
    from concurrent.futures import ThreadPoolExecutor
    if not ordenes_hoy:
        return {"flex": 0, "me": 0, "otras": 0, "sin_dato": 0}
    hoy_s = datetime.now(timezone(timedelta(hours=-3))).strftime("%Y-%m-%d")
    for k in [k for k, (_s, d) in _SHIP_DE_ORDEN_CACHE.items() if d != hoy_s]:
        _SHIP_DE_ORDEN_CACHE.pop(k, None)
    oid_hoy = [str(o.get("order_id") or o.get("id") or "") for o in ordenes_hoy]
    if any(i not in _SHIP_DE_ORDEN_CACHE for i in oid_hoy):  # solo se pide a ML si hay ordenes nuevas
        vivas = (ml_get_orders(access_token, seller_id, limit=500, date_from=hoy_s + "T00:00:00.000-03:00").get("results") or [])
        for o in vivas:
            sid = (o.get("shipping") or {}).get("id")
            if o.get("id"):  # "" = orden sin envio (retiro): se cachea igual para no volver a pedirla
                _SHIP_DE_ORDEN_CACHE[str(o["id"])] = (str(sid or ""), hoy_s)
    ship_de_orden: Dict[str, str] = {i: _SHIP_DE_ORDEN_CACHE[i][0] for i in oid_hoy if _SHIP_DE_ORDEN_CACHE.get(i, ("",))[0]}
    headers = {"Authorization": f"Bearer {access_token}"}

    def _tipo(ship_id: str) -> Optional[str]:
        try:
            r = get_ml_session().get(f"https://api.mercadolibre.com/shipments/{ship_id}", headers=headers, timeout=10)
            return str(r.json().get("logistic_type") or "").lower() if r.status_code == 200 else None
        except Exception:
            return None

    # Cache en memoria hasta fin del dia: solo se consultan los envios que todavia no estan.
    dia = datetime.now(timezone(timedelta(hours=-3))).strftime("%Y-%m-%d")
    for k in [k for k, (_t, d) in _LOGISTICA_CACHE.items() if d != dia]:
        _LOGISTICA_CACHE.pop(k, None)
    ids = sorted(set(ship_de_orden.values()))
    tipos: Dict[str, Optional[str]] = {i: _LOGISTICA_CACHE[i][0] for i in ids if i in _LOGISTICA_CACHE}
    nuevos = [i for i in ids if i not in tipos]
    if nuevos:
        with ThreadPoolExecutor(max_workers=8) as ex:
            for i, t in zip(nuevos, ex.map(_tipo, nuevos)):
                tipos[i] = t
                if t is not None:  # un fallo no se cachea: se reintenta en la proxima carga
                    _LOGISTICA_CACHE[i] = (t, dia)
    out = {"flex": 0, "me": 0, "otras": 0, "sin_dato": 0}
    for o in ordenes_hoy:
        u = unidades_venta(o)
        sid = ship_de_orden.get(str(o.get("order_id") or o.get("id") or ""))
        if sid is None:
            out["otras"] += u  # sin envio (retiro / sin shipping)
            continue
        t = tipos.get(sid)
        if t is None:
            out["sin_dato"] += 1
            out["otras"] += u
        elif t in _LOGISTICA_FLEX:
            out["flex"] += u
        elif t in _LOGISTICA_CORREO:
            out["me"] += u
        else:
            out["otras"] += u
    return out


def _pintar_home_inline(
    container, profile: Optional[Dict], orders_data: Dict[str, Any], user_id: Optional[int] = None, items_data: Optional[Dict[str, Any]] = None, on_refresh: Optional[Callable[[], None]] = None, shipments_today: Optional[Dict[str, int]] = None, questions: Optional[List] = None, dispatch_deadline: Optional[str] = None, pending_labels: Optional[Dict[str, int]] = None, access_token: Optional[str] = None, tiempo_ml: Optional[Dict[str, Any]] = None,
) -> None:
    """Pinta el contenido del Home con los datos ya cargados. on_refresh permite actualizar datos al vuelo."""
    raw_orders = orders_data.get("results") or orders_data.get("orders") or orders_data.get("elements") or []
    # Criterio unico de venta (sales_core): solo paid/partially_refunded; todo lo demas (hoy, periodos,
    # por_mes, top, ultimas ventas, cuotas, promos) sale de esta lista.
    results = [o for o in raw_orders if isinstance(o, dict) and es_venta(o)]
    rep = (profile or {}).get("seller_reputation") or {}
    today_local = datetime.now().date()
    primer_dia_mes = today_local.replace(day=1)
    # Unidades y facturado por dia, hoy + 96 dias atras (90 de la aceleracion + 6 para el promedio de 7 dias del primer punto) (mismo criterio sales_core): alimentan el calendario y la aceleracion.
    ventas_por_dia: Dict[str, int] = {}
    facturacion_por_dia: Dict[str, float] = {}
    for d in range(97):
        fd = today_local - timedelta(days=d)
        ventas_por_dia[fd.strftime("%Y-%m-%d")] = 0
        facturacion_por_dia[fd.strftime("%Y-%m-%d")] = 0.0
    for ord_item in results:
        dt = fecha_venta(ord_item)
        if dt is None or not (0 <= (today_local - dt).days <= 96):
            continue
        ventas_por_dia[dt.strftime("%Y-%m-%d")] += unidades_venta(ord_item)
        facturacion_por_dia[dt.strftime("%Y-%m-%d")] += monto_venta(ord_item)
    hoy_unidades, hoy_monto = 0, 0.0
    flex_hoy = 0
    me_hoy = 0
    otras_hoy = 0
    ventas_mes_actual_unid, ventas_mes_actual_monto = 0, 0.0
    por_mes: Dict[str, Any] = {}
    top_productos: Dict[str, Dict[str, Any]] = {}  # item_id -> {title, units}

    for ord_item in results:
        dt = fecha_venta(ord_item)
        if dt is None:
            continue
        total_amount = monto_venta(ord_item)
        units = unidades_venta(ord_item)
        if dt == today_local:
            hoy_unidades += units
            hoy_monto += total_amount
        if primer_dia_mes <= dt <= today_local:
            ventas_mes_actual_unid += units
            ventas_mes_actual_monto += total_amount
            items = ord_item.get("order_items") or ord_item.get("items") or []
            for it in items:
                if not isinstance(it, dict):
                    continue
                obj = it.get("item") or it
                qty = int(it.get("quantity") or it.get("qty") or 0)
                if qty <= 0:
                    continue
                titulo = (obj.get("title") if isinstance(obj, dict) else None) or it.get("title") or "Sin nombre"
                iid = (str(obj.get("id") or it.get("item_id") or "") if isinstance(obj, dict) else str(it.get("item_id") or "")).strip()
                key_id = iid or titulo[:80]
                if key_id not in top_productos:
                    top_productos[key_id] = {"title": titulo, "units": 0}
                top_productos[key_id]["units"] += qty
        key = dt.strftime("%Y-%m")
        if key not in por_mes:
            por_mes[key] = {"units": 0, "total": 0.0, "orders": 0}
        por_mes[key]["units"] += units
        por_mes[key]["total"] += total_amount
        por_mes[key]["orders"] += 1

    # Logistica de las ventas de hoy (en UNIDADES, para que flex + correo + otras == VENTAS HOY);
    # None = no se pudo consultar (se muestra "—").
    envio_ok = shipments_today is not None
    if envio_ok:
        flex_hoy = shipments_today.get("flex", 0)
        me_hoy = shipments_today.get("me", 0)
        otras_hoy = shipments_today.get("otras", 0)
    # NO CONCRETADAS: ordenes de HOY (fecha en hora Argentina) que no son venta segun sales_core.es_venta
    # (canceladas, pago rechazado, pendientes de pago, contracargo, reembolso total). `results` ya trae
    # solo ventas, por eso se recorre raw_orders.
    nc_n, nc_monto = 0, 0.0
    for _o in raw_orders:
        if isinstance(_o, dict) and not es_venta(_o) and fecha_venta(_o) == today_local:
            nc_n += 1
            nc_monto += monto_venta(_o)
    _pl_total  = (pending_labels or {}).get("total", 0)
    _pl_flex   = (pending_labels or {}).get("flex", 0)
    _pl_correo = (pending_labels or {}).get("correo", 0)

    # Incluir siempre el mes actual aunque no tenga ventas (para que el gráfico muestre marzo, etc.)
    mes_actual_key = today_local.strftime("%Y-%m")
    if mes_actual_key not in por_mes:
        por_mes[mes_actual_key] = {"units": 0, "total": 0.0, "orders": 0}
    meses_orden = sorted(por_mes.keys(), reverse=True)[:6]  # Solo 6 meses para caber en pantalla

    container.clear()
    with container:
        _CARD = "background:#fff;border:1px solid #e0e2e7;border-radius:10px;padding:14px 16px"
        _CARD_NP = "background:#fff;border:1px solid #e0e2e7;border-radius:10px"
        _LBL = "font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:.05em;font-weight:500;margin-bottom:2px"
        _BLUE = "#1d4ed8"
        _GREEN = "#16a34a"

        with ui.column().classes("w-full gap-3"):
            # ── HEADER + KPI ROW ──────────────────────────────────────────────────
            prof = profile or {}

            secure_thumb = _safe_str(prof.get("secure_thumbnail")) or _safe_str(prof.get("thumbnail"))
            logo = _safe_str(prof.get("logo"))
            img_url = logo or secure_thumb
            nickname = _safe_str(prof.get("nickname")) or _safe_str(prof.get("first_name")) or "Usuario ML"
            power = _safe_str(prof.get("power_seller_status"))
            dolar_kpi_str = (get_cotizador_param("dolar_oficial", user_id) or "1475") if user_id else "1475"
            try:
                dolar_kpi = float(str(dolar_kpi_str).replace(",", ".").strip())
                if dolar_kpi <= 0:
                    dolar_kpi = 1475.0
            except (TypeError, ValueError):
                dolar_kpi = 1475.0
            mes_usd_kpi = ventas_mes_actual_monto / dolar_kpi if dolar_kpi > 0 else 0
            ticket_prom_kpi = (ventas_mes_actual_monto / ventas_mes_actual_unid) if ventas_mes_actual_unid > 0 else 0

            meses_nombres = {1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 5: "Mayo", 6: "Junio",
                            7: "Julio", 8: "Agosto", 9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"}
            mes_actual_nom = meses_nombres.get(today_local.month, today_local.strftime("%B"))

            no_concretadas = nc_n
            nc_color = "#dc2626" if no_concretadas > 0 else "#6b7280"
            nc_sub = f"{fmt_m(nc_monto)} perdidas" if (nc_n > 0 and nc_monto > 0) else "cancel./pend."
            _envio_sub = "unid." if envio_ok else "sin datos de envío"

            margen_mes = _margen_por_mes(user_id, results, meses_orden)
            with ui.row().classes("w-full gap-2 flex-wrap items-stretch"):
                # BLOQUE 1 — Tienda
                with ui.element("div").style("flex:1.1;min-width:280px;background:#fff;border:1px solid #e0e2e7;border-radius:10px;padding:10px 14px"):
                    with ui.element("div").style("display:flex;align-items:center;justify-content:space-between;border-bottom:2px solid #1d4ed8;padding-bottom:5px;margin-bottom:8px"):
                        ui.label("TIENDA").style("font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:.04em;font-weight:500")
                        if on_refresh:
                            ui.button("↻ Actualizar", on_click=lambda: on_refresh()).props("unelevated no-caps dense").style(
                                "background:#2563EB !important;color:#fff !important;border-radius:6px;height:22px;min-height:22px;padding:0 8px;"
                                "font-size:11px;font-weight:500;margin:-4px 0")
                    with ui.element("div").style("display:flex;align-items:center;gap:10px"):
                        if img_url:
                            ui.image(img_url).style("width:40px;height:40px;object-fit:cover;border-radius:8px;flex-shrink:0;border:1px solid #e0e2e7")
                        else:
                            initials = "".join(w[0].upper() for w in nickname.split()[:2]) if nickname else "ML"
                            with ui.element("div").style(f"width:40px;height:40px;border-radius:50%;background:{_BLUE};display:flex;align-items:center;justify-content:center;flex-shrink:0"):
                                ui.label(initials).style("color:white;font-size:15px;font-weight:700;line-height:1")
                        with ui.element("div").style("flex:1;min-width:0"):
                            ui.label(nickname).style(f"font-size:14px;font-weight:700;color:{_BLUE};overflow:hidden;text-overflow:ellipsis;white-space:nowrap")
                            if power:
                                with ui.element("span").style(f"background:#eff6ff;color:{_BLUE};font-size:9px;font-weight:600;padding:2px 7px;border-radius:12px;display:inline-block;margin-top:3px"):
                                    ui.label(f"MercadoLíder {power.capitalize()}")

                # BLOQUE 2 — Operaciones de hoy
                with ui.element("div").style("flex:2;min-width:280px;background:#fff;border:1px solid #e0e2e7;border-radius:10px;padding:10px 14px"):
                    with ui.element("div").style("border-bottom:2px solid #1d4ed8;padding-bottom:5px;margin-bottom:8px"):
                        ui.label("OPERACIONES DE HOY").style("font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:.04em;font-weight:500")
                    with ui.element("div").style("display:flex;align-items:flex-start;flex-wrap:wrap"):
                        with ui.element("div").style("flex:1;padding-right:14px;border-right:0.5px solid #e5e7eb"):
                            ui.label("VENTAS HOY").style("font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:.04em")
                            ui.label(str(hoy_unidades)).style(f"font-size:22px;font-weight:600;color:{_BLUE};line-height:1.2")
                            ui.label(fmt_m(hoy_monto) + (f" · otras {fmt_n(otras_hoy)}" if envio_ok and otras_hoy else "")).style("font-size:11px;color:#6b7280")
                        with ui.element("div").style("flex:1;padding:0 14px;border-right:0.5px solid #e5e7eb"):
                            ui.label("MOTO FLEX HOY").style("font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:.04em")
                            ui.label(fmt_n(flex_hoy) if envio_ok else "—").style("font-size:22px;font-weight:600;color:#6b7280;line-height:1.2")
                            ui.label(_envio_sub).style("font-size:11px;color:#6b7280")
                        with ui.element("div").style("flex:1;padding:0 14px;border-right:0.5px solid #e5e7eb"):
                            correo_lbl = f"CORREO ({dispatch_deadline} hs)" if dispatch_deadline else "CORREO"
                            ui.label(correo_lbl).style("font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:.04em")
                            ui.label(fmt_n(me_hoy) if envio_ok else "—").style("font-size:22px;font-weight:600;color:#6b7280;line-height:1.2")
                            ui.label(_envio_sub).style("font-size:11px;color:#6b7280")
                        with ui.element("div").style("flex:1;padding-left:14px"):
                            ui.label("NO CONCRETADAS").style("font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:.04em")
                            ui.label(fmt_n(no_concretadas)).style(f"font-size:22px;font-weight:600;color:{nc_color if no_concretadas else '#6b7280'};line-height:1.2")
                            ui.label(nc_sub).style("font-size:11px;color:#6b7280")

                # BLOQUE 2b — Envíos pendientes
                with ui.element("div").style("flex:1.5;min-width:280px;background:#fff;border:1px solid #e0e2e7;border-radius:10px;padding:10px 14px"):
                    with ui.element("div").style("border-bottom:2px solid #f59e0b;padding-bottom:5px;margin-bottom:8px"):
                        ui.label("ENVÍOS PENDIENTES").style("font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:.04em;font-weight:500")
                    with ui.element("div").style("display:flex;align-items:flex-start;flex-wrap:wrap"):
                        with ui.element("div").style("flex:1;padding-right:14px;border-right:0.5px solid #e5e7eb"):
                            ui.label("TOTAL").style("font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:.04em")
                            ui.label(fmt_n(_pl_total)).style("font-size:22px;font-weight:600;color:#f59e0b;line-height:1.2")
                            ui.label("etiquetas").style("font-size:11px;color:#6b7280")
                        with ui.element("div").style("flex:1;padding:0 14px;border-right:0.5px solid #e5e7eb"):
                            ui.label("FLEX").style("font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:.04em")
                            ui.label(fmt_n(_pl_flex)).style(f"font-size:22px;font-weight:600;color:{_BLUE};line-height:1.2")
                            ui.label("órdenes").style("font-size:11px;color:#6b7280")
                        with ui.element("div").style("flex:1;padding-left:14px"):
                            ui.label("CORREO").style("font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:.04em")
                            ui.label(fmt_n(_pl_correo)).style("font-size:22px;font-weight:600;color:#6b7280;line-height:1.2")
                            ui.label("órdenes").style("font-size:11px;color:#6b7280")

                # BLOQUE 3 — Facturación mes
                with ui.element("div").style("flex:1.3;min-width:280px;background:#fff;border:1px solid #e0e2e7;border-radius:10px;padding:10px 14px"):
                    with ui.element("div").style("display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;column-gap:8px;border-bottom:2px solid #16a34a;padding-bottom:5px;margin-bottom:8px"):
                        ui.label(f"FACTURACIÓN — {mes_actual_nom.upper()}").style("font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:.04em;font-weight:500")
                        _mg_txt, _mg_color, _mg_tip = _margen_visual(margen_mes.get(mes_actual_key))
                        with ui.element("div").style("display:flex;align-items:baseline;gap:4px;margin-left:auto;line-height:13px"):
                            ui.label("Margen").style("font-size:10px;color:#6b7280;font-weight:500")
                            _mg_v = ui.label(_mg_txt).style(f"font-size:14px;font-weight:600;color:{_mg_color};line-height:13px")
                            if _mg_tip:
                                _mg_v.tooltip(_mg_tip)
                    with ui.element("div").style("display:flex;align-items:flex-start;flex-wrap:wrap"):
                        with ui.element("div").style("flex:1;padding-right:14px;border-right:0.5px solid #e5e7eb"):
                            ui.label("FACTURADO").style("font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:.04em")
                            ui.label(fmt_m(ventas_mes_actual_monto)).style(f"font-size:17px;font-weight:600;color:{_GREEN};line-height:1.2")
                            ui.label(f"u$ {fmt_n(mes_usd_kpi)}").style("font-size:11px;color:#6b7280")
                        with ui.element("div").style("flex:1;padding-left:14px"):
                            ui.label("TICKET PROM").style("font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:.04em")
                            ui.label(fmt_m(ticket_prom_kpi)).style(f"font-size:17px;font-weight:600;color:{_BLUE};line-height:1.2")
                            ui.label(f"{fmt_n(ventas_mes_actual_unid)} unidades").style("font-size:11px;color:#6b7280")

            # ── FILA 1: Reputación | Ventas períodos | Facturación | Históricas ───
            metrics = rep.get("metrics", {}) or rep.get("transactions", {}) or {}
            sales_meta = metrics.get("sales", {}) or {}
            completed = sales_meta.get("completed") or 0
            claims = metrics.get("claims", {}) or metrics.get("disputes", {}) or {}
            canc = metrics.get("cancellations", {}) or {}
            delayed = metrics.get("delayed_handling_time", {}) or {}
            mediat = metrics.get("mediations", {}) or metrics.get("disputes", {}) or {}

            def _get_rate(m: Dict[str, Any], total_completed: float = 0) -> Any:
                exc = m.get("excluded") or {}
                if isinstance(exc.get("real_rate"), (int, float)):
                    return exc["real_rate"]
                if isinstance(exc.get("real_value"), (int, float)) and total_completed > 0:
                    return exc["real_value"] / total_completed
                if isinstance(m.get("rate"), (int, float)):
                    return m["rate"]
                if isinstance(m.get("value"), (int, float)) and total_completed > 0:
                    return m["value"] / total_completed
                return None

            try:
                tot = float(completed) if completed else 0
            except (TypeError, ValueError):
                tot = 0
            rate_claims = _get_rate(claims, tot)
            rate_canc = _get_rate(canc, tot)
            rate_delayed = _get_rate(delayed, tot)
            rate_mediat = _get_rate(mediat, tot) if mediat else 0.0
            MAX_CLAIMS, MAX_MEDIAT, MAX_CANC, MAX_DELAYED = 0.01, 0.005, 0.005, 0.08

            def _to_float_rate(v: Any) -> Optional[float]:
                if v is None:
                    return None
                try:
                    x = float(v)
                    return x if 0 < x <= 1 else x / 100.0
                except (TypeError, ValueError):
                    return None

            def _pct_fmt(val: Any) -> str:
                if val is None:
                    return "—"
                try:
                    v = float(val)
                    return f"{(v * 100 if 0 <= v <= 1 else v):.2f}%"
                except (TypeError, ValueError):
                    return "—"

            with ui.row().classes("w-full gap-2 flex-wrap items-stretch overflow-hidden max-w-full"):
                # Card Reputación (R1)
                with ui.element("div").style(f"flex:1;min-width:220px;{_CARD_NP};overflow:hidden;flex-shrink:0"):
                    with ui.element("div").style("padding:12px 14px"):
                        _pintar_reputacion(
                            rep.get("level_id"),
                            [("Reclamos", _to_float_rate(rate_claims), MAX_CLAIMS),
                             ("Mediaciones", _to_float_rate(rate_mediat), MAX_MEDIAT),
                             ("Cancelaciones", _to_float_rate(rate_canc), MAX_CANC),
                             ("Demora envíos", _to_float_rate(rate_delayed), MAX_DELAYED)],
                            len(questions) if questions is not None else None,
                            tiempo_ml, _LBL,
                        )

                # Card Aceleración de ventas (reemplaza Ventas por período)
                with ui.element("div").style(f"flex:1;min-width:300px;{_CARD_NP};overflow:hidden;flex-shrink:0;display:flex;flex-direction:column"):
                    with ui.element("div").style("padding:12px 14px;flex:1;min-height:0;display:flex;flex-direction:column"):
                        with ui.element("div").style("display:flex;flex-wrap:wrap;justify-content:space-between;align-items:baseline;gap:0 8px;margin-bottom:6px"):
                            ui.label("ACELERACIÓN DE VENTAS").style(_LBL)
                            ui.html(
                                '<span style="display:inline-flex;align-items:center;gap:3px;margin-right:8px">'
                                '<svg width="14" height="6" viewBox="0 0 14 6"><line x1="0" y1="3" x2="14" y2="3" stroke="#2563EB" stroke-width="1.8"/></svg>semana</span>'
                                '<span style="display:inline-flex;align-items:center;gap:3px">'
                                '<svg width="14" height="6" viewBox="0 0 14 6"><line x1="0" y1="3" x2="14" y2="3" stroke="#6B7280" stroke-width="1.2" stroke-dasharray="4 3"/></svg>prom. 90 días</span>'
                            ).style("font-size:8.5px;color:#6B7280;white-space:nowrap")
                        _pintar_aceleracion(ventas_por_dia, facturacion_por_dia, today_local)

                # Card Facturación Mensual (echart)
                if meses_orden:
                    dolar_card = _dolar_oficial_de(user_id)
                    # Estado del gráfico: moneda y modo angosto (celular vertical, viewport <= 640 px: solo 6 meses)
                    estado_fm = {"moneda": "ARS", "angosto": False}
                    _card_fm_base = f"{_CARD_NP};overflow:hidden;min-height:185px;flex-shrink:0;display:flex;flex-direction:column"
                    _card_fm_ancha = f"flex:2;min-width:520px;{_card_fm_base}"
                    _card_fm_angosta = f"flex:1 1 100%;min-width:0;max-width:100%;{_card_fm_base}"
                    chart_options, prom_txt = _facturacion_mensual_options(por_mes, today_local, ventas_mes_actual_monto)
                    with ui.element("div").style(_card_fm_ancha) as card_fm:
                        # el toggle baja debajo del título (alineado a la derecha) si no entra en una línea
                        with ui.element("div").style("padding:10px 14px 4px;display:flex;flex-wrap:wrap;align-items:center;justify-content:space-between;gap:4px 8px"):
                            with ui.row().classes("items-baseline gap-1 no-wrap"):
                                ui.label("FACTURACIÓN MENSUAL").style(_LBL)
                                lbl_prom = ui.label("").style("font-size:11px;color:#9ca3af;font-weight:400")
                            pill_ars, pill_usd, _activar_fm = _pills_moneda()
                        chart_fm = ui.echart(chart_options).classes("w-full").style("flex:1;min-height:200px;height:auto")

                        def _redibujar_fm(_chart=chart_fm, _lbl=lbl_prom) -> None:
                            moneda = estado_fm["moneda"]
                            opciones, prom = _facturacion_mensual_options(
                                por_mes, today_local, ventas_mes_actual_monto, moneda, dolar_card,
                                6 if estado_fm["angosto"] else 12)
                            _chart.options.clear()
                            _chart.options.update(opciones)
                            _chart.update()
                            _lbl.set_text(f"(prom. {prom})" if prom else "")
                            _lbl.set_visibility(bool(prom))
                            _activar_fm(moneda)

                        def _aplicar_moneda_fm(moneda: str) -> None:
                            estado_fm["moneda"] = moneda
                            _redibujar_fm()

                        def _modo_angosto_fm(e) -> None:
                            args = e.args if isinstance(e.args, dict) else {"detail": e.args}
                            angosto = bool(args.get("detail"))
                            if angosto == estado_fm["angosto"]:
                                return
                            estado_fm["angosto"] = angosto
                            card_fm.style(replace=_card_fm_angosta if angosto else _card_fm_ancha)
                            _redibujar_fm()

                        pill_ars.on("click", lambda: _aplicar_moneda_fm("ARS"))
                        pill_usd.on("click", lambda: _aplicar_moneda_fm("USD"))
                        chart_fm.on("fm_narrow", _modo_angosto_fm, args=["detail"])
                        _redibujar_fm()
                        # Mide el viewport (media query) y avisa al servidor al cargar y al rotar el celular
                        # (vertical <-> horizontal): 6 <-> 12 meses.
                        ui.timer(0.2, lambda _id=chart_fm.id: ui.run_javascript(
                            "(function go(n){var el=document.getElementById('c" + str(_id) + "');"
                            "if(!el){if(n<60)setTimeout(function(){go(n+1);},100);return;}"
                            "if(window._fmOff)window._fmOff();"
                            "var mq=window.matchMedia('(max-width: 640px)');"
                            "function send(){el.dispatchEvent(new CustomEvent('fm_narrow',{detail:mq.matches}));}"
                            "mq.addEventListener('change',send);"
                            "window._fmOff=function(){mq.removeEventListener('change',send);};"
                            "setTimeout(send,150);})(0);"), once=True)
                else:
                    with ui.element("div").style(f"flex:1;min-width:120px;{_CARD};flex-shrink:0"):
                        ui.label("FACTURACIÓN MENSUAL").style(_LBL)
                        ui.label("Sin datos").style("font-size:12px;color:#9ca3af;margin-top:6px")

                # Card Ventas Históricas
                with ui.element("div").style(f"flex:1;min-width:240px;{_CARD_NP};overflow:hidden;flex-shrink:0"):
                    with ui.element("div").style("padding:12px 14px"):
                        ui.label("VENTAS HISTÓRICAS").style(f"{_LBL};margin-bottom:8px")
                        if not meses_orden:
                            trans = rep.get("transactions", {}) or {}
                            tot_trans = trans.get("total") or trans.get("completed") or 0
                            ui.label(f"Sin datos (perfil: {tot_trans} trans.)" if tot_trans else "No hay órdenes").style("font-size:12px;color:#9ca3af")
                        else:
                            dolar_str = get_cotizador_param("dolar_oficial", user_id) or "1475"
                            dolar_oficial = float(str(dolar_str).replace(",", ".").strip()) if dolar_str else 1475.0
                            if dolar_oficial <= 0:
                                dolar_oficial = 1475.0
                            with ui.element("table").style("width:100%;border-collapse:collapse;font-size:11px"):
                                with ui.element("thead"):
                                    with ui.element("tr").style("background:#f9fafb"):
                                        for ci, col_h in enumerate(["Mes", "Unid", "$ ARS", "u$ USD", "Margen"]):
                                            align = "left" if ci == 0 else "right"
                                            with ui.element("th").style(f"padding:4px 8px;text-align:{align};font-weight:600;font-size:10px;text-transform:uppercase;color:#6b7280;border-bottom:1px solid #e0e2e7"):
                                                ui.label(col_h)
                                with ui.element("tbody"):
                                    for ri, key in enumerate(meses_orden):
                                        v = por_mes[key]
                                        total_usd = (v["total"] / dolar_oficial) if dolar_oficial else 0.0
                                        is_mes_actual = key == mes_actual_key
                                        row_bg = "#eff6ff" if is_mes_actual else ("#ffffff" if ri % 2 == 0 else "#fafafa")
                                        row_color = _BLUE if is_mes_actual else "#374151"
                                        with ui.element("tr").style(f"background:{row_bg};border-bottom:1px solid #f3f4f6"):
                                            with ui.element("td").style("padding:4px 8px;text-align:left"):
                                                if is_mes_actual:
                                                    ui.label(key).style(f"font-size:11px;color:{_BLUE};font-weight:600")
                                                else:
                                                    ui.label(key).style("font-size:11px;color:#374151")
                                            with ui.element("td").style(f"padding:4px 8px;text-align:right;font-weight:{'700' if is_mes_actual else '400'};color:{row_color}"):
                                                ui.label(fmt_n(v["units"]))
                                            with ui.element("td").style(f"padding:4px 8px;text-align:right;font-weight:{'700' if is_mes_actual else '400'};color:{row_color}"):
                                                ui.label(fmt_m(v["total"]))
                                            with ui.element("td").style(f"padding:4px 8px;text-align:right;font-weight:{'700' if is_mes_actual else '400'};color:{row_color if is_mes_actual else '#6b7280'}"):
                                                ui.label(f"u$ {fmt_n(total_usd)}")
                                            _mg_txt, _mg_color, _mg_tip = _margen_visual(margen_mes.get(key))
                                            with ui.element("td").style(f"padding:4px 8px;text-align:right;font-weight:{'700' if is_mes_actual else '500'};color:{_mg_color}"):
                                                _mg_lbl = ui.label(_mg_txt)
                                                if _mg_tip:
                                                    _mg_lbl.tooltip(_mg_tip)

            # ── FILA 2: Top Ventas | Stock | Graf Semanal | Ventas Mes ────────────
            claims_val = (claims.get("value") or claims.get("excluded", {}).get("real_value") or 0)
            mediat_val = (mediat.get("value") or mediat.get("excluded", {}).get("real_value") or 0) if mediat else 0
            canc_val = (canc.get("value") or canc.get("excluded", {}).get("real_value") or 0)
            postventa_total = claims_val + mediat_val + canc_val

            with ui.row().classes("w-full gap-2 flex-wrap items-stretch mt-1"):
                # Card Top Ventas — agrupado por SKU real (misma fuente que el dedup de
                # "Publicaciones": _cuotas_key sobre items_data, que ya trae seller_sku /
                # catalog_product_id por publicación).
                #
                # items_data solo trae publicaciones ACTIVAS (ml_get_my_items(..., False)),
                # así que un producto con TODAS sus publicaciones pausadas (ej. sin stock)
                # no aparece ahí y no se puede mapear a SKU con esa fuente. Para esos casos
                # se hace un fallback puntual: un GET /items?ids=... (mismo endpoint que usa
                # ml_get_my_items) SOLO para los item_id vendidos que quedaron sin mapear,
                # sin importar su status. Si ese fallback también falla (item borrado, error
                # de red, etc.) la publicación queda como fila propia bajo "sin SKU" — nunca
                # se mezcla con otro producto.
                #
                # Es una repartición (partition) de top_productos: cada entrada original
                # aporta sus unidades completas a un único grupo, así que
                # suma(agrupado) == suma(top_productos) siempre, se resuelva o no el SKU.
                _id_to_group_key: Dict[str, tuple] = {}
                _id_to_is_catalog: Dict[str, bool] = {}
                for _it_sku in (items_data or {}).get("results") or []:
                    if isinstance(_it_sku, dict) and _it_sku.get("id"):
                        _iid_sku = str(_it_sku["id"])
                        _id_to_group_key[_iid_sku] = _cuotas_key(_it_sku)
                        _id_to_is_catalog[_iid_sku] = bool(_it_sku.get("catalog_listing"))

                _unmapped_ids = [
                    iid for iid in top_productos
                    if iid and " " not in iid and iid not in _id_to_group_key
                ][:60]  # tope defensivo: nunca más de 3 tandas de 20 ids en este fallback
                if _unmapped_ids and access_token:
                    try:
                        for _b in range(0, len(_unmapped_ids), 20):
                            _chunk = _unmapped_ids[_b:_b + 20]
                            _resp = get_ml_session().get(
                                "https://api.mercadolibre.com/items",
                                params={"ids": ",".join(_chunk)},
                                headers={"Authorization": f"Bearer {access_token}"},
                                timeout=10,
                            )
                            if not _resp.ok:
                                continue
                            for _entry in _resp.json():
                                if not isinstance(_entry, dict):
                                    continue
                                _body = _entry.get("body") if "body" in _entry else _entry
                                if not isinstance(_body, dict) or not _body.get("id"):
                                    continue
                                _parsed = _parse_ml_item_body(_body)
                                _iid_fb = str(_body["id"])
                                _id_to_group_key[_iid_fb] = _cuotas_key(_parsed)
                                _id_to_is_catalog[_iid_fb] = bool(_parsed.get("catalog_listing"))
                    except Exception:
                        logging.exception("[ESTADISTICAS] error en fallback de mapeo SKU para Top Ventas")

                top_groups: Dict[tuple, List[tuple]] = {}
                top_sin_sku = 0
                for _iid, _info in top_productos.items():
                    _gk = _id_to_group_key.get(_iid)
                    if _gk is None:
                        _gk = ("sin_sku", _iid)
                        top_sin_sku += 1
                    top_groups.setdefault(_gk, []).append((_iid, _info))

                # Titulo de NUESTRA publicacion propia (no catalogo) por SKU: la principal del grupo, con el mismo
                # criterio que el dedup de Publicaciones (gold_special y luego mas stock). Stock del SKU: el de
                # productos.stock (lo mantiene _stock_fresco_sync = MAXIMO entre publicaciones hermanas, no suma),
                # leido de la DB: sin llamadas en vivo a ML al renderizar.
                _titulo_propio: Dict[tuple, Tuple[Tuple[int, int], str]] = {}
                for _it_t in (items_data or {}).get("results") or []:
                    if isinstance(_it_t, dict) and _it_t.get("catalog_listing") is not True and _it_t.get("title"):
                        _rank = (1 if str(_it_t.get("listing_type_id") or "").lower() == "gold_special" else 0,
                                 int(_it_t.get("available_quantity") or 0))
                        _kt = _cuotas_key(_it_t)
                        if _kt not in _titulo_propio or _rank > _titulo_propio[_kt][0]:
                            _titulo_propio[_kt] = (_rank, str(_it_t["title"]).strip())
                _stock_sku: Dict[str, int] = {}
                if user_id is not None:
                    try:
                        _conn_st = get_connection()
                        try:
                            for _s, _st in _conn_st.execute("SELECT sku, stock FROM productos WHERE user_id=?", (user_id,)).fetchall():
                                _stock_sku[str(_s)] = int(_st or 0)
                        finally:
                            _conn_st.close()
                    except Exception:
                        logging.exception("[ESTADISTICAS] no se pudo leer productos.stock para Top Ventas")

                top_grouped: Dict[tuple, Dict[str, Any]] = {}
                for _gk, _members in top_groups.items():
                    _units_total = sum(m[1]["units"] for m in _members)
                    _propias = [m for m in _members if _id_to_is_catalog.get(m[0]) is False]
                    _pool = _propias or _members
                    _best_iid, _best_info = max(_pool, key=lambda m: m[1]["units"])
                    _es_solo_catalogo = (not _propias) and all(
                        _id_to_is_catalog.get(m[0]) is True for m in _members)
                    # 1) titulo ACTUAL de la publicacion propia activa del SKU; 2) propia no activa: titulo de la orden
                    # (el que tenia al venderse): ambos COMPLETOS (si no entran en una linea, la fila hace wrap);
                    # 3) solo catalogo sin propia: titulo de la orden/catalogo cortado a 60 en el ultimo espacio, sin "...".
                    if _gk in _titulo_propio:
                        _tit_top = _titulo_propio[_gk][1]
                    elif _propias:
                        _tit_top = _best_info["title"]
                    else:
                        _tit_top = _cortar_titulo(_best_info["title"], 60)
                    top_grouped[_gk] = {
                        "title": _tit_top, "units": _units_total,
                        "solo_catalogo": _es_solo_catalogo,
                        "stock": _stock_sku.get(_gk[1]) if _gk[0] == "sku" else None,
                    }

                top_list = sorted(top_grouped.values(), key=lambda x: x["units"], reverse=True)[:10]
                total_unid_mes = ventas_mes_actual_unid if ventas_mes_actual_unid > 0 else 1

                # Publicaciones propias (se muestran en la tarjeta Top Ventas)
                items_list = (items_data or {}).get("results") or []
                # Deduplicar por SKU — misma lógica que Productos
                _groups: Dict[tuple, list] = {}
                for _it in items_list:
                    if isinstance(_it, dict):
                        _groups.setdefault(_cuotas_key(_it), []).append(_it)
                items_list_dedup: list = []
                for _grupo in _groups.values():
                    if len(_grupo) == 1:
                        items_list_dedup.append(_grupo[0])
                    else:
                        _principal = max(
                            _grupo,
                            key=lambda x: (
                                1 if not x.get("catalog_listing") and
                                     str(x.get("listing_type_id") or "").lower() == "gold_special" else 0,
                                int(x.get("available_quantity") or 0),
                            ),
                        )
                        _fusionado = dict(_principal)
                        _fusionado["sold_quantity"] = sum(int(x.get("sold_quantity") or 0) for x in _grupo)
                        items_list_dedup.append(_fusionado)
                propias = [it for it in items_list_dedup if it.get("catalog_listing") is not True]
                publicaciones_propias_con_stock = sum(1 for it in propias if (it.get("available_quantity") or 0) > 0)
                unidades_propias_en_stock = sum(int(it.get("available_quantity") or 0) for it in propias)
                marcas_propias = [str(it.get("marca") or "").strip() for it in propias]
                marcas_distintas = len({m for m in marcas_propias if m and m != "—"})

                with ui.element("div").style(f"flex:1.3;min-width:280px;{_CARD_NP};overflow:hidden;flex-shrink:0"):
                    with ui.element("div").style("padding:12px 14px 10px"):
                        ui.label(f"TOP VENTAS — {mes_actual_nom.upper()}").style(f"{_LBL};margin-bottom:8px")
                        if not top_list:
                            ui.label("Sin ventas este mes").style("font-size:12px;color:#9ca3af")
                        else:
                            _max_u_top = max((q["units"] for q in top_list), default=1) or 1
                            for i, p in enumerate(top_list):
                                pct = (100.0 * p["units"] / total_unid_mes) if total_unid_mes else 0
                                _stk = "—" if p.get("stock") is None else f'{fmt_n(p["stock"])}u'
                                _fill = max(2.0, 100.0 * p["units"] / _max_u_top)  # puesto 1 = 100% de la barra
                                with ui.element("div").style("display:flex;align-items:center;gap:8px;margin-bottom:3px"):
                                    with ui.element("div").style(f"width:16px;height:16px;border-radius:50%;background:{_BLUE};display:flex;align-items:center;justify-content:center;flex-shrink:0"):
                                        ui.label(str(i + 1)).style("color:white;font-size:8px;font-weight:700")
                                    with ui.element("div").style("flex:1;min-width:0"):
                                        # Titulo completo (sin "..."): si no entra en una linea, hace wrap.
                                        ui.html(f'{_html.escape(p["title"] or "—")} <span style="color:#9CA3AF">({_stk})</span>').style(
                                            "font-size:10px;line-height:13px;color:#111827;overflow-wrap:anywhere")
                                        with ui.element("div").style("display:flex;align-items:center;gap:6px;margin-top:2px"):
                                            with ui.element("div").style("flex:0 1 70%;height:4px;border-radius:2px;background:#F3F4F6;overflow:hidden"):
                                                ui.element("div").style(f"height:4px;width:{_fill:.1f}%;border-radius:2px;background:#3B82F6")
                                            if p.get("solo_catalogo"):
                                                with ui.element("span").style(
                                                        "background:#f3f4f6;color:#6b7280;font-size:8px;font-weight:600;"
                                                        "padding:0 5px;line-height:9px;border-radius:8px;flex-shrink:0;white-space:nowrap"):
                                                    ui.label("CATÁLOGO")
                                    with ui.element("div").style("flex-shrink:0;text-align:right;white-space:nowrap"):
                                        ui.label(f"{p['units']}u").style("font-size:13px;line-height:14px;font-weight:700;color:#1D4ED8")
                                        ui.label(f"{pct:.1f}%".replace(".", ",")).style("font-size:9px;line-height:10px;color:#9CA3AF")
                            if top_sin_sku:
                                ui.label(f"{top_sin_sku} publicación(es) sin SKU mapeado — no se agruparon").style(
                                    "font-size:9px;color:#9ca3af;margin-top:4px")
                        # Resumen de publicaciones en UNA linea (en pantallas angostas puede hacer wrap).
                        _pub_html = " · ".join(
                            f'{_lp} <span style="color:{_BLUE};font-weight:700">{_vp}</span>'
                            for _lp, _vp in (("Marcas", str(marcas_distintas)),
                                             ("Publicaciones propias", str(publicaciones_propias_con_stock)),
                                             ("Unidades propias", fmt_n(unidades_propias_en_stock))))
                        ui.html(_pub_html).style("font-size:11px;line-height:14px;color:#6b7280;margin-top:6px")

                def _orden_fecha(o):
                    ds = o.get("date_closed") or o.get("date_created") or o.get("date_last_updated") or ""
                    return ds[:10] if ds else ""
                ultimas_5_ventas = sorted(results, key=_orden_fecha, reverse=True)[:10]

                with ui.element("div").style(f"flex:1;min-width:260px;{_CARD_NP};overflow:hidden;flex-shrink:0;display:flex;flex-direction:column"):
                    # La fila del grid (items-stretch) toma el alto de Top Ventas, la mas alta; el contenido va en
                    # flex:1 y las secciones 2 y 3 llevan margin-top:auto y se reparten el alto sobrante en vez de dejar el vacio abajo.
                    with ui.element("div").style("padding:12px 14px;flex:1;display:flex;flex-direction:column"):
                        ui.label(f"DATOS DE {mes_actual_nom.upper()}").style(f"{_LBL};margin-bottom:2px")
                        _vd_cuotas: Dict[str, str] = {}
                        try:
                            _vd_conn = get_connection()
                            _vd_rows = _vd_conn.execute(
                                "SELECT order_id, cuotas FROM ventas_datos "
                                "WHERE user_id=? AND order_date >= ? AND cuotas IS NOT NULL",
                                (user_id, primer_dia_mes.strftime("%Y-%m-%d")),
                            ).fetchall()
                            _vd_conn.close()
                            for _r in _vd_rows:
                                _oid_k = str(_r[0])
                                if _oid_k not in _vd_cuotas:
                                    _vd_cuotas[_oid_k] = str(_r[1] or "x1").strip().lower()
                        except Exception:
                            pass
                        cuotas_dist: Dict[int, int] = {1: 0, 3: 0, 6: 0, 9: 0, 12: 0}
                        total_unidades_mes_c = 0
                        _pr_u, _pr_imp, _pr_lista, _pr_aporte = 0, 0.0, 0.0, 0.0
                        for _ord in results:
                            _dt_c = fecha_venta(_ord)
                            if _dt_c is None or not (primer_dia_mes <= _dt_c <= today_local):
                                continue
                            _items_c = _ord.get("order_items") or _ord.get("items") or []
                            _uds_c = unidades_venta(_ord)
                            _ord_id_c = str(_ord.get("order_id") or _ord.get("id") or "")
                            _cuotas_c = _vd_cuotas.get(_ord_id_c) or "x1"
                            _inst_key = int(_cuotas_c.lstrip("x") or "1") if _cuotas_c.startswith("x") and _cuotas_c[1:].isdigit() else 1
                            if _inst_key not in cuotas_dist:
                                _inst_key = 1
                            cuotas_dist[_inst_key] += _uds_c
                            total_unidades_mes_c += _uds_c
                            _po = _promo_de_orden(_items_c, _ord.get("payments"))
                            _pr_u += _po[0]; _pr_imp += _po[1]; _pr_lista += _po[2]; _pr_aporte += _po[3]
                        # Sin ventas con promo en el mes la sección no se muestra (igual que Publicidad sin datos).
                        if _pr_u > 0:
                            _pr_pu = (_pr_u / total_unidades_mes_c * 100) if total_unidades_mes_c else 0.0
                            _pr_pf = (_pr_imp / ventas_mes_actual_monto * 100) if ventas_mes_actual_monto else 0.0
                            _pr_desc = ((_pr_lista - _pr_imp) / _pr_lista * 100) if _pr_lista else 0.0
                            _titulo_seccion("PROMOCIONES", _PROMO_ROSA)
                            _kpi_b([
                                ("Vendidas c/promo", fmt_n(_pr_u), f"{_pr_pu:.0f}% de {fmt_n(total_unidades_mes_c)} vendidas", _pr_pu),
                                ("Facturado c/promo", _fmt_corto_ads(_pr_imp),
                                 f"{_pr_pf:.0f}% de {_fmt_corto_ads(ventas_mes_actual_monto)}", _pr_pf),
                                ("Desc. medio", f"−{_fmt_dec(_pr_desc, 1)}%", "sobre lista", None),
                                ("Cupones", _fmt_corto_ads(_pr_aporte), "en esas ventas", None),
                            ], _PROMO_ROSA, a_contenido=True)
                        _base_c = total_unidades_mes_c or 1
                        _total_str = f"{total_unidades_mes_c:,}".replace(",", ".")
                        _titulo_seccion(f"VENTAS Y CUOTAS · {_total_str} UNID.", _CUOTAS_AZUL,
                                        margin_top="auto" if _pr_u > 0 else 0)
                        _kpi_b([
                            ("Contado" if _cx == 1 else f"{_cx} cuotas", fmt_n(cuotas_dist[_cx]),
                             f"{_fmt_dec(cuotas_dist[_cx] / _base_c * 100, 1)}%", cuotas_dist[_cx] / _base_c * 100)
                            for _cx in (1, 3, 6, 9, 12)
                        ], _CUOTAS_AZUL)

                        _ads = _ads_mes_resumen(user_id, primer_dia_mes, today_local)
                        if _ads:
                            _a_u, _a_imp, _a_inv = _ads["unidades"], _ads["importe"], _ads["cost"]
                            _a_roas = (_a_imp / _a_inv) if _a_inv else 0.0
                            _a_pu = (_a_u / total_unidades_mes_c * 100) if total_unidades_mes_c else 0.0
                            _a_pf = (_a_imp / ventas_mes_actual_monto * 100) if ventas_mes_actual_monto else 0.0
                            _titulo_seccion("PUBLICIDAD", _PUB_VIOLETA, margin_top="auto")
                            _kpi_b([
                                ("Ventas por ads", fmt_n(_a_u), f"{_fmt_dec(_a_pu, 1)}% de tus u." if total_unidades_mes_c else "—",
                                 _a_pu if total_unidades_mes_c else None),
                                ("Facturado ads", _fmt_corto_ads(_a_imp), f"{_fmt_dec(_a_pf, 1)}% del total" if ventas_mes_actual_monto else "—",
                                 _a_pf if ventas_mes_actual_monto else None),
                                ("Gasto en ads", _fmt_corto_ads(_a_inv), f"{fmt_m(_a_inv / _a_u)} x venta" if _a_u else "—", None),
                                ("Retorno", f"x{_fmt_dec(_a_roas, 1)}" if _a_inv else "—",
                                 f"${_fmt_dec(_a_roas, 2)} por $1" if _a_inv else "—", None),
                            ], _PUB_VIOLETA)

                # Card Ventas diarias — calendario de los últimos 30 días
                with ui.element("div").style(f"flex:1;min-width:280px;{_CARD_NP};overflow:hidden;flex-shrink:0;display:flex;flex-direction:column"):
                    with ui.element("div").style("padding:10px 14px;flex:1;min-height:0;display:flex;flex-direction:column"):
                        ui.label("VENTAS DIARIAS — ÚLTIMOS 30 DÍAS").style(f"{_LBL};margin-bottom:6px")
                        _pintar_calendario(ventas_por_dia, facturacion_por_dia, today_local)

                # Card Ventas del mes (V1): facturado vs estimado de fin de mes
                dolar_str2 = (get_cotizador_param("dolar_oficial", user_id) or "1475") if user_id else "1475"
                dolar_oficial2 = float(str(dolar_str2).replace(",", ".").strip()) if dolar_str2 else 1475.0
                if dolar_oficial2 <= 0:
                    dolar_oficial2 = 1475.0
                with ui.element("div").style("flex:1;min-width:240px;flex-shrink:0;overflow:hidden;background:#fff;border:1px solid #e0e2e7;border-radius:10px;padding:12px 14px"):
                    with ui.element("div").style("display:flex;justify-content:space-between;align-items:baseline;margin-bottom:8px"):
                        ui.label(f"VENTAS — {mes_actual_nom.upper()}").style(f"{_LBL};margin-bottom:0")
                        ui.label(f"día {today_local.day} de {calendar.monthrange(today_local.year, today_local.month)[1]} · faltan {calendar.monthrange(today_local.year, today_local.month)[1] - today_local.day}").style("font-size:10px;color:#9CA3AF")
                    # Selector de moneda: re-renderiza solo esta tarjeta con los datos ya cargados (sin recargar ni pedir nada)
                    estado_vm = {"moneda": "ARS"}
                    ads_vm = _ads_gasto_mes(user_id, today_local)
                    _css_ventas_mes()
                    html_arriba = ui.html("")
                    with ui.element("div").style("display:flex;justify-content:flex-end;margin:1px 0"):
                        pill_vm_ars, pill_vm_usd, _activar_vm = _pills_moneda()
                    html_tabla = ui.html("")

                    def _redibujar_vm() -> None:
                        arriba, abajo = _ventas_mes_partes(por_mes, margen_mes, today_local, dolar_oficial2, ads_vm, estado_vm["moneda"])
                        html_arriba.set_content(arriba)
                        html_tabla.set_content(abajo)
                        _activar_vm(estado_vm["moneda"])

                    def _aplicar_moneda_vm(moneda: str) -> None:
                        estado_vm["moneda"] = moneda
                        _redibujar_vm()

                    pill_vm_ars.on("click", lambda: _aplicar_moneda_vm("ARS"))
                    pill_vm_usd.on("click", lambda: _aplicar_moneda_vm("USD"))
                    _redibujar_vm()


# ---------------------------------------------------------------------------
# Tab principal
# ---------------------------------------------------------------------------

def build_tab_estadisticas(estadisticas_container) -> None:
    """Pestaña Estadísticas: datos de la cuenta ML, reputación y ventas. Carga síncrona con botón Actualizar."""
    user = _require_login()
    if not user:
        return

    access_token = get_ml_access_token(user["id"])
    if not access_token:
        with estadisticas_container:
            with ui.column().classes("w-full max-w-2xl gap-4"):
                ui.label("Bienvenido a BDC systems").classes("text-2xl font-semibold")
                ui.label(
                    "Conectá tu cuenta de MercadoLibre en Configuración para ver aquí tu perfil, reputación y ventas."
                ).classes("text-gray-600")
        return

    def cargar_y_pintar() -> None:
        estadisticas_container.clear()
        with estadisticas_container:
            with ui.card().classes("w-full p-8 items-center gap-4"):
                ui.spinner(size="xl")
                ui.label("Cargando datos...").classes("text-xl text-gray-700")
        background_tasks.create(_cargar_estadisticas_async(), name="cargar_estadisticas")

    async def _cargar_estadisticas_async() -> None:
        try:
            t_inicio = time.perf_counter()

            t0 = time.perf_counter()
            profile = await run.io_bound(ml_get_user_profile, access_token)
            logging.warning(f"[TIMING] ml_get_user_profile: {time.perf_counter()-t0:.2f}s")

            t0 = time.perf_counter()
            seller_id = (profile or {}).get("id") or await run.io_bound(ml_get_user_id, access_token)
            logging.warning(f"[TIMING] ml_get_user_id (si aplica): {time.perf_counter()-t0:.2f}s")

            orders_data: Dict[str, Any] = {}
            items_data: Dict[str, Any] = {"results": []}
            shipments_today: Optional[Dict[str, int]] = None
            if seller_id:
                t0 = time.perf_counter()
                orders_data = await run.io_bound(
                    ml_get_orders_incremental, access_token, str(seller_id), user["id"])
                logging.warning(
                    f"[TIMING] ml_get_orders_incremental ({len(orders_data.get('results', []))}): "
                    f"{time.perf_counter()-t0:.2f}s")

                _tz_arg = timezone(timedelta(hours=-3))
                _today_str = datetime.now(_tz_arg).strftime("%Y-%m-%d")
                ordenes_hoy = [_ord for _ord in (orders_data.get("results") or [])
                               if es_venta(_ord) and str(fecha_venta(_ord)) == _today_str]
                try:
                    t0 = time.perf_counter()
                    shipments_today = await run.io_bound(_logistica_hoy, access_token, str(seller_id), ordenes_hoy)
                    logging.warning(f"[TIMING] _logistica_hoy ({len(ordenes_hoy)} ordenes): {time.perf_counter()-t0:.2f}s")
                except Exception:
                    logging.exception("[ESTADISTICAS] no se pudo obtener la logistica de las ventas de hoy")

            pending_labels: Dict[str, int] = {"total": 0, "flex": 0, "correo": 0}
            if seller_id:
                try:
                    t0 = time.perf_counter()
                    pending_labels = await run.io_bound(ml_get_pending_labels, access_token, str(seller_id))
                    logging.warning(f"[TIMING] ml_get_pending_labels: {time.perf_counter()-t0:.2f}s")
                except Exception:
                    pass

            dispatch_deadline: Optional[str] = None
            if seller_id:
                try:
                    t0 = time.perf_counter()
                    _sched = await run.io_bound(ml_get_dispatch_schedule, access_token, str(seller_id))
                    _prefs = await run.io_bound(ml_get_shipping_preferences, access_token, str(seller_id))
                    logging.warning(f"[TIMING] ml_get_dispatch_schedule + preferences: {time.perf_counter()-t0:.2f}s")
                    _picking_type = ((_prefs or {}).get("picking_type") or "")
                    if _sched:
                        _days = {0: "monday", 1: "tuesday", 2: "wednesday",
                                 3: "thursday", 4: "friday", 5: "saturday", 6: "sunday"}
                        _day_key = _days[datetime.now().weekday()]
                        _day_data = (_sched.get("schedule") or {}).get(_day_key, {})
                        if _day_data.get("work") and _day_data.get("detail"):
                            _from = (_day_data["detail"][0].get("from") or "")
                            if _from and ":" in _from:
                                _h, _m = map(int, _from.split(":"))
                                if _picking_type == "cross_docking":
                                    dispatch_deadline = f"{_h:02d}:{_m:02d}"
                                else:
                                    _dl = datetime(2000, 1, 1, _h, _m) - timedelta(minutes=30)
                                    dispatch_deadline = f"{_dl.hour:02d}:{_dl.minute:02d}"
                except Exception:
                    pass

            try:
                t0 = time.perf_counter()
                items_data = await run.io_bound(ml_get_my_items, access_token, False)
                logging.warning(f"[TIMING] ml_get_my_items: {time.perf_counter()-t0:.2f}s")
                # Override de marca (marcas_override, ver tabs/precios.py): corrige marcas mal
                # cargadas en ML que no se pueden editar ahí (ej. "PlayStation" -> "Sony"), para
                # que el KPI de "marcas distintas" y el resto de esta pantalla sean consistentes
                # con Productos.
                _marca_override_map = get_marca_override_map(user["id"])
                if _marca_override_map:
                    for _it in (items_data or {}).get("results", []):
                        _m = _it.get("marca")
                        if _m and _m in _marca_override_map:
                            _it["marca"] = _marca_override_map[_m]
            except Exception:
                pass

            questions: Optional[List[Dict[str, Any]]] = None
            if seller_id:
                try:
                    t0 = time.perf_counter()
                    questions = await run.io_bound(ml_get_unanswered_questions, access_token, str(seller_id))
                    logging.warning(f"[TIMING] ml_get_unanswered_questions: {time.perf_counter()-t0:.2f}s")
                except Exception:
                    logging.exception(
                        "[ESTADISTICAS] no se pudo obtener preguntas sin responder (seller_id=%s)",
                        seller_id,
                    )

            tiempo_ml: Optional[Dict[str, Any]] = None
            if seller_id:
                try:
                    tiempo_ml = await run.io_bound(_tiempo_respuesta_ml, user["id"], access_token, str(seller_id))
                except Exception:
                    logging.exception("[ESTADISTICAS] no se pudo obtener el tiempo de respuesta de ML (seller_id=%s)", seller_id)

            logging.warning(f"[TIMING] TOTAL estadisticas: {time.perf_counter()-t_inicio:.2f}s")

        except Exception as e:
            estadisticas_container.clear()
            with estadisticas_container:
                ui.label(f"❌ Error al cargar datos: {e}").classes("text-negative")
            return
        estadisticas_container.clear()
        with estadisticas_container:
            _pintar_home_inline(estadisticas_container, profile, orders_data, user_id=user["id"], items_data=items_data, on_refresh=cargar_y_pintar, shipments_today=shipments_today, questions=questions, dispatch_deadline=dispatch_deadline, pending_labels=pending_labels, access_token=access_token, tiempo_ml=tiempo_ml)

    cargar_y_pintar()
