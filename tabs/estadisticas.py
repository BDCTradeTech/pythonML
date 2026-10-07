"""
Fase 3 — tabs/estadisticas.py
Pestaña Estadísticas: datos de la cuenta ML, reputación y ventas.
"""
from __future__ import annotations
import calendar
import re
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
    ml_get_shipments_today,
    ml_get_pending_labels,
    ml_get_my_items,
    ml_get_unanswered_questions,
    ml_get_dispatch_schedule,
    ml_get_shipping_preferences,
    _parse_ml_item_body,
)
from db import get_connection, get_cotizador_param, get_marca_override_map, get_ads_campaign_daily_range


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


def _titulo_seccion(texto: str, color: str, margin_top: int = 0) -> None:
    """Título de sección con un punto de color antes (estética B)."""
    with ui.element("div").style(f"display:flex;align-items:center;gap:6px;margin-top:{margin_top}px;margin-bottom:3px"):
        ui.element("div").style(f"width:8px;height:8px;border-radius:50%;background:{color};flex-shrink:0")
        ui.label(texto).style("font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:.05em;font-weight:500")


def _kpi_b(cuadros: List[Tuple[str, str, str, Optional[float]]], color: str) -> None:
    """Fila de cuadros estética B: etiqueta, número gris oscuro, subtexto y (opcional) barra de proporción.
    Todos del mismo alto (la barra reserva su lugar aunque no se muestre). cuadros = (etiqueta, valor, subtexto, % o None)."""
    with ui.row().classes("w-full flex-nowrap").style("gap:5px"):
        for _lx, _val, _sub, _pct in cuadros:
            _w = max(0.0, min(100.0, _pct)) if _pct is not None else 0.0
            _barra = (
                f'<div style="height:4px;background:#E5E7EB;border-radius:2px;margin-top:3px">'
                f'<div style="height:4px;width:{_w:.1f}%;background:{color};border-radius:2px"></div></div>'
                if _pct is not None else '<div style="height:4px;margin-top:3px"></div>'
            )
            ui.html(
                f'<div style="background:#f9fafb;border:1px solid #e5e7eb;border-left:3px solid {color};border-radius:6px;'
                f'padding:4px 2px 4px 6px;width:100%;box-sizing:border-box;overflow:hidden">'
                f'<div style="font-size:11px;color:#6b7280;white-space:nowrap">{_lx}</div>'
                f'<div style="font-size:18px;font-weight:500;color:#111827;line-height:1.2;white-space:nowrap">{_val}</div>'
                f'<div style="font-size:11px;color:#9ca3af;white-space:nowrap">{_sub}</div>'
                f'{_barra}'
                f'</div>'
            ).style("flex:1;min-width:0")


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

def _pintar_home_inline(
    container, profile: Optional[Dict], orders_data: Dict[str, Any], user_id: Optional[int] = None, items_data: Optional[Dict[str, Any]] = None, on_refresh: Optional[Callable[[], None]] = None, shipments_today: Optional[Dict[str, int]] = None, questions: Optional[List] = None, dispatch_deadline: Optional[str] = None, pending_labels: Optional[Dict[str, int]] = None, access_token: Optional[str] = None,
) -> None:
    """Pinta el contenido del Home con los datos ya cargados. on_refresh permite actualizar datos al vuelo."""
    raw_orders = orders_data.get("results") or orders_data.get("orders") or orders_data.get("elements") or []
    results = [o for o in raw_orders if isinstance(o, dict)]
    rep = (profile or {}).get("seller_reputation") or {}
    today_local = datetime.now().date()
    primer_dia_mes = today_local.replace(day=1)
    hoy_unidades, hoy_monto = 0, 0.0
    flex_hoy = 0
    me_hoy = 0
    ayer_unidades, ayer_monto = 0, 0.0
    antes_ayer_unidades, antes_ayer_monto = 0, 0.0
    semana_unidades, semana_monto = 0, 0.0
    d15_unidades, d15_monto = 0, 0.0
    d21_unidades, d21_monto = 0, 0.0
    mes_unidades, mes_monto = 0, 0.0
    d60_unidades, d60_monto = 0, 0.0
    d90_unidades, d90_monto = 0, 0.0
    ventas_mes_actual_unid, ventas_mes_actual_monto = 0, 0.0
    por_mes: Dict[str, Any] = {}
    top_productos: Dict[str, Dict[str, Any]] = {}  # item_id -> {title, units}
    ayer_local = today_local - timedelta(days=1)
    antes_ayer_local = today_local - timedelta(days=2)

    for ord_item in results:
        dt_str = ord_item.get("date_created") or ord_item.get("date_closed") or ord_item.get("date_last_updated") or ""
        if not dt_str or not isinstance(dt_str, str):
            continue
        try:
            dt = datetime.strptime(dt_str[:10], "%Y-%m-%d").date()
        except Exception:
            continue
        total_amount = ord_item.get("total_amount") or ord_item.get("paid_amount")
        if total_amount is None and ord_item.get("payments"):
            pay = ord_item["payments"][0] if isinstance(ord_item["payments"], list) else {}
            total_amount = pay.get("total_amount") or pay.get("total_paid_amount") or pay.get("transaction_amount")
        try:
            total_amount = float(total_amount or 0)
        except (TypeError, ValueError):
            total_amount = 0.0
        items = ord_item.get("order_items") or ord_item.get("items") or []
        units = sum(int(it.get("quantity") or it.get("qty") or 0) for it in items if isinstance(it, dict))
        if units == 0 and total_amount > 0:
            units = 1
        if dt == today_local:
            hoy_unidades += units
            hoy_monto += total_amount
            logistic = (ord_item.get("shipping") or {}).get("logistic_type") or ""
            if logistic == "self_service":
                flex_hoy += 1
            elif logistic in ("fulfillment", "xd_drop_off", "drop_off", "cross_docking"):
                me_hoy += 1
        if dt == ayer_local:
            ayer_unidades += units
            ayer_monto += total_amount
        if dt == antes_ayer_local:
            antes_ayer_unidades += units
            antes_ayer_monto += total_amount
        days_ago = (today_local - dt).days
        if days_ago <= 6:
            semana_unidades += units
            semana_monto += total_amount
        if days_ago <= 14:
            d15_unidades += units
            d15_monto += total_amount
        if days_ago <= 20:
            d21_unidades += units
            d21_monto += total_amount
        if days_ago <= 30:
            mes_unidades += units
            mes_monto += total_amount
        if days_ago <= 59:
            d60_unidades += units
            d60_monto += total_amount
        if days_ago <= 89:
            d90_unidades += units
            d90_monto += total_amount
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

    # Si se obtuvo conteo directo de /shipments/search, tiene prioridad sobre el loop
    if shipments_today is not None:
        flex_hoy = shipments_today.get("flex", 0)
        me_hoy = shipments_today.get("me", 0)
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

            no_concretadas = max(0, hoy_unidades - flex_hoy - me_hoy)
            nc_color = "#dc2626" if no_concretadas > 0 else "#6b7280"

            with ui.row().classes("w-full gap-2 flex-wrap items-stretch"):
                # BLOQUE 1 — Tienda
                with ui.element("div").style("flex:1.1;min-width:280px;background:#fff;border:1px solid #e0e2e7;border-radius:10px;padding:10px 14px"):
                    with ui.element("div").style("display:flex;align-items:center;justify-content:space-between;border-bottom:2px solid #1d4ed8;padding-bottom:5px;margin-bottom:8px"):
                        ui.label("TIENDA").style("font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:.04em;font-weight:500")
                        if on_refresh:
                            ui.button("↻ Actualizar", on_click=lambda: on_refresh()).props("flat dense").style(f"font-size:10px;color:{_BLUE};padding:0;min-height:0")
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
                            ui.label(fmt_m(hoy_monto)).style("font-size:11px;color:#6b7280")
                        with ui.element("div").style("flex:1;padding:0 14px;border-right:0.5px solid #e5e7eb"):
                            ui.label("MOTO FLEX HOY").style("font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:.04em")
                            ui.label(fmt_n(flex_hoy)).style("font-size:22px;font-weight:600;color:#6b7280;line-height:1.2")
                            ui.label("órdenes").style("font-size:11px;color:#6b7280")
                        with ui.element("div").style("flex:1;padding:0 14px;border-right:0.5px solid #e5e7eb"):
                            correo_lbl = f"CORREO ({dispatch_deadline} hs)" if dispatch_deadline else "CORREO"
                            ui.label(correo_lbl).style("font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:.04em")
                            ui.label(fmt_n(me_hoy)).style("font-size:22px;font-weight:600;color:#6b7280;line-height:1.2")
                            ui.label("órdenes").style("font-size:11px;color:#6b7280")
                        with ui.element("div").style("flex:1;padding-left:14px"):
                            ui.label("NO CONCRETADAS").style("font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:.04em")
                            ui.label(fmt_n(no_concretadas)).style("font-size:22px;font-weight:600;color:#6b7280;line-height:1.2")
                            ui.label("cancel./pend.").style("font-size:11px;color:#6b7280")

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
                    with ui.element("div").style("border-bottom:2px solid #16a34a;padding-bottom:5px;margin-bottom:8px"):
                        ui.label(f"FACTURACIÓN — {mes_actual_nom.upper()}").style("font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:.04em;font-weight:500")
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
            level_id = rep.get("level_id") or "—"
            level_label = {"1_red": "Rojo", "2_orange": "Naranja", "3_yellow": "Amarillo", "4_light_green": "Verde claro", "5_green": "Verde"}.get(str(level_id), str(level_id))
            level_colors = {"1_red": "#ef4444", "2_orange": "#f97316", "3_yellow": "#eab308", "4_light_green": "#84cc16", "5_green": "#22c55e"}
            level_color = level_colors.get(str(level_id), "#6b7280")
            MAX_CLAIMS, MAX_MEDIAT, MAX_CANC, MAX_DELAYED = 0.01, 0.005, 0.005, 0.08

            def _to_float_rate(v: Any) -> Optional[float]:
                if v is None:
                    return None
                try:
                    x = float(v)
                    return x if 0 < x <= 1 else x / 100.0
                except (TypeError, ValueError):
                    return None

            def _semaforo(rate_raw: Any, max_val: float, label: str) -> None:
                rate_f = _to_float_rate(rate_raw)
                if rate_f is None:
                    rate_pct_str = "—"
                    color = "#9ca3af"
                    bar_pct = 0.0
                elif rate_f == 0:
                    rate_pct_str = "0,00%"
                    color = "#16A34A"
                    bar_pct = 0.0
                else:
                    rate_pct_str = f"{rate_f * 100:.2f}%".replace(".", ",")
                    ratio = rate_f / max_val if max_val > 0 else 1.0
                    if ratio < 0.5:
                        color = "#16A34A"
                    elif ratio < 0.9:
                        color = "#BA7517"
                    else:
                        color = "#A32D2D"
                    bar_pct = min(ratio * 100, 100)
                with ui.element("div").style("margin-bottom:5px"):
                    with ui.element("div").style("display:flex;align-items:center;gap:6px"):
                        with ui.element("div").style(f"width:8px;height:8px;border-radius:50%;background:{color};flex-shrink:0"):
                            pass
                        ui.label(label).style("font-size:11px;flex:1;color:#374151")
                        ui.label(rate_pct_str).style(f"font-size:11px;font-weight:600;color:{color}")
                    with ui.element("div").style("height:3px;border-radius:2px;background:#f3f4f6;margin-top:3px"):
                        with ui.element("div").style(f"height:3px;border-radius:2px;background:{color};width:{bar_pct:.1f}%"):
                            pass

            def _pct_fmt(val: Any) -> str:
                if val is None:
                    return "—"
                try:
                    v = float(val)
                    return f"{(v * 100 if 0 <= v <= 1 else v):.2f}%"
                except (TypeError, ValueError):
                    return "—"

            with ui.row().classes("w-full gap-2 flex-wrap items-stretch overflow-hidden max-w-full"):
                # Card Reputación
                with ui.element("div").style(f"flex:1;min-width:220px;{_CARD_NP};overflow:hidden;flex-shrink:0"):
                    with ui.element("div").style("padding:12px 14px"):
                        ui.label("REPUTACIÓN").style(f"{_LBL};margin-bottom:8px")
                        with ui.row().classes("gap-2 items-center mb-3"):
                            with ui.element("div").style(f"width:10px;height:10px;border-radius:50%;background:{level_color};flex-shrink:0"):
                                pass
                            ui.label(f"Nivel: {level_label}").style(f"color:{level_color};font-weight:600;font-size:13px")
                        _semaforo(rate_claims, MAX_CLAIMS, f"Reclamos (máx {MAX_CLAIMS*100:.0f}%)")
                        _semaforo(rate_mediat, MAX_MEDIAT, f"Mediaciones (máx {MAX_MEDIAT*100:.1f}%)")
                        _semaforo(rate_canc, MAX_CANC, f"Cancelaciones (máx {MAX_CANC*100:.1f}%)")
                        _semaforo(rate_delayed, MAX_DELAYED, f"Demora envíos (máx {MAX_DELAYED*100:.0f}%)")
                        if questions is not None:
                            n_q = len(questions)
                            q_color = "#16A34A" if n_q == 0 else "#A32D2D"
                            with ui.element("div").style("margin-bottom:5px"):
                                with ui.element("div").style("display:flex;align-items:center;gap:6px"):
                                    with ui.element("div").style(
                                        f"width:8px;height:8px;border-radius:50%;"
                                        f"background:{q_color};flex-shrink:0"):
                                        pass
                                    ui.label("Preguntas sin responder").style("font-size:11px;flex:1;color:#374151")
                                    ui.label(str(n_q)).style(f"font-size:11px;font-weight:600;color:{q_color}")

                # Card Ventas períodos
                with ui.element("div").style(f"flex:1;min-width:300px;{_CARD_NP};overflow:hidden;flex-shrink:0"):
                    with ui.element("div").style("padding:12px 14px"):
                        ui.label("VENTAS POR PERÍODO").style(f"{_LBL};margin-bottom:6px")
                        def _mini(lbl, unid, monto, bg, bdr, col):
                            with ui.element("div").style(f"flex:1;min-width:0;padding:5px 7px;border-radius:4px;background:{bg};border:1px solid {bdr}"):
                                ui.label(lbl).style(f"font-size:10px;color:{col};font-weight:500")
                                ui.label(fmt_n(unid)).style(f"font-size:13px;font-weight:700;color:{col}")
                                ui.label(fmt_m(monto)).style("font-size:9px;color:#6b7280;white-space:nowrap")
                        with ui.column().classes("gap-1 w-full"):
                            with ui.row().classes("gap-1 w-full flex-nowrap"):
                                _mini("Hoy", hoy_unidades, hoy_monto, "#eff6ff", "#bfdbfe", _BLUE)
                                _mini("Ayer", ayer_unidades, ayer_monto, "#f9fafb", "#e5e7eb", "#374151")
                                _mini("Antes de ayer", antes_ayer_unidades, antes_ayer_monto, "#f9fafb", "#e5e7eb", "#374151")
                            with ui.row().classes("gap-1 w-full flex-nowrap"):
                                _mini("7 días", semana_unidades, semana_monto, "#f9fafb", "#e5e7eb", "#374151")
                                _mini("15 días", d15_unidades, d15_monto, "#f9fafb", "#e5e7eb", "#374151")
                                _mini("21 días", d21_unidades, d21_monto, "#f9fafb", "#e5e7eb", "#374151")
                            with ui.row().classes("gap-1 w-full flex-nowrap"):
                                _mini("30 días", mes_unidades, mes_monto, "#f0fdf4", "#d1fae5", _GREEN)
                                _mini("60 días", d60_unidades, d60_monto, "#f9fafb", "#e5e7eb", "#374151")
                                _mini("90 días", d90_unidades, d90_monto, "#f9fafb", "#e5e7eb", "#374151")

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
                            with ui.row().classes("gap-0 no-wrap").style("background:#f3f4f6;border-radius:999px;padding:2px;margin-left:auto"):
                                pill_ars = ui.label("$ ARS")
                                pill_usd = ui.label("US$")
                        chart_fm = ui.echart(chart_options).classes("w-full").style("flex:1;min-height:200px;height:auto")

                        def _redibujar_fm(_chart=chart_fm, _lbl=lbl_prom, _pa=pill_ars, _pu=pill_usd) -> None:
                            moneda = estado_fm["moneda"]
                            opciones, prom = _facturacion_mensual_options(
                                por_mes, today_local, ventas_mes_actual_monto, moneda, dolar_card,
                                6 if estado_fm["angosto"] else 12)
                            _chart.options.clear()
                            _chart.options.update(opciones)
                            _chart.update()
                            _lbl.set_text(f"(prom. {prom})" if prom else "")
                            _lbl.set_visibility(bool(prom))
                            _base = "font-size:10px;font-weight:600;padding:2px 10px;border-radius:999px;cursor:pointer;"
                            _on, _off = f"{_base}background:{_AZUL_BARRA};color:#fff", f"{_base}background:transparent;color:#6b7280"
                            _pa.style(replace=_on if moneda == "ARS" else _off)
                            _pu.style(replace=_on if moneda == "USD" else _off)

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
                                        for ci, col_h in enumerate(["Mes", "Unid", "$ ARS", "u$ USD"]):
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

            # ── FILA 2: Top Ventas | Stock | Graf Semanal | Ventas Mes ────────────
            claims_val = (claims.get("value") or claims.get("excluded", {}).get("real_value") or 0)
            mediat_val = (mediat.get("value") or mediat.get("excluded", {}).get("real_value") or 0) if mediat else 0
            canc_val = (canc.get("value") or canc.get("excluded", {}).get("real_value") or 0)
            postventa_total = claims_val + mediat_val + canc_val

            ventas_por_dia: Dict[str, int] = {}
            facturacion_por_dia: Dict[str, float] = {}
            dias_semana_es = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]
            for d in range(14):
                fd = today_local - timedelta(days=d)
                ventas_por_dia[fd.strftime("%Y-%m-%d")] = 0
                facturacion_por_dia[fd.strftime("%Y-%m-%d")] = 0.0
            for ord_item in results:
                dt_str = ord_item.get("date_created") or ord_item.get("date_closed") or ""
                if not dt_str:
                    continue
                try:
                    dt = datetime.strptime(dt_str[:10], "%Y-%m-%d").date()
                except Exception:
                    continue
                if (today_local - dt).days > 13:
                    continue
                items_ord = ord_item.get("order_items") or ord_item.get("items") or []
                units_ord = sum(int(it.get("quantity") or it.get("qty") or 0) for it in items_ord if isinstance(it, dict))
                if units_ord == 0:
                    total_amount_ord = ord_item.get("total_amount") or ord_item.get("paid_amount") or 0
                    if total_amount_ord and float(total_amount_ord or 0) > 0:
                        units_ord = 1
                key_ord = dt.strftime("%Y-%m-%d")
                if key_ord in ventas_por_dia:
                    ventas_por_dia[key_ord] += units_ord
                if key_ord in facturacion_por_dia:
                    facturacion_por_dia[key_ord] += float(ord_item.get("total_amount") or ord_item.get("paid_amount") or 0)

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

                top_grouped: Dict[tuple, Dict[str, Any]] = {}
                for _gk, _members in top_groups.items():
                    _units_total = sum(m[1]["units"] for m in _members)
                    _propias = [m for m in _members if _id_to_is_catalog.get(m[0]) is False]
                    _pool = _propias or _members
                    _best_iid, _best_info = max(_pool, key=lambda m: m[1]["units"])
                    _es_solo_catalogo = (not _propias) and all(
                        _id_to_is_catalog.get(m[0]) is True for m in _members)
                    top_grouped[_gk] = {
                        "title": _best_info["title"], "units": _units_total,
                        "solo_catalogo": _es_solo_catalogo,
                    }

                top_list = sorted(top_grouped.values(), key=lambda x: x["units"], reverse=True)[:12]
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
                    with ui.element("div").style("padding:12px 14px"):
                        ui.label(f"TOP VENTAS — {mes_actual_nom.upper()}").style(f"{_LBL};margin-bottom:8px")
                        if not top_list:
                            ui.label("Sin ventas este mes").style("font-size:12px;color:#9ca3af")
                        else:
                            for i, p in enumerate(top_list):
                                pct = (100.0 * p["units"] / total_unid_mes) if total_unid_mes else 0
                                tit = _smart_truncate(p["title"] or "—", 60)
                                with ui.row().classes("w-full items-center gap-2").style("margin-bottom:1px"):
                                    with ui.element("div").style(f"width:16px;height:16px;border-radius:50%;background:{_BLUE};display:flex;align-items:center;justify-content:center;flex-shrink:0"):
                                        ui.label(str(i + 1)).style("color:white;font-size:8px;font-weight:700")
                                    ui.label(tit).style("font-size:11px;color:#111827;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;flex:1;min-width:0")
                                    if p.get("solo_catalogo"):
                                        with ui.element("span").style(
                                                "background:#f3f4f6;color:#6b7280;font-size:8px;font-weight:600;"
                                                "padding:1px 5px;border-radius:8px;flex-shrink:0;white-space:nowrap"):
                                            ui.label("CATÁLOGO")
                                    with ui.element("div").style("display:flex;align-items:center;gap:2px;flex-shrink:0"):
                                        ui.label(f"{p['units']}u").style(f"font-size:11px;color:{_BLUE};font-weight:500;white-space:nowrap")
                                        ui.label(f"· {pct:.1f}%").style("font-size:11px;color:#6b7280;white-space:nowrap")
                            if top_sin_sku:
                                ui.label(f"{top_sin_sku} publicación(es) sin SKU mapeado — no se agruparon").style(
                                    "font-size:9px;color:#9ca3af;margin-top:4px")
                        ui.label("PUBLICACIONES").style(f"{_LBL};margin-top:6px;margin-bottom:3px")
                        with ui.row().classes("gap-2 w-full flex-nowrap"):
                            for _lp, _vp in (("Marcas", str(marcas_distintas)),
                                             ("Publicaciones propias", str(publicaciones_propias_con_stock)),
                                             ("Unidades propias", fmt_n(unidades_propias_en_stock))):
                                with ui.element("div").style("flex:1;text-align:center;padding:2px 4px;background:#f9fafb;border:1px solid #e5e7eb;border-radius:4px"):
                                    ui.label(_lp).style("font-size:9px;color:#6b7280")
                                    ui.label(_vp).style(f"font-size:16px;font-weight:700;color:{_BLUE}")

                def _orden_fecha(o):
                    ds = o.get("date_closed") or o.get("date_created") or o.get("date_last_updated") or ""
                    return ds[:10] if ds else ""
                ultimas_5_ventas = sorted(results, key=_orden_fecha, reverse=True)[:10]

                with ui.element("div").style(f"flex:1;min-width:260px;{_CARD_NP};overflow:hidden;flex-shrink:0"):
                    with ui.element("div").style("padding:12px 14px"):
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
                            _dt_s = (_ord.get("date_created") or _ord.get("date_closed")
                                     or _ord.get("date_last_updated") or "")
                            try:
                                _dt_c = datetime.strptime(_dt_s[:10], "%Y-%m-%d").date()
                            except Exception:
                                continue
                            if not (primer_dia_mes <= _dt_c <= today_local):
                                continue
                            if _ord.get("status") not in ("paid", "payment_required", "confirmed"):
                                continue
                            _items_c = _ord.get("order_items") or _ord.get("items") or []
                            _uds_c = sum(int(it.get("quantity") or it.get("qty") or 0)
                                         for it in _items_c if isinstance(it, dict))
                            if _uds_c == 0 and float(_ord.get("total_amount") or _ord.get("paid_amount") or 0) > 0:
                                _uds_c = 1
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
                            _titulo_seccion(f"PROMOCIONES — {mes_actual_nom.upper()}", _PROMO_ROSA)
                            _kpi_b([
                                ("Unid. con promo", fmt_n(_pr_u), f"{_pr_pu:.0f}% · de {fmt_n(total_unidades_mes_c)}", _pr_pu),
                                ("Fact. con promo", _fmt_corto_ads(_pr_imp),
                                 f"{_pr_pf:.0f}% · de {_fmt_corto_ads(ventas_mes_actual_monto)}", _pr_pf),
                                ("Desc. medio", f"−{_fmt_dec(_pr_desc, 1)}%", "sobre lista", None),
                                ("Cupones", _fmt_corto_ads(_pr_aporte), "en esas ventas", None),
                            ], _PROMO_ROSA)
                        _base_c = total_unidades_mes_c or 1
                        _total_str = f"{total_unidades_mes_c:,}".replace(",", ".")
                        _titulo_seccion(f"VENTAS Y CUOTAS — {mes_actual_nom.upper()} · {_total_str} UNID.", _CUOTAS_AZUL,
                                        margin_top=6 if _pr_u > 0 else 0)
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
                            _titulo_seccion(f"PUBLICIDAD — {mes_actual_nom.upper()}", _PUB_VIOLETA, margin_top=6)
                            _kpi_b([
                                ("Ventas por ads", fmt_n(_a_u), f"{_fmt_dec(_a_pu, 1)}% de tus u." if total_unidades_mes_c else "—",
                                 _a_pu if total_unidades_mes_c else None),
                                ("Facturado ads", _fmt_corto_ads(_a_imp), f"{_fmt_dec(_a_pf, 1)}% del total" if ventas_mes_actual_monto else "—",
                                 _a_pf if ventas_mes_actual_monto else None),
                                ("Gasto en ads", _fmt_corto_ads(_a_inv), f"{fmt_m(_a_inv / _a_u)} x venta" if _a_u else "—", None),
                                ("Retorno", f"x{_fmt_dec(_a_roas, 1)}" if _a_inv else "—",
                                 f"${_fmt_dec(_a_roas, 2)} por $1" if _a_inv else "—", None),
                            ], _PUB_VIOLETA)

                # Card Gráfico Semanal — 14 días
                dias_orden = sorted(ventas_por_dia.keys())[-14:]
                uds_esta_semana = sum(ventas_por_dia.get((today_local - timedelta(days=d)).strftime("%Y-%m-%d"), 0) for d in range(7))
                uds_semana_pasada = sum(ventas_por_dia.get((today_local - timedelta(days=d)).strftime("%Y-%m-%d"), 0) for d in range(7, 14))
                var_pct = ((uds_esta_semana - uds_semana_pasada) / uds_semana_pasada * 100) if uds_semana_pasada > 0 else (100.0 if uds_esta_semana > 0 else 0.0)
                def _fmt_compacto(val: float) -> str:
                    if val >= 1_000_000:
                        return f"${val/1_000_000:.1f}M"
                    elif val >= 1_000:
                        return f"${val/1_000:.0f}K"
                    return f"${int(val)}"

                if dias_orden:
                    chart_labels_sem = []
                    chart_data_sem = []
                    for i, key in enumerate(dias_orden):
                        fd = datetime.strptime(key, "%Y-%m-%d").date()
                        dia_sem = dias_semana_es[fd.weekday()]
                        chart_labels_sem.append(f"{dia_sem} {fd.day}")
                        uds_s = ventas_por_dia.get(key, 0)
                        fact_s = facturacion_por_dia.get(key, 0.0)
                        days_back = (today_local - fd).days
                        if days_back == 0:
                            bar_color_s = _GREEN
                        elif days_back <= 6:
                            bar_color_s = "#3b82f6"
                        else:
                            bar_color_s = "#e5e7eb"
                        fact_str = _fmt_compacto(fact_s)
                        lbl_fmt = f"{{fact|{fact_str}}}\n{{uds|{uds_s}}}"
                        chart_data_sem.append({"value": uds_s, "itemStyle": {"color": bar_color_s}, "label": {"formatter": lbl_fmt}})
                    chart_options_sem = {
                        "backgroundColor": "transparent",
                        "grid": {"left": 35, "right": 15, "top": 60, "bottom": 25},
                        "xAxis": {"type": "category", "data": chart_labels_sem, "axisLabel": {"fontSize": 9, "interval": 0, "rotate": 30}},
                        "yAxis": {"type": "value", "axisLabel": {"fontSize": 9}},
                        "series": [{"type": "bar", "data": chart_data_sem, "barWidth": "60%", "label": {
                            "show": True,
                            "position": "top",
                            "rich": {
                                "fact": {"color": "#6b7280", "fontSize": 8, "align": "center"},
                                "uds":  {"color": "#111827", "fontSize": 9, "fontWeight": "bold", "align": "center"},
                            },
                        }}],
                    }
                    with ui.element("div").style(f"flex:1;min-width:280px;{_CARD_NP};overflow:hidden;min-height:185px;flex-shrink:0"):
                        with ui.element("div").style("padding:10px 14px 4px"):
                            ui.label("UNIDADES VENDIDAS — 14 DÍAS").style(_LBL)
                        ui.echart(chart_options_sem).classes("w-full").style("height:220px")
                        with ui.element("div").style("padding:4px 14px 10px"):
                            prom_7 = uds_esta_semana / 7
                            hoy_u = ventas_por_dia.get(today_local.strftime("%Y-%m-%d"), 0)
                            hoy_vs_prom = ((hoy_u - prom_7) / prom_7 * 100) if prom_7 > 0 else (100.0 if hoy_u > 0 else 0.0)
                            variacion_color = _GREEN if var_pct >= 0 else "#dc2626"
                            hoy_vs_color = _GREEN if hoy_vs_prom >= 0 else "#dc2626"
                            _CELL = "background:#f9fafb;border:0.5px solid #e5e7eb;border-radius:0 4px 4px 0;padding:5px 8px;display:flex;justify-content:space-between;align-items:center"
                            with ui.element("div").style("display:grid;grid-template-columns:1fr 1fr;gap:4px"):
                                with ui.element("div").style(f"{_CELL};border-left:3px solid #1d4ed8"):
                                    ui.label("Esta semana").style("font-size:10px;color:#6b7280")
                                    ui.label(f"{fmt_n(uds_esta_semana)} u").style("font-size:12px;font-weight:500;color:#1d4ed8")
                                with ui.element("div").style(f"{_CELL};border-left:3px solid #6b7280"):
                                    ui.label("Sem. anterior").style("font-size:10px;color:#6b7280")
                                    ui.label(f"{fmt_n(uds_semana_pasada)} u").style("font-size:12px;font-weight:500;color:#6b7280")
                                with ui.element("div").style(f"{_CELL};border-left:3px solid {variacion_color}"):
                                    ui.label("Variación").style("font-size:10px;color:#6b7280")
                                    ui.label(f"{var_pct:+.1f}%").style(f"font-size:12px;font-weight:500;color:{variacion_color}")
                                with ui.element("div").style(f"{_CELL};border-left:3px solid {hoy_vs_color}"):
                                    ui.label("Hoy vs prom 7d").style("font-size:10px;color:#6b7280")
                                    ui.label(f"{hoy_vs_prom:+.0f}%").style(f"font-size:12px;font-weight:500;color:{hoy_vs_color}")
                else:
                    with ui.element("div").style(f"flex:1;min-width:120px;{_CARD};flex-shrink:0"):
                        ui.label("UNIDADES VENDIDAS — 14 DÍAS").style(_LBL)
                        ui.label("Sin datos").style("font-size:12px;color:#9ca3af;margin-top:6px")

                # Card Ventas del mes / Estimaciones
                dias_transcurridos = (today_local - primer_dia_mes).days + 1
                dias_del_mes = calendar.monthrange(today_local.year, today_local.month)[1]
                venta_diaria = ventas_mes_actual_monto / dias_transcurridos if dias_transcurridos > 0 else 0
                venta_estimada_mes = venta_diaria * dias_del_mes if dias_transcurridos > 0 else 0
                dolar_str2 = (get_cotizador_param("dolar_oficial", user_id) or "1475") if user_id else "1475"
                dolar_oficial2 = float(str(dolar_str2).replace(",", ".").strip()) if dolar_str2 else 1475.0
                if dolar_oficial2 <= 0:
                    dolar_oficial2 = 1475.0
                venta_estimada_mes_usd = (venta_estimada_mes / dolar_oficial2) if dolar_oficial2 > 0 else 0
                venta_diaria_u = ventas_mes_actual_unid / dias_transcurridos if dias_transcurridos > 0 else 0
                ticket_prom2 = (ventas_mes_actual_monto / ventas_mes_actual_unid) if ventas_mes_actual_unid > 0 else 0
                venta_x_unidad = ventas_mes_actual_monto / ventas_mes_actual_unid if ventas_mes_actual_unid > 0 else 0
                proyeccion_anual = (ventas_mes_actual_monto / dias_transcurridos * 365) if dias_transcurridos > 0 else 0

                with ui.element("div").style("flex:1;min-width:240px;flex-shrink:0;background:#fff;border:1px solid #e0e2e7;border-radius:10px;padding:12px"):
                    ui.label(f"VENTAS — {mes_actual_nom.upper()}").style(f"{_LBL};margin-bottom:8px")
                    # Bloque 1 — Resultados a la fecha
                    with ui.element("div").style("border-left:3px solid #1d4ed8;background:#f8faff;border-radius:0 6px 6px 0;padding:8px 10px;margin-bottom:6px"):
                        ui.label(f"RESULTADOS AL DÍA {dias_transcurridos}").style("font-size:10px;color:#0c447c;text-transform:uppercase;letter-spacing:.04em;font-weight:600;margin-bottom:4px")
                        ui.label(fmt_m(ventas_mes_actual_monto)).style("font-size:18px;font-weight:500;color:#1d4ed8;margin:3px 0;display:block")
                        with ui.element("div").style("display:flex;justify-content:space-between;padding:2px 0;font-size:11px"):
                            ui.label("Días transcurridos").style("color:#6b7280")
                            ui.label(f"{dias_transcurridos}/{dias_del_mes}").style("font-weight:500;color:#374151")
                        with ui.element("div").style("display:flex;justify-content:space-between;padding:2px 0;font-size:11px"):
                            ui.label("Unidades vendidas").style("color:#6b7280")
                            ui.label(fmt_n(ventas_mes_actual_unid)).style("font-weight:500;color:#374151")
                        with ui.element("div").style("display:flex;justify-content:space-between;padding:2px 0;font-size:11px"):
                            ui.label("Prom. diario").style("color:#6b7280")
                            ui.label(fmt_m(venta_diaria)).style("font-weight:500;color:#374151")
                        with ui.element("div").style("display:flex;justify-content:space-between;padding:2px 0;font-size:11px"):
                            ui.label("Ticket promedio").style("color:#6b7280")
                            ui.label(fmt_m(ticket_prom2)).style("font-weight:500;color:#374151")
                    # Bloque 2 — Estimación fin de mes
                    venta_estimada_unid = int(venta_diaria_u * dias_del_mes) if dias_transcurridos > 0 else 0
                    with ui.element("div").style("border-left:3px solid #16a34a;background:#f0fdf4;border-radius:0 6px 6px 0;padding:8px 10px"):
                        ui.label("ESTIMACIÓN FIN DE MES").style("font-size:10px;color:#15803d;text-transform:uppercase;letter-spacing:.04em;font-weight:600;margin-bottom:4px")
                        ui.label(fmt_m(venta_estimada_mes)).style("font-size:18px;font-weight:500;color:#16a34a;margin:3px 0;display:block")
                        with ui.element("div").style("display:flex;justify-content:space-between;padding:2px 0;font-size:11px"):
                            ui.label("En dólares").style("color:#6b7280")
                            ui.label(f"u$ {fmt_n(venta_estimada_mes_usd)}").style("font-weight:500;color:#374151")
                        with ui.element("div").style("display:flex;justify-content:space-between;padding:2px 0;font-size:11px"):
                            ui.label("Unidades estimadas").style("color:#6b7280")
                            ui.label(fmt_n(venta_estimada_unid)).style("font-weight:500;color:#374151")


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
            shipments_today: Dict[str, int] = {"flex": 0, "me": 0}
            if seller_id:
                t0 = time.perf_counter()
                orders_data = await run.io_bound(
                    ml_get_orders_incremental, access_token, str(seller_id), user["id"])
                logging.warning(
                    f"[TIMING] ml_get_orders_incremental ({len(orders_data.get('results', []))}): "
                    f"{time.perf_counter()-t0:.2f}s")

                _tz_arg = timezone(timedelta(hours=-3))
                _today_str = datetime.now(_tz_arg).strftime("%Y-%m-%d")
                shipping_ids_hoy: List[str] = []
                for _ord in (orders_data.get("results") or []):
                    _dt_str = (_ord.get("date_created") or _ord.get("date_closed") or "")[:10]
                    if _dt_str == _today_str:
                        _ship_id = (_ord.get("shipping") or {}).get("id")
                        if _ship_id:
                            shipping_ids_hoy.append(str(_ship_id))
                try:
                    t0 = time.perf_counter()
                    shipments_today = await run.io_bound(ml_get_shipments_today, access_token, shipping_ids_hoy)
                    logging.warning(f"[TIMING] ml_get_shipments_today ({len(shipping_ids_hoy)} ids): {time.perf_counter()-t0:.2f}s")
                except Exception:
                    pass

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

            logging.warning(f"[TIMING] TOTAL estadisticas: {time.perf_counter()-t_inicio:.2f}s")

        except Exception as e:
            estadisticas_container.clear()
            with estadisticas_container:
                ui.label(f"❌ Error al cargar datos: {e}").classes("text-negative")
            return
        estadisticas_container.clear()
        with estadisticas_container:
            _pintar_home_inline(estadisticas_container, profile, orders_data, user_id=user["id"], items_data=items_data, on_refresh=cargar_y_pintar, shipments_today=shipments_today, questions=questions, dispatch_deadline=dispatch_deadline, pending_labels=pending_labels, access_token=access_token)

    cargar_y_pintar()
