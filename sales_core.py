"""
sales_core.py
Criterio ÚNICO de "venta" para todas las pantallas (Estadísticas, Ventas, Balance, Stock, TACOS).

  - Es venta   : orden con status en {paid, partially_refunded}. Excluye cancelled, payment_required,
                 confirmed, etc. (ml_orders_cache se mantiene fresco con ordenes_cache_refresh.py).
                 Tampoco es venta si lo reembolsado de sus pagos (transaction_amount_refunded, o el pago
                 entero si su status es refunded) es >= a lo pagado (pagos que no estan rejected/cancelled).
                 Tampoco es venta si algun pago esta en charged_back. in_mediation SI cuenta como venta.
  - Fecha      : date_created convertida a hora Argentina (UTC-3) usando el offset real de la cadena
                 (ML devuelve -04:00 y eso corre de día las órdenes de las 23:xx).
  - Importe    : Σ unit_price × cantidad de los ítems (sin envío ni comisiones); fallback
                 total_amount / paid_amount / primer pago.
  - Unidades   : Σ cantidad de los ítems (1 si no hay ítems pero hay importe).

Módulo sin dependencias del proyecto (importable desde ml_api, db y tabs sin ciclos).
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

ART = timezone(timedelta(hours=-3))
ESTADOS_VENTA = frozenset({"paid", "partially_refunded"})


def es_venta_status(status: Any) -> bool:
    return str(status or "").strip().lower() in ESTADOS_VENTA


def _num(v: Any) -> float:
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


def reembolso_total(o: Dict[str, Any]) -> bool:
    """True si lo reembolsado de los pagos de la orden es >= a lo pagado. Pagos rejected/cancelled no
    cuentan como pagados; un pago con status refunded cuenta como reembolsado por completo."""
    pagado = reembolsado = 0.0
    for p in o.get("payments") or []:
        if not isinstance(p, dict) or p.get("status") in ("rejected", "cancelled"):
            continue
        monto = _num(p.get("transaction_amount"))
        pagado += monto
        reembolsado += monto if p.get("status") == "refunded" else _num(p.get("transaction_amount_refunded"))
    return pagado > 0 and reembolsado >= pagado - 0.005


def con_contracargo(o: Dict[str, Any]) -> bool:
    return any(isinstance(p, dict) and p.get("status") == "charged_back" for p in o.get("payments") or [])


def es_venta(o: Dict[str, Any]) -> bool:
    return es_venta_status(o.get("status")) and not reembolso_total(o) and not con_contracargo(o)


def fecha_venta(o: Dict[str, Any]) -> Optional[date]:
    """Día (hora Argentina) de date_created; si falta, date_closed / date_last_updated."""
    for k in ("date_created", "date_closed", "date_last_updated"):
        s = o.get(k)
        if not s or not isinstance(s, str):
            continue
        try:
            dt = datetime.fromisoformat(s.strip().replace("Z", "+00:00"))
            if dt.tzinfo is None:
                return dt.date()
            return dt.astimezone(ART).date()
        except ValueError:
            try:
                return datetime.strptime(s[:10], "%Y-%m-%d").date()
            except ValueError:
                continue
    return None


def _items(o: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [it for it in (o.get("order_items") or o.get("items") or []) if isinstance(it, dict)]


def _monto_orden(o: Dict[str, Any]) -> float:
    """total_amount / paid_amount / primer pago (el importe de la orden sin mirar ítems)."""
    amt = o.get("total_amount") or o.get("paid_amount")
    if amt is None and o.get("payments"):
        pay = o["payments"][0] if isinstance(o["payments"], list) and o["payments"] else {}
        amt = pay.get("total_amount") or pay.get("total_paid_amount") or pay.get("transaction_amount")
    return _num(amt)


def lineas_venta(o: Dict[str, Any]) -> List[Dict[str, Any]]:
    """[{item, cantidad, monto}] por ítem (monto = unit_price × cantidad)."""
    out = []
    for it in _items(o):
        q = int(_num(it.get("quantity") or it.get("qty")))
        out.append({"item": it, "cantidad": q, "monto": _num(it.get("unit_price")) * q})
    return out


def unidades_venta(o: Dict[str, Any]) -> int:
    u = sum(l["cantidad"] for l in lineas_venta(o))
    if u == 0 and _monto_orden(o) > 0:
        u = 1
    return u


def _monto_bruto(o: Dict[str, Any]) -> float:
    items = _items(o)
    if items and all(it.get("unit_price") is not None for it in items):
        m = sum(l["monto"] for l in lineas_venta(o))
        if m > 0:
            return m
    return _monto_orden(o)


def monto_reembolsado(o: Dict[str, Any]) -> float:
    return sum(_num(p.get("transaction_amount_refunded")) for p in o.get("payments") or [] if isinstance(p, dict))


def monto_venta(o: Dict[str, Any]) -> float:
    """Importe de la venta. En ordenes partially_refunded se resta lo reembolsado (suma de
    transaction_amount_refunded de sus pagos)."""
    m = _monto_bruto(o)
    if str(o.get("status") or "").strip().lower() == "partially_refunded":
        m = max(0.0, m - monto_reembolsado(o))
    return m


def ventas_en_rango(orders: Iterable[Dict[str, Any]], d0: date, d1: date) -> List[Dict[str, Any]]:
    """Órdenes que son venta y cuya fecha (ART) cae en [d0, d1] inclusive."""
    out = []
    for o in orders:
        if not isinstance(o, dict) or not es_venta(o):
            continue
        f = fecha_venta(o)
        if f is not None and d0 <= f <= d1:
            out.append(o)
    return out


def resumen_ventas(orders: Iterable[Dict[str, Any]], d0: date, d1: date) -> Dict[str, float]:
    sel = ventas_en_rango(orders, d0, d1)
    return {"unidades": sum(unidades_venta(o) for o in sel),
            "monto": sum(monto_venta(o) for o in sel),
            "ordenes": len(sel)}
