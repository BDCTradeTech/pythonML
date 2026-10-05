"""
margen.py
Cálculo de margen por unidad de una publicación ML. Vive acá -- y no en
tabs/dashboard.py -- para que el cron (salud_audit.py) pueda usarlo sin importar
desde tabs/ (que arrastra nicegui). tabs/dashboard.py re-exporta estos nombres, así
que los imports existentes (`from tabs.dashboard import _calc_margen_prod,
_load_params_prod`) siguen funcionando sin cambios.

Costos de venta REALES de ML (verificados contra /orders + /v1/payments, 2026-10-05):
  - comisión: meli_percentage_fee, depende de categoría y listing (15% / 15,5%).
  - financiación: financing_add_on_fee, % FIJO por campaña de cuotas (8,9 / 13,4 / 17,8 /
    21,6% para x3/x6/x9/x12), sale de financiacion_cuotas_ml. NUNCA de cotizador_datos.cuotas_Nx,
    que es el RECARGO de precio de las variantes en cuotas (tabs/cuotas.py), no el costo.
  - costo fijo por unidad (fixed_fee): solo con precio unitario < $33.000, depende del precio.
Los parámetros nuevos de _calc_margen_prod tienen defaults que reproducen el cálculo
anterior (contado, sin costo fijo), así que los llamadores existentes no cambian.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional, Tuple

import requests

from db import get_cotizador_param, get_financiacion_cuotas_ml


def _pr(s: Any, d: float = 0.0) -> float:
    if s is None or str(s).strip() == "":
        return d
    try:
        v = float(str(s).strip().replace(",", "."))
        return v if v <= 1.5 else v / 100.0
    except (ValueError, TypeError):
        return d


def financiacion_real() -> Dict[str, float]:
    """{"x3": 0.089, "x6": 0.134, ...} desde financiacion_cuotas_ml (costo real de la campaña).
    x1 = 0. Si la tabla no se puede leer devuelve {} (el margen queda sin financiación, como antes)."""
    try:
        return {f"x{n}": float(v["pct"]) for n, v in get_financiacion_cuotas_ml().items()}
    except Exception:
        logging.exception("[MARGEN] no se pudo leer financiacion_cuotas_ml")
        return {}


def _load_params_prod(user_id: int) -> dict:
    ev = float(str(get_cotizador_param("ml_envios", user_id) or 5823).replace(",", "."))
    if ev <= 100:
        ev = 5823.0
    do = float(str(get_cotizador_param("dolar_oficial", user_id) or "1475").replace(",", ".")) or 1475.0
    if do <= 0:
        do = 1475.0
    return {
        "ml_comision":         _pr(get_cotizador_param("ml_comision",          user_id), 0.15),
        "ml_debcre":           _pr(get_cotizador_param("ml_debcre",            user_id), 0.006),
        "ml_iibb_per":         _pr(get_cotizador_param("ml_iibb_per",          user_id), 0.055),
        "ml_envios_gratuitos": float(str(get_cotizador_param("ml_envios_gratuitos", user_id) or 33000).replace(",", ".")),
        "ml_envios_val":       ev,
        "dolar_oficial":       do,
        "financiacion":        financiacion_real(),
    }


# ---------------------------------------------------------------------------
# Comisión y costo fijo por publicación (GET /sites/MLA/listing_prices)
# ---------------------------------------------------------------------------

_FEES_TTL_S = 6 * 3600          # cache de comisión/costo fijo
_FEES_TTL_FALLO_S = 600         # tras un fallo no se insiste por 10 min
_FEES_CACHE: Dict[tuple, tuple] = {}   # (categoría, listing, tramo) -> (vence_ts, comision_pct, fixed_fee, origen)
_UMBRAL_FIJO = 33000.0
# Observado en ventas_datos (sept-oct 2026); solo se usa si listing_prices falla.
_FIJO_TRAMOS = ((33000.0, 0.0), (24000.0, 3320.0), (15000.0, 2740.0), (0.0, 1330.0))


def comision_fallback(listing_type_id: Optional[str]) -> float:
    return 0.155 if str(listing_type_id or "").lower() == "gold_pro" else 0.15


def fixed_fee_fallback(precio: float) -> float:
    for desde, fee in _FIJO_TRAMOS:
        if precio >= desde:
            return fee
    return 0.0


def _tramo(precio: float):
    """El costo fijo cambia por tramo de precio; sobre el umbral es 0 y el resto no cambia."""
    return "33k+" if precio >= _UMBRAL_FIJO else int(round(precio / 500.0))


def fees_publicacion(access_token: Optional[str], category_id: Optional[str], listing_type_id: Optional[str],
                     precio: float, red: bool = True) -> Tuple[float, float, str]:
    """(comision_pct en 0..1, fixed_fee por unidad, origen) de UNA publicación.
    origen: 'cache' | 'api' | 'fallback'. Con red=False solo mira el cache y cae al fallback.
    Se usan meli_percentage_fee y fixed_fee por separado; NUNCA sale_fee_amount (ya incluye
    el fixed_fee y la financiación) para no sumarlos dos veces."""
    lt = str(listing_type_id or "").lower()
    fb = (comision_fallback(lt), fixed_fee_fallback(precio))
    if not category_id or not lt or precio <= 0:
        return fb[0], fb[1], "fallback"
    key = (str(category_id), lt, _tramo(precio))
    hit = _FEES_CACHE.get(key)
    ahora = time.time()
    if hit and hit[0] > ahora:
        return hit[1], hit[2], ("cache" if hit[3] == "api" else hit[3])
    if not red or not access_token:
        return fb[0], fb[1], "fallback"
    try:
        r = None
        for espera in (0, 2):
            if espera:
                time.sleep(espera)
            r = requests.get(
                "https://api.mercadolibre.com/sites/MLA/listing_prices",
                params={"price": round(precio, 2), "category_id": category_id,
                        "listing_type_id": lt, "currency_id": "ARS"},
                headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
                timeout=10,
            )
            if r.status_code != 429 and r.status_code < 500:
                break
        if r is not None and r.status_code == 200:
            j = r.json()
            j = j[0] if isinstance(j, list) else j
            d = j.get("sale_fee_details") or {}
            com = float(d.get("meli_percentage_fee")) / 100.0
            fijo = float(d.get("fixed_fee") or 0.0)
            _FEES_CACHE[key] = (ahora + _FEES_TTL_S, com, fijo, "api")
            return com, fijo, "api"
        logging.warning("[MARGEN] listing_prices HTTP %s cat=%s lt=%s", getattr(r, "status_code", None), category_id, lt)
    except Exception:
        logging.exception("[MARGEN] listing_prices fallo cat=%s lt=%s", category_id, lt)
    _FEES_CACHE[key] = (ahora + _FEES_TTL_FALLO_S, fb[0], fb[1], "fallback")
    return fb[0], fb[1], "fallback"


def fee_estimado_orden(access_token: Optional[str], category_id: Optional[str], listing_type_id: Optional[str],
                       unit_price: float, cantidad: int, cuotas: str = "x1") -> Tuple[float, float]:
    """(meli_fee, cuotas_fee) ESTIMADOS de una orden sin charges reales (fee_origen 'estimada'):
    comisión de la publicación (fees_publicacion; fallback 15 / 15,5 %) y financiación REAL de la
    campaña (financiacion_cuotas_ml), ambas sobre el total de la orden. El costo fijo no va acá:
    los llamadores ya lo traen aparte (ml_get_fixed_fee)."""
    total = float(unit_price) * max(int(cantidad or 1), 1)
    com, _fijo, _o = fees_publicacion(access_token, category_id, listing_type_id, float(unit_price))
    fin = financiacion_real().get(str(cuotas or "x1").strip().lower(), 0.0)
    return total * com, total * fin


def bonif_ml_promo(amount: float, meli_pct: Any, seller_pct: Any, *bases: Any) -> float:
    """Monto que aporta ML en una promo cofinanciada. La base del % es la que cumple
    base × (1 − (%ML + %vendedor)) ≈ precio de venta; si ninguna cuadra, la primera disponible."""
    m = float(meli_pct or 0)
    if m <= 0:
        return 0.0
    s = float(seller_pct or 0)
    disp = [float(b) for b in bases if b]
    for b in disp:
        if abs(b * (1 - (m + s) / 100.0) - amount) <= 2.0:
            return b * m / 100.0
    return (disp[0] if disp else float(amount)) * m / 100.0


# ---------------------------------------------------------------------------
# Margen
# ---------------------------------------------------------------------------

def calc_margen_detalle(precio: float, costo_usd: float, tipo_iva: float, p: dict, cantidad: int = 1,
                        cuotas: str = "x1", comision_pct: Optional[float] = None,
                        fixed_fee: float = 0.0, bonif_ml: float = 0.0) -> dict:
    """Desglose por unidad al precio unitario `precio`. `cantidad` (default 1) prorratea el
    envío: el envío es POR ENVÍO, no por unidad (envío / N por unidad), y el umbral de envío
    gratis se evalúa sobre el TOTAL de la orden (precio × cantidad) -- verificado con
    GET /items/{id}/shipping_options?quantity=N, 2026-09-30.
    cuotas: "x1".."x12" -> financiación REAL de p["financiacion"] (0 si no está).
    comision_pct: fracción (0.155); None = p["ml_comision"]. fixed_fee: por unidad.
    bonif_ml: aporte de ML por unidad (suma al margen)."""
    cantidad = max(int(cantidad or 1), 1)
    cpct         = p["ml_comision"] if comision_pct is None else float(comision_pct)
    comision     = precio * cpct
    cobrado      = precio - comision
    deb_cred     = precio * p["ml_debcre"]
    iibb         = precio * p["ml_iibb_per"]
    iva_venta    = precio * tipo_iva / (1 + tipo_iva)
    iva_meli     = comision * 0.21 / 1.21
    iva_impor    = 0.09 * costo_usd * p["dolar_oficial"]
    iva_total    = iva_venta - iva_meli - iva_impor
    envio        = 0.0 if precio * cantidad < p["ml_envios_gratuitos"] else p["ml_envios_val"] / cantidad
    costo_pesos  = costo_usd * p["dolar_oficial"]
    fin_pct      = (p.get("financiacion") or {}).get(str(cuotas or "x1").strip().lower(), 0.0)
    financiacion = precio * fin_pct
    fixed_fee    = float(fixed_fee or 0.0)
    margen = (cobrado - costo_pesos - iva_total - iibb - deb_cred - envio
              - financiacion - fixed_fee + float(bonif_ml or 0.0))
    return {
        "comision": comision, "comision_pct": cpct, "cobrado": cobrado, "deb_cred": deb_cred, "iibb": iibb,
        "iva_venta": iva_venta, "iva_meli": iva_meli, "iva_impor": iva_impor, "iva_total": iva_total,
        "envio": envio, "costo_pesos": costo_pesos, "financiacion": financiacion, "financiacion_pct": fin_pct,
        "fixed_fee": fixed_fee, "bonif_ml": float(bonif_ml or 0.0),
        "margen": margen, "margen_pct": (margen / precio * 100) if precio > 0 else 0.0,
    }


def _calc_margen_prod(precio: float, costo_usd: float, tipo_iva: float, p: dict, cantidad: int = 1,
                      cuotas: str = "x1", comision_pct: Optional[float] = None,
                      fixed_fee: float = 0.0, bonif_ml: float = 0.0) -> Optional[float]:
    """Margen por unidad al precio unitario `precio` (ver calc_margen_detalle). Con los defaults
    (cuotas x1, sin costo fijo ni bonificación) el resultado es idéntico al de siempre, por eso
    dashboard.py, descuentos.py y el motor mayorista, que no pasan los parámetros nuevos, no cambian."""
    if precio <= 0 or costo_usd <= 0:
        return None
    return calc_margen_detalle(precio, costo_usd, tipo_iva, p, cantidad, cuotas, comision_pct,
                               fixed_fee, bonif_ml)["margen"]
