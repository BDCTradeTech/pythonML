"""
margen.py
Cálculo de margen por unidad de una publicación ML (contado, sin financiación de
cuotas). Vive acá -- y no en tabs/dashboard.py -- para que el cron (salud_audit.py)
pueda usarlo sin importar desde tabs/ (que arrastra nicegui). tabs/dashboard.py
re-exporta estos nombres, así que los imports existentes
(`from tabs.dashboard import _calc_margen_prod, _load_params_prod`) siguen
funcionando sin cambios.
"""
from __future__ import annotations

from typing import Any, Optional

from db import get_cotizador_param


def _pr(s: Any, d: float = 0.0) -> float:
    if s is None or str(s).strip() == "":
        return d
    try:
        v = float(str(s).strip().replace(",", "."))
        return v if v <= 1.5 else v / 100.0
    except (ValueError, TypeError):
        return d


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
    }


def _calc_margen_prod(precio: float, costo_usd: float, tipo_iva: float, p: dict, cantidad: int = 1) -> Optional[float]:
    """Margen por unidad al precio unitario `precio`. `cantidad` (default 1) prorratea el
    envío: el envío es POR ENVÍO, no por unidad, así que un tier de N unidades lo reparte
    (envío / N por unidad). Con cantidad=1 el resultado es idéntico al de siempre, por eso
    dashboard.py y descuentos.py, que no lo pasan, no cambian. El umbral de envío
    se evalúa sobre el TOTAL de la orden (precio × cantidad): ML da envío gratis cuando la compra
    supera el umbral, no cuando lo supera cada unidad (verificado con GET /items/{id}/shipping_options
    ?quantity=N, 2026-09-30)."""
    if precio <= 0 or costo_usd <= 0:
        return None
    cantidad = max(int(cantidad or 1), 1)
    comision    = precio * p["ml_comision"]
    cobrado     = precio - comision
    deb_cred    = precio * p["ml_debcre"]
    iibb        = precio * p["ml_iibb_per"]
    iva_meli    = comision * 0.21 / 1.21
    iva_impor   = 0.09 * costo_usd * p["dolar_oficial"]
    iva_total   = precio * tipo_iva / (1 + tipo_iva) - iva_meli - iva_impor
    envio       = 0.0 if precio * cantidad < p["ml_envios_gratuitos"] else p["ml_envios_val"] / cantidad
    costo_pesos = costo_usd * p["dolar_oficial"]
    return cobrado - costo_pesos - iva_total - iibb - deb_cred - envio
