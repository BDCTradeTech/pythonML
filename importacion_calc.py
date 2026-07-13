"""
importacion_calc.py
Motor de cálculo de importación (Excel "Courier") — fuente única de verdad del
costo de traída, compartida por tabs/importacion.py y tabs/couriers.py.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from db import get_cotizador_param, get_cotizador_tabla, COTIZADOR_DEFAULTS
from tabs.admin import (
    TABLA_POSICION_DEFAULT,
    TABLA_COURIER_DEFAULT,
    TABLA_IVA_VS_EXENTO_DEFAULT,
)


def _f(s: Any) -> float:
    if s is None or s == "":
        return 0.0
    try:
        return float(str(s).replace(",", "."))
    except (TypeError, ValueError):
        return 0.0


def calc_courier_row(
    row: Dict[str, Any],
    params: Dict[str, float],
    posicion_by_name: Dict[str, Dict[str, float]],
    courier_by_origen: Dict[str, Dict[str, float]],
    origen_posicion: Dict[str, str],
    iva_vs_exento_by_courier: Optional[Dict[str, Dict[str, bool]]] = None,
) -> Dict[str, Any]:
    """Aplica la lógica del Excel Courier. row contiene: marca, familia, stock, productos, origen, fob, qty, peso_unitario, extras, trafo, cambio_pa."""
    fob = _f(row.get("fob"))
    qty = _f(row.get("qty"))
    peso_unit = _f(row.get("peso_unitario"))
    origen = str(row.get("origen") or "").strip()
    extras = _f(row.get("extras"))
    cambio_pa_manual = _f(row.get("cambio_pa"))

    dolar_oficial = params.get("dolar_oficial", 1475)
    dolar_blue = params.get("dolar_blue", 1450)
    dolar_despacho = params.get("dolar_despacho", 1475)
    ajuste_ana = params.get("ajuste_valor_ana", 1.01)

    fob_total = fob * qty
    peso_total = qty * peso_unit if qty > 0 and peso_unit > 0 else 0

    posicion_nom = str(row.get("posicion") or "").strip()
    if not posicion_nom and origen:
        posicion_nom = origen_posicion.get(origen, "Cambio PA")
    if not posicion_nom:
        posicion_nom = "Cambio PA"

    posicion = posicion_by_name.get(posicion_nom, {})
    derechos_rate = posicion.get("derechos", 0)
    estad_rate = posicion.get("estadisticas", 0)
    iva_rate = posicion.get("iva", 0.105)

    courier = courier_by_origen.get(origen)
    if not courier:
        for k, v in courier_by_origen.items():
            if origen in k or k in origen:
                courier = v
                break
    if not courier:
        courier = {}

    kg_real = _f(courier.get("kg_real"))
    if kg_real <= 0:
        vk = _f(courier.get("valor_kg", 0))
        dc = max(0.001, _f(courier.get("descuento", 1)))
        kg_real = vk / dc if vk > 0 else 0
    almacenaje = _f(courier.get("almacenaje"))
    seguro = _f(courier.get("seguro"))
    res_3244 = _f(courier.get("res_3244"))
    gas_ope = _f(courier.get("gas_ope"))
    env_dom = _f(courier.get("env_dom"))
    iibb = _f(courier.get("iibb"))

    L = derechos_rate * fob_total * dolar_oficial  # Derechos = tasa × FOB Total (en USD × Dólar)
    M = estad_rate * fob_total * dolar_oficial     # Estadística = tasa × FOB Total
    N = kg_real * peso_total * dolar_oficial  # Flete: dólar oficial
    O_val = almacenaje * peso_total * dolar_oficial
    P = res_3244 * dolar_oficial
    Q = seguro * dolar_oficial
    R = gas_ope * dolar_oficial
    S = env_dom * dolar_oficial  # Env Dom: dólar oficial
    # IVA FOB: (FOB + flete + seguro) × dolar_despacho × iva_rate; flete = Peso(total)×2.5; seguro = (FOB+flete)×0.01; CIF = FOB+flete+seguro
    monto_flete = peso_total * 2.5 if peso_total > 0 else 0  # Peso (columna total), no Peso U
    monto_seguro = (fob_total + monto_flete) * 0.01
    cif = fob_total + monto_flete + monto_seguro
    iva_fob_pesos = iva_rate * cif * dolar_despacho  # IVA FOB usa dólar despacho

    # IVA vs Exento: según Datos → IVA vs Exento, cada courier cobra IVA solo en los campos marcados (Origen = courier)
    def _iva_cobra(v: Any) -> bool:
        return v is True or v == "true" or (isinstance(v, str) and v.lower() == "true") or v == 1

    iva_cfg = None
    if iva_vs_exento_by_courier and origen:
        iva_cfg = iva_vs_exento_by_courier.get(origen)
        if not iva_cfg:
            for k, cfg in iva_vs_exento_by_courier.items():
                if origen in k or k in origen:
                    iva_cfg = cfg
                    break
    if iva_cfg is None:
        iva_cfg = {"almacenaje": True, "res_3244": True, "seguro": True, "gas_ope": True, "env_dom": True, "precio_con_iva": True}

    # Si Precio con IVA: IVA = monto - (monto / 1.21). Si no: IVA = monto × 0.21
    precio_con_iva = _iva_cobra(iva_cfg.get("precio_con_iva", True))

    def _calc_iva(monto: float) -> float:
        if monto <= 0:
            return 0
        if precio_con_iva:
            return monto - (monto / 1.21)
        return monto * 0.21

    iva_almacenaje = _calc_iva(O_val) if _iva_cobra(iva_cfg.get("almacenaje", True)) else 0
    iva_res_3244 = _calc_iva(P) if _iva_cobra(iva_cfg.get("res_3244", True)) else 0
    iva_seguro = _calc_iva(Q) if _iva_cobra(iva_cfg.get("seguro", True)) else 0
    iva_gas_ope = _calc_iva(R) if _iva_cobra(iva_cfg.get("gas_ope", True)) else 0
    iva_env_dom = _calc_iva(S) if _iva_cobra(iva_cfg.get("env_dom", True)) else 0
    total_iva_servicios = iva_almacenaje + iva_res_3244 + iva_seguro + iva_gas_ope + iva_env_dom
    T = (total_iva_servicios + iva_fob_pesos) * ajuste_ana
    subtotal_antes_ajuste = total_iva_servicios + iva_fob_pesos
    U = iibb * R
    V_raw = L + M + N + O_val + P + Q + R + S + T + U
    V = V_raw - total_iva_servicios if precio_con_iva else V_raw  # Si Precio con IVA: restar IVA servicios; si no, no restar
    Z = V + extras + (cambio_pa_manual * dolar_blue) - T  # Excel: Datos!$B$2 = Dólar Blue
    AA = Z / (fob_total * dolar_oficial) if fob_total > 0 else 0
    AC = (fob * (AA + 1)) * dolar_oficial
    AD = AC / dolar_oficial if dolar_oficial > 0 else 0

    venta_ml = _f(row.get("venta_ml"))
    cuotas_3x = float(params.get("cuotas_3x", 0.094))
    cuotas_6x = float(params.get("cuotas_6x", 0.151))
    ml_comision = params.get("ml_comision", 0.15)
    ml_debcre = params.get("ml_debcre", 0.006)
    iva_21 = params.get("iva_21", 0.21)
    ml_envios = params.get("ml_envios", 5823)  # ML - Envíos desde Datos
    ml_iibb_per = params.get("ml_iibb_per", 0.055)

    cuotas3 = venta_ml * (1 + cuotas_3x) if venta_ml > 0 else 0
    cuotas6 = venta_ml * (1 + cuotas_6x) if venta_ml > 0 else 0
    markup = ((venta_ml / AC) - 1) if venta_ml > 0 and AC > 0 else 0
    comi_ml = venta_ml * ml_comision if venta_ml > 0 else 0
    cobrado_ml = venta_ml - comi_ml if venta_ml > 0 else 0
    iva_impor = (T / qty) if venta_ml > 0 and qty > 0 else 0
    iva_meli = comi_ml - (comi_ml / 1.21) if venta_ml > 0 else 0
    iva_venta = venta_ml - (venta_ml / (iva_rate + 1)) if venta_ml > 0 else 0
    iva_total = iva_venta - iva_meli - iva_impor
    deb_cred = venta_ml * ml_debcre if venta_ml > 0 else 0
    iibb_per = venta_ml * ml_iibb_per if venta_ml > 0 else 0
    envio = ml_envios
    costo_vta = (((venta_ml - cobrado_ml) + (iva_total if iva_total > 0 else 0) + deb_cred + iibb_per + envio) / venta_ml) if venta_ml > 0 else 0
    margen = (cobrado_ml - AC - iva_total - deb_cred - iibb_per - envio) if venta_ml > 0 else 0
    margen_vta = (margen / venta_ml) if venta_ml > 0 else 0
    margen_costo = (margen / AC) if AC > 0 else 0

    def _fmt(x: float, decimals: int = 0) -> str:
        s = f"{x:,.{decimals}f}"
        return s.replace(",", "X").replace(".", ",").replace("X", ".")

    traida_pct = AA * 100 if AA else 0

    def _mon(s: str) -> str:
        return "$ " + s if s else ""

    return {
        **row,
        "fob_total": "u$ " + _fmt(fob_total, 2),
        "peso_total": _fmt(peso_total, 2),
        "derechos": _mon(_fmt(L, 0)),
        "estadistica": _mon(_fmt(M, 0)),
        "flete_int": _mon(_fmt(N, 0)),
        "almacenaje": _mon(_fmt(O_val, 0)),
        "res_3244": _mon(_fmt(P, 0)),
        "seguro": _mon(_fmt(Q, 0)),
        "gas_ope": _mon(_fmt(R, 0)),
        "env_dom": _mon(_fmt(S, 0)),
        "iva_lhs": _mon(_fmt(T, 0)),
        "iva_lhs_detalle": {
            "lineas": [
                ["Almacenaje", O_val, iva_almacenaje, _iva_cobra(iva_cfg.get("almacenaje", True))],
                ["Res 3244", P, iva_res_3244, _iva_cobra(iva_cfg.get("res_3244", True))],
                ["Seguro", Q, iva_seguro, _iva_cobra(iva_cfg.get("seguro", True))],
                ["Gastos Operativos", R, iva_gas_ope, _iva_cobra(iva_cfg.get("gas_ope", True))],
                ["Envío a Domicilio", S, iva_env_dom, _iva_cobra(iva_cfg.get("env_dom", True))],
            ],
            "precio_con_iva": precio_con_iva,
            "total_iva_servicios": total_iva_servicios,
            "iva_fob": iva_fob_pesos,
            "iva_fob_calc": {"fob_total": fob_total, "monto_flete": monto_flete, "monto_seguro": monto_seguro, "cif": cif, "iva_rate": iva_rate, "dolar_despacho": dolar_despacho},
            "subtotal": subtotal_antes_ajuste,
            "ajuste": ajuste_ana,
            "total": T,
        },
        "iibb": _mon(_fmt(U, 0)),
        "total_courier": _mon(_fmt(V, 0)),
        "total": _mon(_fmt(Z, 0)),
        "traida_excel": _fmt(traida_pct, 2) + "%",
        "traida_real": _fmt(traida_pct, 2) + "%",
        "traida_pct_raw": traida_pct,
        "costo_pesos": _mon(_fmt(AC, 0)),
        "costo_usd": "u$ " + _fmt(AD, 2),
        "cuotas3": _mon(_fmt(cuotas3, 0)),
        "cuotas6": _mon(_fmt(cuotas6, 0)),
        "markup": _fmt(markup * 100, 1) + "%",
        "cobrado_ml": _mon(_fmt(cobrado_ml, 0)),
        "comi_ml": _mon(_fmt(comi_ml, 0)),
        "iva_impor": _mon(_fmt(iva_impor, 0)),
        "iva_meli": _mon(_fmt(iva_meli, 0)),
        "iva_venta": _mon(_fmt(iva_venta, 0)),
        "iva_total": _mon(_fmt(iva_total, 0)),
        "deb_cred": _mon(_fmt(deb_cred, 0)),
        "iibb_per": _mon(_fmt(iibb_per, 0)),
        "envio": _mon(_fmt(envio, 0)),
        "costo_vta": _fmt(costo_vta * 100, 1) + "%",
        "margen": _mon(_fmt(margen, 0)),
        "margen_vta": _fmt(margen_vta * 100, 1) + "%",
        "margen_costo": _fmt(margen_costo * 100, 1) + "%",
        "margen_raw": margen,
        "margen_vta_raw": margen_vta,
        "margen_costo_raw": margen_costo,
        "margen_detalle": {
            "venta_ml": venta_ml,
            "comi_ml": comi_ml,
            "cobrado_ml": cobrado_ml,
            "costo_pesos": AC,
            "iva_total": iva_total,
            "deb_cred": deb_cred,
            "iibb_per": iibb_per,
            "envio": envio,
            "margen": margen,
        },
    }


def load_calc_context(uid: int) -> Dict[str, Any]:
    """Carga params + tablas de referencia (Tasas por Posición, Costos por Courier,
    IVA vs Exento) desde cotizador_datos para el usuario uid — misma fuente que usa
    Importación (con fallback a los defaults de tabs/admin.py)."""

    def _get(key: str) -> str:
        v = get_cotizador_param(key, uid)
        if v is not None:
            return v
        return COTIZADOR_DEFAULTS.get(key, "")

    def _get_tabla(nombre: str, default: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        r = get_cotizador_tabla(nombre, uid)
        return r if r else default

    def _parse_iva_bool(v: Any) -> bool:
        return v is True or v == "true" or (isinstance(v, str) and v.lower() == "true") or v == 1

    posicion_data = _get_tabla("posicion", TABLA_POSICION_DEFAULT)
    courier_data = _get_tabla("courier", TABLA_COURIER_DEFAULT)
    iva_vs_exento_data = _get_tabla("iva_vs_exento", TABLA_IVA_VS_EXENTO_DEFAULT)

    params = {k: _f(_get(k)) for k in COTIZADOR_DEFAULTS}
    posicion_by_name = {
        str(r.get("posicion", "")).strip(): {c: _f(r.get(c)) for c in ["seguro", "flete", "derechos", "estadisticas", "iva", "despachante", "cambio_pa"]}
        for r in posicion_data if r.get("posicion")
    }
    courier_by_origen = {
        str(r.get("courier", "")).strip(): {c: _f(r.get(c)) for c in ["valor_kg", "descuento", "kg_real", "almacenaje", "seguro", "res_3244", "gas_ope", "env_dom", "iibb", "cambio_pa"]}
        for r in courier_data if r.get("courier")
    }
    origen_posicion = {str(r.get("courier", "")).strip(): str(r.get("posicion", "")).strip() for r in courier_data if r.get("courier")}

    iva_vs_exento_by_courier: Dict[str, Dict[str, bool]] = {}
    for r in iva_vs_exento_data:
        courier_nom = str(r.get("courier", "")).strip()
        if courier_nom:
            iva_vs_exento_by_courier[courier_nom] = {
                "almacenaje": _parse_iva_bool(r.get("almacenaje", False)),
                "res_3244": _parse_iva_bool(r.get("res_3244", False)),
                "seguro": _parse_iva_bool(r.get("seguro", False)),
                "gas_ope": _parse_iva_bool(r.get("gas_ope", False)),
                "env_dom": _parse_iva_bool(r.get("env_dom", False)),
                "precio_con_iva": _parse_iva_bool(r.get("precio_con_iva", True)),
            }

    return {
        "params": params,
        "posicion_data": posicion_data,
        "courier_data": courier_data,
        "posicion_by_name": posicion_by_name,
        "courier_by_origen": courier_by_origen,
        "origen_posicion": origen_posicion,
        "iva_vs_exento_by_courier": iva_vs_exento_by_courier,
    }
