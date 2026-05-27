"""
Fase 3 — tabs/importacion.py
Pestaña Importación: calculadora de costos de importación.
Funciones exportadas: build_tab_importacion
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from nicegui import app, background_tasks, run, ui

from db import (
    get_cotizador_param,
    get_cotizador_tabla,
    get_importacion_filas,
    save_importacion_filas,
    COTIZADOR_DEFAULTS,
)
from tabs.admin import (
    TABLA_ORIGEN_DEFAULT,
    TABLA_POSICION_DEFAULT,
    TABLA_COURIER_DEFAULT,
    TABLA_IVA_VS_EXENTO_DEFAULT,
)


# ---------------------------------------------------------------------------
# Helper de sesión (mismo patrón que otros tabs; se unificará en auth.py Fase 4)
# ---------------------------------------------------------------------------

def _require_login() -> Optional[Dict[str, Any]]:
    user = app.storage.user.get("user")
    if not user:
        ui.notify("Debes iniciar sesión para continuar", color="negative")
    return user


# ---------------------------------------------------------------------------
# Helper privado de importacion
# ---------------------------------------------------------------------------

def _calc_courier_row(
    row: Dict[str, Any],
    params: Dict[str, float],
    posicion_by_name: Dict[str, Dict[str, float]],
    courier_by_origen: Dict[str, Dict[str, float]],
    origen_posicion: Dict[str, str],
    iva_vs_exento_by_courier: Optional[Dict[str, Dict[str, bool]]] = None,
) -> Dict[str, Any]:
    """Aplica la lógica del Excel Courier. row contiene: marca, familia, stock, productos, origen, fob, qty, peso_unitario, extras, trafo, cambio_pa."""
    def _f(s: Any) -> float:
        if s is None or s == "": return 0.0
        try:
            return float(str(s).replace(",", "."))
        except (TypeError, ValueError):
            return 0.0

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
    ml_3cuotas = params.get("ml_3cuotas", 1.12149)
    ml_6cuotas = params.get("ml_6cuotas", 1.21067)
    ml_comision = params.get("ml_comision", 0.15)
    ml_debcre = params.get("ml_debcre", 0.006)
    iva_21 = params.get("iva_21", 0.21)
    ml_envios = params.get("ml_envios", 5823)  # ML - Envíos desde Datos
    ml_iibb_per = params.get("ml_iibb_per", 0.055)

    cuotas3 = venta_ml * ml_3cuotas if venta_ml > 0 else 0
    cuotas6 = venta_ml * ml_6cuotas if venta_ml > 0 else 0
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


def build_tab_importacion() -> None:
    """Pestaña Importación: tabla tipo Courier del Excel. Ingresás datos y calcula el resto."""
    user = _require_login()
    if not user:
        return

    uid = user["id"]

    def _get(key: str) -> str:
        v = get_cotizador_param(key, uid)
        if v is not None:
            return v
        return COTIZADOR_DEFAULTS.get(key, "")

    def _get_tabla(nombre: str, default: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        r = get_cotizador_tabla(nombre, uid)
        return r if r else default

    def _parse_float(s: Any) -> float:
        if s is None or s == "": return 0.0
        try:
            return float(str(s).replace(",", "."))
        except (TypeError, ValueError):
            return 0.0

    origen_data = _get_tabla("origen", TABLA_ORIGEN_DEFAULT)
    posicion_data = _get_tabla("posicion", TABLA_POSICION_DEFAULT)
    courier_data = _get_tabla("courier", TABLA_COURIER_DEFAULT)

    params = {k: _parse_float(_get(k)) for k in COTIZADOR_DEFAULTS}
    posicion_by_name = {str(r.get("posicion", "")).strip(): {c: _parse_float(r.get(c)) for c in ["seguro", "flete", "derechos", "estadisticas", "iva", "despachante", "cambio_pa"]} for r in posicion_data if r.get("posicion")}
    courier_by_origen = {str(r.get("courier", "")).strip(): {c: _parse_float(r.get(c)) for c in ["valor_kg", "descuento", "kg_real", "almacenaje", "seguro", "res_3244", "gas_ope", "env_dom", "iibb"]} for r in courier_data if r.get("courier")}

    origen_posicion = {str(r.get("origen", "")).strip(): str(r.get("posicion", "")).strip() for r in origen_data if r.get("origen")}

    # Cargar filas guardadas o empezar con una vacía
    importacion_rows: List[Dict[str, Any]] = get_importacion_filas(user["id"])
    if not importacion_rows:
        importacion_rows = []

    sort_col_importacion: List[Optional[str]] = [None]
    sort_asc_importacion: List[bool] = [True]

    def _parse_sort_val(v: Any, col: str) -> Any:
        """Valor para ordenar: numérico si aplica, sino string."""
        if v is None or v == "":
            return 0.0 if col in ["fob", "qty", "peso_unitario", "extras", "cambio_pa", "venta_ml"] else ""
        s = str(v).replace("$", "").replace(".", "").replace(",", ".").strip()
        try:
            return float(s)
        except (ValueError, TypeError):
            return str(v).lower()

    def toggle_sort_importacion(col: str) -> None:
        if sort_col_importacion[0] == col:
            sort_asc_importacion[0] = not sort_asc_importacion[0]
        else:
            sort_col_importacion[0] = col
            sort_asc_importacion[0] = True
        sync_inputs_to_rows()
        rev = not sort_asc_importacion[0]
        importacion_rows.sort(key=lambda r: _parse_sort_val(r.get(col), col), reverse=rev)
        repintar()

    with ui.column().classes("w-full gap-2 p-2 flex flex-col"):
        ui.label("Importación - Cotizador Courier").classes("text-xl font-semibold")

        cols_input = ["productos", "origen", "impuestos", "fob", "qty", "peso_unitario", "extras", "trafo", "cambio_pa", "venta_ml"]
        cols_calc = ["fob_total", "peso_total", "derechos", "estadistica", "flete_int", "almacenaje", "res_3244", "seguro", "gas_ope", "env_dom", "iva_lhs", "iibb", "total_courier", "total", "traida_excel", "costo_pesos", "costo_usd", "cuotas3", "cuotas6", "markup", "cobrado_ml", "comi_ml", "iva_impor", "iva_meli", "iva_venta", "iva_total", "deb_cred", "iibb_per", "envio", "costo_vta", "margen", "margen_vta", "margen_costo"]
        headers_calc = ["FOB Tot", "Peso", "Derech", "Estad", "Flete", "Almac", "Res3244", "Seguro", "GasOp", "EnvDom", "IVA Total", "IIBB", "Courier", "Total", "Traída", "Costo$ s/iva", "Costo u$ s/iva", "3ctas", "6ctas", "MarkUp", "Cobrado", "Comision", "IVAImp", "IVAMel", "IVAVta", "IVA", "Deb/Cred", "IIBB+PER", "Envio", "Cos Vta", "Margen$", "MargVta", "MargCos"]
        headers_input = ["Productos", "Origen", "Impuestos", "FOB", "QTY", "Peso U", "Extras", "Trafo", "Cam.PA", "Venta"]

        opciones_origen = [r.get("origen", "") for r in origen_data if r.get("origen")]
        opciones_impuestos = [r.get("posicion", "") for r in posicion_data if r.get("posicion")]
        cols_ocultas = ["derechos", "estadistica", "flete_int", "almacenaje", "res_3244", "seguro", "gas_ope", "env_dom", "iva_lhs", "iibb", "cuotas3", "cuotas6", "iva_impor", "iva_meli", "iva_venta"]
        cols_input_ocultas = ["extras", "trafo"]
        vista_completa = [False]

        table_container = ui.column().classes("w-full overflow-auto")
        input_rows_ref: List[Dict[str, Any]] = []

        def col_visible(col: str) -> bool:
            if col in cols_input_ocultas:
                return vista_completa[0]
            if col in cols_input:
                return True
            return vista_completa[0] or col not in cols_ocultas

        def _fmt_imp_usd(val: Any, decimals: int = 2) -> str:
            """Formato u$ con punto miles. decimals=2 para FOB, 0 para Cam.PA."""
            if val is None or str(val).strip() == "": return ""
            try:
                s = str(val).replace("u$", "").replace("$", "").strip()
                if "," in s:
                    s = s.replace(".", "").replace(",", ".")
                n = float(s) if s else 0
                fmt = f"{n:,.{decimals}f}" if decimals else f"{int(n):,}"
                return "u$ " + fmt.replace(",", "X").replace(".", ",").replace("X", ".")
            except (TypeError, ValueError):
                return str(val)

        def _fmt_imp_pesos(val: Any, decimals: int = 0) -> str:
            """Formato $ con punto miles, sin decimales para Venta."""
            if val is None or str(val).strip() == "": return ""
            try:
                s = str(val).replace("u$", "").replace("$", "").strip()
                if "," in s:
                    s = s.replace(".", "").replace(",", ".")
                n = float(s) if s else 0
                fmt = f"{int(n):,}"
                return "$ " + fmt.replace(",", ".")
            except (TypeError, ValueError):
                return str(val)

        def _parse_imp_prefixed(v: Any) -> str:
            """Parsea 'u$ 1.234,56', '$ 64.990' o '$ 10.000' a '1234.56' o '64990'."""
            if v is None or v == "": return ""
            s = str(v).replace("u$", "").replace("$", "").strip()
            if not s: return ""
            if "," in s:
                s = s.replace(".", "").replace(",", ".")
            elif "." in s:
                parts = s.split(".")
                if len(parts) == 2 and len(parts[1]) == 3:
                    s = s.replace(".", "")
                elif len(parts) > 2:
                    s = s.replace(".", "")
            try:
                n = float(s)
                return str(int(n)) if n == int(n) else f"{n:.2f}"
            except (TypeError, ValueError):
                return str(v).strip()

        def aplicar_estilo_fob_ml(inp: Any, es_fob: bool = False) -> None:
            """Actualiza negrita y rojo según si el input tiene valor (al cargar/editar)."""
            v = (inp.value or "").strip()
            base = "min-w-[52px] text-right" if es_fob else "min-w-[60px] text-right"
            if v:
                inp.classes(replace=base + " font-bold text-red-600")
                inp.style("font-weight: bold; color: rgb(220, 38, 38);")
            else:
                inp.classes(replace=base)
                inp.style("font-weight: normal; color: inherit;")

        def repintar() -> None:
            table_container.clear()
            input_rows_ref.clear()
            all_cols = cols_input + cols_calc
            all_headers = headers_input + headers_calc
            with table_container:
                with ui.element("table").classes("w-full border-collapse text-xs").style("table-layout: auto; white-space: nowrap;"):
                    with ui.element("thead"):
                        with ui.element("tr"):
                            for j, (c, h) in enumerate(zip(all_cols, all_headers)):
                                if j < 10:
                                    bg = "bg-sky-100 dark:bg-sky-800"
                                elif j < 27:
                                    bg = "bg-teal-100 dark:bg-teal-800"
                                elif j < 40:
                                    bg = "bg-sky-100 dark:bg-sky-800"
                                else:
                                    bg = "bg-teal-100 dark:bg-teal-800"
                                th_cls = f"font-semibold px-1 py-1 text-center border border-gray-300 whitespace-nowrap text-xs cursor-pointer {bg}"
                                if not col_visible(c):
                                    th_cls += " hidden"
                                th = ui.element("th").classes(th_cls)
                                th.on("click", lambda col=c: toggle_sort_importacion(col))
                                with th:
                                    ui.label(h)
                            with ui.element("th").classes("font-semibold px-0.5 py-1 text-center border border-gray-300 text-xs bg-slate-100 dark:bg-slate-700").style("min-width: 48px;"):
                                ui.label("Ordenar")
                            with ui.element("th").classes("font-semibold px-1 py-1 border border-gray-300 bg-slate-100 dark:bg-slate-700").style("min-width: 40px;"):
                                ui.label("×")
                    with ui.element("tbody"):
                        for i, r in enumerate(importacion_rows):
                            r_in: Dict[str, Any] = {}
                            with ui.element("tr"):
                                for c in cols_input:
                                    raw_val = r.get(c, "")
                                    if c == "fob":
                                        val = _fmt_imp_usd(raw_val, decimals=2)
                                    elif c == "cambio_pa":
                                        val = _fmt_imp_usd(raw_val, decimals=0)
                                    elif c == "venta_ml":
                                        val = _fmt_imp_pesos(raw_val)
                                    else:
                                        val = str(raw_val)
                                    td_cls = "p-0.5 border border-gray-200 min-w-0"
                                    if c in ("fob", "cambio_pa", "venta_ml"):
                                        td_cls += " text-right"
                                    elif c in ("qty", "peso_unitario"):
                                        td_cls += " text-center"
                                    if not col_visible(c):
                                        td_cls += " hidden"
                                    with ui.element("td").classes(td_cls):
                                        if c == "origen":
                                            opts = {o: o for o in opciones_origen if o}
                                            inp = ui.select(opts, value=val or (opciones_origen[0] if opciones_origen else "")).classes("min-w-[120px]").props("dense outlined")
                                        elif c == "impuestos":
                                            opts = {p: p for p in opciones_impuestos if p}
                                            inp = ui.select(opts, value=val or (opciones_impuestos[0] if opciones_impuestos else "")).classes("min-w-[130px]").props("dense outlined")
                                        elif c == "productos":
                                            inp = ui.input(value=val).classes("min-w-[130px]").props("dense")
                                        elif c == "fob":
                                            inp_cls = "min-w-[52px] text-right"
                                            if val:
                                                inp_cls += " font-bold text-red-600"
                                            inp = ui.input(value=val).classes(inp_cls).props("dense")
                                            inp.on_value_change(lambda inp_ref=inp: aplicar_estilo_fob_ml(inp_ref, es_fob=True))
                                            aplicar_estilo_fob_ml(inp, es_fob=True)
                                        elif c in ("qty", "peso_unitario"):
                                            inp = ui.input(value=val).classes("min-w-[40px]").props("dense").style("text-align: center")
                                        elif c == "cambio_pa":
                                            inp = ui.input(value=val).classes("min-w-[52px] text-right").props("dense")
                                        elif c in ("extras", "trafo"):
                                            inp = ui.input(value=val).classes("min-w-[55px]").props("dense")
                                        elif c == "venta_ml":
                                            inp_cls = "min-w-[60px] text-right"
                                            if val:
                                                inp_cls += " font-bold text-red-600"
                                            inp = ui.input(value=val).classes(inp_cls).props("dense")
                                            inp.on_value_change(lambda inp_ref=inp: aplicar_estilo_fob_ml(inp_ref, es_fob=False))
                                            aplicar_estilo_fob_ml(inp, es_fob=False)
                                        else:
                                            inp = ui.input(value=val).classes("min-w-[80px]").props("dense")
                                        r_in[c] = inp
                                for c in cols_calc:
                                    txt = str(r.get(c, ""))
                                    td_classes = "px-0.5 py-0.5 border border-gray-200 bg-gray-50 text-right whitespace-nowrap text-xs"
                                    if not col_visible(c):
                                        td_classes += " hidden"
                                    if c == "costo_pesos" or c == "costo_usd":
                                        td_classes += " font-bold text-blue-600"
                                    elif c in ("margen", "margen_vta", "margen_costo"):
                                        td_classes += " font-bold"
                                        raw = r.get(f"{c}_raw")
                                        if raw is not None:
                                            td_classes += " text-green-600" if raw >= 0 else " text-red-600"
                                    with ui.element("td").classes(td_classes):
                                        if c == "iva_lhs":
                                            detalle = r.get("iva_lhs_detalle")

                                            def _abrir_popup_iva(det: Any) -> None:
                                                d = ui.dialog().props("persistent")
                                                with d:
                                                    with ui.card().classes("p-4 min-w-[360px]"):
                                                        ui.label("Cálculo IVA Total").classes("text-lg font-semibold mb-3")
                                                        if det:
                                                            def _fmt_mon(x: float) -> str:
                                                                s = f"{x:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")
                                                                return f"$ {s}"
                                                            def _fmt_usd(x: float) -> str:
                                                                s = f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                                                                return f"u$ {s}"
                                                            precio_con_iva_popup = det.get("precio_con_iva", True)
                                                            for linea in det.get("lineas", []):
                                                                concepto, monto_ivai, iva, aplica = linea[0], linea[1], linea[2], linea[3]
                                                                if aplica:
                                                                    if precio_con_iva_popup:
                                                                        ui.label(f"{concepto}: {_fmt_mon(monto_ivai)} IVA incl. → IVA = monto - (monto/1,21) = {_fmt_mon(iva)}").classes("text-sm")
                                                                    else:
                                                                        ui.label(f"{concepto}: {_fmt_mon(monto_ivai)} sin IVA → IVA = monto × 0,21 = {_fmt_mon(iva)}").classes("text-sm")
                                                                else:
                                                                    ui.label(f"{concepto}: Exento").classes("text-sm text-gray-500")
                                                            tot_serv = det.get("total_iva_servicios", 0)
                                                            ui.label(f"Total IVA servicios: {_fmt_mon(tot_serv)}").classes("text-sm font-medium mt-1")
                                                            ui.element("hr").classes("my-2 border-gray-300")
                                                            iva_fob_calc = det.get("iva_fob_calc") or {}
                                                            if iva_fob_calc:
                                                                fob = iva_fob_calc.get("fob_total", 0)
                                                                fl = iva_fob_calc.get("monto_flete", 0)
                                                                seg = iva_fob_calc.get("monto_seguro", 0)
                                                                cif_val = iva_fob_calc.get("cif", 0)
                                                                rate = iva_fob_calc.get("iva_rate", 0)
                                                                dol = iva_fob_calc.get("dolar_despacho", 0)
                                                                ui.label("IVA FOB:").classes("text-sm font-medium")
                                                                ui.label(f"  CIF = FOB + flete + seguro = {_fmt_usd(fob)} + {_fmt_usd(fl)} + {_fmt_usd(seg)} = {_fmt_usd(cif_val)}").classes("text-sm")
                                                                dol_str = f"{dol:,.0f}".replace(",", ".")
                                                                with ui.row().classes("gap-1"):
                                                                    ui.label("IVA FOB").classes("text-sm font-bold")
                                                                    ui.label(f" = {_fmt_usd(cif_val)} × {rate} × {dol_str} = ").classes("text-sm")
                                                                    ui.label(_fmt_mon(det.get('iva_fob', 0))).classes("text-sm font-bold")
                                                            else:
                                                                ui.label(f"IVA FOB: {_fmt_mon(det.get('iva_fob', 0))}").classes("text-sm")
                                                            with ui.row().classes("gap-2 mt-1"):
                                                                ui.label("Total IVA: IVA Total Servicios + IVA FOB =").classes("text-sm")
                                                                ui.label(_fmt_mon(det.get("total", 0))).classes("text-sm font-bold text-blue-600")
                                                        else:
                                                            ui.label("Recalculá para ver el detalle del IVA Total.").classes("text-sm text-gray-600")
                                                        ui.button("Cerrar", on_click=d.close).classes("mt-3")
                                                d.open()

                                            btn = ui.button(txt).classes("cursor-pointer underline hover:bg-gray-200 -m-1 px-1").props("flat dense no-caps no-wrap")
                                            btn.on_click(lambda det=detalle: _abrir_popup_iva(det))
                                        elif c == "margen":
                                            detalle_margen = r.get("margen_detalle")

                                            def _abrir_popup_margen(det: Any) -> None:
                                                d = ui.dialog().props("persistent")
                                                with d:
                                                    with ui.card().classes("p-4 min-w-[320px]"):
                                                        ui.label("Cálculo Margen").classes("text-lg font-semibold mb-3")
                                                        if det:
                                                            def _fmt_mon(x: float) -> str:
                                                                s = f"{x:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")
                                                                return f"$ {s}"
                                                            venta = det.get("venta_ml", 0)
                                                            comi = det.get("comi_ml", 0)
                                                            cob = det.get("cobrado_ml", 0)
                                                            costo = det.get("costo_pesos", 0)
                                                            iva = det.get("iva_total", 0)
                                                            deb = det.get("deb_cred", 0)
                                                            iibb = det.get("iibb_per", 0)
                                                            env = det.get("envio", 0)
                                                            marg = det.get("margen", 0)
                                                            with ui.row().classes("gap-2"):
                                                                ui.label("Venta:").classes("text-sm text-black")
                                                                ui.label(_fmt_mon(venta)).classes("text-sm text-blue-600")
                                                            with ui.row().classes("gap-2"):
                                                                ui.label("Comisiones:").classes("text-sm text-black")
                                                                ui.label(_fmt_mon(comi)).classes("text-sm text-negative")
                                                            with ui.row().classes("gap-2"):
                                                                ui.label("Cobrado:").classes("text-sm text-black")
                                                                ui.label(_fmt_mon(cob)).classes("text-sm text-blue-600")
                                                            with ui.row().classes("gap-2"):
                                                                ui.label("Costo sin iva:").classes("text-sm text-black")
                                                                ui.label(_fmt_mon(costo)).classes("text-sm text-negative")
                                                            with ui.row().classes("gap-2"):
                                                                ui.label("IVA:").classes("text-sm text-black")
                                                                ui.label(_fmt_mon(iva)).classes("text-sm text-negative")
                                                            with ui.row().classes("gap-2"):
                                                                ui.label("Deb/Cred:").classes("text-sm text-black")
                                                                ui.label(_fmt_mon(deb)).classes("text-sm text-negative")
                                                            with ui.row().classes("gap-2"):
                                                                ui.label("IIBB:").classes("text-sm text-black")
                                                                ui.label(_fmt_mon(iibb)).classes("text-sm text-negative")
                                                            with ui.row().classes("gap-2"):
                                                                ui.label("Envío:").classes("text-sm text-black")
                                                                ui.label(_fmt_mon(env)).classes("text-sm text-negative")
                                                            marg_cls = "text-positive" if marg >= 0 else "text-negative"
                                                            with ui.row().classes("gap-2 mt-2"):
                                                                ui.label("Margen:").classes("text-sm text-black font-bold")
                                                                ui.label(_fmt_mon(marg)).classes(f"text-sm font-bold {marg_cls}")
                                                        else:
                                                            ui.label("Recalculá para ver el detalle del margen.").classes("text-sm text-gray-600")
                                                        ui.button("Cerrar", on_click=d.close).classes("mt-3")
                                                d.open()

                                            marg_raw = r.get("margen_raw")
                                            btn_cls = "cursor-pointer underline hover:bg-gray-200 -m-1 px-1"
                                            if marg_raw is not None:
                                                btn_cls += " text-green-600" if marg_raw >= 0 else " text-red-600"
                                            btn_m = ui.button(txt).classes(btn_cls).props("flat dense no-caps no-wrap")
                                            btn_m.on_click(lambda det=detalle_margen: _abrir_popup_margen(det))
                                        else:
                                            ui.label(txt)
                                with ui.element("td").classes("p-0.5 border border-gray-200 text-center").style("min-width: 48px;"):
                                    def subir(idx: int) -> None:
                                        if idx > 0:
                                            sync_inputs_to_rows()
                                            importacion_rows[idx], importacion_rows[idx - 1] = importacion_rows[idx - 1], importacion_rows[idx]
                                            repintar()
                                    def bajar(idx: int) -> None:
                                        if idx < len(importacion_rows) - 1:
                                            sync_inputs_to_rows()
                                            importacion_rows[idx], importacion_rows[idx + 1] = importacion_rows[idx + 1], importacion_rows[idx]
                                            repintar()
                                    with ui.row().classes("gap-0 justify-center"):
                                        ui.button("▲", on_click=lambda idx=i: subir(idx)).classes("min-w-0 px-0.5 text-xs").props("flat dense no-caps")
                                        ui.button("▼", on_click=lambda idx=i: bajar(idx)).classes("min-w-0 px-0.5 text-xs").props("flat dense no-caps")
                                with ui.element("td").classes("p-0.5 border border-gray-200 text-center").style("min-width: 40px;"):
                                    def borrar(idx: int) -> None:
                                        if 0 <= idx < len(importacion_rows):
                                            importacion_rows.pop(idx)
                                            repintar()
                                    ui.button("×", on_click=lambda idx=i: borrar(idx)).classes("text-red-600 font-bold text-lg min-w-0 px-1").props("flat dense no-caps")
                            input_rows_ref.append(r_in)

        def _parse_iva_bool(v: Any) -> bool:
            return v is True or v == "true" or (isinstance(v, str) and v.lower() == "true") or v == 1

        def recalcular() -> None:
            params_actual = {k: _parse_float(_get(k)) for k in COTIZADOR_DEFAULTS}
            posicion_actual = _get_tabla("posicion", TABLA_POSICION_DEFAULT)
            courier_actual = _get_tabla("courier", TABLA_COURIER_DEFAULT)
            origen_actual = _get_tabla("origen", TABLA_ORIGEN_DEFAULT)
            iva_vs_exento_actual = _get_tabla("iva_vs_exento", TABLA_IVA_VS_EXENTO_DEFAULT)
            posicion_by_name_actual = {str(r.get("posicion", "")).strip(): {c: _parse_float(r.get(c)) for c in ["seguro", "flete", "derechos", "estadisticas", "iva", "despachante", "cambio_pa"]} for r in posicion_actual if r.get("posicion")}
            courier_by_origen_actual = {str(r.get("courier", "")).strip(): {c: _parse_float(r.get(c)) for c in ["valor_kg", "descuento", "kg_real", "almacenaje", "seguro", "res_3244", "gas_ope", "env_dom", "iibb"]} for r in courier_actual if r.get("courier")}
            origen_posicion_actual = {str(r.get("origen", "")).strip(): str(r.get("posicion", "")).strip() for r in origen_actual if r.get("origen")}
            iva_vs_exento_by_courier_actual = {}
            for r in iva_vs_exento_actual:
                courier_nom = str(r.get("courier", "")).strip()
                if courier_nom:
                    iva_vs_exento_by_courier_actual[courier_nom] = {
                        "almacenaje": _parse_iva_bool(r.get("almacenaje", False)),
                        "res_3244": _parse_iva_bool(r.get("res_3244", False)),
                        "seguro": _parse_iva_bool(r.get("seguro", False)),
                        "gas_ope": _parse_iva_bool(r.get("gas_ope", False)),
                        "env_dom": _parse_iva_bool(r.get("env_dom", False)),
                        "precio_con_iva": _parse_iva_bool(r.get("precio_con_iva", True)),
                    }
            for i, r_in in enumerate(input_rows_ref):
                row_data = {}
                for c in cols_input:
                    v = r_in[c].value
                    if c in ("fob", "cambio_pa", "venta_ml"):
                        row_data[c] = _parse_imp_prefixed(v) if v else ""
                    else:
                        row_data[c] = v if v is not None else ""
                row_data["posicion"] = str(row_data.get("impuestos", "")).strip() or origen_posicion_actual.get(str(row_data.get("origen", "")).strip(), "Cambio PA")
                try:
                    calc = _calc_courier_row(row_data, params_actual, posicion_by_name_actual, courier_by_origen_actual, origen_posicion_actual, iva_vs_exento_by_courier_actual)
                    for k, v in calc.items():
                        if i < len(importacion_rows):
                            importacion_rows[i][k] = v
                except Exception as e:
                    if i < len(importacion_rows):
                        importacion_rows[i]["error"] = str(e)
            repintar()

        def add_row() -> None:
            row = {}
            for c in cols_input + cols_calc:
                row[c] = "0" if c in ("extras", "trafo") else ""
            importacion_rows.append(row)
            recalcular()

        def sync_inputs_to_rows() -> None:
            """Copia los valores actuales de los inputs a importacion_rows antes de repintar."""
            for i, r_in in enumerate(input_rows_ref):
                if i < len(importacion_rows):
                    for c in cols_input:
                        if c in r_in:
                            v = r_in[c].value
                            if c in ("fob", "cambio_pa", "venta_ml"):
                                importacion_rows[i][c] = _parse_imp_prefixed(v)
                            else:
                                importacion_rows[i][c] = str(v) if v is not None else ""

        def toggle_vista() -> None:
            sync_inputs_to_rows()
            vista_completa[0] = not vista_completa[0]
            btn_vista.text = "Mínimo" if vista_completa[0] else "Completo"
            repintar()

        def guardar_tabla_importacion() -> None:
            sync_inputs_to_rows()
            user = _require_login()
            if not user:
                ui.notify("Debe iniciar sesión", color="negative")
                return
            try:
                save_importacion_filas(user["id"], importacion_rows)
                ui.notify(f"Guardadas {len(importacion_rows)} filas", color="positive")
            except Exception as e:
                ui.notify(f"Error al guardar: {e}", color="negative")

        if not importacion_rows:
            add_row()
        else:
            repintar()
            recalcular()

        with ui.row().classes("gap-2 order-first"):
            ui.button("Calcular", on_click=recalcular, color="secondary")
            ui.button("Agregar Fila", on_click=add_row, color="primary")
            btn_vista = ui.button("Completo", on_click=toggle_vista, color="secondary")
            ui.button("Guardar Tabla", on_click=guardar_tabla_importacion, color="secondary")


