"""
Fase 3 — tabs/datos.py
Pestaña Datos: configuración de parámetros del cotizador y tablas de referencia.
Funciones exportadas: build_tab_datos
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from nicegui import app, run, ui

from db import (
    get_cotizador_param,
    set_cotizador_param,
    delete_cotizador_param,
    get_cotizador_tabla,
    set_cotizador_tabla,
    COTIZADOR_DEFAULTS,
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
# Función exportada
# ---------------------------------------------------------------------------

def build_tab_datos() -> None:
    """Pestaña Datos del cotizador de importaciones. Todos los valores son editables."""
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

    with ui.column().classes("w-full gap-4 p-4"):
        ui.label("Datos del cotizador de importaciones").classes("text-2xl font-semibold")

        with ui.row().classes("w-full gap-4 flex-wrap"):
            # Dolar
            def _fmt_dolar_display(v: str) -> str:
                """Formatea valor numérico con punto para miles."""
                if not v or not str(v).strip():
                    return ""
                try:
                    n = float(str(v).replace(".", "").replace(",", "."))
                    return f"{int(n):,}".replace(",", ".")
                except (ValueError, TypeError):
                    return str(v)

            def _parse_dolar(s: Any) -> str:
                """Parsea valor de input ($ 1.475 o 1475) a string sin formato para guardar."""
                if s is None or s == "":
                    return ""
                raw = str(s).replace("$", "").replace(".", "").replace(",", ".").strip()
                try:
                    n = float(raw)
                    return str(int(n)) if n == int(n) else f"{n:.2f}"
                except (ValueError, TypeError):
                    return str(s).strip()

            with ui.card().classes("p-4 w-fit min-w-[180px]"):
                ui.label("Dólar").classes("text-lg font-semibold mb-3")
                inputs_params: Dict[str, Any] = {}
                for label, key in [
                    ("Oficial", "dolar_oficial"), ("Blue", "dolar_blue"), ("Sistema", "dolar_sistema"), ("Despacho", "dolar_despacho"),
                ]:
                    with ui.row().classes("items-center gap-2 py-0.5"):
                        ui.label(label).classes("min-w-[70px] text-sm")
                        val_raw = _get(key)
                        val_fmt = _fmt_dolar_display(val_raw) if val_raw else ""
                        val_display = f"$ {val_fmt}" if val_fmt else ""
                        inputs_params[key] = ui.input(value=val_display).classes("flex-1 max-w-[100px]").props("dense")

            def _fmt_usd_display(v: str) -> str:
                """Formatea valor numérico: punto para miles, coma para decimales."""
                if not v or not str(v).strip():
                    return ""
                try:
                    s = str(v).strip()
                    n = float(s.replace(",", "."))  # asumir . o , como decimal
                    if n == int(n):
                        return f"{int(n):,}".replace(",", ".")
                    return f"{n:.2f}".rstrip("0").rstrip(".").replace(".", ",")
                except (ValueError, TypeError):
                    return str(v)

            def _parse_usd(s: Any) -> str:
                """Parsea valor con u$ a string para guardar."""
                if s is None or s == "":
                    return ""
                raw = str(s).replace("u$", "").replace("$", "").replace(".", "").replace(",", ".").strip()
                try:
                    n = float(raw)
                    return str(int(n)) if n == int(n) else f"{n:.2f}"
                except (ValueError, TypeError):
                    return str(s).strip()

            # Traida por Kilo
            with ui.card().classes("p-4 w-fit min-w-[140px]"):
                ui.label("Traida por Kilo").classes("text-lg font-semibold mb-3")
                with ui.row().classes("items-center gap-2 py-0.5"):
                    ui.label("Kilo").classes("min-w-[60px] text-sm")
                    val_kilo = _get("kilo")
                    val_kilo_disp = f"u$ {_fmt_usd_display(val_kilo)}" if val_kilo else ""
                    inputs_params["kilo"] = ui.input(value=val_kilo_disp).classes("flex-1 max-w-[80px]").props("dense")

            # Mercadolibre
            with ui.card().classes("p-4 w-fit min-w-[220px]"):
                ui.label("Mercadolibre").classes("text-lg font-semibold mb-3")
                for label, key in [
                    ("ML - Comisión", "ml_comision"), ("Comision Fija (menor)", "ml_comision_fija_menor"),
                    ("ML - Deb/Cre", "ml_debcre"), ("ML - Sirtac", "ml_sirtac"), ("ML - Envíos", "ml_envios"),
                    ("ML - IIBB + PER", "ml_iibb_per"), ("ML - Envíos grat.", "ml_envios_gratuitos"),
                    ("ML - Cobrado", "ml_cobrado"),
                    ("Ganancia Neta sobre Venta", "ml_ganancia_neta_venta"),
                ]:
                    with ui.row().classes("items-center gap-2 py-0.5"):
                        ui.label(label).classes("min-w-[100px] text-sm")
                        inputs_params[key] = ui.input(value=_get(key)).classes("flex-1 max-w-[100px]").props("dense")

            # Cuotas y Promociones
            inputs_cuotas: Dict[str, Any] = {}
            with ui.card().classes("p-4 w-fit min-w-[200px]"):
                ui.label("Cuotas y Promociones").classes("text-lg font-semibold mb-3")
                for label, key in [
                    ("Cuotas 3x", "cuotas_3x"), ("Cuotas 6x", "cuotas_6x"),
                    ("Cuotas 9x", "cuotas_9x"), ("Cuotas 12x", "cuotas_12x"),
                    ("ML 3 cuotas", "ml_3cuotas"), ("ML 6 cuotas", "ml_6cuotas"),
                ]:
                    with ui.row().classes("items-center gap-2 py-0.5"):
                        ui.label(label).classes("min-w-[80px] text-sm")
                        inputs_cuotas[key] = ui.input(value=_get(key)).classes("flex-1 max-w-[100px]").props("dense")

            # Miami
            usd_keys_miami = {"valor_kg_miami", "almacenaje_dias_kg_miami"}
            with ui.card().classes("p-4 w-fit min-w-[220px]"):
                ui.label("Miami").classes("text-lg font-semibold mb-3")
                inputs_miami: Dict[str, Any] = {}
                for label, key in [
                    ("Valor KG Miami", "valor_kg_miami"), ("Almac. Días x Kg", "almacenaje_dias_kg_miami"),
                    ("Seguro Miami", "seguro_miami"),
                ]:
                    with ui.row().classes("items-center gap-2 py-0.5"):
                        ui.label(label).classes("min-w-[120px] text-sm")
                        val_raw = _get(key)
                        val_disp = f"u$ {_fmt_usd_display(val_raw)}" if key in usd_keys_miami and val_raw else (val_raw or "")
                        inputs_miami[key] = ui.input(value=val_disp).classes("flex-1 max-w-[100px]").props("dense")

            # China
            usd_keys_china = {"valor_kg_china", "almacenaje_dias_kg_china"}
            with ui.card().classes("p-4 w-fit min-w-[220px]"):
                ui.label("China").classes("text-lg font-semibold mb-3")
                inputs_china: Dict[str, Any] = {}
                for label, key in [
                    ("Valor KG China", "valor_kg_china"), ("Almac. Días x Kg", "almacenaje_dias_kg_china"),
                    ("Seguro China", "seguro_china"), ("Res 3244", "res_3244"), ("Gastos Operativos", "gastos_operativos"),
                    ("Gastos Origen", "gastos_origen"), ("Envío Domicilio", "envio_domicilio"), ("Ajuste valor ANA", "ajuste_valor_ana"),
                ]:
                    with ui.row().classes("items-center gap-2 py-0.5"):
                        ui.label(label).classes("min-w-[120px] text-sm")
                        val_raw = _get(key)
                        val_disp = f"u$ {_fmt_usd_display(val_raw)}" if key in usd_keys_china and val_raw else (val_raw or "")
                        inputs_china[key] = ui.input(value=val_disp).classes("flex-1 max-w-[100px]").props("dense")

        def guardar_params() -> None:
            dolar_keys = {"dolar_oficial", "dolar_blue", "dolar_sistema", "dolar_despacho", "ml_comision_fija_menor"}
            usd_keys = {"kilo", "valor_kg_miami", "almacenaje_dias_kg_miami", "valor_kg_china", "almacenaje_dias_kg_china"}
            for key, inp in {**inputs_params, **inputs_cuotas, **inputs_miami, **inputs_china}.items():
                val = str(inp.value or "").strip()
                if key in dolar_keys:
                    val = _parse_dolar(val)
                elif key in usd_keys:
                    val = _parse_usd(val)
                set_cotizador_param(key, val, uid)
            ui.notify("Parámetros guardados", color="positive")

        ui.button("Guardar parámetros", on_click=guardar_params, color="primary").classes("mb-2")

        # Eliminar tablas obsoletas de la BD si existían
        for k in ["tabla_origen", "tabla_cambio_pa", "tabla_derechos", "tabla_estadisticas"]:
            delete_cotizador_param(k, uid)

        # Tablas editables (headers = encabezados de columnas)
        tabla_trafo_gramos_data = list(_get_tabla("trafo_gramos", TABLA_TRAFO_GRAMOS_DEFAULT))
        tabla_posicion_data = list(_get_tabla("posicion", TABLA_POSICION_DEFAULT))
        tabla_envios_data = list(_get_tabla("envios_ml", TABLA_ENVIOS_ML_DEFAULT))
        tabla_courier_data = list(_get_tabla("courier", TABLA_COURIER_DEFAULT))

        def _parse_num(s: Any) -> float:
            if s is None or s == "": return 0.0
            try:
                return float(str(s).replace(",", "."))
            except (TypeError, ValueError):
                return 0.0

        def _fmt_pesos_display(val: Any) -> str:
            """Formatea valor en pesos: $ y punto para miles."""
            if val is None or str(val).strip() == "":
                return ""
            try:
                n = float(str(val).replace(".", "").replace(",", "."))
                return f"$ {int(n):,}".replace(",", ".") if n == int(n) else f"$ {n:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            except (ValueError, TypeError):
                return str(val)

        def _parse_pesos_fmt(s: Any) -> str:
            """Parsea valor con $ y puntos a string para guardar."""
            if s is None or s == "":
                return ""
            raw = str(s).replace("$", "").replace(".", "").replace(",", ".").strip()
            try:
                n = float(raw)
                return str(int(n)) if n == int(n) else f"{n:.2f}"
            except (ValueError, TypeError):
                return str(s).strip()

        def _tabla_editable(nombre: str, cols: List[str], headers: List[str], data: List[Dict[str, Any]], titulo: str, compact: bool = False, col_widths: Optional[List[str]] = None, card_ancho: Optional[str] = None, computed: Optional[Dict[str, Any]] = None, computed_deps: Optional[Dict[str, List[str]]] = None, ordenable: bool = True, col_formato: Optional[Dict[str, str]] = None) -> None:
            card_classes = "p-4"
            if card_ancho:
                card_classes += f" {card_ancho}"
            elif compact:
                card_classes += " flex-1 min-w-[140px] max-w-[220px]"
            else:
                card_classes += " w-full"
            with ui.card().classes(card_classes):
                ui.label(titulo).classes("text-lg font-semibold mb-3")
                cont = ui.column().classes("w-full gap-2")
                edit_rows: List[Dict[str, Any]] = []

                def repintar() -> None:
                    cont.clear()
                    edit_rows.clear()
                    with cont:
                        with ui.element("table").classes("w-full border-collapse text-sm").style("table-layout: fixed;"):
                            with ui.element("thead"):
                                with ui.element("tr").classes("bg-blue-100 dark:bg-blue-900"):
                                    for j, h in enumerate(headers):
                                        th = ui.element("th").classes("font-semibold px-1.5 py-0.5 text-left border border-gray-300")
                                        if col_widths and j < len(col_widths):
                                            th.style(col_widths[j])
                                        with th:
                                            ui.label(h)
                                    if ordenable:
                                        with ui.element("th").classes("font-semibold px-0.5 py-0.5 text-center border border-gray-300 text-xs").style("min-width: 48px; width: 48px;"):
                                            ui.label("Ordenar")
                                    with ui.element("th").classes("font-semibold px-0.5 py-0.5 text-center border border-gray-300 text-xs").style("min-width: 52px; width: 52px;"):
                                        ui.label("Borrar")
                            with ui.element("tbody"):
                                for idx, row in enumerate(data):
                                    rinputs: Dict[str, Any] = {}
                                    with ui.element("tr"):
                                        for col in cols:
                                            val = str(row.get(col, ""))
                                            if col_formato and col in col_formato:
                                                val = _fmt_pesos_display(val) if val else ""
                                            with ui.element("td").classes("p-0.5 border border-gray-200"):
                                                if computed and col in computed:
                                                    disp = computed[col](row) if callable(computed[col]) else str(row.get(col, ""))
                                                    if col_formato and col in col_formato:
                                                        disp = _fmt_pesos_display(disp) if disp else ""
                                                    lbl = ui.label(disp).classes("text-xs")
                                                    rinputs[col] = lbl
                                                else:
                                                    inp = ui.input(value=val).classes("w-full border-0 text-xs").props("dense")
                                                    rinputs[col] = inp
                                        # Actualizar labels calculados cuando cambian las dependencias
                                        if computed and computed_deps:
                                            def make_updater(comp_col: str, lbl_ref: Any) -> None:
                                                def upd() -> None:
                                                    row = {}
                                                    for c in cols:
                                                        if c in (computed or {}):
                                                            continue
                                                        raw = str(rinputs[c].value or "")
                                                        if col_formato and c in col_formato:
                                                            raw = _parse_pesos_fmt(raw)
                                                        row[c] = raw
                                                    disp = computed[comp_col](row)
                                                    if col_formato and comp_col in col_formato:
                                                        disp = _fmt_pesos_display(disp) if disp else ""
                                                    lbl_ref.text = disp
                                                return upd
                                            for comp_col, deps in computed_deps.items():
                                                if comp_col in rinputs:
                                                    upd = make_updater(comp_col, rinputs[comp_col])
                                                    for d in deps:
                                                        if d in rinputs and hasattr(rinputs[d], "on_value_change"):
                                                            rinputs[d].on_value_change(upd)
                                        if ordenable:
                                            with ui.element("td").classes("p-0.5 border border-gray-200 text-center").style("min-width: 48px; width: 48px;"):
                                                def subir(i: int) -> None:
                                                    if i > 0:
                                                        data[i], data[i - 1] = data[i - 1], data[i]
                                                        repintar()
                                                def bajar(i: int) -> None:
                                                    if i < len(data) - 1:
                                                        data[i], data[i + 1] = data[i + 1], data[i]
                                                        repintar()
                                                with ui.row().classes("gap-0 justify-center"):
                                                    ui.button("▲", on_click=lambda i=idx: subir(i)).classes("min-w-0 px-0.5 text-xs").props("flat dense no-caps")
                                                    ui.button("▼", on_click=lambda i=idx: bajar(i)).classes("min-w-0 px-0.5 text-xs").props("flat dense no-caps")
                                        with ui.element("td").classes("p-0.5 border border-gray-200 text-center").style("min-width: 52px; width: 52px;"):
                                            def borrar_fila(i: int) -> None:
                                                if 0 <= i < len(data):
                                                    data.pop(i)
                                                    repintar()
                                            ui.button("×", on_click=lambda i=idx: borrar_fila(i)).classes("text-red-600 font-bold text-sm min-w-0 px-1").props("flat dense no-caps")
                                    edit_rows.append(rinputs)

                repintar()

                def agregar_fila() -> None:
                    data.append({c: "" for c in cols})
                    repintar()

                def guardar_tabla() -> None:
                    new_data = []
                    for rinputs in edit_rows:
                        row: Dict[str, Any] = {}
                        for c in cols:
                            if computed and c in computed:
                                continue
                            raw = str(rinputs[c].value or "")
                            if col_formato and c in col_formato:
                                raw = _parse_pesos_fmt(raw)
                            row[c] = raw
                        if computed:
                            for c in computed:
                                row[c] = computed[c](row)
                        new_data.append(row)
                    set_cotizador_tabla(nombre, new_data, uid)
                    data.clear()
                    data.extend(new_data)
                    repintar()
                    ui.notify(f"Tabla {titulo} guardada", color="positive")

                with ui.row().classes("gap-2"):
                    ui.button("Agregar Fila", on_click=agregar_fila, color="primary")
                    ui.button("Guardar tabla", on_click=guardar_tabla, color="secondary")

        with ui.row().classes("w-full gap-4 flex-wrap"):
            _tabla_editable("trafo_gramos", ["trafo", "gramos"], ["Trafo", "Gramos"], tabla_trafo_gramos_data, "Trafo y Gramos", card_ancho="w-fit")
            _tabla_editable("posicion", ["posicion", "seguro", "flete", "derechos", "estadisticas", "iva", "despachante", "cambio_pa"],
                ["Posicion", "Seguro", "Flete", "Derechos", "Estadisticas", "IVA", "Despachante", "Cambio PA"],
                tabla_posicion_data, "Tasas por Posición", card_ancho="w-fit")
            _tabla_editable("envios_ml", ["envio", "importe", "porc_10", "costo"],
                ["Envios ML", "Importe", "0,10", "Costo"], tabla_envios_data, "Costos envío MercadoLibre",
                computed={"costo": lambda r: str(int(_parse_num(r.get("importe")) + _parse_num(r.get("porc_10"))))},
                computed_deps={"costo": ["importe", "porc_10"]}, card_ancho="w-fit",
                col_formato={"importe": "$", "porc_10": "$", "costo": "$"})
            _tabla_editable("courier", ["courier", "valor_kg", "descuento", "kg_real", "almacenaje", "seguro", "res_3244", "gas_ope", "env_dom", "iibb", "cif"],
                ["Courier", "Valor KG", "Descuento", "KG Real", "Almacenaje", "Seguro", "Res 3244", "Gas Ope", "Env Dom", "IIBB", "CIF"],
                tabla_courier_data, "Costos por Courier",
                computed={"kg_real": lambda r: f"{_parse_num(r.get('valor_kg')) / max(0.001, _parse_num(r.get('descuento'))):.2f}"},
                computed_deps={"kg_real": ["valor_kg", "descuento"]}, card_ancho="w-fit")

        # Tabla IVA vs Exento (debajo de Costos por Courier)
        tabla_iva_vs_exento_data = list(_get_tabla("iva_vs_exento", TABLA_IVA_VS_EXENTO_DEFAULT))
        iva_vs_exento_headers = ["Courier", "Almacenaje", "Res 3244", "Seguro", "Gastos Operativos", "Envio a Domicilio", "Precio con IVA"]

        def _parse_bool(v: Any) -> bool:
            if v is True or v == "true" or str(v).lower() == "true" or v == 1:
                return True
            return False

        with ui.card().classes("p-4 w-fit"):
            ui.label("IVA vs Exento").classes("text-lg font-semibold mb-3")
            iva_vs_exento_cont = ui.column().classes("w-full gap-2")
            iva_vs_exento_edit_rows: List[Dict[str, Any]] = []

            def repintar_iva() -> None:
                iva_vs_exento_cont.clear()
                iva_vs_exento_edit_rows.clear()
                with iva_vs_exento_cont:
                    with ui.element("table").classes("w-full border-collapse text-sm").style("table-layout: fixed;"):
                        with ui.element("thead"):
                            with ui.element("tr").classes("bg-blue-100 dark:bg-blue-900"):
                                for h in iva_vs_exento_headers:
                                    with ui.element("th").classes("font-semibold px-1.5 py-0.5 text-center border border-gray-300"):
                                        ui.label(h)
                                with ui.element("th").classes("font-semibold px-0.5 py-0.5 text-center border border-gray-300 text-xs").style("min-width: 52px; width: 52px;"):
                                    ui.label("Borrar")
                        with ui.element("tbody"):
                            for idx, row in enumerate(tabla_iva_vs_exento_data):
                                rinputs: Dict[str, Any] = {}
                                with ui.element("tr"):
                                    with ui.element("td").classes("p-0.5 border border-gray-200"):
                                        inp_courier = ui.input(value=str(row.get("courier", ""))).classes("w-full border-0 text-xs min-w-[100px]").props("dense")
                                        rinputs["courier"] = inp_courier
                                    for col in ["almacenaje", "res_3244", "seguro", "gas_ope", "env_dom", "precio_con_iva"]:
                                        with ui.element("td").classes("p-0.5 border border-gray-200 text-center"):
                                            default_val = True if col == "precio_con_iva" else False
                                            chk = ui.checkbox(value=_parse_bool(row.get(col, default_val)))
                                            rinputs[col] = chk
                                    with ui.element("td").classes("p-0.5 border border-gray-200 text-center").style("min-width: 52px; width: 52px;"):
                                        def borrar_iva(i: int) -> None:
                                            if 0 <= i < len(tabla_iva_vs_exento_data):
                                                for j, rinputs in enumerate(iva_vs_exento_edit_rows):
                                                    if j < len(tabla_iva_vs_exento_data):
                                                        tabla_iva_vs_exento_data[j] = {
                                                            "courier": str(rinputs["courier"].value or "").strip(),
                                                            "almacenaje": bool(rinputs["almacenaje"].value),
                                                            "res_3244": bool(rinputs["res_3244"].value),
                                                            "seguro": bool(rinputs["seguro"].value),
                                                            "gas_ope": bool(rinputs["gas_ope"].value),
                                                            "env_dom": bool(rinputs["env_dom"].value),
                                                            "precio_con_iva": bool(rinputs["precio_con_iva"].value),
                                                        }
                                                tabla_iva_vs_exento_data.pop(i)
                                                repintar_iva()
                                        ui.button("×", on_click=lambda i=idx: borrar_iva(i)).classes("text-red-600 font-bold text-sm min-w-0 px-1").props("flat dense no-caps")
                                iva_vs_exento_edit_rows.append(rinputs)

            repintar_iva()

            def agregar_fila_iva() -> None:
                # Sincronizar valores actuales de los inputs antes de repintar para no perder datos
                for i, rinputs in enumerate(iva_vs_exento_edit_rows):
                    if i < len(tabla_iva_vs_exento_data):
                        tabla_iva_vs_exento_data[i] = {
                            "courier": str(rinputs["courier"].value or "").strip(),
                            "almacenaje": bool(rinputs["almacenaje"].value),
                            "res_3244": bool(rinputs["res_3244"].value),
                            "seguro": bool(rinputs["seguro"].value),
                            "gas_ope": bool(rinputs["gas_ope"].value),
                            "env_dom": bool(rinputs["env_dom"].value),
                            "precio_con_iva": bool(rinputs["precio_con_iva"].value),
                        }
                tabla_iva_vs_exento_data.append({"courier": "", "almacenaje": False, "res_3244": False, "seguro": False, "gas_ope": False, "env_dom": False, "precio_con_iva": True})
                repintar_iva()

            def guardar_tabla_iva() -> None:
                new_data = []
                for rinputs in iva_vs_exento_edit_rows:
                    row: Dict[str, Any] = {
                        "courier": str(rinputs["courier"].value or "").strip(),
                        "almacenaje": bool(rinputs["almacenaje"].value),
                        "res_3244": bool(rinputs["res_3244"].value),
                        "seguro": bool(rinputs["seguro"].value),
                        "gas_ope": bool(rinputs["gas_ope"].value),
                        "env_dom": bool(rinputs["env_dom"].value),
                        "precio_con_iva": bool(rinputs["precio_con_iva"].value),
                    }
                    new_data.append(row)
                set_cotizador_tabla("iva_vs_exento", new_data, uid)
                tabla_iva_vs_exento_data.clear()
                tabla_iva_vs_exento_data.extend(new_data)
                repintar_iva()
                ui.notify("Tabla IVA vs Exento guardada", color="positive")

            with ui.row().classes("gap-2"):
                ui.button("Agregar Fila", on_click=agregar_fila_iva, color="primary")
                ui.button("Guardar tabla", on_click=guardar_tabla_iva, color="secondary")


# ==========================
# CALLBACK OAUTH (ruta HTTP directa para evitar 404 con NiceGUI)
# ==========================


