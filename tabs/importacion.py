"""
Fase 3 — tabs/importacion.py
Pestaña Importación: calculadora de costos de importación.
Funciones exportadas: build_tab_importacion
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from nicegui import app, background_tasks, run, ui

from db import get_importacion_filas, save_importacion_filas
from importacion_calc import calc_courier_row, load_calc_context


# ---------------------------------------------------------------------------
# Helper de sesión (mismo patrón que otros tabs; se unificará en auth.py Fase 4)
# ---------------------------------------------------------------------------

def _require_login() -> Optional[Dict[str, Any]]:
    user = app.storage.user.get("user")
    if not user:
        ui.notify("Debes iniciar sesión para continuar", color="negative")
    return user


def build_tab_importacion() -> None:
    """Pestaña Importación: tabla tipo Courier del Excel. Ingresás datos y calcula el resto."""
    user = _require_login()
    if not user:
        return

    uid = user["id"]

    ctx = load_calc_context(uid)
    posicion_data = ctx["posicion_data"]
    courier_data = ctx["courier_data"]

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

        cols_input = ["productos", "origen", "impuestos", "fob", "qty", "peso_unitario", "extras", "cambio_pa", "venta_ml"]
        cols_calc = ["fob_total", "peso_total", "derechos", "estadistica", "flete_int", "almacenaje", "res_3244", "seguro", "gas_ope", "env_dom", "iva_lhs", "iibb", "total_courier", "total", "traida_excel", "costo_pesos", "costo_usd", "cuotas3", "cuotas6", "markup", "cobrado_ml", "comi_ml", "iva_impor", "iva_meli", "iva_venta", "iva_total", "deb_cred", "iibb_per", "envio", "costo_vta", "margen", "margen_vta", "margen_costo"]
        headers_calc = ["FOB Tot", "Peso", "Derech", "Estad", "Flete", "Almac", "Res3244", "Seguro", "GasOp", "EnvDom", "IVA Total", "IIBB", "Courier", "Total", "Traída", "Costo$ s/iva", "Costo u$ s/iva", "3ctas", "6ctas", "MarkUp", "Cobrado", "Comision", "IVAImp", "IVAMel", "IVAVta", "IVA", "Deb/Cred", "IIBB+PER", "Envio", "Cos Vta", "Margen$", "MargVta", "MargCos"]
        headers_input = ["Productos", "Origen", "Impuestos", "FOB", "QTY", "Peso U", "Extras", "Cam.PA", "Venta"]

        opciones_origen = [r.get("courier", "") for r in courier_data if r.get("courier")]
        opciones_impuestos = [r.get("posicion", "") for r in posicion_data if r.get("posicion")]
        cols_ocultas = ["derechos", "estadistica", "flete_int", "almacenaje", "res_3244", "seguro", "gas_ope", "env_dom", "iva_lhs", "iibb", "cuotas3", "cuotas6", "iva_impor", "iva_meli", "iva_venta"]
        cols_input_ocultas = ["extras"]
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
                                        elif c == "extras":
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

        def recalcular() -> None:
            ctx_actual = load_calc_context(uid)
            for i, r_in in enumerate(input_rows_ref):
                row_data = {}
                for c in cols_input:
                    v = r_in[c].value
                    if c in ("fob", "cambio_pa", "venta_ml"):
                        row_data[c] = _parse_imp_prefixed(v) if v else ""
                    else:
                        row_data[c] = v if v is not None else ""
                row_data["posicion"] = str(row_data.get("impuestos", "")).strip() or ctx_actual["origen_posicion"].get(str(row_data.get("origen", "")).strip(), "Cambio PA")
                try:
                    calc = calc_courier_row(row_data, ctx_actual["params"], ctx_actual["posicion_by_name"], ctx_actual["courier_by_origen"], ctx_actual["origen_posicion"], ctx_actual["iva_vs_exento_by_courier"])
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
                row[c] = "0" if c == "extras" else ""
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


