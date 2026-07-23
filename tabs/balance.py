"""
Fase 3 — tabs/balance.py
Pestaña Balance: Gastos (editable), Ingresos (ventas/ganancias) y Resultados.
Funciones exportadas: build_tab_balance
"""
from __future__ import annotations

import calendar
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from nicegui import app, background_tasks, run, ui

from db import get_cotizador_param, get_cotizador_tabla, set_cotizador_tabla, COTIZADOR_DEFAULTS
from ml_api import get_ml_access_token, ml_get_orders_incremental, ml_get_user_id, ml_get_user_profile


# ---------------------------------------------------------------------------
# Helper de sesión (mismo patrón que otros tabs; se unificará en auth.py Fase 4)
# ---------------------------------------------------------------------------

def _require_login() -> Optional[Dict[str, Any]]:
    user = app.storage.user.get("user")
    if not user:
        ui.notify("Debes iniciar sesión para continuar", color="negative")
    return user


# ---------------------------------------------------------------------------
# Helper privado de balance
# ---------------------------------------------------------------------------

def _compute_ingresos_from_orders(orders_data: Dict[str, Any], user_id: int, periodo: str = "mes_actual") -> Dict[str, float]:
    """Calcula ventas y ganancias desde órdenes ML. periodo: mes_actual, mes_anterior o historico."""
    hoy = datetime.now().date()
    primer_dia = hoy.replace(day=1)
    ultimo_mes = primer_dia - timedelta(days=1)
    primer_dia_anterior = ultimo_mes.replace(day=1)
    raw = orders_data.get("results") or orders_data.get("orders") or orders_data.get("elements") or []
    ventas_mes_actual_monto = 0.0
    for o in raw:
        if not isinstance(o, dict):
            continue
        dt_str = o.get("date_created") or o.get("date_closed") or o.get("date_last_updated") or ""
        if not dt_str:
            continue
        try:
            dt = datetime.strptime(dt_str[:10], "%Y-%m-%d").date()
        except Exception:
            continue
        if periodo == "mes_actual":
            if not (primer_dia <= dt <= hoy):
                continue
        elif periodo == "mes_anterior":
            if not (primer_dia_anterior <= dt <= ultimo_mes):
                continue
        # historico: sin filtro de fecha
        elif periodo != "historico":
            continue
        amt = o.get("total_amount") or o.get("paid_amount")
        if amt is None and o.get("payments"):
            p = o["payments"][0] if isinstance(o["payments"], list) else {}
            amt = p.get("total_amount") or p.get("total_paid_amount") or p.get("transaction_amount")
        try:
            ventas_mes_actual_monto += float(amt or 0)
        except (TypeError, ValueError):
            pass
    if periodo == "mes_actual":
        dias_transcurridos = (hoy - primer_dia).days + 1
        dias_del_mes = calendar.monthrange(hoy.year, hoy.month)[1]
    elif periodo == "mes_anterior":
        dias_transcurridos = (ultimo_mes - primer_dia_anterior).days + 1
        dias_del_mes = dias_transcurridos
    else:
        dias_transcurridos = 1
        dias_del_mes = 1
    venta_diaria = ventas_mes_actual_monto / dias_transcurridos if dias_transcurridos > 0 else 0
    venta_estimada_mes = venta_diaria * dias_del_mes if dias_transcurridos > 0 else ventas_mes_actual_monto
    try:
        m = get_cotizador_param("ml_ganancia_neta_venta", user_id) or COTIZADOR_DEFAULTS.get("ml_ganancia_neta_venta", "0.1000")
        margen_val = float(str(m).replace(",", ".").strip())
    except (ValueError, TypeError):
        margen_val = 0.1
    ganancia_a_fecha = ventas_mes_actual_monto * margen_val
    ganancia_estimada_mes = venta_estimada_mes * margen_val
    return {
        "venta_a_fecha": ventas_mes_actual_monto,
        "venta_estimada_mes": venta_estimada_mes,
        "ganancia_a_fecha": ganancia_a_fecha,
        "ganancia_estimada_mes": ganancia_estimada_mes,
    }


def build_tab_balance(container) -> None:
    """Pestaña Balance: Gastos (editable), Ingresos (ventas/ganancias) y Resultados."""
    user = _require_login()
    if not user:
        return

    uid = user["id"]
    access_token = get_ml_access_token(uid)
    gastos_data: List[Dict[str, Any]] = list(get_cotizador_tabla("gastos", uid))
    sort_col_gastos: List[Optional[str]] = [None]
    sort_asc_gastos: List[bool] = [True]

    def _parse_importe(s: Any) -> float:
        if s is None or s == "":
            return 0.0
        try:
            # Quitar puntos de miles, coma decimal
            raw = str(s).replace(".", "").replace(",", ".").strip()
            return float(raw) if raw else 0.0
        except (ValueError, TypeError):
            return 0.0

    def _fmt_importe_display(val: Any) -> str:
        """Formatea importe para mostrar: 1.234.567 (punto miles, coma decimal)"""
        n = _parse_importe(val)
        if n == 0 and (val is None or str(val).strip() == ""):
            return ""
        if abs(n - int(n)) < 0.01:
            return f"{int(n):,}".replace(",", ".")
        entera = int(n)
        dec = round((n - entera) * 100)
        return f"{entera:,}".replace(",", ".") + f",{dec:02d}"

    with container:
        with ui.column().classes("w-full p-8 items-center gap-4"):
            ui.spinner(size="xl")
            ui.label("Cargando Balance...").classes("text-xl text-gray-700")
    ingresos_ref: Dict[str, Any] = {"data": None}

    async def _cargar_y_pintar() -> None:
        orders_data: Dict[str, Any] = {}
        if access_token:
            try:
                profile = await run.io_bound(ml_get_user_profile, access_token)
                seller_id = (profile or {}).get("id") or await run.io_bound(ml_get_user_id, access_token)
                if seller_id:
                    orders_data = await run.io_bound(ml_get_orders_incremental, access_token, str(seller_id), uid)
                ingresos_ref["data"] = _compute_ingresos_from_orders(orders_data, uid, "mes_actual")
            except Exception:
                ingresos_ref["data"] = None
        else:
            ingresos_ref["data"] = None
        _pintar_contenido()

    def _pintar_contenido() -> None:
        container.clear()
        with container:
            # Alto acotado a la ventana (estimacion conservadora del chrome compartido de
            # arriba -- nav + tabs) para que la pagina no scrollee. El header_card (KPIs)
            # mantiene su alto natural (flex:none); la fila principal y la tarjeta de Gastos
            # son flex:1, asi absorben lo que sobre debajo del header_card sin depender de
            # adivinar su altura exacta (que varia si el KPI bar hace wrap).
            with ui.element("div").classes("w-full").style(
                "height:calc(100vh - 210px);min-height:0;overflow:hidden;"
                "display:flex;flex-direction:column"
            ):
                header_card = ui.column().classes("w-full mb-2 p-4").style("flex:none")
                with ui.row().classes("w-full gap-4 p-4 items-start flex-wrap").style("flex:1;min-height:0"):
                    # Columna izquierda: Gastos (tabla + botones)
                    # height:100% (no solo align-self:stretch) porque la fila padre tiene
                    # flex-wrap -- medido en vivo con DevTools: en un flex-row con wrap,
                    # align-self:stretch estira los items HASTA el alto de su linea, pero el
                    # alto de la linea se calcula primero a partir del contenido natural de
                    # los items sin estirar -- como esta columna es la mas alta (por la tabla
                    # larga), termina siendo ella misma la que define la linea, y stretch no
                    # hace nada (868px medidos, igual a su contenido sin recortar). height:100%
                    # SI resuelve contra el alto ya acotado de la fila (flex:1;min-height:0, un
                    # valor definido gracias al calc(100vh - Npx) del contenedor de arriba) y
                    # fuerza el achique real -- confirmado en vivo: bajo de 868px a 537px y
                    # cont paso a scrollHeight(724) > clientHeight(393), con barra visible.
                    with ui.column().classes("gap-2").style(
                        "max-width:500px;flex:1;min-height:0;height:100%;"
                        "display:flex;flex-direction:column"
                    ):
                        with ui.card().classes("w-full p-4").style(
                            "flex:1;min-height:0;display:flex;flex-direction:column;overflow:hidden"
                        ):
                            ui.label("Gastos").classes("text-lg font-semibold mb-2").style("flex:none")
                            cont = ui.column().classes("w-full gap-2").style("flex:1;min-height:0;overflow-y:auto")
                            edit_rows_ref: List[Dict[str, Any]] = []
                            gastos_buttons_row = ui.row().classes("gap-2 mt-2").style("flex:none")
                    # Columna derecha: Parámetros, Ingresos y Resultados Netos
                    with ui.row().classes("gap-4 flex-wrap"):
                        parametros_card = ui.column().classes("gap-1")
                        ingresos_card = ui.column().classes("gap-1")
                        resultados_card = ui.column().classes("gap-1")
        def toggle_sort(col: str) -> None:
            if sort_col_gastos[0] == col:
                sort_asc_gastos[0] = not sort_asc_gastos[0]
            else:
                sort_col_gastos[0] = col
                sort_asc_gastos[0] = True
            repintar()

        row_to_inputs: List[tuple] = []  # (row, rinputs) para mapear al guardar

        def sync_inputs_to_rows() -> None:
            """Copia valores de inputs a row dicts antes de repintar."""
            for row, rinputs in row_to_inputs:
                row["gasto"] = str(rinputs["gasto"].value or "")
                row["importe"] = str(rinputs["importe"].value or "")

        def _pintar_header() -> None:
            sync_inputs_to_rows()
            total_importes = sum(_parse_importe(r.get("importe")) for r in gastos_data)
            inc = ingresos_ref["data"]
            venta_fecha = inc.get("venta_a_fecha", 0) if inc else 0
            ganancia_fecha = inc.get("ganancia_a_fecha", 0) if inc else 0
            resultado_fecha = ganancia_fecha - total_importes
            facturacion_est = inc.get("venta_estimada_mes", 0) if inc else 0
            ganancia_bruta_est = inc.get("ganancia_estimada_mes", 0) if inc else 0
            ganancia_neta_est = ganancia_bruta_est - total_importes
            dolar_str = get_cotizador_param("dolar_oficial", uid) or COTIZADOR_DEFAULTS.get("dolar_oficial", "1000")
            dolar_oficial = float(str(dolar_str).replace(",", ".").strip()) if dolar_str else 0
            if dolar_oficial <= 0:
                dolar_oficial = 1000
            venta_fecha_usd = venta_fecha / dolar_oficial
            ganancia_fecha_usd = ganancia_fecha / dolar_oficial
            total_importes_usd = total_importes / dolar_oficial
            resultado_fecha_usd = resultado_fecha / dolar_oficial
            facturacion_est_usd = facturacion_est / dolar_oficial
            ganancia_bruta_est_usd = ganancia_bruta_est / dolar_oficial
            ganancia_neta_est_usd = ganancia_neta_est / dolar_oficial
            header_card.clear()
            with header_card:
                with ui.card().classes("w-full p-4 bg-grey-2"):
                    with ui.row().classes("w-full gap-6 flex-wrap"):
                        # 1. Datos Actuales Pesos
                        with ui.column().classes("gap-0 border-r border-gray-300 pr-4"):
                            ui.label("Datos Actuales (pesos)").classes("text-xs text-gray-600 font-semibold mb-1")
                            with ui.row().classes("gap-4 flex-wrap"):
                                with ui.column().classes("gap-0"):
                                    ui.label("Venta a la fecha").classes("text-xs text-gray-600")
                                    ui.label(f"$ {venta_fecha:,.0f}".replace(",", ".")).classes("text-base font-bold text-primary")
                                with ui.column().classes("gap-0"):
                                    ui.label("Ganancia bruta a la fecha").classes("text-xs text-gray-600")
                                    ui.label(f"$ {ganancia_fecha:,.0f}".replace(",", ".")).classes("text-base font-bold text-primary")
                                with ui.column().classes("gap-0"):
                                    ui.label("Total Gastos").classes("text-xs text-gray-600")
                                    ui.label(f"$ {total_importes:,.0f}".replace(",", ".")).classes("text-base font-bold text-negative")
                                with ui.column().classes("gap-0"):
                                    ui.label("Resultado neto a la fecha").classes("text-xs text-gray-600")
                                    ui.label(f"$ {resultado_fecha:,.0f}".replace(",", ".")).classes("text-base font-bold " + ("text-positive" if resultado_fecha >= 0 else "text-negative"))
                        # 3. Datos Estimados Pesos
                        with ui.column().classes("gap-0 border-r border-gray-300 pr-4"):
                            ui.label("Datos Estimados (pesos)").classes("text-xs text-gray-600 font-semibold mb-1")
                            with ui.row().classes("gap-4 flex-wrap"):
                                with ui.column().classes("gap-0"):
                                    ui.label("Venta estimada").classes("text-xs text-gray-600")
                                    ui.label(f"$ {facturacion_est:,.0f}".replace(",", ".")).classes("text-base font-bold text-primary")
                                with ui.column().classes("gap-0"):
                                    ui.label("Ganancia bruta estimada").classes("text-xs text-gray-600")
                                    ui.label(f"$ {ganancia_bruta_est:,.0f}".replace(",", ".")).classes("text-base font-bold text-primary")
                                with ui.column().classes("gap-0"):
                                    ui.label("Total Gastos").classes("text-xs text-gray-600")
                                    ui.label(f"$ {total_importes:,.0f}".replace(",", ".")).classes("text-base font-bold text-negative")
                                with ui.column().classes("gap-0"):
                                    ui.label("Resultado neto estimado").classes("text-xs text-gray-600")
                                    ui.label(f"$ {ganancia_neta_est:,.0f}".replace(",", ".")).classes("text-base font-bold " + ("text-positive" if ganancia_neta_est >= 0 else "text-negative"))
                        # 4. Datos Estimados en Dólares
                        with ui.column().classes("gap-0"):
                            ui.label("Datos Estimados (dólares)").classes("text-xs text-gray-600 font-semibold mb-1")
                            with ui.row().classes("gap-4 flex-wrap"):
                                with ui.column().classes("gap-0"):
                                    ui.label("Venta estimada").classes("text-xs text-gray-600")
                                    ui.label(f"u$s {facturacion_est_usd:,.0f}".replace(",", ".")).classes("text-base font-bold text-primary")
                                with ui.column().classes("gap-0"):
                                    ui.label("Ganancia bruta estimada").classes("text-xs text-gray-600")
                                    ui.label(f"u$s {ganancia_bruta_est_usd:,.0f}".replace(",", ".")).classes("text-base font-bold text-primary")
                                with ui.column().classes("gap-0"):
                                    ui.label("Total Gastos").classes("text-xs text-gray-600")
                                    ui.label(f"u$s {total_importes_usd:,.0f}".replace(",", ".")).classes("text-base font-bold text-negative")
                                with ui.column().classes("gap-0"):
                                    ui.label("Resultado neto estimado").classes("text-xs text-gray-600")
                                    ui.label(f"u$s {ganancia_neta_est_usd:,.0f}".replace(",", ".")).classes("text-base font-bold " + ("text-positive" if ganancia_neta_est_usd >= 0 else "text-negative"))
            _pintar_resultados()

        def _pintar_ingresos() -> None:
            ingresos_card.clear()
            with ingresos_card:
                with ui.card().classes("w-full p-4 border-l-4 border-l-emerald-500"):
                    ui.label("Ingresos").classes("text-lg font-semibold text-emerald-700 mb-2")
                    inc = ingresos_ref["data"]
                    if inc is None:
                        ui.label("Conectá MercadoLibre para ver ingresos.").classes("text-gray-500")
                    else:
                        with ui.element("table").classes("w-full border-collapse text-sm"):
                            with ui.element("tbody"):
                                for label, key in [
                                    ("Venta a la fecha", "venta_a_fecha"),
                                    ("Venta estimada del mes", "venta_estimada_mes"),
                                ]:
                                    sin_negrita = key in ("venta_a_fecha", "ganancia_a_fecha")
                                    con_negrita_valor = key in ("venta_estimada_mes", "ganancia_estimada_mes")
                                    with ui.element("tr").classes("border-t border-gray-200"):
                                        with ui.element("td").classes("px-2 py-1 " + ("font-normal" if sin_negrita else "font-medium")):
                                            ui.label(label)
                                        with ui.element("td").classes("px-2 py-1 text-right " + ("font-semibold" if con_negrita_valor else "font-normal" if sin_negrita else "")):
                                            ui.label(f"$ {inc.get(key, 0):,.0f}".replace(",", "."))

        def _pintar_parametros() -> None:
            parametros_card.clear()
            with parametros_card:
                with ui.card().classes("w-full p-4 border-l-4 border-l-amber-500"):
                    ui.label("Parámetros").classes("text-lg font-semibold text-amber-700 mb-2")
                    dolar_str = get_cotizador_param("dolar_oficial", uid) or COTIZADOR_DEFAULTS.get("dolar_oficial", "1000")
                    gan_str   = get_cotizador_param("ml_ganancia_neta_venta", uid) or COTIZADOR_DEFAULTS.get("ml_ganancia_neta_venta", "0.1000")
                    try:
                        dolar_val = float(str(dolar_str).replace(",", ".").strip())
                    except (ValueError, TypeError):
                        dolar_val = 0.0
                    try:
                        gan_val = float(str(gan_str).replace(",", ".").strip())
                    except (ValueError, TypeError):
                        gan_val = 0.0
                    with ui.element("table").classes("w-full border-collapse text-sm"):
                        with ui.element("tbody"):
                            with ui.element("tr").classes("border-t border-gray-200"):
                                with ui.element("td").classes("px-2 py-1 font-normal"):
                                    ui.label("Dólar Oficial")
                                with ui.element("td").classes("px-2 py-1 text-right font-normal"):
                                    ui.label(f"$ {dolar_val:,.0f}".replace(",", "."))
                            with ui.element("tr").classes("border-t border-gray-200"):
                                with ui.element("td").classes("px-2 py-1 font-normal"):
                                    ui.label("Gan. neta %")
                                with ui.element("td").classes("px-2 py-1 text-right font-normal"):
                                    ui.label(f"{gan_val * 100:.2f}%".replace(".", ","))

        def _pintar_resultados() -> None:
            sync_inputs_to_rows()
            total_gastos = sum(_parse_importe(r.get("importe")) for r in gastos_data)
            inc = ingresos_ref["data"]
            resultados_card.clear()
            with resultados_card:
                with ui.card().classes("w-full p-4 border-l-4 border-l-blue-500"):
                    ui.label("Resultados Netos").classes("text-lg font-semibold text-blue-700 mb-2")
                    if inc is None:
                        ui.label("Conectá MercadoLibre para ver resultados.").classes("text-gray-500")
                    else:
                        res_a_fecha = inc.get("ganancia_a_fecha", 0) - total_gastos
                        res_estimado = inc.get("ganancia_estimada_mes", 0) - total_gastos
                        with ui.element("table").classes("w-full border-collapse text-sm"):
                            with ui.element("tbody"):
                                with ui.element("tr").classes("border-t border-gray-200"):
                                    with ui.element("td").classes("px-2 py-1 font-normal"):
                                        ui.label("Resultado neto a la fecha")
                                    with ui.element("td").classes("px-2 py-1 text-right font-normal"):
                                        ui.label(f"$ {res_a_fecha:,.0f}".replace(",", "."))
                                with ui.element("tr").classes("border-t border-gray-200"):
                                    with ui.element("td").classes("px-2 py-1 font-medium"):
                                        ui.label("Resultado neto estimado del mes")
                                    with ui.element("td").classes("px-2 py-1 text-right font-semibold"):
                                        ui.label(f"$ {res_estimado:,.0f}".replace(",", "."))

        def repintar() -> None:
            sync_inputs_to_rows()
            cont.clear()
            edit_rows_ref.clear()
            row_to_inputs.clear()
            datos = list(gastos_data)
            if sort_col_gastos[0] == "gasto":
                rev = not sort_asc_gastos[0]
                datos.sort(key=lambda r: str(r.get("gasto", "")).lower(), reverse=rev)
            elif sort_col_gastos[0] == "importe":
                rev = not sort_asc_gastos[0]
                datos.sort(key=lambda r: _parse_importe(r.get("importe")), reverse=rev)
            with cont:
                with ui.element("table").classes("w-full border-collapse text-sm").style("table-layout: fixed;"):
                    # sticky se pone en cada <th> (no en el <tr>) con el fondo pintado ahi
                    # mismo -- mismo patron que tabs/stock.py. sticky en <tr> es poco fiable
                    # entre navegadores, y si el fondo azul solo estuviera en el <tr> (que no
                    # es sticky), las filas de abajo se transparentarian a traves del <th> al
                    # scrollear por debajo del header.
                    _th_sticky = "position:sticky;top:0;z-index:2;background:#1976D2;color:#fff;"
                    with ui.element("thead"):
                        with ui.element("tr").classes("font-semibold"):
                            with ui.element("th").classes("px-2 py-2 border text-center cursor-pointer").style(_th_sticky + "width: 60%;").on("click", lambda: toggle_sort("gasto")):
                                ui.label("Gasto")
                            with ui.element("th").classes("px-2 py-2 border text-center cursor-pointer").style(_th_sticky + "width: 30%;").on("click", lambda: toggle_sort("importe")):
                                ui.label("Importe")
                            with ui.element("th").classes("px-1 py-2 border text-center").style(_th_sticky + "width: 70px;"):
                                ui.label("Ordenar")
                            with ui.element("th").classes("px-1 py-2 border text-center").style(_th_sticky + "width: 50px;"):
                                ui.label("Borrar")
                    with ui.element("tbody"):
                        for idx, row in enumerate(datos):
                            rinputs: Dict[str, Any] = {}
                            row_idx_in_data = gastos_data.index(row) if row in gastos_data else idx
                            imp_raw = str(row.get("importe", ""))
                            imp_display = _fmt_importe_display(imp_raw) if imp_raw else ""
                            with ui.element("tr").classes("border-t border-gray-200 hover:bg-gray-50"):
                                with ui.element("td").classes("px-2 py-1 border-b border-gray-100").style("width: 60%;"):
                                    inp_gasto = ui.input(value=str(row.get("gasto", ""))).classes("w-full border-0").props("dense")
                                    rinputs["gasto"] = inp_gasto
                                with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-right").style("width: 30%;"):
                                    with ui.row().classes("w-full items-center gap-1 justify-end text-right"):
                                        ui.label("$").classes("text-gray-600 text-sm")
                                        inp_imp = ui.input(value=imp_display).classes("flex-1 min-w-0 border-0").props("dense").style("text-align: right;")
                                        rinputs["importe"] = inp_imp
                                with ui.element("td").classes("px-1 py-1 border-b border-gray-100 text-center"):
                                    def subir(i: int) -> None:
                                        if 0 <= i < len(gastos_data) and i > 0:
                                            sync_inputs_to_rows()
                                            gastos_data[i], gastos_data[i - 1] = gastos_data[i - 1], gastos_data[i]
                                            set_cotizador_tabla("gastos", gastos_data, uid)
                                            repintar()
                                    def bajar(i: int) -> None:
                                        if 0 <= i < len(gastos_data) and i < len(gastos_data) - 1:
                                            sync_inputs_to_rows()
                                            gastos_data[i], gastos_data[i + 1] = gastos_data[i + 1], gastos_data[i]
                                            set_cotizador_tabla("gastos", gastos_data, uid)
                                            repintar()
                                    with ui.row().classes("gap-0 justify-center"):
                                        ui.button("▲", on_click=lambda i=row_idx_in_data: subir(i)).classes("min-w-0 px-0.5 text-xs").props("flat dense no-caps")
                                        ui.button("▼", on_click=lambda i=row_idx_in_data: bajar(i)).classes("min-w-0 px-0.5 text-xs").props("flat dense no-caps")
                                with ui.element("td").classes("px-1 py-1 border-b border-gray-100 text-center"):
                                    def borrar_fila(r: Dict[str, Any]) -> None:
                                        if r in gastos_data:
                                            gastos_data.remove(r)
                                            repintar()
                                    ui.button("×", on_click=lambda r=row: borrar_fila(r)).classes("text-red-600 font-bold text-lg min-w-0 px-1").props("flat dense no-caps")
                            edit_rows_ref.append(rinputs)
                            row_to_inputs.append((row, rinputs))
            _pintar_header()

        repintar()
        _pintar_parametros()
        _pintar_ingresos()
        _pintar_resultados()

        def agregar_fila() -> None:
            gastos_data.append({"gasto": "", "importe": ""})
            repintar()

        def guardar() -> None:
            for row, rinputs in row_to_inputs:
                row["gasto"] = str(rinputs["gasto"].value or "")
                row["importe"] = str(rinputs["importe"].value or "")
            set_cotizador_tabla("gastos", gastos_data, uid)
            repintar()
            _pintar_header()
            ui.notify("Gastos guardados en la base de datos", color="positive")

        gastos_buttons_row.clear()
        with gastos_buttons_row:
            ui.button("Agregar fila", on_click=agregar_fila, color="primary")
            ui.button("Guardar tabla", on_click=guardar, color="secondary")

    background_tasks.create(_cargar_y_pintar(), name="cargar_balance")


