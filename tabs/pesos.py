"""
Fase 3 — tabs/pesos.py
Pestaña Pesos: tabla editable de conversiones de pesos.
Funciones exportadas: build_tab_pesos
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from nicegui import app, run, ui

from db import get_cotizador_tabla, set_cotizador_tabla


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

def build_tab_pesos() -> None:
    """Pestaña Pesos: tabla Pesario (Marca, Producto, Peso, Fuente, Total) en formato Excel."""
    user = _require_login()
    if not user:
        return

    uid = user["id"]

    def _get_tabla(nombre: str, default: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        r = get_cotizador_tabla(nombre, uid)
        return r if r else default

    pesario_data = list(_get_tabla("pesario", TABLA_PESARIO_DEFAULT))
    sort_col_pesario: List[Optional[str]] = [None]
    sort_asc_pesario: List[bool] = [True]

    def _parse_peso(s: Any) -> float:
        if s is None or s == "": return 0.0
        try:
            return float(str(s).replace(".", "").replace(",", "."))
        except (TypeError, ValueError):
            return 0.0

    def _fmt_peso_display(val: Any) -> str:
        """Formatea peso para mostrar con punto como separador de miles."""
        n = _parse_peso(val)
        return f"{int(n):,}".replace(",", ".") if n == int(n) else f"{n:,.1f}".replace(",", ".")

    with ui.column().classes("gap-4 p-4 w-full"):
        cont = ui.column().classes("gap-2 w-full overflow-x-auto")
        edit_rows: List[Dict[str, Any]] = []
        row_to_inputs: List[tuple] = []

        def toggle_sort(col: str) -> None:
            if sort_col_pesario[0] == col:
                sort_asc_pesario[0] = not sort_asc_pesario[0]
            else:
                sort_col_pesario[0] = col
                sort_asc_pesario[0] = True
            repintar()

        def sync_inputs_to_rows() -> None:
            for row_ref, rinputs in row_to_inputs:
                if row_ref in pesario_data:
                    row_ref["marca"] = str(rinputs["marca"].value or "")
                    row_ref["producto"] = str(rinputs["producto"].value or "")
                    row_ref["peso"] = str(rinputs["peso"].value or "")
                    row_ref["fuente"] = str(rinputs["fuente"].value or "")

        def repintar() -> None:
            sync_inputs_to_rows()
            cont.clear()
            edit_rows.clear()
            row_to_inputs.clear()
            datos = list(pesario_data)
            if sort_col_pesario[0] == "marca":
                rev = not sort_asc_pesario[0]
                datos.sort(key=lambda r: str(r.get("marca", "")).lower(), reverse=rev)
            elif sort_col_pesario[0] == "producto":
                rev = not sort_asc_pesario[0]
                datos.sort(key=lambda r: str(r.get("producto", "")).lower(), reverse=rev)
            elif sort_col_pesario[0] == "peso":
                rev = not sort_asc_pesario[0]
                datos.sort(key=lambda r: _parse_peso(r.get("peso")), reverse=rev)
            elif sort_col_pesario[0] == "fuente":
                rev = not sort_asc_pesario[0]
                datos.sort(key=lambda r: _parse_peso(r.get("fuente")), reverse=rev)
            elif sort_col_pesario[0] == "total":
                rev = not sort_asc_pesario[0]
                datos.sort(key=lambda r: _parse_peso(r.get("peso")) + _parse_peso(r.get("fuente")), reverse=rev)
            with cont:
                col_widths = {"marca": "100px", "producto": "399px", "peso": "70px", "fuente": "70px", "total": "90px", "ordenar": "56px", "borrar": "48px"}
                with ui.element("table").classes("border-collapse text-xs shrink-0").style("table-layout: fixed; width: 833px; min-width: 833px; line-height: 1.2;"):
                    with ui.element("colgroup"):
                        ui.element("col").style("width: " + col_widths["marca"])
                        ui.element("col").style("width: " + col_widths["producto"])
                        ui.element("col").style("width: " + col_widths["peso"])
                        ui.element("col").style("width: " + col_widths["fuente"])
                        ui.element("col").style("width: " + col_widths["total"])
                        ui.element("col").style("width: " + col_widths["ordenar"])
                        ui.element("col").style("width: " + col_widths["borrar"])
                    with ui.element("thead"):
                        with ui.element("tr").classes("bg-blue-100 dark:bg-blue-900"):
                            for col_key, h in [("marca", "Marca"), ("producto", "Producto"), ("peso", "Peso (gr)"), ("fuente", "Fuente (gr)"), ("total", "Total (gr)"), (None, "Ordenar"), (None, "Borrar")]:
                                th_cls = "font-semibold px-1 py-0.5 border border-gray-300"
                                th_cls += " text-left" if col_key in ("marca", "producto") else " text-center"
                                if col_key:
                                    th_cls += " cursor-pointer hover:bg-blue-200"
                                th = ui.element("th").classes(th_cls)
                                if col_key:
                                    th.on("click", lambda c=col_key: toggle_sort(c))
                                with th:
                                    ui.label(h)
                    with ui.element("tbody"):
                        for row_idx, row in enumerate(datos):
                            rinputs: Dict[str, Any] = {}
                            row_ref = row
                            idx_in_data = pesario_data.index(row) if row in pesario_data else row_idx
                            with ui.element("tr"):
                                for col in ["marca", "producto", "peso", "fuente"]:
                                    val = str(row.get(col, ""))
                                    if col in ("peso", "fuente") and val and _parse_peso(val) != 0:
                                        val = _fmt_peso_display(val)
                                    td_el = ui.element("td").classes("border border-gray-200").style("padding: 2px 4px; vertical-align: middle;")
                                    td_align = "text-center" if col in ("peso", "fuente") else ""
                                    with td_el:
                                        inp = ui.input(value=val).classes("w-full border-0 text-xs " + td_align).props("dense")
                                        rinputs[col] = inp
                                with ui.element("td").classes("border border-gray-200 bg-gray-50 text-center").style("padding: 2px 4px; vertical-align: middle;"):
                                    p0 = _parse_peso(row.get("peso"))
                                    f0 = _parse_peso(row.get("fuente"))
                                    t0 = p0 + f0
                                    total_txt = _fmt_peso_display(str(int(t0)) if t0 == int(t0) else f"{t0:.1f}")
                                    lbl_total = ui.label(total_txt).classes("px-1")

                                    def actualizar_total(lbl=lbl_total, rinp=rinputs) -> None:
                                        p = _parse_peso(rinp["peso"].value)
                                        f = _parse_peso(rinp["fuente"].value)
                                        t = p + f
                                        txt = _fmt_peso_display(str(int(t)) if t == int(t) else f"{t:.1f}")
                                        lbl.text = txt

                                    rinputs["peso"].on_value_change(actualizar_total)
                                    rinputs["fuente"].on_value_change(actualizar_total)
                                with ui.element("td").classes("border border-gray-200 w-8 text-center").style("padding: 2px 4px; vertical-align: middle;"):
                                    def subir(i: int) -> None:
                                        if 0 <= i < len(pesario_data) and i > 0:
                                            sync_inputs_to_rows()
                                            pesario_data[i], pesario_data[i - 1] = pesario_data[i - 1], pesario_data[i]
                                            repintar()
                                    def bajar(i: int) -> None:
                                        if 0 <= i < len(pesario_data) and i < len(pesario_data) - 1:
                                            sync_inputs_to_rows()
                                            pesario_data[i], pesario_data[i + 1] = pesario_data[i + 1], pesario_data[i]
                                            repintar()
                                    with ui.row().classes("gap-0 justify-center"):
                                        ui.button("▲", on_click=lambda i=idx_in_data: subir(i)).classes("min-w-0 px-0.5 text-xs").props("flat dense no-caps")
                                        ui.button("▼", on_click=lambda i=idx_in_data: bajar(i)).classes("min-w-0 px-0.5 text-xs").props("flat dense no-caps")
                                with ui.element("td").classes("border border-gray-200 w-8 text-center").style("padding: 2px 4px; vertical-align: middle;"):
                                    def borrar_pesario(rref: Dict[str, Any]) -> None:
                                        sync_inputs_to_rows()
                                        if rref in pesario_data:
                                            pesario_data.remove(rref)
                                            repintar()
                                    ui.button("×", on_click=lambda r=row_ref: borrar_pesario(r)).classes("text-red-600 font-bold text-base min-w-0 px-0").props("flat dense no-caps")
                            row_to_inputs.append((row_ref, rinputs))
                            edit_rows.append(rinputs)

        repintar()

        def guardar() -> None:
            sync_inputs_to_rows()
            new_data = []
            for row_ref, rinputs in row_to_inputs:
                p = _parse_peso(rinputs["peso"].value)
                f = _parse_peso(rinputs["fuente"].value)
                t = p + f
                new_data.append({
                    "marca": str(rinputs["marca"].value or ""),
                    "producto": str(rinputs["producto"].value or ""),
                    "peso": str(rinputs["peso"].value or ""),
                    "fuente": str(rinputs["fuente"].value or ""),
                    "total": str(int(t)) if t == int(t) else f"{t:.2f}",
                })
            set_cotizador_tabla("pesario", new_data, uid)
            pesario_data.clear()
            pesario_data.extend(new_data)
            ui.notify("Pesario guardado", color="positive")

        def agregar_fila() -> None:
            pesario_data.append({"marca": "", "producto": "", "peso": "0", "fuente": "0", "total": "0"})
            repintar()

        with ui.row().classes("gap-2"):
            ui.button("Agregar fila", on_click=agregar_fila, color="primary")
            ui.button("Guardar Tabla", on_click=guardar, color="secondary")


