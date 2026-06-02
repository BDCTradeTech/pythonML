"""
tabs/flex.py
Pestaña Flex: gestión de zonas de envíos Flex (tarifa por zona y códigos postales).
Funciones exportadas: build_tab_flex
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from nicegui import app, ui

from db import get_connection


def _require_login() -> Optional[Dict[str, Any]]:
    user = app.storage.user.get("user")
    if not user:
        ui.notify("Debes iniciar sesión para continuar", color="negative")
    return user


def build_tab_flex() -> None:
    user = _require_login()
    if not user:
        return
    uid: int = user["id"]

    def _build_flex(container: Any) -> None:
        conn = get_connection()
        zonas = conn.execute(
            "SELECT id, nombre, codigos_postales, tarifa FROM flex_zonas ORDER BY tarifa"
        ).fetchall()
        conn.close()
        container.clear()
        with container:
            if not zonas:
                ui.label("No hay zonas cargadas.").style("font-size:13px; color:gray")
            for z in zonas:
                tarifa_fmt = f"$ {int(z['tarifa']):,}".replace(",", ".")
                with ui.card().classes("w-full p-3").style("margin-bottom:8px"):
                    with ui.row().classes("w-full items-start justify-between"):
                        with ui.column().style("gap:4px; flex:1"):
                            with ui.row().classes("items-center gap-2"):
                                ui.label(z["nombre"]).style("font-size:14px; font-weight:600")
                                ui.label(tarifa_fmt).style("font-size:14px; color:#0C447C; font-weight:500")
                            ui.label(z["codigos_postales"] or "—").style(
                                "font-size:11px; color:gray; white-space:pre-wrap; word-break:break-all"
                            )
                        with ui.row().classes("items-center gap-1"):
                            ui.button(icon="edit", on_click=lambda z=z: _edit_zona(z, container)).props("flat dense size=sm")
                            ui.button(icon="delete", on_click=lambda zid=z["id"]: _delete_zona(zid, container)).props("flat dense size=sm color=red")
            ui.button("+ Nueva zona", on_click=lambda: _new_zona_form(container)).props("flat dense").style("margin-top:8px")

    def _edit_zona(z: Any, container: Any) -> None:
        with ui.dialog() as dlg, ui.card().style("min-width:400px"):
            ui.label("Editar zona").style("font-size:14px; font-weight:600")
            inp_nombre = ui.input("Nombre", value=z["nombre"]).props("dense outlined").classes("w-full")
            inp_tarifa = ui.input("Tarifa ($)", value=str(int(z["tarifa"]))).props("dense outlined").classes("w-full")
            inp_cps = ui.textarea("Códigos postales", value=z["codigos_postales"] or "").props("dense outlined").classes("w-full")
            with ui.row().classes("gap-2 justify-end w-full"):
                ui.button("Cancelar", on_click=dlg.close).props("flat dense")
                def _save(dlg=dlg, z=z) -> None:
                    try:
                        tarifa = float(inp_tarifa.value or 0)
                    except ValueError:
                        tarifa = 0.0
                    conn = get_connection()
                    conn.execute(
                        "UPDATE flex_zonas SET nombre=?, tarifa=?, codigos_postales=? WHERE id=?",
                        (inp_nombre.value.strip(), tarifa, inp_cps.value.strip(), z["id"]),
                    )
                    conn.commit()
                    conn.close()
                    dlg.close()
                    _build_flex(container)
                ui.button("Guardar", on_click=_save, color="primary").props("dense")
        dlg.open()

    def _delete_zona(zid: int, container: Any) -> None:
        conn = get_connection()
        conn.execute("DELETE FROM flex_zonas WHERE id=?", (zid,))
        conn.commit()
        conn.close()
        _build_flex(container)

    def _new_zona_form(container: Any) -> None:
        with ui.dialog() as dlg, ui.card().style("min-width:400px"):
            ui.label("Nueva zona").style("font-size:14px; font-weight:600")
            inp_nombre = ui.input("Nombre").props("dense outlined").classes("w-full")
            inp_tarifa = ui.input("Tarifa ($)").props("dense outlined").classes("w-full")
            inp_cps = ui.textarea("Códigos postales (uno por línea)").props("dense outlined").classes("w-full")
            with ui.row().classes("gap-2 justify-end w-full"):
                ui.button("Cancelar", on_click=dlg.close).props("flat dense")
                def _save_new(dlg=dlg) -> None:
                    try:
                        tarifa = float(inp_tarifa.value or 0)
                    except ValueError:
                        tarifa = 0.0
                    conn = get_connection()
                    conn.execute(
                        "INSERT INTO flex_zonas (user_id, nombre, codigos_postales, tarifa) VALUES (?,?,?,?)",
                        (uid, inp_nombre.value.strip(), inp_cps.value.strip(), tarifa),
                    )
                    conn.commit()
                    conn.close()
                    dlg.close()
                    _build_flex(container)
                ui.button("Crear", on_click=_save_new, color="primary").props("dense")
        dlg.open()

    with ui.column().classes("w-full").style("padding:16px; gap:12px"):
        with ui.row().classes("items-center gap-2"):
            ui.html('<i class="ti ti-motorbike" style="font-size:20px;color:#0C447C"></i>')
            ui.label("Envíos Flex").style("font-size:18px; font-weight:600; color:#0C447C")
        flex_container = ui.column().classes("w-full")
        _build_flex(flex_container)
