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

    def _load_zonas():
        conn = get_connection()
        rows = conn.execute(
            "SELECT id, nombre, codigos_postales, tarifa FROM flex_zonas WHERE user_id=? ORDER BY tarifa",
            (uid,)
        ).fetchall()
        conn.close()
        return rows

    def _refresh(container: Any, bar_label: Any) -> None:
        zonas = _load_zonas()
        n = len(zonas)
        bar_label.set_text(f"{n} zona{'s' if n != 1 else ''} configurada{'s' if n != 1 else ''}")
        container.clear()
        with container:
            if not zonas:
                ui.label("No hay zonas configuradas.").classes("text-sm text-gray-400 col-span-3")
                return
            for z in zonas:
                tarifa_fmt = f"$ {int(z['tarifa']):,}".replace(",", ".")
                with ui.card().classes("p-2 gap-0.5").style("min-width:0"):
                    with ui.row().classes("w-full justify-between items-start gap-1"):
                        with ui.column().classes("gap-0.5").style("flex:1; min-width:0"):
                            ui.label(z["nombre"]).classes("font-semibold text-sm leading-tight")
                            ui.label(tarifa_fmt).classes("text-sm font-medium").style("color:#0C447C")
                            ui.label(z["codigos_postales"] or "—").classes(
                                "text-xs text-gray-400 leading-tight"
                            ).style(
                                "word-break:break-all; overflow:hidden; display:-webkit-box; "
                                "-webkit-line-clamp:2; -webkit-box-orient:vertical"
                            )
                        with ui.column().classes("gap-0 items-end").style("flex-shrink:0"):
                            ui.button(
                                icon="edit",
                                on_click=lambda z=z: _edit_dialog(z, grid, bar_lbl)
                            ).props("flat dense size=sm")

    def _new_dialog(container: Any, bar_label: Any) -> None:
        with ui.dialog() as dlg, ui.card().style("min-width:420px"):
            ui.label("Nueva zona").classes("text-sm font-semibold mb-1")
            inp_nombre = ui.input("Nombre").props("dense outlined").classes("w-full")
            inp_tarifa = ui.input("Tarifa ($)").props("dense outlined").classes("w-full")
            inp_cps = ui.textarea(
                "Códigos postales (uno por línea o separados por coma)"
            ).props("dense outlined").classes("w-full")
            with ui.row().classes("gap-2 justify-end w-full mt-1"):
                ui.button("Cancelar", on_click=dlg.close).props("flat dense")
                def _save(dlg=dlg):
                    try:
                        tarifa = float(inp_tarifa.value or 0)
                    except ValueError:
                        tarifa = 0.0
                    cps = ",".join(
                        p.strip()
                        for p in inp_cps.value.replace("\n", ",").split(",")
                        if p.strip()
                    )
                    conn = get_connection()
                    conn.execute(
                        "INSERT INTO flex_zonas (user_id, nombre, codigos_postales, tarifa) VALUES (?,?,?,?)",
                        (uid, inp_nombre.value.strip(), cps, tarifa),
                    )
                    conn.commit()
                    conn.close()
                    dlg.close()
                    _refresh(container, bar_label)
                ui.button("Crear", on_click=_save, color="primary").props("dense")
        dlg.open()

    def _edit_dialog(z: Any, container: Any, bar_label: Any) -> None:
        with ui.dialog() as dlg, ui.card().style("min-width:420px"):
            ui.label("Editar zona").classes("text-sm font-semibold mb-1")
            inp_nombre = ui.input("Nombre", value=z["nombre"]).props("dense outlined").classes("w-full")
            inp_tarifa = ui.input("Tarifa ($)", value=str(int(z["tarifa"]))).props("dense outlined").classes("w-full")
            cps_display = (z["codigos_postales"] or "").replace(",", "\n")
            inp_cps = ui.textarea("Códigos postales", value=cps_display).props("dense outlined").classes("w-full")
            with ui.row().classes("gap-2 justify-between w-full mt-1"):
                def _delete_confirm(dlg=dlg, z=z):
                    conn = get_connection()
                    conn.execute("DELETE FROM flex_zonas WHERE id=?", (z["id"],))
                    conn.commit()
                    conn.close()
                    dlg.close()
                    _refresh(container, bar_label)
                ui.button("Eliminar zona", on_click=_delete_confirm).props("flat dense color=negative")
                with ui.row().classes("gap-2"):
                    ui.button("Cancelar", on_click=dlg.close).props("flat dense")
                    def _save(dlg=dlg, z=z):
                        try:
                            tarifa = float(inp_tarifa.value or 0)
                        except ValueError:
                            tarifa = 0.0
                        cps = ",".join(
                            p.strip()
                            for p in inp_cps.value.replace("\n", ",").split(",")
                            if p.strip()
                        )
                        conn = get_connection()
                        conn.execute(
                            "UPDATE flex_zonas SET nombre=?, tarifa=?, codigos_postales=? WHERE id=?",
                            (inp_nombre.value.strip(), tarifa, cps, z["id"]),
                        )
                        conn.commit()
                        conn.close()
                        dlg.close()
                        _refresh(container, bar_label)
                    ui.button("Guardar", on_click=_save, color="primary").props("dense")
        dlg.open()

    # ── Layout principal ──────────────────────────────────────────────────────
    with ui.column().classes("w-full").style("padding:16px; gap:12px"):
        with ui.row().classes("w-full items-center justify-between p-3 rounded").style("background:#f3f4f6"):
            bar_lbl = ui.label("…").classes("text-sm text-gray-600 font-medium")
            ui.button(
                "+ Agregar zona",
                on_click=lambda: _new_dialog(grid, bar_lbl)
            ).props("flat dense").style("color:#0C447C")

        grid = ui.element("div").classes("w-full").style(
            "display:grid; grid-template-columns:repeat(3,1fr); gap:10px"
        )
        _refresh(grid, bar_lbl)
