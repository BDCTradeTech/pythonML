"""
Fase 3 — tabs/compras_lista.py
Pestaña Compras Lista: tabla editable de compras a cotizar (marca, producto, cantidad, estado, usuario_qb).
Funciones exportadas: build_tab_compras_lista
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from nicegui import app, ui

from db import (
    get_marcas,
    get_user_qb_customer,
    get_compras_lista,
    get_compras_lista_row,
    insert_compras_lista,
    update_compras_lista_row,
    delete_compras_lista_row,
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
# Helpers privados de formato / parseo
# ---------------------------------------------------------------------------

def _fmt_fecha_compras(s: str) -> str:
    """Formato fecha: 'Lunes 16-03-26 09:30' (dia dd-mm-aa hora:minutos)."""
    if not s or not str(s).strip():
        return "””"
    s = str(s).strip()
    try:
        if " " in s:
            parts = s.split(" ", 1)
            date_str, time_str = parts[0], parts[1][:5] if len(parts) > 1 else ""
        else:
            date_str, time_str = s[:10], ""
        p = date_str.split("-")
        if len(p) >= 3:
            y, m, d = int(p[0]), int(p[1]), int(p[2])
            dt_obj = datetime(y, m, d)
            dia_nombre = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"][dt_obj.weekday()]
            dd = f"{d:02d}-{m:02d}-{y % 100:02d}"
            if time_str:
                return f"{dia_nombre} {dd} {time_str}"
            return f"{dia_nombre} {dd}"
        return s
    except Exception:
        return str(s)


def _fmt_precio_compras(val: str) -> str:
    """Formatea precio para pantalla: punto -> coma (ej: 1234.56 -> 1234,56)."""
    if not val:
        return ""
    s = str(val).strip()
    return s.replace(".", ",")


def _parse_precio_compras_input(s: str) -> str:
    """Parsea precio: acepta coma o punto como decimal, normaliza a punto para BD."""
    if not s or not str(s).strip():
        return ""
    s = str(s).strip().replace(",", ".")
    # Dejar solo dígitos y un punto
    parts = s.split(".")
    if len(parts) > 2:
        s = parts[0] + "." + "".join(parts[1:])
    return s


def _parse_fecha_compras_input(s: str) -> str:
    """Parsea 'Lun 16-03-26 09:30' o '16-03-26 09:30' a 'YYYY-MM-DD HH:MM'."""
    if not s or not str(s).strip():
        return ""
    s = str(s).strip()
    # Buscar dd-mm-yy (o yy) y opcional hh:mm
    m = re.search(r"(\d{1,2})-(\d{1,2})-(\d{2,4})\s*(\d{1,2}:\d{2})?", s)
    if m:
        d, m_val, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        y_full = 2000 + y if y < 100 else y
        time_part = m.group(4) or "00:00"
        return f"{y_full:04d}-{m_val:02d}-{d:02d} {time_part}"
    # Si ya está en YYYY-MM-DD
    if re.match(r"\d{4}-\d{2}-\d{2}", s):
        return s[:16] if len(s) > 10 else (s + " 00:00")
    return s


def _solo_numeros(val: str) -> str:
    """Filtra a solo dígitos (cantidad entera)."""
    if not val:
        return ""
    return "".join(c for c in str(val) if c.isdigit())


def _sort_key_compras(row: Dict[str, Any], col: str) -> Any:
    """Clave de ordenación para filas de compras_lista."""
    if col == "fecha":
        raw = row.get("fecha") or ""
        try:
            if " " in raw:
                ds, ts = raw.split(" ", 1)
                return ds + (ts[:5] if ts else "")
            return raw[:10] + " 00:00"
        except Exception:
            return ""
    if col in ("cantidad", "precio_sugerido"):
        try:
            return float(row.get(col) or 0)
        except (ValueError, TypeError):
            return 0.0
    return (row.get(col) or "").lower()


def build_tab_compras_lista(container) -> None:
    """Pestaña Compras Lista: tabla editable de compras a cotizar (marca, producto, cantidad, estado, usuario_qb)."""
    user = _require_login()
    if not user:
        return

    container.clear()
    marcas_list = get_marcas()
    qb_cust = get_user_qb_customer(user["id"])
    cliente_default = (qb_cust or {}).get("name", "")

    with container:
        filtro_estado_ref: Dict[str, str] = {"val": "Todas"}
        sort_col_ref: List[str] = [""]
        sort_asc_ref: List[bool] = [True]
        # Filtro arriba de tabla (solo), tabla, botón debajo
        compras_header = ui.column().classes("w-full mb-2")
        filtro_row = ui.column().classes("w-full mb-2")
        tabla_container = ui.column().classes("w-full gap-2")
        boton_row = ui.row().classes("w-full mt-2 items-center")

        user_id_ref: List[int] = [user["id"]]
        tbody_el = None  # se asignará al crear la tabla

        def _filtrar_cantidad_on_input(inp) -> None:
            """Solo permite dígitos en cantidad."""
            if hasattr(inp, "value"):
                actual = getattr(inp, "value", "") or ""
                filtrado = _solo_numeros(actual)
                if filtrado != actual:
                    inp.value = filtrado

        def _filtrar_precio_on_input(inp) -> None:
            """Solo permite dígitos, punto y coma en precio; muestra coma como decimal."""
            if not hasattr(inp, "value"):
                return
            s = getattr(inp, "value", "") or ""
            s = "".join(c for c in str(s) if c.isdigit() or c in ".,")
            # Máximo un separador decimal; mantener primera parte entera y primera decimal
            if s.count(".") + s.count(",") > 1:
                parts = s.replace(",", ".").split(".")
                s = parts[0] + "," + (parts[1] if len(parts) > 1 else "")
            s = s.replace(".", ",")
            if s != (getattr(inp, "value", "") or ""):
                inp.value = s

        def _refrescar_tabla() -> None:
            """Limpia tbody y pinta todas las filas filtradas."""
            uid = user_id_ref[0]
            rows = get_compras_lista(uid)
            filtro_val = filtro_estado_ref.get("val", "Todas")
            if filtro_val and filtro_val != "Todas":
                if filtro_val == "No hay":
                    filtrados = [r for r in rows if (r.get("estado") or "") == ""]
                elif filtro_val == "Cotizar":
                    filtrados = [r for r in rows if r.get("estado") == "Cotizar"]
                elif filtro_val == "Buscando":
                    filtrados = [r for r in rows if r.get("estado") == "Buscando"]
                elif filtro_val == "Comprado":
                    filtrados = [r for r in rows if r.get("estado") == "Comprado"]
                else:
                    filtrados = rows
            else:
                filtrados = rows
            filtrados = sorted(filtrados, key=lambda r: _sort_key_compras(r, sort_col_ref[0] or "fecha"), reverse=not sort_asc_ref[0])
            n_pedidos = len(filtrados)
            total_cotizar = 0.0
            for r in filtrados:
                try:
                    cant = float(str(r.get("cantidad") or "0").replace(",", ".")) if r.get("cantidad") else 0
                except (ValueError, TypeError):
                    cant = 0
                try:
                    precio = float(str(r.get("precio_sugerido") or "0").replace(",", ".")) if r.get("precio_sugerido") else 0
                except (ValueError, TypeError):
                    precio = 0
                total_cotizar += cant * precio
            compras_header.clear()
            with compras_header:
                ui.label("Compras").classes("text-xl font-semibold mb-2")
                with ui.card().classes("w-full p-4 bg-grey-2"):
                    with ui.row().classes("w-full gap-6 flex-wrap items-center"):
                        with ui.column().classes("gap-0"):
                            ui.label("Cantidad de pedidos").classes("text-xs text-gray-600")
                            ui.label(str(n_pedidos)).classes("text-lg font-bold text-primary")
                        ui.element("div").classes("w-px h-8 bg-gray-400 shrink-0")
                        with ui.column().classes("gap-0"):
                            ui.label("Total a cotizar").classes("text-xs text-gray-600")
                            _ts = f"{total_cotizar:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                            ui.label(f"$ {_ts}").classes("text-lg font-bold text-primary")
                        ui.element("div").classes("w-px h-8 bg-gray-400 shrink-0")
                        ui.button("Refrescar", on_click=_refrescar_tabla).props("flat dense no-caps icon=refresh").classes("text-gray-800 hover:bg-gray-200 rounded px-3")
            tbody_el.clear()
            with tbody_el:
                for r in filtrados:
                    _crear_fila_tr(r, uid)

        def _guardar_campo(evt, row_id: int, uid: int, kw: Dict) -> None:
            for k, inp in kw.items():
                v = (getattr(inp, "value", "") or "").strip() if hasattr(inp, "value") else ""
                if k == "fecha":
                    v = _parse_fecha_compras_input(v)
                elif k == "cantidad":
                    v = _solo_numeros(v)
                elif k == "precio_sugerido":
                    v = _parse_precio_compras_input(v)
                update_compras_lista_row(row_id, uid, **{k: v})
            ui.notify("Guardado", color="positive")
            # No refrescar al guardar un campo: evita borrar el producto al pasar a cantidad/precio

        def _guardar_marca(e, row_id: int, uid: int) -> None:
            v = getattr(e, "value", "") or ""
            if not v or v == "(Otras)":
                v = ""
            update_compras_lista_row(row_id, uid, marca=str(v))
            ui.notify("Marca actualizada", color="positive")
            # No refrescar: evita borrar datos de la fila

        def _borrar_fila(row_id: int, uid: int) -> None:
            delete_compras_lista_row(row_id, uid)
            ui.notify("Fila eliminada", color="positive")
            _refrescar_tabla()

        def _on_filtro(e) -> None:
            v = getattr(e, "value", None)
            filtro_estado_ref["val"] = v if v is not None and v != "" else ("Todas" if v == "" else "Cotizar")
            _refrescar_tabla()

        def _agregar_fila() -> None:
            u = _require_login()
            if not u:
                ui.notify("Debe iniciar sesión", color="negative")
                return
            qb = get_user_qb_customer(u["id"])
            cli = (qb or {}).get("name", "")
            now = datetime.now()
            fecha_str = now.strftime("%Y-%m-%d %H:%M")
            new_id = insert_compras_lista(u["id"], fecha_str, estado="Cotizar", usuario_qb=cli)
            new_row = get_compras_lista_row(new_id, u["id"])
            filtro_estado_ref["val"] = "Cotizar"
            if filtro_select_ref[0] is not None:
                filtro_select_ref[0].value = "Cotizar"
            _refrescar_tabla()
            ui.notify("Fila agregada", color="positive")

        filtro_select_ref: List = [None]
        with filtro_row:
            filtro_select_ref[0] = ui.select(
                {"Todas": "Todas", "Cotizar": "Cotizar", "No hay": "No hay", "Buscando": "Buscando", "Comprado": "Comprado"},
                value=filtro_estado_ref.get("val", "Cotizar"),
                label="Estado",
                on_change=lambda e: _on_filtro(e),
            ).classes("w-40").props("dense")
        with boton_row:
            ui.button("Agregar fila", on_click=_agregar_fila, color="primary").props("dense no-caps")

        def _crear_fila_tr(r: Dict, uid: int) -> None:
            """Crea una fila (tr) en el tbody actual."""
            rid = r["id"]
            user_id_row = r.get("user_id", uid)
            with ui.element("tr").classes("border-t hover:bg-gray-50"):
                with ui.element("td").classes("px-2 py-1 border"):
                    fecha_val = _fmt_fecha_compras(r.get("fecha", "")) if r.get("fecha") else ""
                    inp_f = ui.input(value=fecha_val).classes("w-36").props("dense")
                    inp_f.on("keydown.enter", lambda evt, row_id=rid, uid=user_id_row, kw={"fecha": inp_f}: _guardar_campo(evt, row_id, uid, kw))
                    inp_f.on("blur", lambda evt, row_id=rid, uid=user_id_row, kw={"fecha": inp_f}: _guardar_campo(evt, row_id, uid, kw))
                with ui.element("td").classes("px-2 py-1 border"):
                    marcas_opts = {m["nombre"]: m["nombre"] for m in marcas_list}
                    marca_actual = r.get("marca", "") or ""
                    if marca_actual and marca_actual not in marcas_opts:
                        marcas_opts = {marca_actual: marca_actual, **marcas_opts}
                    marcas_opts = {"": "(Otras)", **marcas_opts}
                    ui.select(marcas_opts, value=marca_actual or None, on_change=lambda e, row_id=rid, uid=user_id_row: _guardar_marca(e, row_id, uid)).classes("w-28").props("dense")
                with ui.element("td").classes("px-2 py-1 border"):
                    inp_p = ui.input(value=r.get("producto", "")).classes("w-56").props("dense")
                    inp_p.on("keydown.enter", lambda evt, row_id=rid, uid=user_id_row, kw={"producto": inp_p}: _guardar_campo(evt, row_id, uid, kw))
                    inp_p.on("blur", lambda evt, row_id=rid, uid=user_id_row, kw={"producto": inp_p}: _guardar_campo(evt, row_id, uid, kw))
                with ui.element("td").classes("px-2 py-1 border"):
                    inp_s = ui.input(value=r.get("sku", "")).classes("w-36").props("dense")
                    inp_s.on("keydown.enter", lambda evt, row_id=rid, uid=user_id_row, kw={"sku": inp_s}: _guardar_campo(evt, row_id, uid, kw))
                    inp_s.on("blur", lambda evt, row_id=rid, uid=user_id_row, kw={"sku": inp_s}: _guardar_campo(evt, row_id, uid, kw))
                with ui.element("td").classes("px-2 py-1 border text-center"):
                    cant_val = _solo_numeros(str(r.get("cantidad", "") or ""))
                    inp_c = ui.input(value=cant_val).classes("w-16").props("dense inputmode=numeric")
                    inp_c.on("input", lambda e, inp=inp_c: _filtrar_cantidad_on_input(inp))
                    inp_c.on("keydown.enter", lambda evt, row_id=rid, uid=user_id_row, kw={"cantidad": inp_c}: _guardar_campo(evt, row_id, uid, kw))
                    inp_c.on("blur", lambda evt, row_id=rid, uid=user_id_row, kw={"cantidad": inp_c}: _guardar_campo(evt, row_id, uid, kw))
                with ui.element("td").classes("px-2 py-1 border text-right"):
                    precio_val = _fmt_precio_compras(str(r.get("precio_sugerido", "") or ""))
                    with ui.row().classes("items-center justify-end gap-1"):
                        ui.label("u$").classes("text-gray-600 text-sm")
                        inp_ps = ui.input(value=precio_val).classes("w-20").props("dense")
                    inp_ps.on("input", lambda e, inp=inp_ps: _filtrar_precio_on_input(inp))
                    inp_ps.on("keydown.enter", lambda evt, row_id=rid, uid=user_id_row, kw={"precio_sugerido": inp_ps}: _guardar_campo(evt, row_id, uid, kw))
                    inp_ps.on("blur", lambda evt, row_id=rid, uid=user_id_row, kw={"precio_sugerido": inp_ps}: _guardar_campo(evt, row_id, uid, kw))
                with ui.element("td").classes("px-2 py-1 border"):
                    _est_display = {"": "No hay", "Buscando": "Buscando"}.get(r.get("estado") or "", r.get("estado") or "Cotizar")
                    ui.label(_est_display).classes("text-sm")
                with ui.element("td").classes("px-2 py-1 border"):
                    ui.button("Borrar", on_click=lambda row_id=rid, uid=user_id_row: _borrar_fila(row_id, uid)).props("flat dense no-caps").classes("text-negative")

        def _on_filtro(e) -> None:
            filtro_estado_ref["val"] = getattr(e, "value", "Cotizar") or "Cotizar"
            _refrescar_tabla()

        def _agregar_fila() -> None:
            u = _require_login()
            if not u:
                ui.notify("Debe iniciar sesión", color="negative")
                return
            qb = get_user_qb_customer(u["id"])
            cli = (qb or {}).get("name", "")
            now = datetime.now()
            fecha_str = now.strftime("%Y-%m-%d %H:%M")
            new_id = insert_compras_lista(u["id"], fecha_str, estado="Cotizar", usuario_qb=cli)
            new_row = get_compras_lista_row(new_id, u["id"])
            if new_row and filtro_estado_ref.get("val") == "Cotizar":
                with tbody_el:
                    _crear_fila_tr(new_row, u["id"])
            ui.notify("Fila agregada", color="positive")

        def _th_classes(col_key: str) -> str:
            base = "px-2 py-1 border cursor-pointer hover:bg-primary/80"
            if col_key == "precio_sugerido":
                return f"{base} text-center"
            return f"{base} text-center"

        with tabla_container:
            with ui.element("table").classes("w-full border-collapse text-sm"):
                with ui.element("thead"):
                    with ui.element("tr").classes("bg-primary text-white font-semibold text-center"):
                        for col_key, h in [("fecha", "Fecha"), ("marca", "Marca"), ("producto", "Producto"), ("sku", "SKU"), ("cantidad", "Cantidad"), ("precio_sugerido", "Precio sugerido"), ("estado", "Estado"), ("", "Borrar")]:
                            th = ui.element("th").classes(_th_classes(col_key))
                            if col_key:
                                th.on("click", lambda c=col_key: (sort_col_ref.__setitem__(0, c) if sort_col_ref[0] != c else sort_asc_ref.__setitem__(0, not sort_asc_ref[0]), sort_col_ref.__setitem__(0, c), sort_asc_ref.__setitem__(0, True) if sort_col_ref[0] != c else None, _refrescar_tabla()))
                            with th:
                                ui.label(h)
                with ui.element("tbody") as tbody_el:
                    pass

        _refrescar_tabla()


