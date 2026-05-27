"""
Fase 3 — tabs/pedidos.py
Pestaña Pedidos: vista consolidada de compras/pedidos de todos los clientes.
"""
from __future__ import annotations
from datetime import datetime
from typing import Any, Dict, List, Optional

from nicegui import app, ui

from db import (
    user_can_access_tab,
    get_user_qb_customer,
    get_compras_lista_all,
    update_compras_lista_row,
)


# ---------------------------------------------------------------------------
# Helpers de sesión (copiado de main.py; se unificará en auth.py en Fase 4)
# ---------------------------------------------------------------------------

def _require_login() -> Optional[Dict[str, Any]]:
    user = app.storage.user.get("user")
    if not user:
        ui.notify("Debes iniciar sesión para continuar", color="negative")
    return user


# ---------------------------------------------------------------------------
# Helpers de formato/ordenación (también en main.py para build_tab_compras_lista;
# se unificará cuando esa tab se extraiga)
# ---------------------------------------------------------------------------

def _fmt_fecha_compras(s: str) -> str:
    """Formato fecha: 'Lunes 16-03-26 09:30' (dia dd-mm-aa hora:minutos)."""
    if not s or not str(s).strip():
        return "—"
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


# ---------------------------------------------------------------------------
# Tab principal
# ---------------------------------------------------------------------------

def build_tab_pedidos(container) -> None:
    """Pestaña Pedidos: vista consolidada de compras de todos los clientes (usuario_qb = Cliente QB)."""
    user = _require_login()
    if not user:
        return

    is_admin = user_can_access_tab(user["id"], "admin")
    qb_cust = get_user_qb_customer(user["id"])
    mi_cliente_qb = (qb_cust or {}).get("name", "")

    with container:
        filtro_estado_ref: Dict[str, str] = {"val": "Cotizar+Buscando"}
        filtro_cliente_ref: Dict[str, str] = {"val": ""}
        sort_col_ref: List[str] = [""]
        sort_asc_ref: List[bool] = [True]
        tabla_container = ui.column().classes("w-full gap-2")

        def _refrescar() -> None:
            tabla_container.clear()
            with tabla_container:
                rows = get_compras_lista_all()
                est_val = filtro_estado_ref.get("val", "Cotizar+Buscando")
                if est_val:
                    if est_val == "No hay":
                        rows = [r for r in rows if (r.get("estado") or "") == ""]
                    elif est_val == "Cotizar":
                        rows = [r for r in rows if r.get("estado") == "Cotizar"]
                    elif est_val == "Cotizar+Buscando":
                        rows = [r for r in rows if r.get("estado") in ("Cotizar", "Buscando")]
                    elif est_val == "Buscando":
                        rows = [r for r in rows if r.get("estado") == "Buscando"]
                    elif est_val == "Comprado":
                        rows = [r for r in rows if r.get("estado") == "Comprado"]
                clientes = sorted(set(r.get("usuario_qb") or "" for r in rows if r.get("usuario_qb")))
                cli_val = filtro_cliente_ref.get("val", "")
                if cli_val:
                    rows = [r for r in rows if (r.get("usuario_qb") or "") == cli_val]

                rows = sorted(rows, key=lambda r: _sort_key_compras(r, sort_col_ref[0] or "fecha"), reverse=not sort_asc_ref[0])

                n_pedidos = len(rows)
                total_cotizar = 0.0
                for r in rows:
                    try:
                        cant = float(str(r.get("cantidad") or "0").replace(",", ".")) if r.get("cantidad") else 0
                    except (ValueError, TypeError):
                        cant = 0
                    try:
                        precio = float(str(r.get("precio_sugerido") or "0").replace(",", ".")) if r.get("precio_sugerido") else 0
                    except (ValueError, TypeError):
                        precio = 0
                    total_cotizar += cant * precio

                ui.label("Pedidos").classes("text-xl font-semibold mb-2")
                with ui.card().classes("w-full p-4 bg-grey-2"):
                    with ui.row().classes("w-full gap-6 flex-wrap items-center"):
                        with ui.column().classes("gap-0"):
                            ui.label("Cantidad de pedidos").classes("text-xs text-gray-600")
                            ui.label(str(n_pedidos)).classes("text-lg font-bold text-primary")
                        ui.element("div").classes("w-px h-8 bg-gray-400 shrink-0")
                        with ui.column().classes("gap-0"):
                            ui.label("Total a cotizar").classes("text-xs text-gray-600")
                            total_str = f"{total_cotizar:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                            ui.label(f"$ {total_str}").classes("text-lg font-bold text-primary")
                        ui.element("div").classes("w-px h-8 bg-gray-400 shrink-0")
                        ui.button("Refrescar", on_click=_refrescar).props("flat dense no-caps icon=refresh").classes("text-gray-800 hover:bg-gray-200 rounded px-3")

                with ui.row().classes("w-full gap-2 mb-2 items-center"):
                    ui.select(
                        {"": "Todos", "Cotizar": "Cotizar", "Cotizar+Buscando": "Cotizar+Buscando", "No hay": "No hay", "Buscando": "Buscando", "Comprado": "Comprado"},
                        value=filtro_estado_ref.get("val", "Cotizar+Buscando"),
                        label="Estado",
                        on_change=lambda e: (_refrescar_assign(e, filtro_estado_ref, "val") or _refrescar()),
                    ).classes("w-36").props("dense")
                    ui.select(
                        {"": "Todos", **{c: c or "(sin cliente)" for c in clientes if c}},
                        value=filtro_cliente_ref.get("val", ""),
                        label="Cliente",
                        on_change=lambda e: (_refrescar_assign(e, filtro_cliente_ref, "val") or _refrescar()),
                    ).classes("w-72").props("dense")

                def _refrescar_assign(e, d: Dict, k: str) -> None:
                    d[k] = getattr(e, "value", "") or ""

                with ui.element("table").classes("w-full border-collapse text-sm"):
                    with ui.element("thead"):
                        with ui.element("tr").classes("bg-primary text-white font-semibold text-center"):
                            for col_key, h in [("fecha", "Fecha"), ("marca", "Marca"), ("producto", "Producto"), ("sku", "SKU"), ("cantidad", "Cantidad"), ("precio_sugerido", "Precio sugerido"), ("estado", "Estado"), ("usuario_qb", "Cliente")]:
                                th = ui.element("th").classes("px-2 py-1 border text-center cursor-pointer hover:bg-primary/80")
                                th.on("click", lambda c=col_key: (sort_col_ref.__setitem__(0, c) if sort_col_ref[0] != c else sort_asc_ref.__setitem__(0, not sort_asc_ref[0]), sort_col_ref.__setitem__(0, c), sort_asc_ref.__setitem__(0, True) if sort_col_ref[0] != c else None, _refrescar()))
                                with th:
                                    ui.label(h)
                    with ui.element("tbody"):
                        for r in rows:
                            with ui.element("tr").classes("border-t hover:bg-gray-50"):
                                with ui.element("td").classes("px-2 py-1 border text-center"):
                                    ui.label(_fmt_fecha_compras(r.get("fecha", "")))
                                with ui.element("td").classes("px-2 py-1 border text-center"):
                                    ui.label(r.get("marca", "—"))
                                with ui.element("td").classes("px-2 py-1 border text-center"):
                                    ui.label(r.get("producto", "—"))
                                with ui.element("td").classes("px-2 py-1 border text-center"):
                                    _sku = r.get("sku") or ""
                                    ui.label(_sku if _sku else "—")
                                with ui.element("td").classes("px-2 py-1 border text-center"):
                                    ui.label(str(r.get("cantidad", "—")))
                                with ui.element("td").classes("px-2 py-1 border text-right"):
                                    _ps = r.get("precio_sugerido") or "—"
                                    ui.label(f"u$ {_ps}" if _ps != "—" else "—")
                                with ui.element("td").classes("px-2 py-1 border"):
                                    _est_opts = {"Cotizar": "Cotizar", "No hay": "No hay", "Buscando": "Buscando", "Comprado": "Comprado"}
                                    _est_db = r.get("estado") or ""
                                    _est_actual = "No hay" if _est_db == "" else (_est_db if _est_db in _est_opts else _est_db)
                                    if _est_actual and _est_actual not in _est_opts:
                                        _est_opts[_est_actual] = _est_actual
                                    def _on_estado_pedido(e, rid, uid):
                                        v = getattr(e, "value", "") or ""
                                        estado_guardar = "" if v == "No hay" else v
                                        update_compras_lista_row(rid, uid, estado=estado_guardar)
                                        ui.notify("Estado actualizado", color="positive")
                                        _refrescar()
                                    ui.select(_est_opts, value=_est_actual, on_change=lambda e, rid=r["id"], uid=r.get("user_id") or user["id"]: _on_estado_pedido(e, rid, uid)).classes("w-28").props("dense")
                                with ui.element("td").classes("px-2 py-1 border"):
                                    ui.label(r.get("usuario_qb", "—"))

        _refrescar()
