"""
Fase 3 — tabs/admin.py
Pestaña Admin: gestión de usuarios, permisos, QB credentials, marcas y despachantes.
Funciones exportadas: build_tab_admin
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from nicegui import app, run, ui

from auth import admin_reset_user_password, delete_user_and_all_data
from db import (
    get_qb_app_credentials,
    set_qb_app_credentials,
    get_qb_tokens,
    get_user_qb_customer,
    set_user_qb_customer,
    get_all_users,
    get_user_tab_permissions,
    set_user_tab_permission,
    _enable_tabs_for_user,
    user_can_access_tab,
    get_marcas,
    insert_marca,
    update_marca,
    delete_marca,
    get_despachantes,
    insert_despachante,
    update_despachante,
    delete_despachante,
)
from ml_api import get_ml_access_token
from tabs.constants import TAB_KEYS, TABS_QB


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

def build_tab_admin(container) -> None:
    """Pestaña Admin: tabla de usuarios con permisos por pestaña y estado ML/BDC."""
    container.clear()
    user = _require_login()
    if not user:
        return
    if not user_can_access_tab(user["id"], "admin"):
        with container:
            ui.label("No tenés permiso para acceder a Admin.").classes("text-negative")
        return

    users_list = get_all_users()
    with container:
        with ui.column().classes("w-full gap-2 p-2"):
            # ─── Permisos (tabla transpuesta, colapsable) ───────────────
            _SECTION_PAGES = [
                ("Home",          [("home", "Home"), ("dashboard", "Dashboard")]),
                ("MercadoLibre",  [
                    ("estadisticas",  "Estadísticas"),
                    ("ventas",        "Ventas"),
                    ("productos",     "Productos"),
                    ("cuotas",        "Cuotas"),
                    ("promos",        "Promos"),
                    ("preguntas",     "Preguntas"),
                    ("flex",          "Flex"),
                    ("busqueda",      "Búsqueda"),
                ]),
                ("BDC",           [("balance", "Balance"), ("compras", "Invoices")]),
                ("Comex",         [
                    ("stock",         "Stock"),
                    ("compras_lista", "Compras"),
                    ("pedidos",       "Pedidos"),
                    ("historicos",    "Históricos"),
                    ("importacion",   "Importación"),
                    ("guias",         "Guías"),
                ]),
                ("Impuestos",     [("pesos", "Pesos"), ("arca", "ARCA"), ("gastos", "Gastos"), ("analisis_ml", "Análisis ML")]),
                ("Config",        [("datos", "Datos"), ("configuracion", "Configuración")]),
                ("Admin",         [("admin", "Admin"), ("actividad", "Actividad")]),
            ]

            _all_perms = {u["id"]: get_user_tab_permissions(u["id"]) for u in users_list}
            _all_ml    = {u["id"]: bool(get_ml_access_token(u["id"])) for u in users_list}
            _all_bdc: dict = {}
            for _u2 in users_list:
                _tok2 = get_qb_tokens(_u2["id"])
                _all_bdc[_u2["id"]] = bool(_tok2 and _tok2.get("access_token"))

            _flat_rows: list = []
            for _sname, _spages in _SECTION_PAGES:
                _sn = len(_spages)
                for _si, (_stk, _slbl) in enumerate(_spages):
                    _flat_rows.append({"section": _sname if _si == 0 else None, "sc": _sn, "key": _stk, "label": _slbl})

            def _perm_toggle(uid_i: int, tk_i: str, evt: Any) -> None:
                set_user_tab_permission(uid_i, tk_i, bool(getattr(evt, "value", evt)))
                ui.notify("Permiso actualizado", color="positive")

            def _do_delete_u(tuid: int, tuname: str) -> None:
                with ui.dialog() as _dlg:
                    _dlg.props("persistent")
                    with ui.card().classes("p-4 min-w-[300px]"):
                        ui.label("¿Estás seguro que querés borrarlo?").classes("text-lg font-bold")
                        ui.label(f"Se borrará el usuario {tuname} y todos sus datos.").classes("text-sm text-gray-600 mt-1")
                        with ui.row().classes("mt-3 gap-2 justify-end"):
                            ui.button("Cancelar", on_click=_dlg.close)
                            def _del_confirm(_dr=_dlg, _ti=tuid):
                                if _ti == user["id"]:
                                    ui.notify("No podés borrarte a vos mismo.", color="negative")
                                    _dr.close()
                                    return
                                err = delete_user_and_all_data(_ti)
                                _dr.close()
                                if err:
                                    ui.notify(err, color="negative")
                                else:
                                    ui.notify("Usuario borrado correctamente", color="positive")
                                    build_tab_admin(container)
                            ui.button("Borrar", on_click=_del_confirm, color="negative").props("flat")
                _dlg.open()

            def _do_reset_u(tuid: int) -> None:
                err, email_sent, dest_email, new_pwd = admin_reset_user_password(tuid)
                if err and not new_pwd:
                    ui.notify(err, color="negative")
                elif email_sent and dest_email:
                    ui.notify(f"Enviamos un email con la nueva contraseña a {dest_email}", color="positive")
                elif new_pwd:
                    with ui.dialog() as _dlg:
                        _dlg.props("persistent")
                        with ui.card().classes("p-6 min-w-[400px]"):
                            ui.label("No se pudo enviar el email").classes("text-lg font-semibold text-warning")
                            ui.label(err or "Contraseña actualizada, pero el correo no llegó.").classes("text-sm text-gray-600 mt-2")
                            ui.label("Nueva contraseña generada (copiala y entregala al usuario):").classes("text-sm font-medium mt-4")
                            with ui.row().classes("mt-2 p-3 bg-gray-100 rounded font-mono text-lg select-all"):
                                ui.label(new_pwd)
                            ui.button("Cerrar popup", on_click=_dlg.close).props("flat color=primary").classes("mt-4")
                    _dlg.open()
                else:
                    ui.notify("Contraseña actualizada, pero no se pudo enviar el email.", color="warning")

            _popen = {"v": True}

            with ui.card().classes("w-full p-0 overflow-hidden").style("border: 1px solid #e0e0e0;"):
                with ui.element("div").classes(
                    "flex items-center gap-2 cursor-pointer select-none px-3 py-2 rounded-t"
                ).style("background: var(--q-color-background-secondary, #f5f5f5);") as _phdr:
                    _pchevron = ui.element("i").classes("ti ti-chevron-down").style("font-size: 14px; line-height: 1;")
                    ui.label("Permisos").style("font-size: 13px; font-weight: 600;")

                _pbody = ui.element("div").classes("w-full overflow-x-auto")
                with _pbody:
                    with ui.element("table").classes("border-collapse text-xs").style("width: 100%;"):
                        with ui.element("thead"):
                            with ui.element("tr").style(
                                "background: #2A7AC7; color: white; position: sticky; top: 0; z-index: 10;"
                            ):
                                with ui.element("th").classes("px-2 py-1 border text-left").style(
                                    "min-width: 72px; border-color: #4a9ad4; font-size: 11px;"
                                ):
                                    ui.label("Sección")
                                with ui.element("th").classes("px-2 py-1 border text-left").style(
                                    "min-width: 90px; border-color: #4a9ad4; font-size: 11px;"
                                ):
                                    ui.label("Página")
                                for _u in users_list:
                                    _uid = _u["id"]
                                    _uname = _u.get("username", "")
                                    _local  = _uname.split("@")[0] if "@" in _uname else _uname
                                    _domain = _uname.split("@")[1] if "@" in _uname else ""
                                    _ml_on  = _all_ml[_uid]
                                    _bdc_on = _all_bdc[_uid]
                                    with ui.element("th").classes("px-2 py-1 border text-center").style(
                                        "min-width: 115px; vertical-align: top; border-color: #4a9ad4;"
                                    ):
                                        ui.label(_local).style(
                                            "display:block; font-size:11px; font-weight:600; line-height:1.3;"
                                        )
                                        if _domain:
                                            ui.label(_domain).style(
                                                "display:block; font-size:9px; opacity:0.85; line-height:1.3;"
                                            )
                                        with ui.row().classes("justify-center items-center gap-1 flex-nowrap mt-1"):
                                            ui.element("span").style(
                                                f"display:inline-block;width:8px;height:8px;border-radius:50%;"
                                                f"background:{'#22c55e' if _ml_on else '#ef4444'};"
                                            )
                                            ui.label("ML").style("font-size:9px;")
                                            ui.element("span").style(
                                                f"display:inline-block;width:8px;height:8px;border-radius:50%;"
                                                f"background:{'#22c55e' if _bdc_on else '#ef4444'};"
                                            )
                                            ui.label("BDC").style("font-size:9px;")
                                        with ui.row().classes("justify-center gap-1 flex-nowrap mt-1"):
                                            ui.button(
                                                "Borrar",
                                                on_click=lambda _ui=_uid, _un=_uname: _do_delete_u(_ui, _un),
                                            ).props("flat dense no-caps").style("font-size:10px; color:#dc2626;")
                                            ui.button(
                                                "Reiniciar Pass",
                                                on_click=lambda _ui=_uid: _do_reset_u(_ui),
                                            ).props("flat dense no-caps").style("font-size:10px;")

                        with ui.element("tbody"):
                            for _row in _flat_rows:
                                with ui.element("tr").classes("border-t border-gray-200 hover:bg-gray-50"):
                                    if _row["section"] is not None:
                                        with ui.element("td").props(f'rowspan="{_row["sc"]}"').classes(
                                            "px-2 py-1 border border-gray-200 text-center"
                                        ).style(
                                            "background: var(--q-color-background-secondary, #f5f5f5); "
                                            "font-size: 10px; text-transform: uppercase; font-weight: 600; "
                                            "color: var(--q-color-text-secondary, #888); vertical-align: middle;"
                                        ):
                                            ui.label(_row["section"])
                                    with ui.element("td").classes("py-1 border-b border-gray-100").style(
                                        "padding-left: 10px; padding-right: 8px;"
                                    ):
                                        ui.label(_row["label"])
                                    for _u in users_list:
                                        _uid = _u["id"]
                                        _perms_u = _all_perms[_uid]
                                        with ui.element("td").classes("px-1 py-0 border-b border-gray-100 text-center"):
                                            _val = _perms_u.get(_row["key"], _row["key"] != "admin")
                                            _chk = ui.checkbox(value=_val).props("dense")
                                            _chk.on_value_change(
                                                lambda e, _ui=_uid, _tk=_row["key"]: _perm_toggle(_ui, _tk, e)
                                            )

                def _toggle_perms():
                    _popen["v"] = not _popen["v"]
                    _pbody.set_visibility(_popen["v"])
                    if _popen["v"]:
                        _pchevron.classes(remove="ti-chevron-right", add="ti-chevron-down")
                    else:
                        _pchevron.classes(remove="ti-chevron-down", add="ti-chevron-right")

                _phdr.on("click", _toggle_perms)

            ui.label("ML = MercadoLibre vinculado. BDC = QuickBooks vinculado. Marcá los checkboxes para permitir acceso a cada pestaña.").classes("text-xs text-gray-600")

            # Tarjeta Asignación QuickBooks
            with ui.card().classes("w-full p-3 bg-grey-2"):
                ui.label("Asignación QuickBooks").classes("text-base font-semibold mb-2")
                ui.label("Asignar Customer QB a un usuario habilita automáticamente las tabs Invoices y Compras.").classes("text-xs text-gray-600 mb-3")

                _qb_assign_users = get_all_users()
                _qb_user_options = {str(u["id"]): u.get("username", str(u["id"])) for u in _qb_assign_users}
                _qb_sel_uid: Dict[str, Any] = {"val": None}
                _qb_current_label: Any = {"ref": None}
                _qb_customers_container: Any = {"ref": None}

                with ui.row().classes("items-center gap-3 flex-wrap mb-2"):
                    qb_user_select = ui.select(
                        options=_qb_user_options,
                        label="Seleccionar usuario...",
                        with_input=True,
                    ).props("use-input input-debounce=0 clearable").classes("min-w-[280px]")

                    lbl_current = ui.label("").classes("text-sm text-gray-700")
                    _qb_current_label["ref"] = lbl_current

                def _on_qb_user_select(e: Any) -> None:
                    uid_str = str(e.value) if e.value is not None else None
                    _qb_sel_uid["val"] = uid_str
                    if not uid_str:
                        _qb_current_label["ref"].text = ""
                        return
                    try:
                        uid_int = int(uid_str)
                    except (ValueError, TypeError):
                        return
                    cust = get_user_qb_customer(uid_int)
                    if cust:
                        _qb_current_label["ref"].text = f"Customer actual: {cust.get('name', '””')} (id {cust.get('id', '””')})"
                    else:
                        _qb_current_label["ref"].text = "Sin customer asignado"

                qb_user_select.on_value_change(_on_qb_user_select)

                qb_customers_container = ui.column().classes("w-full gap-1 mt-2")
                _qb_customers_container["ref"] = qb_customers_container

                def _buscar_customers_qb() -> None:
                    qb_customers_container.clear()
                    data, err = _qb_raw_query(user["id"], "SELECT Id, DisplayName, PrimaryEmailAddr FROM Customer MAXRESULTS 100")
                    if err or not data:
                        with qb_customers_container:
                            ui.label(f"Error: {err or 'Sin datos'}").classes("text-negative text-sm")
                        return
                    customers = (data.get("QueryResponse") or {}).get("Customer") or []
                    if not customers:
                        with qb_customers_container:
                            ui.label("No se encontraron customers en QB.").classes("text-sm text-gray-600")
                        return
                    with qb_customers_container:
                        ui.label(f"{len(customers)} customers encontrados. Click en un cliente para asignarlo al usuario seleccionado.").classes("text-xs text-gray-500 mb-1")
                        with ui.element("table").classes("border-collapse text-xs w-full"):
                            with ui.element("thead"):
                                with ui.element("tr").classes("bg-gray-100"):
                                    for col_h in ["ID", "Nombre", "Email"]:
                                        with ui.element("th").classes("px-2 py-1 border text-left"):
                                            ui.label(col_h)
                                    with ui.element("th").classes("px-2 py-1 border text-center"):
                                        ui.label("Asignar")
                            with ui.element("tbody"):
                                for c in customers:
                                    cid = str(c.get("Id", ""))
                                    cname = str(c.get("DisplayName") or c.get("FullyQualifiedName") or "””")
                                    cemail_obj = c.get("PrimaryEmailAddr") or {}
                                    cemail = str(cemail_obj.get("Address") or "””") if isinstance(cemail_obj, dict) else "””"
                                    with ui.element("tr").classes("border-t border-gray-200 hover:bg-blue-50"):
                                        with ui.element("td").classes("px-2 py-1 border-b border-gray-100"):
                                            ui.label(cid)
                                        with ui.element("td").classes("px-2 py-1 border-b border-gray-100"):
                                            ui.label(cname)
                                        with ui.element("td").classes("px-2 py-1 border-b border-gray-100"):
                                            ui.label(cemail)
                                        with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                            def _asignar(cid_inner=cid, cname_inner=cname) -> None:
                                                uid_str = _qb_sel_uid["val"]
                                                if not uid_str:
                                                    ui.notify("Seleccioná un usuario primero", color="warning")
                                                    return
                                                try:
                                                    uid_int = int(uid_str)
                                                except (ValueError, TypeError):
                                                    ui.notify("Usuario inválido", color="negative")
                                                    return
                                                set_user_qb_customer(uid_int, cid_inner, cname_inner)
                                                _enable_tabs_for_user(uid_int, TABS_QB)
                                                # Copiar credenciales QB del admin si el usuario no las tiene
                                                if uid_int != 1:
                                                    creds_admin = get_qb_app_credentials(1)
                                                    creds_usuario = get_qb_app_credentials(uid_int)
                                                    if creds_admin and not creds_usuario:
                                                        set_qb_app_credentials(uid_int, creds_admin["client_id"], creds_admin["client_secret"], creds_admin.get("redirect_uri"))
                                                _qb_current_label["ref"].text = f"Customer actual: {cname_inner} (id {cid_inner})"
                                                ui.notify(f"Asignado {cname_inner} → usuario {uid_str}. Tabs QB habilitadas.", color="positive")
                                            ui.button("Asignar", on_click=_asignar).props("flat dense no-caps").classes("text-xs text-blue-600")

                ui.button("Buscar clientes en QB", on_click=_buscar_customers_qb, color="primary").props("dense no-caps")

            # Tarjetas Marcas y Despachantes lado a lado
            with ui.row().classes("w-full gap-6 flex-wrap"):
                # Tarjeta Marcas (catálogo global para Compras)
                with ui.column().classes("max-w-xl"):
                    marcas_table_container = ui.column().classes("w-full gap-2")

                    def _row_marca(m: Dict) -> None:
                        mid = m["id"]
                        nombre_actual = m.get("nombre", "")
                        with ui.element("tr").classes("border-t border-gray-200 hover:bg-gray-50"):
                            with ui.element("td").classes("px-3 py-1 border-b border-gray-100"):
                                inp = ui.input(value=nombre_actual).classes("w-full").props("dense")
                                def _on_enter(mid_inner=mid, inp_ref_inner=inp):
                                    nuevo = (inp_ref_inner.value or "").strip()
                                    if nuevo and nuevo != nombre_actual:
                                        err = update_marca(mid_inner, nuevo)
                                        if err:
                                            ui.notify(err, color="negative")
                                        else:
                                            ui.notify("Marca actualizada", color="positive")
                                            _refresh_marcas()
                                inp.on("keydown.enter", _on_enter)
                            with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                def _do_delete_marca(mid_inner: int):
                                    err = delete_marca(mid_inner)
                                    if err:
                                        ui.notify(err, color="negative")
                                    else:
                                        ui.notify("Marca eliminada", color="positive")
                                    _refresh_marcas()
                                ui.button("Borrar", on_click=lambda mid_inner=mid: _do_delete_marca(mid_inner)).props("flat dense").classes("text-xs text-red-600")

                    def _refresh_marcas() -> None:
                        marcas_table_container.clear()
                        with marcas_table_container:
                            marcas_data = get_marcas()

                            with ui.card().classes("w-full p-4 bg-grey-2"):
                                with ui.expansion(
                                    "Ver todas las marcas",
                                    icon="",
                                ).classes("w-full mb-2").props("expand-icon-toggle dense"):
                                    with ui.element("table").classes("border-collapse text-sm w-full").style("width: 100%; min-width: 300px"):
                                        with ui.element("thead"):
                                            with ui.element("tr").classes("bg-primary text-white font-semibold"):
                                                with ui.element("th").classes("px-3 py-2 border text-left"):
                                                    ui.label("Nombre")
                                                with ui.element("th").classes("px-2 py-2 border text-center").style("min-width: 80px"):
                                                    ui.label("Eliminar")
                                        with ui.element("tbody"):
                                            for m in marcas_data:
                                                _row_marca(m)
                                with ui.row().classes("gap-2 items-center mt-2"):
                                    inp_nueva = ui.input(placeholder="Nueva marca").props("dense")

                                    def _agregar():
                                        nombre = (inp_nueva.value or "").strip()
                                        if not nombre:
                                            ui.notify("Ingresá un nombre", color="warning")
                                            return
                                        err = insert_marca(nombre)
                                        if err:
                                            ui.notify(err, color="negative")
                                        else:
                                            inp_nueva.value = ""
                                            ui.notify("Marca agregada", color="positive")
                                            _refresh_marcas()

                                    ui.button("Agregar marca", on_click=_agregar, color="primary").props("dense no-caps")

                    _refresh_marcas()

                # Tarjeta Despachantes
                with ui.column().classes("max-w-xl"):
                    despachantes_table_container = ui.column().classes("w-full gap-2")

                    def _row_despachante(d: Dict) -> None:
                        did = d["id"]
                        nombre_actual = d.get("nombre", "")
                        with ui.element("tr").classes("border-t border-gray-200 hover:bg-gray-50"):
                            with ui.element("td").classes("px-3 py-1 border-b border-gray-100"):
                                inp = ui.input(value=nombre_actual).classes("w-full").props("dense")
                                def _on_enter(did_inner=did, inp_ref_inner=inp):
                                    nuevo = (inp_ref_inner.value or "").strip()
                                    if nuevo and nuevo != nombre_actual:
                                        err = update_despachante(did_inner, nuevo)
                                        if err:
                                            ui.notify(err, color="negative")
                                        else:
                                            ui.notify("Despachante actualizado", color="positive")
                                            _refresh_despachantes()
                                inp.on("keydown.enter", _on_enter)
                            with ui.element("td").classes("px-2 py-1 border-b border-gray-100 text-center"):
                                def _do_delete_despachante(did_inner: int):
                                    err = delete_despachante(did_inner)
                                    if err:
                                        ui.notify(err, color="negative")
                                    else:
                                        ui.notify("Despachante eliminado", color="positive")
                                        _refresh_despachantes()
                                ui.button("Borrar", on_click=lambda did_inner=did: _do_delete_despachante(did_inner)).props("flat dense").classes("text-xs text-red-600")

                    def _refresh_despachantes() -> None:
                        despachantes_table_container.clear()
                        with despachantes_table_container:
                            despachantes_data = get_despachantes()

                            with ui.card().classes("w-full p-4 bg-grey-2"):
                                with ui.expansion(
                                    "Ver todos los despachantes",
                                    icon="",
                                ).classes("w-full mb-2").props("expand-icon-toggle dense"):
                                    with ui.element("table").classes("border-collapse text-sm w-full").style("width: 100%; min-width: 300px"):
                                        with ui.element("thead"):
                                            with ui.element("tr").classes("bg-primary text-white font-semibold"):
                                                with ui.element("th").classes("px-3 py-2 border text-left"):
                                                    ui.label("Nombre")
                                                with ui.element("th").classes("px-2 py-2 border text-center").style("min-width: 80px"):
                                                    ui.label("Eliminar")
                                        with ui.element("tbody"):
                                            for d in despachantes_data:
                                                _row_despachante(d)
                                with ui.row().classes("gap-2 items-center mt-2"):
                                    inp_nuevo = ui.input(placeholder="Nuevo despachante").props("dense")

                                    def _agregar_desp():
                                        nombre = (inp_nuevo.value or "").strip()
                                        if not nombre:
                                            ui.notify("Ingresá un nombre", color="warning")
                                            return
                                        err = insert_despachante(nombre)
                                        if err:
                                            ui.notify(err, color="negative")
                                        else:
                                            inp_nuevo.value = ""
                                            ui.notify("Despachante agregado", color="positive")
                                            _refresh_despachantes()

                                    ui.button("Agregar despachante", on_click=_agregar_desp, color="primary").props("dense no-caps")

                    _refresh_despachantes()

            # Sección Sistema (solo user_id == 1)
            if user["id"] == 1:
                with ui.card().classes("w-full p-3").style(
                    "border: 1px solid #ccc; background: var(--q-color-background-secondary, #f5f5f5);"
                ):
                    ui.label("Sistema").style("font-size: 13px; font-weight: 600;").classes("mb-3")
                    with ui.row().classes("gap-3"):

                        async def _restart_service() -> None:
                            with ui.dialog() as dlg_svc:
                                dlg_svc.props("persistent")
                                with ui.card().classes("p-4 min-w-[340px]"):
                                    ui.label("¿Reiniciar el servicio PythonML?").classes("text-base font-semibold")
                                    ui.label("La app estará no disponible por ~5 segundos.").classes("text-sm text-gray-600 mt-1")
                                    with ui.row().classes("mt-4 gap-2 justify-end"):
                                        ui.button("Cancelar", on_click=dlg_svc.close).props("flat")
                                        def _confirm_svc():
                                            if user["id"] != 1:
                                                ui.notify("Sin permiso.", color="negative")
                                                return
                                            import subprocess, logging, threading
                                            logging.warning(f"[ADMIN-RESTART] user_id={user['id']} ejecutó: reiniciar servicio pythonml")
                                            dlg_svc.close()
                                            ui.notify("Servicio reiniciándose...", color="info")
                                            threading.Timer(0.5, lambda: subprocess.Popen(['/bin/systemctl', 'restart', 'pythonml'])).start()
                                        ui.button("Confirmar", on_click=_confirm_svc, color="primary")
                            dlg_svc.open()

                        ui.button("Reiniciar servicio PythonML", icon="ti-refresh", on_click=_restart_service).style(
                            "background: #185FA5; color: white;"
                        ).props("no-caps").tooltip("Reinicia solo la app (~5 segundos)")

                        async def _restart_droplet() -> None:
                            with ui.dialog() as dlg_drop:
                                dlg_drop.props("persistent")
                                with ui.card().classes("p-4 min-w-[380px]"):
                                    ui.label("⚠ ATENCIÓN ⚠").classes("text-lg font-bold text-red-600")
                                    ui.label(
                                        "Esto reiniciará el servidor COMPLETO de DigitalOcean.\n"
                                        "El sistema estará no disponible por 60-90 segundos.\n"
                                        "Todos los procesos se interrumpirán.\n\n"
                                        "¿Estás seguro?"
                                    ).classes("text-sm text-gray-700 mt-2 whitespace-pre-line")
                                    with ui.row().classes("mt-4 gap-2 justify-end"):
                                        ui.button("Cancelar", on_click=dlg_drop.close).props("flat")
                                        def _confirm_drop():
                                            if user["id"] != 1:
                                                ui.notify("Sin permiso.", color="negative")
                                                return
                                            import subprocess, logging, threading
                                            logging.warning(f"[ADMIN-RESTART] user_id={user['id']} ejecutó: reboot droplet")
                                            dlg_drop.close()
                                            ui.notify("Servidor reiniciándose... La página se va a desconectar.", color="warning")
                                            threading.Timer(0.5, lambda: subprocess.Popen(['/sbin/reboot'])).start()
                                        ui.button("Sí, reiniciar", on_click=_confirm_drop).style(
                                            "background: #A32D2D; color: white;"
                                        ).props("no-caps")
                            dlg_drop.open()

                        ui.button("Reiniciar servidor (droplet)", icon="ti-power", on_click=_restart_droplet).style(
                            "background: #A32D2D; color: white;"
                        ).props("no-caps").tooltip("Reinicia el sistema completo (~60-90 segundos)")


TABLA_CAMBIO_PA_DEFAULT = [{"valor": "$0"}, {"valor": "$100"}, {"valor": "$150"}, {"valor": "$200"}, {"valor": "$250"}, {"valor": "$300"}]
TABLA_DERECHOS_DEFAULT = [{"valor": "0,35"}, {"valor": "0,2"}, {"valor": "0,108"}, {"valor": "0"}]
TABLA_ESTADISTICAS_DEFAULT = [{"valor": "0"}, {"valor": "0,03"}]

TABLA_PESARIO_DEFAULT = [
    {"marca": "Amazon", "producto": "Echo Buds 2", "peso": "181", "fuente": "0", "total": "181"},
    {"marca": "Amazon", "producto": "Echo Pop", "peso": "270", "fuente": "115", "total": "385"},
    {"marca": "Amazon", "producto": "Echo Dot 5", "peso": "409", "fuente": "115", "total": "524"},
    {"marca": "Amazon", "producto": "Echo Spot", "peso": "502", "fuente": "115", "total": "617"},
    {"marca": "Amazon", "producto": "Echo Dot Max", "peso": "696", "fuente": "115", "total": "811"},
    {"marca": "Amazon", "producto": "Echo Show 5 3ra", "peso": "554", "fuente": "124", "total": "678"},
    {"marca": "Amazon", "producto": "Echo Show 8 3ra", "peso": "1325", "fuente": "124", "total": "1449"},
    {"marca": "Amazon", "producto": "Fire TV Stick Lite", "peso": "214", "fuente": "0", "total": "214"},
    {"marca": "Amazon", "producto": "Kindle 11\" 4253", "peso": "223", "fuente": "0", "total": "223"},
    {"marca": "Amazon", "producto": "Kindle 6\" 16GB 2024", "peso": "223", "fuente": "0", "total": "223"},
    {"marca": "Google", "producto": "Chromecast 4K", "peso": "243", "fuente": "30", "total": "273"},
    {"marca": "Google", "producto": "Chromecast HD", "peso": "242", "fuente": "30", "total": "272"},
    {"marca": "Google", "producto": "Google TV Streamer", "peso": "350", "fuente": "30", "total": "380"},
    {"marca": "Onn", "producto": "Onn 4K", "peso": "299", "fuente": "49", "total": "348"},
    {"marca": "Onn", "producto": "Onn 4K Plus", "peso": "288", "fuente": "49", "total": "337"},
    {"marca": "Onn", "producto": "Onn Full HD", "peso": "220", "fuente": "49", "total": "269"},
    {"marca": "Onn", "producto": "Tablet Surf 7\"", "peso": "424", "fuente": "49", "total": "473"},
    {"marca": "Roku", "producto": "Express 3960", "peso": "0", "fuente": "0", "total": "0"},
    {"marca": "Roku", "producto": "Premiere 4K 3920", "peso": "0", "fuente": "0", "total": "0"},
    {"marca": "Roku", "producto": "Streaming Stick 3840", "peso": "161", "fuente": "0", "total": "161"},
    {"marca": "Roku", "producto": "Streaming Stick 4K", "peso": "0", "fuente": "0", "total": "0"},
    {"marca": "JBL", "producto": "Flip 7", "peso": "788", "fuente": "0", "total": "788"},
    {"marca": "JBL", "producto": "Go 4", "peso": "320", "fuente": "0", "total": "320"},
    {"marca": "JBL", "producto": "Charge 6", "peso": "1338", "fuente": "0", "total": "1338"},
    {"marca": "JBL", "producto": "Tune 720", "peso": "450", "fuente": "0", "total": "450"},
    {"marca": "JBL", "producto": "520C On Ear", "peso": "330", "fuente": "0", "total": "330"},
    {"marca": "JBL", "producto": "Endurance Run 3", "peso": "50", "fuente": "0", "total": "50"},
    {"marca": "Samsung", "producto": "SSD 970 Evo Plus", "peso": "82", "fuente": "0", "total": "82"},
    {"marca": "Samsung", "producto": "SSD 980 Pro", "peso": "72", "fuente": "0", "total": "72"},
    {"marca": "Samsung", "producto": "SSD 990 Evo", "peso": "83", "fuente": "0", "total": "83"},
    {"marca": "Xiaomi", "producto": "Mini Speaker", "peso": "43", "fuente": "0", "total": "43"},
    {"marca": "Xiaomi", "producto": "Mi Smart Scale 2", "peso": "1472", "fuente": "0", "total": "1472"},
    {"marca": "Xiaomi", "producto": "MI TV Stick 2k - MDZ-24", "peso": "220", "fuente": "0", "total": "220"},
    {"marca": "Xiaomi", "producto": "MI TV Stick 4k - MDZ-27", "peso": "260", "fuente": "0", "total": "260"},
    {"marca": "Xiaomi", "producto": "Redemi Buds 4 Lite", "peso": "76", "fuente": "0", "total": "76"},
    {"marca": "Xiaomi", "producto": "Redmi Buds 3", "peso": "92", "fuente": "0", "total": "92"},
    {"marca": "Xiaomi", "producto": "Redmi Buds Essential", "peso": "71", "fuente": "0", "total": "71"},
    {"marca": "Xiaomi", "producto": "Redmi Pad Pro 12\"", "peso": "942", "fuente": "100", "total": "1042"},
    {"marca": "Xiaomi", "producto": "Redmi Pad SE 11\"", "peso": "735", "fuente": "100", "total": "835"},
    {"marca": "Xiaomi", "producto": "Redmi Pad SE 8,7\"", "peso": "507", "fuente": "100", "total": "607"},
    {"marca": "Xiaomi", "producto": "Redmi Watch 2 Lite", "peso": "202", "fuente": "0", "total": "202"},
    {"marca": "Xiaomi", "producto": "Redmi Watch 3", "peso": "186", "fuente": "0", "total": "186"},
    {"marca": "Xiaomi", "producto": "Redmi Watch 5 Active", "peso": "124", "fuente": "0", "total": "124"},
    {"marca": "Xiaomi", "producto": "Redmi Watch 5 Lite", "peso": "123", "fuente": "0", "total": "123"},
    {"marca": "Xiaomi", "producto": "Smart Band 7", "peso": "112", "fuente": "0", "total": "112"},
    {"marca": "Xiaomi", "producto": "Smart Band 9 Active", "peso": "98", "fuente": "0", "total": "98"},
    {"marca": "Xiaomi", "producto": "TV Box S 3ra - MDZ-32", "peso": "370", "fuente": "0", "total": "370"},
    {"marca": "Xiaomi", "producto": "TV Box S 2da - MDZ-28", "peso": "415", "fuente": "0", "total": "415"},
]

TABLA_POSICION_DEFAULT = [
    {"posicion": "Cambio PA", "seguro": "0.02", "flete": "0.030", "derechos": "0.000", "estadisticas": "0", "iva": "0.105", "despachante": "0.214", "cambio_pa": "1"},
    {"posicion": "10,5I + 0D + 0E", "seguro": "0.02", "flete": "0.030", "derechos": "0.000", "estadisticas": "0", "iva": "0.105", "despachante": "0.214", "cambio_pa": "0"},
    {"posicion": "10,5I + 10,8D + 0E", "seguro": "0.02", "flete": "0.030", "derechos": "0.108", "estadisticas": "0", "iva": "0.105", "despachante": "0.214", "cambio_pa": "0"},
    {"posicion": "10,5I + 16D + 0E", "seguro": "0.02", "flete": "0.030", "derechos": "0.160", "estadisticas": "0", "iva": "0.105", "despachante": "0.214", "cambio_pa": "0"},
    {"posicion": "21I + 0D + 0E", "seguro": "0.02", "flete": "0.030", "derechos": "0.000", "estadisticas": "0", "iva": "0.21", "despachante": "0.214", "cambio_pa": "0"},
    {"posicion": "21I + 20D + 3E", "seguro": "0.02", "flete": "0.030", "derechos": "0.200", "estadisticas": "0.03", "iva": "0.21", "despachante": "0.214", "cambio_pa": "0"},
    {"posicion": "21I + 35D + 3E", "seguro": "0.02", "flete": "0.030", "derechos": "0.350", "estadisticas": "0.03", "iva": "0.21", "despachante": "0.214", "cambio_pa": "0"},
]


TABLA_COURIER_DEFAULT = [
    {"courier": "Mia LHS",     "posicion": "Cambio PA",          "valor_kg": "13.50", "descuento": "1.33267522", "kg_real": "10.13", "almacenaje": "1.80", "seguro": "24.75", "res_3244": "10.00", "gas_ope": "27.00", "env_dom": "10.00", "iibb": "0.03", "cif": "0"},
    {"courier": "Mia Rosario", "posicion": "21I + 20D + 3E",     "valor_kg": "26.00", "descuento": "1",          "kg_real": "22.00", "almacenaje": "0",    "seguro": "0",     "res_3244": "0",     "gas_ope": "0",     "env_dom": "0",     "iibb": "0",    "cif": "0.7$+0.01%"},
    {"courier": "Mia Richard", "posicion": "10,5I + 10,8D + 0E", "valor_kg": "9.50",  "descuento": "1",          "kg_real": "9.50",  "almacenaje": "1.90", "seguro": "29.75", "res_3244": "5.00",  "gas_ope": "25.00", "env_dom": "10.00", "iibb": "0",    "cif": "3$+2%"},
    {"courier": "China",       "posicion": "10,5I + 0D + 0E",    "valor_kg": "27.00", "descuento": "1.33267522", "kg_real": "20.26", "almacenaje": "2.70", "seguro": "29.35", "res_3244": "10.00", "gas_ope": "27.00", "env_dom": "10.00", "iibb": "0.03", "cif": "0"},
    {"courier": "Mia Sixtar",  "posicion": "Cambio PA",          "valor_kg": "7.00",  "descuento": "1",          "kg_real": "7.00",  "almacenaje": "1.2102", "seguro": "37.00", "res_3244": "6.05",  "gas_ope": "139.77", "env_dom": "22.00", "iibb": "0",    "cif": "3$+2%"},
]

TABLA_IVA_VS_EXENTO_DEFAULT: List[Dict[str, Any]] = []


