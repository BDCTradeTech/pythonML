"""
Fase 3 — tabs/config.py
Pestaña Configuración: MercadoLibre, QuickBooks, estadísticas, contraseña, BD.
"""
from __future__ import annotations

import asyncio
import json
import os
import tempfile
from datetime import datetime
from typing import Any, Dict, Optional

from nicegui import app, ui

from auth import update_user_password
from db import (
    get_connection,
    get_cotizador_param,
    set_cotizador_param,
    get_ml_app_credentials,
    set_ml_app_credentials,
    get_qb_app_credentials,
    set_qb_app_credentials,
    get_user_qb_customer,
    export_user_db_data,
    import_user_db_data,
    set_app_config,
)
from qb_api import fetch_qb_customer_detail


def _require_login() -> Optional[Dict[str, Any]]:
    user = app.storage.user.get("user")
    if not user:
        ui.notify("Debes iniciar sesión para continuar", color="negative")
    return user


def build_tab_config() -> None:
    user = _require_login()
    if not user:
        return

    # MercadoLibre + Estado de la cuenta (tarjeta combinada)
    app_creds = get_ml_app_credentials(user["id"])
    default_redirect = os.getenv("ML_REDIRECT_URI", "http://localhost:8083/ml/callback")
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM ml_credentials WHERE user_id = ?", (user["id"],))
    ml_creds = cur.fetchone()
    conn.close()

    # Layout: [ ML | QB ] misma fila, mismo ancho. [ Est | Contraseña | BD ] columna angosta
    _card_class = "w-full p-3 overflow-auto"
    with ui.column().classes("w-full gap-4 p-4"):
        ui.label("Configuración").classes("text-xl font-bold text-gray-800 mb-2")
        with ui.row().classes("w-full gap-4 items-stretch flex-wrap"):
            # 1. MercadoLibre — mismo ancho que QB (ancho fijo para igualar)
            with ui.column().classes("w-[400px] flex-shrink-0 gap-3"):
                with ui.card().classes(_card_class):
                    def _desvincular_ml_impl() -> None:
                        conn = get_connection()
                        try:
                            cur = conn.cursor()
                            cur.execute("DELETE FROM ml_credentials WHERE user_id = ?", (user["id"],))
                            conn.commit()
                        finally:
                            conn.close()
                        ui.notify("Cuenta desvinculada", color="positive")
                        ui.navigate.reload()

                    ui.label("MercadoLibre").classes("text-base font-semibold mb-2")
                    ui.label("App ID, Client Secret, Redirect → Guardar → Conectar").classes("text-xs text-gray-600 mb-1")
                    with ui.expansion("Credenciales", icon="key").classes("w-full").props("expand-icon-toggle dense"):
                        inp_client_id = ui.input("App ID", value=app_creds["client_id"] if app_creds else "").classes("w-full").props("type=password password-toggle dense")
                        inp_client_secret = ui.input("Client Secret", value=app_creds["client_secret"] if app_creds else "").classes("w-full").props("type=password password-toggle dense")
                        inp_redirect = ui.input("Redirect URI", value=(app_creds.get("redirect_uri") or "").strip() or default_redirect if app_creds else default_redirect).classes("w-full").props("type=password password-toggle dense")

                    def guardar_app_ml() -> None:
                        cid = (inp_client_id.value or "").strip()
                        csec = (inp_client_secret.value or "").strip()
                        redir = (inp_redirect.value or "").strip() or default_redirect
                        if not cid or not csec:
                            ui.notify("Ingresá App ID y Client Secret", color="warning")
                            return
                        set_ml_app_credentials(user["id"], cid, csec, redir or None)
                        ui.notify("Credenciales guardadas correctamente", color="positive")

                    def conectar_ml() -> None:
                        cid = (inp_client_id.value or "").strip()
                        csec = (inp_client_secret.value or "").strip()
                        redir = (inp_redirect.value or "").strip() or default_redirect
                        if not cid or not csec:
                            ui.notify("Ingresá App ID y Client Secret y guardá antes de conectar", color="warning")
                            return
                        set_ml_app_credentials(user["id"], cid, csec, redir or None)
                        from urllib.parse import quote
                        scope = quote("offline_access read write")
                        auth_url = f"https://auth.mercadolibre.com.ar/authorization?response_type=code&client_id={cid}&redirect_uri={quote(redir)}&scope={scope}"
                        ui.navigate.to(auth_url)

                    with ui.row().classes("gap-2 mt-2"):
                        ui.button("Guardar credenciales", on_click=guardar_app_ml, color="primary").props("dense no-caps")
                        if ml_creds:
                            ui.button("Desvincular cuenta", on_click=_desvincular_ml_impl, color="secondary").props("dense no-caps")
                        else:
                            ui.button("Conectar cuenta", on_click=conectar_ml, color="secondary").props("dense no-caps")

                    ui.separator().classes("my-2")
                    ui.label("Estado").classes("text-xs font-semibold mb-1")
                    if ml_creds:
                        with ui.row().classes("items-center gap-2"):
                            ui.icon("check_circle", color="positive", size="sm")
                            ui.label("Vinculada").classes("text-positive text-sm")
                        if ml_creds["expires_at"]:
                            try:
                                exp = ml_creds["expires_at"][:19].replace("T", " ")
                                ui.label(f"Token vence: {exp}").classes("text-xs text-gray-600")
                            except Exception:
                                pass
                    else:
                        ui.label("Sin vincular").classes("text-warning text-sm")

            # 1b. IA / Groq
            _gkey_row = None
            try:
                _gc = get_connection()
                _gkey_row = _gc.execute(
                    "SELECT value, updated_at FROM app_config WHERE key = ?",
                    ("groq_api_key",),
                ).fetchone()
                _gc.close()
            except Exception:
                pass
            with ui.column().classes("w-[400px] flex-shrink-0"):
                with ui.card().classes(_card_class):
                    def _desvincular_groq() -> None:
                        _conn = get_connection()
                        try:
                            _conn.execute("DELETE FROM app_config WHERE key = 'groq_api_key'")
                            _conn.commit()
                        finally:
                            _conn.close()
                        ui.notify("API Key desvinculada", color="positive")
                        ui.navigate.reload()

                    ui.label("IA / Groq").classes("text-base font-semibold mb-2")
                    ui.label("Groq API Key para sugerencias automáticas en Preguntas.").classes("text-xs text-gray-600 mb-1")
                    groq_inp = (
                        ui.input(placeholder="gsk_...")
                        .props("dense outlined hide-bottom-space type=password password-toggle")
                        .classes("w-full mt-1")
                    )

                    def _vincular_groq() -> None:
                        val = str(groq_inp.value or "").strip()
                        if not val:
                            ui.notify("Ingresá una API Key válida", type="warning")
                            return
                        set_app_config("groq_api_key", val)
                        groq_inp.value = ""
                        ui.notify("API Key guardada", type="positive")
                        ui.navigate.reload()

                    with ui.row().classes("gap-2 mt-2"):
                        ui.button("Vincular", on_click=_vincular_groq, color="primary").props("dense no-caps")
                        if _gkey_row and _gkey_row["value"]:
                            ui.button("Desvincular", on_click=_desvincular_groq, color="secondary").props("dense no-caps")

                    ui.separator().classes("my-2")
                    ui.label("Estado").classes("text-xs font-semibold mb-1")
                    if _gkey_row and _gkey_row["value"]:
                        with ui.row().classes("items-center gap-2"):
                            ui.icon("check_circle", color="positive", size="sm")
                            ui.label("Vinculada").classes("text-positive text-sm")
                        _ua = str(_gkey_row["updated_at"] or "")[:10]
                        if _ua:
                            ui.label(f"Actualizada: {_ua}").classes("text-xs text-gray-600")
                    else:
                        ui.label("Sin vincular").classes("text-warning text-sm")

            # 2. QuickBooks — un poco más largo que ML para igualar tamaño
            with ui.column().classes("w-[420px] flex-shrink-0"):
                with ui.card().classes(_card_class):
                    ui.label("QuickBooks").classes("text-base font-semibold mb-2")
                    ui.label("Client ID, Secret, Redirect → Guardar credenciales → Conectar cuenta").classes("text-xs text-gray-600 mb-1")
                    qb_app_creds = get_qb_app_credentials(user["id"])
                    ml_redir = os.getenv("ML_REDIRECT_URI", "http://localhost:8083/ml/callback")
                    default_qb_redirect = os.getenv("QB_REDIRECT_URI") or (
                        ml_redir.replace("/ml/callback", "/qb/callback") if "/ml/callback" in ml_redir else "http://localhost:8083/qb/callback"
                    )
                    with ui.expansion("Credenciales QB", icon="account_balance").classes("w-full").props("expand-icon-toggle dense"):
                        inp_qb_cid = ui.input("Client ID", value=qb_app_creds["client_id"] if qb_app_creds else "").classes("w-full").props("type=password password-toggle dense")
                        inp_qb_csec = ui.input("Client Secret", value=qb_app_creds["client_secret"] if qb_app_creds else "").classes("w-full").props("type=password password-toggle dense")
                        inp_qb_redir = ui.input("Redirect URI", value=(qb_app_creds.get("redirect_uri") or "").strip() or default_qb_redirect if qb_app_creds else default_qb_redirect).classes("w-full").props("type=password password-toggle dense")
                        async def _usar_url_actual_qb():
                            try:
                                origin = await ui.run_javascript("window.location.origin")
                                if origin:
                                    inp_qb_redir.value = f"{origin}/qb/callback"
                                    ui.notify(f"Redirect URI: {inp_qb_redir.value}", color="positive")
                            except Exception:
                                ui.notify("No se pudo detectar la URL actual", color="warning")
                        ui.button("Usar URL actual", on_click=_usar_url_actual_qb, color="secondary").props("dense no-caps flat")

                    def guardar_qb_creds() -> None:
                        cid = (inp_qb_cid.value or "").strip()
                        csec = (inp_qb_csec.value or "").strip()
                        redir = (inp_qb_redir.value or "").strip() or default_qb_redirect
                        if not cid or not csec:
                            ui.notify("Ingresá Client ID y Client Secret", color="warning")
                            return
                        set_qb_app_credentials(user["id"], cid, csec, redir or None)
                        ui.notify("Credenciales guardadas", color="positive")

                    def conectar_qb() -> None:
                        cid = (inp_qb_cid.value or "").strip()
                        csec = (inp_qb_csec.value or "").strip()
                        redir = (inp_qb_redir.value or "").strip() or default_qb_redirect
                        if not cid or not csec:
                            ui.notify("Guardá credenciales antes de conectar", color="warning")
                            return
                        set_qb_app_credentials(user["id"], cid, csec, redir or None)
                        from urllib.parse import quote
                        scope = quote("com.intuit.quickbooks.accounting")
                        state = f"qb_{user['id']}"
                        # Intuit: redirect_uri debe coincidir EXACTAMENTE con developer.intuit.com (Keys)
                        # Probar codificación mínima (preserva :/) por si Intuit la prefiere
                        redir_encoded = quote(redir, safe=':/')
                        auth_url = f"https://appcenter.intuit.com/connect/oauth2?response_type=code&client_id={cid}&redirect_uri={redir_encoded}&scope={scope}&state={quote(state)}"
                        ui.navigate.to(auth_url)

                    conn = get_connection()
                    cur = conn.cursor()
                    cur.execute("SELECT * FROM qb_tokens WHERE user_id = ? ORDER BY id DESC LIMIT 1", (user["id"],))
                    qb_tokens = cur.fetchone()
                    conn.close()

                    def desvincular_qb() -> None:
                        conn = get_connection()
                        try:
                            cur = conn.cursor()
                            cur.execute("DELETE FROM qb_tokens WHERE user_id = ?", (user["id"],))
                            conn.commit()
                        finally:
                            conn.close()
                        ui.notify("QuickBooks desvinculado", color="positive")
                        ui.navigate.reload()

                    with ui.row().classes("gap-1 mt-1"):
                        ui.button("Guardar credenciales", on_click=guardar_qb_creds, color="primary").props("dense no-caps")
                        if qb_tokens and qb_tokens["access_token"]:
                            ui.button("Desvincular cuenta", on_click=desvincular_qb, color="secondary").props("dense no-caps")
                        else:
                            ui.button("Conectar cuenta", on_click=conectar_qb, color="secondary").props("dense no-caps")

                    ui.separator().classes("my-2")
                    ui.label("Estado").classes("text-xs font-semibold mb-1")
                    if qb_tokens and qb_tokens["access_token"]:
                        with ui.row().classes("items-center gap-2"):
                            ui.icon("check_circle", color="positive", size="xs")
                            ui.label("Vinculada").classes("text-positive text-sm")
                        if qb_tokens["expires_at"]:
                            try:
                                exp = qb_tokens["expires_at"][:19].replace("T", " ")
                                ui.label(f"Token vence: {exp}").classes("text-xs text-gray-600")
                            except Exception:
                                pass
                        ui.separator().classes("my-2")
                        ui.label("Soy el cliente (en QuickBooks):").classes("text-xs font-semibold mb-1")
                        qb_cust = get_user_qb_customer(user["id"])
                        cust_id = qb_cust.get("id", "") if qb_cust else ""
                        cust_name = "—"
                        if qb_cust and cust_id:
                            cust_detail, _ = fetch_qb_customer_detail(user["id"], cust_id)
                            cust_name = (cust_detail.get("DisplayName") or cust_detail.get("FullyQualifiedName") or cust_detail.get("CompanyName") or "").strip() if cust_detail else (qb_cust.get("name") or "—")
                            if not cust_name:
                                cust_name = qb_cust.get("name", "—")
                        cust_nombre = f"{cust_name} (id {cust_id})" if cust_id else "—"
                        ui.label(cust_nombre).classes("text-sm text-gray-800")
                    else:
                        ui.label("Sin vincular").classes("text-warning text-sm")

            # 3. Estadísticas, Contraseña, Base de datos — apiladas, mismo ancho que ML y QB
            with ui.column().classes("w-[420px] flex-shrink-0 gap-3"):
                with ui.card().classes(_card_class):
                    ui.label("Estadísticas").classes("text-sm font-semibold mb-1")
                    ui.label("Ventas a cargar (órdenes)").classes("text-xs text-gray-600 mb-1")
                    ui.label("Cantidad máxima de órdenes de MercadoLibre a traer en cada actualización. Más órdenes = datos más completos, pero la carga puede tardar más.").classes("text-xs text-gray-500 mb-1")
                    limit_actual = get_cotizador_param("estadisticas_limit_ordenes", user["id"]) or "1000"
                    opts_ventas = {"300": "300", "500": "500", "1000": "1000", "2000": "2000", "3000": "3000", "4000": "4000", "5000": "5000", "7500": "7500", "10000": "10000"}
                    sel_ventas = ui.select(opts_ventas, value=limit_actual, label="").classes("w-full").props("dense")

                    def guardar_limit_ventas() -> None:
                        val = str(sel_ventas.value or "1000").strip()
                        if val not in opts_ventas:
                            val = "1000"
                        set_cotizador_param("estadisticas_limit_ordenes", val, user["id"])
                        ui.notify("Guardado", color="positive")
                    ui.button("Guardar", on_click=guardar_limit_ventas, color="primary").classes("mt-1").props("dense no-caps")

                # Cambiar contraseña (debajo de Estadísticas)
                with ui.card().classes(_card_class):
                    with ui.expansion("Cambiar contraseña", icon="lock").classes("w-full").props("expand-icon-toggle dense"):
                        inp_actual = ui.input("Contraseña actual").classes("w-full").props("type=password password-toggle dense")
                        inp_nueva = ui.input("Nueva contraseña (mín. 4)").classes("w-full").props("type=password password-toggle dense")
                        inp_confirmar = ui.input("Confirmar nueva").classes("w-full").props("type=password password-toggle dense")

                        def cambiar_clave() -> None:
                            actual = (inp_actual.value or "").strip()
                            nueva = (inp_nueva.value or "").strip()
                            confirmar = (inp_confirmar.value or "").strip()
                            if not actual:
                                ui.notify("Ingresá tu contraseña actual", color="warning")
                                return
                            if nueva != confirmar:
                                ui.notify("La nueva contraseña y la confirmación no coinciden", color="negative")
                                return
                            error = update_user_password(user["id"], actual, nueva)
                            if error:
                                ui.notify(error, color="negative")
                                return
                            ui.notify("Contraseña cambiada correctamente", color="positive")
                            inp_actual.value = ""
                            inp_nueva.value = ""
                            inp_confirmar.value = ""

                        ui.button("Cambiar contraseña", on_click=cambiar_clave, color="primary").classes("mt-1").props("dense no-caps")

                # Base de datos
                with ui.card().classes(_card_class) as db_card:
                    ui.label("Base de datos").classes("text-sm font-semibold mb-1")
                    ui.label("Backup: productos, cotizador, importación. Sin credenciales.").classes("text-xs text-gray-600 mb-2")

                    def descargar_backup() -> None:
                        try:
                            content = export_user_db_data(user["id"])
                            fd, path = tempfile.mkstemp(suffix=".json")
                            os.write(fd, content)
                            os.close(fd)
                            nombre = f"backup_{user.get('username', 'user')}_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
                            with db_card:
                                ui.download(path, nombre)
                            ui.notify("Descarga iniciada", color="positive")
                            def _cleanup() -> None:
                                try:
                                    if path and os.path.exists(path):
                                        os.unlink(path)
                                except Exception:
                                    pass
                            ui.timer(5.0, _cleanup, once=True)
                        except Exception as ex:
                            ui.notify(f"Error: {ex}", color="negative")

                    def _formatear_fecha_backup(data: dict) -> str:
                        fd = data.get("fecha_descarga")
                        if fd:
                            return str(fd)
                        ea = data.get("exported_at", "")
                        if ea:
                            try:
                                d = datetime.fromisoformat(ea.replace("Z", "+00:00"))
                                return d.strftime("%Y-%m-%d %H:%M")
                            except Exception:
                                pass
                        return ea or "fecha desconocida"

                    async def on_upload(e) -> None:
                        try:
                            file_obj = getattr(e, "file", None) or (getattr(e, "files", [None])[0] if getattr(e, "files", None) else None) or getattr(e, "content", None)
                            if file_obj is None:
                                ui.notify("Error: no se pudo leer el archivo", color="negative")
                                upload_component.reset()
                                return
                            read_result = file_obj.read()
                            content = await read_result if asyncio.iscoroutine(read_result) else read_result
                            if isinstance(content, str):
                                content = content.encode("utf-8")
                            data = json.loads(content.decode("utf-8"))
                            if not isinstance(data, dict):
                                ui.notify("Error: archivo inválido", color="negative")
                                upload_component.reset()
                                return
                            fecha_str = _formatear_fecha_backup(data)
                            content_bytes = bytes(content)

                            def make_confirm(cb: bytes, upload_el):
                                def confirmar_sobrescribir() -> None:
                                    dlg.close()
                                    msg = import_user_db_data(user["id"], cb)
                                    if msg == "ok":
                                        ui.notify("Backup restaurado correctamente", color="positive")
                                        upload_el.reset()
                                    else:
                                        ui.notify(f"Error al restaurar: {msg}", color="negative")
                                        upload_el.reset()
                                return confirmar_sobrescribir

                            with ui.dialog().props("persistent") as dlg:
                                dlg.classes("w-full max-w-md")
                                with ui.card().classes("w-full p-6 gap-4"):
                                    ui.label("¿Sobrescribir datos?").classes("text-lg font-semibold")
                                    ui.label(
                                        f"¿Estás seguro que querés sobrescribir los datos actuales con el backup del {fecha_str}?"
                                    ).classes("text-gray-600")
                                    with ui.row().classes("w-full justify-end gap-2 pt-2"):
                                        def cancelar_y_reset() -> None:
                                            dlg.close()
                                            upload_component.reset()
                                        ui.button("Cancelar", on_click=cancelar_y_reset, color="secondary").props("flat")
                                        ui.button("Confirmar", on_click=make_confirm(content_bytes, upload_component), color="negative").props("unelevated")
                                dlg.open()
                        except json.JSONDecodeError as ex:
                            ui.notify(f"Error: archivo JSON inválido - {ex}", color="negative")
                            upload_component.reset()
                        except Exception as ex:
                            ui.notify(f"Error: {str(ex)}", color="negative")
                            upload_component.reset()

                    def handle_multi(e) -> None:
                        if getattr(e, "files", None) and len(e.files) > 0:
                            class _FakeEv:
                                file = e.files[0]
                            asyncio.create_task(on_upload(_FakeEv()))

                    upload_component = ui.upload(
                        on_upload=on_upload,
                        max_files=1,
                        max_file_size=10_000_000,
                        auto_upload=True,
                    ).props("accept=.json").classes("hidden")
                    with ui.row().classes("gap-3 items-center w-full"):
                        ui.button("Descargar backup", on_click=descargar_backup, color="primary").classes("flex-1").props("icon=download unelevated no-caps")
                        ui.button("Cargar backup", on_click=lambda: upload_component.run_method("pickFiles"), color="secondary").classes("flex-1").props("icon=upload outline no-caps")
