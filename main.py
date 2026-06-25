from __future__ import annotations

import asyncio

# Polyfill asyncio.to_thread para Python 3.8 (agregado en 3.9). Evita AttributeError en Históricos y otras búsquedas.
if not hasattr(asyncio, "to_thread"):
    def _to_thread_compat(fn, *args, **kwargs):
        import functools
        loop = asyncio.get_running_loop()
        return loop.run_in_executor(None, functools.partial(fn, *args, **kwargs))
    asyncio.to_thread = lambda fn, *args, **kwargs: _to_thread_compat(fn, *args, **kwargs)

import base64
import bcrypt
from cryptography.fernet import Fernet
import hashlib
import logging
import re
import unicodedata

logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
import html
import json
import sqlite3
import calendar
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import os
import sys
import secrets
import socket
import ssl
import smtplib
import subprocess
import tempfile
import threading
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Border, Side, Alignment
import time
import requests
from dotenv import load_dotenv
from fastapi import Request
from fastapi.responses import RedirectResponse
from nicegui import app, background_tasks, context, run, ui

# --- Compatibilidad Fase 1: funciones ML movidas a ml_api.py ---
from ml_api import (
    get_ml_access_token,
    _parse_ml_item_body, _cuotas_desde_item, _body_to_precios_item,
    _tipo_publicacion_desde_item, _extraer_color_desde_texto,
    ml_get_my_items, ml_update_item_price, ml_get_one_item_full,
    ml_get_item_sale_price, ml_get_item_sale_price_full,
    ml_get_item_price_to_win, ml_get_item_performance,
    ml_get_promotion_item_discounts, ml_get_promotion_item_discounts_by_user,
    ml_get_promotion_item_discounts_by_campaign,
    ml_get_item_prices, ml_enriquecer_sale_price, ml_fetch_price_for_item,
    ml_get_product_detail, ml_get_item_description, ml_get_item,
    ml_get_items_multiget, ml_get_items_multiget_with_attributes, ml_get_items_multiget_all,
    ml_get_users_multiget, ml_get_user_id, ml_get_user_profile,
    ml_get_orders, ml_get_shipments_today, ml_search_similar,
)

# --- Compatibilidad Fase 1: funciones QB movidas a qb_api.py ---
from qb_api import (
    _refresh_qb_token_if_needed,
    fetch_qb_customers, _qb_raw_query, fetch_qb_company_info,
    fetch_qb_vendors, fetch_qb_bills, fetch_qb_items,
    fetch_qb_items_search, fetch_qb_item_history,
    fetch_qb_customer_detail, fetch_qb_invoice_pdf, fetch_qb_item_by_id,
    fetch_qb_invoices, fetch_qb_invoice_detail,
    patch_invoice_pdf_line_items,
)

# --- Compatibilidad Fase 1: funciones de auth movidas a auth.py ---
from auth import (
    hash_password, _is_bcrypt_hash, _verify_password,
    send_email, get_user_email,
    create_user, authenticate_user,
    update_user_password, admin_reset_user_password,
    delete_user_and_all_data,
)

# --- Compatibilidad Fase 1: funciones de DB movidas a db.py ---
from db import (
    get_connection, init_db, save_query,
    get_ml_app_credentials, set_ml_app_credentials,
    get_qb_app_credentials, set_qb_app_credentials,
    get_qb_tokens, get_user_qb_customer, set_user_qb_customer,
    get_setting, set_setting,
    get_cotizador_param, set_cotizador_param, delete_cotizador_param,
    get_cotizador_tabla, set_cotizador_tabla,
    list_users_excluding, get_all_users,
    get_user_tab_permissions, set_user_tab_permission,
    user_can_access_tab,
    get_compras_lista, get_compras_lista_all, get_compras_lista_row,
    insert_compras_lista, update_compras_lista_row, delete_compras_lista_row,
    get_pedidos_lista, insert_pedidos_lista, update_pedidos_lista_row, delete_pedidos_lista_row,
    get_marcas, insert_marca, update_marca, delete_marca,
    get_despachantes, insert_despachante, update_despachante, delete_despachante,
    get_invoice_extras, upsert_invoice_extra,
    copy_cotizador_datos,
    get_importacion_filas, save_importacion_filas,
    export_user_db_data, import_user_db_data,
    COTIZADOR_DEFAULTS,
    _enable_tabs_for_user,
)

# --- Fase 3: tabs extraídos a módulos separados ---
from tabs.pedidos import build_tab_pedidos
from tabs.estadisticas import build_tab_estadisticas
from tabs.config import build_tab_config
from tabs.compras import build_tab_compras
from tabs.ventas import build_tab_ventas
from tabs.cuotas import build_tab_cuotas
from tabs.precios import build_tab_precios
from tabs.stock import build_tab_stock
from tabs.balance import build_tab_balance
from tabs.busqueda import build_tab_busqueda
from tabs.admin import build_tab_admin
from tabs.importacion import build_tab_importacion
from tabs.historicos import build_tab_historicos
from tabs.pesos import build_tab_pesos
from tabs.arca import build_tab_arca
from tabs.dashboard import build_tab_dashboard
from tabs.datos import build_tab_datos
from tabs.flex import build_tab_flex
from tabs.promos import build_tab_promos
from tabs.preguntas import build_tab_preguntas
from tabs.misc import build_tab_comparar_precios, build_tab_historial_precios, build_tab_competencia
from tabs.constants import TAB_KEYS, TABS_BASE, TABS_ML, TABS_QB, TAB_DESCRIPTIONS, LABEL_BY_TAB
from tabs.home import build_tab_home_welcome
from tabs.compras_lista import build_tab_compras_lista
from tabs.activity import build_tab_actividad
from tabs.guias import build_tab_guias
from helpers.activity_logger import log_event

DB_PATH = Path(__file__).with_name("app.db")

# Versión del sistema: formato 2.aa.mm.dd.hh (aa=año, mm=mes, dd=día, hh=hora 00-23). Ej.: 2.26.04.14.12
VERSION = "3.26.06.25.09"


# ==========================
# ENCRIPTACIÓN DE SECRETS
# ==========================


def _get_fernet() -> Fernet:
    key = os.getenv("CREDENTIAL_ENCRYPTION_KEY", "")
    if not key:
        raise RuntimeError("CREDENTIAL_ENCRYPTION_KEY no configurado. Ver .env.example")
    return Fernet(key.encode())


def _encrypt_secret(plain: str) -> str:
    return _get_fernet().encrypt(plain.encode()).decode()


def _decrypt_secret(token: str) -> str:
    if not token.startswith("gAAAAA"):
        return token  # plaintext legacy: aún no migrado
    return _get_fernet().decrypt(token.encode()).decode()


# ==========================
# SESIÓN DE USUARIO (NiceGUI)
# ==========================


def get_current_user() -> Optional[Dict[str, Any]]:
    return app.storage.user.get("user")  # type: ignore[no-any-return]


def set_current_user(user: Optional[Dict[str, Any]]) -> None:
    if user:
        app.storage.user["user"] = user
    else:
        app.storage.user.clear()


def require_login() -> Optional[Dict[str, Any]]:
    user = get_current_user()
    if not user:
        ui.notify("Debes iniciar sesión para continuar", color="negative")
    return user


# ==========================
# INTERFAZ NICEGUI
# ==========================


def show_login_screen(container) -> None:
    """Muestra la pantalla de login/registro dentro de un contenedor."""
    container.clear()

    with container:
        # Fila a ancho completo, contenido centrado horizontalmente y más cerca del borde superior
        with ui.row().classes("w-full justify-center q-mt-xl"):
            with ui.column().classes("items-center gap-6"):
                ui.label("BDC systems").classes("text-3xl font-bold")

                with ui.card().classes("w-full max-w-md"):
                    ui.label("Iniciar sesión").classes("text-xl font-semibold mb-4")
                    username = ui.input("Usuario").classes("w-full")
                    password = ui.input(
                        "Contraseña",
                        password=True,
                        password_toggle_button=True,
                    ).classes("w-full")

                    with ui.row().classes("justify-between w-full mt-4"):
                        def on_login() -> None:
                            if not username.value or not password.value:
                                ui.notify("Completa usuario y contraseña", color="negative")
                                return
                            user = authenticate_user(username.value, password.value)
                            if not user:
                                ui.notify("Credenciales inválidas", color="negative")
                                return
                            set_current_user(user)
                            ui.notify(f"Bienvenido {user['username']}", color="positive")
                            show_main_layout(container)

                        def on_register() -> None:
                            with ui.dialog() as dlg:
                                dlg.props("persistent")
                                with ui.card().classes("p-4 min-w-[320px]"):
                                    ui.label("Registrarse").classes("text-lg font-bold")
                                    reg_email = ui.input("Email").classes("w-full").props("type=email")

                                    def _submit_reg() -> None:
                                        e = (reg_email.value or "").strip()
                                        if not e or "@" not in e:
                                            ui.notify("Ingresá un email válido", color="negative")
                                            return
                                        err, new_pwd = create_user(e)
                                        if err:
                                            if new_pwd:
                                                dlg.close()
                                                with ui.dialog() as popup:
                                                    popup.props("persistent")
                                                    with ui.card().classes("p-6 min-w-[400px]"):
                                                        ui.label("Error al enviar el email").classes("text-lg font-semibold text-warning")
                                                        ui.label(err).classes("text-sm text-gray-600 mt-2")
                                                        ui.label("Tu contraseña provisoria (copiala para iniciar sesión):").classes("text-sm font-medium mt-4")
                                                        with ui.row().classes("mt-2 p-3 bg-gray-100 rounded font-mono text-lg select-all"):
                                                            ui.label(new_pwd)
                                                        ui.button("Cerrar popup", on_click=popup.close).props("flat color=primary").classes("mt-4")
                                                popup.open()
                                            else:
                                                ui.notify(err, color="negative")
                                            return
                                        dlg.close()
                                        ui.notify(
                                            "Te enviamos un email con tu contraseña provisoria. Iniciá sesión y cambiá tu contraseña en Configuración.",
                                            color="positive",
                                        )

                                    with ui.row().classes("mt-3 gap-2 justify-end"):
                                        ui.button("Cancelar", on_click=dlg.close)
                                        ui.button("Registrarme", on_click=_submit_reg, color="primary")
                            dlg.open()

                        ui.button("Entrar", on_click=on_login, color="primary")
                        ui.button("Registrarme", on_click=on_register, color="secondary")


def show_main_layout(container) -> None:
    """Muestra el panel principal dentro de un contenedor."""
    container.clear()
    user = get_current_user()

    if not user:
        show_login_screen(container)
        return

    with container:
        perms = get_user_tab_permissions(user["id"])
        ml_linked = bool(get_ml_access_token(user["id"]))
        qb_tokens = get_qb_tokens(user["id"])
        qb_linked = bool(qb_tokens and qb_tokens.get("access_token"))

        # Tabs ocultos (solo para binding con tab_panels)
        with ui.element("div").classes("hidden"):
            with ui.tabs() as tabs:
                tab_home = ui.tab("Home")
                tab_estadisticas = ui.tab("Estadísticas")
                tab_ventas = ui.tab("Ventas")
                tab_precios = ui.tab("Productos")
                tab_cuotas = ui.tab("Cuotas")
                tab_compras = ui.tab("Invoices")
                tab_stock = ui.tab("Stock")
                tab_compras_lista = ui.tab("Compras")
                tab_pedidos = ui.tab("Pedidos")
                tab_historicos = ui.tab("Históricos")
                tab_busqueda = ui.tab("Búsqueda")
                tab_importacion = ui.tab("Importacion")
                tab_guias = ui.tab("Guias")
                tab_datos = ui.tab("Datos")
                tab_pesos = ui.tab("Pesos")
                tab_arca = ui.tab("ARCA")
                tab_balance    = ui.tab("Balance")
                tab_dashboard  = ui.tab("Dashboard")
                tab_promos     = ui.tab("Promos")
                tab_preguntas  = ui.tab("Preguntas")
                tab_flex       = ui.tab("Flex")
                tab_config = ui.tab("Configuración")
                tab_admin = ui.tab("Admin")
                tab_actividad = ui.tab("Actividad")

        tab_map = {
            "Home": tab_home,
            "Estadísticas": tab_estadisticas,
            "Ventas": tab_ventas,
            "Productos": tab_precios,
            "Cuotas": tab_cuotas,
            "Invoices": tab_compras,
            "Stock": tab_stock,
            "Compras": tab_compras_lista,
            "Pedidos": tab_pedidos,
            "Históricos": tab_historicos,
            "Búsqueda": tab_busqueda,
            "Importacion": tab_importacion,
            "Guias": tab_guias,
            "Datos": tab_datos,
            "Pesos": tab_pesos,
            "ARCA": tab_arca,
            "Balance":    tab_balance,
            "Dashboard":  tab_dashboard,
            "Promos":     tab_promos,
            "Preguntas":  tab_preguntas,
            "Flex":       tab_flex,
            "Configuración": tab_config,
            "Admin": tab_admin,
            "Actividad": tab_actividad,
        }
        label_to_key = {"Home": "home", "Estadísticas": "estadisticas", "Ventas": "ventas", "Productos": "productos", "Cuotas": "cuotas", "Promos": "promos", "Preguntas": "preguntas", "Flex": "flex", "Invoices": "compras", "Stock": "stock", "Compras": "compras_lista", "Pedidos": "pedidos", "Históricos": "historicos", "Búsqueda": "busqueda", "Importacion": "importacion", "Guias": "guias", "Datos": "datos", "Pesos": "pesos", "ARCA": "arca", "Balance": "balance", "Dashboard": "dashboard", "Configuración": "configuracion", "Admin": "admin", "Actividad": "actividad"}

        # Lazy-load state
        precios_cargado = [False]
        ventas_cargado = [False]
        estadisticas_cargado = [False]
        balance_cargado   = [False]
        dashboard_cargado = [False]
        compras_cargado = [False]
        stock_cargado = [False]
        compras_lista_cargado = [False]
        pedidos_cargado = [False]
        historicos_cargado = [False]
        admin_cargado = [False]
        cuotas_cargado = [False]
        promos_cargado = [False]
        preguntas_cargado = [False]
        arca_cargado = [False]
        actividad_cargado = [False]
        guias_cargado = [False]

        def _lazy_load(val: str) -> None:
            if val == "Invoices" and not compras_cargado[0]:
                compras_cargado[0] = True
                build_tab_compras(compras_container)
            elif val == "Stock" and not stock_cargado[0]:
                stock_cargado[0] = True
                build_tab_stock(stock_container)
            elif val == "Compras" and not compras_lista_cargado[0]:
                compras_lista_cargado[0] = True
                build_tab_compras_lista(compras_lista_container)
            elif val == "Pedidos" and not pedidos_cargado[0]:
                pedidos_cargado[0] = True
                build_tab_pedidos(pedidos_container)
            elif val == "Productos" and not precios_cargado[0]:
                precios_cargado[0] = True
                build_tab_precios(precios_container)
            elif val == "Cuotas" and not cuotas_cargado[0]:
                cuotas_cargado[0] = True
                build_tab_cuotas(cuotas_container)
            elif val == "Promos" and not promos_cargado[0]:
                promos_cargado[0] = True
                build_tab_promos(promos_container)
            elif val == "Preguntas" and not preguntas_cargado[0]:
                preguntas_cargado[0] = True
                build_tab_preguntas(preguntas_container)
            elif val == "Ventas" and not ventas_cargado[0]:
                ventas_cargado[0] = True
                build_tab_ventas(ventas_container)
            elif val == "Estadísticas" and not estadisticas_cargado[0]:
                estadisticas_cargado[0] = True
                build_tab_estadisticas(estadisticas_container)
            elif val == "Balance" and not balance_cargado[0]:
                balance_cargado[0] = True
                build_tab_balance(balance_container)
            elif val == "Dashboard" and not dashboard_cargado[0]:
                dashboard_cargado[0] = True
                build_tab_dashboard(dashboard_container, navigate_to)
            elif val == "Históricos" and not historicos_cargado[0]:
                historicos_cargado[0] = True
                build_tab_historicos(historicos_container)
            elif val == "Admin" and not admin_cargado[0]:
                admin_cargado[0] = True
                build_tab_admin(admin_container)
            elif val == "ARCA" and not arca_cargado[0]:
                arca_cargado[0] = True
                build_tab_arca(arca_container)
            elif val == "Actividad" and not actividad_cargado[0]:
                actividad_cargado[0] = True
                build_tab_actividad(actividad_container)
            elif val == "Guias" and not guias_cargado[0]:
                guias_cargado[0] = True
                with guias_container:
                    build_tab_guias()

        # Siempre arrancar en Home
        tab_inicial = "Home"

        def _go(lbl: str):
            def f():
                tab_panels.value = tab_map[lbl]
                app.storage.user["last_tab"] = lbl
                _lazy_load(lbl)
            return f

        def navigate_to(lbl: str) -> None:
            _go(lbl)()

        # Barra gris: navegación principal + secundaria | semáforos, versión, usuario
        # Menús secundarios se abren al pasar el mouse (hover). No se cierran al mover hacia los items.
        # Se cierran al seleccionar una opción o al hacer clic fuera (Quasar).
        _open_menus: List[Any] = []  # Referencias a menús abiertos para cerrar otros al abrir uno nuevo

        def _open_and_close_others(menu_obj: Any) -> None:
            for m in _open_menus:
                if m is not menu_obj:
                    try:
                        m.close()
                    except Exception:
                        pass
            _open_menus.clear()
            _open_menus.append(menu_obj)
            menu_obj.open()

        with ui.row().classes("w-full items-center q-pa-md bg-grey-2 gap-2 flex-wrap"):
            with ui.row().classes("items-center gap-1 flex-wrap"):
                _nav_font = "text-lg font-medium"
                if perms.get("home", True):
                    ui.button("HOME", on_click=_go("Home")).props("flat dense no-caps").classes(_nav_font)
                ml_subs = [("DASHBOARD", "Dashboard", "dashboard"), ("ESTADÍSTICAS", "Estadísticas", "estadisticas"), ("VENTAS", "Ventas", "ventas"), ("PRODUCTOS", "Productos", "productos"), ("CUOTAS", "Cuotas", "cuotas"), ("PROMOS", "Promos", "promos"), ("PREGUNTAS", "Preguntas", "preguntas"), ("FLEX", "Flex", "flex"), ("BÚSQUEDA", "Búsqueda", "busqueda"), ("BALANCE", "Balance", "balance")]
                if any(perms.get(k, True) for _, _, k in ml_subs):
                    with ui.element("div").classes("relative inline-block").on("mouseenter", lambda: _open_and_close_others(ml_menu)):
                        with ui.button("MERCADOLIBRE").props("flat dense no-caps").classes(_nav_font):
                            with ui.menu().props("auto-close content-class=text-lg") as ml_menu:
                                for lbl_display, lbl_map, key in ml_subs:
                                    if perms.get(key, True):
                                        def _ml_click(l=lbl_map):
                                            _lazy_load(l)
                                            tab_panels.value = tab_map[l]
                                            app.storage.user["last_tab"] = l
                                        ui.menu_item(lbl_display, _ml_click)
                if perms.get("compras", True) or perms.get("stock", True) or perms.get("compras_lista", True) or perms.get("pedidos", True) or perms.get("historicos", True):
                    with ui.element("div").classes("relative inline-block").on("mouseenter", lambda: _open_and_close_others(compras_menu)):
                        with ui.button("BDC").props("flat dense no-caps").classes(_nav_font):
                            with ui.menu().props("auto-close content-class=text-lg") as compras_menu:
                                if perms.get("compras", True):
                                    def _compras_click():
                                        _lazy_load("Invoices")
                                        tab_panels.value = tab_compras
                                        app.storage.user["last_tab"] = "Invoices"
                                    ui.menu_item("INVOICES", _compras_click)
                                if perms.get("stock", True):
                                    def _stock_click():
                                        _lazy_load("Stock")
                                        tab_panels.value = tab_stock
                                        app.storage.user["last_tab"] = "Stock"
                                    ui.menu_item("STOCK", _stock_click)
                                if perms.get("compras_lista", True):
                                    def _compras_lista_click():
                                        _lazy_load("Compras")
                                        tab_panels.value = tab_compras_lista
                                        app.storage.user["last_tab"] = "Compras"
                                    ui.menu_item("COMPRAS", _compras_lista_click)
                                if perms.get("pedidos", True):
                                    def _pedidos_click():
                                        _lazy_load("Pedidos")
                                        tab_panels.value = tab_pedidos
                                        app.storage.user["last_tab"] = "Pedidos"
                                    ui.menu_item("PEDIDOS", _pedidos_click)
                                if perms.get("historicos", True):
                                    def _historicos_click():
                                        _lazy_load("Históricos")
                                        tab_panels.value = tab_historicos
                                        app.storage.user["last_tab"] = "Históricos"
                                    ui.menu_item("HISTÓRICOS", _historicos_click)
                if perms.get("importacion", True) or perms.get("pesos", True) or perms.get("guias", True):
                    with ui.element("div").classes("relative inline-block").on("mouseenter", lambda: _open_and_close_others(comex_menu)):
                        with ui.button("COMEX").props("flat dense no-caps").classes(_nav_font):
                            with ui.menu().props("auto-close content-class=text-lg") as comex_menu:
                                if perms.get("importacion", True):
                                    def _imp_click():
                                        _lazy_load("Importacion")
                                        tab_panels.value = tab_importacion
                                        app.storage.user["last_tab"] = "Importacion"
                                    ui.menu_item("IMPORTACION", _imp_click)
                                if perms.get("guias", True):
                                    def _guias_click():
                                        _lazy_load("Guias")
                                        tab_panels.value = tab_guias
                                        app.storage.user["last_tab"] = "Guias"
                                    ui.menu_item("GUÍAS", _guias_click)
                                if perms.get("pesos", True):
                                    def _pesos_click():
                                        _lazy_load("Pesos")
                                        tab_panels.value = tab_pesos
                                        app.storage.user["last_tab"] = "Pesos"
                                    ui.menu_item("PESOS", _pesos_click)
                if perms.get("arca", True):
                    with ui.element("div").classes("relative inline-block").on("mouseenter", lambda: _open_and_close_others(impuestos_menu)):
                        with ui.button("IMPUESTOS").props("flat dense no-caps").classes(_nav_font):
                            with ui.menu().props("auto-close content-class=text-lg") as impuestos_menu:
                                def _arca_click():
                                    _lazy_load("ARCA")
                                    tab_panels.value = tab_arca
                                    app.storage.user["last_tab"] = "ARCA"
                                ui.menu_item("ARCA", _arca_click)
                if perms.get("datos", True) or perms.get("configuracion", True):
                    with ui.element("div").classes("relative inline-block").on("mouseenter", lambda: _open_and_close_others(config_menu)):
                        with ui.button("CONFIG").props("flat dense no-caps").classes(_nav_font):
                            with ui.menu().props("auto-close content-class=text-lg") as config_menu:
                                if perms.get("datos", True):
                                    def _datos_click():
                                        _lazy_load("Datos")
                                        tab_panels.value = tab_datos
                                        app.storage.user["last_tab"] = "Datos"
                                    ui.menu_item("DATOS", _datos_click)
                                if perms.get("configuracion", True):
                                    def _config_click():
                                        _lazy_load("Configuración")
                                        tab_panels.value = tab_config
                                        app.storage.user["last_tab"] = "Configuración"
                                    ui.menu_item("CONFIGURACIÓN", _config_click)
                if perms.get("admin", False):
                    with ui.element("div").classes("relative inline-block").on("mouseenter", lambda: _open_and_close_others(admin_menu)):
                        with ui.button("ADMIN").props("flat dense no-caps").classes(_nav_font):
                            with ui.menu().props("auto-close content-class=text-lg") as admin_menu:
                                ui.menu_item("PERMISOS", _go("Admin"))
                                if perms.get("actividad", False):
                                    ui.menu_item("ACTIVIDAD", _go("Actividad"))
            ui.space()
            with ui.row().classes("items-center gap-3 flex-wrap"):
                with ui.row().classes("items-center gap-2"):
                    ui.element("span").classes("w-2.5 h-2.5 rounded-full").style(f"background:{'#22c55e' if ml_linked else '#ef4444'}")
                    ui.label("ML").classes("text-xs text-gray-600")
                with ui.row().classes("items-center gap-2"):
                    ui.element("span").classes("w-2.5 h-2.5 rounded-full").style(f"background:{'#22c55e' if qb_linked else '#ef4444'}")
                    ui.label("BDC").classes("text-xs text-gray-600")
                ui.label(f"Ver {VERSION}").classes("text-sm text-gray-600")
                ui.label(user["username"]).classes("text-sm font-medium")
                def logout() -> None:
                    set_current_user(None)
                    ui.notify("Sesión cerrada", color="positive")
                    show_login_screen(container)
                ui.button("Cerrar sesión", on_click=logout, color="negative").props("flat dense")

        # Breadcrumb
        _BREADCRUMB_MAP = {
            "Home":          ("", "Inicio"),
            "Dashboard":     ("MercadoLibre", "Dashboard"),
            "Estadísticas":  ("MercadoLibre", "Estadísticas"),
            "Ventas":        ("MercadoLibre", "Ventas"),
            "Productos":     ("MercadoLibre", "Productos"),
            "Cuotas":        ("MercadoLibre", "Cuotas"),
            "Promos":        ("MercadoLibre", "Promos"),
            "Flex":          ("MercadoLibre", "Flex"),
            "Búsqueda":      ("MercadoLibre", "Búsqueda"),
            "Balance":       ("MercadoLibre", "Balance"),
            "Preguntas":     ("MercadoLibre", "Preguntas"),
            "Invoices":      ("BDC", "Compras"),
            "Stock":         ("BDC", "Stock"),
            "Compras":       ("BDC", "Lista Compras"),
            "Pedidos":       ("BDC", "Pedidos"),
            "Históricos":    ("BDC", "Históricos"),
            "Importacion":   ("Comex", "Importación"),
            "Guias":         ("Comex", "Guías"),
            "Pesos":         ("Comex", "Pesos"),
            "ARCA":          ("Impuestos", "ARCA"),
            "Datos":         ("Config", "Datos"),
            "Configuración": ("Config", "Configuración"),
            "Admin":         ("Config", "Admin"),
            "Actividad":     ("Config", "Actividad"),
        }

        def _breadcrumb_text(tab: str) -> str:
            seccion, nombre = _BREADCRUMB_MAP.get(tab or "Home", ("", "Inicio"))
            if not seccion:
                return "🏠  Inicio"
            return f"🏠  {seccion}  ›  {nombre}"

        with ui.row().style("width:100%;background:#f1f5f9;border-bottom:0.5px solid #e0e2e7;padding:4px 16px;align-items:center;min-height:28px"):
            ui.label().bind_text_from(app.storage.user, "last_tab", backward=_breadcrumb_text).style("font-size:13px;color:#475569")

        tab_panels = ui.tab_panels(tabs, value=tab_map.get(tab_inicial, tab_home)).classes("w-full")

        with tab_panels:
            with ui.tab_panel(tab_home):
                home_welcome_container = ui.column().classes("w-full")
            build_tab_home_welcome(home_welcome_container)
            with ui.tab_panel(tab_estadisticas):
                estadisticas_container = ui.column().classes("w-full")

            with ui.tab_panel(tab_ventas):
                ventas_container = ui.column().classes("w-full")

            with ui.tab_panel(tab_precios):
                precios_container = ui.column().classes("w-full")

            with ui.tab_panel(tab_compras):
                compras_container = ui.column().classes("w-full")

            with ui.tab_panel(tab_stock):
                stock_container = ui.column().classes("w-full")

            with ui.tab_panel(tab_compras_lista):
                compras_lista_container = ui.column().classes("w-full")

            with ui.tab_panel(tab_pedidos):
                pedidos_container = ui.column().classes("w-full")

            with ui.tab_panel(tab_historicos):
                historicos_container = ui.column().classes("w-full")

            with ui.tab_panel(tab_busqueda):
                build_tab_busqueda()

            with ui.tab_panel(tab_importacion):
                build_tab_importacion()

            with ui.tab_panel(tab_guias):
                guias_container = ui.column().classes("w-full")

            with ui.tab_panel(tab_datos):
                build_tab_datos()

            with ui.tab_panel(tab_pesos):
                build_tab_pesos()

            with ui.tab_panel(tab_arca):
                arca_container = ui.column().classes("w-full")

            with ui.tab_panel(tab_balance):
                balance_container = ui.column().classes("w-full")

            with ui.tab_panel(tab_dashboard):
                dashboard_container = ui.column().classes("w-full")

            with ui.tab_panel(tab_cuotas):
                cuotas_container = ui.column().classes("w-full")

            with ui.tab_panel(tab_promos):
                promos_container = ui.column().classes("w-full")

            with ui.tab_panel(tab_preguntas):
                preguntas_container = ui.column().classes("w-full")

            with ui.tab_panel(tab_flex):
                build_tab_flex()

            with ui.tab_panel(tab_config):
                build_tab_config()

            with ui.tab_panel(tab_admin):
                admin_container = ui.column().classes("w-full")

            with ui.tab_panel(tab_actividad):
                actividad_container = ui.column().classes("w-full")

        tab_enter_times: Dict[str, Any] = {}

        def on_tab_change(e) -> None:
            new_val = getattr(e, "value", None)
            old_val = app.storage.user.get("last_tab")
            if old_val and old_val != new_val and old_val in tab_enter_times:
                elapsed = int((datetime.now() - tab_enter_times[old_val]).total_seconds())
                if elapsed >= 1:
                    log_event(user["id"], old_val, "time_spent", tiempo_segundos=elapsed)
                del tab_enter_times[old_val]
            if new_val:
                app.storage.user["last_tab"] = new_val
                tab_enter_times[new_val] = datetime.now()
                log_event(user["id"], new_val, "page_view")
                _lazy_load(new_val)

        tab_panels.on_value_change(on_tab_change)
        tab_enter_times["Home"] = datetime.now()
        log_event(user["id"], "Home", "page_view")


def _get_base_url(request: Request) -> str:
    """Obtiene la URL base del request (para redirect_uri). Soporta proxy con X-Forwarded-*."""
    forwarded_proto = request.headers.get("X-Forwarded-Proto")
    forwarded_host = request.headers.get("X-Forwarded-Host")
    if forwarded_host:
        scheme = (forwarded_proto or "https").rstrip("/")
        return f"{scheme}://{forwarded_host.split(',')[0].strip()}"
    if request.url:
        return str(request.base_url).rstrip("/")
    return "http://localhost:8083"


async def _ml_callback_redirect(request: Request) -> RedirectResponse:
    """Ruta HTTP directa: redirige a / con el code para que la página principal procese el OAuth."""
    code = request.query_params.get("code")
    error_param = request.query_params.get("error")
    error_desc = request.query_params.get("error_description", "")
    # Pasar la URL recibida para depurar cuando falta el code
    url_recibida = str(request.url) if request.url else ""
    if error_param:
        return RedirectResponse(url=f"/?ml_oauth_error={error_param}&ml_oauth_error_desc={error_desc}", status_code=302)
    if code:
        return RedirectResponse(url=f"/?ml_oauth_code={code}", status_code=302)
    # No vino code: pasar la URL para mostrarla en el mensaje de error
    from urllib.parse import quote
    return RedirectResponse(
        url=f"/?ml_oauth_error=no_code&ml_oauth_error_desc={quote(url_recibida[:200])}",
        status_code=302,
    )


# Registrar la ruta ANTES de las páginas para que responda a GET /ml/callback
app.add_api_route("/ml/callback", _ml_callback_redirect, methods=["GET"])


async def _qb_callback_redirect(request: Request) -> RedirectResponse:
    """Callback OAuth de QuickBooks: redirige a / con el code para procesar el token. Usa URL absoluta para mantener el host (IP o ngrok)."""
    base = _get_base_url(request)
    code = request.query_params.get("code")
    realm_id = request.query_params.get("realmId")
    state = request.query_params.get("state")
    error_param = request.query_params.get("error")
    error_desc = request.query_params.get("error_description", "")
    if error_param:
        from urllib.parse import quote
        return RedirectResponse(url=f"{base}/?qb_oauth_error={error_param}&qb_oauth_error_desc={quote(error_desc[:300])}", status_code=302)
    if code:
        params = f"qb_oauth_code={code}"
        if realm_id:
            params += f"&qb_realm_id={realm_id}"
        if state:
            params += f"&qb_state={state}"
        return RedirectResponse(url=f"{base}/?{params}", status_code=302)
    from urllib.parse import quote
    url_recibida = str(request.url) if request.url else ""
    return RedirectResponse(url=f"{base}/?qb_oauth_error=no_code&qb_oauth_error_desc={quote(url_recibida[:200])}", status_code=302)


app.add_api_route("/qb/callback", _qb_callback_redirect, methods=["GET"])


# ==========================
# ARRANQUE DE LA APP
# ==========================


@ui.page("/")
def index(request: Request) -> None:  # type: ignore[override]
    ui.add_head_html('<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@tabler/icons-webfont@latest/tabler-icons.min.css">')
    root = ui.column().classes("w-full")

    # Procesar callback de OAuth
    ml_code = request.query_params.get("ml_oauth_code")
    ml_error = request.query_params.get("ml_oauth_error")
    qb_oauth_code = request.query_params.get("qb_oauth_code")
    qb_oauth_error = request.query_params.get("qb_oauth_error")
    qb_realm_id = request.query_params.get("qb_realm_id", "")
    if qb_oauth_error:
        with root:
            ui.label(f"âŒ Error de QuickBooks: {qb_oauth_error}").classes("text-negative text-lg mb-4")
            if request.query_params.get("qb_oauth_error_desc"):
                from urllib.parse import unquote
                desc = unquote(request.query_params.get("qb_oauth_error_desc", ""))
                ui.label(f"Detalle: {desc}").classes("text-sm text-gray-600 mb-2")
            ui.link("Volver al inicio", "/").classes("text-primary")
        return
    if ml_error:
        with root:
            ui.label(f"âŒ Error de MercadoLibre: {ml_error}").classes("text-negative text-lg mb-4")
            if request.query_params.get("ml_oauth_error_desc"):
                from urllib.parse import unquote
                desc = unquote(request.query_params.get("ml_oauth_error_desc", ""))
                ui.label(f"URL recibida: {desc}").classes("text-sm text-gray-600 mb-2")
            if ml_error == "no_code":
                ui.label(
                    "El parámetro 'code' no llegó al servidor. Posibles causas:\n"
                    "• Ngrok: si viste la página 'Visit Site', haz clic ahí y vuelve a intentar.\n"
                    "• Redirect URI: en MercadoLibre Developers debe ser EXACTAMENTE la misma URL que en tu .env (con /ml/callback).\n"
                    "• Prueba en ventana de incógnito o con otro navegador."
                ).classes("text-gray-600 mb-4 whitespace-pre-line")
            ui.link("Volver al inicio", "/").classes("text-primary")
        return
    if ml_code:
        user = get_current_user()
        if not user:
            with root:
                ui.label("Debes iniciar sesión en BDC systems antes de vincular MercadoLibre.").classes("text-lg mb-4")
                ui.link("Ir a inicio de sesión", "/").classes("text-primary")
            return
        app_creds = get_ml_app_credentials(user["id"])
        if app_creds:
            client_id = app_creds["client_id"]
            client_secret = app_creds["client_secret"]
            redirect_uri = app_creds.get("redirect_uri") or os.getenv("ML_REDIRECT_URI", "http://localhost:8083/ml/callback")
        else:
            client_id = os.getenv("ML_CLIENT_ID")
            client_secret = os.getenv("ML_CLIENT_SECRET")
            redirect_uri = os.getenv("ML_REDIRECT_URI", "http://localhost:8083/ml/callback")
        if not client_id or not client_secret:
            with root:
                ui.label("âŒ Configurá tu App ID y Client Secret en Configuración antes de conectar.").classes("text-negative mb-4")
            return
        redirect_uri = (redirect_uri or "").strip() or "http://localhost:8083/ml/callback"
        try:
            resp = requests.post(
                "https://api.mercadolibre.com/oauth/token",
                data={
                    "grant_type": "authorization_code",
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "code": ml_code,
                    "redirect_uri": redirect_uri,
                },
                headers={"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"},
                timeout=10,
            )
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            resp_err = getattr(e, "response", None)
            err_msg = str(e)
            try:
                if resp_err is not None:
                    err_body = resp_err.json()
                    err_msg = err_body.get("message") or err_body.get("error") or str(err_body)
            except Exception:
                if resp_err is not None and resp_err.text:
                    err_msg = resp_err.text[:500]
            with root:
                ui.label(f"âŒ Error al obtener token: {e}").classes("text-negative text-lg mb-2")
                ui.label(f"Detalle: {err_msg}").classes("text-sm text-gray-600 mb-2")
                causas = (
                    "Posibles causas:\n"
                    "• redirect_uri debe coincidir EXACTAMENTE con el configurado en MercadoLibre Developers.\n"
                    "• Si tu app tiene PKCE habilitado, desactivá PKCE en la app o recreá la app sin PKCE.\n"
                    "• El código de autorización se usa una sola vez; si recargaste la página, volvé a Conectar."
                )
                if "invalid" in err_msg.lower() or "validating grant" in err_msg.lower():
                    causas += (
                        "\n\nâš ï¸ ¿Intentabas conectar QuickBooks? Si es así, el Redirect URI en developer.intuit.com debe ser /qb/callback, NO /ml/callback. Cada app (ML y QB) tiene su propia URL."
                    )
                ui.label(causas).classes("text-sm text-gray-600 mb-4 whitespace-pre-line")
                ui.link("Volver a Configuración", "/").classes("text-primary")
            return
        except Exception as e:
            with root:
                ui.label(f"âŒ Error al obtener token: {e}").classes("text-negative mb-4")
            return
        data = resp.json()
        access_token = data.get("access_token")
        refresh_token = data.get("refresh_token")
        expires_in = data.get("expires_in")
        if not access_token:
            with root:
                ui.label(f"âŒ Respuesta inesperada: {data}").classes("text-negative mb-4")
            return
        expires_at = None
        if isinstance(expires_in, (int, float)):
            expires_at = (datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=int(expires_in))).isoformat()
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM ml_credentials WHERE user_id = ?", (user["id"],))
        cur.execute(
            "INSERT INTO ml_credentials (user_id, access_token, refresh_token, expires_at, raw_data) VALUES (?, ?, ?, ?, ?)",
            (user["id"], access_token, refresh_token, expires_at, json.dumps(data, ensure_ascii=False)),
        )
        conn.commit()
        conn.close()
        _enable_tabs_for_user(user["id"], TABS_ML)
        # Guardar nickname de ML (para activity_log)
        try:
            me_r = requests.get(
                "https://api.mercadolibre.com/users/me",
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=5,
            )
            if me_r.ok:
                nickname = me_r.json().get("nickname") or ""
                if nickname:
                    from db import set_ml_nickname
                    set_ml_nickname(user["id"], nickname)
        except Exception:
            pass
        # Redirigir a / sin el code para limpiar la URL (el usuario verá el panel y una notificación)
        return RedirectResponse(url="/", status_code=302)

    if qb_oauth_code:
        user = get_current_user()
        if not user:
            with root:
                ui.label("Debes iniciar sesión en BDC systems antes de vincular QuickBooks.").classes("text-lg mb-4")
                ui.link("Ir a inicio de sesión", "/").classes("text-primary")
            return
        qb_app_creds = get_qb_app_credentials(user["id"])
        if not qb_app_creds:
            with root:
                ui.label("âŒ Configurá Client ID y Client Secret de QuickBooks en Configuración antes de conectar.").classes("text-negative mb-4")
                ui.link("Volver a Configuración", "/").classes("text-primary")
            return
        client_id = qb_app_creds["client_id"]
        client_secret = qb_app_creds["client_secret"]
        base_url = _get_base_url(request)
        redirect_uri = base_url.rstrip("/") + "/qb/callback"
        auth_str = f"{client_id}:{client_secret}"
        auth_b64 = base64.b64encode(auth_str.encode()).decode()
        try:
            resp = requests.post(
                "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
                data={
                    "grant_type": "authorization_code",
                    "code": qb_oauth_code,
                    "redirect_uri": redirect_uri,
                },
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Authorization": f"Basic {auth_b64}",
                },
                timeout=15,
            )
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            resp_err = getattr(e, "response", None)
            err_msg = str(e)
            try:
                if resp_err is not None:
                    err_body = resp_err.json()
                    err_msg = err_body.get("error_description") or err_body.get("message") or err_body.get("error") or str(err_body)
            except Exception:
                if resp_err is not None and resp_err.text:
                    err_msg = resp_err.text[:500]
            with root:
                ui.label("âŒ Error al obtener token de QuickBooks").classes("text-negative text-lg mb-2")
                ui.label(f"Detalle: {err_msg}").classes("text-sm text-gray-600 mb-2")
                ui.label(
                    "Posibles causas:\n"
                    "• Redirect URI: en developer.intuit.com → Keys debe ser EXACTAMENTE la misma URL que en Configuración (con /qb/callback).\n"
                    "• NO uses /ml/callback para QuickBooks; debe ser /qb/callback.\n"
                    "• El código de autorización se usa una sola vez; si recargaste, volvé a Conectar."
                ).classes("text-sm text-gray-600 mb-4 whitespace-pre-line")
                ui.link("Volver a Configuración", "/").classes("text-primary")
            return
        except Exception as e:
            with root:
                ui.label(f"âŒ Error al obtener token de QuickBooks: {e}").classes("text-negative mb-4")
                ui.link("Volver al inicio", "/").classes("text-primary")
            return
        data = resp.json()
        access_token = data.get("access_token")
        refresh_token = data.get("refresh_token")
        expires_in = data.get("expires_in")
        if not access_token:
            with root:
                ui.label(f"âŒ Respuesta inesperada de Intuit: {data}").classes("text-negative mb-4")
            return
        expires_at = None
        if isinstance(expires_in, (int, float)):
            expires_at = (datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=int(expires_in))).isoformat()
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM qb_tokens WHERE user_id = ?", (user["id"],))
        cur.execute(
            "INSERT INTO qb_tokens (user_id, access_token, refresh_token, expires_at, realm_id, raw_data) VALUES (?, ?, ?, ?, ?, ?)",
            (user["id"], access_token, refresh_token, expires_at, qb_realm_id or None, json.dumps(data, ensure_ascii=False)),
        )
        conn.commit()
        conn.close()
        return RedirectResponse(url="/", status_code=302)

    user = get_current_user()
    if user:
        show_main_layout(root)
    else:
        show_login_screen(root)


def _iniciar_ngrok(port: int) -> None:
    """Lanza ngrok en segundo plano para exponer el puerto local."""
    try:
        subprocess.Popen(
            ["ngrok", "http", str(port)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0,
        )
        time.sleep(1.5)
        try:
            r = requests.get("http://127.0.0.1:4040/api/tunnels", timeout=2)
            if r.ok:
                data = r.json()
                tunnels = data.get("tunnels", [])
                for t in tunnels:
                    if t.get("public_url", "").startswith("https://"):
                        print(f"  Ngrok: {t['public_url']} -> http://127.0.0.1:{port}")
                        break
        except Exception:
            pass
    except FileNotFoundError:
        print("  Ngrok no encontrado en PATH. Ejecutá 'ngrok http', PORT manualmente si lo necesitás.")
    except Exception as e:
        print(f"  No se pudo iniciar ngrok: {e}")


def _arreglar_storage_nicegui() -> None:
    """Crea .nicegui y elimina archivos de storage corruptos para que NiceGUI los recree."""
    storage_dir = Path(__file__).parent / ".nicegui"
    storage_dir.mkdir(exist_ok=True)
    for f in storage_dir.glob("storage-*.json"):
        try:
            if f.stat().st_size == 0:
                f.unlink()
            else:
                json.loads(f.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            try:
                f.unlink()
            except OSError:
                pass


def main() -> None:
    # Cargar .env desde el directorio del script (importante cuando se ejecuta como servicio o desde otro CWD)
    env_path = Path(__file__).parent / ".env"
    load_dotenv(env_path)
    try:
        import fitz  # noqa: F401  # pymupdf â€â€ Invoices «Otra»
    except ImportError:
        logging.warning(
            "PyMuPDF no instalado (pip install pymupdf). Invoices → botón «Otra» no funcionará hasta instalarlo "
            "en el mismo entorno que ejecuta esta app (p. ej. %s -m pip install pymupdf).",
            sys.executable or "python3",
        )
    init_db()
    _arreglar_storage_nicegui()
    port = int(os.getenv("PORT", 8083))
    # En Render/cloud: PORT lo define la plataforma, no iniciar ngrok
    es_produccion = "PORT" in os.environ or os.getenv("RENDER") == "true"
    if not es_produccion and os.getenv("NGROK_AUTO_START", "0").strip().lower() in ("1", "true", "yes"):
        print("Iniciando ngrok...")
        _iniciar_ngrok(port)
    _secret = os.getenv("STORAGE_SECRET", "")
    if not _secret:
        print("ERROR: STORAGE_SECRET no configurado. Ver .env.example")
        sys.exit(1)
    # host 0.0.0.0 necesario para que Render/cloud pueda acceder al servicio
    ui.run(
        title="BDC systems",
        reload=False,
        host="0.0.0.0" if es_produccion else "127.0.0.1",
        port=port,
        storage_secret=os.getenv("STORAGE_SECRET", ""),
        reconnect_timeout=120,  # Evita "Connection lost" durante carga pesada (Precios con muchos productos)
        message_history_length=2000,  # Más mensajes al reconectar para restaurar UI
    )


if __name__ == "__main__":
    main()


