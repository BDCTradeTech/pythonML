"""
Fase 3 — tabs/home.py
Pestaña Home: bienvenida con descripción de permisos del usuario.
Funciones exportadas: build_tab_home_welcome
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from nicegui import app, ui

from db import get_user_tab_permissions
from tabs.constants import TAB_KEYS, LABEL_BY_TAB, TAB_DESCRIPTIONS


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

def build_tab_home_welcome(container) -> None:
    """Pestaña Home: bienvenida. Muestra qué puede hacer según permisos del usuario."""
    user = _require_login()
    if not user:
        return
    perms = get_user_tab_permissions(user["id"])
    lineas: List[str] = []
    for tab_key, _ in TAB_KEYS:
        if tab_key == "home":
            continue
        if perms.get(tab_key, False):
            label = LABEL_BY_TAB.get(tab_key, tab_key)
            desc = TAB_DESCRIPTIONS.get(tab_key, "")
            if desc:
                lineas.append(f"• {label}: {desc}")
    texto = "\n".join(lineas) if lineas else "No tenés permisos asignados. Contactá al administrador."
    with container:
        ui.label("Bienvenido").classes("text-3xl font-bold text-primary mb-4")
        ui.label(f"Hola, {user.get('username', 'Usuario')}").classes("text-xl text-gray-700 mb-2")
        with ui.column().classes("text-gray-600 mb-4 gap-2 max-w-2xl"):
            ui.label("¿Qué podés hacer en el sistema?").classes("text-base font-semibold text-gray-700")
            ui.label(texto).classes("text-sm whitespace-pre-line")


