"""
Fase 3 — tabs/misc.py
Pestañas auxiliares: Comparar Precios, Historial de Precios, Competencia.
Funciones exportadas: build_tab_comparar_precios, build_tab_historial_precios, build_tab_competencia
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from nicegui import app, ui

from db import save_query


# ---------------------------------------------------------------------------
# Helper de sesión (mismo patrón que otros tabs; se unificará en auth.py Fase 4)
# ---------------------------------------------------------------------------

def _require_login() -> Optional[Dict[str, Any]]:
    user = app.storage.user.get("user")
    if not user:
        ui.notify("Debes iniciar sesión para continuar", color="negative")
    return user


# ---------------------------------------------------------------------------
# Funciones exportadas
# ---------------------------------------------------------------------------

def build_tab_comparar_precios() -> None:
    user = _require_login()
    if not user:
        return

    ui.label("Comparar precios con la competencia").classes("text-lg font-semibold mb-4")
    ui.label(
        "Aquí podrás buscar un producto y ver precios de otros vendedores. "
        "De momento es sólo una pantalla de diseño; luego conectamos con la API."
    ).classes("text-gray-600 mb-4")

    query_input = ui.input("Palabra clave o código de producto").classes("w-full max-w-lg")
    result_area = ui.column().classes("w-full gap-2 mt-4")

    def comparar() -> None:
        if not query_input.value:
            ui.notify("Ingresa un término de búsqueda", color="negative")
            return
        save_query(
            user_id=user["id"],
            query_type="comparar_precios",
            params={"query": query_input.value},
        )
        result_area.clear()
        with result_area:
            ui.label("Aquí mostraremos resultados de la competencia (pendiente de implementar).")

    ui.button("Comparar", on_click=comparar, color="primary")


def build_tab_historial_precios() -> None:
    user = _require_login()
    if not user:
        return

    ui.label("Historial de precios").classes("text-lg font-semibold mb-4")
    ui.label(
        "En esta pestaña podrás ver cómo evolucionaron los precios de tus productos "
        "y los de la competencia. Más adelante conectaremos esta vista con la base de datos."
    ).classes("text-gray-600")


def build_tab_competencia() -> None:
    user = _require_login()
    if not user:
        return

    ui.label("Análisis de competencia").classes("text-lg font-semibold mb-4")
    ui.label(
        "Aquí calcularemos cantidad de vendedores, cantidad de productos y otros KPIs "
        "de la competencia."
    ).classes("text-gray-600 mb-4")

    categoria = ui.input("Categoría o keyword").classes("w-full max-w-lg")

    def calcular() -> None:
        if not categoria.value:
            ui.notify("Ingresa una categoría o palabra clave", color="negative")
            return
        save_query(
            user_id=user["id"],
            query_type="competencia",
            params={"categoria": categoria.value},
        )
        ui.notify("Cálculo de competencia pendiente de implementar.", color="info")

    ui.button("Calcular", on_click=calcular, color="primary")


