"""
tabs/descuentos.py
Pestaña Descuentos -- EXPERIMENTAL, solo lectura.

Calculadora de "precio de lista": dado el precio real de venta hoy de una
publicación (misma lógica que Productos/Salud: sale_price si existe, si no
price) y un % de descuento deseado, muestra a qué precio habría que subir la
publicación para que, al bajarla después al precio actual, ML muestre ese %
de descuento.

NO hace ningún PUT/POST a MercadoLibre -- ni price, ni promotions, ni ningún
otro endpoint de escritura. Solo lee publicaciones vía ml_get_my_items (GET).
Diego decide más adelante si y cuándo esto pasa a escribir de verdad.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from nicegui import app, background_tasks, run, ui

from ml_api import get_ml_access_token, ml_get_my_items
from tabs.cuotas import _cuotas_key


def _require_login() -> Optional[Dict[str, Any]]:
    user = app.storage.user.get("user")
    if not user:
        ui.notify("Debes iniciar sesión", color="negative")
    return user


def _fmt_moneda(v: Any) -> str:
    if v is None:
        return "$0"
    try:
        return "$" + f"{int(round(float(v))):,}".replace(",", ".")
    except (ValueError, TypeError):
        return "$0"


def _fmt_pct(v: float) -> str:
    # Sin decimales si es entero (44%), con hasta 2 si no (44.5%) -- más legible
    # que forzar siempre la misma cantidad de decimales.
    if abs(v - round(v)) < 0.005:
        return f"{int(round(v))}"
    return f"{v:.2f}".rstrip("0").rstrip(".")


def build_tab_descuentos(container) -> None:
    container.clear()
    user = _require_login()
    if not user:
        return
    uid = user["id"]

    with container:
        access_token = get_ml_access_token(uid)
        if not access_token:
            ui.label("⚠️ No tienes MercadoLibre vinculado. Ve a Configuración y conecta tu cuenta.").classes("text-warning mb-4")
            return

        with ui.column().classes("w-full gap-3 p-2"):
            with ui.row().classes("items-center gap-2"):
                ui.label("Descuentos").classes("text-xl font-bold")
                ui.badge("EXPERIMENTAL").props("rounded").style("background:#f59e0b;color:white")
            ui.label(
                "Calculadora de precio de lista para simular un % de descuento visible en ML. "
                "Todavía NO escribe nada en MercadoLibre -- ni precio ni promociones -- es solo "
                "una previsualización."
            ).classes("text-xs text-gray-500")

            body_col = ui.column().classes("w-full gap-2")
            with body_col:
                with ui.card().classes("w-full p-8 items-center gap-4") as loading_card:
                    ui.spinner(size="xl")
                    ui.label("Cargando publicaciones con stock...").classes("text-lg text-gray-700")

            async def _cargar() -> None:
                try:
                    data = await run.io_bound(ml_get_my_items, access_token, False, False, uid)
                except Exception as e:
                    body_col.clear()
                    with body_col:
                        ui.label(f"❌ Error al conectar con MercadoLibre: {e}").classes("text-negative")
                    return
                items = [it for it in (data.get("results") or []) if it.get("id")]
                body_col.clear()
                with body_col:
                    _build_selector(items)

            def _build_selector(items: List[Dict[str, Any]]) -> None:
                if not items:
                    ui.label("No hay publicaciones con stock para esta cuenta.").classes("text-sm text-gray-400")
                    return

                # Agrupación por SKU (misma lógica que Productos/Cuotas -- tabs/cuotas.py
                # _cuotas_key, con la regla de absorción de catálogo huérfano) para que el
                # selector muestre un producto real por opción, no una por publicación
                # (propia + catálogo x planes de cuotas).
                _cpid_to_skus: Dict[str, set] = {}
                for it in items:
                    _cpid = (it.get("catalog_product_id") or "").strip()
                    _sku_it = (it.get("seller_sku") or "").strip()
                    if _cpid and _sku_it:
                        _cpid_to_skus.setdefault(_cpid, set()).add(_sku_it.lower())

                groups: Dict[tuple, List[Dict[str, Any]]] = {}
                for it in items:
                    groups.setdefault(_cuotas_key(it, _cpid_to_skus), []).append(it)

                # Principal del grupo: mismo criterio que tabs/precios.py -- gana la propia
                # gold_special con más stock; si no hay propia gold_special, gana la de más
                # stock entre las restantes (típicamente la de catálogo).
                productos: List[Dict[str, Any]] = []
                for grupo in groups.values():
                    principal = max(
                        grupo,
                        key=lambda x: (
                            1 if not x.get("catalog_listing") and
                                 str(x.get("listing_type_id") or "").lower() == "gold_special" else 0,
                            int(x.get("available_quantity") or 0),
                        ),
                    )
                    if (principal.get("available_quantity") or 0) > 0:
                        productos.append(principal)

                if not productos:
                    ui.label("No hay publicaciones con stock para esta cuenta.").classes("text-sm text-gray-400")
                    return

                items_by_id: Dict[str, Dict[str, Any]] = {str(it["id"]): it for it in productos}
                opciones = {
                    iid: f"{it.get('title') or iid} ({it.get('seller_sku') or 'sin SKU'})"
                    for iid, it in items_by_id.items()
                }

                with ui.row().classes("items-center gap-3 flex-wrap w-full"):
                    sel = ui.select(
                        opciones, value=None, with_input=True, clearable=True,
                        label=f"Producto con stock ({len(opciones)}) -- nombre o SKU",
                    ).props("dense outlined").classes("w-[28rem] max-w-full")
                    descuento_inp = ui.number(
                        label="Descuento deseado (%)", value=44, min=0.01, max=99.99, step=1,
                    ).props("dense outlined").classes("w-48")

                salida = ui.column().classes("w-full gap-1 mt-2")

                def _recalcular() -> None:
                    salida.clear()
                    item_id = sel.value
                    if not item_id:
                        return
                    it = items_by_id.get(str(item_id))
                    if not it:
                        return
                    # Misma lógica que Productos/Salud para el "precio real de hoy":
                    # sale_price si ML lo trae, si no el price de lista actual.
                    precio_raw = it.get("price") or 0
                    sale_price = it.get("sale_price")
                    precio_actual = float(sale_price) if sale_price is not None else float(precio_raw or 0)
                    try:
                        pct = float(descuento_inp.value or 0)
                    except (TypeError, ValueError):
                        pct = 0.0
                    with salida:
                        if precio_actual <= 0:
                            ui.label("Esta publicación no tiene un precio actual válido.").classes("text-sm text-negative")
                            return
                        if not (0 < pct < 100):
                            ui.label("El descuento tiene que ser mayor a 0% y menor a 100%.").classes("text-sm text-negative")
                            return
                        precio_lista = precio_actual / (1 - pct / 100)
                        pct_fmt = _fmt_pct(pct)
                        ui.label(f"Precio actual: {_fmt_moneda(precio_actual)} (el que se vende hoy)").classes("text-sm")
                        ui.label(
                            f"Para que se vea un descuento de {pct_fmt}%, subir el precio a: "
                            f"{_fmt_moneda(precio_lista)}"
                        ).classes("text-sm font-bold")
                        ui.label(
                            f"Después, bajarlo de nuevo a {_fmt_moneda(precio_actual)} para que se muestre "
                            f"-{pct_fmt}% de descuento."
                        ).classes("text-sm")
                        ui.label(
                            "Esto es solo una previsualización -- todavía no se aplicó ningún cambio en ML."
                        ).classes("text-xs text-gray-400 mt-1")

                sel.on_value_change(_recalcular)
                descuento_inp.on_value_change(_recalcular)

            background_tasks.create(_cargar(), name="cargar_descuentos")
