"""
tabs/descuentos.py
Pestaña Descuentos -- EXPERIMENTAL, solo lectura.

Calculadora de "precio de lista": dado el precio real de venta hoy de una
publicación (misma lógica que Productos/Salud: sale_price si existe, si no
price) y un % de descuento deseado, muestra a qué precio habría que subir la
publicación para que, al bajarla después al precio actual, ML muestre ese %
de descuento.

NO hace ningún PUT/POST a MercadoLibre -- ni price, ni promotions, ni ningún
otro endpoint de escritura. Solo lee publicaciones vía ml_get_my_items (GET) y,
para el bloque de mayorista del producto seleccionado, un GET puntual a
/items/{id}/prices (mismo endpoint y header show-all-prices que salud_audit.py).
Diego decide más adelante si y cuándo esto pasa a escribir de verdad.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from nicegui import app, background_tasks, run, ui

from ml_api import _cuotas_desde_item, get_ml_access_token, get_ml_session, ml_get_my_items
from salud_audit import _wholesale_from_prices
from tabs.cuotas import _cuotas_key, _cuotas_score


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
                # grupo completo (todos los hermanos de cuotas) por id del principal --
                # necesario para el bloque de "Cuotas actuales" (3x/6x/9x/12x), que no
                # se puede armar solo con el principal.
                grupos_by_id: Dict[str, List[Dict[str, Any]]] = {}
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
                        grupos_by_id[str(principal.get("id") or "")] = grupo

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

                precio_col = ui.column().classes("w-full gap-1 mt-2")
                cuotas_col = ui.column().classes("w-full gap-1")
                mayorista_col = ui.column().classes("w-full gap-1")

                def _cuotas_siblings(grupo: List[Dict[str, Any]]) -> Dict[str, Optional[Dict[str, Any]]]:
                    """Mismo criterio de tabs/cuotas.py (_build_row) para elegir, por
                    tramo de cuotas, la publicación hermana representativa: clasifica
                    cada ítem del grupo con _cuotas_desde_item (ml_api) y, si hay más de
                    un candidato para el mismo tramo, gana el de mejor _cuotas_score."""
                    best: Dict[str, Optional[Dict[str, Any]]] = {"x3": None, "x6": None, "x9": None, "x12": None}
                    for it in grupo:
                        cuotas = _cuotas_desde_item(it)
                        if cuotas in best:
                            if best[cuotas] is None or _cuotas_score(it) > _cuotas_score(best[cuotas]):
                                best[cuotas] = it
                    return best

                def _fetch_prices_body(token: str, item_id: str) -> dict:
                    # Mismo endpoint y header que salud_audit.py (show-all-prices: TRUE --
                    # sin ese header ML oculta los tiers de precio por cantidad B2B).
                    r = get_ml_session().get(
                        f"https://api.mercadolibre.com/items/{item_id}/prices",
                        headers={"Authorization": f"Bearer {token}", "show-all-prices": "TRUE"},
                        timeout=15,
                    )
                    r.raise_for_status()
                    return r.json()

                def _recalcular_precio() -> None:
                    precio_col.clear()
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
                    with precio_col:
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

                def _render_cuotas(item_id: str) -> None:
                    cuotas_col.clear()
                    grupo = grupos_by_id.get(item_id) or []
                    siblings = _cuotas_siblings(grupo)
                    with cuotas_col:
                        ui.label("Cuotas actuales (precio de hoy, sin aplicar el % de descuento)").classes("text-xs font-bold text-gray-500 uppercase mt-2")
                        with ui.row().classes("gap-6 flex-wrap"):
                            for gkey, glabel in [("x3", "3 cuotas"), ("x6", "6 cuotas"), ("x9", "9 cuotas"), ("x12", "12 cuotas")]:
                                sib = siblings.get(gkey)
                                precio = sib.get("price") if sib else None
                                precio_txt = _fmt_moneda(precio) if precio is not None else "—"
                                with ui.column().classes("items-center gap-0"):
                                    ui.label(glabel).classes("text-xs text-gray-500")
                                    ui.label(precio_txt).classes("text-sm font-semibold" if precio is not None else "text-sm text-gray-400")

                async def _render_mayorista(item_id: str) -> None:
                    mayorista_col.clear()
                    with mayorista_col:
                        ui.label("Mayorista actual").classes("text-xs font-bold text-gray-500 uppercase mt-2")
                        with ui.row().classes("items-center gap-2"):
                            ui.spinner(size="sm")
                            ui.label("Consultando mayorista...").classes("text-xs text-gray-400")
                    try:
                        prices_body = await run.io_bound(_fetch_prices_body, access_token, item_id)
                    except Exception as e:
                        if sel.value != item_id:
                            return  # el usuario ya cambió de selección, no pisar lo nuevo
                        mayorista_col.clear()
                        with mayorista_col:
                            ui.label("Mayorista actual").classes("text-xs font-bold text-gray-500 uppercase mt-2")
                            ui.label(f"No se pudo consultar mayorista ({e}).").classes("text-xs text-negative")
                        return
                    if sel.value != item_id:
                        return  # el usuario ya cambió de selección, no pisar lo nuevo
                    w = _wholesale_from_prices(prices_body)
                    tiers = w.get("tiers") or []
                    standard = w.get("standard_amount")
                    mayorista_col.clear()
                    with mayorista_col:
                        ui.label("Mayorista actual").classes("text-xs font-bold text-gray-500 uppercase mt-2")
                        if not tiers:
                            ui.label("Sin mayorista cargado.").classes("text-sm text-gray-400")
                            return
                        with ui.column().classes("gap-0.5"):
                            for min_qty, amount in tiers:
                                pct_txt = ""
                                if standard:
                                    pct = (standard - amount) / standard * 100
                                    pct_txt = f" (-{pct:.1f}%)"
                                ui.label(f"{min_qty}+ unidades: {_fmt_moneda(amount)}{pct_txt}").classes("text-sm")

                def _on_producto_change() -> None:
                    _recalcular_precio()
                    item_id = sel.value
                    if not item_id:
                        cuotas_col.clear()
                        mayorista_col.clear()
                        return
                    item_id = str(item_id)
                    _render_cuotas(item_id)
                    background_tasks.create(
                        _render_mayorista(item_id), name=f"mayorista_descuentos_{item_id}"
                    )

                sel.on_value_change(_on_producto_change)
                descuento_inp.on_value_change(_recalcular_precio)

            background_tasks.create(_cargar(), name="cargar_descuentos")
