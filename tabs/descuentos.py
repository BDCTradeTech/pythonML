"""
tabs/descuentos.py
Pestaña Descuentos.

Calculadora de "precio de lista": dado el precio real de venta hoy de una
publicación (misma lógica que Productos/Salud: sale_price si existe, si no
price) y un % de descuento deseado, muestra a qué precio habría que subir la
publicación para que, al bajarla después al precio actual, ML muestre ese %
de descuento. Esta parte sigue siendo 100% de solo lectura.

Desde 2026-09-14 (incidente echodot5-azul, ver ml_escrituras/activity_log de
esa fecha) también existe la activación REAL y auditada del truco, para toda
la familia de cuotas del producto seleccionado (contado + 3x/6x/9x/12x, misma
agrupación de _cuotas_key que ya usa Productos/Cuotas):
  Activar:  sube price en las publicaciones de la familia -> recalcula los
            tiers de mayorista (price_per_quantity) contra el precio nuevo,
            reusando el motor de salud_audit.py/tabs/salud.py -> recién
            entonces crea una promoción PRICE_DISCOUNT por publicación
            (deal_price = precio real de siempre de ESA publicación).
  Revertir: borra las promociones, baja los precios, recalcula mayorista de
            nuevo contra el precio restaurado.
Cada paso se hace en ese orden exacto (ML borra solo cualquier PRICE_DISCOUNT
si se sube el precio de un ítem -- por eso precio y mayorista van SIEMPRE
antes que la promoción) y se frena en el primer error de esa publicación
puntual, sin reintentar ni improvisar. Todo PUT de precio queda auditado en
ml_escrituras (ver ml_api.ml_update_item_price); el estado de la activación
(para que Revertir funcione aunque sea horas después, en otra sesión) vive en
la tabla descuentos_activaciones (db.py).
"""
from __future__ import annotations

import asyncio
import json as _json
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from nicegui import app, background_tasks, context, run, ui

from db import (
    actualizar_activacion_descuento,
    crear_activacion_descuento,
    get_activacion_descuento_vigente,
    log_ml_escritura,
)
from ml_api import (
    _cuotas_desde_item,
    _detalle_error_ml,
    _parse_ml_item_body,
    get_ml_access_token,
    get_ml_session,
    ml_create_price_discount,
    ml_delete_price_discount,
    ml_get_my_items,
    ml_get_seller_promotions_item,
    ml_update_item_price,
)
from salud_audit import _calcular_mayorista_recomendado, _standard_amount_de, _tiers_cargados_todos, _wholesale_from_prices
from tabs.cuotas import _cuotas_key, _cuotas_score
from tabs.salud import _escribir_mayorista_pxq

# Rango real que exige ML para PRICE_DISCOUNT (verificado contra el MCP de
# MercadoLibre, 2026-09-14): >=5% y <80%. El simulador de arriba sigue
# permitiendo 0.01-99.99% porque es de solo lectura; esto solo se aplica al
# activar de verdad.
_DTO_MIN_ACTIVAR = 5.0
_DTO_MAX_ACTIVAR = 80.0
_PROMO_DIAS = 14  # plazo máximo que ML permite para un PRICE_DISCOUNT

_TRAMOS = [("contado", "Contado"), ("x3", "3 cuotas"), ("x6", "6 cuotas"), ("x9", "9 cuotas"), ("x12", "12 cuotas")]


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
                "El cálculo de arriba es solo una previsualización -- no escribe nada. "
                "El botón 'Activar descuento real' que aparece al elegir un producto SÍ escribe en "
                "MercadoLibre (precio, mayorista y una promoción real) y hay que confirmarlo a mano."
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
                    precio_deseado_inp = ui.number(
                        label="Precio final deseado", value=None, min=0.01, step=1,
                    ).props("dense outlined").classes("w-48")
                    descuento_inp = ui.number(
                        label="Descuento deseado (%)", value=44, min=0.01, max=99.99, step=1,
                    ).props("dense outlined").classes("w-48")

                precio_col = ui.column().classes("w-full gap-1 mt-2")
                cuotas_col = ui.column().classes("w-full gap-1")
                mayorista_col = ui.column().classes("w-full gap-1")
                activacion_col = ui.column().classes("w-full gap-2 mt-3")

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
                    try:
                        precio_deseado = float(precio_deseado_inp.value or 0)
                    except (TypeError, ValueError):
                        precio_deseado = 0.0
                    with precio_col:
                        if precio_actual <= 0:
                            ui.label("Esta publicación no tiene un precio actual válido.").classes("text-sm text-negative")
                            return
                        if precio_deseado <= 0:
                            ui.label("El precio final deseado tiene que ser mayor a $0.").classes("text-sm text-negative")
                            return
                        if not (0 < pct < 100):
                            ui.label("El descuento tiene que ser mayor a 0% y menor a 100%.").classes("text-sm text-negative")
                            return
                        precio_lista = precio_deseado / (1 - pct / 100)
                        pct_fmt = _fmt_pct(pct)
                        ui.label(f"Precio actual: {_fmt_moneda(precio_actual)} (el que se vende hoy)").classes("text-sm")
                        if abs(precio_deseado - precio_actual) > 0.01:
                            ui.label(
                                f"Calculado sobre el precio final deseado ({_fmt_moneda(precio_deseado)}), "
                                "no sobre el precio actual de ML."
                            ).classes("text-xs text-gray-500")
                        ui.label(
                            f"Para que se vea un descuento de {pct_fmt}%, subir el precio a: "
                            f"{_fmt_moneda(precio_lista)}"
                        ).classes("text-sm font-bold")
                        ui.label(
                            f"Después, bajarlo de nuevo a {_fmt_moneda(precio_deseado)} para que se muestre "
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

                # ── Activar / Revertir: el truco de arriba, pero de verdad ─────────────
                # Sube price + recalcula mayorista + crea PRICE_DISCOUNT en las hasta 5
                # publicaciones de la familia (contado + 3x/6x/9x/12x). Ver docstring del
                # módulo para el orden exacto y por qué importa.

                def _fetch_item_full_sync(token: str, item_id: str) -> Dict[str, Any]:
                    r = get_ml_session().get(
                        f"https://api.mercadolibre.com/items/{item_id}",
                        headers={"Authorization": f"Bearer {token}"},
                        timeout=15,
                    )
                    r.raise_for_status()
                    return _parse_ml_item_body(r.json())

                def _fetch_familia_sync(token: str, candidatos: List[tuple]) -> List[Dict[str, Any]]:
                    out: List[Dict[str, Any]] = []
                    for tramo, base_it in candidatos:
                        iid = str(base_it.get("id"))
                        try:
                            fresco = _fetch_item_full_sync(token, iid)
                        except Exception as e:
                            out.append({"tramo": tramo, "item_id": iid, "error": str(e)})
                            continue
                        precio_raw = fresco.get("price") or 0
                        sale_price = fresco.get("sale_price")
                        precio_actual = float(sale_price) if sale_price is not None else float(precio_raw or 0)
                        out.append({
                            "tramo": tramo,
                            "item_id": iid,
                            "seller_sku": (fresco.get("seller_sku") or "").strip(),
                            "precio_actual": precio_actual,
                            "status": fresco.get("status"),
                        })
                    return out

                async def _resolver_familia_live(item_id: str) -> List[Dict[str, Any]]:
                    """Relee en vivo (GET directo, no la lista de `items` cargada al abrir la
                    pestaña -- puede llevar rato abierta) las hasta 5 publicaciones de la
                    familia: contado (principal ya resuelto por _cuotas_key) + hermanas de
                    cuotas vía _cuotas_siblings. Tramos sin hermana simplemente no entran."""
                    it = items_by_id.get(item_id)
                    grupo = grupos_by_id.get(item_id) or []
                    siblings = _cuotas_siblings(grupo)
                    candidatos: List[tuple] = [("contado", it)]
                    for gkey in ("x3", "x6", "x9", "x12"):
                        sib = siblings.get(gkey)
                        if sib:
                            candidatos.append((gkey, sib))
                    return await run.io_bound(_fetch_familia_sync, access_token, candidatos)

                async def _mayorista_recompute(token: str, item_id: str, precio_objetivo: float):
                    """Relee tiers cargados HOY para item_id y le pide a ML (motor oficial,
                    salud_audit.py) el % correcto para esas mismas cantidades contra
                    precio_objetivo. Devuelve (cargado {qty:monto}, nuevo {qty:(monto,pct)}) --
                    (None, None) si no tiene mayorista cargado. Levanta RuntimeError (nunca
                    inventa un workaround) si ML no puede recalcular alguna cantidad."""
                    prices_body = await run.io_bound(_fetch_prices_body, token, item_id)
                    standard = _standard_amount_de(prices_body)
                    if not standard:
                        return None, None
                    cargado = _tiers_cargados_todos(prices_body, standard)
                    if not cargado:
                        return None, None
                    qtys = tuple(sorted(cargado.keys()))
                    prop = await run.io_bound(_calcular_mayorista_recomendado, token, item_id, precio_objetivo, qtys)
                    if not prop or not prop.get("propuesta"):
                        raise RuntimeError(f"ML no pudo recalcular mayorista para {item_id} (cantidades {list(qtys)})")
                    nuevo = {p["quantity"]: (p["amount"], p["percentage"]) for p in prop["propuesta"]}
                    faltantes = sorted(set(qtys) - set(nuevo.keys()))
                    if faltantes:
                        raise RuntimeError(f"ML no devolvió recomendación de mayorista para las cantidades {faltantes} en {item_id}")
                    return cargado, nuevo

                def _render_activacion(item_id: str) -> None:
                    activacion_col.clear()
                    it = items_by_id.get(item_id)
                    sku = (it.get("seller_sku") or "").strip() if it else ""
                    with activacion_col:
                        if not sku:
                            ui.label(
                                "Esta publicación no tiene SELLER_SKU en ML -- no se puede activar "
                                "el descuento real (queda sin forma de auditar)."
                            ).classes("text-xs text-gray-400")
                            return
                        vigente = get_activacion_descuento_vigente(uid, sku)
                        if vigente:
                            _estado_txt = {
                                "activo": "Activo", "error_parcial": "Parado a mitad de camino (revisar)",
                            }.get(vigente["estado"], vigente["estado"])
                            ui.label(
                                f"⚠️ Descuento real: {_estado_txt} -- activado {vigente['ts_activacion'][:16].replace('T', ' ')} UTC"
                            ).classes("text-xs font-bold").style(
                                "color:#e65100" if vigente["estado"] == "error_parcial" else "color:#c62828"
                            )
                            ui.button(
                                "Revertir descuento real",
                                on_click=lambda: _abrir_dialogo_revertir(item_id, sku, vigente),
                            ).props("outline").style("color:#c62828;border-color:#c62828")
                        else:
                            ui.button(
                                "Activar descuento real (sube precio en ML)",
                                on_click=lambda: _abrir_dialogo_activar(item_id, sku),
                            ).props("outline").style("color:#c62828;border-color:#c62828")

                def _abrir_dialogo_activar(item_id: str, sku: str) -> None:
                    cl = context.client
                    dlg = ui.dialog()
                    with dlg:
                        with ui.card().classes("p-4 min-w-[320px] items-center gap-3"):
                            ui.spinner(size="lg")
                            ui.label("Releyendo la familia y el mayorista en vivo contra ML...").classes("text-sm text-gray-600")
                    dlg.open()

                    async def _preparar() -> None:
                        try:
                            pct = float(descuento_inp.value or 0)
                        except (TypeError, ValueError):
                            pct = 0.0
                        if not (_DTO_MIN_ACTIVAR <= pct < _DTO_MAX_ACTIVAR):
                            with cl:
                                dlg.close()
                                ui.notify(
                                    f"Para activar de verdad, el descuento tiene que estar entre "
                                    f"{_DTO_MIN_ACTIVAR:.0f}% y {_DTO_MAX_ACTIVAR:.0f}% (límite real de ML) -- "
                                    f"ajustá el campo de arriba.",
                                    color="negative", multi_line=True,
                                )
                            return
                        familia = await _resolver_familia_live(item_id)
                        filas: List[Dict[str, Any]] = []
                        for f in familia:
                            if f.get("error"):
                                with cl:
                                    dlg.close()
                                    ui.notify(
                                        f"No se pudo releer {f['item_id']} ({f['tramo']}) en vivo: {f['error']}. "
                                        f"No se activó nada.",
                                        color="negative", multi_line=True,
                                    )
                                return
                            if not f.get("seller_sku"):
                                with cl:
                                    dlg.close()
                                    ui.notify(
                                        f"{f['item_id']} ({f['tramo']}) no tiene SELLER_SKU en ML. No se activó nada.",
                                        color="negative", multi_line=True,
                                    )
                                return
                            if (f.get("status") or "").lower() != "active":
                                with cl:
                                    dlg.close()
                                    ui.notify(
                                        f"{f['item_id']} ({f['tramo']}) no está activo (status={f.get('status')}) -- "
                                        f"ML no permite un descuento ahí. No se activó nada.",
                                        color="negative", multi_line=True,
                                    )
                                return
                            if not f.get("precio_actual") or f["precio_actual"] <= 0:
                                with cl:
                                    dlg.close()
                                    ui.notify(
                                        f"{f['item_id']} ({f['tramo']}) no tiene un precio actual válido. No se activó nada.",
                                        color="negative", multi_line=True,
                                    )
                                return
                            f["precio_lista"] = f["precio_actual"] / (1 - pct / 100)
                            filas.append(f)

                        for f in filas:
                            promos = await run.io_bound(ml_get_seller_promotions_item, access_token, f["item_id"])
                            f["deal_bloqueante"] = next(
                                (p for p in (promos or []) if str(p.get("type")) == "DEAL" and str(p.get("status")) == "started"),
                                None,
                            )
                            try:
                                cargado, nuevo = await _mayorista_recompute(access_token, f["item_id"], f["precio_lista"])
                                f["mayorista_cargado"], f["mayorista_nuevo"] = cargado, nuevo
                            except Exception as e:
                                f["mayorista_cargado"], f["mayorista_nuevo"] = None, None
                                f["mayorista_preview_error"] = str(e)

                        with cl:
                            dlg.close()
                            _mostrar_confirmacion_activar(item_id, sku, pct, filas)

                    background_tasks.create(_preparar())

                def _mostrar_confirmacion_activar(item_id: str, sku: str, pct: float, filas: List[Dict[str, Any]]) -> None:
                    dlg2 = ui.dialog()
                    with dlg2:
                        with ui.card().classes("p-4 min-w-[560px] max-w-[95vw]"):
                            ui.label(f"Activar descuento real -- {sku} ({_fmt_pct(pct)}%)").classes("text-lg font-semibold mb-2")
                            if any(f.get("deal_bloqueante") for f in filas):
                                ui.label(
                                    "⚠️ Al menos una publicación tiene un DEAL activo -- ML no va a aplicar el "
                                    "PRICE_DISCOUNT ahí hasta que termine ese DEAL. Precio y mayorista se actualizan "
                                    "igual; la promoción puede quedar pendiente en esa publicación puntual."
                                ).classes("text-xs mb-2").style("color:#e65100")
                            with ui.element("table").style("width:100%;border-collapse:collapse;font-size:11px"):
                                with ui.element("thead"):
                                    with ui.element("tr"):
                                        for h in ["Tramo", "Publicación", "Precio hoy", "→ Precio de lista", "Mayorista hoy → nuevo"]:
                                            with ui.element("th").style("text-align:left;padding:4px 6px;background:#1976d2;color:white"):
                                                ui.label(h)
                                with ui.element("tbody"):
                                    for f in filas:
                                        with ui.element("tr").style("border-bottom:1px solid #e5e7eb"):
                                            with ui.element("td").style("padding:3px 6px"):
                                                ui.label(dict(_TRAMOS).get(f["tramo"], f["tramo"]))
                                            with ui.element("td").style("padding:3px 6px;font-family:monospace;font-size:10px"):
                                                ui.label(f["item_id"])
                                            with ui.element("td").style("padding:3px 6px;text-align:right"):
                                                ui.label(_fmt_moneda(f["precio_actual"]))
                                            with ui.element("td").style("padding:3px 6px;text-align:right;font-weight:600"):
                                                ui.label(_fmt_moneda(f["precio_lista"]))
                                            with ui.element("td").style("padding:3px 6px"):
                                                if f.get("mayorista_preview_error"):
                                                    ui.label(f["mayorista_preview_error"]).classes("text-negative")
                                                elif not f.get("mayorista_cargado"):
                                                    ui.label("sin mayorista").classes("text-gray-400")
                                                else:
                                                    _txt = "; ".join(
                                                        f"{q}+: {_fmt_moneda(f['mayorista_cargado'][q])} → "
                                                        f"{_fmt_moneda(f['mayorista_nuevo'][q][0])}"
                                                        for q in sorted(f["mayorista_cargado"])
                                                    )
                                                    ui.label(_txt)
                            ui.label(
                                "Esto sube el precio de verdad en ML, recalcula mayorista y crea una promoción "
                                "PRICE_DISCOUNT real en cada publicación de arriba. Es reversible con 'Revertir', "
                                "pero mientras esté activo se puede vender a estos precios."
                            ).classes("text-xs mt-2 mb-2").style("color:#c62828")
                            with ui.row().classes("w-full justify-end gap-2"):
                                ui.button("Cancelar", on_click=dlg2.close).props("flat")
                                ui.button(
                                    "Confirmar y activar",
                                    on_click=lambda: _ejecutar_activar(dlg2, item_id, sku, pct, filas),
                                ).style("background:#c62828;color:white;font-weight:600").props("no-caps")
                    dlg2.open()

                def _ejecutar_activar(dlg2, item_id: str, sku: str, pct: float, filas: List[Dict[str, Any]]) -> None:
                    dlg2.close()
                    cl = context.client
                    items_state = [
                        {
                            "tramo": f["tramo"], "item_id": f["item_id"], "seller_sku": f["seller_sku"],
                            "precio_original": f["precio_actual"], "precio_lista": f["precio_lista"],
                            "estado_price": "pendiente", "estado_mayorista": "pendiente", "estado_promo": "pendiente",
                            "mayorista_qtys": sorted(f["mayorista_cargado"].keys()) if f.get("mayorista_cargado") else [],
                            "detalle_error": None,
                        }
                        for f in filas
                    ]
                    activacion_id = crear_activacion_descuento(uid, sku, pct, _json.dumps(items_state, ensure_ascii=False))

                    async def _correr() -> None:
                        for idx, item in enumerate(items_state):
                            iid = item["item_id"]
                            with cl:
                                ui.notify(
                                    f"[{idx + 1}/{len(items_state)}] {item['tramo']}: subiendo precio a "
                                    f"{_fmt_moneda(item['precio_lista'])}...",
                                    color="info",
                                )
                            try:
                                await run.io_bound(
                                    ml_update_item_price, access_token, iid, item["precio_lista"],
                                    uid, item["seller_sku"], "descuentos_activar", item["precio_original"],
                                )
                                item["estado_price"] = "ok"
                                actualizar_activacion_descuento(activacion_id, _json.dumps(items_state, ensure_ascii=False))
                            except Exception as e:
                                item["estado_price"], item["detalle_error"] = "error", _detalle_error_ml(e)
                                actualizar_activacion_descuento(
                                    activacion_id, _json.dumps(items_state, ensure_ascii=False), estado="error_parcial",
                                )
                                with cl:
                                    ui.notify(
                                        f"PARADO en {item['tramo']} ({iid}) al subir el precio: {e}",
                                        color="negative", timeout=15000, multi_line=True,
                                    )
                                    _render_activacion(item_id)
                                return

                            if item["mayorista_qtys"]:
                                try:
                                    _cargado, nuevo = await _mayorista_recompute(access_token, iid, item["precio_lista"])
                                    if not nuevo:
                                        raise RuntimeError("ML no devolvió tiers cargados en la relectura")
                                    faltan = [q for q in item["mayorista_qtys"] if q not in nuevo]
                                    if faltan:
                                        raise RuntimeError(f"ML no recalculó las cantidades {faltan}")
                                    cambios = {q: nuevo[q][1] for q in item["mayorista_qtys"]}
                                    err, _warn = await run.io_bound(
                                        _escribir_mayorista_pxq, access_token, uid, item["seller_sku"], iid,
                                        cambios, None, "descuentos_activar",
                                    )
                                    if err:
                                        raise RuntimeError(err)
                                    item["estado_mayorista"] = "ok"
                                except Exception as e:
                                    item["estado_mayorista"], item["detalle_error"] = "error", _detalle_error_ml(e)
                                    actualizar_activacion_descuento(
                                        activacion_id, _json.dumps(items_state, ensure_ascii=False), estado="error_parcial",
                                    )
                                    with cl:
                                        ui.notify(
                                            f"PARADO en {item['tramo']} ({iid}) al recalcular mayorista: {e} -- "
                                            f"el precio de esta publicación YA quedó subido; no seguí con las siguientes.",
                                            color="negative", timeout=15000, multi_line=True,
                                        )
                                        _render_activacion(item_id)
                                    return
                            else:
                                item["estado_mayorista"] = "sin_tiers"
                            actualizar_activacion_descuento(activacion_id, _json.dumps(items_state, ensure_ascii=False))

                            # Log único al final (mismo patrón que _escribir_mayorista_pxq: se
                            # audita lo CONFIRMADO, no el intento) -- así ml_escrituras nunca
                            # queda con un "ok" para una promoción que en realidad no llegó a
                            # 'started'.
                            _hoy = date.today()
                            _fin = _hoy + timedelta(days=_PROMO_DIAS)
                            _valor_nuevo_promo = _json.dumps({
                                "deal_price": item["precio_original"],
                                "start_date": _hoy.isoformat(), "finish_date": _fin.isoformat(),
                            }, ensure_ascii=False)
                            _promo_error: Optional[str] = None
                            try:
                                await run.io_bound(
                                    ml_create_price_discount, access_token, iid, item["precio_original"],
                                    f"{_hoy.isoformat()}T00:00:00", f"{_fin.isoformat()}T00:00:00",
                                )
                                item["estado_promo"] = "creada"
                                actualizar_activacion_descuento(activacion_id, _json.dumps(items_state, ensure_ascii=False))
                                _started = False
                                for _ in range(6):
                                    await asyncio.sleep(2.0)
                                    _promos = await run.io_bound(ml_get_seller_promotions_item, access_token, iid)
                                    if any(
                                        str(p.get("type")) == "PRICE_DISCOUNT" and str(p.get("status")) == "started"
                                        for p in (_promos or [])
                                    ):
                                        _started = True
                                        break
                                if not _started:
                                    _promo_error = (
                                        "la promoción se creó pero no llegó a 'started' en ~12s -- puede estar "
                                        "bloqueada por un DEAL activo u otra causa; revisar en ML antes de seguir"
                                    )
                            except Exception as e:
                                _promo_error = _detalle_error_ml(e)

                            if _promo_error is None:
                                item["estado_promo"] = "started"
                                log_ml_escritura(
                                    uid, item["seller_sku"], iid, "promo_price_discount",
                                    None, _valor_nuevo_promo, "descuentos_activar", "ok", None,
                                )
                                actualizar_activacion_descuento(activacion_id, _json.dumps(items_state, ensure_ascii=False))
                            else:
                                item["detalle_error"] = _promo_error
                                log_ml_escritura(
                                    uid, item["seller_sku"], iid, "promo_price_discount",
                                    None, _valor_nuevo_promo, "descuentos_activar", "error", _promo_error,
                                )
                                actualizar_activacion_descuento(
                                    activacion_id, _json.dumps(items_state, ensure_ascii=False), estado="error_parcial",
                                )
                                with cl:
                                    ui.notify(
                                        f"PARADO en {item['tramo']} ({iid}) al crear la promoción: {_promo_error} -- "
                                        f"precio y mayorista de esta publicación ya quedaron aplicados; no seguí con "
                                        f"las siguientes.",
                                        color="negative", timeout=15000, multi_line=True,
                                    )
                                    _render_activacion(item_id)
                                return

                            with cl:
                                ui.notify(f"{item['tramo']} ({iid}) activado OK.", color="positive")

                        actualizar_activacion_descuento(
                            activacion_id, _json.dumps(items_state, ensure_ascii=False), estado="activo",
                        )
                        with cl:
                            ui.notify(
                                f"Activación completa: {len(items_state)}/{len(items_state)} publicaciones.",
                                color="positive", timeout=8000,
                            )
                            _render_activacion(item_id)

                    background_tasks.create(_correr())

                def _abrir_dialogo_revertir(item_id: str, sku: str, vigente: Dict[str, Any]) -> None:
                    try:
                        items_state = _json.loads(vigente["items_json"])
                    except Exception:
                        ui.notify("No se pudo leer el estado guardado de esta activación.", color="negative")
                        return
                    dlg = ui.dialog()
                    with dlg:
                        with ui.card().classes("p-4 min-w-[520px] max-w-[95vw]"):
                            ui.label(f"Revertir descuento real -- {sku}").classes("text-lg font-semibold mb-2")
                            with ui.element("table").style("width:100%;border-collapse:collapse;font-size:11px"):
                                with ui.element("thead"):
                                    with ui.element("tr"):
                                        for h in ["Tramo", "Publicación", "Precio actual → Restaurar a", "Promo", "Mayorista", "Ya revertido"]:
                                            with ui.element("th").style("text-align:left;padding:4px 6px;background:#1976d2;color:white"):
                                                ui.label(h)
                                with ui.element("tbody"):
                                    for it_ in items_state:
                                        with ui.element("tr").style("border-bottom:1px solid #e5e7eb"):
                                            with ui.element("td").style("padding:3px 6px"):
                                                ui.label(dict(_TRAMOS).get(it_["tramo"], it_["tramo"]))
                                            with ui.element("td").style("padding:3px 6px;font-family:monospace;font-size:10px"):
                                                ui.label(it_["item_id"])
                                            with ui.element("td").style("padding:3px 6px;text-align:right"):
                                                ui.label(f"{_fmt_moneda(it_['precio_lista'])} → {_fmt_moneda(it_['precio_original'])}")
                                            with ui.element("td").style("padding:3px 6px"):
                                                ui.label("a borrar" if it_.get("estado_promo") in ("creada", "started") else "—")
                                            with ui.element("td").style("padding:3px 6px"):
                                                ui.label("a recalcular" if it_.get("mayorista_qtys") else "sin mayorista")
                                            with ui.element("td").style("padding:3px 6px"):
                                                ui.label("sí" if it_.get("estado_revertido") == "ok" else "no")
                            ui.label(
                                "Borra la promoción, baja el precio y recalcula mayorista contra el precio restaurado, "
                                "en ese orden. Se salta las publicaciones ya revertidas en un intento anterior."
                            ).classes("text-xs text-gray-500 mt-2 mb-2")
                            with ui.row().classes("w-full justify-end gap-2"):
                                ui.button("Cancelar", on_click=dlg.close).props("flat")
                                ui.button(
                                    "Confirmar y revertir",
                                    on_click=lambda: _ejecutar_revertir(dlg, item_id, sku, vigente["id"], items_state),
                                ).style("background:#c62828;color:white;font-weight:600").props("no-caps")
                    dlg.open()

                def _ejecutar_revertir(dlg, item_id: str, sku: str, activacion_id: int, items_state: List[Dict[str, Any]]) -> None:
                    dlg.close()
                    cl = context.client

                    async def _correr() -> None:
                        for item in items_state:
                            if item.get("estado_revertido") == "ok":
                                continue
                            iid = item["item_id"]

                            if item.get("estado_price") != "ok":
                                # Activar se frenó antes de llegar a subir el precio de esta
                                # publicación -- nunca se tocó en ML, así que no hay nada que
                                # revertir. Sin este guard, el PUT de abajo escribía el mismo
                                # precio que ya tenía (no-op en ML) pero quedaba auditado con
                                # un valor_anterior FALSO (el precio de lista que nunca llegó
                                # a aplicarse) -- detectado 2026-09-14 con Tag-Royal-LF12.
                                item["estado_revertido"] = "ok"
                                actualizar_activacion_descuento(activacion_id, _json.dumps(items_state, ensure_ascii=False))
                                with cl:
                                    ui.notify(
                                        f"{item['tramo']} ({iid}) no había llegado a activarse -- nada que revertir.",
                                        color="info",
                                    )
                                continue

                            if item.get("estado_promo") in ("creada", "started") and item.get("estado_revertido_promo") != "borrada":
                                try:
                                    resp = await run.io_bound(ml_delete_price_discount, access_token, iid)
                                    if resp.status_code not in (200, 404):
                                        raise RuntimeError(f"DELETE promoción status={resp.status_code} {resp.text[:200]}")
                                    item["estado_revertido_promo"] = "borrada"
                                    log_ml_escritura(
                                        uid, item["seller_sku"], iid, "promo_price_discount",
                                        "activa", "borrada", "descuentos_revertir", "ok", None,
                                    )
                                    actualizar_activacion_descuento(activacion_id, _json.dumps(items_state, ensure_ascii=False))
                                except Exception as e:
                                    item["detalle_error"] = _detalle_error_ml(e)
                                    log_ml_escritura(
                                        uid, item["seller_sku"], iid, "promo_price_discount",
                                        "activa", "borrada", "descuentos_revertir", "error", _detalle_error_ml(e),
                                    )
                                    actualizar_activacion_descuento(
                                        activacion_id, _json.dumps(items_state, ensure_ascii=False), estado="error_parcial",
                                    )
                                    with cl:
                                        ui.notify(
                                            f"PARADO revirtiendo {item['tramo']} ({iid}) al borrar la promoción: {e}",
                                            color="negative", timeout=15000, multi_line=True,
                                        )
                                        _render_activacion(item_id)
                                    return

                            try:
                                await run.io_bound(
                                    ml_update_item_price, access_token, iid, item["precio_original"],
                                    uid, item["seller_sku"], "descuentos_revertir", item["precio_lista"],
                                )
                            except Exception as e:
                                item["detalle_error"] = _detalle_error_ml(e)
                                actualizar_activacion_descuento(
                                    activacion_id, _json.dumps(items_state, ensure_ascii=False), estado="error_parcial",
                                )
                                with cl:
                                    ui.notify(
                                        f"PARADO revirtiendo {item['tramo']} ({iid}) al bajar el precio: {e} -- "
                                        f"la promoción de esta publicación ya se borró.",
                                        color="negative", timeout=15000, multi_line=True,
                                    )
                                    _render_activacion(item_id)
                                return

                            if item.get("mayorista_qtys"):
                                try:
                                    _cargado, nuevo = await _mayorista_recompute(access_token, iid, item["precio_original"])
                                    if not nuevo:
                                        raise RuntimeError("ML no devolvió tiers cargados en la relectura")
                                    faltan = [q for q in item["mayorista_qtys"] if q not in nuevo]
                                    if faltan:
                                        raise RuntimeError(f"ML no recalculó las cantidades {faltan}")
                                    cambios = {q: nuevo[q][1] for q in item["mayorista_qtys"]}
                                    err, _warn = await run.io_bound(
                                        _escribir_mayorista_pxq, access_token, uid, item["seller_sku"], iid,
                                        cambios, None, "descuentos_revertir",
                                    )
                                    if err:
                                        raise RuntimeError(err)
                                except Exception as e:
                                    item["detalle_error"] = _detalle_error_ml(e)
                                    actualizar_activacion_descuento(
                                        activacion_id, _json.dumps(items_state, ensure_ascii=False), estado="error_parcial",
                                    )
                                    with cl:
                                        ui.notify(
                                            f"PARADO revirtiendo {item['tramo']} ({iid}) al recalcular mayorista: {e} -- "
                                            f"precio y promoción de esta publicación ya quedaron revertidos.",
                                            color="negative", timeout=15000, multi_line=True,
                                        )
                                        _render_activacion(item_id)
                                    return

                            item["estado_revertido"] = "ok"
                            actualizar_activacion_descuento(activacion_id, _json.dumps(items_state, ensure_ascii=False))
                            with cl:
                                ui.notify(f"{item['tramo']} ({iid}) revertido OK.", color="positive")

                        actualizar_activacion_descuento(
                            activacion_id, _json.dumps(items_state, ensure_ascii=False),
                            estado="revertido", marcar_revertido=True,
                        )
                        with cl:
                            ui.notify("Reversión completa.", color="positive", timeout=8000)
                            _render_activacion(item_id)

                    background_tasks.create(_correr())

                def _on_producto_change() -> None:
                    item_id = sel.value
                    if not item_id:
                        precio_deseado_inp.value = None
                        cuotas_col.clear()
                        mayorista_col.clear()
                        activacion_col.clear()
                        _recalcular_precio()
                        return
                    item_id = str(item_id)
                    # Autocompleta con el precio actual de la publicación recién elegida --
                    # se pisa en CADA cambio de selección, no respeta un valor tipeado a
                    # mano si el usuario vuelve a elegir el mismo producto después.
                    it = items_by_id.get(item_id)
                    if it:
                        precio_raw = it.get("price") or 0
                        sale_price = it.get("sale_price")
                        precio_deseado_inp.value = float(sale_price) if sale_price is not None else float(precio_raw or 0)
                    _recalcular_precio()
                    _render_cuotas(item_id)
                    _render_activacion(item_id)
                    background_tasks.create(
                        _render_mayorista(item_id), name=f"mayorista_descuentos_{item_id}"
                    )

                sel.on_value_change(_on_producto_change)
                precio_deseado_inp.on_value_change(_recalcular_precio)
                descuento_inp.on_value_change(_recalcular_precio)

            background_tasks.create(_cargar(), name="cargar_descuentos")
