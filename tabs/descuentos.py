"""
tabs/descuentos.py
Pestaña Descuentos.

Calculadora de "precio de lista": dado el precio real de venta hoy de una
publicación (misma lógica que Productos/Salud: sale_price si existe, si no
price) y un % de descuento deseado, muestra a qué precio habría que subir la
publicación para que, al bajarla después al precio actual, ML muestre ese %
de descuento. Esta parte sigue siendo 100% de solo lectura.

Desde 2026-09-14 también existe la activación REAL y auditada de un descuento,
para toda la familia de cuotas del producto seleccionado (contado +
3x/6x/9x/12x, misma agrupación de _cuotas_key que ya usa Productos/Cuotas).

Mecanismo (2do intento, el que quedó validado ese mismo día): el 1er intento
-- subir el precio y crear una promoción PRICE_DISCOUNT propia -- causó el
incidente echodot5-azul/Tag-Royal-LF12 (ML rechaza casi cualquier descuento
individual con ERROR_CREDIBILITY_DISCOUNTED_PRICE, y una publicación quedó
horas con el precio inflado sin vender). El mecanismo que sí funciona es
unirse a una SELLER_CAMPAIGN que ML ya tiene disponible para la publicación
(candidate), pidiendo el descuento MÍNIMO que ML acepta (max_discounted_price)
sobre el precio real -- sin inflar nada por default:
  Activar:    por cada tramo de la familia, relee en vivo si hay una campaña
              SELLER_CAMPAIGN candidata, pide su max_discounted_price fresco,
              chequea margen contra el costo real del SKU (si da pérdida o
              <5%, queda afuera del lote salvo confirmación explícita) y se
              une con ml_join_seller_promotion. Opción avanzada (no default):
              inflar el precio de lista antes de unirse, para un badge más
              alto -- ahí si ML rechaza el precio inflado, se revierte solo.
  Desactivar: relee en vivo (seller-promotions, NO la tabla local) qué tramos
              tienen una promoción 'started' ahora mismo -- funciona aunque la
              activación se haya hecho por script y no tenga fila en
              descuentos_activaciones. Borra el join (ml_delete_seller_promotion)
              y, si el precio base quedó inflado, lo baja a su valor real
              (tomado de la fila local si existe, si no sugerido desde el
              historial de ml_escrituras y SIEMPRE confirmado a mano antes de
              escribir -- nunca se asume en silencio).
Todo PUT de precio y cada join/delete de promoción queda auditado en
ml_escrituras; el estado de cada Activar/Desactivar además se guarda en
descuentos_activaciones (db.py) para que quede trazable de acá en adelante.
"""
from __future__ import annotations

import asyncio
import json as _json
from typing import Any, Dict, List, Optional

from nicegui import app, background_tasks, context, run, ui

from db import (
    actualizar_activacion_descuento,
    crear_activacion_descuento,
    get_activacion_descuento_vigente,
    get_historial_precio,
    get_producto_costo,
    log_ml_escritura,
)
from ml_api import (
    _cuotas_desde_item,
    _detalle_error_ml,
    _parse_ml_item_body,
    get_ml_access_token,
    get_ml_session,
    ml_delete_seller_promotion,
    ml_get_my_items,
    ml_get_seller_promotions_item,
    ml_join_seller_promotion,
    ml_update_item_price,
)
from salud_audit import _calcular_mayorista_recomendado, _standard_amount_de, _tiers_cargados_todos, _wholesale_from_prices
from tabs.cuotas import _cuotas_key, _cuotas_score
from tabs.dashboard import _calc_margen_prod, _load_params_prod
from tabs.salud import _escribir_mayorista_pxq

# Mecanismo validado 2026-09-14: unirse a una campaña existente pidiendo el
# descuento mínimo que ML acepta, no crear un PRICE_DISCOUNT propio (ese
# camino queda deprecado, ver docstring del módulo).
_TIPO_CAMPANIA_JOIN = "SELLER_CAMPAIGN"
# Umbral de margen (%) por debajo del cual un tramo queda afuera del lote de
# Activar salvo que el usuario lo tilde a mano -- mismo criterio que se usó a
# mano el 2026-09-14 con Tag-Royal-LF12 (contado quedó afuera por dar 4.3%).
_MARGEN_AJUSTADO_PCT = 5.0

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
                "El cálculo de arriba es solo una previsualización -- no escribe nada. Más abajo, "
                "al elegir un producto, se relee en vivo si sus publicaciones ya tienen un "
                "descuento real activo y se puede Activar/Desactivar de verdad (uniéndose a una "
                "campaña de ML, no crea promociones propias)."
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

                # ── Activar / Desactivar: descuento real, uniéndose a una SELLER_CAMPAIGN ──
                # Ver docstring del módulo para el mecanismo completo y por qué reemplazó al
                # PRICE_DISCOUNT (deprecado, causó el incidente de Tag-Royal-LF12).

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

                def _costo_sku(sku: str) -> Optional[tuple]:
                    return get_producto_costo(sku, uid)

                def _margen(precio: Optional[float], costo: Optional[tuple]) -> Optional[float]:
                    if not costo or not precio or precio <= 0:
                        return None
                    costo_usd, tipo_iva = costo
                    return _calc_margen_prod(precio, costo_usd, tipo_iva, _load_params_prod(uid))

                async def _resolver_familia_con_promos(item_id: str) -> List[Dict[str, Any]]:
                    """Como _resolver_familia_live, pero además trae en vivo -- mismo GET a
                    seller-promotions/items que ya usábamos para diagnosticar a mano -- si
                    cada tramo ya tiene una promoción 'started' ahora mismo, y si hay
                    campañas SELLER_CAMPAIGN 'candidate' con su rango de credibilidad
                    (min/max/suggested_discounted_price) ya calculado por ML contra el
                    precio real actual."""
                    familia = await _resolver_familia_live(item_id)

                    async def _con_promos(f: Dict[str, Any]) -> Dict[str, Any]:
                        if f.get("error"):
                            return f
                        try:
                            promos = await run.io_bound(ml_get_seller_promotions_item, access_token, f["item_id"])
                        except Exception as e:
                            f["error"] = f"no se pudieron leer las promociones: {e}"
                            return f
                        f["promos"] = promos or []
                        f["activa"] = next((p for p in f["promos"] if str(p.get("status")) == "started"), None)
                        f["candidatas"] = [
                            p for p in f["promos"]
                            if str(p.get("type")) == _TIPO_CAMPANIA_JOIN and str(p.get("status")) == "candidate"
                        ]
                        return f

                    return list(await asyncio.gather(*[_con_promos(f) for f in familia]))

                async def _render_activacion(item_id: str) -> None:
                    activacion_col.clear()
                    it = items_by_id.get(item_id)
                    sku = (it.get("seller_sku") or "").strip() if it else ""
                    with activacion_col:
                        if not sku:
                            ui.label(
                                "Esta publicación no tiene SELLER_SKU en ML -- no se puede operar "
                                "el descuento real (queda sin forma de auditar)."
                            ).classes("text-xs text-gray-400")
                            return
                        with ui.row().classes("items-center gap-2"):
                            ui.spinner(size="sm")
                            ui.label("Consultando estado real de promociones en ML...").classes("text-xs text-gray-400")

                    try:
                        familia = await _resolver_familia_con_promos(item_id)
                    except Exception as e:
                        if sel.value != item_id:
                            return
                        activacion_col.clear()
                        with activacion_col:
                            ui.label(f"No se pudo consultar el estado de promociones: {e}").classes("text-xs text-negative")
                        return

                    if sel.value != item_id:
                        return  # el usuario ya cambió de selección, no pisar lo nuevo

                    costo = _costo_sku(sku)

                    activacion_col.clear()
                    with activacion_col:
                        if not costo:
                            ui.label(
                                "⚠️ Este SKU no tiene costo cargado en Precios -- no se puede "
                                "chequear margen antes de activar. Cargalo antes de usar Activar."
                            ).classes("text-xs text-negative mb-1")

                        ui.label("Estado real en ML (recién consultado, en vivo)").classes(
                            "text-xs font-bold text-gray-500 uppercase mt-1"
                        )

                        checks: Dict[str, Any] = {}
                        campania_elegida: Dict[str, Dict[str, Any]] = {}

                        with ui.element("table").style("width:100%;border-collapse:collapse;font-size:11px"):
                            with ui.element("thead"):
                                with ui.element("tr"):
                                    for h in ["", "Tramo", "Publicación", "Precio real", "Estado", "Margen"]:
                                        with ui.element("th").style("text-align:left;padding:4px 6px;background:#1976d2;color:white"):
                                            ui.label(h)
                            with ui.element("tbody"):
                                for f in familia:
                                    with ui.element("tr").style("border-bottom:1px solid #e5e7eb"):
                                        with ui.element("td").style("padding:3px 6px"):
                                            chk = ui.checkbox(value=False)
                                        checks[f["tramo"]] = chk
                                        with ui.element("td").style("padding:3px 6px"):
                                            ui.label(dict(_TRAMOS).get(f["tramo"], f["tramo"]))
                                        with ui.element("td").style("padding:3px 6px;font-family:monospace;font-size:10px"):
                                            ui.label(f["item_id"])

                                        if f.get("error"):
                                            with ui.element("td").style("padding:3px 6px").props("colspan=3"):
                                                ui.label(f["error"]).classes("text-negative")
                                            chk.disable()
                                            f["_accion"] = None
                                            continue

                                        with ui.element("td").style("padding:3px 6px;text-align:right"):
                                            ui.label(_fmt_moneda(f["precio_actual"]))

                                        activa = f.get("activa")
                                        if activa:
                                            precio_promo = activa.get("price")
                                            pct_off = (
                                                100 * (f["precio_actual"] - precio_promo) / f["precio_actual"]
                                                if f["precio_actual"] and precio_promo is not None else 0
                                            )
                                            with ui.element("td").style("padding:3px 6px"):
                                                ui.label(
                                                    f"🟢 Activa: {activa.get('name') or activa.get('type')} "
                                                    f"-{pct_off:.1f}% → {_fmt_moneda(precio_promo)}"
                                                ).classes("text-positive")
                                            margen = _margen(precio_promo, costo)
                                            with ui.element("td").style("padding:3px 6px"):
                                                if margen is None:
                                                    ui.label("—").classes("text-gray-400")
                                                else:
                                                    margen_pct = 100 * margen / precio_promo if precio_promo else 0
                                                    _cls = "text-negative" if margen <= 0 else (
                                                        "text-warning" if margen_pct < _MARGEN_AJUSTADO_PCT else "text-positive"
                                                    )
                                                    ui.label(f"${margen:,.0f} ({margen_pct:.1f}%)").classes(_cls)
                                            chk.value = True
                                            f["_accion"] = "desactivar"
                                        elif f.get("candidatas"):
                                            candidatas = f["candidatas"]
                                            campania_elegida[f["tramo"]] = candidatas[0]
                                            with ui.element("td").style("padding:3px 6px"):
                                                if len(candidatas) > 1:
                                                    _opts = {c["id"]: (c.get("name") or c["id"]) for c in candidatas}
                                                    _sel_camp = ui.select(_opts, value=candidatas[0]["id"]).props("dense")

                                                    def _on_camp_change(e, _f=f, _cands=candidatas):
                                                        campania_elegida[_f["tramo"]] = next(
                                                            c for c in _cands if c["id"] == e.value
                                                        )

                                                    _sel_camp.on_value_change(_on_camp_change)
                                                else:
                                                    ui.label(
                                                        f"Sin promo -- campaña '{candidatas[0].get('name') or candidatas[0]['id']}' disponible"
                                                    ).classes("text-gray-600")
                                            max_dp = candidatas[0].get("max_discounted_price")
                                            margen = _margen(max_dp, costo)
                                            with ui.element("td").style("padding:3px 6px"):
                                                if margen is None or not max_dp:
                                                    ui.label("sin dato de margen").classes("text-gray-400")
                                                    _ajustado = False
                                                else:
                                                    pct_off = 100 * (f["precio_actual"] - max_dp) / f["precio_actual"] if f["precio_actual"] else 0
                                                    margen_pct = 100 * margen / max_dp if max_dp else 0
                                                    _ajustado = margen <= 0 or margen_pct < _MARGEN_AJUSTADO_PCT
                                                    _cls = "text-negative" if margen <= 0 else ("text-warning" if _ajustado else "text-positive")
                                                    ui.label(f"-{pct_off:.1f}% → {_fmt_moneda(max_dp)}: ${margen:,.0f} ({margen_pct:.1f}%)").classes(_cls)
                                                    if _ajustado:
                                                        ui.label("⚠️ margen ajustado, no se incluye por defecto").classes("text-xs text-warning")
                                            chk.value = not (margen is not None and max_dp and (margen <= 0 or 100 * margen / max_dp < _MARGEN_AJUSTADO_PCT))
                                            f["_accion"] = "activar"
                                        else:
                                            with ui.element("td").style("padding:3px 6px"):
                                                ui.label("Sin campaña disponible").classes("text-gray-400")
                                            with ui.element("td").style("padding:3px 6px"):
                                                ui.label("—").classes("text-gray-400")
                                            chk.disable()
                                            f["_accion"] = None

                        hay_activas = any(f.get("_accion") == "desactivar" for f in familia)
                        hay_candidatas = any(f.get("_accion") == "activar" for f in familia)

                        inflar_chk = riesgo_chk = badge_inp = None
                        if hay_candidatas:
                            with ui.expansion(
                                "Opción avanzada: inflar precio para conseguir mayor descuento", icon="warning"
                            ).classes("w-full mt-2").props("dense") as _exp:
                                ui.label(
                                    "⚠️ Sube el precio de lista de verdad ANTES de unirse a la campaña, para "
                                    "conseguir un badge más alto que el mínimo (ej. 40% en vez del mínimo que da "
                                    "ML al precio real). Es el mismo mecanismo que causó el incidente de hoy si "
                                    "algo sale mal: no hay forma de saber el descuento final sin subir el precio "
                                    "primero. Si la campaña no lo acepta, el precio se revierte solo."
                                ).classes("text-xs mb-1").style("color:#c62828")
                                inflar_chk = ui.checkbox("Inflar precio (avanzado)", value=False)
                                badge_inp = ui.number(label="Badge objetivo (%)", value=40, min=5, max=79, step=1).classes("w-40")
                                riesgo_chk = ui.checkbox("Entiendo el riesgo y quiero seguir igual", value=False)

                        with ui.row().classes("w-full justify-end gap-2 mt-2"):
                            if hay_activas:
                                ui.button(
                                    "Desactivar seleccionadas",
                                    on_click=lambda: _confirmar_desactivar(item_id, sku, familia, checks),
                                ).style("background:#c62828;color:white;font-weight:600").props("no-caps")
                            if hay_candidatas:
                                ui.button(
                                    "Activar seleccionadas",
                                    on_click=lambda: _confirmar_activar(
                                        item_id, sku, familia, checks, campania_elegida, costo,
                                        inflar_chk, badge_inp, riesgo_chk,
                                    ),
                                ).style("background:#2e7d32;color:white;font-weight:600").props("no-caps")
                            if not hay_activas and not hay_candidatas:
                                ui.label(
                                    "Ninguna publicación de esta familia tiene una campaña de descuento "
                                    "disponible ahora mismo."
                                ).classes("text-xs text-gray-400")

                def _confirmar_activar(
                    item_id: str, sku: str, familia: List[Dict[str, Any]], checks: Dict[str, Any],
                    campania_elegida: Dict[str, Dict[str, Any]], costo: Optional[tuple],
                    inflar_chk, badge_inp, riesgo_chk,
                ) -> None:
                    seleccion = [f for f in familia if f.get("_accion") == "activar" and checks[f["tramo"]].value]
                    if not seleccion:
                        ui.notify("No hay ningún tramo seleccionado para activar.", color="warning")
                        return
                    inflar = bool(inflar_chk.value) if inflar_chk else False
                    if inflar and not (riesgo_chk and riesgo_chk.value):
                        ui.notify("Tildá 'Entiendo el riesgo' para inflar el precio.", color="negative")
                        return
                    try:
                        badge_objetivo = float(badge_inp.value or 0) if badge_inp else 0.0
                    except (TypeError, ValueError):
                        badge_objetivo = 0.0
                    if inflar and not (5 <= badge_objetivo < 80):
                        ui.notify("El badge objetivo tiene que estar entre 5% y 79%.", color="negative")
                        return

                    dlg = ui.dialog()
                    with dlg:
                        with ui.card().classes("p-4 min-w-[600px] max-w-[95vw]"):
                            ui.label(f"Activar descuento real -- {sku}").classes("text-lg font-semibold mb-2")
                            if inflar:
                                ui.label(
                                    f"⚠️ Se va a subir el precio de lista antes de unirse a la campaña (badge "
                                    f"objetivo {_fmt_pct(badge_objetivo)}%). No hay forma de previsualizar el "
                                    f"descuento final sin subir el precio primero -- si la campaña no lo acepta, "
                                    f"se revierte automáticamente."
                                ).classes("text-xs mb-2").style("color:#c62828")
                            with ui.element("table").style("width:100%;border-collapse:collapse;font-size:11px"):
                                with ui.element("thead"):
                                    with ui.element("tr"):
                                        for h in ["Tramo", "Publicación", "Precio real", "Campaña", "Deal price previsto", "Margen previsto"]:
                                            with ui.element("th").style("text-align:left;padding:4px 6px;background:#1976d2;color:white"):
                                                ui.label(h)
                                with ui.element("tbody"):
                                    for f in seleccion:
                                        camp = campania_elegida.get(f["tramo"]) or f["candidatas"][0]
                                        if inflar:
                                            precio_lista_preview = f["precio_actual"] / (1 - badge_objetivo / 100)
                                            deal_txt = f"lista → {_fmt_moneda(precio_lista_preview)} (descuento real recién se sabe al subir)"
                                            margen_txt = "se calcula después de subir el precio"
                                        else:
                                            max_dp = camp.get("max_discounted_price")
                                            margen = _margen(max_dp, costo)
                                            deal_txt = _fmt_moneda(max_dp)
                                            margen_txt = f"${margen:,.0f} ({100*margen/max_dp:.1f}%)" if (margen is not None and max_dp) else "sin dato"
                                        with ui.element("tr").style("border-bottom:1px solid #e5e7eb"):
                                            with ui.element("td").style("padding:3px 6px"):
                                                ui.label(dict(_TRAMOS).get(f["tramo"], f["tramo"]))
                                            with ui.element("td").style("padding:3px 6px;font-family:monospace;font-size:10px"):
                                                ui.label(f["item_id"])
                                            with ui.element("td").style("padding:3px 6px;text-align:right"):
                                                ui.label(_fmt_moneda(f["precio_actual"]))
                                            with ui.element("td").style("padding:3px 6px"):
                                                ui.label(camp.get("name") or camp.get("id"))
                                            with ui.element("td").style("padding:3px 6px"):
                                                ui.label(deal_txt)
                                            with ui.element("td").style("padding:3px 6px"):
                                                ui.label(margen_txt)
                            ui.label(
                                "Se une cada publicación a su campaña pidiendo el mínimo descuento que ML "
                                "acepta (o el que resulte de inflar el precio, si se activó la opción avanzada). "
                                "Todo queda auditado en ml_escrituras y registrado para poder Desactivar después."
                            ).classes("text-xs text-gray-500 mt-2 mb-2")
                            with ui.row().classes("w-full justify-end gap-2"):
                                ui.button("Cancelar", on_click=dlg.close).props("flat")
                                ui.button(
                                    "Confirmar y activar",
                                    on_click=lambda: _ejecutar_activar_v2(
                                        dlg, item_id, sku, seleccion, campania_elegida, costo, inflar, badge_objetivo,
                                    ),
                                ).style("background:#2e7d32;color:white;font-weight:600").props("no-caps")
                    dlg.open()

                def _ejecutar_activar_v2(
                    dlg, item_id: str, sku: str, seleccion: List[Dict[str, Any]],
                    campania_elegida: Dict[str, Dict[str, Any]], costo: Optional[tuple],
                    inflar: bool, badge_objetivo: float,
                ) -> None:
                    dlg.close()
                    cl = context.client
                    items_state = [
                        {
                            "tramo": f["tramo"], "item_id": f["item_id"], "seller_sku": f["seller_sku"],
                            "mecanismo": "seller_campaign",
                            "precio_real": f["precio_actual"],
                            "precio_lista": f["precio_actual"],
                            "campaign_id": (campania_elegida.get(f["tramo"]) or f["candidatas"][0]).get("id"),
                            "campaign_type": _TIPO_CAMPANIA_JOIN,
                            "deal_price": None, "margen": None, "margen_pct": None,
                            "estado_price": "ok" if not inflar else "pendiente",
                            "estado_join": "pendiente",
                            "detalle_error": None,
                        }
                        for f in seleccion
                    ]
                    activacion_id = crear_activacion_descuento(
                        uid, sku, badge_objetivo if inflar else 0.0, _json.dumps(items_state, ensure_ascii=False),
                    )

                    async def _revertir_precio_seguro(item: Dict[str, Any], motivo: str) -> None:
                        try:
                            await run.io_bound(
                                ml_update_item_price, access_token, item["item_id"], item["precio_real"],
                                uid, item["seller_sku"], "descuentos_activar_v2_revert", item["precio_lista"],
                            )
                            item["estado_price"] = "revertido"
                        except Exception as e:
                            item["detalle_error"] = f"{motivo} -- Y FALLÓ AL REVERTIR EL PRECIO: {_detalle_error_ml(e)} -- REVISAR A MANO YA"
                            with cl:
                                ui.notify(
                                    f"🚨 {item['tramo']} ({item['item_id']}) quedó con el precio inflado y no se "
                                    f"pudo revertir automáticamente: {e} -- revisar a mano YA.",
                                    color="negative", timeout=0, multi_line=True,
                                )
                        actualizar_activacion_descuento(
                            activacion_id, _json.dumps(items_state, ensure_ascii=False), estado="error_parcial",
                        )

                    async def _correr() -> None:
                        for idx, item in enumerate(items_state):
                            iid = item["item_id"]
                            with cl:
                                ui.notify(f"[{idx+1}/{len(items_state)}] {item['tramo']}: procesando...", color="info")

                            if inflar:
                                precio_lista = round(item["precio_real"] / (1 - badge_objetivo / 100), 2)
                                try:
                                    await run.io_bound(
                                        ml_update_item_price, access_token, iid, precio_lista,
                                        uid, item["seller_sku"], "descuentos_activar_v2", item["precio_real"],
                                    )
                                    item["precio_lista"] = precio_lista
                                    item["estado_price"] = "ok"
                                    actualizar_activacion_descuento(activacion_id, _json.dumps(items_state, ensure_ascii=False))
                                except Exception as e:
                                    item["estado_price"], item["detalle_error"] = "error", _detalle_error_ml(e)
                                    actualizar_activacion_descuento(
                                        activacion_id, _json.dumps(items_state, ensure_ascii=False), estado="error_parcial",
                                    )
                                    with cl:
                                        ui.notify(
                                            f"{item['tramo']} ({iid}): no se pudo subir el precio: {e} -- sigo con las demás.",
                                            color="negative", timeout=12000, multi_line=True,
                                        )
                                    continue

                            try:
                                promos = await run.io_bound(ml_get_seller_promotions_item, access_token, iid)
                            except Exception as e:
                                item["estado_join"], item["detalle_error"] = "error", _detalle_error_ml(e)
                                actualizar_activacion_descuento(
                                    activacion_id, _json.dumps(items_state, ensure_ascii=False), estado="error_parcial",
                                )
                                with cl:
                                    ui.notify(f"{item['tramo']} ({iid}): no se pudo releer la campaña: {e}", color="negative", timeout=12000, multi_line=True)
                                if inflar:
                                    await _revertir_precio_seguro(item, "no se pudo releer la campaña tras subir el precio")
                                continue

                            camp = next(
                                (p for p in (promos or []) if p.get("id") == item["campaign_id"] and str(p.get("type")) == _TIPO_CAMPANIA_JOIN),
                                None,
                            )
                            max_dp = camp.get("max_discounted_price") if camp else None
                            if not camp or not max_dp:
                                item["estado_join"], item["detalle_error"] = "error", "la campaña ya no está disponible como candidata a este precio"
                                actualizar_activacion_descuento(
                                    activacion_id, _json.dumps(items_state, ensure_ascii=False), estado="error_parcial",
                                )
                                with cl:
                                    ui.notify(f"{item['tramo']} ({iid}): campaña no disponible a este precio.", color="negative", timeout=12000, multi_line=True)
                                if inflar:
                                    await _revertir_precio_seguro(item, "campaña no disponible tras subir el precio")
                                continue

                            deal_price = round(max_dp, 2)
                            margen = _margen(deal_price, costo)
                            margen_pct = (100 * margen / deal_price) if (margen is not None and deal_price) else None
                            item["deal_price"], item["margen"], item["margen_pct"] = deal_price, margen, margen_pct

                            if margen is not None and margen <= 0:
                                item["estado_join"] = "omitido_perdida"
                                actualizar_activacion_descuento(activacion_id, _json.dumps(items_state, ensure_ascii=False))
                                with cl:
                                    ui.notify(
                                        f"{item['tramo']} ({iid}): el descuento mínimo que acepta ML da PÉRDIDA "
                                        f"(${margen:,.0f}) -- no se activó.",
                                        color="negative", timeout=15000, multi_line=True,
                                    )
                                if inflar:
                                    await _revertir_precio_seguro(item, "margen negativo al precio mínimo aceptado")
                                continue

                            try:
                                await run.io_bound(
                                    ml_join_seller_promotion, access_token, iid, item["campaign_id"], _TIPO_CAMPANIA_JOIN, deal_price,
                                )
                            except Exception as e:
                                item["estado_join"], item["detalle_error"] = "error", _detalle_error_ml(e)
                                actualizar_activacion_descuento(
                                    activacion_id, _json.dumps(items_state, ensure_ascii=False), estado="error_parcial",
                                )
                                with cl:
                                    ui.notify(f"{item['tramo']} ({iid}): ML rechazó el join: {e}", color="negative", timeout=15000, multi_line=True)
                                if inflar:
                                    await _revertir_precio_seguro(item, "ML rechazó el join tras subir el precio")
                                continue

                            log_ml_escritura(
                                uid, item["seller_sku"], iid, "seller_campaign_join",
                                "sin promo", f"deal_price={deal_price}", "descuentos_activar_v2", "ok", None,
                            )

                            started = False
                            for _ in range(6):
                                await asyncio.sleep(2.0)
                                promos2 = await run.io_bound(ml_get_seller_promotions_item, access_token, iid)
                                if any(
                                    p.get("id") == item["campaign_id"] and str(p.get("status")) == "started"
                                    for p in (promos2 or [])
                                ):
                                    started = True
                                    break
                            item["estado_join"] = "ok" if started else "creado_sin_confirmar"
                            actualizar_activacion_descuento(activacion_id, _json.dumps(items_state, ensure_ascii=False))
                            with cl:
                                _pct_off = 100 * (item["precio_lista"] - deal_price) / item["precio_lista"] if item["precio_lista"] else 0
                                ui.notify(
                                    f"{item['tramo']} ({iid}): {'activado OK' if started else 'unido, esperando confirmación de ML'} -- "
                                    f"-{_pct_off:.1f}% → {_fmt_moneda(deal_price)}"
                                    + (f", margen ${margen:,.0f} ({margen_pct:.1f}%)" if margen is not None else ""),
                                    color="positive", timeout=8000,
                                )

                        actualizar_activacion_descuento(activacion_id, _json.dumps(items_state, ensure_ascii=False), estado="activo")
                        with cl:
                            ui.notify("Proceso de activación terminado -- revisá el detalle de cada tramo arriba.", color="positive", timeout=8000)
                            background_tasks.create(_render_activacion(item_id), name=f"activacion_refresh_{item_id}")

                    background_tasks.create(_correr())

                def _confirmar_desactivar(
                    item_id: str, sku: str, familia: List[Dict[str, Any]], checks: Dict[str, Any],
                ) -> None:
                    seleccion = [f for f in familia if f.get("_accion") == "desactivar" and checks[f["tramo"]].value]
                    if not seleccion:
                        ui.notify("No hay ningún tramo seleccionado para desactivar.", color="warning")
                        return

                    vigente = get_activacion_descuento_vigente(uid, sku)
                    tracked_by_item: Dict[str, Dict[str, Any]] = {}
                    if vigente:
                        try:
                            for it_ in _json.loads(vigente["items_json"]):
                                if it_.get("mecanismo") == "seller_campaign" and it_.get("item_id"):
                                    tracked_by_item[it_["item_id"]] = it_
                        except Exception:
                            pass

                    filas = []
                    for f in seleccion:
                        iid = f["item_id"]
                        tracked = tracked_by_item.get(iid)
                        if tracked and tracked.get("precio_real"):
                            precio_real = tracked["precio_real"]
                            origen_precio = "registrado"
                        else:
                            historial = get_historial_precio(iid, uid, limit=20)
                            sugerido = next(
                                (
                                    h["valor_nuevo"] for h in historial
                                    if not str(h.get("origen") or "").startswith(("descuentos_activar", "activar_badge"))
                                ),
                                None,
                            )
                            precio_real = float(sugerido) if sugerido else f["precio_actual"]
                            origen_precio = "sugerido" if sugerido else "sin_historial"
                        filas.append({
                            "tramo": f["tramo"], "item_id": iid, "seller_sku": f["seller_sku"],
                            "campaign_id": f["activa"].get("id"), "campaign_type": f["activa"].get("type") or _TIPO_CAMPANIA_JOIN,
                            "precio_actual_promo": f["activa"].get("price"),
                            "precio_lista_base": f["precio_actual"],
                            "precio_real": precio_real, "origen_precio": origen_precio,
                        })

                    dlg = ui.dialog()
                    with dlg:
                        with ui.card().classes("p-4 min-w-[640px] max-w-[95vw]"):
                            ui.label(f"Desactivar descuento real -- {sku}").classes("text-lg font-semibold mb-2")
                            ui.label(
                                "Confirmá o corregí el precio real de cada publicación antes de revertir -- los "
                                "marcados 'sugerido' se dedujeron del historial de auditoría, no están 100% "
                                "garantizados. Los 'registrado' vienen de una activación hecha desde esta misma "
                                "pantalla."
                            ).classes("text-xs mb-2").style("color:#e65100")
                            inputs: Dict[str, Any] = {}
                            with ui.column().classes("w-full gap-1"):
                                for fila in filas:
                                    with ui.row().classes("items-center gap-2 w-full"):
                                        ui.label(dict(_TRAMOS).get(fila["tramo"], fila["tramo"])).classes("w-20")
                                        ui.label(fila["item_id"]).classes("font-mono text-xs w-40")
                                        ui.label(f"vendiendo hoy: {_fmt_moneda(fila['precio_actual_promo'])}").classes("text-xs w-40")
                                        inp = ui.number(value=fila["precio_real"], label="Precio real a restaurar").classes("w-44")
                                        inputs[fila["tramo"]] = inp
                                        _badge_color = {
                                            "registrado": "#2e7d32", "sugerido": "#e65100", "sin_historial": "#c62828",
                                        }[fila["origen_precio"]]
                                        _badge_txt = {
                                            "registrado": "registrado", "sugerido": "sugerido -- revisar",
                                            "sin_historial": "sin historial, revisar a mano",
                                        }[fila["origen_precio"]]
                                        ui.badge(_badge_txt).style(f"background:{_badge_color}")
                            with ui.row().classes("w-full justify-end gap-2 mt-2"):
                                ui.button("Cancelar", on_click=dlg.close).props("flat")
                                ui.button(
                                    "Confirmar y desactivar",
                                    on_click=lambda: _ejecutar_desactivar_v2(dlg, item_id, sku, filas, inputs, vigente),
                                ).style("background:#c62828;color:white;font-weight:600").props("no-caps")
                    dlg.open()

                def _ejecutar_desactivar_v2(
                    dlg, item_id: str, sku: str, filas: List[Dict[str, Any]], inputs: Dict[str, Any],
                    vigente: Optional[Dict[str, Any]],
                ) -> None:
                    dlg.close()
                    cl = context.client
                    for fila in filas:
                        try:
                            fila["precio_real"] = float(inputs[fila["tramo"]].value or 0)
                        except (TypeError, ValueError):
                            fila["precio_real"] = 0.0

                    items_state = [
                        {
                            "tramo": f["tramo"], "item_id": f["item_id"], "seller_sku": f["seller_sku"],
                            "mecanismo": "seller_campaign",
                            "campaign_id": f["campaign_id"], "campaign_type": f["campaign_type"],
                            "precio_real": f["precio_real"], "precio_lista_base": f["precio_lista_base"],
                            "estado_delete": "pendiente", "estado_price": "pendiente", "detalle_error": None,
                        }
                        for f in filas
                    ]
                    activacion_id = crear_activacion_descuento(uid, sku, 0.0, _json.dumps(items_state, ensure_ascii=False))

                    async def _correr() -> None:
                        for idx, item in enumerate(items_state):
                            iid = item["item_id"]
                            with cl:
                                ui.notify(f"[{idx+1}/{len(items_state)}] {item['tramo']}: sacando de la campaña...", color="info")
                            try:
                                resp = await run.io_bound(
                                    ml_delete_seller_promotion, access_token, iid, item["campaign_id"], item["campaign_type"],
                                )
                                if resp.status_code not in (200, 404):
                                    raise RuntimeError(f"DELETE status={resp.status_code} {resp.text[:200]}")
                                item["estado_delete"] = "ok"
                                log_ml_escritura(
                                    uid, item["seller_sku"], iid, "seller_campaign_delete",
                                    "started", "eliminado", "descuentos_desactivar_v2", "ok", None,
                                )
                                actualizar_activacion_descuento(activacion_id, _json.dumps(items_state, ensure_ascii=False))
                            except Exception as e:
                                item["estado_delete"], item["detalle_error"] = "error", _detalle_error_ml(e)
                                log_ml_escritura(
                                    uid, item["seller_sku"], iid, "seller_campaign_delete",
                                    "started", "eliminado", "descuentos_desactivar_v2", "error", _detalle_error_ml(e),
                                )
                                actualizar_activacion_descuento(
                                    activacion_id, _json.dumps(items_state, ensure_ascii=False), estado="error_parcial",
                                )
                                with cl:
                                    ui.notify(
                                        f"{item['tramo']} ({iid}): no se pudo sacar de la campaña: {e} -- sigo con las demás.",
                                        color="negative", timeout=12000, multi_line=True,
                                    )
                                continue

                            if abs(item["precio_lista_base"] - item["precio_real"]) > 0.01:
                                try:
                                    await run.io_bound(
                                        ml_update_item_price, access_token, iid, item["precio_real"],
                                        uid, item["seller_sku"], "descuentos_desactivar_v2", item["precio_lista_base"],
                                    )
                                    item["estado_price"] = "ok"
                                except Exception as e:
                                    item["estado_price"], item["detalle_error"] = "error", _detalle_error_ml(e)
                                    actualizar_activacion_descuento(
                                        activacion_id, _json.dumps(items_state, ensure_ascii=False), estado="error_parcial",
                                    )
                                    with cl:
                                        ui.notify(
                                            f"{item['tramo']} ({iid}): la campaña ya se borró pero no se pudo bajar "
                                            f"el precio: {e}",
                                            color="negative", timeout=15000, multi_line=True,
                                        )
                                    continue
                            else:
                                item["estado_price"] = "sin_cambios"

                            actualizar_activacion_descuento(activacion_id, _json.dumps(items_state, ensure_ascii=False))
                            with cl:
                                ui.notify(f"{item['tramo']} ({iid}) desactivado OK, precio real {_fmt_moneda(item['precio_real'])}.", color="positive")

                        actualizar_activacion_descuento(
                            activacion_id, _json.dumps(items_state, ensure_ascii=False), estado="revertido", marcar_revertido=True,
                        )
                        # Si venía de una activación registrada previa (misma pantalla), la
                        # cerramos también -- que no quede una fila vieja en 'activo' dando
                        # una falsa sensación de que sigue pendiente.
                        if vigente and vigente.get("estado") in ("activo", "error_parcial"):
                            actualizar_activacion_descuento(
                                vigente["id"], vigente["items_json"], estado="revertido", marcar_revertido=True,
                            )
                        with cl:
                            ui.notify("Desactivación terminada.", color="positive", timeout=8000)
                            background_tasks.create(_render_activacion(item_id), name=f"activacion_refresh_{item_id}")

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
                    background_tasks.create(_render_activacion(item_id), name=f"activacion_descuentos_{item_id}")
                    background_tasks.create(
                        _render_mayorista(item_id), name=f"mayorista_descuentos_{item_id}"
                    )

                sel.on_value_change(_on_producto_change)
                precio_deseado_inp.on_value_change(_recalcular_precio)
                descuento_inp.on_value_change(_recalcular_precio)

            background_tasks.create(_cargar(), name="cargar_descuentos")
