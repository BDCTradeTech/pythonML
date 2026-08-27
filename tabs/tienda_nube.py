"""
tabs/tienda_nube.py — Vinculación: cruce de publicaciones de MercadoLibre contra
productos de Tienda Nube por seller_sku. SOLO LECTURA: no escribe nada en ML ni en TN.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional

from nicegui import app, run, ui

from db import (
    get_tiendanube_credentials,
    get_tiendanube_productos,
    replace_tiendanube_productos,
    get_tiendanube_sync_status,
    set_tiendanube_sync_status,
)
from ml_api import get_ml_access_token, ml_get_my_items
from tiendanube_api import tiendanube_list_products_with_variants

_PLATAFORMA_LABELS = {
    "en_ambos": "En ambos",
    "solo_ml": "Solo en Mercado Libre",
    "solo_tn": "Solo en Tienda Nube",
}


def _require_login() -> Optional[Dict[str, Any]]:
    user = app.storage.user.get("user")
    if not user:
        ui.notify("Debes iniciar sesión", color="negative")
    return user


def _cruzar(ml_items: List[dict], tn_rows: List[dict]) -> tuple:
    """Cruce por seller_sku (case-insensitive). Dos ejes independientes:

    - plataforma (en_ambos/solo_ml/solo_tn): en qué plataformas existe el SKU.
    - duplicado_ml / duplicado_tn: anomalía DENTRO de una plataforma -- más de un
      catalog_product_id (ML) o product_id (TN) DISTINTO comparte el mismo SKU.

    Ítems de ML sin seller_sku cargado no entran al cruce -- no hay clave con la cual
    cruzarlos -- y se cuentan aparte para que no desaparezcan en silencio.

    IMPORTANTE: "duplicado" NO se basa en cuántas publicaciones tiene un SKU. La
    estrategia de cuotas (contado/3/6/9/12) hace que la mayoría de los SKU tengan
    entre 5 y 10 publicaciones activas sin que eso sea una anomalía -- verificado
    contra datos reales de la cuenta: 123 de 140 SKUs reales tienen 10 publicaciones
    (5 tiers de cuotas × copia catálogo + copia sin catálogo, mismo catalog_product_id
    las 10), y otros 7 tienen 5 publicaciones sin catalog_product_id (mismo patrón,
    sin vínculo a catálogo). Ninguno de esos 130 casos es un duplicado real."""
    ml_by_sku: Dict[str, List[dict]] = defaultdict(list)
    tn_by_sku: Dict[str, List[dict]] = defaultdict(list)
    sin_sku_ml = 0
    for it in ml_items:
        sku = (it.get("seller_sku") or "").strip().lower()
        if sku:
            ml_by_sku[sku].append(it)
        else:
            sin_sku_ml += 1
    for r in tn_rows:
        sku = (r.get("sku") or "").strip().lower()
        if sku:
            tn_by_sku[sku].append(r)

    filas = []
    for sku in sorted(set(ml_by_sku) | set(tn_by_sku)):
        ml_m = ml_by_sku.get(sku, [])
        tn_m = tn_by_sku.get(sku, [])
        if ml_m and tn_m:
            plataforma = "en_ambos"
        elif ml_m:
            plataforma = "solo_ml"
        else:
            plataforma = "solo_tn"

        cpids_ml = {(it.get("catalog_product_id") or "").strip() for it in ml_m}
        cpids_ml.discard("")
        duplicado_ml = len(cpids_ml) > 1

        pids_tn = {str(r.get("product_id") or "").strip() for r in tn_m}
        pids_tn.discard("")
        duplicado_tn = len(pids_tn) > 1

        filas.append({
            "sku": sku,
            "plataforma": plataforma,
            "duplicado_ml": duplicado_ml,
            "duplicado_tn": duplicado_tn,
            "ml_publicaciones": len(ml_m),
            "tn_variantes": len(tn_m),
            "ml_nombre": ml_m[0].get("title", "") if ml_m else "",
            "ml_precio": ml_m[0].get("price") if ml_m else None,
            "ml_status": ml_m[0].get("status", "") if ml_m else "",
            "tn_nombre": tn_m[0].get("nombre", "") if tn_m else "",
            "tn_precio": tn_m[0].get("precio") if tn_m else None,  # string crudo de TN, sin convertir
            "tn_stock": tn_m[0].get("stock") if tn_m else None,
        })
    return filas, sin_sku_ml


def build_tab_vinculacion(container) -> None:
    container.clear()
    user = _require_login()
    if not user:
        return
    uid = user["id"]

    access_token = get_ml_access_token(uid)
    tn_creds = get_tiendanube_credentials(uid)
    if not access_token:
        with container:
            ui.label("⚠️ No tenés MercadoLibre vinculado. Andá a Configuración.").classes("text-warning")
        return
    if not tn_creds or not tn_creds.get("store_id") or not tn_creds.get("access_token") or not tn_creds.get("auth_header_style"):
        with container:
            ui.label("⚠️ No tenés Tienda Nube vinculada (o falta 'Probar conexión' en Configuración).").classes("text-warning")
        return

    with container:
        ui.label("Tienda Nube — Vinculación").classes("text-xl font-bold")

        status_container = ui.column().classes("w-full")

        with ui.row().classes("w-full items-center gap-3 flex-wrap"):
            filtro_opciones = {"todos": "Todos", **_PLATAFORMA_LABELS, "duplicados": "Con SKU duplicado (ML o TN)"}
            filtro_sel = ui.select(filtro_opciones, value="todos", label="Estado").props("dense outlined").classes("w-64")
            incluir_pausadas_chk = ui.checkbox("Incluir pausadas (ML)", value=False)
            ui.space()
            actualizar_btn = ui.button("Actualizar").props("unelevated dense no-caps icon=refresh").classes("text-xs")

        contadores_container = ui.row().classes("w-full gap-2 flex-wrap")
        tabla_container = ui.column().classes("w-full")

        def _render_status() -> None:
            status_container.clear()
            st = get_tiendanube_sync_status(uid)
            with status_container:
                if not st or not st.get("last_sync_at"):
                    ui.label("Nunca se sincronizó Tienda Nube en esta pantalla — apretá Actualizar.").classes("text-warning text-sm")
                elif st.get("ok"):
                    ui.label(
                        f"Última sincronización OK: {st['last_sync_at'][:19].replace('T', ' ')} "
                        f"({st.get('items_leidos', 0)} variantes leídas de Tienda Nube)"
                    ).classes("text-xs text-gray-600")
                else:
                    with ui.row().classes("w-full items-center gap-2 p-2 rounded").style("background:#fef2f2;border:1px solid #fecaca"):
                        ui.icon("error", color="negative", size="sm")
                        ui.label(
                            f"Sincronización incompleta/fallida ({st['last_sync_at'][:19].replace('T', ' ')}): {st.get('error') or 'sin detalle'}"
                        ).classes("text-sm text-negative")

        def _render_tabla() -> None:
            ml_data = ml_get_my_items(access_token, include_paused=incluir_pausadas_chk.value)
            tn_rows = get_tiendanube_productos(uid)
            filas, sin_sku_ml = _cruzar(ml_data.get("results", []), tn_rows)

            filtro = filtro_sel.value
            if filtro == "todos":
                visibles = filas
            elif filtro == "duplicados":
                visibles = [f for f in filas if f["duplicado_ml"] or f["duplicado_tn"]]
            else:
                visibles = [f for f in filas if f["plataforma"] == filtro]

            contadores_container.clear()
            with contadores_container:
                for key, label in _PLATAFORMA_LABELS.items():
                    n = sum(1 for f in filas if f["plataforma"] == key)
                    ui.badge(f"{label}: {n}", color="primary").props("outline")
                n_dup = sum(1 for f in filas if f["duplicado_ml"] or f["duplicado_tn"])
                ui.badge(f"Con SKU duplicado: {n_dup}", color="negative" if n_dup else "positive").props("outline")
                if sin_sku_ml:
                    ui.badge(f"ML sin SKU cargado (excluidos del cruce): {sin_sku_ml}", color="warning").props("outline")
                ui.badge(f"Productos en Tienda Nube (variantes): {len(tn_rows)}", color="secondary").props("outline")

            tabla_container.clear()
            with tabla_container:
                columns = [
                    {"name": "sku", "label": "SKU", "field": "sku", "align": "left", "sortable": True},
                    {"name": "plataforma", "label": "Plataforma", "field": "plataforma", "align": "left", "sortable": True},
                    {"name": "duplicado", "label": "Duplicado", "field": "duplicado", "align": "left"},
                    {"name": "ml_publicaciones", "label": "ML — # pub.", "field": "ml_publicaciones", "align": "right"},
                    {"name": "ml_status", "label": "ML — Estado pub.", "field": "ml_status", "align": "left"},
                    {"name": "ml_nombre", "label": "ML — Nombre", "field": "ml_nombre", "align": "left"},
                    {"name": "ml_precio", "label": "ML — Precio", "field": "ml_precio", "align": "right"},
                    {"name": "tn_nombre", "label": "TN — Nombre", "field": "tn_nombre", "align": "left"},
                    {"name": "tn_precio", "label": "TN — Precio", "field": "tn_precio", "align": "right"},
                    {"name": "tn_stock", "label": "TN — Stock", "field": "tn_stock", "align": "right"},
                ]
                rows = []
                for f in visibles:
                    dup_partes = []
                    if f["duplicado_ml"]:
                        dup_partes.append("ML")
                    if f["duplicado_tn"]:
                        dup_partes.append("TN")
                    rows.append({
                        **f,
                        "plataforma": _PLATAFORMA_LABELS[f["plataforma"]],
                        "duplicado": ("⚠ " + "+".join(dup_partes)) if dup_partes else "—",
                        # valores crudos: TN precio viene como string ("25.00") de la API,
                        # ML precio es numérico -- se muestran tal cual, sin normalizar
                        "tn_precio": f["tn_precio"] if f["tn_precio"] is not None else "—",
                        "tn_stock": f["tn_stock"] if f["tn_stock"] is not None else "—",
                        "ml_precio": f["ml_precio"] if f["ml_precio"] is not None else "—",
                    })
                if rows:
                    ui.table(columns=columns, rows=rows, row_key="sku").classes("w-full")
                else:
                    ui.label("Sin resultados para este filtro.").classes("text-sm text-gray-400")

        async def _actualizar() -> None:
            actualizar_btn.props("loading")
            ui.notify("Leyendo Tienda Nube...", color="info")
            try:
                filas_tn, error = await run.io_bound(
                    tiendanube_list_products_with_variants,
                    tn_creds["store_id"], tn_creds["access_token"], tn_creds["auth_header_style"],
                )
                replace_tiendanube_productos(uid, filas_tn)
                set_tiendanube_sync_status(uid, ok=(error is None), error=error, items_leidos=len(filas_tn))
                if error:
                    ui.notify(f"Sincronización incompleta -- ver detalle en pantalla", color="negative")
                else:
                    ui.notify(f"OK: {len(filas_tn)} variantes leídas de Tienda Nube", color="positive")
            finally:
                actualizar_btn.props(remove="loading")
            _render_status()
            _render_tabla()

        actualizar_btn.on_click(_actualizar)
        filtro_sel.on_value_change(lambda: _render_tabla())
        incluir_pausadas_chk.on_value_change(lambda: _render_tabla())

        _render_status()
        _render_tabla()
