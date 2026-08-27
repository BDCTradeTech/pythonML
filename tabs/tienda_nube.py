"""
tabs/tienda_nube.py — Vinculación: cruce de publicaciones de MercadoLibre contra
productos de Tienda Nube por seller_sku. SOLO LECTURA: no escribe nada en ML ni en TN.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional

from nicegui import app, background_tasks, context, run, ui

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


def _fmt_precio_ars(val: Any) -> str:
    """Formato argentino de presentación: $ + punto como separador de miles.
    Sin decimales si son cero (el sort numérico usa el valor crudo, no esto)."""
    if val is None:
        return "—"
    try:
        n = float(str(val).replace(",", "."))
    except (TypeError, ValueError):
        return "—"
    entero = int(n)
    dec = round(abs(n - entero) * 100)
    parte_entera = f"{entero:,}".replace(",", ".")
    if dec == 0:
        return f"${parte_entera}"
    return f"${parte_entera},{dec:02d}"


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
            # ML agrupa el stock por user_product: todas las publicaciones del SKU
            # comparten el mismo available_quantity, por eso alcanza con el primero.
            "ml_stock": ml_m[0].get("available_quantity") if ml_m else None,
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
        header_div_vinc = ui.element("div").style("width:100%;overflow:hidden")
        table_container = ui.element("div").style("width:100%;height:calc(100vh - 454px);overflow-y:scroll;overflow-x:auto")
        _hid_v = header_div_vinc.id
        _cid_v = table_container.id
        _sync_vinc_client = context.client

        async def _setup_sync_vinc() -> None:
            with _sync_vinc_client:
                await ui.run_javascript(
                    f"(function(){{"
                    f"var body=document.getElementById('c{_cid_v}');"
                    f"var hdr=document.getElementById('c{_hid_v}');"
                    f"if(!body||!hdr)return;"
                    f"body.addEventListener('scroll',function(){{hdr.scrollLeft=body.scrollLeft;}});"
                    f"function _sg(){{hdr.style.paddingRight=(body.offsetWidth-body.clientWidth)+'px';}}"
                    f"_sg();new ResizeObserver(_sg).observe(body);"
                    f"}})();"
                )
        background_tasks.create(_setup_sync_vinc())

        columns = [
            {"name": "sku", "label": "SKU", "field": "sku", "align": "left"},
            {"name": "plataforma", "label": "Plataforma", "field": "plataforma", "align": "center"},
            {"name": "duplicado", "label": "Duplicado", "field": "duplicado", "align": "center"},
            {"name": "ml_stock", "label": "ML — Stock", "field": "ml_stock", "align": "right"},
            {"name": "ml_status", "label": "ML — Estado", "field": "ml_status", "align": "center"},
            {"name": "ml_nombre", "label": "ML — Nombre", "field": "ml_nombre", "align": "left"},
            {"name": "ml_precio", "label": "ML — Precio", "field": "ml_precio", "align": "right"},
            {"name": "tn_nombre", "label": "TN — Nombre", "field": "tn_nombre", "align": "center"},
            {"name": "tn_precio", "label": "TN — Precio", "field": "tn_precio", "align": "right"},
            {"name": "tn_stock", "label": "TN — Stock", "field": "tn_stock", "align": "right"},
        ]
        _col_w_vinc = {
            "sku": "110px", "plataforma": "130px", "duplicado": "100px",
            "ml_stock": "70px", "ml_status": "110px", "ml_nombre": "260px",
            "ml_precio": "90px", "tn_nombre": "260px", "tn_precio": "90px", "tn_stock": "70px",
        }

        def _build_colgroup_vinc() -> None:
            with ui.element("colgroup"):
                for col in columns:
                    ui.element("col").style(f"width:{_col_w_vinc.get(col['name'], '90px')}")

        sort_col_ref: Dict[str, Any] = {"val": "sku"}
        sort_asc_ref: Dict[str, bool] = {"val": True}

        def _sort_key_vinc(row: dict, col_name: str) -> Any:
            if col_name == "plataforma":
                return _PLATAFORMA_LABELS.get(row.get("plataforma"), "")
            if col_name == "duplicado":
                return 1 if (row.get("duplicado_ml") or row.get("duplicado_tn")) else 0
            if col_name in ("ml_stock", "ml_publicaciones", "tn_variantes"):
                return int(row.get(col_name) or 0)
            if col_name in ("ml_precio", "tn_precio", "tn_stock"):
                v = row.get(col_name)
                try:
                    return float(str(v).replace(",", ".")) if v is not None else -1.0
                except (ValueError, TypeError):
                    return -1.0
            return str(row.get(col_name) or "").lower()

        def _on_sort_click_vinc(col_name: str) -> None:
            if sort_col_ref.get("val") == col_name:
                sort_asc_ref["val"] = not sort_asc_ref.get("val", True)
            else:
                sort_col_ref["val"] = col_name
                sort_asc_ref["val"] = True
            _render_tabla()

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

            visibles = sorted(
                visibles,
                key=lambda r: _sort_key_vinc(r, sort_col_ref.get("val", "sku")),
                reverse=not sort_asc_ref.get("val", True),
            )

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
                    # el sort ya corrió sobre el valor crudo (visibles, arriba) -- esto
                    # solo formatea la presentación, no altera el dato ni el orden
                    "tn_precio": _fmt_precio_ars(f["tn_precio"]),
                    "tn_stock": f["tn_stock"] if f["tn_stock"] is not None else "—",
                    "ml_precio": _fmt_precio_ars(f["ml_precio"]),
                    "ml_stock": f["ml_stock"] if f["ml_stock"] is not None else "—",
                })

            header_div_vinc.clear()
            table_container.clear()
            if not rows:
                with table_container:
                    ui.label("Sin resultados para este filtro.").classes("text-sm text-gray-400")
                return

            with header_div_vinc:
                with ui.element("table").style("table-layout:fixed;width:100%;border-collapse:separate;border-spacing:0"):
                    _build_colgroup_vinc()
                    with ui.element("thead"):
                        with ui.element("tr").classes("bg-primary text-white font-semibold"):
                            for col in columns:
                                with ui.element("th").classes("px-2 py-1 border text-center").style("line-height:1.1"):
                                    ui.button(
                                        col["label"], on_click=lambda c=col["name"]: _on_sort_click_vinc(c)
                                    ).props("flat dense no-caps").classes(
                                        "text-white hover:bg-white/20 cursor-pointer font-semibold"
                                    ).style(
                                        "white-space:nowrap;overflow:hidden;text-overflow:ellipsis;"
                                        "max-width:100%;min-height:0;padding:2px 6px;line-height:1.1"
                                    )

            with table_container:
                with ui.element("table").style("table-layout:fixed;width:100%;border-collapse:separate;border-spacing:0"):
                    _build_colgroup_vinc()
                    with ui.element("tbody"):
                        for row in rows:
                            with ui.element("tr").classes("border-t border-gray-200 hover:bg-gray-50"):
                                for col in columns:
                                    val = row.get(col["field"])
                                    align = "text-right" if col["align"] == "right" else "text-center" if col["align"] == "center" else "text-left"
                                    with ui.element("td").classes(f"px-2 py-1 border-b border-gray-100 {align} text-xs").style("white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:0"):
                                        ui.label(str(val) if val is not None else "—")

                _recalc_padding_vinc_client = context.client

                async def _recalc_padding_vinc() -> None:
                    with _recalc_padding_vinc_client:
                        await ui.run_javascript(
                            f"(function(){{"
                            f"var body=document.getElementById('c{_cid_v}');"
                            f"var hdr=document.getElementById('c{_hid_v}');"
                            f"if(body&&hdr){{hdr.style.paddingRight=(body.offsetWidth-body.clientWidth)+'px';}}"
                            f"}})();"
                        )
                background_tasks.create(_recalc_padding_vinc())

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
