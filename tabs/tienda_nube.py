"""
tabs/tienda_nube.py — Vinculación: cruce de publicaciones de MercadoLibre contra
productos de Tienda Nube por seller_sku. SOLO LECTURA: no escribe nada en ML ni en TN.
"""
from __future__ import annotations

import re
from collections import defaultdict
from typing import Any, Dict, List, Optional

from nicegui import app, background_tasks, context, run, ui

from db import (
    get_tiendanube_credentials,
    get_tiendanube_productos,
    replace_tiendanube_productos,
    get_tiendanube_sync_status,
    set_tiendanube_sync_status,
    upsert_tiendanube_producto,
    get_tn_categoria_mapeada,
    set_tn_categoria_mapeo,
)
from ml_api import get_ml_access_token, ml_get_my_items, ml_get_item, ml_get_item_description
from tiendanube_api import (
    tiendanube_list_products_with_variants,
    tiendanube_get,
    tiendanube_create_product,
    tiendanube_find_by_sku,
)

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


def _peso_gramos_desde_atributos(attrs: List[dict]) -> Optional[float]:
    """Busca el atributo WEIGHT de ML (name='Peso') y devuelve el valor en GRAMOS.
    ML lo da como texto con unidad libre (ej. '456 g', '8.7 g') -- nunca se asume
    la unidad, se la reconoce del propio texto. Devuelve None si no está cargado
    o si la unidad no se pudo reconocer (mejor no crear el campo que inventar)."""
    for a in attrs or []:
        if (a.get("id") or "").upper() != "WEIGHT":
            continue
        texto = str(a.get("value_name") or "").strip().lower()
        m = re.match(r"^([\d.,]+)\s*(kilogramos?|kg|gramos?|gr|g)\b", texto)
        if not m:
            return None
        valor = float(m.group(1).replace(",", "."))
        unidad = m.group(2)
        if unidad.startswith("kilo") or unidad == "kg":
            return valor * 1000.0
        return valor
    return None


def _kg_desde_gramos(gramos: float) -> str:
    """Convierte a la unidad que Tiendanube espera para el peso: KILOGRAMOS.

    ⚠️ INFERENCIA, NO CONFIRMADO TODAVÍA CONTRA EL RECURSO PRODUCT: la cita textual
    "Order's total weight, in kilograms" está en la documentación oficial del
    recurso ORDER (no del recurso Product/variant, que no aclara la unidad). Es
    razonable asumir que ambos recursos comparten la misma unidad en toda la API,
    pero queda como inferencia hasta que se cree el primer producto real y se
    verifique el peso mostrado en el panel de Tiendanube contra el peso real del
    paquete. Si algún día se detecta que Product usa otra unidad, corregir SOLO
    esta función -- todo el resto del flujo ya trabaja en gramos hasta este punto."""
    return f"{gramos / 1000.0:.3f}"


def _url_maxima_resolucion(url: str) -> str:
    """Reemplaza el sufijo de tamaño de una URL de imagen de ML por -F.

    Verificado EMPÍRICAMENTE 2026-08-27 (descargando las fotos reales y midiendo
    los píxeles con PIL, en 3 fotos de 3 ítems distintos): el sufijo -O (el que
    trae url/secure_url tal cual) da el tamaño CHICO -- coincide exacto con el
    campo "size" del JSON (ej. 500x500, 445x500, 364x500). El sufijo -F da la
    máxima resolución real -- coincide exacto, píxel a píxel, con el campo
    "max_size" del JSON en los 3 casos probados (ej. 1200x1200, 758x852, 855x1175).
    La suposición anterior ("-O ya es la máxima calidad") era incorrecta."""
    return re.sub(r"-[A-Za-z](\.\w+)$", r"-F\1", url)


def _texto_a_html(texto: str) -> str:
    """Envoltorio HTML mínimo para la descripción -- Tiendanube espera HTML por
    idioma (doc oficial: {"es": "<p>...</p>"}), la descripción de ML viene en texto
    plano. Solo se envuelven párrafos/saltos de línea, sin agregar más formato."""
    texto = (texto or "").strip()
    if not texto:
        return ""
    parrafos = [p.strip() for p in texto.split("\n\n") if p.strip()] or [texto]
    return "".join(f"<p>{p.replace(chr(10), '<br>')}</p>" for p in parrafos)


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
            {"name": "acciones", "label": "Acciones", "field": "acciones", "align": "center", "sortable": False},
        ]
        _col_w_vinc = {
            "sku": "110px", "plataforma": "130px", "duplicado": "100px",
            "ml_stock": "70px", "ml_status": "110px", "ml_nombre": "260px",
            "ml_precio": "90px", "tn_nombre": "260px", "tn_precio": "90px", "tn_stock": "70px",
            "acciones": "170px",
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
            ml_items_actuales = ml_data.get("results", [])
            tn_rows = get_tiendanube_productos(uid)
            filas, sin_sku_ml = _cruzar(ml_items_actuales, tn_rows)

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
                    "puede_crear_tn": f["plataforma"] == "solo_ml",
                })

            def _abrir_popup_crear(sku: str, ml_items: List[dict]) -> None:
                """Popup de creación asistida, UN producto por vez -- no hay botón
                de sincronización masiva. Reconstruye el grupo del SKU desde los
                ítems de ML ya cargados (misma agrupación que _cruzar, sin tocarla)."""
                ml_m = [it for it in ml_items if (it.get("seller_sku") or "").strip().lower() == sku]
                if not ml_m:
                    ui.notify("No se encontraron publicaciones de ML para este SKU.", color="negative")
                    return
                propia = next(
                    (x for x in ml_m if not x.get("catalog_listing")
                     and str(x.get("listing_type_id") or "").lower() == "gold_special"),
                    None,
                )
                catalogo = next((x for x in ml_m if x.get("catalog_listing")), None)
                fuente = propia or catalogo or ml_m[0]
                precios_validos = [x.get("price") for x in ml_m if x.get("price") is not None]
                precio_min = min(precios_validos) if precios_validos else None
                stock_pool = ml_m[0].get("available_quantity")
                category_id = ml_m[0].get("category_id")

                with ui.dialog().props("persistent") as dlg, ui.card().classes("w-[720px] max-w-full"):
                    cuerpo = ui.column().classes("w-full gap-2")
                    with cuerpo:
                        ui.label("Crear en Tienda Nube").classes("text-lg font-bold")
                        ui.label(f"SKU: {sku}").classes("text-sm text-gray-600 font-mono")
                        with ui.row().classes("w-full items-center gap-2 py-4"):
                            ui.spinner(size="sm")
                            ui.label("Cargando datos de MercadoLibre y Tienda Nube...")
                dlg.open()

                async def _cargar() -> None:
                    full = await run.io_bound(ml_get_item, access_token, fuente.get("id"))
                    descripcion = await run.io_bound(ml_get_item_description, access_token, fuente.get("id"))
                    cats_resp = await run.io_bound(
                        tiendanube_get, tn_creds["store_id"], tn_creds["access_token"],
                        tn_creds["auth_header_style"], "categories",
                    )
                    categorias: List[tuple] = []
                    if cats_resp.ok:
                        for c in cats_resp.json():
                            nombre_c = c.get("name")
                            if isinstance(nombre_c, dict):
                                nombre_c = nombre_c.get("es") or next(iter(nombre_c.values()), str(c.get("id")))
                            categorias.append((str(c.get("id")), nombre_c))
                    cat_options = dict(categorias)

                    attrs = (full or {}).get("attributes") or []
                    peso_g = _peso_gramos_desde_atributos(attrs)
                    pictures = (full or {}).get("pictures") or []
                    categoria_sugerida = get_tn_categoria_mapeada(uid, str(category_id)) if category_id else None
                    if categoria_sugerida not in cat_options:
                        categoria_sugerida = None

                    cuerpo.clear()
                    with cuerpo:
                        ui.label("Crear en Tienda Nube").classes("text-lg font-bold")
                        ui.label(f"SKU: {sku}").classes("text-sm text-gray-600 font-mono")

                        if not cats_resp.ok:
                            ui.label(
                                f"⚠️ No se pudieron leer las categorías de Tienda Nube (HTTP {cats_resp.status_code})."
                            ).classes("text-sm text-negative")

                        error_area = ui.column().classes("w-full")

                        ui.label("Nombre").classes("text-sm font-semibold mt-2")
                        nombre_input = ui.input(value="").props("outlined dense").classes("w-full")
                        with ui.row().classes("w-full gap-2"):
                            with ui.column().classes("flex-1 border rounded p-2"):
                                ui.label("Propia").classes("text-xs text-gray-500")
                                ui.label(propia.get("title") if propia else "— no hay publicación propia —").classes("text-sm")
                                if propia:
                                    ui.button(
                                        "Usar este", on_click=lambda: nombre_input.set_value(propia.get("title", ""))
                                    ).props("flat dense no-caps size=sm")
                            with ui.column().classes("flex-1 border rounded p-2"):
                                ui.label("Catálogo").classes("text-xs text-gray-500")
                                ui.label(catalogo.get("title") if catalogo else "— no hay publicación de catálogo —").classes("text-sm")
                                if catalogo:
                                    ui.button(
                                        "Usar este", on_click=lambda: nombre_input.set_value(catalogo.get("title", ""))
                                    ).props("flat dense no-caps size=sm")

                        ui.label("Descripción").classes("text-sm font-semibold mt-2")
                        descripcion_input = ui.textarea(value=descripcion).props("outlined dense").classes("w-full").style("min-height:120px")

                        ui.label("Categoría (Tiendanube)").classes("text-sm font-semibold mt-2")
                        categoria_sel = ui.select(cat_options, value=categoria_sugerida).props("outlined dense").classes("w-full")
                        if categoria_sugerida:
                            ui.label("Preseleccionada por el mapeo aprendido de esta categoría de ML.").classes("text-xs text-gray-500")

                        with ui.row().classes("w-full gap-2"):
                            with ui.column().classes("flex-1"):
                                ui.label("Precio (mínimo del grupo -- contado)").classes("text-sm font-semibold")
                                precio_input = ui.number(value=precio_min, format="%.2f").props("outlined dense").classes("w-full")
                            with ui.column().classes("flex-1"):
                                ui.label("Stock (pool de ML)").classes("text-sm font-semibold")
                                stock_input = ui.number(value=stock_pool, format="%.0f").props("outlined dense").classes("w-full")

                        with ui.row().classes("w-full gap-3 items-start"):
                            with ui.column().classes("flex-1"):
                                ui.label("Peso (gramos) -- obligatorio").classes("text-sm font-semibold")
                                peso_input = ui.number(value=peso_g, format="%.1f").props("outlined dense").classes("w-full")
                                peso_kg_lbl = ui.label("").classes("text-xs text-gray-500")
                                if peso_g is None:
                                    ui.label(
                                        "⚠️ ML no tiene el peso cargado para esta publicación. "
                                        "Sin peso no se puede crear -- cargalo a mano."
                                    ).classes("text-xs text-negative")

                                def _actualizar_peso_kg() -> None:
                                    v = peso_input.value
                                    peso_kg_lbl.set_text(
                                        f"= {float(v) / 1000.0:.3f} kg (Tiendanube espera kilogramos -- "
                                        f"verificá el peso en el panel de TN al ver el primer producto creado)"
                                        if v is not None else ""
                                    )
                                peso_input.on_value_change(lambda *_: _actualizar_peso_kg())
                                _actualizar_peso_kg()
                            # Largo/ancho/alto: ML no trae esta info hoy (0/140 verificado)
                            # -- el payload ya deja los 3 campos listos (ver _submit),
                            # agregar los inputs cuando haya dato real es trivial.

                        ui.label("Imágenes (se envían en máxima resolución -- sufijo -F)").classes("text-sm font-semibold mt-2")
                        imagenes_checks: List[tuple] = []
                        with ui.row().classes("w-full flex-wrap gap-2"):
                            if not pictures:
                                ui.label("Esta publicación no tiene fotos.").classes("text-xs text-gray-400")
                            for p in pictures:
                                url_chica = p.get("secure_url") or p.get("url") or ""
                                if not url_chica:
                                    continue
                                url_max = _url_maxima_resolucion(url_chica)
                                with ui.column().classes("items-center gap-1"):
                                    ui.image(url_max).classes("rounded border").style("width:80px;height:80px;object-fit:cover")
                                    chk = ui.checkbox("Incluir", value=True)
                                    imagenes_checks.append((url_max, chk))

                        ui.label(f"SKU: {sku}").classes("text-xs text-gray-500 font-mono mt-2")

                        with ui.row().classes("w-full justify-end gap-2 mt-3"):
                            ui.button("Cancelar", on_click=lambda: dlg.close()).props("flat no-caps")
                            crear_btn = ui.button("Crear en Tienda Nube").props("unelevated no-caps")

                    async def _submit() -> None:
                        error_area.clear()
                        nombre_val = (nombre_input.value or "").strip()
                        categoria_val = categoria_sel.value
                        precio_val = precio_input.value
                        stock_val = stock_input.value
                        peso_val = peso_input.value

                        faltantes = []
                        if not nombre_val:
                            faltantes.append("Nombre")
                        if not categoria_val:
                            faltantes.append("Categoría")
                        if precio_val is None or float(precio_val) <= 0:
                            faltantes.append("Precio")
                        if peso_val is None or float(peso_val) <= 0:
                            faltantes.append("Peso (obligatorio -- Tiendanube lo necesita para cotizar envíos)")
                        if faltantes:
                            with error_area:
                                ui.label("Faltan campos: " + ", ".join(faltantes)).classes("text-sm text-negative")
                            return

                        crear_btn.props("loading")
                        try:
                            existente = await run.io_bound(
                                tiendanube_find_by_sku, tn_creds["store_id"], tn_creds["access_token"],
                                tn_creds["auth_header_style"], sku,
                            )
                            if existente:
                                with error_area:
                                    ui.label(
                                        f"Ya existe en Tienda Nube (product_id={existente['product_id']}, "
                                        f"variant_id={existente['variant_id']}) -- no se creó ningún duplicado."
                                    ).classes("text-sm text-negative")

                                    def _vincular_existente(ex=existente) -> None:
                                        upsert_tiendanube_producto(
                                            uid, ex["variant_id"], ex["product_id"], ex["sku"], nombre_val, None, None,
                                        )
                                        ui.notify("Vinculado al producto existente.", color="positive")
                                        dlg.close()
                                        _render_tabla()

                                    ui.button("Vincular al existente", on_click=_vincular_existente).props("unelevated no-caps dense")
                                return

                            imagenes_payload = [{"src": url} for url, chk in imagenes_checks if chk.value]
                            payload = {
                                "name": {"es": nombre_val},
                                "description": {"es": _texto_a_html(descripcion_input.value or "")},
                                "categories": [int(categoria_val)],
                                "published": True,
                                "images": imagenes_payload,
                                "variants": [{
                                    "price": f"{float(precio_val):.2f}",
                                    "stock": int(stock_val) if stock_val is not None else 0,
                                    "stock_management": True,
                                    "weight": _kg_desde_gramos(float(peso_val)),
                                    "sku": sku,
                                    "width": None,
                                    "height": None,
                                    "depth": None,
                                }],
                            }
                            creado, error = await run.io_bound(
                                tiendanube_create_product, tn_creds["store_id"], tn_creds["access_token"],
                                tn_creds["auth_header_style"], payload,
                            )
                            if error:
                                with error_area:
                                    ui.label(f"Error al crear el producto: {error}").classes("text-sm text-negative")
                                return

                            variantes_creadas = (creado or {}).get("variants") or []
                            if not variantes_creadas:
                                with error_area:
                                    ui.label(
                                        "Tiendanube respondió OK pero sin variantes en el cuerpo -- no se guardó "
                                        f"ningún vínculo porque no hay variant_id. Revisá el producto "
                                        f"{(creado or {}).get('id')} manualmente en el panel."
                                    ).classes("text-sm text-negative")
                                return
                            variante = variantes_creadas[0]

                            upsert_tiendanube_producto(
                                uid, str(variante.get("id")), str(creado.get("id")),
                                variante.get("sku") or sku, nombre_val,
                                variante.get("price"), variante.get("stock"),
                            )
                            if category_id:
                                set_tn_categoria_mapeo(uid, str(category_id), str(categoria_val))

                            ui.notify(f"Producto creado en Tiendanube (id={creado.get('id')}).", color="positive")
                            dlg.close()
                            _render_tabla()
                        finally:
                            crear_btn.props(remove="loading")

                    crear_btn.on_click(_submit)

                background_tasks.create(_cargar())

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
                                    if col.get("sortable", True):
                                        ui.button(
                                            col["label"], on_click=lambda c=col["name"]: _on_sort_click_vinc(c)
                                        ).props("flat dense no-caps").classes(
                                            "text-white hover:bg-white/20 cursor-pointer font-semibold"
                                        ).style(
                                            "white-space:nowrap;overflow:hidden;text-overflow:ellipsis;"
                                            "max-width:100%;min-height:0;padding:2px 6px;line-height:1.1"
                                        )
                                    else:
                                        ui.label(col["label"]).classes("font-semibold").style("line-height:1.1")

            with table_container:
                with ui.element("table").style("table-layout:fixed;width:100%;border-collapse:separate;border-spacing:0"):
                    _build_colgroup_vinc()
                    with ui.element("tbody"):
                        for row in rows:
                            with ui.element("tr").classes("border-t border-gray-200 hover:bg-gray-50"):
                                for col in columns:
                                    align = "text-right" if col["align"] == "right" else "text-center" if col["align"] == "center" else "text-left"
                                    with ui.element("td").classes(f"px-2 py-1 border-b border-gray-100 {align} text-xs").style("white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:0"):
                                        if col["name"] == "acciones":
                                            if row.get("puede_crear_tn"):
                                                ui.button(
                                                    "Crear en Tienda Nube",
                                                    on_click=lambda sku=row["sku"]: _abrir_popup_crear(sku, ml_items_actuales),
                                                ).props("unelevated dense no-caps size=sm").classes("text-xs")
                                            else:
                                                ui.label("—")
                                        else:
                                            val = row.get(col["field"])
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
