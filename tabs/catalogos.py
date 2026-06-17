"""
tabs/catalogos.py
Pestaña Catálogos ML: tabla unificada por SKU, competidores ordenados por precio.
"""
from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional, Set

from nicegui import app, background_tasks, ui

from db import (
    get_connection,
    get_sku_catalogos,
    add_sku_catalogo,
    update_sku_catalogo_name,
    set_sku_catalogo_activo,
    delete_sku_catalogo,
    get_catalogo_competidores,
    upsert_catalogo_competidores,
)
from ml_api import (
    get_ml_access_token,
    ml_get_catalog_items,
    ml_get_users_multiget,
    ml_get_product_detail,
)


def _require_login() -> Optional[Dict[str, Any]]:
    user = app.storage.user.get("user")
    if not user:
        ui.notify("Debes iniciar sesión para continuar", color="negative")
    return user


def _fmt_precio(v: Any) -> str:
    if v is None:
        return "—"
    try:
        return "$" + f"{int(float(v)):,}".replace(",", ".")
    except Exception:
        return "—"


def _tipo_label(lt: str) -> str:
    return {"gold_special": "gold_sp", "gold_pro": "gold_pro", "gold_premium": "gold_prem"}.get(lt, lt or "—")


async def _sync_one_catalog(access_token: str, catalog_product_id: str) -> tuple:
    items = await asyncio.to_thread(ml_get_catalog_items, access_token, catalog_product_id)
    if not items:
        return [], ""
    seller_ids = list({str(it.get("seller_id", "")) for it in items if it.get("seller_id")})
    nicknames: Dict[str, str] = {}
    for i in range(0, len(seller_ids), 20):
        batch = seller_ids[i : i + 20]
        batch_nicks = await asyncio.to_thread(ml_get_users_multiget, access_token, batch)
        nicknames.update(batch_nicks)
    for it in items:
        sid = str(it.get("seller_id", ""))
        it["seller_nickname"] = nicknames.get(sid, f"ID {sid}")
    return items, ""


def _get_our_item_ids(user_id: int) -> Set[str]:
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT ml_id FROM ml_publicaciones WHERE user_id=?", (user_id,)
        ).fetchall()
        return {r["ml_id"] for r in rows}
    finally:
        conn.close()


_TH = "padding:5px 8px;border:1px solid #e5e7eb;white-space:nowrap;font-weight:600;background:#f3f4f6;font-size:12px"
_TD = "padding:3px 8px;border:1px solid #e5e7eb;font-size:13px;vertical-align:middle"


def build_tab_catalogos(container) -> None:
    container.clear()
    user = _require_login()
    if not user:
        return
    uid = user["id"]

    with container:
        access_token = get_ml_access_token(uid)
        if not access_token:
            ui.label("⚠️ No tienes MercadoLibre vinculado. Ve a Configuración.").classes("text-warning")
            return

        state: Dict[str, Any] = {"syncing": False, "our_ids": set()}
        sync_btn_ref: List[Any] = [None]
        sync_spinner_ref: List[Any] = [None]
        counter_ref: List[Any] = [None]

        # ── Header ────────────────────────────────────────────────────────────
        with ui.row().classes("w-full items-center gap-3 mb-2"):
            ui.label("CATÁLOGOS ML").classes("text-xl font-bold")
            counter_lbl = ui.label("").classes("text-sm text-gray-400")
            counter_ref[0] = counter_lbl
            ui.space()
            sp = ui.spinner(size="sm").classes("hidden")
            sync_spinner_ref[0] = sp

            async def _do_sync_all() -> None:
                if state["syncing"]:
                    return
                state["syncing"] = True
                sync_spinner_ref[0].classes(remove="hidden")
                btn = sync_btn_ref[0]
                if btn:
                    btn.props("disable")
                try:
                    cats = get_sku_catalogos()
                    active = [c for c in cats if c.get("activo")]
                    for cat in active:
                        cpid = cat["catalog_product_id"]
                        items, cat_name = await _sync_one_catalog(access_token, cpid)
                        await asyncio.to_thread(upsert_catalogo_competidores, cpid, items)
                        if cat_name and not cat.get("catalog_name"):
                            await asyncio.to_thread(update_sku_catalogo_name, cat["id"], cat_name)
                    _rebuild_table()
                    ui.notify(f"Sincronizados {len(active)} catálogo(s)", color="positive")
                except Exception as ex:
                    ui.notify(f"Error en sync: {ex}", color="negative")
                finally:
                    state["syncing"] = False
                    sync_spinner_ref[0].classes(add="hidden")
                    btn = sync_btn_ref[0]
                    if btn:
                        btn.props(remove="disable")

            btn = ui.button(
                "Sincronizar Todo",
                on_click=lambda: background_tasks.create(_do_sync_all(), name="sync_catalogos"),
                color="primary",
            ).props("no-caps")
            sync_btn_ref[0] = btn

        # ── Tabla principal ───────────────────────────────────────────────────
        table_area = ui.column().classes("w-full overflow-x-auto")

        async def _show_catalog_detail(catalog_product_id: str) -> None:
            detail = await asyncio.to_thread(ml_get_product_detail, access_token, catalog_product_id)
            if not detail:
                ui.notify("No se pudo obtener detalle del catálogo", color="warning")
                return
            with ui.dialog() as dlg:
                with ui.card().classes("w-96 p-4"):
                    ui.label(detail.get("name", catalog_product_id)).classes("text-lg font-bold")
                    ui.label(f"ID: {catalog_product_id}").classes("font-mono text-sm text-gray-500")
                    ui.separator().classes("my-2")
                    with ui.grid(columns=2).classes("gap-x-4 gap-y-1 text-sm w-full"):
                        ui.label("Status:").classes("text-gray-500")
                        ui.label(str(detail.get("status", "—")))
                        ui.label("Dominio:").classes("text-gray-500")
                        ui.label(str(detail.get("domain_id", "—")))
                        if detail.get("parent_id"):
                            ui.label("Padre:").classes("text-gray-500")
                            ui.label(str(detail.get("parent_id")))
                    pickers = detail.get("pickers", [])
                    if pickers:
                        ui.label("Variantes:").classes("font-semibold mt-3 text-sm")
                        for p in pickers:
                            ui.label(
                                f"• {p.get('label', '')}: {len(p.get('products', []))} opciones"
                            ).classes("text-sm text-gray-600 ml-2")
                    winner = detail.get("buy_box_winner")
                    if winner:
                        ui.label("Buy Box Winner:").classes("font-semibold mt-3 text-sm")
                        try:
                            price_fmt = "$" + f"{int(float(winner.get('price', 0))):,}".replace(",", ".")
                        except Exception:
                            price_fmt = str(winner.get("price", "—"))
                        ui.label(f"{winner.get('item_id', '—')} — {price_fmt}").classes("text-sm font-mono ml-2")
                    ui.button("Cerrar", on_click=dlg.close).props("flat no-caps").classes("mt-4")
            dlg.open()

        # Factory functions — captura por valor, evita closure bugs en loops
        def _make_detail_handler(c: str):
            def handler():
                background_tasks.create(_show_catalog_detail(c), name=f"detail_{c}")
            return handler

        def _make_del_handler(c: int):
            def handler():
                delete_sku_catalogo(c)
                _rebuild_table()
            return handler

        def _rebuild_table() -> None:
            table_area.clear()
            all_cats = get_sku_catalogos()
            active_cats = [c for c in all_cats if c.get("activo")]
            total_comps = sum(
                len(get_catalogo_competidores(c["catalog_product_id"])) for c in active_cats
            )
            if counter_ref[0]:
                counter_ref[0].set_text(f"{len(active_cats)} catálogos · {total_comps} competidores")

            with table_area:
                if not all_cats:
                    ui.label("No hay catálogos configurados. Agregá uno abajo.").classes("text-gray-500 py-4")
                    return

                # Agrupar por SKU (orden de inserción)
                sku_to_cats: Dict[str, List[Dict]] = {}
                for cat in all_cats:
                    s = cat["sku"]
                    if s not in sku_to_cats:
                        sku_to_cats[s] = []
                    sku_to_cats[s].append(cat)

                # Construir lista de filas: competidores por precio, luego inactivos/sin datos
                display_rows: List[Dict] = []
                for sku, cats in sku_to_cats.items():
                    comp_rows: List[Dict] = []
                    meta_rows: List[Dict] = []
                    for cat in cats:
                        is_active = bool(cat.get("activo"))
                        if not is_active:
                            meta_rows.append({"sku": sku, "cat": cat, "comp": None, "type": "inactive"})
                        else:
                            comps = get_catalogo_competidores(cat["catalog_product_id"])
                            if not comps:
                                meta_rows.append({"sku": sku, "cat": cat, "comp": None, "type": "nodata"})
                            else:
                                for comp in comps:
                                    comp_rows.append({
                                        "sku": sku, "cat": cat, "comp": comp, "type": "comp",
                                        "_p": float(comp.get("price") or 0),
                                    })
                    comp_rows.sort(key=lambda r: r["_p"])
                    display_rows.extend(comp_rows + meta_rows)

                with ui.element("table").style("width:100%;border-collapse:collapse"):
                    with ui.element("thead"):
                        with ui.element("tr"):
                            for col in ["#", "SKU", "Catálogo ID", "Vendedor", "Precio", "Tipo", "Logística", "Envío", "Acciones"]:
                                with ui.element("th").style(_TH):
                                    ui.label(col)

                    with ui.element("tbody"):
                        row_num = 0
                        prev_sku: Optional[str] = None
                        seen_cpids: Set[str] = set()

                        for row in display_rows:
                            sku = row["sku"]
                            cat = row["cat"]
                            comp = row["comp"]
                            row_type = row["type"]
                            cat_id = cat["id"]
                            cpid = cat["catalog_product_id"]
                            is_active = bool(cat.get("activo"))

                            is_first_sku = sku != prev_sku
                            is_first_cat = cpid not in seen_cpids
                            row_num += 1
                            prev_sku = sku
                            if is_first_cat:
                                seen_cpids.add(cpid)

                            # Borde grueso entre grupos de SKU
                            sep = ";border-top:2px solid #d1d5db" if is_first_sku and row_num > 1 else ""

                            if row_type == "comp":
                                is_ours = comp["item_id"] in state["our_ids"]
                                bg = "background:#f0fdf4" if is_ours else (
                                    "background:#ffffff" if row_num % 2 == 0 else "background:#f9fafb"
                                )
                            elif row_type == "inactive":
                                is_ours = False
                                bg = "background:#f9fafb"
                            else:
                                is_ours = False
                                bg = "background:#fffbeb"

                            with ui.element("tr").style(bg):
                                # #
                                with ui.element("td").style(_TD + sep + ";text-align:center;color:#9ca3af;font-size:12px"):
                                    ui.label(str(row_num))

                                # SKU — solo en primera fila del grupo
                                if is_first_sku:
                                    with ui.element("td").style(_TD + sep + ";font-family:monospace;font-size:12px;font-weight:600"):
                                        ui.label(sku)
                                else:
                                    with ui.element("td").style(_TD):
                                        pass

                                # Catálogo ID — siempre, como botón (factory evita closure bug)
                                clr = "#6b7280" if not is_active else "#2563eb"
                                with ui.element("td").style(_TD + sep):
                                    ui.button(
                                        cpid,
                                        on_click=_make_detail_handler(cpid),
                                    ).props("flat dense no-caps").style(
                                        f"font-family:monospace;font-size:12px;color:{clr};min-height:0;padding:0 2px"
                                    )

                                # Columnas de datos
                                if row_type == "comp":
                                    nick = comp.get("seller_nickname") or f"ID {comp.get('seller_id', '')}"
                                    with ui.element("td").style(
                                        _TD + sep + (";color:#15803d;font-weight:600" if is_ours else "")
                                    ):
                                        ui.label(f"{nick} ✓" if is_ours else nick)
                                    with ui.element("td").style(_TD + sep + ";text-align:right;font-family:monospace;font-weight:600"):
                                        ui.label(_fmt_precio(comp.get("price")))
                                    with ui.element("td").style(_TD + sep + ";font-size:12px"):
                                        ui.label(_tipo_label(comp.get("listing_type") or ""))
                                    with ui.element("td").style(_TD + sep + ";font-size:12px"):
                                        ui.label(comp.get("logistica") or "—")
                                    with ui.element("td").style(_TD + sep + ";text-align:center"):
                                        if comp.get("free_shipping"):
                                            ui.label("Sí").style("color:#16a34a;font-weight:600")
                                        else:
                                            ui.label("No").style("color:#9ca3af")
                                elif row_type == "inactive":
                                    with ui.element("td").style(_TD + sep + ";color:#9ca3af;font-style:italic"):
                                        ui.label("(inactivo)")
                                    for _ in range(4):
                                        with ui.element("td").style(_TD + sep):
                                            pass
                                else:  # nodata
                                    with ui.element("td").style(_TD + sep + ";color:#b45309;font-style:italic"):
                                        ui.label("Sin datos — Sincronizar")
                                    for _ in range(4):
                                        with ui.element("td").style(_TD + sep):
                                            pass

                                # Acciones — toggle+delete solo en primera fila del catálogo
                                with ui.element("td").style(_TD + sep + ";text-align:center"):
                                    if is_first_cat:
                                        with ui.row().classes("gap-0 items-center justify-center flex-nowrap"):
                                            ui.checkbox(
                                                value=is_active,
                                                on_change=lambda e, c=cat_id: (
                                                    set_sku_catalogo_activo(c, 1 if e.value else 0),
                                                    _rebuild_table(),
                                                ),
                                            )
                                            ui.button(
                                                icon="delete",
                                                on_click=_make_del_handler(cat_id),
                                            ).props("flat dense color=negative size=sm")

        # ── Agregar catálogo ──────────────────────────────────────────────────
        ui.separator().classes("my-3")
        with ui.expansion("Agregar catálogo", icon="add_circle").classes("w-full"):
            with ui.row().classes("items-end gap-2 mt-2"):
                sku_input = ui.input("SKU").props("dense outlined").classes("w-28")
                cpid_input = ui.input("Catálogo ID (ej: MLA52897968)").props("dense outlined").classes("w-52")
                name_input = ui.input("Nombre (opcional)").props("dense outlined").classes("w-64")

                async def _do_add() -> None:
                    sku = (sku_input.value or "").strip()
                    cpid = (cpid_input.value or "").strip().upper()
                    if not sku or not cpid:
                        ui.notify("SKU y Catálogo ID son requeridos", color="warning")
                        return
                    name = (name_input.value or "").strip()
                    if not name:
                        detail = await asyncio.to_thread(ml_get_product_detail, access_token, cpid)
                        if detail:
                            name = detail.get("name", "")
                    add_sku_catalogo(sku, cpid, name)
                    sku_input.value = ""
                    cpid_input.value = ""
                    name_input.value = ""
                    _rebuild_table()
                    ui.notify(f"Catálogo {cpid} agregado para SKU {sku}", color="positive")

                ui.button(
                    "Agregar",
                    on_click=lambda: background_tasks.create(_do_add(), name="add_catalogo"),
                    color="secondary",
                ).props("no-caps dense")

        # ── Carga inicial ─────────────────────────────────────────────────────
        async def _init_load() -> None:
            state["our_ids"] = await asyncio.to_thread(_get_our_item_ids, uid)
            cats = get_sku_catalogos()
            active = [c for c in cats if c.get("activo")]
            needs_sync = [
                c for c in active
                if not get_catalogo_competidores(c["catalog_product_id"])
            ]
            for cat in needs_sync:
                items, cat_name = await _sync_one_catalog(access_token, cat["catalog_product_id"])
                await asyncio.to_thread(upsert_catalogo_competidores, cat["catalog_product_id"], items)
                if cat_name and not cat.get("catalog_name"):
                    await asyncio.to_thread(update_sku_catalogo_name, cat["id"], cat_name)
            _rebuild_table()

        background_tasks.create(_init_load(), name="init_catalogos")
