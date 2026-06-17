"""
tabs/catalogos.py
Pestaña Catálogos ML: gestión de catálogos por SKU y monitoreo de competidores.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from nicegui import app, background_tasks, run, ui

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


def _ago(updated_at: str) -> str:
    """Convierte ISO timestamp a 'hace X min / horas / días'."""
    if not updated_at:
        return "—"
    try:
        dt = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        diff = int((datetime.now(timezone.utc) - dt).total_seconds())
        if diff < 60:
            return "ahora"
        if diff < 3600:
            return f"hace {diff // 60} min"
        if diff < 86400:
            return f"hace {diff // 3600} h"
        return f"hace {diff // 86400} días"
    except Exception:
        return updated_at[:16] if len(updated_at) >= 16 else updated_at


async def _sync_one_catalog(
    access_token: str, catalog_product_id: str
) -> tuple[List[Dict], str]:
    """
    Llama /products/{id}/items, resuelve nicknames y devuelve
    (lista_enriquecida, catalog_name_o_vacío).
    """
    items = await asyncio.to_thread(ml_get_catalog_items, access_token, catalog_product_id)
    catalog_name = ""
    if not items:
        return [], catalog_name

    seller_ids = list({str(it.get("seller_id", "")) for it in items if it.get("seller_id")})
    nicknames: Dict[str, str] = {}
    for i in range(0, len(seller_ids), 20):
        batch = seller_ids[i : i + 20]
        batch_nicks = await asyncio.to_thread(ml_get_users_multiget, access_token, batch)
        nicknames.update(batch_nicks)

    for it in items:
        sid = str(it.get("seller_id", ""))
        it["seller_nickname"] = nicknames.get(sid, f"ID {sid}")

    return items, catalog_name


def _get_our_item_ids(user_id: int) -> Set[str]:
    """Devuelve el set de ml_id del usuario para resaltar sus items en la tabla."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT ml_id FROM ml_publicaciones WHERE user_id=?", (user_id,)
        ).fetchall()
        return {r["ml_id"] for r in rows}
    finally:
        conn.close()


def _render_catalogo_section(
    parent,
    cat: Dict,
    competitors: List[Dict],
    our_ids: Set[str],
    spinner_ref: Optional[Any] = None,
) -> None:
    """Pinta la sección de competidores de un catálogo dentro de parent."""
    updated_at = competitors[0]["updated_at"] if competitors else None
    ts_text = _ago(updated_at) if updated_at else "sin datos"

    with parent:
        with ui.row().classes("items-center gap-2 mt-2"):
            ui.label(f"SKU: {cat['sku']}").classes("font-bold text-base")
            ui.label(f"› {cat['catalog_product_id']}").classes("text-gray-500")
            if cat.get("catalog_name"):
                ui.label(cat["catalog_name"]).classes("text-gray-700 text-sm")
            ui.space()
            ui.label(f"Actualizado: {ts_text}").classes("text-xs text-gray-400")

        if not competitors:
            ui.label("Sin competidores (catálogo vacío o sin datos aún).").classes("text-gray-500 text-sm mb-2")
            return

        with ui.element("div").classes("w-full overflow-x-auto"):
            with ui.element("table").classes("w-full text-sm border-collapse"):
                with ui.element("thead"):
                    with ui.element("tr").classes("bg-gray-100 text-left"):
                        for col in ["#", "Item ID", "Vendedor", "Precio", "Tipo", "Logística", "Envío gratis"]:
                            ui.element("th").classes("px-2 py-1 border border-gray-200 font-semibold whitespace-nowrap").set_content(col)
                with ui.element("tbody"):
                    for idx, comp in enumerate(competitors, 1):
                        is_ours = comp["item_id"] in our_ids
                        row_cls = "bg-green-50 font-semibold" if is_ours else ("bg-white" if idx % 2 == 0 else "bg-gray-50")
                        with ui.element("tr").classes(row_cls):
                            ui.element("td").classes("px-2 py-1 border border-gray-200 text-center").set_content(str(idx))
                            item_cell = ui.element("td").classes("px-2 py-1 border border-gray-200 font-mono text-xs")
                            with item_cell:
                                if is_ours:
                                    ui.label(f"{comp['item_id']} ✓").classes("text-green-700")
                                else:
                                    ui.label(comp["item_id"])
                            ui.element("td").classes("px-2 py-1 border border-gray-200").set_content(comp.get("seller_nickname") or f"ID {comp['seller_id']}")
                            ui.element("td").classes("px-2 py-1 border border-gray-200 text-right font-mono").set_content(_fmt_precio(comp["price"]))
                            ui.element("td").classes("px-2 py-1 border border-gray-200 text-xs").set_content(_tipo_label(comp.get("listing_type") or ""))
                            ui.element("td").classes("px-2 py-1 border border-gray-200 text-xs").set_content(comp.get("logistica") or "—")
                            envio_txt = "Sí" if comp.get("free_shipping") else "No"
                            envio_cls = "px-2 py-1 border border-gray-200 text-center " + ("text-green-600" if comp.get("free_shipping") else "text-gray-400")
                            ui.element("td").classes(envio_cls).set_content(envio_txt)


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

        # ── Estado mutable compartido entre closures ──────────────────────────
        state: Dict[str, Any] = {
            "syncing": False,
            "our_ids": set(),
        }

        # ── Sección competidores (reconstruible tras sync) ────────────────────
        catalogos_area = ui.column().classes("w-full gap-4 mt-2")

        def _rebuild_competitors() -> None:
            catalogos_area.clear()
            cats = get_sku_catalogos()
            active_cats = [c for c in cats if c.get("activo")]
            if not active_cats:
                with catalogos_area:
                    ui.label("No hay catálogos activos configurados.").classes("text-gray-500 mt-4")
                return
            for cat in active_cats:
                comps = get_catalogo_competidores(cat["catalog_product_id"])
                with catalogos_area:
                    with ui.card().classes("w-full"):
                        _render_catalogo_section(
                            ui.column().classes("w-full"),
                            cat, comps, state["our_ids"]
                        )

        # ── Header ───────────────────────────────────────────────────────────
        sync_spinner = ui.spinner(size="sm").classes("hidden")
        sync_btn: Any = None

        with ui.row().classes("w-full items-center gap-3 mb-2"):
            ui.label("CATÁLOGOS ML").classes("text-xl font-bold")
            ui.space()

            async def _do_sync_all() -> None:
                if state["syncing"]:
                    return
                state["syncing"] = True
                sync_spinner.classes(remove="hidden")
                if sync_btn:
                    sync_btn.props("disable")
                try:
                    cats = get_sku_catalogos()
                    active = [c for c in cats if c.get("activo")]
                    for cat in active:
                        cpid = cat["catalog_product_id"]
                        items, cat_name = await _sync_one_catalog(access_token, cpid)
                        await asyncio.to_thread(upsert_catalogo_competidores, cpid, items)
                        if cat_name and not cat.get("catalog_name"):
                            await asyncio.to_thread(update_sku_catalogo_name, cat["id"], cat_name)
                    _rebuild_competitors()
                    ui.notify(f"Sincronizados {len(active)} catálogo(s)", color="positive")
                except Exception as ex:
                    ui.notify(f"Error en sync: {ex}", color="negative")
                finally:
                    state["syncing"] = False
                    sync_spinner.classes(add="hidden")
                    if sync_btn:
                        sync_btn.props(remove="disable")

            sync_btn = ui.button(
                "Sincronizar Todo",
                on_click=lambda: background_tasks.create(_do_sync_all(), name="sync_catalogos"),
                color="primary",
            ).props("no-caps")

        sync_spinner  # referenciado arriba

        # ── Sección configurar catálogos ──────────────────────────────────────
        with ui.expansion("Catálogos configurados", icon="settings").classes("w-full"):
            config_area = ui.column().classes("w-full gap-2")

            def _rebuild_config_table() -> None:
                config_area.clear()
                cats = get_sku_catalogos()
                if not cats:
                    with config_area:
                        ui.label("No hay catálogos configurados aún.").classes("text-gray-500 text-sm")
                    return
                with config_area:
                    with ui.element("table").classes("w-full text-sm border-collapse"):
                        with ui.element("thead"):
                            with ui.element("tr").classes("bg-gray-100 text-left"):
                                for col in ["SKU", "Catálogo ID", "Nombre", "Activo", ""]:
                                    ui.element("th").classes("px-2 py-1 border border-gray-200 font-semibold").set_content(col)
                        with ui.element("tbody"):
                            for cat in cats:
                                with ui.element("tr").classes("bg-white"):
                                    ui.element("td").classes("px-2 py-1 border border-gray-200 font-mono text-xs").set_content(cat["sku"])
                                    ui.element("td").classes("px-2 py-1 border border-gray-200 font-mono text-xs").set_content(cat["catalog_product_id"])
                                    ui.element("td").classes("px-2 py-1 border border-gray-200 text-xs").set_content(cat.get("catalog_name") or "—")
                                    with ui.element("td").classes("px-2 py-1 border border-gray-200 text-center"):
                                        chk = ui.checkbox(value=bool(cat.get("activo")))
                                        cat_id = cat["id"]

                                        def _on_toggle(e, cid=cat_id):
                                            set_sku_catalogo_activo(cid, 1 if e.value else 0)
                                            _rebuild_competitors()

                                        chk.on("update:model-value", _on_toggle)
                                    with ui.element("td").classes("px-2 py-1 border border-gray-200 text-center"):
                                        def _del(cid=cat["id"]):
                                            delete_sku_catalogo(cid)
                                            _rebuild_config_table()
                                            _rebuild_competitors()
                                        ui.button(icon="delete", on_click=_del).props("flat dense color=negative")

            _rebuild_config_table()

            # ── Agregar nuevo catálogo ────────────────────────────────────────
            with ui.row().classes("items-end gap-2 mt-3"):
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
                    _rebuild_config_table()
                    ui.notify(f"Catálogo {cpid} agregado para SKU {sku}", color="positive")

                ui.button(
                    "Agregar",
                    on_click=lambda: background_tasks.create(_do_add(), name="add_catalogo"),
                    color="secondary",
                ).props("no-caps dense")

        ui.separator().classes("my-2")

        # ── Carga inicial: our_ids + render ───────────────────────────────────
        async def _init_load() -> None:
            state["our_ids"] = await asyncio.to_thread(_get_our_item_ids, uid)
            cats = get_sku_catalogos()
            active = [c for c in cats if c.get("activo")]
            # Auto-sync catálogos que no tienen datos en cache
            needs_sync = [
                c for c in active
                if not get_catalogo_competidores(c["catalog_product_id"])
            ]
            for cat in needs_sync:
                items, cat_name = await _sync_one_catalog(access_token, cat["catalog_product_id"])
                await asyncio.to_thread(upsert_catalogo_competidores, cat["catalog_product_id"], items)
                if cat_name and not cat.get("catalog_name"):
                    await asyncio.to_thread(update_sku_catalogo_name, cat["id"], cat_name)
            _rebuild_competitors()

        background_tasks.create(_init_load(), name="init_catalogos")
