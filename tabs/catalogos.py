"""
tabs/catalogos.py
Pestaña Catálogos ML: gestión de catálogos por SKU y monitoreo de competidores.
"""
from __future__ import annotations

import asyncio
import html as _html
from datetime import datetime, timezone
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


def _ago(updated_at: str) -> str:
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


def _esc(s: Any) -> str:
    return _html.escape(str(s) if s is not None else "")


async def _sync_one_catalog(access_token: str, catalog_product_id: str) -> tuple:
    """Llama /products/{id}/items, resuelve nicknames. Retorna (items, catalog_name)."""
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


def _build_competitors_html(competitors: List[Dict], our_ids: Set[str]) -> str:
    """Construye el HTML de la tabla de competidores."""
    th_style = "padding:4px 8px;border:1px solid #e5e7eb;white-space:nowrap;font-weight:600;background:#f3f4f6"
    rows_html = ""
    for idx, comp in enumerate(competitors, 1):
        is_ours = comp["item_id"] in our_ids
        bg = "background:#f0fdf4" if is_ours else ("background:#ffffff" if idx % 2 == 0 else "background:#f9fafb")
        fw = "font-weight:600" if is_ours else ""
        item_id_txt = _esc(comp["item_id"]) + (" ✓" if is_ours else "")
        item_color = "color:#15803d" if is_ours else ""
        nick = _esc(comp.get("seller_nickname") or f"ID {comp.get('seller_id','')}")
        precio = _esc(_fmt_precio(comp.get("price")))
        tipo = _esc(_tipo_label(comp.get("listing_type") or ""))
        logistica = _esc(comp.get("logistica") or "—")
        envio = "Sí" if comp.get("free_shipping") else "No"
        envio_color = "color:#16a34a" if comp.get("free_shipping") else "color:#9ca3af"
        td = f"padding:3px 8px;border:1px solid #e5e7eb;{bg};{fw}"
        rows_html += (
            f"<tr>"
            f"<td style='{td};text-align:center'>{idx}</td>"
            f"<td style='{td};font-family:monospace;font-size:12px;{item_color}'>{item_id_txt}</td>"
            f"<td style='{td}'>{nick}</td>"
            f"<td style='{td};text-align:right;font-family:monospace'>{precio}</td>"
            f"<td style='{td};font-size:12px'>{tipo}</td>"
            f"<td style='{td};font-size:12px'>{logistica}</td>"
            f"<td style='{td};text-align:center;{envio_color}'>{envio}</td>"
            f"</tr>"
        )
    return (
        "<div style='overflow-x:auto;width:100%'>"
        "<table style='width:100%;border-collapse:collapse;font-size:13px'>"
        "<thead><tr>"
        f"<th style='{th_style}'>#</th>"
        f"<th style='{th_style}'>Item ID</th>"
        f"<th style='{th_style}'>Vendedor</th>"
        f"<th style='{th_style}'>Precio</th>"
        f"<th style='{th_style}'>Tipo</th>"
        f"<th style='{th_style}'>Logística</th>"
        f"<th style='{th_style}'>Envío</th>"
        "</tr></thead>"
        f"<tbody>{rows_html}</tbody>"
        "</table></div>"
    )


def _render_catalogo_card(parent, cat: Dict, competitors: List[Dict], our_ids: Set[str]) -> None:
    """Pinta la card de un catálogo con su tabla de competidores."""
    updated_at = competitors[0]["updated_at"] if competitors else None
    ts_text = _ago(updated_at) if updated_at else "sin datos"

    with parent:
        with ui.row().classes("items-center gap-2 w-full flex-wrap"):
            ui.label(f"SKU: {cat['sku']}").classes("font-bold")
            ui.label(f"› {cat['catalog_product_id']}").classes("text-gray-500 font-mono text-sm")
            if cat.get("catalog_name"):
                ui.label(cat["catalog_name"]).classes("text-gray-700 text-sm")
            ui.space()
            ui.label(f"Actualizado: {ts_text}").classes("text-xs text-gray-400")

        if not competitors:
            ui.label("Sin competidores (catálogo vacío o sin datos aún).").classes("text-gray-500 text-sm mt-1")
            return

        ui.html(_build_competitors_html(competitors, our_ids)).classes("w-full mt-1")


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

        # ── Header ───────────────────────────────────────────────────────────
        sync_spinner = ui.spinner(size="sm").classes("hidden")
        sync_btn_ref: List[Any] = [None]

        with ui.row().classes("w-full items-center gap-3 mb-2"):
            ui.label("CATÁLOGOS ML").classes("text-xl font-bold")
            ui.space()
            sync_spinner

            async def _do_sync_all() -> None:
                if state["syncing"]:
                    return
                state["syncing"] = True
                sync_spinner.classes(remove="hidden")
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
                    _rebuild_competitors()
                    ui.notify(f"Sincronizados {len(active)} catálogo(s)", color="positive")
                except Exception as ex:
                    ui.notify(f"Error en sync: {ex}", color="negative")
                finally:
                    state["syncing"] = False
                    sync_spinner.classes(add="hidden")
                    btn = sync_btn_ref[0]
                    if btn:
                        btn.props(remove="disable")

            btn = ui.button(
                "Sincronizar Todo",
                on_click=lambda: background_tasks.create(_do_sync_all(), name="sync_catalogos"),
                color="primary",
            ).props("no-caps")
            sync_btn_ref[0] = btn

        # ── Sección configurar catálogos ──────────────────────────────────────
        with ui.expansion("Catálogos configurados", icon="settings").classes("w-full"):
            config_area = ui.column().classes("w-full gap-1")

            def _rebuild_config_table() -> None:
                config_area.clear()
                cats = get_sku_catalogos()
                if not cats:
                    with config_area:
                        ui.label("No hay catálogos configurados aún.").classes("text-gray-500 text-sm")
                    return
                with config_area:
                    with ui.element("table").classes("text-sm border-collapse"):
                        with ui.element("thead"):
                            with ui.element("tr"):
                                for col_h in ["SKU", "Catálogo ID", "Nombre", "Activo", ""]:
                                    with ui.element("th").style("padding:4px 8px;border:1px solid #e5e7eb;background:#f3f4f6;font-weight:600;white-space:nowrap"):
                                        ui.label(col_h)
                        with ui.element("tbody"):
                            for cat in cats:
                                with ui.element("tr"):
                                    td_s = "padding:3px 8px;border:1px solid #e5e7eb"
                                    with ui.element("td").style(td_s + ";font-family:monospace;font-size:12px"):
                                        ui.label(cat["sku"])
                                    with ui.element("td").style(td_s + ";font-family:monospace;font-size:12px"):
                                        ui.label(cat["catalog_product_id"])
                                    with ui.element("td").style(td_s + ";font-size:12px"):
                                        ui.label(cat.get("catalog_name") or "—")
                                    with ui.element("td").style(td_s + ";text-align:center"):
                                        cid = cat["id"]
                                        ui.checkbox(
                                            value=bool(cat.get("activo")),
                                            on_change=lambda e, c=cid: (
                                                set_sku_catalogo_activo(c, 1 if e.value else 0),
                                                _rebuild_competitors(),
                                            ),
                                        )
                                    with ui.element("td").style(td_s + ";text-align:center"):
                                        def _del(c=cat["id"]):
                                            delete_sku_catalogo(c)
                                            _rebuild_config_table()
                                            _rebuild_competitors()
                                        ui.button(icon="delete", on_click=_del).props("flat dense color=negative size=sm")

            _rebuild_config_table()

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

        ui.separator().classes("my-3")

        # ── Área de competidores (se puebla en _rebuild_competitors) ──────────
        catalogos_area = ui.column().classes("w-full gap-4")

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
                        _render_catalogo_card(
                            ui.column().classes("w-full p-1"),
                            cat, comps, state["our_ids"],
                        )

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
            _rebuild_competitors()

        background_tasks.create(_init_load(), name="init_catalogos")
