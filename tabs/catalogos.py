"""
tabs/catalogos.py
Pestaña Catálogos ML: tabla por SKU con estado de competencia en catálogos.
"""
from __future__ import annotations

import asyncio
import json
import re
from typing import Any, Dict, List, Optional, Set

import requests
from nicegui import app, background_tasks, run, ui

from db import (
    get_app_config,
    get_catalogo_competidores,
    get_sku_catalogos,
    add_sku_catalogo,
    delete_sku_catalogo,
    upsert_catalogo_competidores,
)
from ml_api import (
    get_ml_access_token,
    ml_get_catalog_items,
    ml_get_user_id,
    ml_get_users_multiget,
    ml_get_product_detail,
    ml_get_item_price_to_win,
)


def _require_login() -> Optional[Dict[str, Any]]:
    user = app.storage.user.get("user")
    if not user:
        ui.notify("Debes iniciar sesión", color="negative")
    return user


def _fmt_precio(v: Any) -> str:
    if v is None:
        return "—"
    try:
        return "$" + f"{int(float(v)):,}".replace(",", ".")
    except Exception:
        return "—"


def _tipo_label(lt: str) -> str:
    return {
        "gold_special": "Sin cuotas",
        "gold_pro": "Con cuotas",
        "gold_premium": "Premium",
    }.get(lt, lt or "—")


_LOGISTICA_MAP: Dict[str, str] = {
    "cross_docking": "Correo",
    "xd_drop_off": "Correo",
    "drop_off": "Correo",
    "me1": "Correo",
    "me2": "Correo",
    "fulfillment": "Full",
    "self_service": "Flex",
    "flex": "Flex",
    "correo": "Correo",
}


def _logistica_label(lt: str) -> str:
    return _LOGISTICA_MAP.get(lt, lt or "—")


def _item_url(item_id: str) -> str:
    match = re.match(r'^([A-Z]+)(\d+)$', item_id or '')
    if match:
        return f"https://articulo.mercadolibre.com.ar/{match.group(1)}-{match.group(2)}"
    return "https://www.mercadolibre.com.ar"


_ORIGEN_BADGE: Dict[str, str] = {
    "internacional": (
        '<span style="background:#fff7ed;color:#c2410c;border:1px solid #fed7aa;'
        'border-radius:12px;padding:2px 8px;font-size:11px">🌎 Internacional</span>'
    ),
    "local": (
        '<span style="background:#f9fafb;color:#6b7280;border:1px solid #e5e7eb;'
        'border-radius:12px;padding:2px 8px;font-size:11px">📦 Local</span>'
    ),
}


_TIPO_BADGE: Dict[str, str] = {
    "gold_special": (
        '<span style="background:#f3f4f6;color:#374151;border:1px solid #d1d5db;'
        'border-radius:12px;padding:2px 8px;font-size:11px;font-weight:500">1x</span>'
    ),
    "gold_pro": (
        '<span style="background:#dbeafe;color:#1d4ed8;border:1px solid #93c5fd;'
        'border-radius:12px;padding:2px 8px;font-size:11px;font-weight:500">cuotas</span>'
    ),
    "gold_premium": (
        '<span style="background:#ede9fe;color:#6d28d9;border:1px solid #c4b5fd;'
        'border-radius:12px;padding:2px 8px;font-size:11px;font-weight:500">premium</span>'
    ),
}


def _get_cache_items(seller_id: str) -> List[Dict]:
    if not seller_id:
        return []
    raw = get_app_config(f"cache_my_items_{seller_id}_active")
    if not raw:
        raw = get_app_config(f"cache_my_items_{seller_id}_all")
    if not raw:
        return []
    try:
        data = json.loads(raw)
        return data.get("results", []) if isinstance(data, dict) else []
    except Exception:
        return []


def _better_item(a: Dict, b: Dict) -> Dict:
    """Prefiere gold_special sobre gold_pro; desempate por precio menor."""
    _RANK = {"gold_special": 0, "gold_pro": 1}
    rank_a = _RANK.get(str(a.get("listing_type_id") or ""), 2)
    rank_b = _RANK.get(str(b.get("listing_type_id") or ""), 2)
    if rank_a != rank_b:
        return a if rank_a < rank_b else b
    return a if float(a.get("price") or 0) <= float(b.get("price") or 0) else b


def _group_by_sku(items: List[Dict]) -> Dict[str, Dict]:
    """Retorna {sku: {"item": mejor_item, "item_ids": [str]}}"""
    groups: Dict[str, Dict] = {}
    for item in items:
        sku = (item.get("seller_sku") or "").strip()
        if not sku:
            continue
        item_id = str(item.get("id") or "")
        if sku not in groups:
            groups[sku] = {"item": item, "item_ids": [item_id] if item_id else []}
        else:
            groups[sku]["item"] = _better_item(groups[sku]["item"], item)
            if item_id and item_id not in groups[sku]["item_ids"]:
                groups[sku]["item_ids"].append(item_id)
    return groups


def _calc_ganando_v2(
    sku: str,
    item_ids: Set[str],
    all_cats: List[Dict],
    ptw_cache: Dict[str, str],
) -> str:
    cats = [c for c in all_cats if c.get("sku") == sku and c.get("activo")]
    if not cats:
        return "—"
    for item_id in item_ids:
        if ptw_cache.get(item_id) == "winning":
            return "ml"
    for cat in cats:
        comps = get_catalogo_competidores(cat["catalog_product_id"])
        if not comps:
            continue
        prices = [float(c.get("price") or 0) for c in comps if c.get("price") is not None]
        if not prices:
            continue
        min_price = min(prices)
        for comp in comps:
            if str(comp.get("item_id", "")) in item_ids:
                try:
                    if float(comp.get("price") or 0) <= min_price + 0.01:
                        return "catalogo"
                except Exception:
                    pass
    return "perdiendo"


async def _sync_one_catalog(access_token: str, catalog_product_id: str) -> List[Dict]:
    items = await asyncio.to_thread(ml_get_catalog_items, access_token, catalog_product_id)
    if not items:
        return []
    seller_ids = list({str(it.get("seller_id", "")) for it in items if it.get("seller_id")})
    nicknames: Dict[str, str] = {}
    for i in range(0, len(seller_ids), 20):
        batch = seller_ids[i : i + 20]
        batch_nicks = await asyncio.to_thread(ml_get_users_multiget, access_token, batch)
        nicknames.update(batch_nicks)
    for it in items:
        sid = str(it.get("seller_id", ""))
        it["seller_nickname"] = nicknames.get(sid, f"ID {sid}")
    return items


def _search_catalogs_sync(access_token: str, query: str) -> List[Dict]:
    q = query.strip()
    if not q:
        return []
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    try:
        if q.upper().startswith("MLA"):
            resp = requests.get(
                f"https://api.mercadolibre.com/products/{q.upper()}",
                headers=headers, timeout=10,
            )
            if resp.status_code == 200:
                d = resp.json()
                if isinstance(d, dict) and d.get("id"):
                    return [{"id": d["id"], "name": d.get("name", d["id"])}]
            return []
        elif re.match(r"^\d{8,14}$", q):
            resp = requests.get(
                "https://api.mercadolibre.com/products/search",
                params={"product_identifier": q, "site_id": "MLA"},
                headers=headers, timeout=10,
            )
            if resp.status_code == 200:
                results = resp.json().get("results", [])
                return [{"id": r.get("id", ""), "name": r.get("name", r.get("id", ""))} for r in results]
            return []
        else:
            resp = requests.get(
                "https://api.mercadolibre.com/products/search",
                params={"status": "active", "site_id": "MLA", "q": q, "limit": 10},
                headers=headers, timeout=10,
            )
            if resp.status_code == 200:
                results = resp.json().get("results", [])
                return [{"id": r.get("id", ""), "name": r.get("name", r.get("id", ""))} for r in results]
            return []
    except Exception:
        return []


_TH = "padding:5px 8px;border:1px solid #e5e7eb;white-space:nowrap;font-weight:600;background:#f3f4f6;font-size:12px"
_TH_BLUE = (
    "padding:5px 8px;border:1px solid #1565c0;white-space:nowrap;font-weight:500;"
    "background:#1976d2;color:white;font-size:12px;"
    "position:sticky;top:0;z-index:1;cursor:pointer;user-select:none"
)
_TH_BLUE_FIXED = (
    "padding:5px 8px;border:1px solid #1565c0;white-space:nowrap;font-weight:500;"
    "background:#1976d2;color:white;font-size:12px;"
    "position:sticky;top:0;z-index:1"
)
_TD = "padding:3px 8px;border:1px solid #e5e7eb;font-size:13px;vertical-align:middle"

_SORTABLE_COLS = {"SKU", "Marca", "Producto", "Color", "Stock", "Precio", "Catálogos", "Ganando"}


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

        seller_id_ref: List[str] = [""]
        state: Dict[str, Any] = {"syncing": False, "ptw_cache": {}}
        sync_btn_ref: List[Any] = [None]
        spinner_ref: List[Any] = [None]
        counter_ref: List[Any] = [None]
        expanded_skus: Set[str] = set()
        sort_col_ref: Dict[str, Any] = {"col": None, "asc": True}
        filter_state: Dict[str, Any] = {"ganando": "todos", "cats": "todos", "text": ""}

        # ── Header ─────────────────────────────────────────────────────────
        with ui.row().classes("w-full items-center gap-3 mb-2"):
            ui.label("CATÁLOGOS ML").classes("text-xl font-bold")
            counter_lbl = ui.label("").classes("text-sm text-gray-400")
            counter_ref[0] = counter_lbl
            ui.space()
            sp = ui.spinner(size="sm").classes("hidden")
            spinner_ref[0] = sp

            async def _do_sync_all() -> None:
                if state["syncing"]:
                    return
                state["syncing"] = True
                spinner_ref[0].classes(remove="hidden")
                if sync_btn_ref[0]:
                    sync_btn_ref[0].props("disable")
                try:
                    # Auto-agregar catálogos desde cache_my_items_active
                    if not seller_id_ref[0]:
                        seller_id_ref[0] = await run.io_bound(ml_get_user_id, access_token) or ""
                    cache_items = _get_cache_items(seller_id_ref[0])
                    existing_cats = get_sku_catalogos(uid)
                    existing_pairs: Set[tuple] = {
                        (c["sku"], c["catalog_product_id"]) for c in existing_cats
                    }
                    for item in cache_items:
                        item_sku = (item.get("seller_sku") or "").strip()
                        cpid = str(item.get("catalog_product_id") or "").strip()
                        if item_sku and cpid and (item_sku, cpid) not in existing_pairs:
                            add_sku_catalogo(uid, item_sku, cpid, "")
                            existing_pairs.add((item_sku, cpid))

                    # price_to_win por item (solo SKUs con catálogos)
                    new_ptw: Dict[str, str] = {}
                    sku_groups = _group_by_sku(cache_items)
                    all_cats_ptw = get_sku_catalogos(uid)
                    for ptw_sku, grp in sku_groups.items():
                        sku_cats = [
                            c for c in all_cats_ptw
                            if c.get("sku") == ptw_sku and c.get("activo")
                        ]
                        if not sku_cats:
                            continue
                        for item_id in grp["item_ids"]:
                            result = await asyncio.to_thread(
                                ml_get_item_price_to_win, access_token, item_id
                            )
                            if result:
                                new_ptw[item_id] = result.get("status", "")
                    state["ptw_cache"] = new_ptw

                    # Sincronizar competidores
                    all_cats_fresh = get_sku_catalogos(uid)
                    active = [c for c in all_cats_fresh if c.get("activo")]
                    for cat in active:
                        items = await _sync_one_catalog(access_token, cat["catalog_product_id"])
                        await asyncio.to_thread(
                            upsert_catalogo_competidores, cat["catalog_product_id"], items
                        )
                    _rebuild_table()
                    ui.notify(f"Sincronizados {len(active)} catálogo(s)", color="positive")
                except Exception as ex:
                    ui.notify(f"Error en sync: {ex}", color="negative")
                finally:
                    state["syncing"] = False
                    spinner_ref[0].classes(add="hidden")
                    if sync_btn_ref[0]:
                        sync_btn_ref[0].props(remove="disable")

            btn = ui.button(
                "Sincronizar Todo",
                on_click=lambda: background_tasks.create(_do_sync_all(), name="sync_catalogos"),
                color="primary",
            ).props("no-caps")
            sync_btn_ref[0] = btn

        # ── Filtros ──────────────────────────────────────────────────────────
        def _on_filter_ganando(e):
            filter_state["ganando"] = e.value or "todos"
            _rebuild_table()

        def _on_filter_cats(e):
            filter_state["cats"] = e.value or "todos"
            _rebuild_table()

        def _on_filter_text(e):
            filter_state["text"] = e.value or ""
            _rebuild_table()

        with ui.row().classes("w-full gap-3 mb-2 flex-wrap items-end"):
            ui.select(
                options={
                    "todos": "Todos",
                    "ganando": "🏆/📋 Ganando",
                    "perdiendo": "✗ Perdiendo",
                    "sin_catalogo": "— Sin catálogo",
                },
                value="todos",
                label="Ganando",
                on_change=_on_filter_ganando,
            ).props("dense outlined").style("min-width:160px")

            ui.select(
                options={
                    "todos": "Todos",
                    "sin_catalogo": "Sin catálogo asociado",
                    "con_catalogo": "Con catálogos",
                },
                value="todos",
                label="Catálogos",
                on_change=_on_filter_cats,
            ).props("dense outlined").style("min-width:190px")

            ui.input(
                placeholder="Buscar por SKU o nombre...",
                on_change=_on_filter_text,
            ).props("dense outlined clearable").style("min-width:230px")

        # ── Área de tabla ───────────────────────────────────────────────────
        table_area = ui.element("div").style("width:100%;overflow-x:auto")

        # ── Factory: popup detalle catálogo ──────────────────────────────────
        def _make_detail_handler(cpid: str):
            async def handler():
                detail = await asyncio.to_thread(ml_get_product_detail, access_token, cpid)
                if not detail:
                    ui.notify("No se pudo obtener detalle del catálogo", color="warning")
                    return
                with ui.dialog() as dlg:
                    with ui.card().classes("w-96 p-4"):
                        ui.label(detail.get("name", cpid)).classes("text-lg font-bold")
                        ui.label(f"ID: {cpid}").classes("font-mono text-sm text-gray-500")
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
                            ui.label(f"{winner.get('item_id', '—')} — {price_fmt}").classes(
                                "text-sm font-mono ml-2"
                            )
                        ui.button("Cerrar", on_click=dlg.close).props("flat no-caps").classes("mt-4")
                dlg.open()
            return handler

        # ── Factory: dialog gestión de catálogos del SKU ─────────────────────
        def _make_manage_handler(sku: str):
            async def handler():
                with ui.dialog() as dlg:
                    dlg.props("persistent")
                    with ui.card().classes("w-[540px] p-5"):
                        ui.label(f"Catálogos de {sku}").classes("text-lg font-bold mb-3")

                        cats_list_area = ui.column().classes("w-full gap-1 mb-1")

                        def _refresh_cats():
                            cats_list_area.clear()
                            current_cats = [c for c in get_sku_catalogos(uid) if c.get("sku") == sku]
                            if not current_cats:
                                with cats_list_area:
                                    ui.label("Sin catálogos configurados").classes(
                                        "text-gray-400 text-sm italic py-2"
                                    )
                            else:
                                with cats_list_area:
                                    for cat in current_cats:
                                        cpid = cat["catalog_product_id"]
                                        cname = cat.get("catalog_name") or cpid

                                        def _make_del(cid=cat["id"]):
                                            def _do_del():
                                                delete_sku_catalogo(cid, uid)
                                                _refresh_cats()
                                                _rebuild_table()
                                            return _do_del

                                        with ui.row().classes(
                                            "items-center gap-2 w-full py-1 border-b border-gray-100"
                                        ):
                                            ui.label(cpid).classes(
                                                "font-mono text-sm text-blue-600 w-28 flex-none"
                                            )
                                            ui.label(cname[:50]).classes(
                                                "text-sm text-gray-700 flex-1 truncate"
                                            )
                                            ui.button(
                                                icon="delete", on_click=_make_del()
                                            ).props("flat dense color=negative size=xs")

                        _refresh_cats()

                        ui.separator().classes("my-4")
                        ui.label("Buscar catálogo").classes("font-semibold text-sm mb-2")

                        with ui.row().classes("items-center gap-2 w-full"):
                            search_input = ui.input(
                                placeholder="ID (MLA...), EAN (solo números) o texto libre"
                            ).props("dense outlined").classes("flex-1")
                            search_btn = ui.button("Buscar").props("no-caps dense color=primary")

                        results_area = ui.column().classes("w-full mt-2 gap-0")

                        async def _do_catalog_search():
                            q = (search_input.value or "").strip()
                            if not q:
                                return
                            search_btn.props("loading")
                            results_area.clear()
                            try:
                                found = await asyncio.to_thread(
                                    _search_catalogs_sync, access_token, q
                                )
                                with results_area:
                                    if not found:
                                        ui.label("Sin resultados").classes(
                                            "text-gray-400 text-sm italic py-2"
                                        )
                                    else:
                                        for r in found:
                                            rid = r.get("id", "")
                                            rname = r.get("name", rid)
                                            if not rid:
                                                continue

                                            def _make_add(add_id=rid, add_name=rname):
                                                async def _do_add():
                                                    name_to_use = add_name
                                                    if not name_to_use or name_to_use == add_id:
                                                        det = await asyncio.to_thread(
                                                            ml_get_product_detail, access_token, add_id
                                                        )
                                                        if det:
                                                            name_to_use = det.get("name", add_id)
                                                    add_sku_catalogo(uid, sku, add_id, name_to_use)
                                                    _refresh_cats()
                                                    ui.notify(
                                                        f"Catálogo {add_id} agregado, sincronizando...",
                                                        color="info",
                                                    )
                                                    try:
                                                        sync_items = await _sync_one_catalog(
                                                            access_token, add_id
                                                        )
                                                        await asyncio.to_thread(
                                                            upsert_catalogo_competidores, add_id, sync_items
                                                        )
                                                        ui.notify(
                                                            f"{add_id} sincronizado", color="positive"
                                                        )
                                                    except Exception as ex:
                                                        ui.notify(
                                                            f"Error al sincronizar: {ex}", color="warning"
                                                        )
                                                    _rebuild_table()
                                                return _do_add

                                            with ui.row().classes(
                                                "items-center gap-2 w-full py-1 border-b border-gray-100"
                                            ):
                                                ui.label(rid).classes(
                                                    "font-mono text-xs text-blue-600 w-28 flex-none"
                                                )
                                                ui.label(rname[:60]).classes(
                                                    "text-sm flex-1 truncate text-gray-800"
                                                )
                                                ui.button(
                                                    "Agregar", on_click=_make_add()
                                                ).props("no-caps dense size=sm color=secondary")
                            except Exception as ex:
                                with results_area:
                                    ui.label(f"Error: {ex}").classes("text-red-500 text-sm")
                            finally:
                                search_btn.props(remove="loading")

                        search_btn.on("click", _do_catalog_search)
                        search_input.on("keydown.enter", _do_catalog_search)

                        ui.separator().classes("my-4")
                        ui.button("Cerrar", on_click=dlg.close).props("flat no-caps")

                dlg.open()
            return handler

        # ── Factory: sync un SKU ─────────────────────────────────────────────
        def _make_sync_sku_handler(s_sku: str, s_cats: List[Dict]):
            async def _do():
                active = [c for c in s_cats if c.get("activo")]
                if not active:
                    ui.notify("Sin catálogos activos", color="warning")
                    return
                ui.notify(f"Sincronizando {s_sku}...", color="info")
                try:
                    for cat in active:
                        its = await _sync_one_catalog(access_token, cat["catalog_product_id"])
                        await asyncio.to_thread(
                            upsert_catalogo_competidores, cat["catalog_product_id"], its
                        )
                    _rebuild_table()
                    ui.notify(f"{s_sku}: sincronizado", color="positive")
                except Exception as ex:
                    ui.notify(f"Error: {ex}", color="negative")

            def _click():
                background_tasks.create(_do(), name=f"sync_sku_{s_sku}")

            return _click

        # ── Rebuild principal ────────────────────────────────────────────────
        def _rebuild_table() -> None:
            table_area.clear()

            cache_items = _get_cache_items(seller_id_ref[0])
            sku_groups = _group_by_sku(cache_items)
            all_cats = get_sku_catalogos(uid)

            total_skus = len(sku_groups)
            total_cats = len([c for c in all_cats if c.get("activo")])
            compitiendo = sum(
                1 for sku in sku_groups
                if any(
                    bool(get_catalogo_competidores(c["catalog_product_id"]))
                    for c in all_cats
                    if c.get("sku") == sku and c.get("activo")
                )
            )

            if counter_ref[0]:
                counter_ref[0].set_text(
                    f"{total_skus} SKUs · {total_cats} catálogos · {compitiendo} compitiendo"
                )

            with table_area:
                if not cache_items:
                    ui.label(
                        "Sin datos de publicaciones. Actualizá desde la pestaña Productos."
                    ).classes("text-gray-400 italic py-4")
                    return

                rows: List[Dict] = []
                for sku, grp in sku_groups.items():
                    item = grp["item"]
                    item_ids_set: Set[str] = set(grp["item_ids"])
                    sku_cats = [c for c in all_cats if c.get("sku") == sku]
                    n_cats = len([c for c in sku_cats if c.get("activo")])
                    ganando = _calc_ganando_v2(sku, item_ids_set, all_cats, state["ptw_cache"])
                    rows.append({
                        "sku": sku,
                        "grp": grp,
                        "item": item,
                        "item_ids_set": item_ids_set,
                        "sku_cats": sku_cats,
                        "n_cats": n_cats,
                        "ganando": ganando,
                    })

                f_ganando = filter_state["ganando"]
                f_cats = filter_state["cats"]
                f_text = (filter_state["text"] or "").strip().lower()

                def _passes(row: Dict) -> bool:
                    if f_ganando == "ganando" and row["ganando"] not in ("ml", "catalogo"):
                        return False
                    if f_ganando == "perdiendo" and row["ganando"] != "perdiendo":
                        return False
                    if f_ganando == "sin_catalogo" and row["ganando"] != "—":
                        return False
                    if f_cats == "sin_catalogo" and row["n_cats"] > 0:
                        return False
                    if f_cats == "con_catalogo" and row["n_cats"] == 0:
                        return False
                    if f_text:
                        if f_text not in row["sku"].lower() and f_text not in (
                            row["item"].get("title") or ""
                        ).lower():
                            return False
                    return True

                rows = [r for r in rows if _passes(r)]

                active_sort_col = sort_col_ref["col"]
                if active_sort_col:
                    reverse = not sort_col_ref["asc"]

                    def _sort_key(row: Dict):
                        if active_sort_col == "SKU":
                            return row["sku"].lower()
                        if active_sort_col == "Marca":
                            return (row["item"].get("marca") or "").lower()
                        if active_sort_col == "Producto":
                            return (row["item"].get("title") or "").lower()
                        if active_sort_col == "Color":
                            return (row["item"].get("color") or "").lower()
                        if active_sort_col == "Stock":
                            return row["item"].get("available_quantity") or 0
                        if active_sort_col == "Precio":
                            return float(row["item"].get("price") or 0)
                        if active_sort_col == "Catálogos":
                            return row["n_cats"]
                        if active_sort_col == "Ganando":
                            return {"ml": 0, "catalogo": 1, "perdiendo": 2, "—": 3}.get(
                                row["ganando"], 3
                            )
                        return ""

                    rows.sort(key=_sort_key, reverse=reverse)

                def _make_sort_handler(col_name: str):
                    def _do_sort():
                        if sort_col_ref["col"] == col_name:
                            sort_col_ref["asc"] = not sort_col_ref["asc"]
                        else:
                            sort_col_ref["col"] = col_name
                            sort_col_ref["asc"] = True
                        _rebuild_table()
                    return _do_sort

                with ui.element("div").style("width:100%;max-height:65vh;overflow-y:auto"):
                    with ui.element("table").style("width:100%;border-collapse:collapse"):
                        with ui.element("thead"):
                            with ui.element("tr"):
                                for col_name in [
                                    "#", "SKU", "Marca", "Producto", "Color",
                                    "Stock", "Precio", "Catálogos", "Ganando", "↺", "▶",
                                ]:
                                    if col_name in _SORTABLE_COLS:
                                        is_active = sort_col_ref["col"] == col_name
                                        arrow = (" ▲" if sort_col_ref["asc"] else " ▼") if is_active else ""
                                        with ui.element("th").style(_TH_BLUE).on(
                                            "click", _make_sort_handler(col_name)
                                        ):
                                            ui.label(col_name + arrow)
                                    else:
                                        with ui.element("th").style(_TH_BLUE_FIXED):
                                            ui.label(col_name)

                        with ui.element("tbody"):
                            for row_num, row in enumerate(rows, 1):
                                sku = row["sku"]
                                item = row["item"]
                                item_ids_set = row["item_ids_set"]
                                sku_cats = row["sku_cats"]
                                n_cats = row["n_cats"]
                                ganando = row["ganando"]
                                titulo = (item.get("title") or "")[:40]
                                bg = "background:#f9fafb" if row_num % 2 == 0 else "background:#ffffff"

                                with ui.element("tr").style(bg):
                                    with ui.element("td").style(
                                        _TD + ";text-align:center;color:#9ca3af;font-size:12px"
                                    ):
                                        ui.label(str(row_num))
                                    with ui.element("td").style(
                                        _TD + ";font-family:monospace;font-size:12px;font-weight:600"
                                    ):
                                        ui.label(sku)
                                    with ui.element("td").style(_TD + ";font-size:12px"):
                                        ui.label(item.get("marca") or "—")
                                    with ui.element("td").style(
                                        _TD + ";font-size:12px;max-width:200px"
                                    ):
                                        ui.label(titulo)
                                    with ui.element("td").style(_TD + ";font-size:12px"):
                                        ui.label(item.get("color") or "—")
                                    with ui.element("td").style(
                                        _TD + ";text-align:right;font-family:monospace"
                                    ):
                                        qty = item.get("available_quantity")
                                        ui.label(str(qty) if qty is not None else "—")
                                    with ui.element("td").style(
                                        _TD + ";text-align:right;font-family:monospace;font-weight:600"
                                    ):
                                        ui.label(_fmt_precio(item.get("price")))

                                    with ui.element("td").style(_TD + ";text-align:center"):
                                        if n_cats > 0:
                                            ui.label(str(n_cats)).classes(
                                                "font-bold text-blue-600 cursor-pointer hover:underline"
                                            ).on("click", _make_manage_handler(sku))
                                        else:
                                            ui.label("+").classes(
                                                "font-bold text-gray-400 cursor-pointer "
                                                "hover:text-blue-600 text-lg"
                                            ).on("click", _make_manage_handler(sku))

                                    with ui.element("td").style(
                                        _TD + ";text-align:center;font-size:16px"
                                    ):
                                        if ganando == "ml":
                                            ui.html('<span title="Ganando en ML">🏆</span>')
                                        elif ganando == "catalogo":
                                            ui.html('<span title="Ganando en mi catálogo">📋</span>')
                                        elif ganando == "perdiendo":
                                            ui.html('<span style="color:#dc2626">✗</span>')
                                        else:
                                            ui.label("—").classes("text-gray-400")

                                    with ui.element("td").style(_TD + ";text-align:center"):
                                        if n_cats > 0:
                                            ui.button(
                                                "↺",
                                                on_click=_make_sync_sku_handler(sku, sku_cats),
                                            ).props("flat dense size=sm no-caps").style(
                                                "color:#6b7280;font-size:13px"
                                            )

                                    with ui.element("td").style(_TD + ";text-align:center"):
                                        is_exp = sku in expanded_skus

                                        def _make_toggle(s=sku):
                                            def _toggle():
                                                if s in expanded_skus:
                                                    expanded_skus.discard(s)
                                                else:
                                                    expanded_skus.add(s)
                                                _rebuild_table()
                                            return _toggle

                                        ui.button(
                                            "▼" if is_exp else "▶",
                                            on_click=_make_toggle(),
                                        ).props("flat dense size=sm no-caps").style(
                                            "color:#6b7280;font-size:12px"
                                        )

                                if sku in expanded_skus:
                                    active_cats = [c for c in sku_cats if c.get("activo")]
                                    all_comps: List[Dict] = []
                                    for cat in active_cats:
                                        for comp in get_catalogo_competidores(
                                            cat["catalog_product_id"]
                                        ):
                                            entry = dict(comp)
                                            entry["_cpid"] = cat["catalog_product_id"]
                                            all_comps.append(entry)
                                    all_comps.sort(key=lambda c: float(c.get("price") or 0))

                                    with ui.element("tr"):
                                        with ui.element("td").props("colspan=11").style(
                                            "padding:0;border:none"
                                        ):
                                            with ui.element("div").style(
                                                "background:#f0f9ff;padding:8px 12px 12px 24px;"
                                                "border-bottom:1px solid #e5e7eb"
                                            ):
                                                if not all_comps:
                                                    ui.label(
                                                        "Sin datos — presioná Sincronizar Todo"
                                                    ).classes("text-amber-600 text-sm italic py-2")
                                                else:
                                                    with ui.element("table").style(
                                                        "width:100%;border-collapse:collapse"
                                                    ):
                                                        with ui.element("thead"):
                                                            with ui.element("tr"):
                                                                for col in [
                                                                    "#", "Catálogo ID", "Item", "Origen", "Vendedor",
                                                                    "Precio", "Tipo",
                                                                ]:
                                                                    with ui.element("th").style(
                                                                        _TH + ";background:#e0f2fe;font-size:11px"
                                                                    ):
                                                                        ui.label(col)
                                                        with ui.element("tbody"):
                                                            for ci, comp in enumerate(all_comps, 1):
                                                                is_ours = (
                                                                    str(comp.get("item_id", ""))
                                                                    in item_ids_set
                                                                )
                                                                cbg = (
                                                                    "background:#f0fdf4" if is_ours
                                                                    else (
                                                                        "background:#ffffff"
                                                                        if ci % 2 == 0
                                                                        else "background:#f0f9ff"
                                                                    )
                                                                )
                                                                cpid = comp.get("_cpid", "")
                                                                with ui.element("tr").style(cbg):
                                                                    with ui.element("td").style(
                                                                        _TD + ";text-align:center;"
                                                                        "font-size:11px;color:#9ca3af"
                                                                    ):
                                                                        ui.label(str(ci))
                                                                    with ui.element("td").style(
                                                                        _TD + ";text-align:center"
                                                                    ):
                                                                        ui.label(cpid).classes(
                                                                            "font-mono cursor-pointer "
                                                                            "underline text-sm text-blue-600"
                                                                        ).on(
                                                                            "click",
                                                                            _make_detail_handler(cpid),
                                                                        )
                                                                    item_id_val = comp.get("item_id", "")
                                                                    item_url = _item_url(item_id_val)
                                                                    item_link_style = (
                                                                        "color:#15803d;font-weight:600"
                                                                        if is_ours else "color:#2563eb"
                                                                    )
                                                                    with ui.element("td").style(
                                                                        _TD + ";text-align:center"
                                                                    ):
                                                                        ui.html(
                                                                            f'<a href="{item_url}" target="_blank" '
                                                                            f'style="font-family:monospace;font-size:11px;'
                                                                            f'{item_link_style};text-decoration:underline">'
                                                                            f'{item_id_val}</a>'
                                                                        )
                                                                    origen_val = comp.get("origen") or "local"
                                                                    with ui.element("td").style(
                                                                        _TD + ";text-align:center"
                                                                    ):
                                                                        ui.html(_ORIGEN_BADGE.get(origen_val, _ORIGEN_BADGE["local"]))
                                                                    nick = (
                                                                        comp.get("seller_nickname")
                                                                        or f"ID {comp.get('seller_id', '')}"
                                                                    )
                                                                    with ui.element("td").style(
                                                                        _TD
                                                                        + (
                                                                            ";color:#15803d;font-weight:600"
                                                                            if is_ours
                                                                            else ""
                                                                        )
                                                                    ):
                                                                        ui.label(
                                                                            f"{nick} ✓" if is_ours else nick
                                                                        )
                                                                    with ui.element("td").style(
                                                                        _TD + ";text-align:right;"
                                                                        "font-family:monospace;font-weight:600"
                                                                    ):
                                                                        ui.label(
                                                                            _fmt_precio(comp.get("price"))
                                                                        )
                                                                    with ui.element("td").style(
                                                                        _TD + ";text-align:center"
                                                                    ):
                                                                        lt = comp.get("listing_type") or ""
                                                                        badge = _TIPO_BADGE.get(lt)
                                                                        if badge:
                                                                            ui.html(badge)
                                                                        else:
                                                                            ui.label(lt or "—").style(
                                                                                "font-size:11px"
                                                                            )

        # Carga inicial: obtener seller_id async antes de renderizar para no mostrar datos de otro usuario
        async def _init_and_rebuild():
            if not seller_id_ref[0]:
                seller_id_ref[0] = await run.io_bound(ml_get_user_id, access_token) or ""
            _rebuild_table()

        ui.timer(0.0, _init_and_rebuild, once=True)
