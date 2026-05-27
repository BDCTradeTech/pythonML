"""
Fase 3 — tabs/stock.py
Pestaña Stock: inventario de QuickBooks (Items con QtyOnHand > 0).
Funciones exportadas: build_tab_stock
"""
from __future__ import annotations

import tempfile
from typing import Any, Dict, List, Optional

from nicegui import app, background_tasks, run, ui

from db import get_qb_app_credentials, get_qb_tokens
from qb_api import fetch_qb_items, fetch_qb_item_history, fetch_qb_invoice_pdf


# ---------------------------------------------------------------------------
# Helper de sesión (mismo patrón que otros tabs; se unificará en auth.py Fase 4)
# ---------------------------------------------------------------------------

def _require_login() -> Optional[Dict[str, Any]]:
    user = app.storage.user.get("user")
    if not user:
        ui.notify("Debes iniciar sesión para continuar", color="negative")
    return user


# ---------------------------------------------------------------------------
# Helper QB (copiado de main.py — se unificará en utils.py Fase 4)
# ---------------------------------------------------------------------------

def _qb_invoice_pdf_download_basename(doc: Any) -> str:
    """Nombre de archivo sugerido para PDF de invoice."""
    base = str(doc or "invoice").strip().replace(" ", "_")
    for c in '<>:"/\\|?*':
        base = base.replace(c, "_")
    return f"{base[:80]}.pdf"


# ---------------------------------------------------------------------------
# Función exportada
# ---------------------------------------------------------------------------

def build_tab_stock(container) -> None:
    """Pestaña Stock: inventario de QuickBooks (Items con QtyOnHand > 0)."""
    user = _require_login()
    if not user:
        return

    qb_creds = get_qb_app_credentials(user["id"])
    qb_tokens = get_qb_tokens(user["id"])

    with container:
        if not qb_creds:
            ui.label(
                "Configurá QuickBooks en Configuración (Client ID, Client Secret, Redirect URI) y conectá tu cuenta."
            ).classes("text-gray-600")
            return

        if not qb_tokens:
            ui.label(
                "Credenciales configuradas. Andá a Configuración → QuickBooks y hacé clic en 'Conectar cuenta' para autorizar."
            ).classes("text-warning")
            return

        header_card = ui.column().classes("w-full mb-2")
        result_area = ui.column().classes("w-full gap-2")
        items_ref: List[Dict[str, Any]] = []
        sort_col_ref: Dict[str, str] = {"val": "producto"}
        sort_asc_ref: Dict[str, bool] = {"val": True}

        with result_area:
            with ui.card().classes("w-full p-8 items-center gap-4"):
                ui.spinner(size="xl")
                ui.label("Cargando stock de QuickBooks...").classes("text-xl text-gray-700")

        def _sort_key_stock(row: Dict[str, Any], col: str) -> Any:
            if col == "id":
                return str(row.get("id", "")).lower()
            if col == "producto":
                return str(row.get("producto", "")).lower()
            if col == "sku":
                return str(row.get("sku", "")).lower()
            if col == "sales_price":
                return row.get("sales_price", 0)
            if col == "qty":
                return row.get("qty", 0)
            return ""

        def _on_sort_stock(col: str) -> None:
            if sort_col_ref.get("val") == col:
                sort_asc_ref["val"] = not sort_asc_ref.get("val", True)
            else:
                sort_col_ref["val"] = col
                sort_asc_ref["val"] = True
            _pintar_tabla()

        def _pintar_tabla() -> None:
            items = items_ref
            sort_col = sort_col_ref.get("val", "producto")
            asc = sort_asc_ref.get("val", True)
            items_sorted = sorted(items, key=lambda x: _sort_key_stock(x, sort_col), reverse=not asc)
            n_skus = len(items)
            total_qty = sum(i.get("qty", 0) for i in items)
            stock_valorizado = sum((i.get("qty", 0) or 0) * (i.get("sales_price", 0) or 0) for i in items)
            header_card.clear()
            with header_card:
                ui.label("Stock").classes("text-xl font-semibold mb-2")
                with ui.card().classes("w-full p-4 bg-grey-2"):
                    with ui.row().classes("w-full gap-6 flex-wrap items-center"):
                        with ui.column().classes("gap-0"):
                            ui.label("Diferentes SKUs").classes("text-xs text-gray-600")
                            ui.label(str(n_skus)).classes("text-lg font-bold text-primary")
                        ui.element("div").classes("w-px h-8 bg-gray-400 shrink-0")
                        with ui.column().classes("gap-0"):
                            ui.label("Stock valorizado").classes("text-xs text-gray-600")
                            _sv_fmt = f"$ {stock_valorizado:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                            ui.label(_sv_fmt).classes("text-lg font-bold text-primary")
            result_area.clear()
            with result_area:
                with ui.element("table").classes("w-full border-collapse text-sm"):
                    with ui.element("thead"):
                        with ui.element("tr").classes("bg-primary text-white font-semibold text-center"):
                            for col_key, h in [("id", "ID"), ("producto", "Producto"), ("sku", "SKU"), ("sales_price", "Precio venta"), ("qty", "Cantidad"), ("buscar", "Buscar")]:
                                th = ui.element("th").classes("px-3 py-2 border cursor-pointer hover:bg-primary/80")
                                if col_key != "buscar":
                                    th.on("click", lambda c=col_key: _on_sort_stock(c))
                                with th:
                                    ui.label(h)
                    with ui.element("tbody"):
                        for it in items_sorted:
                            with ui.element("tr").classes("border-t hover:bg-gray-50"):
                                with ui.element("td").classes("px-3 py-1 border"):
                                    ui.label(str(it.get("id", "—")))
                                with ui.element("td").classes("px-3 py-1 border"):
                                    ui.label(str(it.get("producto", "—")))
                                with ui.element("td").classes("px-3 py-1 border"):
                                    _sku_val = (it.get("sku") or "").strip()
                                    ui.label(_sku_val if _sku_val else "—")
                                with ui.element("td").classes("px-3 py-1 border text-right"):
                                    _sp = it.get("sales_price") or 0
                                    ui.label(f"$ {_sp:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
                                with ui.element("td").classes("px-3 py-1 border font-medium text-center"):
                                    ui.label(f"{it.get('qty', 0):,}".replace(",", "."))
                                with ui.element("td").classes("px-3 py-1 border text-center"):
                                    def _abrir_historial(uid=user["id"], iid=it.get("id", ""), prod=it.get("producto", "—"), sku_val=(it.get("sku") or "").strip()):
                                        dialog = ui.dialog().props("persistent")
                                        with dialog:
                                            with ui.card().classes("p-6 min-w-[400px] max-w-[600px] max-h-[80vh] overflow-hidden flex flex-col"):
                                                hist_container = ui.column().classes("w-full gap-2 flex-1 min-h-0")
                                                with hist_container:
                                                    ui.spinner(size="lg")
                                                    ui.label("Buscando historial...").classes("text-gray-600")
                                        dialog.open()

                                        async def _cargar_y_mostrar():
                                            hist, err = await run.io_bound(fetch_qb_item_history, uid, iid, sku_val)
                                            hist_container.clear()
                                            with hist_container:
                                                with ui.row().classes("w-full gap-4 mb-4 border-b-2 border-gray-300 pb-3"):
                                                    with ui.column().classes("flex-1 min-w-0 gap-1"):
                                                        ui.label(str(prod)[:80] + ("..." if len(str(prod)) > 80 else "")).classes("text-base font-bold")
                                                        ui.label(f"ID: {iid}").classes("text-sm font-mono text-gray-600")
                                                if err:
                                                    ui.label(f"Error: {err}").classes("text-negative")
                                                    return
                                                if not hist:
                                                    ui.label("No se encontraron ventas, compras ni cotizaciones para este producto.").classes("text-gray-500")
                                                    return
                                                with ui.element("div").classes("w-full overflow-x-auto overflow-y-auto").style("max-height: 320px"):
                                                    with ui.element("table").classes("w-full border-collapse text-sm"):
                                                        with ui.element("thead"):
                                                            with ui.element("tr").classes("bg-primary text-white font-semibold sticky top-0"):
                                                                for hdr in ["Tipo", "Fecha", "Invoice", "P. venta u$"]:
                                                                    with ui.element("th").classes("px-2 py-1 border"):
                                                                        ui.label(hdr)
                                                        with ui.element("tbody"):
                                                            for h in hist:
                                                                with ui.element("tr").classes("border-t hover:bg-gray-50"):
                                                                    with ui.element("td").classes("px-2 py-1 border"):
                                                                        ui.label(h.get("tipo", "—"))
                                                                    with ui.element("td").classes("px-2 py-1 border"):
                                                                        ui.label(h.get("fecha", "—"))
                                                                    with ui.element("td").classes("px-2 py-1 border"):
                                                                        doc_txt = str(h.get("doc", "—"))[:40]
                                                                        qb_id = h.get("qb_id") or ""
                                                                        qb_tipo = h.get("qb_tipo") or ""
                                                                        if qb_tipo == "invoice" and qb_id:
                                                                            async def _descargar_inv(uid=uid, inv_id=qb_id, doc=doc_txt):
                                                                                pdf_bytes, err = await run.io_bound(fetch_qb_invoice_pdf, uid, inv_id)
                                                                                if err:
                                                                                    ui.notify(f"Error: {err}", color="negative")
                                                                                    return
                                                                                with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
                                                                                    f.write(pdf_bytes)
                                                                                    path = f.name
                                                                                nombre = _qb_invoice_pdf_download_basename(doc)
                                                                                ui.download(path, nombre)
                                                                                ui.notify("Descarga iniciada", color="positive")
                                                                            ui.button(doc_txt, on_click=_descargar_inv).props("flat dense no-caps").classes("text-primary underline hover:no-underline cursor-pointer p-0 min-w-0 font-normal")
                                                                        else:
                                                                            ui.label(doc_txt)
                                                                    _p = h.get("precio", 0)
                                                                    _tipo = h.get("tipo", "")
                                                                    _p_fmt = f"{_p:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                                                                    with ui.element("td").classes("px-2 py-1 border text-right"):
                                                                        ui.label(_p_fmt if _tipo == "Venta" else "—")
                                                with ui.row().classes("w-full justify-end mt-4"):
                                                    ui.button("Cerrar", on_click=dialog.close, color="secondary").props("flat")

                                        background_tasks.create(_cargar_y_mostrar(), name="stock_historial")
                                    ui.button("Buscar", on_click=lambda uid=user["id"], iid=it.get("id", ""), prod=it.get("producto", "—"), sku_val=(it.get("sku") or "").strip(): _abrir_historial(uid, iid, prod, sku_val)).props("dense no-caps flat").classes("text-primary hover:bg-primary/10")

        def _cargar() -> None:
            items, err = fetch_qb_items(user["id"])
            if err:
                result_area.clear()
                with result_area:
                    ui.label(f"Error: {err}").classes("text-negative")
                return
            items_ref[:] = [i for i in (items or []) if (i.get("qty") or 0) > 0]
            if not items_ref:
                result_area.clear()
                with result_area:
                    ui.label("No hay items con stock en QuickBooks.").classes("text-gray-500")
                return
            _pintar_tabla()

        async def _cargar_async() -> None:
            await run.io_bound(_cargar)

        background_tasks.create(_cargar_async(), name="cargar_stock_qb")


