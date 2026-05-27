"""
Fase 3 — tabs/historicos.py
Pestaña Historicos: búsqueda en QuickBooks con historial de items.
Funciones exportadas: build_tab_historicos
"""
from __future__ import annotations

import tempfile
from typing import Any, Dict, List, Optional

from nicegui import app, background_tasks, run, ui

from db import get_qb_tokens
from qb_api import fetch_qb_invoice_pdf, fetch_qb_item_history, fetch_qb_items_search


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

def build_tab_historicos(container) -> None:
    """Pestaña Históricos: buscador de productos en QuickBooks. Escribís una palabra y debajo se muestran todos los productos que la contienen."""
    user = _require_login()
    if not user:
        return

    qb_tokens = get_qb_tokens(user["id"])
    if not qb_tokens or not qb_tokens.get("access_token"):
        with container:
            ui.label("Conectá QuickBooks en Configuración para usar el buscador de productos.").classes("text-gray-600")
        return

    with container:
        ui.label("Históricos").classes("text-xl font-semibold mb-4")
        with ui.row().classes("w-full gap-2 items-center"):
            search_input = ui.input("Buscar", placeholder="Escribí una palabra para buscar en QuickBooks...").classes("w-96 max-w-full").props("dense outlined clearable")
            ui.button("Buscar", on_click=lambda: _do_search(), color="primary").props("dense no-caps")
        results_container = ui.column().classes("w-full mt-4")

        def _do_search() -> None:
            txt = (search_input.value or "").strip()
            results_container.clear()
            with results_container:
                if not txt:
                    ui.label("Escribí al menos un carácter para buscar.").classes("text-gray-500 text-sm")
                    return
                ui.spinner(size="lg")
                ui.label("Buscando...").classes("text-gray-600")

            async def _buscar_async() -> None:
                # run.io_bound evita bloquear el event loop y es compatible con Python 3.8
                items, err, total_revisados = await run.io_bound(
                    fetch_qb_items_search, user["id"], txt
                )
                results_container.clear()
                with results_container:
                    if err:
                        ui.label(f"Error: {err}").classes("text-negative text-sm")
                        return
                    if not items:
                        msg = "No se encontraron productos."
                        if total_revisados > 0:
                            msg += f" (Se buscó en {total_revisados} productos de QuickBooks: Name, SKU y Sales Description)"
                        ui.label(msg).classes("text-gray-500 text-sm")
                        return
                    ui.label(f"Se encontraron {len(items)} productos").classes("text-sm font-medium text-gray-700 mb-2")
                    with ui.element("div").classes("w-full overflow-x-auto"):
                        with ui.element("table").classes("w-full border-collapse text-sm"):
                            with ui.element("thead"):
                                with ui.element("tr").classes("bg-primary text-white font-semibold"):
                                    with ui.element("th").classes("px-2 py-2 border text-left"):
                                        ui.label("ID")
                                    with ui.element("th").classes("px-2 py-2 border text-left"):
                                        ui.label("Productos")
                                    with ui.element("th").classes("px-2 py-2 border text-left"):
                                        ui.label("SKU")
                                    with ui.element("th").classes("px-2 py-2 border text-center min-w-[90px]"):
                                        ui.label("Buscar")
                            with ui.element("tbody"):
                                for it in items:
                                    with ui.element("tr").classes("border-t hover:bg-gray-50"):
                                        with ui.element("td").classes("px-2 py-1 border"):
                                            ui.label(str(it.get("id", "—")))
                                        with ui.element("td").classes("px-2 py-1 border"):
                                            ui.label(it.get("producto", it.get("name", "—")))
                                        with ui.element("td").classes("px-2 py-1 border"):
                                            ui.label(it.get("sku") or "—")
                                        with ui.element("td").classes("px-2 py-1 border text-center"):
                                            _uid, _iid = user["id"], it.get("id", "")
                                            _prod, _sku = it.get("producto", it.get("name", "””")), (it.get("sku") or "").strip()

                                            def _abrir_historial(uid, iid, prod, sku):
                                                d = ui.dialog().props("persistent")
                                                with d:
                                                    with ui.card().classes("p-6 min-w-[400px] max-w-[600px] max-h-[80vh] overflow-hidden flex flex-col"):
                                                        cont = ui.column().classes("w-full gap-2 flex-1 min-h-0")
                                                        with cont:
                                                            ui.spinner(size="lg")
                                                            ui.label("Buscando historial...").classes("text-gray-600")
                                                d.open()

                                                async def _cargar(uid=uid, iid=iid, prod=prod, sku=sku, cont=cont, dialog=d):
                                                    hist, err = await run.io_bound(fetch_qb_item_history, uid, iid, sku)
                                                    cont.clear()
                                                    with cont:
                                                        with ui.row().classes("w-full gap-4 mb-4 border-b-2 border-gray-300 pb-3"):
                                                            with ui.column().classes("flex-1 min-w-0 gap-1"):
                                                                ui.label(str(prod)[:80] + ("..." if len(str(prod)) > 80 else "")).classes("text-base font-bold")
                                                                ui.label(f"ID: {iid}").classes("text-sm font-mono text-gray-600")
                                                        if err:
                                                            ui.label(f"Error: {err}").classes("text-negative")
                                                            return
                                                        if not hist:
                                                            ui.label("No se encontraron ventas, compras ni cotizaciones.").classes("text-gray-500")
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
                                                                                    async def _descargar_invoice(uid=uid, inv_id=qb_id, doc=doc_txt):
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
                                                                                    ui.button(doc_txt, on_click=_descargar_invoice).props("flat dense no-caps").classes("text-primary underline hover:no-underline cursor-pointer p-0 min-w-0 font-normal")
                                                                                else:
                                                                                    ui.label(doc_txt)
                                                                            _p = h.get("precio", 0)
                                                                            _tipo = h.get("tipo", "")
                                                                            _p_fmt = f"{_p:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                                                                            with ui.element("td").classes("px-2 py-1 border text-right"):
                                                                                ui.label(_p_fmt if _tipo == "Venta" else "—")
                                                        with ui.row().classes("w-full justify-end mt-4"):
                                                            ui.button("Cerrar", on_click=dialog.close, color="secondary").props("flat")

                                                background_tasks.create(_cargar(), name="historicos_historial")

                                            ui.button("Buscar", on_click=lambda uid=_uid, iid=_iid, prod=_prod, sku=_sku: _abrir_historial(uid, iid, prod, sku)).props("dense no-caps flat").classes("text-primary hover:bg-primary/10")

            background_tasks.create(_buscar_async(), name="historicos_search")

        search_input.on("keydown.enter", lambda: _do_search())


