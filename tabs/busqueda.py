"""
Fase 3 — tabs/busqueda.py
Pestaña Búsqueda: texto + botón, resultados en tabla (nombre, precio, vendedor, stock, tipo).
Funciones exportadas: build_tab_busqueda
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from fastapi.responses import FileResponse, PlainTextResponse
from nicegui import app, background_tasks, context, run, ui
from starlette.background import BackgroundTask

import video_ml
from ml_api import (
    get_ml_access_token,
    ml_fetch_price_for_item,
    ml_get_item,
    ml_get_items_multiget_all,
    ml_get_product_detail,
    ml_get_user_profile,
    ml_get_users_multiget,
    ml_search_similar,
)


# ---------------------------------------------------------------------------
# Helper de sesión (mismo patrón que otros tabs; se unificará en auth.py Fase 4)
# ---------------------------------------------------------------------------

def _require_login() -> Optional[Dict[str, Any]]:
    user = app.storage.user.get("user")
    if not user:
        ui.notify("Debes iniciar sesión para continuar", color="negative")
    return user


# ---------------------------------------------------------------------------
# Descargar video de ML
# ---------------------------------------------------------------------------

video_ml.limpiar_temporales()  # al iniciar el servicio: restos de descargas de más de 1 hora


@app.get("/video_ml/dl/{token}")
def _video_ml_descarga(token: str):
    """Entrega un video ya bajado a un temporal. El token (un solo uso, 10 min) lo emite el botón
    "Descargar" a un usuario logueado; después de servir el archivo se borra el temporal."""
    t = video_ml.tomar_descarga(token)
    if not t:
        return PlainTextResponse("Descarga vencida o ya usada. Volvé a tocar Descargar.", status_code=404)
    ruta, nombre = t
    return FileResponse(ruta, media_type="video/mp4", filename=nombre, background=BackgroundTask(video_ml._borrar, ruta))


def _build_card_video(video_m3u8: Optional[str] = None) -> None:
    """Card "Descargar video de ML": URLs .m3u8 (una por línea) -> ficha por video -> descarga con ffmpeg -c copy."""
    with ui.card().classes("w-full p-4 gap-2"):
        ui.label("Descargar video de ML").classes("text-lg font-semibold")
        ui.label(
            "Pegá la URL del master .m3u8 del video (http2.mlstatic.com/storage/shorts-api/...), una por línea. "
            "Se baja la variante de mayor resolución, sin recomprimir."
        ).classes("text-sm text-gray-600")
        entrada = ui.textarea(placeholder="https://http2.mlstatic.com/storage/shorts-api/.../XXXX.m3u8").classes("w-full").props("outlined dense autogrow")
        avisos = ui.column().classes("w-full gap-1")
        resultados = ui.column().classes("w-full gap-2")

        def _tarjeta(f: Dict[str, Any]) -> None:
            with resultados:
                with ui.card().classes("w-full p-3 gap-1 border"):
                    with ui.row().classes("items-center gap-3 w-full"):
                        ui.label(f["codigo"]).classes("font-mono font-semibold")
                        ui.label(f'{f["w"]}x{f["h"]}').classes("text-sm")
                        ui.label(f["orientacion"]).classes("text-sm")
                        ui.label(video_ml.fmt_dur(f["duracion"])).classes("text-sm")
                        ui.label("~" + video_ml.fmt_tam(f["tam_estimado"])).classes("text-sm")
                        if f["cumple_short"]:
                            ui.label("Cumple Short ✓").classes("text-sm text-positive font-medium")
                        else:
                            ui.label("Cumple Short ✗").classes("text-sm text-negative font-medium")
                    motivo = []
                    if f["orientacion"] != "vertical":
                        motivo.append("no es vertical")
                    if f["duracion"] > video_ml.SHORT_MAX_SEG:
                        motivo.append("dura más de 60 s")
                    ui.label(
                        "Short: vertical y hasta 60 s."
                        + (" No cumple: " + " y ".join(motivo) + "." if motivo else "")
                        + (" Variantes en el master: " + ", ".join(f["variantes"]) + "." if f["variantes"] else "")
                    ).classes("text-xs text-gray-500")
                    if f["excede_tope"]:
                        ui.label("Supera el tope de 200 MB: no se puede descargar.").classes("text-xs text-negative")
                    btn = ui.button("Descargar", icon="download").props("no-caps unelevated color=primary dense")
                    if f["excede_tope"]:
                        btn.disable()

                    async def _bajar(f=f, btn=btn) -> None:
                        btn.disable()
                        try:
                            ui.notify("Descargando en el servidor…", type="info")
                            ruta = await run.io_bound(video_ml.descargar, f["media_url"], f["codigo"])
                            token = video_ml.registrar_descarga(ruta, f["codigo"])
                            ui.download(f"/video_ml/dl/{token}", f["codigo"] + ".mp4")
                        except video_ml.VideoError as e:
                            ui.notify(str(e), type="negative", multi_line=True)
                        except Exception as e:  # noqa: BLE001
                            logging.exception("[VIDEO_ML] descarga")
                            ui.notify(f"Error inesperado: {e}", type="negative")
                        finally:
                            btn.enable()

                    btn.on_click(_bajar)

        async def _buscar() -> None:
            if not app.storage.user.get("user"):
                ui.notify("Debes iniciar sesión para continuar", color="negative")
                return
            avisos.clear()
            resultados.clear()
            validas, links, rechazadas = video_ml.parsear_entrada(entrada.value or "")
            with avisos:
                if links:
                    ui.label(
                        "ML no permite leer la página desde el servidor. Usá el marcador 'Video ML' desde la página de ML "
                        "(abajo: Cómo instalar el marcador)."
                    ).classes("text-sm text-warning")
                for r in rechazadas:
                    ui.label(f"URL rechazada (solo https://*.mlstatic.com/....m3u8): {r[:90]}").classes("text-sm text-negative")
                if len(validas) > video_ml.MAX_URLS:
                    ui.label(f"Máximo {video_ml.MAX_URLS} videos por búsqueda; se usan los primeros.").classes("text-sm text-warning")
                if not (validas or links or rechazadas):
                    ui.label("Pegá al menos una URL .m3u8.").classes("text-sm text-gray-600")
            for u in validas[:video_ml.MAX_URLS]:
                try:
                    ficha = await run.io_bound(video_ml.analizar, u)
                except video_ml.VideoError as e:
                    with avisos:
                        ui.label(f"{video_ml.codigo_de(u)}: {e}").classes("text-sm text-negative")
                    continue
                except Exception as e:  # noqa: BLE001
                    logging.exception("[VIDEO_ML] analizar")
                    with avisos:
                        ui.label(f"{video_ml.codigo_de(u)}: error inesperado: {e}").classes("text-sm text-negative")
                    continue
                _tarjeta(ficha)

        ui.button("Buscar", on_click=_buscar, color="primary").props("no-caps unelevated")

        with ui.expansion('Cómo instalar el marcador "Video ML"', icon="bookmark").classes("w-full text-sm"):
            ui.markdown(
                "1. Creá un marcador nuevo en tu navegador (en cualquier página) y llamalo **Video ML**.\n"
                "2. En la URL del marcador pegá el código de abajo (botón copiar).\n"
                "3. Abrí en MercadoLibre la página con el video (publicación o catálogo), dale play al video de la galería "
                "y tocá el marcador: abre esta pestaña con el video listo para descargar.\n"
                "4. Si no encuentra el video te lo avisa: dale play y tocá el marcador de nuevo."
            ).classes("text-sm")
            codigo = video_ml.bookmarklet_js()
            ui.textarea(value=codigo).props("outlined dense readonly autogrow").classes("w-full font-mono text-xs")
            ui.button(
                "Copiar código del marcador", icon="content_copy",
                on_click=lambda: (ui.run_javascript("navigator.clipboard.writeText(" + json.dumps(codigo) + ")"), ui.notify("Código copiado", type="positive")),
            ).props("no-caps unelevated dense color=secondary")

        if video_m3u8:
            entrada.value = "\n".join(x.strip() for x in video_m3u8.split(",") if x.strip())
            ui.timer(0.2, _buscar, once=True)


# ---------------------------------------------------------------------------
# Función exportada
# ---------------------------------------------------------------------------

def build_tab_busqueda(video_m3u8: Optional[str] = None) -> None:
    """Pestaña Búsqueda: texto + botón, resultados en tabla (nombre, precio, vendedor, stock, tipo)."""
    user = _require_login()
    if not user:
        return

    access_token = get_ml_access_token(user["id"])

    with ui.column().classes("w-full gap-4"):
        ui.label("Búsqueda en MercadoLibre").classes("text-xl font-semibold")
        with ui.row().classes("items-center gap-3"):
            input_busqueda = ui.input(
                "Texto o ID de publicación (ej: MLA1996852282)"
            ).classes("w-96").props("outlined dense")
            input_busqueda.on("keydown.enter", lambda: on_buscar())

            def on_buscar() -> None:
                background_tasks.create(_buscar_async(), name="busqueda")

            def on_borrar() -> None:
                results_container.clear()
                input_busqueda.value = ""
                solo_propias_switch.value = True
                solo_activas_stock_switch.value = True

            ui.button("Buscar", on_click=on_buscar, color="primary")
            ui.button("Borrar", on_click=on_borrar, color="secondary")
        with ui.row().classes("items-center gap-4"):
            solo_propias_switch = ui.checkbox("Solo publicaciones propias (no catálogo)", value=True).classes("text-sm")
            solo_activas_stock_switch = ui.checkbox("Solo activas con stock", value=True).classes("text-sm")
        results_container = ui.column().classes("w-full mt-2")
        _build_card_video(video_m3u8)

        def _norm_busqueda(r: dict, from_catalog: bool) -> dict:
            seller = r.get("seller") or {}
            seller_id = str(
                r.get("seller_id") or r.get("sellerId")
                or (seller.get("id") if isinstance(seller, dict) else None)
                or ""
            ).strip()
            seller_nick = (seller.get("nickname") or "").strip() if isinstance(seller, dict) else ""
            seller_display = seller_nick or (f"ID {seller_id}" if seller_id else "—")
            catalog = from_catalog or r.get("catalog_listing") is True or bool(r.get("catalog_product_id"))
            tipo = "Catálogo" if catalog else "Propia"
            price = r.get("price") or r.get("base_price")
            if price is None:
                prices = r.get("prices")
                if isinstance(prices, dict):
                    price = prices.get("amount") or prices.get("current_price")
                elif isinstance(prices, list) and prices and isinstance(prices[0], dict):
                    price = prices[0].get("amount") or prices[0].get("current_price")
                if price is None:
                    price = r.get("sale_price") or r.get("original_price")
            try:
                price = float(price) if price is not None else None
            except (TypeError, ValueError):
                price = None
            qty_raw = r.get("available_quantity") if r.get("available_quantity") is not None else r.get("availableQuantity") or r.get("quantity")
            if qty_raw is None:
                qty_display, qty_num = "””", 0
            elif isinstance(qty_raw, str):
                qty_display = qty_raw
                # API pública puede devolver rangos: RANGO_1_50, RANGO_51_100, etc.
                if qty_raw.startswith("RANGO_"):
                    try:
                        parts = qty_raw.replace("RANGO_", "").split("_")
                        qty_num = int(parts[0]) if parts else 0
                    except (ValueError, IndexError):
                        qty_num = 0
                else:
                    try:
                        qty_num = int(qty_raw)
                    except ValueError:
                        qty_num = 0
            else:
                try:
                    qty_num = int(qty_raw)
                    qty_display = str(qty_num)
                except (TypeError, ValueError):
                    qty_display, qty_num = "””", 0
            perm = (r.get("permalink") or "").strip()
            if not perm or perm == "#":
                wid = str(r.get("id") or r.get("product_id") or r.get("item_id") or "").strip()
                if wid:
                    perm = f"https://www.mercadolibre.com.ar/p/{wid}" if catalog else f"https://articulo.mercadolibre.com.ar/{wid}-_JM"
            return {
                "title": (r.get("title") or r.get("name") or "").strip(),
                "tipo": tipo,
                "price": price if price is not None else 999999999,
                "price_display": f"$ {int(price):,}".replace(",", ".") if price is not None else "—",
                "available_quantity": qty_num,
                "available_quantity_display": qty_display,
                "seller": seller_display,
                "permalink": perm or "#",
                "status": (r.get("status") or "").strip().lower(),
                "has_item_data": r.get("has_item_data", False),
                "has_active_listing": r.get("has_active_listing", True),
            }

        def _looks_like_ml_item_id(s: str) -> bool:
            """Detecta IDs tipo MLA1996852282 (3 letras + dígitos)."""
            s = s.strip().upper()
            return len(s) >= 10 and s[:3].isalpha() and s[3:].isdigit()

        async def _buscar_async() -> None:
            texto = (input_busqueda.value or "").strip()
            if not texto:
                ui.notify("Ingresá un texto o ID de publicación", color="warning")
                return
            # Si el usuario pega una URL de la API (ej: GET https://api.mercadolibre.com/items/MLA.../sale_price?context=...)
            if "api.mercadolibre.com" in texto.lower():
                metodo = "GET"
                url = texto
                if texto.upper().startswith("GET "):
                    metodo = "GET"
                    url = texto[4:].strip()
                elif texto.upper().startswith("POST "):
                    metodo = "POST"
                    url = texto[5:].strip()
                if not url.startswith("http"):
                    url = "https://" + url.lstrip("/")
                if url.startswith("http"):
                    results_container.clear()
                    with results_container:
                        ui.spinner(size="lg")
                        ui.label(f"Consultando {metodo} {url[:80]}...").classes("text-gray-600")
                    try:
                        def _fetch_api() -> Dict[str, Any]:
                            headers = {"Accept": "application/json"}
                            if access_token:
                                headers["Authorization"] = f"Bearer {access_token}"
                            if metodo.upper() == "GET":
                                r = requests.get(url, headers=headers, timeout=15)
                            else:
                                r = requests.request(metodo.upper(), url, headers=headers, timeout=15)
                            try:
                                return {"status": r.status_code, "body": r.json()}
                            except Exception:
                                return {"status": r.status_code, "body": r.text}
                        resp = await run.io_bound(_fetch_api)
                        results_container.clear()
                        with results_container:
                            ui.label(f"Respuesta ({resp.get('status', '””')})").classes("text-base font-semibold mb-2")
                            body = resp.get("body")
                            if isinstance(body, dict):
                                json_str = json.dumps(body, indent=2, ensure_ascii=False)
                            else:
                                json_str = str(body)
                            ui.html(
                                f'<pre class="p-4 bg-grey-2 rounded overflow-auto text-sm border" style="max-height: 500px;">{html.escape(json_str)}</pre>'
                            )
                            def _copiar_click(datos: str):
                                esc = json.dumps(datos)
                                ui.run_javascript(f'''
                                    (function() {{
                                        var texto = {esc};
                                        var done = function() {{ try {{ window.__copiadoOk = true; }} catch(e) {{}} }};
                                        if (navigator.clipboard && navigator.clipboard.writeText) {{
                                            navigator.clipboard.writeText(texto).then(done).catch(function() {{
                                                var ta = document.createElement("textarea");
                                                ta.value = texto;
                                                ta.style.position = "fixed";
                                                ta.style.left = "-9999px";
                                                document.body.appendChild(ta);
                                                ta.select();
                                                ta.setSelectionRange(0, 999999);
                                                try {{ document.execCommand("copy"); }} catch(e) {{}}
                                                document.body.removeChild(ta);
                                                done();
                                            }});
                                        }} else {{
                                            var ta = document.createElement("textarea");
                                            ta.value = texto;
                                            ta.style.position = "fixed";
                                            ta.style.left = "-9999px";
                                            document.body.appendChild(ta);
                                            ta.select();
                                            ta.setSelectionRange(0, 999999);
                                            try {{ document.execCommand("copy"); }} catch(e) {{}}
                                            document.body.removeChild(ta);
                                            done();
                                        }}
                                    }})();
                                ''')
                                ui.notify("Copiado al portapapeles", type="positive")
                            ui.button("Copiar respuesta", on_click=lambda d=json_str: _copiar_click(d), color="secondary").classes("mt-2").props("no-caps unelevated")
                    except Exception as err:
                        results_container.clear()
                        with results_container:
                            ui.label(f"Error: {err}").classes("text-negative")
                    return
            # Si el usuario ingresa solo números, intentar primero con MLA adelante
            texto_buscar = "MLA" + texto if texto.isdigit() else texto
            texto_fallback = texto if texto.isdigit() else None  # Para reintentar sin MLA si no hay resultados
            results_container.clear()
            with results_container:
                ui.spinner(size="lg")
                ui.label("Buscando en MercadoLibre...").classes("text-gray-600")
            # Si parece ID de publicación (ej MLA1996852282), obtener por ID; si no existe, buscar
            es_item_id = _looks_like_ml_item_id(texto_buscar)
            raw_item = None
            if es_item_id:
                try:
                    raw_item = await run.io_bound(ml_get_item, access_token, texto_buscar)
                except Exception:
                    raw_item = None
                if raw_item is not None:
                    mi_seller_id = None
                    if access_token:
                        try:
                            profile = await run.io_bound(ml_get_user_profile, access_token)
                            mi_seller_id = str((profile or {}).get("id") or "")
                        except Exception:
                            pass
                    seller_id = str(raw_item.get("seller_id") or "")
                    es_propia = mi_seller_id and seller_id and mi_seller_id == seller_id
                    results_container.clear()
                    with results_container:
                        lbl_tipo = "Tu publicación" if es_propia else "Publicación de otro vendedor"
                        ui.label(f"Datos que devuelve MercadoLibre para esta publicación ({lbl_tipo}):").classes(
                            "text-base font-semibold mb-2"
                        )
                        json_str = json.dumps(raw_item, indent=2, ensure_ascii=False)
                        ui.html(
                            f'<pre class="p-4 bg-grey-2 rounded overflow-auto text-sm border" style="max-height: 500px;">{html.escape(json_str)}</pre>'
                        )
                        perm = (raw_item.get("permalink") or "").strip()
                        with ui.row().classes("gap-2 mt-2"):
                            if perm:
                                ui.button("Ver en MercadoLibre", on_click=lambda p=perm: ui.run_javascript(f'window.open({json.dumps(p)})'), color="primary").classes("rounded px-4 py-2").props("no-caps unelevated")
                            def _copiar_click(datos: str):
                                esc = json.dumps(datos)
                                ui.run_javascript(f'''
                                    (function() {{
                                        var texto = {esc};
                                        var done = function() {{
                                            try {{ window.__copiadoOk = true; }} catch(e) {{}}
                                        }};
                                        if (navigator.clipboard && navigator.clipboard.writeText) {{
                                            navigator.clipboard.writeText(texto).then(done).catch(function() {{
                                                var ta = document.createElement("textarea");
                                                ta.value = texto;
                                                ta.style.position = "fixed";
                                                ta.style.left = "-9999px";
                                                document.body.appendChild(ta);
                                                ta.select();
                                                ta.setSelectionRange(0, 999999);
                                                try {{ document.execCommand("copy"); }} catch(e) {{}}
                                                document.body.removeChild(ta);
                                                done();
                                            }});
                                        }} else {{
                                            var ta = document.createElement("textarea");
                                            ta.value = texto;
                                            ta.style.position = "fixed";
                                            ta.style.left = "-9999px";
                                            document.body.appendChild(ta);
                                            ta.select();
                                            ta.setSelectionRange(0, 999999);
                                            try {{ document.execCommand("copy"); }} catch(e) {{}}
                                            document.body.removeChild(ta);
                                            done();
                                        }}
                                    }})();
                                ''')
                                ui.notify("Datos copiados al portapapeles. Pegá con Ctrl+V donde quieras.", type="positive")
                            ui.button("Copiar datos", on_click=lambda d=json_str: _copiar_click(d), color="secondary").classes("rounded px-4 py-2").props("no-caps unelevated")
                    return
            # Búsqueda por texto o por ID cuando ml_get_item no encontró nada
            try:
                solo_propias = getattr(solo_propias_switch, "value", True)
                data = await run.io_bound(ml_search_similar, texto_buscar, 50, access_token, solo_propias)
                # Para IDs: si no hay resultados con propias, probar sin filtrar por propias
                if es_item_id and (not data.get("results") or len(data.get("results", [])) == 0) and solo_propias:
                    data = await run.io_bound(ml_search_similar, texto_buscar, 50, access_token, False)
                # Si ingresó solo números y no hubo resultados con MLA, intentar sin MLA
                if texto_fallback and (not data.get("results") or len(data.get("results", [])) == 0):
                    data = await run.io_bound(ml_search_similar, texto_fallback, 50, access_token, solo_propias)
                    if (not data.get("results") or len(data.get("results", [])) == 0) and solo_propias:
                        data = await run.io_bound(ml_search_similar, texto_fallback, 50, access_token, False)
            except Exception as err:
                data = {"results": [], "error": str(err)}
            results = data.get("results", [])[:50]
            from_catalog = data.get("from_catalog", False)
            ids_to_fetch = [str(r.get("id") or r.get("product_id") or r.get("item_id") or "").strip() or None for r in results]
            ids_list = [x for x in ids_to_fetch if x]
            if results and ids_list:
                with results_container:
                    ui.label("Cargando detalles (precio, vendedor, stock)...").classes("text-gray-600")
                bodies = await run.io_bound(ml_get_items_multiget_all, access_token, ids_list)
                id_to_body = {str(b.get("id")): b for b in bodies if b and isinstance(b, dict)}
                for i, r in enumerate(results):
                    item_id = ids_to_fetch[i]
                    if not item_id:
                        continue
                    full = id_to_body.get(str(item_id))
                    if full is None:
                        full = await run.io_bound(ml_get_item, access_token, item_id)
                    if full and isinstance(full, dict):
                        r["_full_item"] = full  # Para mostrar JSON completo cuando es búsqueda por ID
                        if full.get("price") is not None:
                            r["price"] = full["price"]
                        elif access_token:
                            precio = await run.io_bound(ml_fetch_price_for_item, access_token, item_id, full)
                            if precio is not None:
                                r["price"] = precio
                        if full.get("available_quantity") is not None:
                            r["available_quantity"] = full["available_quantity"]
                        if full.get("seller_id") is not None:
                            r["seller_id"] = full["seller_id"]
                        if full.get("title") is not None:
                            r["title"] = full["title"]
                        if full.get("permalink") is not None:
                            r["permalink"] = full["permalink"]
                        if full.get("seller") is not None:
                            r["seller"] = full["seller"]
                        if full.get("status") is not None:
                            r["status"] = full["status"]
                        r["has_item_data"] = True
                    elif from_catalog and access_token:
                        prod = await run.io_bound(ml_get_product_detail, access_token, item_id)
                        if prod and isinstance(prod, dict):
                            if prod.get("status") is not None:
                                r["status"] = prod.get("status")
                            bw = prod.get("buy_box_winner")
                            r["has_active_listing"] = isinstance(bw, dict) and bool(bw.get("item_id"))
                            br = prod.get("buy_box_winner_price_range") or {}
                            if isinstance(br, dict):
                                amt = br.get("min") or br.get("max") or br.get("amount")
                                if amt is not None:
                                    try:
                                        r["price"] = float(amt)
                                    except (TypeError, ValueError):
                                        pass
                            if isinstance(bw, dict) and bw.get("item_id"):
                                iid = str(bw["item_id"])
                                precio = await run.io_bound(ml_fetch_price_for_item, access_token, iid, None)
                                if precio is not None:
                                    r["price"] = precio
                seller_ids = [
                    str(r.get("seller_id") or (r.get("seller", {}).get("id") if isinstance(r.get("seller"), dict) else ""))
                    for r in results
                    if r.get("seller_id") or (isinstance(r.get("seller"), dict) and r.get("seller", {}).get("id"))
                ]
                seller_ids = list(dict.fromkeys(s for s in seller_ids if s and s != "0"))
                if seller_ids and access_token:
                    nicknames = await run.io_bound(ml_get_users_multiget, access_token, seller_ids)
                    for r in results:
                        sid = str(r.get("seller_id") or "")
                        if sid and sid in nicknames:
                            r["seller"] = {"id": sid, "nickname": nicknames[sid]}
            # Para búsqueda por ID: mostrar JSON completo; para texto: tabla resumida
            mostrar_como_json = es_item_id and results
            rows = [_norm_busqueda(r, from_catalog) for r in results]
            filter_showed_all = False
            if not mostrar_como_json and getattr(solo_activas_stock_switch, "value", True):
                rows_filtradas = [
                    x for x in rows
                    if x.get("has_active_listing", True)
                    and (
                        not x.get("has_item_data")
                        or ((x.get("status") or "") == "active" and (x.get("available_quantity") or 0) > 0)
                    )
                ]
                if rows_filtradas:
                    rows = rows_filtradas
                elif rows:
                    filter_showed_all = True
            if not mostrar_como_json:
                rows.sort(key=lambda x: x["price"])
            results_container.clear()
            with results_container:
                if data.get("error"):
                    ui.label(f"Error: {data['error']}").classes("text-negative")
                    texto_busq = (input_busqueda.value or "").strip()
                    if texto_busq:
                        from urllib.parse import quote
                        busq_url = f"https://listado.mercadolibre.com.ar/{quote(texto_busq)}"
                        ui.button("Buscar en MercadoLibre", on_click=lambda u=busq_url: ui.run_javascript(f'window.open({json.dumps(u)})')).props("flat no-caps").classes("text-primary mt-2")
                elif not (rows if not mostrar_como_json else results):
                    ui.label("No se encontraron resultados.").classes("text-gray-500")
                elif mostrar_como_json:
                    ui.label("Datos que devuelve MercadoLibre para las publicaciones encontradas:").classes(
                        "text-base font-semibold mb-3"
                    )
                    with ui.element("div").classes("w-full overflow-auto").style("max-height: 70vh;"):
                        for i, r in enumerate(results):
                            full_display = r.get("_full_item")
                            if not full_display:
                                full_display = {k: v for k, v in r.items() if k != "_full_item"}
                            tit = (full_display.get("title") or full_display.get("name") or f"Resultado {i+1}")[:80]
                            with ui.card().classes("w-full mt-2"):
                                ui.label(tit).classes("font-semibold text-primary mb-2")
                                json_str_card = json.dumps(full_display, indent=2, ensure_ascii=False)
                                ui.html(f'<pre class="p-4 bg-grey-2 rounded overflow-auto text-sm border" style="max-height: 400px;">{html.escape(json_str_card)}</pre>')
                                perm = (full_display.get("permalink") or "").strip()
                                with ui.row().classes("gap-2 mt-1"):
                                    if perm:
                                        ui.button("Ver en MercadoLibre", on_click=lambda p=perm: ui.run_javascript(f'window.open({json.dumps(p)})'), color="primary").classes("rounded px-3 py-1.5").props("no-caps unelevated")
                                    def _copiar_card(js: str) -> None:
                                        esc = json.dumps(js)
                                        ui.run_javascript(f'''
                                            (function() {{
                                                var texto = {esc};
                                                if (navigator.clipboard && navigator.clipboard.writeText) {{
                                                    navigator.clipboard.writeText(texto).then(function() {{}}).catch(function() {{
                                                        var ta = document.createElement("textarea");
                                                        ta.value = texto;
                                                        ta.style.position = "fixed"; ta.style.left = "-9999px";
                                                        document.body.appendChild(ta);
                                                        ta.select();
                                                        ta.setSelectionRange(0, 999999);
                                                        try {{ document.execCommand("copy"); }} catch(e) {{}}
                                                        document.body.removeChild(ta);
                                                    }});
                                                }} else {{
                                                    var ta = document.createElement("textarea");
                                                    ta.value = texto;
                                                    ta.style.position = "fixed"; ta.style.left = "-9999px";
                                                    document.body.appendChild(ta);
                                                    ta.select();
                                                    ta.setSelectionRange(0, 999999);
                                                    try {{ document.execCommand("copy"); }} catch(e) {{}}
                                                    document.body.removeChild(ta);
                                                }}
                                            }})();
                                        ''')
                                        ui.notify("Datos copiados al portapapeles. Pegá con Ctrl+V.", type="positive")
                                    ui.button("Copiar datos", on_click=lambda j=json_str_card: _copiar_card(j), color="secondary").classes("rounded px-3 py-1.5").props("no-caps unelevated")
                else:
                    if filter_showed_all:
                        ui.label(
                            "No se encontraron publicaciones activas con stock. Mostrando todos los resultados."
                        ).classes("text-amber-600 text-sm mb-2")
                    with ui.element("div").classes("w-full overflow-x-auto border rounded-lg").style("min-width: 800px;"):
                        with ui.row().classes("w-full bg-blue-600 text-white py-2 px-3 font-semibold flex-nowrap"):
                            ui.label("Nombre del producto").classes("min-w-[280px] shrink-0 text-left")
                            ui.label("Precio").classes("min-w-[120px] shrink-0 text-right")
                            ui.label("Vendedor").classes("min-w-[150px] shrink-0 text-left")
                            ui.label("Stock disp.").classes("min-w-[90px] shrink-0 text-right")
                            ui.label("Tipo").classes("min-w-[90px] shrink-0 text-left")
                            ui.label("Acciones").classes("min-w-[180px] shrink-0 text-left")
                        for idx, r in enumerate(rows):
                            raw_for_copiar = results[idx] if idx < len(results) else {}
                            datos_api = raw_for_copiar.get("_full_item") or raw_for_copiar
                            json_para_copiar = json.dumps(datos_api, indent=2, ensure_ascii=False)
                            perm = r.get("permalink", "#")
                            with ui.row().classes("w-full py-2 px-3 border-b border-gray-200 hover:bg-gray-50 flex-nowrap"):
                                tit = (r.get("title") or "")[:80] + ("..." if len(r.get("title") or "") > 80 else "")
                                ui.label(tit).classes("min-w-[280px] shrink-0 text-left")
                                ui.label(r.get("price_display", "—")).classes("min-w-[120px] shrink-0 text-right font-medium")
                                ui.label(str(r.get("seller", "—"))).classes("min-w-[150px] shrink-0 text-left")
                                ui.label(str(r.get("available_quantity_display", r.get("available_quantity", "—")))).classes("min-w-[90px] shrink-0 text-right")
                                ui.label(r.get("tipo", "")).classes("min-w-[90px] shrink-0 text-left")
                                with ui.row().classes("min-w-[180px] shrink-0 gap-1"):
                                    if perm and perm != "#":
                                        ui.button("Ver en ML", on_click=lambda p=perm: ui.run_javascript(f'window.open({json.dumps(p)})'), color="primary").classes("rounded px-2 py-1").props("no-caps unelevated")
                                    def _copiar_tabla(js: str) -> None:
                                        esc = json.dumps(js)
                                        ui.run_javascript(f'''
                                            (function() {{
                                                var texto = {esc};
                                                if (navigator.clipboard && navigator.clipboard.writeText) {{
                                                    navigator.clipboard.writeText(texto).then(function() {{}}).catch(function() {{
                                                        var ta = document.createElement("textarea");
                                                        ta.value = texto;
                                                        ta.style.position = "fixed"; ta.style.left = "-9999px";
                                                        document.body.appendChild(ta);
                                                        ta.select();
                                                        ta.setSelectionRange(0, 999999);
                                                        try {{ document.execCommand("copy"); }} catch(e) {{}}
                                                        document.body.removeChild(ta);
                                                    }});
                                                }} else {{
                                                    var ta = document.createElement("textarea");
                                                    ta.value = texto;
                                                    ta.style.position = "fixed"; ta.style.left = "-9999px";
                                                    document.body.appendChild(ta);
                                                    ta.select();
                                                    ta.setSelectionRange(0, 999999);
                                                    try {{ document.execCommand("copy"); }} catch(e) {{}}
                                                    document.body.removeChild(ta);
                                                }}
                                            }})();
                                        ''')
                                        ui.notify("Datos copiados al portapapeles. Pegá con Ctrl+V.", type="positive")
                                    ui.button("Copiar datos", on_click=lambda j=json_para_copiar: _copiar_tabla(j), color="secondary").classes("rounded px-2 py-1").props("no-caps unelevated")



