"""
tabs/preguntas.py
Pestaña Preguntas: preguntas sin responder recibidas en MercadoLibre.
"""
from __future__ import annotations

import random
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import requests as _requests

from nicegui import app, background_tasks, run, ui

from db import get_app_config
from ml_api import get_ml_access_token, ml_get_user_id


def _require_login() -> Optional[Dict[str, Any]]:
    user = app.storage.user.get("user")
    if not user:
        ui.notify("Debes iniciar sesión", color="negative")
    return user


# ── ML / Gemini helpers (sync → run.io_bound) ───────────────────────────────

def _ml_get_questions(access_token: str, seller_id: str) -> List[dict]:
    resp = _requests.get(
        "https://api.mercadolibre.com/questions/search",
        params={"seller_id": seller_id, "status": "UNANSWERED", "api_version": 4, "limit": 50},
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json().get("questions", [])


def _ml_get_items_titles(access_token: str, item_ids: List[str]) -> Dict[str, str]:
    titles: Dict[str, str] = {}
    for i in range(0, len(item_ids), 20):
        batch = item_ids[i : i + 20]
        resp = _requests.get(
            "https://api.mercadolibre.com/items",
            params={"ids": ",".join(batch), "attributes": "id,title"},
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=15,
        )
        if resp.ok:
            for entry in resp.json():
                body = entry.get("body") or {}
                if body.get("id"):
                    titles[str(body["id"])] = body.get("title") or str(body["id"])
    return titles


def _ml_post_answer(access_token: str, question_id: Any, text: str) -> Dict[str, Any]:
    resp = _requests.post(
        "https://api.mercadolibre.com/answers",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
        json={"question_id": question_id, "text": text},
        timeout=15,
    )
    body: Any = {}
    try:
        body = resp.json()
    except Exception:
        pass
    return {"status_code": resp.status_code, "body": body}


def _groq_generate(api_key: str, prompt: str) -> str:
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 300,
        "temperature": 0.7,
    }
    print(f"[GROQ] llamando API con key={api_key[:8]}...")
    resp = _requests.post(url, headers=headers, json=payload, timeout=15)
    print(f"[GROQ] status={resp.status_code} body={resp.text[:300]}")
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]


def _time_ago(date_str: str) -> str:
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        diff = int((datetime.now(timezone.utc) - dt).total_seconds())
        if diff < 3600:
            return f"{max(diff // 60, 1)}m"
        if diff < 86400:
            return f"{diff // 3600}h"
        return f"{diff // 86400}d"
    except Exception:
        return "—"


# ── build ────────────────────────────────────────────────────────────────────

def build_tab_preguntas(container) -> None:
    container.clear()
    user = _require_login()
    if not user:
        return
    uid = user["id"]

    with container:
        access_token = get_ml_access_token(uid)
        if not access_token:
            ui.label("⚠️ No tienes MercadoLibre vinculado. Ve a Configuración.").classes(
                "text-warning"
            )
            return

        main_area = ui.column().classes("w-full gap-2")
        with main_area:
            with ui.card().classes("w-full p-8 items-center gap-4"):
                ui.spinner(size="xl")
                ui.label("Cargando preguntas...").classes("text-xl text-gray-700")

        async def _cargar_async() -> None:
            try:
                seller_id = await run.io_bound(ml_get_user_id, access_token)
            except Exception as e:
                main_area.clear()
                with main_area:
                    ui.label(f"❌ Error al obtener seller_id: {e}").classes("text-negative p-4")
                return
            if not seller_id:
                main_area.clear()
                with main_area:
                    ui.label("❌ No se pudo obtener el seller_id de MercadoLibre").classes("text-negative p-4")
                return

            try:
                questions = await run.io_bound(_ml_get_questions, access_token, seller_id)
            except Exception as e:
                main_area.clear()
                with main_area:
                    ui.label(f"❌ Error al conectar con ML: {e}").classes("text-negative p-4")
                return

            item_ids = list({str(q.get("item_id") or "") for q in questions if q.get("item_id")})
            item_titles: Dict[str, str] = {}
            if item_ids:
                try:
                    item_titles = await run.io_bound(_ml_get_items_titles, access_token, item_ids)
                except Exception:
                    pass

            main_area.clear()

            if not questions:
                with main_area:
                    with ui.card().classes("w-full p-8 items-center gap-4"):
                        ui.html(
                            '<i class="ti ti-message-check" style="font-size:48px;color:#9ca3af"></i>'
                        )
                        ui.label("No tenés preguntas sin responder").classes(
                            "text-xl text-gray-500"
                        )
                return

            with main_area:
                # ── Stats bar ───────────────────────────────────────────────
                with ui.row().classes(
                    "w-full items-center gap-4 px-3 py-1 bg-grey-2 rounded mb-1 flex-wrap"
                ):
                    with ui.row().classes("items-baseline gap-1"):
                        ui.label("Sin responder:").classes("text-xs text-gray-500")
                        ui.label(str(len(questions))).classes("text-sm font-bold").style(
                            "color:#E24B4A"
                        )
                    ui.space()
                    ui.button(
                        "Actualizar",
                        on_click=lambda: build_tab_preguntas(container),
                    ).props("unelevated dense no-caps icon=refresh").style(
                        "background:#185FA5;color:#E6F1FB"
                    ).classes("text-xs")

                detail_col = ui.column().classes("w-full mt-2")
                detail_col.style("display:none")

                # ── Table ────────────────────────────────────────────────────
                _TH = (
                    "background:#5898D4;color:#ffffff;font-weight:600;font-size:12px;"
                    "padding:5px 8px;white-space:nowrap;position:sticky;top:0;z-index:10"
                )
                _TD = "padding:4px 8px;font-size:12px;border-bottom:1px solid #f0f0f0"

                with ui.element("div").style("width:100%;overflow-x:auto"):
                    with ui.element("table").style(
                        "width:100%;border-collapse:collapse;table-layout:fixed"
                    ):
                        with ui.element("thead"):
                            with ui.element("tr"):
                                for _h, _w in [
                                    ("Producto",  "30%"),
                                    ("Pregunta",  "40%"),
                                    ("Comprador", "15%"),
                                    ("Hace",       "8%"),
                                    ("",           "7%"),
                                ]:
                                    with ui.element("th").style(
                                        f"{_TH};width:{_w};text-align:left"
                                    ):
                                        ui.label(_h)
                        with ui.element("tbody"):
                            for _i, q in enumerate(questions):
                                item_id  = str(q.get("item_id") or "")
                                title    = item_titles.get(item_id, item_id)
                                text     = q.get("text") or ""
                                buyer_id = str((q.get("from") or {}).get("id") or "—")
                                age      = _time_ago(q.get("date_created") or "")
                                _bg      = "#f5f8fd" if _i % 2 == 0 else "#ffffff"
                                with ui.element("tr").style(
                                    f"background:{_bg};cursor:pointer;"
                                    "border-bottom:1px solid #e8e8e8"
                                ).on("click", lambda q=q, t=title: _open_detail(q, t)):
                                    with ui.element("td").style(
                                        f"{_TD};overflow:hidden;text-overflow:ellipsis;white-space:nowrap"
                                    ):
                                        ui.label(title[:60]).style(
                                            "font-size:12px;font-weight:500"
                                        )
                                    with ui.element("td").style(
                                        f"{_TD};overflow:hidden;text-overflow:ellipsis;white-space:nowrap"
                                    ):
                                        ui.label(
                                            text[:80] + ("…" if len(text) > 80 else "")
                                        ).style("font-size:12px;color:#374151")
                                    with ui.element("td").style(
                                        f"{_TD};overflow:hidden;text-overflow:ellipsis;white-space:nowrap"
                                    ):
                                        ui.label(f"#{buyer_id}").style(
                                            "font-size:11px;color:#6b7280;font-family:monospace"
                                        )
                                    with ui.element("td").style(f"{_TD};text-align:center"):
                                        ui.label(age).style("font-size:11px;color:#9ca3af")
                                    with ui.element("td").style(f"{_TD};text-align:center"):
                                        ui.html(
                                            '<i class="ti ti-chevron-right"'
                                            ' style="font-size:14px;color:#9ca3af"></i>'
                                        )

                # ── Detail panel ─────────────────────────────────────────────
                def _open_detail(q: dict, title: str) -> None:
                    detail_col.clear()
                    detail_col.style("display:block")
                    qid  = q.get("id")
                    text = q.get("text") or ""

                    with detail_col:
                        with ui.card().classes("w-full p-4 mt-2 border border-blue-200"):
                            with ui.row().classes("w-full items-start justify-between mb-2"):
                                with ui.column().classes("flex-1 gap-1"):
                                    ui.label(title).classes("font-bold text-sm leading-tight")
                                    ui.label(f"Pregunta #{qid}").classes(
                                        "text-xs font-mono text-gray-400"
                                    )

                                def _cerrar() -> None:
                                    detail_col.clear()
                                    detail_col.style("display:none")

                                ui.button("↩ Cerrar", on_click=_cerrar).props(
                                    "flat dense no-caps"
                                ).classes("text-xs text-gray-500")

                            ui.separator().classes("my-2")

                            ui.label("Pregunta del comprador").classes(
                                "text-xs font-bold uppercase text-gray-500 tracking-wide mb-1"
                            )
                            with ui.card().classes("w-full p-3 bg-blue-50 border border-blue-100"):
                                ui.label(text).style(
                                    "font-size:13px;color:#1e3a5f;line-height:1.5"
                                )

                            ui.separator().classes("my-2")

                            ui.label("Tu respuesta").classes(
                                "text-xs font-bold uppercase text-gray-500 tracking-wide mb-1"
                            )
                            resp_area = ui.textarea(
                                placeholder="Escribí tu respuesta aquí..."
                            ).classes("w-full").props("outlined dense rows=3")

                            with ui.row().classes("w-full items-center gap-2 mt-2 flex-wrap"):
                                gemini_btn = ui.button("💡 Sugerir con Groq").props(
                                    "unelevated dense no-caps"
                                ).style("background:#4285F4;color:#fff").classes("text-xs")
                                enviar_btn = ui.button("📨 Enviar respuesta").props(
                                    "unelevated dense no-caps"
                                ).style("background:#1B7A3E;color:#fff").classes("text-xs")

                    async def _on_gemini_click() -> None:
                        groq_key = get_app_config("groq_api_key")
                        if not groq_key:
                            ui.notify("Configurá tu API key de Groq en Configuración → IA/Groq", type="warning")
                            return

                        # Hora Argentina (UTC-3)
                        tz_arg = timezone(timedelta(hours=-3))
                        hora = datetime.now(tz_arg).hour
                        if 5 <= hora < 12:
                            saludo_hora = "buenos días"
                        elif 12 <= hora < 19:
                            saludo_hora = "buenas tardes"
                        else:
                            saludo_hora = "buenas noches"

                        # Nombre del usuario de la app
                        _user = app.storage.user.get("user") or {}
                        nombre_usuario = (_user.get("name") or _user.get("username") or "").strip() or "vendedor"

                        # Nickname de MercadoLibre
                        try:
                            from ml_api import ml_get_user_profile
                            _profile = await run.io_bound(ml_get_user_profile, access_token)
                            ml_nickname = ((_profile or {}).get("nickname") or "").strip() or nombre_usuario
                        except Exception:
                            ml_nickname = nombre_usuario

                        # Frase aleatoria
                        frase = random.choice([
                            "Esperamos tu compra.",
                            "Estamos para lo que necesites.",
                            "Tenemos el producto en stock.",
                            "No dudes en consultarnos.",
                            "Con gusto te ayudamos.",
                        ])

                        prompt = (
                            f"Sos vendedor en MercadoLibre Argentina. "
                            f"El producto es: {title}. "
                            f"La pregunta del comprador es: {text}. "
                            f"Respondé EXACTAMENTE con este formato, sin agregar nada más:\n\n"
                            f"Hola, {saludo_hora} estimado cliente.\n"
                            f"[respuesta clara y breve en español rioplatense]\n"
                            f"{frase}\n"
                            f"Muchas gracias, {ml_nickname}.\n\n"
                            f"No agregues saludos extra, aclaraciones ni texto fuera del formato."
                        )

                        gemini_btn.props("loading")
                        try:
                            resultado = await run.io_bound(_groq_generate, groq_key, prompt)
                            if resultado and resultado.strip():
                                resp_area.set_value(resultado.strip())
                                ui.notify("Sugerencia lista ✓", color="positive")
                            else:
                                ui.notify("Groq no devolvió texto", type="warning")
                        except Exception as exc:
                            ui.notify(f"Error Groq: {exc}", type="negative")
                        finally:
                            gemini_btn.props(remove="loading")

                    async def _on_enviar_click() -> None:
                        text_resp = (resp_area.value or "").strip()
                        if not text_resp:
                            ui.notify("Escribí una respuesta antes de enviar", type="warning")
                            return
                        try:
                            result = await run.io_bound(
                                _ml_post_answer, access_token, qid, text_resp
                            )
                            if result["status_code"] in (200, 201):
                                ui.notify("Respuesta enviada exitosamente", type="positive")
                                detail_col.clear()
                                detail_col.style("display:none")
                                build_tab_preguntas(container)
                            else:
                                err_msg = (
                                    (result["body"] or {}).get("message")
                                    or str(result["body"])[:200]
                                )
                                ui.notify(f"Error ML: {err_msg}", type="negative")
                        except Exception as exc:
                            ui.notify(f"Error al enviar: {exc}", type="negative")

                    gemini_btn.on_click(_on_gemini_click)
                    enviar_btn.on_click(_on_enviar_click)

        background_tasks.create(_cargar_async(), name="cargar_preguntas")
