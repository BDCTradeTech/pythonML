"""
tabs/preguntas.py
Pestaña Preguntas: preguntas sin responder recibidas en MercadoLibre.
"""
from __future__ import annotations

import json
import random
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import requests as _requests

from nicegui import app, background_tasks, run, ui

from db import get_app_config, set_app_config
from ml_api import get_ml_access_token, ml_get_user_id, ml_get_user_profile

_DEFAULT_SALUDOS = ["Buenos días", "Buenas tardes", "Buenas noches"]
_DEFAULT_FRASES = [
    "Esperamos tu compra.",
    "Estamos para lo que necesites.",
    "Tenemos el producto en stock.",
    "No dudes en consultarnos.",
    "Con gusto te ayudamos.",
]


def _require_login() -> Optional[Dict[str, Any]]:
    user = app.storage.user.get("user")
    if not user:
        ui.notify("Debes iniciar sesión", color="negative")
    return user


def _load_json_config(key: str, default: list) -> list:
    raw = get_app_config(key)
    if not raw:
        return list(default)
    try:
        val = json.loads(raw)
        if isinstance(val, list):
            return val
    except Exception:
        pass
    return list(default)


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


def _ml_get_buyer_nickname(access_token: str, buyer_id: Any) -> str:
    try:
        resp = _requests.get(
            f"https://api.mercadolibre.com/users/{buyer_id}",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=10,
        )
        if resp.ok:
            return (resp.json().get("nickname") or "").strip()
    except Exception:
        pass
    return ""


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
    resp = _requests.post(url, headers=headers, json=payload, timeout=15)
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


def _build_list_card(
    icon: str, title: str, config_key: str, default_items: list, add_label: str
) -> None:
    """Tarjeta con lista editable persistida en app_config."""
    items: list = _load_json_config(config_key, default_items)
    state: dict = {"inputs": []}

    with ui.card().classes("w-full p-4 shadow-sm"):
        ui.label(f"{icon} {title}").classes(
            "text-xs font-bold text-gray-600 uppercase tracking-wide mb-2"
        )

        list_col = ui.column().classes("w-full gap-1")

        def _sync_to_items() -> None:
            for j, ref in enumerate(state["inputs"]):
                if j < len(items):
                    items[j] = ref.value

        def _refresh() -> None:
            state["inputs"] = []
            list_col.clear()
            with list_col:
                for idx in range(len(items)):
                    with ui.row().classes("w-full items-center gap-1 flex-nowrap"):
                        inp = ui.input(value=items[idx]).classes("flex-1").props("dense outlined")
                        state["inputs"].append(inp)

                        def _del(i=idx) -> None:
                            _sync_to_items()
                            items.pop(i)
                            _refresh()

                        ui.button(icon="delete", on_click=_del).props(
                            "flat dense round color=negative"
                        )

        _refresh()

        with ui.row().classes("w-full items-center justify-between mt-2"):
            def _add() -> None:
                _sync_to_items()
                items.append("")
                _refresh()

            ui.button(f"+ {add_label}", on_click=_add).props(
                "flat dense no-caps"
            ).classes("text-xs text-blue-600")

            def _save() -> None:
                _sync_to_items()
                cleaned = [s.strip() for s in items if s.strip()]
                set_app_config(config_key, json.dumps(cleaned))
                ui.notify("Guardado ✓", color="positive", timeout=1500)

            ui.button("💾 Guardar", on_click=_save).props(
                "unelevated dense no-caps"
            ).style("background:#185FA5;color:#E6F1FB").classes("text-xs")


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

        ml_nickname_holder: list = [""]

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
                    ui.label("❌ No se pudo obtener el seller_id de MercadoLibre").classes(
                        "text-negative p-4"
                    )
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

            try:
                _profile = await run.io_bound(ml_get_user_profile, access_token)
                ml_nickname_holder[0] = ((_profile or {}).get("nickname") or "").strip()
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
                # ── Stats bar ────────────────────────────────────────────────
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

                # ── Table ─────────────────────────────────────────────────────
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
                                    ("Pregunta",  "38%"),
                                    ("Comprador", "17%"),
                                    ("Hace",       "8%"),
                                    ("",           "7%"),
                                ]:
                                    with ui.element("th").style(
                                        f"{_TH};width:{_w};text-align:left"
                                    ):
                                        ui.label(_h)

                        with ui.element("tbody"):
                            for _i, q in enumerate(questions):
                                item_id   = str(q.get("item_id") or "")
                                title     = item_titles.get(item_id, item_id)
                                text      = q.get("text") or ""
                                from_obj  = q.get("from") or {}
                                buyer_display = (
                                    from_obj.get("nickname")
                                    or f"#{from_obj.get('id', '—')}"
                                )
                                age = _time_ago(q.get("date_created") or "")
                                _bg = "#f5f8fd" if _i % 2 == 0 else "#ffffff"

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
                                        ui.label(buyer_display).style(
                                            "font-size:11px;color:#6b7280;font-family:monospace"
                                        )
                                    with ui.element("td").style(f"{_TD};text-align:center"):
                                        ui.label(age).style("font-size:11px;color:#9ca3af")
                                    with ui.element("td").style(f"{_TD};text-align:center"):
                                        ui.html(
                                            '<i class="ti ti-chevron-right"'
                                            ' style="font-size:14px;color:#9ca3af"></i>'
                                        )

                # ── Detail panel ──────────────────────────────────────────────
                def _open_detail(q: dict, title: str) -> None:
                    detail_col.clear()
                    detail_col.style("display:block")
                    qid  = q.get("id")
                    text = q.get("text") or ""

                    with detail_col:
                        # Header
                        with ui.row().classes(
                            "w-full items-center justify-between mt-3 mb-1"
                        ):
                            with ui.column().classes("flex-1 gap-0"):
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

                        # ── Fila superior: Pregunta | Respuesta ────────────
                        with ui.row().classes("w-full gap-3"):
                            with ui.card().classes("flex-1 p-4 shadow-sm"):
                                ui.label("📋 Pregunta del comprador").classes(
                                    "text-xs font-bold text-gray-500 uppercase tracking-wide mb-2"
                                )
                                with ui.card().classes(
                                    "w-full p-3 bg-blue-50 border border-blue-100"
                                ):
                                    ui.label(text).style(
                                        "font-size:13px;color:#1e3a5f;line-height:1.5"
                                    )

                            with ui.card().classes("flex-1 p-4 shadow-sm"):
                                ui.label("✍️ Tu respuesta").classes(
                                    "text-xs font-bold text-gray-500 uppercase tracking-wide mb-2"
                                )
                                resp_area = ui.textarea(
                                    placeholder="Escribí tu respuesta aquí..."
                                ).classes("w-full").props("outlined dense rows=4")

                                with ui.row().classes("w-full items-center gap-2 mt-2"):
                                    gemini_btn = ui.button("💡 Sugerir con Groq").props(
                                        "unelevated dense no-caps"
                                    ).style("background:#4285F4;color:#fff").classes("text-xs")
                                    enviar_btn = ui.button("📨 Enviar").props(
                                        "unelevated dense no-caps"
                                    ).style("background:#1B7A3E;color:#fff").classes("text-xs")

                        # ── Fila inferior: Saludos | Frases de cierre ──────
                        with ui.row().classes("w-full gap-3 mt-1"):
                            with ui.element("div").classes("flex-1"):
                                _build_list_card(
                                    "👋", "Saludos",
                                    "preguntas_saludos",
                                    _DEFAULT_SALUDOS,
                                    "Agregar saludo",
                                )
                            with ui.element("div").classes("flex-1"):
                                _build_list_card(
                                    "💬", "Frases de cierre",
                                    "preguntas_frases_cierre",
                                    _DEFAULT_FRASES,
                                    "Agregar frase",
                                )

                    async def _on_gemini_click() -> None:
                        groq_key = get_app_config("groq_api_key")
                        if not groq_key:
                            ui.notify(
                                "Configurá tu API key de Groq en Configuración → IA/Groq",
                                type="warning",
                            )
                            return

                        saludos = _load_json_config("preguntas_saludos", _DEFAULT_SALUDOS)
                        frases  = _load_json_config("preguntas_frases_cierre", _DEFAULT_FRASES)

                        tz_arg   = timezone(timedelta(hours=-3))
                        hora_str = datetime.now(tz_arg).strftime("%H:%M")

                        _user = app.storage.user.get("user") or {}
                        nombre_usuario = (
                            (_user.get("name") or _user.get("username") or "").strip()
                            or "vendedor"
                        )
                        ml_nickname = ml_nickname_holder[0] or nombre_usuario

                        from_id = (q.get("from") or {}).get("id")
                        buyer_nick = ""
                        if from_id:
                            buyer_nick = await run.io_bound(
                                _ml_get_buyer_nickname, access_token, from_id
                            )

                        saludo_prefix = f"Hola {buyer_nick}," if buyer_nick else "Hola,"
                        saludos_str   = ", ".join(f'"{s}"' for s in saludos)
                        frases_str    = ", ".join(f'"{f}"' for f in frases)

                        prompt = (
                            f"Sos vendedor en MercadoLibre Argentina. "
                            f"El producto es: {title}. "
                            f"La pregunta del comprador es: {text}. "
                            f"La hora actual en Argentina es {hora_str}. "
                            f"Respondé EXACTAMENTE con este formato "
                            f"(reemplazá los corchetes con texto real, sin corchetes en el resultado):\n\n"
                            f"{saludo_prefix} [elegí el saludo apropiado para la hora de: {saludos_str}].\n"
                            f"[respuesta clara y breve en español rioplatense]\n"
                            f"[elegí UNA frase de cierre de: {frases_str}]\n"
                            f"Muchas gracias, {ml_nickname}.\n\n"
                            f"IMPORTANTE: sin corchetes ni explicaciones en el resultado final."
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
