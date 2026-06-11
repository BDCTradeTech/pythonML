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


def _groq_generate(api_key: str, prompt: str) -> str:
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 300,
        "temperature": 0.7,
    }
    resp = _requests.post(url, headers=headers, json=payload, timeout=15)
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]


def _get_user_nickname(access_token: str, user_id: Any) -> str:
    try:
        resp = _requests.get(
            f"https://api.mercadolibre.com/users/{user_id}",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=10,
        )
        if resp.ok:
            return (resp.json().get("nickname") or "").strip()
    except Exception:
        pass
    return ""


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


def _saludo_por_hora() -> str:
    tz_arg = timezone(timedelta(hours=-3))
    hora = datetime.now(tz_arg).hour
    if 5 <= hora < 12:
        return "buenos días"
    if 12 <= hora < 19:
        return "buenas tardes"
    return "buenas noches"


def _build_frases_card(resp_area_ref: list) -> None:
    items: list = _load_json_config("preguntas_frases_cierre", _DEFAULT_FRASES)
    state: dict = {"editing_idx": None, "adding": False}

    def _apply_to_textarea(texto: str) -> None:
        ta = resp_area_ref[0]
        if ta is None:
            ui.notify("Seleccioná una pregunta primero", type="warning")
            return
        lines = (ta.value or "").split("\n")
        if len(lines) >= 2:
            lines[-2] = texto
        elif len(lines) == 1 and lines[0]:
            lines.append(texto)
        else:
            lines = [texto]
        ta.set_value("\n".join(lines))

    with ui.element("div").style(
        "display:flex;flex-direction:column;flex:1;"
        "border:0.5px solid var(--color-border-tertiary);"
        "border-top:3px solid #2e7d32;"
        "border-radius:var(--border-radius-md);padding:10px;box-sizing:border-box;"
        "background:var(--color-background-secondary)"
    ):
        with ui.element("div").style(
            "display:flex;align-items:center;gap:4px;margin-bottom:6px"
        ):
            ui.html('<i class="ti ti-quote" style="font-size:13px;color:#2e7d32"></i>')
            ui.label("FRASES DE CIERRE").style(
                "font-size:10px;color:#2e7d32;letter-spacing:0.05em;font-weight:600"
            )

        list_col = ui.column().classes("w-full gap-0").style("flex:1")

        def _refresh() -> None:
            list_col.clear()
            with list_col:
                for idx in range(len(items)):
                    texto = items[idx]
                    with ui.element("div").style(
                        "display:flex;align-items:center;gap:2px;"
                        "border-bottom:0.5px solid #eeeeee;padding:3px 0"
                    ):
                        if state["editing_idx"] == idx:
                            edit_inp = (
                                ui.input(value=texto)
                                .classes("flex-1")
                                .props("dense outlined")
                                .style("font-size:11px")
                            )

                            def _confirm_edit(i=idx, inp=edit_inp) -> None:
                                items[i] = inp.value
                                state["editing_idx"] = None
                                _refresh()

                            ui.button(icon="check", on_click=_confirm_edit).props(
                                "flat dense round color=positive"
                            ).style("font-size:11px")
                        else:
                            ui.label(texto).style(
                                "flex:1;font-size:11px;color:#1976d2;cursor:pointer;line-height:1.4"
                            ).on("click", lambda t=texto: _apply_to_textarea(t))

                            def _edit(i=idx) -> None:
                                state["editing_idx"] = i
                                state["adding"] = False
                                _refresh()

                            def _del(i=idx) -> None:
                                items.pop(i)
                                if state["editing_idx"] == i:
                                    state["editing_idx"] = None
                                elif (
                                    state["editing_idx"] is not None
                                    and state["editing_idx"] > i
                                ):
                                    state["editing_idx"] -= 1
                                _refresh()

                            ui.html(
                                '<i class="ti ti-pencil"'
                                ' style="font-size:11px;color:#9ca3af;cursor:pointer;padding:2px"></i>'
                            ).on("click", _edit)
                            ui.html(
                                '<i class="ti ti-trash"'
                                ' style="font-size:11px;color:#ef4444;cursor:pointer;padding:2px"></i>'
                            ).on("click", _del)

                if state["adding"]:
                    with ui.element("div").style(
                        "display:flex;align-items:center;gap:4px;padding:3px 0"
                    ):
                        add_inp = (
                            ui.input(placeholder="Nueva frase...")
                            .classes("flex-1")
                            .props("dense outlined")
                            .style("font-size:11px")
                        )

                        def _confirm_add() -> None:
                            val = add_inp.value.strip()
                            if val:
                                items.append(val)
                            state["adding"] = False
                            _refresh()

                        ui.button(icon="check", on_click=_confirm_add).props(
                            "flat dense round color=positive"
                        )

        _refresh()

        with ui.element("div").style(
            "display:flex;align-items:center;justify-content:space-between;margin-top:8px"
        ):
            def _add() -> None:
                state["adding"] = True
                state["editing_idx"] = None
                _refresh()

            ui.button("+ Agregar frase", on_click=_add).props(
                "flat dense no-caps"
            ).style("font-size:11px;color:#1976d2")

            def _save() -> None:
                cleaned = [s.strip() for s in items if s.strip()]
                set_app_config("preguntas_frases_cierre", json.dumps(cleaned))
                ui.notify("Guardado ✓", color="positive", timeout=1500)

            ui.button("Guardar", on_click=_save).props(
                "unelevated dense no-caps"
            ).style("background:#1976d2;color:#fff;font-size:11px")


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

        # CSS inyectado una vez; persiste a través de _cargar_async
        ui.html("""
<style>
.pq-row { cursor: pointer; }
.pq-row:hover > td { background: #f5f5f5 !important; }
.pq-row.pq-selected > td { background: #e3f2fd !important; }
.pq-row.pq-selected > td:first-child { border-left: 3px solid #1976d2; }
</style>
""")

        ml_nickname_holder: list = [""]
        resp_area_ref: list = [None]

        main_area = ui.column().classes("w-full gap-0")
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
            resp_area_ref[0] = None

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
                # ── BARRA SUPERIOR ──────────────────────────────────────────────
                with ui.element("div").style(
                    "width:100%;display:flex;align-items:center;justify-content:space-between;"
                    "background:#e3f2fd;border-bottom:0.5px solid #bbdefb;"
                    "padding:6px 12px;box-sizing:border-box"
                ):
                    ui.html(
                        f'<span style="font-size:13px;color:#1565c0">Sin responder:&nbsp;</span>'
                        f'<span style="font-size:13px;font-weight:700;color:#d32f2f">{len(questions)}</span>'
                    )
                    ui.button(
                        "Actualizar",
                        on_click=lambda: background_tasks.create(
                            _cargar_async(), name="cargar_preguntas"
                        ),
                    ).props("flat dense no-caps icon=refresh").style(
                        "font-size:13px;color:#1565c0"
                    )

                # ── TABLA ───────────────────────────────────────────────────────
                _TH = (
                    "background:#e8f4fd;color:#1565c0;font-size:10px;font-weight:600;"
                    "text-transform:uppercase;letter-spacing:0.05em;"
                    "padding:6px 8px;white-space:nowrap;border-bottom:0.5px solid #eeeeee;"
                    "position:sticky;top:0;z-index:10"
                )
                _TD = "padding:5px 8px;font-size:12px;border-bottom:0.5px solid #eeeeee"

                row_elements: List = []

                with ui.element("div").style("width:100%;overflow-x:auto"):
                    with ui.element("table").style(
                        "width:100%;border-collapse:collapse;table-layout:fixed"
                    ):
                        with ui.element("thead"):
                            with ui.element("tr"):
                                for _h, _w, _align in [
                                    ("Producto",  "28%", "left"),
                                    ("Pregunta",  "37%", "left"),
                                    ("Comprador", "17%", "right"),
                                    ("Hace",       "8%", "right"),
                                    ("",          "10%", "center"),
                                ]:
                                    with ui.element("th").style(
                                        f"{_TH};width:{_w};text-align:{_align}"
                                    ):
                                        ui.label(_h)

                        with ui.element("tbody"):
                            for _i, q in enumerate(questions):
                                item_id       = str(q.get("item_id") or "")
                                item_title    = item_titles.get(item_id, item_id)
                                text_q        = q.get("text") or ""
                                from_obj      = q.get("from") or {}
                                buyer_display = f"#{from_obj.get('id', '—')}"
                                age           = _time_ago(q.get("date_created") or "")

                                tr = ui.element("tr").classes("pq-row")
                                row_elements.append(tr)
                                with tr:
                                    with ui.element("td").style(
                                        f"{_TD};overflow:hidden;text-overflow:ellipsis;white-space:nowrap"
                                    ):
                                        ui.label(item_title[:60]).style("font-weight:500")
                                    with ui.element("td").style(
                                        f"{_TD};overflow:hidden;text-overflow:ellipsis;"
                                        "white-space:nowrap;color:#374151"
                                    ):
                                        ui.label(
                                            text_q[:80] + ("…" if len(text_q) > 80 else "")
                                        )
                                    with ui.element("td").style(
                                        f"{_TD};text-align:right;overflow:hidden;"
                                        "text-overflow:ellipsis;white-space:nowrap;"
                                        "color:#6b7280;font-family:monospace;font-size:11px"
                                    ):
                                        ui.label(buyer_display)
                                    with ui.element("td").style(
                                        f"{_TD};text-align:right;color:#9ca3af;font-size:11px"
                                    ):
                                        ui.label(age)
                                    with ui.element("td").style(
                                        f"{_TD};text-align:center"
                                    ):
                                        ui.html(
                                            '<i class="ti ti-chevron-right"'
                                            ' style="font-size:14px;color:#9ca3af"></i>'
                                        )
                                tr.on(
                                    "click",
                                    lambda q=q, t=item_title, _tr=tr: _open_detail(q, t, _tr),
                                )

                # ── PANEL DE DETALLE ────────────────────────────────────────────
                detail_panel = ui.element("div").style(
                    "display:grid;grid-template-columns:1fr 1fr;gap:16px;"
                    "width:100%;background:#fafafa;"
                    "border-top:0.5px solid var(--color-border-tertiary);"
                    "padding:10px;box-sizing:border-box"
                )
                detail_panel.set_visibility(False)

                def _open_detail(q: dict, title: str, active_tr) -> None:
                    for r in row_elements:
                        r.classes(remove="pq-selected")
                    active_tr.classes(add="pq-selected")

                    detail_panel.clear()
                    detail_panel.set_visibility(True)

                    qid         = q.get("id")
                    text        = q.get("text") or ""
                    from_obj    = q.get("from") or {}
                    from_id     = from_obj.get("id")
                    ml_nickname = ml_nickname_holder[0]

                    with detail_panel:
                        # ── Columna izquierda ───────────────────────────────────
                        with ui.element("div").style(
                            "display:flex;flex-direction:column;gap:8px;"
                            "border-right:1.5px solid #e0e0e0;padding-right:8px"
                        ):
                            # Tarjeta PREGUNTA DEL COMPRADOR
                            with ui.element("div").style(
                                "border:0.5px solid var(--color-border-tertiary);"
                                "border-top:3px solid #1976d2;"
                                "border-radius:var(--border-radius-md);padding:10px;"
                                "background:var(--color-background-secondary)"
                            ):
                                with ui.element("div").style(
                                    "display:flex;align-items:center;gap:4px;margin-bottom:6px"
                                ):
                                    ui.html(
                                        '<i class="ti ti-message-circle"'
                                        ' style="font-size:13px;color:#1976d2"></i>'
                                    )
                                    ui.label("PREGUNTA DEL COMPRADOR").style(
                                        "font-size:10px;color:#1565c0;"
                                        "letter-spacing:0.05em;font-weight:600"
                                    )
                                ui.label(text).style(
                                    "font-size:12px;line-height:1.5;color:#374151"
                                )

                            # Tarjeta TU RESPUESTA
                            with ui.element("div").style(
                                "border:0.5px solid var(--color-border-tertiary);"
                                "border-top:3px solid #f57c00;"
                                "border-radius:var(--border-radius-md);padding:10px;"
                                "background:var(--color-background-secondary)"
                            ):
                                with ui.element("div").style(
                                    "display:flex;align-items:center;gap:4px;margin-bottom:6px"
                                ):
                                    ui.html(
                                        '<i class="ti ti-pencil"'
                                        ' style="font-size:13px;color:#f57c00"></i>'
                                    )
                                    ui.label("TU RESPUESTA").style(
                                        "font-size:10px;color:#e65100;"
                                        "letter-spacing:0.05em;font-weight:600"
                                    )
                                resp_area = ui.textarea(
                                    value="",
                                    placeholder="Escribí tu respuesta aquí...",
                                ).classes("w-full").props("outlined rows=8").style(
                                    "font-size:12px"
                                )
                                resp_area_ref[0] = resp_area

                                with ui.element("div").style(
                                    "display:flex;align-items:center;gap:8px;"
                                    "margin-top:8px;flex-wrap:wrap"
                                ):
                                    groq_btn = ui.button("Sugerir con Groq").props(
                                        "unelevated dense no-caps"
                                    ).style(
                                        "background:#f57c00;color:#fff;font-size:12px"
                                    )
                                    enviar_btn = ui.button("Enviar respuesta").props(
                                        "unelevated dense no-caps"
                                    ).style(
                                        "background:#2e7d32;color:#fff;font-size:12px"
                                    )

                        # ── Columna derecha ─────────────────────────────────────
                        with ui.element("div").style(
                            "display:flex;flex-direction:column;height:100%"
                        ):
                            _build_frases_card(resp_area_ref)

                    async def _on_groq_click() -> None:
                        groq_key = get_app_config("groq_api_key")
                        if not groq_key:
                            ui.notify(
                                "Configurá tu API key de Groq en Configuración → IA/Groq",
                                type="warning",
                            )
                            return
                        frases = _load_json_config("preguntas_frases_cierre", _DEFAULT_FRASES)
                        frase_aleatoria = random.choice(frases) if frases else ""
                        groq_btn.props("loading")
                        try:
                            buyer_nick = ""
                            if from_id:
                                buyer_nick = await run.io_bound(
                                    _get_user_nickname, access_token, from_id
                                )
                            if not buyer_nick:
                                buyer_nick = "estimado cliente"
                            saludo = _saludo_por_hora()
                            saludo_completo = f"Hola {buyer_nick}, {saludo}."
                            prompt = (
                                f"Sos vendedor en MercadoLibre Argentina.\n"
                                f"Producto: {title}\n"
                                f"Pregunta: {text}\n\n"
                                f"Respondé SOLO la respuesta a la pregunta, sin saludo ni cierre.\n"
                                f"En español rioplatense, amable y breve. Solo el cuerpo de la respuesta."
                            )
                            texto_groq = await run.io_bound(_groq_generate, groq_key, prompt)
                            texto_groq = (texto_groq or "").strip()
                            if texto_groq:
                                partes = [saludo_completo, texto_groq]
                                if frase_aleatoria:
                                    partes.append(frase_aleatoria)
                                if ml_nickname:
                                    partes.append(f"Muchas gracias, {ml_nickname}.")
                                resp_area.set_value("\n".join(partes))
                                ui.notify("Sugerencia lista ✓", color="positive")
                            else:
                                ui.notify("Groq no devolvió texto", type="warning")
                        except Exception as exc:
                            ui.notify(f"Error Groq: {exc}", type="negative")
                        finally:
                            groq_btn.props(remove="loading")

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
                                ui.notify("Respuesta enviada ✓", color="positive")
                                for r in row_elements:
                                    r.classes(remove="pq-selected")
                                detail_panel.clear()
                                detail_panel.set_visibility(False)
                                resp_area_ref[0] = None
                                background_tasks.create(
                                    _cargar_async(), name="cargar_preguntas"
                                )
                            else:
                                err_msg = (
                                    (result["body"] or {}).get("message")
                                    or str(result["body"])[:200]
                                )
                                ui.notify(f"Error ML: {err_msg}", type="negative")
                        except Exception as exc:
                            ui.notify(f"Error al enviar: {exc}", type="negative")

                    groq_btn.on_click(_on_groq_click)
                    enviar_btn.on_click(_on_enviar_click)

        background_tasks.create(_cargar_async(), name="cargar_preguntas")
