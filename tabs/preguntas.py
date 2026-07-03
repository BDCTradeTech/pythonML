"""
tabs/preguntas.py
Pestaña Preguntas: preguntas sin responder recibidas en MercadoLibre.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import requests as _requests

from nicegui import app, background_tasks, context, run, ui

from db import get_app_config, set_app_config
from ml_api import get_ml_access_token, get_ml_session, ml_get_user_id, ml_get_user_profile

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
    resp = get_ml_session().get(
        "https://api.mercadolibre.com/questions/search",
        params={"seller_id": seller_id, "status": "UNANSWERED", "api_version": 4, "limit": 50},
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json().get("questions", [])


def _ml_get_items_info(access_token: str, item_ids: List[str]) -> Dict[str, dict]:
    info: Dict[str, dict] = {}
    for i in range(0, len(item_ids), 20):
        batch = item_ids[i : i + 20]
        resp = get_ml_session().get(
            "https://api.mercadolibre.com/items",
            params={"ids": ",".join(batch), "attributes": "id,title,status"},
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=15,
        )
        if resp.ok:
            for entry in resp.json():
                body = entry.get("body") or {}
                if body.get("id"):
                    info[str(body["id"])] = {
                        "title": body.get("title") or str(body["id"]),
                        "status": body.get("status") or "",
                    }
    return info


def _ml_delete_question(access_token: str, question_id: Any) -> Dict[str, Any]:
    resp = get_ml_session().delete(
        f"https://api.mercadolibre.com/questions/{question_id}",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=15,
    )
    body: Any = {}
    try:
        body = resp.json()
    except Exception:
        pass
    return {"status_code": resp.status_code, "body": body}


def _ml_post_answer(access_token: str, question_id: Any, text: str) -> Dict[str, Any]:
    resp = get_ml_session().post(
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


def _gemini_generate(api_key: str, prompt: str) -> str:
    from google import genai
    client = genai.Client(api_key=api_key)
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
    )
    return response.text


def _get_user_nickname(access_token: str, user_id: Any) -> str:
    try:
        resp = get_ml_session().get(
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


def _replace_closing_phrase(ta, frase: str) -> None:
    lines = (ta.value or "").split("\n")
    if len(lines) >= 2:
        lines[-2] = frase
    elif len(lines) == 1 and lines[0]:
        lines.append(frase)
    else:
        lines = [frase]
    ta.set_value("\n".join(lines))


def _build_frases_card(resp_groq_ref: list, resp_gemini_ref: list) -> None:
    items: list = _load_json_config("preguntas_frases_cierre", _DEFAULT_FRASES)
    state: dict = {"editing_idx": None, "adding": False}

    def _apply_to_both(texto: str) -> None:
        ta_g = resp_groq_ref[0]
        ta_m = resp_gemini_ref[0]
        if ta_g is None and ta_m is None:
            ui.notify("Seleccioná una pregunta primero", type="warning")
            return
        if ta_g is not None:
            _replace_closing_phrase(ta_g, texto)
        if ta_m is not None:
            _replace_closing_phrase(ta_m, texto)

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
                            ).on("click", lambda t=texto: _apply_to_both(t))

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
        is_mobile_ref = {"val": False}

        async def _detect_mobile():
            w = await ui.run_javascript("return window.innerWidth")
            is_mobile_ref["val"] = int(w or 9999) < 768

        ui.timer(0, _detect_mobile, once=True)

        access_token = get_ml_access_token(uid)
        if not access_token:
            ui.label("⚠️ No tienes MercadoLibre vinculado. Ve a Configuración.").classes(
                "text-warning"
            )
            return

        ui.html("""
<style>
.pq-row { cursor: pointer; }
.pq-row:hover > td { background: #f5f5f5 !important; }
.pq-row.pq-selected > td { background: #e3f2fd !important; }
.pq-row.pq-selected > td:first-child { border-left: 3px solid #1976d2; }
</style>
""")

        ml_nickname_holder: list = [""]
        resp_area_groq_ref: list = [None]
        resp_area_gemini_ref: list = [None]

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
            item_info: Dict[str, dict] = {}
            if item_ids:
                try:
                    item_info = await run.io_bound(_ml_get_items_info, access_token, item_ids)
                except Exception:
                    pass

            try:
                _profile = await run.io_bound(ml_get_user_profile, access_token)
                ml_nickname_holder[0] = ((_profile or {}).get("nickname") or "").strip()
            except Exception:
                pass

            main_area.clear()
            resp_area_groq_ref[0] = None
            resp_area_gemini_ref[0] = None

            if not questions:
                with main_area:
                    with ui.card().classes("w-full p-8 items-center gap-4"):
                        ui.html(
                            '<i class="ti ti-message-check" style="font-size:48px;color:#9ca3af"></i>'
                        )
                        with ui.row().classes("items-center gap-2"):
                            ui.label("✅ No tenés preguntas sin responder. ¡Buen trabajo!").classes(
                                "text-xl text-gray-500"
                            )
                            ui.button(
                                "Actualizar",
                                on_click=lambda: background_tasks.create(
                                    _cargar_async(), name="cargar_preguntas"
                                ),
                            ).props("flat dense no-caps icon=refresh").style(
                                "font-size:13px;color:#1565c0"
                            )
                    _build_frases_card([None], [None])
                return

            with main_area:
                # ── BARRA SUPERIOR ──────────────────────────────────────────────
                with ui.element("div").style(
                    "width:100%;display:flex;align-items:center;justify-content:space-between;"
                    "background:#e3f2fd;border-bottom:0.5px solid #bbdefb;"
                    "padding:6px 12px;box-sizing:border-box"
                ):
                    counter_ref = [len(questions)]
                    counter_label = ui.html(
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
                            _STATUS_DOT = {
                                "active":  "#22c55e",
                                "paused":  "#f59e0b",
                                "closed":  "#ef4444",
                            }
                            for _i, q in enumerate(questions):
                                item_id    = str(q.get("item_id") or "")
                                item_entry = item_info.get(item_id, {})
                                item_title = item_entry.get("title", item_id)
                                item_status = item_entry.get("status", "")
                                text_q     = q.get("text") or ""
                                from_obj   = q.get("from") or {}
                                buyer_display = f"#{from_obj.get('id', '—')}"
                                age        = _time_ago(q.get("date_created") or "")

                                tr = ui.element("tr").classes("pq-row")
                                row_elements.append(tr)
                                with tr:
                                    _dot_color = _STATUS_DOT.get(item_status, "#9ca3af")
                                    with ui.element("td").style(
                                        f"{_TD};overflow:hidden"
                                    ):
                                        ui.html(
                                            f'<div style="display:flex;align-items:center;gap:5px;overflow:hidden">'
                                            f'<span style="width:7px;height:7px;border-radius:50%;'
                                            f'background:{_dot_color};flex-shrink:0" '
                                            f'title="{item_status}"></span>'
                                            f'<span style="overflow:hidden;text-overflow:ellipsis;'
                                            f'white-space:nowrap;font-weight:500">{item_title[:55]}</span>'
                                            f'</div>'
                                        )
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

                    resp_groq_holder   = [None]
                    resp_gemini_holder = [None]
                    groq_spin_ref      = [None]
                    gemini_spin_ref    = [None]
                    groq_err_ref       = [None]
                    gemini_err_ref     = [None]

                    # ── async helpers ───────────────────────────────────────────

                    async def _enviar_respuesta(client, qid, resp_area) -> None:
                        try:
                            await client.run_javascript(
                                "if(document.activeElement) document.activeElement.blur()"
                            )
                        except Exception:
                            pass
                        await asyncio.sleep(0.05)
                        ta = resp_area
                        text_resp = (ta.value or "").strip() if ta else ""
                        if not text_resp:
                            try:
                                await client.run_javascript(
                                    "Quasar.Notify.create({message:'Escribí una respuesta antes de enviar',"
                                    "color:'orange',position:'bottom',timeout:4000})"
                                )
                            except Exception:
                                pass
                            return
                        try:
                            result = await run.io_bound(
                                _ml_post_answer, access_token, qid, text_resp
                            )
                            if result["status_code"] in (200, 201):
                                active_tr.set_visibility(False)
                                new_count = counter_ref[0] - 1
                                counter_ref[0] = new_count
                                counter_label.set_content(
                                    f'<span style="font-size:13px;color:#1565c0">Sin responder:&nbsp;</span>'
                                    f'<span style="font-size:13px;font-weight:700;color:#d32f2f">{new_count}</span>'
                                )
                                detail_panel.clear()
                                detail_panel.set_visibility(False)
                                resp_area_groq_ref[0] = None
                                resp_area_gemini_ref[0] = None
                                try:
                                    await client.run_javascript(
                                        "Quasar.Notify.create({message:'Respuesta enviada ✓',"
                                        "color:'green',position:'bottom'})"
                                    )
                                except Exception:
                                    pass
                                async def _delayed_refresh() -> None:
                                    await asyncio.sleep(3)
                                    await _cargar_async()
                                background_tasks.create(_delayed_refresh(), name="refresh_after_answer")
                            else:
                                _body = result["body"] or {}
                                _error_code = _body.get("error", "")
                                if result["status_code"] == 400 and _error_code == "not_active_item":
                                    _msg = (
                                        "No se puede responder: la publicación está pausada o cerrada. "
                                        "Activala en MercadoLibre primero."
                                    )
                                    try:
                                        await client.run_javascript(
                                            "Quasar.Notify.create({message:"
                                            + json.dumps(_msg)
                                            + ",color:'orange',position:'bottom',timeout:6000})"
                                        )
                                    except Exception:
                                        pass
                                else:
                                    _err_msg = _body.get("message") or str(_body)[:200]
                                    _err_full = f"Error ML: {_err_msg}"
                                    try:
                                        await client.run_javascript(
                                            "Quasar.Notify.create({message:"
                                            + json.dumps(_err_full)
                                            + ",color:'red',position:'bottom',timeout:4000})"
                                        )
                                    except Exception:
                                        pass
                        except Exception as exc:
                            _exc_msg = f"Error al enviar: {str(exc)[:100]}"
                            try:
                                await client.run_javascript(
                                    "Quasar.Notify.create({message:"
                                    + json.dumps(_exc_msg)
                                    + ",color:'red',position:'bottom',timeout:4000})"
                                )
                            except Exception:
                                pass

                    async def _eliminar_pregunta(client) -> None:
                        try:
                            result = await run.io_bound(_ml_delete_question, access_token, qid)
                            if result["status_code"] == 200:
                                active_tr.set_visibility(False)
                                new_count = counter_ref[0] - 1
                                counter_ref[0] = new_count
                                counter_label.set_content(
                                    f'<span style="font-size:13px;color:#1565c0">Sin responder:&nbsp;</span>'
                                    f'<span style="font-size:13px;font-weight:700;color:#d32f2f">{new_count}</span>'
                                )
                                detail_panel.clear()
                                detail_panel.set_visibility(False)
                                try:
                                    await client.run_javascript(
                                        "Quasar.Notify.create({message:'Pregunta eliminada',"
                                        "color:'green',position:'bottom'})"
                                    )
                                except Exception as _e:
                                    logging.warning("ELIMINAR: notify ok falló: %s", _e)
                            else:
                                err_msg = (
                                    (result["body"] or {}).get("message")
                                    or str(result["body"])[:200]
                                )
                                try:
                                    await client.run_javascript(
                                        "Quasar.Notify.create({message:"
                                        + json.dumps(f"Error al eliminar: {err_msg}")
                                        + ",color:'red',position:'bottom',timeout:4000})"
                                    )
                                except Exception as _e:
                                    logging.warning("ELIMINAR: notify err falló: %s", _e)
                        except Exception as exc:
                            try:
                                await client.run_javascript(
                                    "Quasar.Notify.create({message:"
                                    + json.dumps(f"Error al eliminar: {str(exc)[:100]}")
                                    + ",color:'red',position:'bottom',timeout:4000})"
                                )
                            except Exception as _e:
                                logging.warning("ELIMINAR: notify exc falló: %s", _e)

                    async def _load_ais() -> None:
                        groq_key   = get_app_config("groq_api_key")
                        gemini_key = get_app_config("gemini_api_key")

                        frases = _load_json_config("preguntas_frases_cierre", _DEFAULT_FRASES)
                        frase_aleatoria = random.choice(frases) if frases else ""

                        buyer_nick = ""
                        if from_id:
                            try:
                                buyer_nick = await run.io_bound(
                                    _get_user_nickname, access_token, from_id
                                )
                            except Exception:
                                pass
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

                        def _build_resp(body: str) -> str:
                            partes = [saludo_completo, body.strip()]
                            if frase_aleatoria:
                                partes.append(frase_aleatoria)
                            if ml_nickname:
                                partes.append(f"Muchas gracias, {ml_nickname}.")
                            return "\n".join(partes)

                        if not groq_key:
                            groq_spin_ref[0].set_visibility(False)
                            groq_err_ref[0].set_text(
                                "Configurá tu API key de Grok en Config → IA/Sugerencias"
                            )
                            groq_err_ref[0].set_visibility(True)

                        if not gemini_key:
                            gemini_spin_ref[0].set_visibility(False)
                            gemini_err_ref[0].set_text(
                                "Configurá tu API key de Gemini en Config → IA/Sugerencias"
                            )
                            gemini_err_ref[0].set_visibility(True)

                        if not groq_key and not gemini_key:
                            return

                        async def _run_groq() -> None:
                            if not groq_key:
                                return
                            try:
                                texto = await run.io_bound(_groq_generate, groq_key, prompt)
                                resp_groq_holder[0].set_value(_build_resp(texto))
                            except Exception as exc:
                                groq_err_ref[0].set_text(f"Error Grok: {exc}")
                                groq_err_ref[0].set_visibility(True)
                            finally:
                                groq_spin_ref[0].set_visibility(False)

                        async def _run_gemini() -> None:
                            if not gemini_key:
                                return
                            try:
                                texto = await run.io_bound(_gemini_generate, gemini_key, prompt)
                                resp_gemini_holder[0].set_value(_build_resp(texto))
                            except Exception as exc:
                                gemini_err_ref[0].set_text(f"Error Gemini: {exc}")
                                gemini_err_ref[0].set_visibility(True)
                            finally:
                                gemini_spin_ref[0].set_visibility(False)

                        await asyncio.gather(_run_groq(), _run_gemini())

                    # ── UI: layout responsive ───────────────────────────────────────
                    with detail_panel:
                        is_mobile = is_mobile_ref["val"]

                        if not is_mobile:
                            # ── DESKTOP: 3 columnas ──────────────────────────────────
                            with ui.element("div").style(
                                "display:flex;gap:12px;width:100%;align-items:flex-start"
                            ):
                                # ── COL 1 ────────────────────────────────────────────
                                with ui.element("div").style(
                                    "flex:1;display:flex;flex-direction:column;gap:8px;min-width:0"
                                ):
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
                                    _build_frases_card(resp_groq_holder, resp_gemini_holder)
                                    _c_elim = context.client
                                    ui.button(
                                        "Eliminar pregunta",
                                        on_click=lambda c=_c_elim: background_tasks.create(
                                            _eliminar_pregunta(c), name="eliminar_pregunta"
                                        ),
                                    ).props("unelevated dense no-caps icon=delete").style(
                                        "background:#ef5350;color:#fff;font-size:11px;margin-top:6px"
                                    )

                                # ── COL 2 — GROK ─────────────────────────────────────
                                with ui.element("div").style("flex:1;min-width:0"):
                                    with ui.element("div").style(
                                        "border:0.5px solid var(--color-border-tertiary);"
                                        "border-top:3px solid #f57c00;"
                                        "border-radius:var(--border-radius-md);padding:10px;"
                                        "background:var(--color-background-secondary);"
                                        "display:flex;flex-direction:column;gap:8px"
                                    ):
                                        with ui.element("div").style(
                                            "display:flex;align-items:center;gap:4px"
                                        ):
                                            ui.html(
                                                '<i class="ti ti-robot"'
                                                ' style="font-size:13px;color:#e65100"></i>'
                                            )
                                            ui.label("Respuesta Grok").style(
                                                "font-size:10px;color:#e65100;"
                                                "letter-spacing:0.05em;font-weight:600"
                                            )
                                        groq_spin = ui.element("div").style(
                                            "display:flex;align-items:center;gap:6px"
                                        )
                                        groq_spin_ref[0] = groq_spin
                                        with groq_spin:
                                            ui.spinner(size="sm").style("color:#f57c00")
                                            ui.label("generando...").style(
                                                "font-size:11px;color:#f57c00"
                                            )
                                        groq_err = ui.label("").style(
                                            "font-size:11px;color:#e65100"
                                        )
                                        groq_err.set_visibility(False)
                                        groq_err_ref[0] = groq_err
                                        resp_groq = (
                                            ui.textarea(value="", placeholder="")
                                            .classes("w-full")
                                            .props("outlined rows=8")
                                            .style("font-size:12px")
                                        )
                                        resp_groq_holder[0] = resp_groq
                                        resp_area_groq_ref[0] = resp_groq
                                        _c_groq = context.client
                                        def _btn_enviar_groq_click():
                                            background_tasks.create(
                                                _enviar_respuesta(_c_groq, qid, resp_groq_holder[0]),
                                                name="enviar_grok",
                                            )
                                        ui.button(
                                            "Enviar esta respuesta",
                                            on_click=_btn_enviar_groq_click,
                                        ).props("unelevated dense no-caps").style(
                                            "background:#f57c00;color:#fff;font-size:12px"
                                        )

                                # ── COL 3 — GEMINI ───────────────────────────────────
                                with ui.element("div").style("flex:1;min-width:0"):
                                    with ui.element("div").style(
                                        "border:0.5px solid var(--color-border-tertiary);"
                                        "border-top:3px solid #1565c0;"
                                        "border-radius:var(--border-radius-md);padding:10px;"
                                        "background:var(--color-background-secondary);"
                                        "display:flex;flex-direction:column;gap:8px"
                                    ):
                                        with ui.element("div").style(
                                            "display:flex;align-items:center;gap:4px"
                                        ):
                                            ui.html(
                                                '<i class="ti ti-sparkles"'
                                                ' style="font-size:13px;color:#1565c0"></i>'
                                            )
                                            ui.label("Respuesta Gemini").style(
                                                "font-size:10px;color:#1565c0;"
                                                "letter-spacing:0.05em;font-weight:600"
                                            )
                                        gemini_spin = ui.element("div").style(
                                            "display:flex;align-items:center;gap:6px"
                                        )
                                        gemini_spin_ref[0] = gemini_spin
                                        with gemini_spin:
                                            ui.spinner(size="sm").style("color:#1565c0")
                                            ui.label("generando...").style(
                                                "font-size:11px;color:#1565c0"
                                            )
                                        gemini_err = ui.label("").style(
                                            "font-size:11px;color:#1565c0"
                                        )
                                        gemini_err.set_visibility(False)
                                        gemini_err_ref[0] = gemini_err
                                        resp_gemini = (
                                            ui.textarea(value="", placeholder="")
                                            .classes("w-full")
                                            .props("outlined rows=8")
                                            .style("font-size:12px")
                                        )
                                        resp_gemini_holder[0] = resp_gemini
                                        resp_area_gemini_ref[0] = resp_gemini
                                        _c_gemini = context.client
                                        def _btn_enviar_gemini_click():
                                            background_tasks.create(
                                                _enviar_respuesta(_c_gemini, qid, resp_gemini_holder[0]),
                                                name="enviar_gemini",
                                            )
                                        ui.button(
                                            "Enviar esta respuesta",
                                            on_click=_btn_enviar_gemini_click,
                                        ).props("unelevated dense no-caps").style(
                                            "background:#1565c0;color:#fff;font-size:12px"
                                        )

                        else:
                            # ── MOBILE: apilado ──────────────────────────────────────
                            with ui.column().classes("w-full gap-3"):

                                # ── Sección 1 — PREGUNTA ─────────────────────────────
                                with ui.element("div").style(
                                    "border:0.5px solid var(--color-border-tertiary);"
                                    "border-top:3px solid #1976d2;"
                                    "border-radius:var(--border-radius-md);padding:10px;"
                                    "background:var(--color-background-secondary);"
                                    "width:100%;box-sizing:border-box"
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
                                    _c_elim = context.client
                                    ui.button(
                                        "Eliminar pregunta",
                                        on_click=lambda c=_c_elim: background_tasks.create(
                                            _eliminar_pregunta(c), name="eliminar_pregunta"
                                        ),
                                    ).props("unelevated dense no-caps icon=delete").style(
                                        "background:#ef5350;color:#fff;font-size:11px;margin-top:6px"
                                    )

                                # ── Sección 2 — GROK ─────────────────────────────────
                                with ui.element("div").style(
                                    "border:0.5px solid var(--color-border-tertiary);"
                                    "border-top:3px solid #f57c00;"
                                    "border-radius:var(--border-radius-md);padding:10px;"
                                    "background:var(--color-background-secondary);"
                                    "width:100%;box-sizing:border-box;"
                                    "display:flex;flex-direction:column;gap:8px"
                                ):
                                    with ui.element("div").style(
                                        "display:flex;align-items:center;gap:4px"
                                    ):
                                        ui.html(
                                            '<i class="ti ti-robot"'
                                            ' style="font-size:13px;color:#e65100"></i>'
                                        )
                                        ui.label("Respuesta Grok").style(
                                            "font-size:10px;color:#e65100;"
                                            "letter-spacing:0.05em;font-weight:600"
                                        )
                                    groq_spin = ui.element("div").style(
                                        "display:flex;align-items:center;gap:6px"
                                    )
                                    groq_spin_ref[0] = groq_spin
                                    with groq_spin:
                                        ui.spinner(size="sm").style("color:#f57c00")
                                        ui.label("generando...").style(
                                            "font-size:11px;color:#f57c00"
                                        )
                                    groq_err = ui.label("").style(
                                        "font-size:11px;color:#e65100"
                                    )
                                    groq_err.set_visibility(False)
                                    groq_err_ref[0] = groq_err
                                    resp_groq = (
                                        ui.textarea(value="", placeholder="")
                                        .classes("w-full")
                                        .props("outlined rows=6")
                                        .style("font-size:12px")
                                    )
                                    resp_groq_holder[0] = resp_groq
                                    resp_area_groq_ref[0] = resp_groq
                                    _c_groq = context.client
                                    def _btn_enviar_groq_click():
                                        background_tasks.create(
                                            _enviar_respuesta(_c_groq, qid, resp_groq_holder[0]),
                                            name="enviar_grok",
                                        )
                                    ui.button(
                                        "Enviar esta respuesta",
                                        on_click=_btn_enviar_groq_click,
                                    ).props("unelevated dense no-caps").style(
                                        "background:#f57c00;color:#fff;font-size:12px"
                                    )

                                # ── Sección 3 — GEMINI ───────────────────────────────
                                with ui.element("div").style(
                                    "border:0.5px solid var(--color-border-tertiary);"
                                    "border-top:3px solid #1565c0;"
                                    "border-radius:var(--border-radius-md);padding:10px;"
                                    "background:var(--color-background-secondary);"
                                    "width:100%;box-sizing:border-box;"
                                    "display:flex;flex-direction:column;gap:8px"
                                ):
                                    with ui.element("div").style(
                                        "display:flex;align-items:center;gap:4px"
                                    ):
                                        ui.html(
                                            '<i class="ti ti-sparkles"'
                                            ' style="font-size:13px;color:#1565c0"></i>'
                                        )
                                        ui.label("Respuesta Gemini").style(
                                            "font-size:10px;color:#1565c0;"
                                            "letter-spacing:0.05em;font-weight:600"
                                        )
                                    gemini_spin = ui.element("div").style(
                                        "display:flex;align-items:center;gap:6px"
                                    )
                                    gemini_spin_ref[0] = gemini_spin
                                    with gemini_spin:
                                        ui.spinner(size="sm").style("color:#1565c0")
                                        ui.label("generando...").style(
                                            "font-size:11px;color:#1565c0"
                                        )
                                    gemini_err = ui.label("").style(
                                        "font-size:11px;color:#1565c0"
                                    )
                                    gemini_err.set_visibility(False)
                                    gemini_err_ref[0] = gemini_err
                                    resp_gemini = (
                                        ui.textarea(value="", placeholder="")
                                        .classes("w-full")
                                        .props("outlined rows=6")
                                        .style("font-size:12px")
                                    )
                                    resp_gemini_holder[0] = resp_gemini
                                    resp_area_gemini_ref[0] = resp_gemini
                                    _c_gemini = context.client
                                    def _btn_enviar_gemini_click():
                                        background_tasks.create(
                                            _enviar_respuesta(_c_gemini, qid, resp_gemini_holder[0]),
                                            name="enviar_gemini",
                                        )
                                    ui.button(
                                        "Enviar esta respuesta",
                                        on_click=_btn_enviar_gemini_click,
                                    ).props("unelevated dense no-caps").style(
                                        "background:#1565c0;color:#fff;font-size:12px"
                                    )

                                # ── Sección 4 — FRASES DE CIERRE ────────────────────
                                _build_frases_card(resp_groq_holder, resp_gemini_holder)

                    background_tasks.create(_load_ais(), name="load_ais")

        background_tasks.create(_cargar_async(), name="cargar_preguntas")
