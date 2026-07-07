"""
tabs/transferencias.py
Pestaña Transferencias: análisis de transferencias bancarias con IA.
"""
from __future__ import annotations

import json
import logging
import traceback
from typing import Any, Dict, List, Optional

import requests as _requests
from nicegui import app, background_tasks, context, run, ui

from db import get_app_config, get_connection

logger = logging.getLogger(__name__)

PROMPT_TRANSFERENCIA = """
Analizá este documento de transferencia bancaria y extraé los siguientes datos en formato JSON.
Si el dato no existe en el documento ponelo como null.

- fecha: fecha de la transferencia (formato DD/MM/YYYY)
- beneficiario: nombre del beneficiario o destinatario
- banco_origen: banco emisor de la transferencia
- banco_destino: banco receptor
- monto_usd: monto en dólares (USD) — número sin símbolo
- monto_ars: monto en pesos argentinos (ARS) — número sin símbolo
- tipo_cambio: tipo de cambio utilizado si figura — número
- concepto: descripción o concepto de la transferencia
- referencia: número de referencia o comprobante
- nro_invoice: número de invoice relacionado si figura
- estado: estado de la transferencia (ej: acreditada, pendiente, rechazada)
- tipo: tipo de transferencia (ej: SWIFT, SEPA, local, MEP, CCL)
- observaciones: cualquier otra información relevante

{
  "fecha": null,
  "beneficiario": null,
  "banco_origen": null,
  "banco_destino": null,
  "monto_usd": null,
  "monto_ars": null,
  "tipo_cambio": null,
  "concepto": null,
  "referencia": null,
  "nro_invoice": null,
  "estado": null,
  "tipo": null,
  "observaciones": null
}

Respondé SOLO con el JSON, sin texto adicional ni backticks.
"""

_TABLE_HEADERS = [
    "IA", "Fecha", "Beneficiario", "Banco Origen", "Banco Destino",
    "Monto USD", "Monto ARS", "T/C", "Concepto", "Referencia",
    "Invoice", "Estado", "Tipo", "Acciones",
]

_TABLE_COLS = (
    "10fr 14fr 20fr 18fr 18fr "
    "14fr 16fr 12fr 22fr 16fr "
    "14fr 14fr 12fr 16fr"
)

_SORT_KEYS = {
    "Fecha":        lambda r: r.get("fecha") or "",
    "Beneficiario": lambda r: (r.get("beneficiario") or "").lower(),
    "Monto USD":    lambda r: _to_float(r.get("monto_usd")) or 0,
    "Monto ARS":    lambda r: _to_float(r.get("monto_ars")) or 0,
    "T/C":          lambda r: _to_float(r.get("tipo_cambio")) or 0,
    "Estado":       lambda r: (r.get("estado") or "").lower(),
    "Tipo":         lambda r: (r.get("tipo") or "").lower(),
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(str(v).replace(",", ".").replace(" ", "").replace("$", "").strip())
    except (ValueError, TypeError):
        return None


def _fmt_num(v: Any, prefix: str = "") -> str:
    if v is None:
        return "—"
    try:
        n = float(v)
        if abs(n - round(n)) < 0.001:
            return prefix + f"{int(round(n)):,}".replace(",", ".")
        return prefix + f"{n:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except (ValueError, TypeError):
        return "—"


def _fmt_usd(v: Any) -> str:
    return _fmt_num(v, "u$s ")


def _fmt_ars(v: Any) -> str:
    return _fmt_num(v, "$")


def _clean_json(raw: str) -> str:
    raw = raw.strip()
    if raw.startswith("```"):
        lines = raw.split("\n")
        raw = "\n".join(lines[1:])
        if raw.endswith("```"):
            raw = raw[:-3]
    return raw.strip()


# ── DB ────────────────────────────────────────────────────────────────────────

def _init_transferencias_db() -> None:
    conn = get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS transferencias (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id      INTEGER NOT NULL,
            fecha        TEXT,
            beneficiario TEXT,
            banco_origen TEXT,
            banco_destino TEXT,
            monto_usd    TEXT,
            monto_ars    TEXT,
            tipo_cambio  TEXT,
            concepto     TEXT,
            referencia   TEXT,
            nro_invoice  TEXT,
            estado       TEXT,
            tipo         TEXT,
            observaciones TEXT,
            ia_usada     TEXT,
            created_at   DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


def _save_transferencia(user_id: int, data: Dict[str, Any]) -> int:
    cols = [
        "fecha", "beneficiario", "banco_origen", "banco_destino",
        "monto_usd", "monto_ars", "tipo_cambio", "concepto",
        "referencia", "nro_invoice", "estado", "tipo", "observaciones", "ia_usada",
    ]
    vals = [str(data.get(c)) if data.get(c) is not None else None for c in cols]
    col_str = "user_id, " + ", ".join(cols)
    placeholders = ", ".join(["?"] * (len(cols) + 1))
    conn = get_connection()
    cur = conn.execute(
        f"INSERT INTO transferencias ({col_str}) VALUES ({placeholders})",
        [user_id] + vals,
    )
    row_id = cur.lastrowid
    conn.commit()
    conn.close()
    return row_id


def _delete_transferencia(rid: int) -> None:
    conn = get_connection()
    conn.execute("DELETE FROM transferencias WHERE id = ?", [rid])
    conn.commit()
    conn.close()


def _list_transferencias(user_id: int, filtros: dict | None = None) -> List[Dict[str, Any]]:
    where_parts = ["user_id = ?"]
    params: list = [user_id]
    if filtros:
        if filtros.get("estado") and filtros["estado"] != "Todos":
            where_parts.append("LOWER(estado) LIKE ?")
            params.append(f"%{filtros['estado'].lower()}%")
        fecha_f = filtros.get("fecha", "Todas")
        if fecha_f == "Hoy":
            where_parts.append("fecha = strftime('%d/%m/%Y', 'now', 'localtime')")
        elif fecha_f == "Esta semana":
            where_parts.append(
                "SUBSTR(fecha,7,4)||'-'||SUBSTR(fecha,4,2)||'-'||SUBSTR(fecha,1,2) >= DATE('now','localtime','-6 days')"
            )
        elif fecha_f == "Este mes":
            where_parts.append(
                "SUBSTR(fecha,4,2) = strftime('%m','now','localtime') "
                "AND SUBSTR(fecha,7,4) = strftime('%Y','now','localtime')"
            )
        elif fecha_f == "Este año":
            where_parts.append("SUBSTR(fecha,7,4) = strftime('%Y','now','localtime')")
        busqueda = (filtros.get("busqueda") or "").strip()
        if busqueda:
            b = f"%{busqueda.lower()}%"
            where_parts.append(
                "(LOWER(referencia) LIKE ? OR LOWER(beneficiario) LIKE ? OR LOWER(concepto) LIKE ? OR LOWER(nro_invoice) LIKE ?)"
            )
            params.extend([b, b, b, b])
    where_sql = " AND ".join(where_parts)
    conn = get_connection()
    rows = conn.execute(
        f"SELECT * FROM transferencias WHERE {where_sql} ORDER BY id DESC",
        params,
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── AI helpers ────────────────────────────────────────────────────────────────

def _groq_parse_doc(api_key: str, texto: str) -> str:
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    full_prompt = PROMPT_TRANSFERENCIA + "\n\nCONTENIDO DEL DOCUMENTO:\n" + texto
    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [{"role": "user", "content": full_prompt}],
        "max_tokens": 1500,
        "temperature": 0.2,
    }
    resp = _requests.post(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]


def _gemini_parse_doc(api_key: str, data: bytes, mime_type: str) -> str:
    from google import genai
    from google.genai import types
    client = genai.Client(api_key=api_key)
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=[
            types.Part.from_bytes(data=data, mime_type=mime_type),
            PROMPT_TRANSFERENCIA,
        ],
    )
    return response.text


def _extract_pdf_text(data: bytes) -> str:
    import pdfplumber
    import io as _io
    parts = []
    with pdfplumber.open(_io.BytesIO(data)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            parts.append(text)
    return "\n\n".join(parts)


# ── UI — tabla ────────────────────────────────────────────────────────────────

def _rebuild_tabla(
    user_id: int,
    tabla_container,
    sort_state: list,
    filtros: dict | None = None,
    recien: set | None = None,
) -> None:
    recien = recien or set()
    tabla_container.clear()
    rows = _list_transferencias(user_id, filtros)
    sort_col, sort_dir = sort_state
    if sort_col and sort_col in _SORT_KEYS:
        rows.sort(key=_SORT_KEYS[sort_col], reverse=(sort_dir == "desc"))

    with tabla_container:
        if not rows:
            ui.label("No hay transferencias guardadas.").style(
                "font-size:13px;color:#9ca3af;font-style:italic;padding:12px 20px"
            )
            return

        with ui.element("div").style(
            "overflow-x:hidden;overflow-y:auto;max-height:calc(100vh - 300px);width:100%"
        ):
            with ui.element("div").style(
                f"display:grid;grid-template-columns:{_TABLE_COLS};"
                "column-gap:2px;width:100%;align-items:center"
            ):
                # ── Header ────────────────────────────────────────────────────
                _hs = (
                    "padding:4px 6px;background:#2A7AC7;"
                    "border-bottom:1px solid rgba(255,255,255,0.2);"
                    "border-right:0.5px solid rgba(255,255,255,0.15);"
                    "font-size:10px;font-weight:500;color:#FFFFFF;"
                    "white-space:normal;word-break:break-word;line-height:1.2;"
                    "height:44px;display:flex;align-items:center;justify-content:center;"
                    "text-align:center;position:sticky;top:0;z-index:10;overflow:hidden"
                )
                for h in _TABLE_HEADERS:
                    if h in _SORT_KEYS:
                        _active = sort_state[0] == h
                        _arrow = (" ↑" if sort_state[1] == "asc" else " ↓") if _active else ""
                        _hc = "#FFFFFF" if _active else "rgba(255,255,255,0.85)"
                        def _sort_click(col=h):
                            sort_state[1] = "desc" if sort_state[0] == col and sort_state[1] == "asc" else "asc"
                            sort_state[0] = col
                            _rebuild_tabla(user_id, tabla_container, sort_state, filtros, recien)
                        with ui.element("div").style(
                            _hs + f";color:{_hc};cursor:pointer;user-select:none"
                        ).on("click", _sort_click):
                            ui.label(h + _arrow).style("pointer-events:none")
                    else:
                        ui.label(h).style(_hs)

                # ── Filas ─────────────────────────────────────────────────────
                _sep = "border-bottom:0.5px solid #f1f5f9"
                for r in rows:
                    rid = r["id"]
                    _row_bg = "background:#F0FDF4;" if rid in recien else ""
                    _ct = (
                        f"padding:3px 5px;font-size:10px;color:#374151;"
                        f"overflow:hidden;text-overflow:ellipsis;white-space:nowrap;{_sep};{_row_bg}"
                    )

                    # IA badge
                    _ia = r.get("ia_usada") or ""
                    with ui.element("div").style(
                        f"display:flex;justify-content:center;align-items:center;"
                        f"padding:3px 4px;overflow:hidden;{_sep};{_row_bg}"
                    ):
                        if _ia == "Grok":
                            ui.html(
                                '<span style="display:inline-flex;align-items:center;gap:3px;'
                                'background:#E6F1FB;border:0.5px solid #85B7EB;color:#0C447C;'
                                'border-radius:4px;padding:1px 5px;font-size:9px;font-weight:500;white-space:nowrap">'
                                '<i class="ti ti-bolt"></i> Grok</span>'
                            )
                        elif _ia == "Gemini":
                            ui.html(
                                '<span style="display:inline-flex;align-items:center;gap:3px;'
                                'background:#EAF3DE;border:0.5px solid #3B6D11;color:#173404;'
                                'border-radius:4px;padding:1px 5px;font-size:9px;font-weight:500;white-space:nowrap">'
                                '<i class="ti ti-sparkles"></i> Gemini</span>'
                            )
                        else:
                            ui.label("—").style("font-size:10px;color:#9ca3af")

                    ui.label(r.get("fecha") or "—").style(f"{_ct};white-space:nowrap;text-align:center")
                    ui.label(r.get("beneficiario") or "—").style(f"{_ct}")
                    ui.label(r.get("banco_origen") or "—").style(f"{_ct}")
                    ui.label(r.get("banco_destino") or "—").style(f"{_ct}")
                    ui.label(_fmt_usd(r.get("monto_usd"))).style(f"{_ct};text-align:right")
                    ui.label(_fmt_ars(r.get("monto_ars"))).style(f"{_ct};text-align:right")
                    ui.label(_fmt_num(r.get("tipo_cambio"))).style(f"{_ct};text-align:right")
                    ui.label(r.get("concepto") or "—").style(f"{_ct}")
                    ui.label(r.get("referencia") or "—").style(f"{_ct};text-align:center")
                    ui.label(r.get("nro_invoice") or "—").style(f"{_ct};text-align:center")

                    # Estado badge
                    _estado = r.get("estado") or ""
                    _estado_color = (
                        "#166534" if "acredit" in _estado.lower() or "complet" in _estado.lower()
                        else "#92400E" if "pendiente" in _estado.lower()
                        else "#991B1B" if "rechaz" in _estado.lower()
                        else "#374151"
                    )
                    _estado_bg = (
                        "#DCFCE7" if "acredit" in _estado.lower() or "complet" in _estado.lower()
                        else "#FEF3C7" if "pendiente" in _estado.lower()
                        else "#FEE2E2" if "rechaz" in _estado.lower()
                        else "#F9FAFB"
                    )
                    with ui.element("div").style(
                        f"display:flex;justify-content:center;align-items:center;"
                        f"padding:3px 4px;overflow:hidden;{_sep};{_row_bg}"
                    ):
                        if _estado:
                            ui.html(
                                f'<span style="background:{_estado_bg};color:{_estado_color};'
                                f'border-radius:4px;padding:1px 6px;font-size:9px;font-weight:500;'
                                f'white-space:nowrap;max-width:80px;overflow:hidden;text-overflow:ellipsis">'
                                f'{_estado}</span>'
                            )
                        else:
                            ui.label("—").style("font-size:10px;color:#9ca3af")

                    ui.label(r.get("tipo") or "—").style(f"{_ct};text-align:center")

                    # Acciones
                    with ui.element("div").style(
                        f"display:flex;align-items:center;justify-content:center;"
                        f"gap:4px;padding:3px 4px;{_sep};{_row_bg}"
                    ):
                        def _ver(row=r):
                            _show_ver_dialog(row)
                        def _borrar(rid_=rid):
                            with ui.dialog() as d_confirm, ui.card().style("padding:20px;min-width:300px"):
                                ui.label("¿Eliminar esta transferencia?").style(
                                    "font-size:14px;font-weight:500;color:#374151;margin-bottom:16px;display:block"
                                )
                                with ui.row().classes("gap-2").style("justify-content:flex-end"):
                                    ui.button("Cancelar", on_click=d_confirm.close).props("flat")
                                    def _confirmar(d=d_confirm, r_=rid_):
                                        _delete_transferencia(r_)
                                        d.close()
                                        ui.notify("Transferencia eliminada", color="positive")
                                        _rebuild_tabla(user_id, tabla_container, sort_state, filtros, recien)
                                    ui.button("Eliminar", on_click=_confirmar).style(
                                        "background:#dc2626;color:#FFFFFF;border-radius:4px"
                                    )
                            d_confirm.open()
                        ui.button(icon="visibility", on_click=_ver).props("flat dense").style(
                            "color:#1d4ed8;min-width:20px"
                        )
                        ui.button(icon="delete", on_click=_borrar).props("flat dense").style(
                            "color:#dc2626;min-width:20px"
                        )


def _show_ver_dialog(row: Dict[str, Any]) -> None:
    _LABELS = {
        "fecha": "Fecha",
        "beneficiario": "Beneficiario",
        "banco_origen": "Banco Origen",
        "banco_destino": "Banco Destino",
        "monto_usd": "Monto USD",
        "monto_ars": "Monto ARS",
        "tipo_cambio": "Tipo de Cambio",
        "concepto": "Concepto",
        "referencia": "Referencia",
        "nro_invoice": "Nro. Invoice",
        "estado": "Estado",
        "tipo": "Tipo",
        "observaciones": "Observaciones",
        "ia_usada": "IA utilizada",
    }
    with ui.dialog() as d, ui.card().style("padding:24px;min-width:440px;max-width:580px"):
        ui.label("Detalle de transferencia").style(
            "font-size:14px;font-weight:600;color:#374151;margin-bottom:16px;display:block"
        )
        for key, label in _LABELS.items():
            val = row.get(key)
            if not val:
                continue
            with ui.element("div").style(
                "display:flex;align-items:flex-start;gap:8px;padding:5px 0;"
                "border-bottom:0.5px solid #f1f5f9"
            ):
                ui.label(label).style("width:140px;font-size:12px;color:#6b7280;flex-shrink:0;padding-top:2px")
                ui.label(str(val)).style("font-size:12px;color:#374151;flex:1;word-break:break-word")
        ui.button("Cerrar", on_click=d.close).props("flat").style("margin-top:12px;color:#374151")
    d.open()


# ── Build principal ───────────────────────────────────────────────────────────

def build_tab_transferencias() -> None:
    user = app.storage.user.get("user")
    if not user:
        ui.label("Debes iniciar sesión").classes("text-red-500 p-4")
        return

    user_id = user["id"]
    _init_transferencias_db()

    sort_state: list = [None, "asc"]
    recien: set = set()
    _filtros: dict = {"estado": "Todos", "fecha": "Este mes", "busqueda": ""}

    tabla_ref: list = [None]

    # ── Panel de subida ───────────────────────────────────────────────────────
    archivo_data: list = [None]
    archivo_mime: list = [None]
    uploader_ref: list = [None]
    spin_ref: list = [None]
    resultado_ref: list = [None]

    client = context.client

    def _on_upload(e):
        try:
            e.content.seek(0)
            archivo_data[0] = e.content.read()
            archivo_mime[0] = e.type or "application/pdf"
        except Exception as ex:
            logger.error("_on_upload transferencias: %s", ex)

    async def _analizar(usar_gemini: bool) -> None:
        if not archivo_data[0]:
            client.run_javascript(
                "Quasar.Notify.create({message:'Primero subí un archivo',"
                "color:'warning',position:'bottom'})"
            )
            return
        groq_key = get_app_config("groq_api_key")
        gemini_key = get_app_config("gemini_api_key")
        if usar_gemini and not gemini_key:
            client.run_javascript(
                "Quasar.Notify.create({message:'Configurá tu API key de Gemini en Config → IA/Sugerencias',"
                "color:'warning',position:'bottom'})"
            )
            return
        if not usar_gemini and not groq_key:
            client.run_javascript(
                "Quasar.Notify.create({message:'Configurá tu API key de Grok en Config → IA/Sugerencias',"
                "color:'warning',position:'bottom'})"
            )
            return

        spin_ref[0].set_visibility(True)
        resultado_ref[0].set_text("")
        try:
            if usar_gemini:
                raw = await run.io_bound(
                    _gemini_parse_doc, gemini_key, archivo_data[0], archivo_mime[0]
                )
            else:
                texto = await run.io_bound(_extract_pdf_text, archivo_data[0])
                if not texto.strip():
                    client.run_javascript(
                        "Quasar.Notify.create({message:'No se pudo extraer texto del PDF. Probá con Gemini.',"
                        "color:'warning',position:'bottom'})"
                    )
                    return
                raw = await run.io_bound(_groq_parse_doc, groq_key, texto)

            raw = _clean_json(raw)
            parsed = json.loads(raw)
            parsed["ia_usada"] = "Gemini" if usar_gemini else "Grok"
            _id = _save_transferencia(user_id, parsed)
            recien.add(_id)
            _rebuild_tabla(user_id, tabla_ref[0], sort_state, _filtros, recien)
            client.run_javascript(
                "Quasar.Notify.create({message:'Transferencia guardada',"
                "color:'positive',position:'bottom'})"
            )
            archivo_data[0] = None
            archivo_mime[0] = None
            uploader_ref[0].reset()
        except json.JSONDecodeError:
            resultado_ref[0].set_text("Error: JSON inválido devuelto por IA")
        except Exception as exc:
            tb = traceback.format_exc()
            logger.error("Error analizando transferencia: %s\n%s", exc, tb)
            _msg = json.dumps(f"Error: {exc}")
            client.run_javascript(
                f"Quasar.Notify.create({{message:{_msg},color:'negative',position:'bottom'}})"
            )
        finally:
            spin_ref[0].set_visibility(False)

    def _click_grok():
        background_tasks.create(_analizar(False), name="analizar_transferencia_grok")

    def _click_gemini():
        background_tasks.create(_analizar(True), name="analizar_transferencia_gemini")

    # ── Layout ────────────────────────────────────────────────────────────────
    with ui.element("div").style("margin:16px 20px 0"):
        # Panel de subida
        with ui.element("div").style(
            "border:1.5px solid #B0C4D8;border-radius:8px;overflow:hidden;"
            "background:var(--color-background-primary);"
            "box-shadow:0 1px 4px rgba(0,0,0,0.06);max-width:420px"
        ):
            # Header
            with ui.element("div").style(
                "background:#EEF6FD;border-bottom:1px solid #D0E8F8;padding:7px 10px"
            ):
                ui.label("Subir documento").style(
                    "font-size:11px;font-weight:600;color:#185FA5"
                )
            # Body
            with ui.element("div").style(
                "padding:8px 10px;display:flex;flex-direction:column;gap:5px"
            ):
                ui.label("PDF o imagen de la transferencia").style(
                    "font-size:9px;color:var(--color-text-tertiary);"
                    "background:var(--color-background-secondary);"
                    "border:0.5px solid var(--color-border-tertiary);"
                    "border-radius:3px;padding:1px 5px;align-self:flex-start"
                )
                _uploader = ui.upload(
                    label="Subir PDF/IMG",
                    on_upload=_on_upload,
                    auto_upload=True,
                    max_files=1,
                    max_file_size=20_000_000,
                ).props('accept=".pdf,.jpg,.jpeg,.png" flat bordered').style(
                    "width:100%;min-height:72px"
                )
                uploader_ref[0] = _uploader
            # Footer con botones IA
            with ui.element("div").style(
                "background:var(--color-background-secondary);"
                "border-top:0.5px solid var(--color-border-tertiary);"
                "padding:6px 10px;display:flex;align-items:center;gap:6px"
            ):
                ui.button("Grok", icon="bolt", on_click=_click_grok).props("flat dense").style(
                    "height:34px;border:1px solid #85B7EB;color:#185FA5;background:#EEF6FD;"
                    "font-size:11px;padding:0 10px;border-radius:4px;"
                    "display:flex;align-items:center;gap:4px"
                )
                ui.button("Gemini", icon="auto_awesome", on_click=_click_gemini).props("flat dense").style(
                    "height:34px;border:1px solid #85B7EB;color:#185FA5;background:#EEF6FD;"
                    "font-size:11px;padding:0 10px;border-radius:4px;"
                    "display:flex;align-items:center;gap:4px"
                )
                spin = ui.spinner(size="sm").classes("text-blue-500")
                spin.set_visibility(False)
                spin_ref[0] = spin
            resultado_txt = ui.label("").style(
                "font-size:11px;color:#dc2626;font-weight:500;text-align:center;padding:2px 8px 4px"
            )
            resultado_ref[0] = resultado_txt

    # ── Barra de filtros ──────────────────────────────────────────────────────
    with ui.element("div").style(
        "padding:12px 20px 0;display:flex;gap:8px;align-items:flex-end;flex-wrap:nowrap"
    ):
        with ui.element("div").style("display:flex;flex-direction:column;gap:3px"):
            ui.label("Estado").style("font-size:11px;color:var(--color-text-secondary)")
            ui.select(
                options=["Todos", "Acreditada", "Pendiente", "Rechazada"],
                value="Todos",
                on_change=lambda e: _on_filter("estado", e.value),
            ).props("dense outlined").style(
                "font-size:12px;height:34px;border-radius:4px;width:115px"
            )
        with ui.element("div").style("display:flex;flex-direction:column;gap:3px"):
            ui.label("Fecha").style("font-size:11px;color:var(--color-text-secondary)")
            ui.select(
                options=["Todas", "Hoy", "Esta semana", "Este mes", "Este año"],
                value="Este mes",
                on_change=lambda e: _on_filter("fecha", e.value),
            ).props("dense outlined").style(
                "font-size:12px;height:34px;border-radius:4px;width:110px"
            )
        with ui.element("div").style("display:flex;flex-direction:column;gap:3px;flex:1"):
            ui.label("Referencia / Beneficiario / Concepto").style(
                "font-size:11px;color:var(--color-text-secondary)"
            )
            ui.input(
                placeholder="Buscar...",
                on_change=lambda e: _on_filter("busqueda", e.value or ""),
            ).props("dense outlined").style(
                "font-size:12px;height:34px;border-radius:4px;width:100%;min-width:220px"
            )
        with ui.element("button").on(
            "click",
            lambda: _rebuild_tabla(user_id, tabla_ref[0], sort_state, _filtros, recien),
        ).style(
            "height:34px;font-size:12px;font-weight:500;"
            "border:1px solid #2A7AC7;border-radius:4px;background:#2A7AC7;"
            "padding:0 14px;cursor:pointer;display:inline-flex;"
            "align-items:center;gap:6px;color:#FFFFFF"
        ):
            ui.html('<i class="ti ti-refresh" style="font-size:14px"></i> Actualizar')

    # ── Tabla ─────────────────────────────────────────────────────────────────
    with ui.element("div").style("padding:16px 0 24px"):
        tabla_container = ui.element("div").style("width:100%")
        tabla_ref[0] = tabla_container
        _rebuild_tabla(user_id, tabla_container, sort_state, _filtros, recien)

    def _on_filter(key: str, val: str) -> None:
        _filtros[key] = val
        _rebuild_tabla(user_id, tabla_ref[0], sort_state, _filtros, recien)
