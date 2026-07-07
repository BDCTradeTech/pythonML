"""
tabs/transferencias.py
Pestaña Transferencias: análisis de comprobantes de emisión de transferencias bancarias con IA.
"""
from __future__ import annotations

import json
import logging
import traceback
from typing import Any, Dict, List

import requests as _requests
from nicegui import app, background_tasks, context, run, ui

from db import get_app_config, get_connection

logger = logging.getLogger(__name__)

PROMPT_TRANSFERENCIA = """
Analizá este comprobante electrónico de emisión de transferencias bancarias y extraé los siguientes datos en formato JSON.
Si el dato no existe en el documento ponelo como null.

- banco: nombre del banco emisor del comprobante (ej: "BBVA", "Santander", "Galicia"). Solo el nombre, sin "Argentina S.A." ni texto adicional.
- fecha: fecha de la transacción en formato DD/MM/YYYY (campo "Fecha" dentro de "Datos de la transacción").
- operacion: número de Operación/Secuencia tal como aparece (ej: "3185875/01").
- importe: importe total tal como aparece en el documento, incluyendo moneda (ej: "USD 4.966,20").
- importe_moneda: moneda del importe (ej: "USD", "ARS").
- importe_valor: valor numérico del importe sin moneda ni separadores de miles, con punto decimal (ej: 4966.20).
- pagador_gastos_exterior: valor del campo "Pagador de gastos del exterior" (ej: "Ordenante", "Beneficiario").
- concepto: código y descripción del concepto (ej: "B06 - PAGO DIFERIDO IMPO BS").
- beneficiario: nombre del beneficiario o razón social, sección "Datos del beneficiario" (ej: "BDC TRADE TECH LLC").
- banco_beneficiario: nombre del banco beneficiario con número de cuenta si figura (ej: "CITIBANK, N.A. - Cta: 9117775582").
- tipo_cambio: tipo de cambio numérico de la sección "Cierre de Cambio", campo "Tipo de Cambio" (ej: 982.0). Solo el número.
- importe_pesos: importe equivalente en pesos de la sección "Cierre de Cambio", campo "Importe equivalente en Pesos" (ej: 4876808.40). Solo el número.
- estado: estado de la transferencia. Buscar texto como "Tu transferencia se liquidó satisfactoriamente" → devolver "Acreditada". Si no hay estado claro → null.
- fecha_liquidacion: fecha de liquidación si aparece (ej: "18/09/2024"). Buscar en frases como "se liquidó satisfactoriamente el día XX/XX/XXXX".
- facturas: array con TODAS las filas de la tabla "Facturas Aplicadas". Cada elemento tiene:
  - numero: número de factura como string (ej: "5523")
  - emision: fecha de emisión como aparece en el documento (ej: "09-07-2024")
  - moneda: moneda (ej: "USD")
  - monto: valor numérico del monto (ej: 982.80)

{
  "banco": null,
  "fecha": null,
  "operacion": null,
  "importe": null,
  "importe_moneda": null,
  "importe_valor": null,
  "pagador_gastos_exterior": null,
  "concepto": null,
  "beneficiario": null,
  "banco_beneficiario": null,
  "tipo_cambio": null,
  "importe_pesos": null,
  "estado": null,
  "fecha_liquidacion": null,
  "facturas": [
    {"numero": null, "emision": null, "moneda": null, "monto": null}
  ]
}

Respondé SOLO con el JSON, sin texto adicional ni backticks.
"""

_TABLE_HEADERS = [
    "IA", "Fecha", "Banco", "Operación", "Importe", "Pagador",
    "Beneficiario", "Banco Benef.", "T/C", "Equiv. $", "Estado", "Acciones",
]

_TABLE_COLS = (
    "10fr 14fr 12fr 16fr 16fr 14fr "
    "22fr 20fr 14fr 16fr 14fr 18fr"
)

_SORT_KEYS = {
    "Fecha":        lambda r: r.get("fecha") or "",
    "Banco":        lambda r: (r.get("banco") or "").lower(),
    "Operación":    lambda r: r.get("operacion") or "",
    "Importe":      lambda r: _to_float(r.get("importe_valor")) or 0,
    "Beneficiario": lambda r: (r.get("beneficiario") or "").lower(),
    "T/C":          lambda r: _to_float(r.get("tipo_cambio")) or 0,
    "Equiv. $":     lambda r: _to_float(r.get("importe_pesos")) or 0,
    "Estado":       lambda r: (r.get("estado") or "").lower(),
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(str(v).replace(",", ".").replace(" ", "").replace("$", "").strip())
    except (ValueError, TypeError):
        return None


def _fmt_num(v: Any, prefix: str = "", decimals: int = 0) -> str:
    if v is None:
        return "—"
    try:
        n = float(v)
        if decimals == 0:
            return prefix + f"{int(round(n)):,}".replace(",", ".")
        return prefix + f"{n:,.{decimals}f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except (ValueError, TypeError):
        return "—"


def _fmt_usd(v: Any) -> str:
    return _fmt_num(v, "u$s ", 2)


def _fmt_ars(v: Any) -> str:
    return _fmt_num(v, "$")


def _fmt_tc(v: Any) -> str:
    return _fmt_num(v, "", 2)


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
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id               INTEGER NOT NULL,
            banco                 TEXT,
            fecha                 TEXT,
            operacion             TEXT,
            importe               TEXT,
            importe_moneda        TEXT,
            importe_valor         TEXT,
            pagador_gastos_exterior TEXT,
            concepto              TEXT,
            beneficiario          TEXT,
            banco_beneficiario    TEXT,
            tipo_cambio           TEXT,
            importe_pesos         TEXT,
            estado                TEXT,
            fecha_liquidacion     TEXT,
            facturas              TEXT,
            ia_usada              TEXT,
            created_at            DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


_COLS = [
    "banco", "fecha", "operacion", "importe", "importe_moneda", "importe_valor",
    "pagador_gastos_exterior", "concepto", "beneficiario", "banco_beneficiario",
    "tipo_cambio", "importe_pesos", "estado", "fecha_liquidacion", "ia_usada",
]


def _save_transferencia(user_id: int, data: Dict[str, Any]) -> int:
    facturas_json = json.dumps(data.get("facturas") or [], ensure_ascii=False)
    vals = [str(data.get(c)) if data.get(c) is not None else None for c in _COLS]
    col_str = "user_id, facturas, " + ", ".join(_COLS)
    placeholders = ", ".join(["?"] * (len(_COLS) + 2))
    conn = get_connection()
    cur = conn.execute(
        f"INSERT INTO transferencias ({col_str}) VALUES ({placeholders})",
        [user_id, facturas_json] + vals,
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
        if filtros.get("banco") and filtros["banco"] != "Todos":
            where_parts.append("LOWER(banco) LIKE ?")
            params.append(f"%{filtros['banco'].lower()}%")
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
                "(LOWER(operacion) LIKE ? OR LOWER(beneficiario) LIKE ? OR LOWER(concepto) LIKE ?)"
            )
            params.extend([b, b, b])
    where_sql = " AND ".join(where_parts)
    conn = get_connection()
    rows = conn.execute(
        f"SELECT * FROM transferencias WHERE {where_sql} ORDER BY id DESC",
        params,
    ).fetchall()
    conn.close()
    result = []
    for r in rows:
        d = dict(r)
        try:
            d["facturas"] = json.loads(d.get("facturas") or "[]")
        except Exception:
            d["facturas"] = []
        result.append(d)
    return result


# ── AI helpers ────────────────────────────────────────────────────────────────

def _groq_parse_doc(api_key: str, texto: str) -> str:
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    full_prompt = PROMPT_TRANSFERENCIA + "\n\nCONTENIDO DEL DOCUMENTO:\n" + texto
    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [{"role": "user", "content": full_prompt}],
        "max_tokens": 2000,
        "temperature": 0.1,
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
    return "\n\n--- PÁGINA ---\n".join(parts)


# ── Estado badge ──────────────────────────────────────────────────────────────

def _estado_badge_html(estado: str) -> str:
    e = (estado or "").lower()
    if "acredit" in e or "liquid" in e or "complet" in e:
        bg, color = "#DCFCE7", "#166534"
    elif "pendiente" in e:
        bg, color = "#FEF3C7", "#92400E"
    elif "rechaz" in e or "error" in e:
        bg, color = "#FEE2E2", "#991B1B"
    else:
        bg, color = "#F3F4F6", "#374151"
    return (
        f'<span style="background:{bg};color:{color};border-radius:4px;'
        f'padding:1px 6px;font-size:9px;font-weight:500;white-space:nowrap;'
        f'max-width:90px;overflow:hidden;text-overflow:ellipsis;display:inline-block">'
        f'{estado}</span>'
    )


# ── Tabla ─────────────────────────────────────────────────────────────────────

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
            with ui.element("div").style("width:100%"):

                # ── Header ────────────────────────────────────────────────────
                _hs_base = (
                    "padding:4px 6px;background:#2A7AC7;"
                    "border-bottom:1px solid rgba(255,255,255,0.2);"
                    "border-right:0.5px solid rgba(255,255,255,0.15);"
                    "font-size:10px;font-weight:500;color:#FFFFFF;"
                    "white-space:normal;word-break:break-word;line-height:1.2;"
                    "height:44px;display:flex;align-items:center;justify-content:center;"
                    "text-align:center;position:sticky;top:0;z-index:10;overflow:hidden"
                )
                with ui.element("div").style(
                    f"display:grid;grid-template-columns:{_TABLE_COLS};"
                    "column-gap:2px;width:100%;align-items:center"
                ):
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
                                _hs_base + f";color:{_hc};cursor:pointer;user-select:none"
                            ).on("click", _sort_click):
                                ui.label(h + _arrow).style("pointer-events:none")
                        else:
                            ui.label(h).style(_hs_base)

                # ── Filas ─────────────────────────────────────────────────────
                _sep = "border-bottom:0.5px solid #f1f5f9"
                for r in rows:
                    rid = r["id"]
                    _row_bg = "background:#F0FDF4;" if rid in recien else ""
                    _ct = (
                        f"padding:3px 5px;font-size:10px;color:#374151;"
                        f"overflow:hidden;text-overflow:ellipsis;white-space:nowrap;{_sep};{_row_bg}"
                    )
                    det_id = f"tr-det-{rid}"
                    ico_id = f"tr-ico-{rid}"
                    facturas = r.get("facturas") or []

                    def _toggle_row(did=det_id, iid=ico_id):
                        ui.run_javascript(f"""
                            (function() {{
                                var det = document.querySelector('.{did}');
                                var icoEl = document.querySelector('.{iid}');
                                if (!det) return;
                                var isOpen = det.style.display !== 'none' && det.style.display !== '';
                                det.style.display = isOpen ? 'none' : 'block';
                                if (icoEl) {{
                                    var qIcon = icoEl.querySelector('.q-icon');
                                    if (qIcon) {{
                                        qIcon.style.transition = 'transform 0.2s';
                                        qIcon.style.transform = isOpen ? '' : 'rotate(90deg)';
                                    }}
                                }}
                            }})();
                        """)

                    # Fila principal
                    with ui.element("div").style(
                        f"display:grid;grid-template-columns:{_TABLE_COLS};"
                        "column-gap:2px;width:100%;align-items:center"
                    ):
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

                        ui.label(r.get("fecha") or "—").style(f"{_ct};text-align:center")
                        ui.label(r.get("banco") or "—").style(f"{_ct};font-weight:500")
                        ui.label(r.get("operacion") or "—").style(f"{_ct};text-align:center")

                        # Importe
                        _imp_val = _to_float(r.get("importe_valor"))
                        _imp_mon = r.get("importe_moneda") or ""
                        _imp_disp = f"{_imp_mon} {_fmt_num(_imp_val, decimals=2)}" if _imp_val else (r.get("importe") or "—")
                        ui.label(_imp_disp).style(f"{_ct};text-align:right;font-weight:500;color:#185FA5")

                        ui.label(r.get("pagador_gastos_exterior") or "—").style(f"{_ct};text-align:center")
                        ui.label(r.get("beneficiario") or "—").style(f"{_ct}")
                        ui.label(r.get("banco_beneficiario") or "—").style(f"{_ct}")
                        ui.label(_fmt_tc(r.get("tipo_cambio"))).style(f"{_ct};text-align:right")
                        ui.label(_fmt_ars(r.get("importe_pesos"))).style(f"{_ct};text-align:right")

                        # Estado badge
                        _estado = r.get("estado") or ""
                        with ui.element("div").style(
                            f"display:flex;justify-content:center;align-items:center;"
                            f"padding:3px 4px;overflow:hidden;{_sep};{_row_bg}"
                        ):
                            if _estado:
                                ui.html(_estado_badge_html(_estado))
                            else:
                                ui.label("—").style("font-size:10px;color:#9ca3af")

                        # Acciones
                        with ui.element("div").style(
                            f"display:flex;align-items:center;justify-content:center;"
                            f"gap:2px;padding:3px 4px;{_sep};{_row_bg}"
                        ):
                            # Chevron expand (solo si tiene facturas)
                            if facturas:
                                with ui.element("div").classes(ico_id).style("display:inline-flex"):
                                    ui.button(
                                        icon="chevron_right",
                                        on_click=_toggle_row,
                                    ).props("flat dense").style("color:#6b7280;min-width:20px")
                            else:
                                ui.element("div").style("width:20px")

                            def _ver(row=r):
                                _show_ver_dialog(row)
                            def _borrar(rid_=rid):
                                _confirm_delete(rid_, user_id, tabla_container, sort_state, filtros, recien)
                            ui.button(icon="visibility", on_click=_ver).props("flat dense").style(
                                "color:#1d4ed8;min-width:20px"
                            )
                            ui.button(icon="delete", on_click=_borrar).props("flat dense").style(
                                "color:#dc2626;min-width:20px"
                            )

                    # ── Fila detalle: Facturas Aplicadas (colapsable) ──────────
                    if facturas:
                        with ui.element("div").classes(det_id).style("display:none;width:100%"):
                            with ui.element("div").style(
                                "margin:0 0 4px 0;padding:8px 16px;"
                                "background:#F8FAFC;border-bottom:1px solid #E2E8F0"
                            ):
                                ui.label("Facturas Aplicadas").style(
                                    "font-size:10px;font-weight:600;color:#185FA5;"
                                    "margin-bottom:6px;display:block"
                                )
                                with ui.element("table").style(
                                    "border-collapse:collapse;font-size:10px;width:auto;min-width:340px"
                                ):
                                    # Header de la sub-tabla
                                    with ui.element("thead"):
                                        with ui.element("tr"):
                                            for hdr in ["Número", "Emisión", "Moneda", "Monto"]:
                                                ui.element("th").style(
                                                    "padding:3px 10px;background:#E6F1FB;"
                                                    "color:#0C447C;font-weight:600;"
                                                    "border:0.5px solid #C5D9F0;text-align:center"
                                                ).text = hdr
                                    with ui.element("tbody"):
                                        for fac in facturas:
                                            with ui.element("tr"):
                                                for val, align in [
                                                    (str(fac.get("numero") or "—"), "center"),
                                                    (str(fac.get("emision") or "—"), "center"),
                                                    (str(fac.get("moneda") or "—"), "center"),
                                                    (_fmt_usd(fac.get("monto")), "right"),
                                                ]:
                                                    ui.element("td").style(
                                                        f"padding:3px 10px;border:0.5px solid #E2E8F0;"
                                                        f"color:#374151;text-align:{align}"
                                                    ).text = val


def _confirm_delete(rid, user_id, tabla_container, sort_state, filtros, recien):
    with ui.dialog() as d_confirm, ui.card().style("padding:20px;min-width:300px"):
        ui.label("¿Eliminar esta transferencia?").style(
            "font-size:14px;font-weight:500;color:#374151;margin-bottom:16px;display:block"
        )
        with ui.row().classes("gap-2").style("justify-content:flex-end"):
            ui.button("Cancelar", on_click=d_confirm.close).props("flat")
            def _confirmar(d=d_confirm, r_=rid):
                _delete_transferencia(r_)
                d.close()
                ui.notify("Transferencia eliminada", color="positive")
                _rebuild_tabla(user_id, tabla_container, sort_state, filtros, recien)
            ui.button("Eliminar", on_click=_confirmar).style(
                "background:#dc2626;color:#FFFFFF;border-radius:4px"
            )
    d_confirm.open()


def _show_ver_dialog(row: Dict[str, Any]) -> None:
    _LABELS = [
        ("banco",                  "Banco"),
        ("fecha",                  "Fecha"),
        ("operacion",              "Operación"),
        ("importe",                "Importe"),
        ("pagador_gastos_exterior","Pagador gastos exterior"),
        ("concepto",               "Concepto"),
        ("beneficiario",           "Beneficiario"),
        ("banco_beneficiario",     "Banco Beneficiario"),
        ("tipo_cambio",            "Tipo de Cambio"),
        ("importe_pesos",          "Equiv. en Pesos"),
        ("estado",                 "Estado"),
        ("fecha_liquidacion",      "Fecha Liquidación"),
        ("ia_usada",               "IA utilizada"),
    ]
    facturas = row.get("facturas") or []
    with ui.dialog() as d, ui.card().style("padding:24px;min-width:480px;max-width:620px"):
        ui.label("Detalle de transferencia").style(
            "font-size:14px;font-weight:600;color:#374151;margin-bottom:16px;display:block"
        )
        for key, label in _LABELS:
            val = row.get(key)
            if not val:
                continue
            with ui.element("div").style(
                "display:flex;align-items:flex-start;gap:8px;padding:5px 0;"
                "border-bottom:0.5px solid #f1f5f9"
            ):
                ui.label(label).style("width:180px;font-size:12px;color:#6b7280;flex-shrink:0;padding-top:2px")
                if key == "estado":
                    ui.html(_estado_badge_html(str(val)))
                else:
                    ui.label(str(val)).style("font-size:12px;color:#374151;flex:1;word-break:break-word")

        if facturas:
            ui.label("Facturas Aplicadas").style(
                "font-size:12px;font-weight:600;color:#185FA5;margin-top:12px;margin-bottom:6px;display:block"
            )
            with ui.element("table").style("border-collapse:collapse;font-size:11px;width:100%"):
                with ui.element("thead"):
                    with ui.element("tr"):
                        for hdr in ["Número", "Emisión", "Moneda", "Monto"]:
                            ui.element("th").style(
                                "padding:4px 8px;background:#E6F1FB;color:#0C447C;"
                                "font-weight:600;border:0.5px solid #C5D9F0;text-align:center"
                            ).text = hdr
                with ui.element("tbody"):
                    for fac in facturas:
                        with ui.element("tr"):
                            for val, align in [
                                (str(fac.get("numero") or "—"), "center"),
                                (str(fac.get("emision") or "—"), "center"),
                                (str(fac.get("moneda") or "—"), "center"),
                                (_fmt_usd(fac.get("monto")), "right"),
                            ]:
                                ui.element("td").style(
                                    f"padding:4px 8px;border:0.5px solid #E2E8F0;"
                                    f"color:#374151;text-align:{align}"
                                ).text = val

        ui.button("Cerrar", on_click=d.close).props("flat").style("margin-top:16px;color:#374151")
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
    _filtros: dict = {"estado": "Todos", "banco": "Todos", "fecha": "Todas", "busqueda": ""}
    tabla_ref: list = [None]

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

    def _on_filter(key: str, val: str) -> None:
        _filtros[key] = val
        _rebuild_tabla(user_id, tabla_ref[0], sort_state, _filtros, recien)

    # ── Layout ────────────────────────────────────────────────────────────────
    with ui.element("div").style("margin:16px 20px 0"):
        # Panel de subida
        with ui.element("div").style(
            "border:1.5px solid #B0C4D8;border-radius:8px;overflow:hidden;"
            "background:var(--color-background-primary);"
            "box-shadow:0 1px 4px rgba(0,0,0,0.06);max-width:420px"
        ):
            with ui.element("div").style(
                "background:#EEF6FD;border-bottom:1px solid #D0E8F8;padding:7px 10px"
            ):
                ui.label("Subir comprobante").style(
                    "font-size:11px;font-weight:600;color:#185FA5"
                )
            with ui.element("div").style(
                "padding:8px 10px;display:flex;flex-direction:column;gap:5px"
            ):
                ui.label("Comprobante electrónico de emisión de transferencias (PDF)").style(
                    "font-size:9px;color:var(--color-text-tertiary);"
                    "background:var(--color-background-secondary);"
                    "border:0.5px solid var(--color-border-tertiary);"
                    "border-radius:3px;padding:1px 5px;align-self:flex-start"
                )
                _uploader = ui.upload(
                    label="Subir PDF",
                    on_upload=_on_upload,
                    auto_upload=True,
                    max_files=1,
                    max_file_size=20_000_000,
                ).props('accept=".pdf" flat bordered').style(
                    "width:100%;min-height:72px"
                )
                uploader_ref[0] = _uploader
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
            ui.label("Banco").style("font-size:11px;color:var(--color-text-secondary)")
            ui.select(
                options=["Todos", "BBVA", "Santander", "Galicia", "Macro", "ICBC"],
                value="Todos",
                on_change=lambda e: _on_filter("banco", e.value),
            ).props("dense outlined").style(
                "font-size:12px;height:34px;border-radius:4px;width:110px"
            )
        with ui.element("div").style("display:flex;flex-direction:column;gap:3px"):
            ui.label("Estado").style("font-size:11px;color:var(--color-text-secondary)")
            ui.select(
                options=["Todos", "Acreditada", "Pendiente", "Rechazada"],
                value="Todos",
                on_change=lambda e: _on_filter("estado", e.value),
            ).props("dense outlined").style(
                "font-size:12px;height:34px;border-radius:4px;width:110px"
            )
        with ui.element("div").style("display:flex;flex-direction:column;gap:3px"):
            ui.label("Fecha").style("font-size:11px;color:var(--color-text-secondary)")
            ui.select(
                options=["Todas", "Hoy", "Esta semana", "Este mes", "Este año"],
                value="Todas",
                on_change=lambda e: _on_filter("fecha", e.value),
            ).props("dense outlined").style(
                "font-size:12px;height:34px;border-radius:4px;width:110px"
            )
        with ui.element("div").style("display:flex;flex-direction:column;gap:3px;flex:1"):
            ui.label("Operación / Beneficiario / Concepto").style(
                "font-size:11px;color:var(--color-text-secondary)"
            )
            ui.input(
                placeholder="Buscar...",
                on_change=lambda e: _on_filter("busqueda", e.value or ""),
            ).props("dense outlined").style(
                "font-size:12px;height:34px;border-radius:4px;width:100%;min-width:200px"
            )
        with ui.element("button").on(
            "click",
            lambda: _rebuild_tabla(user_id, tabla_ref[0], sort_state, _filtros, recien),
        ).style(
            "height:34px;font-size:12px;font-weight:500;"
            "border:1px solid #2A7AC7;border-radius:4px;background:#2A7AC7;"
            "padding:0 14px;cursor:pointer;display:inline-flex;"
            "align-items:center;gap:6px;color:#FFFFFF;align-self:flex-end"
        ):
            ui.html('<i class="ti ti-refresh" style="font-size:14px"></i> Actualizar')

    # ── Tabla ─────────────────────────────────────────────────────────────────
    with ui.element("div").style("padding:16px 0 24px"):
        tabla_container = ui.element("div").style("width:100%")
        tabla_ref[0] = tabla_container
        _rebuild_tabla(user_id, tabla_container, sort_state, _filtros, recien)
