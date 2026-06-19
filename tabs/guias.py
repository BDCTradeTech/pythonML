"""
tabs/guias.py
Pestaña Guías: análisis de documentos de importación con IA.
"""
from __future__ import annotations

import io
import json
import logging
from typing import Any, Dict, List

import requests as _requests
from nicegui import app, run, ui

from db import get_app_config, get_connection

logger = logging.getLogger(__name__)

PROMPT_GUIA = """
Analizá este documento de importación y extraé los siguientes datos en formato JSON.
Si el dato no existe en el documento ponelo como null.

INSTRUCCIONES ESPECÍFICAS:
- razon_social: nombre o razón social del proveedor o despachante que emite el documento.
- Para flete_aereo, entrega_domicilio, resolucion_3244, seguro_internacional, almacenaje y servicios_honorarios: tomar el valor de la ÚLTIMA columna numérica del documento, que representa el importe en pesos argentinos ($). IGNORAR la primera columna que está en dólares (USD o u$s).
- En el recuadro o tabla separada ubicada en la parte INFERIOR IZQUIERDA del documento, buscar en este orden de arriba hacia abajo:
  1. Derechos de Importación → derechos_importacion
  2. Tasa Estadística → tasa_estadistica
  3. IVA Aduanero → iva_aduanero
- Para tipo_cambio: buscar un valor con formato X/Y/Z y separar en 3 campos individuales (tipo_cambio_1, tipo_cambio_2, tipo_cambio_3).
- Para kgs: buscar el peso total en kilogramos.

{
  "razon_social": null,
  "nro_invoice": null,
  "nro_factura": null,
  "fecha": null,
  "productos": [
    {"descripcion": "", "cantidad": null, "precio_unitario": null, "precio_total": null}
  ],
  "kgs": null,
  "tipo_cambio_1": null,
  "tipo_cambio_2": null,
  "tipo_cambio_3": null,
  "flete_aereo": null,
  "entrega_domicilio": null,
  "resolucion_3244": null,
  "seguro_internacional": null,
  "almacenaje": null,
  "servicios_honorarios": null,
  "iva_aduanero": null,
  "derechos_importacion": null,
  "tasa_estadistica": null
}

Respondé SOLO con el JSON, sin texto adicional ni backticks.
"""

_LABELS = {
    "razon_social": "Razón social",
    "nro_invoice": "Nro. Invoice",
    "nro_factura": "Nro. Factura",
    "fecha": "Fecha",
    "kgs": "Kgs",
    "tipo_cambio_1": "Tipo de cambio 1",
    "tipo_cambio_2": "Tipo de cambio 2",
    "tipo_cambio_3": "Tipo de cambio 3",
    "flete_aereo": "Flete aéreo",
    "entrega_domicilio": "Entrega a domicilio",
    "resolucion_3244": "Resolución 3244",
    "seguro_internacional": "Seguro internacional",
    "almacenaje": "Almacenaje",
    "servicios_honorarios": "Servicios / Honorarios",
    "iva_aduanero": "IVA aduanero",
    "derechos_importacion": "Derechos de importación",
    "tasa_estadistica": "Tasa estadística",
}

_SCALAR_COLS = [
    "razon_social", "nro_invoice", "nro_factura", "fecha",
    "kgs", "tipo_cambio_1", "tipo_cambio_2", "tipo_cambio_3",
    "flete_aereo", "entrega_domicilio", "resolucion_3244",
    "seguro_internacional", "almacenaje", "servicios_honorarios",
    "iva_aduanero", "derechos_importacion", "tasa_estadistica",
]


# ── DB helpers ────────────────────────────────────────────────────────────────

def _init_guias_db() -> None:
    conn = get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS guias_importacion (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id              INTEGER NOT NULL,
            razon_social         TEXT,
            nro_invoice          TEXT,
            nro_factura          TEXT,
            fecha                TEXT,
            productos            TEXT,
            kgs                  TEXT,
            tipo_cambio_1        TEXT,
            tipo_cambio_2        TEXT,
            tipo_cambio_3        TEXT,
            flete_aereo          TEXT,
            entrega_domicilio    TEXT,
            resolucion_3244      TEXT,
            seguro_internacional TEXT,
            almacenaje           TEXT,
            servicios_honorarios TEXT,
            iva_aduanero         TEXT,
            derechos_importacion TEXT,
            tasa_estadistica     TEXT,
            created_at           DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


def _save_guia(user_id: int, data: Dict[str, Any]) -> int:
    productos_json = json.dumps(data.get("productos") or [], ensure_ascii=False)
    vals = [str(data.get(c)) if data.get(c) is not None else None for c in _SCALAR_COLS]
    col_str = "user_id, productos, " + ", ".join(_SCALAR_COLS)
    placeholders = ", ".join(["?"] * (len(_SCALAR_COLS) + 2))
    conn = get_connection()
    cur = conn.execute(
        f"INSERT INTO guias_importacion ({col_str}) VALUES ({placeholders})",
        [user_id, productos_json] + vals,
    )
    row_id = cur.lastrowid
    conn.commit()
    conn.close()
    return row_id


def _list_guias(user_id: int) -> List[Dict[str, Any]]:
    conn = get_connection()
    rows = conn.execute(
        "SELECT id, razon_social, nro_factura, nro_invoice, fecha, created_at "
        "FROM guias_importacion WHERE user_id = ? ORDER BY created_at DESC",
        (user_id,),
    ).fetchall()
    conn.close()
    return [
        {
            "id": r["id"],
            "razon_social": r["razon_social"] or "",
            "nro_factura": r["nro_factura"] or "",
            "nro_invoice": r["nro_invoice"] or "",
            "fecha": r["fecha"] or "",
            "created_at": (r["created_at"] or "")[:16],
        }
        for r in rows
    ]


def _get_guia(guia_id: int, user_id: int) -> Dict[str, Any] | None:
    conn = get_connection()
    cur = conn.execute(
        "SELECT * FROM guias_importacion WHERE id = ? AND user_id = ?",
        (guia_id, user_id),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    data = dict(row)
    try:
        data["productos"] = json.loads(data.get("productos") or "[]")
    except Exception:
        data["productos"] = []
    return data


def _delete_guia(guia_id: int, user_id: int) -> None:
    conn = get_connection()
    conn.execute(
        "DELETE FROM guias_importacion WHERE id = ? AND user_id = ?",
        (guia_id, user_id),
    )
    conn.commit()
    conn.close()


# ── AI helpers ────────────────────────────────────────────────────────────────

def _groq_parse_doc(api_key: str, prompt: str) -> str:
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 2000,
        "temperature": 0.2,
    }
    resp = _requests.post(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]


def _gemini_vision(api_key: str, data: bytes, mime_type: str) -> str:
    from google import genai
    from google.genai import types
    client = genai.Client(api_key=api_key)
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=[
            types.Part.from_bytes(data=data, mime_type=mime_type),
            PROMPT_GUIA,
        ],
    )
    return response.text


def _extract_pdf_text(data: bytes) -> str:
    import pdfplumber
    parts = []
    with pdfplumber.open(io.BytesIO(data)) as pdf:
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                parts.append(t)
    return "\n".join(parts)


def _clean_json(raw: str) -> str:
    raw = raw.strip()
    if raw.startswith("```"):
        lines = raw.split("\n")
        raw = "\n".join(lines[1:])
        if raw.endswith("```"):
            raw = raw[:-3]
    return raw.strip()


# ── UI helpers ────────────────────────────────────────────────────────────────

def _render_campos(data: Dict[str, Any]) -> None:
    for key, label in _LABELS.items():
        val = data.get(key)
        val_str = "" if val is None else str(val)
        with ui.element("div").style(
            "display:flex;align-items:center;gap:8px;padding:5px 0;"
            "border-bottom:0.5px solid #f1f5f9"
        ):
            ui.label(label).style("width:200px;font-size:13px;color:#6b7280;flex-shrink:0")
            ui.input(value=val_str).props("dense outlined").style("flex:1;font-size:13px")

    productos = data.get("productos") or []
    if productos:
        ui.label("Productos").style(
            "font-weight:600;font-size:13px;color:#374151;"
            "margin-top:14px;margin-bottom:6px;display:block"
        )
        for i, prod in enumerate(productos):
            with ui.element("div").style(
                "background:#f8fafc;border:0.5px solid #e2e8f0;"
                "border-radius:6px;padding:10px;margin-bottom:8px"
            ):
                ui.label(f"Producto {i + 1}").style(
                    "font-size:11px;color:#9ca3af;margin-bottom:4px;display:block"
                )
                for pkey, plabel in [
                    ("descripcion", "Descripción"),
                    ("cantidad", "Cantidad"),
                    ("precio_unitario", "Precio unitario"),
                    ("precio_total", "Precio total"),
                ]:
                    pval = prod.get(pkey)
                    pval_str = "" if pval is None else str(pval)
                    with ui.element("div").style(
                        "display:flex;align-items:center;gap:8px;padding:3px 0"
                    ):
                        ui.label(plabel).style(
                            "width:140px;font-size:12px;color:#6b7280;flex-shrink:0"
                        )
                        ui.input(value=pval_str).props("dense outlined").style(
                            "flex:1;font-size:12px"
                        )


def _rebuild_tabla(
    user_id: int,
    tabla_container,
    filas_ref: list,
    parsed_ref: list,
) -> None:
    tabla_container.clear()
    rows = _list_guias(user_id)
    with tabla_container:
        if not rows:
            ui.label("No hay guías guardadas.").style(
                "font-size:13px;color:#9ca3af;font-style:italic;padding:8px 0"
            )
            return

        # encabezado
        with ui.element("div").style(
            "display:grid;"
            "grid-template-columns:1.5fr 1fr 1fr 0.8fr 1.2fr 50px 50px;"
            "gap:6px;padding:6px 10px;"
            "background:#f1f5f9;border-radius:6px 6px 0 0;"
            "border:0.5px solid #e2e8f0;"
            "font-size:11px;font-weight:600;color:#6b7280"
        ):
            for h in ["Razón social", "Nro. Factura", "Nro. Invoice", "Fecha", "Guardado", "", ""]:
                ui.label(h)

        for r in rows:
            rid = r["id"]

            def _ver(rid=rid):
                guia = _get_guia(rid, user_id)
                if guia:
                    parsed_ref[0] = guia
                    filas_ref[0].clear()
                    with filas_ref[0]:
                        _render_campos(guia)
                    ui.notify("Guía cargada en el panel", color="positive")

            def _del(rid=rid):
                _delete_guia(rid, user_id)
                ui.notify("Guía eliminada", color="info")
                _rebuild_tabla(user_id, tabla_container, filas_ref, parsed_ref)

            with ui.element("div").style(
                "display:grid;"
                "grid-template-columns:1.5fr 1fr 1fr 0.8fr 1.2fr 50px 50px;"
                "gap:6px;padding:5px 10px;"
                "border:0.5px solid #e2e8f0;border-top:none;"
                "font-size:12px;color:#374151;align-items:center"
            ):
                ui.label(r["razon_social"]).style(
                    "overflow:hidden;text-overflow:ellipsis;white-space:nowrap"
                )
                ui.label(r["nro_factura"]).style(
                    "overflow:hidden;text-overflow:ellipsis;white-space:nowrap"
                )
                ui.label(r["nro_invoice"]).style(
                    "overflow:hidden;text-overflow:ellipsis;white-space:nowrap"
                )
                ui.label(r["fecha"])
                ui.label(r["created_at"]).style("font-size:11px;color:#9ca3af")
                ui.button(icon="visibility", on_click=_ver).props("flat dense").style(
                    "color:#1d4ed8;min-width:32px"
                )
                ui.button(icon="delete", on_click=_del).props("flat dense").style(
                    "color:#dc2626;min-width:32px"
                )


# ── Tab principal ─────────────────────────────────────────────────────────────

def build_tab_guias() -> None:
    user = app.storage.user.get("user")
    if not user:
        ui.label("Debes iniciar sesión").classes("text-red-500 p-4")
        return

    user_id = user["id"]
    _init_guias_db()

    archivo_data: list = [None]
    archivo_mime: list = [None]
    parsed_ref: list = [None]
    spin_ref: list = [None]
    resultado_ref: list = [None]
    filas_ref: list = [None]
    guardar_btn_ref: list = [None]
    tabla_ref: list = [None]

    # ── Barra de título ───────────────────────────────────────────────────────
    with ui.element("div").style(
        "background:#f1f5f9;border-bottom:0.5px solid #e0e2e7;padding:10px 20px"
    ):
        ui.label("GUÍAS DE IMPORTACIÓN").style(
            "font-size:15px;font-weight:600;color:#374151;letter-spacing:.05em"
        )

    # ── Paneles superior: uploader + resultado ────────────────────────────────
    with ui.element("div").style(
        "display:flex;gap:24px;padding:20px;width:100%;"
        "align-items:flex-start;flex-wrap:wrap"
    ):
        # ── Izquierda: uploader + botones ─────────────────────────────────────
        with ui.element("div").style("flex:0 0 320px;min-width:280px"):
            ui.label("Subir documento").style(
                "font-size:13px;font-weight:600;color:#374151;margin-bottom:8px"
            )

            nombre_lbl = ui.label("Ningún archivo seleccionado").style(
                "font-size:12px;color:#9ca3af;font-style:italic;margin-bottom:8px"
            )

            def on_upload(e):
                archivo_data[0] = e.content.read()
                ext = e.name.rsplit(".", 1)[-1].lower() if "." in e.name else ""
                archivo_mime[0] = (
                    "application/pdf" if ext == "pdf"
                    else "image/jpeg" if ext in ("jpg", "jpeg")
                    else "image/png"
                )
                nombre_lbl.set_text(f"📄 {e.name}")

            ui.upload(
                label="PDF / JPG / PNG",
                on_upload=on_upload,
                auto_upload=True,
                max_files=1,
            ).props('accept=".pdf,.jpg,.jpeg,.png" flat bordered').classes("w-full")

            ui.separator().classes("my-4")

            async def _analizar(usar_gemini: bool) -> None:
                if not archivo_data[0]:
                    ui.notify("Primero subí un archivo", color="warning")
                    return
                groq_key = get_app_config("groq_api_key")
                gemini_key = get_app_config("gemini_api_key")
                es_imagen = archivo_mime[0] and archivo_mime[0].startswith("image/")

                if usar_gemini and not gemini_key:
                    ui.notify(
                        "Configurá tu API key de Gemini en Config → IA/Sugerencias",
                        color="warning",
                    )
                    return
                if not usar_gemini and not groq_key:
                    ui.notify(
                        "Configurá tu API key de Grok en Config → IA/Sugerencias",
                        color="warning",
                    )
                    return
                if not usar_gemini and es_imagen:
                    ui.notify(
                        "Grok solo procesa PDFs con texto. Usá Gemini para imágenes.",
                        color="info",
                    )
                    return

                spin_ref[0].set_visibility(True)
                resultado_ref[0].set_text("")
                filas_ref[0].clear()
                guardar_btn_ref[0].disable()

                try:
                    if usar_gemini:
                        raw = await run.io_bound(
                            _gemini_vision, gemini_key, archivo_data[0], archivo_mime[0]
                        )
                    else:
                        texto_pdf = await run.io_bound(_extract_pdf_text, archivo_data[0])
                        if not texto_pdf.strip():
                            ui.notify(
                                "No se pudo extraer texto del PDF. Probá con Gemini.",
                                color="warning",
                            )
                            return
                        full_prompt = PROMPT_GUIA + "\n\nCONTENIDO DEL DOCUMENTO:\n" + texto_pdf
                        raw = await run.io_bound(_groq_parse_doc, groq_key, full_prompt)

                    raw = _clean_json(raw)
                    try:
                        parsed = json.loads(raw)
                        parsed_ref[0] = parsed
                        with filas_ref[0]:
                            _render_campos(parsed)
                        guardar_btn_ref[0].enable()
                    except json.JSONDecodeError:
                        resultado_ref[0].set_text(raw)
                except Exception as exc:
                    logger.error("Error analizando guía: %s", exc)
                    ui.notify(f"Error: {exc}", color="negative")
                finally:
                    spin_ref[0].set_visibility(False)

            with ui.row().classes("gap-2 flex-wrap"):
                ui.button(
                    "💡 Analizar con Grok",
                    on_click=lambda: _analizar(False),
                ).props("flat").style("background:#fff7ed;color:#c2410c")
                ui.button(
                    "✨ Analizar con Gemini",
                    on_click=lambda: _analizar(True),
                ).props("flat").style("background:#eff6ff;color:#1d4ed8")

            ui.separator().classes("my-3")

            def _guardar():
                if not parsed_ref[0]:
                    ui.notify("Primero analizá un documento", color="warning")
                    return
                _save_guia(user_id, parsed_ref[0])
                ui.notify("Guía guardada correctamente", color="positive")
                _rebuild_tabla(user_id, tabla_ref[0], filas_ref, parsed_ref)

            guardar_btn = ui.button(
                "💾 Guardar guía",
                on_click=_guardar,
            ).props("flat").style("background:#f0fdf4;color:#166534;width:100%")
            guardar_btn.disable()
            guardar_btn_ref[0] = guardar_btn

        # ── Derecha: resultado del análisis ───────────────────────────────────
        with ui.element("div").style("flex:1;min-width:300px"):
            ui.label("Resultado del análisis").style(
                "font-size:13px;font-weight:600;color:#374151;margin-bottom:8px"
            )

            spin = ui.spinner(size="lg").classes("text-blue-500")
            spin.set_visibility(False)
            spin_ref[0] = spin

            resultado_txt = ui.label("").style(
                "white-space:pre-wrap;font-family:monospace;font-size:13px;color:#374151"
            )
            resultado_ref[0] = resultado_txt

            filas_container = ui.element("div").style("width:100%")
            filas_ref[0] = filas_container

    # ── Tabla de guías guardadas ──────────────────────────────────────────────
    ui.separator().classes("mx-4")
    with ui.element("div").style("padding:0 20px 24px"):
        ui.label("Guías guardadas").style(
            "font-size:13px;font-weight:600;color:#374151;margin-bottom:12px;display:block"
        )
        tabla_container = ui.element("div").style("width:100%")
        tabla_ref[0] = tabla_container
        _rebuild_tabla(user_id, tabla_container, filas_ref, parsed_ref)
