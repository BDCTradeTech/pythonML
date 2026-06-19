"""
tabs/guias.py
Pestaña Guías: análisis de documentos de importación con IA.
"""
from __future__ import annotations

import io
import json
import logging
from typing import Any, Dict

import requests as _requests
from nicegui import app, run, ui

from db import get_app_config

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


def _render_campos(data: Dict[str, Any]) -> None:
    for key, label in _LABELS.items():
        val = data.get(key)
        val_str = "" if val is None else str(val)
        with ui.element("div").style(
            "display:flex;align-items:center;gap:8px;padding:5px 0;border-bottom:0.5px solid #f1f5f9"
        ):
            ui.label(label).style("width:200px;font-size:13px;color:#6b7280;flex-shrink:0")
            ui.input(value=val_str).props("dense outlined").style("flex:1;font-size:13px")

    productos = data.get("productos") or []
    if productos:
        ui.label("Productos").style(
            "font-weight:600;font-size:13px;color:#374151;margin-top:14px;margin-bottom:6px;display:block"
        )
        for i, prod in enumerate(productos):
            with ui.element("div").style(
                "background:#f8fafc;border:0.5px solid #e2e8f0;border-radius:6px;padding:10px;margin-bottom:8px"
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
                        ui.label(plabel).style("width:140px;font-size:12px;color:#6b7280;flex-shrink:0")
                        ui.input(value=pval_str).props("dense outlined").style("flex:1;font-size:12px")


def build_tab_guias() -> None:
    user = app.storage.user.get("user")
    if not user:
        ui.label("Debes iniciar sesión").classes("text-red-500 p-4")
        return

    archivo_data: list = [None]
    archivo_mime: list = [None]

    with ui.element("div").style(
        "background:#f1f5f9;border-bottom:0.5px solid #e0e2e7;padding:10px 20px"
    ):
        ui.label("GUÍAS DE IMPORTACIÓN").style(
            "font-size:15px;font-weight:600;color:#374151;letter-spacing:.05em"
        )

    with ui.element("div").style(
        "display:flex;gap:24px;padding:20px;width:100%;align-items:flex-start;flex-wrap:wrap"
    ):
        # ── Izquierda: uploader ─────────────────────────────────────────────
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

            spin_ref: list = [None]
            resultado_ref: list = [None]
            filas_ref: list = [None]

            async def _analizar(usar_gemini: bool) -> None:
                if not archivo_data[0]:
                    ui.notify("Primero subí un archivo", color="warning")
                    return
                groq_key = get_app_config("groq_api_key")
                gemini_key = get_app_config("gemini_api_key")
                es_imagen = archivo_mime[0] and archivo_mime[0].startswith("image/")

                if usar_gemini and not gemini_key:
                    ui.notify("Configurá tu API key de Gemini en Config → IA/Sugerencias", color="warning")
                    return
                if not usar_gemini and not groq_key:
                    ui.notify("Configurá tu API key de Grok en Config → IA/Sugerencias", color="warning")
                    return
                if not usar_gemini and es_imagen:
                    ui.notify("Grok solo procesa PDFs con texto. Usá Gemini para imágenes.", color="info")
                    return

                spin_ref[0].set_visibility(True)
                resultado_ref[0].set_text("")
                filas_ref[0].clear()

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
                        with filas_ref[0]:
                            _render_campos(parsed)
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

        # ── Derecha: resultado ──────────────────────────────────────────────
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
