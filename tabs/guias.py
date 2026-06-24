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

from db import get_app_config, get_connection, get_setting

logger = logging.getLogger(__name__)

PROMPT_GUIA = """
Analizá este documento de importación y extraé los siguientes datos en formato JSON.
Si el dato no existe en el documento ponelo como null.

INSTRUCCIONES ESPECÍFICAS:

IMPORTANTE — Hay DOS tipos de documentos distintos:
1. INVOICE DEL PROVEEDOR EXTRANJERO: tiene un número de referencia propio del proveedor (ej: INV-2024-001, PO-123) → va en nro_invoice
2. FACTURA DEL DESPACHANTE ARGENTINO: tiene número de factura argentina formato XXXX-XXXXXXXX → va en nro_factura
NUNCA poner el mismo valor en ambos campos.
Si solo hay un documento, identificar de qué tipo es y usar el campo correcto, dejar el otro en null.

- razon_social: nombre o razón social del proveedor o despachante que emite el documento.
- pais_procedencia: país de procedencia según consta en el documento ARCA/aduana.
- pos_arancelaria: posición arancelaria según el documento ARCA/aduana.
- desc_mercaderia: descripción de mercadería según el documento ARCA/aduana.
- fob_total: total en USD del proveedor extranjero (balance due del invoice, importe total en dólares).
- Para flete_aereo, entrega_domicilio, resolucion_3244, seguro_internacional, almacenaje y servicios_honorarios: tomar el valor de la ÚLTIMA columna numérica del documento, que representa el importe en pesos argentinos ($). IGNORAR la primera columna que está en dólares (USD o u$s).
- En el recuadro o tabla separada ubicada en la parte INFERIOR IZQUIERDA del documento hay exactamente 3 valores en la columna "Importe", de arriba hacia abajo:
  1. derechos_importacion (primer valor, el más alto del recuadro)
  2. tasa_estadistica (segundo valor)
  3. iva_aduanero (tercer valor, el más bajo del recuadro) — CAMPO OBLIGATORIO
  MÉTODO POSICIONAL: si ya encontraste iva_aduanero (ítem 3), entonces derechos_importacion es el valor que está DOS filas arriba de él en ese mismo recuadro, independientemente de cómo esté etiquetado. No uses solo la etiqueta para identificarlo.
  Etiquetas posibles como referencia (no como único criterio): "Derechos de Importación", "Der. Importación", "Derechos Imp.", "D. Importación", "Der. Imp.", "Dcho. Importación", "Derechos".
  IMPORTANTE: si iva_aduanero > 0 y derechos_importacion sigue siendo 0 o null, releer el recuadro usando la posición relativa descrita arriba.
  CRÍTICO para iva_aduanero: este campo es OBLIGATORIO y siempre tiene valor en el documento. Su etiqueta en el documento es "IVA Aduanero" (sin ambigüedad). Es el tercer ítem del recuadro inferior izquierdo. Si no lo encontrás por etiqueta, buscarlo como el TERCER valor numérico de la columna "Importe" de ese recuadro (contando de arriba hacia abajo). NUNCA devolver 0 — si después de ambos métodos no lo encontrás, devolver null para indicar un error de lectura, no 0. derechos_importacion y tasa_estadistica SÍ pueden ser 0 o null si no están en el documento.
- iva_21: valor en pesos argentinos que aparece con la etiqueta "IVA % 21", "IVA 21%", "I.V.A. 21%" u otras variantes de IVA al 21%. Está en la columna "Importe" del mismo recuadro de tributos.
- total_real: gran total general de la factura/guía en ARS. Buscar la línea etiquetada exactamente como "TOTAL" en mayúsculas en el documento. Es el TOTAL final del documento (no un subtotal ni total parcial). Si no existe o no está claro, devolver null.
- Para tipo_cambio: buscar un valor con formato X/Y/Z y separar en 3 campos individuales (tipo_cambio_1, tipo_cambio_2, tipo_cambio_3).
- Para kgs: buscar el peso total en kilogramos.
- hawb: número de guía aérea. Se encuentra en la primera página, en la parte superior del documento, en una línea que dice "HAWB: XXXXXXX". Extraer solo el valor alfanumérico, sin los dos puntos ni espacios.
- Para el array `productos`: el campo `sku` es el código o referencia del artículo según el invoice del proveedor (puede aparecer como SKU, Part No, Part Number, Item Code, Ref., P/N, Model, etc.). Si no figura en el documento, usar string vacío "".

{
  "razon_social": null,
  "nro_invoice": null,
  "nro_factura": null,
  "hawb": null,
  "fecha": null,
  "pais_procedencia": null,
  "pos_arancelaria": null,
  "desc_mercaderia": null,
  "fob_total": null,
  "productos": [
    {"sku": "", "descripcion": "", "cantidad": null, "precio_unitario": null, "precio_total": null}
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
  "iva_21": null,
  "derechos_importacion": null,
  "tasa_estadistica": null,
  "pa": null,
  "total_real": null
}

Respondé SOLO con el JSON, sin texto adicional ni backticks.
"""

_LABELS = {
    "razon_social": "Razón social",
    "nro_invoice": "Nro. Invoice",
    "nro_factura": "Nro. Factura",
    "hawb": "HAWB",
    "fecha": "Fecha",
    "pais_procedencia": "País de procedencia",
    "pos_arancelaria": "Posición arancelaria",
    "desc_mercaderia": "Desc. mercadería",
    "fob_total": "FOB Total",
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
    "iva_21": "IVA 21%",
    "derechos_importacion": "Derechos de importación",
    "tasa_estadistica": "Tasa estadística",
    "pa": "PA",
    "total_real": "Total real",
}

_SCALAR_COLS = [
    "razon_social", "nro_invoice", "nro_factura", "hawb", "fecha",
    "pais_procedencia", "pos_arancelaria", "desc_mercaderia", "fob_total",
    "kgs", "tipo_cambio_1", "tipo_cambio_2", "tipo_cambio_3",
    "flete_aereo", "entrega_domicilio", "resolucion_3244",
    "seguro_internacional", "almacenaje", "servicios_honorarios",
    "iva_aduanero", "iva_21", "derechos_importacion", "tasa_estadistica",
    "pa", "total_real",
]

_TABLE_HEADERS = [
    "Courier", "Factura", "HAWB", "PA", "Fecha", "Origen", "Invoice Nro",
    "FOB Total", "Peso Total", "Derechos", "Estadística", "IVA Aduanero",
    "Flete Aduanero", "Almacenaje", "Total Factura", "Total real", "Alm/KG", "Valor Kg", "Dolar",
    "Traída u$ s/IVA", "Costo s/IVA", "Total Traída %", "",
]

_TABLE_COLS = (
    "1.4fr 0.9fr 0.8fr 0.5fr 0.7fr 0.8fr 0.9fr "
    "0.7fr 0.7fr 0.8fr 0.8fr 0.8fr 0.8fr 0.7fr "
    "0.7fr 0.7fr 0.7fr 0.7fr 0.6fr 0.8fr 0.8fr 0.8fr 96px"
)


# ── DB helpers ────────────────────────────────────────────────────────────────

def _init_guias_db() -> None:
    conn = get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS guias_importacion (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id              INTEGER NOT NULL,
            razon_social         TEXT,
            hawb                 TEXT,
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
            pais_procedencia     TEXT,
            pos_arancelaria      TEXT,
            desc_mercaderia      TEXT,
            fob_total            TEXT,
            pa                   TEXT,
            created_at           DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    existing = {row[1] for row in conn.execute("PRAGMA table_info(guias_importacion)")}
    for col in ("pais_procedencia", "pos_arancelaria", "desc_mercaderia", "fob_total", "pa", "hawb", "iva_21", "total_real"):
        if col not in existing:
            conn.execute(f"ALTER TABLE guias_importacion ADD COLUMN {col} TEXT")
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


def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(str(v).replace(",", ".").replace(" ", "").replace("$", "").strip())
    except (ValueError, TypeError):
        return None


def _list_guias(user_id: int) -> List[Dict[str, Any]]:
    dolar_blue = get_setting("dolar_blue")
    conn = get_connection()
    rows = conn.execute(
        "SELECT id, razon_social, hawb, pa, fecha, pais_procedencia, nro_invoice, nro_factura, fob_total, kgs, "
        "derechos_importacion, tasa_estadistica, iva_aduanero, iva_21, flete_aereo, "
        "entrega_domicilio, resolucion_3244, seguro_internacional, servicios_honorarios, "
        "almacenaje, tipo_cambio_3, total_real, productos, created_at "
        "FROM guias_importacion WHERE user_id = ? ORDER BY created_at DESC",
        (user_id,),
    ).fetchall()
    conn.close()
    result = []
    for r in rows:
        flete = _to_float(r["flete_aereo"])
        kgs = _to_float(r["kgs"])
        tc3 = _to_float(r["tipo_cambio_3"])
        valor_kg = ""
        if flete and kgs and tc3 and kgs != 0 and tc3 != 0:
            valor_kg = f"{flete / kgs / tc3:.2f}"

        iva21_val = _to_float(r["iva_21"])
        almacenaje_float = _to_float(r["almacenaje"])
        almacenaje_kg = None
        if almacenaje_float and dolar_blue and dolar_blue != 0 and kgs and kgs != 0:
            almacenaje_kg = almacenaje_float / dolar_blue / kgs
        tf_components = [
            ("flete_aereo",          "Flete aéreo",             _to_float(r["flete_aereo"])),
            ("entrega_domicilio",    "Entrega a domicilio",     _to_float(r["entrega_domicilio"])),
            ("resolucion_3244",      "Resolución 3244",         _to_float(r["resolucion_3244"])),
            ("seguro_internacional", "Seguro internacional",    _to_float(r["seguro_internacional"])),
            ("almacenaje",           "Almacenaje",              _to_float(r["almacenaje"])),
            ("servicios_honorarios", "Servicios / Honorarios",  _to_float(r["servicios_honorarios"])),
            ("iva_aduanero",         "IVA aduanero",            _to_float(r["iva_aduanero"])),
            ("derechos_importacion", "Derechos de importación", _to_float(r["derechos_importacion"])),
            ("tasa_estadistica",     "Tasa estadística",        _to_float(r["tasa_estadistica"])),
        ]
        total_factura = sum(v for _, _, v in tf_components if v is not None)

        pa_val = _to_float(r["pa"])
        iva_val = _to_float(r["iva_aduanero"])
        fob_val = _to_float(r["fob_total"])
        traida_usd = None
        if dolar_blue and dolar_blue != 0 and pa_val is not None:
            traida_usd = (
                total_factura + (pa_val * dolar_blue)
                - (iva_val or 0.0)
                - (iva21_val or 0.0)
            ) / dolar_blue

        total_traida_pct = None
        if fob_val and fob_val != 0 and traida_usd is not None:
            total_traida_pct = traida_usd / fob_val

        costo_sin_iva = None
        if fob_val and fob_val != 0 and total_traida_pct is not None:
            costo_sin_iva = fob_val * (1 + total_traida_pct)

        traida_breakdown = {
            "total_factura": total_factura,
            "pa_val": pa_val,
            "iva_val": iva_val or 0.0,
            "iva21_val": iva21_val or 0.0,
            "dolar_blue": dolar_blue,
            "traida_usd": traida_usd,
        }

        result.append({
            "id": r["id"],
            "razon_social": r["razon_social"] or "",
            "nro_factura": r["nro_factura"] or "",
            "hawb": r["hawb"] or "",
            "pa": r["pa"] or "",
            "fecha": r["fecha"] or "",
            "pais_procedencia": r["pais_procedencia"] or "",
            "nro_invoice": r["nro_invoice"] or "",
            "fob_total": r["fob_total"] or "",
            "kgs": r["kgs"] or "",
            "derechos_importacion": r["derechos_importacion"] or "",
            "tasa_estadistica": r["tasa_estadistica"] or "",
            "iva_aduanero": r["iva_aduanero"] or "",
            "iva_21_val": iva21_val,
            "flete_aereo": r["flete_aereo"] or "",
            "almacenaje": r["almacenaje"] or "",
            "valor_kg": valor_kg,
            "tipo_cambio_3": r["tipo_cambio_3"] or "",
            "total_factura": total_factura,
            "tf_components": tf_components,
            "traida_usd": traida_usd,
            "total_traida_pct": total_traida_pct,
            "costo_sin_iva": costo_sin_iva,
            "traida_breakdown": traida_breakdown,
            "total_real": r["total_real"] or "",
            "almacenaje_kg": almacenaje_kg,
            "productos": json.loads(r["productos"] or "[]") if r["productos"] else [],
        })
    return result


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


# ── Formato numérico ──────────────────────────────────────────────────────────

def _fmt_num(v) -> str:
    if v is None:
        return "—"
    try:
        n = round(float(v))
        return f"{n:,}".replace(",", ".")
    except (ValueError, TypeError):
        return "—"


def _fmt_ars(v) -> str:
    if v is None:
        return "—"
    try:
        n = round(float(v))
        return "$" + f"{n:,}".replace(",", ".")
    except (ValueError, TypeError):
        return "—"


def _fmt_usd(v) -> str:
    if v is None:
        return "—"
    try:
        n = round(float(v))
        return "u$s " + f"{n:,}".replace(",", ".")
    except (ValueError, TypeError):
        return "—"


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

        with ui.element("div").style("overflow-x:auto;width:100%"):
            # Single grid — header + todas las filas comparten el mismo grid para alineación perfecta
            with ui.element("div").style(
                f"display:grid;grid-template-columns:{_TABLE_COLS};"
                "column-gap:4px;min-width:1700px;align-items:center"
            ):
                # ── Cabecera ──────────────────────────────────────────────────
                _hs = (
                    "padding:6px 4px;background:#f1f5f9;border-bottom:1px solid #e2e8f0;"
                    "font-size:10px;font-weight:600;color:#6b7280;"
                    "white-space:normal;word-break:break-word;line-height:1.3;"
                    "min-height:44px;display:flex;align-items:center;justify-content:center;text-align:center"
                )
                for h in _TABLE_HEADERS:
                    ui.label(h).style(_hs)

                # ── Filas de datos ─────────────────────────────────────────────
                _sep = "border-bottom:0.5px solid #f1f5f9"
                _ct = f"padding:3px 4px;font-size:11px;color:#374151;{_sep}"

                for r in rows:
                    rid = r["id"]
                    tf_comps = r["tf_components"]
                    traida_bd = r["traida_breakdown"]
                    iv21 = r["iva_21_val"]

                    det_id = f"guia-det-{rid}"
                    ico_id = f"guia-ico-{rid}"

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

                    # Courier
                    ui.label(r["razon_social"]).style(
                        f"{_ct};overflow:hidden;text-overflow:ellipsis;white-space:nowrap"
                    )
                    # Factura
                    nro_fac = r.get("nro_factura") or ""
                    if nro_fac and "-" in nro_fac:
                        nro_fac_disp = nro_fac.split("-", 1)[1]
                    else:
                        nro_fac_disp = nro_fac or "—"
                    ui.label(nro_fac_disp).style(
                        f"{_ct};overflow:hidden;text-overflow:ellipsis;white-space:nowrap;text-align:center"
                    )
                    # HAWB
                    ui.label(r["hawb"]).style(
                        f"{_ct};overflow:hidden;text-overflow:ellipsis;white-space:nowrap;text-align:center"
                    )
                    # PA
                    ui.label(_fmt_usd(r["pa"])).style(
                        f"{_ct};white-space:nowrap;text-align:center"
                    )
                    # Fecha
                    ui.label(r["fecha"]).style(f"{_ct};white-space:nowrap;text-align:center")
                    # Origen — ESTADOS UNIDOS → USA
                    _origen = r["pais_procedencia"]
                    if _origen and "estados uni" in _origen.lower():
                        _origen = "USA"
                    ui.label(_origen).style(
                        f"{_ct};overflow:hidden;text-overflow:ellipsis;white-space:nowrap;text-align:center"
                    )
                    # Invoice Nro
                    ui.label(r["nro_invoice"]).style(
                        f"{_ct};overflow:hidden;text-overflow:ellipsis;white-space:nowrap;text-align:center"
                    )
                    # FOB Total
                    ui.label(_fmt_usd(r["fob_total"])).style(
                        f"{_ct};white-space:nowrap;text-align:right"
                    )
                    # Peso Total
                    ui.label(r["kgs"]).style(f"{_ct};white-space:nowrap;text-align:center")
                    # Derechos
                    ui.label(_fmt_ars(r["derechos_importacion"])).style(
                        f"{_ct};white-space:nowrap;text-align:right"
                    )
                    # Estadística
                    ui.label(_fmt_ars(r["tasa_estadistica"])).style(
                        f"{_ct};white-space:nowrap;text-align:right"
                    )
                    # IVA Aduanero
                    ui.label(_fmt_ars(r["iva_aduanero"])).style(
                        f"{_ct};white-space:nowrap;text-align:right"
                    )
                    # Flete Aduanero
                    ui.label(_fmt_ars(r["flete_aereo"])).style(
                        f"{_ct};white-space:nowrap;text-align:right"
                    )
                    # Almacenaje
                    ui.label(_fmt_ars(r["almacenaje"])).style(f"{_ct};white-space:nowrap;text-align:right")
                    # Total Factura — clickeable sin subrayado
                    with ui.element("div").style(
                        f"display:flex;justify-content:flex-end;align-items:center;"
                        f"padding:3px 4px;{_sep}"
                    ):
                        ui.button(
                            _fmt_ars(r["total_factura"]),
                            on_click=lambda tf=tf_comps, iv=iv21: _show_total_factura_dialog(tf, iv),
                        ).props("flat dense").style(
                            "color:#1d4ed8;font-size:11px;white-space:nowrap;"
                            "padding:0 2px;min-height:0;text-decoration:none"
                        )
                    # Total real
                    ui.label(_fmt_ars(r["total_real"])).style(
                        f"{_ct};white-space:nowrap;text-align:right"
                    )
                    # Almacenaje / KG
                    alm_kg = r.get("almacenaje_kg")
                    ui.label(f"u$s {alm_kg:.2f}" if alm_kg is not None else "—").style(
                        f"{_ct};white-space:nowrap;text-align:center;color:#1d4ed8"
                    )
                    # Valor Kg
                    ui.label(f"u$s {r['valor_kg']}" if r["valor_kg"] else "—").style(
                        f"{_ct};white-space:nowrap;text-align:center;color:#1d4ed8"
                    )
                    # Dolar
                    ui.label(_fmt_ars(r["tipo_cambio_3"])).style(f"{_ct};white-space:nowrap;text-align:right")
                    # Traída u$ s/IVA — clickeable sin subrayado
                    if r["traida_usd"] is not None:
                        with ui.element("div").style(
                            f"display:flex;justify-content:flex-end;align-items:center;"
                            f"padding:3px 4px;{_sep}"
                        ):
                            ui.button(
                                _fmt_usd(r["traida_usd"]),
                                on_click=lambda bd=traida_bd: _show_traida_dialog(bd),
                            ).props("flat dense").style(
                                "color:#374151;font-size:11px;white-space:nowrap;"
                                "padding:0 2px;min-height:0;text-decoration:none"
                            )
                    else:
                        ui.label("—").style(
                            f"{_ct};white-space:nowrap;text-align:right;color:#9ca3af"
                        )
                    # Costo s/IVA
                    ui.label(
                        _fmt_usd(r["costo_sin_iva"]) if r["costo_sin_iva"] is not None else "—"
                    ).style(f"{_ct};white-space:nowrap;text-align:right")
                    # Total Traída %
                    pct = r["total_traida_pct"]
                    ui.label(
                        f"{pct * 100:.2f}%" if pct is not None else "—"
                    ).style(f"{_ct};white-space:nowrap;text-align:center;color:#1d4ed8;font-weight:600")
                    # Acciones
                    with ui.row().classes("gap-0").style(
                        f"justify-content:center;{_sep};padding:3px 0"
                    ):
                        with ui.element("div").classes(ico_id).style("display:inline-flex"):
                            ui.button(
                                icon="chevron_right",
                                on_click=_toggle_row,
                            ).props("flat dense").style("color:#6b7280;min-width:28px")
                        ui.button(
                            icon="visibility",
                            on_click=lambda rid=rid: _show_ver_dialog(rid, user_id),
                        ).props("flat dense").style("color:#1d4ed8;min-width:28px")
                        ui.button(
                            icon="delete",
                            on_click=lambda rid=rid: _show_del_dialog(
                                rid, user_id, tabla_container, filas_ref, parsed_ref
                            ),
                        ).props("flat dense").style("color:#dc2626;min-width:28px")
                    # Fila expandible — abarca todas las columnas del grid
                    det_productos = r.get("productos") or []
                    with ui.element("div").classes(det_id).style(
                        "grid-column:1/-1;display:none;padding:4px 12px 8px 32px"
                    ):
                        if not det_productos:
                            ui.label("Sin productos registrados").style(
                                "font-size:11px;color:#9ca3af;font-style:italic;padding:4px 0"
                            )
                        else:
                            _sub_cols = "0.8fr 3fr 0.5fr 1fr 1fr"
                            with ui.element("div").style(
                                f"display:grid;grid-template-columns:{_sub_cols};"
                                "column-gap:4px;border:1px solid #bfdbfe;"
                                "border-radius:6px;overflow:hidden"
                            ):
                                _sh2 = (
                                    "padding:5px 6px;background:#E6F1FB;font-size:10px;"
                                    "font-weight:600;color:#1d4ed8;text-align:center"
                                )
                                for _h in ["SKU", "Descripción", "Qty", "Precio unitario", "Costo Imp. u$s/IVA"]:
                                    ui.label(_h).style(_sh2)
                                _sp2 = (
                                    "padding:4px 6px;font-size:11px;color:#374151;"
                                    "border-top:0.5px solid #e0edff"
                                )
                                for prod in det_productos:
                                    pu_f = _to_float(prod.get("precio_unitario"))
                                    traida_pct = r["total_traida_pct"]
                                    costo_imp = (
                                        pu_f * (1 + traida_pct)
                                        if pu_f is not None and traida_pct is not None
                                        else None
                                    )
                                    ui.label(str(prod.get("sku") or "—")).style(
                                        f"{_sp2};text-align:center"
                                    )
                                    ui.label(str(prod.get("descripcion") or "—")).style(_sp2)
                                    ui.label(str(prod.get("cantidad") or "—")).style(
                                        f"{_sp2};text-align:center"
                                    )
                                    ui.label(
                                        f"u$s {pu_f:.2f}" if pu_f is not None else "—"
                                    ).style(f"{_sp2};text-align:right")
                                    ui.label(
                                        f"u$s {costo_imp:.2f}" if costo_imp is not None else "—"
                                    ).style(f"{_sp2};text-align:right")


# ── Dialog helpers ────────────────────────────────────────────────────────────

def _show_ver_dialog(guia_id: int, user_id: int) -> None:
    data = _get_guia(guia_id, user_id)
    if not data:
        ui.notify("No se encontró la guía", color="warning")
        return
    with ui.dialog() as d, ui.card().style(
        "min-width:500px;max-width:720px;max-height:80vh;overflow-y:auto;padding:20px"
    ):
        ui.label("Detalle de Guía").style(
            "font-size:15px;font-weight:600;color:#374151;margin-bottom:12px;display:block"
        )
        for key, label in _LABELS.items():
            val = data.get(key)
            val_str = "" if val is None else str(val)
            with ui.element("div").style(
                "display:flex;gap:8px;padding:4px 0;border-bottom:0.5px solid #f1f5f9"
            ):
                ui.label(label).style("width:200px;font-size:13px;color:#6b7280;flex-shrink:0")
                ui.label(val_str).style("font-size:13px;color:#374151")
        productos = data.get("productos") or []
        if productos:
            ui.label("Productos").style(
                "font-weight:600;font-size:13px;color:#374151;"
                "margin-top:14px;margin-bottom:6px;display:block"
            )
            for i, prod in enumerate(productos):
                with ui.element("div").style(
                    "background:#f8fafc;border:0.5px solid #e2e8f0;"
                    "border-radius:6px;padding:8px;margin-bottom:6px"
                ):
                    ui.label(f"Producto {i + 1}").style(
                        "font-size:11px;color:#9ca3af;margin-bottom:4px;display:block"
                    )
                    for pkey, plabel in [
                        ("descripcion", "Descripción"), ("cantidad", "Cantidad"),
                        ("precio_unitario", "Precio unitario"), ("precio_total", "Precio total"),
                    ]:
                        pval = prod.get(pkey)
                        if pval is not None:
                            with ui.element("div").style("display:flex;gap:8px;padding:2px 0"):
                                ui.label(plabel).style(
                                    "width:140px;font-size:12px;color:#6b7280;flex-shrink:0"
                                )
                                ui.label(str(pval)).style("font-size:12px;color:#374151")
        ui.button("Cerrar", on_click=d.close).props("flat").style(
            "margin-top:16px;color:#374151"
        )
    d.open()


def _show_del_dialog(
    rid: int, user_id: int, tabla_container, filas_ref: list, parsed_ref: list
) -> None:
    with ui.dialog() as d, ui.card().style("padding:24px;min-width:280px"):
        ui.label("¿Eliminar esta guía?").style(
            "font-size:14px;font-weight:500;color:#374151;margin-bottom:16px;display:block"
        )
        with ui.row().classes("gap-2"):
            ui.button("Cancelar", on_click=d.close).props("flat")
            def _confirm(d=d):
                d.close()
                _delete_guia(rid, user_id)
                ui.notify("Guía eliminada", color="info")
                _rebuild_tabla(user_id, tabla_container, filas_ref, parsed_ref)
            ui.button("Eliminar", on_click=_confirm).props("flat").style("color:#dc2626")
    d.open()


def _show_total_factura_dialog(tf_components: list, iva21_val=None) -> None:
    with ui.dialog() as d, ui.card().style("padding:20px;min-width:340px"):
        ui.label("Detalle Total Factura").style(
            "font-size:14px;font-weight:600;color:#374151;margin-bottom:12px;display:block"
        )
        for _, label, val in tf_components:
            with ui.element("div").style(
                "display:flex;justify-content:space-between;align-items:center;"
                "padding:4px 0;border-bottom:0.5px solid #f1f5f9;gap:16px"
            ):
                ui.label(label).style("font-size:13px;color:#6b7280")
                ui.label(_fmt_ars(val) if val is not None else "—").style(
                    "font-size:13px;color:#374151"
                )
        if iva21_val:
            with ui.element("div").style(
                "display:flex;justify-content:space-between;align-items:center;"
                "padding:4px 0;border-bottom:0.5px solid #f1f5f9;gap:16px"
            ):
                ui.label("IVA 21% (ya incluido)").style(
                    "font-size:13px;color:#9ca3af;font-style:italic"
                )
                ui.label(_fmt_ars(iva21_val)).style(
                    "font-size:13px;color:#9ca3af;font-style:italic"
                )
        total = sum(v for _, _, v in tf_components if v is not None)
        with ui.element("div").style(
            "display:flex;justify-content:space-between;padding:6px 0;margin-top:4px"
        ):
            ui.label("Total").style("font-size:13px;font-weight:600;color:#374151")
            ui.label(_fmt_ars(total)).style("font-size:13px;font-weight:600;color:#374151")
        ui.button("Cerrar", on_click=d.close).props("flat").style(
            "margin-top:8px;color:#374151"
        )
    d.open()


def _show_traida_dialog(breakdown: dict) -> None:
    tf = breakdown["total_factura"]
    pa_val = breakdown["pa_val"]
    iva_val = breakdown["iva_val"]
    iva21_val = breakdown.get("iva21_val", 0.0)
    dolar_blue = breakdown["dolar_blue"]
    traida_usd = breakdown["traida_usd"]

    with ui.dialog() as d, ui.card().style("padding:20px;min-width:380px"):
        ui.label("Detalle Traída u$ s/IVA").style(
            "font-size:14px;font-weight:600;color:#374151;margin-bottom:12px;display:block"
        )

        def _fila(label: str, val_str: str) -> None:
            with ui.element("div").style(
                "display:flex;justify-content:space-between;align-items:center;"
                "padding:4px 0;border-bottom:0.5px solid #f1f5f9;gap:16px"
            ):
                ui.label(label).style("font-size:13px;color:#6b7280;flex-shrink:0")
                ui.label(val_str).style("font-size:13px;color:#374151;text-align:right")

        _fila("Total Factura (ARS)", _fmt_ars(tf))
        if pa_val is not None and dolar_blue:
            pa_ars = pa_val * dolar_blue
            pa_str = f"{_fmt_usd(pa_val)} × {_fmt_ars(dolar_blue)} = {_fmt_ars(pa_ars)}"
        else:
            pa_str = "—"
        _fila("PA en ARS (pa × dólar blue)", pa_str)
        _fila("IVA Aduanero restado (ARS)", _fmt_ars(iva_val) if iva_val else "—")
        _fila("IVA 21% restado (ARS)", _fmt_ars(iva21_val) if iva21_val else "—")
        _fila("Dólar blue usado", _fmt_ars(dolar_blue) if dolar_blue else "—")

        with ui.element("div").style(
            "display:flex;justify-content:space-between;align-items:center;"
            "border-top:1px solid #e2e8f0;padding-top:8px;margin-top:6px"
        ):
            if traida_usd is not None and pa_val is not None and dolar_blue:
                pa_ars = pa_val * dolar_blue
                formula = (
                    f"({_fmt_ars(tf)} + {_fmt_ars(pa_ars)} − {_fmt_ars(iva_val)} − {_fmt_ars(iva21_val)}) "
                    f"÷ {_fmt_ars(dolar_blue)}"
                )
            else:
                formula = ""
            with ui.element("div"):
                ui.label("Traída u$ s/IVA").style(
                    "font-size:13px;font-weight:600;color:#374151;display:block"
                )
                if formula:
                    ui.label(formula).style(
                        "font-size:10px;color:#9ca3af;word-break:break-word;display:block"
                    )
            ui.label(_fmt_usd(traida_usd) if traida_usd is not None else "—").style(
                "font-size:13px;font-weight:600;color:#374151;white-space:nowrap"
            )

        ui.button("Cerrar", on_click=d.close).props("flat").style(
            "margin-top:10px;color:#374151"
        )
    d.open()


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
    tabla_ref: list = [None]
    uploader_ref: list = [None]

    # ── Panel superior compacto ───────────────────────────────────────────────
    with ui.element("div").style(
        "margin:16px 20px 0;background:#f8fafc;border:0.5px solid #e2e8f0;"
        "border-radius:8px;font-size:13px"
    ):
        # ── Fila 1: upload + nombre + PA ─────────────────────────────────────
        with ui.element("div").style(
            "display:flex;align-items:center;gap:12px;padding:8px 12px;flex-wrap:wrap"
        ):
            def on_upload(e):
                archivo_data[0] = e.content.read()
                ext = e.name.rsplit(".", 1)[-1].lower() if "." in e.name else ""
                archivo_mime[0] = (
                    "application/pdf" if ext == "pdf"
                    else "image/jpeg" if ext in ("jpg", "jpeg")
                    else "image/png"
                )

            _uploader = ui.upload(
                label="Subir PDF/IMG",
                on_upload=on_upload,
                auto_upload=True,
                max_files=1,
            ).props('accept=".pdf,.jpg,.jpeg,.png" flat bordered').style("font-size:12px")
            uploader_ref[0] = _uploader

            pa_select = ui.select(
                options=[0, 100, 150, 200, 250, 300],
                value=200,
                label="PA",
            ).props("dense outlined").style("width:90px;font-size:12px")

        # ── Divisor ───────────────────────────────────────────────────────────
        ui.element("div").style("border-top:0.5px solid #e2e8f0;margin:0 12px")

        # ── Fila 2: botones + spinner + estado ────────────────────────────────
        with ui.element("div").style(
            "display:flex;align-items:center;gap:8px;padding:8px 12px;flex-wrap:wrap"
        ):
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
                        parsed["pa"] = pa_select.value
                        parsed_ref[0] = parsed
                        filas_ref[0].clear()
                        _save_guia(user_id, parsed)
                        _rebuild_tabla(user_id, tabla_ref[0], filas_ref, parsed_ref)
                        ui.notify("Guía agregada automáticamente", color="positive")
                        archivo_data[0] = None
                        archivo_mime[0] = None
                        uploader_ref[0].reset()
                    except json.JSONDecodeError:
                        resultado_ref[0].set_text("Error: JSON inválido")
                except Exception as exc:
                    logger.error("Error analizando guía: %s", exc)
                    ui.notify(f"Error: {exc}", color="negative")
                finally:
                    spin_ref[0].set_visibility(False)

            ui.button(
                "Analizar con Grok",
                icon="bolt",
                on_click=lambda: _analizar(False),
            ).props("flat dense").style("background:#fff7ed;color:#c2410c;font-size:12px")

            ui.button(
                "Analizar con Gemini",
                icon="auto_awesome",
                on_click=lambda: _analizar(True),
            ).props("flat dense").style("background:#faf5ff;color:#7c3aed;font-size:12px")

            spin = ui.spinner(size="sm").classes("text-blue-500")
            spin.set_visibility(False)
            spin_ref[0] = spin

            resultado_txt = ui.label("").style(
                "font-size:12px;color:#16a34a;font-weight:500;margin-left:auto"
            )
            resultado_ref[0] = resultado_txt

    # Container oculto para mantener filas_ref activo (usado por _rebuild_tabla)
    filas_container = ui.element("div").style("display:none")
    filas_ref[0] = filas_container

    # ── Tabla de guías guardadas ──────────────────────────────────────────────
    with ui.element("div").style("padding:16px 0 24px"):
        ui.label("Guías guardadas").style(
            "font-size:13px;font-weight:600;color:#374151;margin-bottom:12px;"
            "display:block;padding-left:20px"
        )
        tabla_container = ui.element("div").style("width:100%")
        tabla_ref[0] = tabla_container
        _rebuild_tabla(user_id, tabla_container, filas_ref, parsed_ref)
