"""
tabs/guias.py
Pestaña Guías: análisis de documentos de importación con IA.
"""
from __future__ import annotations

import io
import json
import logging
import traceback
from typing import Any, Dict, List

import requests as _requests
from nicegui import app, background_tasks, context, run, ui

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

PROMPT_GUIA_NC  = PROMPT_GUIA

PROMPT_GUIA_LHS = """
La primera imagen es la factura del courier LHS (imagen JPG).
La segunda imagen es el invoice del proveedor de BDC Trade Tech LLC (imagen JPG).

De la primera imagen (factura LHS) extraer:
- nro_factura: número de factura argentina formato XXXX-XXXXXXXX
- hawb: número HAWB en la parte superior
- kgs: peso total en kilogramos
- tipo_cambio_1 y tipo_cambio_3: tipos de cambio que aparecen en el documento
- iva_aduanero: OBLIGATORIO, nunca null ni 0. Su etiqueta es "IVA Aduanero". Es el tercer valor
    del recuadro de tributos. Si no encontrás por etiqueta, buscarlo posicionalmente. NUNCA 0.
- flete_aereo: flete internacional en ARS
- almacenaje: almacenaje en ARS
- derechos_importacion: puede ser 0. Buscar posicionalmente: 2 filas arriba de iva_aduanero
    en el recuadro de tributos.
- tasa_estadistica: puede ser 0
- iva_21: valor IVA % 21 en ARS
- total_real: valor "TOTAL" en mayúsculas en ARS
- razon_social: razón social del emisor del documento
- pais_procedencia: buscar "País Origen" o "Pais Origen" en la sección ARCA del documento.
    Si el valor contiene "212" o "ESTADOS UNIDOS" (en cualquier formato) → devolver "USA".
    Devolver null si no aparece.
- fecha: fecha del documento

De la segunda imagen (invoice de BDC Trade Tech LLC) extraer:
- nro_invoice: valor después de "Invoice #" o "Invoice No"
- fob_total: total en USD del invoice
- productos: array con sku (código del proveedor, "" si no figura), descripcion,
    cantidad, precio_unitario, precio_total

Campos que LHS no tiene — dejar SIEMPRE null:
  entrega_domicilio, resolucion_3244, seguro_internacional, servicios_honorarios,
  gastos_administrativos, honorarios, handling, tipo_cambio_2,
  pos_arancelaria, desc_mercaderia

pa: no viene del documento, se inyecta desde la UI. Devolver null.

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
  "gastos_administrativos": null,
  "honorarios": null,
  "handling": null,
  "iva_aduanero": null,
  "iva_21": null,
  "derechos_importacion": null,
  "tasa_estadistica": null,
  "pa": null,
  "total_real": null
}

Respondé SOLO con el JSON, sin texto adicional ni backticks.
"""

PROMPT_GUIA_SIXTAR = """
Analizá este documento de importación de SIXTAR y extraé los siguientes datos en formato JSON.
Si el dato no existe en el documento, ponelo como null.

ESTRUCTURA DEL DOCUMENTO SIXTAR:
- Página 1: factura del courier SIXTAR (datos aduaneros y costos en ARS).
- Página con "Invoice" y "BDC": invoice del proveedor extranjero (en USD, con productos).

razon_social: razón social del courier emisor del documento, en Página 1.

nro_factura: número de factura argentina en Página 1, etiquetado "Factura NRO." o similar.
  Formato XXXX-XXXXXXXX. NUNCA poner el mismo valor en nro_invoice.

nro_invoice: en la página del documento que dice "BDC Trade Tech LLC", buscar la etiqueta
  "Invoice #" o "Invoice No" seguida del número. Extraer solo el valor numérico/alfanumérico
  después del # o los dos puntos. Ejemplo: "Invoice # 7493" → nro_invoice = "7493".
  NUNCA poner el mismo valor en nro_factura.

hawb: en Página 1, parte superior, etiquetado "HAWB" o similar.
  Extraer solo el valor alfanumérico, sin los dos puntos ni espacios.

fecha: fecha del documento en Página 1.

kgs: peso total en kilogramos, etiquetado "Kgs" o similar en Página 1.

tipo_cambio_3: primer tipo de cambio en Página 1, etiquetado "T/Cambio" o "T/C".
tipo_cambio_2: SIEMPRE null — SIXTAR no tiene segundo tipo de cambio independiente.
tipo_cambio_1: segundo tipo de cambio en Página 1 (el que aparece junto al primero, si lo hay).

flete_aereo: en Página 1, etiquetado "Flete Internacional". Valor en ARS.

resolucion_3244: en Página 1, etiquetado "Resolución 3244" o similar. Valor en ARS.
  Devolver null si no aparece.

almacenaje: en Página 1, etiquetado "Almacenaje". Valor en ARS.

servicios_honorarios: en Página 1, etiquetado "Servicios / Honorarios" o similar. Valor en ARS.
  Devolver null si no aparece.

gastos_administrativos: en Página 1, etiquetado "Gastos Administrativos" o "Gs. Administrativos".
  Valor en ARS. Devolver null si no aparece.

honorarios: en Página 1, etiquetado "Honorarios". Valor en ARS. Devolver null si no aparece.
  IMPORTANTE: este campo es distinto de servicios_honorarios.

handling: en Página 1, etiquetado "Handling". Valor en ARS. Devolver null si no aparece.

derechos_importacion: en Página 1, etiquetado "Derechos" o "Derechos de Importación". Valor en ARS.
  Buscar posicionalmente: es el primer valor del grupo de tributos, dos filas arriba de iva_aduanero.
  Puede ser 0 si no aplica.

tasa_estadistica: en Página 1, etiquetado "Estadística" o "Tasa Estadística". Valor en ARS.
  Puede ser 0 si no aplica.

iva_aduanero: en Página 1, etiquetado "IVA % 21" o "IVA Aduanero". Valor en ARS.
  CAMPO OBLIGATORIO — nunca devolver 0 ni null. Si no encontrás el valor, devolver null
  para indicar error de lectura, no 0.

iva_21: en Página 1, etiquetado "IVA % 21". Valor en ARS.
  Si iva_aduanero e iva_21 corresponden al mismo campo del documento, asignar el mismo valor a ambos.

fob_total: total en USD del invoice del proveedor, en la página Invoice/BDC.

productos: array de ítems de la página Invoice/BDC.
  Campos: sku (código del proveedor, "" si no figura), descripcion, cantidad,
  precio_unitario, precio_total.

total_real: valor etiquetado "TOTAL" en mayúsculas en Página 1. Gran total en ARS.

pais_procedencia: en la página de ARCA, buscar el campo "País Origen" o "Pais Origen".
  Si el valor contiene "212" o "ESTADOS UNIDOS" (en cualquier formato, ej: "212-ESTADOS UNIDOS",
  "212 - ESTADOS UNIDOS", "ESTADOS UNIDOS DE AMERICA") → devolver "USA".
  Devolver null si no aparece.
pos_arancelaria: NO buscar en el documento. Devolver null.
desc_mercaderia: NO buscar en el documento. Devolver null.
pa: NO viene del documento — se inyecta desde la UI. Devolver null.

Campos que SIXTAR no incluye — dejar SIEMPRE null:
  entrega_domicilio, seguro_internacional, tipo_cambio_2.

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
  "gastos_administrativos": null,
  "honorarios": null,
  "handling": null,
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
    "gastos_administrativos": "Gastos administrativos",
    "honorarios": "Honorarios",
    "handling": "Handling",
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
    "gastos_administrativos", "honorarios", "handling",
    "iva_aduanero", "iva_21", "derechos_importacion", "tasa_estadistica",
    "pa", "total_real", "courier",
]

_TABLE_HEADERS = [
    "Fecha", "Courier", "Factura", "HAWB", "PA", "Origen", "Invoice Nro",
    "FOB Total", "Peso Total", "Derechos", "Estadística", "IVA Aduanero",
    "Flete Aduanero", "Almacenaje", "Total Factura", "Total real", "Alm/KG", "Valor Kg", "Dolar",
    "Traída u$ s/IVA", "Costo s/IVA", "Total Traída %", "",
]

_TABLE_COLS = (
    "0.7fr 1.4fr 0.9fr 0.8fr 0.5fr 0.8fr 0.9fr "
    "0.7fr 0.7fr 0.8fr 0.8fr 0.8fr 0.8fr minmax(72px,0.7fr) "
    "0.7fr 0.7fr 0.7fr 0.7fr 0.6fr 0.8fr 0.8fr 0.8fr 96px"
)

_SORT_KEYS = {
    "Fecha":            lambda r: r["fecha"] or "",
    "Courier":          lambda r: r.get("courier") or r.get("razon_social") or "",
    "Factura":          lambda r: r["nro_factura"] or "",
    "HAWB":             lambda r: r["hawb"] or "",
    "PA":               lambda r: _to_float(r["pa"]) or 0,
    "Origen":           lambda r: r["pais_procedencia"] or "",
    "Invoice Nro":      lambda r: r["nro_invoice"] or "",
    "FOB Total":        lambda r: _to_float(r["fob_total"]) or 0,
    "Peso Total":       lambda r: _to_float(r["kgs"]) or 0,
    "Derechos":         lambda r: _to_float(r["derechos_importacion"]) or 0,
    "Estadística":      lambda r: _to_float(r["tasa_estadistica"]) or 0,
    "IVA Aduanero":     lambda r: _to_float(r["iva_aduanero"]) or 0,
    "Flete Aduanero":   lambda r: _to_float(r["flete_aereo"]) or 0,
    "Almacenaje":       lambda r: _to_float(r["almacenaje"]) or 0,
    "Total Factura":    lambda r: r["total_factura"] or 0,
    "Total real":       lambda r: _to_float(r["total_real"]) or 0,
    "Alm/KG":           lambda r: r.get("almacenaje_kg") or 0,
    "Valor Kg":         lambda r: _to_float(r["valor_kg"]) or 0,
    "Dolar":            lambda r: _to_float(r["tipo_cambio_3"]) or 0,
    "Traída u$ s/IVA":  lambda r: r["traida_usd"] or 0,
    "Costo s/IVA":      lambda r: r["costo_sin_iva"] or 0,
    "Total Traída %":   lambda r: r["total_traida_pct"] or 0,
}


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
    for col in ("pais_procedencia", "pos_arancelaria", "desc_mercaderia", "fob_total", "pa", "hawb", "iva_21", "total_real", "courier", "gastos_administrativos", "honorarios", "handling"):
        if col not in existing:
            conn.execute(f"ALTER TABLE guias_importacion ADD COLUMN {col} TEXT")
    conn.commit()
    conn.close()


def _save_guia(user_id: int, data: Dict[str, Any]) -> int:
    raw_courier = (data.get("courier") or "").strip()
    if "nc supplies" in raw_courier.lower():
        data = {**data, "courier": "NC Supplies"}
    elif "sixtar" in raw_courier.lower():
        data = {**data, "courier": "Sixtar"}
    elif "lhs" in raw_courier.lower():
        data = {**data, "courier": "LHS"}
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
        "SELECT id, razon_social, courier, hawb, pa, fecha, pais_procedencia, nro_invoice, nro_factura, fob_total, kgs, "
        "derechos_importacion, tasa_estadistica, iva_aduanero, iva_21, flete_aereo, "
        "entrega_domicilio, resolucion_3244, seguro_internacional, servicios_honorarios, "
        "almacenaje, tipo_cambio_1, tipo_cambio_3, total_real, productos, created_at, "
        "gastos_administrativos, honorarios, handling "
        "FROM guias_importacion WHERE user_id = ? ORDER BY created_at DESC",
        (user_id,),
    ).fetchall()
    conn.close()
    result = []
    for r in rows:
        flete = _to_float(r["flete_aereo"])
        kgs   = _to_float(r["kgs"])
        tc3   = _to_float(r["tipo_cambio_3"])
        tc1   = _to_float(r["tipo_cambio_1"])
        tc_for_kg = tc3 if tc3 else tc1
        valor_kg = ""
        if flete and kgs and tc_for_kg and kgs != 0 and tc_for_kg != 0:
            valor_kg = f"{flete / kgs / tc_for_kg:.2f}"

        iva21_val = _to_float(r["iva_21"])
        almacenaje_float = _to_float(r["almacenaje"])
        almacenaje_kg = None
        if almacenaje_float and dolar_blue and dolar_blue != 0 and kgs and kgs != 0:
            almacenaje_kg = almacenaje_float / dolar_blue / kgs
        courier_str = (r["courier"] or r["razon_social"] or "").lower()
        is_sixtar = "sixtar" in courier_str
        if is_sixtar:
            tf_components = [
                ("flete_aereo",            "Flete Internacional",     _to_float(r["flete_aereo"])),
                ("resolucion_3244",        "Resolución 3244",         _to_float(r["resolucion_3244"])),
                ("almacenaje",             "Almacenaje",              _to_float(r["almacenaje"])),
                ("gastos_administrativos", "Gastos Administrativos",  _to_float(r["gastos_administrativos"])),
                ("honorarios",             "Honorarios",              _to_float(r["honorarios"])),
                ("handling",               "Handling",                _to_float(r["handling"])),
                ("derechos_importacion",   "Derechos de Importación", _to_float(r["derechos_importacion"])),
                ("tasa_estadistica",       "Tasa Estadística",        _to_float(r["tasa_estadistica"])),
                ("iva_aduanero",           "IVA Aduanero",            _to_float(r["iva_aduanero"])),
            ]
        else:
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
            "courier": r["courier"] or r["razon_social"] or "",
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


def _update_pa(guia_id: int, user_id: int, new_pa: float) -> None:
    conn = get_connection()
    conn.execute(
        "UPDATE guias_importacion SET pa=? WHERE id=? AND user_id=?",
        (str(new_pa), guia_id, user_id),
    )
    conn.commit()
    conn.close()


def _exists_factura(user_id: int, nro_factura: str, courier: str = "") -> bool:
    conn = get_connection()
    if courier:
        count = conn.execute(
            "SELECT COUNT(*) FROM guias_importacion WHERE user_id=? AND nro_factura=? AND courier=?",
            (user_id, nro_factura, courier),
        ).fetchone()[0]
    else:
        count = conn.execute(
            "SELECT COUNT(*) FROM guias_importacion WHERE user_id=? AND nro_factura=?",
            (user_id, nro_factura),
        ).fetchone()[0]
    conn.close()
    return count > 0


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


def _gemini_vision(api_key: str, data: bytes, mime_type: str, prompt: str | None = None) -> str:
    from google import genai
    from google.genai import types
    if prompt is None:
        prompt = PROMPT_GUIA
    client = genai.Client(api_key=api_key)
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=[
            types.Part.from_bytes(data=data, mime_type=mime_type),
            prompt,
        ],
    )
    return response.text


def _gemini_vision_multi(
    api_key: str,
    data1: bytes, mime1: str,
    data2: bytes, mime2: str,
    prompt: str,
) -> str:
    from google import genai
    from google.genai import types
    client = genai.Client(api_key=api_key)
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=[
            types.Part.from_bytes(data=data1, mime_type=mime1),
            types.Part.from_bytes(data=data2, mime_type=mime2),
            prompt,
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


def _fmt_ars_zero(v) -> str:
    v_str = str(v).strip() if v is not None else ""
    if not v_str or v_str in ("-", "—"):
        return "$0"
    return _fmt_ars(v)


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
    sort_state: list,
) -> None:
    tabla_container.clear()
    rows = _list_guias(user_id)
    sort_col, sort_dir = sort_state
    if sort_col and sort_col in _SORT_KEYS:
        rows.sort(key=_SORT_KEYS[sort_col], reverse=(sort_dir == "desc"))
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
                _hs_base = (
                    "padding:6px 4px;background:#f1f5f9;border-bottom:1px solid #e2e8f0;"
                    "font-size:10px;font-weight:600;"
                    "white-space:normal;word-break:break-word;line-height:1.3;"
                    "min-height:44px;display:flex;align-items:center;justify-content:center;text-align:center"
                )
                _hs = _hs_base + ";color:#6b7280"
                for h in _TABLE_HEADERS:
                    if h and h in _SORT_KEYS:
                        _active = sort_state[0] == h
                        _arrow = (" ↑" if sort_state[1] == "asc" else " ↓") if _active else ""
                        _hc = "#185FA5" if _active else "#6b7280"
                        def _sort_click(col=h):
                            sort_state[1] = "desc" if sort_state[0] == col and sort_state[1] == "asc" else "asc"
                            sort_state[0] = col
                            _rebuild_tabla(user_id, tabla_container, filas_ref, parsed_ref, sort_state)
                        _h_nowrap = ";white-space:nowrap" if h == "Almacenaje" else ""
                        with ui.element("div").style(
                            _hs_base + f";color:{_hc};cursor:pointer;user-select:none{_h_nowrap}"
                        ).on("click", _sort_click):
                            ui.label(h + _arrow).style("pointer-events:none")
                    else:
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

                    # Fecha
                    ui.label(r["fecha"]).style(f"{_ct};white-space:nowrap;text-align:center")
                    # Courier
                    _courier_disp = r.get("courier") or r.get("razon_social") or ""
                    ui.label(_courier_disp).style(
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
                    # PA — chip clickeable para editar
                    with ui.element("div").style(
                        f"display:flex;justify-content:center;align-items:center;padding:3px 4px;{_sep}"
                    ):
                        def _pa_click(rid=rid, hawb=r["hawb"], pa=r["pa"]):
                            _show_edit_pa_dialog(
                                rid, hawb, pa, user_id, tabla_container, filas_ref, parsed_ref, sort_state
                            )
                        with ui.element("div").classes("pa-chip").on("click", _pa_click):
                            ui.label(_fmt_usd(r["pa"])).style("pointer-events:none;font-size:11px;color:#0C447C")
                            ui.html('<i class="ti ti-pencil" style="pointer-events:none;font-size:11px;opacity:0.7;color:#0C447C"></i>')
                    # Origen — ESTADOS UNIDOS / 212 → USA
                    _origen = r["pais_procedencia"]
                    if _origen and ("estados uni" in _origen.lower() or "212" in _origen):
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
                    ui.label(_fmt_ars_zero(r["derechos_importacion"])).style(
                        f"{_ct};white-space:nowrap;text-align:right"
                    )
                    # Estadística
                    ui.label(_fmt_ars_zero(r["tasa_estadistica"])).style(
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
                                rid, user_id, tabla_container, filas_ref, parsed_ref, sort_state
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
                                    _h_extra = ";color:#185FA5;font-weight:500" if _h == "Costo Imp. u$s/IVA" else ""
                                    ui.label(_h).style(_sh2 + _h_extra)
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
                                    ).style(f"{_sp2};text-align:right;color:#185FA5;font-weight:500")


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
    rid: int, user_id: int, tabla_container, filas_ref: list, parsed_ref: list, sort_state: list
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
                _rebuild_tabla(user_id, tabla_container, filas_ref, parsed_ref, sort_state)
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


def _show_edit_pa_dialog(
    rid: int, hawb: str, pa_current: str, user_id: int,
    tabla_container, filas_ref: list, parsed_ref: list, sort_state: list,
) -> None:
    pa_val = _to_float(pa_current) or 0.0
    with ui.dialog() as d, ui.card().style("padding:24px;min-width:320px"):
        with ui.row().classes("items-center gap-2").style("margin-bottom:16px"):
            ui.html('<i class="ti ti-adjustments-horizontal" style="color:#185FA5;font-size:18px"></i>')
            ui.label(f"Editar PA — {hawb}").style(
                "font-size:14px;font-weight:600;color:#374151"
            )
        ui.label("Valor PA (u$s)").style(
            "font-size:12px;color:#6b7280;margin-bottom:4px;display:block"
        )
        pa_input = ui.number(value=pa_val, min=0).props("dense outlined").style("width:100%")
        ui.label(
            "Recalcula: Traída u$s, Total Traída %, Costo s/IVA y Costo Imp. por producto."
        ).style("font-size:11px;color:#9ca3af;margin-top:6px;display:block")
        with ui.row().classes("gap-2").style("margin-top:16px;justify-content:flex-end"):
            ui.button("Cancelar", on_click=d.close).props("flat")
            def _guardar(d=d):
                new_val = pa_input.value
                if new_val is None or new_val < 0:
                    ui.notify("Ingresá un valor válido >= 0", color="warning")
                    return
                _update_pa(rid, user_id, new_val)
                d.close()
                ui.notify("PA actualizado", color="positive")
                _rebuild_tabla(user_id, tabla_container, filas_ref, parsed_ref, sort_state)
            ui.button("Guardar y recalcular", on_click=_guardar).props("flat").style(
                "color:#185FA5;font-weight:600"
            )
    d.open()


# ── Courier panel builder ─────────────────────────────────────────────────────

def _build_courier_panel(
    courier_name: str,
    courier_key: str,
    prompt_str: str,
    user_id: int,
    tabla_ref: list,
    filas_ref: list,
    parsed_ref: list,
    sort_state: list,
    pa_default: int = 200,
) -> None:
    logger.warning("[DBG] _build_courier_panel START courier=%s", courier_key)
    archivo_data: list = [None]
    archivo_mime: list = [None]
    uploader_ref: list = [None]
    spin_ref: list = [None]
    resultado_ref: list = [None]

    with ui.element("div").style(
        "display:flex;flex-direction:column;"
        "background:var(--color-background-secondary);"
        "border:0.5px solid var(--color-border-tertiary);"
        "border-radius:6px;padding:8px"
    ):
        ui.label(courier_name).style(
            "font-size:11px;font-weight:500;color:var(--color-text-secondary);"
            "margin-bottom:6px;display:block"
        )

        def _on_upload(e):
            try:
                e.content.seek(0)
                archivo_data[0] = e.content.read()
                archivo_mime[0] = e.type
                logger.warning("[DBG] _on_upload OK courier=%s len=%d mime=%s", courier_key, len(archivo_data[0]), e.type)
            except Exception as _ue:
                logger.warning("[DBG] _on_upload ERROR courier=%s: %s\n%s", courier_key, _ue, traceback.format_exc())

        _uploader = ui.upload(
            label="Subir PDF/IMG",
            on_upload=_on_upload,
            auto_upload=True,
            max_files=1,
            max_file_size=20_000_000,
        ).props('accept=".pdf,.jpg,.jpeg,.png" flat bordered').style("width:100%")
        uploader_ref[0] = _uploader

        ui.element("div").style("flex:1")

        ui.element("div").style(
            "height:0.5px;background:var(--color-border-tertiary);margin:5px 0"
        )

        with ui.element("div").style("display:flex;justify-content:center;margin-bottom:6px"):
            pa_select = ui.select(
                options=[0, 100, 150, 200, 250, 300],
                value=pa_default,
                label="PA",
            ).props("dense outlined").style("width:80px;font-size:12px")

        client = context.client

        async def _analizar(usar_gemini: bool) -> None:
            logger.warning("[DBG] _analizar courier=%s gemini=%s", courier_key, usar_gemini)
            if not archivo_data[0]:
                client.run_javascript(
                    "Quasar.Notify.create({message:'Primero subí un archivo',"
                    "color:'warning',position:'bottom'})"
                )
                return
            groq_key = get_app_config("groq_api_key")
            gemini_key = get_app_config("gemini_api_key")
            es_imagen = archivo_mime[0] and archivo_mime[0].startswith("image/")
            logger.warning("[DBG] archivo len=%d mime=%s", len(archivo_data[0]) if archivo_data[0] else 0, archivo_mime[0])
            if usar_gemini and not gemini_key:
                client.run_javascript(
                    "Quasar.Notify.create({message:'Configurá tu API key de Gemini en Config \\u2192 IA/Sugerencias',"
                    "color:'warning',position:'bottom'})"
                )
                return
            if not usar_gemini and not groq_key:
                client.run_javascript(
                    "Quasar.Notify.create({message:'Configurá tu API key de Grok en Config \\u2192 IA/Sugerencias',"
                    "color:'warning',position:'bottom'})"
                )
                return
            if not usar_gemini and es_imagen:
                client.run_javascript(
                    "Quasar.Notify.create({message:'Grok solo procesa PDFs con texto. Usá Gemini para imágenes.',"
                    "color:'info',position:'bottom'})"
                )
                return
            spin_ref[0].set_visibility(True)
            resultado_ref[0].set_text("")
            filas_ref[0].clear()
            try:
                if usar_gemini:
                    logger.warning("[DBG] Llamando _gemini_vision courier=%s", courier_key)
                    raw = await run.io_bound(
                        _gemini_vision, gemini_key, archivo_data[0], archivo_mime[0], prompt_str
                    )
                    logger.warning("[DBG] raw IA (500): %s", raw[:500] if raw else "None")
                else:
                    texto_pdf = await run.io_bound(_extract_pdf_text, archivo_data[0])
                    if not texto_pdf.strip():
                        client.run_javascript(
                            "Quasar.Notify.create({message:'No se pudo extraer texto del PDF. Probá con Gemini.',"
                            "color:'warning',position:'bottom'})"
                        )
                        return
                    full_prompt = prompt_str + "\n\nCONTENIDO DEL DOCUMENTO:\n" + texto_pdf
                    logger.warning("[DBG] Llamando _groq_parse_doc courier=%s", courier_key)
                    raw = await run.io_bound(_groq_parse_doc, groq_key, full_prompt)
                    logger.warning("[DBG] raw Grok (500): %s", raw[:500] if raw else "None")
                raw = _clean_json(raw)
                logger.warning("[DBG] JSON limpio (500): %s", raw[:500] if raw else "None")
                try:
                    parsed = json.loads(raw)
                    logger.warning("[DBG] parsed keys: %s", list(parsed.keys()))
                    parsed["pa"] = pa_select.value
                    parsed["courier"] = courier_key
                    parsed_ref[0] = parsed
                    logger.warning("[DBG] pa=%s tc1=%s tc3=%s courier=%s", parsed.get("pa"), parsed.get("tipo_cambio_1"), parsed.get("tipo_cambio_3"), parsed.get("courier"))
                    nro_fac = (parsed.get("nro_factura") or "").strip()
                    if nro_fac and _exists_factura(user_id, nro_fac, courier_key):
                        _msg_dup = json.dumps(f"La factura {nro_fac} ya fue ingresada.")
                        client.run_javascript(
                            f"Quasar.Notify.create({{message:{_msg_dup},"
                            "color:'warning',icon:'warning',position:'bottom'})"
                        )
                    else:
                        filas_ref[0].clear()
                        logger.warning("[DBG] Llamando _save_guia courier=%s", courier_key)
                        _save_guia(user_id, parsed)
                        logger.warning("[DBG] _save_guia OK courier=%s", courier_key)
                        logger.warning("[DBG] Llamando _rebuild_tabla courier=%s", courier_key)
                        _rebuild_tabla(user_id, tabla_ref[0], filas_ref, parsed_ref, sort_state)
                        logger.warning("[DBG] _rebuild_tabla OK courier=%s", courier_key)
                        client.run_javascript(
                            "Quasar.Notify.create({message:'Guía agregada automáticamente',"
                            "color:'positive',position:'bottom'})"
                        )
                        archivo_data[0] = None
                        archivo_mime[0] = None
                        uploader_ref[0].reset()
                except json.JSONDecodeError as jde:
                    tb_str = traceback.format_exc()
                    logger.warning("[DBG] JSONDecodeError courier=%s: %s\n%s", courier_key, jde, tb_str)
                    resultado_ref[0].set_text("Error: JSON inválido")
            except Exception as exc:
                tb_str = traceback.format_exc()
                logger.warning("[DBG] ERROR courier=%s: %s\n%s", courier_key, exc, tb_str)
                logger.error("Error analizando guía (%s): %s\n%s", courier_key, exc, tb_str)
                _msg_exc = json.dumps(f"Error: {exc}")
                client.run_javascript(
                    f"Quasar.Notify.create({{message:{_msg_exc},color:'negative',position:'bottom'}})"
                )
            finally:
                spin_ref[0].set_visibility(False)

        def _click_grok():
            background_tasks.create(_analizar(False), name=f"analizar_{courier_key}_grok")

        def _click_gemini():
            background_tasks.create(_analizar(True), name=f"analizar_{courier_key}_gemini")

        with ui.element("div").style(
            "display:flex;justify-content:center;gap:4px;align-items:center;flex-wrap:wrap"
        ):
            ui.button("Grok", icon="bolt", on_click=_click_grok).props("flat dense").style(
                "border:1px solid #BA7517;color:#633806;background:#FAEEDA;font-size:11px"
            )
            ui.button("Gemini", icon="auto_awesome", on_click=_click_gemini).props("flat dense").style(
                "border:1px solid #534AB7;color:#26215C;background:#EEEDFE;font-size:11px"
            )
            spin = ui.spinner(size="sm").classes("text-blue-500")
            spin.set_visibility(False)
            spin_ref[0] = spin

        resultado_txt = ui.label("").style(
            "font-size:12px;color:#16a34a;font-weight:500;text-align:center;margin-top:2px"
        )
        resultado_ref[0] = resultado_txt

    logger.warning("[DBG] _build_courier_panel END courier=%s", courier_key)


# ── LHS panel (dos uploaders: Factura LHS + Invoice BDC) ─────────────────────

def _build_lhs_panel(
    user_id: int,
    tabla_ref: list,
    filas_ref: list,
    parsed_ref: list,
    sort_state: list,
) -> None:
    archivo_data_lhs1: list = [None]
    archivo_mime_lhs1: list = [None]
    archivo_data_lhs2: list = [None]
    archivo_mime_lhs2: list = [None]
    uploader_ref1: list = [None]
    uploader_ref2: list = [None]
    spin_ref: list = [None]
    resultado_ref: list = [None]

    with ui.element("div").style(
        "display:flex;flex-direction:column;"
        "background:var(--color-background-secondary);"
        "border:0.5px solid var(--color-border-tertiary);"
        "border-radius:6px;padding:8px"
    ):
        ui.label("LHS").style(
            "font-size:11px;font-weight:500;color:var(--color-text-secondary);"
            "margin-bottom:6px;display:block"
        )

        def _on_upload1(e):
            try:
                e.content.seek(0)
                archivo_data_lhs1[0] = e.content.read()
                archivo_mime_lhs1[0] = e.type
            except Exception as _ue:
                logger.error("_on_upload LHS Factura: %s", _ue)

        def _on_upload2(e):
            try:
                e.content.seek(0)
                archivo_data_lhs2[0] = e.content.read()
                archivo_mime_lhs2[0] = e.type
            except Exception as _ue:
                logger.error("_on_upload LHS Invoice: %s", _ue)

        with ui.element("div").style("display:grid;grid-template-columns:1fr 1fr;gap:6px"):
            with ui.element("div").style("display:flex;flex-direction:column;gap:3px"):
                ui.label("Factura LHS").style(
                    "font-size:9px;color:var(--color-text-tertiary);font-weight:500"
                )
                _uploader1 = ui.upload(
                    on_upload=_on_upload1,
                    auto_upload=True,
                    max_files=1,
                    max_file_size=20_000_000,
                ).props('accept=".pdf,.jpg,.jpeg,.png" flat bordered').style(
                    "width:100%;--q-primary:#185FA5"
                )
                uploader_ref1[0] = _uploader1

            with ui.element("div").style("display:flex;flex-direction:column;gap:3px"):
                ui.label("Invoice BDC").style(
                    "font-size:9px;color:var(--color-text-tertiary);font-weight:500"
                )
                _uploader2 = ui.upload(
                    on_upload=_on_upload2,
                    auto_upload=True,
                    max_files=1,
                    max_file_size=20_000_000,
                ).props('accept=".pdf,.jpg,.jpeg,.png" flat bordered').style(
                    "width:100%;--q-primary:#2176AE"
                )
                uploader_ref2[0] = _uploader2

        ui.element("div").style("flex:1")

        ui.element("div").style(
            "height:0.5px;background:var(--color-border-tertiary);margin:5px 0"
        )

        with ui.element("div").style("display:flex;justify-content:center;margin-bottom:6px"):
            pa_select = ui.select(
                options=[0, 100, 150, 200, 250, 300],
                value=200,
                label="PA",
            ).props("dense outlined").style("width:80px;font-size:12px")

        client = context.client

        async def _analizar_lhs(usar_gemini: bool) -> None:
            if not archivo_data_lhs1[0]:
                client.run_javascript(
                    "Quasar.Notify.create({message:'Falta subir la Factura LHS',"
                    "color:'warning',position:'bottom'})"
                )
                return
            if not archivo_data_lhs2[0]:
                client.run_javascript(
                    "Quasar.Notify.create({message:'Falta subir el Invoice BDC',"
                    "color:'warning',position:'bottom'})"
                )
                return
            if not usar_gemini:
                client.run_javascript(
                    "Quasar.Notify.create({message:'LHS solo soporta análisis con Gemini (imágenes JPG/PNG)',"
                    "color:'warning',position:'bottom'})"
                )
                return
            gemini_key = get_app_config("gemini_api_key")
            if not gemini_key:
                client.run_javascript(
                    "Quasar.Notify.create({message:'Configurá tu API key de Gemini en Config \\u2192 IA/Sugerencias',"
                    "color:'warning',position:'bottom'})"
                )
                return

            spin_ref[0].set_visibility(True)
            resultado_ref[0].set_text("")
            filas_ref[0].clear()

            try:
                raw = await run.io_bound(
                    _gemini_vision_multi,
                    gemini_key,
                    archivo_data_lhs1[0], archivo_mime_lhs1[0],
                    archivo_data_lhs2[0], archivo_mime_lhs2[0],
                    PROMPT_GUIA_LHS,
                )
                raw = _clean_json(raw)
                try:
                    parsed = json.loads(raw)
                    parsed["pa"] = pa_select.value
                    parsed["courier"] = "LHS"
                    parsed_ref[0] = parsed
                    nro_fac = (parsed.get("nro_factura") or "").strip()
                    if nro_fac and _exists_factura(user_id, nro_fac, "LHS"):
                        _msg_dup = json.dumps(f"La factura {nro_fac} ya fue ingresada.")
                        client.run_javascript(
                            f"Quasar.Notify.create({{message:{_msg_dup},"
                            "color:'warning',icon:'warning',position:'bottom'})"
                        )
                    else:
                        filas_ref[0].clear()
                        _save_guia(user_id, parsed)
                        _rebuild_tabla(user_id, tabla_ref[0], filas_ref, parsed_ref, sort_state)
                        client.run_javascript(
                            "Quasar.Notify.create({message:'Guía agregada automáticamente',"
                            "color:'positive',position:'bottom'})"
                        )
                        archivo_data_lhs1[0] = None
                        archivo_mime_lhs1[0] = None
                        archivo_data_lhs2[0] = None
                        archivo_mime_lhs2[0] = None
                        uploader_ref1[0].reset()
                        uploader_ref2[0].reset()
                except json.JSONDecodeError as jde:
                    logger.error("JSONDecodeError LHS: %s\n%s", jde, traceback.format_exc())
                    resultado_ref[0].set_text("Error: JSON inválido")
            except Exception as exc:
                logger.error("Error analizando guía LHS: %s\n%s", exc, traceback.format_exc())
                _msg_exc = json.dumps(f"Error: {exc}")
                client.run_javascript(
                    f"Quasar.Notify.create({{message:{_msg_exc},color:'negative',position:'bottom'}})"
                )
            finally:
                spin_ref[0].set_visibility(False)

        def _click_grok():
            background_tasks.create(_analizar_lhs(False), name="analizar_LHS_grok")

        def _click_gemini():
            background_tasks.create(_analizar_lhs(True), name="analizar_LHS_gemini")

        with ui.element("div").style(
            "display:flex;justify-content:center;gap:4px;align-items:center;flex-wrap:wrap"
        ):
            ui.button("Grok", icon="bolt", on_click=_click_grok).props("flat dense").style(
                "border:1px solid #BA7517;color:#633806;background:#FAEEDA;font-size:11px"
            )
            ui.button("Gemini", icon="auto_awesome", on_click=_click_gemini).props("flat dense").style(
                "border:1px solid #534AB7;color:#26215C;background:#EEEDFE;font-size:11px"
            )
            spin = ui.spinner(size="sm").classes("text-blue-500")
            spin.set_visibility(False)
            spin_ref[0] = spin

        resultado_txt = ui.label("").style(
            "font-size:12px;color:#16a34a;font-weight:500;text-align:center;margin-top:2px"
        )
        resultado_ref[0] = resultado_txt


# ── Tab principal ─────────────────────────────────────────────────────────────

def build_tab_guias() -> None:
    user = app.storage.user.get("user")
    if not user:
        ui.label("Debes iniciar sesión").classes("text-red-500 p-4")
        return

    user_id = user["id"]
    ui.add_css("""
.pa-chip {
    background:#E6F1FB;border:1px solid #85B7EB;color:#0C447C;
    border-radius:4px;padding:2px 7px;cursor:pointer;
    display:inline-flex;align-items:center;gap:3px;
    transition:background 0.15s;user-select:none;
}
.pa-chip:hover { background:#B5D4F4 !important; }
""")
    _init_guias_db()

    filas_ref: list = [None]
    tabla_ref: list = [None]
    sort_state: list = [None, "asc"]
    parsed_ref: list = [None]

    # ── Panel superior: tres couriers side by side ────────────────────────────
    logger.warning("[DBG] build_tab_guias: construyendo paneles courier user_id=%s", user_id)
    with ui.element("div").style(
        "margin:16px 20px 0;display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px"
    ):
        logger.warning("[DBG] build_tab_guias: panel NC SUPPLIES...")
        _build_courier_panel(
            "NC Supplies", "NC SUPPLIES", PROMPT_GUIA_NC,
            user_id, tabla_ref, filas_ref, parsed_ref, sort_state,
            pa_default=250,
        )
        logger.warning("[DBG] build_tab_guias: panel SIXTAR...")
        _build_courier_panel(
            "Sixtar", "SIXTAR", PROMPT_GUIA_SIXTAR,
            user_id, tabla_ref, filas_ref, parsed_ref, sort_state,
            pa_default=150,
        )
        logger.warning("[DBG] build_tab_guias: panel LHS...")
        _build_lhs_panel(
            user_id, tabla_ref, filas_ref, parsed_ref, sort_state,
        )
    logger.warning("[DBG] build_tab_guias: paneles OK")

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
        _rebuild_tabla(user_id, tabla_container, filas_ref, parsed_ref, sort_state)
