"""
tabs/gastos.py — Gestión de documentos impositivos mensuales por sección.
Funciones exportadas: build_tab_gastos, procesar_archivo_con_gemini
"""
from __future__ import annotations

import base64
import json
import re
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from nicegui import app, run, ui

from db import (
    delete_gastos_archivo,
    get_app_config,
    get_gastos_archivos,
    get_gastos_prompt,
    insert_gastos_archivo,
    mark_gastos_procesado,
    update_gastos_extraccion,
    upsert_gastos_prompt,
)

_BLUE       = "#2A7AC7"
_BLUE_BG    = "#EEF6FD"
_HDR_BG     = "#EEF6FD"
_HDR_COLOR  = "#185FA5"
_HDR_BORDER = "#85B7EB"
_GREEN      = "#3B6D11"
_YELLOW     = "#E2A93B"
_GRAY       = "#9E9E9E"
_RED        = "#A32D2D"

_DOT = "display:inline-block;width:12px;height:12px;border-radius:9999px;flex-shrink:0;background:{}"

_PROMPT_PRE_STYLE = (
    "font-size:11px;line-height:1.5;white-space:pre-wrap;word-break:break-word;"
    "min-height:120px;max-height:none;height:auto;overflow-y:auto;"
    "padding:8px 10px;"
    "border:0.5px dashed #ccc;background:#f9f9f9;border-radius:4px;"
    "margin:0;font-family:inherit"
)


def _escape_prompt_html(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

_SECCIONES: List[tuple] = [
    ("facturas_ml",  "Facturas MercadoLibre",  ".pdf",  True,  "ti-file-invoice"),
    ("retenciones",  "Retenciones",             ".xlsx", True,  "ti-file-spreadsheet"),
    ("percepciones", "Percepciones",            ".xlsx", True,  "ti-file-spreadsheet"),
    ("pagos_arca",   "Pagos ARCA",              ".pdf",  True,  "ti-file-invoice"),
    ("reportes_ml",  "Reportes MercadoLibre",   ".xlsx", True,  "ti-file-spreadsheet"),
]

_MESES = [
    "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
    "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre",
]

_BASE_PATH = Path(__file__).parent.parent / "gastos"

_PROMPTS_DEFAULT: Dict[str, str] = {
    "facturas_ml": (
        "Analizá esta factura de MercadoLibre y extraé en JSON puro (sin markdown ni bloques de código). "
        "Campos obligatorios: tipo_documento (Factura A/B/C), emisor (razón social), cuit_emisor, "
        "receptor (razón social), cuit_receptor, fecha (DD/MM/AAAA), punto_venta, nro_comprobante, "
        "lineas_antes_subtotal (lista con TODOS los conceptos que aparezcan ANTES del subtotal — "
        "cargos, bonificaciones, servicios, comisiones, etc. — cada uno con concepto y monto numérico; "
        "si una bonificación tiene signo negativo en el documento, devolvé el monto negativo; "
        "si no hay ningún concepto antes del subtotal, devolvé array vacío []), "
        "subtotal (número), "
        "lineas_intermedias (lista con TODOS los conceptos entre Subtotal y Total — "
        "IVA 21%, IVA 10.5%, Percepciones IIBB, Percepción IVA, otros impuestos, descuentos, etc. — "
        "extraé TODAS sin omitir ninguna, cada una con nombre exacto y monto numérico), "
        "total (número), cae, cae_vto (DD/MM/AAAA). "
        "CRÍTICO: Es OBLIGATORIO extraer TODAS las líneas del documento, tanto antes como después del subtotal. "
        "Las líneas antes del subtotal pueden incluir cargos por uso de plataforma, bonificaciones, servicios, etc. "
        "Las líneas entre Subtotal y Total pueden ser 1 sola o hasta 15 líneas distintas, incluyendo: "
        "- Todas las percepciones de IIBB de distintas provincias (Corrientes, Catamarca, CABA, Tucumán, "
        "La Pampa, Neuquén, Buenos Aires, etc.) "
        "- Percepciones IVA de distintas jurisdicciones "
        "- IVA 21% e IVA 10,5% "
        "- Retenciones si las hubiere "
        "- Cualquier otro concepto que aparezca listado entre Subtotal y Total. "
        "NO OMITAS NINGUNA. Si tenés dudas sobre si una línea corresponde, INCLUÍLA. "
        "Es preferible incluir de más que omitir alguna. "
        "Extraé el nombre EXACTAMENTE como aparece en el PDF, sin abreviar ni traducir. "
        'Formato esperado: {"tipo_documento":"Factura A","emisor":"MercadoLibre S.R.L.",'
        '"cuit_emisor":"...","receptor":"...","cuit_receptor":"...","fecha":"DD/MM/AAAA",'
        '"punto_venta":"...","nro_comprobante":"...",'
        '"lineas_antes_subtotal":[{"concepto":"Cargos por uso de la plataforma Mercado Libre","monto":23976326.91},'
        '{"concepto":"Bonificaciones uso plataforma Mercado Libre","monto":-1171322.59}],'
        '"subtotal":0.0,'
        '"lineas_intermedias":[{"concepto":"IVA 21%","monto":0.0},{"concepto":"PERCEPCION IIBB CORRIENTES","monto":0.0}],'
        '"total":0.0,"cae":"...","cae_vto":"DD/MM/AAAA"}'
    ),
    "retenciones": (
        "Analizá este Excel de retenciones. Extraé en JSON puro (sin markdown ni bloques de código). "
        "De la CABECERA del Excel extraé: "
        "usuario (ej: 'NORTHTECHNOLOGY'), "
        "impuesto (ej: 'Impuesto a los IIBB Corrientes'), "
        "fecha_desde (del campo 'Intervalo de fechas consultadas', formato DD/MM/AAAA), "
        "fecha_hasta (del mismo campo, formato DD/MM/AAAA). "
        "De la TABLA de detalle, mirá la columna 'Alícuota': "
        "si el valor es el MISMO en todas las filas, extraé ese valor único. "
        "Si varía, extraé el rango como 'min-max'. "
        "Del BLOQUE DE TOTALES extraé: "
        "base_imponible (número), "
        "importe_retenido (número), "
        "importe_devuelto (número). "
        "IMPORTANTE: NO extraer las filas individuales de la tabla — solo los totales agregados y la alícuota. "
        "El JSON debe ser ESTRICTAMENTE válido. Usá SOLO comillas dobles. "
        "Responde ÚNICAMENTE con el JSON. "
        'Ejemplo: {"usuario":"NORTHTECHNOLOGY","impuesto":"Impuesto a los IIBB Corrientes",'
        '"fecha_desde":"01/04/2026","fecha_hasta":"01/05/2026","alicuota":"2,00 %",'
        '"base_imponible":869973.24,"importe_retenido":17399.46,"importe_devuelto":3777.96}'
    ),
    "percepciones": (
        "Analizá este Excel de percepciones. Extraé en JSON puro (sin markdown ni bloques de código). "
        "De la CABECERA del Excel extraé: "
        "usuario (ej: 'NORTHTECHNOLOGY'), "
        "impuesto (ej: 'Impuesto a los IIBB Corrientes - Percepciones'), "
        "condicion_fiscal (ej: 'Responsable Inscripto', tal como aparece en la cabecera), "
        "fecha_desde (del campo 'Intervalo de fechas consultadas', formato DD/MM/AAAA), "
        "fecha_hasta (del mismo campo, formato DD/MM/AAAA). "
        "De la TABLA de detalle, mirá la columna 'Alícuota': "
        "si el valor es el MISMO en todas las filas, extraé ese valor único. "
        "Si varía, extraé el rango como 'min-max'. "
        "CRÍTICO: base_imponible es la SUMA de TODOS los valores de la columna 'Base imponible' de la tabla "
        "de detalle. NO uses un valor individual — sumá todas las filas. Lo mismo para monto_percibido: "
        "es la SUMA de TODOS los valores de la columna 'Monto percibido' (o 'Importe percibido', "
        "según el nombre que aparezca en el Excel). "
        "IMPORTANTE: NO extraer las filas individuales de la tabla — solo los agregados sumados y la alícuota. "
        "El JSON debe ser ESTRICTAMENTE válido. Usá SOLO comillas dobles. "
        "Responde ÚNICAMENTE con el JSON. "
        'Ejemplo: {"usuario":"NORTHTECHNOLOGY","impuesto":"Impuesto a los IIBB Corrientes",'
        '"condicion_fiscal":"Responsable Inscripto",'
        '"fecha_desde":"01/04/2026","fecha_hasta":"01/05/2026","alicuota":"2,00 %",'
        '"base_imponible":869973.24,"monto_percibido":17399.46}'
    ),
    "pagos_arca": (
        "Analizá este comprobante de pago ARCA/AFIP. Identificá el tipo de documento y extraé en JSON puro "
        "(sin markdown ni bloques de código) TODOS los conceptos financieros con nombre EXACTO y monto numérico. "
        "Campos comunes (siempre extraer): "
        "tipo (IVA | SIFERE Convenio Multilateral | texto descriptivo del documento), "
        "periodo (AAAA-MM), "
        "numero_vep. "
        "Si el documento es SIFERE Convenio Multilateral: "
        "lineas_convenio_multilateral (lista con TODAS las líneas CM que aparezcan antes del importe total — "
        "cada una con jurisdiccion (nombre de la provincia sin el prefijo CM), "
        "codigo (número entre paréntesis) y monto numérico; si no hay, devolvé []). "
        "Si el documento es IVA: "
        "determinacion_del_impuesto (lista de {concepto, monto} con TODOS los ítems de la sección "
        "Determinación del Impuesto, en el orden en que aparecen), "
        "determinacion_posicion_mensual (lista de {concepto, monto} con TODOS los ítems de la sección "
        "Determinación de la Posición Mensual, en el orden en que aparecen). "
        "En cualquier caso, si aparece explícitamente en el documento: "
        "importe_total_a_pagar (número). "
        "CRÍTICO: extraé los nombres EXACTAMENTE como aparecen en el documento, sin abreviar ni traducir. "
        "No omitas ningún concepto. Si un grupo no aparece en el documento, omití esa clave. "
        "El JSON debe ser ESTRICTAMENTE válido — solo comillas dobles, sin trailing commas ni comentarios. "
        "Responde ÚNICAMENTE con el JSON. "
        'Ejemplo SIFERE: {"tipo":"SIFERE Convenio Multilateral","periodo":"2026-04",'
        '"numero_vep":"1629093052",'
        '"lineas_convenio_multilateral":['
        '{"jurisdiccion":"PCIA BS AS","codigo":"5802","monto":2244403.49},'
        '{"jurisdiccion":"CHACO","codigo":"5806","monto":7087.99}],'
        '"importe_total_a_pagar":3383401.10} '
        'Ejemplo IVA: {"tipo":"IVA","periodo":"2026-04","numero_vep":"1172911737",'
        '"determinacion_del_impuesto":['
        '{"concepto":"Total del Débito Fiscal del Período","monto":19292005.21},'
        '{"concepto":"Total del Crédito Fiscal del Período","monto":21863249.64},'
        '{"concepto":"Saldo técnico a favor del contribuyente del período anterior","monto":12105024.60},'
        '{"concepto":"Saldo técnico a favor del contribuyente","monto":14676269.03}],'
        '"determinacion_posicion_mensual":['
        '{"concepto":"Saldo técnico a favor de ARCA","monto":0.00},'
        '{"concepto":"Saldo técnico a favor del contribuyente","monto":14676269.03},'
        '{"concepto":"Saldo a favor de libre disponibilidad del período anterior neto de usos","monto":1727.75},'
        '{"concepto":"Total de retenciones, percepciones y pagos a cuenta neto de restituciones","monto":1534.81},'
        '{"concepto":"Saldo de libre disponibilidad a favor del contribuyente del período","monto":3262.56}],'
        '"importe_total_a_pagar":0.0}'
    ),
    "reportes_ml": (
        "Analizá este reporte de operaciones de MercadoLibre. Identificá las columnas principales y "
        "resumí las primeras 5 filas. Devolvé JSON puro con keys: columnas (lista), resumen_filas (lista de dicts)."
    ),
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _require_login() -> Optional[Dict[str, Any]]:
    user = app.storage.user.get("user")
    if not user:
        ui.notify("Debes iniciar sesión", color="negative")
    return user


def _fmt_size(n: int) -> str:
    if n >= 1_048_576:
        return f"{n / 1_048_576:.1f} MB"
    if n >= 1024:
        return f"{n / 1024:.0f} KB"
    return f"{n} B"


def _semaforo_color(archivos: list) -> str:
    if not archivos:
        return _GRAY
    statuses = [f.get("extraction_status", "pendiente") for f in archivos]
    if all(s == "procesado" for s in statuses):
        return _GREEN
    return _YELLOW


def _badge_html(status: str) -> str:
    _STYLES = {
        "pendiente": f"background:#FBF1DC;color:#7A5A0E;border:1px solid {_YELLOW}",
        "procesado": f"background:#EAF3DE;color:#27500A;border:1px solid {_GREEN}",
        "error":     f"background:#FBE9E9;color:#7A1414;border:1px solid {_RED}",
    }
    _LABELS = {"pendiente": "Pendiente", "procesado": "Procesado", "error": "Error"}
    sty = _STYLES.get(status, _STYLES["pendiente"])
    lbl = _LABELS.get(status, status)
    return (
        f'<span style="font-size:9px;padding:1px 5px;border-radius:3px;'
        f'white-space:nowrap;{sty}">{lbl}</span>'
    )


def _pdf_first_page_b64(path: Path) -> Optional[str]:
    try:
        import fitz
        doc = fitz.open(str(path))
        pix = doc[0].get_pixmap(matrix=fitz.Matrix(1.5, 1.5))
        data = pix.tobytes("png")
        doc.close()
        return base64.b64encode(data).decode()
    except Exception:
        return None


_EXCEL_PREVIEW_HEADERS_DETALLE = (
    "número de venta", "n° de venta", "referencia externa", "detalle de movimientos",
    "fecha del cargo",
)


def _cortar_en_detalle(filas: list) -> int:
    """Índice de la primera fila que pertenece a la tabla de detalle (headers de columnas o de
    agrupamiento tipo "Información sobre..."), para no incluirla ni lo que sigue."""
    for i, linea in enumerate(filas):
        celdas = linea.split("\t")
        primera_celda = celdas[0].strip().lower()
        if any(primera_celda.startswith(h) for h in _EXCEL_PREVIEW_HEADERS_DETALLE):
            return i
        info_count = sum(1 for c in celdas if "información sobre" in c.strip().lower())
        if info_count >= 2:
            return i
    return len(filas)


def _excel_preview_html(path: Path) -> str:
    """Preview acotado: solo metadatos + totales, sin la tabla de detalle completa."""
    try:
        filas = [f for f in leer_excel_completo(path) if not f.startswith("=== HOJA:")]
        if not filas:
            return "<p style='font-size:11px;color:#9e9e9e'>Excel vacío</p>"

        corte = _cortar_en_detalle(filas)
        preview = []
        for linea in filas[:corte]:
            celdas = linea.split("\t")
            # Filas banner/párrafo (una sola celda con texto largo, p.ej. leyendas legales) → ruido, saltar
            no_vacias = [c for c in celdas if c.strip()]
            if len(no_vacias) <= 1 and (not no_vacias or len(no_vacias[0]) > 40):
                continue
            preview.append(linea)
            if len(preview) >= 12:
                break

        titulo = path.stem.replace("_", " ").replace("-", " ").strip()
        titulo_html = (
            '<div style="font-size:12px;font-weight:700;color:#333;'
            'white-space:normal;word-break:break-word;line-height:1.3;padding:8px 10px">'
            f"{titulo}</div>"
        )

        body = ""
        for linea in preview:
            tds = "".join(
                f'<td style="border:1px solid #e0e0e0;padding:2px 6px;white-space:nowrap">{c}</td>'
                for c in linea.split("\t")
            )
            body += f"<tr>{tds}</tr>"
        tabla_html = (
            '<table style="border-collapse:collapse;font-size:11px;width:100%">'
            f"<tbody>{body}</tbody></table>"
        )
        return titulo_html + tabla_html
    except Exception as exc:
        return f"<p style='font-size:11px;color:#a32d2d'>Error al leer Excel: {exc}</p>"


def leer_excel_completo(path: Path) -> list[str]:
    """Lee TODAS las hojas de un Excel y devuelve las filas como líneas tab-separated.

    wb.active solo apunta a la hoja marcada como activa en el workbook, y los
    reportes de retenciones de MercadoPago suelen tener los datos reales en
    una hoja distinta a la activa (que a veces solo trae el título).
    """
    from openpyxl import load_workbook

    wb = load_workbook(path, data_only=True, read_only=False)
    filas_texto = []
    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        filas_texto.append(f"=== HOJA: {sheet_name} ===")
        for row in ws.iter_rows(values_only=True):
            if all(cell is None for cell in row):
                continue
            filas_texto.append("\t".join(str(c) if c is not None else "" for c in row))
    wb.close()
    return filas_texto


def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


_MESES_ABBR_ARCHIVO = {"ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"}


def _extraer_impuesto_percepciones(filename: str) -> Optional[str]:
    """Deduce 'Percepciones IIBB {provincia}' del NOMBRE del archivo.

    El Excel de percepciones no trae el nombre del impuesto explícito en ninguna celda
    (a diferencia de Retenciones, que sí lo trae) — el nombre de archivo es la única fuente confiable.
    """
    palabras = Path(filename).stem.split("-")
    idx = next(
        (i for i, p in enumerate(palabras) if _strip_accents(p.lower()) == "percepcion"),
        None,
    )
    if idx is None or idx + 2 >= len(palabras):
        return None
    provincia_palabras = []
    for p in palabras[idx + 2:]:
        pl = _strip_accents(p.lower())
        if pl in _MESES_ABBR_ARCHIVO or re.fullmatch(r"20\d{2}", p):
            break
        provincia_palabras.append(p)
    if not provincia_palabras:
        return None
    return f"Percepciones IIBB {' '.join(provincia_palabras)}"


def _calcular_totales_percepciones(filas: list) -> Optional[dict]:
    """Suma en Python (100% preciso, sin límite de filas ni de tokens) TODA la tabla de
    detalle de un Excel de percepciones: base_imponible y monto_percibido."""
    def _norm(s: str) -> str:
        return _strip_accents(s.strip().lower())

    idx_base = idx_alic = idx_monto = idx_fecha = header_idx = None
    for i, linea in enumerate(filas):
        celdas_norm = [_norm(c) for c in linea.split("\t")]
        if "base imponible" in celdas_norm:
            idx_base = celdas_norm.index("base imponible")
            if "alicuota" in celdas_norm:
                idx_alic = celdas_norm.index("alicuota")
            for cand in ("monto percibido", "importe percibido"):
                if cand in celdas_norm:
                    idx_monto = celdas_norm.index(cand)
                    break
            for cand in ("fecha del cargo", "fecha de la venta", "fecha de operacion"):
                if cand in celdas_norm:
                    idx_fecha = celdas_norm.index(cand)
                    break
            header_idx = i
            break

    if header_idx is None or idx_base is None or idx_monto is None:
        return None

    total_base = total_monto = 0.0
    alicuota_val = None
    fecha_min = fecha_max = None
    n_filas = 0
    for linea in filas[header_idx + 1:]:
        celdas = linea.split("\t")
        if len(celdas) <= max(idx_base, idx_monto):
            continue
        try:
            base_v = float(celdas[idx_base])
            monto_v = float(celdas[idx_monto])
        except ValueError:
            continue
        total_base += base_v
        total_monto += monto_v
        n_filas += 1
        if alicuota_val is None and idx_alic is not None and idx_alic < len(celdas):
            try:
                alicuota_val = float(celdas[idx_alic])
            except ValueError:
                pass
        if idx_fecha is not None and idx_fecha < len(celdas):
            try:
                dt = datetime.strptime(celdas[idx_fecha].strip().split(" ")[0], "%Y-%m-%d")
                if fecha_min is None or dt < fecha_min:
                    fecha_min = dt
                if fecha_max is None or dt > fecha_max:
                    fecha_max = dt
            except ValueError:
                pass

    if n_filas == 0:
        return None

    alicuota_str = None
    if alicuota_val is not None:
        alicuota_str = f"{alicuota_val * 100:.2f}".replace(".", ",") + " %"

    return {
        "filas_tabla": n_filas,
        "base_imponible": round(total_base, 2),
        "monto_percibido": round(total_monto, 2),
        "alicuota": alicuota_str,
        "fecha_desde": fecha_min.strftime("%d/%m/%Y") if fecha_min else None,
        "fecha_hasta": fecha_max.strftime("%d/%m/%Y") if fecha_max else None,
    }


def _es_notas_credito(filename: str) -> bool:
    """Detecta el 'Reporte notas de crédito' de MercadoLibre por el nombre del archivo
    (p.ej. 'Reporte_Notas_Credito_MercadoLibre_Abr2026.xlsx'), sin importar acentos/mayúsculas."""
    norm = _strip_accents(filename.lower())
    return "notas" in norm and "credito" in norm


def _es_notas_debito(filename: str) -> bool:
    """Detecta el 'Reporte notas de débito' de MercadoLibre por el nombre del archivo
    (p.ej. 'Reporte_Notas_Debito_EnviosFlex_Abr2026.xlsx'), sin importar acentos/mayúsculas."""
    norm = _strip_accents(filename.lower())
    return "notas" in norm and "debito" in norm


_NOTAS_BONIF_CONFIG = {
    # Header exacto (normalizado: sin acentos, minúscula) de la columna de valor en cada
    # reporte real de Envíos Flex, confirmado contra archivos de producción.
    "credito": {
        "valor_headers": {"valor de la bonificacion"},
        "campo_total": "total_bonificado",
        "campo_cantidad": "cantidad_bonificaciones",
    },
    "debito": {
        "valor_headers": {
            "valor de la anulacion", "valor del debito", "valor debitado",
            "monto debitado", "monto debito", "importe debitado",
        },
        "campo_total": "total_debitado",
        "campo_cantidad": "cantidad_debitos",
    },
}


def _calcular_notas_bonificacion(filas: list, tipo: str) -> Optional[dict]:
    """Calcula en Python (100% preciso, sin depender de Gemini) los 4 campos de los reportes
    'notas de crédito' / 'notas de débito' de Envíos Flex: fecha_desde/fecha_hasta (rango de
    fechas de la tabla de detalle) y total/cantidad (suma en valor absoluto y conteo de filas
    con valor != 0) de la columna de valor. En estos reportes los valores pueden venir en
    NEGATIVO (son anulaciones de un cargo previo) — se toma el valor absoluto porque el total
    representa el monto acreditado/debitado, no un saldo con signo."""
    cfg = _NOTAS_BONIF_CONFIG[tipo]

    def _norm(s: str) -> str:
        return _strip_accents(s.strip().lower())

    idx_fecha = idx_valor = header_idx = None
    for i, linea in enumerate(filas):
        celdas_norm = [_norm(c) for c in linea.split("\t")]
        cand_fecha = next((j for j, c in enumerate(celdas_norm) if "fecha" in c), None)
        cand_valor = next(
            (j for j, c in enumerate(celdas_norm) if c in cfg["valor_headers"]), None
        )
        if cand_fecha is not None and cand_valor is not None:
            idx_fecha, idx_valor, header_idx = cand_fecha, cand_valor, i
            break

    if header_idx is None:
        return None

    fechas = []
    valores = []
    for linea in filas[header_idx + 1:]:
        celdas = linea.split("\t")
        if len(celdas) <= max(idx_fecha, idx_valor):
            continue
        if celdas[idx_fecha].strip():
            try:
                fechas.append(datetime.strptime(celdas[idx_fecha].strip().split(" ")[0], "%Y-%m-%d"))
            except ValueError:
                pass
        try:
            v = float(celdas[idx_valor])
        except ValueError:
            v = 0.0
        if v != 0:
            valores.append(abs(v))

    return {
        "fecha_desde": fechas and min(fechas).strftime("%d/%m/%Y") or None,
        "fecha_hasta": fechas and max(fechas).strftime("%d/%m/%Y") or None,
        cfg["campo_total"]: round(sum(valores), 2),
        cfg["campo_cantidad"]: len(valores),
    }


def procesar_archivo_con_gemini(
    archivo_path: str, seccion: str, prompt_custom: Optional[str] = None
) -> dict:
    """Envía el archivo a Gemini y retorna los datos extraídos."""
    try:
        from google import genai
        from google.genai import types as genai_types
    except ImportError:
        return {
            "success": False, "data": {}, "prompt_used": "",
            "error": "Instalar: pip install google-genai",
        }

    api_key = get_app_config("gemini_api_key")
    if not api_key:
        return {
            "success": False, "data": {}, "prompt_used": "",
            "error": "API Key de Gemini no configurada (ir a Config → Gemini)",
        }

    prompt = prompt_custom or _PROMPTS_DEFAULT.get(seccion, "Extraé los datos del documento en JSON puro.")
    path = Path(archivo_path)
    ext = path.suffix.lower()

    print(f"[DBG-GASTOS] seccion={seccion!r} path={archivo_path!r}", flush=True)
    try:
        sz = path.stat().st_size
    except Exception:
        sz = -1
    print(f"[DBG-GASTOS] tamaño={sz} bytes", flush=True)

    calc_percepciones = None

    try:
        _tipo_notas = (
            "credito" if _es_notas_credito(path.name)
            else "debito" if _es_notas_debito(path.name)
            else None
        )
        if seccion == "reportes_ml" and ext in (".xlsx", ".xls") and _tipo_notas:
            filas_sin_hoja = [f for f in leer_excel_completo(path) if not f.startswith("=== HOJA:")]
            calc_notas = _calcular_notas_bonificacion(filas_sin_hoja, _tipo_notas)
            tag = f"DBG-NOTAS-{_tipo_notas.upper()}-CALC"
            if calc_notas:
                print(f"[{tag}] {calc_notas}", flush=True)
                return {
                    "success": True, "data": calc_notas, "error": None,
                    "prompt_used": "(calculado en Python, sin Gemini)",
                }
            print(f"[{tag}] no se encontraron columnas fecha/valor", flush=True)

        client = genai.Client(api_key=api_key)

        if ext == ".pdf":
            data = path.read_bytes()
            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=[
                    genai_types.Part.from_bytes(data=data, mime_type="application/pdf"),
                    prompt,
                ],
            )
        elif ext in (".xlsx", ".xls"):
            filas_todas = leer_excel_completo(path)
            filas_sin_hoja = [f for f in filas_todas if not f.startswith("=== HOJA:")]

            if seccion == "percepciones":
                # Los totales se calculan en Python (sin límite de filas/tokens); a Gemini
                # solo se le manda la cabecera para que extraiga los campos de texto.
                calc_percepciones = _calcular_totales_percepciones(filas_sin_hoja)
                if calc_percepciones:
                    print(
                        f"[DBG-PERCEPCIONES-CALC] filas_tabla={calc_percepciones['filas_tabla']} "
                        f"base_imponible_calc=${calc_percepciones['base_imponible']} "
                        f"monto_percibido_calc=${calc_percepciones['monto_percibido']} "
                        f"fecha_desde_calc={calc_percepciones['fecha_desde']} "
                        f"fecha_hasta_calc={calc_percepciones['fecha_hasta']}",
                        flush=True,
                    )
                corte = _cortar_en_detalle(filas_sin_hoja)
                filas_texto = filas_sin_hoja[:corte] or filas_sin_hoja[:20]
            else:
                filas_texto = filas_todas[:500]

            print(f"[DBG-GASTOS] filas_totales={len(filas_todas)} filas_enviadas={len(filas_texto)}", flush=True)
            print(f"[DBG-GASTOS] primeras_3={filas_texto[:3]}", flush=True)
            print(f"[DBG-GASTOS] ultimas_3_enviadas={filas_texto[-3:]}", flush=True)
            if seccion == "percepciones":
                hojas = [
                    ln.replace("=== HOJA: ", "").replace(" ===", "")
                    for ln in filas_todas if ln.startswith("=== HOJA:")
                ]
                print(f"[DBG-PERCEPCIONES] path={str(path)!r}", flush=True)
                print(f"[DBG-PERCEPCIONES] filas_totales={len(filas_todas)}", flush=True)
                print(f"[DBG-PERCEPCIONES] hojas={hojas}", flush=True)
                print(f"[DBG-PERCEPCIONES] primeras_5={filas_todas[:5]}", flush=True)
                print(f"[DBG-PERCEPCIONES] ultimas_5={filas_todas[-5:]}", flush=True)
            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=f"{prompt}\n\nDatos:\n" + "\n".join(filas_texto),
            )
        else:
            return {
                "success": False, "data": {}, "prompt_used": prompt,
                "error": f"Tipo no soportado: {ext}",
            }

        raw = response.text or ""
        print(f"[DBG-GASTOS] respuesta_gemini={raw[:400]!r}", flush=True)
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        data = json.loads(m.group()) if m else {"respuesta_raw": raw}

        if seccion == "percepciones":
            impuesto_calc = _extraer_impuesto_percepciones(path.name)
            if impuesto_calc:
                data["impuesto"] = impuesto_calc
            if calc_percepciones:
                data["base_imponible"] = calc_percepciones["base_imponible"]
                data["monto_percibido"] = calc_percepciones["monto_percibido"]
                if calc_percepciones["alicuota"]:
                    data["alicuota"] = calc_percepciones["alicuota"]
                if calc_percepciones["fecha_desde"]:
                    data["fecha_desde"] = calc_percepciones["fecha_desde"]
                if calc_percepciones["fecha_hasta"]:
                    data["fecha_hasta"] = calc_percepciones["fecha_hasta"]

        return {"success": True, "data": data, "error": None, "prompt_used": prompt}

    except Exception as exc:
        print(f"[DBG-GASTOS] excepcion={exc!r}", flush=True)
        return {"success": False, "data": {}, "error": str(exc), "prompt_used": prompt}


# ---------------------------------------------------------------------------
# Exported
# ---------------------------------------------------------------------------


def build_tab_gastos(container) -> None:
    user = _require_login()
    if not user:
        return
    with container:
        _build_gastos(user["id"])


# ---------------------------------------------------------------------------
# Main UI
# ---------------------------------------------------------------------------


def _build_gastos(user_id: int) -> None:
    now = datetime.now()

    # ── Barra superior ────────────────────────────────────────────────────────
    with ui.row().classes("w-full items-center gap-4 flex-wrap").style(
        "background:#f8fafc;border-bottom:1px solid #e0e0e0;padding:10px 16px"
    ):
        with ui.row().classes("items-center gap-2"):
            ui.label("Período:").classes("font-semibold text-sm text-gray-600")
            mes_sel = ui.select(options=_MESES, value=_MESES[now.month - 1]).style("width:148px")
            ano_sel = ui.select(
                options=[str(y) for y in range(now.year - 2, now.year + 2)],
                value=str(now.year),
            ).style("width:90px")
        progress_lbl = ui.label("").classes("text-sm text-gray-500 ml-2")
        size_lbl     = ui.label("").classes("text-sm text-gray-400")

    content = ui.column().classes("w-full p-4 gap-4")

    def _get_periodo() -> str:
        return f"{ano_sel.value}-{(_MESES.index(mes_sel.value) + 1):02d}"

    def _build_content() -> None:
        content.clear()
        periodo = _get_periodo()

        archivos_por_sec: Dict[str, list] = {}
        dot_refs:         Dict[str, Any]  = {}
        subtitle_ref:     list            = [None]
        final_btn_ref:    list            = [None]
        _seccion_botones: Dict[str, dict] = {}

        def _sec_state(sk: str) -> dict:
            return _seccion_botones.setdefault(sk, {"proc": None, "borrar": None, "aprobar": []})

        def _set_procesando(sk: str, procesando: bool) -> None:
            """Deshabilita/habilita Procesar Todo, Borrar Todos y los Aprobar de popups abiertos de esa sección."""
            st = _sec_state(sk)
            for key in ("proc", "borrar"):
                b = st.get(key)
                if b:
                    b.disable() if procesando else b.enable()
            for b in st.get("aprobar", []):
                b.disable() if procesando else b.enable()
                b.style(f"opacity:{'0.5' if procesando else '1'}")

        def _sec_verde(sk: str) -> bool:
            fs = archivos_por_sec.get(sk, [])
            return bool(fs) and all(f.get("extraction_status") == "procesado" for f in fs)

        def _count_verdes() -> int:
            return sum(1 for sk, *_ in _SECCIONES if _sec_verde(sk))

        def _refresh_progress() -> None:
            n, total = _count_verdes(), len(_SECCIONES)
            pct = int(n / total * 100) if total else 0
            progress_lbl.text = f"{n} de {total} secciones procesadas — {pct}%"
            tf = sum(len(v) for v in archivos_por_sec.values())
            tb = sum(sum(f.get("size_bytes", 0) for f in v) for v in archivos_por_sec.values())
            size_lbl.text = f"{tf} archivo(s) — {_fmt_size(tb)}" if tf else ""
            if subtitle_ref[0]:
                subtitle_ref[0].text = (
                    "Todas las secciones procesadas — listo para analizar"
                    if n == total
                    else f"Disponible cuando las 5 secciones estén procesadas — actualmente {n} de {total}"
                )
            if final_btn_ref[0]:
                final_btn_ref[0].enable() if n == total else final_btn_ref[0].disable()

        def _refresh_dot(sk: str) -> None:
            dot = dot_refs.get(sk)
            if dot:
                dot.style(_DOT.format(_semaforo_color(archivos_por_sec.get(sk, []))))

        # ── Modal visor de extracción IA ──────────────────────────────────────
        def _open_eye_modal(fa: dict, seccion: str, on_approve=None) -> None:
            path        = Path(fa["filepath"])
            ext         = path.suffix.lower()
            cur_status  = fa.get("extraction_status", "pendiente")
            prompt_init = get_gastos_prompt(user_id, seccion) or _PROMPTS_DEFAULT.get(seccion, "")
            try:
                data_dict = json.loads(fa.get("extracted_data") or "{}") or {}
            except Exception:
                data_dict = {}

            with ui.dialog() as dlg:
                with ui.card().style(
                    "width:90vw;max-width:1200px;max-height:90vh;overflow:hidden;"
                    "display:flex;flex-direction:column;padding:0"
                ):
                    # Header
                    with ui.row().classes("items-start justify-between w-full px-4 py-2 flex-shrink-0 gap-2").style(
                        f"background:{_HDR_BG};border-bottom:1px solid {_HDR_BORDER}"
                    ):
                        _fname_display = (
                            Path(fa["filename"]).stem.replace("_", " ").replace("-", " ")
                            + Path(fa["filename"]).suffix
                        )
                        ui.label(_fname_display).style(
                            f"color:{_HDR_COLOR};font-weight:700;font-size:15px;"
                            "white-space:normal;word-break:break-word;line-height:1.3;flex:1;min-width:0"
                        )
                        async def _cerrar():
                            dlg.close()
                        ui.button(icon="close", on_click=_cerrar).props("flat round dense").classes("flex-shrink-0")

                    # Body: dos columnas
                    with ui.row().classes("w-full flex-1").style("max-height:calc(90vh - 120px);overflow-y:auto;min-height:0"):

                        # IZQUIERDA — documento original
                        with ui.column().classes("p-3 gap-2 flex-1").style(
                            f"border-right:1px solid #e0e0e0;overflow-y:auto;min-width:0"
                        ):
                            ui.label("Documento original").style(
                                f"color:{_HDR_COLOR};font-weight:700;font-size:13px"
                            )
                            ui.label(f"Tipo: {ext.upper().lstrip('.')}").classes("text-xs text-gray-400")
                            ui.separator()
                            if ext == ".pdf":
                                b64 = _pdf_first_page_b64(path)
                                if b64:
                                    ui.html(
                                        f'<img src="data:image/png;base64,{b64}" '
                                        'style="max-width:100%;border:1px solid #e0e0e0;border-radius:4px">'
                                    )
                                    ui.label("(Mostrando página 1)").classes("text-xs text-gray-400 mt-1")
                                else:
                                    ui.label("No se pudo renderizar el PDF").classes("text-xs text-red-500")
                            elif ext in (".xlsx", ".xls"):
                                ui.html(
                                    f'<div style="overflow-x:auto">{_excel_preview_html(path)}</div>'
                                )
                            else:
                                ui.label("Previsualización no disponible").classes("text-xs text-gray-400")

                        # DERECHA — datos extraídos
                        with ui.column().classes("p-3 gap-2 flex-1").style(
                            "overflow-y:auto;min-width:0"
                        ):
                            with ui.row().classes("items-center gap-2"):
                                ui.label("Datos extraídos por Gemini").style(
                                    f"color:{_HDR_COLOR};font-weight:700;font-size:13px"
                                )
                                ui.html(_badge_html(cur_status))

                            ui.separator()

                            kv_container = ui.column().classes("w-full gap-0")

                            def _render_kv(d: dict) -> None:
                                kv_container.clear()
                                with kv_container:
                                    if not d:
                                        ui.label("Sin datos extraídos").classes(
                                            "text-xs text-gray-400 italic"
                                        )
                                        return

                                    _MONEY_KEYS = {
                                        "monto", "total", "subtotal", "precio", "iva",
                                        "neto", "importe", "imponible", "valor", "suma",
                                        "cargo", "bonificacion", "percepcion", "retencion",
                                        "gravado", "honorario", "flete", "almacenaje",
                                    }

                                    def _is_money_key(k: str) -> bool:
                                        kl = k.lower()
                                        if kl.startswith("cantidad"):
                                            return False
                                        return any(mk in kl for mk in _MONEY_KEYS)

                                    def _fmt_money(val) -> str:
                                        try:
                                            n = float(val)
                                            s = f"{abs(n):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                                            return f"$ {'-' if n < 0 else ''}{s}"
                                        except Exception:
                                            return str(val)

                                    def _render_value(k: str, val, is_total: bool = False) -> None:
                                        _bw = ";font-weight:700" if is_total else ""
                                        # None / vacío
                                        if val is None or val == "" or val == []:
                                            ui.label("—").style("font-size:11px;color:#9e9e9e")
                                            return
                                        # Lista de dicts → mini-tabla
                                        if isinstance(val, list) and val and isinstance(val[0], dict):
                                            headers = list(val[0].keys())
                                            ths = "".join(
                                                f'<th style="border:1px solid #d0d0d0;padding:2px 8px;'
                                                f'background:#eef3f8;font-size:10px;font-weight:600;'
                                                f'color:{_HDR_COLOR};text-align:left">'
                                                f'{h.replace("_"," ").capitalize()}</th>'
                                                for h in headers
                                            )
                                            tbody = ""
                                            for row in val:
                                                tds = ""
                                                for h in headers:
                                                    cell = row.get(h)
                                                    if isinstance(cell, (int, float)) and _is_money_key(h):
                                                        cell_str = _fmt_money(cell)
                                                        td_style = (
                                                            f"color:{_BLUE};text-align:right;"
                                                            "font-variant-numeric:tabular-nums"
                                                        )
                                                    elif isinstance(cell, (int, float)):
                                                        cell_str = str(cell)
                                                        td_style = f"color:{_BLUE};text-align:right"
                                                    elif cell is None:
                                                        cell_str = "—"
                                                        td_style = "color:#9e9e9e"
                                                    else:
                                                        cell_str = str(cell)
                                                        td_style = "color:#333"
                                                    tds += (
                                                        f'<td style="border:1px solid #e8e8e8;'
                                                        f'padding:2px 8px;font-size:10px;{td_style}">'
                                                        f"{cell_str}</td>"
                                                    )
                                                tbody += f"<tr>{tds}</tr>"
                                            ui.html(
                                                f'<table style="border-collapse:collapse;width:100%;'
                                                f'background:#f9fbfe;border-radius:3px;margin-top:2px">'
                                                f"<thead><tr>{ths}</tr></thead>"
                                                f"<tbody>{tbody}</tbody></table>"
                                            )
                                            return
                                        # Lista de scalars → bullets
                                        if isinstance(val, list):
                                            with ui.column().classes("gap-0"):
                                                for item in val:
                                                    ui.label(f"• {item}").style("font-size:11px;color:#333")
                                            return
                                        # Dict → sub-tabla 2 columnas
                                        if isinstance(val, dict):
                                            with ui.column().classes("gap-0 w-full"):
                                                for dk, dv in val.items():
                                                    with ui.row().classes("items-start gap-1").style(
                                                        "border-bottom:1px solid #f5f5f5"
                                                    ):
                                                        ui.label(dk).style(
                                                            "font-size:10px;color:#9e9e9e;"
                                                            "width:100px;flex-shrink:0"
                                                        )
                                                        ui.label(str(dv) if dv is not None else "—").style(
                                                            "font-size:10px;color:#333"
                                                        )
                                            return
                                        # Número
                                        if isinstance(val, (int, float)):
                                            if _is_money_key(k):
                                                ui.label(_fmt_money(val)).style(
                                                    f"font-size:11px;color:{_BLUE};"
                                                    "font-variant-numeric:tabular-nums;"
                                                    "text-align:right;padding-right:12px;"
                                                    f"display:block;width:100%{_bw}"
                                                )
                                            else:
                                                ui.label(str(val)).style(
                                                    f"font-size:11px;color:{_BLUE};"
                                                    f"font-variant-numeric:tabular-nums{_bw}"
                                                )
                                            return
                                        # String / fallback
                                        val_str = str(val)
                                        if val_str.startswith("$"):
                                            ui.label(val_str).style(
                                                "font-size:11px;color:#333;"
                                                "text-align:right;padding-right:12px;"
                                                "font-variant-numeric:tabular-nums;"
                                                f"display:block;width:100%{_bw}"
                                            )
                                        else:
                                            ui.label(val_str).style(f"font-size:11px;color:#333{_bw}")

                                    def _concepto_to_key(concepto: str) -> str:
                                        s = str(concepto)
                                        prev = None
                                        while prev != s:
                                            prev = s
                                            s = re.sub(r'([A-Za-z])\.([A-Za-z])', r'\1\2', s)
                                        s = re.sub(r'[^\w\s]', ' ', s)
                                        s = re.sub(r'\s+', '_', s.lower().strip())
                                        s = re.sub(r'_+', '_', s).strip('_')
                                        return s or str(concepto).lower().replace(' ', '_')

                                    def prettify_key(key: str) -> str:
                                        _UPPER = {"iva", "iibb", "cae", "cuit", "rg", "sr", "srl", "caba"}
                                        _STOP = {
                                            'a', 'ante', 'bajo', 'con', 'contra', 'de', 'del', 'desde',
                                            'en', 'entre', 'hacia', 'hasta', 'para', 'por', 'según', 'sin',
                                            'sobre', 'tras', 'y', 'e', 'ni', 'o', 'u', 'el', 'la', 'los',
                                            'las', 'un', 'una', 'al',
                                        }
                                        words = key.replace("_", " ").split()
                                        result = []
                                        for i, w in enumerate(words):
                                            wl = w.lower()
                                            if wl in _UPPER:
                                                result.append(wl.upper())
                                            elif i > 0 and wl in _STOP:
                                                result.append(wl)
                                            else:
                                                result.append(w.capitalize())
                                        return " ".join(result)

                                    def _is_concepto_monto_list(val) -> bool:
                                        return (
                                            isinstance(val, list) and bool(val)
                                            and isinstance(val[0], dict)
                                            and "concepto" in val[0]
                                            and "monto" in val[0]
                                        )

                                    _ROW_STYLE = (
                                        "border-bottom:1px solid #f0f0f0;"
                                        "display:grid;"
                                        "grid-template-columns:minmax(230px,300px) 1fr;"
                                        "gap:0 20px;padding:2px 8px"
                                    )
                                    _KEY_STYLE = (
                                        "white-space:normal;word-break:break-word;"
                                        "padding-right:16px"
                                    )

                                    _HIDDEN_FIELDS = {
                                        'cae', 'cae_vto', 'cuit_receptor',
                                        'receptor', 'emisor', 'cuit_emisor',
                                    }
                                    if seccion == "pagos_arca":
                                        _HIDDEN_FIELDS = _HIDDEN_FIELDS | {
                                            'cuit', 'organismo_recaudador', 'tipo_pago', 'concepto',
                                            'monto_pagado', 'banco', 'fecha_vencimiento', 'subconcepto',
                                            'descripcion_reducida', 'codigo_jurisdiccion', 'nro_inscripcion',
                                            'formulario_origen', 'generado_por_usuario', 'fecha_generacion',
                                            'dia_expiracion', 'generado_desde_presentacion_de_dj_nro',
                                            'denominacion', 'secuencia', 'fecha_presentacion',
                                            'nro_transaccion', 'codigo_identificacion_presentacion_md5',
                                        }
                                    if seccion == "retenciones":
                                        _HIDDEN_FIELDS = _HIDDEN_FIELDS | {
                                            'cuit_agente', 'monto_retenido',
                                            'numero_comprobante', 'agente_retencion',
                                        }
                                    if seccion == "percepciones":
                                        _HIDDEN_FIELDS = _HIDDEN_FIELDS | {
                                            'agente_percepcion', 'agente_percepciones',
                                            'cuit_agente', 'numero_comprobante', 'monto',
                                            'fecha_percepcion', 'agente',
                                        }
                                    if seccion == "reportes_ml" and _es_notas_credito(fa.get("filename", "")):
                                        _CAMPOS_NOTAS_CREDITO = {
                                            'fecha_desde', 'fecha_hasta',
                                            'total_bonificado', 'cantidad_bonificaciones',
                                        }
                                        _HIDDEN_FIELDS = {k for k in d.keys() if k not in _CAMPOS_NOTAS_CREDITO}
                                    if seccion == "reportes_ml" and _es_notas_debito(fa.get("filename", "")):
                                        _CAMPOS_NOTAS_DEBITO = {
                                            'fecha_desde', 'fecha_hasta',
                                            'total_debitado', 'cantidad_debitos',
                                        }
                                        _HIDDEN_FIELDS = {k for k in d.keys() if k not in _CAMPOS_NOTAS_DEBITO}
                                    _SECTION_LABELS = {
                                        "lineas_convenio_multilateral": "Convenio Multilateral",
                                        "determinacion_del_impuesto": "Determinación del Impuesto",
                                        "determinacion_posicion_mensual": "Determinación de la Posición Mensual",
                                    }
                                    _SEP_STYLE = (
                                        f"width:100%;font-size:10px;font-weight:600;"
                                        f"text-transform:uppercase;letter-spacing:0.04em;"
                                        f"color:{_HDR_COLOR};background:{_HDR_BG};"
                                        f"padding:8px 12px 4px;"
                                        f"border-bottom:0.5px solid {_HDR_BORDER};"
                                        f"margin-top:8px;display:block"
                                    )
                                    for k, v in d.items():
                                        if k in _HIDDEN_FIELDS:
                                            continue
                                        # lineas_convenio_multilateral → separador + filas CM
                                        if k == "lineas_convenio_multilateral" and isinstance(v, list):
                                            if seccion == "pagos_arca" and v:
                                                ui.html(
                                                    f'<div style="{_SEP_STYLE}">'
                                                    f'{_SECTION_LABELS.get(k, prettify_key(k))}</div>'
                                                )
                                            for cm_item in v:
                                                juris = cm_item.get("jurisdiccion", "")
                                                code  = cm_item.get("codigo", "")
                                                cm_val = cm_item.get("monto")
                                                cm_key = f"CM {juris} ({code})" if code else f"CM {juris}"
                                                _is_tot = 'total' in cm_key.lower()
                                                _kw = ";font-weight:700" if _is_tot else ""
                                                with ui.row().classes("w-full items-start py-1").style(_ROW_STYLE):
                                                    ui.label(cm_key).classes(
                                                        "text-xs text-gray-500 font-medium"
                                                    ).style(_KEY_STYLE + _kw)
                                                    if isinstance(cm_val, (int, float)):
                                                        ui.label(_fmt_money(cm_val)).style(
                                                            f"font-size:11px;color:{_BLUE};"
                                                            "font-variant-numeric:tabular-nums;"
                                                            "white-space:nowrap;"
                                                            "text-align:right;padding-right:12px;"
                                                            f"display:block;width:100%{_kw}"
                                                        )
                                                    else:
                                                        ui.label(str(cm_val) if cm_val is not None else "—").style(
                                                            f"font-size:11px;color:#333{_kw}"
                                                        )
                                            continue
                                        # concepto+monto list → separador (pagos_arca) + filas con bold
                                        if _is_concepto_monto_list(v):
                                            if seccion == "pagos_arca" and k in _SECTION_LABELS:
                                                ui.html(
                                                    f'<div style="{_SEP_STYLE}">'
                                                    f'{_SECTION_LABELS[k]}</div>'
                                                )
                                            for item in v:
                                                concepto_raw = str(item.get("concepto", k))
                                                row_key = _concepto_to_key(concepto_raw)
                                                row_val = item.get("monto")
                                                _is_tot = 'total' in concepto_raw.lower()
                                                _kw = ";font-weight:700" if _is_tot else ""
                                                with ui.row().classes("w-full items-start py-1").style(_ROW_STYLE):
                                                    ui.label(prettify_key(row_key)).classes(
                                                        "text-xs text-gray-500 font-medium"
                                                    ).style(_KEY_STYLE + _kw)
                                                    if isinstance(row_val, (int, float)):
                                                        ui.label(_fmt_money(row_val)).style(
                                                            f"font-size:11px;color:{_BLUE};"
                                                            "font-variant-numeric:tabular-nums;"
                                                            "white-space:nowrap;"
                                                            "text-align:right;padding-right:12px;"
                                                            f"display:block;width:100%{_kw}"
                                                        )
                                                    else:
                                                        ui.label(str(row_val) if row_val is not None else "—").style(
                                                            f"font-size:11px;color:#333;white-space:nowrap{_kw}"
                                                        )
                                        else:
                                            _is_tot = 'total' in k.lower()
                                            _kw = ";font-weight:700" if _is_tot else ""
                                            with ui.row().classes("w-full items-start py-1").style(_ROW_STYLE):
                                                ui.label(prettify_key(k)).classes(
                                                    "text-xs text-gray-500 font-medium"
                                                ).style(_KEY_STYLE + _kw)
                                                _render_value(k, v, is_total=_is_tot)

                            _render_kv(data_dict)

                            _prompt_open = [False]
                            with ui.column().classes("w-full mt-3 gap-0"):
                                with ui.row().classes("items-center gap-1 py-1").style(
                                    "cursor:pointer;user-select:none"
                                ) as _prompt_hdr:
                                    _prompt_chev = ui.element("i").classes("ti ti-chevron-right").style(
                                        f"color:{_HDR_COLOR};font-size:14px"
                                    )
                                    ui.label("Prompt usado").classes("text-xs font-semibold text-gray-600")

                                prompt_display = ui.html(
                                    f'<pre style="{_PROMPT_PRE_STYLE}">'
                                    f'{_escape_prompt_html(prompt_init)}</pre>'
                                ).classes("w-full hidden")

                                prompt_ta = (
                                    ui.textarea(value=prompt_init)
                                    .props("outlined autogrow")
                                    .classes("w-full hidden")
                                    .style("font-size:11px;line-height:1.5")
                                )

                                def _toggle_prompt():
                                    _prompt_open[0] = not _prompt_open[0]
                                    if _prompt_open[0]:
                                        prompt_display.classes(remove="hidden")
                                        _prompt_chev.classes(remove="ti-chevron-right")
                                        _prompt_chev.classes(add="ti-chevron-down")
                                    else:
                                        prompt_display.classes(add="hidden")
                                        _prompt_chev.classes(remove="ti-chevron-down")
                                        _prompt_chev.classes(add="ti-chevron-right")

                                _prompt_hdr.on("click", _toggle_prompt)

                            reproc_lbl = ui.label("").classes("text-xs text-gray-500 mt-1")
                            reproc_btn_ref: list = [None]

                            async def _reprocesar() -> None:
                                _set_procesando(seccion, True)
                                if reproc_btn_ref[0]:
                                    reproc_btn_ref[0].disable()
                                    reproc_btn_ref[0].text = "Procesando..."
                                reproc_lbl.text = "Procesando con Gemini..."
                                result = await run.io_bound(
                                    procesar_archivo_con_gemini,
                                    fa["filepath"], seccion, prompt_ta.value,
                                )
                                new_status = "procesado" if result["success"] else "error"
                                update_gastos_extraccion(
                                    fa["id"],
                                    extracted_data=json.dumps(result["data"]) if result["success"] else None,
                                    prompt_used=result["prompt_used"],
                                    extraction_status=new_status,
                                    extraction_error=result.get("error"),
                                )
                                archivos_por_sec[seccion] = get_gastos_archivos(user_id, periodo, seccion)
                                _refresh_dot(seccion)
                                _refresh_progress()
                                if result["success"]:
                                    _render_kv(result["data"])
                                    reproc_lbl.text = "Re-procesado correctamente"
                                else:
                                    reproc_lbl.text = f"Error: {result['error']}"
                                if reproc_btn_ref[0]:
                                    reproc_btn_ref[0].enable()
                                    reproc_btn_ref[0].text = "Re-procesar"
                                _set_procesando(seccion, False)

                            async def _aprobar() -> None:
                                mark_gastos_procesado(fa["id"])
                                archivos_por_sec[seccion] = get_gastos_archivos(user_id, periodo, seccion)
                                _refresh_dot(seccion)
                                _refresh_progress()
                                if on_approve:
                                    on_approve(seccion)
                                ui.notify("Archivo aprobado", color="positive")
                                dlg.close()

                            with ui.row().classes("gap-2 mt-2 flex-wrap items-center"):
                                edit_btn = ui.button("Editar prompt").props("outline dense").style(
                                    f"color:{_BLUE};border-color:{_BLUE};font-size:12px"
                                )
                                guardar_btn = ui.button("Guardar").props("dense").style(
                                    f"background:{_GREEN};color:white;font-size:12px"
                                ).classes("hidden")
                                cancelar_btn = ui.button("Cancelar").props("outline dense").style(
                                    "font-size:12px"
                                ).classes("hidden")
                                _rb = ui.button("Re-procesar", on_click=_reprocesar).style(
                                    f"background:{_YELLOW};color:white;font-size:12px"
                                ).props("dense")
                                reproc_btn_ref[0] = _rb
                                _ab = ui.button("Aprobar", on_click=_aprobar).style(
                                    f"background:{_GREEN};color:white;font-size:12px"
                                ).props("dense")
                                _sec_state(seccion)["aprobar"].append(_ab)

                                def _unregister_aprobar(_b=_ab, _sk=seccion):
                                    _lst = _sec_state(_sk)["aprobar"]
                                    if _b in _lst:
                                        _lst.remove(_b)
                                dlg.on("hide", _unregister_aprobar)

                            # Prompt desactualizado → dialog Sí/No para actualizar
                            if seccion == "facturas_ml" and "lineas_antes_subtotal" not in prompt_init:
                                reproc_lbl.text = (
                                    "⚠ El prompt guardado no pide líneas arriba del subtotal."
                                )
                                with ui.dialog() as _upd_dlg:
                                    with ui.card().classes("p-4 gap-2").style("min-width:340px"):
                                        ui.label("Prompt desactualizado").classes("font-semibold text-sm")
                                        ui.label(
                                            "El prompt guardado no extrae lineas_antes_subtotal "
                                            "(bonificaciones y cargos antes del subtotal). "
                                            "¿Actualizar al prompt default nuevo?"
                                        ).classes("text-xs text-gray-600").style("line-height:1.5")
                                        with ui.row().classes("gap-2 justify-end w-full mt-2"):
                                            ui.button("No", on_click=_upd_dlg.close).props("flat dense")
                                            async def _actualizar_prompt(_d=_upd_dlg):
                                                new_p = _PROMPTS_DEFAULT["facturas_ml"]
                                                upsert_gastos_prompt(user_id, seccion, new_p)
                                                prompt_ta.value = new_p
                                                prompt_display.content = (
                                                    f'<pre style="{_PROMPT_PRE_STYLE}">'
                                                    f'{_escape_prompt_html(new_p)}</pre>'
                                                )
                                                reproc_lbl.text = (
                                                    "Prompt actualizado. Reprocesá todos para obtener datos completos."
                                                )
                                                _d.close()
                                            ui.button("Sí, actualizar", on_click=_actualizar_prompt).style(
                                                f"background:{_BLUE};color:white"
                                            ).props("dense")
                                _upd_dlg.open()

                            # Percepciones: datos extraídos con el prompt viejo (sin agregados) → ofrecer reprocesar
                            if seccion == "percepciones" and data_dict and "base_imponible" not in data_dict:
                                with ui.dialog() as _upd_dlg_perc:
                                    with ui.card().classes("p-4 gap-2").style("min-width:340px"):
                                        ui.label("Prompt actualizado").classes("font-semibold text-sm")
                                        ui.label(
                                            "El prompt fue actualizado. "
                                            "¿Reprocesar archivos existentes de esta sección?"
                                        ).classes("text-xs text-gray-600").style("line-height:1.5")
                                        with ui.row().classes("gap-2 justify-end w-full mt-2"):
                                            ui.button("No", on_click=_upd_dlg_perc.close).props("flat dense")
                                            async def _reprocesar_desde_dialog(_d=_upd_dlg_perc):
                                                _d.close()
                                                await _reprocesar()
                                            ui.button("Sí, reprocesar", on_click=_reprocesar_desde_dialog).style(
                                                f"background:{_BLUE};color:white"
                                            ).props("dense")
                                _upd_dlg_perc.open()

                            def _start_edit() -> None:
                                if not _prompt_open[0]:
                                    _toggle_prompt()
                                prompt_display.classes(add="hidden")
                                prompt_ta.classes(remove="hidden")
                                edit_btn.classes(add="hidden")
                                guardar_btn.classes(remove="hidden")
                                cancelar_btn.classes(remove="hidden")

                            async def _guardar_prompt() -> None:
                                new_val = prompt_ta.value
                                upsert_gastos_prompt(user_id, seccion, new_val)
                                prompt_ta.classes(add="hidden")
                                prompt_display.content = (
                                    f'<pre style="{_PROMPT_PRE_STYLE}">'
                                    f'{_escape_prompt_html(new_val)}</pre>'
                                )
                                prompt_display.classes(remove="hidden")
                                edit_btn.classes(remove="hidden")
                                guardar_btn.classes(add="hidden")
                                cancelar_btn.classes(add="hidden")
                                ui.notify("Prompt guardado", color="positive")

                            def _cancelar_edit() -> None:
                                prompt_ta.value = prompt_init
                                prompt_ta.classes(add="hidden")
                                prompt_display.content = (
                                    f'<pre style="{_PROMPT_PRE_STYLE}">'
                                    f'{_escape_prompt_html(prompt_init)}</pre>'
                                )
                                prompt_display.classes(remove="hidden")
                                edit_btn.classes(remove="hidden")
                                guardar_btn.classes(add="hidden")
                                cancelar_btn.classes(add="hidden")

                            edit_btn.on("click", _start_edit)
                            guardar_btn.on("click", _guardar_prompt)
                            cancelar_btn.on("click", _cancelar_edit)

            dlg.open()

        # ── Tarjeta de sección ────────────────────────────────────────────────
        def _build_section_card(sk: str, lbl: str, ext: str, multiple: bool, icon: str) -> None:
            rows = get_gastos_archivos(user_id, periodo, sk)
            archivos_por_sec[sk] = rows
            footer_lbl_ref:    list = [None]
            proc_lbl_ref:      list = [None]
            borrar_btn_ref:    list = [None]
            upload_ref:        list = [None]
            proc_btn_ref:      list = [None]
            proc_btn_html_ref: list = [None]
            is_proc_ref:       list = [False]

            def _proc_btn_html(txt, icon=None, spinning=False):
                if not icon:
                    return f'<span style="font-size:12px;font-weight:500">{txt}</span>'
                spin = "animation:spin 1s linear infinite;" if spinning else ""
                return (
                    f'<i class="ti {icon}" style="font-size:12px;margin-right:4px;'
                    f'vertical-align:middle;{spin}"></i>'
                    f'<span style="font-size:12px;font-weight:500;vertical-align:middle">{txt}</span>'
                )

            def _refresh_proc_btn(sk_=sk) -> None:
                btn = proc_btn_ref[0]
                if btn is None:
                    return
                fs = archivos_por_sec.get(sk_, [])
                _isp = is_proc_ref[0]
                can = bool(fs) if _isp else any(
                    f.get("extraction_status") != "procesado" for f in fs
                )
                btn.enable() if can else btn.disable()

            with ui.card().classes("w-full").style("border:1px solid #e0e0e0"):
                # Header
                with ui.row().classes("items-center gap-2 w-full px-3 py-2").style(
                    f"background:{_HDR_BG};border-bottom:1px solid {_HDR_BORDER};"
                    "border-radius:4px 4px 0 0"
                ):
                    dot = ui.element("span").style(_DOT.format(_semaforo_color(rows)))
                    dot_refs[sk] = dot
                    ui.element("i").classes(f"ti {icon}").style(f"color:{_HDR_COLOR};font-size:16px")
                    ui.label(lbl).style(f"color:{_HDR_COLOR};font-weight:600;font-size:14px")
                    if not multiple:
                        ui.label("(máx. 1 archivo)").classes("text-xs text-gray-400 ml-1")

                # Lista de archivos
                file_list = ui.column().classes("w-full px-3 pt-2 gap-1 min-h-[32px]")

                def _render_list(sk_=sk) -> None:
                    file_list.clear()
                    with file_list:
                        fs = archivos_por_sec.get(sk_, [])
                        if not fs:
                            ui.label("Sin archivos").classes("text-xs text-gray-400 italic")
                        else:
                            for fa in fs:
                                _fa = dict(fa)
                                status = _fa.get("extraction_status", "pendiente")
                                with ui.row().classes("items-center gap-1 w-full flex-nowrap"):
                                    ui.label(
                                        f"{_fa['filename']}  ({_fmt_size(_fa.get('size_bytes', 0))})"
                                    ).classes("text-xs flex-1 truncate text-gray-700")
                                    ui.html(_badge_html(status))

                                    # Ícono ojo
                                    ui.element("i").classes("ti ti-eye").style(
                                        f"color:{_BLUE};font-size:13px;cursor:pointer;flex-shrink:0"
                                    ).tooltip("Ver extracción IA").on(
                                        "click",
                                        lambda _fa_=_fa, sk2=sk_: _open_eye_modal(_fa_, sk2, on_approve=_render_list),
                                    )

                                    # Ícono tacho con confirmación
                                    def _confirm_del(fa_=_fa, sk2=sk_) -> None:
                                        with ui.dialog() as conf, ui.card().classes("p-4"):
                                            ui.label("¿Eliminar este archivo?").classes(
                                                "font-semibold text-sm mb-1"
                                            )
                                            ui.label(fa_["filename"]).classes(
                                                "text-xs text-gray-500 mb-4 truncate"
                                            ).style("max-width:280px")
                                            with ui.row().classes("gap-2 justify-end w-full"):
                                                ui.button("Cancelar", on_click=conf.close).props("flat dense")

                                                def _do_del(fa__=fa_, sk3=sk2, _c=conf) -> None:
                                                    try:
                                                        Path(fa__["filepath"]).unlink(missing_ok=True)
                                                    except Exception:
                                                        pass
                                                    delete_gastos_archivo(fa__["id"])
                                                    archivos_por_sec[sk3] = get_gastos_archivos(
                                                        user_id, periodo, sk3
                                                    )
                                                    _c.close()
                                                    _render_list(sk3)
                                                    _refresh_dot(sk3)
                                                    _refresh_progress()
                                                    ui.notify("Archivo eliminado", color="positive")

                                                ui.button("Eliminar", on_click=_do_del).style(
                                                    f"background:{_RED};color:white"
                                                ).props("dense")
                                        conf.open()

                                    ui.element("i").classes("ti ti-trash").style(
                                        f"color:{_RED};font-size:13px;cursor:pointer;flex-shrink:0"
                                    ).tooltip("Eliminar").on("click", _confirm_del)

                    # Actualizar contador del footer
                    cnt = len(archivos_por_sec.get(sk_, []))
                    if footer_lbl_ref[0]:
                        footer_lbl_ref[0].text = f"{cnt} archivo(s)" if cnt else "Sin archivos"
                    if borrar_btn_ref[0]:
                        borrar_btn_ref[0].enable() if cnt else borrar_btn_ref[0].disable()
                    _refresh_proc_btn(sk_)

                _render_list(sk)

                # Zona de upload
                with ui.element("div").classes("px-3 pb-2 pt-2"):
                    def _on_upload(e, sk_=sk, mul=multiple) -> None:
                        if not mul and archivos_por_sec.get(sk_):
                            ui.notify(
                                "Esta sección admite solo 1 archivo. Eliminá el existente primero.",
                                color="warning",
                            )
                            return
                        dest_dir = _BASE_PATH / str(user_id) / periodo[:4] / periodo[5:] / sk_
                        dest_dir.mkdir(parents=True, exist_ok=True)
                        dest = dest_dir / e.name
                        data = e.content.read()
                        dest.write_bytes(data)
                        insert_gastos_archivo(
                            user_id=user_id, periodo=periodo, seccion=sk_,
                            filename=e.name, filepath=str(dest), size_bytes=len(data),
                        )
                        archivos_por_sec[sk_] = get_gastos_archivos(user_id, periodo, sk_)
                        _render_list(sk_)
                        _refresh_dot(sk_)
                        _refresh_progress()
                        ui.notify(f"'{e.name}' subido", color="positive")

                    upload_ref[0] = ui.upload(
                        multiple=multiple, auto_upload=True, on_upload=_on_upload,
                        label="Arrastrá archivos aquí o hacé clic",
                    ).props(
                        f'accept="{ext}" flat hide-upload-btn color="primary"'
                    ).classes("w-full").style(
                        "border:2px dashed #85B7EB;border-radius:6px;background:#f9fbfe;min-height:56px"
                    )

                # Footer
                with ui.row().classes("items-center gap-2 px-3 pb-3 pt-1 flex-wrap"):
                    cnt0 = len(rows)
                    footer_lbl_ref[0] = ui.label(
                        f"{cnt0} archivo(s)" if cnt0 else "Sin archivos"
                    ).classes("text-xs text-gray-500 flex-1")
                    proc_lbl_ref[0] = ui.label("").classes("text-xs text-gray-400")

                    is_proc   = _sec_verde(sk)
                    is_proc_ref[0] = is_proc
                    btn_color = _GREEN if is_proc else _BLUE
                    btn_icon  = "ti-refresh" if is_proc else None
                    btn_txt   = "Reprocesar Todos" if is_proc else "Procesar Todo"

                    async def _procesar(sk_=sk) -> None:
                        _set_procesando(sk_, True)
                        if proc_btn_html_ref[0]:
                            proc_btn_html_ref[0].content = _proc_btn_html(
                                "Procesando...", "ti-loader-2"
                            )
                        pl = proc_lbl_ref[0]
                        fs = archivos_por_sec.get(sk_, [])
                        if not fs:
                            ui.notify("No hay archivos para procesar", color="warning")
                            if proc_btn_html_ref[0]:
                                proc_btn_html_ref[0].content = _proc_btn_html(btn_txt, btn_icon)
                            _set_procesando(sk_, False)
                            _refresh_proc_btn(sk_)
                            if borrar_btn_ref[0]:
                                borrar_btn_ref[0].enable() if archivos_por_sec.get(sk_) else borrar_btn_ref[0].disable()
                            return
                        pendientes = [f for f in fs if f.get("extraction_status") != "procesado"] or fs
                        if pl:
                            pl.text = f"Procesando 0 de {len(pendientes)}..."
                        _prompt_custom = get_gastos_prompt(user_id, sk_)
                        for i, fa in enumerate(pendientes, 1):
                            if pl:
                                pl.text = f"Procesando {i} de {len(pendientes)}..."
                            result = await run.io_bound(
                                procesar_archivo_con_gemini, fa["filepath"], sk_, _prompt_custom
                            )
                            new_status = "procesado" if result["success"] else "error"
                            update_gastos_extraccion(
                                fa["id"],
                                extracted_data=json.dumps(result["data"]) if result["success"] else None,
                                prompt_used=result["prompt_used"],
                                extraction_status=new_status,
                                extraction_error=result.get("error"),
                            )
                        archivos_por_sec[sk_] = get_gastos_archivos(user_id, periodo, sk_)
                        _render_list(sk_)
                        _refresh_dot(sk_)
                        _refresh_progress()
                        ok    = sum(1 for f in archivos_por_sec[sk_] if f.get("extraction_status") == "procesado")
                        total = len(archivos_por_sec[sk_])
                        if pl:
                            pl.text = f"{ok}/{total} procesados"
                        if ok == total:
                            ui.notify("Procesado correctamente", color="positive")
                        else:
                            ui.notify(f"{total - ok} archivo(s) con error — revisá el ícono ojo", color="warning")
                        if upload_ref[0] is not None:
                            await upload_ref[0].run_method("reset")
                        if proc_btn_html_ref[0]:
                            proc_btn_html_ref[0].content = _proc_btn_html(btn_txt, btn_icon)
                        _set_procesando(sk_, False)
                        _refresh_proc_btn(sk_)
                        if borrar_btn_ref[0]:
                            cnt_now = len(archivos_por_sec.get(sk_, []))
                            borrar_btn_ref[0].enable() if cnt_now else borrar_btn_ref[0].disable()

                    with ui.button(on_click=_procesar).style(
                        f"background:{btn_color};color:white;font-size:12px;"
                        "padding:4px 14px;border-radius:4px"
                    ).props("dense no-caps") as _proc_btn:
                        _html_el = ui.html(_proc_btn_html(btn_txt, btn_icon))
                        proc_btn_html_ref[0] = _html_el
                    proc_btn_ref[0] = _proc_btn
                    _sec_state(sk)["proc"] = _proc_btn
                    _refresh_proc_btn(sk)

                    def _confirm_borrar_todos(sk_=sk) -> None:
                        fs = archivos_por_sec.get(sk_, [])
                        n = len(fs)
                        if not n:
                            return
                        with ui.dialog() as conf_del, ui.card().classes("p-4"):
                            ui.label(
                                f"¿Eliminar los {n} archivos de esta sección?"
                            ).classes("font-semibold text-sm mb-1")
                            ui.label(
                                "Esta acción borra los archivos del disco Y las "
                                "extracciones guardadas. Es irreversible."
                            ).classes("text-xs text-gray-500 mb-4")
                            with ui.row().classes("gap-2 justify-end w-full"):
                                ui.button("Cancelar", on_click=conf_del.close).props("flat dense")
                                def _do_borrar_todos(sk2=sk_, _c=conf_del) -> None:
                                    fs2 = archivos_por_sec.get(sk2, [])
                                    count = len(fs2)
                                    for fa_item in fs2:
                                        try:
                                            Path(fa_item["filepath"]).unlink(missing_ok=True)
                                        except Exception:
                                            pass
                                        delete_gastos_archivo(fa_item["id"])
                                    archivos_por_sec[sk2] = get_gastos_archivos(
                                        user_id, periodo, sk2
                                    )
                                    _c.close()
                                    _render_list(sk2)
                                    _refresh_dot(sk2)
                                    _refresh_progress()
                                    ui.notify(f"{count} archivos eliminados", color="warning")
                                ui.button("Sí, borrar todos", on_click=_do_borrar_todos).style(
                                    f"background:{_RED};color:white"
                                ).props("dense")
                        conf_del.open()

                    with ui.button(on_click=_confirm_borrar_todos).style(
                        f"background:{_RED};color:white;height:30px;padding:0 12px;border-radius:4px"
                    ).props("dense no-caps") as _borrar_btn:
                        ui.html(
                            '<i class="ti ti-trash" style="font-size:12px;margin-right:6px;'
                            'vertical-align:middle"></i>'
                            '<span style="font-size:11px;font-weight:500;vertical-align:middle">'
                            'Borrar todos</span>'
                        )
                    if not cnt0:
                        _borrar_btn.disable()
                    borrar_btn_ref[0] = _borrar_btn
                    _sec_state(sk)["borrar"] = _borrar_btn

        # ── Grid de tarjetas ──────────────────────────────────────────────────
        with content:
            with ui.grid(columns=2).classes("w-full gap-4"):
                for sk, lbl, ext, mul, icon in _SECCIONES:
                    _build_section_card(sk, lbl, ext, mul, icon)

                # Card análisis final
                with ui.card().classes("w-full").style(
                    f"border:2px solid {_BLUE};background:{_BLUE_BG};border-radius:8px"
                ):
                    with ui.column().classes("p-4 gap-1"):
                        ui.label("Análisis consolidado del período").style(
                            f"color:{_BLUE};font-size:16px;font-weight:700"
                        )
                        subtitle = ui.label("").classes("text-sm text-gray-500")
                        subtitle_ref[0] = subtitle

                        def _final_procesar() -> None:
                            ui.notify("Análisis final pendiente de implementación", color="info")

                        fb = ui.button("Procesar análisis final", on_click=_final_procesar).style(
                            f"background:{_BLUE};color:white;font-size:14px;"
                            "font-weight:600;padding:10px 24px;margin-top:8px"
                        )
                        final_btn_ref[0] = fb

            _refresh_progress()

    mes_sel.on("update:model-value", lambda _: _build_content())
    ano_sel.on("update:model-value", lambda _: _build_content())
    _build_content()
