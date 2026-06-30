"""
tabs/gastos.py — Gestión de documentos impositivos mensuales por sección.
Funciones exportadas: build_tab_gastos, procesar_archivo_con_gemini
"""
from __future__ import annotations

import base64
import json
import re
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

_SECCIONES: List[tuple] = [
    ("facturas_ml",  "Facturas MercadoLibre",  ".pdf",  True,  "ti-file-invoice"),
    ("retenciones",  "Retenciones",             ".xlsx", True,  "ti-file-spreadsheet"),
    ("percepciones", "Percepciones",            ".xlsx", True,  "ti-file-spreadsheet"),
    ("pagos_arca",   "Pagos ARCA",              ".pdf",  True,  "ti-file-invoice"),
    ("reportes_ml",  "Reportes MercadoLibre",   ".xlsx", False, "ti-file-spreadsheet"),
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
        "subtotal (número), lineas_intermedias (lista con TODOS los conceptos entre Subtotal y Total — "
        "IVA 21%, IVA 10.5%, Percepciones IIBB, Percepción IVA, otros impuestos, descuentos, etc. — "
        "extraé TODAS sin omitir ninguna, cada una con nombre exacto y monto numérico), "
        "total (número), cae, cae_vto (DD/MM/AAAA). "
        'Formato esperado: {"tipo_documento":"Factura A","emisor":"...","cuit_emisor":"...","receptor":"...",'
        '"cuit_receptor":"...","fecha":"DD/MM/AAAA","punto_venta":"...","nro_comprobante":"...",'
        '"subtotal":0.0,"lineas_intermedias":[{"concepto":"IVA 21%","monto":0.0}],'
        '"total":0.0,"cae":"...","cae_vto":"DD/MM/AAAA"}'
    ),
    "retenciones": (
        "Analizá este comprobante de retención. Extraé en JSON puro: "
        "tipo_retencion, fecha, agente_retencion, cuit_agente, monto_retenido, numero_comprobante."
    ),
    "percepciones": (
        "Analizá este comprobante de percepción. Extraé en JSON puro: "
        "tipo_percepcion, fecha, agente_percepcion, cuit_agente, monto_percibido, numero_comprobante."
    ),
    "pagos_arca": (
        "Analizá este comprobante de pago ARCA/AFIP. Extraé en JSON puro: "
        "tipo, periodo, fecha_vencimiento, monto_pagado, numero_vep, banco."
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


def _excel_preview_html(path: Path, nrows: int = 50) -> str:
    try:
        import openpyxl
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))[: nrows + 1]
        wb.close()
        if not rows:
            return "<p style='font-size:11px;color:#9e9e9e'>Excel vacío</p>"
        header = [str(c) if c is not None else "" for c in rows[0]]
        th = "".join(
            f'<th style="border:1px solid #ccc;padding:2px 6px;background:#f0f0f0;white-space:nowrap">{h}</th>'
            for h in header
        )
        body = ""
        for row in rows[1:]:
            tds = "".join(
                f'<td style="border:1px solid #e0e0e0;padding:2px 6px;white-space:nowrap">'
                f"{str(c) if c is not None else ''}</td>"
                for c in row
            )
            body += f"<tr>{tds}</tr>"
        return (
            '<table style="border-collapse:collapse;font-size:11px;width:100%">'
            f"<thead><tr>{th}</tr></thead><tbody>{body}</tbody></table>"
        )
    except Exception as exc:
        return f"<p style='font-size:11px;color:#a32d2d'>Error al leer Excel: {exc}</p>"


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

    try:
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
            import openpyxl
            wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
            ws = wb.active
            rows = list(ws.iter_rows(values_only=True))[:101]
            wb.close()
            lines = ["\t".join(str(c) if c is not None else "" for c in row) for row in rows]
            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=f"{prompt}\n\nDatos:\n" + "\n".join(lines),
            )
        else:
            return {
                "success": False, "data": {}, "prompt_used": prompt,
                "error": f"Tipo no soportado: {ext}",
            }

        raw = response.text or ""
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        data = json.loads(m.group()) if m else {"respuesta_raw": raw}
        return {"success": True, "data": data, "error": None, "prompt_used": prompt}

    except Exception as exc:
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
                    with ui.row().classes("items-center justify-between w-full px-4 py-2 flex-shrink-0").style(
                        f"background:{_HDR_BG};border-bottom:1px solid {_HDR_BORDER}"
                    ):
                        ui.label(fa["filename"]).style(
                            f"color:{_HDR_COLOR};font-weight:700;font-size:15px"
                        )
                        async def _cerrar():
                            dlg.close()
                        ui.button(icon="close", on_click=_cerrar).props("flat round dense")

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
                                        "neto", "importe", "valor", "suma",
                                    }

                                    def _is_money_key(k: str) -> bool:
                                        kl = k.lower()
                                        return any(mk in kl for mk in _MONEY_KEYS)

                                    def _fmt_money(val) -> str:
                                        try:
                                            n = float(val)
                                            s = f"{abs(n):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                                            return f"$ {'-' if n < 0 else ''}{s}"
                                        except Exception:
                                            return str(val)

                                    def _render_value(k: str, val) -> None:
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
                                                    "font-variant-numeric:tabular-nums"
                                                )
                                            else:
                                                ui.label(str(val)).style(
                                                    f"font-size:11px;color:{_BLUE};"
                                                    "font-variant-numeric:tabular-nums"
                                                )
                                            return
                                        # String / fallback
                                        ui.label(str(val)).style("font-size:11px;color:#333")

                                    for k, v in d.items():
                                        with ui.row().classes("w-full items-start gap-2 py-1").style(
                                            "border-bottom:1px solid #f0f0f0"
                                        ):
                                            ui.label(k).classes("text-xs text-gray-500 font-medium").style(
                                                "width:140px;flex-shrink:0"
                                            )
                                            _render_value(k, v)

                            _render_kv(data_dict)

                            ui.label("Prompt usado").classes("text-xs font-semibold text-gray-600 mt-3")
                            prompt_ta = (
                                ui.textarea(value=prompt_init)
                                .props("outlined dense autogrow readonly")
                                .classes("w-full")
                                .style("font-size:11px")
                            )
                            reproc_lbl = ui.label("").classes("text-xs text-gray-500 mt-1")

                            async def _reprocesar() -> None:
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
                                ui.button("Re-procesar", on_click=_reprocesar).style(
                                    f"background:{_YELLOW};color:white;font-size:12px"
                                ).props("dense")
                                ui.button("Aprobar", on_click=_aprobar).style(
                                    f"background:{_GREEN};color:white;font-size:12px"
                                ).props("dense")

                            def _start_edit() -> None:
                                prompt_ta.props(remove="readonly")
                                edit_btn.classes(add="hidden")
                                guardar_btn.classes(remove="hidden")
                                cancelar_btn.classes(remove="hidden")

                            async def _guardar_prompt() -> None:
                                upsert_gastos_prompt(user_id, seccion, prompt_ta.value)
                                prompt_ta.props("readonly")
                                edit_btn.classes(remove="hidden")
                                guardar_btn.classes(add="hidden")
                                cancelar_btn.classes(add="hidden")
                                ui.notify("Prompt guardado", color="positive")

                            def _cancelar_edit() -> None:
                                prompt_ta.value = prompt_init
                                prompt_ta.props("readonly")
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
            footer_lbl_ref: list = [None]
            proc_lbl_ref:   list = [None]

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

                    ui.upload(
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
                    btn_lbl   = "Reprocesar" if is_proc else "Procesar"
                    btn_color = _GREEN if is_proc else _BLUE

                    async def _procesar(sk_=sk) -> None:
                        pl = proc_lbl_ref[0]
                        fs = archivos_por_sec.get(sk_, [])
                        if not fs:
                            ui.notify("No hay archivos para procesar", color="warning")
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

                    ui.button(btn_lbl, on_click=_procesar).style(
                        f"background:{btn_color};color:white;font-size:12px;padding:4px 14px;border-radius:4px"
                    )

        # ── Grid de tarjetas ──────────────────────────────────────────────────
        with content:
            with ui.grid(columns=2).classes("w-full gap-4"):
                for sk, lbl, ext, mul, icon in _SECCIONES[:4]:
                    _build_section_card(sk, lbl, ext, mul, icon)

            sk, lbl, ext, mul, icon = _SECCIONES[4]
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
