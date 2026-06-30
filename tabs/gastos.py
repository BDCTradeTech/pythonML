"""
tabs/gastos.py
Pestaña Gastos: gestión de documentos impositivos mensuales por sección.
Funciones exportadas: build_tab_gastos
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from nicegui import app, ui

from db import (
    delete_gastos_archivo,
    get_gastos_archivos,
    insert_gastos_archivo,
    mark_gastos_procesado,
)

_BLUE       = "#2A7AC7"
_BLUE_BG    = "#EEF6FD"
_HDR_BG     = "#EEF6FD"
_HDR_COLOR  = "#185FA5"
_HDR_BORDER = "#85B7EB"
_GREEN      = "#3B6D11"
_YELLOW     = "#E2A93B"
_GRAY       = "#9E9E9E"

_DOT = "display:inline-block;width:12px;height:12px;border-radius:9999px;flex-shrink:0;background:{}"

_SECCIONES: List[tuple] = [
    # (key, label, accept_ext, multiple, icon)
    ("facturas_ml",    "Facturas MercadoLibre",    ".pdf",  True,  "ti-file-invoice"),
    ("retenciones",    "Retenciones",              ".xlsx", True,  "ti-file-spreadsheet"),
    ("percepciones",   "Percepciones",             ".xlsx", True,  "ti-file-spreadsheet"),
    ("pagos_arca",     "Pagos ARCA",               ".pdf",  True,  "ti-file-invoice"),
    ("operaciones_ml", "Operaciones MercadoLibre", ".xlsx", False, "ti-file-spreadsheet"),
]

_MESES = [
    "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
    "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre",
]

_BASE_PATH = Path(__file__).parent.parent / "gastos"


def _require_login() -> Optional[Dict[str, Any]]:
    user = app.storage.user.get("user")
    if not user:
        ui.notify("Debes iniciar sesión para continuar", color="negative")
    return user


def _fmt_size(n: int) -> str:
    if n >= 1_048_576:
        return f"{n / 1_048_576:.1f} MB"
    if n >= 1024:
        return f"{n / 1024:.0f} KB"
    return f"{n} B"


def _semaforo(archivos: list, procesado: bool) -> str:
    if not archivos:
        return _GRAY
    return _GREEN if procesado else _YELLOW


# ---------------------------------------------------------------------------
# Función exportada
# ---------------------------------------------------------------------------


def build_tab_gastos(container) -> None:
    """Pestaña Gastos: documentos impositivos por período."""
    user = _require_login()
    if not user:
        return
    with container:
        _build_gastos(user["id"])


# ---------------------------------------------------------------------------
# UI principal
# ---------------------------------------------------------------------------


def _build_gastos(user_id: int) -> None:
    now = datetime.now()

    # ── Barra superior ───────────────────────────────────────────────────────
    with ui.row().classes("w-full items-center gap-4 flex-wrap").style(
        "background:#f8fafc;border-bottom:1px solid #e0e0e0;padding:10px 16px"
    ):
        with ui.row().classes("items-center gap-2"):
            ui.label("Período:").classes("font-semibold text-sm text-gray-600")
            mes_sel = ui.select(
                options=_MESES,
                value=_MESES[now.month - 1],
            ).style("width:148px")
            ano_sel = ui.select(
                options=[str(y) for y in range(now.year - 2, now.year + 2)],
                value=str(now.year),
            ).style("width:90px")

        progress_lbl = ui.label("").classes("text-sm text-gray-500 ml-2")
        size_lbl     = ui.label("").classes("text-sm text-gray-400")

    # ── Área de contenido (se reconstruye al cambiar el período) ─────────────
    content = ui.column().classes("w-full p-4 gap-4")

    def _get_periodo() -> str:
        mes_idx = _MESES.index(mes_sel.value) + 1
        return f"{ano_sel.value}-{mes_idx:02d}"

    def _build_content() -> None:
        content.clear()
        periodo = _get_periodo()

        archivos_por_sec: Dict[str, list] = {}
        procesado_por_sec: Dict[str, bool] = {}
        dot_refs:    Dict[str, Any] = {}
        subtitle_ref: list = [None]
        final_btn_ref: list = [None]

        def _count_procesadas() -> int:
            return sum(1 for sk, *_ in _SECCIONES if procesado_por_sec.get(sk, False))

        def _refresh_progress() -> None:
            n     = _count_procesadas()
            total = len(_SECCIONES)
            pct   = int(n / total * 100) if total else 0
            progress_lbl.text = f"{n} de {total} secciones procesadas — {pct}%"

            total_files = sum(len(v) for v in archivos_por_sec.values())
            total_bytes = sum(
                sum(f.get("size_bytes", 0) for f in v)
                for v in archivos_por_sec.values()
            )
            size_lbl.text = f"{total_files} archivo(s) — {_fmt_size(total_bytes)}" if total_files else ""

            if subtitle_ref[0]:
                if n < total:
                    subtitle_ref[0].text = (
                        f"Disponible una vez que las 5 secciones estén procesadas — actualmente {n} de {total}"
                    )
                else:
                    subtitle_ref[0].text = "Todas las secciones procesadas — listo para analizar"

            if final_btn_ref[0]:
                if n == total:
                    final_btn_ref[0].enable()
                    final_btn_ref[0].style(
                        f"background:{_BLUE};color:white;font-size:14px;"
                        "font-weight:600;padding:10px 24px;margin-top:8px"
                    )
                else:
                    final_btn_ref[0].disable()
                    final_btn_ref[0].style(
                        f"background:{_BLUE};color:white;font-size:14px;"
                        "font-weight:600;padding:10px 24px;margin-top:8px;opacity:0.4;cursor:not-allowed"
                    )

        def _refresh_dot(sk: str) -> None:
            dot = dot_refs.get(sk)
            if dot:
                color = _semaforo(archivos_por_sec.get(sk, []), procesado_por_sec.get(sk, False))
                dot.style(_DOT.format(color))

        # ── Construir tarjeta de sección ─────────────────────────────────────
        def _build_section_card(sk: str, lbl: str, ext: str, multiple: bool, icon: str) -> None:
            rows      = get_gastos_archivos(user_id, periodo, sk)
            archivos_por_sec[sk] = rows
            all_proc  = bool(rows and all(r.get("procesado") for r in rows))
            procesado_por_sec[sk] = all_proc

            with ui.card().classes("w-full").style("border:1px solid #e0e0e0"):
                # Header
                with ui.row().classes("items-center gap-2 w-full px-3 py-2").style(
                    f"background:{_HDR_BG};border-bottom:1px solid {_HDR_BORDER};"
                    "border-radius:4px 4px 0 0"
                ):
                    dot = ui.element("span").style(_DOT.format(_semaforo(rows, all_proc)))
                    dot_refs[sk] = dot
                    ui.element("i").classes(f"ti {icon}").style(
                        f"color:{_HDR_COLOR};font-size:16px"
                    )
                    ui.label(lbl).style(
                        f"color:{_HDR_COLOR};font-weight:600;font-size:14px"
                    )
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
                            return
                        for fa in fs:
                            ok = "✓ " if fa.get("procesado") else ""
                            with ui.row().classes("items-center gap-1 w-full flex-nowrap"):
                                ui.label(
                                    f"{ok}{fa['filename']}  ({_fmt_size(fa.get('size_bytes', 0))})"
                                ).classes("text-xs flex-1 truncate text-gray-700")

                                def _del(fid=fa["id"], sk2=sk_) -> None:
                                    row = next(
                                        (x for x in archivos_por_sec[sk2] if x["id"] == fid), None
                                    )
                                    if row:
                                        try:
                                            Path(row["filepath"]).unlink(missing_ok=True)
                                        except Exception:
                                            pass
                                        delete_gastos_archivo(fid)
                                        archivos_por_sec[sk2] = get_gastos_archivos(user_id, periodo, sk2)
                                        procesado_por_sec[sk2] = bool(
                                            archivos_por_sec[sk2]
                                            and all(f.get("procesado") for f in archivos_por_sec[sk2])
                                        )
                                    _render_list(sk2)
                                    _refresh_dot(sk2)
                                    _refresh_progress()

                                ui.button(
                                    icon="ti-trash", on_click=_del
                                ).props("flat dense round").classes(
                                    "text-red-600"
                                ).style("font-size:14px").tooltip("Eliminar")

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
                            user_id=user_id,
                            periodo=periodo,
                            seccion=sk_,
                            filename=e.name,
                            filepath=str(dest),
                            size_bytes=len(data),
                        )
                        archivos_por_sec[sk_] = get_gastos_archivos(user_id, periodo, sk_)
                        procesado_por_sec[sk_] = False
                        _render_list(sk_)
                        _refresh_dot(sk_)
                        _refresh_progress()
                        ui.notify(f"'{e.name}' subido", color="positive")

                    ui.upload(
                        multiple=multiple,
                        auto_upload=True,
                        on_upload=_on_upload,
                        label="Arrastrá archivos aquí o hacé clic",
                    ).props(
                        f'accept="{ext}" flat hide-upload-btn color="primary"'
                    ).classes("w-full").style(
                        "border:2px dashed #85B7EB;border-radius:6px;"
                        "background:#f9fbfe;min-height:56px"
                    )

                # Footer
                with ui.row().classes("items-center gap-2 px-3 pb-3 pt-1"):
                    cnt = len(archivos_por_sec[sk])
                    ui.label(
                        f"{cnt} archivo(s)" if cnt else "Sin archivos"
                    ).classes("text-xs text-gray-500 flex-1")

                    is_proc   = procesado_por_sec[sk]
                    btn_lbl   = "Reprocesar" if is_proc else "Procesar"
                    btn_color = _GREEN if is_proc else _BLUE

                    def _procesar(sk_=sk) -> None:
                        if not archivos_por_sec.get(sk_):
                            ui.notify("No hay archivos para procesar", color="warning")
                            return
                        for fa in archivos_por_sec[sk_]:
                            mark_gastos_procesado(fa["id"])
                        archivos_por_sec[sk_] = get_gastos_archivos(user_id, periodo, sk_)
                        procesado_por_sec[sk_] = True
                        _render_list(sk_)
                        _refresh_dot(sk_)
                        _refresh_progress()
                        ui.notify("Procesado — implementación pendiente", color="positive")

                    ui.button(
                        btn_lbl, on_click=_procesar
                    ).style(
                        f"background:{btn_color};color:white;font-size:12px;"
                        "padding:4px 14px;border-radius:4px"
                    )

        # ── Contenido principal ───────────────────────────────────────────────
        with content:
            with ui.grid(columns=2).classes("w-full gap-4"):
                for sk, lbl, ext, mul, icon in _SECCIONES[:4]:
                    _build_section_card(sk, lbl, ext, mul, icon)

            # Operaciones ML: fila completa
            sk, lbl, ext, mul, icon = _SECCIONES[4]
            _build_section_card(sk, lbl, ext, mul, icon)

            # Card de análisis final
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

                    fb = ui.button(
                        "Procesar análisis final",
                        on_click=_final_procesar,
                    ).style(
                        f"background:{_BLUE};color:white;font-size:14px;"
                        "font-weight:600;padding:10px 24px;margin-top:8px"
                    )
                    final_btn_ref[0] = fb

            _refresh_progress()

    mes_sel.on("update:model-value", lambda _: _build_content())
    ano_sel.on("update:model-value", lambda _: _build_content())

    _build_content()
