"""
tabs/arca.py
Pestaña ARCA: carga manual de variables fiscales mensuales.
Funciones exportadas: build_tab_arca
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from nicegui import app, ui

from db import (
    get_arca_datos,
    save_arca_datos,
    get_arca_multilateral,
    save_arca_multilateral,
)

_GREEN  = "#3B6D11"
_YELLOW = "#BA7517"
_RED    = "#A32D2D"
_DOT_BASE = "display:inline-block;width:12px;height:12px;border-radius:9999px;flex-shrink:0"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _require_login() -> Optional[Dict[str, Any]]:
    user = app.storage.user.get("user")
    if not user:
        ui.notify("Debes iniciar sesión para continuar", color="negative")
    return user


def _to_float(val: Any, default: float = 0.0) -> float:
    try:
        return float(val) if val is not None and str(val).strip() != "" else default
    except (ValueError, TypeError):
        return default


def _fmt_ts(ts: Optional[str]) -> str:
    if not ts:
        return "Sin datos guardados"
    try:
        return datetime.fromisoformat(ts).strftime("Actualizado: %d/%m/%Y %H:%M")
    except Exception:
        return f"Actualizado: {ts}"


# ---------------------------------------------------------------------------
# Semáforo — funciones de color
# ---------------------------------------------------------------------------

def _color_siper(cat: str) -> str:
    c = (cat or "").strip().upper()
    if c.startswith("A"):
        return _GREEN
    if c.startswith("B") or c.startswith("C"):
        return _YELLOW
    return _RED


def _color_iva(tecnico: str, libre: str) -> str:
    t = _to_float(tecnico)
    l = _to_float(libre)
    if t >= 0 and l >= 0:
        return _GREEN
    if t < 0 and l >= 0:
        return _YELLOW
    return _RED


def _color_sire(retenciones: str, umbral: float = 50_000) -> str:
    r = _to_float(retenciones)
    if r < umbral:
        return _GREEN
    if r < umbral * 2:
        return _YELLOW
    return _RED


def _color_deuda(deuda: str, intimacion: bool) -> str:
    if intimacion:
        return _RED
    if _to_float(deuda) > 0:
        return _RED
    return _GREEN


def _color_multilateral(filas: List[Dict]) -> str:
    for f in filas:
        if _to_float(f.get("saldo")) < 0:
            return _YELLOW
    return _GREEN


# ---------------------------------------------------------------------------
# UI helpers
# ---------------------------------------------------------------------------

def _set_dot(dot, color: str) -> None:
    dot.style(f"{_DOT_BASE};background:{color}")


def _card_header(title: str, color: str):
    """Devuelve el elemento dot para actualizar el color después."""
    with ui.row().classes("items-center gap-2 w-full mb-1"):
        dot = ui.element("span").style(f"{_DOT_BASE};background:{color}")
        ui.label(title).classes("font-bold text-base text-gray-800")
    return dot


def _instructions_box(text: str) -> None:
    with ui.expansion("Instrucciones", icon="help_outline").classes("w-full text-sm"):
        with ui.card().classes("w-full").style("background:#f5f5f5;border:0;padding:8px 12px"):
            with ui.row().classes("items-start gap-2 w-full"):
                ui.label(text).classes("text-sm text-gray-700 flex-1")
                ui.button(
                    icon="content_copy",
                    on_click=lambda t=text: ui.run_javascript(
                        f"navigator.clipboard.writeText({json.dumps(t)})"
                    ),
                ).props("flat dense round").tooltip("Copiar").classes("text-gray-500")


# ---------------------------------------------------------------------------
# Función exportada
# ---------------------------------------------------------------------------

def build_tab_arca(container) -> None:
    """Pestaña ARCA: variables fiscales mensuales."""
    user = _require_login()
    if not user:
        return
    with container:
        _build_arca()


def _build_arca() -> None:
    # ── Cargar datos guardados ────────────────────────────────────────────────
    siper_d = get_arca_datos("siper")
    iva_d   = get_arca_datos("iva")
    sire_d  = get_arca_datos("sire")
    deuda_d = get_arca_datos("deuda")
    ml_rows = get_arca_multilateral()
    clae_d  = get_arca_datos("clae")

    # ── Colores iniciales ─────────────────────────────────────────────────────
    c_siper = _color_siper(siper_d.get("categoria_siper", ""))
    c_iva   = _color_iva(iva_d.get("saldo_tecnico", ""), iva_d.get("saldo_libre_disponibilidad", ""))
    c_sire  = _color_sire(sire_d.get("retenciones_mes", ""), _to_float(sire_d.get("umbral_sire"), 50_000))
    c_deuda = _color_deuda(deuda_d.get("deuda_exigible", ""), deuda_d.get("tiene_intimacion", "") == "true")
    c_multi = _color_multilateral(ml_rows)

    semaforos: Dict[str, str] = {
        "siper": c_siper, "iva": c_iva, "sire": c_sire,
        "deuda": c_deuda, "multilateral": c_multi, "clae": _GREEN,
    }

    with ui.column().classes("w-full gap-4 p-4").style("max-width:740px"):

        # ── Banner de alerta ──────────────────────────────────────────────────
        has_alert = _RED in semaforos.values()
        alert_card = ui.card().style(
            f"background:{_RED};display:{'flex' if has_alert else 'none'};"
            "padding:12px 16px;border-radius:6px;border:0;width:100%;margin-bottom:4px"
        )
        with alert_card:
            ui.label(
                "ALERTA: Hay campos que requieren atención inmediata (deuda, intimación o SIPER D/E)."
            ).style("color:white;font-weight:bold")

        def _refresh_alert() -> None:
            show = _RED in semaforos.values()
            alert_card.style(
                f"background:{_RED};display:{'flex' if show else 'none'};"
                "padding:12px 16px;border-radius:6px;border:0;width:100%;margin-bottom:4px"
            )

        # ─────────────────────────────────────────────────────────────────────
        # Card 1 — SIPER / Condición IVA
        # ─────────────────────────────────────────────────────────────────────
        with ui.card().classes("w-full").style("border:1px solid #e0e0e0"):
            dot_siper = _card_header("SIPER / Condición IVA", c_siper)
            _instructions_box(
                "arca.gob.ar → Servicios → SIPER → Consulta de categoría. "
                "Ingresá con CUIT y clave fiscal nivel 2."
            )
            with ui.column().classes("w-full gap-3 mt-2"):
                inp_cat  = ui.input("Categoría SIPER (ej: A — Sin observaciones)",
                                    value=siper_d.get("categoria_siper", "")).classes("w-full")
                inp_cond = ui.input("Condición IVA",
                                    value=siper_d.get("condicion_iva", "")).classes("w-full")
            siper_ts = ui.label(_fmt_ts(siper_d.get("_ts"))).classes("text-xs text-gray-400 mt-1")

            def _upd_siper(_=None) -> None:
                col = _color_siper(inp_cat.value)
                _set_dot(dot_siper, col)
                semaforos["siper"] = col
                _refresh_alert()

            inp_cat.on("input", _upd_siper)

            def _save_siper() -> None:
                now = datetime.now().isoformat(timespec="seconds")
                save_arca_datos("siper", {
                    "categoria_siper": inp_cat.value,
                    "condicion_iva":   inp_cond.value,
                    "_ts": now,
                })
                siper_ts.text = _fmt_ts(now)
                _upd_siper()
                ui.notify("SIPER guardado", color="positive")

            ui.button("Guardar", on_click=_save_siper).props("flat dense").classes("text-sm mt-2")

        # ─────────────────────────────────────────────────────────────────────
        # Card 2 — Saldo IVA
        # ─────────────────────────────────────────────────────────────────────
        with ui.card().classes("w-full").style("border:1px solid #e0e0e0"):
            dot_iva = _card_header("Saldo IVA", c_iva)
            _instructions_box(
                "arca.gob.ar → Mis aplicaciones → IVA → Declaración jurada → "
                "Ver saldos acumulados."
            )
            with ui.row().classes("w-full gap-3 mt-2 flex-wrap"):
                inp_tec = ui.number("Saldo técnico ($)",
                                    value=_to_float(iva_d.get("saldo_tecnico")),
                                    format="%.2f").classes("flex-1")
                inp_lib = ui.number("Saldo libre disponibilidad ($)",
                                    value=_to_float(iva_d.get("saldo_libre_disponibilidad")),
                                    format="%.2f").classes("flex-1")
            iva_ts = ui.label(_fmt_ts(iva_d.get("_ts"))).classes("text-xs text-gray-400 mt-1")

            def _upd_iva(_=None) -> None:
                col = _color_iva(str(inp_tec.value if inp_tec.value is not None else 0),
                                  str(inp_lib.value if inp_lib.value is not None else 0))
                _set_dot(dot_iva, col)
                semaforos["iva"] = col
                _refresh_alert()

            inp_tec.on("update:model-value", _upd_iva)
            inp_lib.on("update:model-value", _upd_iva)

            def _save_iva() -> None:
                now = datetime.now().isoformat(timespec="seconds")
                save_arca_datos("iva", {
                    "saldo_tecnico":              str(inp_tec.value or 0),
                    "saldo_libre_disponibilidad": str(inp_lib.value or 0),
                    "_ts": now,
                })
                iva_ts.text = _fmt_ts(now)
                _upd_iva()
                ui.notify("IVA guardado", color="positive")

            ui.button("Guardar", on_click=_save_iva).props("flat dense").classes("text-sm mt-2")

        # ─────────────────────────────────────────────────────────────────────
        # Card 3 — Retenciones / Percepciones (SIRE)
        # ─────────────────────────────────────────────────────────────────────
        with ui.card().classes("w-full").style("border:1px solid #e0e0e0"):
            dot_sire = _card_header("Retenciones / Percepciones (SIRE)", c_sire)
            _instructions_box(
                "arca.gob.ar → SIRE → Consulta de retenciones y percepciones sufridas → "
                "Filtrar por período actual."
            )
            with ui.row().classes("w-full gap-3 mt-2 flex-wrap"):
                inp_ret = ui.number("Retenciones del mes ($)",
                                    value=_to_float(sire_d.get("retenciones_mes")),
                                    format="%.2f").classes("flex-1")
                inp_per = ui.number("Percepciones del mes ($)",
                                    value=_to_float(sire_d.get("percepciones_mes")),
                                    format="%.2f").classes("flex-1")
            with ui.row().classes("items-center gap-2 mt-1"):
                ui.label("Umbral de alerta ($):").classes("text-sm text-gray-600")
                inp_umbral = ui.number(
                    value=_to_float(sire_d.get("umbral_sire"), 50_000),
                    format="%.0f",
                ).style("width:130px")
            sire_ts = ui.label(_fmt_ts(sire_d.get("_ts"))).classes("text-xs text-gray-400 mt-1")

            def _upd_sire(_=None) -> None:
                umbral = _to_float(inp_umbral.value, 50_000)
                col = _color_sire(str(inp_ret.value if inp_ret.value is not None else 0), umbral)
                _set_dot(dot_sire, col)
                semaforos["sire"] = col
                _refresh_alert()

            inp_ret.on("update:model-value", _upd_sire)
            inp_umbral.on("update:model-value", _upd_sire)

            def _save_sire() -> None:
                now = datetime.now().isoformat(timespec="seconds")
                save_arca_datos("sire", {
                    "retenciones_mes": str(inp_ret.value or 0),
                    "percepciones_mes": str(inp_per.value or 0),
                    "umbral_sire":     str(inp_umbral.value or 50_000),
                    "_ts": now,
                })
                sire_ts.text = _fmt_ts(now)
                _upd_sire()
                ui.notify("SIRE guardado", color="positive")

            ui.button("Guardar", on_click=_save_sire).props("flat dense").classes("text-sm mt-2")

        # ─────────────────────────────────────────────────────────────────────
        # Card 4 — Deuda / Planes activos
        # ─────────────────────────────────────────────────────────────────────
        with ui.card().classes("w-full").style("border:1px solid #e0e0e0"):
            dot_deuda = _card_header("Deuda / Planes activos", c_deuda)
            _instructions_box(
                "arca.gob.ar → Mis aplicaciones → Deuda → Consulta de deuda consolidada. "
                "Planes: sección Mis Planes."
            )
            with ui.column().classes("w-full gap-3 mt-2"):
                with ui.row().classes("w-full gap-3 flex-wrap"):
                    inp_deuda  = ui.number("Deuda exigible ($)",
                                           value=_to_float(deuda_d.get("deuda_exigible")),
                                           format="%.2f").classes("flex-1")
                    inp_planes = ui.number("Planes activos",
                                           value=int(_to_float(deuda_d.get("planes_activos"))),
                                           format="%.0f").classes("flex-1")
                inp_intim = ui.checkbox(
                    "Tiene intimación",
                    value=deuda_d.get("tiene_intimacion", "") == "true",
                )
            deuda_ts = ui.label(_fmt_ts(deuda_d.get("_ts"))).classes("text-xs text-gray-400 mt-1")

            def _upd_deuda(_=None) -> None:
                col = _color_deuda(str(inp_deuda.value if inp_deuda.value is not None else 0),
                                   inp_intim.value)
                _set_dot(dot_deuda, col)
                semaforos["deuda"] = col
                _refresh_alert()

            inp_deuda.on("update:model-value", _upd_deuda)
            inp_intim.on("update:model-value", _upd_deuda)

            def _save_deuda() -> None:
                now = datetime.now().isoformat(timespec="seconds")
                save_arca_datos("deuda", {
                    "deuda_exigible":   str(inp_deuda.value or 0),
                    "planes_activos":   str(int(inp_planes.value or 0)),
                    "tiene_intimacion": "true" if inp_intim.value else "false",
                    "_ts": now,
                })
                deuda_ts.text = _fmt_ts(now)
                _upd_deuda()
                ui.notify("Deuda guardado", color="positive")

            ui.button("Guardar", on_click=_save_deuda).props("flat dense").classes("text-sm mt-2")

        # ─────────────────────────────────────────────────────────────────────
        # Card 5 — Convenio Multilateral
        # ─────────────────────────────────────────────────────────────────────
        with ui.card().classes("w-full").style("border:1px solid #e0e0e0"):
            dot_multi = _card_header("Convenio Multilateral por provincia", c_multi)
            _instructions_box(
                "arca.gob.ar → Convenio Multilateral → Saldos por jurisdicción → "
                "Seleccionar período y exportar."
            )

            _ml: List[Dict[str, Any]] = [
                {"provincia": r.get("provincia", ""), "saldo": _to_float(r.get("saldo"))}
                for r in ml_rows
            ]
            ml_ts_val = ml_rows[0].get("updated_at") if ml_rows else None

            ml_rows_col = ui.column().classes("w-full gap-2 mt-2")
            multi_ts = ui.label(_fmt_ts(ml_ts_val)).classes("text-xs text-gray-400")

            def _upd_multi() -> None:
                col = _color_multilateral(_ml)
                _set_dot(dot_multi, col)
                semaforos["multilateral"] = col
                _refresh_alert()

            def _render_ml() -> None:
                ml_rows_col.clear()
                with ml_rows_col:
                    for i, row in enumerate(_ml):
                        with ui.row().classes("items-center gap-2 w-full flex-wrap"):
                            prov_inp = ui.input("Provincia", value=row.get("provincia", "")).style("width:170px")
                            sald_inp = ui.number("Saldo ($)", value=_to_float(row.get("saldo")),
                                                  format="%.2f").style("width:150px")

                            def _bind(idx: int, pi, si) -> None:
                                def _on_prov(_=None) -> None:
                                    _ml[idx]["provincia"] = pi.value

                                def _on_sald(_=None) -> None:
                                    _ml[idx]["saldo"] = _to_float(si.value)
                                    _upd_multi()

                                pi.on("input", _on_prov)
                                si.on("update:model-value", _on_sald)

                            _bind(i, prov_inp, sald_inp)

                            def _del(idx=i) -> None:
                                _ml.pop(idx)
                                _render_ml()
                                _upd_multi()

                            ui.button(icon="delete_outline", on_click=_del).props(
                                "flat dense round"
                            ).classes("text-red-700").tooltip("Quitar fila")

            _render_ml()

            def _add_row() -> None:
                _ml.append({"provincia": "", "saldo": 0.0})
                _render_ml()

            def _save_multi() -> None:
                # Sync any province still in-flight (already synced via on_input, but ensure saldo)
                save_arca_multilateral(_ml)
                now = datetime.now().isoformat(timespec="seconds")
                multi_ts.text = _fmt_ts(now)
                _upd_multi()
                ui.notify("Multilateral guardado", color="positive")

            with ui.row().classes("gap-2 mt-2 items-center"):
                ui.button("Agregar provincia", icon="add", on_click=_add_row).props("flat dense").classes("text-sm")
                ui.button("Guardar", on_click=_save_multi).props("flat dense").classes("text-sm")
            multi_ts  # already added above

        # ─────────────────────────────────────────────────────────────────────
        # Card 6 — Actividades CLAE (siempre verde, informativo)
        # ─────────────────────────────────────────────────────────────────────
        with ui.card().classes("w-full").style("border:1px solid #e0e0e0"):
            _card_header("Actividades CLAE", _GREEN)
            _instructions_box(
                "arca.gob.ar → Mis datos registrales → Actividades → "
                "Ver actividades declaradas (CLAE)."
            )
            with ui.column().classes("w-full gap-3 mt-2"):
                inp_princ = ui.input("Actividad principal (CLAE)",
                                     value=clae_d.get("actividad_principal", "")).classes("w-full")
                inp_sec   = ui.input("Actividad secundaria (opcional)",
                                     value=clae_d.get("actividad_secundaria", "")).classes("w-full")
            clae_ts = ui.label(_fmt_ts(clae_d.get("_ts"))).classes("text-xs text-gray-400 mt-1")

            def _save_clae() -> None:
                now = datetime.now().isoformat(timespec="seconds")
                save_arca_datos("clae", {
                    "actividad_principal":  inp_princ.value,
                    "actividad_secundaria": inp_sec.value,
                    "_ts": now,
                })
                clae_ts.text = _fmt_ts(now)
                ui.notify("CLAE guardado", color="positive")

            ui.button("Guardar", on_click=_save_clae).props("flat dense").classes("text-sm mt-2")
