"""
tabs/arca.py
Pestaña ARCA: carga manual de variables fiscales mensuales.
Funciones exportadas: build_tab_arca
"""
from __future__ import annotations

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


def _color_deuda(deuda: str, intimacion: bool) -> str:
    if intimacion:
        return _RED
    if _to_float(deuda) > 0:
        return _RED
    return _GREEN


def _color_multilateral(filas: List[Dict]) -> str:
    if not filas:
        return _RED
    valores = [_to_float(f.get("a_pagar")) for f in filas]
    if any(v > 10_000 for v in valores):
        return _RED
    if any(v > 0 for v in valores):
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


def _hint(text: str) -> None:
    ui.label(text).classes("text-xs text-gray-400 mt-1 mb-1")


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
    deuda_d = get_arca_datos("deuda")
    ml_rows = get_arca_multilateral()
    clae_d  = get_arca_datos("clae")

    # ── Colores iniciales ─────────────────────────────────────────────────────
    c_siper = _color_siper(siper_d.get("categoria_siper", ""))
    c_iva   = _RED if "saldo_tecnico" not in iva_d else _color_iva(
        iva_d.get("saldo_tecnico", ""), iva_d.get("saldo_libre_disponibilidad", "")
    )
    c_deuda = _RED if "deuda_exigible" not in deuda_d else _color_deuda(
        deuda_d.get("deuda_exigible", ""), deuda_d.get("tiene_intimacion", "") == "true"
    )
    c_clae  = _RED if not (clae_d.get("actividad_principal") or "").strip() else _GREEN
    c_multi = _color_multilateral(ml_rows)

    semaforos: Dict[str, str] = {
        "siper": c_siper, "iva": c_iva,
        "deuda": c_deuda, "multilateral": c_multi, "clae": c_clae,
    }

    with ui.column().classes("w-full gap-4 p-4").style("max-width:1100px"):

        # ── Banner de alerta ──────────────────────────────────────────────────
        has_alert = _RED in semaforos.values()
        alert_card = ui.card().style(
            f"background:{_RED};display:{'flex' if has_alert else 'none'};"
            "padding:12px 16px;border-radius:6px;border:0;width:100%;margin-bottom:4px"
        )
        with alert_card:
            ui.label(
                "ALERTA: Hay campos que requieren atención inmediata."
            ).style("color:white;font-weight:bold")

        def _refresh_alert() -> None:
            show = _RED in semaforos.values()
            alert_card.style(
                f"background:{_RED};display:{'flex' if show else 'none'};"
                "padding:12px 16px;border-radius:6px;border:0;width:100%;margin-bottom:4px"
            )

        # ── Grid 2×2 — 4 cards ───────────────────────────────────────────────
        with ui.grid(columns=2).classes("w-full gap-4"):

            # ── Card 1: SIPER ─────────────────────────────────────────────────
            with ui.card().classes("w-full").style("border:1px solid #e0e0e0"):
                dot_siper = _card_header("SIPER", c_siper)
                _hint("arca.gob.ar → Sistema Registral → Trámites → SIPER")
                with ui.column().classes("w-full gap-3 mt-2"):
                    inp_cat = ui.input(
                        "Categoría SIPER (ej: A — Sin observaciones)",
                        value=siper_d.get("categoria_siper", ""),
                    ).classes("w-full")
                siper_ts = ui.label(_fmt_ts(siper_d.get("_ts"))).classes("text-xs text-gray-400 mt-1")

                def _upd_siper(_=None) -> None:
                    col = _color_siper(inp_cat.value)
                    _set_dot(dot_siper, col)
                    semaforos["siper"] = col
                    _refresh_alert()

                inp_cat.on("input", _upd_siper)

                def _save_siper() -> None:
                    now = datetime.now().isoformat(timespec="seconds")
                    save_arca_datos("siper", {"categoria_siper": inp_cat.value, "_ts": now})
                    siper_ts.text = _fmt_ts(now)
                    _upd_siper()
                    ui.notify("SIPER guardado", color="positive")

                ui.button("Guardar", on_click=_save_siper).props("flat dense").classes("text-sm mt-2")

            # ── Card 2: Saldo IVA (F.2051) ────────────────────────────────────
            with ui.card().classes("w-full").style("border:1px solid #e0e0e0"):
                dot_iva = _card_header("Saldo IVA (F.2051)", c_iva)
                _hint("Portal IVA → Consultar → Seleccionar SRL → DJ presentadas → Libro IVA → Ver DJ → Descargar F.2051")
                with ui.column().classes("w-full gap-3 mt-2"):
                    inp_tec = ui.number(
                        "Saldo técnico ($)",
                        value=_to_float(iva_d.get("saldo_tecnico")),
                        format="%.2f",
                    ).classes("w-full")
                    inp_lib = ui.number(
                        "Saldo libre disponibilidad ($)",
                        value=_to_float(iva_d.get("saldo_libre_disponibilidad")),
                        format="%.2f",
                    ).classes("w-full")
                iva_ts = ui.label(_fmt_ts(iva_d.get("_ts"))).classes("text-xs text-gray-400 mt-1")

                def _upd_iva(_=None) -> None:
                    if inp_tec.value is None or inp_lib.value is None:
                        col = _RED
                    else:
                        col = _color_iva(str(inp_tec.value), str(inp_lib.value))
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

            # ── Card 3: Deuda / Planes activos ────────────────────────────────
            with ui.card().classes("w-full").style("border:1px solid #e0e0e0"):
                dot_deuda = _card_header("Deuda / Planes activos", c_deuda)
                _hint("arca.gob.ar → Sistemas de Cuentas Tributarias → Vencimientos")
                with ui.column().classes("w-full gap-3 mt-2"):
                    inp_deuda = ui.number(
                        "Deuda exigible ($)",
                        value=_to_float(deuda_d.get("deuda_exigible")),
                        format="%.2f",
                    ).classes("w-full")
                    inp_planes = ui.number(
                        "Planes activos",
                        value=int(_to_float(deuda_d.get("planes_activos"))),
                        format="%.0f",
                    ).classes("w-full")
                    inp_intim = ui.checkbox(
                        "Tiene intimación",
                        value=deuda_d.get("tiene_intimacion", "") == "true",
                    )
                deuda_ts = ui.label(_fmt_ts(deuda_d.get("_ts"))).classes("text-xs text-gray-400 mt-1")

                def _upd_deuda(_=None) -> None:
                    if inp_deuda.value is None:
                        col = _RED
                    else:
                        col = _color_deuda(str(inp_deuda.value), inp_intim.value)
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

            # ── Card 4: Actividades CLAE ──────────────────────────────────────
            with ui.card().classes("w-full").style("border:1px solid #e0e0e0"):
                dot_clae = _card_header("Actividades CLAE", c_clae)
                _hint("arca.gob.ar → Sistema Registral → Registro Único Tributario → Actividades")
                with ui.column().classes("w-full gap-3 mt-2"):
                    inp_princ = ui.input(
                        "Actividad principal",
                        value=clae_d.get("actividad_principal", ""),
                    ).classes("w-full")
                    inp_sec = ui.input(
                        "Actividad secundaria (opcional)",
                        value=clae_d.get("actividad_secundaria", ""),
                    ).classes("w-full")
                clae_ts = ui.label(_fmt_ts(clae_d.get("_ts"))).classes("text-xs text-gray-400 mt-1")

                def _upd_clae(_=None) -> None:
                    col = _RED if not (inp_princ.value or "").strip() else _GREEN
                    _set_dot(dot_clae, col)
                    semaforos["clae"] = col
                    _refresh_alert()

                inp_princ.on("input", _upd_clae)

                def _save_clae() -> None:
                    now = datetime.now().isoformat(timespec="seconds")
                    save_arca_datos("clae", {
                        "actividad_principal":  inp_princ.value,
                        "actividad_secundaria": inp_sec.value,
                        "_ts": now,
                    })
                    clae_ts.text = _fmt_ts(now)
                    _upd_clae()
                    ui.notify("CLAE guardado", color="positive")

                ui.button("Guardar", on_click=_save_clae).props("flat dense").classes("text-sm mt-2")

        # ── Card 5: Convenio Multilateral CM03 (ancho completo) ───────────────
        with ui.card().classes("w-full").style("border:1px solid #e0e0e0"):
            dot_multi = _card_header("Convenio Multilateral CM03", c_multi)
            _hint("Convenio Multilateral → SIFERE Web → DDJJ → DDJJ mensuales → Listado de DDJJ → Imprimir")

            _ml: List[Dict[str, Any]] = [
                {
                    "provincia":       r.get("provincia", ""),
                    "alicuota":        _to_float(r.get("alicuota")),
                    "a_favor_contrib": _to_float(r.get("a_favor_contrib")),
                    "a_favor_fisco":   _to_float(r.get("a_favor_fisco")),
                    "a_pagar":         _to_float(r.get("a_pagar")),
                }
                for r in ml_rows
            ]
            ml_ts_val = ml_rows[0].get("updated_at") if ml_rows else None

            ml_rows_col = ui.column().classes("w-full gap-1 mt-2")
            multi_ts = ui.label(_fmt_ts(ml_ts_val)).classes("text-xs text-gray-400")

            def _upd_multi() -> None:
                col = _color_multilateral(_ml)
                _set_dot(dot_multi, col)
                semaforos["multilateral"] = col
                _refresh_alert()

            def _render_ml() -> None:
                ml_rows_col.clear()
                with ml_rows_col:
                    with ui.row().classes("w-full gap-2 items-center pb-1").style(
                        "border-bottom:1px solid #e0e0e0"
                    ):
                        ui.label("Provincia").classes("text-xs text-gray-500 font-semibold").style("width:160px;min-width:160px")
                        ui.label("Alícuota %").classes("text-xs text-gray-500 font-semibold").style("width:100px;min-width:100px")
                        ui.label("A favor contrib ($)").classes("text-xs text-gray-500 font-semibold").style("width:140px;min-width:140px")
                        ui.label("A favor fisco ($)").classes("text-xs text-gray-500 font-semibold").style("width:130px;min-width:130px")
                        ui.label("A pagar ($)").classes("text-xs text-gray-500 font-semibold").style("width:120px;min-width:120px")
                    for i, row in enumerate(_ml):
                        with ui.row().classes("items-center gap-2 w-full flex-nowrap"):
                            prov_inp   = ui.input(value=row.get("provincia", "")).style("width:160px;min-width:160px")
                            alic_inp   = ui.number(value=_to_float(row.get("alicuota")),        format="%.4f").style("width:100px;min-width:100px")
                            contrib_inp = ui.number(value=_to_float(row.get("a_favor_contrib")), format="%.2f").style("width:140px;min-width:140px")
                            fisco_inp  = ui.number(value=_to_float(row.get("a_favor_fisco")),   format="%.2f").style("width:130px;min-width:130px")
                            pagar_inp  = ui.number(value=_to_float(row.get("a_pagar")),         format="%.2f").style("width:120px;min-width:120px")

                            def _bind(idx: int, pi, al, co, fi, pa) -> None:
                                def _on_prov(_=None):
                                    _ml[idx]["provincia"] = pi.value
                                def _on_alic(_=None):
                                    _ml[idx]["alicuota"] = _to_float(al.value)
                                def _on_contrib(_=None):
                                    _ml[idx]["a_favor_contrib"] = _to_float(co.value)
                                def _on_fisco(_=None):
                                    _ml[idx]["a_favor_fisco"] = _to_float(fi.value)
                                def _on_pagar(_=None):
                                    _ml[idx]["a_pagar"] = _to_float(pa.value)
                                    _upd_multi()
                                pi.on("input", _on_prov)
                                al.on("update:model-value", _on_alic)
                                co.on("update:model-value", _on_contrib)
                                fi.on("update:model-value", _on_fisco)
                                pa.on("update:model-value", _on_pagar)

                            _bind(i, prov_inp, alic_inp, contrib_inp, fisco_inp, pagar_inp)

                            def _del(idx=i) -> None:
                                _ml.pop(idx)
                                _render_ml()
                                _upd_multi()

                            ui.button(icon="delete_outline", on_click=_del).props(
                                "flat dense round"
                            ).classes("text-red-700").tooltip("Quitar fila")

            _render_ml()

            def _add_row() -> None:
                _ml.append({
                    "provincia": "", "alicuota": 0.0,
                    "a_favor_contrib": 0.0, "a_favor_fisco": 0.0, "a_pagar": 0.0,
                })
                _render_ml()

            def _save_multi() -> None:
                save_arca_multilateral(_ml)
                now = datetime.now().isoformat(timespec="seconds")
                multi_ts.text = _fmt_ts(now)
                _upd_multi()
                ui.notify("Multilateral guardado", color="positive")

            with ui.row().classes("gap-2 mt-3 items-center"):
                ui.button("Agregar provincia", icon="add", on_click=_add_row).props("flat dense").classes("text-sm")
                ui.button("Guardar", on_click=_save_multi).props("flat dense").classes("text-sm")
