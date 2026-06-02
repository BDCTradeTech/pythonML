"""
Fase 3 — tabs/datos.py
Pestaña Datos: configuración de parámetros del cotizador y tablas de referencia.
Funciones exportadas: build_tab_datos
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from nicegui import app, run, ui

from db import (
    get_connection,
    get_cotizador_param,
    set_cotizador_param,
    delete_cotizador_param,
    get_cotizador_tabla,
    set_cotizador_tabla,
    COTIZADOR_DEFAULTS,
)
from tabs.admin import (
    TABLA_TRAFO_GRAMOS_DEFAULT,
    TABLA_POSICION_DEFAULT,
    TABLA_ENVIOS_ML_DEFAULT,
    TABLA_COURIER_DEFAULT,
    TABLA_IVA_VS_EXENTO_DEFAULT,
)

# Campos almacenados como decimal (0.155) pero editados como porcentaje (15.5)
PCT_KEYS = {
    "ml_comision", "ml_debcre", "ml_sirtac", "ml_iibb_per",
    "ml_ganancia_neta_venta",
    "cuotas_3x", "cuotas_6x", "cuotas_9x", "cuotas_12x",
    "iva_105", "iva_21", "iibb_lhs",
}

# Campos con formato peso argentino ($ + punto miles)
DOLAR_DISPLAY_KEYS = {"dolar_oficial", "dolar_blue", "dolar_sistema", "dolar_despacho"}
DOLAR_PARSE_KEYS = DOLAR_DISPLAY_KEYS | {"ml_comision_fija_menor", "ml_envios", "ml_envios_gratuitos"}

# Campos con formato dólar (u$)
USD_KEYS = {
    "kilo",
    "valor_kg_miami", "almacenaje_dias_kg_miami", "almacenaje_miami_x2",
    "valor_kg_china", "almacenaje_dias_kg_china", "almacenaje_china_x3",
}

TOOLTIPS: Dict[str, str] = {
    "ml_cobrado":             "Factor de lo que cobra el vendedor. Ej: 0.836 = 83.6% del precio de venta",
    "ml_comision_fija_menor": "Cargo fijo en $ para ventas con precio menor al mínimo",
    "ml_3cuotas":             "Multiplicador de precio para absorber el costo de cuotas. Ej: 1.15 = precio × 1.15",
    "ml_6cuotas":             "Multiplicador de precio para absorber el costo de cuotas. Ej: 1.15 = precio × 1.15",
    "cuotas_3x":  "Tasa de costo de cuotas que ML cobra al vendedor",
    "cuotas_6x":  "Tasa de costo de cuotas que ML cobra al vendedor",
    "cuotas_9x":  "Tasa de costo de cuotas que ML cobra al vendedor",
    "cuotas_12x": "Tasa de costo de cuotas que ML cobra al vendedor",
    "ml_envios_gratuitos": "Precio de venta mínimo a partir del cual el envío es gratuito para el comprador",
}


# ---------------------------------------------------------------------------
# Helper de sesión
# ---------------------------------------------------------------------------

def _require_login() -> Optional[Dict[str, Any]]:
    user = app.storage.user.get("user")
    if not user:
        ui.notify("Debes iniciar sesión para continuar", color="negative")
    return user


# ---------------------------------------------------------------------------
# Función exportada
# ---------------------------------------------------------------------------

def build_tab_datos() -> None:
    user = _require_login()
    if not user:
        return

    uid = user["id"]

    ui.add_head_html('''<style>
.datos-card .q-field__control { min-height: 26px !important; height: 26px !important; }
.datos-card .q-field__marginal { height: 26px !important; }
.datos-card .q-field { padding-bottom: 0 !important; margin-bottom: 0 !important; }
.datos-card .q-field__inner { min-height: 0 !important; padding-bottom: 0 !important; }
.datos-card .q-field__bottom { display: none !important; }
.datos-card .q-field__native { padding: 0 4px !important; font-size: 11px !important; line-height: 26px !important; }
.datos-card .nicegui-input { margin: 0 !important; padding: 0 !important; }
.datos-tabla .q-field { padding-bottom:0 !important; margin-bottom:0 !important; }
.datos-tabla .q-field__inner { min-height:0 !important; padding-bottom:0 !important; }
.datos-tabla .q-field__bottom { display:none !important; }
.datos-tabla .q-field__control { min-height:28px !important; height:28px !important; background:#f9fafb !important; border:0.5px solid #e5e7eb !important; border-radius:4px !important; padding:0 !important; box-shadow:none !important; }
.datos-tabla .q-field__control::before, .datos-tabla .q-field__control::after { display:none !important; }
.datos-tabla .q-field__native { padding:0 6px !important; font-size:12px !important; text-align:right !important; color:#374151 !important; }
.datos-tabla .nicegui-input { margin:0 !important; padding:0 !important; }
.datos-tabla-fila:hover td { background:#f9fafb; }
.datos-trash { transition:color 0.15s; }
.datos-trash:hover { color:#dc2626 !important; }
</style>''')

    def _get(key: str) -> str:
        v = get_cotizador_param(key, uid)
        if v is not None:
            return v
        return COTIZADOR_DEFAULTS.get(key, "")

    def _get_tabla(nombre: str, default: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        r = get_cotizador_tabla(nombre, uid)
        return r if r else default

    # --- Formatters ---

    def _fmt_dolar_display(v: str) -> str:
        if not v or not str(v).strip():
            return ""
        try:
            n = float(str(v).replace(".", "").replace(",", "."))
            return f"{int(n):,}".replace(",", ".")
        except (ValueError, TypeError):
            return str(v)

    def _fmt_usd_display(v: str) -> str:
        if not v or not str(v).strip():
            return ""
        try:
            n = float(str(v).strip().replace(",", "."))
            if n == int(n):
                return f"{int(n):,}".replace(",", ".")
            return f"{n:.2f}".rstrip("0").rstrip(".").replace(".", ",")
        except (ValueError, TypeError):
            return str(v)

    def _parse_dolar(s: Any) -> str:
        if s is None or s == "":
            return ""
        raw = str(s).replace("$", "").replace(".", "").replace(",", ".").strip()
        try:
            n = float(raw)
            return str(int(n)) if n == int(n) else f"{n:.2f}"
        except (ValueError, TypeError):
            return str(s).strip()

    def _parse_usd(s: Any) -> str:
        if s is None or s == "":
            return ""
        raw = str(s).replace("u$", "").replace("$", "").replace(".", "").replace(",", ".").strip()
        try:
            n = float(raw)
            return str(int(n)) if n == int(n) else f"{n:.2f}"
        except (ValueError, TypeError):
            return str(s).strip()

    # --- Conversores campo ↔ BD ---

    def _field_display(key: str, raw: str) -> str:
        if key in PCT_KEYS:
            if not raw or not str(raw).strip():
                return ""
            try:
                n = float(str(raw).replace(",", ".")) * 100
                return str(int(n)) if n == int(n) else f"{n:.4f}".rstrip("0").rstrip(".")
            except (ValueError, TypeError):
                return str(raw)
        if key in DOLAR_DISPLAY_KEYS:
            fmt = _fmt_dolar_display(raw)
            return f"$ {fmt}" if fmt else ""
        if key in (DOLAR_PARSE_KEYS - DOLAR_DISPLAY_KEYS):
            fmt = _fmt_dolar_display(raw)
            return f"$ {fmt}" if fmt else ""
        if key in USD_KEYS:
            fmt = _fmt_usd_display(raw)
            return f"u$ {fmt}" if raw and fmt else ""
        return raw or ""

    def _field_parse(key: str, display_val: str) -> str:
        val = str(display_val or "").strip()
        if key in PCT_KEYS:
            try:
                n = float(val.replace(",", ".")) / 100
                return f"{n:.8f}".rstrip("0").rstrip(".")
            except (ValueError, TypeError):
                return val
        if key in DOLAR_PARSE_KEYS:
            return _parse_dolar(val)
        if key in USD_KEYS:
            return _parse_usd(val)
        return val

    def _field_validate(key: str, display_val: str) -> tuple:
        val = str(display_val or "").strip()
        if not val:
            return True, ""
        if key in PCT_KEYS:
            try:
                n = float(val.replace(",", "."))
                if n < 0 or n > 100:
                    return False, "debe estar entre 0 y 100"
            except (ValueError, TypeError):
                return False, "valor inválido"
        else:
            stripped = val.replace("u$", "").replace("$", "").strip()
            if key in DOLAR_PARSE_KEYS or key in USD_KEYS:
                stripped = stripped.replace(".", "").replace(",", ".")
            else:
                stripped = stripped.replace(",", ".")
            try:
                n = float(stripped)
                if n < 0:
                    return False, "no puede ser negativo"
            except (ValueError, TypeError):
                pass
        return True, ""

    def _wire_blur(inp: Any, key: str, label_text: str) -> None:
        def _on_blur(_e: Any = None) -> None:
            val = str(inp.value or "").strip()
            ok, msg = _field_validate(key, val)
            if not ok:
                ui.notify(f"{label_text}: {msg}", type="negative")
                return
            set_cotizador_param(key, _field_parse(key, val), uid)
            ui.notify("Guardado", type="positive", position="bottom-right", timeout=1500)
        inp.on("blur", _on_blur)

    # --- UI helpers ---

    def _card_header(icon: str, title: str) -> None:
        with ui.row().classes("items-center gap-1.5 w-full").style(
            "border-bottom:1px solid #e5e7eb; padding-bottom:6px; margin-bottom:6px"
        ):
            ui.html(f'<i class="ti {icon}" style="font-size:14px;color:var(--q-primary)"></i>')
            ui.label(title).classes("leading-none").style(
                "font-size:11px; font-weight:600; color:#374151; text-transform:uppercase; letter-spacing:0.06em"
            )

    def _add_field(container: Dict[str, Any], key: str, label_text: str, unit: str = "") -> None:
        raw = _get(key)
        display_val = _field_display(key, raw)
        texto = f"{label_text} {unit}".strip() if unit else label_text
        is_peso = key in DOLAR_PARSE_KEYS
        value_color = "#185FA5" if is_peso else "#1f2937"
        with ui.row().classes("w-full items-center justify-between").style(
            "padding:3px 0; border-bottom:0.5px solid #f0f0f0; gap:6px; flex-wrap:nowrap"
        ):
            lbl = ui.label(texto).style("font-size:12px; color:#6b7280; flex:1; min-width:0; white-space:nowrap")
            if key in TOOLTIPS:
                lbl.tooltip(TOOLTIPS[key])
            inp = (
                ui.input(value=display_val)
                .props(
                    f'dense outlined hide-bottom-space bg-color=grey-2 '
                    f'input-style="text-align:right; font-size:11px; padding:0 6px; color:{value_color};"'
                )
                .style("width:76px; flex-shrink:0")
            )
            container[key] = inp
        _wire_blur(inp, key, label_text)

    def _divider(subtitle: str = "") -> None:
        if subtitle:
            with ui.row().classes("items-center gap-2 w-full").style("margin:6px 0 2px"):
                ui.element("div").style("flex:1; border-top:1px solid #e5e7eb")
                ui.label(subtitle).style("font-size:10px; color:#9ca3af; white-space:nowrap; padding:0 4px")
                ui.element("div").style("flex:1; border-top:1px solid #e5e7eb")
        else:
            ui.element("div").classes("w-full").style("border-top:1px solid #f3f4f6; margin:4px 0")

    # --- Input dicts por card ---
    inp_dolar:     Dict[str, Any] = {}
    inp_cuotas:    Dict[str, Any] = {}
    inp_ml:        Dict[str, Any] = {}
    inp_miami:     Dict[str, Any] = {}
    inp_china:     Dict[str, Any] = {}
    inp_impuestos: Dict[str, Any] = {}

    def guardar_params() -> None:
        all_inputs = {
            **inp_dolar, **inp_cuotas, **inp_ml,
            **inp_miami, **inp_china, **inp_impuestos,
        }
        for key, inp in all_inputs.items():
            val = str(inp.value or "").strip()
            ok, msg = _field_validate(key, val)
            if not ok:
                ui.notify(f"'{key}': {msg}", type="negative")
                continue
            set_cotizador_param(key, _field_parse(key, val), uid)
        ui.notify("Parámetros guardados", color="positive")

    with ui.column().classes("w-full").style("gap:10px; padding:12px"):

        # ── Barra superior ─────────────────────────────────────────────
        with ui.row().classes("w-full items-center justify-between").style(
            "background:#f8fafc; border:1px solid #e5e7eb; border-radius:8px; padding:10px 16px"
        ):
            with ui.row().classes("items-center gap-2"):
                ui.html('<i class="ti ti-adjustments-horizontal" style="font-size:16px;color:#6b7280"></i>')
                ui.label("Parámetros del sistema").style("font-size:14px; font-weight:600; color:#374151")
            with ui.button(on_click=guardar_params).props("dense color=primary no-caps").style("padding:4px 14px"):
                ui.html('<i class="ti ti-device-floppy" style="font-size:14px; margin-right:6px; vertical-align:middle"></i>')
                ui.label("Guardar parámetros").style("font-size:12px; font-weight:500; color:white; vertical-align:middle")

        # ── Masonry CSS columns: 4 columnas ──────────────────────────────────
        with ui.element("div").style(
            "display:block; column-count:4; column-gap:10px; width:100%"
        ):
            _CS = "break-inside:avoid; -webkit-column-break-inside:avoid; margin-bottom:10px; overflow:hidden"

            # 1. DÓLAR
            with ui.card().classes("p-3 datos-card").style(_CS):
                _card_header("ti-currency-dollar", "Dólar")
                for lbl, key in [
                    ("Oficial",  "dolar_oficial"),
                    ("Blue",     "dolar_blue"),
                    ("Sistema",  "dolar_sistema"),
                    ("Despacho", "dolar_despacho"),
                ]:
                    _add_field(inp_dolar, key, lbl)

            # 2. CUOTAS
            with ui.card().classes("p-3 datos-card").style(_CS):
                _card_header("ti-credit-card", "Cuotas")
                for lbl, key, unit in [
                    ("3x",  "cuotas_3x",  "%"),
                    ("6x",  "cuotas_6x",  "%"),
                    ("9x",  "cuotas_9x",  "%"),
                    ("12x", "cuotas_12x", "%"),
                ]:
                    _add_field(inp_cuotas, key, lbl, unit)
                _divider()
                for lbl, key in [
                    ("ML ×3", "ml_3cuotas"),
                    ("ML ×6", "ml_6cuotas"),
                ]:
                    _add_field(inp_cuotas, key, lbl)

            # 3. MIAMI + Traída × kilo
            with ui.card().classes("p-3 datos-card").style(_CS):
                _card_header("ti-plane", "Miami")
                for lbl, key, unit in [
                    ("KG",            "valor_kg_miami",           "u$"),
                    ("Almac. día/kg", "almacenaje_dias_kg_miami",  "u$"),
                    ("Seguro",        "seguro_miami",              ""),
                    ("Días almac.",   "dias_almacenaje_miami",     ""),
                    ("Almac. ×2",     "almacenaje_miami_x2",       "u$"),
                ]:
                    _add_field(inp_miami, key, lbl, unit)
                _divider("Traída × kilo")
                _add_field(inp_miami, "kilo", "Precio u$/kg")

            # 4. IMPUESTOS
            with ui.card().classes("p-3 datos-card").style(_CS):
                _card_header("ti-receipt-tax", "Impuestos")
                for lbl, key in [
                    ("IVA 10,5%", "iva_105"),
                    ("IVA 21%",   "iva_21"),
                    ("IIBB LHS",  "iibb_lhs"),
                ]:
                    _add_field(inp_impuestos, key, lbl, "%")

            # 5. CHINA
            with ui.card().classes("p-3 datos-card").style(_CS):
                _card_header("ti-world", "China")
                for lbl, key, unit in [
                    ("KG",              "valor_kg_china",           "u$"),
                    ("Almac. día/kg",   "almacenaje_dias_kg_china",  "u$"),
                    ("Seguro",          "seguro_china",              ""),
                    ("Días almac.",     "dias_almacenaje_china",     ""),
                    ("Almac. ×3",       "almacenaje_china_x3",       "u$"),
                    ("Res 3244",        "res_3244",                  ""),
                    ("Gas. operativos", "gastos_operativos",         ""),
                    ("Gas. origen",     "gastos_origen",             ""),
                    ("Envío domicilio", "envio_domicilio",           ""),
                    ("Ajuste ANA",      "ajuste_valor_ana",          ""),
                ]:
                    _add_field(inp_china, key, lbl, unit)

            # 6. MERCADOLIBRE
            with ui.card().classes("p-3 datos-card").style(_CS):
                _card_header("ti-building-store", "MercadoLibre")
                for lbl, key, unit in [
                    ("Comisión", "ml_comision",  "%"),
                    ("Deb/Cre",  "ml_debcre",    "%"),
                    ("SIRTAC",   "ml_sirtac",    "%"),
                    ("IIBB+PER", "ml_iibb_per",  "%"),
                ]:
                    _add_field(inp_ml, key, lbl, unit)
                _divider()
                for lbl, key, unit in [
                    ("Envíos",        "ml_envios",              "$"),
                    ("Gan. neta",     "ml_ganancia_neta_venta", "%"),
                    ("Cobrado",       "ml_cobrado",             ""),
                    ("Com. fija",     "ml_comision_fija_menor", "$"),
                    ("Envíos gratis", "ml_envios_gratuitos",    "$"),
                ]:
                    _add_field(inp_ml, key, lbl, unit)

        # Eliminar tablas obsoletas de la BD si existían
        for k in ["tabla_origen", "tabla_cambio_pa", "tabla_derechos", "tabla_estadisticas"]:
            delete_cotizador_param(k, uid)

        # ══════════════════════════════════════════════════════════════════
        # Tablas editables (sin cambios)
        # ══════════════════════════════════════════════════════════════════
        tabla_trafo_gramos_data = list(_get_tabla("trafo_gramos", TABLA_TRAFO_GRAMOS_DEFAULT))
        tabla_posicion_data     = list(_get_tabla("posicion",     TABLA_POSICION_DEFAULT))
        tabla_envios_data       = list(_get_tabla("envios_ml",    TABLA_ENVIOS_ML_DEFAULT))
        tabla_courier_data      = list(_get_tabla("courier",      TABLA_COURIER_DEFAULT))

        def _parse_num(s: Any) -> float:
            if s is None or s == "": return 0.0
            try:
                return float(str(s).replace(",", "."))
            except (TypeError, ValueError):
                return 0.0

        def _fmt_pesos_display(val: Any) -> str:
            if val is None or str(val).strip() == "":
                return ""
            try:
                n = float(str(val).replace(".", "").replace(",", "."))
                return f"$ {int(n):,}".replace(",", ".") if n == int(n) else f"$ {n:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            except (ValueError, TypeError):
                return str(val)

        def _parse_pesos_fmt(s: Any) -> str:
            if s is None or s == "":
                return ""
            raw = str(s).replace("$", "").replace(".", "").replace(",", ".").strip()
            try:
                n = float(raw)
                return str(int(n)) if n == int(n) else f"{n:.2f}"
            except (ValueError, TypeError):
                return str(s).strip()

        def _tabla_editable(nombre: str, cols: List[str], headers: List[str], data: List[Dict[str, Any]], titulo: str, icon: str = "ti-table", compact: bool = False, col_widths: Optional[List[str]] = None, card_ancho: Optional[str] = None, computed: Optional[Dict[str, Any]] = None, computed_deps: Optional[Dict[str, List[str]]] = None, ordenable: bool = True, col_formato: Optional[Dict[str, str]] = None, collapsed: bool = False) -> None:
            is_collapsed = [collapsed]
            with ui.element("div").style(
                "background:white; border:0.5px solid #e5e7eb; border-radius:12px; overflow:hidden; width:fit-content"
            ):
                with ui.element("div").style(
                    "background:#f3f4f6; padding:10px 14px; cursor:pointer; "
                    "display:flex; align-items:center; justify-content:space-between; gap:12px; "
                    "border-bottom:0.5px solid #e5e7eb"
                ) as header_el:
                    with ui.row().classes("items-center gap-2"):
                        ui.html(f'<i class="ti {icon}" style="font-size:15px;color:#6b7280"></i>')
                        ui.label(titulo).style("font-size:15px; font-weight:500; color:#374151")
                    chev = ui.html(
                        '<i class="ti ' + ('ti-chevron-right' if collapsed else 'ti-chevron-down') +
                        '" style="font-size:16px;color:#9ca3af"></i>'
                    )
                body = ui.element("div")
                if collapsed:
                    body.set_visibility(False)
                def _toggle():
                    is_collapsed[0] = not is_collapsed[0]
                    body.set_visibility(not is_collapsed[0])
                    chev.set_content(
                        '<i class="ti ' + ('ti-chevron-right' if is_collapsed[0] else 'ti-chevron-down') +
                        '" style="font-size:16px;color:#9ca3af"></i>'
                    )
                header_el.on("click", lambda: _toggle())
                with body:
                    cont = ui.element("div")
                    edit_rows: List[Dict[str, Any]] = []

                    def repintar() -> None:
                        cont.clear()
                        edit_rows.clear()
                        with cont:
                            with ui.element("table").classes("datos-tabla").style("border-collapse:collapse; table-layout:auto"):
                                with ui.element("thead"):
                                    with ui.element("tr").style("background:#f3f4f6"):
                                        for j, h in enumerate(headers):
                                            th = ui.element("th").style(
                                                "font-size:11px; font-weight:600; text-transform:uppercase; "
                                                "color:#6b7280; padding:10px 14px; text-align:left; white-space:nowrap"
                                            )
                                            if col_widths and j < len(col_widths):
                                                th.style(col_widths[j])
                                            with th:
                                                ui.label(h)
                                        if ordenable:
                                            with ui.element("th").style(
                                                "font-size:11px; font-weight:600; text-transform:uppercase; color:#6b7280; "
                                                "padding:10px 8px; text-align:center; width:48px; min-width:48px"
                                            ):
                                                ui.label("Ord.")
                                        with ui.element("th").style(
                                            "padding:10px 8px; width:44px; min-width:44px"
                                        ):
                                            ui.label("")
                                with ui.element("tbody"):
                                    for idx, row in enumerate(data):
                                        rinputs: Dict[str, Any] = {}
                                        with ui.element("tr").classes("datos-tabla-fila"):
                                            for col in cols:
                                                val = str(row.get(col, ""))
                                                if col_formato and col in col_formato:
                                                    val = _fmt_pesos_display(val) if val else ""
                                                with ui.element("td").style("padding:8px 14px; border-bottom:0.5px solid #e5e7eb"):
                                                    if computed and col in computed:
                                                        disp = computed[col](row) if callable(computed[col]) else str(row.get(col, ""))
                                                        if col_formato and col in col_formato:
                                                            disp = _fmt_pesos_display(disp) if disp else ""
                                                        lbl = ui.label(disp).style("font-size:12px; color:#374151")
                                                        rinputs[col] = lbl
                                                    else:
                                                        inp = ui.input(value=val).props("dense hide-bottom-space").style("width:100%")
                                                        rinputs[col] = inp
                                            if computed and computed_deps:
                                                def make_updater(comp_col: str, lbl_ref: Any) -> None:
                                                    def upd() -> None:
                                                        row = {}
                                                        for c in cols:
                                                            if c in (computed or {}):
                                                                continue
                                                            raw = str(rinputs[c].value or "")
                                                            if col_formato and c in col_formato:
                                                                raw = _parse_pesos_fmt(raw)
                                                            row[c] = raw
                                                        disp = computed[comp_col](row)
                                                        if col_formato and comp_col in col_formato:
                                                            disp = _fmt_pesos_display(disp) if disp else ""
                                                        lbl_ref.text = disp
                                                    return upd
                                                for comp_col, deps in computed_deps.items():
                                                    if comp_col in rinputs:
                                                        upd = make_updater(comp_col, rinputs[comp_col])
                                                        for d in deps:
                                                            if d in rinputs and hasattr(rinputs[d], "on_value_change"):
                                                                rinputs[d].on_value_change(upd)
                                            if ordenable:
                                                with ui.element("td").style(
                                                    "padding:4px 8px; border-bottom:0.5px solid #e5e7eb; "
                                                    "text-align:center; width:48px; min-width:48px"
                                                ):
                                                    def subir(i: int) -> None:
                                                        if i > 0:
                                                            data[i], data[i - 1] = data[i - 1], data[i]
                                                            repintar()
                                                    def bajar(i: int) -> None:
                                                        if i < len(data) - 1:
                                                            data[i], data[i + 1] = data[i + 1], data[i]
                                                            repintar()
                                                    with ui.row().classes("gap-0 justify-center"):
                                                        ui.button("▲", on_click=lambda i=idx: subir(i)).classes("min-w-0 px-0.5 text-xs").props("flat dense no-caps")
                                                        ui.button("▼", on_click=lambda i=idx: bajar(i)).classes("min-w-0 px-0.5 text-xs").props("flat dense no-caps")
                                            with ui.element("td").style(
                                                "padding:4px 8px; border-bottom:0.5px solid #e5e7eb; "
                                                "text-align:center; width:44px; min-width:44px"
                                            ):
                                                def borrar_fila(i: int) -> None:
                                                    if 0 <= i < len(data):
                                                        data.pop(i)
                                                        repintar()
                                                ui.html(
                                                    '<i class="ti ti-trash datos-trash" '
                                                    'style="font-size:15px;color:#9ca3af;cursor:pointer"></i>'
                                                ).on("click", lambda i=idx: borrar_fila(i))
                                        edit_rows.append(rinputs)

                    repintar()

                    def agregar_fila() -> None:
                        data.append({c: "" for c in cols})
                        repintar()

                    def guardar_tabla() -> None:
                        new_data = []
                        for rinputs in edit_rows:
                            row: Dict[str, Any] = {}
                            for c in cols:
                                if computed and c in computed:
                                    continue
                                raw = str(rinputs[c].value or "")
                                if col_formato and c in col_formato:
                                    raw = _parse_pesos_fmt(raw)
                                row[c] = raw
                            if computed:
                                for c in computed:
                                    row[c] = computed[c](row)
                            new_data.append(row)
                        set_cotizador_tabla(nombre, new_data, uid)
                        data.clear()
                        data.extend(new_data)
                        repintar()
                        ui.notify(f"Tabla {titulo} guardada", color="positive")

                    with ui.row().style(
                        "background:#f9fafb; border-top:1px solid #e5e7eb; padding:10px 14px; gap:8px"
                    ):
                        with ui.button(on_click=agregar_fila).props("flat dense no-caps").style(
                            "color:#185FA5; border:1px solid #378ADD; border-radius:4px; font-size:12px; padding:4px 12px"
                        ):
                            ui.html('<i class="ti ti-plus" style="font-size:13px;margin-right:5px;vertical-align:middle"></i>')
                            ui.label("Agregar fila").style("font-size:12px; vertical-align:middle")
                        with ui.button(on_click=guardar_tabla).props("flat dense no-caps").style(
                            "color:#3B6D11; border:1px solid #639922; border-radius:4px; font-size:12px; padding:4px 12px"
                        ):
                            ui.html('<i class="ti ti-device-floppy" style="font-size:13px;margin-right:5px;vertical-align:middle"></i>')
                            ui.label("Guardar tabla").style("font-size:12px; vertical-align:middle")

        with ui.row().classes("w-full gap-4 flex-wrap").style("align-items:flex-start"):
            _tabla_editable("trafo_gramos", ["trafo", "gramos"], ["Trafo", "Gramos"], tabla_trafo_gramos_data, "Trafo y Gramos", icon="ti-ruler-2", card_ancho="w-fit")
            _tabla_editable("posicion", ["posicion", "seguro", "flete", "derechos", "estadisticas", "iva", "despachante", "cambio_pa"],
                ["Posicion", "Seguro", "Flete", "Derechos", "Estadisticas", "IVA", "Despachante", "Cambio PA"],
                tabla_posicion_data, "Tasas por Posición", icon="ti-list-numbers", card_ancho="w-fit")
            _tabla_editable("envios_ml", ["envio", "importe", "porc_10", "costo"],
                ["Envios ML", "Importe", "0,10", "Costo"], tabla_envios_data, "Costos envío MercadoLibre",
                computed={"costo": lambda r: str(int(_parse_num(r.get("importe")) + _parse_num(r.get("porc_10"))))},
                computed_deps={"costo": ["importe", "porc_10"]}, card_ancho="w-fit",
                col_formato={"importe": "$", "porc_10": "$", "costo": "$"}, icon="ti-building-store")
            _tabla_editable("courier", ["courier", "valor_kg", "descuento", "kg_real", "almacenaje", "seguro", "res_3244", "gas_ope", "env_dom", "iibb", "cif"],
                ["Courier", "Valor KG", "Descuento", "KG Real", "Almacenaje", "Seguro", "Res 3244", "Gas Ope", "Env Dom", "IIBB", "CIF"],
                tabla_courier_data, "Costos por Courier",
                computed={"kg_real": lambda r: f"{_parse_num(r.get('valor_kg')) / max(0.001, _parse_num(r.get('descuento'))):.2f}"},
                computed_deps={"kg_real": ["valor_kg", "descuento"]}, card_ancho="w-fit", icon="ti-truck-delivery")

        # Tabla IVA vs Exento
        tabla_iva_vs_exento_data = list(_get_tabla("iva_vs_exento", TABLA_IVA_VS_EXENTO_DEFAULT))
        iva_vs_exento_headers = ["Courier", "Almacenaje", "Res 3244", "Seguro", "Gastos Operativos", "Envio a Domicilio", "Precio con IVA"]

        def _parse_bool(v: Any) -> bool:
            if v is True or v == "true" or str(v).lower() == "true" or v == 1:
                return True
            return False

        iva_is_collapsed = [False]
        with ui.element("div").style(
            "background:white; border:0.5px solid #e5e7eb; border-radius:12px; overflow:hidden; width:fit-content"
        ):
            with ui.element("div").style(
                "background:#f3f4f6; padding:10px 14px; cursor:pointer; "
                "display:flex; align-items:center; justify-content:space-between; gap:12px; "
                "border-bottom:0.5px solid #e5e7eb"
            ) as iva_header_el:
                with ui.row().classes("items-center gap-2"):
                    ui.html('<i class="ti ti-checklist" style="font-size:15px;color:#6b7280"></i>')
                    ui.label("IVA vs Exento").style("font-size:15px; font-weight:500; color:#374151")
                iva_chev = ui.html('<i class="ti ti-chevron-down" style="font-size:16px;color:#9ca3af"></i>')
            iva_body = ui.element("div")
            def _toggle_iva():
                iva_is_collapsed[0] = not iva_is_collapsed[0]
                iva_body.set_visibility(not iva_is_collapsed[0])
                iva_chev.set_content(
                    '<i class="ti ' + ('ti-chevron-right' if iva_is_collapsed[0] else 'ti-chevron-down') +
                    '" style="font-size:16px;color:#9ca3af"></i>'
                )
            iva_header_el.on("click", lambda: _toggle_iva())
            with iva_body:
                iva_vs_exento_cont = ui.element("div")
                iva_vs_exento_edit_rows: List[Dict[str, Any]] = []

            def repintar_iva() -> None:
                iva_vs_exento_cont.clear()
                iva_vs_exento_edit_rows.clear()
                with iva_vs_exento_cont:
                    with ui.element("table").classes("datos-tabla").style("border-collapse:collapse"):
                        with ui.element("thead"):
                            with ui.element("tr").style("background:#f3f4f6"):
                                for h in iva_vs_exento_headers:
                                    with ui.element("th").style(
                                        "font-size:11px; font-weight:600; text-transform:uppercase; "
                                        "color:#6b7280; padding:10px 14px; text-align:center; white-space:nowrap"
                                    ):
                                        ui.label(h)
                                with ui.element("th").style(
                                    "padding:10px 8px; width:44px; min-width:44px"
                                ):
                                    ui.label("")
                        with ui.element("tbody"):
                            for idx, row in enumerate(tabla_iva_vs_exento_data):
                                rinputs: Dict[str, Any] = {}
                                with ui.element("tr").classes("datos-tabla-fila"):
                                    with ui.element("td").style("padding:8px 14px; border-bottom:0.5px solid #e5e7eb"):
                                        inp_courier = ui.input(value=str(row.get("courier", ""))).props("dense hide-bottom-space").style("width:100%; min-width:100px")
                                        rinputs["courier"] = inp_courier
                                    for col in ["almacenaje", "res_3244", "seguro", "gas_ope", "env_dom", "precio_con_iva"]:
                                        with ui.element("td").style(
                                            "padding:8px 14px; border-bottom:0.5px solid #e5e7eb; text-align:center"
                                        ):
                                            default_val = True if col == "precio_con_iva" else False
                                            chk = ui.checkbox(value=_parse_bool(row.get(col, default_val)))
                                            rinputs[col] = chk
                                    with ui.element("td").style(
                                        "padding:4px 8px; border-bottom:0.5px solid #e5e7eb; text-align:center; width:44px; min-width:44px"
                                    ):
                                        def borrar_iva(i: int) -> None:
                                            if 0 <= i < len(tabla_iva_vs_exento_data):
                                                for j, rinputs in enumerate(iva_vs_exento_edit_rows):
                                                    if j < len(tabla_iva_vs_exento_data):
                                                        tabla_iva_vs_exento_data[j] = {
                                                            "courier": str(rinputs["courier"].value or "").strip(),
                                                            "almacenaje": bool(rinputs["almacenaje"].value),
                                                            "res_3244": bool(rinputs["res_3244"].value),
                                                            "seguro": bool(rinputs["seguro"].value),
                                                            "gas_ope": bool(rinputs["gas_ope"].value),
                                                            "env_dom": bool(rinputs["env_dom"].value),
                                                            "precio_con_iva": bool(rinputs["precio_con_iva"].value),
                                                        }
                                                tabla_iva_vs_exento_data.pop(i)
                                                repintar_iva()
                                        ui.html(
                                            '<i class="ti ti-trash datos-trash" '
                                            'style="font-size:15px;color:#9ca3af;cursor:pointer"></i>'
                                        ).on("click", lambda i=idx: borrar_iva(i))
                                iva_vs_exento_edit_rows.append(rinputs)

            repintar_iva()

            def agregar_fila_iva() -> None:
                for i, rinputs in enumerate(iva_vs_exento_edit_rows):
                    if i < len(tabla_iva_vs_exento_data):
                        tabla_iva_vs_exento_data[i] = {
                            "courier": str(rinputs["courier"].value or "").strip(),
                            "almacenaje": bool(rinputs["almacenaje"].value),
                            "res_3244": bool(rinputs["res_3244"].value),
                            "seguro": bool(rinputs["seguro"].value),
                            "gas_ope": bool(rinputs["gas_ope"].value),
                            "env_dom": bool(rinputs["env_dom"].value),
                            "precio_con_iva": bool(rinputs["precio_con_iva"].value),
                        }
                tabla_iva_vs_exento_data.append({"courier": "", "almacenaje": False, "res_3244": False, "seguro": False, "gas_ope": False, "env_dom": False, "precio_con_iva": True})
                repintar_iva()

            def guardar_tabla_iva() -> None:
                new_data = []
                for rinputs in iva_vs_exento_edit_rows:
                    row: Dict[str, Any] = {
                        "courier": str(rinputs["courier"].value or "").strip(),
                        "almacenaje": bool(rinputs["almacenaje"].value),
                        "res_3244": bool(rinputs["res_3244"].value),
                        "seguro": bool(rinputs["seguro"].value),
                        "gas_ope": bool(rinputs["gas_ope"].value),
                        "env_dom": bool(rinputs["env_dom"].value),
                        "precio_con_iva": bool(rinputs["precio_con_iva"].value),
                    }
                    new_data.append(row)
                set_cotizador_tabla("iva_vs_exento", new_data, uid)
                tabla_iva_vs_exento_data.clear()
                tabla_iva_vs_exento_data.extend(new_data)
                repintar_iva()
                ui.notify("Tabla IVA vs Exento guardada", color="positive")

            with iva_body:
                with ui.row().style(
                    "background:#f9fafb; border-top:1px solid #e5e7eb; padding:10px 14px; gap:8px"
                ):
                    with ui.button(on_click=agregar_fila_iva).props("flat dense no-caps").style(
                        "color:#185FA5; border:1px solid #378ADD; border-radius:4px; font-size:12px; padding:4px 12px"
                    ):
                        ui.html('<i class="ti ti-plus" style="font-size:13px;margin-right:5px;vertical-align:middle"></i>')
                        ui.label("Agregar fila").style("font-size:12px; vertical-align:middle")
                    with ui.button(on_click=guardar_tabla_iva).props("flat dense no-caps").style(
                        "color:#3B6D11; border:1px solid #639922; border-radius:4px; font-size:12px; padding:4px 12px"
                    ):
                        ui.html('<i class="ti ti-device-floppy" style="font-size:13px;margin-right:5px;vertical-align:middle"></i>')
                        ui.label("Guardar tabla").style("font-size:12px; vertical-align:middle")


# ==========================
# CALLBACK OAUTH (ruta HTTP directa para evitar 404 con NiceGUI)
# ==========================
