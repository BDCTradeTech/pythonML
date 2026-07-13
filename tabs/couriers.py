"""
tabs/couriers.py
Comparador de couriers de importacion — 100% efimero, sin persistencia en DB.
Los inputs viven solo en la sesion del navegador (estado reactivo NiceGUI).
"""
from __future__ import annotations

from nicegui import app, ui

TC = 1460  # dolar oficial, hardcodeado por ahora


def _sixstar_usd(kg: float, fob: float, flete_kg: float, pa: float) -> float:
    return 63 + fob * 0.0242 + kg * 3.63 + kg * flete_kg + pa


def _lhs_usd(kg: float, fob: float, flete_kg: float, pa: float) -> float:
    return 27 + kg * flete_kg + pa


def _nc_usd(kg: float, fob: float, flete_kg: float, pa: float) -> float:
    return 31 + kg * flete_kg + pa


def _pct_traida(total_usd: float, fob: float) -> float:
    return (total_usd / fob) * 100 if fob else 0.0


def _fmt_ars(v: float) -> str:
    return f"$ {v:,.0f}".replace(",", ".")


_COURIERS = [
    {
        "key": "sixstar",
        "nombre": "SIXSTAR",
        "color": "#2a78d6",
        "flete_default": 7.50,
        "pa_default": 150.0,
        "usd_fn": _sixstar_usd,
        "fixed_rows_fn": lambda kg, fob, tc: [
            ("Handling (fijo)", "USD 63 × TC", 63 * tc),
            ("G. Administrativos", "FOB × 2.42% × TC", fob * 0.0242 * tc),
            ("Honorarios", "KGS × USD 3.63 × TC", kg * 3.63 * tc),
        ],
    },
    {
        "key": "lhs",
        "nombre": "LHS",
        "color": "#1baf7a",
        "flete_default": 8.50,
        "pa_default": 200.0,
        "usd_fn": _lhs_usd,
        "fixed_rows_fn": lambda kg, fob, tc: [
            ("Gastos Operativos (fijo)", "USD 27 × TC", 27 * tc),
        ],
    },
    {
        "key": "nc",
        "nombre": "NC Supplies",
        "color": "#eda100",
        "flete_default": 9.50,
        "pa_default": 250.0,
        "usd_fn": _nc_usd,
        "fixed_rows_fn": lambda kg, fob, tc: [
            ("Servicios y Honorarios (fijo)", "USD 31 × TC", 31 * tc),
        ],
    },
]


def build_tab_couriers() -> None:
    ui.add_css("""
        @media (max-width: 900px) {
            .couriers-grid { grid-template-columns: 1fr !important; }
        }
    """)
    user = app.storage.user.get("user")
    if not user:
        ui.label("Debes iniciar sesion").classes("text-red-500 p-4")
        return

    state = {"fob": 2000.0, "peso": 20.0}
    for cfg in _COURIERS:
        state[f"flete_{cfg['key']}"] = cfg["flete_default"]
        state[f"pa_{cfg['key']}"] = cfg["pa_default"]

    content_refs: dict = {}
    badge_refs: dict = {}
    derecha_ref: list = [None]

    def _render_card_content(cfg: dict):
        kg, fob = state["peso"], state["fob"]
        flete_kg = state[f"flete_{cfg['key']}"]
        pa = state[f"pa_{cfg['key']}"]
        total_usd = cfg["usd_fn"](kg, fob, flete_kg, pa)
        total_ars = total_usd * TC
        pct = _pct_traida(total_usd, fob)

        badge_refs[cfg["key"]].set_text(f"{pct:.1f}%")

        cont = content_refs[cfg["key"]]
        cont.clear()
        filas = cfg["fixed_rows_fn"](kg, fob, TC) + [
            ("Flete internacional", "KGS × Flete/kg × TC", kg * flete_kg * TC),
            ("Cambio PA", "PA (USD) × TC", pa * TC),
        ]
        with cont:
            for nombre, formula, valor in filas:
                with ui.element("div").style("display:flex;justify-content:space-between;align-items:baseline"):
                    with ui.element("div"):
                        ui.label(nombre).style("font-size:11px;color:#374151;display:block")
                        ui.label(formula).style("font-size:9px;color:#9ca3af;display:block")
                    ui.label(_fmt_ars(valor)).style("font-size:11px;font-weight:500;color:#374151")
            ui.element("div").style("border-top:0.5px solid #e2e8f0;margin:2px 0")
            with ui.element("div").style("display:flex;justify-content:space-between;align-items:center"):
                ui.label("Total courier").style("font-size:12px;font-weight:600;color:#185FA5")
                ui.label(_fmt_ars(total_ars)).style("font-size:13px;font-weight:700;color:#185FA5")

    def _build_card_shell(cfg: dict):
        with ui.element("div").style(
            "border:0.5px solid #e2e8f0;border-radius:8px;overflow:hidden"
        ):
            with ui.element("div").style(
                f"background:{cfg['color']};padding:8px 12px;display:flex;"
                "justify-content:space-between;align-items:center;gap:8px"
            ):
                with ui.row().style("gap:8px;align-items:center"):
                    ui.element("div").style(
                        "width:9px;height:9px;border-radius:50%;background:#fff;flex-shrink:0"
                    )
                    ui.label(cfg["nombre"]).style("font-size:13px;font-weight:600;color:#fff;white-space:nowrap")
                badge = ui.label("0.0%").style(
                    "font-size:11px;font-weight:700;color:#fff;background:rgba(255,255,255,.22);"
                    "padding:2px 10px;border-radius:10px;white-space:nowrap"
                )
                badge_refs[cfg["key"]] = badge

            with ui.element("div").style("padding:10px 12px;display:flex;flex-direction:column;gap:8px"):
                with ui.row().style("gap:10px"):
                    with ui.element("div"):
                        ui.label("Flete/kg (USD)").style("font-size:9px;color:#6b7280;display:block")
                        inp_flete = ui.number(value=cfg["flete_default"], min=0, step=0.5).props(
                            "dense outlined"
                        ).style("width:90px;font-size:11px")
                    with ui.element("div"):
                        ui.label("PA (USD)").style("font-size:9px;color:#6b7280;display:block")
                        inp_pa = ui.number(value=cfg["pa_default"], min=0, max=300, step=5).props(
                            "dense outlined"
                        ).style("width:90px;font-size:11px")

                content = ui.element("div").style("display:flex;flex-direction:column;gap:6px")
                content_refs[cfg["key"]] = content

        def _on_flete(e, key=cfg["key"]):
            state[f"flete_{key}"] = float(e.value or 0)
            _recalcular()
        inp_flete.on_value_change(_on_flete)

        def _on_pa(e, key=cfg["key"]):
            state[f"pa_{key}"] = float(e.value or 0)
            _recalcular()
        inp_pa.on_value_change(_on_pa)

    def _render_derecha():
        derecha_ref[0].clear()
        fob = state["fob"]
        kgs = list(range(0, 61))

        series_defs = []
        for cfg in _COURIERS:
            flete_kg = state[f"flete_{cfg['key']}"]
            pa = state[f"pa_{cfg['key']}"]
            pts = [round(_pct_traida(cfg["usd_fn"](k, fob, flete_kg, pa), fob), 2) for k in kgs]
            series_defs.append((cfg, pts))

        series = []
        for i, (cfg, pts) in enumerate(series_defs):
            serie = {
                "name": cfg["nombre"],
                "type": "line",
                "showSymbol": False,
                "lineStyle": {"color": cfg["color"], "width": 2},
                "itemStyle": {"color": cfg["color"]},
                "data": [[k, v] for k, v in zip(kgs, pts)],
            }
            if i == 0:
                serie["markLine"] = {
                    "symbol": "none",
                    "silent": True,
                    "lineStyle": {"color": "#6b7280", "type": "dashed"},
                    "label": {"formatter": "{c} kg", "position": "insideEndTop", "fontSize": 9, "color": "#6b7280"},
                    "data": [{"xAxis": state["peso"]}],
                }
            series.append(serie)

        chart_options = {
            "backgroundColor": "transparent",
            "grid": {"left": 45, "right": 20, "top": 60, "bottom": 30, "containLabel": True},
            "legend": {"top": 0, "textStyle": {"fontSize": 10}},
            "tooltip": {"trigger": "axis"},
            "xAxis": {
                "type": "value",
                "min": 0,
                "max": 60,
                "name": "kg",
                "axisLabel": {"formatter": "{value} kg", "fontSize": 10},
            },
            "yAxis": {
                "type": "value",
                "axisLabel": {"formatter": "{value}%", "fontSize": 10},
            },
            "series": series,
        }
        with derecha_ref[0]:
            with ui.element("div").style("border:0.5px solid #e2e8f0;border-radius:8px;padding:8px 4px 4px"):
                ui.label("% de traida por kg").style("font-size:11px;font-weight:600;color:#185FA5;padding-left:8px")
                ui.echart(chart_options).classes("w-full").style("height:340px")

    def _recalcular():
        for cfg in _COURIERS:
            _render_card_content(cfg)
        _render_derecha()

    with ui.element("div").style("padding:8px 16px;display:flex;flex-direction:column;gap:12px"):
        # Barra superior — inputs globales
        with ui.element("div").style(
            "display:flex;gap:20px;align-items:center;flex-wrap:wrap;"
            "border:0.5px solid #e2e8f0;border-radius:8px;padding:10px 14px"
        ):
            with ui.row().style("gap:8px;align-items:center"):
                ui.label("FOB Total (USD):").style("font-size:12px;color:#374151;font-weight:500")
                inp_fob = ui.number(value=state["fob"], step=100, min=0).props("dense outlined").style("width:120px;font-size:12px")

            with ui.row().style("gap:8px;align-items:center"):
                ui.label("Peso total:").style("font-size:12px;color:#374151;font-weight:500")
                lbl_peso = ui.label(f"{state['peso']:.0f} kg").style("font-size:12px;font-weight:700;color:#185FA5;min-width:44px")
                sld_peso = ui.slider(min=0, max=60, step=1, value=state["peso"]).style("width:220px")

        def _on_fob(e):
            state["fob"] = float(e.value or 0)
            _recalcular()
        inp_fob.on_value_change(_on_fob)

        def _on_peso(e):
            state["peso"] = float(e.value or 0)
            lbl_peso.set_text(f"{state['peso']:.0f} kg")
            _recalcular()
        sld_peso.on_value_change(_on_peso)

        # Grid dos columnas: izquierda cards, derecha grafico
        with ui.element("div").classes("couriers-grid").style(
            "display:grid;grid-template-columns:2fr 3fr;gap:16px;align-items:start"
        ):
            with ui.element("div").style("display:flex;flex-direction:column;gap:10px"):
                for cfg in _COURIERS:
                    _build_card_shell(cfg)
            derecha = ui.element("div")

        derecha_ref[0] = derecha
        _recalcular()
