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


def _fmt_usd(v: float) -> str:
    return f"USD {v:,.0f}".replace(",", ".")


_COURIERS = [
    {
        "key": "sixstar",
        "nombre": "SIXSTAR",
        "color": "#2a78d6",
        "flete_default": 7.50,
        "pa_default": 150.0,
        "usd_fn": _sixstar_usd,
        "fixed_rows_fn": lambda kg, fob, tc: [
            ("Handling", "USD 63 × TC", 63 * tc),
            ("G. Admin.", "FOB × 2.42% × TC", fob * 0.0242 * tc),
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
            ("Gastos Operativos", "USD 27 × TC", 27 * tc),
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
            ("Serv. y Honorarios", "USD 31 × TC", 31 * tc),
        ],
    },
]


def build_tab_couriers() -> None:
    ui.add_css("""
        @media (max-width: 900px) {
            .couriers-row { flex-direction: column !important; }
            .couriers-topbar { flex-direction: column !important; align-items: stretch !important; }
        }
    """)
    user = app.storage.user.get("user")
    if not user:
        ui.label("Debes iniciar sesion").classes("text-red-500 p-4")
        return

    state = {"fob": 2900.0, "peso": 35.0}
    for cfg in _COURIERS:
        state[f"flete_{cfg['key']}"] = cfg["flete_default"]
        state[f"pa_{cfg['key']}"] = cfg["pa_default"]

    content_refs: dict = {}
    badge_refs: dict = {}
    chart_container_ref: list = [None]

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
                with ui.element("div").style(
                    "display:flex;justify-content:space-between;align-items:baseline;gap:6px"
                ):
                    ui.label(f"{nombre}").style(
                        "font-size:10px;color:#374151;white-space:nowrap;overflow:hidden;"
                        "text-overflow:ellipsis"
                    )
                    ui.label(formula).style(
                        "font-size:8px;color:#9ca3af;white-space:nowrap;overflow:hidden;"
                        "text-overflow:ellipsis;flex:1;text-align:right"
                    )
                    ui.label(_fmt_ars(valor)).style(
                        "font-size:10px;font-weight:500;color:#374151;white-space:nowrap;flex-shrink:0"
                    )
            ui.element("div").style("border-top:0.5px solid #e2e8f0;margin:2px 0")
            with ui.element("div").style("display:flex;justify-content:space-between;align-items:center"):
                ui.label("Total courier").style("font-size:11px;font-weight:600;color:#185FA5")
                ui.label(_fmt_ars(total_ars)).style("font-size:12px;font-weight:700;color:#185FA5")

    def _build_card_shell(cfg: dict):
        with ui.element("div").style(
            "border:0.5px solid #e2e8f0;border-radius:8px;overflow:hidden;flex:1;min-width:0"
        ):
            with ui.element("div").style(
                f"background:{cfg['color']};padding:6px 10px;display:flex;"
                "justify-content:space-between;align-items:center;gap:8px"
            ):
                with ui.row().style("gap:6px;align-items:center"):
                    ui.element("div").style(
                        "width:8px;height:8px;border-radius:50%;background:#fff;flex-shrink:0"
                    )
                    ui.label(cfg["nombre"]).style("font-size:12px;font-weight:600;color:#fff;white-space:nowrap")
                badge = ui.label("0.0%").style(
                    "font-size:10px;font-weight:700;color:#fff;background:rgba(255,255,255,.22);"
                    "padding:1px 8px;border-radius:9px;white-space:nowrap"
                )
                badge_refs[cfg["key"]] = badge

            with ui.element("div").style("padding:6px 10px;display:flex;flex-direction:column;gap:4px"):
                with ui.row().style("gap:8px"):
                    with ui.element("div").style("flex:1;min-width:0"):
                        ui.label("Flete/kg (USD)").style("font-size:8px;color:#6b7280;display:block")
                        inp_flete = ui.number(value=cfg["flete_default"], min=0, step=0.5).props(
                            "dense outlined"
                        ).style("width:100%;font-size:11px")
                    with ui.element("div").style("flex:1;min-width:0"):
                        ui.label("PA (USD)").style("font-size:8px;color:#6b7280;display:block")
                        inp_pa = ui.number(value=cfg["pa_default"], min=0, max=300, step=5).props(
                            "dense outlined"
                        ).style("width:100%;font-size:11px")

                content = ui.element("div").style("display:flex;flex-direction:column;gap:2px;margin-top:2px")
                content_refs[cfg["key"]] = content

        def _on_flete(e, key=cfg["key"]):
            state[f"flete_{key}"] = float(e.value or 0)
            _recalcular()
        inp_flete.on_value_change(_on_flete)

        def _on_pa(e, key=cfg["key"]):
            state[f"pa_{key}"] = float(e.value or 0)
            _recalcular()
        inp_pa.on_value_change(_on_pa)

    def _render_chart():
        chart_container_ref[0].clear()
        fob = state["fob"]
        peso = state["peso"]
        kgs = list(range(0, 61))

        pct_en_peso_by_key = {}
        for cfg in _COURIERS:
            flete_kg = state[f"flete_{cfg['key']}"]
            pa = state[f"pa_{cfg['key']}"]
            pct_en_peso_by_key[cfg["key"]] = round(_pct_traida(cfg["usd_fn"](peso, fob, flete_kg, pa), fob), 1)

        # Las 3 etiquetas del markPoint se separan en direcciones distintas (arriba/
        # derecha/abajo) segun el ranking de valor en el peso actual, para que no se
        # superpongan cuando las curvas estan muy cerca entre si en ese punto.
        ranking = sorted(_COURIERS, key=lambda c: pct_en_peso_by_key[c["key"]], reverse=True)
        _LABEL_SLOTS = [
            {"position": "top", "distance": 8},
            {"position": "right", "distance": 10},
            {"position": "bottom", "distance": 8},
        ]
        label_slot_by_key = {c["key"]: _LABEL_SLOTS[i] for i, c in enumerate(ranking)}

        series = []
        for cfg in _COURIERS:
            flete_kg = state[f"flete_{cfg['key']}"]
            pa = state[f"pa_{cfg['key']}"]
            pts = [round(_pct_traida(cfg["usd_fn"](k, fob, flete_kg, pa), fob), 2) for k in kgs]
            pct_en_peso = pct_en_peso_by_key[cfg["key"]]

            serie = {
                "name": cfg["nombre"],
                "type": "line",
                "showSymbol": False,
                "lineStyle": {"color": cfg["color"], "width": 2},
                "itemStyle": {"color": cfg["color"]},
                "data": [[k, v] for k, v in zip(kgs, pts)],
                "markPoint": {
                    "symbol": "circle",
                    "symbolSize": 8,
                    "itemStyle": {"color": cfg["color"], "borderColor": "#fff", "borderWidth": 1},
                    "label": {
                        "formatter": "{c}%",
                        "fontSize": 10,
                        "fontWeight": "bold",
                        "color": cfg["color"],
                        **label_slot_by_key[cfg["key"]],
                    },
                    "data": [{"coord": [peso, pct_en_peso], "value": pct_en_peso}],
                },
            }
            series.append(serie)

        series[0]["markLine"] = {
            "symbol": "none",
            "silent": True,
            "lineStyle": {"color": "#6b7280", "type": "dashed"},
            "label": {"formatter": "{c} kg", "position": "start", "fontSize": 9, "color": "#6b7280"},
            "data": [{"xAxis": peso}],
        }

        chart_options = {
            "backgroundColor": "transparent",
            "grid": {"left": 45, "right": 20, "top": 55, "bottom": 30, "containLabel": True},
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
        with chart_container_ref[0]:
            ui.echart(chart_options).classes("w-full").style("height:300px")

    def _recalcular():
        for cfg in _COURIERS:
            _render_card_content(cfg)
        _render_chart()

    with ui.element("div").style("padding:8px 16px;display:flex;flex-direction:column;gap:10px;width:100%"):
        # 1) Barra superior — inputs globales (full width, sliders)
        with ui.element("div").classes("couriers-topbar").style(
            "display:flex;gap:24px;align-items:flex-end;width:100%;"
            "border:0.5px solid #e2e8f0;border-radius:8px;padding:8px 16px"
        ):
            with ui.element("div").style("flex:1;min-width:220px"):
                with ui.element("div").style("display:flex;justify-content:space-between;align-items:baseline"):
                    ui.label("FOB Total").style("font-size:11px;color:#374151;font-weight:500")
                    lbl_fob = ui.label(_fmt_usd(state["fob"])).style("font-size:12px;font-weight:700;color:#185FA5")
                sld_fob = ui.slider(min=0, max=3000, step=100, value=state["fob"]).style("width:100%")

            with ui.element("div").style("flex:1;min-width:220px"):
                with ui.element("div").style("display:flex;justify-content:space-between;align-items:baseline"):
                    ui.label("Peso total").style("font-size:11px;color:#374151;font-weight:500")
                    lbl_peso = ui.label(f"{state['peso']:.0f} kg").style("font-size:12px;font-weight:700;color:#185FA5")
                sld_peso = ui.slider(min=0, max=60, step=1, value=state["peso"]).style("width:100%")

        def _on_fob(e):
            state["fob"] = float(e.value or 0)
            lbl_fob.set_text(_fmt_usd(state["fob"]))
            _recalcular()
        sld_fob.on_value_change(_on_fob)

        def _on_peso(e):
            state["peso"] = float(e.value or 0)
            lbl_peso.set_text(f"{state['peso']:.0f} kg")
            _recalcular()
        sld_peso.on_value_change(_on_peso)

        # 2) Fila de 3 tarjetas, una sola fila full-width
        with ui.element("div").classes("couriers-row").style("display:flex;gap:10px;width:100%"):
            for cfg in _COURIERS:
                _build_card_shell(cfg)

        # 3) Grafico full-width
        chart_container = ui.element("div").style("width:100%")
        chart_container_ref[0] = chart_container

        _recalcular()
