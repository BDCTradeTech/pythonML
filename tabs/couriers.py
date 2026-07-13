"""
tabs/couriers.py
Comparador de couriers de importacion (SIXSTAR, LHS, NC Supplies). Usa el motor
real de calculo de Importacion (importacion_calc.calc_courier_row) sobre las
mismas tablas por usuario: Costos por Courier y Tasas por Posicion. Solo lectura:
los valores de cada courier se editan desde Datos -> Costos por Courier.
FOB, peso, posicion y Cambio PA son de sesion (no se persisten).
"""
from __future__ import annotations

from nicegui import app, ui

from importacion_calc import calc_courier_row, load_calc_context

_COURIERS = [
    {"key": "sixstar", "nombre": "SIXSTAR", "color": "#2a78d6", "origen": "Mia Sixtar"},
    {"key": "lhs", "nombre": "LHS", "color": "#1baf7a", "origen": "Mia LHS"},
    {"key": "nc", "nombre": "NC Supplies", "color": "#eda100", "origen": "Mia Richard"},
]


def _fmt_usd0(v: float) -> str:
    return f"USD {v:,.0f}"


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
    uid = user["id"]

    ctx = load_calc_context(uid)
    opciones_posicion = {
        row.get("posicion", ""): row.get("posicion", "")
        for row in ctx["posicion_data"] if row.get("posicion")
    }
    default_posicion = next(iter(opciones_posicion), "")

    state = {"fob": 2900.0, "peso": 35.0, "posicion": default_posicion, "cambio_pa": 0.0}

    content_refs: dict = {}
    badge_refs: dict = {}
    chart_container_ref: list = [None]

    def _calc(cfg: dict) -> dict:
        row = {
            "origen": cfg["origen"],
            "posicion": state["posicion"],
            "fob": state["fob"],
            "qty": 1,
            "peso_unitario": state["peso"],
            "extras": 0,
            "cambio_pa": state["cambio_pa"],
            "venta_ml": "",
        }
        return calc_courier_row(row, ctx["params"], ctx["posicion_by_name"], ctx["courier_by_origen"], ctx["origen_posicion"], ctx["iva_vs_exento_by_courier"])

    def _render_card_content(cfg: dict):
        calc = _calc(cfg)
        pct = calc["traida_pct_raw"]
        badge_refs[cfg["key"]].set_text(f"{pct:.1f}%")

        cont = content_refs[cfg["key"]]
        cont.clear()
        filas = [
            ("Derechos", calc["derechos"]),
            ("Tasa Estadística", calc["estadistica"]),
            ("Flete Internacional", calc["flete_int"]),
            ("Almacenaje", calc["almacenaje"]),
            ("Res 3244", calc["res_3244"]),
            ("Seguro", calc["seguro"]),
            ("Gastos Operativos", calc["gas_ope"]),
            ("Envío a Domicilio", calc["env_dom"]),
            ("IVA Total", calc["iva_lhs"]),
            ("IIBB", calc["iibb"]),
        ]
        with cont:
            for nombre, valor in filas:
                with ui.element("div").style(
                    "display:flex;justify-content:space-between;align-items:baseline;gap:6px"
                ):
                    ui.label(nombre).style(
                        "font-size:10px;color:#374151;white-space:nowrap;overflow:hidden;"
                        "text-overflow:ellipsis"
                    )
                    ui.label(valor).style(
                        "font-size:10px;font-weight:500;color:#374151;white-space:nowrap;flex-shrink:0"
                    )
            ui.element("div").style("border-top:0.5px solid #e2e8f0;margin:2px 0")
            with ui.element("div").style("display:flex;justify-content:space-between;align-items:center"):
                ui.label("Total courier").style("font-size:11px;font-weight:600;color:#374151")
                ui.label(calc["total_courier"]).style("font-size:11px;font-weight:600;color:#374151")
            with ui.element("div").style("display:flex;justify-content:space-between;align-items:center"):
                ui.label("Total").style("font-size:12px;font-weight:700;color:#185FA5")
                ui.label(calc["total"]).style("font-size:13px;font-weight:700;color:#185FA5")

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

            with ui.element("div").style("padding:6px 10px;display:flex;flex-direction:column;gap:2px"):
                content = ui.element("div").style("display:flex;flex-direction:column;gap:2px")
                content_refs[cfg["key"]] = content

    def _render_chart():
        chart_container_ref[0].clear()
        fob = state["fob"]
        peso = state["peso"]
        kgs = list(range(0, 61))

        pct_en_peso_by_key = {cfg["key"]: round(_calc(cfg)["traida_pct_raw"], 1) for cfg in _COURIERS}

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
            pts = []
            for k in kgs:
                row = {
                    "origen": cfg["origen"],
                    "posicion": state["posicion"],
                    "fob": fob,
                    "qty": 1,
                    "peso_unitario": k,
                    "extras": 0,
                    "cambio_pa": state["cambio_pa"],
                    "venta_ml": "",
                }
                calc = calc_courier_row(row, ctx["params"], ctx["posicion_by_name"], ctx["courier_by_origen"], ctx["origen_posicion"], ctx["iva_vs_exento_by_courier"])
                pts.append(round(calc["traida_pct_raw"], 2))
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
            ui.echart(chart_options).classes("w-full").style("height:382px")

    def _recalcular():
        for cfg in _COURIERS:
            _render_card_content(cfg)
        _render_chart()

    with ui.element("div").style("padding:8px 16px;display:flex;flex-direction:column;gap:10px;width:100%"):
        # 1) Barra superior — posicion arancelaria + FOB + Peso (sliders) + Cambio PA
        with ui.element("div").classes("couriers-topbar").style(
            "display:flex;gap:24px;align-items:flex-end;width:100%;"
            "border:0.5px solid #e2e8f0;border-radius:8px;padding:8px 16px"
        ):
            with ui.element("div").style("min-width:220px"):
                ui.label("Posición Arancelaria").style("font-size:11px;color:#374151;font-weight:500;display:block")
                sel_posicion = ui.select(options=opciones_posicion, value=state["posicion"]).props(
                    "dense outlined"
                ).style("width:100%;font-size:12px")

            with ui.element("div").style("flex:1;min-width:220px"):
                with ui.element("div").style("display:flex;justify-content:space-between;align-items:baseline"):
                    ui.label("FOB Total").style("font-size:11px;color:#374151;font-weight:500")
                    lbl_fob = ui.label(_fmt_usd0(state["fob"])).style("font-size:12px;font-weight:700;color:#185FA5")
                sld_fob = ui.slider(min=0, max=3000, step=100, value=state["fob"]).style("width:100%")

            with ui.element("div").style("flex:1;min-width:220px"):
                with ui.element("div").style("display:flex;justify-content:space-between;align-items:baseline"):
                    ui.label("Peso total").style("font-size:11px;color:#374151;font-weight:500")
                    lbl_peso = ui.label(f"{state['peso']:.0f} kg").style("font-size:12px;font-weight:700;color:#185FA5")
                sld_peso = ui.slider(min=0, max=60, step=1, value=state["peso"]).style("width:100%")

            with ui.element("div").style("min-width:140px"):
                ui.label("Cambio PA (u$)").style("font-size:11px;color:#374151;font-weight:500;display:block")
                inp_pa = ui.number(value=state["cambio_pa"], min=0, step=10).props(
                    "dense outlined"
                ).style("width:100%;font-size:12px")

        def _on_posicion(e):
            state["posicion"] = e.value
            _recalcular()
        sel_posicion.on_value_change(_on_posicion)

        def _on_fob(e):
            state["fob"] = float(e.value or 0)
            lbl_fob.set_text(_fmt_usd0(state["fob"]))
            _recalcular()
        sld_fob.on_value_change(_on_fob)

        def _on_peso(e):
            state["peso"] = float(e.value or 0)
            lbl_peso.set_text(f"{state['peso']:.0f} kg")
            _recalcular()
        sld_peso.on_value_change(_on_peso)

        def _on_cambio_pa(e):
            state["cambio_pa"] = float(e.value or 0)
            _recalcular()
        inp_pa.on_value_change(_on_cambio_pa)

        # 2) Fila de 3 tarjetas, una sola fila full-width
        with ui.element("div").classes("couriers-row").style("display:flex;gap:10px;width:100%"):
            for cfg in _COURIERS:
                _build_card_shell(cfg)

        # 3) Grafico full-width
        chart_container = ui.element("div").style("width:100%")
        chart_container_ref[0] = chart_container

        _recalcular()
