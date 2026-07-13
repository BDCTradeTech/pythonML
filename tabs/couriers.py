"""
tabs/couriers.py
Comparador de couriers de importacion (SIXSTAR, LHS, NC Supplies). Usa el motor
real de calculo de Importacion (importacion_calc.calc_courier_row) sobre las
mismas tablas por usuario: Costos por Courier y Tasas por Posicion. Los valores
de cada courier (excepto Cambio PA) se editan desde Datos -> Costos por Courier.
FOB, peso y posicion son de sesion (no se persisten). Cambio PA es editable por
courier desde esta misma pagina y SI se persiste (columna cambio_pa en la misma
fila de tabla_courier) — no se toca desde Datos.
"""
from __future__ import annotations

from nicegui import app, ui

from db import set_cotizador_tabla
from importacion_calc import calc_courier_row, load_calc_context

_COURIERS = [
    {"key": "sixstar", "nombre": "SIXSTAR", "color": "#2a78d6", "origen": "Mia Sixtar"},
    {"key": "lhs", "nombre": "LHS", "color": "#1baf7a", "origen": "Mia LHS"},
    {"key": "nc", "nombre": "NC Supplies", "color": "#eda100", "origen": "Mia Richard"},
]

_DEFAULT_CAMBIO_PA = {"sixstar": 150.0, "lhs": 200.0, "nc": 250.0}


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

    state = {"fob": 2900.0, "peso": 35.0, "posicion": default_posicion}

    _courier_row_by_origen: dict = {
        str(r.get("courier", "")).strip(): r for r in ctx["courier_data"]
    }

    def _cambio_pa_inicial(cfg: dict) -> float:
        row = _courier_row_by_origen.get(cfg["origen"], {})
        raw = row.get("cambio_pa")
        if raw is None or raw == "":
            return _DEFAULT_CAMBIO_PA[cfg["key"]]
        try:
            return float(str(raw).replace(",", "."))
        except (TypeError, ValueError):
            return _DEFAULT_CAMBIO_PA[cfg["key"]]

    pa_state: dict = {cfg["key"]: _cambio_pa_inicial(cfg) for cfg in _COURIERS}

    def _persist_cambio_pa(cfg: dict, val: float) -> None:
        rows = [dict(r) for r in ctx["courier_data"]]
        for r in rows:
            if str(r.get("courier", "")).strip() == cfg["origen"]:
                r["cambio_pa"] = val
                break
        set_cotizador_tabla("courier", rows, uid)
        ctx["courier_data"] = rows
        _courier_row_by_origen.clear()
        _courier_row_by_origen.update({str(r.get("courier", "")).strip(): r for r in rows})

    content_refs: dict = {}
    badge_refs: dict = {}
    pa_input_refs: dict = {}
    chart_container_ref: list = [None]

    def _cambio_pa_efectivo(cfg: dict) -> float:
        if state["posicion"] != "Cambio PA":
            return 0.0
        return pa_state[cfg["key"]]

    def _calc(cfg: dict) -> dict:
        row = {
            "origen": cfg["origen"],
            "posicion": state["posicion"],
            "fob": state["fob"],
            "qty": 1,
            "peso_unitario": state["peso"],
            "extras": 0,
            "cambio_pa": _cambio_pa_efectivo(cfg),
            "venta_ml": "",
        }
        return calc_courier_row(row, ctx["params"], ctx["posicion_by_name"], ctx["courier_by_origen"], ctx["origen_posicion"], ctx["iva_vs_exento_by_courier"])

    def _render_card_content(cfg: dict):
        calc = _calc(cfg)
        pct = calc["traida_pct_raw"]
        badge_refs[cfg["key"]].set_text(f"{pct:.1f}%")

        kg_real = ctx["courier_by_origen"].get(cfg["origen"], {}).get("kg_real", 0.0)

        cont = content_refs[cfg["key"]]
        cont.clear()
        filas = [
            ("Derechos", calc["derechos"]),
            ("Tasa Estadística", calc["estadistica"]),
            (f"Flete Internacional (u$ {kg_real:.2f} kg)", calc["flete_int"]),
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
                with ui.element("div").style(
                    "display:flex;justify-content:space-between;align-items:center;gap:6px;"
                    "padding-bottom:4px;margin-bottom:2px;border-bottom:0.5px solid #e2e8f0"
                ):
                    ui.label("Cambio PA (u$)").style(
                        "font-size:10px;color:#374151;font-weight:500;white-space:nowrap"
                    )
                    pa_input = ui.number(value=pa_state[cfg["key"]], min=0, step=10).props(
                        "dense outlined"
                    ).style("width:64px;font-size:10px")
                    pa_input_refs[cfg["key"]] = pa_input

                def _on_cambio_pa_card(e, cfg=cfg, pa_input=pa_input):
                    val = max(0.0, float(e.value or 0))
                    pa_state[cfg["key"]] = val
                    pa_input.set_value(val)
                    _persist_cambio_pa(cfg, val)
                    _render_card_content(cfg)
                    _render_chart()
                pa_input.on_value_change(_on_cambio_pa_card)

                content = ui.element("div").style("display:flex;flex-direction:column;gap:2px")
                content_refs[cfg["key"]] = content

    def _render_chart():
        chart_container_ref[0].clear()
        fob = state["fob"]
        peso = state["peso"]
        kgs = list(range(0, 56))

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
                    "cambio_pa": _cambio_pa_efectivo(cfg),
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
            "tooltip": {"trigger": "axis"},
            "xAxis": {
                "type": "value",
                "min": 0,
                "max": 55,
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
                with ui.element("div").style("display:flex;gap:6px;align-items:center;width:100%"):
                    sld_fob = ui.slider(min=0, max=3000, step=5, value=state["fob"]).style("flex:1")
                    inp_fob = ui.number(value=state["fob"], min=0, max=3000, step=5).props(
                        "dense outlined"
                    ).style("width:76px;flex-shrink:0;font-size:12px")

            with ui.element("div").style("flex:1;min-width:220px"):
                with ui.element("div").style("display:flex;justify-content:space-between;align-items:baseline"):
                    ui.label("Peso total").style("font-size:11px;color:#374151;font-weight:500")
                    lbl_peso = ui.label(f"{state['peso']:.1f} kg").style("font-size:12px;font-weight:700;color:#185FA5")
                with ui.element("div").style("display:flex;gap:6px;align-items:center;width:100%"):
                    sld_peso = ui.slider(min=0, max=60, step=0.1, value=state["peso"]).style("flex:1")
                    inp_peso = ui.number(value=state["peso"], min=0, max=60, step=0.1).props(
                        "dense outlined"
                    ).style("width:70px;flex-shrink:0;font-size:12px")

        def _on_posicion(e):
            state["posicion"] = e.value
            _recalcular()
        sel_posicion.on_value_change(_on_posicion)

        def _clamp(v, lo, hi):
            return max(lo, min(hi, v))

        def _on_fob_slider(e):
            val = _clamp(float(e.value or 0), 0, 3000)
            state["fob"] = val
            lbl_fob.set_text(_fmt_usd0(val))
            inp_fob.set_value(val)
            _recalcular()
        sld_fob.on_value_change(_on_fob_slider)

        def _on_fob_input(e):
            val = _clamp(float(e.value or 0), 0, 3000)
            state["fob"] = val
            lbl_fob.set_text(_fmt_usd0(val))
            sld_fob.set_value(val)
            _recalcular()
        inp_fob.on_value_change(_on_fob_input)

        def _on_peso_slider(e):
            val = round(_clamp(float(e.value or 0), 0, 60), 1)
            state["peso"] = val
            lbl_peso.set_text(f"{val:.1f} kg")
            inp_peso.set_value(val)
            _recalcular()
        sld_peso.on_value_change(_on_peso_slider)

        def _on_peso_input(e):
            val = round(_clamp(float(e.value or 0), 0, 60), 1)
            state["peso"] = val
            lbl_peso.set_text(f"{val:.1f} kg")
            sld_peso.set_value(val)
            _recalcular()
        inp_peso.on_value_change(_on_peso_input)

        # 2) Fila de 3 tarjetas, una sola fila full-width
        with ui.element("div").classes("couriers-row").style("display:flex;gap:10px;width:100%"):
            for cfg in _COURIERS:
                _build_card_shell(cfg)

        # 3) Grafico full-width
        chart_container = ui.element("div").style("width:100%")
        chart_container_ref[0] = chart_container

        _recalcular()
