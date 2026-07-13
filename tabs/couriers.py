"""
tabs/couriers.py
Comparador de couriers de importacion. Persistencia por usuario en couriers_config
(flete/kg, cambio PA, envio a domicilio por despachante). FOB, peso y posicion
arancelaria son de sesion (no se persisten).
"""
from __future__ import annotations

from nicegui import app, ui

from db import ensure_courier_config_defaults, set_courier_config_campo, get_cotizador_tabla
from tabs.admin import TABLA_POSICION_DEFAULT

TC = 1460.0
_OPCION_CAMBIO_PA = "Cambio PA"


def _pa_efectivo(pa: float, posicion: str) -> float:
    return pa if posicion == _OPCION_CAMBIO_PA else 0.0


def _sixstar_usd(kg: float, fob: float, flete_kg: float, pa_ef: float, envio: float) -> float:
    return 63 + fob * 0.0242 + kg * 3.63 + kg * flete_kg + pa_ef + envio


def _lhs_usd(kg: float, fob: float, flete_kg: float, pa_ef: float, envio: float) -> float:
    sed = 15.0 if fob > 2500 else 0.0
    return 27 + kg * flete_kg + pa_ef + envio + sed


def _nc_usd(kg: float, fob: float, flete_kg: float, pa_ef: float, envio: float) -> float:
    return 31 + kg * flete_kg + pa_ef + envio


def _pct_traida(total_usd: float, fob: float) -> float:
    return (total_usd / fob) * 100 if fob else 0.0


def _fmt_usd(v: float) -> str:
    return f"u$ {v:,.2f}"


def _fmt_usd0(v: float) -> str:
    return f"USD {v:,.0f}"


def _fmt_ars(v: float) -> str:
    return f"$ {v:,.2f}"


_COURIERS = [
    {
        "key": "sixstar",
        "despachante": "SIXSTAR",
        "nombre": "SIXSTAR",
        "color": "#2a78d6",
        "defaults": {"flete_kg": 7.50, "cambio_pa": 150.0, "envio_domicilio": 23.0},
        "usd_fn": _sixstar_usd,
        "fixed_rows_fn": lambda kg, fob: [
            ("Handling", "u$ 63.00", 63.0),
            ("G. Administrativos", "FOB × 2.42%", fob * 0.0242),
            ("Honorarios", "kg × u$ 3.63", kg * 3.63),
        ],
    },
    {
        "key": "lhs",
        "despachante": "LHS",
        "nombre": "LHS",
        "color": "#1baf7a",
        "defaults": {"flete_kg": 8.50, "cambio_pa": 200.0, "envio_domicilio": 10.0},
        "usd_fn": _lhs_usd,
        "fixed_rows_fn": lambda kg, fob: [
            ("Gastos Operativos", "u$ 27.00", 27.0),
        ],
    },
    {
        "key": "nc",
        "despachante": "NC",
        "nombre": "NC Supplies",
        "color": "#eda100",
        "defaults": {"flete_kg": 9.50, "cambio_pa": 250.0, "envio_domicilio": 30.0},
        "usd_fn": _nc_usd,
        "fixed_rows_fn": lambda kg, fob: [
            ("Serv. y Honorarios", "u$ 31.00", 31.0),
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
    uid = user["id"]

    state = {"fob": 2900.0, "peso": 35.0, "posicion": _OPCION_CAMBIO_PA}
    for cfg in _COURIERS:
        vals = ensure_courier_config_defaults(uid, cfg["despachante"], cfg["defaults"])
        state[f"flete_{cfg['key']}"] = float(vals["flete_kg"])
        state[f"pa_{cfg['key']}"] = float(vals["cambio_pa"])
        state[f"envio_{cfg['key']}"] = float(vals["envio_domicilio"])

    content_refs: dict = {}
    badge_refs: dict = {}
    chart_container_ref: list = [None]

    def _render_card_content(cfg: dict):
        kg, fob = state["peso"], state["fob"]
        flete_kg = state[f"flete_{cfg['key']}"]
        pa = state[f"pa_{cfg['key']}"]
        envio = state[f"envio_{cfg['key']}"]
        pa_ef = _pa_efectivo(pa, state["posicion"])
        total_usd = cfg["usd_fn"](kg, fob, flete_kg, pa_ef, envio)
        pct = _pct_traida(total_usd, fob)

        badge_refs[cfg["key"]].set_text(f"{pct:.1f}%")

        cont = content_refs[cfg["key"]]
        cont.clear()
        filas = cfg["fixed_rows_fn"](kg, fob) + [
            ("Flete internacional", "kg × Flete/kg", kg * flete_kg),
            ("Cambio PA", "", pa_ef),
        ]
        if cfg["key"] == "lhs" and fob > 2500:
            filas.append(("SED", "", 15.0))

        with cont:
            for nombre, formula, valor in filas:
                with ui.element("div").style(
                    "display:flex;justify-content:space-between;align-items:baseline;gap:6px"
                ):
                    ui.label(nombre).style(
                        "font-size:10px;color:#374151;white-space:nowrap;overflow:hidden;"
                        "text-overflow:ellipsis"
                    )
                    if formula:
                        ui.label(formula).style(
                            "font-size:8px;color:#9ca3af;white-space:nowrap;overflow:hidden;"
                            "text-overflow:ellipsis;flex:1;text-align:right"
                        )
                    else:
                        ui.element("div").style("flex:1")
                    ui.label(_fmt_usd(valor)).style(
                        "font-size:10px;font-weight:500;color:#374151;white-space:nowrap;flex-shrink:0"
                    )
            # Envío a Domicilio se muestra en pesos (envio_usd x TC); el resto del
            # desglose va en u$. En el Total (u$) el envío se suma en USD igual.
            with ui.element("div").style(
                "display:flex;justify-content:space-between;align-items:baseline;gap:6px"
            ):
                ui.label("Envío a Domicilio").style(
                    "font-size:10px;color:#374151;white-space:nowrap;overflow:hidden;"
                    "text-overflow:ellipsis"
                )
                ui.element("div").style("flex:1")
                ui.label(_fmt_ars(envio * TC)).style(
                    "font-size:10px;font-weight:500;color:#374151;white-space:nowrap;flex-shrink:0"
                )
            ui.element("div").style("border-top:0.5px solid #e2e8f0;margin:2px 0")
            with ui.element("div").style("display:flex;justify-content:space-between;align-items:center"):
                ui.label("Total courier").style("font-size:11px;font-weight:600;color:#185FA5")
                ui.label(_fmt_usd(total_usd)).style("font-size:12px;font-weight:700;color:#185FA5")

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
                with ui.row().style("gap:6px"):
                    with ui.element("div").style("flex:1;min-width:0"):
                        ui.label("Flete/kg (u$)").style("font-size:8px;color:#6b7280;display:block")
                        inp_flete = ui.number(value=state[f"flete_{cfg['key']}"], min=0, step=0.5).props(
                            "dense outlined"
                        ).style("width:100%;font-size:11px")
                    with ui.element("div").style("flex:1;min-width:0"):
                        ui.label("PA (u$)").style("font-size:8px;color:#6b7280;display:block")
                        inp_pa = ui.number(value=state[f"pa_{cfg['key']}"], min=0, max=300, step=5).props(
                            "dense outlined"
                        ).style("width:100%;font-size:11px")
                    with ui.element("div").style("flex:1;min-width:0"):
                        ui.label("Envío a Domicilio (u$)").style("font-size:8px;color:#6b7280;display:block")
                        inp_envio = ui.number(value=state[f"envio_{cfg['key']}"], min=0, step=1).props(
                            "dense outlined"
                        ).style("width:100%;font-size:11px")

                content = ui.element("div").style("display:flex;flex-direction:column;gap:2px;margin-top:2px")
                content_refs[cfg["key"]] = content

        def _on_flete(e, key=cfg["key"], despachante=cfg["despachante"]):
            valor = float(e.value or 0)
            state[f"flete_{key}"] = valor
            set_courier_config_campo(uid, despachante, "flete_kg", valor)
            _recalcular()
        inp_flete.on_value_change(_on_flete)

        def _on_pa(e, key=cfg["key"], despachante=cfg["despachante"]):
            valor = float(e.value or 0)
            state[f"pa_{key}"] = valor
            set_courier_config_campo(uid, despachante, "cambio_pa", valor)
            _recalcular()
        inp_pa.on_value_change(_on_pa)

        def _on_envio(e, key=cfg["key"], despachante=cfg["despachante"]):
            valor = float(e.value or 0)
            state[f"envio_{key}"] = valor
            set_courier_config_campo(uid, despachante, "envio_domicilio", valor)
            _recalcular()
        inp_envio.on_value_change(_on_envio)

    def _render_chart():
        chart_container_ref[0].clear()
        fob = state["fob"]
        peso = state["peso"]
        posicion = state["posicion"]
        kgs = list(range(0, 61))

        pct_en_peso_by_key = {}
        for cfg in _COURIERS:
            flete_kg = state[f"flete_{cfg['key']}"]
            pa_ef = _pa_efectivo(state[f"pa_{cfg['key']}"], posicion)
            envio = state[f"envio_{cfg['key']}"]
            pct_en_peso_by_key[cfg["key"]] = round(
                _pct_traida(cfg["usd_fn"](peso, fob, flete_kg, pa_ef, envio), fob), 1
            )

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
            pa_ef = _pa_efectivo(state[f"pa_{cfg['key']}"], posicion)
            envio = state[f"envio_{cfg['key']}"]
            pts = [round(_pct_traida(cfg["usd_fn"](k, fob, flete_kg, pa_ef, envio), fob), 2) for k in kgs]
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
        # 1) Barra superior — posicion arancelaria + FOB + Peso (sliders)
        posiciones = get_cotizador_tabla("posicion", uid) or TABLA_POSICION_DEFAULT
        opciones_posicion = {_OPCION_CAMBIO_PA: _OPCION_CAMBIO_PA}
        opciones_posicion.update({
            row.get("posicion", ""): row.get("posicion", "")
            for row in posiciones if row.get("posicion")
        })

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

        # 2) Fila de 3 tarjetas, una sola fila full-width
        with ui.element("div").classes("couriers-row").style("display:flex;gap:10px;width:100%"):
            for cfg in _COURIERS:
                _build_card_shell(cfg)

        # 3) Grafico full-width
        chart_container = ui.element("div").style("width:100%")
        chart_container_ref[0] = chart_container

        _recalcular()
