"""
tabs/couriers.py
Comparador de couriers de importacion — 100% efimero, sin persistencia en DB.
Los 3 inputs de arriba viven solo en la sesion del navegador (estado reactivo NiceGUI).
"""
from __future__ import annotations

from nicegui import app, ui

TC = 1460  # dolar oficial, hardcodeado por ahora


def _sixstar_desglose(kg: float, fob_usd: float, pa_usd: float, tc: float = TC) -> dict:
    handling = 63 * tc
    g_admin = fob_usd * 0.0242 * tc
    honorarios = kg * 3.63 * tc
    pa = pa_usd * tc
    return {
        "handling": handling,
        "g_admin": g_admin,
        "honorarios": honorarios,
        "pa": pa,
        "total": handling + g_admin + honorarios + pa,
    }


def _sixstar_pct(kg: float, fob_usd: float, pa_usd: float) -> float:
    if not fob_usd:
        return 0.0
    total_usd = 63 + fob_usd * 0.0242 + kg * 3.63 + pa_usd
    return (total_usd / fob_usd) * 100


def _fmt_ars(v: float) -> str:
    return f"$ {v:,.0f}".replace(",", ".")


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

    state = {"fob": 2000.0, "peso": 20.0, "pa": 100.0}
    izquierda_ref: list = [None]
    derecha_ref: list = [None]

    def _card_sixstar():
        d = _sixstar_desglose(state["peso"], state["fob"], state["pa"])
        pct = _sixstar_pct(state["peso"], state["fob"], state["pa"])
        with ui.element("div").style(
            "border:0.5px solid #e2e8f0;border-radius:8px;overflow:hidden"
        ):
            with ui.element("div").style(
                "background:#2a78d6;padding:8px 12px;display:flex;"
                "justify-content:space-between;align-items:center"
            ):
                with ui.row().style("gap:8px;align-items:center"):
                    ui.element("div").style(
                        "width:9px;height:9px;border-radius:50%;background:#fff"
                    )
                    ui.label("SIXSTAR").style("font-size:13px;font-weight:600;color:#fff")
                ui.label("activo").style(
                    "font-size:9px;color:#fff;background:rgba(255,255,255,.22);"
                    "padding:2px 8px;border-radius:10px"
                )
            with ui.element("div").style("padding:10px 12px;display:flex;flex-direction:column;gap:6px"):
                filas = [
                    ("Handling (fijo)", "USD 63 × TC", d["handling"]),
                    ("G. Administrativos", "FOB × 2.42% × TC", d["g_admin"]),
                    ("Honorarios", "KGS × USD 3.63 × TC", d["honorarios"]),
                    ("Cambio PA", "PA (USD) × TC", d["pa"]),
                ]
                for nombre, formula, valor in filas:
                    with ui.element("div").style("display:flex;justify-content:space-between;align-items:baseline"):
                        with ui.element("div"):
                            ui.label(nombre).style("font-size:11px;color:#374151;display:block")
                            ui.label(formula).style("font-size:9px;color:#9ca3af;display:block")
                        ui.label(_fmt_ars(valor)).style("font-size:11px;font-weight:500;color:#374151")
                ui.element("div").style("border-top:0.5px solid #e2e8f0;margin:2px 0")
                with ui.element("div").style("display:flex;justify-content:space-between;align-items:center"):
                    ui.label("Total courier").style("font-size:12px;font-weight:600;color:#185FA5")
                    ui.label(_fmt_ars(d["total"])).style("font-size:13px;font-weight:700;color:#185FA5")
                with ui.element("div").style("display:flex;justify-content:space-between;align-items:center"):
                    ui.label("% de traida").style("font-size:11px;color:#6b7280")
                    ui.label(f"{pct:.1f}%").style("font-size:12px;font-weight:600;color:#2a78d6")

    def _card_pendiente(nombre: str, color: str, placeholder: str):
        with ui.element("div").style(
            f"border:0.5px solid #e2e8f0;border-radius:8px;overflow:hidden;opacity:.6"
        ):
            with ui.element("div").style(
                f"background:{color};padding:8px 12px;display:flex;"
                "justify-content:space-between;align-items:center"
            ):
                with ui.row().style("gap:8px;align-items:center"):
                    ui.element("div").style(
                        "width:9px;height:9px;border-radius:50%;background:#fff"
                    )
                    ui.label(nombre).style("font-size:13px;font-weight:600;color:#fff")
                ui.label("pendiente").style(
                    "font-size:9px;color:#fff;background:rgba(255,255,255,.22);"
                    "padding:2px 8px;border-radius:10px"
                )
            with ui.element("div").style("padding:18px 12px;text-align:center"):
                ui.label(placeholder).style("font-size:11px;color:#9ca3af;font-style:italic")

    def _render_izquierda():
        izquierda_ref[0].clear()
        with izquierda_ref[0]:
            _card_sixstar()
            _card_pendiente("LHS", "#1baf7a", "Gastos Operativos — Formula pendiente")
            _card_pendiente("NC Supplies", "#eda100", "Servicios y Honorarios — Formula pendiente")

    def _render_derecha():
        derecha_ref[0].clear()
        kgs = list(range(0, 61))
        sixstar_pts = [round(_sixstar_pct(k, state["fob"], state["pa"]), 2) for k in kgs]
        lhs_pts = [round(v * 0.77, 2) for v in sixstar_pts]
        nc_pts = [round(v * 0.88, 2) for v in sixstar_pts]

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
            "series": [
                {
                    "name": "SIXSTAR",
                    "type": "line",
                    "showSymbol": False,
                    "lineStyle": {"color": "#2a78d6", "width": 2},
                    "itemStyle": {"color": "#2a78d6"},
                    "data": [[k, v] for k, v in zip(kgs, sixstar_pts)],
                    "markLine": {
                        "symbol": "none",
                        "silent": True,
                        "lineStyle": {"color": "#6b7280", "type": "dashed"},
                        "label": {"formatter": "{c} kg", "position": "insideEndTop", "fontSize": 9, "color": "#6b7280"},
                        "data": [{"xAxis": state["peso"]}],
                    },
                },
                {
                    "name": "LHS",
                    "type": "line",
                    "showSymbol": False,
                    "lineStyle": {"color": "#1baf7a", "width": 2, "type": "dashed"},
                    "itemStyle": {"color": "#1baf7a"},
                    "data": [[k, v] for k, v in zip(kgs, lhs_pts)],
                },
                {
                    "name": "NC Supplies",
                    "type": "line",
                    "showSymbol": False,
                    "lineStyle": {"color": "#eda100", "width": 2, "type": "dashed"},
                    "itemStyle": {"color": "#eda100"},
                    "data": [[k, v] for k, v in zip(kgs, nc_pts)],
                },
            ],
        }
        with derecha_ref[0]:
            with ui.element("div").style("border:0.5px solid #e2e8f0;border-radius:8px;padding:8px 4px 4px"):
                ui.label("% de traida por kg").style("font-size:11px;font-weight:600;color:#185FA5;padding-left:8px")
                ui.echart(chart_options).classes("w-full").style("height:340px")

    def _recalcular():
        _render_izquierda()
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

            with ui.row().style("gap:8px;align-items:center"):
                ui.label("Cambio PA:").style("font-size:12px;color:#374151;font-weight:500")
                lbl_pa = ui.label(f"USD {state['pa']:.0f}").style("font-size:12px;font-weight:700;color:#185FA5;min-width:60px")
                sld_pa = ui.slider(min=0, max=300, step=5, value=state["pa"]).style("width:220px")

        def _on_fob(e):
            state["fob"] = float(e.value or 0)
            _recalcular()
        inp_fob.on_value_change(_on_fob)

        def _on_peso(e):
            state["peso"] = float(e.value or 0)
            lbl_peso.set_text(f"{state['peso']:.0f} kg")
            _recalcular()
        sld_peso.on_value_change(_on_peso)

        def _on_pa(e):
            state["pa"] = float(e.value or 0)
            lbl_pa.set_text(f"USD {state['pa']:.0f}")
            _recalcular()
        sld_pa.on_value_change(_on_pa)

        # Grid dos columnas: izquierda cards, derecha grafico
        with ui.element("div").classes("couriers-grid").style(
            "display:grid;grid-template-columns:2fr 3fr;gap:16px;align-items:start"
        ):
            izquierda = ui.element("div").style("display:flex;flex-direction:column;gap:10px")
            derecha = ui.element("div")

        izquierda_ref[0] = izquierda
        derecha_ref[0] = derecha
        _recalcular()
