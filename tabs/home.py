"""
Fase 3 — tabs/home.py
Pestaña Home (J1): saludo, 4 tarjetas (ventas hoy, mes, envíos por despachar, preguntas), "Ventas por hora" (hoy vs ayer),
"Últimas ventas" y "Necesita tu atención" compacta. NO llama a ML: todo sale de home_data.cargar_home (DB + JSON que deja el
cron home_refresh).
Se redibuja sola cada 60 s mientras la pestaña está activa. Todo por user_id.
Funciones exportadas: build_tab_home_welcome
"""
from __future__ import annotations

import json
import time
from datetime import datetime
from html import escape
from typing import Any, Callable, Dict, List, Optional

from nicegui import app, ui

from db import get_user_tab_permissions
from home_data import GRIS, NARANJA, ROJO, VERDE, cargar_home
from sales_core import ART

_DIAS = ["lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo"]
_MESES = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"]
_COL_NIVEL = {ROJO: "#DC2626", NARANJA: "#F59E0B", VERDE: "#16A34A", GRIS: "#9CA3AF"}
_CSS = (
    ".hm-w{display:flex;flex-direction:column;gap:12px;min-height:0;overflow:hidden}"
    ".hm-sal{flex:0 0 auto}.hm-sal b{display:block;font-size:24px;line-height:30px;color:#111827;font-weight:600}"
    ".hm-sal span{display:block;font-size:13px;line-height:18px;color:#6B7280}"
    ".hm-tj{flex:0 0 auto;display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px}"
    ".hm-t{background:#fff;border:1px solid #E0E2E7;border-left:4px solid var(--c);border-radius:10px;padding:10px 14px;"
    "height:100px;box-sizing:border-box;overflow:hidden}"
    ".hm-t .l{font-size:10.5px;line-height:14px;color:#6B7280;text-transform:uppercase;letter-spacing:.05em;font-weight:500;white-space:nowrap}"
    ".hm-t .v{font-size:26px;line-height:34px;font-weight:600;color:#111827;white-space:nowrap}"
    ".hm-t .s{font-size:11.5px;line-height:16px;color:#6B7280;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}"
    ".hm-t .a{font-size:10.5px;line-height:14px;color:#9CA3AF;min-height:14px}"
    ".hm-ab{flex:1 1 0;min-height:0;display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);grid-template-rows:minmax(0,1fr);gap:12px}"
    ".hm-dr{min-height:0;display:grid;grid-template-columns:minmax(0,1fr);grid-template-rows:minmax(0,3fr) minmax(0,2fr);gap:12px}"
    ".hm-b{background:#fff;border:1px solid #E0E2E7;border-radius:10px;padding:12px 14px;display:flex;flex-direction:column;"
    "min-height:0;overflow:hidden}"
    ".hm-b h4{margin:0 0 8px;font-size:11px;line-height:14px;color:#6B7280;text-transform:uppercase;letter-spacing:.05em;font-weight:500;"
    "display:flex;justify-content:space-between;align-items:baseline;flex:0 0 auto}"
    ".hm-b h4 a{font-size:12px;text-transform:none;letter-spacing:0;color:#2563EB;cursor:pointer;font-weight:500}.hm-b h4 a:hover{text-decoration:underline}"
    ".hm-vh .hm-g{flex:1 1 0;min-height:0;display:flex;flex-direction:column}.hm-vh .hm-g+.hm-g{margin-top:6px}"
    ".hm-vh .hm-ch{flex:1;min-height:0;height:auto}"
    ".hm-b h4 .r{font-size:12px;text-transform:none;letter-spacing:0;color:#6B7280;font-weight:400}.hm-b h4 .r b{color:#111827;font-weight:600}"
    ".hm-bar{display:flex;height:9px;border-radius:5px;overflow:hidden;margin:3px 0 2px}"
    ".hm-pie{flex:0 0 auto;margin-top:6px;font-size:11.5px;line-height:16px;color:#6B7280;display:flex;flex-wrap:wrap;align-items:center;gap:0 6px}"
    ".hm-pie svg{vertical-align:middle}"
    ".hm-uv{flex:1;min-height:0;overflow:hidden}"
    ".hm-uv .r{display:grid;grid-template-columns:44px minmax(0,1fr) 34px 78px;align-items:center;gap:8px;height:32px;"
    "border-bottom:1px solid #F3F4F6;font-size:13px;color:#111827}"
    ".hm-uv .r span:first-child{color:#6B7280;font-size:12px}"
    ".hm-uv .r .n{white-space:nowrap;overflow:hidden;text-overflow:ellipsis}"
    ".hm-uv .r .u{text-align:right;color:#6B7280}.hm-uv .r .m{text-align:right;font-weight:600}"
    ".hm-vacio{font-size:13px;color:#9CA3AF;padding:6px 0}"
    ".hm-li{flex:1;min-height:0;overflow:hidden;display:grid;grid-template-columns:repeat(2,minmax(0,1fr));column-gap:16px;row-gap:6px;"
    "align-content:start}"
    ".hm-i{min-width:0;display:flex;align-items:flex-start;gap:8px}.hm-i.c{cursor:pointer}.hm-i.c:hover b{text-decoration:underline}"
    ".hm-i .d{flex:0 0 10px;height:10px;border-radius:50%;margin-top:6px}"
    ".hm-i .x{flex:1;min-width:0}.hm-i .x b{display:block;font-size:15px;line-height:20px;color:#111827;font-weight:600;"
    "white-space:nowrap;overflow:hidden;text-overflow:ellipsis}"
    ".hm-i .x span{display:block;font-size:13px;line-height:17px;color:#6B7280;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}"
    "@media (max-width:768px){.hm-w{height:auto!important;overflow:visible}.hm-tj{grid-template-columns:repeat(2,minmax(0,1fr))}"
    ".hm-ab{grid-template-columns:minmax(0,1fr);grid-template-rows:none;flex:none}.hm-dr{grid-template-rows:none}.hm-b{overflow:visible}"
    ".hm-vh .hm-g{flex:none}.hm-vh .hm-ch{flex:none;height:200px}.hm-uv{flex:none}.hm-li{flex:none;grid-template-columns:minmax(0,1fr)}.hm-t .v{font-size:22px}}"
)

# Ajusta el alto del contenedor al alto real de la ventana: top real (getBoundingClientRect + scrollY) hasta el borde inferior
# - 16 px, y despues mide el desborde real de la pagina (scrollHeight - innerHeight) y se lo resta, una sola vez: por debajo de
# la Home Quasar/NiceGUI suman padding del tab panel (16) y de .nicegui-content (16), asi que 16 fijos no alcanzan. Piso de
# 420 px: en ventanas muy bajas se prefiere que la pagina scrollee antes que recortar contenido.
_JS_FIT = """
(function(){
  function filas(){  // Ultimas ventas: se muestran las filas que entran enteras en el bloque (las demas, ocultas)
    var l=document.querySelector('.hm-uv'); if(!l) return;
    var rs=l.querySelectorAll('.r'); rs.forEach(function(r){ r.style.display=''; });
    if(window.innerWidth<=768) return;
    var lb=l.getBoundingClientRect().bottom;
    rs.forEach(function(r){ if(r.getBoundingClientRect().bottom>lb+0.5) r.style.display='none'; });
  }
  function fit(){
    var el=document.querySelector('.hm-w'); if(!el||el.offsetParent===null) return;
    if(window.innerWidth<=768){ el.style.height='auto'; filas(); return; }
    var top=el.getBoundingClientRect().top+(window.scrollY||0);
    var h=window.innerHeight-top-16;
    el.style.height=Math.max(420,h)+'px';
    var ov=document.documentElement.scrollHeight-window.innerHeight;
    if(ov>0){ el.style.height=Math.max(420,h-ov)+'px'; }
    filas();
  }
  window.__hmFit=fit;
  if(!window.__hmFitBound){
    var tm=null;
    window.addEventListener('resize', function(){ clearTimeout(tm); tm=setTimeout(function(){ if(window.__hmFit) window.__hmFit(); },150); });
    window.__hmFitBound=true;
  }
  fit();
  if(document.fonts&&document.fonts.ready){ document.fonts.ready.then(function(){ if(window.__hmFit) window.__hmFit(); }); }
  setTimeout(fit,300);
})();
"""


def _require_login() -> Optional[Dict[str, Any]]:
    user = app.storage.user.get("user")
    if not user:
        ui.notify("Debes iniciar sesión para continuar", color="negative")
    return user


def _saludo(ahora: datetime) -> str:
    h = ahora.hour
    return "Buen día" if 5 <= h < 12 else ("Buenas tardes" if 12 <= h < 20 else "Buenas noches")


def _hace(ts: Optional[float]) -> str:
    """'hace 40 min' solo si el dato tiene más de 15 min; '' si es reciente o no hay hora."""
    if not ts:
        return ""
    m = (time.time() - float(ts)) / 60
    if m <= 15:
        return ""
    if m < 120:
        return f"hace {int(m)} min"
    if m < 48 * 60:
        return f"hace {int(m // 60)} h"
    return f"hace {int(m // 1440)} d"


def _hhmm(ts: Optional[float]) -> str:
    return datetime.fromtimestamp(float(ts), ART).strftime("%H:%M") if ts else ""


def _fmt_n(v: float) -> str:
    return f"{int(round(v)):,}".replace(",", ".")


def _tarjeta(color: str, etiqueta: str, valor_html: str, sub_html: str, antig: str, tip: str = "") -> None:
    with ui.element("div").classes("hm-t").style(f"--c:{color}") as t:
        ui.html(f'<div class="l">{etiqueta}</div><div class="v">{valor_html}</div><div class="s">{sub_html}</div>'
                f'<div class="a">{antig or "&nbsp;"}</div>')
    if tip:
        t.tooltip(tip)


def _tarjetas(d: Dict[str, Any]) -> None:
    from tabs.estadisticas import _abrev_pesos, _fmt_dec, _margen_visual
    tiene = d["usuario"]["tiene_ml"]
    a_ord = _hace(d.get("ts_ordenes"))
    with ui.element("div").classes("hm-tj"):
        # 1. Ventas hoy
        h = d["hoy"]
        if tiene and h:
            dif = h["u"] - h["u_ayer"]
            col = "#16A34A" if dif > 0 else ("#DC2626" if dif < 0 else "#6B7280")
            _tarjeta("#2563EB", "Ventas hoy", f'{_fmt_n(h["u"])} · {_abrev_pesos(h["monto"])}',
                     f'vs <b style="color:{col}">{_fmt_n(h["u_ayer"])}</b> ayer a esta hora', a_ord)
        else:
            _tarjeta("#2563EB", "Ventas hoy", "—", "sin cuenta de MercadoLibre" if not tiene else "sin dato", "")
        # 2. Mes
        m = d["mes"]
        if tiene and m:
            txt, col, tip = _margen_visual({"pct": m["pct"], "falta": m["falta"], "ordenes": m["ordenes"]})
            sub = (f'margen <b style="color:{col}">{txt}</b> · est. {_abrev_pesos(m["est"])}')
            _tarjeta("#16A34A", m["nombre"], _abrev_pesos(m["monto"]), sub, a_ord, tip or "")
        else:
            _tarjeta("#16A34A", _MESES[datetime.now(ART).month - 1].upper(), "—", "sin dato", "")
        # 3. Envíos por despachar
        e = d.get("envios")
        if e:
            _tarjeta("#F59E0B", "Envíos por despachar", _fmt_n(e.get("total", 0)),
                     f'{_fmt_n(e.get("flex", 0))} flex · {_fmt_n(e.get("correo", 0))} correo', _hace(e.get("ts")))
        else:
            _tarjeta("#F59E0B", "Envíos por despachar", "—", "sin dato todavía", "")
        # 4. Preguntas sin responder
        p = d.get("preguntas")
        if p:
            n = int(p.get("n", 0))
            col = "#16A34A" if n == 0 else ("#F59E0B" if n <= 5 else "#DC2626")
            _tarjeta(col, "Preguntas", f'<span style="color:{col}">{_fmt_n(n)}</span>',
                     f'sin responder · dato de las {_hhmm(p.get("ts"))}', _hace(p.get("ts")))
        else:
            _tarjeta("#9CA3AF", "Preguntas", "—", "sin dato todavía", "")


_JS_PESOS_EJE = ("v => v >= 1e6 ? '$' + (v / 1e6).toFixed(v % 1e6 ? 1 : 0).replace('.', ',') + 'M' : "
                 "(v >= 1e3 ? '$' + Math.round(v / 1e3) + 'k' : '$' + v)")


def _opciones_horaria(hz: Dict[str, Any], monto: bool = False) -> Dict[str, Any]:
    """Opciones de un echart de Ventas por hora, con eje X continuo en horas (0 a 24): ayer (gris punteada, 24 h) y hoy (hasta la
    hora actual; azul con área celeste en UNIDADES, verde con área #DCFCE7 en FACTURACIÓN). Los dos tienen un punto en x = hora
    actual fraccional con los valores de la tarjeta, así el marcador de ayer cae sobre su línea. `hz` es la serie de unidades
    (raíz de d["horaria"]) o la de monto (d["horaria"]["monto"], con monto=True). Tooltips ya armados en Python."""
    from tabs.estadisticas import _abrev_pesos
    fmt = _abrev_pesos if monto else (lambda v: f"{int(v)}")
    sufijo = "" if monto else " u"
    color, area = ("#16A34A", "#DCFCE7") if monto else ("#2563EB", "#DBEAFE")
    ah = hz["ahora"]
    x_ahora, n, m = ah["x"], ah["hoy"], ah["ayer"]
    pos_n, pos_m = ("top", "bottom") if n >= m else ("bottom", "top")
    hoy_en = {x: v for x, v in hz["hoy"]}
    tips = []
    for x, v in hz["ayer"]:  # un texto por punto de la serie de ayer (la que tiene todos los x)
        h_txt = ah["hhmm"] if x == x_ahora else (f"{int(x) - 1} h" if x else "0:00")
        t_hoy = f"hoy {fmt(hoy_en[x])}{sufijo} · " if x in hoy_en else ""
        tips.append(f"{h_txt} · {t_hoy}ayer {fmt(v)}{sufijo}")
    datos_hoy: List[Any] = [list(p) for p in hz["hoy"]]
    datos_hoy[-1] = {"value": [x_ahora, n], "symbol": "circle", "symbolSize": 9,
                     "itemStyle": {"color": color, "borderColor": "#fff", "borderWidth": 2},
                     "label": {"show": True, "position": pos_n, "formatter": f"{fmt(n)}{sufijo} ahora", "color": color,
                               "fontWeight": 600, "fontSize": 12}}
    datos_ayer: List[Any] = [list(p) for p in hz["ayer"]]
    k = next(i for i, p in enumerate(hz["ayer"]) if p[0] == x_ahora)
    datos_ayer[k] = {"value": [x_ahora, m], "symbol": "circle", "symbolSize": 8,
                     "itemStyle": {"color": "#6B7280", "borderColor": "#fff", "borderWidth": 2},
                     "label": {"show": True, "position": pos_m, "formatter": f"{fmt(m)}{sufijo} ayer a esta hora", "color": "#6B7280",
                               "fontSize": 11}}
    eje_y: Dict[str, Any] = {"type": "value", "axisLabel": {"color": "#9CA3AF", "fontSize": 11},
                             "splitLine": {"lineStyle": {"color": "#F3F4F6"}}}
    if monto:
        eje_y["axisLabel"][":formatter"] = _JS_PESOS_EJE
    else:
        eje_y["minInterval"] = 1
    return {
        "animation": False,
        "grid": {"left": 48, "right": 70, "top": 26, "bottom": 24},
        "tooltip": {"trigger": "axis",
                    ":formatter": "p => " + json.dumps(tips, ensure_ascii=False) + "[p.find(q => q.seriesName === 'ayer').dataIndex]"},
        "xAxis": {"type": "value", "min": 0, "max": 24, "interval": 1, "axisLabel": {"color": "#6B7280", "fontSize": 11},
                  "axisLine": {"lineStyle": {"color": "#E5E7EB"}}, "axisTick": {"show": False}, "splitLine": {"show": False}},
        "yAxis": eje_y,
        "series": [
            {"name": "ayer", "type": "line", "data": datos_ayer, "symbol": "none", "z": 1,
             "lineStyle": {"color": "#9CA3AF", "width": 1.8, "type": "dashed"}},
            {"name": "hoy", "type": "line", "data": datos_hoy, "symbol": "none", "z": 3,
             "lineStyle": {"color": color, "width": 2.4}, "areaStyle": {"color": area, "opacity": 0.75}},
        ],
    }


def _ventas_hora(d: Dict[str, Any]) -> None:
    """Dos gráficos apilados con el mismo eje de horas: UNIDADES arriba y FACTURACIÓN abajo, repartiéndose el alto del bloque."""
    from tabs.estadisticas import _abrev_pesos
    with ui.element("div").classes("hm-b hm-vh"):
        hz = d.get("horaria")
        if not hz:
            ui.html("<h4>Ventas por hora — hoy vs ayer</h4>")
            ui.html('<div class="hm-vacio">' + ("No se pudo calcular (ver Log)." if d["usuario"]["tiene_ml"]
                                                 else "Sin cuenta de MercadoLibre.") + "</div>")
            return
        hm = hz["monto"]
        for titulo, serie, es_monto, f in (("Unidades por hora", hz, False, lambda v: f"{int(v)}"),
                                           ("Facturación por hora", hm, True, _abrev_pesos)):
            ah = serie["ahora"]
            resumen = (f'hoy <b>{f(ah["hoy"])}</b> · ayer a esta hora <b>{f(ah["ayer"])}</b> · ayer cerró <b>{f(serie["ayer_total"])}</b>')
            with ui.element("div").classes("hm-g"):
                ui.html(f'<h4><span>{titulo}</span><span class="r">{resumen}</span></h4>')
                ui.echart(_opciones_horaria(serie, es_monto)).classes("hm-ch w-full")
        ui.html(
            '<div class="hm-pie"><svg width="18" height="6"><line x1="0" y1="3" x2="18" y2="3" stroke="#2563EB" stroke-width="2.4"/></svg>'
            'hoy (acumulado) · <svg width="18" height="6"><line x1="0" y1="3" x2="18" y2="3" stroke="#9CA3AF" stroke-width="1.8" '
            'stroke-dasharray="4 3"/></svg>ayer</div>')


def _ultimas_ventas(d: Dict[str, Any], puede: Callable[[str], bool], navegar: Optional[Callable[[str], Any]]) -> None:
    from tabs.estadisticas import _abrev_pesos
    uv = d.get("ultimas") or {"dia": "hoy", "filas": []}
    with ui.element("div").classes("hm-b"):
        titulo = "Últimas ventas" + (" (de ayer)" if uv["dia"] == "ayer" and uv["filas"] else "")
        with ui.element("h4"):
            ui.html(f"<span>{titulo}</span>").style("display:contents")
            if navegar and puede("ventas"):
                ver = ui.html("<a>ver Ventas ›</a>").style("display:contents")
                ver.on("click", lambda _e: navegar("ventas"))
        if not uv["filas"]:
            ui.html('<div class="hm-vacio">Sin ventas todavía hoy</div>')
            return
        filas = "".join(
            f'<div class="r" title="{escape(f["titulo"])}"><span>{f["hora"]}</span><span class="n">{escape(f["titulo"])}</span>'
            f'<span class="u">{f["u"]} u</span><span class="m">{_abrev_pesos(f["monto"])}</span></div>' for f in uv["filas"])
        ui.html(f'<div class="hm-uv">{filas}</div>').style("display:contents")


def _barra(b: Optional[Dict[str, int]]) -> str:
    """Barra horizontal de 9 px con 3 tramos proporcionales (más caras / iguales / más baratas); '' si no hay."""
    if not b:
        return ""
    tramos = "".join(f'<div style="flex:{b[k]} 1 0;background:{c}"></div>'
                     for k, c in (("mas", "#F59E0B"), ("igual", "#9CA3AF"), ("menos", "#16A34A")) if b.get(k))
    return f'<div class="hm-bar">{tramos}</div>'


def _atencion(d: Dict[str, Any], puede: Callable[[str], bool], navegar: Optional[Callable[[str], Any]]) -> None:
    with ui.element("div").classes("hm-b"):
        ui.html("<h4>Necesita tu atención</h4>")
        if not d["alertas"]:
            ui.html('<div class="hm-vacio">Sin datos de MercadoLibre para esta cuenta.</div>')
            return
        with ui.element("div").classes("hm-li"):
            for a in d["alertas"]:
                dest = a.get("destino")
                clic = bool(dest and navegar and puede(dest))
                hace = _hace(a.get("ts"))
                with ui.element("div").classes("hm-i" + (" c" if clic else "")) as it:
                    ui.html(
                        f'<div class="d" style="background:{_COL_NIVEL[a["nivel"]]}"></div>'
                        f'<div class="x"><b>{escape(a["titulo"])}</b>{_barra(a.get("barra"))}<span>{escape(a["detalle"])}</span></div>'
                    ).style("display:contents")
                if a.get("tip"):
                    with it:
                        ui.tooltip(a["tip"] + (f" · {hace}" if hace else "")).style("white-space:pre-line")
                else:
                    it.tooltip(f'{a["titulo"]} — {a["detalle"]}' + (f" · {hace}" if hace else ""))
                if clic:
                    it.on("click", lambda _e, k=dest: navegar(k))


def build_tab_home_welcome(container, navegar: Optional[Callable[[str], Any]] = None,
                           activa: Optional[Callable[[], bool]] = None) -> Callable[[], None]:
    """Pestaña Home. navegar(tab_key) lleva a una pestaña; activa() dice si la Home está visible (el redibujado de
    cada 60 s se saltea si no). Devuelve la función de refresco (main la llama al volver a la Home)."""
    user = _require_login()
    if not user:
        return lambda: None
    uid = int(user["id"])
    ui.add_css(_CSS)

    @ui.refreshable
    def _contenido() -> None:
        perms = get_user_tab_permissions(uid)

        def puede(key: str) -> bool:
            return bool(perms.get("admin", False)) if key == "log" else bool(perms.get(key, True))

        d = cargar_home(uid)
        ahora = d["ahora"]
        usr = d["usuario"]
        with ui.element("div").classes("hm-w"):
            tienda = f" · {usr['tienda']}" if usr.get("tienda") else ""
            ui.html(f'<div class="hm-sal"><b>{_saludo(ahora)}, {usr["nombre"] or user.get("username", "")}</b>'
                    f'<span>{_DIAS[ahora.weekday()].capitalize()} {ahora.day} de {_MESES[ahora.month - 1]} de {ahora.year}{tienda}</span></div>')
            _tarjetas(d)
            ui.timer(0.1, lambda: ui.run_javascript(_JS_FIT), once=True)  # fija el alto real una vez montado el contenedor
            with ui.element("div").classes("hm-ab"):
                _ventas_hora(d)
                with ui.element("div").classes("hm-dr"):
                    _ultimas_ventas(d, puede, navegar)
                    _atencion(d, puede, navegar)

    with container:
        _contenido()

        def _tick() -> None:
            if activa is None or activa():
                _contenido.refresh()
        ui.timer(60.0, _tick)
    return _contenido.refresh
