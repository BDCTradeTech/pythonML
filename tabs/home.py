"""
Fase 3 — tabs/home.py
Pestaña Home (H1): saludo, 4 tarjetas (ventas hoy, mes, envíos por despachar, preguntas), "Necesita tu atención" y accesos
rápidos a las pestañas permitidas. NO llama a ML: todo sale de home_data.cargar_home (DB + JSON que deja el cron home_refresh).
Se redibuja sola cada 60 s mientras la pestaña está activa. Todo por user_id.
Funciones exportadas: build_tab_home_welcome
"""
from __future__ import annotations

import time
from datetime import datetime
from html import escape
from typing import Any, Callable, Dict, List, Optional, Tuple

from nicegui import app, ui

from db import get_user_tab_permissions
from home_data import NARANJA, ROJO, VERDE, cargar_home
from sales_core import ART
from tabs.constants import TAB_DESCRIPTIONS, LABEL_BY_TAB

_DIAS = ["lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo"]
_MESES = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"]
_COL_NIVEL = {ROJO: "#DC2626", NARANJA: "#F59E0B", VERDE: "#16A34A"}
# Color del ícono de cada tile según el menú de la barra de arriba al que pertenece.
_COL_MENU = {"MERCADOLIBRE": "#2563EB", "TIENDANUBE": "#0EA5E9", "BDC": "#7C3AED", "COMEX": "#0891B2",
             "IMPUESTOS": "#D97706", "CONFIG": "#6B7280", "ADMIN": "#DC2626"}
_DESC_EXTRA = {"log": "estado y detalle de las corridas de los crons."}  # pestañas sin entrada en TAB_DESCRIPTIONS

# Accesos agrupados por los menús de la barra. Un grupo de más de _TOPE_FILAS tiles se parte en columnas; los chicos se
# juntan en una columna con "A · B" de encabezado; siempre _NCOLS columnas del mismo ancho (más solo si no hay otra forma).
_TOPE_FILAS = 8
_NCOLS = 5
_TILE_MAX, _TILE_MIN, _TILE_GAP = 48, 40, 8  # px: el tile mide 48 y baja hasta 40 si falta alto, antes de agregar columnas

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
    ".hm-ab{flex:1 1 0;min-height:0;display:grid;grid-template-columns:minmax(0,1fr) minmax(0,2fr);grid-template-rows:minmax(0,1fr);gap:12px}"
    ".hm-b{background:#fff;border:1px solid #E0E2E7;border-radius:10px;padding:12px 14px;display:flex;flex-direction:column;"
    "min-height:0;overflow:hidden}"
    ".hm-b h4{margin:0 0 8px;font-size:11px;line-height:14px;color:#6B7280;text-transform:uppercase;letter-spacing:.05em;font-weight:500}"
    ".hm-li{flex:1;min-height:0;display:flex;flex-direction:column;gap:6px}"
    ".hm-i{flex:1 1 0;min-height:0;max-height:120px;display:flex;align-items:center;gap:10px;padding:0 10px;border:1px solid #F3F4F6;"
    "border-radius:8px;background:#F9FAFB;overflow:hidden}"
    ".hm-i.c{cursor:pointer}.hm-i.c:hover{background:#F3F4F6}"
    ".hm-i .d{flex:0 0 10px;height:10px;border-radius:50%}"
    ".hm-i .x{flex:1;min-width:0}.hm-i .x b{display:block;font-size:13px;line-height:18px;color:#111827;font-weight:600;"
    "white-space:nowrap;overflow:hidden;text-overflow:ellipsis}"
    ".hm-i .x span{display:block;font-size:11.5px;line-height:16px;color:#6B7280;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}"
    ".hm-i .e{flex:0 0 auto;font-size:10.5px;color:#9CA3AF;white-space:nowrap}.hm-i .g{flex:0 0 auto;font-size:20px;color:#9CA3AF}"
    ".hm-cols{flex:1;min-height:0;display:grid;grid-template-columns:repeat(var(--nc),minmax(0,1fr));grid-template-rows:minmax(0,1fr);gap:20px}"
    ".hm-col{min-width:0;min-height:0;display:flex;flex-direction:column}"
    ".hm-hd{flex:0 0 auto;height:20px;padding-bottom:5px;margin-bottom:8px;border-bottom:1px solid #E5E7EB;font-size:14px;line-height:20px;"
    "font-weight:700;color:#2563EB;text-transform:uppercase;letter-spacing:.04em;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}"
    ".hm-hd.v{border-bottom-color:transparent}"
    ".hm-tl{flex:1;min-height:0;display:grid;grid-template-rows:repeat(var(--rows),minmax(__TMIN__px,__TMAX__px));gap:__TGAP__px;align-content:start}"
    ".hm-a{display:flex;align-items:center;gap:10px;padding:0 10px;border:1px solid #F3F4F6;border-radius:8px;background:#F9FAFB;"
    "cursor:pointer;min-width:0;min-height:0;overflow:hidden}.hm-a:hover{background:#F3F4F6}"
    ".hm-a .ic{flex:0 0 30px;height:30px;border-radius:8px;display:flex;align-items:center;justify-content:center;color:#fff}"
    ".hm-a .ic i{font-size:20px}"
    ".hm-a b{flex:1;min-width:0;font-size:17px;line-height:22px;color:#111827;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}"
    ".hm-mob{display:none}"
    "@media (max-width:768px){.hm-w{height:auto!important;overflow:visible}.hm-tj{grid-template-columns:repeat(2,minmax(0,1fr))}"
    ".hm-ab{grid-template-columns:minmax(0,1fr);grid-template-rows:none;flex:none}.hm-b{overflow:visible}.hm-li{flex:none}"
    ".hm-i{flex:none;min-height:56px}.hm-t .v{font-size:22px}.hm-desk{display:none}.hm-mob{display:block}"
    ".hm-mg{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));grid-auto-rows:44px;gap:8px;margin-bottom:16px}"
    ".hm-a b{font-size:14px}}"
).replace("__TMIN__", str(_TILE_MIN)).replace("__TMAX__", str(_TILE_MAX)).replace("__TGAP__", str(_TILE_GAP))

# Ajusta el alto del contenedor al alto real de la ventana: top real (getBoundingClientRect) hasta el borde inferior - 16 px.
_JS_FIT = """
(function(){
  function fit(){
    var el=document.querySelector('.hm-w'); if(!el||el.offsetParent===null) return;
    if(window.innerWidth<=768){ el.style.height='auto'; return; }
    var top=el.getBoundingClientRect().top;
    el.style.height=Math.max(420, window.innerHeight-top-16)+'px';
  }
  window.__hmFit=fit;
  if(!window.__hmFitBound){ window.addEventListener('resize', function(){ if(window.__hmFit) window.__hmFit(); }); window.__hmFitBound=true; }
  fit(); setTimeout(fit,60); setTimeout(fit,300);
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


def _atencion(d: Dict[str, Any], puede: Callable[[str], bool], navegar: Optional[Callable[[str], Any]]) -> None:
    with ui.element("div").classes("hm-b"):
        ui.html("<h4>Necesita tu atención</h4>")
        with ui.element("div").classes("hm-li"):
            if not d["alertas"]:
                ui.html('<div style="font-size:12px;color:#9CA3AF">Sin datos de MercadoLibre para esta cuenta.</div>')
            for a in d["alertas"]:
                dest = a.get("destino")
                clic = bool(dest and navegar and puede(dest))
                with ui.element("div").classes("hm-i" + (" c" if clic else "")) as it:
                    ui.html(
                        f'<div class="d" style="background:{_COL_NIVEL[a["nivel"]]}"></div>'
                        f'<div class="x"><b>{escape(a["titulo"])}</b><span>{escape(a["detalle"])}</span></div>'
                        f'<div class="e">{_hace(a.get("ts"))}</div>' + ('<div class="g">›</div>' if clic else '')
                    ).style("display:contents")
                if clic:
                    it.on("click", lambda _e, k=dest: navegar(k))


def _grupos_visibles(menus: List[Any], perms: Dict[str, bool], tiene_tn: bool) -> List[Any]:
    """[(menú, [(etiqueta, nav_key, ícono)])] con los mismos criterios de visibilidad que la barra de arriba: permiso por
    pestaña (get_user_tab_permissions, con el default de cada ítem), TIENDANUBE solo con credenciales y ADMIN solo con permiso admin.
    Un menú sin pestañas permitidas no aparece."""
    out = []
    for nombre, cond, items in menus or []:
        if (cond == "tiendanube" and not tiene_tn) or (cond == "admin" and not perms.get("admin", False)):
            continue
        tiles = [(et, key, ic) for et, key, perm, ic, dflt in items if perms.get(perm, dflt)]
        if tiles:
            out.append((nombre, tiles))
    return out


def _armar_columnas(grupos: List[Any]) -> Tuple[List[Dict[str, Any]], int]:
    """Reparte los grupos en columnas de a lo sumo `tope` tiles (arranca en _TOPE_FILAS y sube solo si hacen falta más de
    _NCOLS columnas). Un grupo más largo que el tope se parte en partes parejas (las siguientes sin encabezado); los grupos
    chicos se juntan, en el orden de la barra, en una columna con "A · B" de encabezado. Devuelve (columnas, tope usado):
    columna = {"head": str, "tiles": [(etiqueta, nav_key, ícono, menú)]}."""
    tope = _TOPE_FILAS
    while True:
        cols: List[Dict[str, Any]] = []
        for nombre, tiles in grupos:
            n = -(-len(tiles) // tope)
            tam = -(-len(tiles) // n)
            for j in range(n):
                parte = [(et, k, ic, nombre) for et, k, ic in tiles[j * tam:(j + 1) * tam]]
                if j == 0 and cols and cols[-1]["abierta"] and len(cols[-1]["tiles"]) + len(parte) <= tope:
                    cols[-1]["tiles"] += parte
                    cols[-1]["names"].append(nombre)
                else:
                    cols.append({"names": [nombre] if j == 0 else [], "tiles": parte, "abierta": j == 0 and n == 1})
        if len(cols) <= _NCOLS or tope >= 40:
            break
        tope += 1
    for c in cols:
        c["head"] = " · ".join(c.pop("names"))
        c.pop("abierta")
    return cols, tope


def _tile(etiqueta: str, key: str, icono: str, menu: str, navegar: Optional[Callable[[str], Any]]) -> None:
    nombre = LABEL_BY_TAB.get(key) or etiqueta.capitalize()
    desc = TAB_DESCRIPTIONS.get(key) or _DESC_EXTRA.get(key, "")
    with ui.element("div").classes("hm-a") as b:
        ui.html(f'<div class="ic" style="background:{_COL_MENU.get(menu, "#6B7280")}"><i class="material-icons">{escape(icono)}</i></div>'
                f'<b>{escape(nombre)}</b>').style("display:contents")
    if desc:
        b.tooltip(desc)  # la descripción va solo en el tooltip
    if navegar:
        b.on("click", lambda _e, k=key: navegar(k))


def _accesos(grupos: List[Any], navegar: Optional[Callable[[str], Any]]) -> None:
    cols, tope = _armar_columnas(grupos)
    filas = max((len(c["tiles"]) for c in cols), default=1)
    with ui.element("div").classes("hm-b"):
        ui.html("<h4>Accesos</h4>")
        with ui.element("div").classes("hm-desk hm-cols").style(f"--nc:{max(_NCOLS, len(cols))};--rows:{filas}"):
            for c in cols:
                with ui.element("div").classes("hm-col"):
                    ui.html(f'<div class="hm-hd{"" if c["head"] else " v"}">{escape(c["head"])}</div>')
                    with ui.element("div").classes("hm-tl"):
                        for et, key, ic, menu in c["tiles"]:
                            _tile(et, key, ic, menu, navegar)
        with ui.element("div").classes("hm-mob"):  # celular: un grupo debajo del otro, tiles en 2 columnas
            for nombre, tiles in grupos:
                ui.html(f'<div class="hm-hd">{escape(nombre)}</div>')
                with ui.element("div").classes("hm-mg"):
                    for et, key, ic in tiles:
                        _tile(et, key, ic, nombre, navegar)


def build_tab_home_welcome(container, navegar: Optional[Callable[[str], Any]] = None,
                           activa: Optional[Callable[[], bool]] = None, menus: Optional[List[Any]] = None,
                           tiene_tn: bool = False) -> Callable[[], None]:
    """Pestaña Home. navegar(tab_key) lleva a una pestaña; activa() dice si la Home está visible (el redibujado de
    cada 60 s se saltea si no). menus = estructura de los menús de la barra (main.HOME_MENUS); tiene_tn = la cuenta tiene
    Tienda Nube vinculada. Devuelve la función de refresco (main la llama al volver a la Home)."""
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
                _atencion(d, puede, navegar)
                _accesos(_grupos_visibles(menus or [], perms, tiene_tn), navegar)

    with container:
        _contenido()

        def _tick() -> None:
            if activa is None or activa():
                _contenido.refresh()
        ui.timer(60.0, _tick)
    return _contenido.refresh
