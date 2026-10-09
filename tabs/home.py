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
from typing import Any, Callable, Dict, Optional

from nicegui import app, ui

from db import get_user_tab_permissions
from home_data import NARANJA, ROJO, VERDE, cargar_home
from sales_core import ART
from tabs.constants import TAB_DESCRIPTIONS, TAB_REGISTRY, LABEL_BY_TAB

_DIAS = ["lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo"]
_MESES = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"]
_COL_NIVEL = {ROJO: "#DC2626", NARANJA: "#F59E0B", VERDE: "#16A34A"}
_COL_SECCION = {"Home": "#16A34A", "MercadoLibre": "#2563EB", "BDC": "#7C3AED", "Comex": "#0891B2", "Impuestos": "#D97706",
                "Config": "#6B7280", "TiendaNube": "#0EA5E9", "Admin": "#DC2626"}
_ICONOS = {"dashboard": "dashboard", "estadisticas": "bar_chart", "ventas": "receipt_long", "productos": "inventory_2",
           "salud": "health_and_safety", "descuentos": "percent", "cuotas": "credit_card", "promos": "local_offer",
           "publicidad": "campaign", "competidores": "compare_arrows", "preguntas": "question_answer", "flex": "two_wheeler",
           "busqueda": "search", "stock": "warehouse", "balance": "account_balance", "compras": "request_quote",
           "stock_bdc": "inventory", "compras_lista": "shopping_cart", "pedidos": "local_shipping", "historicos": "history",
           "importacion": "upload_file", "guias": "flight_land", "transferencias": "swap_horiz", "couriers": "airport_shuttle",
           "pesos": "currency_exchange", "arca": "gavel", "gastos": "payments", "datos": "storage", "configuracion": "settings",
           "tn_vinculacion": "link", "tn_diferencias": "difference", "admin": "admin_panel_settings", "actividad": "monitor_heart"}

# Alturas (px) para entrar sin scroll a 1900x930: ver el cálculo en el reporte. El alto del contenedor es
# 100vh - _CHROME; el bloque de abajo toma lo que sobra (flex:1) y sus grillas reparten ese alto (filas 1fr), nunca crecen.
_CHROME = 150
_FILA_ACCESO_MIN = 44
_ALTO_GRILLA_ACCESOS = 536  # alto estimado de la grilla de accesos a 930 px de viewport (para elegir 2, 3 o 4 columnas)

_CSS = (
    ".hm-w{display:flex;flex-direction:column;gap:12px;height:calc(100vh - %dpx);min-height:520px;overflow:hidden}"
    ".hm-sal{flex:0 0 auto}.hm-sal b{display:block;font-size:24px;line-height:30px;color:#111827;font-weight:600}"
    ".hm-sal span{display:block;font-size:13px;line-height:18px;color:#6B7280}"
    ".hm-tj{flex:0 0 auto;display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px}"
    ".hm-t{background:#fff;border:1px solid #E0E2E7;border-left:4px solid var(--c);border-radius:10px;padding:10px 14px;"
    "height:100px;box-sizing:border-box;overflow:hidden}"
    ".hm-t .l{font-size:10.5px;line-height:14px;color:#6B7280;text-transform:uppercase;letter-spacing:.05em;font-weight:500;white-space:nowrap}"
    ".hm-t .v{font-size:26px;line-height:34px;font-weight:600;color:#111827;white-space:nowrap}"
    ".hm-t .s{font-size:11.5px;line-height:16px;color:#6B7280;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}"
    ".hm-t .a{font-size:10.5px;line-height:14px;color:#9CA3AF;min-height:14px}"
    ".hm-ab{flex:1 1 0;min-height:0;display:grid;grid-template-columns:minmax(0,3fr) minmax(0,2fr);gap:12px}"
    ".hm-b{background:#fff;border:1px solid #E0E2E7;border-radius:10px;padding:12px 14px;display:flex;flex-direction:column;"
    "min-height:0;overflow:hidden}"
    ".hm-b h4{margin:0 0 8px;font-size:11px;line-height:14px;color:#6B7280;text-transform:uppercase;letter-spacing:.05em;font-weight:500}"
    ".hm-li{flex:1;min-height:0;display:flex;flex-direction:column;gap:6px}"
    ".hm-i{flex:0 0 auto;display:flex;align-items:center;gap:10px;padding:8px 10px;border:1px solid #F3F4F6;border-radius:8px;"
    "background:#F9FAFB;min-height:0;overflow:hidden}"
    ".hm-i.c{cursor:pointer}.hm-i.c:hover{background:#F3F4F6}"
    ".hm-i .d{flex:0 0 10px;height:10px;border-radius:50%}"
    ".hm-i .x{flex:1;min-width:0}.hm-i .x b{display:block;font-size:13px;line-height:18px;color:#111827;font-weight:600;"
    "white-space:nowrap;overflow:hidden;text-overflow:ellipsis}"
    ".hm-i .x span{display:block;font-size:11.5px;line-height:16px;color:#6B7280;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}"
    ".hm-i .e{flex:0 0 auto;font-size:10.5px;color:#9CA3AF;white-space:nowrap}.hm-i .g{flex:0 0 auto;font-size:20px;color:#9CA3AF}"
    ".hm-gr{flex:1;min-height:0;display:grid;grid-template-columns:repeat(var(--n),minmax(0,1fr));grid-auto-rows:minmax(0,1fr);gap:6px}"
    ".hm-a{display:flex;align-items:center;gap:8px;padding:0 8px;border:1px solid #F3F4F6;border-radius:8px;background:#F9FAFB;"
    "cursor:pointer;min-width:0;min-height:0;overflow:hidden}.hm-a:hover{background:#F3F4F6}"
    ".hm-a .ic{flex:0 0 28px;height:28px;border-radius:7px;display:flex;align-items:center;justify-content:center;color:#fff}"
    ".hm-a .ic i{font-size:18px}"
    ".hm-a .x{flex:1;min-width:0}.hm-a .x b{display:block;font-size:12.5px;line-height:16px;color:#111827;font-weight:600;"
    "white-space:nowrap;overflow:hidden;text-overflow:ellipsis}"
    ".hm-a .x span{display:block;font-size:10.5px;line-height:13px;color:#9CA3AF;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}"
    "@media (max-width:640px){.hm-w{height:auto;min-height:0;overflow:visible}.hm-tj{grid-template-columns:repeat(2,minmax(0,1fr))}"
    ".hm-ab{grid-template-columns:minmax(0,1fr);flex:none}.hm-b{overflow:visible}.hm-gr{grid-template-columns:repeat(2,minmax(0,1fr));"
    "grid-auto-rows:%dpx}.hm-t .v{font-size:22px}}"
) % (_CHROME, _FILA_ACCESO_MIN)


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


def _accesos(puede: Callable[[str], bool], navegar: Optional[Callable[[str], Any]]) -> None:
    items = [(sec, key, lbl) for sec, key, lbl in TAB_REGISTRY if key != "home" and puede(key)]
    n = len(items)
    filas2 = -(-n // 2)
    cols = 2 if filas2 * _FILA_ACCESO_MIN <= _ALTO_GRILLA_ACCESOS else (3 if -(-n // 3) * _FILA_ACCESO_MIN <= _ALTO_GRILLA_ACCESOS else 4)
    with ui.element("div").classes("hm-b"):
        ui.html("<h4>Accesos rápidos</h4>")
        with ui.element("div").classes("hm-gr").style(f"--n:{cols}"):
            for sec, key, lbl in items:
                desc = TAB_DESCRIPTIONS.get(key, "")
                desc1 = desc.split(". ")[0].split(" -- ")[0].replace("[EXPERIMENTAL, solo lectura] ", "")
                with ui.element("div").classes("hm-a") as b:
                    ui.html(
                        f'<div class="ic" style="background:{_COL_SECCION.get(sec, "#6B7280")}"><i class="material-icons">'
                        f'{_ICONOS.get(key, "apps")}</i></div><div class="x"><b>{LABEL_BY_TAB.get(key, lbl)}</b>'
                        f'<span>{desc1}</span></div>').style("display:contents")
                if desc:
                    b.tooltip(desc)
                if navegar:
                    b.on("click", lambda _e, k=key: navegar(k))


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
            with ui.element("div").classes("hm-ab"):
                _atencion(d, puede, navegar)
                _accesos(puede, navegar)

    with container:
        _contenido()

        def _tick() -> None:
            if activa is None or activa():
                _contenido.refresh()
        ui.timer(60.0, _tick)
    return _contenido.refresh
