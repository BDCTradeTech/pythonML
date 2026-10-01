"""
tabs/salud_mayorista_fix.py
Botón 🔧 de la celda Mayorista de la tabla de Salud (Diego, 2026-09-29).

Deja el mayorista (PxQ %) de TODAS las publicaciones ACTIVAS de un SKU exactamente como
debe ser, en UNA llamada por publicación (set completo de tiers):
  - gold_special (contado): set objetivo = _qtys_mayorista_para_stock(stock de ESA publicación);
    se borra lo que no está en el objetivo y se crea lo que falta.
  - gold_pro (cuotas x3/x6/x9/x12) con tiers cargados: se borran todos.
  - % de cada tier = el que recomienda ML (de a una cantidad, 1 reintento), directo, sobre el
    precio vigente. Sin recomendación -> escala chica. Coherencia estrictamente creciente.
  - Legacy (montos absolutos): se migran (remove-absolute-pxq, ver _escribir_mayorista_pxq).
El margen es SOLO informativo: nunca bloquea.

Todo el motor sale de salud_audit.py (recomendaciones, precio vigente, cantidades por stock,
firma de tiers) y de tabs/salud.py::_escribir_mayorista_pxq (lectura de versión, POST,
verificación, log en ml_escrituras) -- acá no se duplica esa lógica.

Dos partes:
  1) motivos_mayorista_fix(): condición del ícono, SOLO con datos del snapshot (sin ML).
  2) abrir_dialogo_mayorista(): diálogo con lectura en vivo (solo GET + recommendations) y
     escritura al confirmar.
"""
from __future__ import annotations

import html
from typing import Any, Callable, Dict, List, Optional

from nicegui import background_tasks, run, ui

from ml_api import get_ml_access_token, ml_get_user_id
from salud_audit import _NOTA_INCOHERENTE
from salud_mayorista_motor import (  # noqa: F401 -- el motor vive en salud_mayorista_motor.py (compartido con el cron); se re-exporta acá
    ESCALA_CHICA, _MAX_TIERS, _PCT_TECHO, _SIN_REC_MAX_OK, _fmt_ars, _json_o_vacio, _lista_es, _margen, _tiers_actuales,
    aplicar_publicacion, calcular_pcts_objetivo, efectivo, leer_y_planificar, margen_neg_nuevos, motivos_mayorista_fix,
    planificar_publicacion, preparar_directo,
)

_ML_API = "https://api.mercadolibre.com"
ORIGEN = "salud_boton_mayorista"
ORIGEN_DIRECTO = "salud_boton_mayorista_directo"  # 🔧 sin diálogo
_GREY, _OK, _BAD, _MID = "#6B7280", "#2E7D32", "#A32D2D", "#B26A00"


# ---------------------------------------------------------------------------
# 1) Ícono -- se calcula al renderizar con datos del snapshot, SIN llamar a ML
# ---------------------------------------------------------------------------

def render_iconos(mayfix: Optional[Dict[str, Any]], on_click: Callable[[], Any], en_curso: bool = False,
                  refs: Optional[Dict[str, Any]] = None, on_abrir: Optional[Callable[[], Any]] = None) -> None:
    """Íconos de la celda Mayorista (se llama dentro del contenedor de la celda). La 🔧 dispara el modo
    DIRECTO (`on_click`); mientras corre se reemplaza por un spinner (`en_curso`) y no admite otro click.
    `refs` recibe {"icono", "spinner"} para poder alternarlos sin re-renderizar la tabla (marcar_en_curso)."""
    if not mayfix:
        return
    inco = mayfix.get("incoherentes") or []
    if mayfix.get("motivos") or inco:
        if mayfix.get("motivos"):
            color, tip = _MID, "Arreglar mayorista automáticamente\n" + " · ".join(mayfix["motivos"])
        else:  # lo único pendiente son cantidades que ML no admite hoy: 🔧 verde
            color = _OK
            tip = (f"Correcto dentro de lo que ML permite hoy. ML no admite: {_lista_es(inco)} "
                   f"{'unidad' if inco == [1] else 'unidades'}. Se reevalúa cada noche.")
        b = ui.icon("build", size="16px").classes("cursor-pointer").style(f"color:{color}")
        with b:
            ui.tooltip(tip).style("white-space: pre-line")
        b.on("click", lambda: on_click())
        sp = ui.spinner(size="16px", color="orange")
        b.set_visibility(not en_curso)
        sp.set_visibility(en_curso)
        if refs is not None:
            refs["icono"], refs["spinner"] = b, sp
    if mayfix.get("margen_neg"):
        m = ui.icon("trending_down", size="16px").style(f"color:{_BAD}")
        m.tooltip("Margen negativo en: " + ", ".join(mayfix["margen_neg"]) + " (informativo)")
        if on_abrir:  # 📉 sin 🔧: también abre el diálogo (mismo que el número)
            m.classes("cursor-pointer")
            m.on("click", lambda: on_abrir())


def marcar_en_curso(refs: Optional[Dict[str, Any]], activo: bool) -> None:
    """Alterna 🔧 <-> spinner de una fila (si la tabla se re-renderizó y el elemento ya no existe, no hace nada)."""
    try:
        if refs and refs.get("icono") is not None:
            refs["icono"].set_visibility(not activo)
            refs["spinner"].set_visibility(activo)
    except Exception:  # noqa: BLE001 -- cosmético
        pass


# ---------------------------------------------------------------------------
# 2) Plan de cada publicación (lectura en vivo, sin escribir)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# 3) Escritura (al confirmar) -- una publicación a la vez, independientes
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# 3b) Modo DIRECTO (🔧 sin diálogo): mismo motor y mismas protecciones que el diálogo
# ---------------------------------------------------------------------------

_AMBAR_SOBRE_ROJO = "#FFD54F"


def _notificacion_directo_base(planes: List[Dict[str, Any]], resultados: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """kwargs de ui.notify del modo directo. Hubo errores -> rojo fija con el detalle (más una línea ámbar si
    hubo salteadas por margen). Sin errores pero con salteadas por margen -> ÁMBAR fija con botón cerrar.
    Todo bien -> verde que se cierra sola (~6 s)."""
    salt = [p for p in planes if p.get("salteada_margen")]
    base = armar_notificacion([p for p in planes if not p.get("salteada_margen")], resultados)
    if not salt:
        if base["type"] == "positive" and not any(r["ok"] for r in resultados.values()):
            base["message"] = base["message"].replace("Mayorista actualizado en 0 publicaciones", "Mayorista: nada que cambiar")
        return base
    n = len(salt)
    linea = f"{n} publicaci{'ón no se tocó' if n == 1 else 'ones no se tocaron'} por margen negativo — tocá el número para decidir"
    detalle = [f"• {_etiqueta_plan(p)}" for p in salt]
    if base["type"] == "negative":
        base["message"] += "<br>" + f'<span style="color:{_AMBAR_SOBRE_ROJO};font-weight:600">' + "<br>".join(html.escape(t) for t in [linea] + detalle) + "</span>"
        return base
    hechas = sum(1 for r in resultados.values() if r["ok"])
    if hechas:
        cab = base["message"]
    else:
        cab = "Mayorista: no se escribió nada"
    return {"message": html.escape(cab) + "<br>" + "<br>".join(html.escape(t) for t in [linea] + detalle),
            "type": "warning", "position": "bottom", "timeout": 0, "close_button": "Cerrar", "multi_line": True, "html": True}


def armar_notificacion_directo(planes: List[Dict[str, Any]], resultados: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Como _notificacion_directo_base, más las publicaciones SIN COSTO cargado (margen desconocido): ahí no se crea
    ningún tier ni se sube el % de descuento (baja el precio sin saber el margen); se avisa en ámbar."""
    base = _notificacion_directo_base(planes, resultados)
    sc = [p for p in planes if p.get("sin_costo")]
    if not sc:
        return base
    n = len(sc)
    linea = f"{n} publicaci{'ón' if n == 1 else 'ones'} sin costo cargado: no se creó ni se subió el % de descuento (sin margen no se baja el precio)"
    detalle = [f"• {_etiqueta_plan(p)}" for p in sc]
    if base["type"] == "negative":
        base["message"] += "<br>" + f'<span style="color:{_AMBAR_SOBRE_ROJO};font-weight:600">' + "<br>".join(html.escape(t) for t in [linea] + detalle) + "</span>"
        return base
    if base["type"] == "positive":
        base["message"] = html.escape(base["message"])
    base["message"] += "<br>" + "<br>".join(html.escape(t) for t in [linea] + detalle)
    base.update({"type": "warning", "timeout": 0, "close_button": "Cerrar", "multi_line": True, "html": True})
    return base


async def _refrescar_fila(uid: int, sku: str, token: str, al_cerrar: Callable[[Dict[str, Any]], None]) -> None:
    """audit_sku + refresco de la fila, en segundo plano (mismo contrato que el diálogo)."""
    from salud_audit import audit_sku
    try:
        seller_id = await run.io_bound(ml_get_user_id, token)
        resultado = await run.io_bound(audit_sku, uid, seller_id or "", sku, True)
        if resultado and not resultado.get("error"):
            al_cerrar(resultado)
    except Exception as e:  # noqa: BLE001 -- el refresco es cosmético, no debe romper nada
        ui.notify(f"No se pudo refrescar la fila de {sku}: {e}", type="warning", position="bottom")


async def ejecutar_mayorista_directo(uid: int, sku: str, desde_fecha: Optional[str],
                                     al_cerrar: Callable[[Dict[str, Any]], None]) -> None:
    """🔧 en modo directo: lectura en vivo + validación de incoherentes en lote (leer_y_planificar), aplica la
    propuesta completa a cada publicación (relectura de /prices y salteo si cambió, un POST, verificación,
    ml_escrituras con origen 'salud_boton_mayorista_directo') EXCEPTO las que tendrían un tier nuevo con margen
    negativo, que no se tocan y se reportan aparte. Al terminar: notificación y refresco en segundo plano."""
    from tabs.salud_reg import _items_del_sku
    token = get_ml_access_token(uid)
    if not token:
        ui.notify("Mayorista: no hay token de ML para esta cuenta.", type="negative", position="bottom")
        return
    try:
        items = await run.io_bound(_items_del_sku, uid, sku, desde_fecha)
        lectura = await run.io_bound(leer_y_planificar, token, uid, sku, [i["item_id"] for i in items])
    except Exception as e:  # noqa: BLE001
        ui.notify(f"Mayorista {sku}: no se pudo leer ML ({e}). No se escribió nada.", type="negative", position="bottom", timeout=0, close_button="Cerrar")
        return
    if lectura.get("error"):
        ui.notify(f"Mayorista {sku}: {lectura['error']}. No se escribió nada.", type="negative", position="bottom", timeout=0, close_button="Cerrar")
        return
    planes = lectura["planes"]
    preparar_directo(planes)
    resultados: Dict[str, Dict[str, Any]] = {}
    pendientes = [p for p in planes if efectivo(p) is not None]
    try:
        for p in pendientes:
            resultados[p["item_id"]] = await run.io_bound(aplicar_publicacion, token, uid, sku, p, ORIGEN_DIRECTO)
    except Exception as e:  # noqa: BLE001 -- una excepción inesperada no puede dejar la fila colgada
        for p in pendientes:
            resultados.setdefault(p["item_id"], {"ok": False, "cambio": False, "msg": f"error inesperado: {e}"})
    ui.notify(**armar_notificacion_directo(planes, resultados))
    background_tasks.create(_refrescar_fila(uid, sku, token, al_cerrar))


# ---------------------------------------------------------------------------
# 4) Diálogo
# ---------------------------------------------------------------------------

_OPCIONES = {"aplicar": "Aplicar propuesta", "quitar": "Quitar mayorista", "no_tocar": "No tocar"}


def _render_plan(plan: Dict[str, Any], resultado: Optional[Dict[str, Any]],
                 on_opcion: Optional[Callable[[], Any]] = None, bloqueado: bool = False) -> None:
    tipo_txt = "cuotas" if plan["tipo"] == "cuotas" else "contado"
    prop_txt = "catálogo" if plan["catalogo"] else "propia"
    with ui.card().classes("w-full gap-1 p-2").props("flat bordered"):
        with ui.row().classes("items-center gap-2 w-full"):
            ui.label(plan["item_id"]).classes("font-semibold text-sm")
            ui.badge(prop_txt, color="grey").props("outline")
            ui.badge(tipo_txt, color="orange" if plan["tipo"] == "cuotas" else "blue").props("outline")
            ui.label(f"stock {plan['stock']}").classes("text-xs text-gray-600")
            if plan.get("precio_vigente"):
                promo = f" · promo activa (lista {_fmt_ars(plan['precio_base'])})" if plan["promo"] else ""
                ui.label(f"precio vigente {_fmt_ars(plan['precio_vigente'])}{promo}").classes("text-xs text-gray-600")
            ui.space()
            if resultado is not None:
                col = _OK if resultado["ok"] else _BAD
                ui.label(("✅ " if resultado["ok"] else "❌ ") + resultado["msg"]).classes("text-xs font-semibold").style(f"color:{col}")
        if plan["error"]:
            ui.label(f"⚠ {plan['error']}").classes("text-xs").style(f"color:{_BAD}")
            return
        if plan.get("incoherentes"):
            ui.label("Cantidades descartadas: " + _lista_es(plan["incoherentes"]) + " — " + _NOTA_INCOHERENTE).classes("text-xs").style(f"color:{_MID}")
        if plan.get("aviso"):
            ui.label(f"⚠ {plan['aviso']}").classes("text-xs font-semibold").style(f"color:{_MID}")
        puede_quitar = plan["tipo"] != "cuotas" and bool(plan.get("qtys_actuales")) and resultado is None
        if not plan["hay_cambios"]:
            ui.label("Sin cambios, no se escribe").classes("text-xs text-gray-500")
        if (plan["hay_cambios"] and plan["hay_margen_neg"] and resultado is None) or puede_quitar:
            with ui.row().classes("items-center gap-2"):
                if plan["hay_cambios"] and plan["hay_margen_neg"]:
                    ui.icon("trending_down", size="16px").style(f"color:{_BAD}")
                    ui.label("Algún tier nuevo da margen negativo (informativo, no bloquea):").classes("text-xs").style(f"color:{_BAD}")
                opciones = _OPCIONES if plan["hay_cambios"] else {k: v for k, v in _OPCIONES.items() if k != "aplicar"}
                sel = ui.toggle(opciones, value=plan["opcion"]).props("dense size=sm no-caps")
                if bloqueado:
                    sel.props("disable")

                def _cambio(e, plan=plan):
                    plan["opcion"] = e.value
                    if on_opcion:
                        on_opcion()
                sel.on_value_change(_cambio)
            if plan["opcion"] == "quitar":
                ui.label("Se quitan TODOS los tiers de esta publicación (set vacío)").classes("text-xs font-semibold").style(f"color:{_MID}")
                return
            if plan["opcion"] == "no_tocar":
                ui.label("No se toca esta publicación").classes("text-xs text-gray-500")
                return
        if not plan["hay_cambios"]:
            return
        if not plan["filas"] or all(f["accion"] == "borra" for f in plan["filas"]):
            ui.label("Se borran TODOS los tiers (set vacío)" + (" — cuotas no lleva mayorista" if plan["tipo"] == "cuotas" else " — sin stock")).classes("text-xs font-semibold").style(f"color:{_MID}")
        cols = [
            {"name": "q", "label": "Cant.", "field": "q", "align": "center"},
            {"name": "accion", "label": "Acción", "field": "accion", "align": "left"},
            {"name": "pa", "label": "% actual", "field": "pa", "align": "right"},
            {"name": "pn", "label": "% nuevo", "field": "pn", "align": "right"},
            {"name": "precio", "label": "Precio unit.", "field": "precio", "align": "right"},
            {"name": "margen", "label": "Margen $ / %", "field": "margen", "align": "right"},
            {"name": "nota", "label": "Nota", "field": "nota", "align": "left"},
        ]
        etiqueta = {"borra": "se borra", "crea": "se crea", "cambia": "se corrige", "migra": "migra (legacy → %)", "igual": "igual", "omite": "se omite"}
        rows = []
        for f in plan["filas"]:
            marg = "—"
            if f["margen"] is not None:
                marg = f"{_fmt_ars(f['margen'])} / {f['margen_pct']}%" + (" ⚠ NEGATIVO" if f["margen_neg"] else "")
            rows.append({
                "q": f"{f['q']}+", "accion": etiqueta[f["accion"]],
                "pa": "—" if f["pct_actual"] is None else f"{f['pct_actual']:.2f}%" + (" (abs.)" if f["legacy"] else ""),
                "pn": "—" if f["pct_nuevo"] is None else f"{f['pct_nuevo']:.2f}%",
                "precio": _fmt_ars(f["precio_nuevo"]), "margen": marg, "nota": f["nota"] or "",
            })
        ui.table(columns=cols, rows=rows, row_key="q").props("dense flat hide-bottom").classes("w-full text-xs")


def _etiqueta_plan(plan: Dict[str, Any]) -> str:
    return f"{plan['item_id']} · {'catálogo' if plan['catalogo'] else 'propia'} · {'cuotas' if plan['tipo'] == 'cuotas' else 'contado'}"


def armar_notificacion(planes: List[Dict[str, Any]], resultados: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Arma la notificación final del diálogo. Devuelve los kwargs de ui.notify:
    todo OK -> verde, se cierra sola (~6 s); cualquier error o publicación salteada por
    'cambió mientras mirabas' -> rojo, con el detalle de cada una, NO se cierra sola."""
    ok = [p for p in planes if resultados.get(p["item_id"], {}).get("ok")]
    fallas = [(p, resultados[p["item_id"]]) for p in planes if p["item_id"] in resultados and not resultados[p["item_id"]]["ok"]]
    sin_cambios = [p for p in planes if p["item_id"] not in resultados and not p["error"]]
    no_evaluables = [p for p in planes if p["item_id"] not in resultados and p["error"]]
    extra = []
    if sin_cambios:
        extra.append(f"{len(sin_cambios)} sin cambios o sin mayorista")
    if no_evaluables:
        extra.append(f"{len(no_evaluables)} no evaluable(s) (" + "; ".join(f"{_etiqueta_plan(p)}: {p['error']}" for p in no_evaluables) + ")")

    def plural(n: int) -> str:
        return f"{n} publicaci{'ón' if n == 1 else 'ones'}"

    if not fallas:
        msg = f"Mayorista actualizado en {plural(len(ok))}" + (" — " + " · ".join(extra) if extra else "")
        return {"message": msg, "type": "positive", "position": "bottom", "timeout": 6000}
    lineas = [f"Mayorista: {plural(len(fallas))} con problemas, {len(ok)} salieron bien."]
    for p, r in fallas:
        motivo = ("Salteada: " + r["msg"]) if r.get("cambio") else r["msg"]
        lineas.append(f"• {_etiqueta_plan(p)} — {motivo}")
    if extra:
        lineas.append(" · ".join(extra))
    cuerpo = "<br>".join(html.escape(l) for l in lineas)
    return {"message": cuerpo, "type": "negative", "position": "bottom", "timeout": 0, "close_button": "Cerrar",
            "multi_line": True, "html": True}


async def abrir_dialogo_mayorista(uid: int, sku: str, producto: str, desde_fecha: Optional[str],
                                  al_cerrar: Callable[[Dict[str, Any]], None]) -> None:
    """Abre el diálogo del 🔧 para `sku`. `al_cerrar(resultado_audit)` se llama con el diálogo ya
    cerrado y solo si se escribió algo (mismo contrato que abrir_popup_reg)."""
    from tabs.salud_reg import _items_del_sku  # import diferido, mismo patrón que abrir_popup_reg
    from salud_audit import audit_sku

    estado: Dict[str, Any] = {"escribio": False, "leyo_ok": False, "planes": [], "resultados": {}, "aplicando": False,
                              "progreso": (0, 0), "cuotas_abierto": False}
    with ui.dialog().props("persistent") as dlg, ui.card().classes("w-[980px] max-w-full gap-2"):
        dlg.open()
        with ui.row().classes("items-center gap-2 w-full"):
            ui.label(f"🔧 Mayorista — {sku}").classes("text-lg font-bold")
            ui.label(producto or "").classes("text-xs text-gray-500")
        info = ui.column().classes("w-full gap-1")
        body = ui.column().classes("w-full gap-2")
        with body:
            ui.spinner(size="md")
            ui.label("Leyendo en vivo las publicaciones activas (solo lectura)…").classes("text-xs text-gray-500")
        with ui.row().classes("justify-end gap-2 w-full"):
            btn_aplicar = ui.button("Aplicar").props("color=primary")
            btn_aplicar.set_visibility(False)
            btn_cerrar = ui.button("Cancelar").props("flat")

    token = get_ml_access_token(uid)
    if not token:
        body.clear()
        with body:
            ui.label("No se pudo obtener el token de MercadoLibre.").classes("text-negative text-sm")
        btn_cerrar.on_click(dlg.close)
        return

    def _pintar() -> None:
        body.clear()
        info.clear()
        planes = estado["planes"]
        with info:
            if estado.get("ignoradas"):
                ui.label(f"{estado['ignoradas']} publicación(es) pausadas/cerradas/otro tipo se ignoran.").classes("text-xs text-gray-500")
        with body:
            if not planes:
                ui.label("Este SKU no tiene publicaciones activas gold_special / gold_pro.").classes("text-sm text-gray-500")
            elif not any(p["hay_cambios"] for p in planes) and not any(p["error"] or p.get("aviso") for p in planes):
                ui.label("✅ Ya está correcto: no hay nada que cambiar.").classes("text-sm font-semibold").style(f"color:{_OK}")
            bloq = estado["aplicando"]
            for p in [p for p in planes if p["tipo"] != "cuotas"]:
                _render_plan(p, estado["resultados"].get(p["item_id"]), _pintar, bloq)
            cuotas = [p for p in planes if p["tipo"] == "cuotas"]
            if cuotas:
                # colapsado por default; NO cambia qué se aplica (efectivo() mira todos los planes)
                m = sum(1 for p in cuotas if efectivo(p) is not None or estado["resultados"].get(p["item_id"], {}).get("ok"))
                resumen = f"se quita mayorista en {m}" if m else "sin cambios"
                if any(p["error"] for p in cuotas):
                    resumen += f" · {sum(1 for p in cuotas if p['error'])} con error"
                with ui.expansion(f"Cuotas ({len(cuotas)} publicaci{'ón' if len(cuotas) == 1 else 'ones'}) — {resumen}",
                                  icon="credit_card", value=estado["cuotas_abierto"]).classes("w-full border rounded") as exp:
                    exp.on_value_change(lambda e: estado.__setitem__("cuotas_abierto", e.value))
                    for p in cuotas:
                        _render_plan(p, estado["resultados"].get(p["item_id"]), _pintar, bloq)
        if estado["aplicando"]:
            hechas, total = estado["progreso"]
            with info:
                with ui.row().classes("items-center gap-2"):
                    ui.spinner(size="sm")
                    ui.label(f"Aplicando… ({hechas} de {total}) — no cierres esta ventana").classes("text-sm font-semibold")
        pendientes = [p for p in planes if efectivo(p) is not None and p["item_id"] not in estado["resultados"]]
        con_selector = [p for p in planes if not p["error"] and p["tipo"] != "cuotas" and (p["hay_cambios"] or p["qtys_actuales"])]
        if con_selector:
            cnt = {k: sum(1 for p in con_selector if p["opcion"] == k) for k in _OPCIONES}
            with info:
                ui.label("Resumen: " + " · ".join(f"{cnt[k]} {_OPCIONES[k].lower()}" for k in _OPCIONES)).classes("text-sm font-semibold")
        btn_aplicar.set_visibility(bool(pendientes) or estado["aplicando"])
        btn_aplicar.set_text(f"Aplicar a {len(pendientes)} publicaci{'ón' if len(pendientes) == 1 else 'ones'}")
        btn_cerrar.set_text("Cancelar" if pendientes else "Cerrar")

    async def _cargar() -> None:
        items = await run.io_bound(_items_del_sku, uid, sku, desde_fecha)
        lectura = await run.io_bound(leer_y_planificar, token, uid, sku, [i["item_id"] for i in items])
        if lectura.get("error"):
            body.clear()
            with body:
                ui.label(lectura["error"]).classes("text-negative text-sm")
            return
        estado["leyo_ok"] = True  # lectura exitosa: al cerrar se refresca el snapshot del SKU aunque no se escriba
        estado["planes"] = lectura["planes"]
        estado["ignoradas"] = lectura.get("ignoradas", 0)
        estado["resultados"] = {}
        _pintar()

    def _refrescar_en_segundo_plano() -> None:
        """audit_sku + refresco de la fila, sin demorar la notificación ni el cierre del diálogo."""
        async def _tarea() -> None:
            try:
                seller_id = await run.io_bound(ml_get_user_id, token)
                resultado = await run.io_bound(audit_sku, uid, seller_id or "", sku, True)
                if not resultado.get("error"):
                    al_cerrar(resultado)
            except Exception as e:  # noqa: BLE001 -- el refresco es cosmético, no debe romper nada
                ui.notify(f"No se pudo refrescar la fila de {sku}: {e}", type="warning", position="bottom")
        background_tasks.create(_tarea())

    async def _aplicar() -> None:
        if estado["aplicando"]:
            return  # ya está escribiendo: ignorar un segundo click
        pendientes = [p for p in estado["planes"] if efectivo(p) is not None and p["item_id"] not in estado["resultados"]]
        if not pendientes:
            return
        estado["aplicando"] = True
        estado["progreso"] = (0, len(pendientes))
        btn_aplicar.props("loading disable")
        btn_cerrar.props("disable")
        _pintar()
        try:
            for n, p in enumerate(pendientes):
                res = await run.io_bound(aplicar_publicacion, token, uid, sku, p)
                estado["resultados"][p["item_id"]] = res
                if res["ok"]:
                    estado["escribio"] = True
                estado["progreso"] = (n + 1, len(pendientes))
                _pintar()
        except Exception as e:  # noqa: BLE001 -- una excepción inesperada no puede dejar la ventana colgada
            for p in pendientes:
                estado["resultados"].setdefault(p["item_id"], {"ok": False, "cambio": False, "msg": f"error inesperado: {e}"})
        finally:
            estado["aplicando"] = False
        notif = armar_notificacion(estado["planes"], estado["resultados"])
        dlg.close()
        ui.notify(**notif)
        if estado["escribio"] or estado["leyo_ok"]:
            _refrescar_en_segundo_plano()

    async def _cerrar() -> None:
        dlg.close()
        if estado["escribio"] or estado["leyo_ok"]:
            _refrescar_en_segundo_plano()

    btn_aplicar.on_click(_aplicar)
    btn_cerrar.on_click(_cerrar)
    await _cargar()
