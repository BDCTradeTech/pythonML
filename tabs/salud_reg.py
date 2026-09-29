"""Popup de Regulaciones / Homologaciones (columna "Reg." de la tabla de Salud).

ML no marca estos atributos con ningún tag ("regulatory" no existe; todos viven en el grupo
genérico OTHERS, verificado en vivo 2026-09-29 sobre las 42 categorías de user 1), así que la
lista de attr_id es propia y vive SOLO acá (REGULATORIOS_ATTR_IDS). tabs/salud.py la usa para
sacarlos de "Caracterist." y para armar la celda Reg.

Escritura: se reusa salud._escribir_atributo (PUT + relectura de verificación + fila en
ml_escrituras, origen "salud_popup"). Solo se escribe sobre publicaciones PROPIAS: en catálogo
ML devuelve 200 en el PUT pero no lo aplica (confirmado 2026-09-07, ver
salud._clasificar_hallazgos), así que catálogo es informativo. Alcance por user_id: las
publicaciones salen del snapshot con WHERE user_id=? y el token de get_ml_access_token(uid);
el popup nunca recibe item_id desde el cliente.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests
from nicegui import run, ui

from db import get_connection
from ml_api import get_ml_access_token, ml_get_items_multiget_with_attributes, ml_get_user_id

# Única fuente de la lista regulatoria (orden = orden de aparición en el popup).
REGULATORIOS_ATTR_IDS: Tuple[str, ...] = (
    "TELECOMMUNICATION_HOMOLOGATION_NUMBER",
    "OCP_CERTIFICATION_AGENCY",
    "TOY_SAFETY_CERTIFICATE_NUMBER",
    "ELECTRICAL_SAFETY_CERTIFICATE_NUMBER",
    "SAFETY_CERTIFICATE_NUMBER",
)
REGULATORIOS_SET = frozenset(REGULATORIOS_ATTR_IDS)

_OK = "#2E7D32"
_MID = "#BA7517"
_BAD = "#A32D2D"
_GREY = "#9CA3AF"
_ML_API = "https://api.mercadolibre.com"
_MAX_LEN = 255


def es_regulatorio(attr_id: Optional[str]) -> bool:
    return attr_id in REGULATORIOS_SET


def _norm(s: Optional[str]) -> str:
    return " ".join((s or "").split()).casefold()


# ---------------------------------------------------------------------------
# Lectura (bloqueante -- se llama con run.io_bound). SOLO GET.
# ---------------------------------------------------------------------------

def _items_del_sku(uid: int, sku: str, desde_fecha: Optional[str]) -> List[dict]:
    """Publicaciones del SKU según el snapshot (el más nuevo por item_id, desde la misma
    fecha base que arma la tabla). Siempre filtrado por user_id."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT item_id, catalog_listing FROM salud_item_snapshots "
            "WHERE user_id=? AND sku=? AND snapshot_date>=? ORDER BY snapshot_date ASC",
            (uid, sku, desde_fecha or "0000-00-00"),
        ).fetchall()
    finally:
        conn.close()
    por_id: Dict[str, dict] = {}
    for r in rows:
        por_id[r["item_id"]] = {"item_id": r["item_id"], "catalog_listing": bool(r["catalog_listing"])}
    return list(por_id.values())


def _defs_categoria(cat_id: str) -> Dict[str, dict]:
    try:
        r = requests.get(f"{_ML_API}/categories/{cat_id}/attributes", timeout=15)
        return {a["id"]: a for a in r.json() if a.get("id")} if r.status_code == 200 else {}
    except (requests.exceptions.RequestException, ValueError):
        return {}


def leer_estado(token: str, items: List[dict]) -> Dict[str, Any]:
    """Estado en vivo de los atributos regulatorios del SKU. Devuelve
    {"attrs": {attr_id: {...}}, "n_propias": int, "n_catalogo": int, "error": str|None}."""
    ids = [it["item_id"] for it in items]
    bodies: Dict[str, dict] = {}
    for i in range(0, len(ids), 20):
        for b in ml_get_items_multiget_with_attributes(
                token, ids[i:i + 20], "id,category_id,catalog_listing,condition,status,attributes"):
            if b and b.get("id"):
                bodies[b["id"]] = b
    if not bodies:
        return {"attrs": {}, "n_propias": 0, "n_catalogo": 0, "error": "No se pudieron leer las publicaciones en ML."}

    defs_por_cat: Dict[str, Dict[str, dict]] = {}
    attrs: Dict[str, dict] = {}
    for iid, b in bodies.items():
        cat_id = b.get("category_id")
        if cat_id not in defs_por_cat:
            defs_por_cat[cat_id] = _defs_categoria(cat_id) if cat_id else {}
        defs = defs_por_cat[cat_id]
        cond_hidden = {"new": "new_hidden", "used": "used_hidden"}.get((b.get("condition") or "").lower())
        actuales = {a.get("id"): a for a in b.get("attributes") or [] if a.get("id")}
        for aid in REGULATORIOS_ATTR_IDS:
            d = defs.get(aid)
            tags = (d or {}).get("tags") or {}
            if not d or tags.get("hidden") or (cond_hidden and tags.get(cond_hidden)):
                continue
            e = attrs.setdefault(aid, {
                "id": aid, "name": d.get("name") or aid, "value_type": d.get("value_type"),
                "values": d.get("values") or [], "read_only": bool(tags.get("read_only")),
                "propias": [], "catalogo": [],
            })
            a = actuales.get(aid) or {}
            valor = a.get("value_name") or None
            (e["catalogo"] if b.get("catalog_listing") else e["propias"]).append(
                {"item_id": iid, "valor": valor, "value_id": a.get("value_id")})
    return {
        "attrs": attrs,
        "n_propias": sum(1 for b in bodies.values() if not b.get("catalog_listing")),
        "n_catalogo": sum(1 for b in bodies.values() if b.get("catalog_listing")),
        "error": None,
    }


def _distintos(pubs: List[dict]) -> Dict[str, List[str]]:
    """{valor: [item_ids]} de las publicaciones que tienen valor."""
    d: Dict[str, List[str]] = defaultdict(list)
    for p in pubs:
        if p["valor"]:
            d[p["valor"]].append(p["item_id"])
    return dict(d)


def pendientes_de_escritura(attr: dict, nuevo: str) -> Tuple[List[dict], List[dict]]:
    """(a_escribir, a_pisar): propias cuyo valor actual difiere del nuevo, y de ellas las que
    ya tenían un valor distinto (esas exigen confirmación explícita antes de pisar)."""
    a_escribir = [p for p in attr["propias"] if _norm(p["valor"]) != _norm(nuevo)]
    return a_escribir, [p for p in a_escribir if p["valor"]]


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

def construir_cuerpo(body, estado: Dict[str, Any], on_aplicar: Callable, resultados: Dict[str, List[Tuple[str, Optional[str]]]]) -> None:
    """Dibuja una tarjeta por atributo regulatorio aplicable. Separada de abrir_popup_reg para
    poder probarla sin event loop. `on_aplicar(attr, valor_nuevo, payload, mostrado)` es
    async y lo dispara el botón."""
    body.clear()
    with body:
        if estado.get("error"):
            ui.label(estado["error"]).classes("text-negative text-sm")
            return
        if not estado["attrs"]:
            ui.label("Ninguna publicación de este SKU tiene atributos regulatorios o de homologación "
                     "en su categoría.").classes("text-sm text-gray-500")
            return
        for aid in REGULATORIOS_ATTR_IDS:
            attr = estado["attrs"].get(aid)
            if not attr:
                continue
            _tarjeta(attr, on_aplicar, resultados.get(aid))


def _tarjeta(attr: dict, on_aplicar: Callable, resultado: Optional[List[Tuple[str, Optional[str]]]]) -> None:
    propias, catalogo = attr["propias"], attr["catalogo"]
    dist_prop = _distintos(propias)
    dist_cat = _distintos(catalogo)
    con_prop = sum(1 for p in propias if p["valor"])
    con_cat = sum(1 for p in catalogo if p["valor"])

    with ui.card().classes("w-full gap-1").style("border:1px solid #e0e0e0;padding:10px"):
        with ui.row().classes("items-baseline gap-2"):
            ui.label(attr["name"]).classes("font-semibold text-sm")
            ui.label(attr["id"]).classes("text-[10px]").style(f"color:{_GREY}")

        # --- propias (accionable) ---
        with ui.row().classes("items-center gap-1"):
            ui.icon("person", size="12px").style(f"color:{_GREY}")
            if not propias:
                ui.label("Sin publicaciones propias con este atributo.").classes("text-xs text-gray-500")
            else:
                color = _OK if con_prop == len(propias) else (_BAD if con_prop == 0 else _MID)
                ui.label(f"Propias: {con_prop} de {len(propias)} con valor").classes("text-xs font-semibold").style(f"color:{color}")
        for valor, its in dist_prop.items():
            ui.label(f"• {valor} — {len(its)} publicación(es)").classes("text-xs ml-4")
        if len(dist_prop) > 1:
            ui.label("⚠ Valores distintos entre tus publicaciones: revisá cuál es el correcto antes de aplicar.").classes(
                "text-xs").style(f"color:{_MID}")

        # --- catálogo (informativo) ---
        if catalogo:
            with ui.row().classes("items-center gap-1"):
                ui.icon("storefront", size="12px").style(f"color:{_OK}")
                ui.label(f"Catálogo: {con_cat} de {len(catalogo)} con valor (informativo, depende de ML, no accionable)").classes(
                    "text-xs").style(f"color:{_OK}")
            for valor, its in dist_cat.items():
                ui.label(f"• {valor} — {len(its)} publicación(es)").classes("text-xs ml-4 text-gray-600")

        if attr["read_only"]:
            ui.label("ML marca este atributo como solo lectura en la categoría: no se puede cargar por API.").classes(
                "text-xs").style(f"color:{_GREY}")
        elif propias:
            _editor(attr, dist_prop, on_aplicar)

        if resultado:
            for iid, err in resultado:
                if err:
                    ui.label(f"❌ {iid}: {err}").classes("text-xs").style(f"color:{_BAD}")
                else:
                    ui.label(f"✅ {iid}: guardado y verificado").classes("text-xs").style(f"color:{_OK}")


def _editor(attr: dict, dist_prop: Dict[str, List[str]], on_aplicar: Callable) -> None:
    es_lista = attr["value_type"] == "list" and attr["values"]
    unico = next(iter(dist_prop)) if len(dist_prop) == 1 else ""
    with ui.row().classes("items-center gap-2 w-full no-wrap"):
        if es_lista:
            opciones = {str(v["id"]): v["name"] for v in attr["values"] if v.get("id") and v.get("name")}
            previo = next((k for k, n in opciones.items() if _norm(n) == _norm(unico)), None)
            campo = ui.select(opciones, value=previo, label="Valor a aplicar", with_input=True).props("dense outlined").classes("flex-1")
        else:
            campo = ui.input(label="Valor a aplicar", value=unico).props(f"dense outlined maxlength={_MAX_LEN}").classes("flex-1")
        boton = ui.button("Aplicar").props("color=primary dense")
    aviso = ui.label("").classes("text-xs")
    confirmar = ui.checkbox("Confirmo pisar el valor que ya tienen algunas publicaciones").classes("text-xs")
    confirmar.set_visibility(False)

    def _valor_mostrado() -> str:
        if es_lista:
            return opciones.get(str(campo.value), "") if campo.value else ""
        return " ".join((campo.value or "").split())

    def _refrescar(*_a) -> None:
        nuevo = _valor_mostrado()
        if not nuevo:
            aviso.set_text("Ingresá un valor.")
            boton.set_text("Aplicar")
            boton.props("disable")
            confirmar.set_visibility(False)
            return
        if len(nuevo) > _MAX_LEN:
            aviso.set_text(f"Máximo {_MAX_LEN} caracteres.")
            boton.props("disable")
            return
        a_escribir, a_pisar = pendientes_de_escritura(attr, nuevo)
        confirmar.set_visibility(bool(a_pisar))
        if not a_escribir:
            aviso.set_text("Todas tus publicaciones ya tienen este valor.")
            boton.set_text("Aplicar")
            boton.props("disable")
            return
        aviso.set_text(f"Se escribirá en {len(a_escribir)} publicación(es) propia(s)"
                       + (f"; {len(a_pisar)} ya tienen otro valor y se pisará." if a_pisar else "."))
        boton.set_text(f"Aplicar a {len(a_escribir)} propia(s)")
        if a_pisar and not confirmar.value:
            boton.props("disable")
        else:
            boton.props(remove="disable")

    async def _click() -> None:
        nuevo = _valor_mostrado()
        if not nuevo or len(nuevo) > _MAX_LEN:
            return
        a_escribir, a_pisar = pendientes_de_escritura(attr, nuevo)
        if a_pisar and not confirmar.value:
            return
        payload = ({"id": attr["id"], "value_id": str(campo.value)} if es_lista
                   else {"id": attr["id"], "value_name": nuevo})
        boton.props("loading disable")
        await on_aplicar(attr, a_escribir, payload, nuevo)

    campo.on_value_change(_refrescar)
    confirmar.on_value_change(_refrescar)
    boton.on_click(_click)
    _refrescar()


async def abrir_popup_reg(uid: int, sku: str, producto: str, desde_fecha: Optional[str],
                          al_cerrar: Callable[[Dict[str, Any]], None]) -> None:
    """Abre el popup de Reg. para `sku`. `al_cerrar(resultado_audit)` se llama recién con el
    diálogo ya cerrado (nunca mientras está abierto: ver el bug de auto-cierre de salud.py,
    _render() limpia table_container) y solo si se escribió algo."""
    from tabs.salud import _escribir_atributo  # import diferido: salud.py importa este módulo
    from salud_audit import audit_sku

    estado_ui: Dict[str, Any] = {"escribio": False, "estado": None, "resultados": {}}
    with ui.dialog().props("persistent") as dlg, ui.card().classes("w-[720px] max-w-full gap-2"):
        dlg.open()
        with ui.row().classes("items-center gap-2 w-full"):
            ui.label(f"Regulaciones — {sku}").classes("text-lg font-bold")
            ui.label(producto or "").classes("text-xs text-gray-500")
        body = ui.column().classes("w-full gap-2")
        with body:
            ui.spinner(size="md")
        with ui.row().classes("justify-end gap-2 w-full") as pie:
            btn_releer = ui.button("Releer", icon="refresh").props("flat dense")
            btn_cerrar = ui.button("Cerrar").props("color=primary dense")

    token = get_ml_access_token(uid)
    if not token:
        body.clear()
        with body:
            ui.label("No se pudo obtener el token de MercadoLibre.").classes("text-negative text-sm")
        btn_releer.set_visibility(False)
        btn_cerrar.on_click(dlg.close)
        return

    items = await run.io_bound(_items_del_sku, uid, sku, desde_fecha)

    async def _cargar() -> None:
        estado_ui["estado"] = await run.io_bound(leer_estado, token, items)
        construir_cuerpo(body, estado_ui["estado"], _aplicar, estado_ui["resultados"])

    async def _aplicar(attr: dict, a_escribir: List[dict], payload: dict, mostrado: str) -> None:
        lineas: List[Tuple[str, Optional[str]]] = []
        for p in a_escribir:
            err = await run.io_bound(
                _escribir_atributo, token, uid, sku, p["item_id"], attr["id"],
                attr["name"], p["valor"] or "", payload, mostrado)
            lineas.append((p["item_id"], err))
            if not err:
                estado_ui["escribio"] = True
        estado_ui["resultados"][attr["id"]] = lineas
        await _cargar()  # relectura en vivo: refleja también lo que pasó en catálogo

    async def _cerrar() -> None:
        if not estado_ui["escribio"]:
            dlg.close()
            return
        btn_cerrar.props("loading disable")
        seller_id = await run.io_bound(ml_get_user_id, token)
        resultado = await run.io_bound(audit_sku, uid, seller_id or "", sku, True)
        dlg.close()
        if not resultado.get("error"):
            al_cerrar(resultado)

    btn_releer.on_click(_cargar)
    btn_cerrar.on_click(_cerrar)
    await _cargar()
