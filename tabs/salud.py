"""
tabs/salud.py — Salud: auditoría de completitud de publicaciones ML, por SKU
(familia propia+catálogo), NO por publicación individual. Fase 0+1: solo
lectura. La corrida completa la dispara el cron nocturno (salud_audit.py);
esta página solo LEE el último snapshot guardado -- no dispara ninguna
llamada a ML por sí sola (salvo el botón "Actualizar este SKU" de Fase 2,
todavía no implementado acá).

La fila es el SKU, no la publicación: 150 grupos, no 1400 filas. Cuando una
dimensión varía dentro del grupo, la celda muestra la proporción ("3/5 con
descripción"); si todo el grupo coincide, muestra un único valor. El
desglose por ítem queda para el popup (Fase 2).
"""
from __future__ import annotations

import json
import re
import time
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
from nicegui import app, background_tasks, ui, run

from db import GROQ_MODEL, get_app_config, get_connection, log_ml_escritura
from ml_api import (
    get_ml_access_token,
    ml_get_item,
    ml_get_items_multiget_with_attributes,
    ml_get_prices_with_version,
    ml_get_user_id,
    ml_update_item_attributes,
    ml_write_item_description,
    ml_write_price_per_quantity,
)
from salud_audit import (
    _DESVIO_PP_MIN,
    _DESVIO_RATIO_MIN,
    _PCT_TECHO_SANIDAD,
    _QTYS_MAYORISTA,
    _evaluar_mayorista_gold_special,
    _standard_amount_de,
    audit_sku,
)

_OK = "#2E7D32"
_MID = "#BA7517"
_BAD = "#A32D2D"
_GREY = "#9CA3AF"
ML_API = "https://api.mercadolibre.com"


def _fmt_moneda(val: Optional[float]) -> str:
    if val is None:
        return "—"
    try:
        return "$" + f"{int(round(float(val))):,}".replace(",", ".")
    except (TypeError, ValueError):
        return "—"


def _require_login() -> Optional[Dict[str, Any]]:
    user = app.storage.user.get("user")
    if not user:
        ui.notify("Debes iniciar sesión para continuar", color="negative")
    return user


def _latest_snapshot_date(user_id: int) -> Optional[str]:
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT MAX(snapshot_date) AS d FROM salud_item_snapshots WHERE user_id=?",
            (user_id,),
        ).fetchone()
        return row["d"] if row else None
    finally:
        conn.close()


def _ultima_corrida_completa(user_id: int) -> Optional[str]:
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT run_datetime FROM cron_runs WHERE job='salud_audit' AND user_id=? "
            "AND status IN ('ok','partial') ORDER BY run_datetime DESC LIMIT 1",
            (user_id,),
        ).fetchone()
        return row["run_datetime"] if row else None
    finally:
        conn.close()


def _load_items(user_id: int, snapshot_date: str) -> List[Dict[str, Any]]:
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM salud_item_snapshots WHERE user_id=? AND snapshot_date=?",
            (user_id, snapshot_date),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _load_productos(user_id: int) -> Dict[str, Dict[str, Any]]:
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT sku, nombre, marca, stock FROM productos WHERE user_id=?",
            (user_id,),
        ).fetchall()
        return {r["sku"]: dict(r) for r in rows}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Resumen por dimensión: uniforme -> un valor; varía -> "n/total".
# ---------------------------------------------------------------------------

def _bool_dim(items: List[dict], ok_fn) -> Dict[str, Any]:
    vals = []
    for it in items:
        v = ok_fn(it)
        if v is not None:
            vals.append(v)
    total = len(vals)
    n_ok = sum(1 for v in vals if v)
    if total == 0:
        return {"texto": "—", "color": _GREY, "orden": -1.0}
    if n_ok == total:
        return {"texto": "OK", "color": _OK, "orden": 1.0}
    if n_ok == 0:
        return {"texto": "Falta", "color": _BAD, "orden": 0.0}
    return {"texto": f"{n_ok}/{total}", "color": _MID, "orden": n_ok / total}


def _gtin_dim(items: List[dict]) -> Dict[str, Any]:
    """GTIN separado en propias (accionable -- ML permite editarlo) vs. catálogo
    (informativo -- se hereda del producto de catálogo, ML nunca aplica un PUT ahí,
    ver _clasificar_hallazgos/"normal por diseño"). `orden` (para sort y para el
    score de esta dimensión) usa SOLO propias -- un hueco de catálogo nunca cuenta
    como problema del SKU. Confirmado en vivo 2026-09-07 con JBL-T530BT-Azul: antes
    de este fix el "6/10" mezclaba todo y marcaba el SKU con problema aunque las 5
    propias ya tuvieran GTIN completo -- el único hueco real era en las 4 de catálogo,
    donde ML no deja escribirlo."""
    propias = [it for it in items if not it.get("catalog_listing")]
    catalogo = [it for it in items if it.get("catalog_listing")]
    prop_tot, prop_ok = len(propias), sum(1 for it in propias if it.get("gtin"))
    cat_tot, cat_ok = len(catalogo), sum(1 for it in catalogo if it.get("gtin"))
    orden = (prop_ok / prop_tot) if prop_tot else -1.0
    return {
        "propias_ok": prop_ok, "propias_total": prop_tot,
        "catalogo_ok": cat_ok, "catalogo_total": cat_tot,
        "orden": orden,
    }


def _descripcion_dim(items: List[dict]) -> Dict[str, Any]:
    """Mismo split que _gtin_dim (ver esa función): propias (accionable -- ML permite
    editar la descripción) vs. catálogo (informativo -- se hereda del catalog_product,
    ML bloquea el PUT ahí, ver _clasificar_hallazgos/"normal por diseño"). `orden`
    (sort + score de esta dimensión) usa SOLO propias, igual que GTIN -- una descripción
    faltante en catálogo nunca cuenta como problema del SKU."""
    def _tiene_desc(it: dict) -> Optional[bool]:
        v = it.get("descripcion_len")
        return None if v is None else v > 0

    propias = [it for it in items if not it.get("catalog_listing") and _tiene_desc(it) is not None]
    catalogo = [it for it in items if it.get("catalog_listing") and _tiene_desc(it) is not None]
    prop_tot, prop_ok = len(propias), sum(1 for it in propias if _tiene_desc(it))
    cat_tot, cat_ok = len(catalogo), sum(1 for it in catalogo if _tiene_desc(it))
    orden = (prop_ok / prop_tot) if prop_tot else -1.0
    return {
        "propias_ok": prop_ok, "propias_total": prop_tot,
        "catalogo_ok": cat_ok, "catalogo_total": cat_tot,
        "orden": orden,
    }


def _cat_dim(items: List[dict], val_fn, etiquetas: Dict[str, str], color_fn) -> Dict[str, Any]:
    vals = [val_fn(it) for it in items if val_fn(it) is not None]
    total = len(vals)
    if total == 0:
        return {"texto": "—", "color": _GREY, "orden": -1.0}
    distintos = set(vals)
    if len(distintos) == 1:
        v = next(iter(distintos))
        return {"texto": etiquetas.get(v, v), "color": color_fn(v), "orden": 1.0}
    cont = Counter(vals)
    dominante, n_dom = cont.most_common(1)[0]
    return {"texto": f"{n_dom}/{total} {etiquetas.get(dominante, dominante)}", "color": _MID, "orden": n_dom / total}


def _magnitud_dim(items: List[dict], val_fn) -> Dict[str, Any]:
    vals = [val_fn(it) for it in items if val_fn(it) is not None]
    if not vals:
        return {"texto": "—", "color": _GREY, "orden": -1.0}
    lo, hi = min(vals), max(vals)
    texto = str(lo) if lo == hi else f"{lo}–{hi}"
    return {"texto": texto, "color": _OK if lo > 0 else _BAD, "orden": float(lo)}


_STATUS_NO_APLICABLE = {"no_aplica_catalogo", "no_aplica_no_activo", "no_determinable"}


def _perf_status_ok(status: Optional[str]) -> Optional[bool]:
    """Traduce el status crudo de /item/{id}/performance a cumple/no-cumple.
    no_aplica_catalogo (items de catálogo, sin entidad USER_PRODUCT propia) y
    no_determinable (404) quedan afuera del denominador -- no son un hueco."""
    if not status or status in _STATUS_NO_APLICABLE:
        return None
    return status == "COMPLETED"


def _fmt_lista_es(vals: List[int]) -> str:
    if not vals:
        return ""
    if len(vals) == 1:
        return str(vals[0])
    return ", ".join(str(v) for v in vals[:-1]) + f" y {vals[-1]}"


def _mayorista_dim(items: List[dict]) -> Dict[str, Any]:
    """Columna 'Mayorista' de la tabla resumen -- a diferencia de las demás
    dimensiones (que promedian el estado sobre TODOS los ítems de la familia),
    esta cuenta TIERS cargados y sanos sobre las 4 cantidades objetivo (2/3/5/10),
    solo en publicaciones gold_special (contado) -- mismo alcance que
    _evaluar_mayorista_gold_special en el popup, para que ambas vistas sean
    consistentes. Las gold_pro (cuotas) quedan afuera del conteo: el mayorista
    no aplica ahí y no deben diluirlo (bug confirmado en vivo 2026-09-04:
    Echo-Dot5-Kids-Stardust mostraba "2/4 ok" contando 2 gold_pro 'sin_mayorista'
    + 2 gold_special 'ok' como si fueran tiers -- cuando en realidad había 3
    tiers reales (2/3/5u) cargados y sanos, y el "4" nunca fue el denominador
    de cantidades objetivo sino la cantidad de ítems de la familia).
    Consolidado por unión entre las gold_special del grupo -- si dos gold_special
    tienen los mismos 3 tiers cargados, el resultado sigue siendo 3/4, no se
    duplica ni se promedia."""
    gold_special = [it for it in items if it.get("listing_type_id") == "gold_special"]
    if not gold_special:
        return {"texto": "—", "color": _GREY, "orden": -1.0, "tooltip": None}

    ok_qtys: set = set()
    estados_no_ok: List[str] = []
    for it in gold_special:
        estado = it.get("mayorista_estado")
        if estado == "ok":
            try:
                tiers = (json.loads(it.get("mayorista_tiers_json") or "{}") or {}).get("tiers") or []
            except (TypeError, ValueError):
                tiers = []
            for q, _amt in tiers:
                if q in _QTYS_MAYORISTA:
                    ok_qtys.add(q)
        elif estado:
            estados_no_ok.append(estado)

    # ⚠️ "revisar"/"invertido" -- calculado en el cron con cotización de envío real
    # (ver audit_item en salud_audit.py), independiente de si el tier ya cuenta o no
    # en `n`: un tier puede estar cargado y ser "ok" a nivel _wholesale_from_prices
    # (no roto, no invertido) y AUN ASÍ estar muy lejos del % que le corresponde según
    # el cálculo de envío -- confirmado en vivo 2026-09-04, Tag-Royal-LF12: el 5+
    # contaba en el "1/4" como ok, cargado 16.28% vs. 57.05% calculado. mayorista_revisar_json
    # NULL = no se evaluó (0 tiers cargados en ese ítem); {"evaluable": false} = se
    # intentó pero no se pudo cotizar envío -- ninguno de los dos casos prende el ⚠️.
    # Deduplicado entre las gold_special del grupo (mismo criterio de unión que ok_qtys).
    advertencias: List[str] = []
    vistas_tier: set = set()
    invertido_visto = False
    for it in gold_special:
        raw = it.get("mayorista_revisar_json")
        if not raw:
            continue
        try:
            info = json.loads(raw)
        except (TypeError, ValueError):
            continue
        if not info.get("evaluable"):
            continue
        for t in info.get("tiers_revisar") or []:
            clave = (t.get("quantity"), t.get("pct_cargado"), t.get("pct_calculado"))
            if clave in vistas_tier:
                continue
            vistas_tier.add(clave)
            advertencias.append(
                f"Revisar: {t.get('quantity')}+ cargado {_fmt_moneda(t.get('monto_cargado'))} "
                f"({t.get('pct_cargado')}%) vs. sugerido {_fmt_moneda(t.get('monto_calculado'))} ({t.get('pct_calculado')}%)"
            )
        if info.get("invertido") and not invertido_visto:
            invertido_visto = True
            advertencias.append("Invertido: hay tiers cargados en orden invertido — revisar manualmente")

    total = len(_QTYS_MAYORISTA)
    n = len(ok_qtys)
    color = _OK if n == total else (_BAD if n == 0 else _MID)
    texto = f"{n}/{total}"
    if advertencias:
        texto += " ⚠️"

    if n == total:
        tooltip = f"Completo ({'/'.join(str(q) for q in _QTYS_MAYORISTA)} cargados y ok)"
    elif n > 0:
        faltan = sorted(set(_QTYS_MAYORISTA) - ok_qtys)
        tooltip = f"Falta: {_fmt_lista_es(faltan)} unidades"
    elif "roto" in estados_no_ok:
        tooltip = "Roto — ningún tier válido"
    elif "invertido" in estados_no_ok:
        tooltip = "Precios invertidos — revisar tiers"
    elif "error_sin_standard" in estados_no_ok:
        tooltip = "Error: sin precio estándar"
    elif estados_no_ok:
        tooltip = "Sin mayorista cargado"
    else:
        tooltip = "Cargado con cantidades no estándar"

    if advertencias:
        tooltip = "\n".join([tooltip] + advertencias)

    return {"texto": texto, "color": color, "orden": n / total, "tooltip": tooltip}


def _sku_summary(sku: str, items: List[dict], prod_meta: Dict[str, Any]) -> Dict[str, Any]:
    n_items = len(items)

    dims = {
        "gtin": _gtin_dim(items),
        # Mismo split propias/catálogo que GTIN desde 2026-09-07 (ver _descripcion_dim) --
        # antes solo excluía catálogo del denominador (fix 2026-09-04, Echo-Dot5-Kids-Stardust:
        # el "2/4" contaba 2 ítems de catálogo sin descripción -- normal, ML lo bloquea -- como
        # si fueran un hueco real). Ahora además muestra el lado catálogo como informativo.
        # TODO(gap-membresia-grupo, 2026-09-04): quedan 3 gold_pro propias de este mismo SKU
        # sin descripción real (MLA3913903882, MLA3913903838, MLA2062899779) que todavía no
        # entran a ningún diagnóstico porque audit_sku() arranca de salud_item_snapshots
        # (gap de membresía del grupo, pendiente y separado) -- van a aparecer solas acá y en
        # el popup en cuanto ese gap se resuelva, sin tocar nada de esta dimensión.
        "descripcion": _descripcion_dim(items),
        "short": _bool_dim(items, lambda it: _perf_status_ok(it.get("short_status"))),
        "fotos": _magnitud_dim(items, lambda it: it.get("fotos_cantidad")),
        "mayorista": _mayorista_dim(items),
        "flex": _bool_dim(items, lambda it: _perf_status_ok(it.get("flex_status"))),
        "retiro_persona": _bool_dim(items, lambda it: bool(it.get("retiro_persona")) if it.get("retiro_persona") is not None else None),
        "garantia": _bool_dim(items, lambda it: bool(it.get("garantia_tipo"))),
        "envio_gratis": _bool_dim(items, lambda it: bool(it.get("envio_gratis")) if it.get("envio_gratis") is not None else None),
        "condicion": _cat_dim(
            items, lambda it: it.get("condicion"),
            {"new": "Nuevo", "used": "Usado"}, lambda v: _OK if v == "new" else _MID,
        ),
    }

    editables_vals = [it.get("atributos_faltantes_editables") for it in items if it.get("atributos_faltantes_editables") is not None]
    bloqueados_vals = [it.get("atributos_faltantes_bloqueados") for it in items if it.get("atributos_faltantes_bloqueados") is not None]
    # Unión de attr_id únicos entre publicaciones del SKU, no suma -- si una misma
    # característica falta en 3 de 5 publicaciones cuenta 1 vez, no 3 (confirmado en vivo
    # 2026-09-07: SKUs con varias publicaciones casi idénticas mostraban un número inflado
    # por publicación en vez de las características realmente distintas). Requiere
    # atributos_faltantes_json (guarda el id de cada atributo, no solo el conteo); si ningún
    # ítem del grupo lo tiene todavía (snapshot corrido antes de que existiera esa columna),
    # cae a la suma vieja para no perder el dato.
    faltantes_ids: set = set()
    tiene_json = False
    for it in items:
        raw = it.get("atributos_faltantes_json")
        if not raw:
            continue
        tiene_json = True
        try:
            parsed = json.loads(raw) if isinstance(raw, str) else raw
        except (TypeError, ValueError):
            continue
        for entry in (parsed or {}).get("editables") or []:
            aid = entry.get("id")
            if aid:
                faltantes_ids.add(aid)
    total_editables = len(faltantes_ids) if tiene_json else (sum(editables_vals) if editables_vals else None)

    scores = [it.get("performance_score") for it in items if it.get("performance_score") is not None]
    puntaje = round(sum(scores) / len(scores)) if scores else None

    errores = [it for it in items if it.get("error")]

    precios = [it.get("price") for it in items if it.get("price") is not None]
    precio_min = min(precios) if precios else None

    return {
        "sku": sku,
        "producto": (prod_meta.get(sku) or {}).get("nombre") or "",
        "marca": (prod_meta.get(sku) or {}).get("marca") or "",
        "stock": (prod_meta.get(sku) or {}).get("stock"),
        "precio_min": precio_min,
        "n_items": n_items,
        "n_errores": len(errores),
        "dims": dims,
        "regulatoria_texto": "No determinable",
        "atributos_editables_total": total_editables,
        "atributos_bloqueados_total": sum(bloqueados_vals) if bloqueados_vals else 0,
        "puntaje_ml": puntaje,
    }


def _build_rows(user_id: int) -> tuple:
    """Devuelve (filas, snapshot_date) -- filas ya agrupadas por SKU."""
    snap_date = _latest_snapshot_date(user_id)
    if not snap_date:
        return [], None
    items = _load_items(user_id, snap_date)
    prod_meta = _load_productos(user_id)

    por_sku: Dict[str, List[dict]] = defaultdict(list)
    for it in items:
        por_sku[it["sku"]].append(it)

    filas = [_sku_summary(sku, grp, prod_meta) for sku, grp in por_sku.items()]
    return filas, snap_date


def _stock_fresco_sync(uid: int, snap_date: str) -> Dict[str, int]:
    """Corre en un worker thread (run.io_bound desde el caller) -- multiget en vivo de
    available_quantity/status para TODAS las publicaciones de los SKUs que hoy tienen
    productos.stock > 0 en esta cuenta (acotado a esa cuenta, nunca cruza user_id). Una
    publicación pausada (típicamente por quedarse sin stock) cuenta como 0, no como su
    available_quantity crudo -- confirmado en vivo 2026-09-07 que ML pausa la publicación
    y devuelve available_quantity=0 en ese caso, pero no siempre es así en otros motivos
    de pausa, así que se fuerza igual por las dudas. El stock del SKU es la suma de sus
    publicaciones (mismo universo que ya usa el filtro Con/Sin stock).

    Persiste el resultado en productos.stock (mismo UPDATE que ya usa tabs/precios.py) --
    de paso deja el dato más al día también para esa pestaña, sin necesitar un cron nuevo.
    Devuelve {sku: stock_nuevo} para que el caller actualice las filas ya renderizadas."""
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT s.sku, s.item_id
            FROM salud_item_snapshots s
            JOIN productos p ON p.sku = s.sku AND p.user_id = s.user_id
            WHERE s.user_id=? AND s.snapshot_date=? AND p.stock > 0
            """,
            (uid, snap_date),
        ).fetchall()
    finally:
        conn.close()
    if not rows:
        return {}

    por_sku: Dict[str, List[str]] = defaultdict(list)
    for sku, item_id in rows:
        por_sku[sku].append(item_id)
    item_a_sku = {iid: sku for sku, ids in por_sku.items() for iid in ids}
    todos_ids = list(item_a_sku.keys())

    token = get_ml_access_token(uid)
    if not token:
        return {}

    stock_por_item: Dict[str, int] = {}
    for i in range(0, len(todos_ids), 20):
        batch = todos_ids[i:i + 20]
        bodies = ml_get_items_multiget_with_attributes(token, batch, "id,available_quantity,status")
        for b in bodies:
            if not b or not b.get("id"):
                continue
            qty = b.get("available_quantity") or 0
            if b.get("status") != "active":
                qty = 0
            stock_por_item[b["id"]] = qty

    stock_por_sku: Dict[str, int] = {}
    for iid, qty in stock_por_item.items():
        sku = item_a_sku[iid]
        stock_por_sku[sku] = stock_por_sku.get(sku, 0) + qty

    ahora = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    conn = get_connection()
    try:
        conn.executemany(
            "UPDATE productos SET stock=?, updated_at=? WHERE sku=? AND user_id=?",
            [(qty, ahora, sku, uid) for sku, qty in stock_por_sku.items()],
        )
        conn.commit()
    finally:
        conn.close()
    return stock_por_sku


_COLUMNS = [
    {"name": "sku", "label": "SKU", "field": "sku", "align": "left", "w": "130px"},
    {"name": "producto", "label": "Producto", "field": "producto", "align": "left", "w": "320px"},
    {"name": "marca", "label": "Marca", "field": "marca", "align": "left", "w": "90px"},
    {"name": "precio", "label": "Precio", "field": "precio", "align": "right", "w": "85px"},
    {"name": "stock", "label": "Stock", "field": "stock", "align": "right", "w": "70px"},
    {"name": "variantes", "label": "Variantes", "field": "variantes", "align": "right", "w": "75px", "sortable": False},
    {"name": "gtin", "label": "GTIN", "field": "gtin", "align": "center", "w": "65px"},
    {"name": "descripcion", "label": "Descripción", "field": "descripcion", "align": "center", "w": "85px"},
    {"name": "short", "label": "Short", "field": "short", "align": "center", "w": "65px"},
    {"name": "fotos", "label": "Fotos", "field": "fotos", "align": "center", "w": "65px"},
    {"name": "mayorista", "label": "Mayorista", "field": "mayorista", "align": "center", "w": "100px"},
    {"name": "flex", "label": "Flex", "field": "flex", "align": "center", "w": "65px"},
    {"name": "retiro_persona", "label": "Retiro en persona", "field": "retiro_persona", "align": "center", "w": "95px"},
    {"name": "garantia", "label": "Garantía", "field": "garantia", "align": "center", "w": "75px"},
    {"name": "envio_gratis", "label": "Envío gratis", "field": "envio_gratis", "align": "center", "w": "85px"},
    {"name": "regulatoria", "label": "Regulatoria", "field": "regulatoria", "align": "center", "w": "90px", "sortable": False},
    {"name": "condicion", "label": "Condición", "field": "condicion", "align": "center", "w": "75px"},
    {"name": "atributos_editables", "label": "Car. faltantes", "field": "atributos_editables", "align": "right", "w": "95px"},
    {"name": "puntaje_ml", "label": "Puntaje ML", "field": "puntaje_ml", "align": "right", "w": "80px"},
]


def _sort_key(row: dict, col: str):
    if col in ("sku", "producto", "marca"):
        return str(row.get(col) or "").lower()
    if col == "atributos_editables":
        v = row.get("atributos_editables_total")
        return v if v is not None else -1
    if col == "puntaje_ml":
        v = row.get("puntaje_ml")
        return v if v is not None else -1
    if col == "precio":
        v = row.get("precio_min")
        return v if v is not None else -1.0
    if col == "stock":
        v = row.get("stock")
        return v if v is not None else -1
    if col == "regulatoria":
        return 0
    d = row.get("dims", {}).get(col)
    return d["orden"] if d else -1.0


def _tag_cuotas(item: dict) -> Optional[str]:
    for t in (item.get("tags") or []):
        if t.endswith("_campaign"):
            return t.replace("_campaign", "")
    return None


def _item_descriptor(item: dict) -> str:
    """'catálogo, 3x' / 'propia, contado' / 'propia, cuotas' -- para identificar
    sin ambigüedad a qué publicación pertenece cada campo editable del popup."""
    rol = "catálogo" if item.get("catalog_listing") else "propia"
    cuota = _tag_cuotas(item)
    if cuota:
        return f"{rol}, {cuota}"
    if item.get("listing_type_id") == "gold_special":
        return f"{rol}, contado"
    return f"{rol}, cuotas"


_NORMAL_POR_DISENO_INTRO = (
    "ML no aplica cambios en publicaciones de catálogo -- atributos y descripción se "
    "heredan del producto de catálogo (el PUT puede devolver 200 igual, sin aplicarse) -- "
    "y el mayorista solo se carga en la publicación de contado, nunca en cuotas/Nx."
)


def _agrupar_normal_por_diseno(hechos: List[Dict[str, Any]]) -> List[str]:
    """Colapsa los hechos crudos de "normal por diseño" (uno por atributo x publicación,
    hasta decenas por SKU) en pocas líneas legibles: un renglón por publicación para los
    atributos heredados de catálogo (junta los nombres en vez de repetir la publicación
    una vez por atributo), y un único renglón agregado para descripción/mayorista (el
    motivo es el mismo para todas, no aporta nada repetirlo por item_id)."""
    lineas: List[str] = []

    por_item: Dict[str, Dict[str, Any]] = {}
    orden_items: List[str] = []
    for h in hechos:
        if h["tipo"] != "atributo_catalogo":
            continue
        if h["item_id"] not in por_item:
            por_item[h["item_id"]] = {"descriptor": h["descriptor"], "campos": []}
            orden_items.append(h["item_id"])
        por_item[h["item_id"]]["campos"].append(h["campo"])
    for iid in orden_items:
        e = por_item[iid]
        n = len(e["campos"])
        lineas.append(
            f"{n} atributo{'s' if n != 1 else ''} heredado{'s' if n != 1 else ''} de catálogo "
            f"({', '.join(e['campos'])}) — {iid} ({e['descriptor']})"
        )

    n_desc = sum(1 for h in hechos if h["tipo"] == "descripcion_catalogo")
    if n_desc:
        lineas.append(
            f"Descripción no editable en {n_desc} publicaci{'ón' if n_desc == 1 else 'ones'} de catálogo "
            "— se gestiona en el producto de catálogo"
        )

    n_mayor = sum(1 for h in hechos if h["tipo"] == "mayorista_no_aplica")
    if n_mayor:
        lineas.append(
            f"Mayorista no aplica en {n_mayor} publicaci{'ón' if n_mayor == 1 else 'ones'} (cuotas/Nx) "
            "— regla de negocio: solo aplica a la publicación de contado"
        )

    return lineas


def _clasificar_hallazgos(token: str, resultados: List[dict]) -> Dict[str, list]:
    """Separa los hallazgos crudos de audit_item() en 4 grupos (normal por diseño /
    sugerido con valor pre-cargado / necesita decisión de Diego / opcional -- SEO,
    no obligatorio, nunca cuenta como hallazgo accionable). El mayorista de
    publicaciones gold_special se evalúa aparte, con _evaluar_mayorista_gold_special
    (ver más abajo) -- no pasa por acá. No escribe nada -- solo lee y clasifica.

    "normal" viene ya agrupado (ver _agrupar_normal_por_diseno) -- el popup no debe
    repetir la misma explicación una vez por atributo y por publicación (llegó a 47
    líneas casi idénticas en SKUs con varias publicaciones de catálogo, ver
    OpenFit2-T920-Negro 2026-09-07)."""
    normal_hechos: List[Dict[str, Any]] = []
    sugeridos: List[Dict[str, Any]] = []
    decision: List[Dict[str, Any]] = []
    opcionales: List[Dict[str, Any]] = []

    items = [r["item"] for r in resultados]

    # Valor ya cargado de cada atributo (GTIN incluido -- ML lo trata como un
    # atributo más, ver doc "Auditoría de publicaciones ML"), tomado del primer
    # ítem del grupo que lo tenga. Sirve tanto para GTIN como para ficha técnica.
    valores_conocidos: Dict[str, str] = {}
    for it in items:
        for a in (it.get("attributes") or []):
            aid, val = a.get("id"), a.get("value_name")
            if aid and val and aid not in valores_conocidos:
                valores_conocidos[aid] = val

    for r in resultados:
        it, audit = r["item"], r["audit"]
        iid = it["id"]
        desc = _item_descriptor(it)
        try:
            faltantes_raw = json.loads(audit.get("atributos_faltantes_json") or "{}")
        except (TypeError, ValueError):
            faltantes_raw = {}
        faltantes = faltantes_raw.get("editables", [])
        for f in faltantes:
            aid, nombre = f.get("id"), f.get("name") or f.get("id")
            if it.get("catalog_listing"):
                # Mismo bloqueo que la descripción (ver más abajo): ML devuelve 200 en el
                # PUT de un atributo sobre una publicación de catálogo pero no lo aplica
                # -- se hereda del producto de catálogo. Confirmado en vivo el 2026-09-07
                # con OS_VERSION en GoogleTV-GA05662-US (PUT 200, GET siguió sin el valor).
                normal_hechos.append({"tipo": "atributo_catalogo", "item_id": iid, "descriptor": desc, "campo": nombre})
                continue
            entry = {
                "campo": nombre, "attr_id": aid, "item_id": iid,
                "descriptor": desc, "tipo": "atributo",
            }
            if aid in valores_conocidos:
                entry["valor_sugerido"] = valores_conocidos[aid]
                sugeridos.append(entry)
            else:
                entry["valor_sugerido"] = ""
                decision.append(entry)

        # Atributos NO obligatorios (tags.required != true en la categoría, ver
        # salud_audit.audit_item) -- van aparte, nunca mezclados con "sugerido"/
        # "decisión": son mejora de SEO/ficha técnica, no un hallazgo accionable.
        # En catálogo tampoco se pueden escribir (mismo bloqueo de arriba), pero no
        # vale la pena listarlos como "normal" -- ya son opcionales de por sí.
        if not it.get("catalog_listing"):
            for f in faltantes_raw.get("opcionales", []):
                aid, nombre = f.get("id"), f.get("name") or f.get("id")
                opcionales.append({
                    "campo": nombre, "attr_id": aid, "item_id": iid,
                    "descriptor": desc, "tipo": "atributo",
                    "valor_sugerido": valores_conocidos.get(aid, ""),
                })
        else:
            # Antes de FIX 3 (2026-09-07) estos caían en "editables" y el bloqueo de
            # arriba los mandaba a "normal" con este mismo mensaje -- al pasar a
            # "opcionales" quedaban silenciosamente afuera de todo (ni ofrecidos ni
            # explicados). Restaura el mensaje informativo para cualquier atributo
            # opcional bloqueado por catálogo, no solo GTIN.
            for f in faltantes_raw.get("opcionales", []):
                aid, nombre = f.get("id"), f.get("name") or f.get("id")
                normal_hechos.append({"tipo": "atributo_catalogo", "item_id": iid, "descriptor": desc, "campo": nombre})

    # --- descripcion ---
    propias_con_texto = [
        r["item"]["id"] for r in resultados
        if not r["item"].get("catalog_listing") and (r["audit"].get("descripcion_len") or 0) > 0
    ]
    catalogo_con_texto = [
        r["item"]["id"] for r in resultados
        if r["item"].get("catalog_listing") and (r["audit"].get("descripcion_len") or 0) > 0
    ]
    _texto_cache: Dict[str, str] = {}

    def _texto_de(item_id: str) -> str:
        if item_id not in _texto_cache:
            try:
                rr = requests.get(
                    f"{ML_API}/items/{item_id}/description",
                    headers={"Authorization": f"Bearer {token}"}, timeout=15,
                )
                _texto_cache[item_id] = (rr.json().get("plain_text") or "").strip() if rr.status_code == 200 else ""
            except requests.exceptions.RequestException:
                _texto_cache[item_id] = ""
        return _texto_cache[item_id]

    for r in resultados:
        it, audit = r["item"], r["audit"]
        iid = it["id"]
        desc = _item_descriptor(it)
        if (audit.get("descripcion_len") or 0) > 0:
            continue
        if it.get("catalog_listing"):
            # ML rechaza el PUT/POST de descripción en publicaciones de catálogo
            # ("Description is not modifiable on catalog listing item", confirmado
            # en vivo) -- nunca ofrecerlas como destino de escritura. Se gestiona
            # desde el producto de catálogo o desde la publicación propia pareja.
            normal_hechos.append({"tipo": "descripcion_catalogo", "item_id": iid, "descriptor": desc})
            continue
        # Publicación propia: prefiere texto de otra propia; si ninguna otra propia
        # tiene descripción, usa la de una copia de catálogo SOLO como fuente para
        # copiar (nunca se escribe ahí, eso ya se filtró arriba).
        if propias_con_texto:
            fuente, origen_txt = propias_con_texto[0], f"copiado de {propias_con_texto[0]}, propia"
        elif catalogo_con_texto:
            fuente, origen_txt = catalogo_con_texto[0], f"copiado de {catalogo_con_texto[0]}, catálogo"
        else:
            fuente, origen_txt = None, ""
        if fuente:
            sugeridos.append({
                "campo": f"Descripción ({origen_txt})", "item_id": iid, "descriptor": desc,
                "tipo": "descripcion", "valor_sugerido": _texto_de(fuente),
            })
        else:
            decision.append({
                "campo": "Descripción", "item_id": iid, "descriptor": desc,
                "tipo": "descripcion", "valor_sugerido": "",
            })

    # --- mayorista: solo la nota informativa de "no aplica en cuotas" queda acá.
    # La evaluación real (crear/ok/roto/revisar) de las publicaciones gold_special
    # vive en _evaluar_mayorista_gold_special, ver más abajo.
    for r in resultados:
        it, audit = r["item"], r["audit"]
        iid = it["id"]
        desc = _item_descriptor(it)
        if audit.get("mayorista_estado") == "sin_mayorista" and it.get("listing_type_id") != "gold_special":
            normal_hechos.append({"tipo": "mayorista_no_aplica", "item_id": iid, "descriptor": desc})

    normal = {"count": len(normal_hechos), "lineas": _agrupar_normal_por_diseno(normal_hechos)}
    return {"normal": normal, "sugeridos": sugeridos, "decision": decision, "opcionales": opcionales}


def _consolidar(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Agrupa hallazgos por dato (mismo attr_id, o "descripción" en conjunto) en vez
    de por publicación -- un mismo dato (GTIN, un atributo de ficha técnica, la
    descripción) se pide una sola vez aunque falte en varias de las hasta-10
    publicaciones del SKU. Cada grupo guarda la lista de publicaciones a las que se
    va a aplicar el valor cuando se guarde."""
    grupos: Dict[tuple, Dict[str, Any]] = {}
    orden: List[tuple] = []
    for h in entries:
        key = ("atributo", h["attr_id"]) if h["tipo"] == "atributo" else (h["tipo"], None)
        if key not in grupos:
            grupos[key] = {
                "campo": "Descripción" if h["tipo"] == "descripcion" else h["campo"],
                "tipo": h["tipo"],
                "attr_id": h.get("attr_id"),
                "valor_sugerido": "",
                "items": [],
            }
            orden.append(key)
        g = grupos[key]
        if not g["valor_sugerido"] and h.get("valor_sugerido"):
            g["valor_sugerido"] = h["valor_sugerido"]
        g["items"].append({"item_id": h["item_id"], "descriptor": h["descriptor"]})
    return [grupos[k] for k in orden]


def _aplica_a_texto(items: List[Dict[str, str]]) -> str:
    """Incluye el descriptor completo ('propia, 3x' / 'catálogo, contado') -- no solo
    el tier de cuotas -- para que se entienda de entrada si el destino es una
    publicación propia o una copia de catálogo (ver restricciones de escritura)."""
    return "se aplicará a: " + ", ".join(f"{it['item_id']} ({it['descriptor']})" for it in items)


def _con_boton_ia(g: Dict[str, Any], seccion: str) -> bool:
    """Descripción tiene botón de IA en cualquier sección. Atributos de ficha
    técnica solo en "necesita decisión" (los de "sugerido" ya vienen con un valor
    conocido de otra publicación). GTIN queda afuera siempre -- sugerir un código
    de barras es inventarlo, no autocompletarlo."""
    if g["tipo"] == "descripcion":
        return True
    return seccion == "decision" and g["tipo"] == "atributo" and g["attr_id"] != "GTIN"


def _item_principal(items: List[dict]) -> dict:
    """Mismo criterio de 'representante' que usa Productos (tabs/precios.py): la
    propia gold_special (no catálogo) con más stock; si no hay, cualquier ítem."""
    return max(
        items,
        key=lambda it: (
            1 if not it.get("catalog_listing") and str(it.get("listing_type_id") or "").lower() == "gold_special" else 0,
            int(it.get("available_quantity") or 0),
        ),
    )


def _contexto_producto(items: List[dict], marca: str) -> str:
    """Arma el bloque de contexto para el prompt de IA: título, marca, categoría y
    los atributos ya cargados en la publicación representante del grupo. Una sola
    llamada extra (nombre de categoría, sin auth -- endpoint público) por popup, no
    por campo."""
    principal = _item_principal(items)
    partes = [f"Título de la publicación: {principal.get('title') or ''}"]
    if marca:
        partes.append(f"Marca: {marca}")
    cat_id = principal.get("category_id")
    if cat_id:
        cat_nombre = cat_id
        try:
            r = requests.get(f"{ML_API}/categories/{cat_id}", timeout=10)
            if r.status_code == 200:
                cat_nombre = r.json().get("name") or cat_id
        except requests.exceptions.RequestException:
            pass
        partes.append(f"Categoría: {cat_nombre}")
    attrs = [
        f"{a.get('name') or a.get('id')}: {a.get('value_name')}"
        for a in (principal.get("attributes") or [])
        if a.get("value_name") and a.get("id") != "GTIN"
    ]
    if attrs:
        partes.append("Atributos ya cargados: " + "; ".join(attrs))
    return "\n".join(partes)


def _fetch_category_attrs(cat_id: str) -> List[dict]:
    """/categories/{id}/attributes -- endpoint público, sin auth. Se usa para que la
    sugerencia de IA respete el value_type real del atributo (ej. number_unit con
    una única unidad permitida, o lista cerrada de valores) en vez de texto libre
    que ML descarta en silencio -- caso confirmado: USE_TIME es number_unit con
    allowed_units=['h'] y la IA sugirió '60 minutos', que ML no reconoce."""
    try:
        r = requests.get(f"{ML_API}/categories/{cat_id}/attributes", timeout=15)
        return r.json() if r.status_code == 200 else []
    except requests.exceptions.RequestException:
        return []


def _norm(s: Optional[str]) -> str:
    """Normaliza para matchear contra values[]/allowed_units sin que tilde/mayúscula/
    espaciado de más rompan el match (caso confirmado: 'días' vs 'dias', '64mb' vs
    '64 MB'). También pareja el espaciado alrededor de comas: ML devuelve el
    value_name de un atributo multivalued SIN espacio después de la coma -- verificado
    de forma independiente el 2026-09-07 con un GET directo a MLA3403904978
    (AW-Se3-Black-MEH94LW), que devolvió LANGUAGES.value_name = 'Español,Inglés' pese a
    que el popup escribe "Español, Inglés" (coma + espacio). Sin este ajuste, una
    escritura que sí se aplicó bien se reportaba como "GET no coincide" por diferencia
    de formato, no de contenido."""
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()
    s = " ".join(s.strip().lower().split())
    return re.sub(r"\s*,\s*", ",", s)


def _tiene_tag(attr_def: Optional[dict], tag: str) -> bool:
    """La API de atributos devuelve `tags` como dict ({'multivalued': true, ...}) en
    /categories/{id}/attributes, pero la doc de ML también muestra variantes con
    `tags` como lista de strings -- se soportan ambas para no asumir de más."""
    tags = (attr_def or {}).get("tags")
    if isinstance(tags, dict):
        return bool(tags.get(tag))
    if isinstance(tags, list):
        return tag in tags
    return False


def _valores_de(attr_def: Optional[dict]) -> List[dict]:
    return (attr_def or {}).get("values") or []


def _tipo_campo(attr_def: Optional[dict]) -> str:
    """Determina qué widget corresponde para un atributo. El discriminante real NO es
    value_type solo -- es si `values[]` viene poblado. Un atributo "string" con
    values[] poblado y tag multivalued (ej. LANGUAGES) exige matchear nombres exactos
    de esa lista igual que un "list", aunque el tipo diga string. En cambio
    `suggested_values` (values[] vacío) es solo un hint de autocompletado -- ML sigue
    aceptando texto libre nuevo ahí (confirmado contra la doc oficial de atributos).

    Devuelve: "closed" (boolean/list -- nunca texto libre, siempre value_id),
    "number_unit", "multivalued" (string/number con values[] + tag multivalued),
    "closed_or_free" (string/number con values[] sin multivalued -- ML tolera un
    value_name nuevo), o "free" (sin values[], o sin attr_def -- texto libre, sin
    cambios de comportamiento)."""
    if not attr_def:
        return "free"
    value_type = attr_def.get("value_type")
    if value_type in ("boolean", "list"):
        return "closed"
    if value_type == "number_unit":
        return "number_unit"
    valores = _valores_de(attr_def)
    if valores and _tiene_tag(attr_def, "multivalued"):
        return "multivalued"
    if valores:
        return "closed_or_free"
    return "free"


def _match_valor_id(attr_def: Optional[dict], texto: str) -> Optional[str]:
    """Busca en values[] un name que matchee (normalizado) `texto`. Devuelve el
    value_id si matchea, None si no hay match."""
    low = _norm(texto)
    if not low:
        return None
    for v in _valores_de(attr_def):
        if _norm(v.get("name")) == low:
            return v.get("id")
    return None


def _match_valor_nombre(attr_def: Optional[dict], texto: str) -> Optional[str]:
    """Como _match_valor_id pero devuelve el name canónico (con el casing/tildes
    reales de ML) en vez del id -- para armar value_name en vez de value_id."""
    low = _norm(texto)
    if not low:
        return None
    for v in _valores_de(attr_def):
        if _norm(v.get("name")) == low:
            return v.get("name")
    return None


_NUM_RE = re.compile(r"[-+]?\d+(?:[.,]\d+)?")


def _parse_number_unit(texto: str, attr_def: dict) -> Tuple[str, str]:
    """Separa un texto tipo '60 h' / '64mb' / '5 - 7 días' en (número, unidad_id) lo
    mejor posible: toma el primer número que aparece y matchea lo que sigue contra
    allowed_units (normalizado). Si no reconoce la unidad, cae al default_unit de la
    categoría o a la primera de allowed_units -- nunca deja la unidad en blanco si hay
    una sola opción posible (caso USE_TIME: allowed_units=['h'])."""
    unidades = [u.get("id") for u in (attr_def.get("allowed_units") or []) if u.get("id")]
    default_unit = attr_def.get("default_unit") or (unidades[0] if unidades else "")
    m = _NUM_RE.search(texto or "")
    numero = m.group(0).replace(",", ".") if m else ""
    resto = _norm((texto or "")[m.end():]) if m else _norm(texto or "")
    unidad = next((u for u in unidades if _norm(u) == resto), None) or default_unit
    return numero, unidad


def _prompt_ia(g: Dict[str, Any], contexto: str, attr_def: Optional[dict] = None) -> str:
    if g["tipo"] == "descripcion":
        return (
            f"{contexto}\n\n"
            "Escribí una descripción de producto para una publicación de MercadoLibre "
            "en español, clara y comercial, de 150 a 400 palabras, basada solo en la "
            "información disponible arriba (no inventes características que no estén "
            "sugeridas por el título/atributos). Devolvé SOLO el texto de la "
            "descripción, sin comillas ni encabezados."
        )
    tipo_campo = _tipo_campo(attr_def)
    valores = _valores_de(attr_def)
    if tipo_campo == "number_unit":
        unidades = [u.get("id") for u in (attr_def.get("allowed_units") or []) if u.get("id")]
        unidad = attr_def.get("default_unit") or (unidades[0] if unidades else "")
        return (
            f"{contexto}\n\n"
            f"Sugerí el valor para el atributo de ficha técnica \"{g['campo']}\" de este "
            f"producto. Es un valor numérico con unidad, y la ÚNICA unidad válida es "
            f"'{unidad}'. Respondé SOLO con un número seguido de esa unidad (ejemplo: "
            f"'60 {unidad}'). No uses ninguna otra unidad ni la conviertas a otra. Si no "
            "podés inferir el valor con confianza, respondé exactamente VACIO."
        )
    if tipo_campo == "multivalued":
        opciones = ", ".join(v.get("name", "") for v in valores[:80] if v.get("name"))
        return (
            f"{contexto}\n\n"
            f"Elegí las opciones que apliquen para el atributo de ficha técnica "
            f"\"{g['campo']}\" de este producto, ELIGIENDO SOLO entre estas opciones "
            f"exactas (copiá cada una tal cual está escrita; si elegís más de una, "
            f"separalas con coma): {opciones}. Si ninguna aplica con confianza, "
            "respondé exactamente VACIO."
        )
    if valores:
        opciones = ", ".join(v.get("name", "") for v in valores[:80] if v.get("name"))
        return (
            f"{contexto}\n\n"
            f"Elegí el valor más probable para el atributo de ficha técnica \"{g['campo']}\" "
            f"de este producto, ELIGIENDO UNA de estas opciones exactas (respondé copiando "
            f"una tal cual está escrita, sin agregar nada más): {opciones}. Si ninguna "
            "aplica con confianza, respondé exactamente VACIO."
        )
    return (
        f"{contexto}\n\n"
        f"Sugerí el valor más probable para el atributo de ficha técnica \"{g['campo']}\" "
        "de este producto. Respondé SOLO con el valor (una palabra o frase corta), sin "
        "explicaciones ni puntuación extra. Si no podés inferirlo con confianza, "
        "respondé exactamente VACIO."
    )


class _CampoWidget:
    """Envuelve el/los widgets ya renderizados de un campo editable del popup para que
    _guardar() y el botón de IA no necesiten conocer, campo por campo, si el atributo
    es boolean/list (value_id), number_unit (número+unidad), string multivalued (chips)
    o texto libre -- cada _CampoWidget sabe armar su propio payload de escritura y su
    propia visualización, sin que el resto del popup tenga que ramificar por tipo."""

    def __init__(self, tiene_valor, payload, display, set_texto):
        self._tiene_valor = tiene_valor
        self._payload = payload
        self._display = display
        self._set_texto = set_texto

    def tiene_valor(self) -> bool:
        return self._tiene_valor()

    def payload(self) -> Optional[Dict[str, Any]]:
        return self._payload()

    def display(self) -> str:
        return self._display()

    def set_texto(self, texto: str) -> bool:
        """Aplica un texto (típicamente sugerido por IA) al widget. Devuelve True si
        el texto matcheaba limpio contra el dominio cerrado del atributo (o si el
        campo no tiene dominio cerrado que validar), False si no matcheaba."""
        return self._set_texto(texto)


def _groq_generate(api_key: str, prompt: str) -> str:
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": GROQ_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 500,
        "temperature": 0.5,
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=20)
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"].strip()


def _tiers_plan(evaluacion: Dict[str, Any], incluir: set,
                 eliminar: Optional[set] = None) -> Tuple[Dict[int, float], List[int], List[Dict[str, Any]]]:
    """Arma (cambios, bloqueadas, conflictos) para las cantidades que el usuario tildó
    en `incluir` -- generaliza la versión anterior (_tiers_accionables): la decisión de
    qué corregir ahora es 100% del checkbox por tier del popup. "crear"/"roto" vienen
    pre-tildados por default, "revisar" no (ver render). `evaluacion["tiers"]` incluye
    tanto las 4 cantidades estándar (2/3/5/10) como cualquier tier "extra" que el ítem
    ya tenga cargado en otra cantidad (ver _evaluar_mayorista_gold_special) -- ambos se
    tratan con el mismo criterio acá, no hay caso especial por ser "extra".

    Un tier "crear"/"roto"/"revisar" NO tildado no se toca -- si ya tiene un valor
    cargado (roto/revisar/ok), ese valor sigue siendo el PISO para las cantidades mayores
    Y EL TECHO para las cantidades menores: ML exige % ESTRICTAMENTE creciente con la
    cantidad (confirmado en vivo el 2026-09-03 probando AW-S11-Black-MEQT4LW: "Price
    per quantity invalid coherence order" cuando un tier nuevo quedaba más bajo que uno
    preservado en una cantidad menor, y "Price per quantity amount are not unique" al
    igualarlo en vez de superarlo). El caso techo (un tier NO tildado en una cantidad
    MAYOR que queda por debajo de la corrección pedida en una cantidad menor) se
    confirmó en vivo el 2026-09-04 con MLA1944479697/MLA1944467261: un tier de 15+
    cargado al 6.23%, nunca gestionado por el popup hasta ahora, volvía incoherente
    cualquier corrección de 2/5/10 hacia arriba de ese valor -- el 400 de ML no era
    (solo) por el orden del array, era porque nadie chequeaba ese techo antes de
    guardar.

    El techo lo impone CUALQUIER tier no tildado con un valor cargado, sin importar su
    estado -- antes solo lo hacían "roto"/"revisar", dejando afuera un tier sano ("ok")
    en una cantidad mayor. Confirmado en vivo el 2026-09-07 (Samsung-BudsCore-SMR410-
    Blanco/MLA1846800581 y GoogleTV-GA05662-US/MLA2612433346): la cantidad 3 tenía un
    tier "ok" (5.04% y 1.97% respectivamente, sano en soledad), pero corregir la
    cantidad 2 hacia un % mayor lo dejaba incoherente igual -- nadie lo frenaba porque
    "ok" no entraba al cálculo del techo. Mismo error de ML ("Price per quantity invalid
    coherence order"), causa distinta a la de MLA1944479697/MLA1944467261.

    Se recorre en orden creciente de cantidad manteniendo el piso. Antes de eso se
    calcula, de atrás para adelante, el techo que impone cada tier NO tildado (cualquier
    estado, no solo roto o revisar) sobre las cantidades menores. Un tier tildado que quedaría en o por debajo
    del piso se sube a piso + 0.01. Si ese ajuste aleja el % resultante de su propio
    valor calculado más allá del mismo umbral que separa "ok" de "revisar" (4pp y
    1.75x), no se fuerza -- se bloquea esa cantidad y todas las que siguen, y se marca
    para revisión manual. Si en cambio el % resultante iguala o supera el techo de un
    tier NO tildado en una cantidad mayor, también se bloquea, pero además se reporta
    en `conflictos` -- el render lo muestra como "tildá también esa cantidad" en vez
    del mensaje genérico de revisión manual.

    `eliminar`: cantidades tildadas para sacar del array (ver FIX A / tope de 5,
    2026-09-07) -- un tier eliminado no impone piso ni techo (no va a existir más
    del lado de ML), y nunca entra a `cambios` aunque también esté en `incluir`
    (mutuamente excluyente por diseño en el popup; acá es solo la segunda capa)."""
    eliminar = eliminar or set()
    tiers_ordenados = sorted(evaluacion["tiers"], key=lambda x: x["quantity"])

    techo: Optional[Tuple[float, int]] = None  # (pct, cantidad que lo impone)
    techo_por_qty: Dict[int, Optional[Tuple[float, int]]] = {}
    for t in reversed(tiers_ordenados):
        techo_por_qty[t["quantity"]] = techo
        if t["quantity"] not in incluir and t["quantity"] not in eliminar and t.get("pct_cargado") is not None:
            if techo is None or t["pct_cargado"] < techo[0]:
                techo = (t["pct_cargado"], t["quantity"])

    piso = 0.0
    cambios: Dict[int, float] = {}
    bloqueadas: List[int] = []
    conflictos: List[Dict[str, Any]] = []
    for t in tiers_ordenados:
        q = t["quantity"]
        estado = t["estado"]
        if q in eliminar:
            continue
        if estado == "ok":
            piso = max(piso, t["pct_cargado"])
            continue
        if estado not in ("crear", "roto", "revisar"):
            continue
        if q not in incluir:
            if estado in ("roto", "revisar"):
                piso = max(piso, t["pct_cargado"])
            continue
        if bloqueadas:
            bloqueadas.append(q)
            continue
        pct_calc = t["pct_calculado"]
        pct_final = pct_calc if pct_calc > piso else round(piso + 0.01, 2)
        techo_q = techo_por_qty.get(q)
        if techo_q is not None and pct_final >= techo_q[0]:
            bloqueadas.append(q)
            conflictos.append({"quantity": q, "conflicto_con": techo_q[1], "techo_pct": techo_q[0]})
            continue
        diff_pp = abs(pct_final - pct_calc)
        ratio = max(pct_final, pct_calc) / max(min(pct_final, pct_calc), 0.01)
        if diff_pp >= _DESVIO_PP_MIN and ratio >= _DESVIO_RATIO_MIN:
            bloqueadas.append(q)
            continue
        cambios[q] = pct_final
        piso = pct_final
    return cambios, bloqueadas, conflictos


# ---------------------------------------------------------------------------
# Escritura hacia ML -- SIEMPRE con GET de verificación independiente y log en
# ml_escrituras (ok o error), nunca confiando en el 200 del PUT/POST -- y a la
# inversa, tampoco en un status != 200: confirmado en vivo (BHR4245GL) que ML
# puede devolver 500 "Internal error calling prices-validator-api" en el POST
# y aun así aplicar la escritura del lado de ML. El GET de verificación manda
# siempre, incluso cuando el POST/PUT no dio 200 -- si el GET confirma el
# valor, es ok; el detalle del POST solo se usa como mensaje de error cuando
# el GET tampoco confirma.
# ---------------------------------------------------------------------------

def _escribir_atributo(token: str, uid: int, sku: str, item_id: str, attr_id: str,
                        campo_label: str, valor_anterior: str, attr_payload: Dict[str, Any],
                        valor_mostrado: str) -> Optional[str]:
    """Devuelve None si ok, o un mensaje de error para el resumen si falló. `attr_payload`
    ya viene armado por el popup ({"id": attr_id, "value_id": ...} para boolean/list,
    {"id": attr_id, "value_name": ...} para number_unit/multivalued/string/N-A) -- acá
    no se decide el tipo, solo se escribe y se verifica contra el campo que corresponda
    (value_id si se mandó value_id, value_name si no -- comparar siempre por value_name
    cuando se escribió value_id es lo que generaba falsos "no coincide": ML puede
    normalizar/completar el value_name mostrado distinto al que se mandó)."""
    resp = ml_update_item_attributes(token, item_id, [attr_payload])
    post_detalle = f"PUT status={resp.status_code} {resp.text[:200]}" if resp.status_code != 200 else None
    time.sleep(0.4)
    item = ml_get_item(token, item_id)
    actual_attr = None
    if item:
        actual_attr = next((a for a in (item.get("attributes") or []) if a.get("id") == attr_id), None)
    if attr_payload.get("value_id") not in (None, ""):
        ok = actual_attr is not None and str(actual_attr.get("value_id") or "") == str(attr_payload["value_id"])
    else:
        ok = actual_attr is not None and _norm(actual_attr.get("value_name")) == _norm(attr_payload.get("value_name"))
    if ok:
        log_ml_escritura(uid, sku, item_id, f"atributo:{attr_id}", valor_anterior, valor_mostrado, "salud_popup", "ok", None)
        return None
    detalle = post_detalle or f"GET de verificación no coincide (quedó {actual_attr!r})"
    log_ml_escritura(uid, sku, item_id, f"atributo:{attr_id}", valor_anterior, valor_mostrado, "salud_popup", "error", detalle)
    return f"{campo_label} ({item_id}): {detalle}"


def _escribir_descripcion(token: str, uid: int, sku: str, item_id: str,
                           texto_anterior_len: int, texto_nuevo: str) -> Optional[str]:
    resp = ml_write_item_description(token, item_id, texto_nuevo)
    post_detalle = f"status={resp.status_code} {resp.text[:200]}" if resp.status_code not in (200, 201) else None
    time.sleep(0.4)
    try:
        r = requests.get(f"{ML_API}/items/{item_id}/description", headers={"Authorization": f"Bearer {token}"}, timeout=15)
        guardado = (r.json().get("plain_text") or "").strip() if r.status_code == 200 else ""
    except requests.exceptions.RequestException:
        guardado = ""
    ok = guardado == texto_nuevo.strip()
    if ok:
        log_ml_escritura(uid, sku, item_id, "descripcion", f"{texto_anterior_len} chars", f"{len(texto_nuevo)} chars", "salud_popup", "ok", None)
        return None
    detalle = post_detalle or "GET de verificación no coincide"
    log_ml_escritura(uid, sku, item_id, "descripcion", f"{texto_anterior_len} chars", f"{len(texto_nuevo)} chars", "salud_popup", "error", detalle)
    return f"Descripción ({item_id}): {detalle}"


def _tier_body(mpu: int, pct: float) -> Dict[str, Any]:
    return {
        "type": "discount_percentage",
        "percentage": pct,
        "conditions": {
            "context_restrictions": ["channel_marketplace", "user_type_business"],
            "min_purchase_unit": mpu,
            "eligible": True,
        },
    }


def _pct_seguro(pct: Optional[float]) -> bool:
    """Piso de sanidad genérico (independiente de la causa puntual del esquema fijo
    en salud_audit.py, ver _PCT_TECHO_SANIDAD): ningún % que llegue a este punto --
    cualquiera sea su origen -- puede escribirse a ML si da un precio negativo o
    casi regalado. Segundo chequeo, redundante con el de salud_audit.py a propósito."""
    return pct is not None and 0 < pct < _PCT_TECHO_SANIDAD


def _construir_payload_mayorista(prices_info: dict, cambios: Dict[int, float],
                                  eliminar: Optional[set] = None) -> Tuple[List[Dict[str, Any]], bool, List[Dict[str, Any]]]:
    """Arma el body completo para POST /prices/price-per-quantity a partir de lo que
    hay HOY + los cambios pedidos (cantidad -> % nuevo). El endpoint reemplaza el
    array entero: cualquier cantidad que no se re-envíe queda eliminada -- por eso
    acá se reconstruye TODO lo que tiene que sobrevivir (tier de 1 unidad, cantidades
    no estándar, tiers "ok"/"revisar" que no están en `cambios`), no solo lo nuevo.
    Si el ítem tiene el sistema legacy (tag standard_price_by_quantity), sus tiers se
    convierten a % preservando el mismo precio real (remove-absolute-pxq los borra
    del lado de ML de todas formas, así que hay que re-crearlos acá para no perderlos).
    Los tiers % existentes que se preservan sin cambios van con su "id" propio -- por
    la lógica documentada de ML, mandar el id de un precio existente lo deja intacto;
    omitirlo lo borra.

    `eliminar`: cantidades que el usuario tildó explícitamente para sacar del array
    (ver FIX A / tope de 5, 2026-09-07) -- se excluyen de body_items sin importar si
    venían del sistema legacy o del % nuevo, y sin importar si además aparecen en
    `cambios` (eliminar gana; el popup ya las trata como mutuamente excluyentes por
    tier, esto es solo la segunda capa de defensa).

    Devuelve además `descartados`: cantidades pedidas en `cambios` con un % inválido
    (faltante, <=0 o >=_PCT_TECHO_SANIDAD -- ver _pct_seguro) que NO se escribieron.
    Si la cantidad ya tenía un tier cargado, se preserva el valor actual (no se borra
    un tier existente por un cálculo nuevo inválido); si era un tier nuevo ("crear"),
    directamente no se agrega."""
    eliminar = eliminar or set()
    standard_amount = _standard_amount_de(prices_info)
    tiene_absoluto = any(
        p.get("type") == "standard" and (p.get("conditions") or {}).get("min_purchase_unit") is not None
        for p in prices_info.get("prices") or []
    )
    body_items: List[Dict[str, Any]] = []
    vistos: set = set()
    descartados: List[Dict[str, Any]] = []

    for p in prices_info.get("prices") or []:
        cond = p.get("conditions") or {}
        mpu = cond.get("min_purchase_unit")
        if mpu is None or p.get("amount") is None or not standard_amount:
            continue
        vistos.add(mpu)
        if mpu in eliminar:
            continue
        pct = cambios.get(mpu)
        if pct is not None and not _pct_seguro(pct):
            descartados.append({"quantity": mpu, "pct_pedido": pct})
            pct = None
        if pct is None:
            pct = round((1 - float(p["amount"]) / standard_amount) * 100, 2)
        body_items.append(_tier_body(mpu, pct))

    for p in prices_info.get("price_per_quantity") or []:
        if p.get("type") != "discount_percentage":
            continue
        cond = p.get("conditions") or {}
        mpu = cond.get("min_purchase_unit")
        if mpu is None or mpu in vistos:
            continue
        vistos.add(mpu)
        if mpu in eliminar:
            continue
        pct = cambios.get(mpu)
        if mpu in cambios and not _pct_seguro(pct):
            descartados.append({"quantity": mpu, "pct_pedido": pct})
            pct = None
        if pct is not None:
            body_items.append(_tier_body(mpu, pct))
        else:
            preservado = _tier_body(mpu, p.get("percentage"))
            preservado["id"] = p["id"]
            body_items.append(preservado)

    for mpu, pct in cambios.items():
        if mpu not in vistos and mpu not in eliminar:
            if not _pct_seguro(pct):
                descartados.append({"quantity": mpu, "pct_pedido": pct})
                continue
            body_items.append(_tier_body(mpu, pct))

    # Los tiers "crear" (nuevos, recién agregados arriba) quedan al final del array en
    # el orden en que se procesaron, no por cantidad -- confirmado en vivo 2026-09-04
    # (MLA1944479697/MLA1944467261) que ML valida "invalid coherence order" sensible al
    # orden del array además de a los valores en sí. Se ordena siempre por cantidad
    # ascendente antes de enviar, sin importar en qué orden se armó arriba.
    body_items.sort(key=lambda b: b["conditions"]["min_purchase_unit"])
    return body_items, tiene_absoluto, descartados


_ML_MAX_TIERS_PXQ = 5


def _escribir_mayorista_pxq(token: str, uid: int, sku: str, item_id: str,
                             cambios: Dict[int, float],
                             eliminar: Optional[set] = None) -> Tuple[Optional[str], List[str]]:
    """cambios: {cantidad: porcentaje} SOLO para las cantidades a crear/corregir --
    todo lo demás que el ítem ya tenga cargado se preserva (ver _construir_payload_mayorista).
    eliminar: cantidades tildadas para sacar del array y liberar lugar (ver FIX A /
    tope de 5, 2026-09-07).
    Devuelve (error, advertencias) -- advertencias lista las cantidades que
    _construir_payload_mayorista descartó por el piso de sanidad (nunca se
    escribieron a ML), aunque el resto se haya guardado bien (error=None)."""
    prices_info = ml_get_prices_with_version(token, item_id)
    if not prices_info or "version" not in prices_info:
        msg = "no se pudo leer la versión de precios (X-Version) antes de escribir"
        log_ml_escritura(uid, sku, item_id, "mayorista_pxq", None, json.dumps(cambios, ensure_ascii=False), "salud_popup", "error", msg)
        return f"Mayorista ({item_id}): {msg}", []
    version = prices_info["version"]
    body_items, tiene_pxq_absoluto, descartados = _construir_payload_mayorista(prices_info, cambios, eliminar)
    if len(body_items) > _ML_MAX_TIERS_PXQ:
        # Backstop server-side: el popup ya bloquea el guardado antes de llegar acá
        # (banner ⛔ + mayorista_sobre_tope en build_tab_salud), pero el GET de acá es
        # más fresco que el que vio el popup al abrirse -- si algo cambió del lado de
        # ML entre medio
        # (otra escritura, otra pestaña), nunca se manda un POST que ML va a
        # rechazar con "Maximum 5 price_per_quantity entries allowed" (caso real
        # MLA3684456394, 2026-09-07: 5 cargados + 1 "crear" = 6, 400).
        msg = f"quedarían {len(body_items)} precios por cantidad, ML permite máximo {_ML_MAX_TIERS_PXQ} -- no se envió"
        log_ml_escritura(uid, sku, item_id, "mayorista_pxq", None, json.dumps(cambios, ensure_ascii=False), "salud_popup", "error", msg)
        return f"Mayorista ({item_id}): {msg}", []
    advertencias = [
        f"Mayorista ({item_id}) {d['quantity']}+: % pedido inválido ({d['pct_pedido']}) descartado, no se envió a ML"
        for d in descartados
    ]
    cambios_efectivos = {mpu: pct for mpu, pct in cambios.items() if mpu not in {d["quantity"] for d in descartados}}
    if not cambios_efectivos and not eliminar:
        return None, advertencias  # todo lo pedido se descartó por el piso de sanidad -- nada que escribir
    valor_nuevo = json.dumps(
        {"cambios": cambios_efectivos, "eliminados": sorted(eliminar)} if eliminar else cambios_efectivos,
        ensure_ascii=False,
    )
    resp = ml_write_price_per_quantity(token, item_id, body_items, version, remove_absolute_pxq=tiene_pxq_absoluto)
    post_detalle = f"status={resp.status_code} {resp.text[:300]}" if resp.status_code != 200 else None
    time.sleep(0.4)
    verify = ml_get_prices_with_version(token, item_id)
    verify_pct = {}
    if verify:
        for p in verify.get("price_per_quantity") or []:
            cond = p.get("conditions") or {}
            if cond.get("min_purchase_unit") is not None:
                verify_pct[cond["min_purchase_unit"]] = p.get("percentage")
    ok = bool(verify) and len(verify_pct) == len(body_items) and all(
        verify_pct.get(mpu) is not None and abs(verify_pct[mpu] - pct) < 0.05
        for mpu, pct in cambios_efectivos.items()
    )
    if ok:
        log_ml_escritura(uid, sku, item_id, "mayorista_pxq", None, valor_nuevo, "salud_popup", "ok", None)
        return None, advertencias
    detalle = post_detalle or f"GET de verificación no coincide (quedó {verify_pct!r})"
    log_ml_escritura(uid, sku, item_id, "mayorista_pxq", None, valor_nuevo, "salud_popup", "error", detalle)
    return f"Mayorista ({item_id}): {detalle}", advertencias


def build_tab_salud(container) -> None:
    container.clear()
    user = _require_login()
    if not user:
        return
    uid = user["id"]

    # Guard de condición de carrera para el refresh de stock en background (ver más abajo):
    # cada apertura/re-render de esta pestaña bumpea la "generación" guardada en el propio
    # `container` (que persiste entre llamadas, a diferencia de las variables locales). Un
    # refresh en vuelo de una generación anterior (cuenta distinta u otra apertura de la
    # pestaña) se detecta comparando contra esto antes de tocar filas_todas/_render -- así
    # nunca pisa la vista de la cuenta que Diego está mirando ahora.
    generacion = getattr(container, "_salud_generacion", 0) + 1
    container._salud_generacion = generacion

    filas_todas, snap_date = _build_rows(uid)
    ultima_corrida = _ultima_corrida_completa(uid)

    sort_ref: Dict[str, Any] = {"col": "sku", "asc": True}

    with container:
        with ui.column().classes("w-full gap-2 p-2"):
            with ui.row().classes("items-center gap-3 w-full"):
                ui.label("Salud").classes("text-xl font-bold")
                ui.space()
                if ultima_corrida:
                    ui.label(f"Última corrida completa: {ultima_corrida[:16].replace('T', ' ')}").classes("text-xs text-gray-500")
                elif snap_date:
                    ui.label(f"Último snapshot: {snap_date} (corrida manual, no vía cron)").classes("text-xs text-gray-500")
                else:
                    ui.label("Todavía no corrió la auditoría nocturna para esta cuenta.").classes("text-xs text-warning")

            if not filas_todas:
                ui.label(
                    "Sin datos. La corrida completa la dispara el cron nocturno "
                    "(salud_audit.py) -- todavía no generó ningún snapshot para esta cuenta."
                ).classes("text-sm text-gray-400")
                return

            marcas_disponibles = sorted({f["marca"] for f in filas_todas if f["marca"]})

            with ui.row().classes("items-center gap-3 flex-wrap w-full"):
                stock_sel = ui.select(
                    {"con_stock": "Con stock", "sin_stock": "Sin stock", "ambas": "Ambas"},
                    value="con_stock", label="Stock",
                ).props("dense outlined").classes("w-36")
                marca_sel = ui.select(
                    {"": "Todas", **{m: m for m in marcas_disponibles}},
                    value="", label="Marca",
                ).props("dense outlined").classes("w-44")
                buscador = ui.input(placeholder="Buscar por SKU o producto...").props(
                    "dense outlined clearable debounce=300"
                ).classes("w-64")

            contador_lbl = ui.label("").classes("text-xs text-gray-500")
            indicador_stock = ui.label("Actualizando stock…").classes("text-xs").style(f"color:{_MID}")
            indicador_stock.set_visibility(False)

            header_div = ui.element("div").style("width:100%;overflow:hidden")
            table_container = ui.element("div").style("width:100%;height:calc(100vh - 320px);overflow-y:scroll;overflow-x:auto")
            _hid, _cid = header_div.id, table_container.id

            async def _sync_scroll() -> None:
                await ui.run_javascript(
                    f"(function(){{"
                    f"var body=document.getElementById('c{_cid}');"
                    f"var hdr=document.getElementById('c{_hid}');"
                    f"if(!body||!hdr)return;"
                    f"body.addEventListener('scroll',function(){{hdr.scrollLeft=body.scrollLeft;}});"
                    f"function _sg(){{hdr.style.paddingRight=(body.offsetWidth-body.clientWidth)+'px';}}"
                    f"_sg();new ResizeObserver(_sg).observe(body);"
                    f"}})();"
                )
            ui.timer(0.1, _sync_scroll, once=True)

            def _colgroup() -> None:
                with ui.element("colgroup"):
                    for col in _COLUMNS:
                        ui.element("col").style(f"width:{col['w']}")

            def _on_sort(col: str) -> None:
                if sort_ref["col"] == col:
                    sort_ref["asc"] = not sort_ref["asc"]
                else:
                    sort_ref["col"] = col
                    sort_ref["asc"] = True
                _render()

            async def _abrir_popup(sku: str) -> None:
                row_actual = next((f for f in filas_todas if f["sku"] == sku), None)
                if not row_actual:
                    return

                with ui.dialog().props("persistent") as dlg, ui.card().classes("w-[900px] max-w-full gap-2"):
                    dlg.open()
                    with ui.row().classes("items-center gap-2 w-full") as header_row:
                        ui.label(f"{sku} — {row_actual['producto'] or ''}").classes("text-lg font-bold")
                        ui.space()
                        ui.spinner(size="lg")
                    with ui.column().classes("w-full gap-2") as body:
                        ui.label("Cargando diagnóstico en vivo (multiget + description + prices + performance por publicación)...").classes("text-sm text-gray-400")

                    token = get_ml_access_token(uid)
                    if not token:
                        header_row.clear()
                        body.clear()
                        with body:
                            ui.label("No se pudo obtener el token de MercadoLibre.").classes("text-negative text-sm")
                        with header_row:
                            ui.label(sku).classes("text-lg font-bold")
                            ui.space()
                            ui.button("Cerrar", on_click=dlg.close).props("flat")
                        return

                    seller_id = await run.io_bound(ml_get_user_id, token)
                    resultado = await run.io_bound(audit_sku, uid, seller_id or "", sku, True)

                    header_row.clear()
                    with header_row:
                        ui.label(f"{sku} — {row_actual['producto'] or ''}").classes("text-lg font-bold")

                    if resultado.get("error"):
                        body.clear()
                        with body:
                            ui.label(f"No se pudo auditar este SKU: {resultado['error']}").classes("text-negative text-sm")
                        with header_row:
                            ui.space()
                            ui.button("Cerrar", on_click=dlg.close).props("flat")
                        return

                    def _aplicar_resultado_a_fila(resultado_audit: Dict[str, Any]) -> None:
                        prod_meta_single = {sku: {
                            "nombre": row_actual["producto"], "marca": row_actual["marca"], "stock": row_actual["stock"],
                        }}
                        nueva_fila = _sku_summary(sku, [r["audit"] for r in resultado_audit["items"]], prod_meta_single)
                        for idx, f in enumerate(filas_todas):
                            if f["sku"] == sku:
                                filas_todas[idx] = nueva_fila
                                break
                        _render()

                    # Guarda el audit más reciente para aplicarlo a la fila recién al CERRAR el
                    # diálogo -- nunca mientras está abierto. BUG 2026-09-04 (VERSION .15):
                    # llamar _aplicar_resultado_a_fila() (que dispara _render() -> table_container
                    # .clear()) apenas terminaba este audit_sku() cerraba el popup solo -- el
                    # diálogo, creado dentro del handler de click de la fila, queda anidado en el
                    # slot de table_container (patrón normal de NiceGUI para diálogos on-demand),
                    # así que limpiar table_container borraba el propio diálogo de la vista.
                    # _guardar() actualiza este holder con el audit post-guardado (resultado2)
                    # cuando corresponde; el botón "Cancelar" lo aplica recién después de
                    # dlg.close(), momento en que ya no importa tocar table_container.
                    cierre_ref: Dict[str, Any] = {"resultado": resultado}

                    clasif = await run.io_bound(_clasificar_hallazgos, token, resultado["items"])

                    groq_key = get_app_config("groq_api_key")
                    items_crudos = [r["item"] for r in resultado["items"]]
                    contexto_ia = await run.io_bound(_contexto_producto, items_crudos, row_actual["marca"])
                    cat_id_ia = _item_principal(items_crudos).get("category_id")
                    cat_attrs_ia = await run.io_bound(_fetch_category_attrs, cat_id_ia) if cat_id_ia else []
                    cat_attrs_by_id = {a["id"]: a for a in cat_attrs_ia if a.get("id")}

                    decision_editable = clasif["decision"]
                    grupos_dec = _consolidar(decision_editable)

                    mayorista_eval: Dict[str, Dict[str, Any]] = {}
                    for it_body in items_crudos:
                        if it_body.get("listing_type_id") != "gold_special":
                            continue
                        ev = await run.io_bound(_evaluar_mayorista_gold_special, token, seller_id or "", it_body)
                        if ev:
                            mayorista_eval[it_body["id"]] = {"descriptor": _item_descriptor(it_body), **ev}

                    inputs: Dict[str, tuple] = {}
                    mayorista_tildes: Dict[str, Dict[int, bool]] = {}
                    # Cantidades tildadas para SACAR del array de price-per-quantity, para
                    # hacer lugar bajo el tope real de ML (5 -- ver FIX A, 2026-09-07). Un
                    # tier tildado acá queda mutuamente excluyente con mayorista_tildes: si
                    # el usuario tilda "eliminar" en uno que también tenía "corregir"
                    # tildado, el toggle de eliminar lo destilda (ver _on_toggle_eliminar).
                    mayorista_eliminar: Dict[str, set] = {}
                    # Recalculado en cada _render_item() -- True si el estado actual de
                    # tildes/eliminar de ese ítem superaría el tope de 5; _guardar() lo usa
                    # para no mandar nada de ese ítem a ML (ver banner ⛔ en el render).
                    mayorista_sobre_tope: Dict[str, bool] = {}

                    def _render_campo(g: Dict[str, Any], seccion: str) -> _CampoWidget:
                        attr_def = cat_attrs_by_id.get(g.get("attr_id")) if g["tipo"] == "atributo" else None
                        tipo_campo = _tipo_campo(attr_def) if g["tipo"] == "atributo" else "free"
                        valor_inicial = g["valor_sugerido"] if seccion == "sugerido" else ""
                        placeholder = None if seccion == "sugerido" else "(vacío = no tocar)"
                        attr_id = g.get("attr_id")

                        with ui.column().classes("w-full gap-0"):
                            with ui.row().classes("items-center gap-2 w-full"):
                                ui.label(g["campo"]).classes("text-xs w-56")

                                if g["tipo"] == "descripcion":
                                    inp = ui.textarea(
                                        value=valor_inicial, placeholder=placeholder,
                                    ).props("dense outlined").classes("flex-grow").style("min-height:110px")
                                    campo = _CampoWidget(
                                        tiene_valor=lambda inp=inp: bool((inp.value or "").strip()),
                                        payload=lambda: None,
                                        display=lambda inp=inp: (inp.value or "").strip(),
                                        set_texto=lambda texto, inp=inp: (setattr(inp, "value", texto), True)[1],
                                    )

                                elif tipo_campo == "closed":
                                    # boolean/list: NUNCA texto libre -- ML exige value_id de un
                                    # conjunto cerrado de verdad (confirmado contra la doc oficial).
                                    opciones = {v.get("id"): v.get("name") for v in _valores_de(attr_def) if v.get("id") and v.get("name")}
                                    default_id = _match_valor_id(attr_def, valor_inicial) if valor_inicial else None
                                    sel = ui.select(opciones, value=default_id, with_input=True).props("dense outlined").classes("flex-grow")

                                    def _set_texto_closed(texto, sel=sel, attr_def=attr_def):
                                        vid = _match_valor_id(attr_def, texto)
                                        if vid:
                                            sel.value = vid
                                            return True
                                        return False

                                    campo = _CampoWidget(
                                        tiene_valor=lambda sel=sel: bool(sel.value),
                                        payload=lambda sel=sel, attr_id=attr_id: {"id": attr_id, "value_id": sel.value} if sel.value else None,
                                        display=lambda sel=sel, opciones=opciones: opciones.get(sel.value, ""),
                                        set_texto=_set_texto_closed,
                                    )

                                elif tipo_campo == "number_unit":
                                    unidades = [u.get("id") for u in (attr_def.get("allowed_units") or []) if u.get("id")]
                                    numero_ini, unidad_parsed = _parse_number_unit(valor_inicial, attr_def) if valor_inicial else ("", "")
                                    # ui.select revienta con ValueError si `value` no está entre las
                                    # opciones -- unidad_parsed puede no coincidir con `unidades` si
                                    # default_unit viniera mal cargado en ML; nunca confiar ciego.
                                    unidad_ini = unidad_parsed if unidad_parsed in unidades else (unidades[0] if unidades else None)
                                    with ui.row().classes("flex-grow gap-2 items-center no-wrap"):
                                        numero_inp = ui.input(value=numero_ini, placeholder=placeholder or "número").props("dense outlined").classes("w-24")
                                        unidad_sel = ui.select({u: u for u in unidades}, value=unidad_ini).props("dense outlined").classes("w-24")

                                    def _set_texto_nu(texto, numero_inp=numero_inp, unidad_sel=unidad_sel, attr_def=attr_def):
                                        n, u = _parse_number_unit(texto, attr_def)
                                        if not n:
                                            return False
                                        numero_inp.value = n
                                        matched = bool(u) and _norm(u) in _norm(texto)
                                        if u:
                                            unidad_sel.value = u
                                        return matched

                                    campo = _CampoWidget(
                                        tiene_valor=lambda numero_inp=numero_inp: bool((numero_inp.value or "").strip()),
                                        payload=lambda numero_inp=numero_inp, unidad_sel=unidad_sel, attr_id=attr_id: (
                                            {"id": attr_id, "value_name": f"{(numero_inp.value or '').strip()} {unidad_sel.value}"}
                                            if (numero_inp.value or "").strip() else None
                                        ),
                                        display=lambda numero_inp=numero_inp, unidad_sel=unidad_sel: f"{(numero_inp.value or '').strip()} {unidad_sel.value}".strip(),
                                        set_texto=_set_texto_nu,
                                    )

                                elif tipo_campo == "multivalued":
                                    # string/number con values[] + tag multivalued (LANGUAGES,
                                    # FUNCTIONS, SMARTWATCH_FUNCTIONS): igual que "closed", nunca
                                    # texto libre -- cada opción tiene que venir de values[].
                                    opciones_mv = {v.get("name"): v.get("name") for v in _valores_de(attr_def) if v.get("name")}
                                    default_list: List[str] = []
                                    if valor_inicial:
                                        for parte in valor_inicial.split(","):
                                            nombre = _match_valor_nombre(attr_def, parte)
                                            if nombre and nombre not in default_list:
                                                default_list.append(nombre)
                                    sel = ui.select(
                                        opciones_mv, value=default_list, multiple=True, with_input=True,
                                    ).props("dense outlined use-chips").classes("flex-grow")

                                    def _set_texto_mv(texto, sel=sel, attr_def=attr_def):
                                        partes = [p for p in (texto or "").split(",") if p.strip()]
                                        matched: List[str] = []
                                        todas_ok = bool(partes)
                                        for p in partes:
                                            nombre = _match_valor_nombre(attr_def, p)
                                            if nombre:
                                                if nombre not in matched:
                                                    matched.append(nombre)
                                            else:
                                                todas_ok = False
                                        sel.value = matched
                                        return todas_ok and bool(matched)

                                    campo = _CampoWidget(
                                        tiene_valor=lambda sel=sel: bool(sel.value),
                                        payload=lambda sel=sel, attr_id=attr_id: {"id": attr_id, "value_name": ", ".join(sel.value)} if sel.value else None,
                                        display=lambda sel=sel: ", ".join(sel.value or []),
                                        set_texto=_set_texto_mv,
                                    )

                                elif tipo_campo == "closed_or_free":
                                    # string/number con values[] pero sin multivalued (ej.
                                    # OS_VERSION): ML tolera un value_name nuevo (doc: "para el
                                    # caso de nuevos valores basta con enviar únicamente el name"),
                                    # así que el select permite elegir una opción real O escribir
                                    # una nueva -- nunca inventa por su cuenta, eso lo decide Diego.
                                    opciones_cf = {v.get("name"): v.get("name") for v in _valores_de(attr_def) if v.get("name")}
                                    default_cf = None
                                    if valor_inicial:
                                        default_cf = _match_valor_nombre(attr_def, valor_inicial) or valor_inicial
                                        opciones_cf.setdefault(default_cf, default_cf)
                                    sel = ui.select(
                                        opciones_cf, value=default_cf, with_input=True, new_value_mode="add-unique",
                                    ).props("dense outlined").classes("flex-grow")

                                    def _payload_cf(sel=sel, attr_def=attr_def, attr_id=attr_id):
                                        if not sel.value:
                                            return None
                                        vid = _match_valor_id(attr_def, sel.value)
                                        return {"id": attr_id, "value_id": vid} if vid else {"id": attr_id, "value_name": sel.value}

                                    def _set_texto_cf(texto, sel=sel, attr_def=attr_def):
                                        if not texto:
                                            return False
                                        canon = _match_valor_nombre(attr_def, texto) or texto
                                        if canon not in sel.options:
                                            # value=<no registrada en options> no rompe (ValueError solo
                                            # se dispara en el constructor), pero queda "invisible" en el
                                            # dropdown -- se registra antes para que se vea seleccionada.
                                            sel.options[canon] = canon
                                            sel.update()
                                        sel.value = canon
                                        return True

                                    campo = _CampoWidget(
                                        tiene_valor=lambda sel=sel: bool(sel.value),
                                        payload=_payload_cf,
                                        display=lambda sel=sel: sel.value or "",
                                        set_texto=_set_texto_cf,
                                    )

                                else:  # "free" -- string/number sin values[], o sin attr_def: sin cambios.
                                    inp = ui.input(value=valor_inicial, placeholder=placeholder).props("dense outlined").classes("flex-grow")
                                    campo = _CampoWidget(
                                        tiene_valor=lambda inp=inp: bool((inp.value or "").strip()),
                                        payload=lambda inp=inp, attr_id=attr_id: (
                                            {"id": attr_id, "value_name": (inp.value or "").strip()} if (inp.value or "").strip() else None
                                        ),
                                        display=lambda inp=inp: (inp.value or "").strip(),
                                        set_texto=lambda texto, inp=inp: (setattr(inp, "value", texto), True)[1],
                                    )

                                marca_ia = ui.label("✨ sugerido por IA, sin verificar").classes("text-xs").style(f"color:{_MID}")
                                marca_ia.set_visibility(False)
                                if _con_boton_ia(g, seccion):
                                    async def _click_ia(g=g, campo=campo, marca_ia=marca_ia, attr_def=attr_def) -> None:
                                        if not groq_key:
                                            ui.notify("Configurá tu API key de Groq en Config → IA/Sugerencias", color="warning")
                                            return
                                        try:
                                            texto = await run.io_bound(_groq_generate, groq_key, _prompt_ia(g, contexto_ia, attr_def))
                                        except Exception as exc:
                                            ui.notify(f"Error al pedir sugerencia a la IA: {exc}", color="negative")
                                            return
                                        if texto.strip().upper() == "VACIO":
                                            marca_ia.set_text("✨ la IA no encontró un valor confiable -- dejalo vacío o completalo a mano")
                                            marca_ia.style(f"color:{_MID}")
                                            marca_ia.set_visibility(True)
                                            return
                                        ok = campo.set_texto(texto)
                                        if g["tipo"] == "atributo" and not ok:
                                            marca_ia.set_text("✨ sugerido por IA -- no coincide con una opción válida de ML, revisar antes de guardar")
                                            marca_ia.style(f"color:{_BAD}")
                                        else:
                                            marca_ia.set_text("✨ sugerido por IA, sin verificar")
                                            marca_ia.style(f"color:{_MID}")
                                        marca_ia.set_visibility(True)
                                    ui.button(icon="auto_awesome", on_click=_click_ia).props("flat dense round size=sm").tooltip("Sugerir con IA")
                            ui.label(_aplica_a_texto(g["items"])).classes("text-xs text-gray-400 pl-1")
                        return campo

                    body.clear()
                    with body:
                        ui.label(f"{len(resultado['items'])} publicaciones · datos actualizados recién ahora").classes("text-xs text-gray-500")

                        n_normal = clasif["normal"]["count"]
                        if n_normal:
                            etiqueta = f"ℹ️ {n_normal} elemento{'s' if n_normal != 1 else ''} no editable{'s' if n_normal != 1 else ''} por diseño"
                            with ui.expansion(etiqueta, value=False).classes("w-full text-sm"):
                                ui.label(_NORMAL_POR_DISENO_INTRO).classes("text-xs text-gray-400 italic mb-1")
                                for txt in clasif["normal"]["lineas"]:
                                    ui.label(f"• {txt}").classes("text-xs text-gray-500")

                        grupos_sug = _consolidar(clasif["sugeridos"])
                        if grupos_sug:
                            ui.label(f"✏️ Sugerido — revisar y confirmar ({len(grupos_sug)})").classes("font-semibold text-sm mt-2")
                            for i, g in enumerate(grupos_sug):
                                campo = _render_campo(g, "sugerido")
                                inputs[f"sug_{i}"] = (g, campo)

                        if clasif["decision"]:
                            ui.label(f"❓ Necesita tu decisión ({len(grupos_dec)})").classes("font-semibold text-sm mt-2")
                            for i, g in enumerate(grupos_dec):
                                campo = _render_campo(g, "decision")
                                inputs[f"dec_{i}"] = (g, campo)

                        # Opcionales (tags.required != true) -- sección propia, colapsada por
                        # default: mejora de SEO/ficha técnica, nunca un problema. No entran a
                        # grupos_sug/grupos_dec ni al chequeo de "Sin hallazgos accionables" de
                        # abajo -- completarlos es a discreción, no algo que el SKU necesite.
                        grupos_opc = _consolidar(clasif["opcionales"])
                        if grupos_opc:
                            with ui.expansion(f"🔧 Opcionales — SEO / calidad, no obligatorios ({len(grupos_opc)})", value=False).classes("w-full text-sm mt-2"):
                                for i, g in enumerate(grupos_opc):
                                    seccion = "sugerido" if g["valor_sugerido"] else "decision"
                                    campo = _render_campo(g, seccion)
                                    inputs[f"opc_{i}"] = (g, campo)

                        _ESTADO_COLOR = {"crear": _OK, "roto": _BAD, "revisar": _MID, "ok": _GREY, "bloqueada": _MID}
                        if mayorista_eval:
                            ui.label(f"💰 Mayorista (contado) — {len(mayorista_eval)} publicación(es)").classes("font-semibold text-sm mt-2")
                            for item_id, ev in mayorista_eval.items():
                                mayorista_tildes[item_id] = {
                                    t["quantity"]: t["estado"] in ("crear", "roto")
                                    for t in ev["tiers"] if t["estado"] in ("crear", "roto", "revisar")
                                }
                                mayorista_eliminar[item_id] = set()
                                item_box = ui.column().classes("w-full gap-0 border rounded p-2")

                                def _render_item(item_id=item_id, ev=ev, item_box=item_box):
                                    tildes = mayorista_tildes[item_id]
                                    elim = mayorista_eliminar[item_id]
                                    incluir = {q for q, v in tildes.items() if v and q not in elim}
                                    cambios, bloqueadas, conflictos = _tiers_plan(ev, incluir, elim)
                                    conflicto_por_qty = {c["quantity"]: c for c in conflictos}
                                    # Cantidades que hoy tienen ALGO cargado (legacy o % nuevo) --
                                    # exactamente lo que _construir_payload_mayorista preserva vía
                                    # `vistos` si no se elimina. Cualquier `cambios[q]` que no esté
                                    # acá es un tier "crear" nuevo: suma una entrada más al array.
                                    cargado_qtys = {t["quantity"] for t in ev["tiers"] if t.get("pct_cargado") is not None}
                                    nuevas_qtys = {q for q in cambios if q not in cargado_qtys}
                                    total_resultante = len(cargado_qtys - elim) + len(nuevas_qtys)
                                    sobre_tope = total_resultante > _ML_MAX_TIERS_PXQ
                                    mayorista_sobre_tope[item_id] = sobre_tope
                                    item_box.clear()
                                    with item_box:
                                        ui.label(f"{item_id} ({ev['descriptor']}) — precio contado ${_fmt_moneda(ev['precio_base'])}").classes("text-xs font-medium")
                                        if ev["invertido"]:
                                            ui.label(
                                                "⚠️ tiers cargados en orden invertido (una cantidad mayor cuesta más "
                                                "por unidad que una menor) — revisar manualmente, sin corrección automática"
                                            ).classes("text-xs pl-3").style(f"color:{_BAD}")
                                        if sobre_tope:
                                            ui.label(
                                                f"⛔ ML permite máximo {_ML_MAX_TIERS_PXQ} precios por cantidad — hoy tenés "
                                                f"{len(cargado_qtys)} cargados, esto sumaría {total_resultante}. Tildá "
                                                f"\"eliminar\" en alguno de los tiers de abajo para hacer lugar antes de guardar "
                                                f"(no se guarda nada de este ítem hasta que baje de {_ML_MAX_TIERS_PXQ})."
                                            ).classes("text-xs pl-3 font-semibold").style(f"color:{_BAD}")

                                        def _chk_eliminar(q: int):
                                            # Fila propia (no sibling del checkbox de "corregir") -- con los 4
                                            # elementos en una sola fila, un texto largo de tier hacía wrappear
                                            # el layout y el checkbox de "corregir" terminaba visualmente
                                            # separado de su propio texto (confirmado en vivo 2026-09-07,
                                            # MLA3684456394, tier 10+). Separando en dos filas queda inequívoco
                                            # cuál checkbox es cuál sin importar el largo del texto.
                                            with ui.row().classes("items-center gap-1 pl-6"):
                                                chk_e = ui.checkbox(value=q in elim).props("dense size=sm")
                                                ui.label("eliminar").classes("text-xs").style(f"color:{_BAD}")

                                                def _on_toggle_elim(e, item_id=item_id, q=q):
                                                    if e.value:
                                                        mayorista_eliminar[item_id].add(q)
                                                        mayorista_tildes[item_id][q] = False  # mutuamente excluyente con "corregir"
                                                    else:
                                                        mayorista_eliminar[item_id].discard(q)
                                                    _render_item()
                                                chk_e.on_value_change(_on_toggle_elim)

                                        for t in ev["tiers"]:
                                            q = t["quantity"]
                                            estado = t["estado"]
                                            sufijo_qty = f"{q}+" + (" (cantidad no estándar)" if t.get("extra") else "")

                                            if q in elim:
                                                txt = f"{sufijo_qty} unidades: tildado para ELIMINAR — hoy ${_fmt_moneda(t.get('monto_cargado'))} ({t.get('pct_cargado')}% off)"
                                                with ui.row().classes("items-center gap-1 pl-3"):
                                                    chk_e = ui.checkbox(value=True).props("dense size=sm")
                                                    ui.label(txt).classes("text-xs").style(f"color:{_BAD}")

                                                def _on_toggle_elim(e, item_id=item_id, q=q):
                                                    if not e.value:
                                                        mayorista_eliminar[item_id].discard(q)
                                                    _render_item()
                                                chk_e.on_value_change(_on_toggle_elim)
                                                continue
                                            if q in conflicto_por_qty:
                                                c = conflicto_por_qty[q]
                                                txt = (
                                                    f"{sufijo_qty} unidades: no se puede {('corregir' if estado == 'roto' else 'crear')} sin quedar "
                                                    f"incoherente con el tier de {c['conflicto_con']}+ ({c['techo_pct']}% off), que no está tildado — "
                                                    f"tildá también {c['conflicto_con']}+ para poder guardar juntos"
                                                )
                                                ui.label(txt).classes("text-xs pl-3").style(f"color:{_ESTADO_COLOR['bloqueada']}")
                                                continue
                                            if q in bloqueadas:
                                                txt = (
                                                    f"{sufijo_qty} unidades: no se puede {('corregir' if estado == 'roto' else 'crear')} sin quedar "
                                                    f"incoherente con un tier existente en una cantidad menor (ML exige % no decreciente) — revisar a mano"
                                                )
                                                ui.label(txt).classes("text-xs pl-3").style(f"color:{_ESTADO_COLOR['bloqueada']}")
                                                continue
                                            if estado == "ok":
                                                txt = f"{sufijo_qty} unidades: ok — ${_fmt_moneda(t['monto_cargado'])} ({t['pct_cargado']}% off)"
                                                ui.label(txt).classes("text-xs pl-3").style(f"color:{_ESTADO_COLOR['ok']}")
                                                _chk_eliminar(q)
                                                continue
                                            if estado not in ("crear", "roto", "revisar"):
                                                continue
                                            marcado = tildes.get(q, False)
                                            aplica = marcado and q in cambios
                                            monto_sugerido = round(ev["precio_base"] * (1 - t["pct_calculado"] / 100), 2)
                                            if aplica:
                                                pct_final = cambios[q]
                                                monto_final = round(ev["precio_base"] * (1 - pct_final / 100), 2)
                                                ajuste = "" if pct_final == t["pct_calculado"] else f" (ajustado de {t['pct_calculado']}% para no quedar por debajo de un tier existente)"
                                            if estado == "crear":
                                                txt = (f"{sufijo_qty} unidades: crear → ${_fmt_moneda(monto_final)} ({pct_final}% off){ajuste}" if aplica
                                                       else f"{sufijo_qty} unidades: sugerido crear ${_fmt_moneda(monto_sugerido)} ({t['pct_calculado']}%), sin tildar")
                                            elif estado == "roto":
                                                txt = (f"{sufijo_qty} unidades: ROTO — cargado ${_fmt_moneda(t['monto_cargado'])} ({t['pct_cargado']}%) → corregir a ${_fmt_moneda(monto_final)} ({pct_final}%){ajuste}" if aplica
                                                       else f"{sufijo_qty} unidades: ROTO — cargado ${_fmt_moneda(t['monto_cargado'])} ({t['pct_cargado']}%), sugerido ${_fmt_moneda(monto_sugerido)} ({t['pct_calculado']}%), sin tildar")
                                            else:  # revisar
                                                txt = (f"{sufijo_qty} unidades: revisar → corregir a ${_fmt_moneda(monto_final)} ({pct_final}%) (cargado ${_fmt_moneda(t['monto_cargado'])}, {t['pct_cargado']}%){ajuste}" if aplica
                                                       else f"{sufijo_qty} unidades: revisar — cargado ${_fmt_moneda(t['monto_cargado'])} ({t['pct_cargado']}%) vs. sugerido ${_fmt_moneda(monto_sugerido)} ({t['pct_calculado']}%)")
                                            with ui.row().classes("items-center gap-1 pl-3"):
                                                chk = ui.checkbox(value=marcado)
                                                ui.label(txt).classes("text-xs").style(f"color:{_ESTADO_COLOR[estado]}")
                                            if estado in ("roto", "revisar"):  # ya cargado -- también se puede eliminar en vez de corregir
                                                _chk_eliminar(q)

                                            def _on_toggle(e, item_id=item_id, q=q):
                                                mayorista_tildes[item_id][q] = e.value
                                                if e.value:
                                                    mayorista_eliminar[item_id].discard(q)  # mutuamente excluyente con "eliminar"
                                                _render_item()
                                            chk.on_value_change(_on_toggle)

                                _render_item()

                        if not clasif["sugeridos"] and not decision_editable and not mayorista_eval:
                            ui.label("Sin hallazgos accionables -- este SKU está al día.").classes("text-sm").style(f"color:{_OK}")

                        resumen_area = ui.column().classes("w-full gap-1")

                    async def _guardar() -> None:
                        guardar_btn.props("loading")
                        errores: List[str] = []
                        advertencias: List[str] = []
                        aplicados = 0
                        for g, campo in inputs.values():
                            if not campo.tiene_valor():
                                continue
                            for it in g["items"]:
                                if g["tipo"] == "atributo":
                                    payload = campo.payload()
                                    if not payload:
                                        continue
                                    err = await run.io_bound(
                                        _escribir_atributo, token, uid, sku, it["item_id"], g["attr_id"], g["campo"],
                                        None, payload, campo.display(),
                                    )
                                elif g["tipo"] == "descripcion":
                                    err = await run.io_bound(
                                        _escribir_descripcion, token, uid, sku, it["item_id"], 0, campo.display(),
                                    )
                                else:
                                    continue
                                if err:
                                    errores.append(err)
                                else:
                                    aplicados += 1

                        for item_id, ev in mayorista_eval.items():
                            if mayorista_sobre_tope.get(item_id):
                                # Backstop: el banner ⛔ del render ya explica por qué -- acá
                                # solo nos aseguramos de no mandar nada de este ítem a ML
                                # mientras siga por encima del tope de 5 (ver FIX A).
                                errores.append(
                                    f"Mayorista ({item_id}): sin guardar -- por encima del tope de "
                                    f"{_ML_MAX_TIERS_PXQ} precios por cantidad, tildá \"eliminar\" en algún tier primero"
                                )
                                continue
                            elim = mayorista_eliminar.get(item_id) or set()
                            incluir = {q for q, v in mayorista_tildes.get(item_id, {}).items() if v and q not in elim}
                            if not incluir and not elim:
                                continue
                            cambios, _bloqueadas, _conflictos = _tiers_plan(ev, incluir, elim)
                            if not cambios and not elim:
                                continue
                            err, adv = await run.io_bound(
                                _escribir_mayorista_pxq, token, uid, sku, item_id, cambios, elim,
                            )
                            advertencias.extend(adv)
                            if err:
                                errores.append(err)
                            else:
                                aplicados += 1

                        resumen_area.clear()
                        with resumen_area:
                            ui.separator()
                            if aplicados:
                                ui.label(f"✅ {aplicados} campo(s) aplicados y verificados").style(f"color:{_OK}").classes("text-sm")
                            for e in errores:
                                ui.label(f"❌ {e}").style(f"color:{_BAD}").classes("text-xs")
                            for a in advertencias:
                                ui.label(f"⚠️ {a}").style(f"color:{_MID}").classes("text-xs")
                            if not aplicados and not errores and not advertencias:
                                ui.label("No se marcó ningún campo para guardar.").classes("text-xs text-gray-500")

                        if aplicados:
                            # Acá SÍ hace falta releer ML -- el audit de la apertura del popup
                            # (resultado) quedó desactualizado por la escritura que se acaba de
                            # hacer.
                            resultado2 = await run.io_bound(audit_sku, uid, seller_id or "", sku, True)
                            if not resultado2.get("error"):
                                cierre_ref["resultado"] = resultado2
                        guardar_btn.props(remove="loading")

                        if aplicados and not errores:
                            # Guardado exitoso (sin errores) -- cierra solo, como siempre fue el
                            # comportamiento esperado. Si hubo algún error se deja el popup abierto
                            # mostrando el resumen (✅/❌/⚠️) para que el usuario lo vea antes de
                            # cerrar manualmente con "Cancelar". Recién acá se toca table_container
                            # (vía _aplicar_resultado_a_fila -> _render()), con el diálogo ya
                            # cerrándose -- no antes, para no repetir el bug de auto-cierre en
                            # apertura (ver dfa63a8/9da0237).
                            dlg.close()
                            _aplicar_resultado_a_fila(cierre_ref["resultado"])

                    def _cerrar_dialogo() -> None:
                        dlg.close()
                        _aplicar_resultado_a_fila(cierre_ref["resultado"])

                    with ui.row().classes("justify-end gap-2 w-full mt-2"):
                        ui.button("Cancelar", on_click=_cerrar_dialogo).props("flat")
                        guardar_btn = ui.button("Guardar", on_click=_guardar).props("color=primary")

            def _render() -> None:
                stock_filtro = stock_sel.value
                if stock_filtro == "con_stock":
                    visibles = [f for f in filas_todas if (f["stock"] or 0) > 0]
                elif stock_filtro == "sin_stock":
                    visibles = [f for f in filas_todas if not (f["stock"] or 0) > 0]
                else:
                    visibles = list(filas_todas)

                marca_filtro = marca_sel.value
                if marca_filtro:
                    visibles = [f for f in visibles if f["marca"] == marca_filtro]

                busq = (buscador.value or "").strip().lower()
                if busq:
                    visibles = [f for f in visibles if busq in f["sku"].lower() or busq in (f["producto"] or "").lower()]

                visibles = sorted(visibles, key=lambda r: _sort_key(r, sort_ref["col"]), reverse=not sort_ref["asc"])

                contador_lbl.set_text(
                    f"mostrando {len(visibles)} de {len(filas_todas)} · 👤 publicación propia · 🏬 publicación catálogo"
                )

                header_div.clear()
                table_container.clear()
                if not visibles:
                    with table_container:
                        ui.label("Sin resultados para este filtro.").classes("text-sm text-gray-400")
                    return

                with header_div:
                    with ui.element("table").style("table-layout:fixed;width:100%;border-collapse:separate;border-spacing:0;font-size:11px"):
                        _colgroup()
                        with ui.element("thead"):
                            with ui.element("tr").classes("bg-primary text-white font-semibold"):
                                for col in _COLUMNS:
                                    with ui.element("th").classes("px-2 py-1 border text-center").style("line-height:1.1"):
                                        if col.get("sortable", True):
                                            ui.button(
                                                col["label"], on_click=lambda c=col["name"]: _on_sort(c)
                                            ).props("flat dense no-caps").classes(
                                                "text-white hover:bg-white/20 cursor-pointer font-semibold"
                                            ).style(
                                                "white-space:nowrap;overflow:hidden;text-overflow:ellipsis;"
                                                "max-width:100%;min-height:0;padding:2px 6px;line-height:1.1;font-size:11px"
                                            )
                                        else:
                                            ui.label(col["label"]).classes("font-semibold").style("line-height:1.1")

                with table_container:
                    with ui.element("table").style("table-layout:fixed;width:100%;border-collapse:separate;border-spacing:0;font-size:11px"):
                        _colgroup()
                        with ui.element("tbody"):
                            for row in visibles:
                                with ui.element("tr").classes("border-t border-gray-200 hover:bg-gray-50"):
                                    for col in _COLUMNS:
                                        align = "text-right" if col["align"] == "right" else "text-center" if col["align"] == "center" else "text-left"
                                        with ui.element("td").classes(f"px-2 py-1 border-b border-gray-100 {align}").style("white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:0"):
                                            name = col["name"]
                                            if name == "sku":
                                                ui.label(row["sku"]).classes("font-medium")
                                            elif name == "producto":
                                                ui.label(row["producto"] or "—").classes(
                                                    "cursor-pointer text-blue-700 hover:underline"
                                                ).on("click", lambda s=row["sku"]: _abrir_popup(s))
                                            elif name == "marca":
                                                ui.label(row["marca"] or "—")
                                            elif name == "precio":
                                                v = row.get("precio_min")
                                                ui.label(_fmt_moneda(v) if v is not None else "—")
                                            elif name == "stock":
                                                v = row.get("stock")
                                                ui.label(str(v) if v is not None else "—")
                                            elif name == "variantes":
                                                ui.label(str(row["n_items"]))
                                            elif name == "regulatoria":
                                                ui.label(row["regulatoria_texto"]).style(f"color:{_GREY}")
                                            elif name == "atributos_editables":
                                                v = row["atributos_editables_total"]
                                                ui.label(str(v) if v is not None else "—")
                                            elif name == "puntaje_ml":
                                                v = row["puntaje_ml"]
                                                ui.label(str(v) if v is not None else "—")
                                            elif name == "gtin":
                                                d = row["dims"].get("gtin")
                                                if not d or (d["propias_total"] == 0 and d["catalogo_total"] == 0):
                                                    ui.label("—")
                                                else:
                                                    pt, po = d["propias_total"], d["propias_ok"]
                                                    ct, co = d["catalogo_total"], d["catalogo_ok"]
                                                    if pt == 0:
                                                        color_prop = _GREY
                                                    elif po == pt:
                                                        color_prop = _OK
                                                    elif po == 0:
                                                        color_prop = _BAD
                                                    else:
                                                        color_prop = _MID
                                                    tooltip = (
                                                        f"Propias: {po}/{pt} con GTIN (accionable) · "
                                                        f"Catálogo: {co}/{ct} con GTIN (informativo, ML no permite editarlo)"
                                                    )
                                                    with ui.column().classes("gap-0 items-center"):
                                                        with ui.row().classes("items-center gap-0.5") as fila_prop:
                                                            ui.icon("person", size="12px").style(f"color:{color_prop}")
                                                            ui.label(f"{po}/{pt}").classes("text-xs font-semibold").style(f"color:{color_prop}")
                                                        fila_prop.tooltip(tooltip)
                                                        if ct:
                                                            color_cat = _GREY if co == ct else _MID
                                                            with ui.row().classes("items-center gap-0.5") as fila_cat:
                                                                ui.icon("storefront", size="12px").style(f"color:{color_cat}")
                                                                ui.label(f"{co}/{ct}").classes("text-xs").style(f"color:{color_cat}")
                                                            fila_cat.tooltip(tooltip)
                                            elif name == "descripcion":
                                                d = row["dims"].get("descripcion")
                                                if not d or (d["propias_total"] == 0 and d["catalogo_total"] == 0):
                                                    ui.label("—")
                                                else:
                                                    pt, po = d["propias_total"], d["propias_ok"]
                                                    ct, co = d["catalogo_total"], d["catalogo_ok"]
                                                    if pt == 0:
                                                        color_prop = _GREY
                                                    elif po == pt:
                                                        color_prop = _OK
                                                    elif po == 0:
                                                        color_prop = _BAD
                                                    else:
                                                        color_prop = _MID
                                                    tooltip = (
                                                        f"Propias: {po}/{pt} con descripción (accionable) · "
                                                        f"Catálogo: {co}/{ct} con descripción (informativo, heredada del producto de catálogo)"
                                                    )
                                                    with ui.column().classes("gap-0 items-center"):
                                                        with ui.row().classes("items-center gap-0.5") as fila_prop:
                                                            ui.icon("person", size="12px").style(f"color:{color_prop}")
                                                            ui.label(f"{po}/{pt}").classes("text-xs font-semibold").style(f"color:{color_prop}")
                                                        fila_prop.tooltip(tooltip)
                                                        if ct:
                                                            color_cat = _GREY if co == ct else _MID
                                                            with ui.row().classes("items-center gap-0.5") as fila_cat:
                                                                ui.icon("storefront", size="12px").style(f"color:{color_cat}")
                                                                ui.label(f"{co}/{ct}").classes("text-xs").style(f"color:{color_cat}")
                                                            fila_cat.tooltip(tooltip)
                                            else:
                                                d = row["dims"].get(name)
                                                if d:
                                                    lbl = ui.label(d["texto"]).style(f"color:{d['color']};font-weight:600")
                                                    if d.get("tooltip"):
                                                        lbl.tooltip(d["tooltip"])
                                                else:
                                                    ui.label("—")

            stock_sel.on_value_change(lambda: _render())
            marca_sel.on_value_change(lambda: _render())
            buscador.on_value_change(lambda: _render())
            _render()

            async def _refrescar_stock_bg() -> None:
                """Corre en background apenas se termina de pintar la tabla con el dato
                stale -- nunca bloquea el render inicial. Acotado a `uid` (la cuenta que
                se abrió en ESTA llamada de build_tab_salud), nunca cruza cuentas."""
                indicador_stock.set_visibility(True)
                try:
                    stock_por_sku = await run.io_bound(_stock_fresco_sync, uid, snap_date)
                finally:
                    if getattr(container, "_salud_generacion", None) == generacion:
                        indicador_stock.set_visibility(False)
                if not stock_por_sku or getattr(container, "_salud_generacion", None) != generacion:
                    return  # cuenta activa cambió (otro login o se reabrió la pestaña) -- descartar
                cambio = False
                for f in filas_todas:
                    nuevo = stock_por_sku.get(f["sku"])
                    if nuevo is not None and nuevo != f["stock"]:
                        f["stock"] = nuevo
                        cambio = True
                if cambio:
                    _render()

            background_tasks.create(_refrescar_stock_bg(), name=f"salud_refrescar_stock_{uid}")
