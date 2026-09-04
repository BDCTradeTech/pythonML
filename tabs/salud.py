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
import math
import re
import time
from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional, Tuple

import requests
from nicegui import app, ui, run

from db import GROQ_MODEL, get_app_config, get_connection, log_ml_escritura
from ml_api import (
    get_ml_access_token,
    ml_get_item,
    ml_get_prices_with_version,
    ml_get_user_id,
    ml_update_item_attributes,
    ml_write_item_description,
    ml_write_price_per_quantity,
)
from salud_audit import audit_sku

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

    total = len(_QTYS_MAYORISTA)
    n = len(ok_qtys)
    color = _OK if n == total else (_BAD if n == 0 else _MID)
    texto = f"{n}/{total}"

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

    return {"texto": texto, "color": color, "orden": n / total, "tooltip": tooltip}


def _sku_summary(sku: str, items: List[dict], prod_meta: Dict[str, Any]) -> Dict[str, Any]:
    n_items = len(items)

    dims = {
        "gtin": _bool_dim(items, lambda it: bool(it.get("gtin"))),
        # Solo publicaciones propias (no catálogo): ML bloquea editar la descripción en
        # catalog_listing=True (ver _clasificar_hallazgos, "normal por diseño") -- contarlas
        # acá infla el denominador con huecos que no son un problema real (confirmado en vivo
        # 2026-09-04, Echo-Dot5-Kids-Stardust: daba "2/4" contando 2 ítems de catálogo sin
        # descripción -- normal -- como si fueran un hueco, cuando las 2 propias ya la tenían).
        # TODO(gap-membresia-grupo, 2026-09-04): quedan 3 gold_pro propias de este mismo SKU
        # sin descripción real (MLA3913903882, MLA3913903838, MLA2062899779) que todavía no
        # entran a ningún diagnóstico porque audit_sku() arranca de salud_item_snapshots
        # (gap de membresía del grupo, pendiente y separado) -- van a aparecer solas acá y en
        # el popup en cuanto ese gap se resuelva, sin tocar nada de esta dimensión.
        "descripcion": _bool_dim(
            [it for it in items if not it.get("catalog_listing")],
            lambda it: (it.get("descripcion_len") or 0) > 0 if it.get("descripcion_len") is not None else None,
        ),
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
    total_editables = sum(editables_vals) if editables_vals else None

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
    {"name": "atributos_editables", "label": "Características", "field": "atributos_editables", "align": "right", "w": "95px"},
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


def _clasificar_hallazgos(token: str, resultados: List[dict]) -> Dict[str, list]:
    """Separa los hallazgos crudos de audit_item() en 3 grupos (normal por diseño /
    sugerido con valor pre-cargado / necesita decisión de Diego). El mayorista de
    publicaciones gold_special se evalúa aparte, con _evaluar_mayorista_gold_special
    (ver más abajo) -- no pasa por acá. No escribe nada -- solo lee y clasifica."""
    normal: List[str] = []
    sugeridos: List[Dict[str, Any]] = []
    decision: List[Dict[str, Any]] = []

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
            faltantes = json.loads(audit.get("atributos_faltantes_json") or "{}").get("editables", [])
        except (TypeError, ValueError):
            faltantes = []
        for f in faltantes:
            aid, nombre = f.get("id"), f.get("name") or f.get("id")
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
            normal.append(
                f"Descripción no editable en {iid} ({desc}) -- ML no permite escribirla "
                "en publicaciones de catálogo; se gestiona en el producto de catálogo"
            )
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
            normal.append(f"Mayorista no cargado en {iid} ({desc}) — regla de negocio (solo aplica a la publicación de contado)")

    return {"normal": normal, "sugeridos": sugeridos, "decision": decision}


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


def _match_valor_lista(attr_def: Optional[dict], texto: str) -> bool:
    """True si `texto` coincide (case-insensitive) con una opción de la lista cerrada
    del atributo. Si el atributo no tiene lista de valores, no hay nada que validar."""
    valores = (attr_def or {}).get("values") or []
    if not valores:
        return True
    low = texto.strip().lower()
    return any((v.get("name") or "").strip().lower() == low for v in valores)


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
    value_type = (attr_def or {}).get("value_type")
    valores = (attr_def or {}).get("values") or []
    if value_type == "number_unit":
        unidades = [u.get("id") for u in (attr_def.get("allowed_units") or []) if u.get("id")]
        unidad = attr_def.get("default_unit") or (unidades[0] if unidades else "")
        return (
            f"{contexto}\n\n"
            f"Sugerí el valor para el atributo de ficha técnica \"{g['campo']}\" de este "
            f"producto. Es un valor numérico con unidad, y la ÚNICA unidad válida es "
            f"'{unidad}'. Respondé SOLO con un número seguido de esa unidad (ejemplo: "
            f"'60 {unidad}'). No uses ninguna otra unidad ni la conviertas a otra."
        )
    if valores:
        opciones = ", ".join(v.get("name", "") for v in valores[:80] if v.get("name"))
        return (
            f"{contexto}\n\n"
            f"Elegí el valor más probable para el atributo de ficha técnica \"{g['campo']}\" "
            f"de este producto, ELIGIENDO UNA de estas opciones exactas (respondé copiando "
            f"una tal cual está escrita, sin agregar nada más): {opciones}"
        )
    return (
        f"{contexto}\n\n"
        f"Sugerí el valor más probable para el atributo de ficha técnica \"{g['campo']}\" "
        "de este producto. Respondé SOLO con el valor (una palabra o frase corta), sin "
        "explicaciones ni puntuación extra."
    )


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


# ---------------------------------------------------------------------------
# Cálculo de la propuesta 2/3/5/10 por fórmula de envío propia (no la
# recomendación de ML) -- usada tanto para crear tiers donde no hay ninguno como
# para recalcular el valor "correcto" hoy de un tier ya cargado (ver evaluación
# unificada más abajo, _evaluar_mayorista_gold_special).
#
# El tier de 1 unidad queda AFUERA de la propuesta -- por definición no tiene
# ahorro de envío contra sí mismo, así que la fórmula siempre da 0% para esa
# cantidad, y ML rechaza cualquier tier de mayorista con 0% ("Percentage must
# be greater than 0 and less than 100", confirmado en vivo al intentar guardar
# BHR4245GL). No se inventa un valor para ese caso -- la auditoría original ya
# había registrado que el 1 unidad no tiene un % con origen conocido.
# ---------------------------------------------------------------------------

_CANTIDADES_MAYORISTA_NUEVO = (2, 3, 5, 10)

_CM_POR_UNIDAD = {"mm": 0.1, "cm": 1.0, "m": 100.0}
_GRAMOS_POR_UNIDAD = {"mg": 0.001, "g": 1.0, "kg": 1000.0}


def _num_con_unidad(value_name: Optional[str], factores: Dict[str, float]) -> Optional[float]:
    if not value_name:
        return None
    m = re.search(r"(\d+(?:\.\d+)?)\s*(" + "|".join(factores) + r")\b", value_name, re.IGNORECASE)
    return float(m.group(1)) * factores[m.group(2).lower()] if m else None


def _dimensiones_seller_package(item: dict) -> Optional[Tuple[float, float, float, float]]:
    """Lee SELLER_PACKAGE_HEIGHT/LENGTH/WIDTH/WEIGHT del ítem -- shipping.dimensions
    viene null en la práctica (verificado en vivo: 0/6 ítems con el campo poblado en
    esta cuenta). SELLER_PACKAGE_* son los atributos que carga el vendedor para el
    cálculo de envío y coinciden con el caso de referencia validado (item MLA del
    FireTVStick-4K-Max: SELLER_PACKAGE_WEIGHT=250 g, HEIGHT=18 cm, LENGTH=4 cm,
    WIDTH=15 cm)."""
    vals = {a.get("id"): a.get("value_name") for a in (item.get("attributes") or [])}
    l = _num_con_unidad(vals.get("SELLER_PACKAGE_LENGTH"), _CM_POR_UNIDAD)
    w = _num_con_unidad(vals.get("SELLER_PACKAGE_WIDTH"), _CM_POR_UNIDAD)
    h = _num_con_unidad(vals.get("SELLER_PACKAGE_HEIGHT"), _CM_POR_UNIDAD)
    peso = _num_con_unidad(vals.get("SELLER_PACKAGE_WEIGHT"), _GRAMOS_POR_UNIDAD)
    if None in (l, w, h, peso):
        return None
    return (l, w, h, peso)


def _costo_envio_free(token: str, seller_id: str, l: float, w: float, h: float,
                       peso_g: float, item_price: float) -> Optional[float]:
    """GET /users/{seller_id}/shipping_options/free -- costo de envío para un lote de
    dimensiones fijas (L x W x H) y el peso dado. Devuelve coverage.all_country.list_cost
    o None si ML no puede cotizar (sin cobertura, error, etc.)."""
    try:
        r = requests.get(
            f"{ML_API}/users/{seller_id}/shipping_options/free",
            params={
                "dimensions": f"{int(round(l))}x{int(round(w))}x{int(round(h))},{int(round(peso_g))}",
                "item_price": item_price,
                "free_shipping": "true",
            },
            headers={"Authorization": f"Bearer {token}"}, timeout=15,
        )
        if r.status_code != 200:
            return None
        return (r.json().get("coverage") or {}).get("all_country", {}).get("list_cost")
    except requests.exceptions.RequestException:
        return None


def _calcular_mayorista_nuevo(token: str, seller_id: str, item: dict, precio_base: float) -> Optional[Dict[str, Any]]:
    """Arma la propuesta de mayorista 2/3/5/10 para un ítem SIN mayorista cargado.
    Fórmula validada: % = ceil(ahorro_envío / precio_base × 10000) / 10000, donde
    ahorro_envío = costo_envío(1 unidad) − costo_envío(N unidades)/N (el costo a 1
    unidad se usa como base de comparación, nunca se ofrece como tier -- ver nota
    arriba). Las dimensiones (SELLER_PACKAGE_* del propio ítem) quedan fijas -- SOLO
    el peso escala ×N, no se simula apilado. Es una aproximación (no exacta: ML arma
    el paquete combinado con su propia tara, el escalado lineal del peso es la mejor
    aproximación disponible sin una fórmula más exacta documentada). Devuelve None si
    no hay SELLER_PACKAGE_* cargado, no hay precio base, ML no puede cotizar el envío
    para alguna de las 4 cantidades, o ninguna de las 4 da un % > 0 (ML rechaza tiers
    con 0%)."""
    dims = _dimensiones_seller_package(item)
    if not dims or not precio_base:
        return None
    l, w, h, peso = dims
    costo_1 = _costo_envio_free(token, seller_id, l, w, h, peso, precio_base)
    if costo_1 is None:
        return None
    propuesta: List[Dict[str, Any]] = []
    for n in _CANTIDADES_MAYORISTA_NUEVO:
        costo_n = _costo_envio_free(token, seller_id, l, w, h, peso * n, precio_base * n)
        if costo_n is None:
            return None
        ahorro_unit = costo_1 - (costo_n / n)
        pct = math.ceil((ahorro_unit / precio_base) * 10000) / 10000 if ahorro_unit > 0 else 0.0
        if pct <= 0:
            continue  # ML rechaza tiers de mayorista con 0% -- no se ofrece, no se inventa
        monto = round(precio_base * (1 - pct), 2)
        propuesta.append({
            "quantity": n, "amount": monto, "percentage": round(pct * 100, 2),
            "list_cost": costo_n,
        })
    if not propuesta:
        return None
    return {
        "precio_base": precio_base, "dimensiones": f"{l:g}x{w:g}x{h:g}", "peso_base_g": peso,
        "propuesta": propuesta,
    }


# ---------------------------------------------------------------------------
# Evaluación unificada de mayorista para publicaciones gold_special (contado) --
# reemplaza las 2 secciones viejas ("sin cargar" y "a corregir", esta última basada
# en ml_get_pxq_recommendations). Un solo motor: para cada una de las 4 cantidades
# objetivo (2/3/5/10) compara lo cargado hoy contra _calcular_mayorista_nuevo
# recalculado en el momento, y clasifica cada tier en:
#   - "crear": no hay tier cargado en esa cantidad, se ofrece el calculado.
#   - "ok": hay tier cargado y está dentro del margen del cálculo actual.
#   - "roto": el tier cargado da % negativo o cero (precio ≥ precio base) --
#     objetivo, sin ambigüedad de fórmula, se ofrece corregir junto con "crear".
#   - "revisar": el tier cargado difiere del calculado más allá del umbral, pero
#     no es "roto" -- caso ambiguo (la fórmula de envío se puede desviar mucho en
#     productos muy baratos o muy caros, confirmado en el barrido de cuenta del
#     2026-09-03: el % calculado varió entre 0.21% y 121% según el precio del
#     producto). Se muestra como referencia (cargado vs. calculado hoy) pero NUNCA
#     se ofrece aplicar automático.
# El umbral (4pp absolutos Y 1.75x relativo) se validó contra el barrido completo
# de la cuenta: dispara en casos reales como E.Show8-2da-Negro (tier de mayo,
# 6.59% cargado vs 2.10% calculado hoy) sin falsos positivos sobre los 97 tiers
# recién escritos con este mismo cálculo.
#
# Cantidad=1 y cantidades no estándar (ej. 7) quedan SIEMPRE afuera -- nunca se
# evalúan ni se tocan, se preservan tal cual estén (ver _construir_payload_mayorista).
# ---------------------------------------------------------------------------

_QTYS_MAYORISTA = (2, 3, 5, 10)
_DESVIO_PP_MIN = 4.0
_DESVIO_RATIO_MIN = 1.75


def _standard_amount_de(prices_body: dict) -> Optional[float]:
    for p in prices_body.get("prices") or []:
        cond = p.get("conditions") or {}
        if p.get("type") == "standard" and cond.get("min_purchase_unit") is None and not (cond.get("context_restrictions") or []):
            return float(p["amount"])
    return None


def _tiers_cargados_por_cantidad(prices_body: dict, precio_base: float) -> Dict[int, float]:
    """Tiers cargados HOY en las 4 cantidades objetivo, unificando legacy
    (prices[type=standard] con min_purchase_unit) y % B2B nuevo (price_per_quantity),
    ambos a monto absoluto para poder compararlos con el cálculo."""
    cargado: Dict[int, float] = {}
    for p in prices_body.get("prices") or []:
        cond = p.get("conditions") or {}
        mpu = cond.get("min_purchase_unit")
        if mpu in _QTYS_MAYORISTA and p.get("amount") is not None:
            cargado[mpu] = float(p["amount"])
    for p in prices_body.get("price_per_quantity") or []:
        if p.get("type") != "discount_percentage":
            continue
        cond = p.get("conditions") or {}
        if cond.get("eligible") is False:
            continue
        mpu = cond.get("min_purchase_unit")
        pct = p.get("percentage")
        if mpu in _QTYS_MAYORISTA and pct is not None:
            cargado[mpu] = round(precio_base * (1 - pct / 100), 2)
    return cargado


def _evaluar_mayorista_gold_special(token: str, seller_id: str, item: dict) -> Optional[Dict[str, Any]]:
    """Evalúa las 4 cantidades objetivo para UNA publicación gold_special. Devuelve
    None si no se puede evaluar (sin precio base, sin dimensiones, sin cotización de
    envío) o si las 4 están "ok" y no hay nada que mostrar. GET en vivo -- no usa el
    snapshot de la auditoría, que puede estar desactualizado frente a escrituras
    recientes."""
    iid = item["id"]
    try:
        rp = requests.get(f"{ML_API}/items/{iid}/prices", headers={"Authorization": f"Bearer {token}", "show-all-prices": "TRUE"}, timeout=15)
    except requests.exceptions.RequestException:
        return None
    if rp.status_code != 200:
        return None
    prices_body = rp.json()
    precio_base = _standard_amount_de(prices_body)
    if not precio_base:
        return None
    cargado = _tiers_cargados_por_cantidad(prices_body, precio_base)

    prop = _calcular_mayorista_nuevo(token, seller_id, item, precio_base)
    calculado = {p["quantity"]: p["amount"] for p in prop["propuesta"]} if prop else {}
    calculado_pct = {p["quantity"]: p["percentage"] for p in prop["propuesta"]} if prop else {}

    tiers: List[Dict[str, Any]] = []
    for q in _QTYS_MAYORISTA:
        if q not in cargado:
            if q in calculado:
                tiers.append({"quantity": q, "estado": "crear", "pct_calculado": calculado_pct[q], "monto_calculado": calculado[q]})
            continue  # sin tier cargado y sin cálculo posible -- no se puede ofrecer nada
        pct_cargado = round((precio_base - cargado[q]) / precio_base * 100, 2)
        pct_calc = calculado_pct.get(q)
        if pct_calc is None:
            # TODO(mayorista-revisar-popup, 2026-09-04): si no hay cálculo posible (sin
            # SELLER_PACKAGE_*, o ML no cotiza envío), un tier con pct_cargado<=0 (roto)
            # cae acá y queda "ok" en vez de "roto" -- no es un reorden trivial: "roto"
            # siempre trae pct_calculado/monto_calculado (los usa el popup para sugerir
            # la corrección); sin cálculo haría falta un estado nuevo y tocar el render.
            # Evaluar aparte, no mezclado con el fix de checkboxes por tier.
            tiers.append({"quantity": q, "estado": "ok", "pct_cargado": pct_cargado, "monto_cargado": cargado[q]})
            continue
        if pct_cargado <= 0:
            tiers.append({"quantity": q, "estado": "roto", "pct_cargado": pct_cargado, "monto_cargado": cargado[q],
                          "pct_calculado": pct_calc, "monto_calculado": calculado[q]})
            continue
        diff_pp = abs(pct_cargado - pct_calc)
        ratio = max(pct_cargado, pct_calc) / max(min(pct_cargado, pct_calc), 0.01)
        if diff_pp >= _DESVIO_PP_MIN and ratio >= _DESVIO_RATIO_MIN:
            tiers.append({"quantity": q, "estado": "revisar", "pct_cargado": pct_cargado, "monto_cargado": cargado[q],
                          "pct_calculado": pct_calc, "monto_calculado": calculado[q]})
        else:
            tiers.append({"quantity": q, "estado": "ok", "pct_cargado": pct_cargado, "monto_cargado": cargado[q]})

    presentes = sorted(cargado.keys())
    invertido = any(cargado[presentes[i]] < cargado[presentes[i + 1]] for i in range(len(presentes) - 1))

    if not any(t["estado"] != "ok" for t in tiers) and not invertido:
        return None  # las 4 están ok (o no evaluables) y no hay inversión -- nada para mostrar

    return {"precio_base": precio_base, "tiers": tiers, "invertido": invertido}


def _tiers_plan(evaluacion: Dict[str, Any], incluir: set) -> Tuple[Dict[int, float], List[int]]:
    """Arma (cambios, bloqueadas) para las cantidades que el usuario tildó en `incluir`
    -- generaliza la versión anterior (_tiers_accionables): la decisión de qué corregir
    ahora es 100% del checkbox por tier del popup. "crear"/"roto" vienen pre-tildados
    por default, "revisar" no (ver render). Un tier "crear"/"roto"/"revisar" NO tildado
    no se toca -- si ya tiene un valor cargado (roto/revisar), ese valor sigue siendo
    el piso para las cantidades mayores: ML exige % ESTRICTAMENTE creciente con la
    cantidad (confirmado en vivo el 2026-09-03 probando AW-S11-Black-MEQT4LW: "Price
    per quantity invalid coherence order" cuando un tier nuevo quedaba más bajo que uno
    preservado en una cantidad menor, y "Price per quantity amount are not unique" al
    igualarlo en vez de superarlo).

    Se recorre en orden creciente de cantidad manteniendo ese piso. Un tier tildado que
    quedaría en o por debajo del piso se sube a piso + 0.01. Pero si ese ajuste aleja el
    % resultante de su propio valor calculado más allá del mismo umbral que separa "ok"
    de "revisar" (4pp y 1.75x), no se fuerza -- se bloquea esa cantidad y todas las que
    siguen, y se marca para revisión manual."""
    piso = 0.0
    cambios: Dict[int, float] = {}
    bloqueadas: List[int] = []
    for t in sorted(evaluacion["tiers"], key=lambda x: x["quantity"]):
        q = t["quantity"]
        estado = t["estado"]
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
        diff_pp = abs(pct_final - pct_calc)
        ratio = max(pct_final, pct_calc) / max(min(pct_final, pct_calc), 0.01)
        if diff_pp >= _DESVIO_PP_MIN and ratio >= _DESVIO_RATIO_MIN:
            bloqueadas.append(q)
            continue
        cambios[q] = pct_final
        piso = pct_final
    return cambios, bloqueadas


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
                        campo_label: str, valor_anterior: str, valor_nuevo: str) -> Optional[str]:
    """Devuelve None si ok, o un mensaje de error para el resumen si falló."""
    resp = ml_update_item_attributes(token, item_id, [{"id": attr_id, "value_name": valor_nuevo}])
    post_detalle = f"PUT status={resp.status_code} {resp.text[:200]}" if resp.status_code != 200 else None
    time.sleep(0.4)
    item = ml_get_item(token, item_id)
    actual = None
    if item:
        actual = next((a.get("value_name") for a in (item.get("attributes") or []) if a.get("id") == attr_id), None)
    ok = actual == valor_nuevo
    if ok:
        log_ml_escritura(uid, sku, item_id, f"atributo:{attr_id}", valor_anterior, valor_nuevo, "salud_popup", "ok", None)
        return None
    detalle = post_detalle or f"GET de verificación no coincide (quedó {actual!r})"
    log_ml_escritura(uid, sku, item_id, f"atributo:{attr_id}", valor_anterior, valor_nuevo, "salud_popup", "error", detalle)
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


def _construir_payload_mayorista(prices_info: dict, cambios: Dict[int, float]) -> Tuple[List[Dict[str, Any]], bool]:
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
    omitirlo lo borra."""
    standard_amount = _standard_amount_de(prices_info)
    tiene_absoluto = any(
        p.get("type") == "standard" and (p.get("conditions") or {}).get("min_purchase_unit") is not None
        for p in prices_info.get("prices") or []
    )
    body_items: List[Dict[str, Any]] = []
    vistos: set = set()

    for p in prices_info.get("prices") or []:
        cond = p.get("conditions") or {}
        mpu = cond.get("min_purchase_unit")
        if mpu is None or p.get("amount") is None or not standard_amount:
            continue
        vistos.add(mpu)
        pct = cambios.get(mpu)
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
        if mpu in cambios:
            body_items.append(_tier_body(mpu, cambios[mpu]))
        else:
            preservado = _tier_body(mpu, p.get("percentage"))
            preservado["id"] = p["id"]
            body_items.append(preservado)

    for mpu, pct in cambios.items():
        if mpu not in vistos:
            body_items.append(_tier_body(mpu, pct))

    return body_items, tiene_absoluto


def _escribir_mayorista_pxq(token: str, uid: int, sku: str, item_id: str,
                             cambios: Dict[int, float]) -> Optional[str]:
    """cambios: {cantidad: porcentaje} SOLO para las cantidades a crear/corregir --
    todo lo demás que el ítem ya tenga cargado se preserva (ver _construir_payload_mayorista)."""
    prices_info = ml_get_prices_with_version(token, item_id)
    if not prices_info or "version" not in prices_info:
        msg = "no se pudo leer la versión de precios (X-Version) antes de escribir"
        log_ml_escritura(uid, sku, item_id, "mayorista_pxq", None, json.dumps(cambios, ensure_ascii=False), "salud_popup", "error", msg)
        return f"Mayorista ({item_id}): {msg}"
    version = prices_info["version"]
    body_items, tiene_pxq_absoluto = _construir_payload_mayorista(prices_info, cambios)
    valor_nuevo = json.dumps(cambios, ensure_ascii=False)
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
        for mpu, pct in cambios.items()
    )
    if ok:
        log_ml_escritura(uid, sku, item_id, "mayorista_pxq", None, valor_nuevo, "salud_popup", "ok", None)
        return None
    detalle = post_detalle or f"GET de verificación no coincide (quedó {verify_pct!r})"
    log_ml_escritura(uid, sku, item_id, "mayorista_pxq", None, valor_nuevo, "salud_popup", "error", detalle)
    return f"Mayorista ({item_id}): {detalle}"


def build_tab_salud(container) -> None:
    container.clear()
    user = _require_login()
    if not user:
        return
    uid = user["id"]

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

                    def _render_campo(g: Dict[str, Any], seccion: str):
                        with ui.column().classes("w-full gap-0"):
                            with ui.row().classes("items-center gap-2 w-full"):
                                ui.label(g["campo"]).classes("text-xs w-56")
                                if g["tipo"] == "descripcion":
                                    inp = ui.textarea(
                                        value=g["valor_sugerido"] if seccion == "sugerido" else "",
                                        placeholder=None if seccion == "sugerido" else "(vacío = no tocar)",
                                    ).props("dense outlined").classes("flex-grow").style("min-height:110px")
                                else:
                                    inp = ui.input(
                                        value=g["valor_sugerido"] if seccion == "sugerido" else "",
                                        placeholder=None if seccion == "sugerido" else "(vacío = no tocar)",
                                    ).props("dense outlined").classes("flex-grow")
                                marca_ia = ui.label("✨ sugerido por IA, sin verificar").classes("text-xs").style(f"color:{_MID}")
                                marca_ia.set_visibility(False)
                                if _con_boton_ia(g, seccion):
                                    async def _click_ia(g=g, inp=inp, marca_ia=marca_ia) -> None:
                                        if not groq_key:
                                            ui.notify("Configurá tu API key de Groq en Config → IA/Sugerencias", color="warning")
                                            return
                                        attr_def = cat_attrs_by_id.get(g.get("attr_id")) if g["tipo"] == "atributo" else None
                                        try:
                                            texto = await run.io_bound(_groq_generate, groq_key, _prompt_ia(g, contexto_ia, attr_def))
                                        except Exception as exc:
                                            ui.notify(f"Error al pedir sugerencia a la IA: {exc}", color="negative")
                                            return
                                        inp.value = texto
                                        if g["tipo"] == "atributo" and not _match_valor_lista(attr_def, texto):
                                            marca_ia.set_text("✨ sugerido por IA -- no coincide con una opción válida de ML, revisar antes de guardar")
                                            marca_ia.style(f"color:{_BAD}")
                                        marca_ia.set_visibility(True)
                                    ui.button(icon="auto_awesome", on_click=_click_ia).props("flat dense round size=sm").tooltip("Sugerir con IA")
                            ui.label(_aplica_a_texto(g["items"])).classes("text-xs text-gray-400 pl-1")
                        return inp

                    body.clear()
                    with body:
                        ui.label(f"{len(resultado['items'])} publicaciones · datos actualizados recién ahora").classes("text-xs text-gray-500")

                        if clasif["normal"]:
                            with ui.expansion(f"ℹ️ Normal por diseño ({len(clasif['normal'])})", value=False).classes("w-full text-sm"):
                                for txt in clasif["normal"]:
                                    ui.label(f"• {txt}").classes("text-xs text-gray-500")

                        grupos_sug = _consolidar(clasif["sugeridos"])
                        if grupos_sug:
                            ui.label(f"✏️ Sugerido — revisar y confirmar ({len(grupos_sug)})").classes("font-semibold text-sm mt-2")
                            for i, g in enumerate(grupos_sug):
                                inp = _render_campo(g, "sugerido")
                                inputs[f"sug_{i}"] = (g, inp)

                        if clasif["decision"]:
                            ui.label(f"❓ Necesita tu decisión ({len(grupos_dec)})").classes("font-semibold text-sm mt-2")
                            for i, g in enumerate(grupos_dec):
                                inp = _render_campo(g, "decision")
                                inputs[f"dec_{i}"] = (g, inp)

                        _ESTADO_COLOR = {"crear": _OK, "roto": _BAD, "revisar": _MID, "ok": _GREY, "bloqueada": _MID}
                        if mayorista_eval:
                            ui.label(f"💰 Mayorista (contado) — {len(mayorista_eval)} publicación(es)").classes("font-semibold text-sm mt-2")
                            for item_id, ev in mayorista_eval.items():
                                mayorista_tildes[item_id] = {
                                    t["quantity"]: t["estado"] in ("crear", "roto")
                                    for t in ev["tiers"] if t["estado"] in ("crear", "roto", "revisar")
                                }
                                item_box = ui.column().classes("w-full gap-0 border rounded p-2")

                                def _render_item(item_id=item_id, ev=ev, item_box=item_box):
                                    tildes = mayorista_tildes[item_id]
                                    incluir = {q for q, v in tildes.items() if v}
                                    cambios, bloqueadas = _tiers_plan(ev, incluir)
                                    item_box.clear()
                                    with item_box:
                                        ui.label(f"{item_id} ({ev['descriptor']}) — precio contado ${_fmt_moneda(ev['precio_base'])}").classes("text-xs font-medium")
                                        if ev["invertido"]:
                                            ui.label(
                                                "⚠️ tiers cargados en orden invertido (una cantidad mayor cuesta más "
                                                "por unidad que una menor) — revisar manualmente, sin corrección automática"
                                            ).classes("text-xs pl-3").style(f"color:{_BAD}")
                                        for t in ev["tiers"]:
                                            q = t["quantity"]
                                            estado = t["estado"]
                                            if q in bloqueadas:
                                                txt = (
                                                    f"{q}+ unidades: no se puede {('corregir' if estado == 'roto' else 'crear')} sin quedar "
                                                    f"incoherente con un tier existente en una cantidad menor (ML exige % no decreciente) — revisar a mano"
                                                )
                                                ui.label(txt).classes("text-xs pl-3").style(f"color:{_ESTADO_COLOR['bloqueada']}")
                                                continue
                                            if estado == "ok":
                                                txt = f"{q}+ unidades: ok — ${_fmt_moneda(t['monto_cargado'])} ({t['pct_cargado']}% off)"
                                                ui.label(txt).classes("text-xs pl-3").style(f"color:{_ESTADO_COLOR['ok']}")
                                                continue
                                            if estado not in ("crear", "roto", "revisar"):
                                                continue
                                            marcado = tildes.get(q, False)
                                            aplica = marcado and q in cambios
                                            monto_sugerido = round(ev["precio_base"] * (1 - t["pct_calculado"] / 100), 2)
                                            if aplica:
                                                pct_final = cambios[q]
                                                monto_final = round(ev["precio_base"] * (1 - pct_final / 100), 2)
                                                extra = "" if pct_final == t["pct_calculado"] else f" (ajustado de {t['pct_calculado']}% para no quedar por debajo de un tier existente)"
                                            if estado == "crear":
                                                txt = (f"{q}+ unidades: crear → ${_fmt_moneda(monto_final)} ({pct_final}% off){extra}" if aplica
                                                       else f"{q}+ unidades: sugerido crear ${_fmt_moneda(monto_sugerido)} ({t['pct_calculado']}%), sin tildar")
                                            elif estado == "roto":
                                                txt = (f"{q}+ unidades: ROTO — cargado ${_fmt_moneda(t['monto_cargado'])} ({t['pct_cargado']}%) → corregir a ${_fmt_moneda(monto_final)} ({pct_final}%){extra}" if aplica
                                                       else f"{q}+ unidades: ROTO — cargado ${_fmt_moneda(t['monto_cargado'])} ({t['pct_cargado']}%), sugerido ${_fmt_moneda(monto_sugerido)} ({t['pct_calculado']}%), sin tildar")
                                            else:  # revisar
                                                txt = (f"{q}+ unidades: revisar → corregir a ${_fmt_moneda(monto_final)} ({pct_final}%) (cargado ${_fmt_moneda(t['monto_cargado'])}, {t['pct_cargado']}%){extra}" if aplica
                                                       else f"{q}+ unidades: revisar — cargado ${_fmt_moneda(t['monto_cargado'])} ({t['pct_cargado']}%) vs. sugerido ${_fmt_moneda(monto_sugerido)} ({t['pct_calculado']}%)")
                                            with ui.row().classes("items-center gap-1 pl-3"):
                                                chk = ui.checkbox(value=marcado)
                                                ui.label(txt).classes("text-xs").style(f"color:{_ESTADO_COLOR[estado]}")

                                            def _on_toggle(e, item_id=item_id, q=q):
                                                mayorista_tildes[item_id][q] = e.value
                                                _render_item()
                                            chk.on_value_change(_on_toggle)

                                _render_item()

                        if not clasif["sugeridos"] and not decision_editable and not mayorista_eval:
                            ui.label("Sin hallazgos accionables -- este SKU está al día.").classes("text-sm").style(f"color:{_OK}")

                        resumen_area = ui.column().classes("w-full gap-1")

                    async def _guardar() -> None:
                        guardar_btn.props("loading")
                        errores: List[str] = []
                        aplicados = 0
                        for g, inp in inputs.values():
                            valor = (inp.value or "").strip()
                            if not valor:
                                continue
                            for it in g["items"]:
                                if g["tipo"] == "atributo":
                                    err = await run.io_bound(
                                        _escribir_atributo, token, uid, sku, it["item_id"], g["attr_id"], g["campo"], None, valor,
                                    )
                                elif g["tipo"] == "descripcion":
                                    err = await run.io_bound(
                                        _escribir_descripcion, token, uid, sku, it["item_id"], 0, valor,
                                    )
                                else:
                                    continue
                                if err:
                                    errores.append(err)
                                else:
                                    aplicados += 1

                        for item_id, ev in mayorista_eval.items():
                            incluir = {q for q, v in mayorista_tildes.get(item_id, {}).items() if v}
                            if not incluir:
                                continue
                            cambios, _bloqueadas = _tiers_plan(ev, incluir)
                            if not cambios:
                                continue
                            err = await run.io_bound(
                                _escribir_mayorista_pxq, token, uid, sku, item_id, cambios,
                            )
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
                            if not aplicados and not errores:
                                ui.label("No se marcó ningún campo para guardar.").classes("text-xs text-gray-500")

                        if aplicados:
                            resultado2 = await run.io_bound(audit_sku, uid, seller_id or "", sku, True)
                            if not resultado2.get("error"):
                                prod_meta_single = {sku: {
                                    "nombre": row_actual["producto"], "marca": row_actual["marca"], "stock": row_actual["stock"],
                                }}
                                nueva_fila = _sku_summary(sku, [r["audit"] for r in resultado2["items"]], prod_meta_single)
                                for idx, f in enumerate(filas_todas):
                                    if f["sku"] == sku:
                                        filas_todas[idx] = nueva_fila
                                        break
                                _render()
                        guardar_btn.props(remove="loading")

                    with ui.row().classes("justify-end gap-2 w-full mt-2"):
                        ui.button("Cancelar", on_click=dlg.close).props("flat")
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

                contador_lbl.set_text(f"mostrando {len(visibles)} de {len(filas_todas)}")

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
