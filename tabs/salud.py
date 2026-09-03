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
import time
from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional

import requests
from nicegui import app, ui, run

from db import GROQ_MODEL, get_app_config, get_connection, log_ml_escritura
from ml_api import (
    get_ml_access_token,
    ml_get_item,
    ml_get_prices_with_version,
    ml_get_pxq_recommendations,
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


def _mayorista_color(v: str) -> str:
    return {"ok": _OK, "roto": _BAD, "invertido": _BAD, "sin_mayorista": _GREY, "error_sin_standard": _MID}.get(v, _MID)


_STATUS_NO_APLICABLE = {"no_aplica_catalogo", "no_aplica_no_activo", "no_determinable"}


def _perf_status_ok(status: Optional[str]) -> Optional[bool]:
    """Traduce el status crudo de /item/{id}/performance a cumple/no-cumple.
    no_aplica_catalogo (items de catálogo, sin entidad USER_PRODUCT propia) y
    no_determinable (404) quedan afuera del denominador -- no son un hueco."""
    if not status or status in _STATUS_NO_APLICABLE:
        return None
    return status == "COMPLETED"


_MAYORISTA_ETIQUETAS = {
    "ok": "OK", "roto": "ROTO", "invertido": "INVERTIDO",
    "sin_mayorista": "Sin mayorista", "error_sin_standard": "Error",
}


def _sku_summary(sku: str, items: List[dict], prod_meta: Dict[str, Any]) -> Dict[str, Any]:
    n_items = len(items)

    dims = {
        "gtin": _bool_dim(items, lambda it: bool(it.get("gtin"))),
        "descripcion": _bool_dim(items, lambda it: (it.get("descripcion_len") or 0) > 0 if it.get("descripcion_len") is not None else None),
        "short": _bool_dim(items, lambda it: _perf_status_ok(it.get("short_status"))),
        "fotos": _magnitud_dim(items, lambda it: it.get("fotos_cantidad")),
        "mayorista": _cat_dim(
            items, lambda it: it.get("mayorista_estado"), _MAYORISTA_ETIQUETAS, _mayorista_color,
        ),
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
    sugerido con valor pre-cargado / necesita decisión de Diego) + una lista aparte
    de mayoristas ROTO/INVERTIDO a corregir. No escribe nada -- solo lee y clasifica."""
    normal: List[str] = []
    sugeridos: List[Dict[str, Any]] = []
    decision: List[Dict[str, Any]] = []
    mayoristas_roto: List[Dict[str, Any]] = []

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
        # Prioriza una fuente del mismo tipo (propia->propia, catálogo->catálogo) pero
        # cruza al otro tipo si ese no tiene texto -- es el mismo producto, no hay
        # motivo para dejar una copia de catálogo sin descripción solo porque ninguna
        # OTRA copia de catálogo la tiene, cuando la publicación propia sí.
        fuente, origen_txt = None, ""
        propio = not it.get("catalog_listing")
        preferida, alterna = (propias_con_texto, catalogo_con_texto) if propio else (catalogo_con_texto, propias_con_texto)
        preferida_txt, alterna_txt = ("propia", "catálogo") if propio else ("catálogo", "propia")
        if preferida:
            fuente, origen_txt = preferida[0], f"copiado de {preferida[0]}, {preferida_txt}"
        elif alterna:
            fuente, origen_txt = alterna[0], f"copiado de {alterna[0]}, {alterna_txt}"
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

    # --- mayorista ---
    for r in resultados:
        it, audit = r["item"], r["audit"]
        iid = it["id"]
        desc = _item_descriptor(it)
        estado = audit.get("mayorista_estado")
        if estado == "sin_mayorista":
            if _tag_cuotas(it):
                normal.append(f"Mayorista no cargado en {iid} ({desc}) — regla de negocio (publicación de cuotas)")
            else:
                decision.append({
                    "campo": "Mayorista (definir precio)", "item_id": iid, "descriptor": desc,
                    "tipo": "mayorista_decision", "valor_sugerido": "",
                })
        elif estado in ("roto", "invertido"):
            try:
                tiers_info = json.loads(audit.get("mayorista_tiers_json") or "{}")
            except (TypeError, ValueError):
                tiers_info = {}
            standard = tiers_info.get("standard_amount")
            tiers_actuales = tiers_info.get("tiers") or []
            quantities = [t[0] for t in tiers_actuales][:5]
            propuesta: List[Dict[str, Any]] = []
            if standard and quantities:
                rec = ml_get_pxq_recommendations(token, iid, standard, quantities)
                if rec and rec.get("recommendations"):
                    for reco in rec["recommendations"]:
                        if not reco.get("is_incoherent_quantity"):
                            propuesta.append({
                                "quantity": reco["quantity"], "amount": reco["amount"],
                                "percentage": round(reco.get("discount", {}).get("percentage", 0), 2),
                            })
            mayoristas_roto.append({
                "item_id": iid, "descriptor": desc, "estado": estado,
                "standard_amount": standard, "tiers_actuales": tiers_actuales,
                "propuesta": propuesta,
                "tiene_pxq_absoluto": "standard_price_by_quantity" in (it.get("tags") or []),
            })

    return {"normal": normal, "sugeridos": sugeridos, "decision": decision, "mayoristas_roto": mayoristas_roto}


def _descriptor_corto(descriptor: str) -> str:
    """'propia, 3x' -> '3x' -- para la lista compacta "se aplicará a"."""
    return descriptor.split(", ", 1)[1] if ", " in descriptor else descriptor


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
    return "se aplicará a: " + ", ".join(f"{it['item_id']} ({_descriptor_corto(it['descriptor'])})" for it in items)


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


def _prompt_ia(g: Dict[str, Any], contexto: str) -> str:
    if g["tipo"] == "descripcion":
        return (
            f"{contexto}\n\n"
            "Escribí una descripción de producto para una publicación de MercadoLibre "
            "en español, clara y comercial, de 150 a 400 palabras, basada solo en la "
            "información disponible arriba (no inventes características que no estén "
            "sugeridas por el título/atributos). Devolvé SOLO el texto de la "
            "descripción, sin comillas ni encabezados."
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
# Escritura hacia ML -- SIEMPRE con GET de verificación independiente y log en
# ml_escrituras (ok o error), nunca confiando en el 200 del PUT/POST.
# ---------------------------------------------------------------------------

def _escribir_atributo(token: str, uid: int, sku: str, item_id: str, attr_id: str,
                        campo_label: str, valor_anterior: str, valor_nuevo: str) -> Optional[str]:
    """Devuelve None si ok, o un mensaje de error para el resumen si falló."""
    resp = ml_update_item_attributes(token, item_id, [{"id": attr_id, "value_name": valor_nuevo}])
    if resp.status_code != 200:
        detalle = f"PUT status={resp.status_code} {resp.text[:200]}"
        log_ml_escritura(uid, sku, item_id, f"atributo:{attr_id}", valor_anterior, valor_nuevo, "salud_popup", "error", detalle)
        return f"{campo_label} ({item_id}): {detalle}"
    time.sleep(0.4)
    item = ml_get_item(token, item_id)
    actual = None
    if item:
        actual = next((a.get("value_name") for a in (item.get("attributes") or []) if a.get("id") == attr_id), None)
    ok = actual == valor_nuevo
    log_ml_escritura(uid, sku, item_id, f"atributo:{attr_id}", valor_anterior, valor_nuevo, "salud_popup",
                      "ok" if ok else "error", None if ok else f"GET de verificación no coincide (quedó {actual!r})")
    return None if ok else f"{campo_label} ({item_id}): escrito pero el GET de verificación no coincide (quedó {actual!r})"


def _escribir_descripcion(token: str, uid: int, sku: str, item_id: str,
                           texto_anterior_len: int, texto_nuevo: str) -> Optional[str]:
    resp = ml_write_item_description(token, item_id, texto_nuevo)
    if resp.status_code not in (200, 201):
        detalle = f"status={resp.status_code} {resp.text[:200]}"
        log_ml_escritura(uid, sku, item_id, "descripcion", f"{texto_anterior_len} chars", f"{len(texto_nuevo)} chars", "salud_popup", "error", detalle)
        return f"Descripción ({item_id}): {detalle}"
    time.sleep(0.4)
    try:
        r = requests.get(f"{ML_API}/items/{item_id}/description", headers={"Authorization": f"Bearer {token}"}, timeout=15)
        guardado = (r.json().get("plain_text") or "").strip() if r.status_code == 200 else ""
    except requests.exceptions.RequestException:
        guardado = ""
    ok = guardado == texto_nuevo.strip()
    log_ml_escritura(uid, sku, item_id, "descripcion", f"{texto_anterior_len} chars", f"{len(texto_nuevo)} chars", "salud_popup",
                      "ok" if ok else "error", None if ok else "GET de verificación no coincide")
    return None if ok else f"Descripción ({item_id}): escrita pero el GET de verificación no coincide"


def _escribir_mayorista_pxq(token: str, uid: int, sku: str, item_id: str,
                             tiers_deseados: List[Dict[str, Any]], tiene_pxq_absoluto: bool) -> Optional[str]:
    prices_info = ml_get_prices_with_version(token, item_id)
    if not prices_info or "version" not in prices_info:
        msg = "no se pudo leer la versión de precios (X-Version) antes de escribir"
        log_ml_escritura(uid, sku, item_id, "mayorista_pxq", None, json.dumps(tiers_deseados, ensure_ascii=False), "salud_popup", "error", msg)
        return f"Mayorista ({item_id}): {msg}"
    version = prices_info["version"]
    body_items = [
        {
            "type": "discount_percentage",
            "percentage": t["percentage"],
            "conditions": {
                "context_restrictions": ["channel_marketplace", "user_type_business"],
                "min_purchase_unit": t["quantity"],
                "eligible": True,
            },
        }
        for t in tiers_deseados
    ]
    valor_nuevo = json.dumps(tiers_deseados, ensure_ascii=False)
    resp = ml_write_price_per_quantity(token, item_id, body_items, version, remove_absolute_pxq=tiene_pxq_absoluto)
    if resp.status_code != 200:
        detalle = f"status={resp.status_code} {resp.text[:300]}"
        log_ml_escritura(uid, sku, item_id, "mayorista_pxq", None, valor_nuevo, "salud_popup", "error", detalle)
        return f"Mayorista ({item_id}): {detalle}"
    time.sleep(0.4)
    verify = ml_get_prices_with_version(token, item_id)
    ok = bool(verify and len(verify.get("price_per_quantity") or []) == len(tiers_deseados))
    log_ml_escritura(uid, sku, item_id, "mayorista_pxq", None, valor_nuevo, "salud_popup",
                      "ok" if ok else "error", None if ok else "GET de verificación no coincide en cantidad de tiers")
    return None if ok else f"Mayorista ({item_id}): escrito pero el GET de verificación no coincide"


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
                    contexto_ia = await run.io_bound(
                        _contexto_producto, [r["item"] for r in resultado["items"]], row_actual["marca"],
                    )

                    inputs: Dict[str, tuple] = {}
                    mayorista_checks: Dict[str, tuple] = {}

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
                                        try:
                                            texto = await run.io_bound(_groq_generate, groq_key, _prompt_ia(g, contexto_ia))
                                        except Exception as exc:
                                            ui.notify(f"Error al pedir sugerencia a la IA: {exc}", color="negative")
                                            return
                                        inp.value = texto
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

                        decision_editable = [h for h in clasif["decision"] if h["tipo"] != "mayorista_decision"]
                        decision_info = [h for h in clasif["decision"] if h["tipo"] == "mayorista_decision"]
                        grupos_dec = _consolidar(decision_editable)
                        if clasif["decision"]:
                            ui.label(f"❓ Necesita tu decisión ({len(grupos_dec) + len(decision_info)})").classes("font-semibold text-sm mt-2")
                            for i, g in enumerate(grupos_dec):
                                inp = _render_campo(g, "decision")
                                inputs[f"dec_{i}"] = (g, inp)
                            for h in decision_info:
                                ui.label(
                                    f"• {h['campo']} — {h['item_id']} ({h['descriptor']}) "
                                    f"— no autocompletable acá, es una decisión de precio de negocio"
                                ).classes("text-xs text-gray-500")

                        if clasif["mayoristas_roto"]:
                            ui.label(f"⚠️ Mayoristas a corregir ({len(clasif['mayoristas_roto'])})").classes("font-semibold text-sm mt-2").style(f"color:{_BAD}")
                            for m in clasif["mayoristas_roto"]:
                                with ui.column().classes("w-full gap-0 border rounded p-2"):
                                    ui.label(f"{m['item_id']} ({m['descriptor']}) — {m['estado'].upper()}, precio contado ${m['standard_amount']}").classes("text-xs font-medium")
                                    for t in m["tiers_actuales"]:
                                        ui.label(f"tier actual: {t[0]}+ unidades → ${t[1]}").classes("text-xs text-gray-500 pl-3")
                                    if m["propuesta"]:
                                        for p in m["propuesta"]:
                                            ui.label(f"propuesto (ML): {p['quantity']}+ unidades → ${p['amount']} ({p['percentage']}% off)").classes("text-xs pl-3")
                                        chk = ui.checkbox(f"Aplicar corrección propuesta en {m['item_id']}", value=False)
                                        mayorista_checks[m["item_id"]] = (m, chk)
                                    else:
                                        ui.label("Sin sugerencia de ML para este ítem — revisión manual, no se ofrece autofix").classes("text-xs pl-3").style(f"color:{_MID}")

                        if not clasif["sugeridos"] and not decision_editable and not clasif["mayoristas_roto"]:
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

                        for item_id, (m, chk) in mayorista_checks.items():
                            if not chk.value:
                                continue
                            err = await run.io_bound(
                                _escribir_mayorista_pxq, token, uid, sku, item_id, m["propuesta"], m["tiene_pxq_absoluto"],
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
                                                    ui.label(d["texto"]).style(f"color:{d['color']};font-weight:600")
                                                else:
                                                    ui.label("—")

            stock_sel.on_value_change(lambda: _render())
            marca_sel.on_value_change(lambda: _render())
            buscador.on_value_change(lambda: _render())
            _render()
