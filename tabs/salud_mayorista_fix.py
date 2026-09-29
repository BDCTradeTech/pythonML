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

import json
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests
from nicegui import run, ui

from db import get_producto_costo
from margen import _calc_margen_prod, _load_params_prod
from ml_api import ml_get_prices_with_version, get_ml_access_token, ml_get_user_id
from salud_audit import (
    _calcular_mayorista_recomendado,
    _firma_tiers,
    _precio_vigente_de,
    _qtys_mayorista_para_stock,
    _standard_amount_de,
    UMBRAL_PP_MAYORISTA,
)

_ML_API = "https://api.mercadolibre.com"
ORIGEN = "salud_boton_mayorista"
_MAX_TIERS = 5
_PCT_TECHO = 90.0

# % chico por cantidad cuando ML no recomienda nada (204 o cantidad incoherente).
ESCALA_CHICA: Dict[int, float] = {1: 0.25, 2: 0.25, 3: 0.50, 5: 0.75, 10: 1.00}
_SIN_REC_MAX_OK = 1.0  # ícono: un tier sin recomendación de ML con % > 1 se marca (la escala chica llega a 1,00)

_GREY, _OK, _BAD, _MID = "#6B7280", "#2E7D32", "#A32D2D", "#B26A00"


def _fmt_ars(v: Optional[float]) -> str:
    if v is None:
        return "—"
    s = f"{abs(v):,.0f}".replace(",", ".")
    return f"-${s}" if v < 0 else f"${s}"


def _lista_es(vals: List[Any]) -> str:
    vals = [str(v) for v in vals]
    if len(vals) <= 1:
        return "".join(vals)
    return ", ".join(vals[:-1]) + f" y {vals[-1]}"


# ---------------------------------------------------------------------------
# 1) Ícono -- se calcula al renderizar con datos del snapshot, SIN llamar a ML
# ---------------------------------------------------------------------------

def _json_o_vacio(raw: Any) -> dict:
    try:
        v = json.loads(raw) if raw else {}
        return v if isinstance(v, dict) else {}
    except (TypeError, ValueError):
        return {}


def motivos_mayorista_fix(items: List[dict], stock_sku: Optional[int]) -> Dict[str, Any]:
    """Devuelve {"motivos": [str], "margen_neg": [str]} para el SKU.
    `items`: filas de salud_item_snapshots (o el dict audit de audit_item) de las publicaciones
    del SKU. Solo cuentan las ACTIVAS. El stock de cada publicación sale del payload del snapshot
    ("stock", lo guarda el cron desde 2026-09-30); si no está (snapshots anteriores, o
    publicaciones sin tiers) se usa el stock del SKU (productos.stock, el mismo de la fila);
    si tampoco hay, no se juzga el set de cantidades. `legacy_abs` también viene del payload."""
    motivos: List[str] = []
    neg: List[str] = []
    faltan_all: set = set()
    sobran_all: set = set()
    sin_mayorista = 0
    rev: set = set()
    roto: set = set()
    invertido = False
    legacy = False
    sin_rec_alto: set = set()
    cuotas = 0
    tope = False

    for it in items:
        if it.get("status") != "active":
            continue
        lt = it.get("listing_type_id")
        tiers_json = _json_o_vacio(it.get("mayorista_tiers_json")).get("tiers") or []
        cargadas = {int(q) for q, _amt in tiers_json}
        if lt == "gold_pro":
            if cargadas:
                cuotas += 1
            continue
        if lt != "gold_special":
            continue
        if it.get("mayorista_estado") == "error_sin_standard":
            continue  # /prices sin precio estándar de marketplace (solo nodos por canal): el motor no lo puede evaluar
        info = _json_o_vacio(it.get("mayorista_revisar_json"))
        stock = info.get("stock") if info.get("stock") is not None else stock_sku
        if stock is not None:
            objetivo = set(_qtys_mayorista_para_stock(stock))
            if not cargadas and objetivo:
                sin_mayorista += 1
            else:
                faltan_all |= objetivo - cargadas
                sobran_all |= cargadas - objetivo
        if len(cargadas) > _MAX_TIERS:
            tope = True
        if info.get("legacy_abs"):
            legacy = True
        if info.get("invertido"):
            invertido = True
        # snapshots anteriores a la corrida de esta noche no traen tiers_eval: cae a tiers_revisar
        # (solo revisar/roto, regla vieja de 4 pp) -- se normaliza sola con el próximo cron
        for t in (info["tiers_eval"] if info.get("tiers_eval") is not None else info.get("tiers_revisar")) or []:
            q = t.get("quantity")
            if t.get("estado") == "revisar":
                rev.add(q)
            elif t.get("estado") == "roto":
                roto.add(q)
            if t.get("sin_recomendacion") and (t.get("pct_cargado") or 0) > _SIN_REC_MAX_OK:
                sin_rec_alto.add(q)
            mc = t.get("margen_cargado")
            if (mc is not None and mc < 0) or (mc is None and t.get("margen_negativo") and t.get("pct_cargado") is not None):
                m_txt = f" ({_fmt_ars(mc)})" if mc is not None else ""
                neg.append(f"{q}u{m_txt}")

    if sin_mayorista:
        motivos.append("Sin mayorista cargado")
    if faltan_all:
        motivos.append(f"Faltan {_lista_es(sorted(faltan_all))}")
    if sobran_all:
        motivos.append(f"Sobran {_lista_es(sorted(sobran_all))}")
    if tope:
        motivos.append("Más de 5 tiers")
    if rev:
        motivos.append(f"Precio a revisar en {_lista_es(sorted(q for q in rev if q is not None))}")
    if roto:
        motivos.append(f"Tier roto en {_lista_es(sorted(q for q in roto if q is not None))}")
    if invertido:
        motivos.append("Tiers invertidos")
    if legacy:
        motivos.append("Tiers en monto absoluto (legacy)")
    if sin_rec_alto:
        motivos.append(f"Sin recomendación de ML y % > 1 en {_lista_es(sorted(q for q in sin_rec_alto if q is not None))}")
    if cuotas:
        motivos.append("Cuotas con mayorista")
    return {"motivos": motivos, "margen_neg": sorted(set(neg))}


def render_iconos(mayfix: Optional[Dict[str, Any]], on_click: Callable[[], Any]) -> None:
    """Íconos de la celda Mayorista (se llama dentro del contenedor de la celda)."""
    if not mayfix:
        return
    if mayfix.get("motivos"):
        b = ui.icon("build", size="16px").classes("cursor-pointer").style(f"color:{_MID}")
        b.tooltip(" · ".join(mayfix["motivos"]) + " — click para corregir")
        b.on("click", lambda: on_click())
    if mayfix.get("margen_neg"):
        m = ui.icon("trending_down", size="16px").style(f"color:{_BAD}")
        m.tooltip("Margen negativo en: " + ", ".join(mayfix["margen_neg"]) + " (informativo)")


# ---------------------------------------------------------------------------
# 2) Plan de cada publicación (lectura en vivo, sin escribir)
# ---------------------------------------------------------------------------

def _tiers_actuales(body: dict, precio_base: float) -> Dict[int, Dict[str, Any]]:
    """{cantidad: {pct, monto, sistema, id}} de lo cargado hoy (% B2B y legacy absoluto)."""
    out: Dict[int, Dict[str, Any]] = {}
    for p in body.get("prices") or []:
        c = p.get("conditions") or {}
        q = c.get("min_purchase_unit")
        if q is not None and p.get("amount") is not None and precio_base:
            amt = float(p["amount"])
            out[int(q)] = {"pct": round((1 - amt / precio_base) * 100, 2), "monto": amt,
                           "sistema": "absoluto", "id": p.get("id")}
    for p in body.get("price_per_quantity") or []:
        if p.get("type") != "discount_percentage":
            continue
        c = p.get("conditions") or {}
        q = c.get("min_purchase_unit")
        if q is not None and p.get("percentage") is not None:
            out[int(q)] = {"pct": float(p["percentage"]), "monto": None, "sistema": "pct", "id": p.get("id")}
    return out


def calcular_pcts_objetivo(target: Tuple[int, ...], rec: Dict[int, float], sin_rec: Dict[int, str],
                            actuales: Dict[int, Dict[str, Any]],
                            umbral_pp: float = UMBRAL_PP_MAYORISTA) -> Dict[int, Tuple[float, str]]:
    """{cantidad: (pct_nuevo, origen)} para el set objetivo. Orígenes: 'ml' (recomendado directo),
    'mantiene' (ya cargado en % B2B con recomendación de ML, no menos profundo que ella y a <= umbral pp: no se toca),
    'piso' (ML recomienda 0 -> piso de coherencia), 'escala' (sin recomendación),
    'coherencia' (se subió para que el % sea estrictamente creciente).
    Reglas: % estrictamente creciente (+0,01 mínimo), 0 < % < 90. Un % nunca se baja por debajo
    de lo que recomienda ML (sí se puede subir por coherencia)."""
    qs = sorted(target)
    out: Dict[int, Tuple[float, str]] = {}
    prev = 0.0
    for i, q in enumerate(qs):
        r = rec.get(q)
        act = actuales.get(q)
        if r is not None:
            if r <= 0:
                cand, origen = prev + 0.01, "piso"
            elif act and act["sistema"] == "pct" and 0 <= act["pct"] - r <= umbral_pp and act["pct"] > prev:
                cand, origen = act["pct"], "mantiene"
            else:
                cand, origen = r, "ml"
        else:
            cand, origen = ESCALA_CHICA.get(q, 1.0), "escala"
            # tope: estrictamente por debajo del próximo tier CON recomendación
            sig = next((rec[x] for x in qs[i + 1:] if rec.get(x)), None)
            if sig is not None and cand >= sig:
                cand = sig - 0.01
        if cand <= prev:
            cand, origen = prev + 0.01, "coherencia"
        cand = round(cand, 2)
        if cand <= prev:
            cand = round(prev + 0.01, 2)
        out[q] = (cand, origen)
        prev = cand
    return out


def _margen(precio: float, q: int, costo: Optional[Tuple[float, float]], params: dict) -> Optional[float]:
    if not costo:
        return None
    m = _calc_margen_prod(precio, costo[0], costo[1], params, cantidad=q)
    return None if m is None else round(m, 2)


def planificar_publicacion(token: str, uid: int, sku: str, pub: dict) -> Dict[str, Any]:
    """Plan para UNA publicación activa. `pub`: {item_id, catalog_listing, listing_type_id, stock,
    body (prices con version)}. Solo GET + recommendations. Devuelve el plan con las filas
    (una por cantidad involucrada), `cambios`/`eliminar` (lo que se le pasa a
    _escribir_mayorista_pxq) y la `firma` de lo que se mostró."""
    body = pub["body"]
    iid = pub["item_id"]
    base = _standard_amount_de(body)
    plan: Dict[str, Any] = {
        "item_id": iid, "catalogo": bool(pub.get("catalog_listing")), "tipo": "cuotas" if pub["listing_type_id"] == "gold_pro" else "contado",
        "listing_type_id": pub["listing_type_id"], "stock": pub["stock"], "precio_base": base,
        "precio_vigente": None, "promo": False, "filas": [], "cambios": {}, "eliminar": set(),
        "hay_cambios": False, "error": None, "firma": _firma_tiers(body),
        "incoherentes": [], "opcion": "aplicar", "hay_margen_neg": False, "qtys_actuales": [],
    }
    if not base:
        plan["error"] = "sin precio estándar en /prices"
        return plan
    vigente = _precio_vigente_de(body, base)
    plan["precio_vigente"] = vigente
    plan["promo"] = vigente < base - 0.005
    actuales = _tiers_actuales(body, base)

    if pub["listing_type_id"] == "gold_pro":
        target: Tuple[int, ...] = ()
    else:
        target = _qtys_mayorista_para_stock(pub["stock"])

    nuevos: Dict[int, Tuple[float, str]] = {}
    rec: Dict[int, float] = {}
    sin_rec: Dict[int, str] = {}
    if target:
        prop = _calcular_mayorista_recomendado(token, iid, base, target, incluir_cero=True)
        if prop is None:
            plan["error"] = "no se pudo consultar las recomendaciones de ML"
            return plan
        rec = {p["quantity"]: p["percentage"] for p in prop["propuesta"]}
        sin_rec = dict(prop["sin_recomendacion"])
        if any(m == "error" for m in sin_rec.values()):
            plan["error"] = "ML no respondió la recomendación de " + _lista_es([q for q, m in sin_rec.items() if m == "error"]) + " (error de consulta) — reintentá"
            return plan
        incoherentes = {q for q, m in sin_rec.items() if m == "incoherente"}
        plan["incoherentes"] = sorted(incoherentes)
        target = tuple(q for q in target if q not in incoherentes)  # ML no admite esas cantidades hoy (5598): se omiten
        nuevos = calcular_pcts_objetivo(target, rec, sin_rec, actuales) if target else {}
        if any(p >= _PCT_TECHO for p, _o in nuevos.values()):
            plan["error"] = "el set calculado supera el techo de sanidad (90 %) — no se escribe"
            return plan

    costo = get_producto_costo(sku, uid) if sku else None
    params = _load_params_prod(uid) if costo else {}
    cambios: Dict[int, float] = {}
    eliminar: set = set()
    filas: List[Dict[str, Any]] = []
    plan["qtys_actuales"] = sorted(actuales)
    for q in sorted(set(target) | set(actuales) | set(plan["incoherentes"])):
        act = actuales.get(q)
        nuevo = nuevos.get(q)
        fila: Dict[str, Any] = {"q": q, "pct_actual": act["pct"] if act else None, "legacy": bool(act and act["sistema"] == "absoluto"),
                                "pct_nuevo": None, "precio_nuevo": None, "margen": None, "margen_pct": None,
                                "margen_neg": None, "accion": None, "nota": None}
        if nuevo is None:  # cargado pero fuera del objetivo (o cantidad que ML no admite hoy)
            if q in plan["incoherentes"]:
                fila["nota"] = "ML no admite esta cantidad hoy"
                if act is None:
                    fila["accion"] = "omite"
                    filas.append(fila)
                    continue
            fila["accion"] = "borra"
            eliminar.add(q)
        else:
            pct, origen = nuevo
            fila["pct_nuevo"] = pct
            precio = round(vigente * (1 - pct / 100), 2)
            fila["precio_nuevo"] = precio
            m = _margen(precio, q, costo, params)
            fila["margen"] = m
            fila["margen_pct"] = round(m / precio * 100, 1) if (m is not None and precio) else None
            fila["margen_neg"] = None if m is None else m < 0
            if m is not None and m < 0:
                plan["hay_margen_neg"] = True
            nota = {"ml": None, "mantiene": None, "piso": "ML recomienda 0 % → piso de coherencia",
                    "escala": "sin recomendación de ML" + (f" ({sin_rec.get(q)})" if sin_rec.get(q) else "") + " → escala chica",
                    "coherencia": "subido para que el % crezca con la cantidad"}[origen]
            fila["nota"] = nota
            if act is None:
                fila["accion"] = "crea"
                cambios[q] = pct
            elif act["sistema"] == "absoluto":
                fila["accion"] = "migra"
                cambios[q] = pct
            elif abs(act["pct"] - pct) < 0.005:
                fila["accion"] = "igual"
            else:
                fila["accion"] = "cambia"
                cambios[q] = pct
        filas.append(fila)
    plan["filas"] = filas
    plan["cambios"] = cambios
    plan["eliminar"] = eliminar
    plan["hay_cambios"] = bool(cambios or eliminar)
    return plan


def efectivo(plan: Dict[str, Any]) -> Optional[Tuple[Dict[int, float], set]]:
    """(cambios, eliminar) que se escriben según la opción elegida en la publicación:
    'aplicar' (propuesta), 'quitar' (set vacío) o 'no_tocar' (None). None también si no hay nada que escribir."""
    if plan.get("error") or plan.get("opcion") == "no_tocar":
        return None
    if plan.get("opcion") == "quitar":
        return ({}, set(plan["qtys_actuales"])) if plan["qtys_actuales"] else None
    return (dict(plan["cambios"]), set(plan["eliminar"])) if plan["hay_cambios"] else None


def leer_y_planificar(token: str, uid: int, sku: str, item_ids: List[str]) -> Dict[str, Any]:
    """Lectura en vivo (SOLO GET + recommendations) de las publicaciones activas del SKU.
    `item_ids` sale del snapshot WHERE user_id=? (nunca del cliente)."""
    pubs: List[dict] = []
    ignoradas = 0
    ses = requests.Session()
    cuerpos: Dict[str, dict] = {}
    for i in range(0, len(item_ids), 20):
        r = ses.get(f"{_ML_API}/items", params={"ids": ",".join(item_ids[i:i + 20])},
                    headers={"Authorization": f"Bearer {token}"}, timeout=30)
        if r.status_code != 200:
            return {"error": f"no se pudieron leer las publicaciones (status {r.status_code})", "planes": []}
        for e in r.json():
            if e.get("code") == 200:
                cuerpos[e["body"]["id"]] = e["body"]
    for iid in item_ids:
        it = cuerpos.get(iid)
        if not it:
            continue
        if it.get("status") != "active" or it.get("listing_type_id") not in ("gold_special", "gold_pro"):
            ignoradas += 1
            continue
        body = ml_get_prices_with_version(token, iid)
        pubs.append({"item_id": iid, "catalog_listing": bool(it.get("catalog_listing")),
                     "listing_type_id": it["listing_type_id"], "stock": int(it.get("available_quantity") or 0),
                     "body": body})
        time.sleep(0.05)
    planes: List[Dict[str, Any]] = []
    for p in pubs:
        if p["body"] is None or "version" not in p["body"]:
            planes.append({"item_id": p["item_id"], "catalogo": p["catalog_listing"], "listing_type_id": p["listing_type_id"],
                           "tipo": "cuotas" if p["listing_type_id"] == "gold_pro" else "contado", "stock": p["stock"],
                           "filas": [], "cambios": {}, "eliminar": set(), "hay_cambios": False, "precio_base": None,
                           "precio_vigente": None, "promo": False, "firma": None,
                           "error": "no se pudo leer /prices (X-Version)"})
            continue
        planes.append(planificar_publicacion(token, uid, sku, p))
    return {"error": None, "planes": planes, "ignoradas": ignoradas}


# ---------------------------------------------------------------------------
# 3) Escritura (al confirmar) -- una publicación a la vez, independientes
# ---------------------------------------------------------------------------

def aplicar_publicacion(token: str, uid: int, sku: str, plan: Dict[str, Any]) -> Dict[str, Any]:
    """Relee /prices; si difiere de lo que se mostró (precio, promo o tiers) NO escribe. Si no,
    _escribir_mayorista_pxq hace: lectura de versión, UN POST con el set completo, GET de
    verificación y el log en ml_escrituras (origen salud_boton_mayorista, valor_anterior = set
    completo previo). Devuelve {"ok", "cambio", "msg"}."""
    from tabs.salud import _escribir_mayorista_pxq  # import diferido: salud.py importa este módulo
    iid = plan["item_id"]
    ef = efectivo(plan)
    if ef is None or not plan.get("firma"):
        return {"ok": False, "cambio": False, "msg": "nada para escribir en esta publicación"}
    fresco = ml_get_prices_with_version(token, iid)
    if fresco is None:
        return {"ok": False, "cambio": False, "msg": "no se pudo releer /prices antes de escribir — no se escribió"}
    if _firma_tiers(fresco) != plan["firma"]:
        return {"ok": False, "cambio": True, "msg": "Cambió mientras mirabas (precio, promo o tiers): no se escribió, volvé a abrir."}
    err, adv = _escribir_mayorista_pxq(token, uid, sku, iid, ef[0], ef[1], origen=ORIGEN)
    if err:
        return {"ok": False, "cambio": False, "msg": err}
    return {"ok": True, "cambio": False, "msg": "; ".join(adv) if adv else "Escrito y verificado"}


# ---------------------------------------------------------------------------
# 4) Diálogo
# ---------------------------------------------------------------------------

_OPCIONES = {"aplicar": "Aplicar propuesta", "quitar": "Quitar mayorista", "no_tocar": "No tocar"}


def _render_plan(plan: Dict[str, Any], resultado: Optional[Dict[str, Any]],
                 on_opcion: Optional[Callable[[], Any]] = None) -> None:
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
        if not plan["hay_cambios"]:
            ui.label("Sin cambios, no se escribe").classes("text-xs text-gray-500")
            return
        if plan["hay_margen_neg"] and resultado is None:
            with ui.row().classes("items-center gap-2"):
                ui.icon("trending_down", size="16px").style(f"color:{_BAD}")
                ui.label("Algún tier nuevo da margen negativo (informativo, no bloquea):").classes("text-xs").style(f"color:{_BAD}")
                sel = ui.toggle(_OPCIONES, value=plan["opcion"]).props("dense size=sm no-caps")

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


async def abrir_dialogo_mayorista(uid: int, sku: str, producto: str, desde_fecha: Optional[str],
                                  al_cerrar: Callable[[Dict[str, Any]], None]) -> None:
    """Abre el diálogo del 🔧 para `sku`. `al_cerrar(resultado_audit)` se llama con el diálogo ya
    cerrado y solo si se escribió algo (mismo contrato que abrir_popup_reg)."""
    from tabs.salud_reg import _items_del_sku  # import diferido, mismo patrón que abrir_popup_reg
    from salud_audit import audit_sku

    estado: Dict[str, Any] = {"escribio": False, "planes": [], "resultados": {}}
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
            elif not any(p["hay_cambios"] for p in planes) and not any(p["error"] for p in planes):
                ui.label("✅ Ya está correcto: no hay nada que cambiar.").classes("text-sm font-semibold").style(f"color:{_OK}")
            for p in planes:
                _render_plan(p, estado["resultados"].get(p["item_id"]), _pintar)
        pendientes = [p for p in planes if efectivo(p) is not None and p["item_id"] not in estado["resultados"]]
        con_selector = [p for p in planes if p["hay_cambios"] and not p["error"]]
        if con_selector:
            cnt = {k: sum(1 for p in con_selector if p["opcion"] == k) for k in _OPCIONES}
            with info:
                ui.label("Resumen: " + " · ".join(f"{cnt[k]} {_OPCIONES[k].lower()}" for k in _OPCIONES)).classes("text-sm font-semibold")
        btn_aplicar.set_visibility(bool(pendientes))
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
        estado["planes"] = lectura["planes"]
        estado["ignoradas"] = lectura.get("ignoradas", 0)
        estado["resultados"] = {}
        _pintar()

    async def _aplicar() -> None:
        btn_aplicar.props("loading disable")
        btn_cerrar.props("disable")
        try:
            for p in estado["planes"]:
                if efectivo(p) is None or p["item_id"] in estado["resultados"]:
                    continue
                res = await run.io_bound(aplicar_publicacion, token, uid, sku, p)
                estado["resultados"][p["item_id"]] = res
                if res["ok"]:
                    estado["escribio"] = True
                _pintar()
        finally:
            btn_aplicar.props(remove="loading disable")
            btn_cerrar.props(remove="disable")
            _pintar()

    async def _cerrar() -> None:
        if not estado["escribio"]:
            dlg.close()
            return
        btn_cerrar.props("loading disable")
        seller_id = await run.io_bound(ml_get_user_id, token)
        resultado = await run.io_bound(audit_sku, uid, seller_id or "", sku, True)
        dlg.close()
        if not resultado.get("error"):
            al_cerrar(resultado)

    btn_aplicar.on_click(_aplicar)
    btn_cerrar.on_click(_cerrar)
    await _cargar()
