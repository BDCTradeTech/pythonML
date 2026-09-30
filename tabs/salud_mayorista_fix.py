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
import json
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests
from nicegui import background_tasks, run, ui

from db import get_producto_costo
from margen import _calc_margen_prod, _load_params_prod
from ml_api import ml_get_prices_with_version, get_ml_access_token, ml_get_pxq_recommendations, ml_get_user_id
from salud_audit import (
    _NOTA_INCOHERENTE,
    _calcular_mayorista_recomendado,
    _firma_tiers,
    _precio_vigente_de,
    _qtys_mayorista_para_stock,
    _standard_amount_de,
    _validar_coherencia_lote,
    UMBRAL_PP_MAYORISTA,
)

_ML_API = "https://api.mercadolibre.com"
ORIGEN = "salud_boton_mayorista"
ORIGEN_DIRECTO = "salud_boton_mayorista_directo"  # 🔧 sin diálogo
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
    inco_all: set = set()
    cargadas_all: set = set()

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
        # cantidades que ML no admite hoy (incoherentes con las demás, las guarda el cron): no son "faltan"
        inco = {int(q) for q in info.get("incoherentes") or []}
        cargadas_all |= cargadas
        if stock is not None:
            objetivo = set(_qtys_mayorista_para_stock(stock))
            inco_all |= (inco & objetivo) - cargadas
            if not cargadas and (objetivo - inco):
                sin_mayorista += 1
            else:
                faltan_all |= objetivo - cargadas - inco
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
            if t.get("sin_recomendacion") and q not in inco and (t.get("pct_cargado") or 0) > _SIN_REC_MAX_OK:
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
    # incoherentes: solo las que ninguna publicación del SKU tiene cargadas (si alguna la tiene, no falta nada)
    return {"motivos": motivos, "margen_neg": sorted(set(neg)), "incoherentes": sorted(inco_all - cargadas_all)}


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
        "aviso": None, "no_tocar_incoherente": False,
    }
    if not base:
        plan["error"] = "sin precio estándar en /prices"
        return plan
    vigente = _precio_vigente_de(body, base)
    plan["precio_vigente"] = vigente
    plan["promo"] = vigente < base - 0.005
    actuales = _tiers_actuales(body, base)
    plan["qtys_actuales"] = sorted(actuales)  # antes que cualquier return: "Quitar mayorista" tiene que estar siempre

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
        target = tuple(q for q in target if q not in incoherentes)  # ML no admite esas cantidades hoy (5598): se omiten
        if target:
            target, descartadas, err_lote = _validar_coherencia_lote(token, iid, base, target)
            if err_lote:
                plan["incoherentes"] = sorted(incoherentes | set(descartadas))
                plan["error"] = err_lote
                return plan
            incoherentes |= set(descartadas)
        plan["incoherentes"] = sorted(incoherentes)
        if incoherentes and not target:
            plan["no_tocar_incoherente"] = True
            plan["aviso"] = "ML no admite ninguna de las cantidades objetivo hoy (incoherentes) — no se toca esta publicación"
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
                fila["nota"] = _NOTA_INCOHERENTE
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
    if plan["no_tocar_incoherente"]:
        # set vacío solo por incoherencias: no se borra ni se escribe nada
        for f in filas:
            if f["accion"] == "borra":
                f["accion"] = "omite"
                f["nota"] = (f["nota"] + " · " if f["nota"] else "") + "no se toca"
        cambios, eliminar, plan["hay_margen_neg"] = {}, set(), False
    plan["filas"] = filas
    plan["cambios"] = cambios
    plan["eliminar"] = eliminar
    plan["hay_cambios"] = bool(cambios or eliminar)
    if not plan["hay_cambios"]:
        plan["opcion"] = "no_tocar"  # nada que aplicar; el usuario puede elegir "Quitar mayorista"
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

def aplicar_publicacion(token: str, uid: int, sku: str, plan: Dict[str, Any], origen: str = ORIGEN) -> Dict[str, Any]:
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
    err, adv = _escribir_mayorista_pxq(token, uid, sku, iid, ef[0], ef[1], origen=origen)
    if err:
        return {"ok": False, "cambio": False, "msg": err}
    return {"ok": True, "cambio": False, "msg": "; ".join(adv) if adv else "Escrito y verificado"}


# ---------------------------------------------------------------------------
# 3b) Modo DIRECTO (🔧 sin diálogo): mismo motor y mismas protecciones que el diálogo
# ---------------------------------------------------------------------------

def margen_neg_nuevos(plan: Dict[str, Any]) -> bool:
    """True si algún tier que el plan CREA / CAMBIA / MIGRA daría margen negativo (los tiers que ya
    están cargados y quedan 'igual' no cuentan: no se van a escribir)."""
    return any(f.get("margen_neg") and f["accion"] in ("crea", "cambia", "migra") for f in plan.get("filas") or [])


def preparar_directo(planes: List[Dict[str, Any]]) -> None:
    """Marca 'No tocar' las publicaciones con algún tier NUEVO de margen negativo (se reportan aparte)."""
    for p in planes:
        p["salteada_margen"] = False
        if not p["error"] and margen_neg_nuevos(p):
            p["opcion"] = "no_tocar"
            p["salteada_margen"] = True


_AMBAR_SOBRE_ROJO = "#FFD54F"


def armar_notificacion_directo(planes: List[Dict[str, Any]], resultados: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
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
