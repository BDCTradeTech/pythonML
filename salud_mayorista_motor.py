"""
salud_mayorista_motor.py
Motor del mayorista (PxQ %) de Salud, SIN dependencias de UI (no importa nicegui ni tabs/).
Lo usan dos caminos que comparten TODA la lógica y el MISMO camino de escritura:
  - el botón 🔧 de la tabla de Salud (tabs/salud_mayorista_fix.py), y
  - el cron nocturno (salud_audit.py -> autocorregir_usuario), que reemplaza a la
    auto-corrección vieja (la de "solo sube precio de tiers en pérdida real").

Reglas (las del modo directo de la 🔧, Diego 2026-10-01):
  - Solo publicaciones ACTIVAS. Pausadas y cerradas no se tocan.
  - gold_special: set completo según stock (>=10 -> 2,3,5,10 · 5-9 -> 2,3,5 · 3-4 -> 2,3 · 2 -> 2 · 1 -> 1),
    % de ML de a una cantidad, validación de incoherentes EN LOTE, coherencia estrictamente creciente,
    tolerancia de 1 pp para dejar "igual", migración de legacy con remove-absolute-pxq.
  - gold_pro (cuotas) activas con tiers: se borran.
  - Si algún tier NUEVO de una publicación daría margen negativo, esa publicación NO se toca.
  - Relectura de /prices antes de escribir (si cambió algo se saltea), un POST por publicación,
    GET de verificación y ml_escrituras (origen 'cron_auto_mayorista', valor_anterior = set completo).
  - Freno: si para un usuario las publicaciones a escribir superan SALUD_MAYORISTA_MAX_AUTOCORR
    (default 150) no se escribe ninguna.

Movido acá desde tabs/salud_mayorista_fix.py (planificador) y tabs/salud.py (escritor
_escribir_mayorista_pxq y helpers) para que el cron no importe tabs/. Esos módulos re-importan
los nombres, así que todo lo que los usaba sigue funcionando igual.
"""
from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

from db import get_connection, get_producto_costo, log_ml_escritura, mayorista_auto_activo
from margen import _calc_margen_prod, _load_params_prod, fees_publicacion
from ml_api import ml_get_prices_with_version, ml_write_price_per_quantity
from salud_audit import (
    _NOTA_INCOHERENTE,
    _PCT_TECHO_SANIDAD,
    _calcular_mayorista_recomendado,
    _firma_tiers,
    _precio_vigente_de,
    _qtys_mayorista_para_stock,
    _standard_amount_de,
    _tiers_previos_json,
    _validar_coherencia_lote,
    audit_sku,
    UMBRAL_PP_MAYORISTA,
)

_ML_API = "https://api.mercadolibre.com"
ORIGEN = "salud_boton_mayorista"  # default de aplicar_publicacion (diálogo de la 🔧)
ORIGEN_CRON = "cron_auto_mayorista"
MAX_AUTOCORR_POR_CORRIDA = int(os.environ.get("SALUD_MAYORISTA_MAX_AUTOCORR", "150"))

# Interruptor del dry-run: con True, _escribir_mayorista_pxq (el único punto que escribe a ML) levanta
# antes de hacer cualquier POST.
ESCRITURAS_BLOQUEADAS = False


# ---------------------------------------------------------------------------
# Escritor (movido de tabs/salud.py)
# ---------------------------------------------------------------------------


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
                             eliminar: Optional[set] = None,
                             origen: str = "salud_popup") -> Tuple[Optional[str], List[str]]:
    """cambios: {cantidad: porcentaje} SOLO para las cantidades a crear/corregir --
    todo lo demás que el ítem ya tenga cargado se preserva (ver _construir_payload_mayorista).
    eliminar: cantidades tildadas para sacar del array y liberar lugar (ver FIX A /
    tope de 5, 2026-09-07).
    origen: se pasa tal cual a ml_escrituras.origen (Diego, 2026-09-09) -- el default
    preserva el comportamiento de siempre para el popup; el cron de auto-corrección
    nocturna pasa "cron_auto_mayorista" para poder distinguir ambos en el log.
    Devuelve (error, advertencias) -- advertencias lista las cantidades que
    _construir_payload_mayorista descartó por el piso de sanidad (nunca se
    escribieron a ML), aunque el resto se haya guardado bien (error=None)."""
    if ESCRITURAS_BLOQUEADAS:
        raise RuntimeError("dry-run activo: escritura a ML bloqueada")
    prices_info = ml_get_prices_with_version(token, item_id)
    if not prices_info or "version" not in prices_info:
        msg = "no se pudo leer la versión de precios (X-Version) antes de escribir"
        log_ml_escritura(uid, sku, item_id, "mayorista_pxq", None, json.dumps(cambios, ensure_ascii=False), origen, "error", msg)
        return f"Mayorista ({item_id}): {msg}", []
    version = prices_info["version"]
    # set COMPLETO de tiers antes de escribir (2026-09-29; antes valor_anterior quedaba None)
    tiers_previos = _tiers_previos_json(prices_info)
    body_items, tiene_pxq_absoluto, descartados = _construir_payload_mayorista(prices_info, cambios, eliminar)
    if len(body_items) > _ML_MAX_TIERS_PXQ:
        # Backstop server-side: el popup ya bloquea el guardado antes de llegar acá
        # (banner ⛔ + mayorista_sobre_tope en build_tab_salud), pero el GET de acá es
        # más fresco que el que vio el popup al abrirse -- si algo cambió del lado de
        # ML entre medio
        # (otra escritura, otra pestaña), nunca se manda un POST que ML va a
        # rechazar con "Maximum 5 price_per_quantity entries allowed" (caso real
        # MLA3684456394, 2026-09-07: 5 cargados + 1 "crear" = 6, 400). El cron de
        # auto-corrección (origen="cron_auto_mayorista") también depende de este
        # mismo backstop para su regla de "no auto-corregir si se pasa el tope de 5".
        msg = f"quedarían {len(body_items)} precios por cantidad, ML permite máximo {_ML_MAX_TIERS_PXQ} -- no se envió"
        log_ml_escritura(uid, sku, item_id, "mayorista_pxq", tiers_previos, json.dumps(cambios, ensure_ascii=False), origen, "error", msg)
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
        log_ml_escritura(uid, sku, item_id, "mayorista_pxq", tiers_previos, valor_nuevo, origen, "ok", None)
        return None, advertencias
    detalle = post_detalle or f"GET de verificación no coincide (quedó {verify_pct!r})"
    log_ml_escritura(uid, sku, item_id, "mayorista_pxq", tiers_previos, valor_nuevo, origen, "error", detalle)
    return f"Mayorista ({item_id}): {detalle}", advertencias


# ---------------------------------------------------------------------------
# Condición de la 🔧 y planificador (movidos de tabs/salud_mayorista_fix.py)
# ---------------------------------------------------------------------------


_MAX_TIERS = 5


_PCT_TECHO = 90.0


# % chico por cantidad cuando ML no recomienda nada (204 o cantidad incoherente).
ESCALA_CHICA: Dict[int, float] = {1: 0.25, 2: 0.25, 3: 0.50, 5: 0.75, 10: 1.00}


_SIN_REC_MAX_OK = 1.0  # ícono: un tier sin recomendación de ML con % > 1 se marca (la escala chica llega a 1,00)


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


def _json_o_vacio(raw: Any) -> dict:
    try:
        v = json.loads(raw) if raw else {}
        return v if isinstance(v, dict) else {}
    except (TypeError, ValueError):
        return {}


def motivos_mayorista_fix(items: List[dict], stock_sku: Optional[int], sin_mayorista_dispara: bool = True) -> Dict[str, Any]:
    """Devuelve {"motivos": [str], "margen_neg": [str]} para el SKU.
    `items`: filas de salud_item_snapshots (o el dict audit de audit_item) de las publicaciones
    del SKU. Solo cuentan las ACTIVAS. El stock de cada publicación sale del payload del snapshot
    ("stock", lo guarda el cron desde 2026-09-30); si no está (snapshots anteriores, o
    publicaciones sin tiers) se usa el stock del SKU (productos.stock, el mismo de la fila);
    si tampoco hay, no se juzga el set de cantidades. `legacy_abs` también viene del payload.
    `sin_mayorista_dispara=False` (cuentas con el mayorista automático apagado): "Sin mayorista cargado" no cuenta."""
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

    if sin_mayorista and sin_mayorista_dispara:  # cuentas sin mayorista automático: "sin mayorista" no es motivo de 🔧
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


def _margen(precio: float, q: int, costo: Optional[Tuple[float, float]], params: dict,
            token: Optional[str] = None, category_id: Optional[str] = None,
            listing_type_id: Optional[str] = None) -> Optional[float]:
    """Margen por unidad (contado, x1) con la comisión y el costo fijo reales de la publicación
    (listing_prices por categoría + listing + precio unitario del tier; fallback si ML no responde)."""
    if not costo:
        return None
    com, fijo, _o = fees_publicacion(token, category_id, listing_type_id, precio)
    m = _calc_margen_prod(precio, costo[0], costo[1], params, cantidad=q, comision_pct=com, fixed_fee=fijo)
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
        "aviso": None, "no_tocar_incoherente": False, "sin_costo": False, "sin_costo_incoherente": False,
    }
    if not base:
        plan["error"] = "sin precio estándar en /prices"
        plan["no_evaluable"] = True  # estado de la publicación (solo precios por canal), no un error de ML
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
            m = _margen(precio, q, costo, params, token, pub.get("category_id"), pub["listing_type_id"])
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
    # MARGEN DESCONOCIDO (sin costo cargado o margen no calculable): nunca se crea un tier ni se sube el % de
    # descuento (eso baja el precio sin saber si hay margen). Sí se pueden borrar tiers sobrantes, bajar el % y
    # borrar el mayorista de cuotas. Si lo que queda no es estrictamente creciente, no se escribe la publicación.
    bloqueadas = [f for f in filas if f["margen"] is None and f["pct_nuevo"] is not None and f["accion"] in ("crea", "cambia", "migra")
                  and (f["accion"] == "crea" or f["pct_actual"] is None or f["pct_nuevo"] > f["pct_actual"] + 0.0049)]
    if bloqueadas:
        plan["sin_costo"] = True
        for f in bloqueadas:
            cambios.pop(f["q"], None)
            f["nota"] = "sin costo cargado: no se " + ("crea" if f["accion"] == "crea" else "sube el %") + " (baja el precio sin saber el margen)"
            f["accion"] = "omite"
        final = {}
        for f in filas:
            if f["accion"] in ("borra",) or (f["accion"] == "omite" and f["pct_actual"] is None):
                continue
            pct = f["pct_nuevo"] if f["accion"] in ("crea", "cambia", "migra") else f["pct_actual"]
            if pct is not None:
                final[f["q"]] = pct
        pcts = [final[q] for q in sorted(final)]
        if any(b <= a_ for a_, b in zip(pcts, pcts[1:])):
            cambios, eliminar = {}, set()
            plan["sin_costo_incoherente"] = True
            for f in filas:
                if f["accion"] in ("crea", "cambia", "migra", "borra"):
                    f["accion"] = "omite"
                    f["nota"] = (f["nota"] + " · " if f["nota"] else "") + "sin costo: el set quedaría incoherente, no se toca"
    plan["hay_margen_neg"] = any(f["margen_neg"] and f["accion"] in ("crea", "cambia", "migra") for f in filas)
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
        pubs.append({"item_id": iid, "catalog_listing": bool(it.get("catalog_listing")), "category_id": it.get("category_id"),
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


def aplicar_publicacion(token: str, uid: int, sku: str, plan: Dict[str, Any], origen: str = ORIGEN) -> Dict[str, Any]:
    """Relee /prices; si difiere de lo que se mostró (precio, promo o tiers) NO escribe. Si no,
    _escribir_mayorista_pxq hace: lectura de versión, UN POST con el set completo, GET de
    verificación y el log en ml_escrituras (origen salud_boton_mayorista, valor_anterior = set
    completo previo). Devuelve {"ok", "cambio", "msg"}."""
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


# ---------------------------------------------------------------------------
# Cron nocturno: mismo motor que la 🔧 en modo directo, para todos los SKUs que la necesitan
# ---------------------------------------------------------------------------

def skus_con_llave(user_id: int, fecha: str) -> Dict[str, List[str]]:
    """{sku: [item_id, ...]} de los SKUs a los que HOY (snapshot `fecha`) se les muestra la 🔧: misma
    condición que la tabla (motivos_mayorista_fix sobre el snapshot + productos.stock). Los item_id son
    TODOS los del SKU en el snapshot; el estado real (activa/pausada) lo decide la lectura en vivo."""
    conn = get_connection()
    try:
        stock = {r["sku"]: r["stock"] for r in conn.execute("SELECT sku, stock FROM productos WHERE user_id=?", (user_id,))}
        filas = conn.execute("SELECT * FROM salud_item_snapshots WHERE user_id=? AND snapshot_date=?", (user_id, fecha)).fetchall()
    finally:
        conn.close()
    por_sku: Dict[str, List[dict]] = {}
    for r in filas:
        d = dict(r)
        if d.get("sku"):
            por_sku.setdefault(d["sku"], []).append(d)
    out: Dict[str, List[str]] = {}
    for sku, grp in por_sku.items():
        if motivos_mayorista_fix(grp, stock.get(sku)).get("motivos"):
            out[sku] = [g["item_id"] for g in grp]
    return out


def autocorregir_usuario(token: str, user_id: int, seller_id: str, fecha: str, dry_run: bool = False,
                         max_pubs: Optional[int] = None, log: Any = None, opt_in: Optional[bool] = None) -> Dict[str, Any]:
    """Deja el mayorista de cada SKU con 🔧 igual que lo dejaría la 🔧 en modo directo (ver docstring del módulo).
    1) planifica TODOS los SKUs (solo GET + recommendations); 2) si las publicaciones a escribir superan
    `max_pubs` (SALUD_MAYORISTA_MAX_AUTOCORR) no escribe ninguna (freno);
    `opt_in` fuerza el switch de la cuenta (solo para el dry-run; en el cron real lee app_config); 3) si no, las escribe de a una
    (relectura + salteo si cambió, un POST, GET de verificación, ml_escrituras origen 'cron_auto_mayorista');
    4) refresca el snapshot de hoy de los SKUs escritos. dry_run=True: no escribe nada (ni a ML ni a la DB)
    y devuelve además el plan por publicación en "detalle"."""
    global ESCRITURAS_BLOQUEADAS
    max_pubs = MAX_AUTOCORR_POR_CORRIDA if max_pubs is None else max_pubs
    activo = mayorista_auto_activo(user_id) if opt_in is None else opt_in
    if not activo:  # opt-in por cuenta (apagado por defecto): no se planifica ni se escribe nada
        return {"desactivado": True, "skus_con_llave": 0, "skus": [], "pubs_planificadas": 0, "pubs_a_escribir": 0, "pubs_escritas": 0,
                "pubs_sin_cambios": 0, "no_evaluables": [], "sin_costo": [], "salteadas_margen": [], "errores": [],
                "cambio_concurrente": [], "frenado": False, "freno_detalle": None, "acciones": {}, "detalle": []}
    skus = skus_con_llave(user_id, fecha)
    res: Dict[str, Any] = {
        "skus_con_llave": len(skus), "skus": sorted(skus), "pubs_planificadas": 0, "pubs_a_escribir": 0, "pubs_escritas": 0,
        "pubs_sin_cambios": 0, "no_evaluables": [], "sin_costo": [], "salteadas_margen": [], "errores": [], "cambio_concurrente": [], "frenado": False,
        "freno_detalle": None, "acciones": {"crea": 0, "borra": 0, "cambia": 0, "migra": 0, "borra_cuotas_pubs": 0, "borra_cuotas_tiers": 0},
        "detalle": [],
    }
    previo = ESCRITURAS_BLOQUEADAS
    ESCRITURAS_BLOQUEADAS = previo or dry_run
    try:
        pendientes: List[Tuple[str, Dict[str, Any]]] = []
        for sku, ids in skus.items():
            try:
                lectura = leer_y_planificar(token, user_id, sku, ids)
            except Exception as e:  # noqa: BLE001 -- un SKU que falla no corta a los demás
                res["errores"].append({"sku": sku, "item_id": None, "fase": "lectura", "error": str(e)})
                continue
            if lectura.get("error"):
                res["errores"].append({"sku": sku, "item_id": None, "fase": "lectura", "error": lectura["error"]})
                continue
            planes = lectura["planes"]
            preparar_directo(planes)
            for p in planes:
                res["pubs_planificadas"] += 1
                if p.get("sin_costo"):
                    res["sin_costo"].append({"sku": sku, "item_id": p["item_id"], "parcial": efectivo(p) is not None})
                if p.get("no_evaluable"):
                    res["no_evaluables"].append({"sku": sku, "item_id": p["item_id"]})
                elif p.get("error"):
                    res["errores"].append({"sku": sku, "item_id": p["item_id"], "fase": "plan", "error": p["error"]})
                elif p.get("salteada_margen"):
                    res["salteadas_margen"].append({"sku": sku, "item_id": p["item_id"]})
                elif efectivo(p) is None:
                    res["pubs_sin_cambios"] += 1
                else:
                    pendientes.append((sku, p))
        res["pubs_a_escribir"] = len(pendientes)
        for sku, p in pendientes:
            acc = res["acciones"]
            for f in p["filas"]:
                if f["accion"] in ("crea", "borra", "cambia", "migra"):
                    acc[f["accion"]] += 1
            if p["tipo"] == "cuotas":
                acc["borra_cuotas_pubs"] += 1
                acc["borra_cuotas_tiers"] += len(p["eliminar"])
            if dry_run:
                res["detalle"].append({"sku": sku, "item_id": p["item_id"], "tipo": p["tipo"], "stock": p["stock"],
                                       "filas": [{k: f[k] for k in ("q", "accion", "pct_actual", "pct_nuevo", "margen", "margen_neg")} for f in p["filas"]]})
        if len(pendientes) > max_pubs:
            res["frenado"] = True
            res["freno_detalle"] = (f"freno auto-correccion mayorista: {len(pendientes)} publicaciones a escribir > "
                                    f"{max_pubs} (SALUD_MAYORISTA_MAX_AUTOCORR), no se escribio ninguna")
            if log:
                log.warning("user_id=%s: %s", user_id, res["freno_detalle"])
            return res
        if dry_run:
            return res
        skus_escritos = set()
        for sku, p in pendientes:
            try:
                r = aplicar_publicacion(token, user_id, sku, p, ORIGEN_CRON)
            except Exception as e:  # noqa: BLE001
                r = {"ok": False, "cambio": False, "msg": f"error inesperado: {e}"}
            if r["ok"]:
                res["pubs_escritas"] += 1
                skus_escritos.add(sku)
            elif r.get("cambio"):
                res["cambio_concurrente"].append({"sku": sku, "item_id": p["item_id"]})
            else:
                res["errores"].append({"sku": sku, "item_id": p["item_id"], "fase": "escritura", "error": r["msg"]})
            time.sleep(0.4)
        for sku in sorted(skus_escritos):  # la tabla de Salud queda al día sin esperar a mañana
            try:
                audit_sku(user_id, seller_id, sku, True)
            except Exception as e:  # noqa: BLE001 -- refresco cosmético: el mayorista ya quedó escrito y verificado
                if log:
                    log.warning("user_id=%s: no se pudo refrescar el snapshot de %s tras escribir: %s", user_id, sku, e)
        return res
    finally:
        ESCRITURAS_BLOQUEADAS = previo
