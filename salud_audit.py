"""
salud_audit.py
Auditoría de completitud de publicaciones ML (página Salud). Guarda SIEMPRE el
valor crudo leído de la API en salud_item_snapshots -- cualquier interpretación
(ok/roto, completo/incompleto, editable/bloqueado) se calcula al leer en
tabs/salud.py, nunca acá. Si una llamada de detalle falla para un ítem, se
registra el error textual y se sigue con el resto -- una corrida parcial con
errores anotados vale más que una abortada (ver resync_sku_catalogos.py).

Fuente de los campos (validado en la auditoría previa, no redescubrir):
- GTIN, fotos, Short, Flex, envío gratis, puntaje: /item/{id}/performance
  (singular) + atributos crudos del item. Bucket "USER_PRODUCT" trae
  UP_GTIN/UP_PICTURES/UP_SHORTS; el bucket cuya key == item_id trae
  UP_ME_FLEX_ITEM_OPTIN/UP_FREE_SHIPPING (Condiciones de venta).
- Precio: campo "price" del body del item (multiget), sin llamada extra.
- Mayorista: /items/{id}/prices con header show-all-prices: TRUE (sin ese
  header ML devuelve 200 con menos tiers de los que hay, sin ninguna señal).
- Atributos editables faltantes: /categories/{id}/attributes (una vez por
  category_id), attribute no oculto (tags.hidden != True) sin valor en el
  item; tags.read_only distingue bloqueado (no cuenta en el resumen) de
  editable.
- Regulatoria: NO_DETERMINABLE -- no hay campo genérico documentado por ML
  para "aplica/no aplica/vacío"; queda pendiente de una fuente confirmada.

Cron: 30 5 * * * /opt/pythonml/venv/bin/python3 /opt/pythonml/salud_audit.py >> /var/log/pythonml_salud.log 2>&1
(después de resync_sku_catalogos.py 5 3 y competidores_snapshot.py 0 4, antes de
la jornada; el catálogo completo son ~1400 items propios x 4 llamadas c/u)

Uso manual:
  cd /opt/pythonml && set -a && . ./.env && set +a && ./venv/bin/python3 salud_audit.py
  ./venv/bin/python3 salud_audit.py --sku Sony-MDR-ZX110-Negros   (una sola familia, on-demand)
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from dotenv import load_dotenv
load_dotenv(BASE_DIR / ".env")

import requests
from db import get_connection, init_cron_runs_db, init_salud_tables, log_cron_run, log_salud_mayorista_cron
from db import get_producto_costo
from margen import _calc_margen_prod, _load_params_prod
from ml_api import get_ml_access_token, ml_get_pxq_recommendations

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

ML_API = "https://api.mercadolibre.com"
PARAM_SETS = [
    {"status": "active"},
    {"status": "paused"},
    {"status": "closed"},
    {"sub_status": "pending_documentation"},
    {"sub_status": "held"},
]


def _err_detalle(r: requests.Response) -> str:
    try:
        body = r.json()
        return body.get("message") or body.get("error") or ""
    except Exception:
        return (r.text or "")[:150]


def _get_seller_sku(item: dict) -> str:
    for attr in item.get("attributes") or []:
        if attr.get("id") == "SELLER_SKU":
            return (attr.get("value_name") or "").strip()
    return ""


_REINTENTOS_ML = 3
_BACKOFF_S = (1.0, 3.0, 8.0)
_MAX_REINICIOS_SCAN = 2


class _ScrollInvalido(Exception):
    """El scroll_id de un scan expiró/quedó inválido a mitad de camino."""


def _get_con_reintentos(url: str, headers: dict, params: Optional[dict] = None, timeout: int = 15) -> requests.Response:
    """GET con hasta _REINTENTOS_ML reintentos (backoff 1/3/8 s) ante 5xx, 429,
    timeout o error de conexión. Cualquier otro status se devuelve tal cual para
    que el caller decida (ej. 400/404 = scroll_id inválido). Si se agotan los
    reintentos levanta RequestException."""
    ultimo = None
    for intento in range(_REINTENTOS_ML + 1):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=timeout)
            if r.status_code < 500 and r.status_code != 429:
                return r
            ultimo = f"HTTP {r.status_code}"
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            ultimo = repr(e)
        if intento < _REINTENTOS_ML:
            log.warning("GET %s fallo (%s), reintento %d/%d", url, ultimo, intento + 1, _REINTENTOS_ML)
            time.sleep(_BACKOFF_S[intento])
    raise requests.exceptions.RequestException(f"GET {url} fallo tras {_REINTENTOS_ML} reintentos ({ultimo})")


def _scan_ids_status(token: str, seller_id: str, extra: dict) -> List[str]:
    ids: List[str] = []
    scroll_id = None
    while True:
        params = {"search_type": "scan", "limit": 100, **extra}
        if scroll_id:
            params["scroll_id"] = scroll_id
        r = _get_con_reintentos(
            f"{ML_API}/users/{seller_id}/items/search",
            {"Authorization": f"Bearer {token}"}, params=params, timeout=15,
        )
        if scroll_id and r.status_code in (400, 404):
            raise _ScrollInvalido(f"scroll_id invalido (HTTP {r.status_code}) en {extra}")
        r.raise_for_status()
        data = r.json()
        chunk = data.get("results", [])
        if not chunk:
            break
        ids.extend(chunk)
        scroll_id = data.get("scroll_id")
        if not scroll_id:
            break
        time.sleep(0.05)
    return ids


def fetch_all_own_items(token: str, seller_id: str) -> List[dict]:
    """Scan completo (mismos 5 grupos de status que resync_sku_catalogos.py) + multiget.
    Cada GET reintenta ante 5xx/timeout (ver _get_con_reintentos); si el scroll_id de
    un status queda inválido se reinicia el scan de ESE status (hasta
    _MAX_REINICIOS_SCAN veces, descartando lo parcial de ese status). Si igual falla
    levanta la excepción -- run() la registra en cron_runs y sigue con los otros usuarios."""
    all_ids: List[str] = []
    for extra in PARAM_SETS:
        for intento in range(_MAX_REINICIOS_SCAN + 1):
            try:
                all_ids.extend(_scan_ids_status(token, seller_id, extra))
                break
            except _ScrollInvalido as e:
                if intento >= _MAX_REINICIOS_SCAN:
                    raise
                log.warning("%s -- reiniciando scan de ese status (%d/%d)", e, intento + 1, _MAX_REINICIOS_SCAN)
    all_ids = list(dict.fromkeys(all_ids))

    items: List[dict] = []
    for i in range(0, len(all_ids), 20):
        batch = all_ids[i:i + 20]
        r = _get_con_reintentos(f"{ML_API}/items", {"Authorization": f"Bearer {token}"},
                                params={"ids": ",".join(batch)}, timeout=30)
        r.raise_for_status()
        for entry in r.json():
            if entry.get("code") == 200:
                items.append(entry["body"])
        time.sleep(0.05)
    return items


def _wholesale_from_prices(prices_body: dict) -> Dict[str, Any]:
    """Misma clasificación que wholesale_sweep.py: ROTO/INVERTIDO/OK/SIN_MAYORISTA.
    Unifica los DOS sistemas de mayorista de ML: el legacy de precio absoluto
    (prices[type=standard] con min_purchase_unit) y el nuevo de % B2B
    (price_per_quantity[type=discount_percentage], el que escribe el popup de Salud
    vía ml_write_price_per_quantity). Sin esto, cualquier ítem con el sistema nuevo
    cargado clasifica siempre sin_mayorista -- confirmado en vivo (BHR4245GL,
    2026-09-03): 3 tiers % cargados y verificados en ML, pero _wholesale_from_prices
    seguía devolviendo tiers=[] porque nunca miraba price_per_quantity."""
    prices = prices_body.get("prices") or []
    standard_amount = None
    tiers: List[List[float]] = []
    for p in prices:
        if not isinstance(p, dict) or p.get("type") != "standard":
            continue
        cond = p.get("conditions") or {}
        min_pu = cond.get("min_purchase_unit")
        amt = p.get("amount")
        if min_pu is None:
            if amt is not None and not (cond.get("context_restrictions") or []):
                standard_amount = float(amt)
            continue
        if amt is not None:
            tiers.append([int(min_pu), float(amt)])

    for p in prices_body.get("price_per_quantity") or []:
        if not isinstance(p, dict) or p.get("type") != "discount_percentage":
            continue
        cond = p.get("conditions") or {}
        if cond.get("eligible") is False:
            continue
        min_pu = cond.get("min_purchase_unit")
        pct = p.get("percentage")
        if min_pu is None or pct is None or standard_amount is None:
            continue
        tiers.append([int(min_pu), round(standard_amount * (1 - pct / 100), 2)])

    tiers.sort(key=lambda t: t[0])

    if not tiers:
        estado = "sin_mayorista"
    elif standard_amount is None:
        estado = "error_sin_standard"
    else:
        min_q, min_amt = tiers[0]
        if min_amt >= standard_amount:
            estado = "roto"
        elif any(tiers[i][1] < tiers[i + 1][1] for i in range(len(tiers) - 1)):
            estado = "invertido"
        else:
            estado = "ok"
    return {"estado": estado, "standard_amount": standard_amount, "tiers": tiers}


# ---------------------------------------------------------------------------
# Cálculo de la propuesta de mayorista vía el endpoint oficial de ML
# (POST /prices-per-quantity/v1/recommendations, ml_get_pxq_recommendations en
# ml_api.py) -- reemplaza la aproximación por cotización de envío propia y la
# tabla fija que usaba este archivo hasta 2026-09-08 (ninguna de las dos
# consultaba nunca el endpoint real contra el que ML valida "Amount above
# recommended"/cause_id 5599, confirmado en vivo con Awei-H21/MLA1568099745
# el 2026-09-08: la tabla fija proponía 1/2/3/4% cuando ML pedía 6.18/6.19/9.16%).
# ---------------------------------------------------------------------------

# Piso de sanidad genérico: ningún % propuesto puede dejar un precio negativo o
# casi regalado. Se aplica acá (nunca se propone) y de nuevo en
# tabs/salud.py::_construir_payload_mayorista (nunca se escribe a ML), como
# doble chequeo.
_PCT_TECHO_SANIDAD = 90.0


def _calcular_mayorista_recomendado(token: str, item_id: str, precio_base: float,
                                     cantidades: Tuple[int, ...],
                                     incluir_cero: bool = False) -> Optional[Dict[str, Any]]:
    """Pide a ML el % mínimo aceptado para cada cantidad de `cantidades`, vía el
    endpoint oficial de recomendaciones -- UNA cantidad por llamada (desde
    2026-09-29; antes iban en batch). Se usa el % que devuelve ML tal cual, nunca
    se convierte un monto a % sobre otra base. Devuelve None solo si no hay
    cantidades o precio base; si no, un dict con:
      propuesta: [{quantity, amount, percentage}] -- cantidades con recomendación válida
      sin_recomendacion: {quantity: motivo} -- ML devolvió 204 ("204"), marcó
        is_incoherent_quantity ("incoherente"), % fuera del piso/techo de sanidad
        ("fuera_de_rango") o la consulta falló ("error")
    incluir_cero=False (descuentos.py): un % recomendado de 0 se descarta como antes
    (cae en sin_recomendacion "fuera_de_rango"). incluir_cero=True (evaluador de cron
    y popup): se devuelve con percentage=0.0 para clasificar el tier; quien escriba
    tiene que aplicar el piso de coherencia (ver
    _construir_correccion_automatica_mayorista y tabs/salud.py::_tiers_plan),
    nunca mandar 0."""
    if not cantidades or not precio_base:
        return None
    propuesta: List[Dict[str, Any]] = []
    sin_rec: Dict[int, str] = {}
    for q in cantidades:
        rec = ml_get_pxq_recommendations(token, item_id, precio_base, [q])
        if rec is None:
            # error/5xx/timeout (ml_get_pxq_recommendations devuelve None): 1 reintento con
            # backoff corto. Un 204 ({"recommendations": []}) y is_incoherent_quantity NO se reintentan.
            time.sleep(1.0)
            rec = ml_get_pxq_recommendations(token, item_id, precio_base, [q])
        time.sleep(0.05)
        if rec is None:
            sin_rec[q] = "error"
            continue
        recs = rec.get("recommendations") or []
        if not recs:
            sin_rec[q] = "204"
            continue
        r = next((x for x in recs if x.get("quantity") == q), recs[0])
        if r.get("is_incoherent_quantity"):
            sin_rec[q] = "incoherente"
            continue
        pct = (r.get("discount") or {}).get("percentage")
        monto = r.get("amount")
        if pct is None or monto is None or pct < 0 or pct >= _PCT_TECHO_SANIDAD or (pct == 0 and not incluir_cero):
            sin_rec[q] = "fuera_de_rango"
            continue
        propuesta.append({"quantity": q, "amount": monto, "percentage": round(pct, 2)})
    return {"precio_base": precio_base, "propuesta": propuesta, "sin_recomendacion": sin_rec}


# Coherencia en lote (la usan el cron/popup y el botón 🔧 de tabs/salud_mayorista_fix.py)
_RONDAS_LOTE = 3
_NOTA_INCOHERENTE = "ML no admite esta cantidad hoy (incoherente con las demás)"


def _validar_coherencia_lote(token: str, item_id: str, base: float, qtys: Tuple[int, ...]) -> Tuple[Tuple[int, ...], List[int], Optional[str]]:
    """ML calcula is_incoherent_quantity contra las OTRAS cantidades de la misma consulta (de a una
    cantidad casi nunca se activa, y el POST después rechaza con 5598). Consulta el set candidato
    completo en UNA llamada (máx. 5), descarta las marcadas (también las ya cargadas) y reconsulta
    el resto hasta que ninguna quede marcada (máx. _RONDAS_LOTE rondas).
    Devuelve (set validado, descartadas, error). Con error el set NO está validado: no se escribe."""
    cand = tuple(sorted(qtys))
    descartadas: List[int] = []
    for _ in range(_RONDAS_LOTE):
        if not cand:
            return (), descartadas, None
        rec = ml_get_pxq_recommendations(token, item_id, base, list(cand))
        if rec is None:  # error de red / 5xx: 1 reintento con backoff corto
            time.sleep(1.0)
            rec = ml_get_pxq_recommendations(token, item_id, base, list(cand))
        if rec is None:
            return cand, descartadas, "ML no respondió la validación de coherencia en lote (error de consulta) — no se escribe, reintentá"
        marcadas = {x.get("quantity") for x in rec.get("recommendations") or [] if x.get("is_incoherent_quantity")} & set(cand)
        if not marcadas:
            return cand, descartadas, None
        descartadas += sorted(marcadas)
        cand = tuple(q for q in cand if q not in marcadas)
    if not cand:
        return (), descartadas, None
    return cand, descartadas, f"ML sigue marcando cantidades incoherentes tras {_RONDAS_LOTE} rondas — no se escribe"


# ---------------------------------------------------------------------------
# Evaluación unificada de mayorista para publicaciones gold_special (contado) --
# un solo motor: para cada cantidad objetivo (según el stock de la publicación,
# ver _qtys_mayorista_para_stock) compara lo cargado hoy contra la recomendación
# oficial de ML recalculada en el momento, y clasifica cada tier en:
#   - "crear": no hay tier cargado en esa cantidad, se ofrece el recomendado.
#   - "ok": hay tier cargado y está dentro del margen de lo recomendado hoy.
#   - "roto": el tier cargado da % negativo o cero (precio ≥ precio base) --
#     objetivo, sin ambigüedad, se ofrece corregir junto con "crear".
#   - "revisar": el tier cargado difiere de lo recomendado más allá del umbral,
#     pero no es "roto" -- se muestra como referencia (cargado vs. recomendado)
#     pero NUNCA se ofrece aplicar automático desde el popup.
# El umbral es UMBRAL_PP_MAYORISTA (1 pp por defecto) en % cargado vs. % recomendado,
# igual para cron y popup desde 2026-09-29. Los _DESVIO_* (4pp y 1.75x) ya no
# clasifican: solo los usa tabs/salud.py::_tiers_plan para bloquear ajustes manuales.
#
# Cantidades "extra" (cualquier tier ya cargado fuera del objetivo para el stock
# actual -- una cantidad no estándar como 7/15/20, O una cantidad que el stock
# actual ya no admite, ej. un tier de 10 cargado con stock=4) se evalúan con el
# mismo criterio ok/roto/revisar (marcadas "extra": True), nunca se ofrecen para
# "crear" -- el popup ya ofrece "eliminar" para cualquier tier cargado sea cual
# sea su estado (tabs/salud.py), así que un tier que sobra por stock bajo queda
# candidato a eliminar con el mismo mecanismo, sin código nuevo en el popup.
#
# Cantidad=1 se evalúa por el mismo camino genérico que cualquier otra cuando
# corresponde (stock 2-5, ver _qtys_mayorista_para_stock) -- sin tratamiento
# especial desde 2026-09-08 (antes tenía un bloque aparte, _pct_qty1_sano,
# reemplazado por la consulta directa a ML de arriba).
# ---------------------------------------------------------------------------

_DESVIO_PP_MIN = 4.0
_DESVIO_RATIO_MIN = 1.75


def _qtys_mayorista_para_stock(stock: Optional[int]) -> Tuple[int, ...]:
    """Cantidades objetivo de mayorista según el stock disponible de la
    publicación (Diego, 2026-09-29; reemplaza la regla del 2026-09-08):
      stock >= 10 -> (2, 3, 5, 10) | 5-9 -> (2, 3, 5) | 3-4 -> (2, 3)
      stock 2 -> (2,) | stock 1 -> (1,) (único caso con tier de 1 unidad) | 0 -> ()"""
    if not stock or stock < 1:
        return ()
    if stock >= 10:
        return (2, 3, 5, 10)
    if stock >= 5:
        return (2, 3, 5)
    if stock >= 3:
        return (2, 3)
    if stock == 2:
        return (2,)
    return (1,)


def _standard_amount_de(prices_body: dict) -> Optional[float]:
    for p in prices_body.get("prices") or []:
        cond = p.get("conditions") or {}
        if p.get("type") == "standard" and cond.get("min_purchase_unit") is None and not (cond.get("context_restrictions") or []):
            return float(p["amount"])
    return None


def _tiers_cargados_todos(prices_body: dict, precio_base: float) -> Dict[int, float]:
    """TODOS los tiers de mayorista cargados HOY, cualquier cantidad (no solo
    2/3/5/10) -- unifica legacy (prices[type=standard] con min_purchase_unit) y %
    B2B nuevo (price_per_quantity), ambos a monto absoluto para poder compararlos
    con el cálculo. Cantidad=1 (precio base, sin min_purchase_unit) queda afuera --
    no es un tier de mayorista, es el precio de referencia."""
    cargado: Dict[int, float] = {}
    for p in prices_body.get("prices") or []:
        cond = p.get("conditions") or {}
        mpu = cond.get("min_purchase_unit")
        if mpu is not None and p.get("amount") is not None:
            cargado[mpu] = float(p["amount"])
    for p in prices_body.get("price_per_quantity") or []:
        if p.get("type") != "discount_percentage":
            continue
        cond = p.get("conditions") or {}
        if cond.get("eligible") is False:
            continue
        mpu = cond.get("min_purchase_unit")
        pct = p.get("percentage")
        if mpu is not None and pct is not None:
            cargado[mpu] = round(precio_base * (1 - pct / 100), 2)
    return cargado


# Umbral (en puntos porcentuales) para clasificar un tier cargado como "mal" en el
# cron: |% cargado - % recomendado por ML| > umbral. Configurable por env
# (SALUD_MAYORISTA_UMBRAL_PP), default 1 pp (Diego, 2026-09-29). El popup NO lo
# usa (umbral_pp=None = regla legacy 4pp y 1.75x de arriba).
UMBRAL_PP_MAYORISTA = float(os.environ.get("SALUD_MAYORISTA_UMBRAL_PP", "1.0"))


def _promo_hasta_de(prices_body: dict, vigente: float, precio_base: float) -> Optional[str]:
    """end_time (ISO, UTC) de la promo ganadora (la que fija `vigente`), o None si no hay promo
    que baje el precio o el nodo no trae fecha."""
    if vigente >= precio_base:
        return None
    for p in prices_body.get("prices") or []:
        if (p.get("type") == "promotion" and p.get("amount") is not None
                and float(p["amount"]) == vigente
                and (p.get("conditions") or {}).get("min_purchase_unit") is None):
            return (p.get("conditions") or {}).get("end_time")
    return None


def _precio_vigente_de(prices_body: dict, precio_base: float) -> float:
    """Precio vigente de la publicación (lo que paga hoy un comprador de 1 unidad):
    el menor entre el precio de lista y cualquier nodo type=promotion de /prices.
    Verificado 2026-09-29 contra GET /items/{id}/sale_price en 10 publicaciones
    (SELLER_CAMPAIGN, SMART, DEAL, DEAL+SMART -> gana la promo más baja) -- sin
    llamada extra. El % de mayorista se aplica sobre ESTE precio, no sobre la lista."""
    montos = []
    for p in prices_body.get("prices") or []:
        if p.get("type") == "promotion" and p.get("amount") is not None:
            if (p.get("conditions") or {}).get("min_purchase_unit") is None:
                montos.append(float(p["amount"]))
    return min([precio_base] + montos)


def _evaluar_mayorista_gold_special(token: str, item: dict,
                                     prices_body: Optional[dict] = None,
                                     siempre_devolver: bool = False,
                                     umbral_pp: Optional[float] = None) -> Optional[Dict[str, Any]]:  # None = UMBRAL_PP_MAYORISTA
    """Evalúa las cantidades objetivo (según el stock actual de la publicación,
    ver _qtys_mayorista_para_stock) para UNA publicación gold_special. Devuelve
    None si no se puede evaluar (sin precio base) o -- si siempre_devolver=False,
    el default -- si todo está "ok" y no hay nada que mostrar (el popup no pasa
    este flag: quiere el atajo, así no satura la pantalla con ítems totalmente
    sanos).

    prices_body: si se pasa (el cron ya hizo su propio GET /prices para
    _wholesale_from_prices), se reusa en vez de pedirlo de nuevo. El popup NO lo
    pasa: siempre quiere el GET en vivo propio.

    siempre_devolver: el cron (audit_item) lo pasa en True -- distingue "no se pudo
    evaluar" (None real: sin precio base) de "se evaluó y todo está sano".

    umbral_pp: REGLA ÚNICA para cron y popup (Diego, 2026-09-29): "revisar" si
    |% cargado - % recomendado| > umbral_pp, en % y en ambos sentidos. Default
    UMBRAL_PP_MAYORISTA (env SALUD_MAYORISTA_UMBRAL_PP, 1 pp). El bloqueo de ajustes
    manuales exagerados de _tiers_plan (4 pp y 1,75x, _DESVIO_*) es aparte y no cambia.
    Los % recomendados de 0 se conservan (pct_calculado=0.0; quien escriba aplica el
    piso de coherencia, ver _tiers_plan). Cada tier trae diff_pp = cargado - recomendado
    (positivo = descuento más profundo que el mínimo de ML = precio más bajo).

    Las recomendaciones se piden de a UNA cantidad y el % de ML se usa directo
    (nunca se convierte un monto). Un tier cargado sin recomendación (204, cantidad
    incoherente o error de consulta) NO se evalúa por precio: queda estado "ok" con
    sin_recomendacion=True y sin_rec_motivo.

    El stock sale de item.get("available_quantity") -- el PxQ se configura por
    publicación (item_id), así que decide el stock de ESTA publicación.

    Tiers "extra" (cualquier cantidad cargada fuera del objetivo para el stock actual)
    se evalúan con el mismo criterio (marcados "extra": True) pero nunca se ofrecen
    para "crear". El popup ofrece "eliminar" para cualquier tier cargado.

    Devuelve además precio_vigente (base real sobre la que ML aplica el %)."""
    iid = item["id"]
    if prices_body is None:
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
    stock = item.get("available_quantity") or 0
    qtys_objetivo = _qtys_mayorista_para_stock(stock)
    cargado = _tiers_cargados_todos(prices_body, precio_base)
    extra_qtys = tuple(sorted(q for q in cargado if q not in qtys_objetivo))
    qtys_a_evaluar = sorted(set(qtys_objetivo) | set(extra_qtys))
    if umbral_pp is None:
        umbral_pp = UMBRAL_PP_MAYORISTA

    prop = _calcular_mayorista_recomendado(token, iid, precio_base, tuple(qtys_a_evaluar), incluir_cero=True)
    calculado = {p["quantity"]: p["amount"] for p in prop["propuesta"]} if prop else {}
    calculado_pct = {p["quantity"]: p["percentage"] for p in prop["propuesta"]} if prop else {}
    sin_rec: Dict[int, str] = dict(prop["sin_recomendacion"]) if prop else {}

    # Cantidades objetivo que ML no admite hoy: marcadas incoherentes de a una, o al validar el set
    # candidato EN LOTE (ML las calcula contra las otras cantidades de la misma consulta; sin esto el
    # POST rechaza con 5598). No cuentan como "faltan" ni "crear" y la auto-corrección nunca las manda.
    incoherentes = {q for q in qtys_objetivo if sin_rec.get(q) == "incoherente"}
    cand = tuple(q for q in qtys_objetivo if q not in incoherentes)
    if len(cand) >= 2:  # con una sola cantidad el lote es la misma consulta de arriba
        _v, descartadas, _err = _validar_coherencia_lote(token, iid, precio_base, cand)
        incoherentes |= set(descartadas)  # si el lote falla, queda lo detectado hasta ahí (se reevalúa cada noche)

    tiers: List[Dict[str, Any]] = []
    for q in qtys_a_evaluar:
        es_extra = q not in qtys_objetivo
        if q not in cargado:
            if q in incoherentes:
                continue  # ML no admite esta cantidad hoy: no se ofrece crearla
            if q in calculado:
                tiers.append({"quantity": q, "estado": "crear", "extra": es_extra,
                              "pct_calculado": calculado_pct[q], "monto_calculado": calculado[q]})
            continue  # sin tier cargado y sin cálculo posible -- no se puede ofrecer nada
        pct_cargado = round((precio_base - cargado[q]) / precio_base * 100, 2)
        pct_calc = None if q in incoherentes else calculado_pct.get(q)  # cargada pero incoherente: no se corrige
        if pct_calc is None:
            # Sin recomendación (204 / incoherente / error / fuera de rango): no se evalúa por
            # precio, solo presencia. TODO(mayorista-revisar-popup, 2026-09-04): un tier con
            # pct_cargado<=0 (roto) cae acá y queda "ok" -- "roto" necesita pct_calculado para
            # que el popup sugiera la corrección; evaluar aparte.
            t = {"quantity": q, "estado": "ok", "extra": es_extra, "pct_cargado": pct_cargado, "monto_cargado": cargado[q]}
            if q in sin_rec or q in incoherentes:
                t["sin_recomendacion"] = True
                t["sin_rec_motivo"] = sin_rec.get(q, "incoherente")
            tiers.append(t)
            continue
        base_t = {"quantity": q, "extra": es_extra, "pct_cargado": pct_cargado, "monto_cargado": cargado[q],
                  "pct_calculado": pct_calc, "monto_calculado": calculado[q],
                  "diff_pp": round(pct_cargado - pct_calc, 2)}
        if pct_cargado <= 0:
            tiers.append({**base_t, "estado": "roto"})
            continue
        tiers.append({**base_t, "estado": "revisar" if abs(base_t["diff_pp"]) > umbral_pp else "ok"})

    presentes = sorted(cargado.keys())
    invertido = any(cargado[presentes[i]] < cargado[presentes[i + 1]] for i in range(len(presentes) - 1))

    if not siempre_devolver and not any(t["estado"] != "ok" for t in tiers) and not invertido:
        return None  # todo está ok (o no evaluable) y no hay inversión -- nada para mostrar (popup)

    return {"precio_base": precio_base, "precio_vigente": _precio_vigente_de(prices_body, precio_base),
            "tiers": tiers, "invertido": invertido, "incoherentes": sorted(incoherentes)}


# ---------------------------------------------------------------------------
# Margen por tier (Diego, 2026-09-29) -- SOLO informativo: nunca bloquea ni cambia
# una escritura. Precio de cada tier = precio_vigente x (1 - %), margen con
# margen._calc_margen_prod (misma función que el Dashboard; contado, no resta
# financiación de cuotas ni envío por múltiples unidades).
# ---------------------------------------------------------------------------

_MARGEN_PARAMS_CACHE: Dict[int, dict] = {}


def _margen_params(user_id: int) -> dict:
    if user_id not in _MARGEN_PARAMS_CACHE:
        _MARGEN_PARAMS_CACHE[user_id] = _load_params_prod(user_id)
    return _MARGEN_PARAMS_CACHE[user_id]


def _agregar_margen(ev: Dict[str, Any], sku: str, user_id: int) -> None:
    """Agrega a cada tier de `ev` margen_cargado / margen_recomendado (ARS por unidad
    al precio del tier), precio_cargado / precio_recomendado y margen_negativo
    (True si alguno de los dos es < 0; None si no hay costo del SKU). Deja
    ev["margen_negativo"] = True/False/None a nivel publicación."""
    costo = get_producto_costo(sku, user_id) if sku else None
    if not costo:
        ev["margen_negativo"] = None
        return
    costo_usd, tipo_iva = costo
    params = _margen_params(user_id)
    vigente = ev["precio_vigente"]
    hay_negativo = False
    hay_dato = False
    for t in ev["tiers"]:
        negativos: List[Optional[bool]] = []
        for clave, pct in (("cargado", t.get("pct_cargado")), ("recomendado", t.get("pct_calculado"))):
            if pct is None:
                continue
            precio = round(vigente * (1 - pct / 100), 2)
            m = _calc_margen_prod(precio, costo_usd, tipo_iva, params, cantidad=t["quantity"])
            t[f"precio_{clave}"] = precio
            t[f"margen_{clave}"] = None if m is None else round(m, 2)
            if m is not None:
                negativos.append(m < 0)
        if negativos:
            t["margen_negativo"] = any(negativos)
            hay_dato = True
            hay_negativo = hay_negativo or t["margen_negativo"]
        else:
            t["margen_negativo"] = None
    ev["margen_negativo"] = hay_negativo if hay_dato else None


# ---------------------------------------------------------------------------
# Auto-corrección nocturna de mayorista (Diego, 2026-10-01): ya NO vive acá. La hace
# salud_mayorista_motor.autocorregir_usuario con el mismo motor que la 🔧 en modo directo
# (un solo camino de escritura); este módulo sigue evaluando y persistiendo el snapshot.
# ---------------------------------------------------------------------------

def _tiers_previos_json(prices_body: Optional[dict]) -> str:
    """Set COMPLETO de tiers de mayorista cargados hoy (sistema % nuevo y absoluto
    legacy) serializado a JSON, para dejar en valor_anterior de ml_escrituras y en
    mayorista_correcciones_automaticas.tiers_anteriores_json antes de escribir."""
    tiers: List[Dict[str, Any]] = []
    for p in (prices_body or {}).get("price_per_quantity") or []:
        if p.get("type") != "discount_percentage":
            continue
        c = p.get("conditions") or {}
        tiers.append({"sistema": "pct", "quantity": c.get("min_purchase_unit"), "percentage": p.get("percentage"),
                      "id": p.get("id"), "eligible": c.get("eligible")})
    for p in (prices_body or {}).get("prices") or []:
        c = p.get("conditions") or {}
        if c.get("min_purchase_unit") is not None:
            tiers.append({"sistema": "absoluto", "quantity": c.get("min_purchase_unit"), "amount": p.get("amount"),
                          "id": p.get("id")})
    tiers.sort(key=lambda t: (t.get("quantity") is None, t.get("quantity") or 0))
    return json.dumps(tiers, ensure_ascii=False)


def _firma_tiers(prices_body: Optional[dict]) -> list:
    """Firma comparable del estado que se evaluó: precio de lista, precio vigente y TODOS los
    tiers cargados (sistema % y absoluto). Se guarda al evaluar y se compara con un GET /prices
    fresco justo antes de escribir (ver salud_mayorista_motor.aplicar_publicacion): si difiere, alguien
    cambió tiers o precio entre medio y la recomendación quedó vieja."""
    body = prices_body or {}
    base = _standard_amount_de(body)
    firma: list = [("base", None, base, None)]
    if base:
        firma.append(("vigente", None, _precio_vigente_de(body, base), None))
    for p in body.get("price_per_quantity") or []:
        if p.get("type") != "discount_percentage":
            continue
        c = p.get("conditions") or {}
        firma.append(("pct", c.get("min_purchase_unit"), p.get("percentage"), c.get("eligible")))
    for p in body.get("prices") or []:
        c = p.get("conditions") or {}
        if c.get("min_purchase_unit") is not None:
            firma.append(("abs", c.get("min_purchase_unit"), p.get("amount"), None))
    return sorted(firma, key=lambda x: (x[0], x[1] is None, x[1] or 0))


def _mayorista_revisar_payload(token: str, item: dict, prices_body: dict, user_id: Optional[int], sku: str) -> Dict[str, Any]:
    """Arma el dict que se persiste en salud_item_snapshots.mayorista_revisar_json
    para UNA publicación gold_special con tiers cargados.
      - status != active -> {"evaluable": True, "motivo": "no_activa"} (sin llamadas a ML)
      - active con stock 0 -> motivo "sin_stock"
      - active con stock >= 1 -> evaluación completa (recomendaciones de a una cantidad)
    Solo evalúa y arma el payload: nunca escribe a ML (la corrección la hace
    salud_mayorista_motor.autocorregir_usuario después de guardar el snapshot)."""
    status = item.get("status")
    stock = item.get("available_quantity") or 0
    if status != "active":
        return {"evaluable": True, "motivo": "no_activa", "status": status, "stock": stock}
    if stock < 1:
        return {"evaluable": True, "motivo": "sin_stock", "stock": stock}
    ev = _evaluar_mayorista_gold_special(token, item, prices_body=prices_body, siempre_devolver=True,
                                         umbral_pp=UMBRAL_PP_MAYORISTA)
    if ev is None:
        # con siempre_devolver=True, None es inequívoco: no se pudo evaluar (sin precio base)
        return {"evaluable": False}
    if user_id is not None:
        _agregar_margen(ev, sku, user_id)
    tiers_revisar = [t for t in ev["tiers"] if t["estado"] in ("revisar", "roto")]
    payload: Dict[str, Any] = {
        "evaluable": True, "invertido": ev["invertido"], "tiers_revisar": tiers_revisar,
        "precio_vigente": ev["precio_vigente"], "umbral_pp": UMBRAL_PP_MAYORISTA,
        "margen_negativo": ev.get("margen_negativo"),
        "incoherentes": ev.get("incoherentes") or [],
        "tiers_eval": [
            {k: t.get(k) for k in ("quantity", "estado", "extra", "pct_cargado", "pct_calculado", "diff_pp",
                                   "sin_recomendacion", "sin_rec_motivo", "margen_negativo", "margen_cargado")
             if t.get(k) is not None}
            for t in ev["tiers"]
        ],
    }
    sin_rec = {str(t["quantity"]): t.get("sin_rec_motivo") for t in ev["tiers"] if t.get("sin_recomendacion")}
    if sin_rec:
        payload["tiers_sin_recomendacion"] = sin_rec
    return payload


def audit_item(token: str, item: dict, cat_attrs_cache: Dict[str, list],
                seller_id: str = "", session: Optional[requests.Session] = None,
                user_id: Optional[int] = None) -> Dict[str, Any]:
    """Audita UN ítem propio ya traído (item = body completo de /items/{id} o del
    multiget). Devuelve el dict de columnas crudas para salud_item_snapshots.
    Nunca levanta excepción: cualquier llamada que falle deja su campo en None
    y agrega el motivo a data['error'] (concatenado, no pisa errores previos)."""
    S = session or requests
    H = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    iid = item["id"]
    errores: List[str] = []

    data: Dict[str, Any] = {
        "sku": _get_seller_sku(item),
        "catalog_listing": bool(item.get("catalog_listing")),
        "status": item.get("status"),
        "listing_type_id": item.get("listing_type_id"),
        "condicion": item.get("condition"),
        "gtin": "",
        "descripcion_len": None,
        "short_status": None,
        "fotos_cantidad": len(item.get("pictures") or []),
        "mayorista_estado": None,
        "mayorista_tiers_json": None,
        "mayorista_revisar_json": None,
        "flex_status": None,
        "retiro_persona": None,
        "garantia_tipo": "",
        "garantia_tiempo": "",
        "envio_gratis": None,
        "regulatoria_estado": "no_determinable",
        "atributos_faltantes_editables": None,
        "atributos_faltantes_bloqueados": None,
        "atributos_faltantes_json": None,
        "performance_score": None,
        "price": item.get("price"),
        "price_vigente": None,
        "promo_hasta": None,
    }

    # Motivo declarado de GTIN vacío (EMPTY_GTIN_REASON, ver doc "identificadores-de-productos"):
    # va dentro de atributos_faltantes_json["gtin_motivo"] (value_id), NO en `gtin` -- ese campo
    # es solo el código real. Un value_id "-1" (N/A) no es un motivo.
    gtin_motivo = ""
    for attr in item.get("attributes") or []:
        if attr.get("id") == "GTIN":
            data["gtin"] = (attr.get("value_name") or "").strip()
        elif attr.get("id") == "EMPTY_GTIN_REASON" and str(attr.get("value_id") or "") not in ("", "-1"):
            gtin_motivo = str(attr["value_id"])

    shipping = item.get("shipping") or {}
    data["retiro_persona"] = bool(shipping.get("local_pick_up"))
    data["envio_gratis"] = bool(shipping.get("free_shipping"))

    for term in item.get("sale_terms") or []:
        if term.get("id") == "WARRANTY_TYPE":
            data["garantia_tipo"] = term.get("value_name") or ""
        elif term.get("id") == "WARRANTY_TIME":
            data["garantia_tiempo"] = term.get("value_name") or ""

    try:
        r = S.get(f"{ML_API}/items/{iid}/description", headers=H, timeout=15)
        if r.status_code == 200:
            body = r.json()
            texto = body.get("plain_text") or body.get("text") or ""
            data["descripcion_len"] = len(texto.strip())
        elif r.status_code == 404:
            data["descripcion_len"] = 0
        else:
            errores.append(f"description status={r.status_code} {_err_detalle(r)}")
    except requests.exceptions.RequestException as e:
        errores.append(f"description error={e}")

    prices_body_para_revisar: Optional[dict] = None
    tiene_tiers_cargados = False
    try:
        r = S.get(f"{ML_API}/items/{iid}/prices", headers={**H, "show-all-prices": "TRUE"}, timeout=15)
        if r.status_code == 200:
            prices_body_para_revisar = r.json()
            # Precio vigente (con promo) para la columna Precio de Salud: sin llamada extra
            _base = _standard_amount_de(prices_body_para_revisar) or item.get("price")
            if _base:
                data["price_vigente"] = _precio_vigente_de(prices_body_para_revisar, float(_base))
                data["promo_hasta"] = _promo_hasta_de(prices_body_para_revisar, data["price_vigente"], float(_base))
            w = _wholesale_from_prices(prices_body_para_revisar)
            data["mayorista_estado"] = w["estado"]
            tiene_tiers_cargados = bool(w["tiers"])
            import json as _json
            data["mayorista_tiers_json"] = _json.dumps(
                {"standard_amount": w["standard_amount"], "tiers": w["tiers"]}, ensure_ascii=False
            )
        else:
            errores.append(f"prices status={r.status_code} {_err_detalle(r)}")
    except requests.exceptions.RequestException as e:
        errores.append(f"prices error={e}")

    # Mayorista "revisar"/"invertido" -- solo gold_special con >=1 tier cargado (sin
    # nada cargado no hay contra qué comparar). Desde 2026-09-29 el cron evalúa
    # COMPLETO (recomendaciones de ML de a una cantidad) toda publicación ACTIVA con
    # stock >= 1; las pausadas/cerradas no se consultan a ML y quedan con motivo
    # "no_activa" (activas con stock 0: "sin_stock"). Ver _mayorista_revisar_payload.
    # Se PERSISTE en el snapshot -- así la tabla resumen muestra el ⚠️ sin recalcular
    # en cada render (ver _mayorista_dim). "evaluable": false = se intentó pero no se
    # pudo evaluar -- no cuenta como sano ni como revisar, queda "sin evaluar".
    if item.get("listing_type_id") == "gold_special" and seller_id and tiene_tiers_cargados:
        try:
            payload = _mayorista_revisar_payload(
                token, item, prices_body_para_revisar, user_id, data["sku"],
            )
            # stock de ESTA publicación y si tiene tiers legacy (monto absoluto): los usa el ícono 🔧
            # de la tabla (tabs/salud_mayorista_fix.py) sin llamar a ML
            payload.setdefault("stock", item.get("available_quantity") or 0)
            payload["legacy_abs"] = any(
                (p.get("conditions") or {}).get("min_purchase_unit") is not None
                for p in (prices_body_para_revisar or {}).get("prices") or []
            )
            data["mayorista_revisar_json"] = json.dumps(payload, ensure_ascii=False)
        except Exception as e:
            errores.append(f"mayorista_revisar error={e}")

    try:
        r = S.get(f"{ML_API}/item/{iid}/performance", headers=H, timeout=15)
        if r.status_code == 200:
            perf = r.json()
            data["performance_score"] = perf.get("score")
            for bucket in perf.get("buckets") or []:
                variables = {v.get("key"): v for v in (bucket.get("variables") or [])}
                if bucket.get("key") == "USER_PRODUCT":
                    if not data["gtin"] and "UP_GTIN" in variables:
                        pass  # el valor crudo del GTIN ya sale del atributo; acá solo el status si faltara
                    if "UP_SHORTS" in variables:
                        data["short_status"] = variables["UP_SHORTS"].get("status")
                elif bucket.get("key") == iid:
                    if "UP_ME_FLEX_ITEM_OPTIN" in variables:
                        data["flex_status"] = variables["UP_ME_FLEX_ITEM_OPTIN"].get("status")
        elif r.status_code == 404:
            data["short_status"] = "no_determinable"
            data["flex_status"] = "no_determinable"
        elif r.status_code == 400 and "Product items are not supported" in r.text:
            # Confirmado en vivo: /item/{id}/performance no calcula entidad para
            # items de catálogo (solo existe USER_PRODUCT sobre la publicación
            # propia) -- no es un error, es no aplicable a este item.
            data["short_status"] = "no_aplica_catalogo"
            data["flex_status"] = "no_aplica_catalogo"
        elif r.status_code == 400 and "Only status active is supported" in r.text:
            # Confirmado en vivo (478/2366 items de user_id=1): tampoco calcula
            # entidad para publicaciones pausadas/cerradas/pendientes -- misma
            # familia de "no aplica", no un error.
            data["short_status"] = "no_aplica_no_activo"
            data["flex_status"] = "no_aplica_no_activo"
        else:
            errores.append(f"performance status={r.status_code} {_err_detalle(r)}")
    except requests.exceptions.RequestException as e:
        errores.append(f"performance error={e}")

    cat_id = item.get("category_id")
    if cat_id:
        if cat_id not in cat_attrs_cache:
            try:
                r = S.get(f"{ML_API}/categories/{cat_id}/attributes", headers=H, timeout=15)
                cat_attrs_cache[cat_id] = r.json() if r.status_code == 200 else []
                if r.status_code != 200:
                    errores.append(f"categories/{cat_id}/attributes status={r.status_code}")
            except requests.exceptions.RequestException as e:
                cat_attrs_cache[cat_id] = []
                errores.append(f"categories/{cat_id}/attributes error={e}")

        cat_attrs = cat_attrs_cache.get(cat_id) or []
        item_attr_ids = {a.get("id") for a in item.get("attributes") or [] if a.get("id")}
        condicion = (item.get("condition") or "").lower()
        hidden_tag_por_condicion = {"new": "new_hidden", "used": "used_hidden"}.get(condicion)
        editables, bloqueados, opcionales = [], [], []
        for a in cat_attrs:
            aid = a.get("id")
            tags = a.get("tags") or {}
            if not aid or tags.get("hidden"):
                continue
            if hidden_tag_por_condicion and tags.get(hidden_tag_por_condicion):
                continue
            if aid in item_attr_ids:
                continue
            entry = {"id": aid, "name": a.get("name") or aid}
            # Confirmado en vivo el 2026-09-07 (LIGHT_COLOR en AKGN5HYBRIDBLKAM-BDC):
            # ML expone en /categories/{id}/attributes TODOS los atributos de la
            # categoría, no solo los obligatorios -- tags.required es lo único que
            # distingue "ML lo exige" de "existe como opción, completalo si querés
            # mejor SEO/ficha técnica". Sin este filtro, cualquier atributo opcional
            # sin cargar contaba como "pendiente" igual que uno realmente obligatorio.
            if not tags.get("required"):
                opcionales.append(entry)
                continue
            (bloqueados if tags.get("read_only") else editables).append(entry)
        data["atributos_faltantes_editables"] = len(editables)
        data["atributos_faltantes_bloqueados"] = len(bloqueados)
        import json as _json
        faltantes_payload: Dict[str, Any] = {"editables": editables, "bloqueados": bloqueados, "opcionales": opcionales}
        if gtin_motivo:
            faltantes_payload["gtin_motivo"] = gtin_motivo
        data["atributos_faltantes_json"] = _json.dumps(faltantes_payload, ensure_ascii=False)

    data["error"] = " | ".join(errores) if errores else None
    return data


def write_snapshot(conn, user_id: int, item_id: str, data: Dict[str, Any], snapshot_date: str) -> None:
    cols = [
        "sku", "catalog_listing", "status", "listing_type_id", "condicion", "gtin",
        "descripcion_len", "short_status", "fotos_cantidad", "mayorista_estado",
        "mayorista_tiers_json", "mayorista_revisar_json", "flex_status", "retiro_persona", "garantia_tipo",
        "garantia_tiempo", "envio_gratis", "regulatoria_estado",
        "atributos_faltantes_editables", "atributos_faltantes_bloqueados",
        "atributos_faltantes_json", "performance_score", "price", "price_vigente", "promo_hasta", "error",
    ]
    placeholders = ", ".join(["?"] * (len(cols) + 3))
    set_clause = ", ".join(f"{c}=excluded.{c}" for c in cols)
    conn.execute(
        f"""
        INSERT INTO salud_item_snapshots (user_id, item_id, snapshot_date, {", ".join(cols)})
        VALUES ({placeholders})
        ON CONFLICT(user_id, item_id, snapshot_date) DO UPDATE SET {set_clause}
        """,
        [user_id, item_id, snapshot_date] + [data.get(c) for c in cols],
    )


def audit_sku(user_id: int, seller_id: str, sku: str, persist: bool = True) -> Dict[str, Any]:
    """Corrida on-demand de UN SKU (su familia propia+catálogo, ~10 ítems). La
    llama el popup de detalle de Salud, con spinner en la UI.

    Rápido por diseño: en vez de re-escanear las ~2000+ publicaciones del
    catálogo completo (fetch_all_own_items, ~1-2 min), toma los item_id del
    último snapshot guardado para este SKU y hace un multiget directo. Solo
    cae al escaneo completo si el SKU nunca fue auditado todavía (primera vez,
    sin snapshot previo del que partir).

    persist=True (default) además graba el snapshot de HOY con lo recién
    leído -- así la fila de la tabla de Salud queda al día sin esperar al
    cron nocturno. persist=False es solo para inspección sin tocar la DB.

    Devuelve {"sku", "items": [{"item": <item crudo>, "audit": <data de
    audit_item>}, ...]} -- el item crudo se necesita para clasificar los
    hallazgos en el popup (tags de cuotas, catalog_listing, atributos con
    valor ya cargado en otra publicación del grupo, etc).
    """
    token = get_ml_access_token(user_id)
    if not token:
        return {"error": "sin_token"}

    conn = get_connection()
    prev_ids = [
        r["item_id"] for r in conn.execute(
            "SELECT DISTINCT item_id FROM salud_item_snapshots WHERE user_id=? AND sku=?",
            (user_id, sku),
        ).fetchall()
    ]

    session = requests.Session()
    # Publicaciones nuevas del SKU que todavía no están en ningún snapshot (el cron corre una vez
    # por noche): se buscan en ML por seller_sku (active + paused, sin cerradas) y se unen a las
    # conocidas. Si el search falla se sigue solo con las conocidas.
    ids_ml: List[str] = []
    if seller_id:
        for st in ("active", "paused"):
            try:
                r = session.get(
                    f"{ML_API}/users/{seller_id}/items/search",
                    params={"seller_sku": sku, "status": st},
                    headers={"Authorization": f"Bearer {token}"}, timeout=15,
                )
                if r.status_code == 200:
                    ids_ml.extend(r.json().get("results") or [])
                else:
                    log.warning("audit_sku %s: items/search seller_sku status=%s -> HTTP %s", sku, st, r.status_code)
            except Exception:  # noqa: BLE001 -- el search es un extra, no debe romper el popup
                log.exception("audit_sku %s: items/search seller_sku status=%s fallo", sku, st)
    prev_ids = list(dict.fromkeys(prev_ids + ids_ml))
    group: List[dict] = []
    if prev_ids:
        for i in range(0, len(prev_ids), 20):
            batch = prev_ids[i:i + 20]
            r = session.get(
                f"{ML_API}/items", params={"ids": ",".join(batch)},
                headers={"Authorization": f"Bearer {token}"}, timeout=30,
            )
            if r.status_code == 200:
                for entry in r.json():
                    if entry.get("code") == 200 and _get_seller_sku(entry["body"]) == sku:
                        group.append(entry["body"])
    if not group:
        # sin snapshot previo (o SKU cambió de item_ids) -- fallback al escaneo completo
        items = fetch_all_own_items(token, seller_id)
        group = [it for it in items if _get_seller_sku(it) == sku]
    if not group:
        conn.close()
        return {"error": "sku_sin_items_propios", "sku": sku}

    if persist:
        init_salud_tables()
    hoy = date.today().isoformat()
    cat_attrs_cache: Dict[str, list] = {}
    resultados = []
    for it in group:
        data = audit_item(token, it, cat_attrs_cache, seller_id, session)
        if persist:
            write_snapshot(conn, user_id, it["id"], data, hoy)
        resultados.append({"item": it, "audit": data})
        time.sleep(0.08)
    if persist:
        conn.commit()
    conn.close()
    return {"sku": sku, "items": resultados}


def audit_skus_nuevos(user_id: int, seller_id: str, skus: List[str]) -> Dict[str, Any]:
    """Corrida on-demand para SKUs que TODAVÍA no tienen ningún snapshot (botón
    "Auditar SKUs nuevos" de tabs/salud.py). A diferencia de audit_sku() -- que
    siempre cae al escaneo completo (fetch_all_own_items) cuando el SKU no tiene
    snapshot previo del que partir -- esta función hace UN SOLO fetch_all_own_items()
    compartido para todos los `skus` de una sola vez, en vez de repetir el scan
    completo de la cuenta (~1500-7800 publicaciones según la cuenta) una vez por
    cada SKU nuevo.

    Devuelve {"auditados": [sku,...], "huerfanos": [sku,...], "n_items": int}.
    huerfanos = SKUs de `skus` sin ningún ítem propio encontrado en el scan (nunca
    se publicaron, o el resync todavía no los marcó sku_no_encontrado) -- no se
    auditan porque no hay nada que auditar; el caller los lista aparte.

    Puede levantar requests.exceptions.RequestException (incluido HTTPError de
    fetch_all_own_items) -- ni esta función ni fetch_all_own_items tienen
    retry/backoff propio (a diferencia de ml_api.get_ml_session()). El caller
    (botón de tabs/salud.py) debe capturarla y mostrar un mensaje en vez de
    romper la UI."""
    token = get_ml_access_token(user_id)
    if not token:
        return {"error": "sin_token"}

    items = fetch_all_own_items(token, seller_id)

    skus_set = set(skus)
    por_sku: Dict[str, List[dict]] = defaultdict(list)
    for it in items:
        sku_it = _get_seller_sku(it)
        if sku_it in skus_set:
            por_sku[sku_it].append(it)

    huerfanos = sorted(skus_set - set(por_sku.keys()))
    a_auditar = [it for grp in por_sku.values() for it in grp]

    init_salud_tables()
    hoy = date.today().isoformat()
    cat_attrs_cache: Dict[str, list] = {}
    conn = get_connection()
    session = requests.Session()
    try:
        for it in a_auditar:
            data = audit_item(token, it, cat_attrs_cache, seller_id, session)
            write_snapshot(conn, user_id, it["id"], data, hoy)
            time.sleep(0.08)
        conn.commit()
    finally:
        conn.close()

    return {
        "auditados": sorted(por_sku.keys()),
        "huerfanos": huerfanos,
        "n_items": len(a_auditar),
    }


def _run_user(user_id: int, seller_id: str) -> Dict[str, Any]:
    token = get_ml_access_token(user_id)
    if not token:
        return {"error": "sin_token"}
    items = fetch_all_own_items(token, seller_id)
    log.info("user_id=%s: %d publicaciones propias a auditar", user_id, len(items))

    hoy = date.today().isoformat()
    cat_attrs_cache: Dict[str, list] = {}
    conn = get_connection()
    session = requests.Session()
    n_errores = 0
    for idx, it in enumerate(items):
        data = audit_item(token, it, cat_attrs_cache, seller_id, session, user_id=user_id)
        if data.get("error"):
            n_errores += 1
        write_snapshot(conn, user_id, it["id"], data, hoy)
        if idx % 100 == 0:
            conn.commit()
            log.info("user_id=%s progreso: %d/%d", user_id, idx, len(items))
        time.sleep(0.08)
    conn.commit()

    conn.close()

    # Mayorista automático (reemplaza a la auto-corrección vieja): mismo motor que la 🔧 en modo directo,
    # para todos los SKUs con 🔧 en el snapshot que se acaba de guardar. Un fallo acá no invalida la auditoría
    # (ya está guardada): se anota en cron_runs y la corrida queda "partial".
    nota = None
    try:
        from salud_mayorista_motor import autocorregir_usuario  # import diferido: el motor importa este módulo
        token = get_ml_access_token(user_id) or token  # la auditoría tarda minutos: token fresco para escribir
        res = autocorregir_usuario(token, user_id, seller_id, hoy, log=log)
        log_salud_mayorista_cron(hoy, user_id, res)
        if res.get("desactivado"):
            log.info("user_id=%s: mayorista automatico desactivado (app_config mayorista_auto_user_%s)", user_id, user_id)
        else:
            log.info("user_id=%s: mayorista automatico: %d SKUs con llave, %d pubs a escribir, %d escritas, %d con error, "
                   "%d salteadas por margen negativo, %d por cambio concurrente%s", user_id, res["skus_con_llave"],
                   res["pubs_a_escribir"], res["pubs_escritas"], len(res["errores"]), len(res["salteadas_margen"]),
                   len(res["cambio_concurrente"]), " -- FRENADO" if res["frenado"] else "")
        nota = res["freno_detalle"]
    except Exception as e:  # noqa: BLE001
        log.exception("user_id=%s: el mayorista automatico fallo", user_id)
        nota = f"mayorista automatico fallo: {e}"
    return {"items_procesados": len(items), "errores": n_errores, "nota": nota}


def run(only_user: Optional[int] = None) -> None:
    init_cron_runs_db()
    init_salud_tables()
    log.info("=== Salud audit %s ===", date.today().isoformat())
    conn = get_connection()
    creds = conn.execute("SELECT DISTINCT user_id, raw_data FROM ml_credentials").fetchall()
    conn.close()

    for user_id, raw_data in creds:
        if only_user is not None and user_id != only_user:
            continue
        import json as _json
        try:
            seller_id = str(_json.loads(raw_data or "{}").get("user_id") or "")
        except Exception as e:
            log.error("user_id=%s: raw_data invalido (%s)", user_id, e)
            log_cron_run("salud_audit", user_id, "fail", 0, 0, f"raw_data invalido: {e}")
            continue
        if not seller_id:
            log_cron_run("salud_audit", user_id, "fail", 0, 0, "sin seller_id")
            continue

        t0 = time.time()
        try:
            result = _run_user(user_id, seller_id)
        except Exception as e:
            log.exception("user_id=%s: corrida abortada", user_id)
            log_cron_run("salud_audit", user_id, "fail", 0, time.time() - t0, str(e))
            continue
        if "error" in result:
            log_cron_run("salud_audit", user_id, "fail", 0, time.time() - t0, result["error"])
            continue
        nota = result.get("nota")
        status = "ok" if (result["errores"] == 0 and not nota) else "partial"
        log.info("user_id=%s: %s", user_id, result)
        # el dashboard parsea "N items con error" al INICIO del texto: la nota del freno va después
        msg = "; ".join(x for x in (f"{result['errores']} items con error" if result["errores"] else None, nota) if x) or None
        log_cron_run("salud_audit", user_id, status, result["items_procesados"], time.time() - t0, msg)
        time.sleep(1)

    _reportar_resumen_mayorista_cron(date.today().isoformat())
    _reportar_resumen_flags_pendientes(date.today().isoformat())


def _reportar_resumen_mayorista_cron(fecha: str) -> None:
    """Resumen en /var/log/pythonml_salud.log de lo que hizo esta noche el mayorista automatico
    (todas las cuentas), leido de salud_mayorista_cron."""
    conn = get_connection()
    try:
        filas = conn.execute("SELECT * FROM salud_mayorista_cron WHERE fecha=? ORDER BY user_id", (fecha,)).fetchall()
    finally:
        conn.close()
    log.info("=== RESUMEN mayorista automatico -- %s ===", fecha)
    if not filas:
        log.info("  (no corrio / sin resumen guardado)")
    for f in filas:
        log.info("user_id=%s: %d escritas de %d a escribir | %d error | %d salteadas por margen negativo | %d cambio concurrente%s",
                 f["user_id"], f["pubs_escritas"], f["pubs_a_escribir"], f["pubs_error"], f["pubs_margen_neg"],
                 f["pubs_concurrente"], " | FRENADO" if f["frenado"] else "")
        try:
            det = json.loads(f["detalle_json"] or "{}")
        except (TypeError, ValueError):
            det = {}
        for e in det.get("errores", []):
            log.info("  ERROR sku=%s item=%s (%s): %s", e.get("sku"), e.get("item_id"), e.get("fase"), e.get("error"))
        for e in det.get("salteadas_margen", []):
            log.info("  MARGEN NEGATIVO (no se toco) sku=%s item=%s", e.get("sku"), e.get("item_id"))


def _reportar_resumen_flags_pendientes(fecha: str) -> None:
    """Complementa el resumen de arriba: cuántos tiers quedaron flageados como
    "revisar" SIN auto-corregir hoy, separando pérdida real bloqueada (tope de
    5 / conflicto de coherencia -- requieren revisión manual en el popup) de
    falso positivo (no requiere ninguna acción, el % que se ve no es
    comparable por el artefacto de base de /recommendations)."""
    import json as _json
    conn = get_connection()
    filas = conn.execute(
        "SELECT user_id, item_id, sku, mayorista_revisar_json FROM salud_item_snapshots "
        "WHERE snapshot_date=? AND mayorista_revisar_json IS NOT NULL",
        (fecha,),
    ).fetchall()
    conn.close()
    pendientes_perdida_real = []
    n_falsos_positivos = 0
    for f in filas:
        try:
            rj = _json.loads(f["mayorista_revisar_json"])
        except Exception:
            continue
        if not rj.get("evaluable") or not rj.get("tiers_revisar"):
            continue
        motivo = rj.get("motivo_no_autocorregido")
        for t in rj["tiers_revisar"]:
            mc, mcalc = t.get("monto_cargado"), t.get("monto_calculado")
            diff = t.get("diff_pp")
            # desde 2026-09-29 "pérdida real" = descuento más profundo que el mínimo de ML
            # (diff_pp > 0, en %); snapshots viejos no traen diff_pp: comparación en $ como antes.
            if (diff is not None and diff > 0) or (diff is None and mc is not None and mcalc is not None and mc < mcalc):
                pendientes_perdida_real.append({
                    "user_id": f["user_id"], "item_id": f["item_id"], "sku": f["sku"],
                    "quantity": t.get("quantity"), "motivo": motivo or "sin_evaluar_aun",
                })
            else:
                n_falsos_positivos += 1
    log.info(
        "Tiers flageados SIN tocar por perdida real bloqueada (tope de 5 / conflicto de coherencia): %d",
        len(pendientes_perdida_real),
    )
    for p in pendientes_perdida_real:
        log.info("  item=%s sku=%s user_id=%s qty=%s+: motivo=%s", p["item_id"], p["sku"], p["user_id"], p["quantity"], p["motivo"])
    log.info("Tiers flageados SIN tocar por falso positivo (no es perdida real, no requiere accion): %d", n_falsos_positivos)


def _access_token_sin_refresh(user_id: int) -> Tuple[Optional[str], str]:
    """Access token vigente TAL CUAL está en la base. NUNCA refresca (el refresh token
    de ML es de un solo uso y el servicio en producción podría estar renovándolo a la
    vez). Devuelve (token, motivo): token=None si falta o está vencido / vence en <5 min."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT access_token, expires_at FROM ml_credentials WHERE user_id=? ORDER BY id DESC LIMIT 1",
            (user_id,),
        ).fetchone()
    finally:
        conn.close()
    if not row or not row["access_token"]:
        return None, "sin_token"
    exp = row["expires_at"]
    try:
        exp_dt = datetime.strptime(str(exp)[:19].replace("T", " "), "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return None, f"expires_at ilegible ({exp!r})"
    ahora = datetime.now(timezone.utc).replace(tzinfo=None)
    if (exp_dt - ahora).total_seconds() < 300:
        return None, f"vencido (expires_at={exp}, ahora UTC={ahora.isoformat(timespec='seconds')})"
    return row["access_token"], f"vigente hasta {exp}"


def dry_run_mayorista(user_ids: Optional[List[int]], out_path: str, opt_in: Optional[Dict[int, bool]] = None) -> None:
    """DRY-RUN del mayorista automatico: usa el ULTIMO snapshot de cada usuario, planifica los SKUs con 🔧
    exactamente como el cron real y vuelca el resumen (JSON por usuario, con el plan por publicacion) a
    out_path. NO escribe nada: ni a ML (ESCRITURAS_BLOQUEADAS), ni a la DB, ni refresca tokens."""
    from salud_mayorista_motor import autocorregir_usuario
    conn = get_connection()
    creds = conn.execute("SELECT DISTINCT user_id, raw_data FROM ml_credentials").fetchall()
    conn.close()
    with open(out_path, "w", encoding="utf-8") as fh:
        for user_id, raw_data in creds:
            if user_ids and user_id not in user_ids:
                continue
            seller_id = str(json.loads(raw_data or "{}").get("user_id") or "")
            token, motivo_token = _access_token_sin_refresh(user_id)
            if not token:
                log.error("dry-run user_id=%s ABORTADO: token %s", user_id, motivo_token)
                fh.write(json.dumps({"user_id": user_id, "abortado": motivo_token}, ensure_ascii=False) + "\n")
                continue
            conn = get_connection()
            fecha = conn.execute("SELECT MAX(snapshot_date) FROM salud_item_snapshots WHERE user_id=?", (user_id,)).fetchone()[0]
            conn.close()
            log.info("dry-run user_id=%s: token %s, snapshot %s", user_id, motivo_token, fecha)
            res = autocorregir_usuario(token, user_id, seller_id, fecha, dry_run=True, log=log,
                                       opt_in=(opt_in or {}).get(user_id))
            res.update({"user_id": user_id, "snapshot": fecha})
            fh.write(json.dumps(res, ensure_ascii=False) + "\n")
            fh.flush()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sku", help="Corre solo esta familia (on-demand), no la corrida completa")
    parser.add_argument("--user-id", type=int, default=1)
    parser.add_argument("--only-user", type=int, help="Corrida completa (auditoria + auto-correccion) solo para este user_id")
    parser.add_argument("--dry-run-mayorista", metavar="OUT.jsonl",
                        help="Dry-run del motor de mayorista (sin escribir a ML ni a la DB); JSONL a OUT")
    parser.add_argument("--users", help="Con --dry-run-mayorista: lista de user_id separada por comas (default: todos)")
    args = parser.parse_args()

    if args.dry_run_mayorista:
        dry_run_mayorista([int(x) for x in args.users.split(",")] if args.users else None, args.dry_run_mayorista)
    elif args.sku:
        init_salud_tables()
        conn = get_connection()
        row = conn.execute("SELECT raw_data FROM ml_credentials WHERE user_id=?", (args.user_id,)).fetchone()
        conn.close()
        import json as _json
        seller_id = str(_json.loads((row["raw_data"] if row else "") or "{}").get("user_id") or "")
        print(audit_sku(args.user_id, seller_id, args.sku))
    else:
        run(only_user=args.only_user)
