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
import logging
import math
import re
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from dotenv import load_dotenv
load_dotenv(BASE_DIR / ".env")

import requests
from db import get_connection, init_cron_runs_db, init_salud_tables, log_cron_run
from ml_api import get_ml_access_token

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


def fetch_all_own_items(token: str, seller_id: str) -> List[dict]:
    """Scan completo (mismos 5 grupos de status que resync_sku_catalogos.py) + multiget."""
    all_ids: List[str] = []
    for extra in PARAM_SETS:
        scroll_id = None
        while True:
            params = {"search_type": "scan", "limit": 100, **extra}
            if scroll_id:
                params["scroll_id"] = scroll_id
            r = requests.get(
                f"{ML_API}/users/{seller_id}/items/search",
                headers={"Authorization": f"Bearer {token}"}, params=params, timeout=15,
            )
            r.raise_for_status()
            data = r.json()
            chunk = data.get("results", [])
            if not chunk:
                break
            all_ids.extend(chunk)
            scroll_id = data.get("scroll_id")
            if not scroll_id:
                break
            time.sleep(0.05)
    all_ids = list(dict.fromkeys(all_ids))

    items: List[dict] = []
    for i in range(0, len(all_ids), 20):
        batch = all_ids[i:i + 20]
        r = requests.get(f"{ML_API}/items", params={"ids": ",".join(batch)},
                          headers={"Authorization": f"Bearer {token}"}, timeout=30)
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
# Cálculo de la propuesta 2/3/5/10 por fórmula de envío propia (no la
# recomendación de ML) -- usada tanto para crear tiers donde no hay ninguno como
# para recalcular el valor "correcto" hoy de un tier ya cargado (ver evaluación
# unificada más abajo, _evaluar_mayorista_gold_special). Movida acá desde
# tabs/salud.py (2026-09-04) para que audit_item() también pueda usarla en el
# cron nocturno -- el popup (tabs/salud.py) la importa de acá.
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

# ---------------------------------------------------------------------------
# Esquema fijo para publicaciones SIN envío gratis obligatorio -- la fórmula de
# ahorro de envío (_costo_envio_free) no tiene base económica ahí: el costo de
# envío que cotiza ML es ~fijo independientemente del precio, así que en un
# producto barato esa fracción explota. Confirmado en vivo 2026-09-04:
# Google-G1001-USB ($8.999) daba 110.06% en la cantidad de 5+ -> monto de
# -$905 (precio NEGATIVO). El corte se detecta por el tag "mandatory_free_shipping"
# del propio ítem (GET /items/{id}, ya viene en el body -- sin llamada extra),
# no por un precio hardcodeado: se verificó en vivo contra la cuenta que ese tag
# aparece exactamente a partir de $33.000 (coincide con el parámetro
# ml_envios_gratuitos ya usado en cuotas.py/precios.py/promos.py/ventas.py), y
# así queda correcto si ML cambia el umbral en el futuro.
# Esquema confirmado con Diego: 2->1%, 3->2%, 5->3%, 10->4%. Cantidades "extra"
# fuera de esos 4 puntos (tiers ya cargados en otra cantidad, ver
# _evaluar_mayorista_gold_special) se interpolan linealmente entre los dos
# puntos fijos más cercanos; por debajo de 2 o por encima de 10 se extrapola
# con la pendiente del tramo más cercano (2->3, o 5->10).
# ---------------------------------------------------------------------------

_PCTS_FIJOS_SIN_ENVIO_GRATIS = {2: 1.0, 3: 2.0, 5: 3.0, 10: 4.0}

# Piso de sanidad genérico, independiente de la causa puntual de arriba: ningún
# camino (fórmula de envío, esquema fijo, o lo que se agregue después) puede
# proponer un % que dé un precio negativo o casi regalado. Se aplica acá (nunca
# se propone) y de nuevo en tabs/salud.py::_construir_payload_mayorista (nunca
# se escribe a ML), como doble chequeo.
_PCT_TECHO_SANIDAD = 90.0


def _pct_fijo_interpolado(n: int) -> float:
    """% fijo (puntos porcentuales) para la cantidad `n`, ver
    _PCTS_FIJOS_SIN_ENVIO_GRATIS. Interpola/extrapola linealmente para
    cantidades fuera de los 4 puntos definidos."""
    puntos = sorted(_PCTS_FIJOS_SIN_ENVIO_GRATIS.items())
    if n in _PCTS_FIJOS_SIN_ENVIO_GRATIS:
        return _PCTS_FIJOS_SIN_ENVIO_GRATIS[n]
    if n < puntos[0][0]:
        (q1, p1), (q2, p2) = puntos[0], puntos[1]
    elif n > puntos[-1][0]:
        (q1, p1), (q2, p2) = puntos[-2], puntos[-1]
    else:
        q1, p1 = max(((q, p) for q, p in puntos if q < n), key=lambda x: x[0])
        q2, p2 = min(((q, p) for q, p in puntos if q > n), key=lambda x: x[0])
    pendiente = (p2 - p1) / (q2 - q1)
    return p1 + pendiente * (n - q1)


def _propuesta_fija(precio_base: float, cantidades: Tuple[int, ...]) -> Optional[Dict[str, Any]]:
    """Propuesta de mayorista para publicaciones sin envío gratis obligatorio --
    mismo shape de retorno que _calcular_mayorista_nuevo (solo "propuesta" con
    quantity/amount/percentage se usa río abajo, ver _evaluar_mayorista_gold_special)."""
    if not precio_base:
        return None
    propuesta: List[Dict[str, Any]] = []
    for n in cantidades:
        pct = _pct_fijo_interpolado(n)
        if pct <= 0 or pct >= _PCT_TECHO_SANIDAD:
            continue
        monto = round(precio_base * (1 - pct / 100), 2)
        propuesta.append({"quantity": n, "amount": monto, "percentage": round(pct, 2)})
    if not propuesta:
        return None
    return {"precio_base": precio_base, "dimensiones": None, "peso_base_g": None, "propuesta": propuesta}


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


def _calcular_mayorista_nuevo(token: str, seller_id: str, item: dict, precio_base: float,
                               cantidades: Tuple[int, ...] = _CANTIDADES_MAYORISTA_NUEVO) -> Optional[Dict[str, Any]]:
    """Arma la propuesta de mayorista para un ítem, para las `cantidades` pedidas
    (default 2/3/5/10 -- el popup pasa además las cantidades "extra" que el ítem ya
    tenga cargadas fuera de ese set estándar, en una llamada aparte, para no acoplar
    su cotización a la de las 4 estándar: ver _evaluar_mayorista_gold_special).
    Fórmula validada: % = ceil(ahorro_envío / precio_base × 10000) / 10000, donde
    ahorro_envío = costo_envío(1 unidad) − costo_envío(N unidades)/N (el costo a 1
    unidad se usa como base de comparación, nunca se ofrece como tier -- ver nota
    arriba). Las dimensiones (SELLER_PACKAGE_* del propio ítem) quedan fijas -- SOLO
    el peso escala ×N, no se simula apilado. Es una aproximación (no exacta: ML arma
    el paquete combinado con su propia tara, el escalado lineal del peso es la mejor
    aproximación disponible sin una fórmula más exacta documentada). Devuelve None si
    no hay SELLER_PACKAGE_* cargado, no hay precio base, ML no puede cotizar el envío
    para alguna de las cantidades pedidas, o ninguna da un % > 0 (ML rechaza tiers
    con 0%).

    Si el ítem NO tiene envío gratis obligatorio (tag "mandatory_free_shipping"
    ausente en item["shipping"]["tags"]), esta fórmula no aplica -- ver
    _PCTS_FIJOS_SIN_ENVIO_GRATIS más arriba -- y se usa el esquema fijo en su lugar."""
    tags = ((item.get("shipping") or {}).get("tags") or [])
    if "mandatory_free_shipping" not in tags:
        return _propuesta_fija(precio_base, cantidades)
    dims = _dimensiones_seller_package(item)
    if not dims or not precio_base:
        return None
    l, w, h, peso = dims
    costo_1 = _costo_envio_free(token, seller_id, l, w, h, peso, precio_base)
    if costo_1 is None:
        return None
    propuesta: List[Dict[str, Any]] = []
    for n in cantidades:
        costo_n = _costo_envio_free(token, seller_id, l, w, h, peso * n, precio_base * n)
        if costo_n is None:
            return None
        ahorro_unit = costo_1 - (costo_n / n)
        pct = math.ceil((ahorro_unit / precio_base) * 10000) / 10000 if ahorro_unit > 0 else 0.0
        if pct <= 0:
            continue  # ML rechaza tiers de mayorista con 0% -- no se ofrece, no se inventa
        if pct * 100 >= _PCT_TECHO_SANIDAD:
            continue  # piso de sanidad -- nunca proponer un % que deje un precio negativo o casi regalado
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
#     se ofrece aplicar automático desde el popup -- el cron SÍ lo persiste (ver
#     audit_item) para poder mostrar el ⚠️ en la tabla resumen sin recalcular en vivo.
# El umbral (4pp absolutos Y 1.75x relativo) se validó contra el barrido completo
# de la cuenta: dispara en casos reales como E.Show8-2da-Negro (tier de mayo,
# 6.59% cargado vs 2.10% calculado hoy) sin falsos positivos sobre los 97 tiers
# recién escritos con este mismo cálculo.
#
# Cantidades "extra" (cualquier tier ya cargado fuera de 2/3/5/10, ej. 7, 15, 20) --
# desde 2026-09-04 SÍ se evalúan, con el mismo criterio ok/roto/revisar (marcadas
# "extra": True en el tier), para que el popup pueda mostrar y corregir un tier que
# de otro modo quedaba invisible y volvía incoherente cualquier corrección de las 4
# estándar (ver _tiers_plan en tabs/salud.py, caso real MLA1944479697/MLA1944467261,
# tier de 15+ al 6.23%).
#
# Cantidad=1 es la ÚNICA excepción que sigue SIEMPRE afuera: la fórmula de ahorro de
# envío da 0% por definición para n=1 (ahorro contra sí mismo), así que nunca puede
# tener pct_calculado -- incluirla en el piso ascendente de _tiers_plan arriesgaría
# distorsionar el margen exigido a las demás cantidades con un valor sin referencia
# de cálculo. Un tier de 1 unidad cargado se preserva tal cual esté, nunca se evalúa
# ni se toca (ver _construir_payload_mayorista en tabs/salud.py).
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


def _tiers_cargados_por_cantidad(prices_body: dict, precio_base: float) -> Dict[int, float]:
    """Subconjunto de _tiers_cargados_todos acotado a las 4 cantidades objetivo
    estándar (2/3/5/10) -- usado por _wholesale_from_prices-adyacentes que solo
    quieren el set estándar."""
    return {q: m for q, m in _tiers_cargados_todos(prices_body, precio_base).items() if q in _QTYS_MAYORISTA}


def _evaluar_mayorista_gold_special(token: str, seller_id: str, item: dict,
                                     prices_body: Optional[dict] = None,
                                     siempre_devolver: bool = False) -> Optional[Dict[str, Any]]:
    """Evalúa las 4 cantidades objetivo para UNA publicación gold_special. Devuelve
    None si no se puede evaluar (sin precio base, sin dimensiones, sin cotización de
    envío) o -- si siempre_devolver=False, el default -- si las 4 están "ok" y no hay
    nada que mostrar (el popup no pasa este flag: quiere el atajo, así no satura la
    pantalla con ítems totalmente sanos).

    prices_body: si se pasa (el cron ya hizo su propio GET /prices para
    _wholesale_from_prices), se reusa en vez de pedirlo de nuevo -- ahorra una
    llamada por ítem. El popup (tabs/salud.py) NO lo pasa: siempre quiere el GET
    en vivo propio, porque el snapshot de la auditoría puede estar desactualizado
    frente a escrituras recientes.

    siempre_devolver: el cron (audit_item) lo pasa en True -- necesita distinguir
    "no se pudo evaluar" (None real: sin precio base, sin cotización de envío) de
    "se evaluó y las 4 están sanas" (con este flag, devuelve el dict igual en vez
    del atajo de arriba) para no confundir ambos casos en mayorista_revisar_json.

    Tiers "extra" (cualquier cantidad ya cargada fuera de 2/3/5/10, ej. 15+): se
    evalúan con el MISMO criterio ok/roto/revisar que los estándar (marcados con
    "extra": True), pero nunca se ofrecen para "crear" -- por definición ya están
    cargados. Se cotizan aparte (llamada propia a _calcular_mayorista_nuevo, no
    mezclada con la de 2/3/5/10) para que si ML no puede cotizar esa cantidad
    puntual, no tire abajo el cálculo de las 4 estándar. Confirmado en vivo
    2026-09-04 (MLA1944479697/MLA1944467261): un tier de 15+ al 6.23%, invisible
    para este motor hasta ahora, quedaba silenciosamente re-enviado sin tocar por
    _construir_payload_mayorista y volvía incoherente al POST cuando se subían los
    tiers 2/5/10 -- "Price per quantity invalid coherence order" -- porque ML exige
    % no decreciente con la cantidad y nadie evaluaba ese 15+ contra la corrección."""
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
    cargado = _tiers_cargados_todos(prices_body, precio_base)
    extra_qtys = tuple(sorted(q for q in cargado if q not in _QTYS_MAYORISTA and q != 1))

    prop = _calcular_mayorista_nuevo(token, seller_id, item, precio_base)
    calculado = {p["quantity"]: p["amount"] for p in prop["propuesta"]} if prop else {}
    calculado_pct = {p["quantity"]: p["percentage"] for p in prop["propuesta"]} if prop else {}

    if extra_qtys:
        prop_extra = _calcular_mayorista_nuevo(token, seller_id, item, precio_base, cantidades=extra_qtys)
        if prop_extra:
            calculado.update({p["quantity"]: p["amount"] for p in prop_extra["propuesta"]})
            calculado_pct.update({p["quantity"]: p["percentage"] for p in prop_extra["propuesta"]})

    qtys_a_evaluar = sorted(set(_QTYS_MAYORISTA) | set(extra_qtys))

    tiers: List[Dict[str, Any]] = []
    for q in qtys_a_evaluar:
        es_extra = q not in _QTYS_MAYORISTA
        if q not in cargado:
            if q in calculado:
                tiers.append({"quantity": q, "estado": "crear", "extra": es_extra,
                              "pct_calculado": calculado_pct[q], "monto_calculado": calculado[q]})
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
            tiers.append({"quantity": q, "estado": "ok", "extra": es_extra, "pct_cargado": pct_cargado, "monto_cargado": cargado[q]})
            continue
        if pct_cargado <= 0:
            tiers.append({"quantity": q, "estado": "roto", "extra": es_extra, "pct_cargado": pct_cargado, "monto_cargado": cargado[q],
                          "pct_calculado": pct_calc, "monto_calculado": calculado[q]})
            continue
        diff_pp = abs(pct_cargado - pct_calc)
        ratio = max(pct_cargado, pct_calc) / max(min(pct_cargado, pct_calc), 0.01)
        if diff_pp >= _DESVIO_PP_MIN and ratio >= _DESVIO_RATIO_MIN:
            tiers.append({"quantity": q, "estado": "revisar", "extra": es_extra, "pct_cargado": pct_cargado, "monto_cargado": cargado[q],
                          "pct_calculado": pct_calc, "monto_calculado": calculado[q]})
        else:
            tiers.append({"quantity": q, "estado": "ok", "extra": es_extra, "pct_cargado": pct_cargado, "monto_cargado": cargado[q]})

    presentes = sorted(cargado.keys())
    invertido = any(cargado[presentes[i]] < cargado[presentes[i + 1]] for i in range(len(presentes) - 1))

    if not siempre_devolver and not any(t["estado"] != "ok" for t in tiers) and not invertido:
        return None  # las 4 están ok (o no evaluables) y no hay inversión -- nada para mostrar (popup)

    return {"precio_base": precio_base, "tiers": tiers, "invertido": invertido}


def audit_item(token: str, item: dict, cat_attrs_cache: Dict[str, list],
                seller_id: str = "", session: Optional[requests.Session] = None) -> Dict[str, Any]:
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
    }

    for attr in item.get("attributes") or []:
        if attr.get("id") == "GTIN":
            data["gtin"] = (attr.get("value_name") or "").strip()
            break

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

    # Mayorista "revisar"/"invertido" (cotización de envío real por cantidad objetivo) --
    # solo gold_special con >=1 tier cargado (sin nada cargado no hay contra qué comparar).
    # A diferencia del popup (que siempre recalcula en vivo al abrir, tabs/salud.py), acá
    # se PERSISTE en el snapshot -- así la tabla resumen muestra el ⚠️ sin recalcular en
    # cada render (ver _mayorista_dim). "evaluable": false = se intentó pero no se pudo
    # cotizar envío (sin SELLER_PACKAGE_*, o ML no cotiza) -- no cuenta como sano ni como
    # revisar, queda "sin evaluar".
    if item.get("listing_type_id") == "gold_special" and seller_id and tiene_tiers_cargados:
        try:
            ev = _evaluar_mayorista_gold_special(
                token, seller_id, item, prices_body=prices_body_para_revisar, siempre_devolver=True,
            )
            import json as _json
            if ev is None:
                # con siempre_devolver=True, None es inequívoco: no se pudo leer/evaluar
                # (sin precio base o sin cotización de envío posible) -- no "las 4 ok".
                data["mayorista_revisar_json"] = _json.dumps({"evaluable": False}, ensure_ascii=False)
            else:
                tiers_revisar = [t for t in ev["tiers"] if t["estado"] == "revisar"]
                data["mayorista_revisar_json"] = _json.dumps(
                    {"evaluable": True, "invertido": ev["invertido"], "tiers_revisar": tiers_revisar},
                    ensure_ascii=False,
                )
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
        editables, bloqueados = [], []
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
            (bloqueados if tags.get("read_only") else editables).append(entry)
        data["atributos_faltantes_editables"] = len(editables)
        data["atributos_faltantes_bloqueados"] = len(bloqueados)
        import json as _json
        data["atributos_faltantes_json"] = _json.dumps(
            {"editables": editables, "bloqueados": bloqueados}, ensure_ascii=False
        )

    data["error"] = " | ".join(errores) if errores else None
    return data


def write_snapshot(conn, user_id: int, item_id: str, data: Dict[str, Any], snapshot_date: str) -> None:
    cols = [
        "sku", "catalog_listing", "status", "listing_type_id", "condicion", "gtin",
        "descripcion_len", "short_status", "fotos_cantidad", "mayorista_estado",
        "mayorista_tiers_json", "mayorista_revisar_json", "flex_status", "retiro_persona", "garantia_tipo",
        "garantia_tiempo", "envio_gratis", "regulatoria_estado",
        "atributos_faltantes_editables", "atributos_faltantes_bloqueados",
        "atributos_faltantes_json", "performance_score", "price", "error",
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
        data = audit_item(token, it, cat_attrs_cache, seller_id, session)
        if data.get("error"):
            n_errores += 1
        write_snapshot(conn, user_id, it["id"], data, hoy)
        if idx % 100 == 0:
            conn.commit()
            log.info("user_id=%s progreso: %d/%d", user_id, idx, len(items))
        time.sleep(0.08)
    conn.commit()
    conn.close()
    return {"items_procesados": len(items), "errores": n_errores}


def run() -> None:
    init_cron_runs_db()
    init_salud_tables()
    log.info("=== Salud audit %s ===", date.today().isoformat())
    conn = get_connection()
    creds = conn.execute("SELECT DISTINCT user_id, raw_data FROM ml_credentials").fetchall()
    conn.close()

    for user_id, raw_data in creds:
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
        status = "ok" if result["errores"] == 0 else "partial"
        log.info("user_id=%s: %s", user_id, result)
        log_cron_run("salud_audit", user_id, status, result["items_procesados"],
                     time.time() - t0, f"{result['errores']} items con error" if result["errores"] else None)
        time.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sku", help="Corre solo esta familia (on-demand), no la corrida completa")
    parser.add_argument("--user-id", type=int, default=1)
    args = parser.parse_args()

    if args.sku:
        init_salud_tables()
        conn = get_connection()
        row = conn.execute("SELECT raw_data FROM ml_credentials WHERE user_id=?", (args.user_id,)).fetchone()
        conn.close()
        import json as _json
        seller_id = str(_json.loads((row["raw_data"] if row else "") or "{}").get("user_id") or "")
        print(audit_sku(args.user_id, seller_id, args.sku))
    else:
        run()
