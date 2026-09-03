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
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

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


def audit_item(token: str, item: dict, cat_attrs_cache: Dict[str, list],
                session: Optional[requests.Session] = None) -> Dict[str, Any]:
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

    try:
        r = S.get(f"{ML_API}/items/{iid}/prices", headers={**H, "show-all-prices": "TRUE"}, timeout=15)
        if r.status_code == 200:
            w = _wholesale_from_prices(r.json())
            data["mayorista_estado"] = w["estado"]
            import json as _json
            data["mayorista_tiers_json"] = _json.dumps(
                {"standard_amount": w["standard_amount"], "tiers": w["tiers"]}, ensure_ascii=False
            )
        else:
            errores.append(f"prices status={r.status_code} {_err_detalle(r)}")
    except requests.exceptions.RequestException as e:
        errores.append(f"prices error={e}")

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
        "mayorista_tiers_json", "flex_status", "retiro_persona", "garantia_tipo",
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
        data = audit_item(token, it, cat_attrs_cache, session)
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
        data = audit_item(token, it, cat_attrs_cache, session)
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
