"""
stock_snapshot.py
Toma un snapshot diario del available_quantity de todas las publicaciones activas.
Uso: python3 /opt/pythonml/stock_snapshot.py
Cron: 0 11 * * * /opt/pythonml/venv/bin/python3 /opt/pythonml/stock_snapshot.py >> /var/log/pythonml_stock.log 2>&1
(movido de 3am a 11am: a las 3am el token de ML llevaba 9+ horas sin usarse y el refresh
fallaba intermitentemente -- ver [ML_API] _ml_refresh_token en ml_api.py -- a las 11am el
usuario ya viene usando el sistema desde la manana y el token esta fresco)

Nota: para cuentas con warehouse_management/multiwarehouse activo (ver MULTIWAREHOUSE_SELLER_IDS),
available_quantity de /items puede no reflejar el stock real por depósito (ver /user-products/{id}/stock).
Se guarda igual, marcado con is_multiwarehouse=1, para no bloquear el snapshot del resto de las cuentas.
"""
import json
import logging
import sys
import time
from datetime import date
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from dotenv import load_dotenv
load_dotenv(BASE_DIR / ".env")

import requests
from db import get_connection
from ml_api import get_ml_access_token

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

BATCH_SIZE = 20
ATTRIBUTES = "id,available_quantity,status,attributes,price"
MULTIWAREHOUSE_SELLER_IDS = {"1848533798"}  # NORTHTECHNOLOGY (warehouse_management/multiwarehouse)


def get_seller_sku(item: dict) -> str | None:
    for attr in item.get('attributes') or []:
        if attr.get('id') == 'SELLER_SKU':
            return attr.get('value_name')
    return None


def get_seller_id(raw_data: str) -> str | None:
    if not raw_data:
        return None
    try:
        return str(json.loads(raw_data).get("user_id") or "") or None
    except Exception:
        return None


def get_all_item_ids(token: str, seller_id: str) -> tuple[list[str], bool]:
    """Devuelve (ids, truncated). Pagina con search_type=scan + scroll_id (sin techo de 1000,
    a diferencia de offset/limit). truncated=True solo si el scroll se corta a mitad de camino
    (scroll_id vencido/invalido o error de red) -- en ese caso se devuelven los ids ya
    recolectados en vez de perderlos. limit=100 es el maximo real que acepta ML en modo scan
    (confirmado en vivo: pedir 200 o mas se ignora y devuelve 100 igual)."""
    ids = []
    scroll_id = None
    truncated = False
    base_params = {'status': 'active', 'search_type': 'scan', 'limit': 100}
    while True:
        params = dict(base_params)
        if scroll_id:
            params['scroll_id'] = scroll_id
        try:
            r = requests.get(
                f'https://api.mercadolibre.com/users/{seller_id}/items/search',
                params=params,
                headers={'Authorization': f'Bearer {token}'},
                timeout=30
            )
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if not ids:
                # Fallo en la primera pagina: no hay nada que recuperar, es un error real.
                raise
            truncated = True
            resp = e.response
            if resp is not None and resp.status_code == 400 and 'scroll' in resp.text.lower():
                log.warning(
                    "  Scroll cortado tras %d items: scroll_id vencido/invalido (%s) -- "
                    "si esto se repite seguido, hay que revisar el volumen de items o paralelizar",
                    len(ids), resp.text[:200]
                )
            else:
                log.warning(
                    "  Scroll cortado tras %d items: error HTTP %s -- %s",
                    len(ids), resp.status_code if resp is not None else '?',
                    resp.text[:200] if resp is not None else str(e)
                )
            break
        except requests.exceptions.RequestException as e:
            if not ids:
                raise
            truncated = True
            log.warning("  Scroll cortado tras %d items: fallo de red (%s)", len(ids), e)
            break
        data = r.json()
        results = data.get('results', [])
        if not results:
            break
        ids.extend(results)
        scroll_id = data.get('scroll_id')
        if not scroll_id:
            break
    return ids, truncated


def get_items_batch(token: str, item_ids: list[str]) -> list[dict]:
    results = []
    for i in range(0, len(item_ids), BATCH_SIZE):
        batch = item_ids[i:i + BATCH_SIZE]
        r = requests.get(
            'https://api.mercadolibre.com/items',
            params={'ids': ','.join(batch), 'attributes': ATTRIBUTES},
            headers={'Authorization': f'Bearer {token}'},
            timeout=30
        )
        r.raise_for_status()
        for entry in r.json():
            if entry.get('code') == 200:
                results.append(entry['body'])
    return results


def run_snapshot():
    today = date.today().isoformat()
    log.info("=== Stock snapshot %s ===", today)

    conn = get_connection()
    creds = conn.execute(
        "SELECT id, user_id, raw_data FROM ml_credentials"
    ).fetchall()
    conn.close()

    total_saved = 0
    truncated_sellers = []
    for i, (cred_id, user_id, raw_data) in enumerate(creds):
        if i > 0:
            # Espaciar el refresh de token entre usuarios: procesarlos en menos de 1s
            # puede colisionar con el rate limit de ML para /oauth/token.
            time.sleep(5)
        seller_id = get_seller_id(raw_data)
        token = get_ml_access_token(user_id)
        if not token:
            log.warning("Sin token para user_id=%s, salteando", user_id)
            continue
        if not seller_id:
            me = requests.get(
                'https://api.mercadolibre.com/users/me',
                headers={'Authorization': f'Bearer {token}'},
                timeout=10
            )
            seller_id = str(me.json().get('id')) if me.ok else None
        if not seller_id:
            log.warning("No se pudo resolver seller_id para user_id=%s, salteando", user_id)
            continue

        is_multiwarehouse = seller_id in MULTIWAREHOUSE_SELLER_IDS
        log.info("Procesando seller_id=%s (user_id=%s, multiwarehouse=%s)", seller_id, user_id, is_multiwarehouse)
        try:
            item_ids, truncated = get_all_item_ids(token, seller_id)
            if truncated:
                truncated_sellers.append(seller_id)
            log.info(
                "  %d items activos encontrados%s",
                len(item_ids), " (TRUNCADO por limite de paginacion)" if truncated else ""
            )

            items = get_items_batch(token, item_ids)
            log.info("  %d items con datos obtenidos", len(items))

            conn = get_connection()
            saved = 0
            for item in items:
                try:
                    conn.execute("""
                        INSERT OR IGNORE INTO ml_stock_snapshots
                            (user_id, seller_id, item_id, seller_sku, available_qty, price, status, is_multiwarehouse, snapshot_date)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        user_id,
                        seller_id,
                        item['id'],
                        get_seller_sku(item),
                        item.get('available_quantity'),
                        item.get('price'),
                        item.get('status'),
                        1 if is_multiwarehouse else 0,
                        today,
                    ))
                    saved += 1
                except Exception as e:
                    log.error("  Error guardando item %s: %s", item.get('id'), e)
            conn.commit()
            conn.close()
            log.info("  %d snapshots guardados para seller %s", saved, seller_id)
            total_saved += saved

        except Exception as e:
            log.error("Error procesando seller_id=%s: %s", seller_id, e)

    if truncated_sellers:
        log.info(
            "=== Snapshot completado: %d registros totales -- TRUNCADOS por limite de paginacion (seller_id): %s ===",
            total_saved, ", ".join(truncated_sellers)
        )
    else:
        log.info("=== Snapshot completado: %d registros totales ===", total_saved)


if __name__ == '__main__':
    run_snapshot()
