"""
stock_snapshot.py
Toma un snapshot diario del available_quantity de todas las publicaciones activas.
Uso: python3 /opt/pythonml/stock_snapshot.py
Cron: 0 3 * * * /opt/pythonml/venv/bin/python3 /opt/pythonml/stock_snapshot.py >> /var/log/pythonml_stock.log 2>&1

Nota: para cuentas con warehouse_management/multiwarehouse activo (ver MULTIWAREHOUSE_SELLER_IDS),
available_quantity de /items puede no reflejar el stock real por depósito (ver /user-products/{id}/stock).
Se guarda igual, marcado con is_multiwarehouse=1, para no bloquear el snapshot del resto de las cuentas.
"""
import json
import logging
import sys
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
ATTRIBUTES = "id,available_quantity,status,attributes"
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


def get_all_item_ids(token: str, seller_id: str) -> list[str]:
    ids = []
    offset, limit = 0, 100
    while True:
        r = requests.get(
            f'https://api.mercadolibre.com/users/{seller_id}/items/search',
            params={'status': 'active', 'offset': offset, 'limit': limit},
            headers={'Authorization': f'Bearer {token}'},
            timeout=30
        )
        r.raise_for_status()
        data = r.json()
        results = data.get('results', [])
        ids.extend(results)
        if offset + limit >= data.get('paging', {}).get('total', 0):
            break
        offset += limit
    return ids


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
    for cred_id, user_id, raw_data in creds:
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
            item_ids = get_all_item_ids(token, seller_id)
            log.info("  %d items activos encontrados", len(item_ids))

            items = get_items_batch(token, item_ids)
            log.info("  %d items con datos obtenidos", len(items))

            conn = get_connection()
            saved = 0
            for item in items:
                try:
                    conn.execute("""
                        INSERT OR IGNORE INTO ml_stock_snapshots
                            (user_id, seller_id, item_id, seller_sku, available_qty, status, is_multiwarehouse, snapshot_date)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        user_id,
                        seller_id,
                        item['id'],
                        get_seller_sku(item),
                        item.get('available_quantity'),
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

    log.info("=== Snapshot completado: %d registros totales ===", total_saved)


if __name__ == '__main__':
    run_snapshot()
