"""
competidores_snapshot.py
Snapshot diario de competidores en catálogos propios.
Cron: 0 4 * * * /opt/pythonml/venv/bin/python3 /opt/pythonml/competidores_snapshot.py >> /var/log/pythonml_comp.log 2>&1
"""
import asyncio
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

from db import get_connection, get_sku_catalogos, init_competidores_snapshots_db
from ml_api import get_ml_access_token
from tabs.catalogos import _sync_one_catalog

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)


def get_seller_id(raw_data: str) -> str | None:
    if not raw_data:
        return None
    try:
        return str(json.loads(raw_data).get("user_id") or "") or None
    except Exception:
        return None


def get_all_catalog_ids(user_id: int) -> list:
    ids = {c["catalog_product_id"] for c in get_sku_catalogos(user_id) if c.get("catalog_product_id")}
    return list(ids)


def save_snapshot(user_id: int, catalog_product_id: str, items: list) -> int:
    today = date.today().isoformat()
    conn = get_connection()
    saved = 0
    for it in items:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO competidores_snapshots
                    (user_id, catalog_product_id, seller_id, seller_nickname,
                     seller_total_ventas, seller_level_id, seller_power_status,
                     price, item_id, snapshot_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                user_id,
                catalog_product_id,
                str(it.get("seller_id", "")),
                it.get("seller_nickname"),
                it.get("seller_total_ventas"),
                it.get("seller_level_id"),
                it.get("seller_power_status"),
                it.get("price"),
                it.get("item_id") or it.get("id"),
                today,
            ))
            saved += 1
        except Exception as e:
            log.error("Error guardando snapshot item: %s", e)
    conn.commit()
    conn.close()
    return saved


async def run_snapshot_async():
    conn = get_connection()
    creds = conn.execute(
        "SELECT id, user_id, raw_data FROM ml_credentials"
    ).fetchall()
    conn.close()

    total = 0
    today = date.today().isoformat()
    log.info("=== Snapshot competidores %s ===", today)

    for cred_id, user_id, raw_data in creds:
        token = get_ml_access_token(user_id)
        if not token:
            log.warning("Sin token para user_id=%s, salteando", user_id)
            continue
        seller_id = get_seller_id(raw_data)

        catalog_ids = get_all_catalog_ids(user_id)
        log.info("user_id=%s seller=%s — %d catálogos", user_id, seller_id, len(catalog_ids))

        for cpid in catalog_ids:
            try:
                items = await _sync_one_catalog(token, cpid)
                if items:
                    n = save_snapshot(user_id, cpid, items)
                    log.info("  %s → %d competidores guardados", cpid, n)
                    total += n
                time.sleep(0.3)
            except Exception as e:
                log.error("  Error en catálogo %s: %s", cpid, e)

    log.info("=== Total: %d snapshots guardados ===", total)


if __name__ == "__main__":
    init_competidores_snapshots_db()
    asyncio.run(run_snapshot_async())
