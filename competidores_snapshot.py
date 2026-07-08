"""
competidores_snapshot.py
Lógica eficiente en 4 pasos:
1. Recorrer catálogos → recolectar seller_ids nuevos (sin llamar a /users todavía)
2. Deduplicar seller_ids únicos de toda la DB
3. Llamar /users/{seller_id} UNA sola vez por vendedor único
4. Guardar snapshot + borrar vendedores con 0 ventas históricas
Cron: 0 4 * * * /opt/pythonml/venv/bin/python3 /opt/pythonml/competidores_snapshot.py
"""
import sys, json, logging, time
from datetime import date
sys.path.insert(0, '/opt/pythonml')

import requests
from db import get_connection
from ml_api import get_ml_access_token
from tabs.catalogos import _sync_one_catalog
import asyncio

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

ML_API = "https://api.mercadolibre.com"


def get_all_credentials():
    conn = get_connection()
    rows = conn.execute(
        "SELECT id, user_id, raw_data FROM ml_credentials WHERE raw_data IS NOT NULL"
    ).fetchall()
    conn.close()
    result = []
    for r in rows:
        try:
            d = json.loads(r[2]) if r[2] else {}
            seller_id = str(d.get("user_id") or d.get("id") or "")
            if seller_id:
                result.append({"cred_id": r[0], "user_id": r[1], "seller_id": seller_id})
        except Exception:
            pass
    return result


def get_catalog_ids(user_id: int) -> list:
    conn = get_connection()
    rows = conn.execute(
        "SELECT DISTINCT catalog_product_id FROM sku_catalogos WHERE user_id=? AND catalog_product_id IS NOT NULL AND catalog_product_id != ''",
        (user_id,)
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]


def ensure_table():
    conn = get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS competidores_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            catalog_product_id TEXT NOT NULL,
            seller_id TEXT NOT NULL,
            seller_nickname TEXT,
            seller_total_ventas INTEGER,
            seller_level_id TEXT,
            seller_power_status TEXT,
            price REAL,
            item_id TEXT,
            snapshot_date DATE NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_comp_snapshot
        ON competidores_snapshots(user_id, catalog_product_id, seller_id, snapshot_date)
    """)
    # competidores_seguidos se crea de forma perezosa en tabs/competidores.py al agregar
    # el primer seguido; puede no existir todavía en una DB nueva.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS competidores_seguidos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            seller_id TEXT NOT NULL,
            seller_nickname TEXT,
            agregado_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, seller_id)
        )
    """)
    conn.commit()
    conn.close()


async def run():
    ensure_table()
    today = date.today().isoformat()
    log.info("=== Snapshot competidores %s ===", today)

    creds = get_all_credentials()

    for cred in creds:
        user_id = cred["user_id"]
        token = get_ml_access_token(user_id)
        if not token:
            log.warning("Sin token para user_id=%s", user_id)
            continue

        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        catalog_ids = get_catalog_ids(user_id)
        log.info("user_id=%s — %d catálogos", user_id, len(catalog_ids))

        # PASO 1: Recolectar seller_ids de todos los catálogos
        # Estructura: {catalog_product_id: [seller_ids]}
        catalog_sellers = {}  # cpid -> list of {seller_id, price, item_id}

        for cpid in catalog_ids:
            try:
                r = requests.get(
                    f"{ML_API}/products/{cpid}/items",
                    headers=headers, timeout=10
                )
                if r.status_code == 200:
                    items = r.json().get("results", [])
                    catalog_sellers[cpid] = [
                        {
                            "seller_id": str(it.get("seller_id", "")),
                            "price": it.get("price"),
                            "item_id": it.get("item_id") or it.get("id"),
                        }
                        for it in items if it.get("seller_id")
                    ]
            except Exception as e:
                log.error("Error catálogo %s: %s", cpid, e)

        log.info("Catálogos procesados: %d", len(catalog_sellers))

        # PASO 2: Seller IDs únicos (de catálogos + competidores_seguidos)
        all_seller_ids = set()
        for items in catalog_sellers.values():
            for it in items:
                if it["seller_id"]:
                    all_seller_ids.add(it["seller_id"])

        # Agregar los que están en competidores_seguidos
        seguidos = []
        try:
            conn = get_connection()
            seguidos = conn.execute(
                "SELECT seller_id FROM competidores_seguidos WHERE user_id=?", (user_id,)
            ).fetchall()
            conn.close()
        except Exception as e:
            log.error("Error leyendo competidores_seguidos: %s", e)
        for s in seguidos:
            all_seller_ids.add(str(s[0]))

        log.info("Sellers únicos a consultar: %d", len(all_seller_ids))

        # PASO 3: Consultar /users/{seller_id} UNA sola vez por vendedor
        seller_data = {}  # seller_id -> {nickname, total_ventas, level_id, power_status}

        for sid in all_seller_ids:
            try:
                r = requests.get(f"{ML_API}/users/{sid}", headers=headers, timeout=8)
                if r.status_code == 200:
                    d = r.json()
                    rep = d.get("seller_reputation") or {}
                    txn = rep.get("transactions") or {}
                    total = txn.get("total")
                    if total:  # Solo guardar si tiene ventas
                        seller_data[sid] = {
                            "nickname": d.get("nickname") or "",
                            "total_ventas": total,
                            "level_id": rep.get("level_id") or "",
                            "power_status": rep.get("power_seller_status") or "",
                        }
            except Exception as e:
                log.error("Error usuario %s: %s", sid, e)

        log.info("Sellers con ventas históricas: %d", len(seller_data))

        # PASO 4: Guardar snapshots
        conn = get_connection()
        saved = 0
        for cpid, items in catalog_sellers.items():
            for it in items:
                sid = it["seller_id"]
                if sid not in seller_data:
                    continue
                sd = seller_data[sid]
                try:
                    conn.execute("""
                        INSERT OR IGNORE INTO competidores_snapshots
                            (user_id, catalog_product_id, seller_id, seller_nickname,
                             seller_total_ventas, seller_level_id, seller_power_status,
                             price, item_id, snapshot_date)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        user_id, cpid, sid,
                        sd["nickname"], sd["total_ventas"],
                        sd["level_id"], sd["power_status"],
                        it["price"], it["item_id"], today
                    ))
                    saved += 1
                except Exception as e:
                    log.error("Error insert %s/%s: %s", cpid, sid, e)

        # También guardar seguidos que no están en ningún catálogo
        for s in seguidos:
            sid = str(s[0])
            if sid not in seller_data:
                continue
            sd = seller_data[sid]
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO competidores_snapshots
                        (user_id, catalog_product_id, seller_id, seller_nickname,
                         seller_total_ventas, seller_level_id, seller_power_status,
                         snapshot_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    user_id, "SEGUIDO", sid,
                    sd["nickname"], sd["total_ventas"],
                    sd["level_id"], sd["power_status"], today
                ))
            except Exception:
                pass

        conn.commit()
        conn.close()
        log.info("Snapshots guardados: %d", saved)

        # PASO 5: Borrar vendedores con 0 ventas históricas
        conn = get_connection()
        n = conn.execute("""
            DELETE FROM competidores_snapshots
            WHERE user_id=? AND seller_id IN (
                SELECT seller_id FROM competidores_snapshots
                WHERE user_id=?
                GROUP BY seller_id
                HAVING MAX(seller_total_ventas) IS NULL OR MAX(seller_total_ventas) = 0
            )
        """, (user_id, user_id)).rowcount
        conn.commit()
        conn.close()
        if n:
            log.info("Eliminados %d registros con 0 ventas", n)

    log.info("=== COMPLETADO ===")


if __name__ == "__main__":
    asyncio.run(run())
