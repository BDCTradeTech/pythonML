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
from collections import Counter
from datetime import date
sys.path.insert(0, '/opt/pythonml')

import requests
from db import get_connection, init_cron_runs_db, log_cron_run
from ml_api import get_ml_access_token
from tabs.catalogos import _sync_one_catalog
import asyncio

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

ML_API = "https://api.mercadolibre.com"

SLEEP_BETWEEN_REQUESTS = 0.15  # throttling conservador (~6-7 req/s) para no chocar con el rate limit de ML
MAX_RETRIES_429 = 3


def _get_with_retry(url, headers, timeout=10, max_retries=MAX_RETRIES_429):
    """GET con reintento y backoff exponencial (2s, 4s, 8s) ante 429 (rate limit de ML)."""
    backoff = 2
    resp = None
    for attempt in range(max_retries + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
        except Exception:
            if attempt == max_retries:
                raise
            time.sleep(backoff)
            backoff *= 2
            continue
        if resp.status_code == 429 and attempt < max_retries:
            log.warning("429 rate limit en %s — reintento %d/%d en %ds", url, attempt + 1, max_retries, backoff)
            time.sleep(backoff)
            backoff *= 2
            continue
        return resp
    return resp


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
    init_cron_runs_db()
    today = date.today().isoformat()
    log.info("=== Snapshot competidores %s ===", today)

    creds = get_all_credentials()

    for cred in creds:
        user_id = cred["user_id"]
        own_seller_id = cred["seller_id"]
        t0 = time.time()
        status, count, error = "fail", 0, None
        try:
            token = get_ml_access_token(user_id)
            if not token:
                error = "Sin token ML"
                log.warning("Sin token para user_id=%s", user_id)
                continue

            headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
            catalog_ids = get_catalog_ids(user_id)
            log.info("user_id=%s — %d catálogos", user_id, len(catalog_ids))

            # PASO 1: Recolectar seller_ids de todos los catálogos
            # Estructura: {catalog_product_id: [seller_ids]}
            catalog_sellers = {}  # cpid -> list of {seller_id, price, item_id}
            status_counts = Counter()

            for cpid in catalog_ids:
                try:
                    r = _get_with_retry(f"{ML_API}/products/{cpid}/items", headers=headers, timeout=10)
                    if r.status_code == 200:
                        items = r.json().get("results", [])
                        catalog_sellers[cpid] = [
                            {
                                "seller_id": str(it.get("seller_id", "")),
                                "price": it.get("price"),
                                "item_id": it.get("item_id") or it.get("id"),
                            }
                            for it in items
                            if it.get("seller_id") and str(it.get("seller_id", "")) != own_seller_id
                        ]
                        status_counts[200] += 1
                    else:
                        status_counts[r.status_code] += 1
                        log.warning("Catálogo %s status=%s body=%s", cpid, r.status_code, (r.text or "")[:200])
                except Exception as e:
                    status_counts["exception"] += 1
                    log.error("Error catálogo %s: %s", cpid, e)
                time.sleep(SLEEP_BETWEEN_REQUESTS)

            fails = {k: v for k, v in status_counts.items() if k != 200}
            fail_detail = ", ".join(f"{k}={v}" for k, v in sorted(fails.items(), key=lambda kv: str(kv[0])))
            log.info(
                "user_id=%s — catálogos: OK=%d FAIL=%d (%s)",
                user_id, status_counts.get(200, 0), sum(fails.values()), fail_detail or "sin fallos"
            )
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

            # Agregar los del comparador (favoritos armados a mano), incluso si
            # coinciden con own_seller_id: el usuario los eligió explícitamente
            # y deben recibir snapshot diario aunque el PASO 1 los excluya del
            # ranking automático por catálogo.
            comparador = []
            try:
                conn = get_connection()
                comparador = conn.execute(
                    "SELECT seller_id FROM comparador_competidores WHERE user_id=?", (user_id,)
                ).fetchall()
                conn.close()
            except Exception as e:
                log.error("Error leyendo comparador_competidores: %s", e)
            for s in comparador:
                all_seller_ids.add(str(s[0]))

            log.info("Sellers únicos a consultar: %d", len(all_seller_ids))

            # PASO 3: Consultar /users/{seller_id} UNA sola vez por vendedor
            seller_data = {}  # seller_id -> {nickname, total_ventas, level_id, power_status}

            for sid in all_seller_ids:
                try:
                    r = _get_with_retry(f"{ML_API}/users/{sid}", headers=headers, timeout=8)
                    if r.status_code == 200:
                        d = r.json()
                        rep = d.get("seller_reputation") or {}
                        txn = rep.get("transactions") or {}
                        total = txn.get("total")
                        if total:  # Solo guardar si tiene ventas
                            # Guard: nunca guardar un valor menor al máximo histórico
                            # (ML corrige total hacia abajo por cancelaciones)
                            conn = get_connection()
                            max_hist = conn.execute(
                                "SELECT MAX(seller_total_ventas) FROM competidores_snapshots WHERE user_id=? AND seller_id=?",
                                (user_id, sid)
                            ).fetchone()[0] or 0
                            conn.close()
                            valor_final = max(total, max_hist)
                            seller_data[sid] = {
                                "nickname": d.get("nickname") or "",
                                "total_ventas": valor_final,
                                "level_id": rep.get("level_id") or "",
                                "power_status": rep.get("power_seller_status") or "",
                            }
                except Exception as e:
                    log.error("Error usuario %s: %s", sid, e)
                time.sleep(SLEEP_BETWEEN_REQUESTS)

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

            # También guardar los del comparador que no están en ningún catálogo
            # ni en seguidos (ej. own_seller_id agregado a mano como benchmark)
            for s in comparador:
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
                        user_id, "COMPARADOR", sid,
                        sd["nickname"], sd["total_ventas"],
                        sd["level_id"], sd["power_status"], today
                    ))
                except Exception:
                    pass

            conn.commit()
            conn.close()
            log.info("Snapshots guardados: %d", saved)
            count = saved

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
            status = "ok"
        except Exception as e:
            error = str(e)
            log.error("ERROR procesando user_id=%s: %s — continuando con el siguiente", user_id, e)
        finally:
            try:
                log_cron_run("competidores", user_id, status, count, time.time() - t0, error)
            except Exception as log_e:
                log.error("No se pudo loguear cron_runs: %s", log_e)

    log.info("=== COMPLETADO ===")


if __name__ == "__main__":
    asyncio.run(run())
