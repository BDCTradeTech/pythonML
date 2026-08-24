"""
watchdog_precios.py
Watchdog de SOLO LECTURA sobre precio/stock/promo de un puñado de publicaciones
propias, para detectar si un tercero (Ecomm-App u otro) pisa un precio puesto
por PythonML. Experimento acotado y temporal — no toca ninguna publicación.

Uso manual: python3 /opt/pythonml/watchdog_precios.py [--user-id 1]
Cron: */10 * * * * cd /opt/pythonml && set -a && . ./.env && set +a && ./venv/bin/python3 watchdog_precios.py >> /var/log/pythonml_watchdog.log 2>&1

Lista de items vigilados: app_config, clave precio_watchdog_items_{user_id},
JSON list de objetos {"item_id": "...", "seller_sku": "...", "label": "..."}.
El "seller_sku" de la config es SOLO un label de referencia para vos -- el
seller_sku que se graba en cada fila sale siempre del body real de ML (via
ml_api._parse_ml_item_body, la misma función que usa ml_get_my_items() y de
la que depende _cuotas_key en tabs/cuotas.py), para que un reescritor externo
de SKU se detecte como 'cambio' real y no quede enmascarado por la config.

Por corrida: UN multiget /items?ids=... para todos los items vigilados, más UNA
llamada /items/{id}/sale_price por item (sin ml_get_active_promo_prices_bulk:
pagina todas las campañas del seller, demasiado tráfico para cada 10 minutos).

Cada item se procesa de forma independiente: si el multiget no trajo datos para
ese item, o si su sale_price falló, se graba la fila igual con esos campos en
NULL y el motivo del fallo en la columna `error` -- nunca se descarta la corrida
completa por un solo fallo, y un fallo sistemático queda visible en cada corrida
(no se "silencia" después de la primera vez).

Se escribe una fila por item si:
  - hubo error en multiget y/o sale_price para ese item (motivo='error', SIEMPRE
    se escribe, incluso si nada más cambió y aunque sea la primera fila), o
  - no hay fila previa sin error para ese item (primera observación limpia,
    motivo='baseline'), o
  - cambió seller_sku/price/sale_amount/sale_regular_amount/available_quantity/
    status/item_last_updated respecto de la última fila (motivo='cambio'), o
  - no cambió nada pero pasaron >=6 horas desde la última fila de ese item
    (motivo='heartbeat', prueba de que el watchdog sigue vivo).
Si ninguna de esas condiciones se cumple, no se escribe fila ese ciclo (evita
ruido de filas idénticas cada 10 minutos).

NOTA: no hay binario `sqlite3` instalado en el droplet (verificado 2026-08-24, "command
not found") -- todo lo de abajo usa el módulo sqlite3 de Python en su lugar.

=== DESARME (experimento temporal) ===
Cuando termine la prueba:
  1. Cron:  crontab -l | grep -v watchdog_precios.py | crontab -
  2. Tabla + config + cron_runs del job, todo junto:
     python3 -c "
import sys; sys.path.insert(0,'/opt/pythonml')
from db import get_connection
c = get_connection()
c.execute('DROP TABLE IF EXISTS precio_stock_watch')
c.execute(\"DELETE FROM app_config WHERE key='precio_watchdog_items_1'\")
c.execute(\"DELETE FROM cron_runs WHERE job='watchdog_precios'\")
c.commit(); c.close()
print('desarmado')
"
  3. Script: rm /opt/pythonml/watchdog_precios.py  (y git rm watchdog_precios.py + commit en el repo local)
  4. Log: rm /var/log/pythonml_watchdog.log (opcional, o dejarlo como historial)

=== CONSULTA: historial de un item ===
python3 -c "
import sys; sys.path.insert(0,'/opt/pythonml')
from db import get_connection
c = get_connection()
for r in c.execute('''
    SELECT ts_utc, motivo, price, sale_amount, sale_regular_amount,
           available_quantity, status, seller_sku, error
    FROM precio_stock_watch
    WHERE user_id = 1 AND item_id = \"MLA2283297098\"
    ORDER BY ts_utc ASC
'''):
    print(dict(r))
"
"""
import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from dotenv import load_dotenv
load_dotenv(BASE_DIR / ".env")

from db import get_connection, get_app_config, init_cron_runs_db, log_cron_run
from ml_api import (
    get_ml_access_token,
    ml_get_items_multiget,
    ml_get_item_sale_price_full,
    _parse_ml_item_body,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

HEARTBEAT_HOURS = 6
WATCHED_FIELDS = (
    "seller_sku", "price", "sale_amount", "sale_regular_amount",
    "available_quantity", "status", "item_last_updated",
)


def init_precio_stock_watch_db() -> None:
    """Tabla append-only, propia de este script (no vive en db.py: no se toca ningún
    módulo existente para este experimento)."""
    conn = get_connection()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS precio_stock_watch (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id             INTEGER NOT NULL,
            item_id             TEXT NOT NULL,
            seller_sku          TEXT,
            price               REAL,
            sale_amount         REAL,
            sale_regular_amount REAL,
            available_quantity  INTEGER,
            status              TEXT,
            item_last_updated   TEXT,
            motivo              TEXT NOT NULL,
            error               TEXT,
            ts_utc              TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_precio_stock_watch_user_item_ts "
        "ON precio_stock_watch(user_id, item_id, ts_utc)"
    )
    conn.commit()
    conn.close()


def load_watch_list(user_id: int) -> list[dict]:
    raw = get_app_config(f"precio_watchdog_items_{user_id}")
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        log.error("precio_watchdog_items_%s: JSON inválido (%s)", user_id, e)
        return []
    if not isinstance(data, list):
        return []
    out = []
    for entry in data:
        if isinstance(entry, dict) and entry.get("item_id"):
            out.append({"item_id": str(entry["item_id"]), "label": entry.get("label")})
    return out


def get_latest_row(conn, user_id: int, item_id: str):
    cur = conn.execute(
        "SELECT * FROM precio_stock_watch WHERE user_id = ? AND item_id = ? "
        "ORDER BY ts_utc DESC LIMIT 1",
        (user_id, item_id),
    )
    return cur.fetchone()


def run(user_id: int = 1) -> int:
    t0 = time.time()
    init_cron_runs_db()
    init_precio_stock_watch_db()

    watch_list = load_watch_list(user_id)
    if not watch_list:
        msg = f"precio_watchdog_items_{user_id} vacío o ausente en app_config -- nada que vigilar"
        log.error(msg)
        log_cron_run("watchdog_precios", user_id, "fail", 0, time.time() - t0, msg)
        return 1

    token = get_ml_access_token(user_id)  # puede ser None: los calls de abajo degradan solos
    item_ids = [w["item_id"] for w in watch_list]

    bodies = ml_get_items_multiget(token, item_ids)
    # NO asumir que `bodies` viene en el mismo orden que `item_ids` -- verificado en vivo
    # (2026-08-24) que ML puede devolver el array de /items?ids= reordenado respecto del
    # pedido, pese a lo que dice el docstring de ml_get_items_multiget. Indexar siempre por
    # el campo "id" real de cada body, nunca por posición.
    body_by_item = {}
    for body in bodies:
        if body and body.get("id"):
            body_by_item[str(body["id"])] = body

    now_iso = datetime.now(timezone.utc).isoformat()
    conn = get_connection()
    written = 0
    any_error = False
    try:
        for w in watch_list:
            iid = w["item_id"]
            errors = []

            body = body_by_item.get(iid)
            if body is None:
                errors.append("multiget: sin datos para este item")
                seller_sku = price = available_quantity = status = item_last_updated = None
            else:
                parsed = _parse_ml_item_body(body)
                seller_sku = parsed.get("seller_sku") or None
                price = body.get("price")
                available_quantity = body.get("available_quantity")
                status = body.get("status")
                item_last_updated = body.get("last_updated")

            sale = ml_get_item_sale_price_full(token, iid)
            if sale is None:
                errors.append("sale_price: fallo la llamada")
                sale_amount = sale_regular_amount = None
            else:
                sale_amount = sale.get("amount")
                sale_regular_amount = sale.get("regular_amount")

            error_text = "; ".join(errors) or None
            new_vals = {
                "seller_sku": seller_sku, "price": price,
                "sale_amount": sale_amount, "sale_regular_amount": sale_regular_amount,
                "available_quantity": available_quantity, "status": status,
                "item_last_updated": item_last_updated,
            }

            latest = get_latest_row(conn, user_id, iid)
            if error_text:
                any_error = True
                motivo = "error"
            elif latest is None:
                motivo = "baseline"
            elif any(latest[f] != new_vals[f] for f in WATCHED_FIELDS):
                motivo = "cambio"
            else:
                try:
                    last_ts = datetime.fromisoformat(latest["ts_utc"])
                    age_hours = (datetime.now(timezone.utc) - last_ts).total_seconds() / 3600
                except (ValueError, TypeError):
                    age_hours = HEARTBEAT_HOURS  # timestamp corrupto: forzar heartbeat en vez de perder la señal de vida
                motivo = "heartbeat" if age_hours >= HEARTBEAT_HOURS else None

            if motivo:
                conn.execute(
                    """
                    INSERT INTO precio_stock_watch
                        (user_id, item_id, seller_sku, price, sale_amount, sale_regular_amount,
                         available_quantity, status, item_last_updated, motivo, error, ts_utc)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        user_id, iid, seller_sku, price, sale_amount, sale_regular_amount,
                        available_quantity, status, item_last_updated, motivo, error_text, now_iso,
                    ),
                )
                written += 1
                log.info("item=%s motivo=%s error=%s vals=%s", iid, motivo, error_text, new_vals)
            else:
                log.info("item=%s sin cambios, sin heartbeat pendiente -- no se escribe fila", iid)

        conn.commit()
    finally:
        conn.close()

    status = "partial" if any_error else "ok"
    log.info("=== watchdog_precios: %d filas escritas de %d items vigilados (status=%s) ===",
              written, len(watch_list), status)
    log_cron_run("watchdog_precios", user_id, status, written, time.time() - t0, None)
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--user-id", type=int, default=1)
    args = parser.parse_args()
    sys.exit(run(args.user_id))
