"""
ads_snapshot.py
Sync diario de metricas de Publicidad (ML Ads / Product Ads API) por usuario.
Uso: python3 /opt/pythonml/ads_snapshot.py
Cron: 30 10 * * * /opt/pythonml/venv/bin/python3 /opt/pythonml/ads_snapshot.py >> /var/log/pythonml_ads.log 2>&1
(droplet en America/Argentina/Buenos_Aires -- 10:30 hora local. ML actualiza las metricas
de Ads a las 10:00 GMT-3, por eso se corre despues de las 10:30 BA.)

Nivel campania: se re-escriben (upsert) los ultimos ROLLBACK_DAYS dias en cada corrida, para
absorber revisiones de atribucion de ML (la ventana de atribucion de Ads es de 14 dias -- si
solo pidieramos "lo nuevo desde la ultima corrida" nunca volveriamos a pisar dias ya
guardados, aunque ML les cambie el total_amount/units_quantity retroactivamente). Primera
corrida por usuario: backfill de BACKFILL_DAYS dias (90, el maximo que permite la API por
request).

Nivel item: NO se cachea con granularidad diaria -- aggregation_type=DAILY con mas de un
item_id en filters[item_id] devuelve 500 "error_calculating_summarized_metrics" (probado en
vivo, reproducible). En su lugar se cachea un snapshot agregado por periodo (7d/30d/mes) via
aggregation_type=item (default), pisando el ultimo snapshot por item+periodo (sin acumular
historico) para que la tabla no crezca sin control.

Usuarios sin Publicidad habilitada (advertiser_id inexistente, 404 "No permissions found"):
se loguean como "skip" en cron_runs y se sigue con el resto -- no debe romper la corrida.
"""
import logging
import sys
import time
from datetime import date, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from dotenv import load_dotenv
load_dotenv(BASE_DIR / ".env")

from db import (
    get_connection, init_cron_runs_db, init_ads_tables, log_cron_run,
    get_ads_campaign_daily_max_date, upsert_ads_advertiser, upsert_ads_campaigns,
    upsert_ads_campaign_daily, upsert_ads_item_snapshot,
)
from ml_api import (
    get_ml_access_token, ml_ads_get_advertiser, ml_ads_get_campaigns,
    ml_ads_get_campaign_daily, ml_ads_get_items,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

BACKFILL_DAYS = 90
ROLLBACK_DAYS = 14  # ventana de atribucion de ML Ads: se re-escriben estos ultimos dias siempre
PERIODOS = ("7d", "30d", "mes")


def _rango_periodo(periodo: str, hoy: date) -> tuple[date, date]:
    if periodo == "7d":
        return hoy - timedelta(days=6), hoy
    if periodo == "30d":
        return hoy - timedelta(days=29), hoy
    if periodo == "mes":
        return hoy.replace(day=1), hoy
    raise ValueError(periodo)


def sync_user(user_id: int) -> tuple[str, int, str | None]:
    token = get_ml_access_token(user_id)
    if not token:
        return "skip", 0, "Sin token ML"

    advertiser = ml_ads_get_advertiser(token)
    if not advertiser:
        return "skip", 0, "Sin Publicidad habilitada (sin advertiser_id)"

    advertiser_id = advertiser["advertiser_id"]
    site_id = advertiser["site_id"]
    upsert_ads_advertiser(user_id, advertiser_id, site_id,
                           advertiser.get("advertiser_name"), advertiser.get("account_name"))

    hoy = date.today()
    max_date = get_ads_campaign_daily_max_date(user_id)
    if max_date:
        date_from_camp = hoy - timedelta(days=ROLLBACK_DAYS - 1)
    else:
        date_from_camp = hoy - timedelta(days=BACKFILL_DAYS - 1)
        log.info(f"  user_id={user_id}: primera corrida, backfill {BACKFILL_DAYS} dias")

    campaigns = ml_ads_get_campaigns(token, site_id, advertiser_id, date_from_camp.isoformat(), hoy.isoformat())
    if not campaigns:
        return "ok", 0, None
    upsert_ads_campaigns(user_id, site_id, advertiser_id, campaigns)

    count = 0
    for c in campaigns:
        campaign_id = c.get("id")
        if not campaign_id:
            continue

        daily = ml_ads_get_campaign_daily(token, site_id, advertiser_id, campaign_id,
                                           date_from_camp.isoformat(), hoy.isoformat())
        if daily:
            upsert_ads_campaign_daily(user_id, campaign_id, daily)
            count += len(daily)

        for periodo in PERIODOS:
            d_from, d_to = _rango_periodo(periodo, hoy)
            items = ml_ads_get_items(token, site_id, advertiser_id, campaign_id,
                                      d_from.isoformat(), d_to.isoformat())
            if items:
                upsert_ads_item_snapshot(user_id, campaign_id, periodo, items)
                count += len(items)

    return "ok", count, None


def run_sync():
    today = date.today().isoformat()
    log.info(f"=== Ads snapshot {today} ===")
    init_cron_runs_db()
    init_ads_tables()

    conn = get_connection()
    user_ids = [r[0] for r in conn.execute("SELECT DISTINCT user_id FROM ml_credentials").fetchall()]
    conn.close()

    total_count = 0
    for i, user_id in enumerate(user_ids):
        if i > 0:
            # Espaciar el refresh de token entre usuarios (mismo criterio que stock_snapshot.py).
            time.sleep(5)
        t0 = time.time()
        status, count, error = "fail", 0, None
        try:
            status, count, error = sync_user(user_id)
            total_count += count
            log.info(f"  user_id={user_id}: {status} ({count} filas) {error or ''}")
        except Exception as e:
            error = str(e)
            log.exception(f"  user_id={user_id}: error")
        finally:
            try:
                log_cron_run("ads", user_id, status, count, time.time() - t0, error)
            except Exception:
                log.exception("No se pudo loguear cron_runs")

    log.info(f"=== Ads snapshot completado: {total_count} filas guardadas/actualizadas ===")


if __name__ == "__main__":
    run_sync()
