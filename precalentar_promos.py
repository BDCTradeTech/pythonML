"""
Precalienta el cache de promos activas de la pagina Productos (enriq_promo_{user_id})
para que la ventana "fresh" (15 min) nunca se venza durante el horario de uso, evitando
el camino bloqueante de 6-11s de ml_get_active_promo_prices_bulk en la primera carga.

Pensado para correr cada 10 min via cron, en un horario acotado (ver crontab). No toca
el codigo de render de tabs/precios.py ni _cached_or_refresh: escribe la MISMA clave de
cache (f"enriq_promo_{user_id}") con set_cached, asi el hit del usuario es directo.

Uso: cd /opt/pythonml && set -a && . ./.env && set +a && ./venv/bin/python3 precalentar_promos.py
(el .env es necesario porque el refresh de tokens vencidos descifra client_secret con
CREDENTIAL_ENCRYPTION_KEY, que los crons no heredan de systemd).
"""
import logging

from db import get_connection, set_cached
from ml_api import get_ml_access_token, ml_get_user_id, ml_get_active_promo_prices_bulk

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("precalentar_promos")


def _usuarios_con_ml() -> list:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT user_id FROM ml_credentials WHERE access_token IS NOT NULL AND access_token != ''"
        )
        return [r["user_id"] for r in cur.fetchall()]
    finally:
        conn.close()


def _precalentar_usuario(user_id: int) -> bool:
    token = get_ml_access_token(user_id)
    if not token:
        log.warning(f"user_id={user_id}: sin access_token valido (no vinculado o refresh fallo), se salta")
        return False
    seller_id = ml_get_user_id(token)
    if not seller_id:
        log.warning(f"user_id={user_id}: no se pudo resolver seller_id, se salta")
        return False
    promo_data = ml_get_active_promo_prices_bulk(token, seller_id)
    if promo_data is None:
        log.warning(f"user_id={user_id} seller_id={seller_id}: ml_get_active_promo_prices_bulk devolvio None (llamada fallida)")
        return False
    set_cached(f"enriq_promo_{user_id}", promo_data)
    log.info(f"user_id={user_id} seller_id={seller_id}: OK, {len(promo_data)} items con promo activa cacheados")
    return True


def main() -> None:
    user_ids = _usuarios_con_ml()
    log.info(f"Usuarios con ML vinculado: {len(user_ids)}")
    ok = fail = 0
    for uid in user_ids:
        try:
            if _precalentar_usuario(uid):
                ok += 1
            else:
                fail += 1
        except Exception as e:
            log.error(f"user_id={uid}: FALLO inesperado - {e}")
            fail += 1
    log.info(f"Fin. OK={ok} FAIL={fail} de {len(user_ids)} usuarios")


if __name__ == "__main__":
    main()
