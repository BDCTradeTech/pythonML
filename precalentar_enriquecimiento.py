"""
Precalienta las caches de enriquecimiento POR ITEM de la pagina Productos
(enriq_price_to_win_{user_id}_{item_id} y enriq_quality_{user_id}_{item_id})
para que la primera carga del dia no pague el costo bloqueante de estas
llamadas (ml_get_item_price_to_win / ml_get_item_performance: una llamada
por item, sin endpoint bulk equivalente al de promos).

A diferencia de precalentar_promos.py (1-2 llamadas/usuario, corre cada 10
min), esto implica ~100-200 llamadas API por usuario por corrida -> pensado
para correr UNA VEZ POR DIA a las 7:00 (cubre el hueco nocturno), no cada 10
min. El resto del dia el cache se mantiene caliente solo, con el uso real:
cada visita a Productos re-primea el reloj de fresh/stale de estas mismas
claves via _cached_or_refresh_bulk (misma funcion que reusa este script).

Uso: cd /opt/pythonml && set -a && . ./.env && set +a && ./venv/bin/python3 precalentar_enriquecimiento.py
(el .env es necesario porque el refresh de tokens vencidos descifra
client_secret con CREDENTIAL_ENCRYPTION_KEY, que los crons no heredan de
systemd).
"""
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

from db import get_connection
from ml_api import get_ml_access_token, ml_get_my_items, ml_get_item_price_to_win, ml_get_item_performance
from tabs.precios import _cached_or_refresh_bulk

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("precalentar_enriquecimiento")


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


def _fetch_catalog_pos(access_token: str, ids: List[str]) -> Dict[str, Optional[Dict]]:
    res: Dict[str, Optional[Dict]] = {}
    with ThreadPoolExecutor(max_workers=min(16, len(ids))) as ex:
        futures = {ex.submit(ml_get_item_price_to_win, access_token, iid): iid for iid in ids}
        for fut in as_completed(futures):
            iid = futures[fut]
            try:
                res[iid] = fut.result()
            except Exception:
                res[iid] = None
    return res


def _fetch_quality(access_token: str, ids: List[str]) -> Dict[str, Dict]:
    res: Dict[str, Dict] = {}
    with ThreadPoolExecutor(max_workers=min(16, len(ids))) as ex:
        futures = {ex.submit(ml_get_item_performance, access_token, iid): iid for iid in ids}
        for fut in as_completed(futures):
            iid = futures[fut]
            try:
                res[iid] = fut.result()
            except Exception:
                res[iid] = {}
    return res


def _precalentar_usuario(user_id: int) -> bool:
    token = get_ml_access_token(user_id)
    if not token:
        log.warning(f"user_id={user_id}: sin access_token valido (no vinculado o refresh fallo), se salta")
        return False

    data = ml_get_my_items(token, include_paused=True, force_refresh=False, perf_uid=user_id)
    items = data.get("results", [])
    if not items:
        log.info(f"user_id={user_id}: sin publicaciones, nada que precalentar")
        return True

    # Mismo criterio de elegibilidad que _enriquecer_items en tabs/precios.py
    items_para_ptw = [
        r for r in items
        if (r.get("catalog_listing") is True or r.get("catalog_item_id") or bool(r.get("catalog_product_id")))
        and str(r.get("status") or "").lower() == "active"
        and str(r.get("catalog_item_id") or r.get("id") or "").strip()
    ]
    cat_ids = list({str(r.get("catalog_item_id") or r.get("id") or "") for r in items_para_ptw})

    items_para_quality = [
        r for r in items
        if str(r.get("status") or "").lower() == "active"
        and str(r.get("id") or "").strip()
    ]
    quality_ids = list({str(r["id"]) for r in items_para_quality if r.get("id")})

    n_ptw = n_quality = 0
    if cat_ids:
        _cached_or_refresh_bulk(f"enriq_price_to_win_{user_id}", cat_ids, lambda ids: _fetch_catalog_pos(token, ids))
        n_ptw = len(cat_ids)
    if quality_ids:
        _cached_or_refresh_bulk(f"enriq_quality_{user_id}", quality_ids, lambda ids: _fetch_quality(token, ids))
        n_quality = len(quality_ids)

    log.info(f"user_id={user_id}: OK, price_to_win={n_ptw} items, quality={n_quality} items")
    return True


def main() -> None:
    t0 = time.perf_counter()
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
    # Margen para que terminen threads de refresh en background que pudiera
    # haber disparado _cached_or_refresh_bulk (rama "stale", poco probable en
    # esta corrida diaria pero no imposible).
    time.sleep(2)
    log.info(f"Fin. OK={ok} FAIL={fail} de {len(user_ids)} usuarios. tiempo_total={time.perf_counter() - t0:.1f}s")


if __name__ == "__main__":
    main()
