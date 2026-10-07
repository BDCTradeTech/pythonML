"""
ordenes_cache_refresh.py
Refresca el estado de las órdenes de ml_orders_cache (status, pagos, refunds, date_last_updated).

Por qué existe: ml_get_orders_incremental solo re-lee desde max(date_created)-1 día, así que una
orden que se cancela / reembolsa después queda para siempre con el status viejo en el cache.

Uso (cron diario, de madrugada):
    python3 /opt/pythonml/ordenes_cache_refresh.py
Cron: 30 4 * * * cd /opt/pythonml && set -a && . ./.env && set +a && ./venv/bin/python3 ordenes_cache_refresh.py >> /var/log/pythonml_ordenes.log 2>&1
(04:30: después de competidores_snapshot (04:00, termina ~04:15) y antes de salud_audit (05:30).)

Estrategia: orders/search con order.date_last_updated.from = hoy-45d (verificado en vivo con un GET:
filtra por última modificación, devuelve la orden completa con order_items y payments, pagina
offset/limit=50 y solo admite sort=date_desc/date_asc, que ordenan por date_created). Así se
detectan también cancelaciones tardías de órdenes más viejas que 45 días de creadas, siempre que
ML las haya modificado dentro de la ventana. Upsert idempotente, no borra filas.

Este módulo también expone buscar_ordenes() (paginado con reintentos), que usa
corregir_historial_ordenes.py.
"""
from __future__ import annotations

import json
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

log = logging.getLogger("ordenes_cache_refresh")

VENTANA_DIAS = 45
PAGE_SIZE = 50
ART = timezone(timedelta(hours=-3))
JOB = "ordenes_cache"


def _iso_art(dt: datetime) -> str:
    return dt.astimezone(ART).strftime("%Y-%m-%dT%H:%M:%S.000-03:00")


def buscar_ordenes(
    access_token: str,
    seller_id: str,
    params_fecha: Dict[str, str],
    session=None,
    max_paginas: int = 400,
) -> Tuple[List[Dict[str, Any]], bool]:
    """GET orders/search paginado (solo lectura). params_fecha: p.ej.
    {"order.date_last_updated.from": iso} o {"order.date_created.from": iso, "order.date_created.to": iso}.
    Devuelve (ordenes_sin_duplicados, truncado). truncado=True si una página falló después de
    reintentar o si se cortó el paginado antes de agotar el total (se devuelve lo recolectado)."""
    if session is None:
        from ml_api import get_ml_session
        session = get_ml_session()
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    url = "https://api.mercadolibre.com/orders/search"
    out: List[Dict[str, Any]] = []
    vistos: set = set()
    offset = 0
    total: Optional[int] = None
    truncado = False
    for _ in range(max_paginas):
        params = {"seller": seller_id, "sort": "date_desc", "limit": PAGE_SIZE, "offset": offset, **params_fecha}
        data = None
        for intento in range(3):
            try:
                r = session.get(url, params=params, headers=headers, timeout=30)
                if r.ok:
                    data = r.json()
                    break
                log.warning("orders/search %s offset=%s intento %s: %s", r.status_code, offset, intento + 1, r.text[:150])
            except Exception as e:  # red / timeout
                log.warning("orders/search offset=%s intento %s: %s", offset, intento + 1, e)
            time.sleep(2 * (intento + 1))
        if data is None:
            truncado = True
            break
        res = data.get("results") or []
        if total is None:
            total = (data.get("paging") or {}).get("total")
        for o in res:
            oid = str(o.get("id") or "")
            if oid and oid not in vistos:
                vistos.add(oid)
                out.append(o)
        offset += len(res)
        if len(res) < PAGE_SIZE or (total is not None and offset >= total):
            break
    else:
        truncado = True
    if total is not None and len(out) < total and not truncado:
        # offset >= total corta el loop; si quedó por debajo es porque ML devolvió páginas cortas.
        truncado = len(out) < total - 5
    return out, truncado


def _estado_previo(user_id: int) -> Dict[str, str]:
    from db import get_connection
    conn = get_connection()
    try:
        return {r[0]: (r[1] or "") for r in conn.execute(
            "SELECT order_id, status FROM ml_orders_cache WHERE user_id = ?", (user_id,))}
    finally:
        conn.close()


def refrescar_usuario(user_id: int, token: str, seller_id: str, dias: int = VENTANA_DIAS) -> Tuple[str, int, Optional[str]]:
    """Re-lee las órdenes modificadas en los últimos `dias` días y las upserta.
    Devuelve (status, n_ordenes_leidas, error). Log: cuántas cambiaron de estado."""
    from db import upsert_orders_cache
    desde = _iso_art(datetime.now(timezone.utc) - timedelta(days=dias))
    ordenes, truncado = buscar_ordenes(token, seller_id, {"order.date_last_updated.from": desde})
    if not ordenes and truncado:
        return "fail", 0, "orders/search falló sin devolver órdenes"
    previo = _estado_previo(user_id)
    cambios: Dict[str, int] = {}
    nuevas = 0
    for o in ordenes:
        oid = str(o.get("id"))
        if oid not in previo:
            nuevas += 1
        elif (previo[oid] or "") != (o.get("status") or ""):
            k = f"{previo[oid] or '?'}->{o.get('status') or '?'}"
            cambios[k] = cambios.get(k, 0) + 1
    upsert_orders_cache(user_id, ordenes)
    log.info("  user_id=%s: %d órdenes leídas (%d nuevas); cambios de estado: %s",
             user_id, len(ordenes), nuevas, json.dumps(cambios, sort_keys=True) if cambios else "ninguno")
    if truncado:
        return "partial", len(ordenes), "paginado incompleto (una página de orders/search falló)"
    return "ok", len(ordenes), None


def main() -> None:
    import requests
    from db import get_connection, init_cron_runs_db, init_orders_cache_schema, log_cron_run
    from ml_api import get_ml_access_token

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    hoy = datetime.now().date().isoformat()
    log.info("=== Refresco órdenes %s ===", hoy)
    init_cron_runs_db()
    init_orders_cache_schema()
    conn = get_connection()
    creds = conn.execute("SELECT id, user_id, raw_data FROM ml_credentials").fetchall()
    conn.close()

    for i, (_cid, user_id, raw_data) in enumerate(creds):
        if i > 0:
            time.sleep(5)  # espaciar refresh de token entre usuarios (rate limit de /oauth/token)
        t0 = time.time()
        status, count, error = "fail", 0, None
        try:
            log.info("Procesando user_id=%s", user_id)
            token = get_ml_access_token(user_id)
            if not token:
                error = "Sin token ML"
                log.warning("Sin token para user_id=%s, salteando", user_id)
                continue
            seller_id = None
            try:
                seller_id = str(json.loads(raw_data).get("user_id") or "") or None
            except Exception:
                pass
            if not seller_id:
                me = requests.get("https://api.mercadolibre.com/users/me",
                                  headers={"Authorization": f"Bearer {token}"}, timeout=10)
                seller_id = str(me.json().get("id")) if me.ok else None
            if not seller_id:
                error = "No se pudo resolver seller_id"
                continue
            status, count, error = refrescar_usuario(user_id, token, seller_id)
        except Exception as e:
            error = str(e)
            log.error("Error procesando user_id=%s: %s", user_id, e)
        finally:
            try:
                log_cron_run(JOB, user_id, status, count, time.time() - t0, error)
            except Exception as log_e:
                log.error("No se pudo loguear cron_runs: %s", log_e)
    log.info("=== Refresco completado ===")


if __name__ == "__main__":
    main()
