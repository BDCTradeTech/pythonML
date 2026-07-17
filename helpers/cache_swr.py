"""
helpers/cache_swr.py
Cache stale-while-revalidate genérico sobre app_config (ver db.get_cached / get_cached_stale_ok
/ set_cached). fresh se sirve directo, stale se sirve YA y dispara un único refresh en
background, y solo si no hay ni fresh ni stale se hace la llamada bloqueante.

Extraído de tabs/precios.py (mismo patrón usado ahí para price_to_win/quality/promo bulk,
y en ml_api.py para ml_get_my_items) para poder reusarlo en tabs/dashboard.py sin duplicar
la lógica de caché. [PERF-PRODUCTOS]
"""
from __future__ import annotations

import logging
import threading
from typing import Any, Dict, List

from db import get_cached, get_cached_stale_ok, set_cached

FRESH_MIN = 15
STALE_MIN = 60


def cached_or_refresh(cache_key: str, fetch_fn):
    """Cache de un solo valor global (ej. bulk de promos de todo el vendedor).
    fetch_fn: callable sin argumentos que hace el trabajo real y devuelve el valor a cachear."""
    cached = get_cached(cache_key, max_age_minutes=FRESH_MIN)
    if cached is not None:
        return cached
    stale = get_cached_stale_ok(cache_key, max_age_minutes=STALE_MIN)
    if stale is not None:
        def _refresh_bg() -> None:
            try:
                set_cached(cache_key, fetch_fn())
            except Exception:
                logging.exception(f"[CACHE-SWR] cache_bg_refresh_error key={cache_key}")
        threading.Thread(target=_refresh_bg, daemon=True, name="cache_swr_bg_refresh").start()
        return stale
    valor = fetch_fn()
    set_cached(cache_key, valor)
    return valor


def cached_or_refresh_bulk(key_prefix: str, ids: List[str], fetch_fn):
    """Cache individual por id (key = f"{key_prefix}_{id}") para resultados por-item
    (price_to_win, quality, promo por publicación). fetch_fn(ids_a_buscar) hace la llamada real
    (bulk/paralela) y devuelve dict {id: valor}. Un solo thread de refresh en background por
    llamada (no uno por id) para no saturar la API cuando hay muchos ids stale a la vez."""
    out: Dict[str, Any] = {}
    faltantes_fresh: List[str] = []
    for iid in ids:
        cached = get_cached(f"{key_prefix}_{iid}", max_age_minutes=FRESH_MIN)
        if cached is not None:
            out[iid] = cached
        else:
            faltantes_fresh.append(iid)

    stale_ids: List[str] = []
    miss_ids: List[str] = []
    for iid in faltantes_fresh:
        stale = get_cached_stale_ok(f"{key_prefix}_{iid}", max_age_minutes=STALE_MIN)
        if stale is not None:
            out[iid] = stale
            stale_ids.append(iid)
        else:
            miss_ids.append(iid)

    if stale_ids:
        def _refresh_bg() -> None:
            try:
                frescos = fetch_fn(stale_ids) or {}
                for iid, val in frescos.items():
                    if val is not None:
                        set_cached(f"{key_prefix}_{iid}", val)
            except Exception:
                logging.exception(f"[CACHE-SWR] cache_bg_refresh_error prefix={key_prefix}")
        threading.Thread(target=_refresh_bg, daemon=True, name=f"cache_swr_bg_{key_prefix}").start()

    if miss_ids:
        frescos_bloqueante = fetch_fn(miss_ids) or {}
        for iid, val in frescos_bloqueante.items():
            out[iid] = val
            if val is not None:
                set_cached(f"{key_prefix}_{iid}", val)

    return out


# Alias con el nombre histórico (guion bajo) usado en tabs/precios.py, para no tener que
# renombrar los call sites existentes al extraer estas funciones a este módulo.
_cached_or_refresh = cached_or_refresh
_cached_or_refresh_bulk = cached_or_refresh_bulk
