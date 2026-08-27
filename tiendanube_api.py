"""tiendanube_api.py — Integración con la API de Tiendanube/Nuvemshop.

No importar nicegui desde este módulo.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

# Versionado por fecha (no semver) -- Tiendanube lo va a deprecar en algún momento.
# Ver https://tiendanube.github.io/api-documentation/intro
TIENDANUBE_API_VERSION = "2025-03"
API_BASE = f"https://api.tiendanube.com/{TIENDANUBE_API_VERSION}"


def _user_agent() -> str:
    ua = os.getenv("TIENDANUBE_USER_AGENT", "").strip()
    if not ua:
        logging.warning(
            "[TIENDANUBE] TIENDANUBE_USER_AGENT no configurado -- la API responde 400 sin este header"
        )
    return ua


def _headers_for_style(access_token: str, style: str, user_agent: str) -> dict:
    if style == "authorization":
        return {"Authorization": f"Bearer {access_token}", "User-Agent": user_agent}
    if style == "authentication":
        return {"Authentication": f"bearer {access_token}", "User-Agent": user_agent}
    raise ValueError(f"auth_header_style desconocido: {style!r}")


def tiendanube_test_connection(store_id: str, access_token: str) -> Tuple[bool, str, Optional[str]]:
    """Prueba Authorization: Bearer primero (doc viva); si da 401, reintenta con
    Authentication: bearer (lo que usa el SDK oficial, que parece viejo -- usa /v1/
    contra /2025-03/ de la doc actual). Se decide por evidencia (qué responde 200),
    no por criterio. Devuelve (ok, mensaje, auth_header_style_que_funciono_o_None).

    Corta antes de salir a la red si falta TIENDANUBE_USER_AGENT -- sin ese header
    Tiendanube responde 400 y el mensaje llevaría a pensar que el problema es el
    token/store_id cuando en realidad es config del servidor.
    """
    ua = _user_agent()
    if not ua:
        return False, "Falta configurar TIENDANUBE_USER_AGENT en el servidor", None

    url = f"{API_BASE}/{store_id}/store"
    for style in ("authorization", "authentication"):
        try:
            resp = requests.get(url, headers=_headers_for_style(access_token, style, ua), timeout=10)
        except Exception as ex:
            return False, str(ex), None
        if resp.status_code == 200:
            try:
                data = resp.json()
            except Exception:
                data = None
            nombre = "OK"
            if isinstance(data, dict):
                name_field = data.get("name")
                if isinstance(name_field, dict):
                    nombre = name_field.get("es") or next(iter(name_field.values()), "OK")
                elif isinstance(name_field, str) and name_field:
                    nombre = name_field
            return True, nombre, style
        if resp.status_code != 401:
            return False, f"HTTP {resp.status_code}: {resp.text[:200]}", None
        logging.info(f"[TIENDANUBE] test_connection style={style} -> 401, reintentando con el otro header")
    return False, "401 con ambos headers (Authorization y Authentication)", None


def tiendanube_get(store_id: str, access_token: str, auth_header_style: str, path: str, timeout: int = 15) -> requests.Response:
    """GET autenticado usando el estilo de header ya confirmado por tiendanube_test_connection."""
    url = f"{API_BASE}/{store_id}/{path.lstrip('/')}"
    return requests.get(url, headers=_headers_for_style(access_token, auth_header_style, _user_agent()), timeout=timeout)


class _RateLimiter:
    """Leaky bucket: burst 40, refill 2/s (doc oficial de Tiendanube). Se auto-corrige
    con el header x-rate-limit-remaining de cada response, por si el estado local se
    desalinea del real (por ejemplo si otro proceso pega contra la misma tienda)."""

    def __init__(self, capacity: int = 40, refill_per_sec: float = 2.0):
        self._capacity = capacity
        self._tokens = float(capacity)
        self._refill = refill_per_sec
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            self._tokens = min(self._capacity, self._tokens + (now - self._last) * self._refill)
            self._last = now
            if self._tokens < 1:
                sleep_for = (1 - self._tokens) / self._refill
                time.sleep(sleep_for)
                self._tokens = 0.0
            else:
                self._tokens -= 1

    def sync_from_headers(self, headers: Any) -> None:
        remaining = headers.get("x-rate-limit-remaining")
        if remaining is not None:
            try:
                with self._lock:
                    self._tokens = min(self._tokens, float(remaining))
            except ValueError:
                pass


_rate_limiter = _RateLimiter()


def _get_with_backoff(url_or_path: str, store_id: str, access_token: str, auth_header_style: str,
                       max_retries: int = 3) -> requests.Response:
    """GET con rate limiting propio (el SDK oficial de Tiendanube no lo maneja) y
    backoff ante 429. Loguea cada backoff -- con la tienda de prueba vacía no se puede
    estresar el rate limiter de verdad, así que el log es la única forma de confirmar
    que funciona el día que haya volumen real."""
    resp = None
    for intento in range(max_retries + 1):
        _rate_limiter.acquire()
        if url_or_path.startswith("http"):
            resp = requests.get(
                url_or_path,
                headers=_headers_for_style(access_token, auth_header_style, _user_agent()),
                timeout=15,
            )
        else:
            resp = tiendanube_get(store_id, access_token, auth_header_style, url_or_path)
        _rate_limiter.sync_from_headers(resp.headers)
        if resp.status_code != 429:
            return resp
        retry_after = resp.headers.get("Retry-After")
        reset_ms = resp.headers.get("x-rate-limit-reset")
        if retry_after:
            espera = float(retry_after)
        elif reset_ms:
            espera = float(reset_ms) / 1000.0
        else:
            espera = 1.0
        logging.warning(
            f"[TIENDANUBE] 429 (rate limit) en intento {intento + 1}/{max_retries + 1}, "
            f"esperando {espera:.2f}s antes de reintentar -- url_or_path={url_or_path}"
        )
        time.sleep(espera)
    return resp


def tiendanube_list_products_with_variants(
    store_id: str, access_token: str, auth_header_style: str,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """Lee TODOS los productos+variantes de Tiendanube, paginando vía el header Link
    (recomendado por la doc oficial en vez de armar la URL de la página siguiente a
    mano). Devuelve (filas, error). Si una página falla incluso después de agotar los
    reintentos del rate limiter, CORTA la lectura ahí y devuelve un error explícito
    junto con lo leído hasta ese punto -- nunca se descarta en silencio ni se presenta
    una lista parcial como si estuviera completa.

    El precio ("price") viene de la API como STRING (ej. "25.00"), no como número --
    se preserva tal cual, sin convertir ni redondear, hasta que haya una razón real
    (con datos reales) para normalizarlo."""
    filas: List[Dict[str, Any]] = []
    url: Optional[str] = None
    path = "products?page=1&per_page=200"
    pagina = 1
    try:
        while True:
            resp = _get_with_backoff(url or path, store_id, access_token, auth_header_style)
            if resp.status_code != 200:
                error = (
                    f"Lectura incompleta: falló la página {pagina} con HTTP {resp.status_code} "
                    f"tras agotar reintentos -- {resp.text[:300]}. Se leyeron {len(filas)} "
                    f"variantes antes del corte."
                )
                logging.error(f"[TIENDANUBE] {error}")
                return filas, error
            for producto in resp.json():
                nombre_field = producto.get("name")
                if isinstance(nombre_field, dict):
                    nombre = nombre_field.get("es") or next(iter(nombre_field.values()), "")
                else:
                    nombre = nombre_field or ""
                product_id = str(producto.get("id"))
                for variante in producto.get("variants") or []:
                    filas.append({
                        "variant_id": str(variante.get("id")),
                        "product_id": str(variante.get("product_id") or product_id),
                        "sku": (variante.get("sku") or "").strip(),
                        "nombre": nombre,
                        "precio": variante.get("price"),  # crudo, string -- no convertir
                        "stock": variante.get("stock"),  # puede ser None si stock_management=False
                    })
            next_link = resp.links.get("next", {}).get("url") if hasattr(resp, "links") else None
            if not next_link:
                break
            url = next_link
            pagina += 1
        return filas, None
    except Exception as ex:
        error = f"Excepción durante la lectura (página {pagina}): {ex}. Se leyeron {len(filas)} variantes antes del corte."
        logging.exception(f"[TIENDANUBE] tiendanube_list_products_with_variants -- {error}")
        return filas, error
