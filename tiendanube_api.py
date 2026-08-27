"""tiendanube_api.py — Integración con la API de Tiendanube/Nuvemshop.

No importar nicegui desde este módulo.
"""
from __future__ import annotations

import logging
import os
from typing import Optional, Tuple

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
