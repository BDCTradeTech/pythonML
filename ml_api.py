"""ml_api.py — Integración con la API de MercadoLibre.

Extraído de main.py (Fase 1 del refactor).
No importar nicegui ni bcrypt desde este módulo.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

from db import get_connection, get_ml_app_credentials


# ==========================
# INTEGRACIÓN MERCADOLIBRE
# ==========================


def _ml_refresh_token(user_id: int, refresh_token: str) -> Optional[Dict[str, Any]]:
    """Refresca el access_token usando refresh_token. Usa credenciales del usuario o .env."""
    app_creds = get_ml_app_credentials(user_id)
    if app_creds:
        client_id = app_creds["client_id"]
        client_secret = app_creds["client_secret"]
    else:
        client_id = os.getenv("ML_CLIENT_ID")
        client_secret = os.getenv("ML_CLIENT_SECRET")
    if not client_id or not client_secret or not refresh_token:
        return None
    try:
        resp = requests.post(
            "https://api.mercadolibre.com/oauth/token",
            data={
                "grant_type": "refresh_token",
                "client_id": client_id,
                "client_secret": client_secret,
                "refresh_token": refresh_token,
            },
            headers={"Accept": "application/json"},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


def get_ml_access_token(user_id: int) -> Optional[str]:
    """Obtiene un access_token válido de MercadoLibre. Si está vencido, intenta refrescarlo automáticamente."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT access_token, refresh_token, expires_at FROM ml_credentials WHERE user_id = ? ORDER BY id DESC LIMIT 1",
            (user_id,),
        )
        row = cur.fetchone()
        if not row or not row["access_token"]:
            return None
        access_token = row["access_token"]
        refresh_token = row["refresh_token"]
        expires_at = row["expires_at"]

        # Comprobar si el token está vencido o vence en los próximos 5 minutos
        needs_refresh = False
        if expires_at:
            try:
                exp_str = expires_at[:19].replace("T", " ")
                exp_dt = datetime.strptime(exp_str, "%Y-%m-%d %H:%M:%S")
                if datetime.now(timezone.utc).replace(tzinfo=None) >= exp_dt - timedelta(minutes=5):
                    needs_refresh = True
            except (ValueError, TypeError):
                needs_refresh = True  # Por si el formato es raro, intentar refresh

        if needs_refresh and refresh_token:
            data = _ml_refresh_token(user_id, refresh_token)
            if data and data.get("access_token"):
                new_token = data["access_token"]
                new_refresh = data.get("refresh_token") or refresh_token
                expires_in = data.get("expires_in")
                new_expires_at = None
                if isinstance(expires_in, (int, float)):
                    new_expires_at = (datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=int(expires_in))).isoformat()
                cur.execute(
                    "UPDATE ml_credentials SET access_token = ?, refresh_token = ?, expires_at = ?, raw_data = ? WHERE user_id = ?",
                    (new_token, new_refresh, new_expires_at, json.dumps(data, ensure_ascii=False), user_id),
                )
                conn.commit()
                return new_token
            return None  # Refresh falló; el usuario debe volver a vincular

        return access_token
    finally:
        conn.close()


def _parse_ml_item_body(body: dict) -> dict:
    """Convierte el body de la API /items al formato interno de la app."""
    marca = ""
    color = ""
    seller_sku = ""
    for att in body.get("attributes") or []:
        aid = (att.get("id") or "").strip().upper()
        if aid in ("BRAND", "MARCA"):
            val = att.get("value_name") or att.get("value_id")
            marca = str(val) if val is not None else ""
        elif aid in ("COLOR", "COLOUR"):
            val = att.get("value_name") or att.get("value_id")
            if val:
                color = str(val)
        elif aid == "SELLER_SKU":
            v = att.get("value_name") or att.get("value") or att.get("value_id")
            if v is None and att.get("values"):
                v = (att["values"][0] or {}).get("name") or (att["values"][0] or {}).get("value_name")
            if v is not None:
                seller_sku = str(v).strip()
        if marca and color and seller_sku:
            break
    if not seller_sku:
        seller_sku = (body.get("seller_custom_field") or "").strip()
    if not seller_sku:
        for var in body.get("variations") or []:
            for vatt in (var.get("attribute_combinations") or var.get("attributes") or []):
                if (vatt.get("id") or "").strip().upper() == "SELLER_SKU":
                    v = vatt.get("value_name") or vatt.get("value") or vatt.get("value_id")
                    if v is not None:
                        seller_sku = str(v).strip()
                        break
            if seller_sku:
                break
    if not color:
        tit = (body.get("title") or "").lower()
        colores = ["negro", "blanco", "azul", "rojo", "gris", "verde", "amarillo", "naranja", "rosa", "marron", "beige", "celeste", "plateado", "dorado", "violeta", "multicolor", "silver", "space gray", "space grey", "gold", "negro espacial", "midnight"]
        equiv = {"silver": "Plateado", "space gray": "Gris", "space grey": "Gris", "gold": "Dorado", "negro espacial": "Negro", "midnight": "Negro"}
        for c in colores:
            if c in tit:
                color = equiv.get(c, c.capitalize())
                break
    catalog_listing = body.get("catalog_listing") is True
    original_price = body.get("original_price") or body.get("base_price")
    thumbnail = body.get("thumbnail") or ""
    if not thumbnail and body.get("pictures"):
        pic = (body.get("pictures") or [{}])[0]
        thumbnail = pic.get("secure_url") or pic.get("url") or ""
    return {
        "id": body.get("id"),
        "title": body.get("title", ""),
        "thumbnail": thumbnail,
        "price": body.get("price"),
        "sale_price": body.get("sale_price"),
        "original_price": original_price,
        "available_quantity": body.get("available_quantity"),
        "sold_quantity": body.get("sold_quantity"),
        "status": body.get("status", ""),
        "permalink": body.get("permalink", ""),
        "catalog_product_id": body.get("catalog_product_id"),
        "catalog_listing": catalog_listing,
        "listing_type_id": body.get("listing_type_id"),
        "category_id": body.get("category_id"),
        "sale_terms": body.get("sale_terms"),
        "seller_sku": seller_sku,
        "marca": marca or "—",
        "color": color or "—",
        "last_updated": body.get("last_updated"),
        "stop_time": body.get("stop_time"),
        "date_created": body.get("date_created"),
        "promotions": body.get("promotions") or [],
    }


def ml_get_my_items(access_token: str, include_paused: bool = False) -> Dict[str, Any]:
    """Obtiene las publicaciones del vendedor desde la API de MercadoLibre (paginado).
    include_paused=False (default): solo activas, carga más rápido.
    include_paused=True: incluye pausadas (sin stock), carga más lento."""
    base = "https://api.mercadolibre.com"
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}

    # 1. Obtener el user_id de ML del token
    me = requests.get(f"{base}/users/me", headers=headers, timeout=10)
    me.raise_for_status()
    ml_user_id = me.json().get("id")
    if not ml_user_id:
        return {"results": [], "paging": {"total": 0}, "error": "No se pudo obtener el usuario de ML"}

    # 2. Listar IDs: activas siempre; pausadas y closed solo si include_paused (catálogo vendido puede estar en closed)
    # ML limita offset a 1000; pasarlo devuelve 400 Bad Request
    item_ids = []
    seen: set = set()
    MAX_OFFSET = 1000
    statuses = ("active", "paused", "closed") if include_paused else ("active",)
    for status_val in statuses:
        offset = 0
        limit = 50
        while offset <= MAX_OFFSET:
            search = requests.get(
                f"{base}/users/{ml_user_id}/items/search",
                headers=headers,
                params={"limit": limit, "offset": offset, "status": status_val},
                timeout=15,
            )
            search.raise_for_status()
            search_data = search.json()
            chunk = search_data.get("results", [])
            for _id in chunk:
                if _id and _id not in seen:
                    seen.add(_id)
                    item_ids.append(_id)
            if len(chunk) < limit or offset + limit > MAX_OFFSET:
                break
            offset += limit

    paging = search_data.get("paging", {})
    total = paging.get("total", len(item_ids))

    if not item_ids:
        return {"results": [], "paging": {"total": total}, "seller_id": ml_user_id}

    # 3. Obtener detalles de cada ítem (la API acepta hasta 20 IDs por request)
    all_items = []
    for i in range(0, len(item_ids), 20):
        chunk = item_ids[i : i + 20]
        ids_param = ",".join(chunk)
        items_resp = requests.get(
            f"{base}/items",
            params={"ids": ids_param},
            headers=headers,
            timeout=15,
        )
        items_resp.raise_for_status()
        def _item_from_body(body: dict) -> dict:
            return _parse_ml_item_body(body)

        for item_data in items_resp.json():
            if isinstance(item_data, dict) and item_data.get("code") == 200:
                body = item_data.get("body", {})
                all_items.append(_item_from_body(body))
            elif isinstance(item_data, dict) and "body" in item_data:
                body = item_data["body"]
                all_items.append(_item_from_body(body))

    return {"results": all_items, "paging": {"total": total}, "seller_id": ml_user_id}


def _tipo_publicacion_desde_item(item: Dict[str, Any]) -> str:
    """Propia o Catálogo según catalog_listing (igual que en Ventas)."""
    if not item or not isinstance(item, dict):
        return "Propia"
    cl = item.get("catalog_listing")
    return "Catálogo" if (cl is True or str(cl or "").lower() in ("true", "1")) else "Propia"


def _cuotas_desde_item(item: Dict[str, Any]) -> str:
    """x1, x3, x6, x9 o x12 según listing_type_id y sale_terms/attributes (INSTALLMENTS_CAMPAIGN)."""
    listing_type_id = str(item.get("listing_type_id") or "").strip().lower()
    if listing_type_id == "gold_special":
        return "x1"
    if listing_type_id == "gold_pro":
        def _cuotas_desde_campaign(terms: list) -> str:
            for a in terms or []:
                if isinstance(a, dict) and (str(a.get("id") or "").upper() == "INSTALLMENTS_CAMPAIGN"):
                    vn = str(a.get("value_name") or "").lower()
                    if "12x" in vn:
                        return "x12"
                    if "9x" in vn:
                        return "x9"
                    if "6x" in vn:
                        return "x6"
                    if "3x" in vn or "3x_campaign" in vn or vn == "3x_campaign":
                        return "x3"
            return ""
        cuotas = _cuotas_desde_campaign(item.get("sale_terms")) or _cuotas_desde_campaign(item.get("attributes"))
        if cuotas:
            return cuotas
        return "x6"  # gold_pro por defecto: 6 cuotas
    return "x1"


def _body_to_precios_item(body: dict) -> dict:
    return _parse_ml_item_body(body)


def ml_update_item_price(access_token: str, item_id: str, price: float) -> Dict[str, Any]:
    """Actualiza el precio de una publicación en MercadoLibre (PUT /items/{id}). Solo publicaciones propias."""
    base = "https://api.mercadolibre.com"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    resp = requests.put(
        f"{base}/items/{item_id}",
        headers=headers,
        json={"price": int(round(price))},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def ml_get_one_item_full(access_token: str) -> Optional[Dict[str, Any]]:
    """Obtiene el JSON completo de una publicación de ejemplo (la primera) para mostrar qué datos devuelve ML."""
    base = "https://api.mercadolibre.com"
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}

    me = requests.get(f"{base}/users/me", headers=headers, timeout=10)
    me.raise_for_status()
    ml_user_id = me.json().get("id")
    if not ml_user_id:
        return None

    search = requests.get(
        f"{base}/users/{ml_user_id}/items/search",
        headers=headers,
        params={"limit": 1, "offset": 0, "status": "active"},
        timeout=15,
    )
    search.raise_for_status()
    item_ids = search.json().get("results", [])
    if not item_ids:
        return None

    item_id = item_ids[0]
    item_resp = requests.get(
        f"{base}/items/{item_id}",
        headers=headers,
        timeout=15,
    )
    item_resp.raise_for_status()
    return item_resp.json()


def ml_get_item_sale_price(access_token: Optional[str], item_id: str) -> Optional[float]:
    """Obtiene el precio de venta actual de un ítem. API: GET /items/{id}/sale_price
    Requiere token. Usar cuando /items no devuelve price (deprecado)."""
    if not access_token or not str(item_id).strip():
        return None
    try:
        resp = requests.get(
            f"https://api.mercadolibre.com/items/{item_id}/sale_price",
            params={"context": "channel_marketplace"},
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict):
            amt = data.get("amount")
            if amt is not None:
                try:
                    return float(amt)
                except (TypeError, ValueError):
                    pass
    except Exception:
        pass
    return None


def ml_get_item_sale_price_full(access_token: Optional[str], item_id: str) -> Optional[Dict[str, Any]]:
    """Obtiene amount, regular_amount, promotion_id, promotion_type y campaign_id de GET /items/{id}/sale_price.
    promotion_id/type pueden estar en metadata (API ML a veces los pone ahí)."""
    if not access_token or not str(item_id).strip():
        return None
    try:
        resp = requests.get(
            f"https://api.mercadolibre.com/items/{item_id}/sale_price",
            params={"context": "channel_marketplace"},
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict):
            amt = data.get("amount")
            reg = data.get("regular_amount")
            if amt is not None:
                try:
                    metadata = data.get("metadata") or {}
                    meta = metadata if isinstance(metadata, dict) else {}
                    out = {
                        "amount": float(amt),
                        "regular_amount": float(reg) if reg is not None else None,
                        "promotion_id": data.get("promotion_id") or meta.get("promotion_id"),
                        "promotion_type": (data.get("promotion_type") or meta.get("promotion_type") or "").strip() or None,
                        "campaign_id": data.get("campaign_id") or meta.get("campaign_id"),
                    }
                    return out
                except (TypeError, ValueError):
                    pass
    except Exception:
        pass
    return None


def ml_get_item_price_to_win(access_token: str, item_id: str) -> Optional[Dict[str, Any]]:
    """GET /items/{id}/price_to_win — devuelve dict con status, price_to_win, visit_share, reason, competitors."""
    if not access_token or not str(item_id).strip():
        return None
    try:
        resp = requests.get(
            f"https://api.mercadolibre.com/items/{item_id}/price_to_win",
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
            timeout=10,
        )
        if resp.status_code == 200:
            d = resp.json()
            return {
                "status":       d.get("status"),
                "price_to_win": d.get("price_to_win"),
                "visit_share":  d.get("visit_share"),
                "reason":       d.get("reason"),
                "competitors":  d.get("competitors_sharing_first_place"),
            }
    except Exception:
        pass
    return None


def ml_get_item_performance(access_token: str, item_id: str) -> Dict[str, Any]:
    """GET /item/{id}/performance — devuelve score (0-100) y level de calidad."""
    if not access_token or not str(item_id).strip():
        return {}
    try:
        resp = requests.get(
            f"https://api.mercadolibre.com/item/{item_id}/performance",
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
            timeout=10,
        )
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return {}


def ml_get_promotion_item_discounts_by_user(
    access_token: Optional[str], item_id: str, user_id: str, total_discount_pct: float
) -> Optional[Dict[str, float]]:
    """Fallback: cuando sale_price no devuelve promotion_id, buscar en promociones del usuario."""
    if not access_token or not item_id or not user_id:
        return None
    try:
        resp = requests.get(
            "https://api.mercadolibre.com/seller-promotions/users/" + str(user_id),
            params={"app_version": "v2"},
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
            timeout=10,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        promos = data.get("results") or []
        item_id_str = str(item_id or "").strip()
        item_id_short = item_id_str[3:] if item_id_str.upper().startswith("MLA") and len(item_id_str) > 3 else item_id_str
        for p in promos:
            if not isinstance(p, dict):
                continue
            promo_id = p.get("id")
            promo_type = (p.get("type") or "").strip().upper()
            if not promo_id or not promo_type:
                continue
            try:
                items_resp = requests.get(
                    f"https://api.mercadolibre.com/seller-promotions/promotions/{promo_id}/items",
                    params={"promotion_type": promo_type, "item_id": item_id, "app_version": "v2"},
                    headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
                    timeout=10,
                )
                if items_resp.status_code != 200:
                    continue
                items_data = items_resp.json()
                results = items_data.get("results") or []
                for r in results:
                    rid = str(r.get("id", "")).strip() if isinstance(r, dict) else ""
                    rid_short = rid[3:] if rid.upper().startswith("MLA") and len(rid) > 3 else rid
                    if rid and (rid == item_id_str or rid_short == item_id_short):
                        meli = r.get("meli_percentage") or r.get("meli_percent")
                        seller = r.get("seller_percentage") or r.get("seller_percent")
                        if meli is not None or seller is not None:
                            meli_f = float(meli or 0)
                            seller_f = float(seller or 0)
                            return {"meli_pct": meli_f, "seller_pct": seller_f}
                        benefits = p.get("benefits") or {}
                        meli = benefits.get("meli_percent") or benefits.get("meli_percentage")
                        seller = benefits.get("seller_percent") or benefits.get("seller_percentage")
                        if meli is not None or seller is not None:
                            meli_f = float(meli or 0)
                            seller_f = float(seller or 0)
                            if meli_f + seller_f > 0.01:
                                if abs((meli_f + seller_f) - total_discount_pct) < 1:
                                    return {"meli_pct": meli_f, "seller_pct": seller_f}
                                if abs((meli_f + seller_f) - 100) < 1:
                                    return {"meli_pct": total_discount_pct * meli_f / 100, "seller_pct": total_discount_pct * seller_f / 100}
                                return {"meli_pct": meli_f, "seller_pct": seller_f}
                        break
            except Exception:
                continue
    except Exception:
        pass
    return None


def ml_get_promotion_item_discounts_by_campaign(
    access_token: Optional[str], campaign_id: str, item_id: str, total_discount_pct: float, user_id: str,
    promotion_type_hint: Optional[str] = None,
) -> Optional[Dict[str, float]]:
    """Usa campaign_id de metadata. Si sale_price dio promotion_type (ej. MARKETPLACE_CAMPAIGN), lo prueba primero."""
    if not access_token or not campaign_id or not item_id or not user_id:
        return None
    cid = str(campaign_id).strip()
    pt_hint = (promotion_type_hint or "").strip().upper() if promotion_type_hint else ""
    try:
        if pt_hint and pt_hint not in ("OFFER", "OFFER-"):
            out = ml_get_promotion_item_discounts(access_token, cid, pt_hint, item_id, total_discount_pct)
            if out:
                return out
        resp = requests.get(
            "https://api.mercadolibre.com/seller-promotions/users/" + str(user_id),
            params={"app_version": "v2"},
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            promos = data.get("results") or []
            cid_norm = (cid[2:] if cid.upper().startswith("P-") else cid).upper()
            for p in promos:
                if not isinstance(p, dict):
                    continue
                promo_id = str(p.get("id") or "").strip()
                promo_type = (p.get("type") or "").strip().upper()
                pid_norm = (promo_id[2:] if promo_id.upper().startswith("P-") else promo_id).upper()
                if promo_type and pid_norm == cid_norm:
                    out = ml_get_promotion_item_discounts(
                        access_token, cid, promo_type, item_id, total_discount_pct
                    )
                    if out:
                        return out
        if cid.upper().startswith("P-MLA"):
            for fallback_type in ("SMART", "MARKETPLACE_CAMPAIGN"):
                if fallback_type == pt_hint:
                    continue
                out = ml_get_promotion_item_discounts(access_token, cid, fallback_type, item_id, total_discount_pct)
                if out:
                    return out
    except Exception:
        pass
    return None


def _find_item_in_promo_results(
    results: List[Dict], item_id: str, total_discount_pct: float
) -> Optional[Dict[str, float]]:
    """Busca el item en results y devuelve {meli_pct, seller_pct} si tiene meli/seller."""
    item_id_str = str(item_id or "").strip()
    item_id_short = item_id_str[3:] if item_id_str.upper().startswith("MLA") and len(item_id_str) > 3 else item_id_str
    for r in results:
        rid = str(r.get("id", "")).strip() if isinstance(r, dict) else ""
        rid_short = rid[3:] if rid.upper().startswith("MLA") and len(rid) > 3 else rid
        if rid and (rid == item_id_str or rid_short == item_id_short):
            meli = r.get("meli_percentage") or r.get("meli_percent")
            seller = r.get("seller_percentage") or r.get("seller_percent")
            if meli is not None or seller is not None:
                meli_f = float(meli or 0)
                seller_f = float(seller or 0)
                if meli_f + seller_f > 0.01:
                    if abs((meli_f + seller_f) - total_discount_pct) < 1:
                        return {"meli_pct": meli_f, "seller_pct": seller_f}
                    if abs((meli_f + seller_f) - 100) < 1:
                        return {"meli_pct": total_discount_pct * meli_f / 100, "seller_pct": total_discount_pct * seller_f / 100}
                    return {"meli_pct": meli_f, "seller_pct": seller_f}
            break
    return None


def ml_get_promotion_item_discounts(
    access_token: Optional[str], promotion_id: str, promotion_type: str, item_id: str,
    total_discount_pct: float,
) -> Optional[Dict[str, float]]:
    """Obtiene meli y seller % del ítem en la promo. Los benefits pueden ser puntos % (meli+seller=total) o proporción (meli+seller=100)."""
    if not access_token or not promotion_id or not item_id:
        return None
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    try:
        resp = requests.get(
            f"https://api.mercadolibre.com/seller-promotions/promotions/{promotion_id}/items",
            params={"promotion_type": promotion_type, "item_id": item_id, "app_version": "v2"},
            headers=headers,
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            results = data.get("results") or []
            out = _find_item_in_promo_results(results, item_id, total_discount_pct)
            if out:
                return out
            if not results:
                for offset in range(0, 200, 50):
                    r2 = requests.get(
                        f"https://api.mercadolibre.com/seller-promotions/promotions/{promotion_id}/items",
                        params={"promotion_type": promotion_type, "app_version": "v2", "limit": 50, "offset": offset},
                        headers=headers,
                        timeout=10,
                    )
                    if r2.status_code != 200:
                        break
                    data2 = r2.json()
                    results2 = data2.get("results") or []
                    out = _find_item_in_promo_results(results2, item_id, total_discount_pct)
                    if out:
                        return out
                    total = data2.get("paging", {}).get("total", 0)
                    if offset + len(results2) >= total or not results2:
                        break
        promo_resp = requests.get(
            f"https://api.mercadolibre.com/seller-promotions/promotions/{promotion_id}",
            params={"promotion_type": promotion_type, "app_version": "v2"},
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
            timeout=10,
        )
        if promo_resp.status_code == 200:
            promo_data = promo_resp.json()
            benefits = promo_data.get("benefits") or {}
            meli = benefits.get("meli_percent") or benefits.get("meli_percentage")
            seller = benefits.get("seller_percent") or benefits.get("seller_percentage")
            if meli is not None or seller is not None:
                meli_f = float(meli or 0)
                seller_f = float(seller or 0)
                if meli_f + seller_f > 0.01:
                    if abs((meli_f + seller_f) - total_discount_pct) < 1:
                        return {"meli_pct": meli_f, "seller_pct": seller_f}
                    if abs((meli_f + seller_f) - 100) < 1:
                        return {"meli_pct": total_discount_pct * meli_f / 100, "seller_pct": total_discount_pct * seller_f / 100}
                    return {"meli_pct": meli_f, "seller_pct": seller_f}
                elif meli_f > 0 or seller_f > 0:
                    if meli_f > 0 and seller_f == 0:
                        seller_inferred = max(0, total_discount_pct - meli_f)
                        return {"meli_pct": meli_f, "seller_pct": seller_inferred}
                    if seller_f > 0 and meli_f == 0:
                        meli_inferred = max(0, total_discount_pct - seller_f)
                        return {"meli_pct": meli_inferred, "seller_pct": seller_f}
    except Exception:
        pass
    return None


def ml_get_item_prices(access_token: Optional[str], item_id: str) -> Optional[float]:
    """Obtiene precios de un ítem. API: GET /items/{id}/prices. Fallback si sale_price falla."""
    if not access_token or not str(item_id).strip():
        return None
    try:
        resp = requests.get(
            f"https://api.mercadolibre.com/items/{item_id}/prices",
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict):
            prices = data.get("prices") or []
            for p in prices if isinstance(prices, list) else []:
                if isinstance(p, dict):
                    amt = p.get("amount")
                    if amt is not None:
                        try:
                            return float(amt)
                        except (TypeError, ValueError):
                            pass
    except Exception:
        pass
    return None


def ml_enriquecer_sale_price(items: List[Dict[str, Any]], access_token: Optional[str]) -> None:
    """Enriquece items con sale_price (precio real con promoción) si no lo tienen."""
    if not access_token:
        return
    for i in items:
        if i.get("sale_price") is not None:
            continue
        item_id = i.get("id")
        if not item_id:
            continue
        sp = ml_get_item_sale_price(access_token, str(item_id))
        if sp is not None:
            i["sale_price"] = sp


def ml_fetch_price_for_item(
    access_token: Optional[str], item_id: str, body: Optional[Dict[str, Any]] = None
) -> Optional[float]:
    """Obtiene el precio: primero del body, luego sale_price, luego prices."""
    if body is not None:
        for key in ("price", "base_price", "original_price"):
            val = body.get(key)
            if val is not None:
                try:
                    return float(val)
                except (TypeError, ValueError):
                    pass
    if not access_token:
        return None
    return ml_get_item_sale_price(access_token, item_id) or ml_get_item_prices(access_token, item_id)


def ml_get_product_detail(access_token: Optional[str], product_id: str) -> Optional[Dict[str, Any]]:
    """Obtiene detalle de producto de catálogo. Puede incluir buy_box_winner_price_range."""
    if not access_token or not str(product_id).strip():
        return None
    try:
        resp = requests.get(
            f"https://api.mercadolibre.com/products/{product_id}",
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json() if isinstance(resp.json(), dict) else None
    except Exception:
        return None


def _extraer_color_desde_texto(texto: str) -> str:
    """Busca palabras de color en un texto. Devuelve la primera coincidencia o ''."""
    if not texto or not isinstance(texto, str):
        return ""
    t = texto.lower()
    colores = ["negro", "blanco", "azul", "rojo", "gris", "verde", "amarillo", "naranja", "rosa", "marron", "beige", "celeste", "plateado", "dorado", "violeta", "multicolor", "black", "white", "blue", "red", "gray", "grey", "green", "yellow", "orange", "pink", "brown", "silver", "gold"]
    for c in colores:
        if c in t:
            return c.capitalize()
    return ""


def ml_get_item_description(access_token: Optional[str], item_id: str) -> str:
    """Obtiene el texto de la descripción del ítem. Devuelve '' si falla."""
    if not access_token or not str(item_id).strip():
        return ""
    try:
        resp = requests.get(
            f"https://api.mercadolibre.com/items/{item_id}/descriptions",
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
            timeout=8,
        )
        if not resp.ok:
            return ""
        data = resp.json()
        if not isinstance(data, list) or not data:
            return ""
        for d in data:
            if isinstance(d, dict):
                txt = d.get("plain_text") or d.get("text") or ""
                if txt:
                    return str(txt)
        return ""
    except Exception:
        return ""


def ml_get_item(access_token: Optional[str], item_id: str) -> Optional[Dict[str, Any]]:
    """Obtiene el detalle completo de un ítem (precio, stock, seller_id, etc.) por ID.
    Prueba con token y, si falla, sin token (GET /items/{id} a veces es público)."""
    item_id = str(item_id).strip()
    if not item_id:
        return None
    tries = [{"Accept": "application/json"}]
    if access_token:
        tries.insert(0, {"Accept": "application/json", "Authorization": f"Bearer {access_token}"})
    for headers in tries:
        try:
            resp = requests.get(
                f"https://api.mercadolibre.com/items/{item_id}",
                headers=headers,
                timeout=12,
            )
            resp.raise_for_status()
            data = resp.json()
            # La API puede devolver el ítem en "body" (multiget) o en la raíz
            if isinstance(data, dict) and "body" in data and data.get("code") == 200:
                return data.get("body") or data
            return data if isinstance(data, dict) else None
        except Exception:
            continue
    return None


def ml_get_items_multiget(access_token: Optional[str], item_ids: List[str]) -> List[Optional[Dict[str, Any]]]:
    """Obtiene varios ítem en una sola petición. API: GET /items?ids=ID1,ID2,ID3
    Documentación ML: la respuesta es un array en el mismo orden que los ids;
    cada elemento es { "code": 200, "body": { id, title, price, available_quantity, seller_id, permalink } }.
    Prueba sin token primero (listados públicos), luego con token."""
    if not item_ids:
        return []
    ids_clean = [str(i).strip() for i in item_ids if str(i).strip()][:20]
    if not ids_clean:
        return [None] * len(item_ids)
    ids_str = ",".join(ids_clean)
    # Sin attributes: ML está deprecando price en /items; sale_price se usa como fallback
    url = f"https://api.mercadolibre.com/items?ids={ids_str}"
    for headers in (
        ([{"Accept": "application/json", "Authorization": f"Bearer {access_token}"}] if access_token else []),
        [{"Accept": "application/json"}],
    ):
        if not headers:
            continue
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            continue
        # La API puede devolver array o un solo objeto { code, body } cuando hay un id
        if isinstance(data, dict) and "body" in data:
            data = [data]
        if not isinstance(data, list):
            continue
        out = []
        for elem in data:
            if isinstance(elem, dict) and elem.get("code") == 200:
                body = elem.get("body")
                out.append(body if isinstance(body, dict) else None)
            else:
                out.append(None)
        return out
    return [None] * len(ids_clean)


def ml_get_items_multiget_with_attributes(
    access_token: Optional[str], item_ids: List[str], attributes: str = "id,catalog_listing,catalog_product_id,attributes"
) -> List[Optional[Dict[str, Any]]]:
    """Obtiene ítems pidiendo atributos específicos (para catalog_listing). Máx 20 ids."""
    if not item_ids:
        return []
    ids_clean = [str(i).strip() for i in item_ids if str(i).strip()][:20]
    if not ids_clean:
        return [None] * len(item_ids)
    ids_str = ",".join(ids_clean)
    url = f"https://api.mercadolibre.com/items?ids={ids_str}&attributes={attributes}"
    if access_token:
        headers = {"Accept": "application/json", "Authorization": f"Bearer {access_token}"}
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            return [None] * len(ids_clean)
    else:
        return [None] * len(ids_clean)
    if isinstance(data, dict) and "body" in data:
        data = [data]
    if not isinstance(data, list):
        return [None] * len(ids_clean)
    out = []
    for elem in data:
        if isinstance(elem, dict) and elem.get("code") == 200:
            body = elem.get("body")
            out.append(body if isinstance(body, dict) else None)
        else:
            out.append(None)
    return out


def ml_get_items_multiget_all(
    access_token: Optional[str], item_ids: List[str]
) -> List[Optional[Dict[str, Any]]]:
    """Obtiene varios ítems en lotes de 20 (límite de la API). Devuelve lista en el mismo orden."""
    if not item_ids:
        return []
    ids_clean = [str(i).strip() for i in item_ids if str(i).strip()]
    out: List[Optional[Dict[str, Any]]] = []
    for i in range(0, len(ids_clean), 20):
        batch = ids_clean[i : i + 20]
        batch_bodies = ml_get_items_multiget(access_token, batch)
        out.extend(batch_bodies)
    return out


def ml_get_users_multiget(
    access_token: Optional[str], user_ids: List[str]
) -> Dict[str, str]:
    """Obtiene nicknames de usuarios. GET /users?ids=ID1,ID2. Devuelve {user_id: nickname}."""
    if not user_ids:
        return {}
    ids_clean = list(dict.fromkeys(str(i).strip() for i in user_ids if str(i).strip()))[:20]
    if not ids_clean:
        return {}
    ids_str = ",".join(ids_clean)
    url = f"https://api.mercadolibre.com/users?ids={ids_str}"
    headers_list = (
        [{"Accept": "application/json", "Authorization": f"Bearer {access_token}"}] if access_token else []
    ) + [{"Accept": "application/json"}]
    for h in headers_list:
        try:
            resp = requests.get(url, headers=h, timeout=12)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict) and "body" in data:
                data = [data]
            if not isinstance(data, list):
                continue
            out: Dict[str, str] = {}
            for elem in data:
                if isinstance(elem, dict) and elem.get("code") == 200:
                    body = elem.get("body")
                    if isinstance(body, dict):
                        uid = str(body.get("id", ""))
                        nick = (body.get("nickname") or "").strip()
                        if uid:
                            out[uid] = nick or f"ID {uid}"
            return out
        except Exception:
            continue
    return {}


def ml_get_user_id(access_token: str) -> Optional[str]:
    """Obtiene el user_id de MercadoLibre del token (seller_id)."""
    try:
        resp = requests.get(
            "https://api.mercadolibre.com/users/me",
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
            timeout=10,
        )
        resp.raise_for_status()
        return str(resp.json().get("id", "")) or None
    except Exception:
        return None


def ml_get_user_profile(access_token: str) -> Optional[Dict[str, Any]]:
    """Obtiene perfil completo (users/me + users/{id}) con reputación y métricas."""
    try:
        headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
        me = requests.get("https://api.mercadolibre.com/users/me", headers=headers, timeout=10)
        me.raise_for_status()
        data = me.json()
        user_id = data.get("id")
        if not user_id:
            return data
        full = requests.get(f"https://api.mercadolibre.com/users/{user_id}", headers=headers, timeout=10)
        if full.ok:
            prof = full.json()
            # Si metrics vacíos o todo 0, intentar global seller_reputation (multi-marketplace)
            rep = prof.get("seller_reputation") or {}
            metrics = rep.get("metrics") or {}
            has_data = any(
                (metrics.get(k) or {}).get("rate") or (metrics.get(k) or {}).get("value")
                or ((metrics.get(k) or {}).get("excluded") or {}).get("real_rate")
                for k in ["claims", "cancellations", "delayed_handling_time"]
            )
            if not has_data:
                try:
                    gr = requests.get(
                        "https://api.mercadolibre.com/global/users/seller_reputation",
                        headers=headers,
                        timeout=10,
                    )
                    if gr.ok:
                        glob = gr.json()
                        # Respuesta: { user_id, site_id, seller_reputation: [{ user_id, site_id, seller_reputation }] }
                        arr = (glob or {}).get("seller_reputation") or []
                        for item in arr:
                            if str(item.get("user_id")) == str(user_id):
                                sr = item.get("seller_reputation") or {}
                                if sr.get("metrics"):
                                    prof.setdefault("seller_reputation", {})["metrics"] = sr.get("metrics", {})
                                break
                        if not arr and (glob or {}).get("seller_reputation"):
                            sr = (glob.get("seller_reputation") or [{}])[0]
                            if isinstance(sr, dict) and sr.get("metrics"):
                                prof.setdefault("seller_reputation", {})["metrics"] = sr.get("metrics", {})
                except Exception:
                    pass
            return prof
        return data
    except Exception:
        return None


ORDERS_MAX_OFFSET = 100000  # ML puede limitar offset; si devuelve 400 se detiene antes


def ml_get_orders(
    access_token: str,
    seller_id: str,
    limit: int = 100,
    offset: int = 0,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> Dict[str, Any]:
    """Lista órdenes del vendedor. Pagina hasta `limit` (máx 50 por request, ML no acepta más).
    sort=date_desc para órdenes más recientes primero.
    date_from/date_to: ISO 8601 (ej. 2025-02-01T00:00:00.000-03:00) para filtrar por fecha."""
    log = logging.getLogger(__name__)
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    page_size = 50
    date_params: Dict[str, str] = {}
    if date_from:
        date_params["order_created_from"] = date_from
    if date_to:
        date_params["order_created_to"] = date_to

    all_flat: List[Dict[str, Any]] = []
    seen_ids: set = set()

    def _flatten_raw(raw_list: list) -> list:
        out = []
        for r in raw_list:
            if not isinstance(r, dict):
                continue
            nested = r.get("orders") or []
            if nested:
                for o in nested:
                    if isinstance(o, dict):
                        out.append(o)
            else:
                out.append(r)
        return out

    for url, extra in [
        ("https://api.mercadolibre.com/orders/search", {"seller": seller_id}),
        ("https://api.mercadolibre.com/orders/search", {"seller": seller_id, "caller.id": seller_id}),
        ("https://api.mercadolibre.com/marketplace/orders/search", {"seller.id": seller_id}),
        ("https://api.mercadolibre.com/marketplace/orders/search", {"seller.id": seller_id, "caller.id": seller_id}),
    ]:
        off = offset
        while len(all_flat) < limit and off <= ORDERS_MAX_OFFSET:
            params: Dict[str, Any] = {**extra, **date_params, "limit": page_size, "offset": off, "sort": "date_desc"}
            try:
                resp = requests.get(url, params=params, headers=headers, timeout=25)
                if not resp.ok:
                    if off == offset:
                        try:
                            err_body = resp.json()
                        except Exception:
                            err_body = resp.text[:300]
                        log.debug("ML orders %s %s: %s", url.split("/")[-1], resp.status_code, err_body)
                    break
                data = resp.json()
                raw = data.get("results") or data.get("orders") or data.get("elements") or []

                if not raw:
                    break

                if isinstance(raw[0], (int, float)):
                    for oid in raw[:page_size]:
                        try:
                            r = requests.get(f"https://api.mercadolibre.com/orders/{int(oid)}", headers=headers, timeout=10)
                            if r.status_code == 200:
                                ob = r.json()
                                oid_val = ob.get("id")
                                if oid_val and str(oid_val) not in seen_ids:
                                    seen_ids.add(str(oid_val))
                                    all_flat.append(ob)
                        except Exception:
                            pass
                    off += len(raw)
                    if len(raw) < page_size:
                        break
                    continue

                for o in _flatten_raw(raw):
                    oid_val = o.get("id")
                    if oid_val and str(oid_val) not in seen_ids:
                        seen_ids.add(str(oid_val))
                        all_flat.append(o)
                off += len(raw)
                if len(raw) < page_size:
                    break
            except Exception as ex:
                log.debug("ML orders %s: %s", url.split("/")[-1], ex)
                break

        if len(all_flat) >= limit:
            break

    if all_flat:
        faltan_items = [o for o in all_flat[:limit] if not (o.get("order_items") or o.get("items")) and o.get("id")]
        fetches = 0
        max_enrich = min(2000, len(faltan_items))
        for o in faltan_items[:max_enrich]:
            if fetches >= 1000:
                break
            try:
                r = requests.get(f"https://api.mercadolibre.com/orders/{o['id']}", headers=headers, timeout=10)
                if r.status_code == 200:
                    full = r.json()
                    idx = next((i for i, x in enumerate(all_flat) if x.get("id") == o["id"]), -1)
                    if idx >= 0 and (full.get("order_items") or full.get("items")):
                        all_flat[idx] = full
                        fetches += 1
            except Exception:
                pass
        log.debug("ML orders: %d ordenes total", len(all_flat))
        return {"results": all_flat[:limit], "paging": {"total": len(all_flat)}}

    return {"results": [], "paging": {"total": 0}, "error": "No se pudo obtener ordenes"}


def ml_get_shipments_today(access_token: str, shipping_ids: list) -> Dict[str, int]:
    """Obtiene logistic_type de cada shipment_id dado via GET /shipments/{id}.
    Retorna {"flex": N, "me": N}."""
    flex_count = 0
    me_count = 0
    headers = {"Authorization": f"Bearer {access_token}"}
    for ship_id in shipping_ids:
        if not ship_id:
            continue
        try:
            resp = requests.get(
                f"https://api.mercadolibre.com/shipments/{ship_id}",
                headers=headers,
                timeout=10,
            )
            if resp.status_code != 200:
                continue
            data = resp.json()
            lt = str(data.get("logistic_type") or "").lower()
            if lt == "self_service":
                flex_count += 1
            elif lt in ("fulfillment", "xd_drop_off", "drop_off", "cross_docking", "me2"):
                me_count += 1
        except Exception:
            continue
    return {"flex": flex_count, "me": me_count}


def ml_search_similar(
    query: str, limit: int = 20, access_token: Optional[str] = None, solo_propias: bool = False
) -> Dict[str, Any]:
    """Busca publicaciones en /sites/MLA/search (listados con precio, vendedor, stock).
    Solo devuelve resultados cuando hay datos completos. No usa catálogo (sin precio/vendedor)."""
    base = "https://api.mercadolibre.com"
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
        "Referer": "https://www.mercadolibre.com.ar/",
    }
    if access_token:
        headers["Authorization"] = f"Bearer {access_token}"

    search_params: Dict[str, Any] = {"q": query[:200], "limit": limit}

    for try_headers in (
        {**headers} if access_token else {},
        {"Accept": "application/json", "User-Agent": headers.get("User-Agent", "Mozilla/5.0")},
    ):
        if not try_headers:
            continue
        try:
            resp = requests.get(
                f"{base}/sites/MLA/search",
                params=search_params,
                headers=try_headers,
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results", [])
            if solo_propias:
                results = [r for r in results if isinstance(r, dict) and r.get("catalog_listing") is not True]
            return {"results": results, "paging": data.get("paging", {})}
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code in (401, 403):
                continue
            raise
        except Exception:
            continue

    # Fallback: usar catálogo (products/search) - trae nombre y enlace
    if access_token:
        last_403_msg = None
        for params in (
            {"site_id": "MLA", "status": "active", "q": query[:200], "limit": limit},
            {"site_id": "MLA", "q": query[:200], "limit": limit},
        ):
            try:
                prod_resp = requests.get(
                    f"{base}/products/search",
                    params=params,
                    headers=headers,
                    timeout=15,
                )
                prod_resp.raise_for_status()
                prod_data = prod_resp.json()
                raw = prod_data.get("results", [])
                results = []
                for r in raw:
                    if not isinstance(r, dict):
                        continue
                    row = dict(r)
                    if "name" in row and "title" not in row:
                        row["title"] = row["name"]
                    row["catalog_listing"] = True
                    row["permalink"] = f"https://www.mercadolibre.com.ar/p/{row.get('id', '')}"
                    results.append(row)
                return {"results": results, "paging": prod_data.get("paging", {}), "from_catalog": True}
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 403:
                    last_403_msg = (
                        "MercadoLibre bloqueo el acceso (403). Revisa: "
                        "IP en DevCenter, scopes de la app, y que no este bloqueada. "
                        "Mas info: developers.mercadolibre.com.ar/es_ar/error-403"
                    )
                continue
            except Exception:
                continue
        return {"results": [], "paging": {"total": 0}, "error": last_403_msg or "No se pudo conectar con el catalogo de MercadoLibre."}

    return {"results": [], "paging": {"total": 0}, "error": "Vincular la cuenta en Configuracion para buscar."}
