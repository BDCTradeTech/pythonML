"""
resync_sku_catalogos.py
Corrige sku_catalogos.catalog_product_id contra el estado VIGENTE de cada
publicación y setea estado_publicacion según status+stock reales. NO TOCA
'activo' (eso sigue siendo resorte de la UI/usuario). Fetch propio con scroll +
guardarrail de truncamiento -- no reusa ml_get_my_items (esa función no expone
si el scroll se cortó a mitad de camino, y su paging.total queda mal calculado
cuando se piden varios grupos de status/sub_status).

Cron: 5 3 * * * /opt/pythonml/venv/bin/python3 /opt/pythonml/resync_sku_catalogos.py
(debe correr ANTES que competidores_snapshot.py, 0 4 * * *)

Uso manual: cd /opt/pythonml && set -a && . ./.env && set +a && ./venv/bin/python3 resync_sku_catalogos.py
"""
import json
import logging
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from dotenv import load_dotenv
load_dotenv(BASE_DIR / ".env")

import requests
from db import get_connection, init_cron_runs_db, log_cron_run
from ml_api import get_ml_access_token

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

ML_API = "https://api.mercadolibre.com"
PARAM_SETS = [
    {"status": "active"},
    {"status": "paused"},
    {"status": "closed"},
    {"sub_status": "pending_documentation"},
    {"sub_status": "held"},
]
# Tolerancia para items que el /items/search devuelve pero el multiget no puede
# resolver (code != 200) -- confirmado en diagnóstico: 0 casos reales sobre
# ~11.000 items de los 3 usuarios actuales, pero un handful puntual (ítem
# borrado justo entre el search y el multiget, etc.) no debería abortar todo
# el resync del usuario. Más que esto sí es señal de un problema real.
MULTIGET_TOLERANCIA_ABS = 5
MULTIGET_TOLERANCIA_PCT = 0.01


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _fetch_all_items_or_abort(token: str, seller_id: str, user_id: int):
    """Devuelve (items_completos, None) o (None, motivo_abort) -- nunca una lista
    parcial disfrazada de completa. Guardarrail: scroll sin errores Y el total
    recolectado coincide con paging.total reportado por grupo; el multiget de
    detalle tolera un puñado de items inaccesibles (ver constantes arriba)."""
    all_ids = []
    for extra in PARAM_SETS:
        ids, scroll_id, reported_total, truncated = [], None, None, False
        while True:
            params = {"search_type": "scan", "limit": 100, **extra}
            if scroll_id:
                params["scroll_id"] = scroll_id
            try:
                r = requests.get(
                    f"{ML_API}/users/{seller_id}/items/search",
                    headers={"Authorization": f"Bearer {token}"},
                    params=params, timeout=15,
                )
                r.raise_for_status()
            except requests.exceptions.RequestException as e:
                log.error("user_id=%s grupo=%s: scroll cortado tras %d ids (%s)", user_id, extra, len(ids), e)
                truncated = True
                break
            data = r.json()
            if reported_total is None:
                reported_total = data.get("paging", {}).get("total")
            chunk = data.get("results", [])
            if not chunk:
                break
            ids.extend(chunk)
            scroll_id = data.get("scroll_id")
            if not scroll_id:
                break
            time.sleep(0.1)
        if truncated:
            return None, f"scroll_truncado grupo={extra}"
        if reported_total is not None and len(ids) != reported_total:
            return None, f"conteo_no_coincide grupo={extra} reportado={reported_total} recolectado={len(ids)}"
        all_ids.extend(ids)

    # Multiget de detalle (sin restringir attributes -> trae catalog_product_id completo)
    items = []
    for i in range(0, len(all_ids), 20):
        batch = all_ids[i:i + 20]
        try:
            r = requests.get(
                f"{ML_API}/items", params={"ids": ",".join(batch)},
                headers={"Authorization": f"Bearer {token}"}, timeout=30,
            )
            r.raise_for_status()
        except requests.exceptions.RequestException as e:
            return None, f"multiget_fallo batch_inicio={i} ({e})"
        for entry in r.json():
            if entry.get("code") == 200:
                items.append(entry["body"])
        time.sleep(0.1)

    missing = len(all_ids) - len(items)
    if missing:
        tolerancia = max(MULTIGET_TOLERANCIA_ABS, int(len(all_ids) * MULTIGET_TOLERANCIA_PCT))
        if missing > tolerancia:
            return None, f"multiget_incompleto esperado={len(all_ids)} obtenido={len(items)} (excede tolerancia={tolerancia})"
        log.warning(
            "user_id=%s: %d/%d items inaccesibles en multiget (code != 200), tolerado (<= %d) -- se continua sin ellos",
            user_id, missing, len(all_ids), tolerancia,
        )

    return items, None


def _get_seller_sku(item: dict) -> str:
    for attr in item.get("attributes") or []:
        if attr.get("id") == "SELLER_SKU":
            return (attr.get("value_name") or "").strip()
    return ""


def _resync_user(user_id: int, seller_id: str) -> dict:
    counters = {
        "corregidos": 0, "confirmados_sin_cambio": 0, "pausada_o_sin_stock": 0,
        "finalizada": 0, "obsoletos_superseded": 0, "sku_no_encontrado": 0,
        "sin_catalogo_actual": 0, "ambiguo_revisar_manual": 0,
    }
    token = get_ml_access_token(user_id)
    if not token:
        return {"error": "sin_token"}

    items, abort_reason = _fetch_all_items_or_abort(token, seller_id, user_id)
    if abort_reason:
        return {"error": abort_reason}

    log.info("user_id=%s: %d publicaciones vivas leídas (completo, sin truncar)", user_id, len(items))

    items_by_sku: dict[str, list] = {}
    for it in items:
        sku = _get_seller_sku(it)
        if sku:
            items_by_sku.setdefault(sku, []).append(it)

    conn = get_connection()
    existing = conn.execute(
        "SELECT id, sku, catalog_product_id, agregado_at FROM sku_catalogos WHERE user_id=?",
        (user_id,),
    ).fetchall()
    rows_by_sku: dict[str, list] = {}
    for r in existing:
        rows_by_sku.setdefault(r["sku"], []).append(dict(r))

    for sku, rows in rows_by_sku.items():
        sku_items = items_by_sku.get(sku)

        if not sku_items:
            for r in rows:
                conn.execute(
                    "UPDATE sku_catalogos SET estado_publicacion='sku_no_encontrado', last_resync_at=? WHERE id=?",
                    (_now(), r["id"]),
                )
            counters["sku_no_encontrado"] += len(rows)
            continue

        live_cpids = {str(it.get("catalog_product_id") or "").strip() for it in sku_items if it.get("catalog_product_id")}

        if not live_cpids:
            for r in rows:
                conn.execute(
                    "UPDATE sku_catalogos SET estado_publicacion='sin_catalogo_actual', last_resync_at=? WHERE id=?",
                    (_now(), r["id"]),
                )
            counters["sin_catalogo_actual"] += len(rows)
            continue

        if len(live_cpids) > 1:
            log.warning("user_id=%s sku=%s: AMBIGUO, catalog_product_id vigentes=%s -- no se auto-corrige", user_id, sku, live_cpids)
            for r in rows:
                conn.execute(
                    "UPDATE sku_catalogos SET estado_publicacion='ambiguo_revisar_manual', last_resync_at=? WHERE id=?",
                    (_now(), r["id"]),
                )
            counters["ambiguo_revisar_manual"] += len(rows)
            continue

        new_cpid = next(iter(live_cpids))
        relevant = [it for it in sku_items if str(it.get("catalog_product_id") or "").strip() == new_cpid]
        if any(it.get("status") == "active" and (it.get("available_quantity") or 0) > 0 for it in relevant):
            estado = "activo_con_stock"
        elif any(it.get("status") == "active" for it in relevant):
            estado = "sin_stock"
        elif any(it.get("status") == "paused" for it in relevant):
            estado = "pausada"
        else:
            estado = "finalizada"

        target = next((r for r in rows if r["catalog_product_id"] == new_cpid), None)
        if target is not None:
            conn.execute(
                "UPDATE sku_catalogos SET estado_publicacion=?, last_resync_at=? WHERE id=?",
                (estado, _now(), target["id"]),
            )
            others = [r for r in rows if r["id"] != target["id"]]
        else:
            main = sorted(rows, key=lambda r: r["agregado_at"] or "", reverse=True)[0]
            conn.execute(
                "UPDATE sku_catalogos SET catalog_product_id=?, estado_publicacion=?, last_resync_at=? WHERE id=?",
                (new_cpid, estado, _now(), main["id"]),
            )
            others = [r for r in rows if r["id"] != main["id"]]
            counters["corregidos"] += 1

        if target is not None:
            counters["confirmados_sin_cambio"] += 1
        if estado in ("pausada", "sin_stock"):
            counters["pausada_o_sin_stock"] += 1
        elif estado == "finalizada":
            counters["finalizada"] += 1

        for r in others:
            conn.execute(
                "UPDATE sku_catalogos SET estado_publicacion='mapeo_obsoleto_superseded', last_resync_at=? WHERE id=?",
                (_now(), r["id"]),
            )
        counters["obsoletos_superseded"] += len(others)

    conn.commit()
    conn.close()
    return counters


def run():
    init_cron_runs_db()
    log.info("=== Resync sku_catalogos %s ===", date.today().isoformat())
    conn = get_connection()
    creds = conn.execute(
        "SELECT DISTINCT c.user_id, c.raw_data FROM ml_credentials c "
        "JOIN sku_catalogos s ON s.user_id = c.user_id"
    ).fetchall()
    conn.close()

    grand_total = {}
    for user_id, raw_data in creds:
        seller_id = ""
        try:
            seller_id = str(json.loads(raw_data or "{}").get("user_id") or "")
        except Exception as e:
            log.error("user_id=%s: no se pudo leer seller_id de raw_data (%s) -- se saltea", user_id, e)
            log_cron_run("resync_sku_catalogos", user_id, "fail", 0, 0, f"raw_data invalido: {e}")
            continue
        if not seller_id:
            log_cron_run("resync_sku_catalogos", user_id, "fail", 0, 0, "sin seller_id")
            continue

        t0 = time.time()
        result = _resync_user(user_id, seller_id)
        if "error" in result:
            log.error("user_id=%s: ABORTADO sin tocar nada -- %s", user_id, result["error"])
            log_cron_run("resync_sku_catalogos", user_id, "fail", 0, time.time() - t0, result["error"])
            continue

        for k, v in result.items():
            grand_total[k] = grand_total.get(k, 0) + v
        log.info("user_id=%s: %s", user_id, result)
        log_cron_run("resync_sku_catalogos", user_id, "ok", sum(result.values()), time.time() - t0, None)
        time.sleep(2)

    log.info("=== TOTAL: %s ===", grand_total)


if __name__ == "__main__":
    run()
