"""
home_refresh.py
Refresca lo que la pestaña Home necesita y que NO está en la DB, para que la Home no llame a ML al renderizar.

Por cada usuario con credenciales de ML:
  1. órdenes nuevas (ml_get_orders_incremental -> ml_orders_cache)
  2. envíos por despachar (ml_get_pending_labels: total / flex / correo)
  3. preguntas sin responder (GET /questions/search?status=unanswered, total)
  4. reputación (seller_reputation de /users/{id}) + tiempo de respuesta oficial (_tiempo_respuesta_ml, cache de 30 min)
y guarda el resultado en cotizador_datos, clave "home_snapshot" (JSON por usuario). Cada sección lleva su propia hora
("ts", epoch): si una llamada falla se conserva el valor anterior con SU hora vieja (nunca se pisa un dato bueno con 0).

Uso (cron cada 5 min, 7 a 23 h ART; el servidor está en hora ART):
    */5 7-23 * * * cd /opt/pythonml && set -a && . ./.env && set +a && ./venv/bin/python3 home_refresh.py >> /var/log/pythonml_home.log 2>&1
No se solapa: toma un flock no bloqueante; si la corrida anterior sigue, esta sale sin hacer nada.
Se registra en cron_runs con job "home_refresh" (una fila por usuario y día, se pisa en cada corrida):
status ok / partial (alguna sección falló: se conserva su valor anterior con su hora vieja) / fail (ninguna sección se pudo refrescar).
Un usuario que falla se loguea y se sigue con el siguiente.
"""
from __future__ import annotations

import json
import logging
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

log = logging.getLogger("home_refresh")

JOB = "home_refresh"
CLAVE = "home_snapshot"
LOCK_PATH = "/tmp/pythonml_home_refresh.lock"


def leer_snapshot(user_id: int) -> Dict[str, Any]:
    """JSON guardado para el usuario ({} si no hay o está corrupto)."""
    from db import get_cotizador_param
    try:
        raw = get_cotizador_param(CLAVE, user_id)
        d = json.loads(raw) if raw else {}
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _preguntas_sin_responder(token: str, seller_id: str) -> int:
    """Total de preguntas sin responder. Levanta excepción si ML falla (ml_get_unanswered_questions devuelve [] ante un
    error y no se distinguiría de 'cero preguntas')."""
    from ml_api import get_ml_session
    r = get_ml_session().get(
        "https://api.mercadolibre.com/questions/search",
        params={"seller_id": seller_id, "status": "unanswered", "limit": 1},
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"}, timeout=15,
    )
    r.raise_for_status()
    d = r.json()
    total = d.get("total")
    if total is None:
        total = (d.get("paging") or {}).get("total")
    return int(total if total is not None else len(d.get("questions") or []))


def refrescar_usuario(user_id: int, token: str, seller_id: str) -> tuple:
    """Refresca un usuario. Devuelve (status, count, error): count = secciones guardadas OK de 4."""
    from db import set_cotizador_param
    from ml_api import ml_get_orders_incremental, ml_get_pending_labels, ml_get_user_profile

    snap = leer_snapshot(user_id)
    fallas = []
    ok = 0
    ahora = time.time()

    def _seccion(nombre: str, fn) -> None:
        nonlocal ok
        t0 = time.time()
        try:
            snap[nombre] = {**fn(), "ts": time.time()}
            ok += 1
            log.info("user_id=%s %s ok (%.2fs)", user_id, nombre, time.time() - t0)
        except Exception as e:
            fallas.append(f"{nombre}: {e}")
            log.error("user_id=%s %s falló: %s", user_id, nombre, e)

    _seccion("ordenes", lambda: (ml_get_orders_incremental(token, seller_id, user_id, devolver_cache=False, raise_on_error=True), {})[1])
    _seccion("envios", lambda: ml_get_pending_labels(token, seller_id, max_age_minutes=0, raise_on_error=True))

    def _preg() -> Dict[str, Any]:
        return {"n": _preguntas_sin_responder(token, seller_id)}
    _seccion("preguntas", _preg)

    def _rep() -> Dict[str, Any]:
        prof = ml_get_user_profile(token)
        rep = (prof or {}).get("seller_reputation")
        if not rep:
            raise RuntimeError("perfil sin seller_reputation")
        out: Dict[str, Any] = {"reputation": rep}
        try:  # tiempo de respuesta oficial (tiene su propio cache de 30 min; si falla queda el último guardado)
            from tabs.estadisticas import _tiempo_respuesta_ml
            out["tiempo"] = _tiempo_respuesta_ml(user_id, token, seller_id)
        except Exception as e:
            log.warning("user_id=%s tiempo de respuesta no disponible: %s", user_id, e)
            out["tiempo"] = (snap.get("reputacion") or {}).get("tiempo")
        return out
    _seccion("reputacion", _rep)

    snap["ts"] = ahora
    set_cotizador_param(CLAVE, json.dumps(snap), user_id)
    error = "; ".join(fallas)[:500] or None
    if ok == 0:
        return "fail", ok, error  # no se pudo refrescar nada
    return ("partial" if fallas else "ok"), ok, error  # una sección caída conserva su valor y su hora vieja


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    lock = None
    try:
        import fcntl
        lock = open(LOCK_PATH, "w")
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except ImportError:
        pass  # Windows (desarrollo): sin lock
    except OSError:
        log.warning("La corrida anterior sigue en curso: se saltea esta")
        return

    import requests
    from db import get_connection, init_cron_runs_db, init_orders_cache_schema, log_cron_run
    from ml_api import get_ml_access_token

    init_cron_runs_db()
    init_orders_cache_schema()
    conn = get_connection()
    creds = conn.execute("SELECT id, user_id, raw_data FROM ml_credentials").fetchall()
    conn.close()
    vistos = set()
    for _cid, user_id, raw_data in creds:
        if user_id in vistos:
            continue
        vistos.add(user_id)
        t0 = time.time()
        status, count, error = "fail", 0, None
        try:
            token = get_ml_access_token(user_id)
            if not token:
                error = "Sin token ML"
                log.warning("Sin token para user_id=%s, salteando", user_id)
                continue
            seller_id: Optional[str] = None
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
            error = str(e)[:500]
            log.error("Error procesando user_id=%s: %s", user_id, e)
        finally:
            try:
                log_cron_run(JOB, user_id, status, count, time.time() - t0, error)
            except Exception as log_e:
                log.error("No se pudo loguear cron_runs: %s", log_e)
    if lock:
        lock.close()


if __name__ == "__main__":
    main()
