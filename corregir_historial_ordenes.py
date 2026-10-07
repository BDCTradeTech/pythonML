"""
corregir_historial_ordenes.py
Corrección ÚNICA del status de TODAS las órdenes de ml_orders_cache (no solo las últimas semanas).

Por defecto es --dry-run: abre app.db con mode=ro, consulta ML solo con GET y lista, por usuario
y mes de date_created, cuántas órdenes cambiarían de estado (con unidades e importe). No escribe nada.

Con --apply (solo con OK explícito):
  1) hace un backup del cache ANTES de tocar nada, en el droplet:
     /opt/pythonml/backups/ml_orders_cache_<usuario|todos>_<YYYYMMDD_HHMMSS>.db (copia de la tabla via ATTACH);
  2) upserta únicamente las órdenes cuyo status / pagos / date_last_updated cambiaron
     (nunca borra filas, nunca inserta órdenes que no estaban en el cache).

Uso:
    python3 corregir_historial_ordenes.py                 # dry-run, todos los usuarios
    python3 corregir_historial_ordenes.py --user 1        # dry-run de un usuario
    python3 corregir_historial_ordenes.py --apply         # escritura real

Recorre el rango por ventanas de 7 días de date_created (orders/search con order.date_created.from/to)
para no depender de offsets grandes. Sin datos personales: solo agregados.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

DB_PATH = BASE_DIR / "app.db"
VENTANA_DIAS = 7


def _unidades(items: List[Dict[str, Any]]) -> int:
    n = 0
    for it in items or []:
        try:
            n += int(it.get("quantity") or 1)
        except (TypeError, ValueError):
            n += 1
    return n or 1


def _importe(o: Dict[str, Any]) -> float:
    return float(o.get("total_amount") or o.get("paid_amount") or 0)


def _pagos_resumen(pays: List[Dict[str, Any]]) -> List[str]:
    return sorted(f"{p.get('id')}:{p.get('status')}" for p in (pays or []))


def _ventanas(d0: date, d1: date) -> List[Tuple[str, str]]:
    out = []
    cur = d0
    while cur <= d1:
        fin = min(cur + timedelta(days=VENTANA_DIAS - 1), d1)
        out.append((f"{cur.isoformat()}T00:00:00.000-03:00", f"{fin.isoformat()}T23:59:59.999-03:00"))
        cur = fin + timedelta(days=1)
    return out


def _cargar_cache_ro(user_id: int) -> Dict[str, Dict[str, Any]]:
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=15)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT order_id, date_created, total_amount, paid_amount, status, items_json, payments_json "
            "FROM ml_orders_cache WHERE user_id = ?", (user_id,)).fetchall()
    finally:
        conn.close()
    out = {}
    for r in rows:
        d = dict(r)
        d["items"] = json.loads(d.pop("items_json") or "[]")
        d["pays"] = json.loads(d.pop("payments_json") or "[]")
        out[str(d["order_id"])] = d
    return out


def _backup(user_id: Optional[int]) -> Path:
    bdir = BASE_DIR / "backups"
    bdir.mkdir(exist_ok=True)
    destino = bdir / f"ml_orders_cache_{user_id if user_id else 'todos'}_{datetime.now():%Y%m%d_%H%M%S}.db"
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=30)
    try:
        conn.execute("ATTACH DATABASE ? AS bak", (str(destino),))
        # CREATE TABLE ... AS no copia la PK; alcanza para restaurar (es una copia de datos).
        sql = "CREATE TABLE bak.ml_orders_cache AS SELECT * FROM main.ml_orders_cache"
        params: Tuple = ()
        if user_id:
            sql += " WHERE user_id = ?"
            params = (user_id,)
        conn.execute(sql, params)
        n = conn.execute("SELECT COUNT(*) FROM bak.ml_orders_cache").fetchone()[0]
        conn.commit()
    finally:
        conn.close()
    print(f"Backup: {destino} ({n} filas)")
    return destino


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="escribe (default: dry-run)")
    ap.add_argument("--user", type=int, default=None)
    args = ap.parse_args()

    from ordenes_cache_refresh import buscar_ordenes
    from ml_api import get_ml_access_token

    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=15)
    creds = conn.execute("SELECT user_id, raw_data FROM ml_credentials").fetchall()
    conn.close()
    print(f"MODO: {'APPLY (escribe)' if args.apply else 'DRY-RUN (no escribe)'}")

    plan_por_usuario: Dict[int, List[Dict[str, Any]]] = {}
    for uid, raw in creds:
        if args.user and uid != args.user:
            continue
        cache = _cargar_cache_ro(uid)
        if not cache:
            continue
        seller_id = str((json.loads(raw) if raw else {}).get("user_id") or "")
        token = get_ml_access_token(uid)
        if not token or not seller_id:
            print(f"user {uid}: sin token o seller_id, salteado")
            continue
        fechas = sorted(v["date_created"][:10] for v in cache.values() if v.get("date_created"))
        d0, d1 = date.fromisoformat(fechas[0]), date.today()
        print(f"\n=== user {uid}: {len(cache)} órdenes en cache, {d0} → {d1} ===", flush=True)

        vistas: Dict[str, Dict[str, Any]] = {}
        truncados = 0
        for desde, hasta in _ventanas(d0 - timedelta(days=1), d1):
            ords, trunc = buscar_ordenes(token, seller_id, {"order.date_created.from": desde,
                                                             "order.date_created.to": hasta})
            truncados += 1 if trunc else 0
            for o in ords:
                vistas[str(o.get("id"))] = o
            time.sleep(0.2)

        cambios: List[Dict[str, Any]] = []
        for oid, o in vistas.items():
            c = cache.get(oid)
            if c is None:
                continue
            st_api, st_cache = o.get("status") or "", c.get("status") or ""
            pagos_cambian = _pagos_resumen(o.get("payments")) != _pagos_resumen(c["pays"])
            if st_api != st_cache or pagos_cambian:
                cambios.append({"oid": oid, "orden_api": o, "de": st_cache, "a": st_api,
                                "mes": (c.get("date_created") or "")[:7], "solo_pagos": st_api == st_cache,
                                "u": _unidades(c["items"]), "monto": _importe(c)})
        no_encontradas = [oid for oid in cache if oid not in vistas]
        plan_por_usuario[uid] = cambios

        print(f"  leídas de ML: {len(vistas)} · ventanas truncadas: {truncados} · "
              f"en cache y NO devueltas por ML: {len(no_encontradas)}")
        cambia_estado = [c for c in cambios if not c["solo_pagos"]]
        print(f"  órdenes que cambian de estado: {len(cambia_estado)} · solo cambian pagos/refunds: "
              f"{len(cambios) - len(cambia_estado)}")
        por_mes: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"n": 0, "u": 0, "m": 0.0, "t": defaultdict(int)})
        for c in cambia_estado:
            p = por_mes[c["mes"]]
            p["n"] += 1; p["u"] += c["u"]; p["m"] += c["monto"]; p["t"][f"{c['de']}->{c['a']}"] += 1
        if por_mes:
            print("  mes      órdenes  unid.      importe   transiciones")
            for mes in sorted(por_mes):
                p = por_mes[mes]
                print(f"  {mes}   {p['n']:6d} {p['u']:6d} {p['m']:14,.0f}   "
                      + ", ".join(f"{k}:{v}" for k, v in sorted(p['t'].items())))
            tn, tu, tm = (sum(p[k] for p in por_mes.values()) for k in ("n", "u", "m"))
            print(f"  TOTAL    {tn:6d} {tu:6d} {tm:14,.0f}")
        if no_encontradas:
            meses_nf: Dict[str, int] = defaultdict(int)
            for oid in no_encontradas:
                meses_nf[(cache[oid].get("date_created") or "")[:7]] += 1
            print("  no devueltas por ML, por mes: " + ", ".join(f"{m}:{n}" for m, n in sorted(meses_nf.items())))

    if not args.apply:
        print("\nDRY-RUN: no se escribió nada.")
        return

    from db import get_connection, init_orders_cache_schema, upsert_orders_cache
    init_orders_cache_schema()
    _backup(args.user)
    for uid, cambios in plan_por_usuario.items():
        if not cambios:
            continue
        upsert_orders_cache(uid, [c["orden_api"] for c in cambios])
        print(f"user {uid}: {len(cambios)} órdenes actualizadas")
    print("APPLY terminado.")


if __name__ == "__main__":
    main()
