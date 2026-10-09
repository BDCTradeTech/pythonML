"""
home_data.py
Datos de la pestaña Home. Cero llamadas a ML: todo sale de la DB (ml_orders_cache, ventas_datos, productos,
salud_item_snapshots, cron_runs) y del JSON "home_snapshot" que deja el cron home_refresh.py en cotizador_datos.
Sin dependencias de UI: se puede medir con un script. Todo por user_id.
"""
from __future__ import annotations

import calendar
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from db import get_connection, get_orders_cache
from home_refresh import leer_snapshot
from sales_core import ART, es_venta, fecha_venta, monto_venta, unidades_venta

ROJO, NARANJA, VERDE = 0, 1, 2  # orden de la lista: rojos, naranjas, verdes
MAX_ITEMS = 6

# Crons nocturnos esperados: (job en cron_runs, nombre, hora, minuto, gracia en minutos para considerarlo vencido).
CRONS_ESPERADOS = [
    ("stock", "Stock", 3, 0, 30),
    ("resync_sku_catalogos", "Resync de SKUs", 3, 5, 30),
    ("competidores", "Competidores", 4, 0, 45),
    ("ordenes_cache", "Órdenes", 4, 30, 20),
    ("salud_audit", "Salud", 5, 30, 120),
    ("ads", "Publicidad", 10, 30, 30),
]
# Reputación: límites de ML (los mismos de la tarjeta de Estadísticas).
_LIM_REP = {"claims": 0.01, "mediations": 0.005, "cancellations": 0.005, "delayed_handling_time": 0.08}
_NOM_REP = {"claims": "reclamos", "mediations": "mediaciones", "cancellations": "cancelaciones",
            "delayed_handling_time": "demora de envíos"}


def _dt_art(o: Dict[str, Any]) -> Optional[datetime]:
    s = o.get("date_created")
    if not s or not isinstance(s, str):
        return None
    try:
        dt = datetime.fromisoformat(s.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt.astimezone(ART) if dt.tzinfo else dt


def _it(o: Dict[str, Any]):
    for it in (o.get("order_items") or []):
        if isinstance(it, dict):
            yield it


def _sku_de(it: Dict[str, Any]) -> str:
    obj = it.get("item") or it
    return str(obj.get("seller_sku") or obj.get("seller_custom_field") or "").strip().upper()


def _tasa(m: Dict[str, Any], completadas: float) -> Optional[float]:
    exc = m.get("excluded") or {}
    if isinstance(exc.get("real_rate"), (int, float)):
        return float(exc["real_rate"])
    if isinstance(exc.get("real_value"), (int, float)) and completadas > 0:
        return exc["real_value"] / completadas
    if isinstance(m.get("rate"), (int, float)):
        return float(m["rate"])
    if isinstance(m.get("value"), (int, float)) and completadas > 0:
        return m["value"] / completadas
    return None


def _usuario(user_id: int) -> Dict[str, Any]:
    conn = get_connection()
    try:
        r = conn.execute("SELECT nombre, username, ml_nombre_fantasia, ml_razon_social FROM users WHERE id=?", (user_id,)).fetchone()
        tiene_ml = conn.execute("SELECT 1 FROM ml_credentials WHERE user_id=? LIMIT 1", (user_id,)).fetchone() is not None
    finally:
        conn.close()
    if not r:
        return {"nombre": "", "tienda": "", "tiene_ml": tiene_ml}
    nombre = (r["nombre"] or "").strip() or (r["username"] or "").split("@")[0]
    tienda = (r["ml_nombre_fantasia"] or r["ml_razon_social"] or "").strip()
    return {"nombre": nombre, "tienda": tienda, "tiene_ml": tiene_ml}


def _ventas_hoy_mes(ordenes: List[Dict[str, Any]], user_id: int, ahora: datetime) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """(hoy, mes). hoy incluye lo vendido AYER hasta esta misma hora."""
    from tabs.estadisticas import _margen_por_mes, _MESES_ES
    hoy, ayer = ahora.date(), ahora.date() - timedelta(days=1)
    h_u = h_m = a_u = a_m = 0
    cur = hoy.strftime("%Y-%m")
    mes_ord: List[Dict[str, Any]] = []
    m_u, m_m = 0, 0.0
    for o in ordenes:
        if not es_venta(o):
            continue
        f = fecha_venta(o)
        if f is None:
            continue
        if f == hoy:
            h_u += unidades_venta(o)
            h_m += monto_venta(o)
        elif f == ayer:
            d = _dt_art(o)
            if d is not None and d.time() <= ahora.time():
                a_u += unidades_venta(o)
                a_m += monto_venta(o)
        if f.strftime("%Y-%m") == cur:
            mes_ord.append(o)
            m_u += unidades_venta(o)
            m_m += monto_venta(o)
    mg = _margen_por_mes(user_id, mes_ord, [cur]).get(cur) or {}
    dias_m = calendar.monthrange(hoy.year, hoy.month)[1]
    est = m_m / hoy.day * dias_m  # igual que la tarjeta "Ventas del mes" de Estadísticas (hoy incluido)
    return (
        {"u": h_u, "monto": h_m, "u_ayer": a_u, "monto_ayer": a_m},
        {"nombre": _MESES_ES[hoy.month - 1].upper(), "monto": m_m, "unidades": m_u, "pct": mg.get("pct"),
         "falta": mg.get("falta", 0), "ordenes": mg.get("ordenes", 0), "est": est},
    )


def _alerta_stock(user_id: int, ordenes: List[Dict[str, Any]], hoy: date) -> Optional[Dict[str, Any]]:
    """SKUs con stock para menos de 7 días: productos.stock / (unidades vendidas en los últimos 30 días completos / 30)."""
    d0, d1 = hoy - timedelta(days=30), hoy - timedelta(days=1)
    vend: Dict[str, int] = {}
    for o in ordenes:
        if not es_venta(o):
            continue
        f = fecha_venta(o)
        if f is None or not (d0 <= f <= d1):
            continue
        for it in _it(o):
            sku = _sku_de(it)
            if sku:
                vend[sku] = vend.get(sku, 0) + int(it.get("quantity") or 0)
    conn = get_connection()
    try:
        rows = conn.execute("SELECT sku, nombre, stock, updated_at FROM productos WHERE user_id=? AND stock IS NOT NULL", (user_id,)).fetchall()
    finally:
        conn.close()
    if not rows:
        return None
    criticos: List[Tuple[float, str, int]] = []
    for r in rows:
        u = vend.get(str(r["sku"]).strip().upper(), 0)
        if u <= 0:
            continue
        dias = max(0, int(r["stock"])) / (u / 30.0)
        if dias < 7:
            criticos.append((dias, r["nombre"] or r["sku"], int(r["stock"])))
    ts = None
    try:
        ult = max((r["updated_at"] for r in rows if r["updated_at"]), default=None)
        ts = datetime.fromisoformat(ult).replace(tzinfo=ART).timestamp() if ult else None
    except Exception:
        pass
    if not criticos:
        return {"nivel": VERDE, "titulo": "Stock sin urgencias", "detalle": "Ningún SKU con ventas queda con menos de 7 días", "destino": "stock", "ts": ts}
    criticos.sort()
    agot = sum(1 for c in criticos if c[2] <= 0)
    peor = criticos[0]
    det = f"{agot} agotados con ventas · " if agot else ""
    det += f"el más justo: {str(peor[1])[:34]} ({peor[2]} u, ~{peor[0]:.0f} d)"
    return {"nivel": ROJO if (agot or peor[0] < 3) else NARANJA, "titulo": f"{len(criticos)} SKUs con stock para menos de 7 días",
            "detalle": det, "destino": "stock", "ts": ts}


def _alerta_perdidas(user_id: int, ordenes: List[Dict[str, Any]], hoy: date) -> Optional[Dict[str, Any]]:
    """Ventas de ayer con ganancia real negativa (ventas_datos.gan_pesos sumada por orden). Las órdenes sin ganancia
    cargada (ventas_datos se llena desde Ventas / ventas_backfill) no entran y se avisan aparte."""
    ayer = hoy - timedelta(days=1)
    de_ayer = [o for o in ordenes if es_venta(o) and fecha_venta(o) == ayer]
    if not de_ayer:
        return None
    ids = [str(o.get("order_id") or o.get("id") or "") for o in de_ayer]
    conn = get_connection()
    try:
        gan: Dict[str, float] = {}
        tiene: set = set()
        fetched = None
        for i in range(0, len(ids), 500):
            lote = ids[i:i + 500]
            for r in conn.execute(
                f"SELECT order_id, gan_pesos, fetched_at FROM ventas_datos WHERE user_id=? AND order_id IN ({','.join('?' * len(lote))})",
                [user_id] + lote,
            ):
                if r["gan_pesos"] is not None:
                    gan[str(r["order_id"])] = gan.get(str(r["order_id"]), 0.0) + float(r["gan_pesos"])
                    tiene.add(str(r["order_id"]))
                    if r["fetched_at"] and (fetched is None or r["fetched_at"] > fetched):
                        fetched = r["fetched_at"]
    finally:
        conn.close()
    neg = [(oid, g) for oid, g in gan.items() if g < 0]
    sin = len(ids) - len(tiene)
    ts = None
    try:
        ts = datetime.fromisoformat(fetched).replace(tzinfo=ART).timestamp() if fetched else None
    except Exception:
        pass
    fact = sum(monto_venta(o) for o in de_ayer) or 1.0
    nota = f" · {sin} sin ganancia cargada" if sin else ""
    if not neg:
        return {"nivel": VERDE, "titulo": "Sin ventas a pérdida ayer", "detalle": f"{len(tiene)} de {len(ids)} órdenes con ganancia{nota}",
                "destino": "ventas", "ts": ts}
    perdida = -sum(g for _, g in neg)
    from tabs.estadisticas import _abrev_pesos
    return {"nivel": ROJO if perdida / fact > 0.05 else NARANJA, "titulo": f"{len(neg)} ventas a pérdida ayer",
            "detalle": f"−{_abrev_pesos(perdida)} en total sobre {len(ids)} órdenes{nota}", "destino": "ventas", "ts": ts}


def _alerta_caja_abierta(user_id: int) -> Optional[Dict[str, Any]]:
    """Publicaciones activas cuyo SKU/título dicen caja abierta pero ITEM_CONDITION es Nuevo o falta (aviso ⚠️ de Salud)."""
    from salud_audit import clasificar_estado
    conn = get_connection()
    try:
        fecha = conn.execute("SELECT MAX(snapshot_date) FROM salud_item_snapshots WHERE user_id=?", (user_id,)).fetchone()[0]
        if not fecha:
            return None
        rows = conn.execute(
            "SELECT sku, condicion, item_condition, texto_cabierta, created_at FROM salud_item_snapshots "
            "WHERE user_id=? AND snapshot_date=? AND status='active'", (user_id, fecha)).fetchall()
    finally:
        conn.close()
    n, ts = 0, None
    ej = ""
    for r in rows:
        if clasificar_estado(r["condicion"], r["item_condition"], r["sku"], r["texto_cabierta"])["aviso"]:
            n += 1
            ej = ej or str(r["sku"] or "")
        if r["created_at"] and (ts is None or r["created_at"] > ts):
            ts = r["created_at"]
    try:
        ts_f = datetime.fromisoformat(str(ts)).replace(tzinfo=ART).timestamp() if ts else None
    except Exception:
        ts_f = None
    if not n:
        return {"nivel": VERDE, "titulo": "Caja abierta bien cargada", "detalle": "Sin avisos ⚠️ en Salud", "destino": "salud", "ts": ts_f}
    return {"nivel": NARANJA, "titulo": f"{n} publicaciones de caja abierta con aviso ⚠️",
            "detalle": f"ITEM_CONDITION figura Nuevo o falta (p. ej. {ej})", "destino": "salud", "ts": ts_f}


def _alertas_crons(user_id: int, ahora: datetime) -> List[Dict[str, Any]]:
    ref = ahora.date() if ahora.hour >= 3 else ahora.date() - timedelta(days=1)
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT job, run_date, status, error, run_datetime FROM cron_runs WHERE user_id=? AND run_date>=?",
            (user_id, (ref - timedelta(days=7)).isoformat())).fetchall()
        ult_home = conn.execute("SELECT MAX(run_datetime) FROM cron_runs WHERE user_id=? AND job='home_refresh'", (user_id,)).fetchone()[0]
    finally:
        conn.close()
    hist = {r["job"] for r in rows}
    hoy_rows = {r["job"]: r for r in rows if r["run_date"] == ref.isoformat()}
    ok, fallo, no_corrio, parcial = [], [], [], []
    for job, nombre, hh, mm, gracia in CRONS_ESPERADOS:
        if job not in hist:
            continue  # este usuario nunca tuvo corridas de ese job: no aplica
        vence = datetime.combine(ref, datetime.min.time(), tzinfo=ART).replace(hour=hh, minute=mm) + timedelta(minutes=gracia)
        if ahora < vence:
            continue
        r = hoy_rows.get(job)
        if r is None:
            no_corrio.append(nombre)
        elif r["status"] == "fail":
            fallo.append((nombre, r["error"]))
        elif r["status"] == "partial":
            parcial.append(nombre)
        else:
            ok.append(nombre)
    out: List[Dict[str, Any]] = []
    if fallo:
        out.append({"nivel": ROJO, "titulo": "Falló: " + ", ".join(n for n, _ in fallo),
                    "detalle": (fallo[0][1] or "ver Log")[:90], "destino": "log", "ts": None})
    elif no_corrio or parcial:
        partes = (["no corrió " + ", ".join(no_corrio)] if no_corrio else []) + (["parcial " + ", ".join(parcial)] if parcial else [])
        out.append({"nivel": NARANJA, "titulo": "Crons de anoche: " + " · ".join(partes), "detalle": "Revisar el Log", "destino": "log", "ts": None})
    elif ok:
        out.append({"nivel": VERDE, "titulo": "Crons de anoche OK", "detalle": f"{len(ok)} de {len(ok)} corrieron bien", "destino": "log", "ts": None})
    # El propio refresco de la Home (cada 5 min, 7 a 23 h): si se cortó, los números de arriba están viejos.
    if ult_home and 7 <= ahora.hour < 23:
        try:
            edad = (ahora - datetime.fromisoformat(ult_home).replace(tzinfo=ART)).total_seconds() / 60
        except Exception:
            edad = 0
        if edad > 20:
            out.append({"nivel": NARANJA, "titulo": "El refresco de datos de la Home no corre",
                        "detalle": f"Última corrida de home_refresh hace {int(edad)} min", "destino": "log", "ts": None})
    return out


def _reputacion(snap: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """(dato para la tarjeta/alerta, alerta). Usa el seller_reputation guardado por home_refresh."""
    sec = snap.get("reputacion") or {}
    rep = sec.get("reputation") or {}
    if not rep:
        return None, None
    from tabs.estadisticas import _REP_NIVELES
    lvl = str(rep.get("level_id") or "")
    nom = next((n[1] for n in _REP_NIVELES if n[0] == lvl), lvl or "—")
    metrics = rep.get("metrics", {}) or {}
    comp = (metrics.get("sales", {}) or {}).get("completed") or 0
    peor: Optional[Tuple[float, str, float]] = None
    for k, lim in _LIM_REP.items():
        t = _tasa(metrics.get(k, {}) or {}, float(comp or 0))
        if t is not None and (peor is None or t / lim > peor[0] / _LIM_REP[peor[1]]):
            peor = (t, k, lim)
    det = f"Nivel {nom.lower()}"
    if peor:
        det += f" · {_NOM_REP[peor[1]]} {peor[0] * 100:.2f}% (máx {peor[2] * 100:g}%)".replace(".", ",")
    nivel = VERDE if lvl in ("4_light_green", "5_green") else (NARANJA if lvl == "3_yellow" else (ROJO if lvl in ("1_red", "2_orange") else None))
    alerta = None
    if nivel is not None:
        titulo = f"Reputación {nom.lower()}"
        if nivel == VERDE and peor and peor[0] / peor[2] >= 0.8:
            nivel, titulo = NARANJA, titulo + ", cerca del límite"
        alerta = {"nivel": nivel, "titulo": titulo, "detalle": det, "destino": "estadisticas", "ts": sec.get("ts")}
    return {"nivel": lvl, "nombre": nom}, alerta


def cargar_home(user_id: int, ahora: Optional[datetime] = None) -> Dict[str, Any]:
    """Todo lo que pinta la Home para user_id. Devuelve también 'tiempos' (segundos por bloque)."""
    t = {}
    t0 = time.perf_counter()
    ahora = ahora or datetime.now(ART)
    hoy = ahora.date()
    usr = _usuario(user_id)
    snap = leer_snapshot(user_id) if usr["tiene_ml"] else {}
    t["usuario+snapshot"] = time.perf_counter() - t0
    t1 = time.perf_counter()
    desde = (hoy - timedelta(days=31)).isoformat()  # cubre el mes en curso y los 30 días completos del stock
    ordenes = get_orders_cache(user_id, desde=desde) if usr["tiene_ml"] else []
    t["ordenes"] = time.perf_counter() - t1
    t1 = time.perf_counter()
    hoy_d, mes_d = _ventas_hoy_mes(ordenes, user_id, ahora) if usr["tiene_ml"] else ({}, {})
    t["ventas+mes"] = time.perf_counter() - t1

    alertas: List[Dict[str, Any]] = []
    rep_d, rep_alerta = (None, None)
    if usr["tiene_ml"]:
        for nombre, fn in (("stock", lambda: _alerta_stock(user_id, ordenes, hoy)),
                           ("perdidas", lambda: _alerta_perdidas(user_id, ordenes, hoy)),
                           ("caja_abierta", lambda: _alerta_caja_abierta(user_id)),
                           ("crons", lambda: _alertas_crons(user_id, ahora))):
            t1 = time.perf_counter()
            try:
                r = fn()
            except Exception:
                import logging
                logging.exception("[HOME] alerta %s falló (user_id=%s)", nombre, user_id)
                r = None
            if isinstance(r, list):
                alertas.extend(r)
            elif r:
                alertas.append(r)
            t[f"alerta_{nombre}"] = time.perf_counter() - t1
        rep_d, rep_alerta = _reputacion(snap)
        if rep_alerta:
            alertas.append(rep_alerta)
    problemas = [a for a in alertas if a["nivel"] != VERDE]
    if problemas:
        alertas.sort(key=lambda a: a["nivel"])
        alertas = alertas[:MAX_ITEMS]
    elif alertas or usr["tiene_ml"]:
        alertas = [{"nivel": VERDE, "titulo": "Todo en orden",
                    "detalle": "Sin alertas de stock, pérdidas, publicaciones, crons ni reputación", "destino": None, "ts": None}]
    t["total"] = time.perf_counter() - t0
    return {
        "ahora": ahora, "usuario": usr, "hoy": hoy_d, "mes": mes_d,
        "envios": snap.get("envios"), "preguntas": snap.get("preguntas"), "ts_ordenes": (snap.get("ordenes") or {}).get("ts"),
        "reputacion": rep_d, "alertas": alertas, "tiempos": t,
    }
