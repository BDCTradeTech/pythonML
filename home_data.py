"""
home_data.py
Datos de la pestaña Home. Cero llamadas a ML: todo sale de la DB (ml_orders_cache, ventas_datos, productos,
salud_item_snapshots, competidores_snapshots, sku_catalogos, cron_runs) y del JSON "home_snapshot" que deja el cron home_refresh.py en cotizador_datos.
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

ROJO, NARANJA, VERDE, GRIS = 0, 1, 2, 3  # orden de la lista: rojos, naranjas, verdes, grises (sin dato)
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


def _acum_horario(ordenes: List[Dict[str, Any]], ahora: datetime, valor, v_hoy: float, v_ayer_ahora: float) -> Dict[str, Any]:
    """Acumulado por hora ART de hoy y de ayer sobre un eje X continuo en horas (0 a 24), con el criterio de sales_core.
    `valor(orden)` es lo que se acumula (unidades o monto). Cada punto cerrado es el acumulado al CIERRE de la hora (x=1 ->
    hasta 00:59 ... x=24 -> todo el día). Hoy: puntos de las horas ya cerradas + un último punto en x = hora actual fraccional
    (11:36 -> 11,6) con v_hoy. Ayer: las 24 horas + un punto en ese mismo x con v_ayer_ahora. v_hoy / v_ayer_ahora son los de la
    tarjeta VENTAS HOY (una sola fuente); así "ahora" cae sobre las dos líneas. Puntos = [x, valor]."""
    hoy, ayer = ahora.date(), ahora.date() - timedelta(days=1)
    por_h_hoy, por_h_ayer = [0] * 24, [0] * 24
    for o in ordenes:
        if not es_venta(o):
            continue
        f = fecha_venta(o)
        if f != hoy and f != ayer:
            continue
        d = _dt_art(o)
        if d is None:
            continue
        (por_h_hoy if f == hoy else por_h_ayer)[d.hour] += valor(o)
    hora = ahora.hour
    x_ahora = round(hora + (ahora.minute + ahora.second / 60) / 60, 4)
    pts_hoy: List[List[float]] = [[0, 0]]
    pts_ayer: List[List[float]] = [[0, 0]]
    a = b = 0
    for h in range(24):
        a += por_h_hoy[h]
        b += por_h_ayer[h]
        if h == hora:  # el punto "ahora" va antes del cierre de esta hora (o lo reemplaza si son las HH:00 en punto)
            if pts_ayer[-1][0] == x_ahora:
                pts_ayer[-1][1] = v_ayer_ahora
            else:
                pts_ayer.append([x_ahora, v_ayer_ahora])
        if h < hora:
            pts_hoy.append([h + 1, a])
        pts_ayer.append([h + 1, b])
    if pts_hoy[-1][0] == x_ahora:
        pts_hoy[-1][1] = v_hoy
    else:
        pts_hoy.append([x_ahora, v_hoy])
    return {"hora": hora, "hoy": pts_hoy, "ayer": pts_ayer, "ayer_total": b,
            "ahora": {"x": x_ahora, "hoy": v_hoy, "ayer": v_ayer_ahora, "hhmm": ahora.strftime("%H:%M")}}


def _serie_horaria(ordenes: List[Dict[str, Any]], ahora: datetime, hoy_d: Dict[str, Any]) -> Dict[str, Any]:
    """Las dos series de Ventas por hora: unidades (raíz del dict, como siempre) y facturación (clave "monto", monto_venta de
    sales_core). Los valores "ahora" son los de la tarjeta VENTAS HOY (hoy_d), así que los dos gráficos y la tarjeta coinciden."""
    out = _acum_horario(ordenes, ahora, unidades_venta, hoy_d["u"], hoy_d["u_ayer"])
    out["monto"] = _acum_horario(ordenes, ahora, monto_venta, hoy_d["monto"], hoy_d["monto_ayer"])
    return out


def _titulos_ventas(user_id: int, ordenes: List[Dict[str, Any]], elegidas: List[Dict[str, Any]]) -> Dict[str, str]:
    """{item_id: título a mostrar} para los ítems de las órdenes elegidas, sin llamar a ML. Mismo criterio que Top Ventas de
    Estadísticas: si el ítem es una publicación de catálogo se muestra el título de NUESTRA publicación propia (no catálogo)
    del mismo SKU (gold_special primero); si no hay, el título de la orden cortado a 60. Qué es catálogo/propia sale del
    último snapshot de Salud; los títulos, de las órdenes cacheadas (la más nueva de cada ítem)."""
    from tabs.estadisticas import _cortar_titulo
    titulo_orden: Dict[str, str] = {}
    for o in ordenes:  # vienen de la más nueva a la más vieja: gana la primera
        for it in _it(o):
            obj = it.get("item") or it
            iid, t = str(obj.get("id") or ""), str(obj.get("title") or "").strip()
            if iid and t and iid not in titulo_orden:
                titulo_orden[iid] = t
    ids: Dict[str, str] = {}
    for o in elegidas:
        for it in _it(o):
            obj = it.get("item") or it
            if obj.get("id"):
                ids[str(obj["id"])] = _sku_de(it)
    if not ids:
        return {}
    conn = get_connection()
    try:
        fecha = conn.execute("SELECT MAX(snapshot_date) FROM salud_item_snapshots WHERE user_id=?", (user_id,)).fetchone()[0]
        filas = conn.execute(
            "SELECT item_id, UPPER(TRIM(sku)) AS sku, catalog_listing, status, listing_type_id FROM salud_item_snapshots "
            "WHERE user_id=? AND snapshot_date=?", (user_id, fecha)).fetchall() if fecha else []
    finally:
        conn.close()
    por_item = {r["item_id"]: r for r in filas}
    propias_de: Dict[str, List[Any]] = {}
    for r in filas:
        if r["catalog_listing"] != 1 and r["status"] == "active" and r["sku"] and r["item_id"] in titulo_orden:
            propias_de.setdefault(r["sku"], []).append(r)
    out: Dict[str, str] = {}
    for iid, sku_orden in ids.items():
        base = titulo_orden.get(iid, "Sin nombre")
        r = por_item.get(iid)
        if r is None or r["catalog_listing"] != 1:
            out[iid] = base  # publicación propia (o sin dato en Salud): título de la orden
            continue
        cands = sorted(propias_de.get(r["sku"] or sku_orden, []),
                       key=lambda c: (0 if str(c["listing_type_id"] or "").lower() == "gold_special" else 1, c["item_id"]))
        out[iid] = titulo_orden[cands[0]["item_id"]] if cands else _cortar_titulo(base, 60)
    return out


def _ultimas_ventas(user_id: int, ordenes: List[Dict[str, Any]], ahora: datetime, n: int = 8) -> Dict[str, Any]:
    """Las últimas n órdenes de hoy (de ayer si hoy todavía no hay), de la más nueva a la más vieja."""
    hoy = ahora.date()
    ventas = [(o, fecha_venta(o)) for o in ordenes if es_venta(o)]
    dia = hoy if any(f == hoy for _, f in ventas) else hoy - timedelta(days=1)
    del_dia = [o for o, f in ventas if f == dia]
    del_dia.sort(key=lambda o: _dt_art(o) or datetime.min.replace(tzinfo=ART), reverse=True)
    sel = del_dia[:n]
    tit = _titulos_ventas(user_id, ordenes, sel)
    filas = []
    for o in sel:
        items = list(_it(o))
        obj = (items[0].get("item") or items[0]) if items else {}
        nombre = tit.get(str(obj.get("id") or ""), "Sin nombre")
        if len(items) > 1:
            nombre += f" (+{len(items) - 1} más)"
        d = _dt_art(o)
        filas.append({"hora": d.strftime("%H:%M") if d else "", "titulo": nombre, "u": unidades_venta(o), "monto": monto_venta(o),
                      "order_id": str(o.get("order_id") or o.get("id") or "")})
    return {"dia": "hoy" if dia == hoy else "ayer", "filas": filas}


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


_UMBRAL_IGUAL = 0.5   # % de diferencia dentro del cual nuestro precio y el del competidor más barato cuentan como "igual"
_UMBRAL_ROJO = 10.0   # % por encima del más barato desde el cual el punto es rojo
_HORAS_DATO_VIGENTE = 36
COMP_MIN_VENTAS = 50  # competidores con menos ventas totales (seller_total_ventas) no cuentan: precios atípicos de vendedores chicos


def _peso(v: float) -> str:
    return "$" + f"{int(round(v)):,}".replace(",", ".")


def _comparar_catalogos(user_id: int) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """Un renglón por PRODUCTO DE CATÁLOGO activo (sku_catalogos.estado_publicacion='activo_con_stock', el mismo universo del cron)
    con al menos un competidor válido (precio no NULL y al menos COMP_MIN_VENTAS ventas totales) en el último snapshot de competidores_snapshots;
    los catálogos sin competidor válido no entran:
    nuestro = precio de venta más bajo (price_vigente, con promo; si falta, price de lista) entre nuestras publicaciones activas
    del último snapshot de Salud con SKU de ese catálogo; comp = precio mínimo de los competidores. pct = nuestro / comp - 1.
    Devuelve (renglones, snapshot_date de competidores)."""
    conn = get_connection()
    try:
        f_comp = conn.execute("SELECT MAX(snapshot_date) FROM competidores_snapshots WHERE user_id=? AND price IS NOT NULL",
                              (user_id,)).fetchone()[0]
        f_salud = conn.execute("SELECT MAX(snapshot_date) FROM salud_item_snapshots WHERE user_id=?", (user_id,)).fetchone()[0]
        if not f_comp or not f_salud:
            return [], f_comp
        # Tres lecturas simples y el cruce en Python: un JOIN por UPPER(TRIM(sku)) no usa índice y tardaba más de 1 s.
        comp: Dict[str, float] = {}
        for r in conn.execute("SELECT catalog_product_id AS cpid, MIN(price) AS p FROM competidores_snapshots "
                              "WHERE user_id=? AND snapshot_date=? AND price IS NOT NULL AND price>0 AND COALESCE(seller_total_ventas, 0)>=? "
                              "GROUP BY catalog_product_id", (user_id, f_comp, COMP_MIN_VENTAS)):
            comp[r["cpid"]] = float(r["p"])
        precio_sku: Dict[str, float] = {}
        for r in conn.execute("SELECT UPPER(TRIM(sku)) AS sku, MIN(COALESCE(price_vigente, price)) AS p FROM salud_item_snapshots "
                              "WHERE user_id=? AND snapshot_date=? AND status='active' AND COALESCE(price_vigente, price)>0 "
                              "GROUP BY UPPER(TRIM(sku))", (user_id, f_salud)):
            precio_sku[r["sku"]] = float(r["p"])
        catalogo: Dict[str, Dict[str, Any]] = {}
        for r in conn.execute("SELECT catalog_product_id AS cpid, UPPER(TRIM(sku)) AS sku, sku AS sku_raw, catalog_name AS nombre "
                              "FROM sku_catalogos WHERE user_id=? AND estado_publicacion='activo_con_stock'", (user_id,)):
            c = catalogo.setdefault(r["cpid"], {"nuestro": None, "nombre": None, "sku": r["sku_raw"]})
            p = precio_sku.get(r["sku"])
            if p is not None and (c["nuestro"] is None or p < c["nuestro"]):
                c["nuestro"] = p
            c["nombre"] = c["nombre"] or r["nombre"]
    finally:
        conn.close()
    out = []
    for cpid, pc in comp.items():
        c = catalogo.get(cpid)
        if c and c["nuestro"] is not None:
            out.append({"cpid": cpid, "nombre": c["nombre"] or c["sku"] or cpid, "nuestro": c["nuestro"], "comp": pc,
                        "pct": (c["nuestro"] / pc - 1) * 100})
    return out, f_comp


def _alerta_competidores(user_id: int, ahora: datetime) -> Optional[Dict[str, Any]]:
    """COMPETIDORES: cuántos productos de catálogo nuestros están más caros que el competidor más barato (más de 0,5% arriba),
    iguales (±0,5%) o más baratos. Datos de la corrida de las 04:00 (cron_runs 'competidores' + competidores_snapshots), sin ML."""
    conn = get_connection()
    try:
        run = conn.execute("SELECT run_datetime FROM cron_runs WHERE job='competidores' AND user_id=? AND status IN ('ok','partial') "
                           "ORDER BY run_date DESC LIMIT 1", (user_id,)).fetchone()
    finally:
        conn.close()
    if not run:
        return None  # este usuario nunca tuvo corridas de competidores: no aplica
    try:
        ts_dt = datetime.fromisoformat(run["run_datetime"]).replace(tzinfo=ART)
    except Exception:
        ts_dt = None
    filas, f_comp = _comparar_catalogos(user_id)
    viejo = (ts_dt is None or (ahora - ts_dt) > timedelta(hours=_HORAS_DATO_VIGENTE) or not f_comp
             or f_comp < (ahora - timedelta(hours=_HORAS_DATO_VIGENTE)).date().isoformat())
    if viejo:
        return {"nivel": GRIS, "titulo": "Sin dato reciente de competidores",
                "detalle": "Última corrida: " + (ts_dt.strftime("%d/%m %H:%M") if ts_dt else "—"), "destino": None,
                "ts": ts_dt.timestamp() if ts_dt else None}
    if not filas:
        return None
    mas = sorted((f for f in filas if f["pct"] > _UMBRAL_IGUAL), key=lambda f: -f["pct"])
    igual = [f for f in filas if abs(f["pct"]) <= _UMBRAL_IGUAL]
    menos = [f for f in filas if f["pct"] < -_UMBRAL_IGUAL]
    nivel = ROJO if any(f["pct"] > _UMBRAL_ROJO for f in mas) else (NARANJA if mas else VERDE)
    hhmm = ts_dt.strftime("%H:%M")
    top = [f"{str(f['nombre'])[:40]} +{f['pct']:.0f}% ({_peso(f['nuestro'])} vs {_peso(f['comp'])})" for f in mas[:3]]
    pie = [f"Ignora vendedores con menos de {COMP_MIN_VENTAS} ventas", f"dato de las {hhmm}"]
    tip = "\n".join(top + pie) if top else f"Ninguno más caro\n" + "\n".join(pie)
    return {"nivel": nivel, "titulo": f"{len(mas)} de {len(filas)} productos de catálogo más caros",
            "detalle": f"{len(mas)} más caros · {len(igual)} iguales · {len(menos)} más baratos",
            "barra": {"mas": len(mas), "igual": len(igual), "menos": len(menos)}, "tip": tip,
            "destino": None, "ts": ts_dt.timestamp()}


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
    t1 = time.perf_counter()
    horaria, ultimas = None, {"dia": "hoy", "filas": []}
    if usr["tiene_ml"]:
        try:
            horaria = _serie_horaria(ordenes, ahora, hoy_d)  # los de la tarjeta: una sola fuente
            ultimas = _ultimas_ventas(user_id, ordenes, ahora)
        except Exception:
            import logging
            logging.exception("[HOME] ventas por hora / últimas ventas fallaron (user_id=%s)", user_id)
    t["horaria+ultimas"] = time.perf_counter() - t1

    alertas: List[Dict[str, Any]] = []
    rep_d, rep_alerta = (None, None)
    if usr["tiene_ml"]:
        for nombre, fn in (("stock", lambda: _alerta_stock(user_id, ordenes, hoy)),
                           ("perdidas", lambda: _alerta_perdidas(user_id, ordenes, hoy)),
                           ("competidores", lambda: _alerta_competidores(user_id, ahora)),
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
                    "detalle": "Sin alertas de stock, pérdidas, competidores, crons ni reputación", "destino": None, "ts": None}]
    t["total"] = time.perf_counter() - t0
    return {
        "ahora": ahora, "usuario": usr, "hoy": hoy_d, "mes": mes_d, "horaria": horaria, "ultimas": ultimas,
        "envios": snap.get("envios"), "preguntas": snap.get("preguntas"), "ts_ordenes": (snap.get("ordenes") or {}).get("ts"),
        "reputacion": rep_d, "alertas": alertas, "tiempos": t,
    }
