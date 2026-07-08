"""
tabs/competidores.py
Ranking global de competidores con buscador por nickname/URL/ID.
"""
from __future__ import annotations
import json, re, requests
from datetime import date, timedelta
from typing import Dict, List, Optional
from nicegui import app, run, ui
from db import get_connection

_LVL_ICON = {
    "1_green":"🟢","2_green":"🟢","3_green":"🟡",
    "4_green":"⚪","5_yellow":"🟡","6_red":"🔴",
}
_ML_API = "https://api.mercadolibre.com"


def _get_mis_seller_ids(user_id: int) -> set:
    """IDs de ML de las cuentas propias — usa user_id del OAuth token en raw_data."""
    conn = get_connection()
    rows = conn.execute("SELECT raw_data FROM ml_credentials WHERE user_id=? AND raw_data IS NOT NULL", (user_id,)).fetchall()
    conn.close()
    ids = set()
    for r in rows:
        try:
            d = json.loads(r[0]) if r[0] else {}
            # raw_data es la respuesta OAuth → tiene user_id (no id)
            sid = str(d.get("user_id") or d.get("id") or "")
            if sid:
                ids.add(sid)
        except Exception:
            pass
    return ids


def _get_seguidos(user_id: int) -> List[Dict]:
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT seller_id, seller_nickname FROM competidores_seguidos WHERE user_id=?",
            (user_id,)
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []
    finally:
        conn.close()


def _add_seguido(user_id: int, seller_id: str, nickname: str) -> bool:
    conn = get_connection()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS competidores_seguidos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                seller_id TEXT NOT NULL,
                seller_nickname TEXT,
                agregado_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, seller_id)
            )
        """)
        conn.execute(
            "INSERT OR IGNORE INTO competidores_seguidos (user_id, seller_id, seller_nickname) VALUES (?,?,?)",
            (user_id, seller_id, nickname)
        )
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        conn.close()


def _remove_seguido(user_id: int, seller_id: str):
    conn = get_connection()
    try:
        conn.execute("DELETE FROM competidores_seguidos WHERE user_id=? AND seller_id=?", (user_id, seller_id))
        conn.commit()
    finally:
        conn.close()


def _buscar_y_agregar_catalogo(catalog_id: str, user_id: int, access_token: str) -> Dict:
    """
    Trae todos los sellers de un catálogo ML y los agrega a competidores_seguidos.
    Retorna dict con resultados: total, agregados, ya_existian.
    """
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}

    # Traer todos los sellers del catálogo
    r = requests.get(
        f"{_ML_API}/products/{catalog_id}/items",
        headers=headers, timeout=15
    )
    if r.status_code != 200:
        return {"error": f"No se pudo consultar el catálogo ({r.status_code})"}

    seller_ids = list({str(it.get("seller_id","")) for it in r.json().get("results",[]) if it.get("seller_id")})

    agregados = 0
    ya_existian = 0

    conn = get_connection()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS competidores_seguidos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                seller_id TEXT NOT NULL,
                seller_nickname TEXT,
                agregado_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, seller_id)
            )
        """)
        conn.commit()
    finally:
        conn.close()

    for sid in seller_ids:
        # Verificar si ya está en snapshots o seguidos
        conn = get_connection()
        en_snap = conn.execute(
            "SELECT 1 FROM competidores_snapshots WHERE user_id=? AND seller_id=? LIMIT 1",
            (user_id, sid)
        ).fetchone()
        en_seg = conn.execute(
            "SELECT 1 FROM competidores_seguidos WHERE user_id=? AND seller_id=? LIMIT 1",
            (user_id, sid)
        ).fetchone()
        conn.close()

        if en_snap or en_seg:
            ya_existian += 1
            continue

        # Traer perfil del seller
        r2 = requests.get(f"{_ML_API}/users/{sid}", headers=headers, timeout=8)
        if r2.status_code == 200:
            d = r2.json()
            nick = d.get("nickname") or f"ID {sid}"
            _add_seguido(user_id, sid, nick)

            # Guardar snapshot inmediato con ventas históricas
            rep = d.get("seller_reputation") or {}
            txn = rep.get("transactions") or {}
            total_ventas = txn.get("total")
            if total_ventas:
                conn = get_connection()
                try:
                    conn.execute("""
                        INSERT OR IGNORE INTO competidores_snapshots
                            (user_id, catalog_product_id, seller_id, seller_nickname,
                             seller_total_ventas, seller_level_id, seller_power_status,
                             snapshot_date)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        user_id,
                        catalog_id,
                        sid,
                        nick,
                        total_ventas,
                        rep.get("level_id"),
                        rep.get("power_seller_status"),
                        date.today().isoformat(),
                    ))
                    conn.commit()
                finally:
                    conn.close()
                agregados += 1
            else:
                # Sin ventas históricas, no agregar
                pass

    return {
        "catalog_id": catalog_id,
        "total": len(seller_ids),
        "agregados": agregados,
        "ya_existian": ya_existian,
    }


def _buscar_en_db(query: str) -> Optional[Dict]:
    """Busca un vendedor por nickname en los snapshots locales."""
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT seller_id, seller_nickname, seller_level_id,
                   MAX(seller_total_ventas) as ventas
            FROM competidores_snapshots
            WHERE LOWER(seller_nickname) LIKE LOWER(?)
            GROUP BY seller_id
            ORDER BY ventas DESC NULLS LAST
            LIMIT 1
        """, (f"%{query}%",)).fetchall()
        if rows:
            r = dict(rows[0])
            return {
                "seller_id":    r["seller_id"],
                "nickname":     r["seller_nickname"] or "",
                "level_id":     r.get("seller_level_id") or "",
                "power_status": "",
                "total_ventas": r.get("ventas"),
                "registration": "",
                "fuente":       "db",
            }
    except Exception:
        pass
    finally:
        conn.close()
    return None


def _buscar_vendedor(query: str, access_token: str = "") -> Optional[Dict]:
    """Busca un vendedor por URL de ML, nickname o seller_id."""
    query = query.strip()
    if not query:
        return None

    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"} if access_token else {}

    # Extraer nickname de URL: mercadolibre.com.ar/pagina/{nickname}
    if "mercadolibre.com" in query.lower() and "/pagina/" in query.lower():
        try:
            nick = query.split("/pagina/")[1].split("#")[0].split("?")[0].strip()
            query = nick
        except Exception:
            pass

    # Extraer seller_id numérico de URL de perfil: mercadolibre.com.ar/perfil/{id}
    if "mercadolibre.com" in query.lower() and "/perfil/" in query.lower():
        try:
            sid = query.split("/perfil/")[1].split("#")[0].split("?")[0].split("/")[0].strip()
            if sid.isdigit():
                query = sid
        except Exception:
            pass

    # Extraer item_id de URL de publicación o input directo
    item_match = re.search(r'(MLA|MLB|MLM|MCO|MLC|MLU)\d{7,}', query.upper())
    if item_match:
        item_id = item_match.group(0)
        try:
            for hdrs in [headers, {}]:  # primero con token, luego sin token
                r = requests.get(
                    f"{_ML_API}/items/{item_id}",
                    params={"attributes": "seller_id,title"},
                    headers=hdrs, timeout=8
                )
                if r.status_code == 200:
                    seller_id = str(r.json().get("seller_id") or "")
                    if seller_id:
                        r2 = requests.get(f"{_ML_API}/users/{seller_id}", headers=hdrs, timeout=8)
                        if r2.status_code == 200:
                            return _parse_user(r2.json())
                    break
        except Exception:
            pass

    # 1. Buscar en DB local primero (rápido, sin restricciones de IP)
    local = _buscar_en_db(query)
    if local:
        return local

    # 2. Si es numérico, buscar por seller_id directo en API
    if query.isdigit():
        try:
            r = requests.get(f"{_ML_API}/users/{query}", headers=headers, timeout=8)
            if r.status_code == 200:
                return _parse_user(r.json())
        except Exception:
            pass
        return None

    # 3. Buscar por nickname via API (requiere token, puede fallar desde DO)
    try:
        r = requests.get(f"{_ML_API}/users/search",
                         params={"nickname": query},
                         headers=headers, timeout=8)
        if r.status_code == 200:
            results = r.json().get("results", [])
            if results:
                uid = results[0].get("id")
                r2 = requests.get(f"{_ML_API}/users/{uid}", headers=headers, timeout=8)
                if r2.status_code == 200:
                    return _parse_user(r2.json())
    except Exception:
        pass
    return None


def _parse_user(data: Dict) -> Dict:
    rep = data.get("seller_reputation") or {}
    txn = rep.get("transactions") or {}
    return {
        "seller_id":     str(data.get("id") or ""),
        "nickname":      data.get("nickname") or "",
        "level_id":      rep.get("level_id") or "",
        "power_status":  rep.get("power_seller_status") or "",
        "total_ventas":  txn.get("total"),
        "registration":  (data.get("registration_date") or "")[:10],
    }


def _get_ranking_global(user_id: int, dias: Optional[int]) -> List[Dict]:
    conn = get_connection()
    latest_sub = "(SELECT MAX(snapshot_date) FROM competidores_snapshots WHERE user_id=?)"
    if dias is None:
        rows = conn.execute(f"""
            SELECT seller_id, seller_nickname, seller_level_id,
                   MAX(seller_total_ventas) as ventas
            FROM competidores_snapshots
            WHERE user_id=? AND snapshot_date={latest_sub}
            GROUP BY seller_id
            ORDER BY ventas DESC NULLS LAST
        """, (user_id, user_id)).fetchall()
    else:
        fecha_desde = (date.today() - timedelta(days=dias)).isoformat()
        rows = conn.execute(f"""
            SELECT s1.seller_id, s1.seller_nickname, s1.seller_level_id,
                   s1.ventas_hoy - COALESCE(s0.ventas_antes, s1.ventas_hoy) as ventas
            FROM (
                SELECT seller_id, seller_nickname, seller_level_id,
                       MAX(seller_total_ventas) as ventas_hoy
                FROM competidores_snapshots
                WHERE user_id=? AND snapshot_date={latest_sub}
                GROUP BY seller_id
            ) s1
            LEFT JOIN (
                SELECT seller_id, MAX(seller_total_ventas) as ventas_antes
                FROM competidores_snapshots
                WHERE user_id=? AND snapshot_date = COALESCE(
                    (SELECT MAX(snapshot_date) FROM competidores_snapshots
                     WHERE user_id=? AND snapshot_date <= ?),
                    (SELECT MIN(snapshot_date) FROM competidores_snapshots
                     WHERE user_id=?)
                )
                GROUP BY seller_id
            ) s0 ON s0.seller_id = s1.seller_id
            ORDER BY ventas DESC NULLS LAST
        """, (user_id, user_id, user_id, user_id, fecha_desde, user_id)).fetchall()
    conn.close()
    result = []
    for i, r in enumerate(rows, 1):
        d = dict(r)
        d["rank_ventas"] = i
        result.append(d)
    return result


def _render_tabla(rows_orig: List[Dict], mis_ids: set, titulo: str, nota: str):
    total   = len(rows_orig)
    mi_puesto = None
    for r in rows_orig:
        if str(r.get("seller_id") or "") in mis_ids:
            mi_puesto = r.get("rank_ventas")
            break

    sort_state   = {"col": "ventas", "asc": False}
    tbody_ref: list = [None]

    def _sorted():
        col, asc = sort_state["col"], sort_state["asc"]
        key = (lambda r: (r.get("seller_nickname") or "").lower()) if col == "nick"               else (lambda r: r.get("ventas") if r.get("ventas") is not None else -1)
        return sorted(rows_orig, key=key, reverse=not asc)

    def _render_body():
        tbody_ref[0].clear()
        with tbody_ref[0]:
            for r in _sorted():
                sid    = str(r.get("seller_id") or "")
                es_mio = sid in mis_ids
                ventas = r.get("ventas")
                rank   = r.get("rank_ventas", "—")
                nick   = (r.get("seller_nickname") or f"ID {sid}")[:22]
                icon   = _LVL_ICON.get(r.get("seller_level_id") or "", "")
                bg     = "background:#EEF6FD;" if es_mio else ("background:#fafafa;" if isinstance(rank,int) and rank%2==0 else "")
                pc     = "#ca6d00" if rank==1 else "#7c6514" if rank==2 else "#6b7280" if rank==3 else ("#166534" if es_mio else "#9ca3af")
                fw     = "700" if es_mio else ("600" if isinstance(rank,int) and rank<=3 else "400")

                with ui.element("tr").style(bg):
                    with ui.element("td").style(f"padding:2px 4px;text-align:center;border-bottom:0.5px solid #f1f5f9;font-weight:{fw};color:{pc};font-size:10px;white-space:nowrap"):
                        ui.html(str(rank))
                    with ui.element("td").style(f"padding:2px 6px;border-bottom:0.5px solid #f1f5f9;font-size:10px;font-weight:{fw};{'color:#185FA5' if es_mio else 'color:#374151'}"):
                        ui.label(("⭐ " if es_mio else (icon+" " if icon else ""))+nick).style(
                            "overflow:hidden;text-overflow:ellipsis;white-space:nowrap;display:block"
                        )
                    with ui.element("td").style(f"padding:2px 8px;text-align:right;border-bottom:0.5px solid #f1f5f9;font-size:10px;font-weight:{fw};{'color:#185FA5' if es_mio else 'color:#374151'}"):
                        if ventas is not None and int(ventas) >= 0:
                            ui.html(f"{int(ventas):,}".replace(",","."))
                        else:
                            ui.html("<span style='color:#9ca3af'>—</span>")

    def _toggle(col):
        if sort_state["col"] == col:
            sort_state["asc"] = not sort_state["asc"]
        else:
            sort_state["col"] = col
            sort_state["asc"] = (col == "nick")
        _render_body()

    if mi_puesto and total:
        pct = mi_puesto / total
        pos_color = "#86EFAC" if pct <= 0.1 else "#FDE68A" if pct <= 0.3 else "rgba(255,255,255,.85)"
        pos_txt   = f"#{mi_puesto} / {total}"
    else:
        pos_color = "rgba(255,255,255,.45)"
        pos_txt   = f"— / {total}"

    TH = "padding:4px 6px;background:#EEF6FD;color:#185FA5;font-size:9px;font-weight:600;position:sticky;top:0;z-index:2;border-bottom:0.5px solid #d0e8f8;cursor:pointer;user-select:none;white-space:nowrap"

    with ui.element("div").style("flex:1;min-width:160px;border:0.5px solid #e2e8f0;border-radius:8px;overflow:hidden;display:flex;flex-direction:column"):
        with ui.element("div").style("background:#2A7AC7;padding:7px 10px;flex-shrink:0"):
            with ui.element("div").style("display:flex;justify-content:space-between;align-items:center"):
                with ui.element("div"):
                    ui.label(titulo).style("font-size:12px;font-weight:500;color:#fff;display:block")
                    ui.label(nota).style("font-size:9px;color:rgba(255,255,255,.65);display:block")
                ui.label(pos_txt).style(f"font-size:12px;font-weight:700;color:{pos_color};white-space:nowrap;margin-left:8px")

        if not rows_orig:
            ui.label("Sin datos aun — cargando al completarse el snapshot").style("font-size:10px;color:#9ca3af;padding:20px;text-align:center;display:block")
            return

        with ui.element("div").style("overflow-y:auto;max-height:calc(100vh - 380px)"):
            with ui.element("table").style("width:100%;border-collapse:collapse;table-layout:fixed"):
                with ui.element("thead"):
                    with ui.element("tr"):
                        with ui.element("th").style(TH+";width:26px;text-align:center"): ui.html("#")
                        with ui.element("th").style(TH+";text-align:left").on("click",lambda: _toggle("nick")):
                            ui.html("Vendedor <span style='opacity:.5'>↕</span>")
                        with ui.element("th").style(TH+";width:78px;text-align:right").on("click",lambda: _toggle("ventas")):
                            ui.html("Ventas <span style='opacity:.5'>↕</span>")
                tbody = ui.element("tbody")
                tbody_ref[0] = tbody
                _render_body()

        hay_datos = any((r.get("ventas") or 0) > 0 for r in rows_orig)
        if not hay_datos and titulo != "Historica":
            with ui.element("div").style("padding:4px 8px;background:#F8FAFC;border-top:0.5px solid #e2e8f0;font-size:9px;color:#9ca3af;flex-shrink:0"):
                ui.html("Sin diferencias aun — se acumulan con cada snapshot")


def build_tab_competidores() -> None:
    user = app.storage.user.get("user")
    if not user:
        ui.label("Debes iniciar sesion").classes("text-red-500 p-4")
        return
    uid     = user["id"]
    mis_ids = _get_mis_seller_ids(uid)

    PERIODOS = [
        ("Historica", None, "acumulado de por vida"),
        ("Anual",     365,  "ultimos 365 dias"),
        ("Mensual",   30,   "ultimos 30 dias"),
        ("Semanal",   7,    "ultimos 7 dias"),
        ("Diaria",    1,    "ultimas 24 hs"),
    ]

    # resultado del buscador
    buscar_ref:  list = [None]
    tablas_ref:  list = [None]
    notif_ref:   list = [None]

    def _recargar_tablas():
        tablas_ref[0].clear()
        with tablas_ref[0]:
            with ui.card().classes("w-full p-8 items-center gap-4"):
                ui.spinner(size="xl")
                ui.label("Actualizando...").classes("text-xl text-gray-700")

        async def _reload():
            all_data = []
            for titulo, dias, nota in PERIODOS:
                rows = await run.io_bound(_get_ranking_global, uid, dias)
                all_data.append((titulo, dias, nota, rows))
            tablas_ref[0].clear()
            with tablas_ref[0]:
                for titulo, dias, nota, rows in all_data:
                    _render_tabla(rows, mis_ids, titulo, nota)

        from nicegui import background_tasks
        background_tasks.create(_reload(), name="comp_reload")

    async def _buscar(query: str):
        notif_ref[0].set_text("Buscando...")
        resultado_area.clear()
        buscar_ref[0] = None
        try:
            from ml_api import get_ml_access_token as _get_tok_fresh
        except ImportError:
            from db import get_ml_access_token as _get_tok_fresh
        _token_fresh = _get_tok_fresh(uid) or ""

        # Detectar URL o ID de catálogo ML (/p/MLA... o /up/MLAU...)
        catalog_match = re.search(r'/p/(MLA\d+)', query) or re.search(r'/up/(MLAU\d+)', query)

        if catalog_match:
            cid = catalog_match.group(1)
            notif_ref[0].set_text(f"Cargando catálogo {cid}...")
            resultado_area.clear()
            resultado = await run.io_bound(_buscar_y_agregar_catalogo, cid, uid, _token_fresh)
            notif_ref[0].set_text("")
            resultado_area.clear()
            with resultado_area:
                if "error" in resultado:
                    ui.label(resultado["error"]).style("font-size:11px;color:#dc2626")
                else:
                    msg = (f"Catálogo {resultado['catalog_id']}: "
                           f"{resultado['total']} vendedores encontrados — "
                           f"{resultado['agregados']} agregados, "
                           f"{resultado['ya_existian']} ya estaban en seguimiento.")
                    ui.label(msg).style("font-size:11px;color:#166534;font-weight:500")
                    if resultado['agregados'] > 0:
                        _recargar_tablas()
            return

        v = await run.io_bound(_buscar_vendedor, query, _token_fresh)
        buscar_ref[0] = v
        notif_ref[0].set_text("")
        resultado_area.clear()
        if not v:
            with resultado_area:
                ui.label(
                    "No encontrado. Si el vendedor no está en tus catálogos, ingresá su seller ID "
                    "numérico directamente (lo encontrás en el código fuente de su perfil en ML o "
                    "en las requests de red del browser)."
                ).style("font-size:11px;color:#dc2626")
            return
        with resultado_area:
            sid      = v["seller_id"]
            nick     = v["nickname"]
            icon     = _LVL_ICON.get(v.get("level_id") or "", "")
            ventas   = v.get("total_ventas")
            conn = get_connection()
            try:
                ya_en_snapshots = conn.execute(
                    "SELECT 1 FROM competidores_snapshots WHERE user_id=? AND seller_id=? LIMIT 1",
                    (uid, sid)
                ).fetchone() is not None
            finally:
                conn.close()
            ya_en_seguidos = any(s["seller_id"] == sid for s in _get_seguidos(uid))
            ya_sigo = ya_en_snapshots or ya_en_seguidos
            with ui.element("div").style(
                "display:flex;align-items:center;gap:10px;background:#EEF6FD;"
                "border:0.5px solid #85B7EB;border-radius:8px;padding:8px 12px"
            ):
                ui.label(f"{icon} {nick}").style("font-size:13px;font-weight:500;color:#185FA5")
                if ventas:
                    ui.label(f"{int(ventas):,} ventas hist.".replace(",",".")).style("font-size:11px;color:#374151")
                ui.label(f"ID: {sid}").style("font-size:10px;color:#9ca3af")
                ui.element("div").style("flex:1")
                if ya_en_seguidos:
                    def _quitar():
                        _remove_seguido(uid, sid)
                        ui.notify(f"{nick} quitado del seguimiento", color="warning", timeout=2000)
                        _recargar_tablas()
                        resultado_area.clear()
                    ui.button("Quitar seguimiento", on_click=_quitar).props(
                        "dense no-caps unelevated"
                    ).style("background:#FEE2E2;color:#991B1B;font-size:11px;padding:4px 10px;border-radius:4px")
                elif ya_sigo:
                    ui.label("Ya en seguimiento").style(
                        "background:#F3F4F6;color:#6B7280;font-size:11px;padding:4px 10px;border-radius:4px"
                    )
                else:
                    def _agregar():
                        _add_seguido(uid, sid, nick)
                        ui.notify(f"{nick} agregado al seguimiento", color="positive", timeout=2000)
                        _recargar_tablas()
                        resultado_area.clear()
                    ui.button("+ Agregar al seguimiento", on_click=_agregar).props(
                        "dense no-caps unelevated"
                    ).style("background:#2A7AC7;color:#fff;font-size:11px;padding:4px 12px;border-radius:4px")

    with ui.element("div").style("padding:8px 16px 0;display:flex;flex-direction:column;height:calc(100vh - 130px);overflow:hidden"):
        # Buscador
        with ui.element("div").style(
            "background:var(--color-background-primary);border:0.5px solid #e2e8f0;"
            "border-radius:8px;padding:8px 12px;margin-bottom:8px;flex-shrink:0"
        ):
            with ui.row().style("gap:8px;align-items:center;flex-wrap:wrap"):
                with ui.element("div").style("display:flex;gap:0;flex:1;min-width:260px"):
                    inp = ui.input(
                        placeholder="Nickname, URL de perfil ML o seller ID..."
                    ).props("dense outlined").style(
                        "flex:1;font-size:12px;border-radius:4px 0 0 4px"
                    )
                    with ui.element("button").on(
                        "click", lambda: ui.timer(0.05, lambda: _buscar(inp.value), once=True)
                    ).style(
                        "height:36px;padding:0 14px;background:#2A7AC7;color:#fff;"
                        "border:none;border-radius:0 4px 4px 0;font-size:12px;cursor:pointer;flex-shrink:0"
                    ):
                        ui.html('<i class="ti ti-search" style="font-size:14px;color:#fff"></i>')
                notif = ui.label("").style("font-size:10px;color:#9ca3af;align-self:center")
                notif_ref[0] = notif
            resultado_area = ui.element("div").style("margin-top:4px")

        # 5 tablas — spinner inmediato, datos en background
        tablas = ui.element("div").style("display:flex;gap:8px;flex:1;min-height:0;overflow:hidden")
        tablas_ref[0] = tablas
        with tablas:
            with ui.card().classes("w-full p-8 items-center gap-4"):
                ui.spinner(size="xl")
                ui.label("Cargando competidores...").classes("text-xl text-gray-700")

        async def _cargar_tablas():
            all_data = []
            for titulo, dias, nota in PERIODOS:
                rows = await run.io_bound(_get_ranking_global, uid, dias)
                all_data.append((titulo, dias, nota, rows))
            tablas.clear()
            with tablas:
                for titulo, dias, nota, rows in all_data:
                    _render_tabla(rows, mis_ids, titulo, nota)

        from nicegui import background_tasks
        background_tasks.create(_cargar_tablas(), name="comp_load")
