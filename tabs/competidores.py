"""
tabs/competidores.py
Ranking global de competidores con buscador por nickname/URL/ID.
"""
from __future__ import annotations
import html, json, re, requests
from datetime import date, datetime, timedelta
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


def _get_comparador(user_id: int) -> List[Dict]:
    """Carga los competidores del comparador ordenados por ventas históricas DESC."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT cc.seller_id, cc.seller_nickname,
               COALESCE(MAX(cs.seller_total_ventas), 0) as hist
        FROM comparador_competidores cc
        LEFT JOIN competidores_snapshots cs
               ON cs.seller_id = cc.seller_id AND cs.user_id = cc.user_id
        WHERE cc.user_id = ?
        GROUP BY cc.seller_id, cc.seller_nickname
        ORDER BY hist DESC NULLS LAST
    """, (user_id,)).fetchall()
    conn.close()
    return [{"seller_id": r[0], "nickname": r[1] or "", "hist": r[2] or 0} for r in rows]


def _add_comparador(user_id: int, seller_id: str, nickname: str):
    conn = get_connection()
    conn.execute(
        "INSERT OR IGNORE INTO comparador_competidores (user_id, seller_id, seller_nickname) VALUES (?,?,?)",
        (user_id, seller_id, nickname)
    )
    conn.commit()
    conn.close()


def _remove_comparador(user_id: int, seller_id: str):
    conn = get_connection()
    conn.execute(
        "DELETE FROM comparador_competidores WHERE user_id=? AND seller_id=?",
        (user_id, seller_id)
    )
    conn.commit()
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
        fecha_reciente_row = conn.execute(
            "SELECT MAX(snapshot_date) FROM competidores_snapshots WHERE user_id=?",
            (user_id,)
        ).fetchone()
        fecha_reciente = fecha_reciente_row[0] if fecha_reciente_row else None
        if not fecha_reciente:
            conn.close()
            return []

        from datetime import datetime, timedelta
        fecha_desde = (
            datetime.strptime(fecha_reciente, "%Y-%m-%d") - timedelta(days=dias)
        ).strftime("%Y-%m-%d")

        # Fecha de referencia por seller: el último snapshot <= fecha_desde,
        # o si el seller no tiene historia tan vieja, su snapshot más antiguo.
        rows = conn.execute("""
            WITH ref AS (
                SELECT seller_id,
                       COALESCE(
                           MAX(CASE WHEN snapshot_date <= ? THEN snapshot_date END),
                           MIN(snapshot_date)
                       ) AS ref_date
                FROM competidores_snapshots
                WHERE user_id=?
                GROUP BY seller_id
            )
            SELECT s2.seller_id, s2.seller_nickname, s2.seller_level_id,
                   MAX(s2.seller_total_ventas) - MAX(s1.seller_total_ventas) AS ventas
            FROM ref r
            JOIN competidores_snapshots s1
                ON s1.user_id=? AND s1.seller_id=r.seller_id AND s1.snapshot_date=r.ref_date
            JOIN competidores_snapshots s2
                ON s2.user_id=? AND s2.seller_id=r.seller_id AND s2.snapshot_date=?
            GROUP BY s2.seller_id
            ORDER BY ventas DESC NULLS LAST
        """, (fecha_desde, user_id, user_id, user_id, fecha_reciente)).fetchall()
    conn.close()
    result = []
    for i, r in enumerate(rows, 1):
        d = dict(r)
        d["rank_ventas"] = i
        result.append(d)
    return result


def _actualizar_ventas_db(user_id: int, progress_label, cancelar_ref: list) -> Dict:
    """
    Actualiza seller_total_ventas de todos los sellers únicos en competidores_snapshots.
    No escanea catálogos. Solo llama /users/{seller_id} por cada seller ya en la DB.
    """
    try:
        from ml_api import get_ml_access_token as _get_tok
    except ImportError:
        from db import get_ml_access_token as _get_tok

    token = _get_tok(user_id) or ""
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    today = date.today().isoformat()

    conn = get_connection()
    sellers = conn.execute("""
        SELECT DISTINCT seller_id, seller_nickname
        FROM competidores_snapshots WHERE user_id=?
        UNION
        SELECT seller_id, seller_nickname
        FROM competidores_seguidos WHERE user_id=?
    """, (user_id, user_id)).fetchall()
    conn.close()

    total = len(sellers)
    actualizados = 0
    sin_ventas = 0

    for i, row in enumerate(sellers, 1):
        if cancelar_ref[0]:
            break
        sid, nick = row[0], row[1]
        try:
            progress_label.set_text(f"Leyendo {i} / {total}...")
            r = requests.get(f"{_ML_API}/users/{sid}", headers=headers, timeout=8)
            if r.status_code == 200:
                d = r.json()
                rep = d.get("seller_reputation") or {}
                txn = rep.get("transactions") or {}
                total_ventas = txn.get("total")
                nick_nuevo = d.get("nickname") or nick

                if not total_ventas:
                    sin_ventas += 1
                    continue  # ignorar, no borrar

                # Obtener el máximo histórico para no bajar nunca
                conn = get_connection()
                max_hist = conn.execute(
                    "SELECT MAX(seller_total_ventas) FROM competidores_snapshots WHERE user_id=? AND seller_id=?",
                    (user_id, sid)
                ).fetchone()[0] or 0
                valor_final = max(total_ventas, max_hist)
                try:
                    conn.execute("""
                        INSERT INTO competidores_snapshots
                            (user_id, catalog_product_id, seller_id, seller_nickname,
                             seller_total_ventas, seller_level_id, seller_power_status,
                             snapshot_date)
                        VALUES (?, 'MANUAL', ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(user_id, catalog_product_id, seller_id, snapshot_date)
                        DO UPDATE SET
                            seller_total_ventas=CASE
                                WHEN excluded.seller_total_ventas > COALESCE(competidores_snapshots.seller_total_ventas, 0)
                                THEN excluded.seller_total_ventas
                                ELSE competidores_snapshots.seller_total_ventas
                            END,
                            seller_nickname=excluded.seller_nickname,
                            seller_level_id=excluded.seller_level_id,
                            seller_power_status=excluded.seller_power_status,
                            created_at=CURRENT_TIMESTAMP
                    """, (
                        user_id, sid, nick_nuevo, valor_final,
                        rep.get("level_id") or "",
                        rep.get("power_seller_status") or "",
                        today
                    ))
                    conn.commit()
                finally:
                    conn.close()
                actualizados += 1
        except Exception:
            pass

    return {
        "total": total,
        "actualizados": actualizados,
        "sin_ventas": sin_ventas,
    }


def _get_ultima_actualizacion(user_id: int) -> str:
    conn = get_connection()
    row = conn.execute("""
        SELECT MAX(created_at) FROM competidores_snapshots WHERE user_id=?
    """, (user_id,)).fetchone()
    conn.close()
    if row and row[0]:
        try:
            from datetime import datetime, timedelta
            dt = datetime.fromisoformat(str(row[0])) - timedelta(hours=3)
            return dt.strftime("%d/%m/%y — %H:%M")
        except Exception:
            return str(row[0])[:16]
    return "—"


def _render_tabla(rows_orig: List[Dict], mis_ids: set, titulo: str, nota: str, filtro_ref: Optional[list] = None, on_click_nick: Optional[callable] = None):
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
        texto_filtro = (filtro_ref[0]["texto"].lower() if filtro_ref else "")
        with tbody_ref[0]:
            for r in _sorted():
                sid    = str(r.get("seller_id") or "")
                es_mio = sid in mis_ids
                ventas = r.get("ventas")
                rank   = r.get("rank_ventas", "—")
                nick   = (r.get("seller_nickname") or f"ID {sid}")[:22]
                icon   = _LVL_ICON.get(r.get("seller_level_id") or "", "")

                if texto_filtro and texto_filtro not in nick.lower():
                    continue  # ocultar fila que no coincide con el filtro

                if texto_filtro and texto_filtro in nick.lower():
                    idx = nick.lower().find(texto_filtro)
                    nick_html = (
                        html.escape(nick[:idx]) +
                        f'<mark style="background:#FEF08A;padding:0">{html.escape(nick[idx:idx+len(texto_filtro)])}</mark>' +
                        html.escape(nick[idx+len(texto_filtro):])
                    )
                else:
                    nick_html = html.escape(nick)

                bg     = "background:#EEF6FD;" if es_mio else ("background:#fafafa;" if isinstance(rank,int) and rank%2==0 else "")
                pc     = "#ca6d00" if rank==1 else "#7c6514" if rank==2 else "#6b7280" if rank==3 else ("#166534" if es_mio else "#9ca3af")
                fw     = "700" if es_mio else ("600" if isinstance(rank,int) and rank<=3 else "400")

                with ui.element("tr").style(bg):
                    with ui.element("td").style(f"padding:2px 4px;text-align:center;border-bottom:0.5px solid #f1f5f9;font-weight:{fw};color:{pc};font-size:10px;white-space:nowrap"):
                        ui.html(str(rank))
                    with ui.element("td").style(f"padding:2px 6px;border-bottom:0.5px solid #f1f5f9;font-size:10px;font-weight:{fw};{'color:#185FA5' if es_mio else 'color:#374151'}"):
                        prefijo = "⭐ " if es_mio else (icon+" " if icon else "")
                        nick_full = (r.get("seller_nickname") or f"ID {sid}")
                        url = f"https://www.mercadolibre.com.ar/perfil/{nick_full}"
                        nick_el = ui.html(
                            f'<a href="{html.escape(url)}" target="_blank" '
                            f'style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;'
                            f'display:block;text-decoration:none;cursor:pointer;'
                            f'color:{"#185FA5" if es_mio else "#374151"};font-weight:{fw}">'
                            f'{html.escape(prefijo)}{nick_html}</a>'
                        )
                        if on_click_nick:
                            nick_el.on("click", lambda sid=sid, nick_full=nick_full: on_click_nick(sid, nick_full))
                            nick_el.tooltip("Agregar al comparador")
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

    pos_color = "#ffffff"
    pos_txt   = f"#{mi_puesto} / {total}" if mi_puesto else f"— / {total}"

    TH = "padding:4px 6px;background:#EEF6FD;color:#185FA5;font-size:9px;font-weight:600;position:sticky;top:0;z-index:2;border-bottom:0.5px solid #d0e8f8;cursor:pointer;user-select:none;white-space:nowrap"

    with ui.element("div").style("flex:1;min-width:160px;border:0.5px solid #e2e8f0;border-radius:8px;overflow:hidden;display:flex;flex-direction:column"):
        with ui.element("div").style("background:#2A7AC7;padding:7px 10px;flex-shrink:0"):
            with ui.element("div").style("display:flex;justify-content:space-between;align-items:center"):
                with ui.element("div"):
                    ui.label(titulo).style("font-size:12px;font-weight:500;color:#fff;display:block")
                    ui.label(nota).style("font-size:9px;color:rgba(255,255,255,.65);display:block")
                ui.label(pos_txt).style(
                    f"font-size:12px;font-weight:700;color:#ffffff;white-space:nowrap;margin-left:8px"
                )

        if not rows_orig:
            ui.label("Sin datos aun — cargando al completarse el snapshot").style("font-size:10px;color:#9ca3af;padding:20px;text-align:center;display:block")
            return

        with ui.element("div").classes("comp-tabla-scroll").style("overflow-y:auto;max-height:calc(100vh - 420px)"):
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


def _render_comparador(uid: int, mis_ids: set):
    """Ranking de hasta 15 competidores seguidos, repartido en 3 divisiones de 5, ordenable por periodo."""
    POR_DIVISION = 5
    MAX_COMPARADOR = POR_DIVISION * 3
    DIVISIONES = [("Primera A", True), ("Nacional B", False), ("Primera C", False)]
    ORDEN_OPCIONES = [
        ("hist",    "Histórica", None, "Hist."),
        ("mensual", "Mensual",   30,   "Mens."),
        ("semanal", "Semanal",   7,    "Sem."),
        ("diaria",  "Diaria",    1,    "Día"),
    ]
    orden_state = {"col": "hist"}

    comparador_ref = [{"sellers": _get_comparador(uid)}]  # lista de {seller_id, nickname, hist}

    with ui.element("div").style("display:flex;gap:6px;align-items:stretch;flex-shrink:0;flex-wrap:wrap;width:100%"):
        orden_col = ui.element("div").style(
            "display:flex;flex-direction:column;gap:3px;width:58px;flex-shrink:0;height:100%"
        )

        tabla_refs: list = []
        for _ in DIVISIONES:
            tabla_refs.append(ui.element("div").style(
                "border:1px solid #d0e8f8;border-radius:8px;overflow:hidden;flex:1 1 0;min-width:150px"
            ))

        right_col = ui.element("div").style(
            "display:flex;flex-direction:column;gap:6px;align-items:flex-start;"
            "width:180px;flex-shrink:0;padding-left:4px"
        )

        def _render_tabla_comp():
            sellers = _get_comparador(uid)  # {seller_id, nickname, hist}
            comparador_ref[0]["sellers"] = sellers

            rankings = {
                key: {str(r["seller_id"]): r for r in _get_ranking_global(uid, dias)}
                for key, _label, dias, _corto in ORDEN_OPCIONES
            }

            def _valor(entry, key):
                if key == "hist":
                    return entry.get("hist")
                r = rankings[key].get(entry["seller_id"])
                return r.get("ventas") if r else None

            col_actual = orden_state["col"]

            def _sort_key(entry):
                val = _valor(entry, col_actual)
                return (0 if val is not None else 1, -(val or 0))

            sellers_ordenados = sorted(sellers, key=_sort_key)

            def _fila(entry):
                sid  = entry["seller_id"]
                nick = entry["nickname"]

                def _quitar(s=sid):
                    _remove_comparador(uid, s)
                    _render_tabla_comp()

                with ui.element("tr"):
                    with ui.element("td").style("padding:2px 3px;border-bottom:0.5px solid var(--color-border);overflow:hidden"):
                        with ui.row().style("gap:2px;align-items:center;flex-wrap:nowrap"):
                            with ui.element("span").on("click", _quitar).style(
                                "cursor:pointer;color:var(--color-text-secondary);display:inline-flex;align-items:center;flex-shrink:0"
                            ):
                                ui.html('<i class="ti ti-trash" style="font-size:10px" aria-hidden="true"></i>')
                            ui.html(
                                f'<a href="https://www.mercadolibre.com.ar/perfil/{html.escape(nick)}" target="_blank" '
                                f'style="font-size:9px;font-weight:500;color:#185FA5;text-decoration:none;'
                                f'overflow:hidden;text-overflow:ellipsis;white-space:nowrap;display:block">'
                                f'{html.escape(nick[:14])}</a>'
                            )
                    for key, _label, _dias, _corto in ORDEN_OPCIONES:
                        val = _valor(entry, key)
                        activo = key == col_actual
                        bg = "background:#EEF6FD;" if activo else ""
                        fw = "700" if activo else "400"
                        with ui.element("td").style(
                            f"padding:2px 3px;border-bottom:0.5px solid var(--color-border);"
                            f"text-align:right;font-size:9px;{bg}font-weight:{fw};overflow:hidden"
                        ):
                            ui.html(f"{int(val):,}".replace(",",".") if val else "—")

            def _fila_vacia(placeholder: str):
                with ui.element("tr"):
                    with ui.element("td").style(
                        "padding:2px 3px;border-bottom:0.5px solid var(--color-border);"
                        "font-size:9px;color:var(--color-text-secondary);font-style:italic;"
                        "overflow:hidden;text-overflow:ellipsis;white-space:nowrap"
                    ):
                        ui.html(placeholder)
                    for _ in range(len(ORDEN_OPCIONES)):
                        ui.element("td").style("border-bottom:0.5px solid var(--color-border)")

            for idx, (titulo, con_boton) in enumerate(DIVISIONES):
                grupo = sellers_ordenados[idx * POR_DIVISION:(idx + 1) * POR_DIVISION]
                cont = tabla_refs[idx]
                cont.clear()
                with cont:
                    with ui.element("table").style("width:100%;border-collapse:collapse;table-layout:fixed"):
                        with ui.element("thead"):
                            with ui.element("tr"):
                                with ui.element("th").style("background:#2A7AC7;color:#fff;font-size:8px;font-weight:500;padding:3px 3px;text-align:left;width:36%;overflow:hidden"):
                                    with ui.row().style("gap:3px;align-items:center;flex-wrap:nowrap"):
                                        ui.html(titulo)
                                        if con_boton:
                                            with ui.element("span").style(
                                                "display:inline-flex;align-items:center;justify-content:center;"
                                                "width:14px;height:14px;border-radius:3px;background:rgba(255,255,255,.2);"
                                                "cursor:pointer;flex-shrink:0"
                                            ).on("click", lambda: _abrir_popup_competidores()):
                                                ui.html('<i class="ti ti-plus" style="font-size:10px;color:#fff" aria-hidden="true"></i>')
                                for key, _label, _dias, corto in ORDEN_OPCIONES:
                                    activo = key == col_actual
                                    bg = "background:#185FA5;" if activo else "background:#2A7AC7;"
                                    with ui.element("th").style(
                                        f"{bg}color:#fff;font-size:8px;font-weight:500;"
                                        f"padding:3px 3px;text-align:right;width:16%"
                                    ):
                                        ui.html(corto)
                        with ui.element("tbody"):
                            for entry in grupo:
                                _fila(entry)
                            for _ in range(POR_DIVISION - len(grupo)):
                                _fila_vacia("— agregar competidor" if con_boton else "—")

        def _render_orden_botones():
            orden_col.clear()
            with orden_col:
                for key, label, _dias, _corto in ORDEN_OPCIONES:
                    activo = orden_state["col"] == key

                    def _click(k=key):
                        orden_state["col"] = k
                        _render_orden_botones()
                        _render_tabla_comp()

                    estilo = "background:#2A7AC7;color:#fff;" if activo else "background:#EEF6FD;color:#185FA5;"
                    with ui.element("div").on("click", _click).style(
                        estilo +
                        "flex:1;display:flex;align-items:center;justify-content:center;"
                        "font-size:9px;font-weight:600;padding:4px 2px;border-radius:4px;cursor:pointer;"
                        "text-align:center;line-height:1.15;user-select:none"
                    ):
                        ui.html(label)

        def _abrir_popup_competidores():
            sellers_actual = comparador_ref[0]["sellers"]
            ids_actuales = {s["seller_id"] for s in sellers_actual}

            with ui.dialog() as dlg:
                dlg.props("maximized=false persistent=false")
                with ui.card().style("width:460px;max-height:500px;padding:0;overflow:hidden;display:flex;flex-direction:column"):
                    # Header
                    with ui.element("div").style("background:#2A7AC7;padding:10px 14px;display:flex;justify-content:space-between;align-items:center;flex-shrink:0"):
                        with ui.element("div"):
                            ui.label("Agregar competidor").style("font-size:13px;font-weight:500;color:#fff;display:block")
                            n_slots = MAX_COMPARADOR - len(sellers_actual)
                            ui.label(f"{len(sellers_actual)} de {MAX_COMPARADOR} slots usados · {n_slots} disponibles").style("font-size:9px;color:rgba(255,255,255,.7);display:block")
                        ui.button(icon="close", on_click=dlg.close).props("flat dense").style("color:#fff")

                    # Buscador
                    filtro_popup = {"texto": ""}
                    lista_ref = [None]

                    with ui.element("div").style("padding:8px 12px;border-bottom:0.5px solid #e2e8f0;flex-shrink:0"):
                        inp_popup = ui.input(placeholder="Buscar por nombre...").props("dense outlined clearable").style("width:100%;font-size:12px")

                    # Cargar todos los vendedores de la DB ordenados por ventas
                    conn = get_connection()
                    todos = conn.execute("""
                        SELECT seller_id, seller_nickname,
                               MAX(seller_total_ventas) as hist
                        FROM competidores_snapshots
                        WHERE user_id=?
                        GROUP BY seller_id
                        ORDER BY hist DESC NULLS LAST
                    """, (uid,)).fetchall()
                    conn.close()

                    vendedores_data = []
                    for t in todos:
                        sid, nick, hist = str(t[0]), t[1] or "", t[2] or 0
                        vendedores_data.append({"seller_id": sid, "nickname": nick, "hist": hist})

                    def _render_lista():
                        lista_ref[0].clear()
                        texto = filtro_popup["texto"].lower()
                        with lista_ref[0]:
                            with ui.element("table").style("width:100%;border-collapse:collapse"):
                                with ui.element("thead"):
                                    with ui.element("tr"):
                                        for h, a in [("Vendedor","left"),("Hist.","right"),("Sem.","right")]:
                                            with ui.element("th").style(
                                                f"padding:4px 10px;background:var(--color-background-secondary);"
                                                f"font-size:9px;font-weight:600;color:#185FA5;"
                                                f"text-align:{a};position:sticky;top:0;"
                                                f"border-bottom:0.5px solid #e2e8f0"
                                            ):
                                                ui.html(h)
                                        with ui.element("th").style(
                                            "padding:4px 6px;background:var(--color-background-secondary);"
                                            "width:36px;position:sticky;top:0;border-bottom:0.5px solid #e2e8f0"
                                        ):
                                            ui.html("")
                                with ui.element("tbody"):
                                    for v in vendedores_data:
                                        if texto and texto not in v["nickname"].lower():
                                            continue
                                        sid = v["seller_id"]
                                        ya_agregado = sid in ids_actuales

                                        def _agregar(s=sid, n=v["nickname"]):
                                            if len(comparador_ref[0]["sellers"]) >= MAX_COMPARADOR:
                                                ui.notify(f"Máximo {MAX_COMPARADOR} competidores", color="warning", timeout=2000)
                                                return
                                            _add_comparador(uid, s, n)
                                            comparador_ref[0]["sellers"] = _get_comparador(uid)
                                            ids_actuales.add(s)
                                            ui.notify(f"{n} agregado", color="positive", timeout=1500)
                                            comparador_ref[0]["_render"]()
                                            _render_lista()

                                        with ui.element("tr"):
                                            with ui.element("td").style("padding:4px 10px;border-bottom:0.5px solid #f1f5f9;font-size:11px"):
                                                ui.label(v["nickname"][:30]).style("display:block;overflow:hidden;text-overflow:ellipsis;white-space:nowrap")
                                            with ui.element("td").style("padding:4px 10px;border-bottom:0.5px solid #f1f5f9;font-size:11px;text-align:right"):
                                                ui.html(f"{int(v['hist']):,}".replace(",",".") if v['hist'] else "—")
                                            with ui.element("td").style("padding:4px 10px;border-bottom:0.5px solid #f1f5f9;font-size:11px;text-align:right"):
                                                ui.html("—")
                                            with ui.element("td").style("padding:4px 6px;border-bottom:0.5px solid #f1f5f9;text-align:center"):
                                                if ya_agregado:
                                                    with ui.element("span").style(
                                                        "display:inline-flex;align-items:center;justify-content:center;"
                                                        "width:22px;height:22px;border-radius:4px;"
                                                        "background:#DCFCE7;color:#166534;border:0.5px solid #86EFAC"
                                                    ):
                                                        ui.html('<i class="ti ti-check" style="font-size:11px" aria-hidden="true"></i>')
                                                else:
                                                    with ui.element("span").on("click", _agregar).style(
                                                        "display:inline-flex;align-items:center;justify-content:center;"
                                                        "width:22px;height:22px;border-radius:4px;"
                                                        "background:#EEF6FD;color:#185FA5;border:0.5px solid #85B7EB;cursor:pointer"
                                                    ):
                                                        ui.html('<i class="ti ti-plus" style="font-size:11px" aria-hidden="true"></i>')

                    def _on_filtro_popup(e):
                        filtro_popup["texto"] = (e.value or "").strip()
                        _render_lista()
                    inp_popup.on_value_change(_on_filtro_popup)

                    with ui.element("div").style("overflow-y:auto;flex:1"):
                        lista = ui.element("div")
                        lista_ref[0] = lista
                        _render_lista()

            dlg.open()

        _render_orden_botones()
        _render_tabla_comp()

    comparador_ref[0]["_render"] = _render_tabla_comp

    return comparador_ref, right_col


def build_tab_competidores() -> None:
    ui.add_css("""
        @media (max-width: 768px) {
            .comp-tablas { flex-direction: column !important; }
            .comp-tablas > div { min-width: 100% !important; flex: none !important; }
            .comp-tabla-scroll { max-height: 60vh !important; }
        }
    """)
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
    buscar_ref:     list = [None]
    tablas_ref:     list = [None]
    notif_ref:      list = [None]
    ultima_act_ref: list = [None]

    def _recargar_tablas():
        tablas_ref[0].clear()
        with tablas_ref[0]:
            with ui.element("div").style(
                "width:100%;display:flex;flex-direction:column;align-items:center;"
                "justify-content:center;gap:16px;padding:60px 0"
            ):
                ui.html('''
                    <div style="display:flex;gap:8px;align-items:center">
                        <i class="ti ti-users" style="font-size:28px;color:#2A7AC7"></i>
                        <span style="font-size:16px;font-weight:500;color:#185FA5">Competidores</span>
                    </div>
                ''')
                ui.spinner(size="lg", color="#2A7AC7")
                ui.label("Cargando ranking...").style("font-size:12px;color:#9ca3af")

        async def _reload():
            all_data = []
            for titulo, dias, nota in PERIODOS:
                rows = await run.io_bound(_get_ranking_global, uid, dias)
                all_data.append((titulo, dias, nota, rows))
            tablas_ref[0].clear()
            with tablas_ref[0]:
                for titulo, dias, nota, rows in all_data:
                    _render_tabla(rows, mis_ids, titulo, nota, filtro_ref, _on_click_nick)

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
                        _agregar_al_comparador(sid, nick)
                        ui.notify(f"{nick} agregado al seguimiento", color="positive", timeout=2000)
                        _recargar_tablas()
                        resultado_area.clear()
                    ui.button("+ Agregar al seguimiento", on_click=_agregar).props(
                        "dense no-caps unelevated"
                    ).style("background:#2A7AC7;color:#fff;font-size:11px;padding:4px 12px;border-radius:4px")

    with ui.element("div").style("padding:8px 10px;display:flex;flex-direction:column;max-width:100%"):
        filtro_ref: list = [{"texto": ""}]

        with ui.element("div").style("margin-bottom:8px"):
            comparador_ref, right_col = _render_comparador(uid, mis_ids)

        def _agregar_al_comparador(seller_id: str, nickname: str):
            sellers = comparador_ref[0]["sellers"]
            if len(sellers) >= 15:
                ui.notify("Máximo 15 competidores en el comparador", color="warning", timeout=2000)
                return
            if any(s["seller_id"] == seller_id for s in sellers):
                ui.notify(f"{nickname} ya está en el comparador", timeout=1500)
                return
            _add_comparador(uid, seller_id, nickname)
            comparador_ref[0]["sellers"] = _get_comparador(uid)
            comparador_ref[0]["_render"]()
            ui.notify(f"{nickname} agregado al comparador", color="positive", timeout=1500)

        def _on_click_nick(sid: str, nick: str):
            _agregar_al_comparador(sid, nick)

        async def _lanzar_actualizacion():
            cancelar_ref = [False]

            with ui.dialog().props("persistent") as dlg, ui.card().style("min-width:340px;padding:24px;text-align:center"):
                ui.label("Actualizando ventas históricas").style(
                    "font-size:14px;font-weight:500;color:#185FA5;margin-bottom:12px;display:block"
                )
                ui.spinner(size="xl", color="#2A7AC7")
                prog = ui.label("Iniciando...").style(
                    "font-size:12px;color:#6b7280;margin-top:12px;display:block"
                )

                def _cancelar():
                    cancelar_ref[0] = True
                    dlg.close()
                    ui.notify("Actualización cancelada", color="warning", timeout=2000)

                ui.button("Cancelar", on_click=_cancelar).props(
                    "flat no-caps"
                ).style("color:#dc2626;font-size:12px;margin-top:16px")

            dlg.open()
            resultado = await run.io_bound(_actualizar_ventas_db, uid, prog, cancelar_ref)
            if not cancelar_ref[0]:
                dlg.close()
                ui.notify(
                    f"✓ {resultado['actualizados']} actualizados · {resultado['sin_ventas']} sin ventas eliminados",
                    color="positive", timeout=4000
                )
                _recargar_tablas()
            from datetime import datetime, timedelta
            ahora = (datetime.utcnow() - timedelta(hours=3)).strftime("%d/%m/%y — %H:%M")
            ultima_act_ref[0].set_text(f"Últ. act: {ahora}")

        with right_col:
            # 1. Input catálogo + lupa
            with ui.element("div").style("display:flex;gap:0;width:100%"):
                inp = ui.input(placeholder="Link de una publicación de catálogo...").props(
                    "dense outlined"
                ).style("width:100%;font-size:11px;border-radius:4px 0 0 4px")
                with ui.element("button").on(
                    "click", lambda: ui.timer(0.05, lambda: _buscar(inp.value), once=True)
                ).style(
                    "height:34px;padding:0 10px;background:#2A7AC7;color:#fff;"
                    "border:none;border-radius:0 4px 4px 0;font-size:12px;cursor:pointer;flex-shrink:0"
                ):
                    ui.html('<i class="ti ti-search" style="font-size:13px;color:#fff"></i>')

            # 2. Input buscador de competidor
            filtro_input = ui.input(placeholder="Buscar competidor en las tablas...").props(
                "dense outlined clearable"
            ).style("width:100%;font-size:11px")

            def _on_filtro(e):
                filtro_ref[0]["texto"] = (e.value or "").strip()
                _recargar_tablas()
            filtro_input.on_value_change(_on_filtro)

            # 3. Boton actualizar + fecha
            with ui.element("div").style("display:flex;flex-direction:column;align-items:flex-start;width:100%"):
                with ui.button(on_click=_lanzar_actualizacion).props(
                    "unelevated no-caps"
                ).style(
                    "background:#185FA5;color:#fff;height:34px;border-radius:4px;width:100%"
                ).tooltip("Actualizar ventas históricas"):
                    ui.html('''
                        <i class="ti ti-refresh" style="font-size:14px"></i>
                        <span style="margin-left:4px;font-size:11px">Actualizar ventas</span>
                    ''')
                lbl_ultima = ui.label(f"Últ. act: {_get_ultima_actualizacion(uid)}").style(
                    "font-size:9px;color:#9ca3af;margin-top:2px;white-space:nowrap"
                )
                ultima_act_ref[0] = lbl_ultima

        notif = ui.label("").style("font-size:10px;color:#9ca3af")
        notif_ref[0] = notif
        resultado_area = ui.element("div").style("margin-top:4px")

        # 5 tablas — spinner inmediato, datos en background
        tablas = ui.element("div").classes("comp-tablas").style("display:flex;gap:8px;align-items:flex-start")
        tablas_ref[0] = tablas
        with tablas:
            with ui.element("div").style(
                "width:100%;display:flex;flex-direction:column;align-items:center;"
                "justify-content:center;gap:16px;padding:60px 0"
            ):
                ui.html('''
                    <div style="display:flex;gap:8px;align-items:center">
                        <i class="ti ti-users" style="font-size:28px;color:#2A7AC7"></i>
                        <span style="font-size:16px;font-weight:500;color:#185FA5">Competidores</span>
                    </div>
                ''')
                ui.spinner(size="lg", color="#2A7AC7")
                ui.label("Cargando ranking...").style("font-size:12px;color:#9ca3af")

        async def _cargar_tablas():
            all_data = []
            for titulo, dias, nota in PERIODOS:
                rows = await run.io_bound(_get_ranking_global, uid, dias)
                all_data.append((titulo, dias, nota, rows))
            tablas.clear()
            with tablas:
                for titulo, dias, nota, rows in all_data:
                    _render_tabla(rows, mis_ids, titulo, nota, filtro_ref, _on_click_nick)

        from nicegui import background_tasks
        background_tasks.create(_cargar_tablas(), name="comp_load")
