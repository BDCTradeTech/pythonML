"""
backfill_marcas.py — Backfill one-off de productos.marca usando el atributo BRAND/MARCA
de MercadoLibre, para las filas que quedaron con marca=NULL (previo al auto-populate
agregado en tabs/precios.py). Solo alcanza a productos cuyo item_id se puede resolver
vía el último snapshot en ml_stock_snapshots (status=active del cron de las 3am) — los
que no tengan snapshot quedan reportados aparte y no se tocan.

Uso: cd /opt/pythonml && ./venv/bin/python3 backfill_marcas.py
"""
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

from db import get_connection, get_marca_override_map
from ml_api import get_ml_access_token, ml_get_items_multiget_with_attributes, _parse_ml_item_body


def _productos_sin_marca(user_id: int):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM productos WHERE user_id=? AND (marca IS NULL OR marca='')",
            (user_id,),
        )
        total_sin_marca = cur.fetchone()[0]
        cur.execute(
            """
            SELECT p.sku, s.item_id
            FROM productos p
            JOIN (
                SELECT user_id, seller_sku, item_id,
                       ROW_NUMBER() OVER (PARTITION BY user_id, seller_sku ORDER BY snapshot_date DESC) rn
                FROM ml_stock_snapshots
            ) s ON s.user_id = p.user_id AND s.seller_sku = p.sku AND s.rn = 1
            WHERE p.user_id = ? AND (p.marca IS NULL OR p.marca = '')
            """,
            (user_id,),
        )
        pares = [(r["sku"], r["item_id"]) for r in cur.fetchall()]
        return total_sin_marca, pares
    finally:
        conn.close()


def _update_marca(user_id: int, sku: str, marca: str) -> None:
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE productos SET marca=?, updated_at=datetime('now') WHERE sku=? AND user_id=?",
            (marca, sku, user_id),
        )
        conn.commit()
    finally:
        conn.close()


def backfill_user(user_id: int) -> None:
    total_sin_marca, pares = _productos_sin_marca(user_id)
    sin_item_id = total_sin_marca - len(pares)
    print(f"\n=== user_id={user_id} ===")
    print(f"  Sin marca: {total_sin_marca} | con item_id resuelto: {len(pares)} | sin item_id: {sin_item_id}")
    if not pares:
        return
    token = get_ml_access_token(user_id)
    if not token:
        print(f"  ERROR: sin token ML para user_id={user_id}, salteo este usuario")
        return
    marca_override = get_marca_override_map(user_id)

    actualizados = fallidos = sin_marca_en_ml = 0
    item_ids = [iid for _, iid in pares]
    sku_by_item = {iid: sku for sku, iid in pares}

    for i in range(0, len(item_ids), 20):
        batch = item_ids[i:i + 20]
        try:
            bodies = ml_get_items_multiget_with_attributes(token, batch)
        except Exception as e:
            print(f"  ERROR batch offset={i}: {e}")
            fallidos += len(batch)
            continue
        # No asumir que `bodies` viene en el mismo orden que `batch` -- verificado en vivo
        # (2026-08-24) que ML puede devolver /items?ids= reordenado respecto del pedido.
        # Indexar por el "id" real de cada body devuelto, nunca por posición.
        bodies_by_id = {str(b["id"]): b for b in bodies if b and b.get("id")}
        for iid in batch:
            sku = sku_by_item.get(iid)
            if not sku:
                continue
            try:
                body = bodies_by_id.get(iid)
                if body is None:
                    fallidos += 1
                    continue
                marca = (_parse_ml_item_body(body) or {}).get("marca")
                if not marca or marca == "—":
                    sin_marca_en_ml += 1
                    continue
                marca = marca_override.get(marca, marca)
                _update_marca(user_id, sku, marca)
                actualizados += 1
            except Exception as e:
                print(f"  ERROR sku={sku}: {e}")
                fallidos += 1

    print(f"  Actualizados: {actualizados}")
    print(f"  Fallidos: {fallidos}")
    print(f"  Sin marca en ML (BRAND vacío): {sin_marca_en_ml}")
    print(f"  Sin item_id (no intentado): {sin_item_id}")


if __name__ == "__main__":
    conn = get_connection()
    uids = [
        r[0] for r in conn.execute(
            "SELECT DISTINCT user_id FROM productos WHERE marca IS NULL OR marca=''"
        ).fetchall()
    ]
    conn.close()
    for uid in uids:
        backfill_user(uid)
