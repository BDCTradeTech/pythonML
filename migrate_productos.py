"""
Script de migración: precios_producto → productos + ml_publicaciones
Ejecutar una sola vez con: py -3 migrate_productos.py
"""
import sqlite3
import requests
import time
from datetime import datetime

DB_PATH = "app.db"
NOW = datetime.now().isoformat()

# ─── helpers ────────────────────────────────────────────────────────────────

def get_con():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def get_token(con, user_id):
    cur = con.cursor()
    cur.execute("SELECT access_token FROM ml_credentials WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    return row["access_token"] if row else None

def refresh_token(con, user_id):
    cur = con.cursor()
    cur.execute("SELECT refresh_token FROM ml_credentials WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    if not row:
        return None
    cur.execute("SELECT client_id, client_secret FROM ml_app_credentials WHERE user_id = ?", (user_id,))
    creds = cur.fetchone()
    if not creds:
        return None
    resp = requests.post("https://api.mercadolibre.com/oauth/token", data={
        "grant_type": "refresh_token",
        "client_id": creds["client_id"],
        "client_secret": creds["client_secret"],
        "refresh_token": row["refresh_token"],
    }, timeout=15)
    if resp.status_code == 200:
        data = resp.json()
        new_token = data.get("access_token")
        new_refresh = data.get("refresh_token")
        if new_token:
            con.execute(
                "UPDATE ml_credentials SET access_token=?, refresh_token=? WHERE user_id=?",
                (new_token, new_refresh or row["refresh_token"], user_id)
            )
            con.commit()
            print(f"  [token] user_id={user_id} refrescado OK")
            return new_token
    print(f"  [token] user_id={user_id} refresh FALLÓ: {resp.status_code} {resp.text[:200]}")
    return None

def ml_batch(ids, token):
    """Consulta hasta 20 IDs. Devuelve lista de dicts con los cuerpos."""
    url = "https://api.mercadolibre.com/items"
    params = {"ids": ",".join(ids), "attributes": "id,title,seller_custom_field,attributes,variations,status,price,available_quantity,catalog_listing,listing_type_id,sold_quantity"}
    resp = requests.get(url, params=params, headers={"Authorization": f"Bearer {token}"}, timeout=20)
    if resp.status_code == 401:
        return None, 401
    if resp.status_code != 200:
        return [], resp.status_code
    results = resp.json()
    bodies = []
    for r in results:
        code = r.get("code", 200)
        body = r.get("body") or {}
        bodies.append((r.get("code", 200), body))
    return bodies, 200

def extract_sku(body):
    """Extrae seller_sku del body de un item ML."""
    # 1. seller_custom_field directo
    scf = body.get("seller_custom_field")
    if scf:
        return str(scf).strip()
    # 2. Atributo SELLER_SKU
    for attr in (body.get("attributes") or []):
        if attr.get("id") == "SELLER_SKU":
            v = attr.get("value_name") or attr.get("values", [{}])[0].get("name") if attr.get("values") else None
            if v:
                return str(v).strip()
    # 3. Variaciones
    for var in (body.get("variations") or []):
        for attr in (var.get("attributes") or []):
            if attr.get("id") == "SELLER_SKU":
                v = attr.get("value_name")
                if v:
                    return str(v).strip()
    return None

def extract_attr(body, attr_id):
    for attr in (body.get("attributes") or []):
        if attr.get("id") == attr_id:
            v = attr.get("value_name")
            if not v and attr.get("values"):
                v = attr["values"][0].get("name")
            return str(v).strip() if v else None
    return None

# ─── main ────────────────────────────────────────────────────────────────────

con = get_con()

# Crear tablas si no existen
con.execute("""
CREATE TABLE IF NOT EXISTS productos (
    sku          TEXT NOT NULL,
    user_id      INTEGER NOT NULL,
    marca        TEXT,
    nombre       TEXT,
    color        TEXT,
    costo_usd    REAL,
    tipo_iva     REAL DEFAULT 0.105,
    notas        TEXT,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL,
    costo_updated_at TEXT,
    PRIMARY KEY (sku, user_id),
    FOREIGN KEY (user_id) REFERENCES users(id)
)""")
con.execute("""
CREATE TABLE IF NOT EXISTS ml_publicaciones (
    ml_id           TEXT NOT NULL,
    user_id         INTEGER NOT NULL,
    sku             TEXT,
    titulo          TEXT,
    precio          REAL,
    stock           INTEGER,
    estado          TEXT,
    catalog_listing INTEGER DEFAULT 0,
    listing_type_id TEXT,
    sold_quantity   INTEGER,
    ultima_sync     TEXT,
    PRIMARY KEY (ml_id, user_id),
    FOREIGN KEY (user_id) REFERENCES users(id)
)""")
con.commit()
print("Tablas OK")

# Leer precios_producto
rows = con.execute("SELECT id AS ml_id, user_id, tipo_iva, costo_u FROM precios_producto ORDER BY user_id, id").fetchall()
print(f"precios_producto: {len(rows)} filas")

# Tokens por usuario
user_ids = sorted({r["user_id"] for r in rows})
tokens = {}
for uid in user_ids:
    t = get_token(con, uid)
    # probar token
    test = requests.get(
        "https://api.mercadolibre.com/users/me",
        headers={"Authorization": f"Bearer {t}"},
        timeout=10
    )
    if test.status_code == 401:
        print(f"  user_id={uid}: token expirado, refrescando...")
        t = refresh_token(con, uid)
    else:
        print(f"  user_id={uid}: token OK ({test.status_code})")
    tokens[uid] = t

# Agrupar por user_id
from collections import defaultdict
by_user = defaultdict(list)
for r in rows:
    by_user[r["user_id"]].append(r)

# Procesar
productos_ins = []   # (sku, user_id, marca, nombre, color, costo_usd, tipo_iva)
publis_ins = []      # (ml_id, user_id, sku, titulo, precio, stock, estado, catalog_listing, listing_type_id, sold_quantity)

BATCH = 20

for uid, uid_rows in by_user.items():
    token = tokens.get(uid)
    if not token:
        print(f"\nuser_id={uid}: sin token, usando ml_id como sku provisional para {len(uid_rows)} items")
        for r in uid_rows:
            productos_ins.append((r["ml_id"], uid, None, None, None, float(r["costo_u"]) if r["costo_u"] else None, r["tipo_iva"]))
            publis_ins.append((r["ml_id"], uid, r["ml_id"], None, None, None, None, 0, None, None))
        continue

    print(f"\nuser_id={uid}: {len(uid_rows)} items")
    ids = [r["ml_id"] for r in uid_rows]
    costo_map = {r["ml_id"]: (r["costo_u"], r["tipo_iva"]) for r in uid_rows}

    for i in range(0, len(ids), BATCH):
        batch_ids = ids[i:i+BATCH]
        bodies, status = ml_batch(batch_ids, token)
        if status == 401:
            print(f"  lote {i//BATCH+1}: 401, refrescando token...")
            token = refresh_token(con, uid)
            if token:
                tokens[uid] = token
                bodies, status = ml_batch(batch_ids, token)
            else:
                print(f"  lote {i//BATCH+1}: sin token, usando ml_id provisional")
                for mid in batch_ids:
                    cu, tiva = costo_map[mid]
                    productos_ins.append((mid, uid, None, None, None, float(cu) if cu else None, tiva))
                    publis_ins.append((mid, uid, mid, None, None, None, None, 0, None, None))
                continue
        if bodies is None or status not in (200,):
            print(f"  lote {i//BATCH+1}: error {status}")
            for mid in batch_ids:
                cu, tiva = costo_map[mid]
                productos_ins.append((mid, uid, None, None, None, float(cu) if cu else None, tiva))
                publis_ins.append((mid, uid, mid, None, None, None, None, 0, None, None))
            continue

        # No asumir que `bodies` viene en el mismo orden que `batch_ids` -- verificado en vivo
        # (2026-08-24) que ML puede devolver /items?ids= reordenado respecto del pedido. Indexar
        # por el "id" real de cada body devuelto y recorrer batch_ids (no bodies) para que un
        # item sin id en la respuesta (403, etc.) quede con el mid CORRECTO igual, sin adivinar
        # por posición.
        bodies_by_id = {str(b.get("id")): (c, b) for c, b in bodies if b and b.get("id")}
        for mid in batch_ids:
            cu, tiva = costo_map.get(mid, (None, 0.105))
            entry = bodies_by_id.get(mid)
            if entry is None:
                print(f"    {mid}: sin datos en la respuesta — sku provisional")
                productos_ins.append((mid, uid, None, None, None, float(cu) if cu else None, tiva))
                publis_ins.append((mid, uid, mid, None, None, None, None, 0, None, None))
                continue
            code, body = entry
            if code == 403:
                print(f"    {mid}: HTTP {code} — sku provisional")
                productos_ins.append((mid, uid, None, None, None, float(cu) if cu else None, tiva))
                publis_ins.append((mid, uid, mid, None, None, None, None, 0, None, None))
                continue

            sku = extract_sku(body) or mid
            marca = extract_attr(body, "BRAND")
            color = extract_attr(body, "COLOR")
            titulo = body.get("title")
            precio = body.get("price")
            stock = body.get("available_quantity")
            estado = body.get("status")
            catalog = 1 if body.get("catalog_listing") else 0
            listing_type = body.get("listing_type_id")
            sold = body.get("sold_quantity")

            flag = "" if extract_sku(body) else " [sin SKU->ml_id]"
            _line = f"    {mid}: sku={sku}{flag}, marca={marca}, color={color}"
            print(_line.encode("ascii", "replace").decode("ascii"))

            productos_ins.append((sku, uid, marca, titulo, color, float(cu) if cu else None, tiva))
            publis_ins.append((mid, uid, sku, titulo, precio, stock, estado, catalog, listing_type, sold))

        time.sleep(0.15)

# ─── INSERT ──────────────────────────────────────────────────────────────────

print(f"\nInsertando {len(productos_ins)} filas en productos...")
ok_prod = 0
skip_prod = 0
for (sku, uid, marca, nombre, color, costo_usd, tipo_iva) in productos_ins:
    try:
        con.execute(
            """INSERT OR IGNORE INTO productos
               (sku, user_id, marca, nombre, color, costo_usd, tipo_iva, created_at, updated_at, costo_updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (sku, uid, marca, nombre, color, costo_usd, tipo_iva, NOW, NOW, NOW)
        )
        ok_prod += 1
    except Exception as e:
        print(f"  ERROR productos sku={sku} uid={uid}: {e}")
        skip_prod += 1

print(f"Insertando {len(publis_ins)} filas en ml_publicaciones...")
ok_pub = 0
skip_pub = 0
for (ml_id, uid, sku, titulo, precio, stock, estado, catalog, listing_type, sold) in publis_ins:
    try:
        con.execute(
            """INSERT OR IGNORE INTO ml_publicaciones
               (ml_id, user_id, sku, titulo, precio, stock, estado, catalog_listing, listing_type_id, sold_quantity, ultima_sync)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (ml_id, uid, sku, titulo, precio, stock, estado, catalog, listing_type, sold, NOW)
        )
        ok_pub += 1
    except Exception as e:
        print(f"  ERROR ml_publicaciones ml_id={ml_id}: {e}")
        skip_pub += 1

con.commit()
print(f"\nproductos:       {ok_prod} insertados, {skip_prod} errores")
print(f"ml_publicaciones:{ok_pub} insertados, {skip_pub} errores")

# ─── VERIFICACIÓN ────────────────────────────────────────────────────────────

print("\n── VERIFICACIÓN ──")
for uid in user_ids:
    n_prod = con.execute("SELECT COUNT(*) FROM productos WHERE user_id=?", (uid,)).fetchone()[0]
    n_pub  = con.execute("SELECT COUNT(*) FROM ml_publicaciones WHERE user_id=?", (uid,)).fetchone()[0]
    print(f"user_id={uid}: productos={n_prod}, ml_publicaciones={n_pub}")

print("\nEjemplos (primeras 5 filas de productos con costo):")
for r in con.execute("SELECT sku, user_id, marca, nombre, costo_usd, tipo_iva FROM productos WHERE costo_usd IS NOT NULL LIMIT 5").fetchall():
    print(f"  sku={r['sku']} uid={r['user_id']} marca={r['marca']} costo={r['costo_usd']} iva={r['tipo_iva']}")

print("\nEjemplos (primeras 5 filas de ml_publicaciones):")
for r in con.execute("SELECT ml_id, user_id, sku, titulo, stock FROM ml_publicaciones LIMIT 5").fetchall():
    print(f"  ml_id={r['ml_id']} uid={r['user_id']} sku={r['sku']} stock={r['stock']} titulo={str(r['titulo'])[:40]}")

# Verificar que no se perdió ningún costo
n_pp = con.execute("SELECT COUNT(*) FROM precios_producto WHERE costo_u > 0").fetchone()[0]
n_pr = con.execute("SELECT COUNT(*) FROM productos WHERE costo_usd > 0").fetchone()[0]
print(f"\nCostos en precios_producto (costo_u>0): {n_pp}")
print(f"Costos migrados en productos  (costo_usd>0): {n_pr}")
if n_pr >= n_pp:
    print("OK: ningún costo perdido")
else:
    print(f"ADVERTENCIA: {n_pp - n_pr} costos no migrados")

con.close()
print("\nMigración completada.")
