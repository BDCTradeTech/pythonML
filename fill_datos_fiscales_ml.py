"""
Script one-off: carga datos fiscales de MercadoLibre (razón social, nombre de
fantasía, CUIT, tipo de doc) en users, usando el token propio de cada usuario
conectado. Se llena una sola vez; si hace falta refrescar en el futuro, se
re-corre este mismo script.

Ejecutar: python3 fill_datos_fiscales_ml.py
"""
from datetime import datetime, timezone

from db import get_connection
from ml_api import get_ml_access_token, get_ml_session


def main():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT user_id FROM ml_credentials")
    user_ids = [r["user_id"] for r in cur.fetchall()]

    ok = []
    failed = []

    for uid in user_ids:
        try:
            token = get_ml_access_token(uid)
            if not token:
                failed.append((uid, "sin access_token válido (token/refresh ausente)"))
                continue

            resp = get_ml_session().get(
                "https://api.mercadolibre.com/users/me",
                headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()

            identification = data.get("identification") or {}
            company = data.get("company") or {}

            cuit = identification.get("number") or company.get("identification")
            doc_type = identification.get("type")
            razon_social = company.get("corporate_name")
            nombre_fantasia = company.get("brand_name")
            cust_type_id = company.get("cust_type_id")

            cur.execute(
                """
                UPDATE users
                SET ml_cuit = ?, ml_doc_type = ?, ml_razon_social = ?,
                    ml_nombre_fantasia = ?, ml_cust_type_id = ?, ml_billing_updated_at = ?
                WHERE id = ?
                """,
                (
                    cuit, doc_type, razon_social, nombre_fantasia, cust_type_id,
                    datetime.now(timezone.utc).isoformat(),
                    uid,
                ),
            )
            ok.append((uid, razon_social, nombre_fantasia, cuit))
        except Exception as e:
            failed.append((uid, str(e)))

    conn.commit()

    print(f"\nOK: {len(ok)} / fallidos: {len(failed)}\n")
    print(f"{'user_id':<10}{'razón social':<35}{'nombre fantasía':<25}{'CUIT':<15}")
    for uid, razon, fantasia, cuit in ok:
        print(f"{uid:<10}{(razon or ''):<35}{(fantasia or ''):<25}{(cuit or ''):<15}")

    if failed:
        print("\nFallidos:")
        for uid, err in failed:
            print(f"  user_id {uid}: {err}")


if __name__ == "__main__":
    main()
