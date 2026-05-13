#!/usr/bin/env python3
"""
Migración one-shot: encripta client_secret en texto plano con Fernet.

Requisitos:
  - CREDENTIAL_ENCRYPTION_KEY configurado en .env (o variable de entorno)
  - cryptography instalado: pip install cryptography

Ejecutar UNA SOLA VEZ en el servidor antes de hacer deploy del código nuevo:
  py migrate_secrets.py

Es idempotente: filas ya encriptadas (prefijo "gAAAAA") no se tocan.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

# Cargar .env antes de importar main para que CREDENTIAL_ENCRYPTION_KEY esté disponible
from dotenv import load_dotenv
load_dotenv()

# Importar helpers desde main (reutiliza DB_PATH, _decrypt_secret, _encrypt_secret)
import main


def migrate_table(conn: sqlite3.Connection, table: str) -> int:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(f"SELECT user_id, client_secret FROM {table}")
    rows = cur.fetchall()
    migrated = 0
    for row in rows:
        secret = row["client_secret"] or ""
        if secret.startswith("gAAAAA"):
            continue  # ya encriptado
        if not secret:
            continue  # vacío, nada que hacer
        enc = main._encrypt_secret(secret)
        cur.execute(f"UPDATE {table} SET client_secret = ? WHERE user_id = ?", (enc, row["user_id"]))
        migrated += 1
    conn.commit()
    return migrated


def main_migrate() -> None:
    db_path = main.DB_PATH
    if not Path(db_path).exists():
        print(f"ERROR: No se encontró la BD en {db_path}")
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        ml_count = migrate_table(conn, "ml_app_credentials")
        qb_count = migrate_table(conn, "qb_app_credentials")
    finally:
        conn.close()

    print(f"ml_app_credentials: {ml_count} fila(s) migrada(s)")
    print(f"qb_app_credentials: {qb_count} fila(s) migrada(s)")
    print("Migración completada.")


if __name__ == "__main__":
    main_migrate()
