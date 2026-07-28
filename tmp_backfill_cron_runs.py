"""
tmp_backfill_cron_runs.py
Poblado inicial (una sola vez) de cron_runs a partir de la historia ya guardada en
ml_stock_snapshots y competidores_snapshots, para que el panel del Dashboard y la
página ADMIN > Log no arranquen vacíos.

Solo backfillea días ANTERIORES a hoy (no toca "hoy"): así no pisa una corrida real
del cron instrumentado si ya corrió en el día del deploy.

Uso: python3 /opt/pythonml/tmp_backfill_cron_runs.py [dias=14]
"""
import sys
from datetime import date, timedelta

from db import get_connection, init_cron_runs_db

DIAS = int(sys.argv[1]) if len(sys.argv) > 1 else 14


def _account_ids() -> list[int]:
    conn = get_connection()
    try:
        rows = conn.execute("SELECT DISTINCT user_id FROM ml_credentials").fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


def _counts_by_day(table: str, desde: str, hasta: str) -> dict[tuple[int, str], int]:
    conn = get_connection()
    try:
        rows = conn.execute(
            f"SELECT user_id, snapshot_date, COUNT(*) FROM {table}"
            f" WHERE snapshot_date >= ? AND snapshot_date < ? GROUP BY user_id, snapshot_date",
            (desde, hasta)
        ).fetchall()
        return {(r[0], r[1]): r[2] for r in rows}
    finally:
        conn.close()


def main() -> None:
    init_cron_runs_db()

    hoy = date.today()
    desde = (hoy - timedelta(days=DIAS)).isoformat()
    hasta = hoy.isoformat()  # excluyente: no toca hoy

    user_ids = _account_ids()
    dias_list = [(hoy - timedelta(days=i)).isoformat() for i in range(1, DIAS + 1)]

    jobs = {
        "stock": _counts_by_day("ml_stock_snapshots", desde, hasta),
        "competidores": _counts_by_day("competidores_snapshots", desde, hasta),
    }

    conn = get_connection()
    inserted = 0
    try:
        for job, counts in jobs.items():
            for uid in user_ids:
                for d in dias_list:
                    cnt = counts.get((uid, d))
                    if cnt:
                        status, count, error = "ok", cnt, None
                    else:
                        status, count, error = "fail", 0, "Sin snapshot ese día (backfill)"
                    conn.execute(
                        """
                        INSERT INTO cron_runs (job, user_id, run_date, run_datetime, status, count, duration_seconds, error)
                        VALUES (?, ?, ?, ?, ?, ?, NULL, ?)
                        ON CONFLICT(job, user_id, run_date) DO NOTHING
                        """,
                        (job, uid, d, f"{d}T00:00:00", status, count, error),
                    )
                    inserted += 1
        conn.commit()
    finally:
        conn.close()

    print(f"Backfill completado: {inserted} filas evaluadas ({len(user_ids)} cuentas x {DIAS} dias x 2 jobs).")


if __name__ == "__main__":
    main()
