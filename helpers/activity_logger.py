from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from db import get_connection


def log_event(
    user_id: int,
    tab: str,
    accion: str,
    detalle: Optional[str] = None,
    tiempo_segundos: Optional[int] = None,
) -> None:
    try:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT username FROM users WHERE id = ?", (user_id,))
            row = cur.fetchone()
            usuario = row["username"] if row else str(user_id)
            cur.execute(
                "SELECT ml_nickname FROM ml_credentials WHERE user_id = ? ORDER BY id DESC LIMIT 1",
                (user_id,),
            )
            ml_row = cur.fetchone()
            ml_username = ml_row["ml_nickname"] if ml_row and ml_row["ml_nickname"] else None
            cur.execute(
                "INSERT INTO activity_log "
                "(usuario, ml_username, tab, accion, detalle, tiempo_segundos, timestamp) "
                "VALUES (?,?,?,?,?,?,?)",
                (
                    usuario,
                    ml_username,
                    tab,
                    accion,
                    detalle,
                    tiempo_segundos,
                    datetime.now().isoformat(timespec="seconds"),
                ),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        # No propagar: el usuario que disparó la acción auditada no tiene por qué
        # ver romperse su operación porque el logger de auditoría falló. Pero el
        # fallo tiene que quedar visible para el operador (stderr/journald), porque
        # este es el propio mecanismo que permitiría detectar que algo más se rompió.
        logging.exception("[ACTIVITY_LOGGER] log_event falló (user_id=%s, tab=%s, accion=%s)", user_id, tab, accion)
