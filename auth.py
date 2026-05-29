from __future__ import annotations

import hashlib
import logging
import os
import secrets
import smtplib
import socket
import sqlite3
import ssl
import threading
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, Optional

import bcrypt

from db import get_connection

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes de tabs (espejadas de main.py para que create_user funcione
# de forma autónoma; en fases futuras se importarán desde un módulo central)
# ---------------------------------------------------------------------------
TAB_KEYS = [
    ("home", "Home"),
    ("estadisticas", "Estadísticas"),
    ("ventas", "Ventas"),
    ("productos", "Productos"),
    ("cuotas", "Cuotas"),
    ("busqueda", "Busquedas"),
    ("balance", "Balance"),
    ("compras", "Invoices"),
    ("stock", "Stock"),
    ("compras_lista", "Compras"),
    ("pedidos", "Pedidos"),
    ("historicos", "Históricos"),
    ("importacion", "Importacion"),
    ("pesos", "Pesos"),
    ("datos", "Datos"),
    ("configuracion", "Configuración"),
    ("admin", "Admin"),
]
TABS_BASE = {"home", "pedidos", "importacion", "pesos", "datos", "configuracion"}

# Lock global para envío de emails (evita condiciones de carrera con socket)
_email_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Contraseñas
# ---------------------------------------------------------------------------

def hash_password(password: str) -> str:
    """Genera hash bcrypt. Usar solo para passwords NUEVOS."""
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def _is_bcrypt_hash(h: str) -> bool:
    return h.startswith(("$2b$", "$2a$", "$2y$"))


def _verify_password(password: str, stored_hash: str) -> bool:
    """Verifica contra hash bcrypt o SHA-256 legacy."""
    pw_bytes = password.encode("utf-8")
    if _is_bcrypt_hash(stored_hash):
        try:
            return bcrypt.checkpw(pw_bytes, stored_hash.encode("utf-8"))
        except Exception:
            return False
    return hashlib.sha256(pw_bytes).hexdigest() == stored_hash


# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------

def send_email(to_email: str, subject: str, body_plain: str) -> Optional[str]:
    """Envía un email vía SMTP. Devuelve None si OK, mensaje de error si falla.
    Variables: SMTP_HOST, SMTP_PORT (465=SSL, 587=STARTTLS), SMTP_USER, SMTP_PASS, SMTP_FROM, SMTP_FROM_NAME"""
    host = os.getenv("SMTP_HOST", "").strip()
    if not host:
        return "SMTP no configurado: falta SMTP_HOST en .env. Copiá .env.example a .env y completá las variables (ver README)."
    port = int(os.getenv("SMTP_PORT", "465"))
    user = os.getenv("SMTP_USER", "").strip()
    password_env = os.getenv("SMTP_PASS", "").strip().replace(" ", "")
    from_addr_raw = (os.getenv("SMTP_FROM", "") or user).strip() or user
    from_name = (os.getenv("SMTP_FROM_NAME", "") or "BDC systems").strip()
    from_header = f"{from_name} <{from_addr_raw}>" if from_name else from_addr_raw

    if not user or not password_env:
        return "SMTP no configurado: falta SMTP_USER o SMTP_PASS en .env. Copiá .env.example a .env y completá las variables."

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = from_header
        msg["To"] = to_email
        msg.attach(MIMEText(body_plain, "plain", "utf-8"))

        ctx = ssl.create_default_context()
        envelope_from = user

        with _email_lock:
            _orig_getaddrinfo = socket.getaddrinfo

            def _ipv4_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
                return _orig_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)

            socket.getaddrinfo = _ipv4_getaddrinfo
            try:
                if port == 465:
                    with smtplib.SMTP_SSL(host, port, context=ctx, timeout=30) as smtp:
                        smtp.login(user, password_env)
                        smtp.sendmail(envelope_from, to_email, msg.as_string())
                else:
                    with smtplib.SMTP(host, port, timeout=30) as smtp:
                        smtp.starttls(context=ctx)
                        smtp.login(user, password_env)
                        smtp.sendmail(envelope_from, to_email, msg.as_string())
            finally:
                socket.getaddrinfo = _orig_getaddrinfo
        return None
    except Exception as e:
        return f"Error al enviar email: {str(e)}"


# ---------------------------------------------------------------------------
# Usuarios
# ---------------------------------------------------------------------------

def get_user_email(user_id: int) -> Optional[str]:
    """Obtiene el email del usuario. Si no tiene, intenta usar username si parece email."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT email, username FROM users WHERE id = ?", (user_id,))
        row = cur.fetchone()
        if not row:
            return None
        email = (row["email"] or "").strip() if row["email"] else ""
        if email:
            return email
        uname = (row["username"] or "").strip()
        return uname if "@" in uname else None
    finally:
        conn.close()


def create_user(email: str) -> tuple[Optional[str], Optional[str]]:
    """Crea un usuario. Devuelve (mensaje_error, contraseña_si_email_falló). Si OK, (None, None)."""
    email_clean = (email or "").strip().lower()
    if not email_clean or "@" not in email_clean:
        return ("Ingresá un email válido.", None)
    try:
        new_password = secrets.token_urlsafe(8)
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT id FROM users ORDER BY id LIMIT 1")
        first_user = cur.fetchone()
        is_first = first_user is None

        cur.execute(
            "INSERT INTO users (username, password_hash, created_at, email) VALUES (?, ?, ?, ?)",
            (email_clean, hash_password(new_password), datetime.now(timezone.utc).replace(tzinfo=None).isoformat(), email_clean),
        )
        conn.commit()
        uid = cur.lastrowid
        if uid:
            for tab_key, _ in TAB_KEYS:
                can = 1 if tab_key in TABS_BASE or (tab_key == "admin" and is_first) else 0
                cur.execute(
                    "INSERT OR IGNORE INTO user_tab_permissions (user_id, tab_key, can_access) VALUES (?, ?, ?)",
                    (uid, tab_key, can),
                )
            conn.commit()

        # Enviar email con contraseña provisoria
        body = f"""Hola,

Te registraste en BDC systems. Tu contraseña provisoria es: {new_password}

Usuario (email): {email_clean}

Por favor iniciá sesión con tu email y contraseña provisoria, y cambiá tu contraseña en Configuración > Cambiar contraseña.
"""
        err = send_email(
            email_clean,
            "BDC systems - Tu contraseña provisoria",
            body,
        )
        if err:
            return (f"No se pudo enviar el email: {err}", new_password)
        return (None, None)
    except sqlite3.IntegrityError:
        return ("El usuario ya existe.", None)
    finally:
        conn.close()


def authenticate_user(username: str, password: str) -> Optional[Dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE username = ?", (username,))
        row = cur.fetchone()
        if not row:
            return None
        stored = row["password_hash"]
        if not _verify_password(password, stored):
            return None
        # Lazy migration: upgrade SHA-256 hash to bcrypt on successful login
        if not _is_bcrypt_hash(stored):
            new_hash = hash_password(password)
            cur.execute("UPDATE users SET password_hash = ? WHERE username = ?", (new_hash, username))
            conn.commit()
        result = dict(row)
        result.pop("password_hash", None)
        return result
    finally:
        conn.close()


def update_user_password(user_id: int, current_password: str, new_password: str) -> Optional[str]:
    """Actualiza la contraseña del usuario. Devuelve mensaje de error o None si fue bien."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT password_hash FROM users WHERE id = ?", (user_id,))
        row = cur.fetchone()
        if not row:
            return "Usuario no encontrado."
        if not _verify_password(current_password, row["password_hash"]):
            return "Contraseña actual incorrecta."
        new_clean = (new_password or "").strip()
        if len(new_clean) < 4:
            return "La nueva contraseña debe tener al menos 4 caracteres."
        cur.execute("UPDATE users SET password_hash = ? WHERE id = ?", (hash_password(new_clean), user_id))
        conn.commit()
        return None
    finally:
        conn.close()


def admin_reset_user_password(target_user_id: int) -> tuple[Optional[str], bool, Optional[str], Optional[str]]:
    """Genera una nueva contraseña temporal, la guarda y la envía por email al usuario.
    Devuelve (mensaje_error, email_enviado, email_destino, nueva_contraseña).
    La contraseña se devuelve solo cuando el email falla, para mostrarla en un popup."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, username FROM users WHERE id = ?", (target_user_id,))
        row = cur.fetchone()
        if not row:
            return ("Usuario no encontrado.", False, None, None)
        to_email = get_user_email(target_user_id)
        if not to_email:
            return ("El usuario no tiene email registrado.", False, None, None)

        new_password = secrets.token_urlsafe(8)

        cur.execute("UPDATE users SET password_hash = ? WHERE id = ?", (hash_password(new_password), target_user_id))
        conn.commit()

        body = f"""Hola,

Un administrador reinició tu contraseña en BDC systems.

Usuario: {row['username']}
Nueva contraseña: {new_password}

Por favor iniciá sesión y cambiá tu contraseña en Configuración > Cambiar contraseña.
"""
        err = send_email(to_email, "BDC systems - Tu nueva contraseña", body)
        if err is None:
            return (None, True, to_email, None)
        return (f"No se pudo enviar el email: {err}", False, to_email, new_password)
    finally:
        conn.close()


def delete_user_and_all_data(target_user_id: int) -> Optional[str]:
    """Elimina un usuario y todos sus datos asociados. Devuelve mensaje de error o None si OK."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM users WHERE id = ?", (target_user_id,))
        if not cur.fetchone():
            return "Usuario no encontrado."
        # Borrar en orden: tablas que referencian user_id
        tables = [
            "ml_credentials",
            "qb_tokens",
            "ml_app_credentials",
            "qb_app_credentials",
            "user_qb_customer",
            "queries",
            "cotizador_datos",
            "ml_publicaciones",
            "productos",
            "importacion_filas",
            "user_tab_permissions",
        ]
        ALLOWED_USER_TABLES = {
            "ml_credentials", "qb_tokens", "ml_app_credentials",
            "qb_app_credentials", "user_qb_customer", "queries",
            "cotizador_datos", "ml_publicaciones", "productos",
            "importacion_filas", "user_tab_permissions",
        }
        for t in tables:
            if t not in ALLOWED_USER_TABLES:
                continue
            try:
                cur.execute(f"DELETE FROM {t} WHERE user_id = ?", (target_user_id,))
            except sqlite3.OperationalError:
                pass  # tabla puede no existir
        cur.execute("DELETE FROM users WHERE id = ?", (target_user_id,))
        conn.commit()
        return None
    except Exception as e:
        conn.rollback()
        return str(e)
    finally:
        conn.close()
