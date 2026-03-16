#!/usr/bin/env python3
"""
Script para probar email desde el servidor. Ejecutá en el servidor:
  cd /opt/pythonml && source .venv/bin/activate && python test_email_server.py tu-email@gmail.com

Prueba ambos puertos (465 y 587) para ver cuál funciona en tu entorno.
"""
import os
import sys
import socket
import smtplib
import ssl
from pathlib import Path
from email.mime.text import MIMEText
from dotenv import load_dotenv

# Cargar .env desde el directorio del script
script_dir = Path(__file__).resolve().parent
load_dotenv(script_dir / ".env")


def test_smtp(host: str, port: int, user: str, password: str, from_addr: str, to_email: str, use_ssl: bool) -> str | None:
    """Prueba conexión SMTP. Retorna None si OK, mensaje de error si falla."""
    _orig = socket.getaddrinfo
    socket.getaddrinfo = lambda h, p, f=0, t=0, pr=0, fl=0: _orig(h, p, socket.AF_INET, t, pr, fl)
    try:
        msg = MIMEText("Prueba de email desde servidor BDC. Si ves esto, funciona.", "plain", "utf-8")
        msg["Subject"] = f"Test BDC - Puerto {port}"
        msg["From"] = from_addr
        msg["To"] = to_email
        ctx = ssl.create_default_context()
        if use_ssl:
            with smtplib.SMTP_SSL(host, port, context=ctx, timeout=15) as smtp:
                smtp.login(user, password)
                smtp.sendmail(from_addr, to_email, msg.as_string())
        else:
            with smtplib.SMTP(host, port, timeout=15) as smtp:
                smtp.starttls(context=ctx)
                smtp.login(user, password)
                smtp.sendmail(from_addr, to_email, msg.as_string())
        return None
    except Exception as e:
        return str(e)
    finally:
        socket.getaddrinfo = _orig


def main():
    if len(sys.argv) < 2:
        print("Uso: python test_email_server.py tu-email@gmail.com")
        sys.exit(1)
    to_email = sys.argv[1].strip()

    host = os.getenv("SMTP_HOST", "").strip()
    user = os.getenv("SMTP_USER", "").strip()
    password = os.getenv("SMTP_PASS", "").strip().replace(" ", "")
    from_addr = (os.getenv("SMTP_FROM") or user).strip()
    from_name = os.getenv("SMTP_FROM_NAME", "BDC systems").strip()
    from_header = f"{from_name} <{from_addr}>" if from_name else from_addr

    print("=" * 60)
    print("Test de email desde servidor")
    print("=" * 60)
    print(f"SMTP_HOST: {host or '(no configurado)'}")
    print(f"SMTP_USER: {user or '(no configurado)'}")
    print(f"SMTP_FROM: {from_header}")
    print(f"Destino:   {to_email}")
    print()

    if not host or not user or not password:
        print("ERROR: Configurá SMTP_HOST, SMTP_USER y SMTP_PASS en .env")
        sys.exit(1)

    # Probar Puerto 587 (STARTTLS)
    print("Probando puerto 587 (STARTTLS)...")
    err587 = test_smtp(host, 587, user, password, from_header, to_email, use_ssl=False)
    if err587 is None:
        print("  OK - Puerto 587 funciona")
    else:
        print(f"  FALLO: {err587}")

    print()

    # Probar Puerto 465 (SSL directo)
    print("Probando puerto 465 (SSL)...")
    err465 = test_smtp(host, 465, user, password, from_header, to_email, use_ssl=True)
    if err465 is None:
        print("  OK - Puerto 465 funciona")
    else:
        print(f"  FALLO: {err465}")

    print("=" * 60)
    if err587 is None:
        print("Usá SMTP_PORT=587 en tu .env")
        sys.exit(0)
    elif err465 is None:
        print("Usá SMTP_PORT=465 en tu .env")
        sys.exit(0)
    else:
        print("Ningún puerto funcionó. Revisá credenciales y firewall.")
        sys.exit(1)


if __name__ == "__main__":
    main()
