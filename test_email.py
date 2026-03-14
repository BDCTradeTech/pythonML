"""
Script para probar el envío de emails. Ejecutá: python test_email.py tu-email@ejemplo.com
"""
import os
import sys
import smtplib
import ssl
from email.mime.text import MIMEText
from dotenv import load_dotenv

load_dotenv()

def main():
    if len(sys.argv) < 2:
        print("Uso: python test_email.py destino@ejemplo.com")
        sys.exit(1)
    to_email = sys.argv[1].strip()

    host = os.getenv("SMTP_HOST", "").strip()
    port = int(os.getenv("SMTP_PORT", "465"))
    user = os.getenv("SMTP_USER", "").strip()
    password = os.getenv("SMTP_PASS", "").strip().replace(" ", "")
    from_addr = os.getenv("SMTP_FROM", "") or user

    print(f"Conectando a {host}:{port}...")
    print(f"Desde: {from_addr}")
    print(f"Hacia: {to_email}")
    print()

    if not host or not user or not password:
        print("ERROR: Configurá SMTP_HOST, SMTP_USER y SMTP_PASS en .env")
        sys.exit(1)

    try:
        msg = MIMEText("Email de prueba de BDC systems. Si ves esto, el SMTP funciona.", "plain", "utf-8")
        msg["Subject"] = "BDC systems - Prueba de email"
        msg["From"] = from_addr
        msg["To"] = to_email

        ctx = ssl.create_default_context()
        if port == 465:
            with smtplib.SMTP_SSL(host, port, context=ctx, timeout=30) as smtp:
                smtp.login(user, password)
                smtp.sendmail(from_addr, to_email, msg.as_string())
        else:
            with smtplib.SMTP(host, port, timeout=30) as smtp:
                smtp.starttls(context=ctx)
                smtp.login(user, password)
                smtp.sendmail(from_addr, to_email, msg.as_string())

        print("OK: Email enviado. Revisá la bandeja de entrada (y spam) de", to_email)
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
