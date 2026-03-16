#!/usr/bin/env python3
"""
Diagnóstico de conectividad SMTP desde el servidor.
Ejecutá: python test_connectivity.py

Verifica: resolución DNS, IPv4, conexión TCP a Gmail SMTP.
"""
import socket
import sys

HOST = "smtp.gmail.com"
PORT_587 = 587
PORT_465 = 465

print("=" * 60)
print("Diagnóstico de conectividad a Gmail SMTP")
print("=" * 60)

# 1. Resolución DNS IPv4
print("\n1. Resolución DNS (IPv4)...")
try:
    addrs = socket.getaddrinfo(HOST, PORT_587, socket.AF_INET)
    for a in addrs[:3]:
        print(f"   {a[4][0]}")
except Exception as e:
    print(f"   ERROR: {e}")
    sys.exit(1)

# 2. Resolución DNS (por defecto - puede ser IPv6)
print("\n2. Resolución DNS (por defecto)...")
try:
    addrs_default = socket.getaddrinfo(HOST, PORT_587)
    for a in addrs_default[:3]:
        print(f"   {a[0].name} -> {a[4][0]}")
except Exception as e:
    print(f"   ERROR: {e}")

# 3. Conexión TCP directa puerto 587 (raw socket)
print("\n3. Conexión TCP a smtp.gmail.com:587 (5 seg timeout)...")
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.settimeout(5)
try:
    sock.connect((HOST, PORT_587))
    print("   OK - Puerto 587 accesible")
    sock.close()
except socket.timeout:
    print("   FALLO - Timeout (puerto 587 bloqueado o inalcanzable)")
except Exception as e:
    print(f"   FALLO: {e}")
finally:
    try:
        sock.close()
    except Exception:
        pass

# 4. Conexión TCP directa puerto 465
print("\n4. Conexión TCP a smtp.gmail.com:465 (5 seg timeout)...")
sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock2.settimeout(5)
try:
    sock2.connect((HOST, PORT_465))
    print("   OK - Puerto 465 accesible")
    sock2.close()
except socket.timeout:
    print("   FALLO - Timeout (puerto 465 bloqueado o inalcanzable)")
except Exception as e:
    print(f"   FALLO: {e}")
finally:
    try:
        sock2.close()
    except Exception:
        pass

# 5. Probar usando IP directamente (evitar DNS)
print("\n5. Conexión usando IP directamente (evita DNS)...")
try:
    ip = socket.getaddrinfo(HOST, PORT_587, socket.AF_INET)[0][4][0]
    print(f"   IP: {ip}")
    sock3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock3.settimeout(5)
    sock3.connect((ip, PORT_587))
    print("   OK - Conexión por IP funciona")
    sock3.close()
except socket.timeout:
    print("   FALLO - Timeout incluso por IP")
except Exception as e:
    print(f"   FALLO: {e}")
finally:
    try:
        sock3.close()
    except Exception:
        pass

print("\n" + "=" * 60)
print("Si pasos 3 o 4 fallan: DigitalOcean o firewall bloquea SMTP saliente.")
print("Solución: usar Resend/SendGrid (HTTPS) o relay de correo.")
print("=" * 60)
