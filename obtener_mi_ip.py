"""Muestra tu IP pública para agregarla en MercadoLibre DevCenter (Configuración IP)."""
import requests

try:
    r = requests.get("https://api.ipify.org?format=json", timeout=5)
    r.raise_for_status()
    ip = r.json().get("ip", "?")
    print(f"\nTu IP publica: {ip}")
    print("\nAgrega esta IP en MercadoLibre DevCenter:")
    print("  1. developers.mercadolibre.com.ar/devcenter/")
    print("  2. Mis aplicaciones - Tu app - Configuracion IP")
    print("  3. Agregar IP - Pegar:", ip)
    print()
except Exception as e:
    print(f"Error al obtener IP: {e}")
    print("Probá manualmente en: https://www.cual-es-mi-ip.net/")
