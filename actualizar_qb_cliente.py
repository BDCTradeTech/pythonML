"""Actualiza el qb_customer_id para sanjustocentrocomputacion@gmail.com.
Uso: python actualizar_qb_cliente.py <ID>
Ejemplo: python actualizar_qb_cliente.py 156
El ID se obtiene en Configuración > QuickBooks > Soy el cliente (lista de clientes)."""
import sys
from pathlib import Path

# Añadir el directorio del proyecto al path
sys.path.insert(0, str(Path(__file__).parent))

from main import get_connection

def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    qb_id = sys.argv[1].strip()
    if not qb_id:
        print("Error: proporcioná el ID del cliente de QuickBooks.")
        sys.exit(1)
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE qb_customer_preasignado SET qb_customer_id = ? WHERE LOWER(TRIM(email)) = ?",
            (qb_id, "sanjustocentrocomputacion@gmail.com"),
        )
        conn.commit()
        if cur.rowcount > 0:
            print(f"Actualizado: qb_customer_id = {qb_id} para sanjustocentrocomputacion@gmail.com")
        else:
            print("No se encontró el email. Verificá que exista en qb_customer_preasignado.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
