"""
Fase 3 — tabs/constants.py
Constantes globales de pestañas: claves, grupos de acceso, labels y descripciones.
Importado por main.py y por tabs/home.py.
"""
from __future__ import annotations

from typing import Dict, List, Set, Tuple

# ---------------------------------------------------------------------------
# Registro de pestañas (tab_key interno -> label visible)
# compras_lista (Compras) se quitó de la tabla de permisos.
# ---------------------------------------------------------------------------

TAB_KEYS: List[Tuple[str, str]] = [
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

# Grupos de tabs para control de acceso por defecto
TABS_BASE: Set[str] = {"home", "pedidos", "importacion", "pesos", "datos", "configuracion"}
TABS_ML:   Set[str] = {"estadisticas", "ventas", "productos", "busqueda", "balance", "cuotas", "historicos", "stock"}
TABS_QB:   Set[str] = {"compras", "compras_lista"}

# ---------------------------------------------------------------------------
# Mapeo tab_key -> descripción (para pestaña Home) y label visible
# ---------------------------------------------------------------------------

TAB_DESCRIPTIONS: Dict[str, str] = {
    "estadisticas": "ver reputación en MercadoLibre, ventas hoy/ayer/semana/mes.",
    "ventas": "gestión de ventas y órdenes.",
    "productos": "catálogo de productos.",
    "busqueda": "buscar productos en el catálogo.",
    "balance": "gastos, ingresos y resultados.",
    "compras": "facturas de QuickBooks con saldo, estado y seguimiento (Invoices).",
    "stock": "inventario de QuickBooks (Items con cantidad disponible).",
    "compras_lista": "cargar y gestionar compras a cotizar (marca, producto, SKU, cantidad, precio).",
    "pedidos": "ver consolidado de compras de todos los clientes.",
    "importacion": "cargar datos desde archivos.",
    "pesos": "cotización del dólar.",
    "datos": "configuración de marcas, despachantes y otros datos.",
    "configuracion": "vincular MercadoLibre, QuickBooks y configurar email.",
    "admin": "gestión de usuarios y permisos (solo administradores).",
}

LABEL_BY_TAB: Dict[str, str] = {
    "estadisticas": "Estadísticas",
    "ventas": "Ventas",
    "productos": "Productos",
    "busqueda": "Búsqueda",
    "balance": "Balance",
    "compras": "Invoices",
    "stock": "Stock",
    "compras_lista": "Compras",
    "pedidos": "Pedidos",
    "importacion": "Importación",
    "pesos": "Pesos",
    "datos": "Datos",
    "configuracion": "Configuración",
    "admin": "Admin",
}
