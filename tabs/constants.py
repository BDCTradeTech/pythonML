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
    ("promos", "Promos"),
    ("preguntas", "Preguntas"),
    ("flex", "Flex"),
    ("busqueda", "Busquedas"),
    ("balance", "Balance"),
    ("dashboard", "Dashboard"),
    ("compras", "Invoices"),
    ("stock_bdc", "Stock BDC"),
    ("stock", "Stock"),
    ("compras_lista", "Compras"),
    ("pedidos", "Pedidos"),
    ("historicos", "Históricos"),
    ("importacion", "Importacion"),
    ("guias", "Guías"),
    ("pesos", "Pesos"),
    ("arca", "ARCA"),
    ("datos", "Datos"),
    ("configuracion", "Configuración"),
    ("admin", "Admin"),
    ("actividad", "Actividad"),
]

# Grupos de tabs para control de acceso por defecto
TABS_BASE: Set[str] = {"home", "pedidos", "importacion", "pesos", "arca", "datos", "configuracion"}
TABS_ML:   Set[str] = {"estadisticas", "ventas", "productos", "busqueda", "balance", "dashboard", "cuotas", "promos", "preguntas", "flex", "historicos", "stock_bdc", "stock"}
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
    "dashboard": "resumen ejecutivo con alertas, KPIs de productos, ventas y reputación ML.",
    "promos": "descuentos activos en MercadoLibre: precio promo, descuento ML y del vendedor.",
    "preguntas": "ver y responder preguntas sin responder recibidas en MercadoLibre.",
    "flex": "gestión de zonas de envíos Flex con tarifas y códigos postales.",
    "compras": "facturas de QuickBooks con saldo, estado y seguimiento (Invoices).",
    "stock_bdc": "inventario de QuickBooks (Items con cantidad disponible).",
    "stock": "evolución histórica de stock por SKU en MercadoLibre.",
    "compras_lista": "cargar y gestionar compras a cotizar (marca, producto, SKU, cantidad, precio).",
    "pedidos": "ver consolidado de compras de todos los clientes.",
    "importacion": "cargar datos desde archivos.",
    "guias": "gestión de guías de importación y courier.",
    "pesos": "cotización del dólar.",
    "arca": "variables fiscales mensuales (SIPER, IVA, SIRE, Deuda, Multilateral, CLAE).",
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
    "dashboard": "Dashboard",
    "promos": "Promos",
    "preguntas": "Preguntas",
    "flex": "Flex",
    "catalogos": "Catálogos",
    "compras": "Invoices",
    "stock_bdc": "Stock BDC",
    "stock": "Stock",
    "compras_lista": "Compras",
    "pedidos": "Pedidos",
    "importacion": "Importación",
    "guias": "Guías",
    "pesos": "Pesos",
    "arca": "ARCA",
    "datos": "Datos",
    "configuracion": "Configuración",
    "admin": "Admin",
}
