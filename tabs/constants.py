"""
Fase 3 — tabs/constants.py
Constantes globales de pestañas: claves, grupos de acceso, labels y descripciones.
Importado por main.py, tabs/home.py, tabs/admin.py, auth.py y db.py.

TAB_REGISTRY es la ÚNICA fuente de verdad de qué páginas existen y a qué sección
pertenecen (para Admin > Permisos y para los defaults de acceso de usuarios nuevos).
TAB_KEYS y TAB_SECTIONS se derivan de acá — no agregar listas paralelas a mano;
si se agrega una página nueva a la app, agregarla ACÁ.
"""
from __future__ import annotations

from typing import Dict, List, Set, Tuple

# ---------------------------------------------------------------------------
# Registro de páginas: (sección, tab_key interno, label visible)
# compras_lista (Compras) se quitó de la tabla de permisos.
# ---------------------------------------------------------------------------

TAB_REGISTRY: List[Tuple[str, str, str]] = [
    ("Home", "home", "Home"),
    ("Home", "dashboard", "Dashboard"),
    ("MercadoLibre", "estadisticas", "Estadísticas"),
    ("MercadoLibre", "ventas", "Ventas"),
    ("MercadoLibre", "productos", "Productos"),
    ("MercadoLibre", "cuotas", "Cuotas"),
    ("MercadoLibre", "promos", "Promos"),
    ("MercadoLibre", "publicidad", "Publicidad"),
    ("MercadoLibre", "competidores", "Competidores"),
    ("MercadoLibre", "preguntas", "Preguntas"),
    ("MercadoLibre", "flex", "Flex"),
    ("MercadoLibre", "busqueda", "Búsqueda"),
    ("MercadoLibre", "stock", "Stock"),
    ("BDC", "balance", "Balance"),
    ("BDC", "compras", "Invoices"),
    ("BDC", "stock_bdc", "Stock BDC"),
    ("Comex", "compras_lista", "Compras"),
    ("Comex", "pedidos", "Pedidos"),
    ("Comex", "historicos", "Históricos"),
    ("Comex", "importacion", "Importación"),
    ("Comex", "guias", "Guías"),
    ("Comex", "transferencias", "Transferencias"),
    ("Comex", "couriers", "Couriers"),
    ("Impuestos", "pesos", "Pesos"),
    ("Impuestos", "arca", "ARCA"),
    ("Impuestos", "gastos", "Gastos"),
    ("Config", "datos", "Datos"),
    ("Config", "configuracion", "Configuración"),
    ("TiendaNube", "tn_vinculacion", "Vinculación"),
    ("Admin", "admin", "Admin"),
    ("Admin", "actividad", "Actividad"),
]

TAB_KEYS: List[Tuple[str, str]] = [(_key, _label) for _section, _key, _label in TAB_REGISTRY]

TAB_SECTIONS: List[Tuple[str, List[Tuple[str, str]]]] = []
for _section, _key, _label in TAB_REGISTRY:
    if TAB_SECTIONS and TAB_SECTIONS[-1][0] == _section:
        TAB_SECTIONS[-1][1].append((_key, _label))
    else:
        TAB_SECTIONS.append((_section, [(_key, _label)]))

# Permisos que NO son páginas de nivel superior (subsecciones gateadas aparte,
# p.ej. dentro de Gastos) pero igual necesitan default cerrado si el usuario no
# tiene fila explícita. No aparecen en la grilla de Permisos ni en la navegación,
# pero sí en get_user_tab_permissions() para que sigan defaulteando a cerrado.
EXTRA_PERMISSION_KEYS: List[Tuple[str, str]] = [
    ("analisis_ml", "Análisis ML (subsección de Gastos)"),
]

# Grupos de tabs para control de acceso por defecto
TABS_BASE: Set[str] = {"home", "pedidos", "importacion", "pesos", "arca", "datos", "configuracion"}
TABS_ML:   Set[str] = {"estadisticas", "ventas", "productos", "busqueda", "balance", "dashboard", "cuotas", "promos", "publicidad", "competidores", "preguntas", "flex", "historicos", "stock_bdc", "stock"}
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
    "publicidad": "campañas de Publicidad (Product Ads): inversión, ventas por ads, ACOS, ROAS y TACOS.",
    "preguntas": "ver y responder preguntas sin responder recibidas en MercadoLibre.",
    "flex": "gestión de zonas de envíos Flex con tarifas y códigos postales.",
    "compras": "facturas de QuickBooks con saldo, estado y seguimiento (Invoices).",
    "stock_bdc": "inventario de QuickBooks (Items con cantidad disponible).",
    "stock": "evolución histórica de stock por SKU en MercadoLibre.",
    "compras_lista": "cargar y gestionar compras a cotizar (marca, producto, SKU, cantidad, precio).",
    "pedidos": "ver consolidado de compras de todos los clientes.",
    "importacion": "cargar datos desde archivos.",
    "guias": "gestión de guías de importación y courier.",
    "transferencias": "conciliación de transferencias bancarias recibidas.",
    "couriers": "comparador de costos de importación por courier (SIXSTAR, LHS, NC Supplies).",
    "pesos": "cotización del dólar.",
    "arca": "variables fiscales mensuales (SIPER, IVA, SIRE, Deuda, Multilateral, CLAE).",
    "gastos": "carga y análisis de facturas, pagos ARCA y gastos consolidados por período.",
    "competidores": "comparación de precios y posición frente a otros vendedores en MercadoLibre.",
    "datos": "configuración de marcas, despachantes y otros datos.",
    "configuracion": "vincular MercadoLibre, QuickBooks y configurar email.",
    "admin": "gestión de usuarios y permisos (solo administradores).",
    "tn_vinculacion": "cruce de publicaciones de MercadoLibre contra productos de Tienda Nube por SKU.",
}

LABEL_BY_TAB: Dict[str, str] = {
    "estadisticas": "Estadísticas",
    "ventas": "Ventas",
    "productos": "Productos",
    "busqueda": "Búsqueda",
    "balance": "Balance",
    "dashboard": "Dashboard",
    "promos": "Promos",
    "publicidad": "Publicidad",
    "competidores": "Competidores",
    "preguntas": "Preguntas",
    "flex": "Flex",
    "compras": "Invoices",
    "stock_bdc": "Stock BDC",
    "stock": "Stock",
    "compras_lista": "Compras",
    "pedidos": "Pedidos",
    "importacion": "Importación",
    "guias": "Guías",
    "transferencias": "Transferencias",
    "couriers": "Couriers",
    "pesos": "Pesos",
    "arca": "ARCA",
    "gastos": "Gastos",
    "datos": "Datos",
    "configuracion": "Configuración",
    "admin": "Admin",
    "tn_vinculacion": "Vinculación",
}
