"""
Tests de PÁGINA para tabs/ventas.py, contra el código real (no el clasificador
aislado). Objetivo: la clase de bug del viernes 17 (UnboundLocalError por un
rename incompleto de is_rejected -> estado, en una rama poco transitada de
_fetch_real) escapó a los unit tests del clasificador porque el clasificador
nunca tuvo el bug -- estaba en el código que lo consume. Estos tests corren
contra build_tab_ventas real, con la API de ML mockeada pero el resto del
código de la página intacto.

nicegui.testing.User falla el test automáticamente si se logea algún ERROR
(ver user_plugin.py: "There were unexpected ERROR logs") -- exactamente el
tipo de fallo silencioso que causó el incidente (el crash quedaba en el log
del server, la UI solo se quedaba en "Cargando...").

Requiere Python 3.11/3.12 + nicegui==2.24.2 (ver conftest.py de esta carpeta).
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pytest
from nicegui import app, ui
from nicegui.testing import User

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import db  # noqa: E402
import tabs.ventas as ventas_mod  # noqa: E402
from tabs.ventas import build_tab_ventas  # noqa: E402

HOY = datetime.now().strftime("%Y-%m-%dT10:00:00.000-03:00")

USER_FIXTURE = {"id": 1, "username": "test"}

ORDER_APROBADA = {
    "id": "ORD1",
    "status": "paid",
    "date_created": HOY,
    "total_amount": 1000.0,
    "buyer": {"nickname": "comprador1"},
    "payments": [{"id": "PAY1", "status": "approved", "payment_type": "credit_card", "total_amount": 1000.0}],
    "shipping": {"id": ""},
    "order_items": [{
        "item": {"id": "ITEM1", "title": "Producto Aprobado", "catalog_listing": False},
        "quantity": 1,
        "unit_price": 1000.0,
        "sale_fee": 150.0,
    }],
}

ORDER_PENDIENTE_CON_CHARGES = {
    "id": "ORD2",
    "status": "handling",
    "date_created": HOY,
    "total_amount": 800.0,
    "buyer": {"nickname": "comprador2"},
    "payments": [{"id": "PAY2", "status": "pending", "payment_type": "credit_card", "total_amount": 800.0}],
    "shipping": {"id": ""},
    "order_items": [{
        "item": {"id": "ITEM2", "title": "Producto Pendiente", "catalog_listing": False},
        "quantity": 1,
        "unit_price": 800.0,
        "sale_fee": 120.0,
    }],
}

PAY_DATA = {
    "PAY1": {
        "status": "approved",
        "charges_details": [
            {"name": "meli_percentage_fee", "amounts": {"original": 150.0}},
            {"name": "financing_add_on_fee", "amounts": {"original": 0.0}},
        ],
        "transaction_details": {"net_received_amount": 850.0},
        "shipping_amount": 0.0,
    },
    "PAY2": {
        # pendiente PERO ya con charges -- el caso que la regla nueva de Parte 1
        # dice que NO debe ir a placeholder vacío.
        "status": "pending",
        "charges_details": [
            {"name": "meli_percentage_fee", "amounts": {"original": 120.0}},
        ],
        "transaction_details": {},
        "shipping_amount": 0.0,
    },
}


@pytest.fixture
def temp_db(tmp_path, monkeypatch):
    """
    NO llamamos a db.init_db(): tiene un bug pre-existente e independiente de
    este trabajo (init_competidores_snapshots_db abre una segunda conexión con
    la primera todavia sin commit -> 'database is locked' en Windows sobre
    una DB fresca). Mismo patrón que test_auth.py: creamos a mano solo las
    tablas que build_tab_ventas necesita. El esquema de ventas_datos acá
    replica el de PRODUCCIÓN (confirmado por PRAGMA table_info contra el
    servidor), no el de db.py -- db.py le faltan comprador_envio y order_date,
    que sí existen en producción (deuda de migración preexistente, ver aparte).
    """
    db_path = tmp_path / "test_ventas.db"
    monkeypatch.setattr(db, "DB_PATH", db_path)
    conn = db.get_connection()
    _now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.executescript("""
        CREATE TABLE productos (
            sku TEXT NOT NULL, user_id INTEGER NOT NULL, costo_usd REAL, tipo_iva REAL DEFAULT 0.105,
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY (sku, user_id)
        );
        CREATE TABLE cotizador_datos (
            user_id INTEGER NOT NULL, clave TEXT NOT NULL, valor TEXT, PRIMARY KEY (user_id, clave)
        );
        CREATE TABLE flex_zonas (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, nombre TEXT NOT NULL,
            codigos_postales TEXT NOT NULL, tarifa REAL NOT NULL, orden INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE financiacion_cuotas_ml (
            cuotas INTEGER PRIMARY KEY, costo_financiacion REAL NOT NULL, fecha_modificacion TEXT NOT NULL
        );
        CREATE TABLE ventas_datos (
            payment_id TEXT NOT NULL, user_id INTEGER NOT NULL, order_id TEXT,
            gan_pesos REAL, gan_vta_pct REAL, gan_cos_pct REAL, meli_fee REAL, cuotas_fee REAL,
            iva_total REAL, deb_cred REAL, iibb_ret REAL, sirtac REAL, envio_real REAL,
            comprador_envio REAL, logistic_type TEXT, net_rcv REAL, fetched_at TEXT, pay_status TEXT,
            order_date TEXT, cuotas TEXT, costo_pesos REAL, costo_fijo REAL, fee_origen TEXT DEFAULT 'api',
            PRIMARY KEY (payment_id, user_id)
        );
    """)
    conn.execute(
        "INSERT INTO productos (user_id, sku, costo_usd, tipo_iva, created_at, updated_at) VALUES (?,?,?,?,?,?)",
        (1, "SKU-TEST", 100.0, 0.105, _now, _now),
    )
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def mock_ml_api(monkeypatch):
    monkeypatch.setattr(ventas_mod, "get_ml_access_token", lambda user_id: "FAKE_TOKEN")
    monkeypatch.setattr(ventas_mod, "ml_get_user_profile", lambda token: {"id": "SELLER1"})
    monkeypatch.setattr(ventas_mod, "ml_get_user_id", lambda token: "SELLER1")

    def _fake_get_orders(token, seller_id, limit=2000, offset=0, date_from=None, date_to=None):
        return {"results": [ORDER_APROBADA, ORDER_PENDIENTE_CON_CHARGES]}

    monkeypatch.setattr(ventas_mod, "ml_get_orders", _fake_get_orders)

    def _fake_merge_payments(token, payment_ids):
        for pid in payment_ids:
            if pid in PAY_DATA:
                return PAY_DATA[pid]
        return {}

    monkeypatch.setattr(ventas_mod, "ml_merge_payments", _fake_merge_payments)
    monkeypatch.setattr(ventas_mod, "ml_get_fixed_fee", lambda token, up, cat, lt: 0.0)
    monkeypatch.setattr(ventas_mod, "ml_get_item_sale_price_full", lambda token, iid: {})

    def _fake_multiget(token, ids, attrs):
        out = []
        skus = {"ITEM1": "SKU-TEST", "ITEM2": "SKU-TEST"}
        for iid in ids:
            out.append({
                "id": iid, "catalog_listing": False, "listing_type_id": "gold_special",
                "attributes": [{"id": "SELLER_SKU", "value_name": skus.get(iid, "")}],
            })
        return out

    monkeypatch.setattr(ventas_mod, "ml_get_items_multiget_with_attributes", _fake_multiget)

    class _FakeResp:
        status_code = 200
        def json(self):
            return {}

    class _FakeSession:
        def get(self, *a, **kw):
            return _FakeResp()

    monkeypatch.setattr(ventas_mod, "get_ml_session", lambda: _FakeSession())


def _registrar_pagina_test() -> None:
    """Se registra DENTRO de cada test: nicegui_reset_globals (fixture del
    plugin) borra todas las rutas antes de cada test, así que un @ui.page a
    nivel de módulo quedaría registrado antes del reset y el open() de User
    daría 404."""
    @ui.page("/test-ventas")
    def _pagina_ventas_test():
        app.storage.user["user"] = USER_FIXTURE
        container = ui.column().classes("w-full")
        build_tab_ventas(container)


@pytest.mark.usefixtures("temp_db", "mock_ml_api")
async def test_lista_muestra_todas_las_ventas(user: User) -> None:
    """Con 2 órdenes fixture, la lista debe pintar las 2 -- ninguna se pierde
    por una excepción a mitad de camino (el síntoma (b) del viernes)."""
    _registrar_pagina_test()
    await user.open("/test-ventas")
    await user.should_see("Producto Aprobado")
    await user.should_see("Producto Pendiente")


@pytest.mark.usefixtures("temp_db", "mock_ml_api")
async def test_popup_venta_aprobada_muestra_datos_financieros(user: User) -> None:
    """El bug del viernes: el popup de CUALQUIER venta ya cacheada crasheaba
    (UnboundLocalError) y se quedaba en 'Cargando...' para siempre. Acá se abre
    el popup DOS VECES a propósito -- la primera pasa por la rama sin caché,
    la segunda por la rama CON caché, que era exactamente la rama rota."""
    _registrar_pagina_test()
    await user.open("/test-ventas")
    await user.should_see("Producto Aprobado")
    user.find("Producto Aprobado").click()
    await user.should_see("Gan $")
    await user.should_see("Comisión ML")
    user.find("Cerrar").click()
    # segunda apertura: ahora sí pasa por la rama _cached (la que rompía)
    user.find("Producto Aprobado").click()
    await user.should_see("Gan $")
    await user.should_see("Comisión ML")


@pytest.mark.usefixtures("temp_db", "mock_ml_api")
async def test_popup_venta_pendiente_con_charges_muestra_numeros_provisorios(user: User) -> None:
    """Regla nueva de Parte 1: pendiente con charges YA existentes no va a
    placeholder vacío -- se calcula y se marca como provisorio."""
    _registrar_pagina_test()
    await user.open("/test-ventas")
    await user.should_see("Producto Pendiente")
    user.find("Producto Pendiente").click()
    await user.should_see("Comisión ML")
    await user.should_not_see("Cargando...")
