"""
Tests del clasificador de estados de pago de MP (ml_clasificar_pago/ml_charge_neto/
ml_fee_con_fallback en ml_api.py) usado por tabs/ventas.py y ventas_backfill.py.

Contexto: el commit 52d6909 (17/07) introdujo este clasificador pero se revirtió
(6caeaba) porque un rename incompleto de `is_rejected` -> `estado` en una sola rama
de tabs/ventas.py:_fetch_real (no en el clasificador) rompió el popup con
UnboundLocalError. Estos tests cubren el clasificador aislado; la regresión real
del viernes se cubre en test_ventas_pagina.py con nicegui.testing.User contra el
código de la página, no contra el componente.
"""
from __future__ import annotations

import pytest

from ml_api import ml_charge_neto, ml_clasificar_pago, ml_fee_con_fallback


# ── ml_clasificar_pago: los 9 status de MP -> 7 baldes ───────────────────────

def test_approved():
    assert ml_clasificar_pago({"status": "approved"}) == "approved"


def test_rejected():
    assert ml_clasificar_pago({"status": "rejected"}) == "rejected"


def test_in_mediation():
    assert ml_clasificar_pago({"status": "in_mediation"}) == "in_mediation"


def test_refunded_por_status():
    assert ml_clasificar_pago({"status": "refunded"}) == "refunded"


def test_refunded_por_señal_de_orden():
    """Un payment approved pero con la orden marcada has_refund -> refunded."""
    assert ml_clasificar_pago({"status": "approved"}, order_tiene_refund=True) == "refunded"


def test_charged_back():
    assert ml_clasificar_pago({"status": "charged_back"}) == "charged_back"


def test_charged_back_gana_sobre_refund_de_orden():
    """charged_back tiene prioridad aunque la orden también señale refund."""
    assert ml_clasificar_pago({"status": "charged_back"}, order_tiene_refund=True) == "charged_back"


def test_cancelled_por_status():
    assert ml_clasificar_pago({"status": "cancelled"}) == "cancelled"


def test_cancelled_por_señal_de_orden():
    assert ml_clasificar_pago({"status": "approved"}, order_cancelada=True) == "cancelled"


@pytest.mark.parametrize("status", ["pending", "in_process", "authorized", "", "algo_desconocido"])
def test_pendiente_agrupa_estados_sin_resolver(status):
    assert ml_clasificar_pago({"status": status}) == "pendiente"


def test_pendiente_default_sin_pay_data():
    assert ml_clasificar_pago({}) == "pendiente"


# ── ml_charge_neto: resta refunded, filtra por name/contains ────────────────

def test_charge_neto_resta_refunded():
    charges = [{"name": "meli_percentage_fee", "amounts": {"original": 100.0, "refunded": 30.0}}]
    assert ml_charge_neto(charges, name="meli_percentage_fee") == 70.0


def test_charge_neto_sin_refunded_es_igual_a_original():
    charges = [{"name": "meli_percentage_fee", "amounts": {"original": 100.0}}]
    assert ml_charge_neto(charges, name="meli_percentage_fee") == 100.0


def test_charge_neto_contains_case_insensitive():
    charges = [{"name": "IIBB_CABA", "amounts": {"original": 50.0}}]
    assert ml_charge_neto(charges, contains="iibb") == 50.0


def test_charge_neto_no_matchea_devuelve_cero():
    charges = [{"name": "otro_charge", "amounts": {"original": 100.0}}]
    assert ml_charge_neto(charges, name="meli_percentage_fee") == 0.0


def test_charge_neto_suma_multiples_charges_del_mismo_nombre():
    charges = [
        {"name": "meli_percentage_fee", "amounts": {"original": 50.0}},
        {"name": "meli_percentage_fee", "amounts": {"original": 25.0, "refunded": 5.0}},
    ]
    assert ml_charge_neto(charges, name="meli_percentage_fee") == 70.0


# ── ml_fee_con_fallback: escalera api -> orden -> estimada ──────────────────

def test_fallback_usa_charges_reales_cuando_existen():
    charges = [
        {"name": "meli_percentage_fee", "amounts": {"original": 150.0}},
        {"name": "financing_add_on_fee", "amounts": {"original": 20.0}},
    ]
    meli_fee, cuotas_fee, origen = ml_fee_con_fallback(charges, sale_fee_ml=999.0, total_price=1000.0)
    assert (meli_fee, cuotas_fee, origen) == (150.0, 20.0, "api")


def test_fallback_a_sale_fee_de_la_orden_sin_charges():
    meli_fee, cuotas_fee, origen = ml_fee_con_fallback([], sale_fee_ml=180.0, total_price=1000.0)
    assert (meli_fee, cuotas_fee, origen) == (180.0, 0.0, "orden")


def test_fallback_a_estimado_15pct_sin_charges_ni_sale_fee():
    meli_fee, cuotas_fee, origen = ml_fee_con_fallback([], sale_fee_ml=0.0, total_price=1000.0)
    assert (meli_fee, cuotas_fee, origen) == (150.0, 0.0, "estimada")


def test_fallback_no_usa_orden_si_charges_ya_dan_algo_mayor_a_cero():
    """Aunque cuotas_fee real sea 0, si meli_fee real > 0 ya es 'api', no cae a la orden."""
    charges = [{"name": "meli_percentage_fee", "amounts": {"original": 150.0}}]
    meli_fee, cuotas_fee, origen = ml_fee_con_fallback(charges, sale_fee_ml=999.0, total_price=1000.0)
    assert (meli_fee, cuotas_fee, origen) == (150.0, 0.0, "api")
