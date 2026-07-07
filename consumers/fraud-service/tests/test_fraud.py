"""
Unit tests for the fraud detection scoring model.
Run: pytest consumers/fraud-service/tests/
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fraud_consumer import calculate_fraud_risk, FRAUD_SCORE_THRESHOLD


# ── Helper ───────────────────────────────────────────────────────────────────
def _order(**kwargs):
    base = {"order_id": 1, "customer_id": 1234, "amount": 50.0,
            "currency": "USD", "country": "US", "status": "CREATED"}
    base.update(kwargs)
    return base


# ── Fraud cases ───────────────────────────────────────────────────────────────
def test_very_high_amount_risky_country_triggers_fraud():
    score, reasons = calculate_fraud_risk(_order(amount=460, country="CN"))
    assert score >= FRAUD_SCORE_THRESHOLD
    assert "VERY_HIGH_AMOUNT" in reasons
    assert "HIGH_RISK_COUNTRY" in reasons


def test_high_amount_risky_country_triggers_fraud():
    score, reasons = calculate_fraud_risk(_order(amount=350, country="NG"))
    assert score >= FRAUD_SCORE_THRESHOLD
    assert "HIGH_AMOUNT" in reasons
    assert "HIGH_RISK_COUNTRY" in reasons


def test_cancelled_high_value_triggers_fraud():
    score, reasons = calculate_fraud_risk(_order(amount=250, status="CANCELLED"))
    assert score >= FRAUD_SCORE_THRESHOLD
    assert "CANCELLED_HIGH_VALUE" in reasons


def test_suspicious_round_amount_adds_score():
    score, reasons = calculate_fraud_risk(_order(amount=300, country="US"))
    assert "SUSPICIOUS_ROUND_AMOUNT" in reasons


def test_foreign_currency_high_amount_adds_score():
    score, reasons = calculate_fraud_risk(_order(amount=300, currency="EUR"))
    assert "FOREIGN_CURRENCY_HIGH_AMOUNT" in reasons


# ── Safe cases ────────────────────────────────────────────────────────────────
def test_small_amount_safe_country_is_ok():
    score, reasons = calculate_fraud_risk(_order(amount=49.99, country="US"))
    assert score < FRAUD_SCORE_THRESHOLD
    assert reasons == []


def test_medium_amount_safe_country_is_ok():
    score, reasons = calculate_fraud_risk(_order(amount=150, country="CA"))
    assert score < FRAUD_SCORE_THRESHOLD


def test_score_capped_at_100():
    # Pile on every risk factor
    score, _ = calculate_fraud_risk(_order(amount=500, country="NG", status="CANCELLED",
                                           currency="EUR"))
    assert score <= 100


# ── Edge cases ────────────────────────────────────────────────────────────────
def test_zero_amount_is_safe():
    score, reasons = calculate_fraud_risk(_order(amount=0))
    assert score == 0


def test_missing_fields_do_not_crash():
    score, _ = calculate_fraud_risk({})
    assert isinstance(score, int)
