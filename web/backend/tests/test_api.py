"""
Unit tests for the FastAPI backend.
Run: pytest web/backend/tests/
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Patch Couchbase before importing app so startup doesn't try to connect
from unittest.mock import MagicMock, patch
import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client():
    """Return a TestClient with Couchbase mocked out."""
    mock_cluster    = MagicMock()
    mock_collection = MagicMock()
    mock_rows       = [
        {"order_id": 1, "amount": 99.0, "country": "US", "status": "CREATED"},
        {"order_id": 2, "amount": 250.0, "country": "DE", "status": "CONFIRMED"},
    ]
    mock_cluster.query.return_value.rows.return_value = [
        {os.environ.get("COUCHBASE_BUCKET", "order_analytics"): r} for r in mock_rows
    ]

    with patch("app._cluster", mock_cluster), patch("app._collection", mock_collection):
        from app import app as fastapi_app
        with TestClient(fastapi_app) as c:
            yield c


# ── Health check ──────────────────────────────────────────────────────────────
def test_healthz_returns_200(client):
    resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


# ── Authentication ─────────────────────────────────────────────────────────────
def test_analytics_without_api_key_returns_403(client):
    resp = client.get("/api/analytics")
    assert resp.status_code == 403


def test_analytics_with_wrong_key_returns_403(client):
    resp = client.get("/api/analytics", headers={"X-API-Key": "wrong-key"})
    assert resp.status_code == 403


def test_analytics_with_valid_key_returns_200(client):
    # dev-key-change-me is the default key from API_KEYS env var
    os.environ.setdefault("API_KEYS", "dev-key-change-me")
    resp = client.get("/api/analytics", headers={"X-API-Key": "dev-key-change-me"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert "orders" in body
    assert "count" in body


# ── Metrics endpoint ──────────────────────────────────────────────────────────
def test_metrics_endpoint_returns_prometheus_format(client):
    resp = client.get("/metrics")
    assert resp.status_code == 200
    assert "api_requests_total" in resp.text
