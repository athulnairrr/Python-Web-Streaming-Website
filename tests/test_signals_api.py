"""Smoke tests for the market-signals endpoints. Background OTC/live
sources are disabled via conftest.py so counts stay deterministic.
"""

from fastapi.testclient import TestClient

from app.main import app
from app.processing.signal_steps import DIRECTIONS


def test_signals_health_reports_config():
    with TestClient(app) as client:
        res = client.get("/signals/health")
        assert res.status_code == 200
        body = res.json()
        assert body["status"] == "ok"
        assert "timeframes" in body
        assert "telegram_enabled" in body


def test_signals_ingest_is_broadcast_with_a_direction():
    with TestClient(app) as client:
        with client.websocket_connect("/ws/signals") as ws:
            client.post(
                "/signals/ingest",
                json={
                    "id": "otc-1",
                    "source": "otc",
                    "symbol": "EURUSD-OTC",
                    "timeframe": "M1",
                    "price": 1.0842,
                    "timestamp": "2024-01-01T00:00:00+00:00",
                },
            )
            message = ws.receive_json()
            assert message["symbol"] == "EURUSD-OTC"
            assert message["direction"] in DIRECTIONS
            assert "latency_ms" in message


def test_telegram_test_alert_reports_not_configured_by_default():
    with TestClient(app) as client:
        res = client.post("/telegram/test-alert")
        assert res.status_code == 200
        body = res.json()
        assert body["ok"] is False
        assert "not configured" in body["error"].lower()
