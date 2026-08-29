"""Smoke tests for the FastAPI endpoints (health/ingest), independent of the
background demo generator (disabled here so counts are deterministic).
"""

import os

os.environ["DEMO_SOURCE_ENABLED"] = "false"

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402


def test_health_endpoint_reports_status():
    with TestClient(app) as client:
        res = client.get("/health")
        assert res.status_code == 200
        body = res.json()
        assert body["status"] == "ok"
        assert "queue_size" in body


def test_ingest_accepts_event_and_reports_queued():
    with TestClient(app) as client:
        res = client.post("/ingest", json={"id": 1, "category": "cpu", "value": 42})
        assert res.status_code == 200
        assert res.json()["status"] == "queued"


def test_ingested_event_is_broadcast_over_websocket():
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as ws:
            client.post("/ingest", json={"id": 1, "category": "cpu", "value": 42})
            message = ws.receive_json()
            assert message["id"] == 1
            assert message["category"] == "cpu"
            assert "rolling_mean" in message
            assert "latency_ms" in message
