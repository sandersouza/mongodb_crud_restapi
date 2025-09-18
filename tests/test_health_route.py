"""Smoke tests for the health-check endpoint."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture()
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Provide a test client without hitting real MongoDB connections."""

    async def noop(*args, **kwargs):  # pragma: no cover - trivial coroutine
        return None

    monkeypatch.setattr("app.main.mongo_manager.connect", noop)
    monkeypatch.setattr("app.main.mongo_manager.close", noop)

    with TestClient(app) as test_client:
        yield test_client


def test_healthz_returns_ok(client: TestClient) -> None:
    """Ensure the health-check route returns a positive status."""

    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
