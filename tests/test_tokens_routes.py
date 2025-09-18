"""Tests for the API token management endpoints."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from app.db.mongo import mongo_manager
from app.main import app
from app.services import tokens as token_service
from app.services.tokens import CreatedToken


@pytest.fixture()
def admin_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Provide a client authenticated with the administrator token."""

    async def fake_connect() -> None:  # pragma: no cover - trivial coroutine
        return None

    async def fake_close() -> None:  # pragma: no cover - trivial coroutine
        return None

    async def fake_get_collection(database_name: str):  # pragma: no cover - trivial coroutine
        return object()

    monkeypatch.setattr(mongo_manager, "connect", fake_connect)
    monkeypatch.setattr(mongo_manager, "close", fake_close)
    monkeypatch.setattr(
        mongo_manager,
        "get_timeseries_collection_for_database",
        fake_get_collection,
    )

    with TestClient(app) as test_client:
        test_client.headers.update({"Authorization": "Bearer test-admin-token"})
        yield test_client


def test_create_token_returns_secret(monkeypatch: pytest.MonkeyPatch, admin_client: TestClient) -> None:
    """Ensure the creation route returns the generated token secret."""

    created_at = datetime(2024, 1, 1, tzinfo=timezone.utc)

    async def fake_create_token(**kwargs):  # pragma: no cover - trivial coroutine
        return CreatedToken(
            token="generated-token",
            database=kwargs["database"],
            description=kwargs.get("description"),
            created_at=created_at,
            last_used_at=None,
        )

    monkeypatch.setattr(token_service, "create_token", fake_create_token)

    response = admin_client.post(
        "/api/tokens",
        json={"database": "validationsplugin", "description": "Token de teste"},
    )

    assert response.status_code == 201
    payload = response.json()
    assert payload["token"] == "generated-token"
    assert payload["database"] == "validationsplugin"
    assert payload["description"] == "Token de teste"
    returned_created_at = datetime.fromisoformat(payload["created_at"].replace("Z", "+00:00"))
    assert returned_created_at == created_at
    assert payload["last_used_at"] is None


def test_create_token_conflict(monkeypatch: pytest.MonkeyPatch, admin_client: TestClient) -> None:
    """Ensure conflicts are translated into HTTP 409 responses."""

    async def fake_create_token(**kwargs):  # pragma: no cover - trivial coroutine
        raise token_service.TokenConflictError("A token with the provided value already exists.")

    monkeypatch.setattr(token_service, "create_token", fake_create_token)

    response = admin_client.post("/api/tokens", json={"database": "duplicated"})

    assert response.status_code == 409
    assert response.json()["detail"] == "A token with the provided value already exists."


def test_create_token_storage_failure(
    monkeypatch: pytest.MonkeyPatch, admin_client: TestClient
) -> None:
    """Ensure persistence problems surface as HTTP 503 errors."""

    async def fake_create_token(**kwargs):  # pragma: no cover - trivial coroutine
        raise token_service.TokenPersistenceError("Unable to store the new API token.")

    monkeypatch.setattr(token_service, "create_token", fake_create_token)

    response = admin_client.post("/api/tokens", json={"database": "unstable"})

    assert response.status_code == 503
    assert response.json()["detail"] == "Unable to store the new API token."
