"""Tests for the API token management endpoints."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from app.db.mongo import mongo_manager
from app.main import app
from app.services import tokens as token_service
from app.services.tokens import CreatedToken, StoredToken


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
    expires_at = created_at + timedelta(hours=1)

    async def fake_create_token(**kwargs):  # pragma: no cover - trivial coroutine
        assert kwargs["expires_in_seconds"] == 3600
        return CreatedToken(
            token="generated-token",
            database=kwargs["database"],
            description=kwargs.get("description"),
            created_at=created_at,
            last_used_at=None,
            expires_at=expires_at,
        )

    monkeypatch.setattr(token_service, "create_token", fake_create_token)

    response = admin_client.post(
        "/api/tokens",
        json={
            "database": "validationsplugin",
            "description": "Token de teste",
            "expires_in_seconds": 3600,
        },
    )

    assert response.status_code == 201
    payload = response.json()
    assert payload["token"] == "generated-token"
    assert payload["database"] == "validationsplugin"
    assert payload["description"] == "Token de teste"
    returned_created_at = datetime.fromisoformat(payload["created_at"].replace("Z", "+00:00"))
    assert returned_created_at == created_at
    assert payload["last_used_at"] is None
    returned_expires_at = datetime.fromisoformat(payload["expires_at"].replace("Z", "+00:00"))
    assert returned_expires_at == expires_at


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


def test_list_tokens_returns_metadata(
    monkeypatch: pytest.MonkeyPatch, admin_client: TestClient
) -> None:
    """Ensure the listing route returns tokens grouped by database."""

    created_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
    last_used_at = datetime(2024, 1, 2, tzinfo=timezone.utc)
    expires_at = datetime(2024, 1, 3, tzinfo=timezone.utc)

    async def fake_list_tokens(database: str | None = None):  # pragma: no cover
        assert database is None
        return [
            StoredToken(
                id="507f1f77bcf86cd799439011",
                database="analytics",
                description="Relatórios",
                created_at=created_at,
                last_used_at=last_used_at,
                expires_at=expires_at,
            ),
            StoredToken(
                id="507f1f77bcf86cd799439012",
                database="logs",
                description=None,
                created_at=created_at,
                last_used_at=None,
                expires_at=None,
            ),
        ]

    monkeypatch.setattr(token_service, "list_tokens", fake_list_tokens)

    response = admin_client.get("/api/tokens")

    assert response.status_code == 200
    payload = response.json()
    assert payload == [
        {
            "id": "507f1f77bcf86cd799439011",
            "database": "analytics",
            "description": "Relatórios",
            "created_at": created_at.isoformat().replace("+00:00", "Z"),
            "last_used_at": last_used_at.isoformat().replace("+00:00", "Z"),
            "expires_at": expires_at.isoformat().replace("+00:00", "Z"),
        },
        {
            "id": "507f1f77bcf86cd799439012",
            "database": "logs",
            "description": None,
            "created_at": created_at.isoformat().replace("+00:00", "Z"),
            "last_used_at": None,
            "expires_at": None,
        },
    ]


def test_list_tokens_accepts_database_filter(
    monkeypatch: pytest.MonkeyPatch, admin_client: TestClient
) -> None:
    """Ensure the optional database filter is forwarded to the service layer."""

    captured: dict[str, str | None] = {}

    async def fake_list_tokens(database: str | None = None):  # pragma: no cover
        captured["database"] = database
        return []

    monkeypatch.setattr(token_service, "list_tokens", fake_list_tokens)

    response = admin_client.get("/api/tokens", params={"database": "analytics"})

    assert response.status_code == 200
    assert response.json() == []
    assert captured["database"] == "analytics"


def test_list_tokens_storage_failure(
    monkeypatch: pytest.MonkeyPatch, admin_client: TestClient
) -> None:
    """Ensure persistence errors are translated to HTTP 503 responses."""

    async def fake_list_tokens(database: str | None = None):  # pragma: no cover
        raise token_service.TokenPersistenceError("Unable to query stored API tokens.")

    monkeypatch.setattr(token_service, "list_tokens", fake_list_tokens)

    response = admin_client.get("/api/tokens")

    assert response.status_code == 503
    assert response.json()["detail"] == "Unable to query stored API tokens."


def test_revoke_token_success(monkeypatch: pytest.MonkeyPatch, admin_client: TestClient) -> None:
    """Ensure deleting a token delegates to the service layer."""

    captured: dict[str, str] = {}

    async def fake_revoke_token(*, database: str, token_id: str):  # pragma: no cover
        captured["database"] = database
        captured["token_id"] = token_id

    monkeypatch.setattr(token_service, "revoke_token", fake_revoke_token)

    response = admin_client.delete("/api/tokens/analytics/507f1f77bcf86cd799439011")

    assert response.status_code == 204
    assert captured == {
        "database": "analytics",
        "token_id": "507f1f77bcf86cd799439011",
    }


def test_revoke_token_not_found(
    monkeypatch: pytest.MonkeyPatch, admin_client: TestClient
) -> None:
    """Ensure a missing token yields an HTTP 404 response."""

    async def fake_revoke_token(*, database: str, token_id: str):  # pragma: no cover
        raise token_service.TokenNotFoundError("Token not found for the requested database.")

    monkeypatch.setattr(token_service, "revoke_token", fake_revoke_token)

    response = admin_client.delete("/api/tokens/analytics/507f1f77bcf86cd799439011")

    assert response.status_code == 404
    assert response.json()["detail"] == "Token not found for the requested database."


def test_revoke_token_storage_failure(
    monkeypatch: pytest.MonkeyPatch, admin_client: TestClient
) -> None:
    """Ensure persistence errors during deletion surface as HTTP 503 responses."""

    async def fake_revoke_token(*, database: str, token_id: str):  # pragma: no cover
        raise token_service.TokenPersistenceError("Unable to revoke the requested API token.")

    monkeypatch.setattr(token_service, "revoke_token", fake_revoke_token)

    response = admin_client.delete("/api/tokens/analytics/507f1f77bcf86cd799439011")

    assert response.status_code == 503
    assert response.json()["detail"] == "Unable to revoke the requested API token."
