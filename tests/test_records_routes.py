"""Tests for the records API routes."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.db.mongo import mongo_manager
from app.services import records as service
from app.services.tokens import TokenNotFoundError


@pytest.fixture()
def client_without_token(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Create a FastAPI test client with MongoDB interactions disabled."""

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
        yield test_client


@pytest.fixture()
def client(client_without_token: TestClient) -> TestClient:
    """Provide a test client with the administrator token pre-configured."""

    client_without_token.headers.update({"X-API-Token": "test-admin-token"})
    return client_without_token


def test_records_require_token(client_without_token: TestClient) -> None:
    """Ensure requests without a token are rejected."""

    response = client_without_token.get("/api/records/search")

    assert response.status_code == 401
    assert response.json()["detail"] == "API token required."


def test_records_reject_invalid_token(
    monkeypatch: pytest.MonkeyPatch, client_without_token: TestClient
) -> None:
    """Ensure invalid tokens trigger a 401 response."""

    async def fake_fetch_token_metadata(token: str):  # pragma: no cover - trivial coroutine
        raise TokenNotFoundError("Invalid API token.")

    monkeypatch.setattr("app.dependencies.fetch_token_metadata", fake_fetch_token_metadata)

    response = client_without_token.get(
        "/api/records/search", headers={"X-API-Token": "invalid-token"}
    )

    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid API token."


def test_search_rejects_inverted_time_range(monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
    """Ensure the API rejects searches where start_time is after end_time."""

    called = {"value": False}

    async def stub_search_records(**kwargs):  # pragma: no cover - branch guard
        called["value"] = True
        return ([], False)

    monkeypatch.setattr(service, "search_records", stub_search_records)

    response = client.get(
        "/api/records/search",
        params={
            "field": "acronym",
            "value": "swe",
            "latest": "true",
            "limit": 25,
            "start_time": "2026-01-01T00:00:00Z",
            "end_time": "2024-12-31T23:59:59Z",
        },
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "The start_time must be before the end_time."
    assert called["value"] is False


def test_search_route_is_not_shadowed_by_record_id(monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
    """Verify ``/search`` hits the search handler rather than the ``/{record_id}`` routes."""

    captured: dict[str, object] = {}

    async def stub_search_records(**kwargs):
        captured.update(kwargs)
        return ([], True)

    monkeypatch.setattr(service, "search_records", stub_search_records)

    response = client.get(
        "/api/records/search",
        params={"field": "acronym", "value": "swe", "latest": "true"},
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "No records found for the given filters."

    assert captured["field"] == "acronym"
    assert captured["value"] == "swe"
    assert captured["latest"] is True
    assert captured["limit"] == 1
    assert captured["start_time"] is None
    assert captured["end_time"] is None
