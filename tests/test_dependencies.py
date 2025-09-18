"""Tests for FastAPI dependency utilities."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import HTTPException, status

from app.dependencies import (
    TokenContext,
    _extract_bearer_token,
    get_timeseries_collection,
    get_token_context,
    require_admin_context,
)
from app.services.tokens import TokenNotFoundError, TokenPersistenceError


def _mock_settings(**overrides: Any) -> Any:
    defaults = {
        "api_admin_token": "admin-token",
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


@pytest.fixture()
def anyio_backend() -> str:
    """Run anyio-based tests using the asyncio backend."""

    return "asyncio"


def test_extract_bearer_token_happy_path() -> None:
    """A properly formatted Bearer token should be returned without modification."""

    assert _extract_bearer_token("Bearer secret") == "secret"


@pytest.mark.parametrize(
    "header",
    [None, "", "Token secret", "Bearer   "],
)
def test_extract_bearer_token_rejects_invalid_headers(header: str | None) -> None:
    """Invalid headers must raise HTTP 401 errors."""

    with pytest.raises(HTTPException) as excinfo:
        _extract_bearer_token(header)
    assert excinfo.value.status_code == status.HTTP_401_UNAUTHORIZED


@pytest.mark.anyio
async def test_get_token_context_for_admin(monkeypatch: pytest.MonkeyPatch) -> None:
    """Supplying the administrator token should skip metadata lookups."""

    monkeypatch.setattr("app.dependencies.get_settings", lambda: _mock_settings())

    context = await get_token_context(
        authorization="Bearer admin-token",
        database_override="analytics",
    )

    assert context == TokenContext(token="admin-token", database_name="analytics", is_admin=True)


@pytest.mark.anyio
async def test_get_token_context_for_regular_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """Regular tokens should be resolved via the metadata helper."""

    settings = _mock_settings()
    monkeypatch.setattr("app.dependencies.get_settings", lambda: settings)

    async def fake_fetch(token: str) -> Any:
        assert token == "user-token"
        return SimpleNamespace(database="metrics")

    monkeypatch.setattr("app.dependencies.fetch_token_metadata", fake_fetch)

    context = await get_token_context(
        authorization="Bearer user-token",
        database_override=None,
    )

    assert context == TokenContext(token="user-token", database_name="metrics", is_admin=False)


@pytest.mark.anyio
async def test_get_token_context_rejects_invalid_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """Invalid tokens should yield a 401 HTTP error."""

    monkeypatch.setattr("app.dependencies.get_settings", lambda: _mock_settings())

    async def fake_fetch(token: str) -> Any:
        raise TokenNotFoundError("Invalid API token.")

    monkeypatch.setattr("app.dependencies.fetch_token_metadata", fake_fetch)

    with pytest.raises(HTTPException) as excinfo:
        await get_token_context(authorization="Bearer bad-token", database_override=None)
    assert excinfo.value.status_code == status.HTTP_401_UNAUTHORIZED


@pytest.mark.anyio
async def test_get_token_context_handles_persistence_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Failures talking to MongoDB should surface as 503 responses."""

    monkeypatch.setattr("app.dependencies.get_settings", lambda: _mock_settings())

    async def fake_fetch(token: str) -> Any:
        raise TokenPersistenceError("storage down")

    monkeypatch.setattr("app.dependencies.fetch_token_metadata", fake_fetch)

    with pytest.raises(HTTPException) as excinfo:
        await get_token_context(authorization="Bearer token", database_override=None)
    assert excinfo.value.status_code == status.HTTP_503_SERVICE_UNAVAILABLE


@pytest.mark.anyio
async def test_get_token_context_rejects_database_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """Tokens must not be able to access a different database than configured."""

    monkeypatch.setattr("app.dependencies.get_settings", lambda: _mock_settings())

    async def fake_fetch(token: str) -> Any:
        return SimpleNamespace(database="metrics")

    monkeypatch.setattr("app.dependencies.fetch_token_metadata", fake_fetch)

    with pytest.raises(HTTPException) as excinfo:
        await get_token_context(authorization="Bearer token", database_override="other")
    assert excinfo.value.status_code == status.HTTP_403_FORBIDDEN


@pytest.mark.anyio
async def test_require_admin_context(monkeypatch: pytest.MonkeyPatch) -> None:
    """The admin dependency should only accept administrative tokens."""

    admin = TokenContext(token="admin-token", database_name=None, is_admin=True)
    assert await require_admin_context(admin) is admin

    user = TokenContext(token="user", database_name=None, is_admin=False)
    with pytest.raises(HTTPException) as excinfo:
        await require_admin_context(user)
    assert excinfo.value.status_code == status.HTTP_403_FORBIDDEN


class _DummyManager:
    """Simple stand-in for the Mongo manager used by dependencies."""

    def __init__(self, collection: object) -> None:
        self.collection = collection
        self.called_with: list[str] = []
        self.raise_error: Exception | None = None

    async def get_timeseries_collection_for_database(self, name: str) -> object:
        self.called_with.append(name)
        if self.raise_error is not None:
            raise self.raise_error
        return self.collection


@pytest.mark.anyio
async def test_get_timeseries_collection_happy_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """A valid context should yield a collection from the manager."""

    dummy_collection = object()
    manager = _DummyManager(dummy_collection)
    monkeypatch.setattr("app.dependencies.mongo_manager", manager)

    context = TokenContext(token="token", database_name="metrics", is_admin=False)
    dependency = get_timeseries_collection(context)
    collection = await dependency.__anext__()

    assert collection is dummy_collection
    assert manager.called_with == ["metrics"]

    with pytest.raises(StopAsyncIteration):
        await dependency.__anext__()


@pytest.mark.anyio
async def test_get_timeseries_collection_requires_database() -> None:
    """The dependency should enforce specifying a database for non-admin tokens."""

    context = TokenContext(token="token", database_name=None, is_admin=False)
    generator = get_timeseries_collection(context)

    with pytest.raises(HTTPException) as excinfo:
        await generator.__anext__()
    assert excinfo.value.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.anyio
async def test_get_timeseries_collection_handles_connection_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Mongo connection failures should surface as HTTP 503 errors."""

    from app.db.mongo import MongoConnectionError

    manager = _DummyManager(object())
    manager.raise_error = MongoConnectionError("down")
    monkeypatch.setattr("app.dependencies.mongo_manager", manager)

    context = TokenContext(token="token", database_name="metrics", is_admin=False)
    generator = get_timeseries_collection(context)

    with pytest.raises(HTTPException) as excinfo:
        await generator.__anext__()
    assert excinfo.value.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
