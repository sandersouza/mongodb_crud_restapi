"""Unit tests for the token service layer."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Iterable
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.db.mongo import MongoConnectionError
from app.services import tokens
from app.services.tokens import (
    CreatedToken,
    StoredToken,
    TokenConflictError,
    TokenMetadata,
    TokenNotFoundError,
    TokenPersistenceError,
    _hash_token,
)
from tests.conftest import FakePyMongo


@pytest.fixture()
def anyio_backend() -> str:
    """Execute anyio-based tests on the asyncio backend."""

    return "asyncio"


class _Cursor:
    """Simple asynchronous iterator emulating Motor's cursor."""

    def __init__(self, documents: Iterable[dict[str, Any]]) -> None:
        self._documents = list(documents)

    def __aiter__(self):
        self._iter = iter(self._documents)
        return self

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration as exc:
            raise StopAsyncIteration from exc


def _build_manager() -> SimpleNamespace:
    return SimpleNamespace(
        find_token_document=AsyncMock(),
        get_timeseries_collection_for_database=AsyncMock(),
        get_token_collection_for_database=AsyncMock(),
        iter_token_collections=AsyncMock(),
        remember_token_location=MagicMock(),
        forget_token_location=MagicMock(),
    )


@pytest.mark.anyio
async def test_fetch_token_metadata_updates_last_used(fake_pymongo: FakePyMongo, monkeypatch: pytest.MonkeyPatch) -> None:
    """Fetching metadata should update ``last_used_at`` in storage."""

    manager = _build_manager()
    collection = AsyncMock()
    manager.find_token_document.return_value = ({
        "_id": "object-id",
        "token_hash": "hashed",
        "database": "metrics",
        "description": "sensor",
        "created_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
    }, collection)
    monkeypatch.setattr(tokens, "mongo_manager", manager)

    metadata = await tokens.fetch_token_metadata("secret")

    assert isinstance(metadata, TokenMetadata)
    collection.update_one.assert_awaited_once()


@pytest.mark.anyio
async def test_fetch_token_metadata_handles_missing(fake_pymongo: FakePyMongo, monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing tokens should raise ``TokenNotFoundError``."""

    manager = _build_manager()
    manager.find_token_document.return_value = (None, None)
    monkeypatch.setattr(tokens, "mongo_manager", manager)

    with pytest.raises(TokenNotFoundError):
        await tokens.fetch_token_metadata("secret")


@pytest.mark.anyio
async def test_fetch_token_metadata_wraps_update_error(fake_pymongo: FakePyMongo, monkeypatch: pytest.MonkeyPatch) -> None:
    """PyMongo errors should surface as persistence issues."""

    manager = _build_manager()
    collection = AsyncMock()
    collection.update_one = AsyncMock(side_effect=fake_pymongo.PyMongoError("boom"))
    manager.find_token_document.return_value = ({
        "_id": "object-id",
        "token_hash": "hashed",
        "database": "metrics",
        "description": None,
        "created_at": datetime.now(tz=timezone.utc),
    }, collection)
    monkeypatch.setattr(tokens, "mongo_manager", manager)

    with pytest.raises(TokenPersistenceError):
        await tokens.fetch_token_metadata("secret")


@pytest.mark.anyio
async def test_create_token_persists_document(fake_pymongo: FakePyMongo, monkeypatch: pytest.MonkeyPatch) -> None:
    """Creating a token should prepare collections and store metadata."""

    manager = _build_manager()
    token_collection = AsyncMock()
    manager.get_token_collection_for_database.return_value = token_collection
    monkeypatch.setattr(tokens, "mongo_manager", manager)

    created = await tokens.create_token(
        database="metrics",
        token_value="secret",
        description="Sensor access",
        expires_in_seconds=3600,
    )

    assert isinstance(created, CreatedToken)
    manager.get_timeseries_collection_for_database.assert_awaited_once_with("metrics")
    token_collection.insert_one.assert_awaited_once()
    manager.remember_token_location.assert_called_once_with(_hash_token("secret"), "metrics")
    assert created.expires_at is not None


@pytest.mark.anyio
async def test_create_token_handles_duplicate(fake_pymongo: FakePyMongo, monkeypatch: pytest.MonkeyPatch) -> None:
    """Duplicate hashes should raise ``TokenConflictError``."""

    manager = _build_manager()
    token_collection = AsyncMock()
    token_collection.insert_one = AsyncMock(side_effect=fake_pymongo.DuplicateKeyError("exists"))
    manager.get_token_collection_for_database.return_value = token_collection
    monkeypatch.setattr(tokens, "mongo_manager", manager)

    with pytest.raises(TokenConflictError):
        await tokens.create_token(database="metrics", token_value="secret")


@pytest.mark.anyio
async def test_create_token_wraps_generic_errors(fake_pymongo: FakePyMongo, monkeypatch: pytest.MonkeyPatch) -> None:
    """Unexpected insert errors should raise a persistence error."""

    manager = _build_manager()
    token_collection = AsyncMock()
    token_collection.insert_one = AsyncMock(side_effect=fake_pymongo.PyMongoError("boom"))
    manager.get_token_collection_for_database.return_value = token_collection
    monkeypatch.setattr(tokens, "mongo_manager", manager)

    with pytest.raises(TokenPersistenceError):
        await tokens.create_token(database="metrics", token_value="secret")


@pytest.mark.anyio
async def test_create_token_handles_preparation_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Failures preparing the database should surface as persistence errors."""

    manager = _build_manager()
    manager.get_timeseries_collection_for_database.side_effect = MongoConnectionError("down")
    monkeypatch.setattr(tokens, "mongo_manager", manager)

    with pytest.raises(TokenPersistenceError):
        await tokens.create_token(database="metrics")


@pytest.mark.anyio
async def test_list_tokens_collects_documents(fake_pymongo: FakePyMongo, monkeypatch: pytest.MonkeyPatch) -> None:
    """Listing tokens should iterate over every collection."""

    manager = _build_manager()
    collection = SimpleNamespace(find=lambda: _Cursor([
        {
            "_id": "id1",
            "token_hash": "hash",
            "description": "Sensor",
            "created_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
            "last_used_at": None,
            "expires_at": None,
        }
    ]))
    manager.iter_token_collections.return_value = [("metrics", collection)]
    monkeypatch.setattr(tokens, "mongo_manager", manager)

    results = await tokens.list_tokens()

    assert isinstance(results, list)
    assert isinstance(results[0], StoredToken)
    assert results[0].database == "metrics"


@pytest.mark.anyio
async def test_list_tokens_wraps_errors(fake_pymongo: FakePyMongo, monkeypatch: pytest.MonkeyPatch) -> None:
    """Errors while iterating tokens should raise ``TokenPersistenceError``."""

    manager = _build_manager()
    manager.iter_token_collections.side_effect = MongoConnectionError("down")
    monkeypatch.setattr(tokens, "mongo_manager", manager)

    with pytest.raises(TokenPersistenceError):
        await tokens.list_tokens()


@pytest.mark.anyio
async def test_list_tokens_handles_collection_errors(fake_pymongo: FakePyMongo, monkeypatch: pytest.MonkeyPatch) -> None:
    """Collection level failures should also be wrapped."""

    manager = _build_manager()

    class _BadCursor:
        def __aiter__(self):
            return self

        async def __anext__(self):
            raise fake_pymongo.PyMongoError("boom")

    failing_collection = SimpleNamespace(find=lambda: _BadCursor())
    manager.iter_token_collections.return_value = [("metrics", failing_collection)]
    monkeypatch.setattr(tokens, "mongo_manager", manager)

    with pytest.raises(TokenPersistenceError):
        await tokens.list_tokens()


@pytest.mark.anyio
async def test_revoke_token_deletes_document(fake_pymongo: FakePyMongo, monkeypatch: pytest.MonkeyPatch) -> None:
    """Revoking a token should remove it and clear cached locations."""

    manager = _build_manager()
    collection = AsyncMock()
    collection.find_one_and_delete = AsyncMock(return_value={
        "_id": "id1",
        "token_hash": "hash",
    })
    manager.get_token_collection_for_database.return_value = collection
    monkeypatch.setattr(tokens, "mongo_manager", manager)

    await tokens.revoke_token(database="metrics", token_id="507f1f77bcf86cd799439011")

    manager.forget_token_location.assert_called_once_with("hash")


@pytest.mark.anyio
async def test_revoke_token_rejects_invalid_object_id() -> None:
    """Invalid token identifiers should raise ``TokenNotFoundError``."""

    with pytest.raises(TokenNotFoundError):
        await tokens.revoke_token(database="metrics", token_id="not-a-valid-objectid")


@pytest.mark.anyio
async def test_revoke_token_handles_missing_document(fake_pymongo: FakePyMongo, monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing documents should result in a ``TokenNotFoundError``."""

    manager = _build_manager()
    collection = AsyncMock()
    collection.find_one_and_delete = AsyncMock(return_value=None)
    manager.get_token_collection_for_database.return_value = collection
    monkeypatch.setattr(tokens, "mongo_manager", manager)

    with pytest.raises(TokenNotFoundError):
        await tokens.revoke_token(database="metrics", token_id="507f1f77bcf86cd799439011")


@pytest.mark.anyio
async def test_revoke_token_wraps_errors(fake_pymongo: FakePyMongo, monkeypatch: pytest.MonkeyPatch) -> None:
    """PyMongo errors should be wrapped into persistence exceptions."""

    manager = _build_manager()
    collection = AsyncMock()
    collection.find_one_and_delete = AsyncMock(side_effect=fake_pymongo.PyMongoError("boom"))
    manager.get_token_collection_for_database.return_value = collection
    monkeypatch.setattr(tokens, "mongo_manager", manager)

    with pytest.raises(TokenPersistenceError):
        await tokens.revoke_token(database="metrics", token_id="507f1f77bcf86cd799439011")
