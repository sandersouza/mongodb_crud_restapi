"""Tests for MongoDB index management utilities."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from app.db.mongo import ASCENDING, MongoDBManager


@pytest.fixture()
def anyio_backend() -> str:
    """Force the anyio plugin to run tests using the asyncio backend."""

    return "asyncio"


class _FakeSettings:
    """Simple container mimicking the relevant application settings."""

    def __init__(self, ttl_seconds: int | None) -> None:
        self.timeseries_time_field = "timestamp"
        self.mongodb_collection_ttl_seconds = ttl_seconds


@pytest.mark.anyio
async def test_ensure_indexes_converts_plain_index_to_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure a TTL index replaces the plain timestamp index when enabled."""

    manager = MongoDBManager()
    collection = AsyncMock()
    collection.index_information = AsyncMock(return_value={"timestamp_1": {}})
    collection.drop_index = AsyncMock()
    collection.create_index = AsyncMock()

    monkeypatch.setattr("app.db.mongo.get_settings", lambda: _FakeSettings(ttl_seconds=3600))

    await manager._ensure_indexes(collection)

    collection.drop_index.assert_awaited_once_with("timestamp_1")
    collection.create_index.assert_awaited_once_with(
        [("timestamp", ASCENDING)],
        expireAfterSeconds=3600,
        name="timestamp_1",
    )


@pytest.mark.anyio
async def test_ensure_indexes_disables_ttl_when_seconds_is_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure the TTL index is removed when the TTL configuration is disabled."""

    manager = MongoDBManager()
    collection = AsyncMock()
    collection.index_information = AsyncMock(
        return_value={"timestamp_1": {"expireAfterSeconds": 600}}
    )
    collection.drop_index = AsyncMock()
    collection.create_index = AsyncMock()

    monkeypatch.setattr("app.db.mongo.get_settings", lambda: _FakeSettings(ttl_seconds=0))

    await manager._ensure_indexes(collection)

    collection.drop_index.assert_awaited_once_with("timestamp_1")
    collection.create_index.assert_awaited_once_with(
        [("timestamp", ASCENDING)],
        name="timestamp_1",
    )


@pytest.mark.anyio
async def test_ensure_indexes_is_idempotent_when_configuration_matches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure no index changes are performed when the TTL configuration already matches."""

    manager = MongoDBManager()
    collection = AsyncMock()
    collection.index_information = AsyncMock(
        return_value={"timestamp_1": {"expireAfterSeconds": 120}}
    )
    collection.drop_index = AsyncMock()
    collection.create_index = AsyncMock()

    monkeypatch.setattr("app.db.mongo.get_settings", lambda: _FakeSettings(ttl_seconds=120))

    await manager._ensure_indexes(collection)

    collection.drop_index.assert_not_awaited()
    collection.create_index.assert_not_awaited()


@pytest.mark.anyio
async def test_ensure_indexes_creates_plain_index_when_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure a plain index is created when no timestamp index exists and TTL is disabled."""

    manager = MongoDBManager()
    collection = AsyncMock()
    collection.index_information = AsyncMock(return_value={"_id_": {}})
    collection.drop_index = AsyncMock()
    collection.create_index = AsyncMock()

    monkeypatch.setattr("app.db.mongo.get_settings", lambda: _FakeSettings(ttl_seconds=None))

    await manager._ensure_indexes(collection)

    collection.drop_index.assert_not_awaited()
    collection.create_index.assert_awaited_once_with(
        [("timestamp", ASCENDING)],
        name="timestamp_1",
    )
