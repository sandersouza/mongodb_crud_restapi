"""Tests for MongoDB index and TTL management utilities."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, Mock

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
        self.timeseries_meta_field = "metadata"
        self.mongodb_collection = "measurements"


@pytest.mark.anyio
async def test_ensure_indexes_recreates_plain_index_when_ttl_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure TTL-era indexes are replaced by a plain timestamp index."""

    manager = MongoDBManager()
    collection = AsyncMock()
    collection.index_information = AsyncMock(
        return_value={
            "timestamp_1": {
                "key": [("timestamp", ASCENDING)],
                "expireAfterSeconds": 3600,
                "partialFilterExpression": {"metadata": {"$exists": True}},
            }
        }
    )
    collection.drop_index = AsyncMock()
    collection.create_index = AsyncMock()

    monkeypatch.setattr("app.db.mongo.get_settings", lambda: _FakeSettings(ttl_seconds=3600))

    await manager._ensure_indexes(collection)

    collection.drop_index.assert_awaited_once_with("timestamp_1")
    collection.create_index.assert_awaited_once_with(
        [("timestamp", ASCENDING)],
        name="timestamp_1",
    )


@pytest.mark.anyio
async def test_ensure_indexes_is_idempotent_with_plain_index(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure no action is taken when the expected index already exists."""

    manager = MongoDBManager()
    collection = AsyncMock()
    collection.index_information = AsyncMock(
        return_value={"timestamp_1": {"key": [("timestamp", ASCENDING)]}}
    )
    collection.drop_index = AsyncMock()
    collection.create_index = AsyncMock()

    monkeypatch.setattr("app.db.mongo.get_settings", lambda: _FakeSettings(ttl_seconds=None))

    await manager._ensure_indexes(collection)

    collection.drop_index.assert_not_awaited()
    collection.create_index.assert_not_awaited()


@pytest.mark.anyio
async def test_ensure_indexes_creates_plain_index_when_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure a plain index is created when no timestamp index exists."""

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


@pytest.mark.anyio
async def test_synchronize_collection_ttl_updates_value() -> None:
    """Ensure the TTL configuration is updated when it differs."""

    manager = MongoDBManager()
    database = Mock()
    cursor = Mock()
    cursor.to_list = AsyncMock(return_value=[{"options": {"expireAfterSeconds": 120}}])
    database.list_collections = Mock(return_value=cursor)
    database.command = AsyncMock()

    await manager._synchronize_collection_ttl(database, "measurements", desired_ttl=3600)

    database.command.assert_awaited_once_with(
        {"collMod": "measurements", "expireAfterSeconds": 3600}
    )


@pytest.mark.anyio
async def test_synchronize_collection_ttl_disables_when_unset() -> None:
    """Ensure TTL is disabled when the configuration no longer requests it."""

    manager = MongoDBManager()
    database = Mock()
    cursor = Mock()
    cursor.to_list = AsyncMock(return_value=[{"options": {"expireAfterSeconds": 60}}])
    database.list_collections = Mock(return_value=cursor)
    database.command = AsyncMock()

    await manager._synchronize_collection_ttl(database, "measurements", desired_ttl=None)

    database.command.assert_awaited_once_with(
        {"collMod": "measurements", "expireAfterSeconds": "off"}
    )


@pytest.mark.anyio
async def test_synchronize_collection_ttl_is_noop_when_matching() -> None:
    """Ensure no database command is issued when TTL already matches."""

    manager = MongoDBManager()
    database = Mock()
    cursor = Mock()
    cursor.to_list = AsyncMock(return_value=[{"options": {"expireAfterSeconds": 3600}}])
    database.list_collections = Mock(return_value=cursor)
    database.command = AsyncMock()

    await manager._synchronize_collection_ttl(database, "measurements", desired_ttl=3600)

    database.command.assert_not_awaited()


@pytest.mark.anyio
async def test_ensure_timeseries_collection_uses_expire_after_on_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify new collections honor the configured TTL during creation."""

    manager = MongoDBManager()
    settings = _FakeSettings(ttl_seconds=600)
    monkeypatch.setattr("app.db.mongo.get_settings", lambda: settings)

    database = MagicMock()
    database.list_collection_names = AsyncMock(return_value=[])
    database.create_collection = AsyncMock()
    collection = AsyncMock()
    database.__getitem__.return_value = collection

    synchronize_mock = AsyncMock()
    ensure_indexes_mock = AsyncMock()
    monkeypatch.setattr(manager, "_synchronize_collection_ttl", synchronize_mock)
    monkeypatch.setattr(manager, "_ensure_indexes", ensure_indexes_mock)

    result = await manager._ensure_timeseries_collection(database, "analytics")

    database.create_collection.assert_awaited_once_with(
        settings.mongodb_collection,
        timeseries={"timeField": "timestamp", "metaField": "metadata"},
        expireAfterSeconds=600,
    )
    synchronize_mock.assert_awaited_once_with(database, settings.mongodb_collection, 600)
    ensure_indexes_mock.assert_awaited_once_with(collection)
    assert result is collection
