"""Tests for MongoDB index and TTL management utilities."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, call

import pytest

from app.db.mongo import ASCENDING, MongoDBManager


@pytest.fixture()
def anyio_backend() -> str:
    """Force the anyio plugin to run tests using the asyncio backend."""

    return "asyncio"


class _FakeSettings:
    """Simple container mimicking the relevant application settings."""

    def __init__(self) -> None:
        self.timeseries_time_field = "timestamp"
        self.timeseries_meta_field = "metadata"
        self.mongodb_collection = "measurements"


@pytest.mark.anyio
async def test_ensure_indexes_recreates_plain_index_when_ttl_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure legacy TTL indexes on the time field are replaced."""

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

    monkeypatch.setattr("app.db.mongo.get_settings", lambda: _FakeSettings())

    await manager._ensure_indexes(collection)

    assert collection.drop_index.await_args_list == [call("timestamp_1")]
    assert collection.create_index.await_args_list == [
        call([("timestamp", ASCENDING)], name="timestamp_1"),
    ]


@pytest.mark.anyio
async def test_ensure_indexes_is_idempotent_with_expected_indexes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure no action is taken when timestamp and TTL indexes are correct."""

    manager = MongoDBManager()
    collection = AsyncMock()
    collection.index_information = AsyncMock(
        return_value={
            "timestamp_1": {"key": [("timestamp", ASCENDING)]},
        }
    )
    collection.drop_index = AsyncMock()
    collection.create_index = AsyncMock()

    monkeypatch.setattr("app.db.mongo.get_settings", lambda: _FakeSettings())

    await manager._ensure_indexes(collection)

    collection.drop_index.assert_not_awaited()
    collection.create_index.assert_not_awaited()


@pytest.mark.anyio
async def test_ensure_indexes_creates_missing_timestamp_index(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure the timestamp index is created when absent."""

    manager = MongoDBManager()
    collection = AsyncMock()
    collection.index_information = AsyncMock(return_value={"_id_": {}})
    collection.drop_index = AsyncMock()
    collection.create_index = AsyncMock()

    monkeypatch.setattr("app.db.mongo.get_settings", lambda: _FakeSettings())

    await manager._ensure_indexes(collection)

    collection.drop_index.assert_not_awaited()
    assert collection.create_index.await_args_list == [
        call([("timestamp", ASCENDING)], name="timestamp_1"),
    ]


@pytest.mark.anyio
async def test_ensure_indexes_drops_incorrect_ttl_index(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure TTL indexes with wrong settings are removed."""

    manager = MongoDBManager()
    collection = AsyncMock()
    collection.index_information = AsyncMock(
        return_value={
            "timestamp_1": {"key": [("timestamp", ASCENDING)]},
            "expires_at_ttl": {
                "key": [("expires_at", ASCENDING)],
                "expireAfterSeconds": 600,
            },
        }
    )
    collection.drop_index = AsyncMock()
    collection.create_index = AsyncMock()

    monkeypatch.setattr("app.db.mongo.get_settings", lambda: _FakeSettings())

    await manager._ensure_indexes(collection)

    assert collection.drop_index.await_args_list == [call("expires_at_ttl")]
    assert collection.create_index.await_args_list == []


@pytest.mark.anyio
async def test_ensure_indexes_drops_ttl_missing_partial_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure TTL indexes without the expected partial filter are removed."""

    manager = MongoDBManager()
    collection = AsyncMock()
    collection.index_information = AsyncMock(
        return_value={
            "timestamp_1": {"key": [("timestamp", ASCENDING)]},
            "expires_at_ttl": {
                "key": [("expires_at", ASCENDING)],
                "expireAfterSeconds": 0,
            },
        }
    )
    collection.drop_index = AsyncMock()
    collection.create_index = AsyncMock()

    monkeypatch.setattr("app.db.mongo.get_settings", lambda: _FakeSettings())

    await manager._ensure_indexes(collection)

    assert collection.drop_index.await_args_list == [call("expires_at_ttl")]
    assert collection.create_index.await_args_list == []


@pytest.mark.anyio
async def test_ensure_indexes_drops_legacy_ttl_index_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure legacy TTL index names are removed entirely."""

    manager = MongoDBManager()
    collection = AsyncMock()
    collection.index_information = AsyncMock(
        return_value={
            "timestamp_1": {"key": [("timestamp", ASCENDING)]},
            "expires_at_1": {
                "key": [("expires_at", ASCENDING)],
                "expireAfterSeconds": 0,
            },
        }
    )
    collection.drop_index = AsyncMock()
    collection.create_index = AsyncMock()

    monkeypatch.setattr("app.db.mongo.get_settings", lambda: _FakeSettings())

    await manager._ensure_indexes(collection)

    assert collection.drop_index.await_args_list == [call("expires_at_1")]
    assert collection.create_index.await_args_list == []


@pytest.mark.anyio
async def test_ensure_timeseries_collection_creates_collection_when_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a new time-series collection is created without collection-level TTL."""

    manager = MongoDBManager()
    settings = _FakeSettings()
    monkeypatch.setattr("app.db.mongo.get_settings", lambda: settings)

    database = MagicMock()
    database.list_collection_names = AsyncMock(return_value=[])
    database.create_collection = AsyncMock()
    collection = AsyncMock()
    database.__getitem__.return_value = collection

    ensure_indexes_mock = AsyncMock()
    monkeypatch.setattr(manager, "_ensure_indexes", ensure_indexes_mock)

    result = await manager._ensure_timeseries_collection(database, "analytics")

    database.create_collection.assert_awaited_once_with(
        settings.mongodb_collection,
        timeseries={"timeField": "timestamp", "metaField": "metadata"},
    )
    ensure_indexes_mock.assert_awaited_once_with(collection)
    assert result is collection
