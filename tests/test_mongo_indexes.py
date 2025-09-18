"""Tests for MongoDB index and TTL management utilities."""

from __future__ import annotations

from unittest.mock import ANY, AsyncMock, MagicMock, call

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
        self.api_tokens_collection = "api_tokens"
        self.expiration_cleanup_interval_seconds = 300


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


@pytest.mark.anyio
async def test_get_timeseries_collection_triggers_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure cached time-series collections purge expired documents."""

    manager = MongoDBManager()
    settings = _FakeSettings()
    settings.expiration_cleanup_interval_seconds = 0
    monkeypatch.setattr("app.db.mongo.get_settings", lambda: settings)

    collection = AsyncMock()
    collection.delete_many.return_value.deleted_count = 0
    manager._collection_cache["analytics"] = collection

    await manager.get_timeseries_collection_for_database("analytics")

    assert collection.delete_many.await_args_list == [call({"expires_at": {"$lte": ANY}})]


@pytest.mark.anyio
async def test_get_timeseries_collection_respects_cleanup_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure cleanup does not run again before the configured interval."""

    manager = MongoDBManager()
    settings = _FakeSettings()
    settings.expiration_cleanup_interval_seconds = 3600
    monkeypatch.setattr("app.db.mongo.get_settings", lambda: settings)

    collection = AsyncMock()
    collection.delete_many.return_value.deleted_count = 0
    manager._collection_cache["analytics"] = collection

    await manager.get_timeseries_collection_for_database("analytics")
    assert collection.delete_many.await_count == 1

    collection.delete_many.reset_mock()
    await manager.get_timeseries_collection_for_database("analytics")
    collection.delete_many.assert_not_awaited()


@pytest.mark.anyio
async def test_get_token_collection_triggers_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure cached token collections drop expired documents and clear caches."""

    manager = MongoDBManager()
    settings = _FakeSettings()
    settings.expiration_cleanup_interval_seconds = 0
    monkeypatch.setattr("app.db.mongo.get_settings", lambda: settings)

    collection = AsyncMock()
    cursor = AsyncMock()
    cursor.to_list = AsyncMock(return_value=[{"_id": "abc", "token_hash": "hash"}])
    collection.find = MagicMock(return_value=cursor)
    collection.delete_many = AsyncMock()
    collection.delete_many.return_value.deleted_count = 1

    manager._token_collection_cache["analytics"] = collection
    manager._token_hash_cache["hash"] = "analytics"

    await manager.get_token_collection_for_database("analytics")

    collection.find.assert_called_once_with(
        {"expires_at": {"$lte": ANY}},
        projection={"_id": 1, "token_hash": 1},
    )
    cursor.to_list.assert_awaited_once_with(length=None)
    collection.delete_many.assert_awaited_once_with({"_id": {"$in": ["abc"]}})
    assert "hash" not in manager._token_hash_cache


@pytest.mark.anyio
async def test_get_token_collection_respects_cleanup_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure token cleanup is throttled by the configured interval."""

    manager = MongoDBManager()
    settings = _FakeSettings()
    settings.expiration_cleanup_interval_seconds = 3600
    monkeypatch.setattr("app.db.mongo.get_settings", lambda: settings)

    collection = AsyncMock()
    cursor = AsyncMock()
    cursor.to_list = AsyncMock(return_value=[])
    collection.find = MagicMock(return_value=cursor)
    collection.delete_many = AsyncMock()

    manager._token_collection_cache["analytics"] = collection

    await manager.get_token_collection_for_database("analytics")
    collection.find.assert_called_once()
    cursor.to_list.assert_awaited_once_with(length=None)

    collection.find.reset_mock()
    cursor.to_list.reset_mock()
    collection.delete_many.reset_mock()

    await manager.get_token_collection_for_database("analytics")
    collection.find.assert_not_called()
    cursor.to_list.assert_not_called()
    collection.delete_many.assert_not_awaited()
