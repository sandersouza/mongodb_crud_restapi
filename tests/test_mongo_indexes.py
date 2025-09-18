"""Tests for MongoDB index and TTL management utilities."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import ANY, AsyncMock, MagicMock, call

import pytest

from app.db.mongo import ASCENDING, MongoConnectionError, MongoDBManager, PyMongoError
from tests.conftest import FakePyMongo


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


def test_should_run_cleanup_obeys_interval() -> None:
    """The cleanup helper should throttle executions based on the configured interval."""

    manager = MongoDBManager()
    tracker: dict[str, datetime] = {}
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)

    assert manager._should_run_cleanup(tracker, "db", now, 60) is True
    assert manager._should_run_cleanup(tracker, "db", now, 60) is False

    later = now + timedelta(seconds=61)
    assert manager._should_run_cleanup(tracker, "db", later, 60) is True


@pytest.mark.anyio
async def test_get_database_caches_databases(monkeypatch: pytest.MonkeyPatch) -> None:
    """The manager should cache database instances returned by the client."""

    manager = MongoDBManager()

    class _Client:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def __getitem__(self, name: str) -> SimpleNamespace:
            self.calls.append(name)
            database = SimpleNamespace(name=name, list_collection_names=AsyncMock())
            return database

        async def list_database_names(self) -> list[str]:
            return ["existing"]

    manager._client = _Client()

    database = await manager._get_database("analytics")
    assert database.name == "analytics"
    assert manager._database_cache["analytics"] is database

    cached = await manager._get_database("analytics")
    assert cached is database


@pytest.mark.anyio
async def test_ensure_token_collection_creates_indexes(
    fake_pymongo: FakePyMongo, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Token collections should be created and indexed when missing."""

    manager = MongoDBManager()
    settings = _FakeSettings()
    monkeypatch.setattr("app.db.mongo.get_settings", lambda: settings)

    database = MagicMock()
    database.name = "analytics"
    database.list_collection_names = AsyncMock(return_value=[])
    database.create_collection = AsyncMock()
    collection = AsyncMock()
    database.__getitem__.return_value = collection

    result = await manager._ensure_token_collection(database)

    database.create_collection.assert_awaited_once_with(settings.api_tokens_collection)
    assert collection.create_index.await_args_list[0].args == ("token_hash",)
    assert manager._token_collection_cache["analytics"] is result


@pytest.mark.anyio
async def test_get_token_collection_triggers_cleanup(fake_pymongo: FakePyMongo, monkeypatch: pytest.MonkeyPatch) -> None:
    """Retrieving token collections should run cleanup logic when cached."""

    manager = MongoDBManager()
    settings = _FakeSettings()
    settings.expiration_cleanup_interval_seconds = 0
    monkeypatch.setattr("app.db.mongo.get_settings", lambda: settings)

    collection = AsyncMock()
    cursor = AsyncMock()
    cursor.to_list = AsyncMock(return_value=[])
    collection.find = MagicMock(return_value=cursor)
    manager._token_collection_cache["analytics"] = collection

    await manager.get_token_collection_for_database("analytics")

    collection.find.assert_called_once()


@pytest.mark.anyio
async def test_iter_token_collections_discovers_databases(
    fake_pymongo: FakePyMongo, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The iterator should include cached and newly discovered collections."""

    manager = MongoDBManager()
    settings = _FakeSettings()
    monkeypatch.setattr("app.db.mongo.get_settings", lambda: settings)

    cached_collection = AsyncMock()
    manager._token_collection_cache["cached"] = cached_collection

    class _Client:
        def __init__(self) -> None:
            self._databases = {
                "cached": SimpleNamespace(
                    list_collection_names=AsyncMock(return_value=[settings.api_tokens_collection])
                ),
                "remote": SimpleNamespace(
                    list_collection_names=AsyncMock(return_value=[settings.api_tokens_collection])
                ),
            }

        async def list_database_names(self) -> list[str]:
            return ["admin", "cached", "remote"]

        def __getitem__(self, name: str) -> SimpleNamespace:
            return self._databases[name]

    manager._client = _Client()

    async def ensure(database):
        return AsyncMock()

    manager._database_cache["cached"] = manager._client._databases["cached"]
    monkeypatch.setattr(manager, "_ensure_token_collection", AsyncMock(return_value=AsyncMock()))
    monkeypatch.setattr(manager, "_cleanup_token_collection", AsyncMock())

    collections = await manager.iter_token_collections()

    assert {name for name, _ in collections} == {"cached", "remote"}


@pytest.mark.anyio
async def test_iter_token_collections_raises_on_error(
    fake_pymongo: FakePyMongo, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Errors when listing collections should raise ``MongoConnectionError``."""

    manager = MongoDBManager()
    settings = _FakeSettings()
    monkeypatch.setattr("app.db.mongo.get_settings", lambda: settings)

    class _Client:
        async def list_database_names(self) -> list[str]:
            return ["analytics"]

        def __getitem__(self, name: str) -> SimpleNamespace:
            database = SimpleNamespace()
            database.list_collection_names = AsyncMock(side_effect=PyMongoError("boom"))
            return database

    manager._client = _Client()

    with pytest.raises(MongoConnectionError):
        await manager.iter_token_collections()


@pytest.mark.anyio
async def test_find_token_document_prefers_cache(fake_pymongo: FakePyMongo, monkeypatch: pytest.MonkeyPatch) -> None:
    """Token lookups should first consult the hash cache."""

    manager = MongoDBManager()
    settings = _FakeSettings()
    monkeypatch.setattr("app.db.mongo.get_settings", lambda: settings)

    collection = AsyncMock()
    collection.find_one = AsyncMock(return_value={"_id": "id", "token_hash": "hash"})
    manager._token_collection_cache["analytics"] = collection
    manager._token_hash_cache["hash"] = "analytics"
    manager._client = SimpleNamespace(list_database_names=AsyncMock(return_value=[]))
    monkeypatch.setattr(manager, "get_token_collection_for_database", AsyncMock(return_value=collection))

    document, found_collection = await manager.find_token_document("hash")

    assert document["_id"] == "id"
    assert found_collection is collection


@pytest.mark.anyio
async def test_find_token_document_searches_all_databases(fake_pymongo: FakePyMongo, monkeypatch: pytest.MonkeyPatch) -> None:
    """The lookup should iterate uncached databases when necessary."""

    manager = MongoDBManager()
    settings = _FakeSettings()
    monkeypatch.setattr("app.db.mongo.get_settings", lambda: settings)

    remote_collection = AsyncMock()
    remote_collection.find_one = AsyncMock(return_value={"_id": "id", "token_hash": "hash"})

    class _Client:
        def __init__(self) -> None:
            self.database = SimpleNamespace(
                list_collection_names=AsyncMock(return_value=[settings.api_tokens_collection])
            )

        async def list_database_names(self) -> list[str]:
            return ["remote"]

        def __getitem__(self, name: str) -> SimpleNamespace:
            return self.database

    manager._client = _Client()
    manager._database_cache = {}
    monkeypatch.setattr(manager, "_ensure_token_collection", AsyncMock(return_value=remote_collection))

    document, collection = await manager.find_token_document("hash")

    assert document["_id"] == "id"
    assert manager._token_hash_cache["hash"] == "remote"


@pytest.mark.anyio
async def test_close_resets_internal_state() -> None:
    """Closing the manager should drop cached references."""

    manager = MongoDBManager()
    manager._client = SimpleNamespace(close=lambda: None)
    manager._database_cache = {"db": object()}
    manager._collection_cache = {"db": object()}
    manager._token_collection_cache = {"db": object()}
    manager._token_hash_cache = {"hash": "db"}

    await manager.close()

    assert manager._client is None
    assert manager._database_cache == {}
    assert manager._token_collection_cache == {}
    assert manager._token_hash_cache == {}


@pytest.mark.anyio
async def test_connect_initialises_client(fake_pymongo: FakePyMongo, monkeypatch: pytest.MonkeyPatch) -> None:
    """Connect should instantiate the Motor client and clear caches."""

    manager = MongoDBManager()
    settings = SimpleNamespace(
        mongodb_uri="mongodb://localhost:27017",
        mongodb_max_pool_size=5,
        mongodb_username=None,
        mongodb_password=None,
        mongodb_collection="measurements",
        timeseries_time_field="timestamp",
        timeseries_meta_field="metadata",
        api_tokens_collection="api_tokens",
        expiration_cleanup_interval_seconds=60,
    )
    monkeypatch.setattr("app.db.mongo.get_settings", lambda: settings)
    monkeypatch.setattr("app.db.mongo._PYMONGO_AVAILABLE", True)

    class _MotorClient:
        def __init__(self, uri: str, **kwargs: Any) -> None:
            self.uri = uri
            self.kwargs = kwargs
            self.closed = False

        async def server_info(self) -> dict[str, str]:
            return {"version": "mock"}

        def close(self) -> None:
            self.closed = True

    motor_module = SimpleNamespace(AsyncIOMotorClient=_MotorClient)
    monkeypatch.setitem(sys.modules, "motor", SimpleNamespace(motor_asyncio=motor_module))
    monkeypatch.setitem(sys.modules, "motor.motor_asyncio", motor_module)

    await manager.connect()

    assert isinstance(manager._client, _MotorClient)
    await manager.close()
