"""Extended coverage for the records service helper functions."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

from app.services import records
from app.services.records import (
    EmptyUpdateError,
    InvalidRecordIdError,
    RecordDeletionError,
    RecordNotFoundError,
    RecordPersistenceError,
    RecordQueryError,
)
from tests.conftest import FakePyMongo


@pytest.fixture()
def anyio_backend() -> str:
    """Execute async tests using asyncio."""

    return "asyncio"


def test_normalize_field_path_supports_aliases() -> None:
    """Field names should resolve to their MongoDB equivalents."""

    assert records._normalize_field_path("id") == "_id"
    assert records._normalize_field_path("payload.temperature") == "payload.temperature"
    assert records._normalize_field_path("source") == "acronym"


def test_object_id_import_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    """A helpful error should be raised when the bson dependency is missing."""

    monkeypatch.setitem(sys.modules, "bson", None)

    with pytest.raises(InvalidRecordIdError):
        records._object_id("abc123")


def test_normalize_timestamp_converts_naive_datetimes() -> None:
    """Naive timestamps should be coerced to UTC aware values."""

    naive = datetime(2024, 1, 1, 12, 30)
    normalized = records._normalize_timestamp(naive)
    assert normalized.tzinfo == timezone.utc
    assert normalized.hour == 12


def test_is_mock_collection_detects_mocks() -> None:
    """Helper should correctly flag unittest mocks and ignore normal objects."""

    assert records._is_mock_collection(Mock()) is True
    assert records._is_mock_collection(object()) is False


@pytest.mark.anyio
async def test_fetch_record_serializes_document(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fetching a document should normalise the MongoDB identifier."""

    collection = AsyncMock()
    collection.find_one = AsyncMock(return_value={"_id": "abc", "source": "sensor"})

    monkeypatch.setattr(records, "_object_id", lambda value: value)

    document = await records.fetch_record(collection, "abc")

    assert document["id"] == "abc"
    collection.find_one.assert_awaited_once_with({"_id": "abc"})


@pytest.mark.anyio
async def test_fetch_record_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing documents should raise a ``RecordNotFoundError``."""

    collection = AsyncMock()
    collection.find_one = AsyncMock(return_value=None)
    monkeypatch.setattr(records, "_object_id", lambda value: value)

    with pytest.raises(RecordNotFoundError):
        await records.fetch_record(collection, "missing")


class _FakeCursor:
    """Minimal async cursor mimicking Motor behaviour."""

    def __init__(self, documents: List[Dict[str, Any]], error: Exception | None = None) -> None:
        self.documents = documents
        self.error = error
        self.operations: list[tuple[str, Any]] = []

    def sort(self, field: str, order: int) -> "_FakeCursor":
        self.operations.append(("sort", (field, order)))
        return self

    def skip(self, amount: int) -> "_FakeCursor":
        self.operations.append(("skip", amount))
        return self

    def limit(self, amount: int) -> "_FakeCursor":
        self.operations.append(("limit", amount))
        self.limit_amount = amount
        return self

    async def to_list(self, length: int) -> List[Dict[str, Any]]:
        if self.error:
            raise self.error
        self.operations.append(("to_list", length))
        return self.documents


@pytest.mark.anyio
async def test_list_records_returns_serialized_documents(fake_pymongo: FakePyMongo) -> None:
    """Records should be sorted and serialised when listing."""

    documents = [{"_id": "1", "source": "sensor"}]
    cursor = _FakeCursor(documents)
    collection = MagicMock()
    collection.find.return_value = cursor

    results = await records.list_records(collection, limit=5, skip=2)

    assert results == [{"id": "1", "source": "sensor"}]
    assert ("sort", ("timestamp", fake_pymongo.module.DESCENDING)) in cursor.operations
    assert ("limit", 5) in cursor.operations


@pytest.mark.anyio
async def test_list_records_wraps_errors(fake_pymongo: FakePyMongo) -> None:
    """PyMongo errors should surface as ``RecordQueryError``."""

    error = fake_pymongo.PyMongoError("boom")
    cursor = _FakeCursor([], error=error)
    collection = MagicMock()
    collection.find.return_value = cursor

    with pytest.raises(RecordQueryError):
        await records.list_records(collection)


@pytest.mark.anyio
async def test_update_record_with_metadata_only(fake_pymongo: FakePyMongo, monkeypatch: pytest.MonkeyPatch) -> None:
    """Metadata-only updates should use ``find_one_and_update``."""

    collection = AsyncMock()
    updated_doc = {"_id": "abc", "metadata": {"k": "v"}}
    collection.find_one_and_update = AsyncMock(return_value=updated_doc)

    monkeypatch.setattr(records, "_object_id", lambda value: value)

    document = await records.update_record(
        collection,
        "abc",
        records.TimeSeriesRecordUpdate(metadata={"k": "v"}),
    )

    assert document["metadata"] == {"k": "v"}
    collection.find_one_and_update.assert_awaited_once()


@pytest.mark.anyio
async def test_update_record_requires_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    """An empty update should raise a clear validation error."""

    collection = AsyncMock()
    monkeypatch.setattr(records, "_object_id", lambda value: value)

    with pytest.raises(EmptyUpdateError):
        await records.update_record(collection, "abc", records.TimeSeriesRecordUpdate())


@pytest.mark.anyio
async def test_update_record_falls_back_to_replace(fake_pymongo: FakePyMongo, monkeypatch: pytest.MonkeyPatch) -> None:
    """Timeseries restrictions should delegate to the replacement helper."""

    collection = AsyncMock()

    async def failing_find_one_and_update(*args, **kwargs):
        raise fake_pymongo.OperationFailure("time-series restriction")

    collection.find_one_and_update = AsyncMock(side_effect=failing_find_one_and_update)
    replacement = AsyncMock(return_value={"_id": "abc", "source": "sensor"})
    monkeypatch.setattr(records, "_replace_document", replacement)
    monkeypatch.setattr(records, "_object_id", lambda value: value)

    document = await records.update_record(
        collection,
        "abc",
        records.TimeSeriesRecordUpdate(metadata={"k": "v", "extra": True}),
    )

    assert document["source"] == "sensor"
    replacement.assert_awaited()


@pytest.mark.anyio
async def test_replace_document_handles_missing_document(fake_pymongo: FakePyMongo, monkeypatch: pytest.MonkeyPatch) -> None:
    """Replacing a non-existent document should raise ``RecordNotFoundError``."""

    collection = AsyncMock()
    collection.find_one = AsyncMock(return_value=None)

    monkeypatch.setattr(records, "_reload_document", AsyncMock())

    with pytest.raises(RecordNotFoundError):
        await records._replace_document(collection, "abc", {"metadata": {}})


@pytest.mark.anyio
async def test_replace_document_reinserts_on_timeseries_error(fake_pymongo: FakePyMongo, monkeypatch: pytest.MonkeyPatch) -> None:
    """Timeseries restrictions should trigger delete-and-reinsert fallback."""

    existing = {"_id": "abc", "source": "sensor"}
    collection = AsyncMock()
    collection.find_one = AsyncMock(return_value=existing)

    async def replace_one(*args, **kwargs):
        raise fake_pymongo.OperationFailure("time-series restriction")

    collection.replace_one = AsyncMock(side_effect=replace_one)
    reloaded = {"_id": "abc", "source": "sensor", "metadata": {"k": "v"}}
    monkeypatch.setattr(records, "_reload_document", AsyncMock(return_value=reloaded))

    delete_result = SimpleNamespace(deleted_count=1)
    collection.delete_one = AsyncMock(return_value=delete_result)
    collection.insert_one = AsyncMock()

    document = await records._replace_document(collection, "abc", {"metadata": {"k": "v"}})

    assert document == reloaded
    collection.delete_one.assert_awaited_once_with({"_id": "abc"})
    collection.insert_one.assert_awaited_with({"_id": "abc", "metadata": {"k": "v"}, "source": "sensor"})


@pytest.mark.anyio
async def test_replace_document_raises_when_not_matched(fake_pymongo: FakePyMongo) -> None:
    """No documents updated should raise a not-found error."""

    collection = AsyncMock()
    collection.find_one = AsyncMock(return_value={"_id": "abc"})
    collection.replace_one = AsyncMock(return_value=SimpleNamespace(matched_count=0))

    with pytest.raises(RecordNotFoundError):
        await records._replace_document(collection, "abc", {"metadata": {}})


@pytest.mark.anyio
async def test_delete_and_reinsert_propagates_insert_failure(fake_pymongo: FakePyMongo, monkeypatch: pytest.MonkeyPatch) -> None:
    """If reinsertion fails the original document should be restored when possible."""

    collection = AsyncMock()
    collection.delete_one = AsyncMock(return_value=SimpleNamespace(deleted_count=1))
    error = fake_pymongo.PyMongoError("insert failed")
    collection.insert_one = AsyncMock(side_effect=[error, None])
    monkeypatch.setattr(records, "_reload_document", AsyncMock())

    with pytest.raises(fake_pymongo.PyMongoError):
        await records._delete_and_reinsert(collection, {"_id": "abc"}, {"_id": "abc"})

    assert collection.insert_one.await_args_list[1].args[0] == {"_id": "abc"}


@pytest.mark.anyio
async def test_delete_and_reinsert_returns_reloaded(fake_pymongo: FakePyMongo, monkeypatch: pytest.MonkeyPatch) -> None:
    """Successful reinsertion should return the freshly loaded document."""

    collection = AsyncMock()
    collection.delete_one = AsyncMock(return_value=SimpleNamespace(deleted_count=1))
    collection.insert_one = AsyncMock()
    monkeypatch.setattr(records, "_reload_document", AsyncMock(return_value={"_id": "abc"}))

    document = await records._delete_and_reinsert(collection, {"_id": "abc"}, {"_id": "abc"})

    assert document == {"_id": "abc"}


@pytest.mark.anyio
async def test_delete_and_reinsert_requires_existing_document(fake_pymongo: FakePyMongo) -> None:
    """Deleting a missing document should raise a not-found error."""

    collection = AsyncMock()
    collection.delete_one = AsyncMock(return_value=SimpleNamespace(deleted_count=0))

    with pytest.raises(RecordNotFoundError):
        await records._delete_and_reinsert(collection, {"_id": "abc"}, {"_id": "abc"})


@pytest.mark.anyio
async def test_reload_document_requires_document() -> None:
    """The reload helper should raise when a document cannot be found."""

    collection = AsyncMock()
    collection.find_one = AsyncMock(return_value=None)

    with pytest.raises(RecordPersistenceError):
        await records._reload_document(collection, "abc")


def test_is_timeseries_restriction_detects_keywords(fake_pymongo: FakePyMongo) -> None:
    """Error messages mentioning time-series features should be detected."""

    error = fake_pymongo.OperationFailure("Time-series collections cannot update metaField")
    assert records._is_timeseries_restriction(error) is True

    other_error = fake_pymongo.OperationFailure("other failure")
    assert records._is_timeseries_restriction(other_error) is False


@pytest.mark.anyio
async def test_delete_record_success(fake_pymongo: FakePyMongo, monkeypatch: pytest.MonkeyPatch) -> None:
    """Deleting a record should call the collection and return silently."""

    collection = AsyncMock()
    collection.delete_one = AsyncMock(return_value=SimpleNamespace(deleted_count=1))
    monkeypatch.setattr(records, "_object_id", lambda value: value)

    await records.delete_record(collection, "abc")
    collection.delete_one.assert_awaited_once_with({"_id": "abc"})


@pytest.mark.anyio
async def test_delete_record_missing(fake_pymongo: FakePyMongo, monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing records should raise ``RecordNotFoundError``."""

    collection = AsyncMock()
    collection.delete_one = AsyncMock(return_value=SimpleNamespace(deleted_count=0))
    monkeypatch.setattr(records, "_object_id", lambda value: value)

    with pytest.raises(RecordNotFoundError):
        await records.delete_record(collection, "abc")


@pytest.mark.anyio
async def test_delete_record_wraps_errors(fake_pymongo: FakePyMongo, monkeypatch: pytest.MonkeyPatch) -> None:
    """Low level PyMongo errors should surface as ``RecordDeletionError``."""

    collection = AsyncMock()
    collection.delete_one = AsyncMock(side_effect=fake_pymongo.PyMongoError("boom"))
    monkeypatch.setattr(records, "_object_id", lambda value: value)

    with pytest.raises(RecordDeletionError):
        await records.delete_record(collection, "abc")


@pytest.mark.anyio
async def test_search_records_supports_filters(fake_pymongo: FakePyMongo, monkeypatch: pytest.MonkeyPatch) -> None:
    """Search helper should build queries using alias resolution and coercion."""

    documents = [{"_id": "abc", "source": "sensor", "timestamp": datetime.now(tz=timezone.utc)}]
    cursor = MagicMock()
    cursor.sort.return_value = cursor
    cursor.limit.return_value = cursor
    cursor.to_list = AsyncMock(return_value=documents)
    collection = MagicMock()
    collection.find.return_value = cursor

    monkeypatch.setattr(records, "_object_id", lambda value: value)

    results, only_latest = await records.search_records(
        collection,
        field="id",
        value="abc",
        start_time=datetime(2024, 1, 1),
        end_time=datetime(2024, 1, 2),
        latest=False,
        limit=5,
    )

    collection.find.assert_called_once()
    assert results[0]["id"] == "abc"
    assert only_latest is False
    cursor.to_list.assert_awaited_once_with(length=5)


@pytest.mark.anyio
async def test_search_records_returns_latest_only(fake_pymongo: FakePyMongo, monkeypatch: pytest.MonkeyPatch) -> None:
    """Requesting the latest record should enforce a limit of one document."""

    cursor = MagicMock()
    cursor.sort.return_value = cursor
    single_doc = [{"_id": "abc"}]
    limited_cursor = MagicMock()
    limited_cursor.to_list = AsyncMock(return_value=single_doc)
    cursor.limit.return_value = limited_cursor
    collection = MagicMock()
    collection.find.return_value = cursor
    monkeypatch.setattr(records, "_object_id", lambda value: value)

    results, only_latest = await records.search_records(
        collection,
        field=None,
        value=None,
        start_time=None,
        end_time=None,
        latest=True,
        limit=10,
    )

    assert only_latest is True
    assert results == [{"id": "abc"}]
    cursor.limit.assert_called_once_with(1)


@pytest.mark.anyio
async def test_search_records_wraps_errors(fake_pymongo: FakePyMongo) -> None:
    """Errors during search should raise ``RecordQueryError``."""

    collection = MagicMock()
    cursor = MagicMock()
    cursor.sort.return_value = cursor
    cursor.limit.return_value = cursor
    cursor.to_list = AsyncMock(side_effect=fake_pymongo.PyMongoError("boom"))
    collection.find.return_value = cursor

    with pytest.raises(RecordQueryError):
        await records.search_records(collection, None, None, None, None, False, 5)
