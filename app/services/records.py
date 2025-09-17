"""Service layer responsible for MongoDB interactions."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo import DESCENDING, ReturnDocument
from pymongo.errors import PyMongoError

from ..models.time_series import TimeSeriesRecordCreate, TimeSeriesRecordUpdate
from ..utils.parsing import coerce_value


class InvalidRecordIdError(ValueError):
    """Raised when an invalid MongoDB ObjectId is supplied."""


class RecordNotFoundError(LookupError):
    """Raised when a requested record cannot be located."""


class RecordPersistenceError(RuntimeError):
    """Raised when MongoDB fails to persist a document."""


class RecordDeletionError(RuntimeError):
    """Raised when MongoDB cannot delete the requested document."""


class RecordQueryError(RuntimeError):
    """Raised when MongoDB fails while executing a query."""


class EmptyUpdateError(ValueError):
    """Raised when no fields are supplied for an update operation."""


FIELD_ALIASES: Dict[str, str] = {
    "source": "acronym",
    "acronym": "acronym",
    "id": "_id",
    "_id": "_id",
}


def _normalize_field_path(field: str) -> str:
    """Convert API field names into their persisted MongoDB equivalents."""

    for external, internal in FIELD_ALIASES.items():
        if field == external:
            return internal
        if field.startswith(f"{external}."):
            suffix = field[len(external) :]
            return f"{internal}{suffix}"
    return field


def _serialize(document: Dict[str, Any]) -> Dict[str, Any]:
    """Convert MongoDB internal fields to API friendly values."""

    document["id"] = str(document.pop("_id"))
    return document


def _object_id(value: str) -> ObjectId:
    """Convert a string to :class:`ObjectId` raising friendly errors."""

    try:
        return ObjectId(value)
    except Exception as exc:  # noqa: BLE001 - propagate a custom error instead
        raise InvalidRecordIdError("The provided record identifier is invalid.") from exc


def _normalize_timestamp(value: datetime) -> datetime:
    """Ensure timestamps are timezone aware in UTC."""

    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


async def create_record(
    collection: AsyncIOMotorCollection,
    payload: TimeSeriesRecordCreate,
) -> Dict[str, Any]:
    """Insert a new time-series record into MongoDB."""

    document = payload.model_dump(by_alias=True)
    document.setdefault("timestamp", datetime.now(tz=timezone.utc))
    document["timestamp"] = _normalize_timestamp(document["timestamp"])

    try:
        result = await collection.insert_one(document)
    except PyMongoError as exc:  # pragma: no cover - Motor handles communication
        raise RecordPersistenceError("Unable to store the record in MongoDB.") from exc

    inserted = await collection.find_one({"_id": result.inserted_id})
    assert inserted is not None  # Defensive: insert_one succeeded
    return _serialize(inserted)


async def fetch_record(
    collection: AsyncIOMotorCollection,
    record_id: str,
) -> Dict[str, Any]:
    """Fetch a single record by its identifier."""

    oid = _object_id(record_id)
    document = await collection.find_one({"_id": oid})
    if document is None:
        raise RecordNotFoundError("Record not found.")
    return _serialize(document)


async def list_records(
    collection: AsyncIOMotorCollection,
    limit: int = 100,
    skip: int = 0,
) -> List[Dict[str, Any]]:
    """Return a paginated list of time-series records ordered by timestamp."""

    try:
        cursor = (
            collection.find().sort("timestamp", DESCENDING).skip(skip).limit(limit)
        )
        documents = await cursor.to_list(length=limit)
    except PyMongoError as exc:
        raise RecordQueryError("Failed to retrieve records from MongoDB.") from exc

    return [_serialize(document) for document in documents]


async def update_record(
    collection: AsyncIOMotorCollection,
    record_id: str,
    updates: TimeSeriesRecordUpdate,
) -> Dict[str, Any]:
    """Update an existing record with the provided fields."""

    oid = _object_id(record_id)
    update_payload = {
        k: v for k, v in updates.model_dump(by_alias=True, exclude_unset=True).items()
    }

    if "timestamp" in update_payload and isinstance(update_payload["timestamp"], datetime):
        update_payload["timestamp"] = _normalize_timestamp(update_payload["timestamp"])

    if not update_payload:
        raise EmptyUpdateError("At least one field must be provided for update.")

    try:
        document = await collection.find_one_and_update(
            {"_id": oid},
            {"$set": update_payload},
            return_document=ReturnDocument.AFTER,
        )
    except PyMongoError as exc:
        raise RecordPersistenceError("Failed to update the record in MongoDB.") from exc

    if document is None:
        raise RecordNotFoundError("Record not found for update.")

    return _serialize(document)


async def delete_record(
    collection: AsyncIOMotorCollection,
    record_id: str,
) -> None:
    """Remove a record from MongoDB."""

    oid = _object_id(record_id)

    try:
        result = await collection.delete_one({"_id": oid})
    except PyMongoError as exc:
        raise RecordDeletionError("Failed to delete the record from MongoDB.") from exc

    if result.deleted_count == 0:
        raise RecordNotFoundError("Record not found for deletion.")


async def search_records(
    collection: AsyncIOMotorCollection,
    field: Optional[str],
    value: Optional[str],
    start_time: Optional[datetime],
    end_time: Optional[datetime],
    latest: bool,
    limit: int,
) -> Tuple[List[Dict[str, Any]], bool]:
    """Search records with optional filters and pagination."""

    query: Dict[str, Any] = {}

    if field and value is not None:
        normalized_field = _normalize_field_path(field)
        coerced_value = coerce_value(value)
        if normalized_field == "_id":
            coerced_value = _object_id(str(coerced_value))
        query[normalized_field] = coerced_value

    if start_time or end_time:
        range_filter: Dict[str, Any] = {}
        if start_time:
            range_filter["$gte"] = _normalize_timestamp(start_time)
        if end_time:
            range_filter["$lte"] = _normalize_timestamp(end_time)
        query["timestamp"] = range_filter

    try:
        cursor = collection.find(query)
        cursor = cursor.sort("timestamp", DESCENDING)
        if latest:
            document = await cursor.limit(1).to_list(length=1)
            return ([_serialize(doc) for doc in document], True)

        cursor = cursor.limit(limit)
        documents = await cursor.to_list(length=limit)
    except PyMongoError as exc:
        raise RecordQueryError("Failed to perform search on MongoDB.") from exc

    return ([_serialize(document) for document in documents], False)
