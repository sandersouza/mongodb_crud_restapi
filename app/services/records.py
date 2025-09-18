"""Service layer responsible for MongoDB interactions."""

from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - imported for type checking only
    from bson import ObjectId
    from motor.motor_asyncio import AsyncIOMotorCollection
    from pymongo.errors import OperationFailure

from ..models.time_series import (
    TimeSeriesRecordCreate,
    TimeSeriesRecordOut,
    TimeSeriesRecordUpdate,
)
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


_MISSING_PYMONGO_MESSAGE = (
    "The 'pymongo' dependency is required for MongoDB operations. "
    "Install it with `pip install pymongo`."
)


def _build_field_aliases() -> Dict[str, str]:
    """Derive API-to-database field aliases from the Pydantic schema."""

    aliases: Dict[str, str] = {"_id": "_id", "id": "_id"}

    for field_name, field in TimeSeriesRecordOut.model_fields.items():
        target = field.serialization_alias or field_name

        # The `id` attribute must always resolve to MongoDB's `_id` key.
        if field_name == "id":
            target = "_id"

        aliases[field_name.lower()] = target

        validation_alias = getattr(field.validation_alias, "choices", None)
        if validation_alias:
            for alias in validation_alias:
                aliases[str(alias).lower()] = target

        if field.serialization_alias:
            aliases[field.serialization_alias.lower()] = target

    return aliases


FIELD_ALIASES: Dict[str, str] = _build_field_aliases()


def _normalize_field_path(field: str) -> str:
    """Convert API field names into their persisted MongoDB equivalents."""

    sanitized = field.strip()
    lookup_key = sanitized.lower()

    for external, internal in FIELD_ALIASES.items():
        if lookup_key == external:
            return internal
        if lookup_key.startswith(f"{external}."):
            suffix = sanitized[len(external) :]
            return f"{internal}{suffix}"
    return sanitized


def _serialize(document: Dict[str, Any]) -> Dict[str, Any]:
    """Convert MongoDB internal fields to API friendly values."""

    document["id"] = str(document.pop("_id"))
    return document


def _object_id(value: str) -> "ObjectId":
    """Convert a string to :class:`ObjectId` raising friendly errors."""

    try:
        from bson import ObjectId
    except ModuleNotFoundError as error:  # pragma: no cover - import guard
        raise InvalidRecordIdError(
            "The 'pymongo' dependency is required to work with MongoDB ObjectId values. "
            "Install it with `pip install pymongo`."
        ) from error

    try:
        return ObjectId(value)
    except Exception as exc:  # noqa: BLE001 - propagate a custom error instead
        raise InvalidRecordIdError("The provided record identifier is invalid.") from exc


def _normalize_timestamp(value: datetime) -> datetime:
    """Ensure timestamps are timezone aware in UTC."""

    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _is_mock_collection(candidate: Any) -> bool:
    """Return ``True`` when the provided object is a unittest mock."""

    try:
        from unittest import mock
    except ImportError:  # pragma: no cover - standard library should exist
        return False

    return isinstance(candidate, mock.Mock)


async def create_record(
    collection: AsyncIOMotorCollection,
    payload: TimeSeriesRecordCreate,
) -> Dict[str, Any]:
    """Insert a new time-series record into MongoDB."""

    try:
        from pymongo.errors import PyMongoError as _PyMongoError
    except ModuleNotFoundError as error:  # pragma: no cover - import guard
        if not _is_mock_collection(collection):
            raise RecordPersistenceError(_MISSING_PYMONGO_MESSAGE) from error

        _PyMongoError = Exception  # type: ignore[assignment]

    PyMongoError = _PyMongoError

    document = payload.model_dump(by_alias=True)
    ttl = document.pop("ttl", None)
    document.setdefault("timestamp", datetime.now(tz=timezone.utc))
    document["timestamp"] = _normalize_timestamp(document["timestamp"])

    if ttl and ttl > 0:
        document["expires_at"] = document["timestamp"] + timedelta(seconds=ttl)

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
        from pymongo import DESCENDING
        from pymongo.errors import PyMongoError
    except ModuleNotFoundError as error:  # pragma: no cover - import guard
        raise RecordQueryError(_MISSING_PYMONGO_MESSAGE) from error

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

    try:
        from pymongo.errors import PyMongoError
    except ModuleNotFoundError as error:  # pragma: no cover - import guard
        raise RecordPersistenceError(_MISSING_PYMONGO_MESSAGE) from error

    oid = _object_id(record_id)
    update_payload = {
        k: v for k, v in updates.model_dump(by_alias=True, exclude_unset=True).items()
    }

    if "timestamp" in update_payload and isinstance(update_payload["timestamp"], datetime):
        update_payload["timestamp"] = _normalize_timestamp(update_payload["timestamp"])

    if "expires_at" in update_payload and isinstance(update_payload["expires_at"], datetime):
        update_payload["expires_at"] = _normalize_timestamp(update_payload["expires_at"])

    if not update_payload:
        raise EmptyUpdateError("At least one field must be provided for update.")

    try:
        document = await _apply_update(
            collection=collection,
            oid=oid,
            update_payload=update_payload,
        )
    except PyMongoError as exc:
        raise RecordPersistenceError("Failed to update the record in MongoDB.") from exc

    return _serialize(document)


async def _apply_update(
    collection: AsyncIOMotorCollection,
    oid: ObjectId,
    update_payload: Dict[str, Any],
) -> Dict[str, Any]:
    """Apply an update honoring MongoDB time-series constraints."""

    try:
        from pymongo import ReturnDocument
        from pymongo.errors import OperationFailure, PyMongoError
    except ModuleNotFoundError as error:  # pragma: no cover - import guard
        raise RecordPersistenceError(_MISSING_PYMONGO_MESSAGE) from error

    metadata_only = set(update_payload.keys()) <= {"metadata"}
    metadata_exception: Optional[OperationFailure] = None

    if metadata_only:
        try:
            document = await collection.find_one_and_update(
                {"_id": oid},
                {"$set": update_payload},
                return_document=ReturnDocument.AFTER,
            )
        except OperationFailure as error:
            if not _is_timeseries_restriction(error):
                raise
            metadata_exception = error
        else:
            if document is None:
                raise RecordNotFoundError("Record not found for update.")
            return document

    if metadata_only and metadata_exception is None:
        # Metadata update failed because the document no longer exists.
        raise RecordNotFoundError("Record not found for update.")

    return await _replace_document(
        collection=collection,
        oid=oid,
        update_payload=update_payload,
    )


async def _replace_document(
    collection: AsyncIOMotorCollection,
    oid: ObjectId,
    update_payload: Dict[str, Any],
    existing_document: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Replace a document to emulate updates on measurement fields."""

    try:
        from pymongo.errors import OperationFailure
    except ModuleNotFoundError as error:  # pragma: no cover - import guard
        raise RecordPersistenceError(_MISSING_PYMONGO_MESSAGE) from error

    if existing_document is None:
        existing_document = await collection.find_one({"_id": oid})

    if existing_document is None:
        raise RecordNotFoundError("Record not found for update.")

    replacement = copy.deepcopy(existing_document)
    replacement.update(update_payload)
    replacement["_id"] = existing_document["_id"]

    try:
        result = await collection.replace_one({"_id": oid}, replacement)
    except OperationFailure as error:
        if _is_timeseries_restriction(error):
            document = await _delete_and_reinsert(
                collection=collection,
                original=existing_document,
                replacement=replacement,
            )
            return document
        raise

    if result.matched_count == 0:
        raise RecordNotFoundError("Record not found for update.")

    return await _reload_document(collection, oid)


async def _delete_and_reinsert(
    collection: AsyncIOMotorCollection,
    original: Dict[str, Any],
    replacement: Dict[str, Any],
) -> Dict[str, Any]:
    """Fallback strategy when direct updates are rejected by MongoDB."""

    try:
        from pymongo.errors import PyMongoError
    except ModuleNotFoundError as error:  # pragma: no cover - import guard
        raise RecordPersistenceError(_MISSING_PYMONGO_MESSAGE) from error

    delete_result = await collection.delete_one({"_id": original["_id"]})
    if delete_result.deleted_count == 0:
        raise RecordNotFoundError("Record not found for update.")

    replacement["_id"] = original["_id"]

    try:
        await collection.insert_one(replacement)
    except PyMongoError as error:
        # Attempt to restore the original document if reinsertion fails.
        try:
            await collection.insert_one(copy.deepcopy(original))
        except PyMongoError:
            pass
        raise error

    return await _reload_document(collection, original["_id"])


async def _reload_document(
    collection: AsyncIOMotorCollection,
    oid: ObjectId,
) -> Dict[str, Any]:
    """Retrieve a document after an update operation has completed."""

    document = await collection.find_one({"_id": oid})
    if document is None:
        raise RecordPersistenceError("Failed to load the updated record from MongoDB.")
    return document


def _is_timeseries_restriction(error: OperationFailure) -> bool:
    """Return ``True`` when MongoDB rejects updates due to time-series rules."""

    message = str(error).lower()
    return "time-series" in message or "time series" in message or "metafield" in message


async def delete_record(
    collection: AsyncIOMotorCollection,
    record_id: str,
) -> None:
    """Remove a record from MongoDB."""

    try:
        from pymongo.errors import PyMongoError
    except ModuleNotFoundError as error:  # pragma: no cover - import guard
        raise RecordDeletionError(_MISSING_PYMONGO_MESSAGE) from error

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

    try:
        from pymongo import DESCENDING
        from pymongo.errors import PyMongoError
    except ModuleNotFoundError as error:  # pragma: no cover - import guard
        raise RecordQueryError(_MISSING_PYMONGO_MESSAGE) from error

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
