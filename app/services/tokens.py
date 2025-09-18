"""Service utilities for managing API tokens."""

from __future__ import annotations

import hashlib
import logging
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional

try:  # pragma: no cover - exercised indirectly through import guards
    from bson import ObjectId
    from bson.errors import InvalidId
except ModuleNotFoundError:  # pragma: no cover - fallback for test environments
    class InvalidId(ValueError):
        """Exception raised when an ObjectId string is invalid."""


    class ObjectId(str):
        """Lightweight stand-in for ``bson.ObjectId`` used in tests.

        Preserves the validation semantics for 24-character hexadecimal strings so
        unit tests can run without the optional ``bson`` dependency installed.
        """

        def __new__(cls, value: str):
            if not isinstance(value, str):
                raise InvalidId("ObjectId must be created from a hex string.")

            candidate = value.strip()
            if len(candidate) != 24:
                raise InvalidId("ObjectId hex string must be exactly 24 characters long.")

            try:
                int(candidate, 16)
            except ValueError as error:  # pragma: no cover - defensive guard
                raise InvalidId("ObjectId hex string contains non-hexadecimal characters.") from error

            return str.__new__(cls, candidate)

from ..db.mongo import MongoConnectionError, mongo_manager

logger = logging.getLogger(__name__)


class TokenServiceError(RuntimeError):
    """Base exception for token management failures."""


class TokenNotFoundError(TokenServiceError):
    """Raised when no token matches the provided value."""


class TokenConflictError(TokenServiceError):
    """Raised when attempting to create a token that already exists."""


class TokenPersistenceError(TokenServiceError):
    """Raised when the token collection cannot be queried or updated."""


def _require_pymongo_errors():
    """Import PyMongo exceptions, raising a helpful message if missing."""

    try:
        from pymongo.errors import DuplicateKeyError, PyMongoError
    except ModuleNotFoundError as error:  # pragma: no cover - import guard
        raise TokenPersistenceError(
            "The 'pymongo' dependency is required for token storage. "
            "Install it with `pip install pymongo`."
        ) from error

    return DuplicateKeyError, PyMongoError


@dataclass
class TokenMetadata:
    """Persisted information about an API token."""

    database: str
    description: Optional[str]
    created_at: datetime
    last_used_at: Optional[datetime]
    expires_at: Optional[datetime]


@dataclass
class CreatedToken(TokenMetadata):
    """Details about a newly created token including its secret value."""

    token: str


@dataclass
class StoredToken(TokenMetadata):
    """Token metadata augmented with the database document identifier."""

    id: str


def _hash_token(token: str) -> str:
    """Return the SHA-256 hash for ``token``."""

    return hashlib.sha256(token.encode("utf-8")).hexdigest()


async def fetch_token_metadata(token: str) -> TokenMetadata:
    """Retrieve token metadata for ``token``.

    Updates the ``last_used_at`` field upon successful retrieval.
    """

    token_hash = _hash_token(token)

    try:
        document, collection = await mongo_manager.find_token_document(token_hash)
    except MongoConnectionError as error:  # pragma: no cover - sanity guard
        raise TokenPersistenceError("Token storage is not available.") from error

    if document is None:
        raise TokenNotFoundError("Invalid API token.")

    metadata = TokenMetadata(
        database=document["database"],
        description=document.get("description"),
        created_at=document["created_at"],
        last_used_at=document.get("last_used_at"),
        expires_at=document.get("expires_at"),
    )

    _, PyMongoError = _require_pymongo_errors()

    try:
        await collection.update_one(
            {"_id": document["_id"]},
            {"$set": {"last_used_at": datetime.now(timezone.utc)}},
        )
    except PyMongoError as error:
        logger.exception("Failed to update token last usage timestamp: %s", error)
        raise TokenPersistenceError("Unable to update token usage timestamp.") from error

    return metadata


async def create_token(
    *,
    database: str,
    token_value: Optional[str] = None,
    description: Optional[str] = None,
    ttl: Optional[int] = None,
    token_length: int = 32,
) -> CreatedToken:
    """Create a new token associated with ``database``.

    If ``token_value`` is not provided a secure random hex string is generated.
    """

    try:
        await mongo_manager.get_timeseries_collection_for_database(database)
    except MongoConnectionError as error:
        logger.exception("Failed to prepare database %s for new token: %s", database, error)
        raise TokenPersistenceError("Unable to prepare the requested database.") from error

    try:
        collection = await mongo_manager.get_token_collection_for_database(database)
    except MongoConnectionError as error:  # pragma: no cover - sanity guard
        raise TokenPersistenceError("Token storage is not available.") from error

    token_secret = token_value or secrets.token_hex(token_length // 2)
    token_hash = _hash_token(token_secret)
    now = datetime.now(timezone.utc)
    expires_at = (
        now + timedelta(seconds=ttl)
        if ttl and ttl > 0
        else None
    )

    document = {
        "token_hash": token_hash,
        "database": database,
        "description": description,
        "created_at": now,
        "last_used_at": None,
    }

    if expires_at is not None:
        document["expires_at"] = expires_at

    DuplicateKeyError, PyMongoError = _require_pymongo_errors()
    try:
        await collection.insert_one(document)
    except DuplicateKeyError as error:
        raise TokenConflictError("A token with the provided value already exists.") from error
    except PyMongoError as error:
        logger.exception("Failed to persist API token: %s", error)
        raise TokenPersistenceError("Unable to store the new API token.") from error

    mongo_manager.remember_token_location(token_hash, database)

    return CreatedToken(
        token=token_secret,
        database=database,
        description=description,
        created_at=now,
        last_used_at=None,
        expires_at=expires_at,
    )


async def list_tokens(database: Optional[str] = None) -> List[StoredToken]:
    """Return metadata for every stored token, optionally scoped to a database."""

    try:
        collections = await mongo_manager.iter_token_collections(database)
    except MongoConnectionError as error:  # pragma: no cover - defensive guard
        raise TokenPersistenceError("Token storage is not available.") from error

    _, PyMongoError = _require_pymongo_errors()

    tokens: List[StoredToken] = []
    for database_name, collection in collections:
        try:
            async for document in collection.find():
                tokens.append(
                    StoredToken(
                        id=str(document["_id"]),
                        database=database_name,
                        description=document.get("description"),
                        created_at=document["created_at"],
                        last_used_at=document.get("last_used_at"),
                        expires_at=document.get("expires_at"),
                    )
                )
        except PyMongoError as error:
            logger.exception("Failed to list API tokens: %s", error)
            raise TokenPersistenceError("Unable to query stored API tokens.") from error

    return tokens


async def revoke_token(*, database: str, token_id: str) -> None:
    """Delete the token with ``token_id`` persisted inside ``database``."""

    try:
        object_id = ObjectId(token_id)
    except InvalidId as error:
        raise TokenNotFoundError("Token not found for the requested database.") from error

    try:
        collection = await mongo_manager.get_token_collection_for_database(database)
    except MongoConnectionError as error:  # pragma: no cover - defensive guard
        raise TokenPersistenceError("Token storage is not available.") from error

    _, PyMongoError = _require_pymongo_errors()

    try:
        document = await collection.find_one_and_delete({"_id": object_id})
    except PyMongoError as error:
        logger.exception("Failed to revoke API token: %s", error)
        raise TokenPersistenceError("Unable to revoke the requested API token.") from error

    if document is None:
        raise TokenNotFoundError("Token not found for the requested database.")

    token_hash = document.get("token_hash")
    if token_hash:
        mongo_manager.forget_token_location(token_hash)
