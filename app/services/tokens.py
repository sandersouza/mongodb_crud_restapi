"""Service utilities for managing API tokens."""

from __future__ import annotations

import hashlib
import logging
import secrets
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

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


@dataclass
class CreatedToken(TokenMetadata):
    """Details about a newly created token including its secret value."""

    token: str


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

    document = {
        "token_hash": token_hash,
        "database": database,
        "description": description,
        "created_at": now,
        "last_used_at": None,
    }

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
    )
