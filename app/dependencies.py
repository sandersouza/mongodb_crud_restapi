"""FastAPI dependency declarations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import AsyncGenerator, Optional

from fastapi import Depends, Header, HTTPException, status
from motor.motor_asyncio import AsyncIOMotorCollection

from .core.config import get_settings
from .db.mongo import MongoConnectionError, mongo_manager
from .services.tokens import (
    TokenNotFoundError,
    TokenPersistenceError,
    fetch_token_metadata,
)

API_TOKEN_HEADER = "X-API-Token"
DATABASE_OVERRIDE_HEADER = "X-Database-Name"


@dataclass
class TokenContext:
    """Information about the caller extracted from the API token."""

    token: str
    database_name: str
    is_admin: bool


async def get_token_context(
    api_token: Optional[str] = Header(
        default=None,
        alias=API_TOKEN_HEADER,
        convert_underscores=False,
    ),
    database_override: Optional[str] = Header(
        default=None,
        alias=DATABASE_OVERRIDE_HEADER,
        convert_underscores=False,
    ),
) -> TokenContext:
    """Validate the provided API token and resolve the target database."""

    settings = get_settings()

    if not api_token:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="API token required.")

    override = database_override.strip() if database_override else None

    if api_token == settings.api_admin_token:
        database_name = override or settings.mongodb_database
        return TokenContext(token=api_token, database_name=database_name, is_admin=True)

    try:
        metadata = await fetch_token_metadata(api_token)
    except TokenNotFoundError as error:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Invalid API token.") from error
    except TokenPersistenceError as error:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Unable to validate API token.",
        ) from error

    if override and override != metadata.database:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            detail="The provided token does not grant access to the requested database.",
        )

    return TokenContext(token=api_token, database_name=metadata.database, is_admin=False)


async def require_admin_context(
    context: TokenContext = Depends(get_token_context),
) -> TokenContext:
    """Ensure the caller is using the administrator token."""

    if not context.is_admin:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail="Administrator token required.")
    return context


async def get_timeseries_collection(
    context: TokenContext = Depends(get_token_context),
) -> AsyncGenerator[AsyncIOMotorCollection, None]:
    """Provide a MongoDB collection based on the caller token context."""

    try:
        collection = await mongo_manager.get_timeseries_collection_for_database(
            context.database_name
        )
    except MongoConnectionError as error:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="MongoDB connection is not available.",
        ) from error

    yield collection
