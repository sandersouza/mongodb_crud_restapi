"""Routes dedicated to API token management."""

from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status

from ..core.config import get_settings
from ..dependencies import TokenContext, require_admin_context
from ..models.tokens import APITokenCreate, APITokenResponse, APITokenStoredResponse
from ..services import tokens as token_service

settings = get_settings()
router = APIRouter(prefix="/tokens", tags=["tokens"])

include_in_schema = settings.show_token_creation_route


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=APITokenResponse,
    include_in_schema=include_in_schema,
    summary="Create a new API token",
)
async def create_api_token(
    payload: APITokenCreate,
    _: TokenContext = Depends(require_admin_context),
) -> APITokenResponse:
    """Create a token tied to a specific MongoDB database."""

    try:
        created = await token_service.create_token(
            database=payload.database,
            token_value=payload.token,
            description=payload.description,
            ttl=payload.ttl,
        )
    except token_service.TokenConflictError as error:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(error),
        ) from error
    except token_service.TokenPersistenceError as error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(error),
        ) from error

    return APITokenResponse(
        token=created.token,
        database=created.database,
        description=created.description,
        created_at=created.created_at,
        last_used_at=created.last_used_at,
        expires_at=created.expires_at,
    )


@router.get(
    "",
    response_model=List[APITokenStoredResponse],
    include_in_schema=include_in_schema,
    summary="List stored API tokens",
)
async def list_api_tokens(
    database: Optional[str] = Query(
        default=None,
        description="Optional database name to scope the results.",
    ),
    _: TokenContext = Depends(require_admin_context),
) -> List[APITokenStoredResponse]:
    """Return every stored token and its associated database."""

    try:
        tokens = await token_service.list_tokens(database=database)
    except token_service.TokenPersistenceError as error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(error),
        ) from error

    return [
        APITokenStoredResponse(
            id=token.id,
            database=token.database,
            description=token.description,
            created_at=token.created_at,
            last_used_at=token.last_used_at,
            expires_at=token.expires_at,
        )
        for token in tokens
    ]


@router.delete(
    "/{database}/{token_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
    response_model=None,
    include_in_schema=include_in_schema,
    summary="Revoke an API token",
)
async def revoke_api_token(
    database: str,
    token_id: str,
    _: TokenContext = Depends(require_admin_context),
) -> None:
    """Remove a stored API token from the requested database."""

    try:
        await token_service.revoke_token(database=database, token_id=token_id)
    except token_service.TokenNotFoundError as error:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(error),
        ) from error
    except token_service.TokenPersistenceError as error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(error),
        ) from error
