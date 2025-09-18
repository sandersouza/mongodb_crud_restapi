"""Routes dedicated to API token management."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from ..core.config import get_settings
from ..dependencies import TokenContext, require_admin_context
from ..models.tokens import APITokenCreate, APITokenResponse
from ..services import tokens as token_service

settings = get_settings()
router = APIRouter(prefix="/tokens", tags=["tokens"])


if settings.enable_token_creation_route:

    @router.post(
        "",
        status_code=status.HTTP_201_CREATED,
        response_model=APITokenResponse,
        include_in_schema=False,
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
        )
else:  # pragma: no cover - exercised implicitly when the feature is disabled
    router.routes.clear()
