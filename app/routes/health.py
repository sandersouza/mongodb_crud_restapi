"""Health-check endpoints for the service."""

from __future__ import annotations

from fastapi import APIRouter

router = APIRouter(tags=["health"])
router.skip_app_prefix = True  # type: ignore[attr-defined]


@router.get("/healthz", summary="Service health-check")
async def health_check() -> dict[str, str]:
    """Return a successful status when the service is ready."""

    return {"status": "ok"}
