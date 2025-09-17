"""Routes responsible for time-series CRUD operations."""

from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from motor.motor_asyncio import AsyncIOMotorCollection

from ..dependencies import get_timeseries_collection
from ..models.time_series import (
    TimeSeriesRecordCreate,
    TimeSeriesRecordOut,
    TimeSeriesRecordUpdate,
    TimeSeriesSearchResponse,
)
from ..services import records as service

router = APIRouter(prefix="/records", tags=["records"])


def _raise_http_error(error: Exception) -> None:
    """Transform service layer exceptions into HTTP errors."""

    if isinstance(error, service.InvalidRecordIdError):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(error),
        ) from error
    if isinstance(error, service.EmptyUpdateError):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(error)) from error
    if isinstance(error, service.RecordNotFoundError):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(error)) from error
    if isinstance(error, service.RecordPersistenceError):
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(error)) from error
    if isinstance(error, service.RecordDeletionError):
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(error)) from error
    if isinstance(error, service.RecordQueryError):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(error)) from error

    raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Unexpected error.") from error


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=TimeSeriesRecordOut,
    summary="Create a new time-series record",
)
async def create_record(
    record: TimeSeriesRecordCreate,
    collection: AsyncIOMotorCollection = Depends(get_timeseries_collection),
) -> TimeSeriesRecordOut:
    """Persist a new record in MongoDB."""

    try:
        document = await service.create_record(collection, record)
    except Exception as error:  # noqa: BLE001 - deliberate broad handling
        _raise_http_error(error)

    return TimeSeriesRecordOut.model_validate(document)


@router.get(
    "",
    response_model=List[TimeSeriesRecordOut],
    summary="List time-series records",
)
async def list_records(
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of items to return."),
    skip: int = Query(0, ge=0, description="Number of items to skip for pagination."),
    collection: AsyncIOMotorCollection = Depends(get_timeseries_collection),
) -> List[TimeSeriesRecordOut]:
    """Return paginated records ordered from the most recent to the oldest."""

    try:
        documents = await service.list_records(collection, limit=limit, skip=skip)
    except Exception as error:  # noqa: BLE001
        _raise_http_error(error)

    return [TimeSeriesRecordOut.model_validate(document) for document in documents]


@router.get(
    "/{record_id}",
    response_model=TimeSeriesRecordOut,
    summary="Retrieve a specific record",
)
async def get_record(
    record_id: str,
    collection: AsyncIOMotorCollection = Depends(get_timeseries_collection),
) -> TimeSeriesRecordOut:
    """Fetch a record by its identifier."""

    try:
        document = await service.fetch_record(collection, record_id)
    except Exception as error:  # noqa: BLE001
        _raise_http_error(error)

    return TimeSeriesRecordOut.model_validate(document)


@router.put(
    "/{record_id}",
    response_model=TimeSeriesRecordOut,
    summary="Update an existing record",
)
async def update_record(
    record_id: str,
    updates: TimeSeriesRecordUpdate,
    collection: AsyncIOMotorCollection = Depends(get_timeseries_collection),
) -> TimeSeriesRecordOut:
    """Update fields for an existing record."""

    try:
        document = await service.update_record(collection, record_id, updates)
    except Exception as error:  # noqa: BLE001
        _raise_http_error(error)

    return TimeSeriesRecordOut.model_validate(document)


@router.delete(
    "/{record_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
    summary="Delete a record",
)
async def delete_record(
    record_id: str,
    collection: AsyncIOMotorCollection = Depends(get_timeseries_collection),
) -> Response:
    """Remove a record from MongoDB."""

    try:
        await service.delete_record(collection, record_id)
    except Exception as error:  # noqa: BLE001
        _raise_http_error(error)

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get(
    "/search",
    response_model=TimeSeriesSearchResponse,
    summary="Search records by field and time window",
)
async def search_records(
    field: Optional[str] = Query(
        default=None,
        description="Field name to filter (supports dot-notation such as payload.temperature).",
    ),
    value: Optional[str] = Query(default=None, description="Value to match for the given field."),
    start_time: Optional[datetime] = Query(
        default=None,
        description="ISO timestamp marking the beginning of the window.",
    ),
    end_time: Optional[datetime] = Query(
        default=None,
        description="ISO timestamp marking the end of the window.",
    ),
    latest: bool = Query(
        default=False,
        description="Return only the most recent record that matches the filters.",
    ),
    limit: int = Query(
        default=100,
        ge=1,
        le=1000,
        description="Maximum number of records to return when not requesting only the latest.",
    ),
    collection: AsyncIOMotorCollection = Depends(get_timeseries_collection),
) -> TimeSeriesSearchResponse:
    """Search for records by arbitrary field while supporting time windows."""

    if latest:
        limit = 1

    if start_time and end_time and start_time > end_time:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The start_time must be before the end_time.",
        )

    try:
        documents, only_latest = await service.search_records(
            collection=collection,
            field=field,
            value=value,
            start_time=start_time,
            end_time=end_time,
            latest=latest,
            limit=limit,
        )
    except Exception as error:  # noqa: BLE001
        _raise_http_error(error)

    if latest and not documents:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No records found for the given filters.")

    items = [TimeSeriesRecordOut.model_validate(document) for document in documents]
    return TimeSeriesSearchResponse(latest=only_latest, count=len(items), items=items)
