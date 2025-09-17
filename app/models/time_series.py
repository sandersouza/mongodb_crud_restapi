"""Pydantic models used by the API."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class TimeSeriesRecordBase(BaseModel):
    """Base attributes shared across time-series representations."""

    source: str = Field(..., description="Origin identifier for the record.")
    payload: Dict[str, Any] = Field(
        default_factory=dict,
        description="Arbitrary structured data describing the measurement.",
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Optional metadata stored in the time-series meta field.",
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc),
        description="Timestamp of when the measurement was produced (UTC).",
    )


class TimeSeriesRecordCreate(TimeSeriesRecordBase):
    """Payload required to create a new time-series record."""


class TimeSeriesRecordUpdate(BaseModel):
    """Payload used for partial updates of a record."""

    source: Optional[str] = Field(None, description="Updated source identifier.")
    payload: Optional[Dict[str, Any]] = Field(
        None,
        description="Updated structured data for the record.",
    )
    metadata: Optional[Dict[str, Any]] = Field(
        None,
        description="Updated metadata stored alongside the record.",
    )
    timestamp: Optional[datetime] = Field(
        None,
        description="Override timestamp for the record (UTC).",
    )


class TimeSeriesRecordOut(TimeSeriesRecordBase):
    """Representation of a record returned to API consumers."""

    id: str = Field(..., description="MongoDB unique identifier for the record.")

    class Config:
        """Pydantic model configuration."""

        json_encoders = {datetime: lambda dt: dt.isoformat()}


class TimeSeriesSearchResponse(BaseModel):
    """Response payload returned by search endpoints."""

    latest: bool = Field(
        False,
        description="Indicates whether the response contains only the latest record.",
    )
    count: int = Field(..., ge=0, description="Number of records returned in the search.")
    items: List[TimeSeriesRecordOut] = Field(
        default_factory=list,
        description="Collection of time-series records that match the filters.",
    )
