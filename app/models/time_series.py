"""Pydantic models used by the API."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_serializer, field_validator


class TimeSeriesRecordBase(BaseModel):
    """Base attributes shared across time-series representations."""

    model_config = ConfigDict(populate_by_name=True)

    source: str = Field(
        ...,
        validation_alias=AliasChoices("source", "acronym"),
        serialization_alias="acronym",
        description="Origin identifier for the record (alias: acronym).",
    )
    component: Optional[str] = Field(
        default=None,
        description="Logical component associated with the record.",
    )
    payload: Any = Field(
        ...,
        description="Arbitrary data describing the measurement (any JSON value).",
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

    expires_in_seconds: Optional[int] = Field(
        default=None,
        description=(
            "Optional TTL for the record expressed in seconds. "
            "If omitted or set to 0 the record never expires."
        ),
    )

    @field_validator("expires_in_seconds")
    @classmethod
    def validate_ttl(cls, value: Optional[int]) -> Optional[int]:
        if value is None:
            return None
        if value < 0:
            raise ValueError("expires_in_seconds must be greater than or equal to 0.")
        return value


class TimeSeriesRecordUpdate(BaseModel):
    """Payload used for partial updates of a record."""

    source: Optional[str] = Field(
        None,
        validation_alias=AliasChoices("source", "acronym"),
        serialization_alias="acronym",
        description="Updated source identifier (alias: acronym).",
    )
    component: Optional[str] = Field(
        None,
        description="Updated component associated with the record.",
    )
    payload: Optional[Any] = Field(
        None,
        description="Updated data for the record.",
    )
    metadata: Optional[Dict[str, Any]] = Field(
        None,
        description="Updated metadata stored alongside the record.",
    )
    timestamp: Optional[datetime] = Field(
        None,
        description="Override timestamp for the record (UTC).",
    )
    expires_at: Optional[datetime] = Field(
        None,
        description="UTC timestamp indicating when the record should expire.",
    )


class TimeSeriesRecordOut(TimeSeriesRecordBase):
    """Representation of a record returned to API consumers."""

    id: str = Field(..., description="MongoDB unique identifier for the record.")
    expires_at: Optional[datetime] = Field(
        default=None,
        description="UTC timestamp indicating when the record will expire, if any.",
    )

    @field_serializer("timestamp")
    def _serialize_timestamp(self, timestamp: datetime) -> str:
        """Render timestamps using ISO-8601 formatting when serializing to JSON."""

        return timestamp.isoformat()

    @field_serializer("expires_at")
    def _serialize_expires_at(self, expires_at: Optional[datetime]) -> Optional[str]:
        """Render expiration timestamps using ISO-8601 formatting when serializing to JSON."""

        if expires_at is None:
            return None
        return expires_at.isoformat()


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
