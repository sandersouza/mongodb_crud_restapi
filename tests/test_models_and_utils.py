"""Unit tests covering utility functions and Pydantic models."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.models.time_series import (
    TimeSeriesRecordCreate,
    TimeSeriesRecordOut,
    TimeSeriesRecordUpdate,
)
from app.models.tokens import APITokenCreate
from app.utils.parsing import coerce_value


def test_coerce_value_parses_json() -> None:
    """Raw JSON strings should be parsed into the corresponding Python value."""

    assert coerce_value("[1, 2, 3]") == [1, 2, 3]
    assert coerce_value("{\"foo\": \"bar\"}") == {"foo": "bar"}


def test_coerce_value_recognises_booleans() -> None:
    """Human friendly boolean strings should be converted to ``bool`` values."""

    assert coerce_value("true") is True
    assert coerce_value("FALSE") is False


def test_coerce_value_returns_original_string() -> None:
    """Values that cannot be coerced should be returned unchanged."""

    assert coerce_value("not-json") == "not-json"


def test_time_series_create_ttl_validation() -> None:
    """The create payload must reject negative TTL values."""

    payload = TimeSeriesRecordCreate(
        source="sensor",
        payload={},
        metadata={},
        ttl=0,
    )
    assert payload.ttl == 0

    with pytest.raises(ValueError, match="greater than or equal to 0"):
        TimeSeriesRecordCreate(
            source="sensor",
            payload={},
            metadata={},
            ttl=-1,
        )


def test_time_series_update_allows_partial_fields() -> None:
    """Updates should allow omitting optional fields without validation errors."""

    update = TimeSeriesRecordUpdate()
    assert update.model_dump(exclude_unset=True) == {}


def test_time_series_output_serializes_datetimes() -> None:
    """The response model should serialise datetimes to ISO formatted strings."""

    timestamp = datetime(2024, 1, 1, tzinfo=timezone.utc)
    expires_at = datetime(2024, 1, 2, tzinfo=timezone.utc)
    record = TimeSeriesRecordOut(
        id="abc123",
        source="sensor",
        payload={},
        metadata={},
        timestamp=timestamp,
        expires_at=expires_at,
    )

    dumped = record.model_dump(mode="json")
    assert dumped["timestamp"] == "2024-01-01T00:00:00+00:00"
    assert dumped["expires_at"] == "2024-01-02T00:00:00+00:00"


def test_api_token_create_validators() -> None:
    """Token creation payload should normalise whitespace and validate fields."""

    payload = APITokenCreate(database="  analytics  ")
    assert payload.database == "analytics"

    with pytest.raises(ValueError, match="must not be empty"):
        APITokenCreate(database="   ")

    with pytest.raises(ValueError, match="token must not be empty"):
        APITokenCreate(database="db", token="")

    with pytest.raises(ValueError, match="greater than or equal to 0"):
        APITokenCreate(database="db", ttl=-5)
