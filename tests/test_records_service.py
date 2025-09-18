"""Tests covering record service helpers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.models.time_series import TimeSeriesRecordCreate
from app.services import records


@pytest.fixture()
def anyio_backend() -> str:
    """Force the anyio plugin to run tests using the asyncio backend."""

    return "asyncio"


@pytest.mark.anyio
async def test_create_record_applies_expiration() -> None:
    """Ensure records store an expires_at value when requested."""

    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    collection = AsyncMock()
    collection.insert_one = AsyncMock(return_value=SimpleNamespace(inserted_id="abc123"))
    expected_expires_at = now + timedelta(seconds=600)
    collection.find_one = AsyncMock(
        return_value={
            "_id": "abc123",
            "source": "sensor",
            "component": None,
            "payload": {"foo": "bar"},
            "metadata": {},
            "timestamp": now,
            "expires_at": expected_expires_at,
        }
    )

    payload = TimeSeriesRecordCreate(
        source="sensor",
        payload={"foo": "bar"},
        metadata={},
        timestamp=now,
        expires_in_seconds=600,
    )

    document = await records.create_record(collection, payload)

    inserted_document = collection.insert_one.await_args.args[0]
    assert inserted_document["expires_at"] == expected_expires_at
    assert document["expires_at"] == expected_expires_at


@pytest.mark.anyio
async def test_create_record_without_expiration() -> None:
    """Ensure records omit expires_at when no TTL is provided."""

    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    collection = AsyncMock()
    collection.insert_one = AsyncMock(return_value=SimpleNamespace(inserted_id="abc123"))
    collection.find_one = AsyncMock(
        return_value={
            "_id": "abc123",
            "source": "sensor",
            "component": None,
            "payload": {"foo": "bar"},
            "metadata": {},
            "timestamp": now,
        }
    )

    payload = TimeSeriesRecordCreate(
        source="sensor",
        payload={"foo": "bar"},
        metadata={},
        timestamp=now,
    )

    document = await records.create_record(collection, payload)

    inserted_document = collection.insert_one.await_args.args[0]
    assert "expires_at" not in inserted_document
    assert "expires_at" not in document
