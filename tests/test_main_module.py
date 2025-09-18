"""Tests covering application bootstrap logic."""

from __future__ import annotations

import importlib

import pytest
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from unittest.mock import AsyncMock

from app.db.mongo import MongoConnectionError
from app.main import lifespan


@pytest.fixture()
def anyio_backend() -> str:
    """Execute async tests using asyncio."""

    return "asyncio"


@pytest.mark.anyio
async def test_lifespan_invokes_connect_and_close(monkeypatch: pytest.MonkeyPatch) -> None:
    """The application lifespan should manage Mongo connections."""

    connect = AsyncMock()
    close = AsyncMock()
    monkeypatch.setattr("app.main.mongo_manager.connect", connect)
    monkeypatch.setattr("app.main.mongo_manager.close", close)

    async with lifespan(FastAPI()):
        pass

    connect.assert_awaited_once()
    close.assert_awaited_once()


@pytest.mark.anyio
async def test_lifespan_propagates_connection_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Connection failures should bubble up during startup."""

    connect = AsyncMock(side_effect=MongoConnectionError("down"))
    close = AsyncMock()
    monkeypatch.setattr("app.main.mongo_manager.connect", connect)
    monkeypatch.setattr("app.main.mongo_manager.close", close)

    with pytest.raises(MongoConnectionError):
        async with lifespan(FastAPI()):
            pass

    close.assert_not_awaited()


def test_app_includes_cors_when_origins(monkeypatch: pytest.MonkeyPatch) -> None:
    """CORS middleware should be added when allowed origins are configured."""

    import app.main as app_main

    monkeypatch.setenv("ALLOWED_ORIGINS", "http://example.com")
    reloaded = importlib.reload(app_main)
    try:
        assert any(m.cls is CORSMiddleware for m in reloaded.app.user_middleware)
    finally:
        monkeypatch.delenv("ALLOWED_ORIGINS", raising=False)
        importlib.reload(reloaded)
