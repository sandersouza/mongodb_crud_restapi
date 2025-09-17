"""FastAPI dependency declarations."""

from __future__ import annotations

from typing import AsyncGenerator

from motor.motor_asyncio import AsyncIOMotorCollection

from .db.mongo import mongo_manager


async def get_timeseries_collection() -> AsyncGenerator[AsyncIOMotorCollection, None]:
    """Provide the configured MongoDB time-series collection."""

    collection = mongo_manager.collection
    yield collection
