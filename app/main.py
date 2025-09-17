"""Application entry-point for the MongoDB CRUD API."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .core.config import get_settings
from .db.mongo import MongoConnectionError, mongo_manager
from .routes import discover_routers, include_routers

logger = logging.getLogger(__name__)
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle application startup and shutdown."""

    try:
        await mongo_manager.connect()
        logger.info("MongoDB connection ready")
    except MongoConnectionError as error:
        logger.exception("Failed to connect to MongoDB: %s", error)
        raise

    try:
        yield
    finally:
        await mongo_manager.close()
        logger.info("MongoDB connection closed")


app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    docs_url="/docs" if settings.environment != "production" else None,
    redoc_url="/redoc" if settings.environment != "production" else None,
    lifespan=lifespan,
)


if settings.allowed_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.allowed_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


include_routers(app, discover_routers(), prefix=settings.api_prefix)
