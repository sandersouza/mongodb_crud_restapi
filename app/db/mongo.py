"""MongoDB connection and lifecycle management utilities."""

from __future__ import annotations

import logging
from typing import Optional

from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorCollection,
    AsyncIOMotorDatabase,
)
from pymongo import ASCENDING
from pymongo.errors import CollectionInvalid, PyMongoError, ServerSelectionTimeoutError

from ..core.config import get_settings

logger = logging.getLogger(__name__)


class MongoConnectionError(RuntimeError):
    """Raised when the application cannot communicate with MongoDB."""


class MongoDBManager:
    """Manage MongoDB client, database and collection lifecycle."""

    def __init__(self) -> None:
        self._client: Optional[AsyncIOMotorClient] = None
        self._database: Optional[AsyncIOMotorDatabase] = None
        self._collection: Optional[AsyncIOMotorCollection] = None

    @property
    def client(self) -> AsyncIOMotorClient:
        """Return the current MongoDB client instance."""

        if not self._client:
            raise MongoConnectionError("MongoDB client has not been initialized.")
        return self._client

    @property
    def database(self) -> AsyncIOMotorDatabase:
        """Return the current MongoDB database instance."""

        if not self._database:
            raise MongoConnectionError("MongoDB database has not been initialized.")
        return self._database

    @property
    def collection(self) -> AsyncIOMotorCollection:
        """Return the configured time-series collection."""

        if not self._collection:
            raise MongoConnectionError("MongoDB collection has not been initialized.")
        return self._collection

    async def connect(self) -> None:
        """Create a new MongoDB connection if one does not already exist."""

        if self._client:
            return

        settings = get_settings()
        logger.info("Connecting to MongoDB at %s", settings.mongodb_uri)

        connection_kwargs = {"maxPoolSize": settings.mongodb_max_pool_size}
        if settings.mongodb_username and settings.mongodb_password:
            connection_kwargs["username"] = settings.mongodb_username
            connection_kwargs["password"] = settings.mongodb_password

        try:
            self._client = AsyncIOMotorClient(settings.mongodb_uri, **connection_kwargs)
            await self._client.server_info()
        except ServerSelectionTimeoutError as error:
            logger.exception("Could not connect to MongoDB: %s", error)
            raise MongoConnectionError("Unable to establish a connection to MongoDB.") from error

        self._database = self._client[settings.mongodb_database]
        await self._ensure_timeseries_collection()

    async def _ensure_timeseries_collection(self) -> None:
        """Create the configured database and time-series collection if needed."""

        settings = get_settings()
        assert self._database is not None  # For type checkers

        existing_databases = await self._client.list_database_names()  # type: ignore[union-attr]
        if settings.mongodb_database not in existing_databases:
            logger.info("Database %s not found. It will be created automatically.", settings.mongodb_database)

        existing_collections = await self._database.list_collection_names()
        if settings.mongodb_collection not in existing_collections:
            logger.info("Creating time-series collection %s", settings.mongodb_collection)
            timeseries_options = {"timeField": settings.timeseries_time_field}
            if settings.timeseries_meta_field:
                timeseries_options["metaField"] = settings.timeseries_meta_field
            try:
                await self._database.create_collection(
                    settings.mongodb_collection,
                    timeseries=timeseries_options,
                )
            except CollectionInvalid:
                logger.warning(
                    "Collection %s already exists despite initial check.",
                    settings.mongodb_collection,
                )

        self._collection = self._database[settings.mongodb_collection]
        await self._ensure_indexes()

    async def _ensure_indexes(self) -> None:
        """Ensure indexes exist for efficient time-based queries."""

        settings = get_settings()
        assert self._collection is not None
        try:
            await self._collection.create_index([(settings.timeseries_time_field, ASCENDING)])
        except PyMongoError as error:
            logger.exception("Failed to ensure indexes: %s", error)
            raise MongoConnectionError("Failed to ensure MongoDB indexes.") from error

    async def close(self) -> None:
        """Terminate the MongoDB connection."""

        if self._client:
            logger.info("Closing MongoDB connection")
            self._client.close()
            self._client = None
            self._database = None
            self._collection = None


mongo_manager = MongoDBManager()
"""Singleton MongoDB manager used by the application."""
