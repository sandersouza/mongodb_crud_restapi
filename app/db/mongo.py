"""MongoDB connection and lifecycle management utilities."""

from __future__ import annotations

import logging
from typing import Dict, Optional

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
        self._token_collection: Optional[AsyncIOMotorCollection] = None
        self._database_cache: Dict[str, AsyncIOMotorDatabase] = {}
        self._collection_cache: Dict[str, AsyncIOMotorCollection] = {}

    @property
    def client(self) -> AsyncIOMotorClient:
        """Return the current MongoDB client instance."""

        if self._client is None:
            raise MongoConnectionError("MongoDB client has not been initialized.")
        return self._client

    @property
    def database(self) -> AsyncIOMotorDatabase:
        """Return the current MongoDB database instance."""

        if self._database is None:
            raise MongoConnectionError("MongoDB database has not been initialized.")
        return self._database

    @property
    def collection(self) -> AsyncIOMotorCollection:
        """Return the configured time-series collection."""

        if self._collection is None:
            raise MongoConnectionError("MongoDB collection has not been initialized.")
        return self._collection

    @property
    def token_collection(self) -> AsyncIOMotorCollection:
        """Return the collection that stores API tokens."""

        if self._token_collection is None:
            raise MongoConnectionError("MongoDB token collection has not been initialized.")
        return self._token_collection

    async def connect(self) -> None:
        """Create a new MongoDB connection if one does not already exist."""

        if self._client is not None:
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

        self._database_cache.clear()
        self._collection_cache.clear()

        default_database = await self._get_database(settings.mongodb_database)
        self._database = default_database
        self._collection = await self._ensure_timeseries_collection(
            default_database, settings.mongodb_database
        )
        await self._ensure_token_collection(default_database)

    async def _get_database(self, database_name: str) -> AsyncIOMotorDatabase:
        """Return (and cache) a database instance, creating it if necessary."""

        if self._client is None:
            raise MongoConnectionError("MongoDB client has not been initialized.")

        if database_name in self._database_cache:
            return self._database_cache[database_name]

        database = self._client[database_name]
        existing_databases = await self._client.list_database_names()
        if database_name not in existing_databases:
            logger.info("Database %s not found. It will be created automatically.", database_name)

        self._database_cache[database_name] = database
        return database

    async def _ensure_timeseries_collection(
        self, database: AsyncIOMotorDatabase, database_name: str
    ) -> AsyncIOMotorCollection:
        """Create a time-series collection for the given database if needed."""

        settings = get_settings()

        existing_collections = await database.list_collection_names()
        if settings.mongodb_collection not in existing_collections:
            logger.info(
                "Creating time-series collection %s in database %s",
                settings.mongodb_collection,
                database_name,
            )
            timeseries_options = {"timeField": settings.timeseries_time_field}
            if settings.timeseries_meta_field:
                timeseries_options["metaField"] = settings.timeseries_meta_field
            try:
                await database.create_collection(
                    settings.mongodb_collection,
                    timeseries=timeseries_options,
                )
            except CollectionInvalid:
                logger.warning(
                    "Collection %s already exists despite initial check.",
                    settings.mongodb_collection,
                )

        collection = database[settings.mongodb_collection]
        await self._ensure_indexes(collection)
        self._collection_cache[database_name] = collection
        return collection

    async def _ensure_indexes(self, collection: AsyncIOMotorCollection) -> None:
        """Ensure indexes exist for efficient time-based queries."""

        settings = get_settings()
        try:
            await collection.create_index([(settings.timeseries_time_field, ASCENDING)])
        except PyMongoError as error:
            logger.exception("Failed to ensure indexes: %s", error)
            raise MongoConnectionError("Failed to ensure MongoDB indexes.") from error

    async def _ensure_token_collection(self, database: AsyncIOMotorDatabase) -> None:
        """Create the collection responsible for storing API tokens."""

        settings = get_settings()
        collection_name = settings.api_tokens_collection

        existing_collections = await database.list_collection_names()
        if collection_name not in existing_collections:
            logger.info(
                "Creating API token collection %s in database %s",
                collection_name,
                database.name,
            )
            await database.create_collection(collection_name)

        collection = database[collection_name]
        try:
            await collection.create_index("token_hash", unique=True)
        except PyMongoError as error:
            logger.exception("Failed to ensure API token indexes: %s", error)
            raise MongoConnectionError("Failed to ensure MongoDB token indexes.") from error

        self._token_collection = collection

    async def get_timeseries_collection_for_database(
        self, database_name: str
    ) -> AsyncIOMotorCollection:
        """Return the time-series collection associated with ``database_name``."""

        if database_name in self._collection_cache:
            return self._collection_cache[database_name]

        database = await self._get_database(database_name)
        return await self._ensure_timeseries_collection(database, database_name)

    async def close(self) -> None:
        """Terminate the MongoDB connection."""

        if self._client is not None:
            logger.info("Closing MongoDB connection")
            self._client.close()
            self._client = None
            self._database = None
            self._collection = None
            self._token_collection = None
            self._database_cache = {}
            self._collection_cache = {}


mongo_manager = MongoDBManager()
"""Singleton MongoDB manager used by the application."""
