"""MongoDB connection and lifecycle management utilities."""

from __future__ import annotations

import logging
from typing import Dict, Optional, Tuple, TYPE_CHECKING

try:  # pragma: no cover - optional dependency
    from pymongo import ASCENDING
    from pymongo.errors import CollectionInvalid, PyMongoError, ServerSelectionTimeoutError
except ModuleNotFoundError:  # pragma: no cover - fallback definitions for optional dependency
    ASCENDING = 1  # type: ignore[assignment]

    class _MissingPyMongoError(RuntimeError):
        """Base class for placeholder exceptions when PyMongo is unavailable."""

    class CollectionInvalid(_MissingPyMongoError):
        """Placeholder for :class:`pymongo.errors.CollectionInvalid`."""

    class PyMongoError(_MissingPyMongoError):
        """Placeholder for :class:`pymongo.errors.PyMongoError`."""

    class ServerSelectionTimeoutError(_MissingPyMongoError):
        """Placeholder for :class:`pymongo.errors.ServerSelectionTimeoutError`."""

    _PYMONGO_AVAILABLE = False
else:
    _PYMONGO_AVAILABLE = True

if TYPE_CHECKING:  # pragma: no cover - imported for type checking only
    from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection, AsyncIOMotorDatabase

from ..core.config import get_settings

logger = logging.getLogger(__name__)


class MongoConnectionError(RuntimeError):
    """Raised when the application cannot communicate with MongoDB."""


class MongoDBManager:
    """Manage MongoDB client, database and collection lifecycle."""

    def __init__(self) -> None:
        self._client: Optional[AsyncIOMotorClient] = None
        self._database_cache: Dict[str, AsyncIOMotorDatabase] = {}
        self._collection_cache: Dict[str, AsyncIOMotorCollection] = {}
        self._token_collection_cache: Dict[str, AsyncIOMotorCollection] = {}
        self._token_hash_cache: Dict[str, str] = {}

    @property
    def client(self) -> AsyncIOMotorClient:
        """Return the current MongoDB client instance."""

        if self._client is None:
            raise MongoConnectionError("MongoDB client has not been initialized.")
        return self._client

    async def connect(self) -> None:
        """Create a new MongoDB connection if one does not already exist."""

        if self._client is not None:
            return

        settings = get_settings()
        logger.info("Connecting to MongoDB at %s", settings.mongodb_uri)

        try:
            from motor.motor_asyncio import AsyncIOMotorClient  # type: ignore import-not-found
        except ModuleNotFoundError as error:  # pragma: no cover - defensive guard
            raise MongoConnectionError(
                "The 'motor' package is required to connect to MongoDB. Install it with `pip install motor`."
            ) from error

        if not _PYMONGO_AVAILABLE:
            raise MongoConnectionError(
                "The 'pymongo' package is required to connect to MongoDB. Install it with `pip install pymongo`."
            )

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
        self._token_collection_cache.clear()
        self._token_hash_cache.clear()

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

    async def _ensure_token_collection(
        self, database: AsyncIOMotorDatabase
    ) -> AsyncIOMotorCollection:
        """Create the collection responsible for storing API tokens."""

        database_name = database.name
        if database_name in self._token_collection_cache:
            return self._token_collection_cache[database_name]

        settings = get_settings()
        collection_name = settings.api_tokens_collection

        existing_collections = await database.list_collection_names()
        if collection_name not in existing_collections:
            logger.info(
                "Creating API token collection %s in database %s",
                collection_name,
                database_name,
            )
            await database.create_collection(collection_name)

        collection = database[collection_name]
        try:
            await collection.create_index("token_hash", unique=True)
        except PyMongoError as error:
            logger.exception("Failed to ensure API token indexes: %s", error)
            raise MongoConnectionError("Failed to ensure MongoDB token indexes.") from error

        self._token_collection_cache[database_name] = collection
        return collection

    async def get_timeseries_collection_for_database(
        self, database_name: str
    ) -> AsyncIOMotorCollection:
        """Return the time-series collection associated with ``database_name``."""

        if database_name in self._collection_cache:
            return self._collection_cache[database_name]

        database = await self._get_database(database_name)
        return await self._ensure_timeseries_collection(database, database_name)

    async def get_token_collection_for_database(
        self, database_name: str
    ) -> AsyncIOMotorCollection:
        """Return the token collection stored inside ``database_name``."""

        database = await self._get_database(database_name)
        return await self._ensure_token_collection(database)

    def remember_token_location(self, token_hash: str, database_name: str) -> None:
        """Cache the database where ``token_hash`` is persisted."""

        self._token_hash_cache[token_hash] = database_name

    async def find_token_document(
        self, token_hash: str
    ) -> Tuple[Optional[dict], Optional[AsyncIOMotorCollection]]:
        """Locate the token document associated with ``token_hash`` across databases."""

        if self._client is None:
            raise MongoConnectionError("MongoDB client has not been initialized.")

        settings = get_settings()

        cached_database = self._token_hash_cache.get(token_hash)
        if cached_database is not None:
            try:
                collection = await self.get_token_collection_for_database(cached_database)
            except MongoConnectionError:
                self._token_hash_cache.pop(token_hash, None)
            else:
                try:
                    document = await collection.find_one({"token_hash": token_hash})
                except PyMongoError as error:
                    logger.exception("Failed to fetch API token metadata: %s", error)
                    raise MongoConnectionError("Failed to query MongoDB for API tokens.") from error

                if document is not None:
                    return document, collection

                self._token_hash_cache.pop(token_hash, None)

        for database_name, collection in list(self._token_collection_cache.items()):
            try:
                document = await collection.find_one({"token_hash": token_hash})
            except PyMongoError as error:
                logger.exception("Failed to fetch API token metadata: %s", error)
                raise MongoConnectionError("Failed to query MongoDB for API tokens.") from error

            if document is not None:
                self._token_hash_cache[token_hash] = database_name
                return document, collection

        database_names = await self._client.list_database_names()
        system_databases = {"admin", "config", "local"}
        for database_name in database_names:
            if database_name in self._token_collection_cache or database_name in system_databases:
                continue

            database = self._database_cache.get(database_name)
            if database is None:
                database = self._client[database_name]
                self._database_cache[database_name] = database

            try:
                existing_collections = await database.list_collection_names()
            except PyMongoError as error:
                logger.exception(
                    "Failed to inspect database %s for API tokens: %s",
                    database_name,
                    error,
                )
                raise MongoConnectionError("Failed to query MongoDB for API tokens.") from error
            if settings.api_tokens_collection not in existing_collections:
                continue

            collection = await self._ensure_token_collection(database)

            try:
                document = await collection.find_one({"token_hash": token_hash})
            except PyMongoError as error:
                logger.exception("Failed to fetch API token metadata: %s", error)
                raise MongoConnectionError("Failed to query MongoDB for API tokens.") from error

            if document is not None:
                self._token_hash_cache[token_hash] = database_name
                return document, collection

        return None, None

    async def close(self) -> None:
        """Terminate the MongoDB connection."""

        if self._client is not None:
            logger.info("Closing MongoDB connection")
            self._client.close()
            self._client = None
            self._database_cache = {}
            self._collection_cache = {}
            self._token_collection_cache = {}
            self._token_hash_cache = {}


mongo_manager = MongoDBManager()
"""Singleton MongoDB manager used by the application."""
