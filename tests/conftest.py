"""Pytest configuration for the MongoDB CRUD REST API project."""

from __future__ import annotations

import os
import sys
import types
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Iterator

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


os.environ.setdefault("MONGODB_URI", "mongodb://localhost:27017")
os.environ.setdefault("API_ADMIN_TOKEN", "test-admin-token")
os.environ.setdefault("SHOW_TOKEN_CREATION_ROUTE", "true")
os.environ.setdefault("API_TOKENS_COLLECTION", "test_api_tokens")


@dataclass
class FakePyMongo:
    """Container exposing stand-ins for the optional ``pymongo`` dependency."""

    module: types.ModuleType
    errors: types.ModuleType
    ASCENDING: int
    DESCENDING: int
    PyMongoError: type[Exception]
    OperationFailure: type[Exception]
    DuplicateKeyError: type[Exception]
    CollectionInvalid: type[Exception]
    ServerSelectionTimeoutError: type[Exception]
    ReturnDocument: SimpleNamespace


@pytest.fixture()
def fake_pymongo(monkeypatch: pytest.MonkeyPatch) -> Iterator[FakePyMongo]:
    """Provide lightweight ``pymongo`` stand-ins for environments without the dependency."""

    module = types.ModuleType("pymongo")
    errors = types.ModuleType("pymongo.errors")

    class _PyMongoError(Exception):
        """Base error used to emulate ``pymongo.errors.PyMongoError``."""

    class _OperationFailure(_PyMongoError):
        """Error raised when an operation fails."""

    class _DuplicateKeyError(_PyMongoError):
        """Error raised when attempting to insert a duplicate key."""

    class _CollectionInvalid(_PyMongoError):
        """Error raised when collection creation fails."""

    class _ServerSelectionTimeoutError(_PyMongoError):
        """Error raised when connecting to MongoDB times out."""

    errors.PyMongoError = _PyMongoError
    errors.OperationFailure = _OperationFailure
    errors.DuplicateKeyError = _DuplicateKeyError
    errors.CollectionInvalid = _CollectionInvalid
    errors.ServerSelectionTimeoutError = _ServerSelectionTimeoutError

    module.ASCENDING = 1
    module.DESCENDING = -1
    module.ReturnDocument = SimpleNamespace(AFTER="after")
    module.errors = errors

    monkeypatch.setitem(sys.modules, "pymongo", module)
    monkeypatch.setitem(sys.modules, "pymongo.errors", errors)

    yield FakePyMongo(
        module=module,
        errors=errors,
        ASCENDING=module.ASCENDING,
        DESCENDING=module.DESCENDING,
        PyMongoError=_PyMongoError,
        OperationFailure=_OperationFailure,
        DuplicateKeyError=_DuplicateKeyError,
        CollectionInvalid=_CollectionInvalid,
        ServerSelectionTimeoutError=_ServerSelectionTimeoutError,
        ReturnDocument=module.ReturnDocument,
    )
