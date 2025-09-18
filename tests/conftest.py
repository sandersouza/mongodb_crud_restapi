"""Pytest configuration for the MongoDB CRUD REST API project."""

from __future__ import annotations

import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


os.environ.setdefault("MONGODB_URI", "mongodb://localhost:27017")
os.environ.setdefault("MONGODB_DATABASE", "testdb")
os.environ.setdefault("API_ADMIN_TOKEN", "test-admin-token")
os.environ.setdefault("ENABLE_TOKEN_CREATION_ROUTE", "true")
os.environ.setdefault("API_TOKENS_COLLECTION", "test_api_tokens")
