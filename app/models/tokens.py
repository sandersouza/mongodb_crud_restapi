"""Pydantic models for API token management."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class APITokenCreate(BaseModel):
    """Payload for creating a new API token."""

    database: str = Field(..., description="Database name associated with the token.")
    token: Optional[str] = Field(
        default=None,
        description="Optional custom token value. If omitted the API generates one automatically.",
    )
    description: Optional[str] = Field(
        default=None,
        description="Human friendly description to help identify the token.",
    )

    @field_validator("database")
    @classmethod
    def validate_database(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("database must not be empty.")
        return value

    @field_validator("token")
    @classmethod
    def validate_token(cls, value: Optional[str]) -> Optional[str]:
        if value is not None and value == "":
            raise ValueError("token must not be empty when provided.")
        return value


class APITokenResponse(BaseModel):
    """Response returned after creating a new API token."""

    token: str = Field(
        ..., description="The newly created token. Store it securely; it cannot be recovered later."
    )
    database: str = Field(..., description="Database associated with the token.")
    description: Optional[str] = Field(
        default=None, description="Human friendly description associated with the token."
    )
    created_at: datetime = Field(
        ..., description="UTC timestamp indicating when the token was created."
    )
    last_used_at: Optional[datetime] = Field(
        default=None, description="UTC timestamp of the last time the token was used."
    )
