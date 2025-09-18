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
    expires_in_seconds: Optional[int] = Field(
        default=None,
        description=(
            "Optional TTL for the token expressed in seconds. "
            "If omitted or set to 0 the token never expires."
        ),
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

    @field_validator("expires_in_seconds")
    @classmethod
    def validate_ttl(cls, value: Optional[int]) -> Optional[int]:
        if value is None:
            return None
        if value < 0:
            raise ValueError("expires_in_seconds must be greater than or equal to 0.")
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
    expires_at: Optional[datetime] = Field(
        default=None,
        description="UTC timestamp indicating when the token will expire, if any.",
    )


class APITokenStoredResponse(BaseModel):
    """Representation of a stored token when listing existing API tokens."""

    id: str = Field(..., description="Unique identifier of the stored token.")
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
    expires_at: Optional[datetime] = Field(
        default=None,
        description="UTC timestamp indicating when the token will expire, if any.",
    )
