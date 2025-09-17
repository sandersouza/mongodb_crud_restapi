"""Application settings and configuration helpers."""

from __future__ import annotations

import logging
from functools import lru_cache
from pathlib import Path
from typing import List, Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    """Application configuration loaded from environment variables."""

    app_name: str = Field(default="MongoDB CRUD API", alias="APP_NAME")
    api_prefix: str = Field(default="/api", alias="API_PREFIX")
    environment: str = Field(default="development", alias="ENVIRONMENT")
    log_level: str = Field(default="info", alias="LOG_LEVEL")

    mongodb_uri: str = Field(default=..., alias="MONGODB_URI")
    mongodb_username: Optional[str] = Field(default=None, alias="MONGODB_USERNAME")
    mongodb_password: Optional[str] = Field(default=None, alias="MONGODB_PASSWORD")
    mongodb_database: str = Field(default=..., alias="MONGODB_DATABASE")
    mongodb_collection: str = Field(default="measurements", alias="MONGODB_COLLECTION")
    mongodb_max_pool_size: int = Field(default=10, alias="MONGODB_MAX_POOL_SIZE")

    timeseries_time_field: str = Field(default="timestamp", alias="TIMESERIES_TIME_FIELD")
    timeseries_meta_field: Optional[str] = Field(default="metadata", alias="TIMESERIES_META_FIELD")

    allowed_origins: List[str] = Field(default_factory=list, alias="ALLOWED_ORIGINS")

    model_config = SettingsConfigDict(
        env_file=str(Path(__file__).resolve().parents[2] / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @field_validator("allowed_origins", mode="before")
    @classmethod
    def split_origins(cls, value: Optional[str]) -> List[str]:
        """Split a comma separated list of origins into a list."""

        if value is None or value == "":
            return []
        if isinstance(value, str):
            return [origin.strip() for origin in value.split(",") if origin.strip()]
        return value


@lru_cache
def get_settings() -> Settings:
    """Return cached application settings."""

    settings = Settings()
    logging.getLogger().setLevel(settings.log_level.upper())
    logger.debug("Settings loaded for environment %s", settings.environment)
    return settings
