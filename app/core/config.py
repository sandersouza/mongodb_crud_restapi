"""Application settings and configuration helpers."""

from __future__ import annotations

import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import List, Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic_settings.sources import DotEnvSettingsSource, EnvSettingsSource

logger = logging.getLogger(__name__)


def _lenient_json_loads(value: str):
    """Allow simple comma separated strings to pass through JSON parsing."""

    if value == "":
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


class _LenientEnvSettingsSource(EnvSettingsSource):
    """Environment source that tolerates non-JSON list values."""

    def decode_complex_value(self, field_name, field, value):  # noqa: D401 (inherit docs)
        if isinstance(value, str):
            return _lenient_json_loads(value)
        return super().decode_complex_value(field_name, field, value)


class _LenientDotEnvSettingsSource(DotEnvSettingsSource):
    """DotEnv source that tolerates non-JSON list values."""

    def decode_complex_value(self, field_name, field, value):  # noqa: D401 (inherit docs)
        if isinstance(value, str):
            return _lenient_json_loads(value)
        return super().decode_complex_value(field_name, field, value)


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

    api_admin_token: str = Field(default=..., alias="API_ADMIN_TOKEN")
    enable_token_creation_route: bool = Field(
        default=False,
        alias="ENABLE_TOKEN_CREATION_ROUTE",
    )
    api_tokens_collection: str = Field(default="api_tokens", alias="API_TOKENS_COLLECTION")

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

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls,
        init_settings,
        env_settings,
        dotenv_settings,
        file_secret_settings,
    ):
        """Inject lenient env sources before validation executes."""

        init_settings, env_settings, dotenv_settings, file_secret_settings = super().settings_customise_sources(
            settings_cls,
            init_settings,
            env_settings,
            dotenv_settings,
            file_secret_settings,
        )

        lenient_env = (
            _LenientEnvSettingsSource(
                settings_cls,
                case_sensitive=env_settings.case_sensitive,
                env_prefix=env_settings.env_prefix,
                env_nested_delimiter=env_settings.env_nested_delimiter,
                env_ignore_empty=env_settings.env_ignore_empty,
                env_parse_none_str=env_settings.env_parse_none_str,
                env_parse_enums=env_settings.env_parse_enums,
            )
            if env_settings is not None
            else None
        )

        lenient_dotenv = (
            _LenientDotEnvSettingsSource(
                settings_cls,
                env_file=dotenv_settings.env_file,
                env_file_encoding=dotenv_settings.env_file_encoding,
                case_sensitive=dotenv_settings.case_sensitive,
                env_prefix=dotenv_settings.env_prefix,
                env_nested_delimiter=dotenv_settings.env_nested_delimiter,
                env_ignore_empty=dotenv_settings.env_ignore_empty,
                env_parse_none_str=dotenv_settings.env_parse_none_str,
                env_parse_enums=dotenv_settings.env_parse_enums,
            )
            if dotenv_settings is not None
            else None
        )

        return init_settings, lenient_env, lenient_dotenv, file_secret_settings


@lru_cache
def get_settings() -> Settings:
    """Return cached application settings."""

    settings = Settings()
    logging.getLogger().setLevel(settings.log_level.upper())
    logger.debug("Settings loaded for environment %s", settings.environment)
    return settings
