"""Pydantic-based application settings loaded from environment variables."""

from __future__ import annotations

import logging
import os
from typing import Any, get_origin

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, PositiveInt, SecretStr, TypeAdapter
from pydantic_settings import BaseSettings, SettingsConfigDict


class LoggingSettings(BaseModel):
    """Logging configuration."""

    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(..., alias="LOGGING_ID")
    level: int = Field(default=logging.INFO, alias="LOGGING_LEVEL")


class TrinoSettings(BaseModel):
    """Connection and metadata extraction settings for Trino."""

    model_config = ConfigDict(populate_by_name=True)

    host: str = Field(..., alias="TRINO_HOST")
    port: PositiveInt = Field(443, alias="TRINO_PORT")
    user: str = Field(..., alias="TRINO_USER")
    password: SecretStr = Field(..., alias="TRINO_PASSWORD")
    catalogs: list[str] = Field(default_factory=list, alias="TRINO_CATALOGS")

class RangerSettings(BaseModel):
    """Connection and metadata extraction settings for Trino."""

    model_config = ConfigDict(populate_by_name=True)

    api_endpoint: str = Field(..., alias="RANGER_API_ENDPOINT")
    username: str = Field(..., alias="RANGER_USER")
    password: SecretStr = Field(..., alias="RANGER_PASSWORD")

class OutputSettings(BaseModel):
    """Local output configuration for generated files."""

    model_config = ConfigDict(populate_by_name=True)

    directory: str = Field(..., alias="OUTPUT_DIR")
    file_prefix: str = Field(..., alias="OUTPUT_FILE_PREFIX")
    file_size: PositiveInt = Field(default=1_048_576, alias="OUTPUT_FILE_SIZE")


class S3Settings(BaseModel):
    """Settings for S3-compatible storage where exports are written."""

    model_config = ConfigDict(populate_by_name=True)

    starburst_export_bucket: str = Field(..., alias="STARBURST_EXPORT_BUCKET")
    export_default_path: str = Field(..., alias="EXPORT_DEFAULT_PATH")
    starburst_columns_file: str = Field(..., alias="STARBURST_COLUMNS_FILE")
    ranger_policies_file: str = Field(..., alias="RANGER_POLICIES_FILE")
    endpoint: HttpUrl = Field(..., alias="S3_ENDPOINT")
    access_key: str = Field(..., alias="S3_ACCESS_KEY")
    secret_key: SecretStr = Field(..., alias="S3_SECRET_KEY")
    ssl_verify: str = Field(False, alias="STATIC_CLOUDIAN_SSL_VERIFY")


class AppSettings(BaseSettings):
    """Top-level application settings grouped by concern."""

    model_config = SettingsConfigDict(extra="ignore")

    trino: TrinoSettings
    ranger: RangerSettings
    logging: LoggingSettings
    output: OutputSettings
    s3: S3Settings

    @staticmethod
    def _section_from_env(model_cls: type[BaseModel]) -> dict[str, Any]:
        """
        Build a settings section for the given model from environment variables.

        Uses field aliases as environment variable names and performs basic
        conversion/validation using pydantic's TypeAdapter.
        """
        data: dict[str, Any] = {}

        for name, field in model_cls.model_fields.items():
            alias = field.alias or name
            if alias not in os.environ:
                continue

            raw = os.environ[alias]
            ann = field.annotation or str

            if model_cls.__name__ == "LoggingSettings" and name == "level":
                level = logging.getLevelNamesMapping().get(str(raw).upper())
                if level is None:
                    raise ValueError(
                        f"Invalid LOGGING_LEVEL={raw!r}. "
                        "Use one of: INFO, DEBUG, WARNING, ERROR"
                    )
                data[name] = level
            elif get_origin(ann) in (list, tuple) and str(raw).lstrip().startswith("["):
                # Parse JSON list/tuple from env
                data[name] = TypeAdapter(ann).validate_json(raw)
            else:
                data[name] = TypeAdapter(ann).validate_python(raw)

        return data

    @classmethod
    def from_env(cls) -> "AppSettings":
        """
        Construct AppSettings from the current process environment variables.

        Each section is built independently using `_section_from_env`.
        """
        trino_settings = TrinoSettings(**cls._section_from_env(TrinoSettings))
        ranger_settings = RangerSettings(**cls._section_from_env(RangerSettings))
        logging_settings = LoggingSettings(**cls._section_from_env(LoggingSettings))
        output_settings = OutputSettings(**cls._section_from_env(OutputSettings))
        s3_settings = S3Settings(**cls._section_from_env(S3Settings))

        return cls(
            trino=trino_settings,
            ranger=ranger_settings,
            logging=logging_settings,
            output=output_settings,
            s3=s3_settings,
        )
