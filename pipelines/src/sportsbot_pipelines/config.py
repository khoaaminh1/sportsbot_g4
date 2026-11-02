"""Pipeline settings from environment variables (see `.env.example`)."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parents[3]


class PipelineSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    log_format: Literal["json", "text"] = "json"

    database_url: str = (
        "postgresql+psycopg://sportsbot:change-me-local-only@localhost:5432/sportsbot"
    )
    sources_file: Path = REPO_ROOT / "pipelines" / "sources.yaml"
    migrations_dir: Path = REPO_ROOT / "infra" / "postgres" / "migrations"

    # Where Bronze files go: "hdfs" (WebHDFS) in the stack, "local" for development and tests.
    bronze_backend: Literal["hdfs", "local"] = "local"
    bronze_root: str = "/sportsbot/bronze"
    local_bronze_dir: Path = REPO_ROOT / "data" / "bronze"
    hdfs_webhdfs_url: str = ""
    hdfs_user: str = "sportsbot"

    # Polite HTTP: identify ourselves, keep requests slow and small.
    collector_user_agent: str = "SportsBotG4/0.3 (university Big Data project; RSS reader)"
    collector_contact: str = ""
    http_timeout_seconds: float = Field(default=20.0, gt=0, le=120)
    http_max_bytes: int = Field(default=5_000_000, gt=0)
    http_retries: int = Field(default=2, ge=0, le=5)
    min_seconds_between_requests_per_host: float = Field(default=3.0, ge=0)
    max_items_per_fetch: int = Field(default=200, gt=0)
    scheduler_tick_seconds: float = Field(default=30.0, gt=0)

    @field_validator("hdfs_webhdfs_url")
    @classmethod
    def _strip_slash(cls, value: str) -> str:
        return value.rstrip("/")

    @property
    def user_agent(self) -> str:
        if self.collector_contact:
            return f"{self.collector_user_agent} contact: {self.collector_contact}"
        return self.collector_user_agent


@lru_cache
def get_settings() -> PipelineSettings:
    return PipelineSettings()
