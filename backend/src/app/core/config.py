"""Application settings, read from environment variables (see `.env.example`).

Every setting has a safe default for local development except secrets, which default to empty.
`get_settings()` is cached; tests build `Settings(...)` directly and pass it to `create_app`.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Annotated, Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Settings(BaseSettings):
    # env_ignore_empty: an empty value in .env (e.g. `SOURCES_FILE=`) means "use the default".
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore", env_ignore_empty=True
    )

    app_env: Literal["development", "test", "production"] = "development"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    log_format: Literal["json", "text"] = "json"
    # NoDecode: the env value is a comma-separated string, split by the validator below.
    cors_origins: Annotated[list[str], NoDecode] = Field(
        default_factory=lambda: ["http://localhost:5173"]
    )

    database_url: str = (
        "postgresql+psycopg://sportsbot:change-me-local-only@localhost:5432/sportsbot"
    )
    qdrant_url: str = "http://localhost:6333"
    # Empty string disables the HDFS check (the bigdata profile may not be running).
    hdfs_webhdfs_url: str = ""
    health_timeout_seconds: float = Field(default=2.0, gt=0, le=30)

    # Chat (agent/rag.py reads its own settings: GEMINI_*, EMBEDDING_*, RERANK_*, LLM_*).
    chat_enabled: bool = True
    # How often the /ws/news WebSocket looks for newly stored articles.
    news_poll_seconds: float = Field(default=5.0, gt=0, le=300)

    @field_validator("cors_origins", mode="before")
    @classmethod
    def _split_origins(cls, value: object) -> object:
        # Accept "a,b" from the environment as well as a JSON list.
        if isinstance(value, str) and not value.strip().startswith("["):
            return [origin.strip() for origin in value.split(",") if origin.strip()]
        return value

    @field_validator("hdfs_webhdfs_url", "qdrant_url")
    @classmethod
    def _strip_trailing_slash(cls, value: str) -> str:
        return value.rstrip("/")


@lru_cache
def get_settings() -> Settings:
    return Settings()
