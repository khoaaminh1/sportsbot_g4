"""Source registry: load and validate `pipelines/sources.yaml`."""

from __future__ import annotations

from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from pydantic import BaseModel, Field, HttpUrl, field_validator, model_validator


class Source(BaseModel):
    id: str = Field(pattern=r"^[a-z0-9_]+$", max_length=64)
    name: str = Field(min_length=1)
    kind: Literal["rss", "api", "page"]
    url: HttpUrl
    language: str = Field(pattern=r"^[a-z]{2}$")
    interval_minutes: int = Field(ge=5, le=24 * 60)
    domains: list[str] = Field(min_length=1)
    # IANA timezone the source publishes in; applied to dates that carry no offset.
    timezone: str = "UTC"
    enabled: bool = True
    verified: bool = False
    notes: str = ""

    @field_validator("domains")
    @classmethod
    def _lower_domains(cls, value: list[str]) -> list[str]:
        return [d.strip().lower().lstrip(".") for d in value if d.strip()]

    @field_validator("timezone")
    @classmethod
    def _valid_tz(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ValueError(f"unknown timezone: {value}") from exc
        return value

    @model_validator(mode="after")
    def _only_rss_for_now(self) -> Source:
        # API and page collectors arrive later in Phase 3; refuse silently-ignored entries.
        if self.kind != "rss" and self.enabled:
            raise ValueError(f"source {self.id}: kind '{self.kind}' has no collector yet")
        return self


class Registry(BaseModel):
    sources: list[Source]

    @model_validator(mode="after")
    def _unique_ids(self) -> Registry:
        ids = [s.id for s in self.sources]
        duplicates = sorted({i for i in ids if ids.count(i) > 1})
        if duplicates:
            raise ValueError(f"duplicate source ids: {duplicates}")
        return self

    def enabled(self) -> list[Source]:
        return [s for s in self.sources if s.enabled]

    def get(self, source_id: str) -> Source:
        for source in self.sources:
            if source.id == source_id:
                return source
        raise KeyError(f"unknown source id: {source_id}")


def load_registry(path: Path) -> Registry:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return Registry.model_validate(data)
