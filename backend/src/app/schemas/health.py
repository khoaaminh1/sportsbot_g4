from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

CheckStatus = Literal["ok", "error", "skipped"]


class DependencyCheck(BaseModel):
    status: CheckStatus
    required: bool = Field(
        description="A required dependency that fails makes the service unready."
    )
    latency_ms: float | None = None
    detail: str | None = None


class HealthResponse(BaseModel):
    status: Literal["ok", "degraded", "error"] = Field(
        description="ok: all checks pass; degraded: an optional dependency failed; "
        "error: a required dependency failed (HTTP 503)."
    )
    version: str
    environment: str
    checks: dict[str, DependencyCheck]


class LivenessResponse(BaseModel):
    status: Literal["ok"] = "ok"
    version: str
