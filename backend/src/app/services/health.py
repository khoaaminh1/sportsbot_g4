"""Dependency health checks for `GET /health`.

Checks run concurrently, each bounded by `HEALTH_TIMEOUT_SECONDS`, so one hanging dependency
cannot block the endpoint. PostgreSQL and Qdrant are required; HDFS is optional (the bigdata
profile may be stopped while someone works on the backend) and is skipped when
`HDFS_WEBHDFS_URL` is empty.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import httpx
from sqlalchemy import Engine

from app import __version__
from app.core.config import Settings
from app.db.session import ping
from app.schemas.health import DependencyCheck, HealthResponse

logger = logging.getLogger(__name__)

CheckFn = Callable[[], Awaitable[str | None]]


@dataclass(frozen=True)
class Check:
    name: str
    fn: CheckFn | None  # None = skipped (not configured)
    required: bool


async def _timed(check: Check, timeout: float) -> DependencyCheck:
    if check.fn is None:
        return DependencyCheck(status="skipped", required=check.required, detail="not configured")
    start = time.perf_counter()
    try:
        detail = await asyncio.wait_for(check.fn(), timeout=timeout)
        status, message = "ok", detail
    except TimeoutError:
        status, message = "error", f"timed out after {timeout:g}s"
    except Exception as exc:  # report any failure as a failed check, never as a 500
        status, message = "error", f"{type(exc).__name__}: {exc}"[:300]
    latency = round((time.perf_counter() - start) * 1000, 1)
    if status == "error":
        logger.warning("health_check_failed", extra={"dependency": check.name, "detail": message})
    return DependencyCheck(
        status=status, required=check.required, latency_ms=latency, detail=message
    )


class HealthService:
    def __init__(self, settings: Settings, checks: list[Check]) -> None:
        self._settings = settings
        self._checks = checks

    async def run(self) -> HealthResponse:
        timeout = self._settings.health_timeout_seconds
        results = await asyncio.gather(*(_timed(c, timeout) for c in self._checks))
        checks = {c.name: r for c, r in zip(self._checks, results, strict=True)}
        if any(r.status == "error" and r.required for r in checks.values()):
            overall = "error"
        elif any(r.status == "error" for r in checks.values()):
            overall = "degraded"
        else:
            overall = "ok"
        return HealthResponse(
            status=overall, version=__version__, environment=self._settings.app_env, checks=checks
        )


def default_checks(settings: Settings, engine: Engine) -> list[Check]:
    timeout = settings.health_timeout_seconds

    async def postgres() -> str | None:
        version = await asyncio.to_thread(ping, engine)
        return f"schema {version}" if version else "connected; schema_migrations missing"

    async def qdrant() -> str | None:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(f"{settings.qdrant_url}/readyz")
            resp.raise_for_status()
        return "ready"

    async def hdfs() -> str | None:
        url = f"{settings.hdfs_webhdfs_url}/webhdfs/v1/?op=GETFILESTATUS"
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            resp.json()  # WebHDFS answers JSON; anything else is not a NameNode
        return "namenode reachable"

    return [
        Check("postgres", postgres, required=True),
        Check("qdrant", qdrant, required=True),
        Check("hdfs", hdfs if settings.hdfs_webhdfs_url else None, required=False),
    ]
