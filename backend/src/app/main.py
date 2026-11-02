"""FastAPI application factory.

`create_app()` wires settings, logging, middleware, error handlers and routers. Tests pass their
own `Settings` and health checks; production uses `app = create_app()` (uvicorn `app.main:app`).
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app import __version__
from app.api import health
from app.core.config import Settings, get_settings
from app.core.errors import register_error_handlers
from app.core.logging import configure_logging
from app.core.middleware import RequestContextMiddleware
from app.db.session import make_engine, make_session_factory
from app.services.health import Check, HealthService, default_checks

logger = logging.getLogger(__name__)


def create_app(settings: Settings | None = None, checks: list[Check] | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level, settings.log_format)

    # Creating the engine does not connect; the first query (or health check) does.
    engine = make_engine(settings.database_url, settings.health_timeout_seconds)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        logger.info("startup", extra={"version": __version__, "environment": settings.app_env})
        yield
        engine.dispose()
        logger.info("shutdown")

    app = FastAPI(
        title="Vietnam SportsBot API",
        version=__version__,
        description="Multilingual sports news chatbot. Contract: docs/contracts/api.openapi.yaml",
        lifespan=lifespan,
        responses=health.ERROR_RESPONSES,
    )
    app.state.settings = settings
    app.state.engine = engine
    app.state.session_factory = make_session_factory(engine)
    app.state.health_service = HealthService(
        settings, checks if checks is not None else default_checks(settings, engine)
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_methods=["GET", "POST"],
        allow_headers=["*"],
        expose_headers=["X-Request-ID"],
    )
    app.add_middleware(RequestContextMiddleware)
    register_error_handlers(app)
    app.include_router(health.router)
    return app


app = create_app()
