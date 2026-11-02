"""FastAPI application factory.

`create_app()` wires settings, logging, middleware, error handlers and routers. Tests pass their
own `Settings` and health checks; production uses `app = create_app()` (uvicorn `app.main:app`).
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app import __version__
from app.api import chat, health, news
from app.core.config import Settings, get_settings
from app.core.errors import register_error_handlers
from app.core.logging import configure_logging
from app.core.middleware import RequestContextMiddleware
from app.db.session import make_engine, make_session_factory
from app.services.conversations import ConversationStore
from app.services.health import Check, HealthService, default_checks
from app.services.news import NewsRepository, PostgresNewsRepository

logger = logging.getLogger(__name__)


FRONTEND_DIR = Path(__file__).resolve().parents[3] / "frontend"
_DEFAULT: Any = object()


def _build_rag(settings: Settings, engine: Any) -> Any:
    if not settings.chat_enabled:
        return None
    from sportsbot_agent.config import AgentConfig
    from sportsbot_agent.rag import build_rag

    # Creating the clients does not connect; the first question does.
    return build_rag(AgentConfig.from_env(), engine)


def create_app(
    settings: Settings | None = None,
    checks: list[Check] | None = None,
    *,
    news_repo: NewsRepository | None = None,
    rag: Any = _DEFAULT,
    conversation_store: Any = _DEFAULT,
) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level, settings.log_format)

    # Creating the engine does not connect; the first query (or health check) does.
    engine = make_engine(settings.database_url, settings.health_timeout_seconds)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        logger.info("startup", extra={"version": __version__, "environment": settings.app_env})
        yield
        if app.state.rag is not None and hasattr(app.state.rag, "close"):
            app.state.rag.close()
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

    app.state.news_repo = news_repo or PostgresNewsRepository(engine)
    app.state.news_poll_seconds = settings.news_poll_seconds
    app.state.rag = _build_rag(settings, engine) if rag is _DEFAULT else rag
    app.state.conversation_store = (
        ConversationStore(engine) if conversation_store is _DEFAULT else conversation_store
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
    app.include_router(news.router)
    app.include_router(chat.router)
    if FRONTEND_DIR.is_dir():
        # The demo web page (news feed + chat): http://localhost:8000/app/
        app.mount("/app", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")
    return app


app = create_app()
