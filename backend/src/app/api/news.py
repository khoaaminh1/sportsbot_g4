"""News endpoints (REST) and the live news WebSocket."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Path, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.concurrency import run_in_threadpool

from app.core.errors import AppError
from app.services.news import to_summary

logger = logging.getLogger(__name__)

router = APIRouter(tags=["news"])

SPORTS = ("football", "volleyball", "basketball", "tennis", "badminton", "other_sport")


@router.get("/news/latest", summary="Latest articles, newest first")
async def latest_news(
    request: Request,
    sport: str | None = Query(default=None, pattern=f"^({'|'.join(SPORTS)})$"),
    language: str | None = Query(default=None, pattern="^[a-z]{2}$"),
    limit: int = Query(default=20, ge=1, le=100),
    cursor: str | None = None,
) -> dict[str, Any]:
    repo = request.app.state.news_repo
    try:
        return await run_in_threadpool(
            repo.latest, limit=limit, sport=sport, language=language, cursor=cursor
        )
    except ValueError as exc:
        raise AppError("bad_cursor", "Invalid cursor", 422) from exc


@router.get("/articles/{article_id}", summary="Article metadata, excerpt and original link")
async def get_article(
    request: Request, article_id: str = Path(pattern="^[a-f0-9]{16,64}$")
) -> dict[str, Any]:
    article = await run_in_threadpool(request.app.state.news_repo.get, article_id)
    if article is None:
        raise AppError("not_found", "Unknown article", 404)
    return article


@router.get("/categories", summary="Sports categories with article counts")
async def categories(request: Request) -> list[dict[str, Any]]:
    return await run_in_threadpool(request.app.state.news_repo.categories)


@router.get("/sources", summary="Configured sources with their latest collection status")
async def sources(request: Request) -> list[dict[str, Any]]:
    return await run_in_threadpool(request.app.state.news_repo.sources)


@router.websocket("/ws/news")
async def news_feed(websocket: WebSocket) -> None:
    """Push new articles to the browser.

    On connect: the 20 most recently stored articles. Then every `poll_seconds`, articles stored
    since the last push. Polling the indexed `ingested_at` column is simple and survives
    database restarts; at this scale it costs one small query per client every few seconds.
    """
    await websocket.accept()
    app = websocket.app
    repo = app.state.news_repo
    poll = app.state.news_poll_seconds
    last_seen: datetime | None = None
    try:
        while True:
            rows = await run_in_threadpool(repo.since, last_seen, 20)
            if rows:
                last_seen = max(r["ingested_at"] for r in rows)
                items = [to_summary(r) for r in reversed(rows)]  # oldest first
                await websocket.send_json({"type": "articles", "items": items})
            await asyncio.sleep(poll)
    except WebSocketDisconnect:
        return
    except Exception:  # database down etc.: tell the client, then close
        logger.exception("ws_news_failed")
        await websocket.send_json({"type": "error", "message": "news feed unavailable"})
        await websocket.close(code=1011)
