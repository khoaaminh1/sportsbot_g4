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
    """Live feed for the web page: new articles, crawl activity and corpus counters.

    Messages (JSON):
    - {"type": "articles", "items": [ArticleSummary]}  latest 20 on connect, then new ones
    - {"type": "runs", "items": [run]}                  finished collection runs (activity)
    - {"type": "stats", ...counters}                    on connect, then every few polls
    Polling indexed columns every `poll_seconds` is simple and survives database restarts.
    """
    app = websocket.app
    limiter = app.state.ws_limiter
    if not limiter.try_open():
        await websocket.close(code=1013)  # try again later: too many open feeds
        return
    try:
        await _feed(websocket)
    finally:
        limiter.close()


async def _feed(websocket: WebSocket) -> None:
    await websocket.accept()
    app = websocket.app
    repo = app.state.news_repo
    poll = app.state.news_poll_seconds
    last_seen: datetime | None = None
    last_run: int | None = None
    tick = 0
    try:
        while True:
            rows = await run_in_threadpool(repo.since, last_seen, 20)
            if rows:
                last_seen = max(r["ingested_at"] for r in rows)
                items = [to_summary(r) for r in reversed(rows)]  # oldest first
                await websocket.send_json({"type": "articles", "items": items})
            runs = await run_in_threadpool(repo.runs_since, last_run, 15)
            if runs:
                last_run = max(r["run_id"] for r in runs)
                await websocket.send_json({"type": "runs", "items": runs})
            if tick % 3 == 0:
                stats = await run_in_threadpool(repo.stats)
                await websocket.send_json({"type": "stats", **stats})
            tick += 1
            await asyncio.sleep(poll)
    except WebSocketDisconnect:
        return
    except Exception:  # database down etc.: tell the client, then close
        logger.exception("ws_news_failed")
        await websocket.send_json({"type": "error", "message": "news feed unavailable"})
        await websocket.close(code=1011)
