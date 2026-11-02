"""Extractor worker: articles.discovered -> fetch, extract, de-duplicate -> articles.extracted.

Outcomes per message:
- extracted       -> published to articles.extracted
- duplicate       -> dropped (same canonical URL already stored, or same text under another URL)
- dead letter     -> robots.txt disallows, HTTP error, no main text, text too short
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol

from sqlalchemy import Engine, text

from sportsbot_pipelines.collectors.dates import parse_date
from sportsbot_pipelines.collectors.http import FetchError, PoliteClient, RobotsDisallowed
from sportsbot_pipelines.extraction.extract import (
    ExtractedPage,
    content_hash,
    extract_page,
    word_count,
)
from sportsbot_pipelines.streaming import topics
from sportsbot_pipelines.streaming.bus import Bus, dead_letter

logger = logging.getLogger(__name__)

EXTRACTOR_NAME = "trafilatura"


class DedupStore(Protocol):
    def article_exists(self, article_id: str) -> bool: ...

    def claim_content(self, content_hash: str, article_id: str) -> str | None:
        """Remember the hash; return the id of an *other* article that already has it."""


@dataclass
class MemoryDedup:
    articles: set[str] = field(default_factory=set)
    hashes: dict[str, str] = field(default_factory=dict)

    def article_exists(self, article_id: str) -> bool:
        return article_id in self.articles

    def claim_content(self, content_hash: str, article_id: str) -> str | None:
        owner = self.hashes.setdefault(content_hash, article_id)
        if owner == article_id:
            self.articles.add(article_id)
            return None
        return owner


class PostgresDedup:
    """Checks the `articles` table (written by the operational consumer)."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self._recent: dict[str, str] = {}  # hashes seen by this process, not yet in PostgreSQL

    def article_exists(self, article_id: str) -> bool:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT 1 FROM articles WHERE article_id = :id AND content IS NOT NULL"),
                {"id": article_id},
            ).first()
        return row is not None

    def claim_content(self, content_hash: str, article_id: str) -> str | None:
        owner = self._recent.get(content_hash)
        if owner is None:
            with self.engine.connect() as conn:
                owner = conn.execute(
                    text("SELECT article_id FROM articles WHERE content_hash = :h LIMIT 1"),
                    {"h": content_hash},
                ).scalar()
        if owner is None or owner == article_id:
            self._recent[content_hash] = article_id
            if len(self._recent) > 50_000:
                self._recent.clear()
            return None
        return owner


@dataclass
class ExtractorStats:
    extracted: int = 0
    duplicates: int = 0
    dead_lettered: int = 0


def build_extracted(
    message: dict[str, Any],
    page: ExtractedPage,
    *,
    timezone: str = "UTC",
    now: datetime | None = None,
) -> dict[str, Any]:
    """The `articles.extracted` message: discovery fields + extracted text and metadata."""
    published_at = message.get("published_at")
    if page.published:
        parsed = parse_date(page.published, timezone)
        # Prefer the page's own date when the feed gave none (sitemap entries, some feeds).
        if parsed is not None and not published_at:
            published_at = parsed.iso()
    body = page.text
    return {
        **message,
        "schema": topics.EXTRACTED_SCHEMA,
        "title": (message.get("title") or page.title or "").strip(),
        "text": body,
        "author": page.author,
        "published_at": published_at,
        "language": page.language or message.get("language_hint"),
        "word_count": word_count(body),
        "content_hash": content_hash(body),
        "extractor": EXTRACTOR_NAME,
        "extracted_at": (now or datetime.now(UTC)).isoformat(),
    }


def make_handler(
    client: PoliteClient,
    bus: Bus,
    dedup: DedupStore,
    *,
    min_chars: int = 300,
    timezones: dict[str, str] | None = None,
    extract: Callable[[bytes, str], ExtractedPage | None] = extract_page,
    stats: ExtractorStats | None = None,
) -> Callable[[dict[str, Any]], None]:
    timezones = timezones or {}
    stats = stats if stats is not None else ExtractorStats()

    def reject(message: dict[str, Any], reason: str, error: str | None = None) -> None:
        stats.dead_lettered += 1
        bus.publish(
            topics.DEAD_LETTER,
            message.get("source_id", "unknown"),
            dead_letter("extract", reason, message, error),
        )
        logger.info(
            "extract_rejected",
            extra={"reason": reason, "url": message.get("canonical_url"), "error": error},
        )

    def handle(message: dict[str, Any]) -> None:
        article_id = message["article_id"]
        url = message.get("canonical_url") or message["url"]
        if dedup.article_exists(article_id):
            stats.duplicates += 1
            return
        try:
            fetched = client.fetch(url)
        except RobotsDisallowed as exc:
            return reject(message, "robots_disallowed", str(exc))
        except FetchError as exc:
            return reject(message, f"http_{exc.status or 'error'}", str(exc))
        page = extract(fetched.content, url)
        if page is None:
            return reject(message, "no_main_text")
        if len(page.text) < min_chars:
            return reject(message, "too_short", f"{len(page.text)} chars")
        out = build_extracted(message, page, timezone=timezones.get(message["source_id"], "UTC"))
        if not out["title"]:
            return reject(message, "no_title")
        owner = dedup.claim_content(out["content_hash"], article_id)
        if owner is not None:
            stats.duplicates += 1
            logger.info("duplicate_content", extra={"article_id": article_id, "same_as": owner})
            return None
        bus.publish(topics.EXTRACTED, message["source_id"], out)
        stats.extracted += 1
        return None

    return handle
