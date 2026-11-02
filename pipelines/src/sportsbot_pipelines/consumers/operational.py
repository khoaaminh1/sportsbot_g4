"""Operational consumer: `articles.extracted` -> PostgreSQL `articles` (news feed, keyword search).

Idempotent upsert keyed by article_id. If another article already holds the same content hash
(a re-post under a new URL that slipped past the extractor), the message is skipped.
"""

from __future__ import annotations

import html
import logging
import re
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Engine, text
from sqlalchemy.exc import OperationalError

from sportsbot_pipelines.streaming.bus import TransientError

logger = logging.getLogger(__name__)

_TAG = re.compile(r"<[^>]+>")
_SPACE = re.compile(r"\s+")

UPSERT = text(
    """
    INSERT INTO articles (
        article_id, title, content, summary, source_id, source_name, source_url,
        published_at, collected_at, language, content_hash, author, word_count,
        discovered_via, extracted_at
    ) VALUES (
        :article_id, :title, :content, :summary, :source_id, :source_name, :source_url,
        :published_at, :collected_at, :language, :content_hash, :author, :word_count,
        :discovered_via, :extracted_at
    )
    ON CONFLICT (article_id) DO UPDATE SET
        title = EXCLUDED.title,
        content = EXCLUDED.content,
        summary = COALESCE(EXCLUDED.summary, articles.summary),
        published_at = COALESCE(articles.published_at, EXCLUDED.published_at),
        language = EXCLUDED.language,
        content_hash = EXCLUDED.content_hash,
        author = EXCLUDED.author,
        word_count = EXCLUDED.word_count,
        extracted_at = EXCLUDED.extracted_at
    """
)

SAME_CONTENT = text(
    "SELECT article_id FROM articles WHERE content_hash = :h AND article_id <> :id LIMIT 1"
)


def html_to_text(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = _SPACE.sub(" ", html.unescape(_TAG.sub(" ", value))).strip()
    return cleaned or None


def row_from_message(m: dict[str, Any]) -> dict[str, Any]:
    return {
        "article_id": m["article_id"],
        "title": html.unescape(m["title"]),
        "content": m["text"],
        "summary": html_to_text(m.get("summary_html")) or m["text"][:400],
        "source_id": m["source_id"],
        "source_name": m["source_name"],
        "source_url": m.get("canonical_url") or m["url"],
        "published_at": m.get("published_at"),
        "collected_at": m.get("discovered_at") or datetime.now(UTC).isoformat(),
        "language": (m.get("language") or "")[:8] or None,
        "content_hash": m["content_hash"],
        "author": m.get("author"),
        "word_count": m.get("word_count"),
        "discovered_via": m.get("discovered_via"),
        "extracted_at": m.get("extracted_at"),
    }


def make_handler(engine: Engine) -> Callable[[dict[str, Any]], None]:
    def store(row: dict[str, Any]) -> None:
        with engine.begin() as conn:
            other = conn.execute(
                SAME_CONTENT, {"h": row["content_hash"], "id": row["article_id"]}
            ).scalar()
            if other:
                logger.info(
                    "operational_duplicate",
                    extra={"article_id": row["article_id"], "same_as": other},
                )
                return
            conn.execute(UPSERT, row)
        logger.debug("article_stored", extra={"article_id": row["article_id"]})

    def handle(message: dict[str, Any]) -> None:
        try:
            store(row_from_message(message))
        except OperationalError as exc:  # database down or restarting: retry, do not drop
            raise TransientError(f"PostgreSQL unavailable: {exc}") from exc

    return handle
