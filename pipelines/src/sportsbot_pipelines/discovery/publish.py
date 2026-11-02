"""Turn discovered records into `articles.discovered` messages."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from typing import Any

from sportsbot_pipelines.collectors.rss import BronzeRecord
from sportsbot_pipelines.streaming import topics
from sportsbot_pipelines.streaming.bus import Bus

Publisher = Callable[[list[BronzeRecord]], None]


def discovered_message(record: BronzeRecord, discovered_via: str = "rss") -> dict[str, Any]:
    return {
        "schema": topics.DISCOVERED_SCHEMA,
        "article_id": record.record_id,
        "source_id": record.source_id,
        "source_name": record.source_name,
        "language_hint": record.language_hint,
        "url": record.url,
        "canonical_url": record.canonical_url,
        "title": record.title,
        "summary_html": record.summary_html,
        "published_at": record.published_at,
        "published_raw": record.published_raw,
        "tags": record.tags,
        "discovered_via": discovered_via,
        "discovered_at": record.collected_at or datetime.now(UTC).isoformat(),
        "run_id": record.run_id,
    }


def bus_publisher(bus: Bus, discovered_via: str = "rss") -> Publisher:
    def publish(records: Iterable[BronzeRecord]) -> None:
        for record in records:
            bus.publish(
                topics.DISCOVERED, record.source_id, discovered_message(record, discovered_via)
            )
        bus.flush()

    return publish
