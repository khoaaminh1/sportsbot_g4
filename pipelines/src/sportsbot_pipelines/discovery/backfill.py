"""Backfill one source: queue older article URLs (sitemaps, listing pages) for the extractor."""

from __future__ import annotations

import logging
import secrets
from collections.abc import Callable, Iterator
from datetime import UTC, date, datetime

from sportsbot_pipelines.collectors.http import PoliteClient
from sportsbot_pipelines.collectors.registry import Source
from sportsbot_pipelines.discovery.listing import iter_listing_entries
from sportsbot_pipelines.discovery.sitemap import (
    SitemapEntry,
    entry_to_record,
    iter_article_entries,
)
from sportsbot_pipelines.ingestion.run import Publisher
from sportsbot_pipelines.ingestion.state import State

logger = logging.getLogger(__name__)


def backfill_source(
    source: Source,
    client: PoliteClient,
    state: State,
    publish: Publisher,
    *,
    since: date,
    max_urls: int = 5000,
    batch_size: int = 200,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> dict[str, int | str]:
    if not source.sitemaps and source.listing is None:
        return {"source": source.id, "status": "no_sitemaps", "queued": 0}
    run_id = f"backfill-{now():%Y%m%dT%H%M%SZ}-{secrets.token_hex(3)}"

    def fetch(url: str) -> bytes:
        return client.fetch(url).content

    seen = queued = 0
    batch = []

    def flush() -> None:
        nonlocal queued
        if not batch:
            return
        fresh = state.unseen(source.id, [r.record_id for r in batch])
        new = [r for r in batch if r.record_id in fresh]
        if new:
            publish(new)
            state.mark_seen(source.id, [r.record_id for r in new], run_id)
            queued += len(new)
        batch.clear()

    def entries() -> Iterator[tuple[str, SitemapEntry]]:
        if source.sitemaps:
            for entry in iter_article_entries(
                [str(u) for u in source.sitemaps],
                fetch,
                since=since,
                include=source.sitemap_include,
                timezone=source.timezone,
            ):
                yield "sitemap", entry
        if source.listing is not None:
            # Listing pages carry no dates: `since` cannot filter them; max_pages bounds them.
            for entry in iter_listing_entries(source.listing, fetch, source.domains):
                yield "listing", entry

    for kind, entry in entries():
        record = entry_to_record(entry, source, collected_at=now(), run_id=run_id, kind=kind)
        if record is None:
            continue
        seen += 1
        batch.append(record)
        if len(batch) >= batch_size:
            flush()
        if seen >= max_urls:
            break
    flush()
    result = {"source": source.id, "status": "ok", "found": seen, "queued": queued}
    logger.info("backfill_done", extra=result)
    return result
