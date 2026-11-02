"""Turn an RSS/Atom feed into Bronze records.

Bronze keeps the source's data as given: the original title and summary HTML, the original date
strings, tags and links. Only light, lossless additions are made: canonical URL, article id,
the date converted to UTC (when parseable), and ingestion metadata. Cleaning happens in Silver.
"""

from __future__ import annotations

import calendar
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

import feedparser

from sportsbot_pipelines import __version__
from sportsbot_pipelines.collectors.registry import Source
from sportsbot_pipelines.collectors.urls import article_id, canonical_url, host_allowed

BRONZE_SCHEMA_VERSION = 1


@dataclass
class BronzeRecord:
    record_id: str  # article id = sha256(canonical_url)
    source_id: str
    source_name: str
    source_kind: str
    language_hint: str  # the source's expected language; Silver detects the real one
    feed_url: str
    url: str  # link as given by the feed
    canonical_url: str
    title: str
    summary_html: str | None
    published_raw: str | None
    published_at: str | None  # UTC RFC 3339, None when absent or unparseable
    updated_raw: str | None
    authors: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    collected_at: str = ""
    run_id: str = ""
    http_status: int = 200
    collector_version: str = __version__
    schema_version: int = BRONZE_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ParseResult:
    records: list[BronzeRecord]
    entries_total: int
    dropped_no_link: int = 0
    dropped_foreign_domain: int = 0
    dropped_no_title: int = 0
    dropped_duplicate: int = 0
    feed_error: str | None = None


def _utc(struct: Any) -> str | None:
    if not struct:
        return None
    try:
        return datetime.fromtimestamp(calendar.timegm(struct), UTC).isoformat()
    except (OverflowError, ValueError, TypeError):
        return None


def parse_feed(
    content: bytes,
    source: Source,
    *,
    collected_at: datetime,
    run_id: str,
    http_status: int = 200,
    max_items: int = 200,
) -> ParseResult:
    feed = feedparser.parse(content)
    entries = feed.entries[:max_items]
    result = ParseResult(records=[], entries_total=len(feed.entries))
    if feed.bozo and not feed.entries:
        result.feed_error = f"unparseable feed: {type(feed.bozo_exception).__name__}"
        return result

    seen: set[str] = set()
    for entry in entries:
        link = canonical_url(entry.get("link"))
        if not link:
            result.dropped_no_link += 1
            continue
        if not host_allowed(link, source.domains):
            result.dropped_foreign_domain += 1
            continue
        title = (entry.get("title") or "").strip()
        if not title:
            result.dropped_no_title += 1
            continue
        rid = article_id(link)
        if rid in seen:
            result.dropped_duplicate += 1
            continue
        seen.add(rid)
        result.records.append(
            BronzeRecord(
                record_id=rid,
                source_id=source.id,
                source_name=source.name,
                source_kind=source.kind,
                language_hint=source.language,
                feed_url=str(source.url),
                url=entry.get("link"),
                canonical_url=link,
                title=title,
                summary_html=entry.get("summary"),
                published_raw=entry.get("published") or None,
                published_at=_utc(entry.get("published_parsed")),
                # dict.get: FeedParserDict.get("updated") falls back to "published".
                updated_raw=dict.get(entry, "updated") or None,
                authors=[a.get("name") for a in entry.get("authors", []) if a.get("name")],
                tags=[t.get("term") for t in entry.get("tags", []) if t.get("term")],
                collected_at=collected_at.astimezone(UTC).isoformat(),
                run_id=run_id,
                http_status=http_status,
            )
        )
    return result
