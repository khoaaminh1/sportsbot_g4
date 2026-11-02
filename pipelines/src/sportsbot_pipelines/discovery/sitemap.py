"""Sitemap backfill: find older article URLs that RSS no longer lists.

RSS feeds show only the latest 20-100 items, so they cannot fill a corpus quickly. Most news sites
publish sitemaps (often one per day or month, sometimes a Google News sitemap with titles). This
module reads a sitemap or sitemap index, keeps URLs whose `lastmod` is on or after `since` and
whose path matches the source's `sitemap_include` pattern, and returns Bronze-shaped records.
The extractor then fetches each page (robots.txt and the per-host delay still apply).
"""

from __future__ import annotations

import gzip
import logging
import re
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime
from xml.etree import ElementTree as ET

from sportsbot_pipelines.collectors.dates import parse_date
from sportsbot_pipelines.collectors.registry import Source
from sportsbot_pipelines.collectors.rss import BronzeRecord
from sportsbot_pipelines.collectors.urls import article_id, canonical_url, host_allowed

logger = logging.getLogger(__name__)

_NS = re.compile(r"^\{[^}]+\}")


def _local(tag: str) -> str:
    return _NS.sub("", tag)


@dataclass(frozen=True)
class SitemapEntry:
    loc: str
    lastmod: str | None = None
    title: str | None = None  # Google News sitemaps carry <news:title>


@dataclass
class Sitemap:
    is_index: bool
    entries: list[SitemapEntry]


def parse_sitemap(content: bytes) -> Sitemap:
    """Parse a <urlset> or <sitemapindex> (plain or gzip). Raises ValueError on non-XML input."""
    if content[:2] == b"\x1f\x8b":
        content = gzip.decompress(content)
    try:
        root = ET.fromstring(content)
    except ET.ParseError as exc:
        raise ValueError(f"not a sitemap: {exc}") from exc
    kind = _local(root.tag)
    if kind not in ("urlset", "sitemapindex"):
        raise ValueError(f"not a sitemap: root element <{kind}>")
    entries = []
    for node in root:
        if _local(node.tag) not in ("url", "sitemap"):
            continue
        loc = lastmod = title = None
        for child in node.iter():
            name = _local(child.tag)
            text = (child.text or "").strip()
            if name == "loc" and loc is None:
                loc = text
            elif name in ("lastmod", "publication_date") and text and lastmod is None:
                lastmod = text
            elif name == "title" and text:
                title = text
        if loc:
            entries.append(SitemapEntry(loc=loc, lastmod=lastmod, title=title))
    return Sitemap(is_index=kind == "sitemapindex", entries=entries)


def _on_or_after(lastmod: str | None, since: date, tz: str) -> bool:
    if not lastmod:
        return True  # undated entries are kept; the extractor reads the real date from the page
    parsed = parse_date(lastmod, tz)
    if parsed is None:
        return True
    return datetime.fromisoformat(parsed.iso()).date() >= since


def iter_article_entries(
    start_urls: list[str],
    fetch: Callable[[str], bytes],
    *,
    since: date,
    include: str | None,
    timezone: str = "UTC",
    max_sitemaps: int = 200,
) -> Iterator[SitemapEntry]:
    """Walk sitemap indexes breadth-first; yield article entries newer than `since`."""
    pattern = re.compile(include) if include else None
    queue = list(start_urls)
    visited: set[str] = set()
    while queue and len(visited) < max_sitemaps:
        url = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)
        try:
            sitemap = parse_sitemap(fetch(url))
        except Exception as exc:  # one broken sitemap must not end the backfill
            logger.warning("sitemap_failed", extra={"url": url, "error": str(exc)[:200]})
            continue
        for entry in sitemap.entries:
            if not _on_or_after(entry.lastmod, since, timezone):
                continue
            if sitemap.is_index:
                queue.append(entry.loc)
            elif pattern is None or pattern.search(entry.loc):
                yield entry


def entry_to_record(
    entry: SitemapEntry, source: Source, *, collected_at: datetime, run_id: str
) -> BronzeRecord | None:
    link = canonical_url(entry.loc)
    if not link or not host_allowed(link, source.domains):
        return None
    parsed = parse_date(entry.lastmod, source.timezone) if entry.lastmod else None
    return BronzeRecord(
        record_id=article_id(link),
        source_id=source.id,
        source_name=source.name,
        source_kind="sitemap",
        language_hint=source.language,
        feed_url=link,
        url=entry.loc,
        canonical_url=link,
        title=entry.title or "",  # filled by the extractor from the page
        summary_html=None,
        published_raw=entry.lastmod,
        published_at=parsed.iso() if parsed else None,
        published_tz_assumed=parsed.tz_assumed if parsed else None,
        updated_raw=None,
        collected_at=collected_at.astimezone(UTC).isoformat(),
        run_id=run_id,
    )
