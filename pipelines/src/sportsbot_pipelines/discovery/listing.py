"""Listing-page backfill: walk a site's paginated sports section to find older article URLs.

Some publishers (VnExpress, Tuoi Tre, VietNamNet) put every article at a flat URL such as
`https://vnexpress.net/<slug>-<id>.html`, so their sitemaps cannot be filtered to sports. Their
sports section is paginated, though (`/the-thao`, `/the-thao-p2`, ...), and lists only sports
articles. This module reads those pages, keeps links that match the source's article pattern
and allowed domains, and stops when pages stop giving new links. robots.txt and the per-host
delay still apply (the PoliteClient does the fetching); the extractor fetches each article.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Iterator
from html.parser import HTMLParser
from urllib.parse import urljoin

from sportsbot_pipelines.collectors.registry import Listing
from sportsbot_pipelines.collectors.urls import canonical_url, host_allowed
from sportsbot_pipelines.discovery.sitemap import SitemapEntry

logger = logging.getLogger(__name__)


class _Links(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "a":
            for name, value in attrs:
                if name == "href" and value:
                    self.hrefs.append(value.strip())


def page_urls(listing: Listing) -> Iterator[str]:
    """The first page, then `page_url` with {page} = start, start + 1, ... (max_pages in all)."""
    yield str(listing.first_page)
    for n in range(listing.start, listing.start + listing.max_pages - 1):
        yield listing.page_url.format(page=n)


def article_links(
    html: bytes, base_url: str, pattern: re.Pattern[str], domains: list[str]
) -> list[str]:
    """Canonical article URLs linked from a page, in page order, without duplicates."""
    parser = _Links()
    parser.feed(html.decode("utf-8", errors="replace"))
    out: dict[str, None] = {}
    for href in parser.hrefs:
        link = canonical_url(urljoin(base_url, href))
        if link and host_allowed(link, domains) and pattern.search(link):
            out.setdefault(link, None)
    return list(out)


def iter_listing_entries(
    listing: Listing, fetch: Callable[[str], bytes], domains: list[str]
) -> Iterator[SitemapEntry]:
    """Article entries from the listing pages; stops after `stop_after` pages with nothing new."""
    pattern = re.compile(listing.article_pattern)
    seen: set[str] = set()
    idle = 0
    for url in page_urls(listing):
        try:
            links = article_links(fetch(url), url, pattern, domains)
        except Exception as exc:  # a missing page usually means the end of the section
            logger.warning("listing_page_failed", extra={"url": url, "error": str(exc)[:200]})
            links = []
        fresh = [link for link in links if link not in seen]
        idle = 0 if fresh else idle + 1
        for link in fresh:
            seen.add(link)
            yield SitemapEntry(loc=link)
        if idle >= listing.stop_after:
            break
