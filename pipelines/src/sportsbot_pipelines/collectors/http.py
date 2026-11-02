"""Polite HTTP fetching for collectors.

- robots.txt is checked before every fetch (cached per host for 6 hours); a disallowed URL is
  never requested. If robots.txt cannot be read (network error, 5xx), the fetch is skipped, not
  assumed allowed. A 4xx robots.txt means "no rules" (standard behaviour).
- At most one request per host every `min_seconds_between_requests_per_host` seconds.
- Conditional GET (ETag / Last-Modified) so unchanged feeds cost a 304 and no parsing.
- Retries with exponential backoff on timeouts, 429 and 5xx; the response size is capped.
"""

from __future__ import annotations

import logging
import time
import urllib.robotparser
from collections.abc import Callable
from dataclasses import dataclass, field
from urllib.parse import urlsplit

import httpx

logger = logging.getLogger(__name__)

RETRY_STATUS = {429, 500, 502, 503, 504}


class RobotsDisallowed(Exception):
    pass


class FetchError(Exception):
    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


@dataclass
class FetchResult:
    url: str
    status: int
    content: bytes = b""
    etag: str | None = None
    last_modified: str | None = None

    @property
    def not_modified(self) -> bool:
        return self.status == 304


@dataclass
class _RobotsEntry:
    parser: urllib.robotparser.RobotFileParser
    fetched_at: float


@dataclass
class PoliteClient:
    user_agent: str
    timeout: float = 20.0
    max_bytes: int = 5_000_000
    retries: int = 2
    min_interval: float = 3.0
    robots_ttl: float = 6 * 3600
    transport: httpx.BaseTransport | None = None
    sleep: Callable[[float], None] = time.sleep
    clock: Callable[[], float] = time.monotonic
    _robots: dict[str, _RobotsEntry] = field(default_factory=dict, init=False)
    _last_request: dict[str, float] = field(default_factory=dict, init=False)
    _client: httpx.Client | None = field(default=None, init=False)

    def __enter__(self) -> PoliteClient:
        self._client = httpx.Client(
            timeout=self.timeout,
            follow_redirects=True,
            headers={"User-Agent": self.user_agent, "Accept-Encoding": "gzip, deflate"},
            transport=self.transport,
        )
        return self

    def __exit__(self, *exc: object) -> None:
        if self._client:
            self._client.close()
            self._client = None

    @property
    def client(self) -> httpx.Client:
        if self._client is None:
            raise RuntimeError("use PoliteClient as a context manager")
        return self._client

    # ------------------------------------------------------------------ robots ---
    def robots_allowed(self, url: str) -> bool:
        parts = urlsplit(url)
        origin = f"{parts.scheme}://{parts.netloc}"
        entry = self._robots.get(origin)
        if entry is None or self.clock() - entry.fetched_at > self.robots_ttl:
            parser = urllib.robotparser.RobotFileParser()
            robots_url = f"{origin}/robots.txt"
            self._throttle(parts.netloc)
            try:
                resp = self.client.get(robots_url)
            except httpx.HTTPError as exc:
                raise FetchError(f"robots.txt unreachable: {type(exc).__name__}") from exc
            if resp.status_code >= 500:
                raise FetchError(f"robots.txt returned {resp.status_code}", resp.status_code)
            if resp.status_code >= 400:
                parser.parse([])  # no robots.txt: everything allowed
            else:
                parser.parse(resp.text.splitlines())
            entry = _RobotsEntry(parser, self.clock())
            self._robots[origin] = entry
        return entry.parser.can_fetch(self.user_agent, url)

    def sitemaps_for(self, url: str) -> list[str]:
        """`Sitemap:` lines of the site's robots.txt (empty when there are none)."""
        self.robots_allowed(url)
        parts = urlsplit(url)
        entry = self._robots[f"{parts.scheme}://{parts.netloc}"]
        return list(entry.parser.site_maps() or [])

    # ------------------------------------------------------------------- fetch ---
    def fetch(
        self, url: str, etag: str | None = None, last_modified: str | None = None
    ) -> FetchResult:
        if not self.robots_allowed(url):
            raise RobotsDisallowed(f"robots.txt disallows {url}")
        headers = {}
        if etag:
            headers["If-None-Match"] = etag
        if last_modified:
            headers["If-Modified-Since"] = last_modified
        host = urlsplit(url).netloc
        attempt = 0
        while True:
            self._throttle(host)
            try:
                result = self._get(url, headers)
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                error: FetchError = FetchError(f"{type(exc).__name__}: {exc}"[:200])
            else:
                if result.status in RETRY_STATUS:
                    error = FetchError(f"HTTP {result.status}", result.status)
                elif result.status >= 400:
                    raise FetchError(f"HTTP {result.status}", result.status)
                else:
                    return result
            if attempt >= self.retries:
                raise error
            attempt += 1
            backoff = 2**attempt
            logger.info("fetch_retry", extra={"url": url, "attempt": attempt, "error": str(error)})
            self.sleep(backoff)

    def _get(self, url: str, headers: dict[str, str]) -> FetchResult:
        with self.client.stream("GET", url, headers=headers) as resp:
            body = bytearray()
            if resp.status_code == 200:
                for chunk in resp.iter_bytes():
                    body.extend(chunk)
                    if len(body) > self.max_bytes:
                        raise FetchError(f"response larger than {self.max_bytes} bytes")
            return FetchResult(
                url=str(resp.url),
                status=resp.status_code,
                content=bytes(body),
                etag=resp.headers.get("etag"),
                last_modified=resp.headers.get("last-modified"),
            )

    def _throttle(self, host: str) -> None:
        last = self._last_request.get(host)
        if last is not None:
            wait = self.min_interval - (self.clock() - last)
            if wait > 0:
                self.sleep(wait)
        self._last_request[host] = self.clock()
