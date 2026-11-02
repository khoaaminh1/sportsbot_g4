from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from sportsbot_pipelines.collectors.registry import Source

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def feed_bytes() -> bytes:
    return (FIXTURES / "feed_vi.xml").read_bytes()


@pytest.fixture
def source() -> Source:
    return Source(
        id="fixture_vi",
        name="Fixture",
        kind="rss",
        url="https://news.fixture.test/rss/the-thao.rss",
        language="vi",
        interval_minutes=30,
        domains=["fixture.test"],
    )


class FakeSite:
    """httpx MockTransport handler serving robots.txt and a feed, recording every request."""

    def __init__(self, feed: bytes, robots: str = "User-agent: *\nDisallow: /private/\n") -> None:
        self.feed = feed
        self.robots = robots
        self.requests: list[httpx.Request] = []
        self.feed_status: list[int] = []  # statuses to return before succeeding
        self.etag = '"v1"'

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.url.path == "/robots.txt":
            return httpx.Response(200, text=self.robots)
        if self.feed_status:
            return httpx.Response(self.feed_status.pop(0))
        if request.headers.get("if-none-match") == self.etag:
            return httpx.Response(304)
        return httpx.Response(200, content=self.feed, headers={"ETag": self.etag})

    def feed_requests(self) -> list[httpx.Request]:
        return [r for r in self.requests if r.url.path != "/robots.txt"]


@pytest.fixture
def site(feed_bytes) -> FakeSite:
    return FakeSite(feed_bytes)
