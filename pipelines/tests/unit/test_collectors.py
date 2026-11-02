from __future__ import annotations

import gzip
import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest

from sportsbot_pipelines.collectors.http import FetchError, PoliteClient, RobotsDisallowed
from sportsbot_pipelines.collectors.registry import Registry, Source, load_registry
from sportsbot_pipelines.collectors.rss import parse_feed
from sportsbot_pipelines.collectors.urls import article_id, canonical_url, host_allowed
from sportsbot_pipelines.ingestion.bronze import (
    LocalBronzeWriter,
    WebHdfsBronzeWriter,
    bronze_relpath,
    encode,
)

REPO = Path(__file__).resolve().parents[3]
NOW = datetime(2026, 11, 2, 2, 0, tzinfo=UTC)


# ---------------------------------------------------------------- registry -----


def test_repository_registry_is_valid():
    registry = load_registry(REPO / "pipelines" / "sources.json")
    enabled = registry.enabled()
    # Phase 3 acceptance: at least 3 languages. French is disabled until a source accepts us.
    assert len(enabled) >= 6
    assert {s.language for s in enabled} >= {"vi", "en", "es"}
    assert all(s.timezone for s in enabled)


def test_registry_rejects_duplicates_and_bad_values(source):
    with pytest.raises(ValueError, match="duplicate"):
        Registry(sources=[source, source])
    with pytest.raises(ValueError):
        Source(**{**source.model_dump(), "interval_minutes": 1})
    with pytest.raises(ValueError, match="timezone"):
        Source(**{**source.model_dump(), "timezone": "Mars/Olympus"})
    with pytest.raises(ValueError, match="no collector"):
        Source(**{**source.model_dump(), "kind": "page"})


# -------------------------------------------------------------------- urls -----


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("https://News.Example.vn/a.html?utm_source=rss#c", "https://news.example.vn/a.html"),
        ("https://x.vn/a?id=5&utm_medium=x&fbclid=1", "https://x.vn/a?id=5"),
        ("http://x.vn:80/a", "http://x.vn/a"),
        ("https://x.vn", "https://x.vn/"),
        ("javascript:alert(1)", None),
        ("ftp://x.vn/a", None),
        ("", None),
        (None, None),
    ],
)
def test_canonical_url(raw, expected):
    assert canonical_url(raw) == expected


def test_host_allowed_and_article_id():
    assert host_allowed("https://www.vnexpress.net/a", ["vnexpress.net"])
    assert not host_allowed("https://vnexpress.net.evil.test/a", ["vnexpress.net"])
    assert article_id("https://x.vn/a") == article_id("https://x.vn/a")
    assert len(article_id("https://x.vn/a")) == 64


# --------------------------------------------------------------------- rss -----


def test_parse_feed_keeps_valid_items_and_counts_drops(feed_bytes, source):
    result = parse_feed(feed_bytes, source, collected_at=NOW, run_id="r1")
    assert result.entries_total == 7
    assert len(result.records) == 3
    assert result.dropped_duplicate == 1
    assert result.dropped_foreign_domain == 1
    assert result.dropped_no_link == 1
    assert result.dropped_no_title == 1

    first = result.records[0]
    assert first.canonical_url == "https://news.fixture.test/bong-da/bai-1.html"
    assert first.published_at == "2026-11-02T01:30:00+00:00"  # +0700 converted to UTC
    assert first.published_raw == "Mon, 02 Nov 2026 08:30:00 +0700"  # original kept
    assert first.published_tz_assumed is False
    assert first.collected_at == "2026-11-02T02:00:00+00:00"
    assert first.tags == ["Bóng đá"]
    assert "<b>" in first.summary_html  # Bronze keeps the raw HTML
    assert first.updated_raw is None  # no <updated>: not filled from pubDate
    no_date = result.records[2]
    assert no_date.published_at is None and no_date.published_raw is None


def test_parse_feed_reports_garbage(source):
    result = parse_feed(b"<html>not a feed", source, collected_at=NOW, run_id="r1")
    assert result.records == []
    assert result.feed_error


# -------------------------------------------------------------------- http -----


def client_for(site, **kwargs) -> PoliteClient:
    defaults = {"user_agent": "TestBot/1.0", "min_interval": 0, "sleep": lambda s: None}
    return PoliteClient(transport=httpx.MockTransport(site), **{**defaults, **kwargs})


def test_fetch_checks_robots_once_and_uses_conditional_get(site):
    with client_for(site) as client:
        first = client.fetch("https://news.fixture.test/rss.xml")
        second = client.fetch("https://news.fixture.test/rss.xml", etag=first.etag)
    assert first.status == 200 and first.etag == '"v1"'
    assert second.not_modified
    assert [r.url.path for r in site.requests].count("/robots.txt") == 1
    assert site.requests[0].headers["user-agent"] == "TestBot/1.0"


def test_robots_disallow_blocks_request(site):
    with client_for(site) as client, pytest.raises(RobotsDisallowed):
        client.fetch("https://news.fixture.test/private/feed.xml")
    assert site.feed_requests() == []


def test_retries_on_503_then_succeeds(site):
    site.feed_status = [503]
    sleeps: list[float] = []
    with client_for(site, sleep=sleeps.append) as client:
        result = client.fetch("https://news.fixture.test/rss.xml")
    assert result.status == 200
    assert sleeps == [2]


def test_gives_up_after_retries_and_does_not_retry_404(site):
    site.feed_status = [503, 503, 503]
    with client_for(site, retries=2) as client, pytest.raises(FetchError) as err:
        client.fetch("https://news.fixture.test/rss.xml")
    assert err.value.status == 503
    site.feed_status = [404]
    with client_for(site) as client, pytest.raises(FetchError) as err:
        client.fetch("https://news.fixture.test/rss.xml")
    assert err.value.status == 404


def test_size_limit(site):
    with client_for(site, max_bytes=100) as client, pytest.raises(FetchError, match="larger"):
        client.fetch("https://news.fixture.test/rss.xml")


def test_unreachable_robots_is_not_treated_as_allowed():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(503)
        raise AssertionError("feed must not be fetched")

    with (
        PoliteClient(
            user_agent="T", min_interval=0, transport=httpx.MockTransport(handler)
        ) as client,
        pytest.raises(FetchError, match="robots"),
    ):
        client.fetch("https://news.fixture.test/rss.xml")


def test_politeness_delay_between_requests_to_same_host(site):
    clock = iter([0.0, 0.0, 0.5, 0.5, 10.0, 10.0, 10.0, 10.0])
    sleeps: list[float] = []
    with client_for(site, min_interval=3.0, sleep=sleeps.append, clock=lambda: next(clock)) as c:
        c.fetch("https://news.fixture.test/rss.xml")
    assert sleeps and sleeps[0] == pytest.approx(2.5)


# ------------------------------------------------------------------ bronze -----


def test_bronze_path_and_local_writer(tmp_path, feed_bytes, source):
    records = parse_feed(feed_bytes, source, collected_at=NOW, run_id="r1").records
    rel = bronze_relpath("fixture_vi", NOW, "r1")
    assert rel == "source=fixture_vi/ingest_date=2026-11-02/ingest_hour=02/part-r1.jsonl.gz"
    writer = LocalBronzeWriter(tmp_path)
    path = writer.write(rel, encode(records))
    lines = gzip.decompress(Path(path).read_bytes()).decode().splitlines()
    assert [json.loads(line)["record_id"] for line in lines] == [r.record_id for r in records]
    with pytest.raises(FileExistsError):
        writer.write(rel, b"again")  # never overwrite


def test_webhdfs_writer_follows_two_step_create():
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if request.url.host == "namenode":
            assert request.url.params["op"] == "CREATE"
            assert request.url.params["overwrite"] == "false"
            return httpx.Response(307, headers={"Location": "http://datanode1:9864/webhdfs/v1/x"})
        assert request.content == b"data"
        return httpx.Response(201)

    writer = WebHdfsBronzeWriter(
        "http://namenode:9870",
        "/sportsbot/bronze",
        "sportsbot",
        transport=httpx.MockTransport(handler),
    )
    path = writer.write("source=a/part-1.jsonl.gz", b"data")
    assert path == "hdfs:///sportsbot/bronze/source=a/part-1.jsonl.gz"
    assert calls[0].url.path.endswith("/sportsbot/bronze/source%3Da/part-1.jsonl.gz") or calls[
        0
    ].url.path.endswith("/sportsbot/bronze/source=a/part-1.jsonl.gz")
    assert len(calls) == 2


def test_double_escaped_titles_are_unescaped(source):
    from datetime import UTC, datetime

    from sportsbot_pipelines.collectors.rss import parse_feed

    title = "CLB TP.Đồng Nai vẫn dưới đ&amp;aacute;y"  # as the Thanh Niên feed sends it
    feed = (
        '<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>x</title>'
        f"<item><title>{title}</title><link>https://news.fixture.test/a.html</link></item>"
        "</channel></rss>"
    ).encode()
    parsed = parse_feed(feed, source, collected_at=datetime(2026, 11, 3, tzinfo=UTC), run_id="r")
    assert parsed.records[0].title.endswith("dưới đáy")
