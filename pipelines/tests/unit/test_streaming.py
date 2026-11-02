"""Streaming pipeline (ADR 0005): discovery -> extractor -> consumers, all in memory."""

from __future__ import annotations

import gzip
import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from sportsbot_pipelines.collectors.http import FetchError, FetchResult, RobotsDisallowed
from sportsbot_pipelines.collectors.rss import BronzeRecord
from sportsbot_pipelines.consumers import operational, semantic
from sportsbot_pipelines.consumers.archive import ArchiveBuffer
from sportsbot_pipelines.consumers.chunking import chunk_text
from sportsbot_pipelines.discovery.backfill import backfill_source
from sportsbot_pipelines.discovery.publish import bus_publisher, discovered_message
from sportsbot_pipelines.discovery.sitemap import (
    entry_to_record,
    iter_article_entries,
    parse_sitemap,
)
from sportsbot_pipelines.extraction.extract import (
    ExtractedPage,
    clean_text,
    content_hash,
    extract_page,
)
from sportsbot_pipelines.extraction.worker import ExtractorStats, MemoryDedup, make_handler
from sportsbot_pipelines.ingestion.bronze import LocalBronzeWriter
from sportsbot_pipelines.ingestion.state import MemoryState
from sportsbot_pipelines.streaming import topics
from sportsbot_pipelines.streaming.bus import MemoryBus

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
NOW = datetime(2026, 11, 2, 2, 0, tzinfo=UTC)
LONG_TEXT = ("Đội tuyển Việt Nam thắng Thái Lan 2-1 trong trận chung kết. " * 20).strip()


def record(i: int = 1, source_id: str = "fixture_vi", title: str = "Tin số 1") -> BronzeRecord:
    url = f"https://news.fixture.test/the-thao/tin-{i}.html"
    return BronzeRecord(
        record_id=f"id{i}",
        source_id=source_id,
        source_name="Fixture",
        source_kind="rss",
        language_hint="vi",
        feed_url="https://news.fixture.test/rss",
        url=url,
        canonical_url=url,
        title=title,
        summary_html="<p>Tóm tắt</p>",
        published_raw=None,
        published_at="2026-11-01T13:15:00+00:00",
        published_tz_assumed=False,
        updated_raw=None,
        collected_at=NOW.isoformat(),
        run_id="r1",
    )


class FakeClient:
    """Stands in for PoliteClient: url -> bytes, or an exception to raise."""

    def __init__(self, pages: dict[str, bytes | Exception]) -> None:
        self.pages = pages
        self.fetched: list[str] = []

    def fetch(self, url: str, **_: object) -> FetchResult:
        self.fetched.append(url)
        page = self.pages[url]
        if isinstance(page, Exception):
            raise page
        return FetchResult(url=url, status=200, content=page)


def fake_extract(text: str = LONG_TEXT, title: str | None = "Tiêu đề trang"):
    return lambda html, url: ExtractedPage(
        title=title, text=text, author="A", published="2026-11-01T20:15:00+07:00", language="vi"
    )


# ------------------------------------------------------------------ sitemap -----

URLSET = b"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"
        xmlns:news="http://www.google.com/schemas/sitemap-news/0.9">
  <url><loc>https://news.fixture.test/the-thao/a-1.html</loc><lastmod>2026-10-30T08:00:00+07:00</lastmod>
    <news:news><news:title>Tin A</news:title></news:news></url>
  <url><loc>https://news.fixture.test/kinh-te/b-2.html</loc><lastmod>2026-10-30T08:00:00+07:00</lastmod></url>
  <url><loc>https://news.fixture.test/the-thao/old-3.html</loc><lastmod>2025-01-01T08:00:00+07:00</lastmod></url>
  <url><loc>https://other.example/the-thao/x.html</loc></url>
</urlset>"""

INDEX = b"""<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap><loc>https://news.fixture.test/sitemap-2026-10.xml</loc><lastmod>2026-10-31</lastmod></sitemap>
  <sitemap><loc>https://news.fixture.test/sitemap-2024-01.xml</loc><lastmod>2024-01-31T00:00:00Z</lastmod></sitemap>
</sitemapindex>"""


def test_parse_sitemap_reads_urlset_and_index():
    urlset = parse_sitemap(URLSET)
    assert not urlset.is_index and len(urlset.entries) == 4
    assert urlset.entries[0].title == "Tin A"
    assert parse_sitemap(INDEX).is_index
    with pytest.raises(ValueError):
        parse_sitemap(b"<html>not a sitemap</html>")


def test_sitemap_walk_filters_by_date_and_pattern(source):
    pages = {
        "https://news.fixture.test/sitemap.xml": INDEX,
        "https://news.fixture.test/sitemap-2026-10.xml": URLSET,
    }
    fetched: list[str] = []

    def fetch(url: str) -> bytes:
        fetched.append(url)
        return pages[url]

    entries = list(
        iter_article_entries(
            ["https://news.fixture.test/sitemap.xml"],
            fetch,
            since=date(2026, 10, 1),
            include=r"/the-thao/",
            timezone="Asia/Ho_Chi_Minh",
        )
    )
    # The 2024 child sitemap is never fetched; the old and the economy URLs are dropped.
    assert "https://news.fixture.test/sitemap-2024-01.xml" not in fetched
    locs = [e.loc for e in entries]
    assert "https://news.fixture.test/the-thao/a-1.html" in locs
    assert all("/the-thao/" in u for u in locs) and not any("old-3" in u for u in locs)
    records = [entry_to_record(e, source, collected_at=NOW, run_id="b") for e in entries]
    kept = [r for r in records if r is not None]
    assert [r.canonical_url for r in kept] == ["https://news.fixture.test/the-thao/a-1.html"]
    assert kept[0].title == "Tin A" and kept[0].source_kind == "sitemap"


def test_backfill_queues_each_url_once(source):
    src = source.model_copy(
        update={
            "sitemaps": ["https://news.fixture.test/sitemap-2026-10.xml"],
            "sitemap_include": "/the-thao/",
        }
    )
    client = FakeClient({"https://news.fixture.test/sitemap-2026-10.xml": URLSET})
    state, published = MemoryState(), []
    first = backfill_source(src, client, state, published.extend, since=date(2026, 10, 1))
    second = backfill_source(src, client, state, published.extend, since=date(2026, 10, 1))
    assert first["queued"] == 1 and second["queued"] == 0 and len(published) == 1
    assert (
        backfill_source(source, client, state, published.extend, since=date(2026, 1, 1))["status"]
        == "no_sitemaps"
    )


# ---------------------------------------------------------------- discovery -----


def test_discovered_message_and_publisher():
    bus = MemoryBus()
    bus_publisher(bus)([record(1), record(2)])
    values = bus.values(topics.DISCOVERED)
    assert [v["article_id"] for v in values] == ["id1", "id2"]
    assert values[0]["schema"] == topics.DISCOVERED_SCHEMA
    assert values[0]["discovered_via"] == "rss"
    assert [k for k, _ in bus.messages[topics.DISCOVERED]] == ["fixture_vi", "fixture_vi"]
    assert discovered_message(record(3), "sitemap")["discovered_via"] == "sitemap"


# ---------------------------------------------------------------- extraction -----


def test_extract_page_from_real_html():
    page = extract_page((FIXTURES / "article_vi.html").read_bytes(), "https://news.fixture.test/a")
    assert page is not None
    assert "Mỹ Đình" in page.text and "Bản quyền" not in page.text and "Trang chủ" not in page.text
    assert page.title and "Việt Nam" in page.title


def test_content_hash_ignores_spacing_case_and_punctuation():
    assert content_hash("Việt Nam  thắng,  2-1!") == content_hash("việt nam thắng 2 1")
    assert content_hash("a b") != content_hash("a c")
    assert clean_text("a   b\n\n\n\nc") == "a b\n\nc"


def _discovered(i: int = 1) -> dict:
    return discovered_message(record(i))


def test_extractor_publishes_extracted_message():
    bus, stats = MemoryBus(), ExtractorStats()
    msg = _discovered(1)
    handle = make_handler(
        FakeClient({msg["canonical_url"]: b"<html/>"}),
        bus,
        MemoryDedup(),
        extract=fake_extract(),
        stats=stats,
    )
    handle(msg)
    [out] = bus.values(topics.EXTRACTED)
    assert out["schema"] == topics.EXTRACTED_SCHEMA and out["article_id"] == "id1"
    assert out["title"] == "Tin số 1"  # the feed title wins over the page title
    assert out["text"] == LONG_TEXT and out["word_count"] > 50
    assert out["published_at"] == "2026-11-01T13:15:00+00:00"  # feed date kept
    assert stats.extracted == 1 and not bus.values(topics.DEAD_LETTER)


def test_extractor_uses_page_title_and_date_when_discovery_had_none():
    bus = MemoryBus()
    msg = {**_discovered(1), "title": "", "published_at": None}
    make_handler(
        FakeClient({msg["canonical_url"]: b""}), bus, MemoryDedup(), extract=fake_extract()
    )(msg)
    [out] = bus.values(topics.EXTRACTED)
    assert out["title"] == "Tiêu đề trang" and out["published_at"].startswith("2026-11-01T13:15")


@pytest.mark.parametrize(
    ("page", "extract", "reason"),
    [
        (RobotsDisallowed("no"), fake_extract(), "robots_disallowed"),
        (FetchError("gone", status=404), fake_extract(), "http_404"),
        (b"<html/>", lambda html, url: None, "no_main_text"),
        (b"<html/>", fake_extract(text="ngắn"), "too_short"),
        (b"<html/>", fake_extract(title=None), "no_title"),
    ],
)
def test_extractor_dead_letters_failures(page, extract, reason):
    bus, stats = MemoryBus(), ExtractorStats()
    msg = {**_discovered(1), "title": ""} if reason == "no_title" else _discovered(1)
    make_handler(
        FakeClient({msg["canonical_url"]: page}), bus, MemoryDedup(), extract=extract, stats=stats
    )(msg)
    [dead] = bus.values(topics.DEAD_LETTER)
    assert dead["reason"] == reason and dead["stage"] == "extract"
    assert dead["payload"]["article_id"] == "id1"
    assert stats.dead_lettered == 1 and not bus.values(topics.EXTRACTED)


def test_extractor_drops_duplicates_without_refetching():
    bus, dedup, stats = MemoryBus(), MemoryDedup(), ExtractorStats()
    a, b = _discovered(1), _discovered(2)  # different URLs, same text
    client = FakeClient({a["canonical_url"]: b"", b["canonical_url"]: b""})
    handle = make_handler(client, bus, dedup, extract=fake_extract(), stats=stats)
    handle(a)
    handle(b)  # same content under another URL
    handle(a)  # same article again: no fetch
    assert len(bus.values(topics.EXTRACTED)) == 1
    assert stats.duplicates == 2 and client.fetched.count(a["canonical_url"]) == 1


# ---------------------------------------------------------------- consumers -----


def test_chunking_respects_size_and_keeps_all_text():
    paragraphs = [f"Câu số {i} nói về trận đấu bóng đá hôm qua." * 3 for i in range(30)]
    text = "\n".join(paragraphs)
    chunks = chunk_text(text, max_chars=500, overlap_chars=100)
    assert len(chunks) > 3 and all(len(c) <= 500 for c in chunks)
    joined = "\n".join(chunks)
    assert all(p in joined for p in paragraphs)
    assert chunk_text("") == []
    assert all(len(c) <= 300 for c in chunk_text("x" * 1000, max_chars=300))


class FakeEmbedder:
    def embed(self, texts: list[str]) -> list[list[float]]:
        return [[float(len(t)), 1.0] for t in texts]


class FakeStore:
    def __init__(self) -> None:
        self.points: dict[str, dict] = {}

    def upsert(self, points: list[dict]) -> None:
        self.points.update({p["id"]: p for p in points})


def _extracted(i: int = 1) -> dict:
    bus = MemoryBus()
    msg = _discovered(i)
    make_handler(
        FakeClient({msg["canonical_url"]: b""}), bus, MemoryDedup(), extract=fake_extract()
    )(msg)
    return bus.values(topics.EXTRACTED)[0]


def test_semantic_consumer_upserts_idempotently():
    store = FakeStore()
    handle = semantic.make_handler(FakeEmbedder(), store, max_chars=300, overlap_chars=50)
    message = _extracted(1)
    handle(message)
    count = len(store.points)
    handle(message)  # re-delivery: same point ids, nothing added
    assert count > 1 and len(store.points) == count
    payload = next(iter(store.points.values()))["payload"]
    assert payload["article_id"] == "id1" and payload["url"].endswith("tin-1.html")
    assert payload["published_ts"] == int(datetime(2026, 11, 1, 13, 15, tzinfo=UTC).timestamp())
    assert payload["title"] == "Tin số 1" and payload["text"]


def test_operational_row_mapping():
    row = operational.row_from_message(_extracted(1))
    assert row["article_id"] == "id1" and row["content"] == LONG_TEXT
    assert row["summary"] == "Tóm tắt"  # from the feed summary HTML
    assert row["source_url"].endswith("tin-1.html") and row["content_hash"]
    assert row["discovered_via"] == "rss"


def test_archive_buffer_writes_batches(tmp_path):
    archive = ArchiveBuffer(LocalBronzeWriter(tmp_path), batch_size=2, now=lambda: NOW)
    for i in range(3):
        archive.handle({"article_id": f"id{i}"})
    assert len(archive.files_written) == 1
    archive.flush()
    archive.flush()  # empty buffer: nothing written
    files = sorted(tmp_path.rglob("*.jsonl.gz"))
    assert len(files) == 2 and "source=extracted" in str(files[0])
    lines = gzip.decompress(files[0].read_bytes()).decode().splitlines()
    assert {json.loads(x)["article_id"] for x in lines} <= {"id0", "id1", "id2"}


def test_memory_bus_dead_letters_handler_exceptions():
    bus = MemoryBus()
    bus.publish(topics.EXTRACTED, "s", {"article_id": "x", "source_id": "s"})

    def boom(_: dict) -> None:
        raise RuntimeError("db down")

    assert bus.drain(topics.EXTRACTED, "g", boom, stage="operational") == 1
    [dead] = bus.values(topics.DEAD_LETTER)
    assert dead["stage"] == "operational" and dead["reason"] == "handler_exception"
    assert "db down" in dead["error"]
    # Each consumer group reads the topic independently.
    assert bus.drain(topics.EXTRACTED, "g", boom, stage="operational") == 0
    assert bus.drain(topics.EXTRACTED, "other", lambda _: None, stage="x") == 1


def test_end_to_end_in_memory():
    bus, store = MemoryBus(), FakeStore()
    bus_publisher(bus)([record(1), record(2)])
    pages = {_discovered(i)["canonical_url"]: b"" for i in (1, 2)}
    texts = iter([LONG_TEXT, LONG_TEXT.replace("Thái Lan", "Indonesia")])
    extract = lambda html, url: ExtractedPage("T", next(texts), None, None, "vi")  # noqa: E731
    bus.drain(
        topics.DISCOVERED,
        topics.GROUP_EXTRACTOR,
        make_handler(FakeClient(pages), bus, MemoryDedup(), extract=extract),
        stage="extract",
    )
    rows: list[dict] = []
    bus.drain(
        topics.EXTRACTED,
        topics.GROUP_OPERATIONAL,
        lambda m: rows.append(operational.row_from_message(m)),
        stage="operational",
    )
    bus.drain(
        topics.EXTRACTED,
        topics.GROUP_SEMANTIC,
        semantic.make_handler(FakeEmbedder(), store),
        stage="semantic",
    )
    assert [r["article_id"] for r in rows] == ["id1", "id2"]
    assert {p["payload"]["article_id"] for p in store.points.values()} == {"id1", "id2"}
    assert not bus.values(topics.DEAD_LETTER)


def test_parse_gzipped_sitemap():
    assert len(parse_sitemap(gzip.compress(URLSET)).entries) == 4


def test_semantic_outage_is_transient_not_dead_lettered():
    from sportsbot_agent.clients.embeddings import EmbeddingError
    from sportsbot_pipelines.streaming.bus import TransientError

    class DownEmbedder:
        def embed(self, texts):
            raise EmbeddingError("connection refused")

    bus = MemoryBus()
    bus.publish(topics.EXTRACTED, "s", _extracted(1))
    handle = semantic.make_handler(DownEmbedder(), FakeStore())
    with pytest.raises(TransientError):
        bus.drain(topics.EXTRACTED, "g", handle, stage="semantic")
    assert not bus.values(topics.DEAD_LETTER)


def test_network_errors_are_retryable_dead_letters_and_replayable():
    from sportsbot_pipelines.streaming.replay import replay, should_replay

    bus = MemoryBus()
    msg = _discovered(1)
    handle = make_handler(
        FakeClient({msg["canonical_url"]: FetchError("robots.txt unreachable: ConnectError")}),
        bus,
        MemoryDedup(),
        extract=fake_extract(),
    )
    handle(msg)
    [dead] = bus.values(topics.DEAD_LETTER)
    assert dead["reason"] == "network_error" and dead["retryable"] is True
    assert should_replay(dead)
    assert should_replay({**dead, "reason": "http_error", "retryable": None})  # legacy shape
    assert not should_replay({**dead, "reason": "too_short", "retryable": False})
    assert should_replay({**dead, "reason": "too_short", "retryable": False}, {"too_short"})
    assert not should_replay({**dead, "stage": "semantic"})
    out = MemoryBus()
    assert replay([dead, {**dead, "retryable": False, "reason": "x"}], out) == {
        "dead_letters_read": 2,
        "replayed": 1,
    }
    assert out.values(topics.DISCOVERED)[0]["article_id"] == "id1"


def test_http_429_is_retryable_404_is_not():
    for status, retryable in ((429, True), (503, True), (404, False)):
        bus = MemoryBus()
        msg = _discovered(1)
        make_handler(
            FakeClient({msg["canonical_url"]: FetchError("x", status=status)}),
            bus,
            MemoryDedup(),
            extract=fake_extract(),
        )(msg)
        assert bus.values(topics.DEAD_LETTER)[0]["retryable"] is retryable


def test_rss_only_source_stores_feed_summary_without_fetching():
    bus = MemoryBus()
    msg = {
        **_discovered(1),
        "summary_html": "<p>Arsenal win at Anfield for the first time since 2012.</p>",
    }
    client = FakeClient({})
    make_handler(client, bus, MemoryDedup(), extract=fake_extract(), rss_only={"fixture_vi"})(msg)
    [out] = bus.values(topics.EXTRACTED)
    assert client.fetched == [] and out["extractor"] == "rss_summary"
    assert out["text"] == "Arsenal win at Anfield for the first time since 2012."
    short = {**_discovered(2), "summary_html": "<p>Hi</p>"}
    make_handler(client, bus, MemoryDedup(), extract=fake_extract(), rss_only={"fixture_vi"})(short)
    assert bus.values(topics.DEAD_LETTER)[0]["reason"] == "summary_too_short"


def test_semantic_consumer_skips_chunks_the_embedder_rejects():
    class PartialEmbedder:
        def embed(self, texts):
            return [None if i == 0 else [1.0, 0.0] for i in range(len(texts))]

    store = FakeStore()
    semantic.make_handler(PartialEmbedder(), store, max_chars=300, overlap_chars=50)(_extracted(1))
    assert store.points and all(p["payload"]["article_id"] == "id1" for p in store.points.values())


# ------------------------------------------------------------ listing backfill -----


def listing_page(*slugs: str) -> bytes:
    links = "".join(f'<a href="/{s}">{s}</a>' for s in slugs)
    return (
        f'<html><body>{links}<a href="https://other.test/x-1.html">ad</a>'
        '<a href="/the-thao">section</a></body></html>'
    ).encode()


def test_listing_backfill_walks_pages_until_nothing_new(source):
    from sportsbot_pipelines.collectors.registry import Listing

    src = source.model_copy(
        update={
            "listing": Listing(
                first_page="https://news.fixture.test/the-thao",
                page_url="https://news.fixture.test/the-thao-p{page}",
                max_pages=10,
                article_pattern=r"-\d{4,}\.html$",
            )
        }
    )
    client = FakeClient(
        {
            "https://news.fixture.test/the-thao": listing_page("a-1001.html", "b-1002.html"),
            "https://news.fixture.test/the-thao-p2": listing_page("b-1002.html", "c-1003.html"),
            "https://news.fixture.test/the-thao-p3": listing_page("c-1003.html"),
            "https://news.fixture.test/the-thao-p4": FetchError("HTTP 404"),
        }
    )
    bus = MemoryBus()
    result = backfill_source(
        src, client, MemoryState(), bus_publisher(bus, None), since=date(2026, 10, 1)
    )
    values = bus.values(topics.DISCOVERED)
    assert result["queued"] == 3
    assert [v["url"] for v in values] == [
        "https://news.fixture.test/a-1001.html",
        "https://news.fixture.test/b-1002.html",
        "https://news.fixture.test/c-1003.html",
    ]
    assert {v["discovered_via"] for v in values} == {"listing"}
    # p3 gave nothing new and p4 failed: two idle pages, so p5 is never requested.
    assert client.fetched[-1] == "https://news.fixture.test/the-thao-p4"


def test_listing_config_is_validated():
    from pydantic import ValidationError

    from sportsbot_pipelines.collectors.registry import Listing

    with pytest.raises(ValidationError):
        Listing(first_page="https://x.test/s", page_url="https://x.test/s-p", article_pattern="x")
    with pytest.raises(ValidationError):
        Listing(
            first_page="https://x.test/s", page_url="https://x.test/{page}", article_pattern="("
        )
