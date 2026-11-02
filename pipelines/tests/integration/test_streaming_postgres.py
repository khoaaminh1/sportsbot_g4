"""Operational consumer, extractor dedup, keyword search and news queries on real PostgreSQL.

Skipped unless SPORTSBOT_INTEGRATION=1. Rows use the `itest_` source prefix and are removed.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from sportsbot_agent.hybrid import PostgresKeywordSearch
from sportsbot_agent.retrieval import query_terms
from sportsbot_pipelines.collectors.registry import Source
from sportsbot_pipelines.consumers import operational
from sportsbot_pipelines.extraction.extract import content_hash
from sportsbot_pipelines.extraction.worker import PostgresDedup
from sportsbot_pipelines.ingestion.migrate import migrate
from sportsbot_pipelines.ingestion.state import PostgresState

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("SPORTSBOT_INTEGRATION") != "1",
        reason="set SPORTSBOT_INTEGRATION=1 with PostgreSQL running",
    ),
]

MIGRATIONS = Path(__file__).resolve().parents[3] / "infra" / "postgres" / "migrations"
DB_URL = os.environ.get(
    "DATABASE_URL", "postgresql+psycopg://sportsbot:change-me-local-only@localhost:5432/sportsbot"
)
SOURCE = "itest_stream"


def message(n: int, body: str, title: str) -> dict:
    url = f"https://itest.example/the-thao/{n}.html"
    return {
        "article_id": hashlib.sha256(url.encode()).hexdigest(),
        "source_id": SOURCE,
        "source_name": "ITest",
        "url": url,
        "canonical_url": url,
        "title": title,
        "summary_html": None,
        "text": body,
        "published_at": f"2026-11-0{n}T10:00:00+00:00",
        "discovered_at": "2026-11-02T10:00:00+00:00",
        "language": "vi",
        "content_hash": content_hash(body),
        "author": None,
        "word_count": len(body.split()),
        "discovered_via": "rss",
        "extracted_at": "2026-11-02T10:01:00+00:00",
    }


@pytest.fixture(scope="module")
def engine():
    eng = create_engine(DB_URL)
    migrate(eng, MIGRATIONS)
    PostgresState(eng).sync_sources(
        [
            Source(
                id=SOURCE,
                name="ITest",
                kind="rss",
                url="https://itest.example/rss",
                language="vi",
                interval_minutes=30,
                domains=["itest.example"],
            )
        ]
    )
    yield eng
    with eng.begin() as conn:
        conn.execute(text("DELETE FROM articles WHERE source_id = :s"), {"s": SOURCE})
        conn.execute(text("DELETE FROM sources WHERE source_id = :s"), {"s": SOURCE})
    eng.dispose()


def test_operational_upsert_dedup_and_accent_free_search(engine):
    handle = operational.make_handler(engine)
    a = message(1, "Đội tuyển Việt Nam đánh bại Thái Lan 2-1 tại Mỹ Đình. " * 5, "Việt Nam thắng")
    b = message(2, "Câu lạc bộ Hà Nội hòa Hải Phòng ở vòng 8 V-League. " * 5, "Hà Nội hòa")
    dup = {**message(3, a["text"], "Bản sao"), "content_hash": a["content_hash"]}
    for m in (a, b, a, dup):  # a twice: idempotent; dup: same text, other URL -> skipped
        handle(m)
    with engine.connect() as conn:
        count = conn.execute(
            text("SELECT count(*) FROM articles WHERE source_id = :s"), {"s": SOURCE}
        ).scalar()
    assert count == 2

    dedup = PostgresDedup(engine)
    assert dedup.article_exists(a["article_id"])
    assert dedup.claim_content(a["content_hash"], "other-id") == a["article_id"]
    assert dedup.claim_content(content_hash("brand new text"), "new-id") is None

    search = PostgresKeywordSearch(engine)
    hits = search.search(query_terms("doi tuyen viet nam thai lan"), 10)
    ids = [h.article_id for h in hits]
    assert a["article_id"] in ids and b["article_id"] not in ids
    hits = search.search(query_terms("Hà Nội V-League"), 10)
    assert hits and hits[0].article_id == b["article_id"]
    assert hits[0].url.startswith("https://itest.example/")
