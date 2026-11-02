"""PostgresNewsRepository against real PostgreSQL (migrations applied). SPORTSBOT_INTEGRATION=1."""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine, text

from app.services.news import PostgresNewsRepository

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("SPORTSBOT_INTEGRATION") != "1",
        reason="set SPORTSBOT_INTEGRATION=1 with PostgreSQL running",
    ),
]

DB_URL = os.environ.get(
    "DATABASE_URL", "postgresql+psycopg://sportsbot:change-me-local-only@localhost:5432/sportsbot"
)
SOURCE = "itest_news"


@pytest.fixture(scope="module")
def engine():
    eng = create_engine(DB_URL)
    with eng.connect() as conn:
        if conn.execute(text("SELECT to_regclass('public.articles')")).scalar() is None:
            pytest.skip("articles table missing: run the migrations first")
    base = datetime(2026, 11, 1, tzinfo=UTC)
    with eng.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO sources (source_id, name, kind, url) "
                "VALUES (:s, 'ITest', 'rss', 'https://itest.example/rss') ON CONFLICT DO NOTHING"
            ),
            {"s": SOURCE},
        )
        for i in range(5):
            conn.execute(
                text(
                    "INSERT INTO articles (article_id, title, content, summary, source_id, "
                    "source_name, source_url, published_at, collected_at, language, content_hash) "
                    "VALUES (:id, :t, 'body', :sm, :s, 'ITest', :u, :p, :p, 'vi', :h)"
                ),
                {
                    "id": f"{i:x}" * 64,
                    "t": f"Tin {i}",
                    "sm": "x" * 600,
                    "s": SOURCE,
                    "u": f"https://itest.example/{i}",
                    "p": base + timedelta(hours=i),
                    "h": f"{i:x}" * 64,
                },
            )
    yield eng
    with eng.begin() as conn:
        conn.execute(text("DELETE FROM articles WHERE source_id = :s"), {"s": SOURCE})
        conn.execute(text("DELETE FROM sources WHERE source_id = :s"), {"s": SOURCE})
    eng.dispose()


def test_latest_pages_with_cursor_and_limits_excerpt(engine):
    repo = PostgresNewsRepository(engine)
    titles: list[str] = []
    cursor = None
    while True:
        page = repo.latest(limit=2, sport=None, language="vi", cursor=cursor)
        titles += [i["title"] for i in page["items"] if i["source_url"].startswith("https://itest")]
        assert all(len(i["excerpt"]) <= 400 for i in page["items"])
        cursor = page["next_cursor"]
        if cursor is None:
            break
    assert titles == ["Tin 4", "Tin 3", "Tin 2", "Tin 1", "Tin 0"]
    assert repo.get("4" * 64)["title"] == "Tin 4"
    assert repo.get("f" * 64) is None
    assert any(r["source_id"] == SOURCE for r in repo.sources())
    assert len(repo.since(None, 3)) == 3


def test_stats_and_runs_for_the_live_panel(engine):
    repo = PostgresNewsRepository(engine)
    stats = repo.stats()
    assert stats["articles"] >= 5 and stats["languages"] >= 1
    assert {"last_hour", "last_24h", "sources_enabled", "sources_active"} <= stats.keys()
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO collection_runs (source_id, status, items_fetched, items_new, "
                "finished_at) VALUES (:s, 'success', 10, 3, now())"
            ),
            {"s": SOURCE},
        )
    runs = repo.runs_since(None, 5)
    mine = [r for r in runs if r["source_id"] == SOURCE]
    assert mine and mine[-1]["items_new"] == 3 and mine[-1]["source_name"] == "ITest"
    assert repo.runs_since(mine[-1]["run_id"], 5) == []
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM collection_runs WHERE source_id = :s"), {"s": SOURCE})
