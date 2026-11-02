"""PostgresState and the migration runner against a real PostgreSQL.

Skipped unless SPORTSBOT_INTEGRATION=1. Uses DATABASE_URL (the core stack's database in CI,
`make up-core` locally). Creates and removes its own source rows (prefix `itest_`).
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from sportsbot_pipelines.collectors.registry import Source
from sportsbot_pipelines.ingestion.migrate import migrate, pending
from sportsbot_pipelines.ingestion.state import PostgresState, RunRecord

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


@pytest.fixture(scope="module")
def engine():
    eng = create_engine(DB_URL)
    migrate(eng, MIGRATIONS)
    yield eng
    with eng.begin() as conn:
        for table in ("seen_items", "collection_runs", "collector_state"):
            conn.execute(text(f"DELETE FROM {table} WHERE source_id LIKE 'itest_%'"))
        conn.execute(text("DELETE FROM sources WHERE source_id LIKE 'itest_%'"))
    eng.dispose()


def test_all_migrations_applied(engine):
    assert pending(engine, MIGRATIONS) == []
    assert migrate(engine, MIGRATIONS) == []  # idempotent


def test_state_round_trip(engine):
    state = PostgresState(engine)
    src = Source(
        id="itest_src",
        name="Integration",
        kind="rss",
        url="https://example.com/rss",
        language="vi",
        interval_minutes=30,
        domains=["example.com"],
    )
    state.sync_sources([src])
    state.sync_sources([src])  # upsert, no error

    assert state.unseen("itest_src", ["a", "b"]) == {"a", "b"}
    state.mark_seen("itest_src", ["a"], "run-1")
    state.mark_seen("itest_src", ["a"], "run-2")  # duplicate ignored
    assert state.unseen("itest_src", ["a", "b"]) == {"b"}

    assert state.conditional_headers("itest_src") == (None, None)
    state.save_conditional_headers("itest_src", '"e1"', "Mon, 02 Nov 2026 00:00:00 GMT")
    assert state.conditional_headers("itest_src") == ('"e1"', "Mon, 02 Nov 2026 00:00:00 GMT")

    started = datetime(2026, 11, 2, 1, 0, tzinfo=UTC)
    state.save_run(
        RunRecord(
            run_key="itest-run-1",
            source_id="itest_src",
            started_at=started,
            finished_at=started,
            status="success",
            http_status=200,
            items_fetched=5,
            items_new=1,
            items_dropped=1,
            duration_ms=12,
        )
    )
    assert state.last_run_at("itest_src") == started
    state.set_robots("itest_src", True)
    with engine.connect() as conn:
        robots = conn.execute(
            text("SELECT robots_allowed FROM sources WHERE source_id = 'itest_src'")
        ).scalar()
        runs = conn.execute(
            text("SELECT count(*) FROM collection_runs WHERE source_id = 'itest_src'")
        ).scalar()
    assert robots is True and runs == 1
