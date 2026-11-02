from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx

from sportsbot_pipelines.collectors.http import PoliteClient
from sportsbot_pipelines.ingestion.bronze import LocalBronzeWriter
from sportsbot_pipelines.ingestion.run import collect_source
from sportsbot_pipelines.ingestion.scheduler import due
from sportsbot_pipelines.ingestion.state import MemoryState


def client_for(site) -> PoliteClient:
    return PoliteClient(
        user_agent="TestBot/1.0",
        min_interval=0,
        sleep=lambda s: None,
        transport=httpx.MockTransport(site),
    )


class FailingWriter:
    def write(self, relpath: str, data: bytes) -> str:
        raise OSError("disk full")


def test_first_run_writes_new_items_and_rerun_adds_zero(tmp_path, site, source):
    state, writer = MemoryState(), LocalBronzeWriter(tmp_path)
    with client_for(site) as client:
        first = collect_source(source, client, state, writer)
        # Second run: the feed answers 304 thanks to the stored ETag.
        second = collect_source(source, client, state, writer)
        # Third run without the ETag: full feed, but every item was already seen.
        state.headers.clear()
        third = collect_source(source, client, state, writer)

    assert first.status == "success" and first.items_new == 3 and first.items_dropped == 4
    assert first.bronze_path and list(tmp_path.rglob("*.jsonl.gz"))
    assert second.status == "success" and second.http_status == 304 and second.items_new == 0
    assert third.status == "success" and third.items_new == 0 and third.bronze_path is None
    assert len(list(tmp_path.rglob("*.jsonl.gz"))) == 1
    assert [r.status for r in state.runs] == ["success"] * 3
    assert state.robots[source.id] is True


def test_failed_bronze_write_marks_nothing_seen(site, source):
    state = MemoryState()
    with client_for(site) as client:
        run = collect_source(source, client, state, FailingWriter())
    assert run.status == "failed" and "disk full" in run.error
    assert state.seen == {}
    assert state.headers == {}  # ETag not saved, so the next run refetches everything


def test_robots_disallow_is_recorded_as_skipped(site, source, tmp_path):
    site.robots = "User-agent: *\nDisallow: /\n"
    state = MemoryState()
    with client_for(site) as client:
        run = collect_source(source, client, state, LocalBronzeWriter(tmp_path))
    assert run.status == "skipped"
    assert state.robots[source.id] is False
    assert site.feed_requests() == []


def test_http_failure_is_recorded(site, source, tmp_path):
    site.feed_status = [500, 500, 500]
    state = MemoryState()
    with client_for(site) as client:
        run = collect_source(source, client, state, LocalBronzeWriter(tmp_path))
    assert run.status == "failed" and run.http_status == 500
    assert run.finished_at is not None and run.duration_ms is not None


def test_scheduler_due(source):
    now = datetime(2026, 11, 2, tzinfo=UTC)
    assert due(source, None, now)
    assert not due(source, now - timedelta(minutes=10), now)
    assert due(source, now - timedelta(minutes=30), now)


def test_publish_receives_only_new_records(tmp_path, site, source):
    state, published = MemoryState(), []
    with client_for(site) as client:
        collect_source(source, client, state, LocalBronzeWriter(tmp_path), publish=published.extend)
        state.headers.clear()
        collect_source(source, client, state, LocalBronzeWriter(tmp_path), publish=published.extend)
    assert len(published) == 3 and len({r.record_id for r in published}) == 3


def test_failed_publish_marks_nothing_seen(tmp_path, site, source):
    state = MemoryState()

    def fail(records):
        raise RuntimeError("kafka down")

    with client_for(site) as client:
        run = collect_source(source, client, state, LocalBronzeWriter(tmp_path), publish=fail)
    assert run.status == "failed" and not state.seen
