"""One collection run for one source: fetch -> parse -> keep unseen -> write Bronze -> record.

Every run, successful or not, becomes a row in `collection_runs`: these rows are the failure
statistics and freshness evidence the brief asks for (section 3.1, items 9 and 12).
"""

from __future__ import annotations

import logging
import secrets
import time
from collections.abc import Callable
from datetime import UTC, datetime

from sportsbot_pipelines.collectors.http import FetchError, PoliteClient, RobotsDisallowed
from sportsbot_pipelines.collectors.registry import Source
from sportsbot_pipelines.collectors.rss import parse_feed
from sportsbot_pipelines.ingestion.bronze import BronzeWriter, bronze_relpath, encode
from sportsbot_pipelines.ingestion.state import RunRecord, State

logger = logging.getLogger(__name__)


def new_run_key(now: datetime) -> str:
    return f"{now.astimezone(UTC):%Y%m%dT%H%M%SZ}-{secrets.token_hex(3)}"


def collect_source(
    source: Source,
    client: PoliteClient,
    state: State,
    writer: BronzeWriter,
    *,
    max_items: int = 200,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> RunRecord:
    started = now()
    t0 = time.perf_counter()
    run = RunRecord(run_key=new_run_key(started), source_id=source.id, started_at=started)
    try:
        etag, last_modified = state.conditional_headers(source.id)
        try:
            fetched = client.fetch(str(source.url), etag=etag, last_modified=last_modified)
        except RobotsDisallowed as exc:
            state.set_robots(source.id, False)
            run.status, run.error = "skipped", str(exc)
            return run
        state.set_robots(source.id, True)
        run.http_status = fetched.status
        if fetched.not_modified:
            run.status = "success"
            return run

        parsed = parse_feed(
            fetched.content,
            source,
            collected_at=started,
            run_id=run.run_key,
            http_status=fetched.status,
            max_items=max_items,
        )
        if parsed.feed_error:
            run.status, run.error = "failed", parsed.feed_error
            return run
        run.items_fetched = parsed.entries_total
        run.items_dropped = (
            parsed.dropped_no_link
            + parsed.dropped_foreign_domain
            + parsed.dropped_no_title
            + parsed.dropped_duplicate
        )
        ids = [r.record_id for r in parsed.records]
        fresh = state.unseen(source.id, ids)
        new_records = [r for r in parsed.records if r.record_id in fresh]
        if new_records:
            run.bronze_path = writer.write(
                bronze_relpath(source.id, started, run.run_key), encode(new_records)
            )
            state.mark_seen(source.id, [r.record_id for r in new_records], run.run_key)
        run.items_new = len(new_records)
        state.save_conditional_headers(source.id, fetched.etag, fetched.last_modified)
        run.status = "success"
        return run
    except FetchError as exc:
        run.status, run.error, run.http_status = "failed", str(exc)[:500], exc.status
        return run
    except Exception as exc:  # record any unexpected failure; one bad source must not stop others
        logger.exception("collect_unexpected_error", extra={"source_id": source.id})
        run.status, run.error = "failed", f"{type(exc).__name__}: {exc}"[:500]
        return run
    finally:
        run.finished_at = now()
        run.duration_ms = int((time.perf_counter() - t0) * 1000)
        state.save_run(run)
        logger.info(
            "collection_run",
            extra={
                "source_id": source.id,
                "run_key": run.run_key,
                "status": run.status,
                "http_status": run.http_status,
                "items_fetched": run.items_fetched,
                "items_new": run.items_new,
                "items_dropped": run.items_dropped,
                "duration_ms": run.duration_ms,
                "error": run.error,
            },
        )


def collect_all(
    sources: list[Source],
    client: PoliteClient,
    state: State,
    writer: BronzeWriter,
    *,
    max_items: int = 200,
) -> list[RunRecord]:
    return [collect_source(s, client, state, writer, max_items=max_items) for s in sources]
