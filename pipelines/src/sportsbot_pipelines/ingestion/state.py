"""Collector state: seen items, conditional-GET headers and run records.

`PostgresState` is used in the stack; `MemoryState` has the same behaviour for unit tests.
Order of operations in a run (see run.py): find unseen ids -> write Bronze -> mark them seen.
If the Bronze write fails, nothing is marked seen and the next run retries those items.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol

from sqlalchemy import Engine, text

from sportsbot_pipelines.collectors.registry import Source


@dataclass
class RunRecord:
    run_key: str
    source_id: str
    started_at: datetime
    finished_at: datetime | None = None
    status: str = "running"  # running | success | partial | failed | skipped
    http_status: int | None = None
    items_fetched: int = 0
    items_new: int = 0
    items_dropped: int = 0
    error: str | None = None
    bronze_path: str | None = None
    duration_ms: int | None = None


class State(Protocol):
    def sync_sources(self, sources: list[Source]) -> None: ...
    def conditional_headers(self, source_id: str) -> tuple[str | None, str | None]: ...
    def save_conditional_headers(
        self, source_id: str, etag: str | None, last_modified: str | None
    ) -> None: ...
    def unseen(self, source_id: str, record_ids: list[str]) -> set[str]: ...
    def mark_seen(self, source_id: str, record_ids: list[str], run_key: str) -> None: ...
    def save_run(self, run: RunRecord) -> None: ...
    def set_robots(self, source_id: str, allowed: bool) -> None: ...
    def last_run_at(self, source_id: str) -> datetime | None: ...
    def consecutive_failures(self, source_id: str) -> int: ...


@dataclass
class MemoryState:
    seen: dict[str, set[str]] = field(default_factory=dict)
    headers: dict[str, tuple[str | None, str | None]] = field(default_factory=dict)
    runs: list[RunRecord] = field(default_factory=list)
    robots: dict[str, bool] = field(default_factory=dict)
    sources: dict[str, Source] = field(default_factory=dict)

    def sync_sources(self, sources: list[Source]) -> None:
        self.sources = {s.id: s for s in sources}

    def conditional_headers(self, source_id: str) -> tuple[str | None, str | None]:
        return self.headers.get(source_id, (None, None))

    def save_conditional_headers(
        self, source_id: str, etag: str | None, last_modified: str | None
    ) -> None:
        self.headers[source_id] = (etag, last_modified)

    def unseen(self, source_id: str, record_ids: list[str]) -> set[str]:
        return set(record_ids) - self.seen.get(source_id, set())

    def mark_seen(self, source_id: str, record_ids: list[str], run_key: str) -> None:
        self.seen.setdefault(source_id, set()).update(record_ids)

    def save_run(self, run: RunRecord) -> None:
        self.runs.append(run)

    def set_robots(self, source_id: str, allowed: bool) -> None:
        self.robots[source_id] = allowed

    def last_run_at(self, source_id: str) -> datetime | None:
        times = [r.started_at for r in self.runs if r.source_id == source_id]
        return max(times) if times else None

    def consecutive_failures(self, source_id: str) -> int:
        count = 0
        for run in reversed([r for r in self.runs if r.source_id == source_id]):
            if run.status != "failed":
                break
            count += 1
        return count


class PostgresState:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def sync_sources(self, sources: list[Source]) -> None:
        with self.engine.begin() as conn:
            for s in sources:
                conn.execute(
                    text(
                        """
                        INSERT INTO sources (source_id, name, kind, url, language,
                                             interval_minutes, enabled, notes)
                        VALUES (:id, :name, :kind, :url, :language, :interval, :enabled, :notes)
                        ON CONFLICT (source_id) DO UPDATE SET
                            name = EXCLUDED.name, kind = EXCLUDED.kind, url = EXCLUDED.url,
                            language = EXCLUDED.language,
                            interval_minutes = EXCLUDED.interval_minutes,
                            enabled = EXCLUDED.enabled, notes = EXCLUDED.notes, updated_at = now()
                        """
                    ),
                    {
                        "id": s.id,
                        "name": s.name,
                        "kind": s.kind,
                        "url": str(s.url),
                        "language": s.language,
                        "interval": s.interval_minutes,
                        "enabled": s.enabled,
                        "notes": s.notes,
                    },
                )

    def conditional_headers(self, source_id: str) -> tuple[str | None, str | None]:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT etag, last_modified FROM collector_state WHERE source_id = :s"),
                {"s": source_id},
            ).first()
        return (row[0], row[1]) if row else (None, None)

    def save_conditional_headers(
        self, source_id: str, etag: str | None, last_modified: str | None
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO collector_state (source_id, etag, last_modified, last_success_at)
                    VALUES (:s, :e, :m, now())
                    ON CONFLICT (source_id) DO UPDATE SET etag = EXCLUDED.etag,
                        last_modified = EXCLUDED.last_modified, last_success_at = now()
                    """
                ),
                {"s": source_id, "e": etag, "m": last_modified},
            )

    def unseen(self, source_id: str, record_ids: list[str]) -> set[str]:
        if not record_ids:
            return set()
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT record_id FROM seen_items "
                    "WHERE source_id = :s AND record_id = ANY(:ids)"
                ),
                {"s": source_id, "ids": record_ids},
            )
            seen = {r[0] for r in rows}
        return set(record_ids) - seen

    def mark_seen(self, source_id: str, record_ids: list[str], run_key: str) -> None:
        if not record_ids:
            return
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO seen_items (source_id, record_id, run_id) "
                    "SELECT :s, unnest(CAST(:ids AS text[])), :r ON CONFLICT DO NOTHING"
                ),
                {"s": source_id, "ids": record_ids, "r": run_key},
            )

    def save_run(self, run: RunRecord) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO collection_runs (run_key, source_id, started_at, finished_at,
                        status, http_status, items_fetched, items_new, items_dropped, error,
                        bronze_path, duration_ms)
                    VALUES (:run_key, :source_id, :started_at, :finished_at, :status,
                        :http_status, :items_fetched, :items_new, :items_dropped, :error,
                        :bronze_path, :duration_ms)
                    """
                ),
                vars(run),
            )
            conn.execute(
                text(
                    """
                    INSERT INTO collector_state (source_id, last_run_at) VALUES (:s, :t)
                    ON CONFLICT (source_id) DO UPDATE SET last_run_at = EXCLUDED.last_run_at
                    """
                ),
                {"s": run.source_id, "t": run.started_at},
            )

    def set_robots(self, source_id: str, allowed: bool) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE sources SET robots_allowed = :a, updated_at = now() "
                    "WHERE source_id = :s"
                ),
                {"a": allowed, "s": source_id},
            )

    def last_run_at(self, source_id: str) -> datetime | None:
        with self.engine.connect() as conn:
            return conn.execute(
                text("SELECT last_run_at FROM collector_state WHERE source_id = :s"),
                {"s": source_id},
            ).scalar()

    def consecutive_failures(self, source_id: str) -> int:
        with self.engine.connect() as conn:
            statuses = conn.execute(
                text(
                    "SELECT status FROM collection_runs WHERE source_id = :s "
                    "ORDER BY started_at DESC LIMIT 20"
                ),
                {"s": source_id},
            ).scalars()
            count = 0
            for status in statuses:
                if status != "failed":
                    break
                count += 1
            return count
