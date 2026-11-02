"""Alert rules over a pipeline snapshot. Pure functions: easy to test and to explain.

A snapshot is collected every MONITOR_INTERVAL_SECONDS (see monitor.py). Each rule returns
alerts keyed by a stable id; the notifier sends an alert when its key first appears and a
"resolved" message when it disappears, so a long outage gives two messages, not hundreds.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any


@dataclass(frozen=True)
class Thresholds:
    source_stale_hours: float = 3.0  # an enabled source without a successful run for this long
    source_failures: int = 3  # consecutive failed runs of one source
    no_new_articles_hours: float = 2.0  # no article stored in PostgreSQL at all
    embedding_stall_minutes: float = 30.0  # articles grow, vectors do not
    dead_letters_per_check: int = 50  # dead letters since the previous check


@dataclass(frozen=True)
class Alert:
    key: str
    severity: str  # "warning" | "critical"
    message: str


@dataclass
class Snapshot:
    taken_at: datetime
    articles_total: int
    newest_ingested_at: datetime | None
    qdrant_chunks: int | None  # None: Qdrant unreachable
    sources: list[dict[str, Any]] = field(default_factory=list)
    # {"source_id", "enabled", "last_success_at", "consecutive_failures", "last_error"}
    dead_letters: dict[str, int] = field(default_factory=dict)  # "source_id:reason" -> count


def evaluate(
    snap: Snapshot, previous: Snapshot | None, stall_since: datetime | None, t: Thresholds
) -> tuple[list[Alert], datetime | None]:
    """Return the active alerts and the updated 'vectors stalled since' marker."""
    alerts: list[Alert] = []
    now = snap.taken_at

    for s in snap.sources:
        if not s.get("enabled", True):
            continue
        sid = s["source_id"]
        fails = int(s.get("consecutive_failures") or 0)
        if fails >= t.source_failures:
            alerts.append(
                Alert(
                    f"source_failing:{sid}",
                    "warning",
                    f"Source {sid}: {fails} failed runs in a row "
                    f"(last error: {(s.get('last_error') or '?')[:120]}). "
                    "The scheduler backs off; other sources keep running.",
                )
            )
        last_ok = s.get("last_success_at")
        if last_ok is not None and now - last_ok > timedelta(hours=t.source_stale_hours):
            hours = (now - last_ok).total_seconds() / 3600
            alerts.append(
                Alert(
                    f"source_stale:{sid}",
                    "warning",
                    f"Source {sid}: no successful collection for {hours:.1f} h.",
                )
            )

    newest = snap.newest_ingested_at
    if newest is not None and now - newest > timedelta(hours=t.no_new_articles_hours):
        alerts.append(
            Alert(
                "no_new_articles",
                "critical",
                f"No new article stored for {(now - newest).total_seconds() / 3600:.1f} h. "
                "Check: make ps, make logs-stream.",
            )
        )

    if snap.qdrant_chunks is None:
        alerts.append(Alert("qdrant_down", "critical", "Qdrant is unreachable."))
        stall_since = None
    elif previous is not None and previous.qdrant_chunks is not None:
        articles_grew = snap.articles_total > previous.articles_total
        vectors_flat = snap.qdrant_chunks <= previous.qdrant_chunks
        if articles_grew and vectors_flat:
            stall_since = stall_since or previous.taken_at
        elif not vectors_flat:
            stall_since = None
        if stall_since and now - stall_since >= timedelta(minutes=t.embedding_stall_minutes):
            alerts.append(
                Alert(
                    "embedding_stalled",
                    "critical",
                    f"New articles keep arriving but Qdrant has not grown since "
                    f"{stall_since:%H:%M} UTC ({snap.qdrant_chunks} chunks). Check the embedder "
                    "(make up-ai) and consumer-semantic logs; then make reindex.",
                )
            )

    total_dead = sum(snap.dead_letters.values())
    if total_dead >= t.dead_letters_per_check:
        top = sorted(snap.dead_letters.items(), key=lambda kv: kv[1], reverse=True)[:5]
        detail = ", ".join(f"{k} x{n}" for k, n in top)
        alerts.append(
            Alert(
                "dead_letters_spike",
                "warning",
                f"{total_dead} articles rejected since the last check. Top: {detail}. "
                "Retryable ones: make replay-dead-letters.",
            )
        )
    return alerts, stall_since


def diff(active: list[Alert], open_keys: dict[str, Alert]) -> tuple[list[Alert], list[Alert]]:
    """(new alerts to send, alerts that are now resolved)."""
    now_keys = {a.key: a for a in active}
    new = [a for k, a in now_keys.items() if k not in open_keys]
    resolved = [a for k, a in open_keys.items() if k not in now_keys]
    return new, resolved
