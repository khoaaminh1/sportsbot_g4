"""Monitor loop: take a snapshot, evaluate rules, notify on new and resolved alerts.

python -m sportsbot_pipelines monitor          # forever (docker compose service `monitor`)
python -m sportsbot_pipelines monitor --once   # one check, print the snapshot and alerts
"""

from __future__ import annotations

import json
import logging
import signal
import threading
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Engine, text

from sportsbot_pipelines.monitoring.notify import Notifier
from sportsbot_pipelines.monitoring.rules import Alert, Snapshot, Thresholds, diff, evaluate

logger = logging.getLogger(__name__)

SOURCES_SQL = text(
    """
    SELECT s.source_id, s.enabled,
           (SELECT max(finished_at) FROM collection_runs r
             WHERE r.source_id = s.source_id AND r.status = 'success') AS last_success_at,
           (SELECT error FROM collection_runs r WHERE r.source_id = s.source_id
             ORDER BY started_at DESC LIMIT 1) AS last_error,
           (SELECT array_agg(status ORDER BY started_at DESC)
              FROM (SELECT status, started_at FROM collection_runs r
                     WHERE r.source_id = s.source_id ORDER BY started_at DESC LIMIT 20) x)
             AS recent
    FROM sources s ORDER BY s.source_id
    """
)


def _consecutive_failed(statuses: list[str] | None) -> int:
    count = 0
    for st in statuses or []:
        if st != "failed":
            break
        count += 1
    return count


def take_snapshot(
    engine: Engine,
    qdrant_count: Callable[[], int],
    dead_letters: Callable[[], list[dict[str, Any]]],
    now: datetime | None = None,
) -> Snapshot:
    with engine.connect() as conn:
        total, newest = conn.execute(
            text("SELECT count(*), max(ingested_at) FROM articles WHERE content IS NOT NULL")
        ).one()
        rows = conn.execute(SOURCES_SQL).mappings().all()
    try:
        chunks: int | None = qdrant_count()
    except Exception:
        chunks = None
    counts: Counter[str] = Counter()
    try:
        for d in dead_letters():
            payload = d.get("payload") or {}
            counts[f"{payload.get('source_id', '?')}:{d.get('reason', '?')}"] += 1
    except Exception as exc:  # Kafka down: reported by the other rules / ps
        logger.warning("dead_letter_read_failed", extra={"error": str(exc)[:200]})
    return Snapshot(
        taken_at=now or datetime.now(UTC),
        articles_total=int(total or 0),
        newest_ingested_at=newest,
        qdrant_chunks=chunks,
        sources=[
            {
                "source_id": r["source_id"],
                "enabled": r["enabled"],
                "last_success_at": r["last_success_at"],
                "consecutive_failures": _consecutive_failed(r["recent"]),
                "last_error": r["last_error"],
            }
            for r in rows
        ],
        dead_letters=dict(counts),
    )


def format_alert(a: Alert, resolved: bool = False) -> str:
    if resolved:
        return f"✅ SportsBot resolved: {a.message}"
    icon = "🔴" if a.severity == "critical" else "🟠"
    return f"{icon} SportsBot {a.severity}: {a.message}"


class Monitor:
    def __init__(
        self,
        snapshot: Callable[[], Snapshot],
        notifier: Notifier,
        thresholds: Thresholds | None = None,
    ) -> None:
        self.snapshot = snapshot
        self.notifier = notifier
        self.t = thresholds or Thresholds()
        self.previous: Snapshot | None = None
        self.stall_since: datetime | None = None
        self.open: dict[str, Alert] = {}

    def check(self) -> dict[str, Any]:
        snap = self.snapshot()
        active, self.stall_since = evaluate(snap, self.previous, self.stall_since, self.t)
        new, resolved = diff(active, self.open)
        for a in new:
            self.notifier.send(format_alert(a))
        for a in resolved:
            self.notifier.send(format_alert(a, resolved=True))
        self.open = {a.key: a for a in active}
        self.previous = snap
        summary = {
            "taken_at": snap.taken_at.isoformat(),
            "articles": snap.articles_total,
            "qdrant_chunks": snap.qdrant_chunks,
            "dead_letters_since_last_check": sum(snap.dead_letters.values()),
            "active_alerts": [a.key for a in active],
            "new": len(new),
            "resolved": len(resolved),
        }
        logger.info("monitor_check", extra=summary)
        return summary

    def run_forever(self, interval_seconds: float, stop: threading.Event | None = None) -> None:
        stop = stop or threading.Event()
        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGTERM, lambda *_: stop.set())
            signal.signal(signal.SIGINT, lambda *_: stop.set())
        channels = self.notifier.channels or ["log only"]
        self.notifier.send(
            f"ℹ️ SportsBot monitor started (every {interval_seconds / 60:.0f} min; "
            f"channels: {', '.join(channels)})."
        )
        while not stop.is_set():
            try:
                print(json.dumps(self.check(), ensure_ascii=False), flush=True)
            except Exception as exc:  # the monitor itself must not die
                logger.exception("monitor_check_failed")
                key = "monitor_error"
                if key not in self.open:
                    self.open[key] = Alert(key, "critical", f"Monitor check failed: {exc}")
                    self.notifier.send(format_alert(self.open[key]))
            stop.wait(interval_seconds)
