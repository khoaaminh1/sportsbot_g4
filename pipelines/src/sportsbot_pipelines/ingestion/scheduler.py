"""Long-running scheduler: run each source when its interval has passed.

A plain loop instead of APScheduler or cron: one process, one log stream, sources run one after
another (so per-host politeness holds), and SIGTERM from `docker compose down` stops it cleanly
between runs. Collection is periodic, so the system is "near real time", not streaming.
"""

from __future__ import annotations

import logging
import signal
import threading
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from sportsbot_pipelines.collectors.http import PoliteClient
from sportsbot_pipelines.collectors.registry import Source
from sportsbot_pipelines.ingestion.bronze import BronzeWriter
from sportsbot_pipelines.ingestion.run import Publisher, collect_source
from sportsbot_pipelines.ingestion.state import State

logger = logging.getLogger(__name__)


def due(source: Source, last_run: datetime | None, now: datetime) -> bool:
    return last_run is None or now - last_run >= timedelta(minutes=source.interval_minutes)


def run_forever(
    sources: list[Source],
    client: PoliteClient,
    state: State,
    writer: BronzeWriter,
    *,
    tick_seconds: float = 30.0,
    max_items: int = 200,
    publish: Publisher | None = None,
    stop: threading.Event | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> None:
    stop = stop or threading.Event()

    def _stop(signum: int, _frame: object) -> None:
        logger.info("scheduler_stopping", extra={"signal": signum})
        stop.set()

    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGTERM, _stop)
        signal.signal(signal.SIGINT, _stop)

    logger.info("scheduler_started", extra={"sources": [s.id for s in sources]})
    while not stop.is_set():
        for source in sources:
            if stop.is_set():
                break
            if due(source, state.last_run_at(source.id), now()):
                collect_source(source, client, state, writer, max_items=max_items, publish=publish)
        stop.wait(tick_seconds)
    logger.info("scheduler_stopped")
