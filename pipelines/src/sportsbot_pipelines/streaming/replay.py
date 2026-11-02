"""Send retryable dead letters of the extract stage back to `articles.discovered`.

    make replay-dead-letters               # retryable ones (network errors, 429/5xx)
    make replay-dead-letters REASONS=too_short,no_main_text   # chosen reasons

Uses its own consumer group, so each dead letter is replayed once. Messages that fail again
come back to the dead-letter topic as new messages and can be replayed later.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from typing import Any

from sportsbot_pipelines.streaming import topics
from sportsbot_pipelines.streaming.bus import Bus

logger = logging.getLogger(__name__)

REPLAY_GROUP = "sportsbot-replay"
# Before 2026-11-03, network failures were recorded as reason "http_error" without a flag.
LEGACY_RETRYABLE_REASONS = {"http_error"}


def should_replay(dead: dict[str, Any], reasons: set[str] | None = None) -> bool:
    if dead.get("stage") != "extract" or not isinstance(dead.get("payload"), dict):
        return False
    if reasons:
        return dead.get("reason") in reasons
    return bool(dead.get("retryable")) or dead.get("reason") in LEGACY_RETRYABLE_REASONS


def replay(
    dead_letters: Iterable[dict[str, Any]], bus: Bus, reasons: set[str] | None = None
) -> dict[str, int]:
    seen = replayed = 0
    for dead in dead_letters:
        seen += 1
        if should_replay(dead, reasons):
            payload = dead["payload"]
            bus.publish(topics.DISCOVERED, str(payload.get("source_id", "unknown")), payload)
            replayed += 1
    bus.flush()
    return {"dead_letters_read": seen, "replayed": replayed}


def read_dead_letters(
    bootstrap_servers: str, idle_seconds: float = 10.0, group_id: str = REPLAY_GROUP
):
    """Yield dead letters not yet read by `group_id`; stop after `idle_seconds` idle."""
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([topics.DEAD_LETTER])
    try:
        while True:
            msg = consumer.poll(idle_seconds)
            if msg is None:
                return
            if msg.error():
                continue
            try:
                yield json.loads(msg.value())
            except (ValueError, TypeError):
                continue
    finally:
        consumer.close()
