"""Message bus: Kafka in the stack, an in-memory bus in tests.

Handlers are plain functions `handler(value: dict) -> None`. The Kafka loop commits an offset
only after the handler returns, so a crash re-delivers the message (at-least-once). Handlers are
written to be idempotent (upserts keyed by article id), so a re-delivery does no harm.
A handler that raises is not retried forever: the message goes to the dead-letter topic.
"""

from __future__ import annotations

import json
import logging
import signal
import threading
import traceback
from collections import defaultdict
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Protocol

from sportsbot_pipelines.streaming import topics

logger = logging.getLogger(__name__)

Handler = Callable[[dict[str, Any]], None]


class TransientError(Exception):
    """A dependency is down (database, embedder, Qdrant): retry the same message later.

    Raised by handlers instead of failing the message, so an outage does not send the whole
    stream to the dead-letter topic. The Kafka loop waits and retries without committing.
    """


class Bus(Protocol):
    def publish(self, topic: str, key: str, value: dict[str, Any]) -> None: ...

    def flush(self) -> None: ...


def dead_letter(
    stage: str, reason: str, payload: dict[str, Any], error: str | None = None
) -> dict[str, Any]:
    return {
        "schema": topics.DEAD_LETTER_SCHEMA,
        "stage": stage,
        "reason": reason,
        "error": (error or "")[:1000],
        "failed_at": datetime.now(UTC).isoformat(),
        "payload": payload,
    }


class MemoryBus:
    """Keeps published messages per topic; `drain` feeds them to a handler (tests, dry runs)."""

    def __init__(self) -> None:
        self.messages: dict[str, list[tuple[str, dict[str, Any]]]] = defaultdict(list)
        self._offsets: dict[tuple[str, str], int] = defaultdict(int)

    def publish(self, topic: str, key: str, value: dict[str, Any]) -> None:
        # Round-trip through JSON so tests catch values Kafka could not carry.
        self.messages[topic].append((key, json.loads(json.dumps(value))))

    def flush(self) -> None:
        return None

    def values(self, topic: str) -> list[dict[str, Any]]:
        return [v for _, v in self.messages[topic]]

    def drain(self, topic: str, group: str, handler: Handler, *, stage: str) -> int:
        """Deliver unread messages of `topic` to `handler` for consumer group `group`."""
        handled = 0
        while self._offsets[(topic, group)] < len(self.messages[topic]):
            _, value = self.messages[topic][self._offsets[(topic, group)]]
            _safe_handle(self, handler, value, stage=stage)
            self._offsets[(topic, group)] += 1
            handled += 1
        return handled


def _safe_handle(bus: Bus, handler: Handler, value: dict[str, Any], *, stage: str) -> bool:
    try:
        handler(value)
        return True
    except TransientError:
        raise
    except Exception as exc:  # one bad message must not stop the consumer
        logger.exception("handler_failed", extra={"stage": stage})
        key = str(value.get("source_id") or "unknown")
        bus.publish(
            topics.DEAD_LETTER,
            key,
            dead_letter(stage, "handler_exception", value, traceback.format_exc(limit=3)),
        )
        logger.debug("dead_lettered", extra={"error": str(exc)})
        return False


class KafkaBus:
    """confluent-kafka producer. Imported lazily so unit tests do not need librdkafka."""

    def __init__(self, bootstrap_servers: str, client_id: str = "sportsbot") -> None:
        from confluent_kafka import Producer

        self.bootstrap_servers = bootstrap_servers
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": client_id,
                "enable.idempotence": True,
                "compression.type": "zstd",
                "linger.ms": 50,
            }
        )

    @staticmethod
    def _on_delivery(err: Any, msg: Any) -> None:
        if err is not None:  # e.g. broker down longer than the producer's retry window
            logger.error("kafka_delivery_failed", extra={"topic": msg.topic(), "error": str(err)})

    def publish(self, topic: str, key: str, value: dict[str, Any]) -> None:
        data = json.dumps(value, ensure_ascii=False).encode("utf-8")
        while True:
            try:
                self._producer.produce(
                    topic, key=key.encode("utf-8"), value=data, on_delivery=self._on_delivery
                )
                break
            except BufferError:  # local queue full: let it drain, then retry
                self._producer.poll(0.5)
        self._producer.poll(0)

    def flush(self) -> None:
        remaining = self._producer.flush(30)
        if remaining:
            logger.error("kafka_flush_incomplete", extra={"pending_messages": remaining})


def run_consumer(
    bootstrap_servers: str,
    group_id: str,
    topic: str,
    handler: Handler,
    *,
    stage: str,
    bus: Bus,
    stop: threading.Event | None = None,
    batch_log_every: int = 100,
) -> None:
    """Consume `topic` forever (until SIGTERM/SIGINT), committing after each handled message."""
    from confluent_kafka import Consumer, KafkaError

    stop = stop or threading.Event()
    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
        signal.signal(signal.SIGINT, lambda *_: stop.set())

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 900_000,  # article fetches are slow and polite
        }
    )
    consumer.subscribe([topic])
    logger.info("consumer_started", extra={"group": group_id, "topic": topic})
    handled = failed = 0
    try:
        while not stop.is_set():
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.warning("consumer_error", extra={"error": str(msg.error())})
                continue
            try:
                value = json.loads(msg.value())
            except (ValueError, TypeError):
                bus.publish(
                    topics.DEAD_LETTER,
                    "unknown",
                    dead_letter(stage, "invalid_json", {"raw": repr(msg.value())[:2000]}),
                )
                consumer.commit(message=msg, asynchronous=False)
                continue
            ok = None
            wait = 2.0
            while ok is None and not stop.is_set():
                try:
                    ok = _safe_handle(bus, handler, value, stage=stage)
                except TransientError as exc:
                    logger.warning(
                        "dependency_unavailable_retrying",
                        extra={"stage": stage, "error": str(exc)[:200], "retry_in": wait},
                    )
                    stop.wait(wait)
                    wait = min(wait * 2, 60.0)
            if ok is None:  # stopping during an outage: leave the offset uncommitted
                break
            handled += ok
            failed += not ok
            consumer.commit(message=msg, asynchronous=False)
            if (handled + failed) % batch_log_every == 0:
                bus.flush()
                logger.info(
                    "consumer_progress",
                    extra={"stage": stage, "handled": handled, "failed": failed},
                )
    finally:
        bus.flush()
        consumer.close()
        logger.info(
            "consumer_stopped", extra={"stage": stage, "handled": handled, "failed": failed}
        )
