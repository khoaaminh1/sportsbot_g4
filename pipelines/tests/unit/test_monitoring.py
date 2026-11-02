"""Monitoring rules, alert de-duplication, notifier and the scheduler's circuit breaker."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import httpx

from sportsbot_pipelines.collectors.registry import Source
from sportsbot_pipelines.ingestion.scheduler import MAX_BACKOFF, due, wait_for
from sportsbot_pipelines.ingestion.state import MemoryState, RunRecord
from sportsbot_pipelines.monitoring.monitor import Monitor, _consecutive_failed
from sportsbot_pipelines.monitoring.notify import Notifier
from sportsbot_pipelines.monitoring.rules import Snapshot, Thresholds, diff, evaluate

T0 = datetime(2026, 11, 3, 0, 0, tzinfo=UTC)


def snap(minutes: int, articles: int, chunks: int | None, **kw) -> Snapshot:
    return Snapshot(
        taken_at=T0 + timedelta(minutes=minutes),
        articles_total=articles,
        newest_ingested_at=kw.pop("newest", T0 + timedelta(minutes=minutes)),
        qdrant_chunks=chunks,
        **kw,
    )


def keys(alerts) -> set[str]:
    return {a.key for a in alerts}


def test_healthy_snapshot_has_no_alerts():
    alerts, stall = evaluate(snap(15, 120, 400), snap(0, 100, 300), None, Thresholds())
    assert alerts == [] and stall is None


def test_embedding_stall_alerts_after_30_minutes_and_clears():
    t = Thresholds()
    a1, stall = evaluate(snap(15, 120, 17), snap(0, 100, 17), None, t)
    assert keys(a1) == set() and stall == T0  # stalled, but not long enough yet
    a2, stall = evaluate(snap(30, 140, 17), snap(15, 120, 17), stall, t)
    assert keys(a2) == {"embedding_stalled"}
    a3, stall = evaluate(snap(45, 150, 60), snap(30, 140, 17), stall, t)
    assert keys(a3) == set() and stall is None


def test_source_failing_stale_no_articles_qdrant_down_and_dead_letters():
    sources = [
        {
            "source_id": "bbc",
            "enabled": True,
            "consecutive_failures": 4,
            "last_success_at": T0 - timedelta(hours=5),
            "last_error": "HTTP 403",
        },
        {"source_id": "off", "enabled": False, "consecutive_failures": 9, "last_success_at": None},
    ]
    s = snap(
        0,
        100,
        None,
        newest=T0 - timedelta(hours=3),
        sources=sources,
        dead_letters={"bbc:network_error": 40, "x:too_short": 15},
    )
    alerts, _ = evaluate(s, None, None, Thresholds())
    assert keys(alerts) == {
        "source_failing:bbc",
        "source_stale:bbc",
        "no_new_articles",
        "qdrant_down",
        "dead_letters_spike",
    }
    spike = next(a for a in alerts if a.key == "dead_letters_spike")
    assert "bbc:network_error x40" in spike.message


def test_diff_sends_each_alert_once_and_resolves():
    first, _ = evaluate(snap(0, 1, None), None, None, Thresholds())
    new, resolved = diff(first, {})
    assert keys(new) == {"qdrant_down"} and resolved == []
    new, resolved = diff(first, {a.key: a for a in first})
    assert new == [] and resolved == []
    new, resolved = diff([], {a.key: a for a in first})
    assert keys(resolved) == {"qdrant_down"}


def test_monitor_notifies_new_and_resolved_once():
    sent: list[str] = []

    class FakeNotifier:
        channels = ["test"]

        def send(self, text: str) -> list[str]:
            sent.append(text)
            return ["test"]

    snaps = iter([snap(0, 1, None), snap(15, 2, None), snap(30, 3, 10)])
    monitor = Monitor(lambda: next(snaps), FakeNotifier())  # type: ignore[arg-type]
    monitor.check()
    monitor.check()
    summary = monitor.check()
    assert len(sent) == 2
    assert "critical" in sent[0] and "Qdrant is unreachable" in sent[0]
    assert sent[1].startswith("✅") and summary["active_alerts"] == []


def test_notifier_posts_to_discord_and_telegram():
    calls: list[tuple[str, dict]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((str(request.url), json.loads(request.content)))
        return httpx.Response(204 if "discord" in str(request.url) else 200)

    n = Notifier(
        "https://discord.test/api/webhooks/1/x",
        "123:abc",
        "-100",
        transport=httpx.MockTransport(handler),
    )
    assert n.channels == ["discord", "telegram"]
    assert n.send("hello") == ["discord", "telegram"]
    assert calls[0][1] == {"content": "hello"}
    assert calls[1][0].endswith("/bot123:abc/sendMessage") and calls[1][1]["chat_id"] == "-100"
    assert Notifier().channels == [] and Notifier().send("x") == []


def test_consecutive_failed_counts_from_newest():
    assert _consecutive_failed(["failed", "failed", "success", "failed"]) == 2
    assert _consecutive_failed(None) == 0


def test_circuit_breaker_backs_off_and_resets(source: Source):
    base = timedelta(minutes=source.interval_minutes)
    assert wait_for(source, 0) == base and wait_for(source, 2) == base
    assert wait_for(source, 3) == base * 2 and wait_for(source, 4) == base * 4
    assert wait_for(source, 30) == MAX_BACKOFF
    last = T0
    assert not due(source, last, T0 + base, failures=3)
    assert due(source, last, T0 + base * 2, failures=3)

    state = MemoryState()
    for status in ("success", "failed", "failed", "failed"):
        state.save_run(RunRecord(run_key=status, source_id=source.id, started_at=T0, status=status))
    assert state.consecutive_failures(source.id) == 3
    state.save_run(RunRecord(run_key="ok", source_id=source.id, started_at=T0, status="success"))
    assert state.consecutive_failures(source.id) == 0
