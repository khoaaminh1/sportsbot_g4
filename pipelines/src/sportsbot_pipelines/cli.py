"""Command line: `python -m sportsbot_pipelines <command>`.

migrate            apply pending SQL migrations
sources validate   check sources.yaml (no network)
sources check      fetch robots.txt + each feed once and print a table (no writes)
collect            one collection pass over enabled sources (or --source ID)
schedule           run forever, each source on its own interval
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime

from sqlalchemy import create_engine

from sportsbot_pipelines.collectors.http import FetchError, PoliteClient, RobotsDisallowed
from sportsbot_pipelines.collectors.registry import load_registry
from sportsbot_pipelines.collectors.rss import parse_feed
from sportsbot_pipelines.config import PipelineSettings, get_settings
from sportsbot_pipelines.ingestion.bronze import LocalBronzeWriter, WebHdfsBronzeWriter
from sportsbot_pipelines.ingestion.migrate import migrate
from sportsbot_pipelines.ingestion.run import collect_all
from sportsbot_pipelines.ingestion.scheduler import run_forever
from sportsbot_pipelines.ingestion.state import PostgresState
from sportsbot_pipelines.log import configure_logging


def _client(settings: PipelineSettings) -> PoliteClient:
    return PoliteClient(
        user_agent=settings.user_agent,
        timeout=settings.http_timeout_seconds,
        max_bytes=settings.http_max_bytes,
        retries=settings.http_retries,
        min_interval=settings.min_seconds_between_requests_per_host,
    )


def _writer(settings: PipelineSettings):
    if settings.bronze_backend == "hdfs":
        if not settings.hdfs_webhdfs_url:
            sys.exit("BRONZE_BACKEND=hdfs needs HDFS_WEBHDFS_URL")
        return WebHdfsBronzeWriter(
            settings.hdfs_webhdfs_url, settings.bronze_root, settings.hdfs_user
        )
    return LocalBronzeWriter(settings.local_bronze_dir)


def cmd_migrate(settings: PipelineSettings) -> int:
    applied = migrate(create_engine(settings.database_url), settings.migrations_dir)
    print(json.dumps({"applied": applied}))
    return 0


def cmd_sources_validate(settings: PipelineSettings) -> int:
    registry = load_registry(settings.sources_file)
    langs = sorted({s.language for s in registry.enabled()})
    print(
        json.dumps(
            {
                "sources": len(registry.sources),
                "enabled": len(registry.enabled()),
                "languages": langs,
            }
        )
    )
    return 0


def cmd_sources_check(settings: PipelineSettings) -> int:
    registry = load_registry(settings.sources_file)
    now = datetime.now(UTC)
    rows = []
    with _client(settings) as client:
        for s in registry.enabled():
            row = {"source": s.id, "language": s.language}
            try:
                res = client.fetch(str(s.url))
                parsed = parse_feed(res.content, s, collected_at=now, run_id="check")
                row.update(
                    status="ok" if parsed.records else "empty",
                    http=res.status,
                    entries=parsed.entries_total,
                    usable=len(parsed.records),
                    with_date=sum(1 for r in parsed.records if r.published_at),
                    tz_assumed=sum(1 for r in parsed.records if r.published_tz_assumed),
                    date_sample=next(
                        (r.published_raw for r in parsed.records if r.published_raw), None
                    ),
                    error=parsed.feed_error,
                )
            except RobotsDisallowed as exc:
                row.update(status="robots_disallowed", error=str(exc))
            except FetchError as exc:
                row.update(status="error", http=exc.status, error=str(exc))
            rows.append(row)
            print(json.dumps(row, ensure_ascii=False), flush=True)
    ok = sum(1 for r in rows if r["status"] == "ok")
    print(json.dumps({"summary": {"checked": len(rows), "ok": ok, "checked_at": now.isoformat()}}))
    return 0


def _setup(settings: PipelineSettings, source_id: str | None):
    registry = load_registry(settings.sources_file)
    sources = [registry.get(source_id)] if source_id else registry.enabled()
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    migrate(engine, settings.migrations_dir)
    state = PostgresState(engine)
    state.sync_sources(registry.sources)
    return sources, state


def cmd_collect(settings: PipelineSettings, source_id: str | None) -> int:
    sources, state = _setup(settings, source_id)
    with _client(settings) as client:
        runs = collect_all(
            sources, client, state, _writer(settings), max_items=settings.max_items_per_fetch
        )
    summary = {
        "runs": len(runs),
        "success": sum(r.status == "success" for r in runs),
        "failed": sum(r.status == "failed" for r in runs),
        "skipped": sum(r.status == "skipped" for r in runs),
        "items_new": sum(r.items_new for r in runs),
    }
    print(json.dumps({"summary": summary}))
    return 0 if summary["success"] > 0 or not runs else 1


def cmd_schedule(settings: PipelineSettings) -> int:
    sources, state = _setup(settings, None)
    with _client(settings) as client:
        run_forever(
            sources,
            client,
            state,
            _writer(settings),
            tick_seconds=settings.scheduler_tick_seconds,
            max_items=settings.max_items_per_fetch,
        )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="sportsbot_pipelines")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("migrate")
    src = sub.add_parser("sources")
    src.add_argument("action", choices=["validate", "check"])
    col = sub.add_parser("collect")
    col.add_argument("--source", help="collect only this source id")
    sub.add_parser("schedule")
    args = parser.parse_args(argv)

    settings = get_settings()
    configure_logging(settings.log_level, settings.log_format)
    if args.command == "migrate":
        return cmd_migrate(settings)
    if args.command == "sources":
        return (
            cmd_sources_validate(settings)
            if args.action == "validate"
            else cmd_sources_check(settings)
        )
    if args.command == "collect":
        return cmd_collect(settings, args.source)
    return cmd_schedule(settings)
