"""Command line: `python -m sportsbot_pipelines <command>`.

migrate              apply pending SQL migrations
sources validate     check sources.json (no network)
sources check        fetch robots.txt + each feed once and print a table (no writes)
sources sitemaps     print the Sitemap: lines of each source's robots.txt
collect              one collection pass over enabled sources into Bronze (no Kafka)
schedule             run forever, each source on its own interval (no Kafka)

Streaming pipeline (ADR 0005, needs Kafka):
discover [--once]    RSS -> Bronze + articles.discovered (forever, or one pass)
backfill --since D   sitemaps -> articles.discovered (older articles)
extract              articles.discovered -> fetch/extract/dedup -> articles.extracted | dead-letter
consume operational  articles.extracted -> PostgreSQL
consume semantic     articles.extracted -> chunks -> BGE-M3 -> Qdrant
consume archive      articles.extracted -> Bronze full-text files (HDFS)
stats                corpus counts per source (PostgreSQL) and vectors (Qdrant)
export-sample        random articles as CSV for hand labeling (sport category, golden set)
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, date, datetime

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
from sportsbot_pipelines.streaming import topics


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


def cmd_sources_sitemaps(settings: PipelineSettings) -> int:
    registry = load_registry(settings.sources_file)
    with _client(settings) as client:
        for s in registry.sources:
            try:
                maps = client.sitemaps_for(str(s.url))
                row = {"source": s.id, "configured": [str(u) for u in s.sitemaps], "robots": maps}
            except FetchError as exc:
                row = {"source": s.id, "error": str(exc)}
            print(json.dumps(row, ensure_ascii=False), flush=True)
    return 0


def _bus(settings: PipelineSettings):
    from sportsbot_pipelines.streaming.bus import KafkaBus

    return KafkaBus(settings.kafka_bootstrap_servers)


def cmd_discover(settings: PipelineSettings, once: bool, source_id: str | None) -> int:
    from sportsbot_pipelines.discovery.publish import bus_publisher

    sources, state = _setup(settings, source_id)
    bus = _bus(settings)
    publish = bus_publisher(bus, "rss")
    with _client(settings) as client:
        if once:
            runs = collect_all(
                sources,
                client,
                state,
                _writer(settings),
                max_items=settings.max_items_per_fetch,
                publish=publish,
            )
            print(
                json.dumps(
                    {"summary": {"runs": len(runs), "items_new": sum(r.items_new for r in runs)}}
                )
            )
        else:
            run_forever(
                sources,
                client,
                state,
                _writer(settings),
                tick_seconds=settings.scheduler_tick_seconds,
                max_items=settings.max_items_per_fetch,
                publish=publish,
            )
    bus.flush()
    return 0


def cmd_backfill(
    settings: PipelineSettings, since: date, max_urls: int, source_id: str | None
) -> int:
    from sportsbot_pipelines.discovery.backfill import backfill_source
    from sportsbot_pipelines.discovery.publish import bus_publisher

    sources, state = _setup(settings, source_id)
    bus = _bus(settings)
    publish = bus_publisher(bus, "sitemap")
    total = 0
    with _client(settings) as client:
        for source in sources:
            result = backfill_source(source, client, state, publish, since=since, max_urls=max_urls)
            total += int(result.get("queued", 0))
            print(json.dumps(result, ensure_ascii=False), flush=True)
    bus.flush()
    print(json.dumps({"summary": {"queued": total}}))
    return 0


def cmd_extract(settings: PipelineSettings) -> int:
    from sportsbot_pipelines.extraction.worker import PostgresDedup, make_handler
    from sportsbot_pipelines.streaming.bus import run_consumer

    registry = load_registry(settings.sources_file)
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    migrate(engine, settings.migrations_dir)
    bus = _bus(settings)
    with _client(settings) as client:
        handler = make_handler(
            client,
            bus,
            PostgresDedup(engine),
            min_chars=settings.extract_min_chars,
            timezones={s.id: s.timezone for s in registry.sources},
        )
        run_consumer(
            settings.kafka_bootstrap_servers,
            topics.GROUP_EXTRACTOR,
            topics.DISCOVERED,
            handler,
            stage="extract",
            bus=bus,
        )
    return 0


def cmd_consume(settings: PipelineSettings, which: str) -> int:
    from sportsbot_pipelines.streaming.bus import run_consumer

    bus = _bus(settings)
    if which == "operational":
        from sportsbot_pipelines.consumers.operational import make_handler

        _, state = _setup(settings, None)  # migrations + sources rows (foreign key)
        engine = create_engine(settings.database_url, pool_pre_ping=True)
        handler, group = make_handler(engine), topics.GROUP_OPERATIONAL
    elif which == "semantic":
        from sportsbot_agent.clients.embeddings import BGE_M3_DIM, EmbeddingClient
        from sportsbot_agent.clients.qdrant import QdrantClient
        from sportsbot_pipelines.consumers.semantic import make_handler

        qdrant = QdrantClient(settings.qdrant_url)
        qdrant.ensure_collection(BGE_M3_DIM)
        embedder = EmbeddingClient(settings.embedding_base_url, settings.embedding_model)
        handler = make_handler(
            embedder,
            qdrant,
            max_chars=settings.chunk_max_chars,
            overlap_chars=settings.chunk_overlap_chars,
        )
        group = topics.GROUP_SEMANTIC
    else:
        from sportsbot_pipelines.consumers.archive import ArchiveBuffer

        archive = ArchiveBuffer(_writer(settings), batch_size=settings.archive_batch_size)
        try:
            run_consumer(
                settings.kafka_bootstrap_servers,
                "sportsbot-archive",
                topics.EXTRACTED,
                archive.handle,
                stage="archive",
                bus=bus,
            )
        finally:
            archive.flush()
        return 0
    run_consumer(
        settings.kafka_bootstrap_servers, group, topics.EXTRACTED, handler, stage=which, bus=bus
    )
    return 0


def cmd_stats(settings: PipelineSettings) -> int:
    from sqlalchemy import text

    engine = create_engine(settings.database_url, pool_pre_ping=True)
    with engine.connect() as conn:
        rows = (
            conn.execute(
                text(
                    "SELECT source_id, language, count(*) AS articles, "
                    "count(*) FILTER (WHERE ingested_at > now() - interval '24 hours')"
                    " AS last_24h, "
                    "max(published_at) AS newest FROM articles GROUP BY 1, 2 ORDER BY 3 DESC"
                )
            )
            .mappings()
            .all()
        )
    total = 0
    for r in rows:
        total += r["articles"]
        print(json.dumps({**dict(r), "newest": str(r["newest"])}, ensure_ascii=False))
    vectors = None
    try:
        from sportsbot_agent.clients.qdrant import QdrantClient

        vectors = QdrantClient(settings.qdrant_url).count()
    except Exception as exc:  # Qdrant down or collection missing: report, do not fail
        vectors = f"unavailable: {type(exc).__name__}"
    print(json.dumps({"summary": {"articles": total, "qdrant_chunks": vectors}}))
    return 0


LABEL_COLUMNS = [
    "article_id",
    "source_id",
    "language",
    "published_at",
    "title",
    "excerpt",
    "url",
    "sport_label",
    "labeler",
    "notes",
]


def cmd_export_sample(settings: PipelineSettings, n: int, out: str, seed: float) -> int:
    import csv
    from pathlib import Path

    from sqlalchemy import text

    engine = create_engine(settings.database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(text("SELECT setseed(:s)"), {"s": seed})
        rows = (
            conn.execute(
                text(
                    "SELECT article_id, source_id, language, published_at, title, "
                    "left(regexp_replace(content, '\\s+', ' ', 'g'), 300) AS excerpt, "
                    "source_url AS url FROM articles WHERE content IS NOT NULL "
                    "ORDER BY random() LIMIT :n"
                ),
                {"n": n},
            )
            .mappings()
            .all()
        )
    path = Path(out)
    path.parent.mkdir(parents=True, exist_ok=True)
    # utf-8-sig: Excel and Google Sheets open Vietnamese text correctly.
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=LABEL_COLUMNS)
        writer.writeheader()
        for r in rows:
            writer.writerow({**dict(r), "sport_label": "", "labeler": "", "notes": ""})
    print(json.dumps({"exported": len(rows), "file": str(path), "seed": seed}))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="sportsbot_pipelines")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("migrate")
    src = sub.add_parser("sources")
    src.add_argument("action", choices=["validate", "check", "sitemaps"])
    col = sub.add_parser("collect")
    col.add_argument("--source", help="collect only this source id")
    sub.add_parser("schedule")
    disc = sub.add_parser("discover")
    disc.add_argument("--once", action="store_true", help="one pass, then exit")
    disc.add_argument("--source", help="only this source id")
    back = sub.add_parser("backfill")
    back.add_argument("--since", type=date.fromisoformat, required=True, help="YYYY-MM-DD")
    back.add_argument("--max", type=int, default=5000, help="max URLs per source")
    back.add_argument("--source", help="only this source id")
    sub.add_parser("extract")
    con = sub.add_parser("consume")
    con.add_argument("which", choices=["operational", "semantic", "archive"])
    sub.add_parser("stats")
    exp = sub.add_parser("export-sample")
    exp.add_argument("--n", type=int, default=400)
    exp.add_argument("--out", default="data/labeling/sample.csv")
    exp.add_argument("--seed", type=float, default=0.42, help="same seed = same sample")
    args = parser.parse_args(argv)

    settings = get_settings()
    configure_logging(settings.log_level, settings.log_format)
    if args.command == "migrate":
        return cmd_migrate(settings)
    if args.command == "sources":
        actions = {
            "validate": cmd_sources_validate,
            "check": cmd_sources_check,
            "sitemaps": cmd_sources_sitemaps,
        }
        return actions[args.action](settings)
    if args.command == "discover":
        return cmd_discover(settings, args.once, args.source)
    if args.command == "backfill":
        return cmd_backfill(settings, args.since, args.max, args.source)
    if args.command == "extract":
        return cmd_extract(settings)
    if args.command == "consume":
        return cmd_consume(settings, args.which)
    if args.command == "stats":
        return cmd_stats(settings)
    if args.command == "export-sample":
        return cmd_export_sample(settings, args.n, args.out, args.seed)
    if args.command == "collect":
        return cmd_collect(settings, args.source)
    return cmd_schedule(settings)
