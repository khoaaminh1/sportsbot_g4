# pipelines

Collection and processing: discovery, backfill, extraction, Kafka consumers, monitoring, and the
Spark jobs. Owners: TMK (streaming), VNP (Spark). Design: [ADR 0005](../docs/decisions/0005-streaming-pipeline-kafka.md);
operation: [docs/runbooks/streaming-stack.md](../docs/runbooks/streaming-stack.md).

## Status (2026-11-04)
| Part | Status |
|---|---|
| Source registry `sources.json`: 48 feeds, 45 enabled (vi, en, es, fr), sitemaps and section pages | Running |
| Discovery (RSS every 30-60 min), backfill (sitemaps, paginated sections) → Kafka `articles.discovered` | Running |
| Extractor: polite fetch (robots.txt, 3 s per host), trafilatura, exact dedup, dead letters, replay | Running |
| Consumers: operational (PostgreSQL), semantic (chunks → BGE-M3 → Qdrant), archive (Bronze) | Running |
| Monitor: failing / stale sources, stalls, Discord or Telegram alerts, circuit breaker | Running |
| Exports: corpus JSONL, PostgreSQL dump, Bronze archive, labeling sample (Kaggle) | Running |
| Spark Silver (clean, exact + near-duplicate clusters) and Gold (statistics) | To do: VNP, `docs/TASKS.md` V1-V2 |
| Legacy RSS collector into HDFS (`collect` profile, Phase 3a) | Kept; replaced by the streaming pipeline |

## Layout
```
src/sportsbot_pipelines/
  collectors/   registry.py, http.py (polite client), rss.py, urls.py, dates.py
  discovery/    publish.py, sitemap.py, listing.py (section pages), backfill.py
  extraction/   extract.py (trafilatura, content hash), worker.py (Kafka handler)
  consumers/    operational.py, semantic.py, archive.py, chunking.py
  streaming/    topics.py, bus.py (Kafka / in-memory), replay.py
  monitoring/   rules.py, notify.py, monitor.py
  ingestion/    bronze.py, state.py, run.py, scheduler.py, migrate.py
  cli.py        python -m sportsbot_pipelines --help
spark/jobs/     hdfs_smoke.py; Silver/Gold jobs go here (VNP)
sources.json    the source registry
tests/          unit tests, integration tests (real PostgreSQL), SYNTHETIC fixtures
```

## Adding a source
1. Add an entry to `sources.json` with its timezone, allowed `domains`, and optional `sitemaps` +
   `sitemap_include` or `listing` for backfill.
2. Run `make sources-check` from Vietnam; set `verified: true` and write the date and result in
   `notes` only if the status is `ok`.
3. If a site refuses access (403, robots.txt, network block), disable it or use `extract: false`
   (feed summary only): never work around a refusal.
