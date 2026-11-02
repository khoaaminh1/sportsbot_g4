# pipelines

Data collection and processing: collectors, Bronze ingestion into HDFS, and (next) the Spark Silver/Gold jobs. Owners: VNP, LTN. Design: [ADR 0003](../docs/decisions/0003-collection-and-bronze.md).

## Status
| Part | Status |
|---|---|
| Source registry (`sources.yaml`), 7 enabled RSS sources in vi / en / es | Implemented |
| Polite collector (robots.txt, per-host delay, conditional GET, retries, size cap) | Implemented |
| Bronze in HDFS (append-only gzip JSON Lines), state and runs in PostgreSQL | Implemented |
| Scheduler (`collector` container) | Implemented |
| Spark Silver (clean, validate, language ID, dedup) and Gold (classifier, entities, analytics) | Planned (Phase 3b) |
| API and page collectors | Planned |

## Layout
```
src/sportsbot_pipelines/
  collectors/   registry.py, http.py (polite client), rss.py (feed -> Bronze records), urls.py, dates.py
  ingestion/    bronze.py (WebHDFS / local writers), state.py, run.py, scheduler.py, migrate.py
  cli.py        python -m sportsbot_pipelines {migrate, sources validate|check, collect, schedule}
spark/jobs/     hdfs_smoke.py (Phase 2); Silver/Gold jobs come here
sources.yaml    the source registry
tests/          unit tests, integration tests (real PostgreSQL), SYNTHETIC fixtures
```

## Commands
```bash
make sources-check     # fetch robots.txt + each feed once, print a table (no writes)
make up-collect        # PostgreSQL + HDFS + the collector scheduler
make collect-once      # one pass over all enabled sources into HDFS Bronze
make bronze-ls         # list Bronze files in HDFS
```
Without Docker (writes Bronze to `data/bronze/`, needs a PostgreSQL from `make up-core`):
```bash
PYTHONPATH=pipelines/src python -m sportsbot_pipelines collect
```

## Bronze record (schema version 2)
`record_id` (sha256 of canonical URL), `source_id`, `source_name`, `source_kind`, `language_hint`, `feed_url`, `url`, `canonical_url`, `title`, `summary_html` (raw), `published_raw` (as given), `published_at` (UTC or null), `published_tz_assumed`, `updated_raw`, `authors`, `tags`, `collected_at` (UTC), `run_id`, `http_status`, `collector_version`, `schema_version`.

## Adding a source
1. Add an entry to `sources.yaml` with its timezone and allowed `domains`.
2. Run `make sources-check`; set `verified: true` and write the date and result in `notes` only if it returned items.
3. Check the site's terms of use; note anything relevant in `notes`. If a site refuses access (403, robots.txt), disable it: never work around a refusal.
