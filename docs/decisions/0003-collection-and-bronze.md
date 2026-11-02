# 0003. Collection: polite RSS polling into append-only HDFS Bronze, state in PostgreSQL

- Status: accepted (2026-11-02, TMK; VNP and LTN, the pipelines owners, to review)
- Date: 2026-11-02
- Deciders: TMK; VNP, LTN to confirm

## Context
- Brief section 3.1: scheduled collection, incremental ingestion, separate publication and collection times, duplicate detection, failure statistics, respect for robots.txt, terms and rate limits, RSS and official APIs first.
- The prototype (PR #1) polled two RSS feeds into SQLite with a domain allow-list, a size limit and safe URL handling, but no robots.txt check, no retries, no conditional GET, and no HDFS.
- Live check on 2026-11-02 from a GitHub-hosted runner (CI run 41423506743): 6 of 8 sources answered; VnExpress returned 404, L'Équipe 403; Tuổi Trẻ served 50 entries whose dates feedparser could not parse.

## Decision
1. **Registry.** `pipelines/sources.yaml` is the single list of sources (id, URL, language, timezone, interval, allowed domains, verified flag with the check date). It is mirrored into the `sources` table.
2. **Polite client.** robots.txt before every fetch, cached 6 h; an unreadable robots.txt means skip, not allow. One request per host every 3 s, conditional GET, retries with backoff only on timeouts, 429 and 5xx, 5 MB cap, identifying User-Agent with a contact address. A refusal (403, robots disallow) is respected: the source is disabled, never worked around.
3. **Bronze.** Append-only gzip JSON Lines in HDFS, one file per (source, run): `source=/ingest_date=/ingest_hour=/part-<run>.jsonl.gz`, written with WebHDFS `overwrite=false`. Records keep the raw title, summary HTML and date strings; additions are lossless (canonical URL, article id, UTC date, ingestion metadata).
4. **Dates.** Parse the raw date ourselves first; an offset-less date gets the source's timezone and `published_tz_assumed=true`. Reason: feedparser treats offset-less dates as UTC, which shifts Vietnamese times by 7 hours.
5. **Incremental state in PostgreSQL.** `seen_items` (article ids already in Bronze), `collector_state` (ETag, Last-Modified), one `collection_runs` row per run. Order: find unseen, write Bronze, mark seen; a failed write marks nothing, so the next run retries.
6. **Scheduling.** One Python loop in a `collector` container runs each source when its interval has passed (30-60 min). No Kafka, Airflow or cron: about 7 feeds polled every 30-60 minutes is not a stream.

## Consequences
- "Near real time" means within one source interval plus the Silver/Gold batch; the lag is measured from `published_at`, `collected_at` and `collection_runs`.
- Exact duplicates within a source are removed at collection; duplicates across sources (the same story from two outlets) are left for the Silver job (MinHashLSH), which needs all sources together.
- Sources must be re-checked regularly (`make sources-check`); results go into the registry notes.
- Open: VnExpress must be re-checked from Vietnam (the 404 may be specific to the runner's location); a French source that accepts us is still needed.
