# 0005. Streaming ingestion with Kafka; HDFS and Spark stay as the batch layer

- Status: accepted (2026-11-02, team pipeline diagram from TMK)
- Date: 2026-11-02
- Deciders: TMK, VNP, LTN
- Supersedes: the "no Kafka" note in the README technology table (ADR 0001 stays for the rest)

## Context
- The team drew the target pipeline: `sources.json` → RSS scan and URL normalisation → Kafka
  `articles.discovered` → fetch, extract, de-duplicate → Kafka `articles.extracted` (errors to
  `articles.dead-letter`) → an operational consumer (PostgreSQL) and a semantic consumer
  (chunks → BGE-M3 → Qdrant) → advanced RAG → FastAPI (news + WebSocket, chat over SSE) → frontend.
- The presentation moved to **Tuesday 10 November 2026**. The corpus must grow fast: RSS feeds
  list only the latest 20–100 items, and the Phase 3a collector stored feed summaries, not text.
- This is a Big Data course: HDFS + Spark must still carry real work.

## Options considered
1. Keep the batch design (collector → HDFS → hourly Spark → serving stores). Simple, but full
   text and search arrive only after Silver/Gold exist.
2. Follow the diagram with Kafka as the backbone, and keep HDFS/Spark as an archive and
   batch-analytics layer fed by a third consumer.
3. Follow the diagram and drop HDFS/Spark. Fastest, but removes the course's core requirement.

## Decision
Option 2.
- One Kafka broker (KRaft, `apache/kafka:3.9.1`), topics with 6 partitions keyed by source id,
  so one extractor process handles one site at a time (robots.txt and the per-host delay hold
  when the extractor is scaled out). Two weeks of retention for replays.
- Discovery reuses the Phase 3a collector (seen-items state, Bronze files) and publishes new
  items. A sitemap backfill (`make backfill`) adds older articles that RSS no longer lists.
- The extractor uses trafilatura; duplicates are dropped by canonical URL and by a hash of the
  normalised text. Failures (robots, HTTP, no main text, too short) go to the dead-letter topic
  with a reason.
- Consumers are idempotent upserts keyed by article id (at-least-once delivery). A dependency
  outage raises `TransientError`: the consumer waits and retries instead of dead-lettering.
- `consumer-archive` writes the extracted articles as gzip JSON Lines Bronze files (HDFS when
  the bigdata profile runs); Spark builds Silver/Gold, near-duplicate clusters, the classifier
  baseline and the scaling experiments from them.

## Consequences
- The serving stores fill within seconds of extraction, so the chatbot can be demonstrated on
  real data early.
- More moving parts: Kafka adds about 0.7 GB of memory (estimate). Profiles keep it optional.
- The archive consumer commits offsets per message and writes in batches: a hard kill can lose
  up to one batch from the archive only (the articles stay in PostgreSQL and Qdrant).
- Verified so far: unit tests with an in-memory bus, PostgreSQL 16 and Qdrant 1.19.2 for the
  consumers, and a CI job (synthetic feed → Kafka → extractor → PostgreSQL). A run on a team
  laptop is still needed.
