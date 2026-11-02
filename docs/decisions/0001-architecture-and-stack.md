# 0001. Layered RAG architecture on HDFS + Spark, with PostgreSQL and Qdrant for serving

- Status: accepted (2026-11-02, TMK approval of the Phase 1 technical assessment)
- Date: 2026-11-02
- Deciders: TMK (lead); VNP and LTN to confirm at the next team meeting

## Context
- The course is assessed on Big Data engineering (genuine, measurable), AI/ML (at least one trained and evaluated model), a reliable backend, and a web UI built last (master project instruction, sections 4-7).
- The chatbot must answer from newly collected news, cite sources with links and publication times, and support Vietnamese and English questions.
- The team has three members and laptops; no university cluster is known to be available.
- The reference project (`scitech-rag-agent`, MIT) shows a working RAG stack (FastAPI, Qdrant, BM25 + dense hybrid retrieval, Docker Compose), but has no HDFS, no trained model and English-only retrieval.

## Options considered
1. **Reference stack as-is** (files + PySpark local mode, Qdrant, no relational DB). Simplest, but fails the brief's HDFS requirement and has no place for collection runs and conversations.
2. **Layered architecture: HDFS Bronze/Silver/Gold + Spark, serving copies in PostgreSQL, Qdrant and BM25.** Meets the brief; every layer is rebuildable from Bronze.
3. **Streaming stack with Kafka + Spark Structured Streaming + Elasticsearch.** Overbuilt for about 20 feeds polled every 15-60 minutes; three more services to run on laptops.

## Decision
Option 2.
- **Collection:** Python collectors (feedparser, httpx, trafilatura) scheduled per source with APScheduler, writing raw JSON Lines to HDFS Bronze.
- **Storage:** HDFS 3.x with 1 NameNode and 2 DataNodes (replication 2) in Docker.
- **Processing:** Apache Spark (PySpark) in hourly micro-batches: Bronze to Silver (clean, validate, language, dedup with MinHashLSH) to Gold (classifier, entities, analytics), Parquet.
- **Serving:** PostgreSQL (articles, sources, collection runs, conversations), Qdrant (multilingual-e5 vectors), BM25 index. The request path never reads HDFS.
- **Application:** FastAPI backend and agent; the LLM sits behind an OpenAI-compatible client (choice still open, see the assessment's decisions list); React frontend in Phase 6.
- **Not used:** Kafka, Airflow, Elasticsearch, until a measured requirement justifies them.

## Consequences
- The full stack is heavy for a laptop; ADR 0002 splits it into Compose profiles.
- All "distributed" results run on one host unless we later run across members' laptops; reports must say "single-machine / pseudo-distributed".
- "Real time" is defined as near real time (hourly batches) and freshness lag must be measured.
- Revisit Kafka only if live match events (seconds-level updates) are added to scope.
