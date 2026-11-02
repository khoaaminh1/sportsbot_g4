# Phase 2 report: core infrastructure

- Date: 2026-11-02
- Branch / PR: `feature/phase2-core-infrastructure`, [PR #3](https://github.com/khoaaminh1/sportsbot_g4/pull/3)
- Evidence run: [CI run 41417868188](https://github.com/khoaaminh1/sportsbot_g4/actions/runs/41417868188) on commit `9d2e87d`, GitHub-hosted `ubuntu-latest` runner (4 vCPU, 15.6 GiB RAM as reported by `docker stats`). This is a **single-machine** run of all containers, not a multi-node cluster.

## What was implemented
| Area | Result |
|---|---|
| Docker stack | `docker-compose.yml` with profiles `core` (PostgreSQL 16, Qdrant 1.19.2, backend) and `bigdata` (HDFS 3.4.1 NameNode + 2 DataNodes, one-shot `hdfs-init`, Spark 3.5.3 master + worker) |
| Database | `infra/postgres/migrations/001_init.sql`: `schema_migrations`, `sources`, `collection_runs`, `articles`, `conversations`, `messages` |
| Backend foundation | FastAPI app factory, settings from env, JSON logs with request ids, shared error schema, CORS, `/health/live`, `/health` (PostgreSQL + Qdrant required, HDFS optional; concurrent checks with timeouts), non-root Dockerfile |
| Contracts | `docs/contracts/article.schema.json` and `api.openapi.yaml` (2 implemented, 7 planned operations), frozen with contract tests |
| Big Data smoke | `pipelines/spark/jobs/hdfs_smoke.py`: Spark on the cluster writes and reads partitioned Parquet on HDFS (synthetic rows) |
| Testing | pytest unit, contract and integration suites; ruff lint and format |
| CI | `lint-test` job, then a `stack` job that boots everything and checks it end to end |
| Process | `AGENTS.md`, `CONTRIBUTING.md`, PR and issue templates, ADR 0001 (architecture), ADR 0002 (Compose profiles), runbook `docs/runbooks/stack.md`, manual workflow for labels and milestones |

## Test results (real runs)
| Check | Result | Where |
|---|---|---|
| ruff check + format | pass | local (Python 3.11.15) and CI |
| Unit + contract tests | 29 passed | local and CI |
| Migration on PostgreSQL 16 | 6 tables created | local PostgreSQL 16 (authoring workspace) |
| HDFS | 2 live DataNodes; configured capacity 288.51 GB (runner disk) | CI, `hdfs dfsadmin -report` |
| HDFS layout | `/sportsbot/{bronze,silver,gold,_smoke}` created | CI, `hdfs-init` |
| Spark on HDFS | 10,000 rows written and 10,000 read back, 2,000 per sport, 8.83 s, `spark://spark-master:7077`, Spark 3.5.3 | CI, `SMOKE_RESULT` line |
| Replication | default factor 2, average block replication 2.0 | CI, `hdfs fsck /sportsbot/_smoke` |
| Integration tests | 5 passed: backend ready with real PostgreSQL and Qdrant, `schema 001` applied, OpenAPI served, backend sees HDFS, Qdrant reachable | CI |

The smoke data is **synthetic** (generated rows); it tests mechanics only and is not collected news.

## Memory per service (measured)
Idle, right after the Spark smoke job finished and the integration tests ran (`docker stats --no-stream`, CI run above):

| Service | Memory |
|---|---:|
| namenode | 418.3 MiB |
| datanode2 | 291.2 MiB |
| datanode1 | 266.2 MiB |
| spark-worker | 202.8 MiB |
| spark-master | 187.7 MiB |
| backend | 69.8 MiB |
| qdrant | 26.9 MiB |
| postgres | 26.8 MiB |
| **Total** | **about 1,490 MiB** |

This is idle memory. A running Spark job adds up to the worker's 2 GB executor limit plus the driver; Qdrant grows with the number of vectors. The earlier 8-10 GB figure in the assessment was an unmeasured estimate and is replaced by these numbers; peak memory under real Phase 3 jobs still has to be measured.

## Commands
```bash
make lint test                 # ruff + 29 unit/contract tests
make up-core                   # PostgreSQL, Qdrant, backend
make up-bigdata                # HDFS + Spark + /sportsbot layout
make spark-smoke               # Spark job on HDFS
make test-integration          # integration tests against the running stack
```

## Problems found and fixed during the phase
1. Spark master healthcheck probed `127.0.0.1:7077`, but the master RPC port binds only to the container hostname. Fixed by probing the web UI port 8080 (binds `0.0.0.0`).
2. The Spark image runs Python 3.10, which has no `datetime.UTC`. The smoke job now uses `timezone.utc`; Spark jobs must stay Python 3.10 compatible (noted in the job docstring).

## Known limitations
- All containers run on one host (pseudo-distributed). Replication 2 across 2 DataNodes is real HDFS behaviour, but not fault tolerance across machines.
- Development security settings: HDFS permissions disabled, HDFS containers run as root, no TLS, local passwords in `.env`.
- Migrations run only on a new PostgreSQL volume; a migration runner for existing volumes is needed when `002_*.sql` arrives.
- The stack has not yet been started on a team member's laptop (Windows + WSL2). CI proves the configuration, not each laptop.
- `develop` branch not created yet; PRs currently target `main`.

## Remaining tasks before Phase 3 starts
- [ ] Merge PR #2 (README) and PR #3 (this phase).
- [ ] Each member runs `make up-core` and `make up-bigdata` on their own machine and reports problems as issues.
- [ ] Create the `develop` branch and protect `main` (repository settings, owner action).
- [ ] Run the "Bootstrap project" workflow once to create labels and milestones (owner action).
- [ ] Team answers the open decisions (deadline, LLM, etc.) from the Phase 1 assessment.

## Acceptance criteria for Phase 3 (data engineering)
- [ ] At least 8 real sources in at least 3 languages in `pipelines/sources.yaml`, each with a robots.txt result.
- [ ] Collectors run unattended for 7 days; per-run statistics in `collection_runs`.
- [ ] Bronze to Silver to Gold runs incrementally on HDFS; re-running on the same input adds 0 rows.
- [ ] Dedup report with exact and near-duplicate counts and 50 hand-checked pairs.
- [ ] Scaling table at 10k / 100k / 1M with all 8 brief metrics, every dataset labeled by origin.
- [ ] Freshness report: p50/p95 of `published_at` to `collected_at` and `collected_at` to searchable.
