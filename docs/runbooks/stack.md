# Runbook: local Docker stack

How to start, check, and troubleshoot the development stack (ADR 0002): profiles, HDFS and Spark.
The crawler and chatbot now run on the streaming pipeline: see [streaming-stack.md](streaming-stack.md).
The "Collector (Phase 3)" section below describes the legacy RSS collector, kept for reference. Windows users: run every command inside WSL2 (Ubuntu 24.04) with Docker Desktop's WSL integration enabled.

## Prerequisites
- Docker with Compose v2.24 or newer (`docker compose version`), because `env_file` uses `required: false`.
- Python 3.11 and `make` for tests and helpers.
- Free ports: 5432 (PostgreSQL), 6333 (Qdrant), 8000 (backend), 9870 (HDFS UI/WebHDFS), 8080/8081 (Spark UIs).

## First start
```bash
cp .env.example .env            # fill in values; never commit .env
make setup                      # Python dev tools + git hooks
make up-core                    # PostgreSQL + Qdrant + backend, waits until healthy
curl http://localhost:8000/health
make up-bigdata                 # HDFS (NameNode + 2 DataNodes) + Spark, then creates /sportsbot dirs
make hdfs-ls                    # lists /sportsbot/{bronze,silver,gold,_smoke}
make spark-smoke                # Spark job on the cluster writes and reads Parquet on HDFS
```
For the backend to also check HDFS, set `STACK_HDFS_WEBHDFS_URL=http://namenode:9870` in `.env` and run `docker compose up -d backend` (a `restart` keeps the old environment).

## What "healthy" looks like
| Check | Command | Expected |
|---|---|---|
| Backend liveness | `curl localhost:8000/health/live` | `{"status":"ok",...}` |
| Backend readiness | `curl localhost:8000/health` | `status` `ok` (or `degraded` if only HDFS is down); HTTP 503 if PostgreSQL or Qdrant is down |
| Schema applied | same, `checks.postgres.detail` | `schema 001` |
| HDFS | NameNode UI http://localhost:9870 | 2 live DataNodes |
| Spark | Master UI http://localhost:8080 | 1 alive worker, 2 cores |
| Spark on HDFS | `make spark-smoke` | a line `SMOKE_RESULT {... "ok": true ...}` |
| API docs | http://localhost:8000/docs | Swagger UI |

## Resource use
Measured idle in CI (GitHub-hosted runner, 2026-11-02, details in `docs/reports/phase-2.md`): about **1.5 GiB in total** (NameNode 418 MiB, DataNodes 266 and 291 MiB, Spark master 188 MiB, Spark worker 203 MiB, backend 70 MiB, Qdrant and PostgreSQL about 27 MiB each). A running Spark job adds up to the worker's 2 GB executor limit plus the driver. The CI step `Memory use per service` prints fresh numbers on every run. On a small laptop, run only the profile your task needs.

## Troubleshooting
- **Port already in use:** another PostgreSQL or service runs on the host. Stop it, or change `POSTGRES_PORT` in `.env` (other ports: edit `docker-compose.yml` through an issue).
- **`schema_migrations missing` in `/health`:** the PostgreSQL volume was created before the migrations existed. Migrations in `/docker-entrypoint-initdb.d` run only on a new volume. Development fix (deletes local data): `docker compose --profile core down -v && make up-core`.
- **NameNode does not start after an image change:** the formatted metadata in the `namenode_data` volume may not match. Development fix (deletes HDFS data): `make clean-volumes`, then `make up-bigdata`.
- **`hdfs-init` says fewer than 2 DataNodes:** check `docker compose --profile bigdata logs datanode1 datanode2`; DataNodes register with the NameNode by hostname.
- **Spark job cannot reach HDFS:** paths must use `hdfs://namenode:8020/...`; the job runs inside the Compose network, not on the host.
- **Changed `.env` but nothing happened:** recreate the service, e.g. `docker compose up -d backend`.

## Collector (Phase 3, legacy)
```bash
make sources-check              # which feeds answer right now (robots.txt, HTTP status, items, dates)
make up-collect                 # PostgreSQL + HDFS + collector scheduler (runs until make down)
make collect-once               # one pass now, without waiting for the scheduler
make bronze-ls                  # Bronze files in HDFS
docker compose --profile core exec postgres psql -U sportsbot -d sportsbot \
  -c "SELECT source_id, status, http_status, items_new, error FROM collection_runs ORDER BY run_id DESC LIMIT 20"
```
Set `COLLECTOR_CONTACT` in `.env` to a team email: it goes into the User-Agent so site owners can reach us.

| Symptom | Meaning / fix |
|---|---|
| `status = skipped`, error mentions robots.txt | The site disallows the feed path. Disable the source; do not work around it. |
| `http_status = 403` | Access refused. Disable the source and note it in `sources.json`. |
| `http_status = 404` | Feed URL changed or not served to this location. Re-check from Vietnam; update the URL. |
| `http_status = 304`, `items_new = 0` | Normal: the feed has not changed since the last run. |
| `WebHdfsError ... expected 307` | HDFS not running or `hdfs-init` not run: `make up-bigdata`. |

## Stopping
`make down` keeps data in named volumes. `make clean-volumes` deletes PostgreSQL, Qdrant and HDFS data.
