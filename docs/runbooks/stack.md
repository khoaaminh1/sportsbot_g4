# Runbook: local Docker stack

How to start, check, and troubleshoot the development stack (ADR 0002). Windows users: run every command inside WSL2 (Ubuntu 24.04) with Docker Desktop's WSL integration enabled.

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
Measured in CI on a GitHub-hosted runner by the `Memory use per service` step of `.github/workflows/ci.yml`; see the latest run's log and `docs/reports/phase-2.md`. On a small laptop, run only the profile your task needs.

## Troubleshooting
- **Port already in use:** another PostgreSQL or service runs on the host. Stop it, or change `POSTGRES_PORT` in `.env` (other ports: edit `docker-compose.yml` through an issue).
- **`schema_migrations missing` in `/health`:** the PostgreSQL volume was created before the migrations existed. Migrations in `/docker-entrypoint-initdb.d` run only on a new volume. Development fix (deletes local data): `docker compose --profile core down -v && make up-core`.
- **NameNode does not start after an image change:** the formatted metadata in the `namenode_data` volume may not match. Development fix (deletes HDFS data): `make clean-volumes`, then `make up-bigdata`.
- **`hdfs-init` says fewer than 2 DataNodes:** check `docker compose --profile bigdata logs datanode1 datanode2`; DataNodes register with the NameNode by hostname.
- **Spark job cannot reach HDFS:** paths must use `hdfs://namenode:8020/...`; the job runs inside the Compose network, not on the host.
- **Changed `.env` but nothing happened:** recreate the service, e.g. `docker compose up -d backend`.

## Stopping
`make down` keeps data in named volumes. `make clean-volumes` deletes PostgreSQL, Qdrant and HDFS data.
