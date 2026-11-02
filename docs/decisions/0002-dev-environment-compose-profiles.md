# 0002. Development environment: Docker Compose with `core` and `bigdata` profiles

- Status: accepted (2026-11-02, TMK)
- Date: 2026-11-02
- Deciders: TMK; VNP (bigdata profile owner) to confirm

## Context
- The full stack has 8 long-running services: PostgreSQL, Qdrant, backend, HDFS NameNode, 2 DataNodes, Spark master, Spark worker. Its memory need is not measured yet (estimate 8-10 GB).
- Members use Windows laptops (prototype README plans Ubuntu 24.04 on WSL2). Spark needs Java 17+, and Hadoop does not run natively on Windows.
- Backend and agent work (Phases 4-5) does not need HDFS or Spark running; data engineering (Phase 3) does not need the backend.

## Options considered
1. One Compose file, all services always on.
2. **One Compose file with profiles**: `core` (postgres, qdrant, backend) and `bigdata` (namenode, datanode1, datanode2, hdfs-init, spark-master, spark-worker).
3. Separate Compose files per area.

## Decision
Option 2. One file keeps one network and one set of names; profiles let each member start only what their task needs (`make up-core`, `make up-bigdata`, or `make up` for both).

Images (pinned):
- `postgres:16-alpine`
- `qdrant/qdrant:v1.19.2` (the version the reference project tested)
- `apache/hadoop:3.4.1` (official Apache image, configured by environment variables in `infra/hdfs/hadoop.env`)
- `apache/spark:3.5.3-scala2.12-java17-python3-ubuntu` (official image with Python 3)

Development settings, stated so nobody mistakes them for production: HDFS permissions are disabled (`dfs.permissions.enabled=false`), HDFS containers run as root so named volumes are writable, and services listen on localhost ports without TLS.

## Consequences
- Windows members run everything inside WSL2; no native Windows path is supported.
- CI boots both profiles on GitHub-hosted runners, so "the stack starts" is checked on every PR, not only on one laptop.
- Measure the real memory of each profile in Phase 2 and record it in `docs/runbooks/stack.md`; revisit (for example 1 DataNode on small machines) if a member's laptop cannot run `bigdata`.
