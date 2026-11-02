# pipelines

Data collection and processing: collectors, Bronze writer, Spark Silver/Gold jobs, deduplication, enrichment, exports to PostgreSQL / Qdrant / BM25. Owners: VNP, LTN.

Status: **Phase 2** contains only the Spark-on-HDFS smoke job (`spark/jobs/hdfs_smoke.py`, run with `make spark-smoke`). Collectors and the real jobs arrive in Phase 3.
