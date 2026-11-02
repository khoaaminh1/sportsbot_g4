"""Spark-on-HDFS smoke test (Phase 2): proves the cluster can write and read Parquet on HDFS.

Run with `make spark-smoke` (spark-submit on the Spark master, executors on the worker).

The data is SYNTHETIC: 10,000 generated rows that exercise the pipeline mechanics only.
It is not collected news and must never appear in a report as such.
"""

from __future__ import annotations

import json
import sys
import time
from datetime import UTC, datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS_ROOT = "hdfs://namenode:8020/sportsbot/_smoke"
ROWS = 10_000
SPORTS = ["football", "volleyball", "basketball", "tennis", "badminton"]


def main() -> int:
    run_id = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    path = f"{HDFS_ROOT}/run={run_id}"
    spark = SparkSession.builder.appName("sportsbot-hdfs-smoke").getOrCreate()
    started = time.perf_counter()

    sports = F.array(*[F.lit(s) for s in SPORTS])
    df = (
        spark.range(ROWS)
        .withColumn("sport_category", sports[(F.col("id") % len(SPORTS)).cast("int")])
        .withColumn("title", F.concat(F.lit("synthetic smoke row "), F.col("id")))
        .withColumn("content_hash", F.sha2(F.col("title"), 256))
        .withColumn("origin", F.lit("synthetic"))
    )
    df.write.mode("overwrite").partitionBy("sport_category").parquet(path)

    back = spark.read.parquet(path)
    count = back.count()
    per_sport = {
        r["sport_category"]: r["count"] for r in back.groupBy("sport_category").count().collect()
    }
    elapsed = round(time.perf_counter() - started, 2)

    result = {
        "job": "hdfs_smoke",
        "data_origin": "synthetic",
        "path": path,
        "rows_written": ROWS,
        "rows_read": count,
        "per_sport": per_sport,
        "spark_master": spark.sparkContext.master,
        "spark_version": spark.version,
        # Block managers = executors + the driver.
        "block_managers": spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size(),
        "seconds": elapsed,
        "ok": count == ROWS and len(per_sport) == len(SPORTS),
    }
    print("SMOKE_RESULT " + json.dumps(result, sort_keys=True))
    spark.stop()
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
