"""Gold job (plan task 6, docs/handoff/VNP.md, docs/TASKS.md V2): Silver -> statistics tables.

Reads the Silver articles and rejected rows written by silver_articles.py and writes small
Parquet tables for the report under hdfs://namenode:8020/sportsbot/gold/stats/. Prints one
`GOLD_REPORT {json}` line on stdout (Spark logs go to stderr) and stores the same JSON under
gold/_reports/ on HDFS.

Must stay compatible with Python 3.10 (the Spark image's Python) and uses Spark SQL only (the
Spark image has no numpy). Run (cluster), after the Silver job on the same snapshot:

    docker compose --profile bigdata exec -T spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/sportsbot/pipelines/spark/jobs/gold_stats.py --snapshot "<kaggle version, time>"

RULES, fixed before the first reported run (never tuned after seeing results):

daily_counts   articles per source_id x language x day. day = UTC date of published_at (the
               publisher's date), because ingest_date only says when the crawler or a backfill
               fetched the page. Articles without published_at are counted with day = null,
               never dropped and never given another date.
duplicates     per source_id and one row "_all":
               exact_duplicates      Silver rejected rows with reason exact_duplicate
               exact_duplicate_rate  exact_duplicates / (silver_articles + exact_duplicates)
               near_dup_articles     Silver articles whose story cluster has >= 2 articles
               near_duplicate_rate   near_dup_articles / silver_articles
               excluded_rss_summary  articles not compared for near duplicates (extractor
                                     rss_summary, see silver_articles.py)
cluster_sizes  number of story clusters and articles per cluster size (1 = no near duplicate).
freshness      lag = extracted_at - published_at in hours, per discovered_via (rss, sitemap,
               listing) and "_all". RSS lag measures how fast the pipeline picks up news;
               sitemap/listing lag mostly measures how old the backfilled articles were.
               Rows without published_at are counted (missing_published_at), not used.
               Negative lags (published after extraction: wrong publisher clock or time zone)
               are counted (negative_lag) and left out of the percentiles.
               Percentiles p50, p90, p99 are exact (SQL percentile), not approximate.
Checks         sum of daily_counts.articles = sum of cluster_sizes.articles = Silver rows;
               the job exits with code 1 when a check fails.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone

from pyspark.errors import AnalysisException
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

HDFS = "hdfs://namenode:8020/sportsbot"
DEFAULT_SILVER = f"{HDFS}/silver/articles"
DEFAULT_REJECTED = f"{HDFS}/silver/_rejected"
DEFAULT_OUTPUT = f"{HDFS}/gold/stats"
DEFAULT_REPORTS = f"{HDFS}/gold/_reports"

ALL = "_all"
RSS_SUMMARY_EXTRACTOR = "rss_summary"
PERCENTILES = (0.5, 0.9, 0.99)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--snapshot",
        required=True,
        help="Snapshot the Silver data comes from: Kaggle version and export-summary.json time",
    )
    parser.add_argument("--silver", default=DEFAULT_SILVER)
    parser.add_argument("--rejected", default=DEFAULT_REJECTED)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--reports", default=DEFAULT_REPORTS)
    return parser.parse_args(argv)


def rate(numerator: str, denominator: str) -> Column:
    """numerator / denominator, null when the denominator is 0."""
    return F.when(F.col(denominator) > 0, F.col(numerator) / F.col(denominator))


def read_rejected(spark: SparkSession, path: str) -> tuple[DataFrame, bool]:
    """Silver's rejected rows; (empty frame, False) when Silver rejected nothing.

    Spark writes no Parquet file for an empty partitioned DataFrame (only _SUCCESS), so the
    folder of a run without rejected rows cannot be read back.
    """
    try:
        return spark.read.parquet(path), True
    except AnalysisException:
        return spark.createDataFrame([], "source_id string, reason string"), False


def daily_counts(articles: DataFrame) -> DataFrame:
    return (
        articles.withColumn("day", F.to_date(F.to_timestamp("published_at")))
        .groupBy("source_id", "language", "day")
        .agg(F.count("*").alias("articles"))
    )


def cluster_sizes(articles: DataFrame) -> tuple[DataFrame, DataFrame]:
    """(article_id -> cluster_size) and the size distribution."""
    sizes = articles.groupBy("story_cluster_id").agg(F.count("*").alias("cluster_size"))
    per_article = articles.select("article_id", "story_cluster_id").join(sizes, "story_cluster_id")
    distribution = sizes.groupBy("cluster_size").agg(
        F.count("*").alias("clusters"), F.sum("cluster_size").alias("articles")
    )
    return per_article.select("article_id", "cluster_size"), distribution


def duplicates(articles: DataFrame, rejected: DataFrame, per_article: DataFrame) -> DataFrame:
    """Exact and near-duplicate counts and rates per source and for everything ("_all")."""
    flagged = articles.join(per_article, "article_id").select(
        "source_id",
        F.lit(1).alias("silver"),
        (F.col("cluster_size") >= 2).cast("int").alias("near_dup"),
        (F.coalesce(F.col("extractor"), F.lit("")) == RSS_SUMMARY_EXTRACTOR)
        .cast("int")
        .alias("rss_summary"),
        F.lit(0).alias("exact"),
    )
    exact = rejected.where(F.col("reason") == "exact_duplicate").select(
        F.coalesce(F.col("source_id"), F.lit("unknown")).alias("source_id"),
        F.lit(0).alias("silver"),
        F.lit(0).alias("near_dup"),
        F.lit(0).alias("rss_summary"),
        F.lit(1).alias("exact"),
    )
    rows = flagged.unionByName(exact)
    rows = rows.unionByName(rows.withColumn("source_id", F.lit(ALL)))
    return (
        rows.groupBy("source_id")
        .agg(
            F.sum("silver").alias("silver_articles"),
            F.sum("exact").alias("exact_duplicates"),
            F.sum("near_dup").alias("near_dup_articles"),
            F.sum("rss_summary").alias("excluded_rss_summary"),
        )
        .withColumn("all_rows", F.col("silver_articles") + F.col("exact_duplicates"))
        .withColumn("exact_duplicate_rate", rate("exact_duplicates", "all_rows"))
        .withColumn("near_duplicate_rate", rate("near_dup_articles", "silver_articles"))
        .drop("all_rows")
    )


def freshness(articles: DataFrame) -> DataFrame:
    """Extraction lag percentiles in hours per discovered_via and for everything ("_all")."""
    lag_hours = (
        F.unix_timestamp(F.to_timestamp("extracted_at"))
        - F.unix_timestamp(F.to_timestamp("published_at"))
    ) / 3600.0
    rows = articles.select(
        F.coalesce(F.col("discovered_via"), F.lit("unknown")).alias("discovered_via"),
        lag_hours.alias("lag_hours"),
    )
    rows = rows.unionByName(rows.withColumn("discovered_via", F.lit(ALL)))
    usable = F.col("lag_hours") >= 0
    pct = ", ".join(str(p) for p in PERCENTILES)
    stats = rows.groupBy("discovered_via").agg(
        F.count("*").alias("articles"),
        F.sum(F.col("lag_hours").isNull().cast("int")).alias("missing_published_at"),
        F.sum((F.col("lag_hours") < 0).cast("int")).alias("negative_lag"),
        F.sum(usable.cast("int")).alias("lag_articles"),
        F.expr(f"percentile(CASE WHEN lag_hours >= 0 THEN lag_hours END, array({pct}))").alias("p"),
    )
    return stats.select(
        "discovered_via",
        "articles",
        "missing_published_at",
        "negative_lag",
        "lag_articles",
        *[
            F.round(F.col("p")[i], 2).alias(f"lag_p{int(q * 100)}_hours")
            for i, q in enumerate(PERCENTILES)
        ],
    )


def rows_as_dicts(df: DataFrame, order: str) -> list[dict]:
    """A small aggregated table for the run report (tens of rows at most)."""
    return [r.asDict() for r in df.orderBy(order).collect()]


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    # timezone.utc, not datetime.UTC: the Spark image runs Python 3.10.
    started = datetime.now(tz=timezone.utc)  # noqa: UP017
    run_id = started.strftime("%Y%m%dT%H%M%SZ")
    clock = time.perf_counter()

    spark = (
        SparkSession.builder.appName("sportsbot-gold-stats")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    articles = spark.read.parquet(args.silver).cache()
    rejected, rejected_found = read_rejected(spark, args.rejected)
    silver_rows = articles.count()

    tables = {}
    tables["daily_counts"] = daily_counts(articles)
    per_article, tables["cluster_sizes"] = cluster_sizes(articles)
    tables["duplicates"] = duplicates(articles, rejected, per_article)
    tables["freshness"] = freshness(articles)

    for name, table in tables.items():
        # Gold tables are small: one file each.
        table.coalesce(1).write.mode("overwrite").parquet(f"{args.output}/{name}")
    gold = {name: spark.read.parquet(f"{args.output}/{name}").cache() for name in tables}

    daily_total = gold["daily_counts"].agg(F.sum("articles")).first()[0] or 0
    cluster_total = gold["cluster_sizes"].agg(F.sum("articles")).first()[0] or 0
    overall = gold["duplicates"].where(F.col("source_id") == ALL).first()
    checks = {
        "daily_counts_sum_equals_silver": daily_total == silver_rows,
        "cluster_sizes_sum_equals_silver": cluster_total == silver_rows,
        "duplicates_all_equals_silver": bool(overall) and overall["silver_articles"] == silver_rows,
    }
    report = {
        "job": "gold_stats",
        "run_id": run_id,
        "started_at": started.isoformat(),
        "snapshot_file": args.snapshot,
        "data_origin": "collected by SportsBot",
        "input": args.silver,
        "output": args.output,
        "master": spark.sparkContext.master,
        "spark_version": spark.version,
        "silver_rows": silver_rows,
        "rejected_rows": rejected.count(),
        "rejected_files_found": rejected_found,
        "sources": articles.select("source_id").distinct().count(),
        "articles_by_language": {
            r["language"]: r["count"] for r in articles.groupBy("language").count().collect()
        },
        "daily_counts_rows": gold["daily_counts"].count(),
        "articles_without_published_day": gold["daily_counts"]
        .where(F.col("day").isNull())
        .agg(F.sum("articles"))
        .first()[0]
        or 0,
        "duplicates_all": overall.asDict() if overall else None,
        "cluster_sizes": rows_as_dicts(gold["cluster_sizes"], "cluster_size"),
        "freshness": rows_as_dicts(gold["freshness"], "discovered_via"),
        "checks": checks,
        "checks_ok": all(checks.values()),
        "duration_seconds": round(time.perf_counter() - clock, 2),
    }
    text = json.dumps(report, ensure_ascii=False, sort_keys=True, default=str)
    spark.createDataFrame([(text,)], "value string").coalesce(1).write.mode("overwrite").text(
        f"{args.reports}/gold-{run_id}"
    )
    print("GOLD_REPORT " + text, flush=True)
    spark.stop()
    return 0 if report["checks_ok"] else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
