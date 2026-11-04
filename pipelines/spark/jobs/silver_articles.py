"""Silver job (plan task 6, docs/handoff/VNP.md, docs/TASKS.md V1): Bronze articles -> Silver.

Reads the extracted articles archived by the streaming pipeline (Bronze, JSON Lines), cleans
them, removes exact duplicates, groups near-duplicates into story clusters with MinHashLSH and
writes Parquet plus the rejected rows. Prints one `SILVER_REPORT {json}` line on stdout (Spark
logs go to stderr) and stores the same JSON next to the output on HDFS.

Must stay compatible with Python 3.10 (the Spark image's Python). Needs numpy in the Spark image
for pyspark.ml (checked at start). Run (cluster):

    docker compose --profile bigdata exec -T spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/sportsbot/pipelines/spark/jobs/silver_articles.py --snapshot "<kaggle version, time>"

RULES, fixed before the first reported run (never tuned after seeing results):

Input   hdfs://namenode:8020/sportsbot/bronze/source=extracted/*/*/*.jsonl.gz, read with the
        explicit `extracted.v1` schema below (no inference). `source=<feed id>/` folders (raw RSS
        records) are not read.
Reject  (first matching reason, written to silver/_rejected/ partitioned by `reason`):
        malformed_json        the line is not a JSON object of the schema
        missing_article_id    no article_id
        missing_title         title empty after cleaning
        missing_text          text empty after cleaning
        bad_extracted_at      extracted_at missing or not a timestamp
        missing_content_hash  no content_hash (written by the extractor for every article)
        exact_duplicate       same content_hash as an earlier row: the row with the earliest
                              extracted_at is kept (ties: smallest article_id)
        duplicate_article_id  same article_id as an earlier kept row with a different text (an
                              article extracted twice): the earliest extracted_at is kept.
                              Added because article_id must be unique in Silver.
        Every input row ends in exactly one place: input_rows = output_rows + rejected_rows.
Clean   title and text: Unicode NFC, whitespace collapsed, stripped (textnorm.normalize_text).
        All original columns are kept. Added: ingest_date (UTC date of extracted_at),
        story_cluster_id, and `language` falls back to language_hint, then "und" (ISO 639
        "undetermined"), because it is a partition column.
Near    MinHashLSH (numHashTables = 5, fixed seed) on word 3-shingles of the lower-cased,
dups    accent-preserving text (textnorm.word_shingles), hashed into 2^20 binary features.
        A pair is the same story when the exact Jaccard similarity of the two shingle sets is
        >= 0.8 (LSH only proposes candidates; every pair is verified on the shingle arrays).
        Before shingling, a copy of the title at the start of the text is removed
        (textnorm.strip_leading_title: VnExpress repeats the title in the text).
        Excluded from near-duplicate detection, counted in the report: rows with
        extractor = "rss_summary" (BBC feed summaries only, too short to compare) and rows
        without any word to shingle. They keep their own cluster.
Cluster story_cluster_id = smallest article_id in the connected group of near-duplicate pairs
        (connected components by min-label propagation), or the article's own id.
Output  hdfs://namenode:8020/sportsbot/silver/articles/ Parquet, partitioned by language and
        ingest_date, overwritten on each run so a rerun on the same snapshot gives the same data.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

JOBS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, JOBS_DIR)

import textnorm  # noqa: E402

HDFS = "hdfs://namenode:8020/sportsbot"
DEFAULT_INPUT = f"{HDFS}/bronze/source=extracted/*/*/*.jsonl.gz"
DEFAULT_OUTPUT = f"{HDFS}/silver/articles"
DEFAULT_REJECTED = f"{HDFS}/silver/_rejected"
DEFAULT_REPORTS = f"{HDFS}/silver/_reports"

RSS_SUMMARY_EXTRACTOR = "rss_summary"
HASHING_FEATURES = 1 << 20
MINHASH_SEED = 20261105
MAX_COMPONENT_ITERATIONS = 50
CORRUPT = "_corrupt_record"

# The `articles.extracted` message (extracted.v1): discovery fields
# (sportsbot_pipelines/discovery/publish.py) plus extraction fields (extraction/worker.py).
# `tags` is a JSON array in Bronze; read as a string it keeps the raw JSON, so a type surprise
# can never turn a whole article into a malformed row.
BRONZE_SCHEMA = T.StructType(
    [
        T.StructField("schema", T.StringType()),
        T.StructField("article_id", T.StringType()),
        T.StructField("source_id", T.StringType()),
        T.StructField("source_name", T.StringType()),
        T.StructField("language_hint", T.StringType()),
        T.StructField("url", T.StringType()),
        T.StructField("canonical_url", T.StringType()),
        T.StructField("title", T.StringType()),
        T.StructField("summary_html", T.StringType()),
        T.StructField("published_at", T.StringType()),
        T.StructField("published_raw", T.StringType()),
        T.StructField("tags", T.StringType()),
        T.StructField("discovered_via", T.StringType()),
        T.StructField("discovered_at", T.StringType()),
        T.StructField("run_id", T.StringType()),
        T.StructField("text", T.StringType()),
        T.StructField("author", T.StringType()),
        T.StructField("language", T.StringType()),
        T.StructField("word_count", T.LongType()),
        T.StructField("content_hash", T.StringType()),
        T.StructField("extractor", T.StringType()),
        T.StructField("extracted_at", T.StringType()),
        T.StructField(CORRUPT, T.StringType()),
    ]
)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--snapshot",
        required=True,
        help="Snapshot the Bronze files come from: Kaggle version and export-summary.json time",
    )
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--rejected", default=DEFAULT_REJECTED)
    parser.add_argument("--reports", default=DEFAULT_REPORTS)
    return parser.parse_args(argv)


def read_bronze(spark: SparkSession, path: str) -> DataFrame:
    """Bronze JSON Lines with the fixed schema; unparseable lines keep their text in CORRUPT."""
    df = (
        spark.read.schema(BRONZE_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT)
        .json(path)
    )
    # Folder names (ingest_date=..., ingest_hour=...) may be added as partition columns; Silver
    # derives ingest_date from extracted_at instead, so drop them.
    return df.drop("ingest_date", "ingest_hour").withColumn("bronze_file", F.input_file_name())


def clean(df: DataFrame) -> DataFrame:
    """Cleaned title/text, parsed extraction time, ingest_date, partition-safe language."""
    normalize = F.udf(textnorm.normalize_text, T.StringType())
    extracted_ts = F.to_timestamp("extracted_at")
    return (
        df.withColumn("title", normalize("title"))
        .withColumn("text", normalize("text"))
        .withColumn("extracted_ts", extracted_ts)
        .withColumn("ingest_date", F.to_date(extracted_ts))
        .withColumn("language", F.coalesce("language", "language_hint", F.lit("und")))
    )


def with_reason(df: DataFrame) -> DataFrame:
    """`reason` = first rule the row breaks, or null for a row that goes to Silver."""
    valid = (
        F.col(CORRUPT).isNull()
        & F.col("article_id").isNotNull()
        & (F.col("title") != "")
        & (F.col("text") != "")
        & F.col("extracted_ts").isNotNull()
        & F.col("content_hash").isNotNull()
    )
    # Rows are ranked only among valid rows (the flag is part of the window key), so an invalid
    # row can never push a valid one down to "duplicate". bronze_file breaks the last ties, so
    # the kept row is the same on every run.
    df = df.withColumn("is_valid", valid)
    first_by_hash = Window.partitionBy("content_hash", "is_valid").orderBy(
        "extracted_ts", "article_id", "bronze_file"
    )
    df = df.withColumn("hash_rank", F.when(valid, F.row_number().over(first_by_hash)))
    # Ranked again among the rows that survive exact de-duplication only.
    df = df.withColumn("survives", valid & (F.col("hash_rank") == 1))
    first_by_id = Window.partitionBy("article_id", "survives").orderBy(
        "extracted_ts", "content_hash", "bronze_file"
    )
    df = df.withColumn("id_rank", F.when(F.col("survives"), F.row_number().over(first_by_id)))
    reason = (
        F.when(F.col(CORRUPT).isNotNull(), "malformed_json")
        .when(F.col("article_id").isNull(), "missing_article_id")
        .when(F.coalesce(F.col("title"), F.lit("")) == "", "missing_title")
        .when(F.coalesce(F.col("text"), F.lit("")) == "", "missing_text")
        .when(F.col("extracted_ts").isNull(), "bad_extracted_at")
        .when(F.col("content_hash").isNull(), "missing_content_hash")
        .when(F.col("hash_rank") > 1, "exact_duplicate")
        .when(F.col("id_rank") > 1, "duplicate_article_id")
    )
    return df.withColumn("reason", reason).drop("is_valid", "hash_rank", "survives", "id_rank")


def near_duplicate_pairs(kept: DataFrame) -> tuple[DataFrame, dict]:
    """Verified near-duplicate pairs (id_a < id_b, jaccard >= threshold) and their counts."""
    from pyspark.ml.feature import HashingTF, MinHashLSH  # needs numpy in the Spark image

    shingle = F.udf(
        lambda text, title: textnorm.word_shingles(textnorm.strip_leading_title(text, title)),
        T.ArrayType(T.StringType()),
    )
    eligible = kept.where(F.coalesce(F.col("extractor"), F.lit("")) != RSS_SUMMARY_EXTRACTOR)
    with_shingles = eligible.select("article_id", shingle("text", "title").alias("shingles"))
    candidates = with_shingles.where(F.size("shingles") > 0).cache()

    counts = {
        "near_dup_excluded_rss_summary": kept.count() - eligible.count(),
        "near_dup_excluded_no_words": with_shingles.where(F.size("shingles") == 0).count(),
        "near_dup_compared_rows": candidates.count(),
    }

    tf = HashingTF(
        inputCol="shingles", outputCol="features", numFeatures=HASHING_FEATURES, binary=True
    )
    features = tf.transform(candidates)
    lsh = MinHashLSH(
        inputCol="features",
        outputCol="hashes",
        numHashTables=textnorm.MINHASH_NUM_HASH_TABLES,
        seed=MINHASH_SEED,
    )
    model = lsh.fit(features)
    # The join keeps pairs whose distance on the hashed features is below the threshold. A small
    # margin above 1 - 0.8 keeps pairs exactly at 0.8 and pairs nudged by hash collisions; the
    # exact check below decides.
    proposed = (
        model.approxSimilarityJoin(
            features,
            features,
            1.0 - textnorm.NEAR_DUPLICATE_JACCARD + 0.05,
            distCol="hashed_distance",
        )
        .select(
            F.col("datasetA.article_id").alias("id_a"),
            F.col("datasetB.article_id").alias("id_b"),
            F.col("datasetA.shingles").alias("sh_a"),
            F.col("datasetB.shingles").alias("sh_b"),
        )
        .where(F.col("id_a") < F.col("id_b"))  # each unordered pair once, no self-pairs
        .cache()
    )
    exact = F.size(F.array_intersect("sh_a", "sh_b")) / F.size(F.array_union("sh_a", "sh_b"))
    pairs = (
        proposed.withColumn("jaccard", exact)
        .where(F.col("jaccard") >= textnorm.NEAR_DUPLICATE_JACCARD)
        .select("id_a", "id_b", "jaccard")
        .cache()
    )
    counts["lsh_candidate_pairs"] = proposed.count()
    counts["near_duplicate_pairs"] = pairs.count()
    proposed.unpersist()
    candidates.unpersist()
    return pairs, counts


def story_clusters(pairs: DataFrame) -> tuple[DataFrame, int]:
    """(article_id, story_cluster_id) for every article in a pair: min label over its component.

    Min-label propagation: each article starts with its own id; at each round it takes the
    smallest label among itself and its neighbours. It stops when no label changes, after at
    most the component's diameter rounds.
    """
    edges = pairs.select(F.col("id_a").alias("src"), F.col("id_b").alias("dst"))
    edges = edges.union(edges.select(F.col("dst").alias("src"), F.col("src").alias("dst")))
    edges = edges.cache()
    labels = edges.select(F.col("src").alias("article_id")).distinct()
    labels = labels.withColumn("label", F.col("article_id")).localCheckpoint()

    for iteration in range(1, MAX_COMPONENT_ITERATIONS + 1):
        neighbour_min = (
            edges.join(labels, edges["dst"] == labels["article_id"])
            .groupBy("src")
            .agg(F.min("label").alias("neighbour_label"))
        )
        updated = (
            labels.join(neighbour_min, labels["article_id"] == neighbour_min["src"], "left")
            .select(
                "article_id",
                F.least("label", F.coalesce("neighbour_label", "label")).alias("label"),
            )
            .localCheckpoint()
        )
        changed = (
            updated.alias("new")
            .join(labels.alias("old"), "article_id")
            .where(F.col("new.label") != F.col("old.label"))
            .count()
        )
        labels = updated
        if changed == 0:
            edges.unpersist()
            return labels.withColumnRenamed("label", "story_cluster_id"), iteration
    raise RuntimeError(f"connected components did not converge in {MAX_COMPONENT_ITERATIONS}")


def main(argv: list[str]) -> int:
    try:
        import numpy  # noqa: F401  (pyspark.ml imports it)
    except ImportError:
        print("numpy is missing in the Spark image; pyspark.ml needs it", file=sys.stderr)
        return 2

    args = parse_args(argv)
    # timezone.utc, not datetime.UTC: the Spark image runs Python 3.10.
    started = datetime.now(tz=timezone.utc)  # noqa: UP017
    run_id = started.strftime("%Y%m%dT%H%M%SZ")
    clock = time.perf_counter()

    spark = (
        SparkSession.builder.appName("sportsbot-silver-articles")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    # Executors need the helpers for the UDFs.
    spark.sparkContext.addPyFile(os.path.join(JOBS_DIR, "textnorm.py"))

    bronze = read_bronze(spark, args.input)
    rows = with_reason(clean(bronze)).cache()
    input_rows = rows.count()

    kept = rows.where(F.col("reason").isNull()).drop("reason", CORRUPT)
    rejected = rows.where(F.col("reason").isNotNull())

    pairs, near_counts = near_duplicate_pairs(kept)
    clusters, iterations = story_clusters(pairs)
    silver = (
        kept.join(clusters, "article_id", "left")
        .withColumn("story_cluster_id", F.coalesce("story_cluster_id", "article_id"))
        .drop("extracted_ts")
    )

    # One shuffle so each (language, ingest_date) folder gets one file, not one per task.
    (
        silver.repartition("language", "ingest_date")
        .write.mode("overwrite")
        .partitionBy("language", "ingest_date")
        .parquet(args.output)
    )
    rejected.drop("extracted_ts").write.mode("overwrite").partitionBy("reason").parquet(
        args.rejected
    )

    written = spark.read.parquet(args.output)
    output_rows = written.count()
    rejected_by_reason = {
        r["reason"]: r["count"] for r in rows.groupBy("reason").count().collect() if r["reason"]
    }
    rejected_rows = sum(rejected_by_reason.values())
    cluster_sizes = written.groupBy("story_cluster_id").count()
    report = {
        "job": "silver_articles",
        "run_id": run_id,
        "started_at": started.isoformat(),
        "snapshot_file": args.snapshot,
        "data_origin": "collected by SportsBot",
        "input": args.input,
        "output": args.output,
        "master": spark.sparkContext.master,
        "spark_version": spark.version,
        "input_rows": input_rows,
        "output_rows": output_rows,
        "rejected_rows": rejected_rows,
        "rejected_by_reason": rejected_by_reason,
        "exact_duplicates": rejected_by_reason.get("exact_duplicate", 0),
        "near_duplicate_pairs": near_counts["near_duplicate_pairs"],
        "clusters": cluster_sizes.count(),
        "multi_article_clusters": cluster_sizes.where(F.col("count") > 1).count(),
        "articles_in_multi_article_clusters": (
            cluster_sizes.where(F.col("count") > 1).agg(F.sum("count")).first()[0] or 0
        ),
        "component_iterations": iterations,
        **{k: v for k, v in near_counts.items() if k != "near_duplicate_pairs"},
        "accounting_ok": input_rows == output_rows + rejected_rows,
        "rules": {
            "shingle_size": textnorm.SHINGLE_SIZE,
            "jaccard_threshold": textnorm.NEAR_DUPLICATE_JACCARD,
            "minhash_num_hash_tables": textnorm.MINHASH_NUM_HASH_TABLES,
            "minhash_seed": MINHASH_SEED,
            "hashing_features": HASHING_FEATURES,
        },
        "duration_seconds": round(time.perf_counter() - clock, 2),
    }
    text = json.dumps(report, ensure_ascii=False, sort_keys=True)
    spark.createDataFrame([(text,)], "value string").coalesce(1).write.mode("overwrite").text(
        f"{args.reports}/silver-{run_id}"
    )
    print("SILVER_REPORT " + text, flush=True)
    spark.stop()
    return 0 if report["accounting_ok"] else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
