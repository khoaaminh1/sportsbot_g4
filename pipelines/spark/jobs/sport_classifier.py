"""Sport classifier (plan task 7): Spark MLlib baseline vs BGE-M3 vectors, on Silver articles.

Run on the cluster (make spark-classifier SNAPSHOT="<kaggle version, time>"):

    docker compose --profile bigdata exec -T spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 --driver-memory 2g \
        /opt/sportsbot/pipelines/spark/jobs/sport_classifier.py --snapshot "<...>"

Prints one `CLASSIFIER_REPORT {json}` line (saved to docs/reports/runs/ by the make target).

RULES, fixed before the first run:
- Input: Silver articles (hdfs:///sportsbot/silver/articles). Weak labels: sportlabels.weak_label
  (feed, then URL path, then title keyword for volleyball/badminton); unlabelled rows are not used.
- Test sets. Hand labels (hdfs:///sportsbot/labels/sport_test_v1.csv: article_id,sport_label,...)
  when the file exists: those articles never enter training. Always also a weak-label holdout:
  20 % of the weak-labelled rows (seeded split, seed 42), reported separately because its labels
  come from the same rules as the training labels.
- Baseline: RegexTokenizer (Unicode words, lower case) on title + first 2,000 characters of text
  -> HashingTF (2^18) -> IDF -> multinomial LogisticRegression (maxIter 50, regParam 0.01).
- Improved: the article's BGE-M3 vector already stored in Qdrant (chunk 0 of each article, the
  vector the chatbot searches with; 1024-d, normalised) -> the same LogisticRegression. Articles
  without a vector are left out of the improved model's numbers; the report gives the counts.
- Metrics: accuracy, macro-F1 over the classes present, per-class precision/recall/F1, confusion
  matrix, macro-F1 per language. Single machine, pseudo-distributed (Docker), not a production
  cluster.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import IDF, HashingTF, IndexToString, RegexTokenizer, StringIndexer
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

JOBS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, JOBS_DIR)
import sportlabels  # noqa: E402

HDFS = "hdfs://namenode:8020/sportsbot"
SEED = 42
HOLDOUT = 0.2
TEXT_CHARS = 2000
QDRANT_COLLECTION = "sportsbot_chunks"


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--snapshot", default="", help="Kaggle version and export time of the data")
    parser.add_argument("--silver", default=f"{HDFS}/silver/articles")
    parser.add_argument("--labels", default=f"{HDFS}/labels/sport_test_v1.csv")
    parser.add_argument("--qdrant", default="http://qdrant:6333")
    parser.add_argument("--no-vectors", action="store_true", help="baseline only")
    return parser.parse_args(argv)


# ------------------------------------------------------------------- data -----


def weak_labelled(spark: SparkSession, silver: str) -> DataFrame:
    schema = T.StructType(
        [T.StructField("weak_label", T.StringType()), T.StructField("weak_rule", T.StringType())]
    )

    @F.udf(returnType=schema)
    def label(source_id: str, url: str, title: str):  # type: ignore[no-untyped-def]
        hit = sportlabels.weak_label(source_id, url, title)
        return hit if hit else (None, None)

    df = spark.read.parquet(silver).select(
        "article_id", "source_id", "url", "title", "text", "language"
    )
    return (
        df.withColumn("w", label("source_id", "url", "title"))
        .select("*", "w.weak_label", "w.weak_rule")
        .drop("w")
    )


def hand_labels(spark: SparkSession, path: str) -> DataFrame | None:
    try:
        df = spark.read.option("header", True).csv(path)
    except Exception:  # no file: weak-label holdout only
        return None
    df = df.select("article_id", F.trim("sport_label").alias("hand_label")).where(
        F.col("hand_label").isin(*sportlabels.CLASSES)
    )
    return df if df.head(1) else None


def qdrant_vectors(spark: SparkSession, base_url: str) -> DataFrame:
    """article_id -> BGE-M3 vector of chunk 0, scrolled from Qdrant by the driver."""
    rows, offset = [], None
    url = f"{base_url.rstrip('/')}/collections/{QDRANT_COLLECTION}/points/scroll"
    while True:
        body = {
            "filter": {"must": [{"key": "chunk_index", "match": {"value": 0}}]},
            "limit": 256,
            "with_payload": ["article_id"],
            "with_vector": True,
        }
        if offset is not None:
            body["offset"] = offset
        req = urllib.request.Request(
            url, json.dumps(body).encode(), {"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            result = json.load(resp)["result"]
        for p in result["points"]:
            vec = p["vector"]
            if isinstance(vec, dict):  # named vectors
                vec = next(iter(vec.values()))
            rows.append((p["payload"]["article_id"], Vectors.dense(vec)))
        offset = result.get("next_page_offset")
        if offset is None:
            break
    schema = T.StructType(
        [T.StructField("article_id", T.StringType()), T.StructField("vector", VectorUDT())]
    )
    return spark.createDataFrame(rows, schema)


# ----------------------------------------------------------------- models -----


def evaluate(pred: DataFrame, true_col: str) -> dict[str, object]:
    rows = pred.select("language", true_col, "predicted").collect()
    pairs = [(r[true_col], r["predicted"]) for r in rows]
    out = sportlabels.scores(pairs)
    out["per_language"] = sportlabels.scores_by(
        (r["language"] or "und", r[true_col], r["predicted"]) for r in rows
    )
    return out


def fit_and_score(
    features_stages: list, train: DataFrame, tests: dict[str, tuple[DataFrame, str]]
) -> dict[str, object]:
    indexer = StringIndexer(inputCol="weak_label", outputCol="label", handleInvalid="keep")
    lr = LogisticRegression(
        featuresCol="features", labelCol="label", maxIter=50, regParam=0.01, family="multinomial"
    )
    t0 = time.perf_counter()
    model = Pipeline(stages=[*features_stages, indexer, lr]).fit(train)
    fit_s = round(time.perf_counter() - t0, 1)
    labels = model.stages[-2].labels
    to_name = IndexToString(inputCol="prediction", outputCol="predicted", labels=labels)
    out: dict[str, object] = {"train_rows": train.count(), "fit_seconds": fit_s, "classes": labels}
    for name, (df, true_col) in tests.items():
        out[name] = evaluate(to_name.transform(model.transform(df)), true_col)
    return out


def main(argv: list[str]) -> None:
    args = parse_args(argv)
    spark = SparkSession.builder.appName("sportsbot-sport-classifier").getOrCreate()
    spark.sparkContext.addPyFile(os.path.join(JOBS_DIR, "sportlabels.py"))  # for the label UDF
    started = datetime.now(timezone.utc)  # noqa: UP017 - the Spark image runs Python 3.10
    t0 = time.perf_counter()

    articles = (
        weak_labelled(spark, args.silver)
        .withColumn("doc", F.concat_ws(" ", "title", F.substring("text", 1, TEXT_CHARS)))
        .cache()
    )
    hand = hand_labels(spark, args.labels)
    labelled = articles.where(F.col("weak_label").isNotNull())
    if hand is not None:
        hand_rows = articles.join(hand, "article_id")
        labelled = labelled.join(hand.select("article_id"), "article_id", "left_anti")
    train, holdout = labelled.randomSplit([1 - HOLDOUT, HOLDOUT], seed=SEED)
    train, holdout = train.cache(), holdout.cache()

    counts = {
        "silver_rows": articles.count(),
        "weak_labelled": labelled.count(),
        "weak_by_class": {r[0]: r[1] for r in labelled.groupBy("weak_label").count().collect()},
        "weak_by_rule": {r[0]: r[1] for r in labelled.groupBy("weak_rule").count().collect()},
        "hand_labelled": hand_rows.count() if hand is not None else 0,
        "holdout_rows": holdout.count(),
    }

    text_stages = [
        RegexTokenizer(inputCol="doc", outputCol="tokens", pattern=r"\w+", gaps=False),
        HashingTF(inputCol="tokens", outputCol="tf", numFeatures=1 << 18),
        IDF(inputCol="tf", outputCol="features"),
    ]
    tests = {"weak_holdout": (holdout, "weak_label")}
    if hand is not None:
        tests["hand"] = (hand_rows, "hand_label")
    models = {"baseline_tfidf_lr": fit_and_score(text_stages, train, tests)}

    if not args.no_vectors:
        try:
            vectors = qdrant_vectors(spark, args.qdrant).withColumnRenamed("vector", "features")
            vec = lambda df: df.join(vectors, "article_id")  # noqa: E731
            vtests = {k: (vec(df), col) for k, (df, col) in tests.items()}
            improved = fit_and_score([], vec(train), vtests)
            improved["articles_with_vector"] = vectors.count()
            models["bge_m3_lr"] = improved
        except Exception as exc:  # Qdrant not reachable: report the baseline alone
            models["bge_m3_lr"] = {"error": f"{type(exc).__name__}: {exc}"[:300]}

    report = {
        "job": "sport_classifier",
        "run_id": started.strftime("%Y%m%dT%H%M%SZ"),
        "started_at": started.isoformat(),
        "snapshot": args.snapshot,
        "master": spark.sparkContext.master,
        "input": args.silver,
        "seed": SEED,
        "holdout_share": HOLDOUT,
        "counts": counts,
        "models": models,
        "seconds": round(time.perf_counter() - t0, 1),
        "note": "Weak labels from feed/URL/title rules (sportlabels.py). Hand labels, when "
        "present, are the only independent test set. Single machine, pseudo-distributed.",
    }
    print("CLASSIFIER_REPORT " + json.dumps(report, ensure_ascii=False))
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
