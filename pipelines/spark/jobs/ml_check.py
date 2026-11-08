"""Check that Spark MLlib's Python API works on the cluster (numpy on the driver and executors).

Run: make spark-ml-check (also a CI step). Prints one JSON line with "ok": true.
"""

from __future__ import annotations

import json

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName("sportsbot-ml-check").getOrCreate()
    rows = [("messi ghi ban", 1.0), ("djokovic thang set", 0.0)] * 4
    df = spark.createDataFrame(rows, ["text", "label"])
    words = Tokenizer(inputCol="text", outputCol="words").transform(df)
    features = HashingTF(inputCol="words", outputCol="features", numFeatures=64).transform(words)
    model = LogisticRegression(maxIter=5).fit(features)
    # numpy imported inside a task: proves the executors (on the worker) have it too
    executor_numpy = (
        spark.sparkContext.parallelize([0], 1)
        .map(lambda _: __import__("numpy").__version__)
        .first()
    )
    import numpy

    print(
        json.dumps(
            {
                "ok": model.numFeatures == 64,
                "driver_numpy": numpy.__version__,
                "executor_numpy": executor_numpy,
            }
        )
    )
    spark.stop()


if __name__ == "__main__":
    main()
