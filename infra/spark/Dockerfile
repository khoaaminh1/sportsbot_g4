# Spark image with numpy, so pyspark.ml (Spark MLlib) works on the driver and the executors.
# The stock apache/spark Python image has no numpy; MLlib's Python API needs it (plan task 7:
# TF-IDF + logistic regression). Used by spark-master and spark-worker in docker-compose.yml.
ARG SPARK_IMAGE=apache/spark:3.5.3-scala2.12-java17-python3-ubuntu
FROM ${SPARK_IMAGE}

USER root
# numpy 1.26 supports the image's Python 3.10; pinned for repeatable runs.
RUN python3 -m pip install --no-cache-dir numpy==1.26.4
USER spark
