# Collector image (scheduler + one-off commands). Build context: repository root.
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app/pipelines/src \
    SOURCES_FILE=/app/pipelines/sources.yaml \
    MIGRATIONS_DIR=/app/infra/postgres/migrations

WORKDIR /app

COPY pipelines/requirements.txt pipelines/requirements.txt
RUN pip install -r pipelines/requirements.txt

COPY pipelines/src pipelines/src
COPY pipelines/sources.yaml pipelines/sources.yaml
COPY infra/postgres/migrations infra/postgres/migrations

RUN useradd --create-home --uid 10001 collector
USER collector

# Default: run forever, each source on its own interval. One-off: `collect`, `sources check`.
ENTRYPOINT ["python", "-m", "sportsbot_pipelines"]
CMD ["schedule"]
