# Pipeline workers image: collector/discovery, extractor and the Kafka consumers.
# Build context: repository root. One image, many commands (see docker-compose.yml).
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app/pipelines/src:/app/agent/src \
    SOURCES_FILE=/app/pipelines/sources.json \
    MIGRATIONS_DIR=/app/infra/postgres/migrations \
    LOCAL_BRONZE_DIR=/app/data/bronze

WORKDIR /app

COPY pipelines/requirements.txt pipelines/requirements.txt
COPY agent/requirements.txt agent/requirements.txt
RUN pip install -r pipelines/requirements.txt -r agent/requirements.txt

COPY pipelines/src pipelines/src
COPY pipelines/sources.json pipelines/sources.json
COPY agent/src agent/src
COPY infra/postgres/migrations infra/postgres/migrations

# The data dir exists in the image so a named volume mounted there is writable by the user.
RUN useradd --create-home --uid 10001 collector && mkdir -p /app/data/bronze \
    && chown -R collector /app/data
USER collector

# Default: the RSS scheduler without Kafka. Streaming services override the command.
ENTRYPOINT ["python", "-m", "sportsbot_pipelines"]
CMD ["schedule"]
