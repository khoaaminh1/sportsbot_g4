# Backend image. Build context is the repository root (see docker-compose.yml).
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY backend/requirements.txt backend/requirements.txt
COPY agent/requirements.txt agent/requirements.txt
RUN pip install -r backend/requirements.txt -r agent/requirements.txt

COPY backend/src backend/src
# The RAG code (agent/) and the demo page (frontend/, served at /app/).
COPY agent/src agent/src
COPY frontend frontend
ENV PYTHONPATH=/app/agent/src

# Run as a non-root user.
RUN useradd --create-home --uid 10001 appuser
USER appuser

EXPOSE 8000

# Liveness only: readiness (dependencies) is GET /health.
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=6 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health/live', timeout=2).status == 200 else 1)"

CMD ["uvicorn", "app.main:app", "--app-dir", "backend/src", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers"]
