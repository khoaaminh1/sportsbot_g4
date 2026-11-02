"""Tests against the running core stack (`make up-core`, then `make test-integration`).

Skipped unless SPORTSBOT_INTEGRATION=1. CI runs them after booting the stack with Docker Compose.
"""

from __future__ import annotations

import os
import re

import httpx
import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("SPORTSBOT_INTEGRATION") != "1",
        reason="set SPORTSBOT_INTEGRATION=1 with the core stack running",
    ),
]

BACKEND_URL = os.environ.get("BACKEND_URL", "http://localhost:8000")
QDRANT_URL = os.environ.get("QDRANT_URL_HOST", "http://localhost:6333")


def test_backend_is_ready_with_real_dependencies():
    resp = httpx.get(f"{BACKEND_URL}/health", timeout=10)
    body = resp.json()
    assert resp.status_code == 200, body
    assert body["checks"]["postgres"]["status"] == "ok"
    assert body["checks"]["qdrant"]["status"] == "ok"


def test_migrations_applied():
    body = httpx.get(f"{BACKEND_URL}/health", timeout=10).json()
    assert re.fullmatch(r"schema \d{3}", body["checks"]["postgres"]["detail"]), body


def test_openapi_served():
    resp = httpx.get(f"{BACKEND_URL}/openapi.json", timeout=10)
    assert resp.status_code == 200
    assert "/health" in resp.json()["paths"]


@pytest.mark.skipif(
    os.environ.get("EXPECT_HDFS") != "1", reason="set EXPECT_HDFS=1 when the bigdata profile runs"
)
def test_backend_sees_hdfs():
    body = httpx.get(f"{BACKEND_URL}/health", timeout=10).json()
    assert body["checks"]["hdfs"]["status"] == "ok", body
    assert body["status"] == "ok"


def test_qdrant_reachable_from_host():
    assert httpx.get(f"{QDRANT_URL}/readyz", timeout=10).status_code == 200
