from __future__ import annotations

import asyncio

from helpers import failing, ok

from app.services.health import Check


def test_liveness_needs_no_dependencies(client_factory):
    client = client_factory(checks=[Check("postgres", failing(), required=True)])
    resp = client.get("/health/live")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_all_checks_ok(client_factory):
    client = client_factory(
        checks=[Check("postgres", ok("schema 001"), True), Check("qdrant", ok(), True)]
    )
    resp = client.get("/health")
    body = resp.json()
    assert resp.status_code == 200
    assert body["status"] == "ok"
    assert body["checks"]["postgres"]["detail"] == "schema 001"
    assert body["environment"] == "test"


def test_required_failure_returns_503(client_factory):
    client = client_factory(
        checks=[Check("postgres", failing("refused"), True), Check("qdrant", ok(), True)]
    )
    resp = client.get("/health")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "error"
    assert body["checks"]["postgres"]["status"] == "error"
    assert "refused" in body["checks"]["postgres"]["detail"]


def test_optional_failure_is_degraded_not_503(client_factory):
    client = client_factory(
        checks=[Check("postgres", ok(), True), Check("hdfs", failing(), required=False)]
    )
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "degraded"


def test_unconfigured_check_is_skipped(client_factory):
    client = client_factory(checks=[Check("postgres", ok(), True), Check("hdfs", None, False)])
    body = client.get("/health").json()
    assert body["status"] == "ok"
    assert body["checks"]["hdfs"]["status"] == "skipped"


def test_slow_check_times_out(client_factory):
    async def slow() -> str:
        await asyncio.sleep(5)
        return "late"

    client = client_factory(checks=[Check("qdrant", slow, True)], health_timeout_seconds=0.1)
    resp = client.get("/health")
    assert resp.status_code == 503
    assert "timed out" in resp.json()["checks"]["qdrant"]["detail"]


def test_default_checks_fail_cleanly_without_services(client_factory):
    # Real check functions against closed ports: must report errors, not crash.
    client = client_factory()
    resp = client.get("/health")
    assert resp.status_code == 503
    checks = resp.json()["checks"]
    assert checks["postgres"]["status"] == "error"
    assert checks["qdrant"]["status"] == "error"
    assert checks["hdfs"]["status"] == "skipped"
