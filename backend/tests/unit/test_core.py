from __future__ import annotations

import json
import logging

from fastapi import APIRouter
from helpers import make_settings, ok

from app.core.errors import AppError
from app.core.logging import JsonFormatter, RequestIdFilter, request_id_var
from app.services.health import Check


def test_cors_origins_from_comma_separated_env(monkeypatch):
    monkeypatch.setenv("CORS_ORIGINS", "http://a.test, http://b.test")
    settings = make_settings()
    # Explicit kwargs win over env in make_settings only for the keys it sets; CORS comes from env.
    assert settings.cors_origins == ["http://a.test", "http://b.test"]


def test_trailing_slash_removed():
    settings = make_settings(qdrant_url="http://q:6333/", hdfs_webhdfs_url="http://nn:9870/")
    assert settings.qdrant_url == "http://q:6333"
    assert settings.hdfs_webhdfs_url == "http://nn:9870"


def test_request_id_is_generated_and_echoed(client_factory):
    client = client_factory(checks=[Check("postgres", ok(), True)])
    generated = client.get("/health/live").headers["X-Request-ID"]
    assert len(generated) == 32
    echoed = client.get("/health/live", headers={"X-Request-ID": "abc-123"}).headers["X-Request-ID"]
    assert echoed == "abc-123"
    # Unsafe ids are replaced, not echoed back.
    replaced = client.get("/health/live", headers={"X-Request-ID": "bad id\n"}).headers
    assert replaced["X-Request-ID"] != "bad id\n"


def test_unknown_route_uses_error_schema(client_factory):
    client = client_factory(checks=[])
    resp = client.get("/nope", headers={"X-Request-ID": "rid-1"})
    assert resp.status_code == 404
    assert resp.json() == {
        "error": {"code": "not_found", "message": "Not Found", "request_id": "rid-1"}
    }


def test_app_error_and_unhandled_error(client_factory):
    client = client_factory(checks=[])
    router = APIRouter()

    @router.get("/raise-app")
    async def raise_app() -> None:
        raise AppError("source_unavailable", "Source is down", status_code=502, details={"id": 1})

    @router.get("/raise-bug")
    async def raise_bug() -> None:
        raise ValueError("secret internal detail")

    client.app.include_router(router)

    app_resp = client.get("/raise-app")
    assert app_resp.status_code == 502
    assert app_resp.json()["error"]["code"] == "source_unavailable"
    assert app_resp.json()["error"]["details"] == {"id": 1}

    bug_resp = client.get("/raise-bug")
    assert bug_resp.status_code == 500
    assert bug_resp.json()["error"]["code"] == "internal_error"
    assert "secret" not in bug_resp.text


def test_json_log_line_has_request_id_and_extras():
    record = logging.makeLogRecord(
        {"name": "t", "levelno": logging.INFO, "levelname": "INFO", "msg": "hello", "status": 200}
    )
    token = request_id_var.set("rid-42")
    try:
        RequestIdFilter().filter(record)
    finally:
        request_id_var.reset(token)
    line = json.loads(JsonFormatter().format(record))
    assert line["message"] == "hello"
    assert line["request_id"] == "rid-42"
    assert line["status"] == 200
