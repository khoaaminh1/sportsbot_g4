"""Shared test helpers (importable as `helpers`; backend/tests is on pythonpath)."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.core.config import Settings
from app.main import create_app
from app.services.health import Check


def make_settings(**overrides: object) -> Settings:
    base: dict[str, object] = {
        "app_env": "test",
        "log_format": "text",
        "database_url": "postgresql+psycopg://u:p@127.0.0.1:1/none",
        "qdrant_url": "http://127.0.0.1:1",
        "hdfs_webhdfs_url": "",
        "health_timeout_seconds": 0.5,
    }
    base.update(overrides)
    return Settings(_env_file=None, **base)


def ok(detail: str = "ok"):
    async def fn() -> str:
        return detail

    return fn


def failing(message: str = "boom"):
    async def fn() -> str:
        raise RuntimeError(message)

    return fn


@pytest.fixture
def client_factory():
    clients: list[TestClient] = []

    def build(checks: list[Check] | None = None, **settings: object) -> TestClient:
        app = create_app(make_settings(**settings), checks=checks)
        client = TestClient(app, raise_server_exceptions=False)
        clients.append(client)
        return client

    yield build
    for client in clients:
        client.close()
