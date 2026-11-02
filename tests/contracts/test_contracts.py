"""Contract tests: the frozen contracts in docs/contracts/ agree with the code and the schema.

- article.schema.json accepts valid (synthetic) fixtures and rejects broken records;
- every article field has a column in the articles table migration;
- every backend route is in api.openapi.yaml as `implemented`, and vice versa;
- live responses of implemented endpoints validate against the contract schemas.
"""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path

import pytest
import yaml
from fastapi.routing import APIRoute
from helpers import make_settings, ok
from jsonschema import Draft202012Validator, FormatChecker

from app.main import create_app
from app.services.health import Check

ROOT = Path(__file__).resolve().parents[2]
CONTRACTS = ROOT / "docs" / "contracts"
ARTICLE_SCHEMA = json.loads((CONTRACTS / "article.schema.json").read_text(encoding="utf-8"))
OPENAPI = yaml.safe_load((CONTRACTS / "api.openapi.yaml").read_text(encoding="utf-8"))
FIXTURES = json.loads(
    (Path(__file__).parent / "fixtures" / "articles_valid.json").read_text(encoding="utf-8")
)
HTTP_METHODS = {"get", "post", "put", "patch", "delete"}


def article_validator() -> Draft202012Validator:
    Draft202012Validator.check_schema(ARTICLE_SCHEMA)
    return Draft202012Validator(ARTICLE_SCHEMA, format_checker=FormatChecker())


def component_validator(name: str) -> Draft202012Validator:
    # Validate against one component while letting its $refs resolve inside the document.
    schema = {"$ref": f"#/components/schemas/{name}", "components": OPENAPI["components"]}
    return Draft202012Validator(schema, format_checker=FormatChecker())


# ------------------------------------------------------------------- articles ---


def test_fixtures_are_labeled_synthetic():
    assert "SYNTHETIC" in FIXTURES["_origin"]


@pytest.mark.parametrize("index", range(len(FIXTURES["items"])))
def test_valid_articles_pass(index):
    article_validator().validate(FIXTURES["items"][index])


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        (lambda a: a.pop("collected_at"), "collected_at is required"),
        (lambda a: a.update(content_hash="not-a-hash"), "content_hash must be sha256 hex"),
        (lambda a: a.update(sport_category="cricket"), "sport outside the enum"),
        (lambda a: a.update(language="vie"), "language must be ISO 639-1"),
        (lambda a: a.update(source_url="ftp://example.com/x"), "source_url must be http(s)"),
        (lambda a: a.update(published_at="yesterday"), "published_at must be RFC 3339"),
        (lambda a: a.update(rating=5), "no unknown fields"),
        (lambda a: a.update(teams=["A", "A"]), "teams are unique"),
    ],
)
def test_broken_articles_fail(change, reason):
    article = copy.deepcopy(FIXTURES["items"][0])
    change(article)
    errors = list(article_validator().iter_errors(article))
    assert errors, reason


def test_every_article_field_has_a_db_column():
    sql = (ROOT / "infra" / "postgres" / "migrations" / "001_init.sql").read_text(encoding="utf-8")
    block = re.search(r"CREATE TABLE articles \((.*?)\n\);", sql, re.S)
    assert block, "articles table not found in 001_init.sql"
    columns = {
        line.split()[0]
        for line in block.group(1).splitlines()
        if line.strip() and not line.strip().startswith("--")
    }
    missing = set(ARTICLE_SCHEMA["properties"]) - columns
    assert not missing, f"article fields without a column: {sorted(missing)}"


# ------------------------------------------------------------------------ API ---


def contract_operations(status: str | None = None) -> set[tuple[str, str]]:
    ops = set()
    for path, item in OPENAPI["paths"].items():
        for method, op in item.items():
            if method in HTTP_METHODS and (status is None or op.get("x-status") == status):
                ops.add((method.upper(), path))
    return ops


def app_operations() -> set[tuple[str, str]]:
    app = create_app(make_settings(), checks=[])
    return {
        (method, route.path)
        for route in app.routes
        if isinstance(route, APIRoute)
        for method in route.methods
        if method != "HEAD"
    }


def test_openapi_contract_is_well_formed():
    assert OPENAPI["openapi"].startswith("3.1")
    for path, item in OPENAPI["paths"].items():
        for method, op in item.items():
            if method in HTTP_METHODS:
                assert op.get("x-status") in {"implemented", "planned"}, (method, path)
                assert op.get("operationId"), (method, path)
    for name in OPENAPI["components"]["schemas"]:
        Draft202012Validator.check_schema(
            {"$ref": f"#/components/schemas/{name}", "components": OPENAPI["components"]}
        )


def test_backend_routes_match_implemented_contract():
    served = app_operations()
    implemented = contract_operations("implemented")
    assert served == implemented, {
        "served but not marked implemented": sorted(served - implemented),
        "marked implemented but not served": sorted(implemented - served),
    }


def test_health_responses_match_contract(client_factory):
    healthy = client_factory(checks=[Check("postgres", ok(), True), Check("hdfs", None, False)])
    component_validator("HealthResponse").validate(healthy.get("/health").json())
    component_validator("LivenessResponse").validate(healthy.get("/health/live").json())


def test_error_responses_match_contract(client_factory):
    client = client_factory(checks=[])
    component_validator("ErrorResponse").validate(client.get("/does-not-exist").json())
