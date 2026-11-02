"""`.env.example` must list every environment variable the project reads, with safe values only.

Sources of variables checked:
- fields of the pydantic settings classes (backend, pipelines): each field is an env variable;
- direct reads in Python code: os.environ.get(NAME), os.getenv(NAME) and os.environ[NAME];
- ${NAME} interpolation in the Docker Compose files.
A variable counts as documented if `.env.example` has `NAME=` or a commented `# NAME=` line
(commented = optional, for example test-only variables).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from app.core.config import Settings
from sportsbot_pipelines.config import PipelineSettings

ROOT = Path(__file__).resolve().parents[2]
EXAMPLE = ROOT / ".env.example"
SETTINGS_CLASSES = (Settings, PipelineSettings)
# Python files whose env reads are part of the project (not virtualenvs or caches).
CODE_DIRS = ("backend", "pipelines", "agent", "ml", "evaluation", "tests")
ENV_READ = re.compile(
    r"""(?:environ\.get|getenv)\(\s*["']([A-Z][A-Z0-9_]*)["']|environ\[\s*["']([A-Z][A-Z0-9_]*)["']\s*\]"""
)
COMPOSE_VAR = re.compile(r"\$\{([A-Z][A-Z0-9_]*)")
# Read by tools or the OS, not by our code's configuration.
NOT_PROJECT_SETTINGS = {"PATH", "HOME", "PYTHONPATH"}


def documented() -> tuple[dict[str, str], set[str]]:
    """(active KEY -> value, commented keys) from .env.example."""
    active, commented = {}, set()
    for line in EXAMPLE.read_text(encoding="utf-8").splitlines():
        if m := re.match(r"^([A-Z][A-Z0-9_]*)=(.*)$", line):
            active[m.group(1)] = m.group(2)
        elif m := re.match(r"^#\s*([A-Z][A-Z0-9_]*)=", line):
            commented.add(m.group(1))
    return active, commented


def settings_vars() -> set[str]:
    return {name.upper() for cls in SETTINGS_CLASSES for name in cls.model_fields}


def code_vars() -> set[str]:
    found: set[str] = set()
    for folder in CODE_DIRS:
        for path in (ROOT / folder).rglob("*.py"):
            if any(
                part.startswith(".") or part in {"node_modules", "__pycache__"}
                for part in path.parts
            ):
                continue
            for a, b in ENV_READ.findall(path.read_text(encoding="utf-8")):
                found.add(a or b)
    return found - NOT_PROJECT_SETTINGS


def compose_vars() -> set[str]:
    found: set[str] = set()
    for path in ROOT.glob("docker-compose*.yml"):
        found.update(COMPOSE_VAR.findall(path.read_text(encoding="utf-8")))
    return found


@pytest.mark.parametrize(
    ("origin", "collect"),
    [
        ("settings classes", settings_vars),
        ("Python code", code_vars),
        ("Compose files", compose_vars),
    ],
)
def test_every_variable_is_documented(origin, collect):
    active, commented = documented()
    missing = sorted(collect() - set(active) - commented)
    assert not missing, f"variables read by {origin} but missing from .env.example: {missing}"


def test_scanner_finds_known_reads():
    # Guards the regexes: these reads exist today and must be seen.
    assert {"SPORTSBOT_INTEGRATION", "EXPECT_HDFS", "DATABASE_URL"} <= code_vars()
    assert {"POSTGRES_PASSWORD", "STACK_HDFS_WEBHDFS_URL"} <= compose_vars()
    assert {"SOURCES_FILE", "HTTP_MAX_BYTES", "QDRANT_URL"} <= settings_vars()


def test_no_duplicate_keys():
    keys = re.findall(r"^([A-Z][A-Z0-9_]*)=", EXAMPLE.read_text(encoding="utf-8"), re.M)
    assert len(keys) == len(set(keys)), sorted({k for k in keys if keys.count(k) > 1})


def test_values_are_safe_placeholders():
    active, _ = documented()
    for key, value in active.items():
        if any(word in key for word in ("KEY", "TOKEN", "SECRET")):
            assert value == "", f"{key} must be empty in .env.example"
        if "PASSWORD" in key:
            assert value.startswith("change-me"), f"{key} must be an obvious placeholder"
    # The password must match between POSTGRES_PASSWORD and DATABASE_URL, and be a placeholder.
    assert active["POSTGRES_PASSWORD"] in active["DATABASE_URL"]


@pytest.mark.parametrize("cls", SETTINGS_CLASSES)
def test_example_file_loads_as_settings(cls, monkeypatch):
    # Copying .env.example to .env must give working settings: empty values fall back to defaults.
    for key in documented()[0]:
        monkeypatch.delenv(key, raising=False)
    loaded = cls(_env_file=EXAMPLE)
    defaults = cls(_env_file=None)
    if cls is PipelineSettings:
        assert loaded.sources_file == defaults.sources_file
        assert loaded.migrations_dir == defaults.migrations_dir
        assert loaded.local_bronze_dir == defaults.local_bronze_dir
        assert loaded.http_max_bytes == 5_000_000
    else:
        assert loaded.qdrant_url == "http://localhost:6333"
