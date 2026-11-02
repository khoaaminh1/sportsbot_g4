"""Apply SQL migrations that are not yet recorded in `schema_migrations`.

The postgres image runs `infra/postgres/migrations/*.sql` only on a brand-new volume. This runner
brings existing databases up to date. Each file manages its own transaction and inserts its own
version row; files are applied in name order and the version is the prefix before `_`.
"""

from __future__ import annotations

import logging
from pathlib import Path

from sqlalchemy import Engine, text

logger = logging.getLogger(__name__)


def _applied(engine: Engine) -> set[str]:
    with engine.connect() as conn:
        exists = conn.execute(text("SELECT to_regclass('public.schema_migrations')")).scalar()
        if not exists:
            return set()
        return {row[0] for row in conn.execute(text("SELECT version FROM schema_migrations"))}


def pending(engine: Engine, migrations_dir: Path) -> list[Path]:
    done = _applied(engine)
    files = sorted(migrations_dir.glob("[0-9][0-9][0-9]_*.sql"))
    return [f for f in files if f.name.split("_", 1)[0] not in done]


def migrate(engine: Engine, migrations_dir: Path) -> list[str]:
    applied = []
    for path in pending(engine, migrations_dir):
        sql = path.read_text(encoding="utf-8")
        # psycopg runs a multi-statement string as one call; the file's BEGIN/COMMIT apply.
        raw = engine.raw_connection()
        try:
            raw.autocommit = True
            with raw.cursor() as cur:
                cur.execute(sql)
        finally:
            raw.close()
        logger.info("migration_applied", extra={"file": path.name})
        applied.append(path.name)
    return applied
