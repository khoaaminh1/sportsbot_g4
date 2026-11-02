"""Database engine and sessions (SQLAlchemy 2, psycopg 3).

The schema itself is created by the SQL migrations in `infra/postgres/migrations/`, not by the ORM,
so collectors, Spark exports and the API all share one reviewed definition.
"""

from __future__ import annotations

from collections.abc import Iterator

from sqlalchemy import Engine, create_engine, text
from sqlalchemy.orm import Session, sessionmaker


def make_engine(database_url: str, connect_timeout: float = 2.0) -> Engine:
    return create_engine(
        database_url,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=5,
        connect_args={"connect_timeout": max(1, int(connect_timeout))},
    )


def make_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False)


def session_scope(factory: sessionmaker[Session]) -> Iterator[Session]:
    """FastAPI dependency body: one session per request, closed afterwards."""
    session = factory()
    try:
        yield session
    finally:
        session.close()


def ping(engine: Engine) -> str | None:
    """Return the applied schema version, or None if the migrations table is missing."""
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
        row = conn.execute(
            text(
                "SELECT version FROM schema_migrations "
                "ORDER BY applied_at DESC, version DESC LIMIT 1"
            )
        ).first()
        return row[0] if row else None
