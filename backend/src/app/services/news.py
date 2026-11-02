"""News queries on the `articles` table (written by the operational consumer).

Users see title, excerpt (max 400 characters), source, time and the original link: never the
full stored text (copyright; see docs/contracts/api.openapi.yaml, ArticleSummary).
"""

from __future__ import annotations

import base64
from datetime import datetime
from typing import Any, Protocol

from sqlalchemy import Engine, text

COLUMNS = """
    article_id, title, left(coalesce(summary, content), 400) AS excerpt, source_name,
    source_url, published_at, collected_at, ingested_at, language, sport_category, teams,
    athletes, competition
"""


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def to_summary(row: dict[str, Any]) -> dict[str, Any]:
    excerpt = row.get("excerpt")
    return {
        "article_id": row["article_id"],
        "title": row["title"],
        "excerpt": excerpt[:400] if excerpt else None,
        "source_name": row["source_name"],
        "source_url": row["source_url"],
        "published_at": _iso(row.get("published_at")),
        "collected_at": _iso(row.get("collected_at")) or "",
        "language": row.get("language"),
        "sport_category": row.get("sport_category"),
        "teams": list(row.get("teams") or []),
        "athletes": list(row.get("athletes") or []),
        "competition": row.get("competition"),
    }


def encode_cursor(published_at: str | None, article_id: str) -> str:
    return base64.urlsafe_b64encode(f"{published_at or ''}|{article_id}".encode()).decode()


def decode_cursor(cursor: str) -> tuple[str | None, str]:
    raw = base64.urlsafe_b64decode(cursor.encode()).decode()
    ts, _, aid = raw.partition("|")
    if not aid:
        raise ValueError("bad cursor")
    return (ts or None), aid


class NewsRepository(Protocol):
    def latest(
        self, *, limit: int, sport: str | None, language: str | None, cursor: str | None
    ) -> dict[str, Any]: ...

    def get(self, article_id: str) -> dict[str, Any] | None: ...

    def since(self, ingested_after: datetime | None, limit: int) -> list[dict[str, Any]]: ...

    def categories(self) -> list[dict[str, Any]]: ...

    def sources(self) -> list[dict[str, Any]]: ...


class PostgresNewsRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def latest(
        self, *, limit: int, sport: str | None, language: str | None, cursor: str | None
    ) -> dict[str, Any]:
        where = ["content IS NOT NULL", "published_at IS NOT NULL"]
        params: dict[str, Any] = {"limit": limit + 1}
        if sport:
            where.append("sport_category = :sport")
            params["sport"] = sport
        if language:
            where.append("language = :language")
            params["language"] = language
        if cursor:
            ts, aid = decode_cursor(cursor)
            where.append("(published_at, article_id) < (CAST(:cts AS timestamptz), :caid)")
            params.update(cts=ts, caid=aid)
        sql = (
            f"SELECT {COLUMNS} FROM articles WHERE {' AND '.join(where)} "
            "ORDER BY published_at DESC, article_id DESC LIMIT :limit"
        )
        with self.engine.connect() as conn:
            rows = [dict(r) for r in conn.execute(text(sql), params).mappings()]
        items = [to_summary(r) for r in rows[:limit]]
        next_cursor = (
            encode_cursor(items[-1]["published_at"], items[-1]["article_id"])
            if len(rows) > limit
            else None
        )
        return {"items": items, "next_cursor": next_cursor}

    def get(self, article_id: str) -> dict[str, Any] | None:
        with self.engine.connect() as conn:
            row = (
                conn.execute(
                    text(f"SELECT {COLUMNS} FROM articles WHERE article_id = :id"),
                    {"id": article_id},
                )
                .mappings()
                .first()
            )
        return to_summary(dict(row)) if row else None

    def since(self, ingested_after: datetime | None, limit: int) -> list[dict[str, Any]]:
        sql = f"SELECT {COLUMNS} FROM articles WHERE content IS NOT NULL"
        params: dict[str, Any] = {"limit": limit}
        if ingested_after is not None:
            sql += " AND ingested_at > :after"
            params["after"] = ingested_after
        sql += " ORDER BY ingested_at DESC LIMIT :limit"
        with self.engine.connect() as conn:
            rows = [dict(r) for r in conn.execute(text(sql), params).mappings()]
        return rows

    def categories(self) -> list[dict[str, Any]]:
        sql = (
            "SELECT sport_category, count(*) AS article_count, "
            "max(published_at) AS latest_published_at FROM articles "
            "WHERE sport_category IS NOT NULL GROUP BY 1 ORDER BY 2 DESC"
        )
        with self.engine.connect() as conn:
            rows = conn.execute(text(sql)).mappings().all()
        return [{**dict(r), "latest_published_at": _iso(r["latest_published_at"])} for r in rows]

    def sources(self) -> list[dict[str, Any]]:
        sql = """
            SELECT s.source_id, s.name, s.kind, s.language, s.enabled,
                   (SELECT status FROM collection_runs r WHERE r.source_id = s.source_id
                    ORDER BY started_at DESC LIMIT 1) AS last_run_status,
                   (SELECT max(finished_at) FROM collection_runs r
                    WHERE r.source_id = s.source_id AND status = 'success') AS last_success_at
            FROM sources s ORDER BY s.source_id
        """
        with self.engine.connect() as conn:
            rows = conn.execute(text(sql)).mappings().all()
        return [{**dict(r), "last_success_at": _iso(r["last_success_at"])} for r in rows]
