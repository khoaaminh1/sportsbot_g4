"""Load stored articles for the prototype.

Reads Bronze records (JSON Lines, plain or gzip) from a local folder. Today that folder is filled
by `make articles-export`, which copies the collector's Bronze data out of HDFS. Later phases
replace this with the Gold tables in PostgreSQL and the search indexes; the `Article` shape stays.
"""

from __future__ import annotations

import gzip
import html
import json
import re
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

_TAG = re.compile(r"<[^>]+>")
_SPACE = re.compile(r"\s+")


def html_to_text(value: str | None) -> str:
    if not value:
        return ""
    return _SPACE.sub(" ", html.unescape(_TAG.sub(" ", value))).strip()


@dataclass(frozen=True)
class Article:
    article_id: str
    title: str
    summary: str
    source_name: str
    url: str
    published_at: str | None
    collected_at: str
    language: str | None


def _read_lines(path: Path) -> Iterator[str]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as handle:
        yield from handle


def load_articles(folder: Path) -> list[Article]:
    """All articles in folder (recursively), one per article id (the latest collection wins)."""
    if not folder.exists():
        return []
    latest: dict[str, Article] = {}
    files = sorted([*folder.rglob("*.jsonl"), *folder.rglob("*.jsonl.gz")])
    for path in files:
        for line in _read_lines(path):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                article = Article(
                    article_id=rec["record_id"],
                    title=rec["title"].strip(),
                    summary=html_to_text(rec.get("summary_html")),
                    source_name=rec.get("source_name") or rec.get("source_id", ""),
                    url=rec["canonical_url"],
                    published_at=rec.get("published_at"),
                    collected_at=rec.get("collected_at", ""),
                    language=rec.get("language_hint"),
                )
            except (json.JSONDecodeError, KeyError, AttributeError):
                continue  # skip malformed lines; Bronze is raw data
            previous = latest.get(article.article_id)
            if previous is None or article.collected_at >= previous.collected_at:
                latest[article.article_id] = article
    return list(latest.values())
