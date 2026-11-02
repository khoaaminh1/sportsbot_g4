"""Semantic consumer: `articles.extracted` -> chunks -> BGE-M3 vectors -> Qdrant.

Each chunk is embedded together with the article title ("title\\n\\nchunk") so short chunks keep
their topic. The payload carries what the RAG answer needs to cite: title, source, URL and
publication time.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime
from typing import Any, Protocol

import httpx

from sportsbot_agent.clients.embeddings import EmbeddingError
from sportsbot_agent.clients.qdrant import QdrantError, point_id
from sportsbot_pipelines.consumers.chunking import chunk_text
from sportsbot_pipelines.streaming.bus import TransientError

logger = logging.getLogger(__name__)


class Embedder(Protocol):
    def embed(self, texts: list[str]) -> list[list[float]]: ...


class VectorStore(Protocol):
    def upsert(self, points: list[dict[str, Any]]) -> None: ...


def _timestamp(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
    except ValueError:
        return None


def build_points(
    message: dict[str, Any], vectors: list[list[float]], chunks: list[str]
) -> list[dict[str, Any]]:
    points = []
    for i, (chunk, vector) in enumerate(zip(chunks, vectors, strict=True)):
        points.append(
            {
                "id": point_id(message["article_id"], i),
                "vector": vector,
                "payload": {
                    "article_id": message["article_id"],
                    "chunk_index": i,
                    "chunk_count": len(chunks),
                    "text": chunk,
                    "title": message["title"],
                    "source_id": message["source_id"],
                    "source_name": message["source_name"],
                    "url": message.get("canonical_url") or message["url"],
                    "published_at": message.get("published_at"),
                    "published_ts": _timestamp(message.get("published_at")),
                    "language": message.get("language"),
                },
            }
        )
    return points


def make_handler(
    embedder: Embedder,
    store: VectorStore,
    *,
    max_chars: int = 1200,
    overlap_chars: int = 200,
    max_chunks: int = 40,
) -> Callable[[dict[str, Any]], None]:
    def handle(message: dict[str, Any]) -> None:
        chunks = chunk_text(message["text"], max_chars, overlap_chars)[:max_chunks]
        if not chunks:
            return
        inputs = [f"{message['title']}\n\n{c}" for c in chunks]
        try:
            vectors = embedder.embed(inputs)
            store.upsert(build_points(message, vectors, chunks))
        except (EmbeddingError, QdrantError, httpx.HTTPError) as exc:
            raise TransientError(f"semantic layer unavailable: {exc}") from exc
        logger.debug(
            "article_embedded", extra={"article_id": message["article_id"], "chunks": len(chunks)}
        )

    return handle
