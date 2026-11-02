"""Minimal Qdrant REST client (create collection, upsert chunks, vector search).

Plain httpx keeps the dependency list short; the calls used are stable Qdrant REST endpoints.
One point = one chunk of one article. Point ids are UUIDv5 of "<article_id>:<chunk_index>", so
re-processing an article overwrites its points instead of adding duplicates.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

import httpx

CHUNKS_COLLECTION = "sportsbot_chunks"
_NAMESPACE = uuid.UUID("6f1d8f3e-2a7b-4c55-9a43-5b0f1c2d9e10")


class QdrantError(Exception):
    pass


def point_id(article_id: str, chunk_index: int) -> str:
    return str(uuid.uuid5(_NAMESPACE, f"{article_id}:{chunk_index}"))


@dataclass(frozen=True)
class ScoredPoint:
    id: str
    score: float
    payload: dict[str, Any]


class QdrantClient:
    def __init__(
        self,
        base_url: str,
        collection: str = CHUNKS_COLLECTION,
        *,
        timeout: float = 30.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.collection = collection
        self._http = httpx.Client(
            base_url=base_url.rstrip("/"), timeout=timeout, transport=transport
        )

    def _check(self, resp: httpx.Response) -> dict[str, Any]:
        if resp.status_code >= 400:
            raise QdrantError(f"Qdrant HTTP {resp.status_code}: {resp.text[:300]}")
        return resp.json()

    def ensure_collection(self, dim: int) -> bool:
        """Create the collection (cosine) and payload indexes if missing. True if created."""
        resp = self._http.get(f"/collections/{self.collection}")
        if resp.status_code == 200:
            return False
        self._check(
            self._http.put(
                f"/collections/{self.collection}",
                json={"vectors": {"size": dim, "distance": "Cosine"}},
            )
        )
        for field_name, schema in (
            ("article_id", "keyword"),
            ("source_id", "keyword"),
            ("language", "keyword"),
            ("published_ts", "integer"),
        ):
            self._check(
                self._http.put(
                    f"/collections/{self.collection}/index",
                    json={"field_name": field_name, "field_schema": schema},
                )
            )
        return True

    def upsert(self, points: list[dict[str, Any]]) -> None:
        if points:
            self._check(
                self._http.put(
                    f"/collections/{self.collection}/points",
                    params={"wait": "true"},
                    json={"points": points},
                )
            )

    def search(
        self, vector: list[float], limit: int = 30, flt: dict[str, Any] | None = None
    ) -> list[ScoredPoint]:
        body: dict[str, Any] = {"query": vector, "limit": limit, "with_payload": True}
        if flt:
            body["filter"] = flt
        data = self._check(
            self._http.post(f"/collections/{self.collection}/points/query", json=body)
        )
        return [
            ScoredPoint(str(p["id"]), float(p["score"]), p.get("payload") or {})
            for p in data["result"]["points"]
        ]

    def count_article(self, article_id: str) -> int:
        body = {
            "exact": True,
            "filter": {"must": [{"key": "article_id", "match": {"value": article_id}}]},
        }
        data = self._check(
            self._http.post(f"/collections/{self.collection}/points/count", json=body)
        )
        return int(data["result"]["count"])

    def count(self) -> int:
        data = self._check(
            self._http.post(f"/collections/{self.collection}/points/count", json={"exact": True})
        )
        return int(data["result"]["count"])

    def close(self) -> None:
        self._http.close()
